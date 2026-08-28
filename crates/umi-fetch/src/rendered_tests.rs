//! Doc 05.6's policy, checked without a browser.
//!
//! Everything here is a pure function on purpose. A suite that needs Chrome
//! installed is a suite that runs on one box in the fleet and on nobody's
//! laptop, and a rule nobody can check is a rule that drifts. The parts that do
//! need a browser are the pool and the event loop, and those are covered by the
//! render bench and by running a real crawl on server2.

use super::{
    Counts, Decision, Reason, RenderConfig, SUBRESOURCE_CAP, TRACKERS, allowed, decide, entries,
    header_map, markup, net_failure, set, tracker, version_of,
};
use crate::outcome::{Failure, Stage, Version};

use chromiumoxide::cdp::browser_protocol::fetch::HeaderEntry;
use chromiumoxide::cdp::browser_protocol::network::{Headers, ResourceType};

/// A CDP header bag from pairs, the way Chromium sends one.
fn headers(pairs: &[(&str, &str)]) -> Headers {
    let mut object = serde_json::Map::new();
    for (name, value) in pairs {
        object.insert(
            (*name).to_owned(),
            serde_json::Value::String((*value).into()),
        );
    }
    Headers::new(serde_json::Value::Object(object))
}

#[test]
fn allows_what_builds_the_dom() {
    assert!(allowed(&ResourceType::Document));
    assert!(allowed(&ResourceType::Xhr));
    assert!(allowed(&ResourceType::Fetch));
    assert!(allowed(&ResourceType::Script));
}

#[test]
fn blocks_what_only_paints() {
    // Doc 05.6's four named blocks. These are most of the bytes and none of the
    // words, which is the whole reason T3 is affordable at all.
    assert!(!allowed(&ResourceType::Image));
    assert!(!allowed(&ResourceType::Media));
    assert!(!allowed(&ResourceType::Font));
    assert!(!allowed(&ResourceType::Stylesheet));
}

#[test]
fn blocks_what_the_spec_did_not_name() {
    // The default is no. A resource type nobody thought about does not get in
    // because nobody thought about it.
    assert!(!allowed(&ResourceType::Ping));
    assert!(!allowed(&ResourceType::WebSocket));
    assert!(!allowed(&ResourceType::Manifest));
    assert!(!allowed(&ResourceType::Other));
}

#[test]
fn allows_the_preflight_for_a_request_it_allowed() {
    // Refusing this would fail the cross origin fetch we just said yes to,
    // which is not a policy anybody meant to write.
    assert!(allowed(&ResourceType::Preflight));
}

#[test]
fn tracker_list_is_sorted_and_unique() {
    // `tracker` binary searches it, so an unsorted entry is a rule that is in
    // the file and not in the build.
    let mut sorted = TRACKERS;
    sorted.sort_unstable();
    assert_eq!(sorted, TRACKERS);
    let mut seen = std::collections::HashSet::new();
    for domain in TRACKERS {
        assert!(seen.insert(domain), "{domain} is listed twice");
        assert!(!domain.starts_with("www."), "{domain} is not registrable");
        assert!(domain.contains('.'), "{domain} is not a domain");
    }
}

#[test]
fn tracker_matches_the_registrable_domain() {
    assert!(tracker("doubleclick.net"));
    assert!(tracker("google-analytics.com"));
    // Matched exactly, so a lookalike registered by somebody else is not on the
    // list and a subdomain is resolved to its registrable domain first.
    assert!(!tracker("stats.doubleclick.net"));
    assert!(!tracker("doubleclick.net.example.com"));
    assert!(!tracker("example.com"));
}

#[test]
fn refuses_a_third_party_tracker() {
    assert_eq!(
        decide(
            &ResourceType::Script,
            "https://www.googletagmanager.com/gtag/js?id=G-1",
            "example.com",
            0,
            SUBRESOURCE_CAP,
        ),
        Decision::Block(Reason::Tracker)
    );
}

#[test]
fn leaves_first_party_alone_even_on_the_list() {
    // A site's own analytics is often the same bundle that builds the page, and
    // doc 05.6's rule is about third parties. If somebody ever crawls
    // google-analytics.com itself, its own scripts load.
    assert_eq!(
        decide(
            &ResourceType::Script,
            "https://google-analytics.com/analytics.js",
            "google-analytics.com",
            0,
            SUBRESOURCE_CAP,
        ),
        Decision::Allow
    );
}

#[test]
fn subdomains_of_the_page_are_first_party() {
    assert_eq!(
        decide(
            &ResourceType::Xhr,
            "https://api.example.com/v1/articles",
            "example.com",
            0,
            SUBRESOURCE_CAP,
        ),
        Decision::Allow
    );
}

#[test]
fn resource_type_is_checked_before_the_domain() {
    // An image from a tracker is refused for being an image. The order matters
    // only for the counter, and the counter is what tells an operator whether
    // the list is earning its keep.
    assert_eq!(
        decide(
            &ResourceType::Image,
            "https://doubleclick.net/pixel.gif",
            "example.com",
            0,
            SUBRESOURCE_CAP,
        ),
        Decision::Block(Reason::Resource)
    );
}

#[test]
fn budget_stops_a_page_that_keeps_asking() {
    assert_eq!(
        decide(
            &ResourceType::Script,
            "https://example.com/late.js",
            "example.com",
            SUBRESOURCE_CAP,
            SUBRESOURCE_CAP,
        ),
        Decision::Block(Reason::Budget)
    );
    assert_eq!(
        decide(
            &ResourceType::Script,
            "https://example.com/early.js",
            "example.com",
            SUBRESOURCE_CAP - 1,
            SUBRESOURCE_CAP,
        ),
        Decision::Allow
    );
}

#[test]
fn the_document_itself_is_never_over_budget() {
    // The page is not one of its own subresources. A spent budget stopping the
    // navigation would mean a tab that renders nothing at all.
    assert_eq!(
        decide(
            &ResourceType::Document,
            "https://example.com/",
            "example.com",
            SUBRESOURCE_CAP * 4,
            SUBRESOURCE_CAP,
        ),
        Decision::Allow
    );
}

#[test]
fn a_url_with_no_host_is_judged_on_its_type_alone() {
    // `data:` and `blob:` are the page talking to itself, so there is no third
    // party to refuse and the resource rule is the whole decision.
    assert_eq!(
        decide(
            &ResourceType::Script,
            "data:text/javascript,void%200",
            "example.com",
            0,
            SUBRESOURCE_CAP,
        ),
        Decision::Allow
    );
    assert_eq!(
        decide(
            &ResourceType::Image,
            "data:image/gif;base64,R0lGOD",
            "example.com",
            0,
            SUBRESOURCE_CAP,
        ),
        Decision::Block(Reason::Resource)
    );
}

#[test]
fn defaults_are_the_numbers_in_the_spec() {
    let config = RenderConfig::default();
    assert_eq!(config.tabs, 8);
    assert_eq!(config.quiet, std::time::Duration::from_millis(1500));
    assert_eq!(config.ceiling, std::time::Duration::from_secs(10));
    assert_eq!(config.subresource_cap, 2 * 1024 * 1024);
    assert_eq!(config.pages_per_tab, 50);
    assert_eq!(config.tab_lifetime, std::time::Duration::from_secs(600));
    // The sandbox is on until an operator turns it off, and the operator who
    // wants it off should have to say so in a config file.
    assert!(config.sandbox);
}

#[test]
fn every_reason_has_a_word() {
    for reason in [
        Reason::Resource,
        Reason::Tracker,
        Reason::Budget,
        Reason::OffDomain,
        Reason::Redirects,
    ] {
        assert!(!reason.as_str().is_empty());
        assert!(!reason.as_str().contains(' '));
    }
}

#[test]
fn counts_divide_by_pages_and_not_by_zero() {
    let empty = Counts::default();
    assert_eq!(empty.mean_render(), std::time::Duration::ZERO);
    assert_eq!(empty.mean_bytes(), 0);

    let counts = Counts {
        pages: 4,
        nanos: 8_000_000_000,
        bytes: 2048,
        ..Counts::default()
    };
    assert_eq!(counts.mean_render(), std::time::Duration::from_secs(2));
    assert_eq!(counts.mean_bytes(), 512);
}

#[test]
fn folded_headers_come_back_as_separate_headers() {
    // Chromium joins repeated headers with a newline, and a `HeaderValue`
    // cannot hold one. Splitting is what keeps a second `Set-Cookie` from
    // taking the first one down with it.
    let map = header_map(&headers(&[
        ("content-type", "text/html; charset=utf-8"),
        ("set-cookie", "a=1\nb=2"),
    ]));
    assert_eq!(map.get("content-type").unwrap(), "text/html; charset=utf-8");
    assert_eq!(map.get_all("set-cookie").iter().count(), 2);
}

#[test]
fn a_header_that_is_not_a_string_is_dropped_rather_than_guessed() {
    let mut object = serde_json::Map::new();
    object.insert("x-count".to_owned(), serde_json::Value::from(7));
    let map = header_map(&Headers::new(serde_json::Value::Object(object)));
    assert!(map.is_empty());
}

#[test]
fn setting_a_header_replaces_rather_than_appends() {
    // `Fetch.continueRequest` takes the whole header set, so a duplicate would
    // go on the wire as a duplicate.
    let mut head = entries(&headers(&[("Accept", "*/*")]));
    set(&mut head, "accept", "text/html");
    set(&mut head, "if-none-match", "\"v1\"");
    assert_eq!(head.len(), 2);
    assert!(has(&head, "accept", "text/html"));
    assert!(has(&head, "if-none-match", "\"v1\""));
}

/// Whether a header list holds this exact pair, matching the name case
/// insensitively the way HTTP does.
fn has(head: &[HeaderEntry], name: &str, value: &str) -> bool {
    head.iter()
        .any(|entry| entry.name.eq_ignore_ascii_case(name) && entry.value == value)
}

#[test]
fn net_errors_land_in_the_class_doc_05_8_acts_on() {
    assert_eq!(net_failure("net::ERR_NAME_NOT_RESOLVED"), Failure::Dns);
    assert_eq!(net_failure("net::ERR_CERT_DATE_INVALID"), Failure::Tls);
    assert_eq!(net_failure("net::ERR_SSL_PROTOCOL_ERROR"), Failure::Tls);
    assert_eq!(
        net_failure("net::ERR_CONNECTION_TIMED_OUT"),
        Failure::Timeout(Stage::Connect)
    );
    assert_eq!(net_failure("net::ERR_CONNECTION_REFUSED"), Failure::Connect);
    // Anything unrecognised is a connect failure, which is the class doc 09
    // retries. Guessing worse than that would back a host off for our bug.
    assert_eq!(net_failure("net::ERR_SOMETHING_NEW"), Failure::Connect);
}

#[test]
fn alpn_tokens_become_versions() {
    assert_eq!(version_of(Some("h3")), Version::Http3);
    assert_eq!(version_of(Some("h2")), Version::Http2);
    assert_eq!(version_of(Some("http/1.0")), Version::Http10);
    assert_eq!(version_of(Some("http/1.1")), Version::Http11);
    assert_eq!(version_of(None), Version::Http11);
}

#[test]
fn only_markup_is_serialised_as_a_page() {
    assert!(markup("text/html"));
    assert!(markup("text/html; charset=utf-8"));
    assert!(markup("application/xhtml+xml"));
    // A PDF or an image opens in a Chromium viewer, and the viewer's own markup
    // is what `outerHTML` would hand back. That belongs at T1 as bytes.
    assert!(!markup("application/pdf"));
    assert!(!markup("image/png"));
    assert!(!markup("text/plain"));
    assert!(!markup(""));
}

#[test]
fn pool_capacity_is_tabs_over_the_time_a_page_takes() {
    // Doc 05.9's `browser_pool_capacity`, which the crawl loop's render budget
    // reads every tick. The spec works its example at 8 tabs over 2 seconds.
    let mut counts = Counts {
        pages: 4,
        nanos: 4 * 2_000_000_000,
        ..Counts::default()
    };
    assert!((counts.capacity(8) - 4.0).abs() < f64::EPSILON);

    // And what the bench actually measures, which is most of a second slower
    // per page and so most of a page a second lower.
    counts.nanos = 4 * 3_500_000_000;
    assert!(counts.capacity(8) < 2.4);
    assert!(counts.capacity(8) > 2.2);
}

#[test]
fn a_pool_that_has_rendered_nothing_has_no_measured_capacity() {
    // Zero rather than infinity, because the budget reads this as how many
    // pages a second it may hand over. `Renderer::rate` is what covers the gap
    // before the first page, with an estimate that says where it came from.
    let counts = Counts::default();
    assert!((counts.capacity(8) - 0.0).abs() < f64::EPSILON);

    // Nor does a pool with no tabs, which is doc 05.6's server1.
    let rendered = Counts {
        pages: 1,
        nanos: 1_000_000_000,
        ..Counts::default()
    };
    assert!((rendered.capacity(0) - 0.0).abs() < f64::EPSILON);
}
