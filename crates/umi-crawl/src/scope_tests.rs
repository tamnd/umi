//! Doc 13's scope, tested against the documents it comes from.
//!
//! Most of these are matcher cases, and they are worth spelling out one at a
//! time because a scope is the only thing between a crawl and the rest of the
//! web. The expensive bug here is not a crash, it is a focused crawl that
//! quietly wandered off its own site and a report nobody questioned.

use super::scope::{ContentFilter, Corpus, LinkPolicy, Matcher, RateOverride, Scope};

fn url(text: &str) -> url::Url {
    url::Url::parse(text).expect("test url parses")
}

#[test]
fn the_general_crawl_is_profile_zero_and_admits_everything() {
    let scope = Scope::general();
    assert_eq!(scope.id, 0);
    assert!(scope.is_general());
    assert!(!scope.filters_links());
    assert!(scope.allows("https://example.com/"));
    assert!(scope.allows("https://anything.example.org/deep/path?q=1"));
}

#[test]
fn a_host_suffix_takes_subdomains_and_not_a_lookalike() {
    let m = Matcher::HostSuffix("example.com".to_owned());
    assert!(m.matches(&url("https://example.com/")));
    assert!(m.matches(&url("https://www.example.com/")));
    assert!(m.matches(&url("https://a.b.example.com/x")));
    // The one that matters. A suffix compare without the dot check admits
    // notexample.com, which is a different site owned by somebody else.
    assert!(!m.matches(&url("https://notexample.com/")));
    assert!(!m.matches(&url("https://example.com.evil.test/")));
}

#[test]
fn a_host_is_exact_and_a_pld_is_the_registrable_domain() {
    let host = Matcher::Host("www.example.com".to_owned());
    assert!(host.matches(&url("https://www.example.com/")));
    assert!(!host.matches(&url("https://example.com/")));

    let pld = Matcher::Pld("example.com".to_owned());
    assert!(pld.matches(&url("https://deep.sub.example.com/")));
    assert!(!pld.matches(&url("https://example.org/")));
}

#[test]
fn a_path_prefix_needs_the_host_and_the_prefix() {
    let m = Matcher::PathPrefix {
        host: "doc.rust-lang.org".to_owned(),
        prefix: "/std/".to_owned(),
    };
    assert!(m.matches(&url("https://doc.rust-lang.org/std/vec/index.html")));
    assert!(!m.matches(&url("https://doc.rust-lang.org/book/")));
    assert!(!m.matches(&url("https://other.rust-lang.org/std/")));
}

#[test]
fn matching_is_case_insensitive_on_the_host() {
    // A URL parser lowercases the host, but a profile author writes what they
    // like, and a profile that silently matched nothing because somebody typed
    // a capital letter would be a bad afternoon.
    let scope = Scope::for_target("Example.COM").expect("target parses");
    assert!(scope.allows("https://www.example.com/x"));
}

#[test]
fn exclude_beats_include() {
    let profile = r#"
name = "rust-docs"
include = [{ host_suffix = "rust-lang.org" }]
exclude = [{ path_prefix = { host = "doc.rust-lang.org", prefix = "/nightly/" } }]
"#;
    let scope = Scope::from_toml(profile).expect("profile parses");
    assert!(scope.allows("https://doc.rust-lang.org/std/"));
    assert!(!scope.allows("https://doc.rust-lang.org/nightly/std/"));
    assert!(!scope.allows("https://example.com/"));
}

#[test]
fn a_url_regex_is_anchored_at_the_start() {
    let profile = r#"
name = "blog"
include = [{ url_regex = "https://example\\.com/blog/" }]
"#;
    let scope = Scope::from_toml(profile).expect("profile parses");
    assert!(scope.allows("https://example.com/blog/a-post"));
    // Unanchored, this would match, because the pattern appears in the query.
    // Doc 13.2 says the pattern is anchored, and this is the case that shows
    // why: a redirector on somebody else's host is not in scope.
    assert!(!scope.allows("https://evil.test/?to=https://example.com/blog/"));
}

#[test]
fn a_bad_regex_is_an_error_and_not_a_scope_that_matches_nothing() {
    let profile = r#"
name = "broken"
include = [{ url_regex = "([unclosed" }]
"#;
    assert!(Scope::from_toml(profile).is_err());
}

#[test]
fn a_target_is_read_the_way_a_person_means_it() {
    let domain = Scope::for_target("example.com").expect("domain");
    assert!(domain.allows("https://www.example.com/x"));
    assert!(!domain.allows("https://example.org/x"));

    let path = Scope::for_target("https://doc.rust-lang.org/std/").expect("url with path");
    assert!(path.allows("https://doc.rust-lang.org/std/vec/"));
    assert!(!path.allows("https://doc.rust-lang.org/book/"));

    let bare = Scope::for_target("https://example.com/").expect("url with no path");
    assert!(bare.allows("https://sub.example.com/anything"));

    assert!(Scope::for_target("not a domain").is_err());
}

#[test]
fn a_profile_gets_an_id_from_its_bytes_and_never_gets_zero() {
    let a = Scope::for_target("example.com").expect("target");
    let b = Scope::for_target("example.com").expect("target");
    let c = Scope::for_target("example.org").expect("target");
    // Determinism, which is doc 11.1's rule and is also what makes the
    // crawl_profile column mean anything across two machines.
    assert_eq!(a.id, b.id);
    assert_eq!(a.digest, b.digest);
    assert_ne!(a.id, c.id);
    assert_ne!(a.id, 0, "zero is reserved for the general crawl");
}

#[test]
fn a_full_profile_parses_the_way_doc_13_4_writes_it() {
    let profile = r#"
name = "rust-docs"
max_depth = 6
link_policy = "one_hop"

include = [
  { host_suffix = "rust-lang.org" },
  { pld = "crates.io" },
]
exclude = [
  { path_prefix = { host = "doc.rust-lang.org", prefix = "/nightly/" } },
]

[content]
content_types = ["text/html"]
languages = ["en"]
max_bytes = 4194304

[budget]
max_pages = 500000
max_duration = "6h"
stop_when_idle = true

[rate]
max_rps_per_host = 1.5
concurrency = 8

[seed]
sitemaps = true
robots_sitemaps = true
urls = ["https://doc.rust-lang.org/std/"]
"#;
    let scope = Scope::from_toml(profile).expect("profile parses");
    assert_eq!(scope.name, "rust-docs");
    assert_eq!(scope.max_depth, Some(6));
    assert_eq!(scope.link_policy, LinkPolicy::OneHop);
    assert_eq!(scope.include.len(), 2);
    assert_eq!(scope.exclude.len(), 1);
    assert_eq!(scope.content.max_bytes, 4 << 20);
    assert_eq!(scope.budget.max_pages, Some(500_000));
    assert_eq!(
        scope.budget.max_duration,
        Some(std::time::Duration::from_secs(6 * 3600))
    );
    assert!(scope.budget.stop_when_idle);
    assert_eq!(scope.rate.concurrency, 8);
    assert_eq!(scope.seed.sitemaps, Some(true));
    assert_eq!(scope.seed.robots_sitemaps, Some(true));
    assert_eq!(scope.seed.urls.len(), 1);
    assert!(scope.allows("https://crates.io/crates/serde"));
}

#[test]
fn a_minimal_profile_is_a_name_and_nothing_else() {
    let scope = Scope::from_toml("name = \"everything\"").expect("profile parses");
    assert!(scope.is_general());
    assert!(scope.budget.stop_when_idle, "focused default is to stop");
    assert_eq!(scope.link_policy, LinkPolicy::InScopeOnly);
    // Unset and not false. The caller turns these into a decision and it has
    // to be able to see that the profile said nothing, because the pass is on
    // when nobody asks for it either way.
    assert_eq!(scope.seed.sitemaps, None);
    assert_eq!(scope.seed.robots_sitemaps, None);
}

#[test]
fn a_profile_that_says_no_to_sitemaps_is_not_a_profile_that_said_nothing() {
    let scope = Scope::from_toml("name = \"x\"\n[seed]\nsitemaps = false").expect("profile parses");
    assert_eq!(scope.seed.sitemaps, Some(false));
    assert_eq!(scope.seed.robots_sitemaps, None);
}

#[test]
fn an_unknown_key_is_an_error() {
    // deny_unknown_fields, on purpose. A profile with `max_dept = 3` in it that
    // ran happily at unlimited depth is worse than one that refuses to start.
    assert!(Scope::from_toml("name = \"x\"\nmax_dept = 3").is_err());
}

#[test]
fn a_duration_is_a_number_and_a_unit() {
    for (text, secs) in [("30s", 30), ("30m", 1800), ("6h", 21600), ("2d", 172_800)] {
        let scope = Scope::from_toml(&format!(
            "name = \"x\"\n[budget]\nmax_duration = \"{text}\""
        ))
        .expect("profile parses");
        assert_eq!(
            scope.budget.max_duration,
            Some(std::time::Duration::from_secs(secs))
        );
    }
    assert!(Scope::from_toml("name = \"x\"\n[budget]\nmax_duration = \"6\"").is_err());
    assert!(Scope::from_toml("name = \"x\"\n[budget]\nmax_duration = \"6w\"").is_err());
}

#[test]
fn the_content_filter_ignores_charset_and_enforces_size() {
    let filter = ContentFilter {
        content_types: vec!["text/html".to_owned()],
        languages: Vec::new(),
        max_bytes: 1000,
    };
    assert!(filter.accepts_response(Some("text/html; charset=utf-8"), 500));
    assert!(filter.accepts_response(Some("TEXT/HTML"), 500));
    assert!(!filter.accepts_response(Some("application/pdf"), 500));
    assert!(!filter.accepts_response(Some("text/html"), 1001));
    assert!(!filter.accepts_response(None, 500));
}

#[test]
fn a_language_filter_keeps_the_undeclared_and_matches_the_primary_subtag() {
    let filter = ContentFilter {
        content_types: Vec::new(),
        languages: vec!["en".to_owned()],
        max_bytes: 1 << 20,
    };
    assert!(filter.accepts_lang(Some("en")));
    assert!(filter.accepts_lang(Some("en-gb")));
    assert!(filter.accepts_lang(None), "an undeclared page is kept");
    assert!(!filter.accepts_lang(Some("de")));

    let exact = ContentFilter {
        languages: vec!["en-GB".to_owned()],
        ..filter
    };
    assert!(exact.accepts_lang(Some("en-gb")));
    assert!(!exact.accepts_lang(Some("en-us")));
}

#[test]
fn a_rate_override_can_only_lower() {
    // Doc 13.3. A profile is a file an operator writes, and the ceiling is the
    // promise umi makes to a site it is pointed at, so the profile does not get
    // to raise it.
    let greedy = RateOverride {
        max_rps_per_host: 50.0,
        concurrency: 64,
    };
    assert!((greedy.clamped() - RateOverride::MAX_RPS_PER_HOST).abs() < f32::EPSILON);
    let gentle = RateOverride {
        max_rps_per_host: 0.25,
        concurrency: 1,
    };
    assert!((gentle.clamped() - 0.25).abs() < f32::EPSILON);
}

#[test]
fn a_profile_publishes_to_its_own_repository_unless_it_says_otherwise() {
    // The default matters more than the feature does. Every profile written
    // before the key existed carries no `corpus` line, and none of them should
    // start publishing into the sample everybody else computes over.
    let profile = r#"
name = "rust-blog"
include = [{ host_suffix = "blog.rust-lang.org" }]
"#;
    let scope = Scope::from_toml(profile).expect("profile parses");
    assert_eq!(scope.corpus, Corpus::Focus);
}

#[test]
fn a_profile_with_no_matchers_can_ask_for_the_general_corpus() {
    let profile = r#"
name = "wide"
corpus = "general"
"#;
    let scope = Scope::from_toml(profile).expect("profile parses");
    assert_eq!(scope.corpus, Corpus::General);
}

#[test]
fn a_scoped_profile_cannot_ask_for_the_general_corpus() {
    // Doc 13.7. A crawl that restricts where it may go is a focused crawl
    // whatever it calls itself, and the answer is to refuse rather than to
    // quietly publish somewhere other than where the profile says.
    let scoped = r#"
name = "wide"
corpus = "general"
include = [{ host_suffix = "example.com" }]
"#;
    assert!(Scope::from_toml(scoped).is_err());

    let excluded = r#"
name = "wide"
corpus = "general"
exclude = [{ host_suffix = "example.com" }]
"#;
    assert!(Scope::from_toml(excluded).is_err());
}

#[test]
fn a_target_on_the_command_line_is_always_a_focused_crawl() {
    // `for_target` builds on `Scope::general`, which is the general corpus, so
    // this is the test that stops an inherited field from putting one site's
    // pages in `umi-pages`.
    let scope = Scope::for_target("example.com").expect("domain");
    assert_eq!(scope.corpus, Corpus::Focus);
}

#[test]
fn an_include_flag_cannot_smuggle_a_scope_into_the_general_corpus() {
    // The same rule as the profile check, at the other door. `--include` is
    // applied after the profile parses, so a profile that asked for the general
    // corpus and passed would otherwise gain matchers afterwards and keep the
    // corpus it no longer qualifies for.
    let mut scope = Scope::from_toml("name = \"wide\"\ncorpus = \"general\"").expect("parses");
    assert!(scope.add_include(&["example.com".to_owned()]).is_err());

    let mut excluding =
        Scope::from_toml("name = \"wide\"\ncorpus = \"general\"").expect("parses");
    assert!(excluding.add_exclude(&["example.com".to_owned()]).is_err());
}
