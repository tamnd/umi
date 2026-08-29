//! Fetching robots.txt, which is where the mistakes that cost a site are.
//!
//! The parser has its own suite. `umi-robots` runs the Google conformance
//! corpus in `tests/conformance.rs`, which is the transpiled upstream file and
//! covers what a line means, and `tests/no_bypass.rs` covers doc 14.10. What is
//! left for here is the fetch, and the fetch is the half that goes wrong
//! quietly: nothing in a corpus of parser cases says what to do when the file
//! is a 503, when it is bigger than we will read, or when it points somewhere
//! else. Every case below says what the wrong answer would cost the site,
//! because that is the reason to have the test rather than the behaviour.

use std::sync::Arc;
use std::time::Duration;

use umi_state::{HostRow, State};
use umi_types::RowKey;

use crate::run_tests::{Canned, Collected, T0, crawler, page, robots_tick, seeded};

const ORIGIN: &str = "https://example.com";

/// The host record for a URL, after a tick has run.
async fn host_of(state: &Arc<dyn State>, url: &str) -> HostRow {
    let key = RowKey::for_url(url, None).expect("a crawlable url");
    state
        .host(key.host)
        .await
        .expect("the store is in memory")
        .expect("the tick leased this host, so it has a record")
}

/// A robots.txt of at least `bytes`, with `rules` on the front of it.
///
/// The filler is comment lines, so a parser that reads the whole file reaches
/// the same answer as one that stops at the limit, and the only thing the size
/// changes is whether `tail` is seen.
fn padded(rules: &str, bytes: usize, tail: &str) -> String {
    let mut out = String::with_capacity(bytes + tail.len() + rules.len());
    out.push_str(rules);
    let filler = format!("# {}\n", "padding".repeat(12));
    while out.len() < bytes {
        out.push_str(&filler);
    }
    out.push_str(tail);
    out
}

#[tokio::test]
async fn a_server_error_on_robots_disallows_the_whole_host() {
    // RFC 9309 section 2.3.1.4, and the case the gate is named for. A 5xx is
    // not an empty file. The wrong answer here is the tempting one: no rules
    // came back, so nothing is disallowed, so crawl the site. That reading
    // sends a full crawl at an origin whose own robots.txt handler is already
    // failing, which is to say at an origin that is having a bad day, and it
    // does it at exactly the moment the site has the least capacity to absorb
    // it. It is also unsafe in the ordinary sense: the rules the site wrote
    // are still there, we just cannot read them, so crawling anyway means
    // crawling what the site asked us not to.
    let state = seeded(&[&format!("{ORIGIN}/a")]).await;
    let fetch = Canned::new()
        .outcome(
            &format!("{ORIGIN}/robots.txt"),
            umi_fetch::Outcome::Failed {
                failure: umi_fetch::Failure::ServerError,
                status: Some(503),
                retry_after: None,
            },
        )
        .html(&format!("{ORIGIN}/a"), &page("A", &[]));
    let crawler = crawler(fetch, state);

    robots_tick(&crawler, &Collected::default()).await;
    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.disallowed, 1);
    assert!(!crawler.fetcher().asked_for(&format!("{ORIGIN}/a")));
}

#[tokio::test]
async fn a_robots_file_longer_than_the_limit_is_honoured_up_to_the_limit() {
    // Doc 07.4 and RFC 9309 section 2.5: parse the first 500 KiB and ignore
    // the rest. Two wrong answers are available and both cost the site. Giving
    // up on a file this size and treating it as no rules crawls a `/private/`
    // the site wrote down in the first line, and sites with robots.txt files
    // this long are exactly the large sites with the most they do not want
    // crawled. Reading the whole thing instead is a different bill: a
    // megabytes long file per host, parsed on a crawler that touches a few
    // thousand hosts an hour, is memory spent on a file that is that size by
    // accident.
    let seen = "User-agent: *\nDisallow: /private/\n";
    let unseen = "Disallow: /late/\n";
    let body = padded(seen, umi_robots::MAX_BYTES + 1024, unseen);
    assert!(body.len() > umi_robots::MAX_BYTES);

    let state = seeded(&[
        &format!("{ORIGIN}/private/x"),
        &format!("{ORIGIN}/late/y"),
        &format!("{ORIGIN}/a"),
    ])
    .await;
    let fetch = Canned::new()
        .robots(ORIGIN, &body)
        .html(&format!("{ORIGIN}/private/x"), &page("X", &[]))
        .html(&format!("{ORIGIN}/late/y"), &page("Y", &[]))
        .html(&format!("{ORIGIN}/a"), &page("A", &[]));
    let crawler = crawler(fetch, state);

    // One tick per URL, because doc 07.6 gives a host one request a second and
    // the clock here does not move on its own.
    for _ in 0..3 {
        crawler.tick(&Collected::default()).await.expect("tick");
    }
    let asked = crawler.fetcher();
    assert!(
        !asked.asked_for(&format!("{ORIGIN}/private/x")),
        "a rule inside the limit was ignored: {:?}",
        asked.asked()
    );
    assert!(
        asked.asked_for(&format!("{ORIGIN}/late/y")),
        "a rule past the limit was obeyed, so the whole file was read"
    );
}

#[tokio::test]
async fn a_robots_file_the_fetcher_would_not_finish_disallows_the_host() {
    // The other oversized case, and the one where the size beats the fetcher
    // rather than the parser. `FetchConfig::body_cap` stops reading at 512 KiB
    // and drops what it has, so a robots.txt bigger than that arrives as no
    // bytes at all rather than as a truncated file, and there is nothing to
    // parse. Disallow is the only honest answer to that: the site published
    // rules, we could not read one of them, and the alternative is crawling a
    // site on the assumption that a file we never saw said yes.
    //
    // It costs the site its crawl, which is why the gap between the two limits
    // is 12 KiB rather than something a site would land in by accident, and
    // why reading a truncated body for robots.txt specifically is worth doing
    // later.
    let state = seeded(&[&format!("{ORIGIN}/a")]).await;
    let fetch = Canned::new()
        .outcome(
            &format!("{ORIGIN}/robots.txt"),
            umi_fetch::Outcome::Failed {
                failure: umi_fetch::Failure::TooLarge,
                status: Some(200),
                retry_after: None,
            },
        )
        .html(&format!("{ORIGIN}/a"), &page("A", &[]));
    let crawler = crawler(fetch, state);

    robots_tick(&crawler, &Collected::default()).await;
    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.disallowed, 1);
    assert!(!crawler.fetcher().asked_for(&format!("{ORIGIN}/a")));
}

#[tokio::test]
async fn a_robots_redirect_off_the_domain_is_followed() {
    // RFC 9309 section 2.3.1.2 asks for at least five hops, followed even
    // across authorities, and doc 07.5 accepts the round trip. The fetcher
    // stops at a redirect that leaves the registrable domain, which is right
    // for a page and wrong for this file, so the follow lives here.
    //
    // The wrong answer costs the site everything. A robots.txt on a CDN or on
    // an apex that redirects to a vanity domain is an ordinary arrangement,
    // and treating the redirect as unreachable disallows the entire host on
    // the strength of a configuration the site is entitled to have. Treating
    // it as allow-all is the same mistake in the other direction: the rules
    // are one hop away and we chose not to read them.
    let elsewhere = "https://cdn.example.net/robots.txt";
    let state = seeded(&[&format!("{ORIGIN}/private/x"), &format!("{ORIGIN}/a")]).await;
    let fetch = Canned::new()
        .outcome(
            &format!("{ORIGIN}/robots.txt"),
            umi_fetch::Outcome::RedirectedOffDomain {
                redirects: Vec::new(),
                target: elsewhere.to_owned(),
                status: 301,
            },
        )
        .html(elsewhere, "User-agent: *\nDisallow: /private/\n")
        .html(&format!("{ORIGIN}/private/x"), &page("X", &[]))
        .html(&format!("{ORIGIN}/a"), &page("A", &[]));
    let crawler = crawler(fetch, state);

    robots_tick(&crawler, &Collected::default()).await;
    for _ in 0..2 {
        crawler.tick(&Collected::default()).await.expect("tick");
    }
    let asked = crawler.fetcher();
    assert!(
        asked.asked_for(elsewhere),
        "the redirect was not followed: {:?}",
        asked.asked()
    );
    assert!(
        asked.asked_for(&format!("{ORIGIN}/a")),
        "the host was disallowed by a redirect it is allowed to have"
    );
    assert!(
        !asked.asked_for(&format!("{ORIGIN}/private/x")),
        "the rules at the end of the redirect were not applied to this origin"
    );
}

#[tokio::test]
async fn a_robots_redirect_that_never_lands_disallows_the_host() {
    // The other end of the same rule. Five hops is the budget, and a chain
    // longer than that is not a site with an unusual layout, it is a loop or a
    // misconfiguration, and following it forever is how one host eats a
    // fetcher. Disallow rather than allow, per the fail closed reading of RFC
    // 9309 section 2.3.1.2: we asked six times and never got the file.
    //
    // The cost of getting the budget wrong in the other direction is a crawler
    // that spends its politeness budget on a redirect chain and never fetches
    // a page from the site at all, which reads to the origin as a crawler
    // hammering one URL.
    let mut fetch = Canned::new();
    for hop in 0..8 {
        fetch = fetch.outcome(
            &format!("https://hop{hop}.example/robots.txt"),
            umi_fetch::Outcome::RedirectedOffDomain {
                redirects: Vec::new(),
                target: format!("https://hop{}.example/robots.txt", hop + 1),
                status: 302,
            },
        );
    }
    let fetch = fetch
        .outcome(
            &format!("{ORIGIN}/robots.txt"),
            umi_fetch::Outcome::RedirectedOffDomain {
                redirects: Vec::new(),
                target: "https://hop0.example/robots.txt".to_owned(),
                status: 301,
            },
        )
        .html(&format!("{ORIGIN}/a"), &page("A", &[]));
    let state = seeded(&[&format!("{ORIGIN}/a")]).await;
    let crawler = crawler(fetch, state);

    robots_tick(&crawler, &Collected::default()).await;
    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.disallowed, 1);
    assert!(!crawler.fetcher().asked_for(&format!("{ORIGIN}/a")));
    // Five hops attempted and then stopped, so the sixth target is never
    // asked for. The first request is the origin's own robots.txt.
    let hops = crawler
        .fetcher()
        .asked()
        .into_iter()
        .filter(|url| url.starts_with("https://hop"))
        .count();
    assert_eq!(hops, 5, "the hop budget was not five");
}

#[tokio::test]
async fn a_crawl_delay_is_clamped_and_reaches_the_host_record() {
    // Doc 07.4 clamps a published `Crawl-delay` into 100 ms to 300 s, and doc
    // 07.6's pacer reads the host record rather than the parsed file, so a
    // delay that stops at the robots cache is a delay nobody honours. That was
    // the state of this code until the test below existed: the file was parsed
    // and the number was thrown away, so a site asking for one request every
    // hour got one every second.
    //
    // The clamp is not the site being overruled for our convenience. An hour
    // between requests on a site with a million URLs is a crawl that finishes
    // in a century, so the choice is between capping the delay and not
    // crawling the site, and doc 07.4 caps it and deprioritises the host.
    let url = format!("{ORIGIN}/a");
    let state = seeded(&[&url]).await;
    let reading = Arc::clone(&state);
    let fetch = Canned::new()
        .robots(ORIGIN, "User-agent: *\nCrawl-delay: 3600\nAllow: /\n")
        .html(&url, &page("A", &[]));
    let crawler = crawler(fetch, state);

    crawler.tick(&Collected::default()).await.expect("tick");
    let host = host_of(&reading, &url).await;
    assert_eq!(
        host.crawl_delay_ms,
        Some(300_000),
        "the clamp did not apply"
    );
    assert_eq!(
        host.delay(),
        Duration::from_millis(300_000),
        "the pacer is still on its own delay"
    );
    // The timer this tick set is still the old one, because the completion
    // owns doc 07.6's pacing columns and runs before the ladder is relearned.
    // The published delay takes effect from the next request, which is the
    // right place for it: the request that fetched robots.txt is the one that
    // had no way of knowing.
    assert_eq!(
        host.next_allowed_ms,
        T0 + u64::from(HostRow::INITIAL_DELAY_MS)
    );
}

#[tokio::test]
async fn a_crawl_delay_below_the_floor_is_raised_to_it() {
    // The same clamp at the other end, and the reason it is not symmetric.
    // A site publishing `Crawl-delay: 0.01` is telling us it can take a
    // hundred requests a second, and taking it at its word is how a crawler
    // knocks over a site that was trying to be helpful. The floor is ours to
    // set because the cost of being wrong lands on the site, not on us.
    let url = format!("{ORIGIN}/a");
    let state = seeded(&[&url]).await;
    let reading = Arc::clone(&state);
    let fetch = Canned::new()
        .robots(ORIGIN, "User-agent: *\nCrawl-delay: 0.01\nAllow: /\n")
        .html(&url, &page("A", &[]));
    let crawler = crawler(fetch, state);

    crawler.tick(&Collected::default()).await.expect("tick");
    let host = host_of(&reading, &url).await;
    assert_eq!(host.crawl_delay_ms, Some(100));
    // The floor is below our own starting delay, so the published number
    // changes nothing here, which is the point of taking the larger of the
    // two rather than the published one.
    assert_eq!(
        host.delay(),
        Duration::from_millis(u64::from(HostRow::INITIAL_DELAY_MS))
    );
}

#[tokio::test]
async fn the_file_a_host_served_is_described_in_its_host_record() {
    // Doc 08.3 puts a `RobotsRef` on the host record, which is the digest and
    // the two times rather than the rules, and nothing wrote it. The robots
    // cache is memory only, so a coordinator that restarted started with
    // nothing and refetched robots.txt for every host it touched, on hosts
    // that mostly had not changed their file. Doc 07.4 gives the file a
    // twenty four hour lifetime precisely so that does not happen, and at a
    // few thousand hosts an hour it is a few thousand requests nobody needed,
    // sent to origins that get nothing out of them.
    //
    // The digest is the other half. Without it a refetch cannot tell a file
    // that changed from one that did not, so doc 07.7's rule about a
    // `Disallow` that appears later has nothing to fire on. It is over the
    // body rather than over the rules on purpose: two files that parse to the
    // same rules for our user agent can differ everywhere else.
    const BODY: &str = "User-agent: *\nAllow: /\nSitemap: https://example.com/s.xml\n";
    let url = format!("{ORIGIN}/a");
    let state = seeded(&[&url]).await;
    let reading = Arc::clone(&state);
    let fetch = Canned::new()
        .robots(ORIGIN, BODY)
        .html(&url, &page("A", &[]));
    let crawler = crawler(fetch, state);

    crawler.tick(&Collected::default()).await.expect("tick");
    let host = host_of(&reading, &url).await;
    let robots = host
        .robots
        .expect("the fetch that read the file wrote it down");
    assert_eq!(
        robots.digest,
        umi_types::Digest::from_bytes(*blake3::hash(BODY.as_bytes()).as_bytes())
    );
    assert_eq!(robots.fetched_ms, T0);
    assert_eq!(robots.expires_ms, T0 + crate::robots::TTL_MS);
    assert!(robots.authoritative, "a 200 is an answer");
    // Doc 13.6 reads the `Sitemap` lines during seeding and then dropped them,
    // so a later crawl of the same host had no record that the site had told
    // us where its sitemap is.
    assert_eq!(host.sitemaps, vec!["https://example.com/s.xml".to_owned()]);
}

#[tokio::test]
async fn a_server_error_is_written_down_as_an_answer_we_did_not_get() {
    // The flag exists for exactly this. A 5xx and a 404 both leave us with no
    // rules and they are not the same thing: RFC 9309 2.3.1.3 says a site with
    // no robots.txt has published no restrictions, which is an answer, and
    // 2.3.1.4 says a 5xx disallows the host for a day, which is the absence of
    // one. A coordinator deciding on restart whether it knows this host's
    // rules has to be able to tell them apart.
    //
    // The reference is still written, because the fact being recorded is that
    // we asked and when. Skipping the write on a failure is how a host whose
    // robots.txt is down gets asked for it again every time the coordinator
    // comes back up, which is the origin that can least afford it.
    let url = format!("{ORIGIN}/a");
    let state = seeded(&[&url]).await;
    let reading = Arc::clone(&state);
    let fetch = Canned::new()
        .outcome(
            &format!("{ORIGIN}/robots.txt"),
            umi_fetch::Outcome::Failed {
                failure: umi_fetch::Failure::ServerError,
                status: Some(503),
                retry_after: None,
            },
        )
        .html(&url, &page("A", &[]));
    let crawler = crawler(fetch, state);

    crawler.tick(&Collected::default()).await.expect("tick");
    let host = host_of(&reading, &url).await;
    let robots = host.robots.expect("we asked, so that is worth recording");
    assert!(!robots.authoritative, "a 503 is not an answer");
    assert_eq!(robots.expires_ms, T0 + crate::robots::TTL_MS);
    // Nothing was parsed, so nothing the site published is overwritten. A 5xx
    // that cleared a delay the site had asked for would mean the day the file
    // came back the site got crawled at our pace instead of its own.
    assert_eq!(host.crawl_delay_ms, None);
    assert!(host.sitemaps.is_empty());
}

#[tokio::test]
async fn a_file_with_no_crawl_delay_leaves_the_host_on_our_own_pace() {
    // The common case, and the one that has to stay cheap. The one lease that
    // fetched the file writes the host record and no other lease on that host
    // reads it, so a healthy crawl does not pay a host lookup per page in
    // order to discover that the file said nothing.
    let url = format!("{ORIGIN}/a");
    let state = seeded(&[&url]).await;
    let reading = Arc::clone(&state);
    let fetch = Canned::new()
        .robots(ORIGIN, "User-agent: *\nAllow: /\n")
        .html(&url, &page("A", &[]));
    let crawler = crawler(fetch, state);

    crawler.tick(&Collected::default()).await.expect("tick");
    let host = host_of(&reading, &url).await;
    assert_eq!(host.crawl_delay_ms, None);
}
