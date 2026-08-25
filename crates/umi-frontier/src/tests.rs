//! The scheduler against the reference store.
//!
//! Two of these are issue 10's stated bar: a host with `Crawl-delay: 10` takes
//! the time it should, and the per pay level domain cap holds when a site has
//! many subdomains. The rest are the cases that made those two pass for the
//! wrong reason at some point while this was being written.
//!
//! Time is a variable here rather than a clock. Every test runs the scheduler
//! over milliseconds it makes up, which is the whole reason nothing in this
//! crate or in [`umi_state`] reads the time itself, and it is why a test for a
//! ten second crawl delay runs in microseconds.

use std::collections::BTreeSet;

use umi_state::{
    Discovery, FetchOutcome, FetchResult, HostRow, Lease, MemoryState, Revalidator, State,
};
use umi_types::{HostId, PldId, RowKey, Tier};

use crate::{Ask, Config, Frontier, MAX_DEPTH, Rate, depth_score};

/// Where the clock starts. A real one, because zero is not a time any crawl
/// runs at and the state layer treats a fetch at the epoch as one that has not
/// happened. Everything below is an offset from here, so the assertions read
/// as durations.
const T0: u64 = 1_760_000_000_000;

fn frontier(config: Config) -> Frontier<MemoryState> {
    Frontier::new(MemoryState::new(), config)
}

fn pld_of(url: &str) -> PldId {
    RowKey::for_url(url, None).expect("test url").pld
}

fn host_of(url: &str) -> HostId {
    RowKey::for_url(url, None).expect("test url").host
}

/// Apply a plain success, so the URL is not offered again.
async fn complete(state: &MemoryState, leases: &[Lease], now_ms: u64) {
    let outcomes: Vec<FetchOutcome> = leases
        .iter()
        .map(|lease| FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: now_ms,
            tier_used: Tier::Plain,
            result: FetchResult::Fetched {
                status: 200,
                content_hash: [1u8; 8],
                revalidate: Revalidator::default(),
            },
        })
        .collect();
    state.complete(&outcomes).await.expect("complete");
}

#[tokio::test]
async fn a_host_with_a_ten_second_crawl_delay_is_fetched_every_ten_seconds() {
    // Issue 10's first bar. Five URLs on one host that asked for ten seconds
    // between requests, crawled by a scheduler ticking every 100 ms, and the
    // question is how far apart the requests come out and how long the five
    // take altogether.
    let front = frontier(Config::default());
    let urls = [
        "https://slow.example.com/a",
        "https://slow.example.com/b",
        "https://slow.example.com/c",
        "https://slow.example.com/d",
        "https://slow.example.com/e",
    ];
    front.seed(&urls, T0).await.expect("seed");

    let key = RowKey::for_url(urls[0], None).expect("test url");
    front
        .state()
        .put_host(&[HostRow {
            crawl_delay_ms: Some(10_000),
            ..HostRow::new(key.host, key.pld)
        }])
        .await
        .expect("put host");

    let mut issued: Vec<u64> = Vec::new();
    let mut now = T0;
    while now <= T0 + 60_000 && issued.len() < urls.len() {
        let leases = front.tick(&Ask::new(now, 1)).await.expect("tick");
        for lease in &leases {
            assert_eq!(
                lease.not_before_ms, now,
                "a lease was handed out before it could be fetched"
            );
            issued.push(lease.not_before_ms - T0);
        }
        complete(front.state(), &leases, now).await;
        now += 100;
    }

    assert_eq!(issued.len(), urls.len(), "the crawl did not finish");
    assert_eq!(issued, vec![0, 10_000, 20_000, 30_000, 40_000]);
}

#[tokio::test]
async fn the_larger_of_the_crawl_delay_and_the_adaptive_delay_wins() {
    // Doc 09.3 is `max(crawl_delay, adaptive_delay)`, so a host that publishes
    // one second while we have decided it needs five gets five, and a host
    // that publishes ten while we think one second is fine still gets ten.
    for (crawl, adaptive, expected) in [(1_000u32, 5_000u32, 5_000u64), (10_000, 1_000, 10_000)] {
        let front = frontier(Config::default());
        let urls = ["https://h.example.com/a", "https://h.example.com/b"];
        front.seed(&urls, T0).await.expect("seed");
        let key = RowKey::for_url(urls[0], None).expect("test url");
        front
            .state()
            .put_host(&[HostRow {
                crawl_delay_ms: Some(crawl),
                adaptive_delay_ms: adaptive,
                ..HostRow::new(key.host, key.pld)
            }])
            .await
            .expect("put host");

        let leases = front.tick(&Ask::new(T0, 2)).await.expect("tick");
        assert_eq!(leases.len(), 2, "both should be offered, spaced apart");
        assert_eq!(leases[0].not_before_ms, T0);
        assert_eq!(leases[1].not_before_ms, T0 + expected);
    }
}

#[tokio::test]
async fn the_domain_cap_holds_when_a_site_has_many_subdomains() {
    // Issue 10's second bar, and the reason the cap is per pay level domain
    // rather than per host. Two hundred subdomains, each of which is polite on
    // its own terms, add up to two hundred requests a second to one operator
    // unless something above the host is counting.
    let front = frontier(Config::default());
    let urls: Vec<String> = (0..200)
        .map(|n| format!("https://n{n}.example.com/"))
        .collect();
    let links: Vec<&str> = urls.iter().map(String::as_str).collect();
    front.seed(&links, T0).await.expect("seed");

    // Every one of those is the same pay level domain, which is the thing the
    // cap counts. Without this the test would be measuring nothing.
    let plds: BTreeSet<PldId> = links.iter().map(|url| pld_of(url)).collect();
    assert_eq!(plds.len(), 1);
    assert_eq!(front.domains(), 1);

    let cap = usize::try_from(Rate::DEFAULT_PER_SECOND).expect("in range");
    let mut per_second = vec![0usize; 10];
    let mut now = T0;
    while now < T0 + 10_000 {
        let leases = front.tick(&Ask::new(now, 64)).await.expect("tick");
        per_second[usize::try_from((now - T0) / 1000).expect("in range")] += leases.len();
        complete(front.state(), &leases, now).await;
        now += 100;
    }

    let total: usize = per_second.iter().sum();
    // Ten seconds at twenty a second, plus the one burst a domain that has
    // never been fetched is allowed to open with.
    assert!(
        total <= 10 * cap + cap,
        "{total} requests in ten seconds against a cap of {cap} a second: {per_second:?}"
    );
    // And it is not throttling to nothing either, which is how this test would
    // pass for the wrong reason.
    assert!(total >= 9 * cap, "only {total} requests: {per_second:?}");
    // The worst one second window is the burst plus a second of refill, which
    // is what a bucket of twenty tokens refilling at twenty a second gives and
    // is the structure doc 09.3 asks for by name. Every second after that is
    // the rate, because the burst has been spent and does not come back while
    // the domain is being crawled.
    assert!(
        per_second[0] <= 2 * cap,
        "the opening second went over burst plus rate: {per_second:?}"
    );
    assert!(
        per_second[1..].iter().all(|n| *n <= cap),
        "a later second went over the rate: {per_second:?}"
    );
}

#[tokio::test]
async fn a_thousand_separate_domains_are_not_capped_against_each_other() {
    // The other half of the same rule. The cap is per domain, so a crawl
    // spread across many sites is limited by the fleet's own throughput and
    // not by anything in here.
    let front = frontier(Config {
        max_domains: 1024,
        ..Config::default()
    });
    let urls: Vec<String> = (0..1000).map(|n| format!("https://s{n}.test/")).collect();
    let links: Vec<&str> = urls.iter().map(String::as_str).collect();
    front.seed(&links, T0).await.expect("seed");
    assert_eq!(front.domains(), 1000);

    let leases = front.tick(&Ask::new(T0, 1000)).await.expect("tick");
    assert_eq!(leases.len(), 1000, "one from each of a thousand domains");
}

#[tokio::test]
async fn a_domain_that_takes_nothing_is_not_charged_for_it() {
    // The reason the gate charges after the fact. Every URL on this domain is
    // inside its host's politeness window, so the tick comes back empty, and
    // if that had cost the domain its budget the crawl would slow down in
    // proportion to how often it looked.
    let front = frontier(Config::default());
    let url = "https://q.example.org/a";
    front.seed(&[url], T0).await.expect("seed");
    let pld = pld_of(url);
    front
        .state()
        .put_host(&[HostRow {
            next_allowed_ms: T0 + 5_000,
            ..HostRow::new(host_of(url), pld)
        }])
        .await
        .expect("put host");

    for now in (T0..T0 + 5_000).step_by(100) {
        assert!(
            front
                .tick(&Ask::new(now, 8))
                .await
                .expect("tick")
                .is_empty()
        );
    }
    // The schedule was never advanced, so it is still where a domain that has
    // not been fetched starts.
    assert_eq!(front.next_ready_ms(pld), Some(0));
    let leases = front.tick(&Ask::new(T0 + 5_000, 8)).await.expect("tick");
    assert_eq!(leases.len(), 1);
}

#[tokio::test]
async fn the_shallow_pages_go_first() {
    // Doc 09.2's depth decay, seen from the outside: a frontier holding a seed
    // and something nine hops out offers the seed first.
    let front = frontier(Config::default());
    front
        .seed(&["https://d.example.net/"], T0)
        .await
        .expect("seed");
    front
        .discover(&["https://d.example.net/deep"], 9, T0, Discovery::Trusted)
        .await
        .expect("discover");

    let leases = front.tick(&Ask::new(T0, 1)).await.expect("tick");
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].url, "https://d.example.net/");
    assert_eq!(leases[0].depth, 0);
    assert_eq!(leases[0].priority, depth_score(0));
}

#[tokio::test]
async fn two_pages_at_the_same_depth_go_in_discovery_order() {
    // The discovery order half of issue 10's priority, which is not a term in
    // the score: it falls out of the due time tiebreak in `State::lease`,
    // because a URL is admitted due at the moment it was discovered.
    let front = frontier(Config::default());
    front
        .discover(
            &["https://o.example.net/second"],
            0,
            T0 + 2_000,
            Discovery::Trusted,
        )
        .await
        .expect("discover");
    front
        .discover(
            &["https://o.example.net/first"],
            0,
            T0 + 1_000,
            Discovery::Trusted,
        )
        .await
        .expect("discover");

    let leases = front.tick(&Ask::new(T0 + 10_000, 2)).await.expect("tick");
    let urls: Vec<&str> = leases.iter().map(|lease| lease.url.as_str()).collect();
    assert_eq!(
        urls,
        vec![
            "https://o.example.net/first",
            "https://o.example.net/second"
        ]
    );
    assert_eq!(leases[0].priority, leases[1].priority);
}

#[tokio::test]
async fn the_depth_cap_drops_a_link_without_remembering_it() {
    let front = frontier(Config::default());
    let report = front
        .discover(
            &["https://t.example.net/x"],
            MAX_DEPTH,
            T0,
            Discovery::Trusted,
        )
        .await
        .expect("discover");
    assert_eq!(report.too_deep, 1);
    assert_eq!(report.total(), 1);
    assert_eq!(report.admitted.total(), 0);
    assert!(front.tick(&Ask::new(T0, 8)).await.expect("tick").is_empty());

    // The same URL found on a shallower page is admitted normally, which is
    // what not remembering it buys.
    let report = front
        .discover(&["https://t.example.net/x"], 0, T0, Discovery::Trusted)
        .await
        .expect("discover");
    assert_eq!(report.admitted.admitted, 1);
}

#[tokio::test]
async fn a_link_that_is_not_http_never_reaches_the_store() {
    let front = frontier(Config::default());
    let report = front
        .discover(
            &[
                "mailto:someone@example.com",
                "javascript:void(0)",
                "ftp://files.example.com/x",
                "https://ok.example.com/",
            ],
            0,
            T0,
            Discovery::Trusted,
        )
        .await
        .expect("discover");
    assert_eq!(report.uncrawlable, 3);
    assert_eq!(report.admitted.admitted, 1);
    assert_eq!(report.total(), 4);
}

#[tokio::test]
async fn the_same_batch_twice_admits_once() {
    let front = frontier(Config::default());
    let links = ["https://i.example.com/a", "https://i.example.com/b"];
    let first = front.seed(&links, T0).await.expect("seed");
    let second = front.seed(&links, T0).await.expect("seed");
    assert_eq!(first.admitted.admitted, 2);
    assert_eq!(second.admitted.admitted, 0);
    assert_eq!(second.admitted.seen, 2);
}

#[tokio::test]
async fn the_same_run_twice_over_produces_the_same_crawl() {
    // Gate 1.2 in doc 16, at the scale one process can check: the scheduler is
    // a function of the store and the time it is given, so two identical runs
    // choose the same URLs in the same order at the same moments.
    async fn run() -> Vec<(String, u64)> {
        let front = frontier(Config::default());
        let urls: Vec<String> = (0..50)
            .map(|n| format!("https://r{}.example.com/p{n}", n % 7))
            .collect();
        let links: Vec<&str> = urls.iter().map(String::as_str).collect();
        front.seed(&links, T0).await.expect("seed");
        let mut out = Vec::new();
        for now in (T0..T0 + 3_000).step_by(100) {
            let leases = front.tick(&Ask::new(now, 16)).await.expect("tick");
            complete(front.state(), &leases, now).await;
            for lease in leases {
                out.push((lease.url, lease.not_before_ms - T0));
            }
        }
        out
    }
    let first = run().await;
    assert!(!first.is_empty());
    assert_eq!(first, run().await);
}

#[tokio::test]
async fn a_tick_never_hands_back_more_than_it_was_asked_for() {
    let front = frontier(Config::default());
    let urls: Vec<String> = (0..100).map(|n| format!("https://m{n}.example/")).collect();
    let links: Vec<&str> = urls.iter().map(String::as_str).collect();
    front.seed(&links, T0).await.expect("seed");
    for want in [0u32, 1, 7, 100] {
        let leases = front.tick(&Ask::new(T0, want)).await.expect("tick");
        assert!(
            leases.len() <= want as usize,
            "asked {want}, got {}",
            leases.len()
        );
    }
}

#[tokio::test]
async fn a_tick_visits_at_most_the_configured_number_of_domains() {
    let front = frontier(Config {
        max_domains: 3,
        ..Config::default()
    });
    let urls: Vec<String> = (0..20).map(|n| format!("https://v{n}.example/")).collect();
    let links: Vec<&str> = urls.iter().map(String::as_str).collect();
    front.seed(&links, T0).await.expect("seed");
    let leases = front.tick(&Ask::new(T0, 100)).await.expect("tick");
    let touched: BTreeSet<PldId> = leases.iter().map(|lease| lease.key.pld).collect();
    assert_eq!(touched.len(), 3);
}

#[tokio::test]
async fn a_domain_does_not_starve_behind_the_ones_that_sort_before_it() {
    // A tick that can only visit two domains out of six. The gate orders by
    // how far each domain's schedule has run, so a domain that was served last
    // tick sorts behind the ones that were not, and everybody gets a turn.
    // Ordering on the earliest permitted time instead would put all six at
    // zero and let the domain id decide, forever.
    let front = frontier(Config {
        max_domains: 2,
        max_per_host: 1,
        ..Config::default()
    });
    let urls: Vec<String> = (0..6)
        .flat_map(|d| (0..20).map(move |p| format!("https://w{d}.example/p{p}")))
        .collect();
    let links: Vec<&str> = urls.iter().map(String::as_str).collect();
    front.seed(&links, T0).await.expect("seed");

    let mut seen: BTreeSet<PldId> = BTreeSet::new();
    let mut now = T0;
    while now < T0 + 1_000 {
        let leases = front.tick(&Ask::new(now, 8)).await.expect("tick");
        for lease in &leases {
            seen.insert(lease.key.pld);
        }
        complete(front.state(), &leases, now).await;
        now += 100;
    }
    assert_eq!(seen.len(), 6, "some domain never got a turn");
}

#[tokio::test]
async fn an_evicted_domain_stops_being_scheduled() {
    let front = frontier(Config::default());
    let kept = "https://kept.example.com/";
    let going = "https://going.example.org/";
    front.seed(&[kept, going], T0).await.expect("seed");
    assert_eq!(front.domains(), 2);

    // Nothing may be in flight over an eviction, so take the one URL on that
    // domain and answer it first.
    let leases = front.tick(&Ask::new(T0, 8)).await.expect("tick");
    assert_eq!(leases.len(), 2);
    complete(front.state(), &leases, T0).await;

    // Through the frontier and not through the store, because the scheduler no
    // longer re-reads the resident set on every tick and so has to be told.
    let report = front.evict(&[pld_of(going)]).await.expect("evict");
    assert_eq!(report.evicted, 1);

    assert_eq!(front.domains(), 1);
    assert_eq!(front.next_ready_ms(pld_of(going)), None);
    assert!(front.next_ready_ms(pld_of(kept)).is_some());

    let leases = front.tick(&Ask::new(T0 + 1_000, 8)).await.expect("tick");
    assert!(
        leases.iter().all(|lease| lease.key.pld != pld_of(going)),
        "an evicted domain was leased from"
    );
}

#[tokio::test]
async fn a_restart_picks_the_schedule_back_up_from_the_resident_shards() {
    // Doc 09.8: the frontier is entirely in state, so a coordinator that comes
    // up with a store that has URLs in it schedules them without being seeded
    // again.
    let first = frontier(Config::default());
    let urls = ["https://a.example.com/1", "https://b.example.org/1"];
    first.seed(&urls, T0).await.expect("seed");

    let restarted = Frontier::new(first.into_state(), Config::default());
    assert_eq!(restarted.domains(), 0, "a fresh gate starts empty");

    let recovered = restarted.resume().await.expect("resume");
    assert_eq!(recovered, 2);
    let leases = restarted
        .tick(&Ask::new(T0 + 1_000, 8))
        .await
        .expect("tick");
    assert_eq!(leases.len(), 2);
}

#[tokio::test]
async fn an_unverified_link_is_held_rather_than_scheduled() {
    use umi_types::FetcherId;

    let front = frontier(Config::default());
    let report = front
        .discover(
            &["https://u.example.com/x"],
            0,
            T0,
            Discovery::Unverified(FetcherId::from_bytes([7u8; 32])),
        )
        .await
        .expect("discover");
    assert_eq!(report.admitted.held, 1);
    assert_eq!(report.admitted.admitted, 0);
    assert!(front.tick(&Ask::new(T0, 8)).await.expect("tick").is_empty());
}

#[tokio::test]
async fn a_blocked_host_is_never_leased() {
    let front = frontier(Config::default());
    let url = "https://blocked.example.com/x";
    front
        .state()
        .put_host(&[HostRow {
            blocked: true,
            ..HostRow::new(host_of(url), pld_of(url))
        }])
        .await
        .expect("put host");
    let report = front.seed(&[url], T0).await.expect("seed");
    assert_eq!(report.admitted.excluded, 1);
    assert!(front.tick(&Ask::new(T0, 8)).await.expect("tick").is_empty());
}
