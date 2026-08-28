//! The loop, driven by a fetcher made of canned answers.
//!
//! Nothing here opens a socket. A test that stood up an HTTP server to check
//! that a disallowed URL is never fetched would be slow, would be flaky on a
//! loaded build box, and would mostly be testing hyper. The interesting
//! question is whether the loop asks for robots.txt before it asks for the
//! page, and a fetcher that records what it was asked for answers that
//! directly.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use umi_fetch::outcome::{Page, Version};
use umi_fetch::{FetchError, Media, Outcome};
use umi_state::{Budget, Candidate, MemoryState, State};
use umi_types::{FetcherId, Revalidator, Tier};

use crate::clock::{Clock, FixedClock};
use crate::fetch::Fetch;
use crate::page::PageRow;
use crate::run::{CrawlConfig, CrawlError, Crawler, Sink, TickReport};
use crate::scope::Scope;

const T0: u64 = 1_760_000_000_000;

/// A fetcher that answers from a map and remembers what it was asked.
#[derive(Default)]
struct Canned {
    pages: HashMap<String, Outcome>,
    asked: Mutex<Vec<String>>,
}

impl Canned {
    fn new() -> Self {
        Self::default()
    }

    /// Serve `body` at `url` with a 200.
    fn html(mut self, url: &str, body: &str) -> Self {
        self.pages.insert(url.to_owned(), ok_page(url, body));
        self
    }

    /// Serve a robots.txt at an origin.
    fn robots(mut self, origin: &str, body: &str) -> Self {
        let url = format!("{origin}/robots.txt");
        self.pages.insert(url.clone(), ok_page(&url, body));
        self
    }

    /// Serve a specific outcome at a URL.
    fn outcome(mut self, url: &str, outcome: Outcome) -> Self {
        self.pages.insert(url.to_owned(), outcome);
        self
    }

    fn asked(&self) -> Vec<String> {
        self.asked.lock().expect("not poisoned").clone()
    }

    fn asked_for(&self, url: &str) -> bool {
        self.asked().iter().any(|seen| seen == url)
    }
}

#[async_trait::async_trait]
impl Fetch for Canned {
    async fn fetch(
        &self,
        url: &str,
        _revalidate: Option<&Revalidator>,
    ) -> Result<Outcome, FetchError> {
        self.asked
            .lock()
            .expect("not poisoned")
            .push(url.to_owned());
        Ok(self.pages.get(url).cloned().unwrap_or(Outcome::Failed {
            failure: umi_fetch::Failure::NotFound,
            status: Some(404),
            retry_after: None,
        }))
    }
}

/// The same fetcher, noting the clock as each request goes out.
///
/// [`Canned`] records the order of requests, which answers whether robots.txt
/// came first. It cannot answer whether two requests to one host were far
/// enough apart, because order says nothing about spacing, and spacing is the
/// whole of doc 07.6.
struct Timed {
    inner: Canned,
    clock: Arc<FixedClock>,
    sent: Mutex<Vec<(String, u64)>>,
}

impl Timed {
    /// Build the inner fetcher with `pages`, reading `clock` as it serves.
    fn new(clock: &Arc<FixedClock>, pages: impl FnOnce(Canned) -> Canned) -> Self {
        Self {
            inner: pages(Canned::new()),
            clock: Arc::clone(clock),
            sent: Mutex::new(Vec::new()),
        }
    }

    /// What went out and when, in the order it went out.
    fn sent(&self) -> Vec<(String, u64)> {
        self.sent.lock().expect("not poisoned").clone()
    }
}

#[async_trait::async_trait]
impl Fetch for Timed {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
    ) -> Result<Outcome, FetchError> {
        self.sent
            .lock()
            .expect("not poisoned")
            .push((url.to_owned(), self.clock.now_ms()));
        self.inner.fetch(url, revalidate).await
    }
}

fn ok_page(url: &str, body: &str) -> Outcome {
    let bytes = Bytes::from(body.as_bytes().to_vec());
    Outcome::Ok(Box::new(Page {
        final_url: url.to_owned(),
        status: 200,
        version: Version::Http2,
        redirects: Vec::new(),
        headers_kept: vec![("content-type".to_owned(), "text/html".to_owned())],
        headers_digest: [0u8; 32],
        content_type: Some("text/html; charset=utf-8".to_owned()),
        media: Media::Html,
        body_digest: *blake3::hash(&bytes).as_bytes(),
        body: bytes,
        revalidate: Revalidator::default(),
        elapsed: Duration::from_millis(40),
    }))
}

/// Rows in a vector, which is what a test wants and what `umi crawl --dry-run`
/// wants for a different reason.
#[derive(Default)]
struct Collected(Mutex<Vec<PageRow>>);

#[async_trait::async_trait]
impl Sink for Collected {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.0
            .lock()
            .expect("not poisoned")
            .extend(rows.iter().cloned());
        Ok(())
    }
}

impl Collected {
    fn rows(&self) -> Vec<PageRow> {
        self.0.lock().expect("not poisoned").clone()
    }
}

/// A sink that always refuses, to check what the loop does about it.
struct Broken;

#[async_trait::async_trait]
impl Sink for Broken {
    async fn take(&self, _rows: &[PageRow]) -> Result<(), CrawlError> {
        Err(CrawlError::Sink("disk is full".to_owned()))
    }
}

fn config() -> CrawlConfig {
    CrawlConfig {
        fetcher: FetcherId::LOCAL,
        batch: 64,
        in_flight: 8,
        max_per_host: 64,
        max_tier: Tier::Plain,
        lease_for: Duration::from_secs(60),
        max_depth: 4,
        scope: Arc::new(Scope::general()),
        budget: Budget::DEFAULT,
        rate: umi_frontier::Rate::default(),
        max_domains: 64,
    }
}

/// A state layer with `urls` already in the frontier.
async fn seeded(urls: &[&str]) -> Arc<dyn State> {
    let state = Arc::new(MemoryState::new());
    let batch: Vec<Candidate<'_>> = urls
        .iter()
        .map(|url| {
            let mut c = Candidate::new(url, T0).expect("a crawlable url");
            c.discovery = umi_state::Discovery::Seed;
            c
        })
        .collect();
    state.admit(&batch).await.expect("admit");
    state
}

fn crawler(fetch: Canned, state: Arc<dyn State>) -> Crawler<Arc<Canned>, Arc<FixedClock>> {
    with_scope(fetch, state, Scope::general())
}

/// The same crawler under a doc 13 scope.
fn with_scope(
    fetch: Canned,
    state: Arc<dyn State>,
    scope: Scope,
) -> Crawler<Arc<Canned>, Arc<FixedClock>> {
    Crawler::new(
        Arc::new(fetch),
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            scope: Arc::new(scope),
            ..config()
        },
    )
}

/// Tick until the frontier runs dry, moving the clock on between ticks.
///
/// A crawl of more than one page needs this and it is not a test artefact.
/// Doc 07.6 allows one request per host per adaptive delay, which starts at a
/// second, so a fixed clock leases the first URL on a host and then nothing
/// ever again. A daemon sleeps between ticks for the same reason; this is that
/// sleep, without the sleeping.
///
/// The step is two seconds because [`HostRow::INITIAL_DELAY_MS`] is one, and
/// the ceiling is there so a loop that never drains fails the test rather than
/// hanging the suite.
async fn drain<F: Fetch>(
    crawler: &Crawler<F, Arc<FixedClock>>,
    clock: &FixedClock,
    sink: &dyn Sink,
) -> TickReport {
    let mut total = TickReport::default();
    for _ in 0..64 {
        let report = crawler.tick(sink).await.expect("tick");
        if report.idle() {
            return total;
        }
        total.leased += report.leased;
        total.rows += report.rows;
        total.fetched += report.fetched;
        total.not_modified += report.not_modified;
        total.failed += report.failed;
        total.disallowed += report.disallowed;
        total.links_seen += report.links_seen;
        total.links_admitted += report.links_admitted;
        clock.advance(2000);
    }
    panic!("the crawl did not drain in 64 ticks: {total:?}");
}

fn page(title: &str, links: &[&str]) -> String {
    let mut out = format!(
        "<html lang='en'><head><title>{title}</title></head><body><h1>{title}</h1><p>Some prose about the subject, at a length that extracts to something.</p>"
    );
    for link in links {
        out.push_str(&format!("<p><a href='{link}'>a link</a></p>"));
    }
    out.push_str("</body></html>");
    out
}

#[tokio::test]
async fn a_tick_leases_fetches_and_completes() {
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", &page("A", &[]));
    let crawler = crawler(fetch, Arc::clone(&state));
    let sink = Collected::default();

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 1);
    assert_eq!(report.rows, 1);
    assert_eq!(report.fetched, 1);
    assert_eq!(report.failed, 0);

    let rows = sink.rows();
    assert_eq!(rows[0].url, "https://example.com/a");
    assert_eq!(rows[0].status, 200);
    assert_eq!(rows[0].outcome, umi_types::OutcomeCode::Ok);
    assert_eq!(rows[0].title.as_deref(), Some("A"));

    // The completion is durable before the tick returns, so a second tick has
    // nothing to do rather than handing the same URL out again.
    let again = crawler.tick(&sink).await.expect("tick");
    assert!(again.idle(), "the same url was leased twice: {again:?}");
}

#[tokio::test]
async fn robots_is_fetched_before_the_page_and_only_once_per_host() {
    let state = seeded(&[
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
    ])
    .await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", &page("A", &[]))
        .html("https://example.com/b", &page("B", &[]))
        .html("https://example.com/c", &page("C", &[]));
    let crawler = crawler(fetch, state);

    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.fetched, 3);

    // Three pages on one host is one robots.txt. A cache that let every task
    // discover the miss would send three, and at 250 pages a second on a big
    // site it would send hundreds.
    let robots = crawler
        .fetcher()
        .asked()
        .into_iter()
        .filter(|u| u.ends_with("/robots.txt"))
        .count();
    assert_eq!(robots, 1, "asked for robots.txt {robots} times");
}

#[tokio::test]
async fn a_batch_covering_one_host_is_spread_out_rather_than_sent_at_once() {
    // Doc 07.6. The state layer already staggers the leases of a batch by the
    // host's delay and reports each one's earliest send in `not_before_ms`, and
    // for a while the loop dropped that on the floor and pushed the whole batch
    // into the window at once. It is invisible from our side: the totals are
    // right, the rate averaged over a minute is right, and the origin is the
    // only party who sees four requests land together.
    //
    // Measured on a real site before the fix, `umi crawl blog.rust-lang.org
    // --rps 1` sent four requests in 138 ms.
    let urls = [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
        "https://example.com/d",
    ];
    let state = seeded(&urls).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Timed::new(&clock, |f| {
        let mut f = f.robots("https://example.com", "User-agent: *\nAllow: /\n");
        for url in urls {
            f = f.html(url, &page("P", &[]));
        }
        f
    }));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        state,
        Arc::clone(&clock),
        CrawlConfig {
            in_flight: 8,
            ..config()
        },
    );

    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.fetched, 4, "{report:?}");

    let sent: Vec<u64> = fetch
        .sent()
        .into_iter()
        .filter(|(url, _)| !url.ends_with("/robots.txt"))
        .map(|(_, at)| at)
        .collect();
    assert_eq!(sent.len(), 4, "{sent:?}");

    let delay = u64::from(umi_state::HostRow::INITIAL_DELAY_MS);
    for pair in sent.windows(2) {
        let gap = pair[1].saturating_sub(pair[0]);
        assert!(
            gap >= delay,
            "two requests to one host {gap} ms apart, {delay} ms asked for: {sent:?}"
        );
    }
}

#[tokio::test]
async fn forty_hosts_under_one_domain_are_still_one_domain() {
    // Doc 09.3's cap, and the reason the loop schedules through the frontier
    // rather than leasing from the store directly. Every host here is polite on
    // its own terms, one request in flight and a second between them, and forty
    // of them at once is still forty of our connections arriving at one
    // operator. The store cannot see that, because it hands out work per host.
    // The frontier can, because it counts per pay level domain.
    let urls: Vec<String> = (0..40)
        .map(|n| format!("https://h{n}.example.com/a"))
        .collect();
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let mut fetch = Canned::new();
    for n in 0..40 {
        fetch = fetch
            .robots(
                &format!("https://h{n}.example.com"),
                "User-agent: *\nAllow: /\n",
            )
            .html(&format!("https://h{n}.example.com/a"), &page("P", &[]));
    }
    let clock = Arc::new(FixedClock::at(T0));
    let crawler = Crawler::new(Arc::new(fetch), state, Arc::clone(&clock), config());

    // The cap is 20 a second and the burst is a second's worth, so the first
    // tick spends the lot and the second gets nothing until time moves.
    let first = crawler.tick(&Collected::default()).await.expect("tick");
    let cap = umi_frontier::Rate::DEFAULT_PER_SECOND as usize;
    assert_eq!(first.leased, cap, "{first:?}");

    let second = crawler.tick(&Collected::default()).await.expect("tick");
    assert!(
        second.idle(),
        "the domain was over its cap and got {} more urls anyway",
        second.leased
    );

    // A second later it may go again, and the twenty urls left are the twenty
    // it takes. Without the frontier on the path both of these ticks would have
    // handed out all forty at once.
    clock.advance(1000);
    let third = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(third.leased, urls.len() - cap, "{third:?}");
}

#[tokio::test]
async fn a_lease_that_is_ready_now_does_not_wait_behind_one_that_is_not() {
    // The other half. Honouring `not_before_ms` costs nothing if a slow host
    // can park the window while it waits, so the batch goes out earliest first
    // and a host with work ready keeps going while another host serves its
    // penalty. Two hosts, four urls each, one window: the fourth url of the
    // second host must not be waiting on the fourth url of the first.
    let mut urls = Vec::new();
    for host in ["a.example", "b.example"] {
        for n in 0..4 {
            urls.push(format!("https://{host}/p{n}"));
        }
    }
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Timed::new(&clock, |f| {
        let mut f = f
            .robots("https://a.example", "User-agent: *\nAllow: /\n")
            .robots("https://b.example", "User-agent: *\nAllow: /\n");
        for url in &urls {
            f = f.html(url, &page("P", &[]));
        }
        f
    }));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        state,
        Arc::clone(&clock),
        CrawlConfig {
            in_flight: 8,
            ..config()
        },
    );

    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.fetched, 8, "{report:?}");

    // Three delays covers the fourth request on either host. If the two hosts
    // were being served one after the other it would take seven.
    let delay = u64::from(umi_state::HostRow::INITIAL_DELAY_MS);
    let last = fetch
        .sent()
        .into_iter()
        .map(|(_, at)| at)
        .max()
        .expect("something was sent");
    assert_eq!(
        last - T0,
        3 * delay,
        "eight urls over two hosts took {} ms, and one host's worth is {} ms",
        last - T0,
        3 * delay
    );
}

#[tokio::test]
async fn a_disallowed_url_is_never_fetched() {
    // Doc 14.10: there is no flag that turns this off, so there had better be
    // a test that says it is on.
    let state = seeded(&["https://example.com/private/secret"]).await;
    let fetch = Canned::new()
        .robots(
            "https://example.com",
            "User-agent: *\nDisallow: /private/\n",
        )
        .html("https://example.com/private/secret", &page("Secret", &[]));
    let crawler = crawler(fetch, state);
    let sink = Collected::default();

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 1);
    assert_eq!(report.disallowed, 1);
    assert_eq!(report.rows, 0, "a disallowed url produced a row");
    assert!(
        !crawler
            .fetcher()
            .asked_for("https://example.com/private/secret"),
        "the page was fetched despite robots.txt: {:?}",
        crawler.fetcher().asked()
    );
    assert!(sink.rows().is_empty());
}

#[tokio::test]
async fn a_server_error_on_robots_disallows_the_whole_host() {
    // RFC 9309 section 2.3.1.4. A 5xx is not an empty file.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .outcome(
            "https://example.com/robots.txt",
            Outcome::Failed {
                failure: umi_fetch::Failure::ServerError,
                status: Some(503),
                retry_after: None,
            },
        )
        .html("https://example.com/a", &page("A", &[]));
    let crawler = crawler(fetch, state);

    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.disallowed, 1);
    assert!(!crawler.fetcher().asked_for("https://example.com/a"));
}

#[tokio::test]
async fn a_missing_robots_allows_the_host() {
    // RFC 9309 section 2.3.1.3, and the common case: most sites have no
    // robots.txt and a crawler that read a 404 as "disallow" would crawl
    // almost nothing.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new().html("https://example.com/a", &page("A", &[]));
    let crawler = crawler(fetch, state);

    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.disallowed, 0);
    assert_eq!(report.fetched, 1);
}

#[tokio::test]
async fn links_found_on_a_page_go_into_the_frontier() {
    let clock = Arc::new(FixedClock::at(T0));
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .robots("https://elsewhere.example", "User-agent: *\nAllow: /\n")
        .html(
            "https://example.com/a",
            &page("A", &["/b", "/c", "https://elsewhere.example/d"]),
        )
        .html("https://example.com/b", &page("B", &[]))
        .html("https://example.com/c", &page("C", &[]))
        .html("https://elsewhere.example/d", &page("D", &[]));
    let crawler = Crawler::new(Arc::new(fetch), state, Arc::clone(&clock), config());
    let sink = Collected::default();

    let first = crawler.tick(&sink).await.expect("tick");
    assert_eq!(first.links_seen, 3);
    assert_eq!(first.links_admitted, 3);

    // And they are actually leasable, which is the part that would break if
    // the candidate keys did not match what the frontier stores. All three
    // come back, though not in one tick: two of them share a host and doc
    // 07.6 only allows one of those at a time.
    clock.advance(2000);
    let rest = drain(&crawler, &clock, &sink).await;
    assert_eq!(rest.leased, 3, "the admitted links were not handed back");
    assert_eq!(rest.fetched, 3);

    let mut urls: Vec<String> = sink.rows().into_iter().map(|r| r.url).collect();
    urls.sort();
    assert_eq!(
        urls,
        [
            "https://elsewhere.example/d",
            "https://example.com/a",
            "https://example.com/b",
            "https://example.com/c",
        ]
    );
}

#[tokio::test]
async fn a_nofollow_page_keeps_its_row_and_gives_up_its_links() {
    // Doc 11.4. The page is still indexed, the links are still not followed,
    // and confusing the two is how a crawler ends up ignoring a directive it
    // was told to respect.
    let state = seeded(&["https://example.com/a"]).await;
    let body = "<html><head><title>A</title><meta name='robots' content='nofollow'>\
                </head><body><p>Prose here.</p><a href='/b'>b</a></body></html>";
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", body);
    let crawler = crawler(fetch, state);
    let sink = Collected::default();

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.rows, 1);
    assert_eq!(report.links_seen, 0, "a nofollow page contributed links");
    assert_eq!(sink.rows()[0].title.as_deref(), Some("A"));
}

#[tokio::test]
async fn a_rel_nofollow_link_is_not_followed_but_is_still_in_the_row() {
    let state = seeded(&["https://example.com/a"]).await;
    let body = "<html><head><title>A</title></head><body><p>Prose.</p>\
                <a href='/good'>good</a>\
                <a href='/bad' rel='nofollow'>bad</a></body></html>";
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", body);
    let crawler = crawler(fetch, state);
    let sink = Collected::default();

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.links_seen, 1);

    // Doc 10.5's `links` column is what the page said, not what we chose to
    // do about it, so both links are in the row.
    let rows = sink.rows();
    assert_eq!(rows[0].links.len(), 2);
}

#[tokio::test]
async fn the_depth_ceiling_stops_the_crawl_going_deeper() {
    let state = seeded(&["https://example.com/0"]).await;
    let mut fetch = Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n");
    for n in 0..8 {
        let next = format!("/{}", n + 1);
        fetch = fetch.html(
            &format!("https://example.com/{n}"),
            &page(&format!("P{n}"), &[&next]),
        );
    }
    let clock = Arc::new(FixedClock::at(T0));
    let crawler = Crawler::new(Arc::new(fetch), state, Arc::clone(&clock), config());
    let sink = Collected::default();

    let total = drain(&crawler, &clock, &sink).await;

    // The seed is at depth 0 and `max_depth` is 4, so /0 through /4 are
    // fetched and /4 contributes nothing because it is at the ceiling. Eight
    // pages are on offer and five is where it has to stop.
    assert_eq!(total.fetched, 5, "crawled past the depth ceiling");
    let mut urls: Vec<String> = sink.rows().into_iter().map(|r| r.url).collect();
    urls.sort();
    assert_eq!(urls.last().expect("rows"), "https://example.com/4");
}

#[tokio::test]
async fn a_failed_fetch_still_produces_a_row() {
    // A 503 is data. Doc 10.5's `pages` stream is what happened to a URL, and
    // dropping the failures would leave a dataset that quietly overstates how
    // much of the web answers.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .outcome(
            "https://example.com/a",
            Outcome::Failed {
                failure: umi_fetch::Failure::ServerError,
                status: Some(503),
                retry_after: None,
            },
        );
    let crawler = crawler(fetch, state);
    let sink = Collected::default();

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.rows, 1);
    assert_eq!(report.failed, 1);
    assert_eq!(report.fetched, 0);

    let rows = sink.rows();
    assert_eq!(rows[0].status, 503);
    assert_eq!(rows[0].outcome, umi_types::OutcomeCode::ServerError);
    assert!(rows[0].markdown.is_none());
}

#[tokio::test]
async fn a_sink_that_fails_leaves_the_url_uncompleted() {
    // Gate 1.3's rule, as a test. Rows are stored before completions, so a
    // sink that refuses means the URL is still owed and will be handed out
    // again once the lease expires. The alternative loses the page.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", &page("A", &[]));
    let crawler = crawler(fetch, Arc::clone(&state));

    let failed = crawler.tick(&Broken).await;
    assert!(matches!(failed, Err(CrawlError::Sink(_))), "{failed:?}");

    // The lease is still out, so nothing is leasable until it expires. Once it
    // does, the URL comes back rather than being lost.
    let stats = state.stats().await.expect("stats");
    assert_eq!(
        stats.leases_in_flight, 1,
        "the lease was released after a sink error"
    );
    assert_eq!(stats.urls_fetched, 0, "the url was recorded as fetched");
}

#[tokio::test]
async fn an_empty_frontier_is_idle_and_not_an_error() {
    let state = seeded(&[]).await;
    let crawler = crawler(Canned::new(), state);
    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report, TickReport::default());
    assert!(report.idle());
}

#[tokio::test]
async fn two_runs_over_the_same_pages_produce_the_same_rows() {
    // Doc 11.1's determinism promise, at the level of the loop rather than of
    // one function. The clock is fixed, so the only thing that could differ is
    // the loop itself, and the interesting part is that the fetches complete
    // in whatever order the executor felt like and the rows still match.
    let rows_of = || async {
        let state = seeded(&["https://example.com/a", "https://example.com/b"]).await;
        let fetch = Canned::new()
            .robots("https://example.com", "User-agent: *\nAllow: /\n")
            .html("https://example.com/a", &page("A", &["/b"]))
            .html("https://example.com/b", &page("B", &["/a"]));
        let crawler = crawler(fetch, state);
        let sink = Collected::default();
        crawler.tick(&sink).await.expect("tick");
        let mut rows = sink.rows();
        rows.sort_by(|a, b| a.url.cmp(&b.url));
        rows
    };

    let first = rows_of().await;
    let second = rows_of().await;
    assert_eq!(first.len(), 2);
    for (a, b) in first.iter().zip(&second) {
        assert_eq!(a.url, b.url);
        assert_eq!(a.body_digest, b.body_digest);
        assert_eq!(a.extract_digest, b.extract_digest);
        assert_eq!(a.chunk_root, b.chunk_root);
        assert_eq!(a.text_digest, b.text_digest);
        assert_eq!(a.sketch.simhash, b.sketch.simhash);
    }
}

#[tokio::test]
async fn the_robots_cache_expires_after_a_day() {
    let clock = Arc::new(FixedClock::at(T0));
    let state = seeded(&["https://example.com/a", "https://example.com/b"]).await;
    let fetch = Arc::new(
        Canned::new()
            .robots("https://example.com", "User-agent: *\nAllow: /\n")
            .html("https://example.com/a", &page("A", &[]))
            .html("https://example.com/b", &page("B", &[])),
    );
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        state,
        Arc::clone(&clock),
        CrawlConfig {
            batch: 1,
            ..config()
        },
    );

    crawler.tick(&Collected::default()).await.expect("tick");
    let after_first = fetch
        .asked()
        .iter()
        .filter(|u| u.ends_with("/robots.txt"))
        .count();
    assert_eq!(after_first, 1);

    clock.advance(crate::robots::TTL_MS + 1);
    crawler.tick(&Collected::default()).await.expect("tick");
    let after_second = fetch
        .asked()
        .iter()
        .filter(|u| u.ends_with("/robots.txt"))
        .count();
    assert_eq!(after_second, 2, "an expired robots.txt was reused");
}

#[tokio::test]
async fn a_scope_keeps_the_crawl_on_its_own_site() {
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .robots("https://elsewhere.test", "User-agent: *\nAllow: /\n")
        .html(
            "https://example.com/a",
            &page("A", &["/b", "https://elsewhere.test/x"]),
        )
        .html("https://example.com/b", &page("B", &[]))
        .html("https://elsewhere.test/x", &page("X", &[]));

    let scope = Scope::for_target("example.com").expect("target");
    let crawler = with_scope(fetch, Arc::clone(&state), scope);
    let clock = Arc::clone(crawler.clock());
    let sink = Collected::default();
    let report = drain(&crawler, &clock, &sink).await;

    // Both links were seen, because the row records what the page said. One
    // was admitted, because only one is in scope.
    assert_eq!(report.links_seen, 2);
    assert_eq!(report.links_admitted, 1);
    assert_eq!(report.rows, 2);
    assert!(
        !crawler.fetcher().asked_for("https://elsewhere.test/x"),
        "the out of scope page was never fetched"
    );
    assert!(
        !crawler
            .fetcher()
            .asked_for("https://elsewhere.test/robots.txt"),
        "and neither was its robots.txt, which is a request we did not have to make"
    );
}

#[tokio::test]
async fn one_hop_fetches_the_page_it_cites_and_stops_there() {
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .robots("https://elsewhere.test", "User-agent: *\nAllow: /\n")
        .html(
            "https://example.com/a",
            &page("A", &["https://elsewhere.test/x"]),
        )
        .html(
            "https://elsewhere.test/x",
            &page("X", &["https://further.test/y"]),
        );

    let scope = Scope {
        link_policy: crate::scope::LinkPolicy::OneHop,
        ..Scope::for_target("example.com").expect("target")
    };
    let crawler = with_scope(fetch, Arc::clone(&state), scope);
    let clock = Arc::clone(crawler.clock());
    let sink = Collected::default();
    let report = drain(&crawler, &clock, &sink).await;

    assert_eq!(report.rows, 2, "the cited page is fetched");
    assert!(crawler.fetcher().asked_for("https://elsewhere.test/x"));
    assert!(
        !crawler.fetcher().asked_for("https://further.test/y"),
        "and the second hop is not taken"
    );
}

#[tokio::test]
async fn a_scope_max_depth_lowers_the_ceiling_and_cannot_raise_it() {
    let scope = Scope {
        max_depth: Some(1),
        ..Scope::general()
    };
    let config = CrawlConfig {
        scope: Arc::new(scope),
        ..config()
    };
    assert_eq!(config.depth_limit(), 1);

    let greedy = CrawlConfig {
        scope: Arc::new(Scope {
            max_depth: Some(200),
            ..Scope::general()
        }),
        ..config
    };
    assert_eq!(greedy.depth_limit(), 4, "the process ceiling wins");
}

#[tokio::test]
async fn a_filtered_content_type_costs_the_fetch_and_produces_no_row() {
    let state = seeded(&["https://example.com/a", "https://example.com/paper.pdf"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", &page("A", &[]))
        .outcome("https://example.com/paper.pdf", {
            let bytes = Bytes::from_static(b"%PDF-1.7 not really a pdf");
            Outcome::Ok(Box::new(Page {
                final_url: "https://example.com/paper.pdf".to_owned(),
                status: 200,
                version: Version::Http2,
                redirects: Vec::new(),
                headers_kept: Vec::new(),
                headers_digest: [0u8; 32],
                content_type: Some("application/pdf".to_owned()),
                media: Media::Pdf,
                body_digest: *blake3::hash(&bytes).as_bytes(),
                body: bytes,
                revalidate: Revalidator::default(),
                elapsed: Duration::from_millis(40),
            }))
        });

    let scope = Scope {
        content: crate::scope::ContentFilter {
            content_types: vec!["text/html".to_owned()],
            ..crate::scope::ContentFilter::default()
        },
        ..Scope::for_target("example.com").expect("target")
    };
    let crawler = with_scope(fetch, Arc::clone(&state), scope);
    let clock = Arc::clone(crawler.clock());
    let sink = Collected::default();
    let report = drain(&crawler, &clock, &sink).await;

    assert_eq!(report.leased, 2, "both were leased and both were fetched");
    assert!(crawler.fetcher().asked_for("https://example.com/paper.pdf"));
    assert_eq!(report.rows, 1, "only the html got a row");
    assert_eq!(sink.rows()[0].url, "https://example.com/a");
}

#[tokio::test]
async fn a_filtered_language_produces_no_row_either() {
    let state = seeded(&["https://example.com/de"]).await;
    let body = "<html lang='de'><head><title>Seite</title></head><body><h1>Seite</h1>\
                <p>Ein Absatz mit genug Text, damit die Extraktion etwas findet.</p></body></html>";
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/de", body);

    let scope = Scope {
        content: crate::scope::ContentFilter {
            languages: vec!["en".to_owned()],
            ..crate::scope::ContentFilter::default()
        },
        ..Scope::for_target("example.com").expect("target")
    };
    let crawler = with_scope(fetch, Arc::clone(&state), scope);
    let sink = Collected::default();
    let report = crawler.tick(&sink).await.expect("tick");

    assert_eq!(report.leased, 1);
    assert_eq!(report.rows, 0);
    assert!(sink.rows().is_empty());
}

#[tokio::test]
async fn rows_are_stamped_with_the_scope_that_admitted_them() {
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", &page("A", &[]));

    let scope = Scope::for_target("example.com").expect("target");
    let id = scope.id;
    assert_ne!(id, 0);
    let crawler = with_scope(fetch, Arc::clone(&state), scope);
    let sink = Collected::default();
    crawler.tick(&sink).await.expect("tick");

    assert_eq!(sink.rows()[0].crawl_profile, id);
}

#[tokio::test]
async fn what_the_origin_said_about_its_rate_reaches_the_scheduler() {
    // The end to end half of doc 07.6. A 429 has no body and so no row, and
    // the `Retry-After` on it is the only thing the origin told us, so if the
    // loop drops it on the way to `complete` then nothing else can honour it.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .outcome(
            "https://example.com/a",
            Outcome::Failed {
                failure: umi_fetch::Failure::RateLimited,
                status: Some(429),
                retry_after: Some(umi_fetch::RetryAfter::After(600)),
            },
        );
    let crawler = crawler(fetch, Arc::clone(&state));

    let report = crawler.tick(&Collected::default()).await.expect("tick");
    assert_eq!(report.failed, 1);

    let host = umi_types::RowKey::for_url("https://example.com/a", None)
        .expect("a crawlable url")
        .host;
    let row = state
        .host(host)
        .await
        .expect("host")
        .expect("the host was fetched, so it has a record");
    assert_eq!(
        row.next_allowed_ms,
        T0 + 600_000,
        "ten minutes was asked for and something else was honoured"
    );
    assert_eq!(row.adaptive_delay_ms, 4000, "a 429 is doc 07.6's 4.0 rung");
    assert_eq!(row.consecutive_failures, 1);
}

#[tokio::test]
async fn a_page_that_came_back_quickly_counts_towards_the_fast_floor() {
    // The other direction, and the one that is easy to leave unwired: the
    // fetcher measured 40 ms and the host row has to hear about it, or no host
    // ever earns doc 07.6's 200 ms floor and the crawl is capped at one page
    // per host per second forever.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", &page("A", &[]));
    let crawler = crawler(fetch, Arc::clone(&state));

    crawler.tick(&Collected::default()).await.expect("tick");

    let host = umi_types::RowKey::for_url("https://example.com/a", None)
        .expect("a crawlable url")
        .host;
    let row = state.host(host).await.expect("host").expect("a record");
    assert_eq!(row.fetches, 1);
    assert_eq!(row.failures, 0);
    assert_eq!(row.fast_streak, 1, "a 40 ms answer is a fast one");
    assert_eq!(
        row.adaptive_delay_ms,
        umi_state::HostRow::DEFAULT_FLOOR_MS,
        "one fast answer does not earn the lower floor"
    );
}
