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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use umi_fetch::outcome::{Page, Version};
use umi_fetch::{FetchError, Media, Outcome, Served};
use umi_state::{Budget, Candidate, MemoryState, State};
use umi_types::{FetcherId, Revalidator, Tier};

use crate::clock::{Clock, FixedClock};
use crate::fetch::Fetch;
use crate::page::PageRow;
use crate::render::RenderPolicy;
use crate::run::{CrawlConfig, CrawlError, Crawler, Sink, TickReport};
use crate::scope::Scope;

pub(crate) const T0: u64 = 1_760_000_000_000;

/// How far a test moves the clock between two ticks.
///
/// Two seconds, because [`umi_state::HostRow::INITIAL_DELAY_MS`] is one and a
/// host that owes a second of politeness leases nothing until the clock has
/// passed it. A daemon sleeps for the same reason; this is that sleep, without
/// the sleeping.
const TICK_STEP_MS: u64 = 2000;

/// When the first page of a crawl is fetched.
///
/// A host's first lease fetches its robots.txt and then waits out one
/// politeness delay before it sends the page, so the page a test is about goes
/// out [`umi_state::HostRow::INITIAL_DELAY_MS`] after [`T0`] and every
/// timestamp it produces is measured from here.
const T1: u64 = T0 + umi_state::HostRow::INITIAL_DELAY_MS as u64;

/// Long enough that a url is due again whatever doc 09 made of its history.
///
/// The refresh interval grows as a page is seen not to change, and a test
/// about revalidation is a test about a page that does not change, so stepping
/// the clock by the initial interval works twice and then quietly stops
/// leasing anything. This is [`umi_state::MAX_REFRESH`] plus a day.
const PAST_MAX_REFRESH: u64 = 181 * 24 * 60 * 60 * 1000;

/// A fetcher that answers from a map and remembers what it was asked.
#[derive(Default)]
pub(crate) struct Canned {
    pages: HashMap<String, Outcome>,
    asked: Mutex<Vec<String>>,
    /// What this fetcher claims its browser pool can do, in pages a second.
    /// `None` is a fetcher with no browser, which is every test but the ones
    /// about doc 05.9's budget.
    render: Option<f64>,
}

impl Canned {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Serve `body` at `url` with a 200.
    pub(crate) fn html(mut self, url: &str, body: &str) -> Self {
        self.pages.insert(url.to_owned(), ok_page(url, body));
        self
    }

    /// Serve a robots.txt at an origin.
    pub(crate) fn robots(mut self, origin: &str, body: &str) -> Self {
        let url = format!("{origin}/robots.txt");
        self.pages.insert(url.clone(), ok_page(&url, body));
        self
    }

    /// Claim a browser pool of `rate` pages a second, for doc 05.9's budget.
    pub(crate) const fn renders(mut self, rate: f64) -> Self {
        self.render = Some(rate);
        self
    }

    /// Serve a specific outcome at a URL.
    pub(crate) fn outcome(mut self, url: &str, outcome: Outcome) -> Self {
        self.pages.insert(url.to_owned(), outcome);
        self
    }

    pub(crate) fn asked(&self) -> Vec<String> {
        self.asked.lock().expect("not poisoned").clone()
    }

    pub(crate) fn asked_for(&self, url: &str) -> bool {
        self.asked().iter().any(|seen| seen == url)
    }
}

#[async_trait::async_trait]
impl Fetch for Canned {
    async fn fetch(
        &self,
        url: &str,
        _revalidate: Option<&Revalidator>,
        tier: umi_types::Tier,
    ) -> Result<Served, FetchError> {
        self.asked
            .lock()
            .expect("not poisoned")
            .push(url.to_owned());
        // Every canned answer comes off the rung it was asked for, since there
        // is no ladder here to move between them. The tests that care about a
        // path that moved build it themselves.
        let outcome = self.pages.get(url).cloned().unwrap_or(Outcome::Failed {
            failure: umi_fetch::Failure::NotFound,
            status: Some(404),
            retry_after: None,
        });
        Ok(Served::at(tier, outcome))
    }

    fn render_capacity(&self) -> Option<f64> {
        self.render
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
        tier: umi_types::Tier,
    ) -> Result<Served, FetchError> {
        self.sent
            .lock()
            .expect("not poisoned")
            .push((url.to_owned(), self.clock.now_ms()));
        self.inner.fetch(url, revalidate, tier).await
    }
}

/// A fetcher that takes real time over every request.
///
/// Real time and not the fake clock, which is the only unusual thing about it
/// and is the point. Everything else in this file wants a clock it controls so
/// that a politeness delay costs nothing to test. A window's occupancy is the
/// one measurement that cannot be taken that way: it is lease time over tick
/// time, both read from `Instant`, and a fetcher that answers in the same poll
/// it was asked leaves a tick with no time in it to divide by.
struct Slow {
    inner: Canned,
    per_request: Duration,
}

#[async_trait::async_trait]
impl Fetch for Slow {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: umi_types::Tier,
    ) -> Result<Served, FetchError> {
        tokio::time::sleep(self.per_request).await;
        self.inner.fetch(url, revalidate, tier).await
    }
}

/// A fetcher that blocks one URL until it is told to stop.
///
/// Doc 05.8's de-escalation cannot be written against a map of canned answers,
/// because the whole of it is an origin that behaved one way and then behaved
/// another. This serves a block for `url` until [`Flip::relent`] is called and
/// leaves everything else, robots.txt included, to the inner fetcher.
struct Flip {
    inner: Canned,
    url: String,
    blocking: std::sync::atomic::AtomicBool,
}

impl Flip {
    fn new(url: &str, inner: Canned) -> Self {
        Self {
            inner,
            url: url.to_owned(),
            blocking: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Serve the real page from now on.
    fn relent(&self) {
        self.blocking
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl Fetch for Flip {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: umi_types::Tier,
    ) -> Result<Served, FetchError> {
        if url == self.url && self.blocking.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(Served::at(tier, blocked()));
        }
        self.inner.fetch(url, revalidate, tier).await
    }
}

/// A fetcher with no browser, answering leases that wanted one.
///
/// Doc 05.4 says a missing rung is served from the rung below rather than
/// being an error, and doc 04.5 says the answer carries the rungs it took.
/// Everything a lease asks for above T1 comes back off T1 here, with the path
/// saying so, which is the shape a build without the `render` feature has on
/// every host doc 05.8 has escalated.
struct NoBrowser(Canned);

#[async_trait::async_trait]
impl Fetch for NoBrowser {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: umi_types::Tier,
    ) -> Result<Served, FetchError> {
        let served = self.0.fetch(url, revalidate, tier).await?;
        Ok(Served::descended(tier, Tier::Plain, served.outcome))
    }

    fn render_capacity(&self) -> Option<f64> {
        self.0.render_capacity()
    }
}

/// An origin with an opinion about conditional requests.
///
/// Doc 05.3 is three behaviours that look identical from the fetcher's side
/// and differ only in what a second request would have returned, so a map of
/// canned answers cannot express any of them. This serves one url according to
/// [`Mode`] and records whether each request for it carried a validator, which
/// is the other half of what the tests need to see.
struct Cache {
    inner: Canned,
    url: String,
    mode: Mode,
    body: String,
    conditional: Mutex<Vec<bool>>,
}

/// How an origin answers a request that carries a validator.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// 304, which is the whole point of T0.
    Honest,
    /// The full body again, unchanged. Doc 05.3's first trap.
    Weak,
    /// 304, while an unconditional request gets a body that has moved on.
    /// Doc 05.3's second trap.
    Lying,
}

impl Cache {
    fn new(url: &str, mode: Mode, body: &str, inner: Canned) -> Self {
        Self {
            inner,
            url: url.to_owned(),
            mode,
            body: body.to_owned(),
            conditional: Mutex::new(Vec::new()),
        }
    }

    /// Whether each request for the watched url carried a validator, in order.
    fn conditional(&self) -> Vec<bool> {
        self.conditional.lock().expect("not poisoned").clone()
    }
}

#[async_trait::async_trait]
impl Fetch for Cache {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: umi_types::Tier,
    ) -> Result<Served, FetchError> {
        if url != self.url {
            return self.inner.fetch(url, revalidate, tier).await;
        }
        let sent = revalidate.is_some_and(|r| !r.is_empty());
        let first = {
            let mut log = self.conditional.lock().expect("not poisoned");
            let first = log.is_empty();
            log.push(sent);
            first
        };
        Ok(Served::at(
            tier,
            match (self.mode, sent) {
                (Mode::Honest | Mode::Lying, true) => not_modified(),
                // A lying origin has to serve the real page once before it can lie
                // about it, because the lie is a claim that the page has not
                // changed since the copy we hold.
                (Mode::Lying, false) if !first => tagged(url, &page("Moved on", &[])),
                _ => tagged(url, &self.body),
            },
        ))
    }
}

/// What doc 05.8 calls a block signal, as the fetcher reports one.
fn blocked() -> Outcome {
    Outcome::Failed {
        failure: umi_fetch::Failure::Blocked,
        status: Some(403),
        retry_after: None,
    }
}

/// A challenge page: a 200, a vendor marker and almost no text.
fn interstitial() -> String {
    "<html><head><title>Just a moment...</title></head><body>\
     <div id='cf-browser-verification'></div>\
     <p>Checking your browser before accessing example.com.</p>\
     </body></html>"
        .to_owned()
}

pub(crate) fn ok_page(url: &str, body: &str) -> Outcome {
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

/// A 200 carrying a `Content-Usage` response header, which is the half of
/// AIPREF that robots.txt does not carry.
fn with_usage(url: &str, body: &str, usage: &str) -> Outcome {
    let Outcome::Ok(mut page) = ok_page(url, body) else {
        unreachable!("ok_page returns Ok")
    };
    page.headers_kept
        .push(("content-usage".to_owned(), usage.to_owned()));
    Outcome::Ok(page)
}

/// A 200 carrying an `ETag`, which is what a page has to send before there is
/// anything for T0 to revalidate against.
fn tagged(url: &str, body: &str) -> Outcome {
    let Outcome::Ok(mut page) = ok_page(url, body) else {
        unreachable!("ok_page returns Ok")
    };
    page.revalidate = Revalidator {
        etag: Some("\"v1\"".to_owned()),
        last_modified_ms: Some(T0),
    };
    Outcome::Ok(page)
}

/// A 304, as an origin that honours a validator answers one.
fn not_modified() -> Outcome {
    Outcome::NotModified {
        revalidate: Revalidator {
            etag: Some("\"v1\"".to_owned()),
            last_modified_ms: Some(T0),
        },
        headers_kept: vec![("cache-control".to_owned(), "max-age=60".to_owned())],
        headers_digest: [7u8; 32],
        elapsed: Duration::from_millis(9),
    }
}

/// Rows in a vector, which is what a test wants and what `umi crawl --dry-run`
/// wants for a different reason.
#[derive(Default)]
pub(crate) struct Collected(Mutex<Vec<PageRow>>);

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
    pub(crate) fn rows(&self) -> Vec<PageRow> {
        self.0.lock().expect("not poisoned").clone()
    }
}

/// A sink that takes real time over every batch.
///
/// The counterpart to [`Slow`] and for the same reason. The loop hands a full
/// window to the store and goes back to harvesting, and a sink that answers in
/// the poll it was called in leaves nothing to overlap with the fetches.
struct Sleepy {
    inner: Collected,
    per_batch: Duration,
    /// How many times the loop handed it a batch, for the tests that care
    /// about the shape of the write path and not just its cost.
    batches: AtomicUsize,
}

impl Sleepy {
    fn new(per_batch: Duration) -> Self {
        Self {
            inner: Collected::default(),
            per_batch,
            batches: AtomicUsize::new(0),
        }
    }

    fn batches(&self) -> usize {
        self.batches.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Sink for Sleepy {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.batches.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.per_batch).await;
        self.inner.take(rows).await
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

pub(crate) fn config() -> CrawlConfig {
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
        // Off by default in tests. The audit fires a second request and a
        // canned fetcher has no way to tell the two apart, so the tests that
        // want it turn it on themselves.
        audit_every: 0,
        render: RenderPolicy::default(),
    }
}

/// A state layer with `urls` already in the frontier.
pub(crate) async fn seeded(urls: &[&str]) -> Arc<dyn State> {
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

/// A store that takes real time over an ask and no time over anything else.
///
/// The one call the loop makes that is a scan rather than a write, and the
/// reason it has its own decorator is that it is the only call whose cost the
/// loop is meant to hide. Everything else here delegates, which is why there is
/// so much of it: the trait has eighteen methods and a wrapper that implemented
/// four would not be a store.
struct SlowLease {
    inner: MemoryState,
    per_ask: Duration,
}

#[async_trait::async_trait]
impl State for SlowLease {
    async fn lease(
        &self,
        req: &umi_state::LeaseRequest<'_>,
    ) -> umi_state::Result<Vec<umi_state::Lease>> {
        tokio::time::sleep(self.per_ask).await;
        self.inner.lease(req).await
    }

    async fn admit(&self, batch: &[Candidate<'_>]) -> umi_state::Result<umi_state::AdmitReport> {
        self.inner.admit(batch).await
    }

    async fn complete(&self, outcomes: &[umi_state::FetchOutcome]) -> umi_state::Result<()> {
        self.inner.complete(outcomes).await
    }

    async fn release(
        &self,
        lease_ids: &[umi_state::LeaseId],
        reason: umi_state::NackReason,
    ) -> umi_state::Result<()> {
        self.inner.release(lease_ids, reason).await
    }

    async fn host(&self, id: umi_types::HostId) -> umi_state::Result<Option<umi_state::HostRow>> {
        self.inner.host(id).await
    }

    async fn put_host(&self, rows: &[umi_state::HostRow]) -> umi_state::Result<()> {
        self.inner.put_host(rows).await
    }

    async fn block(
        &self,
        rows: &[umi_state::BlockRow],
    ) -> umi_state::Result<umi_state::BlockReport> {
        self.inner.block(rows).await
    }

    async fn blocks(&self) -> umi_state::Result<Vec<umi_state::BlockRow>> {
        self.inner.blocks().await
    }

    async fn supervise(&self, rows: &[umi_state::SupervisionRow]) -> umi_state::Result<usize> {
        self.inner.supervise(rows).await
    }

    async fn supervision(&self) -> umi_state::Result<Vec<umi_state::SupervisionRow>> {
        self.inner.supervision().await
    }

    async fn put_segment(&self, rows: &[umi_state::SegmentRow]) -> umi_state::Result<()> {
        self.inner.put_segment(rows).await
    }

    async fn segment(
        &self,
        id: umi_types::Ulid,
    ) -> umi_state::Result<Option<umi_state::SegmentRow>> {
        self.inner.segment(id).await
    }

    async fn segments(
        &self,
        query: umi_state::SegmentQuery,
    ) -> umi_state::Result<Vec<umi_state::SegmentRow>> {
        self.inner.segments(query).await
    }

    async fn warm(&self, plds: &[umi_types::PldId]) -> umi_state::Result<()> {
        self.inner.warm(plds).await
    }

    async fn evict(&self, plds: &[umi_types::PldId]) -> umi_state::Result<umi_state::EvictReport> {
        self.inner.evict(plds).await
    }

    async fn resident(&self) -> umi_state::Result<Vec<umi_types::PldId>> {
        self.inner.resident().await
    }

    async fn checkpoint(&self, now_ms: u64) -> umi_state::Result<umi_state::Checkpoint> {
        self.inner.checkpoint(now_ms).await
    }

    async fn stats(&self) -> umi_state::Result<umi_state::StateStats> {
        self.inner.stats().await
    }
}

pub(crate) fn crawler(
    fetch: Canned,
    state: Arc<dyn State>,
) -> Crawler<Arc<Canned>, Arc<FixedClock>> {
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
/// The step is [`TICK_STEP_MS`], and the ceiling is there so a loop that never
/// drains fails the test rather than hanging the suite.
async fn drain<F: Fetch + 'static, S: Sink + 'static>(
    crawler: &Crawler<F, Arc<FixedClock>>,
    clock: &FixedClock,
    sink: &Arc<S>,
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
        total.challenged += report.challenged;
        total.learned += report.learned;
        clock.advance(TICK_STEP_MS);
    }
    panic!("the crawl did not drain in 64 ticks: {total:?}");
}

pub(crate) fn page(title: &str, links: &[&str]) -> String {
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
    let sink = Arc::new(Collected::default());

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

    let first = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(first.fetched, 3);

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
async fn the_lease_that_fetches_robots_goes_on_to_fetch_its_page() {
    // The lease used to stop at the file and put its url back for the next
    // tick to offer, on the reasoning that this costs a tick per host per day.
    // It costs far more than that. The frontier visits `max_domains` domains a
    // tick, so a host that gives its lease back is not offered again until the
    // rotation comes round, and on a broad crawl that is most of the frontier.
    //
    // Measured on server3 against a seed of twenty thousand distinct hosts:
    // twelve minutes of crawling fetched 2,074 robots.txt files and not one
    // page, because every lease in every tick was the first lease its host had
    // ever had. Doc 16's gate 3.1 wants 250 pages a second out of that same
    // box.
    let url = "https://example.com/a";
    let state = seeded(&[url]).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Timed::new(&clock, |f| {
        f.robots("https://example.com", "User-agent: *\nAllow: /\n")
            .html(url, &page("A", &[]))
    }));
    let crawler = Crawler::new(Arc::clone(&fetch), state, Arc::clone(&clock), config());
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 1, "{report:?}");
    assert_eq!(report.fetched, 1, "the page waited for another tick");
    assert_eq!(report.deferred, 0, "{report:?}");
    assert_eq!(sink.rows().len(), 1);

    // And the two requests are a delay apart, because doc 07.6 counts
    // robots.txt as a request to the same host. One lease, two requests, and
    // the origin sees exactly what it would have seen across two ticks.
    let mut sent = fetch.sent();
    sent.sort_by_key(|(_, at)| *at);
    let at: Vec<u64> = sent.iter().map(|(_, at)| *at).collect();
    assert!(sent[0].0.ends_with("/robots.txt"), "{sent:?}");
    assert_eq!(sent[1].0, url, "{sent:?}");
    assert_eq!(
        at,
        vec![T0, T0 + u64::from(umi_state::HostRow::INITIAL_DELAY_MS)],
        "{sent:?}"
    );
}

#[tokio::test]
async fn robots_is_asked_for_when_the_lease_arrives_and_not_when_it_is_dispatched() {
    // The point of the prefetch. Measured on server3 at a window of 1024
    // against the real seed, a page cost 4309 ms of which 3597 ms was
    // robots.txt, so five sixths of every slot in the window was a slot not
    // fetching a page. A lease waits in the queue for about as long as the
    // window takes to drain, and that wait is free runway for the file.
    let urls: Vec<String> = (0..8).map(|n| format!("https://w{n}.example/a")).collect();
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let mut fetch = Canned::new();
    for (n, url) in urls.iter().enumerate() {
        fetch = fetch
            .robots(
                &format!("https://w{n}.example"),
                "User-agent: *\nAllow: /\n",
            )
            .html(url, &page("A", &[]));
    }
    let crawler = crawler(fetch, state);

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.fetched, urls.len(), "{report:?}");
    // One per host, and every one of them off the window rather than out of it.
    assert_eq!(report.robots_warmed, urls.len(), "{report:?}");

    // And the second tick warms nothing, because the cache holds all eight and
    // a host we have the file for is a host the prefetch skips. This is the
    // half that keeps the prefetch from being a second request per page.
    let next = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(next.robots_warmed, 0, "{next:?}");
}

#[tokio::test]
async fn a_delay_the_prefetch_read_still_reaches_the_host_record() {
    // The one thing the prefetch could quietly break. Doc 07.4 puts a published
    // `Crawl-delay` in the host record, and the record used to learn it from
    // the lease that fetched the file. Once a prefetch is doing the fetching no
    // lease ever has that flag set, so a lease that finds it was spaced for
    // less than the file asks for writes the number down itself.
    //
    // It goes back to the frontier at the same time and for the same reason it
    // always did: this lease was spaced for one second and the site asked for
    // two, so the request it was about to make is the request the file was
    // written to stop.
    let url = "https://slow.example/a";
    let state = seeded(&[url]).await;
    let fetch = Canned::new()
        .robots("https://slow.example", "User-agent: *\nCrawl-delay: 2\n")
        .html(url, &page("A", &[]));
    let crawler = crawler(fetch, Arc::clone(&state));

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.fetched, 0, "the page went out anyway: {report:?}");
    assert_eq!(report.deferred, 1, "{report:?}");

    let key = umi_types::RowKey::for_url(url, None).expect("a crawlable url");
    let host = state.host(key.host).await.expect("host").expect("a record");
    assert_eq!(
        host.crawl_delay_ms,
        Some(2000),
        "the record never learned what the file asked for"
    );
}

#[tokio::test]
async fn a_tick_asks_the_scheduler_again_rather_than_letting_its_window_drain() {
    // The gate 3.1 shape. A tick used to take its whole batch in one ask, put
    // as much of it on the wire as the window allowed, and top the window up
    // only from what was left of that one ask. So the window emptied out at the
    // end of every tick and the rate was the batch over the slowest lease in
    // it, which on the twenty thousand host probe on server3 was 3.8 pages a
    // second. Now the tick asks again as the window drains and the rate is the
    // window over the mean.
    //
    // Twenty four urls on twenty four domains, and a scheduler allowed to visit
    // eight domains per ask. One url per domain, so an ask is worth eight
    // leases and no more, and twenty four fetched in one tick means the tick
    // asked three times. Before this it fetched eight and returned.
    let urls: Vec<String> = (0..24)
        .map(|n| format!("https://site{n}.example{n}.com/a"))
        .collect();
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Timed::new(&clock, |f| {
        let mut f = f;
        for n in 0..24 {
            f = f
                .robots(
                    &format!("https://site{n}.example{n}.com"),
                    "User-agent: *\nAllow: /\n",
                )
                .html(
                    &format!("https://site{n}.example{n}.com/a"),
                    &page("P", &[]),
                );
        }
        f
    }));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        state,
        Arc::clone(&clock),
        CrawlConfig {
            max_domains: 8,
            in_flight: 8,
            batch: 64,
            ..config()
        },
    );

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.leased, urls.len(), "{report:?}");
    assert_eq!(report.fetched, urls.len(), "{report:?}");

    // Two requests per url, because the first lease on a host fetches its
    // robots.txt before it fetches its page, and nothing was fetched twice.
    let sent = fetch.sent();
    assert_eq!(sent.len(), urls.len() * 2, "{sent:?}");
    for url in &urls {
        assert_eq!(
            sent.iter().filter(|(sent, _)| sent == url).count(),
            1,
            "{url}"
        );
    }
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

    let first = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(first.fetched, 4, "{first:?}");

    // robots.txt is in the list rather than filtered out of it. Doc 07.6
    // counts that file as a request to the same origin as the page, so five
    // requests went to this host and the spacing has to hold across all five.
    // Filtered out, this test passed while the lease that fetched the file
    // sent its page in the same millisecond as the next lease's.
    let mut sent: Vec<u64> = fetch.sent().into_iter().map(|(_, at)| at).collect();
    sent.sort_unstable();
    assert_eq!(sent.len(), 5, "{sent:?}");

    let delay = u64::from(umi_state::HostRow::INITIAL_DELAY_MS);
    for pair in sent.windows(2) {
        let gap = pair[1].saturating_sub(pair[0]);
        assert!(
            gap >= delay,
            "two requests to one host {gap} ms apart, {delay} ms asked for: {sent:?}"
        );
    }
    // And no wider than that, which is the half nothing was checking. Politeness
    // has an obvious safe direction and it is not free: the tick is as long as
    // its slowest host, so a host whose slots drift apart is a tick that takes
    // longer for no benefit to anybody. Five requests one delay apart is four
    // delays end to end, and the bug this catches turned that into twelve by
    // having each waiting lease take itself to the back of the queue every time
    // another lease on the same host claimed a slot.
    let span = sent[sent.len() - 1] - sent[0];
    assert_eq!(
        span,
        4 * delay,
        "five requests one {delay} ms delay apart should span {} ms: {sent:?}",
        4 * delay
    );
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
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Timed::new(&clock, |f| {
        let mut f = f;
        for n in 0..40 {
            f = f
                .robots(
                    &format!("https://h{n}.example.com"),
                    "User-agent: *\nAllow: /\n",
                )
                .html(&format!("https://h{n}.example.com/a"), &page("P", &[]));
        }
        f
    }));
    let crawler = Crawler::new(Arc::clone(&fetch), state, Arc::clone(&clock), config());

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.leased, urls.len(), "{report:?}");

    // The cap is 20 a second and the burst is a second's worth, so the forty
    // leases go out over two seconds however wide the window is. A tick refills
    // its window from the frontier as the window drains, so all forty land
    // inside one tick and what there is to check is when they went out rather
    // than how many ticks it took. Without the frontier on the path all forty
    // would have gone out at once.
    let cap = umi_frontier::Rate::DEFAULT_PER_SECOND as usize;
    let mut started: Vec<u64> = fetch
        .sent()
        .into_iter()
        .filter(|(url, _)| url.ends_with("/robots.txt"))
        .map(|(_, at)| at)
        .collect();
    started.sort_unstable();
    assert_eq!(started.len(), urls.len(), "{started:?}");
    for (n, at) in started.iter().enumerate() {
        let window = &started[n.saturating_sub(cap)..=n];
        let span = at.saturating_sub(window[0]);
        assert!(
            window.len() <= cap || span >= 1000,
            "{} requests to one domain inside {span} ms: {started:?}",
            window.len()
        );
    }

    let next = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert!(
        next.idle(),
        "the frontier was empty and handed out {} anyway",
        next.leased
    );
}

#[tokio::test]
async fn a_tick_reports_how_full_its_fetch_window_actually_was() {
    // Doc 14.3's progress line said "in flight" and printed the batch size,
    // which is the same number every tick and is not a measurement. The
    // question it looks like it is answering is the one that matters for gate
    // 3.1: a crawl configured for 256 that is running 30 is not fetching
    // slowly, it is not fetching, and nothing else on the line says so.
    //
    // Sixteen urls on sixteen hosts with room for four at a time, and a fetcher
    // that takes real time so that there is a window to be full of anything.
    // Four is the answer. The answer that must not come back is sixteen, which
    // is what counting the tick's fetch tasks gives: the loop takes one out and
    // puts one in on every pass, so that count is pinned to the window size and
    // reads full for a crawl that has stopped fetching.
    let (urls, canned) = a_page_each(16);
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let fetch = Slow {
        inner: canned,
        per_request: Duration::from_millis(20),
    };
    let crawler = Crawler::new(
        fetch,
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            in_flight: 4,
            ..config()
        },
    );

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.fetched, 16, "{report:?}");
    // Loosely, because the tick also has a store in it and the last four leases
    // are not replaced. The bug this catches is off by a factor of four, so a
    // band this wide still catches it.
    let mean = report.window_mean();
    assert!(
        (2.0..=4.5).contains(&mean),
        "a window of four averaged {mean} over {} leases in {} ms",
        report.leased,
        report.elapsed_ms
    );
}

/// Sixteen hosts with one page each, and the canned robots.txt to reach them.
fn a_page_each(count: usize) -> (Vec<String>, Canned) {
    let urls: Vec<String> = (0..count)
        .map(|n| format!("https://h{n}.example/p"))
        .collect();
    let fetch = urls.iter().fold(Canned::new(), |f, url| {
        let origin = url.trim_end_matches("/p");
        f.robots(origin, "User-agent: *\nAllow: /\n")
            .html(url, &page("P", &[]))
    });
    (urls, fetch)
}

#[tokio::test]
async fn the_loop_keeps_fetching_while_the_last_window_is_being_written() {
    // Doc 16's gate 3.1 is a rate on one box, and on server3 twenty seconds of
    // a hundred and ten second tick were the loop task inside the store rather
    // than putting the next lease on the wire. Every socket in the window goes
    // quiet for that whole time, which is both a slower crawl and a rude one.
    //
    // Thirty two urls on thirty two hosts, a window of four, and both ends of
    // the loop given real time to spend: ten milliseconds a request and twenty
    // a batch. Eight windows, so eight writes, and seven of them have a window
    // of fetching to hide behind. Only the last has nothing left to overlap
    // with, so the loop waits for one write out of eight.
    let (urls, canned) = a_page_each(32);
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let fetch = Slow {
        inner: canned,
        per_request: Duration::from_millis(10),
    };
    let crawler = Crawler::new(
        fetch,
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            in_flight: 4,
            ..config()
        },
    );

    let sink = Arc::new(Sleepy::new(Duration::from_millis(20)));
    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.rows, 32, "{report:?}");
    assert_eq!(sink.inner.rows().len(), 32, "{report:?}");
    assert!(
        report.store_ms >= 100,
        "eight writes of twenty milliseconds took {} ms, so the sink was not \
         the slow part and this test proves nothing",
        report.store_ms
    );
    // Three and not eight, because the seven that overlap do not overlap
    // perfectly and a loaded machine widens every one of them. The failure
    // this catches is the loop waiting for all eight, and that is a factor of
    // eight away from here.
    assert!(
        report.store_waited_ms * 3 < report.store_ms,
        "the loop waited {} ms of a {} ms write path, so the store is still on it",
        report.store_waited_ms,
        report.store_ms
    );
    // The sink is the only thing in this write path with any time in it, so
    // the split has to put the time there and the whole has to cover the split.
    // A split that is not a split of anything is worse than no split at all,
    // because it is the number the next change gets chosen off.
    assert!(
        report.rows_ms >= 100,
        "the sink slept for {} ms of a {} ms write path",
        report.rows_ms,
        report.store_ms
    );
    assert!(
        report.rows_ms + report.complete_ms + report.admit_ms <= report.store_ms,
        "the parts came to more than the whole: {report:?}"
    );
}

#[tokio::test]
async fn a_store_that_falls_behind_gets_a_wider_batch_rather_than_the_loop() {
    // The store running beside the loop is only half of it. The other half is
    // what happens when the store is slower than a window of fetching, which on
    // server3 it was for most of a tick: 184 seconds of a 195 second tick spent
    // at the barrier, with the window sitting at 97 of its 256 slots because
    // the loop was not harvesting and not leasing.
    //
    // Sixty four urls through a window of four, five milliseconds a request and
    // a hundred a batch, so the sink is twenty times slower than the fetching
    // it is meant to hide behind. Sixteen windows. The loop that waits for each
    // one writes sixteen times and spends the tick doing it. The loop that
    // carries on writes five, because `SLACK` lets a batch reach four windows
    // and then stops it.
    let (urls, canned) = a_page_each(64);
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let fetch = Slow {
        inner: canned,
        per_request: Duration::from_millis(5),
    };
    let crawler = Crawler::new(
        fetch,
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            in_flight: 4,
            ..config()
        },
    );

    let sink = Arc::new(Sleepy::new(Duration::from_millis(100)));
    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.rows, 64, "{report:?}");
    assert_eq!(sink.inner.rows().len(), 64, "{report:?}");
    // Eight and not five, because a batch is handed over on the harvest that
    // fills it and the loop does not get to choose when that lands. The failure
    // this catches is sixteen, and that is a factor of two away from here.
    assert!(
        sink.batches() <= 8,
        "the loop wrote {} batches of sixty four rows through a window of \
         four, so it is still waiting for the store between windows",
        sink.batches()
    );
    // The other side of it, and the reason `SLACK` exists. A loop that never
    // waits holds the whole tick in memory and hands the sink one batch, which
    // is the shape storing per window was there to avoid.
    assert!(
        sink.batches() >= 4,
        "sixty four rows in {} batches means a batch grew past the four \
         windows SLACK allows it",
        sink.batches()
    );
}

#[tokio::test]
async fn the_loop_keeps_fetching_while_the_next_ask_is_on_its_way() {
    // The ask is a scan of the store and it is what is left on the loop task
    // after #177. On server3 it was thirty seconds of a sixty second tick, and
    // it grew with the frontier: nine seconds at ten thousand rows and thirty
    // at eight hundred thousand, against a gate 3.1 frontier of five hundred
    // million. Every one of those seconds is the window draining with nothing
    // to refill it.
    //
    // A hundred and twenty eight urls through a window of sixteen, so eight
    // asks, each taking thirty milliseconds against a window that takes forty
    // to work through. Seven of the eight have a full queue to hide behind.
    // The first cannot: a tick with nothing in hand has to wait for its first
    // ask, and that is the one this asserts around.
    let (urls, canned) = a_page_each(128);
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state: Arc<dyn State> = Arc::new(SlowLease {
        inner: MemoryState::new(),
        per_ask: Duration::from_millis(30),
    });
    let batch: Vec<Candidate<'_>> = refs
        .iter()
        .map(|url| {
            let mut c = Candidate::new(url, T0).expect("a crawlable url");
            c.discovery = umi_state::Discovery::Seed;
            c
        })
        .collect();
    state.admit(&batch).await.expect("admit");

    let fetch = Slow {
        inner: canned,
        per_request: Duration::from_millis(20),
    };
    let crawler = Crawler::new(
        fetch,
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            batch: 128,
            in_flight: 16,
            ..config()
        },
    );

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.fetched, 128, "{report:?}");
    assert!(
        report.asks >= 8,
        "a window of sixteen over a batch of a hundred and twenty eight is eight \
         asks and this made {}",
        report.asks
    );
    assert!(
        report.ask_ms >= 150,
        "eight asks of thirty milliseconds took {} ms, so the store was not the \
         slow part and this test proves nothing",
        report.ask_ms
    );
    // Three and not eight, because the seven that overlap do not overlap
    // perfectly and a loaded machine widens every one of them. The failure
    // this catches is the loop waiting for all eight.
    assert!(
        report.ask_waited_ms * 3 < report.ask_ms,
        "the loop waited {} ms of {} ms of asking, so the ask is still on it",
        report.ask_waited_ms,
        report.ask_ms
    );
}

#[tokio::test]
async fn the_running_count_agrees_with_the_report_the_tick_ends_with() {
    // The counters a caller reads mid tick and the report it gets at the end
    // are two accounts of the same work, and the whole value of the first one
    // is that it can be believed. So they have to agree once the tick is over.
    //
    // Sixteen pages through a window of four, so the count is read after four
    // separate stores rather than one, and the last of them is the one on the
    // loop's own task.
    let (urls, canned) = a_page_each(16);
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let crawler = Crawler::new(
        canned,
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            in_flight: 4,
            ..config()
        },
    );
    let live = crawler.live();
    assert_eq!(live.rows(), 0, "nothing has been fetched yet");

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.rows, 16, "{report:?}");
    assert_eq!(live.rows(), 16, "{report:?}");
    assert_eq!(live.failed(), report.failed as u64, "{report:?}");
    assert_eq!(live.bytes_fetched(), report.bytes_fetched, "{report:?}");
    assert_eq!(
        live.in_flight(),
        0,
        "a tick that has returned has nothing on the wire"
    );
}

#[tokio::test]
async fn a_write_that_fails_off_the_loop_still_fails_the_tick() {
    // The store runs on a task of its own, and a task of its own is a place
    // for an error to go missing. A tick whose rows were not written must not
    // report them as stored, because the completions for that window are
    // written by the same call and a tick that swallowed this would be a tick
    // that marked pages crawled and threw them away.
    //
    // Sixteen urls against a window of four, so the failure happens in a
    // spawned write and not in the last one on the loop's own task.
    let (urls, _) = a_page_each(16);
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let (_, canned) = a_page_each(16);
    let crawler = Crawler::new(
        canned,
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            in_flight: 4,
            ..config()
        },
    );

    let failed = crawler.tick(&Arc::new(Broken)).await;
    assert!(matches!(failed, Err(CrawlError::Sink(_))), "{failed:?}");
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

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.fetched, 8, "{report:?}");
    assert_eq!(report.deferred, 0, "{report:?}");

    // Four delays and not three, because each host took five requests and not
    // four: its robots.txt went first and the page of the lease that fetched
    // it took the slot at the back. If the two hosts were being served one
    // after the other it would take nine.
    let delay = u64::from(umi_state::HostRow::INITIAL_DELAY_MS);
    let last = fetch
        .sent()
        .into_iter()
        .map(|(_, at)| at)
        .max()
        .expect("something was sent");
    assert_eq!(
        last - T0,
        4 * delay,
        "eight urls over two hosts took {} ms, and one host's worth is {} ms",
        last - T0,
        4 * delay
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
    let sink = Arc::new(Collected::default());

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
async fn aipref_reaches_the_row_from_both_sources_at_once() {
    // Doc 07.5. AIPREF can arrive in robots.txt, in a response header, or in
    // both, and a site using both is not a contradiction to resolve in the
    // page's favour. The vocab draft reconciles most restrictive wins, so the
    // header saying `train-ai=y` here does not undo the file saying no, and
    // the header's `search=n` is added because the file said nothing about it.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots(
            "https://example.com",
            "User-agent: *\nAllow: /\nContent-Usage: train-ai=n\n",
        )
        .outcome(
            "https://example.com/a",
            with_usage(
                "https://example.com/a",
                &page("A", &[]),
                "train-ai=y, search=n",
            ),
        );
    let crawler = crawler(fetch, state);
    let sink = Arc::new(Collected::default());

    crawler.tick(&sink).await.expect("tick");
    let rows = sink.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].content_usage.as_deref(),
        Some("train-ai=n, search=n")
    );
}

#[tokio::test]
async fn a_pattern_scoped_aipref_line_reaches_only_the_pages_it_names() {
    // The attach draft lets a line carry a path pattern, and the pattern uses
    // the same matcher as `Allow` and `Disallow`. A crawl that put every line
    // on every row would file the whole site under a preference the site made
    // about one directory.
    let state = seeded(&[
        "https://example.com/ai-ok/yes",
        "https://example.com/blog/no",
    ])
    .await;
    let fetch = Canned::new()
        .robots(
            "https://example.com",
            "User-agent: *\nAllow: /\nContent-Usage: /ai-ok/ train-ai=y\n",
        )
        .html("https://example.com/ai-ok/yes", &page("Yes", &[]))
        .html("https://example.com/blog/no", &page("No", &[]));
    let crawler = crawler(fetch, state);
    let sink = Arc::new(Collected::default());

    crawler.tick(&sink).await.expect("tick");
    let rows = sink.rows();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let want = if row.url.contains("/ai-ok/") {
            Some("train-ai=y")
        } else {
            None
        };
        assert_eq!(row.content_usage.as_deref(), want, "{}", row.url);
    }
}

#[tokio::test]
async fn an_unreadable_aipref_value_is_recorded_rather_than_dropped() {
    // AIPREF is two drafts and neither is an RFC. A directive from a later one
    // is worth more in the corpus verbatim than it is worth guessed at, and a
    // parser that dropped what it did not recognise would leave the reader who
    // does recognise it with nothing.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots(
            "https://example.com",
            "User-agent: *\nAllow: /\nContent-Usage: ai-input=n\n",
        )
        .html("https://example.com/a", &page("A", &[]));
    let crawler = crawler(fetch, state);
    let sink = Arc::new(Collected::default());

    crawler.tick(&sink).await.expect("tick");
    assert_eq!(
        sink.rows()[0].content_usage.as_deref(),
        Some("ai-input=n"),
        "an unknown directive was thrown away"
    );
}

#[tokio::test]
async fn a_missing_robots_allows_the_host() {
    // RFC 9309 section 2.3.1.3, and the common case: most sites have no
    // robots.txt and a crawler that read a 404 as "disallow" would crawl
    // almost nothing.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new().html("https://example.com/a", &page("A", &[]));
    let crawler = crawler(fetch, state);

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
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
    let sink = Arc::new(Collected::default());

    let first = crawler.tick(&sink).await.expect("tick");
    assert_eq!(first.links_seen, 3);
    assert_eq!(first.links_admitted, 3);

    // And they are actually leasable, which is the part that would break if
    // the candidate keys did not match what the frontier stores. All three
    // come back, though not in one tick: two of them share a host and doc
    // 07.6 only allows one of those at a time.
    clock.advance(TICK_STEP_MS);
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
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.rows, 1);
    assert_eq!(report.links_seen, 0, "a nofollow page contributed links");
    assert_eq!(sink.rows()[0].title.as_deref(), Some("A"));
}

#[tokio::test]
async fn a_rel_nofollow_link_is_followed_and_is_still_in_the_row() {
    // Doc 11.4: recorded, not obeyed. The two links here are treated
    // identically, and the only difference between them is the published
    // column, which consumers can weight on if they want to.
    let state = seeded(&["https://example.com/a"]).await;
    let body = "<html><head><title>A</title></head><body><p>Prose.</p>\
                <a href='/good'>good</a>\
                <a href='/spam' rel='nofollow'>spam</a></body></html>";
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", body);
    let crawler = crawler(fetch, state);
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.links_seen, 2);
    assert_eq!(report.links_admitted, 2);

    let rows = sink.rows();
    assert_eq!(rows[0].links.len(), 2);
}

#[tokio::test]
async fn the_head_puts_its_pages_in_the_frontier_and_keeps_its_assets_out() {
    // The head of a real page, cut down. Found by crawling excalidraw.com on
    // server3, which admitted two favicons, a stylesheet and a bundle, fetched
    // all four and stored a row for each.
    let state = seeded(&["https://example.com/a"]).await;
    let body = "<html><head><title>A</title>\
                <link rel='icon' href='/favicon-32x32.png'>\
                <link rel='stylesheet' href='/assets/index.css'>\
                <link rel='modulepreload' href='/assets/chunk.js'>\
                <link rel='manifest' href='/manifest.webmanifest'>\
                <link rel='next' href='/b'>\
                </head><body><p>Prose here, enough of it to extract.</p></body></html>";
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", body)
        .html("https://example.com/b", &page("B", &[]));
    let clock = Arc::new(FixedClock::at(T0));
    let crawler = Crawler::new(Arc::new(fetch), state, Arc::clone(&clock), config());
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.links_seen, 1, "an asset was offered to the frontier");
    assert_eq!(report.links_admitted, 1);

    // All five are still in doc 10.5's column, because that column is what the
    // page said and this decision does not change what the page said.
    assert_eq!(sink.rows()[0].links.len(), 5);

    // Past doc 07.6's delay, so the one admitted link is leasable.
    clock.advance(TICK_STEP_MS);
    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.fetched, 1);
    let urls: Vec<String> = sink.rows().into_iter().map(|r| r.url).collect();
    assert_eq!(urls, ["https://example.com/a", "https://example.com/b"]);
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
    let sink = Arc::new(Collected::default());

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
    let sink = Arc::new(Collected::default());

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

    let failed = crawler.tick(&Arc::new(Broken)).await;
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
    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(
        TickReport {
            // The one ask it took to find that out, which is the report saying
            // it went and looked rather than that it never went.
            asks: 0,
            asks_empty: 0,
            ..report
        },
        TickReport::default()
    );
    assert_eq!((report.asks, report.asks_empty), (1, 1));
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
        let sink = Arc::new(Collected::default());
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

    crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    let after_first = fetch
        .asked()
        .iter()
        .filter(|u| u.ends_with("/robots.txt"))
        .count();
    assert_eq!(after_first, 1);

    clock.advance(crate::robots::TTL_MS + 1);
    crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
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
    let sink = Arc::new(Collected::default());
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
    let sink = Arc::new(Collected::default());
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
    let sink = Arc::new(Collected::default());
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
    let sink = Arc::new(Collected::default());
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
    let sink = Arc::new(Collected::default());
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

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
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
        T1 + 600_000,
        "ten minutes was asked for and something else was honoured"
    );
    assert_eq!(row.adaptive_delay_ms, 4000, "a 429 is doc 07.6's 4.0 rung");
    assert_eq!(row.consecutive_failures, 1);
}

#[tokio::test]
async fn a_retry_after_moves_the_leases_the_tick_is_still_holding() {
    // Doc 07.6 folds the ask into the host record, which covers the next tick
    // and not this one. A batch is spaced when it is leased, so the two urls
    // behind the 429 were already scheduled a second apart before the origin
    // said six seconds, and a crawler that only wrote the number down would
    // send both of them inside the window it was asked to leave empty.
    //
    // Measured from the outside on gate 2.3's origin before this existed: a
    // 429 asking for six seconds was followed by another request 960 ms later,
    // out of the same batch.
    let state = seeded(&["https://example.com/warm"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/warm", &page("W", &[]))
        .outcome(
            "https://example.com/a",
            Outcome::Failed {
                failure: umi_fetch::Failure::RateLimited,
                status: Some(429),
                retry_after: Some(umi_fetch::RetryAfter::After(6)),
            },
        )
        .html("https://example.com/b", &page("B", &[]))
        .html("https://example.com/c", &page("C", &[]));
    let crawler = crawler(fetch, Arc::clone(&state));
    let sink = Arc::new(Collected::default());

    // A tick of its own to pay for the robots.txt, and the three under test
    // admitted after it, so that the tick under test leases all three as one
    // batch and the 429 is the first request in it. A tick that paid for
    // robots.txt as well would send the other two while the lease that owed
    // the file was still fetching it, and then there would be nothing left
    // waiting for the 429 to hold back.
    crawler.tick(&sink).await.expect("warm");
    let mut rest = Vec::new();
    for url in [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
    ] {
        let mut seed = Candidate::new(url, T0).expect("a crawlable url");
        seed.discovery = umi_state::Discovery::Seed;
        rest.push(seed);
    }
    state.admit(&rest).await.expect("admit");
    // Past the politeness window the warm tick left behind, or the host has
    // nothing due and the tick under test leases none of the three.
    crawler.clock().advance(TICK_STEP_MS);
    let began = crawler.clock().now_ms();

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 3, "the three were not leased together");
    assert_eq!(report.failed, 1);
    assert_eq!(report.fetched, 2);
    assert!(
        crawler.clock().now_ms() >= began + 6_000,
        "the six seconds the origin asked for were not left empty"
    );
}

#[tokio::test]
async fn a_lease_whose_turn_is_too_far_out_goes_back_rather_than_holding_its_slot() {
    // The same shape as the test above with a bigger number on it, and the
    // number is the point. Honouring a `Retry-After` exactly is right and
    // waiting it out inside a slot in the fetch window is not: `umi-fetch`
    // reads one up to a day, so on the open web a single 429 can park every
    // lease behind it for the rest of the run. Measured on server3 before this
    // existed: a crawl asked to stop sat at one lease in flight for over six
    // minutes on an idle box, which is a window of 256 doing the work of one.
    //
    // Nothing here disagrees with the origin. The two urls behind the 429 are
    // not sent, they keep their due time, and the ask that says the origin has
    // asked for ten minutes is written to the host record by the completion,
    // so the scheduler will not offer this host again until it may. What
    // changes is who waits: the state layer, which costs nothing, rather than
    // two sockets.
    let state = seeded(&["https://example.com/warm"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/warm", &page("W", &[]))
        .outcome(
            "https://example.com/a",
            Outcome::Failed {
                failure: umi_fetch::Failure::RateLimited,
                status: Some(429),
                retry_after: Some(umi_fetch::RetryAfter::After(600)),
            },
        )
        .html("https://example.com/b", &page("B", &[]))
        .html("https://example.com/c", &page("C", &[]));
    let crawler = crawler(fetch, Arc::clone(&state));
    let sink = Arc::new(Collected::default());

    crawler.tick(&sink).await.expect("warm");
    let mut rest = Vec::new();
    for url in [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
    ] {
        let mut seed = Candidate::new(url, T0).expect("a crawlable url");
        seed.discovery = umi_state::Discovery::Seed;
        rest.push(seed);
    }
    state.admit(&rest).await.expect("admit");
    crawler.clock().advance(TICK_STEP_MS);
    let began = crawler.clock().now_ms();

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 3, "the three were not leased together");
    assert_eq!(report.failed, 1, "{report:?}");
    assert_eq!(report.deferred, 2, "{report:?}");
    assert_eq!(report.fetched, 0, "nothing was sent inside the ten minutes");
    assert_eq!(report.completed(), 1, "{report:?}");
    assert!(
        crawler.clock().now_ms() < began + 600_000,
        "the tick waited out the whole ask instead of giving the leases back"
    );

    // And they are still there to fetch, which is what makes giving them back
    // different from dropping them.
    let counts = state.stats().await.expect("stats");
    assert_eq!(counts.urls_pending, 2, "{counts:?}");
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

    crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");

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

#[tokio::test]
async fn a_challenge_page_is_not_counted_as_a_fetch() {
    // Doc 05.8's first rule, and the one that is easy to get wrong quietly. A
    // challenge page is a 200 with a body on it, so every part of the loop
    // downstream of the fetcher is happy to treat it as a page, and a crawl
    // that stores a few million of them looks like a crawl that is working.
    let state = seeded(&["https://example.com/a"]).await;
    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html("https://example.com/a", &interstitial());
    let crawler = crawler(fetch, Arc::clone(&state));
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.challenged, 1);
    assert_eq!(report.fetched, 0, "a wall is not a page");
    assert_eq!(report.rows, 0);
    assert!(sink.rows().is_empty(), "a challenge page reached the sink");

    // And the host heard about it. One block is enough to move the ladder,
    // because waiting for a second one means fetching a second wall.
    assert_eq!(report.learned, 1);
    let host = umi_types::RowKey::for_url("https://example.com/a", None)
        .expect("a crawlable url")
        .host;
    let row = state.host(host).await.expect("host").expect("a record");
    assert_eq!(row.tier.preferred, Tier::Emulated);
    assert_eq!(row.tier.consecutive_blocks, 1);
}

#[tokio::test]
async fn an_origin_that_stops_blocking_is_probed_back_down() {
    // The other half of the ladder, and the half that costs money if it is
    // missing: an origin that blocked us once in a bad week is otherwise on
    // the expensive tier forever.
    let state = seeded(&["https://example.com/a"]).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Flip::new(
        "https://example.com/a",
        Canned::new()
            .robots("https://example.com", "User-agent: *\nAllow: /\n")
            .html("https://example.com/a", &page("A", &[])),
    ));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        Arc::clone(&state),
        Arc::clone(&clock),
        config(),
    );
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.failed, 1);
    assert_eq!(report.learned, 1);

    let host = umi_types::RowKey::for_url("https://example.com/a", None)
        .expect("a crawlable url")
        .host;
    let row = state.host(host).await.expect("host").expect("a record");
    assert_eq!(row.tier.preferred, Tier::Emulated);
    // From `T0` and not from [`T1`], because the backoff is measured off the
    // tick's own clock reading and a tick takes its reading once, at the top,
    // before any of its leases have waited for anything.
    assert_eq!(
        row.next_allowed_ms,
        T0 + umi_state::TierPolicy::BACKOFF_MS[0],
        "a block backs the host off, not just the url"
    );

    // This fetcher runs T1 and the host now wants T2, so there is nothing here
    // for it. That is the state a host would be stuck in forever if the probe
    // were a sweep over hosts nobody leases.
    clock.advance(2 * umi_state::TierPolicy::BACKOFF_MS[0]);
    let idle = crawler.tick(&sink).await.expect("tick");
    assert_eq!(idle.leased, 0, "a T2 host was offered to a T1 fetcher");

    // A week later the origin has stopped blocking and the probe finds out.
    fetch.relent();
    clock.advance(umi_state::TierPolicy::PROBE_EVERY_MS);
    // A week is well past the day robots.txt is good for, so the probe reads
    // the file again on its way out. That costs the lease a delay and not a
    // tick, and this host was blocked so the delay it owes is the ceiling
    // rather than the usual second.
    let probe = crawler.tick(&sink).await.expect("tick");
    assert_eq!(probe.leased, 1, "the weekly probe never came");
    assert_eq!(probe.fetched, 1);
    assert_eq!(probe.learned, 1);

    let row = state.host(host).await.expect("host").expect("a record");
    assert_eq!(row.tier.preferred, Tier::Plain, "the host stayed escalated");
    assert_eq!(row.tier.consecutive_blocks, 0);

    // Two rows and not one. An honest 403 is an answer and keeps its row, and
    // only the 200 that was really a wall is thrown away.
    let rows = sink.rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].outcome, umi_types::OutcomeCode::Blocked);
    assert_eq!(rows[1].outcome, umi_types::OutcomeCode::Ok);
}

#[tokio::test]
async fn a_learned_tier_outlives_the_process() {
    // Learning a host takes a blocked fetch to find out, so a ladder that is
    // rebuilt from nothing on every start is a ladder that is paid for again
    // every start. This is the sqlite backend rather than the memory one for
    // exactly that reason.
    let dir = tempfile::TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");
    let url = "https://example.com/a";
    let host = umi_types::RowKey::for_url(url, None)
        .expect("a crawlable url")
        .host;

    {
        let state: Arc<dyn State> =
            Arc::new(umi_state_sqlite::SqliteState::open(&path).expect("a new store"));
        let mut seed = Candidate::new(url, T0).expect("a crawlable url");
        seed.discovery = umi_state::Discovery::Seed;
        state.admit(&[seed]).await.expect("admit");

        let fetch = Canned::new()
            .robots("https://example.com", "User-agent: *\nAllow: /\n")
            .outcome(url, blocked());
        let crawler = crawler(fetch, Arc::clone(&state));
        let report = crawler
            .tick(&Arc::new(Collected::default()))
            .await
            .expect("tick");
        assert_eq!(report.learned, 1);
    }

    let state = umi_state_sqlite::SqliteState::open(&path).expect("the same store again");
    let row = state.host(host).await.expect("host").expect("a record");
    assert_eq!(row.tier.preferred, Tier::Emulated);
    assert_eq!(row.tier.max, umi_state::TierPolicy::CEILING);
    assert_eq!(row.tier.consecutive_blocks, 1);

    // Persisting the number is not the point. Acting on it is, and the store
    // is what acts on it: the same url is invisible to a T1 fetcher and there
    // for a T2 one, on a process that never saw the block.
    let later = T0 + 10 * umi_state::TierPolicy::BACKOFF_MS[0];
    let plain = umi_state::LeaseRequest::new(FetcherId::LOCAL, later, 4);
    assert!(
        state.lease(&plain).await.expect("lease").is_empty(),
        "an escalated host was offered to a T1 fetcher after a restart"
    );

    let browser = umi_state::LeaseRequest {
        max_tier: Tier::Emulated,
        ..plain
    };
    let leases = state.lease(&browser).await.expect("lease");
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].tier, Tier::Emulated);
    assert!(!leases[0].probe, "a fresh escalation is not a probe");
}

#[tokio::test]
async fn a_304_moves_the_schedule_and_writes_no_row() {
    // Doc 05.3 is explicit that a 304 writes no row. At steady state most of
    // what the crawler does is revalidate, so a 304 that produced a row would
    // eventually make the published corpus mostly empty rows, and every one of
    // them would be a second copy of a url that already has a real one.
    let url = "https://example.com/a";
    let state = seeded(&[url]).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Cache::new(
        url,
        Mode::Honest,
        &page("A", &[]),
        Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n"),
    ));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        Arc::clone(&state),
        Arc::clone(&clock),
        config(),
    );
    let sink = Arc::new(Collected::default());

    let first = crawler.tick(&sink).await.expect("tick");
    assert_eq!(first.fetched, 1);
    assert_eq!(first.rows, 1);

    // Past doc 09's initial refresh interval, which is what puts the url back
    // in front of the scheduler at all. It is also past the day robots.txt is
    // good for, so the file is due again and a lease pays for it again.
    clock.advance(PAST_MAX_REFRESH);
    let second = crawler.tick(&sink).await.expect("tick");
    assert_eq!(second.leased, 1);
    assert_eq!(second.not_modified, 1, "the second fetch was not a 304");
    assert_eq!(second.rows, 0, "a 304 wrote a page row");
    assert_eq!(second.fetched, 0);
    assert_eq!(second.failed, 0);
    assert_eq!(sink.rows().len(), 1, "the sink was given a row for a 304");

    // The validator came off the ledger and went back out, which is the round
    // trip the tier is worth nothing without.
    assert_eq!(fetch.conditional(), vec![false, true]);

    // And the schedule moved, so the 304 was a fetch as far as the frontier is
    // concerned even though it left nothing behind. Without this the url comes
    // straight back and the crawler revalidates it in a loop.
    let stats = state.stats().await.expect("stats");
    assert_eq!(stats.urls_fetched, 1);
    assert_eq!(stats.urls_pending, 0);
    let req = umi_state::LeaseRequest::new(FetcherId::LOCAL, clock.now_ms() + 60_000, 4);
    assert!(
        state.lease(&req).await.expect("lease").is_empty(),
        "a 304 left the url due again"
    );
}

#[tokio::test]
async fn three_full_bodies_for_a_conditional_request_drop_t0() {
    // Doc 05.3's first trap. The origin is not lying, it just ignores the
    // validator, and the cost of that is a full body every refresh for a page
    // that never changes. Three of them is the point where the crawler stops
    // paying the extra request headers for nothing.
    let url = "https://example.com/a";
    let state = seeded(&[url]).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Cache::new(
        url,
        Mode::Weak,
        &page("A", &[]),
        Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n"),
    ));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        Arc::clone(&state),
        Arc::clone(&clock),
        config(),
    );
    let sink = Arc::new(Collected::default());
    let host = umi_types::RowKey::for_url(url, None)
        .expect("a crawlable url")
        .host;

    // The first fetch has nothing to revalidate against, so it teaches
    // nothing. The three after it are the three doc 05.3 counts.
    for round in 0..4 {
        crawler.tick(&sink).await.expect("tick");
        clock.advance(PAST_MAX_REFRESH);

        let policy = state
            .host(host)
            .await
            .expect("host")
            .expect("a record")
            .tier;
        assert_eq!(policy.weak_hits, round, "a fetch was counted twice");
    }

    let policy = state
        .host(host)
        .await
        .expect("host")
        .expect("a record")
        .tier;
    assert_eq!(policy.weak_hits, umi_state::TierPolicy::WEAK_HITS_TO_DROP);
    assert!(policy.weak_revalidator());
    assert!(!policy.conditional(), "T0 survived three full bodies");

    // Every request so far carried a validator except the first, and the next
    // one does not, which is the whole saving.
    assert_eq!(fetch.conditional(), vec![false, true, true, true]);
    crawler.tick(&sink).await.expect("tick");
    assert_eq!(fetch.conditional().len(), 5);
    assert!(!fetch.conditional()[4], "T0 was dropped and sent anyway");

    // The count stops at the threshold rather than climbing forever, so a
    // host that is never revalidated again is not written on every fetch.
    let policy = state
        .host(host)
        .await
        .expect("host")
        .expect("a record")
        .tier;
    assert_eq!(policy.weak_hits, umi_state::TierPolicy::WEAK_HITS_TO_DROP);

    // And after five fetches of a page that has said the same thing every
    // time, the corpus holds one copy of it. This is what the origin cost us
    // and what it did not: four full bodies of bandwidth, and no duplicate
    // rows.
    assert_eq!(sink.rows().len(), 1, "the same page was published twice");
}

#[tokio::test]
async fn a_refresh_holding_a_validator_is_leased_at_t0() {
    // The rung the crawl was missing. Gate 2.1 fetched 79,628 pages and not
    // one of them went out conditionally, because the tier on a lease came off
    // the host ladder and the ladder has no way to know that this particular
    // url has an etag sitting in the ledger. At 250 pages a second the
    // difference is around 43 MB a second of bodies against a few hundred
    // kilobytes of 304s.
    let url = "https://example.com/a";
    let state = seeded(&[url]).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Cache::new(
        url,
        Mode::Honest,
        &page("A", &[]),
        Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n"),
    ));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        Arc::clone(&state),
        Arc::clone(&clock),
        config(),
    );
    let sink = Arc::new(Collected::default());

    crawler.tick(&sink).await.expect("tick");
    let rows = sink.rows();
    assert_eq!(
        rows[0].tier_used,
        Tier::Plain.as_u8(),
        "a first fetch is T1"
    );

    clock.advance(PAST_MAX_REFRESH);
    let req = umi_state::LeaseRequest::new(FetcherId::LOCAL, clock.now_ms(), 4);
    let leases = state.lease(&req).await.expect("lease");
    let due = leases
        .iter()
        .find(|lease| lease.url == url)
        .expect("the page is due again");
    assert_eq!(
        due.tier,
        Tier::Revalidate,
        "a refresh with an etag in hand was not leased at T0"
    );
    assert!(due.revalidate.is_some(), "the etag never left the ledger");

    // And the host ladder is not dragged down with it. The next url on this
    // host has nothing to revalidate and has to be leased at T1.
    let host = umi_types::RowKey::for_url(url, None)
        .expect("a crawlable url")
        .host;
    let policy = state
        .host(host)
        .await
        .expect("host")
        .expect("a record")
        .tier;
    assert_eq!(policy.preferred, Tier::Plain);
}

#[tokio::test]
async fn a_full_body_that_has_not_changed_writes_no_second_row() {
    // Doc 05.3 says a 304 writes no row. An origin that ignores the validator
    // and answers with the body we already have has told us the same thing and
    // charged us for it, and the row it would leave behind is a byte for byte
    // copy of one already published. Gate 2.1's output has 1,778 of these in
    // 79,628 rows.
    let url = "https://example.com/a";
    let state = seeded(&[url]).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Cache::new(
        url,
        Mode::Weak,
        &page("A", &[]),
        Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n"),
    ));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        Arc::clone(&state),
        Arc::clone(&clock),
        config(),
    );
    let sink = Arc::new(Collected::default());

    let first = crawler.tick(&sink).await.expect("tick");
    assert_eq!(first.rows, 1);
    assert_eq!(first.unchanged, 0, "a first fetch has nothing to match");

    clock.advance(PAST_MAX_REFRESH);
    let second = crawler.tick(&sink).await.expect("tick");
    assert_eq!(second.fetched, 1, "the body arrived and was paid for");
    assert_eq!(second.unchanged, 1, "the body was the one we already had");
    assert_eq!(second.rows, 0, "a page that did not change wrote a row");
    assert_eq!(sink.rows().len(), 1);

    // The schedule still moved, exactly as a 304 would have moved it, so the
    // url is not offered straight back.
    let req = umi_state::LeaseRequest::new(FetcherId::LOCAL, clock.now_ms() + 60_000, 4);
    assert!(
        state.lease(&req).await.expect("lease").is_empty(),
        "an unchanged page was left due again"
    );
}

#[tokio::test]
async fn a_304_contradicted_by_the_body_disables_t0_for_the_host() {
    // Doc 05.3's second trap, and the one that loses pages rather than money.
    // An origin behind a misconfigured cache answers 304 forever and the
    // crawler believes it, so the page is frozen at whatever it said the day
    // the cache broke. Nothing in the 304 gives this away, which is why the
    // audit fetches the page again without the validator.
    let url = "https://example.com/a";
    let state = seeded(&[url]).await;
    let clock = Arc::new(FixedClock::at(T0));
    let fetch = Arc::new(Cache::new(
        url,
        Mode::Lying,
        &page("A", &[]),
        Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n"),
    ));
    let crawler = Crawler::new(
        Arc::clone(&fetch),
        Arc::clone(&state),
        Arc::clone(&clock),
        CrawlConfig {
            // Every 304, rather than doc 05.3's one in a hundred. A test that
            // wanted the sampling to fire on its own would have to crawl a
            // hundred pages to see one audit.
            audit_every: 1,
            ..config()
        },
    );
    let sink = Arc::new(Collected::default());
    let host = umi_types::RowKey::for_url(url, None)
        .expect("a crawlable url")
        .host;

    crawler.tick(&sink).await.expect("tick");
    // Far enough on that robots.txt has expired too, so a lease goes on the
    // file again before one goes on the page.
    clock.advance(PAST_MAX_REFRESH);

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.learned, 1);
    // The audit found a body, so the page is a fetch and not a 304. Keeping
    // the 304 would mean throwing away a body we already paid for and that we
    // now know is the current one.
    assert_eq!(report.fetched, 1);
    assert_eq!(report.not_modified, 0);
    assert_eq!(report.rows, 1);

    let policy = state
        .host(host)
        .await
        .expect("host")
        .expect("a record")
        .tier;
    assert!(policy.lying_revalidator, "the lie was not recorded");
    assert!(!policy.conditional(), "T0 survived a caught lie");

    // Three requests: the first fetch, the 304, and the audit that caught it.
    assert_eq!(fetch.conditional(), vec![false, true, false]);

    // And from here the host is never asked conditionally again, so it cannot
    // answer 304 again and the page cannot freeze.
    clock.advance(PAST_MAX_REFRESH);
    let last = crawler.tick(&sink).await.expect("tick");
    assert_eq!(fetch.conditional(), vec![false, true, false, false]);
    // That last fetch came back with the body the audit already stored, so it
    // is an observation and not a version, and it keeps no row.
    assert_eq!(last.fetched, 1);
    assert_eq!(last.unchanged, 1);
    assert_eq!(last.rows, 0);

    let rows = sink.rows();
    assert_eq!(rows.len(), 2);
    assert!(
        rows[1].text_digest != rows[0].text_digest,
        "the audit stored the body the 304 was hiding"
    );
}

#[tokio::test]
async fn a_validator_outlives_the_process() {
    // The saving T0 exists for is only real if the etag is still there after a
    // restart. A crawler that revalidates only within one process run pays for
    // a full body on every url the first time it sees it again, which on a
    // daemon that is restarted for a deploy is most of them.
    let dir = tempfile::TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");
    let url = "https://example.com/a";
    let body = page("A", &[]);

    {
        let state: Arc<dyn State> =
            Arc::new(umi_state_sqlite::SqliteState::open(&path).expect("a new store"));
        let mut seed = Candidate::new(url, T0).expect("a crawlable url");
        seed.discovery = umi_state::Discovery::Seed;
        state.admit(&[seed]).await.expect("admit");

        let fetch = Arc::new(Cache::new(
            url,
            Mode::Honest,
            &body,
            Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n"),
        ));
        let crawler = Crawler::new(
            fetch,
            Arc::clone(&state),
            Arc::new(FixedClock::at(T0)),
            config(),
        );
        assert_eq!(
            crawler
                .tick(&Arc::new(Collected::default()))
                .await
                .expect("tick")
                .fetched,
            1
        );
    }

    let state: Arc<dyn State> =
        Arc::new(umi_state_sqlite::SqliteState::open(&path).expect("the same store again"));
    let later = T0 + PAST_MAX_REFRESH;
    let clock = Arc::new(FixedClock::at(later));
    let fetch = Arc::new(Cache::new(
        url,
        Mode::Honest,
        &body,
        Canned::new().robots("https://example.com", "User-agent: *\nAllow: /\n"),
    ));
    let crawler = Crawler::new(Arc::clone(&fetch), Arc::clone(&state), clock, config());

    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.not_modified, 1, "the etag did not survive the store");
    assert_eq!(report.rows, 0);
    assert_eq!(fetch.conditional(), vec![true]);
}

/// Put every host of these urls where doc 05.8 would put one that serves a
/// client rendered shell: escalated to a browser and settled there.
///
/// `last_success` matches `preferred` so the ladder is not due one of doc
/// 05.8's probes back down, which would lease at T1 and make these tests about
/// something else.
async fn wants_rendering(state: &Arc<dyn State>, urls: &[&str]) {
    let mut hosts = Vec::new();
    for url in urls {
        let key = umi_types::RowKey::for_url(url, None).expect("a crawlable url");
        let mut host = umi_state::HostRow::new(key.host, key.pld);
        host.tier.preferred = Tier::Rendered;
        host.tier.last_success = Tier::Rendered;
        host.tier.max = Tier::Rendered;
        hosts.push(host);
    }
    state.put_host(&hosts).await.expect("put_host");
}

/// A crawler that will run T3 and knows what its browser can do.
fn with_browser(
    fetch: Canned,
    state: Arc<dyn State>,
    clock: &Arc<FixedClock>,
) -> Crawler<Arc<Canned>, Arc<FixedClock>> {
    Crawler::new(
        Arc::new(fetch),
        state,
        Arc::clone(clock),
        CrawlConfig {
            max_tier: Tier::Rendered,
            // Everything gated before anything is fetched, so the budget's
            // answers do not depend on which fetch finished first.
            in_flight: 64,
            ..config()
        },
    )
}

#[tokio::test]
async fn rendering_past_the_budget_is_deferred_and_not_fetched() {
    // Doc 05.9. Eight hosts have escalated to a browser and the browser can do
    // one page a second, which with the default five second window is six of
    // them. The other two do not fail, are not fetched at a cheaper tier and do
    // not become rows. They go back where they came from.
    let urls: Vec<String> = (0..8).map(|n| format!("https://s{n}.example/a")).collect();
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    wants_rendering(&state, &refs).await;

    let mut fetch = Canned::new().renders(1.0);
    for (n, url) in urls.iter().enumerate() {
        fetch = fetch
            .robots(
                &format!("https://s{n}.example"),
                "User-agent: *\nAllow: /\n",
            )
            .html(url, &page("A", &[]));
    }

    let clock = Arc::new(FixedClock::at(T0));
    let crawler = with_browser(fetch, Arc::clone(&state), &clock);
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 8);
    assert_eq!(
        report.rendered, 6,
        "the budget let the wrong number through"
    );
    assert_eq!(report.deferred, 2);
    assert_eq!(report.fetched, 6);
    assert_eq!(report.rows, 6, "a deferred page produced a row");
    assert_eq!(sink.rows().len(), 6);
    assert_eq!(crawler.render().granted(), 6);
    assert_eq!(crawler.render().deferred(), 2);

    // And the two are still there, at the due time they always had, rather
    // than waiting out a lease or a failure backoff. That is the deferred
    // queue: the frontier already orders by priority and already survives a
    // restart, so it is a better queue than one held here would be.
    //
    // No `advance` before this one. The tick above spent a second waiting out
    // the politeness delay after eight robots.txt files, and doc 05.9's budget
    // refills over time, so the two slots it was short are there by the time
    // it returns.
    let again = crawler.tick(&sink).await.expect("tick");
    assert_eq!(again.leased, 2, "the deferred pages did not come back");
    assert_eq!(again.rendered, 2);
    assert_eq!(again.deferred, 0);
    assert_eq!(again.fetched, 2);
}

#[tokio::test]
async fn a_fleet_with_no_browser_at_all_fetches_the_page_anyway() {
    // The failure this is really about is a spin. A deferred lease keeps its
    // due time, so if a machine with no browser deferred every page that wants
    // one, the next tick would lease the same pages, defer them again, and do
    // that for the week until doc 05.8 probes the host back down. Meanwhile
    // nothing else gets leased.
    //
    // So a missing browser behaves like a missing T2: the page goes out at the
    // rung the ladder has. The answer is probably a shell, which is a worse row
    // than a rendered one and a much better outcome than a crawl that stopped.
    let url = "https://example.com/a";
    let state = seeded(&[url]).await;
    wants_rendering(&state, &[url]).await;

    let fetch = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html(url, &page("A", &[]));
    let clock = Arc::new(FixedClock::at(T0));
    let crawler = with_browser(fetch, Arc::clone(&state), &clock);
    let sink = Arc::new(Collected::default());

    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 1);
    assert_eq!(report.deferred, 0, "there is no queue worth waiting in");
    assert_eq!(report.rendered, 0, "nothing rendered, because nothing can");
    assert_eq!(report.rows, 1);
    assert!(
        crawler.fetcher().asked().contains(&url.to_owned()),
        "the page never went out"
    );
    assert_eq!(crawler.render().deferred(), 0);
    assert_eq!(crawler.render().granted(), 0);

    // And the tick after it has nothing left, rather than the same page again.
    let again = crawler.tick(&sink).await.expect("tick");
    assert_eq!(again.leased, 0, "the same page came back around");
}

#[tokio::test]
async fn a_page_deferred_for_a_busy_browser_is_not_a_failure() {
    // The other half of the deferral: a url that came back as failed would take
    // a backoff and count against the host, and nothing about a busy browser is
    // the host's doing.
    let urls: Vec<String> = (0..2).map(|n| format!("https://s{n}.example/a")).collect();
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    wants_rendering(&state, &refs).await;

    let mut fetch = Canned::new().renders(1.0);
    for url in &refs {
        let origin = url.trim_end_matches("/a");
        fetch = fetch
            .robots(origin, "User-agent: *\nAllow: /\n")
            .html(url, &page("A", &[]));
    }
    let clock = Arc::new(FixedClock::at(T0));
    let crawler = with_browser(fetch, Arc::clone(&state), &clock);
    let sink = Arc::new(Collected::default());

    // One slot a second and a five second wait, so the second page is inside
    // the wait and both go through. Making the second one defer needs a tick
    // wider than the wait, which the budget test covers directly.
    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.leased, 2);
    assert_eq!(report.rendered, 2);
    for url in &refs {
        let host = umi_types::RowKey::for_url(url, None)
            .expect("a crawlable url")
            .host;
        let row = state.host(host).await.expect("host").expect("a record");
        assert_eq!(row.consecutive_failures, 0);
        assert_eq!(row.tier.preferred, Tier::Rendered, "the ladder moved");
    }
}

#[tokio::test]
async fn a_row_records_the_rungs_that_ran_and_not_the_rung_it_asked_for() {
    // Doc 05.5 publishes `tier_used` and `tier_path` as the record of which
    // fraction of the web needs which tier. The host here has escalated to a
    // browser and this build has no browser, so doc 05.4 serves the page from
    // T1 and the row has to say T1. A row that repeated the lease back would
    // put a rendered page in that dataset for a page nothing rendered, and the
    // dataset is the whole reason for the column.
    let url = "https://example.com/a";
    let memory = Arc::new(MemoryState::new());
    let mut seed = Candidate::new(url, T0).expect("a crawlable url");
    seed.discovery = umi_state::Discovery::Seed;
    memory.admit(&[seed]).await.expect("admit");
    let state: Arc<dyn State> = Arc::clone(&memory) as Arc<dyn State>;
    wants_rendering(&state, &[url]).await;

    let inner = Canned::new()
        .robots("https://example.com", "User-agent: *\nAllow: /\n")
        .html(url, &page("A", &[]));
    let clock = Arc::new(FixedClock::at(T0));
    let crawler = Crawler::new(
        Arc::new(NoBrowser(inner)),
        Arc::clone(&state),
        Arc::clone(&clock),
        CrawlConfig {
            max_tier: Tier::Rendered,
            ..config()
        },
    );

    let sink = Arc::new(Collected::default());
    let report = crawler.tick(&sink).await.expect("tick");
    assert_eq!(report.rows, 1);

    let row = sink.rows().remove(0);
    assert_eq!(row.tier_used, Tier::Plain.as_u8(), "T1 answered");
    assert_eq!(
        row.tier_path,
        vec![Tier::Rendered.as_u8(), Tier::Plain.as_u8()],
        "the lease wanted a browser and did not get one"
    );

    // The ledger keeps the same answer, since its field is documented as the
    // tier that produced the answer too.
    let key = umi_types::RowKey::for_url(url, None).expect("a crawlable url");
    let ledger = memory.row(&key).expect("a ledger row");
    assert_eq!(ledger.tier_used, Tier::Plain);

    // The host ladder is the one thing that does not move. Doc 05.8 learns
    // from the tier the lease was for, because this page came back off T1 only
    // because there is no browser here, which is not evidence that the host
    // stopped needing one.
    let host = state.host(key.host).await.expect("host").expect("a record");
    assert_eq!(host.tier.preferred, Tier::Rendered, "the ladder stayed put");
}

#[tokio::test]
async fn the_budget_only_holds_back_the_tier_it_is_for() {
    // A crawl with no browser at all still runs, at full speed, and the budget
    // is not in the path of a T1 page. This is the case that matters for the
    // 250 pages a second gate: doc 05.9 rations one percent of the crawl and
    // must cost the other 99 percent nothing.
    let urls: Vec<String> = (0..4).map(|n| format!("https://s{n}.example/a")).collect();
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;

    let mut fetch = Canned::new();
    for (n, url) in urls.iter().enumerate() {
        fetch = fetch
            .robots(
                &format!("https://s{n}.example"),
                "User-agent: *\nAllow: /\n",
            )
            .html(url, &page("A", &[]));
    }

    let clock = Arc::new(FixedClock::at(T0));
    let crawler = with_browser(fetch, Arc::clone(&state), &clock);
    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.fetched, 4);
    assert_eq!(report.deferred, 0);
    assert_eq!(report.rendered, 0);
    assert_eq!(report.emulated, 0);
}

#[tokio::test]
async fn a_lease_scale_of_zero_leases_nothing_and_says_it_was_the_ladder() {
    // Doc 15.3's rung three. The distinction the flag carries is the whole
    // point: an idle report on its own is what a finished crawl looks like,
    // and `umi crawl` stops on one of those.
    let state = seeded(&["https://example.com/a"]).await;
    let crawler = crawler(Canned::new(), state);
    crawler.restrain(crate::Allowance {
        lease_scale: 0.0,
        ..crate::Allowance::default()
    });
    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert!(report.idle());
    assert!(report.restrained);

    // And it comes back. Nothing about the pause is recorded against the url,
    // so the tick after the pressure lifts fetches what the paused one would
    // have.
    crawler.restrain(crate::Allowance::default());
    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert!(!report.restrained);
    assert_eq!(report.leased, 1);
}

#[tokio::test]
async fn a_half_lease_scale_takes_half_the_batch() {
    // Rung two. Sixteen urls on sixteen hosts so that nothing but the batch
    // size is holding the tick back, and a scale of a half over a batch of
    // eight is four.
    let urls: Vec<String> = (0..16).map(|n| format!("https://s{n}.example/a")).collect();
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let state = seeded(&refs).await;
    let crawler = Crawler::new(
        Arc::new(Canned::new()),
        state,
        Arc::new(FixedClock::at(T0)),
        CrawlConfig {
            batch: 8,
            ..CrawlConfig::default()
        },
    );
    crawler.restrain(crate::Allowance {
        lease_scale: 0.5,
        ..crate::Allowance::default()
    });
    let report = crawler
        .tick(&Arc::new(Collected::default()))
        .await
        .expect("tick");
    assert_eq!(report.leased, 4);
    // Smaller than the configured batch, which is the thing the caller has to
    // know so that it does not read the short tick as a drained frontier.
    assert!(report.restrained);
}

#[tokio::test]
async fn the_allowance_lowers_the_tier_ceiling_and_never_raises_it() {
    // Doc 15.3 caps at T2 on rung one, and a process configured for T1 stays
    // at T1. The ladder is a ceiling on a ceiling, and a config that never
    // wanted a browser does not get one because the disk filled up.
    let state = seeded(&[]).await;
    let crawler = crawler(Canned::new(), state);
    assert_eq!(crawler.config().max_tier, Tier::Plain);
    crawler.restrain(crate::Allowance {
        max_tier: Tier::Emulated,
        ..crate::Allowance::default()
    });
    assert_eq!(crawler.allowance().max_tier, Tier::Emulated);
    assert_eq!(
        crawler.config().max_tier.min(crawler.allowance().max_tier),
        Tier::Plain
    );
}
