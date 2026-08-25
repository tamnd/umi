//! The loop: lease, check robots, fetch, extract, row, complete.
//!
//! Doc 03 puts this in one crate for a reason. `umid` runs it as a daemon and
//! `umi crawl` runs it once against a scope, and if each of them owned its own
//! copy of the order the two would drift, which for a crawler means one of them
//! quietly stops checking something.
//!
//! # Shape
//!
//! One [`tick`](Crawler::tick) is a batch: take leases from the state layer,
//! run them all concurrently, and hand the answers back in one call. The batch
//! is the unit because doc 08.5's whole design is batched, and because the
//! alternative costs a database round trip per URL, which at 250 pages a second
//! is 250 of them a second for no benefit.
//!
//! Concurrency inside a tick is a [`FuturesUnordered`] and not a join over
//! fixed chunks. The difference matters more than it looks: fetch times on the
//! web run from 40 ms to the timeout, so a barrier every N URLs spends most of
//! its time with almost every slot idle waiting for the slowest site in the
//! chunk. Unordered keeps every slot full and finishes a tick in roughly the
//! time of its slowest single fetch rather than the sum of its slowest per
//! chunk.
//!
//! # What is not here
//!
//! Writing segments and publishing. The loop produces rows and hands them to a
//! [`Sink`], and where they go is the caller's problem, because `umi crawl
//! --dry-run` wants them counted, the tests want them in a `Vec` and `umid`
//! wants them in a `.umi` file. Splitting it that way also keeps this file
//! free of any I/O that is not a fetch.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use umi_extract::extract;
use umi_fetch::Outcome;
use umi_state::{
    Candidate, Discovery, FetchOutcome, FetchResult, LeaseRequest, Priority, State, StateError,
};
use umi_types::{FetcherId, Revalidator, RowKey, Tier, Verification};

use crate::clock::Clock;
use crate::fetch::Fetch;
use crate::page::{Crawled, PageRow};
use crate::robots::RobotsCache;

/// Where finished rows go.
///
/// One method, taking a slice, for the same reason the state trait takes
/// slices: a segment writer wants a batch and a counter does not care, so the
/// batched signature costs the counter nothing and saves the writer everything.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Take a batch of rows.
    ///
    /// # Errors
    ///
    /// Whatever the sink reports. An error stops the tick before the
    /// completions go back to the state layer, so the leases expire and the
    /// URLs are handed out again. That is the right failure: a page whose row
    /// could not be stored has not been crawled, however well the fetch went.
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError>;
}

/// What went wrong.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CrawlError {
    /// The state layer.
    #[error("state: {0}")]
    State(#[from] StateError),
    /// The sink.
    #[error("sink: {0}")]
    Sink(String),
}

/// How the loop behaves.
#[derive(Clone, Debug)]
pub struct CrawlConfig {
    /// Who we are, for doc 04's receipts and doc 10.5's `fetcher_id` column.
    pub fetcher: FetcherId,
    /// How many URLs to take per tick.
    pub batch: u32,
    /// How many fetches to have in flight at once.
    ///
    /// Not the same as `batch` and usually smaller. The batch is how much work
    /// is held, this is how much of it is on the wire, and the gap between them
    /// is what lets a tick start its next fetch the instant one finishes rather
    /// than at the top of the next tick.
    pub in_flight: usize,
    /// Ceiling per host inside one tick, on top of doc 07.6's one request in
    /// flight per host.
    pub max_per_host: u32,
    /// The most expensive tier this process will run.
    pub max_tier: Tier,
    /// How long a lease is good for.
    pub lease_for: Duration,
    /// Doc 11's depth ceiling for links found on a page.
    pub max_depth: u8,
    /// The `crawl_profile` stamped on every row.
    pub crawl_profile: u32,
}

impl Default for CrawlConfig {
    fn default() -> Self {
        Self {
            fetcher: FetcherId::LOCAL,
            batch: 512,
            // Doc 05.4's client allows 2 connections per host and doc 07.6
            // allows one request in flight per host, so the useful ceiling is
            // set by how many distinct hosts a batch covers rather than by the
            // socket count. 128 keeps a 4 core box busy without putting so
            // much in flight that a tick's tail is one site holding 127 idle
            // slots open.
            in_flight: 128,
            max_per_host: 4,
            max_tier: Tier::Plain,
            lease_for: Duration::from_secs(60),
            max_depth: umi_frontier::MAX_DEPTH,
            crawl_profile: 0,
        }
    }
}

/// What one tick did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Leases taken.
    pub leased: usize,
    /// Rows produced, which is one per lease that got as far as an answer.
    pub rows: usize,
    /// Fetches that produced a body.
    pub fetched: usize,
    /// Conditional requests that held.
    pub not_modified: usize,
    /// Answers that were a failure of some kind.
    pub failed: usize,
    /// URLs robots.txt said no to, which are completed as excluded and never
    /// fetched.
    pub disallowed: usize,
    /// Links seen on the pages in this tick.
    pub links_seen: usize,
    /// Links that were new and went to the frontier.
    pub links_admitted: usize,
}

impl TickReport {
    /// Whether the tick found nothing to do, which is how a caller knows to
    /// sleep rather than spin.
    #[must_use]
    pub const fn idle(&self) -> bool {
        self.leased == 0
    }
}

/// The loop.
pub struct Crawler<F, C> {
    fetch: F,
    state: Arc<dyn State>,
    clock: C,
    robots: RobotsCache,
    config: CrawlConfig,
}

impl<F: Fetch, C: Clock> Crawler<F, C> {
    /// Build a crawler.
    #[must_use]
    pub fn new(fetch: F, state: Arc<dyn State>, clock: C, config: CrawlConfig) -> Self {
        Self {
            fetch,
            state,
            clock,
            robots: RobotsCache::new(),
            config,
        }
    }

    /// The fetcher, so a caller can measure what it did.
    #[must_use]
    pub const fn fetcher(&self) -> &F {
        &self.fetch
    }

    /// The robots cache, so a caller can prime it on startup or measure it.
    #[must_use]
    pub const fn robots(&self) -> &RobotsCache {
        &self.robots
    }

    /// The clock this crawler reads.
    ///
    /// A daemon needs it to decide how long to sleep between ticks, and it
    /// has to be this clock rather than another one or the sleep and the
    /// politeness window disagree.
    #[must_use]
    pub const fn clock(&self) -> &C {
        &self.clock
    }

    /// The configuration this was built with.
    #[must_use]
    pub const fn config(&self) -> &CrawlConfig {
        &self.config
    }

    /// One batch of work, start to finish.
    ///
    /// Returns an idle report when the frontier had nothing ready, which is
    /// normal rather than an error: it usually means every host with work is
    /// inside its politeness window, and the answer is to wait rather than to
    /// ask again immediately.
    ///
    /// # Errors
    ///
    /// [`CrawlError::State`] if the leases could not be taken or the
    /// completions could not be recorded, and [`CrawlError::Sink`] if the rows
    /// could not be stored. In every case the leases are left to expire rather
    /// than released, because an error here means we do not know what happened
    /// and the safe reading of that is that the work was not done.
    pub async fn tick(&self, sink: &dyn Sink) -> Result<TickReport, CrawlError> {
        let now_ms = self.clock.now_ms();
        let leases = self
            .state
            .lease(&LeaseRequest {
                fetcher: self.config.fetcher,
                now_ms,
                max_urls: self.config.batch,
                max_per_host: self.config.max_per_host,
                max_tier: self.config.max_tier,
                lease_for: self.config.lease_for,
                plds: &[],
            })
            .await?;

        let mut report = TickReport {
            leased: leases.len(),
            ..TickReport::default()
        };
        if leases.is_empty() {
            return Ok(report);
        }

        let mut pending = FuturesUnordered::new();
        let mut queue = leases.into_iter();
        let mut rows = Vec::with_capacity(report.leased);
        let mut outcomes = Vec::with_capacity(report.leased);
        let mut candidates: Vec<(String, u8)> = Vec::new();

        // Fill the window, then top it up as each one lands, which is what
        // keeps every slot busy rather than waiting on the slowest of a chunk.
        for lease in queue.by_ref().take(self.config.in_flight) {
            pending.push(self.one(lease));
        }
        while let Some(done) = pending.next().await {
            if let Some(lease) = queue.next() {
                pending.push(self.one(lease));
            }
            let Fetched {
                row,
                outcome,
                links,
                disallowed,
            } = done;

            if disallowed {
                report.disallowed += 1;
            }
            if let Some(row) = row {
                match row.outcome {
                    umi_types::OutcomeCode::Ok => report.fetched += 1,
                    umi_types::OutcomeCode::NotModified => report.not_modified += 1,
                    _ => report.failed += 1,
                }
                report.links_seen += links.len();
                candidates.extend(links);
                rows.push(row);
            }
            outcomes.push(outcome);
        }

        report.rows = rows.len();

        // Rows first, then completions. The order is the crash safety rule
        // from doc 16's gate 1.3 and it is not an implementation detail: a
        // completion recorded before its row is stored is a URL the crawler
        // believes it has and will not fetch again, and the page is gone. The
        // other order costs a refetch, which is the cheaper mistake.
        if !rows.is_empty() {
            sink.take(&rows).await?;
        }
        self.state.complete(&outcomes).await?;

        report.links_admitted = self.admit(&candidates, now_ms).await?;
        Ok(report)
    }

    /// One lease, from robots check to row.
    async fn one(&self, lease: umi_state::Lease) -> Fetched {
        let now = || self.clock.now_ms();
        let Some(origin) = origin_of(&lease.url) else {
            return Fetched::malformed(&lease, now());
        };

        let (decision, entry) = self
            .robots
            .decide(&self.fetch, lease.key.host, &origin, &lease.url, now())
            .await;
        if !decision.is_allowed() {
            let mut out = Fetched::excluded(&lease, now(), umi_state::ExcludeReason::Robots);
            out.disallowed = true;
            return out;
        }

        let robots_checked_ms = entry.fetched_ms;
        let outcome = match self
            .fetch
            .fetch(&lease.url, lease.revalidate.as_ref())
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => return Fetched::malformed(&lease, now()),
        };

        let fetched_at_ms = now();
        let host = host_of(&lease.url).unwrap_or_default();
        let extracted = match &outcome {
            Outcome::Ok(page) if page.media == umi_fetch::Media::Html => {
                url::Url::parse(&page.final_url)
                    .ok()
                    .map(|base| extract(page.body.as_ref(), &base))
            }
            _ => None,
        };

        let row = PageRow::build(&Crawled {
            url: &lease.url,
            keys: lease.key,
            host: &host,
            fetched_at_ms,
            outcome: &outcome,
            extracted: extracted.as_ref(),
            tier_used: lease.tier,
            tier_path: std::slice::from_ref(&lease.tier),
            robots_checked_ms,
            content_usage: entry.robots.content_usage().first().map(String::as_str),
            fetcher_id: self.config.fetcher,
            verification: Verification::Local,
            crawl_profile: self.config.crawl_profile,
        });

        // Doc 11.4: a page that says nofollow keeps its own row and gives up
        // its links. Depth is the page's plus one, and the ceiling is applied
        // here rather than at admit time so that a page at the limit does not
        // cost a batch of candidates that all get rejected.
        let links = extracted
            .as_ref()
            .filter(|e| !e.robots.nofollow)
            .filter(|_| lease.depth < self.config.max_depth)
            .map(|e| {
                e.links
                    .links
                    .iter()
                    .filter(|l| !l.rel.has(umi_extract::Rel::NOFOLLOW))
                    .map(|l| (l.url.clone(), lease.depth.saturating_add(1)))
                    .collect()
            })
            .unwrap_or_default();

        let outcome = FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: fetched_at_ms,
            tier_used: lease.tier,
            result: result_of(&row, &outcome),
        };
        Fetched {
            row: Some(row),
            outcome,
            links,
            disallowed: false,
        }
    }

    /// Put the links found this tick into the frontier.
    ///
    /// Doc 08.5's batch, in chunks, because a page with ten thousand links is
    /// a real page and a state backend is allowed to refuse a batch that size.
    async fn admit(&self, links: &[(String, u8)], now_ms: u64) -> Result<usize, CrawlError> {
        let mut admitted = 0;
        for chunk in links.chunks(umi_state::BATCH) {
            let batch: Vec<Candidate<'_>> = chunk
                .iter()
                .filter_map(|(url, depth)| {
                    let key = RowKey::for_url(url, None).ok()?;
                    Some(Candidate {
                        key,
                        url,
                        depth: *depth,
                        priority: Priority::default(),
                        discovered_ms: now_ms,
                        discovery: Discovery::Trusted,
                    })
                })
                .collect();
            if batch.is_empty() {
                continue;
            }
            admitted += self.state.admit(&batch).await?.admitted as usize;
        }
        Ok(admitted)
    }
}

/// One finished lease.
struct Fetched {
    row: Option<PageRow>,
    outcome: FetchOutcome,
    links: Vec<(String, u8)>,
    disallowed: bool,
}

impl Fetched {
    /// A lease that was never fetched, because robots.txt said no or the URL
    /// was not usable. No row, because doc 10.5's `pages` stream is what was
    /// crawled and this was not.
    fn excluded(lease: &umi_state::Lease, now_ms: u64, reason: umi_state::ExcludeReason) -> Self {
        Self::answered(lease, now_ms, FetchResult::Excluded { reason })
    }

    /// A lease whose URL this fetcher could not send at all.
    ///
    /// `Malformed` rather than `Excluded`, because exclusion is a decision
    /// about a URL we understand and this is a URL we do not. It came out of
    /// the frontier, so it went in through `RowKey::for_url` and canonicalised
    /// once already, which makes this a corrupt row rather than a bad link and
    /// worth leaving a failure on rather than quietly retiring.
    fn malformed(lease: &umi_state::Lease, now_ms: u64) -> Self {
        Self::answered(
            lease,
            now_ms,
            FetchResult::Failed {
                status: None,
                kind: umi_state::FailureKind::Malformed,
            },
        )
    }

    fn answered(lease: &umi_state::Lease, now_ms: u64, result: FetchResult) -> Self {
        Self {
            row: None,
            outcome: FetchOutcome {
                lease: lease.id,
                key: lease.key,
                finished_ms: now_ms,
                tier_used: lease.tier,
                result,
            },
            links: Vec::new(),
            disallowed: false,
        }
    }
}

/// Doc 08.3's scheduling view of what happened, from doc 10.5's row.
///
/// The row is the record and this is the consequence, and they are computed
/// from the same outcome so they cannot disagree about whether a page changed.
fn result_of(row: &PageRow, outcome: &Outcome) -> FetchResult {
    use umi_types::OutcomeCode as Code;
    let revalidate = match outcome {
        Outcome::Ok(page) => page.revalidate.clone(),
        Outcome::NotModified { revalidate, .. } => revalidate.clone(),
        _ => Revalidator::default(),
    };
    match row.outcome {
        Code::Ok => FetchResult::Fetched {
            status: row.status,
            content_hash: content_hash(row),
            revalidate,
        },
        Code::NotModified => FetchResult::NotModified {
            status: row.status,
            revalidate,
        },
        Code::Gone => FetchResult::Gone { status: row.status },
        Code::RobotsChanged => FetchResult::Excluded {
            reason: umi_state::ExcludeReason::Robots,
        },
        code => FetchResult::Failed {
            status: (row.status != 0).then_some(row.status),
            kind: failure_kind(code),
        },
    }
}

/// Doc 08.3's truncated content hash, off the row's full one.
fn content_hash(row: &PageRow) -> [u8; 8] {
    let mut out = [0u8; 8];
    out.copy_from_slice(&row.text_digest[..8]);
    out
}

/// Doc 04.5's outcome as doc 08.3's failure kind.
///
/// Coarser on purpose. The published row keeps which of seventeen things
/// happened and the scheduler only needs to know how to back off, and a
/// scheduler with seventeen retry policies is a scheduler nobody can reason
/// about.
const fn failure_kind(code: umi_types::OutcomeCode) -> umi_state::FailureKind {
    use umi_state::FailureKind as Kind;
    use umi_types::OutcomeCode as Code;
    match code {
        Code::DnsFailure | Code::ConnectFailure => Kind::Connect,
        Code::TlsFailure => Kind::Tls,
        Code::Timeout => Kind::Timeout,
        Code::ServerError => Kind::ServerError,
        // A 429 backs the host off and so does a challenge, and doc 08.3 has
        // one class for that. The row keeps the difference; the scheduler does
        // not need it to decide how long to wait.
        Code::RateLimited | Code::Blocked | Code::Challenge => Kind::Blocked,
        Code::TooLarge | Code::RedirectedOffHost => Kind::Rejected,
        Code::Malformed => Kind::Malformed,
        _ => Kind::NotFound,
    }
}

/// The scheme and authority of a URL, which is where its robots.txt lives.
fn origin_of(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    Some(match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    })
}

/// Just the host, for doc 10.5's `host` column.
fn host_of(url: &str) -> Option<String> {
    url::Url::parse(url).ok()?.host_str().map(ToOwned::to_owned)
}
