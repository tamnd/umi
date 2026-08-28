//! The loop: lease, check robots, fetch, extract, row, complete.
//!
//! Doc 03 puts this in one crate for a reason. `umid` runs it as a daemon and
//! `umi crawl` runs it once against a scope, and if each of them owned its own
//! copy of the order the two would drift, which for a crawler means one of them
//! quietly stops checking something.
//!
//! # Shape
//!
//! One [`tick`](Crawler::tick) is a batch: take leases from the scheduler, run
//! them all concurrently, and hand the answers back in one call. The batch is
//! the unit because doc 08.5's whole design is batched, and because the
//! alternative costs a database round trip per URL, which at 250 pages a second
//! is 250 of them a second for no benefit.
//!
//! The leases come from [`Frontier`] rather than from the store, which is what
//! makes doc 09.3's cap on a pay level domain real. The store schedules per
//! host and cannot see the domain: a big site is dozens of hosts, each of them
//! politely held to one request at a time, and all of them the same operator.
//! The frontier picks the domains that have room, asks the store for work from
//! each, and charges the domain for what it got.
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
use umi_frontier::{Ask, Config as FrontierConfig, Frontier, Rate};
use umi_state::{Budget, Discovery, FetchOutcome, FetchResult, Pace, State, StateError};
use umi_types::{FetcherId, Revalidator, Tier, TierSignal, Verification};

use crate::clock::Clock;
use crate::fetch::Fetch;
use crate::page::{Crawled, PageRow};
use crate::robots::RobotsCache;
use crate::scope::{LinkPolicy, Scope};

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
    ///
    /// The hard ceiling, which a scope can lower and cannot raise.
    pub max_depth: u8,
    /// Doc 13's scope: what this crawl is allowed to fetch, and what its rows
    /// are stamped with.
    ///
    /// The default is [`Scope::general`], which is id 0 and admits everything,
    /// so the general crawl and a focused one run the same code.
    pub scope: Arc<Scope>,
    /// How doc 09.5's refresh classes divide a tick's capacity between new
    /// URLs and the ones already crawled.
    pub budget: Budget,
    /// Doc 09.3's cap on one pay level domain, which is 20 requests a second.
    ///
    /// Separate from `max_per_host` and coarser. A big site is dozens of hosts
    /// under one domain, and holding each host to its own polite delay still
    /// adds up to a lot of traffic at one operator, so the cap that matters to
    /// them is this one.
    pub rate: Rate,
    /// How many domains one tick may take work from.
    ///
    /// The scheduler asks the store once per domain, so this is also the most
    /// round trips a tick can cost. See [`umi_frontier::Config::max_domains`].
    pub max_domains: usize,
}

impl CrawlConfig {
    /// The scheduler's half of this, for the frontier the loop runs on.
    ///
    /// Two config types over one set of knobs, because the scheduler is a crate
    /// that knows nothing about fetching and the loop is a crate that has to
    /// configure it. This is the seam, and it is a function rather than a field
    /// so there is one place the two can disagree and it is here.
    #[must_use]
    pub fn frontier(&self) -> FrontierConfig {
        FrontierConfig {
            rate: self.rate,
            max_domains: self.max_domains,
            max_per_host: self.max_per_host,
            lease_for: self.lease_for,
            max_depth: self.max_depth,
            budget: self.budget,
        }
    }

    /// How deep links from a page at `depth` may go.
    ///
    /// The lower of the process ceiling and the scope's, because a profile
    /// asking for depth 200 on a crawler built for 16 should get 16 rather
    /// than an error at the point where the frontier runs out of room.
    #[must_use]
    pub fn depth_limit(&self) -> u8 {
        self.scope
            .max_depth
            .map_or(self.max_depth, |d| d.min(self.max_depth))
    }
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
            scope: Arc::new(Scope::general()),
            budget: Budget::DEFAULT,
            rate: Rate::default(),
            max_domains: FrontierConfig::default().max_domains,
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
    /// Body bytes as they arrived, before decompression.
    ///
    /// Doc 13.2's `max_bytes` is a budget on this number and doc 14.3's
    /// progress line prints it, and both want what the origin actually sent
    /// rather than what we kept, because that is the figure the origin sees
    /// on its own bill.
    pub bytes_fetched: u64,
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
    /// Responses that were a wall rather than a page, per doc 05.8.
    ///
    /// Counted apart from `failed` because it is the number that says whether
    /// a crawl is being refused rather than going wrong, and they are not the
    /// same problem. None of these produced a row.
    pub challenged: usize,
    /// Hosts whose tier ladder moved this tick.
    ///
    /// Normally zero. A number that stays high means hosts are escalating and
    /// de-escalating in turn, which is worth looking at.
    pub learned: usize,
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
    frontier: Frontier<Arc<dyn State>>,
    clock: C,
    robots: RobotsCache,
    config: CrawlConfig,
}

impl<F: Fetch, C: Clock> Crawler<F, C> {
    /// Build a crawler.
    #[must_use]
    pub fn new(fetch: F, state: Arc<dyn State>, clock: C, config: CrawlConfig) -> Self {
        let frontier = Frontier::new(state, config.frontier());
        Self {
            fetch,
            frontier,
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

    /// The store underneath, for the callers that go straight to it.
    ///
    /// Segment rows, host records and checkpoints do not concern the scheduler,
    /// and neither do completions: the domain was charged when the lease was
    /// issued, so there is nothing for the frontier to do when the answer comes
    /// back.
    #[must_use]
    pub const fn state(&self) -> &Arc<dyn State> {
        self.frontier.state()
    }

    /// The scheduler, so a caller can look at what is being scheduled.
    #[must_use]
    pub const fn frontier(&self) -> &Frontier<Arc<dyn State>> {
        &self.frontier
    }

    /// Rebuild the domain schedule from what the store holds, per doc 09.8.
    ///
    /// Call it once after the seeds are in and before the first
    /// [`tick`](Self::tick). The schedule is in memory and the urls are not, so
    /// a coordinator that comes back up has every url and no idea which domains
    /// they belong to until this runs, and a tick before it leases nothing.
    ///
    /// Returns how many domains are being scheduled afterwards.
    ///
    /// # Errors
    ///
    /// [`CrawlError::State`] if the store could not be read. The schedule is
    /// left untouched.
    pub async fn resume(&self) -> Result<usize, CrawlError> {
        Ok(self.frontier.resume().await?)
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
        // The schedule lives in memory and the urls do not, so a crawler whose
        // gate is empty has no domains to take work from and leases nothing
        // however full the store is. Seeds go in through `umi seed` and through
        // the store directly, which is the case this covers.
        //
        // Here rather than in `resume`, so that a caller who forgets to call it
        // gets a working crawl instead of a silent stall. It costs a `resident`
        // on a tick that found no domains, which is a store that is empty or a
        // crawl that is over, and nothing at all once anything is scheduled.
        if self.frontier.domains() == 0 {
            self.frontier.resume().await?;
        }

        let now_ms = self.clock.now_ms();
        // Through the scheduler and not straight to the store. The store hands
        // out work per host; doc 09.3's cap is per pay level domain, and the
        // two are not the same thing. A site on fifty hosts under one domain is
        // fifty polite hosts and one operator wondering why fifty of our
        // connections turned up at once, and the frontier is where that is
        // counted.
        let leases = self
            .frontier
            .tick(&Ask {
                fetcher: self.config.fetcher,
                now_ms,
                max_urls: self.config.batch,
                max_tier: self.config.max_tier,
            })
            .await?;

        let mut report = TickReport {
            leased: leases.len(),
            ..TickReport::default()
        };
        if leases.is_empty() {
            return Ok(report);
        }

        // Earliest first, because a lease that is not due yet still occupies a
        // slot in the window while it waits. Sorted, the only time a slot holds
        // a waiting lease is when everything ahead of it is waiting too, so
        // nothing that could have been fetched is sitting behind something that
        // could not. Unsorted, one host that owes a minute of politeness can
        // park the whole window while other hosts have work ready to go.
        let mut leases = leases;
        leases.sort_by_key(|lease| lease.not_before_ms);

        let mut pending = FuturesUnordered::new();
        let mut queue = leases.into_iter();
        let mut rows = Vec::with_capacity(report.leased);
        let mut outcomes = Vec::with_capacity(report.leased);
        let mut candidates: Vec<(String, u8)> = Vec::new();
        let mut signals: Vec<Learned> = Vec::new();

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
                links_seen,
                disallowed,
                signal,
            } = done;

            if disallowed {
                report.disallowed += 1;
            }
            if let Some(learned) = signal {
                // A block with no row behind it is a challenge page: doc 05.8
                // says a 200 carrying a wall is not a fetch, so `one` throws
                // the row away and this is what is left of it. A block that
                // does have a row is an honest 403 and is counted as a
                // failure like any other.
                if learned.signal == TierSignal::Blocked && row.is_none() {
                    report.challenged += 1;
                }
                // The `teaches` check belongs here and not in `relearn`, even
                // though `relearn` would reach the same answer. `learn` is a
                // scan of what the tick has collected so far, so folding in a
                // page that cannot teach anything costs a pass over the batch
                // per page, and a healthy crawl is nothing but those pages.
                if learned.teaches() {
                    learn(&mut signals, learned);
                }
            }
            if let Some(row) = row {
                match row.outcome {
                    umi_types::OutcomeCode::Ok => report.fetched += 1,
                    umi_types::OutcomeCode::NotModified => report.not_modified += 1,
                    _ => report.failed += 1,
                }
                report.bytes_fetched += u64::from(row.content_length);
                report.links_seen += links_seen;
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
        self.state().complete(&outcomes).await?;

        // After the completions, because both write the host record and the
        // completion is the one that owns doc 07.6's pacing columns. Reading
        // first would mean writing back a politeness timer from before this
        // tick moved it.
        report.learned = self.relearn(&signals, now_ms).await?;
        report.links_admitted = self.admit(&candidates, now_ms).await?;
        Ok(report)
    }

    /// Move the tier ladders of the hosts this tick learned something about.
    ///
    /// Doc 05.8 is per host state and it is learned rather than configured, so
    /// this is where the learning is written down. It is also the one part of
    /// a tick that touches the host table for a reason other than pacing, and
    /// the cost of it has to stay near zero on a crawl where nothing is going
    /// wrong. Two things keep it there: [`Learned::teaches`] drops the answers
    /// that cannot have changed anything as the tick collects them, so on a
    /// healthy crawl `signals` is empty and this is a function call, and
    /// [`TierPolicy::observe`] reports whether it actually moved, so a host
    /// that is read and found unchanged is not written back.
    async fn relearn(&self, signals: &[Learned], now_ms: u64) -> Result<usize, CrawlError> {
        let mut moved = 0;
        for learned in signals {
            let Some(mut host) = self.state().host(learned.host).await? else {
                continue;
            };
            let mut changed = host.tier.observe(learned.signal, learned.tier, now_ms);

            if learned.signal == TierSignal::Blocked {
                // Doc 05.8's exponential backoff. Separate from doc 07.6's
                // adaptive delay, which the completion has already applied,
                // and larger: a host that is refusing us is not asking us to
                // slow down, it is asking us to go away for a while.
                let until = now_ms.saturating_add(host.tier.backoff_ms());
                if until > host.next_allowed_ms {
                    host.next_allowed_ms = until;
                    changed = true;
                }
                // Thirty days of it, and the host leaves the frontier with a
                // record of why. Never set back here: coming back is a
                // decision, and doc 05.8 gives it to the monthly probe.
                if host.tier.refusing() && !host.refusing {
                    host.refusing = true;
                    changed = true;
                }
            }

            if changed {
                self.state().put_host(&[host]).await?;
                moved += 1;
            }
        }
        Ok(moved)
    }

    /// One lease, from robots check to row.
    async fn one(&self, lease: umi_state::Lease) -> Fetched {
        // Doc 07.6, and the whole reason `not_before_ms` exists. The state
        // layer already spaced the leases of a batch by each host's politeness
        // delay, and until this line the loop threw that away and sent the
        // batch as fast as the window allowed. A crawl asking for one request a
        // second put four on blog.rust-lang.org inside 138 ms, which is the
        // kind of mistake the origin sees and we do not.
        //
        // Before the robots check rather than after, since robots.txt is a
        // request to the same host and counts the same way.
        self.clock.sleep_until_ms(lease.not_before_ms).await;

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
        let started_ms = now();
        let outcome = match self
            .fetch
            .fetch(&lease.url, lease.revalidate.as_ref())
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => return Fetched::malformed(&lease, now()),
        };

        let fetched_at_ms = now();
        let pace = pace_of(&outcome, started_ms, fetched_at_ms);

        // Doc 13.2's content filter, first half. This is the first moment the
        // type and the size exist, and a page rejected here is never extracted.
        // A crawl scoped to English HTML still has to spend the fetch to find
        // out that a page is a German PDF, so what this saves is the extraction
        // and the row rather than the request. The completion still goes back,
        // so the URL is not handed out again.
        let filter = &self.config.scope.content;
        if let Outcome::Ok(page) = &outcome
            && !filter.accepts_response(
                page.content_type.as_deref(),
                u32::try_from(page.body.len()).unwrap_or(u32::MAX),
            )
        {
            let reason = umi_state::ExcludeReason::ContentType;
            return Fetched::excluded(&lease, fetched_at_ms, reason).paced(pace);
        }

        let host = host_of(&lease.url).unwrap_or_default();
        let extracted = match &outcome {
            Outcome::Ok(page) if page.media == umi_fetch::Media::Html => {
                url::Url::parse(&page.final_url)
                    .ok()
                    .map(|base| extract(page.body.as_ref(), &base))
            }
            _ => None,
        };

        // Doc 05.8's signal, which needs the fetch and the extraction
        // together. The status alone cannot tell a page from a wall, and the
        // extraction alone cannot tell a thin page from a shell.
        let signal = tier_signal(&outcome, extracted.as_ref());
        let learned = signal.map(|signal| Learned {
            host: lease.key.host,
            signal,
            tier: lease.tier,
            probe: lease.probe,
        });

        // A challenge page is not a fetch. It keeps no row, so it is never
        // published and never counted as content, and it goes back as a
        // failure so that the host backs off and the url is tried again later
        // rather than being marked crawled. Doc 05.8 is explicit that a 200
        // with a challenge in it is not a success, and treating it as one is
        // how a corpus ends up with a million copies of the same interstitial
        // and a frontier that believes those pages are done.
        if signal == Some(TierSignal::Blocked) && matches!(outcome, Outcome::Ok(_)) {
            let status = match &outcome {
                Outcome::Ok(page) => Some(page.status),
                _ => None,
            };
            let result = FetchResult::Failed {
                status,
                kind: umi_state::FailureKind::Blocked,
            };
            let mut out = Fetched::answered(&lease, fetched_at_ms, result).paced(pace);
            out.signal = learned;
            return out;
        }

        // The second half, which needed the parse. A page filtered out by
        // language keeps no row and no links, the same as one filtered out by
        // type, because the crawl asked not to have it.
        if let Some(e) = extracted.as_ref()
            && !filter.accepts_lang(e.meta.declared_lang.as_deref())
        {
            let reason = umi_state::ExcludeReason::ContentType;
            return Fetched::excluded(&lease, fetched_at_ms, reason).paced(pace);
        }

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
            crawl_profile: self.config.scope.id,
        });

        // Doc 11.4: a page that says nofollow keeps its own row and gives up
        // its links. Depth is the page's plus one, and the ceiling is applied
        // here rather than at admit time so that a page at the limit does not
        // cost a batch of candidates that all get rejected.
        let limit = self.config.depth_limit();
        let followable: Vec<&str> = extracted
            .as_ref()
            .filter(|e| !e.robots.nofollow)
            .filter(|_| lease.depth < limit)
            .map(|e| {
                e.links
                    .links
                    .iter()
                    .filter(|l| !l.rel.has(umi_extract::Rel::NOFOLLOW))
                    .map(|l| l.url.as_str())
                    .collect()
            })
            .unwrap_or_default();

        // Counted before the scope runs, so a report can say how many links a
        // page offered as well as how many went in. Without the split the two
        // numbers are the same number and a focused crawl looks like it is
        // reading pages with no links on them.
        let links_seen = followable.len();
        let links = followable
            .into_iter()
            .filter_map(|url| self.follow(url, lease.depth, limit))
            .collect();

        let outcome = FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: fetched_at_ms,
            tier_used: lease.tier,
            result: result_of(&row, &outcome),
            pace,
        };
        Fetched {
            row: Some(row),
            outcome,
            links,
            links_seen,
            disallowed: false,
            signal: learned,
        }
    }

    /// Doc 13.2's link policy: whether to enqueue one link, and at what depth.
    ///
    /// `None` drops it. The link is still in doc 10.5's `links` column either
    /// way, because that column is what the page said and a scope does not
    /// change that.
    ///
    /// The general crawl never parses anything here. An empty include list with
    /// an empty exclude list admits every URL, and at 140 links a page and 250
    /// pages a second that is 35000 parses a second bought for nothing.
    fn follow(&self, url: &str, depth: u8, limit: u8) -> Option<(String, u8)> {
        let next = depth.saturating_add(1);
        let scope = &self.config.scope;
        if !scope.filters_links() {
            return Some((url.to_owned(), next));
        }
        if scope.allows(url) {
            return Some((url.to_owned(), next));
        }
        match scope.link_policy {
            LinkPolicy::InScopeOnly | LinkPolicy::RecordOutOfScope => None,
            // Admitted at the ceiling, so the depth check above drops whatever
            // it links to. One hop is expressed in the depth the frontier
            // already carries rather than in a second field that every backend
            // would then have to store and every query would have to know
            // about.
            LinkPolicy::OneHop => Some((url.to_owned(), limit)),
        }
    }

    /// Put the links found this tick into the frontier.
    ///
    /// Grouped by depth, because [`Frontier::admit_at`] takes one depth for a
    /// batch and doc 13.2's one hop policy admits an out of scope link at the
    /// ceiling rather than at the parent's depth plus one. A general crawl has
    /// one group and a one hop crawl has two, so the grouping is a partition of
    /// a vector rather than anything that shows up in a profile.
    ///
    /// Through the frontier rather than straight to the store, so that a domain
    /// we have just discovered starts being scheduled now instead of at the
    /// next restart, and so that a link gets doc 09.7's depth score rather than
    /// the default priority every other link also has.
    async fn admit(&self, links: &[(String, u8)], now_ms: u64) -> Result<usize, CrawlError> {
        let mut by_depth: Vec<(u8, Vec<&str>)> = Vec::new();
        for (url, depth) in links {
            match by_depth.iter_mut().find(|(d, _)| d == depth) {
                Some((_, group)) => group.push(url),
                None => by_depth.push((*depth, vec![url])),
            }
        }

        let mut admitted = 0;
        for (depth, group) in by_depth {
            for chunk in group.chunks(umi_state::BATCH) {
                let report = self
                    .frontier
                    .admit_at(chunk, depth, now_ms, Discovery::Trusted)
                    .await?;
                admitted += report.admitted.admitted as usize;
            }
        }
        Ok(admitted)
    }
}

/// One finished lease.
struct Fetched {
    row: Option<PageRow>,
    outcome: FetchOutcome,
    links: Vec<(String, u8)>,
    links_seen: usize,
    disallowed: bool,
    /// What the answer said about the tier it came back on, when it said
    /// anything. A robots exclusion and a malformed URL say nothing, because
    /// no request was sent.
    signal: Option<Learned>,
}

/// What one tick learned about one host's ladder.
///
/// A tick can cover a host several times, so this is folded rather than
/// collected: one host is one entry, and the worst news wins. A tick that saw
/// four pages and one challenge from a host learned that the host is
/// challenging us, and recording that as four successes and a block in some
/// order would make the answer depend on which fetch finished first.
struct Learned {
    host: umi_types::HostId,
    signal: TierSignal,
    /// The tier the fetch that produced `signal` ran at.
    tier: Tier,
    /// Whether that fetch was doc 05.8's probe at a cheaper tier.
    probe: bool,
}

impl Learned {
    /// How bad the news is, for the fold. Higher wins.
    const fn weight(signal: TierSignal) -> u8 {
        match signal {
            TierSignal::Success => 0,
            TierSignal::Shell => 1,
            TierSignal::Blocked => 2,
        }
    }

    /// Whether this is worth reading the host record for.
    ///
    /// The answer is no for essentially every page of a healthy crawl, and
    /// that is the point. A success at T0 or T1 on a host that was not being
    /// probed can only have come from a host whose ladder is already at the
    /// bottom, because the lease query does not offer a T1 lease for a host
    /// that wants more than T1. There is nothing to learn from it and nothing
    /// to write, so it never becomes a lookup.
    const fn teaches(&self) -> bool {
        match self.signal {
            TierSignal::Success => self.probe || self.tier as u8 > Tier::Plain as u8,
            TierSignal::Blocked | TierSignal::Shell => true,
        }
    }
}

/// Fold one answer into what the tick knows about that host.
fn learn(signals: &mut Vec<Learned>, learned: Learned) {
    let Some(seen) = signals.iter_mut().find(|l| l.host == learned.host) else {
        signals.push(learned);
        return;
    };
    if Learned::weight(learned.signal) > Learned::weight(seen.signal) {
        *seen = learned;
    }
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
                // No request was made, so there is nothing for doc 07.6's rate
                // limiter to read. The two callers that did make one put it
                // back with [`Fetched::paced`].
                pace: Pace::default(),
            },
            links: Vec::new(),
            links_seen: 0,
            disallowed: false,
            signal: None,
        }
    }

    /// Attach what the origin did, for the exclusions that happen after the
    /// fetch rather than before it.
    ///
    /// Doc 13.2's content and language filters both run on a response that
    /// already arrived. The row is dropped because the crawl asked not to have
    /// it, and the request still happened, so the politeness timer still moves.
    fn paced(mut self, pace: Pace) -> Self {
        self.outcome.pace = pace;
        self
    }
}

/// Doc 05.8's four way split, done where the fetch and the extraction meet.
///
/// The issue behind this is worth restating: a 403 from an origin, a 403 from
/// a CDN, a 200 with a challenge page in it and a 200 with a real page in it
/// are four different things, and only the last one is success. The first two
/// are already told apart in `umi-fetch`, which reads the vendor headers and
/// the body of a refusal. The last two are told apart here, because the only
/// thing that separates them is how much text came out.
///
/// `None` means the answer says nothing about the ladder. A 404, a timeout and
/// a DNS failure are all facts about the url or the network and none of them
/// is a fact about the tier, and treating them as one would escalate a host to
/// a browser because its hosting provider had a bad afternoon.
fn tier_signal(
    outcome: &Outcome,
    extracted: Option<&umi_extract::Extracted>,
) -> Option<TierSignal> {
    match outcome {
        Outcome::Failed {
            failure: umi_fetch::Failure::Blocked,
            ..
        } => Some(TierSignal::Blocked),
        // A 304 held, which means the conditional request got through, which
        // means the tier we used works.
        Outcome::NotModified { .. } => Some(TierSignal::Success),
        Outcome::Ok(page) => {
            // Bytes rather than characters, which doc 05.8 says. They are the
            // same number for the interstitials, which are English and ASCII,
            // and where they differ the byte count is the larger, so a page
            // that is 200 bytes of one non Latin script is not called a
            // challenge on a technicality.
            let text = extracted.map_or(0, |e| e.signals.text_bytes) as usize;
            umi_fetch::challenge::read_ok(page.body.as_ref(), text).or(Some(TierSignal::Success))
        }
        _ => None,
    }
}

/// What one fetch looked like to doc 07.6's rate limiter.
///
/// The latency comes from the fetcher's own [`Instant`](std::time::Instant)
/// when there is one, because that is monotonic and the crawl clock is not.
/// The fallback is the wall clock either side of the call, which is what a
/// failure leaves us with: a connection that never opened has no elapsed time
/// of its own, and how long we waited to find that out is the honest number.
fn pace_of(outcome: &Outcome, started_ms: u64, finished_ms: u64) -> Pace {
    let measured = match outcome {
        Outcome::Ok(page) => Some(page.elapsed),
        Outcome::NotModified { elapsed, .. } => Some(*elapsed),
        _ => None,
    };
    let latency_ms = measured.map_or_else(
        || finished_ms.saturating_sub(started_ms),
        |elapsed| elapsed.as_millis().try_into().unwrap_or(u64::MAX),
    );
    let retry_after = match outcome {
        Outcome::Failed { retry_after, .. } => *retry_after,
        _ => None,
    };
    Pace {
        latency_ms: Some(u32::try_from(latency_ms).unwrap_or(u32::MAX)),
        retry_after_ms: retry_after.map(|after| after.ms_from(finished_ms)),
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
