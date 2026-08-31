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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use umi_extract::extract;
use umi_fetch::Outcome;
use umi_frontier::{Ask, Config as FrontierConfig, Frontier, Rate};
use umi_robots::Provenance;
use umi_state::{
    Budget, Discovery, FetchOutcome, FetchResult, LeaseId, NackReason, Pace, RobotsRef, State,
    StateError,
};
use umi_types::{FetcherId, HostId, Revalidator, Tier, TierSignal, Verification};

use crate::backpressure::Allowance;
use crate::clock::Clock;
use crate::fetch::Fetch;
use crate::page::{Crawled, PageRow};
use crate::render::{RenderBudget, RenderPolicy, Slot};
use crate::robots::{Entry as RobotsEntry, RobotsCache};
use crate::scope::{LinkPolicy, Scope};

/// How big a tick has to be before its T2 share is worth an alert.
///
/// Doc 05.9's line is 15 percent of volume, and on a tick of twelve leases that
/// is two pages. The floor is here so that the alert means what it says: a
/// vendor changed a default rule, rather than a small crawl visiting a couple
/// of sites that have always wanted T2.
const ALERT_FLOOR: usize = 100;

/// The earliest a host may be asked again, for the leases a tick is still
/// holding.
///
/// Doc 07.6's delay is applied when a batch is leased, so a batch of one
/// host's urls comes out of the store already spaced. That is the right place
/// for a rate, which is a running estimate and belongs to the next decision.
/// It is the wrong place for one thing: `Retry-After` is not an estimate, it
/// is an origin naming a time, and a batch that was spaced a second apart
/// before the origin said "six seconds" has six requests already scheduled
/// inside the window it just asked us to leave empty. Measured from the
/// outside on gate 2.3's origin: a 429 asking for six seconds was followed by
/// another request 960 ms later, from the same batch.
///
/// So the ask is written here as well, and every lease in flight checks it
/// before it goes out. It lives for one tick because the next tick leases
/// against a host record the state layer has already moved.
#[derive(Debug, Default)]
struct HostFloors(Mutex<HashMap<HostId, u64>>);

impl HostFloors {
    /// The floor for one host, or zero for a host nobody has asked us to wait
    /// for, which is almost all of them.
    fn get(&self, host: HostId) -> u64 {
        self.lock().get(&host).copied().unwrap_or(0)
    }

    /// Move a host's floor out, never in. Two origins behind one host name
    /// disagreeing about the wait is not a thing to average.
    fn push(&self, host: HostId, at_ms: u64) {
        let mut floors = self.lock();
        let slot = floors.entry(host).or_default();
        *slot = (*slot).max(at_ms);
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<HostId, u64>> {
        // Nothing under this lock can panic, and recovering the guard keeps a
        // poisoned map from taking the rest of the tick down.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

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
    /// One in how many 304s to check by fetching the page anyway.
    ///
    /// Doc 05.3's second trap is an origin that answers 304 when the content
    /// has changed, usually a misconfigured cache in front of it, and there is
    /// no way to notice it from the 304 itself. The sampled unconditional
    /// fetch is the only thing that catches it, and one in a hundred costs
    /// about a percent of the saving T0 exists for. Zero turns it off.
    pub audit_every: u32,
    /// Doc 05.9's ceiling on how much of a crawl may be rendered.
    ///
    /// The other half of that budget, the browser pool's own capacity, is
    /// asked for every tick rather than configured. See [`crate::render`].
    pub render: RenderPolicy,
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
            // Doc 05.3's 1 percent.
            audit_every: 100,
            render: RenderPolicy::default(),
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
    /// Fetches that came back with a full body carrying the text we already
    /// had, so no row was written.
    ///
    /// The expensive version of `not_modified`, and the ratio between the two
    /// is what doc 05.3's first trap looks like from the outside. A number
    /// that climbs means origins are ignoring the validators we send, which
    /// costs bandwidth and nothing else: the pages are not lost, they are the
    /// pages we already have.
    pub unchanged: usize,
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
    /// Leases fetched at T2, for doc 05.9's 15 percent alert.
    pub emulated: usize,
    /// Leases fetched at T3 or above.
    ///
    /// Counted where doc 05.9's budget grants the slot, which is before the
    /// lease finds out whether it has to spend itself on the host's
    /// robots.txt. One that does gives the slot back and this number is a
    /// render high for that tick, once per host per day.
    pub rendered: usize,
    /// Leases given back to the frontier without an answer.
    ///
    /// Two things end up here. Doc 05.9's render budget turns some T3 leases
    /// away, and the lease that has to fetch a host's robots.txt spends its
    /// slot on that file instead of on its page. Both keep their due time and
    /// their priority and the next tick offers the most important of them
    /// again. They are not failures and no row exists for them, which is why
    /// they are counted apart from everything else.
    pub deferred: usize,
    /// Whether doc 15.3's ladder is what kept this tick small.
    ///
    /// A caller has no other way to tell a tick that leased nothing because
    /// the frontier was empty from one that leased nothing because the disk
    /// is full, and the two want opposite responses: the first one means the
    /// crawl is over and the second one means wait.
    pub restrained: bool,
}

impl TickReport {
    /// Whether the tick found nothing to do, which is how a caller knows to
    /// sleep rather than spin.
    #[must_use]
    pub const fn idle(&self) -> bool {
        self.leased == 0
    }
}

/// Apply doc 15.3's lease scale to a count.
///
/// Zero in gives zero out and so does a scale of zero, and anything in between
/// gives at least one. The floor is there because the scale is a rate cut and
/// not a stop: a batch of one under a scale of a hundredth is a crawl that is
/// still moving, and rounding it to zero would turn every rung into rung three.
fn scale(count: u32, scale: f32) -> u32 {
    if count == 0 || scale <= 0.0 {
        return 0;
    }
    if scale >= 1.0 {
        return count;
    }
    let scaled = f64::from(count) * f64::from(scale);
    (scaled as u32).max(1).min(count)
}

/// The loop.
pub struct Crawler<F, C> {
    fetch: F,
    frontier: Frontier<Arc<dyn State>>,
    clock: C,
    robots: RobotsCache,
    render: RenderBudget,
    config: CrawlConfig,
    /// Doc 15.3's answer to the last set of signals.
    ///
    /// A lock rather than a field on the config because the config is what the
    /// operator asked for and this is what the box can afford, and the two
    /// have to stay apart: pressure comes and goes, and a ladder that edited
    /// the config would leave the crawl permanently smaller than it was
    /// started with once the disk emptied again.
    restraint: Mutex<Allowance>,
}

impl<F: Fetch, C: Clock> Crawler<F, C> {
    /// Build a crawler.
    #[must_use]
    pub fn new(fetch: F, state: Arc<dyn State>, clock: C, config: CrawlConfig) -> Self {
        let frontier = Frontier::new(state, config.frontier());
        let render = RenderBudget::new(config.render);
        Self {
            fetch,
            frontier,
            clock,
            robots: RobotsCache::new(),
            render,
            config,
            restraint: Mutex::new(Allowance::default()),
        }
    }

    /// Tell the loop what doc 15.3's ladder currently allows.
    ///
    /// Takes effect on the next tick and not on the one in flight, which is
    /// the right granularity: the fetches already on the wire cost bandwidth
    /// that has been spent and bytes that have to be written somewhere either
    /// way, and abandoning them would waste both without freeing anything.
    pub fn restrain(&self, allowance: Allowance) {
        *self.restraint() = allowance;
    }

    /// What the ladder allows right now.
    #[must_use]
    pub fn allowance(&self) -> Allowance {
        *self.restraint()
    }

    fn restraint(&self) -> MutexGuard<'_, Allowance> {
        // An `Allowance` is nine `Copy` fields and nothing under this lock can
        // panic, so recovering the guard is recovering from a panic somewhere
        // else rather than papering over one here.
        self.restraint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Doc 05.9's render budget, so a caller can report what it is doing.
    #[must_use]
    pub const fn render(&self) -> &RenderBudget {
        &self.render
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

    /// Seed from an origin's own sitemaps, per doc 13.6.
    ///
    /// Call it with the origins the seeds are on, before the first
    /// [`tick`](Self::tick). It is the difference between starting a site at
    /// its front page and starting it with everything the site says it has, and
    /// on a site whose pages are behind a search form it is the only way to
    /// find them at all.
    ///
    /// This makes requests. They go through the same robots decision and the
    /// same politeness delay a page fetch does, but not through the frontier,
    /// because a sitemap is not a page: there is no row to write and nothing to
    /// publish, and putting it in the ledger would mean a sitemap turning up in
    /// the corpus. What it costs is bounded by
    /// [`SitemapLimits`](crate::sitemap::SitemapLimits).
    ///
    /// # Errors
    ///
    /// [`CrawlError::State`] if the URLs could not be admitted. A sitemap that
    /// is missing, unparseable or disallowed is not an error and is in the
    /// report.
    pub async fn seed_from_sitemaps(
        &self,
        origin: &str,
        limits: crate::sitemap::SitemapLimits,
    ) -> Result<crate::sitemap::SitemapReport, CrawlError> {
        Ok(crate::sitemap::discover(
            &self.fetch,
            &self.clock,
            &self.robots,
            &self.frontier,
            origin,
            limits,
        )
        .await?)
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

        // Every tick, because the pool's measurement of itself moves as it
        // renders and because a browser can come and go under a long running
        // daemon. It is a load of eight atomics and a division.
        self.render.observe(self.fetch.render_capacity());

        let now_ms = self.clock.now_ms();
        // Doc 15.3, read once and used for the whole tick, so that a ladder
        // that moves halfway through does not leave a batch leased under one
        // set of rules and fetched under another.
        let allowance = self.allowance();
        let batch = scale(self.config.batch, allowance.lease_scale);
        if batch == 0 {
            // Rung three: lease nothing. Not the same as stopping, and the
            // caller is told which of the two this is because an idle report
            // on its own reads as a finished crawl.
            return Ok(TickReport {
                restrained: true,
                ..TickReport::default()
            });
        }
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
                max_urls: batch,
                // The lower of the two, always. The config is the most this
                // process will ever pay for and the allowance is the most it
                // can afford today, and neither may raise the other.
                max_tier: self.config.max_tier.min(allowance.max_tier),
                budget: Some(allowance.budget(self.config.budget)),
            })
            .await?;

        let mut report = TickReport {
            leased: leases.len(),
            restrained: batch < self.config.batch,
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

        // Before `pending`, because the futures in it borrow this and the
        // borrow has to outlive them.
        let floors = HostFloors::default();
        let mut pending = FuturesUnordered::new();
        let mut queue = leases.into_iter();
        let mut rows = Vec::with_capacity(report.leased);
        let mut outcomes = Vec::with_capacity(report.leased);
        let mut candidates: Vec<(String, u8)> = Vec::new();
        let mut signals: Vec<Learned> = Vec::new();
        let mut deferred: Vec<LeaseId> = Vec::new();

        // Fill the window, then top it up as each one lands, which is what
        // keeps every slot busy rather than waiting on the slowest of a chunk.
        // Scaled with the batch, because a window as wide as the unrestrained
        // one over a batch half the size just means every lease is on the wire
        // at once, which is the rate the ladder was trying to cut.
        let window = scale(
            u32::try_from(self.config.in_flight).unwrap_or(u32::MAX),
            allowance.lease_scale,
        ) as usize;
        for _ in 0..window {
            let Some((lease, at)) = self.gate(&mut queue, &mut deferred, &mut report) else {
                break;
            };
            pending.push(self.one(lease, at, &floors));
        }
        while let Some(done) = pending.next().await {
            // Before the lease that replaces it, and before anything else this
            // loop does, so that a `Retry-After` on this answer reaches the
            // rest of the batch rather than the request after next.
            if let Some(after) = done.outcome.pace.retry_after_ms {
                floors.push(
                    done.outcome.key.host,
                    done.outcome.finished_ms.saturating_add(u64::from(after)),
                );
            }
            if let Some((lease, at)) = self.gate(&mut queue, &mut deferred, &mut report) {
                pending.push(self.one(lease, at, &floors));
            }
            let Fetched {
                row,
                outcome,
                links,
                links_seen,
                disallowed,
                unchanged,
                give_back,
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
                if learned.signal == Some(TierSignal::Blocked) && row.is_none() {
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
            // After the signal, because a lease that spent its slot on
            // robots.txt still learned what the file said and that is the one
            // lease a day that knows it. Before everything else, because there
            // is no answer here to record: the url keeps its due time and the
            // next tick offers it again.
            if give_back {
                deferred.push(outcome.lease);
                continue;
            }
            // Counted off the completion rather than off the row, because a
            // 304 has no row. Everything else does.
            if matches!(outcome.result, umi_state::FetchResult::NotModified { .. }) {
                report.not_modified += 1;
            }
            if let Some(row) = row {
                match row.outcome {
                    umi_types::OutcomeCode::Ok => report.fetched += 1,
                    _ => report.failed += 1,
                }
                report.bytes_fetched += u64::from(row.content_length);
                report.links_seen += links_seen;
                candidates.extend(links);
                // The bytes and the links are counted either way, because we
                // paid for both. Only the row is dropped, and only when it
                // would be the second copy of one already published.
                if unchanged {
                    report.unchanged += 1;
                } else {
                    rows.push(row);
                }
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

        // After the completions rather than before, so that a store that
        // refuses the release still keeps the answers of everything this tick
        // did fetch. A deferred lease that is never released expires on its own
        // and comes back anyway, a lease time later.
        //
        // `Refused` and not `Expired`, because that is what happened: doc 08.3
        // says `Refused` is a fetcher declining work it cannot do, the URL is
        // rescheduled at the due time it already had, and `fail_streak` is
        // untouched. A page the fleet has no browser for has not failed.
        //
        // The frontier charged the domain for these when it leased them and
        // the charge is not given back. That is deliberate and it is the safe
        // direction: doc 09.3's cap is a promise to the operator about how
        // often we turn up, and the alternative is a crawl that defers a lot
        // of rendering and spends the returned budget on that domain instead.
        report.deferred = deferred.len();
        if !deferred.is_empty() {
            self.state().release(&deferred, NackReason::Refused).await?;
        }
        self.alert(&report);

        // After the completions, because both write the host record and the
        // completion is the one that owns doc 07.6's pacing columns. Reading
        // first would mean writing back a politeness timer from before this
        // tick moved it.
        report.learned = self.relearn(&signals, now_ms).await?;
        report.links_admitted = self.admit(&candidates, now_ms).await?;
        Ok(report)
    }

    /// The next lease this tick can actually run, and the earliest moment it
    /// may start.
    ///
    /// Everything below T3 comes straight back with its politeness time. A T3
    /// lease has to get past doc 05.9's budget first, and one that cannot is
    /// taken off the queue, collected for release and skipped, so a tick that
    /// leased more rendering than the fleet has browser for still fills its
    /// window with the work it can do.
    ///
    /// Skipping rather than waiting is the whole point. The budget is global,
    /// so a tick that blocked on it would hold in flight slots open for pages
    /// that a different tick will be better placed to run.
    fn gate(
        &self,
        queue: &mut impl Iterator<Item = umi_state::Lease>,
        deferred: &mut Vec<LeaseId>,
        report: &mut TickReport,
    ) -> Option<(umi_state::Lease, u64)> {
        for lease in queue.by_ref() {
            let due = lease.not_before_ms;
            if lease.tier < Tier::Rendered {
                if lease.tier == Tier::Emulated {
                    report.emulated += 1;
                }
                return Some((lease, due));
            }
            match self.render.take(self.clock.now_ms()) {
                // A fleet with no browser at all serves the rung it does have,
                // for the reason on `Slot::NoBrowser`. It is not counted as a
                // render, because nothing rendered.
                Slot::NoBrowser => return Some((lease, due)),
                Slot::At(at) => {
                    report.rendered += 1;
                    // The later of the two waits. Politeness is owed to the
                    // origin and the budget is owed to the browser, and a page
                    // that satisfies one of them early has not satisfied the
                    // other.
                    return Some((lease, due.max(at)));
                }
                Slot::Defer => deferred.push(lease.id),
            }
        }
        None
    }

    /// Doc 05.9's T2 alert.
    ///
    /// An alert and not a throttle, because a T2 share this high usually means
    /// a vendor changed a default rule rather than that anything here is wrong,
    /// and the useful response is a person looking at it. Nothing is throttled
    /// and nothing is deferred.
    fn alert(&self, report: &TickReport) {
        if report.leased < ALERT_FLOOR {
            return;
        }
        let share = report.emulated as f64 / report.leased as f64;
        let ceiling = self.config.render.max_emulated;
        if share > ceiling {
            tracing::warn!(
                emulated = report.emulated,
                leased = report.leased,
                share = format!("{:.1}%", share * 100.0),
                ceiling = format!("{:.1}%", ceiling * 100.0),
                "T2 is over doc 05.9's share, which usually means a vendor changed a rule"
            );
        }
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
            let mut changed = match learned.signal {
                Some(signal) => host.tier.observe(signal, learned.tier, now_ms),
                None => false,
            };

            // Doc 05.3's two verdicts. Both of them cost the host its T0, and
            // both are applied before the ladder's backoff below so that a
            // host that is both lying and blocking gets one write.
            for _ in 0..learned.weak {
                changed |= host.tier.saw_full_body();
            }
            if learned.lie {
                changed |= host.tier.saw_lie();
            }

            // Everything robots.txt taught, from the one lease a day that
            // fetched the file.
            //
            // The reference goes down whatever the fetch did, because it is
            // the record that we asked. Without it a coordinator that restarts
            // has no idea which hosts it already has a fresh answer for and
            // refetches robots.txt for every host it touches, which at a few
            // thousand hosts an hour is a few thousand requests nobody needed,
            // sent to origins that get nothing out of them.
            if let Some(facts) = &learned.robots {
                if host.robots.as_ref() != Some(&facts.reference) {
                    host.robots = Some(facts.reference);
                    changed = true;
                }
                if let Some(parsed) = &facts.parsed {
                    // Doc 07.4's crawl delay. The pacer takes the larger of
                    // this and doc 07.6's adaptive delay, so a site asking for
                    // less than we were already giving it changes nothing and
                    // a site asking for more is honoured.
                    if host.crawl_delay_ms != parsed.crawl_delay_ms {
                        host.crawl_delay_ms = parsed.crawl_delay_ms;
                        changed = true;
                    }
                    // Doc 13.6's sitemap lines. Read during seeding and then
                    // dropped, so a later crawl of the same host had no record
                    // that the site had told us where its sitemap is.
                    if host.sitemaps != parsed.sitemaps {
                        host.sitemaps.clone_from(&parsed.sitemaps);
                        changed = true;
                    }
                }
            }

            if learned.signal == Some(TierSignal::Blocked) {
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
    ///
    /// `start_ms` is when it may go out, which is its politeness time and, for
    /// a rendered page, the render slot doc 05.9's budget gave it.
    async fn one(&self, lease: umi_state::Lease, start_ms: u64, floors: &HostFloors) -> Fetched {
        // Doc 07.6, and the whole reason `not_before_ms` exists. The state
        // layer already spaced the leases of a batch by each host's politeness
        // delay, and until this line the loop threw that away and sent the
        // batch as fast as the window allowed. A crawl asking for one request a
        // second put four on blog.rust-lang.org inside 138 ms, which is the
        // kind of mistake the origin sees and we do not.
        //
        // Before the robots check rather than after, since robots.txt is a
        // request to the same host and counts the same way.
        //
        // A loop and not one sleep, because the wait can grow while it is
        // being served. This batch was spaced at lease time and an origin that
        // answers one of it with `Retry-After` is asking for the rest to move,
        // which is what [`HostFloors`] carries. Waking up to find the wait
        // longer than it was is the ordinary case for that, not an error.
        let mut until = start_ms;
        loop {
            self.clock.sleep_until_ms(until).await;
            let floor = floors.get(lease.key.host);
            if floor <= self.clock.now_ms() {
                break;
            }
            until = floor;
        }

        let now = || self.clock.now_ms();
        let Some(origin) = origin_of(&lease.url) else {
            return Fetched::malformed(&lease, now());
        };

        let (decision, entry, robots_fetched) = self
            .robots
            .decide(
                &self.fetch,
                lease.key.host,
                &origin,
                &lease.url,
                lease.tier,
                now(),
            )
            .await;
        // Only the lease that paid for the fetch writes anything down. Every
        // other lease on this host today read the same entry out of the cache
        // and has nothing to say the host record does not already hold.
        let robots = robots_fetched.then(|| RobotsFacts::of(&entry));
        // Doc 07.6, and the reason the sleep above is before the robots check
        // rather than after it: robots.txt is a request to the same host and
        // counts the same way. The state layer spaced this lease out for one
        // request, so the lease that fetched the file has spent its slot and
        // the page waits for the next one. The url keeps its due time and the
        // next tick offers it again, so this costs a tick per host per day and
        // no extra request at all.
        //
        // Measured from the outside before it was written: gate 2.3's origin
        // logged robots.txt and the front page six milliseconds apart.
        if robots_fetched {
            // A T3 lease was holding one of doc 05.9's render slots, taken
            // before anything here knew there was a robots.txt to fetch. No
            // browser ran, so the slot goes back. The guard is the tier and
            // not whether a slot was granted, because the one case that
            // reaches here without a grant is a process with no browser at
            // all, and `refund` on that process does nothing.
            if lease.tier >= Tier::Rendered {
                self.render.refund();
            }
            return Fetched::robots_cost(&lease, now()).taught(&lease, robots);
        }
        if !decision.is_allowed() {
            let mut out = Fetched::excluded(&lease, now(), umi_state::ExcludeReason::Robots);
            out.disallowed = true;
            return out.taught(&lease, robots);
        }

        let robots_checked_ms = entry.fetched_ms;
        let started_ms = now();
        let served = match self
            .fetch
            .fetch(&lease.url, lease.revalidate.as_ref(), lease.tier)
            .await
        {
            Ok(served) => served,
            Err(_) => return Fetched::malformed(&lease, now()).taught(&lease, robots),
        };
        // Doc 04.5's path, off the fetcher rather than off the lease. The two
        // differ whenever the ladder moved: a build with no browser serving a
        // T3 lease at T1, or a browser handing back something that is not a
        // document and the plain client finishing the job. Doc 05.5 publishes
        // this column as the record of what the web needs, so it has to be the
        // rungs that ran.
        let tier_path = served.path;
        let outcome = served.outcome;

        let (outcome, audited) = self.audit(&lease, outcome).await;
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
            return Fetched::excluded(&lease, fetched_at_ms, reason)
                .paced(pace)
                .taught(&lease, robots);
        }

        // Doc 05.3: a 304 is not a page. No extraction runs and no row is
        // written, only the ledger's last_checked and the next due time move.
        // A row here would be a copy of one we published already with the
        // content taken out, and at steady state most of a crawl is 304s, so
        // writing them would fill the corpus with them. The one 304 that does
        // go on past here is the audited one, which `audit` has already turned
        // back into the body it was hiding.
        if let Outcome::NotModified { revalidate, .. } = &outcome {
            let result = FetchResult::NotModified {
                status: 304,
                revalidate: revalidate.clone(),
            };
            let mut out = Fetched::answered(&lease, fetched_at_ms, result).paced(pace);
            out.signal = Some(Learned::tiered(
                lease.key.host,
                TierSignal::Success,
                lease.tier,
                lease.probe,
            ));
            return out.taught(&lease, robots);
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
        let learned =
            signal.map(|signal| Learned::tiered(lease.key.host, signal, lease.tier, lease.probe));

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
            return out.taught(&lease, robots);
        }

        // The second half, which needed the parse. A page filtered out by
        // language keeps no row and no links, the same as one filtered out by
        // type, because the crawl asked not to have it.
        if let Some(e) = extracted.as_ref()
            && !filter.accepts_lang(e.meta.declared_lang.as_deref())
        {
            let reason = umi_state::ExcludeReason::ContentType;
            return Fetched::excluded(&lease, fetched_at_ms, reason)
                .paced(pace)
                .taught(&lease, robots);
        }

        // Doc 07.5, both halves of it. AIPREF attaches a preference two ways,
        // a `Content-Usage` line in robots.txt and a `Content-Usage` response
        // header, and a site can use either or both. They are reconciled by
        // the vocab draft's rule, which is most restrictive wins rather than
        // most specific wins, and rendered once so that a reader building a
        // training set filters on one column with one predicate.
        //
        // Against the lease URL rather than the final one, because that is the
        // URL robots.txt was checked for and the URL this row is filed under.
        let mut usage = entry.robots.usage_for_url(&lease.url);
        if let Outcome::Ok(page) = &outcome
            && let Some((_, value)) = page
                .headers_kept
                .iter()
                .find(|(name, _)| name == "content-usage")
        {
            usage.merge(&umi_robots::Usage::parse(value));
        }
        let content_usage = usage.render();

        let row = PageRow::build(&Crawled {
            url: &lease.url,
            keys: lease.key,
            host: &host,
            fetched_at_ms,
            outcome: &outcome,
            extracted: extracted.as_ref(),
            tier_used: tier_path.used(),
            tier_path: tier_path.as_slice(),
            robots_checked_ms,
            content_usage: content_usage.as_deref(),
            fetcher_id: self.config.fetcher,
            verification: Verification::Local,
            crawl_profile: self.config.scope.id,
        });

        // Doc 11.4: a page that says nofollow keeps its own row and gives up
        // its links. Depth is the page's plus one, and the ceiling is applied
        // here rather than at admit time so that a page at the limit does not
        // cost a batch of candidates that all get rejected.
        //
        // The filter is `is_page` and nothing else. A link's own `rel=nofollow`
        // is deliberately not consulted, which doc 11.4 states and the extract
        // crate's module docs state again: it is a 2005 comment spam measure,
        // it has not meant anything about crawlability for a long time, and the
        // sites that stamp it on every outbound link are the ones with the most
        // outbound links worth having.
        let limit = self.config.depth_limit();
        let followable: Vec<&str> = extracted
            .as_ref()
            .filter(|e| !e.robots.nofollow)
            .filter(|_| lease.depth < limit)
            .map(|e| {
                e.links
                    .links
                    .iter()
                    .filter(|l| l.is_page())
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

        // Doc 05.3's two traps, both of which need the stored hash and the new
        // one side by side and so cannot be seen anywhere but here.
        let learned = match reval_signal(&lease, &row, audited) {
            None => learned,
            Some(reval) => {
                let mut learned = learned
                    .unwrap_or_else(|| Learned::nothing(lease.key.host, lease.tier, lease.probe));
                match reval {
                    Reval::Weak => learned.weak += 1,
                    Reval::Lie => learned.lie = true,
                }
                Some(learned)
            }
        };

        // The row we would be publishing is the row we published last time,
        // with a later timestamp on it. Doc 05.3 already refuses to write one
        // of these when the origin says 304, and an origin that says it with a
        // full body has not made it a different page. Gate 2.1's segments
        // carry 1,778 of them in 79,628 rows, which is a fifth of a percent of
        // the corpus that is a byte for byte copy of something already in it.
        //
        // The completion still goes back carrying the content hash, so the
        // freshness estimator counts an unchanged observation and the schedule
        // moves exactly as a 304 would have moved it.
        //
        // Text and not bytes, and only where there is text. Two fetches of a
        // PDF both extract to nothing, and treating that as evidence the file
        // has not moved would quietly stop recording every non HTML url after
        // its first visit. An audited fetch is left alone as well: that one
        // asked without a validator on purpose and its body is the record.
        let unchanged = !audited
            && row.outcome == umi_types::OutcomeCode::Ok
            && row.text_bytes > 0
            && lease.content_hash == Some(content_hash(&row));

        let outcome = FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: fetched_at_ms,
            // Also the rung that answered, which is what the field is
            // documented to hold. Not the same thing as `Learned::tier` a few
            // lines up: that one feeds doc 05.8's host ladder and has to stay
            // the tier the lease was for, because a favicon that T1 finished
            // after a browser refused it says nothing about whether this
            // host's pages still need a browser.
            tier_used: tier_path.used(),
            result: result_of(&row, &outcome),
            pace,
        };
        Fetched {
            row: Some(row),
            outcome,
            links,
            links_seen,
            disallowed: false,
            unchanged,
            give_back: false,
            signal: learned,
        }
        .taught(&lease, robots)
    }

    /// Doc 05.3's sampled check on a 304, which is the only way to catch an
    /// origin that answers one when the content has moved.
    ///
    /// The second request goes out a politeness delay after the first, not
    /// straight away. Doc 07.6 is one request per host per delay and it has no
    /// exception for a request we are making to check up on the host, so an
    /// audit that skipped the wait would be the crawler breaking its own rule
    /// in the course of enforcing one.
    ///
    /// A refetch that fails leaves the 304 standing. The audit is a check on
    /// the origin and a failed check is not evidence of anything.
    async fn audit(&self, lease: &umi_state::Lease, outcome: Outcome) -> (Outcome, bool) {
        let sampled = matches!(outcome, Outcome::NotModified { .. })
            && lease.content_hash.is_some()
            && self.sampled(lease);
        if !sampled {
            return (outcome, false);
        }

        let delay = u64::from(lease.delay_ms);
        self.clock
            .sleep_until_ms(self.clock.now_ms().saturating_add(delay))
            .await;
        // T1 and not the lease's tier, when the lease was T0. The whole point
        // of the audit is that this request carries no validator, and a tier
        // path saying T0 would put a conditional request in the published
        // column for a request that deliberately was not one.
        let tier = lease.tier.max(Tier::Plain);
        match self.fetch.fetch(&lease.url, None, tier).await {
            Ok(fresh) if matches!(fresh.outcome, Outcome::Ok(_)) => (fresh.outcome, true),
            _ => (outcome, false),
        }
    }

    /// Whether this lease is one of the sampled fraction.
    ///
    /// The url key and the attempt count together, so that the sample is a
    /// different set of urls every time round rather than the same one percent
    /// of the frontier for the life of the crawl. Deterministic, so a test can
    /// ask for all of them or none.
    fn sampled(&self, lease: &umi_state::Lease) -> bool {
        let every = u64::from(self.config.audit_every);
        if every == 0 {
            return false;
        }
        let mut head = [0u8; 8];
        head.copy_from_slice(&lease.key.url.as_bytes()[..8]);
        let mixed =
            u64::from_le_bytes(head) ^ u64::from(lease.attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        mixed % every == 0
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
    /// Whether the row is a copy of one we have already published.
    ///
    /// Doc 05.3 says a 304 writes no row, because a revalidation is an
    /// observation and not a new version. An origin that ignored the validator
    /// and spent a full body on text we already hold has said the same thing
    /// the expensive way, and this is where the row it would have left behind
    /// is dropped. Everything else the fetch produced is kept: the links go in,
    /// the completion goes back, and the host learns that its validators are
    /// worth nothing.
    unchanged: bool,
    /// Whether this lease is going back on the queue with no answer.
    ///
    /// One case, and it is doc 07.6's rate rather than doc 05.9's budget: this
    /// is the lease that found the host's robots.txt missing and fetched it.
    /// That file is a request to the same origin as the page, and the state
    /// layer spaced this lease out for one request, so sending the page as
    /// well would put two on the origin at once. Giving the lease back costs a
    /// tick and costs the origin nothing.
    give_back: bool,
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
    /// What the answer said about the ladder, when it said anything. A 404 and
    /// a timeout say nothing, but they can still carry a revalidation verdict.
    signal: Option<TierSignal>,
    /// The tier the fetch that produced `signal` ran at.
    tier: Tier,
    /// Whether that fetch was doc 05.8's probe at a cheaper tier.
    probe: bool,
    /// How many times in this tick the host answered a conditional request
    /// with a full body carrying content we already had. Doc 05.3's first
    /// trap, counted rather than flagged because it takes three of them.
    weak: u16,
    /// Whether the host was caught saying 304 about content that had moved.
    /// Doc 05.3's second trap, which takes one.
    lie: bool,
    /// What the host's robots.txt asked for, on the one lease a day that
    /// fetched the file. `None` on every other lease, including the ones that
    /// read the same file out of the cache, which is what keeps this off the
    /// per page bill.
    robots: Option<RobotsFacts>,
}

/// What one robots.txt fetch said about the host that owns it, beyond the
/// allow and disallow rules the cache already holds.
///
/// Doc 07.4 puts these in the host record rather than in the robots cache,
/// because doc 07.6's pacer reads the host record and never sees a parsed
/// file. Until this existed the pacer never saw a site's `Crawl-delay` at all,
/// so a site asking for one request every ten seconds got one every second,
/// which is the request the file was written to stop.
#[derive(Clone, PartialEq, Eq, Debug)]
struct RobotsFacts {
    /// Doc 08.3's `RobotsRef`: the digest, the two times and whether the
    /// origin actually answered.
    ///
    /// Written whatever the fetch did, unlike the parsed half below, because
    /// the fact being recorded is that we asked this host at this time and
    /// got this answer. That is exactly what a coordinator coming back from a
    /// restart needs to know, and it is what doc 07.7's rule about a
    /// `Disallow` that appears later compares against.
    reference: RobotsRef,
    /// The things only a real file can say. `None` on the three provenances
    /// that are not a parse.
    ///
    /// A 5xx disallows the host for a day under RFC 9309 2.3.1.4 and carries
    /// no delay and no sitemaps, and writing its emptiness into the host
    /// record would throw away numbers the site published, so that the day the
    /// file came back the site would be crawled at the default pace with no
    /// idea where its sitemap is.
    parsed: Option<ParsedFacts>,
}

/// The half of [`RobotsFacts`] that only a file the origin served can carry.
#[derive(Clone, PartialEq, Eq, Debug)]
struct ParsedFacts {
    /// The delay the file asked for, already clamped by umi-robots to doc
    /// 07.4's bounds of 100 ms to 300 s. `None` is a file with no
    /// `Crawl-delay` line on it, and it clears a delay the host used to have
    /// rather than leaving the old number in place, because a site that took
    /// the line out asked for exactly that.
    crawl_delay_ms: Option<u32>,
    /// The `Sitemap` lines, which doc 13.6 reads during seeding and which
    /// nothing used to keep. An empty list clears the stored one for the same
    /// reason the delay does.
    sitemaps: Vec<String>,
}

impl RobotsFacts {
    /// What a cache entry teaches about the host that owns it.
    fn of(entry: &RobotsEntry) -> Self {
        let provenance = entry.robots.provenance();
        Self {
            reference: RobotsRef {
                digest: entry.digest,
                fetched_ms: entry.fetched_ms,
                expires_ms: entry.expires_ms,
                // A 404 is authoritative. RFC 9309 2.3.1.3 says a site with no
                // robots.txt has published no restrictions, and that is an
                // answer rather than the absence of one. A 5xx and a timeout
                // are the absence of one: both disallow the host for a day,
                // and a coordinator that later has to decide whether it knows
                // this host's rules should be able to tell them apart from a
                // site that simply does not have a file.
                authoritative: matches!(provenance, Provenance::Parsed | Provenance::NotFound),
            },
            parsed: (provenance == Provenance::Parsed).then(|| ParsedFacts {
                crawl_delay_ms: entry
                    .robots
                    .crawl_delay()
                    .map(|d| u32::try_from(d.as_millis()).unwrap_or(u32::MAX)),
                sitemaps: entry.robots.sitemaps().to_vec(),
            }),
        }
    }
}

impl Learned {
    /// One answer that has taught nothing yet, which is where the paths that
    /// learn one thing at a time start.
    fn nothing(host: umi_types::HostId, tier: Tier, probe: bool) -> Self {
        Self {
            host,
            signal: None,
            tier,
            probe,
            weak: 0,
            lie: false,
            robots: None,
        }
    }

    /// The tier half of one answer, with nothing learned about revalidation.
    fn tiered(host: umi_types::HostId, signal: TierSignal, tier: Tier, probe: bool) -> Self {
        Self {
            signal: Some(signal),
            ..Self::nothing(host, tier, probe)
        }
    }

    /// How bad the news is, for the fold. Higher wins.
    const fn weight(signal: Option<TierSignal>) -> u8 {
        match signal {
            None => 0,
            Some(TierSignal::Success) => 1,
            Some(TierSignal::Shell) => 2,
            Some(TierSignal::Blocked) => 3,
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
    fn teaches(&self) -> bool {
        if self.weak > 0 || self.lie || self.robots.is_some() {
            return true;
        }
        match self.signal {
            Some(TierSignal::Success) => self.probe || self.tier as u8 > Tier::Plain as u8,
            Some(TierSignal::Blocked | TierSignal::Shell) => true,
            None => false,
        }
    }
}

/// Fold one answer into what the tick knows about that host.
///
/// The two halves fold differently and have to. The tier signal is a verdict,
/// so the worst one in the tick wins and the rest are noise. The weak
/// revalidator count is evidence, so it adds up, because doc 05.3 asks for
/// three observations and a host that produced three of them inside one tick
/// has still produced three of them.
fn learn(signals: &mut Vec<Learned>, learned: Learned) {
    let Some(seen) = signals.iter_mut().find(|l| l.host == learned.host) else {
        signals.push(learned);
        return;
    };
    if Learned::weight(learned.signal) > Learned::weight(seen.signal) {
        seen.signal = learned.signal;
        seen.tier = learned.tier;
        seen.probe = learned.probe;
    }
    seen.weak = seen.weak.saturating_add(learned.weak);
    seen.lie |= learned.lie;
    // First one wins, and there is only ever one: the cache fetches a host's
    // robots.txt once and every other lease in the tick reads that entry.
    if seen.robots.is_none() {
        seen.robots = learned.robots;
    }
}

/// Which of doc 05.3's two traps this answer walked into, if either.
///
/// Both of them are a comparison between the body we just got and the digest
/// of the one we already had, which is why neither can be seen in the fetcher
/// or in the store. The fetcher does not know what we had and the store does
/// not see the body.
fn reval_signal(lease: &umi_state::Lease, row: &PageRow, audited: bool) -> Option<Reval> {
    let stored = lease.content_hash?;
    if row.outcome != umi_types::OutcomeCode::Ok {
        return None;
    }
    let same = content_hash(row) == stored;
    if audited {
        // We asked without a validator on purpose, so the body is the truth
        // and the 304 that preceded it was a claim about that truth.
        return (!same).then_some(Reval::Lie);
    }
    // We sent a validator and the origin spent a full body telling us what we
    // already had. One of those is a coincidence, three is a habit.
    (lease.revalidate.is_some() && same).then_some(Reval::Weak)
}

/// One host's answer to doc 05.3's question about whether it revalidates.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reval {
    /// Ignored a validator we sent and returned content we already had.
    Weak,
    /// Said 304 about content that had changed.
    Lie,
}

impl Fetched {
    /// A lease that was never fetched, because robots.txt said no or the URL
    /// was not usable. No row, because doc 10.5's `pages` stream is what was
    /// crawled and this was not.
    fn excluded(lease: &umi_state::Lease, now_ms: u64, reason: umi_state::ExcludeReason) -> Self {
        Self::answered(lease, now_ms, FetchResult::Excluded { reason })
    }

    /// A lease that spent its slot on robots.txt.
    ///
    /// The url is untouched and comes straight back, at the due time it
    /// already had, which the state layer has already moved past this
    /// request. Doc 08.3's `Refused` is what the release says, because that is
    /// what happened: the loop declined this work now and the page has not
    /// failed at anything.
    fn robots_cost(lease: &umi_state::Lease, now_ms: u64) -> Self {
        // The outcome is built and thrown away. `give_back` sends the lease
        // down the release path instead, and building one anyway keeps this
        // constructor the same shape as the others rather than making
        // `outcome` an `Option` that only one caller ever leaves empty.
        let mut out = Self::excluded(lease, now_ms, umi_state::ExcludeReason::Robots);
        out.give_back = true;
        out
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
            unchanged: false,
            give_back: false,
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

    /// Attach what this lease's robots.txt fetch taught about the host.
    ///
    /// Every exit out of [`Crawler::one`] past the robots check goes through
    /// here, including the ones that failed, because what the file said about
    /// the host is true whatever happened to the URL afterwards. A lease that
    /// read the file out of the cache passes `None` and this does nothing.
    fn taught(mut self, lease: &umi_state::Lease, robots: Option<RobotsFacts>) -> Self {
        if robots.is_some() {
            self.signal
                .get_or_insert_with(|| Learned::nothing(lease.key.host, lease.tier, lease.probe))
                .robots = robots;
        }
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
