//! Scheduling and politeness, which is the milestone 1 half of
//! `docs/spec/09-frontier-and-freshness.md`.
//!
//! The frontier decides what to fetch next. This crate is the part of that
//! decision which does not need a crawl to have already happened: the depth
//! term of doc 09.2's score, the per host next allowed fetch time from doc
//! 09.3, and the per pay level domain rate cap from the same section. The
//! change rate estimator in doc 09.4 and the refresh classes in doc 09.5 are
//! milestone 2, and the realtime path in doc 09.6 is milestone 3.
//!
//! # Where politeness is enforced
//!
//! At lease issue, and nowhere else. Doc 09.3 is explicit about this and the
//! reason is doc 05: the fetcher fleet is open, so anybody can run one, and a
//! rate limit that lives in the fetcher is a rate limit that holds only for
//! the fetchers we wrote. The coordinator owns the timers, a URL is not
//! offered until its host's timer has passed, and a fetcher that wants to be
//! rude has to be given something to be rude with first.
//!
//! Per host, the gap is `last_fetch + max(crawl_delay, adaptive_delay)`, and
//! that lives in [`umi_state`] because the timer is a column on the host row
//! and every backend has to apply it the same way. Per domain, the cap is 20
//! requests a second and it lives in [`bucket`] here, because it is a decision
//! about scheduling rather than a fact about a row, and because a backend that
//! had its own idea of the domain cap would make the crawl's manners depend on
//! which store an operator picked.
//!
//! # The loop
//!
//! [`Frontier::tick`] is doc 09.3's scheduler loop. It takes the domains whose
//! rate limit has room, in most overdue order, and leases from each of them in
//! turn. Leasing per domain rather than in one call is what makes the domain
//! cap enforceable, and it is also the shape doc 09.3 asks for: a warm shard is
//! expensive to get and worth draining, so the unit of work is a domain and not
//! a URL.
//!
//! A tick costs what it schedules and not what is resident, which is the one
//! thing in here the benchmark changed. It used to read the resident set and
//! walk it to keep the schedule in step, which is a fine thing to do at a
//! thousand domains and 22 percent of a 100 ms tick at a hundred thousand, and
//! more than a whole tick at a million. Doc 08.6 makes local disk a cache with
//! a large resident working set, so a hundred thousand domains is the ordinary
//! case rather than the extreme one. Domains reach the schedule from
//! [`Frontier::discover`] and leave it through [`Frontier::evict`], both of
//! which happen when something actually changed, and
//! [`Frontier::resume`] rebuilds the whole thing once at startup for doc 09.8.
//!
//! Warming is not here. Doc 09.3 puts it in a background task on purpose,
//! since a cold shard costs an object GET and doing that inside the loop would
//! cap the whole crawl at twenty domains a second, so `tick` skips a domain
//! that is not resident rather than waiting for it. A domain in the schedule
//! that is not resident costs one lease call that comes back empty, which is
//! why eviction goes through the frontier.

#![forbid(unsafe_code)]

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use umi_state::{
    AdmitReport, BATCH, Budget, Candidate, Discovery, EvictReport, Lease, LeaseRequest, Priority,
    Result, State,
};
use umi_types::{FetcherId, PldId, RowKey, Tier, canonicalize};

pub mod bucket;
pub mod priority;

#[cfg(test)]
mod tests;

pub use bucket::{Gate, Rate};
pub use priority::{MAX_DEPTH, MAX_DEPTH_SCORE, depth_score, within_depth};

/// How the scheduler is tuned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Config {
    /// The per pay level domain cap from doc 09.3.
    pub rate: Rate,
    /// How many domains one [`tick`](Frontier::tick) will visit, which is doc
    /// 09.3's `lease_batch`. Each one is a round trip to the state layer, so
    /// this bounds the work an ask can do rather than the work it will do.
    ///
    /// Per ask and not per crawl tick. A crawl tick asks several times as its
    /// fetch window drains, and the rate a domain is charged at is [`rate`],
    /// which is measured against the clock rather than counted per ask.
    ///
    /// [`rate`]: Self::rate
    pub max_domains: usize,
    /// How many URLs one [`tick`](Frontier::tick) will take from a single host.
    /// Doc 07.6 already holds a host to one request in flight, so this bounds
    /// the queue a fetcher carries and not the concurrency it may use.
    ///
    /// Per ask, for the same reason as [`max_domains`](Self::max_domains).
    pub max_per_host: u32,
    /// How long a lease is good for.
    pub lease_for: Duration,
    /// The link distance past which a URL is not admitted, from doc 09.7.
    pub max_depth: u8,
    /// How doc 09.5's refresh classes divide a tick's capacity.
    ///
    /// The default is the table in that section. Doc 16 raises the discovery
    /// share during the initial land grab and lowers it once coverage is
    /// respectable, which is a change to this and to nothing else.
    pub budget: Budget,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rate: Rate::default(),
            // One domain per URL in a fetcher's batch, because on a broad
            // crawl that is the ratio. The old number here was 64, on the
            // reasoning that a batch of 512 needs 64 domains offering 8 URLs
            // each. That holds for a focused crawl of a few large sites and is
            // wrong everywhere else: a broad crawl reaches millions of
            // domains, almost all of which have one URL due and the rest of
            // their frontier inside a politeness window, so 64 domains is a
            // tick of 64 URLs however big the batch is. Doc 16's gate 3.1 asks
            // for 250 pages a second and doc 07.6 holds a host to one request
            // at a time, so a tick that visits 64 domains cannot reach the
            // number by any amount of concurrency.
            max_domains: 512,
            max_per_host: 8,
            lease_for: Duration::from_secs(60),
            max_depth: MAX_DEPTH,
            budget: Budget::DEFAULT,
        }
    }
}

/// What one fetcher is asking for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Ask {
    /// Who the work is for, recorded on every lease so a completion can be
    /// attributed.
    pub fetcher: FetcherId,
    /// The caller's notion of now, in milliseconds since the Unix epoch.
    /// Nothing in this crate reads a clock, for the reason in doc 08: a
    /// scheduler that reads its own clock cannot be replayed, and gate 1.2 in
    /// doc 16 asks for exactly that.
    pub now_ms: u64,
    /// The most this tick will hand back. Fewer, including none, is normal.
    pub max_urls: u32,
    /// The most expensive tier this fetcher will run.
    pub max_tier: Tier,
    /// Doc 09.5's split for this tick, when it is not the configured one.
    ///
    /// `None` means the one the frontier was built with, which is the normal
    /// case. It is here rather than in [`Config`] because doc 15.3's ladder
    /// moves the discovery share as pressure comes and goes, and a knob that
    /// changes several times an hour does not belong in the same struct as
    /// the ones an operator sets once.
    pub budget: Option<Budget>,
}

impl Ask {
    /// An ask from the coordinator's own fetcher at the plain tier.
    #[must_use]
    pub const fn new(now_ms: u64, max_urls: u32) -> Self {
        Self {
            fetcher: FetcherId::LOCAL,
            now_ms,
            max_urls,
            max_tier: Tier::Plain,
            budget: None,
        }
    }
}

/// What a batch of discovered links turned into.
///
/// The three counts and `report.total()` sum to the number of links offered,
/// so a caller can assert that nothing went missing.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct DiscoverReport {
    /// What the state layer made of the links that got that far.
    pub admitted: AdmitReport,
    /// Past the depth cap in doc 09.7, so not admitted and not remembered.
    /// The same link from a shallower page is admitted normally, which is the
    /// point of not remembering it.
    pub too_deep: u32,
    /// Not a crawlable http or https URL, so there is nothing to fetch.
    pub uncrawlable: u32,
}

impl DiscoverReport {
    /// How many links were offered.
    #[must_use]
    pub fn total(&self) -> u32 {
        self.admitted
            .total()
            .saturating_add(self.too_deep)
            .saturating_add(self.uncrawlable)
    }
}

/// The scheduler.
///
/// One per coordinator. It owns the domain rate limits, which are in memory
/// and are rebuilt from the resident shards after a restart, per doc 09.8. The
/// URLs themselves are all in state, so there is nothing durable in here to
/// lose.
#[derive(Debug)]
pub struct Frontier<S> {
    state: S,
    config: Config,
    gate: Mutex<Gate>,
}

impl<S: State> Frontier<S> {
    /// A frontier over this store.
    pub fn new(state: S, config: Config) -> Self {
        Self {
            state,
            gate: Mutex::new(Gate::new(config.rate)),
            config,
        }
    }

    /// The store underneath, for completions, host records and checkpoints.
    ///
    /// Completions do not come back through the frontier because nothing here
    /// changes when one lands: the domain was charged when the lease was
    /// issued, and the host timer and the next due time are the state layer's
    /// to move.
    pub const fn state(&self) -> &S {
        &self.state
    }

    /// How the scheduler is tuned.
    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// Give the store back, leaving the schedule behind.
    ///
    /// The schedule is the only thing in here that is not in state, and doc
    /// 09.8 says it is rebuildable, so dropping it is a supported thing to do.
    /// The frontier that picks the store back up calls
    /// [`resume`](Self::resume).
    #[must_use]
    pub fn into_state(self) -> S {
        self.state
    }

    /// Score a batch of discovered links and admit the ones worth having.
    ///
    /// `parent_depth` is the depth of the page the links came off, so the
    /// links themselves sit one hop further out. Anything past
    /// [`Config::max_depth`] is dropped and anything that is not an http or
    /// https URL never reaches the state layer.
    ///
    /// The batch is canonicalised once and the keys are derived from the
    /// result rather than from the original string, because this is the
    /// hottest call in the system and doc 08.1 measures it at about 12500
    /// candidates a second per host.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means nothing in the batch was
    /// admitted.
    pub async fn discover(
        &self,
        links: &[&str],
        parent_depth: u8,
        now_ms: u64,
        discovery: Discovery,
    ) -> Result<DiscoverReport> {
        self.admit_at(links, parent_depth.saturating_add(1), now_ms, discovery)
            .await
    }

    /// Admit seeds, which are depth zero because they are where depth is
    /// measured from.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    pub async fn seed(&self, urls: &[&str], now_ms: u64) -> Result<DiscoverReport> {
        self.admit_at(urls, 0, now_ms, Discovery::Trusted).await
    }

    /// Admit a batch at a depth the caller has already worked out.
    ///
    /// [`discover`](Self::discover) is this with the depth being the parent's
    /// plus one, which is what a link off a page is. Doc 13.2's one hop policy
    /// is the exception: a link out of scope is admitted at the ceiling so that
    /// whatever it links to is dropped, and the crawl loop has to be able to
    /// say so. Everything else about the two is the same, including the depth
    /// cap and the domain becoming schedulable.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means nothing in the batch was
    /// admitted.
    pub async fn admit_at(
        &self,
        links: &[&str],
        depth: u8,
        now_ms: u64,
        discovery: Discovery,
    ) -> Result<DiscoverReport> {
        self.admit_each(
            links.iter().map(|link| (*link, None)),
            links.len(),
            depth,
            now_ms,
            discovery,
        )
        .await
    }

    /// Admit a batch where the publisher has told us when each URL last
    /// changed.
    ///
    /// This is [`admit_at`](Self::admit_at) for a sitemap or a feed, which
    /// carry a `lastmod` per URL. Doc 13.6 wants that date used rather than
    /// thrown away, and what it does is in
    /// [`Candidate::lastmod_ms`](umi_state::Candidate::lastmod_ms): on a URL we
    /// have never fetched, nothing, since it is due immediately anyway, and on
    /// one we have, it brings the next visit forward if the page moved after we
    /// last looked. A `None` in the list is a URL the document did not date and
    /// behaves exactly like a link off a page.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means nothing in the batch was
    /// admitted.
    pub async fn admit_dated(
        &self,
        links: &[(&str, Option<u64>)],
        depth: u8,
        now_ms: u64,
        discovery: Discovery,
    ) -> Result<DiscoverReport> {
        self.admit_each(links.iter().copied(), links.len(), depth, now_ms, discovery)
            .await
    }

    /// The body both of the above share.
    ///
    /// It takes an iterator and a count rather than a slice so that the hot
    /// path, a page's worth of links with no dates on them, does not have to
    /// build a vector of pairs to get here.
    async fn admit_each<'a>(
        &self,
        links: impl Iterator<Item = (&'a str, Option<u64>)>,
        count: usize,
        depth: u8,
        now_ms: u64,
        discovery: Discovery,
    ) -> Result<DiscoverReport> {
        let mut report = DiscoverReport::default();
        if !within_depth(depth, self.config.max_depth) {
            report.too_deep = u32::try_from(count).unwrap_or(u32::MAX);
            return Ok(report);
        }

        let mut urls: Vec<String> = Vec::with_capacity(count);
        let mut keys: Vec<RowKey> = Vec::with_capacity(count);
        let mut dates: Vec<Option<u64>> = Vec::with_capacity(count);
        for (link, lastmod_ms) in links {
            let Ok(canonical) = canonicalize(link, None) else {
                report.uncrawlable += 1;
                continue;
            };
            let Ok(key) = RowKey::for_canonical(&canonical) else {
                report.uncrawlable += 1;
                continue;
            };
            urls.push(canonical);
            keys.push(key);
            dates.push(lastmod_ms);
        }

        let priority = depth_score(depth);
        let batch: Vec<Candidate<'_>> = urls
            .iter()
            .zip(&keys)
            .zip(&dates)
            .map(|((url, key), lastmod_ms)| Candidate {
                key: *key,
                url,
                depth,
                priority,
                discovered_ms: now_ms,
                discovery,
                lastmod_ms: *lastmod_ms,
            })
            .collect();

        // Every domain we admit to becomes schedulable here rather than at the
        // next `resume`, so a seed is leasable on the tick after it lands
        // instead of on the next restart.
        {
            let mut gate = self.gate();
            for key in &keys {
                gate.note(key.pld);
            }
        }

        for chunk in batch.chunks(BATCH) {
            let part = self.state.admit(chunk).await?;
            report.admitted = add(report.admitted, part);
        }
        Ok(report)
    }

    /// One turn of doc 09.3's scheduler loop.
    ///
    /// Takes the domains whose rate limit has room, in most overdue order, and
    /// leases from each until the ask is filled or the domains run out.
    /// Returning fewer leases than asked for, including none, is the normal
    /// case and is not an error: it usually means every ready host is inside
    /// its politeness window.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means some leases may already have
    /// been issued and the caller has lost track of them, so it waits for them
    /// to expire rather than retrying immediately.
    pub async fn tick(&self, ask: &Ask) -> Result<Vec<Lease>> {
        let ready = self.gate().ready(ask.now_ms, self.config.max_domains);

        let mut out: Vec<Lease> = Vec::new();
        for (pld, allowance) in ready {
            let room = usize::try_from(ask.max_urls)
                .unwrap_or(usize::MAX)
                .saturating_sub(out.len());
            if room == 0 {
                break;
            }
            let take = room.min(usize::try_from(allowance).unwrap_or(usize::MAX));
            let leases = self.lease_from(pld, ask, take).await?;
            self.gate().charge(
                pld,
                u32::try_from(leases.len()).unwrap_or(u32::MAX),
                ask.now_ms,
            );
            out.extend(leases);
        }
        Ok(out)
    }

    /// Rebuild the domain schedule from the resident shards, which is doc
    /// 09.8's restart path.
    ///
    /// Call it once when a coordinator comes up, before the first
    /// [`tick`](Self::tick). Doc 09.8 puts the rebuild at about a second per
    /// thousand resident domains, and that is a startup cost rather than a
    /// running one. Calling it again on a live frontier is safe and is what
    /// [`evict`](Self::evict) does.
    ///
    /// An empty answer leaves the gate alone rather than emptying it. A store
    /// with nothing in it has nothing to schedule either way, so the two
    /// readings agree on the only case that reaches here, and leaving the gate
    /// alone is the safe one to be wrong about.
    ///
    /// Returns how many domains are being scheduled afterwards.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error leaves the schedule untouched.
    pub async fn resume(&self) -> Result<usize> {
        let resident = self.state.resident().await?;
        let mut gate = self.gate();
        if !resident.is_empty() {
            for pld in &resident {
                gate.note(*pld);
            }
            gate.retain(&resident);
        }
        Ok(gate.len())
    }

    /// Drop these domains from local disk and stop scheduling them.
    ///
    /// Doc 08.6 makes local disk a cache, so a domain that has been sealed and
    /// uploaded stops being schedulable. Going through here rather than calling
    /// [`State::evict`] directly is what keeps the schedule honest: the
    /// scheduler no longer reads the resident set on every tick, so eviction
    /// has to say so.
    ///
    /// A domain with leases outstanding is kept, per
    /// [`EvictReport::in_use`](umi_state::EvictReport::in_use), and stays
    /// scheduled.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    pub async fn evict(&self, plds: &[PldId]) -> Result<EvictReport> {
        let report = self.state.evict(plds).await?;
        // Reconciling against the whole resident set costs a walk of it, which
        // is exactly the walk that came out of `tick`. It is affordable here
        // because eviction happens when a shard is sealed, not sixty times a
        // second.
        let resident = self.state.resident().await?;
        let mut gate = self.gate();
        if resident.is_empty() {
            // The store came back empty, so there is nothing left to schedule
            // and forgetting what was asked for is the whole job.
            for pld in plds {
                gate.forget(*pld);
            }
        } else {
            gate.retain(&resident);
        }
        Ok(report)
    }

    /// When this domain may next be fetched, or `None` if it is not being
    /// scheduled. For a dashboard and for tests.
    #[must_use]
    pub fn next_ready_ms(&self, pld: PldId) -> Option<u64> {
        self.gate().next_ready_ms(pld)
    }

    /// How many domains are being scheduled.
    #[must_use]
    pub fn domains(&self) -> usize {
        self.gate().len()
    }

    async fn lease_from(&self, pld: PldId, ask: &Ask, take: usize) -> Result<Vec<Lease>> {
        let plds = [pld];
        let req = LeaseRequest {
            fetcher: ask.fetcher,
            now_ms: ask.now_ms,
            max_urls: u32::try_from(take).unwrap_or(u32::MAX),
            max_per_host: self.config.max_per_host,
            max_tier: ask.max_tier,
            lease_for: self.config.lease_for,
            plds: &plds,
            budget: ask.budget.unwrap_or(self.config.budget),
        };
        self.state.lease(&req).await
    }

    fn gate(&self) -> MutexGuard<'_, Gate> {
        // Nothing under this lock can panic, and recovering the guard keeps
        // one bad tick from taking the scheduler down with it.
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The score a URL gets on admission, which is [`depth_score`] and nothing
/// else in milestone 1. Here so a caller writing its own [`Candidate`] gets
/// the same answer the frontier would have given it.
#[must_use]
pub fn admission_priority(depth: u8) -> Priority {
    depth_score(depth)
}

fn add(a: AdmitReport, b: AdmitReport) -> AdmitReport {
    AdmitReport {
        seen: a.seen.saturating_add(b.seen),
        admitted: a.admitted.saturating_add(b.admitted),
        held: a.held.saturating_add(b.held),
        excluded: a.excluded.saturating_add(b.excluded),
        shard_misses: a.shard_misses.saturating_add(b.shard_misses),
        refreshed: a.refreshed.saturating_add(b.refreshed),
    }
}
