//! An in memory [`State`], for tests and for proving the conformance suite
//! runs.
//!
//! This is not a backend. It keeps everything in a `BTreeMap` behind one
//! mutex, it never writes to disk, and everything in it is gone when the
//! process exits, so the durability promises in the trait are vacuously true
//! here and the crash suite in milestone 1 has nothing to test against it.
//!
//! It exists for two reasons. The first is that a conformance suite nobody has
//! run is not a conformance suite, it is a wish, so [`conformance`](crate::conformance)
//! is run against this on every `cargo test` and the real backends inherit a
//! suite that is known to be passable. The second is that the crates above
//! state, the frontier and the CLI, need something to test against that is not
//! a file, and writing a second test double in each of them would produce
//! three different opinions about what `complete` does.
//!
//! Where the reference makes a choice the trait leaves open, it makes the
//! simplest correct one and says so, so that a real backend can differ without
//! that being a bug.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};

use umi_types::{CANON_VERSION, FetcherId, HostId, PldId, RowKey, Ulid, UrlKey, UrlKeyFull};

use crate::{
    AdmitReport, Candidate, Checkpoint, Discovery, EvictReport, FetchOutcome, FetchResult, HostRow,
    Lease, LeaseId, LeaseRequest, LedgerRow, NackReason, Priority, Result, Revalidator,
    SegmentQuery, SegmentRow, State, StateStats, UrlState, next_due_after, retry_after_ms,
};

/// A [`State`] that lives entirely in memory.
///
/// Cheap to create, so a test makes one per case rather than sharing one.
#[derive(Debug, Default)]
pub struct MemoryState {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// The seen set. Every URL we have ever been offered is in here, whether
    /// it went to the frontier, the holding pen or straight to excluded.
    seen: BTreeSet<UrlKey>,
    /// The ledger, ordered by `(pld, host, url)` exactly as doc 08.2 says, so
    /// that iterating it gives the scan shape a real backend gets for free.
    ledger: BTreeMap<RowKey, Entry>,
    hosts: HashMap<HostId, HostRow>,
    /// Sealed segments, keyed by ULID. A `BTreeMap` so that iteration is in
    /// seal order already and the sort in `segments` is doing nothing on the
    /// common path.
    segments: BTreeMap<Ulid, SegmentRow>,
    pen: BTreeMap<(FetcherId, UrlKey), Held>,
    /// Live leases, so `release` can find a row from an id alone.
    leases: HashMap<LeaseId, RowKey>,
    resident: BTreeSet<PldId>,
    /// The interned ETag pool a `LedgerRow::etag_ref` points into. One pool
    /// for the whole store here; a sharded backend has one per shard.
    etags: Vec<String>,
    etag_index: HashMap<String, u32>,
    next_lease: u64,
    checkpoint_seq: u64,
    shard_misses: u64,
}

#[derive(Debug)]
struct Entry {
    row: LedgerRow,
    /// The URL text, which the ledger row deliberately does not carry. Doc
    /// 08.3 lists no URL column, so a backend keeps the text beside the row
    /// and only joins it in when building a [`Lease`].
    url: String,
    lease: Option<InFlight>,
}

#[derive(Clone, Copy, Debug)]
struct InFlight {
    id: LeaseId,
    expires_ms: u64,
}

/// One URL parked in the holding pen.
///
/// Everything [`admit`](State::admit) was told about it is kept, because
/// graduating it later means putting it into the frontier with the depth and
/// priority it was discovered at, not with defaults.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Held {
    /// The keys.
    pub key: RowKey,
    /// The canonical URL.
    pub url: String,
    /// Link distance from the nearest seed, as the discovering fetcher
    /// reported it.
    pub depth: u8,
    /// The score it would have been admitted at.
    pub priority: Priority,
    /// When it was discovered, which is what the 30 day expiry in doc 06.2
    /// measures from.
    pub discovered_ms: u64,
}

impl MemoryState {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic while holding this lock would be a bug in this file, and
        // there is nothing in here that can panic. Recovering the guard rather
        // than propagating the poison keeps one bad test from cascading into
        // every other one.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Inner {
    /// Note that a domain is being worked, warming its shard if it is not
    /// local. Returns whether this was a miss.
    fn touch(&mut self, pld: PldId) -> bool {
        if self.resident.insert(pld) {
            self.shard_misses += 1;
            true
        } else {
            false
        }
    }

    fn intern_etag(&mut self, etag: &str) -> u32 {
        if let Some(index) = self.etag_index.get(etag) {
            return *index;
        }
        let index = u32::try_from(self.etags.len()).unwrap_or(LedgerRow::NO_ETAG);
        if index == LedgerRow::NO_ETAG {
            return LedgerRow::NO_ETAG;
        }
        self.etags.push(etag.to_owned());
        self.etag_index.insert(etag.to_owned(), index);
        index
    }

    fn etag(&self, reference: u32) -> Option<String> {
        if reference == LedgerRow::NO_ETAG {
            return None;
        }
        self.etags.get(reference as usize).cloned()
    }

    /// The host record, or the defaults for a host we have never fetched. The
    /// defaults matter: a brand new host has to be leasable, or nothing would
    /// ever be crawled for the first time.
    fn host_or_default(&self, key: &RowKey) -> HostRow {
        self.hosts
            .get(&key.host)
            .cloned()
            .unwrap_or_else(|| HostRow::new(key.host, key.pld))
    }

    fn count(&self, state: UrlState) -> u64 {
        self.ledger
            .values()
            .filter(|entry| entry.row.state == state)
            .count() as u64
    }

    fn stats(&self) -> StateStats {
        StateStats {
            urls_seen: self.seen.len() as u64,
            urls_pending: self.count(UrlState::Pending),
            urls_fetched: self.count(UrlState::Fetched),
            urls_failed: self.count(UrlState::Failed),
            urls_gone: self.count(UrlState::Gone),
            urls_excluded: self.count(UrlState::Excluded),
            urls_held: self.pen.len() as u64,
            hosts: self.hosts.len() as u64,
            leases_in_flight: self.leases.len() as u64,
            resident_plds: self.resident.len() as u64,
            shard_misses: self.shard_misses,
            // Nothing is on disk, and reporting an estimate would make the
            // one number an operator uses to size a machine a lie.
            bytes_on_disk: 0,
        }
    }
}

#[async_trait::async_trait]
impl State for MemoryState {
    async fn admit(&self, batch: &[Candidate<'_>]) -> Result<AdmitReport> {
        let mut inner = self.lock();
        let mut report = AdmitReport::default();
        let mut warmed = BTreeSet::new();

        for candidate in batch {
            if inner.touch(candidate.key.pld) {
                warmed.insert(candidate.key.pld);
            }

            // The seen set is checked first, so a URL that is already known is
            // `seen` whatever its ledger state is. That is what makes
            // admitting the same batch twice idempotent, and it is why a
            // duplicate inside one batch is counted as seen: the first
            // occurrence puts it in the set.
            if !inner.seen.insert(candidate.key.url) {
                report.seen += 1;
                continue;
            }

            // A blocked host is the only exclusion this reference applies.
            // robots and scope are decided above state, because state does not
            // fetch robots.txt and does not own the crawl's configuration.
            if inner
                .hosts
                .get(&candidate.key.host)
                .is_some_and(|h| h.blocked)
            {
                let mut row = LedgerRow::pending(
                    &candidate.key,
                    candidate.url,
                    candidate.depth,
                    candidate.priority,
                    candidate.discovered_ms,
                );
                row.state = UrlState::Excluded;
                row.next_due_ms = u64::MAX;
                inner.ledger.insert(
                    candidate.key,
                    Entry {
                        row,
                        url: candidate.url.to_owned(),
                        lease: None,
                    },
                );
                report.excluded += 1;
                continue;
            }

            if let Discovery::Unverified(fetcher) = candidate.discovery {
                inner.pen.insert(
                    (fetcher, candidate.key.url),
                    Held {
                        key: candidate.key,
                        url: candidate.url.to_owned(),
                        depth: candidate.depth,
                        priority: candidate.priority,
                        discovered_ms: candidate.discovered_ms,
                    },
                );
                report.held += 1;
                continue;
            }

            inner.ledger.insert(
                candidate.key,
                Entry {
                    row: LedgerRow::pending(
                        &candidate.key,
                        candidate.url,
                        candidate.depth,
                        candidate.priority,
                        candidate.discovered_ms,
                    ),
                    url: candidate.url.to_owned(),
                    lease: None,
                },
            );
            report.admitted += 1;
        }

        report.shard_misses = u32::try_from(warmed.len()).unwrap_or(u32::MAX);
        Ok(report)
    }

    async fn lease(&self, req: &LeaseRequest<'_>) -> Result<Vec<Lease>> {
        let mut inner = self.lock();

        // Choose first, mutate second. Deciding and writing in one pass would
        // make the result depend on map iteration order in a way the
        // determinism promise does not allow.
        let mut ready: Vec<(RowKey, Priority, u64)> = Vec::new();
        for (key, entry) in &inner.ledger {
            if !req.plds.is_empty() && !req.plds.contains(&key.pld) {
                continue;
            }
            if entry
                .lease
                .is_some_and(|lease| lease.expires_ms > req.now_ms)
            {
                continue;
            }
            if !entry.row.is_due(req.now_ms) {
                continue;
            }
            // Politeness is enforced by not issuing the lease, not by asking
            // the fetcher to wait. Doc 07.6 is explicit that a fetcher cannot
            // cause a second concurrent request to a host because a second
            // lease is not issued, and that only holds if the timer is checked
            // here.
            let host = inner.host_or_default(key);
            if !host.is_fetchable(req.now_ms) || !host.tier.reachable_by(req.max_tier) {
                continue;
            }
            ready.push((*key, entry.row.priority, entry.row.next_due_ms));
        }

        // High priority first, then whatever has been waiting longest, then
        // the key. The key is the tiebreak that makes this a total order, and
        // a total order is what makes a replay produce the same crawl.
        ready.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.2.cmp(&b.2)).then(a.0.cmp(&b.0)));

        let mut leases = Vec::new();
        let mut per_host: HashMap<HostId, u32> = HashMap::new();
        // The politeness clock this call is advancing, per host. It starts at
        // the stored timer and moves forward by the host's delay for every
        // lease handed out, so a fetcher holding eight URLs for one host is
        // told to space them out rather than trusted to.
        let mut clock: HashMap<HostId, u64> = HashMap::new();

        for (key, _, _) in ready {
            if leases.len() >= req.max_urls as usize {
                break;
            }
            let taken = per_host.entry(key.host).or_default();
            if *taken >= req.max_per_host {
                continue;
            }

            let host = inner.host_or_default(&key);
            let delay = host.delay().as_millis() as u64;
            let slot = clock
                .entry(key.host)
                .or_insert_with(|| host.next_allowed_ms.max(req.now_ms));
            let not_before_ms = *slot;
            *slot = not_before_ms.saturating_add(delay);
            *taken += 1;

            inner.next_lease += 1;
            let id = LeaseId::from_raw(inner.next_lease);
            let expires_ms = not_before_ms.saturating_add(req.lease_for.as_millis() as u64);

            let (url, row) = {
                let entry = inner.ledger.get(&key).expect("chosen from this map");
                (entry.url.clone(), entry.row)
            };
            let revalidate = Revalidator {
                etag: inner.etag(row.etag_ref),
                last_modified_ms: (row.last_mod_ms != 0).then_some(row.last_mod_ms),
            };

            let entry = inner.ledger.get_mut(&key).expect("chosen from this map");
            entry.lease = Some(InFlight { id, expires_ms });
            inner.leases.insert(id, key);

            leases.push(Lease {
                id,
                key,
                url,
                depth: row.depth,
                priority: row.priority,
                attempt: row.fetch_count,
                tier: host.tier.start_at(req.max_tier),
                not_before_ms,
                expires_ms,
                // A host that lies about its revalidators gets unconditional
                // requests, per doc 05.8. Sending one it will ignore costs a
                // round trip and saves nothing.
                revalidate: (!revalidate.is_empty() && !host.tier.lying_revalidator)
                    .then_some(revalidate),
            });
        }

        // Persist the politeness clock, so the next lease call cannot hand out
        // the same host again immediately. This is the structural half of doc
        // 07.6: a fetcher cannot make a second concurrent request to a host
        // because a second lease is not issued.
        for (host_id, next_allowed_ms) in clock {
            let key_pld = leases
                .iter()
                .find(|lease| lease.key.host == host_id)
                .map_or_else(PldId::default, |lease| lease.key.pld);
            inner
                .hosts
                .entry(host_id)
                .or_insert_with(|| HostRow::new(host_id, key_pld))
                .next_allowed_ms = next_allowed_ms;
        }

        Ok(leases)
    }

    async fn complete(&self, outcomes: &[FetchOutcome]) -> Result<()> {
        let mut inner = self.lock();
        for outcome in outcomes {
            inner.leases.remove(&outcome.lease);
            let Some(before) = inner.ledger.get(&outcome.key).map(|entry| entry.row) else {
                // A completion for a URL we do not have a row for. That is a
                // bug above us, not corruption down here, and dropping it is
                // better than inventing a row with no depth and no priority.
                continue;
            };

            // Idempotence without an unbounded set of retired lease ids: an
            // answer only counts if it is newer than the one already recorded.
            // A retried completion carries the same `finished_ms` and so
            // changes nothing, while a completion from a lease that expired
            // still lands, because the page really was fetched and throwing
            // that away because the coordinator got impatient would cost a
            // refetch against an origin that did nothing wrong.
            if outcome.finished_ms <= before.last_fetch_ms {
                if let Some(entry) = inner.ledger.get_mut(&outcome.key)
                    && entry.lease.is_some_and(|lease| lease.id == outcome.lease)
                {
                    entry.lease = None;
                }
                continue;
            }

            let mut row = before;
            row.tier_used = outcome.tier_used;

            match &outcome.result {
                FetchResult::Fetched {
                    status,
                    content_hash,
                    revalidate,
                } => {
                    let changed = *content_hash != before.content_hash;
                    row.state = UrlState::Fetched;
                    row.status = *status;
                    row.fetch_count = before.fetch_count.saturating_add(1);
                    row.last_fetch_ms = outcome.finished_ms;
                    row.content_hash = *content_hash;
                    row.fail_streak = 0;
                    if changed {
                        row.change_count = before.change_count.saturating_add(1);
                        row.last_change_ms = outcome.finished_ms;
                    }
                    row.etag_ref = match &revalidate.etag {
                        Some(etag) => inner.intern_etag(etag),
                        None => LedgerRow::NO_ETAG,
                    };
                    row.last_mod_ms = revalidate.last_modified_ms.unwrap_or(0);
                    row.next_due_ms = next_due_after(&before, changed, outcome.finished_ms);
                }
                FetchResult::NotModified { status, revalidate } => {
                    // The content did not move, so `content_hash` and
                    // `last_change_ms` are left exactly where they were. That
                    // is the whole point of a conditional request: it is a
                    // cheap observation, not a new version.
                    row.state = UrlState::Fetched;
                    row.status = *status;
                    row.fetch_count = before.fetch_count.saturating_add(1);
                    row.last_fetch_ms = outcome.finished_ms;
                    row.fail_streak = 0;
                    if let Some(etag) = &revalidate.etag {
                        row.etag_ref = inner.intern_etag(etag);
                    }
                    if let Some(last_mod_ms) = revalidate.last_modified_ms {
                        row.last_mod_ms = last_mod_ms;
                    }
                    row.next_due_ms = next_due_after(&before, false, outcome.finished_ms);
                }
                FetchResult::Failed { status, kind: _ } => {
                    row.state = UrlState::Failed;
                    row.status = status.unwrap_or(0);
                    row.last_fetch_ms = outcome.finished_ms;
                    row.fail_streak = before.fail_streak.saturating_add(1);
                    row.next_due_ms = outcome
                        .finished_ms
                        .saturating_add(retry_after_ms(row.fail_streak));
                }
                FetchResult::Gone { status } => {
                    row.state = UrlState::Gone;
                    row.status = *status;
                    row.last_fetch_ms = outcome.finished_ms;
                    row.next_due_ms = u64::MAX;
                }
                FetchResult::Excluded { reason: _ } => {
                    // The reason is not stored. Rechecking robots is cheap and
                    // a stale reason on a row is worse than no reason at all.
                    row.state = UrlState::Excluded;
                    row.last_fetch_ms = outcome.finished_ms;
                    row.next_due_ms = u64::MAX;
                }
            }

            if let Some(entry) = inner.ledger.get_mut(&outcome.key) {
                entry.row = row;
                entry.lease = None;
            }

            // Doc 07.6's rate limiter, after the ledger row and inside the
            // same call, so a crash cannot record the page and lose the reason
            // to slow down for it.
            let mut host = inner.host_or_default(&outcome.key);
            if host.observe(&outcome.result, outcome.pace, outcome.finished_ms) {
                inner.hosts.insert(outcome.key.host, host);
            }
        }
        Ok(())
    }

    async fn release(&self, lease_ids: &[LeaseId], _reason: NackReason) -> Result<()> {
        let mut inner = self.lock();
        for id in lease_ids {
            // An id the store does not know is not an error. A fetcher and a
            // coordinator timing out at the same moment produces exactly that,
            // and it happens in normal operation.
            let Some(key) = inner.leases.remove(id) else {
                continue;
            };
            if let Some(entry) = inner.ledger.get_mut(&key)
                && entry.lease.is_some_and(|lease| lease.id == *id)
            {
                // The due time is left alone, so the URL is leasable again at
                // once. `fail_streak` is not touched, because a fetcher going
                // away says nothing about the URL.
                entry.lease = None;
            }
        }
        Ok(())
    }

    async fn host(&self, id: HostId) -> Result<Option<HostRow>> {
        Ok(self.lock().hosts.get(&id).cloned())
    }

    async fn put_host(&self, rows: &[HostRow]) -> Result<()> {
        let mut inner = self.lock();
        for row in rows {
            inner.hosts.insert(row.host, row.clone());
        }
        Ok(())
    }

    async fn put_segment(&self, rows: &[SegmentRow]) -> Result<()> {
        let mut inner = self.lock();
        for row in rows {
            inner.segments.insert(row.id, row.clone());
        }
        Ok(())
    }

    async fn segment(&self, id: Ulid) -> Result<Option<SegmentRow>> {
        Ok(self.lock().segments.get(&id).cloned())
    }

    async fn segments(&self, query: SegmentQuery) -> Result<Vec<SegmentRow>> {
        let inner = self.lock();
        let mut found: Vec<SegmentRow> = inner
            .segments
            .values()
            .filter(|row| match query {
                SegmentQuery::Unpublished => row.remote.is_none(),
                SegmentQuery::Collectable => row.ledger_complete() && row.local(),
                SegmentQuery::SealedBetween { from_ms, to_ms } => {
                    row.sealed_at_ms >= from_ms && row.sealed_at_ms < to_ms
                }
            })
            .cloned()
            .collect();
        // A ULID already sorts by time, so this is only doing work when two
        // segments were sealed in the same millisecond, which happens when a
        // restart seals every open writer at once.
        found.sort_by_key(|row| (row.sealed_at_ms, row.id));
        Ok(found)
    }

    async fn warm(&self, plds: &[PldId]) -> Result<()> {
        let mut inner = self.lock();
        for pld in plds {
            inner.resident.insert(*pld);
        }
        Ok(())
    }

    async fn evict(&self, plds: &[PldId]) -> Result<EvictReport> {
        let mut inner = self.lock();
        let mut report = EvictReport::default();
        for pld in plds {
            if !inner.resident.contains(pld) {
                report.not_resident += 1;
                continue;
            }
            let busy = inner
                .ledger
                .iter()
                .any(|(key, entry)| key.pld == *pld && entry.lease.is_some());
            if busy {
                report.in_use += 1;
                continue;
            }
            inner.resident.remove(pld);
            report.evicted += 1;
        }
        // Nothing was written, because there is nowhere to write it. A real
        // backend seals the shard, uploads it and updates the manifest before
        // dropping the local copy, and reports the bytes that cost.
        Ok(report)
    }

    async fn resident(&self) -> Result<Vec<PldId>> {
        Ok(self.lock().resident.iter().copied().collect())
    }

    async fn checkpoint(&self, now_ms: u64) -> Result<Checkpoint> {
        let mut inner = self.lock();
        inner.checkpoint_seq += 1;
        let sequence = inner.checkpoint_seq;
        let stats = inner.stats();
        Ok(Checkpoint {
            sequence,
            taken_ms: now_ms,
            canon_version: CANON_VERSION.to_owned(),
            path: None,
            digest: None,
            stats,
        })
    }

    async fn stats(&self) -> Result<StateStats> {
        Ok(self.lock().stats())
    }
}

impl MemoryState {
    /// The URLs sitting in the holding pen for one fetcher, oldest first.
    ///
    /// Not part of [`State`], because graduating a held URL is doc 06.2's job
    /// and the rules for it land in milestone 2. This is here so the tests for
    /// that work have something to look at, and so that the pen being keyed by
    /// fetcher is observable: one bad fetcher's discoveries have to be findable
    /// and droppable without touching anyone else's.
    #[must_use]
    pub fn held_for(&self, fetcher: FetcherId) -> Vec<Held> {
        let inner = self.lock();
        let mut out: Vec<Held> = inner
            .pen
            .iter()
            .filter(|((who, _), _)| *who == fetcher)
            .map(|(_, held)| held.clone())
            .collect();
        out.sort_by_key(|held| (held.discovered_ms, held.key));
        out
    }

    /// The ledger row for one URL, for tests only.
    ///
    /// [`State`] has no `get(url)` and is not getting one. Doc 08.4 is explicit
    /// that a caller wanting to ask something else asks DuckDB against a
    /// published checkpoint, not the live store. This is an inherent method on
    /// the reference so that a test can assert what `complete` wrote without
    /// that widening the trait every backend has to serve.
    #[must_use]
    pub fn row(&self, key: &RowKey) -> Option<LedgerRow> {
        self.lock().ledger.get(key).map(|entry| entry.row)
    }
}

/// A convenience for tests: the keys and text of a URL, panicking if it is not
/// canonicalisable.
///
/// Only for test setup, where a bad literal is a bug in the test and there is
/// nothing useful to do with the error.
#[must_use]
pub fn key_of(url: &str) -> RowKey {
    RowKey::for_url(url, None).expect("test urls are well formed")
}

/// The defaults a fresh host record gets, exposed so a test can put one back
/// with one field changed.
#[must_use]
pub fn host_row(url: &str) -> HostRow {
    let key = key_of(url);
    HostRow {
        tier: crate::TierPolicy::new(),
        ..HostRow::new(key.host, key.pld)
    }
}

/// A content hash for tests, so that two different strings reliably produce
/// two different hashes and the same string produces the same one.
#[must_use]
pub fn content_hash(text: &str) -> [u8; 8] {
    let full = UrlKeyFull::derive(text.as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&full.as_bytes()[..8]);
    out
}
