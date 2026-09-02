//! The `State` trait: everything mutable the crawler needs to decide what to
//! fetch next.
//!
//! Specified in `docs/spec/08-state-layer.md`. State is which URLs we know,
//! which we have fetched, what we found, when to look again, and how each host
//! behaves. It is strictly separated from the crawled results in doc 10, which
//! are immutable and append only, and the two never share a file, a lock or a
//! lifecycle.
//!
//! # Why the surface is this narrow
//!
//! There is no `get(url)`, no `scan`, and no generic query, and that is the
//! whole design rather than an omission. Doc 08.1 measures the workload: about
//! 12500 candidate URLs per second per host, of which well over 95 percent are
//! already known. The dominant operation is "is this 80 bit fingerprint
//! present, and if not insert it", and any trait with a per URL method in it
//! cannot serve that no matter what is underneath, because the method call
//! overhead alone eats the budget before a backend does any work. So every
//! method here takes a batch.
//!
//! Four backends have to be honest about this trait: sqlite by default, `nami`
//! as the experimental single file engine, Postgres for a real server, and
//! DuckDB read only against a checkpoint. A trait extracted from one of them
//! would be a description of that one, so this crate compiles with no backend
//! at all and every backend is written against it afterwards. The in memory
//! reference in [`memory`] exists to prove the [`conformance`] suite runs, and
//! to give the crates above state something to test against.
//!
//! # Time is an argument
//!
//! Nothing in here reads a clock. `now_ms` is passed in, lease deadlines are
//! absolute, and refresh scheduling is a pure function of the row plus the
//! time it is given. Politeness, expiry and refresh are all functions of time,
//! and a store that reads its own clock cannot be replayed, which is exactly
//! what gate 1.2 in doc 16 asks for. Milliseconds since the Unix epoch
//! throughout.
//!
//! # What this promises
//!
//! State is single writer per coordinator. A pay level domain is owned by
//! exactly one coordinator at a time, per doc 03.3, so there is no concurrent
//! mutation to resolve and there is no consensus protocol here. On top of
//! that, doc 08.7 gives three rules, and every method below states which one
//! it is under:
//!
//! - [`complete`](State::complete) is durable before it returns.
//! - A [`Lease`] is durable before it is handed out.
//! - Everything else, [`admit`](State::admit) included, may buffer and can
//!   lose up to the group commit window in [`GROUP_COMMIT`]. Losing a batch of
//!   admissions costs re-discovering those URLs next time we crawl a page that
//!   links to them, which is free.

#![forbid(unsafe_code)]

use std::time::Duration;

use umi_types::{HostId, PldId, Ulid, UrlKey};

pub mod conformance;
pub mod freshness;
pub mod memory;
pub mod pace;
mod types;

#[cfg(test)]
mod tests;

pub use freshness::{
    Budget, CLASSES, DAILY_UNDER_MS, HOURLY_UNDER_MS, INITIAL_REFRESH, MAX_REFRESH, MIN_REFRESH,
    Quotas, REALTIME_UNDER_MS, RefreshClass, WEEKLY_UNDER_MS, initial_refresh_ms, next_due_after,
    next_due_dated, refresh_interval_dated, refresh_interval_ms,
};
pub use memory::MemoryState;
pub use pace::Pace;
pub use types::{
    AdmitReport, BlockReport, BlockRow, Candidate, Checkpoint, Discovery, EvictReport,
    ExcludeReason, FailureKind, FetchOutcome, FetchResult, HostRow, Lease, LeaseId, LeaseRequest,
    LedgerRow, NackReason, Priority, RemoteCopy, Revalidator, RobotsRef, SegmentQuery, SegmentRow,
    Shard, SpillRow, StateStats, Stream, SupervisionRow, TierPolicy, UrlState,
};

/// The batch size the whole design is tuned around, from doc 08.5.
///
/// Four thousand and ninety six inserts in one transaction is roughly three
/// orders of magnitude faster than 4096 transactions, and that ratio is the
/// reason every method here takes a slice. A backend may accept larger
/// batches, and must not silently truncate one.
pub const BATCH: usize = 4096;

/// The batch size for admission, which is a different problem from the rest.
///
/// Every other method here works on a set of rows that is roughly the size of
/// the fetch window. Admission works on the links off those pages, which is
/// about forty of them per page, so it is the one call that arrives with
/// millions of rows behind it and the one whose cost grows with the frontier it
/// is writing into.
///
/// That cost is almost entirely write amplification. The ledger is keyed on a
/// hash, so a batch of new urls lands all over a tree that is gigabytes wide,
/// and a page touched once has to be written out whole however few bytes of it
/// changed. Sorting the batch first fixes the order the pages are visited in
/// but not how many of them there are, and that number only comes down when the
/// batch is large enough that several rows share a page.
///
/// Measured on server3 against the 3.1 million row frontier a twelve minute
/// crawl leaves behind, inserting 1.4 million new rows, which is what one tick
/// admits:
///
/// ```text
/// batch      4096    453.5 s    3,087 rows/s   27.63 GB written   21,191 bytes a row
/// batch     65536     60.4 s    6,624 rows/s    2.40 GB written    6,455 bytes a row
/// batch    400000     47.8 s    8,368 rows/s    0.68 GB written    1,831 bytes a row
/// batch   1400000     46.3 s   30,210 rows/s    1.11 GB written      848 bytes a row
/// ```
///
/// The row itself is about 170 bytes, so at [`BATCH`] the disk sees a hundred
/// times what the frontier gained. A million is where the curve has flattened
/// and is still one transaction the machine can hold: the sort indexes cost
/// nine bytes a row, and the urls are already in memory because they are the
/// links the tick just extracted.
///
/// This is a smaller constant than the problem deserves. Amplification is a
/// function of the batch against the tree, so a fixed number stops working once
/// the frontier is large enough, and doc 16's five hundred million rows are
/// large enough. The frontier that holds at that size is `umi-nami`, which
/// merges sorted runs instead of inserting into a tree at all.
pub const ADMIT_BATCH: usize = 1 << 20;

/// How much unflushed work a backend may hold, from doc 08.7.
///
/// A crash loses at most this much admission. It never loses a completion and
/// it never loses a lease.
pub const GROUP_COMMIT: Duration = Duration::from_millis(200);

/// What can go wrong underneath.
///
/// Deliberately small. A backend that wants to report something specific wraps
/// it in [`StateError::Backend`] rather than growing this enum, because the
/// callers above state do not branch on backend detail: they retry, or they
/// stop the crawl.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StateError {
    /// The underlying store could not be read or written.
    #[error("state store io: {0}")]
    Io(#[from] std::io::Error),

    /// The store is structurally wrong, which is corruption or a bug and is
    /// never retried automatically. Exits with
    /// [`Exit::Verification`](umi_types::Exit::Verification).
    #[error("state store is corrupt: {0}")]
    Corrupt(String),

    /// A shard is not local and could not be brought in from cold storage.
    /// Retryable, and the operator's signal that object storage is unwell.
    #[error("shard for pld {pld} is unavailable: {reason}")]
    ShardUnavailable {
        /// The pay level domain whose shard is missing.
        pld: PldId,
        /// What the warm attempt reported.
        reason: String,
    },

    /// A batch was larger than this backend will take. Never a truncation:
    /// the caller is told rather than quietly served part of its request.
    #[error("batch of {got} exceeds this backend's limit of {limit}")]
    BatchTooLarge {
        /// What was offered.
        got: usize,
        /// What the backend accepts.
        limit: usize,
    },

    /// The store was closed, or the process is shutting down.
    #[error("state store is closed")]
    Closed,

    /// Anything specific to one backend.
    #[error(transparent)]
    Backend(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// The result type every [`State`] method returns.
pub type Result<T> = std::result::Result<T, StateError>;

/// The state layer.
///
/// Twenty one methods, all batched, all taking time as an argument. Implement it
/// and then run [`conformance::check`] against it: the suite is the definition
/// of what these doc comments mean, and a backend that has not been through it
/// has not implemented this trait, it has implemented something that compiles.
#[async_trait::async_trait]
pub trait State: Send + Sync + 'static {
    /// Dedup a batch of candidates against the seen set and enqueue the new
    /// ones.
    ///
    /// The single hottest call in the system. Must be O(batch) amortised, not
    /// O(batch log n), and must not do one round trip per candidate.
    ///
    /// Every candidate lands in exactly one bucket of the returned
    /// [`AdmitReport`], so `report.total()` equals `batch.len()`. Duplicates
    /// within one batch are resolved inside the call: the first occurrence is
    /// admitted and the rest are counted as seen, so admitting the same batch
    /// twice is the same as admitting it once.
    ///
    /// A candidate whose [`Discovery`] is [`Discovery::Unverified`] goes to
    /// the holding pen instead of the frontier and is counted as `held`. It is
    /// in the seen set either way, so the same URL from a trusted source later
    /// is not admitted twice.
    ///
    /// **Durability: buffered.** May return before the batch is on disk and
    /// may lose up to [`GROUP_COMMIT`] on a crash. That is the one place in
    /// this trait where durability is traded for throughput, and it is safe
    /// because a lost admission costs a rediscovery and nothing else.
    ///
    /// # Errors
    ///
    /// [`StateError::BatchTooLarge`] if the backend has a smaller limit than
    /// the batch, and whatever the store reports otherwise. An error means
    /// nothing in the batch was admitted.
    async fn admit(&self, batch: &[Candidate<'_>]) -> Result<AdmitReport>;

    /// Hand out work, respecting per host politeness, per domain caps and
    /// priority.
    ///
    /// Returns leases already marked in flight with the given deadline. A URL
    /// under lease is not offered again until the lease expires, is completed,
    /// or is released, so two fetchers never hold the same URL. Returning
    /// fewer leases than asked for, including zero, is normal and is not an
    /// error: it usually means every ready host is inside its politeness
    /// window.
    ///
    /// Ordering is deterministic. Rows are chosen by priority descending, then
    /// by due time ascending, then by [`RowKey`](umi_types::RowKey), so the
    /// same store in the same state at the same `now_ms` produces the same
    /// leases. That is what makes a crawl replayable. Within that order the
    /// batch is split across doc 09.5's refresh classes by
    /// [`LeaseRequest::budget`], so that discovery cannot crowd out refresh or
    /// the reverse, and the returned leases are in the order above whichever
    /// classes they came from.
    ///
    /// **Durability: durable.** A lease is on disk before it is returned.
    /// Otherwise a crash would leave a fetcher holding work the coordinator
    /// has no record of, and the same URL would be fetched twice.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means no leases were issued.
    async fn lease(&self, req: &LeaseRequest<'_>) -> Result<Vec<Lease>>;

    /// Record outcomes, update the change rate model and compute the next due
    /// time.
    ///
    /// Idempotent by lease id: applying the same outcome twice leaves the row
    /// where the first application left it, so a fetcher that retries a
    /// completion after a timeout does not inflate `fetch_count`.
    ///
    /// An outcome naming a lease that has already expired is still applied.
    /// The page really was fetched, and throwing the work away because the
    /// coordinator got impatient would be worse than the double fetch it is
    /// trying to avoid. The lease id is used to clear the in flight marker, not
    /// to decide whether the answer counts.
    ///
    /// **Durability: durable before it returns.** This is the promise the rest
    /// of the system is built on. Data is already in a `.umi` segment by the
    /// time this is called, so a state layer that lost a completion would
    /// refetch a page it already has, and at 250 pages/s per server a crash
    /// that costs even a few seconds of completions is thousands of wasted
    /// fetches against origins that did nothing wrong.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means the batch may have been
    /// partially applied, so the caller retries it whole and relies on the
    /// idempotence above.
    async fn complete(&self, outcomes: &[FetchOutcome]) -> Result<()>;

    /// Give leases back without an answer.
    ///
    /// The URLs are rescheduled immediately, at the due time they already had,
    /// and `fail_streak` is not touched. A fetcher going away says nothing
    /// about the URL, and treating a disconnect as a fetch failure would back
    /// off origins for a problem on our side.
    ///
    /// Releasing a lease id the store does not know, or one that has already
    /// been completed, is not an error. Both happen in normal operation when a
    /// fetcher and a coordinator time out at the same moment.
    ///
    /// **Durability: buffered.** The worst a lost release can do is leave a
    /// URL in flight until its deadline passes, at which point it is leasable
    /// again anyway.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn release(&self, lease_ids: &[LeaseId], reason: NackReason) -> Result<()>;

    /// Read one host record.
    ///
    /// One of two unbatched reads in the trait, and it is here because the
    /// host table is about 50 million rows fleet wide and fits in memory even
    /// on server1, so a backend can serve this without touching disk. Returns
    /// `None` for a host we have never fetched.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn host(&self, id: HostId) -> Result<Option<HostRow>>;

    /// Write host records, replacing any that exist.
    ///
    /// Last write wins per host, and within one batch the last occurrence of a
    /// host wins. There is no read modify write here on purpose: the caller
    /// owns the host record it is updating, because a pay level domain is
    /// owned by one coordinator and nothing else is writing it.
    ///
    /// **Durability: buffered**, except that a `blocked` host is durable
    /// before this returns. Doc 07.7 commits to applying a block within an
    /// hour of a valid request, and a block that a crash can undo is not a
    /// block.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn put_host(&self, rows: &[HostRow]) -> Result<()>;

    /// Stop crawling a domain, or record that a block has been lifted.
    ///
    /// Doc 07.7's mechanism. A block takes a domain out of the frontier, keeps
    /// it out of future admissions, and stays until somebody records lifting
    /// it. A row whose [`lifted_ms`](BlockRow::lifted_ms) is set is a lift, and
    /// it puts the domain's excluded URLs back rather than deleting the record,
    /// because doc 07.7 wants a dated record of both events.
    ///
    /// Last write wins per domain, and within one batch the last occurrence of
    /// a domain wins, as with [`put_host`](State::put_host).
    ///
    /// **Durability: durable before it returns.** Doc 07.7 commits to applying
    /// a block within an hour of a valid request, and a block a crash can undo
    /// is not a block. This is the one call in the trait an operator makes
    /// because somebody asked them to, so it is also the one where the answer
    /// has to be true when it is given rather than shortly afterwards.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means the batch may have been
    /// partially applied, so the caller retries it whole: applying the same
    /// block twice moves nothing the second time.
    async fn block(&self, rows: &[BlockRow]) -> Result<BlockReport>;

    /// The block list, in domain order, lifted entries included.
    ///
    /// Lifted entries are in it because the list is published and the record is
    /// the point. A consumer honouring doc 07.7 reads
    /// [`in_force`](BlockRow::in_force) and a person auditing us reads the
    /// dates.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn blocks(&self) -> Result<Vec<BlockRow>>;

    /// Put domains on doc 05.7's T4 allowlist, or take them off it.
    ///
    /// Last write wins per domain, and within one batch the last occurrence of
    /// a domain wins, as with [`put_host`](State::put_host).
    ///
    /// This is the only route to T4 in the system. Nothing escalates into it,
    /// doc 05.8 caps learned escalation at T3, and the tier a lease asks for
    /// is raised here or not at all, so an empty allowlist means a crawl that
    /// cannot run a supervised fetch however it is configured.
    ///
    /// **Durability: durable before it returns.** The published list is what
    /// makes this tier disclosed rather than secret, and an entry a crash can
    /// undo is one the published list could disagree with.
    ///
    /// # Errors
    ///
    /// Whatever the store reports. An error means the batch may have been
    /// partially applied, so the caller retries it whole: writing the same
    /// entry twice changes nothing the second time.
    async fn supervise(&self, rows: &[SupervisionRow]) -> Result<usize>;

    /// The T4 allowlist, in domain order, removed entries included.
    ///
    /// Removed entries are in it for the same reason lifted blocks are: the
    /// list is published and the record is the point. A consumer reads
    /// [`in_force`](SupervisionRow::in_force) and a person auditing us reads
    /// the dates.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn supervision(&self) -> Result<Vec<SupervisionRow>>;

    /// Write segment records, replacing any that exist.
    ///
    /// Called twice in a segment's life. Once when it is sealed, with the
    /// remote fields empty, and once when it has been uploaded and read back,
    /// with them filled in. Last write wins per id, as with
    /// [`put_host`](State::put_host), and for the same reason: the caller owns
    /// the record because one coordinator owns the segment.
    ///
    /// **Durability: durable.** This is the only method in the trait that is
    /// unconditionally so, and doc 12.7 is why. A local file is deleted once
    /// the record says where the remote copy is, so a record that a crash can
    /// lose is a file that gets deleted and then looks like data loss to the
    /// next reconciliation pass, which would refetch urls that were published
    /// perfectly well. The write happens about a thousand times a day against
    /// a table of a few hundred thousand rows, so an fsync here costs nothing
    /// worth measuring.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn put_segment(&self, rows: &[SegmentRow]) -> Result<()>;

    /// Read one segment record.
    ///
    /// The other unbatched read, and it is here for the same reason: doc 08.3
    /// sizes a year of segment history at well under 100 MB, so a lookup is
    /// never the thing that costs anything.
    ///
    /// Returns `None` for a ULID this store has never sealed, which is not an
    /// error: it is the answer doc 12.8 acts on when it finds a file on the
    /// hub that no segment record claims.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn segment(&self, id: Ulid) -> Result<Option<SegmentRow>>;

    /// Read the segment records matching a query, oldest first.
    ///
    /// Ordered by seal time so that the publisher works through a backlog in
    /// the order it accumulated, which is also the order that empties the disk
    /// soonest, since the oldest segment is the one whose day folder is most
    /// likely already committed.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn segments(&self, query: SegmentQuery) -> Result<Vec<SegmentRow>>;

    /// Bring shards in from cold storage so the domains are local.
    ///
    /// Warming is what makes 100 billion URLs possible against 342 GB of local
    /// disk: local is a cache and object storage is the truth, per doc 08.6.
    /// It is speculative and asynchronous by design, so the scheduler calls
    /// this ahead of the domains it is about to work rather than blocking on a
    /// 50 to 100 ms object GET inside its loop.
    ///
    /// Warming a domain that is already resident, or one that has no shard
    /// because we have never seen it, is a no op rather than an error.
    /// Backends that do not shard implement this as a no op entirely.
    ///
    /// # Errors
    ///
    /// [`StateError::ShardUnavailable`] when a shard exists but could not be
    /// fetched or did not match its digest.
    async fn warm(&self, plds: &[PldId]) -> Result<()>;

    /// Seal, upload and drop shards for domains that have gone idle.
    ///
    /// A domain with leases in flight is kept and counted in
    /// [`EvictReport::in_use`], because evicting it would strand the
    /// completions. A domain that is not resident is counted in
    /// [`EvictReport::not_resident`] and is not an error.
    ///
    /// **Durability: durable.** A shard is in cold storage and its digest is
    /// in the manifest before the local copy is dropped. Nothing is deleted
    /// from cold storage here: old versions are garbage collected after seven
    /// days, which is the rollback window.
    ///
    /// # Errors
    ///
    /// Whatever the store or the object store reports. An error means some
    /// shards may have been evicted and the report is not returned, so the
    /// caller re-reads [`resident`](State::resident) rather than assuming.
    async fn evict(&self, plds: &[PldId]) -> Result<EvictReport>;

    /// Which domains are local right now.
    ///
    /// Sorted, so two calls with nothing in between compare equal. A backend
    /// with no cold tier answers with every domain it holds a URL for, because
    /// on such a backend local is the only place anything is. Empty means the
    /// store is empty and nothing else, which is what lets doc 09.8's restart
    /// rebuild the domain schedule from this and trust the answer.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn resident(&self) -> Result<Vec<PldId>>;

    /// Read one domain's rows out, in key order, so they can be written into a
    /// frontier segment.
    ///
    /// The read half of doc 08.6's evict, and the one place the trait hands
    /// back whole rows. That is not a hole in the "no `get(url)`" rule next
    /// door: this is a sequential walk of one domain's contiguous key range,
    /// which is the shape the ledger is stored in, and it is called once per
    /// eviction rather than once per URL.
    ///
    /// `after` is where to carry on from, exclusive, so a domain larger than
    /// one batch is read in pieces without holding a cursor across calls. The
    /// answer is in key order and no longer than `limit`, and a short answer is
    /// the end of the domain.
    ///
    /// The URL text and the ETag come back resolved rather than as references,
    /// because the interning pool is local and the segment this is going into
    /// is not. A caller that wrote `etag_ref` into a published file would be
    /// writing an index into a table nobody else has.
    ///
    /// Nothing is changed by this call. Evicting is publish, check the read
    /// back digest, move the index, then [`unload`](State::unload), and a read
    /// that mutated would make the first three steps unrepeatable after a
    /// crash.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn spill(&self, pld: PldId, after: Option<UrlKey>, limit: usize)
    -> Result<Vec<SpillRow>>;

    /// Drop a domain's local rows, because they are safely on the hub.
    ///
    /// The last step of an eviction and the only one that loses anything. The
    /// caller is asserting that every row it read with [`spill`](State::spill)
    /// is in a published file whose read back digest matched, which is doc
    /// 12.7's fourth condition applied to state instead of to pages, and this
    /// call cannot check that for itself.
    ///
    /// A domain with a lease in flight is left alone, because dropping its rows
    /// would strand the completion, and the returned list is what was actually
    /// unloaded so the caller can tell. The seen set is not touched: it is the
    /// one structure doc 08.6 keeps local at any size, and dropping
    /// fingerprints would mean re-admitting every URL the next time a page
    /// linked to one.
    ///
    /// **Durability: durable.**
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn unload(&self, plds: &[PldId]) -> Result<Vec<PldId>>;

    /// Record where a domain's rows went when they were evicted.
    ///
    /// Doc 08.6's local index, one entry per domain. An entry for a domain that
    /// already has one replaces it, because the index is what makes a version
    /// current: the older rows stay in the file they were written to, nothing
    /// points at them any more, and the file they are in is the rollback window
    /// for as long as it exists.
    ///
    /// Writing an entry is not what makes an eviction safe. The order doc 08.6
    /// asks for is publish, check the read back digest, move the index, then
    /// drop the local rows, so a caller that writes an entry before the segment
    /// is on the hub has written a pointer to a file the hub has never heard
    /// of.
    ///
    /// **Durability: durable.** The index is the only thing standing between a
    /// restart and a backlog nobody can find again.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn put_shards(&self, shards: &[Shard]) -> Result<()>;

    /// Where the named domains' rows are, for the ones that have been evicted.
    ///
    /// Only the ones that have an entry, in the order they were asked for, so a
    /// shorter answer than the question is the normal case and means the rest
    /// are local or unknown. A domain that has never had a URL admitted and one
    /// whose rows are all resident look the same here, which is correct: both
    /// have nothing to fetch back.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn shards(&self, plds: &[PldId]) -> Result<Vec<Shard>>;

    /// Forget where a domain's rows were, because they are local again.
    ///
    /// The other half of a warm. The published file is untouched and stays
    /// where it is, doc 12's rule holding here as everywhere else. What goes is
    /// the pointer, so that the next eviction writes a fresh one and a read of
    /// the index never hands back a range whose rows are also in the ledger.
    ///
    /// Domains with no entry are skipped rather than reported, in keeping with
    /// [`warm`](State::warm) treating an already resident domain as a no op.
    ///
    /// **Durability: durable.**
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn clear_shards(&self, plds: &[PldId]) -> Result<()>;

    /// Take a consistent point in time snapshot, for publishing and analytics.
    ///
    /// The snapshot is a value nothing further mutates. It is what
    /// `umi checkpoint --format duckdb` attaches to and what doc 15's
    /// dashboard queries, and it exists so that analytics never touches the
    /// live store.
    ///
    /// [`Checkpoint::sequence`] is monotonic within one store's lifetime, so
    /// two checkpoints can always be ordered. It carries
    /// [`CANON_VERSION`](umi_types::CANON_VERSION), because a consumer reading
    /// a checkpoint taken under a different canonicalisation is looking at keys
    /// it cannot join against.
    ///
    /// `now_ms` is what [`Checkpoint::taken_ms`] is stamped with. Doc 08.4
    /// writes this method without a time argument, but a backend cannot fill
    /// that field without reading a clock, and nothing in this crate reads a
    /// clock. Leaving the field at zero would have been the alternative, and a
    /// snapshot an operator cannot date is not much of an artefact.
    ///
    /// **Durability: durable.** Everything a completed
    /// [`complete`](State::complete) recorded before this call is in the
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn checkpoint(&self, now_ms: u64) -> Result<Checkpoint>;

    /// The counters an operator watches.
    ///
    /// Point in time and not promised to be exact under concurrent admission,
    /// because making them exact would put a lock across the hot path. They
    /// are for dashboards and capacity planning, not for accounting.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn stats(&self) -> Result<StateStats>;
}

/// A shared store is a store.
///
/// The crawl loop holds its store as an `Arc<dyn State>`, because the daemon,
/// the CLI and the tests all want the same one from more than one place, and
/// the frontier in umi-frontier takes its store by value so that it can hand it
/// back. Without this the two cannot be composed and the loop has to choose
/// between the scheduler and being shared, which is not a real choice.
///
/// Every method forwards and nothing else happens here. `?Sized` so that
/// `Arc<dyn State>` is covered and not just `Arc<SqliteState>`.
#[async_trait::async_trait]
impl<T: State + ?Sized> State for std::sync::Arc<T> {
    async fn admit(&self, batch: &[Candidate<'_>]) -> Result<AdmitReport> {
        (**self).admit(batch).await
    }

    async fn lease(&self, req: &LeaseRequest<'_>) -> Result<Vec<Lease>> {
        (**self).lease(req).await
    }

    async fn complete(&self, outcomes: &[FetchOutcome]) -> Result<()> {
        (**self).complete(outcomes).await
    }

    async fn release(&self, lease_ids: &[LeaseId], reason: NackReason) -> Result<()> {
        (**self).release(lease_ids, reason).await
    }

    async fn host(&self, id: HostId) -> Result<Option<HostRow>> {
        (**self).host(id).await
    }

    async fn put_host(&self, rows: &[HostRow]) -> Result<()> {
        (**self).put_host(rows).await
    }

    async fn block(&self, rows: &[BlockRow]) -> Result<BlockReport> {
        (**self).block(rows).await
    }

    async fn blocks(&self) -> Result<Vec<BlockRow>> {
        (**self).blocks().await
    }

    async fn supervise(&self, rows: &[SupervisionRow]) -> Result<usize> {
        (**self).supervise(rows).await
    }

    async fn supervision(&self) -> Result<Vec<SupervisionRow>> {
        (**self).supervision().await
    }

    async fn put_segment(&self, rows: &[SegmentRow]) -> Result<()> {
        (**self).put_segment(rows).await
    }

    async fn segment(&self, id: Ulid) -> Result<Option<SegmentRow>> {
        (**self).segment(id).await
    }

    async fn segments(&self, query: SegmentQuery) -> Result<Vec<SegmentRow>> {
        (**self).segments(query).await
    }

    async fn warm(&self, plds: &[PldId]) -> Result<()> {
        (**self).warm(plds).await
    }

    async fn evict(&self, plds: &[PldId]) -> Result<EvictReport> {
        (**self).evict(plds).await
    }

    async fn spill(
        &self,
        pld: PldId,
        after: Option<UrlKey>,
        limit: usize,
    ) -> Result<Vec<SpillRow>> {
        (**self).spill(pld, after, limit).await
    }

    async fn unload(&self, plds: &[PldId]) -> Result<Vec<PldId>> {
        (**self).unload(plds).await
    }

    async fn put_shards(&self, shards: &[Shard]) -> Result<()> {
        (**self).put_shards(shards).await
    }

    async fn shards(&self, plds: &[PldId]) -> Result<Vec<Shard>> {
        (**self).shards(plds).await
    }

    async fn clear_shards(&self, plds: &[PldId]) -> Result<()> {
        (**self).clear_shards(plds).await
    }

    async fn resident(&self) -> Result<Vec<PldId>> {
        (**self).resident().await
    }

    async fn checkpoint(&self, now_ms: u64) -> Result<Checkpoint> {
        (**self).checkpoint(now_ms).await
    }

    async fn stats(&self) -> Result<StateStats> {
        (**self).stats().await
    }
}

/// How long to wait after a failure, from the ladder in doc 05.8.
///
/// One minute, five, twenty five, two hours, twelve hours, then daily. The
/// ladder is per URL here; the same shape applies per host to block signals,
/// and the two are separate because a 404 on one page says nothing about the
/// host while a challenge page says everything.
#[must_use]
pub fn retry_after_ms(fail_streak: u8) -> u64 {
    const LADDER_SECS: [u64; 6] = [60, 300, 1500, 7200, 43_200, 86_400];
    let index = usize::from(fail_streak.saturating_sub(1)).min(LADDER_SECS.len() - 1);
    LADDER_SECS[index] * 1000
}
