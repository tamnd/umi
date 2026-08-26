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

use umi_types::{HostId, PldId, Ulid};

pub mod conformance;
pub mod freshness;
pub mod memory;
pub mod pace;
mod types;

#[cfg(test)]
mod tests;

pub use freshness::{
    INITIAL_REFRESH, MAX_REFRESH, MIN_REFRESH, next_due_after, refresh_interval_ms,
};
pub use memory::MemoryState;
pub use pace::Pace;
pub use types::{
    AdmitReport, Candidate, Checkpoint, Discovery, EvictReport, ExcludeReason, FailureKind,
    FetchOutcome, FetchResult, HostRow, Lease, LeaseId, LeaseRequest, LedgerRow, NackReason,
    Priority, RemoteCopy, Revalidator, RobotsRef, SegmentQuery, SegmentRow, StateStats, Stream,
    TierPolicy, UrlState,
};

/// The batch size the whole design is tuned around, from doc 08.5.
///
/// Four thousand and ninety six inserts in one transaction is roughly three
/// orders of magnitude faster than 4096 transactions, and that ratio is the
/// reason every method here takes a slice. A backend may accept larger
/// batches, and must not silently truncate one.
pub const BATCH: usize = 4096;

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
/// Fourteen methods, all batched, all taking time as an argument. Implement it
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
    /// leases. That is what makes a crawl replayable.
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
    /// Sorted, so two calls with nothing in between compare equal. Empty on a
    /// backend that does not shard, which is not the same as "no domains
    /// known" and is why nothing schedules off this alone.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    async fn resident(&self) -> Result<Vec<PldId>>;

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
