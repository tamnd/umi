# 08 State layer

State is everything mutable: which URLs we know, which we have fetched, what we found, when to look again, and how each host behaves. It is strictly separated from the crawled results in doc 10, which are immutable and append only. The two never share a file, a lock, or a lifecycle, and that separation is what lets both be fast.

## 8.1 The shape of the problem

The workload is unusual and it is worth stating before any design, because the obvious designs fail on it.

**Writes dominate reads, and most writes are no-ops.** At 750 pages/s and roughly 50 links per page, admission processes about 37500 candidate URLs per second fleet wide, or 12500 per host. Well over 95 percent of those are already known. The dominant operation is therefore "is this 80 bit fingerprint present, and if not insert it", at 12500 per second per host, forever.

**The working set is a tiny slice of the key space.** At any moment the crawler is working a few thousand pay level domains. It does not need the other 200 million.

**The full key space does not fit anywhere near local disk.** 100 billion URLs at 20 bytes of ledger is 2 TB against 342 GB of free disk fleet wide, and that is before the seen set. This is not a tuning problem, it is a structural one.

**Range scans are always within one PLD.** "Give me the next URLs to fetch for example.com" is the only scan shape that matters. There is never a query that ranges across unrelated domains.

Those four facts produce the design: shard by pay level domain, keep hot shards local, keep cold shards on object storage, and batch everything.

## 8.2 Keys

```rust
type PldId  = [u8; 8];    // blake3(registrable_domain)[0..8], PSL derived
type HostId = [u8; 8];    // blake3(host)[0..8]
type UrlKey = [u8; 10];   // blake3(canonical_url)[0..10], 80 bits
```

80 bits for the URL key rather than 64, and the reason is arithmetic. Expected birthday collisions at n keys in a b bit space are about n squared over 2 to the power b+1. At 100 billion keys, 64 bits gives about 271 expected collisions and 80 bits gives about 0.004. A collision means a URL is permanently treated as already seen and never crawled, which is a silent data loss, so 271 of them is 271 too many for a two byte saving.

The full 128 bit blake3 prefix is kept in the ledger, where the extra 6 bytes cost little and make the ledger's identity unambiguous.

Records are ordered by `(PldId, HostId, UrlKey)`. That ordering is the reason everything else works: all of a domain's state is one contiguous range, so a shard is a byte range, eviction is a truncation, and loading a domain is one sequential read.

Canonicalisation is in doc 11.2 and is part of the key. Changing it changes every key in the system, so it is versioned, and a canonicalisation change is a migration with a plan, not a patch.

## 8.3 The five tables

**Seen set.** `UrlKey -> ()`. The membership structure. This is the hot one.

**Ledger.** `UrlKey -> LedgerRow`. What we know about a URL we have fetched or scheduled.

```rust
struct LedgerRow {
    url_key_full:   [u8; 16],
    host_id:        HostId,
    depth:          u8,
    priority:       u16,          // fixed point, doc 09
    state:          UrlState,     // Pending | Fetched | Failed | Gone | Excluded
    next_due_ms:    u64,          // when to fetch or refetch
    last_fetch_ms:  u64,
    last_change_ms: u64,          // last time content_hash actually changed
    fetch_count:    u32,
    change_count:   u32,          // for the change rate estimator
    content_hash:   [u8; 8],      // truncated blake3 of the extracted text
    etag_ref:       u32,          // interned into a per shard ETag pool
    last_mod_ms:    u64,
    status:         u16,
    tier_used:      u8,
    fail_streak:    u8,
}
```

That is 76 bytes laid out naively and 24 to 32 bytes once the shard encoding in 8.6 does its work, which is where the "under 20 bytes per known URL" target in doc 01 comes from and why that target is stated as a measurement rather than a promise.

**Host records.** `HostId -> HostRow`. Robots cache pointer, adaptive delay, tier policy from doc 05.8, error counters, next allowed fetch time, `Content-Usage` string, sitemap pointers, block flags. Small, maybe 50 million rows fleet wide, and the whole thing fits in memory even on server1.

**Holding pen.** `(FetcherId, UrlKey) -> PendingDiscovery`. Doc 06.2 layer 7. Bounded and expiring, so it never grows without limit.

**Segments.** `SegmentUlid -> SegmentRow`. This one was not in the first draft of this doc, and doc 12.7's fourth GC condition does not work without it: a local file is only deleted once "the state ledger rows for that segment carry the remote repository, path and digest", and there was nowhere for those to live.

```rust
struct SegmentRow {
    stream:        StreamKind,   // Pages | Receipts | Robots
    local_path:    String,
    sealed_at_ms:  u64,
    rows:          u64,
    bytes:         u64,
    local_digest:  Digest,       // blake3 of the sealed .umi file
    remote_repo:   Option<String>,
    remote_path:   Option<String>,
    remote_digest: Option<Digest>,   // read back, not the upload's echo
    manifest_day:  Option<u32>,      // YYYYMMDD of the manifest that lists it
    deleted_at_ms: Option<u64>,
}
```

It is tiny by construction. A coordinator seals about a thousand segments a day and a row is under 200 bytes, so a year of history is 70 MB and there is no reason to prune it, which is convenient because the row surviving the file is exactly what lets an operator answer "where did that segment go" after the local copy is gone. The three `remote_*` columns move from null to set in one write, after the read back digest has been compared, so a crash can leave a segment unpublished but can never leave it half published in a way that satisfies condition 4.

## 8.4 The trait

Narrow on purpose. It is not a plugin system and it is not a database abstraction. It is the four operations the crawler actually performs, in batch form, because per row operations at 12500 per second are the thing that kills naive designs.

```rust
#[async_trait]
pub trait State: Send + Sync + 'static {
    /// Dedup a batch of candidates against the seen set and enqueue the new ones.
    /// The single hottest call in the system. Must be O(batch) amortised, not
    /// O(batch * log n), and must not do one round trip per candidate.
    async fn admit(&self, batch: &[Candidate]) -> Result<AdmitReport>;

    /// Hand out work. Respects per host politeness, per PLD caps and priority.
    /// Returns leases already marked in flight with the given deadline.
    async fn lease(&self, req: &LeaseRequest) -> Result<Vec<Lease>>;

    /// Record outcomes, update the change rate model, compute next_due.
    /// Must be durable before it returns.
    async fn complete(&self, outcomes: &[FetchOutcome]) -> Result<()>;

    /// Release leases without an outcome. Reschedules immediately.
    async fn release(&self, lease_ids: &[LeaseId], reason: NackReason) -> Result<()>;

    async fn host(&self, id: HostId) -> Result<Option<HostRow>>;
    async fn put_host(&self, rows: &[HostRow]) -> Result<()>;

    /// Shard lifecycle. Backends that do not shard implement these as no-ops.
    async fn warm(&self, plds: &[PldId]) -> Result<()>;
    async fn evict(&self, plds: &[PldId]) -> Result<EvictReport>;
    async fn resident(&self) -> Result<Vec<PldId>>;

    /// Segment bookkeeping. `published` is the write that makes doc 12.7's
    /// fourth GC condition true, and it takes the read back digest so that a
    /// caller cannot make it true by passing the one it uploaded.
    async fn put_segment(&self, rows: &[SegmentRow]) -> Result<()>;
    async fn published(&self, id: SegmentUlid, at: &RemoteObject) -> Result<()>;
    async fn unpublished(&self) -> Result<Vec<SegmentRow>>;

    /// Consistent point in time snapshot for publishing and analytics.
    /// `now_ms` is passed in rather than read, for the reason doc 11.1 gives.
    async fn checkpoint(&self, now_ms: u64) -> Result<Checkpoint>;

    async fn stats(&self) -> Result<StateStats>;
}
```

Every call that stamps a time takes it as an argument. Doc 11.1 requires that the same input bytes and the same version produce the same output on every machine, and a store that reads the clock cannot be tested against that requirement, because the test would have to assert on a value the test cannot control. The clock is read once, at the top of the crawl loop, and passed down. This costs one parameter on a handful of methods and buys a state layer whose every behaviour is reproducible from a fixture.

Everything else is derived. There is deliberately no `get(url)`, no `scan()`, no generic query. If a caller needs to ask something else, it asks DuckDB against a published checkpoint, not the live store.

`admit` returning `AdmitReport` rather than a bool per candidate matters:

```rust
struct AdmitReport {
    seen: u32,          // already known, dropped
    admitted: u32,      // new, enqueued
    held: u32,          // went to the holding pen (doc 06)
    excluded: u32,      // robots, block list, scope filter
    shard_misses: u32,  // PLDs that had to be warmed from cold storage
}
```

`shard_misses` is the number the operator watches. It is the state layer's cache miss rate and it is the difference between a crawl running at rate and a crawl waiting on object storage.

## 8.5 Backends

Four, with a clear statement of where each one stops. That statement matters more than the implementations, because the failure mode here is someone running the default backend at a scale it was never meant for and concluding the design is wrong.

### sqlite, the default

`rusqlite` with WAL, one database file per coordinator, one table per structure, `WITHOUT ROWID` on the seen set keyed by `UrlKey`.

Configuration that is not optional: `journal_mode=WAL`, `mmap_size` set to the smaller of the file size and available memory, `cache_size` negative to express KiB, `wal_autocheckpoint` tuned so checkpoints do not stall admission.

`synchronous` is the one setting that cannot be stated once for the whole connection, and an earlier draft of this doc said `NORMAL` and left it there, which contradicts 8.7. Under WAL with `synchronous=NORMAL` a committed transaction is not on the platter until the next checkpoint, so a power loss loses it. That is the correct trade for `admit`, which 8.7 explicitly allows to lose a group commit window, and the wrong trade for `complete` and for lease issue, which 8.7 requires to be durable before they return. So the setting is per transaction: `PRAGMA synchronous=NORMAL` is the connection default and the `complete` and `lease` paths run `PRAGMA synchronous=FULL` for the duration of their transaction. Two connections with different defaults would be tidier and would cost a second write lock on a single writer database, which is worse. `admit` is one prepared `INSERT OR IGNORE` inside one transaction per batch, and the batch is the whole point: 4096 inserts in one transaction is roughly three orders of magnitude faster than 4096 transactions.

**Where it stops: about 100 million URLs on this hardware.** Beyond that the seen set index stops fitting in page cache and admission goes to disk on every candidate, which at 12500 per second is over. On server1's SSD it might reach 200 million. On server2's rotational disk it will struggle at 50 million.

This is the right default anyway, because a focused crawl of one site is under a million URLs, the file is portable and inspectable with any SQLite tool, there is no daemon, and correctness is not in question. Most people running umi will never exceed it.

### nami, the experimental single file engine

`umi-nami`, the 波 to umi's 海. This is the backend that targets the full design, and it is experimental until milestone 3 proves it.

Three structures in one file, all built around the `(PldId, HostId, UrlKey)` ordering.

**The seen set** is not a B-tree and not a general LSM. It is a per PLD sorted array of 80 bit fingerprints, delta encoded and bit packed, with a per shard block index. Sorted fingerprints in a dense key space delta encode extremely well: at 100 million URLs under one PLD the average gap is small and the deltas pack into far fewer than 80 bits. Measured expectation is 4 to 6 bytes per key against 10 raw, and that number is a milestone 3 gate.

Membership testing goes through a two level front line. In memory, per resident shard, a ribbon filter at about 1.1 bytes per key and a 0.5 percent false positive rate. A filter hit falls through to the packed array, which is a binary search over the block index then a scan of one 4 KiB block. A filter miss is a definitive absent and costs nothing. At 0.5 percent FP and a 95 percent true-seen rate, the array is touched on about 95.5 percent of candidates, so the filter mostly saves work on the genuinely new URLs, which is the case where we are about to do a lot of other work anyway.

Insertion never touches the packed array. New fingerprints go to an in memory sorted buffer, and when it reaches 64k entries it is merged into the shard's array by a sequential rewrite of that shard. This is the DRUM idea from IRLbot, and it is the reason nami can absorb 12500 candidates per second without random I/O.

**The ledger** is a per PLD columnar block, one column per field, using the same encodings as doc 10: delta plus bit packing for the timestamps and counters, dictionary for the small enums, FSST for the interned ETags. Column layout rather than row layout because the scheduler reads `next_due_ms` and `priority` for a whole domain at once and does not care about the other fields, and reading 2 columns of 100k rows is 400 KB rather than 7.6 MB.

**The frontier index** is a per PLD min-heap over `next_due_ms` for pending URLs, plus a global heap over hosts keyed by next allowed fetch time. The global heap is small because it is per host, not per URL, and it is the Mercator structure from doc 09 almost unchanged.

The file itself follows the same crash safety discipline as doc 10: 4 KiB blocks, a blake3 truncated checksum per block, a generation counter in the header, a commit record written last, and a torn tail truncated on open. Unlike doc 10 it is not append only, since shards get rewritten in place during merges, so it also carries a small redo log for the merge operation. A merge is idempotent, so replay is safe.

**Where it stops: whatever fits on disk plus whatever object storage can page in.** That is the point of it.

### postgres

For a coordinator that wants durable, queryable, replicated state, or for a deployment with more than one process touching state. Partition the seen set and ledger by `PldId` range using declarative partitioning, use `COPY` into a temp table plus `INSERT ... ON CONFLICT DO NOTHING` for `admit`, and use `SELECT ... FOR UPDATE SKIP LOCKED` for `lease`, which is exactly the pattern this workload wants.

**Where it stops: a few billion URLs with real partitioning and a real server**, which none of server1, server2 or server3 is. It is here because it is the right answer for someone running umi on a proper machine, and because it makes the dashboard trivial.

### duckdb

Read mostly, for analytics. It attaches to a published state checkpoint rather than to the live store, because DuckDB is not built for 12500 point writes per second and using it that way would be a category error.

`umi checkpoint --format duckdb` writes a `.duckdb` file with the ledger, host and holding pen tables as columnar relations, plus views for the queries operators actually run: crawl rate by TLD, tier distribution by host, staleness distribution, frontier depth by PLD, fetcher reputation over time, block signal rate by vendor. Doc 15 builds the dashboard on this.

It also reads the published Parquet directly, so the same DuckDB session can join live frontier state against the published corpus. That combination is what makes monitoring pleasant rather than a chore, and it is a large part of why the state layer is abstracted at all.

## 8.6 Cold state, the part that makes 100 billion possible

At 100 billion URLs the state is roughly 2 TB in nami's encoding, against 342 GB of free local disk. So state has the same lifecycle as data: local is a cache, object storage is the truth.

The unit is the PLD shard. A shard holds the seen set, ledger, and frontier index for one registrable domain, self contained, in the same encoding used in the local file. Shards are content addressed by the blake3 of their bytes and stored at `s3://umi-state/<pld_id[0:2]>/<pld_id>.nami-shard`, with the mapping from PLD to current shard digest held in a small manifest that does fit locally.

Lifecycle:

**Warm.** The scheduler decides to work a PLD. If the shard is not resident, fetch it, verify the digest, and map it in. A shard for a typical domain is a few hundred KB and a warm costs one object GET, so 50 to 100 ms.

**Work.** All operations are local. The shard is dirty.

**Evict.** When the domain goes idle, or under memory or disk pressure, the shard is sealed, rewritten with its merges applied, uploaded, and the manifest updated to the new digest. Then the local copy is dropped.

**Forget.** Nothing is deleted from object storage. Old shard versions are garbage collected after 7 days, which gives a rollback window.

The resident set is sized by disk, not by policy: hold as many shards as fit in the local state budget, evict least recently used. On server3 with 112 GB free, splitting the budget with the data segments, roughly 40 GB of state is maybe 60 to 100 thousand resident domains. That is far more than the few thousand being actively worked, so the hit rate should be very high, and `shard_misses` from `admit` is how we find out it is not.

The failure mode to design against is thrash. A crawl that touches domains uniformly at random defeats the cache entirely. The scheduler in doc 09 therefore works in domain batches with locality as an explicit objective, and that is not a performance tweak, it is what makes the cold tier viable.

SlateDB was considered for this and rejected for the hot path, because its 50 to 100 ms object round trips are fine for a memtable flush and hopeless for a membership check. What we take from it is the batching discipline and the observation that object storage is a perfectly good durability tier if you never put it in the critical path of a point read.

## 8.7 Consistency and durability

State is single writer per coordinator. There is no distributed transaction and there is no consensus protocol. A PLD is owned by exactly one coordinator at a time, per doc 03.3, so there is no concurrent mutation to resolve.

`complete` is durable before it returns. A lease is durable before it is handed out. Everything else, including `admit`, is allowed to buffer and can lose up to the group commit window, which is 200 ms by default. Losing a batch of admissions costs re-discovering those URLs the next time we crawl a page that links to them, which is free.

Crash recovery is: open the file, verify the header generation, scan back to the last valid commit record, truncate the torn tail, replay the merge redo log, and re-issue leases whose deadline has passed. Recovery time is bounded by the redo log size, which is capped at 64 MB.

There is no replication. State is reconstructable from the cold shards, which are on object storage with the provider's durability, plus at most the last group commit window. Losing a coordinator loses minutes, not the crawl.

## 8.8 What lives in state and what does not

In state: URL fingerprints, ledger rows, host records, robots cache, tier policy, politeness timers, holding pen, fetcher reputation, block list.

Not in state: page content, extracted text, links as content, receipts, anything that gets published. Those go to `.umi` segments and then to Parquet, and they are never read back by the crawler. The one exception is `content_hash` in the ledger, which is 8 bytes and is how change detection works without reading the corpus.

The test for whether something belongs in state is whether the crawler needs it to decide what to do next. If not, it is data, and it leaves.
