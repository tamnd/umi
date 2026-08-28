//! SQLite state backend, the portable default.
//!
//! Specified in `docs/spec/08-state-layer.md` section 8.5. This is what
//! `umi crawl` uses when nobody has said otherwise, and it is the default for
//! one reason: the first command a person runs has to work with no setup, and a
//! file is the only thing that satisfies that. There is no daemon, no
//! connection string, no schema to install, and the result is a single file that
//! any SQLite tool can open.
//!
//! # Where it stops
//!
//! About 100 million URLs on this hardware. Past that the seen set index stops
//! fitting in page cache and admission goes to disk on every candidate, which at
//! 12500 candidates a second is over. On an SSD it may reach 200 million; on a
//! rotational disk it will struggle at 50 million. That number is not a defect
//! to be tuned away, it is what this backend is for, and `nami` is what the
//! 100 billion URL design in doc 08.6 is for.
//!
//! It is the right default anyway. A focused crawl of one site is under a
//! million URLs, the file is portable and inspectable, and correctness is not in
//! question. Most people running umi will never exceed it.
//!
//! # Portability
//!
//! A crawl directory can be copied between an x86 machine and an arm one and
//! resumed. That is not luck. The SQLite file format is fixed and big endian on
//! disk, so it is architecture independent by itself, and this crate never
//! hands SQLite a serialised Rust value to store opaquely. Every key is a blob
//! of the bytes the key type already sorts by, every number is a SQLite
//! integer, and every enum is the byte its `from_u8` accepts. There is nothing
//! in the file whose meaning depends on the machine that wrote it.
//!
//! # Durability
//!
//! Doc 08.7 splits the trait's methods into buffered and durable, and this
//! backend implements that split literally rather than picking one setting for
//! everything.
//!
//! The connection runs in WAL mode. `synchronous` is `NORMAL` for the buffered
//! methods, which means a commit survives a process crash and may lose the last
//! transactions to a power cut, and that is exactly the group commit window doc
//! 08.7 allows [`admit`](umi_state::State::admit) to lose. For the durable
//! methods, [`lease`](umi_state::State::lease),
//! [`complete`](umi_state::State::complete),
//! [`evict`](umi_state::State::evict),
//! [`checkpoint`](umi_state::State::checkpoint) and a
//! [`put_host`](umi_state::State::put_host) carrying a blocked host, it is
//! `FULL` for the duration of that transaction, so the commit is fsynced before
//! the call returns. Changing the pragma is a connection level setting that
//! takes effect immediately and costs nothing, so the two promises can share one
//! connection.
//!
//! # Blocking
//!
//! SQLite is synchronous. Every method here does its work under
//! [`tokio::task::block_in_place`] when it is running on a multi threaded
//! runtime, so a batch transaction moves the other tasks off the worker thread
//! instead of stalling them, and runs inline otherwise. Batching is what keeps
//! that bounded: one transaction of 4096 admissions is a few milliseconds, and
//! at 250 pages/s per server there are only a handful of transactions a second.

#![forbid(unsafe_code)]

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};
use umi_state::{
    AdmitReport, CLASSES, Candidate, Checkpoint, Discovery, EvictReport, FetchOutcome, FetchResult,
    HostRow, Lease, LeaseId, LeaseRequest, LedgerRow, NackReason, Priority, RefreshClass, Result,
    Revalidator, SegmentQuery, SegmentRow, State, StateError, StateStats, UrlState, next_due_after,
    retry_after_ms,
};
use umi_types::{CANON_VERSION, Digest, HostId, PldId, RowKey, Tier, Ulid, UrlKeyFull};

mod row;
mod schema;
mod sql;

#[cfg(test)]
mod tests;

pub use schema::{APPLICATION_ID, SCHEMA_VERSION};

/// How many ledger rows [`lease`](State::lease) will look at before it stops
/// looking.
///
/// It normally stops as soon as it has what it was asked for, because the index
/// hands rows back in exactly the order the trait promises. The bound is for one
/// pathological shape: thousands of due URLs on a single host sitting at the
/// head of the queue under a small per host cap, where every row after the cap
/// is skipped. Returning fewer leases than asked for is already normal and is
/// documented as such on the trait, so hitting this is not an error, but it is
/// worth knowing it exists.
///
/// It is the total over the class scans and not a bound on each of them, so
/// splitting a batch across doc 09.5's six classes cannot cost six times the
/// scan.
pub const LEASE_SCAN_LIMIT: usize = 65_536;

/// How the store is opened.
///
/// The pragmas doc 08.5 calls not optional are not configurable: WAL and the
/// `synchronous` split above are the backend, not a preference. What is here is
/// the sizing, which depends on the machine.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SqliteConfig {
    /// Where the file lives, or `None` for an in memory store.
    pub path: Option<PathBuf>,
    /// SQLite page cache, in bytes. The pragma takes KiB as a negative number,
    /// which this converts, because expressing a cache in pages means the
    /// answer changes when the page size does.
    pub cache_bytes: u64,
    /// Ceiling on the memory mapped window. SQLite grows the mapping up to the
    /// smaller of this and the file size, so setting it larger than the file
    /// costs nothing today and helps once the file grows.
    pub mmap_bytes: u64,
    /// WAL pages before SQLite folds the log back into the main file. Small
    /// enough that a checkpoint is quick, large enough that admission is not
    /// checkpointing on every batch.
    pub wal_autocheckpoint_pages: u32,
    /// How long to wait on a locked database before giving up. Only reachable
    /// when something else has the file open, which the single writer rule in
    /// doc 08.7 says should not happen.
    pub busy_timeout: Duration,
    /// Whether [`checkpoint`](State::checkpoint) writes a separate snapshot
    /// file.
    ///
    /// On, a checkpoint is `VACUUM INTO` a new file, which is a real point in
    /// time value that nothing further mutates and that DuckDB can attach to.
    /// Off, a checkpoint is a durability barrier and a sequence number and
    /// nothing else, which is what an operator short of disk wants.
    pub snapshots: bool,
    /// Where snapshots go. Defaults to a `.checkpoints` directory beside the
    /// database file.
    pub snapshot_dir: Option<PathBuf>,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: None,
            cache_bytes: 64 * 1024 * 1024,
            mmap_bytes: 1024 * 1024 * 1024,
            wal_autocheckpoint_pages: 4096,
            busy_timeout: Duration::from_secs(5),
            snapshots: true,
            snapshot_dir: None,
        }
    }
}

impl SqliteConfig {
    /// A configuration for a store at `path`, with the default sizing.
    #[must_use]
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            ..Self::default()
        }
    }

    fn snapshot_dir(&self) -> Option<PathBuf> {
        if !self.snapshots {
            return None;
        }
        if let Some(dir) = &self.snapshot_dir {
            return Some(dir.clone());
        }
        let path = self.path.as_ref()?;
        let name = path.file_name()?.to_string_lossy().into_owned();
        Some(path.with_file_name(format!("{name}.checkpoints")))
    }
}

/// The [`State`] trait on one SQLite file.
#[derive(Debug)]
pub struct SqliteState {
    inner: Mutex<Inner>,
    config: SqliteConfig,
}

#[derive(Debug)]
struct Inner {
    conn: Connection,
    /// Hosts an operator has blocked, so [`admit`](State::admit) can reject a
    /// candidate without a query. There are only about 50 million host records
    /// fleet wide and a vanishing fraction of them are blocked, so this is
    /// small enough to hold and important enough to be exact.
    blocked: HashSet<HostId>,
    next_lease: u64,
    checkpoint_seq: u64,
}

/// Whether this transaction is one of the ones doc 08.7 requires on disk before
/// the call returns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Sync {
    /// Survives a process crash, may lose the last commits to a power cut.
    Buffered,
    /// Fsynced before the commit returns.
    Durable,
}

impl SqliteState {
    /// Open, creating and migrating the file if it is not there yet.
    ///
    /// # Errors
    ///
    /// [`StateError::Backend`] if SQLite cannot open the file, and
    /// [`StateError::Corrupt`] if the file is a database written by a newer umi
    /// than this one.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with(SqliteConfig::at(path))
    }

    /// A store with no file behind it, for tests.
    ///
    /// The durability promises are vacuous here, so this is not a way to run a
    /// crawl. It is a way to exercise the same SQL the real store runs.
    ///
    /// # Errors
    ///
    /// [`StateError::Backend`] if SQLite will not create the database.
    pub fn in_memory() -> Result<Self> {
        Self::open_with(SqliteConfig::default())
    }

    /// Open with an explicit configuration.
    ///
    /// # Errors
    ///
    /// As [`open`](Self::open).
    pub fn open_with(config: SqliteConfig) -> Result<Self> {
        let conn = match &config.path {
            Some(path) => {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    std::fs::create_dir_all(parent)?;
                }
                Connection::open(path).state()?
            }
            None => Connection::open_in_memory().state()?,
        };

        configure(&conn, &config)?;
        migrate(&conn)?;

        let blocked = load_blocked(&conn)?;
        let next_lease = meta_u64(&conn, "next_lease")?.unwrap_or(0);
        let checkpoint_seq = meta_u64(&conn, "checkpoint_seq")?.unwrap_or(0);

        Ok(Self {
            inner: Mutex::new(Inner {
                conn,
                blocked,
                next_lease,
                checkpoint_seq,
            }),
            config,
        })
    }

    /// Where the file is, or `None` for an in memory store.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        self.config.path.as_deref()
    }

    /// Recompute the counters behind [`State::stats`] by scanning, and say
    /// where they disagree with what the store is carrying.
    ///
    /// `stats` reads a maintained row, which is what makes it free and what
    /// makes it worth checking. The triggers that maintain it are inside every
    /// writing transaction, so the counts and the rows are committed together
    /// and a crash cannot separate them. What can separate them is somebody
    /// editing the file with the `sqlite3` shell, restoring half a backup, or a
    /// bug in a trigger, and none of those announce themselves. So there is a
    /// scan available on demand, and it is the same scan the migration used to
    /// fill the row in.
    ///
    /// The returned list is empty when everything agrees. Each entry is the
    /// name of a counter, what the row says and what the scan says, in that
    /// order, because a number that is wrong is only useful next to the number
    /// it should have been.
    ///
    /// # Errors
    ///
    /// Whatever reading the store reports.
    pub fn recount(&self) -> Result<Vec<(&'static str, u64, u64)>> {
        /// The names, in the column order both queries share.
        const NAMES: [&str; 9] = [
            "seen", "pending", "fetched", "failed", "gone", "excluded", "held", "hosts", "leases",
        ];

        let guard = self.lock();
        let held = counters(&guard.conn, sql::SELECT_COUNTS)?;
        let scanned = counters(&guard.conn, sql::RECOUNT)?;
        Ok(NAMES
            .iter()
            .zip(held)
            .zip(scanned)
            .filter(|((_, held), scanned)| held != scanned)
            .map(|((name, held), scanned)| (*name, held, scanned))
            .collect())
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Recovering the guard rather than propagating the poison keeps one
        // panicking caller from turning every later call into a different
        // error than the one that actually happened.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Run a synchronous block without stalling the runtime it is on.
///
/// On a multi threaded runtime this hands the worker's other tasks to another
/// thread for the duration. On a current thread runtime, or off a runtime
/// entirely, there is nothing to hand them to, so it runs inline. Calling
/// `block_in_place` on a current thread runtime panics, which is why this
/// checks rather than calling it unconditionally.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    use tokio::runtime::{Handle, RuntimeFlavor};

    match Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// The pragmas from doc 08.5, plus the sizing from the config.
fn configure(conn: &Connection, config: &SqliteConfig) -> Result<()> {
    conn.busy_timeout(config.busy_timeout).state()?;

    if config.path.is_some() {
        // WAL is not optional. It is what lets a reader and the writer coexist,
        // and it is what makes the durability split above expressible.
        let mode: String = conn
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .state()?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(StateError::Corrupt(format!(
                "the file would not go into WAL mode, it is in {mode} mode"
            )));
        }
    }

    // `recursive_triggers` is on for one specific reason and it is not
    // recursion. `put_host` and `put_segment` are `INSERT OR REPLACE`, and
    // SQLite only fires the delete trigger for the row REPLACE removes when
    // this is on. With it off, an upsert over an existing host fires the
    // insert trigger and nothing else, and schema version 3's host counter
    // climbs by one every time a host record is written. Nothing here is
    // recursive: the triggers all write to `counts`, and `counts` has no
    // triggers on it, so there is nothing for them to set off.
    //
    // Negative expresses KiB, which is the only way to size the cache in bytes
    // rather than in pages.
    let cache_kib = -i64::try_from(config.cache_bytes / 1024).unwrap_or(i64::MAX);
    conn.execute_batch(&format!(
        "PRAGMA cache_size = {cache_kib};
         PRAGMA mmap_size = {};
         PRAGMA wal_autocheckpoint = {};
         PRAGMA temp_store = MEMORY;
         PRAGMA foreign_keys = OFF;
         PRAGMA recursive_triggers = ON;",
        config.mmap_bytes, config.wal_autocheckpoint_pages
    ))
    .state()?;
    set_sync(conn, Sync::Buffered)
}

fn set_sync(conn: &Connection, sync: Sync) -> Result<()> {
    let value = match sync {
        Sync::Buffered => "NORMAL",
        Sync::Durable => "FULL",
    };
    conn.execute_batch(&format!("PRAGMA synchronous = {value}"))
        .state()
}

/// Create or bring forward the schema.
fn migrate(conn: &Connection) -> Result<()> {
    let have: u32 = conn
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .state()?
        .try_into()
        .unwrap_or(0);

    if have > SCHEMA_VERSION {
        // Opening it read only and dropping the columns we do not understand
        // would be worse: the crawl would run, and it would quietly lose
        // whatever the newer version was keeping.
        return Err(StateError::Corrupt(format!(
            "this state file is at schema {have} and this build understands {SCHEMA_VERSION}, so it was written by a newer umi"
        )));
    }
    if have == SCHEMA_VERSION {
        return Ok(());
    }

    for step in schema::MIGRATIONS.iter().skip(have as usize) {
        conn.execute_batch(step).state()?;
    }
    conn.execute_batch(&format!(
        "PRAGMA application_id = {APPLICATION_ID};
         PRAGMA user_version = {SCHEMA_VERSION};"
    ))
    .state()
}

fn load_blocked(conn: &Connection) -> Result<HashSet<HostId>> {
    let mut stmt = conn
        .prepare("SELECT host FROM hosts WHERE blocked = 1")
        .state()?;
    let hosts = stmt
        .query_map([], |r| row::host(r, "host"))
        .state()?
        .collect::<rusqlite::Result<HashSet<_>>>()
        .state()?;
    Ok(hosts)
}

fn meta_u64(conn: &Connection, key: &str) -> Result<Option<u64>> {
    let value: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0))
        .optional()
        .state()?;
    Ok(value.and_then(|v| v.parse().ok()))
}

impl Inner {
    fn put_meta(&self, key: &str, value: u64) -> Result<()> {
        self.conn
            .execute(sql::PUT_META, params![key, value.to_string()])
            .state()?;
        Ok(())
    }
}

/// `rusqlite::Error` is a backend detail, and the callers above state do not
/// branch on it. They retry, or they stop the crawl.
trait SqlExt<T> {
    fn state(self) -> Result<T>;
}

impl<T> SqlExt<T> for rusqlite::Result<T> {
    fn state(self) -> Result<T> {
        self.map_err(|e| StateError::Backend(Box::new(e)))
    }
}

/// One row `lease` has decided to hand out, read before anything is written.
///
/// Choosing and writing are two passes because a `SELECT` that is being stepped
/// while the same table is updated does not promise which rows it will still
/// see, and the trait promises a deterministic order.
struct Chosen {
    key: RowKey,
    url: String,
    depth: u8,
    priority: Priority,
    attempt: u32,
    etag: Option<String>,
    next_due_ms: u64,
    last_mod_ms: u64,
    delay_ms: u64,
    next_allowed_ms: u64,
    tier: Tier,
    lying_revalidator: bool,
}

/// What a lease call has settled on so far, across the class scans.
struct Picks {
    /// The batch, in the order the scans happened to produce rather than the
    /// order it will be handed out in.
    rows: Vec<Chosen>,
    /// How much of the batch each host has, for `max_per_host`.
    per_host: HashMap<HostId, u32>,
    /// Ledger rows read so far, over every class, against
    /// [`LEASE_SCAN_LIMIT`].
    examined: usize,
}

/// Take up to `want` more rows from one class's cursor, in the order doc 08.4
/// promises.
///
/// The cursor stays open between the two rounds of the split, so the round that
/// spends what a class did not want resumes exactly where the first round
/// stopped. A `LIMIT` and an `OFFSET` would read the same thing twice: SQLite
/// runs the joins for the rows an `OFFSET` skips.
fn pull(
    rows: &mut rusqlite::Rows<'_>,
    want: usize,
    req: &LeaseRequest<'_>,
    picks: &mut Picks,
) -> Result<()> {
    let mut taken = 0usize;
    while taken < want && picks.examined < LEASE_SCAN_LIMIT {
        let Some(r) = rows.next().state()? else { break };
        picks.examined += 1;

        let key = RowKey {
            pld: row::pld(r, "pld").state()?,
            host: row::host(r, "host").state()?,
            url: row::url_key(r, "url_key").state()?,
        };
        let held = picks.per_host.entry(key.host).or_default();
        if *held >= req.max_per_host {
            continue;
        }
        *held += 1;
        taken += 1;

        let adaptive: i64 = r.get("adaptive_delay_ms").state()?;
        let crawl: Option<i64> = r.get("crawl_delay_ms").state()?;
        let preferred =
            Tier::from_u8(u8::try_from(r.get::<_, i64>("tier_preferred").state()?).unwrap_or(0))
                .unwrap_or_default();
        let ceiling =
            Tier::from_u8(u8::try_from(r.get::<_, i64>("tier_max").state()?).unwrap_or(0))
                .unwrap_or_default();

        picks.rows.push(Chosen {
            key,
            url: r.get("url").state()?,
            depth: u8::try_from(r.get::<_, i64>("depth").state()?).unwrap_or(u8::MAX),
            priority: Priority::from_raw(
                u16::try_from(r.get::<_, i64>("priority").state()?).unwrap_or(0),
            ),
            attempt: u32::try_from(r.get::<_, i64>("fetch_count").state()?).unwrap_or(u32::MAX),
            etag: r.get("etag").state()?,
            next_due_ms: row::from_ms(r.get("next_due_ms").state()?),
            last_mod_ms: row::from_ms(r.get("last_mod_ms").state()?),
            delay_ms: adaptive.max(crawl.unwrap_or(0)).max(0) as u64,
            next_allowed_ms: row::from_ms(r.get("next_allowed_ms").state()?),
            tier: preferred.min(ceiling).min(req.max_tier),
            lying_revalidator: r.get::<_, i64>("lying_revalidator").state()? != 0,
        });
    }

    Ok(())
}

#[async_trait::async_trait]
impl State for SqliteState {
    async fn admit(&self, batch: &[Candidate<'_>]) -> Result<AdmitReport> {
        if batch.is_empty() {
            return Ok(AdmitReport::default());
        }

        blocking(|| {
            let mut guard = self.lock();
            let Inner { conn, blocked, .. } = &mut *guard;
            set_sync(conn, Sync::Buffered)?;
            let tx = conn.transaction().state()?;
            let mut report = AdmitReport::default();

            {
                let mut see = tx.prepare_cached(sql::INSERT_SEEN).state()?;
                let mut into_ledger = tx.prepare_cached(sql::INSERT_LEDGER).state()?;
                let mut into_pen = tx.prepare_cached(sql::INSERT_PEN).state()?;

                for candidate in batch {
                    // The seen set decides first, so a url that is already
                    // known is `seen` whatever its ledger state is. That is
                    // what makes admitting the same batch twice idempotent,
                    // and it is why a duplicate inside one batch is seen: the
                    // first occurrence put it in the set.
                    let fresh = see
                        .execute(params![&candidate.key.url.as_bytes()[..]])
                        .state()?;
                    if fresh == 0 {
                        report.seen += 1;
                        continue;
                    }

                    if blocked.contains(&candidate.key.host) {
                        let mut row = pending_row(candidate);
                        row.state = UrlState::Excluded;
                        row.next_due_ms = u64::MAX;
                        insert_ledger(&mut into_ledger, &candidate.key, candidate.url, &row)?;
                        report.excluded += 1;
                        continue;
                    }

                    if let Discovery::Unverified(fetcher) = candidate.discovery {
                        into_pen
                            .execute(params![
                                &fetcher.as_bytes()[..],
                                &candidate.key.url.as_bytes()[..],
                                &candidate.key.pld.as_bytes()[..],
                                &candidate.key.host.as_bytes()[..],
                                candidate.url,
                                i64::from(candidate.depth),
                                i64::from(candidate.priority.raw()),
                                row::to_ms(candidate.discovered_ms),
                            ])
                            .state()?;
                        report.held += 1;
                        continue;
                    }

                    let row = pending_row(candidate);
                    insert_ledger(&mut into_ledger, &candidate.key, candidate.url, &row)?;
                    report.admitted += 1;
                }
            }

            tx.commit().state()?;
            // One file, no cold tier, so nothing was ever warmed. Doc 08.4 is
            // explicit that this is zero on a backend that does not shard, and
            // reporting anything else would make the operator's cache miss rate
            // a fiction.
            report.shard_misses = 0;
            Ok(report)
        })
    }

    async fn lease(&self, req: &LeaseRequest<'_>) -> Result<Vec<Lease>> {
        if req.max_urls == 0 {
            return Ok(Vec::new());
        }

        blocking(|| {
            let mut guard = self.lock();
            let inner = &mut *guard;
            // A lease is durable before it is handed out. Otherwise a crash
            // leaves a fetcher holding work the coordinator has no record of,
            // and the url gets fetched twice.
            set_sync(&inner.conn, Sync::Durable)?;
            let tx = inner.conn.transaction().state()?;

            if !req.plds.is_empty() {
                tx.execute_batch(sql::CREATE_LEASE_PLDS).state()?;
                let mut add = tx.prepare_cached(sql::INSERT_LEASE_PLD).state()?;
                for pld in req.plds {
                    add.execute(params![&pld.as_bytes()[..]]).state()?;
                }
            }

            let max_urls = req.max_urls as usize;
            let mut picks = Picks {
                rows: Vec::new(),
                per_host: HashMap::new(),
                examined: 0,
            };

            // Doc 09.5 splits the batch across the refresh classes, so that
            // discovery cannot crowd out refresh or the reverse. Each class is
            // a prefix of `ledger_ready`, so this is one ordered scan per class
            // rather than one scan of everything and a filter, and the class
            // with two thousand rows behind a hundred thousand discovery rows
            // is still reachable.
            //
            // All six cursors stay open across both rounds below, which is what
            // makes the second round free.
            {
                let statement = if req.plds.is_empty() {
                    sql::SELECT_READY
                } else {
                    sql::SELECT_READY_IN_PLDS
                };
                let now = row::to_ms(req.now_ms);
                let max_tier = i64::from(req.max_tier.as_u8());
                let mut statements = Vec::with_capacity(CLASSES.len());
                for _ in CLASSES {
                    statements.push(tx.prepare_cached(statement).state()?);
                }
                let mut cursors = Vec::with_capacity(CLASSES.len());
                for (stmt, class) in statements.iter_mut().zip(CLASSES) {
                    cursors.push(
                        stmt.query(params![now, max_tier, i64::from(class.as_u8())])
                            .state()?,
                    );
                }

                for (rows, class) in cursors.iter_mut().zip(CLASSES) {
                    let room = max_urls - picks.rows.len();
                    let want = (req.budget.quota(class, req.max_urls) as usize).min(room);
                    pull(rows, want, req, &mut picks)?;
                }

                // A share is a floor and not a cap. Whatever the classes did
                // not want is offered back to them in turn, each carrying on
                // from where it stopped, so a frontier that is all discovery
                // still fills a batch completely and the split costs nothing
                // when there is nothing to split.
                for rows in &mut cursors {
                    let want = max_urls - picks.rows.len();
                    if want == 0 {
                        break;
                    }
                    pull(rows, want, req, &mut picks)?;
                }
            }

            // The scans went class by class, so the batch is in class order and
            // has to be put back into the order doc 08.4 promises before it
            // leaves. It is at most `max_urls` long, which is a thousand.
            let mut chosen = picks.rows;
            chosen.sort_unstable_by(|a, b| {
                b.priority
                    .cmp(&a.priority)
                    .then(a.next_due_ms.cmp(&b.next_due_ms))
                    .then(a.key.cmp(&b.key))
            });

            // The politeness clock this call is advancing, per host. It starts
            // at the stored timer and moves forward by the host's delay for
            // every lease handed out, so a fetcher holding eight urls for one
            // host is told to space them out rather than trusted to.
            let mut clock: HashMap<HostId, u64> = HashMap::new();
            let mut leases = Vec::with_capacity(chosen.len());

            {
                let mut mark = tx.prepare_cached(sql::MARK_LEASED).state()?;
                for row in &chosen {
                    let slot = clock
                        .entry(row.key.host)
                        .or_insert_with(|| row.next_allowed_ms.max(req.now_ms));
                    let not_before_ms = *slot;
                    *slot = not_before_ms.saturating_add(row.delay_ms);

                    inner.next_lease += 1;
                    let id = LeaseId::from_raw(inner.next_lease);
                    let expires_ms = not_before_ms.saturating_add(req.lease_for.as_millis() as u64);

                    mark.execute(params![
                        row::to_ms(id.raw()),
                        row::to_ms(expires_ms),
                        &row.key.pld.as_bytes()[..],
                        &row.key.host.as_bytes()[..],
                        &row.key.url.as_bytes()[..],
                    ])
                    .state()?;

                    let revalidate = Revalidator {
                        etag: row.etag.clone(),
                        last_modified_ms: (row.last_mod_ms != 0).then_some(row.last_mod_ms),
                    };
                    leases.push(Lease {
                        id,
                        key: row.key,
                        url: row.url.clone(),
                        depth: row.depth,
                        priority: row.priority,
                        attempt: row.attempt,
                        tier: row.tier,
                        not_before_ms,
                        expires_ms,
                        // A host that lies about its revalidators gets
                        // unconditional requests, per doc 05.8. Sending one it
                        // will ignore costs a round trip and saves nothing.
                        revalidate: (!revalidate.is_empty() && !row.lying_revalidator)
                            .then_some(revalidate),
                    });
                }
            }

            // Persist the politeness clock, so the next call cannot hand out
            // the same host again immediately. This is the structural half of
            // doc 07.6: a fetcher cannot make a second concurrent request to a
            // host, because a second lease is not issued.
            {
                let mut bump = tx.prepare_cached(sql::BUMP_HOST_CLOCK).state()?;
                for (host, next_allowed_ms) in &clock {
                    let pld = chosen
                        .iter()
                        .find(|row| row.key.host == *host)
                        .map_or_else(PldId::default, |row| row.key.pld);
                    bump.execute(params![
                        &host.as_bytes()[..],
                        &pld.as_bytes()[..],
                        row::to_ms(*next_allowed_ms),
                        i64::from(HostRow::INITIAL_DELAY_MS),
                        i64::from(Tier::default().as_u8()),
                    ])
                    .state()?;
                }
            }

            // The lease counter is part of the same transaction as the leases
            // it names, so a crash cannot reissue an id that is already out.
            tx.execute(
                sql::PUT_META,
                params!["next_lease", inner.next_lease.to_string()],
            )
            .state()?;
            tx.commit().state()?;
            Ok(leases)
        })
    }

    async fn complete(&self, outcomes: &[FetchOutcome]) -> Result<()> {
        if outcomes.is_empty() {
            return Ok(());
        }

        blocking(|| {
            let mut guard = self.lock();
            let inner = &mut *guard;
            set_sync(&inner.conn, Sync::Durable)?;
            let tx = inner.conn.transaction().state()?;

            // Doc 07.6's rate limiter, held per host across the whole batch
            // rather than read and written per url. A tick's completions run
            // heavily to a handful of hosts, so this turns two statements per
            // url into two per host, and it is also the only way the streak
            // and the failure counters come out right: eight completions for
            // one host are eight observations of that host in order, not eight
            // independent reads of the same starting row.
            // The flag is whether anything in the batch actually moved this
            // host. A tick made entirely of robots exclusions is a real tick
            // and it made no requests, so it must not leave a host record
            // behind saying we spoke to somebody we did not.
            let mut paced: HashMap<HostId, (HostRow, bool)> = HashMap::new();

            for outcome in outcomes {
                let before: Option<LedgerRow> = tx
                    .prepare_cached(sql::SELECT_LEDGER)
                    .state()?
                    .query_row(
                        params![
                            &outcome.key.pld.as_bytes()[..],
                            &outcome.key.host.as_bytes()[..],
                            &outcome.key.url.as_bytes()[..],
                        ],
                        row::ledger,
                    )
                    .optional()
                    .state()?;
                let Some(before) = before else {
                    // A completion for a url we have no row for. That is a bug
                    // above us, not corruption down here, and dropping it beats
                    // inventing a row with no depth and no priority.
                    continue;
                };

                // Idempotence without an unbounded set of retired lease ids: an
                // answer only counts if it is newer than the one already
                // recorded. A retried completion carries the same
                // `finished_ms` and changes nothing, while a completion from a
                // lease that already expired still lands, because the page
                // really was fetched.
                if outcome.finished_ms <= before.last_fetch_ms {
                    tx.prepare_cached(sql::CLEAR_LEASE)
                        .state()?
                        .execute(params![row::to_ms(outcome.lease.raw())])
                        .state()?;
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
                        row.observed_secs = before.observed_secs_after(outcome.finished_ms);
                        row.last_fetch_ms = outcome.finished_ms;
                        row.content_hash = *content_hash;
                        row.fail_streak = 0;
                        if changed {
                            row.change_count = before.change_count.saturating_add(1);
                            row.last_change_ms = outcome.finished_ms;
                        }
                        row.etag_ref = match &revalidate.etag {
                            Some(etag) => intern_etag(&tx, etag)?,
                            None => LedgerRow::NO_ETAG,
                        };
                        row.last_mod_ms = revalidate.last_modified_ms.unwrap_or(0);
                        row.next_due_ms = next_due_after(&before, changed, outcome.finished_ms);
                    }
                    FetchResult::NotModified { status, revalidate } => {
                        // The content did not move, so `content_hash` and
                        // `last_change_ms` stay exactly where they were. That
                        // is the whole point of a conditional request: it is a
                        // cheap observation, not a new version.
                        row.state = UrlState::Fetched;
                        row.status = *status;
                        row.fetch_count = before.fetch_count.saturating_add(1);
                        row.observed_secs = before.observed_secs_after(outcome.finished_ms);
                        row.last_fetch_ms = outcome.finished_ms;
                        row.fail_streak = 0;
                        if let Some(etag) = &revalidate.etag {
                            row.etag_ref = intern_etag(&tx, etag)?;
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
                        // The reason is not stored. Rechecking robots is cheap
                        // and a stale reason on a row is worse than none.
                        row.state = UrlState::Excluded;
                        row.last_fetch_ms = outcome.finished_ms;
                        row.next_due_ms = u64::MAX;
                    }
                    _ => {
                        // `FetchResult` is non exhaustive, so a variant added
                        // to umi-state later arrives here on a build of this
                        // crate that predates it. Recording it as a failure is
                        // the only safe reading: the url comes back under the
                        // retry ladder instead of being marked done on the
                        // strength of an answer this build cannot read.
                        row.state = UrlState::Failed;
                        row.last_fetch_ms = outcome.finished_ms;
                        row.fail_streak = before.fail_streak.saturating_add(1);
                        row.next_due_ms = outcome
                            .finished_ms
                            .saturating_add(retry_after_ms(row.fail_streak));
                    }
                }

                tx.prepare_cached(sql::UPDATE_LEDGER)
                    .state()?
                    .execute(params![
                        i64::from(row.priority.raw()),
                        i64::from(row.state as u8),
                        row::to_ms(row.next_due_ms),
                        row::to_ms(row.last_fetch_ms),
                        row::to_ms(row.last_change_ms),
                        i64::from(row.fetch_count),
                        i64::from(row.change_count),
                        &row.content_hash[..],
                        i64::from(row.etag_ref),
                        row::to_ms(row.last_mod_ms),
                        i64::from(row.status),
                        i64::from(row.tier_used.as_u8()),
                        i64::from(row.fail_streak),
                        i64::from(row.observed_secs),
                        // Written from the schedule it belongs to, in the same
                        // statement, so the index cannot disagree with the row.
                        i64::from(RefreshClass::of_row(&row).as_u8()),
                        &outcome.key.pld.as_bytes()[..],
                        &outcome.key.host.as_bytes()[..],
                        &outcome.key.url.as_bytes()[..],
                    ])
                    .state()?;

                // A completion with no latency behind it never reached the
                // wire, so it has nothing to say about the host and there is no
                // point reading one. A tick spent entirely on a disallowed site
                // is all of these, and it should cost no host statements at all.
                if outcome.pace.latency_ms.is_none() {
                    continue;
                }

                // Read through on the first completion for a host and keep it,
                // so the observations below stack. A host with no record yet
                // starts from `HostRow::new`, which is what the lease query's
                // COALESCE defaults already assume.
                let seat = match paced.entry(outcome.key.host) {
                    Entry::Occupied(seat) => seat.into_mut(),
                    Entry::Vacant(seat) => {
                        let stored = tx
                            .prepare_cached(sql::SELECT_PACE)
                            .state()?
                            .query_row(params![&outcome.key.host.as_bytes()[..]], |read| {
                                row::pacing(read, outcome.key.host)
                            })
                            .optional()
                            .state()?;
                        let host = stored
                            .unwrap_or_else(|| HostRow::new(outcome.key.host, outcome.key.pld));
                        seat.insert((host, false))
                    }
                };
                seat.1 |= seat
                    .0
                    .observe(&outcome.result, outcome.pace, outcome.finished_ms);
            }

            {
                let mut pace = tx.prepare_cached(sql::PACE_HOST).state()?;
                for (host, moved) in paced.values() {
                    if !*moved {
                        continue;
                    }
                    pace.execute(params![
                        &host.host.as_bytes()[..],
                        &host.pld.as_bytes()[..],
                        i64::from(host.adaptive_delay_ms),
                        row::to_ms(host.next_allowed_ms),
                        row::to_ms(host.fetches),
                        row::to_ms(host.failures),
                        i64::from(host.consecutive_failures),
                        i64::from(host.fast_streak),
                        i64::from(Tier::default().as_u8()),
                    ])
                    .state()?;
                }
            }

            tx.commit().state()
        })
    }

    async fn release(&self, lease_ids: &[LeaseId], _reason: NackReason) -> Result<()> {
        if lease_ids.is_empty() {
            return Ok(());
        }

        blocking(|| {
            let mut guard = self.lock();
            let inner = &mut *guard;
            set_sync(&inner.conn, Sync::Buffered)?;
            let tx = inner.conn.transaction().state()?;
            {
                let mut clear = tx.prepare_cached(sql::CLEAR_LEASE).state()?;
                for id in lease_ids {
                    // An id the store does not know is not an error. A fetcher
                    // and a coordinator timing out at the same moment produces
                    // exactly that. The due time is left alone, so the url is
                    // leasable again at once, and `fail_streak` is untouched,
                    // because a fetcher going away says nothing about the url.
                    clear.execute(params![row::to_ms(id.raw())]).state()?;
                }
            }
            tx.commit().state()
        })
    }

    async fn host(&self, id: HostId) -> Result<Option<HostRow>> {
        blocking(|| {
            let guard = self.lock();
            guard
                .conn
                .prepare_cached(sql::SELECT_HOST)
                .state()?
                .query_row(params![&id.as_bytes()[..]], row::host_record)
                .optional()
                .state()
        })
    }

    async fn put_host(&self, rows: &[HostRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        blocking(|| {
            let mut guard = self.lock();
            let Inner { conn, blocked, .. } = &mut *guard;
            // Doc 07.7 commits to applying a block within an hour of a valid
            // request, and a block a crash can undo is not a block.
            let sync = if rows.iter().any(|row| row.blocked) {
                Sync::Durable
            } else {
                Sync::Buffered
            };
            set_sync(conn, sync)?;
            let tx = conn.transaction().state()?;
            {
                let mut put = tx.prepare_cached(sql::PUT_HOST).state()?;
                for host in rows {
                    let robots = host.robots;
                    put.execute(params![
                        &host.host.as_bytes()[..],
                        &host.pld.as_bytes()[..],
                        robots.map(|r| r.digest.as_bytes().to_vec()),
                        robots.map(|r| row::to_ms(r.fetched_ms)),
                        robots.map(|r| row::to_ms(r.expires_ms)),
                        robots.map(|r| i64::from(r.authoritative)),
                        i64::from(host.adaptive_delay_ms),
                        host.crawl_delay_ms.map(i64::from),
                        row::to_ms(host.next_allowed_ms),
                        i64::from(host.tier.preferred.as_u8()),
                        i64::from(host.tier.max.as_u8()),
                        i64::from(host.tier.last_success.as_u8()),
                        i64::from(host.tier.consecutive_blocks),
                        row::to_ms(host.tier.last_probe_down_ms),
                        i64::from(host.tier.render_required),
                        i64::from(host.tier.weak_revalidator),
                        i64::from(host.tier.lying_revalidator),
                        host.content_usage.as_deref(),
                        row::join_sitemaps(&host.sitemaps),
                        row::to_ms(host.fetches),
                        row::to_ms(host.failures),
                        i64::from(host.consecutive_failures),
                        i64::from(host.fast_streak),
                        i64::from(host.blocked),
                        i64::from(host.refusing),
                    ])
                    .state()?;
                }
            }
            tx.commit().state()?;

            // Last write wins, so a host that was blocked and is not any more
            // has to leave the set as well as enter it.
            for host in rows {
                if host.blocked {
                    blocked.insert(host.host);
                } else {
                    blocked.remove(&host.host);
                }
            }
            Ok(())
        })
    }

    async fn put_segment(&self, rows: &[SegmentRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        blocking(|| {
            let mut guard = self.lock();
            let conn = &mut guard.conn;
            // Durable, always, and the trait says why: a segment record a
            // crash can lose is a local file that gets deleted and then reads
            // as data loss to the next reconciliation pass. About a thousand
            // of these a day, so the fsync is free in any sense that matters.
            set_sync(conn, Sync::Durable)?;
            let tx = conn.transaction().state()?;
            {
                let mut put = tx.prepare_cached(sql::PUT_SEGMENT).state()?;
                for segment in rows {
                    let remote = segment.remote.as_ref();
                    put.execute(params![
                        &segment.id.as_bytes()[..],
                        i64::from(segment.stream.as_u8()),
                        segment.local_path.as_str(),
                        row::to_ms(segment.sealed_at_ms),
                        row::to_ms(segment.rows),
                        row::to_ms(segment.bytes),
                        &segment.local_digest.as_bytes()[..],
                        remote.map(|r| r.repo.as_str()),
                        remote.map(|r| r.path.as_str()),
                        remote.map(|r| r.digest.as_bytes().to_vec()),
                        segment.manifest_day.map(i64::from),
                        segment.deleted_at_ms.map(row::to_ms),
                    ])
                    .state()?;
                }
            }
            tx.commit().state()
        })
    }

    async fn segment(&self, id: Ulid) -> Result<Option<SegmentRow>> {
        blocking(|| {
            let guard = self.lock();
            guard
                .conn
                .prepare_cached(sql::SELECT_SEGMENT)
                .state()?
                .query_row(params![&id.as_bytes()[..]], row::segment)
                .optional()
                .state()
        })
    }

    async fn segments(&self, query: SegmentQuery) -> Result<Vec<SegmentRow>> {
        blocking(|| {
            let guard = self.lock();
            let text = match query {
                SegmentQuery::Unpublished => sql::SELECT_UNPUBLISHED,
                SegmentQuery::Collectable => sql::SELECT_COLLECTABLE,
                SegmentQuery::SealedBetween { .. } => sql::SELECT_SEALED_BETWEEN,
            };
            let mut stmt = guard.conn.prepare_cached(text).state()?;
            let found = match query {
                SegmentQuery::SealedBetween { from_ms, to_ms } => stmt
                    .query_map(
                        params![row::to_ms(from_ms), row::to_ms(to_ms)],
                        row::segment,
                    )
                    .state()?
                    .collect::<rusqlite::Result<Vec<_>>>(),
                _ => stmt
                    .query_map([], row::segment)
                    .state()?
                    .collect::<rusqlite::Result<Vec<_>>>(),
            };
            found.state()
        })
    }

    async fn warm(&self, _plds: &[PldId]) -> Result<()> {
        // One file, one coordinator, no cold tier. Everything is already local,
        // so warming is a no op rather than an error, exactly as the trait says
        // it is for a backend that does not shard. The shard lifecycle in doc
        // 08.6 is `nami`'s to implement.
        Ok(())
    }

    async fn evict(&self, plds: &[PldId]) -> Result<EvictReport> {
        // There is no cold tier here, so the local copy is the only copy and
        // dropping it would be losing the crawl rather than freeing a cache.
        // A domain the ledger holds is kept, which is what `in_use` means, and
        // one it has never seen is `not_resident`. Neither is `evicted`, and a
        // scheduler under disk pressure needs to see that so it knows asking
        // this backend to free space did not free any.
        blocking(|| {
            let guard = self.lock();
            let mut stmt = guard.conn.prepare_cached(sql::LEDGER_HAS_PLD).state()?;
            let mut report = EvictReport::default();
            for pld in plds {
                if stmt.exists(params![&pld.as_bytes()[..]]).state()? {
                    report.in_use += 1;
                } else {
                    report.not_resident += 1;
                }
            }
            Ok(report)
        })
    }

    async fn resident(&self) -> Result<Vec<PldId>> {
        // Every domain in the ledger, because on a backend with no cold tier
        // every domain it knows about is local by definition. It used to answer
        // nothing on the grounds that a store which does not shard has no
        // residency to report, and that reading made the scheduler in
        // umi-frontier unusable across a restart: the domain rate limits are
        // rebuilt from this, so a store that reports nothing comes back up with
        // nothing scheduled and a resumed crawl sits there leasing zero urls.
        //
        // In key order already, since that is the order the scan walks, so
        // nothing here sorts.
        blocking(|| {
            let guard = self.lock();
            let mut stmt = guard.conn.prepare_cached(sql::SELECT_RESIDENT).state()?;
            let found = stmt
                .query_map([], |row| row::pld(row, "pld"))
                .state()?
                .collect::<rusqlite::Result<Vec<_>>>();
            found.state()
        })
    }

    async fn checkpoint(&self, now_ms: u64) -> Result<Checkpoint> {
        blocking(|| {
            let mut guard = self.lock();
            set_sync(&guard.conn, Sync::Durable)?;
            guard.checkpoint_seq += 1;
            let sequence = guard.checkpoint_seq;
            guard.put_meta("checkpoint_seq", sequence)?;

            // Fold the write ahead log back into the main file, so the database
            // on disk is the whole story and a snapshot taken from it is too.
            guard
                .conn
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .state()?;

            let (path, digest) = match self.config.snapshot_dir() {
                Some(dir) => {
                    std::fs::create_dir_all(&dir)?;
                    let path = dir.join(format!("checkpoint-{sequence:012}.umistate"));
                    if path.exists() {
                        std::fs::remove_file(&path)?;
                    }
                    // `VACUUM INTO` is SQLite's own point in time snapshot. The
                    // file it writes is a complete database that nothing
                    // further mutates, which is what the trait promises and
                    // what DuckDB attaches to.
                    guard
                        .conn
                        .execute("VACUUM INTO ?1", params![path.to_string_lossy()])
                        .state()?;
                    let digest = digest_file(&path)?;
                    (Some(path), Some(digest))
                }
                // Snapshots off, or an in memory store. The sequence number and
                // the durability barrier are still real; there is just no
                // artefact to point at.
                None => (self.config.path.clone(), None),
            };

            let stats = stats(&guard.conn, &self.config)?;
            Ok(Checkpoint {
                sequence,
                taken_ms: now_ms,
                canon_version: CANON_VERSION.to_owned(),
                path,
                digest,
                stats,
            })
        })
    }

    async fn stats(&self) -> Result<StateStats> {
        blocking(|| {
            let guard = self.lock();
            stats(&guard.conn, &self.config)
        })
    }
}

/// The row `admit` writes, built through the same constructor the reference
/// backend uses so the two cannot disagree about what a fresh row looks like.
fn pending_row(candidate: &Candidate<'_>) -> LedgerRow {
    LedgerRow::pending(
        &candidate.key,
        candidate.url,
        candidate.depth,
        candidate.priority,
        candidate.discovered_ms,
    )
}

fn insert_ledger(
    stmt: &mut rusqlite::CachedStatement<'_>,
    key: &RowKey,
    url: &str,
    row: &LedgerRow,
) -> Result<()> {
    stmt.execute(params![
        &key.pld.as_bytes()[..],
        &key.host.as_bytes()[..],
        &key.url.as_bytes()[..],
        url,
        &row.url_key_full.as_bytes()[..],
        i64::from(row.depth),
        i64::from(row.priority.raw()),
        i64::from(row.state as u8),
        row::to_ms(row.next_due_ms),
        row::to_ms(row.last_fetch_ms),
        row::to_ms(row.last_change_ms),
        i64::from(row.fetch_count),
        i64::from(row.change_count),
        &row.content_hash[..],
        i64::from(row.etag_ref),
        row::to_ms(row.last_mod_ms),
        i64::from(row.status),
        i64::from(row.tier_used.as_u8()),
        i64::from(row.fail_streak),
    ])
    .state()?;
    Ok(())
}

fn intern_etag(tx: &rusqlite::Transaction<'_>, etag: &str) -> Result<u32> {
    let id: i64 = tx
        .prepare_cached(sql::INTERN_ETAG)
        .state()?
        .query_row(params![etag], |row| row.get(0))
        .state()?;
    // The pool would have to hold four billion distinct ETags to reach this,
    // which is well past where this backend stops being the right one anyway.
    Ok(u32::try_from(id).unwrap_or(LedgerRow::NO_ETAG - 1))
}

fn stats(conn: &Connection, config: &SqliteConfig) -> Result<StateStats> {
    let counts = counters(conn, sql::SELECT_COUNTS)?;
    Ok(StateStats {
        urls_seen: counts[0],
        urls_pending: counts[1],
        urls_fetched: counts[2],
        urls_failed: counts[3],
        urls_gone: counts[4],
        urls_excluded: counts[5],
        urls_held: counts[6],
        hosts: counts[7],
        leases_in_flight: counts[8],
        // Nothing is resident because nothing is shardable. See `warm`.
        resident_plds: 0,
        shard_misses: 0,
        bytes_on_disk: bytes_on_disk(conn, config)?,
    })
}

/// The nine numbers, in the one order both queries produce them in.
fn counters(conn: &Connection, query: &str) -> Result<[u64; 9]> {
    conn.query_row(query, [], |row| {
        let mut counts = [0u64; 9];
        for (index, count) in counts.iter_mut().enumerate() {
            *count = u64::try_from(row.get::<_, i64>(index)?).unwrap_or(0);
        }
        Ok(counts)
    })
    .state()
}

fn bytes_on_disk(conn: &Connection, config: &SqliteConfig) -> Result<u64> {
    let Some(path) = &config.path else {
        return Ok(0);
    };
    let pages: i64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .state()?;
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .state()?;
    let main = u64::try_from(pages * page_size).unwrap_or(0);
    // The write ahead log is part of what the store occupies, and leaving it
    // out would understate the number an operator sizes a disk with by however
    // much has not been checkpointed yet.
    let wal = {
        let mut wal_path = path.clone();
        let name = wal_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        match name {
            Some(name) => {
                wal_path.set_file_name(format!("{name}-wal"));
                std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0)
            }
            None => 0,
        }
    };
    Ok(main + wal)
}

fn digest_file(path: &Path) -> Result<Digest> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(Digest::from_bytes(*hasher.finalize().as_bytes()))
}

/// The full 128 bit fingerprint of a url, exposed because a caller reading rows
/// out of the file with plain SQL needs to be able to recompute it.
#[must_use]
pub fn url_key_full(url: &str) -> UrlKeyFull {
    UrlKeyFull::derive(url.as_bytes())
}
