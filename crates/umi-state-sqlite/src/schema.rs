//! The tables, the indexes and the migration path.
//!
//! Every column here is a scalar SQLite type and every key is a blob of the
//! bytes the key type already sorts by. Nothing is a serialised Rust struct and
//! nothing is a native endian integer, which is the whole reason a crawl
//! directory can be copied from an x86 machine to an arm one and resumed: the
//! SQLite file format is fixed and big endian on disk, so as long as we never
//! hand it our own bytes to store opaquely, portability is free rather than
//! something to test for on every architecture we have not tried.
//!
//! Migrations are numbered and forward only. `PRAGMA user_version` holds the
//! number, [`MIGRATIONS`] holds one statement batch per step, and opening a
//! store runs whatever steps are missing. A store written by a newer umi is
//! refused rather than opened, because the alternative is a crawl that silently
//! drops the columns it does not understand.

/// The schema this build writes and understands.
pub const SCHEMA_VERSION: u32 = 2;

/// Stamped into the SQLite header so `file` and any SQLite tool can say what
/// this is. "umi" plus the format generation.
pub const APPLICATION_ID: i32 = 0x756d_6901;

/// The states the frontier will ever offer again, as the one string both the
/// partial index and the lease query are built from.
///
/// They have to be the same string. A partial index is only usable when the
/// query's `WHERE` provably implies the index's, and "provably" here means
/// SQLite matching the expressions, so the two drifting apart would silently
/// turn every lease into a full table scan plus a sort. Sharing a constant
/// would not be enough, because both sites need a literal: the index text goes
/// into a `CREATE INDEX` that is written once and the query is assembled at
/// compile time by [`concat!`]. A macro is what both of those accept.
macro_rules! schedulable {
    () => {
        "state IN (0, 1, 2)"
    };
}

pub(crate) use schedulable;

/// The same text, for the test that asserts both sites still contain it.
#[cfg(test)]
pub const SCHEDULABLE: &str = schedulable!();

/// One statement batch per schema version, in order.
pub const MIGRATIONS: [&str; SCHEMA_VERSION as usize] = [V1, V2];

/// Version 1: the four tables from doc 08.3, plus the ETag pool the ledger's
/// `etag_ref` points into.
const V1: &str = concat!(
    r"
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

-- The seen set. `WITHOUT ROWID` because the whole row is the key, so a rowid
-- would mean a second b-tree and a second lookup for the hottest operation in
-- the system.
CREATE TABLE seen (
    url_key BLOB PRIMARY KEY
) WITHOUT ROWID;

-- The ledger. The primary key is (pld, host, url_key) in that order, which is
-- doc 08.2's ordering, so all of a domain's rows are one contiguous range and
-- a per domain scan is sequential.
CREATE TABLE ledger (
    pld            BLOB    NOT NULL,
    host           BLOB    NOT NULL,
    url_key        BLOB    NOT NULL,
    url            TEXT    NOT NULL,
    url_key_full   BLOB    NOT NULL,
    depth          INTEGER NOT NULL,
    priority       INTEGER NOT NULL,
    state          INTEGER NOT NULL,
    next_due_ms    INTEGER NOT NULL,
    last_fetch_ms  INTEGER NOT NULL,
    last_change_ms INTEGER NOT NULL,
    fetch_count    INTEGER NOT NULL,
    change_count   INTEGER NOT NULL,
    content_hash   BLOB    NOT NULL,
    etag_ref       INTEGER NOT NULL,
    last_mod_ms    INTEGER NOT NULL,
    status         INTEGER NOT NULL,
    tier_used      INTEGER NOT NULL,
    fail_streak    INTEGER NOT NULL,
    lease_id       INTEGER,
    lease_expires  INTEGER,
    PRIMARY KEY (pld, host, url_key)
) WITHOUT ROWID;

-- The order `lease` hands work out in, as an index, so that choosing the next
-- thousand urls is a prefix scan rather than a sort of the frontier. Partial,
-- because a gone or excluded row is never coming back and has no business
-- taking up space in the index the scheduler reads on every call.
CREATE INDEX ledger_ready
    ON ledger (priority DESC, next_due_ms, pld, host, url_key)
    WHERE ",
    schedulable!(),
    r";

-- `release` gets a lease id and nothing else, so it needs a way in that is not
-- the primary key.
CREATE INDEX ledger_lease ON ledger (lease_id) WHERE lease_id IS NOT NULL;

-- Host records. Small, and there are only about 50 million fleet wide, so this
-- is the one table doc 08.3 expects to sit in page cache in its entirety.
CREATE TABLE hosts (
    host                 BLOB PRIMARY KEY,
    pld                  BLOB    NOT NULL,
    robots_digest        BLOB,
    robots_fetched_ms    INTEGER,
    robots_expires_ms    INTEGER,
    robots_authoritative INTEGER,
    adaptive_delay_ms    INTEGER NOT NULL,
    crawl_delay_ms       INTEGER,
    next_allowed_ms      INTEGER NOT NULL,
    tier_preferred       INTEGER NOT NULL,
    tier_max             INTEGER NOT NULL,
    tier_last_success    INTEGER NOT NULL,
    tier_blocks          INTEGER NOT NULL,
    tier_probe_down_ms   INTEGER NOT NULL,
    render_required      INTEGER NOT NULL,
    weak_revalidator     INTEGER NOT NULL,
    lying_revalidator    INTEGER NOT NULL,
    content_usage        TEXT,
    sitemaps             TEXT    NOT NULL,
    fetches              INTEGER NOT NULL,
    failures             INTEGER NOT NULL,
    consecutive_failures INTEGER NOT NULL,
    blocked              INTEGER NOT NULL,
    refusing             INTEGER NOT NULL
) WITHOUT ROWID;

-- The holding pen from doc 06.2 layer 7, keyed by the fetcher that found the
-- url so that one bad fetcher's discoveries can be dropped without touching
-- anyone else's.
CREATE TABLE pen (
    fetcher       BLOB    NOT NULL,
    url_key       BLOB    NOT NULL,
    pld           BLOB    NOT NULL,
    host          BLOB    NOT NULL,
    url           TEXT    NOT NULL,
    depth         INTEGER NOT NULL,
    priority      INTEGER NOT NULL,
    discovered_ms INTEGER NOT NULL,
    PRIMARY KEY (fetcher, url_key)
) WITHOUT ROWID;

-- The interned ETag pool. ETags repeat heavily within a site, and storing them
-- inline would roughly double the ledger for no gain.
CREATE TABLE etags (
    id   INTEGER PRIMARY KEY,
    etag TEXT NOT NULL UNIQUE
);
"
);

/// Version 2: doc 08.3's segments table.
///
/// It was not in the first draft of doc 08 and it arrived because doc 12.7's
/// fourth GC condition does not work without it: a local file is only deleted
/// once the state ledger carries the remote repository, path and digest, and
/// there was nowhere for those to live. Adding it as a migration rather than
/// folding it into V1 is deliberate even though nothing has published yet,
/// because the migration path is the thing this design has to get right and a
/// path that has never run once is not one anybody should trust.
const V2: &str = r"
-- Sealed segments. Tiny by construction: a coordinator seals about a thousand
-- a day and a row is under 200 bytes, so a year is well under 100 MB and there
-- is no prune step. The row outliving the file is the point, since it is how
-- an operator answers 'where did that segment go' once the local copy is gone.
--
-- The three remote_ columns are written together or not at all. Doc 08.3 asks
-- for that so a crash can leave a segment unpublished but never half
-- published, and the CHECK is what makes it true of the file rather than true
-- of the code that usually writes it.
CREATE TABLE segments (
    id            BLOB PRIMARY KEY,
    stream        INTEGER NOT NULL,
    local_path    TEXT    NOT NULL,
    sealed_at_ms  INTEGER NOT NULL,
    rows          INTEGER NOT NULL,
    bytes         INTEGER NOT NULL,
    local_digest  BLOB    NOT NULL,
    remote_repo   TEXT,
    remote_path   TEXT,
    remote_digest BLOB,
    manifest_day  INTEGER,
    deleted_at_ms INTEGER,
    CHECK ((remote_repo IS NULL) = (remote_path IS NULL)),
    CHECK ((remote_repo IS NULL) = (remote_digest IS NULL))
) WITHOUT ROWID;

-- The publisher's backlog and the reconciliation window, in seal order. The
-- id is in the index so the ordering is total: two segments sealed in the same
-- millisecond happen on every restart, when every open writer is sealed at
-- once, and a scan that returns them in an arbitrary order would make the
-- publisher non deterministic for no reason.
CREATE INDEX segments_sealed ON segments (sealed_at_ms, id);

-- The publisher's backlog. Partial for the same reason as the one below, and
-- it is here because the benchmark said so: without it, finding the 3 rows a
-- keeping-up coordinator has outstanding took 555 ms against a year of
-- history, because SQLite walked all 365000 entries of segments_sealed and
-- tested the null on each. The publisher polls this, so that was 555 ms per
-- poll growing by a millisecond a day forever.
CREATE INDEX segments_unpublished
    ON segments (sealed_at_ms, id)
    WHERE remote_repo IS NULL;

-- What the GC pass walks. Partial, because the set it wants is small and
-- shrinking while the table only grows, so a full index would be almost
-- entirely rows that can never match again.
CREATE INDEX segments_collectable
    ON segments (sealed_at_ms, id)
    WHERE remote_repo IS NOT NULL AND manifest_day IS NOT NULL
      AND deleted_at_ms IS NULL;
";
