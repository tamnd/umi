//! Every statement this backend runs, in one place.
//!
//! They are here rather than inline at the call sites for two reasons. A
//! statement that is a constant can be prepared once and cached by rusqlite
//! across calls, which matters when `admit` runs the same three statements
//! twelve thousand times a second. And a person auditing what this crate does
//! to a file can read this module instead of reading the whole backend.
//!
//! Column lists are spelled out everywhere and `SELECT *` appears nowhere. A
//! star would keep compiling after a migration added a column and would quietly
//! shift every read in [`row`](crate::row) onto the wrong column.
//!
//! The lease query is assembled from four macros instead of being written out
//! twice. It has to end up as a single string literal, because the partial
//! index only applies when SQLite can match the query's `WHERE` against the
//! index's, and the domain restricted variant differs from the plain one by a
//! single clause in the middle. Macros are what [`concat!`] will take.

use crate::schema::schedulable;

/// Remember one small thing across restarts, such as the lease counter.
pub const PUT_META: &str = "
INSERT INTO meta (key, value) VALUES (?1, ?2)
ON CONFLICT(key) DO UPDATE SET value = excluded.value";

/// Claim a url for the seen set.
///
/// `OR IGNORE` rather than a select and then an insert, so the check and the
/// claim are one statement and one b-tree descent. The number of rows changed
/// is the answer: one means this caller is the first to see the url, zero means
/// somebody already had it. This is the hottest statement in the system and
/// doc 08.1 gives it a budget of 12500 a second.
pub const INSERT_SEEN: &str = "INSERT OR IGNORE INTO seen (url_key) VALUES (?1)";

/// Put an admitted url in the frontier.
///
/// Also `OR IGNORE`. The seen set has already decided this url is new, so a
/// conflict here means the ledger and the seen set disagree, which can only
/// happen if a crash lost the tail of the write ahead log between the two
/// tables. Keeping the older row is right in that case: it is the one with the
/// fetch history on it.
///
/// `observed_secs` and `refresh_class` are missing from the list on purpose.
/// Their column defaults are the right answer for a URL nobody has fetched, a
/// window of nothing and doc 09.5's discovery class, and naming them here would
/// be two more values to bind on the hottest write in the system.
pub const INSERT_LEDGER: &str = "
INSERT OR IGNORE INTO ledger (
    pld, host, url_key, url, url_key_full, depth, priority, state,
    next_due_ms, last_fetch_ms, last_change_ms, fetch_count, change_count,
    content_hash, etag_ref, last_mod_ms, status, tier_used, fail_streak
) VALUES (
    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
    ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19
)";

/// Hold a url found by a fetcher we do not trust yet, per doc 06.2 layer 7.
pub const INSERT_PEN: &str = "
INSERT OR IGNORE INTO pen (
    fetcher, url_key, pld, host, url, depth, priority, discovered_ms
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";

/// The columns the lease scan reads, and the joins behind them.
///
/// A host with no record behind it has to behave exactly like `HostRow::new`,
/// because a host nobody has fetched yet is the normal case at the start of a
/// crawl and it would be absurd for it to be unleasable. That is what the
/// `COALESCE` defaults are for, and `the_sql_host_defaults_are_the_rust_ones`
/// in the tests asserts the literals here are still the constants in umi-state.
macro_rules! lease_columns {
    () => {
        "
SELECT ledger.pld                              AS pld,
       ledger.host                             AS host,
       ledger.url_key                          AS url_key,
       ledger.url                              AS url,
       ledger.depth                            AS depth,
       ledger.priority                         AS priority,
       ledger.fetch_count                      AS fetch_count,
       ledger.next_due_ms                      AS next_due_ms,
       ledger.last_mod_ms                      AS last_mod_ms,
       ledger.content_hash                     AS content_hash,
       etags.etag                              AS etag,
       COALESCE(hosts.adaptive_delay_ms, 1000) AS adaptive_delay_ms,
       hosts.crawl_delay_ms                    AS crawl_delay_ms,
       COALESCE(hosts.next_allowed_ms, 0)      AS next_allowed_ms,
       COALESCE(hosts.tier_preferred, 1)       AS tier_preferred,
       COALESCE(hosts.tier_max, 2)             AS tier_max,
       COALESCE(hosts.tier_last_success, 1)    AS tier_last_success,
       COALESCE(hosts.tier_probe_down_ms, 0)   AS tier_probe_down_ms,
       COALESCE(hosts.weak_revalidator, 0)     AS weak_revalidator,
       COALESCE(hosts.lying_revalidator, 0)    AS lying_revalidator
  FROM ledger
  LEFT JOIN hosts ON hosts.host = ledger.host
  LEFT JOIN etags ON etags.id = ledger.etag_ref
 WHERE "
    };
}

/// What a row has to satisfy to be worth handing out, minus the domain
/// restriction.
///
/// The lease clause is not `lease_id IS NULL`. An expired lease is work the
/// fetcher holding it failed to return, and doc 08.4 says that goes back in the
/// frontier without anybody having to call `release` for it, so the expiry is
/// part of the test.
///
/// The tier clause is `MIN(preferred, max)` and not `preferred` alone, because
/// a host whose preferred tier sits above its own ceiling is served at the
/// ceiling. It is `TierPolicy::reachable_by` written in SQL, which is the point.
///
/// The `CASE` in front of it is doc 05.8's de-escalation probe. A host that has
/// escalated away from a tier it used to answer on is offered that tier again
/// once a week, and it has to happen here rather than in a sweep: a host whose
/// preferred tier is above what any fetcher in the fleet can run drops out of
/// this query entirely, and a host that is not in the query is a host nothing
/// will ever probe. Written as a condition, the probe brings it back by itself.
///
/// The class clause is first because it is the leading column of `ledger_ready`
/// and doc 09.5's budget is enforced by asking for one class at a time. See
/// [`crate::schema`] version 6 for why the class is a column at all.
macro_rules! lease_conditions {
    () => {
        "
   AND ledger.refresh_class = ?3
   AND ledger.next_due_ms <= ?1
   AND (ledger.lease_id IS NULL OR ledger.lease_expires <= ?1)
   AND COALESCE(hosts.blocked, 0) = 0
   AND COALESCE(hosts.refusing, 0) = 0
   AND COALESCE(hosts.next_allowed_ms, 0) <= ?1
   AND MIN(CASE WHEN COALESCE(hosts.tier_preferred, 1)
                     > COALESCE(hosts.tier_last_success, 1)
                 AND COALESCE(hosts.tier_probe_down_ms, 0) + 604800000 <= ?1
                THEN COALESCE(hosts.tier_last_success, 1)
                ELSE COALESCE(hosts.tier_preferred, 1)
           END, COALESCE(hosts.tier_max, 2)) <= ?2"
    };
}

/// The order doc 08.4 promises, spelled to match the `ledger_ready` index
/// column for column so SQLite walks it instead of sorting.
///
/// If this and the index ever drift apart the query still returns the right
/// rows in the right order, it just sorts the whole frontier on every lease
/// call to get there. That is a performance bug no correctness test would
/// catch, which is why `the_lease_query_walks_the_index` reads the query plan.
macro_rules! lease_order {
    () => {
        "
 ORDER BY ledger.priority DESC, ledger.next_due_ms,
          ledger.pld, ledger.host, ledger.url_key"
    };
}

/// Ready work of one refresh class, anywhere in the frontier.
///
/// There is no `LIMIT`. The scheduler runs one of these per class and reads as
/// far down each as it needs, so the row count is decided in Rust with the
/// cursor still open. A `LIMIT` here would mean a second query and an `OFFSET`
/// to resume, and an `OFFSET` in SQLite is not a seek: it does the joins for
/// every row it skips.
pub const SELECT_READY: &str = concat!(
    lease_columns!(),
    schedulable!(),
    lease_conditions!(),
    lease_order!()
);

/// The same, restricted to the domains the caller asked for.
///
/// A temp table and a subquery rather than an `IN (?, ?, ...)` list, because
/// the scheduler in doc 09.4 asks for whatever is resident and that can be
/// thousands of domains, well past `SQLITE_MAX_VARIABLE_NUMBER`. A query that
/// works up to some number of domains and then starts failing is worse than one
/// that never had the limit.
pub const SELECT_READY_IN_PLDS: &str = concat!(
    lease_columns!(),
    schedulable!(),
    lease_conditions!(),
    "
   AND ledger.pld IN (SELECT pld FROM lease_plds)",
    lease_order!()
);

/// Temp, so it goes when the connection does, and emptied on every call because
/// the resident set changes between them.
pub const CREATE_LEASE_PLDS: &str = "
CREATE TEMP TABLE IF NOT EXISTS lease_plds (pld BLOB PRIMARY KEY) WITHOUT ROWID;
DELETE FROM lease_plds;";

/// One domain the caller will accept work from.
pub const INSERT_LEASE_PLD: &str = "INSERT OR IGNORE INTO lease_plds (pld) VALUES (?1)";

/// Mark a row in flight, in the same transaction as the lease it describes.
pub const MARK_LEASED: &str = "
UPDATE ledger SET lease_id = ?1, lease_expires = ?2
 WHERE pld = ?3 AND host = ?4 AND url_key = ?5";

/// Move a host's politeness timer forward, creating the host record if this is
/// the first work we have ever handed out for it.
///
/// Only `next_allowed_ms` is touched on conflict. Every other field on a host
/// record belongs to `put_host` and to the robots and tier logic above this
/// crate, and a lease quietly overwriting an adaptive delay that doc 07.6 spent
/// an hour learning would be a real loss.
pub const BUMP_HOST_CLOCK: &str = "
INSERT INTO hosts (
    host, pld, adaptive_delay_ms, next_allowed_ms,
    tier_preferred, tier_max, tier_last_success, tier_blocks,
    tier_probe_down_ms, render_required, weak_revalidator, lying_revalidator,
    sitemaps, fetches, failures, consecutive_failures, blocked, refusing
) VALUES (?1, ?2, ?4, ?3, ?5, ?6, ?5, 0, 0, 0, 0, 0, '', 0, 0, 0, 0, 0)
ON CONFLICT(host) DO UPDATE SET next_allowed_ms = ?3";

/// The eight columns doc 07.6's rate limiter reads, and no more.
///
/// `complete` calls this once per host in the batch and the wide
/// [`SELECT_HOST`] would make it pay for a robots digest, a tier policy and two
/// string allocations it has no use for. The rate limiter reads seven integers
/// and writes six of them, so this reads seven integers.
pub const SELECT_PACE: &str = "
SELECT pld, adaptive_delay_ms, crawl_delay_ms, next_allowed_ms,
       fetches, failures, consecutive_failures, fast_streak
  FROM hosts WHERE host = ?1";

/// Write back what doc 07.6's rate limiter decided, and nothing else.
///
/// Narrow for the same reason [`BUMP_HOST_CLOCK`] is narrow. This path owns six
/// integers and nothing else on the record, and writing all twenty five columns
/// would mean every completion also rewrote a robots digest and a sitemap list
/// it had read a moment earlier and not changed. Today the single writer rule in
/// doc 08.7 means that would still be correct, just wasteful. It stops being
/// correct the day the writer is no longer alone, and there is no reason to
/// leave that lying around.
///
/// The insert branch is for a host we have somehow never leased, which cannot
/// happen through the crawl loop because a completion follows a lease. It is
/// here so that a store hand fed a completion still ends up with a coherent
/// record rather than none.
pub const PACE_HOST: &str = "
INSERT INTO hosts (
    host, pld, adaptive_delay_ms, next_allowed_ms,
    tier_preferred, tier_max, tier_last_success, tier_blocks,
    tier_probe_down_ms, render_required, weak_revalidator, lying_revalidator,
    sitemaps, fetches, failures, consecutive_failures, fast_streak,
    blocked, refusing
) VALUES (?1, ?2, ?3, ?4, ?9, ?10, ?9, 0, 0, 0, 0, 0, '', ?5, ?6, ?7, ?8, 0, 0)
ON CONFLICT(host) DO UPDATE SET
    adaptive_delay_ms    = ?3,
    next_allowed_ms      = ?4,
    fetches              = ?5,
    failures             = ?6,
    consecutive_failures = ?7,
    fast_streak          = ?8";

/// The row `complete` needs before it can work out what changed.
pub const SELECT_LEDGER: &str = "
SELECT host, url_key_full, depth, priority, state, next_due_ms, last_fetch_ms,
       last_change_ms, fetch_count, change_count, content_hash, etag_ref,
       last_mod_ms, status, tier_used, fail_streak, observed_secs
  FROM ledger
 WHERE pld = ?1 AND host = ?2 AND url_key = ?3";

/// Write back what a fetch turned up, and drop the lease in the same statement.
///
/// Dropping the lease here rather than in a second statement is what makes a
/// completion atomic. A crash between the two would leave a row that is both
/// recorded and in flight, and the next lease call would hand out a url that
/// had only just been fetched.
pub const UPDATE_LEDGER: &str = "
UPDATE ledger SET
    priority       = ?1,
    state          = ?2,
    next_due_ms    = ?3,
    last_fetch_ms  = ?4,
    last_change_ms = ?5,
    fetch_count    = ?6,
    change_count   = ?7,
    content_hash   = ?8,
    etag_ref       = ?9,
    last_mod_ms    = ?10,
    status         = ?11,
    tier_used      = ?12,
    fail_streak    = ?13,
    observed_secs  = ?14,
    refresh_class  = ?15,
    lease_id       = NULL,
    lease_expires  = NULL
 WHERE pld = ?16 AND host = ?17 AND url_key = ?18";

/// Doc 09.4's publisher signal: a sitemap says this page moved after we last
/// looked at it, so it is due now.
///
/// Everything about this is a `WHERE` clause, because every condition is a case
/// where the answer is to do nothing. `?5` is the publisher's date and it has to
/// be later than our last fetch, or a sitemap that lists the whole site would
/// refetch the whole site on every poll. `?4` is now and it has to be earlier
/// than the due time already on the row, since this can bring a visit forward
/// and never push one back. `lease_id IS NULL` because a url somebody is
/// fetching this second does not need rescheduling. The state test is the
/// scheduler's own, so a gone or excluded row is left alone, and a pending row
/// falls out on the due time anyway because it has never been fetched.
///
/// `refresh_class` moves with `next_due_ms` and has to, because doc 09.5's
/// index is on the class and a row filed under the wrong one is a row the
/// budget for its real class never offers. The boundaries come in as `?6` to
/// `?9` rather than being written into the statement so that
/// [`RefreshClass::of`](umi_state::RefreshClass::of) stays the only place they
/// are decided. The first arm is that function's own first line: a row nobody
/// has fetched is discovery whatever its interval works out to.
pub const REFRESH_LEDGER: &str = concat!(
    "
UPDATE ledger SET
    next_due_ms   = ?4,
    refresh_class = CASE
        WHEN fetch_count = 0 THEN 5
        WHEN ?4 - last_fetch_ms <  ?6 THEN 0
        WHEN ?4 - last_fetch_ms <  ?7 THEN 1
        WHEN ?4 - last_fetch_ms <  ?8 THEN 2
        WHEN ?4 - last_fetch_ms <  ?9 THEN 3
        ELSE 4
    END
 WHERE pld = ?1 AND host = ?2 AND url_key = ?3
   AND ",
    schedulable!(),
    "
   AND lease_id IS NULL
   AND last_fetch_ms < ?5
   AND next_due_ms > ?4"
);

/// Give a url back without recording anything about it.
///
/// `next_due_ms` and `fail_streak` are deliberately untouched. A fetcher going
/// away says nothing at all about the url, so it goes back in the frontier
/// exactly as it was and is leasable again at once.
pub const CLEAR_LEASE: &str = "
UPDATE ledger SET lease_id = NULL, lease_expires = NULL WHERE lease_id = ?1";

/// Everything known about one host.
pub const SELECT_HOST: &str = "
SELECT host, pld, robots_digest, robots_fetched_ms, robots_expires_ms,
       robots_authoritative, adaptive_delay_ms, crawl_delay_ms, next_allowed_ms,
       tier_preferred, tier_max, tier_last_success, tier_blocks,
       tier_probe_down_ms, render_required, weak_revalidator, lying_revalidator,
       content_usage, sitemaps, fetches, failures, consecutive_failures,
       fast_streak, blocked, refusing
  FROM hosts
 WHERE host = ?1";

/// Write a host record whole.
///
/// Last write wins, which is what the trait says. The caller read the record,
/// changed it and is handing back the result, so merging here would be this
/// crate second guessing the robots and tier logic that owns those fields.
pub const PUT_HOST: &str = "
INSERT OR REPLACE INTO hosts (
    host, pld, robots_digest, robots_fetched_ms, robots_expires_ms,
    robots_authoritative, adaptive_delay_ms, crawl_delay_ms, next_allowed_ms,
    tier_preferred, tier_max, tier_last_success, tier_blocks,
    tier_probe_down_ms, render_required, weak_revalidator, lying_revalidator,
    content_usage, sitemaps, fetches, failures, consecutive_failures,
    fast_streak, blocked, refusing
) VALUES (
    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
    ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25
)";

/// Put an ETag in the pool and get back the reference a ledger row stores.
///
/// The `DO UPDATE` looks pointless and is not. `DO NOTHING` makes the statement
/// return no rows when the ETag is already there, so the caller would need a
/// second select for the common case. Assigning the column to itself makes the
/// upsert always produce a row, and `RETURNING` then gives the id whether it
/// was new or not.
pub const INTERN_ETAG: &str = "
INSERT INTO etags (etag) VALUES (?1)
ON CONFLICT(etag) DO UPDATE SET etag = excluded.etag
RETURNING id";

/// Every pay level domain the ledger holds a url for, in key order.
///
/// The obvious spelling is `SELECT DISTINCT pld FROM ledger`, and the obvious
/// spelling reads the whole table. `ledger` is `WITHOUT ROWID` on
/// `(pld, host, url_key)`, so `MIN(pld) WHERE pld > ?` is a seek to the front
/// of the next domain, and repeating it walks the domains while skipping the
/// urls. That is one b-tree descent per domain rather than one page read per
/// url, which is the difference between a cost that grows as a crawl deepens
/// and one that grows only as it widens.
///
/// Measured on server3 against two million rows in page cache. Over a hundred
/// thousand domains, where a domain is twenty urls and there is nothing much
/// to skip, the two are the same: 270 ms against 280 ms. Over a thousand
/// domains of two thousand urls each, which is the shape a coordinator that
/// owns whole sites actually has, `DISTINCT` is 290 ms and this is 10 ms.
///
/// The `IS NOT NULL` guards are the termination condition. The subquery
/// returns null once there is no domain past the last one, and the recursive
/// term stops on it rather than looping forever, so the outer filter is only
/// dropping that final null row.
pub const SELECT_RESIDENT: &str = "
WITH RECURSIVE domains(pld) AS (
    SELECT MIN(pld) FROM ledger
    UNION ALL
    SELECT (SELECT MIN(pld) FROM ledger WHERE pld > domains.pld)
      FROM domains WHERE domains.pld IS NOT NULL
)
SELECT pld FROM domains WHERE pld IS NOT NULL";

/// Whether the ledger holds anything at all for one domain.
pub const LEDGER_HAS_PLD: &str = "SELECT 1 FROM ledger WHERE pld = ?1 LIMIT 1";

/// The maintained counters, for [`stats`](crate::SqliteState::stats).
///
/// One row of one page, which is what makes the crawl loop's idle tick and doc
/// 14.3's progress line free. The triggers in schema version 3 keep it true.
pub const SELECT_COUNTS: &str = "
SELECT seen, pending, fetched, failed, gone, excluded, held, hosts, leases
FROM counts WHERE id = 0";

/// The same numbers the hard way, by scanning, which is what the counters mean.
///
/// It is the migration's backfill and it is what a later `umi check` compares
/// the counters against. Nothing on a hot path calls it, and the column order
/// matches [`SELECT_COUNTS`] so the two results can be compared directly.
pub const RECOUNT: &str = "
SELECT
    (SELECT COUNT(*) FROM seen),
    (SELECT COUNT(*) FROM ledger WHERE state = 0),
    (SELECT COUNT(*) FROM ledger WHERE state = 1),
    (SELECT COUNT(*) FROM ledger WHERE state = 2),
    (SELECT COUNT(*) FROM ledger WHERE state = 3),
    (SELECT COUNT(*) FROM ledger WHERE state = 4),
    (SELECT COUNT(*) FROM pen),
    (SELECT COUNT(*) FROM hosts),
    (SELECT COUNT(*) FROM ledger WHERE lease_id IS NOT NULL)";

/// The column list every segment read shares.
///
/// A macro rather than a `const`, because each of the four queries below needs
/// it as a literal to be assembled by [`concat!`] at compile time. Written
/// once so that a later migration adding a column is one edit and not four,
/// and so that [`row::segment`](crate::row::segment), which reads columns by
/// name, cannot be handed a query that is missing one.
macro_rules! segment_columns {
    () => {
        "SELECT id, stream, local_path, sealed_at_ms, rows, bytes, local_digest,
                remote_repo, remote_path, remote_digest, manifest_day, deleted_at_ms
           FROM segments"
    };
}

/// One segment by id.
pub const SELECT_SEGMENT: &str = concat!(segment_columns!(), " WHERE id = ?1");

/// The publisher's backlog: sealed and not on the hub.
///
/// `segments_unpublished` in [`schema`](crate::schema) is behind this, and the
/// `WHERE` matches it word for word so the partial index is usable. The first
/// version of this had no index on the theory that a coordinator keeping up
/// has three rows outstanding and SQLite could just walk the seal order index
/// and test the null. The benchmark disagreed: that walk was 555 ms against a
/// year of history and got a millisecond slower every day. Three rows out of
/// 365000 is exactly the shape a partial index is for.
pub const SELECT_UNPUBLISHED: &str = concat!(
    segment_columns!(),
    " WHERE remote_repo IS NULL
      ORDER BY sealed_at_ms, id"
);

/// What doc 12.7's rule is evaluated over.
///
/// The `WHERE` matches `segments_collectable` in
/// [`schema`](crate::schema) word for word, which is what makes the partial
/// index usable. The same trap as the frontier's `ledger_ready` applies: if
/// the two drift, nothing breaks and every GC pass quietly becomes a full
/// table scan of a table that only grows.
pub const SELECT_COLLECTABLE: &str = concat!(
    segment_columns!(),
    " WHERE remote_repo IS NOT NULL AND manifest_day IS NOT NULL
        AND deleted_at_ms IS NULL
      ORDER BY sealed_at_ms, id"
);

/// Doc 12.8's reconciliation window, half open.
pub const SELECT_SEALED_BETWEEN: &str = concat!(
    segment_columns!(),
    " WHERE sealed_at_ms >= ?1 AND sealed_at_ms < ?2
      ORDER BY sealed_at_ms, id"
);

/// Write a segment record whole.
///
/// Last write wins, as with [`PUT_HOST`], and for the same reason: one
/// coordinator owns the segment and is handing back the record it just
/// changed.
pub const PUT_SEGMENT: &str = "
INSERT OR REPLACE INTO segments (
    id, stream, local_path, sealed_at_ms, rows, bytes, local_digest,
    remote_repo, remote_path, remote_digest, manifest_day, deleted_at_ms
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";
