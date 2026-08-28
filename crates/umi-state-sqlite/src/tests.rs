//! What this backend has to get right, beyond conforming.
//!
//! The conformance suite in umi-state says what the trait promises and is the
//! bulk of the coverage here. It cannot see inside a store, though, and half of
//! what makes this backend the right default lives inside one: that the file is
//! a portable SQLite database and not a serialisation format wearing one, that
//! the schema is versioned and refuses a future it cannot read, and that the
//! lease query walks an index rather than sorting the frontier. Those are the
//! cases in this module.

use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::Connection;
use tempfile::TempDir;
use umi_state::{
    BlockRow, Candidate, DAILY_UNDER_MS, Discovery, FetchOutcome, FetchResult, HOURLY_UNDER_MS,
    HostRow, LeaseRequest, Pace, REALTIME_UNDER_MS, RefreshClass, Revalidator, SegmentRow, State,
    Stream, TierPolicy, WEEKLY_UNDER_MS, conformance,
};
use umi_types::{Digest, FetcherId, RowKey, Tier, Ulid};

use super::{APPLICATION_ID, SCHEMA_VERSION, SqliteConfig, SqliteState, schema, sql};

/// The same fixed instant the conformance suite runs from.
const T0: u64 = 1_700_000_000_000;

/// A distinctive six byte value, for the test that looks at raw file bytes.
/// Six bytes because that is a serial type SQLite stores as a plain big endian
/// integer, and distinctive because the test asserts the reverse of it does not
/// appear anywhere in the file.
const MARKER_MS: u64 = 0x0001_0203_0405_0607 >> 8;

fn fetcher() -> FetcherId {
    FetcherId::LOCAL
}

/// A store on disk, with the directory that owns it.
fn on_disk() -> (TempDir, SqliteState) {
    let dir = TempDir::new().expect("a temp directory");
    let state = SqliteState::open(dir.path().join("state.umistate")).expect("a new store");
    (dir, state)
}

async fn admit_one(state: &SqliteState, url: &str) {
    let candidate = Candidate::new(url, T0).expect("a crawlable url");
    state.admit(&[candidate]).await.expect("admit");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sqlite_backend_conforms() {
    // On disk rather than in memory, because the point of running the suite
    // against this backend is to exercise the file, and because a `lease` that
    // is durable before it returns is not something an in memory store can be
    // wrong about.
    let dir = TempDir::new().expect("a temp directory");
    let n = AtomicU64::new(0);

    conformance::check(|| async {
        let id = n.fetch_add(1, Ordering::Relaxed);
        SqliteState::open(dir.path().join(format!("case-{id}.umistate"))).expect("a fresh store")
    })
    .await
    .assert_ok();
}

#[tokio::test]
async fn a_store_reopens_with_everything_still_in_it() {
    let dir = TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");

    {
        let state = SqliteState::open(&path).expect("a new store");
        admit_one(&state, "https://example.com/one").await;
        admit_one(&state, "https://example.com/two").await;
        let leases = state
            .lease(&LeaseRequest::new(fetcher(), T0, 1))
            .await
            .expect("lease");
        assert_eq!(leases.len(), 1, "one url per host per politeness window");
    }

    let state = SqliteState::open(&path).expect("the same store again");
    let stats = state.stats().await.expect("stats");
    assert_eq!(stats.urls_seen, 2);
    assert_eq!(
        stats.leases_in_flight, 1,
        "a lease is durable before it is handed out, so closing the store does not drop it"
    );

    // The lease counter is part of the file and not part of the process. If it
    // restarted at zero, a completion from before the restart would be
    // attributed to whatever url happened to get id 1 afterwards.
    let more = state
        .lease(&LeaseRequest::new(fetcher(), T0 + 60_000, 4))
        .await
        .expect("lease");
    assert!(
        more.iter().all(|lease| lease.id.raw() > 1),
        "the lease counter restarted after reopening"
    );
}

#[tokio::test]
async fn the_file_says_what_it_is() {
    let (dir, state) = on_disk();
    admit_one(&state, "https://example.com/").await;
    state.checkpoint(T0).await.expect("checkpoint");

    let bytes = std::fs::read(dir.path().join("state.umistate")).expect("the file");
    assert!(
        bytes.starts_with(b"SQLite format 3\0"),
        "this is supposed to be a database any sqlite tool can open"
    );
    // Offset 68 of the header is the application id, big endian, which is what
    // `file` and every SQLite tool reads to tell one application's databases
    // from another's. Reading it here also happens to be a direct check that
    // the header is architecture independent.
    assert_eq!(
        &bytes[68..72],
        &APPLICATION_ID.to_be_bytes(),
        "the file does not identify itself as umi state"
    );
}

#[tokio::test]
async fn the_schema_version_is_stamped_and_a_newer_one_is_refused() {
    let dir = TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");

    {
        let state = SqliteState::open(&path).expect("a new store");
        let version: u32 = state
            .lock()
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .map(|v| u32::try_from(v).unwrap_or(0))
            .expect("user_version");
        assert_eq!(version, SCHEMA_VERSION, "an empty file was not migrated");
    }

    // A file from a umi that knows more than this one. Opening it read only and
    // ignoring the columns we do not understand would let the crawl run and
    // quietly lose whatever the newer version was keeping, so it is refused.
    {
        let conn = Connection::open(&path).expect("the file");
        conn.execute_batch(&format!("PRAGMA user_version = {}", SCHEMA_VERSION + 1))
            .expect("stamp a newer version");
    }
    let err = SqliteState::open(&path).expect_err("a newer schema must be refused");
    let message = err.to_string();
    assert!(
        message.contains("newer umi"),
        "the error does not tell the operator what happened: {message}"
    );
}

#[tokio::test]
async fn a_store_from_an_older_umi_gains_the_new_tables_and_keeps_its_rows() {
    // The migration path is the thing this design has to get right, and a path
    // that has never run is not one anybody should trust. So this builds a
    // version 1 file the way version 1 built it, puts a url in it, and opens it
    // with this build.
    let dir = TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");

    {
        let conn = Connection::open(&path).expect("the file");
        conn.execute_batch(schema::MIGRATIONS[0])
            .expect("the version 1 schema");
        // A url in the seen set, written the way version 1 wrote it. Opening
        // the store to admit it would migrate the file first and there would
        // be nothing left to test.
        let key = RowKey::for_url("https://example.com/before-the-migration", None)
            .expect("a crawlable url");
        conn.execute(
            "INSERT INTO seen (url_key) VALUES (?1)",
            [&key.url.as_bytes()[..]],
        )
        .expect("a version 1 row");
        conn.execute_batch("PRAGMA user_version = 1")
            .expect("stamp version 1");
    }

    let state = SqliteState::open(&path).expect("a version 1 store opens");
    let stats = state.stats().await.expect("stats");
    assert_eq!(
        stats.urls_seen, 1,
        "the migration lost the url that was there"
    );

    let version: u32 = state
        .lock()
        .conn
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map(|v| u32::try_from(v).unwrap_or(0))
        .expect("user_version");
    assert_eq!(version, SCHEMA_VERSION, "the file was not brought forward");

    // And the table the migration exists for is usable, not just present.
    let row = SegmentRow {
        id: Ulid::new(1_700_000_000_000, [7; 10]),
        stream: Stream::Pages,
        local_path: "./crawl/segments/one.umi".to_owned(),
        sealed_at_ms: 1_700_000_000_000,
        rows: 118_671,
        bytes: 128 << 20,
        local_digest: Digest::from_bytes([3; 32]),
        remote: None,
        manifest_day: None,
        deleted_at_ms: None,
    };
    state
        .put_segment(std::slice::from_ref(&row))
        .await
        .expect("put_segment");
    let read = state.segment(row.id).await.expect("segment");
    assert_eq!(read.as_ref(), Some(&row), "the segment did not round trip");
}

#[tokio::test]
async fn the_three_remote_columns_are_written_together_or_not_at_all() {
    // Doc 08.3 asks for this so a crash can leave a segment unpublished but
    // never half published in a way that satisfies doc 12.7's fourth
    // condition. `RemoteCopy` being one `Option` is what makes it true of this
    // crate; the CHECK is what makes it true of the file, including for the
    // `sqlite3` shell and for anything else that opens it.
    let dir = TempDir::new().expect("a temp directory");
    let state = SqliteState::open(dir.path().join("state.umistate")).expect("a store");
    let err = state
        .lock()
        .conn
        .execute_batch(
            "INSERT INTO segments (
                 id, stream, local_path, sealed_at_ms, rows, bytes, local_digest,
                 remote_repo, remote_path, remote_digest, manifest_day, deleted_at_ms
             ) VALUES (x'00', 1, 'p', 0, 0, 0, x'00', 'open-index/x', NULL, NULL, NULL, NULL)",
        )
        .expect_err("a half published row must be refused");
    assert!(
        err.to_string().to_lowercase().contains("constraint"),
        "the file let a half published segment in: {err}"
    );
}

#[test]
fn the_migration_list_covers_every_version() {
    // Forward only and numbered, so the number is also the count. A version
    // bump without a migration beside it would leave a store one step behind
    // and nothing would say so until a query hit a column that was never added.
    assert_eq!(schema::MIGRATIONS.len(), SCHEMA_VERSION as usize);
}

#[tokio::test]
async fn opening_a_store_twice_does_not_migrate_it_twice() {
    let dir = TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");
    let first = SqliteState::open(&path).expect("a new store");
    admit_one(&first, "https://example.com/").await;
    drop(first);

    let second = SqliteState::open(&path).expect("the same store again");
    let stats = second.stats().await.expect("stats");
    assert_eq!(
        stats.urls_seen, 1,
        "the second open re-ran a migration and dropped what was there"
    );
}

#[tokio::test]
async fn keys_are_stored_as_their_own_bytes() {
    // The seen set is the case that matters most, because it is the biggest
    // table and the one a person is most likely to want to query with plain
    // SQL. A key stored as anything other than the bytes the key type sorts by
    // would make that query need this crate, which defeats the point.
    let (_dir, state) = on_disk();
    let url = "https://example.com/one";
    admit_one(&state, url).await;

    let key = RowKey::for_url(url, None).expect("a crawlable url");
    let stored: Vec<u8> = state
        .lock()
        .conn
        .query_row("SELECT url_key FROM seen", [], |row| row.get(0))
        .expect("the one row");
    assert_eq!(stored, key.url.as_bytes(), "the seen set is not the key");
}

#[tokio::test]
async fn the_file_holds_no_native_endian_integers() {
    // This is the architecture portability claim, made in the only way a test
    // on one machine can make it. SQLite writes integers into a record big
    // endian whatever the host is, so a value stored here appears in the file
    // most significant byte first, and the little endian spelling of it does
    // not appear at all. If anything in this crate ever writes a Rust integer
    // to a blob, this is what notices.
    let (dir, state) = on_disk();
    let url = "https://example.com/one";
    admit_one(&state, url).await;

    let lease = state
        .lease(&LeaseRequest::new(fetcher(), T0, 1))
        .await
        .expect("lease")
        .pop()
        .expect("one lease");
    state
        .complete(&[FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: MARKER_MS,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Fetched {
                status: 200,
                content_hash: [7u8; 8],
                revalidate: Revalidator::default(),
            },
        }])
        .await
        .expect("complete");
    // Fold the write ahead log in, so the value is in the file being read.
    state.checkpoint(T0).await.expect("checkpoint");

    let bytes = std::fs::read(dir.path().join("state.umistate")).expect("the file");
    let big: Vec<u8> = MARKER_MS.to_be_bytes()[2..].to_vec();
    let little: Vec<u8> = big.iter().rev().copied().collect();
    assert!(
        bytes.windows(big.len()).any(|w| w == big),
        "the timestamp is not in the file big endian, so this test is not testing what it thinks"
    );
    assert!(
        !bytes.windows(little.len()).any(|w| w == little),
        "something wrote a native endian integer, which is a file that will not move to arm"
    );
}

#[tokio::test]
async fn the_lease_query_walks_the_index() {
    // A correctness test cannot catch this. Without the index the query returns
    // exactly the same rows in exactly the same order, it just sorts the whole
    // frontier to get there, which is fine at ten thousand urls and is the
    // whole crawl at a hundred million.
    let (_dir, state) = on_disk();
    let guard = state.lock();
    let mut stmt = guard
        .conn
        .prepare(&format!("EXPLAIN QUERY PLAN {}", sql::SELECT_READY))
        .expect("the lease query parses");
    let plan: Vec<String> = stmt
        .query_map([0i64, 0i64, 0i64], |row| row.get::<_, String>("detail"))
        .expect("a plan")
        .collect::<rusqlite::Result<_>>()
        .expect("a plan");
    let plan = plan.join("\n");

    assert!(
        plan.contains("ledger_ready"),
        "the lease query is not using the partial index:\n{plan}"
    );
    assert!(
        !plan.contains("TEMP B-TREE"),
        "the lease query is sorting the frontier instead of walking it:\n{plan}"
    );
}

#[tokio::test]
async fn listing_the_domains_seeks_between_them_instead_of_reading_the_urls() {
    // Same trap as the lease query and the same reason a correctness test misses
    // it. `SELECT DISTINCT pld FROM ledger` returns the identical answer and
    // reads every url to get there, which costs whatever the crawl has grown to
    // rather than whatever it is spread over. The plan is the only place the
    // difference shows up before it is a problem in production.
    let (_dir, state) = on_disk();
    let guard = state.lock();
    let mut stmt = guard
        .conn
        .prepare(&format!("EXPLAIN QUERY PLAN {}", sql::SELECT_RESIDENT))
        .expect("the resident query parses");
    let plan: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>("detail"))
        .expect("a plan")
        .collect::<rusqlite::Result<_>>()
        .expect("a plan");
    let plan = plan.join("\n");

    assert!(
        plan.contains("SEARCH ledger USING PRIMARY KEY (pld>?)"),
        "listing the domains is not seeking past each one:\n{plan}"
    );
    assert!(
        !plan.contains("SCAN ledger"),
        "listing the domains is reading the whole ledger:\n{plan}"
    );
}

#[test]
fn the_index_and_the_query_agree_on_what_is_schedulable() {
    // Both are built from the same macro, so this cannot fail as written. It is
    // here because the day somebody decides one of the two needs a fourth state
    // and edits it in place, this says out loud that the partial index stops
    // applying.
    assert!(schema::MIGRATIONS[0].contains(schema::SCHEDULABLE));
    assert!(schema::MIGRATIONS[5].contains(schema::SCHEDULABLE));
    assert!(sql::SELECT_READY.contains(schema::SCHEDULABLE));
    assert!(sql::SELECT_READY_IN_PLDS.contains(schema::SCHEDULABLE));
}

#[test]
fn the_sql_class_boundaries_are_the_rust_ones() {
    // Version 6 backfills doc 09.5's refresh class for rows that predate the
    // column, and it has to spell the boundaries out because a migration is a
    // string. `RefreshClass::of` is the definition; this is the only other copy
    // of those numbers and it is the one nothing would notice was wrong.
    let sql = schema::MIGRATIONS[5];
    for boundary in [
        REALTIME_UNDER_MS,
        HOURLY_UNDER_MS,
        DAILY_UNDER_MS,
        WEEKLY_UNDER_MS,
    ] {
        assert!(
            sql.contains(&format!("< {boundary} THEN")),
            "the version 6 backfill does not use {boundary}, which is a boundary in umi-state"
        );
    }
    assert!(
        sql.contains(&format!(
            "fetch_count = 0 THEN {}",
            RefreshClass::Discovery.as_u8()
        )),
        "the version 6 backfill disagrees with Rust about what a never fetched row is"
    );
}

#[tokio::test]
async fn the_stored_class_is_rewritten_on_every_completion() {
    // Doc 09.5 says whatever writes the schedule writes the class in the same
    // statement, and this backend keeps the class in a column so the lease scan
    // can walk one index per class rather than computing a class per row. A
    // column is a copy, and a copy that is written once and then left is the
    // failure worth a test: the row would be scheduled forever out of the class
    // it was in on its first fetch, and the evidence that moved it would be
    // sitting in the same row, unread.
    let (_dir, state) = on_disk();
    let url = "https://example.com/news";
    admit_one(&state, url).await;
    assert_eq!(stored_class(&state), RefreshClass::Discovery);

    // A page that turns over on every visit. Eight rounds crosses two
    // boundaries, which is what makes this a test of the write rather than of
    // the first value happening to be right.
    let mut seen = Vec::new();
    let mut now = T0;
    for round in 0..8 {
        let ask = LeaseRequest::new(FetcherId::LOCAL, now, 1);
        let leases = state.lease(&ask).await.expect("lease");
        assert_eq!(leases.len(), 1, "nothing came due in round {round}");
        state
            .complete(&[FetchOutcome {
                lease: leases[0].id,
                key: leases[0].key,
                finished_ms: now + 500,
                tier_used: Tier::Plain,
                pace: Pace::default(),
                result: FetchResult::Fetched {
                    status: 200,
                    // Different on every round, which is the whole input: the
                    // page changed, so the estimator has to shorten.
                    content_hash: [round, 0, 0, 0, 0, 0, 0, 0],
                    revalidate: Revalidator::default(),
                },
            }])
            .await
            .expect("complete");
        let class = stored_class(&state);
        if seen.last() != Some(&class) {
            seen.push(class);
        }
        now = due_at(&state);
    }
    assert_eq!(
        seen,
        vec![RefreshClass::Daily, RefreshClass::Hourly],
        "the stored class did not follow the schedule the same completions wrote"
    );
}

/// The class column on the one row the test above admits.
fn stored_class(state: &SqliteState) -> RefreshClass {
    let byte: i64 = state
        .lock()
        .conn
        .query_row("SELECT refresh_class FROM ledger", [], |row| row.get(0))
        .expect("one ledger row");
    RefreshClass::from_u8(u8::try_from(byte).expect("a class byte")).expect("a known class")
}

/// The due time column on the same row, so the test can be at the instant the
/// estimator picked rather than guessing at one.
fn due_at(state: &SqliteState) -> u64 {
    let ms: i64 = state
        .lock()
        .conn
        .query_row("SELECT next_due_ms FROM ledger", [], |row| row.get(0))
        .expect("one ledger row");
    u64::try_from(ms).expect("a due time")
}

#[test]
fn the_sql_host_defaults_are_the_rust_ones() {
    // A host nobody has fetched yet has no record, so the lease query has to
    // invent one, and the values it invents are `COALESCE` literals rather than
    // anything the compiler checks. This is the check.
    let sql = sql::SELECT_READY;
    assert!(
        sql.contains(&format!(
            "COALESCE(hosts.adaptive_delay_ms, {})",
            HostRow::INITIAL_DELAY_MS
        )),
        "the default delay in SQL is not doc 07.6's starting delay"
    );
    let fresh = TierPolicy::new();
    assert!(
        sql.contains(&format!(
            "COALESCE(hosts.tier_preferred, {})",
            fresh.preferred.as_u8()
        )),
        "the default preferred tier in SQL is not the Rust default"
    );
    assert!(
        sql.contains(&format!("COALESCE(hosts.tier_max, {})", fresh.max.as_u8())),
        "the default tier ceiling in SQL is not the Rust default"
    );
    assert!(
        sql.contains(&format!(
            "COALESCE(hosts.tier_last_success, {})",
            fresh.last_success.as_u8()
        )),
        "the default last good tier in SQL is not the Rust default"
    );
    // Doc 05.8's de-escalation probe is a `CASE` in the lease query and a
    // comparison in `TierPolicy::probing`, and the two have to be the same
    // week or an escalated host is probed either constantly or never.
    assert!(
        sql.contains(&format!("+ {} <= ?1", TierPolicy::PROBE_EVERY_MS)),
        "the probe window in SQL is not TierPolicy::PROBE_EVERY_MS"
    );
}

#[tokio::test]
async fn a_default_host_behaves_the_same_whether_its_record_exists_or_not() {
    // The other half of the check above, made through the trait rather than by
    // reading the SQL. Two stores, identical except that one has been told
    // about the host in so many words, have to hand out the same work.
    let (_a, implied) = on_disk();
    let (_b, explicit) = on_disk();

    let url = "https://example.com/one";
    admit_one(&implied, url).await;
    admit_one(&explicit, url).await;

    let key = RowKey::for_url(url, None).expect("a crawlable url");
    explicit
        .put_host(&[HostRow::new(key.host, key.pld)])
        .await
        .expect("put_host");

    let one = implied
        .lease(&LeaseRequest::new(fetcher(), T0, 4))
        .await
        .expect("lease");
    let two = explicit
        .lease(&LeaseRequest::new(fetcher(), T0, 4))
        .await
        .expect("lease");

    assert_eq!(one.len(), two.len());
    assert_eq!(one[0].url, two[0].url);
    assert_eq!(one[0].not_before_ms, two[0].not_before_ms);
    assert_eq!(one[0].tier, two[0].tier);
}

#[tokio::test]
async fn a_checkpoint_is_a_database_of_its_own() {
    let (dir, state) = on_disk();
    admit_one(&state, "https://example.com/one").await;

    let first = state.checkpoint(T0).await.expect("checkpoint");
    let path = first.path.clone().expect("a snapshot file");
    assert!(
        path.exists(),
        "the checkpoint named a file that is not there"
    );
    assert_eq!(first.stats.urls_seen, 1);
    assert!(first.digest.is_some(), "a snapshot with no digest to check");

    // Admitting more must not reach back into a snapshot already taken. That is
    // the whole reason analytics reads a checkpoint instead of the live store.
    admit_one(&state, "https://example.com/two").await;
    let snapshot = Connection::open(&path).expect("the snapshot opens on its own");
    let seen: i64 = snapshot
        .query_row("SELECT COUNT(*) FROM seen", [], |row| row.get(0))
        .expect("the snapshot has the schema");
    assert_eq!(seen, 1, "the snapshot moved after it was taken");

    let second = state.checkpoint(T0 + 1).await.expect("checkpoint");
    assert!(second.sequence > first.sequence);
    assert_eq!(second.stats.urls_seen, 2);
    assert_ne!(
        second.path.as_deref(),
        Some(path.as_path()),
        "the second checkpoint overwrote the first"
    );
    drop(dir);
}

#[tokio::test]
async fn checkpoints_can_be_turned_off_without_turning_off_the_barrier() {
    // An operator short of disk wants the durability barrier and the sequence
    // number without a copy of the database every hour.
    let dir = TempDir::new().expect("a temp directory");
    let state = SqliteState::open_with(SqliteConfig {
        snapshots: false,
        ..SqliteConfig::at(dir.path().join("state.umistate"))
    })
    .expect("a new store");
    admit_one(&state, "https://example.com/").await;

    let checkpoint = state.checkpoint(T0).await.expect("checkpoint");
    assert_eq!(checkpoint.sequence, 1);
    assert!(checkpoint.digest.is_none(), "nothing was snapshotted");
    assert!(
        !dir.path().join("state.umistate.checkpoints").exists(),
        "snapshots are off and a snapshot directory appeared anyway"
    );
}

#[tokio::test]
async fn a_blocked_host_keeps_urls_out_of_the_frontier_across_a_restart() {
    // Doc 07.7 commits to applying a block within an hour of a valid request
    // and to never silently reversing one. A block the process was holding in
    // memory and nothing else would be reversed by a restart.
    let dir = TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");
    let key = RowKey::for_url("https://blocked.example.com/", None).expect("a crawlable url");

    {
        let state = SqliteState::open(&path).expect("a new store");
        state
            .put_host(&[HostRow {
                blocked: true,
                ..HostRow::new(key.host, key.pld)
            }])
            .await
            .expect("put_host");
    }

    let state = SqliteState::open(&path).expect("the same store again");
    let report = state
        .admit(&[Candidate::new("https://blocked.example.com/one", T0).expect("a crawlable url")])
        .await
        .expect("admit");
    assert_eq!(report.excluded, 1, "a block did not survive a restart");
    assert_eq!(report.admitted, 0);

    let leases = state
        .lease(&LeaseRequest::new(fetcher(), T0, 4))
        .await
        .expect("lease");
    assert!(leases.is_empty(), "a blocked host was leased");
}

#[tokio::test]
async fn a_block_survives_a_restart_and_covers_the_whole_domain() {
    // Doc 07.7 again, for the other list. The conformance suite says what a
    // block does to a store that stays open; this says it is still true after
    // the process that applied it has gone, which is the half an operator
    // depends on and the half only a file can be wrong about.
    let dir = TempDir::new().expect("a temp directory");
    let path = dir.path().join("state.umistate");
    let reason = "the site owner asked us to stop on 2026-08-14, ticket 41";

    {
        let state = SqliteState::open(&path).expect("a new store");
        admit_one(&state, "https://news.example.com/one").await;
        let report = state
            .block(&[BlockRow::new("news.example.com", reason, T0)])
            .await
            .expect("block");
        assert_eq!(report.excluded, 1, "the block left the url in the frontier");
    }

    let state = SqliteState::open(&path).expect("the same store again");

    // A different host under the same registrable domain, because a block is
    // about the domain and somebody typing a host name is asking for the site
    // to stop being crawled.
    let report = state
        .admit(&[Candidate::new("https://www.example.com/two", T0).expect("a crawlable url")])
        .await
        .expect("admit");
    assert_eq!(report.excluded, 1, "a block did not survive a restart");
    assert_eq!(report.admitted, 0);

    let leases = state
        .lease(&LeaseRequest::new(fetcher(), T0 + 86_400_000, 4))
        .await
        .expect("lease");
    assert!(leases.is_empty(), "a blocked domain was leased");

    let list = state.blocks().await.expect("blocks");
    assert_eq!(list.len(), 1, "the block list did not come back");
    assert_eq!(
        list[0].domain, "example.com",
        "the block was not widened to the registrable domain"
    );
    assert_eq!(list[0].reason, reason, "the reason did not survive");
    assert!(list[0].in_force(), "the block came back already lifted");
}

#[tokio::test]
async fn a_row_that_arrived_around_the_block_is_still_not_leased() {
    // The block sweep takes the domain's urls out of the frontier, so this can
    // only happen to a row that got there some other way: an import, a hand
    // edit, a bug above us. Enforcement at lease issue is what makes the answer
    // the same either way, and this is the case that would notice if the sweep
    // were the only thing holding a block up.
    let (_dir, state) = on_disk();
    admit_one(&state, "https://example.com/one").await;
    state
        .block(&[BlockRow::new("example.com", "ticket 41", T0)])
        .await
        .expect("block");

    // Back into the frontier behind the store's back, which is exactly what
    // this is testing the store does not trust.
    let moved = state
        .lock()
        .conn
        .execute("UPDATE ledger SET state = 0, next_due_ms = 0", [])
        .expect("the hand edit");
    assert_eq!(moved, 1, "the test did not put anything back");

    let leases = state
        .lease(&LeaseRequest::new(fetcher(), T0, 4))
        .await
        .expect("lease");
    assert!(
        leases.is_empty(),
        "a blocked domain was leased from a row the sweep never saw"
    );
}

#[tokio::test]
async fn a_url_from_an_untrusted_fetcher_is_held_and_not_leased() {
    let (_dir, state) = on_disk();
    let mut candidate = Candidate::new("https://example.com/one", T0).expect("a crawlable url");
    candidate.discovery = Discovery::Unverified(FetcherId::from_bytes([7u8; 32]));

    let report = state.admit(&[candidate]).await.expect("admit");
    assert_eq!(report.held, 1);
    assert_eq!(report.admitted, 0);

    let leases = state
        .lease(&LeaseRequest::new(fetcher(), T0, 4))
        .await
        .expect("lease");
    assert!(leases.is_empty(), "a held url was handed out anyway");

    let stats = state.stats().await.expect("stats");
    assert_eq!(stats.urls_held, 1);
    assert_eq!(
        stats.urls_seen, 1,
        "a held url is still seen, or the same url from a trusted source later is admitted twice"
    );
}

#[tokio::test]
async fn etags_are_interned_once_however_often_they_repeat() {
    // ETags repeat heavily within a site, which is why the ledger stores a
    // reference instead of the text. A pool that grew a row per fetch would
    // turn that saving into a cost.
    let (_dir, state) = on_disk();
    for n in 0..4 {
        admit_one(&state, &format!("https://example.com/{n}")).await;
    }

    let mut now = T0;
    for _ in 0..4 {
        let Some(lease) = state
            .lease(&LeaseRequest::new(fetcher(), now, 1))
            .await
            .expect("lease")
            .pop()
        else {
            break;
        };
        now = lease.not_before_ms + 1000;
        state
            .complete(&[FetchOutcome {
                lease: lease.id,
                key: lease.key,
                finished_ms: now,
                tier_used: Tier::Plain,
                pace: Pace::default(),
                result: FetchResult::Fetched {
                    status: 200,
                    content_hash: [1u8; 8],
                    revalidate: Revalidator {
                        etag: Some("\"same-everywhere\"".to_owned()),
                        last_modified_ms: None,
                    },
                },
            }])
            .await
            .expect("complete");
    }

    let pooled: i64 = state
        .lock()
        .conn
        .query_row("SELECT COUNT(*) FROM etags", [], |row| row.get(0))
        .expect("the pool");
    assert_eq!(pooled, 1, "the same etag was interned {pooled} times");
}

#[tokio::test]
async fn the_counters_survive_everything_that_writes() {
    // `stats` reads a maintained row instead of scanning, which is only worth
    // doing if the row is right. So this drives every write path the backend
    // has, in an order that makes rows move between states rather than only
    // arrive, and then recomputes by scanning and compares.
    let (_dir, state) = on_disk();

    for n in 0..40 {
        admit_one(&state, &format!("https://example.com/{n}")).await;
    }
    // Admitting the same urls again is the already seen path, which must not
    // count anything twice.
    for n in 0..40 {
        admit_one(&state, &format!("https://example.com/{n}")).await;
    }

    // A blocked host, so rows land as excluded rather than pending.
    let mut candidate = Candidate::new("https://blocked.example/one", T0).expect("a crawlable url");
    candidate.discovery = Discovery::Unverified(FetcherId::from_bytes([9u8; 32]));
    state.admit(&[candidate]).await.expect("admit held");

    // A host record, twice, because `put_host` is an upsert and the second
    // call must not add a second host.
    let key = RowKey::for_url("https://example.com/one", None).expect("a crawlable url");
    let host = HostRow::new(key.host, key.pld);
    state
        .put_host(std::slice::from_ref(&host))
        .await
        .expect("put_host");
    state.put_host(&[host]).await.expect("put_host again");

    // Lease some, complete them four different ways, and release the rest so
    // the lease counter goes both up and down.
    let leases = state
        .lease(&LeaseRequest::new(fetcher(), T0, 20))
        .await
        .expect("lease");
    assert!(leases.len() >= 8, "not enough work to move the counters");

    let mut outcomes = Vec::new();
    let mut released = Vec::new();
    for (n, lease) in leases.into_iter().enumerate() {
        let result = match n % 5 {
            0 => FetchResult::Fetched {
                status: 200,
                content_hash: [1u8; 8],
                revalidate: Revalidator::default(),
            },
            1 => FetchResult::Failed {
                status: Some(503),
                kind: umi_state::FailureKind::ServerError,
            },
            2 => FetchResult::Gone { status: 410 },
            3 => FetchResult::Excluded {
                reason: umi_state::ExcludeReason::Robots,
            },
            _ => {
                released.push(lease.id);
                continue;
            }
        };
        outcomes.push(FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: T0 + 1000,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result,
        });
    }
    state.complete(&outcomes).await.expect("complete");
    state
        .release(&released, umi_state::NackReason::Expired)
        .await
        .expect("release");

    // A second lease and completion over rows that are already fetched or
    // failed, which is the update trigger's other case: a state change out of
    // something other than pending.
    let again = state
        .lease(&LeaseRequest::new(fetcher(), T0 + 600_000, 5))
        .await
        .expect("lease again");
    let outcomes: Vec<FetchOutcome> = again
        .into_iter()
        .map(|lease| FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: T0 + 700_000,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Fetched {
                status: 200,
                content_hash: [2u8; 8],
                revalidate: Revalidator::default(),
            },
        })
        .collect();
    state.complete(&outcomes).await.expect("complete again");

    let disagreements = state.recount().expect("recount");
    assert!(
        disagreements.is_empty(),
        "the counters drifted from the rows: {disagreements:?}"
    );

    // And the numbers reaching the caller are the counted ones, not zero,
    // which is what an empty comparison would also allow.
    let stats = state.stats().await.expect("stats");
    assert_eq!(stats.urls_seen, 41);
    assert_eq!(stats.urls_held, 1);
    assert_eq!(stats.hosts, 1);
    assert_eq!(stats.leases_in_flight, 0);
    assert!(stats.urls_fetched > 0 && stats.urls_gone > 0 && stats.urls_excluded > 0);
}

#[tokio::test]
async fn a_hand_edited_file_is_caught_by_the_recount() {
    // The counters cannot drift through this crate's own writes, because the
    // triggers are inside the same transaction as the rows. They can drift if
    // somebody opens the file with the sqlite3 shell, so there has to be a way
    // to find out, and this is the test that the way works.
    let (_dir, state) = on_disk();
    admit_one(&state, "https://example.com/one").await;
    assert!(state.recount().expect("recount").is_empty());

    state
        .lock()
        .conn
        .execute_batch("UPDATE counts SET pending = pending + 7")
        .expect("a hand edit");

    let disagreements = state.recount().expect("recount");
    assert_eq!(disagreements, vec![("pending", 8, 1)]);
}
