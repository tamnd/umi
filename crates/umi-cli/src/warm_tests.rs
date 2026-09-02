//! `umi warm` against a directory on disk, with no hub.
//!
//! What can be tested without a network is the part that decides: which
//! domains come back, in what order, grouped into how many files, and what
//! happens when there is nothing to do. The reading itself is tested in
//! `umi-publish` against a real Parquet file, and the round trip through the
//! columns is tested in `umi-crawl`, so what is left here is the command's own
//! arithmetic.

use umi_state::{Shard, State};
use umi_state_sqlite::SqliteState;
use umi_types::{PldId, Ulid};

use crate::warm::{Options, domains, files, warm};

/// The moment every entry in this file is dated from.
const T0: u64 = 1_760_000_000_000;

/// A crawl directory, which is a directory with a profile in it.
fn crawl_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("profile.toml"),
        "name = \"test\"\nmax_depth = 0\n",
    )
    .expect("profile");
    dir
}

/// An entry saying a domain went into one file at one row group range.
fn shard(name: &str, segment: u8, first_group: u32, evicted_at_ms: u64) -> Shard {
    Shard {
        pld: PldId::derive(name.as_bytes()),
        segment: Ulid::new(T0 + u64::from(segment), [segment; 10]),
        first_group,
        last_group: first_group,
        rows: 100,
        evicted_at_ms,
    }
}

/// Publishing config with a token that is never used, since nothing here
/// reaches a hub.
fn publishing() -> crate::crawl::Publishing {
    crate::crawl::Publishing {
        org: "open-index".to_owned(),
        token: "not-a-real-token".to_owned(),
        key: "00".repeat(32),
        slice: 0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_says_how_many_would_come_back_and_brings_none() {
    let dir = crawl_dir();
    let path = dir.path().to_owned();
    tokio::task::spawn_blocking(move || {
        let state = SqliteState::open(path.join("state.sqlite")).expect("open");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        // Three domains in two files, so the message has something to get
        // wrong. Two of them share a segment, which is the ordinary case: an
        // eviction writes a thousand domains into one file.
        let entries = [
            shard("one.example", 1, 0, T0),
            shard("two.example", 1, 1, T0),
            shard("three.example", 2, 0, T0 + 1),
        ];
        runtime
            .block_on(state.put_shards(&entries))
            .expect("put_shards");
        drop(state);

        let options = Options {
            dir: path.clone(),
            limit: 10,
            dry_run: true,
        };
        let warmed = warm(&options, &publishing()).expect("dry run");
        assert_eq!(warmed.domains, 0, "a dry run brought something back");
        assert_eq!(warmed.files, 0);

        // Still cold, which is the point of a dry run.
        let state = SqliteState::open(path.join("state.sqlite")).expect("reopen");
        let cold = runtime.block_on(state.cold(10)).expect("cold");
        assert_eq!(cold.len(), 3, "a dry run cleared a pointer");
    })
    .await
    .expect("blocking");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_with_nothing_evicted_is_nothing_to_do_and_not_a_success() {
    // Worth its own case because the exit code differs. A warm that found
    // nothing cold is not a warm that failed, and an operator running this on a
    // schedule should not get an alert every time the disk is comfortable.
    let dir = crawl_dir();
    let path = dir.path().to_owned();
    tokio::task::spawn_blocking(move || {
        let options = Options {
            dir: path,
            limit: 10,
            ..Options::default()
        };
        let failure = warm(&options, &publishing()).expect_err("nothing evicted");
        assert!(matches!(failure, crate::Error::NothingToDo(_)), "{failure}");
    })
    .await
    .expect("blocking");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_directory_that_is_not_a_crawl_directory_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_owned();
    tokio::task::spawn_blocking(move || {
        let options = Options {
            dir: path,
            ..Options::default()
        };
        warm(&options, &publishing()).expect_err("no profile.toml");
    })
    .await
    .expect("blocking");
}

#[test]
fn domains_that_share_a_file_are_one_open_and_not_three() {
    // The claim the whole command is shaped around. A footer is read once per
    // file, so three domains evicted together cost one footer and three ranged
    // reads rather than three of each.
    let together = [
        shard("one.example", 1, 0, T0),
        shard("two.example", 1, 1, T0),
        shard("three.example", 1, 2, T0),
    ];
    assert_eq!(files(&together), 1);

    let apart = [
        shard("one.example", 1, 0, T0),
        shard("two.example", 2, 0, T0),
        shard("three.example", 3, 0, T0),
    ];
    assert_eq!(files(&apart), 3);
    assert_eq!(domains(&apart).len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_limit_takes_the_oldest_evictions_and_a_second_run_carries_on() {
    // A warm under a limit has to be resumable. If the two runs took different
    // slices of the same set, a domain could sit cold forever while the same
    // ones came back over and over.
    let dir = crawl_dir();
    let path = dir.path().to_owned();
    tokio::task::spawn_blocking(move || {
        let state = SqliteState::open(path.join("state.sqlite")).expect("open");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let entries = [
            shard("newest.example", 3, 0, T0 + 300),
            shard("oldest.example", 1, 0, T0 + 100),
            shard("middle.example", 2, 0, T0 + 200),
        ];
        runtime
            .block_on(state.put_shards(&entries))
            .expect("put_shards");

        let first = runtime.block_on(state.cold(2)).expect("cold");
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].evicted_at_ms, T0 + 100);
        assert_eq!(first[1].evicted_at_ms, T0 + 200);

        // What a warm does to the two it took, without the hub in the way.
        let done: Vec<PldId> = first.iter().map(|shard| shard.pld).collect();
        runtime
            .block_on(state.clear_shards(&done))
            .expect("clear_shards");

        let second = runtime.block_on(state.cold(2)).expect("cold");
        assert_eq!(second.len(), 1, "the second run took a domain twice");
        assert_eq!(second[0].evicted_at_ms, T0 + 300);
    })
    .await
    .expect("blocking");
}
