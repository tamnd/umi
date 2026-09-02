//! What `umi evict` does before it needs a hub.
//!
//! Everything past the spill wants a token, a signing key and a network, so
//! what is checkable here is the part that decides which domains move and the
//! part that refuses to do anything. Both are worth checking: a command that
//! picked the wrong domains would publish the wrong backlog, and a command that
//! ran happily on an empty store would report success for doing nothing.

use umi_state::{Candidate, Discovery, Priority, State};
use umi_state_sqlite::SqliteState;
use umi_types::RowKey;

use crate::Error;
use crate::crawl::Publishing;
use crate::evict::{Options, evict};

const T0: u64 = 1_760_000_000_000;

/// The smallest profile that parses, which is what makes a directory a crawl
/// directory as far as this command is concerned.
const PROFILE: &str = "name = \"test\"\nmax_depth = 0\n";

/// Secrets that are never used, because every test here stops before the hub.
fn publishing() -> Publishing {
    Publishing {
        org: "open-index".to_owned(),
        token: "not-a-token".to_owned(),
        key: "0".repeat(64),
        slice: 0,
    }
}

/// A crawl directory holding `hosts` domains with one url each.
async fn directory(hosts: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("profile.toml"), PROFILE).expect("profile");
    std::fs::create_dir_all(dir.path().join("segments")).expect("segments");
    let state = SqliteState::open(dir.path().join("state.sqlite")).expect("state");
    let urls: Vec<String> = (0..hosts)
        .map(|i| format!("https://site{i}.example/index.html"))
        .collect();
    let batch: Vec<Candidate<'_>> = urls
        .iter()
        .map(|url| Candidate {
            key: RowKey::for_url(url, None).expect("well formed"),
            url,
            depth: 0,
            priority: Priority::DEFAULT,
            discovered_ms: T0,
            discovery: Discovery::Trusted,
            lastmod_ms: None,
        })
        .collect();
    state.admit(&batch).await.expect("admit");
    dir
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_says_how_many_would_move_and_moves_none() {
    // The runtime inside `evict` is a current thread one, so the test needs a
    // thread to block on and this needs to be a multi thread flavour.
    let dir = directory(20).await;
    let summary = tokio::task::spawn_blocking({
        let path = dir.path().to_path_buf();
        move || {
            evict(
                &Options {
                    dir: path,
                    limit: 5,
                    dry_run: true,
                },
                &publishing(),
            )
        }
    })
    .await
    .expect("join")
    .expect("dry run");

    assert_eq!(summary.published, 0);
    assert_eq!(summary.files, 0);

    // Nothing left the store, which is the half of a dry run that matters.
    let state = SqliteState::open(dir.path().join("state.sqlite")).expect("state");
    assert_eq!(state.resident().await.expect("resident").len(), 20);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_store_is_nothing_to_do_and_not_a_success() {
    // The exit code doc 14.9 wants for this is the one that says a command
    // that could not have done anything did not, so an operator scripting it
    // can tell it apart from a run that freed no space because the hub was
    // down.
    let dir = directory(0).await;
    let failure = tokio::task::spawn_blocking({
        let path = dir.path().to_path_buf();
        move || {
            evict(
                &Options {
                    dir: path,
                    limit: 5,
                    dry_run: true,
                },
                &publishing(),
            )
        }
    })
    .await
    .expect("join")
    .expect_err("an empty store has nothing to evict");

    assert!(matches!(failure, Error::NothingToDo(_)), "{failure}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_directory_that_is_not_a_crawl_directory_is_refused() {
    // A profile is what tells one directory from another, and a command that
    // opened a state file wherever it was pointed would be a command you could
    // aim at the wrong disk.
    let dir = tempfile::tempdir().expect("tempdir");
    let failure = tokio::task::spawn_blocking({
        let path = dir.path().to_path_buf();
        move || {
            evict(
                &Options {
                    dir: path,
                    ..Options::default()
                },
                &publishing(),
            )
        }
    })
    .await
    .expect("join")
    .expect_err("no profile, no crawl directory");

    assert!(matches!(failure, Error::Io(_)), "{failure}");
}
