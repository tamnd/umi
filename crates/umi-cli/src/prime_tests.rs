//! `umi prime` without a hub.
//!
//! Two things decide what a prime does and neither of them needs a network.
//! The first is which corpus rows become documents at all, which is the ttl
//! and the body cap. The second is which of those documents actually reach the
//! store, which is the read that stops a published row from replacing a later
//! local one. The reading of Parquet itself is tested in `umi-publish` against
//! a real file, so it is not tested again here.

use std::collections::HashSet;
use std::io::Write as _;

use umi_state::{RobotsDoc, State};
use umi_state_sqlite::SqliteState;
use umi_types::HostId;

use crate::prime::{Options, Primed, Row, candidate, hosts_in, write};

/// The moment every row in this file is dated from.
const T0: u64 = 1_760_000_000_000;

/// A day, which is the ttl doc 07.4 gives a robots.txt.
const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// A store on disk, since that is the one every prime writes to.
fn store() -> (tempfile::TempDir, SqliteState) {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = SqliteState::open(dir.path().join("state.sqlite")).expect("open");
    (dir, state)
}

#[test]
fn a_row_fetched_inside_the_ttl_imports() {
    let mut done = Primed::default();
    let row = Row {
        host: Some("example.com"),
        fetched_ms: T0,
        status: 200,
        body: Some("User-agent: *\nDisallow: /private\n"),
    };
    let doc = candidate(&row, None, DAY_MS, T0 + 1000, 8192, &mut done).expect("a document");
    assert_eq!(doc.host, HostId::derive(b"example.com"));
    assert_eq!(doc.status, 200);
    assert_eq!(doc.fetched_ms, T0);
    // The expiry comes off the fetch and not off the import, which is the
    // whole point: a row is a day old from when the host served it.
    assert_eq!(doc.expires_ms, T0 + DAY_MS);
    assert_eq!(done, Primed::default());
}

#[test]
fn a_row_older_than_the_ttl_is_counted_and_left() {
    let mut done = Primed::default();
    let row = Row {
        host: Some("example.com"),
        fetched_ms: T0,
        status: 200,
        body: Some("User-agent: *\n"),
    };
    assert!(candidate(&row, None, DAY_MS, T0 + DAY_MS + 1, 8192, &mut done).is_none());
    assert_eq!(done.stale, 1);
    assert_eq!(done.oversized, 0);
}

#[test]
fn a_body_over_the_cap_is_counted_and_left() {
    let mut done = Primed::default();
    let big = "a".repeat(9000);
    let row = Row {
        host: Some("example.com"),
        fetched_ms: T0,
        status: 200,
        body: Some(&big),
    };
    assert!(candidate(&row, None, DAY_MS, T0, 8192, &mut done).is_none());
    assert_eq!(done.oversized, 1);
    assert_eq!(done.stale, 0);
}

#[test]
fn a_host_that_answered_nothing_still_imports() {
    // Status zero with no body is 41 percent of the corpus and it is the half
    // worth having most, because a host that never answers costs a full
    // connection timeout every time somebody asks it.
    let mut done = Primed::default();
    let row = Row {
        host: Some("gone.example"),
        fetched_ms: T0,
        status: 0,
        body: None,
    };
    let doc = candidate(&row, None, DAY_MS, T0, 8192, &mut done).expect("a document");
    assert_eq!(doc.status, 0);
    assert!(doc.body.is_none());
}

#[test]
fn a_null_hostname_is_skipped_without_a_reason() {
    let mut done = Primed::default();
    let row = Row {
        host: None,
        fetched_ms: T0,
        status: 200,
        body: None,
    };
    assert!(candidate(&row, None, DAY_MS, T0, 8192, &mut done).is_none());
    assert_eq!(done, Primed::default());
}

#[test]
fn a_host_outside_the_scope_is_counted_and_left() {
    let mut done = Primed::default();
    let scope: HashSet<HostId> = [HostId::derive(b"wanted.example")].into_iter().collect();
    let row = Row {
        host: Some("other.example"),
        fetched_ms: T0,
        status: 200,
        body: Some("User-agent: *\n"),
    };
    assert!(candidate(&row, Some(&scope), DAY_MS, T0, 8192, &mut done).is_none());
    assert_eq!(done.unwanted, 1);
    // Out of scope is checked before the ttl and the cap, so a row that is
    // both is only counted once and counted as the reason that matters.
    assert_eq!(done.stale, 0);
    assert_eq!(done.oversized, 0);
}

#[test]
fn a_host_inside_the_scope_imports() {
    let mut done = Primed::default();
    let scope: HashSet<HostId> = [HostId::derive(b"wanted.example")].into_iter().collect();
    let row = Row {
        host: Some("wanted.example"),
        fetched_ms: T0,
        status: 200,
        body: Some("User-agent: *\n"),
    };
    let doc = candidate(&row, Some(&scope), DAY_MS, T0, 8192, &mut done).expect("a document");
    assert_eq!(doc.host, HostId::derive(b"wanted.example"));
    assert_eq!(done, Primed::default());
}

#[test]
fn a_host_list_reads_urls_and_bare_names_the_same_way() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("seed.txt");
    let mut file = std::fs::File::create(&path).expect("create");
    // A URL, a bare hostname, an uppercase name, a comment, a blank line and
    // a line that names no host. A seed file has all of these in it.
    file.write_all(
        b"https://one.example/some/path?q=1\n\
          two.example\n\
          THREE.example\n\
          # a comment\n\
          \n\
          http://one.example/another\n\
          http://\n",
    )
    .expect("write");
    drop(file);

    let hosts = hosts_in(&path).expect("the list reads");
    // Three hosts, because the two one.example lines are the same host and
    // the uppercase one canonicalises down to the same bytes the corpus holds.
    assert_eq!(hosts.len(), 3);
    assert!(hosts.contains(&HostId::derive(b"one.example")));
    assert!(hosts.contains(&HostId::derive(b"two.example")));
    assert!(hosts.contains(&HostId::derive(b"three.example")));
}

#[test]
fn a_host_list_that_names_nothing_is_not_an_empty_scope() {
    // An empty scope would import the whole corpus by accident, which is the
    // exact thing the flag exists to stop, so it stops the run instead.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("seed.txt");
    std::fs::write(&path, "# nothing but a comment\n\n").expect("write");
    assert!(hosts_in(&path).is_err());
}

#[tokio::test]
async fn a_published_row_does_not_replace_a_later_local_one() {
    let (_dir, state) = store();
    let host = HostId::derive(b"example.com");
    // What a crawl fetched an hour after the corpus did.
    state
        .put_robots(&[RobotsDoc {
            host,
            digest: umi_types::Digest::derive(b"local"),
            fetched_ms: T0 + 3_600_000,
            expires_ms: T0 + 3_600_000 + DAY_MS,
            status: 200,
            body: Some("local".to_owned()),
        }])
        .await
        .expect("put");

    let mut done = Primed::default();
    let mut batch = vec![RobotsDoc {
        host,
        digest: umi_types::Digest::derive(b"published"),
        fetched_ms: T0,
        expires_ms: T0 + DAY_MS,
        status: 200,
        body: Some("published".to_owned()),
    }];
    write(&state, &mut batch, &Options::default(), &mut done)
        .await
        .expect("write");

    assert_eq!(done.fresher, 1);
    assert_eq!(done.imported, 0);
    let held = state.robots(&[host]).await.expect("robots");
    assert_eq!(held[0].body.as_deref(), Some("local"));
}

#[tokio::test]
async fn a_published_row_replaces_an_earlier_local_one() {
    let (_dir, state) = store();
    let host = HostId::derive(b"example.com");
    state
        .put_robots(&[RobotsDoc {
            host,
            digest: umi_types::Digest::derive(b"local"),
            fetched_ms: T0 - 3_600_000,
            expires_ms: T0 - 3_600_000 + DAY_MS,
            status: 200,
            body: Some("local".to_owned()),
        }])
        .await
        .expect("put");

    let mut done = Primed::default();
    let mut batch = vec![RobotsDoc {
        host,
        digest: umi_types::Digest::derive(b"published"),
        fetched_ms: T0,
        expires_ms: T0 + DAY_MS,
        status: 200,
        body: Some("published".to_owned()),
    }];
    write(&state, &mut batch, &Options::default(), &mut done)
        .await
        .expect("write");

    assert_eq!(done.fresher, 0);
    assert_eq!(done.imported, 1);
    let held = state.robots(&[host]).await.expect("robots");
    assert_eq!(held[0].body.as_deref(), Some("published"));
}

#[tokio::test]
async fn a_dry_run_counts_and_writes_nothing() {
    let (_dir, state) = store();
    let host = HostId::derive(b"example.com");
    let options = Options {
        dry_run: true,
        ..Options::default()
    };
    let mut done = Primed::default();
    let mut batch = vec![RobotsDoc {
        host,
        digest: umi_types::Digest::derive(b"published"),
        fetched_ms: T0,
        expires_ms: T0 + DAY_MS,
        status: 200,
        body: Some("published".to_owned()),
    }];
    write(&state, &mut batch, &options, &mut done)
        .await
        .expect("write");

    // The count is what would have gone in, so an operator can size the import
    // before paying for it.
    assert_eq!(done.imported, 1);
    assert!(state.robots(&[host]).await.expect("robots").is_empty());
}
