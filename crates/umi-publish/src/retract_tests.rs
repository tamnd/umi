//! Tests for the retraction planner.
//!
//! Everything here is about the chain. Dropping a file out of a manifest is
//! bookkeeping and would be hard to get wrong; relinking the days after it is
//! the part that decides whether the repository still verifies afterwards, and
//! a mistake there is invisible until somebody tries to check a signature.

use crate::manifest::{FileEntry, MANIFEST_VERSION, Manifest, Verification};

const REPO: &str = "open-index/umi-robots";

fn entry(name: &str, rows: u64) -> FileEntry {
    FileEntry {
        path: format!("data/20260901/{name}.parquet"),
        bytes: 134_217_728,
        rows,
        blake3: [1u8; 32],
        sha256: [2u8; 32],
        segment_ulid: name.to_owned(),
        coordinator: "server3".to_owned(),
        extractor: "umi-extract/0.0.1".to_owned(),
        fetched_at_min_ms: 1_756_732_800_000,
        fetched_at_max_ms: 1_756_733_640_000,
        verification: Verification {
            local: rows,
            ..Verification::default()
        },
    }
}

fn day(day: &str, files: Vec<FileEntry>, prev: Option<[u8; 32]>) -> Manifest {
    Manifest {
        manifest_version: MANIFEST_VERSION,
        repo: REPO.to_owned(),
        day: day.to_owned(),
        prev,
        canon_version: umi_types::CANON_VERSION.to_owned(),
        schema_id: "umi-robots/1".to_owned(),
        files,
    }
}

/// Three days, chained the way the publisher chains them.
fn chain() -> Vec<Manifest> {
    let one = day("20260901", vec![entry("A", 100), entry("B", 200)], None);
    let two = day(
        "20260902",
        vec![entry("C", 300)],
        Some(one.digest().expect("digest")),
    );
    let three = day(
        "20260903",
        vec![entry("D", 400)],
        Some(two.digest().expect("digest")),
    );
    vec![one, two, three]
}

/// Whether every link in a run of manifests points at the day before it.
fn linked(days: &[Manifest]) -> bool {
    days.windows(2)
        .all(|pair| pair[1].prev == Some(pair[0].digest().expect("digest")))
}

#[test]
fn the_chain_the_fixture_builds_is_a_chain() {
    // If this is not true the rest of the file proves nothing.
    assert!(linked(&chain()));
}

#[test]
fn taking_a_file_out_of_the_first_day_relinks_every_day_after_it() {
    // The whole reason this is a module rather than a shell loop. Removing a
    // file changes its day's digest, so the next day's `prev` stops naming
    // anything, and so on to the end of the repository. A retraction that
    // rewrote only the day it touched would leave a chain that reads as
    // tampered from the second day onward.
    let before = chain();
    let paths = vec![entry("A", 100).path];
    let plan = super::plan(&before, &paths).expect("plan");

    assert_eq!(plan.manifests.len(), 3);
    assert_eq!(plan.manifests[0].files.len(), 1, "the file did not go");
    assert_eq!(plan.manifests[0].files[0].path, entry("B", 200).path);
    assert!(linked(&plan.manifests), "the chain was left broken");

    // And every day is genuinely rewritten, because day one's digest moved.
    assert_eq!(super::changed(&before, &plan.manifests).len(), 3);
}

#[test]
fn taking_a_file_out_of_the_last_day_rewrites_one_manifest_and_not_all_of_them() {
    // The other end of the same rule. Nothing before the first change needs to
    // move, so a retraction against a recent day is cheap on a repository with
    // a year of history in it, and rewriting the untouched days would be
    // churning signatures for no reason.
    let before = chain();
    let paths = vec![entry("D", 400).path];
    let plan = super::plan(&before, &paths).expect("plan");

    assert_eq!(super::changed(&before, &plan.manifests), ["20260903"]);
    assert_eq!(plan.manifests[0], before[0]);
    assert_eq!(plan.manifests[1], before[1]);
    assert!(plan.manifests[2].files.is_empty());
    assert!(linked(&plan.manifests));
}

#[test]
fn a_day_left_with_nothing_in_it_keeps_its_link() {
    // Dropping the manifest of an emptied day would be the tidy looking answer
    // and it would break the chain a second time, because the day after it
    // points at this one. An empty manifest is a link that still carries a
    // signature, which is what a reader walking the chain needs.
    let before = chain();
    let paths = vec![entry("C", 300).path];
    let plan = super::plan(&before, &paths).expect("plan");

    assert_eq!(plan.manifests.len(), 3);
    assert!(plan.manifests[1].files.is_empty());
    assert_eq!(plan.manifests[1].day, "20260902");
    assert!(linked(&plan.manifests));
}

#[test]
fn the_record_remembers_the_digest_the_file_had() {
    // The one field the record exists for. A reader whose recorded digest stops
    // matching has to be able to find out that we removed the file, rather than
    // concluding they were served something altered, and that is only possible
    // if the old digest is written down before it is gone.
    let before = chain();
    let paths = vec![entry("A", 100).path, entry("C", 300).path];
    let plan = super::plan(&before, &paths).expect("plan");

    assert_eq!(plan.removed.len(), 2);
    assert_eq!(plan.removed[0].rows, 100);
    assert_eq!(plan.removed[1].rows, 300);
    for removed in &plan.removed {
        assert_eq!(
            removed.digest,
            format!("blake3:{}", "01".repeat(32)),
            "the record does not carry the digest the manifest had",
        );
    }
}

#[test]
fn retracting_something_the_repository_does_not_have_is_refused() {
    // A typo in a path is the likeliest way this command gets used wrongly, and
    // the failure mode without this check is a commit that deletes nothing,
    // rewrites every manifest anyway and re-signs the lot. Every reader's
    // recorded digest breaks for no change at all.
    let before = chain();
    let paths = vec!["data/20260901/NOPE.parquet".to_owned()];
    let failed = super::plan(&before, &paths);
    assert!(failed.is_err(), "a path that is not there was accepted");
}

#[test]
fn the_record_names_where_it_goes_and_survives_a_round_trip() {
    let record = super::Retraction {
        repo: REPO.to_owned(),
        at_ms: 1_756_800_000_000,
        reason: "overlapping runs published the same hosts twice".to_owned(),
        removed: vec![super::Removed {
            path: entry("A", 100).path,
            rows: 100,
            bytes: 134_217_728,
            digest: format!("blake3:{}", "01".repeat(32)),
        }],
        rewritten: vec!["20260901".to_owned()],
    };
    assert_eq!(
        record.path(),
        "retractions/1756800000000-open-index_umi-robots.json",
    );

    let json = record.to_json().expect("json");
    let read: super::Retraction = serde_json::from_slice(&json).expect("parse");
    assert_eq!(read, record);
}
