//! The eight steps against a hub that actually keeps the files.
//!
//! The scripted hub in [`crate::scripted`] answers requests. This one stores
//! them, which is the difference that matters here: the pipeline uploads a
//! file, reads it back, digests what came back, writes a manifest and then
//! reads that back too, and none of that proves anything against a hub that
//! replies with whatever the test decided in advance. So the fake keeps a map
//! of paths to bytes, serves ranges out of it, and lets the assertions be about
//! the thing doc 12.7 actually cares about: that the bytes on the far end are
//! the bytes that went up, and that the local file is only deleted when they
//! are.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use umi_file::{Create, SegmentWriter, StreamKind, WriterConfig, sample};
use umi_state::{MemoryState, SegmentQuery, SegmentRow, State, Stream};
use umi_types::{Digest, Ulid};

use crate::keys::{Role, SigningKey};
use crate::manifest::Manifest;
use crate::pipeline::{PublishConfig, Publisher};
use crate::scripted::{Say, Scripted, Seen};
use crate::{Blocked, Error};

/// Doc 12.4 puts a segment in the day its earliest row was fetched, so the
/// tests pick times rather than letting a clock pick them.
const DAY_ONE: u64 = 1_787_000_000_000;
const DAY_TWO: u64 = DAY_ONE + 86_400_000;

/// A repository path and the bytes at it, shared with the socket thread.
type Stored = Arc<Mutex<BTreeMap<(String, String), Vec<u8>>>>;

/// A path to the bytes to serve there, likewise.
type ByPath = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

/// A Hugging Face that remembers what it was given.
#[derive(Clone, Default)]
struct Files {
    /// `repo` then path inside it, to bytes.
    stored: Stored,
    /// lfs objects by oid, waiting for the commit that names them.
    blobs: ByPath,
    /// Bytes to serve instead of the truth, by path. This is how a corrupted
    /// remote copy is staged, and it only affects reads, so the upload still
    /// looks like it worked.
    lying: ByPath,
}

impl Files {
    fn get(&self, repo: &str, path: &str) -> Option<Vec<u8>> {
        if let Some(lie) = self.lying.lock().expect("not poisoned").get(path) {
            return Some(lie.clone());
        }
        self.stored
            .lock()
            .expect("not poisoned")
            .get(&(repo.to_owned(), path.to_owned()))
            .cloned()
    }

    fn put(&self, repo: &str, path: &str, bytes: Vec<u8>) {
        self.stored
            .lock()
            .expect("not poisoned")
            .insert((repo.to_owned(), path.to_owned()), bytes);
    }

    /// The stored files of one repository whose paths pass a test, in the
    /// shape the hub describes a file in. Both endpoints that answer that
    /// question use it, and they differ only in the test.
    fn entries<F>(&self, repo: &str, wanted: F) -> Vec<serde_json::Value>
    where
        F: Fn(&str) -> bool,
    {
        self.stored
            .lock()
            .expect("not poisoned")
            .iter()
            .filter(|((in_repo, in_path), _)| in_repo == repo && wanted(in_path))
            .map(|((_, in_path), bytes)| {
                serde_json::json!({
                    "type": "file",
                    "path": in_path,
                    "size": 130,
                    "lfs": { "oid": "an-oid", "size": bytes.len() },
                })
            })
            .collect()
    }

    fn paths(&self) -> Vec<String> {
        self.stored
            .lock()
            .expect("not poisoned")
            .keys()
            .map(|(_, path)| path.clone())
            .collect()
    }

    fn manifest(&self, repo: &str, day: &str) -> Manifest {
        let bytes = self
            .get(repo, &format!("_manifest/{day}.json"))
            .expect("the manifest is on the hub");
        Manifest::parse(&bytes).expect("the manifest parses")
    }

    /// Serve what was uploaded, but with these bytes at this path.
    fn corrupt(&self, path: &str, bytes: Vec<u8>) {
        self.lying
            .lock()
            .expect("not poisoned")
            .insert(path.to_owned(), bytes);
    }

    /// Answer one request the way the hub would.
    fn route(&self, request: &Seen, addr: std::net::SocketAddr) -> Say {
        let path = request
            .path
            .split('?')
            .next()
            .unwrap_or_default()
            .to_owned();

        if path == "/api/repos/create" {
            return Say::ok(serde_json::json!({ "url": "created" }));
        }
        if path.contains("/preupload/") {
            let files: Vec<_> = request.json()["files"]
                .as_array()
                .expect("files")
                .iter()
                .map(|file| serde_json::json!({ "path": file["path"], "uploadMode": "lfs" }))
                .collect();
            return Say::ok(serde_json::json!({ "files": files }));
        }
        if path.contains("/info/lfs/objects/batch") {
            let oid = request.json()["objects"][0]["oid"]
                .as_str()
                .expect("an oid")
                .to_owned();
            return Say::ok(serde_json::json!({
                "objects": [{ "actions": { "upload": { "href": format!("http://{addr}/put/{oid}") } } }],
            }));
        }
        if let Some(oid) = path.strip_prefix("/put/") {
            self.blobs
                .lock()
                .expect("not poisoned")
                .insert(oid.to_owned(), request.body.clone());
            return Say::status(200).header("etag", "\"an-etag\"");
        }
        if let Some(rest) = path.strip_suffix("/commit/main")
            && let Some(repo) = rest.strip_prefix("/api/datasets/")
        {
            for line in request.lines() {
                let value = &line["value"];
                match line["key"].as_str().unwrap_or_default() {
                    "file" => {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(value["content"].as_str().unwrap_or_default())
                            .expect("base64");
                        self.put(repo, value["path"].as_str().expect("a path"), bytes);
                    }
                    "lfsFile" => {
                        let oid = value["oid"].as_str().expect("an oid");
                        let bytes = self
                            .blobs
                            .lock()
                            .expect("not poisoned")
                            .get(oid)
                            .cloned()
                            .expect("the bytes were pushed before the commit named them");
                        self.put(repo, value["path"].as_str().expect("a path"), bytes);
                    }
                    _ => {}
                }
            }
            return Say::ok(serde_json::json!({ "commitOid": "c0ffee" }));
        }
        if let Some(rest) = path.strip_prefix("/api/datasets/")
            && let Some(repo) = rest.strip_suffix("/paths-info/main")
        {
            let wanted = request.json()["paths"][0]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            return Say::ok(serde_json::json!(
                self.entries(repo, |in_path| { in_path == wanted })
            ));
        }
        if let Some(rest) = path.strip_prefix("/api/datasets/")
            && let Some((repo, prefix)) = rest.split_once("/tree/main/")
        {
            // The real hub answers a path that names a file with a 404 rather
            // than with the file, which is the trap `Hub::info` exists to
            // avoid, so the fake one does it too.
            let named_a_file = self.get(repo, prefix).is_some();
            if named_a_file {
                return Say::status(404);
            }
            let prefix = prefix.to_owned();
            return Say::ok(serde_json::json!(
                self.entries(repo, |in_path| in_path.starts_with(&prefix))
            ));
        }
        if let Some(rest) = path.strip_prefix("/datasets/")
            && let Some((repo, in_repo)) = rest.split_once("/resolve/main/")
        {
            let Some(bytes) = self.get(repo, in_repo) else {
                return Say::status(404);
            };
            let Some(range) = request.headers.get("range") else {
                return Say::bytes(200, &bytes);
            };
            let (at, last) = range
                .trim_start_matches("bytes=")
                .split_once('-')
                .expect("a range");
            let at: usize = at.parse().expect("a number");
            let last: usize = last.parse().expect("a number");
            let end = (last + 1).min(bytes.len());
            return Say::bytes(206, &bytes[at.min(end)..end]);
        }
        Say::status(404)
    }
}

/// Everything a test needs: a hub with files in it, a store, and a publisher.
struct Fixture {
    files: Files,
    state: MemoryState,
    publisher: Publisher,
    dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        Self::with(|config| config).await
    }

    async fn with(tune: impl FnOnce(PublishConfig) -> PublishConfig) -> Self {
        let files = Files::default();
        let routing = files.clone();
        let hub =
            Scripted::routed(move |addr| move |request: &Seen| routing.route(request, addr)).await;
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = tune(PublishConfig {
            staging: dir.path().join("parquet"),
            coordinator: hex::encode([9u8; 32]),
            // Never, so that every test exercises the sampled path unless it
            // says otherwise. The full path has its own test.
            full_every: 0,
            ..PublishConfig::default()
        });
        let publisher = Publisher::new(
            hub.hub(),
            SigningKey::from_seed(Role::Publishing, [3u8; 32]),
            config,
        )
        .expect("the publisher builds");
        Self {
            files,
            state: MemoryState::new(),
            publisher,
            dir,
        }
    }

    /// Write a sealed segment and record it in state the way the crawl loop
    /// would.
    async fn seal(&self, n: u8, rows: usize, first_ms: u64) -> SegmentRow {
        let id = Ulid::new(first_ms, [n; 10]);
        let path = self.dir.path().join(format!("{}.umi", id.to_text()));
        let batch = sample::pages(rows);
        let mut writer = SegmentWriter::create(
            &path,
            Create {
                stream: StreamKind::Pages,
                segment_id: *id.as_bytes(),
                coordinator: [9u8; 32],
                created_ms: first_ms,
                canon_version: 1,
                extractor_version: 1,
                crawl_profile: 0,
            },
            WriterConfig::default(),
        )
        .expect("the writer opens");
        // Every row's `fetched_at_ms` decides the day folder, so it is set
        // here rather than left wherever the sample generator put it.
        let batch = stamp(&batch, first_ms);
        writer.push(&batch).expect("push");
        writer.seal().expect("seal");

        let bytes = std::fs::metadata(&path).expect("stat").len();
        let row = SegmentRow {
            id,
            stream: Stream::Pages,
            local_path: path.to_string_lossy().into_owned(),
            sealed_at_ms: first_ms + 1000,
            rows: rows as u64,
            bytes,
            local_digest: Digest::from_bytes([0u8; 32]),
            remote: None,
            manifest_day: None,
            deleted_at_ms: None,
        };
        self.state
            .put_segment(std::slice::from_ref(&row))
            .await
            .expect("the record goes in");
        row
    }
}

/// Rewrite `fetched_at_ms` so a test can decide which day a segment lands in.
fn stamp(batch: &arrow::record_batch::RecordBatch, ms: u64) -> arrow::record_batch::RecordBatch {
    use arrow::array::UInt64Array;
    use std::sync::Arc;

    let at = batch
        .schema()
        .index_of("fetched_at_ms")
        .expect("pages have a fetch time");
    let mut columns = batch.columns().to_vec();
    columns[at] = Arc::new(UInt64Array::from(vec![ms; batch.num_rows()]));
    arrow::record_batch::RecordBatch::try_new(batch.schema(), columns).expect("the batch rebuilds")
}

#[tokio::test]
async fn a_segment_goes_up_and_the_local_file_goes_away() {
    let fixture = Fixture::new().await;
    let row = fixture.seal(1, 64, DAY_ONE).await;

    let published = fixture
        .publisher
        .publish(&fixture.state, &row, DAY_ONE + 5000)
        .await
        .expect("it publishes");

    assert_eq!(published.blocked, None, "nothing should have blocked it");
    assert_eq!(published.rows, 64);
    assert!(
        published.path.starts_with("data/"),
        "doc 12.4 puts files under data/<day>/, not at {}",
        published.path
    );
    assert!(
        !std::path::Path::new(&row.local_path).exists(),
        "step 8 should have deleted the segment"
    );

    let stored = fixture
        .state
        .segment(row.id)
        .await
        .expect("the ledger answers")
        .expect("the record is there");
    let remote = stored.remote.expect("the remote copy is recorded");
    assert_eq!(remote.repo, published.repo);
    assert_eq!(remote.path, published.path);
    assert_eq!(remote.digest, published.digest, "doc 12.7 condition 4");
    assert_eq!(stored.deleted_at_ms, Some(DAY_ONE + 5000));
    assert_eq!(stored.manifest_day, Some(published.day));
}

#[tokio::test]
async fn the_manifest_is_committed_after_the_file_it_names() {
    let fixture = Fixture::new().await;
    let row = fixture.seal(2, 32, DAY_ONE).await;
    let published = fixture
        .publisher
        .publish(&fixture.state, &row, DAY_ONE)
        .await
        .expect("it publishes");

    let manifest = fixture.files.manifest(&published.repo, "20260817");
    assert!(
        manifest.contains(&published.path),
        "the manifest should name the file it was committed after"
    );
    let entry = manifest
        .files
        .iter()
        .find(|file| file.path == published.path)
        .expect("the entry");
    assert_eq!(entry.blake3, *published.digest.as_bytes());
    assert_eq!(entry.rows, 64.min(published.rows));
    assert_eq!(
        entry.verification.total(),
        published.rows,
        "every row is accounted for in doc 12.5's four counts"
    );

    let signature = fixture
        .files
        .get(&published.repo, "_manifest/20260817.json.sig")
        .expect("the signature is on the hub");
    let signature: [u8; 64] = signature.as_slice().try_into().expect("64 bytes");
    manifest
        .verify(
            &SigningKey::from_seed(Role::Publishing, [3u8; 32]).verifying(),
            &signature,
        )
        .expect("the published signature verifies");
}

#[tokio::test]
async fn a_second_segment_the_same_day_appends_to_the_manifest() {
    let fixture = Fixture::new().await;
    let first = fixture.seal(3, 16, DAY_ONE).await;
    let second = fixture.seal(4, 16, DAY_ONE + 60_000).await;

    let one = fixture
        .publisher
        .publish(&fixture.state, &first, DAY_ONE)
        .await
        .expect("the first publishes");
    let two = fixture
        .publisher
        .publish(&fixture.state, &second, DAY_ONE)
        .await
        .expect("the second publishes");

    assert_eq!(one.repo, two.repo, "the same week is the same repository");
    let manifest = fixture.files.manifest(&one.repo, &one.day.to_string());
    assert_eq!(
        manifest.files.len(),
        2,
        "the second commit should have appended, not replaced"
    );
    assert!(manifest.contains(&one.path) && manifest.contains(&two.path));
    let paths: Vec<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
    let mut sorted = paths.clone();
    sorted.sort_unstable();
    assert_eq!(paths, sorted, "doc 12.6 keeps a manifest in path order");
}

#[tokio::test]
async fn a_new_day_chains_to_the_day_before_it() {
    let fixture = Fixture::new().await;
    let first = fixture.seal(5, 16, DAY_ONE).await;
    let second = fixture.seal(6, 16, DAY_TWO).await;

    let one = fixture
        .publisher
        .publish(&fixture.state, &first, DAY_ONE)
        .await
        .expect("day one publishes");
    let two = fixture
        .publisher
        .publish(&fixture.state, &second, DAY_TWO)
        .await
        .expect("day two publishes");
    assert_ne!(one.day, two.day, "the two should be different days");

    let day_one = fixture.files.manifest(&one.repo, &one.day.to_string());
    let day_two = fixture.files.manifest(&two.repo, &two.day.to_string());
    assert_eq!(day_one.prev, None, "the first day of a repository has none");
    assert!(
        day_two.follows(&day_one).expect("both digest"),
        "doc 12.5's chain should link day two to day one"
    );
}

#[tokio::test]
async fn a_corrupted_remote_copy_is_not_recorded_and_is_not_deleted() {
    let fixture = Fixture::new().await;
    let row = fixture.seal(7, 64, DAY_ONE).await;

    // Serve a different byte at every offset the sampled read will look at.
    // The upload itself still succeeds, which is the case doc 12.7's second
    // condition exists for: a hub that took the bytes and gave back others.
    let id = row.id.to_text();
    fixture
        .files
        .corrupt(&format!("data/20260817/{id}.parquet"), vec![0u8; 1 << 20]);

    let failed = fixture
        .publisher
        .publish(&fixture.state, &row, DAY_ONE)
        .await
        .expect_err("a copy that reads back differently is not published");
    assert!(
        matches!(failed, Error::NotPublished(Blocked::DigestMismatch)),
        "expected doc 12.7 condition 2 to fail, got {failed}"
    );

    assert!(
        std::path::Path::new(&row.local_path).exists(),
        "the local segment must survive a failed verification"
    );
    let stored = fixture
        .state
        .segment(row.id)
        .await
        .expect("the ledger answers")
        .expect("the record is there");
    assert_eq!(
        stored.remote, None,
        "nothing should be recorded for a copy that did not check out"
    );
    let due = fixture
        .state
        .segments(SegmentQuery::Unpublished)
        .await
        .expect("the ledger answers");
    assert_eq!(due.len(), 1, "it should still be due");
}

#[tokio::test]
async fn a_full_read_back_is_the_same_check_by_a_longer_route() {
    // `full_every: 1` makes every segment take doc 12.7's one in a hundred
    // path, which downloads the whole object instead of three ranges.
    let fixture = Fixture::with(|config| PublishConfig {
        full_every: 1,
        ..config
    })
    .await;
    let row = fixture.seal(8, 32, DAY_ONE).await;

    let published = fixture
        .publisher
        .publish(&fixture.state, &row, DAY_ONE)
        .await
        .expect("it publishes");
    assert_eq!(published.blocked, None);
    assert!(!std::path::Path::new(&row.local_path).exists());
}

#[tokio::test]
async fn draining_publishes_everything_due_and_reports_what_it_could_not() {
    let fixture = Fixture::new().await;
    let good = fixture.seal(9, 16, DAY_ONE).await;
    let bad = fixture.seal(10, 16, DAY_ONE).await;
    let id = bad.id.to_text();
    fixture
        .files
        .corrupt(&format!("data/20260817/{id}.parquet"), vec![1u8; 1 << 20]);

    let (done, failed) = fixture
        .publisher
        .drain(&fixture.state, DAY_ONE)
        .await
        .expect("the drain runs");

    assert_eq!(done.len(), 1, "one segment should have gone up");
    assert_eq!(done[0].segment, good.id);
    assert_eq!(failed.len(), 1, "the other should be reported, not hidden");
    assert_eq!(failed[0].0, bad.id);
    assert_eq!(
        fixture
            .state
            .segments(SegmentQuery::Unpublished)
            .await
            .expect("the ledger answers")
            .len(),
        1,
        "a failed segment stays due so the next drain retries it"
    );
}

#[tokio::test]
async fn collecting_deletes_the_files_a_crash_left_behind() {
    let fixture = Fixture::new().await;
    let row = fixture.seal(11, 16, DAY_ONE).await;
    fixture
        .publisher
        .publish(&fixture.state, &row, DAY_ONE)
        .await
        .expect("it publishes");

    // Put the local file back and clear the deletion mark, which is what a
    // crash between step 7 and step 8 leaves: a complete ledger row and a file
    // still on disk.
    std::fs::write(&row.local_path, b"whatever was there").expect("write");
    let stored = fixture
        .state
        .segment(row.id)
        .await
        .expect("the ledger answers")
        .expect("the record is there");
    fixture
        .state
        .put_segment(&[SegmentRow {
            deleted_at_ms: None,
            ..stored
        }])
        .await
        .expect("the record goes in");

    let collected = fixture
        .publisher
        .collect(&fixture.state, DAY_ONE + 9000)
        .await
        .expect("the collection runs");
    assert_eq!(collected, 1);
    assert!(!std::path::Path::new(&row.local_path).exists());
    assert_eq!(
        fixture
            .state
            .segments(SegmentQuery::Collectable)
            .await
            .expect("the ledger answers")
            .len(),
        0,
        "a collected segment is not offered again"
    );
}

#[tokio::test]
async fn the_staged_parquet_does_not_survive_the_publish() {
    let fixture = Fixture::new().await;
    let row = fixture.seal(12, 16, DAY_ONE).await;
    fixture
        .publisher
        .publish(&fixture.state, &row, DAY_ONE)
        .await
        .expect("it publishes");

    let staged: Vec<_> = std::fs::read_dir(fixture.dir.path().join("parquet"))
        .expect("the staging directory is there")
        .filter_map(std::result::Result::ok)
        .collect();
    assert!(
        staged.is_empty(),
        "publishing twice the disk of a segment is doc 12.1's failure mode"
    );
    assert!(
        fixture
            .files
            .paths()
            .iter()
            .any(|path| path.ends_with(".parquet")),
        "the file should be on the hub even though the staged copy is gone"
    );
}

#[test]
fn the_two_spellings_of_a_stream_still_agree() {
    // Doc 08.3's `Stream` and doc 10's `StreamKind` are the same three values
    // written out in two crates that do not depend on each other. This is the
    // test the comment on `umi_state::Stream` promises.
    for (state, file) in [
        (Stream::Pages, StreamKind::Pages),
        (Stream::Receipts, StreamKind::Receipts),
        (Stream::Robots, StreamKind::Robots),
    ] {
        assert_eq!(crate::stream_kind(state), file);
        assert_eq!(crate::stream_of(file), state);
        assert_eq!(
            state.as_u8(),
            file as u8,
            "the discriminants have to line up or a stored segment changes stream"
        );
    }
}
