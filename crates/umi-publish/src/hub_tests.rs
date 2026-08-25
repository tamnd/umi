//! The hub client against a scripted Hugging Face.
//!
//! A real hub is not available to a test and a mocked client is not worth
//! testing, so what runs here is the actual `reqwest` client against an actual
//! socket that speaks the actual protocol. That covers the things this module
//! can get wrong: the shape of the ndjson, the order of the requests, the
//! numeric sort on multipart, the statuses that are answers rather than
//! failures, and the token going exactly one place.

use std::time::Duration;

use super::{Hub, Retry, Upload, bare, message, retryable, scatter, split};
use crate::Error;
use crate::scripted::{Say, Scripted, Seen};

/// A file on disk with known bytes, and its sha256.
fn blob(dir: &std::path::Path, name: &str, size: usize) -> (Upload, Vec<u8>) {
    use sha2::Digest as _;
    let bytes: Vec<u8> = (0..size).map(|n| (n % 251) as u8).collect();
    let local = dir.join(name);
    std::fs::write(&local, &bytes).expect("write");
    let sha256 = hex::encode(sha2::Sha256::digest(&bytes));
    (
        Upload::Blob {
            path: format!("data/20260817/{name}"),
            local,
            size: size as u64,
            sha256: format!("sha256:{sha256}"),
        },
        bytes,
    )
}

/// The three requests an lfs upload makes, answered the usual way.
fn usual(request: &Seen, upload: &str) -> Option<Say> {
    if request.path.contains("/preupload/") {
        let files = request.json()["files"]
            .as_array()
            .expect("files")
            .iter()
            .map(|file| serde_json::json!({ "path": file["path"], "uploadMode": "lfs" }))
            .collect::<Vec<_>>();
        return Some(Say::ok(serde_json::json!({ "files": files })));
    }
    if request.path.contains("/info/lfs/objects/batch") {
        return Some(Say::ok(serde_json::json!({
            "objects": [{ "actions": { "upload": { "href": upload } } }],
        })));
    }
    if request.path.contains("/commit/") {
        return Some(Say::ok(serde_json::json!({ "commitOid": "deadbeef" })));
    }
    None
}

#[tokio::test]
async fn a_small_file_rides_inside_the_commit() {
    let hub = Scripted::new(|request| {
        assert!(!request.path.contains("preupload"), "nothing to preupload");
        Say::ok(serde_json::json!({ "commitOid": "abc123" }))
    })
    .await;

    let commit = hub
        .hub()
        .upload(
            "open-index/umi-pages-2026w34-00",
            &[Upload::Inline {
                path: "_manifest/20260817.json".to_owned(),
                bytes: b"{\"manifest_version\":1}".to_vec(),
            }],
            "one manifest",
        )
        .await
        .expect("commit");

    assert_eq!(commit.oid, "abc123");
    assert_eq!(commit.deduplicated, 0);
    let seen = hub.seen();
    assert_eq!(seen.len(), 1, "one request, and it is the commit");
    let lines = seen[0].lines();
    assert_eq!(lines[0]["key"], "header");
    assert_eq!(lines[1]["key"], "file");
    assert_eq!(lines[1]["value"]["path"], "_manifest/20260817.json");
    assert_eq!(lines[1]["value"]["encoding"], "base64");
    use base64::Engine as _;
    let content = base64::engine::general_purpose::STANDARD
        .decode(lines[1]["value"]["content"].as_str().expect("content"))
        .expect("base64");
    assert_eq!(content, b"{\"manifest_version\":1}");
}

#[tokio::test]
async fn a_parquet_file_goes_through_lfs_and_the_commit_names_its_digest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (file, bytes) = blob(dir.path(), "01K2M8Q0P7R3XN5.parquet", 4096);
    let Upload::Blob { sha256, .. } = &file else {
        unreachable!()
    };
    let expected = sha256.clone();

    let hub = Scripted::routed(|addr| {
        move |request: &Seen| {
            if request.method == "PUT" {
                return Say::status(200).header("etag", "\"an-etag\"");
            }
            usual(request, &format!("http://{addr}/upload")).unwrap_or_else(|| Say::status(500))
        }
    })
    .await;

    let commit = hub
        .hub()
        .upload("open-index/umi-pages-2026w34-00", &[file], "one segment")
        .await
        .expect("commit");

    assert_eq!(commit.deduplicated, 0, "the hub did not have these bytes");
    let paths = hub.paths();
    assert_eq!(paths.len(), 4, "preupload, batch, put, commit: {paths:?}");
    assert!(paths[0].contains("/preupload/main"), "{paths:?}");
    assert!(paths[1].contains("/info/lfs/objects/batch"), "{paths:?}");
    assert_eq!(paths[2], "PUT /upload", "{paths:?}");
    assert!(paths[3].contains("/commit/main"), "{paths:?}");

    assert_eq!(
        hub.seen()[2].body,
        bytes,
        "the bytes that went up are the file"
    );
    let batch = hub.seen()[1].json();
    assert_eq!(batch["objects"][0]["size"], bytes.len());
    assert_eq!(
        batch["objects"][0]["oid"],
        expected.trim_start_matches("sha256:"),
        "lfs wants the bare digest and never the prefixed spelling"
    );
    assert_eq!(batch["hash_algo"], "sha_256");

    let lines = hub.seen()[3].lines();
    assert_eq!(lines[1]["key"], "lfsFile");
    assert_eq!(lines[1]["value"]["algo"], "sha256");
    assert_eq!(lines[1]["value"]["size"], bytes.len());
}

#[tokio::test]
async fn bytes_the_hub_already_has_are_not_uploaded_twice() {
    // Doc 12.6's point about content addressed storage, which is what makes
    // doc 12.8's recovery path cheap enough to use without thinking about it.
    let dir = tempfile::tempdir().expect("tempdir");
    let (file, _) = blob(dir.path(), "01K2M8Q0P7R3XN5.parquet", 2048);

    let hub = Scripted::new(|request| {
        if request.path.contains("/info/lfs/objects/batch") {
            // No actions at all is git-lfs for "already present".
            return Say::ok(serde_json::json!({ "objects": [{}] }));
        }
        usual(request, "unused").unwrap_or_else(|| Say::status(500))
    })
    .await;

    let commit = hub
        .hub()
        .upload("open-index/umi-pages-2026w34-00", &[file], "again")
        .await
        .expect("commit");

    assert_eq!(commit.deduplicated, 1);
    assert!(
        hub.seen().iter().all(|request| request.method != "PUT"),
        "nothing was uploaded"
    );
    assert!(
        hub.paths().last().expect("a commit").contains("/commit/"),
        "and it was still committed, because the commit is what publishes it"
    );
}

#[tokio::test]
async fn a_multipart_upload_puts_its_parts_in_numeric_order() {
    // The part urls arrive keyed by a number that is a string, and string
    // order puts 10 before 2. Getting this wrong produces a file of exactly
    // the right size with the wrong bytes in it, which no size check catches.
    let dir = tempfile::tempdir().expect("tempdir");
    let (file, bytes) = blob(dir.path(), "01K2M8Q0P7R3XN5.parquet", 1100);

    let hub = Scripted::routed(|addr| {
        move |request: &Seen| {
            if request.method == "PUT" {
                return Say::status(200).header("etag", &format!("etag{}", request.path));
            }
            if request.path == "/complete" {
                return Say::status(200);
            }
            if request.path.contains("/info/lfs/objects/batch") {
                let mut header = serde_json::Map::new();
                header.insert("chunk_size".into(), "100".into());
                for part in 1..=11 {
                    header.insert(
                        part.to_string(),
                        format!("http://{addr}/part/{part}").into(),
                    );
                }
                return Say::ok(serde_json::json!({
                    "objects": [{ "actions": { "upload": {
                        "href": format!("http://{addr}/complete"),
                        "header": header,
                    }}}],
                }));
            }
            usual(request, "unused").unwrap_or_else(|| Say::status(500))
        }
    })
    .await;

    hub.hub()
        .upload("open-index/umi-pages-2026w34-00", &[file], "eleven parts")
        .await
        .expect("commit");

    let puts: Vec<Seen> = hub
        .seen()
        .into_iter()
        .filter(|request| request.method == "PUT")
        .collect();
    assert_eq!(puts.len(), 11);
    let order: Vec<String> = puts.iter().map(|put| put.path.clone()).collect();
    assert_eq!(order[1], "/part/2", "part 2 before part 10: {order:?}");
    assert_eq!(order[9], "/part/10", "{order:?}");

    let rebuilt: Vec<u8> = puts.iter().flat_map(|put| put.body.clone()).collect();
    assert_eq!(rebuilt, bytes, "the parts reassemble into the file");
    assert_eq!(
        puts[10].body.len(),
        100,
        "the eleventh part is the last 100"
    );

    let complete = hub
        .seen()
        .into_iter()
        .find(|request| request.path == "/complete")
        .expect("the completion post");
    let parts = complete.json();
    assert_eq!(parts["parts"][0]["partNumber"], 1);
    assert_eq!(parts["parts"][9]["partNumber"], 10);
    assert_eq!(parts["parts"][9]["etag"], "etag/part/10");
}

#[tokio::test]
async fn every_file_is_up_before_the_commit_that_names_it() {
    // Doc 12.6's ordering rule, which is the one thing here a consumer can
    // observe. The other order publishes a manifest promising files a crash
    // lost, and nobody reading it can tell that apart from us having lied.
    let dir = tempfile::tempdir().expect("tempdir");
    let (one, _) = blob(dir.path(), "01K2M8Q0P7R3XN5.parquet", 512);
    let (two, _) = blob(dir.path(), "01K2M8QF2A1C9WZ.parquet", 512);

    let hub = Scripted::routed(|addr| {
        move |request: &Seen| {
            if request.method == "PUT" {
                return Say::status(200);
            }
            if request.path.contains("/preupload/") {
                let files = request.json()["files"]
                    .as_array()
                    .expect("files")
                    .iter()
                    .map(|file| serde_json::json!({ "path": file["path"], "uploadMode": "lfs" }))
                    .collect::<Vec<_>>();
                return Say::ok(serde_json::json!({ "files": files }));
            }
            usual(request, &format!("http://{addr}/upload")).unwrap_or_else(|| Say::status(500))
        }
    })
    .await;

    hub.hub()
        .upload(
            "open-index/umi-pages-2026w34-00",
            &[
                one,
                two,
                Upload::Inline {
                    path: "_manifest/20260817.json".to_owned(),
                    bytes: b"{}".to_vec(),
                },
            ],
            "two segments and the manifest",
        )
        .await
        .expect("commit");

    let paths = hub.paths();
    let commit = paths
        .iter()
        .position(|path| path.contains("/commit/"))
        .expect("a commit");
    let uploads = paths
        .iter()
        .enumerate()
        .filter(|(_, path)| path.starts_with("PUT"))
        .count();
    assert_eq!(uploads, 2, "both blobs went up: {paths:?}");
    assert_eq!(
        commit,
        paths.len() - 1,
        "and the commit was last: {paths:?}"
    );
}

#[tokio::test]
async fn a_range_read_gives_back_exactly_the_bytes_it_asked_for() {
    let hub = Scripted::new(|request| {
        assert_eq!(
            request.headers.get("range").map(String::as_str),
            Some("bytes=1048576-2097151")
        );
        Say::bytes(206, &vec![7u8; 1 << 20])
    })
    .await;

    let bytes = hub
        .hub()
        .read_range(
            "open-index/umi-pages-2026w34-00",
            "data/20260817/01K2M8Q0P7R3XN5.parquet",
            1 << 20,
            1 << 20,
        )
        .await
        .expect("range");
    assert_eq!(bytes.len(), 1 << 20);
    assert!(bytes.iter().all(|byte| *byte == 7));
}

#[tokio::test]
async fn a_hub_that_ignores_the_range_is_an_error_and_not_a_full_download() {
    // Doc 12.7 condition 2 checks the returned bytes against doc 10's chunk
    // tree at the offsets it asked for. Bytes from somewhere else in the file
    // would fail that check for the wrong reason, and silently downloading 128
    // MB is the bandwidth doc 12.7 chose sampling to avoid.
    let hub = Scripted::new(|_| Say::bytes(200, &[0u8; 64])).await;
    let error = hub
        .hub()
        .read_range("open-index/x", "data/a.parquet", 0, 64)
        .await
        .expect_err("a 200 is not a range");
    assert!(format!("{error}").contains("whole file"), "got {error}");
}

#[tokio::test]
async fn a_listing_reports_the_content_size_and_not_the_pointers() {
    // An lfs entry's top level size is the 130 byte pointer file, which is
    // never the answer to doc 12.7's first condition.
    let hub = Scripted::new(|_| {
        Say::ok(serde_json::json!([
            { "type": "directory", "path": "data/20260817" },
            { "type": "file", "path": "data/20260817/a.parquet", "size": 130,
              "lfs": { "oid": "9c11", "size": 134_217_728u64 } },
            { "type": "file", "path": "_manifest/20260817.json", "size": 689 },
        ]))
    })
    .await;

    let files = hub
        .hub()
        .list("open-index/umi-pages-2026w34-00", "")
        .await
        .expect("list");
    assert_eq!(files.len(), 2, "directories are not files");
    assert_eq!(files[0].size, 134_217_728);
    assert_eq!(files[0].sha256.as_deref(), Some("9c11"));
    assert_eq!(files[1].size, 689);
    assert_eq!(
        files[1].sha256, None,
        "an inline file's oid is a git blob hash and not a digest of anything"
    );
}

#[tokio::test]
async fn a_repository_that_is_already_there_is_not_a_failure() {
    let hub = Scripted::new(|_| Say::status(409)).await;
    assert!(
        !hub.hub()
            .ensure_dataset("open-index/umi-pages-2026w34-00")
            .await
            .expect("409 is an answer"),
        "it did not create it"
    );

    let fresh = Scripted::new(|request| {
        let body = request.json();
        assert_eq!(body["type"], "dataset");
        assert_eq!(body["organization"], "open-index");
        assert_eq!(body["name"], "umi-pages-2026w34-00");
        Say::ok(serde_json::json!({}))
    })
    .await;
    assert!(
        fresh
            .hub()
            .ensure_dataset("open-index/umi-pages-2026w34-00")
            .await
            .expect("created")
    );
}

#[tokio::test]
async fn a_repository_with_nothing_in_it_lists_as_empty() {
    let hub = Scripted::new(|_| Say::status(404)).await;
    let files = hub
        .hub()
        .list("open-index/not-yet", "")
        .await
        .expect("list");
    assert!(files.is_empty());
}

#[tokio::test]
async fn a_token_that_cannot_write_is_visible_before_anything_depends_on_it() {
    let hub = Scripted::new(|request| {
        assert_eq!(request.path, "/api/whoami-v2");
        Say::ok(serde_json::json!({
            "name": "someone",
            "orgs": [{ "name": "open-index" }],
            "auth": { "accessToken": { "role": "read" } },
        }))
    })
    .await;

    let who = hub.hub().whoami().await.expect("whoami");
    assert_eq!(who.name, "someone");
    assert_eq!(who.orgs, vec!["open-index".to_owned()]);
    assert!(!who.write);
}

#[tokio::test]
async fn a_fine_grained_token_counts_as_one_that_can_write() {
    // The token this project actually uses is fine grained, with write scoped
    // to one organisation, and the whoami answer does not say which. Calling
    // that read only would refuse the only token we have.
    let hub = Scripted::new(|_| {
        Say::ok(serde_json::json!({
            "name": "someone",
            "auth": { "accessToken": { "role": "fineGrained" } },
        }))
    })
    .await;
    assert!(hub.hub().whoami().await.expect("whoami").write);
}

#[tokio::test]
async fn the_token_goes_in_one_header_and_nowhere_else() {
    let hub = Scripted::new(|_| Say::ok(serde_json::json!({ "name": "someone" }))).await;
    let client = hub.hub();
    assert!(
        !format!("{client:?}").contains("hf_scripted_token"),
        "Debug does not print it"
    );
    client.whoami().await.expect("whoami");

    let request = &hub.seen()[0];
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Bearer hf_scripted_token")
    );
    assert!(
        !request.path.contains("hf_scripted_token"),
        "not in the url"
    );
    for (name, value) in &request.headers {
        assert!(
            name == "authorization" || !value.contains("hf_scripted_token"),
            "{name} carried it too"
        );
    }
}

#[tokio::test]
async fn a_refusal_is_not_retried_and_a_failure_is() {
    let hub =
        Scripted::new(|_| Say::json(403, serde_json::json!({ "error": "no write access" }))).await;
    let error = hub
        .hub()
        .whoami()
        .await
        .expect_err("403 is an answer, not a hiccup");
    assert!(matches!(&error, Error::Hub { status: 403, .. }));
    assert!(format!("{error}").contains("no write access"));
    assert_eq!(hub.seen().len(), 1, "asked once");

    let flaky = Scripted::new(|_| Say::status(503)).await;
    let error = flaky.hub().whoami().await.expect_err("503 every time");
    assert!(matches!(error, Error::Hub { status: 503, .. }));
    assert_eq!(flaky.seen().len(), 3, "the configured three attempts");
}

#[tokio::test]
async fn a_rate_limit_is_a_failure_worth_waiting_out() {
    let hub = Scripted::new(|_| Say::status(429)).await;
    assert!(hub.hub().whoami().await.is_err());
    assert_eq!(hub.seen().len(), 3, "429 is the one 4xx that is retried");
}

/// The real hub, which is the only thing that can tell us the scripted one is
/// scripted wrong.
///
/// Ignored by default and run by hand, because it needs a token and it talks
/// to somebody else's servers. Doc 12.2 makes measuring real upload throughput
/// from each box a milestone 1 gate, and this is the shape of that
/// measurement, run as:
///
/// ```text
/// UMI_HUB_PROBE=open-index/umi-bench cargo test -p umi-publish -- --ignored --nocapture
/// ```
///
/// It needs `HF_TOKEN` in the environment. The probe repository is a scratch
/// one on purpose: doc 12.10 says a published dataset repository is never
/// deleted, and a repository full of timing runs is not a published dataset.
#[tokio::test]
#[ignore = "talks to huggingface.co and needs a token"]
async fn the_real_hub_answers_the_way_the_scripted_one_does() {
    let Ok(token) = std::env::var("HF_TOKEN") else {
        panic!("HF_TOKEN is not set");
    };
    let hub = Hub::new(token).expect("the client builds");

    let who = hub.whoami().await.expect("whoami");
    println!(
        "token belongs to {} in {:?}, write {}",
        who.name, who.orgs, who.write
    );
    assert!(who.write, "a read only token cannot publish");

    let Ok(repo) = std::env::var("UMI_HUB_PROBE") else {
        println!("UMI_HUB_PROBE is not set, so the upload half did not run");
        return;
    };

    hub.ensure_dataset(&repo).await.expect("the repository");

    // 8 MiB rather than doc 12.3's 128 MB, because the number this measures is
    // megabytes a second and a sixteenth of the transfer measures it just as
    // well while costing a sixteenth of somebody else's bandwidth.
    let dir = tempfile::tempdir().expect("tempdir");
    let (file, bytes) = blob(dir.path(), "probe.parquet", 8 << 20);
    let Upload::Blob { local, .. } = &file else {
        unreachable!()
    };
    let local = local.clone();

    let started = std::time::Instant::now();
    let commit = hub
        .upload(&repo, &[file], "throughput probe")
        .await
        .expect("upload");
    let took = started.elapsed();
    let rate = bytes.len() as f64 / took.as_secs_f64() / (1 << 20) as f64;
    println!(
        "8 MiB in {took:?}, {rate:.1} MB/s, deduplicated {}",
        commit.deduplicated
    );
    println!(
        "a 128 MB segment at that rate is {:.1}s against doc 12.2's 13s budget",
        128.0 / rate
    );

    // And doc 12.7's second condition, against the real thing: a fresh read of
    // bytes we did not keep a connection to, digested here rather than trusted
    // from a header.
    let path = commit.paths[0].clone();
    let back = hub
        .read_range(&repo, &path, 1 << 20, 1 << 20)
        .await
        .expect("a range");
    assert_eq!(back, bytes[1 << 20..2 << 20], "the hub stored what we sent");
    drop(local);

    let listed = hub.list(&repo, "").await.expect("list");
    let found = listed
        .iter()
        .find(|remote| remote.path == path)
        .expect("the file we just uploaded");
    assert_eq!(found.size, bytes.len() as u64, "doc 12.7's first condition");
}

#[test]
fn the_retry_ladder_stays_inside_doc_12_6s_ten_minutes() {
    let retry = Retry::default();
    assert_eq!(retry.attempts, 6);
    let total: Duration = (2..=retry.attempts).map(|next| retry.wait(next)).sum();
    assert!(
        total < Duration::from_secs(600),
        "six attempts inside ten minutes, got {total:?}"
    );
    // And it is not so eager that it hammers a hub that is down.
    let longest = (2..=retry.attempts)
        .map(|next| retry.wait(next))
        .max()
        .expect("waits");
    assert!(longest > Duration::from_secs(1), "got {longest:?}");
}

#[test]
fn two_publishers_do_not_back_off_on_the_same_schedule() {
    // The whole point of jitter, and the reason the seed is a coordinator
    // identifier rather than a constant.
    let one = Retry {
        seed: 1,
        ..Retry::default()
    };
    let two = Retry {
        seed: 2,
        ..Retry::default()
    };
    assert!((2..=6).any(|next| one.wait(next) != two.wait(next)));
    // And the same seed is the same schedule every time, which is what makes
    // a log readable a week later.
    assert_eq!(
        one.wait(4),
        Retry {
            seed: 1,
            ..Retry::default()
        }
        .wait(4)
    );
}

#[test]
fn the_small_pieces_do_what_they_say() {
    assert_eq!(bare("sha256:9c11"), "9c11");
    assert_eq!(bare("9c11"), "9c11");
    assert_eq!(
        split("open-index/umi-pages-2026w34-00").expect("split"),
        ("open-index", "umi-pages-2026w34-00")
    );
    assert!(split("umi-pages").is_err());

    assert!(retryable(429) && retryable(500) && retryable(503));
    assert!(!retryable(401) && !retryable(403) && !retryable(404) && !retryable(409));

    assert_eq!(message(br#"{"error":"nope"}"#), "nope");
    assert_eq!(message(b"<html>gateway</html>"), "<html>gateway</html>");
    assert_eq!(
        message(&vec![b'x'; 4096]).len(),
        200,
        "an error that pastes a kilobyte of markup into a log is one nobody reads"
    );

    assert_ne!(scatter(1, 2), scatter(1, 3));
    assert_eq!(scatter(1, 2), scatter(1, 2));
}
