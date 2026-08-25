//! Doc 13.7's contract, checked both halves.

use std::io::Write;

use super::{Error, Limits, Source, seed};

/// Collect a whole stream, keeping the error if there was one.
fn drain(source: Source, limits: Limits) -> (Vec<String>, super::Stats, Option<Error>) {
    let mut stream = seed(source, limits).expect("start");
    let mut urls = Vec::new();
    let mut failure = None;
    for item in &mut stream {
        match item {
            Ok(seed) => urls.push(seed.url),
            Err(error) => failure = Some(error),
        }
    }
    (urls, stream.stats(), failure)
}

/// A file of URLs, which is the source with no process in it.
fn file(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("seeds.txt");
    let mut handle = std::fs::File::create(&path).expect("create");
    handle.write_all(body.as_bytes()).expect("write");
    (dir, path)
}

#[test]
fn a_seeder_is_a_program_that_prints_urls() {
    let (urls, stats, failure) = drain(
        Source::shell("printf 'https://example.com/a\\nhttps://example.com/b\\n'"),
        Limits::default(),
    );
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls, ["https://example.com/a", "https://example.com/b"]);
    assert_eq!(stats.accepted, 2);
    assert_eq!(stats.lines, 2);
}

#[test]
fn the_argv_form_runs_the_program_without_a_shell() {
    // The argument has a shell metacharacter in it. Under `sh -c` it would be
    // a wildcard, and the point of the argv form is that it is not.
    let (urls, _, failure) = drain(
        Source::command(["echo", "https://example.com/a*b"]),
        Limits::default(),
    );
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls, ["https://example.com/a*b"]);
}

#[test]
fn a_non_zero_exit_is_a_failure_and_not_an_empty_frontier() {
    // The seeder prints two good URLs and then dies. Both URLs are still
    // delivered, because they are real candidates, and the run still fails,
    // because a partial list presented as a complete one is how a crawl
    // silently gets smaller.
    let (urls, _, failure) = drain(
        Source::shell("printf 'https://example.com/a\\nhttps://example.com/b\\n'; exit 3"),
        Limits::default(),
    );
    assert_eq!(urls.len(), 2);
    match failure {
        Some(Error::Failed { status, .. }) => assert_eq!(status, "exited with status 3"),
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[test]
fn a_failing_seeder_quotes_its_own_standard_error() {
    let (_, _, failure) = drain(
        Source::shell("echo 'the api key expired' >&2; exit 1"),
        Limits::default(),
    );
    match failure {
        Some(error) => {
            let text = error.to_string();
            assert!(text.contains("the api key expired"), "{text}");
            assert!(text.contains("exited with status 1"), "{text}");
        }
        None => panic!("expected a failure"),
    }
}

#[test]
fn a_seeder_that_cannot_be_started_says_so_before_anything_is_read() {
    let error = seed(
        Source::command(["umi-no-such-seeder-exists", "list"]),
        Limits::default(),
    )
    .err()
    .expect("expected a spawn failure");
    assert!(matches!(error, Error::Spawn { .. }), "{error:?}");
}

#[test]
fn urls_are_canonicalised_and_deduplicated() {
    // The same page four ways: a default port, an uppercase host, a tracking
    // parameter and a fragment. Doc 11.2 says these are one URL.
    let (dir, path) = file(
        "https://example.com:443/a\n\
         https://EXAMPLE.com/a\n\
         https://example.com/a?utm_source=x\n\
         https://example.com/a#section\n",
    );
    let (urls, stats, failure) = drain(Source::File(path), Limits::default());
    drop(dir);
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls, ["https://example.com/a"]);
    assert_eq!(stats.accepted, 1);
    assert_eq!(stats.duplicate, 3);
}

#[test]
fn blanks_and_comments_are_skipped_and_junk_is_rejected() {
    let (dir, path) = file(
        "# a seed list somebody edits by hand\n\
         \n\
         https://example.com/a\n\
         mailto:someone@example.com\n\
         ftp://example.com/a\n\
         /just/a/path\n\
         not a url at all\n",
    );
    let (urls, stats, failure) = drain(Source::File(path), Limits::default());
    drop(dir);
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls, ["https://example.com/a"]);
    assert_eq!(stats.skipped, 2, "the comment and the blank line");
    assert_eq!(stats.rejected, 4, "not http or https, or not absolute");
    // The reason breakdown, because "4 rejected" tells an operator that the
    // seed list is wrong and not which of two different bugs it has.
    assert_eq!(stats.why.not_http, 2, "the mailto and the ftp URL");
    assert_eq!(stats.why.malformed, 2, "the bare path and the sentence");
    assert_eq!(
        stats.why.worst(),
        Some((umi_types::CanonError::NotHttp, 2)),
        "a tie goes to the first reason listed, which is the common one"
    );
    assert!(
        stats
            .to_string()
            .contains("mostly not an http or https url")
    );
}

#[test]
fn an_enormous_line_is_skipped_rather_than_read_into_memory() {
    // Half a megabyte with no newline in it, between two real URLs. The cap is
    // 8 KiB, so the middle line never exists as a string and the two around it
    // still arrive.
    let mut body = String::from("https://example.com/a\n");
    body.push_str(&"x".repeat(512 * 1024));
    body.push_str("\nhttps://example.com/b\n");
    let (dir, path) = file(&body);
    let (urls, stats, failure) = drain(Source::File(path), Limits::default());
    drop(dir);
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls, ["https://example.com/a", "https://example.com/b"]);
    assert_eq!(stats.too_long, 1);
    assert_eq!(stats.lines, 3);
}

#[test]
fn a_line_that_is_exactly_the_cap_still_arrives() {
    // The boundary, because an off by one here silently drops the longest URLs
    // a site has and nothing about the crawl looks wrong afterwards.
    let limits = Limits {
        max_line: 40,
        ..Limits::default()
    };
    let padded = format!("https://example.com/{}", "a".repeat(20));
    assert_eq!(padded.len(), 40);
    let (dir, path) = file(&format!("{padded}\n{padded}b\n"));
    let (urls, stats, failure) = drain(Source::File(path), limits);
    drop(dir);
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls, [padded]);
    assert_eq!(stats.too_long, 1, "one byte over is one byte too many");
}

#[test]
fn a_line_that_is_not_utf8_is_counted_rather_than_guessed_at() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("seeds.txt");
    let mut handle = std::fs::File::create(&path).expect("create");
    handle
        .write_all(b"https://example.com/a\n\xff\xfe\nhttps://example.com/b\n")
        .expect("write");
    drop(handle);
    let (urls, stats, failure) = drain(Source::File(path), Limits::default());
    drop(dir);
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls.len(), 2);
    assert_eq!(stats.not_utf8, 1);
}

#[test]
fn the_last_line_counts_even_without_a_newline() {
    let (dir, path) = file("https://example.com/a\nhttps://example.com/b");
    let (urls, _, failure) = drain(Source::File(path), Limits::default());
    drop(dir);
    assert!(failure.is_none(), "{failure:?}");
    assert_eq!(urls, ["https://example.com/a", "https://example.com/b"]);
}

#[test]
fn deduplication_stops_at_the_cap_rather_than_growing() {
    let limits = Limits {
        max_seen: 2,
        ..Limits::default()
    };
    let (dir, path) = file(
        "https://example.com/a\n\
         https://example.com/b\n\
         https://example.com/c\n\
         https://example.com/a\n",
    );
    let (urls, stats, failure) = drain(Source::File(path), limits);
    drop(dir);
    assert!(failure.is_none(), "{failure:?}");
    // The fourth line is a repeat of the first, and it comes through because
    // the set gave up. Doc 08's seen set catches it at admission.
    assert_eq!(urls.len(), 4);
    assert!(stats.undeduplicated);
    assert_eq!(stats.duplicate, 0);
}

#[test]
fn dropping_the_stream_early_kills_the_seeder() {
    // A seeder that never ends on its own. If dropping the stream left it
    // running, this test would hang on the temporary directory or leak a
    // process for the rest of the suite.
    let mut stream = seed(
        Source::shell("while :; do echo https://example.com/a; done"),
        Limits::default(),
    )
    .expect("start");
    let first = stream.next().expect("one url").expect("a url");
    assert_eq!(first.url, "https://example.com/a");
    drop(stream);
}

#[test]
fn the_keys_are_the_ones_admission_would_derive() {
    // The whole point of canonicalising here is that the key matches. If these
    // two ever disagree, seeding puts rows in the frontier that the seen set
    // cannot find and every seed is crawled twice.
    let (dir, path) = file("https://EXAMPLE.com:443/a?utm_source=x\n");
    let mut stream = seed(Source::File(path), Limits::default()).expect("start");
    let seeded = stream.next().expect("one url").expect("a url");
    drop(stream);
    drop(dir);
    let direct = umi_types::RowKey::for_url("https://EXAMPLE.com:443/a?utm_source=x", None)
        .expect("canonicalise");
    assert_eq!(seeded.keys, direct);
}

#[test]
fn stdin_is_a_source_like_any_other() {
    // Not read here, because a test harness has no stdin worth reading, but
    // opening it must not fail and the label must be usable in an error.
    let stream = seed(Source::Stdin, Limits::default()).expect("start");
    assert_eq!(stream.stats().lines, 0);
}
