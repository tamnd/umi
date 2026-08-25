//! Tests for the parts of the command line that have a right answer.
//!
//! Issue #14 asks for the config precedence and the exit codes to be covered,
//! and those two are here in full because they are the parts a person hits at
//! 2am. The `ls` and `cat` tests run against a real sealed segment and a real
//! Parquet file converted from it, rather than a fixture, so that a change to
//! doc 10's layout or doc 12's writer properties breaks them.

use std::path::{Path, PathBuf};

use umi_file::sample::T0;
use umi_file::{Create, SegmentWriter, StreamKind, WriterConfig, sample};
use umi_types::Exit;

use crate::config::{Config, Env, Error as ConfigError, Flags, Origin, Paths, Secret};
use crate::{Error, inspect};

/// A `Paths` pointing at a temporary directory, so no test can be affected by
/// the developer's own `~/.config/umi/config.toml`.
fn paths(dir: &Path) -> Paths {
    Paths {
        local: dir.join("umi.toml"),
        user: Some(dir.join("home").join("config.toml")),
    }
}

fn write(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

fn env(pairs: &[(&str, &str)]) -> Env {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn defaults_when_nothing_says_otherwise() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::load(&paths(dir.path()), &Env::new(), &Flags::default()).unwrap();

    assert_eq!(config.rps.value, 1.0);
    assert_eq!(config.concurrency.value, 4);
    assert_eq!(config.tier_max.value, 3);
    assert_eq!(config.backend.value, "sqlite");
    assert_eq!(config.org.value, "open-index");
    assert_eq!(config.rps.origin, Origin::Default);
    assert!(config.token.is_none());
    assert!(config.files.is_empty());
}

#[test]
fn the_user_file_loses_to_the_local_file() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    write(&paths.local, "[crawl]\nrps = 3.0\n");
    write(
        paths.user.as_ref().unwrap(),
        "[crawl]\nrps = 9.0\nconcurrency = 32\n",
    );

    let config = Config::load(&paths, &Env::new(), &Flags::default()).unwrap();

    assert_eq!(config.rps.value, 3.0);
    assert_eq!(config.rps.origin, Origin::File(paths.local.clone()));
    // The setting the local file did not mention still comes from the user
    // file, which is the whole point of layering rather than picking one file.
    assert_eq!(config.concurrency.value, 32);
    assert_eq!(
        config.concurrency.origin,
        Origin::File(paths.user.clone().unwrap())
    );
    assert_eq!(config.files.len(), 2);
}

#[test]
fn the_environment_beats_both_files() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    write(&paths.local, "[crawl]\nrps = 3.0\n");

    let config = Config::load(&paths, &env(&[("UMI_RPS", "7.5")]), &Flags::default()).unwrap();

    assert_eq!(config.rps.value, 7.5);
    assert_eq!(config.rps.origin, Origin::Env("UMI_RPS".to_owned()));
}

#[test]
fn a_flag_beats_everything() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    write(&paths.local, "[crawl]\nrps = 3.0\n");

    let flags = Flags {
        rps: Some(0.5),
        ..Flags::default()
    };
    let config = Config::load(&paths, &env(&[("UMI_RPS", "7.5")]), &flags).unwrap();

    assert_eq!(config.rps.value, 0.5);
    assert_eq!(config.rps.origin, Origin::Flag);
}

#[test]
fn all_five_layers_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    write(&paths.local, "[crawl]\ntier_max = 2\n");
    write(
        paths.user.as_ref().unwrap(),
        "[state]\nbackend = \"nami\"\n",
    );

    let flags = Flags {
        rps: Some(0.25),
        ..Flags::default()
    };
    let config = Config::load(&paths, &env(&[("UMI_CONCURRENCY", "12")]), &flags).unwrap();

    assert_eq!((config.rps.value, config.rps.origin), (0.25, Origin::Flag));
    assert_eq!(config.concurrency.value, 12);
    assert_eq!(config.tier_max.value, 2);
    assert_eq!(config.backend.value, "nami");
    // And the layer nobody set still answers.
    assert_eq!(config.org.value, "open-index");
    assert_eq!(config.org.origin, Origin::Default);
}

#[test]
fn a_missing_file_is_not_an_error_and_a_broken_one_is() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    assert!(Config::load(&paths, &Env::new(), &Flags::default()).is_ok());

    write(&paths.local, "[crawl]\nrps = \"fast\"\n");
    let failed = Config::load(&paths, &Env::new(), &Flags::default()).unwrap_err();
    assert!(matches!(failed, ConfigError::Parse(_, _)));
    assert_eq!(Error::from(failed).exit(), Exit::Usage);
}

#[test]
fn an_unknown_key_is_a_typo_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    // `deny_unknown_fields`, because silently ignoring `[crawl] rsp = 50` is
    // the failure mode this whole module exists to prevent.
    write(&paths.local, "[crawl]\nrsp = 50\n");
    assert!(matches!(
        Config::load(&paths, &Env::new(), &Flags::default()).unwrap_err(),
        ConfigError::Parse(_, _)
    ));
}

#[test]
fn a_bad_environment_value_names_the_variable() {
    let dir = tempfile::tempdir().unwrap();
    let failed = Config::load(
        &paths(dir.path()),
        &env(&[("UMI_CONCURRENCY", "lots")]),
        &Flags::default(),
    )
    .unwrap_err();
    let message = failed.to_string();
    assert!(message.contains("UMI_CONCURRENCY"), "{message}");
    assert!(message.contains("lots"), "{message}");
}

#[test]
fn secrets_stay_indirect() {
    assert_eq!(
        Secret::parse("env:HF_TOKEN"),
        Secret::Env("HF_TOKEN".to_owned())
    );
    assert_eq!(
        Secret::parse("file:/etc/umi/token"),
        Secret::File(PathBuf::from("/etc/umi/token"))
    );
    assert_eq!(
        Secret::parse("hf_realtoken"),
        Secret::Literal("hf_realtoken".to_owned())
    );

    assert!(Secret::parse("env:HF_TOKEN").warning().is_none());
    assert!(Secret::parse("file:/etc/umi/token").warning().is_none());
    // Accepted, because refusing it would break somebody's crawl on an
    // upgrade, and warned about on every run until it is fixed.
    assert!(Secret::parse("hf_realtoken").warning().is_some());
}

#[test]
fn a_secret_in_a_file_is_read_without_its_newline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    write(&path, "hf_abc123\n");
    let secret = Secret::parse(&format!("file:{}", path.display()));
    assert_eq!(secret.read().unwrap(), "hf_abc123");
}

#[test]
fn a_secret_that_cannot_be_read_does_not_leak_where_it_pointed() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope");
    let failed = Secret::parse(&format!("file:{}", missing.display()))
        .read()
        .unwrap_err();
    // The path is in the message and no value is, because there is no value.
    assert!(failed.to_string().contains("nope"));
}

#[test]
fn the_token_comes_out_of_the_layers_as_a_secret() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    write(&paths.local, "[publish]\ntoken = \"env:HF_TOKEN\"\n");

    let config = Config::load(&paths, &Env::new(), &Flags::default()).unwrap();
    let token = config.token.unwrap();
    assert_eq!(token.value, Secret::Env("HF_TOKEN".to_owned()));
    assert_eq!(token.origin, Origin::File(paths.local));
}

#[test]
fn a_token_in_the_environment_is_not_a_literal_in_a_config_file() {
    // Doc 14.7 names UMI_TOKEN as a place a token comes from and warns about
    // literals in config files. Warning about the first because it looks like
    // the second tells somebody who did the right thing to do the right thing.
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(dir.path());
    let config = Config::load(
        &paths,
        &env(&[("UMI_TOKEN", "hf_realtoken")]),
        &Flags::default(),
    )
    .unwrap();
    let token = config.token.unwrap();
    assert_eq!(token.value, Secret::Env("UMI_TOKEN".to_owned()));
    assert!(token.value.warning().is_none());
    assert_eq!(token.origin, Origin::Env("UMI_TOKEN".to_owned()));

    // And the indirection still works when it is spelled out, so that a
    // variable can point at another one.
    let config = Config::load(
        &paths,
        &env(&[("UMI_TOKEN", "env:HF_TOKEN")]),
        &Flags::default(),
    )
    .unwrap();
    assert_eq!(
        config.token.unwrap().value,
        Secret::Env("HF_TOKEN".to_owned())
    );
}

#[test]
fn every_error_has_the_exit_code_doc_14_9_gives_it() {
    let cases: [(Error, Exit); 15] = [
        (Error::NoColumn("body".to_owned()), Exit::Usage),
        (Error::BadUrl("not a url".to_owned()), Exit::Usage),
        (Error::Missing("publish.token".to_owned()), Exit::Usage),
        (Error::Empty, Exit::NothingToDo),
        (Error::Fetch("connection reset".to_owned()), Exit::Network),
        (Error::Unreadable(3), Exit::Verification),
        (Error::NotReady, Exit::Resource),
        (Error::NotBuilt("milestone 2"), Exit::Failure),
        (Error::Io(std::io::Error::other("disk")), Exit::Failure),
        // The publishing cases, which are the reason `Error::Publish` keeps
        // the cause instead of flattening it to a string. A hub that timed out
        // is worth retrying, and a copy that came back short, digested
        // differently or held the wrong number of rows is not.
        (
            Error::Publish(umi_publish::Error::Transport {
                what: "uploading",
                cause: "timed out".to_owned(),
            }),
            Exit::Network,
        ),
        (
            Error::Publish(umi_publish::Error::Hub {
                status: 503,
                what: "uploading",
                body: "busy".to_owned(),
            }),
            Exit::Network,
        ),
        (
            Error::Publish(umi_publish::Error::NotPublished(
                umi_publish::Blocked::DigestMismatch,
            )),
            Exit::Verification,
        ),
        (
            Error::Publish(umi_publish::Error::NotPublished(
                umi_publish::Blocked::RemoteSize {
                    expected: 1024,
                    found: 1023,
                },
            )),
            Exit::Verification,
        ),
        (
            Error::Publish(umi_publish::Error::RowCount {
                expected: 65,
                found: 64,
            }),
            Exit::Verification,
        ),
        (
            Error::Publish(umi_publish::Error::Secret("expected env:NAME")),
            Exit::Failure,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(error.exit(), expected, "{error}");
    }
}

#[test]
fn nothing_to_do_and_failure_are_not_the_same_code() {
    // Doc 14.9 is explicit that a script has to tell "finished, nothing to
    // crawl" from "stopped early". If these ever collapse the codes are a lie.
    assert_ne!(Error::Empty.exit(), Error::NotReady.exit());
    assert_ne!(Exit::NothingToDo as u8, Exit::BudgetExhausted as u8);
}

/// Seal a segment of sample page rows and hand back its path.
fn segment(dir: &Path, rows: usize) -> PathBuf {
    let path = dir.join("pages.umi");
    let create = Create {
        stream: StreamKind::Pages,
        segment_id: [3u8; 16],
        coordinator: [4u8; 32],
        created_ms: T0,
        canon_version: 1,
        extractor_version: 4,
        crawl_profile: 0,
    };
    let mut writer =
        SegmentWriter::create(&path, create, WriterConfig::for_memory(64 << 20)).unwrap();
    writer
        .push(&sample::batch(StreamKind::Pages, rows))
        .unwrap();
    writer.seal().unwrap();
    path
}

#[test]
fn ls_finds_a_segment_and_counts_its_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = segment(dir.path(), 500);
    assert!(path.exists());
    ls_local(dir.path()).unwrap();
}

#[test]
fn ls_on_an_empty_directory_is_exit_3_and_not_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let failed = ls_local(dir.path()).unwrap_err();
    assert_eq!(failed.exit(), Exit::NothingToDo);
}

#[test]
fn cat_writes_one_json_object_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = segment(dir.path(), 40);
    let out = capture(|sink| cat_to(&path, Some(10), None, sink));
    assert_eq!(out.lines().count(), 10);
    for line in out.lines() {
        assert!(line.starts_with('{') && line.ends_with('}'), "{line}");
    }
}

#[test]
fn cat_projects_the_columns_it_was_given() {
    let dir = tempfile::tempdir().unwrap();
    let path = segment(dir.path(), 8);
    let out = capture(|sink| cat_to(&path, Some(1), Some(&["url"]), sink));
    let row: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
    assert_eq!(row.as_object().unwrap().len(), 1);
    assert!(row.get("url").is_some(), "{row}");
}

#[test]
fn cat_on_a_column_that_is_not_there_is_a_usage_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = segment(dir.path(), 4);
    let failed = capture_err(|sink| cat_to(&path, None, Some(&["nonesuch"]), sink));
    assert!(matches!(failed, Error::NoColumn(_)));
    assert_eq!(failed.exit(), Exit::Usage);
}

#[test]
fn a_converted_parquet_file_reads_back_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = segment(dir.path(), 64);
    let parquet = dir.path().join("pages.parquet");
    let segment = umi_file::Segment::open(&path).unwrap();
    let converted = umi_publish::convert(&segment, &parquet).unwrap();
    assert_eq!(converted.rows, 64);

    // The same command, the same flags, the other file kind. A consumer who
    // learned `umi cat` on a local segment should not have to learn it again
    // on a published file.
    let from_umi = capture(|sink| cat_to(&path, Some(3), Some(&["url"]), sink));
    let from_parquet = capture(|sink| cat_to(&parquet, Some(3), Some(&["url"]), sink));
    assert_eq!(from_umi, from_parquet);

    ls_local(dir.path()).unwrap();
}

/// `umi ls` against a path, with no token and the default organisation, which
/// is every local case.
fn ls_local(dir: &Path) -> Result<(), Error> {
    inspect::ls(&inspect::Ls {
        target: &dir.display().to_string(),
        token: None,
        org: "open-index",
    })
}

/// `cat` writes to stdout, so the tests reach one layer under it. The
/// alternative is redirecting the process's stdout, which races every other
/// test in the binary.
fn cat_to(
    path: &Path,
    limit: Option<u64>,
    columns: Option<&[&str]>,
    sink: &mut Vec<u8>,
) -> Result<(), Error> {
    inspect::cat_into(path, columns, limit.unwrap_or(u64::MAX), sink)
}

fn capture(body: impl FnOnce(&mut Vec<u8>) -> Result<(), Error>) -> String {
    let mut sink = Vec::new();
    body(&mut sink).unwrap();
    String::from_utf8(sink).unwrap()
}

fn capture_err(body: impl FnOnce(&mut Vec<u8>) -> Result<(), Error>) -> Error {
    let mut sink = Vec::new();
    body(&mut sink).unwrap_err()
}
