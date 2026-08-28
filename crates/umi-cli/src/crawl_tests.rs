//! The parts of `umi crawl` that have a right answer without a network.
//!
//! The loop itself is tested in `umi-crawl` against a canned fetcher, so what
//! is left here is the wiring, and the wiring is where a focused crawl goes
//! wrong quietly: a scope that came out wider than the flags said, a profile
//! that does not read back the same, a budget that never fires. All three of
//! those produce a crawl that looks like it worked.

use std::time::Duration;

use super::{
    Backoff, HEARTBEAT, Heartbeat, IDLE_WAIT, Layout, Log, Options, Publishing, Settings, Stall,
    Stop, Summary, WATCH_MAX_WAIT, adopt, day, default_out, delay_ms, profile_toml, scope_for,
    seed_url, settings, sources, span, spent, tier,
};
use crate::config::{Config, Flags, Paths};

fn options(target: &str) -> Options {
    Options {
        target: target.to_owned(),
        ..Options::default()
    }
}

#[test]
fn a_domain_target_becomes_a_host_suffix_scope() {
    let scope = scope_for(&options("example.com")).expect("scope");
    assert!(scope.allows("https://example.com/"));
    assert!(scope.allows("https://docs.example.com/a"));
    assert!(!scope.allows("https://notexample.com/"));
    assert!(!scope.allows("https://elsewhere.test/"));
}

#[test]
fn a_path_target_stays_on_that_path() {
    let scope = scope_for(&options("https://example.com/blog/")).expect("scope");
    assert!(scope.allows("https://example.com/blog/post"));
    assert!(!scope.allows("https://example.com/shop"));
}

#[test]
fn an_exclude_flag_beats_the_target_it_was_added_to() {
    let mut options = options("example.com");
    options.exclude = vec!["shop.example.com".to_owned()];
    let scope = scope_for(&options).expect("scope");
    assert!(scope.allows("https://docs.example.com/a"));
    assert!(
        !scope.allows("https://shop.example.com/a"),
        "exclude wins, which is doc 13.2's rule and the only safe order"
    );
}

#[test]
fn adding_a_matcher_changes_the_profile_id() {
    // Doc 10.5 stamps every row with `crawl_profile`, so two scopes that
    // admitted different pages must not carry the same number or a published
    // dataset cannot say which crawl a row came from.
    let plain = scope_for(&options("example.com")).expect("scope");
    let mut wider = options("example.com");
    wider.include = vec!["example.org".to_owned()];
    let wider = scope_for(&wider).expect("scope");
    assert_ne!(plain.id, wider.id);
    assert!(wider.allows("https://example.org/"));
}

#[test]
fn a_profile_reads_back_as_the_scope_that_wrote_it() {
    // This is doc 13.5's portability claim in one assertion. `tar` the
    // directory, `umi resume` it somewhere else, and the crawl has to stay
    // inside the same scope it started in.
    let mut options = options("example.com");
    options.include = vec!["example.org".to_owned(), "docs.example.net".to_owned()];
    options.exclude = vec!["shop.example.com".to_owned(), "ads.example.org".to_owned()];
    options.depth = Some(3);
    options.links = "one-hop".to_owned();
    options.max_pages = Some(500);
    options.max_duration = Some("30m".to_owned());
    let scope = scope_for(&options).expect("scope");

    let text = profile_toml(&options, &scope);
    let read = umi_crawl::Scope::from_toml(&text).expect("the profile we just wrote");

    assert_eq!(read.max_depth, Some(3));
    assert_eq!(read.budget.max_pages, Some(500));
    assert_eq!(read.budget.max_duration, Some(Duration::from_secs(1800)));
    assert!(read.allows("https://docs.example.com/a"));
    assert!(
        read.allows("https://example.org/a"),
        "the second include too"
    );
    assert!(!read.allows("https://shop.example.com/a"));
    assert!(
        !read.allows("https://ads.example.org/a"),
        "and the second exclude"
    );
    assert!(!read.allows("https://elsewhere.test/"));
}

#[test]
fn an_output_directory_never_leaves_the_place_it_was_asked_for() {
    // A scope name comes from a target, and a target is a string the operator
    // typed. Nothing that reaches `PathBuf` from here should be able to name a
    // parent directory.
    assert_eq!(default_out("example.com"), "./example.com");
    assert_eq!(default_out("example.com-blog"), "./example.com-blog");
    assert_eq!(default_out("../../etc"), "./..-..-etc");
    assert_eq!(
        default_out(".."),
        "./crawl",
        "the two names that mean something to a filesystem never survive"
    );
    assert_eq!(default_out("."), "./crawl");
    assert_eq!(default_out(""), "./crawl");
}

#[test]
fn the_rate_is_clamped_to_doc_13_3s_ceiling() {
    assert_eq!(delay_ms(1.0), 1000, "doc 14.3's default is one a second");
    assert_eq!(delay_ms(2.0), 500);
    assert_eq!(
        delay_ms(50.0),
        500,
        "a focused crawl cannot ask for more than 2 a second"
    );
    assert_eq!(delay_ms(0.5), 2000, "and it can always ask for less");
    assert_eq!(delay_ms(0.0), 100_000, "including nothing at all");
}

#[test]
fn tier_numbers_map_onto_the_ladder_and_stop_at_the_top_of_it() {
    assert_eq!(tier(1), umi_types::Tier::Plain);
    assert_eq!(tier(2), umi_types::Tier::Emulated);
    assert_eq!(tier(3), umi_types::Tier::Rendered);
    assert_eq!(
        tier(9),
        umi_types::Tier::Rendered,
        "tier 4 is opt in and allowlisted, never reached by typing a number"
    );
}

fn with_budget(pages: Option<u64>, seconds: Option<u64>) -> Settings {
    let mut options = Options {
        max_pages: pages,
        max_duration: seconds.map(|s| format!("{s}s")),
        ..Options::default()
    };
    options.target = "example.com".to_owned();
    settings(&options).expect("settings")
}

#[test]
fn a_page_budget_stops_the_crawl_and_no_budget_does_not() {
    let settings = with_budget(Some(100), None);
    let mut summary = Summary {
        rows: 99,
        ..Summary::default()
    };
    assert_eq!(spent(&summary, &settings, 0, 1000), None);
    summary.rows = 100;
    assert_eq!(spent(&summary, &settings, 0, 1000), Some(Stop::Budget));

    let forever = with_budget(None, None);
    summary.rows = 10_000_000;
    assert_eq!(spent(&summary, &forever, 0, 1000), None);
}

#[test]
fn a_wall_clock_budget_stops_the_crawl_on_the_clock_it_was_given() {
    let settings = with_budget(None, Some(30));
    let summary = Summary::default();
    assert_eq!(spent(&summary, &settings, 1000, 30_999), None);
    assert_eq!(
        spent(&summary, &settings, 1000, 31_000),
        Some(Stop::Budget),
        "measured against the start, not against a clock read here"
    );
}

#[test]
fn a_frontier_that_is_only_waiting_its_turn_is_not_a_stalled_one() {
    // This is the bug the loop had on its first live run: it broke on the
    // first idle tick and finished a 25 page crawl after 5 pages, because one
    // host inside doc 07.6's politeness window leases nothing while hundreds of
    // its urls are still queued. Nothing here may call that stuck.
    let mut stall = Stall::default();
    let mut at = 1_000;
    for pending in (100..400).rev() {
        assert!(
            !stall.stuck(pending, at),
            "the count is falling, so pages are being fetched"
        );
        at += 1_500;
    }
    // Well past the limit in wall clock terms, and still moving.
    assert!(at > 300_000);
}

#[test]
fn a_frontier_that_never_moves_gives_up_eventually() {
    // A lease left behind by a killed process, or a host whose robots.txt will
    // not fetch, leaves rows pending that no tick can pick up. Without this the
    // loop waits on them forever.
    let mut stall = Stall::default();
    assert!(!stall.stuck(7, 0), "the first sighting is never a stall");
    assert!(!stall.stuck(7, 299_999));
    assert!(stall.stuck(7, 300_000), "five minutes of nothing at all");
}

#[test]
fn any_movement_at_all_resets_the_stall_clock() {
    let mut stall = Stall::default();
    assert!(!stall.stuck(7, 0));
    assert!(!stall.stuck(8, 299_999), "one url admitted is progress");
    assert!(
        !stall.stuck(8, 300_000),
        "and the five minutes now runs from there, not from the start"
    );
    assert!(stall.stuck(8, 599_999));
}

#[test]
fn a_watch_backs_off_to_a_minute_and_stays_there() {
    // The reason this exists is arithmetic. A fortnight at one second is 1.2
    // million lease queries and as many counter reads, all of them finding
    // nothing, and the counter read is the expensive one on a real frontier.
    let mut backoff = Backoff::default();
    assert_eq!(backoff.next(), IDLE_WAIT, "the first wait is the short one");
    let mut waits = vec![];
    for _ in 0..20 {
        waits.push(backoff.next());
    }
    assert_eq!(waits[0], IDLE_WAIT * 2);
    assert_eq!(waits[1], IDLE_WAIT * 4);
    assert_eq!(*waits.last().expect("waits"), WATCH_MAX_WAIT);
    assert!(
        waits.iter().all(|w| *w <= WATCH_MAX_WAIT),
        "the ceiling is what bounds how late a refresh can be"
    );
}

#[test]
fn one_leased_url_puts_the_wait_back_on_the_floor() {
    // A watch that has just found work is a watch that is about to find more,
    // because doc 09 schedules a domain's urls near each other in time. Coming
    // out of the long wait has to be immediate or the first refresh of the day
    // drags the rest of the day behind it a minute at a time.
    let mut backoff = Backoff::default();
    for _ in 0..20 {
        backoff.next();
    }
    backoff.reset();
    assert_eq!(backoff.next(), IDLE_WAIT);
}

#[test]
fn a_watch_says_something_immediately_and_then_on_the_heartbeat() {
    let mut heartbeat = Heartbeat::default();
    let every = HEARTBEAT.as_millis() as u64;
    assert!(
        heartbeat.due(5_000),
        "a watch that printed nothing for five minutes would look like a hung one"
    );
    assert!(!heartbeat.due(5_000 + every - 1));
    assert!(heartbeat.due(5_000 + every));
    assert!(!heartbeat.due(5_000 + every + 1));
}

#[test]
fn durations_in_the_log_are_readable_at_every_scale() {
    assert_eq!(span(0), "0s");
    assert_eq!(span(59_999), "59s");
    assert_eq!(span(60_000), "1m");
    assert_eq!(span(3_599_999), "59m");
    assert_eq!(span(3_600_000), "1h00m");
    assert_eq!(span(86_399_000), "23h59m");
    assert_eq!(span(86_400_000), "1d00h");
    // The number the whole command is for: doc 16's gate 2.5 is a watch left
    // running for a fortnight.
    assert_eq!(span(14 * 86_400_000 + 3_600_000), "14d01h");
}

#[test]
fn seed_sources_come_out_in_the_order_the_flags_went_in() {
    let options = Options {
        seed: Some("-".to_owned()),
        seeder: vec!["ccrawl-cli urls example.com".to_owned()],
        ..Options::default()
    };
    let sources = sources(&options);
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[0], umi_seed::Source::Stdin);
    assert!(matches!(sources[1], umi_seed::Source::Shell(_)));
}

#[test]
fn a_target_seeds_itself_and_a_profile_does_not() {
    assert_eq!(
        seed_url("example.com").as_deref(),
        Some("https://example.com/")
    );
    assert_eq!(
        seed_url("https://example.com/blog/").as_deref(),
        Some("https://example.com/blog/")
    );
    assert_eq!(
        seed_url("./rust-docs/profile.toml"),
        None,
        "a profile carries its own seeds and the path is not one of them"
    );
}

#[test]
fn the_manifest_day_is_the_day_the_crawl_started() {
    // 2026-08-25T00:00:00Z, and the second before it, so an off by one lands
    // on the wrong day rather than passing quietly.
    assert_eq!(day(1_787_616_000_000), "20260825");
    assert_eq!(day(1_787_615_999_000), "20260824");
    assert_eq!(day(0), "19700101");
}

#[test]
fn resuming_something_that_is_not_a_crawl_says_so() {
    let dir = tempfile::tempdir().expect("tempdir");
    let error = super::resume(dir.path(), false, None, None).expect_err("no profile.toml");
    assert!(
        format!("{error}").contains("is not a crawl directory"),
        "got {error}"
    );
    assert_eq!(error.exit(), umi_types::Exit::Failure);
}

#[test]
fn publishing_without_a_token_is_refused_before_anything_is_fetched() {
    // The dangerous version of this is a `--publish` that crawls happily and
    // silently publishes nothing, because the operator then deletes the
    // directory believing the data is somewhere else. So the check is at the
    // front, and it names the setting rather than saying "not configured".
    let config = config_with(&[]);
    let error = Publishing::resolve(&config, true).expect_err("no token");
    assert!(format!("{error}").contains("publish.token"), "got {error}");
    assert_eq!(error.exit(), umi_types::Exit::Usage);

    // A token and no key is the same answer about the other secret. Doc 12.5
    // keeps them separate and neither one on its own is enough to publish.
    let error = Publishing::resolve(&config_with(&[("UMI_TOKEN", "env:UMI_TEST_TOKEN")]), true)
        .expect_err("no key");
    assert!(format!("{error}").contains("publish.key"), "got {error}");
}

#[test]
fn a_token_that_points_at_nothing_is_refused_too() {
    // Configured, and the indirection does not resolve. A different failure
    // from not being configured at all, and the message has to say which,
    // because the fix is different.
    let config = config_with(&[
        ("UMI_TOKEN", "env:UMI_TEST_TOKEN_THAT_IS_NOT_SET"),
        ("UMI_PUBLISH_KEY", "env:UMI_TEST_KEY_THAT_IS_NOT_SET"),
    ]);
    let error = Publishing::resolve(&config, true).expect_err("unset");
    let shown = format!("{error}");
    assert!(
        shown.contains("UMI_TEST_TOKEN_THAT_IS_NOT_SET"),
        "got {shown}"
    );
}

#[test]
fn without_the_flag_no_secret_is_read_at_all() {
    // A machine with no token still crawls. `--publish` is the only thing that
    // makes the token a requirement, so resolving must not fail without it.
    assert!(
        Publishing::resolve(&config_with(&[]), false)
            .expect("no flag, no secrets")
            .is_none()
    );
}

#[test]
fn the_resolved_secrets_do_not_show_up_in_a_debug_line() {
    // `Options` derives `Debug` and gets logged. This is the guard that stops
    // that from being a token in somebody's crawl log.
    let publishing = Publishing {
        org: "open-index".to_owned(),
        token: "hf_the_actual_token".to_owned(),
        key: "2a".repeat(32),
        slice: 0,
    };
    let shown = format!(
        "{:?}",
        Options {
            publish: Some(publishing),
            ..options("example.com")
        }
    );
    assert!(!shown.contains("hf_the_actual_token"), "{shown}");
    assert!(!shown.contains("2a2a"), "{shown}");
    assert!(shown.contains("open-index"), "{shown}");
}

#[tokio::test]
async fn a_recorded_segment_is_one_the_publisher_will_pick_up() {
    // The handover between this file and doc 12.2's pipeline. Everything the
    // publisher needs to find a segment, open it and check it later has to be
    // in the row, and the digest has to be over the file rather than over
    // anything convenient.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("01K2M8Q0P7R3XN500000000000.umi");
    std::fs::write(&path, b"not a real segment, but a real file").expect("write");

    let sealed = [umi_crawl::Sealed {
        path: path.clone(),
        id: umi_types::Ulid::parse("01K2M8Q0P7R3XN500000000000").expect("parse"),
        stats: umi_file::SegmentStats {
            rows: 4096,
            ..umi_file::SegmentStats::default()
        },
    }];
    let state = umi_state::MemoryState::default();
    let state: &dyn umi_state::State = &state;
    super::record(&sealed, state, 1_787_616_000_000)
        .await
        .expect("record");

    let due = state
        .segments(umi_state::SegmentQuery::Unpublished)
        .await
        .expect("query");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, sealed[0].id);
    assert_eq!(due[0].local_path, path.to_string_lossy());
    assert_eq!(due[0].rows, 4096);
    assert_eq!(due[0].bytes, 35);
    assert_eq!(
        due[0].local_digest.as_bytes(),
        blake3::hash(b"not a real segment, but a real file").as_bytes()
    );
    // Not published and not deleted, which is what makes it due rather than
    // collectable. Getting this backwards would delete a segment nobody kept.
    assert!(due[0].remote.is_none());
    assert!(due[0].local());
}

#[test]
fn publishing_something_that_is_not_a_crawl_says_so() {
    // The same guard `umi resume` has, and it matters more here: this command
    // uploads, and the directory it is pointed at is the whole of what it
    // uploads. It also has to fail before the hub client is built, so that a
    // typo in a path is a message about the path rather than about a token.
    let dir = tempfile::tempdir().expect("tempdir");
    let error = super::publish(dir.path(), &secrets()).expect_err("no profile.toml");
    assert!(
        format!("{error}").contains("is not a crawl directory"),
        "got {error}"
    );
    // And it left the directory alone rather than making it look like a crawl.
    assert!(!dir.path().join("segments").exists());
}

#[tokio::test]
async fn adopting_a_directory_gives_every_local_file_a_ledger_row() {
    // What makes `umi publish <dir>` work at all. A crawl that ran without
    // `--publish` writes no segment rows, so the publisher would find nothing
    // to do unless this step puts the files it left behind into the ledger.
    let dir = tempfile::tempdir().expect("tempdir");
    let layout = Layout::create(dir.path()).expect("layout");
    let kept = "01K2M8Q0P7R3XN500000000000";
    let unconverted = "01K2M8Q0P7R3XN500000000001";

    // One of each: a Parquet file in `data/`, which is what doc 13.5's
    // directory normally holds, and a sealed `.umi` in `segments/`, which is
    // what a crawl that died between sealing and converting leaves.
    write_segment(&layout.segments.join(format!("{unconverted}.umi")), 300);
    let staging = write_segment(&dir.path().join("scratch.umi"), 700);
    umi_publish::convert(
        &umi_file::Segment::open(&staging).expect("open"),
        &layout.data.join(format!("{kept}.parquet")),
    )
    .expect("convert");
    std::fs::remove_file(&staging).expect("remove");
    // A file that did not come from a crawl. Skipped rather than adopted under
    // an invented identifier, because a published file has to trace back to
    // the segment it came from.
    std::fs::write(layout.data.join("notes.parquet"), b"not ours").expect("write");

    let state = umi_state::MemoryState::default();
    let store: &dyn umi_state::State = &state;
    let mut log = Log::open(&layout.log).expect("log");
    assert_eq!(adopt(&layout, store, &mut log).await.expect("adopt"), 2);

    let due = store
        .segments(umi_state::SegmentQuery::Unpublished)
        .await
        .expect("query");
    assert_eq!(due.len(), 2);
    for row in &due {
        let id = row.id.to_text();
        assert!(id == kept || id == unconverted, "{id}");
        // Off the ULID rather than off the clock, so publishing an old
        // directory records when the rows were written.
        assert_eq!(row.sealed_at_ms, row.id.timestamp_ms());
        assert_eq!(row.rows, if id == kept { 700 } else { 300 });
        assert_eq!(
            row.bytes,
            std::fs::metadata(&row.local_path).expect("stat").len()
        );
        assert!(row.remote.is_none());
    }

    // Twice is not four rows. A second `umi publish` on the same directory
    // has to be a no op rather than a set of duplicate ledger rows pointing at
    // files that are already on the hub.
    assert_eq!(adopt(&layout, store, &mut log).await.expect("again"), 0);
}

/// A sealed segment of sample page rows at `path`.
fn write_segment(path: &std::path::Path, rows: usize) -> std::path::PathBuf {
    use umi_file::{Create, SegmentWriter, StreamKind, WriterConfig, sample};
    let create = Create {
        stream: StreamKind::Pages,
        segment_id: [7u8; 16],
        coordinator: [8u8; 32],
        created_ms: umi_file::sample::T0,
        canon_version: 1,
        extractor_version: 4,
        crawl_profile: 0,
    };
    let mut writer =
        SegmentWriter::create(path, create, WriterConfig::for_memory(64 << 20)).expect("create");
    writer
        .push(&sample::batch(StreamKind::Pages, rows))
        .expect("push");
    writer.seal().expect("seal");
    path.to_path_buf()
}

/// A [`Publishing`] with secrets that are the right shape and belong to
/// nobody, for the tests that have to build one and never reach the network.
fn secrets() -> Publishing {
    Publishing {
        org: "open-index".to_owned(),
        token: "hf_this_token_is_not_real".to_owned(),
        key: "2a".repeat(32),
        slice: 0,
    }
}

/// A [`Config`] built from an environment and nothing else, so that a test
/// does not depend on whether the developer has a `umi.toml`.
fn config_with(env: &[(&str, &str)]) -> Config {
    let paths = Paths {
        local: std::path::PathBuf::from("/nonexistent/umi.toml"),
        user: None,
    };
    let env = env
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    Config::load(&paths, &env, &Flags::default()).expect("no files to fail on")
}
