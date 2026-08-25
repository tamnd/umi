//! The parts of `umi crawl` that have a right answer without a network.
//!
//! The loop itself is tested in `umi-crawl` against a canned fetcher, so what
//! is left here is the wiring, and the wiring is where a focused crawl goes
//! wrong quietly: a scope that came out wider than the flags said, a profile
//! that does not read back the same, a budget that never fires. All three of
//! those produce a crawl that looks like it worked.

use std::time::Duration;

use super::{
    Options, Settings, Stall, Stop, Summary, day, default_out, delay_ms, profile_toml, scope_for,
    seed_url, settings, sources, spent, tier,
};

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
    let error = super::resume(dir.path(), false).expect_err("no profile.toml");
    assert!(
        format!("{error}").contains("is not a crawl directory"),
        "got {error}"
    );
    assert_eq!(error.exit(), umi_types::Exit::Failure);
}

#[test]
fn publishing_is_refused_rather_than_ignored() {
    // The dangerous version of this is a `--publish` that crawls happily and
    // silently publishes nothing, because the operator then deletes the
    // directory believing the data is somewhere else.
    let options = Options {
        publish: true,
        ..options("example.com")
    };
    let error = super::crawl(&options).expect_err("not built yet");
    assert!(format!("{error}").contains("Hugging Face"), "got {error}");
}
