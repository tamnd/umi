//! What the conformance suite does not cover.
//!
//! The suite in [`conformance`](crate::conformance) is written entirely against
//! the trait, so it can only assert things the crawler could also observe. That
//! is the right constraint for a suite four backends have to pass, and it
//! leaves two gaps. The first is the pure functions, [`next_due_after`] and
//! [`retry_after_ms`], which every backend calls and none of them may vary. The
//! second is the reference backend's own behaviour where it goes past what the
//! trait promises, which a real backend is free to do differently and so must
//! not be in the suite.

use std::time::Duration;

use super::*;
use crate::memory::{content_hash, host_row, key_of};
use umi_types::{FetcherId, PldId, Tier, TierSignal};

const T0: u64 = 1_700_000_000_000;
const HOUR: u64 = 60 * 60 * 1000;
const DAY: u64 = 24 * HOUR;

#[tokio::test]
async fn the_reference_backend_conforms() {
    // The one test in this file that matters. A conformance suite nobody has
    // run is a wish, and this is what makes it a fact before umi-state-sqlite
    // inherits it.
    let report = conformance::check(|| async { MemoryState::new() }).await;
    report.assert_ok();
    assert!(
        report.cases.len() >= 30,
        "the suite has shrunk to {} cases",
        report.cases.len()
    );
}

/// A row that has been fetched, with no revalidator on it.
///
/// [`LedgerRow::default`] is not that. Its `etag_ref` of zero means the first
/// ETag in the shard's pool rather than none, which is what
/// [`LedgerRow::NO_ETAG`] is for, and the estimator treats a row with a
/// revalidator differently.
fn watched(fetch_count: u32, change_count: u32, observed_ms: u64) -> LedgerRow {
    LedgerRow {
        fetch_count,
        change_count,
        observed_secs: (observed_ms / 1000) as u32,
        last_fetch_ms: T0,
        etag_ref: LedgerRow::NO_ETAG,
        ..LedgerRow::default()
    }
}

#[test]
fn a_first_fetch_is_looked_at_again_the_next_day() {
    // A first fetch has nothing to compare against, so `changed` does not come
    // into it. It is true for every first fetch anyway, because the stored
    // hash was zero, and letting that halve the interval would put every page
    // on Earth on a twelve hour cycle after one look at it.
    let row = LedgerRow::default();
    assert_eq!(next_due_after(&row, false, T0), T0 + DAY);
    assert_eq!(next_due_after(&row, true, T0), T0 + DAY);
    // And it has watched the page for nothing so far, whatever the clock says.
    assert_eq!(row.observed_secs_after(T0), 0);
}

#[test]
fn a_first_fetch_with_a_date_on_it_uses_the_date_instead() {
    // Doc 09.4's publisher signal on the one fetch the estimator has nothing
    // to say about. A page whose `Last-Modified` was a day ago has given us
    // one interval, a day, so the rate is one change a day and the interval
    // that expects half a change is half a day.
    let row = LedgerRow::default();
    let lastmod = T0 - DAY;
    assert_eq!(next_due_dated(&row, false, T0, Some(lastmod)), T0 + DAY / 2);
    // A page that has not moved in a year is not looked at in half a year,
    // because the ceiling is 180 days.
    let stale = T0 - 365 * DAY;
    assert_eq!(
        refresh_interval_dated(&row, false, T0, Some(stale)),
        MAX_REFRESH.as_millis() as u64
    );
    // And one that moved a second ago does not get a five second interval,
    // because the floor is five minutes.
    let just_now = T0 - 1000;
    assert_eq!(
        refresh_interval_dated(&row, false, T0, Some(just_now)),
        MIN_REFRESH.as_millis() as u64
    );
    // A date in the future is a site with a broken clock or a hopeful CMS.
    // Treated as now, which lands on the floor rather than on an interval of
    // zero or one that wrapped.
    assert_eq!(
        refresh_interval_dated(&row, false, T0, Some(T0 + DAY)),
        MIN_REFRESH.as_millis() as u64
    );
    // No date is the old answer, unchanged.
    assert_eq!(refresh_interval_dated(&row, false, T0, None), DAY);
    assert_eq!(
        initial_refresh_ms(None, T0),
        INITIAL_REFRESH.as_millis() as u64
    );
}

#[test]
fn a_date_only_counts_on_the_fetch_that_has_no_history() {
    // Once the estimator has observations of its own it is measuring what the
    // page does rather than what the page says, and a `Last-Modified` header
    // is already in the row as evidence the origin revalidates. Feeding it in
    // twice would halve intervals on the strength of one header.
    let row = watched(4, 2, 4 * DAY);
    let now = T0 + 4 * DAY;
    assert_eq!(
        refresh_interval_dated(&row, false, now, Some(now - 60_000)),
        refresh_interval_ms(&row, false, now)
    );
}

#[test]
fn a_page_that_keeps_changing_converges_downward() {
    // Fetch it, find it changed every time, and the interval walks down rather
    // than hunting around. It does not reach the floor: a page that changes on
    // every visit at a five minute spacing is above the churn ceiling and doc
    // 09.4 says to stop tracking it and sample it instead, which is what the
    // last assertion is about.
    let mut row = watched(1, 1, 0);
    let mut now = T0 + DAY;
    let mut previous = DAY;
    for _ in 0..6 {
        let interval = refresh_interval_ms(&row, true, now);
        assert!(interval <= previous, "{interval} is not below {previous}");
        previous = interval;
        row.observed_secs = row.observed_secs_after(now);
        row.last_fetch_ms = now;
        row.fetch_count += 1;
        row.change_count += 1;
        now += interval;
    }
    assert!(previous < 6 * HOUR, "settled at {previous}");
    assert!(previous >= MIN_REFRESH.as_millis() as u64);
}

#[test]
fn a_page_that_never_changes_converges_upward() {
    // No change ever seen is a rate of zero, so the only thing holding the
    // interval down is how long we have actually watched the page. That makes
    // the walk geometric rather than a jump straight to the ceiling, which is
    // the difference between two fetches an hour apart and real evidence.
    let mut row = watched(1, 1, 0);
    let mut now = T0 + DAY;
    for _ in 0..16 {
        let next = next_due_after(&row, false, now);
        row.observed_secs = row.observed_secs_after(now);
        row.last_fetch_ms = now;
        row.fetch_count += 1;
        now = next;
    }
    assert_eq!(now - row.last_fetch_ms, MAX_REFRESH.as_millis() as u64);
}

#[test]
fn the_estimator_finds_a_change_period_it_was_not_told() {
    // The one that matters. A page really changes every `period`, we only ever
    // learn changed or not changed, and the interval has to end up near the
    // period rather than at either clamp. `K_Q16` is a half, so the target is
    // half the period, and the band is wide because six or seven visits is not
    // a lot of evidence.
    for period in [2 * HOUR, DAY, 7 * DAY, 30 * DAY] {
        let mut row = watched(1, 1, 0);
        let mut now = T0 + period;
        let mut last_change = T0;
        // Long enough for the observation window to stop being the binding
        // constraint at every period in the list.
        for _ in 0..40 {
            let changed = now - last_change >= period;
            if changed {
                last_change = now;
                row.change_count += 1;
            }
            let interval = refresh_interval_ms(&row, changed, now);
            row.observed_secs = row.observed_secs_after(now);
            row.last_fetch_ms = now;
            row.fetch_count += 1;
            now += interval;
        }
        let settled = now - row.last_fetch_ms;
        assert!(
            settled > period / 4 && settled < period,
            "a {period} ms page settled at {settled} ms"
        );
    }
}

#[test]
fn a_page_that_changes_faster_than_we_can_follow_gets_sampled() {
    // Doc 09.4's non monotonic part. A page changing every minute cannot be
    // kept fresh at any budget, so the policy gives up on tracking it. What
    // that has to buy is a bounded number of fetches, and a minute long change
    // period over thirty days is 43200 changes.
    let mut row = watched(1, 1, 0);
    let mut now = T0 + 60 * 1000;
    let mut fetches = 0;
    while now < T0 + 30 * DAY {
        let interval = refresh_interval_ms(&row, true, now);
        row.observed_secs = row.observed_secs_after(now);
        row.last_fetch_ms = now;
        row.fetch_count += 1;
        row.change_count += 1;
        now += interval;
        fetches += 1;
    }
    // The floor would allow 8640 in that window. What holds it to a fraction
    // of that is the churn ceiling, and what stops it reaching the daily sample
    // doc 09.4 asks for is that a page changing every minute and a page
    // changing every hour look identical when you only look every hour. See the
    // module docs.
    assert!(fetches < 800, "spent {fetches} fetches on a hopeless page");
}

#[test]
fn a_revalidator_buys_a_shorter_interval() {
    // A 304 is about 500 bytes against a page fetch of a hundred times that,
    // so a page that offers one is worth checking twice as often.
    let bare = watched(4, 3, 4 * DAY);
    let with_etag = LedgerRow {
        etag_ref: 7,
        ..bare
    };
    let now = T0 + DAY;
    assert_eq!(
        refresh_interval_ms(&with_etag, true, now),
        refresh_interval_ms(&bare, true, now) / 2
    );
}

#[test]
fn nothing_is_extrapolated_past_the_evidence_for_it() {
    // Two looks an hour apart with no change is not grounds for a six month
    // interval, however confident the arithmetic is.
    let row = watched(1, 1, 0);
    assert_eq!(refresh_interval_ms(&row, false, T0 + HOUR), 2 * HOUR);
}

#[test]
fn a_304_counts_as_evidence_the_page_did_not_change() {
    // The subtle one. A conditional request that comes back 304 is an
    // observation, and a backend that only counted 200s would learn nothing
    // from the cheapest answer the web gives us.
    let row = watched(3, 1, 3 * DAY);
    let now = T0 + DAY;
    assert!(refresh_interval_ms(&row, false, now) > refresh_interval_ms(&row, true, now));
}

#[test]
fn the_refresh_interval_is_never_outside_its_clamps() {
    // The clamp is load bearing. Without a floor, a page whose extracted text
    // includes a timestamp changes on every fetch and turns into a hot loop
    // against one origin.
    for gap in [0, 1, 1000, DAY, 400 * DAY, u64::MAX / 4] {
        for (fetch_count, change_count) in [(0, 0), (1, 1), (3, 1), (3, 3), (u32::MAX, u32::MAX)] {
            let row = LedgerRow {
                observed_secs: u32::MAX,
                ..watched(fetch_count, change_count, 0)
            };
            for changed in [true, false] {
                let interval = refresh_interval_ms(&row, changed, T0 + gap);
                assert!(
                    interval >= MIN_REFRESH.as_millis() as u64,
                    "gap {gap} changed {changed} gave {interval}"
                );
                assert!(
                    interval <= MAX_REFRESH.as_millis() as u64,
                    "gap {gap} changed {changed} gave {interval}"
                );
            }
        }
    }
}

#[test]
fn the_retry_ladder_is_the_one_in_doc_05() {
    assert_eq!(retry_after_ms(0), 60 * 1000);
    assert_eq!(retry_after_ms(1), 60 * 1000);
    assert_eq!(retry_after_ms(2), 5 * 60 * 1000);
    assert_eq!(retry_after_ms(3), 25 * 60 * 1000);
    assert_eq!(retry_after_ms(4), 2 * HOUR);
    assert_eq!(retry_after_ms(5), 12 * HOUR);
    assert_eq!(retry_after_ms(6), 24 * HOUR);
    // It stops at daily rather than growing forever, so a host that comes back
    // after a long outage is found within a day.
    assert_eq!(retry_after_ms(u8::MAX), 24 * HOUR);
}

#[test]
fn the_retry_ladder_never_goes_backwards() {
    let mut previous = 0;
    for streak in 0..=u8::MAX {
        let wait = retry_after_ms(streak);
        assert!(
            wait >= previous,
            "streak {streak} waits less than the one before it"
        );
        previous = wait;
    }
}

#[test]
fn an_admit_report_accounts_for_the_whole_batch() {
    let report = AdmitReport {
        seen: 90,
        admitted: 5,
        held: 3,
        excluded: 2,
        shard_misses: 40,
        refreshed: 7,
    };
    // shard_misses counts domains, not urls, and refreshed counts a subset of
    // seen, so neither is part of the sum. Folding either in would make the one
    // number an operator watches change the arithmetic they check it with.
    assert_eq!(report.total(), 100);
}

#[test]
fn a_priority_covers_the_unit_interval() {
    assert_eq!(Priority::from_unit(0.0), Priority::MIN);
    assert_eq!(Priority::from_unit(1.0), Priority::MAX);
    assert_eq!(Priority::from_unit(-5.0), Priority::MIN);
    assert_eq!(Priority::from_unit(5.0), Priority::MAX);
    assert!(Priority::from_unit(0.4) < Priority::from_unit(0.6));
    assert!(Priority::MIN < Priority::DEFAULT && Priority::DEFAULT < Priority::MAX);
}

#[test]
fn only_a_live_url_is_ever_scheduled() {
    assert!(UrlState::Pending.is_schedulable());
    assert!(UrlState::Fetched.is_schedulable());
    assert!(UrlState::Failed.is_schedulable());
    assert!(!UrlState::Gone.is_schedulable());
    assert!(!UrlState::Excluded.is_schedulable());
    for state in [
        UrlState::Pending,
        UrlState::Fetched,
        UrlState::Failed,
        UrlState::Gone,
        UrlState::Excluded,
    ] {
        assert_eq!(UrlState::from_u8(state as u8), Some(state));
    }
    assert_eq!(UrlState::from_u8(5), None);
}

#[test]
fn a_hosts_delay_is_the_larger_of_the_two_it_has() {
    let mut row = host_row("https://example.com/");
    assert_eq!(row.adaptive_delay_ms, HostRow::INITIAL_DELAY_MS);
    row.crawl_delay_ms = Some(5000);
    assert_eq!(row.delay().as_millis(), 5000);
    row.adaptive_delay_ms = 9000;
    assert_eq!(row.delay().as_millis(), 9000);
    // A published Crawl-delay shorter than our own adaptive delay is not a
    // licence to go faster.
    row.crawl_delay_ms = Some(1);
    assert_eq!(row.delay().as_millis(), 9000);
}

#[test]
fn a_tier_policy_starts_where_all_three_ceilings_allow() {
    let mut policy = TierPolicy::new();
    assert_eq!(policy.start_at(Tier::Rendered, T0), Tier::Plain);
    // A host that has escalated is still held down by what the fetcher can
    // actually run, which is what stops a T3 host being handed to a plain HTTP
    // fetcher that will only get a challenge page back.
    policy.preferred = Tier::Rendered;
    policy.max = Tier::Rendered;
    policy.last_probe_down_ms = T0;
    assert_eq!(policy.start_at(Tier::Plain, T0), Tier::Plain);
    assert!(!policy.reachable_by(Tier::Plain, T0));
    assert!(policy.reachable_by(Tier::Rendered, T0));
}

#[test]
fn a_host_that_starts_blocking_climbs_the_ladder_and_stops_at_its_ceiling() {
    let mut policy = TierPolicy::new();
    assert!(policy.observe(TierSignal::Blocked, Tier::Plain, T0));
    assert_eq!(policy.preferred, Tier::Emulated);
    assert_eq!(policy.consecutive_blocks, 1);
    assert_eq!(policy.backoff_ms(), 60_000);

    // The ceiling is T2 without evidence, so more blocks buy more backoff and
    // no more tiers. A crawler that answered every block with a browser would
    // spend its whole browser pool on sites that have said no.
    assert!(policy.observe(TierSignal::Blocked, Tier::Emulated, T0 + HOUR));
    assert_eq!(policy.preferred, Tier::Emulated);
    assert_eq!(policy.backoff_ms(), 300_000);
    assert!(!policy.refusing());

    // Doc 05.8's thirty days of daily probes.
    for n in 3..=TierPolicy::REFUSING_AFTER_BLOCKS {
        policy.observe(TierSignal::Blocked, Tier::Emulated, T0 + DAY * u64::from(n));
    }
    assert_eq!(policy.backoff_ms(), TierPolicy::BACKOFF_FLOOR_MS);
    assert!(policy.refusing());
}

#[test]
fn an_escalated_host_is_probed_back_down_and_a_clean_probe_sticks() {
    let mut policy = TierPolicy::new();
    policy.observe(TierSignal::Blocked, Tier::Plain, T0);
    assert_eq!(policy.wants(T0), Tier::Emulated);

    // Six days later it is still T2, because a probe a day would be a probe
    // that costs as much as the thing it is avoiding.
    assert!(!policy.probing(T0 + 6 * DAY));
    assert_eq!(policy.wants(T0 + 6 * DAY), Tier::Emulated);

    // A week later it is offered T1 again, once.
    let week = T0 + TierPolicy::PROBE_EVERY_MS;
    assert!(policy.probing(week));
    assert_eq!(policy.wants(week), Tier::Plain);

    // And the probe comes back clean, so the host stays on T1.
    assert!(policy.observe(TierSignal::Success, Tier::Plain, week));
    assert_eq!(policy.preferred, Tier::Plain);
    assert_eq!(policy.consecutive_blocks, 0);
    assert!(!policy.probing(week + TierPolicy::PROBE_EVERY_MS));
}

#[test]
fn a_probe_that_is_blocked_again_goes_straight_back_up() {
    let mut policy = TierPolicy::new();
    policy.observe(TierSignal::Blocked, Tier::Plain, T0);
    let week = T0 + TierPolicy::PROBE_EVERY_MS;
    assert_eq!(policy.wants(week), Tier::Plain);

    policy.observe(TierSignal::Blocked, Tier::Plain, week);
    assert_eq!(policy.wants(week), Tier::Emulated);
    // And the week starts again from the block, not from the last probe, so
    // the next one is a week away rather than immediately.
    assert!(!policy.probing(week + DAY));
}

#[test]
fn a_shell_asks_for_a_browser_and_the_weekly_probe_takes_it_back() {
    let mut policy = TierPolicy::new();
    assert!(policy.observe(TierSignal::Shell, Tier::Plain, T0));
    assert!(policy.render_required);
    assert_eq!(policy.wants(T0), Tier::Rendered);

    // A site that was a shell for a week and is a page again does not keep a
    // browser to itself, which is the half of doc 05.8 that gets forgotten.
    let week = T0 + TierPolicy::PROBE_EVERY_MS;
    assert_eq!(policy.wants(week), Tier::Plain);
    policy.observe(TierSignal::Success, Tier::Plain, week);
    assert_eq!(policy.wants(week), Tier::Plain);
}

#[test]
fn a_healthy_host_learns_nothing_and_is_never_written_back() {
    // The return value is what keeps the ladder off the hot path, so it is
    // worth a test of its own: an ordinary page from an ordinary host must
    // not report a change, or the crawl loop writes a host record per page.
    let mut policy = TierPolicy::new();
    assert!(!policy.observe(TierSignal::Success, Tier::Plain, T0));
    assert!(!policy.observe(TierSignal::Success, Tier::Plain, T0 + DAY));
    assert_eq!(policy, TierPolicy::new());
}

#[test]
fn a_ledger_row_stays_small_enough_to_be_worth_encoding() {
    // Doc 08.3 lists no url column, and at 100 billion rows that is the
    // difference between meeting the under 20 bytes per url target in doc 01
    // and missing it by an order of magnitude. This is really a check that
    // nobody adds a String to the row. Doc 08.3 counts 76 bytes of fields and
    // Rust's default layout pads that to 88, which the shard encoding takes
    // down to 24 to 32.
    assert!(
        size_of::<LedgerRow>() <= 88,
        "a ledger row grew to {} bytes",
        size_of::<LedgerRow>()
    );
}

#[tokio::test]
async fn a_held_url_is_visible_to_the_fetcher_that_found_it() {
    // Graduating a held url is doc 06.2's job and lands in milestone 2. What
    // milestone 1 owes it is that the pen is keyed by fetcher, so one bad
    // fetcher's discoveries can be found and dropped without touching anyone
    // else's.
    let state = MemoryState::new();
    let good = FetcherId::from_bytes([1u8; 32]);
    let bad = FetcherId::from_bytes([2u8; 32]);
    let urls = ["https://a.example.com/", "https://b.example.com/"];

    state
        .admit(&[
            Candidate {
                discovery: Discovery::Unverified(good),
                ..Candidate::new(urls[0], T0).unwrap()
            },
            Candidate {
                discovery: Discovery::Unverified(bad),
                ..Candidate::new(urls[1], T0).unwrap()
            },
        ])
        .await
        .unwrap();

    assert_eq!(state.held_for(good).len(), 1);
    assert_eq!(state.held_for(bad).len(), 1);
    assert_eq!(state.held_for(good)[0].url, urls[0]);
    assert_eq!(state.held_for(FetcherId::LOCAL).len(), 0);
}

#[tokio::test]
async fn a_conditional_request_does_not_move_the_change_clock() {
    // A 304 says the page did not change, so last_change_ms and content_hash
    // have to stay exactly where they were. Getting this wrong would make every
    // conditional request look like a change to the estimator in doc 12 and
    // drive the refresh interval to the floor for the whole web.
    let state = MemoryState::new();
    let url = "https://example.com/page";
    let key = key_of(url);

    state
        .admit(&[Candidate::new(url, T0).unwrap()])
        .await
        .unwrap();

    let first = state
        .lease(&LeaseRequest::new(FetcherId::LOCAL, T0, 1))
        .await
        .unwrap();
    state
        .complete(&[FetchOutcome {
            lease: first[0].id,
            key,
            finished_ms: T0 + 1000,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Fetched {
                status: 200,
                content_hash: content_hash("body one"),
                revalidate: Revalidator {
                    etag: Some("\"v1\"".to_owned()),
                    last_modified_ms: None,
                },
            },
        }])
        .await
        .unwrap();

    let after_fetch = state.row(&key).unwrap();
    assert_eq!(after_fetch.change_count, 1);
    assert_eq!(after_fetch.last_change_ms, T0 + 1000);

    let second = state
        .lease(&LeaseRequest::new(
            FetcherId::LOCAL,
            after_fetch.next_due_ms,
            1,
        ))
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0]
            .revalidate
            .as_ref()
            .and_then(|r| r.etag.as_deref()),
        Some("\"v1\"")
    );

    let finished = after_fetch.next_due_ms + 1000;
    state
        .complete(&[FetchOutcome {
            lease: second[0].id,
            key,
            finished_ms: finished,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::NotModified {
                status: 304,
                revalidate: Revalidator::default(),
            },
        }])
        .await
        .unwrap();

    let after_304 = state.row(&key).unwrap();
    assert_eq!(after_304.fetch_count, 2, "a 304 is still a fetch");
    assert_eq!(after_304.change_count, 1, "a 304 counted as a change");
    assert_eq!(
        after_304.last_change_ms,
        T0 + 1000,
        "a 304 moved the change clock"
    );
    assert_eq!(after_304.content_hash, content_hash("body one"));
    assert_eq!(after_304.last_fetch_ms, finished);
}

#[tokio::test]
async fn a_repeated_etag_is_interned_once() {
    // Two urls on one host with the same etag share one pool entry, which is
    // the whole reason the ledger stores a u32 rather than a string.
    let state = MemoryState::new();
    let urls = ["https://example.com/a", "https://example.com/b"];

    for url in urls {
        state
            .admit(&[Candidate::new(url, T0).unwrap()])
            .await
            .unwrap();
    }
    let mut now = T0;
    for _ in urls {
        let leases = state
            .lease(&LeaseRequest::new(FetcherId::LOCAL, now, 1))
            .await
            .unwrap();
        assert_eq!(leases.len(), 1, "nothing leased at {now}");
        state
            .complete(&[FetchOutcome {
                lease: leases[0].id,
                key: leases[0].key,
                finished_ms: now + 1,
                tier_used: Tier::Plain,
                pace: Pace::default(),
                result: FetchResult::Fetched {
                    status: 200,
                    content_hash: content_hash("shared"),
                    revalidate: Revalidator {
                        etag: Some("\"same\"".to_owned()),
                        last_modified_ms: None,
                    },
                },
            }])
            .await
            .unwrap();
        now += 10_000;
    }

    let a = state.row(&key_of(urls[0])).unwrap();
    let b = state.row(&key_of(urls[1])).unwrap();
    assert_eq!(a.etag_ref, b.etag_ref, "the same etag was interned twice");
    assert_ne!(a.etag_ref, LedgerRow::NO_ETAG);
}

#[tokio::test]
async fn a_completion_from_a_lease_that_already_expired_still_counts() {
    // The page really was fetched. Throwing that away because the coordinator
    // got impatient would cost a refetch against an origin that did nothing
    // wrong, which is the opposite of what the timeout is for.
    let state = MemoryState::new();
    let url = "https://example.com/slow";
    let key = key_of(url);

    state
        .admit(&[Candidate::new(url, T0).unwrap()])
        .await
        .unwrap();
    let leases = state
        .lease(&LeaseRequest {
            lease_for: Duration::from_secs(5),
            ..LeaseRequest::new(FetcherId::LOCAL, T0, 1)
        })
        .await
        .unwrap();

    let long_after = leases[0].expires_ms + HOUR;
    state
        .complete(&[FetchOutcome {
            lease: leases[0].id,
            key,
            finished_ms: long_after,
            tier_used: Tier::Emulated,
            pace: Pace::default(),
            result: FetchResult::Fetched {
                status: 200,
                content_hash: content_hash("late"),
                revalidate: Revalidator::default(),
            },
        }])
        .await
        .unwrap();

    let row = state.row(&key).unwrap();
    assert_eq!(row.state, UrlState::Fetched);
    assert_eq!(row.fetch_count, 1);
    assert_eq!(row.tier_used, Tier::Emulated);
}

#[tokio::test]
async fn a_lease_batch_for_one_host_is_spaced_out_rather_than_trusted() {
    // Doc 04's min_gap_ms is a belt on top of this brace. The coordinator does
    // not hand a fetcher four urls for one host and hope, it stamps each one
    // with the earliest moment it may be sent.
    let state = MemoryState::new();
    let urls: Vec<String> = (0..4)
        .map(|n| format!("https://example.com/p/{n}"))
        .collect();

    for url in &urls {
        state
            .admit(&[Candidate::new(url, T0).unwrap()])
            .await
            .unwrap();
    }
    let leases = state
        .lease(&LeaseRequest::new(FetcherId::LOCAL, T0, 10))
        .await
        .unwrap();
    assert_eq!(leases.len(), 4);

    let mut times: Vec<u64> = leases.iter().map(|lease| lease.not_before_ms).collect();
    times.sort_unstable();
    for pair in times.windows(2) {
        assert_eq!(
            pair[1] - pair[0],
            u64::from(HostRow::INITIAL_DELAY_MS),
            "two requests to one host were stamped less than the delay apart"
        );
    }

    // And the host's timer has moved past the whole batch, so a second fetcher
    // asking now gets nothing for this host.
    let more = state
        .lease(&LeaseRequest::new(FetcherId::LOCAL, T0, 10))
        .await
        .unwrap();
    assert!(
        more.is_empty(),
        "the politeness timer did not absorb the batch"
    );
}

#[tokio::test]
async fn eviction_keeps_a_domain_that_still_has_work_in_flight() {
    let state = MemoryState::new();
    let url = "https://example.com/page";
    let pld = key_of(url).pld;

    state
        .admit(&[Candidate::new(url, T0).unwrap()])
        .await
        .unwrap();
    let leases = state
        .lease(&LeaseRequest::new(FetcherId::LOCAL, T0, 1))
        .await
        .unwrap();
    assert_eq!(leases.len(), 1);

    let busy = state.evict(&[pld]).await.unwrap();
    assert_eq!(busy.in_use, 1, "a domain with a lease out was evicted");
    assert_eq!(busy.evicted, 0);

    state
        .release(&[leases[0].id], NackReason::Shutdown)
        .await
        .unwrap();
    let idle = state.evict(&[pld]).await.unwrap();
    assert_eq!(idle.evicted, 1, "an idle domain was not evicted");
    assert!(state.resident().await.unwrap().is_empty());
}

#[tokio::test]
async fn admitting_a_url_warms_its_domain_and_counts_the_miss() {
    let state = MemoryState::new();
    // Two registrable domains, not two hosts under one. Residency is per pay
    // level domain, so a.example.com and b.example.com are one shard.
    let first = state
        .admit(&[
            Candidate::new("https://example.com/", T0).unwrap(),
            Candidate::new("https://sub.example.com/two", T0).unwrap(),
            Candidate::new("https://example.net/", T0).unwrap(),
        ])
        .await
        .unwrap();
    assert_eq!(first.shard_misses, 2, "misses are counted per domain");
    assert_eq!(first.admitted, 3);

    let second = state
        .admit(&[Candidate::new("https://example.com/three", T0).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        second.shard_misses, 0,
        "a resident domain was counted as a miss"
    );

    let stats = state.stats().await.unwrap();
    assert_eq!(stats.shard_misses, 2);
    assert_eq!(stats.resident_plds, 2);
}

#[tokio::test]
async fn a_checkpoint_carries_the_counters_as_of_the_snapshot() {
    let state = MemoryState::new();
    state
        .admit(&[Candidate::new("https://example.com/", T0).unwrap()])
        .await
        .unwrap();
    let before = state.checkpoint(T0).await.unwrap();
    assert_eq!(before.stats.urls_seen, 1);

    state
        .admit(&[Candidate::new("https://example.com/two", T0).unwrap()])
        .await
        .unwrap();
    let after = state.checkpoint(T0).await.unwrap();
    assert_eq!(after.stats.urls_seen, 2);
    // The earlier snapshot is a value, and admitting more did not reach back
    // into it.
    assert_eq!(before.stats.urls_seen, 1);
    assert!(after.sequence > before.sequence);
}

#[test]
fn an_error_says_which_domain_could_not_be_warmed() {
    // The operator reading this line needs the domain, not just the fact that
    // object storage is unwell.
    let pld = PldId::derive(b"example.com");
    let error = StateError::ShardUnavailable {
        pld,
        reason: "connection reset".to_owned(),
    };
    let text = error.to_string();
    assert!(text.contains(&pld.to_string()), "{text}");
    assert!(text.contains("connection reset"), "{text}");

    let too_big = StateError::BatchTooLarge {
        got: 9000,
        limit: BATCH,
    };
    assert!(too_big.to_string().contains("9000"));
    assert!(too_big.to_string().contains(&BATCH.to_string()));
}

/// One host to run the rate limiter over, at its starting delay.
fn paced_host() -> HostRow {
    host_row("https://example.com/")
}

/// A response that arrived, with a latency and nothing else.
const fn took(ms: u32) -> Pace {
    Pace {
        latency_ms: Some(ms),
        retry_after_ms: None,
    }
}

/// What doc 07.6 calls a fast 200.
const fn ok() -> FetchResult {
    FetchResult::Fetched {
        status: 200,
        content_hash: [0u8; 8],
        revalidate: Revalidator {
            etag: None,
            last_modified_ms: None,
        },
    }
}

const fn failed(status: Option<u16>, kind: FailureKind) -> FetchResult {
    FetchResult::Failed { status, kind }
}

#[test]
fn latency_creep_alone_backs_us_off() {
    // The rung that matters most and the one a crawler is most tempted to
    // skip. Nothing here errors: the origin answers every single request with
    // a 200, it just takes longer and longer about it, and doc 07.6 wants us
    // to notice that before the operator does.
    let mut host = paced_host();
    assert_eq!(host.adaptive_delay_ms, HostRow::INITIAL_DELAY_MS);

    for _ in 0..5 {
        assert!(host.observe(&ok(), took(3000), T0));
    }
    assert_eq!(
        host.adaptive_delay_ms, 3712,
        "five slow answers should be five 1.3 steps"
    );
    assert_eq!(host.failures, 0, "nothing failed");
    assert_eq!(host.fetches, 5);
}

#[test]
fn the_four_rungs_are_the_ones_doc_07_6_wrote_down() {
    let cases = [
        (ok(), took(100), 900),
        (ok(), took(2001), 1300),
        (failed(Some(429), FailureKind::Blocked), took(50), 4000),
        (failed(Some(503), FailureKind::ServerError), took(50), 4000),
        (failed(Some(500), FailureKind::ServerError), took(50), 2000),
        (failed(None, FailureKind::Connect), took(50), 2000),
        (failed(None, FailureKind::Tls), took(50), 2000),
        (failed(None, FailureKind::Timeout), took(50), 2000),
    ];
    for (result, pace, want) in cases {
        let mut host = paced_host();
        // The 0.9 rung is invisible at the default floor, which is also 1000,
        // so this asks what the step computed rather than what the clamp
        // allowed. Every host in the table starts from the same delay.
        host.fast_streak = super::pace::FAST_STREAK;
        assert!(host.observe(&result, pace, T0));
        assert_eq!(host.adaptive_delay_ms, want, "{result:?} at {pace:?}");
    }
}

#[test]
fn an_answer_that_is_not_about_load_changes_nothing() {
    // A 404 is a correct, cheap answer about a page that is not there. A site
    // with a lot of dead links must not read as a site with spare capacity,
    // and it must not read as a site in trouble either.
    for kind in [
        FailureKind::NotFound,
        FailureKind::Rejected,
        FailureKind::Malformed,
    ] {
        let mut host = paced_host();
        host.fast_streak = super::pace::FAST_STREAK;
        assert!(!host.observe(&failed(Some(404), kind), took(30), T0));
        assert_eq!(host.adaptive_delay_ms, HostRow::INITIAL_DELAY_MS);
        assert_eq!(host.fetches, 0, "{kind:?} moved a counter");
    }

    let mut host = paced_host();
    assert!(!host.observe(&FetchResult::Gone { status: 410 }, took(30), T0));
    assert_eq!(host.next_allowed_ms, 0);
}

#[test]
fn a_lease_that_never_became_a_request_is_not_an_observation() {
    // Robots excluded it before anything went on the wire, so there is no
    // latency, no host to be polite to yet, and nothing to write. Doc 08.4
    // counts hosts, and a crawl of a disallowed site must not leave fifty
    // thousand records saying we spoke to somebody we never contacted.
    let mut host = paced_host();
    let excluded = FetchResult::Excluded {
        reason: crate::ExcludeReason::Robots,
    };
    assert!(!host.observe(&excluded, Pace::default(), T0));
    assert_eq!(host.fetches, 0);

    // The same variant with a latency is doc 13.2's content filter, which runs
    // on a response the origin actually served, and that one counts.
    let mut host = paced_host();
    host.fast_streak = super::pace::FAST_STREAK;
    assert!(host.observe(&excluded, took(50), T0));
    assert_eq!(host.fetches, 1);
    assert_eq!(host.adaptive_delay_ms, 900);
}

#[test]
fn recovery_is_gradual_and_stops_at_the_floor() {
    // Backing off is one step and coming back is many, which is the whole
    // asymmetry. Count them, because "it recovers eventually" is also true of
    // a crawler that recovers in two requests.
    let mut host = paced_host();
    host.fast_streak = super::pace::FAST_STREAK;
    host.observe(&failed(Some(429), FailureKind::Blocked), took(20), T0);
    assert_eq!(host.adaptive_delay_ms, 4000);
    assert_eq!(host.fast_streak, 0, "a 429 is not a fast success");

    let mut steps = 0;
    while host.adaptive_delay_ms > HostRow::DEFAULT_FLOOR_MS {
        host.observe(&ok(), took(100), T0);
        steps += 1;
        assert!(steps < 100, "the delay is not coming down");
    }
    assert_eq!(steps, 14, "0.9 per clean answer, from 4000 to 1000");

    // And it stays there, because the fast floor has to be earned separately.
    for _ in 0..10 {
        host.observe(&ok(), took(100), T0);
    }
    assert_eq!(host.adaptive_delay_ms, HostRow::DEFAULT_FLOOR_MS);
}

#[test]
fn the_fast_floor_is_earned_by_a_streak_and_lost_by_one_bad_answer() {
    let mut host = paced_host();
    for _ in 0..super::pace::FAST_STREAK - 1 {
        host.observe(&ok(), took(100), T0);
    }
    assert_eq!(host.floor_ms(), HostRow::DEFAULT_FLOOR_MS);
    assert_eq!(host.adaptive_delay_ms, HostRow::DEFAULT_FLOOR_MS);

    host.observe(&ok(), took(100), T0);
    assert_eq!(host.floor_ms(), HostRow::FAST_FLOOR_MS);

    // A response that is merely not slow does not count towards the streak.
    // Five a second is the most this crawler ever sends anybody and a host
    // that takes a second to answer has not earned it.
    host.observe(&ok(), took(super::pace::FAST_MS + 1), T0);
    assert_eq!(host.floor_ms(), HostRow::DEFAULT_FLOOR_MS);
    assert_eq!(host.fast_streak, 0);
}

#[test]
fn nothing_ever_goes_below_five_requests_a_second_or_above_a_minute() {
    let mut host = paced_host();
    host.fast_streak = super::pace::FAST_STREAK;
    for _ in 0..500 {
        host.observe(&ok(), took(10), T0);
    }
    assert_eq!(host.adaptive_delay_ms, HostRow::FAST_FLOOR_MS);

    for _ in 0..500 {
        host.observe(&failed(Some(503), FailureKind::ServerError), took(10), T0);
    }
    assert_eq!(host.adaptive_delay_ms, HostRow::MAX_DELAY_MS);
    assert_eq!(host.consecutive_failures, 500);
}

#[test]
fn retry_after_is_a_minimum_and_never_shortens_our_own_wait() {
    // An origin that asks for a second while we are already waiting eight gets
    // eight. Waiting longer than we were asked to has never annoyed anybody,
    // and treating the header as the answer rather than as a floor is how a
    // crawler talks itself into speeding up.
    let mut host = paced_host();
    host.adaptive_delay_ms = 8000;
    host.observe(
        &failed(Some(429), FailureKind::Blocked),
        Pace {
            latency_ms: Some(40),
            retry_after_ms: Some(1000),
        },
        T0,
    );
    assert_eq!(
        host.next_allowed_ms,
        T0 + 32_000,
        "8000 backed off to 32000"
    );

    let mut host = paced_host();
    host.observe(
        &failed(Some(503), FailureKind::ServerError),
        Pace {
            latency_ms: Some(40),
            retry_after_ms: Some(600_000),
        },
        T0,
    );
    assert_eq!(
        host.next_allowed_ms,
        T0 + 600_000,
        "ten minutes is longer than the delay ceiling and it is honoured exactly"
    );
}

#[test]
fn a_crawl_delay_from_robots_is_honoured_alongside_the_adaptive_one() {
    // Two floors, both of them minimums, and the larger wins. A site that
    // published `Crawl-delay: 30` said thirty seconds even on the request that
    // came back in forty milliseconds.
    let mut host = paced_host();
    host.crawl_delay_ms = Some(30_000);
    host.fast_streak = super::pace::FAST_STREAK;
    host.observe(&ok(), took(40), T0);
    assert_eq!(host.adaptive_delay_ms, 900);
    assert_eq!(host.next_allowed_ms, T0 + 30_000);
}

#[test]
fn the_politeness_timer_only_moves_forward() {
    // `lease` has already spaced out everything else it handed out for this
    // host. A completion that arrives out of order, or one for a request that
    // started long before, must not pull that back and let the next url go
    // early.
    let mut host = paced_host();
    host.next_allowed_ms = T0 + 60_000;
    host.observe(&ok(), took(40), T0);
    assert_eq!(host.next_allowed_ms, T0 + 60_000);
}
