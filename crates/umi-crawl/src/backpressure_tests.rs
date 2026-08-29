//! Doc 15.3's ladder, one rung and one threshold at a time.
//!
//! Every number in here is written out in the document, so a test that fails
//! after somebody edits a constant is doing its job: the thresholds are not
//! implementation detail, they are the operational contract that gate 3.6
//! measures against.

use umi_state::{Budget, RefreshClass};
use umi_types::Tier;

use super::backpressure::{
    Allowance, Backpressure, Cause, DISCOVERY_NORMAL, DISCOVERY_UNDER_PRESSURE, Ladder, Signals,
};

const GB: u64 = 1 << 30;
const MINUTE: u64 = 60_000;

/// A crawl with nothing wrong with it. Plenty of disk, nothing queued, and no
/// memory budget, which turns the memory ladder off.
fn calm() -> Signals {
    Signals {
        free_disk_bytes: 500 * GB,
        ..Signals::default()
    }
}

#[test]
fn nothing_wrong_is_level_zero_on_all_three_and_says_so() {
    let mut ladder = Backpressure::new();
    assert!(ladder.observe(&calm(), 0).is_empty());
    assert!(ladder.normal());
    assert_eq!(ladder.to_string(), "normal");
    assert_eq!(ladder.allowance(), Allowance::default());
    assert_eq!(ladder.allowance().discovery_share, DISCOVERY_NORMAL);
}

#[test]
fn four_gigabytes_unpublished_stops_rendering_and_cuts_discovery() {
    // Doc 15.3's level 1, and the trade it is there to make: barely any page
    // rate given up, roughly 40 percent of the bytes.
    let mut ladder = Backpressure::new();
    let signals = Signals {
        unpublished_bytes: 4 * GB + 1,
        ..calm()
    };
    let moved = ladder.observe(&signals, 0);
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].ladder, Ladder::Disk);
    assert_eq!(moved[0].from, 0);
    assert_eq!(moved[0].to, 1);
    assert_eq!(moved[0].cause, Cause::Unpublished);
    assert_eq!(moved[0].value, 4 * GB + 1);

    let allowed = ladder.allowance();
    assert_eq!(allowed.max_tier, Tier::Emulated, "T3 is still running");
    assert_eq!(allowed.discovery_share, DISCOVERY_UNDER_PRESSURE);
    assert_eq!(allowed.lease_scale, 1.0, "level 1 does not cut page rate");
    assert!(allowed.community_leasing);
}

#[test]
fn exactly_four_gigabytes_is_not_over_four_gigabytes() {
    // Doc 15.3 says above 4 GB. An off by one here is a ladder that fires on a
    // crawl that is doing what it was told to do.
    let mut ladder = Backpressure::new();
    let signals = Signals {
        unpublished_bytes: 4 * GB,
        ..calm()
    };
    assert!(ladder.observe(&signals, 0).is_empty());
    assert_eq!(ladder.level(Ladder::Disk), 0);
}

#[test]
fn any_one_of_the_three_disk_signals_is_enough() {
    for (signals, cause) in [
        (
            Signals {
                publish_lag_ms: 21 * MINUTE,
                ..calm()
            },
            Cause::PublishLag,
        ),
        (
            Signals {
                free_disk_bytes: 39 * GB,
                ..calm()
            },
            Cause::FreeDisk,
        ),
        (
            Signals {
                unpublished_bytes: 5 * GB,
                ..calm()
            },
            Cause::Unpublished,
        ),
    ] {
        let mut ladder = Backpressure::new();
        let moved = ladder.observe(&signals, 0);
        assert_eq!(moved.len(), 1, "{cause} did not move the ladder");
        assert_eq!(moved[0].cause, cause);
        assert_eq!(ladder.level(Ladder::Disk), 1);
    }
}

#[test]
fn the_signal_that_asks_hardest_is_the_one_reported() {
    // Unpublished is at rung 1 and free disk is at rung 3. An operator reading
    // the log needs the number that actually moved the ladder.
    let mut ladder = Backpressure::new();
    let signals = Signals {
        unpublished_bytes: 5 * GB,
        free_disk_bytes: 14 * GB,
        ..calm()
    };
    let moved = ladder.observe(&signals, 0);
    assert_eq!(moved[0].to, 3);
    assert_eq!(moved[0].cause, Cause::FreeDisk);
    assert_eq!(moved[0].value, 14 * GB);
}

#[test]
fn level_two_halves_the_leases_and_stops_the_community() {
    let mut ladder = Backpressure::new();
    ladder.observe(
        &Signals {
            unpublished_bytes: 9 * GB,
            ..calm()
        },
        0,
    );
    assert_eq!(ladder.level(Ladder::Disk), 2);

    let allowed = ladder.allowance();
    assert_eq!(allowed.lease_scale, 0.5);
    assert!(!allowed.community_leasing);
    assert!(allowed.prefer_known_t1);
    assert!(allowed.accept_deliveries, "in flight work is never dropped");
}

#[test]
fn level_three_stops_leasing_and_keeps_everything_in_flight() {
    let mut ladder = Backpressure::new();
    ladder.observe(
        &Signals {
            publish_lag_ms: 91 * MINUTE,
            ..calm()
        },
        0,
    );
    assert_eq!(ladder.level(Ladder::Disk), 3);

    let allowed = ladder.allowance();
    assert_eq!(allowed.lease_scale, 0.0);
    assert!(
        allowed.accept_deliveries,
        "the crawl stops growing, nothing in flight is lost"
    );
    assert!(!allowed.seal_open_segments);
}

#[test]
fn only_a_full_disk_reaches_level_four() {
    // Doc 15.3 gives rung 4 one trigger. Publish lag of a day on a box with
    // room to spare is a publishing problem, not an emergency, and refusing
    // deliveries would turn it into one.
    let mut ladder = Backpressure::new();
    ladder.observe(
        &Signals {
            unpublished_bytes: 500 * GB,
            publish_lag_ms: 24 * 60 * MINUTE,
            ..calm()
        },
        0,
    );
    assert_eq!(ladder.level(Ladder::Disk), 3);
    assert!(ladder.allowance().accept_deliveries);

    let mut ladder = Backpressure::new();
    ladder.observe(
        &Signals {
            free_disk_bytes: 4 * GB,
            ..calm()
        },
        0,
    );
    assert_eq!(ladder.level(Ladder::Disk), 4);
    let allowed = ladder.allowance();
    assert!(!allowed.accept_deliveries);
    assert!(allowed.seal_open_segments);
}

#[test]
fn the_ladder_climbs_in_one_step_when_the_disk_goes_in_one_step() {
    // A filesystem that loses 400 GB to somebody else's log file does not
    // walk the ladder up on its way. Going up is immediate and it is immediate
    // to wherever the signals actually are.
    let mut ladder = Backpressure::new();
    let moved = ladder.observe(
        &Signals {
            free_disk_bytes: 3 * GB,
            ..calm()
        },
        0,
    );
    assert_eq!(moved[0].from, 0);
    assert_eq!(moved[0].to, 4);
}

#[test]
fn coming_down_takes_ten_minutes_a_rung() {
    let mut ladder = Backpressure::new();
    ladder.observe(
        &Signals {
            unpublished_bytes: 17 * GB,
            ..calm()
        },
        0,
    );
    assert_eq!(ladder.level(Ladder::Disk), 3);

    // Everything is fine from the tick at 1, and the ten minutes are counted
    // from there rather than from when the ladder went up.
    assert!(ladder.observe(&calm(), 1).is_empty());
    assert!(ladder.observe(&calm(), 9 * MINUTE).is_empty());
    assert!(
        ladder.observe(&calm(), 10 * MINUTE).is_empty(),
        "one millisecond short is still short"
    );

    let moved = ladder.observe(&calm(), 10 * MINUTE + 1);
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].from, 3);
    assert_eq!(moved[0].to, 2);
    assert_eq!(moved[0].cause, Cause::Recovered);

    // And the next rung takes its own ten minutes rather than coming free.
    assert!(ladder.observe(&calm(), 20 * MINUTE).is_empty());
    assert_eq!(ladder.observe(&calm(), 20 * MINUTE + 1)[0].to, 1);
    assert_eq!(ladder.observe(&calm(), 30 * MINUTE + 1)[0].to, 0);
    assert!(ladder.normal());
}

#[test]
fn a_signal_that_comes_back_up_spends_the_ten_minutes() {
    // The rule is ten minutes continuously below. A disk that frees up for
    // nine minutes and fills again has not recovered, and a ladder that
    // counted those nine minutes would oscillate, which is the exact failure
    // the hysteresis exists to prevent.
    let mut ladder = Backpressure::new();
    let pressure = Signals {
        unpublished_bytes: 9 * GB,
        ..calm()
    };
    ladder.observe(&pressure, 0);
    assert_eq!(ladder.level(Ladder::Disk), 2);

    assert!(ladder.observe(&calm(), 9 * MINUTE).is_empty());
    assert!(ladder.observe(&pressure, 9 * MINUTE + 1).is_empty());
    assert_eq!(ladder.level(Ladder::Disk), 2);

    // The clock starts again here, so ten minutes from the last calm tick is
    // not enough and ten minutes from this one is.
    assert!(ladder.observe(&calm(), 10 * MINUTE).is_empty());
    assert!(ladder.observe(&calm(), 19 * MINUTE).is_empty());
    assert_eq!(ladder.observe(&calm(), 20 * MINUTE)[0].to, 1);
}

#[test]
fn a_full_queue_sheds_the_expensive_tiers_before_it_cuts_the_rate() {
    let mut ladder = Backpressure::new();
    let busy = Signals {
        extract_queue: 2001,
        ..calm()
    };
    let moved = ladder.observe(&busy, 0);
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].ladder, Ladder::Cpu);
    assert_eq!(moved[0].cause, Cause::ExtractQueue);
    assert_eq!(moved[0].value, 2001);

    let allowed = ladder.allowance();
    assert_eq!(allowed.max_tier, Tier::Plain);
    assert_eq!(
        allowed.lease_scale, 1.0,
        "the rate is the second answer, not the first"
    );

    // Still busy half a minute later, so the second answer is tried.
    let moved = ladder.observe(&busy, 30_000);
    assert_eq!(moved[0].to, 2);
    assert_eq!(ladder.allowance().lease_scale, 0.5);
}

#[test]
fn a_saturated_pool_has_to_stay_saturated_for_thirty_seconds() {
    // A pool with no idle worker for one tick is a pool that is being used.
    let mut ladder = Backpressure::new();
    let busy = Signals {
        extractor_saturated: true,
        ..calm()
    };
    assert!(ladder.observe(&busy, 0).is_empty());
    assert!(ladder.observe(&busy, 29_999).is_empty());
    let moved = ladder.observe(&busy, 30_000);
    assert_eq!(moved[0].ladder, Ladder::Cpu);
    assert_eq!(moved[0].cause, Cause::Extractor);
    assert_eq!(moved[0].to, 1);
}

#[test]
fn one_idle_worker_resets_the_saturation_clock() {
    let mut ladder = Backpressure::new();
    let busy = Signals {
        extractor_saturated: true,
        ..calm()
    };
    assert!(ladder.observe(&busy, 0).is_empty());
    assert!(ladder.observe(&calm(), 20_000).is_empty());
    assert!(
        ladder.observe(&busy, 21_000).is_empty(),
        "the twenty seconds before the gap were counted"
    );
    assert!(ladder.observe(&busy, 50_000).is_empty());
    assert_eq!(ladder.observe(&busy, 51_000)[0].to, 1);
}

#[test]
fn the_memory_ladder_is_off_without_a_budget() {
    // A caller that does not know its own budget says zero, and the answer to
    // not knowing is to leave the ladder alone rather than throttle a crawl
    // over an invented number.
    let mut ladder = Backpressure::new();
    let signals = Signals {
        rss_bytes: 64 * GB,
        rss_budget_bytes: 0,
        ..calm()
    };
    assert!(ladder.observe(&signals, 0).is_empty());
    assert_eq!(ladder.level(Ladder::Memory), 0);
}

#[test]
fn eighty_five_percent_shrinks_the_working_set_and_ninety_five_stops_leasing() {
    let mut ladder = Backpressure::new();
    let moved = ladder.observe(
        &Signals {
            rss_bytes: 17 * GB,
            rss_budget_bytes: 20 * GB,
            ..calm()
        },
        0,
    );
    assert_eq!(moved[0].ladder, Ladder::Memory);
    assert_eq!(moved[0].cause, Cause::Rss);
    assert_eq!(moved[0].value, 85);
    let allowed = ladder.allowance();
    assert!(allowed.shoal_cap_at_floor);
    assert!(allowed.evict_cold_shards);
    assert_eq!(allowed.lease_scale, 1.0);

    let moved = ladder.observe(
        &Signals {
            rss_bytes: 19 * GB,
            rss_budget_bytes: 20 * GB,
            ..calm()
        },
        1,
    );
    assert_eq!(moved[0].to, 2);
    assert_eq!(ladder.allowance().lease_scale, 0.0);
}

#[test]
fn the_three_ladders_move_independently_and_combine_to_the_strictest() {
    // The case doc 15.3 splits them for. Memory pressure on server1 must not
    // turn off community leasing, and disk pressure must not shrink the shoal
    // cap, but a lease rate cut from either one is a lease rate cut.
    let mut ladder = Backpressure::new();
    let moved = ladder.observe(
        &Signals {
            unpublished_bytes: 5 * GB,
            extract_queue: 3000,
            rss_bytes: 18 * GB,
            rss_budget_bytes: 20 * GB,
            ..calm()
        },
        0,
    );
    assert_eq!(moved.len(), 3, "one transition per ladder, all at once");
    assert_eq!(ladder.level(Ladder::Disk), 1);
    assert_eq!(ladder.level(Ladder::Cpu), 1);
    assert_eq!(ladder.level(Ladder::Memory), 1);
    assert_eq!(ladder.to_string(), "disk 1 cpu 1 memory 1");

    let allowed = ladder.allowance();
    assert_eq!(allowed.max_tier, Tier::Plain, "the cpu rung is stricter");
    assert_eq!(allowed.discovery_share, DISCOVERY_UNDER_PRESSURE);
    assert!(allowed.community_leasing, "disk 1 does not touch that");
    assert!(allowed.shoal_cap_at_floor);
    assert_eq!(allowed.lease_scale, 1.0);
}

#[test]
fn a_transition_reads_as_a_sentence() {
    let mut ladder = Backpressure::new();
    let moved = ladder.observe(
        &Signals {
            free_disk_bytes: 30 * GB,
            ..calm()
        },
        0,
    );
    assert_eq!(
        moved[0].to_string(),
        format!("disk backpressure 0 to 1, free disk {}", 30 * GB)
    );
}

#[test]
fn a_calm_allowance_leaves_doc_09_5s_split_exactly_alone() {
    // The identity case, and the one that would go unnoticed if it broke. An
    // unpressured crawl runs the operator's budget and not an arithmetic
    // approximation of it, so the shares have to come back byte for byte.
    let budget = Allowance::default().budget(Budget::DEFAULT);
    for class in umi_state::CLASSES {
        assert_eq!(
            budget.share(class),
            Budget::DEFAULT.share(class),
            "{class:?} moved under no pressure"
        );
    }
    assert!((DISCOVERY_NORMAL - 0.35).abs() < f32::EPSILON);
}

#[test]
fn pressure_moves_discovery_and_leaves_the_refresh_classes_where_they_were() {
    // Doc 15.3's rung one: 35 percent to 15. The refresh classes are not
    // touched, because the rung is about not taking on new work rather than
    // about refreshing differently, and a crawl that stopped revalidating
    // under disk pressure would publish a staler corpus for no saving.
    let under = Allowance {
        discovery_share: DISCOVERY_UNDER_PRESSURE,
        ..Allowance::default()
    };
    let budget = under.budget(Budget::DEFAULT);
    for class in umi_state::CLASSES {
        if class != RefreshClass::Discovery {
            assert_eq!(budget.share(class), Budget::DEFAULT.share(class));
        }
    }
    // The shares are a ratio, so what is asserted is the fraction of a batch
    // discovery gets rather than the raw number. Doc 15.3 says 15 percent and
    // a whole number of shares cannot hit that exactly, so a point either way
    // is the tolerance.
    let quota = budget.quota(RefreshClass::Discovery, 1000);
    assert!(
        (140..=160).contains(&quota),
        "{quota} of 1000 is not doc 15.3's 15 percent"
    );
}

#[test]
fn discovery_never_goes_to_zero_however_hard_the_ladder_pushes() {
    // Zero is a different instruction. It stops the class outright, and doc
    // 15.3 never asks for that: even rung four is about refusing new bytes
    // from other people rather than about never discovering a url again.
    let starved = Allowance {
        discovery_share: 0.0,
        ..Allowance::default()
    };
    assert!(
        starved
            .budget(Budget::DEFAULT)
            .share(RefreshClass::Discovery)
            >= 1
    );
}

#[test]
fn a_budget_that_is_all_discovery_is_left_alone() {
    // Scaling discovery against nothing has no answer, and a caller who wrote
    // that budget asked for a crawl that does nothing but discover. Changing
    // it would be inventing refresh classes the operator switched off.
    let only = Budget::new([0, 0, 0, 0, 0, 100]);
    let under = Allowance {
        discovery_share: DISCOVERY_UNDER_PRESSURE,
        ..Allowance::default()
    };
    assert_eq!(under.budget(only).share(RefreshClass::Discovery), 100);
}
