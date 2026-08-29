use umi_types::{OutcomeCode, Tier};

use crate::labels::{AdmitResult, FrontierState, Label, PublishStep, VerifyLayer, VerifyResult};
use crate::metric::{Counter, Histogram, SECONDS_WIDE};
use crate::registry::{Metrics, PEER_CAP, Peers};

/// The two label enums that come from `umi-types` are indexed by their
/// discriminant, so this checks that the discriminant order and the `ALL`
/// order are the same thing. If somebody reorders `ALL` without reordering the
/// enum, every tier in the dashboard silently shifts by one.
#[test]
fn the_borrowed_label_enums_index_themselves_correctly() {
    for (index, tier) in Tier::ALL.iter().enumerate() {
        assert_eq!(tier.index(), index);
        assert_eq!(<Tier as Label>::from_index(index), *tier);
    }
    for (index, outcome) in OutcomeCode::ALL.iter().enumerate() {
        assert_eq!(outcome.index(), index);
        assert_eq!(<OutcomeCode as Label>::from_index(index), *outcome);
    }
}

/// Same check for the enums declared here, driven through the macro's `ALL`.
#[test]
fn every_label_value_round_trips_through_its_index() {
    fn check<L: Label + PartialEq + std::fmt::Debug>(all: &[L]) {
        assert_eq!(all.len(), L::COUNT);
        for (index, label) in all.iter().enumerate() {
            assert_eq!(label.index(), index);
            assert_eq!(L::from_index(index), *label);
        }
    }
    check(FrontierState::ALL);
    check(AdmitResult::ALL);
    check(PublishStep::ALL);
    check(VerifyLayer::ALL);
    check(VerifyResult::ALL);
}

/// Two label values are two separate counters. Sounds obvious, and an index
/// arithmetic mistake in a two label family is exactly the bug that would make
/// it not so.
#[test]
fn a_two_label_family_keeps_its_pairs_apart() {
    let metrics = Metrics::new();
    metrics
        .pages_fetched()
        .get(Tier::Plain, OutcomeCode::Ok)
        .add(7);
    metrics
        .pages_fetched()
        .get(Tier::Rendered, OutcomeCode::Ok)
        .add(3);

    assert_eq!(
        metrics
            .pages_fetched()
            .get(Tier::Plain, OutcomeCode::Ok)
            .get(),
        7
    );
    assert_eq!(
        metrics
            .pages_fetched()
            .get(Tier::Rendered, OutcomeCode::Ok)
            .get(),
        3
    );

    let touched: u64 = metrics
        .pages_fetched()
        .iter()
        .map(|(_, _, counter)| counter.get())
        .sum();
    assert_eq!(touched, 10, "nothing else should have moved");
}

/// Walking the pairs gives back the pairs that were written, in the order the
/// two `ALL` lists imply.
#[test]
fn walking_a_two_label_family_recovers_both_labels() {
    let metrics = Metrics::new();
    metrics
        .verify()
        .get(VerifyLayer::Canary, VerifyResult::Ban)
        .inc();

    let found: Vec<_> = metrics
        .verify()
        .iter()
        .filter(|(_, _, counter)| counter.get() > 0)
        .map(|(layer, result, _)| (layer, result))
        .collect();
    assert_eq!(found, vec![(VerifyLayer::Canary, VerifyResult::Ban)]);
}

/// A counter at the top does not wrap. Wrapping would show up in Prometheus as
/// a process restart, and a restart that did not happen is worse than a number
/// that stops.
#[test]
fn a_counter_at_the_ceiling_stays_there() {
    let counter = Counter::new();
    counter.add(u64::MAX);
    counter.inc();
    assert_eq!(counter.get(), u64::MAX);
}

/// Buckets are cumulative, so each one carries everything below it, and the
/// count carries the ones above the top bound as well.
#[test]
fn a_histogram_counts_upwards_and_keeps_the_overflow() {
    let histogram = Histogram::new(SECONDS_WIDE);
    histogram.observe(0.004);
    histogram.observe(0.4);
    histogram.observe(9_000.0);

    let cumulative = histogram.cumulative();
    assert_eq!(cumulative[0].1, 1, "the 5 ms bucket holds the first one");
    assert_eq!(cumulative[6].1, 2, "the 500 ms bucket holds two");
    assert_eq!(
        cumulative[cumulative.len() - 1].1,
        2,
        "the top bound does not hold the one above it"
    );
    assert_eq!(histogram.count(), 3, "but the count does");
    assert!((histogram.sum() - 9_000.404).abs() < 0.001);
}

/// A value exactly on a bound belongs to that bound, because the exposition
/// format's `le` is less than or equal.
#[test]
fn a_value_on_the_bound_lands_in_that_bucket() {
    let histogram = Histogram::new(SECONDS_WIDE);
    histogram.observe(SECONDS_WIDE[3]);
    assert_eq!(histogram.cumulative()[3].1, 1);
    assert_eq!(histogram.cumulative()[2].1, 0);
}

/// The peer set is the one runtime string label in the crate, so this is the
/// test that stands in for the whole cardinality argument: a thousand names
/// produce eight series.
#[test]
fn a_thousand_peers_produce_at_most_the_cap() {
    let peers = Peers::new();
    for index in 0..1_000 {
        peers.set(&format!("coordinator-{index}"), index as f64);
    }
    assert_eq!(peers.snapshot().len(), PEER_CAP);
}

/// A name already known keeps its slot rather than taking a second one, which
/// is what makes a heartbeat every few seconds safe.
#[test]
fn a_peer_that_reports_twice_has_one_series() {
    let peers = Peers::new();
    peers.set("server3", 4.0);
    peers.set("server3", 11.5);
    let snapshot = peers.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert!((snapshot[0].1 - 11.5).abs() < f64::EPSILON);
}

/// Handing the registry across threads is the normal case, since the fetch loop
/// and the scrape are never on the same one.
#[test]
fn counters_survive_being_incremented_from_several_threads() {
    let metrics = std::sync::Arc::new(Metrics::new());
    let mut handles = Vec::new();
    for _ in 0..4 {
        let metrics = std::sync::Arc::clone(&metrics);
        handles.push(std::thread::spawn(move || {
            for _ in 0..10_000 {
                metrics.bytes_in().add(6_000);
                metrics
                    .pages_fetched()
                    .get(Tier::Plain, OutcomeCode::Ok)
                    .inc();
            }
        }));
    }
    for handle in handles {
        handle.join().expect("no thread should panic");
    }
    assert_eq!(metrics.bytes_in().get(), 4 * 10_000 * 6_000);
    assert_eq!(
        metrics
            .pages_fetched()
            .get(Tier::Plain, OutcomeCode::Ok)
            .get(),
        40_000
    );
}
