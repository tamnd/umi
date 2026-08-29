use umi_types::{OutcomeCode, Tier};

use crate::encode::encode;
use crate::labels::{DiskRole, Ladder, PublishStep};
use crate::registry::{Metrics, PEER_CAP};

/// Doc 15.4's list, copied out verbatim. This is the test that fails when
/// somebody renames a series, which is the change a dashboard notices.
const DOC_15_4: [&str; 29] = [
    "umi_pages_fetched_total",
    "umi_fetch_duration_seconds",
    "umi_bytes_in_total",
    "umi_bytes_out_total",
    "umi_frontier_size",
    "umi_admit_total",
    "umi_shard_miss_total",
    "umi_state_bytes",
    "umi_state_op_duration_seconds",
    "umi_hosts_backing_off",
    "umi_robots_fetch_total",
    "umi_render_pool_busy",
    "umi_extract_queue_depth",
    "umi_extract_duration_seconds",
    "umi_segments_unpublished",
    "umi_unpublished_bytes",
    "umi_publish_lag_seconds",
    "umi_publish_duration_seconds",
    "umi_publish_failures_total",
    "umi_disk_free_bytes",
    "umi_backpressure_level",
    "umi_fetchers_connected",
    "umi_fetcher_reputation",
    "umi_verify_total",
    "umi_verify_disagreement_ratio",
    "umi_quarantine_size",
    "umi_dns_duration_seconds",
    "umi_dns_failures_total",
    "umi_peer_lag_seconds",
];

#[test]
fn every_series_doc_15_4_names_is_in_the_scrape() {
    let scrape = encode(&Metrics::new());
    for name in DOC_15_4 {
        assert!(
            scrape.contains(&format!("# TYPE {name} ")),
            "{name} is in doc 15.4 and not in the output"
        );
    }
}

/// Nothing beyond doc 15.4 either. An extra series is not free: it is a line in
/// every scrape forever and a thing a reader has to decide to ignore.
#[test]
fn nothing_beyond_doc_15_4_is_in_the_scrape() {
    let scrape = encode(&Metrics::new());
    for line in scrape.lines() {
        let Some(rest) = line.strip_prefix("# TYPE ") else {
            continue;
        };
        let name = rest.split_whitespace().next().expect("a name follows TYPE");
        assert!(
            DOC_15_4.contains(&name),
            "{name} is in the output and not in doc 15.4"
        );
    }
}

/// A fresh registry emits every series at zero rather than emitting nothing.
/// `rate()` over an absent series returns nothing at all, so a panel built on
/// one stays blank until the first event instead of showing a flat line.
#[test]
fn a_registry_that_has_seen_nothing_still_reports_everything() {
    let scrape = encode(&Metrics::new());
    let series = scrape.lines().filter(|line| !line.starts_with('#')).count();
    // Every label combination except the peers, which have no names yet.
    assert!(
        series > 400,
        "{series} lines reads like a family that did not print"
    );
    assert!(
        scrape.contains("umi_pages_fetched_total{tier=\"T4\",outcome=\"blocked\"} 0"),
        "an untouched combination should still be there"
    );
}

/// The whole cardinality claim in one assertion. Every counter in the process
/// gets hammered with every label value it has, a thousand peers report, and
/// the output does not grow past the fixed shape.
#[test]
fn hammering_every_label_does_not_grow_the_output() {
    let metrics = Metrics::new();
    let before = encode(&metrics).lines().count();

    for tier in Tier::ALL {
        for outcome in OutcomeCode::ALL {
            metrics.pages_fetched().get(tier, outcome).inc();
        }
        metrics.fetch_duration().get(tier).observe(0.2);
    }
    for step in PublishStep::ALL {
        metrics.publish_failures().get(*step).inc();
    }
    for peer in 0..1_000 {
        metrics.peer_lag().set(&format!("peer-{peer}"), 1.0);
    }

    let after = encode(&metrics).lines().count();
    assert_eq!(
        after,
        before + PEER_CAP,
        "only the peers, and only up to the cap, add lines"
    );
}

/// The bucket, sum and count lines of a labelled histogram, in the shape the
/// exposition format wants, with the label and `le` in one set of braces.
#[test]
fn a_labelled_histogram_writes_its_labels_and_its_bounds_together() {
    let metrics = Metrics::new();
    metrics.fetch_duration().get(Tier::Emulated).observe(0.3);
    let scrape = encode(&metrics);

    assert!(scrape.contains("umi_fetch_duration_seconds_bucket{tier=\"T2\",le=\"0.25\"} 0"));
    assert!(scrape.contains("umi_fetch_duration_seconds_bucket{tier=\"T2\",le=\"0.5\"} 1"));
    assert!(scrape.contains("umi_fetch_duration_seconds_bucket{tier=\"T2\",le=\"+Inf\"} 1"));
    assert!(scrape.contains("umi_fetch_duration_seconds_sum{tier=\"T2\"} 0.3"));
    assert!(scrape.contains("umi_fetch_duration_seconds_count{tier=\"T2\"} 1"));
}

/// An unlabelled histogram writes no empty braces. `name_sum{} 0` parses, and
/// it reads like a bug to anyone looking at the raw output.
#[test]
fn an_unlabelled_histogram_writes_no_empty_braces() {
    let metrics = Metrics::new();
    metrics.dns_duration().observe(0.02);
    let scrape = encode(&metrics);

    assert!(scrape.contains("umi_dns_duration_seconds_bucket{le=\"0.025\"} 1"));
    assert!(scrape.contains("umi_dns_duration_seconds_sum 0.02"));
    assert!(scrape.contains("umi_dns_duration_seconds_count 1"));
    assert!(
        !scrape.contains("{}"),
        "no family should print empty braces"
    );
}

/// Bounds come out as the short decimal a person expects rather than as the
/// nearest double spelled in full.
#[test]
fn bucket_bounds_are_not_written_as_binary_floats() {
    let scrape = encode(&Metrics::new());
    assert!(
        scrape.contains("le=\"0.00001\""),
        "the fine bounds are short"
    );
    assert!(!scrape.contains("0000000001"), "no double came out raw");
}

/// The `{path}` label is a role, so a reconfigured box does not leave dead
/// series behind named after directories.
#[test]
fn disk_free_is_labelled_by_role_and_not_by_path() {
    let metrics = Metrics::new();
    metrics.disk_free().get(DiskRole::Segments).set(387 << 30);
    let scrape = encode(&metrics);
    assert!(scrape.contains(&format!(
        "umi_disk_free_bytes{{path=\"segments\"}} {}",
        387u64 << 30
    )));
    assert!(!scrape.contains('/'), "no path should reach the output");
}

/// Doc 15.6's second alarm reads `umi_backpressure_level` across the three
/// ladders, so all three have to be there whether or not anything is wrong.
#[test]
fn the_three_ladders_report_even_when_calm() {
    let metrics = Metrics::new();
    metrics.backpressure().get(Ladder::Disk).set(3);
    let scrape = encode(&metrics);
    assert!(scrape.contains("umi_backpressure_level{ladder=\"disk\"} 3"));
    assert!(scrape.contains("umi_backpressure_level{ladder=\"cpu\"} 0"));
    assert!(scrape.contains("umi_backpressure_level{ladder=\"memory\"} 0"));
}

/// A peer name out of a config file is not trusted to be well behaved.
#[test]
fn a_peer_name_with_a_quote_in_it_still_parses() {
    let metrics = Metrics::new();
    metrics.peer_lag().set("odd\"name\\here", 2.5);
    let scrape = encode(&metrics);
    assert!(scrape.contains("umi_peer_lag_seconds{peer=\"odd\\\"name\\\\here\"} 2.5"));
}

/// Every line is either a comment or a sample, and every sample has a name, one
/// space and a value. Weaker than a real parser and strong enough to catch a
/// missing newline or a stray brace anywhere in the twenty nine families.
#[test]
fn the_whole_scrape_is_shaped_like_the_exposition_format() {
    let metrics = Metrics::new();
    metrics.peer_lag().set("server1", 0.5);
    let scrape = encode(&metrics);
    assert!(scrape.ends_with('\n'));

    for line in scrape.lines() {
        if line.starts_with("# HELP ") || line.starts_with("# TYPE ") {
            continue;
        }
        assert!(!line.starts_with('#'), "unexpected comment: {line}");
        let (name, value) = line.rsplit_once(' ').unwrap_or_else(|| {
            panic!("sample line has no value: {line}");
        });
        assert!(!name.is_empty(), "sample line has no name: {line}");
        assert_eq!(
            name.matches('{').count(),
            name.matches('}').count(),
            "unbalanced braces: {line}"
        );
        assert!(
            value.parse::<f64>().is_ok(),
            "value is not a number: {line}"
        );
    }
}
