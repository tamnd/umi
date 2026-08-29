//! The Prometheus text exposition format, version 0.0.4.
//!
//! One `String`, built on scrape and thrown away. Nothing here runs on a crawl
//! path, so it is written for a human reading the output with `curl` rather
//! than for speed, and `benches/scrape.rs` says a full render is under a
//! millisecond, which against a fifteen second scrape interval is nothing.
//!
//! Every series is emitted every time, including the ones still at zero. An
//! absent series and a series at zero read the same to a person and completely
//! differently to `rate()`, which cannot tell a counter that has not moved from
//! a counter that has not been created and so returns nothing at all for the
//! second one. Doc 15.6's alarms are rate queries, so a dashboard that goes
//! blank until the first failure is a dashboard that stays blank.

use std::fmt::Write;

use crate::labels::Label;
use crate::metric::{Counter, FloatGauge, Gauge, Histogram};
use crate::registry::{Family, Family2, Metrics};

/// Render everything, ready to hand back to a scrape.
#[must_use]
pub fn encode(metrics: &Metrics) -> String {
    // Roughly what a full render measures, so the common case is one
    // allocation. Being wrong costs a realloc and nothing else.
    let mut out = String::with_capacity(48 * 1024);

    family2_counter(
        &mut out,
        "umi_pages_fetched_total",
        "Pages fetched, by tier and outcome.",
        metrics.pages_fetched(),
    );
    family_histogram(
        &mut out,
        "umi_fetch_duration_seconds",
        "Time from request to last byte, by tier.",
        metrics.fetch_duration(),
    );
    counter(
        &mut out,
        "umi_bytes_in_total",
        "Bytes read off the network.",
        metrics.bytes_in(),
    );
    counter(
        &mut out,
        "umi_bytes_out_total",
        "Bytes written to the network, requests and uploads.",
        metrics.bytes_out(),
    );
    family_gauge(
        &mut out,
        "umi_frontier_size",
        "URLs the scheduler knows about, by state.",
        metrics.frontier_size(),
    );
    family_counter(
        &mut out,
        "umi_admit_total",
        "Candidate URLs offered to the state, by what became of them.",
        metrics.admit(),
    );
    counter(
        &mut out,
        "umi_shard_miss_total",
        "Leases that wanted a shard which was not resident.",
        metrics.shard_miss(),
    );
    gauge(
        &mut out,
        "umi_state_bytes",
        "Size of the state on disk.",
        metrics.state_bytes(),
    );
    family_histogram(
        &mut out,
        "umi_state_op_duration_seconds",
        "Time for one state operation, by operation.",
        metrics.state_op_duration(),
    );
    gauge(
        &mut out,
        "umi_hosts_backing_off",
        "Hosts the pacer is currently holding off.",
        metrics.hosts_backing_off(),
    );
    family_counter(
        &mut out,
        "umi_robots_fetch_total",
        "robots.txt fetches, by result.",
        metrics.robots_fetch(),
    );
    gauge(
        &mut out,
        "umi_render_pool_busy",
        "Render slots in use.",
        metrics.render_pool_busy(),
    );
    gauge(
        &mut out,
        "umi_extract_queue_depth",
        "Fetched pages waiting to be extracted.",
        metrics.extract_queue_depth(),
    );
    scalar_histogram(
        &mut out,
        "umi_extract_duration_seconds",
        "Time to extract one page.",
        metrics.extract_duration(),
    );
    gauge(
        &mut out,
        "umi_segments_unpublished",
        "Sealed segments that have not reached Hugging Face.",
        metrics.segments_unpublished(),
    );
    gauge(
        &mut out,
        "umi_unpublished_bytes",
        "Bytes held in those segments.",
        metrics.unpublished_bytes(),
    );
    float_gauge(
        &mut out,
        "umi_publish_lag_seconds",
        "Age of the oldest segment that has not been published.",
        metrics.publish_lag(),
    );
    family_histogram(
        &mut out,
        "umi_publish_duration_seconds",
        "Time for one step of the publish pipeline.",
        metrics.publish_duration(),
    );
    family_counter(
        &mut out,
        "umi_publish_failures_total",
        "Publish steps that failed, by step.",
        metrics.publish_failures(),
    );
    family_gauge(
        &mut out,
        "umi_disk_free_bytes",
        "Free space, by what the directory is used for.",
        metrics.disk_free(),
    );
    family_gauge(
        &mut out,
        "umi_backpressure_level",
        "Which rung of the backpressure ladder is engaged, zero when calm.",
        metrics.backpressure(),
    );
    family_gauge(
        &mut out,
        "umi_fetchers_connected",
        "Fetchers on the protocol, by what they are doing.",
        metrics.fetchers_connected(),
    );
    scalar_histogram(
        &mut out,
        "umi_fetcher_reputation",
        "Reputation across connected fetchers, between zero and one.",
        metrics.fetcher_reputation(),
    );
    family2_counter(
        &mut out,
        "umi_verify_total",
        "Verification decisions, by layer and outcome.",
        metrics.verify(),
    );
    float_gauge(
        &mut out,
        "umi_verify_disagreement_ratio",
        "Share of verified deliveries where two sources did not agree.",
        metrics.disagreement_ratio(),
    );
    gauge(
        &mut out,
        "umi_quarantine_size",
        "Rows held for review rather than published.",
        metrics.quarantine_size(),
    );
    scalar_histogram(
        &mut out,
        "umi_dns_duration_seconds",
        "Time for one DNS resolution.",
        metrics.dns_duration(),
    );
    counter(
        &mut out,
        "umi_dns_failures_total",
        "DNS resolutions that did not answer.",
        metrics.dns_failures(),
    );
    peers(&mut out, metrics);

    out
}

/// The two comment lines that open a family.
fn head(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn counter(out: &mut String, name: &str, help: &str, metric: &Counter) {
    head(out, name, "counter", help);
    let _ = writeln!(out, "{name} {}", metric.get());
}

fn gauge(out: &mut String, name: &str, help: &str, metric: &Gauge) {
    head(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {}", metric.get());
}

fn float_gauge(out: &mut String, name: &str, help: &str, metric: &FloatGauge) {
    head(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {}", number(metric.get()));
}

fn family_counter<L: Label>(out: &mut String, name: &str, help: &str, family: &Family<L, Counter>) {
    head(out, name, "counter", help);
    for (label, metric) in family.iter() {
        let _ = writeln!(
            out,
            "{name}{{{}=\"{}\"}} {}",
            L::KEY,
            label.value(),
            metric.get()
        );
    }
}

fn family_gauge<L: Label>(out: &mut String, name: &str, help: &str, family: &Family<L, Gauge>) {
    head(out, name, "gauge", help);
    for (label, metric) in family.iter() {
        let _ = writeln!(
            out,
            "{name}{{{}=\"{}\"}} {}",
            L::KEY,
            label.value(),
            metric.get()
        );
    }
}

fn family2_counter<A: Label, B: Label>(
    out: &mut String,
    name: &str,
    help: &str,
    family: &Family2<A, B, Counter>,
) {
    head(out, name, "counter", help);
    for (a, b, metric) in family.iter() {
        let _ = writeln!(
            out,
            "{name}{{{}=\"{}\",{}=\"{}\"}} {}",
            A::KEY,
            a.value(),
            B::KEY,
            b.value(),
            metric.get()
        );
    }
}

fn scalar_histogram(out: &mut String, name: &str, help: &str, metric: &Histogram) {
    head(out, name, "histogram", help);
    buckets(out, name, "", metric);
}

fn family_histogram<L: Label>(
    out: &mut String,
    name: &str,
    help: &str,
    family: &Family<L, Histogram>,
) {
    head(out, name, "histogram", help);
    for (label, metric) in family.iter() {
        let labels = format!("{}=\"{}\"", L::KEY, label.value());
        buckets(out, name, &labels, metric);
    }
}

/// The bucket, sum and count lines for one histogram.
///
/// `labels` is already rendered and may be empty, which is why the brace
/// handling is done by hand rather than with a join.
fn buckets(out: &mut String, name: &str, labels: &str, metric: &Histogram) {
    let separator = if labels.is_empty() { "" } else { "," };
    for (bound, count) in metric.cumulative() {
        let _ = writeln!(
            out,
            "{name}_bucket{{{labels}{separator}le=\"{}\"}} {count}",
            number(bound)
        );
    }
    // The `+Inf` bucket is everything, which is what the count already is.
    let _ = writeln!(
        out,
        "{name}_bucket{{{labels}{separator}le=\"+Inf\"}} {}",
        metric.count()
    );
    let braces = if labels.is_empty() {
        String::new()
    } else {
        format!("{{{labels}}}")
    };
    let _ = writeln!(out, "{name}_sum{braces} {}", number(metric.sum()));
    let _ = writeln!(out, "{name}_count{braces} {}", metric.count());
}

fn peers(out: &mut String, metrics: &Metrics) {
    head(
        out,
        "umi_peer_lag_seconds",
        "gauge",
        "How far behind each coordinator is.",
    );
    for (peer, seconds) in metrics.peer_lag().snapshot() {
        let _ = writeln!(
            out,
            "umi_peer_lag_seconds{{peer=\"{}\"}} {}",
            escape(&peer),
            number(seconds)
        );
    }
}

/// A float the way the exposition format wants it.
///
/// Rust's own formatting is already the shortest string that reads back as the
/// same double, so 0.005 stays `0.005` rather than becoming `0.005000000001`.
/// The three special values have their own spellings in the format and do not
/// come out of `Display` that way.
fn number(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    value.to_string()
}

/// A label value with the three characters the format reserves taken out.
///
/// Only [`peers`] needs this. Every other label value in this crate comes from
/// an enum and is a Rust identifier spelled in lower case, but a peer name
/// comes from a config file and a backslash in one should produce a scrape that
/// still parses.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}
