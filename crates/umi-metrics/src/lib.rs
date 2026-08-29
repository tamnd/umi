//! The numbers `docs/spec/15-operations.md` says somebody will act on.
//!
//! Doc 15.4 lists about thirty series and says they were chosen because someone
//! will act on them. This crate is that list, one struct, with the counters and
//! gauges behind it and the Prometheus text format on top. It holds no socket
//! and starts no thread: serving the scrape is the caller's job, because the
//! admin listener belongs to the process that has one and doc 14 says it is
//! localhost only by default.
//!
//! ```
//! use umi_metrics::{AdmitResult, Metrics, encode};
//! use umi_types::{OutcomeCode, Tier};
//!
//! let metrics = Metrics::new();
//! metrics.pages_fetched().get(Tier::Plain, OutcomeCode::Ok).inc();
//! metrics.fetch_duration().get(Tier::Plain).observe(0.184);
//! metrics.admit().get(AdmitResult::Admitted).add(37);
//!
//! let scrape = encode(&metrics);
//! assert!(scrape.contains("umi_pages_fetched_total{tier=\"T1\",outcome=\"ok\"} 1"));
//! ```
//!
//! # Cardinality
//!
//! The done-when on this work is that a crawl of a million domains does not
//! produce a million label values, and the answer is that there is no way to
//! write that crawl. Every label is a closed enum, there is no function
//! anywhere that takes a label as a string, and the number of series is a
//! product of variant counts that the compiler could work out. It comes to
//! roughly five hundred lines of scrape, which is a few tens of kilobytes.
//!
//! Two labels in doc 15.4 look unbounded on the page. `{path}` becomes
//! [`DiskRole`], because an alert is written about the disk the segments land
//! on and not about `/var/lib/umi`, and a box that gets reconfigured twice
//! should not leave a trail of series named after directories that no longer
//! exist. `{peer}` stays a string, because peer names come from the config
//! file, and is capped at [`PEER_CAP`] instead.
//!
//! # Every alarm in doc 15.6
//!
//! The other half of the done-when is that doc 15.6's five alarms are all
//! expressible against these series without adding more.
//!
//! Publish lag above 90 minutes is `umi_publish_lag_seconds > 5400`.
//! Backpressure at level 3 or above for more than 15 minutes is
//! `max(umi_backpressure_level) >= 3` held for `15m`. A verification
//! disagreement ratio doubling week over week is
//! `umi_verify_disagreement_ratio > 2 * (umi_verify_disagreement_ratio offset
//! 7d)`. A coordinator unreachable for more than an hour is
//! `umi_peer_lag_seconds > 3600`. The fifth, a manifest chain break, is
//! `increase(umi_publish_failures_total{step="manifest"}[15m]) > 0`, which is
//! the one that needed a label rather than a series of its own: doc 12.5's
//! chain is checked as part of step 6, so a break is a failure of that step and
//! nothing else can make that step fail.
//!
//! # What it costs
//!
//! `benches/scrape.rs` measures the two things that matter, and on server3 the
//! answer to both is that it does not matter. Everything one fetched page
//! touches, which is six counters, two histograms and a gauge, comes to 72 ns,
//! against the 4 ms a page gets at gate 1.1's rate. That is under a five
//! hundredth of one percent. With four cores writing the same counters at once
//! it is 254 ns, because a relaxed add to a shared cache line is not free, and
//! that is still under a hundredth of a percent.
//!
//! A full render of all 593 series is 33 KB and 0.137 ms. At one scrape every
//! fifteen seconds that is a millionth of a core.

pub mod encode;
pub mod labels;
pub mod metric;
pub mod registry;

#[cfg(test)]
mod encode_tests;
#[cfg(test)]
mod registry_tests;

pub use encode::encode;
pub use labels::{
    AdmitResult, DiskRole, FetcherState, FrontierState, Label, Ladder, PublishStep, RobotsResult,
    StateOp, VerifyLayer, VerifyResult,
};
pub use metric::{
    BUCKETS, Counter, FloatGauge, Gauge, Histogram, SECONDS_FINE, SECONDS_WIDE, UNIT,
};
pub use registry::{Family, Family2, Metrics, PEER_CAP, Peers};
