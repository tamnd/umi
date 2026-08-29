//! What measuring costs, against gate 1.1's 250 pages a second.
//!
//! Two questions, and they have nothing to do with each other. The first is
//! what a crawl pays to keep the numbers, which lands on the fetch path and has
//! to be nothing. The second is what a scrape costs, which lands once every
//! fifteen seconds on a thread nobody is waiting on and only has to be sane.
//!
//! Gate 1.1 is 250 pages a second on one server. A page that touches a handful
//! of counters and two histograms has to pay for them out of the 4 ms it gets,
//! and the interesting number is not the nanoseconds, it is the share.
//!
//! Run it pinned, since an unpinned run on a machine that is also crawling
//! measures the scheduler:
//!
//! ```text
//! taskset -c 5 chrt --fifo 50 ./target/release/deps/scrape-<hash> --bench
//! ```

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use umi_metrics::{
    AdmitResult, DiskRole, FrontierState, Ladder, Metrics, PublishStep, StateOp, VerifyLayer,
    VerifyResult, encode,
};
use umi_types::{OutcomeCode, Tier};

/// Doc 03's per server rate, which is what every share below is against.
const PAGES_PER_SECOND: f64 = 250.0;

/// How long one page gets on one core at that rate.
const PAGE_BUDGET: Duration = Duration::from_nanos(1_000_000_000 / 250);

/// Enough iterations that the timer resolution is not the measurement.
const CALLS: usize = 5_000_000;

/// One measurement: how long, over how many items.
#[derive(Clone, Copy)]
struct Run {
    elapsed: Duration,
    items: usize,
}

impl Run {
    fn per_item(self) -> Duration {
        self.elapsed / u32::try_from(self.items).unwrap_or(u32::MAX)
    }
}

/// Best of `n`, because the worst case on a shared machine is the scheduler and
/// the best case is the code.
fn best(n: usize, mut body: impl FnMut() -> usize) -> Run {
    let mut best = Run {
        elapsed: Duration::MAX,
        items: 1,
    };
    for _ in 0..n {
        let at = Instant::now();
        let items = body();
        let elapsed = at.elapsed();
        if elapsed < best.elapsed {
            best = Run { elapsed, items };
        }
    }
    best
}

/// One row of the write path table: nanoseconds, and the share of a page.
fn line(name: &str, run: Run) {
    let per = run.per_item().as_secs_f64();
    println!(
        "{name:<40}{:>12.1}{:>16.4}",
        per * 1e9,
        per / PAGE_BUDGET.as_secs_f64() * 100.0
    );
}

fn main() {
    println!("doc 15.4's metrics, against gate 1.1's {PAGES_PER_SECOND} pages a second\n");
    println!(
        "one page gets {:.0} us on one core, so a metric costing 100 ns is 0.0025 percent of it",
        PAGE_BUDGET.as_secs_f64() * 1e6
    );
    println!();

    write_path();
    println!();
    contention();
    println!();
    render();
}

/// Part 1. What the fetch path pays.
fn write_path() {
    let metrics = Metrics::new();

    println!("part 1: the write path, single threaded");
    println!("{:<40}{:>12}{:>16}", "call", "ns/call", "% of a page");

    line(
        "counter, no label",
        best(5, || {
            for _ in 0..CALLS {
                black_box(metrics.bytes_in()).add(6_000);
            }
            CALLS
        }),
    );
    line(
        "counter, one label",
        best(5, || {
            for _ in 0..CALLS {
                black_box(metrics.admit().get(AdmitResult::Admitted)).inc();
            }
            CALLS
        }),
    );
    line(
        "counter, two labels",
        best(5, || {
            for _ in 0..CALLS {
                black_box(metrics.pages_fetched().get(Tier::Plain, OutcomeCode::Ok)).inc();
            }
            CALLS
        }),
    );
    line(
        "gauge set",
        best(5, || {
            for _ in 0..CALLS {
                black_box(metrics.frontier_size().get(FrontierState::Pending)).set(2_400_000);
            }
            CALLS
        }),
    );
    line(
        "histogram, first bucket",
        best(5, || {
            for _ in 0..CALLS {
                black_box(metrics.state_op_duration().get(StateOp::Lease)).observe(0.000_004);
            }
            CALLS
        }),
    );
    line(
        "histogram, last bucket",
        best(5, || {
            for _ in 0..CALLS {
                black_box(metrics.fetch_duration().get(Tier::Plain)).observe(28.0);
            }
            CALLS
        }),
    );
    line(
        "everything one page touches",
        best(5, || {
            for _ in 0..CALLS {
                one_page(&metrics);
            }
            CALLS
        }),
    );
}

/// Everything a single fetched page moves, which is the number that matters.
///
/// Six counters, two histograms and a gauge. A page that is leased, fetched,
/// timed, extracted, counted by tier and outcome, and whose links are offered
/// back to the frontier, touches this much and no more.
fn one_page(metrics: &Metrics) {
    metrics
        .pages_fetched()
        .get(Tier::Plain, OutcomeCode::Ok)
        .inc();
    metrics.fetch_duration().get(Tier::Plain).observe(0.184);
    metrics.bytes_in().add(150 * 1024);
    metrics.bytes_out().add(512);
    metrics
        .state_op_duration()
        .get(StateOp::Lease)
        .observe(0.000_02);
    metrics.extract_duration().observe(0.0011);
    metrics.admit().get(AdmitResult::Seen).add(50);
    metrics.admit().get(AdmitResult::Admitted).add(11);
    metrics
        .frontier_size()
        .get(FrontierState::Pending)
        .set(2_400_000);
}

/// Part 2. What the same work costs with every core doing it.
///
/// A relaxed atomic add is cheap alone and is not cheap when eight cores are
/// adding to the same cache line. The crawl has one counter per tier and
/// outcome pair, and every thread fetching a normal page hits the same one, so
/// this is the shape the real thing has.
fn contention() {
    println!("part 2: the same page's worth of metrics, on N threads at once");
    println!("{:<40}{:>12}{:>16}", "threads", "ns/page", "% of a page");

    let cores = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4);
    let mut threads = vec![1usize, 2, 4];
    if cores > 4 {
        threads.push(cores);
    }

    // Fewer per thread than part 1, since the point is the interference and not
    // the throughput, and every thread has to still be running when the last
    // one starts.
    const PER_THREAD: usize = 500_000;

    for count in threads {
        let metrics = Arc::new(Metrics::new());
        let run = best(3, || {
            let mut handles = Vec::with_capacity(count);
            for _ in 0..count {
                let metrics = Arc::clone(&metrics);
                handles.push(std::thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        one_page(&metrics);
                    }
                }));
            }
            for handle in handles {
                handle.join().expect("no thread should panic");
            }
            count * PER_THREAD
        });
        line(&format!("{count}"), run);
    }
}

/// Part 3. What a scrape costs.
fn render() {
    println!("part 3: one full render, every series at every label value");

    let metrics = Metrics::new();
    // A registry that has been running a while, so no bucket, counter or peer
    // is at a value that formats shorter than a real one would.
    for tier in Tier::ALL {
        for outcome in OutcomeCode::ALL {
            metrics.pages_fetched().get(tier, outcome).add(918_273_645);
        }
        metrics.fetch_duration().get(tier).observe(0.184_926_1);
    }
    for op in StateOp::ALL {
        metrics.state_op_duration().get(*op).observe(0.000_123_4);
    }
    for step in PublishStep::ALL {
        metrics.publish_duration().get(*step).observe(12.5);
        metrics.publish_failures().get(*step).add(3);
    }
    for layer in VerifyLayer::ALL {
        for result in VerifyResult::ALL {
            metrics.verify().get(*layer, *result).add(4_096);
        }
    }
    for role in DiskRole::ALL {
        metrics.disk_free().get(*role).set(387 << 30);
    }
    for ladder in Ladder::ALL {
        metrics.backpressure().get(*ladder).set(1);
    }
    for peer in ["server1", "server2", "server3"] {
        metrics.peer_lag().set(peer, 3.75);
    }
    metrics.publish_lag().set(41.5);
    metrics.disagreement_ratio().set(0.0031);

    let bytes = encode(&metrics).len();
    let lines = encode(&metrics).lines().count();
    let run = best(20, || {
        black_box(encode(black_box(&metrics)));
        1
    });
    let per = run.per_item().as_secs_f64();
    println!(
        "{lines} series, {:.1} KB, {:.3} ms per render",
        bytes as f64 / 1024.0,
        per * 1e3
    );
    println!(
        "at one scrape every 15 s that is {:.6} percent of one core",
        per / 15.0 * 100.0
    );
}
