//! What one ask against a real crawl directory actually hands back.
//!
//! The benchmark next door measures the scheduler against `MemoryState` with a
//! frontier it built itself, which says what the code costs and nothing about
//! what a directory that has been crawling for a while does. This says the
//! second thing. Point it at a `state.sqlite` from a real run and it reports,
//! per ask, how many leases came back against how many were asked for and how
//! long the round trip took.
//!
//! ```text
//! cargo run --release --example ask -- /root/lg-out/state.sqlite 20 1024
//! ```
//!
//! It leases, so it writes: a lease marks rows in flight and moves host
//! timers, and nothing here gives them back. Run it on a copy.
//!
//! The first block is the gate on its own, because the two halves of a short
//! answer look identical from the outside. Either the scheduler offered the
//! store plenty of domains and the store found little under them, or the
//! scheduler had little to offer. `ready` is the same call `tick` makes, so
//! the domains and the allowance groups printed there are exactly what the ask
//! below went to the store with.

use std::process::ExitCode;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use umi_frontier::{Ask, Config, Frontier, Gate};
use umi_state::State;
use umi_state_sqlite::SqliteState;
use umi_types::PldId;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: ask <state.sqlite> [asks] [max_urls]");
        return ExitCode::from(2);
    };
    let asks: u32 = args.next().and_then(|a| a.parse().ok()).unwrap_or(20);
    let max_urls: u32 = args.next().and_then(|a| a.parse().ok()).unwrap_or(1024);

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("ask: no runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(&path, asks, max_urls)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ask: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(path: &str, asks: u32, max_urls: u32) -> Result<(), Box<dyn std::error::Error>> {
    let state = SqliteState::open(path)?;
    let config = Config::default();
    let frontier = Frontier::new(state, config);

    let began = Instant::now();
    let domains = frontier.resume().await?;
    println!(
        "{path}: {domains} domains scheduled, rebuilt in {} ms",
        began.elapsed().as_millis()
    );

    offered(frontier.state(), &config, now_ms()).await?;

    println!();
    println!(
        "{:>4}  {:>8}  {:>8}  {:>7}  {:>7}  {:>9}",
        "ask", "wanted", "leased", "domains", "hosts", "ms"
    );
    let mut total = 0usize;
    let mut spent = std::time::Duration::ZERO;
    for n in 1..=asks {
        let ask = Ask::new(now_ms(), max_urls);
        let from = Instant::now();
        let leases = frontier.tick(&ask).await?;
        let took = from.elapsed();
        spent += took;
        total += leases.len();

        let mut plds: Vec<PldId> = leases.iter().map(|lease| lease.key.pld).collect();
        plds.sort_unstable();
        plds.dedup();
        let mut hosts: Vec<_> = leases.iter().map(|lease| lease.key.host).collect();
        hosts.sort_unstable();
        hosts.dedup();
        println!(
            "{n:>4}  {max_urls:>8}  {:>8}  {:>7}  {:>7}  {:>9}",
            leases.len(),
            plds.len(),
            hosts.len(),
            took.as_millis()
        );
    }
    println!();
    let ms = spent.as_millis().max(1);
    println!(
        "{total} leases over {asks} asks in {ms} ms, {} a second",
        total as u128 * 1000 / ms
    );
    Ok(())
}

/// What the gate would offer the store, without asking the store anything.
///
/// A second gate built from the same resident set rather than the frontier's
/// own, because the frontier does not hand its gate out and a read only copy
/// answers the question just as well. Both are built by `resume`, so at this
/// point in the run the two agree row for row.
async fn offered(
    state: &SqliteState,
    config: &Config,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let resident = state.resident().await?;
    let mut gate = Gate::new(config.rate);
    for pld in &resident {
        gate.note(*pld);
    }
    let ready = gate.ready(now, config.max_domains);
    let mut groups: Vec<u32> = ready.iter().map(|(_, allowance)| *allowance).collect();
    groups.sort_unstable();
    groups.dedup();
    println!(
        "gate: {} resident, {} offered of a {} cap, {} allowance groups, {} leases on offer",
        resident.len(),
        ready.len(),
        config.max_domains,
        groups.len(),
        ready
            .iter()
            .map(|(_, allowance)| u64::from(*allowance))
            .sum::<u64>()
    );
    Ok(())
}

/// The wall clock, since this is measuring a real directory rather than
/// replaying one.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
