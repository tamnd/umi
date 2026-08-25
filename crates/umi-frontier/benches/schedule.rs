//! What the scheduler costs, against doc 16 gate 1.1.
//!
//! Gate 1.1 is 250 pages a second on one server, so the frontier has to issue
//! 250 leases a second on a fraction of one core and admit the links those
//! pages carry at the same time. Doc 08.1 puts the admission side at about
//! 12500 candidate URLs a second per host, which is the larger of the two
//! numbers by a wide margin, so both are measured here.
//!
//! Three parts, because they fail differently:
//!
//! 1. The gate on its own, as the number of resident domains grows. This is
//!    the part with an architectural question in it: `tick` walks the resident
//!    set once per tick to keep the schedule in step with what has been warmed
//!    and evicted, and a walk that is fine at a thousand domains and not fine
//!    at a million would be a design problem rather than a tuning one.
//! 2. Admission, which is canonicalisation, key derivation and a batched
//!    insert. Doc 11.2's canonicalisation dominates it.
//! 3. The whole loop against the reference store, for a leases per second
//!    number. Read that one knowing what is underneath it: `MemoryState` is a
//!    `BTreeMap` it scans in full on every `lease` call, so this measures a
//!    scheduler sitting on a store with no index, which is the floor and not
//!    the number a backend that stores rows in `(pld, host, url)` order will
//!    give. The interesting output here is the split between the two, printed
//!    as the frontier's own share.
//!
//! Environment:
//!
//! * `UMI_BENCH_DOMAINS` how many domains the loop benchmark spreads over,
//!   default 200
//! * `UMI_BENCH_URLS` how many URLs per domain, default 50
//! * `UMI_BENCH_TICKS` how many scheduler ticks to run, default 200

use std::hint::black_box;
use std::time::Instant;

use umi_frontier::{Ask, Config, Frontier, Gate, Rate};
use umi_state::{Discovery, MemoryState, State};
use umi_types::PldId;

fn main() {
    gate();
    admission();
    loop_throughput();
}

/// Part 1. The gate as the resident set grows.
fn gate() {
    println!("gate, one tick of keep-in-step plus a lease round over 64 domains");
    println!(
        "{:>10}  {:>12}  {:>14}  {:>10}",
        "domains", "us/tick", "ticks/s", "of a 100ms tick"
    );

    for count in [1_000usize, 10_000, 100_000, 1_000_000] {
        let plds: Vec<PldId> = (0..count).map(pld).collect();
        let mut sorted = plds.clone();
        sorted.sort_unstable();

        let mut gate = Gate::new(Rate::default());
        for pld in &sorted {
            gate.note(*pld);
        }

        let ticks: u64 = if count >= 1_000_000 { 200 } else { 2_000 };
        let start = Instant::now();
        let mut now = 0u64;
        for _ in 0..ticks {
            for pld in &sorted {
                gate.note(*pld);
            }
            gate.retain(&sorted);
            let ready = gate.ready(now, 64);
            for (pld, allowance) in &ready {
                gate.charge(*pld, black_box(*allowance).min(2), now);
            }
            black_box(ready.len());
            now += 100;
        }
        let elapsed = start.elapsed();

        let per_tick_us = elapsed.as_micros() / u128::from(ticks);
        let per_second = 1_000_000u128.checked_div(per_tick_us).unwrap_or(u128::MAX);
        // Doc 09.3 runs the loop once per 100 ms, so the number that matters
        // is what share of that budget the bookkeeping takes. Tenths of a
        // percent, in integers.
        let share = per_tick_us * 1000 / 100_000;
        println!(
            "{count:>10}  {per_tick_us:>12}  {:>14}  {:>9}.{}%",
            if per_second > 10_000_000 {
                ">10M".to_owned()
            } else {
                per_second.to_string()
            },
            share / 10,
            share % 10
        );
    }
    println!();
}

/// Part 2. Admission, which doc 08.1 puts at 12500 a second per host.
fn admission() {
    let runtime = runtime();
    println!("admission, canonicalise plus derive plus insert");
    println!("{:>10}  {:>14}  {:>12}", "batch", "candidates/s", "ns each");

    for batch in [64usize, 1_000, 4_096] {
        let urls: Vec<String> = (0..batch)
            .map(|n| {
                format!(
                    "https://Host{}.Example.com:443/path/{n}?b=2&a=1#frag",
                    n % 97
                )
            })
            .collect();
        let links: Vec<&str> = urls.iter().map(String::as_str).collect();

        // Fresh store each round, so this measures admitting new URLs rather
        // than the cheaper path of recognising ones already seen.
        let rounds = 200usize;
        let mut fronts: Vec<Frontier<MemoryState>> = (0..rounds)
            .map(|_| Frontier::new(MemoryState::new(), Config::default()))
            .collect();

        let start = Instant::now();
        runtime.block_on(async {
            for front in &mut fronts {
                let report = front
                    .discover(&links, 0, 1_000, Discovery::Trusted)
                    .await
                    .expect("discover");
                black_box(report.total());
            }
        });
        let elapsed = start.elapsed();

        let total = (batch * rounds) as u128;
        let nanos = elapsed.as_nanos();
        println!(
            "{batch:>10}  {:>14}  {:>12}",
            total * 1_000_000_000 / nanos.max(1),
            nanos / total
        );
    }
    println!();
}

/// Part 3. The whole loop, for a leases per second number.
fn loop_throughput() {
    let runtime = runtime();
    let domains = env("UMI_BENCH_DOMAINS", 200);
    let per_domain = env("UMI_BENCH_URLS", 50);
    let ticks = env("UMI_BENCH_TICKS", 200);

    let urls: Vec<String> = (0..domains)
        .flat_map(|d| (0..per_domain).map(move |u| format!("https://site{d}.example/page{u}")))
        .collect();
    let links: Vec<&str> = urls.iter().map(String::as_str).collect();

    let front = Frontier::new(MemoryState::new(), Config::default());
    runtime.block_on(async {
        front.seed(&links, 0).await.expect("seed");
    });
    println!(
        "loop, {} urls over {} domains, {} ticks of 100ms of crawl time",
        urls.len(),
        domains,
        ticks
    );

    let mut leased = 0usize;
    let start = Instant::now();
    runtime.block_on(async {
        let mut now = 0u64;
        for _ in 0..ticks {
            let out = front.tick(&Ask::new(now, 64)).await.expect("tick");
            leased += out.len();
            // Nothing is completed, so a URL under lease stays under lease and
            // the store keeps having to skip it. That is the pessimistic case
            // for a scan based backend and the point is not to flatter it.
            black_box(out.len());
            now += 100;
        }
    });
    let elapsed = start.elapsed();

    let nanos = elapsed.as_nanos().max(1);
    let per_second = (leased as u128) * 1_000_000_000 / nanos;
    println!("  leased            {leased}");
    println!("  wall              {} ms", elapsed.as_millis());
    println!("  leases/s          {per_second}");
    println!("  us/tick           {}", nanos / 1000 / u128::from(ticks));

    // Gate 1.1 needs 250 a second on one server. Say plainly whether this
    // clears it and by how much, rather than leaving the reader to divide.
    let headroom = per_second / 250;
    println!("  gate 1.1 (250/s)  {}x", headroom);

    let resident = runtime.block_on(async { front.state().resident().await.expect("resident") });
    println!("  resident domains  {}", resident.len());
    println!();
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
}

fn env(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn pld(n: usize) -> PldId {
    let mut bytes = [0u8; PldId::LEN];
    let source = (n as u64).to_be_bytes();
    let take = bytes.len().min(source.len());
    bytes[..take].copy_from_slice(&source[..take]);
    PldId::from_bytes(bytes)
}
