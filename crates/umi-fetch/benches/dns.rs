//! What names cost, against gate 3.1's 250 pages a second.
//!
//! Every fetch of a host we have not seen today starts with a lookup, so the
//! resolver is in front of the whole ladder and its ceiling is the crawl's
//! ceiling. `src/resolver.rs` says why the crate speaks DNS itself instead of
//! calling `getaddrinfo`, and quotes numbers from an ad hoc measurement. This
//! is that measurement, kept, so the claim can be rechecked on a box that has
//! changed or a resolver that has.
//!
//! Two things come out of it. Cold throughput at several window sizes, which
//! is the number gate 3.1 has to clear, and it has to clear it twice over,
//! because a page needs a name and its robots.txt needed one first. And the
//! cached path, which is what a broad crawl mostly does and which says how much
//! of the win is the cache rather than the transport.
//!
//! It needs a list of real hosts and it goes to the network, so it does
//! nothing unless one is named:
//!
//! ```text
//! cargo bench -p umi-fetch --bench dns --no-run
//! UMI_DNS_LIST=/root/real-https-seed.txt ./target/release/deps/dns-<hash>
//! ```
//!
//! The list is consumed in order and no name is used twice, because a second
//! lookup of a name is a cache hit and would measure the cache. That is what
//! the last pass measures on purpose, and it is why it comes last.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt as _};
use reqwest::dns::Resolve as _;
use umi_fetch::resolver::Resolver;

/// The windows to measure, which bracket what a crawl and a bulk prefetch
/// actually run at.
const WINDOWS: [usize; 5] = [64, 128, 256, 512, 1024];

/// How many names each window resolves. Enough that the slow tail is
/// represented, since the tail is what decides the mean.
const NAMES: usize = 2_000;

/// The environment variable holding the list.
const LIST: &str = "UMI_DNS_LIST";

fn main() {
    let Ok(path) = std::env::var(LIST) else {
        println!("{LIST} is not set, so there is no list of hosts to resolve. Nothing measured.");
        return;
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(cause) => {
            println!("{path}: {cause}");
            return;
        }
    };

    let mut seen = HashSet::new();
    let hosts: Vec<String> = text
        .lines()
        .filter_map(host_of)
        .filter(|host| seen.insert(host.clone()))
        .collect();
    let wanted = NAMES * WINDOWS.len();
    if hosts.len() < wanted {
        println!(
            "{path} has {} usable hosts and {wanted} are needed, {NAMES} for each of {} windows",
            hosts.len(),
            WINDOWS.len()
        );
        return;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");

    println!("{} hosts read from {path}\n", hosts.len());
    println!(
        "{:<24}{:>10}{:>12}{:>12}{:>12}",
        "", "names/s", "ms/name", "answered", "failed"
    );

    let mut cut = 0;
    let mut first: Option<&[String]> = None;
    for window in WINDOWS {
        let slice = &hosts[cut..cut + NAMES];
        cut += NAMES;
        if first.is_none() {
            first = Some(slice);
        }
        // A fresh resolver per window, because the point of each pass is a cold
        // lookup and the shared one would be carrying the last pass's answers.
        let resolver = Resolver::fresh();
        let run = runtime.block_on(sweep(&resolver, slice, window));
        line(&format!("cold, {window} in flight"), &run);
    }

    // The same names again, on a resolver that has just answered them. This is
    // the path a broad crawl is mostly on: doc 08 has the fleet coming back to
    // a host many times in a day and every one of those is this line and not
    // the ones above it.
    if let Some(slice) = first {
        let resolver = Resolver::fresh();
        let _ = runtime.block_on(sweep(&resolver, slice, WINDOWS[0]));
        let run = runtime.block_on(sweep(&resolver, slice, WINDOWS[0]));
        line("cached, same names again", &run);
    }
}

/// What one pass did.
struct Run {
    elapsed: Duration,
    answered: usize,
    failed: usize,
}

/// Resolve every name in `hosts`, never more than `window` at once.
async fn sweep(resolver: &Resolver, hosts: &[String], window: usize) -> Run {
    let started = Instant::now();
    let mut answered = 0;
    let mut failed = 0;
    let mut next = 0;
    let mut inflight = FuturesUnordered::new();

    loop {
        while inflight.len() < window && next < hosts.len() {
            let name = hosts[next].clone();
            next += 1;
            inflight.push(async move {
                match name.parse() {
                    Ok(name) => resolver.resolve(name).await.is_ok(),
                    Err(_) => false,
                }
            });
        }
        let Some(ok) = inflight.next().await else {
            break;
        };
        if ok {
            answered += 1;
        } else {
            failed += 1;
        }
    }

    Run {
        elapsed: started.elapsed(),
        answered,
        failed,
    }
}

/// One row of the table.
fn line(name: &str, run: &Run) {
    let total = run.answered + run.failed;
    let seconds = run.elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "{:<24}{:>10.0}{:>12.1}{:>12}{:>12}",
        name,
        total as f64 / seconds,
        seconds * 1000.0 / total.max(1) as f64,
        run.answered,
        run.failed
    );
}

/// A line of the list as a host, the same way `umi robots` reads one.
fn host_of(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let rest = line.split_once("://").map_or(line, |(_, rest)| rest);
    let host = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let host = host.split(':').next().unwrap_or(host);
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || !host.contains('.') || host.contains(' ') {
        return None;
    }
    Some(host)
}
