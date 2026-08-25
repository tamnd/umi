//! Write a state file on one machine, verify it on another.
//!
//! The claim this backend makes is that a crawl directory can be moved between
//! an x86 machine and an arm one and picked up where it left off. The unit
//! tests check the properties that make that true, which is as far as a test
//! running on one machine can get. This is the other half: build it here, copy
//! it there, and have the other machine say whether it agrees.
//!
//! ```text
//! cargo run --example portable -- write /tmp/portable.umistate
//! scp /tmp/portable.umistate server1:/tmp/
//! ssh server1 'cd umi && cargo run --example portable -- verify /tmp/portable.umistate'
//! ```
//!
//! `verify` also leases and completes a url before it returns, so the file that
//! comes back has been written by the second machine as well as read by it.
//! Copy it back and run `verify` again to close the loop.

use std::process::ExitCode;

use umi_state::{
    Candidate, FetchOutcome, FetchResult, HostRow, LeaseRequest, NackReason, Pace, Priority,
    Revalidator, State,
};
use umi_state_sqlite::SqliteState;
use umi_types::{FetcherId, RowKey, Tier};

/// A fixed instant, so both machines agree on every timestamp in the file
/// without either of them reading a clock.
const T0: u64 = 1_700_000_000_000;

/// How many urls the fixture holds. Enough that the ledger is more than one
/// page and the ordering has something to be wrong about.
const URLS: u32 = 500;

/// How many rounds of leasing the fixture runs, so a good share of the
/// frontier has a fetch history behind it rather than all of it being pending.
const ROUNDS: u32 = 4;

/// How many urls go in at the top of the priority range, so that the far side
/// of the move leases rows that have been fetched rather than rows that have
/// only been discovered.
const PROMOTED: u32 = 100;

/// How many hosts the urls are spread over.
const HOSTS: u32 = 50;

/// One week in milliseconds, the step each hop of the round trip takes.
const WEEK: u64 = 7 * 24 * 3600 * 1000;

fn url(n: u32) -> String {
    format!("https://h{}.example.com/p/{n}", n % HOSTS)
}

/// The last instant any machine did work in this file, read back out of it.
///
/// Leasing bumps a host's politeness window to the moment the lease was handed
/// out plus its delay, so the newest of those windows is a lower bound on when
/// the previous hop stopped. That is all a later hop needs to pick a clock that
/// runs forward.
async fn last_touched(state: &SqliteState) -> Result<u64, String> {
    let mut latest = T0;
    for n in 0..HOSTS {
        let text = url(n);
        let key = RowKey::for_url(&text, None).map_err(|e| format!("{text}: {e}"))?;
        if let Some(host) = state
            .host(key.host)
            .await
            .map_err(|e| format!("host {text}: {e}"))?
        {
            latest = latest.max(host.next_allowed_ms);
        }
    }
    Ok(latest)
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (Some(mode), Some(path)) = (args.next(), args.next()) else {
        eprintln!("usage: portable <write|verify> <path>");
        return ExitCode::FAILURE;
    };

    let result = match mode.as_str() {
        "write" => write(&path).await,
        "verify" => verify(&path).await,
        other => Err(format!("unknown mode {other}, expected write or verify")),
    };

    match result {
        Ok(message) => {
            println!("ok on {}: {message}", std::env::consts::ARCH);
            ExitCode::SUCCESS
        }
        Err(why) => {
            eprintln!("failed on {}: {why}", std::env::consts::ARCH);
            ExitCode::FAILURE
        }
    }
}

/// Build the fixture: every url admitted, a quarter of them fetched, one host
/// blocked, and one url still out on a lease.
async fn write(path: &str) -> Result<String, String> {
    if std::path::Path::new(path).exists() {
        std::fs::remove_file(path).map_err(|e| format!("could not replace {path}: {e}"))?;
    }
    let state = SqliteState::open(path).map_err(|e| format!("open: {e}"))?;

    let urls: Vec<String> = (0..URLS).map(url).collect();
    // The first slice goes in at the top of the range. Priority is the first
    // sort key, so these are the urls that get fetched below and the ones that
    // come back first when the other machine leases, which is what puts a row
    // with a real fetch history and an interned ETag in front of `verify`.
    // A pending row is due at discovery and a refreshed one is not due for a
    // day, so without this every lease on the far side would be a url nobody
    // had ever looked at.
    let batch: Vec<Candidate<'_>> = urls
        .iter()
        .enumerate()
        .map(|(n, u)| {
            Candidate::new(u, T0)
                .map(|c| Candidate {
                    priority: if n < PROMOTED as usize {
                        Priority::MAX
                    } else {
                        Priority::DEFAULT
                    },
                    ..c
                })
                .map_err(|e| format!("{u} is not crawlable: {e}"))
        })
        .collect::<Result<_, _>>()?;
    let report = state
        .admit(&batch)
        .await
        .map_err(|e| format!("admit: {e}"))?;
    if report.admitted != URLS {
        return Err(format!("admitted {} of {URLS}", report.admitted));
    }

    // A spread of states, so `verify` has something to compare that is not just
    // a count of rows. One url per host per round, then the clock moves past
    // the politeness window and the next round goes out, which is the shape a
    // real crawl has.
    let mut now = T0;
    let mut round = 0u32;
    while round < ROUNDS {
        let leases = state
            .lease(&LeaseRequest {
                max_per_host: 1,
                ..LeaseRequest::new(FetcherId::LOCAL, now, 64)
            })
            .await
            .map_err(|e| format!("lease: {e}"))?;
        if leases.is_empty() {
            break;
        }
        for (n, lease) in leases.iter().enumerate() {
            // One is deliberately left in flight at the end, so the file
            // carries a lease across the move and the other machine has to
            // honour it rather than handing the url out again.
            if round + 1 == ROUNDS && n + 1 == leases.len() {
                break;
            }
            state
                .complete(&[FetchOutcome {
                    lease: lease.id,
                    key: lease.key,
                    finished_ms: lease.not_before_ms + 1,
                    tier_used: Tier::Plain,
                    pace: Pace::default(),
                    result: FetchResult::Fetched {
                        status: 200,
                        content_hash: u64::from(round * 1000 + n as u32).to_le_bytes(),
                        revalidate: Revalidator {
                            etag: Some(format!("\"etag-{}\"", n % 7)),
                            last_modified_ms: Some(T0 - 1000),
                        },
                    },
                }])
                .await
                .map_err(|e| format!("complete: {e}"))?;
        }
        now = leases
            .iter()
            .map(|lease| lease.not_before_ms)
            .max()
            .unwrap_or(now)
            + HostRow::INITIAL_DELAY_MS as u64
            + 1;
        round += 1;
    }

    let stats = state.stats().await.map_err(|e| format!("stats: {e}"))?;
    Ok(format!(
        "wrote {path}: {} seen, {} fetched, {} in flight",
        stats.urls_seen, stats.urls_fetched, stats.leases_in_flight
    ))
}

/// Open the fixture, check everything about it that a move could have broken,
/// then write to it so the file carries this machine's work back.
async fn verify(path: &str) -> Result<String, String> {
    let state = SqliteState::open(path).map_err(|e| format!("open: {e}"))?;
    let stats = state.stats().await.map_err(|e| format!("stats: {e}"))?;

    if stats.urls_seen != u64::from(URLS) {
        return Err(format!(
            "the seen set holds {}, not {URLS}",
            stats.urls_seen
        ));
    }
    if stats.urls_fetched == 0 {
        return Err("nothing came back as fetched, so the state column did not survive".to_owned());
    }
    // At least one lease was outstanding when the file was closed, and a lease
    // is two nullable columns, which is exactly the shape that goes wrong when
    // a row is rewritten by a build that reads the schema differently. The
    // count is a floor rather than an exact number because a verify leaves its
    // own leases behind, so a file that has been round tripped twice carries
    // more than the one the writer left.
    if stats.leases_in_flight == 0 {
        return Err("no lease survived the move, so the lease columns did not".to_owned());
    }

    // Keys are the part most likely to be wrong after a move, because they are
    // the only thing in the file that is bytes rather than a number, and a key
    // that does not match is a url the crawl would fetch a second time.
    for n in [0u32, 1, URLS / 2, URLS - 1] {
        let text = url(n);
        let key = RowKey::for_url(&text, None).map_err(|e| format!("{text}: {e}"))?;
        let seen = state
            .admit(&[Candidate::new(&text, T0).map_err(|e| format!("{text}: {e}"))?])
            .await
            .map_err(|e| format!("admit: {e}"))?;
        if seen.seen != 1 {
            return Err(format!(
                "{text} was not recognised, so key {:?} does not match what is on disk",
                key.url
            ));
        }
    }

    // A fixed clock would only work on the first hop. Every verify leases and
    // completes, which pushes both the politeness window and the refresh date
    // forward, so the second machine to see the file has to start later than
    // the first one finished. Take that instant from the file rather than from
    // a constant, by asking the hosts how far they have been pushed.
    let now = last_touched(&state).await? + WEEK;

    // The frontier still hands out work, in the order the trait promises.
    let leases = state
        .lease(&LeaseRequest::new(FetcherId::LOCAL, now, 32))
        .await
        .map_err(|e| format!("lease: {e}"))?;
    if leases.is_empty() {
        return Err("the frontier came back empty on a file that has 500 urls in it".to_owned());
    }
    for pair in leases.windows(2) {
        if pair[0].priority < pair[1].priority {
            return Err("the lease order did not survive the move".to_owned());
        }
    }

    // A revalidator that was interned on the other machine has to come back as
    // the same text, or every conditional request after a move is a full fetch.
    let revalidated = leases
        .iter()
        .filter(|lease| lease.revalidate.is_some())
        .count();
    if revalidated == 0 {
        return Err(
            "no lease carried a revalidator, so the interned etag pool did not survive the move"
                .to_owned(),
        );
    }

    // Write, so the file that goes back has been touched by this machine.
    let mut done = Vec::new();
    for lease in leases.iter().take(8) {
        let finished_ms = now + done.len() as u64;
        state
            .complete(&[FetchOutcome {
                lease: lease.id,
                key: lease.key,
                finished_ms,
                tier_used: Tier::Plain,
                pace: Pace::default(),
                result: FetchResult::NotModified {
                    status: 304,
                    revalidate: lease.revalidate.clone().unwrap_or_default(),
                },
            }])
            .await
            .map_err(|e| format!("complete: {e}"))?;
        done.push(lease.key);
    }
    let completed = done.len();

    // A completion has to have landed in the file, not just in this process.
    // Counting fetched urls would not show it: the leases above were mostly
    // rows the other machine had already fetched, and a 304 leaves those in
    // the state they were in. What a completion does move is the schedule, so
    // ask the frontier for work at the same instant again and check that none
    // of the eight come back.
    let reoffered = state
        .lease(&LeaseRequest::new(FetcherId::LOCAL, now, 64))
        .await
        .map_err(|e| format!("lease: {e}"))?;
    if let Some(back) = reoffered.iter().find(|lease| done.contains(&lease.key)) {
        return Err(format!(
            "{} was handed out again right after it was completed, so the write did not land",
            back.url
        ));
    }
    let taken: Vec<_> = reoffered.iter().map(|lease| lease.id).collect();
    state
        .release(&taken, NackReason::Shutdown)
        .await
        .map_err(|e| format!("release: {e}"))?;

    let checkpoint = state
        .checkpoint(now)
        .await
        .map_err(|e| format!("checkpoint: {e}"))?;

    let after = state.stats().await.map_err(|e| format!("stats: {e}"))?;
    if after.urls_seen != stats.urls_seen {
        return Err("the seen set changed under a read only verify".to_owned());
    }

    Ok(format!(
        "{path}: {} seen, {} fetched, {revalidated} of {} leases carried a revalidator, \
         {completed} completed here, checkpoint {} digest {}",
        after.urls_seen,
        after.urls_fetched,
        leases.len(),
        checkpoint.sequence,
        checkpoint
            .digest
            .map_or_else(|| "none".to_owned(), |d| hex(d.as_bytes()))
    ))
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
