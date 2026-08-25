//! Sustained bandwidth, doc 16's gate 1.1.
//!
//! Doc 01's whole capacity plan rests on how many bytes a host can pull per
//! second, and until this ran it was an assumption with a provider's marketing
//! page behind it. The gate asks for inbound and outbound sustained for at
//! least 60 seconds on server1, server2 and server3.
//!
//! # Why this is not three lines of `curl`
//!
//! The first attempt at this gate reported 1 to 16 Mbit/s on all three boxes
//! and looked like a catastrophic result. It was wrong. The endpoint it used,
//! `speed.cloudflare.com/__down?bytes=N`, returned a single byte, and what got
//! measured was background noise on the interface. Nothing in the output said
//! so, because a speed test that moves no bytes is indistinguishable from a
//! slow link: both report a small number.
//!
//! So two rules come out of that and both are in the code rather than in a
//! comment. A measurement asserts it moved a plausible number of bytes before
//! it is allowed to be a measurement, and it names the endpoints it used along
//! with what each one delivered, so a single dead endpoint reads as a dead
//! endpoint rather than as a slow link.
//!
//! # What gets counted
//!
//! Bytes this process actually received or sent, and on Linux the interface
//! counters over the same window as a cross check. The two disagree, and the
//! gap is the point: the interface number includes whatever else the box was
//! doing, which on server3 was 56 Mbit/s of somebody else's traffic. Doc 01
//! records both the raw number and the number net of idle for that reason.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Where the inbound bytes come from.
///
/// Four providers on three continents, all of them serving a large file for
/// exactly this purpose. More than one, because a single endpoint having a bad
/// day is the failure mode this module exists to tell apart from a slow link,
/// and geographically spread, because a crawler pulls from the whole web
/// rather than from the nearest datacentre.
const INBOUND: &[&str] = &[
    "https://ash-speed.hetzner.com/1GB.bin",
    "https://fsn1-speed.hetzner.com/1GB.bin",
    "https://hel1-speed.hetzner.com/1GB.bin",
    "https://cachefly.cachefly.net/100mb.test",
];

/// Where the outbound bytes go.
///
/// Cloudflare's speed test upload endpoint, which takes a POST of any size and
/// discards it. There is no second one, because public endpoints that accept
/// megabytes of anonymous upload are rare and for good reason.
const OUTBOUND: &str = "https://speed.cloudflare.com/__up";

/// One POST. Small enough that the tail lost when the clock runs out mid
/// request is under a percent of a 60 second run, large enough that the
/// request overhead is not what gets measured.
const UPLOAD_CHUNK: usize = 8 << 20;

/// Below this, a run is not a measurement. Sixty seconds of the slowest link
/// worth having still moves an order of magnitude more than this, so the only
/// thing it rejects is an endpoint that handed back nothing.
const PLAUSIBLE: u64 = 64 << 20;

/// Where the streams put what went wrong.
///
/// Shared rather than returned, because every stream ends by being cancelled at
/// the deadline and a cancelled future returns nothing. A mutex around a `Vec`
/// is the right amount of machinery for eight writers that append a line each
/// every few seconds.
type Log = Arc<std::sync::Mutex<Vec<String>>>;

/// Append to the log, ignoring a poisoned mutex.
///
/// A poisoned mutex here means another stream panicked, which is worth neither
/// panicking over nor losing the whole measurement to. The line is dropped and
/// the bytes still counted.
fn note(log: &Log, line: String) {
    if let Ok(mut held) = log.lock() {
        held.push(line);
    }
}

/// How long to run and how hard.
pub struct Options {
    /// Seconds per direction. Doc 16 wants at least 60.
    pub seconds: u64,
    /// Concurrent streams. One stream measures one TCP connection, which on a
    /// long fat link is not the same thing as the link.
    pub streams: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            seconds: 60,
            streams: 8,
        }
    }
}

/// What one direction came back with.
pub struct Direction {
    /// `inbound` or `outbound`.
    pub name: &'static str,
    /// Bytes this process moved.
    pub bytes: u64,
    /// How long it took, which is not exactly what was asked for.
    pub elapsed: Duration,
    /// What each endpoint delivered, in the order they are listed above. An
    /// endpoint at zero is the failure this module was written after.
    pub per_endpoint: Vec<(&'static str, u64)>,
    /// Everything that went wrong, deduplicated. Not fatal on its own: one
    /// stream losing a connection at second 40 of 60 is a Tuesday.
    pub errors: Vec<String>,
    /// Bytes the interface counters moved over the same window, on Linux. This
    /// includes everything else the box was doing.
    pub interface: Option<u64>,
}

impl Direction {
    /// Megabits per second, which is the unit doc 01 states capacity in.
    #[must_use]
    pub fn mbit(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64().max(0.001);
        self.bytes as f64 * 8.0 / seconds / 1e6
    }

    /// Pages per second at doc 01's measured 53.1 KB per fetched page.
    #[must_use]
    pub fn pages_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64().max(0.001);
        self.bytes as f64 / seconds / 53_100.0
    }

    /// Whether enough bytes moved for the number above to mean anything.
    ///
    /// Both halves matter. Too few bytes is the dead endpoint case. Too little
    /// time is a run that gave up early, and a fast number over two seconds is
    /// a burst rather than the sustained rate doc 16 asks for.
    #[must_use]
    pub fn plausible(&self, options: &Options) -> bool {
        self.bytes >= PLAUSIBLE && self.elapsed.as_secs_f64() >= options.seconds as f64 * 0.9
    }

    /// The endpoints that delivered nothing.
    #[must_use]
    pub fn dead(&self) -> Vec<&'static str> {
        self.per_endpoint
            .iter()
            .filter(|(_, bytes)| *bytes == 0)
            .map(|(url, _)| *url)
            .collect()
    }
}

/// Run both directions. Takes `2 * options.seconds` plus setup.
///
/// # Errors
///
/// When the runtime or the HTTP client will not build, which are the two ways
/// this can fail without having measured anything.
pub fn measure(options: &Options) -> Result<(Direction, Direction), String> {
    // A multi threaded runtime, because eight TLS streams at a few hundred
    // megabits is real CPU work in the decrypt path and a current thread
    // runtime would measure one core rather than the link. Four workers is
    // enough for that and leaves server1's other two alone.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|cause| format!("the runtime will not build: {cause}"))?;
    let client = client().map_err(|cause| format!("the client will not build: {cause}"))?;
    let inbound = runtime.block_on(inbound(&client, options));
    let outbound = runtime.block_on(outbound(&client, options));
    Ok((inbound, outbound))
}

fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        // No overall timeout. A request here is meant to run until the clock
        // says stop, and a timeout would turn a working long download into an
        // error at whatever the timeout was set to. The connect timeout is the
        // one that should exist, because a dead endpoint should be reported as
        // dead rather than eating a share of the window.
        .connect_timeout(Duration::from_secs(10))
        .user_agent(umi_fetch::USER_AGENT)
        .build()
}

async fn inbound(client: &reqwest::Client, options: &Options) -> Direction {
    use futures_util::StreamExt as _;

    let counters: Vec<Arc<AtomicU64>> = INBOUND
        .iter()
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    let log = Log::default();
    let before = interface();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(options.seconds);

    let mut streams = Vec::with_capacity(options.streams);
    for stream in 0..options.streams {
        let client = client.clone();
        let counters = counters.clone();
        let log = log.clone();
        // Cancelled at the deadline rather than asked to stop at it. The loop
        // below can only notice the clock between chunks, and a stream waiting
        // on a chunk that never comes waits past the end: the first run of this
        // asked for 60 seconds and took 86, which turns a rate into a smaller
        // rate. Cancelling mid chunk loses only the chunk in flight, which was
        // never counted, so what remains is bytes that arrived inside the
        // window and a window that is the length it says it is.
        let stop = tokio::time::Instant::from_std(deadline);
        streams.push(async move {
            let work = async move {
                let mut round = 0;
                while Instant::now() < deadline {
                    // Round robin rather than one endpoint per stream, so that the
                    // eight streams stay spread over the four endpoints as the
                    // slow ones finish their file and come back for another.
                    let which = (stream + round) % INBOUND.len();
                    round += 1;
                    let counter = &counters[which];
                    let request = client
                        .get(INBOUND[which])
                        // Identity, so that what gets counted is what crossed the
                        // wire. These files are incompressible and no sane server
                        // would try, but a proxy in the middle might.
                        .header(http::header::ACCEPT_ENCODING, "identity")
                        .send()
                        .await;
                    let response = match request {
                        Ok(response) => response,
                        Err(cause) => {
                            note(&log, format!("{}: {cause}", INBOUND[which]));
                            continue;
                        }
                    };
                    let mut body = response.bytes_stream();
                    while let Some(chunk) = body.next().await {
                        match chunk {
                            Ok(bytes) => counter.fetch_add(bytes.len() as u64, Ordering::Relaxed),
                            Err(cause) => {
                                note(&log, format!("{}: {cause}", INBOUND[which]));
                                break;
                            }
                        };
                        if Instant::now() >= deadline {
                            break;
                        }
                    }
                }
            };
            // The result is discarded because the errors are in the shared log
            // above. Every stream ends by being cancelled here, so a return
            // value would carry nothing back on the path that always happens.
            let _ = tokio::time::timeout_at(stop, work).await;
        });
    }
    futures_util::future::join_all(streams).await;

    finish(
        "inbound",
        INBOUND,
        start,
        counters,
        &log,
        before,
        interface(),
    )
}

async fn outbound(client: &reqwest::Client, options: &Options) -> Direction {
    // One buffer, shared by every stream, because `Bytes` is refcounted and
    // server1 has essentially no free memory. Eight streams holding their own
    // 8 MiB copy is how the first attempt at this got OOM killed.
    //
    // Filled with a cheap keyed pattern rather than zeros, so that anything in
    // the path that decides to compress a large upload cannot make the link
    // look faster than it is.
    let chunk = {
        let mut buffer = vec![0u8; UPLOAD_CHUNK];
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for byte in &mut buffer {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = (state >> 24) as u8;
        }
        bytes::Bytes::from(buffer)
    };

    let counters = vec![Arc::new(AtomicU64::new(0))];
    let log = Log::default();
    let before = interface();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(options.seconds);

    let mut streams = Vec::with_capacity(options.streams);
    for _ in 0..options.streams {
        let client = client.clone();
        let counter = counters[0].clone();
        let chunk = chunk.clone();
        let log = log.clone();
        let stop = tokio::time::Instant::from_std(deadline);
        streams.push(async move {
            let work = async move {
                while Instant::now() < deadline {
                    // Counted when the request completes, not as the bytes are
                    // handed to the socket, because bytes sitting in a send buffer
                    // when the clock stops have not crossed the link. That loses
                    // the request in flight at the end, which is under a percent
                    // of a 60 second run and errs low, which is the right way for
                    // a capacity number to be wrong.
                    match client
                        .post(OUTBOUND)
                        .header(http::header::CONTENT_TYPE, "application/octet-stream")
                        .body(chunk.clone())
                        .send()
                        .await
                    {
                        Ok(response) if response.status().is_success() => {
                            counter.fetch_add(UPLOAD_CHUNK as u64, Ordering::Relaxed);
                        }
                        Ok(response) => note(&log, format!("{OUTBOUND}: {}", response.status())),
                        Err(cause) => note(&log, format!("{OUTBOUND}: {cause}")),
                    }
                }
            };
            let _ = tokio::time::timeout_at(stop, work).await;
        });
    }
    futures_util::future::join_all(streams).await;

    finish(
        "outbound",
        &[OUTBOUND],
        start,
        counters,
        &log,
        before,
        interface(),
    )
}

fn finish(
    name: &'static str,
    endpoints: &[&'static str],
    start: Instant,
    counters: Vec<Arc<AtomicU64>>,
    log: &Log,
    before: Option<(u64, u64)>,
    after: Option<(u64, u64)>,
) -> Direction {
    let per_endpoint: Vec<(&'static str, u64)> = endpoints
        .iter()
        .zip(&counters)
        .map(|(url, counter)| (*url, counter.load(Ordering::Relaxed)))
        .collect();
    let bytes = counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .sum();

    // Deduplicated and capped. Eight streams losing the same endpoint produces
    // eight copies of one fact, and a report is not a log.
    let mut flat: Vec<String> = log.lock().map(|held| held.clone()).unwrap_or_default();
    flat.sort();
    flat.dedup();
    flat.truncate(4);

    let interface = match (before, after) {
        (Some((rx0, tx0)), Some((rx1, tx1))) => Some(if name == "inbound" {
            rx1.saturating_sub(rx0)
        } else {
            tx1.saturating_sub(tx0)
        }),
        _ => None,
    };

    Direction {
        name,
        bytes,
        elapsed: start.elapsed(),
        per_endpoint,
        errors: flat,
        interface,
    }
}

/// Received and transmitted bytes across every interface but the loopback.
///
/// Linux only, and `None` everywhere else rather than a guess. This is the
/// cross check, not the measurement: it counts what the whole box did, which
/// on a shared VPS is more than what this process did.
fn interface() -> Option<(u64, u64)> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let text = std::fs::read_to_string("/proc/net/dev").ok()?;
    Some(counters(&text))
}

/// The sum over `/proc/net/dev`, split out so it can be tested without one.
fn counters(text: &str) -> (u64, u64) {
    let mut rx = 0u64;
    let mut tx = 0u64;
    for line in text.lines() {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        // The loopback carries the DNS resolver and anything else local, and
        // counting it would credit the link with traffic that never left.
        if name == "lo" || name.is_empty() {
            continue;
        }
        let fields: Vec<&str> = rest.split_whitespace().collect();
        // Received bytes first, transmitted bytes ninth. The header two lines
        // above says so and the format has not moved since 2.6.
        if let (Some(got), Some(sent)) = (fields.first(), fields.get(8)) {
            rx += got.parse::<u64>().unwrap_or(0);
            tx += sent.parse::<u64>().unwrap_or(0);
        }
    }
    (rx, tx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_loopback_does_not_count_as_bandwidth() {
        let text = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets
    lo: 1000000    10    0    0    0     0          0         0  2000000     20
  eth0:  500000     5    0    0    0     0          0         0   300000      3
  eth1:  100000     1    0    0    0     0          0         0    50000      1
";
        assert_eq!(counters(text), (600_000, 350_000));
    }

    #[test]
    fn a_line_that_is_not_an_interface_is_skipped() {
        assert_eq!(counters("nonsense\nmore nonsense\n"), (0, 0));
    }

    /// The failure this module was written after: an endpoint returned one
    /// byte and the report said "slow link" instead of "dead endpoint".
    #[test]
    fn a_run_that_moved_nothing_is_not_a_measurement() {
        let options = Options::default();
        let direction = Direction {
            name: "inbound",
            bytes: 1,
            elapsed: Duration::from_secs(60),
            per_endpoint: INBOUND.iter().map(|url| (*url, 0)).collect(),
            errors: Vec::new(),
            interface: Some(20_000_000),
        };
        assert!(!direction.plausible(&options));
        assert_eq!(direction.dead().len(), INBOUND.len());
    }

    /// And the other half: a fast number over a window too short to be
    /// sustained is a burst, and doc 16 asked for sustained.
    #[test]
    fn a_burst_is_not_a_sustained_rate() {
        let options = Options::default();
        let direction = Direction {
            name: "inbound",
            bytes: 2 << 30,
            elapsed: Duration::from_secs(2),
            per_endpoint: INBOUND.iter().map(|url| (*url, 1 << 29)).collect(),
            errors: Vec::new(),
            interface: None,
        };
        assert!(!direction.plausible(&options));
        assert!(direction.dead().is_empty());
    }

    #[test]
    fn the_rate_is_reported_in_the_units_doc_01_uses() {
        let direction = Direction {
            name: "inbound",
            bytes: 100_000_000,
            elapsed: Duration::from_secs(8),
            per_endpoint: Vec::new(),
            errors: Vec::new(),
            interface: None,
        };
        // 100 MB in 8 s is 12.5 MB/s, which is 100 Mbit/s, which at doc 01's
        // measured 53.1 KB a page is 235 pages a second.
        assert!((direction.mbit() - 100.0).abs() < 0.01);
        assert!((direction.pages_per_second() - 235.4).abs() < 0.1);
    }
}
