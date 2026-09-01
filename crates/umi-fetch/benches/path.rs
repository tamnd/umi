//! What the fetch path costs when the network is not in the way.
//!
//! Issue #208 is that the fleet fetches at about the same rate whether 256 or
//! 1024 requests are in flight, on under two cores of eight. That is the shape
//! of something serialised, but a run against the real web cannot tell our code
//! apart from DNS, from a handshake, or from the far end simply being slow. So
//! this runs the same fetcher against an origin on loopback, where a request
//! costs a few microseconds of somebody else's time and everything left over is
//! ours.
//!
//! The number to read is the ratio between the windows. If 1024 in flight moves
//! four times what 64 does then the path scales and the fleet's ceiling is out
//! on the wire. If it flattens here too then it is in here.
//!
//! The other axis is how the window is held. A `FuturesUnordered` of plain
//! futures is one task, and one task is one thread however many futures are in
//! it, so a window of a thousand is a thousand things taking turns on a single
//! core. A `FuturesUnordered` of join handles is a thousand tasks the runtime
//! can put on every core it has. The two look identical at the call site and
//! this is the difference between them.
//!
//! Loopback is not the web and this is not a throughput claim. Nothing here
//! pays for a name, a handshake, congestion or a slow origin, which between them
//! are most of what a real fetch is. It is an upper bound and it is only useful
//! as one.
//!
//! ```text
//! ulimit -n 200000
//! cargo bench -p umi-fetch --bench path
//! ```
//!
//! The file descriptor limit matters. Every request in flight holds a socket at
//! each end, so a thousand in flight against a local origin is over two thousand
//! open files, and a default limit of 1024 turns this into a measurement of
//! `EMFILE`.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use umi_fetch::{FetchConfig, Fetcher, Outcome};

/// The windows to measure. The first is well under what one box runs at and the
/// last is what the robots prefetch is running at right now.
const WINDOWS: [usize; 4] = [64, 256, 512, 1024];

/// How long each pass runs. Long enough that the ramp up is a rounding error
/// and short enough that the whole bench is a couple of minutes.
const SECONDS: u64 = 5;

/// Distinct loopback addresses to spread requests over.
///
/// Not one, because `per_host` is 2 and a single host would cap every window at
/// two in flight. This many because the largest window needs half of them even
/// if the spread were perfect.
const HOSTS: usize = 1024;

/// A robots.txt sized body, which is what the prefetch that raised #208 reads.
const SMALL: usize = 4 * 1024;

/// A page sized body, near doc 05.4's 512 KiB cap. The difference between this
/// and the small one is what reading and hashing a body costs.
const LARGE: usize = 256 * 1024;

/// A permit table big enough that it never sweeps, which is the default and is
/// not what a real run gets.
const ROOMY_TABLE: usize = 4096;

/// A permit table smaller than the number of hosts in play, so it is always
/// full and every request pays for the sweep. A real run is in this state all
/// the time, because the fleet touches millions of hosts and the table holds
/// four thousand.
const TIGHT_TABLE: usize = 64;

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");

    let addr = runtime.block_on(origin());
    let hosts = runtime.block_on(reachable(addr.port()));
    println!(
        "origin on port {}, {} loopback address{} in use, {} worker threads\n",
        addr.port(),
        hosts.len(),
        if hosts.len() == 1 { "" } else { "es" },
        std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get)
    );
    if hosts.len() < HOSTS {
        println!(
            "Only 127.0.0.1 answers here, so `per_host` is raised to the window to keep the\n\
             permit table from being the limit. Linux has all of 127.0.0.0/8 and gives the\n\
             honest shape; macOS configures one address and does not.\n"
        );
    }

    println!(
        "{:<30}{:>10}{:>12}{:>12}{:>12}",
        "", "fetch/s", "ms/fetch", "ok", "fail"
    );

    // Connection per request first, because that is what crawling looks like:
    // a host is visited once and the socket is no use to anyone afterwards.
    // One task first, because that is what `umi robots` does today.
    for window in WINDOWS {
        let fetcher = client(&hosts, window, ROOMY_TABLE);
        let run = runtime.block_on(sweep(&fetcher, &hosts, addr.port(), window, "c", false));
        line(&format!("one task, {window} in flight"), &run);
    }

    // The same work as one task per fetch, which is the only thing that
    // changes between these two groups.
    for window in WINDOWS {
        let fetcher = client(&hosts, window, ROOMY_TABLE);
        let run = runtime.block_on(sweep(&fetcher, &hosts, addr.port(), window, "c", true));
        line(&format!("spawned, {window} in flight"), &run);
    }

    // Then with the socket kept, which takes the accept and the close out of
    // the picture and leaves our code holding the bill.
    for window in WINDOWS {
        let fetcher = client(&hosts, window, ROOMY_TABLE);
        let run = runtime.block_on(sweep(&fetcher, &hosts, addr.port(), window, "k", true));
        line(&format!("spawned, kept open, {window}"), &run);
    }

    // A page sized body. Everything above reads four kilobytes, and a page is
    // sixty times that through the cap check, the sniffer and blake3.
    let window = WINDOWS[WINDOWS.len() - 1];
    let fetcher = client(&hosts, window, ROOMY_TABLE);
    let run = runtime.block_on(sweep(&fetcher, &hosts, addr.port(), window, "kl", true));
    line(&format!("spawned, {LARGE} byte body"), &run);

    // The permit table under pressure. A real run touches far more hosts than
    // the table holds, so it is always full and every request pays for the
    // sweep. This forces that with a small cap rather than a lot of hosts.
    let fetcher = client(&hosts, window, TIGHT_TABLE);
    let run = runtime.block_on(sweep(&fetcher, &hosts, addr.port(), window, "k", true));
    line(&format!("spawned, host table cap {TIGHT_TABLE}"), &run);
}

/// What one pass did.
struct Run {
    elapsed: Duration,
    ok: usize,
    fail: usize,
}

/// A fetcher configured for one pass.
///
/// `per_host` only moves when there is one address to talk to, because there it
/// is the window and not the path that would be measured.
fn client(hosts: &[String], window: usize, host_table_cap: usize) -> Arc<Fetcher> {
    let mut config = FetchConfig::default();
    config.host_table_cap = host_table_cap;
    if hosts.len() == 1 {
        config.per_host = window;
    }
    Arc::new(Fetcher::with_config(config).expect("a client"))
}

/// One fetch, either as a future this loop will poll itself or as a task the
/// runtime owns.
///
/// Boxed either way so the two shapes are the same type here. It costs an
/// allocation per fetch, which both shapes pay and which is nothing next to a
/// socket.
type Done = Pin<Box<dyn Future<Output = bool> + Send>>;

/// Set one fetch going.
fn one(fetcher: &Arc<Fetcher>, url: String, spawn: bool) -> Done {
    let fetcher = Arc::clone(fetcher);
    let fetch = async move { matches!(fetcher.fetch(&url, None).await, Ok(Outcome::Ok(_))) };
    if spawn {
        let handle = tokio::spawn(fetch);
        // A task that panicked did not fetch anything, which is a failure and
        // not a reason to stop the pass.
        Box::pin(async move { handle.await.unwrap_or(false) })
    } else {
        Box::pin(fetch)
    }
}

/// Fetch for [`SECONDS`], never more than `window` at once.
///
/// `flags` go in the path and tell the origin what to do: `k` to keep the
/// connection, `l` for a page sized body.
async fn sweep(
    fetcher: &Arc<Fetcher>,
    hosts: &[String],
    port: u16,
    window: usize,
    flags: &str,
    spawn: bool,
) -> Run {
    let deadline = Instant::now() + Duration::from_secs(SECONDS);
    let started = Instant::now();
    let mut ok = 0;
    let mut fail = 0;
    let mut next = 0usize;
    let mut inflight = FuturesUnordered::new();

    loop {
        while inflight.len() < window && Instant::now() < deadline {
            let url = format!("http://{}:{port}/{flags}/{next}", hosts[next % hosts.len()]);
            next += 1;
            inflight.push(one(fetcher, url, spawn));
        }
        let Some(good) = inflight.next().await else {
            break;
        };
        if good {
            ok += 1;
        } else {
            fail += 1;
        }
    }

    Run {
        elapsed: started.elapsed(),
        ok,
        fail,
    }
}

/// One row of the table.
fn line(name: &str, run: &Run) {
    let total = run.ok + run.fail;
    let seconds = run.elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "{:<30}{:>10.0}{:>12.2}{:>12}{:>12}",
        name,
        total as f64 / seconds,
        seconds * 1000.0 / total.max(1) as f64,
        run.ok,
        run.fail
    );
}

/// The loopback addresses that actually answer on `port`.
///
/// Linux routes the whole of 127.0.0.0/8 to the loopback interface, so a
/// listener on the wildcard address is reachable at every one of them and the
/// permit table sees a thousand hosts. macOS configures 127.0.0.1 and nothing
/// else, so this comes back with one and the caller adjusts.
async fn reachable(port: u16) -> Vec<String> {
    let names: Vec<String> = (0..HOSTS)
        .map(|i| format!("127.0.{}.{}", i / 254, i % 254 + 1))
        .collect();
    // The second address is the whole question, so ask it once rather than
    // opening a thousand connections to find out.
    if TcpStream::connect(format!("{}:{port}", names[1]))
        .await
        .is_err()
    {
        return vec![names[0].clone()];
    }
    names
}

/// An origin that answers every request the same way and as fast as it can.
///
/// Deliberately not a real server. It parses nothing, it decides what to send
/// from two characters of the request line, and both responses are built once
/// and then written verbatim. A benchmark of the client is worth nothing if the
/// server is the slow half.
async fn origin() -> SocketAddr {
    let listener = TcpListener::bind("0.0.0.0:0").await.expect("bind");
    let addr = listener.local_addr().expect("a bound port has an address");

    // [keep the connection][page sized body], built once.
    let replies: Arc<[[Arc<Vec<u8>>; 2]; 2]> = Arc::new([
        [reply(SMALL, false), reply(LARGE, false)],
        [reply(SMALL, true), reply(LARGE, true)],
    ]);

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let replies = Arc::clone(&replies);
            tokio::spawn(async move {
                let _ = serve(socket, &replies).await;
            });
        }
    });

    addr
}

/// One connection, for as many requests as the client sends down it.
async fn serve(mut socket: TcpStream, replies: &[[Arc<Vec<u8>>; 2]; 2]) -> io::Result<()> {
    let _ = socket.set_nodelay(true);
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    loop {
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..read]);
        // Every request here is a GET with no body, so the blank line is the
        // whole of the framing that matters.
        let Some(end) = find(&buffer, b"\r\n\r\n") else {
            continue;
        };

        let flags = flags(&buffer[..end]);
        let keep = flags.contains('k');
        let reply = Arc::clone(&replies[usize::from(keep)][usize::from(flags.contains('l'))]);
        buffer.drain(..end + 4);

        socket.write_all(&reply).await?;
        if !keep {
            socket.flush().await?;
            return socket.shutdown().await;
        }
    }
}

/// The flag characters out of a request line like `GET /kl/17 HTTP/1.1`.
fn flags(head: &[u8]) -> &str {
    let line = std::str::from_utf8(head).unwrap_or("");
    let path = line.split(' ').nth(1).unwrap_or("");
    path.trim_start_matches('/').split('/').next().unwrap_or("")
}

/// A whole response, ready to write.
fn reply(body: usize, keep: bool) -> Arc<Vec<u8>> {
    let connection = if keep { "keep-alive" } else { "close" };
    let mut out = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/html; charset=utf-8\r\n\
         content-length: {body}\r\nconnection: {connection}\r\n\r\n"
    )
    .into_bytes();
    out.extend(std::iter::repeat_n(b'x', body));
    Arc::new(out)
}

/// Where `needle` starts in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
