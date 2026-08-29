//! An origin that behaves badly on purpose, for doc 16's gate 2.3.
//!
//! The gate asks for the crawler to be measured from the server's logs rather
//! than from its own counters, and that is the whole reason this exists. A
//! crawler's counters are a record of what it believes it did. Every politeness
//! bug worth catching is one where those two disagree: a retry that goes out
//! without passing the limiter, a connection reused after a reset, a backoff
//! timer that resets itself on an error. None of those are visible from the
//! inside, and all of them are obvious in a list of arrival times.
//!
//! So this writes one line per request, with the time it arrived, and never
//! looks at the crawler at all. `scripts/check-politeness.py` reads the file
//! afterwards and says whether the rate held.
//!
//! HTTP/1.1 by hand, in about three hundred lines, because the four behaviours
//! the gate names are things no real server would agree to do. A framework
//! would not let us reset a connection halfway through a body, and the parsing
//! we need is one request line and a blank line.
//!
//! Run it as:
//!
//! ```text
//! cargo run --release -p umi-crawl --example adversarial_origin -- \
//!     --port 8099 --log /tmp/origin.tsv
//! ```

use std::collections::HashSet;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long a 429 asks the crawler to wait.
///
/// Six seconds, and the number is chosen to be binding rather than to be
/// realistic. Doc 07.6 multiplies the host's delay by four on a 429, so a
/// crawler that threw the header away and used its own backoff would still
/// wait four seconds and would still look polite. Anything at or under four
/// seconds here tests nothing. Six is the smallest round number above it.
const RETRY_AFTER_SECS: u64 = 6;

/// How long a slow page takes to finish, in total.
///
/// Over `umi_state::SLOW_MS`, which is two seconds, so that a slow page lands
/// on doc 07.6's 1.3 rung rather than being counted as a fast answer. That
/// rung is the interesting one: it is the crawler easing off on latency alone,
/// before the origin has failed at anything, which is the whole argument for
/// watching latency instead of waiting for errors.
///
/// Still comfortably inside `FetchConfig`'s ten second read timeout, so a slow
/// page is a page that arrives rather than a timeout wearing a disguise. The
/// two failures are different and the gate wants the first one.
const TRICKLE_TOTAL: Duration = Duration::from_millis(2500);

/// How many pieces a slow body arrives in.
const TRICKLE_PIECES: usize = 6;

/// How many pages the site has.
///
/// The crawl walks a graph rather than a list, so this is the number of
/// distinct paths that exist, and a page links onward to two more until it
/// runs out.
///
/// Small, because doc 07.6 is deliberately asymmetric and the run has to
/// finish. A 429 or a 503 multiplies the delay by four and a clean answer
/// divides it by 1.11, so an origin that failed one request in five would
/// drive the crawler to the one minute ceiling and hold it there. That is the
/// correct behaviour and it is not a thing to measure for an hour, so this
/// site has two of each bad behaviour and the rest are ordinary pages.
const PAGES: u32 = 24;

/// What this origin does to a request.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Behaviour {
    /// A normal 200 with a page on it.
    Ok,
    /// A 429 with `Retry-After`, once, then a 200.
    RateLimited,
    /// A 503 with no `Retry-After` at all, every time.
    ServerError,
    /// A 200 whose body arrives in pieces over more than a second.
    Slow,
    /// The request is read and the connection is closed with nothing written.
    Reset,
}

impl Behaviour {
    /// The word that goes in the log.
    const fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::RateLimited => "429",
            Self::ServerError => "503",
            Self::Slow => "slow",
            Self::Reset => "reset",
        }
    }

    /// The path prefix that selects it.
    const fn prefix(self) -> &'static str {
        match self {
            Self::Ok => "/ok/",
            Self::RateLimited => "/429/",
            Self::ServerError => "/503/",
            Self::Slow => "/slow/",
            Self::Reset => "/reset/",
        }
    }

    /// Every behaviour, for parsing a path back into one.
    const ALL: [Self; 5] = [
        Self::Ok,
        Self::Slow,
        Self::RateLimited,
        Self::ServerError,
        Self::Reset,
    ];

    /// What page `n` does.
    ///
    /// Written out rather than computed from a modulus, so that the mix is
    /// readable and so that the bad pages are spread through the walk instead
    /// of arriving together. The gate asks for the four behaviours interleaved
    /// and this is where that happens: the crawler meets a reset while it is
    /// still backed off from a 503, which is the case where a limiter that
    /// keeps its state in the wrong place goes wrong.
    const fn of(n: u32) -> Self {
        match n {
            5 | 17 => Self::RateLimited,
            9 | 18 => Self::ServerError,
            13 | 21 => Self::Reset,
            3 | 7 | 11 | 15 => Self::Slow,
            _ => Self::Ok,
        }
    }

    /// The behaviour a path asks for, and the page number on it.
    fn parse(path: &str) -> Option<(Self, u32)> {
        for kind in Self::ALL {
            if let Some(rest) = path.strip_prefix(kind.prefix()) {
                return rest.parse().ok().map(|n| (kind, n));
            }
        }
        None
    }
}

/// The path of page `n`.
fn page_path(n: u32) -> String {
    format!("{}{n}", Behaviour::of(n).prefix())
}

/// A page with links onward to two more, so the crawl has somewhere to go.
fn page_body(n: u32) -> String {
    let mut out = format!(
        "<html lang='en'><head><title>Page {n}</title></head><body><h1>Page {n}</h1>\
         <p>This page exists so that the crawler has a reason to come back to \
         this origin, and so that the paragraph is long enough that the \
         extraction produces a row rather than deciding the page is empty.</p>"
    );
    for next in [n * 2 + 1, n * 2 + 2] {
        if next < PAGES {
            out.push_str(&format!(
                "<p><a href='{}'>page {next}</a></p>",
                page_path(next)
            ));
        }
    }
    out.push_str("</body></html>");
    out
}

/// The front page, which links at every behaviour at once.
fn front_body() -> String {
    let mut out = String::from(
        "<html lang='en'><head><title>Adversary</title></head><body><h1>Adversary</h1>\
         <p>An origin that returns rate limits, server errors, slow bodies and \
         connection resets, so that a crawler can be measured against it from \
         the outside.</p>",
    );
    for n in 1..8 {
        out.push_str(&format!("<p><a href='{}'>page {n}</a></p>", page_path(n)));
    }
    out.push_str("</body></html>");
    out
}

/// One line per request, appended as it arrives.
///
/// Opened once and locked per write rather than buffered, because the file has
/// to survive the origin being killed at the end of a run and a buffered line
/// is a line that might not be there.
struct Log {
    file: Mutex<std::fs::File>,
    started: std::time::Instant,
}

impl Log {
    /// Create or truncate the log at `path`.
    fn create(path: &str) -> std::io::Result<Self> {
        Ok(Self {
            file: Mutex::new(std::fs::File::create(path)?),
            started: std::time::Instant::now(),
        })
    }

    /// Write one arrival.
    ///
    /// Both clocks, on purpose. The elapsed one is monotonic and is what the
    /// gaps are measured with, and the wall clock one is what lines the log up
    /// against the crawler's own output when something needs explaining.
    fn note(&self, path: &str, behaviour: Behaviour, detail: &str) {
        let elapsed = self.started.elapsed().as_millis();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let line = format!(
            "{elapsed}\t{wall}\t{path}\t{}\t{detail}\n",
            behaviour.name()
        );
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(line.as_bytes());
            let _ = file.flush();
        }
    }
}

/// A complete HTTP/1.1 response, framed so the connection closes after it.
fn response(status: u16, reason: &str, headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
    let mut out = format!("HTTP/1.1 {status} {reason}\r\n");
    for (name, value) in headers {
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    out.push_str(&format!("content-length: {}\r\n", body.len()));
    out.push_str("connection: close\r\n\r\n");
    let mut bytes = out.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

/// An ordinary HTML 200.
fn html(body: &str) -> Vec<u8> {
    response(
        200,
        "OK",
        &[("content-type", "text/html; charset=utf-8".to_owned())],
        body.as_bytes(),
    )
}

/// Read the request line off a socket, stopping at the blank line.
///
/// A byte at a time, which is slow and does not matter: the whole point of
/// this origin is that it is slower than the crawler, and reading a request
/// head is a few hundred bytes.
async fn read_path(socket: &mut TcpStream) -> Option<String> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match socket.read(&mut byte).await {
            Ok(0) | Err(_) => return None,
            Ok(_) => head.push(byte[0]),
        }
        if head.len() > 16 * 1024 {
            return None;
        }
    }
    let text = String::from_utf8_lossy(&head);
    let line = text.lines().next()?;
    line.split_whitespace().nth(1).map(str::to_owned)
}

/// Write a body in pieces, waiting between them.
async fn trickle(socket: &mut TcpStream, bytes: &[u8]) {
    let piece = bytes.len().div_ceil(TRICKLE_PIECES).max(1);
    let gap = TRICKLE_TOTAL / u32::try_from(TRICKLE_PIECES).unwrap_or(1);
    for chunk in bytes.chunks(piece) {
        if socket.write_all(chunk).await.is_err() {
            return;
        }
        let _ = socket.flush().await;
        tokio::time::sleep(gap).await;
    }
}

/// Serve one connection.
async fn serve(mut socket: TcpStream, log: Arc<Log>, seen_429: Arc<Mutex<HashSet<String>>>) {
    let Some(path) = read_path(&mut socket).await else {
        return;
    };

    if path == "/robots.txt" {
        // Allow everything and publish nothing about the crawl delay, so that
        // the rate the gate measures is the crawler's own politeness rather
        // than a number this origin handed it. A `Crawl-delay` here would make
        // the test pass for the wrong reason.
        log.note(&path, Behaviour::Ok, "robots");
        let body = "User-agent: *\nAllow: /\n";
        let head = response(
            200,
            "OK",
            &[("content-type", "text/plain".to_owned())],
            body.as_bytes(),
        );
        let _ = socket.write_all(&head).await;
        return;
    }

    if path == "/" {
        log.note(&path, Behaviour::Ok, "front");
        let _ = socket.write_all(&html(&front_body())).await;
        return;
    }

    let Some((behaviour, n)) = Behaviour::parse(&path) else {
        log.note(&path, Behaviour::Ok, "404");
        let _ = socket
            .write_all(&response(404, "Not Found", &[], b"no"))
            .await;
        return;
    };

    match behaviour {
        Behaviour::Ok => {
            log.note(&path, behaviour, "200");
            let _ = socket.write_all(&html(&page_body(n))).await;
        }
        Behaviour::Slow => {
            // Logged on arrival rather than on completion, because the arrival
            // time is what the gaps are measured from. A crawler that started
            // its next request while this body was still arriving would show
            // up as two arrivals inside the trickle window.
            log.note(&path, behaviour, "200 trickled");
            let bytes = html(&page_body(n));
            trickle(&mut socket, &bytes).await;
        }
        Behaviour::RateLimited => {
            // The first request for any 429 path gets the 429. After that this
            // origin relents, so a crawler that waited out the `Retry-After`
            // makes progress and one that ignored it does not get a second
            // chance to look polite.
            let first = seen_429
                .lock()
                .map_or(true, |mut seen| seen.insert(path.clone()));
            if first {
                log.note(
                    &path,
                    behaviour,
                    &format!("429 retry-after {RETRY_AFTER_SECS}"),
                );
                let head = response(
                    429,
                    "Too Many Requests",
                    &[("retry-after", RETRY_AFTER_SECS.to_string())],
                    b"slow down",
                );
                let _ = socket.write_all(&head).await;
            } else {
                log.note(&path, behaviour, "200 after 429");
                let _ = socket.write_all(&html(&page_body(n))).await;
            }
        }
        Behaviour::ServerError => {
            // No `Retry-After` on this one, which is the common case in the
            // wild and the one where the crawler has to pick a backoff itself.
            log.note(&path, behaviour, "503");
            let head = response(503, "Service Unavailable", &[], b"down");
            let _ = socket.write_all(&head).await;
        }
        Behaviour::Reset => {
            // Read the request, write nothing, drop the socket. From the
            // client this is a connection that went away before any response
            // arrived, which is a different failure from a timeout and from a
            // 5xx, and is the one that catches a pool handing back a socket
            // the origin has already given up on.
            //
            // A close rather than a kernel level RST. Forcing the second one
            // needs `SO_LINGER` with a zero timeout, tokio deprecated that
            // because it blocks the thread on drop, and the client cannot tell
            // the difference: both arrive as a connection error before the
            // response head, which is the case the crawler has to handle.
            log.note(&path, behaviour, "closed with no response");
            drop(socket);
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut port = 8099u16;
    let mut log_path = String::from("/tmp/adversarial-origin.tsv");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => port = args.next().and_then(|v| v.parse().ok()).unwrap_or(port),
            "--log" => log_path = args.next().unwrap_or(log_path),
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
    }

    let log = Arc::new(Log::create(&log_path)?);
    let seen_429 = Arc::new(Mutex::new(HashSet::new()));
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("listening on http://127.0.0.1:{port}, logging to {log_path}");

    loop {
        let (socket, _) = listener.accept().await?;
        let log = Arc::clone(&log);
        let seen_429 = Arc::clone(&seen_429);
        tokio::spawn(serve(socket, log, seen_429));
    }
}
