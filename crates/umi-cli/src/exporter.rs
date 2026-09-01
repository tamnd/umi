//! The socket doc 15.4's series are served on.
//!
//! `umi-metrics` holds the numbers and deliberately holds no socket, because
//! the crate that has a listener should be the one that opens it. This is that
//! listener. It serves `GET /metrics` in the Prometheus text format and
//! nothing else, over HTTP/1.1, on the address the operator named.
//!
//! # Why this is not a web framework
//!
//! One route, no body to parse, no state to share beyond an `Arc`, no routing
//! table, and a client that is a scraper on the same box rather than a
//! browser. What that needs is a read, a match on the request line and a write,
//! which is what is below. Pulling in a server framework to get those three
//! things would add a dependency tree to the `umi` binary that every operator
//! then has to have audited, and doc 02 is clear that a dependency is a thing
//! you carry rather than a thing you get.
//!
//! The trade is real and it is worth naming. This speaks a small and strict
//! subset of HTTP/1.1: one request per connection, `Connection: close` on every
//! response, no keep alive, no chunked request bodies, no HTTP/2. Prometheus,
//! `curl` and every other scraper handle that, because closing after the
//! response has been legal since HTTP/1.0 and is what `Connection: close`
//! means. Anything that needs more than that should be talking to `umid` over
//! the admin listener in doc 14.6, not to this.
//!
//! # What it refuses
//!
//! Doc 14.6 and doc 15.4 both say the admin surface is localhost only by
//! default, and this goes further: it binds exactly where it is told and warns
//! when that is not a loopback address. It never guesses, it has no
//! `0.0.0.0` default, and the flag that turns it on is off. A metrics endpoint
//! is a description of what a box is doing and how much of it, and that is not
//! something to put on a public interface by accident.
//!
//! Requests are capped at eight kilobytes and connections that go quiet are
//! dropped after ten seconds. Both exist because this listens on a socket and
//! a socket is reachable by things that are not a scraper.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use umi_metrics::{Metrics, encode};

use crate::Error;

/// The most of a request this will read before giving up on it.
///
/// A scrape is a request line and a handful of headers, so anything past this
/// is not a scraper. Small on purpose: the buffer is per connection and the
/// point of the cap is that a client cannot make this process allocate.
const REQUEST_CAP: usize = 8 * 1024;

/// How long a connection may go without saying anything.
///
/// Long enough that a scraper on a loaded box still gets its request in, short
/// enough that a connection opened and abandoned is not a slot held forever.
const IDLE: Duration = Duration::from_secs(10);

/// A listener serving [`Metrics`] for as long as this is alive.
///
/// Dropping it aborts the accept loop, which is what makes the exporter end
/// when the crawl does without anything having to remember to stop it.
pub struct Exporter {
    /// The numbers being served. The crawl writes these and the accept loop
    /// reads them, which is why they are behind an `Arc` and not a lock: every
    /// counter and gauge in the registry is an atomic.
    metrics: Arc<Metrics>,
    /// Where it actually bound, which is not always where it was asked to bind
    /// because port 0 means "any".
    addr: SocketAddr,
    /// The accept loop.
    task: tokio::task::JoinHandle<()>,
}

impl Exporter {
    /// Bind `addr` and start serving.
    ///
    /// Binding here rather than inside the spawned task, and returning the
    /// error rather than logging it, because an operator who asked for metrics
    /// and did not get them should find out before the crawl starts rather
    /// than from the absence of a dashboard an hour later. This is the same
    /// rule the publisher follows with its token.
    pub async fn start(addr: &str) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|cause| Error::Metrics(format!("cannot listen on {addr}: {cause}")))?;
        let addr = listener.local_addr().map_err(|cause| {
            Error::Metrics(format!("cannot read the listening address: {cause}"))
        })?;
        let metrics = Arc::new(Metrics::new());
        let serving = Arc::clone(&metrics);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    // An accept that fails is usually the file descriptor
                    // limit, which a crawl at a window of 1024 can reach. The
                    // next one will very likely work and there is nothing
                    // useful to do here but try again, so this does not take
                    // the exporter down over it.
                    continue;
                };
                let metrics = Arc::clone(&serving);
                tokio::spawn(async move {
                    // One connection failing is one scrape missing.
                    let _ = serve(stream, &metrics).await;
                });
            }
        });
        Ok(Self {
            metrics,
            addr,
            task,
        })
    }

    /// The numbers to write into.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Where it is listening.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether the address it bound is reachable from off this box.
    ///
    /// Used for the one line of warning the crawl prints. Not used to refuse,
    /// because an operator running behind a firewall with a scraper on another
    /// host has a good reason to bind an interface, and a tool that will not
    /// do the thing you asked is a tool people work around.
    pub fn is_public(&self) -> bool {
        !self.addr.ip().is_loopback()
    }
}

impl Drop for Exporter {
    /// Stops accepting.
    ///
    /// Connections already being served are dropped with it. A scrape that
    /// arrived in the same instant the crawl ended is a scrape nobody needed.
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl std::fmt::Debug for Exporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Exporter({})", self.addr)
    }
}

/// Read one request, write one response, close.
async fn serve(mut stream: TcpStream, metrics: &Metrics) -> std::io::Result<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };
    let response = match route(&request) {
        Route::Metrics => ok("text/plain; version=0.0.4; charset=utf-8", &encode(metrics)),
        // A person who opens this in a browser to see what is on the port
        // should be told, rather than getting a 404 that looks like the
        // exporter is broken.
        Route::Root => ok("text/plain; charset=utf-8", "umi metrics: GET /metrics\n"),
        Route::Missing => status(404, "not found\n"),
        Route::Unsupported => status(405, "GET only\n"),
        Route::Malformed => status(400, "bad request\n"),
    };
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

/// The request line, or `None` if the client went away without sending one.
///
/// Only the first line is kept. Headers are read because the request is not
/// over until the blank line and a client that sent headers deserves to have
/// them taken off the socket, but nothing here looks at any of them.
async fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<String>> {
    let mut buffer = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    loop {
        let read = match tokio::time::timeout(IDLE, stream.read(&mut chunk)).await {
            Ok(read) => read?,
            // A connection that opened and then said nothing. Nothing to
            // answer and nothing worth logging.
            Err(_) => return Ok(None),
        };
        if read == 0 {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > REQUEST_CAP {
            // Past the cap the request is not a scrape, and reading more of it
            // to find out what it is would be doing what it wants.
            return Ok(Some(String::new()));
        }
        if let Some(end) = find_blank_line(&buffer) {
            let head = String::from_utf8_lossy(&buffer[..end]).into_owned();
            return Ok(Some(head.lines().next().unwrap_or_default().to_owned()));
        }
    }
}

/// Where the head of the request ends, which is the first `\r\n\r\n`.
fn find_blank_line(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|four| four == b"\r\n\r\n")
}

/// What a request line asks for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Route {
    /// `GET /metrics`.
    Metrics,
    /// `GET /`.
    Root,
    /// A GET of something else.
    Missing,
    /// A method that is not GET.
    Unsupported,
    /// Not a request line at all.
    Malformed,
}

/// Read a request line.
///
/// Deliberately strict. A query string is allowed and ignored, because
/// scrapers append one, and everything else about the line has to be the shape
/// HTTP says it is.
fn route(line: &str) -> Route {
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Route::Malformed;
    };
    if parts.next().is_some() || !version.starts_with("HTTP/") {
        return Route::Malformed;
    }
    // HEAD is not answered as if it were GET. A scraper does not send one, and
    // answering it correctly means writing the headers without the body, which
    // is a special case in the writer for no caller.
    if method != "GET" {
        return Route::Unsupported;
    }
    match target.split('?').next().unwrap_or_default() {
        "/metrics" => Route::Metrics,
        "/" => Route::Root,
        _ => Route::Missing,
    }
}

/// A 200 carrying `body`.
fn ok(content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

/// Any other answer, which is always a short line of plain text.
fn status(code: u16, body: &str) -> String {
    let reason = match code {
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {code} {reason}\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scrape_is_routed_to_the_metrics_page() {
        assert_eq!(route("GET /metrics HTTP/1.1"), Route::Metrics);
    }

    #[test]
    fn a_scraper_may_append_a_query_string() {
        assert_eq!(route("GET /metrics?collect=all HTTP/1.1"), Route::Metrics);
    }

    #[test]
    fn the_root_says_where_the_metrics_are() {
        assert_eq!(route("GET / HTTP/1.1"), Route::Root);
    }

    #[test]
    fn anything_else_is_missing_rather_than_served() {
        assert_eq!(route("GET /../etc/passwd HTTP/1.1"), Route::Missing);
        assert_eq!(route("GET /metrics/extra HTTP/1.1"), Route::Missing);
    }

    #[test]
    fn a_method_that_is_not_get_is_refused_including_head() {
        assert_eq!(route("POST /metrics HTTP/1.1"), Route::Unsupported);
        assert_eq!(route("HEAD /metrics HTTP/1.1"), Route::Unsupported);
        assert_eq!(route("DELETE /metrics HTTP/1.1"), Route::Unsupported);
    }

    #[test]
    fn a_line_that_is_not_a_request_line_is_malformed() {
        assert_eq!(route(""), Route::Malformed);
        assert_eq!(route("GET /metrics"), Route::Malformed);
        assert_eq!(route("GET /metrics HTTP/1.1 extra"), Route::Malformed);
        assert_eq!(route("GET /metrics SPDY/3"), Route::Malformed);
    }

    #[test]
    fn a_response_carries_the_length_of_its_own_body() {
        let response = ok("text/plain", "hello\n");
        assert!(response.contains("Content-Length: 6\r\n"));
        assert!(response.ends_with("\r\n\r\nhello\n"));
        assert!(response.contains("Connection: close\r\n"));
    }

    #[test]
    fn the_head_of_a_request_ends_at_the_blank_line() {
        assert_eq!(find_blank_line(b"GET / HTTP/1.1\r\n\r\n"), Some(14));
        assert_eq!(find_blank_line(b"GET / HTTP/1.1\r\n"), None);
    }

    #[tokio::test]
    async fn the_exporter_serves_a_scrape_and_stops_when_it_is_dropped() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpStream;

        // Port zero, so the test does not need a port nobody else is using.
        let exporter = Exporter::start("127.0.0.1:0").await.expect("bind");
        assert!(!exporter.is_public());
        exporter.metrics().bytes_in().add(4096);
        let addr = exporter.addr();

        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write");
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.expect("read");
        assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
        assert!(answer.contains("umi_bytes_in_total 4096"), "{answer}");

        drop(exporter);
        // The listener is closed with the task that owned it, so a connect to
        // the same port no longer gets an exporter. Retried because the abort
        // and the close are not instantaneous.
        for _ in 0..50 {
            if TcpStream::connect(addr).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the exporter kept listening after it was dropped");
    }

    #[tokio::test]
    async fn a_request_for_something_else_is_a_404_and_a_post_is_a_405() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpStream;

        let exporter = Exporter::start("127.0.0.1:0").await.expect("bind");
        let addr = exporter.addr();

        for (request, expected) in [
            (&b"GET /nope HTTP/1.1\r\n\r\n"[..], "HTTP/1.1 404"),
            (&b"POST /metrics HTTP/1.1\r\n\r\n"[..], "HTTP/1.1 405"),
            (&b"nonsense\r\n\r\n"[..], "HTTP/1.1 400"),
        ] {
            let mut stream = TcpStream::connect(addr).await.expect("connect");
            stream.write_all(request).await.expect("write");
            let mut answer = String::new();
            stream.read_to_string(&mut answer).await.expect("read");
            assert!(answer.starts_with(expected), "{answer}");
        }
    }
}
