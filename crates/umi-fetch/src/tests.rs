//! Tests against a real socket.
//!
//! Every one of these runs against a hand written origin on localhost rather
//! than a mock, because the things worth testing here are timeouts, a body that
//! arrives in pieces and an origin that stops talking, and none of those exist
//! above the socket. The origin speaks HTTP/1.1 by hand, which is about sixty
//! lines and lets a test do things no real server would agree to.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use base64::Engine as _;

use super::webbotauth::{COVERED, LIFETIME_SECS, Params, signature_base, verify};
use super::{
    Directory, Failure, FetchConfig, Fetcher, Jwk, Ladder, Media, Outcome, RetryAfter, Revalidator,
    SignatureError, Signer, Stage, Tier, USER_AGENT,
};

/// What the scripted origin does once it has read a request.
enum Reply {
    /// Write these bytes and close.
    Bytes(Vec<u8>),
    /// Write these bytes in pieces, waiting between them. A slow origin.
    Trickle {
        bytes: Vec<u8>,
        piece: usize,
        gap: Duration,
    },
    /// Write these bytes and then say nothing, holding the connection open. A
    /// stalled origin, which is a different failure from a slow one.
    Silence(Vec<u8>),
}

impl Reply {
    /// A complete response, framed so the connection closes after it.
    fn response(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Self {
        let mut out = format!("HTTP/1.1 {status} X\r\n");
        for (name, value) in headers {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
        // A 304 is defined to have no body, and sending a length with one is
        // the kind of thing that makes a client wait for bytes that never come.
        if status != 304 {
            out.push_str(&format!("content-length: {}\r\n", body.len()));
        }
        out.push_str("connection: close\r\n\r\n");
        let mut bytes = out.into_bytes();
        bytes.extend_from_slice(body);
        Self::Bytes(bytes)
    }
}

/// A scripted HTTP origin on a loopback port.
struct Origin {
    addr: SocketAddr,
    /// Every request head the origin has read, in order.
    seen: Arc<Mutex<Vec<String>>>,
    /// The most requests that were ever being served at the same time.
    peak: Arc<AtomicUsize>,
}

impl Origin {
    async fn start<F>(reply: F) -> Self
    where
        F: Fn(&str) -> Reply + Send + Sync + 'static,
    {
        Self::holding(Duration::ZERO, reply).await
    }

    /// The same, but each connection is held open for `hold` before it answers.
    ///
    /// The hold is what makes the concurrency count mean something: without it
    /// a request is served faster than the next one can be made, and the peak
    /// would be one however many are allowed. The in flight count drops before
    /// the reply is written, so a client that releases its permit the instant
    /// the bytes land cannot make the next connection overlap this one.
    async fn holding<F>(hold: Duration, reply: F) -> Self
    where
        F: Fn(&str) -> Reply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("a bound port has an address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let peak = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(AtomicUsize::new(0));

        let reply = Arc::new(reply);
        let listen_seen = Arc::clone(&seen);
        let listen_peak = Arc::clone(&peak);

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let reply = Arc::clone(&reply);
                let seen = Arc::clone(&listen_seen);
                let peak = Arc::clone(&listen_peak);
                let live = Arc::clone(&live);

                tokio::spawn(async move {
                    let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);

                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match socket.read(&mut byte).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    let request = String::from_utf8_lossy(&head).into_owned();
                    let scripted = reply(&request);
                    seen.lock().expect("not poisoned").push(request);

                    if !hold.is_zero() {
                        tokio::time::sleep(hold).await;
                    }
                    live.fetch_sub(1, Ordering::SeqCst);

                    match scripted {
                        Reply::Bytes(bytes) => {
                            let _ = socket.write_all(&bytes).await;
                        }
                        Reply::Trickle { bytes, piece, gap } => {
                            for chunk in bytes.chunks(piece) {
                                if socket.write_all(chunk).await.is_err() {
                                    break;
                                }
                                let _ = socket.flush().await;
                                tokio::time::sleep(gap).await;
                            }
                        }
                        Reply::Silence(bytes) => {
                            let _ = socket.write_all(&bytes).await;
                            let _ = socket.flush().await;
                            // Open, and with nothing on it, until the test ends.
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                        }
                    }
                    let _ = socket.shutdown().await;
                });
            }
        });

        Self { addr, seen, peak }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn requests(&self) -> Vec<String> {
        self.seen.lock().expect("not poisoned").clone()
    }
}

/// Short timeouts, so a test that waits for one finishes in under a second.
fn impatient() -> FetchConfig {
    FetchConfig {
        connect_timeout: Duration::from_millis(500),
        read_timeout: Duration::from_millis(200),
        total_timeout: Duration::from_millis(700),
        ..FetchConfig::default()
    }
}

fn fetcher(config: FetchConfig) -> Fetcher {
    Fetcher::with_config(config).expect("the client builds")
}

#[tokio::test]
async fn a_lease_above_the_top_of_the_ladder_is_served_by_the_top_of_the_ladder() {
    // Doc 05.8 escalates a host to T2 whether or not this binary has T2, and
    // the whole crawl would stop for that host if a missing rung were an
    // error. So the ladder serves the highest rung it has and lets the block
    // count keep climbing, which is the outcome doc 05.8 already knows how to
    // handle.
    let origin =
        Origin::start(|_| Reply::response(200, &[("content-type", "text/html")], b"ok")).await;
    let ladder = Ladder::with_config(FetchConfig::default()).expect("the ladder builds");

    for tier in Tier::ALL {
        let outcome = ladder
            .fetch(&origin.url("/"), None, tier)
            .await
            .expect("a url we can crawl");
        assert!(matches!(outcome, Outcome::Ok(_)), "{tier:?}: {outcome:?}");
    }
    assert_eq!(origin.requests().len(), Tier::ALL.len());
}

#[test]
fn the_ladder_says_which_rungs_it_has() {
    // The build without `emulation` has one rung and the build with it has
    // two. `umi get` prints this and refuses to pretend, which is the only
    // reason the constant is public.
    let expected = if cfg!(feature = "emulation") {
        Tier::Emulated
    } else {
        Tier::Plain
    };
    assert_eq!(Ladder::highest(), expected);
}

#[tokio::test]
async fn a_plain_fetch_comes_back_whole() {
    let body = b"<!doctype html><html><title>hi</title></html>";
    let origin = Origin::start(move |_| {
        Reply::response(
            200,
            &[
                ("content-type", "text/html; charset=utf-8"),
                ("etag", "\"v1\""),
                ("last-modified", "Sun, 06 Nov 1994 08:49:37 GMT"),
                ("set-cookie", "session=secret"),
                ("x-cache", "HIT"),
            ],
            body,
        )
    })
    .await;

    let outcome = fetcher(FetchConfig::default())
        .fetch(&origin.url("/"), None)
        .await
        .expect("the url parses");

    let Outcome::Ok(page) = outcome else {
        panic!("expected a page, got {outcome:?}");
    };
    assert_eq!(page.status, 200);
    assert_eq!(page.body.as_ref(), body);
    assert_eq!(page.body_digest, *blake3::hash(body).as_bytes());
    assert_eq!(page.media, Media::Html);
    assert_eq!(
        page.content_type.as_deref(),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(page.revalidate.etag.as_deref(), Some("\"v1\""));
    assert_eq!(page.revalidate.last_modified_ms, Some(784_111_777_000));
    assert!(page.redirects.is_empty());
    assert!(page.final_url.ends_with('/'));

    let kept: Vec<&str> = page
        .headers_kept
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert!(kept.contains(&"content-type") && kept.contains(&"etag"));
    assert!(
        !kept.contains(&"set-cookie") && !kept.contains(&"x-cache"),
        "doc 11.5 publishes sixteen headers and these are not two of them"
    );
    assert_ne!(
        page.headers_digest, [0u8; 32],
        "the digest covers the dropped headers too"
    );
}

#[tokio::test]
async fn we_say_who_we_are_on_every_request() {
    // Doc 07.1's whole argument is that a site operator can identify us in one
    // log search. That only works if the string is the one on the bot page.
    let origin = Origin::start(|_| Reply::response(200, &[], b"ok")).await;
    let _ = fetcher(FetchConfig::default())
        .fetch(&origin.url("/"), None)
        .await;

    let request = origin.requests().into_iter().next().expect("one request");
    assert!(
        request.contains(USER_AGENT),
        "the user agent was not {USER_AGENT} in {request:?}"
    );
    assert_eq!(USER_AGENT, "umi/1.0 (+https://umi.dev/bot)");
}

#[tokio::test]
async fn a_conditional_request_sends_both_validators_and_a_304_holds() {
    // Doc 05.3 sends both because origins are inconsistent about which one they
    // honour, and the pair costs about sixty bytes.
    let origin = Origin::start(|request| {
        let both = request.contains("if-none-match: \"v1\"")
            && request.contains("if-modified-since: Sun, 06 Nov 1994 08:49:37 GMT");
        if both {
            Reply::response(304, &[("etag", "\"v2\"")], b"")
        } else {
            Reply::response(200, &[], b"a conditional header was missing")
        }
    })
    .await;

    let outcome = fetcher(FetchConfig::default())
        .fetch(
            &origin.url("/"),
            Some(&Revalidator {
                etag: Some("\"v1\"".to_owned()),
                last_modified_ms: Some(784_111_777_000),
            }),
        )
        .await
        .expect("the url parses");

    let Outcome::NotModified { revalidate, .. } = outcome else {
        panic!("expected a 304, got {outcome:?}");
    };
    assert_eq!(
        revalidate.etag.as_deref(),
        Some("\"v2\""),
        "an origin may refresh the etag on a 304 and the new one has to be kept"
    );
}

#[tokio::test]
async fn a_304_is_not_read_as_a_redirect() {
    // It is a 3xx, so a client that checks the range before the status walks
    // into a `Location` that is not there and calls the response malformed.
    let origin = Origin::start(|_| Reply::response(304, &[], b"")).await;
    let outcome = fetcher(FetchConfig::default())
        .fetch(&origin.url("/"), None)
        .await
        .expect("the url parses");
    assert!(
        matches!(outcome, Outcome::NotModified { .. }),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_304_carrying_a_body_is_still_a_304() {
    // The third of doc 05.3's misbehaviours. An origin that sends a body with
    // a 304 is doing something HTTP does not allow, and the two ways to get it
    // wrong both cost real money. Reading the body as content would store a
    // page the origin just told us had not changed, and treating the response
    // as malformed would fail a url that answered correctly apart from some
    // bytes nobody asked for.
    //
    // Framed by hand rather than through `Reply::response`, which will not
    // send a content-length on a 304 precisely because this is not a thing an
    // origin is supposed to do.
    let body = "<html><body>the origin should not have sent this</body></html>";
    let head = format!(
        "HTTP/1.1 304 X\r\netag: \"v2\"\r\ncontent-type: text/html\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let mut raw = head.into_bytes();
    raw.extend_from_slice(body.as_bytes());
    let origin = Origin::start(move |_| Reply::Bytes(raw.clone())).await;

    let outcome = fetcher(FetchConfig::default())
        .fetch(
            &origin.url("/"),
            Some(&Revalidator {
                etag: Some("\"v1\"".to_owned()),
                last_modified_ms: None,
            }),
        )
        .await
        .expect("the url parses");

    let Outcome::NotModified { revalidate, .. } = outcome else {
        panic!("a 304 with a body was not read as a 304: {outcome:?}");
    };
    assert_eq!(revalidate.etag.as_deref(), Some("\"v2\""));
}

#[tokio::test]
async fn a_same_domain_redirect_is_followed_and_written_down() {
    let origin = Origin::start(|request| {
        if request.starts_with("GET /a ") {
            Reply::response(301, &[("location", "/b")], b"")
        } else {
            Reply::response(200, &[], b"arrived")
        }
    })
    .await;

    let outcome = fetcher(FetchConfig::default())
        .fetch(&origin.url("/a"), None)
        .await
        .expect("the url parses");

    let Outcome::Ok(page) = outcome else {
        panic!("expected a page, got {outcome:?}");
    };
    assert_eq!(page.body.as_ref(), b"arrived");
    assert_eq!(page.redirects.len(), 1);
    assert_eq!(page.redirects[0].status, 301);
    assert!(page.redirects[0].from.ends_with("/a"));
    assert!(page.redirects[0].to.ends_with("/b"));
    assert!(
        page.final_url.ends_with("/b"),
        "doc 11.2 resolves relative links against the final url, so it is the one to store"
    );
}

#[tokio::test]
async fn a_redirect_off_the_domain_stops_rather_than_being_followed() {
    // Doc 04.7. Following it would fetch a host whose robots.txt nobody has
    // read, which is the politeness bug this rule exists to remove.
    let origin = Origin::start(|_| {
        Reply::response(302, &[("location", "https://example.com/elsewhere")], b"")
    })
    .await;

    let outcome = fetcher(FetchConfig::default())
        .fetch(&origin.url("/"), None)
        .await
        .expect("the url parses");

    let Outcome::RedirectedOffDomain {
        target,
        status,
        redirects,
    } = outcome
    else {
        panic!("expected an off domain stop, got {outcome:?}");
    };
    assert_eq!(target, "https://example.com/elsewhere");
    assert_eq!(status, 302);
    assert!(redirects.is_empty());
    assert_eq!(
        origin.requests().len(),
        1,
        "the second host must not have been contacted"
    );
}

#[tokio::test]
async fn a_redirect_loop_stops_at_the_hop_cap() {
    let origin = Origin::start(|_| Reply::response(302, &[("location", "/round")], b"")).await;

    let outcome = fetcher(FetchConfig {
        max_redirects: 3,
        ..FetchConfig::default()
    })
    .fetch(&origin.url("/round"), None)
    .await
    .expect("the url parses");

    assert!(
        matches!(
            outcome,
            Outcome::Failed {
                failure: Failure::Malformed,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        origin.requests().len(),
        4,
        "three hops, plus the request that started the chain"
    );
}

#[tokio::test]
async fn a_declared_length_over_the_cap_is_refused_before_the_body_is_read() {
    let origin = Origin::start(|_| Reply::response(200, &[], &vec![b'x'; 4096])).await;

    let outcome = fetcher(FetchConfig {
        body_cap: 1024,
        ..FetchConfig::default()
    })
    .fetch(&origin.url("/"), None)
    .await
    .expect("the url parses");

    assert_eq!(
        outcome,
        Outcome::Failed {
            status: Some(200),
            failure: Failure::TooLarge,
            retry_after: None,
        }
    );
}

#[tokio::test]
async fn a_body_that_declares_no_length_is_still_capped() {
    // `Content-Length` is optional, and an origin that marks the end of a body
    // by closing the connection declares nothing at all. The cap has to hold
    // against the bytes that arrive or it is not a cap.
    let origin = Origin::start(|_| {
        let mut out = b"HTTP/1.1 200 X\r\nconnection: close\r\n\r\n".to_vec();
        out.extend_from_slice(&vec![b'x'; 8192]);
        Reply::Bytes(out)
    })
    .await;

    let outcome = fetcher(FetchConfig {
        body_cap: 1024,
        ..FetchConfig::default()
    })
    .fetch(&origin.url("/"), None)
    .await
    .expect("the url parses");

    assert_eq!(
        outcome,
        Outcome::Failed {
            status: Some(200),
            failure: Failure::TooLarge,
            retry_after: None,
        }
    );
}

#[tokio::test]
async fn an_origin_that_goes_quiet_mid_body_hits_the_read_timeout() {
    // The case a total timeout alone does not catch quickly: the headers
    // arrive, the connection stays open, and nothing else is ever sent.
    let origin = Origin::start(|_| {
        Reply::Silence(b"HTTP/1.1 200 X\r\ncontent-length: 100\r\n\r\npartial".to_vec())
    })
    .await;

    let outcome = fetcher(impatient())
        .fetch(&origin.url("/"), None)
        .await
        .expect("the url parses");

    assert_eq!(
        outcome,
        Outcome::Failed {
            status: Some(200),
            failure: Failure::Timeout(Stage::Read),
            retry_after: None,
        }
    );
}

#[tokio::test]
async fn an_origin_that_trickles_forever_hits_the_total_timeout() {
    // Every piece arrives inside the read timeout, so the connection never
    // looks stalled. Without a total budget this holds a slot for as long as
    // the origin likes, which is how a handful of slow hosts take a crawl below
    // its rate.
    let origin = Origin::start(|_| {
        let mut bytes = b"HTTP/1.1 200 X\r\ncontent-length: 100000\r\n\r\n".to_vec();
        bytes.extend_from_slice(&vec![b'x'; 100_000]);
        Reply::Trickle {
            bytes,
            piece: 64,
            gap: Duration::from_millis(50),
        }
    })
    .await;

    let outcome = fetcher(impatient())
        .fetch(&origin.url("/"), None)
        .await
        .expect("the url parses");

    assert_eq!(
        outcome,
        Outcome::Failed {
            status: None,
            failure: Failure::Timeout(Stage::Total),
            retry_after: None,
        }
    );
}

/// One status, the headers that came with it, and what the pair should mean.
struct Case {
    status: u16,
    extra: &'static [(&'static str, &'static str)],
    expected: Outcome,
}

#[tokio::test]
async fn the_status_classes_land_where_the_scheduler_expects_them() {
    let failed = |status, failure| Outcome::Failed {
        status: Some(status),
        failure,
        retry_after: None,
    };
    let cases = [
        Case {
            status: 410,
            extra: &[],
            expected: Outcome::Gone,
        },
        Case {
            status: 404,
            extra: &[],
            expected: failed(404, Failure::NotFound),
        },
        Case {
            status: 500,
            extra: &[],
            expected: failed(500, Failure::ServerError),
        },
        Case {
            status: 403,
            extra: &[("cf-mitigated", "challenge")],
            expected: failed(403, Failure::Blocked),
        },
        Case {
            status: 503,
            extra: &[("server", "AkamaiGHost")],
            expected: failed(503, Failure::Blocked),
        },
        Case {
            status: 503,
            extra: &[],
            expected: failed(503, Failure::ServerError),
        },
    ];

    for Case {
        status,
        extra,
        expected,
    } in cases
    {
        let origin = Origin::start(move |_| Reply::response(status, extra, b"")).await;

        let outcome = fetcher(FetchConfig::default())
            .fetch(&origin.url("/"), None)
            .await
            .expect("the url parses");
        assert_eq!(outcome, expected, "status {status} with {extra:?}");
    }
}

#[tokio::test]
async fn a_rate_limit_is_not_a_block() {
    // They both mean back off and they mean different things about the tier.
    // Escalating to a browser because a site published a rate limit spends the
    // scarcest resource in the fleet on the one case that does not need it.
    let origin = Origin::start(|_| Reply::response(429, &[("retry-after", "60")], b"")).await;
    let outcome = fetcher(FetchConfig::default())
        .fetch(&origin.url("/"), None)
        .await
        .expect("the url parses");

    assert_eq!(
        outcome,
        Outcome::Failed {
            status: Some(429),
            failure: Failure::RateLimited,
            // Doc 07.6 honours this and a 429 has no body to carry it, so the
            // outcome is the only place it can survive the fetch.
            retry_after: Some(RetryAfter::After(60)),
        }
    );
    assert!(!outcome.is_block_signal());
    assert_eq!(outcome.wire(), "rate_limited");
}

#[tokio::test]
async fn a_rate_limit_with_a_vendor_marker_on_it_is_a_block() {
    // The same status from bot management is the other thing, and the marker is
    // the only way to tell the two apart from outside.
    let origin = Origin::start(|_| Reply::response(429, &[("x-datadome", "protected")], b"")).await;
    let outcome = fetcher(FetchConfig::default())
        .fetch(&origin.url("/"), None)
        .await
        .expect("the url parses");

    assert!(outcome.is_block_signal(), "{outcome:?}");
    assert_eq!(outcome.wire(), "blocked");
}

#[tokio::test]
async fn the_per_host_cap_holds_under_concurrency() {
    // Doc 05.4 caps us at 2 per host. The origin holds each connection open for
    // long enough that eight requests would overlap if nothing stopped them.
    let origin = Origin::holding(Duration::from_millis(80), |_| {
        Reply::response(200, &[], b"ok")
    })
    .await;

    let fetcher = fetcher(FetchConfig {
        per_host: 2,
        ..FetchConfig::default()
    });
    let url = origin.url("/");

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let fetcher = fetcher.clone();
        let url = url.clone();
        tasks.push(tokio::spawn(async move { fetcher.fetch(&url, None).await }));
    }
    for task in tasks {
        let outcome = task.await.expect("no panic").expect("the url parses");
        assert!(matches!(outcome, Outcome::Ok(_)), "{outcome:?}");
    }

    assert_eq!(origin.requests().len(), 8);
    let peak = origin.peak.load(Ordering::SeqCst);
    assert!(
        peak <= 2,
        "{peak} requests were in flight at once, the cap is 2"
    );
}

#[test]
fn a_host_with_a_request_in_flight_keeps_its_permits() {
    // A fleet at rate touches millions of hosts, so the permit table is swept.
    // The sweep must not take a semaphore that is in use: two requests to one
    // host holding different semaphores would each get the full allowance,
    // which is the one thing the table exists to prevent.
    let fetcher = fetcher(FetchConfig {
        host_table_cap: 4,
        ..FetchConfig::default()
    });
    let busy = fetcher.inner.permits("busy.example.com");
    for n in 0..64 {
        drop(fetcher.inner.permits(&format!("idle{n}.example.com")));
    }

    let live = fetcher.inner.live_hosts();
    assert!(
        live <= 4,
        "the host table grew to {live} entries past a cap of 4"
    );
    assert!(
        Arc::ptr_eq(&busy, &fetcher.inner.permits("busy.example.com")),
        "the busy host lost the semaphore its in flight request was counted against"
    );
}

#[tokio::test]
async fn nothing_listening_is_a_connect_failure_and_not_a_panic() {
    let outcome = fetcher(impatient())
        .fetch("http://127.0.0.1:1/", None)
        .await
        .expect("the url parses");
    assert!(
        matches!(
            outcome,
            Outcome::Failed {
                failure: Failure::Connect | Failure::Timeout(Stage::Connect),
                status: None,
                ..
            }
        ),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_url_we_cannot_crawl_is_an_error_and_not_an_outcome() {
    // A fetch that fails is a result the scheduler acts on. A URL that is not a
    // URL is a caller bug, and reporting it as a failed fetch would put it back
    // in the frontier to fail again forever.
    let fetcher = fetcher(FetchConfig::default());
    for url in ["", "not a url", "ftp://example.com/x", "mailto:a@b.c"] {
        assert!(
            fetcher.fetch(url, None).await.is_err(),
            "{url:?} was accepted"
        );
    }
}

#[test]
fn nothing_in_the_lockfile_pulls_in_native_tls() {
    // Half of gate 2.2. Reading the lockfile rather than this crate's own
    // dependencies is the stricter check and the useful one: two TLS stacks in
    // one binary is the problem, and it does not matter which crate pulled the
    // second one in.
    //
    // BoringSSL is deliberately not in this list any more. `wreq` is an
    // optional dependency and an optional dependency is still locked, so
    // `btls` has been in Cargo.lock since T2 landed and asserting against it
    // here would only mean deleting this test the next time anyone looked.
    // What a build actually compiles is a `cargo tree` question, and
    // `scripts/check-tls.sh` is where it is asked. This half is still worth
    // keeping, because it catches the accident: a new dependency that quietly
    // brings native TLS with it, on a path nobody chose.
    //
    // What is checked is a second implementation, so `schannel`,
    // `security-framework` and `openssl-probe` are all fine to see here. They
    // are how rustls-platform-verifier reads the trust store on Windows, macOS
    // and Linux, they do no cryptography, and only one of the three is ever
    // compiled for a given target.
    let lock = include_str!("../../../Cargo.lock");
    for forbidden in ["openssl-sys", "native-tls", "aws-lc-fips-sys"] {
        assert!(
            !lock.contains(&format!("name = \"{forbidden}\"")),
            "{forbidden} is in the lockfile, so something pulled in native tls"
        );
    }
    assert!(
        lock.contains("name = \"rustls\""),
        "rustls is not in the lockfile, so this test is asserting nothing"
    );
}

// Doc 07.2's Web Bot Auth signatures.
//
// Two kinds of test here. The first kind checks the bytes: a signature base
// written out by hand from RFC 9421's rules, and a key thumbprint taken from
// RFC 8037's own example. Those are the ones that would catch us being self
// consistently wrong, which is the failure mode a round trip cannot see,
// because an origin verifies with somebody else's library and not with ours.
// The second kind checks the behaviour doc 07.2 promises: rotation without
// breaking old requests, and a signature that is worth nothing anywhere but
// on the request it was made for.

/// A key nobody uses, for tests that only need two of them to differ.
const SEED: [u8; 32] = [42; 32];
/// The nonce seed, fixed here so a failure is reproducible.
const NONCE_SEED: [u8; 16] = [7; 16];
/// A fixed moment, so the numbers in the expected base are readable.
const T0: u64 = 1_756_400_000;

fn signer_at(secs: u64) -> Signer {
    Signer::fixed(SEED, "https://umi.dev", NONCE_SEED, secs).expect("the agent is a url")
}

fn parsed(url: &str) -> url::Url {
    url::Url::parse(url).expect("a url")
}

/// The headers out of a request head the scripted origin recorded.
fn headers_of(request: &str) -> http::HeaderMap {
    let mut headers = http::HeaderMap::new();
    for line in request.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if let Ok(name) = name.trim().parse::<http::HeaderName>()
            && let Ok(value) = value.trim().parse::<http::HeaderValue>()
        {
            headers.insert(name, value);
        }
    }
    headers
}

#[test]
fn the_key_id_is_rfc_8037s_own_thumbprint() {
    // RFC 8037 appendix A.3 publishes an Ed25519 JWK and the thumbprint of it.
    // Computing the same string from the same key is the only check here that
    // does not go through our own code twice, and it is the one that would
    // catch a canonical form with a space in it or the members in the wrong
    // order.
    let x = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo";
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(x)
        .expect("the rfc's key is base64url");
    let bytes: [u8; 32] = raw.try_into().expect("32 bytes");
    let key = ed25519_dalek::VerifyingKey::from_bytes(&bytes).expect("on the curve");

    let entry = Jwk::new(&key, None, None);
    assert_eq!(entry.x, x);
    assert_eq!(entry.kid, "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k");
}

#[test]
fn the_signature_base_is_the_bytes_rfc_9421_describes() {
    // Written out by hand from section 2.5 rather than produced and pasted
    // back. One line per component, `"name": value`, newline separated, the
    // parameters last with nothing after them. An origin builds this same
    // string from the request it received, so a stray space or a trailing
    // newline here is a signature that never verifies anywhere.
    let params = Params {
        created: T0,
        expires: T0 + 60,
        keyid: "kid".to_owned(),
        alg: "ed25519".to_owned(),
        nonce: "nonce".to_owned(),
        tag: "web-bot-auth".to_owned(),
    };
    let covered: Vec<String> = COVERED.iter().map(|n| (*n).to_owned()).collect();
    let mut fields = http::HeaderMap::new();
    fields.insert(
        "signature-agent",
        "\"https://umi.dev\"".parse().expect("a value"),
    );

    let base = signature_base(
        "get",
        &parsed("https://example.com/a/b?q=1"),
        &covered,
        &params,
        &fields,
    )
    .expect("the url has a host");

    let expected = concat!(
        "\"@authority\": example.com\n",
        "\"@method\": GET\n",
        "\"@path\": /a/b\n",
        "\"signature-agent\": \"https://umi.dev\"\n",
        "\"@signature-params\": (\"@authority\" \"@method\" \"@path\" \"signature-agent\")",
        ";created=1756400000;expires=1756400060;keyid=\"kid\";alg=\"ed25519\"",
        ";nonce=\"nonce\";tag=\"web-bot-auth\""
    );
    assert_eq!(base, expected);
}

#[test]
fn the_default_port_is_not_in_the_authority() {
    // RFC 9421 says the authority omits a default port, and getting this wrong
    // would produce signatures that verify in tests and fail on the web,
    // because nothing we crawl names its port.
    let params = Params {
        created: T0,
        expires: T0 + 60,
        keyid: "kid".to_owned(),
        alg: "ed25519".to_owned(),
        nonce: "n".to_owned(),
        tag: "web-bot-auth".to_owned(),
    };
    let one = vec!["@authority".to_owned()];
    let fields = http::HeaderMap::new();
    let base = |url| {
        signature_base("GET", &parsed(url), &one, &params, &fields).expect("the url has a host")
    };

    assert!(base("https://example.com:443/").starts_with("\"@authority\": example.com\n"));
    assert!(base("http://example.com:80/").starts_with("\"@authority\": example.com\n"));
    assert!(base("https://example.com:8443/").starts_with("\"@authority\": example.com:8443\n"));
}

#[test]
fn a_signed_request_verifies_against_the_published_directory() {
    let signer = signer_at(T0);
    let url = parsed("https://example.com/page");
    let signed = signer.sign("GET", &url).expect("the url has a host");

    let mut headers = http::HeaderMap::new();
    for (name, value) in signed.headers() {
        headers.insert(name, value.parse().expect("a header value"));
    }

    let directory = Directory {
        keys: vec![signer.jwk(None, None)],
    };
    let verified =
        verify("GET", &url, &headers, &directory, T0 + 1).expect("our own signature verifies");
    assert_eq!(verified.keyid, signer.keyid());
    assert_eq!(verified.agent, "https://umi.dev");
    assert_eq!(verified.created, T0);
}

#[test]
fn a_signature_is_worth_nothing_on_another_request() {
    // The whole point of covering the authority, the method and the path. A
    // signature lifted off one request and pasted onto another has to fail, or
    // an origin that saw one of our fetches could speak as us anywhere.
    let signer = signer_at(T0);
    let signed = signer
        .sign("GET", &parsed("https://example.com/page"))
        .expect("the url has a host");
    let mut headers = http::HeaderMap::new();
    for (name, value) in signed.headers() {
        headers.insert(name, value.parse().expect("a header value"));
    }
    let directory = Directory {
        keys: vec![signer.jwk(None, None)],
    };

    for elsewhere in [
        "https://other.example/page",
        "https://example.com/other",
        "https://example.com:8443/page",
    ] {
        let result = verify("GET", &parsed(elsewhere), &headers, &directory, T0 + 1);
        assert!(
            matches!(result, Err(SignatureError::BadSignature)),
            "{elsewhere} verified"
        );
    }
}

#[test]
fn a_rotated_key_keeps_verifying_the_requests_it_signed() {
    // Doc 07.2 rotates quarterly with an overlap window. The retired key stays
    // in the directory and gains an `exp`, so a request from before the
    // rotation still verifies and a request from after it does not. Deleting
    // the entry instead would make every past request unverifiable, which is
    // the thing an archive of signed fetches exists to prevent.
    let retired_at = T0 + 3600;
    let signer = signer_at(T0);
    let url = parsed("https://example.com/page");
    let directory = Directory {
        keys: vec![signer.jwk(None, Some(retired_at))],
    };

    let before = signer.sign_at("GET", &url, T0).expect("a host");
    let mut headers = http::HeaderMap::new();
    for (name, value) in before.headers() {
        headers.insert(name, value.parse().expect("a header value"));
    }
    assert!(verify("GET", &url, &headers, &directory, T0 + 1).is_ok());

    let after = signer
        .sign_at("GET", &url, retired_at + 10)
        .expect("a host");
    let mut headers = http::HeaderMap::new();
    for (name, value) in after.headers() {
        headers.insert(name, value.parse().expect("a header value"));
    }
    let result = verify("GET", &url, &headers, &directory, retired_at + 11);
    assert!(
        matches!(result, Err(SignatureError::KeyWindow)),
        "{result:?}"
    );
}

#[test]
fn a_signature_outside_its_window_or_key_is_refused() {
    let signer = signer_at(T0);
    let url = parsed("https://example.com/page");
    let signed = signer.sign("GET", &url).expect("a host");
    let mut headers = http::HeaderMap::new();
    for (name, value) in signed.headers() {
        headers.insert(name, value.parse().expect("a header value"));
    }
    let directory = Directory {
        keys: vec![signer.jwk(None, None)],
    };

    let late = verify("GET", &url, &headers, &directory, T0 + LIFETIME_SECS);
    assert!(matches!(late, Err(SignatureError::Expired)), "{late:?}");
    let early = verify("GET", &url, &headers, &directory, T0 - 1);
    assert!(matches!(early, Err(SignatureError::Expired)), "{early:?}");

    let stranger = verify("GET", &url, &headers, &Directory::default(), T0 + 1);
    assert!(
        matches!(stranger, Err(SignatureError::UnknownKey(_))),
        "{stranger:?}"
    );
}

#[test]
fn two_requests_never_carry_the_same_nonce() {
    // The clock does not move in this signer, which is the case that matters:
    // at 250 pages a second most requests share a second with several others,
    // and a nonce that only varied with the clock would be no nonce at all.
    let signer = signer_at(T0);
    let url = parsed("https://example.com/page");
    let mut seen = std::collections::HashSet::new();
    for _ in 0..1000 {
        let signed = signer.sign("GET", &url).expect("a host");
        let nonce = signed
            .input
            .split("nonce=\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .expect("there is a nonce")
            .to_owned();
        assert!(seen.insert(nonce), "a nonce came round twice");
    }
}

#[test]
fn the_directory_round_trips_and_names_the_well_known_path() {
    let signer = signer_at(T0);
    let directory = Directory {
        keys: vec![signer.jwk(Some(T0), None)],
    };
    let json = directory.to_json().expect("it serialises");
    assert!(json.ends_with('\n'), "the file needs a trailing newline");
    assert_eq!(Directory::parse(&json).expect("it parses"), directory);
    assert_eq!(
        signer.directory_url(),
        "https://umi.dev/.well-known/http-message-signatures-directory"
    );
    assert_eq!(
        directory.find(signer.keyid()).expect("the key is there").x,
        signer.jwk(None, None).x
    );
}

#[tokio::test]
async fn the_three_headers_reach_the_origin_and_verify_there() {
    // End to end through a real socket, because the thing that would break
    // this in production is a header the client rewrites or drops rather than
    // anything in the signing code.
    let origin =
        Origin::start(|_| Reply::response(200, &[("content-type", "text/html")], b"ok")).await;
    let signer = Arc::new(signer_at(T0));
    let fetcher = Fetcher::with_signer(FetchConfig::default(), Some(Arc::clone(&signer)))
        .expect("the client builds");

    let url = origin.url("/signed");
    let outcome = fetcher.fetch(&url, None).await.expect("a url we can crawl");
    assert!(matches!(outcome, Outcome::Ok(_)), "{outcome:?}");

    let request = origin.requests().pop().expect("the origin saw it");
    let headers = headers_of(&request);
    let directory = Directory {
        keys: vec![signer.jwk(None, None)],
    };
    let verified =
        verify("GET", &parsed(&url), &headers, &directory, T0 + 1).expect("what arrived verifies");
    assert_eq!(verified.keyid, signer.keyid());
}

#[tokio::test]
async fn a_fetcher_with_no_key_sends_none_of_the_three_headers() {
    // A volunteer's build has no crawl identity key until doc 06 gives them a
    // fetcher key, and an unsigned request is the honest thing to send. Half a
    // signature, or a signature under a key nobody published, would be worse
    // than none.
    let origin =
        Origin::start(|_| Reply::response(200, &[("content-type", "text/html")], b"ok")).await;
    let fetcher = fetcher(FetchConfig::default());
    fetcher
        .fetch(&origin.url("/plain"), None)
        .await
        .expect("a url we can crawl");

    let request = origin
        .requests()
        .pop()
        .expect("the origin saw it")
        .to_lowercase();
    for name in ["signature-agent", "signature-input", "signature:"] {
        assert!(!request.contains(name), "{name} was sent without a key");
    }
}
