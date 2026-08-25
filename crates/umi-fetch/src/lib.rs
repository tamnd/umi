//! Tier 1, the plain HTTP client.
//!
//! Specified in `docs/spec/05-fetch-tiers.md` section 5.4. Hyper over rustls,
//! HTTP/2 preferred with an HTTP/1.1 fallback, an honest and fixed identity, a
//! connection cap per host, a timeout at every stage and a hard cap on the
//! body. It does not try to look like a browser and it should not: sending
//! Chrome's header set from a rustls stack produces a mismatch between the TLS
//! fingerprint and the HTTP layer that is more suspicious than being honestly
//! a bot, and it is exactly what JA4 plus JA4H correlation is built to catch.
//!
//! This is deliberately the boring rung and deliberately the first one. Doc
//! 05.2 assumes a plain client answers about 90 percent of everything that is
//! not a revalidate, and building the ladder before measuring whether the
//! bottom rung is enough would be optimising against a guess.
//!
//! # What is not here
//!
//! T0 revalidation as a policy is milestone 2, though the mechanism is here:
//! give [`Fetcher::fetch`] a [`Revalidator`] and it sends the conditional
//! headers and reports a 304 as [`Outcome::NotModified`]. T2, T3 and T4 are
//! milestone 2 as well. So is Web Bot Auth request signing from doc 07.2 and
//! the escalation state machine from doc 05.8, and this crate deliberately
//! makes no scheduling decision at all: it reports what happened and doc 09
//! decides what that means.
//!
//! robots.txt is not consulted here either, and that is doc 04.7's rule rather
//! than an omission. The robots decision belongs to the coordinator, a
//! disallowed URL is never leased, and a community fetcher therefore cannot
//! make the crawl impolite through a parsing bug.
//!
//! # No clock
//!
//! Nothing here reads a wall clock. Elapsed time comes from [`Instant`], which
//! is monotonic, and anything that needs to be stamped with a date is stamped
//! by the caller. That is gate 1.2's rule and it is what lets a fetch be
//! replayed.
//!
//! # rustls only
//!
//! There is no `openssl-sys` anywhere in the tree, and the test named
//! `the_tree_is_rustls_only` asserts that against the lockfile. It matters now
//! because a static binary that dynamically links OpenSSL is not a static
//! binary, and it will matter more in milestone 2 when `wreq` arrives with
//! BoringSSL and the two would otherwise be in the same process.
//!
//! Roots come from the platform store through rustls-platform-verifier, which
//! is reqwest's own default. A volunteer running a fetcher behind a corporate
//! root should not have to configure anything, and pinning a root set here
//! would mean shipping a way to unpin it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use http::HeaderMap;
use tokio::sync::Semaphore;
use url::Url;

pub mod date;
pub mod headers;
pub mod outcome;
pub mod sniff;

pub use outcome::{Failure, Hop, Outcome, OutcomeCode, Page, RetryAfter, Stage, Version};
pub use sniff::Media;
pub use umi_types::Revalidator;

/// The user agent from `docs/spec/07-politeness-and-identity.md` section 7.1.
///
/// One string, for every tier, forever. The URL in it resolves to a page that
/// says who runs the crawler, what the data is for, where the corpus is, and
/// how to block us in one line. A site operator reading their logs should be
/// able to identify us in one search, and that only works if the string never
/// varies.
pub const USER_AGENT: &str = "umi/1.0 (+https://umi.dev/bot)";

/// What we tell origins we will take.
///
/// Deliberately not a browser's `Accept`. We want markup, we will read
/// anything, and we do not pretend to prefer image formats we cannot decode.
pub const ACCEPT: &str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

/// The knobs, with doc 05.4's numbers as the defaults.
#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub struct FetchConfig {
    /// How long to wait for a connection. Doc 05.4 says 10 seconds.
    pub connect_timeout: Duration,
    /// How long a connection may go quiet mid body before we give up. Not in
    /// doc 05.4, and the reason it is here is that a total timeout alone lets
    /// an origin trickle one byte every 29 seconds and hold a connection for
    /// the full budget. A slow origin is a real thing and a stalled one is a
    /// different real thing, and the fleet cannot afford to treat them alike.
    pub read_timeout: Duration,
    /// The whole fetch, connect to last byte. Doc 05.4 says 30 seconds.
    pub total_timeout: Duration,
    /// The body cap. Doc 05.4 says 512 KiB, which holds well over 99 percent
    /// of HTML and cuts off the video files that get served with an HTML
    /// content type.
    pub body_cap: usize,
    /// Same domain redirects to follow. Doc 09's loop rule says 5.
    pub max_redirects: usize,
    /// Concurrent requests per host. Doc 05.4 caps connections at 2, and with
    /// HTTP/2 multiplexing this is the stricter of the two readings.
    pub per_host: usize,
    /// How many hosts to keep permit sets for before pruning the idle ones. A
    /// fleet at rate touches millions of hosts and the map would otherwise be
    /// a slow leak.
    pub host_table_cap: usize,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(10),
            total_timeout: Duration::from_secs(30),
            body_cap: 512 * 1024,
            max_redirects: 5,
            per_host: 2,
            host_table_cap: 4096,
        }
    }
}

/// Something went wrong before a request could be made.
///
/// Everything that goes wrong after that point is an [`Outcome`], because a
/// fetch that fails is a result and not an error. This is only for the two
/// cases where there was nothing to fetch: a URL that does not parse, and a
/// client that will not build.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FetchError {
    /// The URL did not parse, or was not http(s).
    #[error("not a crawlable url: {0}")]
    Url(String),
    /// The HTTP client could not be built, which in practice means the
    /// platform has no usable certificate store.
    #[error("could not build the http client: {0}")]
    Client(String),
}

type Result<T> = std::result::Result<T, FetchError>;

/// The T1 client.
///
/// Cheap to clone: the connection pool and the per host permits are shared, so
/// every task in a worker pool should hold a clone of one `Fetcher` rather
/// than build its own. Two `Fetcher`s do not share a pool and would each get
/// the full per host allowance, which is how a crawler accidentally opens 200
/// connections to one site.
#[derive(Clone, Debug)]
pub struct Fetcher {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    client: reqwest::Client,
    config: FetchConfig,
    hosts: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl Fetcher {
    /// A client with doc 05.4's defaults.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when the TLS backend will not initialise.
    pub fn new() -> Result<Self> {
        Self::with_config(FetchConfig::default())
    }

    /// A client with the knobs turned.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when the TLS backend will not initialise.
    pub fn with_config(config: FetchConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .connect_timeout(config.connect_timeout)
            // Redirects are followed by hand, in `fetch`, because doc 04.7
            // stops at the first one that leaves the registrable domain and a
            // policy closure cannot report which URL it stopped at.
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(config.per_host)
            .https_only(false)
            .build()
            .map_err(|e| FetchError::Client(e.to_string()))?;

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                config,
                hosts: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// The configuration this client was built with.
    #[must_use]
    pub fn config(&self) -> &FetchConfig {
        &self.inner.config
    }

    /// Fetch one URL.
    ///
    /// Pass a [`Revalidator`] to make it conditional, which is doc 05.3's T0.
    /// Passing `None` is T1. Everything that can go wrong on the wire comes
    /// back as an [`Outcome`] rather than an error, because a 404 and a
    /// timeout are results the scheduler acts on and not exceptions.
    ///
    /// # Errors
    ///
    /// [`FetchError::Url`] when the URL does not parse or is not http(s).
    /// Nothing else.
    pub async fn fetch(&self, url: &str, revalidate: Option<&Revalidator>) -> Result<Outcome> {
        let parsed = Url::parse(url).map_err(|e| FetchError::Url(format!("{url}: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(FetchError::Url(format!(
                "{url}: scheme is not http or https"
            )));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| FetchError::Url(format!("{url}: no host")))?
            .to_owned();

        let permits = self.permits(&host);
        // The semaphore is never closed, so the only way to fail here is a
        // poisoned lock, which would already have panicked.
        let _permit = permits.acquire().await.expect("the semaphore is open");

        let started = Instant::now();
        let deadline = self.inner.config.total_timeout;
        match tokio::time::timeout(deadline, self.walk(parsed, revalidate, started)).await {
            Ok(outcome) => Ok(outcome),
            Err(_) => Ok(Outcome::Failed {
                status: None,
                failure: Failure::Timeout(Stage::Total),
                retry_after: None,
            }),
        }
    }

    /// One request, then any same domain redirects, then the body.
    async fn walk(
        &self,
        mut url: Url,
        revalidate: Option<&Revalidator>,
        started: Instant,
    ) -> Outcome {
        let origin_domain = registrable_domain(&url);
        let mut redirects = Vec::new();

        loop {
            let response = match self.send(&url, revalidate).await {
                Ok(response) => response,
                Err(failure) => {
                    return Outcome::Failed {
                        status: None,
                        failure,
                        retry_after: None,
                    };
                }
            };

            let status = response.status().as_u16();
            let version = Version::from(response.version());
            let head = response.headers().clone();
            // Read once, at the top, so that every way out of this loop that
            // has a response behind it carries what the origin asked for. A
            // `Retry-After` on a 429 is the whole message and the 429 has no
            // body to put it in.
            let retry_after = headers::retry_after(&head);

            // 304 is a 3xx and is not a redirect, so it has to be answered
            // before the `Location` handling below ever looks at it.
            if status == 304 {
                return Outcome::NotModified {
                    revalidate: revalidator(&head),
                    headers_kept: headers::kept(&head),
                    headers_digest: headers::digest(&head),
                    elapsed: started.elapsed(),
                };
            }

            if let Some(target) = redirect_target(&url, &head, status) {
                if redirects.len() >= self.inner.config.max_redirects {
                    return Outcome::Failed {
                        status: Some(status),
                        failure: Failure::Malformed,
                        retry_after,
                    };
                }
                if registrable_domain(&target) != origin_domain {
                    return Outcome::RedirectedOffDomain {
                        redirects,
                        target: target.to_string(),
                        status,
                    };
                }
                redirects.push(Hop {
                    from: url.to_string(),
                    to: target.to_string(),
                    status,
                });
                url = target;
                continue;
            }

            if let Some(failure) = classify(status, &head) {
                return match failure {
                    None => Outcome::Gone,
                    Some(failure) => Outcome::Failed {
                        status: Some(status),
                        failure,
                        retry_after,
                    },
                };
            }

            // Believing `Content-Length` is how a crawler gets talked into
            // buffering a gigabyte, so it is only ever used to decline early.
            // The real cap is enforced against the bytes that arrive.
            if declared_length(&head).is_some_and(|len| len > self.inner.config.body_cap as u64) {
                return Outcome::Failed {
                    status: Some(status),
                    failure: Failure::TooLarge,
                    retry_after,
                };
            }

            let content_type = header(&head, "content-type");
            let body = match self.read_body(response).await {
                Ok(body) => body,
                Err(failure) => {
                    return Outcome::Failed {
                        status: Some(status),
                        failure,
                        retry_after,
                    };
                }
            };

            let head_bytes = &body[..body.len().min(sniff::SNIFF_BYTES)];
            return Outcome::Ok(Box::new(Page {
                final_url: url.to_string(),
                status,
                version,
                redirects,
                headers_kept: headers::kept(&head),
                headers_digest: headers::digest(&head),
                media: sniff::decide(content_type.as_deref(), head_bytes),
                content_type,
                body_digest: *blake3::hash(&body).as_bytes(),
                body,
                revalidate: revalidator(&head),
                elapsed: started.elapsed(),
            }));
        }
    }

    async fn send(
        &self,
        url: &Url,
        revalidate: Option<&Revalidator>,
    ) -> std::result::Result<reqwest::Response, Failure> {
        let mut request = self
            .inner
            .client
            .get(url.clone())
            .header(http::header::ACCEPT, ACCEPT);

        // Doc 05.3 sends both when both are known, because origins are
        // inconsistent about which one they honour and sending the pair costs
        // about sixty bytes.
        if let Some(revalidate) = revalidate {
            if let Some(etag) = &revalidate.etag {
                request = request.header(http::header::IF_NONE_MATCH, etag);
            }
            if let Some(ms) = revalidate.last_modified_ms {
                request = request.header(http::header::IF_MODIFIED_SINCE, date::format(ms));
            }
        }

        request.send().await.map_err(transport_failure)
    }

    /// Read the body with the cap and the idle timeout both live.
    async fn read_body(&self, response: reqwest::Response) -> std::result::Result<Bytes, Failure> {
        let cap = self.inner.config.body_cap;
        let idle = self.inner.config.read_timeout;
        let mut stream = response.bytes_stream();
        let mut body = BytesMut::new();

        loop {
            let next = match tokio::time::timeout(idle, stream.next()).await {
                Ok(next) => next,
                Err(_) => return Err(Failure::Timeout(Stage::Read)),
            };
            let Some(chunk) = next else { break };
            let chunk = chunk.map_err(transport_failure)?;

            if body.len() + chunk.len() > cap {
                // Dropping the stream here closes the connection, which is the
                // point: the alternative is draining a body we have already
                // decided to throw away.
                return Err(Failure::TooLarge);
            }
            body.extend_from_slice(&chunk);
        }

        Ok(body.freeze())
    }

    /// The permit set for a host, creating it if this is the first request.
    fn permits(&self, host: &str) -> Arc<Semaphore> {
        let mut hosts = self.inner.hosts.lock().unwrap_or_else(|e| e.into_inner());

        if hosts.len() >= self.inner.config.host_table_cap {
            // An entry nobody else holds a reference to has no requests in
            // flight and no permits taken, so dropping it loses nothing. This
            // is a sweep rather than an eviction policy because the table is
            // only a leak and never a cache.
            hosts.retain(|_, permits| Arc::strong_count(permits) > 1);
        }

        Arc::clone(
            hosts
                .entry(host.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(self.inner.config.per_host))),
        )
    }
}

/// Where a redirect points, resolved against the URL that served it.
///
/// A `Location` that does not parse is not a redirect, and treating it as one
/// would turn a broken origin into a fetch of the wrong page.
fn redirect_target(url: &Url, head: &HeaderMap, status: u16) -> Option<Url> {
    if !matches!(status, 301 | 302 | 303 | 307 | 308) {
        return None;
    }
    let location = header(head, "location")?;
    url.join(&location)
        .ok()
        .filter(|target| matches!(target.scheme(), "http" | "https"))
}

/// What a status means, once it is not a redirect and not a 304.
///
/// `None` means the body is worth reading. `Some(None)` is the one status that
/// means never again. `Some(Some(failure))` is everything else.
fn classify(status: u16, head: &HeaderMap) -> Option<Option<Failure>> {
    if (200..300).contains(&status) {
        return None;
    }
    let marked = block_marker(head).is_some();
    let failure = match status {
        410 => return Some(None),
        403 | 503 if marked => Failure::Blocked,
        429 if marked => Failure::Blocked,
        429 => Failure::RateLimited,
        400..500 => Failure::NotFound,
        500..600 => Failure::ServerError,
        // 1xx never reaches here and a status above 599 is not HTTP.
        _ => Failure::Malformed,
    };
    Some(Some(failure))
}

/// The bot management vendors doc 05.8 names, recognised by the header they
/// cannot help sending.
///
/// Only the header side of the signal lives here. The body side, an
/// interstitial page or a client rendered shell, needs the extracted text and
/// is milestone 2. Presence is what is checked and not the value, because the
/// values are opaque and change.
fn block_marker(head: &HeaderMap) -> Option<&'static str> {
    const MARKERS: [&str; 5] = [
        "cf-mitigated",
        "x-datadome",
        "x-sucuri-block",
        "x-iinfo",
        "x-cdn-request-id",
    ];
    if let Some(name) = MARKERS.into_iter().find(|name| head.contains_key(*name)) {
        return Some(name);
    }
    // Akamai's challenge is served by a named server rather than a header of
    // its own, which is why this one is a value check.
    header(head, "server")
        .is_some_and(|server| server.starts_with("AkamaiGHost"))
        .then_some("server")
}

/// `Content-Length`, when the origin sent one that parses.
fn declared_length(head: &HeaderMap) -> Option<u64> {
    header(head, "content-length")?.trim().parse().ok()
}

/// The conditional headers this response earns us next time.
fn revalidator(head: &HeaderMap) -> Revalidator {
    Revalidator {
        etag: header(head, "etag"),
        last_modified_ms: header(head, "last-modified")
            .as_deref()
            .and_then(date::parse),
    }
}

/// One header as a string, dropping values that are not text.
fn header(head: &HeaderMap, name: &str) -> Option<String> {
    head.get(name)?.to_str().ok().map(str::to_owned)
}

/// The registrable domain a URL belongs to, for doc 04.7's redirect rule.
///
/// An IP literal is its own domain, which is what falling back to the host
/// string does. That is the right answer: a redirect from one address to
/// another is off domain by any reading.
fn registrable_domain(url: &Url) -> String {
    url.host_str()
        .map(|host| umi_types::pay_level_domain(host).to_string())
        .unwrap_or_default()
}

/// Turn a `reqwest` error into the failure class the scheduler acts on.
///
/// DNS, connect and TLS failures are one error type with one message, so the
/// only way to tell them apart is the source chain, and the only thing in the
/// source chain is text. That is fragile and it is written down rather than
/// hidden: if a `reqwest` or `hyper` release changes the wording, these fall
/// back to [`Failure::Connect`], which is the safe reading because it retries
/// the URL rather than escalating a tier.
fn transport_failure(error: reqwest::Error) -> Failure {
    if error.is_timeout() {
        return Failure::Timeout(if error.is_connect() {
            Stage::Connect
        } else {
            Stage::Read
        });
    }
    if error.is_body() || error.is_decode() {
        return Failure::Malformed;
    }

    let mut text = String::new();
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        text.push_str(&current.to_string().to_ascii_lowercase());
        text.push(' ');
        source = current.source();
    }

    if text.contains("dns error") || text.contains("failed to lookup address") {
        Failure::Dns
    } else if text.contains("certificate")
        || text.contains("handshake")
        || text.contains("tls")
        || text.contains("alert")
    {
        Failure::Tls
    } else {
        Failure::Connect
    }
}

#[cfg(test)]
mod tests;
