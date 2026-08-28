//! One response pipeline, whichever client put the request on the wire.
//!
//! T1 and T2 differ in exactly one thing: the socket. What comes back is HTTP
//! either way, and every rule about what to do with it is a rule out of the
//! spec rather than a property of the client. The body cap, doc 04.7's stop at
//! the first redirect that leaves the registrable domain, answering a 304
//! before the `Location` handling ever sees it, doc 05.8's four way split
//! between a dead URL and a wall, and reading the body of a 403 to find out
//! which one it is are the same rules at both tiers.
//!
//! Two copies of them would drift, and the drift would be invisible. A crawl
//! would keep running and the tier a page happened to come back on would
//! quietly decide whether the crawler followed its own redirect rule. So there
//! is one engine, generic over the client, and [`Transport`] is the seam.
//!
//! It is a private trait on purpose. Doc 04.5 already has a public seam for
//! somebody else's fetcher and it is a network protocol, not a Rust trait.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use http::HeaderMap;
use tokio::sync::Semaphore;
use url::Url;

use crate::outcome::{Failure, Hop, Outcome, Page, Stage, Version};
use crate::{FetchConfig, FetchError, Result, Revalidator, challenge, date, headers, sniff};

/// Everything about a response that is settled before the body arrives.
///
/// A struct of its own rather than the client's response type, because the
/// pipeline reads these three things and nothing else. Saying so here is what
/// stops a second client from smuggling its own behaviour in.
pub(crate) struct Head {
    /// The status as a number, because 304 and 410 are the interesting ones
    /// and neither is well served by an enum with sixty variants.
    pub(crate) status: u16,
    /// Which HTTP version answered, for doc 10.5's row.
    pub(crate) version: Version,
    /// Every response header, cloned out, because the pipeline still reads
    /// them after the body has been consumed.
    pub(crate) headers: HeaderMap,
}

/// A response body, already classified into the failures the scheduler acts on.
///
/// Boxed, which costs one allocation per fetch. That is nothing next to a
/// network round trip, and the alternative is an associated type no client can
/// actually name: both `bytes_stream` methods return an opaque `impl Stream`.
pub(crate) type BodyStream =
    Pin<Box<dyn Stream<Item = std::result::Result<Bytes, Failure>> + Send>>;

/// A client that can put a GET on the wire.
///
/// Header policy is deliberately not here. T1 sends our own `Accept` and
/// nothing else, and T2 sends a browser profile's whole header set because doc
/// 05.5 says the profile is used as a unit. Only the conditional headers are
/// shared, through [`conditional`], because those carry meaning rather than a
/// fingerprint.
pub(crate) trait Transport: Send + Sync {
    /// The client's response, before the body is read.
    type Response: Send;

    /// Send the GET, with the conditional headers when there are any.
    fn send(
        &self,
        url: &Url,
        revalidate: Option<&Revalidator>,
    ) -> impl Future<Output = std::result::Result<Self::Response, Failure>> + Send;

    /// The status, version and headers, read before the body.
    fn head(response: &Self::Response) -> Head;

    /// The body, as a stream the engine can cap and time out.
    fn body(response: Self::Response) -> BodyStream;
}

/// The fetch loop, over one client.
///
/// Holds the per host permits as well as the client, because doc 05.4's cap is
/// per host and per client. Two engines sharing a host would each hand out the
/// full allowance, which is how a crawler accidentally opens two hundred
/// connections to one site.
pub(crate) struct Engine<T> {
    transport: T,
    config: FetchConfig,
    hosts: Mutex<HashMap<String, Arc<Semaphore>>>,
}

// By hand rather than derived, because a derive would put a `T: Debug` bound
// on the whole type and one of the two clients does not implement it.
impl<T> std::fmt::Debug for Engine<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<T: Transport> Engine<T> {
    /// An engine over a client that is already built.
    pub(crate) fn new(transport: T, config: FetchConfig) -> Self {
        Self {
            transport,
            config,
            hosts: Mutex::new(HashMap::new()),
        }
    }

    /// The configuration this engine was built with.
    pub(crate) const fn config(&self) -> &FetchConfig {
        &self.config
    }

    /// Fetch one URL.
    ///
    /// # Errors
    ///
    /// [`FetchError::Url`] when the URL does not parse or is not http(s), and
    /// nothing else. Everything that goes wrong on the wire is an [`Outcome`].
    pub(crate) async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
    ) -> Result<Outcome> {
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
        let deadline = self.config.total_timeout;
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
            let response = match self.transport.send(&url, revalidate).await {
                Ok(response) => response,
                Err(failure) => {
                    return Outcome::Failed {
                        status: None,
                        failure,
                        retry_after: None,
                    };
                }
            };

            let Head {
                status,
                version,
                headers: head,
            } = T::head(&response);
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
                if redirects.len() >= self.config.max_redirects {
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

            let verdict = classify(status, &head);
            match verdict {
                Verdict::Page | Verdict::Suspect(_) => {}
                Verdict::Gone => return Outcome::Gone,
                Verdict::Failed(failure) => {
                    return Outcome::Failed {
                        status: Some(status),
                        failure,
                        retry_after,
                    };
                }
            }

            // Believing `Content-Length` is how a crawler gets talked into
            // buffering a gigabyte, so it is only ever used to decline early.
            // The real cap is enforced against the bytes that arrive.
            if declared_length(&head).is_some_and(|len| len > self.config.body_cap as u64) {
                return Outcome::Failed {
                    status: Some(status),
                    failure: Failure::TooLarge,
                    retry_after,
                };
            }

            let content_type = header(&head, "content-type");
            let body = match self.read_body(T::body(response)).await {
                Ok(body) => body,
                Err(failure) => {
                    return Outcome::Failed {
                        status: Some(status),
                        failure,
                        retry_after,
                    };
                }
            };

            // The suspect statuses come back here with their body in hand.
            // An interstitial makes it a block, which doc 05.8 answers with a
            // tier, and anything else is the plain failure the status already
            // said it was.
            if let Verdict::Suspect(fallback) = verdict {
                let failure = if challenge::interstitial(&body).is_some() {
                    Failure::Blocked
                } else {
                    fallback
                };
                return Outcome::Failed {
                    status: Some(status),
                    failure,
                    retry_after,
                };
            }

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

    /// Read the body with the cap and the idle timeout both live.
    async fn read_body(&self, mut stream: BodyStream) -> std::result::Result<Bytes, Failure> {
        let cap = self.config.body_cap;
        let idle = self.config.read_timeout;
        let mut body = BytesMut::new();

        loop {
            let next = match tokio::time::timeout(idle, stream.next()).await {
                Ok(next) => next,
                Err(_) => return Err(Failure::Timeout(Stage::Read)),
            };
            let Some(chunk) = next else { break };
            let chunk = chunk?;

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

    /// How many hosts the permit table is holding. For the sweep test.
    #[cfg(test)]
    pub(crate) fn live_hosts(&self) -> usize {
        self.hosts.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// The permit set for a host, creating it if this is the first request.
    pub(crate) fn permits(&self, host: &str) -> Arc<Semaphore> {
        let mut hosts = self.hosts.lock().unwrap_or_else(|e| e.into_inner());

        if hosts.len() >= self.config.host_table_cap {
            // An entry nobody else holds a reference to has no requests in
            // flight and no permits taken, so dropping it loses nothing. This
            // is a sweep rather than an eviction policy because the table is
            // only a leak and never a cache.
            hosts.retain(|_, permits| Arc::strong_count(permits) > 1);
        }

        Arc::clone(
            hosts
                .entry(host.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(self.config.per_host))),
        )
    }
}

/// Doc 05.3's conditional headers, as name and value pairs.
///
/// Both when both are known, because origins are inconsistent about which one
/// they honour and sending the pair costs about sixty bytes. Returned rather
/// than applied, because each client has its own request builder and this is
/// the part that is the same.
pub(crate) fn conditional(revalidate: Option<&Revalidator>) -> Vec<(http::HeaderName, String)> {
    let mut out = Vec::new();
    let Some(revalidate) = revalidate else {
        return out;
    };
    if let Some(etag) = &revalidate.etag {
        out.push((http::header::IF_NONE_MATCH, etag.clone()));
    }
    if let Some(ms) = revalidate.last_modified_ms {
        out.push((http::header::IF_MODIFIED_SINCE, date::format(ms)));
    }
    out
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

/// What one status means, before the body has been read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Verdict {
    /// A success. Read the body, it is the page.
    Page,
    /// A 410, which is the one status that means never again.
    Gone,
    /// Settled on the headers alone.
    Failed(Failure),
    /// A refusal that might be a bot manager. Read the body to find out, and
    /// fall back to this failure if it is an ordinary one.
    Suspect(Failure),
}

/// What a status means, once it is not a redirect and not a 304.
pub(crate) fn classify(status: u16, head: &HeaderMap) -> Verdict {
    if (200..300).contains(&status) {
        return Verdict::Page;
    }
    let marked = block_marker(head).is_some();
    match status {
        410 => Verdict::Gone,
        403 | 429 | 503 if marked => Verdict::Failed(Failure::Blocked),
        // Doc 05.8's four way split. A 403 from an origin that does not want
        // us is a dead url and a 403 from something in front of it is a tier
        // problem, and the only thing that tells them apart is the page. The
        // body of a refusal is small and refusals are rare, so this is a read
        // we can afford on the three statuses that carry a wall and on no
        // others.
        403 => Verdict::Suspect(Failure::NotFound),
        429 => Verdict::Suspect(Failure::RateLimited),
        503 => Verdict::Suspect(Failure::ServerError),
        400..500 => Verdict::Failed(Failure::NotFound),
        500..600 => Verdict::Failed(Failure::ServerError),
        // 1xx never reaches here and a status above 599 is not HTTP.
        _ => Verdict::Failed(Failure::Malformed),
    }
}

/// The bot management vendors doc 05.8 names, recognised by the header they
/// cannot help sending.
///
/// Only the header side of the signal lives here. The body side, an
/// interstitial page or a client rendered shell, is [`challenge`]'s job.
/// Presence is what is checked and not the value, because the values are
/// opaque and change.
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
pub(crate) fn revalidator(head: &HeaderMap) -> Revalidator {
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

/// Classify a transport error by walking its source chain for the words that
/// name the stage.
///
/// Shared by both clients because both are hyper underneath, both fold DNS,
/// connect and TLS into one error type with one message, and the only thing
/// left to tell them apart is the text. That is fragile and it is written down
/// rather than hidden: if a release changes the wording these fall back to
/// [`Failure::Connect`], which is the safe reading because it retries the URL
/// rather than escalating a tier.
pub(crate) fn failure_from_text(error: &(dyn std::error::Error + 'static)) -> Failure {
    let mut text = String::new();
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
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
