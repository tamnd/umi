//! What one fetch turned into.
//!
//! This is the client's answer, not the wire form. Doc 04.5 fixes a string
//! enum that goes in a receipt, and [`Outcome::wire`] maps onto it, but the
//! two are not the same type on purpose: the receipt is a protocol object with
//! a signature over it and it belongs to `umi-proto`, while this is what a
//! caller in the same process gets back and it carries the body.
//!
//! Nothing here decides anything. Whether a block backs off the host, whether
//! a failure escalates a tier, and when the URL comes back are all doc 05.8
//! and doc 09 questions, and they are answered above this crate with the
//! host record in hand.

use std::time::Duration;

use bytes::Bytes;
pub use umi_types::OutcomeCode;
use umi_types::Revalidator;

use crate::sniff::Media;

/// One step of a redirect chain that was followed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Hop {
    /// Where the redirect was served from.
    pub from: String,
    /// Where it pointed.
    pub to: String,
    /// The status that carried it.
    pub status: u16,
}

/// The HTTP version a response arrived over.
///
/// Recorded because doc 05.10 publishes it: which fraction of the web answers
/// HTTP/2 to a plain client, over time, is a series nobody currently has.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Version {
    /// HTTP/1.0, still served by a long tail of appliances.
    Http10,
    /// HTTP/1.1.
    #[default]
    Http11,
    /// HTTP/2, which T1 prefers and gets on most of the web.
    Http2,
    /// HTTP/3. T1 does not negotiate it, so this only appears if a future
    /// client does.
    Http3,
}

impl Version {
    /// The form doc 04.5's receipt carries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http10 => "1.0",
            Self::Http11 => "1.1",
            Self::Http2 => "2",
            Self::Http3 => "3",
        }
    }
}

impl From<http::Version> for Version {
    fn from(version: http::Version) -> Self {
        match version {
            http::Version::HTTP_10 | http::Version::HTTP_09 => Self::Http10,
            http::Version::HTTP_2 => Self::Http2,
            http::Version::HTTP_3 => Self::Http3,
            _ => Self::Http11,
        }
    }
}

/// A body that arrived, with everything a receipt and an extractor need.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Page {
    /// The URL after any same domain redirects. Equal to the requested URL
    /// when there were none.
    pub final_url: String,
    /// The status, which is a 2xx to be here.
    pub status: u16,
    /// What the response arrived over.
    pub version: Version,
    /// The redirects that were followed to get here, in order.
    pub redirects: Vec<Hop>,
    /// The published subset from doc 11.5.
    pub headers_kept: Vec<(String, String)>,
    /// Blake3 over every header, including the ones not published.
    pub headers_digest: [u8; 32],
    /// The declared type, kept verbatim including its parameters, because the
    /// charset in it is an input to extraction.
    pub content_type: Option<String>,
    /// What the body actually is, per [`crate::sniff`].
    pub media: Media,
    /// The body, decompressed, capped at the configured size.
    pub body: Bytes,
    /// Blake3 over the body exactly as it is here.
    pub body_digest: [u8; 32],
    /// What to send next time, from `ETag` and `Last-Modified`.
    pub revalidate: Revalidator,
    /// Wall time from the start of the request to the last byte of the body.
    pub elapsed: Duration,
}

/// What a `Retry-After` header said, in the form the origin used.
///
/// Kept in the origin's form rather than resolved to a wait, because the date
/// form needs to know what time it is and nothing in this crate reads a clock.
/// That is the same rule doc 11.1 puts on the row builder and it holds here for
/// the same reason: a fetcher that reads the wall clock cannot be replayed, and
/// two fetchers of the same response would disagree about a header that the
/// origin was perfectly precise about. [`RetryAfter::ms_from`] is where the
/// caller, which does know the time, turns it into a number of milliseconds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RetryAfter {
    /// The delta-seconds form, which is what almost every origin sends.
    After(u32),
    /// The HTTP-date form, as milliseconds since the Unix epoch.
    At(u64),
}

impl RetryAfter {
    /// Doc 07.6 honours `Retry-After` up to a day and no further.
    ///
    /// A longer one is not a lie, it is a site telling us to come back next
    /// week, and the frontier already has a way to express that: the url falls
    /// due later. Parking the whole host for a fortnight on the strength of one
    /// header is a much bigger decision than the header is making.
    pub const MAX_MS: u32 = 24 * 60 * 60 * 1000;

    /// How long to wait, for a caller that knows what time it is.
    ///
    /// A date in the past is zero rather than an error. Clocks disagree by a
    /// few seconds all the time and an origin that says "retry at 12:00:03"
    /// when we think it is 12:00:05 is asking for nothing, not for a negative
    /// wait.
    #[must_use]
    pub fn ms_from(self, now_ms: u64) -> u32 {
        let ms = match self {
            Self::After(seconds) => u64::from(seconds).saturating_mul(1000),
            Self::At(at_ms) => at_ms.saturating_sub(now_ms),
        };
        u32::try_from(ms).unwrap_or(u32::MAX).min(Self::MAX_MS)
    }
}

/// Why a fetch produced no body.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum Failure {
    /// DNS did not resolve.
    Dns,
    /// The connection did not open.
    Connect,
    /// The TLS handshake failed. Kept apart from [`Failure::Connect`] because
    /// doc 05.8 reads a handshake failure that succeeds under a browser
    /// profile as a block signal rather than as an outage.
    Tls,
    /// A stage timed out. Which stage is in [`Stage`].
    Timeout(Stage),
    /// A 5xx that carried no challenge marker.
    ServerError,
    /// A 4xx other than 404 and 410, or a 404.
    NotFound,
    /// A 403 or 503 that carried a bot management marker. Doc 05.8 backs the
    /// host off and escalates the tier rather than retrying the URL.
    Blocked,
    /// A 429 with no bot management marker on it, which is an origin asking us
    /// politely to slow down. Kept apart from [`Failure::Blocked`] because the
    /// answer is doc 07.6's rate limiter and not doc 05.8's ladder: escalating
    /// to a browser because a site published a rate limit would spend the
    /// scarcest resource in the fleet on the one case that does not need it.
    RateLimited,
    /// The body went past the cap.
    TooLarge,
    /// The response did not parse as HTTP, or the redirect chain did not
    /// terminate inside the hop limit.
    Malformed,
}

/// Which timeout ran out, because the three mean different things.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Stage {
    /// No connection inside the connect timeout. Usually the origin, sometimes
    /// a firewall dropping SYNs, and never our fault.
    Connect,
    /// A connection that opened and then went quiet for longer than the read
    /// timeout. This is the one that catches a trickling origin, which a total
    /// timeout alone would let hold a connection for its full budget.
    Read,
    /// The whole fetch went past its total budget while still making progress.
    /// A very large body over a very slow link lands here.
    Total,
}

/// What one fetch turned into.
#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum Outcome {
    /// A body arrived.
    Ok(Box<Page>),
    /// A conditional request held. No body, and the stored content hash stays
    /// as it was.
    NotModified {
        /// A refreshed revalidator, if the origin sent one. Origins are
        /// allowed to send `ETag` on a 304 and many do.
        revalidate: Revalidator,
        /// The published subset, which a 304 still carries and which still
        /// updates the row's cache directives.
        headers_kept: Vec<(String, String)>,
        /// Blake3 over every header on the 304.
        headers_digest: [u8; 32],
        /// Wall time for the round trip.
        elapsed: Duration,
    },
    /// A 410, and nothing else. The one status that means never again.
    Gone,
    /// It did not work.
    Failed {
        /// The status, when we got far enough to have one.
        status: Option<u16>,
        /// What went wrong.
        failure: Failure,
        /// `Retry-After`, when the response carried one. Doc 07.6's rate
        /// limiter honours it, which is why it is on the outcome rather than
        /// only in the kept headers: a 429 has no body and so no row, and the
        /// one thing the origin said is the one thing we would otherwise
        /// throw away.
        retry_after: Option<RetryAfter>,
    },
    /// A redirect left the registrable domain the lease was for. Doc 04.7 says
    /// a fetcher must not follow one: it stops, and the coordinator admits the
    /// target as a fresh candidate so that robots is checked for the new host.
    RedirectedOffDomain {
        /// The hops taken before the one that left, which may be empty.
        redirects: Vec<Hop>,
        /// The URL that was refused, for admission.
        target: String,
        /// The status that carried it.
        status: u16,
    },
}

impl Outcome {
    /// Which of doc 04.5's outcomes this is.
    ///
    /// This enum is richer than that one, because it carries the body and the
    /// hops and the timeout stage, and the code is the part that goes in a
    /// receipt and in the `outcome` column. Two variants collapse on the way
    /// through and both are deliberate: every [`Stage`] of timeout is one
    /// `timeout`, since which timer fired is a diagnostic and not something a
    /// consumer of the dataset can act on, and a 404 and any other unhandled
    /// 4xx are both `not_found`, since the retry schedule is the same.
    #[must_use]
    pub const fn code(&self) -> OutcomeCode {
        match self {
            Self::Ok(_) => OutcomeCode::Ok,
            Self::NotModified { .. } => OutcomeCode::NotModified,
            Self::Gone => OutcomeCode::Gone,
            Self::RedirectedOffDomain { .. } => OutcomeCode::RedirectedOffHost,
            Self::Failed { failure, .. } => match failure {
                Failure::Dns => OutcomeCode::DnsFailure,
                Failure::Tls => OutcomeCode::TlsFailure,
                Failure::Connect => OutcomeCode::ConnectFailure,
                Failure::Timeout(_) => OutcomeCode::Timeout,
                Failure::ServerError => OutcomeCode::ServerError,
                Failure::NotFound => OutcomeCode::NotFound,
                Failure::Blocked => OutcomeCode::Blocked,
                Failure::RateLimited => OutcomeCode::RateLimited,
                Failure::TooLarge => OutcomeCode::TooLarge,
                Failure::Malformed => OutcomeCode::Malformed,
            },
        }
    }

    /// The string doc 04.5's receipt carries.
    #[must_use]
    pub const fn wire(&self) -> &'static str {
        self.code().wire()
    }

    /// The page, if there is one.
    #[must_use]
    pub fn page(&self) -> Option<&Page> {
        match self {
            Self::Ok(page) => Some(page),
            _ => None,
        }
    }

    /// Whether doc 05.8 should read this as a reason to escalate the tier.
    ///
    /// Only the status and header side of the signal is decided here. The body
    /// side, an interstitial or a client rendered shell, needs the extracted
    /// text and is milestone 2.
    #[must_use]
    pub const fn is_block_signal(&self) -> bool {
        matches!(
            self,
            Self::Failed {
                failure: Failure::Blocked | Failure::Tls,
                ..
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Failure, Outcome, Stage, Version};

    #[test]
    fn every_failure_has_a_wire_name_and_they_are_distinct() {
        let failures = [
            Failure::Dns,
            Failure::Connect,
            Failure::Tls,
            Failure::Timeout(Stage::Connect),
            Failure::ServerError,
            Failure::NotFound,
            Failure::Blocked,
            Failure::RateLimited,
            Failure::TooLarge,
            Failure::Malformed,
        ];
        let mut names: Vec<_> = failures
            .iter()
            .map(|failure| {
                Outcome::Failed {
                    status: None,
                    failure: *failure,
                    retry_after: None,
                }
                .wire()
            })
            .collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            failures.len(),
            "two failures share a wire name, so a receipt cannot tell them apart"
        );
    }

    #[test]
    fn the_three_timeout_stages_are_one_outcome_on_the_wire() {
        // The distinction matters to the operator dashboard in doc 15 and not
        // to the protocol, where a peer only needs to know the fetch did not
        // finish.
        for stage in [Stage::Connect, Stage::Read, Stage::Total] {
            assert_eq!(
                Outcome::Failed {
                    status: None,
                    failure: Failure::Timeout(stage),
                    retry_after: None,
                }
                .wire(),
                "timeout"
            );
        }
    }

    #[test]
    fn a_block_and_a_handshake_failure_escalate_and_a_server_error_does_not() {
        let blocked = Outcome::Failed {
            status: Some(403),
            failure: Failure::Blocked,
            retry_after: None,
        };
        let tls = Outcome::Failed {
            status: None,
            failure: Failure::Tls,
            retry_after: None,
        };
        let broken = Outcome::Failed {
            status: Some(500),
            failure: Failure::ServerError,
            retry_after: None,
        };
        assert!(blocked.is_block_signal());
        assert!(tls.is_block_signal(), "doc 05.8 names the handshake case");
        assert!(
            !broken.is_block_signal(),
            "escalating a tier because an origin is down wastes browser capacity"
        );
        assert!(!Outcome::Gone.is_block_signal());
    }

    #[test]
    fn the_http_version_survives_the_round_trip() {
        for (from, expected, text) in [
            (http::Version::HTTP_10, Version::Http10, "1.0"),
            (http::Version::HTTP_11, Version::Http11, "1.1"),
            (http::Version::HTTP_2, Version::Http2, "2"),
            (http::Version::HTTP_3, Version::Http3, "3"),
        ] {
            let version = Version::from(from);
            assert_eq!(version, expected);
            assert_eq!(version.as_str(), text);
        }
        assert_eq!(
            Version::from(http::Version::HTTP_09),
            Version::Http10,
            "0.9 has no receipt spelling and is close enough to 1.0"
        );
    }
}
