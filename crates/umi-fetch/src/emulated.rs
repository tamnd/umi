//! T2's socket: wreq over BoringSSL, wearing a browser profile.
//!
//! Specified in `docs/spec/05-fetch-tiers.md` section 5.5. T2 exists for the
//! case where a site's bot management refuses a non browser TLS stack even
//! though robots.txt allows us, which is usually a default rule somebody
//! turned on rather than a decision anyone made about this crawler.
//!
//! # Why a profile and not a fingerprint
//!
//! JA3, JA4 and the Akamai HTTP/2 fingerprint are hashes of what a client
//! actually sent: the cipher list in order, the extension list in order, the
//! supported groups, the ALPN, the SETTINGS frame, the WINDOW_UPDATE, the
//! pseudo header order. You cannot work backwards from the hash. `wreq` sets
//! the inputs instead, and `wreq-util` keeps a curated set of them per browser
//! build, which is why doc 05.5 picks it over anything that takes a hash.
//!
//! Doc 05.5 also says the profile is used as a unit rather than cherry picked,
//! and it means it. Getting JA4 right while sending the wrong header order is
//! worse than not trying, because a client whose layers disagree is a stronger
//! signal than a client that is honestly not a browser.
//!
//! # The one deviation, and why it is the only one
//!
//! Doc 07.1 requires the same user agent at every tier including this one, and
//! is explicit that this makes T2 inconsistent with itself: a browser
//! fingerprint under `umi/1.0 (+https://umi.dev/bot)` is a mismatch that bot
//! management will score. We take the score. The alternative is presenting as
//! Chrome, which is the thing that turns a crawler into a scraper in every
//! sense that matters, including legally.
//!
//! So the `sec-ch-ua` client hints keep saying Chromium while the user agent
//! says umi, and that is deliberate rather than an oversight. Stripping them
//! would be exactly the cherry picking doc 05.5 rules out, and it would not
//! make us more honest: the user agent is the header a site operator reads and
//! the one their robots.txt matches on, and it is ours.
//!
//! # Byte parity
//!
//! Not attempted, per doc 05.5. `curl-impersonate` gets there by shipping a
//! patched BoringSSL and spawning a process per request, which is not
//! available to us at 250 pages per second. A domain that genuinely needs byte
//! parity belongs at T3 or T4.
//!
//! # Two things that differ from T1 and are not bugs
//!
//! There is no cookie jar, because `wreq`'s is behind a feature we do not turn
//! on. That is the behaviour we want anyway: doc 11.5 never stores a
//! `Set-Cookie`, and a crawler carrying session state between pages is keeping
//! something it has no use for.
//!
//! Roots are Mozilla's bundled set rather than the platform store, because
//! that is what `wreq` ships and BoringSSL has no equivalent of
//! rustls-platform-verifier. So a host behind a corporate root that works at
//! T1 will fail at T2 with [`Failure::Tls`]. The blast radius is small, since
//! T2 is only reached for hosts that already refused T1, and the alternative
//! is `set_default_paths`, which reads OpenSSL's compiled in directory and is
//! empty on macOS and Windows.

use futures_util::StreamExt;
use url::Url;
use wreq_util::{Emulation, Platform, Profile};

use crate::engine::{BodyStream, Head, Transport, conditional, failure_from_text};
use crate::outcome::{Failure, Stage, Version};
use crate::{FetchConfig, FetchError, Result, Revalidator, USER_AGENT};

/// The browser build T2 presents as.
///
/// One profile, pinned, rather than a rotation. Doc 07.8 rules out anything
/// that looks like evasion and rotating an identity per request is that; and a
/// pinned profile is the only kind whose JA4 can be asserted at startup, which
/// doc 05.5 asks for. It moves when a dependency bump moves it and that is a
/// visible change in a diff rather than a silent one.
pub const PROFILE: Profile = Profile::Chrome149;

/// The operating system the profile claims.
///
/// Linux, because that is what the fleet runs, and because the client hints
/// and the user agent should at least agree with each other about the machine
/// even though doc 07.1 makes them disagree about the program.
pub const PLATFORM: Platform = Platform::Linux;

/// Where doc 05.5's self check asks what we look like.
///
/// A third party service, which is a dependency worth being uneasy about, and
/// it is the reason the check is a function somebody calls rather than
/// something that happens on every boot of every fetcher. There is no way
/// around needing an outside observer: the whole question is what the bytes
/// looked like after they left, and the only process that can answer it is one
/// that is not this one.
pub const ECHO_URL: &str = "https://tls.browserleaks.com/json";

/// What [`PROFILE`] presented as, the last time anybody looked.
///
/// Observed on 2026-08-28 from [`ECHO_URL`], on `wreq` 0.16.1 with
/// `wreq-util` 0.2.0. T1 answers `t13d1011h2_61a7ad8aa9b6_f9531d972513` on the
/// same endpoint, which is the difference this rung exists to make.
///
/// This is a drift check and not a claim about ground truth. Nothing here can
/// prove the value equals what a real Chrome 149 sends, because the only thing
/// that could is a real Chrome 149. What it does prove is that a dependency
/// bump has not quietly changed the cipher list or dropped an extension, which
/// is the failure doc 05.5 is worried about, because that failure looks like
/// nothing at all from inside the process.
///
/// JA4 rather than JA3 or the ordered JA4_o, deliberately. JA4's cipher and
/// extension hashes are over sorted lists with the GREASE values removed, so
/// it does not move when a TLS library shuffles the order it happens to write
/// extensions in, and a self check that fails for that reason would be turned
/// off within a week.
pub const EXPECTED_JA4: &str = "t13d1516h2_8daaf6152771_d8a2da3f94cd";

/// The browser shaped client.
pub(crate) struct Browser {
    client: wreq::Client,
}

impl Browser {
    /// Build the client, profile first and identity second.
    ///
    /// The order matters and is not cosmetic. `emulation` installs the whole
    /// header set the profile ships with, including its user agent, so the
    /// override has to come after it or the profile would win.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when BoringSSL or the certificate store will not
    /// initialise.
    pub(crate) fn build(config: &FetchConfig) -> Result<Self> {
        let emulation = Emulation::builder()
            .profile(PROFILE)
            .platform(PLATFORM)
            .build();

        let client = wreq::Client::builder()
            .emulation(emulation)
            .user_agent(USER_AGENT)
            .connect_timeout(config.connect_timeout)
            // Same reason as T1: doc 04.7 stops at the first redirect that
            // leaves the registrable domain, and it has to report which one.
            .redirect(wreq::redirect::Policy::none())
            .pool_max_idle_per_host(config.per_host)
            .build()
            .map_err(|e| FetchError::Client(e.to_string()))?;

        Ok(Self { client })
    }
}

impl Transport for Browser {
    type Response = wreq::Response;

    async fn send(
        &self,
        url: &Url,
        revalidate: Option<&Revalidator>,
    ) -> std::result::Result<Self::Response, Failure> {
        // No `Accept` here, unlike T1. The profile already carries the one the
        // browser sends, in the position the browser sends it, and `Accept` is
        // an input to JA4H.
        let mut request = self.client.get(url.as_str());
        for (name, value) in conditional(revalidate) {
            request = request.header(name, value);
        }
        request.send().await.map_err(transport_failure)
    }

    fn head(response: &Self::Response) -> Head {
        Head {
            status: response.status().as_u16(),
            version: Version::from(response.version()),
            headers: response.headers().clone(),
        }
    }

    fn body(response: Self::Response) -> BodyStream {
        Box::pin(
            response
                .bytes_stream()
                .map(|chunk| chunk.map_err(transport_failure)),
        )
    }
}

/// Pull the `ja4` field out of an echo endpoint's JSON.
///
/// By hand rather than with a JSON parser, because this is the only JSON in
/// the crate and a self check is not worth a dependency. A JA4 is `[a-z0-9_]`
/// and nothing else, so there is no escaping to get wrong, and a field that
/// does not look like one is treated as absent rather than trusted.
pub(crate) fn ja4_of(body: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let start = text.find("\"ja4\"")? + "\"ja4\"".len();
    let rest = text
        .get(start..)?
        .trim_start()
        .strip_prefix(':')?
        .trim_start();
    let value = rest.strip_prefix('"')?;
    let end = value.find('"')?;
    let value = &value[..end];
    value
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        .then(|| value.to_owned())
}

/// Turn a wreq error into the failure class the scheduler acts on.
///
/// Same shape as T1's, and separate from it because the two error types are
/// unrelated even though wreq is a fork of reqwest. The typed questions first,
/// then the shared source chain walk.
fn transport_failure(error: wreq::Error) -> Failure {
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
    failure_from_text(&error)
}

#[cfg(test)]
mod tests {
    use super::{ECHO_URL, EXPECTED_JA4, ja4_of};
    use crate::{Emulated, USER_AGENT};

    #[test]
    fn a_ja4_is_read_out_of_the_echo_json() {
        let body =
            br#"{"user_agent":"umi/1.0","ja4":"t13d1516h2_8daaf6152771_d8a2da3f94cd","ja4_r":"x"}"#;
        assert_eq!(ja4_of(body).as_deref(), Some(EXPECTED_JA4));
    }

    #[test]
    fn json_without_a_ja4_is_not_a_ja4() {
        // Each of these has been an actual shape somebody's endpoint returned:
        // an error page, a null, a number, and a value with a quote in it. The
        // point is that none of them come back as a string the caller would
        // then compare against the expected value and log a mismatch for.
        for body in [
            &b"not json at all"[..],
            br#"{"error":"rate limited"}"#,
            br#"{"ja4":null}"#,
            br#"{"ja4":1516}"#,
            br#"{"ja4":"has a Capital"}"#,
            br#"{"ja4":"#,
        ] {
            assert_eq!(ja4_of(body), None, "{:?}", String::from_utf8_lossy(body));
        }
    }

    #[test]
    fn whitespace_around_the_colon_is_fine() {
        let body = br#"{ "ja4" :   "t13d1516h2_8daaf6152771_d8a2da3f94cd" }"#;
        assert_eq!(ja4_of(body).as_deref(), Some(EXPECTED_JA4));
    }

    #[test]
    fn t2_keeps_the_honest_user_agent() {
        // Doc 07.1, and the one line of this module that is a policy rather
        // than a mechanism. The profile ships Chrome's user agent and the
        // builder has to be putting ours back after it.
        let fetcher = Emulated::new().expect("boringssl initialises");
        assert_eq!(fetcher.config().per_host, 2);
        assert_eq!(USER_AGENT, "umi/1.0 (+https://umi.dev/bot)");
    }

    /// Doc 05.5's self check, against the real internet.
    ///
    /// Ignored by default because it needs a network and a third party service
    /// that can be down or rate limiting, and a test that fails for those
    /// reasons teaches people to ignore test failures. Run it deliberately,
    /// after a dependency bump:
    ///
    /// ```text
    /// cargo test -p umi-fetch --features emulation -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "needs the network and a third party echo endpoint"]
    async fn the_fingerprint_is_still_the_one_we_pinned() {
        let fetcher = Emulated::new().expect("boringssl initialises");
        let observed = fetcher
            .observed_ja4(ECHO_URL)
            .await
            .expect("the echo endpoint answered");
        assert_eq!(
            observed, EXPECTED_JA4,
            "the tls fingerprint moved, so either wreq changed or the profile did"
        );
    }
}
