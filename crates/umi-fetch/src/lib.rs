//! The fetch tiers, and the one pipeline they share.
//!
//! Specified in `docs/spec/05-fetch-tiers.md`. [`Fetcher`] is T1 from section
//! 5.4: hyper over rustls, HTTP/2 preferred with an HTTP/1.1 fallback, an
//! honest and fixed identity, a connection cap per host, a timeout at every
//! stage and a hard cap on the body. It does not try to look like a browser
//! and it should not, because sending Chrome's header set from a rustls stack
//! produces a mismatch between the TLS fingerprint and the HTTP layer that is
//! more suspicious than being honestly a bot.
//!
//! T1 is deliberately the boring rung and deliberately the first one. Doc 05.2
//! assumes a plain client answers about 90 percent of everything that is not a
//! revalidate, and the crawl should keep being mostly that.
//!
//! `Emulated` is T2 from section 5.5, which is a browser's TLS and HTTP/2
//! fingerprint for the hosts whose bot management refuses a plain client even
//! though robots.txt allows us. It is behind the non default `emulation`
//! feature, because it links BoringSSL and BoringSSL cannot share a process
//! with `openssl-sys`, so it is named here rather than linked: the type is not
//! in this build unless the feature is on, and a link that resolves only half
//! the time is a broken build on the other half.
//!
//! [`Ladder`] is what a crawl actually holds: every tier that got compiled in,
//! picked between by the tier the scheduler leased the URL at.
//!
//! # What is not here
//!
//! T3 and T4, Web Bot Auth request signing from doc 07.2, and the escalation
//! state machine from doc 05.8. This crate deliberately makes no scheduling
//! decision at all: it reports what happened and doc 09 decides what that
//! means.
//!
//! robots.txt is not consulted here either, and that is doc 04.7's rule rather
//! than an omission. The robots decision belongs to the coordinator, a
//! disallowed URL is never leased, and a community fetcher therefore cannot
//! make the crawl impolite through a parsing bug.
//!
//! # No clock
//!
//! Nothing here reads a wall clock. Elapsed time comes from `Instant`, which
//! is monotonic, and anything that needs to be stamped with a date is stamped
//! by the caller. That is gate 1.2's rule and it is what lets a fetch be
//! replayed.
//!
//! # rustls only, unless somebody asks
//!
//! The default build has no `openssl-sys` and no BoringSSL anywhere in its
//! tree. `the_tree_is_rustls_only` asserts the first half against the
//! lockfile, and `scripts/check-tls.sh` asserts the whole of it against
//! `cargo tree` on a default build, which is gate 2.2. The lockfile alone
//! stopped being enough the moment `wreq` became an optional dependency,
//! because an optional dependency is still locked.
//!
//! It matters because a static binary that dynamically links OpenSSL is not a
//! static binary, and because BoringSSL and OpenSSL share symbol prefixes: a
//! tree holding both either fails to link or links and segfaults.
//!
//! Roots come from the platform store through rustls-platform-verifier, which
//! is reqwest's own default. A volunteer running a fetcher behind a corporate
//! root should not have to configure anything, and pinning a root set here
//! would mean shipping a way to unpin it.

use std::sync::Arc;
use std::time::Duration;

pub mod challenge;
pub mod date;
mod engine;
pub mod headers;
pub mod outcome;
mod plain;
pub mod sniff;

#[cfg(feature = "emulation")]
mod emulated;

use engine::Engine;

pub use outcome::{Failure, Hop, Outcome, OutcomeCode, Page, RetryAfter, Stage, Version};
pub use sniff::Media;
pub use umi_types::{Revalidator, Tier};

#[cfg(feature = "emulation")]
pub use emulated::{ECHO_URL, EXPECTED_JA4, PLATFORM, PROFILE};

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
    /// Doc 05.5's T2 self check could not be run at all, which is different
    /// from running and finding a mismatch. A mismatch is a value the caller
    /// compares; this is the echo endpoint being down or answering something
    /// that is not the JSON it documents.
    #[error("could not read the tls fingerprint: {0}")]
    SelfCheck(String),
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
    inner: Arc<Engine<plain::Plain>>,
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
        let transport = plain::Plain::build(&config)?;
        Ok(Self {
            inner: Arc::new(Engine::new(transport, config)),
        })
    }

    /// The configuration this client was built with.
    #[must_use]
    pub fn config(&self) -> &FetchConfig {
        self.inner.config()
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
        self.inner.fetch(url, revalidate).await
    }
}

/// T2, the browser shaped client from doc 05.5.
///
/// Same pipeline as [`Fetcher`] and the same rules applied to what comes back.
/// The difference is the socket: a BoringSSL handshake and an HTTP/2 SETTINGS
/// frame that match a real Chrome build, which is what gets past bot
/// management that refuses a plain client.
///
/// The user agent is still ours. See [`emulated`] for why, and for why that
/// is the only thing about the profile that is changed.
///
/// Cheap to clone, for the same reason [`Fetcher`] is.
#[cfg(feature = "emulation")]
#[derive(Clone, Debug)]
pub struct Emulated {
    inner: Arc<Engine<emulated::Browser>>,
}

#[cfg(feature = "emulation")]
impl Emulated {
    /// A T2 client with doc 05.4's defaults.
    ///
    /// The timeouts and the body cap are T1's on purpose. A browser
    /// fingerprint is not a reason to be more patient with an origin.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when BoringSSL will not initialise.
    pub fn new() -> Result<Self> {
        Self::with_config(FetchConfig::default())
    }

    /// A T2 client with the knobs turned.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when BoringSSL will not initialise.
    pub fn with_config(config: FetchConfig) -> Result<Self> {
        let transport = emulated::Browser::build(&config)?;
        Ok(Self {
            inner: Arc::new(Engine::new(transport, config)),
        })
    }

    /// The configuration this client was built with.
    #[must_use]
    pub fn config(&self) -> &FetchConfig {
        self.inner.config()
    }

    /// Fetch one URL at T2.
    ///
    /// # Errors
    ///
    /// [`FetchError::Url`] when the URL does not parse or is not http(s).
    /// Nothing else.
    pub async fn fetch(&self, url: &str, revalidate: Option<&Revalidator>) -> Result<Outcome> {
        self.inner.fetch(url, revalidate).await
    }

    /// The JA4 an echo endpoint says we just presented.
    ///
    /// Doc 05.5 asks for this as a startup check rather than an assumption,
    /// and the reason is worth restating: a dependency bump that changed the
    /// cipher list or dropped an extension would leave T2 with a fingerprint
    /// that is neither Chrome's nor rustls's, which is a worse thing to be
    /// than either. Nothing about the failure would be visible, because the
    /// requests would keep succeeding on the hosts that were never the problem.
    ///
    /// Compare what comes back against [`emulated::EXPECTED_JA4`]. This
    /// returns the observed value rather than a boolean so that a mismatch can
    /// be logged with both halves in it, which is the only form of the message
    /// anyone can act on.
    ///
    /// # Errors
    ///
    /// [`FetchError::SelfCheck`] when the endpoint does not answer or does not
    /// answer with the JSON it documents, and [`FetchError::Url`] when `echo`
    /// is not a URL.
    pub async fn observed_ja4(&self, echo: &str) -> Result<String> {
        let outcome = self.fetch(echo, None).await?;
        let Outcome::Ok(page) = outcome else {
            return Err(FetchError::SelfCheck(format!(
                "{echo} answered {outcome:?}"
            )));
        };
        emulated::ja4_of(&page.body)
            .ok_or_else(|| FetchError::SelfCheck(format!("{echo} sent no ja4 field")))
    }
}

/// Every tier this binary was built with, picked between by lease.
///
/// Doc 05.8 stores a preferred tier per host and hands it to the fetcher on
/// the lease. This is the thing that reads it. It is not the escalation state
/// machine, which lives in `umi-state` and decides what a block means; this
/// only routes a request to the rung it was asked for.
///
/// # When a rung is missing
///
/// A build without the `emulation` feature has no T2, and a lease that asks
/// for T2 is served by T1 instead. That is the honest failure mode rather than
/// an error: the crawl keeps making progress, the host keeps getting blocked,
/// the block count climbs and doc 05.8 backs it off to `refusing` on its own.
/// [`Ladder::highest`] is how an operator finds out before that happens, and
/// `umi crawl` logs it at startup.
///
/// T3 and T4 do not exist yet, and `TierPolicy::CEILING` is `Tier::Emulated`,
/// so nothing leases above T2 today. When they arrive they land here.
#[derive(Clone, Debug)]
pub struct Ladder {
    plain: Fetcher,
    #[cfg(feature = "emulation")]
    emulated: Emulated,
}

impl Ladder {
    /// Build every tier that is compiled in, with doc 05.4's defaults.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when any tier's TLS backend will not initialise.
    pub fn new() -> Result<Self> {
        Self::with_config(FetchConfig::default())
    }

    /// Build every tier that is compiled in, with the knobs turned.
    ///
    /// One config for all of them. A per tier config would be a knob nobody
    /// has asked for and a way for two tiers to disagree about the body cap.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when any tier's TLS backend will not initialise.
    pub fn with_config(config: FetchConfig) -> Result<Self> {
        Ok(Self {
            #[cfg(feature = "emulation")]
            emulated: Emulated::with_config(config.clone())?,
            plain: Fetcher::with_config(config)?,
        })
    }

    /// The highest rung this build actually has.
    #[must_use]
    pub const fn highest() -> Tier {
        if cfg!(feature = "emulation") {
            Tier::Emulated
        } else {
            Tier::Plain
        }
    }

    /// The T1 client, for callers that want that rung by name.
    #[must_use]
    pub const fn plain(&self) -> &Fetcher {
        &self.plain
    }

    /// Fetch one URL at the tier the lease asked for.
    ///
    /// `Tier::Revalidate` and `Tier::Plain` are the same client and differ
    /// only in whether `revalidate` is set, which is the caller's decision.
    ///
    /// # Errors
    ///
    /// [`FetchError::Url`] when the URL does not parse or is not http(s).
    /// Nothing else.
    pub async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: Tier,
    ) -> Result<Outcome> {
        match tier {
            #[cfg(feature = "emulation")]
            Tier::Emulated | Tier::Rendered | Tier::Supervised => {
                self.emulated.fetch(url, revalidate).await
            }
            _ => self.plain.fetch(url, revalidate).await,
        }
    }
}

#[cfg(test)]
mod tests;
