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
//! T3 and T4, and the escalation state machine from doc 05.8. This crate
//! deliberately makes no scheduling decision at all: it reports what happened
//! and doc 09 decides what that means.
//!
//! Doc 07.2's Web Bot Auth request signing is here, in [`webbotauth`], because
//! the signature covers the request and the request is built here. A `Fetcher`
//! built with a signer puts three extra headers on every GET at every tier. A
//! `Fetcher` built without one sends none of them, which is what a volunteer's
//! build does until doc 06 gives them a fetcher key.
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
//! A Web Bot Auth signature carries a `created` and an `expires` in unix
//! seconds, which is the one thing in this crate that genuinely needs a wall
//! clock. [`webbotauth::Signer`] takes it as a closure the caller supplies, so
//! the rule still holds: there is no clock in this crate, there is a place to
//! put one.
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
pub mod resolver;
pub mod sniff;
pub mod webbotauth;

#[cfg(feature = "emulation")]
mod emulated;

#[cfg(feature = "render")]
pub mod rendered;

use engine::Engine;

pub use outcome::{Failure, Hop, Outcome, OutcomeCode, Page, RetryAfter, Stage, Version};
pub use sniff::Media;
pub use umi_types::{Revalidator, Tier, TierPath};
pub use webbotauth::{Directory, Jwk, SignatureError, Signer};

#[cfg(feature = "emulation")]
pub use emulated::{ECHO_URL, EXPECTED_JA4, PLATFORM, PROFILE};

#[cfg(feature = "render")]
pub use rendered::{Counts, RenderConfig, Renderer};

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
        Self::with_signer(config, None)
    }

    /// A client that signs every request under doc 07.2.
    ///
    /// The signer is shared rather than owned because its nonce counter must
    /// not be duplicated, and because a fleet holds one crawl identity key and
    /// not one per tier.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when the TLS backend will not initialise.
    pub fn with_signer(config: FetchConfig, signer: Option<Arc<Signer>>) -> Result<Self> {
        let transport = plain::Plain::build(&config, signer)?;
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
/// The user agent is still ours. Doc 07.1 is why, and the module this type
/// lives in spells out why that is the only thing about the profile that is
/// changed.
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
        Self::with_signer(config, None)
    }

    /// A T2 client that signs every request under doc 07.2.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when BoringSSL will not initialise.
    pub fn with_signer(config: FetchConfig, signer: Option<Arc<Signer>>) -> Result<Self> {
        let transport = emulated::Browser::build(&config, signer)?;
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
    /// Compare what comes back against [`EXPECTED_JA4`]. This
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
/// T3 is here when the build has the `render` feature and a browser actually
/// started, which is two conditions rather than one: a box with no Chrome
/// installed compiles T3 in and still has no T3. T4 does not exist yet, and
/// `TierPolicy::CEILING` is `Tier::Emulated`, so nothing leases above T2 today.
#[derive(Clone, Debug)]
pub struct Ladder {
    plain: Fetcher,
    #[cfg(feature = "emulation")]
    emulated: Emulated,
    /// `None` on a box that has no browser, which is most of them.
    #[cfg(feature = "render")]
    rendered: Option<Renderer>,
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
        Self::with_signer(config, None)
    }

    /// Build every tier that is compiled in, signing every request.
    ///
    /// One signer for all of them, for the same reason there is one config:
    /// doc 07.2 says one crawl identity per crawler, and a site operator who
    /// looked up our key should get the same answer whichever rung answered
    /// their page.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when any tier's TLS backend will not initialise.
    pub fn with_signer(config: FetchConfig, signer: Option<Arc<Signer>>) -> Result<Self> {
        Ok(Self {
            #[cfg(feature = "emulation")]
            emulated: Emulated::with_signer(config.clone(), signer.clone())?,
            #[cfg(feature = "render")]
            rendered: None,
            plain: Fetcher::with_signer(config, signer)?,
        })
    }

    /// Build the whole ladder, T3 included, and start a browser for it.
    ///
    /// Separate from [`Ladder::with_signer`] and async because this one is not
    /// free: it spawns a Chromium process that holds a gigabyte or more once
    /// its tabs are warm. A caller that wants T3 asks for it here, and doc
    /// 05.6's zero tab cap on a box like server1 is the way to say no.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when a tier's TLS backend will not initialise or
    /// when the browser will not start. Not having a browser is an error here
    /// rather than a silent `None`, because a caller reached this constructor
    /// on purpose and would otherwise run a whole crawl at T2 without knowing.
    #[cfg(feature = "render")]
    pub async fn with_rendered(
        config: FetchConfig,
        signer: Option<Arc<Signer>>,
        render: RenderConfig,
    ) -> Result<Self> {
        let mut ladder = Self::with_signer(config.clone(), signer.clone())?;
        ladder.rendered = Some(Renderer::launch(config, render, signer).await?);
        Ok(ladder)
    }

    /// The highest rung this build actually has.
    ///
    /// Takes `&self` because T3 is not a compile time fact. The feature can be
    /// on and the browser can be absent, and an operator reading a startup log
    /// wants to know what this process can do rather than what its build could
    /// have done.
    #[must_use]
    pub fn highest(&self) -> Tier {
        #[cfg(feature = "render")]
        if self.rendered.is_some() {
            return Tier::Rendered;
        }
        if cfg!(feature = "emulation") {
            Tier::Emulated
        } else {
            Tier::Plain
        }
    }

    /// The browser pool, when this process has one.
    ///
    /// This is where doc 05.6's per page cost comes from, through
    /// [`Renderer::counts`].
    #[cfg(feature = "render")]
    #[must_use]
    pub const fn rendered(&self) -> Option<&Renderer> {
        self.rendered.as_ref()
    }

    /// Doc 05.9's `browser_pool_capacity`, in pages a second, or `None` when
    /// this process has no browser.
    ///
    /// Not behind the feature, unlike everything else about T3, because the
    /// crawl loop's render budget has to compile the same way either way. A
    /// `cfg` here saves two more crates from carrying the feature, and `None`
    /// is the honest answer for a build that cannot render.
    #[must_use]
    pub fn render_rate(&self) -> Option<f64> {
        #[cfg(feature = "render")]
        let rate = self.rendered.as_ref().map(Renderer::rate);
        #[cfg(not(feature = "render"))]
        let rate = None;
        rate
    }

    /// Close the browser, if there is one.
    ///
    /// Dropping the ladder also kills Chromium, because the child is spawned
    /// with `kill_on_drop`, but it leaves the profile directory behind. A
    /// fetcher shutting down cleanly should call this.
    pub async fn shutdown(self) {
        #[cfg(feature = "render")]
        if let Some(rendered) = self.rendered {
            rendered.shutdown().await;
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
    /// The answer carries the rungs it took to get it, because the tier a
    /// lease asks for and the tier that answers are not always the same one
    /// and doc 05.5 publishes the second of those.
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
    ) -> Result<Served> {
        // T3 first, so that a lease for it gets a browser when there is one and
        // falls through to T2 and then T1 when there is not. Doc 05.4's rule
        // about a missing rung is the same at every height: serve the page from
        // the rung below rather than error, and let the block count say so.
        #[cfg(feature = "render")]
        if matches!(tier, Tier::Rendered | Tier::Supervised)
            && let Some(rendered) = &self.rendered
        {
            let outcome = rendered.fetch(url, revalidate).await?;
            // A browser handed a PDF, an image or a stylesheet says so rather
            // than serialising the viewer around it, and the answer is the rung
            // below, where those come back as the bytes they are. Without this
            // the row is a failure with a status the fetch never really got and
            // a length of zero, and the host wears the failure on its counter
            // for a URL that would have fetched perfectly well at T1.
            //
            // One step down and no loop. T1 cannot return this, so the second
            // attempt is the last one either way.
            if !matches!(
                outcome,
                Outcome::Failed {
                    failure: Failure::NotDocument,
                    ..
                }
            ) {
                // `descended` and not `at`, which matters only for T4 and
                // matters a lot there. Doc 05.7's T4 is a real browser with a
                // real profile driven by or with a human, and this is the T3
                // engine: one incognito context, a temporary profile, nobody
                // watching. It is the rung below and the path has to say so.
                // Labelling this T4 would put the tier in the published column
                // and in the row an operator gets shown, and the whole reason
                // the allowlist is published is that those have to be true.
                return Ok(Served::descended(tier, Tier::Rendered, outcome));
            }
            let outcome = self.plain.fetch(url, revalidate).await?;
            return Ok(Served {
                path: TierPath::new(tier).then(Tier::Plain),
                outcome,
            });
        }
        #[cfg(feature = "emulation")]
        if matches!(tier, Tier::Emulated | Tier::Rendered | Tier::Supervised) {
            let outcome = self.emulated.fetch(url, revalidate).await?;
            return Ok(Served::descended(tier, Tier::Emulated, outcome));
        }
        // T0 and T1 are the same client, so a lease for either of them lands
        // on the rung it asked for and has not descended. Anything higher that
        // reaches here is a rung this build does not have, and doc 05.4 says
        // serve it from the highest rung that exists rather than error.
        let served = match tier {
            Tier::Revalidate | Tier::Plain => tier,
            _ => Tier::Plain,
        };
        let outcome = self.plain.fetch(url, revalidate).await?;
        Ok(Served::descended(tier, served, outcome))
    }
}

/// One answer, and the rungs it took to get it.
#[derive(Debug)]
pub struct Served {
    /// What came back.
    pub outcome: Outcome,
    /// Doc 04.5's `tier_path`, which starts at the tier the lease asked for
    /// and ends at the tier that answered.
    pub path: TierPath,
}

impl Served {
    /// An answer from the rung that was asked for, which is nearly all of
    /// them.
    #[must_use]
    pub const fn at(tier: Tier, outcome: Outcome) -> Self {
        Self {
            outcome,
            path: TierPath::new(tier),
        }
    }

    /// An answer from `served` when `asked` was requested, which is one rung
    /// when they are the same and two when the build does not have the rung
    /// the lease wanted.
    #[must_use]
    pub const fn descended(asked: Tier, served: Tier, outcome: Outcome) -> Self {
        // By byte rather than by `==`, which is not something a const fn can
        // call on 1.98.
        let path = if asked.as_u8() == served.as_u8() {
            TierPath::new(served)
        } else {
            TierPath::new(asked).then(served)
        };
        Self { outcome, path }
    }
}

#[cfg(test)]
mod tests;
