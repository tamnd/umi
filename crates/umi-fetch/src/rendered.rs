//! T3's browser: headless Chromium, driven over the DevTools Protocol.
//!
//! Specified in `docs/spec/05-fetch-tiers.md` section 5.6. T3 is for the pages
//! whose content only exists after a script has run, which is a different
//! problem from T2's. T2 answers a bot manager that does not like our socket.
//! T3 answers an origin that sends an empty shell and builds the page in the
//! client, and no amount of fingerprint work at T2 gets that page.
//!
//! # T3 is for rendering and not for evasion
//!
//! Doc 05.6 is explicit and this module holds the line. Nothing here hides
//! `navigator.webdriver`, normalises the `HeadlessChrome/` token or patches the
//! plugin list. `chromiumoxide` ships an `enable_stealth_mode` and it is not
//! called. The user agent is [`USER_AGENT`] at this rung exactly as it is at
//! every other one, because doc 07.1 says so and because a browser that says
//! umi is the only honest thing a browser we drive can say.
//!
//! If a site blocks headless Chromium specifically, the page does not get
//! crawled. That is the whole of the policy.
//!
//! # Why the pipeline is not shared with T1 and T2
//!
//! `engine::Engine` is generic over a client that puts a GET on the wire and
//! streams one body back, and a browser does neither. What comes back here is
//! a DOM that a hundred requests contributed to, and the interesting decisions
//! are about which of those hundred requests to allow at all.
//!
//! The rules that do transfer are shared rather than copied. The status
//! classification, doc 05.8's four way split and the interstitial check are
//! `engine::classify` and [`challenge`], the same code T1 and T2 run, so a 403
//! means the same thing whichever rung saw it.
//!
//! # Subresources are not crawled
//!
//! A rendered page pulls scripts and API responses that were never leased and
//! never checked against robots.txt. That is not a hole in doc 04.7 and it is
//! worth saying why. Those requests are the ones any browser makes to display
//! the page a person asked for, they are made once, they go away when the tab
//! is recycled, and not one byte of them is stored, extracted or published.
//! The only thing that leaves this module is the rendered document, which is
//! the URL that was leased and the URL robots was checked for.
//!
//! [`decide`] is what keeps that true in practice: a third party tracker is
//! refused, anything that does not put a word in the DOM is refused, and the
//! total is capped.
//!
//! # No clock
//!
//! Same rule as the rest of the crate. Every deadline here is an `Instant`,
//! which is monotonic, and the only wall clock in a signed request comes from
//! the closure the caller gave [`Signer`].

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::fetch::{
    ContinueRequestParams, EventRequestPaused, FailRequestParams, HeaderEntry,
};
use chromiumoxide::cdp::browser_protocol::network::{
    ErrorReason, EventLoadingFailed, EventLoadingFinished, EventRequestWillBeSent,
    EventResponseReceived, Headers, RequestId, ResourceType, Response,
};
use chromiumoxide::cdp::browser_protocol::page::FrameId;
use chromiumoxide::cdp::browser_protocol::target::{
    CreateBrowserContextParams, CreateTargetParams,
};
use chromiumoxide::{Page as Tab, cdp::IntoEventKind};
use futures_util::StreamExt;
use http::HeaderMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use url::Url;

use crate::engine::{Verdict, classify, conditional, revalidator};
use crate::outcome::{Failure, Hop, Outcome, Page, Stage, Version};
use crate::webbotauth::Signer;
use crate::{FetchConfig, FetchError, Result, Revalidator, USER_AGENT, challenge, headers, sniff};

/// How long the network has to stay quiet before the page is called done.
///
/// Doc 05.6's `networkIdle0` with a 1500 ms quiet period. Zero requests in
/// flight rather than the looser two request form, because the pages the
/// looser rule exists for are the ones that hold a connection open forever and
/// they hit [`CEILING`] either way.
pub const QUIET: Duration = Duration::from_millis(1500);

/// The hard ceiling on one render, doc 05.6.
///
/// A page that has not settled in ten seconds is not going to, and the tab is
/// worth more to the next URL than to this one. Whatever has rendered by then
/// is what gets serialised, which is usually the whole page: the ceiling is
/// normally reached because of a beacon that retries forever rather than
/// because content is still missing.
pub const CEILING: Duration = Duration::from_secs(10);

/// The total subresource budget for one page, doc 05.6.
///
/// Two megabytes is the number in the spec and it is generous. Doc 05.6 puts
/// the difference between a filtered render and an unfiltered one at 600 KB
/// against 3 MB, so the cap is a backstop for the pages that defeat the
/// resource filter rather than where the saving comes from.
pub const SUBRESOURCE_CAP: u64 = 2 * 1024 * 1024;

/// How many tabs may be open at once, doc 05.6.
///
/// Eight on server2, which is the only fleet box that runs a browser, and zero
/// on server1. At 150 to 300 MB of resident memory per tab this is most of
/// what a six core box has spare, which is the real reason T3 is under one
/// percent of volume rather than a scheduling preference.
pub const TABS: usize = 8;

/// How many pages one tab serves before it is recycled, doc 05.6.
pub const PAGES_PER_TAB: u32 = 50;

/// What a document costs before the quiet period starts.
///
/// Only used to seed [`Renderer::rate`] before anything has been rendered,
/// because doc 05.9's budget has to have a number on the first tick and the
/// pool has no measurement yet. A second is a round number that is not
/// optimistic: the load only figure in the T3 bench is 504 ms on server2 and
/// 578 ms on server3, and the pool converges on its own mean within a few
/// pages anyway.
pub const LOAD: Duration = Duration::from_secs(1);

/// How long one tab lives before it is recycled, doc 05.6.
///
/// Both limits are here because they catch different leaks. A tab that serves
/// fifty heavy pages leaks through the renderer, and a tab that sits parked
/// for an hour leaks through whatever the last page left running.
pub const TAB_LIFETIME: Duration = Duration::from_secs(600);

/// The third party domains a rendered page never gets to load.
///
/// Doc 05.6's tracker list. Registrable domains, sorted, matched exactly, and
/// only ever applied to a third party: a site's own analytics is first party
/// and is left alone, because it is often the same bundle that builds the page.
///
/// This is a short list of the ones that turn up on everything rather than a
/// replacement for a real blocklist. Shipping a hundred thousand rules would
/// mean shipping somebody else's list, keeping it current and taking a licence
/// with it, and the last few percent of coverage is not worth that when
/// [`SUBRESOURCE_CAP`] is already there to catch what leaks through.
pub const TRACKERS: [&str; 47] = [
    "33across.com",
    "adjust.com",
    "adnxs.com",
    "adsafeprotected.com",
    "adsrvr.org",
    "amplitude.com",
    "appsflyer.com",
    "bidswitch.net",
    "braze.com",
    "bugsnag.com",
    "casalemedia.com",
    "chartbeat.com",
    "chartbeat.net",
    "clarity.ms",
    "cloudflareinsights.com",
    "cookielaw.org",
    "crazyegg.com",
    "criteo.com",
    "criteo.net",
    "doubleclick.net",
    "doubleverify.com",
    "fullstory.com",
    "google-analytics.com",
    "googleadservices.com",
    "googlesyndication.com",
    "googletagmanager.com",
    "hotjar.com",
    "luckyorange.com",
    "mixpanel.com",
    "moatads.com",
    "mouseflow.com",
    "newrelic.com",
    "nr-data.net",
    "onetrust.com",
    "openx.net",
    "optimizely.com",
    "outbrain.com",
    "parsely.com",
    "pubmatic.com",
    "quantserve.com",
    "rubiconproject.com",
    "scorecardresearch.com",
    "segment.com",
    "segment.io",
    "sharethrough.com",
    "smartadserver.com",
    "taboola.com",
];

/// The knobs T3 has that the other rungs do not.
///
/// Separate from [`FetchConfig`] rather than folded into it, because every
/// field here is meaningless at T1 and T2, and a config where two thirds of the
/// fields apply to one rung is a config people misread.
#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub struct RenderConfig {
    /// The tab cap. [`TABS`] on a box that renders and zero on one that does
    /// not, and zero is refused by [`Renderer::launch`] rather than quietly
    /// meaning something else.
    pub tabs: usize,
    /// The quiet period, [`QUIET`].
    pub quiet: Duration,
    /// The hard ceiling on one render, [`CEILING`].
    pub ceiling: Duration,
    /// The subresource byte budget, [`SUBRESOURCE_CAP`].
    pub subresource_cap: u64,
    /// Pages per tab before recycling, [`PAGES_PER_TAB`].
    pub pages_per_tab: u32,
    /// Age per tab before recycling, [`TAB_LIFETIME`].
    pub tab_lifetime: Duration,
    /// Where the browser is, when it is not on `PATH` under one of the names
    /// Chrome and Chromium ship as.
    pub executable: Option<PathBuf>,
    /// Whether Chromium keeps its sandbox.
    ///
    /// True, and it should stay true. The sandbox is what stands between a
    /// renderer bug in a page we fetched from a stranger and the machine, which
    /// is exactly the risk a crawler signs up for.
    ///
    /// Chromium refuses to start as root with the sandbox on, so a fetcher
    /// running as root has to turn this off. The fix is to not run the fetcher
    /// as root, and [`Renderer::launch`] says so in the error rather than
    /// turning the sandbox off on its own.
    pub sandbox: bool,
}

impl Default for RenderConfig {
    fn default() -> Self {
        Self {
            tabs: TABS,
            quiet: QUIET,
            ceiling: CEILING,
            subresource_cap: SUBRESOURCE_CAP,
            pages_per_tab: PAGES_PER_TAB,
            tab_lifetime: TAB_LIFETIME,
            executable: None,
            sandbox: true,
        }
    }
}

/// Why one request was refused.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reason {
    /// The resource type is not one doc 05.6 allows.
    Resource,
    /// A third party on [`TRACKERS`].
    Tracker,
    /// The page has already spent its [`RenderConfig::subresource_cap`].
    Budget,
    /// The top frame tried to leave the registrable domain the lease was for.
    /// An HTTP redirect that does this is doc 04.7's case and ends the render;
    /// a script that does it is refused and the page we already have is what
    /// comes back.
    OffDomain,
    /// More redirects than [`FetchConfig::max_redirects`].
    Redirects,
}

impl Reason {
    /// The word that goes in a log line.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resource => "resource",
            Self::Tracker => "tracker",
            Self::Budget => "budget",
            Self::OffDomain => "off-domain",
            Self::Redirects => "redirects",
        }
    }
}

/// What to do with one intercepted request.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decision {
    /// Let it go to the network.
    Allow,
    /// Fail it, with the reason for the counters.
    Block(Reason),
}

/// Whether doc 05.6 lets this kind of request through at all.
///
/// Allow `Document`, `XHR`, `Fetch` and `Script`, block `Image`, `Media`,
/// `Font` and `Stylesheet`, and block everything doc 05.6 did not name. The
/// unnamed ones are pings, beacons, prefetches, manifests and web sockets, and
/// not one of them puts a word in the DOM.
///
/// `Preflight` is the one addition and it is not a widening. A preflight is the
/// browser's own half of a cross origin `XHR` or `Fetch` that we just allowed,
/// so refusing it would fail the request we said yes to, which is not a policy
/// anybody meant to write.
#[must_use]
pub fn allowed(resource: &ResourceType) -> bool {
    matches!(
        *resource,
        ResourceType::Document
            | ResourceType::Xhr
            | ResourceType::Fetch
            | ResourceType::Script
            | ResourceType::Preflight
    )
}

/// Whether a registrable domain is on [`TRACKERS`].
#[must_use]
pub fn tracker(domain: &str) -> bool {
    TRACKERS.binary_search(&domain).is_ok()
}

/// Doc 05.6's subresource policy, as one function over one request.
///
/// Pure, so that the policy can be tested without a browser, which matters more
/// here than usual: the alternative is a suite that only runs on a box with
/// Chrome on it, and a rule nobody can check is a rule that drifts.
///
/// `page` is the registrable domain of the URL that was leased, `spent` is the
/// subresource bytes this render has used so far, and `cap` is
/// [`RenderConfig::subresource_cap`].
#[must_use]
pub fn decide(resource: &ResourceType, url: &str, page: &str, spent: u64, cap: u64) -> Decision {
    if !allowed(resource) {
        return Decision::Block(Reason::Resource);
    }
    if let Some(domain) = registrable(url)
        && domain != page
        && tracker(&domain)
    {
        return Decision::Block(Reason::Tracker);
    }
    // The top document is the page itself and is not a subresource, so it is
    // never what the budget refuses. An iframe document is one, and lands here.
    if *resource != ResourceType::Document && spent >= cap {
        return Decision::Block(Reason::Budget);
    }
    Decision::Allow
}

/// The registrable domain of a URL, when it has one.
fn registrable(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    Some(umi_types::pay_level_domain(host).to_owned())
}

/// What the browser pool has done since it started.
///
/// Doc 05.6's gate asks for per page cost in the metrics, which is
/// [`Counts::mean_render`]. The rest is here because a pool quietly reaping
/// every second tab is a pool with a problem, and a counter is the only way
/// anybody finds out.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Counts {
    /// Renders that produced an outcome, of any kind.
    pub pages: u64,
    /// Time inside [`Renderer::fetch`], summed.
    pub nanos: u64,
    /// Tabs opened, ever.
    pub opened: u64,
    /// Tabs closed because they hit the page or age limit.
    pub recycled: u64,
    /// Tabs closed because they stopped answering. This is the one that should
    /// stay near zero.
    pub reaped: u64,
    /// Requests let through.
    pub allowed: u64,
    /// Requests refused.
    pub blocked: u64,
    /// Subresource bytes that arrived, as Chromium counted them on the wire.
    pub bytes: u64,
}

impl Counts {
    /// Mean wall time per rendered page, which is doc 05.6's number.
    #[must_use]
    pub fn mean_render(&self) -> Duration {
        if self.pages == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.nanos / self.pages)
    }

    /// Doc 05.9's `browser_pool_capacity`, in pages a second.
    ///
    /// Tabs over mean render time, which is the formula in the spec. It runs a
    /// little high, because the mean is the render alone and a page also
    /// spends time waiting for a tab: the T3 bench measures 1.8 pages a second
    /// on server2 where this says 2.3. Left as the spec has it rather than
    /// corrected by a fudge factor, because the other half of doc 05.9's `min`
    /// is well under both numbers and the correction would never be the
    /// binding one.
    ///
    /// `tabs` is the pool's cap. Zero tabs or no measurement is zero capacity,
    /// which the budget reads as no rendering.
    #[must_use]
    pub fn capacity(&self, tabs: usize) -> f64 {
        let mean = self.mean_render().as_secs_f64();
        if tabs == 0 || mean <= 0.0 {
            return 0.0;
        }
        tabs as f64 / mean
    }

    /// Mean subresource bytes per rendered page.
    #[must_use]
    pub const fn mean_bytes(&self) -> u64 {
        match self.bytes.checked_div(self.pages) {
            Some(mean) => mean,
            None => 0,
        }
    }
}

/// The live counters behind [`Counts`].
#[derive(Debug, Default)]
struct Stats {
    pages: AtomicU64,
    nanos: AtomicU64,
    opened: AtomicU64,
    recycled: AtomicU64,
    reaped: AtomicU64,
    allowed: AtomicU64,
    blocked: AtomicU64,
    bytes: AtomicU64,
}

impl Stats {
    fn read(&self) -> Counts {
        Counts {
            pages: self.pages.load(Ordering::Relaxed),
            nanos: self.nanos.load(Ordering::Relaxed),
            opened: self.opened.load(Ordering::Relaxed),
            recycled: self.recycled.load(Ordering::Relaxed),
            reaped: self.reaped.load(Ordering::Relaxed),
            allowed: self.allowed.load(Ordering::Relaxed),
            blocked: self.blocked.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

/// Bump one counter.
fn bump(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

/// One tab, with the two numbers that decide when it gets thrown away.
struct Lease {
    tab: Tab,
    domain: String,
    served: u32,
    born: Instant,
}

/// A render that ended because the tab did.
struct Dead {
    reason: String,
    failure: Failure,
}

impl Dead {
    /// The tab stopped answering, which is our problem and not the origin's.
    ///
    /// [`Failure::Connect`] is the class on purpose. It is the one doc 09
    /// answers by trying the URL again, and the alternatives are all worse: a
    /// block would back the host off for something the host did not do, and
    /// there is nothing above T3 to escalate to that is not a person.
    fn tab(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            failure: Failure::Connect,
        }
    }
}

impl std::fmt::Display for Dead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

/// T3, the rendered tier from doc 05.6.
///
/// One browser process, one incognito context per registrable domain, and a
/// pool of tabs under a hard cap. Cheap to clone, and every clone shares the
/// one browser: two `Renderer`s would be two Chromium processes and twice the
/// memory plan.
#[derive(Clone)]
pub struct Renderer {
    inner: Arc<Pool>,
}

// Hand written rather than derived, because the useful thing to print is what
// the pool is doing and not the CDP connection behind it.
impl std::fmt::Debug for Renderer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Renderer")
            .field("tabs", &self.inner.render.tabs)
            .field("parked", &self.parked())
            .field("counts", &self.counts())
            .finish()
    }
}

/// The browser, its tabs, and everything shared between renders.
struct Pool {
    browser: Browser,
    /// The task that polls the CDP connection. Nothing makes progress if it
    /// stops, so it is held rather than detached, and aborted on shutdown.
    driver: JoinHandle<()>,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<Lease>>,
    contexts: Mutex<HashMap<String, String>>,
    render: RenderConfig,
    fetch: FetchConfig,
    signer: Option<Arc<Signer>>,
    stats: Stats,
    profile: PathBuf,
}

impl Renderer {
    /// Start a browser and take the pool.
    ///
    /// This spawns a Chromium process, so it is the one constructor in this
    /// crate that costs something real. A fetcher calls it once at startup and
    /// clones the result.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when there is no browser to run, when the tab cap
    /// is zero, or when Chromium starts and then will not talk. The message
    /// names [`RenderConfig::sandbox`] for the root case, because that failure
    /// otherwise looks like a launch timeout and costs somebody an afternoon.
    pub async fn launch(
        fetch: FetchConfig,
        render: RenderConfig,
        signer: Option<Arc<Signer>>,
    ) -> Result<Self> {
        if render.tabs == 0 {
            return Err(FetchError::Client(
                "the render tab cap is zero, so this box has no T3 and needs no browser".to_owned(),
            ));
        }

        // One profile directory per process rather than the crate's shared
        // default, so that two umi processes on one box do not fight over the
        // same lock file. Chromium takes an exclusive lock and the second
        // process simply fails.
        let profile = std::env::temp_dir().join(format!("umi-render-{}", std::process::id()));

        let mut config = BrowserConfig::builder()
            .new_headless_mode()
            .enable_request_intercept()
            // A crawler that serves itself from its own cache is measuring its
            // own cache. Every request goes to the network or is refused.
            .disable_cache()
            .user_data_dir(&profile)
            .request_timeout(fetch.total_timeout)
            // Doc 07.1, at this rung as at every other one. This is the header
            // a site operator reads and it is ours.
            .arg(format!("--user-agent={USER_AGENT}"));
        if let Some(path) = &render.executable {
            config = config.chrome_executable(path);
        }
        if !render.sandbox {
            config = config.no_sandbox();
        }
        let config = config.build().map_err(FetchError::Client)?;

        let (browser, mut handler) = Browser::launch(config).await.map_err(|e| {
            FetchError::Client(format!(
                "{e}: chromium will not start as root with its sandbox on, and the fix for \
                 that is to run the fetcher as an ordinary user rather than to turn the \
                 sandbox off"
            ))
        })?;

        // The handler is the connection. Nothing else in the process makes
        // progress unless it is polled, so it gets a task of its own, and the
        // task ends when the websocket does.
        let driver = tokio::spawn(async move { while handler.next().await.is_some() {} });

        Ok(Self {
            inner: Arc::new(Pool {
                browser,
                driver,
                permits: Arc::new(Semaphore::new(render.tabs)),
                idle: Mutex::new(Vec::new()),
                contexts: Mutex::new(HashMap::new()),
                render,
                fetch,
                signer,
                stats: Stats::default(),
                profile,
            }),
        })
    }

    /// The counters, doc 05.6's per page cost among them.
    #[must_use]
    pub fn counts(&self) -> Counts {
        self.inner.stats.read()
    }

    /// How many tabs are parked and not in use.
    #[must_use]
    pub fn parked(&self) -> usize {
        self.inner
            .idle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// The render knobs this pool was built with.
    #[must_use]
    pub fn config(&self) -> &RenderConfig {
        &self.inner.render
    }

    /// Render one URL.
    ///
    /// Pass a [`Revalidator`] to make the document request conditional, which
    /// works here the way it does at T1: the headers go on the top frame
    /// request and a 304 comes back as [`Outcome::NotModified`] with no render.
    ///
    /// # Errors
    ///
    /// [`FetchError::Url`] when the URL does not parse or is not http(s), and
    /// [`FetchError::Client`] when the browser will not give us a tab. Anything
    /// that goes wrong once the navigation has started is an [`Outcome`].
    pub async fn fetch(&self, url: &str, revalidate: Option<&Revalidator>) -> Result<Outcome> {
        self.inner.fetch(url, revalidate).await
    }

    /// How many pages a second this pool can render, doc 05.9.
    ///
    /// Measured once anything has been rendered and estimated before that, and
    /// the estimate is deliberately the pessimistic one: a budget that starts
    /// too high sends work to a browser that cannot take it, and the pages
    /// that get deferred as a result are the ones the crawl most wanted.
    #[must_use]
    pub fn rate(&self) -> f64 {
        let counts = self.counts();
        let tabs = self.inner.render.tabs;
        if counts.pages == 0 {
            let seed = self.inner.render.quiet + LOAD;
            return tabs as f64 / seed.as_secs_f64();
        }
        counts.capacity(tabs)
    }

    /// Close the browser and take its profile directory with it.
    ///
    /// Dropping a `Renderer` kills Chromium too, because the child is spawned
    /// with `kill_on_drop`, but it leaves the profile behind and leaves the
    /// process to be reaped whenever the runtime gets to it. A fetcher shutting
    /// down should call this.
    pub async fn shutdown(self) {
        let Some(pool) = Arc::into_inner(self.inner) else {
            return;
        };
        pool.stop().await;
    }
}

impl Pool {
    /// Render one URL, from the URL check to the outcome.
    async fn fetch(&self, url: &str, revalidate: Option<&Revalidator>) -> Result<Outcome> {
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
        let domain = umi_types::pay_level_domain(&host).to_owned();

        // The cap, held for the whole render. Doc 05.6's eight tabs is a memory
        // number, so what it has to bound is tabs that exist rather than
        // renders that have started.
        let _permit: OwnedSemaphorePermit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| FetchError::Client("the render pool is closed".to_owned()))?;

        let mut lease = self.lease(&domain).await?;
        let started = Instant::now();
        let result = self
            .render(&lease.tab, &parsed, &domain, revalidate, started)
            .await;
        let elapsed = started.elapsed();

        bump(&self.stats.pages, 1);
        bump(
            &self.stats.nanos,
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
        );

        match result {
            Ok(outcome) => {
                lease.served += 1;
                self.park(lease).await;
                Ok(outcome)
            }
            Err(dead) => {
                tracing::warn!(url = %parsed, reason = %dead, "reaping a rendered tab");
                bump(&self.stats.reaped, 1);
                self.discard(lease.tab).await;
                Ok(Outcome::Failed {
                    status: None,
                    failure: dead.failure,
                    retry_after: None,
                })
            }
        }
    }

    /// A tab for this domain, parked or new.
    async fn lease(&self, domain: &str) -> Result<Lease> {
        let parked = {
            let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
            idle.iter()
                .position(|lease| lease.domain == domain)
                .map(|at| idle.swap_remove(at))
        };
        if let Some(lease) = parked {
            return Ok(lease);
        }

        let context = self.context(domain).await?;
        let target = CreateTargetParams::builder()
            .url("about:blank")
            .browser_context_id(context)
            .build()
            .map_err(FetchError::Client)?;
        let tab = self
            .browser
            .new_page(target)
            .await
            .map_err(|e| FetchError::Client(format!("could not open a tab: {e}")))?;
        bump(&self.stats.opened, 1);
        Ok(Lease {
            tab,
            domain: domain.to_owned(),
            served: 0,
            born: Instant::now(),
        })
    }

    /// The incognito context for a domain, doc 05.6's one per PLD.
    ///
    /// One context per registrable domain keeps a cookie, a service worker or a
    /// cache entry from one site out of another site's render. That is not a
    /// privacy measure for us, it is a correctness one: two sites sharing
    /// storage means the second one gets a page the first one shaped.
    async fn context(&self, domain: &str) -> Result<String> {
        if let Some(id) = self
            .contexts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(domain)
        {
            return Ok(id.clone());
        }
        let created = self
            .browser
            .create_browser_context(CreateBrowserContextParams::default())
            .await
            .map_err(|e| FetchError::Client(format!("could not open a browser context: {e}")))?;
        let id = created.inner().clone();
        self.contexts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(domain.to_owned(), id.clone());
        Ok(id)
    }

    /// Park a tab for reuse, or close it if it has had enough.
    async fn park(&self, lease: Lease) {
        if lease.served >= self.render.pages_per_tab
            || lease.born.elapsed() >= self.render.tab_lifetime
        {
            bump(&self.stats.recycled, 1);
            self.discard(lease.tab).await;
            return;
        }
        // Blank it before parking. A loaded page keeps its timers, its web
        // sockets and its intervals running, and a parked tab that is still
        // working is exactly the leak the recycling rule is about.
        //
        // The budget is the connect timeout rather than something small and
        // invented. A blank navigation is nothing but bookkeeping, so the only
        // reason it takes real time is that eight tabs are sharing six cores,
        // and a tight deadline there throws away healthy tabs by the dozen: at
        // two seconds this reaped a third of them on server2 and the pool spent
        // its afternoon opening replacements. A tab that cannot blank itself
        // inside a TCP connect budget is genuinely sick.
        let blanked =
            tokio::time::timeout(self.fetch.connect_timeout, lease.tab.goto("about:blank"))
                .await
                .is_ok_and(|result| result.is_ok());
        if !blanked {
            bump(&self.stats.reaped, 1);
            self.discard(lease.tab).await;
            return;
        }
        self.idle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(lease);
    }

    /// Close one tab, giving up rather than blocking if it will not go.
    ///
    /// A hung tab is the case this exists for, so the close is not allowed to
    /// hang in turn. Chromium reclaims the renderer when the browser exits
    /// either way, and the tab permit is released by the caller regardless.
    async fn discard(&self, tab: Tab) {
        let _ = tokio::time::timeout(self.fetch.connect_timeout, tab.close()).await;
    }

    /// Shut the browser down and remove the profile directory.
    async fn stop(self) {
        let mut browser = self.browser;
        let _ = tokio::time::timeout(Duration::from_secs(10), browser.close()).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), browser.wait()).await;
        self.driver.abort();
        let _ = std::fs::remove_dir_all(&self.profile);
    }
}

/// Everything one render learned, before it is turned into an [`Outcome`].
#[derive(Default)]
struct Session {
    /// The top frame's document response, which is where the status, the
    /// headers and the final URL come from.
    document: Option<Response>,
    /// The network id of the top frame document request, so that its own bytes
    /// are kept out of the subresource budget and its own failure is told apart
    /// from a subresource's.
    request: Option<RequestId>,
    /// Same domain hops, in order, exactly as doc 04.7 wants them.
    redirects: Vec<Hop>,
    /// A redirect that left the registrable domain, which ends the render.
    off_domain: Option<String>,
    /// The redirect chain went past [`FetchConfig::max_redirects`].
    too_many: bool,
    /// The render stopped because it ran out of [`RenderConfig::ceiling`].
    ceiling: bool,
    /// The net error Chromium reported for the document request.
    net_error: Option<String>,
    /// Subresource bytes spent so far.
    bytes: u64,
}

impl Session {
    /// Whether the off domain redirect is fully known, so the loop can stop.
    fn settled(&self) -> bool {
        let Some(target) = &self.off_domain else {
            return false;
        };
        self.redirects.last().is_some_and(|hop| &hop.to == target)
    }
}

impl Pool {
    /// Navigate, filter, wait for quiet, and serialise the DOM.
    async fn render(
        &self,
        tab: &Tab,
        url: &Url,
        domain: &str,
        revalidate: Option<&Revalidator>,
        started: Instant,
    ) -> std::result::Result<Outcome, Dead> {
        let main = tab
            .mainframe()
            .await
            .map_err(|e| Dead::tab(format!("asking for the main frame: {e}")))?
            .ok_or_else(|| Dead::tab("the tab has no main frame"))?;

        let mut paused = listen::<EventRequestPaused>(tab).await?;
        let mut sent = listen::<EventRequestWillBeSent>(tab).await?;
        let mut received = listen::<EventResponseReceived>(tab).await?;
        let mut finished = listen::<EventLoadingFinished>(tab).await?;
        let mut failed = listen::<EventLoadingFailed>(tab).await?;

        let conditional = conditional(revalidate);
        let mut session = Session::default();
        let mut inflight: HashSet<RequestId> = HashSet::new();
        let mut navigating = true;
        let deadline = tokio::time::Instant::now() + self.render.ceiling;
        let mut last = tokio::time::Instant::now();
        let mut nav = Box::pin(tab.goto(url.as_str()));

        loop {
            let quiet_at = last + self.render.quiet;
            tokio::select! {
                // Biased, and the order is the point. The network domain is
                // drained before the interception arm, so that a redirect's
                // status has arrived by the time we have to decide about the
                // URL it points at.
                biased;

                Some(event) = sent.next() => {
                    last = tokio::time::Instant::now();
                    inflight.insert(event.request_id.clone());
                    if event.frame_id.as_ref() == Some(&main)
                        && event.r#type == Some(ResourceType::Document)
                    {
                        if session.request.is_none() {
                            session.request = Some(event.request_id.clone());
                        }
                        if let Some(previous) = &event.redirect_response {
                            session.redirects.push(Hop {
                                from: previous.url.clone(),
                                to: event.request.url.clone(),
                                status: status_of(previous.status),
                            });
                        }
                    }
                    if session.settled() {
                        break;
                    }
                }

                Some(event) = finished.next() => {
                    last = tokio::time::Instant::now();
                    inflight.remove(&event.request_id);
                    // The document's own bytes are the page, not a subresource,
                    // and doc 05.6's cap is on the subresources.
                    if session.request.as_ref() != Some(&event.request_id) {
                        session.bytes = session.bytes.saturating_add(bytes_of(
                            event.encoded_data_length,
                        ));
                    }
                }

                Some(event) = failed.next() => {
                    last = tokio::time::Instant::now();
                    inflight.remove(&event.request_id);
                    if session.request.as_ref() == Some(&event.request_id) {
                        session.net_error = Some(event.error_text.clone());
                    }
                }

                Some(event) = received.next() => {
                    last = tokio::time::Instant::now();
                    if event.frame_id.as_ref() == Some(&main)
                        && event.r#type == ResourceType::Document
                    {
                        session.document = Some(event.response.clone());
                    }
                }

                Some(event) = paused.next() => {
                    last = tokio::time::Instant::now();
                    self.intercept(tab, &event, domain, &main, &conditional, &mut session)
                        .await?;
                    if session.too_many || session.settled() {
                        break;
                    }
                }

                result = &mut nav, if navigating => {
                    navigating = false;
                    last = tokio::time::Instant::now();
                    // An abort here is usually our own: refusing the top frame
                    // navigation is how doc 04.7's rule is enforced in a
                    // browser. The real reason is already in `net_error`.
                    if let Err(error) = result
                        && session.net_error.is_none()
                    {
                        session.net_error = Some(error.to_string());
                    }
                }

                () = tokio::time::sleep_until(deadline) => {
                    session.ceiling = true;
                    break;
                }

                () = tokio::time::sleep_until(quiet_at), if !navigating && inflight.is_empty() => {
                    break;
                }
            }
        }

        bump(&self.stats.bytes, session.bytes);
        self.finish(tab, session, started).await
    }

    /// Decide about one paused request and answer Chromium.
    async fn intercept(
        &self,
        tab: &Tab,
        event: &EventRequestPaused,
        domain: &str,
        main: &FrameId,
        conditional: &[(http::HeaderName, String)],
        session: &mut Session,
    ) -> std::result::Result<(), Dead> {
        let top = event.resource_type == ResourceType::Document && event.frame_id == *main;

        if top && registrable(&event.request.url).as_deref() != Some(domain) {
            // Doc 04.7. A fetcher never follows a redirect off the registrable
            // domain: it stops and the coordinator admits the target as a fresh
            // candidate, so that robots is checked for the new host. A script
            // that navigates the top frame away is not a redirect and does not
            // end the render, it is simply refused.
            if event.redirected_request_id.is_some() {
                session.off_domain = Some(event.request.url.clone());
            }
            return self.refuse(tab, event, Reason::OffDomain).await;
        }
        if top
            && !session.redirects.is_empty()
            && session.redirects.len() > self.fetch.max_redirects
        {
            session.too_many = true;
            return self.refuse(tab, event, Reason::Redirects).await;
        }

        if let Decision::Block(reason) = decide(
            &event.resource_type,
            &event.request.url,
            domain,
            session.bytes,
            self.render.subresource_cap,
        ) {
            return self.refuse(tab, event, reason).await;
        }

        let mut head = entries(&event.request.headers);
        if top {
            for (name, value) in conditional {
                set(&mut head, name.as_str(), value);
            }
        }
        // Signed here rather than through `Network.setExtraHTTPHeaders`,
        // because doc 07.2's signature covers `@authority`, `@method` and
        // `@path`. One header set for every request the page makes would be a
        // signature for the document sent on each of its subresources, which is
        // a signature that does not verify.
        if let Some(signer) = &self.signer
            && let Ok(target) = Url::parse(&event.request.url)
            && let Ok(signed) = signer.sign(&event.request.method, &target)
        {
            for (name, value) in signed.headers() {
                set(&mut head, name, value);
            }
        }

        let params = ContinueRequestParams::builder()
            .request_id(event.request_id.clone())
            .headers(head)
            .build()
            .map_err(Dead::tab)?;
        tab.execute(params)
            .await
            .map_err(|e| Dead::tab(format!("continuing a request: {e}")))?;
        bump(&self.stats.allowed, 1);
        Ok(())
    }

    /// Fail one paused request, and count it.
    async fn refuse(
        &self,
        tab: &Tab,
        event: &EventRequestPaused,
        reason: Reason,
    ) -> std::result::Result<(), Dead> {
        bump(&self.stats.blocked, 1);
        tracing::trace!(url = %event.request.url, reason = reason.as_str(), "refusing a request");
        // `BlockedByClient` rather than `Aborted`, because that is what it is:
        // the client refused it, and a page that checks why its beacon failed
        // should get a true answer.
        let params = FailRequestParams::new(event.request_id.clone(), ErrorReason::BlockedByClient);
        tab.execute(params)
            .await
            .map_err(|e| Dead::tab(format!("failing a request: {e}")))?;
        Ok(())
    }

    /// Turn a finished session into the outcome the scheduler acts on.
    ///
    /// Every rule from here down is `engine`'s rule, called rather than copied.
    async fn finish(
        &self,
        tab: &Tab,
        session: Session,
        started: Instant,
    ) -> std::result::Result<Outcome, Dead> {
        let Session {
            document,
            redirects,
            off_domain,
            too_many,
            ceiling,
            net_error,
            ..
        } = session;

        if let Some(target) = off_domain {
            let mut redirects = redirects;
            // The hop that pointed off domain was not followed, so it is not
            // one of the hops. 302 is the fallback for the case where Chromium
            // told us the request came from a redirect before it told us which
            // status carried it, and it is the status the overwhelming majority
            // of them are.
            let status = if redirects.last().is_some_and(|hop| hop.to == target) {
                redirects.pop().map_or(302, |hop| hop.status)
            } else {
                302
            };
            return Ok(Outcome::RedirectedOffDomain {
                redirects,
                target,
                status,
            });
        }
        if too_many {
            return Ok(Outcome::Failed {
                status: None,
                failure: Failure::Malformed,
                retry_after: None,
            });
        }

        let Some(response) = document else {
            let failure = match (net_error.as_deref(), ceiling) {
                (Some(text), _) => net_failure(text),
                (None, true) => Failure::Timeout(Stage::Total),
                (None, false) => Failure::Connect,
            };
            return Ok(Outcome::Failed {
                status: None,
                failure,
                retry_after: None,
            });
        };

        let head = header_map(&response.headers);
        let retry_after = headers::retry_after(&head);
        let status = status_of(response.status);

        if status == 304 {
            return Ok(Outcome::NotModified {
                revalidate: revalidator(&head),
                headers_kept: headers::kept(&head),
                headers_digest: headers::digest(&head),
                elapsed: started.elapsed(),
            });
        }

        let verdict = classify(status, &head);
        match verdict {
            Verdict::Page | Verdict::Suspect(_) => {}
            Verdict::Gone => return Ok(Outcome::Gone),
            Verdict::Failed(failure) => {
                return Ok(Outcome::Failed {
                    status: Some(status),
                    failure,
                    retry_after,
                });
            }
        }

        // The one thing a browser can hand back that is not the document. A
        // PDF, an image or a plain text file opens in a viewer, and the
        // viewer's own markup is what `outerHTML` would return. Putting that in
        // the corpus as the page would be worse than not having the page, and
        // the URL belongs at T1 anyway, where the bytes come back as bytes.
        //
        // `NotDocument` rather than `Malformed` so the ladder can act on that
        // last sentence. Nothing about the response was wrong, the rung was.
        if !markup(&response.mime_type) {
            return Ok(Outcome::Failed {
                status: Some(status),
                failure: Failure::NotDocument,
                retry_after,
            });
        }

        let text = tab
            .content()
            .await
            .map_err(|e| Dead::tab(format!("serialising the dom: {e}")))?;
        let body = Bytes::from(text.into_bytes());
        if body.len() > self.fetch.body_cap {
            return Ok(Outcome::Failed {
                status: Some(status),
                failure: Failure::TooLarge,
                retry_after,
            });
        }

        // Doc 05.8's four way split, with the rendered page in hand rather than
        // the shell. This is the one place where T3 answers the question better
        // than the rungs below it: an interstitial that only appears after its
        // script runs is invisible to T1 and T2.
        if let Verdict::Suspect(fallback) = verdict {
            let failure = if challenge::interstitial(&body).is_some() {
                Failure::Blocked
            } else {
                fallback
            };
            return Ok(Outcome::Failed {
                status: Some(status),
                failure,
                retry_after,
            });
        }

        let content_type = header(&head, "content-type");
        let head_bytes = &body[..body.len().min(sniff::SNIFF_BYTES)];
        Ok(Outcome::Ok(Box::new(Page {
            final_url: response.url.clone(),
            status,
            version: version_of(response.protocol.as_deref()),
            redirects,
            headers_kept: headers::kept(&head),
            headers_digest: headers::digest(&head),
            media: sniff::decide(content_type.as_deref(), head_bytes),
            content_type,
            body_digest: *blake3::hash(&body).as_bytes(),
            body,
            revalidate: revalidator(&head),
            elapsed: started.elapsed(),
        })))
    }
}

/// Subscribe to one event kind on a tab.
async fn listen<T: IntoEventKind + Unpin>(
    tab: &Tab,
) -> std::result::Result<chromiumoxide::listeners::EventStream<T>, Dead> {
    tab.event_listener::<T>().await.map_err(|e| {
        Dead::tab(format!(
            "subscribing to {}: {e}",
            std::any::type_name::<T>()
        ))
    })
}

/// Whether a mime type is something a DOM serialisation actually describes.
///
/// Chromium's own viewers are what this is guarding against, so the list is the
/// markup types and nothing else. An `image/svg+xml` served as a top level
/// document is markup and is included on purpose.
fn markup(mime: &str) -> bool {
    let mime = mime.split(';').next().unwrap_or_default().trim();
    matches!(
        mime.to_ascii_lowercase().as_str(),
        "text/html" | "application/xhtml+xml" | "text/xml" | "application/xml" | "image/svg+xml"
    )
}

/// A CDP status, which is an `i64` because the protocol says so.
fn status_of(status: i64) -> u16 {
    u16::try_from(status).unwrap_or(0)
}

/// A CDP byte count, which is an `f64` because the protocol says so.
fn bytes_of(length: f64) -> u64 {
    if length.is_finite() && length > 0.0 {
        length as u64
    } else {
        0
    }
}

/// The HTTP version behind an ALPN token.
fn version_of(protocol: Option<&str>) -> Version {
    match protocol.unwrap_or_default() {
        "h3" | "http/3" => Version::Http3,
        "h2" | "http/2" | "http/2.0" => Version::Http2,
        "http/1.0" => Version::Http10,
        _ => Version::Http11,
    }
}

/// Chromium's net error names, in the failure classes doc 05.8 acts on.
///
/// The names are stable and public, from `net/base/net_error_list.h`, which
/// makes this a much steadier reading than the source chain walk T1 and T2 need
/// against a client that folds every stage into one message.
fn net_failure(text: &str) -> Failure {
    let text = text.to_ascii_uppercase();
    if text.contains("NAME_NOT_RESOLVED") || text.contains("NAME_RESOLUTION") {
        Failure::Dns
    } else if text.contains("CERT") || text.contains("SSL") {
        Failure::Tls
    } else if text.contains("TIMED_OUT") {
        Failure::Timeout(Stage::Connect)
    } else {
        Failure::Connect
    }
}

/// A CDP header bag as an `http::HeaderMap`.
///
/// Chromium folds repeated headers into one value with newlines between them,
/// which is the one thing that needs undoing: a `HeaderValue` cannot hold a
/// newline, so a folded pair would be dropped whole rather than split into the
/// two headers it came from.
fn header_map(headers: &Headers) -> HeaderMap {
    let mut out = HeaderMap::new();
    let Some(object) = headers.inner().as_object() else {
        return out;
    };
    for (name, value) in object {
        let Ok(name) = http::HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Some(text) = value.as_str() else { continue };
        for line in text.split('\n') {
            if let Ok(value) = http::HeaderValue::from_str(line) {
                out.append(name.clone(), value);
            }
        }
    }
    out
}

/// A CDP header bag as the list `Fetch.continueRequest` takes.
fn entries(headers: &Headers) -> Vec<HeaderEntry> {
    let mut out = Vec::new();
    let Some(object) = headers.inner().as_object() else {
        return out;
    };
    for (name, value) in object {
        if let Some(text) = value.as_str() {
            out.push(HeaderEntry::new(name.clone(), text));
        }
    }
    out
}

/// Set a header on a `continueRequest` list, replacing any that is there.
///
/// `Fetch.continueRequest` replaces the whole header set rather than merging
/// into it, so the list has to be complete and it has to be free of duplicates
/// of the ones we are adding.
fn set(head: &mut Vec<HeaderEntry>, name: &str, value: &str) {
    head.retain(|entry| !entry.name.eq_ignore_ascii_case(name));
    head.push(HeaderEntry::new(name, value));
}

/// One header as a string, dropping values that are not text.
fn header(head: &HeaderMap, name: &str) -> Option<String> {
    head.get(name)?.to_str().ok().map(str::to_owned)
}

#[cfg(test)]
#[path = "rendered_tests.rs"]
mod tests;
