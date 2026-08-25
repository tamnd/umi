//! `umi crawl` and `umi resume`, which is doc 13.5's crawl directory.
//!
//! Everything below this file already exists and is tested on its own: the
//! scope decides what is in, the frontier decides what is next, the fetcher
//! gets bytes, the loop turns them into rows, the sink turns rows into
//! segments and `umi-publish` turns segments into Parquet. This is the file
//! that picks the parts, puts them in a directory and keeps going until a
//! budget says to stop. Nothing here is clever on purpose. When a focused
//! crawl produces a wrong answer the question is always which component was
//! wrong, and that question is much easier to answer when the thing that
//! wired them together did no thinking of its own.
//!
//! # The directory
//!
//! Doc 13.5 says a focused crawl is a directory and the directory is the unit
//! you move around:
//!
//! ```text
//! example.com/
//!   profile.toml       the scope, verbatim
//!   state.sqlite       doc 08's default backend
//!   segments/          sealed .umi, deleted once converted
//!   data/*.parquet     what you keep
//!   manifest.json      doc 12.5's schema, unsigned until published
//!   crawl.log
//! ```
//!
//! `segments/` is the one entry doc 13.5 does not list, and that is because it
//! is not supposed to survive: a segment is converted the moment it seals and
//! the `.umi` is removed once the Parquet is on disk and its digests are
//! known. It exists as a directory rather than a temporary file so that a
//! crash leaves the segment where somebody can look at it.
//!
//! # With `--publish`
//!
//! Two entries change and one appears. `data/` becomes `parquet/`, a staging
//! directory whose files are deleted as soon as the copy on the hub verifies,
//! and `manifest.json` is not written at all, because with `--publish` the
//! manifests that matter are doc 12.5's signed day documents in the published
//! repositories and a second unsigned one in the crawl directory would be a
//! second answer to the same question. What appears is `published.jsonl`, one
//! line per segment saying which repository it went to and under what digest,
//! which is the operator's record of where their crawl ended up.
//!
//! # Deleting things
//!
//! Without `--publish`, only one path in here deletes anything, and it deletes
//! a `.umi` whose rows are already in a Parquet file that has been read back.
//! Doc 12.7's delete after publish rule does not apply, because doc 13.5 is
//! explicit that it does not: nothing was published, and the operator's disk is
//! the operator's business.
//!
//! With `--publish` it does apply, and it is not implemented here either. The
//! deleting is `umi_publish::Publisher`'s, under doc 12.7's four conditions,
//! and this file's only part in it is handing the publisher a state ledger to
//! record the fourth condition in.

use core::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use umi_crawl::{
    Clock, CrawlConfig, Crawler, Scope, SegmentInfo, SegmentSink, SystemClock, TickReport,
};
use umi_fetch::{FetchConfig, Fetcher};
use umi_file::{StreamKind, WriterConfig};
use umi_publish::manifest::{FileEntry, Manifest, Verification};
use umi_publish::repo::Corpus;
use umi_publish::{Hub, PublishConfig, Published, Publisher, Role, SigningKey};
use umi_state::{Candidate, SegmentRow, State, Stream};
use umi_state_sqlite::SqliteState;
use umi_types::{Digest, FetcherId, Tier};

use crate::Error;

#[cfg(test)]
#[path = "crawl_tests.rs"]
mod tests;

/// The file names doc 13.5 fixes.
const PROFILE: &str = "profile.toml";
const STATE: &str = "state.sqlite";
const SEGMENTS: &str = "segments";
const DATA: &str = "data";
const MANIFEST: &str = "manifest.json";
const LOG: &str = "crawl.log";

/// Where `--publish` stages Parquet on its way to the hub, and the record it
/// leaves behind. Neither is in doc 13.5, because doc 13.5 describes a crawl
/// that keeps its output.
const STAGING: &str = "parquet";
const PUBLISHED: &str = "published.jsonl";

/// How long to wait before asking an idle frontier again.
///
/// Doc 07.6 starts every host at a one second delay, so a frontier that is
/// idle is usually a frontier where every host with work is inside its
/// politeness window, and the shortest useful wait is about that long. Asking
/// faster is a spin loop that produces no fetches.
const IDLE_WAIT: Duration = Duration::from_millis(1000);

/// How long a frontier may hold work it never hands out before we stop.
///
/// This exists because "there are pending urls" and "one of them will ever be
/// leased" are not the same statement. A host whose robots.txt keeps failing,
/// or a lease left behind by a killed process, leaves rows in `pending` that no
/// tick can pick up, and without a limit the loop waits on them forever. Five
/// minutes is comfortably longer than the longest `Crawl-delay` worth honouring
/// and short enough that a stuck crawl is a thing the operator sees rather than
/// a thing they find in the morning. Under `--watch` there is no limit, because
/// waiting is what `--watch` was asked to do.
const STALL_LIMIT: Duration = Duration::from_secs(300);

/// What the operator asked for, after doc 14.7's precedence has run.
///
/// One struct rather than fifteen arguments, and it is deliberately the shape
/// of doc 14.3's flag list rather than the shape of [`CrawlConfig`]: the
/// translation from one to the other is the part worth reading, so it happens
/// in one visible place, in `settings`, instead of being spread over the parser.
#[derive(Clone, Debug)]
pub struct Options {
    /// A domain, a host, a URL, or a path to a scope profile.
    pub target: String,
    /// Extra include matchers, in doc 13.4's grammar.
    pub include: Vec<String>,
    /// Exclude matchers, likewise.
    pub exclude: Vec<String>,
    /// Hops from a seed.
    pub depth: Option<u8>,
    /// `in-scope`, `record` or `one-hop`.
    pub links: String,
    /// Stop after this many pages.
    pub max_pages: Option<u64>,
    /// Stop after this long, in doc 13.4's spelling.
    pub max_duration: Option<String>,
    /// Do not stop when the frontier drains.
    pub watch: bool,
    /// Requests per second per host, clamped by doc 07.6 and never raised.
    pub rps: f32,
    /// Simultaneous fetches in flight.
    pub concurrency: u16,
    /// Highest tier allowed.
    pub tier_max: u8,
    /// A file of URLs, or `-` for standard input.
    pub seed: Option<String>,
    /// Any program that prints URLs, repeatable.
    pub seeder: Vec<String>,
    /// Where the directory goes. Defaults to `./<scope name>`.
    pub out: Option<String>,
    /// Doc 12's pipeline, or nothing when `--publish` was not given.
    pub publish: Option<Publishing>,
}

/// What `--publish` needs, after doc 14.7's five layers have run.
///
/// The two secrets are resolved by the time they get here rather than kept as
/// indirections, because the crawl loop should not be the thing that discovers
/// halfway through that `$HF_TOKEN` is unset. They are also the reason this is
/// a separate struct with a hand written [`fmt::Debug`]: [`Options`] derives
/// `Debug` and is the sort of thing that ends up in a log line.
#[derive(Clone)]
pub struct Publishing {
    /// The Hugging Face organisation, `open-index` unless configured.
    pub org: String,
    /// The write token.
    pub token: String,
    /// Doc 12.5's publishing key, 64 hex characters.
    pub key: String,
    /// Doc 12.4's `NN`, the slice inside the week's repository family.
    pub slice: u16,
}

impl Publishing {
    /// Resolve the two secrets `--publish` needs, or nothing without the flag.
    ///
    /// Both are read here, before a single page is fetched, rather than when
    /// the first segment seals. A crawl that ran for twenty minutes and then
    /// found out that `$HF_TOKEN` was not set would have twenty minutes of
    /// segments sitting on the disk that `--publish` is what keeps empty.
    ///
    /// # Errors
    ///
    /// [`Error::Missing`] when the flag was given and the configuration does
    /// not say where the token or the key comes from, and [`Error::Config`]
    /// when it says and the answer is not there.
    pub fn resolve(config: &crate::config::Config, wanted: bool) -> Result<Option<Self>, Error> {
        if !wanted {
            return Ok(None);
        }
        let missing = |what: &str, var: &str| {
            Error::Missing(format!(
                "--publish needs publish.{what}: set it in umi.toml as env:NAME or file:/path, \
                 or point ${var} at one of those"
            ))
        };
        let token = config
            .token
            .as_ref()
            .ok_or_else(|| missing("token", "UMI_TOKEN"))?;
        let key = config
            .key
            .as_ref()
            .ok_or_else(|| missing("key", "UMI_PUBLISH_KEY"))?;
        Ok(Some(Self {
            org: config.org.value.clone(),
            token: token.value.read()?,
            key: key.value.read()?,
            // Doc 12.4 allocates slices on demand as a repository approaches
            // the 300 GB ceiling, and that allocation needs the byte counts in
            // `umi-meta`, which nothing reads yet. Zero until it does, which is
            // correct for every crawl small enough to fit in one slice and is
            // the first thing to fix when one is not.
            slice: 0,
        }))
    }
}

impl fmt::Debug for Publishing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Publishing")
            .field("org", &self.org)
            .field("token", &"<redacted>")
            .field("key", &"<redacted>")
            .field("slice", &self.slice)
            .finish()
    }
}

/// What a crawl did, which is what the caller prints and what a test asserts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    /// Rows stored.
    pub rows: u64,
    /// Fetches that produced a body.
    pub fetched: u64,
    /// Body bytes as they arrived.
    pub bytes_fetched: u64,
    /// Answers that were a failure of some kind.
    pub failed: u64,
    /// Parquet files produced, whether they were kept or published.
    pub files: usize,
    /// How many of those went to the hub. Zero without `--publish`.
    pub published: usize,
    /// Bytes those files take.
    pub bytes_stored: u64,
    /// Why the loop stopped.
    pub stopped: Stop,
}

/// Why a crawl ended, which decides the exit code.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Stop {
    /// The frontier drained and `--watch` was not given. Doc 14.9's exit 0.
    #[default]
    Idle,
    /// A budget in doc 13.2 was reached. Doc 14.9's exit 4.
    Budget,
}

/// Start a new crawl in a fresh or existing directory.
///
/// # Errors
///
/// [`Error::Config`] if the target is not a scope anything can be made of,
/// [`Error::Io`] for the directory and the log, and whatever the state backend,
/// the fetcher or the converter reports.
pub fn crawl(options: &Options) -> Result<Summary, Error> {
    let scope = scope_for(options)?;
    let dir = PathBuf::from(
        options
            .out
            .clone()
            .unwrap_or_else(|| default_out(&scope.name)),
    );
    let layout = Layout::create(&dir)?;

    // The profile is written before anything is fetched, so that a crawl
    // killed in its first second is still a directory `umi resume`
    // understands. Written once and never rewritten: doc 13.5 says the
    // profile is the scope verbatim, and a resume that silently rewrote it
    // with today's flags would be a crawl whose own record of what it was
    // doing changed under it.
    if !layout.profile.exists() {
        std::fs::write(&layout.profile, profile_toml(options, &scope)).map_err(Error::Io)?;
    }

    run(&layout, &scope, &settings(options)?, options)
}

/// Continue the crawl in `dir` from wherever it stopped.
///
/// # Errors
///
/// As [`crawl`], plus [`Error::Io`] if the directory has no `profile.toml`,
/// which is what tells a crawl directory apart from any other directory.
pub fn resume(dir: &Path, watch: bool, publish: Option<Publishing>) -> Result<Summary, Error> {
    let layout = Layout::create(dir)?;
    let text = std::fs::read_to_string(&layout.profile).map_err(|cause| {
        Error::Io(std::io::Error::new(
            cause.kind(),
            format!("{} is not a crawl directory: {cause}", dir.display()),
        ))
    })?;
    let scope = Scope::from_toml(&text).map_err(|e| Error::Scope(e.to_string()))?;

    // No seeding and no target. Doc 13.5's promise is that the directory is
    // the unit, so everything a resume needs is in it, and a resume that
    // reseeded would put the seeds back in the frontier on every restart.
    // Publishing comes from the flags and the configuration rather than from
    // the profile, because the profile is checked into somebody's repository
    // and the two things `--publish` needs are secrets.
    let options = Options {
        target: scope.name.clone(),
        watch,
        seed: None,
        seeder: Vec::new(),
        publish,
        ..Options::default()
    };
    let mut config = settings(&options)?;
    config.watch = watch;
    run(&layout, &scope, &config, &options)
}

/// The parts of [`Options`] the loop needs after the scope has been built.
struct Settings {
    config: CrawlConfig,
    watch: bool,
    max_pages: Option<u64>,
    max_bytes: Option<u64>,
    max_duration: Option<Duration>,
    delay_ms: u32,
}

/// Doc 13.5's directory, as paths.
struct Layout {
    dir: PathBuf,
    profile: PathBuf,
    state: PathBuf,
    segments: PathBuf,
    data: PathBuf,
    staging: PathBuf,
    manifest: PathBuf,
    published: PathBuf,
    log: PathBuf,
}

impl Layout {
    fn create(dir: &Path) -> Result<Self, Error> {
        let dir = dir.to_path_buf();
        std::fs::create_dir_all(dir.join(SEGMENTS)).map_err(Error::Io)?;
        std::fs::create_dir_all(dir.join(DATA)).map_err(Error::Io)?;
        Ok(Self {
            profile: dir.join(PROFILE),
            state: dir.join(STATE),
            segments: dir.join(SEGMENTS),
            data: dir.join(DATA),
            staging: dir.join(STAGING),
            manifest: dir.join(MANIFEST),
            published: dir.join(PUBLISHED),
            log: dir.join(LOG),
            dir,
        })
    }
}

/// Build the scope doc 13.2 describes from the target and the extra matchers.
fn scope_for(options: &Options) -> Result<Scope, Error> {
    let mut scope = if options.target.ends_with(".toml") {
        let text = std::fs::read_to_string(&options.target).map_err(Error::Io)?;
        Scope::from_toml(&text).map_err(|e| Error::Scope(e.to_string()))?
    } else {
        Scope::for_target(&options.target).map_err(|e| Error::Scope(e.to_string()))?
    };
    scope
        .add_include(&options.include)
        .map_err(|e| Error::Scope(e.to_string()))?;
    scope
        .add_exclude(&options.exclude)
        .map_err(|e| Error::Scope(e.to_string()))?;
    if let Some(depth) = options.depth {
        scope.max_depth = Some(depth);
    }
    scope.link_policy = match options.links.as_str() {
        "record" => umi_crawl::LinkPolicy::RecordOutOfScope,
        "one-hop" => umi_crawl::LinkPolicy::OneHop,
        _ => umi_crawl::LinkPolicy::InScopeOnly,
    };
    scope.budget.max_pages = options.max_pages.or(scope.budget.max_pages);
    if let Some(text) = &options.max_duration {
        scope.budget.max_duration =
            Some(umi_crawl::scope::parse_duration(text).map_err(|e| Error::Scope(e.to_string()))?);
    }
    scope.rate.max_rps_per_host = options.rps;
    Ok(scope)
}

fn settings(options: &Options) -> Result<Settings, Error> {
    let max_duration = match &options.max_duration {
        Some(text) => {
            Some(umi_crawl::scope::parse_duration(text).map_err(|e| Error::Scope(e.to_string()))?)
        }
        None => None,
    };
    Ok(Settings {
        config: CrawlConfig {
            fetcher: FetcherId::LOCAL,
            in_flight: usize::from(options.concurrency.max(1)),
            max_tier: tier(options.tier_max),
            ..CrawlConfig::default()
        },
        watch: options.watch,
        max_pages: options.max_pages,
        max_bytes: None,
        max_duration,
        // Doc 13.3 caps a focused crawl at 2 requests a second per host and a
        // rate override can only lower, never raise, so the delay this turns
        // into is never shorter than 500 ms whatever the operator typed.
        delay_ms: delay_ms(options.rps),
    })
}

/// Requests per second, as the delay doc 07.6's scheduler actually uses.
fn delay_ms(rps: f32) -> u32 {
    let clamped = rps.clamp(0.01, umi_crawl::RateOverride::MAX_RPS_PER_HOST);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped above, so the quotient is between 500 and 100000"
    )]
    let ms = (1000.0 / clamped) as u32;
    ms
}

fn tier(max: u8) -> Tier {
    match max {
        0 => Tier::Revalidate,
        1 => Tier::Plain,
        2 => Tier::Emulated,
        3 => Tier::Rendered,
        // Doc 05.6 says tier 4 is allowlisted and opted into, never reached by
        // typing a bigger number, so `--tier 9` means the highest tier a crawl
        // can ask for on its own rather than the highest one that exists.
        _ => Tier::Rendered,
    }
}

/// `./example.com`, which is doc 14.3's default output directory.
fn default_out(name: &str) -> String {
    // A scope name comes from a host or a profile and both can hold a slash,
    // so this is not decoration: a name that reached `PathBuf::from` with a
    // slash in it would put the crawl somewhere the operator did not ask for.
    let safe: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    // Dots survive the pass above because `example.com` needs them, which
    // leaves exactly two names that mean something to a filesystem rather than
    // naming a directory. Neither can come from a real target, and both would
    // put a crawl somewhere surprising.
    let safe = match safe.as_str() {
        "" | "." | ".." => "crawl",
        other => other,
    };
    format!("./{safe}")
}

/// The scope, written back out in doc 13.4's spelling.
///
/// Written by hand rather than by serialising the [`Scope`], and that is the
/// awkward choice rather than the obvious one. A scope holds compiled regexes
/// and a stamped identifier, so the parsed form and the written form are not
/// the same shape, and a `Serialize` on the parsed form would be a second
/// spelling of the profile that could drift from the one `from_toml` reads.
/// The test that keeps this honest writes a profile and reads it straight
/// back, which is the property doc 13.5 actually promises.
fn profile_toml(options: &Options, scope: &Scope) -> String {
    let mut out = String::new();
    out.push_str("# Written by umi crawl. This is the scope the crawl ran with,\n");
    out.push_str("# and umi resume reads it back rather than the flags.\n");
    out.push_str(&format!("name = {:?}\n\n", scope.name));

    // No table header. Doc 13.4 puts the matchers at the top level next to
    // `name`, and the tables below hold the things that are not the scope.
    // One array and not one line per matcher. Two `include =` lines is a
    // duplicate key, which TOML rejects, so a two matcher profile written the
    // other way would be a file this program cannot read back.
    out.push_str(&format!("include = [{}]\n", matchers_toml(&scope.include)));
    out.push_str(&format!("exclude = [{}]\n", matchers_toml(&scope.exclude)));
    if let Some(depth) = scope.max_depth {
        out.push_str(&format!("max_depth = {depth}\n"));
    }
    out.push_str(&format!(
        "link_policy = {:?}\n\n",
        match scope.link_policy {
            umi_crawl::LinkPolicy::RecordOutOfScope => "record_out_of_scope",
            umi_crawl::LinkPolicy::OneHop => "one_hop",
            // `LinkPolicy` is non exhaustive so that adding a policy is not a
            // breaking change, and doc 14.3's default is the one to fall back
            // to because it is the one that cannot leave the scope.
            _ => "in_scope_only",
        }
    ));

    out.push_str("[budget]\n");
    if let Some(pages) = scope.budget.max_pages {
        out.push_str(&format!("max_pages = {pages}\n"));
    }
    if let Some(bytes) = scope.budget.max_bytes {
        out.push_str(&format!("max_bytes = {bytes}\n"));
    }
    if let Some(duration) = scope.budget.max_duration {
        out.push_str(&format!("max_duration = \"{}s\"\n", duration.as_secs()));
    }
    out.push_str(&format!("stop_when_idle = {}\n\n", !options.watch));

    out.push_str("[rate]\n");
    out.push_str(&format!(
        "max_rps_per_host = {}\n",
        scope.rate.max_rps_per_host
    ));
    out.push_str(&format!("concurrency = {}\n", options.concurrency));
    out
}

/// A matcher list as the inside of a TOML array.
fn matchers_toml(matchers: &[umi_crawl::Matcher]) -> String {
    matchers
        .iter()
        .map(matcher_toml)
        .collect::<Vec<_>>()
        .join(", ")
}

fn matcher_toml(matcher: &umi_crawl::Matcher) -> String {
    match matcher {
        umi_crawl::Matcher::Pld(v) => format!("{{ pld = {v:?} }}"),
        umi_crawl::Matcher::Host(v) => format!("{{ host = {v:?} }}"),
        umi_crawl::Matcher::HostSuffix(v) => format!("{{ host_suffix = {v:?} }}"),
        umi_crawl::Matcher::PathPrefix { host, prefix } => {
            format!("{{ path_prefix = {{ host = {host:?}, prefix = {prefix:?} }} }}")
        }
        umi_crawl::Matcher::UrlRegex(re) => format!("{{ url_regex = {:?} }}", re.as_str()),
        // `Matcher` is non exhaustive so that adding one is not a breaking
        // change, and this arm is what makes that true here. A matcher this
        // build does not know how to write is not one it could have parsed.
        _ => "{ }".to_owned(),
    }
}

/// The loop, which is the part worth reading.
fn run(
    layout: &Layout,
    scope: &Scope,
    settings: &Settings,
    options: &Options,
) -> Result<Summary, Error> {
    let state: Arc<dyn State> =
        Arc::new(SqliteState::open(&layout.state).map_err(|e| Error::State(e.to_string()))?);
    // The T1 client, which is the only tier that exists today. `--tier` is
    // already on `CrawlConfig` and the loop honours it as a ceiling, so a
    // crawl asking for tier 3 gets tier 1 rather than an error, which is the
    // right behaviour when the ladder grows a rung: doc 05 escalates, and a
    // ladder with one rung on it escalates to that rung.
    let mut fetch_config = FetchConfig::default();
    // Doc 05.4's ceiling rather than the 512 KB the one page commands default
    // to. A crawl that truncated every large page would produce rows whose
    // `content_length` and body digest describe a prefix of what the origin
    // sent, which is worse than not having the page.
    fetch_config.body_cap = 8 << 20;
    let fetcher = Fetcher::with_config(fetch_config)?;
    let clock = SystemClock;
    let started_ms = clock.now_ms();

    let scope = Arc::new(scope.clone());
    let sink = SegmentSink::create(
        &layout.segments,
        SegmentInfo {
            stream: StreamKind::Pages,
            coordinator: coordinator_key(&layout.dir),
            crawl_profile: scope.id,
            ..SegmentInfo::default()
        },
        WriterConfig::default(),
    )
    .map_err(Error::Io)?;

    let crawler = Crawler::new(
        fetcher,
        Arc::clone(&state),
        clock,
        CrawlConfig {
            scope: Arc::clone(&scope),
            ..settings.config.clone()
        },
    );

    // One runtime for the whole crawl, multi threaded because gate 1.1 wants
    // 250 pages a second and the extract and the sketch are CPU work that a
    // single threaded runtime would serialise behind the fetches.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;

    let mut log = Log::open(&layout.log)?;
    let mut summary = Summary::default();
    let mut manifest = Manifest::new(&scope.name, &day(started_ms), StreamKind::Pages, None);
    let publisher = match &options.publish {
        Some(publishing) => Some(publisher(publishing, &scope, layout)?),
        None => None,
    };

    runtime.block_on(async {
        // Doc 12.7's step 8 for whatever a previous run got as far as step 6
        // and then lost the process. Before the first fetch rather than after
        // the last one, because the reason it matters is disk, and disk is
        // what the crawl about to start is going to want.
        if let Some(publisher) = &publisher {
            // Doc 12.5's key directory, before anything is signed with it. A
            // manifest signed by a key that is not published is a manifest
            // nobody outside this machine can check, which is the thing doc
            // 16's gate 1.5 exists to prevent, and finding that out at the end
            // of the crawl would be finding it out too late.
            let added = publisher
                .announce(&meta_repo(&options.publish), clock.now_ms())
                .await?;
            if added {
                log.line("the publishing key was added to the key directory")?;
            }

            let collected = publisher.collect(&*state, clock.now_ms()).await?;
            if collected > 0 {
                log.line(&format!("{collected} segments left behind were collected"))?;
            }
        }

        let seeded = seed(&*state, &scope, options, started_ms, settings.delay_ms).await?;
        log.line(&format!(
            "seeded {seeded} urls into {}",
            layout.state.display()
        ))?;

        let mut stall = Stall::default();
        let mut frontier = Frontier::default();
        loop {
            let report = crawler
                .tick(&sink)
                .await
                .map_err(|e| Error::Crawl(e.to_string()))?;
            add(&mut summary, &report);
            harvest(
                &sink,
                layout,
                &*state,
                publisher.as_ref(),
                &mut manifest,
                &mut summary,
                &mut log,
                clock.now_ms(),
            )
            .await?;

            if report.leased > 0 {
                let now_ms = clock.now_ms();
                let queued = frontier.get(&*state, now_ms).await?;
                log.line(&progress(&summary, &report, queued, started_ms, now_ms))?;
            }

            if let Some(stop) = spent(&summary, settings, started_ms, clock.now_ms()) {
                summary.stopped = stop;
                break;
            }

            // An idle tick is not an empty frontier. One host under doc 07.6's
            // politeness delay hands out nothing for a second at a time while
            // hundreds of its urls wait their turn, and that is the normal
            // shape of a focused crawl rather than the end of one. Breaking
            // here on the first idle tick is how a `--max-pages 25` crawl
            // finishes after five pages and still exits zero, which is the
            // worst kind of wrong: it looks like a complete crawl.
            //
            // So an idle tick asks the state what is left. The count is two
            // table scans on the sqlite backend, which is why it is on this
            // branch and not on every tick: an idle tick is by definition one
            // with no fetching to slow down.
            if report.idle() {
                let now_ms = clock.now_ms();
                let stats = state
                    .stats()
                    .await
                    .map_err(|e| Error::State(e.to_string()))?;
                let waiting = stats.urls_pending + stats.leases_in_flight;
                frontier.set(waiting, now_ms);
                if waiting == 0 && !settings.watch {
                    break;
                }
                if !settings.watch && stall.stuck(waiting, now_ms) {
                    log.line(&format!(
                        "{waiting} urls pending and none leaseable for {}s, stopping",
                        STALL_LIMIT.as_secs()
                    ))?;
                    break;
                }
                tokio::time::sleep(IDLE_WAIT).await;
            } else {
                stall = Stall::default();
            }
        }
        Ok::<(), Error>(())
    })?;

    // The open segment last, so that the rows from the final tick end up in
    // Parquet like every other row rather than in a `.umi` the operator has to
    // know about.
    sink.finish().map_err(|e| Error::Crawl(e.to_string()))?;
    runtime.block_on(harvest(
        &sink,
        layout,
        &*state,
        publisher.as_ref(),
        &mut manifest,
        &mut summary,
        &mut log,
        clock.now_ms(),
    ))?;
    log.line(&format!(
        "stopped: {} rows, {} files, {} bytes fetched",
        summary.rows, summary.files, summary.bytes_fetched
    ))?;
    if options.publish.is_some() {
        log.line(&format!(
            "{} files published, {} still local",
            summary.published,
            summary.files - summary.published
        ))?;
    }
    Ok(summary)
}

/// Doc 12.4's registry, under whichever organisation is publishing.
///
/// Takes the option rather than the [`Publishing`] because the caller holds
/// one and the answer for `None` never gets used: without `--publish` there is
/// no publisher to announce a key to.
fn meta_repo(publishing: &Option<Publishing>) -> String {
    let org = publishing
        .as_ref()
        .map_or(umi_publish::repo::ORG, |p| p.org.as_str());
    format!("{org}/umi-meta")
}

/// Assemble doc 12.2's pipeline for this crawl directory.
///
/// The coordinator in the manifest is this directory's key, the same one the
/// segments are named from, so a published file can be traced back to the crawl
/// that produced it without anything else having to be recorded.
///
/// The corpus is the focused one, named after the scope, because every crawl
/// this file runs is a focused crawl. Doc 13.7 keeps those out of
/// `umi-pages-*`: the general corpus is meant to be an unbiased sample of the
/// web, a crawl of one domain is not, and mixing them poisons every statistic
/// anyone computes over the corpus afterwards.
fn publisher(publishing: &Publishing, scope: &Scope, layout: &Layout) -> Result<Publisher, Error> {
    let hub = Hub::new(publishing.token.clone())?;
    let key = SigningKey::from_hex(Role::Publishing, &publishing.key)?;
    let publisher = Publisher::new(
        hub,
        key,
        PublishConfig {
            staging: layout.staging.clone(),
            corpus: Corpus::focused(&publishing.org, &scope.name),
            slice: publishing.slice,
            coordinator: hex::encode(coordinator_key(&layout.dir)),
            ..PublishConfig::default()
        },
    )?;
    Ok(publisher)
}

fn add(summary: &mut Summary, report: &TickReport) {
    summary.rows += report.rows as u64;
    summary.fetched += report.fetched as u64;
    summary.failed += report.failed as u64;
    summary.bytes_fetched += report.bytes_fetched;
}

/// Whether a doc 13.2 budget has been reached.
fn spent(summary: &Summary, settings: &Settings, started_ms: u64, now_ms: u64) -> Option<Stop> {
    if settings.max_pages.is_some_and(|max| summary.rows >= max) {
        return Some(Stop::Budget);
    }
    if settings
        .max_bytes
        .is_some_and(|max| summary.bytes_fetched >= max)
    {
        return Some(Stop::Budget);
    }
    if let Some(limit) = settings.max_duration
        && now_ms.saturating_sub(started_ms) >= limit.as_millis() as u64
    {
        return Some(Stop::Budget);
    }
    None
}

/// Deal with every segment that sealed since the last call.
///
/// Two ways to do that, and which one runs is the whole of what `--publish`
/// means. Without it, each segment is converted here and the `.umi` deleted,
/// which is the focused crawl doc 13.5 describes. With it, each segment is
/// recorded in the state ledger and `umi-publish` does the converting, the
/// uploading and eventually the deleting, under doc 12.2's eight steps and doc
/// 12.7's four conditions.
///
/// The publisher is drained only when something sealed, rather than on every
/// tick. A drain is a query against the segments table and at gate 1.1's rate
/// that would be a query every few milliseconds to find nothing new almost
/// every time. A segment that fails to publish is retried by the next seal's
/// drain, and failing that by the one after `sink.finish`, and failing that it
/// is still on disk with its ledger row unpublished for `umi resume` to pick
/// up. Nothing is lost by waiting.
#[expect(
    clippy::too_many_arguments,
    reason = "the alternative is a struct that exists to be one call's argument list"
)]
async fn harvest(
    sink: &SegmentSink,
    layout: &Layout,
    state: &dyn State,
    publisher: Option<&Publisher>,
    manifest: &mut Manifest,
    summary: &mut Summary,
    log: &mut Log,
    now_ms: u64,
) -> Result<(), Error> {
    let sealed = sink.sealed();
    let Some(publisher) = publisher else {
        keep(&sealed, layout, manifest, summary)?;
        return write_manifest(&layout.manifest, manifest);
    };
    if sealed.is_empty() {
        return Ok(());
    }

    record(&sealed, state, now_ms).await?;
    let (done, failed) = publisher.drain(state, now_ms).await?;
    for published in &done {
        summary.files += 1;
        summary.published += 1;
        summary.bytes_stored += published.bytes;
        receipt(&layout.published, published)?;
        if let Some(blocked) = &published.blocked {
            // Published, and the local copy is still on disk because one of
            // doc 12.7's conditions did not hold. Not an error: the next
            // `collect` tries again. It is worth a log line, because a crawl
            // whose disk is filling up while it publishes successfully is
            // otherwise a mystery.
            log.line(&format!(
                "{} is on the hub but the local copy stays: {blocked}",
                published.segment
            ))?;
        }
    }
    for (segment, error) in &failed {
        log.line(&format!("{segment} did not publish, will retry: {error}"))?;
    }
    Ok(())
}

/// Record a sealed segment in the ledger, which is what makes it publishable.
///
/// The digest is over the sealed `.umi` and is the only reason this reads the
/// file at all. It is what doc 12.8's reconciliation compares a recovered local
/// file against, and computing it now costs about 50 ms per 128 MB segment
/// against the 30 seconds doc 12.2 budgets for the conversion that follows.
async fn record(sealed: &[umi_crawl::Sealed], state: &dyn State, now_ms: u64) -> Result<(), Error> {
    let mut rows = Vec::with_capacity(sealed.len());
    for segment in sealed {
        let bytes = std::fs::metadata(&segment.path).map_err(Error::Io)?.len();
        rows.push(SegmentRow {
            id: segment.id,
            stream: Stream::Pages,
            local_path: segment.path.to_string_lossy().into_owned(),
            sealed_at_ms: now_ms,
            rows: segment.stats.rows,
            bytes,
            local_digest: Digest::from_bytes(digest_of(&segment.path)?),
            remote: None,
            manifest_day: None,
            deleted_at_ms: None,
        });
    }
    state
        .put_segment(&rows)
        .await
        .map_err(|e| Error::State(e.to_string()))
}

/// blake3 of a file, read in chunks rather than into memory.
fn digest_of(path: &Path) -> Result<[u8; 32], Error> {
    let mut file = std::fs::File::open(path).map_err(Error::Io)?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut file, &mut hasher).map_err(Error::Io)?;
    Ok(*hasher.finalize().as_bytes())
}

/// One line of `published.jsonl`, appended and flushed.
///
/// Appended rather than rewritten, and one object per line rather than one
/// array, so that a crawl killed halfway leaves a file that still parses up to
/// the last complete line.
fn receipt(path: &Path, published: &Published) -> Result<(), Error> {
    use std::io::Write as _;
    let line = serde_json::json!({
        "segment": published.segment.to_text(),
        "repo": published.repo,
        "path": published.path,
        "day": published.day,
        "blake3": published.digest.to_string(),
        "rows": published.rows,
        "bytes": published.bytes,
        "local_deleted": published.blocked.is_none(),
    });
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(Error::Io)?;
    writeln!(file, "{line}").map_err(Error::Io)?;
    file.flush().map_err(Error::Io)
}

/// Convert every sealed segment into `data/`, and delete the `.umi`.
///
/// Doc 12.2's checksum verification happens inside `convert`, so a segment
/// whose chunks do not decode stops the crawl here rather than turning into a
/// Parquet file nobody notices is wrong. That is also why the delete is after
/// the convert and not part of it.
fn keep(
    sealed: &[umi_crawl::Sealed],
    layout: &Layout,
    manifest: &mut Manifest,
    summary: &mut Summary,
) -> Result<(), Error> {
    for sealed in sealed {
        let name = format!("{}.parquet", sealed.id.to_text());
        let out = layout.data.join(&name);
        let segment = umi_file::Segment::open(&sealed.path)?;
        let converted =
            umi_publish::convert(&segment, &out).map_err(|e| Error::Crawl(e.to_string()))?;
        drop(segment);

        manifest.insert(FileEntry {
            path: format!("{DATA}/{name}"),
            bytes: converted.bytes,
            rows: converted.rows,
            blake3: converted.blake3,
            sha256: converted.sha256,
            segment_ulid: sealed.id.to_text(),
            coordinator: "local".to_owned(),
            extractor: umi_types::CANON_VERSION.to_owned(),
            fetched_at_min_ms: converted.first_ms,
            fetched_at_max_ms: converted.last_ms,
            // Everything in a focused crawl was fetched by this machine, so
            // doc 06 has nobody to disagree with and every row is `local`.
            verification: Verification {
                local: converted.rows,
                ..Verification::default()
            },
        });
        summary.files += 1;
        summary.bytes_stored += converted.bytes;
        std::fs::remove_file(&sealed.path).map_err(Error::Io)?;
    }
    Ok(())
}

fn write_manifest(path: &Path, manifest: &Manifest) -> Result<(), Error> {
    let json = manifest
        .to_json()
        .map_err(|e| Error::Crawl(format!("manifest: {e}")))?;
    std::fs::write(path, json).map_err(Error::Io)
}

/// Put the starting URLs in the frontier.
///
/// The target is always a seed, because `umi crawl example.com` that fetched
/// nothing until somebody piped it a URL list would be a surprising command.
/// Everything else comes from doc 13.6's sources, and a seeder is any program
/// that writes URLs to stdout and exits zero.
async fn seed(
    state: &dyn State,
    scope: &Scope,
    options: &Options,
    now_ms: u64,
    delay_ms: u32,
) -> Result<u64, Error> {
    let mut urls: Vec<String> = Vec::new();
    if let Some(url) = seed_url(&options.target) {
        urls.push(url);
    }
    for source in sources(options) {
        let stream = umi_seed::seed(source, umi_seed::Limits::default())
            .map_err(|e| Error::Crawl(e.to_string()))?;
        for item in stream {
            let seed = item.map_err(|e| Error::Crawl(e.to_string()))?;
            urls.push(seed.url);
        }
    }

    // The scope filters the seeds too. A URL list that wandered off the target
    // would otherwise be a focused crawl that quietly is not one, and the seed
    // is exactly where that is cheapest to catch.
    urls.retain(|url| scope.allows(url));
    urls.sort();
    urls.dedup();

    let candidates: Vec<Candidate<'_>> = urls
        .iter()
        .filter_map(|url| {
            let mut candidate = Candidate::new(url, now_ms).ok()?;
            candidate.discovery = umi_state::Discovery::Seed;
            Some(candidate)
        })
        .collect();
    if candidates.is_empty() {
        return Ok(0);
    }
    let admitted = state
        .admit(&candidates)
        .await
        .map_err(|e| Error::State(e.to_string()))?;

    // The rate override, applied to the hosts we know about at the start.
    // Doc 13.3 lets a profile lower a host's rate and never raise it, and the
    // adaptive delay in doc 07.6 takes over from here, so this is a starting
    // point rather than a setting.
    let mut hosts: Vec<umi_state::HostRow> = Vec::new();
    let mut seen: Vec<umi_types::HostId> = Vec::new();
    for candidate in &candidates {
        if seen.contains(&candidate.key.host) {
            continue;
        }
        seen.push(candidate.key.host);
        let mut row = umi_state::HostRow::new(candidate.key.host, candidate.key.pld);
        row.adaptive_delay_ms = delay_ms.max(umi_state::HostRow::DEFAULT_FLOOR_MS);
        hosts.push(row);
    }
    state
        .put_host(&hosts)
        .await
        .map_err(|e| Error::State(e.to_string()))?;

    Ok(u64::from(admitted.admitted))
}

/// The sources doc 13.6 lists, in the order the flags were given.
fn sources(options: &Options) -> Vec<umi_seed::Source> {
    let mut out = Vec::new();
    match options.seed.as_deref() {
        Some("-") => out.push(umi_seed::Source::Stdin),
        Some(path) => out.push(umi_seed::Source::File(PathBuf::from(path))),
        None => {}
    }
    for command in &options.seeder {
        out.push(umi_seed::Source::shell(command.clone()));
    }
    out
}

/// The URL a target seeds with, or none when the target is a profile.
fn seed_url(target: &str) -> Option<String> {
    if target.ends_with(".toml") {
        return None;
    }
    if target.starts_with("http://") || target.starts_with("https://") {
        return Some(target.to_owned());
    }
    Some(format!("https://{target}/"))
}

/// Doc 04's coordinator key, which for a local crawl is the directory.
///
/// A stable value rather than a random one, so that resuming a crawl continues
/// the same identifier stream rather than starting a new one, and so that two
/// crawls in two directories on one machine never produce the same segment
/// name.
fn coordinator_key(dir: &Path) -> [u8; 32] {
    let absolute = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    *blake3::hash(absolute.as_os_str().as_encoded_bytes()).as_bytes()
}

/// The `YYYYMMDD` a manifest is filed under.
///
/// Days since the epoch turned into a date by the civil from days algorithm,
/// which is about ten lines and has no dependency, against a date crate that
/// would be a dependency for this one call.
fn day(ms: u64) -> String {
    let days = i64::try_from(ms / 86_400_000).unwrap_or(0);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}{m:02}{d:02}")
}

/// Doc 14.3's queue depth, asked for on a timer.
///
/// The sqlite backend counts pending urls with a table scan, and at gate 1.1's
/// 250 pages a second a count per progress line is a scan every few
/// milliseconds. So the number is refreshed at most this often and reused in
/// between. Nothing decides anything on the cached value: the stop rule in
/// [`run`] asks the state itself and then hands the fresh answer back here.
const FRONTIER_EVERY: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Frontier {
    pending: u64,
    asked_ms: u64,
    asked: bool,
}

impl Frontier {
    async fn get(&mut self, state: &dyn State, now_ms: u64) -> Result<u64, Error> {
        let due = !self.asked
            || now_ms.saturating_sub(self.asked_ms) >= FRONTIER_EVERY.as_millis() as u64;
        if due {
            let stats = state
                .stats()
                .await
                .map_err(|e| Error::State(e.to_string()))?;
            self.set(stats.urls_pending + stats.leases_in_flight, now_ms);
        }
        Ok(self.pending)
    }

    fn set(&mut self, pending: u64, now_ms: u64) {
        self.pending = pending;
        self.asked_ms = now_ms;
        self.asked = true;
    }
}

/// The "nothing is moving" detector behind [`STALL_LIMIT`].
///
/// It watches the pending count rather than a clock on its own, because a
/// crawl of one slow host is idle most of the time and is not stuck. What
/// stuck means here is that the number of urls waiting has not moved at all
/// while no tick has leased anything.
#[derive(Default)]
struct Stall {
    /// The count when it last changed, and the time we first saw that count.
    seen: Option<(u64, u64)>,
}

impl Stall {
    fn stuck(&mut self, waiting: u64, now_ms: u64) -> bool {
        match self.seen {
            Some((count, since)) if count == waiting => {
                now_ms.saturating_sub(since) >= STALL_LIMIT.as_millis() as u64
            }
            _ => {
                self.seen = Some((waiting, now_ms));
                false
            }
        }
    }
}

/// Doc 14.3's progress line.
///
/// `in flight` is the tick's lease count rather than a live number, because a
/// tick waits for its own fetches and by the time this prints they are all
/// answered. It is still the field an operator wants next to `queued`: it is
/// how much of the configured concurrency the frontier could actually use.
fn progress(
    summary: &Summary,
    report: &TickReport,
    queued: u64,
    started_ms: u64,
    now_ms: u64,
) -> String {
    let elapsed = now_ms.saturating_sub(started_ms).max(1) as f64 / 1000.0;
    format!(
        "{} done  {} in flight  {} queued  {:.1} p/s  {} MB fetched  {} MB stored  \
         {} failed  bottleneck: {}",
        summary.rows,
        report.leased,
        queued,
        summary.rows as f64 / elapsed,
        summary.bytes_fetched / (1 << 20),
        summary.bytes_stored / (1 << 20),
        summary.failed,
        bottleneck(report),
    )
}

/// Doc 14.3's one word answer to "why is this not faster".
///
/// One word and not a number, because the question everybody asks first is
/// whether raising concurrency would help, and `politeness` answers it.
fn bottleneck(report: &TickReport) -> &'static str {
    if report.leased == 0 {
        "politeness"
    } else if report.failed * 4 > report.leased {
        "origin-slow"
    } else {
        "none"
    }
}

/// `crawl.log`, appended to and flushed every line.
///
/// Flushed rather than buffered because the reason this file exists is to be
/// readable while the crawl is running, and a buffered log is empty for the
/// first eight kilobytes of a crawl that has been going for an hour.
struct Log(std::fs::File);

impl Log {
    fn open(path: &Path) -> Result<Self, Error> {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map(Self)
            .map_err(Error::Io)
    }

    fn line(&mut self, text: &str) -> Result<(), Error> {
        use std::io::Write;
        writeln!(self.0, "{text}").map_err(Error::Io)?;
        eprintln!("umi: {text}");
        self.0.flush().map_err(Error::Io)
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            target: String::new(),
            include: Vec::new(),
            exclude: Vec::new(),
            depth: None,
            links: "in-scope".to_owned(),
            max_pages: None,
            max_duration: None,
            watch: false,
            rps: 1.0,
            concurrency: 4,
            tier_max: 3,
            seed: None,
            seeder: Vec::new(),
            out: None,
            publish: None,
        }
    }
}
