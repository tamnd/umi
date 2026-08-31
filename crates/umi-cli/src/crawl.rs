//! `umi crawl`, `umi resume` and `umi watch`, which is doc 13.5's crawl
//! directory.
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
//! # With `--watch`
//!
//! The same loop, without the rule that stops it when the frontier drains.
//! Everything that keeps a watch honest is in doc 09: a completed row gets a
//! due time from the change rate estimator and is leasable again when that
//! time arrives, so a watch does not need a scheduler of its own and does not
//! have one. What it needs is to survive being left alone for a fortnight,
//! which is two things. It backs off between empty ticks, because a crawl that
//! is meant to be idle most of the time must not poll a database once a second
//! to prove it. And it stops on the first interrupt rather than dying, because
//! a process killed between two seals loses the rows in the open segment, and
//! ctrl-c is how every long running command ends.
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
    Backpressure, Clock, CrawlConfig, Crawler, Recorded, Scope, SegmentInfo, SegmentSink, Signals,
    SupervisedLedger, SystemClock, TickReport, probe,
};
use umi_fetch::webbotauth::Signer;
use umi_fetch::{FetchConfig, Ladder};
use umi_file::{StreamKind, WriterConfig};
use umi_publish::manifest::{FileEntry, Manifest, Verification};
use umi_publish::repo::Corpus;
use umi_publish::{BlockEntry, Hub, PublishConfig, Published, Publisher, Role, SigningKey};
use umi_state::{Candidate, SegmentQuery, SegmentRow, State, StateStats, Stream};
use umi_state_sqlite::SqliteState;
use umi_types::{Digest, FetcherId, Tier, Ulid};

use crate::Error;

#[cfg(test)]
#[path = "crawl_tests.rs"]
mod tests;

/// The file names doc 13.5 fixes.
const PROFILE: &str = "profile.toml";
pub(crate) const STATE: &str = "state.sqlite";
const SEGMENTS: &str = "segments";
const DATA: &str = "data";
const MANIFEST: &str = "manifest.json";
const LOG: &str = "crawl.log";

/// Where `--publish` stages Parquet on its way to the hub, and the record it
/// leaves behind. Neither is in doc 13.5, because doc 13.5 describes a crawl
/// that keeps its output.
const STAGING: &str = "parquet";
const PUBLISHED: &str = "published.jsonl";

/// How many times a tick's fetch window refills before the tick commits and
/// returns, which is the batch as a multiple of `--concurrency`.
const REFILLS: usize = 16;

/// How long to wait before asking an idle frontier again.
///
/// Doc 07.6 starts every host at a one second delay, so a frontier that is
/// idle is usually a frontier where every host with work is inside its
/// politeness window, and the shortest useful wait is about that long. Asking
/// faster is a spin loop that produces no fetches.
const IDLE_WAIT: Duration = Duration::from_millis(1000);

/// The longest a watching crawl waits between asking an idle frontier again.
///
/// A watch is idle most of the time on purpose, and at [`IDLE_WAIT`] it spends
/// that time waking up once a second to run a lease query and a counter read
/// that find nothing. Neither is expensive on its own, which is why this is a
/// minute and not an hour, but doc 16 runs a watch for a fortnight next to a
/// general crawl that wants the whole box, and a wakeup a second for a
/// fortnight is a million of them. Measured over three minutes of watching a
/// drained frontier on server3: 0.55 seconds of cpu and 824 voluntary context
/// switches at one second, against 0.19 and 66 with the backoff.
///
/// What it costs in return is up to a minute of delay on a refresh. Doc 09.4
/// will not schedule a URL closer than five minutes out however fast it
/// changes, so the worst case is a page fetched at six minutes rather than
/// five, and every other class is hours or days.
const WATCH_MAX_WAIT: Duration = Duration::from_secs(60);

/// How often a watching crawl says it is still there.
///
/// Doc 14.1 asks for progress that means something, and a command that prints
/// nothing for six hours is indistinguishable from a hung one. This is slow
/// enough that a fortnight of it is a readable log.
const HEARTBEAT: Duration = Duration::from_secs(300);

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
    /// Doc 05.7's opt in. Off by default and the only thing that lets a lease
    /// come back at T4, and then only for a domain somebody put on the
    /// supervised list with `umi supervise`. Two separate switches on purpose:
    /// one says which domains may be crawled that way and the other says this
    /// machine is willing to do it, and neither is any use without the other.
    pub allow_supervised: bool,
    /// Doc 05.6's tab cap. Zero means this machine does not render, which is
    /// the default and is also what a build without the `render` feature can
    /// do regardless of what is configured.
    pub tabs: u16,
    /// A file of URLs, or `-` for standard input.
    pub seed: Option<String>,
    /// Any program that prints URLs, repeatable.
    pub seeder: Vec<String>,
    /// Follow the seed origins' sitemaps before the first tick, doc 13.6.
    ///
    /// Nothing when neither `--sitemaps` nor `--no-sitemaps` was given, which
    /// hands the decision to doc 13.4's profile and then to the default.
    pub sitemaps: Option<bool>,
    /// Where the directory goes. Defaults to `./<scope name>`.
    pub out: Option<String>,
    /// Doc 12's pipeline, or nothing when `--publish` was not given.
    pub publish: Option<Publishing>,
    /// Doc 07.2's crawl identity key, or nothing when none is configured.
    pub identity: Option<Identity>,
}

/// The key doc 07.2 signs outgoing requests with.
///
/// Its own struct with its own [`fmt::Debug`], for the reason [`Publishing`]
/// has one: [`Options`] derives `Debug` and a private key that reached a log
/// line would have to be rotated.
#[derive(Clone)]
pub struct Identity {
    /// The seed, 32 bytes.
    seed: [u8; 32],
}

impl Identity {
    /// Read the key out of doc 14.7's five layers, or nothing when the
    /// operator has not configured one.
    ///
    /// Not configuring one is a supported way to run. An unsigned crawler is
    /// what umi was before this existed and it still works, it just does not
    /// get the benefit of the doors Web Bot Auth opens. A key that is
    /// configured and unreadable is an error, because that is somebody who
    /// meant to sign and would otherwise find out from a site's logs.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the indirection does not resolve, and
    /// [`Error::Missing`] when what it points at is not 64 hex characters.
    pub fn resolve(config: &crate::config::Config) -> Result<Option<Self>, Error> {
        let Some(secret) = &config.identity_key else {
            return Ok(None);
        };
        let text = secret.value.read()?;
        let mut seed = [0u8; 32];
        hex::decode_to_slice(text.trim(), &mut seed).map_err(|_| {
            Error::Missing(
                "crawl.identity_key must point at 64 hex characters, which is doc 07.2's                  ed25519 seed"
                    .to_owned(),
            )
        })?;
        Ok(Some(Self { seed }))
    }

    /// The thumbprint of this key, which is what a site operator looks up in
    /// the published directory and what `umi doctor` prints.
    ///
    /// # Errors
    ///
    /// The same as [`Identity::signer`].
    pub fn keyid(&self) -> Result<String, Error> {
        Ok(self.signer(0)?.keyid().to_owned())
    }

    /// Build the signer, mixing a per process nonce seed.
    ///
    /// The nonce seed is derived rather than drawn from a random source,
    /// because a nonce has to be unique and unguessable and neither of those
    /// needs entropy when there is already a secret in the room. blake3 over
    /// the private key gives the unguessable half and the start time and the
    /// process id give the per process half, so two coordinators sharing a key
    /// do not share a nonce stream.
    ///
    /// # Errors
    ///
    /// [`Error::Missing`] when the agent url does not parse, which it does.
    pub fn signer(&self, started_ms: u64) -> Result<Signer, Error> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.seed);
        hasher.update(b"umi/web-bot-auth/nonce");
        hasher.update(&started_ms.to_be_bytes());
        hasher.update(&std::process::id().to_be_bytes());
        let mut nonce_seed = [0u8; 16];
        nonce_seed.copy_from_slice(&hasher.finalize().as_bytes()[..16]);

        Signer::new(
            self.seed,
            umi_fetch::webbotauth::AGENT,
            nonce_seed,
            Box::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_secs())
            }),
        )
        .map_err(|e| Error::Missing(e.to_string()))
    }
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity").finish_non_exhaustive()
    }
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
    /// The operator interrupted it. Doc 14.9 has no code for that and does not
    /// need one: stopping is what was asked for, so it is exit 0 like any other
    /// command that did the thing.
    Signal,
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
pub fn resume(
    dir: &Path,
    watch: bool,
    publish: Option<Publishing>,
    identity: Option<Identity>,
) -> Result<Summary, Error> {
    // Before the layout, so that pointing either of these at the wrong
    // directory says so rather than leaving two empty directories in it.
    let scope = profile_of(dir)?;
    let layout = Layout::create(dir)?;

    // No seeding and no target. Doc 13.5's promise is that the directory is
    // the unit, so everything a resume needs is in it, and a resume that
    // reseeded would put the seeds back in the frontier on every restart.
    // Publishing comes from the flags and the configuration rather than from
    // the profile, because the profile is checked into somebody's repository
    // and the two things `--publish` needs are secrets.
    //
    // Sitemaps off, and said out loud rather than left to the default. The
    // profile's own `seed.urls` are read on a resume, so the origins are no
    // longer empty here, and a full seeding pass over them on every restart
    // would refetch sitemap files that can run to tens of thousands per host
    // to learn nothing. Doc 09's polling pass is the one that revisits them.
    let options = Options {
        target: scope.name.clone(),
        watch,
        seed: None,
        seeder: Vec::new(),
        sitemaps: Some(false),
        publish,
        identity,
        ..Options::default()
    };
    let mut config = settings(&options)?;
    config.watch = watch;
    run(&layout, &scope, &config, &options)
}

/// `umi publish <dir>`: push a finished crawl directory through doc 12.2.
///
/// The same pipeline `umi crawl --publish` runs, pointed at a directory that is
/// already sitting on the disk. That is the case doc 14.6 is written for and it
/// is the common one, because `umi crawl example.com` is the first command
/// anybody runs and it keeps its output rather than publishing it. Deciding to
/// publish afterwards should not mean crawling the site again.
///
/// Two things happen before the pipeline. The publishing key goes into doc
/// 12.5's directory, for the same reason it does at the top of a crawl: a
/// manifest signed by a key nobody can look up is a manifest nobody can check.
/// Then every local file gets a ledger row if it does not have one, which is
/// what [`Publisher::drain`] reads. A directory that ran without `--publish`
/// has no segment rows at all, so without that step there would be nothing for
/// the publisher to find and this command would cheerfully do nothing.
///
/// What it does not do is crawl, seed or convert. If the directory is short of
/// pages the answer is `umi resume`, and this stays the command that publishes
/// what is there.
///
/// # Errors
///
/// [`Error::Io`] when the directory has no `profile.toml`, which is what tells
/// a crawl directory apart from any other directory, [`Error::NothingToDo`]
/// when everything in it is already published, and whatever the hub or the
/// state ledger reports. A run where some files published and others did not
/// prints its summary and then returns the first failure, so the exit code
/// reflects the worst thing that happened rather than the last.
pub fn publish(dir: &Path, publishing: &Publishing) -> Result<Summary, Error> {
    let scope = profile_of(dir)?;
    let layout = Layout::create(dir)?;
    let state: Arc<dyn State> =
        Arc::new(SqliteState::open(&layout.state).map_err(|e| Error::State(e.to_string()))?);
    let publisher = publisher(publishing, &scope, &layout)?;
    let clock = SystemClock;
    let mut log = Log::open(&layout.log)?;

    // Single threaded, unlike the crawl loop. There is no fetching here and no
    // extraction, so the work is one upload at a time against one hub, and a
    // second runtime thread would have nothing to do but exist.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;

    let mut summary = Summary::default();
    let mut collected = 0;
    let failure = runtime.block_on(async {
        let now_ms = clock.now_ms();
        if publisher
            .announce(&meta_repo(&publishing.org), now_ms)
            .await?
        {
            log.line("the publishing key was added to the key directory")?;
        }

        let adopted = adopt(&layout, &*state, &mut log).await?;
        if adopted > 0 {
            log.line(&format!("{adopted} local files were added to the ledger"))?;
        }

        collected = publisher.collect(&*state, now_ms).await?;
        if collected > 0 {
            log.line(&format!(
                "{collected} files published by an earlier run were deleted locally"
            ))?;
        }

        let (done, failed) = publisher.drain(&*state, now_ms).await?;
        for published in &done {
            summary.files += 1;
            summary.published += 1;
            summary.rows += published.rows;
            summary.bytes_stored += published.bytes;
            receipt(&layout.published, published)?;
            if let Some(blocked) = &published.blocked {
                log.line(&format!(
                    "{} is on the hub but the local copy stays: {blocked}",
                    published.segment
                ))?;
            }
        }
        for (segment, error) in &failed {
            summary.files += 1;
            log.line(&format!("{segment} did not publish: {error}"))?;
        }
        // The first one rather than a count, because the exit code doc 14.9
        // asks for depends on which kind of failure it was and a count has no
        // kind. The rest are in the log a line above.
        Ok::<Option<Error>, Error>(failed.into_iter().next().map(|(_, cause)| cause.into()))
    })?;

    if summary.files == 0 && collected == 0 {
        return Err(Error::NothingToDo(
            "everything in this crawl directory is already published",
        ));
    }
    log.line(&format!(
        "{} of {} files published, {} rows",
        summary.published, summary.files, summary.rows
    ))?;
    match failure {
        Some(cause) => Err(cause),
        None => Ok(summary),
    }
}

/// Give every local file in a crawl directory a ledger row, and say how many
/// that took.
///
/// The publisher works off the ledger and nothing else, which is right: doc
/// 12.7's fourth condition is a ledger row and a pipeline that could publish a
/// file it had no row for would have nothing to write that condition into. So
/// this is the step that turns a directory of files into a set of rows, and it
/// is deliberately the only place in the CLI that does.
///
/// Both kinds of local file are picked up. `data/*.parquet` is what a plain
/// crawl leaves and is the usual case; `segments/*.umi` is what a crawl that
/// died between sealing a segment and converting it leaves, and there is no
/// reason to make the operator convert it by hand first.
///
/// The seal time comes out of the ULID rather than off the clock. A ULID's
/// first 48 bits are the millisecond it was minted, which is the millisecond
/// the segment sealed, so adopting an old directory records when the rows were
/// really written rather than when somebody got round to publishing them. A
/// file whose name is not a ULID is skipped and reported: it did not come from
/// a crawl, and inventing an identifier for it would put a file in the corpus
/// that traces back to nothing.
async fn adopt(layout: &Layout, state: &dyn State, log: &mut Log) -> Result<usize, Error> {
    let mut rows = Vec::new();
    for path in local_files(layout)? {
        let Some(id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(Ulid::parse)
        else {
            log.line(&format!(
                "{} is not named after a segment, skipping it",
                path.display()
            ))?;
            continue;
        };
        let known = state
            .segment(id)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        if known.is_some() {
            continue;
        }
        rows.push(SegmentRow {
            id,
            // Every file a focused crawl writes is a page segment, because the
            // sink in `run` is created with one stream and doc 13.5's directory
            // has one place to put files.
            stream: Stream::Pages,
            local_path: path.to_string_lossy().into_owned(),
            sealed_at_ms: id.timestamp_ms(),
            rows: rows_in(&path)?,
            bytes: std::fs::metadata(&path).map_err(Error::Io)?.len(),
            local_digest: Digest::from_bytes(digest_of(&path)?),
            remote: None,
            manifest_day: None,
            deleted_at_ms: None,
        });
    }
    let adopted = rows.len();
    if adopted > 0 {
        state
            .put_segment(&rows)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
    }
    Ok(adopted)
}

/// Everything in the directory that holds rows, in name order.
///
/// The staging directory is not one of the two, and that matters: with
/// `--publish` it holds a copy of a file that is being uploaded right now, and
/// adopting that copy would give one segment two ledger rows.
fn local_files(layout: &Layout) -> Result<Vec<PathBuf>, Error> {
    let mut found = Vec::new();
    for (dir, extension) in [(&layout.data, "parquet"), (&layout.segments, "umi")] {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries {
            let path = entry.map_err(Error::Io)?.path();
            if path.extension().is_some_and(|kind| kind == extension) {
                found.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

/// How many rows a local file holds, off its metadata rather than by decoding
/// it.
///
/// Cheap on purpose. This number goes in the ledger row, and the publisher
/// compares it against the rows it decodes on the way out, so reading it by
/// decoding the file here would make that comparison two readings of the same
/// bytes agreeing with each other, which proves nothing. Read cheaply, check
/// expensively, and a file whose footer disagrees with its pages is caught.
fn rows_in(path: &Path) -> Result<u64, Error> {
    if path.extension().is_some_and(|kind| kind == "parquet") {
        use parquet::file::reader::FileReader as _;
        let file = std::fs::File::open(path).map_err(Error::Io)?;
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file)?;
        let rows = reader.metadata().file_metadata().num_rows();
        return Ok(u64::try_from(rows).unwrap_or(0));
    }
    Ok(umi_file::Segment::open(path)?.stats().rows)
}

/// The scope a crawl directory was run with.
///
/// `profile.toml` is doc 13.5's first entry and it is what tells a crawl
/// directory apart from any other directory, so the error says that rather than
/// repeating the operating system's word for a missing file.
fn profile_of(dir: &Path) -> Result<Scope, Error> {
    let text = std::fs::read_to_string(dir.join(PROFILE)).map_err(|cause| {
        Error::Io(std::io::Error::new(
            cause.kind(),
            format!("{} is not a crawl directory: {cause}", dir.display()),
        ))
    })?;
    Scope::from_toml(&text).map_err(|e| Error::Scope(e.to_string()))
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
    // Doc 16's gate 3.1 needs the fetch window to refill many times inside one
    // tick, because a tick that fills its window once and drains it runs at the
    // batch over its slowest lease rather than the window over the mean.
    // Sixteen refills puts the cost of the last drain at a sixteenth of the
    // tick. Capped by `--max-pages`, because a tick runs to the end of its
    // batch and the batch is therefore also how far past the page limit a crawl
    // can go before the loop between ticks notices.
    let in_flight = usize::from(options.concurrency.max(1));
    let batch = u32::try_from(in_flight.saturating_mul(REFILLS))
        .unwrap_or(u32::MAX)
        .min(u32::try_from(options.max_pages.unwrap_or(u64::MAX)).unwrap_or(u32::MAX))
        .max(1);
    Ok(Settings {
        config: CrawlConfig {
            fetcher: FetcherId::LOCAL,
            in_flight,
            batch,
            max_tier: tier(options.tier_max, options.allow_supervised),
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

fn tier(max: u8, allow_supervised: bool) -> Tier {
    // Doc 05.7's opt in, and it is the only thing in the workspace that
    // produces `Tier::Supervised` from operator input. It raises the ceiling
    // and nothing more: the lease still comes back at T3 unless the domain is
    // on the supervised list, which is the state layer's decision and not this
    // one. An explicit `--tier` below 3 still wins, because somebody who capped
    // the ladder meant it and a flag about one rung should not undo a cap about
    // all of them.
    if allow_supervised && max >= 3 {
        return Tier::Supervised;
    }
    match max {
        0 => Tier::Revalidate,
        1 => Tier::Plain,
        2 => Tier::Emulated,
        // Doc 05.7 says T4 is allowlisted and opted into, never reached by
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

/// Every rung this build has, with a browser behind T3 when the machine asked
/// for one.
///
/// Async because starting Chrome is, and it takes the tab cap rather than
/// reading it from anywhere so that the two ways of ending up without a
/// browser stay separate: `tabs = 0` is a machine saying no, and a build with
/// no `render` feature is a binary that could not have said yes. Both end with
/// a T2 ladder and the second one says so in the log, because a crawl that
/// silently ignored a configured tab cap would look like a browser that keeps
/// failing to help.
async fn ladder(
    config: FetchConfig,
    signer: Option<Arc<umi_fetch::webbotauth::Signer>>,
    tabs: u16,
) -> Result<Ladder, Error> {
    if tabs == 0 {
        return Ok(Ladder::with_signer(config, signer)?);
    }
    #[cfg(feature = "render")]
    {
        // Assigned rather than built with a struct literal because
        // `RenderConfig` is non exhaustive, which is the point: the rest of
        // doc 05.6's numbers are defaults the crawl does not second guess.
        let mut render = umi_fetch::RenderConfig::default();
        render.tabs = usize::from(tabs);
        Ok(Ladder::with_rendered(config, signer, render).await?)
    }
    #[cfg(not(feature = "render"))]
    {
        eprintln!("umi: this build has no tier 3, so the tab cap of {tabs} does nothing");
        Ok(Ladder::with_signer(config, signer)?)
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
    // Every rung this binary was built with. `--tier` is already on
    // `CrawlConfig` and the loop honours it as a ceiling, so a crawl asking
    // for a tier above the top of the ladder gets the top of the ladder rather
    // than an error, which is the right behaviour while the ladder is still
    // growing rungs.
    let mut fetch_config = FetchConfig::default();
    // Doc 05.4's ceiling rather than the 512 KB the one page commands default
    // to. A crawl that truncated every large page would produce rows whose
    // `content_length` and body digest describe a prefix of what the origin
    // sent, which is worse than not having the page.
    fetch_config.body_cap = 8 << 20;
    let clock = SystemClock;
    let started_ms = clock.now_ms();

    // Doc 07.2. Every rung signs, or none of them does. A crawl with no key
    // configured sends the same requests it always sent.
    let signer = match &options.identity {
        Some(identity) => Some(Arc::new(identity.signer(started_ms)?)),
        None => None,
    };
    let signed_as = signer.as_ref().map(|s| s.keyid().to_owned());

    // One runtime for the whole crawl, multi threaded because gate 1.1 wants
    // 250 pages a second and the extract and the sketch are CPU work that a
    // single threaded runtime would serialise behind the fetches.
    //
    // Built before the ladder rather than after it, because starting a browser
    // is async and a browser started on a runtime that is then dropped takes
    // its connection with it.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    let fetcher = runtime.block_on(ladder(fetch_config, signer, options.tabs))?;

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

    // Doc 05.7's record. It writes nothing until something is leased at T4, so
    // a crawl that never opts in leaves no file behind, and it wraps the
    // segment sink rather than replacing it because the same rows go to both.
    let ledger = SupervisedLedger::in_dir(&layout.dir);

    let crawler = Crawler::new(
        fetcher,
        Arc::clone(&state),
        clock,
        CrawlConfig {
            scope: Arc::clone(&scope),
            ..settings.config.clone()
        },
    );

    let mut log = Log::open(&layout.log)?;
    if let Some(keyid) = &signed_as {
        log.line(&format!(
            "signing requests as {keyid}, published at {}{}",
            umi_fetch::webbotauth::AGENT,
            umi_fetch::webbotauth::DIRECTORY_PATH
        ))?;
    }
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
            let org = options
                .publish
                .as_ref()
                .map_or(umi_publish::repo::ORG, |p| p.org.as_str());
            let added = publisher.announce(&meta_repo(org), clock.now_ms()).await?;
            if added {
                log.line("the publishing key was added to the key directory")?;
            }

            // Doc 07.7's block list, and the reason it is here rather than at
            // the end is the reason the key directory is. A block is fleet
            // wide, and the published list is how one reaches a coordinator
            // that was not the machine an operator typed it into. Applied
            // before the first lease, so a domain somebody asked us to stop is
            // out of the frontier rather than being noticed on the way past.
            let published = umi_publish::published_blocks(publisher.hub(), &meta_repo(org)).await?;
            let blocks: Vec<_> = published.iter().map(BlockEntry::to_row).collect();
            if !blocks.is_empty() {
                let report = state
                    .block(&blocks)
                    .await
                    .map_err(|e| Error::State(e.to_string()))?;
                log.line(&format!(
                    "{} domains on the published block list, {} urls excluded",
                    blocks.len(),
                    report.excluded
                ))?;
            }

            let collected = publisher.collect(&*state, clock.now_ms()).await?;
            if collected > 0 {
                log.line(&format!("{collected} segments left behind were collected"))?;
            }
        }

        let seeded = seed(&*state, &scope, options, started_ms, settings.delay_ms).await?;
        log.line(&format!(
            "seeded {} urls into {}{}",
            seeded.urls,
            layout.state.display(),
            match seeded.outside {
                0 => String::new(),
                n => format!(", {n} outside the scope"),
            }
        ))?;

        // Doc 13.6, and the difference between starting a site at its front
        // page and starting it with everything the site says it has. Before
        // `resume`, so that the domains these URLs are on are scheduled with
        // the rest rather than waiting for the loop to notice them.
        let sitemaps = sitemap_sources(options, &scope);
        if sitemaps.from_robots || sitemaps.well_known {
            for origin in &seeded.origins {
                let found = crawler
                    .seed_from_sitemaps(origin, sitemaps)
                    .await
                    .map_err(|e| Error::Crawl(e.to_string()))?;
                if found.files == 0 {
                    continue;
                }
                log.line(&format!(
                    "{origin} sitemaps: {} files, {} urls, {} admitted",
                    found.files, found.urls, found.admitted
                ))?;
            }
        }

        // Doc 09.8. The seeds went straight into the store, and the domain rate
        // limits are in memory, so this is where a fresh crawl and a resumed
        // one both learn which domains they are working. The loop does it for
        // itself if nobody asks, and asking here is what puts the count in the
        // log where an operator resuming a crawl can see it.
        let domains = crawler
            .resume()
            .await
            .map_err(|e| Error::Crawl(e.to_string()))?;
        log.line(&format!("scheduling {domains} domains"))?;

        let mut stall = Stall::default();
        let mut frontier = Frontier::default();
        let mut backoff = Backoff::default();
        let mut heartbeat = Heartbeat::default();
        let mut interrupt = interrupt();
        let mut pressure = Pressure::new(layout);
        loop {
            // Before the tick, so a box that has just filled up is restrained
            // on the next batch rather than after one more.
            let tick_ms = clock.now_ms();
            if pressure.due(tick_ms) {
                for moved in pressure.observe(&*state, tick_ms).await? {
                    log.line(&moved.to_string())?;
                }
                crawler.restrain(pressure.ladder.allowance());
                // Rung four. The open segment is the only thing on this box
                // holding rows that the publisher cannot see, and sealing it
                // is the one move left that turns disk into something that
                // can leave.
                if pressure.ladder.allowance().seal_open_segments {
                    sink.finish().map_err(|e| Error::Crawl(e.to_string()))?;
                }
            }

            let report = crawler
                .tick(&Recorded::new(&ledger, &sink))
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
                backoff.reset();
            }

            // After the tick and after the harvest, so an interrupt costs at
            // most the fetches already in flight and never a segment. The
            // completions this tick is holding are written either way.
            if *interrupt.borrow_and_update() {
                log.line("interrupted, stopping")?;
                summary.stopped = Stop::Signal;
                break;
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
            // So an idle tick asks the state what is left. It is on this branch
            // and not on every tick because an idle tick is by definition one
            // with no fetching to slow down, and at gate 1.1's 250 pages a
            // second a counter read per tick is a lock taken on the store's
            // connection several hundred times a second for a number nothing
            // decides anything on.
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
                // A tick the ladder held back is not a stall. The counts do not
                // move because nothing was leased, which is the ladder working
                // rather than a crawl that has got stuck, and stopping over it
                // would be the one outcome doc 15.3 exists to avoid. Cleared
                // rather than skipped, so the timer starts again from the
                // first tick after the pressure lifts instead of counting the
                // hour the crawl spent waiting for the publisher.
                if report.restrained {
                    stall = Stall::default();
                } else if !settings.watch && stall.stuck(waiting, now_ms) {
                    log.line(&format!(
                        "{waiting} urls pending and none leaseable for {}s, stopping",
                        STALL_LIMIT.as_secs()
                    ))?;
                    break;
                }
                if settings.watch && heartbeat.due(now_ms) {
                    log.line(&watching(&summary, &stats, started_ms, now_ms))?;
                }
                let wait = if settings.watch {
                    backoff.next()
                } else {
                    IDLE_WAIT
                };
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    // Waiting a minute to notice ctrl-c would make a watch feel
                    // hung at exactly the moment somebody is trying to stop it.
                    // A closed channel means there is no interrupt to wait for,
                    // and `select!` drops a branch whose pattern does not match,
                    // so that case leaves the sleep on its own rather than
                    // spinning on an error.
                    Ok(()) = interrupt.changed() => {}
                }
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
fn meta_repo(org: &str) -> String {
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

/// Which sitemaps doc 13.6's pass starts from, once the flags and doc 13.4's
/// profile have both had their say.
///
/// Doc 14.7's precedence, applied to a pair of keys rather than to one: the
/// flag wins over the profile, the profile wins over the default, and the
/// default is to follow both. `--no-sitemaps` therefore turns the whole pass
/// off whatever the profile asked for, which is what an operator typing it
/// means, and a profile is free to turn off one starting point and keep the
/// other.
fn sitemap_sources(options: &Options, scope: &Scope) -> umi_crawl::SitemapLimits {
    let seed = &scope.seed;
    umi_crawl::SitemapLimits {
        from_robots: options.sitemaps.or(seed.robots_sitemaps).unwrap_or(true),
        well_known: options.sitemaps.or(seed.sitemaps).unwrap_or(true),
        ..umi_crawl::SitemapLimits::seeding()
    }
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
) -> Result<Seeded, Error> {
    let mut urls: Vec<String> = Vec::new();
    if let Some(url) = seed_url(&options.target) {
        urls.push(url);
    }
    // Doc 13.4's `seed.urls`, which is where a profile that travels on its own
    // says where to start. A resume reads them again, and that is fine rather
    // than something to guard against: admission dedups against the seen set,
    // so the ones already fetched cost one call and go nowhere, and a url
    // added to the profile between two runs is picked up on the second.
    urls.extend(scope.seed.urls.iter().cloned());
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
    urls.sort();
    urls.dedup();
    let before = urls.len();
    urls.retain(|url| scope.allows(url));
    // Counted rather than passed over in silence. A seed list that the scope
    // rejects in full leaves a crawl that starts, drains and exits zero, and
    // the operator has nothing to go on. One number in the log is the whole
    // difference between that and an obvious mistake.
    let outside = u64::try_from(before - urls.len()).unwrap_or(u64::MAX);

    let candidates: Vec<Candidate<'_>> = urls
        .iter()
        .filter_map(|url| {
            let mut candidate = Candidate::new(url, now_ms).ok()?;
            candidate.discovery = umi_state::Discovery::Seed;
            Some(candidate)
        })
        .collect();
    if candidates.is_empty() {
        return Ok(Seeded {
            outside,
            ..Seeded::default()
        });
    }
    let admitted = state
        .admit(&candidates)
        .await
        .map_err(|e| Error::State(e.to_string()))?;

    // The rate override, applied to the hosts we know about at the start.
    // Doc 13.3 lets a profile lower a host's rate and never raise it, and the
    // adaptive delay in doc 07.6 takes over from here, so this is a starting
    // point rather than a setting.
    //
    // Read before writing. `put_host` is `INSERT OR REPLACE` and a `HostRow`
    // covers the whole record, so building a fresh one here and handing it over
    // resets everything the crawler had learned about the host: doc 05.8's tier
    // ladder, doc 07.6's adaptive delay, the robots digest and expiry, the
    // crawl delay the site published, the backoff we are currently serving, and
    // the `blocked` and `refusing` flags. That last pair is the one that
    // matters most, because `umi block` is meant to stop a domain permanently
    // and on the record, and a later crawl of the same directory would have
    // quietly undone it.
    let mut hosts: Vec<umi_state::HostRow> = Vec::new();
    let mut seen: Vec<umi_types::HostId> = Vec::new();
    let floor = delay_ms.max(umi_state::HostRow::DEFAULT_FLOOR_MS);
    for candidate in &candidates {
        if seen.contains(&candidate.key.host) {
            continue;
        }
        seen.push(candidate.key.host);
        let known = state
            .host(candidate.key.host)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        let mut row =
            known.unwrap_or_else(|| umi_state::HostRow::new(candidate.key.host, candidate.key.pld));
        // The larger of the two delays, which is the smaller of the two rates.
        // A host that has already been slowed down, by its own robots.txt or by
        // doc 07.6 watching it struggle, keeps the slower number, and `--rps`
        // can only ever make it slower still.
        row.adaptive_delay_ms = row.adaptive_delay_ms.max(floor);
        hosts.push(row);
    }
    state
        .put_host(&hosts)
        .await
        .map_err(|e| Error::State(e.to_string()))?;

    Ok(Seeded {
        urls: u64::from(admitted.admitted),
        outside,
        origins: origins(&urls),
    })
}

/// What the seeding step produced.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
struct Seeded {
    /// URLs admitted.
    urls: u64,
    /// URLs the scope rejected before admission ever saw them.
    outside: u64,
    /// The distinct origins they are on, which is what doc 13.6's sitemap pass
    /// runs against. Origins rather than hosts, because a sitemap lives at a
    /// scheme and a port as much as at a name.
    origins: Vec<String>,
}

/// The distinct origins of a seed list, in a stable order.
///
/// Sorted and deduplicated rather than kept in seed order, so that two runs of
/// the same crawl fetch the same sitemaps in the same order and a log from one
/// can be read against a log from the other.
fn origins(urls: &[String]) -> Vec<String> {
    let mut out: Vec<String> = urls
        .iter()
        .filter_map(|url| {
            let parsed = url::Url::parse(url).ok()?;
            let host = parsed.host_str()?;
            Some(match parsed.port() {
                Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
                None => format!("{}://{host}", parsed.scheme()),
            })
        })
        .collect();
    out.sort();
    out.dedup();
    out
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

/// The stop switch, as something the loop can both test and wait on.
///
/// A watch runs for days, and the way it ends is somebody stopping it, so what
/// happens then is part of the design rather than an afterthought. The default
/// handler kills the process wherever it happens to be, and the rows fetched
/// since the last seal are in a segment whose footer nobody has written yet. So
/// the first signal sets this, the loop sees it at the top of its next pass,
/// and it leaves through the same path a budget leaves through, which seals the
/// segment and converts it.
///
/// The second interrupt is the escape hatch, because the first one still waits
/// for the fetches in flight and a slow origin can hold those for a timeout.
/// 130 is the shell's number for a process ended by SIGINT, and it is not in
/// doc 14.9 because doc 14.9 is about codes umi chooses. Only ctrl-c is taken
/// for that one. A supervisor that wants a stop to be over escalates to SIGKILL
/// rather than sending its terminate twice, and nothing can be done about that
/// one anyway.
fn interrupt() -> tokio::sync::watch::Receiver<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        // An error here is a platform that will not give us the signal. Then
        // there is nothing to report and nothing to wait for, and the sender
        // dropping is how the loop finds that out.
        if first_stop().await.is_err() {
            return;
        }
        eprintln!("umi: stopping after the fetches in flight, interrupt again to quit now");
        tx.send_replace(true);
        if tokio::signal::ctrl_c().await.is_ok() {
            std::process::exit(130);
        }
    });
    rx
}

/// Whichever stop signal arrives first.
///
/// Doc 14.6. ctrl-c is how a person stops a watch and SIGTERM is how everything
/// else does: systemd, docker, kubernetes and a plain `kill` all send that one
/// and none of them send an interrupt. A command meant to run for a fortnight
/// spends almost all of its life under one of those, so taking the default
/// handler for SIGTERM would lose the open segment on every ordinary restart,
/// which is the one thing this whole path exists to prevent.
#[cfg(unix)]
async fn first_stop() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        // `None` is the stream closing, which cannot happen while the handler
        // is alive, and waiting forever is the right answer if it somehow does.
        _ = terminate.recv() => Ok(()),
    }
}

/// Whichever stop signal arrives first, where there is only one of them.
#[cfg(not(unix))]
async fn first_stop() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

/// How long to wait after an idle tick, which is the whole cost of a watch.
///
/// It doubles from [`IDLE_WAIT`] up to [`WATCH_MAX_WAIT`] and drops back to the
/// floor as soon as a tick leases anything. Doubling is right because the two
/// reasons a tick comes back empty are opposite. A host inside doc 07.6's
/// politeness window has work coming in about a second, and a frontier whose
/// next refresh is due at four in the morning has nothing coming for hours.
/// Neither says which it is, but how long it lasts does, so the wait is short
/// while it might be the first and long once it is clearly the second.
#[derive(Default)]
struct Backoff {
    /// The last wait handed out, or `None` before the first one.
    wait: Option<Duration>,
}

impl Backoff {
    fn next(&mut self) -> Duration {
        let wait = self
            .wait
            .map_or(IDLE_WAIT, |last| (last * 2).min(WATCH_MAX_WAIT));
        self.wait = Some(wait);
        wait
    }

    fn reset(&mut self) {
        self.wait = None;
    }
}

/// The timer behind [`HEARTBEAT`].
#[derive(Default)]
struct Heartbeat {
    said_ms: Option<u64>,
}

impl Heartbeat {
    /// Whether it is time to say something, which it always is the first time.
    /// A watch that printed nothing until five minutes in would look like a
    /// watch that had not started.
    fn due(&mut self, now_ms: u64) -> bool {
        let due = self
            .said_ms
            .is_none_or(|last| now_ms.saturating_sub(last) >= HEARTBEAT.as_millis() as u64);
        if due {
            self.said_ms = Some(now_ms);
        }
        due
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

/// How often doc 15.3's signals are read.
///
/// Ten seconds against a hysteresis of ten minutes. Free disk is a `df` and a
/// process spawn, the unpublished bytes are a query, and neither is a thing to
/// do several hundred times a second for a ladder that cannot move faster than
/// once every ten minutes on the way down anyway.
const SAMPLE_EVERY_MS: u64 = 10_000;

/// Doc 15.3's ladder and the sampling that feeds it.
struct Pressure {
    ladder: Backpressure,
    /// When the signals were last read, or nothing before the first read.
    sampled_ms: Option<u64>,
    /// The directory segments land in, which is the filesystem that matters.
    /// Not the working directory: a crawl writing to a mounted volume fills
    /// that volume and the one the binary was started from stays empty.
    dir: PathBuf,
}

impl Pressure {
    fn new(layout: &Layout) -> Self {
        Self {
            ladder: Backpressure::new(),
            sampled_ms: None,
            dir: layout.segments.clone(),
        }
    }

    /// Whether it is time to read the signals again.
    fn due(&self, now_ms: u64) -> bool {
        self.sampled_ms
            .is_none_or(|last| now_ms.saturating_sub(last) >= SAMPLE_EVERY_MS)
    }

    /// Read the signals and move the ladders.
    async fn observe(
        &mut self,
        state: &dyn State,
        now_ms: u64,
    ) -> Result<Vec<umi_crawl::Transition>, Error> {
        self.sampled_ms = Some(now_ms);
        let signals = self.read(state, now_ms).await?;
        Ok(self.ladder.observe(&signals, now_ms))
    }

    async fn read(&self, state: &dyn State, now_ms: u64) -> Result<Signals, Error> {
        // Unpublished segments, which is where two of doc 15.3's three disk
        // signals come from. A crawl without `--publish` has none of these
        // recorded, and that is the right answer rather than a gap: there is
        // no publisher to be behind, and free space still protects the box.
        let unpublished = state
            .segments(SegmentQuery::Unpublished)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        let unpublished_bytes = unpublished.iter().map(|row| row.bytes).sum();
        let publish_lag_ms = unpublished
            .iter()
            .map(|row| now_ms.saturating_sub(row.sealed_at_ms))
            .max()
            .unwrap_or(0);
        // On a blocking thread, because `df` is a fork and an exec and
        // benches/tick.rs part 6 measures it at 4 to 5 ms. That is a few
        // fetches worth of the loop's time, once every ten seconds, and there
        // is no reason to spend it on the thread doing the fetching.
        let dir = self.dir.clone();
        let free = tokio::task::spawn_blocking(move || probe::free_disk_bytes(&dir))
            .await
            .unwrap_or(None);
        Ok(Signals {
            unpublished_bytes,
            publish_lag_ms,
            // A platform with no reading leaves this at what a calm box looks
            // like. Not at zero: zero free bytes is the top rung, and a
            // missing reading has to mean nothing is wrong rather than
            // everything is.
            free_disk_bytes: free.unwrap_or(u64::MAX),
            // Doc 15.3's CPU ladder is about the extraction pool, which this
            // binary extracts inline rather than pooling, so there is no queue
            // to measure and no saturation to time.
            extract_queue: 0,
            extractor_saturated: false,
            rss_bytes: probe::rss_bytes().unwrap_or(0),
            // Off. Doc 03.4's 1.5 GB cap is umid's budget and `umi crawl` has
            // none of its own, and a ladder run against a budget nobody set
            // would stop a crawl over a number that was guessed. The reading
            // above is still taken and still logged, so the day umid arrives
            // with a budget this is one line.
            rss_budget_bytes: 0,
        })
    }
}

/// Doc 14.3's progress line.
///
/// `in flight` used to be the tick's lease count, which is the batch size and
/// is the same number every tick. It is now the mean occupancy of the fetch
/// window over the tick, which is the thing an operator is trying to read off
/// this line: a crawl configured for 256 that is running 30 is not fetching
/// slowly, it is not fetching, and no other field says so.
///
/// `ms per page` is what one lease cost the window from claim to answer. The
/// window over that number is the rate, so those two fields next to each other
/// are the whole of doc 16's gate 3.1 arithmetic, and the rest of the line is
/// what the crawl has to show for it. The two figures in brackets are the parts
/// of it that are not the page: the robots.txt the first lease on a host has to
/// fetch before it may ask for anything, and doc 07.6's politeness delay, which
/// a lease waits out twice on a host it has never seen.
fn progress(
    summary: &Summary,
    report: &TickReport,
    queued: u64,
    started_ms: u64,
    now_ms: u64,
) -> String {
    let elapsed = now_ms.saturating_sub(started_ms).max(1) as f64 / 1000.0;
    format!(
        "{} done  {:.0} in flight  {} queued  {:.1} p/s  {} ms per page ({} robots, {} polite)  \
         {} MB fetched  {} MB stored  {} failed  bottleneck: {}",
        summary.rows,
        report.window_mean(),
        queued,
        summary.rows as f64 / elapsed,
        report.lease_mean_ms(),
        report.robots_mean_ms(),
        report.waited_mean_ms(),
        summary.bytes_fetched / (1 << 20),
        summary.bytes_stored / (1 << 20),
        summary.failed,
        bottleneck(report),
    )
}

/// The line a watch prints while there is nothing to fetch.
///
/// Doc 14.1 asks for progress that means something, and for a watch the useful
/// thing is not pages per second, it is what the frontier is holding and how
/// much of it has ever been fetched. Doc 09's schedule is what decides when the
/// next one comes due, and a watch with a healthy schedule looks exactly like a
/// watch that has hung, so the log has to be the difference.
fn watching(summary: &Summary, stats: &StateStats, started_ms: u64, now_ms: u64) -> String {
    format!(
        "watching  {} scheduled  {} never fetched  {} in flight  {} rows this run  up {}",
        stats.urls_fetched,
        stats.urls_pending,
        stats.leases_in_flight,
        summary.rows,
        span(now_ms.saturating_sub(started_ms)),
    )
}

/// A duration in the shortest form that is still honest, for a log a person
/// reads a fortnight of.
fn span(ms: u64) -> String {
    let secs = ms / 1000;
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86_400 => format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60),
        _ => format!("{}d{:02}h", secs / 86_400, (secs % 86_400) / 3600),
    }
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
            allow_supervised: false,
            tabs: 0,
            seed: None,
            seeder: Vec::new(),
            sitemaps: None,
            out: None,
            publish: None,
            identity: None,
        }
    }
}
