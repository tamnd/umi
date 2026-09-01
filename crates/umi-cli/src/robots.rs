//! `umi robots`, doc 07.4's corpus fetched as a job of its own.
//!
//! A crawl already writes a robots row for every host it meets, and that is
//! where most of the corpus comes from. This command exists because of what
//! the meeting costs. Measured on server3 over four hundred cold hosts, a page
//! took 1865 ms of wall clock and 927 ms of that was the robots.txt in front of
//! it: DNS, TLS and a round trip for a file that is the same file for every
//! crawler on the web and does not change for a day. A fleet spreading across
//! a frontier of five hundred million URLs meets tens of thousands of new hosts
//! an hour and pays that every time.
//!
//! So this fetches robots.txt and nothing else, off a list of hosts, and
//! publishes the result to `open-index/umi-robots`. What that buys is two
//! things. The corpus itself, which doc 07.4 wants and which nobody else
//! publishes at scale. And a cache the fleet can start from, so a coordinator
//! meeting a host for the first time reads a row instead of opening a
//! connection.
//!
//! # Where the list comes from
//!
//! A file, or standard input, or a Hugging Face dataset. The last one is the
//! interesting case and it is the default: `open-index/ccrawl-domains` holds
//! 125 million registrable domains in harmonic centrality order, which is
//! close to the order a crawl meets them in, and streaming it from the hub
//! means the machine doing the fetching never stores the list. That matters
//! because the fleet's disks are a cache and not a library, and two gigabytes
//! of hostnames sitting on every box is two gigabytes not holding pages.
//!
//! # Politeness
//!
//! One request per host, which is the whole of it. There is no per host rate
//! limit here because there is no second request to a host to space out, and
//! the hosts arrive in an order that has nothing to do with which IP serves
//! them, so a run at a few hundred in flight is a few hundred different
//! origins. What the run does honour is doc 07.7's block list, which is pulled
//! from the published list when there is a hub to pull it from, because a
//! domain somebody asked us to leave alone should not be getting requests for
//! its robots.txt either.

use std::collections::HashSet;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Array as _;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use tokio::sync::mpsc;
use umi_crawl::{Clock, RobotsBuilder, RobotsRow, SegmentInfo, SegmentSink, SystemClock};
use umi_fetch::{FetchConfig, Ladder, Tier};
use umi_file::{StreamKind, WriterConfig};
use umi_publish::{BlockEntry, Hub};
use umi_state::{State, Stream};
use umi_state_sqlite::SqliteState;
use umi_types::HostId;

use crate::Error;
use crate::crawl::{self, Identity, Publishing, Stop, Summary};

/// The domain list a run reads when the operator does not name one.
///
/// Doc 12.4 does not own this repository. It is ccrawl's ranking of the web by
/// harmonic centrality, published as Parquet, and the reason it is the default
/// is that the order is the useful part: a run that stops after a million hosts
/// has the million hosts most likely to be worth having.
pub const DOMAINS: &str = "open-index/ccrawl-domains";

/// The column in the default list that holds a registrable domain.
pub const DOMAIN_COLUMN: &str = "domain";

/// How many fetches a run keeps in flight when nobody says otherwise.
///
/// Higher than a crawl's default because the work is different. A crawl at 256
/// is 256 sockets against hosts it is also rate limiting; this is one request
/// per host against hosts it will never speak to again, and most of the wall
/// clock is DNS and the connect. The tail of hosts that never answer is thick,
/// so the number that matters is how many of those a run can be waiting on at
/// once.
pub const CONCURRENCY: u16 = 256;

/// The directory a run writes into when nobody says where.
const DEFAULT_OUT: &str = "./umi-robots";

/// How many hosts the reader keeps queued ahead of the fetchers.
///
/// Enough that downloading the next Parquet part, which takes a few seconds,
/// does not leave the fetchers with nothing to do, and small enough that the
/// queue is not where the run's memory goes. A host is about thirty bytes, so
/// this is a quarter of a megabyte.
const QUEUE: usize = 8192;

/// How many rows go into the sink at a time.
///
/// The sink encodes a batch under a lock, so a flush per row would put a lock
/// acquisition on the hot path for no reason. This is about a second of work
/// at the rate a fast run manages.
const FLUSH: usize = 4096;

/// How often the run says what it is doing.
const PROGRESS: Duration = Duration::from_secs(10);

/// The profile a run leaves behind, so the directory is one `umi publish`
/// understands.
const PROFILE: &str = "\
# Written by umi robots. This directory holds doc 07.4's robots corpus and no
# pages, so the scope is the smallest one that parses: it exists to make the
# directory something umi publish and umi ls can read.
name = \"robots\"
max_depth = 0

[budget]
stop_when_idle = true
";

/// What the operator asked for.
#[derive(Clone, Debug)]
pub struct Options {
    /// A path, `-` for standard input, or a Hugging Face dataset repository.
    pub source: String,
    /// The column to read when the source is a dataset.
    pub column: String,
    /// Only files under this prefix, when the source is a dataset. A dataset
    /// with a year of snapshots in it is one repository and a run wants one
    /// snapshot.
    pub prefix: Option<String>,
    /// Where to write.
    pub out: Option<String>,
    /// Simultaneous in flight fetches.
    pub concurrency: u16,
    /// Stop after this many hosts.
    pub limit: Option<u64>,
    /// Skip this many hosts from the front of the list, which is how a run
    /// continues where the last one stopped.
    pub skip: u64,
    /// Stop after this long.
    pub max_duration: Option<String>,
    /// Publish to Hugging Face, and delete local copies once they verify.
    pub publish: Option<Publishing>,
    /// Doc 07.2's request signing.
    pub identity: Option<Identity>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            source: DOMAINS.to_owned(),
            column: DOMAIN_COLUMN.to_owned(),
            prefix: None,
            out: None,
            concurrency: CONCURRENCY,
            limit: None,
            skip: 0,
            max_duration: None,
            publish: None,
            identity: None,
        }
    }
}

/// Fetch robots.txt for every host in the source.
///
/// # Errors
///
/// [`Error::Io`] for the directory and the log, [`Error::Missing`] when the
/// source is neither a file nor a repository name, and whatever the hub, the
/// state ledger or the segment writer reports. A host that does not answer is
/// not an error: it is a row with a zero status in it.
pub fn robots(options: &Options) -> Result<Summary, Error> {
    let dir = PathBuf::from(
        options
            .out
            .clone()
            .unwrap_or_else(|| DEFAULT_OUT.to_owned()),
    );
    let layout = crawl::Layout::create(&dir)?;
    if !layout.profile.exists() {
        std::fs::write(&layout.profile, PROFILE).map_err(Error::Io)?;
    }
    let source = Source::parse(options)?;
    let max_duration = match &options.max_duration {
        Some(text) => {
            Some(umi_crawl::scope::parse_duration(text).map_err(|e| Error::Scope(e.to_string()))?)
        }
        None => None,
    };

    let clock = SystemClock;
    let started_ms = clock.now_ms();
    let signer = match &options.identity {
        Some(identity) => Some(Arc::new(identity.signer(started_ms)?)),
        None => None,
    };

    // Multi threaded, though nothing here extracts a page. The work is a few
    // hundred sockets and their TLS handshakes, and a single threaded runtime
    // would do every handshake on the thread that is also driving every read.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    let fetcher = Ladder::with_signer(FetchConfig::default(), signer)?;

    let state: Arc<dyn State> =
        Arc::new(SqliteState::open(&layout.state).map_err(|e| Error::State(e.to_string()))?);
    let sink = SegmentSink::create(
        &layout.robots_segments,
        SegmentInfo {
            stream: StreamKind::Robots,
            coordinator: crawl::coordinator_key(&layout.dir),
            ..SegmentInfo::default()
        },
        WriterConfig::default(),
    )
    .map_err(Error::Io)?;
    let publisher = match &options.publish {
        // The general corpus, not a focused one, because doc 12.4 puts the
        // robots family in one repository for everybody and a focused name
        // would only change where pages go, and this run has no pages.
        Some(publishing) => Some(crawl::publisher(
            publishing,
            &umi_crawl::Scope::general(),
            &layout,
        )?),
        None => None,
    };

    let mut log = crawl::Log::open(&layout.log)?;
    let mut summary = Summary::default();
    let mut counts = Counts::default();

    let result = runtime.block_on(async {
        if let Some(publisher) = &publisher {
            let org = options
                .publish
                .as_ref()
                .map_or(umi_publish::repo::ORG, |p| p.org.as_str());
            if publisher
                .announce(&crawl::meta_repo(org), clock.now_ms())
                .await?
            {
                log.line("the publishing key was added to the key directory")?;
            }
        }
        let blocked = blocked(publisher.as_ref(), options).await?;
        if !blocked.is_empty() {
            log.line(&format!(
                "{} domains on the published block list will not be asked",
                blocked.len()
            ))?;
        }

        log.line(&format!(
            "reading hosts from {source}, {} in flight",
            options.concurrency
        ))?;

        let (tx, mut rx) = mpsc::channel::<String>(QUEUE);
        let reader = tokio::spawn(source.drive(Admit::new(options, blocked), tx));

        let mut inflight = FuturesUnordered::new();
        let mut rows: Vec<RobotsRow> = Vec::with_capacity(FLUSH);
        let want = usize::from(options.concurrency.max(1));
        let mut drained = false;
        let mut said_ms = started_ms;
        let mut stopped = Stop::Idle;

        loop {
            // Top up the window. A queue that is empty while fetches are in
            // flight is the reader falling behind, and waiting on it here
            // would idle every socket that is already open, so the loop goes
            // back to collecting instead and asks again next time round.
            while !drained && inflight.len() < want {
                match rx.try_recv() {
                    Ok(host) => inflight.push(one(&fetcher, host, clock.now_ms())),
                    Err(mpsc::error::TryRecvError::Empty) => {
                        if !inflight.is_empty() {
                            break;
                        }
                        match rx.recv().await {
                            Some(host) => inflight.push(one(&fetcher, host, clock.now_ms())),
                            None => drained = true,
                        }
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => drained = true,
                }
            }
            if inflight.is_empty() {
                break;
            }

            if let Some(row) = inflight.next().await {
                counts.add(&row);
                summary.fetched += 1;
                if row.status == 0 {
                    summary.failed += 1;
                }
                rows.push(row);
            }

            if rows.len() >= FLUSH {
                flush(
                    &mut rows,
                    &sink,
                    &layout,
                    &*state,
                    publisher.as_ref(),
                    &mut summary,
                    &mut log,
                    clock.now_ms(),
                )
                .await?;
            }

            let now_ms = clock.now_ms();
            if now_ms.saturating_sub(said_ms) >= PROGRESS.as_millis() as u64 {
                said_ms = now_ms;
                log.line(&progress(&summary, &counts, started_ms, now_ms))?;
            }
            if let Some(limit) = max_duration
                && now_ms.saturating_sub(started_ms) >= limit.as_millis() as u64
            {
                stopped = Stop::Budget;
                break;
            }
        }

        // Dropping the receiver is how the reader is told to stop, which
        // matters on a run that ended on its budget with a hundred million
        // hosts still to come.
        drop(rx);
        // The tail rows first, then the open segment, then a second drain to
        // pick up what sealing it produced. The other order writes rows into a
        // sink that has already been finished.
        flush(
            &mut rows,
            &sink,
            &layout,
            &*state,
            publisher.as_ref(),
            &mut summary,
            &mut log,
            clock.now_ms(),
        )
        .await?;
        sink.finish().map_err(|e| Error::Crawl(e.to_string()))?;
        flush(
            &mut rows,
            &sink,
            &layout,
            &*state,
            publisher.as_ref(),
            &mut summary,
            &mut log,
            clock.now_ms(),
        )
        .await?;

        // The reader's own failure, which until here has only been a channel
        // that closed early. A run that stopped because the hub would not
        // serve the list has to say so rather than report a clean finish over
        // however many hosts it managed.
        match reader.await {
            Ok(result) => result?,
            Err(joined) => return Err(Error::Crawl(joined.to_string())),
        }
        // A run that stopped because it had all the hosts it asked for stopped
        // on a budget, the same as one that ran out of time, and doc 14.9 has
        // a code for that so a script can tell it from a list that ran out.
        if options.limit.is_some_and(|limit| summary.fetched >= limit) {
            stopped = Stop::Budget;
        }
        summary.stopped = stopped;
        Ok::<(), Error>(())
    });

    let now_ms = clock.now_ms();
    log.line(&progress(&summary, &counts, started_ms, now_ms))?;
    result?;
    Ok(summary)
}

/// Fetch one host's robots.txt and turn it into the published row.
///
/// T1 and only T1. Doc 05.8 moves a host up the ladder from what a crawl
/// learned about it, and a run that has never fetched a page from any of these
/// hosts has learned nothing, so escalating here would be spending a browser on
/// a guess. A host whose bot management refuses a plain client gets a row that
/// says so, and the crawl that meets it later fetches its robots.txt at the
/// tier it has by then earned.
async fn one(fetch: &Ladder, host: String, now_ms: u64) -> RobotsRow {
    let entry = umi_crawl::fetch_entry(fetch, &origin(&host), Tier::Plain, now_ms).await;
    // A domain list holds registrable domains and plenty of sites only exist
    // under `www`, so an apex that never answered at all is worth one more
    // request. Only when nothing came back: a 404 or a 403 is the apex
    // answering, and asking `www` after an answer would put two rows in the
    // corpus for one site.
    if entry.status == 0 && host.split('.').count() == 2 {
        let www = format!("www.{host}");
        let second = umi_crawl::fetch_entry(fetch, &origin(&www), Tier::Plain, now_ms).await;
        if second.status != 0 {
            return RobotsRow::build(&www, &second);
        }
    }
    RobotsRow::build(&host, &entry)
}

/// The origin to ask, which is https and nothing else.
///
/// Doc 07.5 has the fetcher fall back to plain HTTP for a host that does not
/// speak TLS, so starting at https costs a redirect at worst and asking over
/// http first would send every request on the open internet for no reason.
fn origin(host: &str) -> String {
    format!("https://{host}")
}

/// Write what has been fetched, and publish whatever that sealed.
///
/// The same two paths a crawl has. Without a publisher the segment becomes a
/// Parquet file under `data/robots` and stays there. With one it gets a ledger
/// row and doc 12.2's pipeline takes it from there, including deleting the
/// local copy once the four conditions in doc 12.7 hold, which is the whole
/// point on a box whose disk is a cache.
#[expect(
    clippy::too_many_arguments,
    reason = "the alternative is a struct that exists to be one call's argument list"
)]
async fn flush(
    rows: &mut Vec<RobotsRow>,
    sink: &SegmentSink,
    layout: &crawl::Layout,
    state: &dyn State,
    publisher: Option<&umi_publish::Publisher>,
    summary: &mut Summary,
    log: &mut crawl::Log,
    now_ms: u64,
) -> Result<(), Error> {
    if !rows.is_empty() {
        summary.rows += rows.len() as u64;
        sink.write::<RobotsBuilder>(rows)
            .map_err(|e| Error::Crawl(e.to_string()))?;
        rows.clear();
    }
    let sealed = sink.sealed();
    if sealed.is_empty() {
        return Ok(());
    }
    let Some(publisher) = publisher else {
        return crawl::keep(&sealed, &layout.robots_data, &mut None, summary);
    };
    crawl::record(&sealed, Stream::Robots, state, now_ms).await?;
    let (done, failed) = publisher.drain(state, now_ms).await?;
    for published in &done {
        summary.files += 1;
        summary.published += 1;
        summary.bytes_stored += published.bytes;
        crawl::receipt(&layout.published, published)?;
        if let Some(blocked) = &published.blocked {
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

/// Doc 07.7's block list, as registrable domains, or nothing when there is no
/// hub to ask.
///
/// A run without `--publish` has no token and no client, and going to the
/// network anyway to build one would mean a command that reads a list off the
/// disk still needs the internet to start. That is the honest gap: a local run
/// applies whatever blocks are in its own state file and a publishing run
/// applies the fleet's.
async fn blocked(
    publisher: Option<&umi_publish::Publisher>,
    options: &Options,
) -> Result<HashSet<String>, Error> {
    let Some(publisher) = publisher else {
        return Ok(HashSet::new());
    };
    let org = options
        .publish
        .as_ref()
        .map_or(umi_publish::repo::ORG, |p| p.org.as_str());
    let published = umi_publish::published_blocks(publisher.hub(), &crawl::meta_repo(org)).await?;
    Ok(published
        .iter()
        .filter(|entry: &&BlockEntry| entry.lifted_ms.is_none())
        .map(|entry| entry.domain.to_ascii_lowercase())
        .collect())
}

/// Where the hosts come from.
enum Source {
    /// A file of hosts, or standard input.
    Lines(Option<PathBuf>),
    /// Parquet files on the hub, read one at a time.
    Dataset {
        repo: String,
        prefix: String,
        column: String,
    },
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lines(None) => write!(f, "standard input"),
            Self::Lines(Some(path)) => write!(f, "{}", path.display()),
            Self::Dataset { repo, column, .. } => write!(f, "{repo}, column {column}"),
        }
    }
}

impl Source {
    /// Work out what the operator meant.
    ///
    /// A path that exists is a path, which is the rule `umi ls` uses and it is
    /// the right way round: a directory that is really there is never a
    /// repository name somebody mistyped. After that a name with a slash in it
    /// is a repository and anything else is a mistake worth naming, because
    /// guessing an organisation onto a bare word would turn a typo into a
    /// request for somebody else's dataset.
    fn parse(options: &Options) -> Result<Self, Error> {
        if options.source == "-" {
            return Ok(Self::Lines(None));
        }
        let path = Path::new(&options.source);
        if path.exists() {
            return Ok(Self::Lines(Some(path.to_path_buf())));
        }
        if options.source.contains('/') {
            return Ok(Self::Dataset {
                repo: options.source.clone(),
                prefix: options.prefix.clone().unwrap_or_default(),
                column: options.column.clone(),
            });
        }
        Err(Error::Missing(format!(
            "{:?} is not a file and is not an org/name repository",
            options.source
        )))
    }

    /// Send every host in the source, in order, until it runs out or nobody is
    /// listening any more.
    async fn drive(self, mut admit: Admit, tx: mpsc::Sender<String>) -> Result<(), Error> {
        match self {
            Self::Lines(path) => lines(path, admit, tx).await,
            Self::Dataset {
                repo,
                prefix,
                column,
            } => dataset(&repo, &prefix, &column, &mut admit, &tx).await,
        }
    }
}

/// Read hosts out of a file or standard input.
///
/// On a blocking thread, because the file may be gigabytes and reading it a
/// line at a time on the runtime would put a disk read between two fetch
/// completions. `blocking_send` is what a blocking thread has instead of an
/// await, and it applies the same backpressure.
async fn lines(
    path: Option<PathBuf>,
    mut admit: Admit,
    tx: mpsc::Sender<String>,
) -> Result<(), Error> {
    let joined = tokio::task::spawn_blocking(move || {
        let reader: Box<dyn BufRead> = match &path {
            Some(path) => Box::new(std::io::BufReader::new(std::fs::File::open(path)?)),
            None => Box::new(std::io::BufReader::new(std::io::stdin())),
        };
        for line in reader.lines() {
            let line = line?;
            let Some(host) = admit.take(&line) else {
                if admit.done() {
                    break;
                }
                continue;
            };
            if tx.blocking_send(host).is_err() {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    })
    .await;
    match joined {
        Ok(result) => result.map_err(Error::Io),
        Err(cause) => Err(Error::Crawl(cause.to_string())),
    }
}

/// Read hosts out of a Hugging Face dataset, one Parquet file at a time.
///
/// The file is downloaded whole and decoded in memory. A part of the default
/// list is five million rows and a couple of hundred megabytes, which is a few
/// seconds of network against the hours of fetching those five million hosts
/// take, and holding one part costs less than the queue in front of it does
/// once the column is projected away.
async fn dataset(
    repo: &str,
    prefix: &str,
    column: &str,
    admit: &mut Admit,
    tx: &mpsc::Sender<String>,
) -> Result<(), Error> {
    // No token. Everything this reads is public, and a run that needed a
    // credential to read a public list would be a run a stranger could not
    // reproduce.
    let hub = Hub::new("")?;
    let mut files: Vec<String> = hub
        .list(repo, prefix)
        .await?
        .into_iter()
        .map(|remote| remote.path)
        .filter(|path| path.ends_with(".parquet"))
        .collect();
    if files.is_empty() {
        return Err(Error::Missing(format!(
            "{repo} has no Parquet files under {prefix:?}"
        )));
    }
    // Name order, which for every list published this way is also the order
    // the rows were ranked in. A run that stops early has to stop at the same
    // place every time or `--skip` means nothing.
    files.sort();

    for path in files {
        let Some(bytes) = hub.read(repo, &path).await? else {
            continue;
        };
        // Decoding five million rows is a second of CPU, and doing it on the
        // runtime would stall every socket that is open at the time.
        let owned = column.to_owned();
        let hosts = tokio::task::spawn_blocking(move || hosts_in(bytes, &owned))
            .await
            .map_err(|cause| Error::Crawl(cause.to_string()))??;
        for host in hosts {
            let Some(host) = admit.take(&host) else {
                if admit.done() {
                    return Ok(());
                }
                continue;
            };
            if tx.send(host).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// One column of one Parquet file, as strings.
///
/// Projected down to the column that was asked for before anything is decoded,
/// so a list with a page rank and a host count in it costs what the domain
/// column costs and not what the file does.
fn hosts_in(bytes: Vec<u8>, column: &str) -> Result<Vec<String>, Error> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))?;
    let schema = builder.parquet_schema();
    let index = builder
        .schema()
        .index_of(column)
        .map_err(|_| Error::NoColumn(column.to_owned()))?;
    let mask = ProjectionMask::roots(schema, [index]);
    let reader = builder.with_projection(mask).build()?;

    let mut found = Vec::new();
    for batch in reader {
        let batch = batch?;
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| Error::NoColumn(column.to_owned()))?;
        for i in 0..values.len() {
            if values.is_valid(i) {
                found.push(values.value(i).to_owned());
            }
        }
    }
    Ok(found)
}

/// Which hosts get asked, and how many.
///
/// One place rather than two, because both readers need the same four
/// decisions and a run whose file source deduplicated differently from its
/// dataset source would produce two different corpora off the same list.
struct Admit {
    seen: HashSet<HostId>,
    blocked: HashSet<String>,
    skip: u64,
    limit: Option<u64>,
    passed: u64,
    sent: u64,
}

impl Admit {
    fn new(options: &Options, blocked: HashSet<String>) -> Self {
        Self {
            seen: HashSet::new(),
            blocked,
            skip: options.skip,
            limit: options.limit,
            passed: 0,
            sent: 0,
        }
    }

    /// The host to ask, or nothing when this line is not one to ask.
    fn take(&mut self, line: &str) -> Option<String> {
        if self.done() {
            return None;
        }
        let host = host_of(line)?;
        // Skipped before the duplicate check and before the limit, so that two
        // runs with `--skip` a million apart cover the list exactly once
        // between them. Counting only the lines that would have been asked is
        // what makes the number mean the same thing on both runs.
        self.passed += 1;
        if self.passed <= self.skip {
            return None;
        }
        if self.blocked.contains(&host) || blocked_under(&self.blocked, &host) {
            return None;
        }
        if !self.seen.insert(HostId::derive(host.as_bytes())) {
            return None;
        }
        self.sent += 1;
        Some(host)
    }

    /// Whether the run has all the hosts it asked for.
    fn done(&self) -> bool {
        self.limit.is_some_and(|limit| self.sent >= limit)
    }
}

/// Whether a host sits under a blocked registrable domain.
///
/// A block names a domain and doc 07.7 means the whole of it, so a block on
/// `example.com` covers `www.example.com` and every other subdomain. Walked
/// rather than parsed with a public suffix list, because the entries are
/// already registrable domains and the question is only whether this host ends
/// in one of them on a label boundary.
fn blocked_under(blocked: &HashSet<String>, host: &str) -> bool {
    let mut rest = host;
    while let Some((_, parent)) = rest.split_once('.') {
        if blocked.contains(parent) {
            return true;
        }
        rest = parent;
    }
    false
}

/// A line of the list as a host, or nothing when it is not one.
///
/// Lists are written by people and by other tools, so this takes a bare domain,
/// a host with a scheme on it, a URL with a path, a comment and a blank line,
/// and gives back the host in the one form the fetcher wants. Anything with no
/// dot in it is dropped: a single label is a machine on somebody's LAN and not
/// a host on the web, and a run that asked one of those would be asking the
/// resolver about the machine it is running on.
fn host_of(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let rest = match line.split_once("://") {
        Some((_, rest)) => rest,
        None => line,
    };
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(rest)
        .split('@')
        .next_back()
        .unwrap_or(rest);
    // A port is not part of the host and doc 11.2 drops it from a URL key, so
    // a list with one in it should not produce a second row for the same site.
    let host = host.split(':').next().unwrap_or(host);
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || !host.contains('.') || host.contains(' ') {
        return None;
    }
    Some(host)
}

/// What the run has seen, by what the file said.
///
/// Four numbers rather than a histogram of statuses, because these are the four
/// a person watching the run is asking about: how many hosts published rules,
/// how many said they have none, how many refused, and how many never answered
/// at all. A corpus where the last of those is climbing is a run that is
/// hitting the network's limits rather than the web's.
#[derive(Default)]
struct Counts {
    rules: u64,
    none: u64,
    refused: u64,
    silent: u64,
}

impl Counts {
    fn add(&mut self, row: &RobotsRow) {
        match row.status {
            0 => self.silent += 1,
            200..=299 if row.body.is_some() => self.rules += 1,
            // A 429 is a 4xx that means not now rather than there is no file,
            // and RFC 9309 groups it with the 5xx for exactly that reason.
            429 => self.refused += 1,
            400..=499 => self.none += 1,
            _ => self.refused += 1,
        }
    }
}

/// The line the run prints while it works.
fn progress(summary: &Summary, counts: &Counts, started_ms: u64, now_ms: u64) -> String {
    let elapsed = now_ms.saturating_sub(started_ms).max(1) as f64 / 1000.0;
    format!(
        "{} hosts  {:.1} h/s  {} with rules  {} with none  {} refused  {} silent  \
         {} rows  {} files  {} MB stored",
        summary.fetched,
        summary.fetched as f64 / elapsed,
        counts.rules,
        counts.none,
        counts.refused,
        counts.silent,
        summary.rows,
        summary.files,
        summary.bytes_stored / (1 << 20),
    )
}

#[cfg(test)]
#[path = "robots_tests.rs"]
mod tests;
