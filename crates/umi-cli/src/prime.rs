//! `umi prime`, a crawl's robots cache filled from the published corpus.
//!
//! The other half of `umi robots`. That command asks hosts for their
//! robots.txt and publishes what they served. This reads the published result
//! back into a crawl directory's state, so the run that follows finds an
//! answer in a table instead of opening a connection.
//!
//! # What it is worth
//!
//! Measured on server2 on 2026-09-08, six minutes at 1024 in flight against
//! the gate 3.1 seed: 60,699 rows at 152.9 pages a second, 3329 ms of wall
//! clock per page, and 1765 of those milliseconds were the robots.txt in front
//! of the page. That is 53 percent of the fetch budget spent on a file that is
//! the same file for every crawler on the web and that we have already
//! published 42.8 million of. The run met 69,984 distinct hosts in those six
//! minutes and loaded exactly zero robots documents, because it started on an
//! empty state directory and had nothing to load.
//!
//! # Why there is a body cap
//!
//! A robots.txt has a median size of 381 bytes and a mean of 8210. The mean is
//! the interesting number because of what makes it: of the 46,137 hosts in
//! that run that served a body at all, 1513 of them, 3.3 percent, held 305 of
//! the 379 megabytes. Eighty one percent of the bytes belong to three percent
//! of the hosts.
//!
//! So the cap is where the value is. Importing every host under eight
//! kilobytes keeps 92 percent of them for a tenth of the disk, which turns the
//! whole corpus from something near 250 gigabytes of body text into something
//! near 25. A host over the cap is left out and gets asked the way it is asked
//! today, so the cap costs coverage and never correctness.
//!
//! # Dates
//!
//! A row is imported with the expiry its own fetch date gives it, which is
//! `fetched_at_ms` plus `--ttl`, and not with an expiry counted from now. Doc
//! 07.4 gives a robots.txt a day, and a corpus published last week is a week
//! old however it is loaded. Rows already past their expiry are counted and
//! skipped rather than quietly renewed, so a prime against a stale corpus says
//! it imported nothing instead of teaching the crawl to act on old rules. An
//! operator who has decided a longer life is acceptable passes a longer
//! `--ttl` and can see in the report exactly how much of the import that
//! decision is carrying.
//!
//! # Not overwriting a fresher answer
//!
//! A crawl that has already run in this directory has rows of its own, and
//! some of them are newer than the corpus. Each batch reads what is there
//! before it writes, and a published row loses to a local row that was fetched
//! later. Without that a prime run in the middle of a crawl's life would
//! quietly replace today's answers with last week's.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Array as _;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use umi_crawl::{Clock as _, SystemClock};
use umi_publish::{Hub, HubFile, footer, read_column};
use umi_state::{RobotsDoc, State};
use umi_state_sqlite::SqliteState;
use umi_types::{Digest, HostId};

use crate::Error;
use crate::crawl::{self, Layout, Publishing};

/// The corpus a prime reads when the operator does not name one.
///
/// The same repository `umi robots --known` reads, spelled out for the same
/// reason it is spelled out there: a constant cannot format, and this is the
/// literal that `umi_publish::repo::ORG` and the robots family's stem produce.
pub const CORPUS: &str = "open-index/umi-robots";

/// Bodies longer than this are left in the corpus.
///
/// Eight kilobytes, from the distribution in this module's header. It keeps 92
/// percent of the hosts that serve a body and a tenth of the bytes, and a host
/// over it is asked during the crawl exactly as it is today.
pub const MAX_BODY: usize = 8 * 1024;

/// How long an imported answer is good for, in hours.
///
/// Doc 07.4's day. Counted from the fetch the corpus records and not from the
/// import, so this is the age at which a published row stops being usable
/// rather than a lease of life the import hands out.
pub const TTL_HOURS: u64 = 24;

/// How many published files a run reads at once.
///
/// Lower than the host column reads in `umi robots`, because this reads the
/// body column and that is the whole file rather than a couple of megabytes of
/// it. Four files in flight keeps the link busy without holding four decoded
/// bodies columns in memory next to a sqlite writer.
pub const FILES: usize = 4;

/// The column holding a hostname.
const HOST_COLUMN: &str = "host";

/// The column holding when the corpus fetched the file.
const FETCHED_COLUMN: &str = "fetched_at_ms";

/// The column holding the HTTP status.
const STATUS_COLUMN: &str = "status";

/// The column holding the text the host served.
const BODY_COLUMN: &str = "body";

/// What the operator asked for.
#[derive(Clone, Debug)]
pub struct Options {
    /// The crawl directory to fill.
    pub dir: PathBuf,
    /// The published corpus to read.
    pub corpus: String,
    /// How many published files to read, newest last. `None` reads all of
    /// them.
    pub files: Option<usize>,
    /// Bodies longer than this are skipped.
    pub max_body: usize,
    /// How long after its own fetch a published row stays usable, in hours.
    pub ttl_hours: u64,
    /// Read the corpus, say what would be imported, and write nothing.
    pub dry_run: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            corpus: CORPUS.to_owned(),
            files: None,
            max_body: MAX_BODY,
            ttl_hours: TTL_HOURS,
            dry_run: false,
        }
    }
}

/// What a run did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Primed {
    /// Published files that were read.
    pub files: usize,
    /// Rows the corpus held in those files.
    pub seen: u64,
    /// Rows now in the crawl's state because of this run.
    pub imported: u64,
    /// Rows whose own fetch date puts them past `--ttl` already.
    pub stale: u64,
    /// Rows whose body is longer than `--max-body`.
    pub oversized: u64,
    /// Rows the directory already had a later answer for.
    pub fresher: u64,
}

/// Fill a crawl directory's robots cache from a published corpus.
///
/// # Errors
///
/// [`Error::Io`] when the directory has no `profile.toml`, which is what tells
/// a crawl directory apart from any other directory, [`Error::NothingToDo`]
/// when the corpus has no files in it, and whatever the hub or the state store
/// reports.
pub fn prime(options: &Options, publishing: Option<&Publishing>) -> Result<Primed, Error> {
    let layout = Layout::create(&options.dir)?;
    crawl::profile_of(&options.dir)?;
    let state: Arc<dyn State> =
        Arc::new(SqliteState::open(&layout.state).map_err(|e| Error::State(e.to_string()))?);
    let mut log = crawl::Log::open(&layout.log)?;

    // Single threaded for the reason `umi warm` is single threaded. The work
    // is ranged reads against one hub feeding one sqlite writer, and a second
    // runtime thread would have nothing to do but exist.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;

    runtime.block_on(read(options, &*state, publishing, &mut log))
}

/// The whole of a run, once there is a runtime to do it on.
async fn read(
    options: &Options,
    state: &dyn State,
    publishing: Option<&Publishing>,
    log: &mut crawl::Log,
) -> Result<Primed, Error> {
    // A token when the run has one and none when it does not. The corpus is
    // public so an anonymous read works, and the token is here for the reason
    // the silent list carries one: reading many files at once anonymously gets
    // 429s and a note from Hugging Face asking us to log in.
    let hub = Hub::new(publishing.map_or("", |p| p.token.as_str()))?;
    let mut files: Vec<String> = hub
        .list(&options.corpus, "data")
        .await?
        .into_iter()
        .map(|remote| remote.path)
        .filter(|path| path.ends_with(".parquet"))
        .collect();
    if files.is_empty() {
        return Err(Error::NothingToDo("the corpus has no published files"));
    }
    files.sort();
    // Newest last, so a run with a file limit takes the newest ones. The names
    // are ULIDs under a dated directory, so sorted is oldest first and the
    // limit has to come off the end.
    if let Some(limit) = options.files
        && files.len() > limit
    {
        files.drain(..files.len() - limit);
    }
    log.line(&format!(
        "reading {} published files from {}",
        files.len(),
        options.corpus
    ))?;

    let ttl_ms = options.ttl_hours.saturating_mul(60 * 60 * 1000);
    let now_ms = SystemClock.now_ms();
    let mut primed = Primed::default();
    let mut chunks = files.chunks(FILES);
    for batch in &mut chunks {
        let mut reading = FuturesUnordered::new();
        for path in batch {
            reading.push(one_file(
                &hub,
                &options.corpus,
                path,
                options,
                ttl_ms,
                now_ms,
                state,
            ));
        }
        while let Some(found) = reading.next().await {
            match found {
                Ok(done) => {
                    primed.files += 1;
                    primed.seen += done.seen;
                    primed.imported += done.imported;
                    primed.stale += done.stale;
                    primed.oversized += done.oversized;
                    primed.fresher += done.fresher;
                }
                // One unreadable file does not stop a run, for the reason a
                // warm does not stop: what it costs is coverage, and the hosts
                // in it get asked during the crawl the way they are asked
                // today.
                Err(cause) => log.line(&format!("a published file did not import: {cause}"))?,
            }
        }
    }
    log.line(&format!(
        "{} rows imported from {} read, {} already stale, {} over {} bytes, {} the directory had fresher",
        primed.imported,
        primed.seen,
        primed.stale,
        primed.oversized,
        options.max_body,
        primed.fresher
    ))?;
    Ok(primed)
}

/// Import one published file.
///
/// Four projections of the same footer rather than one read of the file. Three
/// of the columns are a few bytes a row and the fourth is the body, so this
/// moves the bytes that matter and skips the six columns of parsed summary
/// that the crawl reparses from the body anyway.
async fn one_file(
    hub: &Hub,
    repo: &str,
    path: &str,
    options: &Options,
    ttl_ms: u64,
    now_ms: u64,
    state: &dyn State,
) -> Result<Primed, Error> {
    let source = HubFile::open(hub, repo, path).await?;
    let metadata = Arc::new(footer(&source).await?);
    let hosts = strings(
        &read_column(&source, &metadata, HOST_COLUMN).await?,
        HOST_COLUMN,
    )?;
    let fetched = fetched_at(&read_column(&source, &metadata, FETCHED_COLUMN).await?)?;
    let statuses = statuses(&read_column(&source, &metadata, STATUS_COLUMN).await?)?;
    let bodies = strings(
        &read_column(&source, &metadata, BODY_COLUMN).await?,
        BODY_COLUMN,
    )?;
    if hosts.len() != fetched.len() || hosts.len() != statuses.len() || hosts.len() != bodies.len()
    {
        return Err(Error::Arrow(
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "{path} has {} hosts, {} dates, {} statuses and {} bodies",
                hosts.len(),
                fetched.len(),
                statuses.len(),
                bodies.len()
            )),
        ));
    }

    let mut done = Primed::default();
    let mut batch: Vec<RobotsDoc> = Vec::with_capacity(umi_state::BATCH);
    for i in 0..hosts.len() {
        done.seen += 1;
        let row = Row {
            host: hosts[i].as_deref(),
            fetched_ms: fetched[i],
            status: statuses[i],
            body: bodies[i].as_deref(),
        };
        if let Some(doc) = candidate(&row, ttl_ms, now_ms, options.max_body, &mut done) {
            batch.push(doc);
        }
        if batch.len() == umi_state::BATCH {
            write(state, &mut batch, options, &mut done).await?;
        }
    }
    write(state, &mut batch, options, &mut done).await?;
    Ok(done)
}

/// One row of the corpus, as the four columns give it.
///
/// A struct rather than four arguments because [`candidate`] would be at eight
/// otherwise, and because the four are only ever passed together.
struct Row<'a> {
    /// The hostname, or nothing when the column was null.
    host: Option<&'a str>,
    /// When the corpus fetched the file.
    fetched_ms: u64,
    /// The status it came back with, zero for no answer at all.
    status: u16,
    /// The text the host served, when it served one.
    body: Option<&'a str>,
}

/// The document one corpus row imports as, or nothing and a reason counted.
///
/// The three ways a row does not import are a null hostname, an age past the
/// ttl, and a body over the cap. None of them is an error: each one leaves a
/// host that the crawl asks itself, which is what every host does today.
fn candidate(
    row: &Row<'_>,
    ttl_ms: u64,
    now_ms: u64,
    max_body: usize,
    done: &mut Primed,
) -> Option<RobotsDoc> {
    let host = row.host?;
    let expires_ms = row.fetched_ms.saturating_add(ttl_ms);
    if expires_ms <= now_ms {
        done.stale += 1;
        return None;
    }
    if row.body.is_some_and(|body| body.len() > max_body) {
        done.oversized += 1;
        return None;
    }
    Some(RobotsDoc {
        host: HostId::derive(host.as_bytes()),
        // Over the bytes the host served, which is what the fetch hashed, so a
        // row imported here and the same row fetched later carry the same
        // digest and a conditional refetch can tell that they match.
        digest: Digest::derive(row.body.unwrap_or_default().as_bytes()),
        fetched_ms: row.fetched_ms,
        expires_ms,
        status: row.status,
        body: row.body.map(ToOwned::to_owned),
    })
}

/// Store one batch, leaving alone any host the directory answers better.
///
/// The read before the write is what stops a prime from undoing a crawl. A
/// local row fetched after the published one is a later answer to the same
/// question, and `put_robots` replaces rather than merges, so without this the
/// corpus would win every time regardless of which of the two is true now.
async fn write(
    state: &dyn State,
    batch: &mut Vec<RobotsDoc>,
    options: &Options,
    done: &mut Primed,
) -> Result<(), Error> {
    if batch.is_empty() {
        return Ok(());
    }
    let hosts: Vec<HostId> = batch.iter().map(|doc| doc.host).collect();
    let held = state
        .robots(&hosts)
        .await
        .map_err(|e| Error::State(e.to_string()))?;
    batch.retain(|doc| {
        let fresher = held
            .iter()
            .any(|row| row.host == doc.host && row.fetched_ms >= doc.fetched_ms);
        if fresher {
            done.fresher += 1;
        }
        !fresher
    });

    done.imported += batch.len() as u64;
    if !options.dry_run && !batch.is_empty() {
        state
            .put_robots(batch)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
    }
    batch.clear();
    Ok(())
}

/// One string column, flattened across its row groups.
///
/// The whole column and not an iterator, because the four columns are walked
/// in step and the check that they are the same length is the thing that says
/// the nth of each is the same row.
fn strings(
    batches: &[arrow::array::RecordBatch],
    column: &str,
) -> Result<Vec<Option<String>>, Error> {
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| Error::NoColumn(column.to_owned()))?;
        for i in 0..values.len() {
            out.push(values.is_valid(i).then(|| values.value(i).to_owned()));
        }
    }
    Ok(out)
}

/// The fetch date column. A null reads as zero, which imports as stale.
fn fetched_at(batches: &[arrow::array::RecordBatch]) -> Result<Vec<u64>, Error> {
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| Error::NoColumn(FETCHED_COLUMN.to_owned()))?;
        for i in 0..values.len() {
            out.push(if values.is_valid(i) {
                values.value(i)
            } else {
                0
            });
        }
    }
    Ok(out)
}

/// The status column. A null reads as zero, which is the corpus spelling for a
/// host that answered nothing, and that is the safe way to be wrong: a zero
/// parses to the ruleset a failed fetch gets rather than to one that allows.
fn statuses(batches: &[arrow::array::RecordBatch]) -> Result<Vec<u16>, Error> {
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt16Array>()
            .ok_or_else(|| Error::NoColumn(STATUS_COLUMN.to_owned()))?;
        for i in 0..values.len() {
            out.push(if values.is_valid(i) {
                values.value(i)
            } else {
                0
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
#[path = "prime_tests.rs"]
mod tests;
