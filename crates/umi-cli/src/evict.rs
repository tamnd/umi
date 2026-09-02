//! `umi evict`, doc 08.6's backlog on its way off the local disk.
//!
//! A crawl admits far more URLs than it fetches. Every link on every page is a
//! candidate, most of them are never due, and all of them sit in the ledger
//! taking room on a disk doc 15 says is a cache. At 100 billion URLs the state
//! is around 2 TB against 342 GB of free local disk on the fleet, so the
//! backlog has to live somewhere else and come back a piece at a time.
//!
//! Somewhere else is `open-index/umi-frontier-*`, published the same way pages
//! and robots are published, with the same manifests, the same digests and the
//! same signatures. That is a change from the version of doc 08.6 that put one
//! object per domain in a private bucket, and it is better for three reasons.
//! A domain becomes a range of rows in a sorted file rather than an object, so
//! warming a site is a byte range against one row group. 100 billion URLs is
//! hundreds of thousands of files rather than 200 million objects. And a
//! backlog is worth more to everybody else than it is to us, so publishing it
//! costs nothing and hands doc 04's fetcher protocol a bulk work transport for
//! free.
//!
//! # The order of the five steps
//!
//! Spill, publish, verify, index, unload. Nothing local is deleted until the
//! copy that replaces it is on the hub and its read back digest has matched,
//! which is doc 12.7's fourth condition applied to state instead of to pages.
//! The step that has to be durable is the index write, because a crash between
//! the rows going away and the pointer arriving leaves a domain that is neither
//! local nor findable. A crash the other way round leaves a domain that is both
//! local and pointed at, which is only wasted disk.
//!
//! A run that publishes nothing deletes nothing. That is worth saying plainly
//! because it is the difference between this command and a command that frees
//! space: if the hub is unreachable, this writes a segment, fails to upload it,
//! and leaves every domain exactly where it was.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use umi_crawl::{Clock, Placement, SegmentInfo, SegmentSink, SystemClock};
use umi_file::{StreamKind, WriterConfig};
use umi_state::{Shard, State, Stream};
use umi_state_sqlite::SqliteState;
use umi_types::PldId;

use crate::Error;
use crate::crawl::{self, Layout, Publishing, Summary};

/// How many domains a run moves when the operator does not say.
///
/// A thousand, which at a few hundred rows each is a segment or two and a few
/// minutes of uploading. Small enough that a run that goes wrong went wrong on
/// a thousand domains rather than on the whole store, and a scheduler under
/// disk pressure calls this repeatedly rather than once.
pub const DOMAINS: usize = 1000;

/// What the operator asked for.
#[derive(Clone, Debug)]
pub struct Options {
    /// The crawl directory to work on.
    pub dir: PathBuf,
    /// How many domains to move.
    pub limit: usize,
    /// Say what would move and move nothing.
    pub dry_run: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            limit: DOMAINS,
            dry_run: false,
        }
    }
}

/// Move the coldest part of a crawl directory's backlog onto the hub.
///
/// # Errors
///
/// [`Error::Io`] when the directory has no `profile.toml`, which is what tells
/// a crawl directory apart from any other directory, [`Error::NothingToDo`]
/// when the store holds no domains, and whatever the hub, the sink or the
/// state ledger reports.
pub fn evict(options: &Options, publishing: &Publishing) -> Result<Summary, Error> {
    let scope = crawl::profile_of(&options.dir)?;
    let layout = Layout::create(&options.dir)?;
    let state: Arc<dyn State> =
        Arc::new(SqliteState::open(&layout.state).map_err(|e| Error::State(e.to_string()))?);
    let clock = SystemClock;
    let mut log = crawl::Log::open(&layout.log)?;

    // Single threaded, like `umi publish` and for the same reason. The work is
    // reading rows, encoding them, and one upload at a time against one hub,
    // so a second runtime thread would have nothing to do but exist.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;

    runtime.block_on(async {
        let plds = state
            .resident()
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        if plds.is_empty() {
            return Err(Error::NothingToDo("this store holds no domains"));
        }
        let chosen: Vec<PldId> = plds.into_iter().take(options.limit).collect();
        if options.dry_run {
            log.line(&format!("{} domains would move", chosen.len()))?;
            return Ok(Summary::default());
        }

        let publisher = crawl::publisher(publishing, &scope, &layout)?;
        let now_ms = clock.now_ms();
        if publisher
            .announce(&crawl::meta_repo(&publishing.org), now_ms)
            .await?
        {
            log.line("the publishing key was added to the key directory")?;
        }

        let sink = frontier_sink(&layout.frontier_segments)?;
        let placed = spill(&*state, &chosen, &sink, &mut log).await?;
        if placed.is_empty() {
            return Err(Error::NothingToDo("every resident domain is already empty"));
        }

        // Sealed before recording, because an open segment is a file nobody
        // has finished writing and the publisher works off the ledger.
        sink.finish().map_err(|e| Error::Crawl(e.to_string()))?;
        let sealed = sink.sealed();
        crawl::record(&sealed, Stream::Frontier, &*state, now_ms).await?;

        let mut summary = Summary::default();
        let (done, failed) = publisher.drain(&*state, now_ms).await?;
        for published in &done {
            summary.files += 1;
            summary.published += 1;
            summary.rows += published.rows;
            summary.bytes_stored += published.bytes;
            crawl::receipt(&layout.published, published)?;
        }
        for (segment, error) in &failed {
            summary.files += 1;
            log.line(&format!("{segment} did not publish: {error}"))?;
        }

        // Only the domains whose segment really landed. A segment that failed
        // to upload leaves its domains resident, which is the correct outcome
        // and not a partial one: nothing was written down about them, so the
        // next run picks them up again from the top.
        let landed: Vec<Shard> = placed
            .iter()
            .filter(|(_, place)| done.iter().any(|p| p.segment == place.segment))
            .map(|(pld, place)| Shard {
                pld: *pld,
                segment: place.segment,
                first_group: place.first_group,
                last_group: place.last_group,
                rows: place.rows,
                evicted_at_ms: now_ms,
            })
            .collect();
        if landed.is_empty() {
            log.line("nothing published, so nothing was dropped locally")?;
            return finish(summary, failed.into_iter().next().map(|(_, e)| e.into()));
        }

        // Index first, then drop. A crash between the two leaves a domain that
        // is both local and pointed at, which costs disk. The other order
        // leaves one that is neither, which loses the backlog.
        state
            .put_shards(&landed)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        let plds: Vec<PldId> = landed.iter().map(|shard| shard.pld).collect();
        let unloaded = state
            .unload(&plds)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        let rows: u64 = landed
            .iter()
            .filter(|shard| unloaded.contains(&shard.pld))
            .map(|shard| shard.rows)
            .sum();
        log.line(&format!(
            "{} domains and {rows} rows moved to the hub, {} kept because a lease is in flight",
            unloaded.len(),
            landed.len() - unloaded.len()
        ))?;
        finish(summary, failed.into_iter().next().map(|(_, e)| e.into()))
    })
}

/// The summary, or the first upload failure if there was one.
///
/// The first rather than a count, because doc 14.9's exit code depends on which
/// kind of failure it was and a count has no kind. The rest are in the log.
fn finish(summary: Summary, failure: Option<Error>) -> Result<Summary, Error> {
    match failure {
        Some(cause) => Err(cause),
        None => Ok(summary),
    }
}

/// A sink over the frontier segment directory.
fn frontier_sink(dir: &Path) -> Result<SegmentSink, Error> {
    SegmentSink::create(
        dir,
        SegmentInfo {
            stream: StreamKind::Frontier,
            ..SegmentInfo::default()
        },
        WriterConfig::default(),
    )
    .map_err(Error::Io)
}

/// Write each domain into the sink and remember where it went.
///
/// A domain that fails is logged and skipped rather than stopping the run. The
/// usual reason is [`umi_crawl::evict::ROW_CEILING`], a site too big to move in
/// one piece, and one of those in a batch of a thousand should not keep the
/// other 999 on the disk.
async fn spill(
    state: &dyn State,
    plds: &[PldId],
    sink: &SegmentSink,
    log: &mut crawl::Log,
) -> Result<Vec<(PldId, Placement)>, Error> {
    let mut placed = Vec::new();
    for pld in plds {
        match umi_crawl::spill_into(state, *pld, sink).await {
            Ok(Some(place)) => placed.push((*pld, place)),
            // No rows is the normal answer for a domain that was admitted and
            // then excluded. There is nothing to publish and nothing to point
            // at, so it is not counted and not written down.
            Ok(None) => {}
            Err(cause) => log.line(&format!("{pld} was not moved: {cause}"))?,
        }
    }
    Ok(placed)
}
