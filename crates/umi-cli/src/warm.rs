//! `umi warm`, doc 08.6's backlog coming back off the hub.
//!
//! The other direction from `umi evict`. A domain that was spilled into a
//! published frontier file is a shard entry saying which file and which row
//! groups, and this turns that entry back into local ledger rows so the
//! scheduler can work the domain again.
//!
//! # What a warm costs
//!
//! Two requests for the first domain in a file and one for every domain after
//! it. Opening a file reads its footer, and a frontier footer carries
//! statistics for twenty columns of every row group, so it is proportional to
//! how many domains landed in the file rather than to how big they were. The
//! rows themselves are one ranged GET of the span from the domain's first
//! column chunk to its last, which for a typical domain is a few hundred
//! kilobytes.
//!
//! So the work is grouped by file rather than by domain. Warming a hundred
//! domains that were evicted together is a hundred and one requests, and
//! warming them one at a time would be two hundred with half the bytes spent
//! on the same footer over and over.
//!
//! # The order of the three steps
//!
//! Read, restore, clear. The mirror of an eviction and durable in the same
//! place: the rows have to be local before the pointer goes, because a crash
//! between clearing the pointer and writing the rows leaves a domain that is
//! neither local nor findable. A crash the other way leaves a domain that is
//! both local and pointed at, and the next warm restores rows that are already
//! there, which restore is built to answer with a count of zero.
//!
//! Nothing published is touched. The file stays where it is, which is doc 12's
//! rule, and the entry that goes is only the pointer. What that costs is dead
//! rows accumulating in old files, and doc 08.6 says compaction collects them
//! later rather than a delete collecting them now.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use umi_crawl::read_frontier;
use umi_publish::{Hub, HubFile, footer, read_row_groups};
use umi_state::{Shard, State};
use umi_state_sqlite::SqliteState;
use umi_types::{PldId, Ulid};

use crate::Error;
use crate::crawl::{self, Layout, Publishing};

/// How many domains a run brings back when the operator does not say.
///
/// The same thousand `umi evict` moves, so a warm and an eviction called in
/// turn move the same amount of work in each direction and neither one runs
/// away from the other.
pub const DOMAINS: usize = 1000;

/// What the operator asked for.
#[derive(Clone, Debug)]
pub struct Options {
    /// The crawl directory to work on.
    pub dir: PathBuf,
    /// How many domains to bring back.
    pub limit: usize,
    /// Say what would come back and bring nothing.
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

/// What a run did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Warmed {
    /// Domains whose rows are local again.
    pub domains: usize,
    /// Rows that became local, which is what `restore` reported and not what
    /// the files held. A row already local is not counted twice.
    pub rows: usize,
    /// Published files that were opened, which is what the footer reads cost.
    pub files: usize,
}

/// Bring the coldest part of a crawl directory's backlog back to local disk.
///
/// # Errors
///
/// [`Error::Io`] when the directory has no `profile.toml`, which is what tells
/// a crawl directory apart from any other directory, [`Error::NothingToDo`]
/// when nothing has been evicted, and whatever the hub or the state ledger
/// reports.
pub fn warm(options: &Options, publishing: &Publishing) -> Result<Warmed, Error> {
    let layout = Layout::create(&options.dir)?;
    crawl::profile_of(&options.dir)?;
    let state: Arc<dyn State> =
        Arc::new(SqliteState::open(&layout.state).map_err(|e| Error::State(e.to_string()))?);
    let mut log = crawl::Log::open(&layout.log)?;

    // Single threaded for the same reason `umi evict` is. The work is ranged
    // reads against one hub and rows going into one sqlite writer, so a second
    // runtime thread would have nothing to do but exist.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;

    runtime.block_on(async {
        let cold = state
            .cold(options.limit)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        if cold.is_empty() {
            return Err(Error::NothingToDo("nothing has been evicted"));
        }

        let by_file = group(&cold);
        if options.dry_run {
            log.line(&format!(
                "{} domains in {} files would come back",
                cold.len(),
                by_file.len()
            ))?;
            return Ok(Warmed::default());
        }

        // The hub and not a publisher, because a warm only reads. It needs no
        // signing key and it announces nothing.
        let hub = Hub::new(publishing.token.clone())?;
        let mut warmed = Warmed::default();
        for (segment, shards) in &by_file {
            match file(&hub, &*state, *segment, shards, &mut log).await {
                Ok(done) => {
                    warmed.files += 1;
                    warmed.domains += done.domains;
                    warmed.rows += done.rows;
                }
                // One unreachable file does not stop the run. Its domains keep
                // their pointers and stay cold, which is the same outcome as
                // never having asked, so the next run picks them up again.
                Err(failure) => log.line(&format!("{segment} did not warm: {failure}"))?,
            }
        }
        log.line(&format!(
            "{} domains and {} rows came back from {} files",
            warmed.domains, warmed.rows, warmed.files
        ))?;
        Ok(warmed)
    })
}

/// The shard entries grouped by the file they point at, in eviction order.
///
/// A `BTreeMap` keyed by the segment id, which is a ULID, so the files come out
/// in the order they were sealed. That is the same order the cold list is in,
/// so a warm that stops at a limit and runs again carries on rather than
/// reopening a file it has already finished with.
fn group(cold: &[Shard]) -> BTreeMap<Ulid, Vec<Shard>> {
    let mut by_file: BTreeMap<Ulid, Vec<Shard>> = BTreeMap::new();
    for shard in cold {
        by_file.entry(shard.segment).or_default().push(*shard);
    }
    for shards in by_file.values_mut() {
        // By row group, so the ranged reads walk the file forwards. It costs
        // nothing against a CDN and it is the order a future prefetch would
        // want, since two domains in adjacent row groups are one read.
        shards.sort_by_key(|shard| shard.first_group);
    }
    by_file
}

/// Warm every domain that landed in one published file.
async fn file(
    hub: &Hub,
    state: &dyn State,
    segment: Ulid,
    shards: &[Shard],
    log: &mut crawl::Log,
) -> Result<Warmed, Error> {
    let row = state
        .segment(segment)
        .await
        .map_err(|e| Error::State(e.to_string()))?
        .ok_or_else(|| Error::State(format!("{segment} is not a segment this store knows")))?;
    let remote = row.remote.ok_or_else(|| {
        // A shard entry is only written after the upload verified, so this is
        // not a race with a publish in flight. It means the two tables have
        // drifted, and reading some other file would be worse than stopping.
        Error::State(format!("{segment} has no published copy to read from"))
    })?;

    let source = HubFile::open(hub, &remote.repo, &remote.path).await?;
    // Once, for the whole file. This is the reason the domains are grouped.
    let metadata = Arc::new(footer(&source).await?);

    let mut warmed = Warmed::default();
    for shard in shards {
        let batches =
            read_row_groups(&source, &metadata, shard.first_group, shard.last_group).await?;
        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(read_frontier(batch).map_err(|e| Error::Crawl(e.to_string()))?);
        }
        // The row groups hold whole domains, but a range can hold more than one
        // if two small domains shared a group, so this keeps only the rows the
        // entry is about. Restoring a neighbour would work and would leave a
        // domain that is local and still pointed at.
        rows.retain(|spill| spill.key.pld == shard.pld);
        if rows.is_empty() {
            log.line(&format!(
                "{} held no rows for this domain, so its pointer stays",
                shard.segment
            ))?;
            continue;
        }

        // Rows first, pointer second. A crash between them leaves a domain that
        // is both local and pointed at, and the next warm restores rows that
        // are already there and reports nothing.
        let restored = state
            .restore(&rows)
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        state
            .clear_shards(&[shard.pld])
            .await
            .map_err(|e| Error::State(e.to_string()))?;
        warmed.domains += 1;
        warmed.rows += restored;
    }
    Ok(warmed)
}

/// The domains a warm would touch, for a caller that wants to look first.
///
/// Public because `umi state stats` wants the same grouping to say how much of
/// the backlog is cold and in how many files, and doing it twice would be two
/// answers that can disagree.
#[must_use]
pub fn files(cold: &[Shard]) -> usize {
    group(cold).len()
}

/// The domains in a cold list, for the same reason.
#[must_use]
pub fn domains(cold: &[Shard]) -> Vec<PldId> {
    cold.iter().map(|shard| shard.pld).collect()
}
