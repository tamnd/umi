//! Doc 08.6's evict, the half of it that is not publishing.
//!
//! A domain that has gone idle has its ledger rows read out of the local store,
//! written into the open frontier segment, and then dropped once the file they
//! went into is on the hub and its digest has been checked. This module does
//! the reading and the writing. It does not publish, it does not verify and it
//! does not drop anything, because those three need the publisher, the network
//! and the caller's judgement about whether the upload really happened, and a
//! function that did all six would be untestable without all three.
//!
//! So the shape is: [`spill_into`] moves one domain into the sink and hands
//! back a [`Placement`] saying where it went, and the caller publishes the
//! segment, checks the read back digest, writes the placement into the local
//! index with `put_shards`, and only then calls `unload`. The order matters and
//! it is doc 12.7's fourth condition applied to state instead of to pages:
//! nothing local is deleted until the copy that replaces it is provably there.
//!
//! # Why a domain is read whole
//!
//! [`State::spill`] pages, because a store that had to materialise a domain of
//! ten million rows to answer one call would be a store with a memory bound
//! nobody set. This reads every page and writes all of them in one call to the
//! sink, which costs the memory the trait was avoiding, and it buys the one
//! property the local index needs: a segment rolls between calls to the sink
//! and never inside one, so a domain written in a single call is a domain in a
//! single file with a contiguous range of row groups. Written page by page it
//! could straddle a roll, and then the index entry for it would point at half a
//! domain with no way to say so.
//!
//! What that costs is bounded by the domain. A typical one is a few hundred
//! rows and the largest sites are in the millions, which at roughly two hundred
//! bytes of `SpillRow` is hundreds of megabytes for the worst domain on the
//! web. That is a real limit and it is written down in
//! [`ROW_CEILING`]: a domain above it is refused rather than
//! evicted, because a refusal is a domain that stays resident and a straddled
//! write is an index that lies.

use umi_state::{SpillRow, State};
use umi_types::PldId;

use crate::frontier::FrontierBuilder;
use crate::run::CrawlError;
use crate::sink::{Placement, SegmentSink};

/// How many rows one call to [`State::spill`] asks for.
///
/// Big enough that a normal domain comes back in one call, small enough that
/// the page is a few megabytes rather than a few hundred. Nothing depends on
/// the value being this and not twice it.
pub const PAGE: usize = 8192;

/// The largest domain this will evict in one piece.
///
/// Four million rows, which is around eight hundred megabytes of `SpillRow`
/// held at once and covers every domain we have seen in the Common Crawl host
/// ranks. Above it the domain stays resident and the caller is told, because
/// the alternative is writing it across a segment roll and recording an index
/// entry that points at part of it.
pub const ROW_CEILING: usize = 4 << 20;

/// Move one domain's ledger rows into `sink`.
///
/// Nothing is deleted and nothing in the state layer changes. The domain is
/// still resident and still correct when this returns, which is what makes it
/// safe to call and then fail to publish.
///
/// `None` means the domain had no rows, which is the normal answer for a domain
/// that was admitted and then excluded, and is not an error. A caller that gets
/// `None` should clear any index entry it had rather than write one.
///
/// # Errors
///
/// [`CrawlError::Sink`] if the store could not be read, if the domain is over
/// [`ROW_CEILING`], or if the write failed. Every one of them leaves the domain
/// exactly as it was.
pub async fn spill_into(
    state: &dyn State,
    pld: PldId,
    sink: &SegmentSink,
) -> Result<Option<Placement>, CrawlError> {
    let rows = read(state, pld).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    sink.write_grouped::<FrontierBuilder>(&rows)
}

/// Every row of one domain, in key order.
async fn read(state: &dyn State, pld: PldId) -> Result<Vec<SpillRow>, CrawlError> {
    let mut rows: Vec<SpillRow> = Vec::new();
    loop {
        // The cursor is the last key read and the range is exclusive of it, so
        // a page that comes back short is the end of the domain and a page that
        // comes back full is asked to continue. Comparing lengths against the
        // page size is the only stop condition, which is why the store's
        // contract is that a short page means no more rows.
        let after = rows.last().map(|row| row.key.url);
        let page = state
            .spill(pld, after, PAGE)
            .await
            .map_err(|e| CrawlError::Sink(e.to_string()))?;
        let short = page.len() < PAGE;
        rows.extend(page);
        if rows.len() > ROW_CEILING {
            return Err(CrawlError::Sink(format!(
                "{pld} has more than {ROW_CEILING} rows and is too big to evict in one piece"
            )));
        }
        if short {
            return Ok(rows);
        }
    }
}
