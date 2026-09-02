//! Doc 08.6's backlog, on its way out of the local store and into a file.
//!
//! This is the write half of an eviction. The state layer hands back a
//! [`SpillRow`] per ledger row it is about to drop, and this turns a run of
//! them into a batch matching [`StreamKind::Frontier`]. What happens to the
//! batch after that is the same path pages and robots already take: a segment,
//! a Parquet file, a manifest entry, a published dataset.
//!
//! It lives here rather than in `umi-frontier` because the [`Rows`] trait and
//! the sink that drives it are in this crate, and because `umi-frontier` is
//! about choosing what to fetch next rather than about columns. The name is
//! the stream's, not the crate's.
//!
//! # Why the columns are mostly nullable
//!
//! The rows worth spilling are overwhelmingly rows nothing has fetched. That
//! is the point of a backlog. So everything describing a fetch that already
//! happened is null when it has not happened, rather than zero: a zero
//! `last_fetch_ms` reads as a real fetch at the epoch, and a reader computing a
//! refetch interval off it would get 1970. The local store cannot do this,
//! since a fixed width row has nowhere to put a null, so it stores zero and
//! this is where the two conventions meet.
//!
//! [`Rows`]: crate::sink::Rows

use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, RecordBatch, StringBuilder, UInt8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use umi_file::StreamKind;
use umi_state::{SpillRow, UrlState};
use umi_types::{HostId, PldId, UrlKey, UrlKeyFull};

/// [`SpillRow`]s into doc 10.5's frontier batch.
///
/// The same shape as the page and robots builders: rows go in one at a time,
/// the builder says when it has had enough, and `finish` produces a batch that
/// matches [`StreamKind::Frontier`] exactly.
pub struct FrontierBuilder {
    pld_id: FixedSizeBinaryBuilder,
    host_id: FixedSizeBinaryBuilder,
    url_key: FixedSizeBinaryBuilder,
    url_key_full: FixedSizeBinaryBuilder,
    url: StringBuilder,
    depth: UInt8Builder,
    priority: UInt16Builder,
    state: UInt8Builder,
    next_due_ms: UInt64Builder,
    last_fetch_ms: UInt64Builder,
    last_change_ms: UInt64Builder,
    fetch_count: UInt32Builder,
    change_count: UInt32Builder,
    observed_secs: UInt32Builder,
    content_hash: FixedSizeBinaryBuilder,
    etag: StringBuilder,
    last_mod_ms: UInt64Builder,
    status: UInt16Builder,
    tier_used: UInt8Builder,
    fail_streak: UInt8Builder,
    rows: usize,
    bytes: usize,
}

impl Default for FrontierBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontierBuilder {
    /// The row half of the shoal cap.
    ///
    /// Four times the page limit because a frontier row is small and fixed
    /// width apart from the URL and the ETag. Sixty five thousand of them is
    /// around eight megabytes before encoding, which is the same size class as
    /// a page shoal of sixteen thousand.
    pub const ROW_LIMIT: usize = 65_536;

    /// The byte half of the shoal cap, doc 10.4's 32 MiB.
    pub const BYTE_LIMIT: usize = 32 << 20;

    /// An empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pld_id: FixedSizeBinaryBuilder::new(PldId::LEN as i32),
            host_id: FixedSizeBinaryBuilder::new(HostId::LEN as i32),
            url_key: FixedSizeBinaryBuilder::new(UrlKey::LEN as i32),
            url_key_full: FixedSizeBinaryBuilder::new(UrlKeyFull::LEN as i32),
            url: StringBuilder::new(),
            depth: UInt8Builder::new(),
            priority: UInt16Builder::new(),
            state: UInt8Builder::new(),
            next_due_ms: UInt64Builder::new(),
            last_fetch_ms: UInt64Builder::new(),
            last_change_ms: UInt64Builder::new(),
            fetch_count: UInt32Builder::new(),
            change_count: UInt32Builder::new(),
            observed_secs: UInt32Builder::new(),
            content_hash: FixedSizeBinaryBuilder::new(8),
            etag: StringBuilder::new(),
            last_mod_ms: UInt64Builder::new(),
            status: UInt16Builder::new(),
            tier_used: UInt8Builder::new(),
            fail_streak: UInt8Builder::new(),
            rows: 0,
            bytes: 0,
        }
    }

    /// How many rows have gone in.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Whether this shoal is full.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.rows >= Self::ROW_LIMIT || self.bytes >= Self::BYTE_LIMIT
    }

    /// Whether anything has gone in.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append one row.
    pub fn push(&mut self, spill: &SpillRow) {
        let row = &spill.row;
        self.pld_id
            .append_value(spill.key.pld.as_bytes())
            .expect("pld_id is 8 bytes");
        // From the key and not from `row.host_id`. The two agree on every row
        // the store wrote, and the key is the one the file is sorted by, so if
        // they ever disagreed the sorted column is the one a reader's row group
        // statistics were computed from.
        self.host_id
            .append_value(spill.key.host.as_bytes())
            .expect("host_id is 8 bytes");
        self.url_key
            .append_value(spill.key.url.as_bytes())
            .expect("url_key is 10 bytes");
        self.url_key_full
            .append_value(row.url_key_full.as_bytes())
            .expect("url_key_full is 16 bytes");
        self.url.append_value(&spill.url);
        self.depth.append_value(row.depth);
        self.priority.append_value(row.priority.raw());
        self.state.append_value(row.state as u8);
        self.next_due_ms.append_value(row.next_due_ms);
        self.last_fetch_ms.append_option(when(row.last_fetch_ms));
        self.last_change_ms.append_option(when(row.last_change_ms));
        self.fetch_count.append_value(row.fetch_count);
        self.change_count.append_value(row.change_count);
        self.observed_secs.append_value(row.observed_secs);
        // An all zero content hash is what a row that has never been fetched
        // carries, and it is not a digest anything hashed to. Writing it would
        // give a hundred million untouched rows the same non null hash and make
        // every change detector think they are duplicates of each other.
        if row.content_hash == [0u8; 8] {
            self.content_hash.append_null();
        } else {
            self.content_hash
                .append_value(row.content_hash)
                .expect("content_hash is 8 bytes");
        }
        self.etag.append_option(spill.etag.as_deref());
        self.last_mod_ms.append_option(when(row.last_mod_ms));
        // Status zero means no response ever arrived, which is not a status
        // code, so it is a null here for the same reason the timestamps are.
        self.status
            .append_option((row.status != 0).then_some(row.status));
        // Tier is nullable because a row nothing has fetched was not fetched at
        // any tier, and `Tier::Revalidate` is zero, so a zero would read as a
        // conditional request that never happened.
        self.tier_used
            .append_option((row.state != UrlState::Pending).then(|| row.tier_used.as_u8()));
        self.fail_streak.append_value(row.fail_streak);

        self.rows += 1;
        self.bytes += spill.url.len() + spill.etag.as_ref().map_or(0, String::len) + FIXED_BYTES;
    }

    /// Finish the batch.
    #[must_use]
    pub fn finish(mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.pld_id.finish()),
            Arc::new(self.host_id.finish()),
            Arc::new(self.url_key.finish()),
            Arc::new(self.url_key_full.finish()),
            Arc::new(self.url.finish()),
            Arc::new(self.depth.finish()),
            Arc::new(self.priority.finish()),
            Arc::new(self.state.finish()),
            Arc::new(self.next_due_ms.finish()),
            Arc::new(self.last_fetch_ms.finish()),
            Arc::new(self.last_change_ms.finish()),
            Arc::new(self.fetch_count.finish()),
            Arc::new(self.change_count.finish()),
            Arc::new(self.observed_secs.finish()),
            Arc::new(self.content_hash.finish()),
            Arc::new(self.etag.finish()),
            Arc::new(self.last_mod_ms.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.tier_used.finish()),
            Arc::new(self.fail_streak.finish()),
        ];
        RecordBatch::try_new(StreamKind::Frontier.arrow(), columns)
            .expect("the frontier builder matches doc 10.5")
    }
}

/// The fixed width part of one row, for the byte half of the shoal cap.
///
/// The two keys, the two fingerprints, the content hash and the eleven scalar
/// columns. It is an estimate of the uncompressed size and not of the file,
/// which is what the cap wants: the cap exists so a shoal fits in memory while
/// it is being built.
const FIXED_BYTES: usize = PldId::LEN + HostId::LEN + UrlKey::LEN + UrlKeyFull::LEN + 8 + 40;

/// A timestamp column's value, or `None` for a row it never happened on.
///
/// Zero is the local store's way of writing "never", because a fixed width row
/// has nowhere to put a null. The published file has somewhere, and a zero
/// there would read as the Unix epoch.
const fn when(ms: u64) -> Option<u64> {
    if ms == 0 { None } else { Some(ms) }
}
