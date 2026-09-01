//! The crawl loop, and the row it produces.
//!
//! Everything below this crate does one job each. `umi-frontier` decides what
//! to fetch, `umi-fetch` fetches it, `umi-robots` says whether we were allowed
//! to, `umi-extract` turns bytes into markdown and links, `umi-dedup` reduces
//! that to a sketch and `umi-file` writes columns. This crate is the only
//! place that knows the order they go in, and it is where two programs meet:
//! `umid` runs the loop as a service and `umi crawl` runs it once from a
//! terminal, and neither should be a dependency of the other.
//!
//! Right now it holds the row builder and nothing else. That is the piece
//! everything upstream was blocked on, because doc 10.5's `pages` schema has
//! non null `minhash`, `simhash`, `chunk_root` and `extract_digest` columns
//! and there was no honest way to fill them. Filling them with zeroes for one
//! milestone and fixing it in the next would have meant a published dataset
//! whose dedup columns are meaningless for its first few hundred million rows,
//! and nothing later can repair that without refetching. So the row comes
//! first and the loop follows.
//!
//! # What a row costs
//!
//! Gate 1.1 in doc 16 wants 250 pages a second on one server. The row builder
//! is the last stage of the pipeline and it does real work: a chunk tree over
//! the body, a sketch over the text, a digest over the extraction, and then
//! the Arrow appends. `benches/rows.rs` measures all of it and the number to
//! beat is 250, per core, with everything else in the pipeline still to pay
//! for.

pub mod backpressure;
pub mod clock;
pub mod digest;
pub mod fetch;
pub mod ledger;
pub mod page;
pub mod probe;
pub mod render;
pub mod robots;
pub mod run;
pub mod scope;
pub mod sink;
pub mod sitemap;

#[cfg(test)]
mod backpressure_tests;
#[cfg(test)]
mod render_tests;
#[cfg(test)]
mod robots_tests;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod scope_tests;
#[cfg(test)]
mod sink_tests;
#[cfg(test)]
mod sitemap_tests;
#[cfg(test)]
mod tests;

pub use backpressure::{Allowance, Backpressure, Cause, Ladder, Signals, Transition};
pub use clock::{Clock, FixedClock, SystemClock};
pub use digest::extract_digest;
pub use fetch::Fetch;
pub use ledger::{Recorded, SupervisedLedger};
pub use page::{Crawled, PageBuilder, PageRow, Snippet, SnippetKind};
pub use render::{RenderBudget, RenderPolicy, Slot};
pub use robots::RobotsCache;
pub use run::{CrawlConfig, CrawlError, Crawler, Live, Sink, TickReport};
pub use scope::{
    Budget, ContentFilter, Corpus, LinkPolicy, Matcher, RateOverride, Scope, ScopeError, Seed,
};
pub use sink::{Sealed, SegmentInfo, SegmentSink};
pub use sitemap::{SitemapLimits, SitemapReport};
