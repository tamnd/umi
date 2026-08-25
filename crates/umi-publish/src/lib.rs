//! Publishing, from `docs/spec/12-publishing.md`.
//!
//! On most crawlers, publishing is the last thing that happens and the first
//! thing that slips. Here it is the thing that keeps the crawl alive. Doc 01
//! measured 342 GB of free disk across the fleet against roughly 390 GB of
//! output a day, so the local disk holds under a day of crawling even if it
//! holds nothing else. Publishing is not an export step, it is the mechanism by
//! which server1, server2 and server3 stay under their disk limits, and if it
//! stops, the crawl stops.
//!
//! That gives everything here a bias. Doc 12.1 states it: prefer a simple
//! operation that always completes over a clever one that is faster on average,
//! because the failure mode of a stalled publisher is a full disk and a stopped
//! crawl, and the failure mode of a slightly slow publisher is nothing at all.
//!
//! # The pipeline
//!
//! Doc 12.2's eight steps, and which module owns each.
//!
//! ```text
//! segment sealed (128 MB .umi)
//!    ├─ 1  verify every chunk checksum          convert, folded into step 2
//!    ├─ 2  convert shoals to Parquet row groups convert
//!    ├─ 3  digest the Parquet, blake3 + sha256  convert
//!    ├─ 4  upload to Hugging Face               not in this crate yet
//!    ├─ 5  verify the remote copy independently gc::Evidence
//!    ├─ 6  append and sign the manifest, push   manifest
//!    ├─ 7  write remote locations into state    not in this crate yet
//!    └─ 8  GC deletes the local files           gc
//! ```
//!
//! # What is deliberately not here
//!
//! No clock and no random source. Doc 11.1's determinism rule is that the same
//! input bytes plus the same version produce byte identical output on every
//! machine, and a manifest is the last place anyone wants that to stop holding.
//! Every timestamp is an argument, the repository path comes from the segment's
//! own fetch range rather than from when the publisher ran, and the sampled
//! verification in [`gc::sample_ranges`] takes a seed.
//!
//! No network either, yet. The HTTP client, the batched commits from doc 12.6
//! and the reconciliation pass from doc 12.8 land next, on top of these pieces.

#![forbid(unsafe_code)]

pub mod convert;
pub mod gc;
pub mod keys;
pub mod manifest;
pub mod repo;

#[cfg(test)]
mod tests;

pub use convert::{Converted, convert};
pub use gc::{Blocked, Cleared, Evidence};
pub use keys::{Role, SigningKey, VerifyingKey};
pub use manifest::{FileEntry, Manifest, Verification};
pub use repo::{Family, Location, locate};

/// What can go wrong publishing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The filesystem said no.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The segment could not be read or decoded.
    #[error("segment: {0}")]
    Segment(#[from] umi_file::Error),

    /// Parquet refused to write or read. Kept as a string rather than wrapping
    /// the error, so that this crate's public surface does not force every
    /// caller to depend on the parquet crate's error type.
    #[error("parquet: {0}")]
    Parquet(String),

    /// A manifest did not parse, was a version this build does not know, or did
    /// not match its own digest.
    #[error("manifest: {0}")]
    Manifest(&'static str),

    /// A key was not on the curve, or was the wrong key for the job.
    #[error("the key is not usable for this")]
    Key,

    /// A signature did not verify. Never distinguished from a malformed one,
    /// because a caller that told them apart would be an oracle.
    #[error("the signature did not verify")]
    BadSignature,

    /// A key source was not `env:NAME` or `file:PATH`, or what it pointed at
    /// was not a key. The message never contains the value.
    #[error("key source: {0}")]
    Secret(&'static str),
}

/// The result type this crate returns.
pub type Result<T> = std::result::Result<T, Error>;
