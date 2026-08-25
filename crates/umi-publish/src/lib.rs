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
//!    ├─ 4  upload to Hugging Face               hub
//!    ├─ 5  verify the remote copy independently hub::read_range, gc::Evidence
//!    ├─ 6  append and sign the manifest, push   manifest, hub
//!    ├─ 7  write remote locations into state    pipeline, via umi-state
//!    └─ 8  GC deletes the local files           gc
//! ```
//!
//! [`pipeline`] is the module that runs all eight in order. The others are
//! pieces and can be used on their own, which is what the tests do.
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
//! The network is in [`hub`] and stays there. Everything else in this crate
//! takes bytes and returns bytes, which is what lets the conversion, the
//! manifest chain and the GC rule be tested without a socket, and it is also
//! doc 12.7's shape: the four conditions are checked by a caller that has both
//! a local fact and a remote one, and neither half gets to decide on its own.
//!
//! Doc 12.8's reconciliation pass is not here yet. [`hub::Hub::list`] is the
//! half of it that needs the network.

#![forbid(unsafe_code)]

pub mod convert;
pub mod directory;
pub mod gc;
pub mod hub;
pub mod keys;
pub mod manifest;
pub mod pipeline;
pub mod repo;
pub mod verify;

#[cfg(test)]
mod scripted;
#[cfg(test)]
mod tests;

pub use convert::{Converted, Tally, convert};
pub use gc::{Blocked, Cleared, Evidence};
pub use hub::{Commit, Hub, HubConfig, Remote, Retry, Upload, Who};
pub use keys::{Role, SigningKey, VerifyingKey};
pub use manifest::{FileEntry, Manifest, Verification};
pub use pipeline::{PublishConfig, Published, Publisher, stream_kind, stream_of};
pub use repo::{Corpus, Family, Location, locate};
pub use verify::{Report, verify};

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

    /// The upload landed but did not check out, so nothing was recorded and
    /// the segment is still due. Carries whichever of doc 12.7's first three
    /// conditions failed.
    ///
    /// Separate from the conditions being checked before a delete, which is
    /// what [`Blocked`] usually reports, because this one means the publish
    /// did not happen. A caller retries it; a blocked delete is retried by the
    /// next collection pass instead.
    #[error("the published copy did not check out: {0}")]
    NotPublished(Blocked),

    /// The state ledger would not answer. Kept as a string for the same
    /// reason as [`Error::Parquet`]: a caller of this crate should not have to
    /// name a state backend's error type to handle a publish failure.
    #[error("state: {0}")]
    State(String),

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

    /// The hub answered, and the answer was no. The body is capped and never
    /// carries a credential, because the only url with one in it is the
    /// presigned upload target and nothing formats that.
    #[error("hugging face said {status} while {what}: {body}")]
    Hub {
        /// The HTTP status.
        status: u16,
        /// Which step of doc 12.6 was in progress.
        what: &'static str,
        /// What the hub said, trimmed.
        body: String,
    },

    /// The request did not get an answer at all, after doc 12.6's retries.
    #[error("hugging face was unreachable while {what}: {cause}")]
    Transport {
        /// Which step of doc 12.6 was in progress.
        what: &'static str,
        /// What the client said, with the url removed.
        cause: String,
    },
}

/// The result type this crate returns.
pub type Result<T> = std::result::Result<T, Error>;
