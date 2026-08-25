//! Manifests and the hash chain, from `docs/spec/12-publishing.md` section
//! 12.5.
//!
//! Doc 12.5 puts it plainly: the Parquet is just bytes, the manifest is the
//! claim. Everything a consumer is asked to trust is in here, so the whole
//! module is arranged around one property, which is that two people who have
//! the same manifest compute the same digest for it. That has to hold across
//! machines, across builds and across languages, because the point of
//! publishing the chain is that somebody who is not us can check it.
//!
//! # The canonical form
//!
//! A manifest is UTF-8 JSON with no insignificant whitespace, members in the
//! order this module declares them, and the `digest` member last. Its canonical
//! form is the same document with the `digest` member removed, which is exactly
//! the bytes up to but not including the final `,"digest":...` and then a
//! closing brace.
//!
//! `digest` is blake3 over the canonical form. The detached signature is
//! Ed25519 over the same canonical form, under the publishing role from
//! [`crate::keys`], and over the manifest rather than over the digest, so that
//! a verifier never has to trust a digest to check a signature.
//!
//! Fixed member order rather than sorted keys because the order in doc 12.5 is
//! the order a person reads it in, and because a sorted order would put `files`
//! before `repo` and make the document harder to skim for no benefit that a
//! machine can see.

use serde::{Deserialize, Serialize};
use umi_file::StreamKind;
use umi_types::Ulid;

use crate::keys::{SigningKey, VerifyingKey};
use crate::{Error, Result};

/// The serde pair for a digest written as a prefix and 64 hex characters.
///
/// A macro rather than a generic because serde's `with` wants a module, and
/// three modules that differ only in a string literal is exactly what a macro
/// is for.
macro_rules! prefixed {
    ($prefix:literal) => {
        use serde::{Deserialize as _, Deserializer, Serializer};

        pub(super) fn serialize<S: Serializer>(
            value: &[u8; 32],
            ser: S,
        ) -> core::result::Result<S::Ok, S::Error> {
            ser.serialize_str(&format!("{}{}", $prefix, hex::encode(value)))
        }

        pub(super) fn deserialize<'de, D: Deserializer<'de>>(
            de: D,
        ) -> core::result::Result<[u8; 32], D::Error> {
            let text = String::deserialize(de)?;
            super::decode_prefixed($prefix, &text).map_err(serde::de::Error::custom)
        }
    };
}

/// The manifest format this build writes and the only one it reads.
pub const MANIFEST_VERSION: u32 = 1;

/// Doc 06's outcome distribution for one file.
///
/// Doc 12.5 publishes these per file so that a consumer who only wants pages we
/// fetched ourselves, or only pages that survived cross fetcher quorum, can
/// filter at file granularity before downloading anything. That is the reason
/// they are counts in the manifest rather than a column somebody has to read
/// the file to see.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct Verification {
    /// Fetched by this coordinator, so there is nobody else to disagree with.
    pub local: u64,
    /// Corroborated by a second fetcher under doc 06's quorum rule.
    pub quorum: u64,
    /// Refetched by us and matched.
    pub replayed: u64,
    /// Arrived from a known fetcher and was not corroborated. Doc 12.10 is
    /// explicit that this does not mean "not checked at all".
    pub unverified: u64,
}

impl Verification {
    /// How many rows the four counts cover.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.local + self.quorum + self.replayed + self.unverified
    }
}

/// One published Parquet file.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct FileEntry {
    /// Repository relative, `data/20260817/01K2M8Q0P7R3XN5.parquet`.
    pub path: String,
    /// The file's length, which is the cheap half of doc 12.7's first check.
    pub bytes: u64,
    /// How many rows are in it.
    pub rows: u64,
    /// `blake3:` and 64 hex characters.
    #[serde(with = "prefixed_blake3")]
    pub blake3: [u8; 32],
    /// `sha256:` and 64 hex characters. Doc 12.5 publishes both because blake3
    /// is what umi computes everywhere else and sha256 is what every other tool
    /// on Earth can check without installing anything.
    #[serde(with = "prefixed_sha256")]
    pub sha256: [u8; 32],
    /// The segment this came from, in its 26 character text form.
    pub segment_ulid: String,
    /// Which host converted and published it.
    pub coordinator: String,
    /// The extractor build, `umi-extract/0.4.1`.
    pub extractor: String,
    /// The earliest fetch in the file.
    pub fetched_at_min_ms: u64,
    /// The latest.
    pub fetched_at_max_ms: u64,
    /// Doc 06's counts for this file.
    pub verification: Verification,
}

/// One day of one repository.
///
/// Deliberately not `Serialize` into anything but the canonical form. Everything
/// that writes one of these goes through [`Manifest::canonical`] or
/// [`Manifest::to_json`], so there is no second spelling of the document that
/// could drift from the one the digest covers.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Manifest {
    /// Always [`MANIFEST_VERSION`] on write. A reader that meets a version it
    /// does not know refuses rather than guessing, because a manifest is the
    /// thing everything else is trusted against.
    pub manifest_version: u32,
    /// `open-index/umi-pages-2026w34-03`.
    pub repo: String,
    /// The `YYYYMMDD` day folder.
    pub day: String,
    /// The digest of the previous day's manifest in the same repository, or
    /// none for the first manifest of a chain that starts at a head recorded in
    /// `umi-meta`.
    #[serde(with = "prefixed_blake3_option")]
    pub prev: Option<[u8; 32]>,
    /// `canon/1`, from doc 11.2.
    pub canon_version: String,
    /// `umi-pages/1`, from doc 10.5.
    pub schema_id: String,
    /// Every file in the day folder, in path order.
    pub files: Vec<FileEntry>,
}

impl Manifest {
    /// Start an empty manifest for a day.
    #[must_use]
    pub fn new(repo: &str, day: &str, stream: StreamKind, prev: Option<[u8; 32]>) -> Self {
        Self {
            manifest_version: MANIFEST_VERSION,
            repo: repo.to_owned(),
            day: day.to_owned(),
            prev,
            canon_version: umi_types::CANON_VERSION.to_owned(),
            schema_id: schema_id(stream),
            files: Vec::new(),
        }
    }

    /// Add a file and keep the list in path order.
    ///
    /// Sorted rather than appended, because doc 12.6 batches uploads into one
    /// commit per 32 files and a crash between batches would otherwise leave
    /// the order dependent on which files went first. Path order is also ULID
    /// order, so a manifest reads as a timeline.
    pub fn insert(&mut self, entry: FileEntry) {
        match self.files.binary_search_by(|f| f.path.cmp(&entry.path)) {
            Ok(at) => self.files[at] = entry,
            Err(at) => self.files.insert(at, entry),
        }
    }

    /// Whether a path is already claimed.
    #[must_use]
    pub fn contains(&self, path: &str) -> bool {
        self.files
            .binary_search_by(|f| f.path.as_str().cmp(path))
            .is_ok()
    }

    /// Total rows and bytes across the day, which is what `umi-meta` records.
    #[must_use]
    pub fn totals(&self) -> (u64, u64) {
        self.files
            .iter()
            .fold((0, 0), |(rows, bytes), f| (rows + f.rows, bytes + f.bytes))
    }

    /// The bytes the digest and the signature are both taken over.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] only if serialisation fails, which for this shape
    /// means the process is out of memory.
    pub fn canonical(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| Error::Manifest("could not serialise"))
    }

    /// blake3 over [`canonical`](Self::canonical).
    ///
    /// # Errors
    ///
    /// As [`canonical`](Self::canonical).
    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(*blake3::hash(&self.canonical()?).as_bytes())
    }

    /// The published document: the canonical form with `digest` appended last.
    ///
    /// # Errors
    ///
    /// As [`canonical`](Self::canonical).
    pub fn to_json(&self) -> Result<Vec<u8>> {
        let mut out = self.canonical()?;
        let digest = *blake3::hash(&out).as_bytes();
        // The canonical form is a JSON object, so its last byte is the closing
        // brace and splicing before it is exact rather than a guess. Written
        // this way, and not by round tripping through a map, because a map
        // would reorder the members and the digest would stop covering the
        // document a reader sees.
        out.pop();
        out.extend_from_slice(br#","digest":"blake3:"#);
        out.extend_from_slice(hex::encode(digest).as_bytes());
        out.extend_from_slice(br#""}"#);
        Ok(out)
    }

    /// Read a published manifest and check its own digest.
    ///
    /// The digest is recomputed from the parsed manifest rather than from the
    /// bytes either side of the `digest` member, which means this also proves
    /// the document was in canonical form. A manifest that means the right
    /// thing but is spelled differently, reordered members, added whitespace,
    /// an unknown extra field, is refused here rather than accepted with a
    /// digest nobody else would compute.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] for a document that does not parse, is a version
    /// this build does not know, or whose digest does not match.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let published: Published =
            serde_json::from_slice(bytes).map_err(|_| Error::Manifest("could not parse"))?;
        if published.manifest.manifest_version != MANIFEST_VERSION {
            return Err(Error::Manifest("unknown manifest version"));
        }
        let manifest = published.manifest;
        if manifest.digest()? != published.digest {
            return Err(Error::Manifest("the digest does not cover the document"));
        }
        // And the bytes themselves, which catches whitespace and member order.
        if manifest.to_json()? != bytes {
            return Err(Error::Manifest("the document is not in canonical form"));
        }
        Ok(manifest)
    }

    /// Sign the canonical form under the publishing key.
    ///
    /// # Errors
    ///
    /// As [`canonical`](Self::canonical), and [`Error::Key`] if the key is not
    /// a publishing key. A lease key that signed a manifest would produce a
    /// signature nobody could verify, and failing at the call site is better
    /// than failing at whoever downloads it.
    pub fn sign(&self, key: &SigningKey) -> Result<[u8; 64]> {
        if key.role() != crate::keys::Role::Publishing {
            return Err(Error::Key);
        }
        Ok(key.sign(&self.canonical()?))
    }

    /// Check a detached signature over the canonical form.
    ///
    /// # Errors
    ///
    /// [`Error::BadSignature`] when it does not check out, [`Error::Key`] when
    /// the key is not a publishing key.
    pub fn verify(&self, key: &VerifyingKey, signature: &[u8; 64]) -> Result<()> {
        if key.role() != crate::keys::Role::Publishing {
            return Err(Error::Key);
        }
        key.verify(&self.canonical()?, signature)
    }

    /// Whether this manifest continues from that one.
    ///
    /// The whole chain is this check applied repeatedly. Doc 12.5's claim is
    /// that someone who has verified the head has verified everything under it,
    /// and that only holds if each link is checked against the digest of the
    /// document rather than against a digest recorded next to it.
    ///
    /// # Errors
    ///
    /// As [`digest`](Self::digest).
    pub fn follows(&self, previous: &Self) -> Result<bool> {
        Ok(self.repo == previous.repo && self.prev == Some(previous.digest()?))
    }
}

/// The parse side of [`Manifest::to_json`].
#[derive(Deserialize)]
struct Published {
    #[serde(flatten)]
    manifest: Manifest,
    #[serde(with = "prefixed_blake3")]
    digest: [u8; 32],
}

/// Doc 10.5's schema identifier in the text form doc 12.5 publishes.
#[must_use]
pub fn schema_id(stream: StreamKind) -> String {
    let name = match stream {
        StreamKind::Pages => "umi-pages",
        StreamKind::Receipts => "umi-receipts",
        StreamKind::Robots => "umi-robots",
    };
    format!("{name}/{}", stream.schema_id())
}

/// The text form of a segment identifier, for [`FileEntry::segment_ulid`].
#[must_use]
pub fn segment_text(segment_id: [u8; 16]) -> String {
    Ulid::from_bytes(segment_id).to_text()
}

/// `blake3:` then 64 hex characters, which is how every digest in doc 12.5 is
/// written. The prefix is not decoration: it is what lets the format add a
/// second hash later without a reader having to guess from the length.
mod prefixed_blake3 {
    prefixed!("blake3:");
}

mod prefixed_sha256 {
    prefixed!("sha256:");
}

/// The same, but `null` for the first manifest in a chain.
mod prefixed_blake3_option {
    use serde::{Deserialize as _, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        value: &Option<[u8; 32]>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => ser.serialize_str(&format!("blake3:{}", hex::encode(bytes))),
            None => ser.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<Option<[u8; 32]>, D::Error> {
        let text = Option::<String>::deserialize(de)?;
        match text {
            None => Ok(None),
            Some(text) => super::decode_prefixed("blake3:", &text)
                .map(Some)
                .map_err(serde::de::Error::custom),
        }
    }
}

fn decode_prefixed(prefix: &str, text: &str) -> core::result::Result<[u8; 32], &'static str> {
    let hex_text = text.strip_prefix(prefix).ok_or("wrong digest prefix")?;
    let mut out = [0u8; 32];
    hex::decode_to_slice(hex_text, &mut out).map_err(|_| "not 64 hex characters")?;
    Ok(out)
}
