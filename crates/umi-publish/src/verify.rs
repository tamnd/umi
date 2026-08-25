//! Checking a published repository from the outside, for doc 16's gate 1.5.
//!
//! Everything else in this crate publishes. This module is the other half of
//! the claim, and it is deliberately written as though it had never seen the
//! crawl: it takes a repository name and a hub, reads the manifests, reads the
//! keys out of `umi-meta`, and answers whether what is published holds
//! together. No state ledger, no local files, no configuration beyond which
//! organisation to trust for the keys. Gate 1.5 asks for exactly that, because
//! verification that only works where the crawl ran is verification of the
//! local disk.
//!
//! # What is checked
//!
//! In order, and each one for a reason doc 12.5 gives:
//!
//! 1. Every day manifest parses and is in canonical form, which is
//!    [`Manifest::parse`] and catches a document that was edited after signing
//!    in a way that keeps it valid json.
//! 2. Its detached signature verifies under one of the published publishing
//!    keys. A day with no signature is a failure and not a skip.
//! 3. Each day's `prev` is the digest of the day before it, so the repository
//!    is the hash chain doc 12.5 says it is rather than a pile of documents.
//! 4. Every file the manifest names is on the hub, at the size the manifest
//!    says, and where the hub stores it through lfs, under the sha256 the
//!    manifest says.
//!
//! # The cheap check and the expensive one
//!
//! Step 4 is free. Hugging Face stores a large file through lfs and lfs names
//! an object by the sha256 of its content, so the digest in the listing is a
//! digest of the bytes rather than of a git blob header, and comparing it to
//! the manifest checks the whole file without downloading a byte of it. That
//! is a real check but it trusts the hub to have computed it honestly, which
//! is why [`Options::full`] exists: it downloads each file and digests it
//! here, which trusts nothing and costs the bandwidth.
//!
//! The default is the cheap one for every file. A verifier that downloaded a
//! week of the corpus by default would be a verifier nobody runs.

use crate::directory::published_keys;
use crate::hub::Hub;
use crate::manifest::Manifest;
use crate::repo::META_REPO;
use crate::{Error, Result};

/// What to check and where to look for the keys.
#[derive(Clone, Debug)]
pub struct Options {
    /// Download every file and digest it here, rather than comparing against
    /// the digest the hub reports.
    pub full: bool,
    /// Where the publishing keys are published. `open-index/umi-meta` unless
    /// somebody is running their own hub.
    pub meta_repo: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            full: false,
            meta_repo: META_REPO.to_owned(),
        }
    }
}

/// One day of a repository, after checking.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Day {
    /// The `YYYYMMDD` folder.
    pub day: String,
    /// How many files the manifest names.
    pub files: usize,
    /// How many rows they hold, as the manifest says and, under
    /// [`Options::full`], as the files themselves say.
    pub rows: u64,
    /// How many bytes, likewise.
    pub bytes: u64,
    /// The [`crate::directory::key_id`] of the key whose signature checked out.
    pub signed_by: String,
    /// How many files were downloaded and digested here rather than compared
    /// against what the hub reported.
    pub downloaded: usize,
}

/// What a whole repository came to.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Report {
    /// Which repository.
    pub repo: String,
    /// Each day, oldest first.
    pub days: Vec<Day>,
}

impl Report {
    /// Totals over every day, for the one line an operator reads.
    #[must_use]
    pub fn totals(&self) -> (usize, u64, u64) {
        self.days.iter().fold((0, 0, 0), |(f, r, b), day| {
            (f + day.files, r + day.rows, b + day.bytes)
        })
    }
}

/// Check a published repository.
///
/// # Errors
///
/// [`Error::Manifest`] for a repository with nothing to check, a manifest that
/// does not parse or does not chain, and a file that is missing or the wrong
/// size. [`Error::BadSignature`] when no published key signed a day. Doc 14.9
/// maps all of those to exit 6, which is never retried automatically, and that
/// is the right answer for every one of them: a repository that does not
/// verify does not start verifying because you asked twice.
pub async fn verify(hub: &Hub, repo: &str, options: &Options) -> Result<Report> {
    let keys = published_keys(hub, &options.meta_repo).await?;
    if keys.is_empty() {
        return Err(Error::Manifest("no publishing keys are published"));
    }

    let mut days: Vec<String> = hub
        .list(repo, "_manifest")
        .await?
        .into_iter()
        .filter_map(|entry| {
            entry
                .path
                .strip_prefix("_manifest/")
                .and_then(|name| name.strip_suffix(".json"))
                .map(ToOwned::to_owned)
        })
        .collect();
    // Oldest first, which is both the order the chain was built in and the
    // order a reader wants to see it reported in. The names are `YYYYMMDD` so
    // sorting them as text sorts them as dates.
    days.sort();
    if days.is_empty() {
        return Err(Error::Manifest("the repository has no manifests"));
    }

    let mut report = Report {
        repo: repo.to_owned(),
        days: Vec::with_capacity(days.len()),
    };
    let mut previous: Option<Manifest> = None;
    for day in days {
        let checked = one_day(hub, repo, &day, &keys, previous.as_ref(), options).await?;
        report.days.push(checked.0);
        previous = Some(checked.1);
    }
    Ok(report)
}

/// One day: parse, signature, chain, then the files.
async fn one_day(
    hub: &Hub,
    repo: &str,
    day: &str,
    keys: &[crate::keys::VerifyingKey],
    previous: Option<&Manifest>,
    options: &Options,
) -> Result<(Day, Manifest)> {
    let path = format!("_manifest/{day}.json");
    let bytes = hub
        .read(repo, &path)
        .await?
        .ok_or(Error::Manifest("a manifest in the listing would not read"))?;
    let manifest = Manifest::parse(&bytes)?;

    let signature = hub
        .read(repo, &format!("{path}.sig"))
        .await?
        .ok_or(Error::Manifest("a manifest has no signature"))?;
    let signature: [u8; 64] = signature
        .try_into()
        .map_err(|_| Error::Manifest("a signature is not 64 bytes"))?;
    let signed_by = keys
        .iter()
        .find(|key| manifest.verify(key, &signature).is_ok())
        .map(crate::directory::key_id)
        .ok_or(Error::BadSignature)?;

    if let Some(previous) = previous
        && !manifest.follows(previous)?
    {
        return Err(Error::Manifest("a day does not chain to the day before it"));
    }

    let (rows, bytes_total) = manifest.totals();
    let mut downloaded = 0;
    for entry in &manifest.files {
        if options.full {
            full(hub, repo, entry).await?;
            downloaded += 1;
        } else {
            listed(hub, repo, entry).await?;
        }
    }

    Ok((
        Day {
            day: day.to_owned(),
            files: manifest.files.len(),
            rows,
            bytes: bytes_total,
            signed_by,
            downloaded,
        },
        manifest,
    ))
}

/// The cheap check: the hub has it, at that size, under that sha256.
async fn listed(hub: &Hub, repo: &str, entry: &crate::manifest::FileEntry) -> Result<()> {
    let found = hub
        .info(repo, &entry.path)
        .await?
        .ok_or(Error::Manifest("a file the manifest names is not there"))?;
    if found.size != entry.bytes {
        return Err(Error::Manifest("a file is not the size the manifest says"));
    }
    // An inline file has no content digest to compare, only a git blob hash,
    // and doc 12.3 sizes every published file well past the point where the
    // hub stores it through lfs. So a published Parquet file always has one
    // and the only way not to is to be something else.
    match &found.sha256 {
        Some(sha256) if same_digest(sha256, &entry.sha256) => Ok(()),
        Some(_) => Err(Error::Manifest("a file's sha256 is not the published one")),
        None => Err(Error::Manifest("a file is not stored under a content hash")),
    }
}

/// The expensive check: read it back and digest it here.
async fn full(hub: &Hub, repo: &str, entry: &crate::manifest::FileEntry) -> Result<()> {
    let bytes = hub
        .read(repo, &entry.path)
        .await?
        .ok_or(Error::Manifest("a file the manifest names is not there"))?;
    if bytes.len() as u64 != entry.bytes {
        return Err(Error::Manifest("a file is not the size the manifest says"));
    }
    if blake3::hash(&bytes).as_bytes() != &entry.blake3 {
        return Err(Error::Manifest("a file's blake3 is not the published one"));
    }
    if sha256(&bytes) != entry.sha256 {
        return Err(Error::Manifest("a file's sha256 is not the published one"));
    }
    Ok(())
}

/// Whether a digest the hub wrote as text is a digest we hold as bytes.
///
/// A manifest writes `blake3:` and `sha256:` prefixes and lfs does not, so the
/// prefix is optional here. Decoding the text rather than encoding the bytes
/// means a hub that answered with a truncated or overlong digest fails on the
/// length rather than on a comparison that happened to disagree.
fn same_digest(text: &str, digest: &[u8; 32]) -> bool {
    let bare = text.rsplit(':').next().unwrap_or(text);
    let mut decoded = [0u8; 32];
    hex::decode_to_slice(bare, &mut decoded).is_ok() && &decoded == digest
}

/// sha256 of a slice, which is the one digest this crate needs and does not
/// otherwise have a home for.
fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_prefixed_digest_and_a_bare_one_are_the_same_digest() {
        let digest = [0xab; 32];
        let bare = "ab".repeat(32);
        assert!(super::same_digest(&bare, &digest));
        assert!(super::same_digest(&format!("sha256:{bare}"), &digest));
        assert!(super::same_digest(&bare.to_uppercase(), &digest));
        assert!(
            !super::same_digest(&"cd".repeat(32), &digest),
            "and two different ones are not"
        );
        assert!(
            !super::same_digest(&bare[..32], &digest),
            "nor is a prefix of one"
        );
        assert!(
            !super::same_digest("not hex at all", &digest),
            "nor is something that is not a digest"
        );
    }
}
