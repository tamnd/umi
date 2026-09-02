//! Taking a published file back out, and leaving the repository honest.
//!
//! Doc 12.2's rule is that a published file is never rewritten and never
//! deleted, and the reason is good: both break every checksum anyone recorded.
//! This module exists because the rule was stated as if a deletion could not
//! happen, and an operator who decides one has to happen anyway would otherwise
//! do it with a shell loop, which is the worst version of it. A file removed by
//! hand leaves the day manifest naming a file that is not there, leaves the
//! chain pointing at a digest that no longer exists, and leaves no record of
//! who did it or why. A reader checking that repository finds a broken chain and
//! cannot tell our edit from an attack, which is the exact thing the chain is
//! for.
//!
//! So a retraction is the supported, recorded form of the thing the rule warns
//! about. It is not a rehabilitation of deleting files. It is an admission that
//! if one happens it should be visible.
//!
//! # What it does
//!
//! One commit per affected repository, holding the deletions and the rewritten
//! manifests together. That is the property worth having: there is no moment at
//! which a reader can fetch a manifest that names a file the same commit
//! removed, because the hub applies a commit whole or not at all.
//!
//! Then the chain is relinked. Doc 12.5 gives each day manifest the digest of
//! the day before it, so rewriting one day changes its digest and orphans every
//! day after it. Every later manifest in the repository is rewritten with the
//! new `prev` and re-signed, in day order, because doing it in any other order
//! would link a day to a digest that is about to change.
//!
//! # What it cannot fix
//!
//! Anyone who recorded a manifest digest before the retraction holds a digest
//! that will not match again. Nothing here repairs that and nothing can. The
//! record written to `umi-meta` is the whole mitigation: it names every removed
//! file with the digest it had, so a reader whose check now fails can find out
//! what happened rather than concluding they were served tampered data.

use serde::{Deserialize, Serialize};

use crate::hub::{Hub, Upload};
use crate::keys::SigningKey;
use crate::manifest::Manifest;
use crate::{Error, Result};

/// Where a retraction record goes in the meta repository.
pub const PREFIX: &str = "retractions";

/// One file that was taken out, as the record remembers it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Removed {
    /// Repository relative, the path it had.
    pub path: String,
    /// How many rows it held.
    pub rows: u64,
    /// How long it was.
    pub bytes: u64,
    /// The digest it had, `blake3:` and 64 hex characters, exactly as the
    /// manifest carried it.
    ///
    /// This is the field the record exists for. A reader whose recorded digest
    /// stopped matching can find it here and know the file was retracted rather
    /// than altered.
    pub digest: String,
}

/// The record of one retraction, appended to `umi-meta` and never modified.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Retraction {
    /// The repository the files came out of.
    pub repo: String,
    /// When it happened, milliseconds since the epoch.
    pub at_ms: u64,
    /// Why, in a sentence a stranger can read in a year. Required, for the same
    /// reason doc 07.7 requires one on a block.
    pub reason: String,
    /// Every file that went.
    pub removed: Vec<Removed>,
    /// The day manifests that were rewritten as a result, in day order. Longer
    /// than the days the files were in, because relinking the chain rewrites
    /// every later day too.
    pub rewritten: Vec<String>,
}

impl Retraction {
    /// The path this record takes in the meta repository.
    ///
    /// Named after the repository and the moment, so two retractions against
    /// the same repository never collide and the listing sorts into the order
    /// they happened.
    #[must_use]
    pub fn path(&self) -> String {
        let repo = self.repo.replace('/', "_");
        format!("{PREFIX}/{}-{repo}.json", self.at_ms)
    }

    /// The record as it is published.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] if it will not serialise, which it will.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|_| Error::Manifest("could not write the record"))
    }
}

/// What a retraction would do, worked out before anything is committed.
#[derive(Clone, Debug)]
pub struct Plan {
    /// The files that will go.
    pub removed: Vec<Removed>,
    /// The rewritten manifests, in day order, ready to commit.
    pub manifests: Vec<Manifest>,
}

/// Work out the new manifests for a repository with `paths` taken out.
///
/// `days` is every day manifest in the repository, in day order, which is the
/// order the chain runs in. The result carries the same days back with the
/// named files dropped and `prev` relinked from the first change forward.
///
/// A path that no manifest names is an error rather than a shrug. It means the
/// operator is deleting something this repository does not have, and the two
/// ways that happens are a typo and a file that was already removed, neither of
/// which should quietly commit.
///
/// A day left with no files is kept as an empty manifest rather than dropped,
/// because the chain runs through it and removing the link would break every
/// day after it a second time.
///
/// # Errors
///
/// [`Error::Manifest`] if a path is not in any of the days, or if a digest will
/// not compute.
pub fn plan(days: &[Manifest], paths: &[String]) -> Result<Plan> {
    let mut removed = Vec::new();
    for path in paths {
        let found = days
            .iter()
            .flat_map(|day| &day.files)
            .find(|entry| &entry.path == path)
            .ok_or(Error::Manifest("a path to retract is not in any manifest"))?;
        removed.push(Removed {
            path: found.path.clone(),
            rows: found.rows,
            bytes: found.bytes,
            digest: format!("blake3:{}", hex(&found.blake3)),
        });
    }

    let mut manifests = Vec::new();
    let mut prev: Option<[u8; 32]> = None;
    let mut relinking = false;
    for day in days {
        let keep: Vec<_> = day
            .files
            .iter()
            .filter(|entry| !paths.contains(&entry.path))
            .cloned()
            .collect();
        let changed = keep.len() != day.files.len();
        // Once one day has changed, every day after it has to be rewritten
        // whether its own files moved or not, because its `prev` no longer
        // names anything. Before the first change nothing is touched, so a
        // retraction against the last day of a long repository rewrites one
        // manifest and not all of them.
        if !changed && !relinking {
            prev = Some(day.digest()?);
            manifests.push(day.clone());
            continue;
        }
        relinking = true;
        let mut rewritten = day.clone();
        rewritten.files = keep;
        rewritten.prev = prev;
        prev = Some(rewritten.digest()?);
        manifests.push(rewritten);
    }

    Ok(Plan { removed, manifests })
}

/// Which of the planned manifests actually changed, as day strings.
///
/// The commit only carries these. A repository whose retraction touched the
/// last day should not rewrite the days before it, and comparing against what
/// was read is how that stays true without the planner having to remember.
#[must_use]
pub fn changed(before: &[Manifest], after: &[Manifest]) -> Vec<String> {
    after
        .iter()
        .zip(before)
        .filter(|(new, old)| new != old)
        .map(|(new, _)| new.day.clone())
        .collect()
}

/// Every day manifest in a repository, oldest first.
///
/// Oldest first because that is the order the chain runs in and the order
/// [`plan`] wants. The names are `YYYYMMDD`, so sorting them as text sorts them
/// as dates.
///
/// # Errors
///
/// Whatever the hub says, [`Error::Manifest`] for a repository with no
/// manifests at all, and for one in the listing that will not read or parse. A
/// day that cannot be read is fatal rather than skipped: a plan built on a
/// partial view of the chain would relink days to digests it never saw.
pub async fn days(hub: &Hub, repo: &str) -> Result<Vec<Manifest>> {
    let mut names: Vec<String> = hub
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
    names.sort();
    if names.is_empty() {
        return Err(Error::Manifest("the repository has no manifests"));
    }

    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let bytes = hub
            .read(repo, &format!("_manifest/{name}.json"))
            .await?
            .ok_or(Error::Manifest("a manifest in the listing would not read"))?;
        out.push(Manifest::parse(&bytes)?);
    }
    Ok(out)
}

/// Delete the files and commit the rewritten manifests in one commit.
///
/// One commit, for the reason in the module doc: a reader must never see a
/// manifest that names a file the same operation removed.
///
/// # Errors
///
/// Whatever the hub reports, and [`Error::Key`] if the key is not a publishing
/// key.
pub async fn commit(
    hub: &Hub,
    key: &SigningKey,
    repo: &str,
    paths: &[String],
    manifests: &[Manifest],
    reason: &str,
) -> Result<()> {
    let mut files: Vec<Upload> = paths
        .iter()
        .map(|path| Upload::Delete { path: path.clone() })
        .collect();
    for manifest in manifests {
        let signature = manifest.sign(key)?;
        files.push(Upload::Inline {
            path: format!("_manifest/{}.json", manifest.day),
            bytes: manifest.to_json()?,
        });
        files.push(Upload::Inline {
            path: format!("_manifest/{}.json.sig", manifest.day),
            bytes: signature.to_vec(),
        });
    }
    hub.upload(repo, &files, &format!("Retraction: {reason}"))
        .await?;
    Ok(())
}

/// Append the record to the meta repository.
///
/// Written before the deletion, not after. If the two cannot both happen the
/// survivable order is a record of a retraction that did not occur, which reads
/// as a mistake somebody can check, rather than files that vanished with
/// nothing saying why, which reads as an attack.
///
/// # Errors
///
/// Whatever the hub says, and [`Error::Manifest`] if something is already at
/// that path. The record list is append only, so a collision is a bug and not
/// something to write through.
pub async fn record(hub: &Hub, meta_repo: &str, retraction: &Retraction) -> Result<()> {
    hub.ensure_dataset(meta_repo).await?;
    let path = retraction.path();
    if hub.read(meta_repo, &path).await?.is_some() {
        return Err(Error::Manifest(
            "a retraction record is already at that path",
        ));
    }
    hub.upload(
        meta_repo,
        &[Upload::Inline {
            path,
            bytes: retraction.to_json()?,
        }],
        &format!("Record a retraction from {}", retraction.repo),
    )
    .await?;
    Ok(())
}

/// Lower case hex, for the digest a record carries.
fn hex(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
#[path = "retract_tests.rs"]
mod tests;
