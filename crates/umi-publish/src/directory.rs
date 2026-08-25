//! The published key directory, from `docs/spec/12-publishing.md` section 12.5.
//!
//! Doc 12.5 signs every manifest and says the three keys are published in
//! `umi-meta`. That second half is the one that makes the first half worth
//! anything: a signature is a claim about a key, and a key nobody outside this
//! machine can find turns the claim into a decoration. Doc 16's gate 1.5 says
//! it out loud, that a stranger on a clean machine has to verify a published
//! dataset from the published artifacts and the published keys alone, so the
//! key has to be somewhere they can get it before the first manifest goes up.
//!
//! # Which key signed this
//!
//! Nothing records that. A manifest carries no key identifier and the detached
//! signature is 64 raw bytes, so a verifier tries every publishing key in the
//! directory and accepts the manifest if one of them works. That sounds
//! wasteful and is not: the directory holds one entry per publishing key ever
//! used, Ed25519 verification is microseconds, and the alternative is a field
//! in the manifest that says which key to trust, which is a field an attacker
//! would fill in. Rotation costs nothing either, because an old key stays in
//! the directory and old manifests keep verifying.
//!
//! # What is not here
//!
//! Revocation. Doc 12.5 gives the three keys different rotation schedules but
//! says nothing about withdrawing one, and inventing a mechanism here would be
//! inventing policy. When it exists it belongs next to doc 06's ban list,
//! which is in `umi-meta` too.

use crate::hub::{Hub, Upload};
use crate::keys::{Role, VerifyingKey};
use crate::{Error, Result};

/// Where in `umi-meta` the publishing keys live.
pub const KEY_DIR: &str = "keys/publishing";

/// A short name for a key, from the key itself.
///
/// Eight bytes of blake3 over the raw public key, hex. It names the file in
/// the directory and gives an operator something to say out loud that is not
/// 64 characters long. It is not a security boundary: the file's contents are
/// the key and a verifier reads the key rather than the name.
#[must_use]
pub fn key_id(key: &VerifyingKey) -> String {
    let digest = blake3::hash(&key.to_bytes());
    hex::encode(&digest.as_bytes()[..8])
}

/// The path in `umi-meta` for one key.
#[must_use]
pub fn key_path(key: &VerifyingKey) -> String {
    format!("{KEY_DIR}/{}.json", key_id(key))
}

/// One entry in the directory, as it is written.
///
/// Deliberately small and deliberately readable. Somebody who wants to check
/// what we published should be able to `cat` this and understand all of it.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct KeyEntry {
    /// `publishing`, which is the only role this directory holds.
    pub role: String,
    /// The public key, hex, prefixed `ed25519:` so that the algorithm travels
    /// with the bytes rather than being assumed.
    pub key: String,
    /// [`key_id`] of the key above, repeated here so the file stands alone
    /// when it has been copied out of the repository.
    pub key_id: String,
    /// When it was added, in milliseconds. An argument everywhere it is
    /// produced, per doc 11.1, and never read from a clock in this crate.
    pub added_ms: u64,
}

impl KeyEntry {
    /// The entry for a key.
    #[must_use]
    pub fn new(key: &VerifyingKey, added_ms: u64) -> Self {
        Self {
            role: "publishing".to_owned(),
            key: format!("ed25519:{}", key.to_hex()),
            key_id: key_id(key),
            added_ms,
        }
    }

    /// The bytes as they go into the repository, with the trailing newline a
    /// text file gets so that `cat` behaves.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] if serialisation fails, which it does not for a
    /// struct of four owned scalars.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(self)
            .map_err(|_| Error::Manifest("the key entry would not serialise"))?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Read one back, checking the key is on the curve.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] when the document does not parse or is not a
    /// publishing key, and [`Error::Key`] when the key is not usable.
    pub fn parse(bytes: &[u8]) -> Result<(Self, VerifyingKey)> {
        let entry: Self = serde_json::from_slice(bytes)
            .map_err(|_| Error::Manifest("a key entry did not parse"))?;
        if entry.role != "publishing" {
            return Err(Error::Manifest("a key entry is not a publishing key"));
        }
        let hex_text = entry
            .key
            .strip_prefix("ed25519:")
            .ok_or(Error::Manifest("a key entry is not an ed25519 key"))?;
        let key = VerifyingKey::parse(Role::Publishing, hex_text)?;
        Ok((entry, key))
    }
}

/// Put a key in the directory if it is not there already.
///
/// Returns whether this call added it. Idempotent, because it runs before
/// every publishing session and the answer is no on all but the first.
///
/// The read before the write is not an optimisation. `umi-meta` is the one
/// repository doc 12.4 says we rewrite, and a commit per crawl that changes
/// nothing would bury the commits that do change something.
///
/// # Errors
///
/// Whatever the hub says. A key that cannot be published is a failure worth
/// stopping for, because a manifest signed by a key nobody can find is a
/// manifest nobody can check.
pub async fn publish_key(
    hub: &Hub,
    meta_repo: &str,
    key: &VerifyingKey,
    added_ms: u64,
) -> Result<bool> {
    let path = key_path(key);
    hub.ensure_dataset(meta_repo).await?;
    if hub.read(meta_repo, &path).await?.is_some() {
        return Ok(false);
    }
    let entry = KeyEntry::new(key, added_ms);
    hub.upload(
        meta_repo,
        &[Upload::Inline {
            path,
            bytes: entry.to_json()?,
        }],
        &format!("Add publishing key {}", entry.key_id),
    )
    .await?;
    Ok(true)
}

/// Every publishing key in the directory.
///
/// An empty directory is an empty list rather than an error, so that the
/// caller can say what an empty one means. To a verifier it means nothing
/// here can be checked, which is a much clearer message than "not found".
///
/// # Errors
///
/// When the hub will not answer, or when an entry is there and unreadable. A
/// malformed entry is not skipped: a directory that quietly drops the key that
/// signed the manifest in front of you produces a verification failure that
/// sends people to the wrong place.
pub async fn published_keys(hub: &Hub, meta_repo: &str) -> Result<Vec<VerifyingKey>> {
    let listing = hub.list(meta_repo, KEY_DIR).await?;
    let mut keys = Vec::with_capacity(listing.len());
    for entry in listing {
        if !entry.path.ends_with(".json") {
            continue;
        }
        let Some(bytes) = hub.read(meta_repo, &entry.path).await? else {
            continue;
        };
        keys.push(KeyEntry::parse(&bytes)?.1);
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::{KeyEntry, key_id, key_path};
    use crate::keys::{Role, SigningKey};

    fn key(seed: u8) -> crate::keys::VerifyingKey {
        SigningKey::from_seed(Role::Publishing, [seed; 32]).verifying()
    }

    #[test]
    fn an_entry_reads_back_as_the_key_that_wrote_it() {
        let key = key(9);
        let bytes = KeyEntry::new(&key, 1_787_000_000_000)
            .to_json()
            .expect("json");
        let (entry, read) = KeyEntry::parse(&bytes).expect("parse");
        assert_eq!(read.to_bytes(), key.to_bytes());
        assert_eq!(entry.key_id, key_id(&key));
        assert_eq!(entry.added_ms, 1_787_000_000_000);
        assert!(
            bytes.ends_with(b"\n"),
            "a text file in a repository ends with a newline"
        );
    }

    #[test]
    fn two_keys_do_not_share_a_file() {
        assert_ne!(key_path(&key(1)), key_path(&key(2)));
        assert!(key_path(&key(1)).starts_with("keys/publishing/"));
    }

    #[test]
    fn an_entry_that_is_not_a_publishing_key_is_refused() {
        let bytes = br#"{"role":"lease","key":"ed25519:00","key_id":"00","added_ms":0}"#;
        let error = KeyEntry::parse(bytes).expect_err("a lease key is not one of these");
        assert!(
            format!("{error}").contains("not a publishing key"),
            "{error}"
        );

        let bytes = br#"{"role":"publishing","key":"00","key_id":"00","added_ms":0}"#;
        let error = KeyEntry::parse(bytes).expect_err("no algorithm on the front");
        assert!(format!("{error}").contains("ed25519"), "{error}");
    }
}
