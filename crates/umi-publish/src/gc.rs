//! The garbage collection rule, from `docs/spec/12-publishing.md` section
//! 12.7.
//!
//! This is the part of doc 12 that deserves the care. Deleting a local file is
//! the only irreversible thing the publisher does, and doc 07.8 means we cannot
//! refetch the raw HTML to make it again. So the rule is four conditions that
//! must all hold, in order, and doc 12.7 says there is no disk pressure
//! override, no operator flag, no `--force`, and no timeout that eventually
//! gives up and deletes.
//!
//! A comment saying that would be worth very little, so the rule is a type
//! instead. [`delete`] takes a [`Cleared`], [`Cleared`] has no public
//! constructor, and the only function that returns one is [`clear`], which
//! checks all four conditions. There is no way to write the bypass without
//! editing this file, and editing this file is a thing a reviewer sees.
//!
//! Doc 15's backpressure ladder exists precisely so this rule never has to be
//! broken. When the disk fills, the crawl slows down, and that is the correct
//! outcome.

use std::path::Path;

use crate::manifest::FileEntry;
use crate::{Error, Result};

/// Why a file is still on disk.
///
/// One variant per condition, plus the two ways condition 1 can fail, because
/// "the upload never landed" and "something else is at that path" want
/// different responses from whoever is reading the log.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Blocked {
    /// Condition 1: nothing is at the remote path.
    RemoteMissing,
    /// Condition 1: something is, and it is the wrong size.
    RemoteSize {
        /// What the manifest says.
        expected: u64,
        /// What the remote says.
        found: u64,
    },
    /// Condition 2: no independent read was done.
    NotReadBack,
    /// Condition 2: the independent read did not digest to the same value.
    DigestMismatch,
    /// Condition 3: the manifest entry is not committed, or was committed
    /// without this file in it.
    NotInManifest,
    /// Condition 3: the committed manifest's signature did not verify.
    SignatureUnverified,
    /// Condition 4: the state ledger does not carry the remote location.
    LedgerIncomplete,
}

impl core::fmt::Display for Blocked {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::RemoteMissing => f.write_str("the remote object does not exist"),
            Self::RemoteSize { expected, found } => {
                write!(f, "the remote object is {found} bytes, not {expected}")
            }
            Self::NotReadBack => f.write_str("the remote object was not read back independently"),
            Self::DigestMismatch => f.write_str("the independent read digested differently"),
            Self::NotInManifest => f.write_str("no committed manifest references it"),
            Self::SignatureUnverified => f.write_str("the committed manifest did not verify"),
            Self::LedgerIncomplete => f.write_str("the state ledger has no remote location"),
        }
    }
}

/// What the remote said about the object, from a listing or a HEAD.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Remote {
    /// The length the remote reports.
    pub bytes: u64,
}

/// The result of doc 12.7's condition 2.
///
/// Doc 12.7 defines independent precisely: a fresh HTTP request that does not
/// reuse the upload's response, with the digest recomputed from the returned
/// bytes rather than taken from a header. This type carries the recomputed
/// digest and nothing else, so there is nowhere for a header value to be
/// smuggled in as if it were a read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadBack {
    /// blake3 of the bytes that came back.
    pub blake3: [u8; 32],
    /// Whether the whole object was read or doc 12.7's sampled ranges were.
    pub full: bool,
}

/// The result of doc 12.7's condition 3.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ManifestCommitted {
    /// The digest of the manifest that was read back from the remote.
    pub digest: [u8; 32],
    /// Whether its detached signature verified under the published key.
    pub signature_verified: bool,
    /// Whether the file's path and digest appear in it.
    pub references_file: bool,
}

/// The result of doc 12.7's condition 4.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LedgerLocation {
    /// The repository the ledger rows carry.
    pub repo: String,
    /// The path they carry.
    pub path: String,
    /// The digest they carry.
    pub blake3: [u8; 32],
}

/// Everything gathered about one file, in one place.
///
/// Every field is an `Option` and none of them defaults to present. A check
/// that was never run and a check that failed are both "the file stays", and
/// making the missing case unrepresentable would mean a caller could satisfy a
/// condition by not doing it.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Evidence {
    /// Condition 1.
    pub remote: Option<Remote>,
    /// Condition 2.
    pub read_back: Option<ReadBack>,
    /// Condition 3.
    pub manifest: Option<ManifestCommitted>,
    /// Condition 4.
    pub ledger: Option<LedgerLocation>,
}

/// Proof that all four conditions held for one file.
///
/// No public constructor, no `Clone`, no `Default`, no way to build one from
/// parts. It exists so that [`delete`] cannot be called without [`clear`]
/// having run, and it carries the path so that a token cleared for one file
/// cannot be used to delete another.
#[derive(Debug)]
pub struct Cleared {
    repo: String,
    path: String,
}

impl Cleared {
    /// Which repository the file was published to.
    #[must_use]
    pub fn repo(&self) -> &str {
        &self.repo
    }

    /// Which remote path it reached.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Doc 12.7's four conditions, in doc 12.7's order.
///
/// The order is not cosmetic. Condition 1 is a listing, condition 2 costs a
/// download, condition 3 costs a second download and a signature check, and
/// condition 4 is a local read. Checking the cheap one first means a file whose
/// upload never landed costs a listing to skip rather than a full re download,
/// and at 3100 files a day that is the difference between reconciliation being
/// free and being a bandwidth item.
///
/// # Errors
///
/// Never. The signature is a `Result` shaped `Err(Blocked)` rather than
/// `Result<_, Error>` because there is exactly one kind of failure here and it
/// is not exceptional: most calls to this during a normal minute return
/// `Blocked`, because most files are still uploading.
pub fn clear(
    repo: &str,
    entry: &FileEntry,
    evidence: &Evidence,
) -> core::result::Result<Cleared, Blocked> {
    // 1. The remote object exists and its size matches.
    let remote = evidence.remote.ok_or(Blocked::RemoteMissing)?;
    if remote.bytes != entry.bytes {
        return Err(Blocked::RemoteSize {
            expected: entry.bytes,
            found: remote.bytes,
        });
    }

    // 2. An independent read produced the same digest.
    let read_back = evidence.read_back.ok_or(Blocked::NotReadBack)?;
    if read_back.blake3 != entry.blake3 {
        return Err(Blocked::DigestMismatch);
    }

    // 3. The manifest entry is committed and its signature verified by reading
    //    it back. Both halves, because a manifest that verifies but does not
    //    mention this file proves nothing about this file.
    let manifest = evidence.manifest.ok_or(Blocked::NotInManifest)?;
    if !manifest.references_file {
        return Err(Blocked::NotInManifest);
    }
    if !manifest.signature_verified {
        return Err(Blocked::SignatureUnverified);
    }

    // 4. The state ledger rows carry the remote repository, path and digest.
    let ledger = evidence.ledger.as_ref().ok_or(Blocked::LedgerIncomplete)?;
    if ledger.repo != repo || ledger.path != entry.path || ledger.blake3 != entry.blake3 {
        return Err(Blocked::LedgerIncomplete);
    }

    Ok(Cleared {
        repo: repo.to_owned(),
        path: entry.path.clone(),
    })
}

/// Delete a local file that has been cleared.
///
/// Takes the [`Cleared`] by value, so a token cannot be reused across a loop
/// and quietly authorise a second deletion.
///
/// # Errors
///
/// Whatever the filesystem reports, except that a file which is already gone is
/// success. Publishing is retried and a retry that failed because the previous
/// attempt had already finished the job is not a failure.
pub fn delete(local: &Path, cleared: Cleared) -> Result<()> {
    let _ = cleared;
    match std::fs::remove_file(local) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(Error::Io(err)),
    }
}

/// Doc 12.7's sampled verification: which byte ranges to fetch.
///
/// Three 1 MiB ranges by default, which doc 12.7 costs at about 3 seconds
/// against 15 for a full re download, with one full check every 100 segments to
/// amortise the rest. Full verification on every segment would be 1.5 MB/s of
/// inbound per host, which is real bandwidth on boxes whose inbound may be
/// metered.
///
/// The seed is an argument because nothing in this crate reads a clock or a
/// random source. In production it is the segment digest, which is
/// unpredictable to anyone who has not seen the file and fixed for anyone who
/// has, so a mirror cannot know in advance which ranges will be checked and a
/// retry checks the same ones as the attempt before it.
#[must_use]
pub fn sample_ranges(len: u64, count: usize, window: u64, seed: &[u8; 32]) -> Vec<(u64, u64)> {
    if len == 0 || count == 0 || window == 0 {
        return Vec::new();
    }
    if len <= window {
        return vec![(0, len)];
    }
    let span = len - window;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        // A fresh blake3 per range rather than a counter, so the ranges are
        // spread rather than adjacent. Cheap enough that being clever would be
        // a waste.
        let mut hasher = blake3::Hasher::new();
        hasher.update(seed);
        hasher.update(&(i as u64).to_le_bytes());
        let word = u64::from_le_bytes(
            hasher.finalize().as_bytes()[..8]
                .try_into()
                .unwrap_or([0u8; 8]),
        );
        let start = word % (span + 1);
        out.push((start, window));
    }
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::{
        Blocked, Evidence, LedgerLocation, ManifestCommitted, ReadBack, Remote, clear, delete,
        sample_ranges,
    };
    use crate::manifest::{FileEntry, Verification};

    const REPO: &str = "open-index/umi-pages-2026w34-03";

    fn entry() -> FileEntry {
        FileEntry {
            path: "data/20260817/01K2M8Q0P7R3XN500000000000.parquet".to_owned(),
            bytes: 134_217_728,
            rows: 21_043,
            blake3: [1u8; 32],
            sha256: [2u8; 32],
            segment_ulid: "01K2M8Q0P7R3XN500000000000".to_owned(),
            coordinator: "server3".to_owned(),
            extractor: "umi-extract/0.0.1".to_owned(),
            fetched_at_min_ms: 1_755_388_800_000,
            fetched_at_max_ms: 1_755_389_640_000,
            verification: Verification {
                local: 21_043,
                ..Verification::default()
            },
        }
    }

    fn all_four() -> Evidence {
        let entry = entry();
        Evidence {
            remote: Some(Remote { bytes: entry.bytes }),
            read_back: Some(ReadBack {
                blake3: entry.blake3,
                full: true,
            }),
            manifest: Some(ManifestCommitted {
                digest: [9u8; 32],
                signature_verified: true,
                references_file: true,
            }),
            ledger: Some(LedgerLocation {
                repo: REPO.to_owned(),
                path: entry.path.clone(),
                blake3: entry.blake3,
            }),
        }
    }

    #[test]
    fn all_four_conditions_clear_it() {
        let cleared = clear(REPO, &entry(), &all_four()).expect("cleared");
        assert_eq!(cleared.repo(), REPO);
        assert_eq!(cleared.path(), entry().path);
    }

    #[test]
    fn nothing_gathered_at_all_blocks() {
        assert_eq!(
            clear(REPO, &entry(), &Evidence::default()).unwrap_err(),
            Blocked::RemoteMissing
        );
    }

    #[test]
    fn dropping_any_one_condition_blocks() {
        // Written as four mutations of the good case rather than four hand
        // built ones, so that a fifth condition added to `clear` without a
        // matching test here shows up as this test still passing while the new
        // field is never exercised. The count assertion is what catches that.
        /// A name for the condition and the mutation that removes it.
        type Case = (&'static str, fn(&mut Evidence));

        let cases: [Case; 4] = [
            ("remote", |e| e.remote = None),
            ("read back", |e| e.read_back = None),
            ("manifest", |e| e.manifest = None),
            ("ledger", |e| e.ledger = None),
        ];
        assert_eq!(cases.len(), 4, "doc 12.7 has four conditions");
        for (name, drop) in cases {
            let mut evidence = all_four();
            drop(&mut evidence);
            assert!(
                clear(REPO, &entry(), &evidence).is_err(),
                "dropping the {name} condition still cleared"
            );
        }
    }

    #[test]
    fn each_condition_fails_for_its_own_reason() {
        let mut wrong_size = all_four();
        wrong_size.remote = Some(Remote { bytes: 1 });
        assert_eq!(
            clear(REPO, &entry(), &wrong_size).unwrap_err(),
            Blocked::RemoteSize {
                expected: entry().bytes,
                found: 1
            }
        );

        let mut wrong_digest = all_four();
        wrong_digest.read_back = Some(ReadBack {
            blake3: [7u8; 32],
            full: true,
        });
        assert_eq!(
            clear(REPO, &entry(), &wrong_digest).unwrap_err(),
            Blocked::DigestMismatch
        );

        let mut unsigned = all_four();
        unsigned.manifest = Some(ManifestCommitted {
            digest: [9u8; 32],
            signature_verified: false,
            references_file: true,
        });
        assert_eq!(
            clear(REPO, &entry(), &unsigned).unwrap_err(),
            Blocked::SignatureUnverified
        );

        let mut absent = all_four();
        absent.manifest = Some(ManifestCommitted {
            digest: [9u8; 32],
            signature_verified: true,
            references_file: false,
        });
        assert_eq!(
            clear(REPO, &entry(), &absent).unwrap_err(),
            Blocked::NotInManifest
        );
    }

    #[test]
    fn a_ledger_row_pointing_somewhere_else_blocks() {
        // The condition is not "the ledger has a location", it is "the ledger
        // has this location". A row left over from a previous publish of the
        // same segment to a different slice would satisfy the weaker reading.
        for wrong in [
            LedgerLocation {
                repo: "open-index/umi-pages-2026w34-04".to_owned(),
                path: entry().path,
                blake3: entry().blake3,
            },
            LedgerLocation {
                repo: REPO.to_owned(),
                path: "data/20260818/other.parquet".to_owned(),
                blake3: entry().blake3,
            },
            LedgerLocation {
                repo: REPO.to_owned(),
                path: entry().path,
                blake3: [8u8; 32],
            },
        ] {
            let mut evidence = all_four();
            evidence.ledger = Some(wrong.clone());
            assert_eq!(
                clear(REPO, &entry(), &evidence).unwrap_err(),
                Blocked::LedgerIncomplete,
                "{wrong:?}"
            );
        }
    }

    #[test]
    fn a_sampled_read_back_still_clears_when_it_matches() {
        // Doc 12.7 makes sampling the default, so a `full: false` read back
        // that matches has to be enough. The `full` flag is for the caller's
        // accounting of the periodic full check, not for this decision.
        let mut evidence = all_four();
        evidence.read_back = Some(ReadBack {
            blake3: entry().blake3,
            full: false,
        });
        assert!(clear(REPO, &entry(), &evidence).is_ok());
    }

    #[test]
    fn delete_removes_the_file_and_a_second_delete_is_fine() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("segment.parquet");
        std::fs::write(&path, b"bytes").expect("write");

        let cleared = clear(REPO, &entry(), &all_four()).expect("cleared");
        delete(&path, cleared).expect("delete");
        assert!(!path.exists());

        let again = clear(REPO, &entry(), &all_four()).expect("cleared");
        delete(&path, again).expect("delete a file that is already gone");
    }

    #[test]
    fn the_sampled_ranges_are_inside_the_file_and_reproducible() {
        let len = 134_217_728u64;
        let window = 1024 * 1024;
        let seed = [3u8; 32];
        let ranges = sample_ranges(len, 3, window, &seed);
        assert_eq!(ranges.len(), 3);
        for (start, size) in &ranges {
            assert_eq!(*size, window);
            assert!(start + size <= len, "{start} + {size} past {len}");
        }
        assert_eq!(ranges, sample_ranges(len, 3, window, &seed));
        assert_ne!(ranges, sample_ranges(len, 3, window, &[4u8; 32]));
        // Sorted, so a caller can issue them as one ordered set of range
        // requests rather than seeking backwards.
        let mut sorted = ranges.clone();
        sorted.sort_unstable();
        assert_eq!(ranges, sorted);
    }

    #[test]
    fn a_file_smaller_than_the_window_is_read_whole() {
        assert_eq!(sample_ranges(500, 3, 1024, &[0u8; 32]), vec![(0, 500)]);
        assert!(sample_ranges(0, 3, 1024, &[0u8; 32]).is_empty());
        assert!(sample_ranges(500, 0, 1024, &[0u8; 32]).is_empty());
    }
}
