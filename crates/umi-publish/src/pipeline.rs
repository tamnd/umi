//! Doc 12.2's eight steps, run in order, for one sealed segment.
//!
//! Every other module in this crate is a piece: [`convert`](mod@crate::convert)
//! turns a segment into Parquet, [`hub`](mod@crate::hub) talks to Hugging Face,
//! [`manifest`](mod@crate::manifest) builds and signs the day document, and
//! [`gc`](mod@crate::gc) decides whether a local file may be deleted. This is the
//! module that runs them in the order doc 12.2 gives, with the state ledger in
//! the middle where step 7 puts it.
//!
//! ```text
//! 1  verify every chunk checksum      convert, on the way through
//! 2  convert shoals to row groups     convert
//! 3  digest the Parquet               convert
//! 4  upload to Hugging Face           hub::upload
//! 5  verify the remote copy           read_back, below
//! 6  append, sign and push the day    manifest, then hub::upload again
//! 7  write remote locations to state  State::put_segment
//! 8  GC deletes the local files       gc::clear then gc::delete
//! ```
//!
//! # Why the order cannot be rearranged
//!
//! Doc 12.6 says the manifest is committed last, after every file it
//! references, and doc 12.7's third condition is that a committed manifest
//! names the file. Together those mean the manifest commit is the point of no
//! return: before it, a crash leaves an orphan Parquet file that doc 12.8
//! adopts on the next pass, and after it, the file is published whether or not
//! this process survives to write the ledger row. Step 7 comes after step 6 for
//! that reason and not for tidiness, and step 8 comes after step 7 because the
//! ledger row is what a later reconciliation reads to know the local file was
//! safe to lose.
//!
//! # Time and randomness
//!
//! Neither is read here. `now_ms` is an argument to [`Publisher::publish`], the
//! sampled verification seed is the segment's own digest, and the repository a
//! segment lands in comes from its earliest fetch time rather than from when
//! the publisher happened to run. That is doc 11.1, and it is what lets the
//! tests in this crate assert on exact paths and exact manifest digests.

use std::path::{Path, PathBuf};

use umi_file::{Segment, StreamKind};
use umi_state::{RemoteCopy, SegmentQuery, SegmentRow, State, Stream};
use umi_types::{Digest, Ulid};

use crate::convert::{Converted, convert};
use crate::gc::{self, Blocked, Evidence, LedgerLocation, ManifestCommitted, ReadBack};
use crate::hub::{Hub, Upload};
use crate::keys::SigningKey;
use crate::manifest::{FileEntry, Manifest, Verification, segment_text};
use crate::repo::{self, Corpus, Family, Location};
use crate::{Error, Result};

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod tests;

/// Everything about publishing that is not the hub and not the key.
#[derive(Clone, Debug)]
pub struct PublishConfig {
    /// Where Parquet files are staged before they go up.
    ///
    /// Separate from the segment directory on purpose. A staged file is
    /// temporary and a segment is not, and a reconciliation pass that walked
    /// the segment directory should not have to tell them apart by extension.
    pub staging: PathBuf,
    /// Which corpus this is publishing into: the organisation doc 14.7 spells
    /// `publish.org`, and the focused crawl name from doc 13.7 if there is one.
    pub corpus: Corpus,
    /// Doc 12.4's `NN`, the slice inside the week's repository family.
    pub slice: u16,
    /// Doc 04's coordinator key, hex, for the manifest entry.
    pub coordinator: String,
    /// Doc 11.3's extractor version, as the manifest writes it.
    pub extractor: String,
    /// How many ranges doc 12.7's sampled verification reads. Three.
    pub samples: usize,
    /// How long each one is. One MiB.
    pub window: u64,
    /// One segment in this many is read back whole instead of sampled.
    ///
    /// Doc 12.7 costs a full re download at 15 seconds against 3 for the
    /// sample, and amortises the difference by doing one in a hundred. Zero
    /// means never, which no production configuration should use and a test
    /// that only cares about the sampled path may.
    pub full_every: u64,
}

impl Default for PublishConfig {
    fn default() -> Self {
        Self {
            staging: PathBuf::from("parquet"),
            corpus: Corpus::new(repo::ORG),
            slice: 0,
            coordinator: String::new(),
            extractor: format!("umi/{}", env!("CARGO_PKG_VERSION")),
            samples: 3,
            window: 1024 * 1024,
            full_every: 100,
        }
    }
}

/// What publishing one segment produced.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Published {
    /// Which segment.
    pub segment: Ulid,
    /// Where it went.
    pub repo: String,
    /// The repository relative path of the Parquet file.
    pub path: String,
    /// The `YYYYMMDD` day, as the number the state ledger stores.
    pub day: u32,
    /// blake3 of the published file.
    pub digest: Digest,
    /// How many rows.
    pub rows: u64,
    /// How many bytes the Parquet file is, which is not how many the segment
    /// was.
    pub bytes: u64,
    /// Whether step 8 deleted the local segment, and if not, why not.
    ///
    /// `None` means it was deleted. A publisher that got all the way here and
    /// still could not clear the file has hit something worth reading about,
    /// and swallowing it would leave a disk filling up with no explanation.
    pub blocked: Option<Blocked>,
}

/// Runs doc 12.2's pipeline against one hub, one key and one staging directory.
pub struct Publisher {
    hub: Hub,
    key: SigningKey,
    config: PublishConfig,
}

impl Publisher {
    /// Build a publisher.
    ///
    /// # Errors
    ///
    /// Whatever creating the staging directory reports.
    pub fn new(hub: Hub, key: SigningKey, config: PublishConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.staging)?;
        Ok(Self { hub, key, config })
    }

    /// The hub this publishes to, for a caller that wants to check the token
    /// before starting.
    #[must_use]
    pub const fn hub(&self) -> &Hub {
        &self.hub
    }

    /// Publish every segment the state ledger has no remote copy for.
    ///
    /// Oldest first, which is [`SegmentQuery::Unpublished`]'s order and also
    /// the order that keeps the disk shrinking: the oldest segment is the one
    /// whose deletion buys the most time before doc 15's backpressure ladder
    /// has to slow the crawl down.
    ///
    /// One failure does not stop the rest. A segment whose upload fails is
    /// left unpublished, which means the next call picks it up again, and that
    /// is the whole retry mechanism. Doc 12.1's bias applies: a publisher that
    /// gave up on the batch because one file would not go up is a publisher
    /// that fills a disk over one bad segment.
    ///
    /// # Errors
    ///
    /// Only when the state ledger cannot be read. Everything else is reported
    /// per segment.
    pub async fn drain(
        &self,
        state: &dyn State,
        now_ms: u64,
    ) -> Result<(Vec<Published>, Vec<(Ulid, Error)>)> {
        let due = state.segments(SegmentQuery::Unpublished).await?;
        let mut done = Vec::new();
        let mut failed = Vec::new();
        for row in due {
            match self.publish(state, &row, now_ms).await {
                Ok(published) => done.push(published),
                Err(error) => failed.push((row.id, error)),
            }
        }
        Ok((done, failed))
    }

    /// Delete the local files of segments that were published earlier.
    ///
    /// Step 8 runs inside [`Publisher::publish`] for the segment it just
    /// pushed, so this is for the ones that got past step 6 and then lost the
    /// process: their ledger row is complete, their local file is still there,
    /// and nothing else would ever come back for them.
    ///
    /// # Errors
    ///
    /// Only when the state ledger cannot be read or written.
    pub async fn collect(&self, state: &dyn State, now_ms: u64) -> Result<usize> {
        let mut deleted = 0;
        for row in state.segments(SegmentQuery::Collectable).await? {
            let Some(remote) = row.remote.clone() else {
                continue;
            };
            // The four conditions were checked once already, when this row was
            // written, and the file is still on disk because the process died
            // between the two. Re checking the remote here would cost a listing
            // and a read back per segment for a fact the ledger already
            // records, and the ledger is the thing doc 12.7 says to trust for
            // condition 4. So this rebuilds the evidence from what was
            // recorded, which is honest as long as the recording only ever
            // happens after all four held, and it does: nothing else in this
            // crate writes a `RemoteCopy`.
            let entry = FileEntry {
                path: remote.path.clone(),
                bytes: row.bytes,
                rows: row.rows,
                blake3: *remote.digest.as_bytes(),
                sha256: [0u8; 32],
                segment_ulid: row.id.to_text(),
                coordinator: self.config.coordinator.clone(),
                extractor: self.config.extractor.clone(),
                fetched_at_min_ms: 0,
                fetched_at_max_ms: 0,
                verification: Verification::default(),
            };
            let evidence = Evidence {
                remote: Some(gc::Remote { bytes: row.bytes }),
                read_back: Some(ReadBack {
                    blake3: *remote.digest.as_bytes(),
                    full: false,
                }),
                manifest: Some(ManifestCommitted {
                    digest: [0u8; 32],
                    signature_verified: true,
                    references_file: true,
                }),
                ledger: Some(LedgerLocation {
                    repo: remote.repo.clone(),
                    path: remote.path.clone(),
                    blake3: *remote.digest.as_bytes(),
                }),
            };
            if let Ok(cleared) = gc::clear(&remote.repo, &entry, &evidence) {
                gc::delete(Path::new(&row.local_path), cleared)?;
                self.mark_deleted(state, &row, now_ms).await?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Doc 12.2's eight steps for one segment.
    ///
    /// # Errors
    ///
    /// Anything the conversion, the hub, the manifest or the state ledger
    /// reports. A failure before step 6 leaves nothing published; a failure
    /// after it leaves a published file whose ledger row is missing, which is
    /// the case doc 12.8's reconciliation adopts.
    pub async fn publish(
        &self,
        state: &dyn State,
        row: &SegmentRow,
        now_ms: u64,
    ) -> Result<Published> {
        let stream = stream_kind(row.stream);

        // Steps 1, 2 and 3. The checksum verification is folded into the
        // conversion because the bytes are in memory anyway, and both digests
        // come off the finished file rather than off the Arrow.
        let staged = self
            .config
            .staging
            .join(format!("{}.parquet", row.id.to_text()));
        let converted = {
            let segment = Segment::open(Path::new(&row.local_path))?;
            convert(&segment, &staged)?
        };

        let location = self.config.corpus.locate(
            Family::of(stream),
            converted.first_ms,
            self.config.slice,
            row.id,
        );
        let entry = self.entry(row, &location, &converted);

        // Step 4. `ensure_dataset` first, because the first segment of a week
        // lands in a repository nobody has created, and a create that finds it
        // already there is a 409 the hub client reads as success.
        self.hub.ensure_dataset(&location.repo).await?;
        self.hub
            .upload(
                &location.repo,
                &[Upload::Blob {
                    path: location.path.clone(),
                    local: staged.clone(),
                    size: converted.bytes,
                    sha256: hex::encode(converted.sha256),
                }],
                &format!("Add {}", location.path),
            )
            .await?;

        // Step 5. Both halves of it: the hub's own idea of the object's size,
        // and bytes read back over a fresh request and digested here.
        let remote = self.remote(&location).await?;
        let read_back = self.read_back(&location, &staged, &converted).await?;

        // Step 6. The manifest is built from what the hub has, not from what
        // this process remembers, so that a publisher restarted mid day
        // appends to the day rather than replacing it.
        let manifest = self.append(&location, stream, entry.clone()).await?;
        let committed = self.commit_manifest(&location, &manifest).await?;

        // Conditions 1, 2 and 3, before anything is recorded. Running the real
        // rule with no ledger is the tidy way to ask "is everything except the
        // ledger in order": `clear` checks the conditions in order and stops at
        // the first failure, so `LedgerIncomplete` is exactly the answer that
        // the other three held.
        //
        // This gate is what makes `collect` sound. That method rebuilds the
        // evidence from the ledger row rather than re checking the hub, which
        // is only honest if a `RemoteCopy` is never written for a copy that did
        // not verify. So it is not written. A segment that fails here stays
        // unpublished and the next drain tries it again.
        let checked = Evidence {
            remote: Some(remote),
            read_back: Some(read_back),
            manifest: Some(committed),
            ledger: None,
        };
        if let Err(blocked) = gc::clear(&location.repo, &entry, &checked)
            && blocked != Blocked::LedgerIncomplete
        {
            let _ = std::fs::remove_file(&staged);
            return Err(Error::NotPublished(blocked));
        }

        // Step 7. Durable, and the reason `put_segment` is the one method in
        // the state trait that always fsyncs.
        let day = day_number(&location.day);
        let published = SegmentRow {
            remote: Some(RemoteCopy {
                repo: location.repo.clone(),
                path: location.path.clone(),
                // What came back, not what went out. They are equal or the
                // read back above would have failed, and writing the one that
                // was checked is what makes the ledger row evidence rather
                // than a restatement of an intention.
                digest: Digest::from_bytes(read_back.blake3),
            }),
            manifest_day: Some(day),
            ..row.clone()
        };
        state.put_segment(std::slice::from_ref(&published)).await?;

        // Step 8. Condition 4 is read back out of the ledger rather than
        // assumed from the value just written, because the point of the
        // condition is that the record survived, and the only way to know that
        // is to ask.
        let ledger = state.segment(row.id).await?.and_then(|stored| {
            stored.remote.map(|copy| LedgerLocation {
                repo: copy.repo,
                path: copy.path,
                blake3: *copy.digest.as_bytes(),
            })
        });
        let evidence = Evidence { ledger, ..checked };

        let blocked = match gc::clear(&location.repo, &entry, &evidence) {
            Ok(cleared) => {
                gc::delete(Path::new(&row.local_path), cleared)?;
                self.mark_deleted(state, &published, now_ms).await?;
                None
            }
            Err(blocked) => Some(blocked),
        };
        // The staged copy goes either way. It is not the published file and it
        // is not the segment, so nothing in doc 12.7 protects it, and leaving
        // it would double the disk cost of publishing.
        let _ = std::fs::remove_file(&staged);

        Ok(Published {
            segment: row.id,
            repo: location.repo,
            path: location.path,
            day,
            digest: Digest::from_bytes(converted.blake3),
            rows: converted.rows,
            bytes: converted.bytes,
            blocked,
        })
    }

    /// The manifest entry for one converted segment.
    fn entry(&self, row: &SegmentRow, location: &Location, converted: &Converted) -> FileEntry {
        let tally = converted.verification;
        FileEntry {
            path: location.path.clone(),
            bytes: converted.bytes,
            rows: converted.rows,
            blake3: converted.blake3,
            sha256: converted.sha256,
            segment_ulid: segment_text(*row.id.as_bytes()),
            coordinator: self.config.coordinator.clone(),
            extractor: self.config.extractor.clone(),
            fetched_at_min_ms: converted.first_ms,
            fetched_at_max_ms: converted.last_ms,
            verification: Verification {
                local: tally.local,
                quorum: tally.quorum,
                replayed: tally.replayed,
                unverified: tally.unverified,
            },
        }
    }

    /// Doc 12.7's first condition: the object is there and it is the right
    /// size.
    async fn remote(&self, location: &Location) -> Result<gc::Remote> {
        let found = self
            .hub
            .info(&location.repo, &location.path)
            .await?
            .ok_or(Error::Manifest("the upload is not on the hub"))?;
        Ok(gc::Remote { bytes: found.size })
    }

    /// Doc 12.7's second condition: bytes read back over a fresh request,
    /// digested here.
    ///
    /// The sampled form is the interesting one. Doc 12.7 asks for three 1 MiB
    /// ranges rather than a full re download, and doc 12.7 also asks for the
    /// recomputed digest to equal the published one, which cannot come from
    /// three ranges on its own. So the ranges are spliced into the local copy
    /// and the result is digested whole. If every fetched range matches the
    /// bytes underneath it, the splice is the local file and the digest is the
    /// published digest; if any range differs by a byte, blake3 says so. The
    /// claim that buys is precise and worth stating plainly: the remote object
    /// matched the local one at the sampled offsets, and doc 12.7's one in a
    /// hundred full read is what covers the rest.
    async fn read_back(
        &self,
        location: &Location,
        staged: &Path,
        converted: &Converted,
    ) -> Result<ReadBack> {
        let full = self.config.full_every > 0 && {
            // Which segments get the full treatment is decided by the digest,
            // so it is spread evenly, fixed for a given file, and not
            // something a mirror can predict without having the file.
            let mut pick = [0u8; 8];
            pick.copy_from_slice(&converted.blake3[..8]);
            u64::from_le_bytes(pick) % self.config.full_every == 0
        };

        let mut bytes = std::fs::read(staged)?;
        if full {
            let whole = self
                .hub
                .read(&location.repo, &location.path)
                .await?
                .ok_or(Error::Manifest("the upload was gone by the read back"))?;
            return Ok(ReadBack {
                blake3: *blake3::hash(&whole).as_bytes(),
                full: true,
            });
        }

        for (at, len) in gc::sample_ranges(
            converted.bytes,
            self.config.samples,
            self.config.window,
            &converted.blake3,
        ) {
            let fetched = self
                .hub
                .read_range(&location.repo, &location.path, at, len)
                .await?;
            let at = at as usize;
            let end = (at + fetched.len()).min(bytes.len());
            bytes[at..end].copy_from_slice(&fetched[..end - at]);
        }
        Ok(ReadBack {
            blake3: *blake3::hash(&bytes).as_bytes(),
            full: false,
        })
    }

    /// Read the day's manifest off the hub, add this file, and return it
    /// unsigned.
    async fn append(
        &self,
        location: &Location,
        stream: StreamKind,
        entry: FileEntry,
    ) -> Result<Manifest> {
        let path = location.manifest_path();
        let mut manifest = match self.hub.read(&location.repo, &path).await? {
            Some(bytes) => Manifest::parse(&bytes)?,
            None => {
                let prev = self.previous(location).await?;
                Manifest::new(&location.repo, &location.day, stream, prev)
            }
        };
        manifest.insert(entry);
        Ok(manifest)
    }

    /// The digest of the day before this one in the same repository, if there
    /// is one.
    ///
    /// Doc 12.5's chain is per repository and each link points at the digest of
    /// the document, so it has to be recomputed from the bytes rather than read
    /// out of a field. A repository whose first day this is has no previous,
    /// and that is the head recorded in `umi-meta`.
    async fn previous(&self, location: &Location) -> Result<Option<[u8; 32]>> {
        let today = location.manifest_path();
        let listing = self.hub.list(&location.repo, "_manifest").await?;
        let latest = listing
            .into_iter()
            .map(|entry| entry.path)
            .filter(|path| path.ends_with(".json") && *path < today)
            .max();
        let Some(latest) = latest else {
            return Ok(None);
        };
        match self.hub.read(&location.repo, &latest).await? {
            Some(bytes) => Ok(Some(Manifest::parse(&bytes)?.digest()?)),
            None => Ok(None),
        }
    }

    /// Sign the manifest, push it and its signature, then read both back.
    ///
    /// Read back rather than assumed, because doc 12.7's third condition is
    /// about what is committed and a commit that returned success is not the
    /// same fact. The signature is verified against this publisher's own
    /// verifying key, which checks that the bytes on the hub are the bytes that
    /// were signed and nothing more; a consumer verifies against the key
    /// published in `umi-meta`, which is the check that matters to them.
    async fn commit_manifest(
        &self,
        location: &Location,
        manifest: &Manifest,
    ) -> Result<ManifestCommitted> {
        let signature = manifest.sign(&self.key)?;
        self.hub
            .upload(
                &location.repo,
                &[
                    Upload::Inline {
                        path: location.manifest_path(),
                        bytes: manifest.to_json()?,
                    },
                    Upload::Inline {
                        path: location.signature_path(),
                        bytes: signature.to_vec(),
                    },
                ],
                &format!("Manifest for {}", location.day),
            )
            .await?;

        let bytes = self
            .hub
            .read(&location.repo, &location.manifest_path())
            .await?
            .ok_or(Error::Manifest("the manifest was gone after the commit"))?;
        let committed = Manifest::parse(&bytes)?;
        let signature = self
            .hub
            .read(&location.repo, &location.signature_path())
            .await?
            .ok_or(Error::Manifest("the signature was gone after the commit"))?;
        let signature: [u8; 64] = signature
            .as_slice()
            .try_into()
            .map_err(|_| Error::Manifest("the signature is not 64 bytes"))?;

        Ok(ManifestCommitted {
            digest: committed.digest()?,
            signature_verified: committed.verify(&self.key.verifying(), &signature).is_ok(),
            references_file: committed.contains(&location.path),
        })
    }

    /// Record that the local file is gone.
    async fn mark_deleted(&self, state: &dyn State, row: &SegmentRow, now_ms: u64) -> Result<()> {
        let gone = SegmentRow {
            deleted_at_ms: Some(now_ms),
            ..row.clone()
        };
        state.put_segment(&[gone]).await?;
        Ok(())
    }
}

/// The three arm match doc 08.3 promised.
///
/// [`umi_state::Stream`] and [`umi_file::StreamKind`] are the same three
/// values written out in two crates, because the state layer does not depend
/// on the file format and should not. This is the one place both are in scope,
/// and the test below asserts the discriminants still agree.
#[must_use]
pub const fn stream_kind(stream: Stream) -> StreamKind {
    match stream {
        Stream::Pages => StreamKind::Pages,
        Stream::Receipts => StreamKind::Receipts,
        Stream::Robots => StreamKind::Robots,
    }
}

/// The other direction, for a caller writing a sealed segment into state.
#[must_use]
pub const fn stream_of(kind: StreamKind) -> Stream {
    match kind {
        StreamKind::Pages => Stream::Pages,
        StreamKind::Receipts => Stream::Receipts,
        StreamKind::Robots => Stream::Robots,
    }
}

/// `20260825` from `"20260825"`.
///
/// The state ledger stores the day as a number because that is what sorts and
/// compares without a string allocation on every row, and doc 12.4 writes it as
/// a folder name. A day folder that is not eight digits cannot come out of
/// [`crate::repo::locate`], so this returns zero rather than failing, and zero
/// is a day nothing was ever fetched in.
fn day_number(day: &str) -> u32 {
    day.parse().unwrap_or(0)
}

impl From<umi_state::StateError> for Error {
    fn from(error: umi_state::StateError) -> Self {
        Self::State(error.to_string())
    }
}
