//! The Hugging Face side of doc 12.6.
//!
//! Steps 4 and part of 5 in doc 12.2's pipeline: get a Parquet file onto a
//! dataset repository, get a manifest on after it, and read bytes back off
//! well enough that doc 12.7's second condition means something.
//!
//! # Why this is written against the HTTP API
//!
//! There is a git path to the same place and it is the wrong one for this. A
//! 128 MB file every eight minutes on three machines is a repository with
//! three thousand commits a day in it, and `git` would want a working copy of
//! all of it on a disk that doc 12.1 already said holds under a day of
//! crawling. The commit endpoint takes files it has never seen a history for,
//! which is exactly the shape of what a publisher does.
//!
//! # The upload, end to end
//!
//! ```text
//! preupload        does the hub already have these bytes, and how should
//!                  they go up: inline in the commit, or through lfs
//! lfs batch        an upload url for each object it does not have
//! put              the bytes, in one request or in parts
//! verify           the hub reads what it stored and agrees on the size
//! commit           one commit naming every file at once
//! ```
//!
//! A file the hub already has skips the middle three, which is not an
//! optimisation we built. Content addressed storage deduplicates whether we
//! ask it to or not, and the reason it matters here is doc 12.8: re uploading
//! after a crash is cheap, so the recovery path can be the simple one.
//!
//! # Batching
//!
//! Doc 12.6 batches into one commit per 32 files or per 5 minutes, whichever
//! comes first. That policy is the caller's, and [`Hub::upload`] takes a slice
//! because of it: it commits exactly what it is given, in one commit, and the
//! decision about when to call it belongs where the clock is.
//!
//! The ordering rule is not the caller's, though, and it is the one thing in
//! doc 12.6 that a consumer can observe. A manifest is only pushed once every
//! file it names is durably present, so nobody who trusts a manifest is ever
//! pointed at a file that is not there. The other order leaves a manifest
//! promising files that a crash lost, and there is no way for a reader to tell
//! that apart from us having lied.
//!
//! # The token
//!
//! It arrives as a `String` that the caller resolved from `env:` or `file:`,
//! per doc 14.7, and it leaves in an `Authorization` header and nowhere else.
//! [`Hub`]'s `Debug` does not print it, no error in this module carries it,
//! and the one place a URL could carry a credential is the presigned upload
//! target, whose query string is a signature. So errors from that request drop
//! the URL rather than formatting it.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use serde::Deserialize;

use crate::{Error, Result};

#[cfg(test)]
#[path = "hub_tests.rs"]
mod tests;

/// The public instance, and the default.
pub const HUB: &str = "https://huggingface.co";

/// The branch every commit goes to.
///
/// A publisher does not branch. Doc 12.8's correction mechanism is an
/// exclusion list in `umi-meta` and never a rewrite, so there is nothing for a
/// second branch to hold.
pub const MAIN: &str = "main";

/// How big a piece of a multipart upload we hold in memory at once.
///
/// The hub tells us the real chunk size in the batch response and this is only
/// the ceiling we refuse past. A hub asking us to buffer a gigabyte to make
/// one part is a hub we should stop talking to rather than one we should obey.
const MAX_CHUNK: u64 = 256 << 20;

/// Doc 12.6's retry budget: six attempts over roughly ten minutes.
///
/// The jitter is derived rather than drawn. Nothing else in this crate reads a
/// random source, for the reasons at the top of [`crate`], and a retry
/// schedule that a test cannot reproduce is a retry schedule nobody can debug
/// from a log. Two publishers backing off the same second is the thing jitter
/// exists to prevent, and two publishers with different seeds do not.
#[derive(Clone, Copy, Debug)]
pub struct Retry {
    /// Total tries, including the first. Six is doc 12.6's number.
    pub attempts: u32,
    /// The first wait, doubled each time after.
    pub backoff: Duration,
    /// Where the jitter comes from. A coordinator's own identifier is the
    /// obvious thing to put here.
    pub seed: u64,
}

impl Default for Retry {
    fn default() -> Self {
        // 5, 10, 20, 40, 80 and 160 seconds is 315 seconds of waiting across
        // six attempts, and the jitter takes it to somewhere between five and
        // ten minutes, which is doc 12.6's "roughly 10 minutes".
        Self {
            attempts: 6,
            backoff: Duration::from_secs(5),
            seed: 0,
        }
    }
}

impl Retry {
    /// How long to wait before attempt `next`, counting the first as 1.
    ///
    /// Full jitter rather than a fraction of the window: the wait is uniform
    /// over the whole interval, which spreads a thundering herd better than
    /// anything that keeps a floor.
    fn wait(&self, next: u32) -> Duration {
        let step = self.backoff.as_millis() as u64;
        let doubled = step.saturating_mul(1 << next.saturating_sub(2).min(20));
        let capped = doubled.min(300_000);
        Duration::from_millis(scatter(self.seed, next) % capped.max(1))
    }
}

/// A small mixer, so that the same seed and attempt always give the same wait.
const fn scatter(seed: u64, attempt: u32) -> u64 {
    let mut x = seed ^ (attempt as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Everything about a hub that is not the token.
#[derive(Clone, Debug)]
pub struct HubConfig {
    /// Where the hub is. Overridable so the tests can point at a socket on
    /// localhost, and for anyone running a private deployment.
    pub base: String,
    /// Per request, not per upload. A 128 MB body at doc 12.2's assumed 10
    /// MB/s is 13 seconds, and this leaves room for a link an order of
    /// magnitude slower before the retry ladder gets involved.
    pub timeout: Duration,
    /// Doc 12.6's ladder.
    pub retry: Retry,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            base: HUB.to_owned(),
            timeout: Duration::from_secs(300),
            retry: Retry::default(),
        }
    }
}

/// A client for one hub and one token.
pub struct Hub {
    client: reqwest::Client,
    base: String,
    token: String,
    retry: Retry,
}

impl fmt::Debug for Hub {
    /// Deliberately hand written. A derived `Debug` on a struct with a token
    /// in it is one `dbg!` away from a token in a log file, and the derive
    /// would come back silently the next time somebody adds a field.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hub")
            .field("base", &self.base)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// What a commit produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    /// The commit object identifier, which is what a manifest chain can be
    /// pinned against later.
    pub oid: String,
    /// Files that went up, in the order they were given.
    pub paths: Vec<String>,
    /// Files the hub already had, which is a subset of `paths`. Worth
    /// reporting because doc 12.8's reconciliation pass wants to know that a
    /// re upload was free rather than assume it.
    pub deduplicated: usize,
}

/// One file in a commit.
#[derive(Clone, Debug)]
pub enum Upload {
    /// A file on disk, big enough to go through lfs. Parquet, in practice.
    ///
    /// The digest is not recomputed here. It comes off [`crate::Converted`],
    /// which produced it from the same bytes that were written, and computing
    /// it again would read 128 MB to check that the disk did not change under
    /// us between two lines of the same function.
    Blob {
        /// Where it goes in the repository, from [`crate::Location`].
        path: String,
        /// Where it is now.
        local: PathBuf,
        /// Its length, which lfs needs before it will hand out an upload url.
        size: u64,
        /// Its sha256, hex, with or without a `sha256:` prefix.
        sha256: String,
    },
    /// Bytes small enough to ride inside the commit itself: a manifest, a
    /// signature, a dataset card.
    Inline {
        /// Where it goes in the repository.
        path: String,
        /// The bytes.
        bytes: Vec<u8>,
    },
}

impl Upload {
    /// Where this goes in the repository.
    #[must_use]
    pub fn path(&self) -> &str {
        match self {
            Self::Blob { path, .. } | Self::Inline { path, .. } => path,
        }
    }
}

/// One entry in a repository listing, for doc 12.8.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Remote {
    /// Path inside the repository.
    pub path: String,
    /// Size in bytes. For an lfs file this is the real size and not the
    /// pointer's, which is the thing that makes a listing usable for doc
    /// 12.7's first condition.
    pub size: u64,
    /// The sha256 of the content for an lfs file, hex and unprefixed, or
    /// `None` for a file committed inline, whose oid is a git blob hash and is
    /// not a digest of anything a consumer can check.
    pub sha256: Option<String>,
}

/// One entry as the hub writes it, shared by the two endpoints that describe
/// files: the tree listing and `paths-info`. They return the same shape, and
/// the shape has a trap in it, so it is decoded in one place.
#[derive(Deserialize)]
struct Entry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    lfs: Option<Lfs>,
}

#[derive(Deserialize)]
struct Lfs {
    #[serde(default)]
    oid: String,
    #[serde(default)]
    size: u64,
}

impl Entry {
    /// The entry as a [`Remote`], or `None` if it is a directory.
    fn file(self) -> Option<Remote> {
        if self.kind != "file" {
            return None;
        }
        // The trap. The `size` at the top level of an lfs entry is the pointer
        // file's, which is 130 bytes and is never what anyone asking this
        // question wants.
        let (size, sha256) = match self.lfs {
            Some(lfs) if !lfs.oid.is_empty() => (lfs.size, Some(lfs.oid)),
            _ => (self.size, None),
        };
        Some(Remote {
            path: self.path,
            size,
            sha256,
        })
    }
}

/// Who the token belongs to and what it can do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Who {
    /// The account name.
    pub name: String,
    /// Organisations the token can act for. `umi doctor` checks `open-index`
    /// is in here, because the alternative is finding out at the end of the
    /// first segment.
    pub orgs: Vec<String>,
    /// Whether the token may write. A read token that uploads nothing for an
    /// hour and then fails is the failure this field exists to move earlier.
    pub write: bool,
}

impl Hub {
    /// A client against the public hub.
    ///
    /// # Errors
    ///
    /// When the HTTP client will not build, which is a TLS or configuration
    /// problem on this machine and not a network one.
    pub fn new(token: impl Into<String>) -> Result<Self> {
        Self::with_config(token, &HubConfig::default())
    }

    /// A client against a named hub.
    ///
    /// # Errors
    ///
    /// As [`Hub::new`].
    pub fn with_config(token: impl Into<String>, config: &HubConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .user_agent(concat!("umi/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|cause| Error::Transport {
                what: "building the client",
                cause: cause.without_url().to_string(),
            })?;
        Ok(Self {
            client,
            base: config.base.trim_end_matches('/').to_owned(),
            token: token.into(),
            retry: config.retry,
        })
    }

    /// Check the token before anything depends on it.
    ///
    /// # Errors
    ///
    /// When the hub is unreachable, or when it does not recognise the token.
    pub async fn whoami(&self) -> Result<Who> {
        #[derive(Deserialize)]
        struct Body {
            name: String,
            #[serde(default)]
            orgs: Vec<Org>,
            #[serde(default)]
            auth: Auth,
        }
        #[derive(Deserialize)]
        struct Org {
            name: String,
        }
        #[derive(Default, Deserialize)]
        struct Auth {
            #[serde(rename = "accessToken", default)]
            access_token: Option<Token>,
        }
        #[derive(Deserialize)]
        struct Token {
            #[serde(default)]
            role: String,
        }

        let url = format!("{}/api/whoami-v2", self.base);
        let body = self
            .send("whoami", || self.client.get(&url))
            .await?
            .into_json::<Body>("whoami")?;
        // `fineGrained` tokens report their role as `fineGrained` and carry
        // the actual permissions elsewhere, so the honest answer for one of
        // those is "maybe", and "maybe" has to mean yes or `umi doctor` would
        // refuse the token the project actually uses.
        let role = body
            .auth
            .access_token
            .map(|token| token.role)
            .unwrap_or_default();
        Ok(Who {
            name: body.name,
            orgs: body.orgs.into_iter().map(|org| org.name).collect(),
            write: role != "read",
        })
    }

    /// Create a dataset repository, or notice it is already there.
    ///
    /// Returns whether this call created it. Idempotent on purpose: a
    /// publisher starting a new ISO week per doc 12.4 should not have to know
    /// whether it or another coordinator got there first.
    ///
    /// # Errors
    ///
    /// When the hub refuses for any reason other than the repository already
    /// existing.
    pub async fn ensure_dataset(&self, repo: &str) -> Result<bool> {
        let (org, name) = split(repo)?;
        let url = format!("{}/api/repos/create", self.base);
        let body = serde_json::json!({
            "type": "dataset",
            "name": name,
            "organization": org,
            "private": false,
        });
        let response = self
            .send_allowing("creating the repository", &[409], || {
                json_body(self.client.post(&url), &body)
            })
            .await?;
        Ok(response.status != 409)
    }

    /// Put files on a repository in one commit.
    ///
    /// # Errors
    ///
    /// When any upload or the commit itself fails after doc 12.6's retries. A
    /// partial failure leaves whatever went up in lfs storage and no commit,
    /// which is the orphan case doc 12.8 adopts or deletes, and is why this
    /// returns an error rather than a partial success: there is nothing useful
    /// a caller could do with "three of your five files are somewhere".
    pub async fn upload(&self, repo: &str, files: &[Upload], summary: &str) -> Result<Commit> {
        if files.is_empty() {
            return Err(Error::Manifest("a commit with no files in it"));
        }
        let plan = self.preupload(repo, files).await?;
        let mut deduplicated = 0;
        for (file, mode) in files.iter().zip(&plan) {
            if let (Upload::Blob { local, size, .. }, Mode::Lfs { oid }) = (file, mode)
                && self.push(repo, local, *size, oid).await?
            {
                deduplicated += 1;
            }
        }
        let oid = self.commit(repo, files, &plan, summary).await?;
        Ok(Commit {
            oid,
            paths: files.iter().map(|file| file.path().to_owned()).collect(),
            deduplicated,
        })
    }

    /// Read bytes back off the hub, for doc 12.7's second condition.
    ///
    /// Doc 12.7 is specific about what independent means and this is the
    /// shape of it: a fresh request that reuses nothing from the upload, and
    /// bytes the caller digests itself. This returns the bytes and computes
    /// nothing, because a function that returned a digest would be a function
    /// a caller could be tempted to trust.
    ///
    /// # Errors
    ///
    /// When the hub will not serve the range, or serves a different one. A
    /// hub that ignores `Range` and sends the whole file is a hub whose answer
    /// cannot be checked against a chunk tree, so that is an error here and
    /// not a silent full download.
    pub async fn read_range(&self, repo: &str, path: &str, at: u64, len: u64) -> Result<Vec<u8>> {
        let url = format!("{}/datasets/{repo}/resolve/{MAIN}/{path}", self.base);
        let last = at + len - 1;
        let response = self
            .send("reading a range back", || {
                self.client
                    .get(&url)
                    .header(reqwest::header::RANGE, format!("bytes={at}-{last}"))
            })
            .await?;
        if response.status != 206 {
            return Err(Error::Hub {
                status: response.status,
                what: "reading a range back",
                body: "the hub served the whole file instead of the range".to_owned(),
            });
        }
        if response.body.len() as u64 != len {
            return Err(Error::Hub {
                status: response.status,
                what: "reading a range back",
                body: format!("asked for {len} bytes and got {}", response.body.len()),
            });
        }
        Ok(response.body)
    }

    /// Read a whole small file, or `None` if it is not there.
    ///
    /// This is for manifests and their signatures and nothing else. Doc 12.5
    /// sizes a day manifest at a few hundred kilobytes and a signature at 64
    /// bytes, so both fit in memory without a thought, and both have to be
    /// readable without knowing their length in advance, which is what stops
    /// [`Hub::read_range`] from being the method for the job.
    ///
    /// A missing file is `Ok(None)` because that is the normal answer on the
    /// first segment of a day. The caller starts a new manifest rather than
    /// treating it as a failure.
    ///
    /// # Errors
    ///
    /// When the hub answers with anything other than success or a 404.
    pub async fn read(&self, repo: &str, path: &str) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/datasets/{repo}/resolve/{MAIN}/{path}", self.base);
        let response = self
            .send_allowing("reading a file", &[404], || self.client.get(&url))
            .await?;
        if response.status == 404 {
            return Ok(None);
        }
        Ok(Some(response.body))
    }

    /// Everything under a directory, for doc 12.8's reconciliation.
    ///
    /// The prefix has to name a directory or the empty string. The hub's tree
    /// endpoint answers a path that names a file with a 404, which arrives
    /// here as an empty listing, so asking this about one file gets the wrong
    /// answer rather than an error. [`Hub::info`] is the method for that
    /// question.
    ///
    /// # Errors
    ///
    /// When the hub will not list. A repository that does not exist is an
    /// empty listing and not an error, because "no files" is the right answer
    /// to "what is in the repository we have not created yet".
    pub async fn list(&self, repo: &str, prefix: &str) -> Result<Vec<Remote>> {
        let url = format!(
            "{}/api/datasets/{repo}/tree/{MAIN}/{prefix}?recursive=1",
            self.base
        );
        let response = self
            .send_allowing("listing the repository", &[404], || self.client.get(&url))
            .await?;
        if response.status == 404 {
            return Ok(Vec::new());
        }
        let entries = response.into_json::<Vec<Entry>>("listing the repository")?;
        Ok(entries.into_iter().filter_map(Entry::file).collect())
    }

    /// One file, or `None` if the hub does not have it.
    ///
    /// Doc 12.7's first condition is about a single object and this is the
    /// question it asks. It goes to the hub's `paths-info` endpoint rather
    /// than the tree endpoint [`Hub::list`] uses, because the tree endpoint
    /// takes a directory and answers a file path with a 404 whether the file
    /// is there or not, and a garbage collector that reads "not there" as "not
    /// published yet" is one that never deletes anything.
    ///
    /// # Errors
    ///
    /// When the hub will not answer. A missing file is `Ok(None)`, as is a
    /// repository that does not exist yet.
    pub async fn info(&self, repo: &str, path: &str) -> Result<Option<Remote>> {
        let url = format!("{}/api/datasets/{repo}/paths-info/{MAIN}", self.base);
        let body = serde_json::json!({ "paths": [path], "expand": false });
        let response = self
            .send_allowing("asking about a file", &[404], || {
                json_body(self.client.post(&url), &body)
            })
            .await?;
        if response.status == 404 {
            return Ok(None);
        }
        let entries = response.into_json::<Vec<Entry>>("asking about a file")?;
        Ok(entries
            .into_iter()
            .filter_map(Entry::file)
            .find(|entry| entry.path == path))
    }

    /// Ask the hub how each file should go up, and which it already has.
    async fn preupload(&self, repo: &str, files: &[Upload]) -> Result<Vec<Mode>> {
        #[derive(Deserialize)]
        struct Body {
            files: Vec<Entry>,
        }
        #[derive(Deserialize)]
        struct Entry {
            path: String,
            #[serde(rename = "uploadMode", default)]
            mode: String,
        }

        let mut asking = Vec::new();
        for file in files {
            if let Upload::Blob {
                path, local, size, ..
            } = file
            {
                // The sample is how the hub decides between inline and lfs
                // without seeing the file, and it is the first kilobyte
                // because that is what its own client sends. A Parquet file
                // starts with `PAR1` and goes to lfs on size alone anyway.
                let sample = read_at(local, 0, (*size).min(1024))?;
                asking.push(serde_json::json!({
                    "path": path,
                    "size": size,
                    "sample": base64::engine::general_purpose::STANDARD.encode(&sample),
                }));
            }
        }
        if asking.is_empty() {
            return Ok(files.iter().map(|_| Mode::Inline).collect());
        }

        let url = format!("{}/api/datasets/{repo}/preupload/{MAIN}", self.base);
        let body = serde_json::json!({ "files": asking });
        let answer = self
            .send("preupload", || json_body(self.client.post(&url), &body))
            .await?
            .into_json::<Body>("preupload")?;

        files
            .iter()
            .map(|file| match file {
                Upload::Inline { .. } => Ok(Mode::Inline),
                Upload::Blob { path, sha256, .. } => {
                    let said = answer
                        .files
                        .iter()
                        .find(|entry| &entry.path == path)
                        .map(|entry| entry.mode.as_str());
                    match said {
                        // A hub that wants a 128 MB Parquet inline in a json
                        // commit body is a hub that has changed its mind about
                        // something, and guessing which is worse than saying so.
                        Some("regular") => Err(Error::Hub {
                            status: 200,
                            what: "preupload",
                            body: format!("the hub wants {path} inline, which will not fit"),
                        }),
                        Some(_) => Ok(Mode::Lfs {
                            oid: bare(sha256).to_owned(),
                        }),
                        None => Err(Error::Hub {
                            status: 200,
                            what: "preupload",
                            body: format!("the hub said nothing about {path}"),
                        }),
                    }
                }
            })
            .collect()
    }

    /// Get one blob's bytes into lfs storage. Returns whether it was already
    /// there.
    async fn push(&self, repo: &str, local: &Path, size: u64, oid: &str) -> Result<bool> {
        let url = format!("{}/datasets/{repo}.git/info/lfs/objects/batch", self.base);
        let body = serde_json::json!({
            "operation": "upload",
            "transfers": ["basic", "multipart"],
            "hash_algo": "sha_256",
            "objects": [{ "oid": oid, "size": size }],
        });
        let batch = self
            .send("the lfs batch", || lfs_body(self.client.post(&url), &body))
            .await?
            .into_json::<BatchBody>("the lfs batch")?;

        let Some(object) = batch.objects.into_iter().next() else {
            return Err(Error::Hub {
                status: 200,
                what: "the lfs batch",
                body: "the hub answered about no objects at all".to_owned(),
            });
        };
        if let Some(error) = object.error {
            return Err(Error::Hub {
                status: error.code,
                what: "the lfs batch",
                body: error.message,
            });
        }
        let Some(actions) = object.actions else {
            // No actions means the hub already has these bytes. Doc 12.6's
            // point about content addressed storage, and the whole reason the
            // recovery path in doc 12.8 can just upload again.
            return Ok(true);
        };
        let Some(upload) = actions.upload else {
            return Ok(true);
        };

        if let Some(chunk) = upload.header.get("chunk_size") {
            self.push_parts(local, size, oid, &upload, chunk).await?;
        } else {
            let bytes = read_at(local, 0, size)?;
            self.put("uploading the file", &upload.href, &upload.header, bytes)
                .await?;
        }

        if let Some(verify) = actions.verify {
            let body = serde_json::json!({ "oid": oid, "size": size });
            self.send("the lfs verify", || {
                lfs_body(self.client.post(&verify.href), &body)
            })
            .await?;
        }
        Ok(false)
    }

    /// The multipart form of [`Hub::push`], one chunk in memory at a time.
    async fn push_parts(
        &self,
        local: &Path,
        size: u64,
        oid: &str,
        upload: &Action,
        chunk: &str,
    ) -> Result<()> {
        let chunk: u64 = chunk.parse().map_err(|_| Error::Hub {
            status: 200,
            what: "the lfs batch",
            body: format!("{chunk:?} is not a chunk size"),
        })?;
        if chunk == 0 || chunk > MAX_CHUNK {
            return Err(Error::Hub {
                status: 200,
                what: "the lfs batch",
                body: format!("a chunk size of {chunk} is not one we will buffer"),
            });
        }

        // The part urls arrive as header entries keyed by part number, which
        // is a string, and string order puts part 10 before part 2. An upload
        // assembled in the wrong order is a file that is the right size and
        // the wrong bytes, so the sort is numeric and the parse failing is an
        // error rather than a skip.
        let mut parts: Vec<(u32, &String)> = Vec::new();
        for (key, value) in &upload.header {
            if let Ok(number) = key.parse::<u32>() {
                parts.push((number, value));
            }
        }
        parts.sort_unstable_by_key(|(number, _)| *number);
        if parts.is_empty() {
            return Err(Error::Hub {
                status: 200,
                what: "the lfs batch",
                body: "a multipart upload with no parts in it".to_owned(),
            });
        }

        let mut done = Vec::with_capacity(parts.len());
        for (number, href) in parts {
            let at = u64::from(number - 1) * chunk;
            let len = chunk.min(size.saturating_sub(at));
            if len == 0 {
                break;
            }
            let bytes = read_at(local, at, len)?;
            let etag = self
                .put("uploading a part", href, &Headers::new(), bytes)
                .await?;
            done.push(serde_json::json!({ "partNumber": number, "etag": etag }));
        }

        let body = serde_json::json!({ "oid": oid, "parts": done });
        self.send("completing the upload", || {
            lfs_body(self.client.post(&upload.href), &body)
        })
        .await?;
        Ok(())
    }

    /// One commit naming every file, in doc 12.6's ndjson.
    async fn commit(
        &self,
        repo: &str,
        files: &[Upload],
        plan: &[Mode],
        summary: &str,
    ) -> Result<String> {
        #[derive(Deserialize)]
        struct Body {
            #[serde(rename = "commitOid", default)]
            oid: String,
        }

        let mut ndjson = String::new();
        let header = serde_json::json!({
            "key": "header",
            "value": { "summary": summary, "description": "" },
        });
        ndjson.push_str(&header.to_string());
        ndjson.push('\n');

        for (file, mode) in files.iter().zip(plan) {
            let line = match (file, mode) {
                (Upload::Inline { path, bytes }, _) => serde_json::json!({
                    "key": "file",
                    "value": {
                        "path": path,
                        "encoding": "base64",
                        "content": base64::engine::general_purpose::STANDARD.encode(bytes),
                    },
                }),
                (Upload::Blob { path, size, .. }, Mode::Lfs { oid }) => serde_json::json!({
                    "key": "lfsFile",
                    "value": { "path": path, "algo": "sha256", "oid": oid, "size": size },
                }),
                (Upload::Blob { path, .. }, Mode::Inline) => {
                    return Err(Error::Hub {
                        status: 200,
                        what: "the commit",
                        body: format!("{path} has no upload mode"),
                    });
                }
            };
            ndjson.push_str(&line.to_string());
            ndjson.push('\n');
        }

        let url = format!("{}/api/datasets/{repo}/commit/{MAIN}", self.base);
        let answer = self
            .send("the commit", || {
                self.client
                    .post(&url)
                    .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
                    .body(ndjson.clone())
            })
            .await?
            .into_json::<Body>("the commit")?;
        Ok(answer.oid)
    }

    /// A PUT of bytes to a presigned url, returning the `ETag`.
    ///
    /// This is the one request whose url is a credential, so nothing here
    /// formats it, including on the way out through an error.
    async fn put(
        &self,
        what: &'static str,
        href: &str,
        headers: &Headers,
        bytes: Vec<u8>,
    ) -> Result<String> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut request = self.client.put(href).body(bytes.clone());
            for (key, value) in headers {
                if key != "chunk_size" && key.parse::<u32>().is_err() {
                    request = request.header(key, value);
                }
            }
            let sent = request.send().await;
            match sent {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let etag = response
                        .headers()
                        .get(reqwest::header::ETAG)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    if (200..300).contains(&status) {
                        return Ok(etag);
                    }
                    if !retryable(status) || attempt >= self.retry.attempts {
                        return Err(Error::Hub {
                            status,
                            what,
                            body: String::new(),
                        });
                    }
                }
                Err(cause) => {
                    if attempt >= self.retry.attempts {
                        return Err(Error::Transport {
                            what,
                            cause: cause.without_url().to_string(),
                        });
                    }
                }
            }
            tokio::time::sleep(self.retry.wait(attempt + 1)).await;
        }
    }

    /// A request, with doc 12.6's ladder around it.
    async fn send<F>(&self, what: &'static str, build: F) -> Result<Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        self.send_allowing(what, &[], build).await
    }

    /// As [`Hub::send`], but some non success statuses are answers rather than
    /// failures. A 409 from repository creation means it is already there and
    /// a 404 from a listing means there is nothing in it, and both of those
    /// are things the caller wanted to know.
    async fn send_allowing<F>(
        &self,
        what: &'static str,
        allowed: &[u16],
        build: F,
    ) -> Result<Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let sent = build()
                .header(reqwest::header::AUTHORIZATION, self.bearer())
                .send()
                .await;
            let failure = match sent {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let body = response.bytes().await.unwrap_or_default().to_vec();
                    if (200..300).contains(&status) || allowed.contains(&status) {
                        return Ok(Response { status, body });
                    }
                    if !retryable(status) {
                        return Err(Error::Hub {
                            status,
                            what,
                            body: message(&body),
                        });
                    }
                    Error::Hub {
                        status,
                        what,
                        body: message(&body),
                    }
                }
                Err(cause) => Error::Transport {
                    what,
                    cause: cause.without_url().to_string(),
                },
            };
            if attempt >= self.retry.attempts {
                return Err(failure);
            }
            tokio::time::sleep(self.retry.wait(attempt + 1)).await;
        }
    }

    fn bearer(&self) -> String {
        format!("Bearer {}", self.token)
    }
}

/// How one file is going up.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Mode {
    /// Inside the commit body.
    Inline,
    /// Through lfs, under this oid.
    Lfs { oid: String },
}

type Headers = std::collections::BTreeMap<String, String>;

/// The lfs batch response, which is git-lfs's wire format and not the hub's.
#[derive(Deserialize)]
struct BatchBody {
    #[serde(default)]
    objects: Vec<Object>,
}

#[derive(Deserialize)]
struct Object {
    #[serde(default)]
    actions: Option<Actions>,
    #[serde(default)]
    error: Option<ObjectError>,
}

#[derive(Deserialize)]
struct Actions {
    #[serde(default)]
    upload: Option<Action>,
    #[serde(default)]
    verify: Option<Action>,
}

#[derive(Deserialize)]
struct Action {
    href: String,
    #[serde(default)]
    header: Headers,
}

#[derive(Deserialize)]
struct ObjectError {
    #[serde(default)]
    code: u16,
    #[serde(default)]
    message: String,
}

/// A status and a body, with the transport already finished with.
struct Response {
    status: u16,
    body: Vec<u8>,
}

impl Response {
    fn into_json<T: serde::de::DeserializeOwned>(self, what: &'static str) -> Result<T> {
        serde_json::from_slice(&self.body).map_err(|cause| Error::Hub {
            status: self.status,
            what,
            body: format!("the answer did not parse: {cause}"),
        })
    }
}

const LFS_JSON: &str = "application/vnd.git-lfs+json";

/// A json body without reqwest's `json` feature.
///
/// The feature would pull `serde_json` into the fetcher's dependency tree as
/// well, and the fetcher is the crate whose tree gate 2.2 asserts things
/// about. Two lines here is cheaper than an argument later about why the
/// crawler links a json serialiser it never calls.
fn json_body(
    builder: reqwest::RequestBuilder,
    value: &serde_json::Value,
) -> reqwest::RequestBuilder {
    builder
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(value.to_string())
}

/// As [`json_body`], in git-lfs's content type, which the batch endpoint
/// checks and refuses without.
fn lfs_body(
    builder: reqwest::RequestBuilder,
    value: &serde_json::Value,
) -> reqwest::RequestBuilder {
    builder
        .header(reqwest::header::ACCEPT, LFS_JSON)
        .header(reqwest::header::CONTENT_TYPE, LFS_JSON)
        .body(value.to_string())
}

/// Whether a status is worth trying again.
///
/// 429 and 5xx only. A 401 retried six times over ten minutes is ten minutes
/// of a publisher not saying the one thing the operator needs to hear, and a
/// 403 does not become a 200 by asking again.
const fn retryable(status: u16) -> bool {
    status == 429 || status >= 500
}

/// The useful part of an error body, if there is one.
///
/// The hub answers errors as `{"error": "..."}` and everything else as a page
/// of html when something in front of it answered instead. Either way this is
/// capped, because an error that pastes a kilobyte of markup into a log is an
/// error nobody reads.
fn message(body: &[u8]) -> String {
    #[derive(Deserialize)]
    struct Body {
        error: String,
    }
    if let Ok(parsed) = serde_json::from_slice::<Body>(body) {
        return parsed.error;
    }
    let text = String::from_utf8_lossy(body);
    text.chars().take(200).collect()
}

/// `org/name`, which every repository doc 12.4 names is.
fn split(repo: &str) -> Result<(&str, &str)> {
    repo.split_once('/').ok_or(Error::Manifest(
        "a repository is org/name and this one has no slash",
    ))
}

/// A digest with or without its algorithm prefix, without it.
fn bare(digest: &str) -> &str {
    digest.strip_prefix("sha256:").unwrap_or(digest)
}

/// `len` bytes from `at`, without reading the rest of the file.
///
/// Doc 12.1's bias, applied to memory: an upload holds one part at a time
/// rather than streaming, because a stream that has to be rebuilt for every
/// retry is more moving parts than a 20 MB buffer is worth, and a publisher
/// that always finishes beats one that is occasionally cheaper.
fn read_at(path: &Path, at: u64, len: u64) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(at))?;
    let mut bytes = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}
