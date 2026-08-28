//! The published block list, from `docs/spec/07-politeness-and-identity.md`
//! section 7.7.
//!
//! A block is our statement that we have stopped crawling a domain. Doc 07.7
//! says it is published, and the reason it gives is worth repeating: a
//! downstream consumer holding an older snapshot has to be able to honour a
//! block we applied after they took their copy. An open corpus that cannot be
//! retroactively corrected is a liability, and the list is what makes the
//! correction something a stranger can act on rather than something they have
//! to ask us about.
//!
//! The other half of publishing it is that it makes us auditable. A block
//! nobody can see is one nobody can check, and the reason travelling with it is
//! what keeps the record readable years later, when whoever applied it has
//! forgotten the ticket number.
//!
//! # One file per domain
//!
//! Rather than one list everybody rewrites. The list only grows and entries
//! only change when a block is lifted, so a per domain file means a commit
//! touches exactly what changed, two operators working at once cannot lose each
//! other's entry, and a consumer who cares about one domain reads one small
//! file. A single JSON array would be a merge conflict waiting for the second
//! complaint.
//!
//! # Lifted blocks stay
//!
//! Doc 07.7 says blocks are never silently reversed and that a domain asking to
//! be unblocked gets a dated record of both events. So a lift rewrites the
//! file with two more fields in it and never deletes it. A consumer decides
//! what to do from [`BlockEntry::in_force`], and a person auditing us reads the
//! dates.

use umi_state::BlockRow;

use crate::hub::{Hub, Upload};
use crate::{Error, Result};

/// Where in `umi-meta` the block list lives.
pub const BLOCK_DIR: &str = "blocks";

/// The path in `umi-meta` for one domain.
///
/// # Errors
///
/// [`Error::Manifest`] if the domain is not something that can name a file. A
/// domain reaches this from [`BlockRow::new`], which widens whatever it was
/// given to a registrable domain, so anything with a slash or a segment of
/// dots in it did not come from there and is not going to be written into a
/// repository path on trust.
pub fn block_path(domain: &str) -> Result<String> {
    let usable = !domain.is_empty()
        && domain.len() <= 253
        && domain
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains("..");
    if !usable {
        return Err(Error::Manifest("a block names a domain that cannot be one"));
    }
    Ok(format!("{BLOCK_DIR}/{domain}.json"))
}

/// One block, as it is written.
///
/// Small and readable on purpose, like the key directory next door. Somebody
/// who wants to know why we stopped crawling their site should be able to `cat`
/// this and have the whole answer.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct BlockEntry {
    /// The registrable domain that is blocked. Subdomains are covered: the
    /// block is about the site and not about one host under it.
    pub domain: String,
    /// Why we stopped, in the words of whoever applied it.
    pub reason: String,
    /// When the block was applied, in milliseconds.
    pub blocked_ms: u64,
    /// When it was lifted, absent while it is in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifted_ms: Option<u64>,
    /// Why it was lifted, absent while it is in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifted_reason: Option<String>,
}

impl BlockEntry {
    /// The entry for a stored block.
    #[must_use]
    pub fn new(row: &BlockRow) -> Self {
        Self {
            domain: row.domain.clone(),
            reason: row.reason.clone(),
            blocked_ms: row.blocked_ms,
            lifted_ms: row.lifted_ms,
            // Empty and absent are the same thing here, and writing one of them
            // rather than both is one less shape for a reader to handle.
            lifted_reason: (!row.lifted_reason.is_empty()).then(|| row.lifted_reason.clone()),
        }
    }

    /// Whether this block still stops anything.
    #[must_use]
    pub const fn in_force(&self) -> bool {
        self.lifted_ms.is_none()
    }

    /// The stored form, for a coordinator applying the published list to its
    /// own frontier.
    ///
    /// This is the other half of doc 07.7 being fleet wide. An operator applies
    /// a block on one machine, it is published, and every other coordinator
    /// reads it back through here rather than being told separately.
    #[must_use]
    pub fn to_row(&self) -> BlockRow {
        BlockRow {
            lifted_ms: self.lifted_ms,
            lifted_reason: self.lifted_reason.clone().unwrap_or_default(),
            // Through `new` rather than field by field, so the domain is put
            // through the same widening it was when it was first typed. A list
            // entry naming a host would otherwise block less here than it did
            // on the machine that wrote it.
            ..BlockRow::new(&self.domain, &self.reason, self.blocked_ms)
        }
    }

    /// The bytes as they go into the repository, with the trailing newline a
    /// text file gets so that `cat` behaves.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] if serialisation fails, which it does not for a
    /// struct of owned scalars.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(self)
            .map_err(|_| Error::Manifest("the block entry would not serialise"))?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Read one back.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] when the document does not parse or has no reason on
    /// it. A block with no reason is not publishable: the reason is the part
    /// doc 07.7 says has to explain itself years later.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let entry: Self = serde_json::from_slice(bytes)
            .map_err(|_| Error::Manifest("a block entry did not parse"))?;
        if entry.reason.trim().is_empty() {
            return Err(Error::Manifest("a block entry has no reason on it"));
        }
        block_path(&entry.domain)?;
        Ok(entry)
    }
}

/// Publish a block list, writing only the entries that are not already there as
/// they are.
///
/// Returns how many files the commit carried. Zero means the published list
/// already said all of this, which is the answer on every run but the one after
/// a complaint, and it is why the read comes before the write: `umi-meta` is
/// the one repository doc 12.4 says we rewrite, and a commit per crawl that
/// changes nothing would bury the commits that change something.
///
/// One commit for the whole batch, so a list that grew by three entries is
/// three files landing together rather than three commits and two windows where
/// a consumer reading the list gets half of it.
///
/// # Errors
///
/// Whatever the hub says, and [`Error::Manifest`] for an entry that cannot name
/// a file. A block that will not publish is worth stopping for: doc 07.7 makes
/// publishing part of what applying a block means.
pub async fn publish_blocks(hub: &Hub, meta_repo: &str, rows: &[BlockRow]) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    hub.ensure_dataset(meta_repo).await?;
    let mut uploads = Vec::new();
    for row in rows {
        let entry = BlockEntry::new(row);
        let path = block_path(&entry.domain)?;
        let bytes = entry.to_json()?;
        // Byte equality rather than parsing what is there and comparing. The
        // writer is deterministic, so anything that differs is something that
        // changed, and a file an older umi wrote in a slightly different shape
        // is one worth rewriting anyway.
        if hub.read(meta_repo, &path).await? == Some(bytes.clone()) {
            continue;
        }
        uploads.push(Upload::Inline { path, bytes });
    }

    if uploads.is_empty() {
        return Ok(0);
    }
    let message = match uploads.len() {
        1 => format!("Block {}", rows[0].domain),
        n => format!("Update the block list, {n} domains"),
    };
    hub.upload(meta_repo, &uploads, &message).await?;
    Ok(uploads.len())
}

/// Every block in the published list, in the order the hub lists them.
///
/// Lifted entries are included, because the list is the record and not just the
/// enforcement. A consumer honouring doc 07.7 filters on
/// [`in_force`](BlockEntry::in_force).
///
/// # Errors
///
/// When the hub will not answer, or when an entry is there and unreadable. A
/// malformed entry stops the read rather than being skipped: a list that
/// quietly drops the domain somebody asked us to stop crawling is worse than no
/// list, because it reads as an answer.
pub async fn published_blocks(hub: &Hub, meta_repo: &str) -> Result<Vec<BlockEntry>> {
    let listing = hub.list(meta_repo, BLOCK_DIR).await?;
    let mut blocks = Vec::with_capacity(listing.len());
    for entry in listing {
        if !entry.path.ends_with(".json") {
            continue;
        }
        let Some(bytes) = hub.read(meta_repo, &entry.path).await? else {
            continue;
        };
        blocks.push(BlockEntry::parse(&bytes)?);
    }
    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use umi_state::BlockRow;

    use super::{BlockEntry, block_path};

    const T0: u64 = 1_787_000_000_000;

    fn row() -> BlockRow {
        BlockRow::new(
            "news.example.com",
            "the site owner asked us to stop on 2026-08-14, ticket 41",
            T0,
        )
    }

    #[test]
    fn an_entry_reads_back_as_what_was_blocked() {
        let row = row();
        let bytes = BlockEntry::new(&row).to_json().expect("json");
        let entry = BlockEntry::parse(&bytes).expect("parse");
        assert_eq!(
            entry.domain, "example.com",
            "the published block is about the registrable domain"
        );
        assert_eq!(entry.reason, row.reason);
        assert_eq!(entry.blocked_ms, T0);
        assert!(entry.in_force(), "a fresh block is not lifted");
        assert!(
            bytes.ends_with(b"\n"),
            "a text file in a repository ends with a newline"
        );
    }

    #[test]
    fn a_lift_carries_both_dates_and_both_reasons() {
        let row = row().lift("they changed their minds, ticket 41", T0 + 86_400_000);
        let bytes = BlockEntry::new(&row).to_json().expect("json");
        let entry = BlockEntry::parse(&bytes).expect("parse");
        assert!(!entry.in_force(), "a lifted block still reads as in force");
        assert_eq!(entry.blocked_ms, T0, "the lift lost the original date");
        assert_eq!(entry.lifted_ms, Some(T0 + 86_400_000));
        assert!(
            entry.lifted_reason.is_some(),
            "the lift has no reason on it"
        );
        assert_eq!(entry.reason, row.reason, "the lift lost the first reason");
        assert_eq!(
            entry.to_row(),
            row,
            "the lift did not survive the round trip"
        );
    }

    #[test]
    fn a_published_entry_comes_back_as_the_row_that_wrote_it() {
        let row = row();
        let back = BlockEntry::new(&row).to_row();
        assert_eq!(back, row, "the block did not come back as it went out");
        assert!(back.in_force(), "a published block came back lifted");
    }

    #[test]
    fn an_entry_with_nothing_to_say_is_refused() {
        let bytes = br#"{"domain":"example.com","reason":"  ","blocked_ms":0}"#;
        let error = BlockEntry::parse(bytes).expect_err("a block with no reason");
        assert!(format!("{error}").contains("no reason"), "{error}");
    }

    #[test]
    fn a_domain_that_could_escape_the_directory_is_refused() {
        for domain in ["../keys/publishing/one", "example.com/..", "", "."] {
            block_path(domain).expect_err(domain);
        }
        assert_eq!(
            block_path("example.com").expect("a real domain"),
            "blocks/example.com.json"
        );
    }
}
