//! The published supervised list, from `docs/spec/05-fetch-tiers.md` section
//! 5.7.
//!
//! T4 is the one rung nothing reaches by learning. Every other tier is a
//! decision the crawler makes about a page in front of it, and T4 is a decision
//! a person made about a domain, wrote down, and signed their name to. This
//! module is the part where the name gets published.
//!
//! Publishing it is not politeness. A crawler that will drive a real browser at
//! a site, on a list only its operator can see, is asking to be trusted about
//! something nobody can check. The list is what makes it checkable: the domain,
//! who put it there, why, and when. If somebody finds their own domain on it
//! and does not recognise the reason, that is exactly the complaint the list
//! exists to make possible.
//!
//! The shape follows the block list next door, for the same reasons: one file
//! per domain so a commit touches what changed, entries that only ever grow,
//! and a removal that rewrites the file with two more fields rather than
//! deleting it. A removed entry is still a record of a domain we once ran a
//! browser at, and deleting it would leave nobody able to say that happened.

use umi_state::SupervisionRow;

use crate::hub::{Hub, Upload};
use crate::{Error, Result, domain_path};

/// Where in `umi-meta` the supervised list lives.
pub const SUPERVISED_DIR: &str = "supervised";

/// The path in `umi-meta` for one domain.
///
/// # Errors
///
/// [`Error::Manifest`] if the domain is not something that can name a file.
pub fn supervised_path(domain: &str) -> Result<String> {
    domain_path(SUPERVISED_DIR, domain)
}

/// One supervised domain, as it is written.
///
/// Four facts and two dates, which is the whole of what anybody needs to decide
/// whether to be annoyed about it.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct SupervisedEntry {
    /// The registrable domain. Subdomains are covered, because the agreement is
    /// with a site and not with one host under it.
    pub domain: String,
    /// Who put it on the list. A person, not a machine, because the point of
    /// the field is that somebody can be asked about it.
    pub operator: String,
    /// Why, in the words of whoever added it.
    pub reason: String,
    /// When it went on the list, in milliseconds.
    pub added_ms: u64,
    /// When it came off, absent while it is in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_ms: Option<u64>,
    /// Why it came off, absent while it is in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_reason: Option<String>,
}

impl SupervisedEntry {
    /// The entry for a stored row.
    #[must_use]
    pub fn new(row: &SupervisionRow) -> Self {
        Self {
            domain: row.domain.clone(),
            operator: row.operator.clone(),
            reason: row.reason.clone(),
            added_ms: row.added_ms,
            removed_ms: row.removed_ms,
            removed_reason: (!row.removed_reason.is_empty()).then(|| row.removed_reason.clone()),
        }
    }

    /// Whether this entry still lets anything reach T4.
    #[must_use]
    pub const fn in_force(&self) -> bool {
        self.removed_ms.is_none()
    }

    /// The stored form, for a coordinator applying the published list to its
    /// own state.
    #[must_use]
    pub fn to_row(&self) -> SupervisionRow {
        SupervisionRow {
            removed_ms: self.removed_ms,
            removed_reason: self.removed_reason.clone().unwrap_or_default(),
            // Through `new`, so the domain is widened the same way it was on
            // the machine that wrote it. An entry naming a host would otherwise
            // permit less here than it did there, which is the safe direction,
            // but a list that means two different things in two places is not
            // a list anybody can audit.
            ..SupervisionRow::new(&self.domain, &self.operator, &self.reason, self.added_ms)
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
            .map_err(|_| Error::Manifest("the supervised entry would not serialise"))?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Read one back.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] when the document does not parse, or when it has no
    /// operator or no reason on it. Both are refused rather than defaulted:
    /// an anonymous entry with no explanation is the exact thing publishing the
    /// list is meant to prevent, and accepting one would let it through while
    /// still looking like disclosure.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let entry: Self = serde_json::from_slice(bytes)
            .map_err(|_| Error::Manifest("a supervised entry did not parse"))?;
        if entry.operator.trim().is_empty() {
            return Err(Error::Manifest("a supervised entry names nobody"));
        }
        if entry.reason.trim().is_empty() {
            return Err(Error::Manifest("a supervised entry has no reason on it"));
        }
        supervised_path(&entry.domain)?;
        Ok(entry)
    }
}

/// Publish a supervised list, writing only the entries that are not already
/// there as they are.
///
/// Returns how many files the commit carried. Zero means the published list
/// already said all of this, which is the answer on nearly every run, and it is
/// why the read comes before the write.
///
/// # Errors
///
/// Whatever the hub says, and [`Error::Manifest`] for an entry that cannot name
/// a file. An entry that will not publish is worth stopping for, because doc
/// 05.7 makes publishing part of what putting a domain on the list means.
pub async fn publish_supervised(
    hub: &Hub,
    meta_repo: &str,
    rows: &[SupervisionRow],
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    hub.ensure_dataset(meta_repo).await?;
    let mut uploads = Vec::new();
    for row in rows {
        let entry = SupervisedEntry::new(row);
        let path = supervised_path(&entry.domain)?;
        let bytes = entry.to_json()?;
        if hub.read(meta_repo, &path).await? == Some(bytes.clone()) {
            continue;
        }
        uploads.push(Upload::Inline { path, bytes });
    }

    if uploads.is_empty() {
        return Ok(0);
    }
    let message = match uploads.len() {
        1 => format!("Supervise {}", rows[0].domain),
        n => format!("Update the supervised list, {n} domains"),
    };
    hub.upload(meta_repo, &uploads, &message).await?;
    Ok(uploads.len())
}

/// Every entry in the published supervised list, in the order the hub lists
/// them.
///
/// Removed entries are included, because the list is the record and not only
/// the permission. A consumer that wants the live set filters on
/// [`in_force`](SupervisedEntry::in_force).
///
/// # Errors
///
/// When the hub will not answer, or when an entry is there and unreadable. A
/// malformed entry stops the read rather than being skipped, for the same
/// reason as the block list: a disclosure that quietly drops a domain reads as
/// an answer while being the opposite of one.
pub async fn published_supervised(hub: &Hub, meta_repo: &str) -> Result<Vec<SupervisedEntry>> {
    let listing = hub.list(meta_repo, SUPERVISED_DIR).await?;
    let mut rows = Vec::with_capacity(listing.len());
    for entry in listing {
        if !entry.path.ends_with(".json") {
            continue;
        }
        let Some(bytes) = hub.read(meta_repo, &entry.path).await? else {
            continue;
        };
        rows.push(SupervisedEntry::parse(&bytes)?);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use umi_state::SupervisionRow;

    use super::{SupervisedEntry, supervised_path};

    const T0: u64 = 1_787_000_000_000;

    fn row() -> SupervisionRow {
        SupervisionRow::new(
            "catalogue.example.com",
            "tam",
            "the archive asked us to mirror their catalogue, agreed 2026-08-20",
            T0,
        )
    }

    #[test]
    fn an_entry_reads_back_as_what_was_allowed() {
        let row = row();
        let bytes = SupervisedEntry::new(&row).to_json().expect("json");
        let entry = SupervisedEntry::parse(&bytes).expect("parse");
        assert_eq!(
            entry.domain, "example.com",
            "the published entry is about the registrable domain"
        );
        assert_eq!(entry.operator, "tam");
        assert_eq!(entry.reason, row.reason);
        assert_eq!(entry.added_ms, T0);
        assert!(entry.in_force(), "a fresh entry is not removed");
        assert!(
            bytes.ends_with(b"\n"),
            "a text file in a repository ends with a newline"
        );
    }

    #[test]
    fn a_removal_carries_both_dates_and_both_reasons() {
        let row = row().remove("the mirror is finished", T0 + 86_400_000);
        let bytes = SupervisedEntry::new(&row).to_json().expect("json");
        let entry = SupervisedEntry::parse(&bytes).expect("parse");
        assert!(!entry.in_force(), "a removed entry still reads as in force");
        assert_eq!(entry.added_ms, T0, "the removal lost the original date");
        assert_eq!(entry.removed_ms, Some(T0 + 86_400_000));
        assert!(
            entry.removed_reason.is_some(),
            "the removal has no reason on it"
        );
        assert_eq!(
            entry.to_row(),
            row,
            "the removal did not survive the round trip"
        );
    }

    #[test]
    fn an_entry_that_names_nobody_is_refused() {
        let bytes = br#"{"domain":"example.com","operator":" ","reason":"why","added_ms":0}"#;
        let error = SupervisedEntry::parse(bytes).expect_err("an entry naming nobody");
        assert!(format!("{error}").contains("names nobody"), "{error}");
    }

    #[test]
    fn an_entry_with_nothing_to_say_is_refused() {
        let bytes = br#"{"domain":"example.com","operator":"tam","reason":"  ","added_ms":0}"#;
        let error = SupervisedEntry::parse(bytes).expect_err("an entry with no reason");
        assert!(format!("{error}").contains("no reason"), "{error}");
    }

    #[test]
    fn a_domain_that_could_escape_the_directory_is_refused() {
        for domain in ["../keys/publishing/one", "example.com/..", "", "."] {
            supervised_path(domain).expect_err(domain);
        }
        assert_eq!(
            supervised_path("example.com").expect("a real domain"),
            "supervised/example.com.json"
        );
    }
}
