//! `umi retract`, taking published files back out on the record.
//!
//! Doc 12.2 says a published file is never rewritten and never deleted, and the
//! reason is good: both break every checksum anyone recorded. This command does
//! not soften that rule. It exists because an operator who has decided a
//! deletion has to happen anyway will otherwise do it with a shell loop, and the
//! shell loop is the worst version of it. Files disappear, the day manifest goes
//! on naming them, the chain goes on pointing at a digest nothing computes to,
//! and a reader checking the repository finds damage they cannot tell from an
//! attack.
//!
//! So the choice this command makes is that if a retraction happens it should be
//! visible. The deletions and the rewritten manifests go in one commit, the
//! chain is relinked to the end of the repository, and a record naming every
//! removed file with the digest it had is appended to the meta repository first,
//! so the explanation is published before the thing it explains.
//!
//! It is deliberately awkward to run. There is no glob, no threshold and no
//! prefix: the caller names each file, or points at a list of them, and gives a
//! reason in words. Doc 07.7 requires a reason on a block for the same purpose,
//! which is that the operator a year from now is a stranger.

use std::path::Path;

use umi_publish::retract::{Retraction, changed, commit, days, plan, record};
use umi_publish::{Hub, Role, SigningKey};

use crate::Error;

/// What `umi retract` was asked to do.
pub struct Options<'a> {
    /// The repository, with or without the organisation on it.
    pub repo: &'a str,
    /// Repository relative paths to remove.
    pub files: Vec<String>,
    /// Why, in words, published with the record.
    pub reason: &'a str,
    /// Work out the plan and print it without committing anything.
    pub dry_run: bool,
    /// The meta repository the record goes to.
    pub meta_repo: &'a str,
    /// The organisation, for a bare repository name.
    pub org: &'a str,
    /// Now, in milliseconds. An argument for the same reason every other
    /// timestamp in this workspace is one.
    pub now_ms: u64,
}

/// What a run did, so the caller can print it and a test can assert on it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// The repository, qualified.
    pub repo: String,
    /// Paths that went, in the order they were given.
    pub removed: Vec<String>,
    /// Rows in them.
    pub rows: u64,
    /// Bytes in them.
    pub bytes: u64,
    /// Days whose manifest was rewritten, which is longer than the days the
    /// files were in because relinking touches every later day.
    pub rewritten: Vec<String>,
    /// Where the record went in the meta repository.
    pub record: String,
}

/// Read the paths a run was given, from the flag and from a list.
///
/// A list because the case this was built for names forty one files, and a
/// command line with forty one paths on it is one nobody can check before
/// pressing return. Blank lines and `#` comments are skipped so the list can be
/// the output of whatever query decided which files these are, with a note at
/// the top saying what the query was.
///
/// # Errors
///
/// [`Error::Io`] when the list will not read, and [`Error::Missing`] when
/// nothing is named at all.
pub fn paths(files: &[String], from: Option<&Path>) -> Result<Vec<String>, Error> {
    let mut out: Vec<String> = files.to_vec();
    if let Some(path) = from {
        let text = std::fs::read_to_string(path).map_err(Error::Io)?;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            out.push(line.to_owned());
        }
    }
    if out.is_empty() {
        return Err(Error::Missing(
            "umi retract needs at least one --file, or a --from list of them".to_owned(),
        ));
    }
    // The same path twice would delete once and count twice, so the report
    // would overstate what happened. Sorting first so the duplicate is next to
    // itself, then back to the order given, is not worth it: the order files
    // are named in carries no meaning.
    out.sort();
    out.dedup();
    Ok(out)
}

/// Retract the named files.
///
/// # Errors
///
/// [`Error::Missing`] without a token or a key, since this writes and signs,
/// and without a reason. Otherwise whatever the hub and the planner say. A path
/// that is in no manifest stops the run before anything is committed.
pub fn run(options: &Options<'_>, token: String, key: &str) -> Result<Report, Error> {
    if options.reason.trim().is_empty() {
        return Err(Error::Missing(
            "umi retract needs --reason: the record is published and a stranger has to be able \
             to read it"
                .to_owned(),
        ));
    }
    let key = SigningKey::from_hex(Role::Publishing, key)?;
    let hub = Hub::new(token)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    runtime.block_on(apply(&hub, &key, options))
}

/// The whole run, once there is a hub and a key to do it with.
async fn apply(hub: &Hub, key: &SigningKey, options: &Options<'_>) -> Result<Report, Error> {
    let repo = qualified(options.repo, options.org);
    let before = days(hub, &repo).await?;
    let plan = plan(&before, &options.files)?;
    let rewritten = changed(&before, &plan.manifests);

    let retraction = Retraction {
        repo: repo.clone(),
        at_ms: options.now_ms,
        reason: options.reason.to_owned(),
        removed: plan.removed.clone(),
        rewritten: rewritten.clone(),
    };
    let report = Report {
        repo: repo.clone(),
        removed: plan.removed.iter().map(|f| f.path.clone()).collect(),
        rows: plan.removed.iter().map(|f| f.rows).sum(),
        bytes: plan.removed.iter().map(|f| f.bytes).sum(),
        rewritten,
        record: retraction.path(),
    };
    if options.dry_run {
        return Ok(report);
    }

    // The record first, for the reason in `record`: a published explanation of
    // a retraction that did not happen is a mistake somebody can check, and
    // files that went with nothing saying why is what an attack looks like.
    record(hub, options.meta_repo, &retraction).await?;

    // Only the days that moved. A repository with a year of history should not
    // have every signature in it rewritten because the last day lost a file.
    let touched: Vec<_> = plan
        .manifests
        .iter()
        .filter(|manifest| report.rewritten.contains(&manifest.day))
        .cloned()
        .collect();
    commit(hub, key, &repo, &options.files, &touched, options.reason).await?;
    Ok(report)
}

/// The repository with the organisation on it, whether or not it arrived that
/// way.
fn qualified(name: &str, org: &str) -> String {
    if name.contains('/') {
        name.to_owned()
    } else {
        format!("{org}/{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_name_gets_the_organisation_and_a_qualified_one_is_left_alone() {
        assert_eq!(
            qualified("umi-robots", "open-index"),
            "open-index/umi-robots"
        );
        assert_eq!(
            qualified("someone/umi-robots", "open-index"),
            "someone/umi-robots"
        );
    }

    #[test]
    fn a_list_of_files_reads_the_way_a_person_writes_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("doomed.txt");
        std::fs::write(
            &path,
            "# picked by the superseded row query, 2026-09-02\n\
             data/20260901/A.parquet\n\
             \n\
             data/20260902/B.parquet\n",
        )
        .expect("write");

        let got = paths(&["data/20260901/A.parquet".to_owned()], Some(&path)).expect("paths");
        assert_eq!(
            got,
            ["data/20260901/A.parquet", "data/20260902/B.parquet"],
            "the file named twice should be deleted once and counted once",
        );
    }

    #[test]
    fn a_run_that_names_nothing_says_so_rather_than_committing_an_empty_change() {
        // Without this a mistyped flag reaches the planner, changes no
        // manifest, and commits nothing while looking like it worked.
        let err = paths(&[], None).expect_err("no files");
        assert!(err.to_string().contains("at least one"), "said {err}");
    }

    #[test]
    fn a_retraction_without_a_reason_is_refused() {
        let options = Options {
            repo: "umi-robots",
            files: vec!["data/20260901/A.parquet".to_owned()],
            reason: "   ",
            dry_run: true,
            meta_repo: "open-index/umi-meta",
            org: "open-index",
            now_ms: 1_756_800_000_000,
        };
        let err = run(&options, "token".to_owned(), &"11".repeat(32)).expect_err("no reason");
        assert!(err.to_string().contains("--reason"), "said {err}");
    }
}
