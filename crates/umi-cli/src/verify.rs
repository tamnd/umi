//! `umi verify`, which is doc 16's gate 1.5 seen from the command line.
//!
//! The whole point of the command is that it needs nothing except a network
//! and a repository name. No crawl directory, no state, no token unless the
//! repository is private, and no configuration beyond which organisation to
//! read the keys from. Somebody who has never met this project runs it against
//! something we published and finds out whether the manifests, the signatures,
//! the chain and the files agree.
//!
//! The checking itself is [`umi_publish::verify`]. This file is the printing
//! and the exit code.

use umi_publish::Hub;
use umi_publish::verify::{Options, verify};

use crate::Error;

/// Check a published repository and print what it found.
///
/// The token is optional and usually absent. Everything this project publishes
/// is public, and a verification that required a credential would not be the
/// verification gate 1.5 asks for.
///
/// # Errors
///
/// Doc 14.9's exit 6 for anything that did not check out, which is what
/// [`umi_publish::Error`] maps to for a manifest or a signature. A hub that
/// will not answer is exit 5 and worth retrying.
pub fn run(repo_name: &str, token: Option<String>, full: bool, org: &str) -> Result<(), Error> {
    let hub = Hub::new(token.unwrap_or_default())?;
    let options = Options {
        full,
        meta_repo: format!("{org}/umi-meta"),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    let report = runtime.block_on(verify(&hub, &qualified(repo_name, org), &options))?;

    for day in &report.days {
        println!(
            "{}  {} files  {} rows  {} bytes  signed by {}{}",
            day.day,
            day.files,
            day.rows,
            day.bytes,
            day.signed_by,
            if day.downloaded > 0 {
                format!("  {} downloaded", day.downloaded)
            } else {
                String::new()
            }
        );
    }
    let (files, rows, bytes) = report.totals();
    println!(
        "{} verified: {} days, {files} files, {rows} rows, {bytes} bytes",
        report.repo,
        report.days.len()
    );
    if !full {
        // Said every time rather than in the help, because the difference
        // between "the hub says the bytes are right" and "the bytes are right"
        // is the entire subject of the command.
        println!("digests compared against the hub's; --full downloads and checks them here");
    }
    Ok(())
}

/// Let an operator type the short name of one of our own repositories.
///
/// `umi verify umi-focus-example.com` is the common case and spelling the
/// organisation twice is the common annoyance. Anything with a slash in it is
/// already qualified and is left exactly as typed, because guessing at a name
/// somebody spelled out in full is how you verify the wrong repository.
fn qualified(name: &str, org: &str) -> String {
    if name.contains('/') {
        name.to_owned()
    } else {
        format!("{org}/{name}")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_short_name_gets_the_organisation_and_a_full_one_does_not() {
        assert_eq!(
            super::qualified("umi-focus-example.com", "open-index"),
            "open-index/umi-focus-example.com"
        );
        assert_eq!(
            super::qualified("somebody/umi-focus-theirs", "open-index"),
            "somebody/umi-focus-theirs",
            "a name that was spelled out is used as spelled"
        );
    }

    #[test]
    fn the_meta_repository_follows_the_organisation() {
        // Doc 14.7 lets an operator set publish.org, and a verifier that read
        // the keys from open-index while checking somebody else's repository
        // would be checking a signature against the wrong key directory.
        assert_eq!(umi_publish::repo::META_REPO, "open-index/umi-meta");
    }
}
