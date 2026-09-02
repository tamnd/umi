//! `umi cards`, rewriting doc 12.9's dataset card on repositories that already
//! exist.
//!
//! A card is written once, on the commit that creates a repository, and then
//! never again. That is deliberate: the card is generated, so writing it on
//! every publish would be a no change commit per segment and a repository whose
//! history is ten thousand identical README commits is one nobody can read the
//! real history of. The cost of that decision is that a card improvement, or a
//! correction that readers need to see, reaches nothing already published.
//!
//! This is the pass that fixes it. It is a separate job from publishing on
//! purpose and it is meant to be run rarely, by hand, when the card generator
//! has changed and the change is worth a commit on every repository.
//!
//! It writes nothing to a repository umi did not publish. An organisation holds
//! other things, `open-index/ccrawl-domains` among them, and a card refresh that
//! guessed at family from a name it did not recognise would put a robots schema
//! on somebody else's dataset.

use umi_publish::{Corpus, Family, Hub, Upload, card};

use crate::Error;

/// What a run did, so the caller can print it and a test can assert on it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Repositories that got a new card.
    pub written: Vec<String>,
    /// Repositories whose card was already exactly this, so nothing was
    /// committed.
    pub unchanged: Vec<String>,
    /// Repositories in the organisation that umi did not publish and that were
    /// left alone.
    pub skipped: Vec<String>,
}

/// Rewrite the card on one repository, or on every umi repository in the
/// organisation.
///
/// # Errors
///
/// [`Error::Missing`] without a token, since this writes, and when a named
/// repository is not one of umi's families. Otherwise whatever the hub says.
pub fn run(
    target: Option<&str>,
    token: Option<String>,
    org: &str,
    extractor: &str,
    dry_run: bool,
) -> Result<Report, Error> {
    let Some(token) = token.filter(|t| !t.is_empty()) else {
        return Err(Error::Missing(
            "umi cards writes to the hub, so it needs a token: set publish.token in umi.toml as \
             env:NAME or file:/path, or point $UMI_TOKEN at one of those"
                .to_owned(),
        ));
    };
    let hub = Hub::new(token)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    runtime.block_on(refresh(&hub, target, org, extractor, dry_run))
}

/// The whole pass, once there is a hub to run it against.
async fn refresh(
    hub: &Hub,
    target: Option<&str>,
    org: &str,
    extractor: &str,
    dry_run: bool,
) -> Result<Report, Error> {
    let repos = match target {
        Some(name) => vec![qualified(name, org)],
        None => hub.datasets(org).await?,
    };
    // A named repository that is not a family is the operator's mistake and
    // worth an error. The same repository turning up in a listing of the whole
    // organisation is not a mistake, it is `ccrawl-domains`, and it is skipped.
    if let (Some(name), [only]) = (target, repos.as_slice())
        && Family::of_repo(only).is_none()
    {
        return Err(Error::Missing(format!(
            "{name} is not one of umi's dataset families, so there is no card to write for it"
        )));
    }

    let corpus = Corpus::new(org);
    let mut report = Report::default();
    for repo in repos {
        let Some(family) = Family::of_repo(&repo) else {
            report.skipped.push(repo);
            continue;
        };
        let card = card(&corpus, family, extractor);
        // Read before writing. A commit that changes nothing still shows up in
        // the history and still moves the revision every reader has pinned, so
        // a refresh that runs twice should be a no op the second time.
        let current = hub.read(&repo, "README.md").await?;
        if current.as_deref() == Some(card.as_bytes()) {
            report.unchanged.push(repo);
            continue;
        }
        if !dry_run {
            hub.upload(
                &repo,
                &[Upload::Inline {
                    path: "README.md".to_owned(),
                    bytes: card.into_bytes(),
                }],
                "Refresh the dataset card",
            )
            .await?;
        }
        report.written.push(repo);
    }
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
            qualified("someone-else/umi-robots", "open-index"),
            "someone-else/umi-robots"
        );
    }

    #[test]
    fn a_run_without_a_token_says_so_rather_than_failing_at_the_hub() {
        let err = run(None, None, "open-index", "umi/0.0.1", false).expect_err("no token");
        assert!(
            err.to_string().contains("needs a token"),
            "said {err} instead"
        );
        // An empty variable is the same as no variable. A shell that exported
        // the name and not the value should not get a 401 from the hub as its
        // explanation.
        let err = run(None, Some(String::new()), "open-index", "umi/0.0.1", false)
            .expect_err("empty token");
        assert!(
            err.to_string().contains("needs a token"),
            "said {err} instead"
        );
    }
}
