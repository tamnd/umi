//! `umi block`, from `docs/spec/07-politeness-and-identity.md` section 7.7 and
//! `docs/spec/14-cli.md`.
//!
//! This is the command an operator runs because somebody asked us to stop. Doc
//! 07.7 commits to applying a block within one hour of a valid request, so
//! everything here is arranged around that promise rather than around
//! convenience: the block is durable before the command returns, it takes the
//! domain's urls out of the frontier in the same transaction, and it goes to
//! the published list so that the other coordinators and anyone holding an
//! older snapshot see it too.
//!
//! Three decisions are worth saying out loud, because they all cost something
//! and all of them are deliberate.
//!
//! The unit is the registrable domain. Somebody typing `news.example.com` is
//! asking for that site to stop being crawled, and blocking one host while the
//! rest of the site keeps being fetched would be honouring the letter of the
//! request rather than the request. The command says when it has widened what
//! it was given, because a block that quietly covers more than was typed is as
//! bad as one that quietly covers less.
//!
//! Lifting is a record and not a delete. Doc 07.7 says blocks are never
//! silently reversed and that a domain asking to be unblocked gets a dated
//! record of both events, so `--lift` writes the lift onto the same entry and
//! the entry stays in the published list forever.
//!
//! Publishing is part of applying. Without a token the block still lands
//! locally and the command says the list was not published, which is a warning
//! and not a failure: a block that only half worked because the network was
//! down is still better than no block, and the next run publishes it.

use std::io::{BufWriter, Write};
use std::path::Path;

use umi_publish::{Hub, publish_blocks};
use umi_state::{BlockRow, State};
use umi_state_sqlite::SqliteState;

use crate::Error;

/// What `umi block` was asked to do.
pub struct Options<'a> {
    /// The crawl directory holding the state file.
    pub dir: &'a Path,
    /// The domain, or nothing to print the list.
    pub domain: Option<&'a str>,
    /// Why, which is published with the block. Required with a domain.
    pub reason: Option<&'a str>,
    /// Record that this block has been lifted, with `reason` as the note.
    pub lift: bool,
    /// The Hugging Face write token, or nothing to skip publishing.
    pub token: Option<String>,
    /// The organisation the meta repository lives in.
    pub org: &'a str,
    /// Now, in milliseconds. An argument for the same reason every other
    /// timestamp in this workspace is one.
    pub now_ms: u64,
}

/// Apply a block, lift one, or print the list.
///
/// # Errors
///
/// [`Error::Missing`] when a domain is given without a reason, or when a lift
/// names a domain that was never blocked. [`Error::State`] when the store will
/// not open or will not take the block, and [`Error::Publish`] when the block
/// landed locally and the published list would not take it.
pub fn run(options: &Options<'_>) -> Result<(), Error> {
    let path = options.dir.join(crate::crawl::STATE);
    if !path.exists() {
        return Err(Error::Missing(format!(
            "no crawl state at {}: umi block works on a crawl directory",
            path.display()
        )));
    }
    let state = SqliteState::open(&path).map_err(|e| Error::State(e.to_string()))?;

    // One runtime, current thread, because this is one small transaction and
    // at most one commit to a repository. Nothing here is concurrent.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    runtime.block_on(apply(&state, options))
}

async fn apply(state: &SqliteState, options: &Options<'_>) -> Result<(), Error> {
    let Some(domain) = options.domain else {
        return list(state).await;
    };
    let Some(reason) = options.reason else {
        return Err(Error::Missing(
            "umi block needs --reason: it is published with the block and it is what \
             explains the block to somebody reading it in a year"
                .to_owned(),
        ));
    };

    let row = row_for(state, domain, reason, options).await?;
    if row.domain != domain {
        // Said before the block is applied rather than after, so that an
        // operator who meant only the one host finds out at the moment they
        // can still change their mind.
        eprintln!(
            "umi: {domain} is part of {}, and the whole domain is being blocked",
            row.domain
        );
    }

    let report = state
        .block(std::slice::from_ref(&row))
        .await
        .map_err(|e| Error::State(e.to_string()))?;
    if options.lift {
        println!(
            "{} lifted, {} urls back in the frontier",
            row.domain, report.restored
        );
    } else {
        println!(
            "{} blocked, {} urls out of the frontier",
            row.domain, report.excluded
        );
    }

    publish(&row, options).await
}

/// The row to write, which for a lift is the stored block with the lift on it.
///
/// A lift has to find the block it is lifting. Building a fresh row and marking
/// it lifted would work in the sense that the domain would be crawlable again,
/// and it would lose the original date and the original reason, which is the
/// half of the record doc 07.7 actually cares about.
async fn row_for(
    state: &SqliteState,
    domain: &str,
    reason: &str,
    options: &Options<'_>,
) -> Result<BlockRow, Error> {
    let fresh = BlockRow::new(domain, reason, options.now_ms);
    if !options.lift {
        return Ok(fresh);
    }
    let stored = state
        .blocks()
        .await
        .map_err(|e| Error::State(e.to_string()))?
        .into_iter()
        .find(|block| block.pld == fresh.pld)
        .ok_or_else(|| {
            Error::Missing(format!(
                "{} is not blocked, so there is nothing to lift",
                fresh.domain
            ))
        })?;
    if !stored.in_force() {
        return Err(Error::Missing(format!(
            "{} was already lifted, and doc 07.7 keeps the first record",
            stored.domain
        )));
    }
    Ok(stored.lift(reason, options.now_ms))
}

/// Put the block in `umi-meta`, or say why it is only local.
///
/// A missing token is a warning and not an error. The block is applied either
/// way, and doc 07.7's one hour promise is about stopping the crawling. A crawl
/// directory an operator uses for a focused run may well have no write token
/// anywhere near it, and refusing to block a domain because of that would be
/// the wrong answer to the wrong question.
async fn publish(row: &BlockRow, options: &Options<'_>) -> Result<(), Error> {
    let Some(token) = &options.token else {
        eprintln!(
            "umi: the block is local only, because there is no publish.token to \
             put it in {}/umi-meta with",
            options.org
        );
        return Ok(());
    };
    let hub = Hub::new(token.clone())?;
    let repo = format!("{}/umi-meta", options.org);
    let written = publish_blocks(&hub, &repo, std::slice::from_ref(row)).await?;
    if written == 0 {
        println!("{repo} already says this");
    } else {
        println!("published to {repo}");
    }
    Ok(())
}

/// Print the list, which is what the command with no domain does.
///
/// In domain order rather than in the order the store keeps them, which is by
/// the hash of the domain. Both are stable and only one of them is readable.
async fn list(state: &SqliteState) -> Result<(), Error> {
    let mut blocks = state
        .blocks()
        .await
        .map_err(|e| Error::State(e.to_string()))?;
    if blocks.is_empty() {
        return Err(Error::NothingToDo("nothing is blocked"));
    }
    blocks.sort_by(|a, b| a.domain.cmp(&b.domain));

    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    writeln!(
        out,
        "{:<32} {:<12} {:<8} reason",
        "domain", "applied", "status"
    )
    .map_err(Error::Io)?;
    for block in &blocks {
        let status = if block.in_force() {
            "blocked"
        } else {
            "lifted"
        };
        writeln!(
            out,
            "{:<32} {:<12} {status:<8} {}",
            block.domain,
            date(block.blocked_ms),
            block.reason
        )
        .map_err(Error::Io)?;
        if let Some(lifted_ms) = block.lifted_ms {
            // Indented under the block it undoes, because the two dates
            // together are the record and a lift on its own is half a story.
            writeln!(
                out,
                "{:<32} {:<12} {:<8} {}",
                "",
                date(lifted_ms),
                "",
                block.lifted_reason
            )
            .map_err(Error::Io)?;
        }
    }
    out.flush().map_err(Error::Io)
}

/// A timestamp as `YYYY-MM-DD`, which is the precision a block has.
fn date(ms: u64) -> String {
    let (year, month, day) = umi_types::date::civil_from_days(ms / 86_400_000);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use umi_state::{Candidate, State};
    use umi_state_sqlite::SqliteState;

    use super::{Options, run};

    const T0: u64 = 1_787_000_000_000;

    fn options<'a>(dir: &'a std::path::Path, domain: Option<&'a str>) -> Options<'a> {
        Options {
            dir,
            domain,
            reason: Some("the site owner asked us to stop, ticket 41"),
            lift: false,
            token: None,
            org: "open-index",
            now_ms: T0,
        }
    }

    /// A crawl directory with one url in its frontier.
    fn crawl_dir() -> TempDir {
        let dir = TempDir::new().expect("a temp directory");
        let state =
            SqliteState::open(dir.path().join(crate::crawl::STATE)).expect("a new state file");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            state
                .admit(&[
                    Candidate::new("https://news.example.com/one", T0).expect("a crawlable url")
                ])
                .await
                .expect("admit");
        });
        dir
    }

    /// How many urls are in the frontier and how many are out of it.
    ///
    /// Counted rather than leased, because a lease is not a question: it takes
    /// the url and starts the host's politeness timer, so asking twice would
    /// answer something different the second time for a reason that has
    /// nothing to do with the block.
    fn frontier(dir: &std::path::Path) -> (u64, u64) {
        let state = SqliteState::open(dir.join(crate::crawl::STATE)).expect("the state file");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            let stats = state.stats().await.expect("stats");
            (stats.urls_pending, stats.urls_excluded)
        })
    }

    #[test]
    fn a_block_stops_the_domain_and_a_lift_gives_it_back() {
        let dir = crawl_dir();
        assert_eq!(
            frontier(dir.path()),
            (1, 0),
            "the fixture has nothing in it"
        );

        run(&options(dir.path(), Some("news.example.com"))).expect("block");
        assert_eq!(
            frontier(dir.path()),
            (0, 1),
            "a blocked domain was left in the frontier"
        );

        let mut lift = options(dir.path(), Some("news.example.com"));
        lift.lift = true;
        lift.reason = Some("they changed their minds, ticket 41");
        run(&lift).expect("lift");
        assert_eq!(
            frontier(dir.path()),
            (1, 0),
            "a lifted domain did not come back"
        );

        // And the record of both events, which is the half doc 07.7 is
        // strictest about. A lift that left nothing behind would pass every
        // assertion above.
        let state = SqliteState::open(dir.path().join(crate::crawl::STATE)).expect("the state");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        let blocks = runtime.block_on(async { state.blocks().await.expect("blocks") });
        assert_eq!(blocks.len(), 1, "the lift deleted the record");
        assert_eq!(blocks[0].blocked_ms, T0, "the lift lost the original date");
        assert!(blocks[0].lifted_ms.is_some(), "the lift was not dated");
    }

    #[test]
    fn lifting_something_that_was_never_blocked_says_so() {
        let dir = crawl_dir();
        let mut lift = options(dir.path(), Some("example.com"));
        lift.lift = true;
        let error = run(&lift).expect_err("nothing to lift");
        assert!(format!("{error}").contains("not blocked"), "{error}");
    }

    #[test]
    fn a_domain_with_no_reason_is_refused() {
        let dir = crawl_dir();
        let mut bare = options(dir.path(), Some("example.com"));
        bare.reason = None;
        let error = run(&bare).expect_err("no reason");
        assert!(format!("{error}").contains("--reason"), "{error}");
    }

    #[test]
    fn a_directory_that_is_not_a_crawl_says_so_rather_than_creating_one() {
        // Opening a sqlite file creates it, so the check has to be here rather
        // than left to the store. A typo in the directory would otherwise
        // report a block applied to a state file nothing will ever read.
        let dir = TempDir::new().expect("a temp directory");
        let error = run(&options(dir.path(), Some("example.com"))).expect_err("no crawl there");
        assert!(format!("{error}").contains("no crawl state"), "{error}");
    }

    #[test]
    fn an_empty_list_is_doc_14_9s_nothing_to_do() {
        let dir = crawl_dir();
        let error = run(&options(dir.path(), None)).expect_err("nothing blocked yet");
        assert_eq!(error.exit(), umi_types::Exit::NothingToDo, "{error}");
    }
}
