//! `umi supervise`, from `docs/spec/05-fetch-tiers.md` section 5.7 and
//! `docs/spec/14-cli.md`.
//!
//! T4 is the one rung nothing reaches by learning. Every other tier in doc 05
//! is the crawler making a decision about a page in front of it, and the whole
//! ladder is arranged so that those decisions cost the site as little as
//! possible. T4 is different in kind: it is a person deciding that one domain
//! gets a real browser, writing down why, and putting their name on it. This
//! command is where that happens, and it is deliberately the only route.
//!
//! Three things are load bearing.
//!
//! It is an allowlist and never a heuristic. There is no signal anywhere in
//! umi that escalates a host to T4, no counter that gets there after enough
//! failures, and no config key that lowers the bar. `TierPolicy::CEILING` is
//! T2 and the only thing above it that any host reaches on its own is T3 on a
//! shell page. If a domain is at T4 it is because somebody typed this command.
//!
//! Every entry names a person. Not a machine, not a service account, a person
//! who can be asked about it later. An entry with no operator is refused here
//! and refused again by the publisher, because an anonymous allowlist entry is
//! the exact thing publishing the list is meant to prevent.
//!
//! The list is published and so is the record of what it was used for. The
//! entries go to `umi-meta` next to the block list, and `--show` prints the
//! per fetch ledger for one domain, which is what an operator gets sent when
//! they ask what we pointed a browser at.

use std::io::{BufWriter, Write};
use std::path::Path;

use umi_crawl::SupervisedLedger;
use umi_publish::{Hub, publish_supervised};
use umi_state::{State, SupervisionRow};
use umi_state_sqlite::SqliteState;

use crate::Error;

/// What `umi supervise` was asked to do.
pub struct Options<'a> {
    /// The crawl directory holding the state file.
    pub dir: &'a Path,
    /// The domain, or nothing to print the list.
    pub domain: Option<&'a str>,
    /// Why, which is published with the entry. Required with a domain.
    pub reason: Option<&'a str>,
    /// Who is adding it. Required when adding.
    pub operator: Option<&'a str>,
    /// Take the domain off the list, with `reason` as the note.
    pub remove: bool,
    /// Print what T4 has fetched from this domain instead of changing
    /// anything.
    pub show: bool,
    /// The Hugging Face write token, or nothing to skip publishing.
    pub token: Option<String>,
    /// The organisation the meta repository lives in.
    pub org: &'a str,
    /// Now, in milliseconds.
    pub now_ms: u64,
}

/// Add a domain, remove one, print the list, or show what T4 did.
///
/// # Errors
///
/// [`Error::Missing`] when a domain is given without the fields an entry needs,
/// or when a removal names a domain that was never on the list.
/// [`Error::State`] when the store will not open or will not take the entry,
/// and [`Error::Publish`] when the entry landed locally and the published list
/// would not take it.
pub fn run(options: &Options<'_>) -> Result<(), Error> {
    let path = options.dir.join(crate::crawl::STATE);
    if !path.exists() {
        return Err(Error::Missing(format!(
            "no crawl state at {}: umi supervise works on a crawl directory",
            path.display()
        )));
    }
    let state = SqliteState::open(&path).map_err(|e| Error::State(e.to_string()))?;

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
    if options.show {
        return show(domain, options);
    }
    let Some(reason) = options.reason else {
        return Err(Error::Missing(
            "umi supervise needs --reason: it is published with the entry and it is \
             what explains a browser at somebody's site to them"
                .to_owned(),
        ));
    };

    let row = row_for(state, domain, reason, options).await?;
    if row.domain != domain {
        eprintln!(
            "umi: {domain} is part of {}, and the whole domain is going on the list",
            row.domain
        );
    }

    let written = state
        .supervise(std::slice::from_ref(&row))
        .await
        .map_err(|e| Error::State(e.to_string()))?;
    if options.remove {
        println!("{} is off the supervised list", row.domain);
    } else {
        println!("{} is supervised, added by {}", row.domain, row.operator);
        // Said every time rather than once, because the gap between the list
        // and what the fetchers will do is the thing an operator is most
        // likely to be surprised by, and the surprise is worse in the other
        // direction.
        eprintln!(
            "umi: this raises a ceiling and nothing else. Only a fetcher run with \
             --allow-supervised will take T4 work, and no build has the supervised \
             engine yet, so fetches descend to T3 and say so."
        );
    }
    let _ = written;

    publish(&row, options).await
}

/// The row to write, which for a removal is the stored entry with the removal
/// on it.
///
/// A removal has to find the entry it is removing, for the same reason a lifted
/// block does: building a fresh row and marking it removed would take the
/// domain off the list and lose the date it went on and the person who put it
/// there, which is the half of the record worth keeping.
async fn row_for(
    state: &SqliteState,
    domain: &str,
    reason: &str,
    options: &Options<'_>,
) -> Result<SupervisionRow, Error> {
    if options.remove {
        let want = SupervisionRow::new(domain, "", reason, options.now_ms);
        let stored = state
            .supervision()
            .await
            .map_err(|e| Error::State(e.to_string()))?
            .into_iter()
            .find(|row| row.pld == want.pld)
            .ok_or_else(|| {
                Error::Missing(format!(
                    "{} is not supervised, so there is nothing to remove",
                    want.domain
                ))
            })?;
        if !stored.in_force() {
            return Err(Error::Missing(format!(
                "{} was already removed on {}",
                stored.domain,
                date(stored.removed_ms.unwrap_or_default())
            )));
        }
        return Ok(stored.remove(reason, options.now_ms));
    }

    let Some(operator) = options.operator else {
        return Err(Error::Missing(
            "umi supervise needs --operator: doc 05.7 wants a person on every entry, \
             because the point of publishing the list is that somebody can be asked \
             about what is on it"
                .to_owned(),
        ));
    };
    if operator.trim().is_empty() {
        return Err(Error::Missing(
            "umi supervise needs a real name in --operator".to_owned(),
        ));
    }
    Ok(SupervisionRow::new(
        domain,
        operator,
        reason,
        options.now_ms,
    ))
}

/// Put the entry in `umi-meta`, or say why it is only local.
///
/// A missing token is a warning here and not an error, the same as it is for a
/// block, and for a weaker reason: nothing gets crawled at T4 by a fetcher that
/// has not opted in, so an unpublished entry is inert rather than dangerous.
/// The warning still says it, because an operator who thinks the list is public
/// and finds out later that it never left the box has been misled by us.
async fn publish(row: &SupervisionRow, options: &Options<'_>) -> Result<(), Error> {
    let Some(token) = &options.token else {
        eprintln!(
            "umi: the entry is local only, because there is no publish.token to put \
             it in {}/umi-meta with",
            options.org
        );
        return Ok(());
    };
    let hub = Hub::new(token.clone())?;
    let repo = format!("{}/umi-meta", options.org);
    let written = publish_supervised(&hub, &repo, std::slice::from_ref(row)).await?;
    if written == 0 {
        println!("{repo} already says this");
    } else {
        println!("published to {repo}");
    }
    Ok(())
}

/// Print the list, which is what the command with no domain does.
async fn list(state: &SqliteState) -> Result<(), Error> {
    let mut rows = state
        .supervision()
        .await
        .map_err(|e| Error::State(e.to_string()))?;
    if rows.is_empty() {
        return Err(Error::NothingToDo("nothing is supervised"));
    }
    rows.sort_by(|a, b| a.domain.cmp(&b.domain));

    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    writeln!(
        out,
        "{:<32} {:<12} {:<16} {:<10} reason",
        "domain", "added", "operator", "status"
    )
    .map_err(Error::Io)?;
    for row in &rows {
        let status = if row.in_force() {
            "in force"
        } else {
            "removed"
        };
        writeln!(
            out,
            "{:<32} {:<12} {:<16} {status:<10} {}",
            row.domain,
            date(row.added_ms),
            row.operator,
            row.reason
        )
        .map_err(Error::Io)?;
        if let Some(removed_ms) = row.removed_ms {
            writeln!(
                out,
                "{:<32} {:<12} {:<16} {:<10} {}",
                "",
                date(removed_ms),
                "",
                "",
                row.removed_reason
            )
            .map_err(Error::Io)?;
        }
    }
    out.flush().map_err(Error::Io)
}

/// Print every T4 fetch under one domain, which is the answer to the question
/// the whole allowlist exists to be able to answer.
///
/// Straight out of the ledger and unaggregated. Somebody who asks what we
/// fetched from their site wants the urls, and a summary would be us deciding
/// which part of our own record they get to see.
fn show(domain: &str, options: &Options<'_>) -> Result<(), Error> {
    let ledger = SupervisedLedger::in_dir(options.dir);
    let entries = ledger
        .under(domain)
        .map_err(|e| Error::Crawl(e.to_string()))?;
    if entries.is_empty() {
        return Err(Error::NothingToDo(
            "nothing has been fetched at T4 from this domain",
        ));
    }

    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    writeln!(
        out,
        "{:<12} {:<7} {:<10} {:<5} url",
        "date", "status", "bytes", "tier"
    )
    .map_err(Error::Io)?;
    for entry in &entries {
        let tier = umi_types::Tier::from_u8(entry.tier_used)
            .map_or_else(|| "?".to_owned(), |tier| tier.to_string());
        writeln!(
            out,
            "{:<12} {:<7} {:<10} {tier:<5} {}",
            date(entry.fetched_at_ms),
            entry.status,
            entry.bytes,
            entry.url
        )
        .map_err(Error::Io)?;
    }
    writeln!(
        out,
        "{} fetches, from {}",
        entries.len(),
        ledger.path().display()
    )
    .map_err(Error::Io)?;
    out.flush().map_err(Error::Io)
}

/// A timestamp as `YYYY-MM-DD`, which is the precision an entry has.
fn date(ms: u64) -> String {
    let (year, month, day) = umi_types::date::civil_from_days(ms / 86_400_000);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use umi_state::State;
    use umi_state_sqlite::SqliteState;

    use super::{Options, run};

    const T0: u64 = 1_787_000_000_000;

    fn options<'a>(dir: &'a std::path::Path, domain: Option<&'a str>) -> Options<'a> {
        Options {
            dir,
            domain,
            reason: Some("the archive asked us to mirror their catalogue, agreed 2026-08-20"),
            operator: Some("tam"),
            remove: false,
            show: false,
            token: None,
            org: "open-index",
            now_ms: T0,
        }
    }

    fn crawl_dir() -> TempDir {
        let dir = TempDir::new().expect("a temp directory");
        SqliteState::open(dir.path().join(crate::crawl::STATE)).expect("a new state file");
        dir
    }

    fn stored(dir: &std::path::Path) -> Vec<umi_state::SupervisionRow> {
        let state = SqliteState::open(dir.join(crate::crawl::STATE)).expect("the state file");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        runtime.block_on(async { state.supervision().await.expect("supervision") })
    }

    #[test]
    fn a_domain_goes_on_the_list_and_comes_off_it_on_the_record() {
        let dir = crawl_dir();
        run(&options(dir.path(), Some("catalogue.example.com"))).expect("supervise");
        let rows = stored(dir.path());
        assert_eq!(rows.len(), 1, "the entry did not land");
        assert_eq!(
            rows[0].domain, "example.com",
            "the entry is about the registrable domain"
        );
        assert_eq!(rows[0].operator, "tam");
        assert!(rows[0].in_force(), "a fresh entry is not in force");

        let mut off = options(dir.path(), Some("catalogue.example.com"));
        off.remove = true;
        off.reason = Some("the mirror is finished");
        run(&off).expect("remove");

        let rows = stored(dir.path());
        assert_eq!(rows.len(), 1, "the removal deleted the record");
        assert!(!rows[0].in_force(), "the removal did not take");
        assert_eq!(rows[0].added_ms, T0, "the removal lost the original date");
        assert_eq!(rows[0].operator, "tam", "the removal lost the operator");
    }

    #[test]
    fn an_entry_with_nobody_on_it_is_refused() {
        let dir = crawl_dir();
        let mut bare = options(dir.path(), Some("example.com"));
        bare.operator = None;
        let error = run(&bare).expect_err("no operator");
        assert!(format!("{error}").contains("--operator"), "{error}");
        assert!(
            stored(dir.path()).is_empty(),
            "a refused entry was written anyway"
        );
    }

    #[test]
    fn an_entry_with_nothing_to_say_is_refused() {
        let dir = crawl_dir();
        let mut bare = options(dir.path(), Some("example.com"));
        bare.reason = None;
        let error = run(&bare).expect_err("no reason");
        assert!(format!("{error}").contains("--reason"), "{error}");
    }

    #[test]
    fn removing_something_that_was_never_on_the_list_says_so() {
        let dir = crawl_dir();
        let mut off = options(dir.path(), Some("example.com"));
        off.remove = true;
        let error = run(&off).expect_err("nothing to remove");
        assert!(format!("{error}").contains("not supervised"), "{error}");
    }

    #[test]
    fn an_empty_list_is_doc_14_9s_nothing_to_do() {
        let dir = crawl_dir();
        let error = run(&options(dir.path(), None)).expect_err("nothing supervised yet");
        assert_eq!(error.exit(), umi_types::Exit::NothingToDo, "{error}");
    }

    #[test]
    fn showing_a_domain_nothing_has_touched_is_nothing_to_do() {
        let dir = crawl_dir();
        let mut ask = options(dir.path(), Some("example.com"));
        ask.show = true;
        let error = run(&ask).expect_err("no fetches");
        assert_eq!(error.exit(), umi_types::Exit::NothingToDo, "{error}");
    }

    #[test]
    fn a_directory_that_is_not_a_crawl_says_so_rather_than_creating_one() {
        let dir = TempDir::new().expect("a temp directory");
        let error = run(&options(dir.path(), Some("example.com"))).expect_err("no crawl there");
        assert!(format!("{error}").contains("no crawl state"), "{error}");
    }
}
