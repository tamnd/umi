//! The record of what T4 touched, from `docs/spec/05-fetch-tiers.md` section
//! 5.7.
//!
//! Putting a domain on the supervised allowlist is a promise to somebody, and
//! the thing that makes a promise checkable is a record of what was done under
//! it. This is that record: one line per T4 fetch, appended, in a file that
//! nothing rewrites.
//!
//! It has to be its own file rather than a query over the crawled rows, for a
//! reason that is specific to how umi runs. Doc 12 says the local disk is a
//! cache: once a segment is published and verified, the local copy is deleted.
//! So the rows that would answer "what did you fetch from my site with a
//! browser" are exactly the rows that go away. The ledger stays, it is small
//! because T4 is rare, and it is the file an operator gets shown when they ask.
//!
//! # Why JSON lines
//!
//! An append is one `write` of one line, so a crash leaves whole lines and at
//! worst loses the last one, and nothing has to be read back to add to it. The
//! alternative shapes all involve rewriting a document that only ever grows,
//! which is how a record ends up truncated by the thing that was meant to
//! extend it.
//!
//! # What is not in a line
//!
//! No body, no title, no extracted text. The line says which url, when, what
//! came back and how long it was, which is what answers the operator's actual
//! question. The content is in the published corpus under the same url, where
//! it is anyway, and copying it here would turn a small audit file into a
//! second copy of the crawl.
//!
//! # A line is written for the tier that was asked for
//!
//! Not the tier that answered. The operator's question is what the allowlist
//! entry was used for, and it was used the moment a url was leased at T4,
//! whatever rung ended up serving the bytes. Writing the line on the tier that
//! answered would mean a build with no supervised engine kept an empty ledger
//! while happily leasing at T4, which is the shape of record that looks clean
//! because it is not writing anything down. The `tier_used` field on each line
//! says what actually ran.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use umi_types::Tier;

use crate::page::PageRow;
use crate::{CrawlError, Sink};

/// The file name inside a crawl directory.
pub const LEDGER: &str = "supervised.jsonl";

/// An append only record of every fetch that ran at T4.
pub struct SupervisedLedger {
    path: PathBuf,
    // Opened once and held, rather than opened per batch. T4 batches are rare
    // but a crawl runs for weeks, and reopening a file to append one line is a
    // syscall pattern worth not writing down as an example.
    file: Mutex<Option<File>>,
}

impl SupervisedLedger {
    /// A ledger writing to `supervised.jsonl` in a crawl directory.
    ///
    /// The file is not created here. A crawl that never reaches T4 should
    /// leave no ledger behind, because an empty file and a missing one say the
    /// same thing and only one of them makes somebody wonder.
    #[must_use]
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            path: dir.join(LEDGER),
            file: Mutex::new(None),
        }
    }

    /// Where it writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a line for every row in the batch that ran at T4.
    ///
    /// # Errors
    ///
    /// [`CrawlError::Sink`] if the file will not open or will not take the
    /// line. A T4 fetch that cannot be recorded stops the crawl, which is the
    /// deliberate part: doc 05.7 makes the record half of what the permission
    /// means, so carrying on without it would be running the browser on an
    /// agreement we had stopped keeping.
    pub fn record(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        let supervised = Tier::Supervised as u8;
        let mut lines = String::new();
        for row in rows
            .iter()
            .filter(|row| row.tier_path.first() == Some(&supervised))
        {
            line(&mut lines, row);
        }
        if lines.is_empty() {
            return Ok(());
        }

        let mut held = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if held.is_none() {
            *held = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .map_err(|e| CrawlError::Sink(e.to_string()))?,
            );
        }
        let file = held.as_mut().expect("just opened");
        // One write for the whole batch, so a batch appears whole or not at
        // all, and then flushed to the operating system. Not fsynced: this is
        // a record and not a lease, and a crash that loses the last line of it
        // has also lost the segment those fetches were going into.
        file.write_all(lines.as_bytes())
            .map_err(|e| CrawlError::Sink(e.to_string()))?;
        file.flush().map_err(|e| CrawlError::Sink(e.to_string()))
    }

    /// Every recorded fetch under one registrable domain, as written.
    ///
    /// Lines are returned in the order they were appended, which is the order
    /// they happened. A line that does not parse is skipped rather than
    /// refused, because the point of reading this back is to answer a question
    /// from somebody who is already annoyed, and half an answer beats an error
    /// message about the last line being torn.
    ///
    /// # Errors
    ///
    /// [`CrawlError::Sink`] if the file is there and unreadable. A file that is
    /// not there is not an error: it means nothing ever ran at T4.
    pub fn under(&self, domain: &str) -> Result<Vec<Entry>, CrawlError> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(CrawlError::Sink(e.to_string())),
        };
        let want = umi_types::pay_level_domain(domain);
        Ok(text
            .lines()
            .filter_map(|line| serde_json::from_str::<Entry>(line).ok())
            .filter(|entry| umi_types::pay_level_domain(&entry.host) == want)
            .collect())
    }
}

#[async_trait::async_trait]
impl Sink for SupervisedLedger {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.record(rows)
    }
}

/// One recorded T4 fetch.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    /// The url the lease was for.
    pub url: String,
    /// The host it was on, which is what the domain filter reads.
    pub host: String,
    /// When it finished, in milliseconds.
    pub fetched_at_ms: u64,
    /// The HTTP status, or zero when there was not one.
    pub status: u16,
    /// Body bytes as received.
    pub bytes: u32,
    /// The rung that actually answered, which is not always T4. A build with
    /// no supervised engine descends to T3 and this is where that shows.
    pub tier_used: u8,
}

/// Append one line to `out`.
///
/// Written by hand rather than through `serde_json::to_string`, because the
/// line has five fields and one of them is a url, and doing it here means the
/// escaping and the field order are visible next to the format they define.
/// The order matters only for reading the file with human eyes, which is the
/// main way it will be read.
fn line(out: &mut String, row: &PageRow) {
    use std::fmt::Write as _;

    let url = escape(&row.url);
    let host = escape(&row.host);
    let _ = writeln!(
        out,
        r#"{{"url":"{url}","host":"{host}","fetched_at_ms":{},"status":{},"bytes":{},"tier_used":{}}}"#,
        row.fetched_at_ms, row.status, row.content_length, row.tier_used
    );
}

/// A string as a JSON string body, without the quotes.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// A sink that writes the ledger and then passes the batch on.
///
/// The ledger is not a replacement for the segment sink, it is a second thing
/// that happens to the same rows, and this is what lets the crawl loop keep one
/// sink argument. The ledger goes first: a T4 fetch that cannot be recorded
/// should not reach the corpus, because then the corpus would hold a row the
/// record does not explain.
pub struct Recorded<'a, S> {
    ledger: &'a SupervisedLedger,
    inner: &'a S,
}

impl<'a, S: Sink> Recorded<'a, S> {
    /// Wrap a sink.
    #[must_use]
    pub const fn new(ledger: &'a SupervisedLedger, inner: &'a S) -> Self {
        Self { ledger, inner }
    }
}

#[async_trait::async_trait]
impl<S: Sink + Sync> Sink for Recorded<'_, S> {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.ledger.record(rows)?;
        self.inner.take(rows).await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use umi_types::Tier;

    use super::SupervisedLedger;
    use crate::page::PageRow;

    const T0: u64 = 1_787_000_000_000;

    /// A row for a fetch that was leased at `asked` and answered by `used`.
    fn row(url: &str, host: &str, asked: Tier, used: Tier) -> PageRow {
        PageRow {
            url: url.to_owned(),
            host: host.to_owned(),
            fetched_at_ms: T0,
            status: 200,
            content_length: 4096,
            tier_used: used as u8,
            tier_path: if asked == used {
                vec![used as u8]
            } else {
                vec![asked as u8, used as u8]
            },
            ..blank()
        }
    }

    fn blank() -> PageRow {
        PageRow {
            url: String::new(),
            final_url: None,
            url_key: [0; 10],
            pld_id: [0; 8],
            host: String::new(),
            fetched_at_ms: 0,
            status: 0,
            outcome: umi_types::OutcomeCode::Ok,
            tier_used: 0,
            tier_path: Vec::new(),
            content_type: None,
            content_length: 0,
            lang: None,
            body_digest: [0; 32],
            chunk_root: [0; 32],
            extract_digest: [0; 32],
            markdown: None,
            title: None,
            description: None,
            headings: Vec::new(),
            snippets: Vec::new(),
            links: Vec::new(),
            headers_kept: Vec::new(),
            content_usage: None,
            sketch: umi_dedup::Sketch::default(),
            text_digest: [0; 32],
            text_bytes: 0,
            link_count: 0,
            fetcher_id: [0; 32],
            verification: 0,
            robots_checked_ms: 0,
            crawl_profile: 0,
        }
    }

    #[test]
    fn only_supervised_fetches_are_recorded() {
        let dir = TempDir::new().expect("a temp directory");
        let ledger = SupervisedLedger::in_dir(dir.path());
        ledger
            .record(&[
                row(
                    "https://a.example.com/1",
                    "a.example.com",
                    Tier::Plain,
                    Tier::Plain,
                ),
                row(
                    "https://a.example.com/2",
                    "a.example.com",
                    Tier::Rendered,
                    Tier::Rendered,
                ),
            ])
            .expect("record");
        assert!(
            !ledger.path().exists(),
            "a crawl that never reached T4 left a ledger behind"
        );

        ledger
            .record(&[row(
                "https://a.example.com/3",
                "a.example.com",
                Tier::Supervised,
                Tier::Rendered,
            )])
            .expect("record");
        let entries = ledger.under("example.com").expect("read back");
        assert_eq!(entries.len(), 1, "the wrong number of fetches was recorded");
        assert_eq!(
            entries[0].tier_used,
            Tier::Rendered as u8,
            "the line claims a rung that did not run"
        );
        assert_eq!(entries[0].url, "https://a.example.com/3");
        assert_eq!(entries[0].status, 200);
        assert_eq!(entries[0].bytes, 4096);
    }

    #[test]
    fn the_ledger_answers_for_one_domain_and_not_the_others() {
        let dir = TempDir::new().expect("a temp directory");
        let ledger = SupervisedLedger::in_dir(dir.path());
        ledger
            .record(&[
                row(
                    "https://a.example.com/1",
                    "a.example.com",
                    Tier::Supervised,
                    Tier::Supervised,
                ),
                row(
                    "https://b.example.org/1",
                    "b.example.org",
                    Tier::Supervised,
                    Tier::Supervised,
                ),
                row(
                    "https://c.example.com/1",
                    "c.example.com",
                    Tier::Supervised,
                    Tier::Supervised,
                ),
            ])
            .expect("record");

        // Two hosts under one registrable domain, because the allowlist entry
        // covers the domain and so does the answer to somebody asking about it.
        let mine = ledger.under("example.com").expect("read back");
        assert_eq!(mine.len(), 2, "the domain filter is not on the pld");
        assert!(mine.iter().all(|e| e.host.ends_with("example.com")));
    }

    #[test]
    fn appending_keeps_what_was_already_there() {
        let dir = TempDir::new().expect("a temp directory");
        let ledger = SupervisedLedger::in_dir(dir.path());
        for n in 0..3 {
            ledger
                .record(&[row(
                    &format!("https://a.example.com/{n}"),
                    "a.example.com",
                    Tier::Supervised,
                    Tier::Supervised,
                )])
                .expect("record");
        }
        // Reopened, because the interesting failure is a second process
        // truncating the first one's record rather than adding to it.
        let reopened = SupervisedLedger::in_dir(dir.path());
        reopened
            .record(&[row(
                "https://a.example.com/3",
                "a.example.com",
                Tier::Supervised,
                Tier::Supervised,
            )])
            .expect("record");
        let entries = reopened.under("example.com").expect("read back");
        assert_eq!(
            entries.len(),
            4,
            "the ledger was rewritten rather than appended"
        );
        assert_eq!(
            entries[0].url, "https://a.example.com/0",
            "the order changed"
        );
    }

    #[test]
    fn a_url_that_could_break_the_line_does_not() {
        let dir = TempDir::new().expect("a temp directory");
        let ledger = SupervisedLedger::in_dir(dir.path());
        ledger
            .record(&[row(
                "https://a.example.com/\"quoted\"\\and\\backslashed",
                "a.example.com",
                Tier::Supervised,
                Tier::Supervised,
            )])
            .expect("record");
        let entries = ledger.under("example.com").expect("read back");
        assert_eq!(entries.len(), 1, "the line did not survive its own url");
        assert!(entries[0].url.contains('"'), "the url came back changed");
    }
}
