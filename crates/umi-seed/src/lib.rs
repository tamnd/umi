//! Where a crawl gets its first URLs.
//!
//! Doc 13.7 makes the seeder contract one sentence: a seeder is any program
//! that writes URLs to stdout, one per line, and exits zero. There is no plugin
//! API, no shared library and nothing to register. The payoff is that the 604
//! repositories named `tamnd/*-cli` are already seed sources without a line of
//! change to any of them, that a seeder can be written in any language, and
//! that a seeder which crashes cannot take the crawler down with it.
//!
//! ```no_run
//! use umi_seed::{Limits, Source, seed};
//!
//! let mut stream = seed(Source::shell("arxiv-cli list --category cs.IR"), Limits::default())?;
//! for item in &mut stream {
//!     let item = item?;
//!     println!("{} {}", item.keys.url, item.url);
//! }
//! println!("{:?}", stream.stats());
//! # Ok::<(), umi_seed::Error>(())
//! ```
//!
//! Three things about that loop are the whole crate.
//!
//! It streams. Nothing here ever holds the seeder's output, and a seeder that
//! prints for six hours is read as it prints. Even one line is bounded: a
//! program that emits a gigabyte with no newline in it gets that line counted
//! as too long and skipped, rather than being read into memory to find out.
//!
//! A non zero exit from the seeder is the last item of the iterator and it is
//! an error. Doc 13.7's contract has two halves and the exit code is the half
//! that is easy to drop, which turns "the API key expired" into "the frontier
//! is empty" and then into a crawl that quietly does nothing.
//!
//! Every URL goes through doc 11.2 canonicalisation and comes out with the same
//! [`RowKey`] admission would derive. A seeder cannot set a priority, cannot
//! mark a URL fetched and cannot skip robots. It is a source of candidates and
//! nothing else, which is exactly why the contract can be one sentence long.
//!
//! Two of doc 13.6's sources are not seeders and are here as parsers instead.
//! [`sitemap`] reads a sitemap, a sitemap index or the plain text form, and
//! [`feed`] reads RSS and Atom. Both are in the crawler rather than behind the
//! stdout contract because both carry a date the site wrote down, doc 09 feeds
//! that date to the freshness model, and a line of text on a pipe has nowhere
//! to put it.

#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::path::PathBuf;

use umi_types::{CanonError, RowKey, UrlKey, canonicalize};

pub mod date;
pub mod feed;
mod lines;
mod run;
pub mod sitemap;
mod xml;

#[cfg(test)]
mod feed_tests;
#[cfg(test)]
mod sitemap_tests;
#[cfg(test)]
mod tests;

pub use feed::Feed;
pub use run::{SeedStream, seed};
pub use sitemap::{Caps, Entry, Sitemap};

/// One accepted candidate: the canonical URL and the keys derived from it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seed {
    /// The URL after doc 11.2 canonicalisation, which is the form that gets
    /// stored, hashed and compared everywhere else.
    pub url: String,
    /// The pay level domain, host and URL keys for `url`.
    pub keys: RowKey,
}

/// Where the URLs come from.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Source {
    /// Read standard input, which is what `umi seed -` does.
    Stdin,
    /// Read a file of URLs, one per line.
    File(PathBuf),
    /// Run a program directly, with the arguments already split.
    ///
    /// No shell is involved, so nothing in the arguments is expanded and a
    /// path with a space in it needs no quoting. This is the form a config
    /// file should use.
    Command(Vec<String>),
    /// Run a command line through `sh -c`.
    ///
    /// Doc 13.7 writes seeders as shell one liners, pipes and all, and an
    /// operator typing `--seeder 'foo list | grep bar'` means the shell. This
    /// runs whatever the operator typed with the operator's own privileges,
    /// the same as a line in their Makefile, and it is not a place to accept a
    /// string that came from anywhere other than the operator.
    Shell(String),
}

impl Source {
    /// A shell command line, for the `--seeder` form.
    #[must_use]
    pub fn shell(command: impl Into<String>) -> Self {
        Self::Shell(command.into())
    }

    /// A program and its arguments, with no shell in the way.
    #[must_use]
    pub fn command<I, S>(argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Command(argv.into_iter().map(Into::into).collect())
    }

    /// How to name this source in an error message.
    fn label(&self) -> String {
        match self {
            Self::Stdin => "stdin".to_owned(),
            Self::File(path) => path.display().to_string(),
            Self::Command(argv) => argv.first().cloned().unwrap_or_default(),
            Self::Shell(line) => line.clone(),
        }
    }
}

/// The two numbers that stop a hostile or broken seeder from being a memory
/// problem.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    /// The longest line to keep, in bytes.
    ///
    /// Four times doc 11.2's 2048 byte URL cap. A URL that is merely too long
    /// is rejected by canonicalisation with a reason that says so, so anything
    /// reaching this limit is not a URL at all and there is nothing to be
    /// gained by reading the rest of it.
    pub max_line: usize,
    /// How many URL keys to remember for deduplication.
    ///
    /// This set is a courtesy, not the seen set. Doc 08 keeps the real one and
    /// admission checks it again, so the only thing this saves is the cost of
    /// canonicalising and admitting a URL the same seeder already printed. It
    /// is capped because a seeder that prints a hundred million URLs should
    /// not decide how much memory the crawler uses, and past the cap the
    /// stream stops deduplicating and says so in [`Stats::undeduplicated`]
    /// rather than dropping anything.
    pub max_seen: usize,
}

impl Default for Limits {
    fn default() -> Self {
        // 8 million keys is about 100 MB of table, against doc 01's box. A
        // seeder that prints more than that in one run is enumerating a whole
        // site and its output is mostly distinct anyway.
        Self {
            max_line: 4 * umi_types::canon::MAX_URL_LEN,
            max_seen: 8_000_000,
        }
    }
}

/// What a seeding run did, which is what the operator needs to see when a
/// crawl starts smaller than expected.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Stats {
    /// Lines read, including blanks and comments.
    pub lines: u64,
    /// URLs handed to the caller.
    pub accepted: u64,
    /// Lines that were blank or a comment.
    pub skipped: u64,
    /// URLs this run had already produced.
    pub duplicate: u64,
    /// Lines that did not canonicalise to a crawlable http or https URL.
    pub rejected: u64,
    /// Why they were rejected.
    pub why: Rejected,
    /// Lines longer than [`Limits::max_line`], which were skipped unread.
    pub too_long: u64,
    /// Lines that were not UTF-8.
    pub not_utf8: u64,
    /// Whether the deduplication set hit [`Limits::max_seen`], after which
    /// duplicates are passed through rather than dropped.
    pub undeduplicated: bool,
}

/// A count per reason for the lines that did not canonicalise.
///
/// One number for rejections is enough to know a seed list is bad and not
/// enough to fix it. A million `NotHttp` means the seeder is printing
/// `mailto:` and `javascript:` links, a million `Malformed` means it is
/// printing relative paths and needs a base, and those are different bugs in
/// somebody else's program. The variants are doc 11.2's, so this stays in step
/// with canonicalisation for free.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Rejected {
    /// Not an http or https URL.
    pub not_http: u64,
    /// Unparseable, or relative with no base to resolve against.
    pub malformed: u64,
    /// Parsed, but with no host in it.
    pub no_host: u64,
    /// The host failed IDNA validation.
    pub bad_host: u64,
    /// Longer than doc 11.2's 2048 byte cap once canonicalised.
    pub too_long: u64,
}

impl Rejected {
    fn count(&mut self, error: CanonError) {
        match error {
            CanonError::NotHttp => self.not_http += 1,
            CanonError::Malformed => self.malformed += 1,
            CanonError::NoHost => self.no_host += 1,
            CanonError::BadHost => self.bad_host += 1,
            CanonError::TooLong => self.too_long += 1,
        }
    }

    /// The reason that fired most, and how often, for a one line summary.
    #[must_use]
    pub fn worst(&self) -> Option<(CanonError, u64)> {
        [
            (CanonError::NotHttp, self.not_http),
            (CanonError::Malformed, self.malformed),
            (CanonError::NoHost, self.no_host),
            (CanonError::BadHost, self.bad_host),
            (CanonError::TooLong, self.too_long),
        ]
        .into_iter()
        .filter(|&(_, count)| count > 0)
        // Not `max_by_key`, which keeps the last of an equal run. A tie should
        // report the same reason on every machine and the order above is the
        // one doc 11.2 lists, so the first wins.
        .reduce(|best, next| if next.1 > best.1 { next } else { best })
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} lines, {} accepted, {} duplicate, {} rejected, {} skipped",
            self.lines, self.accepted, self.duplicate, self.rejected, self.skipped
        )?;
        if let Some((reason, count)) = self.why.worst() {
            write!(f, " (mostly {reason}, {count})")?;
        }
        if self.too_long > 0 {
            write!(f, ", {} too long", self.too_long)?;
        }
        if self.not_utf8 > 0 {
            write!(f, ", {} not utf-8", self.not_utf8)?;
        }
        if self.undeduplicated {
            f.write_str(", deduplication gave up at the cap")?;
        }
        Ok(())
    }
}

/// What can go wrong while seeding.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Reading the source failed.
    #[error("reading {source_name}: {cause}")]
    Read {
        /// Which source.
        source_name: String,
        /// The underlying failure.
        #[source]
        cause: io::Error,
    },
    /// The seeder program could not be started at all.
    #[error("could not start the seeder `{program}`: {cause}")]
    Spawn {
        /// The command line as the operator wrote it.
        program: String,
        /// The underlying failure.
        #[source]
        cause: io::Error,
    },
    /// The seeder ran and then failed.
    ///
    /// Doc 13.7 says a seeder exits zero. One that does not has not produced
    /// an empty list of URLs, it has failed, and the difference matters
    /// because the first one starts a crawl of nothing and looks like success.
    #[error("the seeder `{program}` {status}{tail}")]
    Failed {
        /// The command line as the operator wrote it.
        program: String,
        /// How it ended, as a human readable phrase.
        status: String,
        /// The last of the seeder's own standard error, already prefixed with
        /// a newline when there is any.
        tail: String,
    },
}

/// The dedup set, kept here so the stream stays about I/O.
struct Seen {
    keys: HashSet<[u8; UrlKey::LEN]>,
    cap: usize,
    full: bool,
}

impl Seen {
    fn new(cap: usize) -> Self {
        Self {
            keys: HashSet::new(),
            cap,
            full: false,
        }
    }

    /// Whether this key is new. Once the cap is reached everything is new,
    /// because passing a duplicate to admission costs a hash lookup there and
    /// growing without bound costs the machine.
    fn admit(&mut self, key: UrlKey) -> bool {
        if self.full {
            return true;
        }
        if self.keys.len() >= self.cap {
            self.full = true;
            // Drop the table rather than hold 100 MB that will never be read
            // again. Nothing after this point looks at it.
            self.keys = HashSet::new();
            return true;
        }
        self.keys.insert(*key.as_bytes())
    }
}

/// Turn one line into a seed, or say why not.
///
/// Blank lines and lines starting with `#` are skipped rather than rejected.
/// A seed file people edit by hand wants comments in it, and no canonical URL
/// can begin with either, so nothing is lost by allowing them.
fn parse(line: &[u8]) -> Result<Option<Seed>, Rejection> {
    let text = core::str::from_utf8(line).map_err(|_| Rejection::NotUtf8)?;
    let text = text.trim();
    if text.is_empty() || text.starts_with('#') {
        return Ok(None);
    }
    let url = canonicalize(text, None).map_err(Rejection::Canon)?;
    let keys = RowKey::for_canonical(&url).map_err(Rejection::Canon)?;
    Ok(Some(Seed { url, keys }))
}

/// Why one line did not become a seed.
enum Rejection {
    NotUtf8,
    Canon(CanonError),
}
