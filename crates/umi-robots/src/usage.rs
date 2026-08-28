//! AIPREF `Content-Usage`, read from robots.txt and from the response header.
//!
//! Doc 07.5 is the policy and it is short: parse the preference, record it on
//! every affected row, and do not act on it. AIPREF is about AI usage and our
//! purpose is index building, so refusing to index a page because somebody
//! said `train-ai=n` would be answering a question nobody asked. Carrying the
//! preference into the published corpus lets a reader building a training set
//! filter on it with one predicate, which is the outcome the site wanted and
//! is a thing no other web scale corpus offers.
//!
//! Two drafts define this and neither is an RFC yet. `draft-ietf-aipref-vocab`
//! defines exactly two categories, `train-ai` and `search`, each `y`, `n` or
//! absent, written `train-ai=n, search=y`. `draft-ietf-aipref-attach` attaches
//! them to content two ways: a `Content-Usage` line in robots.txt, optionally
//! prefixed with a path pattern as in `Content-Usage: /ai-ok/ train-ai=y`, and
//! a `Content-Usage` HTTP response header carrying the same list.
//!
//! Because they are drafts, the parser's job is as much to preserve as to
//! understand. Anything it cannot read is kept verbatim rather than dropped,
//! so a site that is early to a directive we have never heard of still has its
//! words in the corpus and a reader who does know the directive can act on it.
//! Guessing at a meaning would be worse than either.
//!
//! Reconciliation across sources is the vocab draft's section 5.1 and it is
//! the one place this differs from `Allow` and `Disallow`: the most
//! restrictive answer wins rather than the most specific one. A robots.txt
//! that says `train-ai=n` and a header on one page that says `train-ai=y` are
//! not a contradiction to resolve in the page's favour, they are a `n`.

use crate::{Robots, escape_pattern, matches_pattern};

/// How many unreadable items are kept per URL.
///
/// There has to be a number here. robots.txt is capped at 500 KiB and every
/// line of it could be a `Content-Usage` line full of text, and this value is
/// written on every row of that host, so an uncapped version is a way for one
/// site to bloat the corpus. Eight is far past anything real: the vocabulary
/// has two categories and the draft's own examples never carry more than two
/// items.
///
/// Nothing is lost by the cap either, because doc 12 publishes each host's
/// robots.txt verbatim in `open-index/umi-robots`. This column is the
/// convenient copy and that snapshot is the record.
pub const MAX_UNREADABLE: usize = 8;

/// One category's stated preference.
///
/// Absent is a third state and it is not the same as either of these, so it is
/// an `Option<Preference>` everywhere rather than a variant here. A site that
/// says nothing about `search` has said nothing about `search`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Preference {
    /// `y`, the use is allowed.
    Allowed,
    /// `n`, the use is not allowed.
    Disallowed,
}

impl Preference {
    /// The draft's spelling, which is also what goes in the column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "y",
            Self::Disallowed => "n",
        }
    }

    /// A value, if it is one of the two the vocabulary defines.
    fn parse(value: &str) -> Option<Self> {
        match value {
            "y" => Some(Self::Allowed),
            "n" => Some(Self::Disallowed),
            _ => None,
        }
    }
}

/// The AIPREF preferences that apply to one URL.
///
/// Built by merging every source that has something to say about it: the
/// robots.txt lines whose path pattern matches, and the response header. The
/// merge is most restrictive wins, per the vocab draft section 5.1.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Usage {
    train_ai: Option<Preference>,
    search: Option<Preference>,
    unreadable: Vec<String>,
}

impl Usage {
    /// Parse one `Content-Usage` value, with no path pattern on the front.
    ///
    /// This is the header form, and it is also the robots form once the
    /// pattern has been split off. Items are comma separated, whitespace
    /// around them is not significant, and an item that is not one of the two
    /// known categories set to one of the two known values is kept as it was
    /// written.
    #[must_use]
    pub fn parse(value: &str) -> Self {
        let mut out = Self::default();
        for item in value.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let parsed = item.split_once('=').and_then(|(key, value)| {
                // Both halves are folded, the same way the rest of this crate
                // folds field names and user agent tokens. A site that wrote
                // `train-ai=N` meant `n`, and filing that under unreadable
                // would hide a preference from exactly the filter it was
                // written for.
                let value = Preference::parse(&value.trim().to_ascii_lowercase())?;
                match key.trim().to_ascii_lowercase().as_str() {
                    "train-ai" => Some((true, value)),
                    "search" => Some((false, value)),
                    _ => None,
                }
            });
            match parsed {
                // Two lines can disagree inside one file just as two sources
                // can, so the same rule applies here rather than last one
                // wins.
                Some((true, value)) => out.train_ai = stricter(out.train_ai, Some(value)),
                Some((false, value)) => out.search = stricter(out.search, Some(value)),
                None => out.keep(item),
            }
        }
        out
    }

    /// The `train-ai` preference, if anything stated one.
    #[must_use]
    pub const fn train_ai(&self) -> Option<Preference> {
        self.train_ai
    }

    /// The `search` preference, if anything stated one.
    #[must_use]
    pub const fn search(&self) -> Option<Preference> {
        self.search
    }

    /// Items this parser could not read, in the order they were written.
    ///
    /// A directive from a later draft, a typo, or a value outside the
    /// vocabulary all land here. Capped at [`MAX_UNREADABLE`].
    #[must_use]
    pub fn unreadable(&self) -> &[String] {
        &self.unreadable
    }

    /// Whether anything at all was stated.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.train_ai.is_none() && self.search.is_none() && self.unreadable.is_empty()
    }

    /// Fold another source's preferences into this one.
    ///
    /// Most restrictive wins for the known categories, and unreadable items
    /// accumulate without duplicates so that a header repeating what
    /// robots.txt already said does not double the column.
    pub fn merge(&mut self, other: &Self) {
        self.train_ai = stricter(self.train_ai, other.train_ai);
        self.search = stricter(self.search, other.search);
        for item in &other.unreadable {
            self.keep(item);
        }
    }

    /// The column value, or `None` when there is nothing to record.
    ///
    /// The order is fixed rather than source order, so that two hosts saying
    /// the same thing in a different order produce the same string and a
    /// reader can match on it. Known categories first, in the vocabulary's own
    /// order, then whatever we could not read.
    #[must_use]
    pub fn render(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        let mut parts = Vec::with_capacity(2 + self.unreadable.len());
        if let Some(value) = self.train_ai {
            parts.push(format!("train-ai={}", value.as_str()));
        }
        if let Some(value) = self.search {
            parts.push(format!("search={}", value.as_str()));
        }
        parts.extend(self.unreadable.iter().cloned());
        Some(parts.join(", "))
    }

    /// Record an item we could not read, once, up to the cap.
    fn keep(&mut self, item: &str) {
        if self.unreadable.len() >= MAX_UNREADABLE
            || self.unreadable.iter().any(|seen| seen == item)
        {
            return;
        }
        self.unreadable.push(item.to_owned());
    }
}

impl Robots {
    /// The AIPREF preferences this file states about one path.
    ///
    /// A `Content-Usage` line with no path pattern applies to the whole site.
    /// One with a pattern applies where the pattern matches, using the same
    /// matcher `Allow` and `Disallow` use, so `*` and `$` mean what they mean
    /// everywhere else in the file.
    ///
    /// `path` is the path, parameters and query, percent-encoded the way a URL
    /// in flight is. [`path_of`](crate::path_of) produces it.
    #[must_use]
    pub fn usage_for(&self, path: &str) -> Usage {
        let mut out = Usage::default();
        for line in &self.content_usage {
            let (pattern, prefs) = split_pattern(line);
            if let Some(pattern) = pattern
                && !matches_pattern(&escape_pattern(pattern), path)
            {
                continue;
            }
            out.merge(&Usage::parse(prefs));
        }
        out
    }

    /// [`Robots::usage_for`] for a whole URL.
    ///
    /// The empty case is checked first because it is almost every case. AIPREF
    /// is new and hardly any site has a `Content-Usage` line yet, and this runs
    /// once per page at 250 pages a second per server, so the
    /// [`path_of`](crate::path_of) allocation is worth not making when there is
    /// nothing to match it against.
    #[must_use]
    pub fn usage_for_url(&self, url: &str) -> Usage {
        if self.content_usage.is_empty() {
            return Usage::default();
        }
        self.usage_for(&crate::path_of(url))
    }

    /// The preferences that apply to the whole host, which is the ones stated
    /// without a path pattern.
    ///
    /// This is what goes on the host record. A pattern scoped line is about
    /// part of the site and putting it on the host would say the site meant it
    /// everywhere.
    #[must_use]
    pub fn usage(&self) -> Usage {
        let mut out = Usage::default();
        for line in &self.content_usage {
            let (pattern, prefs) = split_pattern(line);
            if pattern.is_none() {
                out.merge(&Usage::parse(prefs));
            }
        }
        out
    }
}

/// Split a robots.txt `Content-Usage` value into its optional path pattern and
/// its preference list.
///
/// The draft's grammar is `[ path-pattern 1*WS ] usage-pref`, and a path
/// pattern is an RFC 9309 pattern, which always starts with `/`. A preference
/// list never can, because it starts with a category name, so the leading
/// slash is the whole discriminator and there is no ambiguity to resolve.
///
/// A value that is only a pattern states no preference about anything, so it
/// goes through whole and ends up recorded verbatim rather than silently
/// becoming a site wide rule.
fn split_pattern(value: &str) -> (Option<&str>, &str) {
    let value = value.trim();
    if !value.starts_with('/') {
        return (None, value);
    }
    match value.find(char::is_whitespace) {
        Some(at) => (Some(&value[..at]), value[at..].trim_start()),
        None => (None, value),
    }
}

/// The more restrictive of two preferences, treating absent as saying nothing.
///
/// The vocab draft section 5.1: any disallowed wins, otherwise any allowed,
/// otherwise nothing was stated. This is the opposite of how `Allow` beats
/// `Disallow` on a tie in RFC 9309, which is worth being deliberate about,
/// because these two rules live in the same file and reading one as the other
/// would publish a preference the site did not express.
const fn stricter(a: Option<Preference>, b: Option<Preference>) -> Option<Preference> {
    match (a, b) {
        (Some(Preference::Disallowed), _) | (_, Some(Preference::Disallowed)) => {
            Some(Preference::Disallowed)
        }
        (Some(Preference::Allowed), _) | (_, Some(Preference::Allowed)) => {
            Some(Preference::Allowed)
        }
        (None, None) => None,
    }
}
