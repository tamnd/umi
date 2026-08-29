//! Doc 13's scope: what a crawl is allowed to fetch.
//!
//! A scope is a declarative document evaluated during admission, before the
//! seen set check. It is pure and it does no I/O, which is what lets a link be
//! tested against it at admission rate without a round trip anywhere.
//!
//! The general crawl is scope id 0 with an empty include list, so there is one
//! code path and not two. Doc 13.2 is explicit about that and it is worth
//! keeping: a focused crawl that ran different code from the general crawl
//! would be a second crawler with the same name, and the bugs the two of them
//! did not share would be the expensive kind.
//!
//! # What is not here
//!
//! The budget is not enforced here and neither is the rate. A scope says what
//! is in and what is out; how long to keep going and how fast to go are the
//! runner's job, because both need a clock and this file does not read one.
//! [`Budget`] and [`RateOverride`] are carried here because they belong in the
//! profile a person writes, not because this module acts on them.

use std::time::Duration;

use serde::Deserialize;
use url::Url;

/// The scope of a crawl.
#[derive(Clone, Debug)]
pub struct Scope {
    /// Doc 10.5's `crawl_profile` column.
    ///
    /// Zero is the general crawl. Anything else is the first four bytes of the
    /// profile digest, so a row can be traced back to the scope that admitted
    /// it without publishing the profile alongside every segment.
    pub id: u32,
    /// The profile digest, for doc 12's manifest.
    pub digest: [u8; 32],
    /// What the operator called it.
    pub name: String,
    /// A URL is in scope if it matches at least one of these. Empty means
    /// everything, which is the general crawl.
    pub include: Vec<Matcher>,
    /// A URL is out of scope if it matches any of these, whatever `include`
    /// says.
    pub exclude: Vec<Matcher>,
    /// Hops from a seed, not path segments.
    pub max_depth: Option<u8>,
    /// What to do with a link that leaves the scope.
    pub link_policy: LinkPolicy,
    /// What to keep once the bytes are here.
    pub content: ContentFilter,
    /// When to stop.
    pub budget: Budget,
    /// How fast, within what doc 07.6 already allows.
    pub rate: RateOverride,
    /// Where the first URLs come from.
    pub seed: Seed,
}

impl Default for Scope {
    fn default() -> Self {
        Self::general()
    }
}

impl Scope {
    /// The general crawl: everything, forever.
    #[must_use]
    pub fn general() -> Self {
        Self {
            id: 0,
            digest: [0u8; 32],
            name: "general".to_owned(),
            include: Vec::new(),
            exclude: Vec::new(),
            max_depth: None,
            link_policy: LinkPolicy::InScopeOnly,
            content: ContentFilter::default(),
            budget: Budget::general(),
            rate: RateOverride::default(),
            seed: Seed::default(),
        }
    }

    /// A scope for one target, which is what `umi crawl example.com` builds.
    ///
    /// The target is read the way a person means it. A bare name is the
    /// registrable domain and everything under it, a name with a `www` or a
    /// `docs` on the front is still the domain because nobody typing
    /// `docs.example.com` wants the crawl to stop at a redirect to
    /// `www.docs.example.com`, and a URL with a path is that path prefix on
    /// that host.
    ///
    /// # Errors
    ///
    /// [`ScopeError::Target`] when the target is neither a URL nor a hostname.
    pub fn for_target(target: &str) -> Result<Self, ScopeError> {
        let matcher = if target.contains("://") {
            let url = Url::parse(target).map_err(|_| ScopeError::Target(target.to_owned()))?;
            let host = url
                .host_str()
                .ok_or_else(|| ScopeError::Target(target.to_owned()))?
                .to_owned();
            if url.path().len() > 1 {
                Matcher::PathPrefix {
                    host,
                    prefix: url.path().to_owned(),
                }
            } else {
                Matcher::HostSuffix(host)
            }
        } else if target.contains('/') {
            let (host, path) = target.split_once('/').unwrap_or((target, ""));
            Matcher::PathPrefix {
                host: host.to_ascii_lowercase(),
                prefix: format!("/{path}"),
            }
        } else if target.contains('.') {
            Matcher::HostSuffix(target.to_ascii_lowercase())
        } else {
            return Err(ScopeError::Target(target.to_owned()));
        };

        let name = target
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .replace('/', "-");
        let mut scope = Self {
            name,
            include: vec![matcher],
            ..Self::general()
        };
        scope.stamp(target.as_bytes());
        Ok(scope)
    }

    /// Add include matchers from doc 14.3's repeatable `--include`.
    ///
    /// Each string is read the same way a target is, so `--include
    /// docs.example.com` and `umi crawl docs.example.com` mean the same
    /// matcher. A second grammar for the flags would be a second thing to get
    /// wrong, and the identifier is restamped afterwards because a scope with
    /// different matchers is a different scope and doc 10.5's `crawl_profile`
    /// column has to say so.
    ///
    /// # Errors
    ///
    /// [`ScopeError::Target`] for a string that names nothing.
    pub fn add_include(&mut self, targets: &[String]) -> Result<(), ScopeError> {
        self.extend(targets, true)
    }

    /// Add exclude matchers, as [`add_include`](Self::add_include).
    ///
    /// # Errors
    ///
    /// As [`add_include`](Self::add_include).
    pub fn add_exclude(&mut self, targets: &[String]) -> Result<(), ScopeError> {
        self.extend(targets, false)
    }

    fn extend(&mut self, targets: &[String], include: bool) -> Result<(), ScopeError> {
        if targets.is_empty() {
            return Ok(());
        }
        for target in targets {
            let parsed = Self::for_target(target)?;
            let list = if include {
                &mut self.include
            } else {
                &mut self.exclude
            };
            list.extend(parsed.include);
        }
        let source = format!("{}|{}", self.name, targets.join("|"));
        self.stamp(source.as_bytes());
        Ok(())
    }

    /// Read doc 13.4's profile.
    ///
    /// # Errors
    ///
    /// [`ScopeError::Toml`] when the file does not parse, [`ScopeError::Regex`]
    /// when a `url_regex` does not compile, and [`ScopeError::Duration`] when a
    /// duration is not a number followed by s, m, h or d.
    pub fn from_toml(text: &str) -> Result<Self, ScopeError> {
        let file: ProfileFile = toml::from_str(text)?;
        let mut scope = Self {
            id: 0,
            digest: [0u8; 32],
            name: file.name,
            include: matchers(file.include)?,
            exclude: matchers(file.exclude)?,
            max_depth: file.max_depth,
            link_policy: file.link_policy,
            content: file.content,
            budget: Budget {
                max_pages: file.budget.max_pages,
                max_bytes: file.budget.max_bytes,
                max_duration: file
                    .budget
                    .max_duration
                    .as_deref()
                    .map(parse_duration)
                    .transpose()?,
                stop_when_idle: file.budget.stop_when_idle,
            },
            rate: file.rate,
            seed: file.seed,
        };
        scope.stamp(text.as_bytes());
        Ok(scope)
    }

    /// Give this scope its identity, which is the digest of what it came from.
    ///
    /// Zero is the general crawl and cannot be minted, so a profile whose
    /// digest starts with four zero bytes gets id 1 instead. That collision has
    /// a probability of one in four billion and a cost of two profiles sharing
    /// a column value, which is why it is worth two lines rather than a wider
    /// column.
    fn stamp(&mut self, source: &[u8]) {
        self.digest = *blake3::hash(source).as_bytes();
        let id = u32::from_le_bytes([
            self.digest[0],
            self.digest[1],
            self.digest[2],
            self.digest[3],
        ]);
        self.id = if id == 0 { 1 } else { id };
    }

    /// Whether this crawl may fetch `url`.
    #[must_use]
    pub fn allows(&self, url: &str) -> bool {
        Url::parse(url).is_ok_and(|parsed| self.allows_url(&parsed))
    }

    /// Whether this crawl may fetch a URL that is already parsed.
    ///
    /// Take this one when the caller has a [`Url`] in hand. Admission tests
    /// every candidate on every page and reparsing a URL that was parsed a
    /// moment ago is the kind of cost that only shows up at 12500 candidates a
    /// second.
    #[must_use]
    pub fn allows_url(&self, url: &Url) -> bool {
        let included = self.include.is_empty() || self.include.iter().any(|m| m.matches(url));
        included && !self.exclude.iter().any(|m| m.matches(url))
    }

    /// Whether the scope names anything at all, which is how the runner knows
    /// it is in focused mode.
    #[must_use]
    pub fn is_general(&self) -> bool {
        self.include.is_empty()
    }

    /// Whether [`allows`](Self::allows) can ever say no.
    ///
    /// The general crawl has nothing to test, and testing it anyway would mean
    /// parsing every link on every page to reach a conclusion that was already
    /// known. The caller checks this before the parse rather than after.
    #[must_use]
    pub fn filters_links(&self) -> bool {
        !self.include.is_empty() || !self.exclude.is_empty()
    }
}

/// One rule about URLs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Matcher {
    /// The registrable domain, and everything under it.
    Pld(String),
    /// Exactly this host.
    Host(String),
    /// This host and anything under it.
    HostSuffix(String),
    /// This path prefix on this host.
    PathPrefix {
        /// The host, exactly.
        host: String,
        /// The prefix, including its leading slash.
        prefix: String,
    },
    /// A regular expression over the whole URL.
    ///
    /// RE2 semantics through the `regex` crate, chosen in doc 13.2 because
    /// there is no backtracking in it and therefore no way for a profile to be
    /// a denial of service against admission.
    UrlRegex(regex::Regex),
}

impl Matcher {
    /// Whether `url` matches.
    #[must_use]
    pub fn matches(&self, url: &Url) -> bool {
        let host = url.host_str().unwrap_or_default();
        match self {
            Self::Pld(pld) => umi_types::pay_level_domain(host).eq_ignore_ascii_case(pld),
            Self::Host(want) => host.eq_ignore_ascii_case(want),
            Self::HostSuffix(suffix) => {
                host.eq_ignore_ascii_case(suffix)
                    || host.len().checked_sub(suffix.len() + 1).is_some_and(|at| {
                        host.as_bytes()[at] == b'.' && host[at + 1..].eq_ignore_ascii_case(suffix)
                    })
            }
            Self::PathPrefix { host: want, prefix } => {
                host.eq_ignore_ascii_case(want) && url.path().starts_with(prefix.as_str())
            }
            Self::UrlRegex(re) => re.is_match(url.as_str()),
        }
    }
}

/// What to do with a link that leaves the scope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LinkPolicy {
    /// Drop it. The default, and the only one that keeps a crawl inside what
    /// the operator asked for.
    #[default]
    InScopeOnly,
    /// Keep it in doc 10.5's `links` column and never enqueue it.
    ///
    /// The link is in the row either way, so this differs from the default
    /// only in that it is a promise: a reader of the corpus can tell that the
    /// out of scope links were recorded deliberately rather than by accident.
    RecordOutOfScope,
    /// Fetch it, but do not follow its links.
    ///
    /// This is what somebody crawling a documentation site means when they also
    /// want the pages it cites. It terminates because the second hop is not
    /// taken.
    OneHop,
}

/// What to keep once the bytes have arrived.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContentFilter {
    /// Media types to keep. Empty means any.
    pub content_types: Vec<String>,
    /// Language tags to keep, applied after extraction. Empty means any.
    pub languages: Vec<String>,
    /// The largest body this crawl wants.
    pub max_bytes: u32,
}

impl Default for ContentFilter {
    fn default() -> Self {
        Self {
            content_types: Vec::new(),
            languages: Vec::new(),
            // Doc 05.4's ceiling. A filter that defaulted to zero would reject
            // everything, and a filter that defaulted to u32::MAX would let a
            // profile quietly raise the fetcher's own limit.
            max_bytes: 8 << 20,
        }
    }
}

impl ContentFilter {
    /// Whether a response is worth extracting, from its headers and its size.
    ///
    /// Two methods rather than one because the two halves of the filter can be
    /// answered at different moments. The type and the size are known when the
    /// last byte lands, the language is not known until the document has been
    /// parsed, and asking for all three at once would mean either extracting a
    /// page this crawl does not want or guessing a language from bytes nobody
    /// has looked at yet.
    ///
    /// The content type is compared on the part before the semicolon, because
    /// `text/html; charset=utf-8` and `text/html` are the same type and a
    /// profile that had to list both would be a profile with a bug in it.
    #[must_use]
    pub fn accepts_response(&self, content_type: Option<&str>, bytes: u32) -> bool {
        if bytes > self.max_bytes {
            return false;
        }
        if self.content_types.is_empty() {
            return true;
        }
        let media = content_type
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim();
        self.content_types
            .iter()
            .any(|want| media.eq_ignore_ascii_case(want))
    }

    /// Whether an extracted page is in a language this crawl wants.
    ///
    /// A page with no language is kept. Doc 11.6's detector is not built yet,
    /// so an unknown language here mostly means the page did not declare one,
    /// and throwing those away would throw away most of the web.
    ///
    /// Matching is on the primary subtag, so `en` in a profile keeps `en-GB`.
    /// A profile that writes `en-GB` gets only `en-GB`, because somebody who
    /// went to the trouble of writing the region meant it.
    #[must_use]
    pub fn accepts_lang(&self, lang: Option<&str>) -> bool {
        if self.languages.is_empty() {
            return true;
        }
        let Some(lang) = lang else { return true };
        self.languages.iter().any(|want| {
            lang.eq_ignore_ascii_case(want)
                || (!want.contains('-')
                    && lang
                        .split_once('-')
                        .is_some_and(|(primary, _)| primary.eq_ignore_ascii_case(want)))
        })
    }
}

/// When to stop.
///
/// Not deserialised directly, because `max_duration` is written as `6h` and a
/// [`Duration`] is not. There is a private written form next to the rest of the
/// profile types, and this is the parsed one the runner reads.
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    /// Stop after this many pages.
    pub max_pages: Option<u64>,
    /// Stop after this many bytes fetched.
    pub max_bytes: Option<u64>,
    /// Stop after this long.
    pub max_duration: Option<Duration>,
    /// Stop when the frontier drains, which is the focused default.
    pub stop_when_idle: bool,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_pages: None,
            max_bytes: None,
            max_duration: None,
            stop_when_idle: true,
        }
    }
}

impl Budget {
    /// The general crawl's budget, which is no budget and never stops.
    #[must_use]
    pub const fn general() -> Self {
        Self {
            max_pages: None,
            max_bytes: None,
            max_duration: None,
            stop_when_idle: false,
        }
    }
}

/// How fast, within what doc 07.6 already allows.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateOverride {
    /// Requests per second per host.
    pub max_rps_per_host: f32,
    /// Fetches in flight.
    pub concurrency: u16,
}

impl Default for RateOverride {
    fn default() -> Self {
        Self {
            max_rps_per_host: 1.0,
            concurrency: 4,
        }
    }
}

impl RateOverride {
    /// Doc 13.3's ceiling for a focused crawl.
    ///
    /// A focused crawl is the only thing a site sees from us, where a general
    /// crawl at 250 pages a second spread over millions of hosts is invisible
    /// to any one of them. So the ceiling is lower here than doc 07.6's, and
    /// it is enforced rather than trusted from the profile.
    pub const MAX_RPS_PER_HOST: f32 = 2.0;

    /// The rate this override actually gets, which is never above the ceiling
    /// and never below a stop.
    #[must_use]
    pub fn clamped(&self) -> f32 {
        self.max_rps_per_host.clamp(0.0, Self::MAX_RPS_PER_HOST)
    }
}

/// Where the first URLs come from.
///
/// The two sitemap keys are options rather than plain booleans because the
/// pass is on by default. A profile that does not mention sitemaps at all and
/// a profile that says `sitemaps = false` have to be told apart, and a bare
/// `bool` reads both of them as false, which would turn the pass off for every
/// profile ever written.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Seed {
    /// Follow `/sitemap.xml`. Unset means the default, which is yes.
    pub sitemaps: Option<bool>,
    /// Follow the `Sitemap` lines in robots.txt. Unset means the default,
    /// which is yes.
    pub robots_sitemaps: Option<bool>,
    /// URLs written out in the profile.
    pub urls: Vec<String>,
}

/// What went wrong reading a scope.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScopeError {
    /// The profile is not valid TOML or is missing something.
    #[error("profile: {0}")]
    Toml(#[from] toml::de::Error),
    /// A `url_regex` did not compile.
    #[error("url_regex: {0}")]
    Regex(#[from] regex::Error),
    /// A duration was not a number followed by s, m, h or d.
    #[error("not a duration: {0}")]
    Duration(String),
    /// The target of `umi crawl` was neither a URL nor a hostname.
    #[error("not a domain, host or url: {0}")]
    Target(String),
}

/// Doc 13.4's profile, exactly as it is written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    name: String,
    #[serde(default)]
    include: Vec<MatcherFile>,
    #[serde(default)]
    exclude: Vec<MatcherFile>,
    #[serde(default)]
    max_depth: Option<u8>,
    #[serde(default)]
    link_policy: LinkPolicy,
    #[serde(default)]
    content: ContentFilter,
    #[serde(default)]
    budget: BudgetFile,
    #[serde(default)]
    rate: RateOverride,
    #[serde(default)]
    seed: Seed,
}

/// The budget as written, where the duration is still a string.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct BudgetFile {
    max_pages: Option<u64>,
    max_bytes: Option<u64>,
    max_duration: Option<String>,
    stop_when_idle: bool,
}

impl Default for BudgetFile {
    /// Written out rather than derived, because `stop_when_idle` defaults to
    /// true and `bool` defaults to false. A per field `default` attribute would
    /// not help: a container level `#[serde(default)]` fills every missing
    /// field from this one impl, so the derived version would silently turn a
    /// profile with no budget table into a crawl that never stops.
    fn default() -> Self {
        Self {
            max_pages: None,
            max_bytes: None,
            max_duration: None,
            stop_when_idle: true,
        }
    }
}

/// A matcher as written, which is a table with one key in it.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum MatcherFile {
    Pld(String),
    Host(String),
    HostSuffix(String),
    PathPrefix { host: String, prefix: String },
    UrlRegex(String),
}

fn matchers(file: Vec<MatcherFile>) -> Result<Vec<Matcher>, ScopeError> {
    file.into_iter()
        .map(|m| {
            Ok(match m {
                MatcherFile::Pld(pld) => Matcher::Pld(pld.to_ascii_lowercase()),
                MatcherFile::Host(host) => Matcher::Host(host.to_ascii_lowercase()),
                MatcherFile::HostSuffix(host) => Matcher::HostSuffix(host.to_ascii_lowercase()),
                MatcherFile::PathPrefix { host, prefix } => Matcher::PathPrefix {
                    host: host.to_ascii_lowercase(),
                    prefix,
                },
                // Anchored at the start, which doc 13.2 asks for. Wrapping is
                // how that is done rather than trusting the author to write a
                // caret, and a pattern that already has one still works.
                MatcherFile::UrlRegex(pattern) => {
                    Matcher::UrlRegex(regex::Regex::new(&format!("^(?:{pattern})"))?)
                }
            })
        })
        .collect()
}

/// A number followed by s, m, h or d.
///
/// Not a general duration parser. Doc 13.4's example is `6h` and doc 14.3's is
/// `--for 30m`, and a profile that wants a duration nobody can read at a glance
/// is a profile with a different problem.
///
/// Public because `--for` on the command line takes the same spelling as
/// `max_duration` in the profile, and two parsers that were supposed to agree
/// would eventually not.
///
/// # Errors
///
/// [`ScopeError::Duration`] for anything that is not a number and one of those
/// four letters.
pub fn parse_duration(text: &str) -> Result<Duration, ScopeError> {
    let text = text.trim();
    let (number, unit) = text.split_at(text.len().saturating_sub(1));
    let value: u64 = number
        .parse()
        .map_err(|_| ScopeError::Duration(text.to_owned()))?;
    let seconds = match unit {
        "s" => value,
        "m" => value * 60,
        "h" => value * 3600,
        "d" => value * 86400,
        _ => return Err(ScopeError::Duration(text.to_owned())),
    };
    Ok(Duration::from_secs(seconds))
}
