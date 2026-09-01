//! Where a segment goes, from `docs/spec/12-publishing.md` section 12.4.
//!
//! Doc 12.4 says the naming scheme has to be mechanical rather than curated,
//! and the reason is the count: about 2000 repositories over the 4.2 years doc
//! 01 computes for 100 billion pages. Nobody is going to name those by hand, so
//! every name in this module is a pure function of a timestamp and a slice
//! number, and the functions are here rather than inlined at the call site so
//! that the publisher and the reconciler cannot disagree about where a file
//! lives.
//!
//! Nothing here reads a clock either. A path is derived from the segment's own
//! `fetched_at` range, not from when the publisher happened to run, because
//! otherwise a segment republished after a crash would land in a different day
//! folder from the one the manifest already claims.

use core::fmt;

use umi_types::Ulid;

/// The organisation every published repository lives under.
pub const ORG: &str = "open-index";

/// Doc 12.4's registry and entry point, which is the one repository we rewrite.
pub const META_REPO: &str = "open-index/umi-meta";

/// Doc 12.4's robots corpus, which is one repository rather than one per week
/// because doc 07.4 sizes the whole thing at a few hundred compressed bytes per
/// host per snapshot.
pub const ROBOTS_REPO: &str = "open-index/umi-robots";

/// Which family of repository a segment belongs to.
///
/// This is [`umi_file::StreamKind`] seen from the publishing side. They are
/// deliberately not the same type: a stream is what a segment holds and a
/// family is where it is published, and doc 12.4 maps robots to a single
/// repository while pages, receipts and the frontier get one per week and
/// slice.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Family {
    /// `umi-pages-<YYYY>w<WW>-<NN>`, the corpus.
    Pages,
    /// `umi-receipts-<YYYY>w<WW>-<NN>`, doc 04's audit trail.
    Receipts,
    /// `umi-robots`, doc 07.4's longitudinal corpus.
    Robots,
    /// `umi-frontier-<YYYY>w<WW>-<NN>`, doc 08.6's spilled backlog.
    ///
    /// Sliced like pages and receipts rather than held in one repository like
    /// robots, because the backlog is the biggest thing the project has: 100
    /// billion known URLs is terabytes even after the local ledger's columns
    /// have been dropped, and no single repository is going to hold it. Nothing
    /// looks a spill up by repository name, so slicing costs nothing. A
    /// coordinator finds a domain's rows through the per domain pointer doc
    /// 08.6 keeps locally, and everyone else reads the manifest.
    Frontier,
}

impl Family {
    /// The family a segment of this stream publishes into.
    #[must_use]
    pub const fn of(stream: umi_file::StreamKind) -> Self {
        match stream {
            umi_file::StreamKind::Pages => Self::Pages,
            umi_file::StreamKind::Receipts => Self::Receipts,
            umi_file::StreamKind::Robots => Self::Robots,
            umi_file::StreamKind::Frontier => Self::Frontier,
        }
    }

    /// The stream a repository of this family holds.
    ///
    /// The inverse of [`Family::of`], and total in both directions because the
    /// two enums have the same members for the same reason. The card
    /// generator wants it: it knows which repository it is writing a README for
    /// and needs the schema that repository's files carry.
    #[must_use]
    pub const fn stream(self) -> umi_file::StreamKind {
        match self {
            Self::Pages => umi_file::StreamKind::Pages,
            Self::Receipts => umi_file::StreamKind::Receipts,
            Self::Robots => umi_file::StreamKind::Robots,
            Self::Frontier => umi_file::StreamKind::Frontier,
        }
    }

    /// The name stem, without the week or the slice.
    #[must_use]
    pub const fn stem(self) -> &'static str {
        match self {
            Self::Pages => "umi-pages",
            Self::Receipts => "umi-receipts",
            Self::Robots => "umi-robots",
            Self::Frontier => "umi-frontier",
        }
    }

    /// Whether this family splits by week and slice at all.
    #[must_use]
    pub const fn is_sliced(self) -> bool {
        !matches!(self, Self::Robots)
    }
}

/// An ISO 8601 week: the year the week belongs to, which is not always the year
/// the day falls in, and the week number.
///
/// Doc 12.4 names repositories after the ISO week and that is worth taking
/// literally rather than approximately. The last days of December belong to
/// week 1 of the following year when the week has more days in January, and a
/// publisher that used the calendar year would put those files in a repository
/// whose name says a different week from the one the manifest says.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct IsoWeek {
    /// The ISO week numbering year.
    pub year: i32,
    /// The week within it, 1 through 53.
    pub week: u32,
}

impl fmt::Display for IsoWeek {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}w{:02}", self.year, self.week)
    }
}

/// A civil date, which is all the calendar this crate needs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Date {
    /// Proleptic Gregorian year.
    pub year: i32,
    /// Month, 1 through 12.
    pub month: u32,
    /// Day of month, 1 through 31.
    pub day: u32,
}

impl Date {
    /// The UTC date a millisecond timestamp falls on.
    ///
    /// UTC and not local time, because a fleet spread over three hosts in two
    /// timezones that agreed on local time would put the same segment in two
    /// day folders depending on which box published it.
    #[must_use]
    pub const fn from_ms(ms: u64) -> Self {
        civil_from_days((ms / 86_400_000) as i64)
    }

    /// The `YYYYMMDD` folder name from doc 12.4.
    #[must_use]
    pub fn folder(self) -> String {
        format!("{:04}{:02}{:02}", self.year, self.month, self.day)
    }

    /// Days since the Unix epoch.
    #[must_use]
    pub const fn to_days(self) -> i64 {
        days_from_civil(self.year, self.month, self.day)
    }

    /// The ISO week this date belongs to.
    #[must_use]
    pub const fn iso_week(self) -> IsoWeek {
        // The standard trick: the ISO year of a date is the calendar year of
        // the Thursday of its week, because a week belongs to whichever year
        // holds four or more of its days and Thursday is the middle day.
        let days = self.to_days();
        let dow = iso_weekday(days);
        let thursday = days + (4 - dow as i64);
        let year = civil_from_days(thursday).year;
        // Week 1 is the week holding the 4th of January, by definition.
        let jan4 = days_from_civil(year, 1, 4);
        let week1_monday = jan4 - (iso_weekday(jan4) as i64 - 1);
        let week = ((thursday - week1_monday) / 7 + 1) as u32;
        IsoWeek { year, week }
    }
}

/// Monday is 1 and Sunday is 7, which is what ISO 8601 says.
const fn iso_weekday(days_since_epoch: i64) -> u32 {
    // 1970-01-01 was a Thursday, so day 0 is weekday 4. `rem_euclid` rather
    // than `%` because the epoch is not the earliest date this can be handed.
    ((days_since_epoch + 3).rem_euclid(7) + 1) as u32
}

// The two calendar conversions below are Howard Hinnant's `days_from_civil` and
// `civil_from_days`, which are exact over the whole proleptic Gregorian range
// and are about fifteen lines each. Pulling in a date library for two functions
// that never change and never need a timezone would be the wrong trade, and
// this crate publishes files whose names must not shift under a dependency
// bump.

const fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year } as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

const fn civil_from_days(days: i64) -> Date {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    Date {
        year: (if month <= 2 { y + 1 } else { y }) as i32,
        month,
        day,
    }
}

/// Everything needed to say where one segment's Parquet file goes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Location {
    /// `open-index/umi-pages-2026w34-03`, or whichever family and week.
    pub repo: String,
    /// The `YYYYMMDD` day folder.
    pub day: String,
    /// The repository relative path of the Parquet file.
    pub path: String,
}

impl Location {
    /// The repository relative path of the day's manifest.
    #[must_use]
    pub fn manifest_path(&self) -> String {
        format!("_manifest/{}.json", self.day)
    }

    /// The repository relative path of the day's detached signature.
    #[must_use]
    pub fn signature_path(&self) -> String {
        format!("_manifest/{}.json.sig", self.day)
    }
}

/// Work out where a segment publishes to.
///
/// `slice` is doc 12.4's `NN`, allocated on demand starting at zero as each
/// repository approaches the 300 GB soft ceiling. It is an argument rather than
/// something computed here because the allocation needs the current byte counts
/// from `umi-meta`, which is I/O, and this module is deliberately pure.
///
/// `first_ms` is the segment's earliest `fetched_at_ms` and not its seal time.
/// A segment that fills across midnight lands in the day it started, so that
/// the day folder a manifest names never depends on how long the fill took.
#[must_use]
pub fn locate(family: Family, first_ms: u64, slice: u16, segment: Ulid) -> Location {
    locate_in(ORG, family, first_ms, slice, segment)
}

/// [`locate`], for a deployment that is not `open-index`.
///
/// Doc 12.4 fixes the organisation for the corpus this project publishes, and
/// [`ORG`] is that answer. The organisation is still an argument here because
/// doc 14.7 lets an operator set `publish.org`, and a configuration setting
/// that quietly did nothing would be worse than not having one. Anyone running
/// their own hub gets the same layout under their own name.
#[must_use]
pub fn locate_in(org: &str, family: Family, first_ms: u64, slice: u16, segment: Ulid) -> Location {
    Corpus::new(org).locate(family, first_ms, slice, segment)
}

/// Which corpus a publisher is writing into.
///
/// Two things vary and neither of them is doc 12.4's layout. The organisation
/// varies because doc 14.7 lets an operator set `publish.org`, and the focused
/// crawl name varies because doc 13.8 sends a focused crawl somewhere else
/// entirely. Everything below the repository name is the same in all cases,
/// which is why this is one struct with one method rather than two families of
/// function.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Corpus {
    /// The Hugging Face organisation. [`ORG`] unless configured otherwise.
    pub org: String,
    /// The focused crawl's name, or `None` for the general crawl.
    pub focus: Option<String>,
}

impl Corpus {
    /// The general crawl under an organisation.
    #[must_use]
    pub fn new(org: &str) -> Self {
        Self {
            org: org.to_owned(),
            focus: None,
        }
    }

    /// A focused crawl's corpus, named after its scope.
    ///
    /// The name is put through [`slug`] here rather than at the call site, so
    /// that two callers holding the same scope cannot disagree about which
    /// repository it publishes to.
    #[must_use]
    pub fn focused(org: &str, name: &str) -> Self {
        Self {
            org: org.to_owned(),
            focus: Some(slug(name)),
        }
    }

    /// Where a segment of this family goes.
    ///
    /// Only pages move when the crawl is focused. Doc 13.8's rule is about the
    /// corpus being an unbiased sample and a focused crawl not being one, and
    /// that argument is about pages: receipts are doc 04's audit trail and
    /// robots is doc 07.4's longitudinal record, neither of which anyone
    /// computes a corpus statistic over. Keeping them where they are also keeps
    /// one schema per repository, which is what stops a reader that opens
    /// `data/` from finding two of them.
    #[must_use]
    pub fn locate(&self, family: Family, first_ms: u64, slice: u16, segment: Ulid) -> Location {
        let date = Date::from_ms(first_ms);
        let org = &self.org;
        let repo = match (&self.focus, family) {
            (Some(name), Family::Pages) => format!("{org}/umi-focus-{name}"),
            _ if family.is_sliced() => {
                format!("{org}/{}-{}-{slice:02}", family.stem(), date.iso_week())
            }
            _ => format!("{org}/{}", family.stem()),
        };
        let day = date.folder();
        Location {
            path: format!("data/{day}/{segment}.parquet"),
            repo,
            day,
        }
    }
}

/// A scope name as a repository name can hold it.
///
/// Hugging Face takes letters, digits, dots, dashes and underscores, up to 96
/// characters, and a scope name is whatever the operator typed at `umi crawl`.
/// Everything outside that set becomes a dash, runs of dashes collapse so that
/// a URL target does not turn into a row of them, and the result is lowercased
/// because a repository name that differs from another only in case is a
/// repository nobody can talk about out loud.
///
/// The 64 character cap leaves room for the `umi-focus-` prefix inside the
/// hub's limit. A name that hits it is truncated rather than hashed, because a
/// truncated name is still recognisable and a collision between two focused
/// crawls whose names agree for 64 characters is the operator's to notice.
#[must_use]
pub fn slug(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '.' || c == '_' {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    let trimmed = out.trim_matches(['-', '.']);
    let capped = match trimmed.char_indices().nth(64) {
        Some((at, _)) => &trimmed[..at],
        None => trimmed,
    };
    if capped.is_empty() {
        "crawl".to_owned()
    } else {
        capped.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{Date, Family, IsoWeek, locate};
    use umi_types::Ulid;

    fn ms(year: i32, month: u32, day: u32) -> u64 {
        (super::days_from_civil(year, month, day) * 86_400_000) as u64
    }

    #[test]
    fn the_calendar_round_trips_over_a_few_centuries() {
        // Every day from 1900 to 2100, which is 73049 of them and takes about a
        // millisecond. Worth doing exhaustively rather than on samples, because
        // the failure mode of a leap year bug is one wrong day folder a year.
        let mut days = super::days_from_civil(1900, 1, 1);
        let end = super::days_from_civil(2100, 1, 1);
        while days < end {
            let date = super::civil_from_days(days);
            assert_eq!(date.to_days(), days, "{date:?}");
            days += 1;
        }
    }

    #[test]
    fn the_iso_week_edge_cases_are_the_ones_the_standard_names() {
        // These are the worked examples in ISO 8601 itself, which is the only
        // reason to trust the implementation over a plausible looking one.
        let cases = [
            // 2026-08-17 is a Monday in week 34, the example doc 12.4 uses.
            ((2026, 8, 17), (2026, 34)),
            // A January date that belongs to the previous year's last week.
            ((2027, 1, 1), (2026, 53)),
            ((2021, 1, 1), (2020, 53)),
            // A December date that belongs to the next year's first week.
            ((2019, 12, 30), (2020, 1)),
            ((2024, 12, 30), (2025, 1)),
            // A year that genuinely has 53 weeks.
            ((2020, 12, 31), (2020, 53)),
            // The first day of a year that starts on a Thursday is week 1.
            ((2015, 1, 1), (2015, 1)),
            // And one that starts on a Friday is not.
            ((2016, 1, 1), (2015, 53)),
        ];
        for ((y, m, d), (year, week)) in cases {
            let date = Date {
                year: y,
                month: m,
                day: d,
            };
            assert_eq!(date.iso_week(), IsoWeek { year, week }, "{date:?}");
        }
    }

    #[test]
    fn a_segment_lands_where_doc_12_4_says() {
        let ulid = Ulid::parse("01K2M8Q0P7R3XN500000000000").expect("parse");
        let at = locate(Family::Pages, ms(2026, 8, 17) + 3_600_000, 3, ulid);
        assert_eq!(at.repo, "open-index/umi-pages-2026w34-03");
        assert_eq!(at.day, "20260817");
        assert_eq!(at.path, format!("data/20260817/{ulid}.parquet"));
        assert_eq!(at.manifest_path(), "_manifest/20260817.json");
        assert_eq!(at.signature_path(), "_manifest/20260817.json.sig");
    }

    #[test]
    fn robots_is_one_repository_rather_than_one_a_week() {
        let ulid = Ulid::new(ms(2026, 8, 17), [1; 10]);
        let a = locate(Family::Robots, ms(2026, 8, 17), 0, ulid);
        let b = locate(Family::Robots, ms(2027, 3, 2), 0, ulid);
        assert_eq!(a.repo, super::ROBOTS_REPO);
        assert_eq!(a.repo, b.repo);
        assert_ne!(a.day, b.day);
    }

    #[test]
    fn a_segment_that_filled_across_midnight_lands_in_the_day_it_started() {
        // The argument is the earliest fetch, so this is really a statement
        // about the call site, and the test exists to keep it stated.
        let ulid = Ulid::new(0, [0; 10]);
        let late = ms(2026, 8, 17) + 86_400_000 - 1_000;
        assert_eq!(locate(Family::Pages, late, 0, ulid).day, "20260817");
    }

    #[test]
    fn a_focused_crawl_never_lands_in_the_general_corpus() {
        // Doc 13.8. The general corpus is supposed to be an unbiased sample of
        // the web and a focused crawl is by definition not one, so mixing them
        // poisons every statistic anyone computes over the corpus. Nothing
        // else moves: receipts and robots are not the corpus.
        let ulid = Ulid::new(ms(2026, 8, 17), [2; 10]);
        let focus = super::Corpus::focused("open-index", "blog.rust-lang.org");
        let pages = focus.locate(Family::Pages, ms(2026, 8, 17), 0, ulid);
        assert_eq!(pages.repo, "open-index/umi-focus-blog.rust-lang.org");
        assert_eq!(pages.day, "20260817", "the day layout is unchanged");
        assert_eq!(
            focus
                .locate(Family::Receipts, ms(2026, 8, 17), 3, ulid)
                .repo,
            "open-index/umi-receipts-2026w34-03",
            "doc 04's audit trail is not the corpus and does not move"
        );
        assert_eq!(
            focus.locate(Family::Robots, ms(2026, 8, 17), 0, ulid).repo,
            super::ROBOTS_REPO
        );

        let general = super::Corpus::new("open-index");
        assert_eq!(
            general.locate(Family::Pages, ms(2026, 8, 17), 3, ulid).repo,
            "open-index/umi-pages-2026w34-03",
            "and without a focus nothing changed at all"
        );
    }

    #[test]
    fn a_focus_name_survives_being_a_repository_name() {
        let cases = [
            ("blog.rust-lang.org", "blog.rust-lang.org"),
            ("https://example.com/docs/", "https-example.com-docs"),
            ("Rust Docs", "rust-docs"),
            ("../../etc", "etc"),
            ("...", "crawl"),
            ("", "crawl"),
        ];
        for (name, want) in cases {
            assert_eq!(super::slug(name), want, "{name}");
        }
        let long = super::slug(&"a".repeat(200));
        assert_eq!(long.len(), 64, "inside the hub's limit with the prefix on");
    }

    #[test]
    fn the_family_of_every_stream_is_named() {
        for stream in umi_file::sample::EVERY_STREAM {
            let family = Family::of(stream);
            assert!(!family.stem().is_empty());
        }
        assert!(Family::of(umi_file::StreamKind::Pages).is_sliced());
        assert!(!Family::of(umi_file::StreamKind::Robots).is_sliced());
    }
}
