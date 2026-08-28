//! The two date grammars a sitemap or a feed can carry.
//!
//! A sitemap `lastmod` is a W3C datetime, which is the ISO 8601 profile in
//! <https://www.w3.org/TR/NOTE-datetime>: a year, optionally narrowing to a
//! month, a day, a time, and a time zone offset. An Atom `updated` is RFC 3339,
//! which is the full form of the same thing. An RSS `pubDate` is an RFC 822
//! date as amended by RFC 1123, which looks nothing like either.
//!
//! All three matter for the same reason, and it is doc 09.4's: `lastmod` is a
//! site telling us exactly when a page changed, which is worth more than every
//! signal the change rate estimator can infer on its own. A parser that reads
//! the full timestamp and gives up on `2026-08` throws away the sites that
//! publish a date and no clock, which is a lot of them.
//!
//! What comes out is milliseconds since the epoch, always UTC. A value with an
//! offset is converted, a value with no offset is read as UTC, and a partial
//! value is read as the first instant it could mean, so `2026-08` is
//! `2026-08-01T00:00:00Z`. That last choice is deliberate: `lastmod` is used as
//! "not modified since", and the earliest instant is the one that cannot claim
//! a page is fresher than the site said.

use umi_types::date::epoch_ms;

/// Parse a `lastmod` or an `updated`, in any of the W3C datetime forms.
///
/// Returns `None` for anything that is not one of them, including a date
/// before 1970. Sitemaps are full of `0000-00-00` and of dates a templating
/// bug produced, and none of those is a timestamp.
#[must_use]
pub fn w3c(text: &str) -> Option<u64> {
    let text = text.trim();
    let (date, rest) = match text.find(['T', 't', ' ']) {
        Some(at) => (&text[..at], &text[at + 1..]),
        None => (text, ""),
    };

    let mut parts = date.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u32 = match parts.next() {
        Some(m) => m.parse().ok()?,
        None => 1,
    };
    let day: u32 = match parts.next() {
        Some(d) => d.parse().ok()?,
        None => 1,
    };
    if parts.next().is_some() {
        return None;
    }
    if rest.is_empty() {
        return epoch_ms(year, month, day, 0, 0, 0);
    }

    // The offset, which is the last thing on the line and is either a `Z`, a
    // sign and an offset, or missing. Split it off before the time is read so
    // that the time parser only ever sees digits and colons.
    let (time, offset) = split_offset(rest)?;
    let mut fields = time.split(':');
    let hour: u32 = fields.next()?.parse().ok()?;
    let minute: u32 = match fields.next() {
        Some(m) => m.parse().ok()?,
        None => 0,
    };
    // Fractional seconds are allowed and are below the resolution anything
    // downstream cares about, so they are read and dropped rather than
    // rejected.
    let second: u32 = match fields.next() {
        Some(s) => s.split(['.', ',']).next()?.parse().ok()?,
        None => 0,
    };
    if fields.next().is_some() {
        return None;
    }

    apply_offset(epoch_ms(year, month, day, hour, minute, second)?, offset)
}

/// Parse an RSS `pubDate`, which is RFC 822 as RFC 1123 amended it.
///
/// `Tue, 10 Jun 2003 09:41:01 GMT` is the shape, with the day name optional,
/// the seconds optional, and the zone either a name or a four digit offset.
/// Two digit years are read the way RFC 1123 says, which is the rule that
/// keeps a feed written in 1999 from landing in the year 99.
#[must_use]
pub fn rfc822(text: &str) -> Option<u64> {
    let text = text.trim();
    // The day name is decoration. RFC 822 makes it optional and plenty of
    // feeds get it wrong anyway, so it is dropped rather than checked.
    let text = match text.find(", ") {
        Some(at) => text[at + 2..].trim(),
        None => text,
    };

    let mut fields = text.split_whitespace();
    let day: u32 = fields.next()?.parse().ok()?;
    let month = month_number(fields.next()?)?;
    let year = year_number(fields.next()?)?;
    let time = fields.next().unwrap_or("00:00:00");
    let zone = fields.next().unwrap_or("GMT");

    let mut hms = time.split(':');
    let hour: u32 = hms.next()?.parse().ok()?;
    let minute: u32 = hms.next()?.parse().ok()?;
    let second: u32 = match hms.next() {
        Some(s) => s.parse().ok()?,
        None => 0,
    };

    apply_offset(
        epoch_ms(year, month, day, hour, minute, second)?,
        zone_of(zone)?,
    )
}

/// Either grammar, tried in the order that costs less.
///
/// Feeds mix the two constantly. An Atom feed with an RSS date in it is a
/// broken feed, but it is a broken feed with a real date in it, and a reader
/// that refuses it loses the freshness signal to be right about a format.
#[must_use]
pub fn any(text: &str) -> Option<u64> {
    w3c(text).or_else(|| rfc822(text))
}

/// Split a time from its trailing zone, as minutes east of UTC.
fn split_offset(rest: &str) -> Option<(&str, i32)> {
    let rest = rest.trim();
    if let Some(time) = rest.strip_suffix(['Z', 'z']) {
        return Some((time, 0));
    }
    // From the right, because the time itself has no sign in it and a date
    // this far along cannot either.
    let at = rest.rfind(['+', '-'])?;
    let (time, sign) = rest.split_at(at);
    let minutes = offset_minutes(sign)?;
    Some((time, minutes))
}

/// `+05:30`, `-0800` or `+00` as minutes east of UTC.
fn offset_minutes(text: &str) -> Option<i32> {
    let (sign, digits) = match text.as_bytes().first()? {
        b'+' => (1, &text[1..]),
        b'-' => (-1, &text[1..]),
        _ => return None,
    };
    let digits: String = digits.chars().filter(|c| *c != ':').collect();
    let (hours, minutes) = match digits.len() {
        2 => (digits.parse::<i32>().ok()?, 0),
        4 => (
            digits[..2].parse::<i32>().ok()?,
            digits[2..].parse::<i32>().ok()?,
        ),
        _ => return None,
    };
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(sign * (hours * 60 + minutes))
}

/// An RFC 822 zone, which is either a name from the obsolete list or an
/// offset.
///
/// The single letter military zones are the one place RFC 822 and RFC 1123
/// disagree with each other, and RFC 1123 says to read them as UTC because the
/// original table had the sign backwards and everybody implemented the bug.
/// That is what this does.
fn zone_of(text: &str) -> Option<i32> {
    match text {
        "GMT" | "UT" | "UTC" | "Z" => Some(0),
        "EST" => Some(-5 * 60),
        "EDT" => Some(-4 * 60),
        "CST" => Some(-6 * 60),
        "CDT" => Some(-5 * 60),
        "MST" => Some(-7 * 60),
        "MDT" => Some(-6 * 60),
        "PST" => Some(-8 * 60),
        "PDT" => Some(-7 * 60),
        _ if text.len() == 1 && text.as_bytes()[0].is_ascii_alphabetic() => Some(0),
        _ => offset_minutes(text),
    }
}

/// Shift a UTC timestamp by an offset given in minutes east.
///
/// A value that would land before the epoch is `None` rather than a wrap,
/// because a `u64` of milliseconds has nowhere to put it.
fn apply_offset(ms: u64, minutes_east: i32) -> Option<u64> {
    let shift = i64::from(minutes_east) * 60_000;
    u64::try_from(i64::try_from(ms).ok()? - shift).ok()
}

fn month_number(name: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let name = name.to_ascii_lowercase();
    let head = name.get(..3)?;
    MONTHS
        .iter()
        .position(|m| *m == head)
        .map(|i| u32::try_from(i).unwrap_or(0) + 1)
}

/// RFC 1123's two digit year rule, plus the three digit years a handful of
/// broken generators emit.
fn year_number(text: &str) -> Option<i32> {
    let value: i32 = text.parse().ok()?;
    match text.len() {
        // 00 through 68 is 2000s, 69 through 99 is 1900s, which is what every
        // other reader does and what RFC 2822 wrote down.
        2 if value < 69 => Some(2000 + value),
        2 => Some(1900 + value),
        3 => Some(1900 + value),
        4 => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-08-28T00:00:00Z, which is what most of the cases below are written
    /// against so that a wrong answer is obvious rather than arithmetic.
    const DAY: u64 = 1_787_875_200_000;

    #[test]
    fn the_full_form_is_the_easy_one() {
        assert_eq!(w3c("2026-08-28T00:00:00Z"), Some(DAY));
        assert_eq!(w3c("2026-08-28T12:30:45Z"), Some(DAY + 45_045_000));
    }

    #[test]
    fn a_partial_date_is_the_first_instant_it_could_mean() {
        assert_eq!(w3c("2026-08-28"), Some(DAY));
        assert_eq!(w3c("2026-08"), w3c("2026-08-01"));
        assert_eq!(w3c("2026"), w3c("2026-01-01"));
    }

    #[test]
    fn an_offset_is_applied_rather_than_ignored() {
        // Same instant, written three ways, which is the whole point of
        // carrying the offset through instead of taking the wall clock.
        assert_eq!(w3c("2026-08-28T00:00:00+00:00"), Some(DAY));
        assert_eq!(w3c("2026-08-27T19:00:00-05:00"), Some(DAY));
        assert_eq!(w3c("2026-08-28T05:30:00+05:30"), Some(DAY));
        assert_eq!(w3c("2026-08-27T16:00:00-0800"), Some(DAY));
    }

    #[test]
    fn the_pieces_that_are_allowed_to_be_sloppy_are() {
        // A lowercase separator, a space instead of a `T`, fractional seconds
        // and no seconds at all are all things real sitemaps write.
        assert_eq!(w3c("2026-08-28t00:00:00z"), Some(DAY));
        assert_eq!(w3c("2026-08-28 00:00:00Z"), Some(DAY));
        assert_eq!(w3c("2026-08-28T00:00:00.123Z"), Some(DAY));
        assert_eq!(w3c("  2026-08-28T00:00Z  "), Some(DAY));
    }

    #[test]
    fn a_date_that_is_not_a_date_is_none() {
        assert_eq!(w3c(""), None);
        assert_eq!(w3c("0000-00-00"), None);
        assert_eq!(w3c("2026-13-01"), None);
        assert_eq!(w3c("last tuesday"), None);
        assert_eq!(w3c("1969-12-31T23:59:59Z"), None);
        assert_eq!(w3c("2026-08-28T25:00:00Z"), None);
    }

    #[test]
    fn a_day_that_overshoots_the_month_rolls_forward() {
        // 30 February is a day the arithmetic produces rather than refuses,
        // because the field checks are per field and 30 is a legal day number.
        // It lands on 2 March, which is what every other lenient reader does
        // and is not worth a calendar table to get right, since a sitemap that
        // says 30 February has a broken clock either way.
        assert_eq!(w3c("2026-02-30T00:00:00Z"), w3c("2026-03-02T00:00:00Z"));
    }

    #[test]
    fn the_rss_grammar_reads() {
        assert_eq!(rfc822("Fri, 28 Aug 2026 00:00:00 GMT"), Some(DAY));
        assert_eq!(rfc822("28 Aug 2026 00:00:00 GMT"), Some(DAY));
        assert_eq!(rfc822("Fri, 28 Aug 2026 00:00 GMT"), Some(DAY));
        assert_eq!(rfc822("Thu, 27 Aug 2026 19:00:00 EST"), Some(DAY));
        assert_eq!(rfc822("Thu, 27 Aug 2026 16:00:00 -0800"), Some(DAY));
    }

    #[test]
    fn a_two_digit_year_lands_in_the_century_rfc_1123_says() {
        assert_eq!(rfc822("28 Aug 26 00:00:00 GMT"), w3c("2026-08-28"));
        assert_eq!(rfc822("28 Aug 99 00:00:00 GMT"), w3c("1999-08-28"));
        assert_eq!(rfc822("28 Aug 68 00:00:00 GMT"), w3c("2068-08-28"));
        assert_eq!(rfc822("28 Aug 69 00:00:00 GMT"), w3c("1969-08-28"));
    }

    #[test]
    fn a_single_letter_zone_is_read_as_utc() {
        // RFC 1123 section 5.2.14: the military zones in RFC 822 had the sign
        // backwards, so nobody can be trusted to have meant them.
        assert_eq!(rfc822("Fri, 28 Aug 2026 00:00:00 A"), Some(DAY));
        assert_eq!(rfc822("Fri, 28 Aug 2026 00:00:00 Z"), Some(DAY));
    }

    #[test]
    fn either_grammar_is_accepted_where_either_is_written() {
        // Feeds mix them, and both of these appear in feeds that claim to be
        // the format the other one belongs to.
        assert_eq!(any("Fri, 28 Aug 2026 00:00:00 GMT"), Some(DAY));
        assert_eq!(any("2026-08-28T00:00:00Z"), Some(DAY));
        assert_eq!(any("nonsense"), None);
    }
}
