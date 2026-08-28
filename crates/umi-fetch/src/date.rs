//! HTTP dates, in the three formats RFC 9110 says a parser has to accept.
//!
//! `Last-Modified` is half of a conditional request and it has to survive a
//! round trip through the ledger as a number, because doc 08.3 stores a `u64`
//! of milliseconds and not a string. Origins send all three of the formats,
//! including the obsolete ones, and a parser that only reads the preferred one
//! silently turns a T0 revalidate into a full fetch on the sites that use them.
//!
//! There is no date crate here. The three grammars are fixed, they are ASCII,
//! and the epoch arithmetic is `umi_types::date`, which three crates share
//! because three crates parse a different fixed width date and all of them
//! need the same days from civil algorithm underneath. Pulling in a general
//! date library to parse a fixed width string would be a dependency in every
//! fetcher a volunteer builds.

use umi_types::date::{civil_from_days, epoch_ms};

/// Parse an HTTP date into milliseconds since the Unix epoch.
///
/// Accepts the preferred IMF-fixdate, `Sun, 06 Nov 1994 08:49:37 GMT`, and the
/// two obsolete formats RFC 9110 section 5.6.7 still requires: RFC 850,
/// `Sunday, 06-Nov-94 08:49:37 GMT`, and asctime, `Sun Nov  6 08:49:37 1994`.
/// Returns `None` for anything else, including dates before 1970, which cannot
/// be stored and are always a broken clock rather than a real timestamp.
#[must_use]
pub fn parse(text: &str) -> Option<u64> {
    let text = text.trim();
    let (day, month, year, time) = imf_fixdate(text)
        .or_else(|| rfc850(text))
        .or_else(|| asctime(text))?;
    let (hour, minute, second) = time;
    epoch_ms(year, month, day, hour, minute, second)
}

/// Format milliseconds since the epoch as an IMF-fixdate.
///
/// Only the preferred format is produced. `If-Modified-Since` is generated
/// rather than echoed, because the ledger holds a number, and RFC 9110 says a
/// sender must use IMF-fixdate.
#[must_use]
pub fn format(ms: u64) -> String {
    let seconds = ms / 1000;
    let days = seconds / 86_400;
    let rest = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);

    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    // 1970-01-01 was a Thursday, which is why the table starts there.
    let weekday = WEEKDAYS[(days % 7) as usize];
    let month_name = MONTHS[(month - 1) as usize];
    format!(
        "{weekday}, {day:02} {month_name} {year:04} {:02}:{:02}:{:02} GMT",
        rest / 3600,
        (rest % 3600) / 60,
        rest % 60
    )
}

type Parts = (u32, u32, i32, (u32, u32, u32));

/// `Sun, 06 Nov 1994 08:49:37 GMT`
fn imf_fixdate(text: &str) -> Option<Parts> {
    let rest = text.split_once(", ")?.1;
    let mut fields = rest.split(' ');
    let day = fields.next()?.parse().ok()?;
    let month = month_number(fields.next()?)?;
    let year = fields.next()?.parse().ok()?;
    let time = hms(fields.next()?)?;
    (fields.next() == Some("GMT") && fields.next().is_none()).then_some((day, month, year, time))
}

/// `Sunday, 06-Nov-94 08:49:37 GMT`
fn rfc850(text: &str) -> Option<Parts> {
    let rest = text.split_once(", ")?.1;
    let mut fields = rest.split(' ');
    let mut date = fields.next()?.split('-');
    let day = date.next()?.parse().ok()?;
    let month = month_number(date.next()?)?;
    let two_digit: i32 = date.next()?.parse().ok()?;
    if date.next().is_some() || !(0..=99).contains(&two_digit) {
        return None;
    }
    // RFC 9110's rule for the two digit year: a date more than 50 years in the
    // future is the past century. Anchoring on 2000 rather than on today keeps
    // the parser a pure function, which matters because gate 1.2 wants the
    // whole pipeline replayable.
    let year = if two_digit >= 70 {
        1900 + two_digit
    } else {
        2000 + two_digit
    };
    let time = hms(fields.next()?)?;
    (fields.next() == Some("GMT") && fields.next().is_none()).then_some((day, month, year, time))
}

/// `Sun Nov  6 08:49:37 1994`, where the day is space padded.
fn asctime(text: &str) -> Option<Parts> {
    let mut fields = text.split_ascii_whitespace();
    fields.next()?;
    let month = month_number(fields.next()?)?;
    let day = fields.next()?.parse().ok()?;
    let time = hms(fields.next()?)?;
    let year = fields.next()?.parse().ok()?;
    fields.next().is_none().then_some((day, month, year, time))
}

fn hms(text: &str) -> Option<(u32, u32, u32)> {
    let mut parts = text.split(':');
    let hour = parts.next()?.parse().ok()?;
    let minute = parts.next()?.parse().ok()?;
    let second = parts.next()?.parse().ok()?;
    parts.next().is_none().then_some((hour, minute, second))
}

fn month_number(name: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    MONTHS
        .iter()
        .position(|month| *month == name)
        .map(|index| index as u32 + 1)
}

/// Days since 1970-01-01, or `None` before it.
#[cfg(test)]
mod tests {
    use super::{format, parse};

    /// The example RFC 9110 uses for all three grammars, so all three have to
    /// land on the same instant.
    const REFERENCE_MS: u64 = 784_111_777_000;

    #[test]
    fn the_three_grammars_agree_on_the_reference_date() {
        assert_eq!(parse("Sun, 06 Nov 1994 08:49:37 GMT"), Some(REFERENCE_MS));
        assert_eq!(
            parse("Sunday, 06-Nov-94 08:49:37 GMT"),
            Some(REFERENCE_MS),
            "the RFC 850 form is obsolete and still deployed"
        );
        assert_eq!(
            parse("Sun Nov  6 08:49:37 1994"),
            Some(REFERENCE_MS),
            "asctime pads the day with a space, so splitting on one space fails"
        );
    }

    #[test]
    fn the_epoch_and_a_leap_day_come_out_right() {
        assert_eq!(parse("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        assert_eq!(
            parse("Mon, 29 Feb 2016 12:00:00 GMT"),
            Some(1_456_747_200_000)
        );
        assert_eq!(
            parse("Tue, 29 Feb 2000 00:00:00 GMT"),
            Some(951_782_400_000),
            "2000 is a leap year and the century rule says it should not be"
        );
        assert_eq!(
            parse("Wed, 01 Mar 2100 00:00:00 GMT"),
            Some(4_107_542_400_000),
            "2100 is not a leap year, which is where a naive rule goes wrong"
        );
    }

    #[test]
    fn formatting_is_the_inverse_of_parsing() {
        for text in [
            "Thu, 01 Jan 1970 00:00:00 GMT",
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Mon, 29 Feb 2016 12:00:00 GMT",
            "Sat, 24 Aug 2024 23:59:59 GMT",
        ] {
            let ms = parse(text).expect("parses");
            assert_eq!(format(ms), text);
            assert_eq!(parse(&format(ms)), Some(ms));
        }
    }

    #[test]
    fn a_leap_second_becomes_the_second_before_it() {
        // There is nowhere to put :60 in a Unix timestamp, and dropping the
        // date instead would cost a revalidator over a correct value.
        assert_eq!(
            parse("Sun, 31 Dec 2016 23:59:60 GMT"),
            parse("Sun, 31 Dec 2016 23:59:59 GMT")
        );
    }

    #[test]
    fn the_two_digit_year_pivots_at_seventy() {
        assert_eq!(
            parse("Sunday, 06-Nov-94 08:49:37 GMT"),
            parse("Sun, 06 Nov 1994 08:49:37 GMT")
        );
        assert_eq!(
            parse("Monday, 06-Nov-06 08:49:37 GMT"),
            parse("Mon, 06 Nov 2006 08:49:37 GMT")
        );
    }

    #[test]
    fn a_date_before_the_epoch_is_not_a_date_we_can_store() {
        assert_eq!(parse("Wed, 31 Dec 1969 23:59:59 GMT"), None);
    }

    #[test]
    fn rubbish_is_rejected_rather_than_guessed_at() {
        for text in [
            "",
            "not a date",
            "Sun, 06 Nov 1994 08:49:37",       // no zone
            "Sun, 06 Nov 1994 08:49:37 PST",   // wrong zone
            "Sun, 32 Nov 1994 08:49:37 GMT",   // no such day
            "Sun, 06 Nov 1994 25:49:37 GMT",   // no such hour
            "Sun, 06 Foo 1994 08:49:37 GMT",   // no such month
            "Sun, 06 Nov 1994 08:49 GMT",      // no seconds
            "Sun, 06 Nov 1994 08:49:37 GMT x", // trailing rubbish
        ] {
            assert_eq!(parse(text), None, "{text:?} should not parse");
        }
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        // Header values arrive trimmed, but a value folded across lines by an
        // old intermediary does not.
        assert_eq!(
            parse("  Sun, 06 Nov 1994 08:49:37 GMT  "),
            Some(REFERENCE_MS)
        );
    }
}
