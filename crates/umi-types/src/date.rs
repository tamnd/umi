//! Civil dates to and from days since the Unix epoch.
//!
//! Two functions, and they are here rather than in one crate because three
//! crates need them and none of them needs a date library. `umi-fetch` parses
//! the three HTTP date formats RFC 9110 requires, `umi-seed` parses the W3C
//! datetime a sitemap `lastmod` carries and the RFC 822 date an RSS `pubDate`
//! carries, and every one of those grammars is a fixed ASCII string with the
//! same arithmetic underneath.
//!
//! That arithmetic is Howard Hinnant's days from civil, which is exact for any
//! year the proleptic Gregorian calendar is defined over and needs no lookup
//! tables and no leap second list. Pulling in a general date library to do it
//! would put chrono or time in every fetcher a volunteer downloads, for thirty
//! lines of shifts and divides.
//!
//! Nothing here knows about time zones. Every grammar these serve is either
//! UTC by definition or carries its own offset, so the offset is the caller's
//! to apply before it gets here.

/// Days from 1970-01-01 to a civil date, or `None` before the epoch.
///
/// Dates before 1970 cannot be stored in the `u64` of milliseconds doc 08.3
/// uses, and in practice a pre epoch date on the web is a broken clock rather
/// than a real timestamp, so refusing it loses nothing.
///
/// `month` is 1 through 12 and `day` is 1 through 31. Neither is validated
/// here, because the callers have already validated them against their own
/// grammar and a second check would just be a second place to disagree.
#[must_use]
pub fn days_from_civil(year: i32, month: u32, day: u32) -> Option<u64> {
    if year < 1970 {
        return None;
    }
    let year = year - i32::from(month <= 2);
    let era = year / 400;
    let year_of_era = year - era * 400;
    let day_of_year =
        (153 * (month as i32 + if month > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    u64::try_from(era * 146_097 + day_of_era - 719_468).ok()
}

/// The inverse: a civil year, month and day from days since the epoch.
#[must_use]
pub fn civil_from_days(days: u64) -> (i32, u32, u32) {
    let z = days as i64 + 719_468;
    let era = z / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    (
        (year + i64::from(month <= 2)) as i32,
        month as u32,
        day as u32,
    )
}

/// Milliseconds since the epoch from a civil date and a time of day.
///
/// The one place the two halves are put together, so that three callers do not
/// each write the same multiply and each get a different answer about a leap
/// second. A `second` of 60 is folded to 59, because there is nowhere to put a
/// leap second in a count of milliseconds and rejecting the timestamp would
/// lose a real date over a value that was correct when it was written.
#[must_use]
pub fn epoch_ms(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<u64> {
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let seconds = days_from_civil(year, month, day)?.checked_mul(86_400)?
        + u64::from(hour) * 3600
        + u64::from(minute) * 60
        + u64::from(second.min(59));
    seconds.checked_mul(1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_is_day_zero() {
        assert_eq!(days_from_civil(1970, 1, 1), Some(0));
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn the_two_directions_agree_over_a_long_run() {
        // Every day for 150 years, which covers four century rules and every
        // leap year shape between them.
        for day in 0..54_787u64 {
            let (y, m, d) = civil_from_days(day);
            assert_eq!(days_from_civil(y, m, d), Some(day), "day {day}");
        }
    }

    #[test]
    fn a_leap_day_is_a_real_day() {
        assert_eq!(
            civil_from_days(days_from_civil(2000, 2, 29).unwrap()),
            (2000, 2, 29)
        );
        assert_eq!(
            civil_from_days(days_from_civil(2024, 2, 29).unwrap()),
            (2024, 2, 29)
        );
    }

    #[test]
    fn before_the_epoch_is_none_rather_than_a_wrap() {
        assert_eq!(days_from_civil(1969, 12, 31), None);
        assert_eq!(epoch_ms(1969, 12, 31, 23, 59, 59), None);
    }

    #[test]
    fn a_known_timestamp_comes_out_right() {
        // 2001-09-09T01:46:40Z, the billionth second.
        assert_eq!(epoch_ms(2001, 9, 9, 1, 46, 40), Some(1_000_000_000_000));
    }

    #[test]
    fn a_leap_second_lands_on_the_second_before() {
        assert_eq!(
            epoch_ms(2016, 12, 31, 23, 59, 60),
            epoch_ms(2016, 12, 31, 23, 59, 59)
        );
    }

    #[test]
    fn an_impossible_field_is_none() {
        assert_eq!(epoch_ms(2020, 13, 1, 0, 0, 0), None);
        assert_eq!(epoch_ms(2020, 1, 0, 0, 0, 0), None);
        assert_eq!(epoch_ms(2020, 1, 1, 24, 0, 0), None);
        assert_eq!(epoch_ms(2020, 1, 1, 0, 60, 0), None);
    }
}
