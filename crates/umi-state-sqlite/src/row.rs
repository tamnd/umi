//! Turning SQLite rows into the trait's types and back.
//!
//! All of it is by hand and none of it is derived. That is deliberate: the
//! moment a row becomes a serialised struct, the file stops being something a
//! person can open with the `sqlite3` shell and read, and doc 08.5's argument
//! for this backend being the default is exactly that it is portable and
//! inspectable with any SQLite tool.
//!
//! Two conversions here deserve a note.
//!
//! SQLite integers are signed 64 bit and the trait's timestamps are unsigned.
//! Every real timestamp fits comfortably in both, but "never due again" is
//! `u64::MAX`, which does not. [`to_ms`] saturates it to [`i64::MAX`], which is
//! a moment about 292 million years from now and is therefore still never in
//! any sense that matters, and which keeps `next_due_ms <= ?now` a correct
//! comparison in SQL rather than an accidental negative number.
//!
//! Booleans are stored as 0 and 1 rather than as SQLite's untyped truthiness,
//! so that a `blocked = 0` in a query means what it looks like.

use rusqlite::Row;
use rusqlite::types::Type;
use umi_state::{
    HostRow, LedgerRow, Priority, RemoteCopy, RobotsRef, SegmentRow, Stream, TierPolicy, UrlState,
};
use umi_types::{Digest, HostId, PldId, Tier, Ulid, UrlKey, UrlKeyFull};

/// A column held something this build cannot read back.
#[derive(Debug)]
pub struct BadColumn {
    column: String,
    why: String,
}

impl std::fmt::Display for BadColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "column {}: {}", self.column, self.why)
    }
}

impl std::error::Error for BadColumn {}

fn bad(column: &str, why: String, kind: Type) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        kind,
        Box::new(BadColumn {
            column: column.to_owned(),
            why,
        }),
    )
}

/// A fixed width key column, checked rather than truncated.
pub(crate) fn bytes<const N: usize>(row: &Row<'_>, column: &str) -> rusqlite::Result<[u8; N]> {
    let raw: Vec<u8> = row.get(column)?;
    let got = raw.len();
    raw.try_into().map_err(|_| {
        bad(
            column,
            format!("expected {N} bytes, found {got}"),
            Type::Blob,
        )
    })
}

/// A timestamp on its way into the file.
#[must_use]
pub fn to_ms(ms: u64) -> i64 {
    i64::try_from(ms).unwrap_or(i64::MAX)
}

/// A timestamp on its way out. Negative cannot happen through [`to_ms`], and
/// clamping rather than wrapping means a hand edited file produces a row that
/// is due now instead of one due before the epoch.
#[must_use]
pub fn from_ms(ms: i64) -> u64 {
    u64::try_from(ms).unwrap_or(0)
}

pub(crate) fn tier(row: &Row<'_>, column: &str) -> rusqlite::Result<Tier> {
    let raw: i64 = row.get(column)?;
    u8::try_from(raw)
        .ok()
        .and_then(Tier::from_u8)
        .ok_or_else(|| bad(column, format!("{raw} is not a tier"), Type::Integer))
}

fn url_state(row: &Row<'_>, column: &str) -> rusqlite::Result<UrlState> {
    let raw: i64 = row.get(column)?;
    u8::try_from(raw)
        .ok()
        .and_then(UrlState::from_u8)
        .ok_or_else(|| bad(column, format!("{raw} is not a url state"), Type::Integer))
}

pub(crate) fn small<T: TryFrom<i64>>(row: &Row<'_>, column: &str) -> rusqlite::Result<T> {
    let raw: i64 = row.get(column)?;
    T::try_from(raw).map_err(|_| bad(column, format!("{raw} is out of range"), Type::Integer))
}

/// A counter, which is unsigned in the trait and signed in the file.
fn count(row: &Row<'_>, column: &str) -> rusqlite::Result<u64> {
    let raw: i64 = row.get(column)?;
    u64::try_from(raw).map_err(|_| bad(column, format!("{raw} is negative"), Type::Integer))
}

/// The pay level domain of a row.
pub fn pld(row: &Row<'_>, column: &str) -> rusqlite::Result<PldId> {
    Ok(PldId::from_bytes(bytes(row, column)?))
}

/// The host of a row.
pub fn host(row: &Row<'_>, column: &str) -> rusqlite::Result<HostId> {
    Ok(HostId::from_bytes(bytes(row, column)?))
}

/// The url fingerprint of a row.
pub fn url_key(row: &Row<'_>, column: &str) -> rusqlite::Result<UrlKey> {
    Ok(UrlKey::from_bytes(bytes(row, column)?))
}

/// Sitemap urls, stored newline separated.
///
/// A newline cannot appear in a url, so this is unambiguous without a
/// serialisation format, and it keeps the column readable in the `sqlite3`
/// shell. An empty column is no sitemaps rather than one empty sitemap.
#[must_use]
pub fn join_sitemaps(sitemaps: &[String]) -> String {
    sitemaps.join("\n")
}

fn split_sitemaps(joined: &str) -> Vec<String> {
    if joined.is_empty() {
        return Vec::new();
    }
    joined.split('\n').map(ToOwned::to_owned).collect()
}

/// Read a whole ledger row, for [`complete`](umi_state::State::complete).
///
/// The column list is spelled out at the call site rather than `SELECT *`, so
/// that adding a column in a later migration is a compile error here instead of
/// a silent shift of every index.
pub fn ledger(row: &Row<'_>) -> rusqlite::Result<LedgerRow> {
    Ok(LedgerRow {
        url_key_full: UrlKeyFull::from_bytes(bytes(row, "url_key_full")?),
        host_id: host(row, "host")?,
        depth: small(row, "depth")?,
        priority: Priority::from_raw(small(row, "priority")?),
        state: url_state(row, "state")?,
        next_due_ms: from_ms(row.get("next_due_ms")?),
        last_fetch_ms: from_ms(row.get("last_fetch_ms")?),
        last_change_ms: from_ms(row.get("last_change_ms")?),
        fetch_count: small(row, "fetch_count")?,
        change_count: small(row, "change_count")?,
        content_hash: bytes(row, "content_hash")?,
        etag_ref: small(row, "etag_ref")?,
        last_mod_ms: from_ms(row.get("last_mod_ms")?),
        status: small(row, "status")?,
        tier_used: tier(row, "tier_used")?,
        fail_streak: small(row, "fail_streak")?,
        observed_secs: small(row, "observed_secs")?,
    })
}

/// Read a whole host record.
pub fn host_record(row: &Row<'_>) -> rusqlite::Result<HostRow> {
    // All four robots columns are null together or set together, so the digest
    // being present is the whole test.
    let robots_digest: Option<Vec<u8>> = row.get("robots_digest")?;
    let robots = match robots_digest {
        Some(_) => Some(RobotsRef {
            digest: Digest::from_bytes(bytes(row, "robots_digest")?),
            fetched_ms: from_ms(row.get("robots_fetched_ms")?),
            expires_ms: from_ms(row.get("robots_expires_ms")?),
            authoritative: row.get::<_, i64>("robots_authoritative")? != 0,
        }),
        None => None,
    };

    Ok(HostRow {
        host: host(row, "host")?,
        pld: pld(row, "pld")?,
        robots,
        adaptive_delay_ms: small(row, "adaptive_delay_ms")?,
        crawl_delay_ms: row
            .get::<_, Option<i64>>("crawl_delay_ms")?
            .map(|ms| u32::try_from(ms).unwrap_or(u32::MAX)),
        next_allowed_ms: from_ms(row.get("next_allowed_ms")?),
        tier: TierPolicy {
            preferred: tier(row, "tier_preferred")?,
            max: tier(row, "tier_max")?,
            last_success: tier(row, "tier_last_success")?,
            consecutive_blocks: small(row, "tier_blocks")?,
            last_probe_down_ms: from_ms(row.get("tier_probe_down_ms")?),
            render_required: row.get::<_, i64>("render_required")? != 0,
            weak_hits: small(row, "weak_revalidator")?,
            lying_revalidator: row.get::<_, i64>("lying_revalidator")? != 0,
        },
        content_usage: row.get("content_usage")?,
        sitemaps: split_sitemaps(&row.get::<_, String>("sitemaps")?),
        fetches: count(row, "fetches")?,
        failures: count(row, "failures")?,
        consecutive_failures: small(row, "consecutive_failures")?,
        fast_streak: small(row, "fast_streak")?,
        blocked: row.get::<_, i64>("blocked")? != 0,
        refusing: row.get::<_, i64>("refusing")? != 0,
    })
}

/// The pacing half of a host record, for [`sql::SELECT_PACE`].
///
/// Everything doc 07.6's rate limiter does not read is left at its default,
/// which is safe only because the write side is [`sql::PACE_HOST`] and that is
/// as narrow as this is. A row from here must never be handed to `put_host`,
/// which would write those defaults over a real robots record.
///
/// [`sql::SELECT_PACE`]: crate::sql::SELECT_PACE
/// [`sql::PACE_HOST`]: crate::sql::PACE_HOST
pub fn pacing(row: &Row<'_>, host_id: HostId) -> rusqlite::Result<HostRow> {
    Ok(HostRow {
        host: host_id,
        pld: pld(row, "pld")?,
        adaptive_delay_ms: small(row, "adaptive_delay_ms")?,
        crawl_delay_ms: row
            .get::<_, Option<i64>>("crawl_delay_ms")?
            .map(|ms| u32::try_from(ms).unwrap_or(u32::MAX)),
        next_allowed_ms: from_ms(row.get("next_allowed_ms")?),
        fetches: count(row, "fetches")?,
        failures: count(row, "failures")?,
        consecutive_failures: small(row, "consecutive_failures")?,
        fast_streak: small(row, "fast_streak")?,
        ..HostRow::default()
    })
}

fn stream(row: &Row<'_>, column: &str) -> rusqlite::Result<Stream> {
    let raw: i64 = row.get(column)?;
    u8::try_from(raw)
        .ok()
        .and_then(Stream::from_u8)
        .ok_or_else(|| bad(column, format!("{raw} is not a stream"), Type::Integer))
}

/// Read a whole segment record.
pub fn segment(row: &Row<'_>) -> rusqlite::Result<SegmentRow> {
    // The schema has a CHECK that keeps the three remote columns null together
    // or set together, so testing one is testing all three. That is the same
    // shape as the robots columns above and it is deliberate: a nullable group
    // in this file always means one optional value, never three independent
    // ones.
    let repo: Option<String> = row.get("remote_repo")?;
    let remote = match repo {
        Some(repo) => Some(RemoteCopy {
            repo,
            path: row.get("remote_path")?,
            digest: Digest::from_bytes(bytes(row, "remote_digest")?),
        }),
        None => None,
    };

    Ok(SegmentRow {
        id: Ulid::from_bytes(bytes(row, "id")?),
        stream: stream(row, "stream")?,
        local_path: row.get("local_path")?,
        sealed_at_ms: from_ms(row.get("sealed_at_ms")?),
        rows: count(row, "rows")?,
        bytes: count(row, "bytes")?,
        local_digest: Digest::from_bytes(bytes(row, "local_digest")?),
        remote,
        manifest_day: row
            .get::<_, Option<i64>>("manifest_day")?
            .map(|day| u32::try_from(day).unwrap_or(0)),
        deleted_at_ms: row.get::<_, Option<i64>>("deleted_at_ms")?.map(from_ms),
    })
}
