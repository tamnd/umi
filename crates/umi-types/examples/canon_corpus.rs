//! Run canonicalisation over a file of real URLs and report what it did.
//!
//! A generated corpus tests the cases I thought of. This tests the cases the
//! web actually contains, which is a different and larger set. Point it at a
//! newline delimited file of URLs, for example a `url` column dumped out of
//! the seed parquet in `open-index/ccrawl-urls`:
//!
//! ```text
//! cargo run --release -p umi-types --example canon_corpus -- /tmp/urls.txt
//! ```
//!
//! It checks idempotence and the output shape on every line, prints the
//! rejection reasons and the collapse rate, and exits nonzero if anything was
//! not idempotent. The collapse rate is the number worth reading: it is the
//! fraction of the seed set that canonicalisation merges away, and it feeds
//! straight into how big the frontier has to be.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use umi_types::RowKey;
use umi_types::canon::{CanonError, canonicalize};

fn main() -> std::process::ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: canon_corpus <file of urls, one per line>");
        return umi_types::Exit::Usage.into();
    };

    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("cannot read {path}: {e}");
            return umi_types::Exit::Usage.into();
        }
    };

    let mut total = 0u64;
    let mut ok = 0u64;
    let mut changed = 0u64;
    let mut rejected: HashMap<&'static str, u64> = HashMap::new();
    let mut not_idempotent = Vec::new();
    let mut malformed_output = Vec::new();
    let mut keys = std::collections::HashSet::new();
    let mut samples: Vec<(String, String)> = Vec::new();
    let mut sample_hosts = std::collections::HashSet::new();
    let mut categories: HashMap<&'static str, u64> = HashMap::new();

    let start = Instant::now();
    for line in BufReader::new(file).lines() {
        let Ok(url) = line else { continue };
        let url = url.trim();
        if url.is_empty() {
            continue;
        }
        total += 1;

        let canonical = match canonicalize(url, None) {
            Ok(c) => c,
            Err(e) => {
                *rejected.entry(reason(e)).or_default() += 1;
                if std::env::var_os("CANON_SHOW_REJECT").is_some() {
                    eprintln!("REJECT {} {url}", reason(e));
                }
                continue;
            }
        };
        ok += 1;

        if canonical != url {
            changed += 1;
            for label in classify(url, &canonical) {
                *categories.entry(label).or_default() += 1;
            }
            // One sample per host. The seed parquet is sorted by surt key, so
            // taking the first 25 rewrites gives 25 rows from one site and
            // says nothing about what canonicalisation does to the web.
            let host = host_of(url);
            if samples.len() < 25 && sample_hosts.insert(host.to_owned()) {
                samples.push((url.to_owned(), canonical.clone()));
            }
        }

        match canonicalize(&canonical, None) {
            Ok(twice) if twice == canonical => {}
            Ok(twice) => {
                if not_idempotent.len() < 25 {
                    not_idempotent.push((url.to_owned(), canonical.clone(), twice));
                }
            }
            Err(e) => {
                if not_idempotent.len() < 25 {
                    not_idempotent.push((url.to_owned(), canonical.clone(), format!("error: {e}")));
                }
            }
        }

        if (canonical.contains('#') || canonical.contains('@') || canonical.ends_with('?'))
            && malformed_output.len() < 10
        {
            malformed_output.push(canonical.clone());
        }

        if let Ok(key) = RowKey::for_url(url, None) {
            keys.insert(key.url);
        }
    }
    let elapsed = start.elapsed();

    println!("input_urls: {total}");
    println!("canonicalised: {ok}");
    println!("rejected: {}", total - ok);
    let mut reasons: Vec<_> = rejected.iter().collect();
    reasons.sort_by_key(|(_, n)| std::cmp::Reverse(**n));
    for (why, n) in reasons {
        println!("  {why}: {n}");
    }
    println!(
        "rewritten: {changed} ({:.2}% of canonicalised)",
        pct(changed, ok)
    );
    if !categories.is_empty() {
        println!("what changed, urls may count in more than one row:");
        let mut cats: Vec<_> = categories.iter().collect();
        cats.sort_by_key(|(_, n)| std::cmp::Reverse(**n));
        for (label, n) in cats {
            println!("  {label}: {n} ({:.2}%)", pct(*n, ok));
        }
    }
    println!("distinct_url_keys: {}", keys.len());
    println!(
        "collapse_rate: {:.4}% of canonicalised urls merged into an existing key",
        pct(ok - keys.len() as u64, ok)
    );
    println!(
        "throughput: {:.0} urls/s ({:.2}s wall)",
        total as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64()
    );

    if !samples.is_empty() {
        println!("\nrewrites, first {}:", samples.len());
        for (before, after) in &samples {
            println!("  {}\n    -> {}", clip(before), clip(after));
        }
    }

    if !malformed_output.is_empty() {
        println!("\nMALFORMED OUTPUT, {} shown:", malformed_output.len());
        for out in &malformed_output {
            println!("  {out}");
        }
    }

    if !not_idempotent.is_empty() {
        println!("\nNOT IDEMPOTENT, {} shown:", not_idempotent.len());
        for (input, once, twice) in &not_idempotent {
            println!("  {input}\n    once:  {once}\n    twice: {twice}");
        }
        return umi_types::Exit::Verification.into();
    }

    if !malformed_output.is_empty() {
        return umi_types::Exit::Verification.into();
    }

    println!("\nidempotent on every url that canonicalised");
    umi_types::Exit::Success.into()
}

/// The host of a URL, with the port, the query and any trailing path removed.
///
/// Splitting on `/` and taking field three is the obvious way to do this and
/// it is wrong twice over: `https://host.com?q=1` has no third slash, so the
/// query comes back as the host, and `https://host.com:443/` keeps the port,
/// so stripping a default port reads as the host changing. Both showed up as
/// bogus "host rewritten" rows on the first run over real data.
fn host_of(url: &str) -> &str {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    // An IPv6 literal is bracketed and full of colons, so only strip a port
    // when what follows the last colon is entirely digits and the host does
    // not end in a bracket.
    match authority.rsplit_once(':') {
        Some((host, port))
            if !port.is_empty()
                && port.bytes().all(|b| b.is_ascii_digit())
                && !authority.ends_with(']') =>
        {
            host
        }
        _ => authority,
    }
}

/// Why a URL was rewritten. Coarse and heuristic, because its job is to say
/// which canonicalisation steps actually fire on the open web and in what
/// proportion, not to be a second implementation of the steps.
fn classify(before: &str, after: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    if before.contains('#') && !after.contains('#') {
        out.push("fragment removed");
    }
    let q_before = before.split_once('?').map(|(_, q)| q).unwrap_or("");
    let q_after = after.split_once('?').map(|(_, q)| q).unwrap_or("");
    if q_before.len() > q_after.len() {
        out.push("query parameters removed");
    }
    if before.contains('@') && !after.contains('@') {
        out.push("userinfo removed");
    }
    if before.contains(";jsessionid") || before.contains(";sid=") || before.contains(";phpsessid") {
        out.push("path parameter removed");
    }
    let host_before = host_of(before);
    let host_after = host_of(after);
    if host_before != host_after {
        if host_before.eq_ignore_ascii_case(host_after) {
            out.push("host lowercased");
        } else if host_after.contains("xn--") {
            out.push("host idna encoded");
        } else {
            out.push("host otherwise rewritten");
            if std::env::var_os("CANON_SHOW_HOST").is_some() {
                eprintln!("HOSTCHANGE {host_before} -> {host_after}   {before}");
            }
        }
    }
    if out.is_empty() {
        out.push("path or encoding normalised");
    }
    out
}

/// Real URLs run to two kilobytes of nested referer chains and printing them
/// whole buries the summary that the report exists for.
fn clip(s: &str) -> String {
    const WIDTH: usize = 110;
    if s.len() <= WIDTH {
        return s.to_owned();
    }
    let head: String = s.chars().take(WIDTH - 20).collect();
    let tail: String = s
        .chars()
        .rev()
        .take(15)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head} ...{} more... {tail}", s.len() - WIDTH + 5)
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 * 100.0 / d as f64
    }
}

fn reason(e: CanonError) -> &'static str {
    match e {
        CanonError::NotHttp => "not http(s)",
        CanonError::Malformed => "malformed",
        CanonError::NoHost => "no host",
        CanonError::BadHost => "bad host",
        CanonError::TooLong => "over 2048 bytes",
    }
}
