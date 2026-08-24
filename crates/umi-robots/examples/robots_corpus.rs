//! Run the parser over a directory of real robots.txt files and report what it
//! found.
//!
//! The conformance suite tests the cases the RFC authors thought of. This
//! tests the cases the web actually contains, which is a different and much
//! stranger set. Point it at a directory of files, one per host, named after
//! the host:
//!
//! ```text
//! cargo run --release -p umi-robots --example robots_corpus -- /tmp/robots
//! ```
//!
//! It reports how many files parse, how many name us, what the rule counts and
//! `Crawl-delay` values look like across the sample, and how many sites are a
//! blanket disallow. The numbers that matter for the crawl are the blanket
//! disallow rate, which is how much of the frontier we lose before we start,
//! and the clamped delay rate, which is how many hosts publish a delay that is
//! a soft block rather than politeness.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use umi_robots::{DEFAULT_TTL, MAX_BYTES, Robots};

fn main() -> std::process::ExitCode {
    let Some(dir) = std::env::args().nth(1) else {
        eprintln!("usage: robots_corpus <directory of robots.txt files>");
        return std::process::ExitCode::from(2);
    };

    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("cannot read {dir}: {e}");
            return std::process::ExitCode::from(2);
        }
    };

    let mut files = 0u64;
    let mut empty = 0u64;
    let mut not_utf8 = 0u64;
    let mut over_cap = 0u64;
    let mut with_rules = 0u64;
    let mut blanket = 0u64;
    let mut names_us = 0u64;
    let mut with_sitemap = 0u64;
    let mut with_content_usage = 0u64;
    let mut with_delay = 0u64;
    let mut clamped_delay = 0u64;
    let mut lenient_colon = 0u64;
    let mut total_rules = 0u64;
    let mut max_rules = (0usize, String::new());
    let mut delays: Vec<Duration> = Vec::new();
    let mut rule_counts: Vec<usize> = Vec::new();
    let mut bytes = 0u64;
    let mut disallowed_root = 0u64;
    let mut looks_like_html = 0u64;
    let mut unknown_fields: HashMap<String, u64> = HashMap::new();

    let start = Instant::now();
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(body) = std::fs::read(&path) else {
            continue;
        };
        let host = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        files += 1;
        bytes += body.len() as u64;
        if body.is_empty() {
            empty += 1;
            continue;
        }
        if body.len() > MAX_BYTES {
            over_cap += 1;
        }
        if std::str::from_utf8(&body).is_err() {
            not_utf8 += 1;
        }

        // Count the lines a strict reading would have thrown away, so the
        // leniency in `split_line` is a measured decision rather than a
        // guess.
        let text = String::from_utf8_lossy(&body);
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() || line.contains(':') {
                if let Some((field, _)) = line.split_once(':') {
                    let field = field.trim().to_ascii_lowercase();
                    if !matches!(
                        field.as_str(),
                        "user-agent"
                            | "useragent"
                            | "allow"
                            | "disallow"
                            | "sitemap"
                            | "crawl-delay"
                            | "content-usage"
                    ) && !field.is_empty()
                    {
                        *unknown_fields.entry(field).or_default() += 1;
                    }
                }
                continue;
            }
            if let Some((field, _)) = line.split_once([' ', '\t'])
                && matches!(
                    field.to_ascii_lowercase().as_str(),
                    "user-agent" | "useragent" | "allow" | "disallow"
                )
            {
                lenient_colon += 1;
            }
        }

        // A soft 404: an HTML error page served as 200 at /robots.txt. Common
        // enough to measure, and the reason to measure it is that parsing one
        // must produce no rules. A stray rule out of an error page would block
        // a site that never published a robots.txt at all.
        if text.to_ascii_lowercase().contains("<html") {
            looks_like_html += 1;
        }

        let robots = Robots::parse(&body);
        let count = robots.rule_count();
        total_rules += count as u64;
        rule_counts.push(count);
        if count > max_rules.0 {
            max_rules = (count, host.clone());
        }
        if count > 0 {
            with_rules += 1;
        }
        if robots.is_blanket_disallow() {
            blanket += 1;
        }
        if !robots.allows("/").is_allowed() {
            disallowed_root += 1;
        }
        if !robots.sitemaps().is_empty() {
            with_sitemap += 1;
        }
        if !robots.content_usage().is_empty() {
            with_content_usage += 1;
        }
        if let Some(delay) = robots.crawl_delay() {
            with_delay += 1;
            delays.push(delay);
            if robots.crawl_delay_was_clamped() {
                clamped_delay += 1;
            }
        }

        // Does the file address us by name rather than by `*`. Comparing the
        // `umi` group against the `*` group is the only way to tell, because a
        // file with no `umi` group parses to the `*` group's rules.
        let ours = Robots::parse_for(&text, &["umi"]);
        let wildcard = Robots::parse_for(&text, &["*"]);
        if ours.rule_count() > 0 && ours.rule_count() != wildcard.rule_count() {
            names_us += 1;
        }
    }
    let elapsed = start.elapsed();

    rule_counts.sort_unstable();
    delays.sort_unstable();

    println!("files: {files}");
    println!("bytes: {bytes} ({:.1} KiB mean)", mean_kib(bytes, files));
    println!("empty: {empty} ({:.2}%)", pct(empty, files));
    println!("not valid utf8: {not_utf8} ({:.2}%)", pct(not_utf8, files));
    println!(
        "html served as robots.txt: {looks_like_html} ({:.2}%)",
        pct(looks_like_html, files)
    );
    println!(
        "over the {} KiB cap: {over_cap} ({:.2}%)",
        MAX_BYTES / 1024,
        pct(over_cap, files)
    );
    println!(
        "with at least one rule for us: {with_rules} ({:.2}%)",
        pct(with_rules, files)
    );
    println!(
        "naming umi explicitly: {names_us} ({:.2}%)",
        pct(names_us, files)
    );
    println!("blanket disallow: {blanket} ({:.2}%)", pct(blanket, files));
    println!(
        "root disallowed by any rule: {disallowed_root} ({:.2}%)",
        pct(disallowed_root, files)
    );
    println!(
        "with a sitemap: {with_sitemap} ({:.2}%)",
        pct(with_sitemap, files)
    );
    println!(
        "with aipref content-usage: {with_content_usage} ({:.2}%)",
        pct(with_content_usage, files)
    );
    println!(
        "with a crawl-delay: {with_delay} ({:.2}%), of those {clamped_delay} clamped down",
        pct(with_delay, files)
    );
    if !delays.is_empty() {
        println!(
            "crawl-delay p50 {:?}, p90 {:?}, max {:?}",
            percentile(&delays, 50),
            percentile(&delays, 90),
            delays[delays.len() - 1]
        );
    }
    println!("total rules: {total_rules}");
    if !rule_counts.is_empty() {
        println!(
            "rules per file p50 {}, p90 {}, p99 {}, max {} on {}",
            percentile(&rule_counts, 50),
            percentile(&rule_counts, 90),
            percentile(&rule_counts, 99),
            max_rules.0,
            max_rules.1
        );
    }
    println!("lines saved by assuming a missing colon: {lenient_colon} across {files} files");
    println!(
        "throughput: {:.0} files/s ({:.2}s wall), ttl {} h",
        files as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64(),
        DEFAULT_TTL.as_secs() / 3600
    );

    if !unknown_fields.is_empty() {
        let mut fields: Vec<_> = unknown_fields.into_iter().collect();
        fields.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
        println!("\nunknown fields, top 15 of {}:", fields.len());
        for (field, n) in fields.iter().take(15) {
            println!("  {field}: {n}");
        }
    }

    std::process::ExitCode::SUCCESS
}

fn percentile<T: Copy>(sorted: &[T], p: usize) -> T {
    sorted[(sorted.len() - 1) * p / 100]
}

fn pct(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 * 100.0 / d as f64
    }
}

fn mean_kib(bytes: u64, files: u64) -> f64 {
    if files == 0 {
        0.0
    } else {
        bytes as f64 / files as f64 / 1024.0
    }
}
