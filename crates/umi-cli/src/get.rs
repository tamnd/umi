//! `umi get`, doc 14.6's debugging workhorse.
//!
//! One URL, through the ladder, with the tier reporting visible and whatever
//! the caller asked to see printed afterwards. The point of it is answering
//! "why did this page extract badly", so it prints the extractor version
//! whether you ask for it or not: an answer that is not reproducible is not an
//! answer.

use std::time::Duration;

use umi_extract::Extracted;
use umi_fetch::{FetchConfig, Fetcher, Outcome};
use url::Url;

use crate::Error;

/// Which parts of the result to print. Everything off means the summary only,
/// which is the common case when the question is "did this even work".
#[derive(Default)]
pub struct Show {
    /// `--markdown`.
    pub markdown: bool,
    /// `--text`.
    pub text: bool,
    /// `--links`.
    pub links: bool,
    /// `--meta`.
    pub meta: bool,
    /// `--headers`.
    pub headers: bool,
    /// `--receipt`.
    pub receipt: bool,
    /// `--raw`.
    pub raw: bool,
}

/// Fetch one URL and print it.
///
/// # Errors
///
/// When the URL will not parse, the fetcher cannot be built, or the fetch
/// failed. A 404 is a failure here and not an empty result, because somebody
/// typing `umi get` wants to know.
pub fn get(url: &str, tier: Option<u8>, show: &Show) -> Result<(), Error> {
    let parsed = Url::parse(url).map_err(|_| Error::BadUrl(url.to_owned()))?;

    if let Some(tier) = tier
        && tier > 1
    {
        // Honest rather than silently doing something else. Doc 05's T2 and T3
        // are milestone 2 and milestone 3, and pretending a `--tier 3` request
        // was served would make this command useless for the exact question it
        // exists to answer.
        eprintln!("tier {tier} is not built yet, doing tier 1: see docs/spec/16-roadmap.md");
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    let fetcher = Fetcher::with_config(FetchConfig::default())?;
    let outcome = runtime.block_on(fetcher.fetch(parsed.as_str(), None))?;

    match outcome {
        Outcome::Ok(page) => {
            report(&page, &parsed);
            if show.headers {
                section("headers");
                for (name, value) in &page.headers_kept {
                    println!("{name}: {value}");
                }
            }
            if show.raw {
                section("raw");
                // Bytes, not a string. The body may not be UTF-8 and guessing
                // at it is exactly the bug this command is used to find.
                use std::io::Write as _;
                std::io::stdout().write_all(&page.body).map_err(Error::Io)?;
                println!();
            }
            let extracted = umi_extract::extract_with_headers(
                &page.body,
                &parsed,
                umi_extract::Headers {
                    x_robots_tag: header(&page, "x-robots-tag"),
                    link: header(&page, "link"),
                },
            );
            show_extracted(&extracted, show);
            if show.receipt {
                section("receipt");
                println!("body_digest    blake3:{}", hex::encode(page.body_digest));
                println!("headers_digest blake3:{}", hex::encode(page.headers_digest));
                println!("extractor      {}", extracted.version);
                println!("canon          {}", umi_types::CANON_VERSION);
            }
            Ok(())
        }
        Outcome::NotModified { elapsed, .. } => {
            println!("304 not modified in {}", millis(elapsed));
            Ok(())
        }
        Outcome::Gone => Err(Error::Fetch("410 gone".to_owned())),
        Outcome::Failed {
            status, failure, ..
        } => Err(Error::Fetch(match status {
            Some(code) => format!("{code}, {failure:?}"),
            None => format!("{failure:?}"),
        })),
        Outcome::RedirectedOffDomain { target, status, .. } => Err(Error::Fetch(format!(
            "{status} left the registrable domain, to {target}"
        ))),
        // `Outcome` is non exhaustive on purpose, so a new variant added by a
        // later tier shows up here rather than being silently reported as a
        // success by a wildcard that returns `Ok`.
        other => Err(Error::Fetch(format!("{other:?}"))),
    }
}

/// One of doc 11.5's kept headers, by name, case insensitively.
fn header<'a>(page: &'a umi_fetch::Page, wanted: &str) -> Option<&'a str> {
    page.headers_kept
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(wanted))
        .map(|(_, value)| value.as_str())
}

fn report(page: &umi_fetch::Page, requested: &Url) {
    println!("tier 1  http  {}  {}", page.status, millis(page.elapsed));
    if page.final_url != requested.as_str() {
        println!("final   {}", page.final_url);
    }
    for hop in &page.redirects {
        println!("hop     {} {}", hop.status, hop.to);
    }
    println!(
        "type    {}  {:?}  {} bytes",
        page.content_type.as_deref().unwrap_or("none"),
        page.media,
        page.body.len()
    );
    println!("proto   {:?}", page.version);
}

fn show_extracted(extracted: &Extracted, show: &Show) {
    let signals = extracted.signals;
    println!(
        "extract {}  {} text bytes, {} links, {}% density{}",
        extracted.version,
        signals.text_bytes,
        signals.link_count,
        signals.link_density,
        if extracted.boilerplate_uncertain {
            ", boilerplate uncertain"
        } else if extracted.declared_root {
            ", declared root"
        } else {
            ""
        }
    );
    if let Some(why) = extracted.content_withheld {
        println!("withheld {why:?}");
    }

    if show.markdown {
        section("markdown");
        println!("{}", extracted.markdown);
    }
    if show.text {
        section("text");
        println!("{}", umi_extract::plain_text(&extracted.markdown));
    }
    if show.links {
        section("links");
        for link in &extracted.links.links {
            println!("{:?}  {}  {}", link.kind, link.url, link.anchor);
        }
        println!(
            "{} kept, {} dropped, {} other schemes",
            extracted.links.links.len(),
            extracted.links.dropped,
            extracted.links.dropped_scheme
        );
    }
    if show.meta {
        section("meta");
        let meta = &extracted.meta;
        print_opt("title", meta.title.as_deref());
        print_opt("description", meta.description.as_deref());
        print_opt("canonical", meta.canonical.as_deref());
        print_opt("lang", meta.declared_lang.as_deref());
        print_opt("published", meta.published.as_deref());
        print_opt("modified", meta.modified.as_deref());
        for heading in &meta.headings {
            println!("h{}           {}", heading.level, heading.text);
        }
        for feed in &meta.feeds {
            println!("feed         {feed}");
        }
    }
}

fn print_opt(name: &str, value: Option<&str>) {
    if let Some(value) = value {
        println!("{name:<12} {value}");
    }
}

fn section(name: &str) {
    // Headings on stderr so that `umi get --markdown ... > page.md` gives a
    // file that is markdown and not markdown with a banner in it.
    eprintln!("--- {name}");
}

fn millis(elapsed: Duration) -> String {
    format!("{:.0} ms", elapsed.as_secs_f64() * 1000.0)
}
