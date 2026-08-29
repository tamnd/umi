//! `umi get`, doc 14.6's debugging workhorse.
//!
//! One URL, through the ladder, with the tier reporting visible and whatever
//! the caller asked to see printed afterwards. The point of it is answering
//! "why did this page extract badly", so it prints the extractor version
//! whether you ask for it or not: an answer that is not reproducible is not an
//! answer.

use std::time::Duration;

use umi_extract::Extracted;
use umi_fetch::{FetchConfig, Ladder, Outcome, Tier};
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

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;

    // Honest rather than silently doing something else. T2 and T3 are behind
    // cargo features and T4 is not written, so pretending a `--tier 4` request
    // was served would make this command useless for the exact question it
    // exists to answer.
    let fetcher = runtime.block_on(ladder(tier))?;
    let highest = fetcher.highest();
    if let Some(byte) = tier
        && byte > highest.as_u8()
    {
        eprintln!(
            "tier {byte} is not built here, doing tier {}: see docs/spec/16-roadmap.md",
            highest.as_u8()
        );
    }
    let asked = tier
        .and_then(Tier::from_u8)
        .unwrap_or(Tier::Plain)
        .min(highest);

    let served = runtime.block_on(fetcher.fetch(parsed.as_str(), None, asked));
    // Close the browser before returning, whichever way this went. Dropping
    // the ladder kills Chromium anyway, but it leaves the profile directory
    // behind, and a debugging command that leaves a few hundred megabytes in
    // the temp directory every time somebody runs it is a command people stop
    // running.
    let result = served.map_err(Error::from).and_then(|served| {
        // The rung that answered rather than the rung that was asked for. They
        // are the same here, since `asked` is already clamped to what this
        // build has, but reading it off the answer is how it stays that way.
        let used = served.path.used();
        present(served.outcome, &parsed, used, show)
    });
    runtime.block_on(fetcher.shutdown());
    result
}

/// The ladder this command runs the page through.
///
/// T3 starts a browser and no other rung does, so it is built only when the
/// caller asked for T3 or higher by name. Doc 14.6 says this command is the
/// one you reach for when a page extracted badly, and a page that extracted
/// badly because it is a client rendered shell is exactly the case where the
/// answer is invisible without a browser. Refusing to start one here would
/// leave the only rung that can explain that page unreachable from the command
/// line, which is how `umi get --tier 3` came to report tier 2 on every client
/// rendered site somebody pointed it at.
///
/// One tab and not doc 05.6's eight. This fetches one url and then exits, so
/// the pool never has a second page to hand out and the tabs would cost a few
/// hundred megabytes to sit idle for the length of one fetch.
async fn ladder(tier: Option<u8>) -> Result<Ladder, Error> {
    #[cfg(feature = "render")]
    if tier.is_some_and(|byte| byte >= Tier::Rendered.as_u8()) {
        let mut config = FetchConfig::default();
        // Doc 05.4's ceiling rather than the 512 KB the one page commands
        // default to, for this rung only. What T3 returns is the serialised
        // DOM after the scripts have run, and that is routinely several times
        // the html the origin sent: vercel.com is 200 KB on the wire and over
        // half a megabyte rendered. Leaving the small cap here meant `--tier
        // 3` answered "200, TooLarge" on exactly the client rendered sites it
        // exists to look at.
        config.body_cap = 8 << 20;
        // Assigned rather than built with a struct literal because
        // `RenderConfig` is non exhaustive, which is the point: the rest of
        // doc 05.6's numbers are defaults this command does not second guess.
        let mut render = umi_fetch::RenderConfig::default();
        render.tabs = 1;
        return Ok(Ladder::with_rendered(config, None, render).await?);
    }
    #[cfg(not(feature = "render"))]
    let _ = tier;
    Ok(Ladder::with_config(FetchConfig::default())?)
}

/// Print whatever came back, which is the rest of the command.
fn present(outcome: Outcome, parsed: &Url, used: Tier, show: &Show) -> Result<(), Error> {
    match outcome {
        Outcome::Ok(page) => {
            report(&page, parsed, used);
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
                parsed,
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

fn report(page: &umi_fetch::Page, requested: &Url, tier: Tier) {
    println!(
        "tier {}  http  {}  {}",
        tier.as_u8(),
        page.status,
        millis(page.elapsed)
    );
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
