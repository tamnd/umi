//! The `umi` binary: argument parsing, dispatch and the exit code.
//!
//! The parser here is the real one for every command in doc 14, including the
//! ones that are not built yet. That is deliberate: the shape of the command
//! line is a design decision and reviewing it is cheaper now than after there
//! are scripts depending on it. Commands that are not built yet exit 1 and name
//! the milestone that builds them, which is in `docs/spec/16-roadmap.md`. They
//! never do something adjacent and call it done.
//!
//! Everything a command actually does lives in the `umi-cli` library next to
//! this file.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use umi_cli::{Error, config, doctor, get, inspect};
use umi_types::{CANON_VERSION, Exit};

/// An internet scale web crawler that publishes what it finds.
#[derive(Parser)]
#[command(name = "umi", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a focused crawl of a domain, host, path prefix or scope profile.
    Crawl(CrawlArgs),
    /// Continue a crawl directory from where it stopped.
    Resume {
        /// The crawl directory, as laid out in doc 13.5.
        dir: String,
    },
    /// Continue a crawl and keep it fresh instead of stopping when idle.
    Watch {
        /// The crawl directory.
        dir: String,
    },
    /// Contribute fetch capacity to a coordinator.
    Fetch(FetchArgs),
    /// Check that this machine can do the thing it is about to do.
    Doctor {
        /// Skip every check that touches the network.
        #[arg(long)]
        offline: bool,
        /// The directory a crawl would write into, for the disk check.
        #[arg(long)]
        out: Option<String>,
    },
    /// Print the effective configuration and where every value came from.
    Config,
    /// Add URLs to a frontier from an external source.
    Seed {
        /// Where the URLs come from: cc, sitemap, feed, corpus, or `-` for stdin.
        source: String,
        /// The argument that source takes, if it takes one.
        target: Option<String>,
    },
    /// List segments or published files with row counts and time ranges.
    Ls {
        /// A crawl directory or a published repository.
        target: String,
    },
    /// Stream rows out of a segment as newline delimited JSON.
    Cat {
        /// The segment or Parquet file.
        path: String,
        /// Stop after this many rows.
        #[arg(long)]
        limit: Option<u64>,
        /// Only these columns, comma separated.
        #[arg(long)]
        columns: Option<String>,
    },
    /// Fetch one URL through the full tier ladder and print what came back.
    Get {
        /// The URL.
        url: String,
        /// Highest tier to escalate to.
        #[arg(long, value_name = "N")]
        tier: Option<u8>,
        /// Print the extracted markdown.
        #[arg(long)]
        markdown: bool,
        /// Print the extracted plain text.
        #[arg(long)]
        text: bool,
        /// Print the extracted links.
        #[arg(long)]
        links: bool,
        /// Print the extracted metadata.
        #[arg(long)]
        meta: bool,
        /// Print the response headers doc 11.5 keeps.
        #[arg(long)]
        headers: bool,
        /// Print the digests and versions a receipt would carry.
        #[arg(long)]
        receipt: bool,
        /// Print the response body exactly as it arrived.
        #[arg(long)]
        raw: bool,
    },
    /// Run DuckDB over local Parquet or a published checkpoint.
    Sql {
        /// The query.
        query: String,
        /// The crawl directory or repository to attach.
        #[arg(long)]
        data: Option<String>,
    },
    /// Inspect and manage the state store.
    State {
        /// One of: stats, warm, evict.
        action: String,
        /// The pay level domain, for warm and evict.
        pld: Option<String>,
    },
    /// Write a portable state checkpoint.
    Checkpoint {
        /// Checkpoint format: native or duckdb.
        #[arg(long, default_value = "native")]
        format: String,
    },
    /// Push a crawl directory through the publishing pipeline.
    Publish {
        /// The crawl directory.
        dir: String,
    },
    /// Re verify manifests, signatures and digests.
    Verify {
        /// A crawl directory or a published repository.
        target: String,
    },
    /// Print or validate a manifest chain.
    Manifest {
        /// The repository.
        repo: String,
    },
    /// Stop crawling a domain, permanently and on the record.
    Block {
        /// The domain to block.
        domain: String,
        /// Why, which is published alongside the block.
        #[arg(long)]
        reason: String,
    },
    /// Evaluate a scope profile against a list of URLs.
    Scope {
        /// One of: check.
        action: String,
        /// The profile path.
        profile: String,
    },
    /// Ask a local or remote coordinator how it is doing.
    Status {
        /// Emit an object instead of a screen.
        #[arg(long)]
        json: bool,
    },
    /// Show coordinator peering state.
    Peers,
    /// Show connected fetchers, their rates and their reputation.
    Fetchers,
}

/// Options for `umi crawl`, from doc 14.3.
#[derive(clap::Args)]
struct CrawlArgs {
    /// A domain, a host, a URL, or a path to a scope profile.
    target: String,

    /// Extra include matcher, repeatable.
    #[arg(long)]
    include: Vec<String>,
    /// Exclude matcher, repeatable.
    #[arg(long)]
    exclude: Vec<String>,
    /// Maximum hops from a seed. Not path segments.
    #[arg(long)]
    depth: Option<u8>,
    /// What to do with links that leave the scope.
    #[arg(long, default_value = "in-scope", value_parser = ["in-scope", "record", "one-hop"])]
    links: String,

    /// Stop after this many pages.
    #[arg(long)]
    max_pages: Option<u64>,
    /// Stop after this long.
    #[arg(long, value_name = "DURATION")]
    r#for: Option<String>,
    /// Do not stop when the frontier drains. Keep it fresh instead.
    #[arg(long)]
    watch: bool,

    /// Requests per second per host. Clamped by the politeness rules in doc 07
    /// and never raised past them.
    #[arg(long)]
    rps: Option<f32>,
    /// Simultaneous in flight fetches.
    #[arg(long)]
    concurrency: Option<u16>,

    /// Highest tier allowed.
    #[arg(long)]
    tier: Option<u8>,
    /// Never open a browser. Equivalent to `--tier 2`.
    #[arg(long)]
    no_render: bool,

    /// Seed from a file of URLs, or `-` for stdin.
    #[arg(long)]
    seed: Option<String>,
    /// Any program that prints URLs to stdout, repeatable.
    #[arg(long)]
    seeder: Vec<String>,
    /// Seed from Common Crawl before starting.
    #[arg(long)]
    from_cc: bool,

    /// Output directory.
    #[arg(long)]
    out: Option<String>,
    /// State backend.
    #[arg(long, value_parser = ["sqlite", "nami", "postgres"])]
    state: Option<String>,
    /// Publish to Hugging Face, and delete local copies once they verify.
    #[arg(long)]
    publish: bool,
}

/// Options for `umi fetch`, from doc 14.4.
#[derive(clap::Args)]
struct FetchArgs {
    /// The coordinator to lease work from.
    #[arg(long)]
    coordinator: Option<String>,
    /// Pages per second you are willing to sustain.
    #[arg(long)]
    rate: Option<f32>,
    /// Simultaneous in flight fetches.
    #[arg(long, default_value_t = 8)]
    concurrency: u16,
    /// Highest tier you are willing to run.
    #[arg(long, default_value_t = 2)]
    tier: u8,
    /// Where the ed25519 identity lives. Generated on first run.
    #[arg(long)]
    identity: Option<String>,
    /// Do not offer tier 3 even if a browser is present.
    #[arg(long)]
    no_render: bool,
    /// Refuse to fetch this domain from this machine, repeatable.
    #[arg(long)]
    refuse: Vec<String>,
}

impl Command {
    /// The flags that feed doc 14.7's precedence, pulled out of whichever
    /// subcommand is running.
    fn flags(&self) -> config::Flags {
        match self {
            Self::Crawl(args) => config::Flags {
                rps: args.rps,
                concurrency: args.concurrency,
                // `--no-render` is doc 14.3's spelling of `--tier 2`, and the
                // explicit flag wins when both are given because a person who
                // typed a number meant it.
                tier_max: args.tier.or(if args.no_render { Some(2) } else { None }),
                out: args.out.clone(),
                backend: args.state.clone(),
                ..config::Flags::default()
            },
            Self::Fetch(args) => config::Flags {
                coordinator: args.coordinator.clone(),
                rate: args.rate,
                ..config::Flags::default()
            },
            _ => config::Flags::default(),
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli.command) {
        Ok(()) => ExitCode::from(Exit::Success),
        Err(error) => {
            let exit = error.exit();
            eprintln!("umi: {error}");
            ExitCode::from(exit)
        }
    }
}

fn run(command: &Command) -> Result<(), Error> {
    match command {
        Command::Doctor { offline, out } => {
            let config = load(command)?;
            let checks = doctor::doctor(&doctor::Options {
                offline: *offline,
                out: out.clone().unwrap_or(config.out.value).into(),
            })?;
            match doctor::worst(&checks) {
                doctor::Verdict::Bad => Err(Error::NotReady),
                _ => Ok(()),
            }
        }
        Command::Config => print_config(&load(command)?),
        Command::Ls { target } => inspect::ls(target),
        Command::Cat {
            path,
            limit,
            columns,
        } => inspect::cat(path, *limit, columns.as_deref()),
        Command::Get {
            url,
            tier,
            markdown,
            text,
            links,
            meta,
            headers,
            receipt,
            raw,
        } => {
            // The flag and not `config.tier_max`. That setting is how high a
            // crawl is allowed to climb, and `umi get` is the command you run
            // to ask what one specific tier does to one specific page, so
            // inheriting a fleet wide ceiling here would answer a different
            // question than the one being asked.
            get::get(
                url,
                *tier,
                &get::Show {
                    markdown: *markdown,
                    text: *text,
                    links: *links,
                    meta: *meta,
                    headers: *headers,
                    receipt: *receipt,
                    raw: *raw,
                },
            )
        }
        other => Err(not_built(other)),
    }
}

fn load(command: &Command) -> Result<config::Config, Error> {
    let cwd = std::env::current_dir().map_err(Error::Io)?;
    let config = config::Config::load(
        &config::Paths::discover(&cwd),
        &config::env_from_process(),
        &command.flags(),
    )?;
    if let Some(token) = &config.token
        && let Some(warning) = token.value.warning()
    {
        eprintln!("umi: {} says {warning}", token.origin);
    }
    Ok(config)
}

/// `umi config`, which doc 14.7 describes as the thing you want at 2am when a
/// setting is not taking effect. Every line carries its source for that reason.
fn print_config(config: &config::Config) -> Result<(), Error> {
    println!("{:<20} {:<28} from", "setting", "value");
    let line = |name: &str, value: String, origin: &config::Origin| {
        println!("{name:<20} {value:<28} {origin}");
    };
    line(
        "crawl.rps",
        config.rps.value.to_string(),
        &config.rps.origin,
    );
    line(
        "crawl.concurrency",
        config.concurrency.value.to_string(),
        &config.concurrency.origin,
    );
    line(
        "crawl.tier_max",
        config.tier_max.value.to_string(),
        &config.tier_max.origin,
    );
    line("crawl.out", config.out.value.clone(), &config.out.origin);
    line(
        "state.backend",
        config.backend.value.clone(),
        &config.backend.origin,
    );
    line("publish.org", config.org.value.clone(), &config.org.origin);
    match &config.token {
        // Never the value. The whole reason `Secret` is a type is that a
        // command whose job is printing configuration must not print this one.
        // The indirection is printed with whether it currently resolves, which
        // is the question somebody running `umi config` is actually asking.
        Some(token) => {
            let resolves = match token.value.read() {
                Ok(_) => "resolves",
                Err(_) => "does not resolve",
            };
            line(
                "publish.token",
                match &token.value {
                    config::Secret::Env(name) => format!("env:{name}, {resolves}"),
                    config::Secret::File(path) => {
                        format!("file:{}, {resolves}", path.display())
                    }
                    config::Secret::Literal(_) => "a literal, not shown".to_owned(),
                },
                &token.origin,
            );
        }
        None => line(
            "publish.token",
            "unset".to_owned(),
            &config::Origin::Default,
        ),
    }
    line(
        "fetch.coordinator",
        config.coordinator.value.clone(),
        &config.coordinator.origin,
    );
    line(
        "fetch.rate",
        config.rate.value.to_string(),
        &config.rate.origin,
    );
    println!();
    if config.files.is_empty() {
        println!("no config file was read");
    } else {
        for path in &config.files {
            println!("read {}", path.display());
        }
    }
    println!("canonicalisation {CANON_VERSION}");
    Ok(())
}

/// The message a specified but unbuilt command gives. It names the milestone,
/// because "not implemented" without a date is indistinguishable from
/// abandoned.
fn not_built(command: &Command) -> Error {
    Error::NotBuilt(match command {
        Command::Crawl(_) | Command::Resume { .. } | Command::Seed { .. } => {
            "the crawl loop is milestone 1 and lands next"
        }
        Command::Publish { .. } | Command::Verify { .. } | Command::Manifest { .. } => {
            "publishing needs the Hugging Face client, milestone 1"
        }
        Command::Watch { .. } | Command::Block { .. } | Command::Scope { .. } => {
            "milestone 2 builds this"
        }
        Command::State { .. } | Command::Checkpoint { .. } | Command::Sql { .. } => {
            "milestone 3 builds this"
        }
        _ => "milestone 4 builds this, when there is a fleet to talk to",
    })
}
