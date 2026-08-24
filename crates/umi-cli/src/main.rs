//! The `umi` command line.
//!
//! The surface is specified in `docs/spec/14-cli.md`. The parser here is the
//! real one and it is deliberately ahead of the implementation, because the
//! shape of the command line is a design decision and reviewing it is cheaper
//! now than after there are scripts depending on it.
//!
//! Commands that are not built yet exit 1 with a pointer at the milestone that
//! builds them, which is `docs/spec/16-roadmap.md`.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
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
    Doctor,
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
        /// Print the extracted links.
        #[arg(long)]
        links: bool,
        /// Print the receipt.
        #[arg(long)]
        receipt: bool,
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
    #[arg(long, default_value_t = 1.0)]
    rps: f32,
    /// Simultaneous in flight fetches.
    #[arg(long, default_value_t = 4)]
    concurrency: u16,

    /// Highest tier allowed.
    #[arg(long, default_value_t = 3)]
    tier: u8,
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
    #[arg(long, default_value = "sqlite", value_parser = ["sqlite", "nami", "postgres"])]
    state: String,
    /// Publish to Hugging Face, and delete local copies once they verify.
    #[arg(long)]
    publish: bool,
}

/// Options for `umi fetch`, from doc 14.4.
#[derive(clap::Args)]
struct FetchArgs {
    /// The coordinator to lease work from.
    #[arg(long, default_value = "https://umi.dev")]
    coordinator: String,
    /// Pages per second you are willing to sustain.
    #[arg(long, default_value_t = 2.0)]
    rate: f32,
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

fn main() -> ExitCode {
    let cli = Cli::parse();

    let (milestone, doc) = match &cli.command {
        Command::Crawl(_) | Command::Resume { .. } => (1, "16-roadmap.md, milestone 1"),
        Command::Ls { .. } | Command::Cat { .. } | Command::Get { .. } => {
            (1, "16-roadmap.md, milestone 1")
        }
        Command::Doctor | Command::Publish { .. } | Command::Verify { .. } => {
            (1, "16-roadmap.md, milestone 1")
        }
        Command::Manifest { .. } | Command::Seed { .. } => (1, "16-roadmap.md, milestone 1"),
        Command::Watch { .. } | Command::Block { .. } | Command::Scope { .. } => {
            (2, "16-roadmap.md, milestone 2")
        }
        Command::State { .. } | Command::Checkpoint { .. } | Command::Sql { .. } => {
            (3, "16-roadmap.md, milestone 3")
        }
        Command::Fetch(_) | Command::Status { .. } | Command::Peers | Command::Fetchers => {
            (4, "16-roadmap.md, milestone 4")
        }
    };

    eprintln!("umi {} ({CANON_VERSION})", env!("CARGO_PKG_VERSION"));
    eprintln!("this command is specified but not built yet: see docs/spec/{doc}");
    eprintln!("milestone {milestone} is where it lands");

    ExitCode::from(Exit::Failure)
}
