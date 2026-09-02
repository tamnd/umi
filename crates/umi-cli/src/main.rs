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
use umi_cli::{
    Error, block, cards, config, crawl, doctor, evict, get, inspect, retract, robots, supervise,
    verify, warm,
};
use umi_crawl::{Clock, SystemClock};
use umi_types::{CANON_VERSION, Exit};

/// The allocator every command runs on.
///
/// Here and not in the library, because a `#[global_allocator]` is a choice
/// about a process and a library has no business making it for anybody who
/// links it. The binary is the process, so this is where the choice belongs.
///
/// It is worth having. Profiled on server3 under a crawl at a window of 1024,
/// glibc malloc and free came to 21 percent of the process before counting the
/// memmove that goes with them, spread across `_int_malloc`, `unlink_chunk`,
/// `cfree`, `malloc_consolidate` and `_int_free_merge_chunk`. None of that is
/// a page being parsed or a byte being fetched. It is the cost of a lot of
/// threads asking a small number of shared arenas for a lot of short lived
/// buffers of every size, which is the exact shape of html5ever tokenising a
/// page into tendrils and dropping them again. mimalloc gives each thread its
/// own heap, so most of the contention and most of the free list walking stops
/// existing rather than getting faster.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
        /// Publish to Hugging Face, and delete local copies once they verify.
        ///
        /// A flag here rather than something remembered from the first run,
        /// because publishing needs a token and a signing key and neither
        /// belongs in a profile that gets checked in.
        #[arg(long)]
        publish: bool,
    },
    /// Continue a crawl and keep it fresh instead of stopping when idle.
    Watch {
        /// The crawl directory.
        dir: String,
        /// Publish to Hugging Face, and delete local copies once they verify.
        #[arg(long)]
        publish: bool,
    },
    /// Contribute fetch capacity to a coordinator.
    Fetch(FetchArgs),
    /// Fetch robots.txt for a list of hosts and publish doc 07.4's corpus.
    Robots(RobotsArgs),
    /// Check that this machine can do the thing it is about to do.
    Doctor {
        /// Skip every check that touches the network.
        #[arg(long)]
        offline: bool,
        /// The directory a crawl would write into, for the disk check.
        #[arg(long)]
        out: Option<String>,
        /// Measure sustained inbound and outbound for real, doc 16's gate 1.1.
        /// This moves several gigabytes and takes a couple of minutes, so it is
        /// off unless asked for.
        #[arg(long)]
        bandwidth: bool,
        /// Seconds per direction. Doc 16's gate wants at least 60.
        #[arg(long, default_value_t = 60, value_name = "SECONDS")]
        bandwidth_secs: u64,
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
    /// Move the backlog off the local disk and onto the hub, doc 08.6's evict.
    ///
    /// A crawl admits far more URLs than it fetches and all of them sit in the
    /// ledger, so on a fleet whose disks are a cache the backlog is what fills
    /// them. This writes the coldest domains into a frontier segment, publishes
    /// it, records where each domain went, and only then drops the local rows.
    /// A run that publishes nothing deletes nothing.
    Evict {
        /// The crawl directory.
        dir: String,
        /// How many domains to move.
        #[arg(long, default_value_t = evict::DOMAINS)]
        limit: usize,
        /// Say how many domains would move and move none of them.
        #[arg(long)]
        dry_run: bool,
    },
    /// Bring the backlog back off the hub, doc 08.6's warm.
    ///
    /// The other direction from evict. A domain that was spilled into a
    /// published frontier file is a pointer to a row group range, and this
    /// reads that range back and puts the ledger rows where they were. The
    /// published file is untouched: what goes is the pointer, so the domain is
    /// local again and the next eviction writes a fresh one.
    Warm {
        /// The crawl directory.
        dir: String,
        /// How many domains to bring back.
        #[arg(long, default_value_t = warm::DOMAINS)]
        limit: usize,
        /// Say how many domains would come back and bring none of them.
        #[arg(long)]
        dry_run: bool,
    },
    /// Rewrite the dataset card on repositories that already have one.
    ///
    /// A card is written when a repository is created and not again, so an
    /// improvement to the generator reaches nothing already published. This is
    /// the pass that carries it across. Run it by hand and rarely.
    Cards {
        /// One repository, with or without the organisation on it. Left out,
        /// every umi repository in the organisation.
        repo: Option<String>,
        /// Say what would be written without writing it.
        #[arg(long)]
        dry_run: bool,
    },
    /// Take published files back out, on the record.
    ///
    /// Doc 12.2 says a published file is never rewritten and never deleted,
    /// because both break every checksum anyone recorded, and this command does
    /// not soften that. It is here so that an operator who has decided a
    /// deletion has to happen anyway does it in a form that leaves the
    /// repository verifiable: the deletions and the rewritten day manifests go
    /// in one commit, the chain is relinked to the end of the repository, and a
    /// record naming every removed file with the digest it had is published to
    /// the meta repository first.
    Retract {
        /// The repository, with or without the organisation on it.
        repo: String,
        /// A repository relative path to remove. Repeat it, or use --from.
        #[arg(long = "file", value_name = "PATH")]
        files: Vec<String>,
        /// A file holding one path per line, with blank lines and # comments
        /// skipped. For the case where there are too many to read on one
        /// command line.
        #[arg(long, value_name = "PATH")]
        from: Option<String>,
        /// Why, in words. Published with the record and required.
        #[arg(long)]
        reason: Option<String>,
        /// Print what would go and commit nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Re verify manifests, signatures and digests.
    Verify {
        /// A published repository, with or without the organisation on it.
        target: String,
        /// Download every file and digest it here, rather than comparing
        /// against the digest the hub reports.
        #[arg(long)]
        full: bool,
    },
    /// Print or validate a manifest chain.
    Manifest {
        /// The repository.
        repo: String,
    },
    /// Stop crawling a domain, permanently and on the record.
    Block {
        /// The domain to block. Left out, the command prints the list.
        domain: Option<String>,
        /// Why, which is published alongside the block.
        #[arg(long)]
        reason: Option<String>,
        /// Record that this block has been lifted, with the reason as the
        /// note. The entry stays in the list either way, per doc 07.7.
        #[arg(long)]
        lift: bool,
        /// The crawl directory to apply it to.
        #[arg(long, default_value = ".")]
        dir: String,
    },
    /// Put a domain on doc 05.7's supervised allowlist, or take it off.
    Supervise {
        /// The domain. Left out, the command prints the list.
        domain: Option<String>,
        /// Why, which is published alongside the entry.
        #[arg(long)]
        reason: Option<String>,
        /// Who is adding it. A person, because doc 05.7 wants somebody who can
        /// be asked about it later.
        #[arg(long)]
        operator: Option<String>,
        /// Take the domain off the list, with the reason as the note. The entry
        /// stays in the list either way.
        #[arg(long)]
        remove: bool,
        /// Print every tier 4 fetch from this domain instead of changing
        /// anything. This is what an operator gets sent when they ask.
        #[arg(long)]
        show: bool,
        /// The crawl directory to apply it to.
        #[arg(long, default_value = ".")]
        dir: String,
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
    /// Doc 05.7's opt in to tier 4. Off by default. It still only reaches the
    /// domains on the supervised list, which `umi supervise` writes.
    #[arg(long)]
    allow_supervised: bool,
    /// Browser tabs for tier 3, doc 05.6. Zero, the default, is a machine
    /// that does not render.
    #[arg(long)]
    tabs: Option<u16>,

    /// Seed from a file of URLs, or `-` for stdin.
    #[arg(long)]
    seed: Option<String>,
    /// Any program that prints URLs to stdout, repeatable.
    #[arg(long)]
    seeder: Vec<String>,
    /// Follow the site's own sitemaps before crawling. On by default.
    #[arg(long, overrides_with = "no_sitemaps")]
    sitemaps: bool,
    /// Do not follow sitemaps.
    #[arg(long)]
    no_sitemaps: bool,
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
    /// Serve doc 15.4's Prometheus series while the crawl runs. Bare, it binds
    /// 127.0.0.1:9772. Give an address to bind somewhere else, and read the
    /// note in `umi_cli::exporter` before you bind a public one.
    #[arg(long, value_name = "ADDR", num_args = 0..=1, default_missing_value = "127.0.0.1:9772")]
    metrics: Option<String>,
}

impl CrawlArgs {
    /// The flags and the configuration, as one plan.
    ///
    /// Doc 14.7's precedence has already run by the time this is called, so
    /// `config` holds the answer for anything the flags left out and this is
    /// only the translation into doc 13's vocabulary.
    fn plan(&self, config: &config::Config) -> Result<crawl::Options, Error> {
        Ok(crawl::Options {
            target: self.target.clone(),
            include: self.include.clone(),
            exclude: self.exclude.clone(),
            depth: self.depth,
            links: self.links.clone(),
            max_pages: self.max_pages,
            max_duration: self.r#for.clone(),
            watch: self.watch,
            rps: config.rps.value,
            concurrency: config.concurrency.value,
            tier_max: config.tier_max.value,
            allow_supervised: self.allow_supervised,
            tabs: config.tabs.value,
            seed: self.seed.clone(),
            seeder: self.seeder.clone(),
            // Nothing when neither flag was given, because doc 13.4 lets the
            // profile decide and a flag that was not typed must not outvote a
            // key that was written. The two flags override each other, so at
            // most one of these is true.
            sitemaps: match (self.sitemaps, self.no_sitemaps) {
                (true, _) => Some(true),
                (_, true) => Some(false),
                _ => None,
            },
            out: self.out.clone(),
            publish: crawl::Publishing::resolve(config, self.publish)?,
            identity: crawl::Identity::resolve(config)?,
            metrics: self.metrics.clone(),
        })
    }
}

/// Options for `umi robots`, from doc 14.5.
#[derive(clap::Args)]
struct RobotsArgs {
    /// A file of hosts, `-` for standard input, or an `org/name` dataset on
    /// the hub. The default is the published domain ranking.
    #[arg(default_value = robots::DOMAINS)]
    source: String,

    /// The column to read when the source is a dataset.
    #[arg(long, default_value = robots::DOMAIN_COLUMN)]
    column: String,
    /// Only files under this prefix, when the source is a dataset.
    #[arg(long)]
    prefix: Option<String>,
    /// Do not ask hosts a published robots corpus already answers for. Bare
    /// for the corpus this project publishes, or name another one.
    #[arg(long, num_args = 0..=1, default_missing_value = robots::KNOWN, value_name = "REPO")]
    known: Option<String>,

    /// Simultaneous in flight fetches.
    #[arg(long, default_value_t = robots::CONCURRENCY)]
    concurrency: u16,
    /// Stop after this many hosts.
    #[arg(long)]
    limit: Option<u64>,
    /// Skip this many hosts from the front of the list, which is how one run
    /// continues where the last one stopped.
    #[arg(long, default_value_t = 0)]
    skip: u64,
    /// Stop after this long.
    #[arg(long, value_name = "DURATION")]
    r#for: Option<String>,

    /// Output directory.
    #[arg(long)]
    out: Option<String>,
    /// Publish to Hugging Face, and delete local copies once they verify.
    #[arg(long)]
    publish: bool,
}

impl RobotsArgs {
    fn plan(&self, config: &config::Config) -> Result<robots::Options, Error> {
        Ok(robots::Options {
            source: self.source.clone(),
            column: self.column.clone(),
            prefix: self.prefix.clone(),
            known: self.known.clone(),
            out: self.out.clone(),
            concurrency: self.concurrency,
            limit: self.limit,
            skip: self.skip,
            max_duration: self.r#for.clone(),
            publish: crawl::Publishing::resolve(config, self.publish)?,
            identity: crawl::Identity::resolve(config)?,
        })
    }
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
                // `--no-render` has to reach the tab cap as well as the tier
                // ceiling, or a box with tabs configured would still start a
                // browser and then never send it a page.
                tabs: if args.no_render { Some(0) } else { args.tabs },
                out: args.out.clone(),
                backend: args.state.clone(),
                ..config::Flags::default()
            },
            Self::Robots(args) => config::Flags {
                out: args.out.clone(),
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
        Command::Doctor {
            offline,
            out,
            bandwidth,
            bandwidth_secs,
        } => {
            let config = load(command)?;
            let identity = crawl::Identity::resolve(&config)?;
            let keyid = match identity {
                Some(identity) => Some(identity.keyid()?),
                None => None,
            };
            let checks = doctor::doctor(&doctor::Options {
                offline: *offline,
                out: out.clone().unwrap_or(config.out.value).into(),
                bandwidth: bandwidth.then_some(*bandwidth_secs),
                identity: keyid,
            })?;
            match doctor::worst(&checks) {
                doctor::Verdict::Bad => Err(Error::NotReady),
                _ => Ok(()),
            }
        }
        Command::Config => print_config(&load(command)?),
        Command::Ls { target } => {
            let config = load(command)?;
            inspect::ls(&inspect::Ls {
                target,
                token: token(&config)?,
                org: &config.org.value,
            })
        }
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
        Command::Crawl(args) => {
            let config = load(command)?;
            finish(crawl::crawl(&args.plan(&config)?))
        }
        Command::Robots(args) => {
            let config = load(command)?;
            let summary = robots::robots(&args.plan(&config)?)?;
            println!(
                "{} hosts asked, {} rows in {} files, {} published, {} never answered",
                summary.fetched, summary.rows, summary.files, summary.published, summary.failed
            );
            match summary.stopped {
                crawl::Stop::Budget => Err(Error::Budget),
                crawl::Stop::Idle | crawl::Stop::Signal => Ok(()),
            }
        }
        Command::Resume { dir, publish } => {
            let config = load(command)?;
            let publishing = crawl::Publishing::resolve(&config, *publish)?;
            let identity = crawl::Identity::resolve(&config)?;
            finish(crawl::resume(
                std::path::Path::new(dir),
                false,
                publishing,
                identity,
            ))
        }
        Command::Retract {
            repo,
            files,
            from,
            reason,
            dry_run,
        } => {
            let config = load(command)?;
            // The same pair `umi publish` needs, because this writes and signs
            // exactly like a publish does. Read before anything else so a run
            // without a key fails now rather than after the record is up.
            let publishing = crawl::Publishing::resolve(&config, true)?.ok_or_else(|| {
                Error::Missing("umi retract needs publish.token and publish.key".to_owned())
            })?;
            let report = retract::run(
                &retract::Options {
                    repo,
                    files: retract::paths(files, from.as_ref().map(std::path::Path::new))?,
                    reason: reason.as_deref().unwrap_or_default(),
                    dry_run: *dry_run,
                    meta_repo: umi_publish::repo::META_REPO,
                    org: &config.org.value,
                    now_ms: SystemClock.now_ms(),
                },
                publishing.token.clone(),
                &publishing.key,
            )?;
            for path in &report.removed {
                println!("{path}: {}", if *dry_run { "would go" } else { "gone" });
            }
            println!(
                "{} files, {} rows, {} bytes, {} manifests rewritten, record at {}",
                report.removed.len(),
                report.rows,
                report.bytes,
                report.rewritten.len(),
                report.record
            );
            Ok(())
        }
        Command::Verify { target, full } => {
            let config = load(command)?;
            verify::run(target, token(&config)?, *full, &config.org.value)
        }
        Command::Cards { repo, dry_run } => {
            let config = load(command)?;
            // The same extractor string the publisher stamps into a manifest,
            // because the card quotes it and a card that disagreed with the
            // manifests underneath it would be worse than no card.
            let extractor = umi_publish::PublishConfig::default().extractor;
            let report = cards::run(
                repo.as_deref(),
                token(&config)?,
                &config.org.value,
                &extractor,
                *dry_run,
            )?;
            for name in &report.written {
                println!(
                    "{name}: card {}",
                    if *dry_run { "would change" } else { "written" }
                );
            }
            println!(
                "{} written, {} already current, {} not umi's",
                report.written.len(),
                report.unchanged.len(),
                report.skipped.len()
            );
            Ok(())
        }
        Command::Publish { dir } => {
            match crawl::Publishing::resolve(&load(command)?, true)? {
                Some(publishing) => {
                    let summary = crawl::publish(std::path::Path::new(dir), &publishing)?;
                    println!(
                        "{} of {} files published, {} rows, {} bytes",
                        summary.published, summary.files, summary.rows, summary.bytes_stored
                    );
                    Ok(())
                }
                // `resolve` returns a value or an error when publishing was
                // asked for, and this command is the asking.
                None => Err(Error::Missing(
                    "umi publish needs publish.token and publish.key".to_owned(),
                )),
            }
        }
        Command::Evict {
            dir,
            limit,
            dry_run,
        } => match crawl::Publishing::resolve(&load(command)?, true)? {
            Some(publishing) => {
                let summary = evict::evict(
                    &evict::Options {
                        dir: std::path::PathBuf::from(dir),
                        limit: *limit,
                        dry_run: *dry_run,
                    },
                    &publishing,
                )?;
                println!(
                    "{} of {} files published, {} rows, {} bytes",
                    summary.published, summary.files, summary.rows, summary.bytes_stored
                );
                Ok(())
            }
            // The backlog goes to the hub or it does not go anywhere. There is
            // no local mode here on purpose: writing a frontier segment and
            // leaving it on the disk is not offloading the disk.
            None => Err(Error::Missing(
                "umi evict needs publish.token and publish.key".to_owned(),
            )),
        },
        Command::Warm {
            dir,
            limit,
            dry_run,
        } => match crawl::Publishing::resolve(&load(command)?, true)? {
            Some(publishing) => {
                let warmed = warm::warm(
                    &warm::Options {
                        dir: std::path::PathBuf::from(dir),
                        limit: *limit,
                        dry_run: *dry_run,
                    },
                    &publishing,
                )?;
                println!(
                    "{} domains and {} rows from {} files",
                    warmed.domains, warmed.rows, warmed.files
                );
                Ok(())
            }
            // A warm reads from the hub and nowhere else, because the hub is
            // where the rows went. Without a token there is nothing to read.
            None => Err(Error::Missing("umi warm needs publish.token".to_owned())),
        },
        Command::Block {
            domain,
            reason,
            lift,
            dir,
        } => {
            let config = load(command)?;
            block::run(&block::Options {
                dir: std::path::Path::new(dir),
                domain: domain.as_deref(),
                reason: reason.as_deref(),
                lift: *lift,
                token: token(&config)?,
                org: &config.org.value,
                now_ms: SystemClock.now_ms(),
            })
        }
        Command::Supervise {
            domain,
            reason,
            operator,
            remove,
            show,
            dir,
        } => {
            let config = load(command)?;
            supervise::run(&supervise::Options {
                dir: std::path::Path::new(dir),
                domain: domain.as_deref(),
                reason: reason.as_deref(),
                operator: operator.as_deref(),
                remove: *remove,
                show: *show,
                token: token(&config)?,
                org: &config.org.value,
                now_ms: SystemClock.now_ms(),
            })
        }
        Command::Watch { dir, publish } => {
            let config = load(command)?;
            let publishing = crawl::Publishing::resolve(&config, *publish)?;
            let identity = crawl::Identity::resolve(&config)?;
            finish(crawl::resume(
                std::path::Path::new(dir),
                true,
                publishing,
                identity,
            ))
        }
        other => Err(not_built(other)),
    }
}

/// Turn a summary into doc 14.9's exit code.
///
/// A crawl that stopped on a budget is exit 4 and not exit 0, because doc 14.9
/// is explicit that a script has to be able to tell "finished, nothing left to
/// crawl" from "stopped early, there is more", and those are the only two ways
/// a successful crawl ends.
fn finish(result: Result<crawl::Summary, Error>) -> Result<(), Error> {
    let summary = result?;
    println!(
        "{} rows in {} files, {} pages fetched, {} failed",
        summary.rows, summary.files, summary.fetched, summary.failed
    );
    match summary.stopped {
        crawl::Stop::Budget => Err(Error::Budget),
        // An interrupt is exit 0 with the rest, because the operator asked it
        // to stop and it stopped, having written everything it had.
        crawl::Stop::Idle | crawl::Stop::Signal => Ok(()),
    }
}

fn load(command: &Command) -> Result<config::Config, Error> {
    let cwd = std::env::current_dir().map_err(Error::Io)?;
    let config = config::Config::load(
        &config::Paths::discover(&cwd),
        &config::env_from_process(),
        &command.flags(),
    )?;
    for secret in [&config.token, &config.key, &config.identity_key]
        .into_iter()
        .flatten()
    {
        if let Some(warning) = secret.value.warning() {
            eprintln!("umi: {} says {warning}", secret.origin);
        }
    }
    Ok(config)
}

/// The Hugging Face token, read out of wherever doc 14.7 says it lives, or
/// nothing when none is configured.
///
/// Nothing is the normal answer for the two commands that call this. Everything
/// this project publishes is public, so `umi ls` and `umi verify` both work
/// against it with no credential at all, and that is the point of them.
fn token(config: &config::Config) -> Result<Option<String>, Error> {
    match &config.token {
        Some(secret) => Ok(Some(secret.value.read()?)),
        None => Ok(None),
    }
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
    // Never the value. The whole reason `Secret` is a type is that a command
    // whose job is printing configuration must not print these two. The
    // indirection is printed with whether it currently resolves, which is the
    // question somebody running `umi config` is actually asking.
    for (name, secret) in [
        ("publish.token", &config.token),
        ("publish.key", &config.key),
        ("crawl.identity_key", &config.identity_key),
    ] {
        match secret {
            Some(found) => {
                let resolves = match found.value.read() {
                    Ok(_) => "resolves",
                    Err(_) => "does not resolve",
                };
                line(
                    name,
                    match &found.value {
                        config::Secret::Env(var) => format!("env:{var}, {resolves}"),
                        config::Secret::File(path) => {
                            format!("file:{}, {resolves}", path.display())
                        }
                        config::Secret::Literal(_) => "a literal, not shown".to_owned(),
                    },
                    &found.origin,
                );
            }
            None => line(name, "unset".to_owned(), &config::Origin::Default),
        }
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
    line(
        "render.tabs",
        config.tabs.value.to_string(),
        &config.tabs.origin,
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
        Command::Manifest { .. } => {
            "reading a manifest chain back is milestone 2, and umi verify checks one today"
        }
        Command::Scope { .. } => "milestone 2 builds this",
        Command::State { .. } | Command::Checkpoint { .. } | Command::Sql { .. } => {
            "milestone 3 builds this"
        }
        _ => "milestone 4 builds this, when there is a fleet to talk to",
    })
}
