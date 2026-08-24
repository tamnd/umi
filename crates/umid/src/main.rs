//! The umi coordinator daemon.
//!
//! `umid` owns the state store, serves the `umi/1` fetch protocol, runs the
//! writer and the publisher, and hosts the local fetcher pool. The process
//! model is in `docs/spec/03-architecture.md` and the deployment is in
//! `docs/spec/15-operations.md`.
//!
//! It takes almost no flags on purpose. A daemon configured by command line is
//! a daemon that ends up configured differently on each of three boxes.

use std::process::ExitCode;

use clap::Parser;
use umi_types::Exit;

/// The umi coordinator daemon.
#[derive(Parser)]
#[command(name = "umid", version, about, long_about = None)]
struct Args {
    /// The configuration file.
    #[arg(long, default_value = "/etc/umi/umid.toml")]
    config: String,

    /// Validate the configuration, resolve peers, open the state store read
    /// only, check disk headroom and clock skew, then exit.
    ///
    /// This is what the systemd unit runs as `ExecStartPre`, and it is the
    /// difference between a bad config being caught in one second and being
    /// caught after the crawl has been down for an hour.
    #[arg(long)]
    check: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("UMI_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!(config = %args.config, check = args.check, "umid starting");
    tracing::error!("umid is specified but not built yet: see docs/spec/16-roadmap.md milestone 1");

    ExitCode::from(Exit::Failure)
}
