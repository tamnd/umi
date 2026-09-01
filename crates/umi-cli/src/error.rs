//! One error type for the whole command line, and the map from it onto doc
//! 14.9's exit codes.
//!
//! The map is the point. Doc 14.9 says exit 3 and exit 4 are separate on
//! purpose, that exit 6 is never retried automatically, and that a script needs
//! to tell "finished, nothing to crawl" from "stopped early, there is more".
//! None of that is true unless every failure path decides which code it is, so
//! every variant here answers that question in one place rather than at each
//! call site.

use umi_types::Exit;

/// Anything the command line can fail with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Configuration would not load.
    #[error(transparent)]
    Config(#[from] crate::config::Error),

    /// A file would not open, read or write.
    #[error("{0}")]
    Io(#[source] std::io::Error),

    /// A `.umi` segment would not open or decode.
    #[error(transparent)]
    Segment(#[from] umi_file::Error),

    /// A Parquet file would not open or decode.
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow refused a batch, which in practice means a projection that does
    /// not match the schema.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// A column was asked for by name and the file does not have it.
    #[error("no column named {0:?} in this file")]
    NoColumn(String),

    /// The URL will not parse.
    #[error("{0:?} is not a URL")]
    BadUrl(String),

    /// The fetcher would not build, which is a configuration problem and not a
    /// network one.
    #[error(transparent)]
    Fetcher(#[from] umi_fetch::FetchError),

    /// A fetch happened and did not produce a page.
    #[error("fetch failed: {0}")]
    Fetch(String),

    /// There was nothing there. Doc 14.9's exit 3.
    #[error("nothing to list")]
    Empty,

    /// Everything was in order and there was no work left. Doc 14.9's exit 3
    /// as well, and separate from [`Error::Empty`] only so the message can say
    /// what there was none of. A script reads the code either way.
    #[error("{0}")]
    NothingToDo(&'static str),

    /// Some files in a listing could not be read. Doc 14.9's exit 6, because a
    /// segment that will not open is either corruption or a bug.
    #[error("{0} files could not be read")]
    Unreadable(usize),

    /// A scope would not build from a target, a profile or a flag.
    #[error("{0}")]
    Scope(String),

    /// A flag asked for something that configuration does not say how to do.
    /// The message names the setting and how to set it, because "missing
    /// configuration" on its own sends people to the source.
    #[error("{0}")]
    Missing(String),

    /// The state backend refused. Doc 14.9 calls this a general failure and
    /// not a usage error, because by the time a crawl is running the operator
    /// has already been told whether their arguments made sense.
    #[error("state: {0}")]
    State(String),

    /// The crawl loop stopped on something that was not a fetch.
    #[error("crawl: {0}")]
    Crawl(String),

    /// Doc 12's pipeline refused. Kept whole rather than flattened to a string
    /// like the two above, because the exit code depends on which half of it
    /// failed: a hub that will not answer is worth retrying and a copy that
    /// does not check out is not.
    #[error("publish: {0}")]
    Publish(#[from] umi_publish::Error),

    /// A budget in doc 13.2 was reached. Doc 14.9's exit 4, which a script
    /// reads as "stopped early, there is more" rather than as a failure.
    #[error("budget exhausted")]
    Budget,

    /// A `doctor` check came back bad.
    #[error("this machine is not ready: see the report above")]
    NotReady,

    /// The metrics listener would not bind.
    ///
    /// Its own variant rather than an `Io`, because the thing an operator
    /// needs to read is the address, and because this is the one failure in a
    /// crawl that happens before any work and is entirely about this box.
    #[error("metrics: {0}")]
    Metrics(String),

    /// The command is in doc 14 and is not built yet.
    #[error("{0}: see docs/spec/16-roadmap.md")]
    NotBuilt(&'static str),
}

impl Error {
    /// Which of doc 14.9's eight codes this is.
    #[must_use]
    pub fn exit(&self) -> Exit {
        match self {
            // A bad flag, a bad config file or a column that does not exist is
            // the operator having typed something wrong, which is exit 2 and
            // not a failure of the run.
            Self::Config(_)
            | Self::NoColumn(_)
            | Self::BadUrl(_)
            | Self::Scope(_)
            | Self::Missing(_) => Exit::Usage,
            Self::Budget => Exit::BudgetExhausted,
            Self::Empty | Self::NothingToDo(_) => Exit::NothingToDo,
            Self::Fetch(_) => Exit::Network,
            // Doc 14.9: a digest or a file that will not decode is either
            // corruption or a bug, and both deserve a human rather than a
            // retry loop.
            Self::Unreadable(_) | Self::Segment(_) | Self::Parquet(_) => Exit::Verification,
            Self::Publish(cause) => publishing(cause),
            // A port already in use or an address this box does not have. Doc
            // 14.9's exit 7 is about the machine rather than the command, and
            // both of those are the machine.
            Self::NotReady | Self::Metrics(_) => Exit::Resource,
            Self::Io(_)
            | Self::Arrow(_)
            | Self::Fetcher(_)
            | Self::State(_)
            | Self::Crawl(_)
            | Self::NotBuilt(_) => Exit::Failure,
        }
    }
}

/// Which exit code a publishing failure is.
///
/// The split doc 14.9 cares about is whether a script should try again. A hub
/// that timed out or returned a 503 is exit 5 and a retry is the right move. A
/// signature that did not verify, a manifest that did not parse, or an upload
/// that landed and then digested differently is exit 6, and doc 14.9 says exit
/// 6 is never retried automatically, because retrying a verification failure
/// either loops forever or eventually succeeds by accident.
fn publishing(cause: &umi_publish::Error) -> Exit {
    use umi_publish::Error as Publish;
    match cause {
        Publish::Hub { .. } | Publish::Transport { .. } => Exit::Network,
        Publish::NotPublished(_)
        | Publish::BadSignature
        | Publish::Manifest(_)
        | Publish::RowCount { .. }
        | Publish::Segment(_)
        | Publish::Parquet(_) => Exit::Verification,
        // A missing token, an unusable key, a full disk. All of them are this
        // machine's configuration rather than the corpus, and none of them get
        // better by being run again.
        _ => Exit::Failure,
    }
}
