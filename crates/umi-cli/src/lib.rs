//! The commands behind the `umi` binary.
//!
//! The surface is specified in `docs/spec/14-cli.md`. The parser lives in the
//! binary and everything it calls lives here, which is what lets the tests and
//! the bench drive a command without spawning a process and parsing its output.
//!
//! Two rules hold across every command in here. Rows go to stdout and
//! everything else goes to stderr, so `umi cat ... | head` and
//! `umi get --markdown ... > page.md` both give a clean file. And nothing
//! returns a bare failure: every error path picks one of doc 14.9's exit codes,
//! in [`Error::exit`].

pub mod addresses;
pub mod bandwidth;
pub mod block;
pub mod cards;
pub mod config;
pub mod crawl;
pub mod doctor;
mod error;
pub mod evict;
pub mod exporter;
pub mod get;
pub mod inspect;
pub mod rdns;
pub mod robots;
pub mod supervise;
pub mod verify;

#[cfg(test)]
mod evict_tests;
#[cfg(test)]
mod tests;

pub use error::Error;
