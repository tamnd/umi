//! Doc 14.7's configuration, with the source of every value kept.
//!
//! The precedence is flags, then `UMI_*` environment variables, then
//! `./umi.toml`, then `~/.config/umi/config.toml`, then built in defaults. That
//! is five layers, and five layers is exactly the number at which "why is this
//! setting not taking effect" becomes a real question, so every resolved value
//! carries where it came from and `umi config` prints it.
//!
//! Nothing here reads a clock or the network. Loading is a pure function of the
//! two file paths, the environment and the flags, which is what lets the tests
//! drive it from a temporary directory.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Where a resolved value came from. Ordered highest precedence first, which is
/// also the order doc 14.7 lists them in.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Origin {
    /// A command line flag.
    Flag,
    /// A `UMI_*` environment variable.
    Env(String),
    /// A TOML file, named so that the message says which one.
    File(PathBuf),
    /// Nothing said otherwise.
    Default,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Flag => f.write_str("flag"),
            Self::Env(name) => write!(f, "${name}"),
            Self::File(path) => write!(f, "{}", path.display()),
            Self::Default => f.write_str("default"),
        }
    }
}

/// A value and the reason it has that value.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Sourced<T> {
    /// The value itself.
    pub value: T,
    /// Which layer won.
    pub origin: Origin,
}

impl<T> Sourced<T> {
    fn new(value: T, origin: Origin) -> Self {
        Self { value, origin }
    }
}

/// A secret, which doc 14.7 says is never literal in a config file.
///
/// `env:NAME` reads an environment variable and `file:/path` reads a file. A
/// bare string is accepted, because refusing it would mean somebody's crawl
/// stops working on an upgrade, but it warns on every run until it is fixed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Secret {
    /// Read from the named environment variable at use time.
    Env(String),
    /// Read from the named file at use time, with trailing whitespace trimmed.
    File(PathBuf),
    /// Written out in the config file, which is the case that warns.
    Literal(String),
}

impl Secret {
    /// Parse the `env:` and `file:` prefixes doc 14.7 defines.
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        if let Some(name) = raw.strip_prefix("env:") {
            Self::Env(name.to_owned())
        } else if let Some(path) = raw.strip_prefix("file:") {
            Self::File(PathBuf::from(path))
        } else {
            Self::Literal(raw.to_owned())
        }
    }

    /// Read the secret. Errors carry the indirection that failed rather than
    /// the secret, which is the whole reason this is a type and not a String.
    ///
    /// # Errors
    ///
    /// When the variable is unset or the file cannot be read.
    pub fn read(&self) -> Result<String, Error> {
        match self {
            Self::Env(name) => std::env::var(name).map_err(|_| Error::SecretEnv(name.clone())),
            Self::File(path) => std::fs::read_to_string(path)
                .map(|body| body.trim_end().to_owned())
                .map_err(|cause| Error::SecretFile(path.clone(), cause)),
            Self::Literal(value) => Ok(value.clone()),
        }
    }

    /// The warning doc 14.7 asks for, or nothing when the secret is indirect.
    #[must_use]
    pub fn warning(&self) -> Option<&'static str> {
        match self {
            Self::Literal(_) => {
                Some("a literal secret in a config file: use env:NAME or file:/path instead")
            }
            _ => None,
        }
    }
}

/// What went wrong loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A config file could not be read.
    #[error("cannot read {0}: {1}")]
    Read(PathBuf, #[source] std::io::Error),
    /// A config file was not valid TOML, or had a field of the wrong type.
    #[error("{0} is not valid configuration: {1}")]
    Parse(PathBuf, String),
    /// An environment variable held something that is not a number.
    #[error("${0} is not a {1}: {2:?}")]
    EnvType(String, &'static str, String),
    /// A secret pointed at an environment variable that is not set.
    #[error("${0} is not set, and it is where a secret was supposed to come from")]
    SecretEnv(String),
    /// A secret pointed at a file that could not be read.
    #[error("cannot read the secret in {0}: {1}")]
    SecretFile(PathBuf, #[source] std::io::Error),
}

/// One TOML file, every field optional, because a file that sets one thing is
/// the normal case and a missing field means "ask the next layer down".
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    crawl: CrawlFile,
    #[serde(default)]
    state: StateFile,
    #[serde(default)]
    publish: PublishFile,
    #[serde(default)]
    fetch: FetchFile,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CrawlFile {
    rps: Option<f32>,
    concurrency: Option<u16>,
    tier_max: Option<u8>,
    out: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateFile {
    backend: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishFile {
    org: Option<String>,
    token: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchFile {
    coordinator: Option<String>,
    rate: Option<f32>,
}

/// The flags that participate in configuration, as options, so that "the user
/// did not say" is distinguishable from "the user said the default".
#[derive(Default)]
pub struct Flags {
    /// `--rps`.
    pub rps: Option<f32>,
    /// `--concurrency`.
    pub concurrency: Option<u16>,
    /// `--tier`.
    pub tier_max: Option<u8>,
    /// `--out`.
    pub out: Option<String>,
    /// `--state`.
    pub backend: Option<String>,
    /// `--coordinator`.
    pub coordinator: Option<String>,
    /// `--rate`.
    pub rate: Option<f32>,
}

/// The effective configuration, with every value's origin.
#[derive(Debug)]
pub struct Config {
    /// Requests a second per host, before doc 07's politeness clamps it.
    pub rps: Sourced<f32>,
    /// Simultaneous in flight fetches.
    pub concurrency: Sourced<u16>,
    /// Highest tier the ladder is allowed to reach.
    pub tier_max: Sourced<u8>,
    /// Where crawl directories go.
    pub out: Sourced<String>,
    /// Which state backend to open.
    pub backend: Sourced<String>,
    /// The Hugging Face organisation doc 12.4 publishes into.
    pub org: Sourced<String>,
    /// The publishing token, still indirect.
    pub token: Option<Sourced<Secret>>,
    /// The coordinator `umi fetch` leases from.
    pub coordinator: Sourced<String>,
    /// Pages a second a fetcher offers.
    pub rate: Sourced<f32>,
    /// The files that were actually read, in precedence order, for `umi config`.
    pub files: Vec<PathBuf>,
}

/// Where the two config files live, kept as a struct so the tests can point it
/// somewhere that is not the developer's home directory.
pub struct Paths {
    /// `./umi.toml`, or wherever the caller says the working directory is.
    pub local: PathBuf,
    /// `~/.config/umi/config.toml`.
    pub user: Option<PathBuf>,
}

impl Paths {
    /// The real locations doc 14.7 names.
    #[must_use]
    pub fn discover(cwd: &Path) -> Self {
        // `HOME` rather than a home directory crate. The crate would pull in a
        // dependency to read the same variable on every platform we build for,
        // and on Windows the tests set it anyway.
        let user = std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(".config").join("umi").join("config.toml"));
        Self {
            local: cwd.join("umi.toml"),
            user,
        }
    }
}

/// The environment, as a map, so that a test does not have to mutate the
/// process environment and race every other test in the binary.
pub type Env = BTreeMap<String, String>;

/// Read the environment into the form [`Config::load`] wants.
#[must_use]
pub fn env_from_process() -> Env {
    std::env::vars()
        .filter(|(name, _)| name.starts_with("UMI_"))
        .collect()
}

impl Config {
    /// Resolve every value across the five layers.
    ///
    /// # Errors
    ///
    /// When a file exists and does not parse, or a `UMI_*` variable holds
    /// something of the wrong type. A missing file is not an error, because
    /// the overwhelmingly common case is having neither.
    pub fn load(paths: &Paths, env: &Env, flags: &Flags) -> Result<Self, Error> {
        let local = read(&paths.local)?;
        let user = match &paths.user {
            Some(path) => read(path)?,
            None => None,
        };

        let mut files = Vec::new();
        if local.is_some() {
            files.push(paths.local.clone());
        }
        if user.is_some()
            && let Some(path) = &paths.user
        {
            files.push(path.clone());
        }

        let layers = Layers {
            local: local.map(|file| (paths.local.clone(), file)),
            user: user
                .zip(paths.user.clone())
                .map(|(file, path)| (path, file)),
            env,
        };

        Ok(Self {
            rps: layers.number(flags.rps, "UMI_RPS", "number", |f| f.crawl.rps, 1.0)?,
            concurrency: layers.number(
                flags.concurrency,
                "UMI_CONCURRENCY",
                "whole number",
                |f| f.crawl.concurrency,
                4,
            )?,
            tier_max: layers.number(
                flags.tier_max,
                "UMI_TIER",
                "whole number",
                |f| f.crawl.tier_max,
                3,
            )?,
            out: layers.text(flags.out.clone(), "UMI_OUT", |f| f.crawl.out.clone(), ".")?,
            backend: layers.text(
                flags.backend.clone(),
                "UMI_STATE",
                |f| f.state.backend.clone(),
                "sqlite",
            )?,
            org: layers.text(None, "UMI_ORG", |f| f.publish.org.clone(), "open-index")?,
            token: layers
                .optional_text("UMI_TOKEN", |f| f.publish.token.clone())
                .map(|found| Sourced::new(Secret::parse(&found.value), found.origin)),
            coordinator: layers.text(
                flags.coordinator.clone(),
                "UMI_COORDINATOR",
                |f| f.fetch.coordinator.clone(),
                "https://umi.dev",
            )?,
            rate: layers.number(flags.rate, "UMI_RATE", "number", |f| f.fetch.rate, 2.0)?,
            files,
        })
    }
}

fn read(path: &Path) -> Result<Option<FileConfig>, Error> {
    let body = match std::fs::read_to_string(path) {
        Ok(body) => body,
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(cause) => return Err(Error::Read(path.to_owned(), cause)),
    };
    toml::from_str(&body)
        .map(Some)
        .map_err(|cause| Error::Parse(path.to_owned(), cause.message().to_owned()))
}

struct Layers<'a> {
    local: Option<(PathBuf, FileConfig)>,
    user: Option<(PathBuf, FileConfig)>,
    env: &'a Env,
}

impl Layers<'_> {
    /// Walk the two files in precedence order, local first.
    fn in_files<T>(&self, get: impl Fn(&FileConfig) -> Option<T>) -> Option<Sourced<T>> {
        for layer in [&self.local, &self.user] {
            if let Some((path, file)) = layer
                && let Some(value) = get(file)
            {
                return Some(Sourced::new(value, Origin::File(path.clone())));
            }
        }
        None
    }

    fn number<T: std::str::FromStr>(
        &self,
        flag: Option<T>,
        var: &str,
        kind: &'static str,
        get: impl Fn(&FileConfig) -> Option<T>,
        fallback: T,
    ) -> Result<Sourced<T>, Error> {
        if let Some(value) = flag {
            return Ok(Sourced::new(value, Origin::Flag));
        }
        if let Some(raw) = self.env.get(var) {
            let value = raw
                .parse()
                .map_err(|_| Error::EnvType(var.to_owned(), kind, raw.clone()))?;
            return Ok(Sourced::new(value, Origin::Env(var.to_owned())));
        }
        Ok(self
            .in_files(get)
            .unwrap_or_else(|| Sourced::new(fallback, Origin::Default)))
    }

    fn text(
        &self,
        flag: Option<String>,
        var: &str,
        get: impl Fn(&FileConfig) -> Option<String>,
        fallback: &str,
    ) -> Result<Sourced<String>, Error> {
        if let Some(value) = flag {
            return Ok(Sourced::new(value, Origin::Flag));
        }
        Ok(self
            .optional_text(var, get)
            .unwrap_or_else(|| Sourced::new(fallback.to_owned(), Origin::Default)))
    }

    fn optional_text(
        &self,
        var: &str,
        get: impl Fn(&FileConfig) -> Option<String>,
    ) -> Option<Sourced<String>> {
        if let Some(raw) = self.env.get(var) {
            return Some(Sourced::new(raw.clone(), Origin::Env(var.to_owned())));
        }
        self.in_files(get)
    }
}
