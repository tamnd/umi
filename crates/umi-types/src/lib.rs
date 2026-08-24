//! Keys, digests and exit codes shared by every umi crate.
//!
//! This is the one crate everything else depends on, so it holds no I/O, no
//! async and no configuration. The rule in `docs/spec/03-architecture.md` is
//! that a change here should not require touching six crates, and the way to
//! keep that true is to keep the surface small.
//!
//! The key types come from `docs/spec/08-state-layer.md` and the exit codes
//! from `docs/spec/14-cli.md`.

use core::fmt;

pub mod canon;

pub use canon::{CanonError, canonicalize, pay_level_domain};

/// The canonicalisation version these keys are derived under.
///
/// Changing URL canonicalisation changes every key in the system, so it is
/// versioned rather than patched. See `docs/spec/11-extraction-and-dedup.md`
/// section 11.2, which also explains why the version is recorded in every
/// segment header and every state checkpoint.
pub const CANON_VERSION: &str = "canon/1";

/// The format and protocol generation this build speaks.
pub const PROTOCOL_VERSION: u32 = 1;

macro_rules! fixed_key {
    ($name:ident, $len:expr, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name([u8; $len]);

        impl $name {
            /// The width of this key in bytes.
            pub const LEN: usize = $len;

            /// Wrap raw bytes that are already a key.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; $len]) -> Self {
                Self(bytes)
            }

            /// Borrow the key as bytes, in the order it sorts in.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $len] {
                &self.0
            }

            /// Derive a key by truncating a blake3 digest of `input`.
            ///
            /// Truncation is safe here because blake3 output is uniform, so
            /// the first `LEN` bytes are as good as any other `LEN`.
            #[must_use]
            pub fn derive(input: &[u8]) -> Self {
                let full = blake3::hash(input);
                let mut out = [0u8; $len];
                out.copy_from_slice(&full.as_bytes()[..$len]);
                Self(out)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in &self.0 {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }
    };
}

fixed_key!(
    PldId,
    8,
    "Identifier for a pay level domain, the unit of partitioning and politeness.\n\nEvery URL under `example.co.uk` shares one `PldId`, which is what lets a\nsite's politeness timer, robots cache and tier policy live on exactly one\ncoordinator. See `docs/spec/03-architecture.md` section 3.3."
);

fixed_key!(
    HostId,
    8,
    "Identifier for one host, which is the unit rate limiting applies to.\n\nA `PldId` can cover thousands of `HostId`s, and\n`docs/spec/07-politeness-and-identity.md` caps the whole pay level domain\nas well as each host so that a site with 5000 subdomains does not get 5000\ntimes the traffic."
);

fixed_key!(
    UrlKey,
    10,
    "The 80 bit fingerprint of a canonical URL, and the seen set's key.\n\nEighty bits gives 0.004 expected collisions at 100 billion URLs, against\n271 for 64 bits, which is the arithmetic in `docs/spec/08-state-layer.md`.\nA collision in the seen set is silent, so anything already fetched also\ncarries a [`UrlKeyFull`] that makes one detectable."
);

fixed_key!(
    UrlKeyFull,
    16,
    "The 128 bit fingerprint carried by rows we have actually fetched.\n\nIts only job is to make a [`UrlKey`] collision detectable rather than\nsilent. See `docs/spec/17-open-questions.md` section 17.3."
);

fixed_key!(
    Digest,
    32,
    "A full blake3-256 digest: response bodies, extractions, chunk roots and\nmanifest entries all use this.\n\nThe rule from `docs/spec/17-open-questions.md` section 17.4 is that digests\nare taken over logical values and never over encoded bytes, except when\nchecking that one specific file arrived intact."
);

/// The identity of a fetcher: its ed25519 public key, as it appears in
/// `docs/spec/04-fetch-protocol.md`.
///
/// This is not derived from anything, which is why it is spelled out here
/// rather than built with the same macro as the key types. A fetcher chooses
/// its own keypair, the coordinator learns the public half at handshake, and
/// every lease, receipt and published row carries it so that work can be
/// attributed and reputation can be kept per doc 06.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct FetcherId([u8; 32]);

impl FetcherId {
    /// The width of a fetcher id in bytes.
    pub const LEN: usize = 32;

    /// The coordinator's own id, used for work it fetches itself.
    ///
    /// An all zero key is not a valid ed25519 public key, so nothing that
    /// completes a handshake can ever collide with it.
    pub const LOCAL: Self = Self([0u8; 32]);

    /// Wrap a public key that has already been decoded.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the public key.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Whether this is the coordinator itself rather than a remote fetcher.
    #[must_use]
    pub const fn is_local(&self) -> bool {
        // Written as a loop because array equality is not const yet, and this
        // being const is what lets a match arm compare against `LOCAL`.
        let mut i = 0;
        while i < Self::LEN {
            if self.0[i] != 0 {
                return false;
            }
            i += 1;
        }
        true
    }
}

impl fmt::Display for FetcherId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for FetcherId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_local() {
            f.write_str("FetcherId(local)")
        } else {
            write!(f, "FetcherId({self})")
        }
    }
}

/// The fetch ladder from `docs/spec/05-anti-bot-ladder.md` section 5.2.
///
/// The ordering is the cost ordering, so `<` really does mean cheaper, and the
/// escalation rules in 5.8 are written as comparisons against it. Tiers live
/// here rather than in `umi-fetch` because the state layer stores the tier a
/// host prefers, the protocol negotiates which tiers a fetcher can run, and
/// the file format records the tier a page came from.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(u8)]
pub enum Tier {
    /// Conditional request against a known revalidator. No body if it holds.
    Revalidate = 0,
    /// Plain HTTP with an honest identity. The default and the bulk of the
    /// crawl.
    #[default]
    Plain = 1,
    /// A matched browser TLS and HTTP/2 fingerprint, for hosts whose bot
    /// management refuses a non browser stack.
    Emulated = 2,
    /// Headless Chromium, for pages that are a client rendered shell.
    Rendered = 3,
    /// A supervised real browser. Allowlisted, opt in, never dispatched to a
    /// fetcher that did not ask for it.
    Supervised = 4,
}

impl Tier {
    /// Every tier, cheapest first.
    pub const ALL: [Self; 5] = [
        Self::Revalidate,
        Self::Plain,
        Self::Emulated,
        Self::Rendered,
        Self::Supervised,
    ];

    /// Recover a tier from the byte a stored row or a protocol frame holds.
    #[must_use]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Revalidate),
            1 => Some(Self::Plain),
            2 => Some(Self::Emulated),
            3 => Some(Self::Rendered),
            4 => Some(Self::Supervised),
            _ => None,
        }
    }

    /// The byte a stored row or a protocol frame holds.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// The next tier up, or `None` at the top of the ladder.
    #[must_use]
    pub const fn escalate(self) -> Option<Self> {
        Self::from_u8(self as u8 + 1)
    }

    /// The next tier down, or `None` at the bottom.
    #[must_use]
    pub const fn de_escalate(self) -> Option<Self> {
        match self as u8 {
            0 => None,
            n => Self::from_u8(n - 1),
        }
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Revalidate => "T0",
            Self::Plain => "T1",
            Self::Emulated => "T2",
            Self::Rendered => "T3",
            Self::Supervised => "T4",
        })
    }
}

/// The ordering the state layer stores rows in.
///
/// Sorting by pay level domain, then host, then URL puts everything a
/// scheduler reads together next to each other on disk, which is what makes
/// the columnar ledger in `docs/spec/08-state-layer.md` worth having.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct RowKey {
    /// The owning pay level domain.
    pub pld: PldId,
    /// The host within that domain.
    pub host: HostId,
    /// The URL fingerprint.
    pub url: UrlKey,
}

impl RowKey {
    /// Derive the three keys for a URL, canonicalising it first.
    ///
    /// This is the only sanctioned way to make a `RowKey`, because a key
    /// derived from a URL that skipped canonicalisation is a key nothing else
    /// in the system will ever match.
    ///
    /// # Errors
    ///
    /// Returns [`CanonError`] when the URL is not a crawlable http(s) URL.
    pub fn for_url(url: &str, base: Option<&str>) -> Result<Self, CanonError> {
        let canonical = canonicalize(url, base)?;
        let host = host_of(&canonical).ok_or(CanonError::NoHost)?;
        Ok(Self {
            pld: PldId::derive(pay_level_domain(host).as_bytes()),
            host: HostId::derive(host.as_bytes()),
            url: UrlKey::derive(canonical.as_bytes()),
        })
    }
}

/// The host of an already canonical URL, found by slicing rather than
/// reparsing. Canonical form is `scheme://host[:port]/...` with no userinfo,
/// which makes this exact.
fn host_of(canonical: &str) -> Option<&str> {
    let after_scheme = canonical.split_once("://")?.1;
    let authority = after_scheme.split(['/', '?']).next()?;
    Some(match authority.rsplit_once(':') {
        Some((host, port)) if port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => authority,
    })
}

/// Process exit codes, as defined in `docs/spec/14-cli.md` section 14.9.
///
/// [`Self::NothingToDo`] and [`Self::BudgetExhausted`] are separate on
/// purpose. "Finished, there was nothing to crawl" and "stopped early, there
/// is more" are different outcomes and a script needs to tell them apart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Exit {
    /// Everything asked for was done.
    Success = 0,
    /// Something failed and no more specific code applies.
    Failure = 1,
    /// The command line or configuration was wrong.
    Usage = 2,
    /// The scope was empty, robots disallowed everything, or the frontier
    /// drained with nothing left to do.
    NothingToDo = 3,
    /// A page, byte or time budget was reached while work remained.
    BudgetExhausted = 4,
    /// The network failed and the retries were used up.
    Network = 5,
    /// A digest, signature or manifest did not check out. Never retried
    /// automatically, because it is either corruption or a bug.
    Verification = 6,
    /// The disk filled, publishing stalled, or the daemon refused to proceed.
    /// See the backpressure ladder in `docs/spec/15-operations.md`.
    Resource = 7,
}

impl From<Exit> for std::process::ExitCode {
    fn from(exit: Exit) -> Self {
        Self::from(exit as u8)
    }
}

impl fmt::Display for Exit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Usage => "usage error",
            Self::NothingToDo => "nothing to do",
            Self::BudgetExhausted => "budget exhausted",
            Self::Network => "network failure",
            Self::Verification => "verification failure",
            Self::Resource => "resource pressure",
        };
        f.write_str(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_stable_across_runs() {
        let a = UrlKey::derive(b"https://example.com/");
        let b = UrlKey::derive(b"https://example.com/");
        assert_eq!(a, b);
        assert_eq!(a.to_string().len(), UrlKey::LEN * 2);
    }

    #[test]
    fn keys_are_a_prefix_of_the_full_digest() {
        // The seen set stores 80 bits and a fetched row stores 128, and both
        // come off the same hash. If that stopped being true, a collision
        // check against UrlKeyFull would compare two unrelated numbers.
        let url = b"https://example.com/a";
        let short = UrlKey::derive(url);
        let long = UrlKeyFull::derive(url);
        assert_eq!(&long.as_bytes()[..UrlKey::LEN], short.as_bytes());
    }

    #[test]
    fn the_tier_ladder_is_ordered_by_cost() {
        // Doc 05.8 writes escalation as "start at preferred, stop at max", so
        // the comparison operators have to mean what that sentence assumes.
        assert!(Tier::Revalidate < Tier::Plain);
        assert!(Tier::Plain < Tier::Emulated);
        assert!(Tier::Emulated < Tier::Rendered);
        assert!(Tier::Rendered < Tier::Supervised);
        assert_eq!(Tier::default(), Tier::Plain);
    }

    #[test]
    fn a_tier_round_trips_through_the_byte_a_row_stores() {
        for tier in Tier::ALL {
            assert_eq!(Tier::from_u8(tier.as_u8()), Some(tier));
        }
        assert_eq!(Tier::from_u8(5), None);
        assert_eq!(Tier::Supervised.escalate(), None);
        assert_eq!(Tier::Revalidate.de_escalate(), None);
        assert_eq!(Tier::Plain.de_escalate(), Some(Tier::Revalidate));
    }

    #[test]
    fn the_local_fetcher_id_cannot_be_a_real_key() {
        // An all zero ed25519 public key is not on the curve, so no fetcher
        // that finished a handshake can present one.
        assert!(FetcherId::LOCAL.is_local());
        assert!(!FetcherId::from_bytes([1u8; 32]).is_local());
        assert_eq!(format!("{:?}", FetcherId::LOCAL), "FetcherId(local)");
        assert_eq!(FetcherId::from_bytes([1u8; 32]).to_string().len(), 64);
    }

    #[test]
    fn distinct_urls_get_distinct_keys() {
        assert_ne!(
            UrlKey::derive(b"https://example.com/a"),
            UrlKey::derive(b"https://example.com/b")
        );
    }

    #[test]
    fn row_keys_group_by_domain_before_anything_else() {
        // Ordering is over the key bytes, which are hash output, so it is not
        // alphabetical by domain and nothing should ever assume it is. What
        // the ledger needs is only that one domain's rows land contiguously
        // and that host is the tiebreak within it.
        let pld = PldId::derive(b"example.com");
        let other = PldId::derive(b"other.com");
        let host_a = HostId::derive(b"a.example.com");
        let host_b = HostId::derive(b"b.example.com");
        let (first_host, second_host) = if host_a < host_b {
            (host_a, host_b)
        } else {
            (host_b, host_a)
        };

        let mut rows = [
            RowKey {
                pld: other,
                host: HostId::derive(b"www.other.com"),
                url: UrlKey::derive(b"https://www.other.com/"),
            },
            RowKey {
                pld,
                host: second_host,
                url: UrlKey::derive(b"https://second.example.com/"),
            },
            RowKey {
                pld,
                host: first_host,
                url: UrlKey::derive(b"https://first.example.com/"),
            },
        ];
        rows.sort();

        let group: Vec<_> = rows.iter().filter(|r| r.pld == pld).collect();
        assert_eq!(group.len(), 2);
        assert_eq!(group[0].host, first_host);
        assert!(rows.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn exit_codes_match_the_spec() {
        assert_eq!(Exit::Success as u8, 0);
        assert_eq!(Exit::Usage as u8, 2);
        assert_eq!(Exit::NothingToDo as u8, 3);
        assert_eq!(Exit::BudgetExhausted as u8, 4);
        assert_eq!(Exit::Verification as u8, 6);
        assert_eq!(Exit::Resource as u8, 7);
    }

    #[test]
    fn hex_rendering_is_lowercase_and_padded() {
        let key = PldId::from_bytes([0x00, 0x0f, 0xff, 0x10, 0x00, 0x00, 0x00, 0x01]);
        assert_eq!(key.to_string(), "000fff1000000001");
    }
}
