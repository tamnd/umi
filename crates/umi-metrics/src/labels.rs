//! Every label value this process can ever emit.
//!
//! The done-when on issue #44 is that a crawl of a million domains does not
//! produce a million label values, and this module is the whole answer. There
//! is no API anywhere in this crate that takes a label as a string. A label is
//! an enum, an enum has a fixed variant set, and the number of series a family
//! can grow to is `COUNT` multiplied out, known at compile time. Cardinality is
//! not something a reviewer has to check for, because there is no way to write
//! the unbounded version.
//!
//! [`crate::Peers`] is the single exception, and it is bounded by a cap rather
//! than by a type, because peer names are configured at runtime. Everything
//! else here is closed.
//!
//! # Why these mirror other crates instead of importing them
//!
//! Several of these enums have a near twin somewhere else. [`FrontierState`]
//! looks like `umi_state::UrlState`, [`RobotsResult`] looks like
//! `umi_robots::Provenance`, and [`Ladder`] looks like `umi_crawl::Ladder`.
//! They are written out again on purpose, for two reasons.
//!
//! The first is that this crate has to stay a leaf. Everything that produces a
//! number depends on it, so it can depend on almost nothing, and `umi-types` is
//! the only crate that is below everybody.
//!
//! The second is that a label set is the metric's vocabulary and not the
//! domain's. A dashboard, an alert rule and a year of stored samples are all
//! written against these strings, so renaming one is a breaking change of a
//! different kind than renaming a Rust variant. Pinning the label to a domain
//! enum would mean an ordinary refactor two crates away silently renames a
//! series and breaks a panel nobody was looking at. The mirror costs a `match`
//! at the call site and buys a vocabulary that only changes when somebody meant
//! to change it.

use umi_types::{OutcomeCode, Tier};

/// One dimension of a metric family.
///
/// Implemented for a closed enum and nothing else. `COUNT` sizes the family's
/// storage, `index` picks the member, and encoding walks `0..COUNT` and calls
/// `from_index` so that every series appears in a scrape whether or not it has
/// been touched. Absent series and zero series read the same to a human and
/// completely differently to a rate query.
pub trait Label: Copy {
    /// The label name, the part left of the equals sign.
    const KEY: &'static str;

    /// How many values there are.
    const COUNT: usize;

    /// The value at this index.
    ///
    /// # Panics
    ///
    /// If `index` is at or above `COUNT`. Only encoding calls this and it
    /// walks the range, so a panic here is a broken implementation of this
    /// trait rather than anything a caller did.
    fn from_index(index: usize) -> Self;

    /// Where this value's storage lives.
    fn index(self) -> usize;

    /// The value as it appears in the scrape.
    fn value(self) -> &'static str;
}

/// Declare a closed label enum and its [`Label`] impl.
macro_rules! label {
    (
        $(#[$outer:meta])*
        $name:ident as $key:literal {
            $( $(#[$inner:meta])* $variant:ident => $wire:literal ),+ $(,)?
        }
    ) => {
        $(#[$outer])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
        pub enum $name {
            $( $(#[$inner])* $variant, )+
        }

        impl $name {
            /// Every value, in index order.
            pub const ALL: &'static [Self] = &[ $( Self::$variant, )+ ];
        }

        impl Label for $name {
            const KEY: &'static str = $key;
            const COUNT: usize = $name::ALL.len();

            fn from_index(index: usize) -> Self {
                Self::ALL[index]
            }

            fn index(self) -> usize {
                self as usize
            }

            fn value(self) -> &'static str {
                match self {
                    $( Self::$variant => $wire, )+
                }
            }
        }
    };
}

label! {
    /// What the frontier thinks of a URL, for `umi_frontier_size`.
    ///
    /// Mirrors `umi_state::UrlState`. The two terminal states are here even
    /// though nothing schedules them, because the ratio of `gone` and
    /// `excluded` to the rest is how doc 08 says a frontier is filling up with
    /// rows it will never lease.
    FrontierState as "state" {
        /// Known and due, waiting for a fetcher.
        Pending => "pending",
        /// Fetched at least once, due again for refresh.
        Fetched => "fetched",
        /// The last attempt failed and it is in backoff.
        Failed => "failed",
        /// A 410. Terminal.
        Gone => "gone",
        /// Robots, block list or scope says no.
        Excluded => "excluded",
    }
}

label! {
    /// What happened to a candidate URL, for `umi_admit_total`.
    ///
    /// The four words are doc 15.4's, and the shape of the ratio between them
    /// is the discovery health check: `admitted` over `seen` falling means the
    /// crawl is going in circles, `held` climbing means doc 09's holding pen is
    /// backing up.
    AdmitResult as "result" {
        /// Offered to the state, whatever came of it.
        Seen => "seen",
        /// New, in scope and now in the frontier.
        Admitted => "admitted",
        /// Parked in the holding pen pending a reason to believe it.
        Held => "held",
        /// Refused by robots, the block list or the scope.
        Excluded => "excluded",
    }
}

label! {
    /// Which state operation was timed, for `umi_state_op_duration_seconds`.
    ///
    /// Nine rather than one per trait method. Doc 15.7's runbook asks which of
    /// the writes is slow, and the eighteen methods collapse into these without
    /// losing that: the ones that are one row lookups all behave the same and
    /// share `read`.
    StateOp as "op" {
        /// Offering candidate URLs.
        Admit => "admit",
        /// Taking a batch of work.
        Lease => "lease",
        /// Recording fetch outcomes.
        Complete => "complete",
        /// Handing a lease back unfetched.
        Release => "release",
        /// Recording a sealed or published segment.
        PutSegment => "put_segment",
        /// Pulling a shard into memory.
        Warm => "warm",
        /// Pushing a shard out of memory.
        Evict => "evict",
        /// Doc 08's durability point.
        Checkpoint => "checkpoint",
        /// Any of the single row or small list reads.
        Read => "read",
    }
}

label! {
    /// How a robots.txt fetch came out, for `umi_robots_fetch_total`.
    ///
    /// Mirrors `umi_robots::Provenance`. `server_error` climbing is the one to
    /// watch, because RFC 9309 makes a 5xx a full disallow and a site that is
    /// briefly unwell disappears from the crawl while it is.
    RobotsResult as "result" {
        /// A 2xx that parsed.
        Parsed => "parsed",
        /// A 4xx, which RFC 9309 reads as no restrictions.
        NotFound => "not_found",
        /// A 5xx, which RFC 9309 reads as fully disallowed.
        ServerError => "server_error",
        /// The fetch failed outright or redirected too far.
        Unreachable => "unreachable",
    }
}

label! {
    /// Which step of doc 12.2's pipeline, for the two publish families.
    PublishStep as "step" {
        /// Step 1, every chunk checksum and the row count.
        Verify => "verify",
        /// Step 2, shoals to Parquet row groups.
        Convert => "convert",
        /// Step 3, blake3 and sha256 over the Parquet file.
        Digest => "digest",
        /// Step 4, the upload to Hugging Face.
        Upload => "upload",
        /// Step 5, reading the remote copy back.
        Confirm => "confirm",
        /// Step 6, appending and signing the manifest entry.
        Manifest => "manifest",
        /// Step 7, writing remote locations into the ledger.
        Ledger => "ledger",
        /// Step 8, doc 12.7 deleting the local copies.
        Collect => "collect",
    }
}

label! {
    /// Which directory, for `umi_disk_free_bytes`.
    ///
    /// A role rather than a path. This is one of the two labels in doc 15.4
    /// that would otherwise be unbounded, and a crawler that is reconfigured a
    /// few times leaves a trail of dead series named after directories that no
    /// longer exist. The role is what an alert wants anyway: nobody writes a
    /// rule about `/var/lib/umi`, they write one about the disk the segments
    /// land on.
    ///
    /// All three can be the same filesystem and usually are, in which case
    /// three series carry the same number, which is cheap and honest.
    DiskRole as "path" {
        /// Where the state file or database lives.
        State => "state",
        /// Where sealed segments wait for the publisher.
        Segments => "segments",
        /// Where the publisher builds Parquet before uploading.
        Staging => "staging",
    }
}

label! {
    /// Which of doc 15.3's three ladders, for `umi_backpressure_level`.
    Ladder as "ladder" {
        /// Unpublished bytes, publish lag and free space.
        Disk => "disk",
        /// Extract queue depth and extractor saturation.
        Cpu => "cpu",
        /// Resident set against the budget.
        Memory => "memory",
    }
}

label! {
    /// What a connected fetcher is doing, for `umi_fetchers_connected`.
    ///
    /// Milestone 4's protocol may want more of these, and adding one is a
    /// compile error at every `match`, which is the point of the enum.
    FetcherState as "state" {
        /// Connected, key not yet checked.
        Handshaking => "handshaking",
        /// Checked and holding no leases.
        Idle => "idle",
        /// Holding leases and fetching.
        Leased => "leased",
        /// Finishing what it holds and not taking more.
        Draining => "draining",
    }
}

label! {
    /// Which of doc 06.2's seven layers ran, for `umi_verify_total`.
    VerifyLayer as "layer" {
        /// Layer 1, does the delivery agree with itself.
        SelfConsistency => "self_consistency",
        /// Layer 2, does the content look like what it claims to be.
        Plausibility => "plausibility",
        /// Layer 3, we fetched it again.
        Replay => "replay",
        /// Layer 4, two fetchers were asked the same question.
        Quorum => "quorum",
        /// Layer 5, a page we already know the bytes of.
        Canary => "canary",
        /// Layer 6, the TLS chain the fetcher reported.
        Tls => "tls",
        /// Layer 7, does the link structure corroborate.
        Corroboration => "corroboration",
    }
}

label! {
    /// What a layer decided, for `umi_verify_total`.
    ///
    /// Doc 06.3 names three failure outcomes and this adds the one it does not
    /// bother to name, because a ratio needs a denominator.
    VerifyResult as "result" {
        /// The layer ran and was satisfied.
        Pass => "pass",
        /// Discarded and rescheduled, small reputation hit.
        Reject => "reject",
        /// Kept for review, sampling to 1.0 for a day.
        Quarantine => "quarantine",
        /// The key is finished. Doc 06.3 allows exactly two causes.
        Ban => "ban",
    }
}

impl Label for Tier {
    const KEY: &'static str = "tier";
    const COUNT: usize = Self::ALL.len();

    fn from_index(index: usize) -> Self {
        Self::ALL[index]
    }

    fn index(self) -> usize {
        self as usize
    }

    fn value(self) -> &'static str {
        // Not `Display`, which would allocate, and not lowercase, because T0
        // through T4 is what doc 05 and every log line in the system call
        // them and a dashboard should read like the docs.
        match self {
            Self::Revalidate => "T0",
            Self::Plain => "T1",
            Self::Emulated => "T2",
            Self::Rendered => "T3",
            Self::Supervised => "T4",
        }
    }
}

impl Label for OutcomeCode {
    const KEY: &'static str = "outcome";
    const COUNT: usize = Self::ALL.len();

    fn from_index(index: usize) -> Self {
        Self::ALL[index]
    }

    fn index(self) -> usize {
        self as usize
    }

    fn value(self) -> &'static str {
        // Already snake case and already the wire vocabulary, so the label and
        // the protocol cannot drift apart.
        self.wire()
    }
}
