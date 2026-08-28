//! The one entry point the loop uses to get bytes.
//!
//! Doc 04 makes fetching a protocol so the community can host fetchers, and a
//! protocol needs a seam. This is that seam, on the coordinator's side: the
//! loop below calls [`Fetch::fetch`] and does not know whether the answer came
//! from the in-process T1 client, from a browser on the same machine, or from
//! a volunteer's box three time zones away with a doc 04 receipt attached.
//!
//! It is also what makes the loop testable. A crawl test that had to stand up
//! an HTTP server to check that a 304 lands in the right column would be slow,
//! flaky and would test hyper rather than umi, so the tests here implement this
//! trait over a map of canned responses instead.

use umi_fetch::{FetchError, Fetcher, Ladder, Outcome, Revalidator, Tier};

/// Somewhere bytes come from.
///
/// `async_trait` rather than a native async fn, for the same reason
/// `umi_state::State` uses it: the fetcher is chosen from config at runtime,
/// so it is held as `dyn Fetch`, and a native async fn in a trait is not dyn
/// compatible on 1.98.
#[async_trait::async_trait]
pub trait Fetch: Send + Sync {
    /// Fetch one URL, following doc 05's tier ladder as far as this fetcher
    /// goes.
    ///
    /// `revalidate` is what to put in `If-None-Match` and `If-Modified-Since`,
    /// and passing it is how a recrawl of an unchanged page costs a 304
    /// instead of a body. A fetcher is allowed to ignore it, because doc 07.6
    /// notes some origins lie about conditional requests, but a fetcher that
    /// ignores it on a host that honours it is wasting the origin's bandwidth
    /// as well as ours.
    ///
    /// `tier` is what doc 05.8 learned about this host, off the lease. It is a
    /// request and not an instruction: a fetcher that does not have the rung
    /// serves the highest one it does have, which for a build without the
    /// `emulation` feature means a lease at T2 comes back over plain HTTP. The
    /// loop cannot tell, and does not need to. It reports the block either way
    /// and doc 05.8 backs the host off on its own.
    ///
    /// # Errors
    ///
    /// Only for things that are wrong with the request rather than with the
    /// world. A connection refused, a timeout and a 503 are all
    /// [`Outcome`]s, because the loop has to record them against the URL and
    /// back the host off, and an error type that mixed those with "this string
    /// is not a URL" would make the loop match on both.
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: Tier,
    ) -> Result<Outcome, FetchError>;
}

#[async_trait::async_trait]
impl Fetch for Ladder {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: Tier,
    ) -> Result<Outcome, FetchError> {
        Self::fetch(self, url, revalidate, tier).await
    }
}

/// T1 on its own, for callers that have no ladder to offer.
///
/// `umi get` is the honest example: it fetches one URL that a person typed and
/// there is no host history to have learned a tier from. The tier is ignored
/// rather than rejected, because refusing would turn a missing rung into an
/// error the loop would have to handle.
#[async_trait::async_trait]
impl Fetch for Fetcher {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        _tier: Tier,
    ) -> Result<Outcome, FetchError> {
        Self::fetch(self, url, revalidate).await
    }
}

#[async_trait::async_trait]
impl<T: Fetch + ?Sized> Fetch for std::sync::Arc<T> {
    async fn fetch(
        &self,
        url: &str,
        revalidate: Option<&Revalidator>,
        tier: Tier,
    ) -> Result<Outcome, FetchError> {
        (**self).fetch(url, revalidate, tier).await
    }
}
