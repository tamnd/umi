//! The suite every [`State`] backend has to pass.
//!
//! The doc comments on [`State`] say what the methods promise. This says the
//! same thing in a form that fails a build, which is the only form that stays
//! true. A backend that has not been through here has not implemented the
//! trait, it has implemented something that compiles.
//!
//! It is written entirely against the trait. There is no `get(url)` to check a
//! row with and no way to look inside a store, so every assertion is made the
//! way the crawler itself would make it: admit something, lease it, complete
//! it, and see what comes back next. That is a constraint worth keeping.
//! Anything this suite cannot observe is something the crawler cannot observe
//! either, and a promise the crawler cannot observe is not a promise.
//!
//! Point it at a factory that returns a fresh, empty store:
//!
//! ```
//! # use umi_state::{MemoryState, conformance};
//! # async fn example() {
//! conformance::check(|| async { MemoryState::new() })
//!     .await
//!     .assert_ok();
//! # }
//! ```
//!
//! Each case gets its own store, so a case that leaves a mess cannot make the
//! next one fail. The suite reports every case rather than stopping at the
//! first failure, because when a new backend first runs it the useful output is
//! the whole list.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::time::Duration;

use umi_types::{CANON_VERSION, Digest, FetcherId, PldId, RowKey, Tier, Ulid};

use crate::{
    BlockRow, Budget, Candidate, Discovery, FailureKind, FetchOutcome, FetchResult, HostRow,
    LeaseRequest, NackReason, Pace, Priority, RemoteCopy, Revalidator, SegmentQuery, SegmentRow,
    State, Stream, SupervisionRow, TierPolicy, retry_after_ms,
};

/// A fixed instant to run every case from, so nothing in here depends on when
/// it was run. Roughly November 2023, chosen only for being a plausible
/// millisecond timestamp.
const T0: u64 = 1_700_000_000_000;

/// Milliseconds in an hour, for the cases that move inside one refresh window.
const HOUR: u64 = 60 * 60 * 1000;

/// Milliseconds in a day, for the far future cases.
const DAY: u64 = 24 * HOUR;

type Outcome = Result<(), String>;

macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(format!($($arg)*));
        }
    };
}

macro_rules! ensure_eq {
    ($left:expr, $right:expr, $($arg:tt)*) => {{
        let left = $left;
        let right = $right;
        if left != right {
            return Err(format!(
                "{}: expected {right:?}, got {left:?}",
                format_args!($($arg)*)
            ));
        }
    }};
}

/// How one case went.
#[derive(Clone, Debug)]
pub struct CaseResult {
    /// The case name, which is the function name in this module.
    pub name: &'static str,
    /// What went wrong, or `None` if it passed.
    pub failure: Option<String>,
}

/// How the whole suite went.
#[derive(Clone, Debug, Default)]
pub struct Report {
    /// Every case, in the order it ran.
    pub cases: Vec<CaseResult>,
}

impl Report {
    /// How many cases passed.
    #[must_use]
    pub fn passed(&self) -> usize {
        self.cases.iter().filter(|c| c.failure.is_none()).count()
    }

    /// The cases that did not.
    pub fn failures(&self) -> impl Iterator<Item = &CaseResult> {
        self.cases.iter().filter(|c| c.failure.is_some())
    }

    /// Whether the backend conforms.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.failures().next().is_none()
    }

    /// Fail the test, listing every case that did not pass.
    ///
    /// # Panics
    ///
    /// If any case failed.
    #[track_caller]
    pub fn assert_ok(&self) {
        assert!(self.is_ok(), "{self}");
    }

    fn record(&mut self, name: &'static str, outcome: Outcome) {
        self.cases.push(CaseResult {
            name,
            failure: outcome.err(),
        });
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "state conformance: {} of {} cases passed",
            self.passed(),
            self.cases.len()
        )?;
        for case in self.failures() {
            let why = case.failure.as_deref().unwrap_or("");
            writeln!(f, "  FAIL {}: {why}", case.name)?;
        }
        Ok(())
    }
}

/// Run every case against a fresh store from `new`, and report.
///
/// `new` is called once per case. It has to return a store with nothing in it,
/// because the cases assume they are the only thing that has ever touched it.
pub async fn check<F, Fut, S>(new: F) -> Report
where
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
    S: State,
{
    let mut report = Report::default();

    macro_rules! run {
        ($case:ident) => {{
            let state = new().await;
            let outcome = $case(&state as &dyn State).await;
            report.record(stringify!($case), outcome);
        }};
    }

    run!(admit_of_an_empty_batch_reports_nothing);
    run!(admit_accounts_for_every_candidate_exactly_once);
    run!(admitting_the_same_batch_twice_admits_nothing_the_second_time);
    run!(a_duplicate_inside_one_batch_is_admitted_once);
    run!(a_publisher_date_brings_a_known_url_forward);
    run!(an_unverified_discovery_is_held_rather_than_admitted);
    run!(only_admitted_urls_are_ever_leased);
    run!(lease_never_returns_more_than_was_asked_for);
    run!(lease_honours_the_per_host_cap);
    run!(lease_honours_the_per_domain_cap);
    run!(a_domain_cap_of_zero_is_not_a_cap_of_zero);
    run!(lease_of_an_empty_store_is_not_an_error);
    run!(a_leased_url_is_not_leased_again_while_the_lease_holds);
    run!(an_expired_lease_makes_the_url_leasable_again);
    run!(release_makes_a_url_leasable_at_once);
    run!(releasing_a_lease_the_store_never_issued_is_not_an_error);
    run!(a_completed_url_is_not_due_again_immediately);
    run!(completing_the_same_outcome_twice_counts_one_fetch);
    run!(a_gone_url_is_never_leased_again);
    run!(an_excluded_url_is_never_leased_again);
    run!(a_failed_url_comes_back_only_after_its_backoff);
    run!(a_revalidator_comes_back_on_the_next_lease);
    run!(a_url_with_a_validator_is_leased_at_t0);
    run!(a_host_that_ignores_validators_is_leased_at_t1_again);
    run!(a_page_that_changed_comes_back_sooner_than_one_that_did_not);
    run!(discovery_cannot_take_the_whole_batch_from_refresh);
    run!(a_higher_priority_url_is_leased_first);
    run!(lease_returns_work_in_the_order_the_trait_promises);
    run!(a_host_politeness_timer_holds_the_whole_host);
    run!(latency_creep_slows_a_host_down_with_nothing_having_failed);
    run!(retry_after_holds_the_whole_host_for_what_it_asked_for);
    run!(a_lease_that_never_reached_the_wire_is_not_counted_as_a_fetch);
    run!(a_host_record_round_trips);
    run!(an_unknown_host_reads_back_as_none);
    run!(put_host_replaces_rather_than_merges);
    run!(a_blocked_host_is_never_leased);
    run!(a_block_takes_the_domain_out_of_the_frontier);
    run!(a_blocked_domain_is_not_admitted_again);
    run!(a_block_survives_being_applied_twice);
    run!(a_lift_is_dated_and_gives_the_domain_back);
    run!(the_block_list_reads_back_with_its_reason);
    run!(nothing_reaches_t4_without_an_allowlist_entry);
    run!(an_allowlist_entry_is_the_only_route_to_t4);
    run!(an_allowlist_entry_does_not_overrule_the_fetcher);
    run!(taking_a_domain_off_the_allowlist_takes_t4_with_it);
    run!(a_segment_record_round_trips);
    run!(an_unknown_segment_reads_back_as_none);
    run!(put_segment_replaces_rather_than_merges);
    run!(a_sealed_segment_is_unpublished_until_it_has_a_remote_copy);
    run!(a_segment_is_collectable_only_once_the_ledger_is_complete);
    run!(a_deleted_segment_is_not_offered_for_collection_again);
    run!(segments_come_back_in_seal_order);
    run!(a_seal_window_is_half_open);
    run!(warming_a_domain_the_store_has_never_seen_is_not_an_error);
    run!(evicting_a_domain_that_is_not_resident_is_not_an_error);
    run!(resident_is_sorted_and_free_of_duplicates);
    run!(a_domain_the_store_holds_a_url_for_is_local);
    run!(stats_account_for_what_was_admitted);
    run!(checkpoint_sequence_is_monotonic);
    run!(a_checkpoint_names_the_canonicalisation_its_keys_are_under);
    run!(a_checkpoint_is_stamped_with_the_time_it_was_given);

    report
}

// Helpers. `host` numbers a distinct host, `path` a distinct URL on one host,
// so a case can choose whether per host politeness is part of what it is
// testing.

fn host_url(n: usize) -> String {
    format!("https://h{n}.example.com/")
}

fn path_url(n: usize) -> String {
    format!("https://one.example.com/p/{n}")
}

fn key(url: &str) -> RowKey {
    RowKey::for_url(url, None).expect("conformance urls are well formed")
}

fn candidate<'a>(url: &'a str, priority: Priority) -> Candidate<'a> {
    Candidate {
        key: key(url),
        url,
        depth: 1,
        priority,
        discovered_ms: T0,
        discovery: Discovery::Trusted,
        lastmod_ms: None,
    }
}

/// A distinct segment id, sealed `n` milliseconds after [`T0`].
///
/// The timestamp is in the ULID, so ids made this way sort in seal order, and
/// the entropy is the index rather than anything random because doc 11.1's
/// determinism rule applies to the test suite too: a case that fails once in
/// a thousand runs is worse than no case.
fn segment_id(n: u8) -> Ulid {
    Ulid::new(T0 + u64::from(n), [n; 10])
}

fn sealed(n: u8) -> SegmentRow {
    SegmentRow {
        id: segment_id(n),
        stream: Stream::Pages,
        local_path: format!("./crawl/segments/{n}.umi"),
        sealed_at_ms: T0 + u64::from(n),
        rows: 118_671,
        bytes: 128 << 20,
        local_digest: Digest::from_bytes([n; 32]),
        remote: None,
        manifest_day: None,
        deleted_at_ms: None,
    }
}

/// The same row as it looks after doc 12.2 steps 4 through 6 have run.
fn published(n: u8) -> SegmentRow {
    SegmentRow {
        remote: Some(RemoteCopy {
            repo: "open-index/umi-pages-2026w34-01".to_owned(),
            path: format!("data/20260825/{n}.parquet"),
            digest: Digest::from_bytes([n.wrapping_add(1); 32]),
        }),
        manifest_day: Some(20_260_825),
        ..sealed(n)
    }
}

fn request<'a>(now_ms: u64, max_urls: u32) -> LeaseRequest<'a> {
    LeaseRequest {
        max_tier: Tier::Rendered,
        ..LeaseRequest::new(FetcherId::LOCAL, now_ms, max_urls)
    }
}

fn fetched(lease: &crate::Lease, finished_ms: u64, body: &str) -> FetchOutcome {
    FetchOutcome {
        lease: lease.id,
        key: lease.key,
        finished_ms,
        tier_used: Tier::Plain,
        pace: Pace::default(),
        result: FetchResult::Fetched {
            status: 200,
            content_hash: crate::memory::content_hash(body),
            revalidate: Revalidator::default(),
        },
    }
}

async fn admit_all(state: &dyn State, urls: &[String]) -> Result<crate::AdmitReport, String> {
    let batch: Vec<_> = urls
        .iter()
        .map(|url| candidate(url, Priority::DEFAULT))
        .collect();
    state
        .admit(&batch)
        .await
        .map_err(|e| format!("admit failed: {e}"))
}

async fn lease_one(state: &dyn State, now_ms: u64) -> Result<Vec<crate::Lease>, String> {
    state
        .lease(&request(now_ms, 16))
        .await
        .map_err(|e| format!("lease failed: {e}"))
}

// The cases.

async fn admit_of_an_empty_batch_reports_nothing(state: &dyn State) -> Outcome {
    let report = state.admit(&[]).await.map_err(|e| e.to_string())?;
    ensure_eq!(report.total(), 0, "an empty batch produced dispositions");
    Ok(())
}

async fn admit_accounts_for_every_candidate_exactly_once(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..8).map(path_url).collect();
    let report = admit_all(state, &urls).await?;
    ensure_eq!(
        report.total(),
        8,
        "the four dispositions must sum to the batch length"
    );
    ensure_eq!(report.admitted, 8, "eight new urls should all be admitted");
    ensure_eq!(report.seen, 0, "nothing was known before this call");
    Ok(())
}

async fn admitting_the_same_batch_twice_admits_nothing_the_second_time(
    state: &dyn State,
) -> Outcome {
    let urls: Vec<String> = (0..5).map(path_url).collect();
    admit_all(state, &urls).await?;
    let second = admit_all(state, &urls).await?;
    ensure_eq!(second.admitted, 0, "the second admit re-admitted urls");
    ensure_eq!(second.seen, 5, "the second admit did not recognise them");
    Ok(())
}

async fn a_duplicate_inside_one_batch_is_admitted_once(state: &dyn State) -> Outcome {
    let url = path_url(1);
    let batch = vec![
        candidate(&url, Priority::DEFAULT),
        candidate(&url, Priority::DEFAULT),
        candidate(&url, Priority::DEFAULT),
    ];
    let report = state.admit(&batch).await.map_err(|e| e.to_string())?;
    ensure_eq!(report.admitted, 1, "a repeat inside one batch was admitted");
    ensure_eq!(report.seen, 2, "the repeats were not counted as seen");
    ensure_eq!(report.total(), 3, "dispositions do not sum to the batch");
    Ok(())
}

async fn a_publisher_date_brings_a_known_url_forward(state: &dyn State) -> Outcome {
    // Doc 13.6's reason for keeping `lastmod` rather than throwing it away.
    // Fetch a url, so it is scheduled a day out, then admit it again with a
    // date on it later than that fetch. The site has just said the page moved,
    // which beats anything the estimator worked out, so it is due now.
    let url = host_url(1);
    admit_all(state, std::slice::from_ref(&url)).await?;
    let leases = lease_one(state, T0).await?;
    let lease = leases.first().ok_or("the url was not leased")?;
    let done = fetched(lease, T0 + 500, "hello");
    state
        .complete(&[done])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let hour = T0 + HOUR;
    ensure!(
        lease_one(state, hour).await?.is_empty(),
        "a url fetched an hour ago is not due yet"
    );

    // A date before the fetch is the ordinary case on a sitemap that lists a
    // whole site, and treating it as news would refetch everything on every
    // poll.
    let batch = vec![Candidate {
        discovered_ms: hour,
        lastmod_ms: Some(T0 - DAY),
        ..candidate(&url, Priority::DEFAULT)
    }];
    let stale = state.admit(&batch).await.map_err(|e| e.to_string())?;
    ensure_eq!(stale.seen, 1, "a known url was not reported as seen");
    ensure_eq!(stale.refreshed, 0, "an old date moved the schedule");
    ensure_eq!(stale.total(), 1, "refreshed leaked into the dispositions");
    ensure!(
        lease_one(state, hour).await?.is_empty(),
        "an old date brought the url forward"
    );

    // A date after it is the site telling us our copy is stale.
    let batch = vec![Candidate {
        discovered_ms: hour,
        lastmod_ms: Some(T0 + 600),
        ..candidate(&url, Priority::DEFAULT)
    }];
    let fresh = state.admit(&batch).await.map_err(|e| e.to_string())?;
    ensure_eq!(fresh.seen, 1, "a known url was not reported as seen");
    ensure_eq!(fresh.refreshed, 1, "a newer date did not move the schedule");
    ensure_eq!(fresh.total(), 1, "refreshed leaked into the dispositions");
    let leases = lease_one(state, hour).await?;
    ensure_eq!(leases.len(), 1, "the url did not come due after the date");
    Ok(())
}

async fn an_unverified_discovery_is_held_rather_than_admitted(state: &dyn State) -> Outcome {
    let url = path_url(1);
    let batch = vec![Candidate {
        discovery: Discovery::Unverified(FetcherId::from_bytes([7u8; 32])),
        ..candidate(&url, Priority::DEFAULT)
    }];
    let report = state.admit(&batch).await.map_err(|e| e.to_string())?;
    ensure_eq!(report.held, 1, "an unverified link skipped the holding pen");
    ensure_eq!(
        report.admitted,
        0,
        "an unverified link reached the frontier"
    );

    let leases = lease_one(state, T0).await?;
    ensure!(
        leases.is_empty(),
        "a held url was leased: {} came back",
        leases.len()
    );
    Ok(())
}

async fn only_admitted_urls_are_ever_leased(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..3).map(host_url).collect();
    admit_all(state, &urls).await?;
    let leases = lease_one(state, T0).await?;
    for lease in &leases {
        ensure!(
            urls.contains(&lease.url),
            "leased a url that was never admitted: {}",
            lease.url
        );
        ensure_eq!(lease.key, key(&lease.url), "the lease key is not the url's");
    }
    Ok(())
}

async fn lease_never_returns_more_than_was_asked_for(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..10).map(host_url).collect();
    admit_all(state, &urls).await?;
    let leases = state
        .lease(&request(T0, 3))
        .await
        .map_err(|e| e.to_string())?;
    ensure!(leases.len() <= 3, "asked for 3, got {}", leases.len());
    Ok(())
}

async fn lease_honours_the_per_host_cap(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..10).map(path_url).collect();
    admit_all(state, &urls).await?;
    let leases = state
        .lease(&LeaseRequest {
            max_per_host: 2,
            ..request(T0, 100)
        })
        .await
        .map_err(|e| e.to_string())?;
    ensure!(
        leases.len() <= 2,
        "ten urls on one host, cap of 2, got {}",
        leases.len()
    );
    Ok(())
}

async fn lease_honours_the_per_domain_cap(state: &dyn State) -> Outcome {
    // Doc 09.3's cap is the scheduler's, but the scheduler spends it in one
    // call covering every domain it is ready to ask about, so the store is
    // what has to count it. Ten hosts under one domain and ten under another,
    // and a cap of three: without it the first domain takes the whole batch,
    // because the order the trait promises has nothing to do with domains.
    let mut urls: Vec<String> = (0..10).map(host_url).collect();
    urls.extend((0..10).map(|n| format!("https://h{n}.other.example/")));
    admit_all(state, &urls).await?;
    let leases = state
        .lease(&LeaseRequest {
            max_per_pld: 3,
            ..request(T0, 100)
        })
        .await
        .map_err(|e| e.to_string())?;

    let mut per_pld: BTreeMap<PldId, usize> = BTreeMap::new();
    for lease in &leases {
        *per_pld.entry(lease.key.pld).or_default() += 1;
    }
    for (pld, taken) in &per_pld {
        ensure!(
            *taken <= 3,
            "domain {pld:?} took {taken} against a cap of 3"
        );
    }
    // And the cap is a cap and not a batch size. Both domains had ten urls due
    // and both should have been offered their three, or a scheduler that asks
    // once for five hundred domains gets one domain's worth of work back.
    ensure!(
        per_pld.len() == 2,
        "two domains were due and {} came back: {leases:?}",
        per_pld.len()
    );
    Ok(())
}

/// A cap of zero is no cap, which is what a caller that does not care sends.
async fn a_domain_cap_of_zero_is_not_a_cap_of_zero(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..10).map(host_url).collect();
    admit_all(state, &urls).await?;
    let leases = state
        .lease(&LeaseRequest {
            max_per_pld: 0,
            max_per_host: 8,
            ..request(T0, 100)
        })
        .await
        .map_err(|e| e.to_string())?;
    ensure!(
        leases.len() == 10,
        "ten urls on one domain with no domain cap returned {}",
        leases.len()
    );
    Ok(())
}

async fn lease_of_an_empty_store_is_not_an_error(state: &dyn State) -> Outcome {
    let leases = lease_one(state, T0).await?;
    ensure!(leases.is_empty(), "an empty store handed out work");
    Ok(())
}

async fn a_leased_url_is_not_leased_again_while_the_lease_holds(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let first = lease_one(state, T0).await?;
    ensure_eq!(first.len(), 1, "the only admitted url was not leased");

    // Ten seconds later: past any politeness delay, well inside the minute
    // long lease. The url must still be held.
    let second = lease_one(state, T0 + 10_000).await?;
    ensure!(
        second.is_empty(),
        "a url under lease was handed out twice: {:?}",
        second.first().map(|l| &l.url)
    );
    Ok(())
}

async fn an_expired_lease_makes_the_url_leasable_again(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let first = state
        .lease(&LeaseRequest {
            lease_for: Duration::from_secs(30),
            ..request(T0, 1)
        })
        .await
        .map_err(|e| e.to_string())?;
    ensure_eq!(first.len(), 1, "nothing was leased");

    let after = first[0].expires_ms + 1000;
    let second = lease_one(state, after).await?;
    ensure_eq!(
        second.len(),
        1,
        "an expired lease did not return the url to the frontier"
    );
    ensure!(
        second[0].id != first[0].id,
        "the second lease reused the first lease's id"
    );
    Ok(())
}

async fn release_makes_a_url_leasable_at_once(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let first = lease_one(state, T0).await?;
    ensure_eq!(first.len(), 1, "nothing was leased");

    state
        .release(&[first[0].id], NackReason::FetcherGone)
        .await
        .map_err(|e| format!("release failed: {e}"))?;

    // Ten seconds on, so the host's politeness timer is not what is being
    // tested. The url itself must be available again.
    let second = lease_one(state, T0 + 10_000).await?;
    ensure_eq!(second.len(), 1, "a released url did not come back");
    ensure_eq!(&second[0].url, &first[0].url, "a different url came back");
    Ok(())
}

async fn releasing_a_lease_the_store_never_issued_is_not_an_error(state: &dyn State) -> Outcome {
    state
        .release(
            &[crate::LeaseId::from_raw(9_999_999)],
            NackReason::FetcherGone,
        )
        .await
        .map_err(|e| format!("releasing an unknown lease failed: {e}"))?;
    Ok(())
}

async fn a_completed_url_is_not_due_again_immediately(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    state
        .complete(&[fetched(&leases[0], T0 + 500, "hello")])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let again = lease_one(state, T0 + 60_000).await?;
    ensure!(
        again.is_empty(),
        "a page fetched a minute ago was scheduled again at once"
    );
    Ok(())
}

async fn completing_the_same_outcome_twice_counts_one_fetch(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    let outcome = fetched(&leases[0], T0 + 500, "hello");
    state
        .complete(std::slice::from_ref(&outcome))
        .await
        .map_err(|e| format!("first complete failed: {e}"))?;
    state
        .complete(std::slice::from_ref(&outcome))
        .await
        .map_err(|e| format!("second complete failed: {e}"))?;

    // `attempt` is `fetch_count`, and it is the only way to see from outside
    // whether the second completion was applied.
    let later = lease_one(state, T0 + 90 * DAY).await?;
    ensure_eq!(later.len(), 1, "the url never came due for refresh");
    ensure_eq!(later[0].attempt, 1, "a repeated completion counted twice");
    Ok(())
}

async fn a_gone_url_is_never_leased_again(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    state
        .complete(&[FetchOutcome {
            lease: leases[0].id,
            key: leases[0].key,
            finished_ms: T0 + 500,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Gone { status: 410 },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let later = lease_one(state, T0 + 3650 * DAY).await?;
    ensure!(later.is_empty(), "a 410 was scheduled again ten years on");
    Ok(())
}

async fn an_excluded_url_is_never_leased_again(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    state
        .complete(&[FetchOutcome {
            lease: leases[0].id,
            key: leases[0].key,
            finished_ms: T0 + 500,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Excluded {
                reason: crate::ExcludeReason::Robots,
            },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let later = lease_one(state, T0 + 3650 * DAY).await?;
    ensure!(later.is_empty(), "a robots exclusion was scheduled again");
    Ok(())
}

async fn a_failed_url_comes_back_only_after_its_backoff(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    let finished = T0 + 500;
    state
        .complete(&[FetchOutcome {
            lease: leases[0].id,
            key: leases[0].key,
            finished_ms: finished,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Failed {
                status: Some(500),
                kind: FailureKind::ServerError,
            },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let backoff = retry_after_ms(1);
    let too_soon = lease_one(state, finished + backoff / 2).await?;
    ensure!(
        too_soon.is_empty(),
        "a failed url was retried inside its backoff"
    );

    let due = lease_one(state, finished + backoff + 1000).await?;
    ensure_eq!(
        due.len(),
        1,
        "a failed url never came back after its backoff"
    );
    Ok(())
}

async fn a_revalidator_comes_back_on_the_next_lease(state: &dyn State) -> Outcome {
    admit_all(state, &[host_url(1)]).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    ensure!(
        leases[0].revalidate.is_none(),
        "a url never fetched came with a revalidator"
    );

    state
        .complete(&[FetchOutcome {
            lease: leases[0].id,
            key: leases[0].key,
            finished_ms: T0 + 500,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Fetched {
                status: 200,
                content_hash: crate::memory::content_hash("hello"),
                revalidate: Revalidator {
                    etag: Some("\"abc\"".to_owned()),
                    last_modified_ms: Some(T0),
                },
            },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let later = lease_one(state, T0 + 90 * DAY).await?;
    ensure_eq!(later.len(), 1, "the url never came due for refresh");
    let revalidate = later[0]
        .revalidate
        .as_ref()
        .ok_or_else(|| "the etag from the last fetch was not offered back".to_owned())?;
    ensure_eq!(
        revalidate.etag.as_deref(),
        Some("\"abc\""),
        "the etag came back changed"
    );
    Ok(())
}

async fn a_url_with_a_validator_is_leased_at_t0(state: &dyn State) -> Outcome {
    // Doc 05.3's rung, and the one the crawl was missing. A first fetch has
    // nothing to revalidate against and runs at T1, and the refresh after it
    // has an etag and runs at T0, which is the same client with
    // `If-None-Match` on it. Without this the refresh spends a full body to
    // find out the page has not changed, and the segment gets a second copy of
    // a row it already published.
    admit_all(state, &[host_url(1)]).await?;
    let first = lease_one(state, T0).await?;
    ensure_eq!(first.len(), 1, "nothing was leased");
    ensure_eq!(
        first[0].tier,
        Tier::Plain,
        "a url with nothing to revalidate should be leased at T1"
    );

    state
        .complete(&[FetchOutcome {
            lease: first[0].id,
            key: first[0].key,
            finished_ms: T0 + 500,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Fetched {
                status: 200,
                content_hash: crate::memory::content_hash("hello"),
                revalidate: Revalidator {
                    etag: Some("\"abc\"".to_owned()),
                    last_modified_ms: None,
                },
            },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let refresh = lease_one(state, T0 + 90 * DAY).await?;
    ensure_eq!(refresh.len(), 1, "the url never came due for refresh");
    ensure_eq!(
        refresh[0].tier,
        Tier::Revalidate,
        "a refresh holding an etag should be leased at T0"
    );
    Ok(())
}

async fn a_host_that_ignores_validators_is_leased_at_t1_again(state: &dyn State) -> Outcome {
    // The other half of doc 05.3. A host that has ignored our validators
    // enough times to be believed gets no conditional request, and a lease
    // with nothing to send is not a T0 lease. The two have to move together:
    // a T0 lease with no `If-None-Match` on it is a plain GET wearing the
    // wrong label, and the label is published.
    let url = host_url(1);
    admit_all(state, std::slice::from_ref(&url)).await?;
    let first = lease_one(state, T0).await?;
    ensure_eq!(first.len(), 1, "nothing was leased");
    state
        .complete(&[FetchOutcome {
            lease: first[0].id,
            key: first[0].key,
            finished_ms: T0 + 500,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Fetched {
                status: 200,
                content_hash: crate::memory::content_hash("hello"),
                revalidate: Revalidator {
                    etag: Some("\"abc\"".to_owned()),
                    last_modified_ms: None,
                },
            },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let mut host = HostRow::new(key(&url).host, key(&url).pld);
    host.tier.weak_hits = TierPolicy::WEAK_HITS_TO_DROP;
    state
        .put_host(&[host])
        .await
        .map_err(|e| format!("put_host failed: {e}"))?;

    let refresh = lease_one(state, T0 + 90 * DAY).await?;
    ensure_eq!(refresh.len(), 1, "the url never came due for refresh");
    ensure!(
        refresh[0].revalidate.is_none(),
        "a host that ignores validators was still sent one"
    );
    ensure_eq!(
        refresh[0].tier,
        Tier::Plain,
        "a lease with no validator to send should not be at T0"
    );
    Ok(())
}

async fn a_page_that_changed_comes_back_sooner_than_one_that_did_not(state: &dyn State) -> Outcome {
    // Doc 09.4's estimator, through the only window the trait gives onto it.
    // Two urls on two hosts, fetched at the same two times, and the only
    // difference between them is that one came back 304 and the other came back
    // with different bytes. A backend that ignored the 304, or that ran its own
    // refresh rule, would schedule them the same.
    let urls = [host_url(1), host_url(2)];
    admit_all(state, &urls).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 2, "both urls should have been leased");
    let first: Vec<_> = leases
        .iter()
        .map(|lease| fetched(lease, T0 + 500, "hello"))
        .collect();
    state
        .complete(&first)
        .await
        .map_err(|e| format!("first complete failed: {e}"))?;

    // A day on, which is the fixed interval a single fetch earns, both are due.
    let day = T0 + DAY + 500;
    let leases = lease_one(state, day).await?;
    ensure_eq!(leases.len(), 2, "neither url came due after the first day");
    let steady = leases
        .iter()
        .find(|lease| lease.url.contains("h1."))
        .ok_or("the unchanged url was not leased")?;
    let moving = leases
        .iter()
        .find(|lease| lease.url.contains("h2."))
        .ok_or("the changed url was not leased")?;
    state
        .complete(&[
            FetchOutcome {
                lease: steady.id,
                key: steady.key,
                finished_ms: day + 500,
                tier_used: Tier::Plain,
                pace: Pace::default(),
                result: FetchResult::NotModified {
                    status: 304,
                    revalidate: Revalidator::default(),
                },
            },
            fetched(moving, day + 500, "goodbye"),
        ])
        .await
        .map_err(|e| format!("second complete failed: {e}"))?;

    // Six hours on, neither is due. Thirteen hours on, only the one that
    // changed is: two intervals, one of which ended in a change, puts it at
    // about eleven hours, while nothing seen to change gets the whole
    // observation window over again.
    let soon = lease_one(state, day + DAY / 4).await?;
    ensure!(soon.is_empty(), "a url was refetched within six hours");

    let later = lease_one(state, day + DAY / 2 + 3_600_000).await?;
    ensure_eq!(later.len(), 1, "expected exactly the changed url");
    ensure!(
        later[0].url.contains("h2."),
        "the url that came back 304 was refetched first"
    );
    Ok(())
}

async fn discovery_cannot_take_the_whole_batch_from_refresh(state: &dyn State) -> Outcome {
    // Doc 09.5's budget, through the only window the trait gives onto it. One
    // url that has been fetched once, so it is due again a day later and sits
    // in the daily class, against fifty that have never been fetched. Every row
    // is at the default priority and the fetched one is due last, so in the
    // order doc 08.4 promises it is the fifty first of fifty one rows and a
    // batch of twenty would never reach it.
    let refresh = host_url(1);
    admit_all(state, std::slice::from_ref(&refresh)).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    state
        .complete(&[fetched(&leases[0], T0 + 500, "hello")])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let discovery: Vec<String> = (10..60).map(host_url).collect();
    admit_all(state, &discovery).await?;
    let due = T0 + DAY + 3_600_000;

    // The control. Told to spend everything on discovery, a backend has to
    // leave the refresh out, and a case where the refresh gets in either way
    // would prove nothing about the budget.
    let all_discovery = LeaseRequest {
        budget: Budget::new([0, 0, 0, 0, 0, 100]),
        ..request(due, 20)
    };
    let batch = state
        .lease(&all_discovery)
        .await
        .map_err(|e| format!("lease failed: {e}"))?;
    ensure!(
        !batch.iter().any(|lease| lease.url == refresh),
        "a budget with nothing in it for refresh still spent some on refresh"
    );
    let ids: Vec<_> = batch.iter().map(|lease| lease.id).collect();
    state
        .release(&ids, NackReason::Shutdown)
        .await
        .map_err(|e| format!("release failed: {e}"))?;

    // The same frontier and the same instant, under doc 09.5's split. A lease
    // is a second later so that the hosts the control touched are past their
    // politeness delay again.
    let batch = state
        .lease(&request(due + 2_000, 20))
        .await
        .map_err(|e| format!("lease failed: {e}"))?;
    ensure_eq!(
        batch.len(),
        20,
        "the split turned into a cap and left the batch short"
    );
    ensure!(
        batch.iter().any(|lease| lease.url == refresh),
        "fifty new urls crowded the one url that was due for refresh out of the batch"
    );
    Ok(())
}

async fn a_higher_priority_url_is_leased_first(state: &dyn State) -> Outcome {
    // Two hosts, so per host politeness is not what decides this.
    let low = host_url(1);
    let high = host_url(2);
    state
        .admit(&[
            candidate(&low, Priority::from_raw(10)),
            candidate(&high, Priority::from_raw(60_000)),
        ])
        .await
        .map_err(|e| e.to_string())?;

    let leases = state
        .lease(&request(T0, 1))
        .await
        .map_err(|e| e.to_string())?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    ensure_eq!(&leases[0].url, &high, "priority did not decide the order");
    Ok(())
}

async fn lease_returns_work_in_the_order_the_trait_promises(state: &dyn State) -> Outcome {
    // Twelve urls admitted at one instant with one priority, so the RowKey
    // tiebreak is the only thing left to order them by. That tiebreak is not
    // an implementation detail: without a total order the same store in the
    // same state can lease a different set, and a crawl that cannot be
    // replayed cannot be debugged.
    let urls: Vec<String> = (0..12).map(host_url).collect();
    admit_all(state, &urls).await?;

    let leases = lease_one(state, T0).await?;
    ensure!(
        leases.len() >= 2,
        "only {} leases came back, which is too few to have an order",
        leases.len()
    );
    for pair in leases.windows(2) {
        ensure!(
            pair[0].priority >= pair[1].priority,
            "a lower priority url was leased before a higher one: {:?} then {:?}",
            pair[0].priority,
            pair[1].priority
        );
        if pair[0].priority == pair[1].priority {
            ensure!(
                pair[0].key < pair[1].key,
                "two urls of equal priority were not ordered by key: {:?} then {:?}",
                pair[0].key,
                pair[1].key
            );
        }
    }
    Ok(())
}

async fn a_host_politeness_timer_holds_the_whole_host(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..4).map(path_url).collect();
    admit_all(state, &urls).await?;

    let host = key(&urls[0]);
    state
        .put_host(&[HostRow {
            next_allowed_ms: T0 + 60 * 60 * 1000,
            ..crate::memory::host_row(&urls[0])
        }])
        .await
        .map_err(|e| format!("put_host failed: {e}"))?;

    let held = lease_one(state, T0).await?;
    ensure!(
        held.iter().all(|lease| lease.key.host != host.host),
        "a host inside its politeness window was leased anyway"
    );

    let later = lease_one(state, T0 + 2 * 60 * 60 * 1000).await?;
    ensure!(
        !later.is_empty(),
        "the host never became leasable after its timer passed"
    );
    Ok(())
}

async fn latency_creep_slows_a_host_down_with_nothing_having_failed(state: &dyn State) -> Outcome {
    // Doc 07.6's 1.3 rung, through the trait. Every request here succeeds and
    // the origin never complains, it only gets slower, and a backend that
    // waits for an error before easing off has already made somebody's
    // afternoon worse.
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    let host = key(&urls[0]).host;

    let before = state
        .host(host)
        .await
        .map_err(|e| format!("host failed: {e}"))?
        .map_or(HostRow::INITIAL_DELAY_MS, |row| row.adaptive_delay_ms);

    let mut now = T0;
    for _ in 0..3 {
        let leases = lease_one(state, now).await?;
        ensure!(!leases.is_empty(), "nothing was leased");
        let slow = Pace {
            latency_ms: Some(5000),
            retry_after_ms: None,
        };
        state
            .complete(&[FetchOutcome {
                pace: slow,
                ..fetched(&leases[0], now + 5000, "body")
            }])
            .await
            .map_err(|e| format!("complete failed: {e}"))?;
        now += 60 * 60 * 1000;
    }

    let after = state
        .host(host)
        .await
        .map_err(|e| format!("host failed: {e}"))?
        .ok_or("the host has no record after three fetches")?;
    ensure!(
        after.adaptive_delay_ms > before,
        "three slow answers did not slow the crawler down"
    );
    ensure_eq!(
        after.failures,
        0,
        "nothing failed and something was counted"
    );
    Ok(())
}

async fn retry_after_holds_the_whole_host_for_what_it_asked_for(state: &dyn State) -> Outcome {
    // The url that was rate limited has its own backoff, so the interesting
    // question is about the rest of the host. An origin that says "not for
    // five minutes" means the site, not the page, and a crawler that moves on
    // to the next url on the same host has honoured nothing.
    let urls: Vec<String> = (0..4).map(path_url).collect();
    admit_all(state, &urls).await?;

    let leases = state
        .lease(&request(T0, 1))
        .await
        .map_err(|e| format!("lease failed: {e}"))?;
    ensure_eq!(leases.len(), 1, "nothing was leased");

    let finished = T0 + 200;
    let asked_ms = 5 * 60 * 1000;
    state
        .complete(&[FetchOutcome {
            lease: leases[0].id,
            key: leases[0].key,
            finished_ms: finished,
            tier_used: Tier::Plain,
            pace: Pace {
                latency_ms: Some(200),
                retry_after_ms: Some(asked_ms),
            },
            result: FetchResult::Failed {
                status: Some(429),
                kind: FailureKind::Blocked,
            },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let too_soon = lease_one(state, finished + u64::from(asked_ms) / 2).await?;
    ensure!(
        too_soon.is_empty(),
        "a host was crawled again inside the window it asked for"
    );

    let due = lease_one(state, finished + u64::from(asked_ms) + 1000).await?;
    ensure!(!due.is_empty(), "the host never came back after the window");
    Ok(())
}

async fn a_lease_that_never_reached_the_wire_is_not_counted_as_a_fetch(
    state: &dyn State,
) -> Outcome {
    // Robots excluded it before anything went on the wire. Doc 08.4 counts
    // fetches per host and a crawl of a disallowed site must not report
    // thousands of requests it never made.
    admit_all(state, &[host_url(1)]).await?;
    let leases = lease_one(state, T0).await?;
    ensure_eq!(leases.len(), 1, "nothing was leased");
    let host = leases[0].key.host;

    state
        .complete(&[FetchOutcome {
            lease: leases[0].id,
            key: leases[0].key,
            finished_ms: T0 + 5,
            tier_used: Tier::Plain,
            pace: Pace::default(),
            result: FetchResult::Excluded {
                reason: crate::ExcludeReason::Robots,
            },
        }])
        .await
        .map_err(|e| format!("complete failed: {e}"))?;

    let fetches = state
        .host(host)
        .await
        .map_err(|e| format!("host failed: {e}"))?
        .map_or(0, |row| row.fetches);
    ensure_eq!(fetches, 0, "an exclusion was counted as a request");
    Ok(())
}

async fn a_host_record_round_trips(state: &dyn State) -> Outcome {
    let url = host_url(1);
    let row = HostRow {
        adaptive_delay_ms: 2500,
        crawl_delay_ms: Some(5000),
        next_allowed_ms: T0 + 1234,
        content_usage: Some("train-ai=n".to_owned()),
        sitemaps: vec!["https://h1.example.com/sitemap.xml".to_owned()],
        fetches: 42,
        failures: 3,
        consecutive_failures: 1,
        ..crate::memory::host_row(&url)
    };
    state
        .put_host(std::slice::from_ref(&row))
        .await
        .map_err(|e| format!("put_host failed: {e}"))?;

    let read = state
        .host(row.host)
        .await
        .map_err(|e| format!("host failed: {e}"))?
        .ok_or_else(|| "a host that was just written read back as none".to_owned())?;
    ensure_eq!(read, row, "the host record did not survive the round trip");
    Ok(())
}

async fn an_unknown_host_reads_back_as_none(state: &dyn State) -> Outcome {
    let read = state
        .host(key(&host_url(99)).host)
        .await
        .map_err(|e| format!("host failed: {e}"))?;
    ensure!(
        read.is_none(),
        "a host we have never seen came back as some"
    );
    Ok(())
}

async fn put_host_replaces_rather_than_merges(state: &dyn State) -> Outcome {
    let url = host_url(1);
    let first = HostRow {
        adaptive_delay_ms: 9000,
        content_usage: Some("train-ai=n".to_owned()),
        ..crate::memory::host_row(&url)
    };
    let second = HostRow {
        adaptive_delay_ms: 1000,
        content_usage: None,
        ..crate::memory::host_row(&url)
    };
    state
        .put_host(&[first, second.clone()])
        .await
        .map_err(|e| format!("put_host failed: {e}"))?;

    let read = state
        .host(second.host)
        .await
        .map_err(|e| format!("host failed: {e}"))?
        .ok_or_else(|| "the host vanished".to_owned())?;
    ensure_eq!(
        read,
        second,
        "the second write merged with the first instead of replacing it"
    );
    Ok(())
}

async fn a_blocked_host_is_never_leased(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    state
        .put_host(&[HostRow {
            blocked: true,
            ..crate::memory::host_row(&urls[0])
        }])
        .await
        .map_err(|e| format!("put_host failed: {e}"))?;

    let leases = lease_one(state, T0 + DAY).await?;
    ensure!(
        leases.is_empty(),
        "a blocked host was leased: {:?}",
        leases.first().map(|lease| &lease.url)
    );
    Ok(())
}

/// The domain every URL in this file falls under, so a block here covers the
/// whole suite's namespace and both URL shapes.
const BLOCKED_DOMAIN: &str = "example.com";

/// Doc 07.7's reason, written the way a real one would be.
const BECAUSE: &str = "the site owner asked us to stop on 2026-08-14, ticket 41";

async fn a_block_takes_the_domain_out_of_the_frontier(state: &dyn State) -> Outcome {
    // Both shapes, because a block is about the registrable domain and not
    // about one host under it. Blocking on the strength of the host we happened
    // to be given would leave the rest of the site being crawled, which is
    // honouring the letter of a request rather than the request.
    let urls: Vec<String> = (0..3).map(path_url).chain((0..3).map(host_url)).collect();
    admit_all(state, &urls).await?;
    ensure!(
        !lease_one(state, T0).await?.is_empty(),
        "nothing was leasable before the block, so this case proves nothing"
    );

    let report = apply(state, &[BlockRow::new(BLOCKED_DOMAIN, BECAUSE, T0)]).await?;
    ensure!(
        report.excluded >= 6,
        "a block left urls in the frontier: {} excluded of 6",
        report.excluded
    );

    // Later than the leases the first call handed out, so a politeness timer
    // cannot be what is keeping this empty.
    let leases = lease_one(state, T0 + DAY).await?;
    ensure!(
        leases.is_empty(),
        "a blocked domain was leased: {:?}",
        leases.first().map(|lease| &lease.url)
    );
    Ok(())
}

async fn a_blocked_domain_is_not_admitted_again(state: &dyn State) -> Outcome {
    apply(state, &[BlockRow::new(BLOCKED_DOMAIN, BECAUSE, T0)]).await?;

    // Doc 07.7 says a block prevents future admission, and the url has to end
    // up in the seen set anyway: a candidate that is refused and forgotten is
    // one that costs the same decision again on every page that links to it.
    let urls: Vec<String> = (0..4).map(path_url).collect();
    let report = admit_all(state, &urls).await?;
    ensure_eq!(report.admitted, 0, "a blocked domain was admitted");
    ensure_eq!(report.total(), 4, "the batch was not accounted for");

    let second = admit_all(state, &urls).await?;
    ensure_eq!(
        second.seen,
        4,
        "a refused candidate was not remembered, so it will be refused again"
    );
    ensure!(
        lease_one(state, T0 + DAY).await?.is_empty(),
        "a blocked domain was leased after being admitted into it"
    );
    Ok(())
}

async fn a_block_survives_being_applied_twice(state: &dyn State) -> Outcome {
    // The trait says a caller that gets an error retries the batch whole, so
    // applying one twice has to be the same as applying it once. It is also
    // what happens when an operator does not know the block is already there.
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    let block = BlockRow::new(BLOCKED_DOMAIN, BECAUSE, T0);
    let first = apply(state, std::slice::from_ref(&block)).await?;
    let second = apply(state, std::slice::from_ref(&block)).await?;
    ensure_eq!(first.excluded, 3, "the first block moved the wrong number");
    ensure_eq!(second.excluded, 0, "the second block moved something");

    let list = state
        .blocks()
        .await
        .map_err(|e| format!("blocks failed: {e}"))?;
    ensure_eq!(list.len(), 1, "one domain produced more than one entry");
    Ok(())
}

async fn a_lift_is_dated_and_gives_the_domain_back(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    let block = BlockRow::new(BLOCKED_DOMAIN, BECAUSE, T0);
    apply(state, std::slice::from_ref(&block)).await?;

    let lifted_ms = T0 + DAY;
    let lift = block.lift("they changed their minds, ticket 41", lifted_ms);
    let report = apply(state, &[lift]).await?;
    ensure_eq!(report.restored, 3, "a lift did not give the urls back");

    let leases = lease_one(state, lifted_ms).await?;
    ensure!(!leases.is_empty(), "a lifted domain is still not leasable");

    // Doc 07.7 wants a dated record of both events, so the entry stays and
    // carries both. A lift that deleted the row would leave nobody able to say
    // what happened or when.
    let list = state
        .blocks()
        .await
        .map_err(|e| format!("blocks failed: {e}"))?;
    let entry = list
        .first()
        .ok_or_else(|| "a lifted block left no record behind".to_owned())?;
    ensure_eq!(entry.blocked_ms, T0, "the block date was lost by the lift");
    ensure_eq!(entry.lifted_ms, Some(lifted_ms), "the lift was not dated");
    ensure!(entry.reason == BECAUSE, "the original reason was lost");
    ensure!(
        !entry.lifted_reason.is_empty(),
        "the lift has no reason on it"
    );
    Ok(())
}

async fn the_block_list_reads_back_with_its_reason(state: &dyn State) -> Outcome {
    // The list is published, and the reason travels with it so that the record
    // explains itself years later. That makes the round trip part of the
    // contract rather than a convenience for the CLI.
    let empty = state
        .blocks()
        .await
        .map_err(|e| format!("blocks failed: {e}"))?;
    ensure!(empty.is_empty(), "a fresh store already blocks something");

    let block = BlockRow::new(BLOCKED_DOMAIN, BECAUSE, T0);
    apply(state, std::slice::from_ref(&block)).await?;
    let list = state
        .blocks()
        .await
        .map_err(|e| format!("blocks failed: {e}"))?;
    ensure_eq!(list.as_slice(), &[block], "the block did not round trip");
    Ok(())
}

/// [`State::block`], with the error turned into the string the suite reports.
async fn apply(state: &dyn State, rows: &[BlockRow]) -> Result<crate::BlockReport, String> {
    state
        .block(rows)
        .await
        .map_err(|e| format!("block failed: {e}"))
}

/// Doc 05.7's reason, written the way a real one would be.
const SUPERVISED_BECAUSE: &str =
    "the archive asked us to mirror their catalogue, agreed 2026-08-20";

async fn nothing_reaches_t4_without_an_allowlist_entry(state: &dyn State) -> Outcome {
    // The point of the whole tier. A fetcher that says it will run supervised
    // work still gets ordinary leases, because the allowlist is empty and
    // nothing else in the system can raise the ceiling that far.
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    let req = LeaseRequest {
        max_tier: Tier::Supervised,
        ..LeaseRequest::new(FetcherId::LOCAL, T0, 16)
    };
    let leases = state
        .lease(&req)
        .await
        .map_err(|e| format!("lease failed: {e}"))?;
    ensure!(
        !leases.is_empty(),
        "nothing was leasable, so this proves nothing"
    );
    ensure!(
        leases.iter().all(|lease| lease.tier < Tier::Supervised),
        "a lease reached T4 with an empty allowlist"
    );
    Ok(())
}

async fn an_allowlist_entry_is_the_only_route_to_t4(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    let entry = SupervisionRow::new(BLOCKED_DOMAIN, "tam", SUPERVISED_BECAUSE, T0);
    state
        .supervise(std::slice::from_ref(&entry))
        .await
        .map_err(|e| format!("supervise failed: {e}"))?;

    // A fetcher that has opted in gets the tier the list allows.
    let opted_in = LeaseRequest {
        max_tier: Tier::Supervised,
        ..LeaseRequest::new(FetcherId::LOCAL, T0, 16)
    };
    let leases = state
        .lease(&opted_in)
        .await
        .map_err(|e| format!("lease failed: {e}"))?;
    ensure!(
        leases.iter().all(|lease| lease.tier == Tier::Supervised),
        "an allowlisted domain did not lease at T4: {:?}",
        leases.iter().map(|lease| lease.tier).collect::<Vec<_>>()
    );

    Ok(())
}

async fn an_allowlist_entry_does_not_overrule_the_fetcher(state: &dyn State) -> Outcome {
    // The allowlist raises a ceiling, it does not push work up to it. A fetcher
    // with no browser and nobody watching it says so in its lease request, and
    // an entry on the list is not allowed to argue.
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    state
        .supervise(&[SupervisionRow::new(
            BLOCKED_DOMAIN,
            "tam",
            SUPERVISED_BECAUSE,
            T0,
        )])
        .await
        .map_err(|e| format!("supervise failed: {e}"))?;

    let leases = lease_one(state, T0).await?;
    ensure!(
        !leases.is_empty(),
        "nothing was leasable, so this proves nothing"
    );
    ensure!(
        leases.iter().all(|lease| lease.tier < Tier::Supervised),
        "a fetcher that did not opt in was handed supervised work"
    );
    Ok(())
}

async fn taking_a_domain_off_the_allowlist_takes_t4_with_it(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..3).map(path_url).collect();
    admit_all(state, &urls).await?;
    let entry = SupervisionRow::new(BLOCKED_DOMAIN, "tam", SUPERVISED_BECAUSE, T0);
    state
        .supervise(std::slice::from_ref(&entry))
        .await
        .map_err(|e| format!("supervise failed: {e}"))?;
    let removed_ms = T0 + DAY;
    let off = entry.remove("the mirror is finished", removed_ms);
    state
        .supervise(&[off])
        .await
        .map_err(|e| format!("supervise failed: {e}"))?;

    let req = LeaseRequest {
        max_tier: Tier::Supervised,
        ..LeaseRequest::new(FetcherId::LOCAL, removed_ms, 16)
    };
    let leases = state
        .lease(&req)
        .await
        .map_err(|e| format!("lease failed: {e}"))?;
    ensure!(
        !leases.is_empty(),
        "nothing was leasable, so this proves nothing"
    );
    ensure!(
        leases.iter().all(|lease| lease.tier < Tier::Supervised),
        "a domain taken off the allowlist still leased at T4"
    );

    // The record stays, dated, because the published list is the disclosure
    // and a deleted row is a record only whoever deleted it can describe.
    let list = state
        .supervision()
        .await
        .map_err(|e| format!("supervision failed: {e}"))?;
    let held = list
        .first()
        .ok_or_else(|| "a removed entry left no record behind".to_owned())?;
    ensure_eq!(list.len(), 1, "one domain produced more than one entry");
    ensure_eq!(held.added_ms, T0, "the date it went on the list was lost");
    ensure_eq!(
        held.removed_ms,
        Some(removed_ms),
        "the removal was not dated"
    );
    ensure!(held.operator == "tam", "the operator was lost");
    ensure!(held.reason == SUPERVISED_BECAUSE, "the reason was lost");
    Ok(())
}

async fn a_segment_record_round_trips(state: &dyn State) -> Outcome {
    let row = published(1);
    state
        .put_segment(std::slice::from_ref(&row))
        .await
        .map_err(|e| format!("put_segment failed: {e}"))?;

    let read = state
        .segment(row.id)
        .await
        .map_err(|e| format!("segment failed: {e}"))?
        .ok_or_else(|| "a segment that was just written read back as none".to_owned())?;
    ensure_eq!(
        read,
        row,
        "the segment record did not survive the round trip"
    );
    Ok(())
}

async fn an_unknown_segment_reads_back_as_none(state: &dyn State) -> Outcome {
    // Doc 12.8 acts on this answer rather than treating it as an error: a file
    // on the hub that no segment record claims is an orphan from a crash
    // between upload and manifest push, and deciding that needs the store to
    // say "never heard of it" plainly.
    let read = state
        .segment(segment_id(99))
        .await
        .map_err(|e| format!("segment failed: {e}"))?;
    ensure!(read.is_none(), "a segment nothing wrote read back as some");
    Ok(())
}

async fn put_segment_replaces_rather_than_merges(state: &dyn State) -> Outcome {
    let sealed = sealed(1);
    state
        .put_segment(std::slice::from_ref(&sealed))
        .await
        .map_err(|e| format!("put_segment failed: {e}"))?;
    let published = published(1);
    state
        .put_segment(std::slice::from_ref(&published))
        .await
        .map_err(|e| format!("the second put_segment failed: {e}"))?;

    let read = state
        .segment(sealed.id)
        .await
        .map_err(|e| format!("segment failed: {e}"))?
        .ok_or_else(|| "the segment vanished".to_owned())?;
    ensure_eq!(
        read,
        published,
        "the second write did not replace the first"
    );
    Ok(())
}

async fn a_sealed_segment_is_unpublished_until_it_has_a_remote_copy(state: &dyn State) -> Outcome {
    state
        .put_segment(&[sealed(1), sealed(2)])
        .await
        .map_err(|e| format!("put_segment failed: {e}"))?;

    let backlog = state
        .segments(SegmentQuery::Unpublished)
        .await
        .map_err(|e| format!("segments failed: {e}"))?;
    ensure_eq!(backlog.len(), 2, "both sealed segments are unpublished");

    state
        .put_segment(&[published(1)])
        .await
        .map_err(|e| format!("the second put_segment failed: {e}"))?;
    let backlog = state
        .segments(SegmentQuery::Unpublished)
        .await
        .map_err(|e| format!("segments failed: {e}"))?;
    ensure_eq!(backlog.len(), 1, "one is on the hub now");
    ensure_eq!(
        backlog[0].id,
        segment_id(2),
        "the wrong segment is still in the backlog"
    );
    Ok(())
}

async fn a_segment_is_collectable_only_once_the_ledger_is_complete(state: &dyn State) -> Outcome {
    // Doc 12.7's fourth condition, which is the one this table exists for. A
    // segment on the hub whose manifest has not been pushed yet is not
    // collectable, because the chain that proves it is there is not committed.
    let half = SegmentRow {
        manifest_day: None,
        ..published(1)
    };
    state
        .put_segment(&[sealed(2), half])
        .await
        .map_err(|e| format!("put_segment failed: {e}"))?;

    let ready = state
        .segments(SegmentQuery::Collectable)
        .await
        .map_err(|e| format!("segments failed: {e}"))?;
    ensure!(
        ready.is_empty(),
        "a segment with no manifest entry must not be collectable, found {}",
        ready.len()
    );

    state
        .put_segment(&[published(1)])
        .await
        .map_err(|e| format!("the second put_segment failed: {e}"))?;
    let ready = state
        .segments(SegmentQuery::Collectable)
        .await
        .map_err(|e| format!("segments failed: {e}"))?;
    ensure_eq!(ready.len(), 1, "the manifest landed, so it is collectable");
    ensure_eq!(ready[0].id, segment_id(1), "the wrong segment came back");
    Ok(())
}

async fn a_deleted_segment_is_not_offered_for_collection_again(state: &dyn State) -> Outcome {
    let row = published(1);
    state
        .put_segment(std::slice::from_ref(&row))
        .await
        .map_err(|e| format!("put_segment failed: {e}"))?;
    let gone = SegmentRow {
        deleted_at_ms: Some(T0 + DAY),
        ..row
    };
    state
        .put_segment(std::slice::from_ref(&gone))
        .await
        .map_err(|e| format!("the second put_segment failed: {e}"))?;

    let ready = state
        .segments(SegmentQuery::Collectable)
        .await
        .map_err(|e| format!("segments failed: {e}"))?;
    ensure!(
        ready.is_empty(),
        "the local file is already gone, so there is nothing to collect"
    );
    // The record itself survives, because that is how an operator answers
    // "where did that segment go" after the local copy is deleted.
    let read = state
        .segment(gone.id)
        .await
        .map_err(|e| format!("segment failed: {e}"))?;
    ensure!(read.is_some(), "the record must outlive the file");
    Ok(())
}

async fn segments_come_back_in_seal_order(state: &dyn State) -> Outcome {
    // Written out of order on purpose. A publisher working a backlog has to
    // take the oldest first, and a store that returned insertion order would
    // pass every other case in here.
    state
        .put_segment(&[sealed(3), sealed(1), sealed(2)])
        .await
        .map_err(|e| format!("put_segment failed: {e}"))?;

    let backlog = state
        .segments(SegmentQuery::Unpublished)
        .await
        .map_err(|e| format!("segments failed: {e}"))?;
    let order: Vec<Ulid> = backlog.iter().map(|row| row.id).collect();
    ensure_eq!(
        order,
        vec![segment_id(1), segment_id(2), segment_id(3)],
        "the backlog is not in seal order"
    );
    Ok(())
}

async fn a_seal_window_is_half_open(state: &dyn State) -> Outcome {
    state
        .put_segment(&[sealed(1), sealed(2), sealed(3)])
        .await
        .map_err(|e| format!("put_segment failed: {e}"))?;

    let window = state
        .segments(SegmentQuery::SealedBetween {
            from_ms: T0 + 1,
            to_ms: T0 + 3,
        })
        .await
        .map_err(|e| format!("segments failed: {e}"))?;
    let order: Vec<Ulid> = window.iter().map(|row| row.id).collect();
    ensure_eq!(
        order,
        vec![segment_id(1), segment_id(2)],
        "the window must include its start and exclude its end, so that doc 12.8 \
         can walk consecutive days without seeing a segment twice"
    );
    Ok(())
}

async fn warming_a_domain_the_store_has_never_seen_is_not_an_error(state: &dyn State) -> Outcome {
    state
        .warm(&[PldId::derive(b"nowhere.invalid")])
        .await
        .map_err(|e| format!("warming an unknown domain failed: {e}"))?;
    Ok(())
}

async fn evicting_a_domain_that_is_not_resident_is_not_an_error(state: &dyn State) -> Outcome {
    let report = state
        .evict(&[PldId::derive(b"nowhere.invalid")])
        .await
        .map_err(|e| format!("evicting an unknown domain failed: {e}"))?;
    ensure_eq!(
        report.evicted,
        0,
        "a domain the store has never seen was reported as evicted"
    );
    Ok(())
}

async fn resident_is_sorted_and_free_of_duplicates(state: &dyn State) -> Outcome {
    // Six distinct registrable domains, not six hosts under one, because
    // residency is per pay level domain.
    let urls: Vec<String> = (0..6).map(|n| format!("https://example{n}.com/")).collect();
    admit_all(state, &urls).await?;
    let plds: Vec<_> = urls.iter().map(|url| key(url).pld).collect();
    state
        .warm(&plds)
        .await
        .map_err(|e| format!("warm failed: {e}"))?;

    let resident = state
        .resident()
        .await
        .map_err(|e| format!("resident failed: {e}"))?;
    ensure!(
        resident.windows(2).all(|pair| pair[0] < pair[1]),
        "resident is not sorted, or repeats a domain: {resident:?}"
    );
    Ok(())
}

async fn a_domain_the_store_holds_a_url_for_is_local(state: &dyn State) -> Outcome {
    // Doc 09.8 rebuilds the domain rate limits from `resident` when a
    // coordinator comes back up. A store that admits a URL and then does not
    // count its domain as local reads as an empty schedule after a restart, and
    // the crawl resumes into leasing nothing at all, which is not a failure any
    // caller can see from the outside.
    let urls: Vec<String> = (0..4)
        .map(|n| format!("https://local{n}.example/a"))
        .collect();
    admit_all(state, &urls).await?;

    let resident = state
        .resident()
        .await
        .map_err(|e| format!("resident failed: {e}"))?;
    for url in &urls {
        let pld = key(url).pld;
        ensure!(
            resident.contains(&pld),
            "{url} was admitted and its domain is not local: {resident:?}"
        );
    }
    Ok(())
}

async fn stats_account_for_what_was_admitted(state: &dyn State) -> Outcome {
    let urls: Vec<String> = (0..7).map(path_url).collect();
    admit_all(state, &urls).await?;
    let stats = state
        .stats()
        .await
        .map_err(|e| format!("stats failed: {e}"))?;
    ensure_eq!(
        stats.urls_seen,
        7,
        "the seen set does not hold what was admitted"
    );
    ensure_eq!(
        stats.urls_pending,
        7,
        "the frontier does not hold what was admitted"
    );
    ensure_eq!(stats.leases_in_flight, 0, "nothing was leased yet");

    let leases = lease_one(state, T0).await?;
    let after = state
        .stats()
        .await
        .map_err(|e| format!("stats failed: {e}"))?;
    ensure_eq!(
        after.leases_in_flight,
        leases.len() as u64,
        "in flight leases were not counted"
    );
    Ok(())
}

async fn checkpoint_sequence_is_monotonic(state: &dyn State) -> Outcome {
    let first = state
        .checkpoint(T0)
        .await
        .map_err(|e| format!("checkpoint failed: {e}"))?;
    let second = state
        .checkpoint(T0)
        .await
        .map_err(|e| format!("checkpoint failed: {e}"))?;
    ensure!(
        second.sequence > first.sequence,
        "checkpoint {} did not follow {}",
        second.sequence,
        first.sequence
    );
    Ok(())
}

async fn a_checkpoint_names_the_canonicalisation_its_keys_are_under(state: &dyn State) -> Outcome {
    let checkpoint = state
        .checkpoint(T0)
        .await
        .map_err(|e| format!("checkpoint failed: {e}"))?;
    ensure_eq!(
        checkpoint.canon_version.as_str(),
        CANON_VERSION,
        "a checkpoint that does not name its canonicalisation cannot be joined against"
    );
    Ok(())
}

async fn a_checkpoint_is_stamped_with_the_time_it_was_given(state: &dyn State) -> Outcome {
    // Nothing in this crate reads a clock, so the only time a backend has is
    // the one it was handed. A backend that ignores it leaves the operator
    // with a set of snapshot files nothing can date, which defeats most of
    // what doc 15's dashboard wants them for.
    let checkpoint = state
        .checkpoint(T0)
        .await
        .map_err(|e| format!("checkpoint failed: {e}"))?;
    ensure_eq!(
        checkpoint.taken_ms,
        T0,
        "the checkpoint was stamped with a time nobody passed in"
    );
    Ok(())
}
