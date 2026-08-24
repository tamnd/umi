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

use std::fmt;
use std::future::Future;
use std::time::Duration;

use umi_types::{CANON_VERSION, FetcherId, PldId, RowKey, Tier};

use crate::{
    Candidate, Discovery, FailureKind, FetchOutcome, FetchResult, HostRow, LeaseRequest,
    NackReason, Priority, Revalidator, State, retry_after_ms,
};

/// A fixed instant to run every case from, so nothing in here depends on when
/// it was run. Roughly November 2023, chosen only for being a plausible
/// millisecond timestamp.
const T0: u64 = 1_700_000_000_000;

/// Milliseconds in a day, for the far future cases.
const DAY: u64 = 24 * 60 * 60 * 1000;

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
    run!(an_unverified_discovery_is_held_rather_than_admitted);
    run!(only_admitted_urls_are_ever_leased);
    run!(lease_never_returns_more_than_was_asked_for);
    run!(lease_honours_the_per_host_cap);
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
    run!(a_higher_priority_url_is_leased_first);
    run!(lease_returns_work_in_the_order_the_trait_promises);
    run!(a_host_politeness_timer_holds_the_whole_host);
    run!(a_host_record_round_trips);
    run!(an_unknown_host_reads_back_as_none);
    run!(put_host_replaces_rather_than_merges);
    run!(a_blocked_host_is_never_leased);
    run!(warming_a_domain_the_store_has_never_seen_is_not_an_error);
    run!(evicting_a_domain_that_is_not_resident_is_not_an_error);
    run!(resident_is_sorted_and_free_of_duplicates);
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
