//! Doc 05.9's budget, tested as arithmetic.
//!
//! No crawl and no browser here. What the budget has to get right is the shape
//! of the answer over time, and that is a clock and a counter, so these drive
//! it with a number instead of a fixed clock and check the two things a wrong
//! budget would get wrong: letting more renders through than the pool can take,
//! and deferring work it had room for.

use std::time::Duration;

use crate::render::{RenderBudget, RenderPolicy, Slot};

const T0: u64 = 1_700_000_000_000;

fn budget(capacity: f64) -> RenderBudget {
    let budget = RenderBudget::new(RenderPolicy::default());
    budget.observe(Some(capacity));
    budget
}

#[test]
fn a_process_with_no_browser_says_so_rather_than_deferring() {
    let budget = RenderBudget::new(RenderPolicy::default());
    assert_eq!(budget.rate(), 0.0);
    assert_eq!(budget.take(T0), Slot::NoBrowser);
    // Not a deferral. Deferring here is the spin described on `Slot`, since
    // the lease would come straight back on the next tick and get the same
    // answer for the week until doc 05.8 probes the host down again.
    assert_eq!(budget.deferred(), 0);

    // And it stays that way when the fetcher says so, rather than falling back
    // to some default rate. A build without the render feature answers `None`
    // here, and reading that as unlimited would send every escalated page to a
    // browser that does not exist.
    budget.observe(None);
    assert_eq!(budget.take(T0), Slot::NoBrowser);
    assert_eq!(budget.granted(), 0);
}

#[test]
fn the_smaller_of_the_two_halves_wins() {
    // Doc 05.9's `min`. The pool is the binding constraint on the fleet as it
    // stands, which is the whole reason the formula has two terms: a box with a
    // faster browser than server2 still does not get to render more than one
    // percent of what it crawls.
    let policy = RenderPolicy::default();
    let fraction = policy.page_rate * policy.max_fraction;
    assert_eq!(fraction, 2.5);

    let slow = budget(1.8);
    assert!(
        slow.rate() <= 1.8,
        "the pool was not the binding constraint"
    );

    let fast = RenderBudget::new(policy);
    fast.observe(Some(40.0));
    assert!(
        fast.rate() <= fraction,
        "a fast browser got past the fraction"
    );
}

#[test]
fn renders_come_out_one_interval_apart() {
    // Two pages a second is one every 500 ms, and the times the budget hands
    // back are what the loop waits for. If they came back all at once the
    // browser would be handed the whole tick's rendering in the first
    // millisecond of it.
    let budget = budget(2.0);
    assert_eq!(budget.take(T0), Slot::At(T0));
    assert_eq!(budget.take(T0), Slot::At(T0 + 500));
    assert_eq!(budget.take(T0), Slot::At(T0 + 1000));
    assert_eq!(budget.granted(), 3);
    assert_eq!(budget.deferred(), 0);
}

#[test]
fn a_rate_that_does_not_divide_a_second_rounds_down_the_budget() {
    // 1.8 pages a second is 555.55 ms apart, and the interval is rounded up so
    // the answer comes out under the budget rather than over it. Over is the
    // direction that costs somebody else something: it means handing the
    // browser more than it measured itself able to do.
    let budget = budget(1.8);
    assert_eq!(budget.take(T0), Slot::At(T0));
    assert_eq!(budget.take(T0), Slot::At(T0 + 556));
    assert!(budget.rate() < 1.8);
}

#[test]
fn work_that_would_wait_too_long_is_deferred_rather_than_queued() {
    // The line between waiting and deferring, which is what makes this a
    // budget and not just a rate limiter. At one render a second and a five
    // second window, six pages can wait and the seventh goes back to the
    // frontier for a later tick to offer again.
    let budget = budget(1.0);
    for hop in 0..6 {
        assert_eq!(budget.take(T0), Slot::At(T0 + hop * 1000), "hop {hop}");
    }
    assert_eq!(budget.take(T0), Slot::Defer);
    assert_eq!(budget.granted(), 6);
    assert_eq!(budget.deferred(), 1);

    // A deferral takes no slot. If it did, a crawl asking for more rendering
    // than the fleet has would push the queue further out every time it asked
    // and eventually stop rendering at all.
    assert_eq!(budget.take(T0 + 6000), Slot::At(T0 + 6000));
}

#[test]
fn a_budget_that_was_idle_does_not_bank_the_time() {
    // The thing being rationed is a browser, and a browser cannot catch up.
    // An hour of no rendering does not buy an hour's worth of renders at once,
    // it buys one now and the rest at the usual spacing.
    let budget = budget(2.0);
    assert_eq!(budget.take(T0), Slot::At(T0));

    let later = T0 + 3_600_000;
    assert_eq!(budget.take(later), Slot::At(later));
    assert_eq!(budget.take(later), Slot::At(later + 500));
}

#[test]
fn the_budget_moves_when_the_pool_does() {
    // The pool measures itself as it renders, so the budget it reports on the
    // first tick is an estimate and the one it reports later is not. Doc 05.9's
    // own estimate is out by about a factor of two, which is exactly why this
    // is asked for again every tick rather than read once at startup.
    let budget = budget(2.0);
    assert_eq!(budget.take(T0), Slot::At(T0));
    assert_eq!(budget.take(T0), Slot::At(T0 + 500));

    budget.observe(Some(0.5));
    assert_eq!(budget.take(T0), Slot::At(T0 + 1000));
    assert_eq!(
        budget.take(T0),
        Slot::At(T0 + 3000),
        "the slower pace applied"
    );
}

#[test]
fn a_wait_of_zero_makes_it_a_gate() {
    // Not the default and not recommended, but it has to mean something
    // sensible: nothing waits, so the budget hands out one render per interval
    // to whoever asks at the right moment and defers everything else.
    let policy = RenderPolicy {
        wait: Duration::ZERO,
        ..RenderPolicy::default()
    };
    let budget = RenderBudget::new(policy);
    budget.observe(Some(2.0));

    assert_eq!(budget.take(T0), Slot::At(T0));
    assert_eq!(budget.take(T0), Slot::Defer);
    assert_eq!(budget.take(T0 + 500), Slot::At(T0 + 500));
}
