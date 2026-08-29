#!/usr/bin/env python3
"""Check doc 16's gate 2.3 against an origin's arrival log.

The gate asks for the crawler to be judged from the server's logs rather than
from its own counters, so this reads the TSV that
`crates/umi-crawl/examples/adversarial_origin.rs` writes and never looks at the
crawler at all. Every check below is a statement about arrival times.

The one worth reading twice is the backoff check. Doc 07.6 is a small state
machine and the whole file is a running product, so a crawler can look polite
on average while getting every individual step wrong. This replays that state
machine over the behaviours the origin recorded, but it does not compare the
result to one gap at a time, and the reason is worth writing down: the state
layer spaces a batch of one host's urls when it leases them, using the delay it
knows at that moment. A response that arrives while the batch is running moves
the delay for the next batch and cannot move the requests already scheduled in
this one. So the honest outside statement is a cumulative one, allowing the
schedule to run up to a batch ahead of what the replay says it owes, and the
allowance is exactly `max_per_host` from the frontier's config.

`Retry-After` is checked separately and strictly, with no allowance at all,
because it is not a rate. It is an origin naming a time, and the crawler moves
the leases it is still holding when it hears one.

Run it as:

    scripts/check-politeness.py /tmp/adversarial-origin.tsv --restart-at 1234
"""

import argparse
import sys

# Doc 07.6's constants, copied from crates/umi-state/src/pace.rs. They are
# repeated rather than imported because the point of an outside check is that it
# does not share code with the thing it is checking. If somebody changes the
# multipliers in one place and not the other, this file failing is the correct
# outcome.
INITIAL_DELAY_MS = 1000
DEFAULT_FLOOR_MS = 1000
FAST_FLOOR_MS = 200
MAX_DELAY_MS = 60_000
FAST_MS = 500
SLOW_MS = 2000
FAST_STREAK = 50

# How many requests the schedule may run ahead of the replay. This is
# `max_per_host` from crates/umi-frontier/src/lib.rs, which is the most urls of
# one host the store puts in a single lease batch, and therefore the most that
# can already be spaced by a delay the origin has since changed.
LEASE_AHEAD = 8

# How much slack a gap gets. Two arrivals a second apart are measured by a
# server that timestamps them after the accept, so the number carries a
# scheduler wakeup on both sides. A tenth of the floor is well under any real
# violation and well over the jitter, which measured 40 ms on server3.
TOLERANCE_MS = 100


class Arrival:
    """One request, as the origin saw it."""

    def __init__(self, elapsed_ms, wall_ms, path, behaviour, detail):
        self.elapsed_ms = elapsed_ms
        self.wall_ms = wall_ms
        self.path = path
        self.behaviour = behaviour
        self.detail = detail

    @property
    def is_robots(self):
        return self.path == "/robots.txt"

    @property
    def retry_after_ms(self):
        """What this response asked the crawler to wait, if anything."""
        if self.detail.startswith("429 retry-after "):
            return int(self.detail.rsplit(" ", 1)[1]) * 1000
        return None

    def step(self):
        """This response's rung on doc 07.6's table, as a ratio.

        `None` means the table says nothing, which is the 404 case. The origin
        writes down what it did rather than what the crawler concluded, so the
        mapping from a behaviour to a rung is made here and it is the same
        mapping `factor` in pace.rs makes from a `FetchResult`.
        """
        if self.detail == "404":
            return None
        if self.behaviour == "429" and self.detail.startswith("429 "):
            return (4, 1)
        if self.behaviour == "503":
            return (4, 1)
        if self.behaviour == "reset":
            return (2, 1)
        # Everything left is a 200. Only the slow pages are over doc 07.6's two
        # second line, and they are slow because this origin dribbles the body
        # out rather than because the machine is busy.
        if self.behaviour == "slow":
            return (13, 10)
        return (9, 10)

    def was_fast(self):
        """Whether this counts towards the streak that earns the low floor."""
        return self.behaviour in ("ok", "429") and not self.detail.startswith("429 ")


def read_log(path):
    """Parse the origin's TSV into arrivals, in the order they landed."""
    out = []
    with open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            line = line.rstrip("\n")
            if not line:
                continue
            fields = line.split("\t")
            if len(fields) != 5:
                sys.exit(f"{path}:{number}: expected 5 fields, found {len(fields)}")
            out.append(
                Arrival(int(fields[0]), int(fields[1]), fields[2], fields[3], fields[4])
            )
    return out


class Pacer:
    """Doc 07.6's limiter, replayed from outside.

    Only the two numbers that decide a gap are kept, because they are the only
    two an arrival log can be checked against. The failure counters in the real
    `HostRow` feed doc 08.4's reporting and doc 05.8's tier moves, neither of
    which changes when a request is allowed to go out.
    """

    def __init__(self):
        self.delay_ms = INITIAL_DELAY_MS
        self.streak = 0

    @property
    def floor_ms(self):
        return FAST_FLOOR_MS if self.streak >= FAST_STREAK else DEFAULT_FLOOR_MS

    def observe(self, arrival):
        """Fold one response in and return the wait the crawler now owes."""
        step = arrival.step()
        if step is None:
            return self.delay_ms
        if arrival.was_fast():
            self.streak += 1
        else:
            self.streak = 0
        numerator, denominator = step
        scaled = self.delay_ms * numerator // denominator
        self.delay_ms = max(self.floor_ms, min(scaled, MAX_DELAY_MS))
        return max(self.delay_ms, arrival.retry_after_ms or 0)


def check_coverage(pages, failures, notes):
    """Every behaviour was reached, so the rest of the checks mean something."""
    behaviours = {}
    for arrival in pages:
        behaviours[arrival.behaviour] = behaviours.get(arrival.behaviour, 0) + 1
    for wanted in ("ok", "slow", "429", "503", "reset"):
        if not behaviours.get(wanted):
            failures.append(f"no {wanted} request arrived, the gate did not cover it")
    notes.append(
        "behaviours seen: "
        + ", ".join(f"{k} {v}" for k, v in sorted(behaviours.items()))
    )


def check_floor(arrivals, failures, notes):
    """Nothing ever arrives inside doc 07.6's floor.

    Every arrival counts, robots.txt included. It is a request to the same
    origin and it costs the same socket, and it is the one request in a crawl
    that no page lease pays for, which is exactly why a crawler can end up
    sending it alongside a page and never notice.
    """
    gaps = [(b.elapsed_ms - a.elapsed_ms, a, b) for a, b in zip(arrivals, arrivals[1:])]
    smallest, before, after = min(gaps, key=lambda g: g[0])
    widest, wide_before, wide_after = max(gaps, key=lambda g: g[0])
    notes.append(
        f"smallest gap between requests: {smallest} ms, {before.path} then {after.path}"
    )
    notes.append(
        f"largest gap between requests: {widest} ms, "
        f"{wide_before.path} then {wide_after.path}"
    )
    if smallest < DEFAULT_FLOOR_MS - TOLERANCE_MS:
        failures.append(
            f"{after.path} arrived {smallest} ms after {before.path}, "
            f"under the {DEFAULT_FLOOR_MS} ms floor"
        )


def check_retry_after(arrivals, failures, notes):
    """The window a 429 asked for came back empty.

    No allowance here. The origin named a time, and a crawler that had other
    work already scheduled for this host inside that window is supposed to move
    it rather than send it.
    """
    asked = 0
    for index, arrival in enumerate(arrivals):
        wanted = arrival.retry_after_ms
        if wanted is None:
            continue
        asked += 1
        until = arrival.elapsed_ms + wanted - TOLERANCE_MS
        inside = [a for a in arrivals[index + 1 :] if a.elapsed_ms < until]
        notes.append(
            f"a 429 at {arrival.elapsed_ms} ms asked for {wanted} ms and "
            f"{len(inside)} requests arrived inside it"
        )
        for late in inside:
            failures.append(
                f"{late.path} arrived {late.elapsed_ms - arrival.elapsed_ms} ms "
                f"after a 429 that asked for {wanted} ms"
            )
    if asked == 0:
        failures.append("no 429 was ever followed by another request, nothing was tested")


def check_backoff(pages, failures, notes):
    """The run took at least as long as doc 07.6's product says it should.

    Cumulative rather than gap by gap, for the reason in the module docstring:
    a batch is spaced when it is leased and a response cannot move what is
    already scheduled beside it. Allowing the schedule to run `LEASE_AHEAD`
    requests ahead of the replay is exactly that effect and no more, so a
    crawler that dropped the multipliers entirely still fails here, and fails
    early, because its whole run is shorter than the sum.
    """
    pacer = Pacer()
    owed = [pacer.observe(page) for page in pages]
    running = 0
    worst = None
    print(f"{'arrived':>10}  {'gap':>8}  {'owed':>8}  {'behind':>9}  path")
    for index, page in enumerate(pages):
        gap = pages[index + 1].elapsed_ms - page.elapsed_ms if index + 1 < len(pages) else 0
        ahead = index - LEASE_AHEAD
        if ahead >= 0:
            running += owed[ahead]
        elapsed = page.elapsed_ms - pages[0].elapsed_ms
        behind = running - elapsed
        if worst is None or behind > worst[0]:
            worst = (behind, page)
        print(
            f"{page.elapsed_ms:>10}  {gap:>8}  {owed[index]:>8}  "
            f"{behind:>9}  {page.path}"
        )
        if behind > TOLERANCE_MS:
            failures.append(
                f"{page.path} arrived {elapsed} ms into the run and doc 07.6 "
                f"owed {running} ms of waiting by then"
            )
    notes.append(
        f"furthest the crawler ever ran ahead of doc 07.6: {worst[0]} ms, "
        f"at {worst[1].path}"
    )


def check_restart(arrivals, restart_at_ms, failures, notes):
    """The delay lived in the state store rather than in the process.

    A crawler that came back up and started from the initial one second would
    show a short gap here and nowhere else, which is why this is measured on
    its own even though the floor check covers the same pair.
    """
    spanning = [
        (a, b)
        for a, b in zip(arrivals, arrivals[1:])
        if a.wall_ms <= restart_at_ms <= b.wall_ms
    ]
    if not spanning:
        failures.append("no request spans the restart, so it was not measured")
    for a, b in spanning:
        gap = b.elapsed_ms - a.elapsed_ms
        notes.append(f"across the restart: {a.path} then {b.path}, {gap} ms")
        if gap < DEFAULT_FLOOR_MS - TOLERANCE_MS:
            failures.append(
                f"{b.path} arrived {gap} ms after {a.path} across the restart"
            )


def check(arrivals, restart_at_ms):
    """Run every check and return the failures, worst first."""
    failures = []
    notes = []

    pages = [a for a in arrivals if not a.is_robots]
    if len(pages) < 10:
        failures.append(f"only {len(pages)} page requests, the run did not happen")
        return failures, notes

    check_coverage(pages, failures, notes)
    check_floor(arrivals, failures, notes)
    check_retry_after(arrivals, failures, notes)
    check_backoff(pages, failures, notes)
    if restart_at_ms is not None:
        check_restart(arrivals, restart_at_ms, failures, notes)

    return failures, notes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", help="the TSV the adversarial origin wrote")
    parser.add_argument(
        "--restart-at",
        type=int,
        default=None,
        help="unix milliseconds when the crawler was restarted",
    )
    args = parser.parse_args()

    arrivals = read_log(args.log)
    print(f"{len(arrivals)} requests in {args.log}")
    failures, notes = check(arrivals, args.restart_at)

    print()
    for note in notes:
        print(note)
    print()
    if failures:
        print(f"gate 2.3 failed, {len(failures)} problems")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print("gate 2.3 passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
