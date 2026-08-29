#!/usr/bin/env python3
"""Check doc 16's gate 2.3 against an origin's arrival log.

The gate asks for the crawler to be judged from the server's logs rather than
from its own counters, so this reads the TSV that
`crates/umi-crawl/examples/adversarial_origin.rs` writes and never looks at the
crawler at all. Every check below is a statement about arrival times.

The interesting one is the third. Doc 07.6 is a small state machine and the
whole file is a running product, so a crawler can look polite on average while
getting every individual step wrong. This replays that state machine over the
behaviours the origin recorded and asserts that each gap was at least as long
as the delay the crawler should have been sitting on at that moment. It is a
lower bound rather than an equality because the gap also contains the response
time, which the origin knows and this does not bother to reconstruct.

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

# How much slack a gap gets. Two arrivals a second apart are measured by a
# server that timestamps them after the accept, so the number carries a
# scheduler wakeup on both sides. Fifty milliseconds is well under the smallest
# gap the crawler is allowed and well over the jitter.
TOLERANCE_MS = 50


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


def check(arrivals, restart_at_ms):
    """Run every check and return the failures, worst first."""
    failures = []
    notes = []

    pages = [a for a in arrivals if not a.is_robots]
    if len(pages) < 10:
        failures.append(f"only {len(pages)} page requests, the run did not happen")
        return failures, notes

    behaviours = {}
    for arrival in pages:
        behaviours[arrival.behaviour] = behaviours.get(arrival.behaviour, 0) + 1
    for wanted in ("ok", "slow", "429", "503", "reset"):
        if not behaviours.get(wanted):
            failures.append(f"no {wanted} request arrived, the gate did not cover it")
    notes.append("behaviours seen: " + ", ".join(f"{k} {v}" for k, v in sorted(behaviours.items())))

    # Check one, the floor. Doc 07.6 never lets a host see two requests inside
    # a second until it has answered fifty in a row quickly, and this origin is
    # never quick for fifty in a row.
    gaps = [(b.elapsed_ms - a.elapsed_ms, a, b) for a, b in zip(pages, pages[1:])]
    smallest, before, after = min(gaps, key=lambda g: g[0])
    notes.append(
        f"smallest gap between page requests: {smallest} ms, "
        f"{before.path} then {after.path}"
    )
    if smallest < DEFAULT_FLOOR_MS - TOLERANCE_MS:
        failures.append(
            f"{after.path} arrived {smallest} ms after {before.path}, "
            f"under the {DEFAULT_FLOOR_MS} ms floor"
        )

    # Check two, robots.txt. It is a request to the same origin and it counts
    # towards the same rate, so it is measured against the same floor. It is
    # reported on its own because it is the one request the crawl makes that no
    # lease pays for.
    for arrival, following in zip(arrivals, arrivals[1:]):
        if not arrival.is_robots:
            continue
        gap = following.elapsed_ms - arrival.elapsed_ms
        notes.append(f"robots.txt to {following.path}: {gap} ms")
        if gap < DEFAULT_FLOOR_MS - TOLERANCE_MS:
            failures.append(
                f"{following.path} arrived {gap} ms after robots.txt, "
                f"under the {DEFAULT_FLOOR_MS} ms floor"
            )

    # Check three, Retry-After. A crawler that dropped the header would still
    # back off four seconds on its own, so the origin asks for longer than that
    # and this is the difference between the two.
    asked = 0
    for arrival, following in zip(pages, pages[1:]):
        wanted = arrival.retry_after_ms
        if wanted is None:
            continue
        asked += 1
        gap = following.elapsed_ms - arrival.elapsed_ms
        notes.append(f"after a Retry-After of {wanted} ms the next request came in {gap} ms")
        if gap < wanted - TOLERANCE_MS:
            failures.append(
                f"{following.path} arrived {gap} ms after a 429 that asked for {wanted} ms"
            )
    if asked == 0:
        failures.append("no 429 was ever followed by another request, nothing was tested")

    # Check four, the backoff itself. Replay doc 07.6 over what the origin did
    # and require every gap to be at least the delay the crawler owed.
    pacer = Pacer()
    print(f"{'arrived':>10}  {'gap':>8}  {'owed':>8}  {'delay':>8}  path")
    for index, arrival in enumerate(pages):
        owed = pacer.observe(arrival)
        if index + 1 == len(pages):
            print(f"{arrival.elapsed_ms:>10}  {'':>8}  {'':>8}  {pacer.delay_ms:>8}  {arrival.path}")
            break
        gap = pages[index + 1].elapsed_ms - arrival.elapsed_ms
        mark = " " if gap >= owed - TOLERANCE_MS else " <-- too soon"
        print(
            f"{arrival.elapsed_ms:>10}  {gap:>8}  {owed:>8}  "
            f"{pacer.delay_ms:>8}  {arrival.path}{mark}"
        )
        if gap < owed - TOLERANCE_MS:
            failures.append(
                f"{pages[index + 1].path} arrived {gap} ms after {arrival.path}, "
                f"and doc 07.6 owed {owed} ms at that point"
            )

    # Check five, the restart. The delay lives in the state store, so a crawler
    # that came back up and started from the initial one second would show a
    # short gap here and nowhere else.
    if restart_at_ms is not None:
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
