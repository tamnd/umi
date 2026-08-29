#!/usr/bin/env python3
"""Doc 16's gate 2.5: freshness beats Common Crawl on a real domain.

Takes the reference clock that `gate-2-5-poll.py` wrote, the crawl directory
that `umi watch` filled, and optionally the Common Crawl answers that
`gate-2-5-cc.py` collected, and prints the staleness distribution.

Staleness at detection is `fetched_at_ms - published_ms`: the gap between the
publisher saying a story exists and umi having a row for it. The gate is a
median under six hours. Common Crawl's number for the same urls is the
comparison, and doc 00's claim is that it is around thirty days.

    ./scripts/gate-2-5.py \\
        --reference /root/gate-2-5/reference.jsonl \\
        --crawl /root/gate-2-5/crawl \\
        --cc /root/gate-2-5/commoncrawl.jsonl

Reads the crawl through `umi cat`, which emits one JSON object per row, so
this needs no parquet library and works on a `.umi` segment and a published
Parquet file the same way.

Every url that does not join is counted and reported. A freshness number
computed over the subset that happened to line up is not a freshness number,
and the coverage line is the one to read first.
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import urllib.parse

HOUR_MS = 3600 * 1000

# Query parameters that identify a referrer rather than a document. Al
# Jazeera's feed appends `traffic_source=rss` to every link and its own
# robots.txt disallows that form, so the url in the feed and the url umi
# crawled are never byte equal. Doc 11.2 canonicalises these away on the crawl
# side and this is the same list on the reference side.
TRACKING = ("traffic_source", "fbclid", "gclid", "gb", "prx_t", "_ptid")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--reference", required=True,
                    help="the jsonl gate-2-5-poll.py wrote")
    ap.add_argument("--crawl", required=True, help="the umi crawl directory")
    ap.add_argument("--umi", default="umi", help="the umi binary")
    ap.add_argument("--cc", help="the jsonl gate-2-5-cc.py wrote")
    ap.add_argument("--budget-hours", type=float, default=6.0,
                    help="the gate, default 6")
    ap.add_argument("--json", help="write the numbers here as well")
    args = ap.parse_args()

    reference = load_reference(args.reference)
    if not reference:
        die("the reference clock is empty, so there is nothing to measure")
    fetched = load_crawl(args.umi, args.crawl)
    if not fetched:
        die("the crawl has no rows in it")

    print("reference: %d events over %s"
          % (len(reference), window(reference)))
    print("crawl:     %d urls fetched" % len(fetched))
    print()

    fresh, backlog = drop_backlog(reference)
    print("backlog:   %d events were already published when the clock started"
          % backlog)
    print()
    if not fresh:
        die("every event is backlog, so nothing was observed happening")

    ours, missed, early = join(fresh, fetched)
    report("umi", ours, len(fresh), missed, early, args.budget_hours)

    theirs = None
    if args.cc:
        theirs = load_cc(args.cc, fresh)
        print()
        report_cc(theirs)

    print()
    verdict(ours, theirs, args.budget_hours)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({
                "reference_events": len(fresh),
                "reference_backlog": backlog,
                "umi_detected": len(ours),
                "umi_missed": missed,
                "umi_before_the_feed": early,
                "umi_staleness_ms": sorted(ours),
                "budget_hours": args.budget_hours,
            }, f, indent=2, sort_keys=True)

    ok = ours and statistics.median(ours) < args.budget_hours * HOUR_MS
    sys.exit(0 if ok else 1)


def load_reference(path):
    """One event per line, keyed by url and kept in the order it happened.

    An `updated` event is a second detection event on the same url, so the key
    is the url and the timestamp together rather than the url alone.
    """
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("kind") not in ("item", "updated"):
                continue
            if row.get("published_ms") is None or not row.get("url"):
                continue
            events.append({
                "url": normalise(row["url"]),
                "published_ms": row["published_ms"],
                "first_seen_ms": row.get("first_seen_ms"),
                "kind": row["kind"],
            })
    return events


def drop_backlog(reference):
    """Only the stories that were published while we were watching.

    The first poll of a feed returns everything currently on it, and those
    items were published hours or days before anybody started measuring. Their
    apparent staleness is their age at startup and has nothing to do with how
    fast the crawler is. Counting them would make a fourteen day run look
    worse the longer the feed's window is, which is a property of the feed.

    The cut is the first poll's own timestamp. An item published before that
    instant was not observed appearing, so it is not an event.
    """
    start = min(e["first_seen_ms"] for e in reference if e["first_seen_ms"])
    fresh = [e for e in reference if e["published_ms"] >= start]
    return fresh, len(reference) - len(fresh)


def load_crawl(umi, directory):
    """The earliest successful fetch of every url, out of every file we wrote.

    Earliest rather than latest because the gate is about detection: the row
    that matters is the first one, and a revisit an hour later does not make
    the discovery any fresher.
    """
    first = {}
    for path in files_in(directory):
        proc = subprocess.run(
            [umi, "cat", path, "--columns", "url,fetched_at_ms,status"],
            capture_output=True, text=True)
        if proc.returncode != 0:
            print("umi cat %s: %s" % (path, proc.stderr.strip()),
                  file=sys.stderr)
            continue
        for line in proc.stdout.splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("status") != 200 or not row.get("url"):
                continue
            url = normalise(row["url"])
            at = row["fetched_at_ms"]
            if url not in first or at < first[url]:
                first[url] = at
    return first


def files_in(directory):
    out = []
    for sub, suffix in (("data", ".parquet"), ("segments", ".umi")):
        d = os.path.join(directory, sub)
        if not os.path.isdir(d):
            continue
        for name in sorted(os.listdir(d)):
            if name.endswith(suffix):
                out.append(os.path.join(d, name))
    return out


def join(reference, fetched):
    """Staleness per event, plus the two ways an event does not produce one."""
    staleness = []
    missed = 0
    early = 0
    for event in reference:
        at = fetched.get(event["url"])
        if at is None:
            missed += 1
            continue
        gap = at - event["published_ms"]
        if gap < 0:
            # We had the url before the feed announced it, which happens when
            # a story is linked from a section page before it is syndicated.
            # Counted rather than folded in as a zero, because a negative gap
            # is a different fact from a fast one and averaging it in would
            # flatter the number.
            early += 1
            continue
        staleness.append(gap)
    return staleness, missed, early


def report(name, staleness, total, missed, early, budget_hours):
    if not staleness:
        print("%s: nothing joined" % name)
        return
    ordered = sorted(staleness)
    budget = budget_hours * HOUR_MS
    under = sum(1 for ms in ordered if ms < budget)
    print("%s staleness at detection, %d of %d events"
          % (name, len(ordered), total))
    for label, value in (("min", ordered[0]),
                         ("p25", pct(ordered, 25)),
                         ("median", pct(ordered, 50)),
                         ("p75", pct(ordered, 75)),
                         ("p90", pct(ordered, 90)),
                         ("max", ordered[-1])):
        print("  %-8s %s" % (label, human(value)))
    print("  under %g h  %d of %d, %.1f%%"
          % (budget_hours, under, len(ordered), 100.0 * under / len(ordered)))
    print("  never fetched  %d" % missed)
    print("  fetched before the feed said so  %d" % early)


def load_cc(path, reference):
    """What Common Crawl has for the same urls, as staleness or as absent."""
    answers = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if not row.get("url"):
                continue
            answers[normalise(row["url"])] = row.get("cc_first_ms")

    staleness = []
    absent = 0
    unknown = 0
    for event in reference:
        if event["url"] not in answers:
            unknown += 1
            continue
        at = answers[event["url"]]
        if at is None:
            absent += 1
            continue
        gap = at - event["published_ms"]
        if gap >= 0:
            staleness.append(gap)
    return {"staleness": staleness, "absent": absent, "unknown": unknown,
            "total": len(reference)}


def report_cc(cc):
    print("common crawl, same urls")
    if cc["staleness"]:
        ordered = sorted(cc["staleness"])
        for label, value in (("p25", pct(ordered, 25)),
                             ("median", pct(ordered, 50)),
                             ("p75", pct(ordered, 75))):
            print("  %-8s %s" % (label, human(value)))
    else:
        print("  no url in the window has appeared in a published index yet")
    print("  in an index but not these urls  %d" % cc["absent"])
    print("  no published index covers the window yet  %d" % cc["unknown"])
    print("  the second number is not a gap in the measurement. A Common Crawl")
    print("  snapshot is published weeks after the crawl it came from, and that")
    print("  latency is most of what doc 00's thirty days is made of.")


def verdict(ours, theirs, budget_hours):
    if not ours:
        print("VERDICT: no events joined, so the gate is not measured")
        return
    median = statistics.median(ours)
    passed = median < budget_hours * HOUR_MS
    print("VERDICT: median %s against a %g hour gate, %s"
          % (human(median), budget_hours, "pass" if passed else "fail"))
    if theirs and theirs["staleness"]:
        cc = statistics.median(theirs["staleness"])
        print("         common crawl's median on the same urls is %s, which is"
              % human(cc))
        print("         %.0f times slower" % (cc / max(median, 1)))
    elif theirs:
        print("         common crawl has published nothing covering the window,")
        print("         so its staleness is at least the age of the window")


def pct(ordered, p):
    if not ordered:
        return 0
    k = (len(ordered) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def human(ms):
    seconds = ms / 1000.0
    if seconds < 90:
        return "%.0f s" % seconds
    minutes = seconds / 60.0
    if minutes < 90:
        return "%.1f min" % minutes
    hours = minutes / 60.0
    if hours < 48:
        return "%.2f h" % hours
    return "%.1f days" % (hours / 24.0)


def normalise(url):
    """The same url written the same way on both sides of the join.

    Not a canonicaliser. Doc 11.2's is in the crawler and this only has to
    undo the differences between what a feed prints and what the crawler
    stored, which is the fragment, the tracking parameters and a trailing
    slash.
    """
    parts = urllib.parse.urlsplit(url)
    query = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query)
             if k.lower() not in TRACKING and not k.lower().startswith("utm_")]
    path = parts.path.rstrip("/") or "/"
    return urllib.parse.urlunsplit((
        parts.scheme.lower(), parts.netloc.lower(), path,
        urllib.parse.urlencode(query), ""))


def window(reference):
    first = min(e["first_seen_ms"] for e in reference if e["first_seen_ms"])
    last = max(e["first_seen_ms"] for e in reference if e["first_seen_ms"])
    return human(last - first)


def die(message):
    print("gate 2.5: %s" % message, file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
