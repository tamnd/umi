#!/usr/bin/env python3
"""Doc 16's gate 2.1: what fraction of the web needs a browser.

Reads the crawl that `gate-2-1-sample.sh` seeded, joins every row back to the
rank stratum its url was drawn from, and prints the share of pages answered at
each tier.

    ./scripts/gate-2-1.py \\
        --crawl /root/gate-2-1/crawl \\
        --strata /root/gate-2-1/strata.csv

Doc 05.1's assumption is roughly 95 percent at T0 and T1, 5 to 9 percent at T2
and under 1 percent at T3. The number that decides anything is T3: doc 01's
capacity plan sizes the browser pool for 1 percent, and at 5 percent the pool
becomes the bottleneck for the whole project and the honest response is to
narrow the scope rather than to buy more machines.

Two things are reported and they are not the same:

    reached      the tier the row was finally served at, from `tier_used`
    tried        every tier the ladder walked on the way, from `tier_path`

A page that succeeded at T1 has a path of length one and cost one request. A
page that was refused at T1 and served at T2 cost two, and the difference is
what the capacity plan is actually made of. Reporting only `tier_used` would
undercount the work by exactly the number of failed cheap attempts.

Reads the crawl through `umi cat`, so it needs no parquet library.
"""

import argparse
import collections
import csv
import json
import os
import subprocess
import sys

# Doc 05.1's table, and the reason this script exists is to find out whether it
# is true. Written down here so the report prints the assumption next to the
# measurement instead of leaving the reader to go and look it up.
ASSUMED = {0: None, 1: 0.95, 2: 0.07, 3: 0.01, 4: 0.0}

TIER_NAMES = {0: "T0 revalidate", 1: "T1 plain", 2: "T2 emulated",
              3: "T3 rendered", 4: "T4 supervised"}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--crawl", required=True, help="the umi crawl directory")
    ap.add_argument("--strata", required=True,
                    help="the csv gate-2-1-sample.sh wrote")
    ap.add_argument("--umi", default="umi", help="the umi binary")
    ap.add_argument("--json", help="write the numbers here as well")
    args = ap.parse_args()

    strata = load_strata(args.strata)
    if not strata:
        die("the strata file is empty")
    rows = load_crawl(args.umi, args.crawl)
    if not rows:
        die("the crawl has no rows in it")

    print("sample:  %d urls over %d strata"
          % (len(strata), len(set(strata.values()))))
    print("crawled: %d rows, %d of them joined to a stratum"
          % (len(rows), sum(1 for url in rows if url in strata)))
    print()

    served, walked, refused = tally(rows, strata)
    overall(served, walked, refused)
    print()
    by_stratum(rows, strata)
    print()
    verdict(served)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({
                "sample_urls": len(strata),
                "crawled_rows": len(rows),
                "served_by_tier": served,
                "tried_by_tier": walked,
                "refused": refused,
            }, f, indent=2, sort_keys=True)

    # The gate is about T3 and nothing else. A T2 share above doc 05's range is
    # worth knowing and is not a reason to change the plan, because T2 is cheap
    # on cpu and its cost is a connection pool. T3 costs a browser.
    total = sum(served.values())
    t3 = (served.get(3, 0) + served.get(4, 0)) / total if total else 1.0
    sys.exit(0 if t3 < 0.05 else 1)


def load_strata(path):
    """url to stratum, as the sampler drew it."""
    out = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            out[row["url"]] = int(row["stratum"])
    return out


def load_crawl(umi, directory):
    """One record per url, the tier it was served at and the path it walked."""
    rows = {}
    for path in files_in(directory):
        proc = subprocess.run(
            [umi, "cat", path, "--columns", "url,status,tier_used,tier_path"],
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
            if not row.get("url"):
                continue
            rows[row["url"]] = {
                "status": row.get("status", 0),
                "tier_used": row.get("tier_used", 0),
                "tier_path": row.get("tier_path") or [],
            }
    return rows


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


def tally(rows, strata):
    """Served, tried and refused, over the rows that came from the sample."""
    served = collections.Counter()
    walked = collections.Counter()
    refused = collections.Counter()
    for url, row in rows.items():
        if url not in strata:
            continue
        served[row["tier_used"]] += 1
        path = row["tier_path"] or [row["tier_used"]]
        for tier in path:
            walked[tier] += 1
        # Every tier on the path except the last one was tried and did not
        # produce the page. That count is the wasted work, and it is the part
        # of the capacity plan that a tier share on its own does not show.
        for tier in path[:-1]:
            refused[tier] += 1
    return dict(served), dict(walked), dict(refused)


def overall(served, walked, refused):
    total = sum(served.values())
    print("tier share over %d pages" % total)
    print("  %-16s %10s %10s %10s %10s"
          % ("", "served", "share", "doc 05", "refused"))
    for tier in sorted(set(served) | set(walked)):
        count = served.get(tier, 0)
        share = count / total if total else 0.0
        assumed = ASSUMED.get(tier)
        print("  %-16s %10d %9.2f%% %10s %10d"
              % (TIER_NAMES.get(tier, "T%d" % tier), count, 100.0 * share,
                 "-" if assumed is None else "%.0f%%" % (100.0 * assumed),
                 refused.get(tier, 0)))
    attempts = sum(walked.values())
    print("  %d fetches for %d pages, %.2f attempts a page"
          % (attempts, total, attempts / total if total else 0.0))


def by_stratum(rows, strata):
    """The same share per rank decade, which is the point of stratifying.

    If the shares are flat across strata then rank does not predict how much a
    site fights a crawler and a uniform sample would have been fine after all.
    If they slope, the slope is the finding, and any number quoted without the
    stratum it came from is meaningless.
    """
    per = collections.defaultdict(collections.Counter)
    for url, row in rows.items():
        if url in strata:
            per[strata[url]][row["tier_used"]] += 1
    print("by rank stratum")
    print("  %-8s %8s %8s %8s %8s %8s"
          % ("stratum", "pages", "T0+T1", "T2", "T3", "T4"))
    for stratum in sorted(per):
        counts = per[stratum]
        total = sum(counts.values())
        cheap = counts.get(0, 0) + counts.get(1, 0)
        print("  %-8s %8d %7.1f%% %7.2f%% %7.2f%% %7.2f%%"
              % (label(stratum), total,
                 100.0 * cheap / total,
                 100.0 * counts.get(2, 0) / total,
                 100.0 * counts.get(3, 0) / total,
                 100.0 * counts.get(4, 0) / total))


def label(stratum):
    """The rank decade a stratum covers, as a person would say it."""
    return ("1-1k", "1k-10k", "10k-100k", "100k-1M",
            "1M-10M", "10M-100M", "100M+")[stratum - 1]


def verdict(served):
    total = sum(served.values())
    if not total:
        print("VERDICT: nothing joined, so the gate is not measured")
        return
    t3 = (served.get(3, 0) + served.get(4, 0)) / total
    print("VERDICT: %.2f%% of pages needed a browser, against doc 05's 1 "
          "percent" % (100.0 * t3))
    if t3 < 0.05:
        print("         under the 5 percent that would break doc 01's "
              "capacity plan, so the plan stands")
    else:
        print("         over 5 percent, so doc 01's browser pool is undersized")
        print("         by roughly %.0f times and section 16.8 says the answer"
              % (t3 / 0.01))
        print("         is to narrow the scope rather than to buy machines")


def die(message):
    print("gate 2.1: %s" % message, file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
