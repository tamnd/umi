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

Two numbers, and the second one is the one people forget:

    delivered    a row with status 200, counted by the tier that produced it
    refused      a row with any other status, counted by the deepest tier tried

The tier share is over delivered pages, because doc 05's table is about which
tier produces a page and a 403 produces nothing. But a page the ladder gave up
on is browser demand that this measurement cannot see, so the refused count is
printed next to the share as the bound on how wrong it could be. A run that
delivers 99 percent at T1 and refuses the other 1 percent has not shown that
one percent of the web needs a browser, it has shown that at most one percent
might.

Escalation happens across leases and not inside one fetch. Doc 05.7's tier
signal raises the floor on the host record and the next lease for that host
starts higher, so a page that was refused at T1 and later served at T2 is two
rows with paths of length one rather than one row with a path of length two.
That is why the same url can appear twice, and why the join keeps the delivered
row when there is one.

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

    joined = [url for url in rows if url in strata]
    print("sample:  %d urls over %d strata"
          % (len(strata), len(set(strata.values()))))
    print("crawled: %d urls answered, %d of them joined to a stratum"
          % (len(rows), len(joined)))
    print()

    delivered, refused, statuses = tally(rows, strata)
    outcomes(delivered, refused, statuses)
    print()
    overall(delivered, refused)
    print()
    by_stratum(rows, strata)
    print()
    verdict(delivered, refused)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({
                "sample_urls": len(strata),
                "answered_urls": len(joined),
                "delivered_by_tier": delivered,
                "refused_by_tier": refused,
                "statuses": statuses,
            }, f, indent=2, sort_keys=True)

    # The gate is about T3 and nothing else. A T2 share above doc 05's range is
    # worth knowing and is not a reason to change the plan, because T2 is cheap
    # on cpu and its cost is a connection pool. T3 costs a browser.
    total = sum(delivered.values())
    t3 = (delivered.get(3, 0) + delivered.get(4, 0)) / total if total else 1.0
    sys.exit(0 if t3 < 0.05 else 1)


def load_strata(path):
    """url to stratum, as the sampler drew it."""
    out = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            out[row["url"]] = int(row["stratum"])
    return out


def load_crawl(umi, directory):
    """One record per url, and the best answer the ladder got for it.

    A url can have two rows: refused at T1, then delivered at T2 on the lease
    after the host record's floor went up. The delivered one is the answer, and
    where there is no delivered one the deepest tier tried is what the ladder
    got to before it gave up.
    """
    rows = {}
    for path in files_in(directory):
        proc = subprocess.run(
            [umi, "cat", path, "--columns", "url,status,tier_used"],
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
            new = {"status": row.get("status", 0),
                   "tier_used": row.get("tier_used", 0)}
            old = rows.get(row["url"])
            if old is None or better(new, old):
                rows[row["url"]] = new
    return rows


def better(new, old):
    """A 200 beats anything, and among failures the deeper tier is the answer."""
    if (new["status"] == 200) != (old["status"] == 200):
        return new["status"] == 200
    return new["tier_used"] > old["tier_used"]


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
    """Delivered and refused by tier, plus the statuses behind the refusals."""
    delivered = collections.Counter()
    refused = collections.Counter()
    statuses = collections.Counter()
    for url, row in rows.items():
        if url not in strata:
            continue
        if row["status"] == 200:
            delivered[row["tier_used"]] += 1
        else:
            refused[row["tier_used"]] += 1
            statuses[row["status"]] += 1
    return dict(delivered), dict(refused), dict(statuses)


def outcomes(delivered, refused, statuses):
    """What happened, before any of it is divided into tiers.

    Read this first. A tier share computed over delivered pages says nothing
    about the pages that were never delivered, and those are the ones the ladder
    was built for.
    """
    got = sum(delivered.values())
    lost = sum(refused.values())
    total = got + lost
    print("outcomes over %d urls" % total)
    print("  delivered %d, %.1f%%" % (got, 100.0 * got / total if total else 0))
    print("  refused   %d, %.1f%%" % (lost, 100.0 * lost / total if total else 0))
    if statuses:
        common = sorted(statuses.items(), key=lambda kv: -kv[1])[:8]
        print("  %s" % ", ".join(
            "%s x%d" % ("no answer" if code == 0 else code, count)
            for code, count in common))


def overall(delivered, refused):
    total = sum(delivered.values())
    print("tier share over %d delivered pages" % total)
    print("  %-16s %10s %10s %10s %10s"
          % ("", "delivered", "share", "doc 05", "refused"))
    for tier in sorted(set(delivered) | set(refused)):
        count = delivered.get(tier, 0)
        share = count / total if total else 0.0
        assumed = ASSUMED.get(tier)
        print("  %-16s %10d %9.2f%% %10s %10d"
              % (TIER_NAMES.get(tier, "T%d" % tier), count, 100.0 * share,
                 "-" if assumed is None else "%.0f%%" % (100.0 * assumed),
                 refused.get(tier, 0)))
    print("  the refused column is where the ladder stopped, not where it")
    print("  succeeded, so those pages are demand this run could not measure")


def by_stratum(rows, strata):
    """The same share per rank decade, which is the point of stratifying.

    If the shares are flat across strata then rank does not predict how much a
    site fights a crawler and a uniform sample would have been fine after all.
    If they slope, the slope is the finding, and any number quoted without the
    stratum it came from is meaningless.
    """
    per = collections.defaultdict(collections.Counter)
    lost = collections.Counter()
    for url, row in rows.items():
        if url not in strata:
            continue
        if row["status"] == 200:
            per[strata[url]][row["tier_used"]] += 1
        else:
            lost[strata[url]] += 1
    print("by rank stratum")
    print("  %-10s %8s %8s %8s %8s %8s %9s"
          % ("stratum", "pages", "T0+T1", "T2", "T3", "T4", "refused"))
    for stratum in sorted(set(per) | set(lost)):
        counts = per[stratum]
        total = sum(counts.values())
        if not total:
            print("  %-10s %8d" % (label(stratum), 0))
            continue
        cheap = counts.get(0, 0) + counts.get(1, 0)
        print("  %-10s %8d %7.1f%% %7.2f%% %7.2f%% %7.2f%% %9d"
              % (label(stratum), total,
                 100.0 * cheap / total,
                 100.0 * counts.get(2, 0) / total,
                 100.0 * counts.get(3, 0) / total,
                 100.0 * counts.get(4, 0) / total,
                 lost.get(stratum, 0)))


def label(stratum):
    """The rank decade a stratum covers, as a person would say it."""
    return ("1-1k", "1k-10k", "10k-100k", "100k-1M",
            "1M-10M", "10M-100M", "100M+")[stratum - 1]


def verdict(delivered, refused):
    total = sum(delivered.values())
    if not total:
        print("VERDICT: nothing was delivered, so the gate is not measured")
        return
    t3 = (delivered.get(3, 0) + delivered.get(4, 0)) / total
    print("VERDICT: %.2f%% of delivered pages needed a browser, against doc "
          "05's 1 percent" % (100.0 * t3))
    if t3 < 0.05:
        print("         under the 5 percent that would break doc 01's "
              "capacity plan, so the plan stands")
    else:
        print("         over 5 percent, so doc 01's browser pool is undersized")
        print("         by roughly %.0f times and section 16.8 says the answer"
              % (t3 / 0.01))
        print("         is to narrow the scope rather than to buy machines")
    # The honest upper bound. Every refused page is one the ladder did not
    # deliver, and there is no way from here to tell which of them a browser
    # would have got. If that number is large next to the T3 share then the
    # verdict above is not the whole answer and the run should say so rather
    # than let a reader take the first line on its own.
    lost = sum(refused.values())
    ceiling = (delivered.get(3, 0) + delivered.get(4, 0) + lost) / (total + lost)
    print("         %d pages were refused at every tier the ladder reached, so"
          % lost)
    print("         the true share is between %.2f%% and %.2f%%"
          % (100.0 * t3, 100.0 * ceiling))


def die(message):
    print("gate 2.1: %s" % message, file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
