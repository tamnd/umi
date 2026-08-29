#!/usr/bin/env python3
"""What Common Crawl has for the urls doc 16's gate 2.5 is measuring.

The gate is a comparison and this is the other half of it. For every url the
reference clock recorded, find the earliest time Common Crawl fetched it, or
record that it never did.

One query per index rather than one query per url. The CDX API takes a domain
match and pages through everything it holds for that domain, so a run over
twenty thousand urls is a few hundred requests instead of twenty thousand, and
the filtering happens here. That is kinder to an index everybody depends on
and it is also faster.

    ./scripts/gate-2-5-cc.py \\
        --reference /root/gate-2-5/reference.jsonl \\
        --domain aljazeera.com \\
        --out /root/gate-2-5/commoncrawl.jsonl

A url with no answer is written as `cc_first_ms: null` rather than left out,
because "Common Crawl does not have this" is the finding and a missing line
would look like a bug in the script.

The indexes searched are the ones whose window ends after the first reference
event. A Common Crawl snapshot is published weeks after the fetches in it, so
a run made the day the fourteen days end will usually find that no index
covers the window at all. That is not a failure of this script, it is the
measurement, and `gate-2-5.py` reports it as its own line.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

AGENT = "umi-gate-2-5/1.0 (+https://umi.dev/bot)"
COLLINFO = "https://index.commoncrawl.org/collinfo.json"

# The index is a shared service and this script is not in a hurry. One request
# every two seconds is slower than the API would allow and it costs a run over
# a domain a few minutes, which is nothing next to fourteen days of polling.
DELAY = 2.0

TRACKING = ("traffic_source", "fbclid", "gclid", "gb", "prx_t", "_ptid")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--reference", required=True,
                    help="the jsonl gate-2-5-poll.py wrote")
    ap.add_argument("--domain", required=True,
                    help="the registrable domain to query, without a scheme")
    ap.add_argument("--out", required=True, help="the jsonl to write")
    ap.add_argument("--indexes", type=int, default=4,
                    help="how many recent indexes to search, default 4")
    args = ap.parse_args()

    wanted, since = load_reference(args.reference)
    if not wanted:
        print("gate 2.5: the reference clock is empty", file=sys.stderr)
        sys.exit(2)
    print("%d urls to look for, window opens %s"
          % (len(wanted), stamp(since)), file=sys.stderr)

    indexes = [c for c in collections()
               if to_ms(c["to"]) >= since][:args.indexes]
    if not indexes:
        print("no published index reaches into the window", file=sys.stderr)
    for index in indexes:
        print("searching %s, %s to %s"
              % (index["id"], index["from"], index["to"]), file=sys.stderr)

    first = {}
    for index in indexes:
        for url, at in walk(index["cdx-api"], args.domain):
            key = normalise(url)
            if key not in wanted:
                continue
            if key not in first or at < first[key]:
                first[key] = at

    with open(args.out, "w", encoding="utf-8") as f:
        for url in sorted(wanted):
            f.write(json.dumps({
                "url": url,
                "cc_first_ms": first.get(url),
                "indexes": [i["id"] for i in indexes],
            }, sort_keys=True) + "\n")
    print("%d of %d urls found in common crawl"
          % (len(first), len(wanted)), file=sys.stderr)


def collections():
    """Every published index, newest first, which is the order it comes in."""
    return json.loads(get(COLLINFO).decode("utf-8"))


def walk(api, domain):
    """Every record the index holds for a domain, as url and fetch time.

    The page count the API reports is an upper bound and the last few pages of
    a domain query usually 404, so a 404 is the end of the walk rather than a
    failure. Anything else is retried, because a 503 in the middle drops a
    page of results and the script would not otherwise notice it had.
    """
    pages = num_pages(api, domain)
    for page in range(pages):
        query = urllib.parse.urlencode({
            "url": "%s/*" % domain,
            "output": "json",
            "page": page,
        })
        body = page_of("%s?%s" % (api, query), page, pages)
        if body is None:
            break
        for line in body.decode("utf-8", "replace").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("status") != "200" or not row.get("url"):
                continue
            at = to_ms_stamp(row.get("timestamp"))
            if at is not None:
                yield row["url"], at
        time.sleep(DELAY)


def page_of(url, page, pages):
    """One page, or None when the walk has run off the end.

    Three tries, and the wait between them grows, which is what doc 07.6 asks
    of the crawler and there is no reason for a measurement script to behave
    worse towards a service it does not pay for.
    """
    wait = DELAY
    for attempt in range(3):
        try:
            return get(url)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            print("  page %d of %d: %s, try %d"
                  % (page, pages, e, attempt + 1), file=sys.stderr)
        except Exception as e:
            print("  page %d of %d: %s, try %d"
                  % (page, pages, str(e)[:120], attempt + 1), file=sys.stderr)
        time.sleep(wait)
        wait *= 4
    print("  page %d of %d gave up, its records are missing from this run"
          % (page, pages), file=sys.stderr)
    return b""


def num_pages(api, domain):
    query = urllib.parse.urlencode({
        "url": "%s/*" % domain,
        "output": "json",
        "showNumPages": "true",
    })
    try:
        answer = json.loads(get("%s?%s" % (api, query)).decode("utf-8"))
    except Exception as e:
        print("  no page count: %s" % str(e)[:120], file=sys.stderr)
        return 0
    time.sleep(DELAY)
    return int(answer.get("pages", 0))


def get(url):
    req = urllib.request.Request(url, headers={"user-agent": AGENT})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def load_reference(path):
    wanted = set()
    since = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("kind") not in ("item", "updated"):
                continue
            if not row.get("url"):
                continue
            wanted.add(normalise(row["url"]))
            at = row.get("first_seen_ms")
            if at is not None and (since is None or at < since):
                since = at
    return wanted, since or 0


def normalise(url):
    """The same rule `gate-2-5.py` joins on, kept in step by hand."""
    parts = urllib.parse.urlsplit(url)
    query = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query)
             if k.lower() not in TRACKING and not k.lower().startswith("utm_")]
    path = parts.path.rstrip("/") or "/"
    return urllib.parse.urlunsplit((
        parts.scheme.lower(), parts.netloc.lower(), path,
        urllib.parse.urlencode(query), ""))


def to_ms(text):
    """collinfo.json writes `2026-08-20T01:52:41`, in UTC."""
    import datetime
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return 0
    return int(parsed.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def to_ms_stamp(text):
    """A CDX timestamp is `20260721144908`, in UTC."""
    import datetime
    if not text or len(text) != 14:
        return None
    try:
        parsed = datetime.datetime.strptime(text, "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return int(parsed.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def stamp(ms):
    import datetime
    return datetime.datetime.fromtimestamp(
        ms / 1000.0, datetime.timezone.utc).isoformat()


if __name__ == "__main__":
    main()
