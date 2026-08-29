#!/usr/bin/env python3
"""The reference clock for doc 16's gate 2.5.

The gate is median staleness at detection under six hours, meaning the gap
between the origin changing and umi noticing. That needs two clocks and they
have to be independent: one for when the origin changed, which is this, and
one for when umi noticed, which is the crawl's own rows.

Doc 16 says to measure against the origin's own timestamps where they exist
and against a high frequency reference poll where they do not. A news site's
feed gives both at once. Every item carries the publisher's own `pubDate`,
which is the origin timestamp and is the number the gate is really about, and
polling the feed often enough to catch items while they are fresh is the high
frequency part. It is also one request per cycle to one host, which is the
whole reason to poll a feed rather than a set of article urls: measuring
freshness by hammering a newsroom would be a poor way to demonstrate that we
are polite.

Each cycle appends one line per item the feed has not shown before, and one
line per item whose date moved, which is how a correction or an update to a
running story shows up. Nothing is ever rewritten, so a run that is killed and
restarted continues the same record.

    ./scripts/gate-2-5-poll.py \\
        --feed https://www.aljazeera.com/xml/rss/all.xml \\
        --out /root/gate-2-5/reference.jsonl \\
        --every 300 --days 14

Stdlib only, on purpose. This runs unattended for two weeks on a server that
has no business growing a dependency tree for the sake of a measurement.
"""

import argparse
import email.utils
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

# Identifiable and traceable, the same way doc 07.2 asks the crawler's own
# agent string to be. Somebody reading their access log during these two weeks
# should be able to find out what this is in one search.
AGENT = "umi-gate-2-5/1.0 (+https://umi.dev/bot)"

# Atom and Dublin Core, which is most of what a feed that is not plain RSS
# turns out to be.
ATOM = "{http://www.w3.org/2005/Atom}"
DC = "{http://purl.org/dc/elements/1.1/}"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--feed", action="append", required=True,
                    help="a feed url, repeatable")
    ap.add_argument("--out", required=True, help="the jsonl to append to")
    ap.add_argument("--every", type=int, default=300,
                    help="seconds between cycles, default 300")
    ap.add_argument("--days", type=float, default=14.0,
                    help="how long to run, default 14")
    args = ap.parse_args()

    deadline = time.time() + args.days * 86400.0
    seen = load_seen(args.out)
    print("%d items already on record in %s" % (len(seen), args.out),
          file=sys.stderr)

    while time.time() < deadline:
        started = time.time()
        for feed in args.feed:
            try:
                cycle(feed, args.out, seen)
            except Exception as e:
                # A poller that dies on one bad cycle loses the rest of the
                # window, and the window is two weeks long. Record the miss
                # and carry on, because a gap in the reference clock is a fact
                # the analysis needs rather than something to hide.
                append(args.out, {"kind": "poll_failed", "feed": feed,
                                  "at_ms": now_ms(), "error": str(e)[:200]})
        # From the start of the cycle rather than the end, so the poll interval
        # is the interval and not the interval plus however long the feed took.
        time.sleep(max(1.0, args.every - (time.time() - started)))


def cycle(feed, out, seen):
    """One fetch of one feed, and whatever it turned out to say."""
    at = now_ms()
    body = fetch(feed)
    for item in parse(body, feed):
        url = item["url"]
        published = item["published_ms"]
        known = seen.get(url)
        if known is None:
            seen[url] = published
            append(out, {"kind": "item", "feed": feed, "url": url,
                         "published_ms": published, "first_seen_ms": at,
                         "title": item["title"]})
        elif published is not None and known != published:
            # The publisher moved the date, so the story changed. A second
            # detection event on the same url, which is the shape doc 09's
            # revisit schedule is judged on rather than discovery.
            seen[url] = published
            append(out, {"kind": "updated", "feed": feed, "url": url,
                         "published_ms": published, "first_seen_ms": at,
                         "title": item["title"]})


def fetch(url):
    req = urllib.request.Request(url, headers={
        "user-agent": AGENT,
        "accept": "application/rss+xml, application/atom+xml, application/xml",
    })
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def parse(body, feed):
    """Every item in an RSS or Atom document, as url plus origin timestamp."""
    root = ET.fromstring(body)
    out = []
    for node in root.iter():
        tag = node.tag
        if tag not in ("item", ATOM + "entry"):
            continue
        url = link_of(node)
        if not url:
            continue
        out.append({
            "url": urllib.parse.urljoin(feed, url),
            "published_ms": date_of(node),
            "title": (text_of(node, "title") or text_of(node, ATOM + "title")
                      or "")[:200],
        })
    return out


def link_of(node):
    text = text_of(node, "link")
    if text:
        return text.strip()
    for child in node:
        if child.tag == ATOM + "link":
            rel = child.get("rel", "alternate")
            if rel == "alternate" and child.get("href"):
                return child.get("href").strip()
    # Some feeds put the canonical url in the guid and nowhere else.
    guid = text_of(node, "guid")
    if guid and guid.strip().startswith("http"):
        return guid.strip()
    return None


def date_of(node):
    """The publisher's own timestamp, in milliseconds, or None.

    The order matters. `pubDate` and Atom's `published` are when the story went
    out, which is the event the gate measures against. `updated` and Dublin
    Core's `date` are taken only when there is nothing better, because a feed
    that rewrites `updated` on every build would otherwise turn every cycle
    into a fake change event.
    """
    for tag in ("pubDate", ATOM + "published", DC + "date", ATOM + "updated"):
        raw = text_of(node, tag)
        if raw:
            ms = to_ms(raw.strip())
            if ms is not None:
                return ms
    return None


def text_of(node, tag):
    for child in node:
        if child.tag == tag:
            return child.text
    return None


def to_ms(raw):
    """RFC 822 first, since that is what RSS says, then ISO 8601 for Atom."""
    try:
        return int(email.utils.parsedate_to_datetime(raw).timestamp() * 1000)
    except (TypeError, ValueError):
        pass
    try:
        import datetime
        text = raw.replace("Z", "+00:00")
        parsed = datetime.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return int(parsed.timestamp() * 1000)
    except ValueError:
        return None


def load_seen(path):
    """What the record already holds, so a restart does not replay the feed."""
    seen = {}
    if not os.path.exists(path):
        return seen
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("kind") in ("item", "updated") and row.get("url"):
                seen[row["url"]] = row.get("published_ms")
    return seen


def append(path, row):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    # Opened and closed per line rather than held, because this process runs
    # for two weeks and the file it is writing is the only copy of the
    # reference clock. A held handle with a buffer in it is a fortnight of
    # measurement waiting to be lost to a kill.
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")
        f.flush()


def now_ms():
    return int(time.time() * 1000)


if __name__ == "__main__":
    main()
