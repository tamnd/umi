#!/usr/bin/env python3
"""Pull real pages out of a Common Crawl WARC file into a directory of HTML.

The golden corpus is 23 documents chosen to break things, which makes it a good
correctness test and a bad benchmark: it is 40 KiB of hand written HTML and the
real web is 150 KiB of generated markup with 400 inline styles in it. Doc 11.9
budgets 3 to 8 ms per page and that budget only means something measured against
pages somebody else wrote.

So this takes a WARC, walks it, and writes out the response bodies that came back
as HTML with a 200. Sampling is every Nth record rather than the first N, because
the first thousand records of a WARC are frequently one host.

Usage:
    scripts/bench-corpus.py <file.warc.gz> <out-dir> [count]

Get a WARC with ccrawl (github.com/tamnd/ccrawl-cli):
    ccrawl download warc -n 1 --flat --out ./warc
"""

import gzip
import os
import sys


def records(stream):
    """Yield (headers, body) for every WARC record in an open binary stream."""
    while True:
        line = stream.readline()
        if not line:
            return
        if not line.strip():
            continue
        if not line.startswith(b"WARC/"):
            raise ValueError(f"expected a record header, got {line[:40]!r}")
        headers = {}
        while True:
            line = stream.readline()
            if not line or line in (b"\r\n", b"\n"):
                break
            name, _, value = line.decode("latin-1").partition(":")
            headers[name.strip().lower()] = value.strip()
        body = stream.read(int(headers.get("content-length", 0)))
        stream.read(4)  # the two CRLFs between records
        yield headers, body


def payload(body):
    """The HTTP body and content type out of a WARC response record."""
    head, _, rest = body.partition(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    if not lines or len(lines[0].split()) < 2:
        return None, None
    try:
        status = int(lines[0].split()[1])
    except ValueError:
        return None, None
    if status != 200:
        return None, None
    content_type, encoding = "", ""
    for line in lines[1:]:
        name, _, value = line.decode("latin-1").partition(":")
        name = name.strip().lower()
        if name == "content-type":
            content_type = value.strip().lower()
        elif name == "content-encoding":
            encoding = value.strip().lower()
    if "gzip" in encoding:
        try:
            rest = gzip.decompress(rest)
        except (OSError, EOFError):
            return None, None
    return rest, content_type


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    source, out = sys.argv[1], sys.argv[2]
    wanted = int(sys.argv[3]) if len(sys.argv) > 3 else 2000
    stride = 7

    os.makedirs(out, exist_ok=True)
    written, seen, bytes_out = 0, 0, 0
    with gzip.open(source, "rb") as stream:
        for headers, body in records(stream):
            if headers.get("warc-type") != "response":
                continue
            content, content_type = payload(body)
            if content is None or "html" not in content_type:
                continue
            seen += 1
            if seen % stride:
                continue
            with open(os.path.join(out, f"{written:05d}.html"), "wb") as handle:
                handle.write(content)
            written += 1
            bytes_out += len(content)
            if written >= wanted:
                break

    mean = bytes_out / written / 1024 if written else 0
    print(f"{written} pages, {bytes_out / 1024 / 1024:.1f} MiB, {mean:.1f} KiB mean, in {out}")


if __name__ == "__main__":
    main()
