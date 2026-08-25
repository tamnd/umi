#!/usr/bin/env bash
# Build the wide golden corpus for doc 16's gate 1.2 out of real crawled pages.
#
# The small corpus in crates/umi-extract/corpus is 23 documents chosen because
# each one breaks an extractor in a specific way, and it lives in the repository
# because a reviewer should be able to read the diff. The wide corpus is the
# other half of doc 11.10: ten thousand pages nobody chose, off real hosts, in
# real encodings, with the real proportion of broken markup. It cannot live in
# the repository, because it is well over a gigabyte, so it is published as a
# dataset and only its digests are checked in.
#
# Usage, on a machine that has the recrawl output and duckdb:
#
#   scripts/build-golden-corpus.sh ~/ab-out ~/umi-golden
#
# The selection has to be reproducible or the digests mean nothing, so: one row
# per distinct body, html only, ordered by url, first ten thousand. Running it
# again over the same input produces the same file. Running it over different
# input produces a different corpus, which is why the corpus digest is recorded
# next to the extraction digests and checked before any of them are compared.
set -euo pipefail

input="${1:-$HOME/ab-out}"
out="${2:-$HOME/umi-golden}"
count="${3:-10000}"

mkdir -p "$out"

duckdb -c "
COPY (
    SELECT url, body
    FROM (
        SELECT DISTINCT ON (md5(body)) url, body
        FROM '$input/*.parquet'
        WHERE body IS NOT NULL
          AND octet_length(body) > 0
          AND content_type ILIKE '%html%'
        ORDER BY md5(body), url
    )
    ORDER BY url
    LIMIT $count
) TO '$out/wide.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 19);
"

duckdb -c "
SELECT
    count(*) AS pages,
    count(DISTINCT url) AS urls,
    sum(octet_length(body)) AS bytes
FROM '$out/wide.parquet';
"

ls -l "$out/wide.parquet"
b3sum "$out/wide.parquet" 2>/dev/null || sha256sum "$out/wide.parquet"
