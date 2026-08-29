#!/usr/bin/env bash
# The sample for doc 16's gate 2.1, tier share on 100000 stratified urls.
#
# Issue #33 says stratified and says why: a uniform sample of Common Crawl is
# a sample of pages, and pages are not spread evenly over sites. The biggest
# sites hold most of the urls and they are also the sites most likely to run
# serious bot management, so a uniform draw would report a tier share that is
# pessimistic by an amount nobody could put a number on. Stratifying by domain
# rank fixes that: every decade of the rank distribution contributes the same
# number of urls, so the answer is a property of the web rather than a property
# of whichever twenty sites happen to be enormous.
#
# Two datasets, both already published under open-index:
#
#   ccrawl-domains   364 million domains with a harmonic centrality rank
#   ccrawl-urls      2.1 billion urls from CC-MAIN-2026-25, with the domain
#
# The rank table is the april to june web graph and the url index is the june
# crawl, so the two line up in time and a domain sampled from one has a fair
# chance of appearing in the other.
#
# DuckDB does the work because it reads both straight off Hugging Face and only
# pulls the column chunks a query touches. Nothing is downloaded whole.
#
#     ./scripts/gate-2-1-sample.sh
#
# Writes into $OUT, default $HOME/gate-2-1:
#
#   seed.txt      one url per line, ready for umi crawl --seed
#   strata.csv    url, domain, rank, stratum, for the report to join on
#   sample.db     the duckdb file, kept so a rerun does not repeat the scan
set -euo pipefail

OUT="${OUT:-$HOME/gate-2-1}"
# 100000 total over 7 strata. Doc 16 and issue #33 both say 100000 and the
# number is not arbitrary: a tier share of 1 percent measured over 100000 urls
# has a standard error of about 0.03 percent, which is fine enough to tell
# doc 05's "under 1 percent" apart from the 5 percent that would break doc 01's
# capacity plan. Ten thousand urls would not be.
TOTAL="${TOTAL:-100000}"
# Far more domains than the arithmetic needs, for two reasons.
#
# The first is that a domain in the rank table is a node in the web graph and a
# node can be linked to without ever having been fetched, so plenty of them
# contribute no urls at all. That is not spread evenly: at rank one thousand
# four domains in five have urls in the index and out past ten million only one
# in five does. A draw sized for the head leaves the tail strata half empty,
# which is what the first full run did.
#
# The second is that tier behaviour is a property of a site rather than of a
# page. Thirty urls from one domain almost always answer at the same tier, so
# they are close to one observation and not thirty, and the sample size that
# decides how precise the answer is, is the number of domains. Drawing wide and
# trimming round robin at the end spends the same hundred thousand fetches
# across many more sites, which is strictly a better measurement.
PER_STRATUM_DOMAINS="${PER_STRATUM_DOMAINS:-7000}"
# The cap that stops one site filling a stratum on its own.
PER_DOMAIN_URLS="${PER_DOMAIN_URLS:-30}"
# Well under what the box has, because the out of memory killer does not care
# that this is a measurement and a run that dies at minute fifty has to start
# again from the top.
MEMORY="${MEMORY:-8GB}"

DOMAINS="${DOMAINS:-hf://datasets/open-index/ccrawl-domains/data/cc-main-2026-apr-may-jun/*.parquet}"
URLS="${URLS:-hf://datasets/open-index/ccrawl-urls/data/CC-MAIN-2026-25/*.parquet}"

mkdir -p "$OUT"
db="$OUT/sample.db"

echo "sampling into $db"
echo "this reads both datasets off hugging face and takes about an hour"

duckdb "$db" <<SQL
SET memory_limit = '$MEMORY';
SET temp_directory = '$OUT/duckdb-tmp';
SET preserve_insertion_order = false;

-- The last rank in the table, read rather than assumed. The tail stratum runs
-- from a hundred million to whatever the table actually ends at, and getting
-- that bound wrong would make the systematic step too coarse and draw a
-- handful of domains where it should draw a thousand.
CREATE OR REPLACE TABLE bounds AS
  SELECT max(harmonic_pos) AS top FROM '$DOMAINS';

-- Seven strata, one per decade of harmonic rank, and the last one running to
-- the end of the table. Equal allocation rather than proportional, because the
-- question is whether the tier share differs by rank and a proportional draw
-- would put almost every url in the last stratum and leave the head too thin
-- to say anything about.
CREATE OR REPLACE TABLE strata AS
  SELECT stratum, lo,
         CASE WHEN stratum = 7 THEN (SELECT top FROM bounds) ELSE hi END AS hi
  FROM (VALUES
    (1,         1,      1000),
    (2,      1001,     10000),
    (3,     10001,    100000),
    (4,    100001,   1000000),
    (5,   1000001,  10000000),
    (6,  10000001, 100000000),
    (7, 100000001, 100000001)
  ) t(stratum, lo, hi);

-- A systematic draw over the rank ordering: every (size // wanted)th rank in
-- the stratum. Deterministic, so a rerun produces the same sample and two runs
-- of the crawl are comparable, and even over the stratum by construction,
-- which a random draw would only be on average.
--
-- Integer division and not the plain slash. DuckDB divides with a float
-- whatever the operands are, so a thousand over four hundred is two and a
-- half, and a remainder against two and a half is zero only on multiples of
-- five. That quietly draws a fifth of what was asked for. In the tail stratum,
-- where the divisor is in the millions and its fractional part is tiny, it
-- drew exactly one domain.
CREATE OR REPLACE TABLE sampled_domains AS
  SELECT d.domain, d.harmonic_pos AS rank, s.stratum
  FROM strata s
  JOIN '$DOMAINS' d
    ON d.harmonic_pos BETWEEN s.lo AND s.hi
   AND (d.harmonic_pos - s.lo) % greatest(1, (s.hi - s.lo + 1) // $PER_STRATUM_DOMAINS) = 0;

SELECT stratum, count(*) AS domains, min(rank), max(rank)
FROM sampled_domains GROUP BY stratum ORDER BY stratum;

-- Only pages. A 404, a redirect and a pdf are all things the ladder answers at
-- tier 1 without learning anything, and counting them would dilute the share
-- towards T1 by an amount that depends on how much junk is in the index.
--
-- The bias this leaves is worth writing down rather than hiding: a site that
-- blocked Common Crawl outright has no rows here, so it cannot be sampled, and
-- those are exactly the sites most likely to need T2 or T3. The measured share
-- is therefore a floor. There is no way around it, because Common Crawl is the
-- only url population large enough to stratify by rank in the first place, and
-- the count of sampled domains that returned nothing is reported next to the
-- result as the size of the hole.
--
-- min_by and not a window function. The obvious way to write this is
-- row_number over a partition by domain with a qualify on the end, and it gets
-- killed by the out of memory killer, because a window has to hold every row of
-- every partition before it can number them and some of these domains have
-- millions of urls in the index. min_by keeps a heap of thirty per group and
-- nothing else, so the memory is eight thousand groups wide regardless of how
-- big the biggest site is. The urls it keeps are the ones with the smallest
-- hash, which is a deterministic draw rather than an arbitrary one.
CREATE OR REPLACE TABLE picks AS
  SELECT u.url_host_registered_domain AS domain, d.rank, d.stratum,
         min_by(u.url, hash(u.url), $PER_DOMAIN_URLS) AS urls
  FROM '$URLS' u
  JOIN sampled_domains d ON u.url_host_registered_domain = d.domain
  WHERE u.fetch_status = 200
    AND u.content_mime_detected = 'text/html'
  GROUP BY 1, 2, 3;

-- At most thirty urls a domain from here on, so the window below is cheap.
CREATE OR REPLACE TABLE candidates AS
  SELECT url, domain, rank, stratum,
         row_number() OVER (PARTITION BY domain ORDER BY hash(url)) AS pick
  FROM (SELECT domain, rank, stratum, unnest(urls) AS url FROM picks);

-- Round robin rather than a straight cut. Taking the first url from every
-- domain, then the second from every domain, and stopping when the stratum is
-- full spreads the trim across sites instead of filling the stratum from
-- whichever domains sort first.
CREATE OR REPLACE TABLE chosen AS
  SELECT url, domain, rank, stratum
  FROM (
    SELECT *, row_number() OVER (PARTITION BY stratum
                                 ORDER BY pick, hash(url)) AS slot
    FROM candidates
  )
  WHERE slot <= $TOTAL // 7;

SELECT c.stratum,
       count(*) AS urls,
       count(DISTINCT c.domain) AS domains_with_urls,
       (SELECT count(*) FROM sampled_domains s WHERE s.stratum = c.stratum)
         AS domains_drawn
FROM chosen c GROUP BY c.stratum ORDER BY c.stratum;

SELECT count(*) AS total_urls FROM chosen;

COPY (SELECT url FROM chosen ORDER BY hash(url)) TO '$OUT/seed.txt' (FORMAT csv, HEADER false);
COPY (SELECT url, domain, rank, stratum FROM chosen) TO '$OUT/strata.csv' (FORMAT csv, HEADER true);
SQL

echo
echo "seed:   $(wc -l < "$OUT/seed.txt") urls in $OUT/seed.txt"
echo "strata: $OUT/strata.csv"
echo
echo "now crawl it, on a box with chrome and the browser features built in:"
echo "  cargo build --release -p umi-cli --features emulation,render"
echo "  ./target/release/umi crawl scripts/gate-2-1.toml \\"
echo "      --seed $OUT/seed.txt --out $OUT/crawl \\"
echo "      --tabs 4 --concurrency 128"
echo
echo "then report it:"
echo "  ./scripts/gate-2-1.py --crawl $OUT/crawl --strata $OUT/strata.csv"
