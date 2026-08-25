# 01 Goals and constraints

This document exists to stop the rest of the spec from lying. Every other document assumes the numbers here.

## 1.1 What umi is for

umi acquires the open web and publishes it as an open dataset. It is not a search engine, it is not a scraper for one site, and it is not a general purpose data pipeline. It has one job with three properties that Common Crawl does not have together: continuous rather than batched, revisited rather than snapshotted, and rendered when rendering is the only way to see the content.

The output is the input to Spec 2050. If umi works, someone can build an index on top of it without ever running a crawler, which is the whole point.

## 1.2 The measured hardware

These are the actual boxes, probed on 2026-08-24, not a capacity plan someone wrote down once.

| Host | vCPU | RAM total | RAM available | Disk total | Disk free | Storage |
| --- | --- | --- | --- | --- | --- | --- |
| server1 | 4 | 6 GB | ~0 GB | 391 GB | 163 GB | SSD |
| server2 | 6 | 11 GB | ~4 GB | 193 GB | 67 GB | mixed, mostly rotational |
| server3 | 8 | 23 GB | ~10 GB | 387 GB | 112 GB | mixed, mostly rotational |
| **fleet** | **18** | **40 GB** | **~14 GB** | **971 GB** | **342 GB** | |

server1 has essentially no free memory right now and server2 and server3 are both running other things. The fleet has 14 GB of headroom and 342 GB of free disk. Treat those as the real budget, not the totals.

All three are VPS instances on commodity providers. server1 and server3 resolve to Contabo ranges. Two things about that class of host matter more than the CPU: the port speed, which is commonly 200 Mbit/s guaranteed on older plans and 1 Gbit/s on newer ones, and the transfer allowance, which is typically stated as 32 TB per month and typically applies to outbound traffic only.

That second point deserves its own line, because the whole capacity plan turns on it. A crawler is an inbound heavy workload. Its outbound traffic is a few hundred bytes of request per page plus whatever it publishes. If the provider meters inbound at the same 32 TB, the sustained rate per host is 81 pages/s and this spec needs different numbers throughout. **Measuring this is task one of milestone 1**, and it is a `curl` against a large file plus a month of `vnstat`, not a support ticket.

## 1.3 The arithmetic that follows

The target is 250 pages per second **per host**, so 750 pages per second across server1, server2 and server3.

**Bandwidth was verified first, and both of the numbers in this section turned out to be wrong.** Gate 1.1 in doc 16 ran on 2026-08-24 and what follows is the measured version. The original estimate is kept at the end of the section, because the size of the error is itself worth remembering.

The page size assumption was the larger mistake. The original derivation took Common Crawl's 364 TiB over 2.14 billion pages, which is about 170 KB per page, and rounded to 150 KB. That figure averages over all content types and over uncompressed bytes, and a crawler pays neither. What it pays is the compressed response, and that is measurable rather than assumable. Across 20705569 rows with `fetch_status = 200` from `open-index/ccrawl-urls` on CC-MAIN-2026-25:

```
all types      mean 53.1 KB   p50 21.8 KB   p90 101.5 KB   p99 475.6 KB

text/html               6128452 rows      45.2 KB mean
application/xhtml+xml    715516 rows      28.4 KB mean
application/pdf           31528 rows    1187.0 KB mean
text/plain                 4187 rows      14.9 KB mean
```

PDFs are half a percent of documents at 26 times the mean size. Doc 11.3 already defers the PDF handler to milestone 5 on extraction cost grounds, and the bandwidth argument points the same way.

At 53.1 KB the requirement is 2.8 times smaller than this section originally claimed:

```
per host:   250 pages/s x 53.1 KB = 13.3 MB/s  = 106 Mbit/s sustained inbound
                                  = 1.15 TB/day = 34.4 TB/month inbound

fleet:      750 pages/s           = 39.8 MB/s  = 319 Mbit/s sustained inbound
                                  = 103 TB/month inbound

outbound:   750 pages/s x 6 KB    = 4.5 MB/s   = 36 Mbit/s
                                  = 11.8 TB/month across the whole fleet
```

The measured capacity, from `umi doctor --bandwidth` on each box: eight concurrent streams for 60 seconds in each direction, against three Hetzner endpoints and Cachefly inbound and Cloudflare's speed test outbound.

| Host | inbound | outbound | pages/s at 53.1 KB |
| --- | --- | --- | --- |
| server1 | 178 Mbit/s | 182 Mbit/s | 420 |
| server2 | 247 Mbit/s | 219 Mbit/s | 582 |
| server3 | 466 Mbit/s | 519 Mbit/s | 1098 |
| **fleet** | **892 Mbit/s** | **920 Mbit/s** | **2100** |

Those are one run each and the run to run spread is wide: server1 came back anywhere from 143 to 187 Mbit/s inbound across four runs on the same afternoon, and the endpoint mix moved too, with Cachefly delivering 54 MB in one run and 429 MB in the next. Treat the table as the order of magnitude rather than as four significant figures. None of the three reports a NIC link speed, all read `-1`, so the port ceiling is unknown and these are floors rather than caps in that direction too.

The first set of numbers this section carried was lower across the board, at 77, 216 and 270 Mbit/s inbound with server1's outbound unmeasured because the test got OOM killed. Those came from a shell script rather than from `umi doctor`, they counted bytes only at the interface, and they let a stalled stream run past the end of the window, which turns a rate into a smaller rate. The current numbers count what the process received, cancel at the deadline, and measure both directions on all three boxes.

**Bandwidth is not the constraint this section was written to worry about.** The failure case it feared was 81 pages/s per host. The fleet instead has roughly 2.8 times the headroom the 750 pages/s target needs, and every individual box clears 250 pages/s on its own. Two things change as a result.

server1 is still the weak box, at 420 pages/s inbound against server3's 1098, and more to the point it has essentially no free memory: `umi doctor` reports 373 MB available against the 1536 MB a crawl budgets, and calls the box not ready. Doc 15 gives it the coordinator role, which is the right assignment for a reason other than the one written down there: it should carry the least fetching, not the most.

Metering is now the binding open question rather than raw speed. At 250 pages/s a host moves 34.4 TB/month inbound, which is just over a typical 32 TB allowance, and a 32 TB inbound cap works out to 232 pages/s. That is marginally under target rather than catastrophically under it. `vnstat` is now running on server2 and server3, collecting the history that settles this, and server1 needs it installed by somebody with root. A month of data answers the question and nothing before then does.

The two fallbacks are unchanged and are now comfortable rather than essential. Conditional revalidation is the big one: a 304 costs about 500 bytes against 53 KB, so a revisit heavy mix is nearly free, and doc 09 already prefers it. Range limiting is the other: cap fetches at 512 KB and abort past it, which at p99 of 475.6 KB loses well under 1 percent of documents. Neither changes the architecture.

For the record, the original estimate in this section read 300 Mbit/s per host and 900 Mbit/s fleet wide, against 97 TB and 292 TB per month. It was wrong by 2.8x in the safe direction, which is the direction an unverified assumption should be wrong in, but it was wrong enough that it would have driven the design toward a community fleet dependency from day one that the hardware does not actually require.

**100 billion pages is 4.2 years away at this rate.**

```
750 pages/s x 86400        = 64.8 million pages/day
750 pages/s x 86400 x 365  = 23.7 billion pages/year
100e9 / 23.7e9             = 4.2 years
100e9 in 18 months         requires 2114 pages/s
100e9 in 12 months         requires 3170 pages/s
```

There is no clever scheduling that closes a 4x gap. The only thing that closes it is more egress from more machines, which is why the fetcher is specified as a protocol in doc 04 rather than as an internal module. server1, server2 and server3 are the coordinator and the reference fetcher fleet, and they carry roughly a quarter of the load at the 18 month target. The rest belongs to fetchers we do not own.

To be concrete about what the community fleet has to look like: closing the gap to 2114 pages/s means another 1364 pages/s, which at 150 KB per page is 205 MB/s or 1.6 Gbit/s aggregate. Spread over volunteers that is roughly 450 fetchers at 3 pages/s each, or 140 fetchers at 10 pages/s, or 27 well provisioned partner nodes at 50 pages/s. The 3 pages/s figure is deliberately small because it is what a home connection can give without anyone noticing.

**Local disk holds less than one day of output.**

At 6 KB of stored, extracted, compressed output per page (doc 10 justifies this number), 250 pages/s produces 130 GB per host per day. server1 has 163 GB free, which is 1.25 days. server2 has 67 GB free, which is 12 hours. server3 has 112 GB free, which is 21 hours. And that assumes we never keep raw HTML.

This is the single hardest local constraint in the project. Publish and delete is not a background maintenance job, it is on the critical path, and it has to run continuously rather than nightly. Segments seal at 128 MB rather than at some comfortable multi gigabyte size specifically so that the publish loop can keep up. Doc 12 budgets a segment from seal to deleted in under 10 minutes, and doc 15 defines the backpressure that throttles the crawl when it cannot.

**RAM will not hold a global seen set, ever.**

100 billion URLs at a 10 byte fingerprint is 1 TB. There is no arrangement of 14 GB of headroom that makes that resident. The seen set has to be sharded by pay level domain, kept cold on object storage, and paged in only for the domains currently being worked. Doc 08 specifies that, and it is the single biggest structural difference between umi and every crawler design that assumes a big machine.

**Rendering is a rounding error, not a strategy.**

A headless Chromium tab costs 150 to 300 MB of RSS and 1 to 3 seconds per page. On 14 GB of headroom the fleet can hold maybe 40 tabs, and at 2 seconds per page that is 20 pages/s in theory and closer to 5 pages/s once the CPU is doing anything else. Against a fleet target of 750 pages/s, rendering caps out below 1 percent of volume. Most of it has to be delegated to community fetchers that have spare cores, and the tier policy in doc 05 has to treat a browser render as a scarce resource to be rationed rather than a fallback to be taken whenever the cheap tier fails.

**CPU is tighter than it looks on server1.**

Extraction runs 3 to 8 ms per page per core for a 150 KB document, so 250 pages/s needs 1.25 cores of extraction at 5 ms, plus roughly half a core for the fetch and TLS path, plus the state store. Call it 2 of 4 vCPU on server1, which already has other workloads and zero free memory. If server1 cannot give `umid` 1.5 GB of RSS and 2 cores, it should run as a fetcher only node and hand its PLD share to server3. Doc 03.4 and doc 15 both say this and it is not a hypothetical.

**Storage of the finished corpus is 600 to 800 TB.**

100 billion pages at 6 to 8 KB of Parquet per page. The link graph adds 40 to 60 TB. Hugging Face's published guidance is a 300 GB soft cap per public repo, 200 GB hard cap per file, 100k files per repo and 10k files per folder. At 300 GB per repo, the corpus is roughly 2200 repositories. That is a number to raise with Hugging Face before milestone 5, not after. Doc 12 sizes the shards so that each repo lands comfortably inside both the size and file count limits.

## 1.4 Targets

These are the acceptance targets. Doc 16 turns them into gated milestones.

**Throughput.** 250 pages/s per host sustained over 24 hours, so 750 pages/s across the fleet, with fetch and extraction together using no more than 2 vCPU per host and no more than 1.5 GB RSS on server1. If the bandwidth measurement in 1.2 comes back badly, this target drops to whatever the metered allowance supports and the community fleet target in doc 16 rises to compensate.

**Latency to published.** A page fetched at time T is queryable in a published Parquet file at T plus 60 minutes at p50 and T plus 4 hours at p99. Common Crawl's equivalent is measured in weeks. The Internet Archive's Wayback lag is 3 to 10 hours, and beating that is a reasonable bar.

**Freshness.** A fresh core of at least 100 million URLs revisited on a schedule derived from measured change rate, with p50 staleness under 24 hours for the news and feed classes and under 15 days for the general class. The size of the fresh core scales linearly with fetch capacity, so this target grows with the community fleet rather than being fixed.

**Incrementality.** Restarting any component loses no more than the in flight lease window, which is 60 seconds by default. No full recrawl is ever required to add a document, and no rebuild is ever required to publish.

**Compactness.** Under 8 KB per page of published Parquet including text, links, and metadata, and under 20 bytes per known URL of state. Both measured, not estimated, at milestone 3.

**Verifiability.** Every published row carries the content digest, the fetcher identity, and the receipt id that produced it. Any third party can re fetch a URL and check whether our copy is plausible. Doc 06 defines plausible precisely, since exact byte equality is not achievable on a live web.

## 1.5 Non goals

**We are not building a search index.** No inverted index, no ranking, no query serving. That is Spec 2050.

**We are not archiving bytes.** umi does not aim to be a Wayback Machine. We store extracted, canonical, structured output and the digests of the raw bytes we saw, not the raw bytes themselves beyond a short audit window. Raw preservation at 100 billion pages is a petabyte scale storage commitment and the Internet Archive already does it better than we would.

**We are not defeating access controls.** Doc 07 draws the line: robots.txt is authoritative and is obeyed at every tier. The tier ladder in doc 05 exists so that a well behaved crawler is not misclassified by generic heuristics, not so that an explicit deny can be worked around. Tier 4 requires an explicit per domain allowlist and an operator who has taken responsibility for it.

**We are not building a general ETL framework.** The extraction pipeline in doc 11 produces one schema. If you want a different schema, read the Parquet and transform it yourself.

**We are not supporting arbitrary state backends.** Four are specified in doc 08 and the trait is narrow on purpose. It is not a plugin system.

**We are not doing P2P storage or a token.** The fetcher fleet is federated and the data lives on Hugging Face. Every attempt at fully decentralized web search has failed on relevance or latency, and there is no reason to think our version would be the exception.

## 1.6 Design principles

**Cache, do not accumulate.** Local disk is a staging buffer with a TTL. Anything that cannot be reconstructed from the published corpus plus the state shards is a bug.

**Separate the immutable from the mutable.** Crawled results are append only and content addressed, and they go in `.umi` files. Frontier and scheduling are mutable and they go in state. They never share a file, a lock, or a lifecycle. This is the reason both can be fast.

**The fetcher is a protocol, not a process.** Anything the internal fetcher can do, an external fetcher can do over the same wire contract, and anything an external fetcher can claim, the coordinator can check.

**Verify everything that came from outside.** Every byte from a community fetcher is untrusted until a receipt has been checked, and links from untrusted fetchers do not steer the frontier until they are corroborated.

**Publish, then delete, then forget.** The GC is not a maintenance task, it is part of the write path.

**Honest identity by default.** A stable user agent with a documented URL, forward confirmable IPs, RFC 9421 request signatures, and full robots compliance. Legitimacy is cheaper to maintain than to recover.

## 1.7 Why Rust

The crawl loop is I/O bound and the extraction loop is CPU bound, and they have to run in the same process on a 4 vCPU box with no spare memory. That combination rules out a GC language for the extractor and rules out per page process spawning for the fetcher.

The concrete reasons are memory ceilings that hold under load, a mature async I/O story in tokio and hyper, the best available TLS fingerprint control through the BoringSSL backed clients described in doc 05, zero copy columnar reads through the Arrow and Parquet crates, and a single static binary that a volunteer can run without installing a runtime. That last one matters more than it sounds, because doc 04 only works if running a fetcher is a one file download.

Target toolchain is Rust 1.98 with edition 2024. Pin it in `rust-toolchain.toml` and treat toolchain upgrades as their own change with their own benchmark run.
