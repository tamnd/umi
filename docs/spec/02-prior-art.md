# 02 Prior art

Everything in umi has been tried by someone. This document is what each of them got right, where each of them stops, and what we take.

## 2.1 Common Crawl

Common Crawl has crawled since 2008 and publishes a monthly snapshot as WARC, WAT and WET with a CDX URL index and a Parquet columnar index on top. It is the reason a project like this is thinkable at all, because it removes the cold start problem.

The current numbers matter for calibration. CC-MAIN-2026-30, the July 2026 crawl, contains 2.14 billion pages and 364 TiB of uncompressed content across 40.5 million hosts, crawled over 18 days. April 2026 was 2.19 billion and May 2026 was 2.16 billion. The trend is down, not up: the last eighteen crawls have all come in under 3 billion, against 3.03 billion in CC-MAIN-2025-05, a decline of about 29 percent. The cumulative corpus is over 300 billion pages, though with heavy overlap between snapshots.

What it gets right: openness with no key and no gate, a stable format the whole ecosystem reads, a URL index that makes the corpus addressable, and two decades of institutional legitimacy with site owners.

Where it stops, and where umi is different:

**No JavaScript.** CCBot stores the raw HTTP response and does not execute JavaScript or use cookies. Client rendered pages land as an empty shell. A growing fraction of the modern web is invisible to it. umi's tier 3 exists precisely for this.

**Monthly batches, no revisit within a batch.** A snapshot is a snapshot. There is no notion of a page changing during a crawl, and no per URL change rate model. umi is continuous.

**Declining coverage.** 2.14 billion pages per snapshot against an index worthy web on the order of 100 billion is roughly 2 percent. The 10x claim in the project brief is against a monthly snapshot, not against the cumulative corpus.

**WARC is a preservation format, not an analysis format.** Getting text out of Common Crawl means either using WET, which is a lowest common denominator text extraction, or reprocessing WARCs yourself at petabyte scale. umi publishes the extracted, structured output as the primary artifact, and treats the raw bytes as something to digest rather than to keep.

What we take: the URL index model, Parquet as the publication format, the CDX style addressability, and the seed. `tamnd/ccrawl-cli` already speaks the CDX API, the S3 layout, the WARC/WAT/WET streams, the columnar Parquet index and the domain rank tables, so it is the seeding tool for milestone 1. Doc 13 covers that path.

## 2.2 OpenWebSearch.eu and the Open Web Index

The EU funded OpenWebSearch.EU project ran from September 2022 with 14 institutions and 8.5 million euro under grant 101070014, and released its pilot infrastructure in June 2025. The Open Web Index is now available under a general research licence with commercial terms handled case by case, and data shards and daily statistics are published at openwebindex.eu. In 2026 it added a LUMI AI Factory collaboration for dataset as a service and spawned a follow on project called SOURCE.

Their crawler is OWLer, built on StormCrawler and therefore on Apache Storm, with preprocessing through resiliparse on Spark. CERN built the URL frontier tracking and reports roughly 9 million URLs per hour and about 3 TB of public web data per day, targeting 30 to 50 percent of the text based web.

9 million URLs per hour is 2500 pages per second. That is a useful calibration in two directions. It is roughly triple what the three umi boxes can do at 750 pages/s, which says our fleet target is in the right order of magnitude rather than fantasy. And it is what a well funded consortium with EuroHPC access and a five site iRODS federation achieves, which says that getting past it needs machines we do not buy.

What it gets right: the strategic framing that an index is shared infrastructure decoupled from the search application, publishing the crawl as reusable shards, and taking robots and legitimacy seriously from day one.

Where umi differs: OWI is an institutional project with HPC allocation and a research licence. umi is three cheap boxes and a public dataset with no licence gate. OWI's stack is JVM and Spark, which is the correct choice when you have a cluster and the wrong choice when you have 6 GB of RAM. And OWI, like Common Crawl, is snapshot oriented rather than continuously refreshed.

What we take: the shard as the unit of publication, the daily statistics discipline, and the argument for why any of this is worth doing.

## 2.3 Internet Archive

The Wayback Machine crossed one trillion page captures in October 2025 and holds 99 PB of unique data, over 212 PB with redundancy. The often quoted ingest figure is around 498 million pages per day, roughly 5760 pages per second, though the Archive does not publish an official real time rate. Hundreds of crawls run concurrently. The lag between capture and appearance in the Wayback Machine is 3 to 10 hours as of 2026. The CDX API is rate limited to about 60 requests per minute.

What it gets right: continuous crawling with many concurrent crawl profiles, revisits at wildly different frequencies per site, and a public API over the whole thing. The 3 to 10 hour capture to queryable lag is the bar umi's 60 minute p50 target is measured against.

Where umi differs: the Archive preserves bytes for the long now. umi extracts and discards. Their storage commitment is 99 PB and ours is under 1 TB of live disk. Different jobs, and we should not pretend to be doing theirs.

What we take: the multi profile concurrent crawl model, and the honest admission that "captures per day" is a much softer number than it looks. Their public capture counts report hours with at least one snapshot rather than actual snapshot counts, which is a good reminder to define our own metrics precisely before publishing them.

## 2.4 The Mercator, IRLbot and UbiCrawler lineage

The academic crawler literature settled the core data structures twenty years ago and nothing since has replaced them.

**Mercator** gave us the two layer frontier: front queues carry priority, each back queue holds exactly one host, and a heap keyed by next fetch time enforces the per host delay. Doc 09 implements this almost unchanged, because there is no better answer.

**IRLbot** gave us the single server throughput bar, about 1789 pages per second at 319 Mbit/s, and DRUM, the disk repository with update management that keeps the seen set bounded when it exceeds RAM. The DRUM idea, which is to batch URL membership checks and merge them against a sorted on disk structure instead of doing them one at a time, is the direct ancestor of the nami design in doc 08. IRLbot also established that the seen set, not the fetcher, is where naive crawlers die.

**UbiCrawler** gave us consistent hashing by host for partition assignment with no central coordinator. We hash by pay level domain rather than host so that all of `blog.example.com` and `www.example.com` share politeness state, and so that consistent hashing limits reassignment when a node joins or leaves.

The Google frontier patent literature is worth reading for one idea in particular, which is that recrawl is not a separate system. The crawler selects a URL, downloads it, and then either removes it from the frontier or reschedules it. Continuous crawling is a scheduling decision, not an architecture.

What we take: all of it. Doc 09 is Mercator with a change rate model bolted on, and doc 08 is DRUM with the sorted structure living on object storage instead of local disk.

## 2.5 Freshness and change rate

The classic result is that page change is well modelled as a Poisson process with a per page rate, and that the optimal revisit schedule under a bandwidth budget is not uniform and is not proportional to change rate either, because a page that changes constantly is never worth keeping fresh. The practical baseline that research uses is Last-Obs, which simply prioritises by time since last visit, and any real system should be able to beat it measurably or it has not earned its complexity.

The cheap wins are the ones every crawler should take first: conditional GET with `If-Modified-Since` and `If-None-Match`, so an unchanged page costs a 304 and a few hundred bytes instead of 150 KB. Sitemap `lastmod`. RSS and Atom feeds. These signals are free, publisher supplied, and umi's realtime path in doc 09 is built on them rather than on prediction.

Recent work frames revisit scheduling as a reinforcement learning problem over change rate, source importance, and available bandwidth. That is the right long term direction and the wrong milestone 1 decision. We ship the estimator first and leave a seam for a learned policy.

## 2.6 Anti bot systems as they actually work in 2026

This shapes doc 05 more than anything else in this document.

JA3 is dead as a defence and dead as an evasion. Chrome 110 randomised TLS extension order in January 2023 and broke it. JA4 replaced it with a structured fingerprint that strips GREASE and sorts cipher suites and extensions before hashing, so shuffling does not change it, and the readable prefix such as `t13d1516h2` carries TLS version, transport and ALPN without a lookup. By 2026 Cloudflare, DataDome, Akamai, Imperva, F5 and AWS WAF all run JA4 or the wider JA4+ suite in production.

The important architectural fact is that TLS fingerprinting is one of four or more layers, and vendors score them together into a trust score rather than gating on any single signal. The stack is TCP/IP characteristics in the p0f tradition, TLS through JA4, HTTP/2 frame and SETTINGS analysis, HTTP header order and casing through JA4H, and then client side JavaScript, canvas, WebGL and challenge widgets. Single layer evasion fails because the layers are correlated.

The consequence for a crawler that never runs JavaScript is precise and useful: TLS, HTTP/2 and headers are the entire detection budget for that request. Getting those three consistent with a real browser is the whole game at tier 2, and there is no partial credit.

The counter pressure is worth stating plainly. Cloudflare, which fronts roughly 20 percent of the web, moved to blocking AI crawlers by default in July 2025 and launched a pay per crawl marketplace using HTTP 402, later reworked toward a pay per use model that bills on content actually surfacing in an answer rather than per fetch. Their stated 2026 default blocks mixed use crawlers on monetised pages while leaving pure search crawling allowed. That distinction is the single most important policy fact for umi, because it means declaring purpose honestly and separating crawler functions is worth more than any fingerprint work. Doc 07 acts on this.

The other side of the same coin is Web Bot Auth. `draft-meunier-webbotauth-httpsig-protocol-02`, authored by Cloudflare and Google and dated 18 August 2026, applies RFC 9421 HTTP Message Signatures to crawler traffic: sign requests with Ed25519, publish public keys at a well known directory, and let sites verify the signature instead of trusting the user agent. Cloudflare activated it at the edge in March 2026 and their verified bots program accepts it. Common Crawl is among the supported agents. This is the cheapest legitimacy umi can buy and doc 07 makes it mandatory.

The parallel standards track is IETF AIPREF, with `draft-ietf-aipref-vocab` and `draft-ietf-aipref-attach` defining a `Content-Usage` line for robots.txt and an HTTP response header carrying the same preferences, with reconciliation rules that differ from Allow/Disallow in that conflicting preferences apply separately rather than resolving to the more permissive. A final RFC is not expected before August 2026 and the group has had consensus trouble. umi should parse and honour `Content-Usage` now and record it in the published data regardless of standards status, because the cost is a parser and the benefit is being on the right side of it.

## 2.7 Storage formats

Parquet was designed in 2013 for batch scans over modest width tables and the assumptions have broken. The last three years produced a genuine research wave: BtrBlocks on cascaded lightweight encodings, FastLanes from CWI on a unified 1024 value virtual vector layout transposed so the same encoded bytes decode across any SIMD width and onto GPUs, ALP for floats, FSST for strings with O(1) random access, then Lance and Vortex from industry and Nimble from Meta.

Vortex is the one to study. It claims 100 to 200x faster random access than Parquet, 2 to 10x faster scans, similar compression, and zero copy zero parse metadata, and it is now an incubation stage project at LFAI and Data. Its architecture separates logical and physical concerns, encodes each column independently based on statistics, allows encodings to nest, runs compute kernels directly on encoded data, and lands decompressed output straight into Arrow arrays. It uses FastLanes for bit packed integers, FSST for strings and ALP for floats, with BtrBlocks as the default compression strategy.

The `.umi` format in doc 10 borrows the cascade model, the FastLanes layout and the zero parse footer idea. It does not borrow FSST. Doc 10.6 built it and measured it against zstd and it lost by a wide margin, for a reason that is about this workload rather than about the encoding: a symbol table sees eight bytes at a time and our chunks are a megabyte of one column read front to back, which is the case a general purpose compressor is built for and the opposite of the random access case FSST is built for. It does not try to be Vortex. It is narrower on purpose: one schema, one writer, append only, crash safe, and convertible to Parquet without a re encode of the logical values. The SQLite influence is the crash safety discipline, which is a generation counter in the header, a checksum on every block, and a footer written last so a torn tail truncates cleanly.

For state, the Rust embedded store landscape as of 2026 is fjall for LSM, redb for copy on write B+trees, SlateDB for object storage backed LSM, and SQLite through rusqlite for everything conservative. Fjall 3.0 shipped in January 2026 with a new disk format aimed at longevity and forward compatibility. SlateDB is the interesting one for umi because it depends on object storage alone for durability, batches memtable flushes to amortise PUT cost, and reports aggregate write throughput comparable to RocksDB with p99 read latency well behind. Doc 08 takes SlateDB's cold tier idea and rejects its hot path, because 50 to 100 ms per object round trip is not compatible with 12500 URL membership checks per second.

## 2.8 Deduplication

The dominant approach is unchanged: MinHash with LSH banding, or SimHash. In head to head evaluation SimHash comes out behind MinHash and behind newer estimators like DotHash, though the comparisons are sensitive to parameter choice. The production pattern is cascaded, and Olmo 3 is the clearest published reference: global exact deduplication on content hashes, then 32 way sharded MinHash with exact Jaccard verification, then sharded fuzzy suffix array dedup to strip repeated boilerplate. Running exact before fuzzy is technically redundant and is much faster overall, so everyone does it.

Scale evidence exists at the right order of magnitude, including a 10 billion document MinHash LSH deduplication run. RefinedWeb retained roughly 65 percent of documents after aggressive MinHash dedup and filtering, which is a useful expectation to set for our own retention rate.

There is a live debate about local versus global dedup, with recent work arguing that per dump and per language dedup preserves diversity better than global. umi's position in doc 11 is that exact dedup is global and cheap, near duplicate clustering is computed and published as an annotation rather than applied as a filter, and the consumer decides. We are producing a corpus, not a training set, and we should not make that call for people.

## 2.9 What poisoning looks like

Carlini and co authors showed that poisoning web scale training sets is practical, and their analysis of why it is hard to defend applies directly to us. There is no golden snapshot to diff against, no trusted curator, no realistic bound on how much a page can legitimately change between versions, and no principled notion of which domains to trust. Their suggested mitigation is consensus, which is to trust content only when it appears across many independent sources, forcing an attacker to poison a much larger surface.

The volunteer computing literature has the other half of the answer, which is redundant task validation in the BOINC tradition, plus reputation weighting and canary tasks whose answers the coordinator already knows. There is no published protocol specifically for verified web fetching by untrusted parties, so doc 06 assembles one from these pieces rather than citing one.

The relevant negative result to keep in mind is that authenticating data from unidentified sources in a distributed environment is an open problem. We are not going to solve it. Doc 06 is a defence in depth design that raises the cost of poisoning and makes it detectable after the fact through provenance, not a proof of correctness.

## 2.10 What we are actually contributing

Nothing in sections 2.1 through 2.9 is ours. The parts that are new, or at least not obviously assembled anywhere else, are these four.

A crawl whose fetcher is an open protocol with a verification design, rather than an internal component of one organisation's cluster.

A crawl that treats local disk as a cache with a TTL and object storage as the only durable tier, including for its own frontier state, which is what makes 100 billion pages tractable on hardware that cannot hold 100 billion pages.

A columnar single file format tuned for one schema and one append only writer, which is a much easier problem than the general one Vortex and Parquet solve, and should therefore be much faster at it.

Continuous publication into a public dataset repository, with a capture to queryable lag measured in minutes, so the open corpus stops being a monthly artifact and becomes a stream.
