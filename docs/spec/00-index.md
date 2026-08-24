# umi (海) Specification, Spec 2124

umi is an internet scale web crawler written in Rust. It fetches the open web continuously, stores what it finds in a compact columnar single file format, and publishes the result as Parquet into public Hugging Face repositories under `open-index/*`. The local disk is a cache and nothing more. Once a segment is published and verified, the local copy is deleted.

The repository is `tamnd/umi`. The name is 海, the sea. Its predecessor `tamnd/kumo` (雲 cloud, 蜘蛛 spider) stays what it already is, a pure Go binary that crawls one host well. umi is the thing kumo feeds into. Nothing in this spec deprecates kumo and nothing here should be built by rewriting it.

umi is the first component of the open index described in Spec 2050. That spec covers the whole crawl to answer pipeline including the inverted index, ranking, and the answer engine. This spec covers only acquisition and storage, in much more depth than 2050 doc 03 does, and it supersedes 2050 doc 03 wherever the two disagree.

## What we are actually claiming

Three numbers drive every decision in here.

**100 billion pages.** That is roughly the size of the index-worthy web and roughly 45 times the size of a single Common Crawl monthly snapshot, which came in at 2.14 billion pages for CC-MAIN-2026-30. It is also about a third of Common Crawl's entire cumulative corpus.

**250 pages per second on each of server1, server2 and server3.** That is 750 pages/s for the fleet, 64.8 million pages a day, and 23.7 billion pages a year. At that rate 100 billion pages takes 4.2 years, and hitting it in 18 months needs 2114 pages/s. That gap is the central engineering fact of the project and doc 01 deals with it directly rather than hiding it behind a diagram. It is also the entire reason doc 04 exists.

**Freshness measured in hours, not months.** Common Crawl publishes roughly monthly and does not revisit within a snapshot. umi maintains a continuously revisited fresh core with per URL change rate estimation, and the size of that core is a direct function of available fetch capacity.

## How to read this

Read 01 first. It states the targets, the measured hardware, the arithmetic that follows from both, and what we are explicitly not building. If you only read one document, read that one, because it is the one that constrains everything else.

02 is prior art. It is the honest survey of Common Crawl, the European Open Web Index, the Internet Archive, the Mercator and IRLbot lineage, and the recent storage format work, with what we take from each and what we deliberately do differently.

03 through 07 are acquisition. 03 is the architecture and process model. 04 is the fetch protocol, which is the wire contract that lets people outside the project run fetchers. 05 is the tier ladder that deals with anti bot systems. 06 is trust and verification, which is what makes an open fetcher fleet survivable. 07 is politeness and identity, which is what keeps us legitimate.

08 through 11 are storage. 08 is the state layer, the mutable side, with its backend abstraction. 09 is the frontier and the freshness model that rides on top of state. 10 is the `.umi` file format, the immutable side. 11 is extraction and deduplication, which decides what actually goes into a row.

12 through 17 are the outside world. 12 is publishing to Hugging Face and the delete after publish rule. 13 is focused crawling. 14 is the command line surface. 15 is operations on the real boxes. 16 is the roadmap with acceptance gates. 17 is the list of things we do not know yet.

## Document index

| Doc | Title | What it covers |
| --- | --- | --- |
| [01](01-goals-and-constraints.md) | Goals and constraints | Targets, measured hardware, capacity arithmetic, non goals, the honest gap between 250 pages/s and 100B pages |
| [02](02-prior-art.md) | Prior art | Common Crawl, OWI/OWLer, Internet Archive, Mercator/IRLbot/UbiCrawler, Brave, Vortex/FastLanes/BtrBlocks, what we borrow |
| [03](03-architecture.md) | Architecture | Components, process model, data flow, what runs where, crate layout |
| [04](04-fetch-protocol.md) | Fetch protocol | The `umi/1` wire contract, leases, receipts, digests, the community fetcher contract |
| [05](05-fetch-tiers.md) | Fetch tiers | T0 revalidate through T4 supervised, escalation policy, per host tier learning, cost model |
| [06](06-trust-and-verification.md) | Trust and verification | Poisoning threat model, replay sampling, quorum, canaries, reputation, the holding pen for links |
| [07](07-politeness-and-identity.md) | Politeness and identity | RFC 9309, AIPREF `Content-Usage`, Web Bot Auth signing, rate limits, the block versus disallow distinction |
| [08](08-state-layer.md) | State layer | The `State` trait, PLD sharding, cold state on object storage, SQLite/nami/Postgres/DuckDB backends |
| [09](09-frontier-and-freshness.md) | Frontier and freshness | Priority, politeness scheduling, change rate estimation, refresh classes, the realtime path |
| [10](10-umi-file-format.md) | The .umi file format | Single file columnar container, shoals, encodings, footer, crash safety, zero copy reads |
| [11](11-extraction-and-dedup.md) | Extraction and dedup | Markdown, text, links, snippets, metadata, exact and near duplicate detection, canonical output |
| [12](12-publishing.md) | Publishing | Parquet schemas, Hugging Face repo layout, manifests and signatures, delete after publish, the GC rule |
| [13](13-focused-crawl.md) | Focused crawl | Scope profiles, per domain crawls, portable state and data pairs, the tamnd/*-cli on ramp |
| [14](14-cli.md) | Command line | The `umi` binary, subcommands, config file, output formats |
| [15](15-operations.md) | Operations | Deployment on server1/2/3, capacity plan, monitoring, the DuckDB dashboard, runbook |
| [16](16-roadmap.md) | Roadmap | Six milestones with literal acceptance gates, from single host crawl to community fleet |
| [17](17-open-questions.md) | Open questions | Risks, unknowns, decisions deferred, things that could kill the project |

## Status

All eighteen documents are written. Specification only, no code has been written. Rust 1.98, edition 2024, is the target toolchain.

The spec is written to be implemented in the order of doc 16, and each milestone there has a gate that can be checked mechanically. If a milestone gate cannot be met on the real hardware, the correct response is to change this spec rather than to move the gate.
