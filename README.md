# umi (海)

An internet scale web crawler in Rust. It fetches the open web continuously, stores what it finds in a compact columnar single file format, and publishes the result as Parquet into public datasets under [`open-index`](https://huggingface.co/open-index) on Hugging Face. The local disk is a cache and nothing more: once a segment is published and verified, the local copy is deleted.

umi is the acquisition layer for an open index. Crawling is the part of a search engine that nobody can borrow, so it is the part that has to exist first.

> **Status: specification complete, implementation starting.** The eighteen documents in [`docs/spec`](docs/spec) are written and the workspace is laid out against them. Almost every crate is a stub. `umi --help` shows the whole command surface and every command exits 1 with a pointer at the milestone that builds it. See [the roadmap](docs/spec/16-roadmap.md).

## The three numbers

**100 billion pages.** Roughly the size of the index-worthy web, about 45 times a single Common Crawl monthly snapshot, and about a third of Common Crawl's entire cumulative corpus.

**250 pages per second on each of three servers.** That is 750 pages/s for the fleet, 64.8 million pages a day, 23.7 billion a year, and 4.2 years to 100 billion. Reaching it in 18 months would need 2114 pages/s. That gap is the central engineering fact of the project, it is stated plainly in [doc 01](docs/spec/01-goals-and-constraints.md), and closing it is the entire reason the fetcher is a protocol rather than a thread pool.

**Freshness in hours, not months.** Common Crawl publishes roughly monthly and does not revisit within a snapshot. umi keeps a continuously revisited fresh core with per URL change rate estimation, and the size of that core is a direct function of available fetch capacity.

## How it is put together

**A fetcher is a protocol, not a process.** `umi/1` is a wire contract: lease work from a coordinator, fetch it, return a signed receipt with digests over the response. Anyone can run a fetcher on a spare box and connect it. That is what makes the rate gap closable by people rather than by budget. Because the fleet is open it is also assumed hostile, so [doc 06](docs/spec/06-trust-and-verification.md) covers replay sampling, quorum on disagreement, canary URLs, reputation curves, and a holding pen for links that arrive from an unproven source.

**Anti-bot is a ladder, not a switch.** T0 is a conditional revalidate, T1 is a plain HTTP client, T2 is a browser-shaped TLS and HTTP/2 client, T3 is a real headless browser, T4 is supervised and rare. Escalation is per host, learned, and recorded, because a browser costs roughly two orders of magnitude more than a conditional GET and spending that on a page that would have returned 304 is how a crawler stops being able to afford the web.

**Two storage systems, on purpose.** The immutable side is `.umi`, a single file columnar container of markdown, links, snippets, metadata and digests, tuned for one thing: fill in 90 seconds, live less than 10 minutes, convert to Parquet, get deleted. The mutable side is the state layer, which holds the frontier, the seen set and the recrawl schedule under 20 bytes per URL, behind a `State` trait with backends for SQLite (the default), nami (a single high performance file), Postgres, and DuckDB for dashboards and reports.

**Politeness is not configurable downward.** RFC 9309 robots, AIPREF `Content-Usage`, Web Bot Auth request signing so an origin can verify who is calling, per host and per pay level domain rate caps, and a published identity at [umi.dev/bot](https://umi.dev/bot). There is no `--ignore-robots`, no `--user-agent`, no proxy configuration and no CAPTCHA solving. [Doc 14 section 10](docs/spec/14-cli.md) lists what is deliberately absent and why.

**Everything published is verifiable.** Manifests are hash chained and Ed25519 signed with three separate keys, and a stranger can verify a published corpus from a clean machine with the released binary. Local data is deleted only when four conditions hold at once, with no override flag.

## Try it

```sh
git clone https://github.com/tamnd/umi
cd umi
cargo build --workspace
./target/debug/umi --help
```

Rust 1.98 or newer, edition 2024. The toolchain is pinned in `rust-toolchain.toml`, so `cargo` picks up the right compiler on its own.

The command surface is real even though the commands are not:

```sh
umi crawl example.com --max-pages 10000 --publish   # focused crawl of one domain
umi fetch --coordinator https://umi.dev --rate 2    # contribute capacity to the fleet
umi get https://example.com/ --markdown --receipt   # one URL through the full ladder
umi doctor                                          # can this machine do the thing
umi sql "select host, count(*) from pages group by 1 order by 2 desc limit 20"
```

Any program that prints URLs to stdout, one per line, and exits zero is a seeder. That is the whole contract, which is why it is `--seeder`, a pipe, and not a plugin API.

## The specification

Written before the code, and meant to be read in order. Start with [01](docs/spec/01-goals-and-constraints.md) if you read only one, because it constrains everything else.

| Doc | Title |
| --- | --- |
| [01](docs/spec/01-goals-and-constraints.md) | Goals and constraints |
| [02](docs/spec/02-prior-art.md) | Prior art |
| [03](docs/spec/03-architecture.md) | Architecture |
| [04](docs/spec/04-fetch-protocol.md) | Fetch protocol |
| [05](docs/spec/05-fetch-tiers.md) | Fetch tiers |
| [06](docs/spec/06-trust-and-verification.md) | Trust and verification |
| [07](docs/spec/07-politeness-and-identity.md) | Politeness and identity |
| [08](docs/spec/08-state-layer.md) | State layer |
| [09](docs/spec/09-frontier-and-freshness.md) | Frontier and freshness |
| [10](docs/spec/10-umi-file-format.md) | The .umi file format |
| [11](docs/spec/11-extraction-and-dedup.md) | Extraction and dedup |
| [12](docs/spec/12-publishing.md) | Publishing |
| [13](docs/spec/13-focused-crawl.md) | Focused crawl |
| [14](docs/spec/14-cli.md) | Command line |
| [15](docs/spec/15-operations.md) | Operations |
| [16](docs/spec/16-roadmap.md) | Roadmap |
| [17](docs/spec/17-open-questions.md) | Open questions |

[Doc 17](docs/spec/17-open-questions.md) is the one to read if you want to know where this is weakest. It lists what has not been measured, which decisions were made without evidence, and what could kill the project.

## Roadmap

Six milestones, each with gates that can be checked mechanically rather than argued about. Every milestone is an issue tracker milestone too.

| | Milestone | The gate that matters most |
| --- | --- | --- |
| M1 | One host, end to end | A published dataset, verified from a clean machine that never saw the crawl |
| M2 | The ladder and the clock | Freshness beats Common Crawl on one real domain over 14 days |
| M3 | Rate on one box | 250 pages/s sustained on server3 for 24 hours |
| M4 | Three coordinators and the protocol | A third party writes a working fetcher from [doc 04](docs/spec/04-fetch-protocol.md) alone |
| M5 | Open | 10 billion pages published, corpus verified by a stranger |
| M6 | The long run | Quarterly gates on coverage, staleness, growth and disagreement |

## Contributing

The useful thing right now is review of the specification, not patches to stubs. If something in `docs/spec` is wrong, expensive, or already solved better somewhere else, open an issue and say so. Issues labelled `kind/gate` are the acceptance criteria and issues labelled `measurement` are numbers the spec asserts without evidence, which are the two places where being contradicted is worth the most.

See [CONTRIBUTING.md](CONTRIBUTING.md). Prose in `docs/` follows a house style that CI enforces: plain english, no em dashes, no horizontal rules, no hard wrapping in the middle of a sentence.

## Licence

Apache-2.0 for the code. Everything umi itself creates, meaning manifests, indexes, digests and annotations, is CC0. Crawled content is published on the same basis as Common Crawl, and [doc 12 section 9](docs/spec/12-publishing.md) covers takedowns and the append-only exclusion list.
