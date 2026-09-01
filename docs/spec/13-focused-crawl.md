# 13 Focused crawl

## 13.1 Why this is not a side feature

Everything else in this spec describes a system that is only interesting once it is large. A frontier of 100 billion URLs, a fleet of community fetchers, 2000 Hugging Face repositories. None of that exists on day one, and a project whose first useful output arrives in year two is a project that dies in month four.

Focused crawl is the part that is useful immediately. `umi crawl docs.rust-lang.org` on a laptop, ten minutes later a directory of Parquet with the whole documentation set as markdown, links and metadata. That is a tool someone would use even if the 100 billion page version never happened, and it is the same code path: the same fetcher, the same tier ladder, the same extractor, the same state trait, the same file format. Nothing about focused mode is a simplified reimplementation, and that constraint is what keeps it honest.

It is also, in order of how much it matters to the project:

**The test harness.** Every component in docs 04 through 12 can be exercised end to end against one site in under a minute. If the only way to test the crawler is to run the crawler at scale, nothing gets tested.

**The on ramp.** Doc 16 makes milestone 1 a single host crawl of one domain, and the reason that is achievable as a first milestone is that focused crawl is not extra work on top of the general crawler, it is the general crawler with a scope filter and a small default backend.

**The seeding path.** Doc 09 gives discovery 35 percent of the budget and observes that this alone reaches 100 billion pages in 12 years. Seeding from external sources is how the frontier gets populated faster than link discovery can populate it, and seeding is a focused crawl by another name.

**The demo.** People understand `umi crawl example.com` immediately. Nobody understands a coordinator.

## 13.2 The scope model

A scope is a declarative document evaluated during admission, before the seen set check, in doc 03.6's order. It is pure, it does no I/O, and it is versioned alongside the canonicalisation version in doc 11.2, because a scope is part of what defines a crawl's identity.

```rust
pub struct Scope {
    pub id:            u32,              // the crawl_profile column in doc 10.5
    pub name:          String,
    pub include:       Vec<Matcher>,
    pub exclude:       Vec<Matcher>,
    pub max_depth:     Option<u8>,       // hops from a seed, not path segments
    pub link_policy:   LinkPolicy,
    pub content:       ContentFilter,
    pub budget:        Budget,
    pub rate:          RateOverride,
    pub corpus:        Corpus,         // which published repository, see 13.8
}

pub enum Matcher {
    Pld(String),                 // example.com and everything under it
    Host(String),                // exactly docs.example.com
    HostSuffix(String),          // *.example.com
    PathPrefix { host: String, prefix: String },
    UrlRegex(String),            // anchored, compiled once, RE2 semantics
}

pub enum LinkPolicy {
    InScopeOnly,                 // default: discard out of scope links
    RecordOutOfScope,            // store them in the links column, never enqueue
    OneHop,                      // fetch out of scope targets, do not follow their links
}

pub struct ContentFilter {
    pub content_types: Vec<String>,      // empty means any
    pub languages:     Vec<String>,      // empty means any, applied after fetch
    pub max_bytes:     u32,
}

pub struct Budget {
    pub max_pages:     Option<u64>,
    pub max_bytes:     Option<u64>,
    pub max_duration:  Option<Duration>,
    pub stop_when_idle: bool,            // default true in focused mode
}

pub struct RateOverride {
    pub max_rps_per_host: f32,           // clamped by doc 07.6, never raised past it
    pub concurrency:      u16,
}
```

`include` and `exclude` are evaluated as: a URL is in scope if it matches at least one `include` and no `exclude`. An empty `include` list means everything, which is the general crawl, so the general crawl is scope id 0 with an empty include list and the entire mechanism is one code path.

`UrlRegex` uses RE2 semantics through the `regex` crate specifically because there is no backtracking and therefore no way for a scope to be a denial of service against admission. Admission runs at 12500 candidates per second and a catastrophically backtracking regex there would stop the crawl.

`max_depth` counts hops from a seed rather than path segments. Path depth is a bad proxy for anything, since a flat site puts everything at depth 1 and a deep site puts an article at depth 6, and link distance from a seed is what people actually mean when they say depth.

`OneHop` exists because it is what people want when they crawl a documentation site and would also like the pages it cites. It fetches out of scope targets but does not extract their links into the frontier, so the crawl still terminates.

## 13.3 How scope interacts with everything else

**Politeness is unaffected.** Every rule in doc 07 applies identically. `RateOverride.max_rps_per_host` can only lower the rate, never raise it, and the clamp is enforced in the scheduler rather than trusted from the profile. A focused crawl hits one site much harder than a general crawl does, because a general crawl at 750 pages per second spread over millions of hosts is invisible to any one of them while a focused crawl is the only thing that site sees from us. So focused mode defaults to a lower per host rate than general mode: one request at a time and a 1000 ms delay, no adaptive speed up below 500 ms, and a hard ceiling of 2 requests per second regardless of what the site tolerates.

**Priority is dominated by scope.** Doc 09.2's `scope_bonus` term is zero in a general crawl and is large enough in a focused crawl to override every other term. Inside a focused crawl the remaining priority terms still order the work, so a scoped crawl still fetches the shallow, well linked, likely to change pages first, which matters when a budget cuts the crawl off before it finishes.

**Tiers are more permissive.** Doc 05 caps rendered fetches at under 1 percent of volume because at 750 pages per second the browser pool is the bottleneck. A focused crawl of 5000 pages can afford to render all of them, so focused mode allows T3 by default up to its own page budget. T4 still requires the explicit per domain allowlist from doc 05, and focused mode is not a way around it.

**Freshness mostly does not apply.** Focused crawls default to `stop_when_idle`, so they run until the frontier is empty and exit. A focused crawl can be run in continuous mode with `--watch`, in which case doc 09's change rate estimation applies normally within the scope, and that is the right way to keep a mirror of a documentation site current.

**Verification is simpler.** A focused crawl on one machine fetches everything itself, so every row is `local` in doc 06's terms and none of the quorum machinery runs. Focused crawls do not lease work to community fetchers, because the whole point of a scope is that the operator chose it and the fleet did not.

## 13.4 The profile file

Scopes are TOML, because a scope is configuration a human writes and TOML is the format people can write without looking anything up. The CLI flags in doc 14 build one of these in memory, so there is exactly one representation.

```toml
name = "rust-docs"

include = [
  { host_suffix = "rust-lang.org" },
]

exclude = [
  { url_regex = '^https://doc\.rust-lang\.org/(nightly|beta)/' },
  { path_prefix = { host = "doc.rust-lang.org", prefix = "/src/" } },
]

max_depth   = 6
link_policy = "record_out_of_scope"

[content]
content_types = ["text/html"]
max_bytes     = 4_000_000

[budget]
max_pages    = 200_000
max_duration = "6h"

[rate]
max_rps_per_host = 1.0
concurrency      = 4

[seed]
sitemaps = true
robots_sitemaps = true
urls = ["https://doc.rust-lang.org/book/"]
```

The profile is hashed and its digest goes into doc 10's segment header and doc 12's manifest, so every published row can be traced back to the exact scope that admitted it. Profiles used for published crawls are themselves published in `umi-meta`.

## 13.5 Portable crawls

A focused crawl is a directory, and the directory is the unit you move around.

```
rust-docs/
  profile.toml            the scope, verbatim
  state.sqlite            doc 08's default backend
  data/
    01K2M8Q0P7R3XN5.parquet
    01K2M8QF2A1C9WZ.parquet
  manifest.json           same schema as doc 12.5, unsigned unless published
  crawl.log
```

That is the whole thing. `tar` it, send it, `umi resume ./rust-docs` on another machine and the crawl continues from where it stopped, because the state backend is a single SQLite file and the data is immutable Parquet.

This is why doc 08 makes SQLite the default rather than nami. Portability, inspectability with tools everyone already has, and no daemon. Doc 08 puts SQLite's ceiling at about 100 million URLs, and a focused crawl that exceeds 100 million URLs is not focused, so the default is correct for the case it defaults to.

`umi pack ./rust-docs` produces a single file that is the directory with the state vacuumed and the Parquet unchanged, and `umi unpack` reverses it. Packing does not convert anything, because the Parquet is already the portable format and re encoding it would be work in exchange for nothing.

In focused mode the delete after publish rule in doc 12.7 does not apply, because nothing was published and the operator's disk is the operator's problem. Segments are converted to Parquet and kept locally. `--publish` opts into the doc 12 pipeline, which then does delete local copies after verifying remote ones, and that difference is stated explicitly in the CLI help because it surprises people.

With `--publish` the directory is slightly different, because two of the entries above are answers to questions that publishing answers elsewhere:

```
rust-docs/
  profile.toml            the scope, verbatim
  state.sqlite            doc 08's default backend
  parquet/                staging, emptied as each file verifies on the hub
  published.jsonl         one line per segment: repo, path, digest, rows
  crawl.log
```

`data/` becomes `parquet/` because a file there is on its way somewhere rather than something you keep, and `manifest.json` is not written at all. The manifests that count are doc 12.5's signed day documents in the published repositories, and a second unsigned one sitting in the crawl directory would be a second answer to the same question with no signature on it. One crawl can also span several weekly repositories, which a single manifest cannot describe. `published.jsonl` is the operator's record of where their crawl ended up, appended as each segment verifies, and it is JSON lines rather than a document so that a crash leaves every line before the last one intact.

## 13.6 Seeding

A crawl needs a first URL and the general crawl needs several billion of them. Six sources, in rough order of how many URLs each one is worth.

**Common Crawl through ccrawl-cli.** `tamnd/ccrawl-cli` already speaks the CDX index, the columnar Parquet index, and the domain rank data. It can enumerate every URL Common Crawl has seen for a host, or every host above a rank threshold, and it can do it without us writing a single line of Common Crawl client code. This is the largest seed source available to anyone and it costs nothing. `umi seed cc --host example.com` and `umi seed cc --top-domains 10000000` shell out to `ccrawl` if it is on the path and fall back to the CDX HTTP API if it is not.

Seeding the general crawl from Common Crawl's cumulative URL set is the single fastest way to a large frontier, and doc 09's discovery budget then extends it. It also means our first published corpus is directly comparable against Common Crawl's on the same URL set, which is the honest way to make the "10 times bigger, much fresher" claim in doc 00 checkable rather than rhetorical.

**Sitemaps.** From `robots.txt` `Sitemap` lines, from `/sitemap.xml`, and from sitemap index files, recursively, with a cap of 50000 sitemap files and 50 million URLs per host. Doc 07.4 already extracts the lines. Sitemaps carry `lastmod`, which doc 09's change rate estimator uses as a prior for a URL it has never fetched, so a sitemap seeds both the frontier and the schedule.

**Feeds.** RSS and Atom from `link[rel=alternate]`, discovered by doc 11.6. Small in volume, enormous in freshness value, and doc 09's realtime path is built on them.

**A URL list.** A file, one URL per line, or stdin. The least clever option and the one most people will use.

**An existing umi corpus.** `umi seed corpus open-index/umi-pages-2026w34-03 --column links.href` reads published Parquet and admits every outlink. This is how a fresh coordinator bootstraps from published data without recrawling, and it is the reason the `links` column is published in full rather than truncated.

**The tamnd `-cli` fleet.** Covered next, because it is unusual enough to need its own section.

## 13.7 The `-cli` on ramp

There are 604 repositories named `tamnd/*-cli` on this machine. Each is a small command line tool that knows how to talk to one specific site or API: `arxiv-cli`, `aclanthology-cli`, `archwiki-cli`, `bbc-cli`, and 600 more. Each one already encodes the thing that is expensive to learn about a site, which is how to enumerate it: what the pagination looks like, where the listing endpoints are, which identifiers are dense, what the API returns.

That is a seed source nobody else has, and the way to use it is to not integrate with it at all.

The seeder contract is one line: **a seeder is any program that writes URLs to stdout, one per line, and exits zero.** No plugin API, no shared library, no protocol, no registration. If a program can print URLs it is a seeder.

```sh
umi crawl --profile arxiv.toml --seeder 'arxiv-cli list --category cs.IR --since 2026-01-01'
arxiv-cli list --category cs.IR | umi seed -
```

URLs from a seeder go through exactly the same admission as any other candidate: canonicalised by doc 11.2, scope filtered, robots checked, dedup checked against the seen set. A seeder cannot bypass anything, cannot set priority, and cannot mark a URL as already fetched. It is a source of candidates and nothing more, which is why the contract can be this small.

Making it a pipe rather than an interface means the 604 existing tools need zero changes, tools written in any language work, and a seeder that crashes cannot take the crawler with it. The cost is that we get no metadata from the seeder beyond the URL, which is fine, because everything else we want we will learn by fetching the page.

## 13.8 Publishing focused crawls

Default is local only. A focused crawl of someone's site is a thing the operator did for themselves and publishing it should be a deliberate act.

`--publish` sends it through doc 12 into `open-index/umi-focus-<name>`, a separate repository family from the general corpus, with the profile, the seed sources and the operator recorded in the dataset card. Focused crawls do not go into `umi-pages-*`, because the general corpus is supposed to be an unbiased sample of the web and a focused crawl is by definition not that. Mixing them would quietly poison every statistic anyone computes over the corpus.

Everything in doc 12 otherwise applies unchanged: manifests, signatures, the exclusion list, the licence split, the takedown path.

The general corpus has to have a producer, though, and `umi crawl` is the only thing that writes pages. So a profile can say `corpus = "general"` and its pages go to `umi-pages-<iso week>-<slice>` instead. The default is `"focus"` and every profile written before this key existed keeps the old behaviour, which is the direction where being wrong is cheap. A crawl that lands in its own repository by mistake costs somebody a repository name. A crawl that lands in the general corpus by mistake costs everybody the corpus.

The key is a declaration by the operator and not a fact the code works out, because what makes a crawl an unbiased sample is the seed as much as the matchers, and nothing in the code can tell whether a list of ten thousand hostnames is representative of the web. What the code does check is the half it can see: a profile that carries any `include` or `exclude` matcher is a focused crawl whatever it calls itself, and asking for the general corpus alongside one is an error rather than a downgrade. An operator who wrote both lines meant one of them, and a crawl that publishes somewhere other than where its profile says is worse than a crawl that refuses to start.

## 13.9 Limits

A focused crawl inherits SQLite's ceiling of roughly 100 million URLs, and the CLI warns at 10 million and again at 50 million with the suggestion to switch backends. Above that the answer is a coordinator and one of doc 08's other backends, and the profile carries over unchanged.

Scope is evaluated on the canonical URL, so a site that serves the same content under two hostnames where only one is in scope will be crawled once per hostname. Doc 11.7's exact duplicate detection catches it after the fact and records it, which is the correct outcome but does not save the fetches.

`OneHop` can be much more expensive than people expect. A documentation site with 5000 pages and 40 outbound links each is 200000 one hop fetches, spread across thousands of hosts, most of which are slow. The CLI estimates this before starting and asks for confirmation when the estimate exceeds ten times the in scope page count.

A scope cannot express "pages about X". Topic focused crawling in the classic sense, with a classifier steering the frontier, is not in this spec. It is a natural extension, doc 09's priority function has the term for it, and doc 17 records it as deliberately deferred rather than rejected.
