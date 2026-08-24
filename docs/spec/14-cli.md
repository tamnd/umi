# 14 Command line

## 14.1 Rules for the surface

The CLI is the only part of umi most people will ever touch, and the rest of the spec is judged through it. Five rules, and they are not negotiable during implementation because every one of them is cheap now and expensive later.

**The first command works.** `umi crawl example.com` with no configuration, no account, no token, no daemon, and no flags produces a directory of Parquet. Anything that requires setup before the first useful result is a design failure.

**Everything pipes.** Every command that produces a list produces it on stdout, one record per line, in a format another program can read. `--json` for objects, `--ndjson` for streams, `--csv` for spreadsheets, and a human readable table only when stdout is a terminal. This is how `tamnd/*-cli` tools behave and umi is not going to be the odd one out.

**Nothing dangerous is one flag away.** There is no `--ignore-robots`, no `--user-agent`, no `--force` on the GC rule, and no flag that raises a rate limit past doc 07's ceiling. Doc 14.11 lists what is deliberately absent and why. A flag that does not exist cannot be pasted into a Stack Overflow answer.

**Long running commands report progress that means something.** Pages per second, frontier size, tier distribution, disk headroom, and the current bottleneck as a word. Not a spinner.

**One binary.** `umi` covers focused crawling, community fetching, operator commands against a local or remote `umid`, seeding, inspection and publishing. `umid` is a separate binary because a daemon and a CLI have different lifetimes, but `umi` can start and stop one.

## 14.2 The command map

```
umi crawl <target>            run a focused crawl, doc 13
umi resume <dir>              continue a crawl directory
umi watch <dir>               continue and keep it fresh, doc 09

umi fetch                     run as a community fetcher, doc 04
umi doctor                    check this machine can do the thing it is about to do

umi seed cc                   seed from Common Crawl via ccrawl-cli, doc 13.6
umi seed sitemap <host>       seed from sitemaps
umi seed feed <url>           seed from RSS or Atom
umi seed corpus <repo>        seed from a published umi corpus
umi seed -                    seed from stdin, one URL per line

umi ls <dir|repo>             list segments or files with row counts and ranges
umi cat <segment>             stream rows as ndjson
umi get <url>                 fetch one URL through the full ladder and print it
umi sql <query>               DuckDB over local Parquet or a published checkpoint

umi state stats               size, shard residency, hit rates, doc 08
umi state warm <pld>          pull a cold shard, doc 8.6
umi state evict <pld>         push a shard out
umi checkpoint                write a portable state checkpoint, doc 8.5

umi publish <dir>             push through the doc 12 pipeline
umi verify <repo|dir>         re verify manifests, signatures and digests
umi manifest <repo>           print or validate a manifest chain

umi block <domain>            add to the block list, doc 07.7
umi scope check <profile>     evaluate a scope against a list of URLs

umid                          the coordinator daemon, doc 03.2
umi status                    talk to a local or remote umid
umi peers                     coordinator peering state
umi fetchers                  connected fetchers, reputation, rates, doc 06
```

Twenty five commands is more than a small tool and fewer than a platform. The test for adding one is whether it does something the others cannot compose into.

## 14.3 `umi crawl`

The flagship.

```
umi crawl <target> [options]

Target is a domain, a host, a URL, or a profile path.
  umi crawl example.com                  the whole registrable domain
  umi crawl docs.example.com             one host
  umi crawl https://example.com/blog/    that path prefix
  umi crawl ./rust-docs/profile.toml     a full scope, doc 13.4

Scope
  --include <matcher>          repeatable, same grammar as the profile
  --exclude <matcher>          repeatable
  --depth <n>                  hops from a seed, default unlimited
  --links <policy>             in-scope | record | one-hop   [default: in-scope]
  --content-type <mime>        repeatable, default text/html
  --lang <bcp47>               repeatable, applied after fetch

Budget
  --max-pages <n>
  --max-bytes <size>
  --for <duration>             wall clock limit
  --watch                      do not stop when idle, keep it fresh

Rate
  --rps <f>                    per host, clamped by doc 07.6, default 1.0
  --concurrency <n>            default 4

Fetching
  --tier <max>                 highest tier allowed, default 3 in focused mode
  --no-render                  equivalent to --tier 2
  --timeout <duration>         per request, default 30s

Seeding
  --seed <file|->              URL list
  --seeder <cmd>               repeatable, any program that prints URLs
  --sitemaps                   follow sitemaps, default on
  --from-cc                    seed from Common Crawl first

Output
  --out <dir>                  default ./<name>
  --state <backend>            sqlite | nami | postgres    [default: sqlite]
  --publish                    run doc 12 and delete local copies after verify
  --format <fmt>               progress output: auto | json | quiet
```

The default when everything is omitted: whole registrable domain, unlimited depth, in scope links only, `text/html`, 1 request per second per host, 4 concurrent, tiers up to rendered, stop when the frontier is empty, output to `./example.com`, SQLite state, nothing published.

Progress on a terminal:

```
example.com   4212 done   118 in flight   9871 queued   3.9 p/s   T1 92% T2 7% T3 1%
              184 MB fetched   26 MB stored   0 failed   robots 1 host   bottleneck: politeness
```

`bottleneck` is the single most useful field and it is one word: `politeness`, `cpu`, `disk`, `network`, `render-pool`, `origin-slow`, or `none`. It comes from whichever queue is saturated. A user who sees `politeness` understands immediately that raising concurrency will not help, which is the question everyone asks first.

## 14.4 `umi fetch`

The one command in doc 04's design constraint.

```
umi fetch [--coordinator <url>]

  --coordinator <url>     default https://umi.dev
  --rate <f>              pages/s you are willing to sustain, default 2.0
  --concurrency <n>       default 8
  --tier <max>            highest tier you will run, default 2
  --identity <path>       default ~/.umi/identity.key, generated on first run
  --no-render             do not offer T3 even if Chromium is present
  --refuse <domain>       repeatable, local blocklist, honoured by the coordinator
  --audit-cache <size>    raw body ring buffer, default 2GB
  --audit-window <dur>    default 24h
```

Everything has a default and the defaults are conservative. A volunteer who runs `umi fetch` with no arguments contributes 2 pages per second at tier 2 with a 2 GB audit cache, which is polite, cheap, and enough to be useful. The identity key is generated silently on first run and the fetcher id is printed once so the operator can find themselves in `umi fetchers`.

`--refuse` is doc 04's `operator_refused` and it exists so that saying "not from my IP, not to that site" is a supported thing rather than something people achieve by lying.

Output is a status line with granted rate against offered rate, reputation, deliveries accepted, audits served, and any reputation events. Reputation going down should be visible immediately and should say why, because a volunteer whose reputation silently collapses will just stop volunteering.

## 14.5 The daemon and operator commands

`umid` reads a config file and runs. It takes almost no flags, because a daemon configured by command line is a daemon that gets configured differently on each of three boxes.

```
umid --config /etc/umi/umid.toml [--check]
```

`--check` validates the config, resolves peers, opens the state store read only, verifies disk headroom and clock skew, and exits. That is what the systemd unit runs as `ExecStartPre`, and it is the difference between a bad config being caught in one second and being caught after the crawl has been down for an hour.

Operator commands talk to a `umid` over the same HTTP/2 the fetch protocol uses, on a separate admin listener bound to localhost by default:

```
umi status                       one screen: rates, queues, disk, publish lag, alarms
umi status --json                the same as an object, for the dashboard
umi peers                        each coordinator, PLD share, last contact, lag
umi fetchers                     id, impl, rate, reputation, accepted, quarantined
umi fetchers ban <id> --reason   doc 06, writes to the published ban list
umi pause [--pld <p>] [--host]   stop leasing, keep serving deliveries
umi resume
umi drain                        seal segments, publish, stop cleanly
```

`umi drain` is the one to get right. It stops leasing, waits for outstanding leases to complete or expire, seals every open segment, runs the publish pipeline to completion, verifies, deletes, checkpoints state, and exits. A clean shutdown loses nothing. An unclean one loses at most the shoal in flight, per doc 10.7, and doc 09.8 covers frontier recovery.

## 14.6 Inspection

```
umi ls ./rust-docs                       segments, rows, byte ranges, time ranges
umi ls open-index/umi-pages-2026w34-03   the same against a published repo
umi cat ./rust-docs/data/01K2M8.parquet --limit 10 --columns url,title,text_bytes
umi get https://example.com --tier 3 --markdown
umi sql "select status, count(*) from pages group by 1" --data ./rust-docs
```

`umi get` is the debugging workhorse. It runs one URL through the full ladder with verbose tier reporting and prints whatever you ask for: `--markdown`, `--text`, `--links`, `--meta`, `--headers`, `--receipt`, `--raw`. It is also the fastest way to answer "why did this page extract badly", and it prints the extractor version so the answer is reproducible.

`umi sql` is DuckDB, either embedded through doc 08's `umi-state-duck` or shelled out to a `duckdb` binary on the path, the same fallback ccrawl-cli uses. It attaches local Parquet as `pages`, `receipts` and `links`, and it can attach a published repository over HTTP so that a question about the corpus does not require downloading the corpus. This is the command that makes doc 15's dashboard mostly unnecessary.

## 14.7 Configuration

Precedence, highest first: command line flags, `UMI_*` environment variables, `./umi.toml` in the working directory, `~/.config/umi/config.toml`, built in defaults.

```toml
# ~/.config/umi/config.toml
[crawl]
rps         = 1.0
concurrency = 4
tier_max    = 3
out         = "~/crawls"

[state]
backend = "sqlite"

[publish]
org   = "open-index"
token = "env:HF_TOKEN"

[fetch]
coordinator = "https://umi.dev"
rate        = 2.0
```

Secrets are never literal in a config file. `token = "env:HF_TOKEN"` reads an environment variable, `token = "file:/run/secrets/hf"` reads a file, and a literal string is accepted with a warning printed on every run until it is fixed. There is no keyring integration, because a keyring is a portability problem in exchange for very little.

`umi config` prints the effective configuration with the source of every value, which is the thing you want at 2am when a setting is not taking effect.

## 14.8 `umi doctor`

Runs before anything else and checks the things that actually break.

```
$ umi doctor
  rust toolchain        1.98.0                                        ok
  tls backend           rustls 0.24, no openssl-sys in tree           ok
  emulation feature     wreq present, boringssl symbols prefixed      ok
  chromium              /usr/bin/chromium 141.0                       ok
  clock skew            +12 ms against pool.ntp.org                   ok
  dns                   resolves, no captive portal                   ok
  disk /var/lib/umi     112 GB free, 24 GB needed for 8 segments      ok
  memory                10.2 GB available, 1.5 GB budgeted            ok
  outbound to hf        11.4 MB/s measured over 8s                    ok
  inbound sample        38.1 MB/s measured over 8s                    ok
  hf token              valid, write access to open-index             ok
```

Two of these lines are load bearing. The `openssl-sys` check is doc 05.5's CI assertion run at runtime, because the BoringSSL symbol prefix conflict produces link failures and segfaults rather than clean errors and it is worth catching before a crawl rather than during one. The bandwidth measurements are doc 01's milestone 1 gate, and having them in `doctor` means the number gets measured on every box every time rather than once by whoever set it up.

## 14.9 Output and exit codes

Human readable when stdout is a terminal, machine readable otherwise, and `--json` forces the machine form. No colour when `NO_COLOR` is set or stdout is not a terminal. Progress goes to stderr so that `umi cat ... | head` behaves.

```
0  success
1  general failure
2  usage error
3  nothing to do (empty scope, everything disallowed by robots, frontier empty)
4  budget exhausted (pages, bytes or time limit reached, work remained)
5  network failure after retries
6  verification failure (digest mismatch, bad signature, manifest broken)
7  resource pressure (disk full, publish stalled, refused to proceed)
```

Exit 3 and exit 4 are separate on purpose. "Finished, there was nothing to crawl" and "stopped early, there is more" are different outcomes and a script needs to tell them apart.

Exit 6 is never retried automatically and always prints exactly what failed to match, both expected and actual. A digest mismatch is either corruption or a bug and both deserve a human.

## 14.10 What is deliberately not here

**No `--ignore-robots`, `--respect-robots=false`, or any equivalent.** Doc 07.8 says we do not do it, and a spec that says that while shipping a flag that does it is a spec nobody should believe.

**No `--user-agent`.** Doc 07.1's user agent is fixed for every tier, including the emulated one. Making it configurable makes umi a scraping tool with a crawler's documentation.

**No proxy configuration.** Doc 07.8 rules out residential proxies and rotation. `HTTPS_PROXY` is honoured for reaching the coordinator and Hugging Face, because that is a corporate network concern and not an evasion one, and it is explicitly not applied to crawl fetches.

**No CAPTCHA solving integration, no `--solve`, and no plugin hook that could become one.**

**No `--force` on GC.** Doc 12.7's four conditions cannot be bypassed from the command line. If the disk is full the answer is doc 15's backpressure, not deleting unpublished data.

**No `--no-verify` on publish.** Publishing unverified data is the one thing that would make the corpus worthless, and doc 03.7 lists it as a rule the system never breaks.

**No shell completion generator in v1.** It is genuinely useful and it is not milestone 1. Doc 17 has it under deferred.
