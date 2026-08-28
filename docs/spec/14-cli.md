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

`--publish` is a flag on all three of `crawl`, `resume` and `watch`, and it means the same thing on each one. A crawl started with it and resumed without it keeps its next segments locally, which is deliberate: the flag is the operator saying what this run should do, not a property the directory remembers, because the two things it needs are secrets and secrets do not belong in a directory that gets moved around.

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
  --no-sitemaps                do not follow them
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

### `umi publish`

```
umi publish ./example.com
```

`--publish` on the crawl is the decision made in advance. `umi publish <dir>` is the same decision made afterwards, against a directory that is already on the disk, and it has to exist because `umi crawl example.com` with no flags is the first command anybody runs and it keeps its output. Somebody who crawls a site, looks at what came out and then decides it is worth sharing should not have to crawl the site again to share it.

It reads `profile.toml` to find out what the crawl was, which is also how it tells a crawl directory from any other directory, and it refuses a directory that does not have one. Then it puts the publishing key in doc 12.5's directory if it is not already there, gives every local file a row in the state ledger, and hands the ledger to doc 12.2's pipeline. That middle step is the whole of what this command adds: a crawl that ran without `--publish` writes no segment rows, so without it there would be nothing for the publisher to find.

Both kinds of local file are picked up. `data/*.parquet` is what a finished crawl holds and is the usual case, and `segments/*.umi` is what a crawl that died between sealing a segment and converting it left behind. A file whose name is not the segment's ULID is skipped and reported rather than published, because a file in the corpus has to trace back to the segment it came from and inventing an identifier for it would break that.

Everything after that is doc 12, unchanged. The same eight steps, the same read back, the same signed day manifest, and the same four conditions in doc 12.7 before a local file is deleted. A directory with nothing left to publish is exit 3. A run where some files published and others did not prints what it did and then exits on the worst failure it saw, so a script does not read a partial success as a success.

It does not crawl, seed or extract. If the directory is short of pages the answer is `umi resume`, and this stays the command that publishes what is there.

### `umi resume` and `umi watch`

```
umi resume ./example.com
umi watch ./example.com
```

Both continue a crawl directory and neither takes a scope, because doc 13.5's promise is that the directory is the unit and everything a continuation needs is already in it. Neither reseeds either, since a resume that put the seeds back in the frontier on every restart would be a crawl that never finished.

The difference is the stop rule. `umi resume` stops when the frontier is empty, the same as the crawl that made the directory. `umi watch` does not stop, and refreshes what it has instead, on the schedule doc 09.4's estimator wrote onto each row when it was fetched. There is nothing else to it: a completed row carries its own due time and becomes leasable again when that time arrives, so watching is the ordinary loop without the exit.

Two things make it survivable over days rather than minutes. It backs off between empty ticks, from one second to a minute, and drops back to a second the moment a tick leases anything, because a command that is meant to be idle most of the time should not wake up once a second for a fortnight to prove it. And it stops on the first interrupt rather than dying on it: ctrl-c lets the fetches in flight finish, seals the open segment, converts it like any other, and exits 0, because the operator asked it to stop and it stopped. A second interrupt is the escape hatch for a slow origin holding the last fetch, and that one is the shell's 130.

SIGTERM is the same stop as ctrl-c. A command built to run for a fortnight spends almost all of its life under something that starts it and stops it, and systemd, docker, kubernetes and a plain `kill` all stop it that way rather than by sending an interrupt. Taking the default handler there would lose the open segment on every ordinary restart, which is the one thing the graceful stop exists to prevent.

While it is quiet it says so every five minutes, with what the frontier holds and how long it has been up. A watch with a healthy schedule and a watch that has hung look exactly alike otherwise, and the log is the difference.

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

`umi ls` takes a directory or a repository and the columns are the same either way, because the question is the same. Against a repository it reads doc 12.5's day manifests for the row counts and the time ranges, which is the only place those numbers exist over the network, and it reads the repository listing for what the hub actually holds. The last column is where those two answers meet:

```
$ umi ls open-index/umi-focus-blog.rust-lang.org
file                                              day         rows          bytes       span      hub
...260825/01M0WVX2QCSQS1EJYVA57SYJBY.parquet 20260825           61         228216        61s       ok
1 file, 61 rows, 228216 bytes over 1 day in open-index/umi-focus-blog.rust-lang.org
```

`ok` is the file being on the hub at the size the manifest published. `missing` is a manifest naming a file the hub does not have, `size` is the hub having it at some other length, and `unnamed` is a file under `data/` that no manifest claims, which is what doc 12.8's reconciliation exists to clean up. A file only the hub knows about still gets a row, with a dash where the row count would be, because the hub can say how many bytes a file is and has no way to say how many rows are in it.

Two things `ls` deliberately does not do. It does not check a signature, a chain or a digest, so a repository it prints happily is not a repository that has been verified, and the summary line says so by pointing at `umi verify` whenever the manifests and the hub disagree. And it does not fail on a disagreement: it prints the rows and exits zero, because a listing that refused to describe a broken repository would be useless on exactly the repository somebody needs to look at. An empty repository is exit 3 and a manifest that will not parse is exit 6, the same as `umi verify` gives it.

A target that exists on this disk is a path. After that a name with a slash in it is a repository, and a bare name is one too if it starts with `umi-`, which is how doc 12.4 spells every repository this project publishes. Everything else stays a path, so a mistyped directory reports a mistyped directory instead of going to the network to look for it.

`umi get` is the debugging workhorse. It runs one URL through the full ladder with verbose tier reporting and prints whatever you ask for: `--markdown`, `--text`, `--links`, `--meta`, `--headers`, `--receipt`, `--raw`. It is also the fastest way to answer "why did this page extract badly", and it prints the extractor version so the answer is reproducible.

`umi sql` is DuckDB, either embedded through doc 08's `umi-state-duck` or shelled out to a `duckdb` binary on the path, the same fallback ccrawl-cli uses. It attaches local Parquet as `pages`, `receipts` and `links`, and it can attach a published repository over HTTP so that a question about the corpus does not require downloading the corpus. This is the command that makes doc 15's dashboard mostly unnecessary.

### `umi verify`

```
umi verify open-index/umi-focus-blog.rust-lang.org
umi verify umi-focus-blog.rust-lang.org --full
```

`umi verify` checks a published repository from the outside. It needs a network and a repository name and nothing else: no crawl directory, no state store, no token unless the repository is private, and no file left over from the machine that did the crawl. That is the whole point of it. Verification that only works where the crawl ran is verification of the local disk, and doc 16's gate 1.5 is the test that says so out loud, so this command is written as though it had never seen the crawl. A name with no slash in it gets the configured organisation in front of it, and a name that was spelled out in full is used exactly as typed.

It reads every day manifest in the repository, checks that each one parses and is in canonical form, checks the detached signature against the publishing keys published in `umi-meta`, checks that each day's `prev` is the digest of the day before it, and then checks that every file the manifest names is on the hub at the size and the sha256 the manifest gives. A day with no signature is a failure and not a skip.

The file check is free by default and that is worth explaining. Hugging Face stores a large file through lfs, and lfs names an object by the sha256 of its content, so the digest in the listing is a digest of the bytes rather than of a git blob header, and comparing it to the manifest checks the whole file without downloading a byte of it. It is a real check that trusts the hub to have computed the digest honestly. `--full` downloads each file and digests it locally, which trusts nothing and costs the bandwidth. The default is the cheap one because a verifier that downloaded a week of the corpus every time is a verifier nobody runs, and the output says which of the two ran so that nobody has to guess.

Everything that fails to check out is exit 6, which doc 14.9 never retries automatically. A repository that does not verify does not start verifying because you asked twice.

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
key   = "env:UMI_PUBLISH_KEY"

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

`--bandwidth` is the gate itself rather than the sample. It runs eight concurrent streams for sixty seconds in each direction, `--bandwidth-secs` changes the sixty, and it moves a few gigabytes and takes a couple of minutes, which is why the plain `doctor` takes a one request sample and says out loud that the sample is too small to be the gate.

```
$ umi doctor --bandwidth
  measuring 60 s in each direction over 8 streams, this takes about 2 minutes
  inbound 466.4 Mbit/s, 3.50 GB in 60.0 s
      https://ash-speed.hetzner.com/1GB.bin  249 MB
      https://fsn1-speed.hetzner.com/1GB.bin  359 MB
      https://hel1-speed.hetzner.com/1GB.bin  313 MB
      https://cachefly.cachefly.net/100mb.test  2578 MB
      the interface counters saw 3.57 GB over the same window, which includes whatever else this box is doing
  ...
  inbound sustained     466 Mbit/s, 1098 pages/s at 53.1 KB              ok
  outbound sustained    519 Mbit/s, 10807 pages/s at 6.0 KB             ok
```

The per endpoint block is not decoration. The first attempt at doc 16's gate 1.1 reported single digit megabits on all three boxes and looked like a catastrophic result, and it was wrong: the endpoint it used returned one byte and what got measured was background noise. A speed test that moves no bytes is indistinguishable from a slow link, because both report a small number, so the measurement asserts it moved a plausible number of bytes before it is allowed to be a measurement, and it names what each endpoint delivered so that a dead endpoint reads as a dead endpoint.

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
