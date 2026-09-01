# 15 Operations

## 15.1 The three boxes

Doc 01 measured them and doc 03.4 assigned roles. This is what actually gets installed.

| | server1 | server2 | server3 |
| --- | --- | --- | --- |
| vCPU | 4 | 6 | 8 |
| RAM total | 6 GB | 11 GB | 23 GB |
| RAM available | ~0 GB | ~4 GB | ~10 GB |
| Disk free | 163 GB | 67 GB | 387 GB total, 112 GB free |
| Storage | SSD | mostly rotational | mostly rotational |
| Role | third coordinator, smallest PLD share | second coordinator, the only Chromium host | primary coordinator, publisher, analytics |
| `umid` RSS cap | 1.5 GB | 3.5 GB | 8 GB |
| CPU quota | 200% | 400% | 600% |
| Target rate | 250 pages/s | 250 pages/s | 250 pages/s |

server1 is the fragile one and the spec has said so three times already. It has the only SSD, which is where write heavy state shards belong, and essentially no free memory, which is where everything else goes wrong. The standing instruction is unchanged: if `umid` cannot live inside 1.5 GB there, demote it to a fetcher only node and give its PLD share to server3. That is a configuration change and a restart, not a redesign, precisely because doc 03.3 uses rendezvous hashing.

Layout on every box:

```
/usr/local/bin/umid                 the daemon
/usr/local/bin/umi                  the CLI
/etc/umi/umid.toml                  config, mode 0640, owner umi
/etc/umi/secrets/                   HF token, signing keys, mode 0600
/var/lib/umi/state/                 doc 08, on the fastest disk available
/var/lib/umi/segments/              doc 10, sized for 8 unpublished segments
/var/lib/umi/audit/                 raw body ring buffer, 24h, capped
/var/log/umi/                       journald is primary, this is the overflow
```

The user is `umi`, fixed rather than dynamic, because state files outlive the unit and a dynamic UID makes recovery from a backup an adventure.

```ini
[Service]
User=umi
ExecStartPre=/usr/local/bin/umid --config /etc/umi/umid.toml --check
ExecStart=/usr/local/bin/umid --config /etc/umi/umid.toml
ExecStop=/usr/local/bin/umi drain
Restart=on-failure
RestartSec=10
TimeoutStopSec=900
MemoryMax=1536M
MemoryHigh=1280M
CPUQuota=200%
IOWeight=50
LimitNOFILE=1048576
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
NoNewPrivileges=yes
ReadWritePaths=/var/lib/umi /var/log/umi
```

`TimeoutStopSec=900` because `umi drain` seals segments, publishes them, verifies the remote copies and deletes the local ones, and doc 12 budgets 10 minutes at p99 for that. A shorter timeout turns every restart into a SIGKILL, which doc 10.7 survives but which loses the shoal in flight for no reason.

`MemoryHigh` below `MemoryMax` matters more than it looks. `MemoryHigh` throttles and reclaims, `MemoryMax` kills. On server1 the difference is between a slow crawl and an OOM loop.

## 15.2 The kernel and the network

A crawler at 250 pages per second per host opens a lot of short lived connections to a lot of distinct addresses, and the defaults on a stock VPS are not set up for it. These are not micro optimisations, they are the difference between working and not.

```
net.ipv4.ip_local_port_range = 10240 65535
net.ipv4.tcp_tw_reuse        = 1
net.ipv4.tcp_fin_timeout     = 20
net.core.somaxconn           = 4096
net.core.rmem_max            = 16777216
net.core.wmem_max            = 16777216
net.netfilter.nf_conntrack_max = 262144
fs.file-max                  = 2097152
vm.swappiness                = 10
```

`nf_conntrack_max` is the one that bites first. At 250 pages per second with a 30 second timeout, steady state is a few thousand tracked connections, but a slow origin or a retry storm multiplies it, and a full conntrack table drops packets silently while every other metric looks fine. If the box does not need a stateful firewall, take conntrack out of the path entirely with a `NOTRACK` rule for outbound port 80 and 443 and the problem disappears.

**DNS is a first class operational concern and it is usually where a new crawler falls over.** At 750 pages per second across a long tail of hosts, we issue somewhere between 50 and 200 distinct lookups per second per box, and public resolvers rate limit that. Every box runs a local caching recursive resolver, `unbound`, with a large cache and prefetching on. `umid` additionally keeps its own in process cache through `hickory-resolver`, honouring TTLs with a floor of 60 seconds and a ceiling of 24 hours, holding roughly 2 million entries on server3 and 200000 on server1. A DNS failure is retried once and then the host is backed off for an hour rather than being retried into the ground, because a host whose DNS is broken stays broken and the retries are pure waste.

IPv6 is enabled and preferred where available. A meaningful fraction of the web is dual stacked, IPv6 addresses are less likely to be on a shared block reputation list, and Happy Eyeballs handles the failures.

## 15.3 The backpressure ladder

This is the most important section in the document. Doc 01 established that the fleet produces more data per day than it can store, doc 12 established that publishing is on the critical path, and doc 12.7 established that unpublished data is never deleted. Those three facts together mean the crawl must be able to slow itself down, automatically, before the disk fills.

The controlling signals are unpublished bytes on disk, publish lag, free disk, extraction queue depth, and RSS against budget. The ladder has five rungs and the daemon moves up immediately and down slowly.

**Level 0, normal.** Everything runs. Discovery is at doc 09's 35 percent, T3 rendering is enabled up to its 1 percent cap, community fetchers are being leased work.

**Level 1** at unpublished above 4 GB, or publish lag above 20 minutes, or free disk under 40 GB. Stop T3 rendering. Cut the discovery share from 35 percent to 15 percent, which shifts the mix toward revalidation, and revalidation is mostly 304 responses that cost almost no storage. This rung alone cuts byte production by roughly 40 percent while barely cutting page rate, which is exactly the trade we want first.

**Level 2** at unpublished above 8 GB, or lag above 45 minutes, or free disk under 25 GB. Halve the local lease rate. Stop leasing to community fetchers from this coordinator, since their deliveries land on our disk too and they have other coordinators to talk to. Drop T2 to T1 wherever T1 has ever worked for the host.

**Level 3** at unpublished above 16 GB, or lag above 90 minutes, or free disk under 15 GB. Stop leasing entirely. Keep accepting deliveries for outstanding leases, keep extracting, keep writing, keep publishing. The crawl stops growing but nothing in flight is lost.

**Level 4** at free disk under 5 GB. Refuse deliveries with `retry_after`, per doc 04's flow control, so fetchers hold their results rather than dropping them. Seal every open segment. Publish only. Alarm loudly. This is the emergency rung and reaching it means something upstream is broken, usually Hugging Face being unreachable for an hour.

Coming down requires being below the next lower threshold continuously for 10 minutes. Without that hysteresis the daemon oscillates between rungs every few seconds, which is worse than staying at the higher rung.

Two separate ladders run alongside the disk one. **CPU**: when the extraction queue exceeds 2000 rows or the extraction pool has been saturated for 30 seconds, shed T2 and T3 first, then cut lease rate, because a saturated extractor eventually expires leases and doc 06 counts expired leases against fetchers who did nothing wrong. **Memory**: at 85 percent of the RSS budget, drop doc 10's shoal cap to the 64 MB floor and evict the least recently used cold state shards; at 95 percent, stop leasing until it recovers. On server1 the memory ladder is the one that will actually fire.

Every rung transition is logged with the signal that caused it, is exported as a metric, and appears in `umi status` as a single line. An operator should never have to guess why the crawl slowed down.

## 15.4 What we measure

Prometheus text format on the admin listener, which is localhost only by default. About 30 series, chosen because someone will act on them.

```
umi_pages_fetched_total{tier,outcome}
umi_fetch_duration_seconds{tier}                 histogram
umi_bytes_in_total
umi_bytes_out_total
umi_frontier_size{state}
umi_admit_total{result}                          seen|admitted|held|excluded
umi_shard_miss_total                             doc 08's number to watch
umi_state_bytes
umi_state_op_duration_seconds{op}                histogram
umi_hosts_backing_off
umi_robots_fetch_total{result}
umi_render_pool_busy
umi_extract_queue_depth
umi_extract_duration_seconds                     histogram
umi_segments_unpublished
umi_unpublished_bytes
umi_publish_lag_seconds
umi_publish_duration_seconds{step}               histogram
umi_publish_failures_total{step}
umi_disk_free_bytes{path}
umi_backpressure_level{ladder}                   disk|cpu|memory
umi_fetchers_connected{state}
umi_fetcher_reputation                           histogram
umi_verify_total{layer,result}                   doc 06
umi_verify_disagreement_ratio                    the milestone 4 number
umi_quarantine_size
umi_dns_duration_seconds                         histogram
umi_dns_failures_total
umi_peer_lag_seconds{peer}
```

`umi_verify_disagreement_ratio` deserves special mention. Doc 06.7 makes it the number that must be calibrated before the community fleet opens, and it needs to be on the dashboard from milestone 1 so that we have months of baseline before it matters.

Scraping runs on server3 only. Running a metrics stack on server1 would consume the memory budget that `umid` needs, and if server3 is down the metrics are the least of the problems.

The endpoint exists ahead of `umid`, on `umi crawl --metrics`, because a crawl that runs for a day is the thing we are currently trying to measure and it should not have to wait for the daemon. Eight of the series above are filled from it today: `umi_pages_fetched_total`, `umi_bytes_in_total`, `umi_admit_total` for `seen` and `admitted`, `umi_unpublished_bytes`, `umi_publish_lag_seconds`, `umi_disk_free_bytes` and `umi_backpressure_level`. The rest render at zero, which is the honest answer for a number nothing has written yet, and each one needs a source rather than an estimate: the robots counters need the result of each fetch rather than the count of them, the state histograms need per call timings rather than a tick total, and the frontier gauge needs a count per state that the loop does not currently ask for.

## 15.5 The DuckDB view

Doc 08 includes a DuckDB backend for exactly this, and the design intent is that it is read mostly and attaches to published checkpoints rather than competing with the live crawl for the state file.

`umi checkpoint --format duckdb` writes a consistent snapshot of the ledger, host records and frontier statistics as a DuckDB file, on a schedule, defaulting to hourly on server3. Anything that wants to ask a complicated question asks it there, and a complicated question can then take 30 seconds without slowing the crawl by a millisecond.

On top of that sit a handful of views, which are the report rather than a dashboard framework:

```sql
-- coverage by pay level domain
select pld, count(*) urls, count(*) filter (where state = 'Fetched') fetched,
       max(last_fetch_ms) newest from ledger group by 1 order by 2 desc limit 100;

-- staleness distribution of the fresh core
select width_bucket((now_ms() - last_fetch_ms)/86400000, 0, 30, 30) day_bucket,
       count(*) from ledger where refresh_class in ('realtime','hourly','daily') group by 1;

-- tier mix by host, the doc 05 assumption under test
select host, count(*) n, avg(tier_used) mean_tier,
       count(*) filter (where tier_used >= 3)::float / count(*) render_share
from ledger group by 1 having n > 100 order by render_share desc limit 50;

-- publish health over the last day
select date_trunc('hour', to_timestamp(sealed_ms/1000)) h, count(*) segments,
       avg(published_ms - sealed_ms)/1000 mean_seconds
from segments where sealed_ms > now_ms() - 86400000 group by 1 order by 1;
```

`umi sql` from doc 14.6 runs these, and the nightly job renders them to a static HTML file. That is the whole dashboard. Grafana works fine against the Prometheus endpoint if someone already runs it, and nothing here requires it.

The distinction that matters: Prometheus answers "is it healthy right now", DuckDB answers "is the crawl good". They are different questions and trying to make one tool answer both is how monitoring setups become a second project.

## 15.6 Alarms

Five things page a human. Everything else is a graph.

**Publish lag above 90 minutes.** The crawl is on its way to level 3 and the disk is the countdown.

**Backpressure at level 3 or above for more than 15 minutes.**

**A manifest chain break**, from doc 12.8's reconciliation. This means published data disagrees with itself and it stops the publisher rather than being repaired automatically.

**Verification disagreement ratio doubling week over week.** Doc 06's threat model says a coordinated poisoning attempt looks exactly like this, and by the time it shows in the corpus it is too late.

**A coordinator unreachable for more than an hour.** Its PLDs are simply not being crawled, per doc 03.3, which is survivable but not indefinitely.

Deliberately not alarms: a single host blocking us, a spike in 429s, a fetcher being banned, a segment failing to publish once, T3 being disabled by backpressure. All of those are normal operation of a crawler on the 2026 web and paging on them trains people to ignore pages.

## 15.7 Runbook

**Crawl rate dropped and `bottleneck` says `politeness`.** Normal. We are polite and the frontier is concentrated in too few hosts. Check `umi_hosts_backing_off` and the PLD distribution. The fix is more hosts in the frontier, which means more discovery, not more concurrency.

**Crawl rate dropped and `bottleneck` says `cpu`.** Check `umi_extract_duration_seconds`. If the p99 has moved, a new extractor version or a new class of pathological document is responsible. `umi get` the slowest URLs from the last hour and profile.

**Publish lag climbing.** Check `umi_publish_failures_total{step}`. Upload failures mean Hugging Face or the network. Verify failures mean stop and look, because doc 12.7 will correctly refuse to delete anything and the disk will fill behind it.

**Disk filling despite publishing working.** Something is writing outside `segments/`. Usually the audit ring buffer with its cap misconfigured, or journald.

**A site operator complains.** `umi block <domain> --reason` immediately, then investigate. Doc 07.7 gives one hour and blocking first costs nothing since the block is reversible with a dated record.

**Reputation collapse across many fetchers at once.** Almost certainly our bug, not their attack. Check whether the coordinator's extractor version changed, because doc 11.10's version skew handling is the usual culprit. Do not ban anyone until this is ruled out.

**Frontier not growing.** Check `umi_admit_total{result}`. A high `excluded` count means a scope or block list problem, a high `held` count means doc 06's holding pen is not graduating anything, which means corroboration is failing, which means something upstream is broken.

**server1 OOM loop.** The demotion described in 15.1 and doc 03.4. Set it to fetcher only, restart, move its PLD share. This is a planned outcome and not an incident.

## 15.8 Recovery

**A coordinator dies and comes back.** State is on disk, doc 08's commit records truncate any torn tail, doc 09.8 rebuilds the in memory frontier heaps from the ledger, and unpublished segments are recovered by doc 10.7's commit record scan and pushed through publishing. Expected loss is the shoal in flight, which is under 5600 pages. Expected recovery time is a few minutes, dominated by rebuilding heaps for resident PLDs.

**A coordinator dies and does not come back.** Its PLDs are unowned and uncrawled until it returns, by design, and doc 03.3 explains why there is no failover. To move them permanently, remove the peer from the config on the other two and let rendezvous hashing reassign. The new owners have no ledger for those PLDs, so they refetch, which costs bandwidth and loses change history but is otherwise correct.

**State is lost entirely.** Recoverable, slowly. Cold state shards live in object storage per doc 8.6 and come back directly. What is not in object storage is rebuilt from the published corpus: `umi seed corpus` reads back the URLs and their `fetched_at_ms`, which reconstructs the seen set and a usable approximation of the ledger. Change rate history is gone and the estimator restarts from its prior. This is a day of work and it is not data loss in any sense that matters, which is the payoff for publishing everything.

**Segments are lost before publishing.** Up to 8 segments per host, about 170000 pages. Doc 12.8 resets their ledger rows to due and they are refetched within the hour.

**Hugging Face is unavailable.** Segments accumulate, the ladder climbs, the crawl slows and eventually stops at level 3 with everything intact. At 1 GB per host per 15 minutes of level 0 crawling, server3 has roughly 24 hours of headroom before level 3 and several days before anything is at risk. An outage longer than that means switching the publish target, and the manifest chain accommodates that because it records the repository per entry.

**A bad extractor version reaches production.** Published rows carry the extractor version, so the damage is identifiable with one predicate. The response is to pin the previous version, add the affected files to doc 12.8's exclusion list, and mark the affected URLs due for refetch. Nothing is rewritten and nothing is deleted.

## 15.9 Upgrades

Roll one box at a time, always server1 first, because it is the smallest share and the most likely to reveal a memory regression.

`umi drain`, upgrade, `umid --check`, start, watch for 15 minutes, move on. Draining rather than restarting means no segment is lost and no lease expires against an innocent fetcher.

State format changes get a version in the header and a one way migration run explicitly by the operator, never automatically on start. A daemon that silently migrates state on start is a daemon that cannot be rolled back.

Protocol changes follow doc 04's version field and the minimum supported version in `HelloAck`. Doc 11.10 gives 30 days of notice before the minimum extractor version moves, and the same applies to the protocol. We do not control the fleet and cannot force an upgrade, so every change has to be compatible for a window.

## 15.10 What this costs to run

Three existing VPS instances, so the marginal infrastructure cost is object storage for cold state shards and egress to Hugging Face.

Cold state at doc 08's target of under 20 bytes per known URL is roughly 2 TB at 100 billion URLs, which is a few tens of dollars a month on any object store. Egress is doc 01's 11.8 TB per month, which fits inside the existing allowances. Hugging Face hosting is free for public datasets, which is the entire reason the publishing target is Hugging Face and not our own storage, and doc 12.6 makes the conversation about repository count a milestone 5 deliverable rather than a surprise for them.

The real cost is attention. A crawler at this scale is not a thing you deploy and forget, and the honest operational commitment is one person checking the dashboard daily and being reachable for the alarms in 15.6. Doc 17 lists that as a project risk, because it is one.
