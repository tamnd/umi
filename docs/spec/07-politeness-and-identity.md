# 07 Politeness and identity

A crawler at 750 pages per second is a visible participant in the web whether it wants to be or not. Legitimacy is much cheaper to maintain than to recover, and in 2026 there is a specific reason it is cheaper than it used to be: the infrastructure now has cryptographic ways to tell a well behaved crawler from an anonymous scraper, and using them puts us on the right side of defaults that are otherwise closing.

## 7.1 Identity

One user agent, fixed, for every tier including T2 and T3.

```
umi/1.0 (+https://umi.dev/bot)
```

The URL resolves to a page that states who runs the crawler, what the data is used for, where the published corpus is, the IP ranges we crawl from, how to rate limit us, how to block us, and a contact address that a human reads. That page is a deliverable, not a nicety, and it ships before the first crawl outside our own domains.

T2 is the awkward case. Its whole purpose is to present a browser's TLS and HTTP/2 fingerprint, and a browser fingerprint with a bot user agent is an inconsistency. We keep the honest user agent anyway. The alternative is to present ourselves as Chrome, which is the thing that turns a crawler into a scraper in every sense that matters, including legally. If a site's bot management scores us as suspicious because the layers disagree, that is a signal we should accept rather than paper over.

Forward confirmable reverse DNS is set up for every crawling address, so `62.171.131.190` reverses to `fetch-3.umi.dev` and `fetch-3.umi.dev` resolves back to `62.171.131.190`. This is how Googlebot and Bingbot are verified and it is the check most site operators know how to run. It works because the two halves are held by different parties: anyone can put `umi.dev` in a PTR record for an address they own, and only we can put an address in a name under `umi.dev`. The names are `fetch-N.umi.dev`, under the same domain as the bot page and the key directory, so one domain answers every question about who we are.

The addresses are also published as a list, at `https://umi.dev/bot/umi.json`, in the same shape as Google's `googlebot.json` down to the field names. An operator who wants to allow us, rate limit us or just recognise us should not have to write a second reader for our version of the same file, and the tooling that already eats Google's list works on ours unchanged. The one addition is a `name` on each entry carrying the reverse DNS name for that range, which a reader that does not expect it ignores. Ranges today are single addresses, written `/32` and `/128`, because three servers is what there is.

```json
{
  "creationTime": "2026-08-29T00:00:00.000000",
  "prefixes": [
    { "ipv4Prefix": "62.171.131.190/32", "name": "fetch-3.umi.dev" }
  ]
}
```

Every box checks its own reverse DNS rather than trusting that somebody set it up. `umi doctor` finds the address the kernel would send from, looks it up in the published list, and for an address that is on the list does the full forward confirmation and says which name came back. An address that is not on the list is skipped rather than judged, because a volunteer's laptop has whatever PTR record their provider gave it and none of this asks anything of it. The query goes to a public resolver and not to the one in `/etc/resolv.conf`, which matters more than it sounds: a hosting provider that puts the box's own name in `/etc/hosts` makes the local resolver answer the forward half with a loopback address, so the box would fail its own check while the rest of the internet saw it pass. The question is what somebody else sees.

When an address changes, the order is fixed and it is the reverse of what feels natural. The published list gains the new address first and keeps the old one, then reverse DNS is set for the new address and forward confirmation is checked from the box itself, then the crawl moves, and only after a full crawl cycle on the new address does the old entry come out of the list. Publishing last would mean crawling from an address that operators have no record of, which is exactly the thing an allowlist is meant to prevent, and removing the old entry early would strand anybody who checks a log from yesterday. An address that is retired for cause, rather than by planning, comes out of the list immediately and the bot page says when and why.

## 7.2 Web Bot Auth

Every request from a coordinator operated fetcher is signed under RFC 9421 HTTP Message Signatures following `draft-meunier-webbotauth-httpsig-protocol`, which as of August 2026 is at revision 02, authored by Cloudflare and Google, and backed by an IETF working group.

The mechanics are three headers. `Signature-Agent` points at our public key directory. `Signature-Input` names the covered components. `Signature` carries the Ed25519 signature. We sign at minimum the `@authority` derived component, per Cloudflare's guidance, plus `@method`, `@path`, and a nonce and timestamp for replay resistance.

Keys are Ed25519, published at `https://umi.dev/.well-known/http-message-signatures-directory`, rotated quarterly with an overlap window, and the rotation is announced on the bot page. The directory is a JSON Web Key Set, one entry per key we have ever signed with, and a `keyid` is the RFC 7638 thumbprint of the key it names. A rotated key keeps its entry and gains an `exp`, rather than being deleted, because deleting it would make every request we have ever signed unverifiable after the fact and the point of signing is that somebody can check later.

The covered components are `@authority`, `@method`, `@path` and the `Signature-Agent` header, and a signature is good for sixty seconds. `Signature-Agent` is covered as well as sent, which is the draft's rule and is not a formality: an unsigned pointer at a key directory would let anyone replay one of our signatures while naming a directory they control.

The private key stays on the coordinator that owns it. It is configured as `crawl.identity_key` in doc 14.7's file, it is an `env:` or a `file:` indirection like every other secret, and nothing in the system writes it to a log, a manifest or a published row. A crawl with no key configured signs nothing and says so in its log, which is the honest way for a volunteer's build to run before doc 06 has given them a fetcher key.

This is worth doing for a concrete reason rather than as standards compliance theatre. Cloudflare activated Web Bot Auth at their edge in March 2026, fronts roughly 20 percent of the web, and moved to blocking AI crawlers by default in July 2025. Their verified bots program accepts message signatures, and Common Crawl is among the supported agents. Being verifiable is the difference between being allowed by default on a fifth of the web and being blocked by default on it.

Community fetchers present a different signature, tied to their own fetcher key, plus a `Signature-Agent` pointing at the coordinator's directory of active fetcher keys. That directory is public and machine readable, which means a site operator can distinguish a coordinator fetch from a volunteer fetch and rate limit them separately if they want to. It also means a badly behaved volunteer is attributable, which is doc 06's problem but this is how it becomes visible from outside.

## 7.3 Purpose declaration

Cloudflare's 2026 default distinguishes crawler purposes, allowing pure search indexing by default while blocking mixed use crawlers that blend search, agent use and training on monetised pages. The stated requirement for AI companies wanting access is to separate crawlers by function and identify themselves honestly.

umi's purpose is a single one and we declare it: **building a public, openly licensed web index**. We do not train models on the corpus, we do not run an agent that browses on a user's behalf, and we do not resell access. The corpus is published and what other people do with it is out of our control, which is a fact we state plainly on the bot page rather than eliding. Anyone can and will train on an open dataset, and pretending otherwise would be the dishonest version.

The bot page carries this as a structured declaration, and each published `pages` row carries the crawl purpose and the crawl profile that fetched it, so a consumer can filter by it.

We do not operate separate crawler identities for separate purposes because we only have one purpose. If that changes, it gets a separate user agent, a separate key, and a separate robots token, because the whole value of purpose declaration is that it is granular enough to act on.

## 7.4 robots.txt

RFC 9309, implemented properly, with the specific behaviours that trip people up spelled out.

Fetch `/robots.txt` per scheme, host and port. Cache for 24 hours per the RFC's guidance, or for `Cache-Control: max-age` when it is shorter. On 5xx, treat as full disallow and retry with backoff, per the RFC. On 4xx, treat as full allow. On a redirect, follow up to 5 hops. Cap the parsed size at 500 KiB and ignore the remainder, per the RFC.

Group selection matches on the user agent token, case insensitively, longest match wins. Our tokens in precedence order are `umi`, then `*`. We do not match on `umibot`, `umi/1.0` or any variant, and the bot page states the exact token to use. Sites that block `*` block us.

`Allow` and `Disallow` resolve by longest matching path, with `Allow` winning ties, which is the RFC's rule and differs from the historical Google behaviour only in edge cases.

`Crawl-delay` is not in RFC 9309 but is widely deployed and we honour it, clamped to [0.1 s, 300 s]. A `Crawl-delay` above 300 seconds is treated as 300 and the host is deprioritised rather than blocked, since some sites publish absurd values that amount to a soft block.

`Sitemap` lines are extracted and fed to the seeding path in doc 13. This is the single highest value line in robots.txt for us and it is often ignored by crawlers.

The robots decision is made by the coordinator, never by the fetcher, and doc 04 makes disallowed URLs unleaseable. A community fetcher does not need a robots parser and cannot make us impolite through a parsing bug. This is worth the extra round trip on off host redirects.

Robots snapshots are published. `open-index/umi-robots` carries the raw text, fetch time, host, and parsed decision summary for every host we have checked. Nobody publishes a longitudinal robots corpus at scale and it is directly useful for studying how the web's crawl permissions are changing, which given the last two years is a live question.

## 7.5 AIPREF and Content-Usage

The IETF AIPREF working group is producing `draft-ietf-aipref-vocab`, a vocabulary for expressing AI usage preferences, and `draft-ietf-aipref-attach`, which attaches those preferences to content through a `Content-Usage` line in robots.txt and an equivalent HTTP response header. A final RFC is not expected before August 2026 and the group has had consensus problems, so this is a moving target.

umi parses `Content-Usage` now, from both robots.txt and the HTTP response header, and stores it per host and per URL. The parse follows the draft: identify lines labelled `Content-Usage`, ignore whitespace around the label and the colon, take the remainder up to the first CR, LF or `#` as the rule value. The important semantic difference from `Allow` and `Disallow` is that conflicting preferences on identical paths apply separately per the vocab draft's reconciliation process rather than resolving to the more permissive option.

What we do with it: we record it in the published data on every affected row and every host row, and we do not act on it for crawling decisions, because our purpose is index building and the AIPREF vocabulary is about AI usage. A `train-ai=n` preference is a statement about downstream use, and the honest thing is to propagate it to downstream users rather than to refuse to index. A consumer building a training set from our corpus can filter on it in one predicate, which is a better outcome than the preference being lost at crawl time.

If the final RFC includes a directive that clearly covers index building, we honour it as a crawl decision. Until then, propagate rather than interpret.

The cost of all of this is a parser and a column, and the benefit is that the open corpus carries the web's stated preferences alongside its content. That is a thing no existing web scale corpus does.

## 7.6 Rate limiting

Per host, always, with no configuration that disables it.

The default is one request in flight per host and a delay between requests of `max(crawl_delay, adaptive_delay)` where the adaptive delay starts at 1000 ms and moves with observed behaviour:

```
adaptive_delay_next = clamp(
    adaptive_delay * f(response),
    floor_for_host,
    60_000 ms
)

f: 200 fast          -> 0.9   (speed up gently)
   200 slow (>2s)    -> 1.3   (the origin is struggling)
   429 or 503        -> 4.0   (back off hard)
   connection error  -> 2.0
   5xx               -> 2.0
```

`floor_for_host` is 200 ms for hosts in the top 10k by size where we have observed sustained fast responses, 1000 ms for everything else. Nothing goes below 200 ms and no host ever sees more than 5 requests per second from the entire fleet, including community fetchers.

Fleet wide enforcement is the hard part with an open fetcher fleet, and it is solved structurally rather than by trust. A host's politeness timer lives on exactly one coordinator, the one that owns its PLD, and leases for that host are only issued when the timer allows. A fetcher cannot cause a second concurrent request to a host because a second lease is not issued. Doc 04's `min_gap_ms` is a belt on top of that brace, covering the case where a fetcher holds two leases for the same host from different lease calls.

`Retry-After` is honoured exactly, including the HTTP date form, up to 24 hours.

Per PLD, not just per host, there is a global cap of 20 requests per second across all hosts under one registrable domain, so a site with 5000 subdomains does not get 5000 times the traffic.

## 7.7 Handling complaints

There is a contact address on the bot page and it goes to a person. The operational commitment is a response within one business day and a block applied within one hour of a valid request.

`umi block <domain> --reason <text>` writes a permanent entry to state that removes the domain from the frontier, prevents future admission, and marks existing published rows for exclusion in the next corpus revision. The block list is published in `open-index/umi-meta` so that a downstream consumer of an older snapshot can honour it too. This matters: an open corpus that cannot be retroactively corrected is a liability, and the mechanism has to exist before the first complaint, not after.

The unit is the registrable domain and not the host. Somebody typing `news.example.com` is asking for that site to stop being crawled, and blocking one host while the rest of the site keeps being fetched would honour the letter of the request rather than the request. The command widens what it was given and says so, because a block that quietly covers more than was typed is as bad as one that quietly covers less.

Enforcement is at lease issue and not at fetch. Applying a block takes the domain's known URLs out of the frontier in the same transaction that records it, and the check at lease issue is what covers a URL that arrives afterwards by some other route. A block that depended on the sweep having reached everything would be a block with an ordering bug waiting in it.

The published list is how a block reaches the rest of the fleet. Each entry is one file under `blocks/` in `open-index/umi-meta`, holding the domain, the reason and the dates, and a coordinator applies the published list to its own frontier before its first fetch. One file per domain rather than one list everybody rewrites, so that two operators working at once cannot lose each other's entry and a consumer who cares about one domain reads one small file.

Blocks are never silently reversed. A domain that asks to be unblocked gets a dated record of both events. `umi block <domain> --lift --reason <text>` writes the lift onto the entry that is already there, keeping the original date and the original reason, and the entry stays in the published list forever. A lift gives back every URL of the domain that was excluded, not only the ones the block took, because the ledger does not record why a URL was excluded and section 08.4 is deliberate about that. The robots layer excludes the robots ones again the next time it looks, which costs one recheck of a file we are about to fetch anyway.

## 7.8 What we do not do

We do not ignore robots.txt for any reason, including for URLs that a partner has asked us to crawl. If a partner wants their own site crawled despite their robots.txt, they change their robots.txt.

We do not use residential proxies, proxy rotation services, or IP pools designed to obscure origin. Our IPs are published and forward confirmable. A crawler that hides where it comes from cannot claim legitimacy, and legitimacy is the entire strategy.

We do not solve CAPTCHAs, use CAPTCHA solving services, or run challenge bypass. A challenge is an answer and the answer is no.

We do not crawl anything behind authentication, including content that is accessible with a free account. If it needs a login it is not the open web.

We do not fetch URLs discovered only from a page we were not allowed to fetch.

We do not retain raw response bodies beyond the audit window in doc 04.4, which is 24 hours. This limits the blast radius of any single mistake, and it means a request to delete content is satisfiable from the published corpus alone.
