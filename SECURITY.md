# Security

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/tamnd/umi/security/advisories/new), or email tamnd87@gmail.com. Please do not open a public issue for anything that would let somebody poison a corpus, forge a receipt, or make umi hit a third party harder than it should.

Expect an acknowledgement within 72 hours. There is no bounty programme.

## If umi is crawling your site badly

This is treated as a security-class report, not a feature request, and it gets answered first.

The fastest path is the block request at [umi.dev/bot](https://umi.dev/bot), which stops the whole fleet rather than one machine. `umi block <domain> --reason <why>` is what an operator runs, and the block is permanent and published alongside the corpus rather than applied quietly.

Robots is honoured under RFC 9309 and there is no flag to disable it. If you have a `Disallow` in place and umi fetched the path anyway, that is a bug and we want the URL, the timestamp and the user agent string you saw.

## What the threat model actually is

The fetcher fleet is open, so it is assumed hostile. [Doc 06](docs/spec/06-trust-and-verification.md) is the full treatment, and the summary is that a fetcher is never trusted to tell the truth about what it fetched. Receipts are signed, a sampled fraction of work is replayed by an independent fetcher, disagreement is measured rather than assumed to be attack, canary URLs with known content are mixed into leases, reputation is earned slowly and lost fast, and links from an unproven source sit in a holding pen before they can affect the frontier.

The attacks that matter most, in order:

**Corpus poisoning.** A fetcher returns content the origin never served, aiming at what gets trained on downstream. This is the reason for replay sampling and quorum.

**Frontier poisoning.** A fetcher returns real content and fabricated outlinks, steering the crawl. This is the reason links have a holding pen and a source.

**Politeness laundering.** Someone uses the fetcher protocol to aim the fleet at a target. This is the reason rate limits live on the coordinator, keyed by pay level domain, and not on the fetcher.

**Supply chain.** Release binaries carry sigstore build provenance and a checksum, because a fetcher is a binary strangers run against other people's servers.

## Scope

In scope: anything that lets a fetcher's output be trusted when it should not be, anything that lets a crawl exceed its politeness budget, anything that lets a published manifest or signature be forged or replayed, and anything that lets a crawled page compromise the machine parsing it.

Out of scope: the fact that crawled content itself can be hostile is by design, since the whole job is fetching bytes strangers wrote. Reports that umi can be made to fetch a URL are not vulnerabilities unless the fetch violates robots, rate limits, or the scope it was given.
