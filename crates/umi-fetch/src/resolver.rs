//! Names to addresses, without getaddrinfo.
//!
//! The reqwest default hands every name to `getaddrinfo` on a pool of blocking
//! threads. That is the right default for a program that makes a few requests
//! and the wrong one for a crawler, because `getaddrinfo` goes through the
//! platform name service switch, holds a process wide lock on most libc
//! builds, and answers no faster when more threads ask. Measured on server3
//! against twenty thousand distinct hosts: 75 lookups a second on sixteen
//! threads, 112 on sixty four, 122 on two hundred and fifty six. It does not
//! scale, and under load single calls sat for more than twenty seconds, which
//! from the crawl loop's point of view is a fetch slot doing nothing.
//!
//! The same box in the same minutes, asked over raw UDP at the same resolver,
//! answered 288 queries a second with sixty four outstanding and 614 with five
//! hundred and twelve, and did scale. So the resolver was never the problem.
//! Doc 16's gate 3.1 wants 250 pages a second on one box, and a page needs at
//! least one name, so the ceiling `getaddrinfo` imposes is half the gate.
//!
//! What is here is hickory speaking DNS itself, with three settings changed
//! from its defaults because its defaults are also sized for a program that
//! makes a few requests. See [`Resolver::shared`].
//!
//! One resolver per process rather than one per client. The cache is the whole
//! point and two clients on the same box crawl the same hosts, so a T1 client
//! and a T2 client sharing it is a cache hit rather than a second lookup.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hickory_resolver::TokioResolver;
use hickory_resolver::config::{LookupIpStrategy, NameServerConfig, ResolverConfig};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// How many answers to keep. A broad crawl comes back to a host many times
/// over a day and every hit is a query that never leaves the box, so this is
/// the setting that decides how much DNS the fleet does at all. Hickory
/// defaults to 32, which for a crawler is the same as no cache.
const CACHE: u64 = 262_144;

/// How many queries may be outstanding on one nameserver connection. Hickory
/// defaults to 32 and answers everything past that with a busy error, which
/// would show up in the crawl as a connect failure and retire the host. The
/// crawl runs hundreds of fetches at once and each one may want a name, so
/// this is sized above the fetch window rather than near it.
const ACTIVE: usize = 4096;

/// How long to wait for one attempt. Hickory defaults to 5 seconds and tries
/// twice, so a name behind a resolver that has stopped answering holds a fetch
/// slot for 10. Two seconds and two attempts is the same two chances in less
/// than half the time, which matters because a fetch slot is the scarce thing
/// and a name that is slow to resolve is usually a name that will not resolve.
const ATTEMPT: Duration = Duration::from_secs(2);

/// The environment variable that names the resolvers to ask.
///
/// Comma separated addresses, and it overrides whatever the platform is
/// configured with. It exists because of what the fleet found on its own
/// boxes. Ubuntu points `/etc/resolv.conf` at `127.0.0.53`, the
/// systemd-resolved stub, and the stub is a single process that answers one
/// query at a time from a cache it sizes itself. Measured on server3 against
/// two thousand cold names: 133 answers a second with 64 outstanding and 138
/// with 512, and past 256 the failures climb because queries are waiting on
/// the stub rather than on the network. Doc 16's gate 3.1 wants 250 pages a
/// second and each cold page needs a name and its robots.txt needed another,
/// so the stub alone is well under half the gate.
///
/// Naming the upstream resolvers directly takes the stub out. That is an
/// operator's decision and not ours to make silently, because the stub is
/// also what carries a box's VPN and split horizon configuration, and a
/// crawler that quietly went around it would resolve differently from
/// everything else on the machine.
pub const SERVERS: &str = "UMI_DNS_SERVERS";

/// The process resolver.
///
/// Cheap to clone, and clones share the cache. Build it with
/// [`shared`](Self::shared).
#[derive(Clone, Debug)]
pub struct Resolver {
    /// Built on first use rather than on construction, because building it
    /// needs a Tokio runtime and a client can be built outside one.
    inner: Arc<OnceLock<TokioResolver>>,
}

impl Resolver {
    /// The one for this process.
    ///
    /// The nameservers come from the platform, which on Unix means
    /// `/etc/resolv.conf` and on Windows means the registry, unless the
    /// environment names them instead. See the SERVERS constant for what that
    /// is worth on a box whose platform answer is the systemd-resolved stub.
    /// Both address families are asked for, because the fleet has v6 and a v6
    /// only host is a host nobody else is crawling.
    #[must_use]
    pub fn shared() -> Self {
        static SHARED: OnceLock<Resolver> = OnceLock::new();
        SHARED
            .get_or_init(|| Self {
                inner: Arc::new(OnceLock::new()),
            })
            .clone()
    }

    /// A resolver of its own, with a cache nobody else is filling.
    ///
    /// Everything in the fleet wants [`shared`](Self::shared), because the
    /// cache is the whole point and a second cache is a second set of
    /// queries. This exists for the DNS benchmark, which measures what a cold
    /// lookup costs and cannot measure that on a resolver the rest of the
    /// process has spent the day warming.
    #[must_use]
    pub fn fresh() -> Self {
        Self {
            inner: Arc::new(OnceLock::new()),
        }
    }

    /// The resolver, built if this is the first call.
    ///
    /// Returns `None` when the platform has no usable DNS configuration at
    /// all, which is a fetch that fails rather than a crawl that stops.
    fn get(&self) -> Option<&TokioResolver> {
        if let Some(built) = self.inner.get() {
            return Some(built);
        }
        let mut builder = match configured() {
            Some(config) => {
                TokioResolver::builder_with_config(config, TokioRuntimeProvider::default())
            }
            None => TokioResolver::builder_tokio().ok()?,
        };
        let options = builder.options_mut();
        options.cache_size = CACHE;
        options.max_active_requests = ACTIVE;
        options.timeout = ATTEMPT;
        options.attempts = 2;
        options.ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
        let built = builder.build().ok()?;
        let _ = self.inner.set(built);
        self.inner.get()
    }
}

/// The resolvers named in the environment, if any are.
///
/// An address that will not parse is skipped rather than fatal, and a variable
/// holding nothing usable falls back to the platform. A crawler that refused
/// to start over a typo in a tuning knob would be trading a slow run for no
/// run.
fn configured() -> Option<ResolverConfig> {
    let listed = std::env::var(SERVERS).ok()?;
    let servers = addresses(&listed);
    if servers.is_empty() {
        return None;
    }
    // UDP and TCP both, because a large answer comes back truncated over UDP
    // and the retry has to have somewhere to go.
    let name_servers = servers
        .into_iter()
        .map(NameServerConfig::udp_and_tcp)
        .collect();
    Some(ResolverConfig::from_parts(None, Vec::new(), name_servers))
}

/// The addresses in a comma separated list, skipping anything that is not one.
fn addresses(listed: &str) -> Vec<IpAddr> {
    listed
        .split(',')
        .filter_map(|entry| entry.trim().parse().ok())
        .collect()
}

impl Resolve for Resolver {
    fn resolve(&self, name: Name) -> Resolving {
        let this = self.clone();
        Box::pin(async move {
            let resolver = this
                .get()
                .ok_or_else(|| Box::<dyn std::error::Error + Send + Sync>::from("no dns config"))?;
            let found = resolver.lookup_ip(name.as_str()).await?;
            // Collected rather than borrowed because the iterator reqwest
            // wants outlives the lookup it came from.
            let addrs: Vec<SocketAddr> = found.iter().map(|ip| SocketAddr::new(ip, 0)).collect();
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_resolver_answers_from_the_hosts_file() {
        // No network in CI, so the only name worth asking for is the one the
        // platform answers out of its own hosts file. What this checks is that
        // the resolver builds and that a lookup gets as far as an address,
        // which is the part that used to be reqwest's job.
        let addrs: Vec<SocketAddr> = Resolver::shared()
            .resolve("localhost".parse().expect("name"))
            .await
            .expect("lookup")
            .collect();
        assert!(
            addrs.iter().any(|a| a.ip().is_loopback()),
            "localhost resolved to {addrs:?}"
        );
    }

    #[test]
    fn a_list_of_servers_reads_as_addresses_and_skips_what_is_not_one() {
        assert_eq!(
            addresses("127.0.0.1, 9.9.9.9,2606:4700:4700::1111"),
            vec![
                "127.0.0.1".parse::<IpAddr>().expect("v4"),
                "9.9.9.9".parse().expect("v4"),
                "2606:4700:4700::1111".parse().expect("v6"),
            ],
        );
        // A typo in a tuning knob is worth ignoring rather than refusing to
        // start over, and a variable holding nothing usable falls back to the
        // platform.
        assert_eq!(
            addresses("1.1.1.1,not-an-address"),
            vec!["1.1.1.1".parse::<IpAddr>().expect("v4")],
        );
        assert!(addresses("").is_empty());
        assert!(addresses("localhost").is_empty());
    }

    #[tokio::test]
    async fn one_resolver_and_therefore_one_cache() {
        // Clones share the built resolver rather than each building their own,
        // which is what makes the cache worth having.
        let (a, b) = (Resolver::shared(), Resolver::shared());
        assert!(a.get().is_some(), "the platform has no dns configuration");
        assert!(
            std::ptr::eq(a.get().expect("a"), b.get().expect("b")),
            "two resolvers, two caches"
        );
    }
}
