//! The published address list, doc 07.1.
//!
//! Every address umi crawls from, in a file a site operator can fetch once a
//! day and match against. It exists because the alternative for an operator
//! who wants to allow or rate limit us is a reverse lookup on every request,
//! and a list is what their tooling already knows how to eat.
//!
//! The shape is Google's `googlebot.json` on purpose, down to the field names.
//! An operator who has already written something that reads Google's file, or
//! who uses one of the many tools that do, should not have to write a second
//! thing to read ours. The one addition is a `name` on each entry, which
//! carries the reverse DNS name that address answers with, and a JSON reader
//! that does not expect it ignores it.
//!
//! The file is compiled into the binary rather than fetched. A box checking
//! whether it is a published crawler has to be able to answer that with no
//! network, and a published list that a running crawler could be talked out of
//! by a DNS answer would be a strange thing to build.

use std::fmt;
use std::net::IpAddr;

use serde::Deserialize;

/// The published file, as it ships.
pub const PUBLISHED: &str = include_str!("../../../identity/umi.json");

/// Where it is served, and what goes on the bot page.
pub const URL: &str = "https://umi.dev/bot/umi.json";

/// The domain every crawling address reverses into.
pub const DOMAIN: &str = "umi.dev";

/// One published range and the name it answers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The range, which today is always a single address.
    pub prefix: Prefix,
    /// The reverse DNS name, when the entry names one.
    pub name: Option<String>,
}

/// An address and how many of its leading bits are the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Prefix {
    addr: IpAddr,
    bits: u8,
}

impl Prefix {
    /// Parse `address/bits`.
    ///
    /// # Errors
    ///
    /// A sentence naming what was wrong with it, for a report line.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (addr, bits) = text
            .split_once('/')
            .ok_or_else(|| format!("{text} has no prefix length"))?;
        let addr: IpAddr = addr
            .parse()
            .map_err(|_| format!("{addr} is not an address"))?;
        let bits: u8 = bits
            .parse()
            .map_err(|_| format!("{bits} is not a prefix length"))?;
        let width = if addr.is_ipv4() { 32 } else { 128 };
        if bits > width {
            return Err(format!("/{bits} is too long for {addr}"));
        }
        Ok(Self { addr, bits })
    }

    /// Is this address inside the range.
    #[must_use]
    pub fn contains(&self, addr: IpAddr) -> bool {
        match (self.addr, addr) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => leading(&net.octets(), &ip.octets(), self.bits),
            (IpAddr::V6(net), IpAddr::V6(ip)) => leading(&net.octets(), &ip.octets(), self.bits),
            _ => false,
        }
    }
}

impl fmt::Display for Prefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.bits)
    }
}

/// Do two addresses agree on their first `bits` bits.
fn leading(net: &[u8], addr: &[u8], bits: u8) -> bool {
    let whole = usize::from(bits / 8);
    let rest = bits % 8;
    if net[..whole] != addr[..whole] {
        return false;
    }
    if rest == 0 {
        return true;
    }
    let mask = 0xffu8 << (8 - rest);
    net[whole] & mask == addr[whole] & mask
}

/// The parsed list.
#[derive(Debug, Clone)]
pub struct Published {
    /// When the list was last changed, in the format Google's file uses.
    pub creation_time: String,
    /// Every range, in the order the file lists them.
    pub entries: Vec<Entry>,
}

impl Published {
    /// Parse the list that shipped with this binary.
    ///
    /// # Errors
    ///
    /// A sentence naming what was wrong with the file. A test in this module
    /// checks that the shipped one parses, so this can only fire on a build
    /// somebody has edited.
    pub fn shipped() -> Result<Self, String> {
        Self::parse(PUBLISHED)
    }

    /// Parse a list.
    ///
    /// # Errors
    ///
    /// A sentence naming what was wrong with the text.
    pub fn parse(text: &str) -> Result<Self, String> {
        let raw: RawFile = serde_json::from_str(text).map_err(|cause| cause.to_string())?;
        let mut entries = Vec::with_capacity(raw.prefixes.len());
        for entry in raw.prefixes {
            let text = entry
                .ipv4_prefix
                .or(entry.ipv6_prefix)
                .ok_or_else(|| "an entry names neither an ipv4 nor an ipv6 prefix".to_owned())?;
            entries.push(Entry {
                prefix: Prefix::parse(&text)?,
                name: entry.name,
            });
        }
        Ok(Self {
            creation_time: raw.creation_time,
            entries,
        })
    }

    /// The entry covering an address, if the list has one.
    #[must_use]
    pub fn find(&self, addr: IpAddr) -> Option<&Entry> {
        self.entries
            .iter()
            .find(|entry| entry.prefix.contains(addr))
    }
}

/// The file as JSON, before the prefixes are parsed.
#[derive(Deserialize)]
struct RawFile {
    #[serde(rename = "creationTime")]
    creation_time: String,
    prefixes: Vec<RawEntry>,
}

/// One entry as JSON. Google's file uses one key or the other and never both,
/// and a reader that insisted on that would break on a file that is otherwise
/// fine, so this takes whichever is there.
#[derive(Deserialize)]
struct RawEntry {
    #[serde(rename = "ipv4Prefix")]
    ipv4_prefix: Option<String>,
    #[serde(rename = "ipv6Prefix")]
    ipv6_prefix: Option<String>,
    name: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use super::{DOMAIN, Prefix, Published};

    #[test]
    fn the_shipped_list_parses_and_names_every_range() {
        let published = Published::shipped().expect("the shipped list parses");
        assert!(
            published.entries.len() >= 3,
            "the fleet is three boxes, so the list cannot be shorter than that"
        );
        for entry in &published.entries {
            let name = entry.name.as_deref().unwrap_or_default();
            assert!(
                crate::rdns::under(name, DOMAIN),
                "{} is published as {name}, which is not under {DOMAIN}",
                entry.prefix
            );
        }
    }

    #[test]
    fn a_published_address_is_found_and_a_neighbour_is_not() {
        let published = Published::shipped().expect("the shipped list parses");
        let ours: IpAddr = "62.171.131.190".parse().expect("an address");
        let theirs: IpAddr = "62.171.131.191".parse().expect("an address");
        assert_eq!(
            published.find(ours).and_then(|entry| entry.name.as_deref()),
            Some("fetch-3.umi.dev")
        );
        assert!(published.find(theirs).is_none());
    }

    #[test]
    fn a_prefix_covers_its_own_addresses_and_stops() {
        let net = Prefix::parse("192.0.2.0/24").expect("a prefix");
        assert!(net.contains("192.0.2.0".parse().expect("an address")));
        assert!(net.contains("192.0.2.255".parse().expect("an address")));
        assert!(!net.contains("192.0.3.0".parse().expect("an address")));
        // A v4 address is not inside a v6 prefix however it is written.
        let six = Prefix::parse("2a02:c207::/32").expect("a prefix");
        assert!(six.contains("2a02:c207:2339:1933::1".parse().expect("an address")));
        assert!(!six.contains("2a02:c208::1".parse().expect("an address")));
        assert!(!six.contains("192.0.2.1".parse().expect("an address")));
    }

    #[test]
    fn a_prefix_that_is_not_one_is_refused() {
        assert!(Prefix::parse("192.0.2.0").is_err());
        assert!(Prefix::parse("192.0.2.0/33").is_err());
        assert!(Prefix::parse("not an address/24").is_err());
        assert!(Prefix::parse("2a02:c207::/129").is_err());
    }

    #[test]
    fn an_odd_prefix_length_masks_the_partial_byte() {
        let net = Prefix::parse("192.178.5.0/27").expect("a prefix");
        assert!(net.contains("192.178.5.31".parse().expect("an address")));
        assert!(!net.contains("192.178.5.32".parse().expect("an address")));
    }
}
