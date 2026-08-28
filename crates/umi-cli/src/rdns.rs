//! Forward confirmable reverse DNS, doc 07.1.
//!
//! The check every site operator already knows how to run. Take the address a
//! request came from, ask what name it reverses to, then ask what that name
//! resolves to, and see the address come back. It is how Googlebot and
//! Bingbot are told apart from something claiming to be them, and it works
//! because the two halves are controlled by different parties: anyone can put
//! `googlebot.com` in a PTR record, but only the owner of the address can put
//! a PTR record there at all.
//!
//! Doc 07.2's signatures are the better mechanism and this is the one with the
//! installed base, so we do both. What is in this module is our own side of it:
//! the box asks the question an operator would ask about itself, so that a
//! misconfigured PTR record is found by us on a Tuesday rather than by a site
//! owner in the middle of a crawl.
//!
//! There is no resolver crate behind this. A PTR query and an address query
//! are a few dozen bytes each and the answer parsing is a hundred lines, most
//! of it the name compression in RFC 1035 section 4.1.4, which is less code
//! than the configuration surface of a general resolver and does not put a
//! second async DNS stack in a binary that already has one inside reqwest.
//!
//! The transaction id is not a security boundary here and it is worth saying
//! why. The socket is connected to the resolver we chose, so an off path
//! forger has to guess the source port as well as the id, and the worst a
//! forged answer can do is put a wrong line in a report that a person reads.
//! Nothing in the crawl path acts on what this module returns.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The resolver every question in this module goes to. See
/// [`Resolver::public`] for why it is not the one on the box.
pub const PUBLIC: &str = "1.1.1.1:53";

/// Where to point a socket to find out which IPv4 address this box sends
/// from. Nothing is sent to it, so it only has to be an address the routing
/// table has an opinion about.
pub const V4_PROBE: &str = "1.1.1.1:53";

/// The same for IPv6.
pub const V6_PROBE: &str = "[2606:4700:4700::1111]:53";

/// `PTR`, RFC 1035.
const PTR: u16 = 12;
/// `A`, RFC 1035.
const A: u16 = 1;
/// `AAAA`, RFC 3596.
const AAAA: u16 = 28;
/// `OPT`, RFC 6891.
const OPT: u16 = 41;
/// The internet class, which is the only one anything still uses.
const IN: u16 = 1;
/// What we tell the resolver we can receive in one datagram, RFC 6891. Large
/// enough that a PTR answer never comes back truncated, small enough to clear
/// the path MTU everywhere.
const UDP_PAYLOAD: u16 = 1232;

/// What went wrong, at the level a report line can use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsError {
    /// The question could not be encoded, which means the name was not a name.
    Question(&'static str),
    /// The socket would not send, receive or bind.
    Socket(String),
    /// The answer did not fit in a datagram, and we do not fall back to TCP
    /// because nothing we ask has an answer that large.
    Truncated,
    /// The resolver answered with an RCODE. 3 is the interesting one: no such
    /// name, which for a PTR question means no reverse record exists.
    Rcode(u8),
    /// The answer is not a well formed message.
    Malformed(&'static str),
    /// The answer is to a different question than the one we asked.
    Mismatch,
}

impl fmt::Display for DnsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Question(why) => write!(f, "{why}"),
            Self::Socket(cause) => write!(f, "{cause}"),
            Self::Truncated => f.write_str("the answer was truncated"),
            Self::Rcode(3) => f.write_str("no such name"),
            Self::Rcode(code) => write!(f, "the resolver answered rcode {code}"),
            Self::Malformed(why) => write!(f, "{why}"),
            Self::Mismatch => f.write_str("the answer is to another question"),
        }
    }
}

/// One answer record, of the three kinds this module asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Record {
    /// A `PTR` target.
    Name(String),
    /// An `A` or `AAAA` address.
    Addr(IpAddr),
}

/// Somewhere to send a question.
#[derive(Debug, Clone)]
pub struct Resolver {
    server: SocketAddr,
    timeout: Duration,
}

impl Resolver {
    /// Ask this server, with a three second patience.
    #[must_use]
    pub fn new(server: SocketAddr) -> Self {
        Self {
            server,
            timeout: Duration::from_secs(3),
        }
    }

    /// Ask [`PUBLIC`], which is the resolver to use for this question.
    ///
    /// Not the one in `/etc/resolv.conf`, and the reason is a real answer from
    /// a real box. Contabo puts `127.0.1.1 vmi3112167.contaboserver.net` in
    /// `/etc/hosts`, the local stub resolver answers from it, and the forward
    /// half of the check comes back as loopback. The box would have failed its
    /// own check while every site operator on the internet saw it pass. What
    /// doc 07.1 is asking is what somebody else sees, and somebody else is not
    /// using our `resolv.conf`.
    #[must_use]
    pub fn public() -> Self {
        Self::new(PUBLIC.parse().expect("the constant is a socket address"))
    }

    /// Which server the answers came from, for a report line that has to say.
    #[must_use]
    pub fn server(&self) -> SocketAddr {
        self.server
    }

    /// The names an address reverses to, lowercased and without the trailing
    /// dot.
    ///
    /// # Errors
    ///
    /// [`DnsError`], including `Rcode(3)` when the address has no PTR record
    /// at all, which is a fact about the address rather than a failure of the
    /// lookup.
    pub fn names(&self, addr: IpAddr) -> Result<Vec<String>, DnsError> {
        let question = arpa(addr);
        Ok(self
            .ask(&question, PTR)?
            .into_iter()
            .filter_map(|record| match record {
                Record::Name(name) => Some(name),
                Record::Addr(_) => None,
            })
            .collect())
    }

    /// The addresses a name resolves to, both families.
    ///
    /// A name with no AAAA record is the normal case rather than a failure, so
    /// a `Rcode(3)` on the second question is folded into whatever the first
    /// one found.
    ///
    /// # Errors
    ///
    /// [`DnsError`] when neither question could be answered.
    pub fn addresses(&self, name: &str) -> Result<Vec<IpAddr>, DnsError> {
        let v4 = self.ask(name, A);
        let v6 = self.ask(name, AAAA);
        let mut out = Vec::new();
        for found in [&v4, &v6].into_iter().flatten() {
            for record in found {
                if let Record::Addr(addr) = record {
                    out.push(*addr);
                }
            }
        }
        if out.is_empty()
            && let (Err(cause), Err(_)) = (v4, v6)
        {
            return Err(cause);
        }
        Ok(out)
    }

    /// One question, one datagram, one answer.
    fn ask(&self, qname: &str, qtype: u16) -> Result<Vec<Record>, DnsError> {
        let id = transaction_id();
        let query = encode(id, qname, qtype)?;
        let bind: SocketAddr = if self.server.is_ipv4() {
            (Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            (Ipv6Addr::UNSPECIFIED, 0).into()
        };
        let socket = UdpSocket::bind(bind).map_err(|e| DnsError::Socket(e.to_string()))?;
        socket
            .set_read_timeout(Some(self.timeout))
            .map_err(|e| DnsError::Socket(e.to_string()))?;
        socket
            .connect(self.server)
            .map_err(|e| DnsError::Socket(e.to_string()))?;
        socket
            .send(&query)
            .map_err(|e| DnsError::Socket(e.to_string()))?;
        let mut buffer = [0u8; UDP_PAYLOAD as usize];
        let read = socket
            .recv(&mut buffer)
            .map_err(|e| DnsError::Socket(e.to_string()))?;
        decode(&buffer[..read], id, qtype)
    }
}

/// How a box came out of the check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmation {
    /// The address reverses to a name under the domain we expected, and that
    /// name resolves back to the address. This is the only good answer.
    Confirmed(String),
    /// It reverses and it comes back, but the name is somebody else's. True
    /// of most addresses on the internet, including a hosting provider's
    /// default.
    Foreign(String),
    /// It reverses to a name that does not resolve back to it, which is the
    /// case an operator's verification tooling rejects.
    NoReturn(String, Vec<IpAddr>),
    /// There is no PTR record.
    NoName,
    /// The question could not be asked.
    Failed(DnsError),
}

/// Run doc 07.1's check on one address.
///
/// `domain` is the suffix a confirmed name has to sit under, with no leading
/// dot. A name matches when it is the domain or ends in a dot and the domain,
/// so `umi.dev` and `fetch-1.umi.dev` both match `umi.dev` and
/// `notumi.dev` does not.
#[must_use]
pub fn confirm(resolver: &Resolver, addr: IpAddr, domain: &str) -> Confirmation {
    let names = match resolver.names(addr) {
        Ok(names) => names,
        // No such name on a reverse question is not a lookup that failed, it
        // is an address with no PTR record, which is the thing doc 07.1 asks
        // an operator to fix.
        Err(DnsError::Rcode(3)) => return Confirmation::NoName,
        Err(cause) => return Confirmation::Failed(cause),
    };
    let Some(first) = names.first().cloned() else {
        return Confirmation::NoName;
    };
    let mut found = Vec::new();
    for name in names {
        let back = match resolver.addresses(&name) {
            Ok(back) => back,
            Err(cause) => return Confirmation::Failed(cause),
        };
        if back.contains(&addr) {
            return if under(&name, domain) {
                Confirmation::Confirmed(name)
            } else {
                Confirmation::Foreign(name)
            };
        }
        found.extend(back);
    }
    Confirmation::NoReturn(first, found)
}

/// Is `name` the domain or a name under it.
#[must_use]
pub fn under(name: &str, domain: &str) -> bool {
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    let domain = domain.trim_end_matches('.').to_ascii_lowercase();
    name == domain || name.ends_with(&format!(".{domain}"))
}

/// The source address the kernel would use to reach `target`.
///
/// A connected UDP socket sends nothing, so this asks the routing table rather
/// than the network, and it answers the question that matters: which address a
/// site sees us arrive from. Enumerating interfaces would need libc, which
/// this workspace does not call, and would also answer the wrong question on a
/// box with more than one address.
#[must_use]
pub fn source_address(target: &str) -> Option<IpAddr> {
    let target: SocketAddr = target.parse().ok()?;
    let bind: SocketAddr = if target.is_ipv4() {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    } else {
        (Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = UdpSocket::bind(bind).ok()?;
    socket.connect(target).ok()?;
    let local = socket.local_addr().ok()?.ip();
    let loopback = match local {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_unspecified(),
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified(),
    };
    if loopback { None } else { Some(local) }
}

/// The reverse zone name for an address: octets backwards under
/// `in-addr.arpa`, nibbles backwards under `ip6.arpa`.
#[must_use]
pub fn arpa(addr: IpAddr) -> String {
    match addr {
        IpAddr::V4(v4) => {
            let [a, b, c, d] = v4.octets();
            format!("{d}.{c}.{b}.{a}.in-addr.arpa")
        }
        IpAddr::V6(v6) => {
            let mut out = String::with_capacity(72);
            for byte in v6.octets().iter().rev() {
                out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
                out.push('.');
                out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
                out.push('.');
            }
            out.push_str("ip6.arpa");
            out
        }
    }
}

/// A query for one name and one type, with an EDNS0 record on the end so the
/// answer arrives whole.
fn encode(id: u16, qname: &str, qtype: u16) -> Result<Vec<u8>, DnsError> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&id.to_be_bytes());
    // Recursion desired, and nothing else.
    out.extend_from_slice(&0x0100u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    write_name(&mut out, qname)?;
    out.extend_from_slice(&qtype.to_be_bytes());
    out.extend_from_slice(&IN.to_be_bytes());
    // The OPT record: root name, type, our receive size in place of the class,
    // an all zero extended rcode and version, no options.
    out.push(0);
    out.extend_from_slice(&OPT.to_be_bytes());
    out.extend_from_slice(&UDP_PAYLOAD.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    Ok(out)
}

/// Length prefixed labels, then a root label.
fn write_name(out: &mut Vec<u8>, name: &str) -> Result<(), DnsError> {
    let name = name.trim_end_matches('.');
    if name.is_empty() {
        return Err(DnsError::Question("the name is empty"));
    }
    if name.len() > 253 {
        return Err(DnsError::Question("the name is too long"));
    }
    for label in name.split('.') {
        if label.is_empty() {
            return Err(DnsError::Question("the name has an empty label"));
        }
        let len = u8::try_from(label.len())
            .map_err(|_| DnsError::Question("the name has a label over 63 bytes"))?;
        if len > 63 {
            return Err(DnsError::Question("the name has a label over 63 bytes"));
        }
        out.push(len);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    Ok(())
}

/// Pull the answers of the type we asked about out of a response.
fn decode(response: &[u8], id: u16, qtype: u16) -> Result<Vec<Record>, DnsError> {
    if response.len() < 12 {
        return Err(DnsError::Malformed("the header is short"));
    }
    if u16::from_be_bytes([response[0], response[1]]) != id {
        return Err(DnsError::Mismatch);
    }
    let flags = u16::from_be_bytes([response[2], response[3]]);
    if flags & 0x8000 == 0 {
        return Err(DnsError::Malformed("the answer is not a response"));
    }
    if flags & 0x0200 != 0 {
        return Err(DnsError::Truncated);
    }
    let rcode = u8::try_from(flags & 0x000f).unwrap_or(0);
    if rcode != 0 {
        return Err(DnsError::Rcode(rcode));
    }
    let questions = u16::from_be_bytes([response[4], response[5]]);
    let answers = u16::from_be_bytes([response[6], response[7]]);

    let mut pos = 12;
    for _ in 0..questions {
        let (_, next) = read_name(response, pos)?;
        pos = next
            .checked_add(4)
            .ok_or(DnsError::Malformed("the question runs off the end"))?;
    }

    let mut out = Vec::new();
    for _ in 0..answers {
        let (_, next) = read_name(response, pos)?;
        pos = next;
        let head = response
            .get(pos..pos + 10)
            .ok_or(DnsError::Malformed("a record header runs off the end"))?;
        let rtype = u16::from_be_bytes([head[0], head[1]]);
        let class = u16::from_be_bytes([head[2], head[3]]);
        let rdlen = usize::from(u16::from_be_bytes([head[8], head[9]]));
        pos += 10;
        let rdata = response
            .get(pos..pos + rdlen)
            .ok_or(DnsError::Malformed("record data runs off the end"))?;
        if class == IN && rtype == qtype {
            match (rtype, rdlen) {
                (PTR, _) => out.push(Record::Name(read_name(response, pos)?.0)),
                (A, 4) => out.push(Record::Addr(IpAddr::V4(Ipv4Addr::new(
                    rdata[0], rdata[1], rdata[2], rdata[3],
                )))),
                (AAAA, 16) => {
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(rdata);
                    out.push(Record::Addr(IpAddr::V6(Ipv6Addr::from(octets))));
                }
                _ => {}
            }
        }
        pos += rdlen;
    }
    Ok(out)
}

/// A name out of a message, following the compression pointers.
///
/// The returned offset is where the name ends in the stream the caller is
/// walking, which is the byte after the first pointer rather than the byte
/// after wherever it led. A pointer has to point strictly backwards, which is
/// what RFC 1035 section 4.1.4 says and is also what makes a loop impossible:
/// every jump strictly decreases the offset, so the walk terminates.
fn read_name(buf: &[u8], start: usize) -> Result<(String, usize), DnsError> {
    let mut name = String::new();
    let mut pos = start;
    let mut after = None;
    loop {
        let len = usize::from(
            *buf.get(pos)
                .ok_or(DnsError::Malformed("a name runs off the end"))?,
        );
        if len & 0xc0 == 0xc0 {
            let low = usize::from(
                *buf.get(pos + 1)
                    .ok_or(DnsError::Malformed("a pointer runs off the end"))?,
            );
            let target = ((len & 0x3f) << 8) | low;
            if target >= pos {
                return Err(DnsError::Malformed("a name points at itself or forwards"));
            }
            if after.is_none() {
                after = Some(pos + 2);
            }
            pos = target;
            continue;
        }
        if len & 0xc0 != 0 {
            return Err(DnsError::Malformed("a label length is reserved"));
        }
        if len == 0 {
            if after.is_none() {
                after = Some(pos + 1);
            }
            break;
        }
        let label = buf
            .get(pos + 1..pos + 1 + len)
            .ok_or(DnsError::Malformed("a label runs off the end"))?;
        let text =
            std::str::from_utf8(label).map_err(|_| DnsError::Malformed("a label is not utf8"))?;
        if !name.is_empty() {
            name.push('.');
        }
        name.push_str(text);
        if name.len() > 255 {
            return Err(DnsError::Malformed("a name is too long"));
        }
        pos += 1 + len;
    }
    Ok((name.to_ascii_lowercase(), after.unwrap_or(pos)))
}

/// Something the resolver has not just answered, cheaply.
///
/// Not a random number and it does not have to be. See the module header for
/// what this does and does not defend against.
fn transaction_id() -> u16 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.subsec_nanos());
    let pid = std::process::id();
    u16::try_from((nanos ^ pid.wrapping_mul(2_654_435_761)) & 0xffff).unwrap_or(0)
}

#[cfg(test)]
#[path = "rdns_tests.rs"]
mod tests;
