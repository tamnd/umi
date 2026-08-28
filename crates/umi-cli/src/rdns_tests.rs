//! Doc 07.1's check, against hand written packets and a loopback resolver.
//!
//! The wire format tests use bytes written from RFC 1035 rather than bytes
//! captured from our own encoder, because a parser checked against its own
//! writer agrees with itself whatever both of them do. The loopback resolver
//! is the other half: a real socket, a real query, a real answer, so the
//! encode and decode paths meet somewhere other than in a test helper.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::thread;
use std::time::Duration;

use super::{
    A, AAAA, Confirmation, DnsError, PTR, Resolver, arpa, decode, encode, read_name, under,
    write_name,
};

#[test]
fn a_query_is_the_bytes_rfc_1035_describes() {
    let query = encode(0x1234, "umi.dev", A).expect("the name encodes");
    assert_eq!(
        query,
        vec![
            // Header: id, recursion desired, one question, one additional.
            0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            // "umi.dev" as length prefixed labels, then the root.
            0x03, b'u', b'm', b'i', 0x03, b'd', b'e', b'v', 0x00, // A, IN.
            0x00, 0x01, 0x00, 0x01,
            // The OPT record: root name, type 41, 1232 bytes of receive
            // buffer where a class would be, no flags, no options.
            0x00, 0x00, 0x29, 0x04, 0xd0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
    );
}

#[test]
fn a_name_that_is_not_a_name_is_refused_before_the_socket() {
    assert!(encode(1, "", A).is_err());
    assert!(encode(1, "umi..dev", A).is_err());
    let long = "a".repeat(64);
    assert!(encode(1, &format!("{long}.dev"), A).is_err());
}

#[test]
fn the_reverse_name_is_the_address_backwards() {
    assert_eq!(
        arpa(IpAddr::V4(Ipv4Addr::new(62, 171, 131, 190))),
        "190.131.171.62.in-addr.arpa"
    );
    // The one reverse name everybody has seen.
    assert_eq!(
        arpa(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        "1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa"
    );
}

#[test]
fn a_name_reads_back_through_a_compression_pointer() {
    let mut buf = vec![0u8; 12];
    // "umi.dev" at offset 12.
    buf.extend_from_slice(&[0x03, b'u', b'm', b'i', 0x03, b'd', b'e', b'v', 0x00]);
    // "fetch-1" at offset 21, then a pointer back to offset 12.
    buf.extend_from_slice(&[0x07, b'f', b'e', b't', b'c', b'h', b'-', b'1', 0xc0, 0x0c]);

    let (name, after) = read_name(&buf, 21).expect("the name reads");
    assert_eq!(name, "fetch-1.umi.dev");
    // The offset is where the name ends in this record, not where the
    // pointer led.
    assert_eq!(after, 31);
}

#[test]
fn a_pointer_that_does_not_point_backwards_is_refused() {
    // A pointer to itself is the classic way to hang a parser. So is a
    // pointer forwards into a name that points back.
    let mut buf = vec![0u8; 12];
    buf.extend_from_slice(&[0xc0, 0x0c]);
    assert!(matches!(read_name(&buf, 12), Err(DnsError::Malformed(_))));

    let mut forward = vec![0u8; 12];
    forward.extend_from_slice(&[0xc0, 0x0e, 0xc0, 0x0c]);
    assert!(matches!(
        read_name(&forward, 12),
        Err(DnsError::Malformed(_))
    ));
}

#[test]
fn an_answer_to_another_question_is_refused() {
    let mut response = head(0x1111, 0x8180, 0, 0);
    response.extend_from_slice(&[]);
    assert_eq!(decode(&response, 0x2222, PTR), Err(DnsError::Mismatch));
}

#[test]
fn a_truncated_answer_is_refused_rather_than_half_read() {
    // Nothing we ask has an answer that needs TCP, so a set truncation bit
    // means something is wrong rather than something is big.
    let response = head(7, 0x8200, 0, 0);
    assert_eq!(decode(&response, 7, PTR), Err(DnsError::Truncated));
}

#[test]
fn no_such_name_is_told_apart_from_a_broken_lookup() {
    // An address with no PTR record is a fact about the address. It has to
    // reach the report as a different thing from a resolver that did not
    // answer, because one of them is a DNS zone to fix and the other is a
    // network to fix.
    let response = head(7, 0x8183, 0, 0);
    assert_eq!(decode(&response, 7, PTR), Err(DnsError::Rcode(3)));
}

#[test]
fn a_record_that_runs_off_the_end_is_refused() {
    let mut response = head(7, 0x8180, 1, 1);
    write_name(&mut response, "umi.dev").expect("the name encodes");
    response.extend_from_slice(&A.to_be_bytes());
    response.extend_from_slice(&1u16.to_be_bytes());
    // An answer that claims four bytes of address and supplies none.
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&A.to_be_bytes());
    response.extend_from_slice(&1u16.to_be_bytes());
    response.extend_from_slice(&300u32.to_be_bytes());
    response.extend_from_slice(&4u16.to_be_bytes());
    assert!(matches!(
        decode(&response, 7, A),
        Err(DnsError::Malformed(_))
    ));
}

#[test]
fn a_published_address_confirms_both_ways() {
    let addr = IpAddr::V4(Ipv4Addr::new(62, 171, 131, 190));
    let resolver = fake(vec![
        (arpa(addr), PTR, name_data("fetch-3.umi.dev")),
        ("fetch-3.umi.dev".to_owned(), A, vec![62, 171, 131, 190]),
    ]);

    assert_eq!(
        resolver.names(addr).expect("the ptr answers"),
        vec!["fetch-3.umi.dev".to_owned()]
    );
    assert_eq!(
        super::confirm(&resolver, addr, "umi.dev"),
        Confirmation::Confirmed("fetch-3.umi.dev".to_owned())
    );
}

#[test]
fn a_name_that_does_not_come_back_is_not_confirmed() {
    // The case an operator's tooling rejects, and the reason the check is
    // called forward confirmable rather than reverse lookup. Anybody can put
    // any name in their own PTR record.
    let addr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
    let resolver = fake(vec![
        (arpa(addr), PTR, name_data("fetch-9.umi.dev")),
        ("fetch-9.umi.dev".to_owned(), A, vec![62, 171, 131, 190]),
    ]);

    assert_eq!(
        super::confirm(&resolver, addr, "umi.dev"),
        Confirmation::NoReturn(
            "fetch-9.umi.dev".to_owned(),
            vec![IpAddr::V4(Ipv4Addr::new(62, 171, 131, 190))]
        )
    );
}

#[test]
fn a_providers_default_name_confirms_but_is_not_ours() {
    let addr = IpAddr::V4(Ipv4Addr::new(62, 171, 131, 190));
    let resolver = fake(vec![
        (arpa(addr), PTR, name_data("vmi3391933.contaboserver.net")),
        (
            "vmi3391933.contaboserver.net".to_owned(),
            A,
            vec![62, 171, 131, 190],
        ),
    ]);

    assert_eq!(
        super::confirm(&resolver, addr, "umi.dev"),
        Confirmation::Foreign("vmi3391933.contaboserver.net".to_owned())
    );
}

#[test]
fn an_address_with_no_reverse_record_says_so() {
    let addr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 4));
    let resolver = fake(Vec::new());
    assert_eq!(
        super::confirm(&resolver, addr, "umi.dev"),
        Confirmation::NoName
    );
}

#[test]
fn a_v6_address_confirms_the_same_way() {
    let addr: IpAddr = "2a02:c207:2339:1933::1".parse().expect("a v6 address");
    let IpAddr::V6(v6) = addr else {
        unreachable!("parsed as v6")
    };
    let resolver = fake(vec![
        (arpa(addr), PTR, name_data("fetch-3.umi.dev")),
        ("fetch-3.umi.dev".to_owned(), AAAA, v6.octets().to_vec()),
    ]);
    assert_eq!(
        super::confirm(&resolver, addr, "umi.dev"),
        Confirmation::Confirmed("fetch-3.umi.dev".to_owned())
    );
}

#[test]
fn under_matches_a_domain_and_its_children_and_nothing_else() {
    assert!(under("umi.dev", "umi.dev"));
    assert!(under("fetch-1.umi.dev", "umi.dev"));
    assert!(under("fetch-1.umi.dev.", "umi.dev"));
    assert!(under("FETCH-1.UMI.DEV", "umi.dev"));
    assert!(!under("notumi.dev", "umi.dev"));
    assert!(!under("umi.dev.example.com", "umi.dev"));
    assert!(!under("dev", "umi.dev"));
}

/// A twelve byte header with the counts a test wants.
fn head(id: u16, flags: u16, questions: u16, answers: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&questions.to_be_bytes());
    out.extend_from_slice(&answers.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out
}

/// A PTR record's data, which is a name in wire form.
fn name_data(name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    write_name(&mut out, name).expect("the name encodes");
    out
}

/// A resolver on loopback that answers from a fixed zone.
///
/// It answers with a compression pointer for the owner name, the way a real
/// server does, so the answer walking in [`decode`] gets exercised rather than
/// only the easy case.
fn fake(zone: Vec<(String, u16, Vec<u8>)>) -> Resolver {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("bind");
    let server = socket.local_addr().expect("the socket has an address");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("a read timeout");

    thread::spawn(move || {
        let mut buffer = [0u8; 1500];
        // Enough queries for any test here, and a timeout on the socket so
        // the thread ends even when a test asks fewer.
        for _ in 0..16 {
            let Ok((read, from)) = socket.recv_from(&mut buffer) else {
                return;
            };
            let query = &buffer[..read];
            let Ok((name, after)) = read_name(query, 12) else {
                return;
            };
            let qtype = u16::from_be_bytes([query[after], query[after + 1]]);
            let answers: Vec<&(String, u16, Vec<u8>)> = zone
                .iter()
                .filter(|(zname, ztype, _)| *zname == name && *ztype == qtype)
                .collect();

            let flags = if answers.is_empty() { 0x8183 } else { 0x8180 };
            let mut out = head(
                u16::from_be_bytes([query[0], query[1]]),
                flags,
                1,
                u16::try_from(answers.len()).unwrap_or(0),
            );
            out.extend_from_slice(&query[12..after + 4]);
            for (_, ztype, rdata) in answers {
                out.extend_from_slice(&[0xc0, 0x0c]);
                out.extend_from_slice(&ztype.to_be_bytes());
                out.extend_from_slice(&1u16.to_be_bytes());
                out.extend_from_slice(&300u32.to_be_bytes());
                out.extend_from_slice(&u16::try_from(rdata.len()).unwrap_or(0).to_be_bytes());
                out.extend_from_slice(rdata);
            }
            if socket.send_to(&out, from).is_err() {
                return;
            }
        }
    });

    Resolver::new(server)
}
