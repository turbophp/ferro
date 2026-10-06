//! The address guard's classification (SPEC §23.8.5): which IP addresses an upstream may reach.
//!
//! This is the pure half of the guard. Slice F4 owns resolution and *pinning* (the TCP connect
//! goes to exactly the checked `SocketAddr`); F3 owns the decision, so it can be tested in
//! isolation and applied to an IP-literal `ORIGIN` at configuration time.
//!
//! Classification first refuses the IPv6 rows of the always-refused table, then unwraps the
//! IPv4 address an IPv6 address carries (IPv4-mapped, NAT64 `64:ff9b::/96` and `64:ff9b:1::/48`,
//! and every `FERRO_HTTP_NAT64_PREFIXES` entry), then classifies. Refusing the IPv6 rows *before*
//! unwrapping means an operator-declared NAT64 prefix can never widen an always-refused range
//! (a prefix declared inside `fe80::/10` still classifies as link-local).
//!
//! `64:ff9b:1::/48` (RFC 8215, local-use) may carry its IPv4 address in any RFC 6052 layout whose
//! prefix length is at least 48, and the engine cannot see which one the network uses. Every
//! candidate (layouts /48, /56, /64 and /96) is classified, and an address is admitted only if
//! every candidate is (SPEC §22.2 (cw)).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// The classes `ADDRESS_CLASSES` admits (§23.8.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AddressClass {
    Public,
    Private,
    Loopback,
}

impl AddressClass {
    /// The `ADDRESS_CLASSES` token, and the `range` label of `ferro_http_address_skipped_total`.
    pub fn label(self) -> &'static str {
        match self {
            AddressClass::Public => "public",
            AddressClass::Private => "private",
            AddressClass::Loopback => "loopback",
        }
    }
}

/// The rows of §23.8.5's always-refused table. Rows marked *M* there are [`is_metadata`] and are
/// the only ones `ALLOW_METADATA=1` admits.
///
/// [`is_metadata`]: RefusedRange::is_metadata
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RefusedRange {
    /// `0.0.0.0/8`, `::`.
    Unspecified,
    /// `224.0.0.0/4`, `ff00::/8`, `255.255.255.255`, `240.0.0.0/4`.
    MulticastReserved,
    /// `169.254.0.0/16`, `fe80::/10` (*M*).
    LinkLocal,
    /// `fd00:ec2::/32` (*M*).
    AwsImdsV6,
    /// `fd20:ce::254/128` (*M*): GCP's IPv6 metadata server — documented by GCP, not verified
    /// here. It is inside ULA, which `ADDRESS_CLASSES=private` would otherwise admit (review F-9).
    GcpImdsV6,
    /// `100.100.100.200/32` (*M*).
    AlibabaMetadata,
    /// `168.63.129.16/32` (*M*).
    AzureWireServer,
    /// `192.0.0.0/24` (*M*).
    IetfSpecial,
    /// `::/96` other than `::` and `::1`, `2002::/16`, `2001::/32`, `fec0::/10`.
    DeprecatedV6,
}

impl RefusedRange {
    pub const ALL: [RefusedRange; 9] = [
        RefusedRange::Unspecified,
        RefusedRange::MulticastReserved,
        RefusedRange::LinkLocal,
        RefusedRange::AwsImdsV6,
        RefusedRange::GcpImdsV6,
        RefusedRange::AlibabaMetadata,
        RefusedRange::AzureWireServer,
        RefusedRange::IetfSpecial,
        RefusedRange::DeprecatedV6,
    ];

    /// Whether `ALLOW_METADATA=1` admits this row.
    pub fn is_metadata(self) -> bool {
        matches!(
            self,
            RefusedRange::LinkLocal
                | RefusedRange::AwsImdsV6
                | RefusedRange::GcpImdsV6
                | RefusedRange::AlibabaMetadata
                | RefusedRange::AzureWireServer
                | RefusedRange::IetfSpecial
        )
    }

    /// The closed `range` label vocabulary of `ferro_http_address_skipped_total` (§23.10.2).
    pub fn label(self) -> &'static str {
        match self {
            RefusedRange::Unspecified => "unspecified",
            RefusedRange::MulticastReserved => "multicast_reserved",
            RefusedRange::LinkLocal => "link_local",
            RefusedRange::AwsImdsV6 => "aws_imds_v6",
            RefusedRange::GcpImdsV6 => "gcp_imds_v6",
            RefusedRange::AlibabaMetadata => "alibaba_metadata",
            RefusedRange::AzureWireServer => "azure_wireserver",
            RefusedRange::IetfSpecial => "ietf_special",
            RefusedRange::DeprecatedV6 => "deprecated_v6",
        }
    }
}

/// What one address (or one embedded IPv4 candidate) is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Classification {
    Refused(RefusedRange),
    Class(AddressClass),
}

/// The operator's extra NAT64 `/96` prefixes (`FERRO_HTTP_NAT64_PREFIXES`), as their first 12
/// bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Nat64Prefixes(Vec<[u8; 12]>);

impl Nat64Prefixes {
    pub fn new(prefixes: Vec<[u8; 12]>) -> Self {
        Nat64Prefixes(prefixes)
    }

    /// Parse one `FERRO_HTTP_NAT64_PREFIXES` entry: an IPv6 address, `/96`, with its low 32 bits
    /// zero. Any other length is refused, because §23.3.1 decodes these entries "like
    /// `64:ff9b::/96`" and a different layout would silently classify the wrong 32 bits.
    pub fn parse_entry(s: &str) -> Option<[u8; 12]> {
        let (addr, len) = s.split_once('/')?;
        if len != "96" {
            return None;
        }
        let a: Ipv6Addr = addr.parse().ok()?;
        let o = a.octets();
        if o[12..] != [0, 0, 0, 0] {
            return None;
        }
        let mut p = [0u8; 12];
        p.copy_from_slice(&o[..12]);
        Some(p)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn v4_in(a: Ipv4Addr, base: [u8; 4], len: u32) -> bool {
    let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
    (u32::from(a) & mask) == (u32::from(Ipv4Addr::from(base)) & mask)
}

fn v6_in(a: &Ipv6Addr, base: Ipv6Addr, len: u32) -> bool {
    let mask = if len == 0 {
        0
    } else {
        u128::MAX << (128 - len)
    };
    (u128::from(*a) & mask) == (u128::from(base) & mask)
}

/// Classify an IPv4 address.
pub fn classify_v4(a: Ipv4Addr) -> Classification {
    use AddressClass::*;
    use RefusedRange::*;
    let refused: [([u8; 4], u32, RefusedRange); 8] = [
        ([0, 0, 0, 0], 8, Unspecified),
        ([224, 0, 0, 0], 4, MulticastReserved),
        ([240, 0, 0, 0], 4, MulticastReserved), // includes 255.255.255.255
        ([169, 254, 0, 0], 16, LinkLocal),
        ([100, 100, 100, 200], 32, AlibabaMetadata),
        ([168, 63, 129, 16], 32, AzureWireServer),
        ([192, 0, 0, 0], 24, IetfSpecial),
        ([255, 255, 255, 255], 32, MulticastReserved), // inside 240/4; listed as the table lists it
    ];
    for (base, len, row) in refused {
        if v4_in(a, base, len) {
            return Classification::Refused(row);
        }
    }
    if v4_in(a, [127, 0, 0, 0], 8) {
        return Classification::Class(Loopback);
    }
    let private: [([u8; 4], u32); 5] = [
        ([10, 0, 0, 0], 8),
        ([172, 16, 0, 0], 12),
        ([192, 168, 0, 0], 16),
        ([100, 64, 0, 0], 10),
        ([198, 18, 0, 0], 15),
    ];
    for (base, len) in private {
        if v4_in(a, base, len) {
            return Classification::Class(Private);
        }
    }
    Classification::Class(Public)
}

fn v4_from(bytes: [u8; 4]) -> Ipv4Addr {
    Ipv4Addr::from(bytes)
}

/// The IPv4 addresses `a` may carry, or `None` when it is not an embedding §23.8.5 unwraps.
pub fn embedded_v4(a: &Ipv6Addr, nat64: &Nat64Prefixes) -> Option<Vec<Ipv4Addr>> {
    let o = a.octets();
    let last = [o[12], o[13], o[14], o[15]];
    // IPv4-mapped ::ffff:0:0/96.
    if o[..10] == [0; 10] && o[10] == 0xff && o[11] == 0xff {
        return Some(vec![v4_from(last)]);
    }
    // SIIT IPv4-translated ::ffff:0:0:0/96 (RFC 2765, deprecated): it carries an IPv4 address the
    // same way, and a translator that still honours it reaches that address (review F-7).
    if o[..8] == [0; 8] && o[8] == 0xff && o[9] == 0xff && o[10] == 0 && o[11] == 0 {
        return Some(vec![v4_from(last)]);
    }
    // NAT64 well-known prefix 64:ff9b::/96.
    if v6_in(a, Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 0), 96) {
        return Some(vec![v4_from(last)]);
    }
    // Operator-declared /96 prefixes.
    if nat64.0.iter().any(|p| o[..12] == p[..]) {
        return Some(vec![v4_from(last)]);
    }
    // Local-use 64:ff9b:1::/48: every RFC 6052 layout with a prefix length >= 48 (byte 8 is the
    // `u` octet, skipped by the /48, /56 and /64 layouts).
    if v6_in(a, Ipv6Addr::new(0x64, 0xff9b, 1, 0, 0, 0, 0, 0), 48) {
        return Some(vec![
            v4_from([o[6], o[7], o[9], o[10]]),   // /48
            v4_from([o[7], o[9], o[10], o[11]]),  // /56
            v4_from([o[9], o[10], o[11], o[12]]), // /64
            v4_from(last),                        // /96
        ]);
    }
    None
}

/// Classify an address into one classification per candidate. An IPv4 address, or an IPv6
/// address that carries none, has exactly one; a `64:ff9b:1::/48` address has four.
pub fn classify(addr: IpAddr, nat64: &Nat64Prefixes) -> Vec<Classification> {
    use AddressClass::*;
    use RefusedRange::*;
    let a = match addr {
        IpAddr::V4(v4) => return vec![classify_v4(v4)],
        IpAddr::V6(a) => a,
    };
    if a == Ipv6Addr::UNSPECIFIED {
        return vec![Classification::Refused(Unspecified)];
    }
    if a == Ipv6Addr::LOCALHOST {
        return vec![Classification::Class(Loopback)];
    }
    // The IPv6 rows of the always-refused table come FIRST (see the module docs).
    let refused: [(Ipv6Addr, u32, RefusedRange); 8] = [
        (Ipv6Addr::UNSPECIFIED, 96, DeprecatedV6), // IPv4-compatible
        (
            Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0),
            8,
            MulticastReserved,
        ),
        (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10, LinkLocal),
        (
            Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0),
            32,
            AwsImdsV6,
        ),
        (
            Ipv6Addr::new(0xfd20, 0xce, 0, 0, 0, 0, 0, 0x254),
            128,
            GcpImdsV6,
        ),
        (Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16, DeprecatedV6), // 6to4
        (Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 32, DeprecatedV6), // Teredo
        (Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0), 10, DeprecatedV6), // site-local
    ];
    for (base, len, row) in refused {
        if v6_in(&a, base, len) {
            return vec![Classification::Refused(row)];
        }
    }
    if let Some(v4s) = embedded_v4(&a, nat64) {
        return v4s.into_iter().map(classify_v4).collect();
    }
    if v6_in(&a, Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7) {
        return vec![Classification::Class(Private)];
    }
    vec![Classification::Class(Public)]
}

/// The set of classes an upstream admits (`ADDRESS_CLASSES`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassSet {
    pub public: bool,
    pub private: bool,
    pub loopback: bool,
}

impl ClassSet {
    pub const PUBLIC_ONLY: ClassSet = ClassSet {
        public: true,
        private: false,
        loopback: false,
    };

    pub fn contains(self, c: AddressClass) -> bool {
        match c {
            AddressClass::Public => self.public,
            AddressClass::Private => self.private,
            AddressClass::Loopback => self.loopback,
        }
    }
}

/// Why an address was refused: an always-refused row, or a class the upstream does not admit.
/// Its [`label`](AddressRefusal::label) is the metrics `range` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressRefusal {
    Range(RefusedRange),
    Class(AddressClass),
}

impl AddressRefusal {
    pub fn label(self) -> &'static str {
        match self {
            AddressRefusal::Range(r) => r.label(),
            AddressRefusal::Class(c) => c.label(),
        }
    }
}

/// One upstream's address policy: `ADDRESS_CLASSES`, `ALLOW_METADATA`, and the IP literal of an
/// IP-literal `ORIGIN`, which it implicitly admits (unless that literal is always refused).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddressPolicy {
    pub classes: ClassSet,
    pub allow_metadata: bool,
    pub literal: Option<IpAddr>,
}

impl AddressPolicy {
    /// Admit or refuse one address. Every candidate must be admitted.
    ///
    /// `ALLOW_METADATA=1` admits the *M* rows outright, whatever `ADDRESS_CLASSES` says: the key
    /// "permits the link-local and cloud-metadata ranges for this upstream", and those rows sit
    /// across classes (Azure's WireServer is a public address, Alibaba's is inside CGNAT).
    pub fn check(&self, addr: IpAddr, nat64: &Nat64Prefixes) -> Result<(), AddressRefusal> {
        let is_literal = self.literal == Some(addr);
        for c in classify(addr, nat64) {
            match c {
                Classification::Refused(r) if r.is_metadata() && self.allow_metadata => {}
                Classification::Refused(r) => return Err(AddressRefusal::Range(r)),
                Classification::Class(_) if is_literal => {}
                Classification::Class(cl) if self.classes.contains(cl) => {}
                Classification::Class(cl) => return Err(AddressRefusal::Class(cl)),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(s: &str) -> Vec<Classification> {
        classify(s.parse().unwrap(), &Nat64Prefixes::default())
    }

    use AddressClass::*;
    use Classification::{Class as K, Refused as R};
    use RefusedRange::*;

    #[test]
    fn every_row_of_the_always_refused_table() {
        let cases: &[(&str, RefusedRange)] = &[
            ("0.0.0.0", Unspecified),
            ("0.255.1.2", Unspecified),
            ("::", Unspecified),
            ("224.0.0.1", MulticastReserved),
            ("239.255.255.255", MulticastReserved),
            ("ff02::1", MulticastReserved),
            ("255.255.255.255", MulticastReserved),
            ("240.0.0.1", MulticastReserved),
            ("169.254.169.254", LinkLocal),
            ("169.254.170.2", LinkLocal),
            ("169.254.0.23", LinkLocal),
            ("fe80::1", LinkLocal),
            ("febf::1", LinkLocal),
            ("fd00:ec2::254", AwsImdsV6),
            ("fd00:ec2::23", AwsImdsV6),
            ("100.100.100.200", AlibabaMetadata),
            ("168.63.129.16", AzureWireServer),
            ("192.0.0.192", IetfSpecial),
            ("192.0.0.0", IetfSpecial),
            ("::7f00:1", DeprecatedV6),
            ("::a9fe:a9fe", DeprecatedV6),
            ("2002:a9fe:a9fe::1", DeprecatedV6),
            ("2001:0:4136:e378:8000:63bf:3fff:fdd2", DeprecatedV6),
            ("fec0::1", DeprecatedV6),
        ];
        for (a, row) in cases {
            assert_eq!(c(a), vec![R(*row)], "{a}");
        }
    }

    #[test]
    fn classes() {
        let cases: &[(&str, AddressClass)] = &[
            ("127.0.0.1", Loopback),
            ("127.255.255.254", Loopback),
            ("::1", Loopback),
            ("10.1.2.3", Private),
            ("172.16.0.1", Private),
            ("172.31.255.255", Private),
            ("192.168.1.1", Private),
            ("100.64.0.1", Private),
            ("100.127.255.255", Private),
            ("198.18.0.1", Private),
            ("198.19.255.255", Private),
            ("fc00::1", Private),
            ("fd12:3456::1", Private),
            ("fd00:ec3::1", Private),
            // Controls just outside each range.
            ("172.15.255.255", Public),
            ("172.32.0.0", Public),
            ("100.63.255.255", Public),
            ("100.128.0.0", Public),
            ("198.17.255.255", Public),
            ("198.20.0.0", Public),
            ("169.253.255.255", Public),
            ("169.255.0.0", Public),
            ("192.0.1.0", Public),
            ("100.100.100.199", Private), // inside CGNAT 100.64/10
            ("168.63.129.17", Public),
            ("223.255.255.255", Public),
            ("1.1.1.1", Public),
            ("2606:4700::1111", Public),
            ("2001:1::1", Public),
            ("2001:db8::1", Public),
            ("fe00::1", Public),
        ];
        for (a, cl) in cases {
            assert_eq!(c(a), vec![K(*cl)], "{a}");
        }
    }

    #[test]
    fn embeddings_unwrap_to_their_ipv4() {
        assert_eq!(c("::ffff:127.0.0.1"), vec![K(Loopback)]);
        assert_eq!(c("::ffff:169.254.169.254"), vec![R(LinkLocal)]);
        assert_eq!(c("64:ff9b::a9fe:a9fe"), vec![R(LinkLocal)]);
        assert_eq!(c("64:ff9b::808:808"), vec![K(Public)]);
        assert_eq!(c("64:ff9b::a00:1"), vec![K(Private)]);
        // 64:ff9b:1::/96 layout carrying 169.254.169.254: one of the four candidates is refused.
        assert!(c("64:ff9b:1::a9fe:a9fe").contains(&R(LinkLocal)));
        // /48 layout: 169.254 in bytes 6-7, 169.254 in bytes 9-10.
        assert!(c("64:ff9b:1:a9fe:a9:fe00::").contains(&R(LinkLocal)));
        // An operator prefix.
        let p = Nat64Prefixes::new(vec![
            Nat64Prefixes::parse_entry("2001:db8:64::/96").unwrap(),
        ]);
        assert_eq!(
            classify("2001:db8:64::7f00:1".parse().unwrap(), &p),
            vec![K(Loopback)]
        );
        assert_eq!(
            classify(
                "2001:db8:64::7f00:1".parse().unwrap(),
                &Nat64Prefixes::default()
            ),
            vec![K(Public)],
            "control: the same address without the declaration is plain public IPv6"
        );
    }

    #[test]
    fn a_declared_prefix_cannot_widen_a_refused_row() {
        let p = Nat64Prefixes::new(vec![Nat64Prefixes::parse_entry("fe80::/96").unwrap()]);
        assert_eq!(
            classify("fe80::808:808".parse().unwrap(), &p),
            vec![R(LinkLocal)]
        );
    }

    #[test]
    fn nat64_prefix_entries_are_strict() {
        assert!(Nat64Prefixes::parse_entry("2001:db8::/96").is_some());
        for bad in [
            "2001:db8::",
            "2001:db8::/64",
            "2001:db8::/097",
            "2001:db8::1/96",
            "garbage/96",
            "10.0.0.0/96",
        ] {
            assert!(Nat64Prefixes::parse_entry(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn policy_admission() {
        let none = Nat64Prefixes::default();
        let p = AddressPolicy {
            classes: ClassSet::PUBLIC_ONLY,
            allow_metadata: false,
            literal: None,
        };
        assert_eq!(p.check("1.1.1.1".parse().unwrap(), &none), Ok(()));
        assert_eq!(
            p.check("127.0.0.1".parse().unwrap(), &none),
            Err(AddressRefusal::Class(Loopback))
        );
        assert_eq!(
            p.check("169.254.169.254".parse().unwrap(), &none),
            Err(AddressRefusal::Range(LinkLocal))
        );
        let meta = AddressPolicy {
            allow_metadata: true,
            ..p.clone()
        };
        assert_eq!(
            meta.check("169.254.169.254".parse().unwrap(), &none),
            Ok(())
        );
        assert_eq!(
            meta.check("0.0.0.0".parse().unwrap(), &none),
            Err(AddressRefusal::Range(Unspecified)),
            "ALLOW_METADATA never admits a non-M row"
        );
        let lit = AddressPolicy {
            literal: Some("10.0.0.5".parse().unwrap()),
            ..p.clone()
        };
        assert_eq!(lit.check("10.0.0.5".parse().unwrap(), &none), Ok(()));
        assert_eq!(
            lit.check("10.0.0.6".parse().unwrap(), &none),
            Err(AddressRefusal::Class(Private)),
            "the literal admits only itself"
        );
        let bad_lit = AddressPolicy {
            literal: Some("169.254.169.254".parse().unwrap()),
            ..p
        };
        assert_eq!(
            bad_lit.check("169.254.169.254".parse().unwrap(), &none),
            Err(AddressRefusal::Range(LinkLocal)),
            "a literal in the always-refused table is not admitted by being the literal"
        );
    }
}
