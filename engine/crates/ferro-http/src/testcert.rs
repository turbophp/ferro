//! **Test scaffolding only** (feature `test-certs`, or this crate's own tests): X.509 certificates
//! generated IN-TEST, so the Ferro HTTP TLS suites (M6-F5a) need no committed key material and can
//! build exactly the certificate each fate cell needs (an expired one, one for the wrong name, one
//! from an unknown CA).
//!
//! `rcgen` is not in the lock and D20 does not list it, so this is a deliberately small DER writer:
//! ECDSA P-256 keys and ECDSA-with-SHA256 signatures from `ring` — the crypto backend D20 already
//! adopts — over a fixed v3 certificate shape (subject CN, validity, SAN, basic constraints, key
//! usage, extended key usage). It is not a general X.509 library and never runs in a daemon.
//!
//! **Trusting it.** A malformed encoding would make every TLS test fail for the wrong reason, so the
//! suites always pair a refusal with a POSITIVE control built by the same code (a good leaf from the
//! same CA is accepted), and `tls`'s unit tests assert the exact `rustls` error each variant
//! produces (`UnknownIssuer`, `NotValidForName`, `Expired`), not merely that verification failed.

use std::net::IpAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

const OID_ECDSA_SHA256: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x02];
const OID_EC_PUBLIC_KEY: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x02, 0x01];
const OID_P256: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x03, 0x01, 0x07];
const OID_CN: &[u8] = &[0x55, 0x04, 0x03];
const OID_BASIC_CONSTRAINTS: &[u8] = &[0x55, 0x1D, 0x13];
const OID_KEY_USAGE: &[u8] = &[0x55, 0x1D, 0x0F];
const OID_EXT_KEY_USAGE: &[u8] = &[0x55, 0x1D, 0x25];
const OID_SAN: &[u8] = &[0x55, 0x1D, 0x11];
const OID_SERVER_AUTH: &[u8] = &[0x2B, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01];
const OID_CLIENT_AUTH: &[u8] = &[0x2B, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x02];

fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let n = content.len();
    if n < 0x80 {
        out.push(n as u8);
    } else {
        let bytes = n.to_be_bytes();
        let skip = bytes.iter().take_while(|b| **b == 0).count();
        out.push(0x80 | (bytes.len() - skip) as u8);
        out.extend_from_slice(&bytes[skip..]);
    }
    out.extend_from_slice(content);
    out
}

fn seq(parts: &[Vec<u8>]) -> Vec<u8> {
    tlv(0x30, &parts.concat())
}

fn oid(o: &[u8]) -> Vec<u8> {
    tlv(0x06, o)
}

fn uint(v: u64) -> Vec<u8> {
    let bytes = v.to_be_bytes();
    let skip = bytes
        .iter()
        .take_while(|b| **b == 0)
        .count()
        .min(bytes.len() - 1);
    let mut c = bytes[skip..].to_vec();
    if c[0] & 0x80 != 0 {
        c.insert(0, 0);
    }
    tlv(0x02, &c)
}

fn bit_string(content: &[u8], unused: u8) -> Vec<u8> {
    let mut c = vec![unused];
    c.extend_from_slice(content);
    tlv(0x03, &c)
}

fn name(cn: &str) -> Vec<u8> {
    seq(&[tlv(0x31, &seq(&[oid(OID_CN), tlv(0x0C, cn.as_bytes())]))])
}

/// Days since 1970-01-01 → (year, month, day) (Howard Hinnant's `civil_from_days`).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn time(t: SystemTime) -> Vec<u8> {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64;
    let (y, mo, d) = civil(secs.div_euclid(86_400));
    let rem = secs.rem_euclid(86_400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    if (1950..2050).contains(&y) {
        tlv(
            0x17,
            format!("{:02}{mo:02}{d:02}{h:02}{mi:02}{s:02}Z", y % 100).as_bytes(),
        )
    } else {
        tlv(
            0x18,
            format!("{y:04}{mo:02}{d:02}{h:02}{mi:02}{s:02}Z").as_bytes(),
        )
    }
}

fn extension(id: &[u8], critical: bool, value: Vec<u8>) -> Vec<u8> {
    let mut parts = vec![oid(id)];
    if critical {
        parts.push(vec![0x01, 0x01, 0xFF]);
    }
    parts.push(tlv(0x04, &value));
    seq(&parts)
}

/// What a certificate is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Usage {
    /// A CA (basic constraints `cA`, `keyCertSign`).
    Ca,
    /// A TLS server leaf (`serverAuth`).
    Server,
    /// A TLS client leaf (`clientAuth`), for mTLS.
    Client,
}

/// One generated key pair and its certificate.
pub struct Issued {
    pkcs8: Vec<u8>,
    der: Vec<u8>,
    subject: Vec<u8>,
}

impl Issued {
    pub fn cert(&self) -> CertificateDer<'static> {
        CertificateDer::from(self.der.clone())
    }

    pub fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.pkcs8.clone()))
    }

    /// The certificate as PEM (`CA_FILE`, `CLIENT_CERT_FILE`).
    pub fn cert_pem(&self) -> String {
        pem("CERTIFICATE", &self.der)
    }

    /// The private key as PKCS#8 PEM (`CLIENT_KEY_FILE`).
    pub fn key_pem(&self) -> String {
        pem("PRIVATE KEY", &self.pkcs8)
    }
}

/// What to issue.
#[derive(Clone, Debug)]
pub struct Spec {
    pub cn: String,
    pub dns: Vec<String>,
    pub ips: Vec<IpAddr>,
    pub usage: Usage,
    pub not_before: SystemTime,
    pub not_after: SystemTime,
    /// An X.509 **v1** certificate: no version field and no extensions (`openssl x509 -req`'s
    /// default shape for a self-signed root). `webpki` refuses v1 as an end-entity certificate;
    /// servers that accept it as a chain or root certificate exist (M6-F5c review F2).
    pub v1: bool,
}

impl Spec {
    /// Valid from a day ago for a year.
    pub fn new(cn: &str, usage: Usage) -> Spec {
        let now = SystemTime::now();
        Spec {
            cn: cn.into(),
            dns: Vec::new(),
            ips: Vec::new(),
            usage,
            not_before: now - Duration::from_secs(86_400),
            not_after: now + Duration::from_secs(365 * 86_400),
            v1: false,
        }
    }

    pub fn dns(mut self, name: &str) -> Spec {
        self.dns.push(name.into());
        self
    }

    pub fn ip(mut self, ip: IpAddr) -> Spec {
        self.ips.push(ip);
        self
    }

    /// An X.509 v1 certificate (see [`Spec::v1`]'s field).
    pub fn v1(mut self) -> Spec {
        self.v1 = true;
        self
    }

    /// Expired: valid from ten days ago until yesterday.
    pub fn expired(mut self) -> Spec {
        let now = SystemTime::now();
        self.not_before = now - Duration::from_secs(10 * 86_400);
        self.not_after = now - Duration::from_secs(86_400);
        self
    }
}

static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn issue(spec: &Spec, issuer: Option<&Issued>) -> Issued {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
        .expect("generate a P-256 key")
        .as_ref()
        .to_vec();
    let kp = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &pkcs8, &rng)
        .expect("load the key just generated");
    let subject = name(&spec.cn);
    let (issuer_name, signer_pkcs8) = match issuer {
        Some(i) => (i.subject.clone(), i.pkcs8.clone()),
        None => (subject.clone(), pkcs8.clone()),
    };
    let sig_alg = seq(&[oid(OID_ECDSA_SHA256)]);
    let spki = seq(&[
        seq(&[oid(OID_EC_PUBLIC_KEY), oid(OID_P256)]),
        bit_string(kp.public_key().as_ref(), 0),
    ]);
    let mut exts = Vec::new();
    match spec.usage {
        Usage::Ca => {
            exts.push(extension(
                OID_BASIC_CONSTRAINTS,
                true,
                seq(&[vec![0x01, 0x01, 0xFF]]),
            ));
            // keyCertSign (bit 5) + cRLSign (bit 6).
            exts.push(extension(OID_KEY_USAGE, true, bit_string(&[0x06], 1)));
        }
        Usage::Server | Usage::Client => {
            // digitalSignature (bit 0).
            exts.push(extension(OID_KEY_USAGE, true, bit_string(&[0x80], 7)));
            let eku = if spec.usage == Usage::Server {
                OID_SERVER_AUTH
            } else {
                OID_CLIENT_AUTH
            };
            exts.push(extension(OID_EXT_KEY_USAGE, false, seq(&[oid(eku)])));
        }
    }
    if !spec.dns.is_empty() || !spec.ips.is_empty() {
        let mut names = Vec::new();
        for d in &spec.dns {
            names.push(tlv(0x82, d.as_bytes()));
        }
        for ip in &spec.ips {
            names.push(match ip {
                IpAddr::V4(a) => tlv(0x87, &a.octets()),
                IpAddr::V6(a) => tlv(0x87, &a.octets()),
            });
        }
        exts.push(extension(OID_SAN, false, seq(&names)));
    }
    let serial = uint(SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1000);
    let validity = seq(&[time(spec.not_before), time(spec.not_after)]);
    let tbs = if spec.v1 {
        // v1: the version field is absent (DEFAULT v1) and v1 has no extensions.
        seq(&[
            serial,
            sig_alg.clone(),
            issuer_name,
            validity,
            subject.clone(),
            spki,
        ])
    } else {
        seq(&[
            tlv(0xA0, &uint(2)),
            serial,
            sig_alg.clone(),
            issuer_name,
            validity,
            subject.clone(),
            spki,
            tlv(0xA3, &seq(&exts)),
        ])
    };
    let signer = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &signer_pkcs8, &rng)
        .expect("load the issuer key");
    let sig = signer.sign(&rng, &tbs).expect("sign the certificate");
    let der = seq(&[tbs, sig_alg, bit_string(sig.as_ref(), 0)]);
    Issued {
        pkcs8,
        der,
        subject,
    }
}

/// A self-signed CA.
pub fn ca(cn: &str) -> Issued {
    issue(&Spec::new(cn, Usage::Ca), None)
}

/// A self-signed X.509 **v1** root (no extensions), as `openssl x509 -req -signkey` makes one.
pub fn ca_v1(cn: &str) -> Issued {
    issue(&Spec::new(cn, Usage::Ca).v1(), None)
}

impl Issued {
    /// Issue `spec` signed by this (CA) certificate's key.
    pub fn sign(&self, spec: &Spec) -> Issued {
        issue(spec, Some(self))
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64(data: &[u8]) -> String {
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(B64[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn pem(label: &str, der: &[u8]) -> String {
    let b = base64(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in b.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap_or_default());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn civil_dates() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(19_723), (2024, 1, 1));
        assert_eq!(civil(20_513), (2026, 3, 1));
    }

    #[test]
    fn pem_round_trips_through_pki_types() {
        use rustls_pki_types::pem::PemObject;
        let c = ca("Round Trip CA");
        let back = CertificateDer::from_pem_slice(c.cert_pem().as_bytes()).unwrap();
        assert_eq!(back.as_ref(), c.cert().as_ref());
        let k = PrivateKeyDer::from_pem_slice(c.key_pem().as_bytes()).unwrap();
        assert_eq!(k.secret_der(), c.key().secret_der());
    }
}
