use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use openssl::bn::BigNumContext;
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::rsa::{Padding, Rsa};
use openssl::sign::{RsaPssSaltlen, Signer};

use crate::http::{self, HttpOptions};
use crate::json::{b64url_decode, b64url_encode, parse, Json};
use crate::jwt::{self, Alg, Policy};

fn rsa_key() -> PKey<Private> {
    PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap()
}

fn ec_key(nid: Nid) -> PKey<Private> {
    PKey::from_ec_key(EcKey::generate(&EcGroup::from_curve_name(nid).unwrap()).unwrap()).unwrap()
}

fn rsa_jwk(key: &PKey<Private>, kid: &str) -> String {
    let rsa = key.rsa().unwrap();
    format!(
        r#"{{"kty":"RSA","kid":"{kid}","use":"sig","n":"{}","e":"{}"}}"#,
        b64url_encode(&rsa.n().to_vec()),
        b64url_encode(&rsa.e().to_vec())
    )
}

fn ec_jwk(key: &PKey<Private>, kid: &str, crv: &str, len: usize) -> String {
    let ec = key.ec_key().unwrap();
    let mut ctx = BigNumContext::new().unwrap();
    let mut x = openssl::bn::BigNum::new().unwrap();
    let mut y = openssl::bn::BigNum::new().unwrap();
    ec.public_key().affine_coordinates(ec.group(), &mut x, &mut y, &mut ctx).unwrap();
    format!(
        r#"{{"kty":"EC","kid":"{kid}","crv":"{crv}","x":"{}","y":"{}"}}"#,
        b64url_encode(&x.to_vec_padded(len as i32).unwrap()),
        b64url_encode(&y.to_vec_padded(len as i32).unwrap())
    )
}

fn sign(key: &PKey<Private>, alg: Alg, kid: Option<&str>, claims: &str) -> String {
    let header = match kid {
        Some(k) => format!(r#"{{"alg":"{}","typ":"JWT","kid":"{k}"}}"#, alg.name()),
        None => format!(r#"{{"alg":"{}","typ":"JWT"}}"#, alg.name()),
    };
    let input = format!("{}.{}", b64url_encode(header.as_bytes()), b64url_encode(claims.as_bytes()));
    let digest = match alg {
        Alg::Rs256 | Alg::Ps256 | Alg::Es256 => MessageDigest::sha256(),
        Alg::Rs384 | Alg::Ps384 | Alg::Es384 => MessageDigest::sha384(),
        Alg::Rs512 | Alg::Ps512 => MessageDigest::sha512(),
    };
    let mut signer = Signer::new(digest, key).unwrap();
    if matches!(alg, Alg::Ps256 | Alg::Ps384 | Alg::Ps512) {
        signer.set_rsa_padding(Padding::PKCS1_PSS).unwrap();
        signer.set_rsa_pss_saltlen(RsaPssSaltlen::DIGEST_LENGTH).unwrap();
    }
    signer.update(input.as_bytes()).unwrap();
    let mut sig = signer.sign_to_vec().unwrap();
    if matches!(alg, Alg::Es256 | Alg::Es384) {
        let n = if alg == Alg::Es256 { 32 } else { 48 };
        let der = openssl::ecdsa::EcdsaSig::from_der(&sig).unwrap();
        sig = der.r().to_vec_padded(n).unwrap();
        sig.extend(der.s().to_vec_padded(n).unwrap());
    }
    format!("{input}.{}", b64url_encode(&sig))
}

fn keys_from(jwks: &str) -> Vec<jwt::Jwk> {
    jwt::parse_jwks(&format!(r#"{{"keys":[{jwks}]}}"#)).unwrap().0
}

fn verify_with(token: &str, jwks: &str) -> Result<bool, String> {
    let jws = jwt::parse_jws(token)?;
    let keys = keys_from(jwks);
    let cands = jwt::candidate_keys(&jws, &keys);
    if cands.is_empty() {
        return Err("no candidate key".into());
    }
    let mut ok = false;
    for k in cands {
        ok |= jwt::verify_signature(&jws, &k.key)?;
    }
    Ok(ok)
}

const CLAIMS: &str = r#"{"iss":"https://issuer.example","aud":"pg","sub":"alice","exp":4102444800,"scope":"openid postgres"}"#;

#[test]
fn base64url_roundtrip() {
    for n in 0..20 {
        let data: Vec<u8> = (0..n).map(|i| (i * 37 + 11) as u8).collect();
        assert_eq!(b64url_decode(&b64url_encode(&data)).unwrap(), data);
    }
    assert_eq!(b64url_decode("_-8=").unwrap(), vec![0xff, 0xef]);
    assert!(b64url_decode("a").is_err());
    assert!(b64url_decode("a*").is_err());
}

#[test]
fn json_parser() {
    let v = parse(r#"{"a":[1,2.5,-3e2,true,null,"x\u00e9\ud83d\ude00\n"],"b":{"c":"d"},"a":"last"}"#).unwrap();
    assert_eq!(v.get("a").unwrap().as_str(), Some("last"));
    assert_eq!(v.get("b").unwrap().get("c").unwrap().as_str(), Some("d"));
    let arr = parse(r#"[1,2.5,-3e2,true,null,"x\u00e9\ud83d\ude00\n"]"#).unwrap();
    let a = arr.as_arr().unwrap();
    assert_eq!(a[0].as_i64(), Some(1));
    assert_eq!(a[1].as_i64(), None);
    assert_eq!(a[2], Json::Num(-300.0));
    assert_eq!(a[3].as_bool(), Some(true));
    assert_eq!(a[4], Json::Null);
    assert_eq!(a[5].as_str(), Some("xé😀\n"));
    for bad in ["{", "[1,]", "{\"a\" 1}", "\"\\ud83d\"", "01x", "\"a\nb\"", "{} x"] {
        assert!(parse(bad).is_err(), "{bad}");
    }
    let deep = "[".repeat(100) + &"]".repeat(100);
    assert!(parse(&deep).is_err());
}

#[test]
fn rsa_and_ec_signatures_verify() {
    let rsa = rsa_key();
    let p256 = ec_key(Nid::X9_62_PRIME256V1);
    let p384 = ec_key(Nid::SECP384R1);
    let jwks = format!(
        "{},{},{}",
        rsa_jwk(&rsa, "r1"),
        ec_jwk(&p256, "e1", "P-256", 32),
        ec_jwk(&p384, "e2", "P-384", 48)
    );
    for alg in [Alg::Rs256, Alg::Rs384, Alg::Rs512, Alg::Ps256, Alg::Ps384, Alg::Ps512] {
        assert_eq!(verify_with(&sign(&rsa, alg, Some("r1"), CLAIMS), &jwks), Ok(true), "{}", alg.name());
    }
    assert_eq!(verify_with(&sign(&p256, Alg::Es256, Some("e1"), CLAIMS), &jwks), Ok(true));
    assert_eq!(verify_with(&sign(&p384, Alg::Es384, Some("e2"), CLAIMS), &jwks), Ok(true));
    // No kid: every key of the family is tried.
    assert_eq!(verify_with(&sign(&rsa, Alg::Rs256, None, CLAIMS), &jwks), Ok(true));
    assert_eq!(verify_with(&sign(&p256, Alg::Es256, None, CLAIMS), &jwks), Ok(true));
}

#[test]
fn signature_rejections() {
    let rsa = rsa_key();
    let other = rsa_key();
    let jwks = rsa_jwk(&rsa, "r1");
    // wrong key
    assert_eq!(verify_with(&sign(&other, Alg::Rs256, Some("r1"), CLAIMS), &jwks), Ok(false));
    // tampered payload
    let tok = sign(&rsa, Alg::Rs256, Some("r1"), CLAIMS);
    let mut parts: Vec<&str> = tok.split('.').collect();
    let forged = b64url_encode(CLAIMS.replace("alice", "mallory").as_bytes());
    parts[1] = &forged;
    assert_eq!(verify_with(&parts.join("."), &jwks), Ok(false));
    // unknown kid
    assert!(verify_with(&sign(&rsa, Alg::Rs256, Some("zz"), CLAIMS), &jwks).is_err());
    // EC token against RSA-only JWKS
    let p256 = ec_key(Nid::X9_62_PRIME256V1);
    assert!(verify_with(&sign(&p256, Alg::Es256, Some("r1"), CLAIMS), &jwks).is_err());
    // alg=none and HMAC are refused at parse time
    let none = format!("{}.{}.", b64url_encode(br#"{"alg":"none"}"#), b64url_encode(CLAIMS.as_bytes()));
    assert!(jwt::parse_jws(&none).unwrap_err().contains("unsupported JWS algorithm"));
    let hs = format!("{}.{}.AAAA", b64url_encode(br#"{"alg":"HS256"}"#), b64url_encode(CLAIMS.as_bytes()));
    assert!(jwt::parse_jws(&hs).unwrap_err().contains("unsupported JWS algorithm"));
    let crit = format!("{}.{}.AAAA", b64url_encode(br#"{"alg":"RS256","crit":["x"]}"#), b64url_encode(CLAIMS.as_bytes()));
    assert!(jwt::parse_jws(&crit).unwrap_err().contains("crit"));
    assert!(jwt::parse_jws("a.b").is_err());
    assert!(!jwt::looks_like_jws("opaque-token"));
    assert!(jwt::looks_like_jws(&tok));
}

fn policy<'a>(aud: &'a [String], scopes: &'a [&'a str]) -> Policy<'a> {
    Policy {
        issuer: "https://issuer.example",
        audiences: aud,
        required_scopes: scopes,
        identity_claim: "sub",
        clock_skew: 60,
        now: 1_700_000_000,
    }
}

#[test]
fn claim_policy() {
    let aud = vec!["pg".to_string(), "pg2".to_string()];
    let scopes = ["openid", "postgres"];
    let p = policy(&aud, &scopes);
    let ok = |c: &str| jwt::check_claims(&parse(c).unwrap(), &p, true);
    assert_eq!(ok(r#"{"iss":"https://issuer.example","aud":"pg","sub":"alice","exp":1700000100,"scope":"openid postgres extra"}"#), Ok("alice".into()));
    assert_eq!(ok(r#"{"iss":"https://issuer.example","aud":["x","pg2"],"sub":"bob","exp":1700000100,"nbf":1700000050,"iat":1699999999,"scp":["postgres","openid"]}"#), Ok("bob".into()));
    let err = |c: &str| ok(c).unwrap_err();
    assert!(err(r#"{"aud":"pg","sub":"a","exp":1700000100,"scope":"openid postgres"}"#).contains("no \"iss\""));
    assert!(err(r#"{"iss":"https://evil","aud":"pg","sub":"a","exp":1700000100,"scope":"openid postgres"}"#).contains("issuer"));
    assert!(err(r#"{"iss":"https://issuer.example","sub":"a","exp":1700000100,"scope":"openid postgres"}"#).contains("no \"aud\""));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"other","sub":"a","exp":1700000100,"scope":"openid postgres"}"#).contains("audience"));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","scope":"openid postgres"}"#).contains("no \"exp\""));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":1699999900,"scope":"openid postgres"}"#).contains("expired"));
    // inside skew: exp 30 s ago is still accepted
    assert!(ok(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":1699999970,"scope":"openid postgres"}"#).is_ok());
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":1700000100,"nbf":1700000200,"scope":"openid postgres"}"#).contains("not valid before"));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":1700000100,"iat":1700009999,"scope":"openid postgres"}"#).contains("future"));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":"soon","scope":"openid postgres"}"#).contains("not a number"));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":1700000100,"scope":"openid"}"#).contains("postgres"));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":1700000100}"#).contains("no \"scope\""));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","exp":1700000100,"scope":"openid postgres"}"#).contains("no \"sub\""));
    assert!(err(r#"{"iss":"https://issuer.example","aud":"pg","sub":"","exp":1700000100,"scope":"openid postgres"}"#).contains("no \"sub\""));

    // scopes not required
    let none: [&str; 0] = [];
    let p2 = policy(&aud, &none);
    assert_eq!(jwt::check_claims(&parse(r#"{"iss":"https://issuer.example","aud":"pg","sub":"a","exp":1700000100}"#).unwrap(), &p2, true), Ok("a".into()));

    // introspection: optional iss/aud/exp, username fallback
    let lax = |c: &str| jwt::check_claims(&parse(c).unwrap(), &p, false);
    assert_eq!(lax(r#"{"active":true,"username":"carol","scope":"openid postgres"}"#), Ok("carol".into()));
    assert_eq!(lax(r#"{"active":true,"sub":"s1","username":"carol","scope":"openid postgres"}"#), Ok("s1".into()));
    assert!(lax(r#"{"active":true,"scope":"openid postgres"}"#).is_err());
    assert!(lax(r#"{"active":true,"sub":"s1","exp":1,"scope":"openid postgres"}"#).unwrap_err().contains("expired"));
    assert!(lax(r#"{"active":true,"sub":"s1","aud":"nope","scope":"openid postgres"}"#).unwrap_err().contains("audience"));
    // A JSON null aud is absent, like null iss/exp: skipped in lax mode,
    // "no aud claim" in strict mode; other non-string types stay rejected.
    assert_eq!(lax(r#"{"active":true,"sub":"s1","aud":null,"scope":"openid postgres"}"#), Ok("s1".into()));
    assert!(err(r#"{"iss":"https://issuer.example","aud":null,"sub":"a","exp":1700000100,"scope":"openid postgres"}"#).contains("no \"aud\""));
    assert!(lax(r#"{"active":true,"sub":"s1","aud":42,"scope":"openid postgres"}"#).unwrap_err().contains("not a string or array"));
}

#[test]
fn http_malformed_chunked_is_an_error_not_a_panic() {
    // Chunk data without its trailing CRLF and no terminating 0-chunk.
    let (port, _h) = serve_once("200 OK", "Transfer-Encoding: chunked\r\n", b"5\r\nhello".to_vec());
    let url = http::parse_url(&format!("http://127.0.0.1:{port}/")).unwrap();
    let r = http::get(&url, &insecure());
    assert!(r.is_err(), "expected a clean error, got {r:?}");

    // A chunk size that overflows usize arithmetic.
    let (port, _h) = serve_once("200 OK", "Transfer-Encoding: chunked\r\n", b"ffffffffffffffff\r\nx".to_vec());
    let url = http::parse_url(&format!("http://127.0.0.1:{port}/")).unwrap();
    let r = http::get(&url, &insecure());
    assert!(r.is_err(), "expected a clean error, got {r:?}");

    // Bytes other than CRLF after the chunk data.
    let (port, _h) = serve_once("200 OK", "Transfer-Encoding: chunked\r\n", b"5\r\nhelloXX0\r\n\r\n".to_vec());
    let url = http::parse_url(&format!("http://127.0.0.1:{port}/")).unwrap();
    assert!(http::get(&url, &insecure()).is_err());
}

#[test]
fn issuer_forms() {
    assert_eq!(jwt::issuer_identifier("https://a.example"), "https://a.example");
    assert_eq!(jwt::issuer_identifier("https://a.example/"), "https://a.example");
    assert_eq!(jwt::issuer_identifier("https://a.example/.well-known/openid-configuration"), "https://a.example");
    assert_eq!(jwt::issuer_identifier("https://a.example/.well-known/oauth-authorization-server"), "https://a.example");
    assert_eq!(jwt::issuer_identifier("https://a.example/.well-known/oauth-authorization-server/alternate"), "https://a.example/alternate");
    assert_eq!(jwt::issuer_identifier("https://a.example/.well-known/openid-configuration/param"), "https://a.example/param");
    assert_eq!(jwt::discovery_url("https://a.example/"), "https://a.example/.well-known/openid-configuration");
    assert_eq!(jwt::discovery_url("https://a.example/.well-known/oauth-authorization-server/x"), "https://a.example/.well-known/oauth-authorization-server/x");
}

#[test]
fn jwks_skips_unusable_keys() {
    let rsa = rsa_key();
    let text = format!(
        r#"{{"keys":[{{"kty":"oct","k":"x"}},{{"kty":"RSA","use":"enc","n":"AQ","e":"AQ"}},{{"kty":"EC","crv":"secp256k1","x":"AQ","y":"AQ"}},{}]}}"#,
        rsa_jwk(&rsa, "good")
    );
    let (keys, skipped) = jwt::parse_jwks(&text).unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].kid.as_deref(), Some("good"));
    assert_eq!(skipped.len(), 3);
    assert!(jwt::parse_jwks(r#"{"nokeys":1}"#).is_err());
}

#[test]
fn url_parsing() {
    let u = http::parse_url("https://idp.example/.well-known/jwks.json?x=1#frag").unwrap();
    assert_eq!((u.https, u.host.as_str(), u.port, u.path.as_str()), (true, "idp.example", 443, "/.well-known/jwks.json?x=1"));
    let u = http::parse_url("http://[::1]:8080").unwrap();
    assert_eq!((u.https, u.host.as_str(), u.port, u.path.as_str()), (false, "::1", 8080, "/"));
    assert_eq!(http::parse_url("http://h:81/p").unwrap().origin(), "http://h:81");
    assert!(http::parse_url("ftp://x").is_err());
    assert!(http::parse_url("https://user:pw@x/").is_err());
    assert!(http::parse_url("https://:443/").is_err());
}

fn serve_once(status: &'static str, headers: &'static str, body: Vec<u8>) -> (u16, std::thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let h = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut req = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = s.read(&mut buf).unwrap();
            req.extend_from_slice(&buf[..n]);
            if let Some(i) = req.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&req[..i]).to_string();
                let want: usize = head
                    .lines()
                    .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
                    .unwrap_or(0);
                while req.len() < i + 4 + want {
                    let n = s.read(&mut buf).unwrap();
                    req.extend_from_slice(&buf[..n]);
                }
                break;
            }
        }
        s.write_all(format!("HTTP/1.1 {status}\r\n{headers}\r\n").as_bytes()).unwrap();
        s.write_all(&body).unwrap();
        req
    });
    (port, h)
}

fn insecure() -> HttpOptions {
    HttpOptions { allow_insecure_http: true, ca_file: None, timeout: Duration::from_secs(5) }
}

#[test]
fn http_get_content_length_and_chunked() {
    let (port, h) = serve_once("200 OK", "Content-Type: application/json\r\nContent-Length: 13\r\n", b"{\"keys\":[]}\r\n".to_vec());
    let url = http::parse_url(&format!("http://127.0.0.1:{port}/jwks")).unwrap();
    let r = http::get(&url, &insecure()).unwrap();
    assert_eq!((r.status, r.body.as_slice()), (200, &b"{\"keys\":[]}\r\n"[..]));
    let req = String::from_utf8(h.join().unwrap()).unwrap();
    assert!(req.starts_with("GET /jwks HTTP/1.1\r\n"));
    assert!(req.contains(&format!("Host: 127.0.0.1:{port}\r\n")));

    let (port, h) = serve_once("200 OK", "Transfer-Encoding: chunked\r\n", b"5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\n\r\n".to_vec());
    let url = http::parse_url(&format!("http://127.0.0.1:{port}/")).unwrap();
    let r = http::get(&url, &insecure()).unwrap();
    assert_eq!(r.body, b"hello world");
    h.join().unwrap();

    let (port, h) = serve_once("404 Not Found", "", b"gone".to_vec());
    let url = http::parse_url(&format!("http://127.0.0.1:{port}/")).unwrap();
    let r = http::get(&url, &insecure()).unwrap();
    assert_eq!((r.status, r.body.as_slice()), (404, &b"gone"[..]));
    h.join().unwrap();
}

#[test]
fn http_post_form_carries_basic_auth() {
    let (port, h) = serve_once("200 OK", "Content-Length: 15\r\n", b"{\"active\":true}".to_vec());
    let url = http::parse_url(&format!("http://127.0.0.1:{port}/introspect")).unwrap();
    let r = http::post_form(&url, "token=abc", Some("Y2lkOnNlYw=="), &insecure()).unwrap();
    assert_eq!(r.status, 200);
    let req = String::from_utf8(h.join().unwrap()).unwrap();
    assert!(req.starts_with("POST /introspect HTTP/1.1\r\n"));
    assert!(req.contains("Authorization: Basic Y2lkOnNlYw==\r\n"));
    assert!(req.contains("Content-Type: application/x-www-form-urlencoded\r\n"));
    assert!(req.ends_with("\r\n\r\ntoken=abc"));
}

#[test]
fn http_refuses_plaintext_by_default() {
    let url = http::parse_url("http://127.0.0.1:9/").unwrap();
    let opts = HttpOptions { allow_insecure_http: false, ca_file: None, timeout: Duration::from_secs(1) };
    assert!(http::get(&url, &opts).unwrap_err().contains("allow_insecure_http"));
}
