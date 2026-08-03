// Client-side auth ladder: trust, cleartext password, md5, SCRAM-SHA-256
// (no channel binding), then ParameterStatus/BackendKeyData collection up to
// ReadyForQuery.
//
// Every byte parsed here arrives from the SERVER, which dblink/postgres_fdw/
// walreceiver may point at an arbitrary host. A truncated or malformed
// message must therefore surface as a connection error, never a panic: all
// reads below are bounds-checked before slicing.
use types_error::PgResult;

use crate::{be_i32, msg, parse_error_fields, PgConn};

/// Authentication request code from an 'R' message body. Errors (instead of
/// panicking) when the server sends fewer than the 4 header bytes.
fn auth_req_code(mbody: &[u8]) -> Result<i32, String> {
    if mbody.len() < 4 {
        return Err("received malformed authentication request from server".into());
    }
    Ok(be_i32(&mbody[0..4]))
}

/// MD5 salt: the 4 bytes following the request code. A short message is a
/// protocol violation, not a crash.
fn md5_salt(mbody: &[u8]) -> Result<&[u8], String> {
    mbody
        .get(4..8)
        .ok_or_else(|| "received malformed MD5 authentication request from server".into())
}

/// SASL mechanism list: NUL-terminated names after the request code, closed
/// by an empty string (a final extra NUL). Rejects a list whose terminator —
/// of either kind — is missing, rather than silently reading to the buffer
/// edge (or past it).
fn sasl_mechanisms(mbody: &[u8]) -> Result<Vec<String>, String> {
    let malformed = || "received malformed SASL mechanism list from server".to_string();
    let list = mbody.get(4..).ok_or_else(malformed)?;
    let mut mechs = Vec::new();
    let mut p = 0;
    loop {
        match list.get(p) {
            None => return Err(malformed()),
            Some(0) => return Ok(mechs),
            Some(_) => {
                let end = list[p..].iter().position(|&c| c == 0).ok_or_else(malformed)? + p;
                mechs.push(String::from_utf8_lossy(&list[p..end]).into_owned());
                p = end + 1;
            }
        }
    }
}

pub(crate) fn handshake(
    conn: &mut PgConn,
    user: &str,
    password: Option<&str>,
) -> PgResult<Result<(), String>> {
    loop {
        let (t, mbody) = match conn.read_message(conn.we.connect)? {
            Ok(m) => m,
            Err(e) => return Ok(Err(e)),
        };
        match t {
            b'R' => {
                let authtype = match auth_req_code(&mbody) {
                    Ok(v) => v,
                    Err(e) => return Ok(Err(e)),
                };
                match authtype {
                    0 => {}
                    3 => {
                        conn.used_password = true;
                        let Some(pw) = password else {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        };
                        let mut b = pw.as_bytes().to_vec();
                        b.push(0);
                        if let Err(e) = conn.send_all(&msg(b'p', &b)) {
                            return Ok(Err(e));
                        }
                    }
                    5 => {
                        conn.used_password = true;
                        let Some(pw) = password else {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        };
                        let salt = match md5_salt(&mbody) {
                            Ok(v) => v,
                            Err(e) => return Ok(Err(e)),
                        };
                        let stage1 = pg_md5::pg_md5_encrypt(pw.as_bytes(), user.as_bytes());
                        // stage1 is a fixed-size local digest ("md5" + hex),
                        // not server bytes; the [3..] skip of the prefix is
                        // always in bounds.
                        let hex = &stage1[3..];
                        let stage2 = pg_md5::pg_md5_encrypt(hex, salt);
                        let mut b = stage2.to_vec();
                        b.push(0);
                        if let Err(e) = conn.send_all(&msg(b'p', &b)) {
                            return Ok(Err(e));
                        }
                    }
                    10 => {
                        conn.used_password = true;
                        let Some(pw) = password else {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        };
                        let mechs = match sasl_mechanisms(&mbody) {
                            Ok(v) => v,
                            Err(e) => return Ok(Err(e)),
                        };
                        if !mechs.iter().any(|m| m == scram_common::SCRAM_SHA_256_NAME) {
                            return Ok(Err(format!(
                                "none of the server's SASL authentication mechanisms are supported (offered: {})",
                                mechs.join(", ")
                            )));
                        }
                        if let Err(e) = scram_exchange(conn, pw)? {
                            return Ok(Err(e));
                        }
                    }
                    other => {
                        return Ok(Err(format!("authentication method {other} not supported")))
                    }
                }
            }
            b'S' | b'K' | b'N' | b'A' => conn.note_async(t, &mbody),
            b'E' => return Ok(Err(parse_error_fields(&mbody))),
            b'Z' => {
                conn.txn_status = mbody.first().copied().unwrap_or(b'I');
                return Ok(Ok(()));
            }
            other => {
                return Ok(Err(format!(
                    "unexpected message type \"{}\" during connection startup",
                    other as char
                )))
            }
        }
    }
}

fn b64(data: &[u8]) -> String {
    let mut dst = vec![0u8; pg_b64::pg_b64_enc_len(data.len() as i32) as usize];
    let dstlen = dst.len() as i32;
    let n = pg_b64::pg_b64_encode(data, data.len() as i32, &mut dst, dstlen);
    assert!(n >= 0, "base64 encode failed");
    String::from_utf8_lossy(&dst[..n as usize]).into_owned()
}

fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut dst = vec![0u8; pg_b64::pg_b64_dec_len(s.len() as i32) as usize];
    let dstlen = dst.len() as i32;
    let n = pg_b64::pg_b64_decode(s.as_bytes(), s.len() as i32, &mut dst, dstlen);
    if n < 0 {
        return Err("malformed base64 in SCRAM message".into());
    }
    dst.truncate(n as usize);
    Ok(dst)
}

fn scram_attr<'a>(fields: &'a [&'a str], name: char) -> Result<&'a str, String> {
    fields
        .iter()
        .find(|f| f.starts_with(name) && f.as_bytes().get(1) == Some(&b'='))
        .and_then(|f| f.get(2..))
        .ok_or_else(|| format!("malformed SCRAM message (missing \"{name}\" attribute)"))
}

/// Parsed, validated server-first SCRAM message.
struct ServerFirst {
    server_nonce: String,
    salt: Vec<u8>,
    iterations: i32,
}

/// Parse and validate the server-first-message payload (the bytes after the
/// SASLContinue request code). `client_nonce` is ours: the server's nonce
/// must strictly extend it — an echo with no server contribution is rejected.
fn parse_server_first(payload: &[u8], client_nonce: &str) -> Result<ServerFirst, String> {
    let server_first = String::from_utf8_lossy(payload).into_owned();
    let fields: Vec<&str> = server_first.split(',').collect();
    let server_nonce = scram_attr(&fields, 'r')?.to_string();
    if server_nonce.len() <= client_nonce.len() || !server_nonce.starts_with(client_nonce) {
        return Err("invalid SCRAM response (nonce mismatch)".into());
    }
    let salt = b64_decode(scram_attr(&fields, 's')?)?;
    let iterations = match scram_attr(&fields, 'i')?.parse::<i32>() {
        Ok(v) if v > 0 => v,
        _ => return Err("malformed SCRAM message (invalid iteration count)".into()),
    };
    Ok(ServerFirst { server_nonce, salt, iterations })
}

/// Extract the server signature (v=...) from the server-final-message payload.
fn parse_server_final(payload: &[u8]) -> Result<String, String> {
    let server_final = String::from_utf8_lossy(payload).into_owned();
    let fields: Vec<&str> = server_final.split(',').collect();
    Ok(scram_attr(&fields, 'v')?.to_string())
}

// SCRAM-SHA-256 client exchange, no channel binding (gs2 = "n,,").
fn scram_exchange(conn: &mut PgConn, password: &str) -> PgResult<Result<(), String>> {
    let scratch = mcx::MemoryContext::new("pgclient scram");
    let prep = saslprep::pg_saslprep(scratch.mcx(), password.as_bytes())
        .ok()
        .flatten()
        .map(|v| v.as_slice().to_vec())
        .unwrap_or_else(|| password.as_bytes().to_vec());

    // Entropy comes through the sanctioned seam: failure is a connection
    // error, and the sim harness can substitute a deterministic source.
    let mut raw_nonce = [0u8; scram_common::SCRAM_RAW_NONCE_LEN];
    if !pg_strong_random::pg_strong_random(&mut raw_nonce) {
        return Ok(Err("could not generate nonce".into()));
    }
    let client_nonce = b64(&raw_nonce);
    let client_first_bare = format!("n=,r={client_nonce}");

    let mut body = Vec::new();
    body.extend_from_slice(scram_common::SCRAM_SHA_256_NAME.as_bytes());
    body.push(0);
    let initial = format!("n,,{client_first_bare}");
    body.extend_from_slice(&((initial.len() as u32).to_be_bytes()));
    body.extend_from_slice(initial.as_bytes());
    if let Err(e) = conn.send_all(&msg(b'p', &body)) {
        return Ok(Err(e));
    }

    let (t, mbody) = match conn.read_message(conn.we.connect)? {
        Ok(m) => m,
        Err(e) => return Ok(Err(e)),
    };
    if t == b'E' {
        return Ok(Err(parse_error_fields(&mbody)));
    }
    if t != b'R' || auth_req_code(&mbody) != Ok(11) {
        return Ok(Err("expected SASL continue message from server".into()));
    }
    let server_first = String::from_utf8_lossy(&mbody[4..]).into_owned();
    let sf = match parse_server_first(&mbody[4..], &client_nonce) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };

    let salted = scram_common::scram_salted_password(&prep, &sf.salt, sf.iterations)?;
    let client_key = scram_common::scram_client_key(&salted);
    let stored_key = scram_common::scram_h(&client_key);

    let client_final_wo_proof = format!("c=biws,r={}", sf.server_nonce);
    let auth_message = format!("{client_first_bare},{server_first},{client_final_wo_proof}");
    let client_sig = pg_hmac::hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = client_key;
    for (p, s) in proof.iter_mut().zip(client_sig.iter()) {
        *p ^= s;
    }
    let client_final = format!("{client_final_wo_proof},p={}", b64(&proof));
    if let Err(e) = conn.send_all(&msg(b'p', client_final.as_bytes())) {
        return Ok(Err(e));
    }

    let (t, mbody) = match conn.read_message(conn.we.connect)? {
        Ok(m) => m,
        Err(e) => return Ok(Err(e)),
    };
    if t == b'E' {
        return Ok(Err(parse_error_fields(&mbody)));
    }
    if t != b'R' || auth_req_code(&mbody) != Ok(12) {
        return Ok(Err("expected SASL final message from server".into()));
    }
    let server_sig_b64 = match parse_server_final(&mbody[4..]) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let server_key = scram_common::scram_server_key(&salted);
    let expected = b64(&pg_hmac::hmac_sha256(&server_key, auth_message.as_bytes()));
    if server_sig_b64 != expected {
        return Ok(Err("incorrect server signature in SCRAM exchange".into()));
    }
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64_roundtrip() {
        let data = [7u8; 18];
        assert_eq!(b64_decode(&b64(&data)).unwrap(), data);
    }

    #[test]
    fn scram_attr_lookup() {
        let f: Vec<&str> = "r=abc,s=c2FsdA==,i=4096".split(',').collect();
        assert_eq!(scram_attr(&f, 'r').unwrap(), "abc");
        assert_eq!(scram_attr(&f, 'i').unwrap(), "4096");
        assert!(scram_attr(&f, 'v').is_err());
    }

    // ---- hostile/broken-server byte sequences: must error, never panic ----

    #[test]
    fn truncated_auth_request_header() {
        // 'R' message bodies shorter than the 4-byte request code.
        for body in [&[][..], &[0x00][..], &[0x00, 0x00][..], &[0x00, 0x00, 0x00][..]] {
            assert!(auth_req_code(body).is_err());
        }
        assert_eq!(auth_req_code(&[0, 0, 0, 10]), Ok(10));
    }

    #[test]
    fn short_md5_salt() {
        // AuthenticationMD5Password with a truncated (or absent) salt.
        assert!(md5_salt(&[0, 0, 0, 5]).is_err());
        assert!(md5_salt(&[0, 0, 0, 5, 0xaa]).is_err());
        assert!(md5_salt(&[0, 0, 0, 5, 0xaa, 0xbb, 0xcc]).is_err());
        assert_eq!(md5_salt(&[0, 0, 0, 5, 1, 2, 3, 4]).unwrap(), &[1, 2, 3, 4]);
    }

    #[test]
    fn sasl_mechanism_list_parses_terminated_list() {
        let mut body = vec![0, 0, 0, 10];
        body.extend_from_slice(b"SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0\0");
        assert_eq!(
            sasl_mechanisms(&body).unwrap(),
            vec!["SCRAM-SHA-256".to_string(), "SCRAM-SHA-256-PLUS".to_string()]
        );
    }

    #[test]
    fn sasl_mechanism_list_unterminated_is_rejected() {
        // Missing the final empty-string terminator.
        let mut body = vec![0, 0, 0, 10];
        body.extend_from_slice(b"SCRAM-SHA-256\0");
        assert!(sasl_mechanisms(&body).is_err());
        // Mechanism name itself runs off the end of the message.
        let mut body = vec![0, 0, 0, 10];
        body.extend_from_slice(b"SCRAM-SHA-256");
        assert!(sasl_mechanisms(&body).is_err());
        // No list bytes at all.
        assert!(sasl_mechanisms(&[0, 0, 0, 10]).is_err());
        // Body shorter than the request code.
        assert!(sasl_mechanisms(&[0, 0]).is_err());
    }

    #[test]
    fn server_first_valid() {
        let sf = parse_server_first(b"r=clientXYZserver,s=c2FsdA==,i=4096", "clientXYZ").unwrap();
        assert_eq!(sf.server_nonce, "clientXYZserver");
        assert_eq!(sf.salt, b"salt");
        assert_eq!(sf.iterations, 4096);
    }

    #[test]
    fn server_first_nonce_must_extend_client_nonce() {
        // Exact echo of the client nonce: no server contribution — reject.
        assert!(parse_server_first(b"r=clientXYZ,s=c2FsdA==,i=4096", "clientXYZ").is_err());
        // Wrong prefix.
        assert!(parse_server_first(b"r=evilnonceZZ,s=c2FsdA==,i=4096", "clientXYZ").is_err());
        // Shorter than the client nonce.
        assert!(parse_server_first(b"r=cli,s=c2FsdA==,i=4096", "clientXYZ").is_err());
        // Empty nonce.
        assert!(parse_server_first(b"r=,s=c2FsdA==,i=4096", "clientXYZ").is_err());
    }

    #[test]
    fn server_first_iteration_count_must_be_positive() {
        for bad in ["i=0", "i=-1", "i=-4096", "i=", "i=abc", "i=99999999999999999999"] {
            let m = format!("r=clientXYZserver,s=c2FsdA==,{bad}");
            assert!(parse_server_first(m.as_bytes(), "clientXYZ").is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn server_first_missing_attributes() {
        assert!(parse_server_first(b"s=c2FsdA==,i=4096", "clientXYZ").is_err());
        assert!(parse_server_first(b"r=clientXYZserver,i=4096", "clientXYZ").is_err());
        assert!(parse_server_first(b"r=clientXYZserver,s=c2FsdA==", "clientXYZ").is_err());
        assert!(parse_server_first(b"", "clientXYZ").is_err());
        assert!(parse_server_first(b"garbage", "clientXYZ").is_err());
    }

    #[test]
    fn server_first_bad_base64_salt() {
        assert!(parse_server_first(b"r=clientXYZserver,s=!!!!,i=4096", "clientXYZ").is_err());
    }

    #[test]
    fn server_first_non_utf8_bytes_do_not_panic() {
        // Invalid UTF-8 from the server must degrade to a parse error.
        let mut m = b"r=clientXYZ".to_vec();
        m.extend_from_slice(&[0xff, 0xfe]);
        m.extend_from_slice(b",s=c2FsdA==,i=4096");
        let _ = parse_server_first(&m, "clientXYZ");
    }

    #[test]
    fn server_final_signature_extraction() {
        assert_eq!(parse_server_final(b"v=c2ln").unwrap(), "c2ln");
        assert!(parse_server_final(b"").is_err());
        assert!(parse_server_final(b"e=other-error").is_err());
        // Bare attribute name with no '=' payload.
        assert!(parse_server_final(b"v").is_err());
    }
}
