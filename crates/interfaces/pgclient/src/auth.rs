// Client-side auth ladder: trust, cleartext password, md5, SCRAM-SHA-256
// (no channel binding), then ParameterStatus/BackendKeyData collection up to
// ReadyForQuery.
use types_error::PgResult;

use crate::{be_i32, msg, parse_error_fields, PgConn};

const MALFORMED_AUTH_REQUEST: &str = "malformed authentication request from server";

fn authentication_type(body: &[u8]) -> Result<i32, String> {
    body.get(..4).map(be_i32).ok_or_else(|| MALFORMED_AUTH_REQUEST.into())
}

fn sasl_mechanisms(body: &[u8]) -> Result<Vec<String>, String> {
    let mut mechanisms = Vec::new();
    let mut pos = 4;
    loop {
        let tail = body.get(pos..).ok_or_else(|| MALFORMED_AUTH_REQUEST.to_string())?;
        let end = tail
            .iter()
            .position(|&byte| byte == 0)
            .ok_or_else(|| MALFORMED_AUTH_REQUEST.to_string())?;
        if end == 0 {
            if pos + 1 != body.len() {
                return Err(MALFORMED_AUTH_REQUEST.into());
            }
            return Ok(mechanisms);
        }
        mechanisms.push(String::from_utf8_lossy(&tail[..end]).into_owned());
        pos += end + 1;
    }
}

fn authentication_payload<'a>(
    body: &'a [u8],
    expected_type: i32,
    unexpected_message: &'static str,
) -> Result<&'a [u8], String> {
    if authentication_type(body)? != expected_type {
        return Err(unexpected_message.into());
    }
    Ok(&body[4..])
}

fn generate_nonce() -> Result<[u8; scram_common::SCRAM_RAW_NONCE_LEN], String> {
    let mut nonce = [0u8; scram_common::SCRAM_RAW_NONCE_LEN];
    if !pg_strong_random::pg_strong_random(&mut nonce) {
        return Err("could not generate nonce".into());
    }
    Ok(nonce)
}

fn scram_iterations(fields: &[&str]) -> Result<i32, String> {
    match scram_attr(fields, 'i').and_then(|value| {
        value
            .parse::<i32>()
            .map_err(|_| "malformed SCRAM message (invalid iteration count)".to_string())
    }) {
        Ok(iterations) if iterations > 0 => Ok(iterations),
        _ => Err("malformed SCRAM message (invalid iteration count)".into()),
    }
}

fn validate_server_nonce(client_nonce: &str, server_nonce: &str) -> Result<(), String> {
    if server_nonce.len() <= client_nonce.len() || !server_nonce.starts_with(client_nonce) {
        return Err("invalid SCRAM response (nonce mismatch)".into());
    }
    Ok(())
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
                let authtype = match authentication_type(&mbody) {
                    Ok(authtype) => authtype,
                    Err(err) => return Ok(Err(err)),
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
                        let Some(salt) = mbody.get(4..8) else {
                            return Ok(Err(MALFORMED_AUTH_REQUEST.into()));
                        };
                        let stage1 = pg_md5::pg_md5_encrypt(pw.as_bytes(), user.as_bytes());
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
                            Ok(mechs) => mechs,
                            Err(err) => return Ok(Err(err)),
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
        .map(|f| &f[2..])
        .ok_or_else(|| format!("malformed SCRAM message (missing \"{name}\" attribute)"))
}

// SCRAM-SHA-256 client exchange, no channel binding (gs2 = "n,,").
fn scram_exchange(conn: &mut PgConn, password: &str) -> PgResult<Result<(), String>> {
    let scratch = mcx::MemoryContext::new("pgclient scram");
    let prep = saslprep::pg_saslprep(scratch.mcx(), password.as_bytes())
        .ok()
        .flatten()
        .map(|v| v.as_slice().to_vec())
        .unwrap_or_else(|| password.as_bytes().to_vec());

    let raw_nonce = match generate_nonce() {
        Ok(nonce) => nonce,
        Err(err) => return Ok(Err(err)),
    };
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
    if t != b'R' {
        return Ok(Err("expected SASL continue message from server".into()));
    }
    let server_first = match authentication_payload(
        &mbody,
        11,
        "expected SASL continue message from server",
    ) {
        Ok(payload) => String::from_utf8_lossy(payload).into_owned(),
        Err(err) => return Ok(Err(err)),
    };
    let fields: Vec<&str> = server_first.split(',').collect();
    let server_nonce = match scram_attr(&fields, 'r') {
        Ok(v) => v.to_string(),
        Err(e) => return Ok(Err(e)),
    };
    if let Err(err) = validate_server_nonce(&client_nonce, &server_nonce) {
        return Ok(Err(err));
    }
    let salt = match scram_attr(&fields, 's').and_then(b64_decode) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let iterations = match scram_iterations(&fields) {
        Ok(iterations) => iterations,
        Err(err) => return Ok(Err(err)),
    };

    let salted = scram_common::scram_salted_password(&prep, &salt, iterations)?;
    let client_key = scram_common::scram_client_key(&salted);
    let stored_key = scram_common::scram_h(&client_key);

    let client_final_wo_proof = format!("c=biws,r={server_nonce}");
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
    if t != b'R' {
        return Ok(Err("expected SASL final message from server".into()));
    }
    let server_final = match authentication_payload(
        &mbody,
        12,
        "expected SASL final message from server",
    ) {
        Ok(payload) => String::from_utf8_lossy(payload).into_owned(),
        Err(err) => return Ok(Err(err)),
    };
    let ffields: Vec<&str> = server_final.split(',').collect();
    let server_sig_b64 = match scram_attr(&ffields, 'v') {
        Ok(v) => v.to_string(),
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

    #[test]
    fn authentication_messages_require_complete_headers_and_lists() {
        assert!(authentication_type(&[]).is_err());
        assert_eq!(authentication_type(&10i32.to_be_bytes()).unwrap(), 10);
        assert!(authentication_payload(&[0, 0, 0], 11, "wrong type").is_err());

        let mut request = 10i32.to_be_bytes().to_vec();
        request.extend_from_slice(b"SCRAM-SHA-256\0OTHER\0\0");
        assert_eq!(
            sasl_mechanisms(&request).unwrap(),
            vec!["SCRAM-SHA-256", "OTHER"]
        );

        request.pop();
        assert!(sasl_mechanisms(&request).is_err());
        request.extend_from_slice(b"\0trailing");
        assert!(sasl_mechanisms(&request).is_err());
    }

    #[test]
    fn scram_parameters_reject_invalid_iterations_and_nonce_reuse() {
        assert_eq!(scram_iterations(&["i=4096"]).unwrap(), 4096);
        assert!(scram_iterations(&["i=0"]).is_err());
        assert!(scram_iterations(&["i=-1"]).is_err());
        assert!(scram_iterations(&["i=invalid"]).is_err());

        assert!(validate_server_nonce("client", "client-server").is_ok());
        assert!(validate_server_nonce("client", "client").is_err());
        assert!(validate_server_nonce("client", "other-server").is_err());
    }

    #[test]
    fn nonce_uses_the_strong_random_source() {
        assert!(generate_nonce().is_ok());
    }
}
