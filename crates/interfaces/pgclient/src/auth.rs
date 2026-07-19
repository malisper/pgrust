// Client-side auth ladder: trust, cleartext password, md5, SCRAM-SHA-256
// (no channel binding), then ParameterStatus/BackendKeyData collection up to
// ReadyForQuery.
use types_error::PgResult;

use crate::{be_i32, cstr_at, msg, parse_error_fields, PgConn};

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
                let authtype = be_i32(&mbody[0..4]);
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
                        let salt = &mbody[4..8];
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
                        let mut mechs = Vec::new();
                        let mut p = 4;
                        while p < mbody.len() && mbody[p] != 0 {
                            let (m, next) = cstr_at(&mbody, p);
                            mechs.push(m);
                            p = next;
                        }
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

    let mut raw_nonce = [0u8; scram_common::SCRAM_RAW_NONCE_LEN];
    if std::io::Read::read_exact(
        &mut std::fs::File::open("/dev/urandom").expect("open /dev/urandom"),
        &mut raw_nonce,
    )
    .is_err()
    {
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
    if t != b'R' || be_i32(&mbody[0..4]) != 11 {
        return Ok(Err("expected SASL continue message from server".into()));
    }
    let server_first = String::from_utf8_lossy(&mbody[4..]).into_owned();
    let fields: Vec<&str> = server_first.split(',').collect();
    let server_nonce = match scram_attr(&fields, 'r') {
        Ok(v) => v.to_string(),
        Err(e) => return Ok(Err(e)),
    };
    if !server_nonce.starts_with(&client_nonce) {
        return Ok(Err("invalid SCRAM response (nonce mismatch)".into()));
    }
    let salt = match scram_attr(&fields, 's').and_then(b64_decode) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let iterations: i32 = match scram_attr(&fields, 'i').map(|s| s.parse::<i32>()) {
        Ok(Ok(v)) => v,
        _ => return Ok(Err("malformed SCRAM message (invalid iteration count)".into())),
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
    if t != b'R' || be_i32(&mbody[0..4]) != 12 {
        return Ok(Err("expected SASL final message from server".into()));
    }
    let server_final = String::from_utf8_lossy(&mbody[4..]).into_owned();
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
}
