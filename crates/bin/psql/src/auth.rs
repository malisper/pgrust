//! Client-side auth ladder: trust, cleartext password, md5, SCRAM-SHA-256
//! (RFC 5802, no channel binding), then ParameterStatus/BackendKeyData
//! collection up to ReadyForQuery. Cribbed from crates/interfaces/pgclient
//! (which mirrors libpq fe-auth.c / fe-auth-scram.c), retargeted at psql's
//! own blocking Conn.

use crate::proto::{be_i32, cstr_at, msg, parse_diag, Conn};

fn error_text(f: &crate::proto::ErrorFields) -> String {
    let mut out = format!("{}:  {}", f.severity, f.primary);
    if !f.detail.is_empty() {
        out.push_str(&format!("\nDETAIL:  {}", f.detail));
    }
    if !f.hint.is_empty() {
        out.push_str(&format!("\nHINT:  {}", f.hint));
    }
    out
}

pub(crate) fn handshake(
    conn: &mut Conn,
    user: &str,
    password: Option<&str>,
) -> Result<(), String> {
    loop {
        let (t, mbody) = conn.read_message()?;
        match t {
            b'R' => {
                let authtype = be_i32(&mbody[0..4]);
                if matches!(authtype, 3 | 5 | 10) {
                    conn.used_password = true;
                }
                match authtype {
                    0 => {}
                    3 => {
                        let Some(pw) = password else {
                            return Err("fe_sendauth: no password supplied".into());
                        };
                        let mut b = pw.as_bytes().to_vec();
                        b.push(0);
                        conn_send(conn, &msg(b'p', &b))?;
                    }
                    5 => {
                        let Some(pw) = password else {
                            return Err("fe_sendauth: no password supplied".into());
                        };
                        let salt = &mbody[4..8];
                        let stage1 = pg_md5::pg_md5_encrypt(pw.as_bytes(), user.as_bytes());
                        let hex = &stage1[3..];
                        let stage2 = pg_md5::pg_md5_encrypt(hex, salt);
                        let mut b = stage2.to_vec();
                        b.push(0);
                        conn_send(conn, &msg(b'p', &b))?;
                    }
                    10 => {
                        let Some(pw) = password else {
                            return Err("fe_sendauth: no password supplied".into());
                        };
                        let mut mechs = Vec::new();
                        let mut p = 4;
                        while p < mbody.len() && mbody[p] != 0 {
                            let (m, next) = cstr_at(&mbody, p);
                            mechs.push(m);
                            p = next;
                        }
                        if !mechs.iter().any(|m| m == scram_common::SCRAM_SHA_256_NAME) {
                            return Err(format!(
                                "none of the server's SASL authentication mechanisms are supported (offered: {})",
                                mechs.join(", ")
                            ));
                        }
                        scram_exchange(conn, pw)?;
                    }
                    other => {
                        return Err(format!("authentication method {other} not supported"))
                    }
                }
            }
            b'S' | b'K' | b'N' | b'A' => conn.note_async(t, &mbody),
            b'E' => return Err(error_text(&parse_diag(&mbody))),
            b'Z' => {
                conn.txn_status = mbody.first().copied().unwrap_or(b'I');
                return Ok(());
            }
            other => {
                return Err(format!(
                    "unexpected message type \"{}\" during connection startup",
                    other as char
                ))
            }
        }
    }
}

fn conn_send(conn: &mut Conn, buf: &[u8]) -> Result<(), String> {
    // send_all is private to proto; route through a tiny shim.
    conn.send_raw(buf)
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

#[cfg(not(target_family = "wasm"))]
fn random_nonce() -> Result<[u8; scram_common::SCRAM_RAW_NONCE_LEN], String> {
    let mut raw = [0u8; scram_common::SCRAM_RAW_NONCE_LEN];
    let mut f = std::fs::File::open("/dev/urandom")
        .map_err(|e| format!("could not open /dev/urandom: {e}"))?;
    std::io::Read::read_exact(&mut f, &mut raw)
        .map_err(|_| "could not generate nonce".to_string())?;
    Ok(raw)
}

// wasm32-wasip1: no /dev/urandom. The SCRAM client nonce only needs
// uniqueness (it is sent in cleartext); derive it from time+pid via SHA-256.
#[cfg(target_family = "wasm")]
fn random_nonce() -> Result<[u8; scram_common::SCRAM_RAW_NONCE_LEN], String> {
    let mut seed = Vec::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    seed.extend_from_slice(&now.as_nanos().to_le_bytes());
    seed.extend_from_slice(&std::process::id().to_le_bytes());
    let digest = pg_hmac::hmac_sha256(b"psql-scram-nonce", &seed);
    let mut raw = [0u8; scram_common::SCRAM_RAW_NONCE_LEN];
    let n = raw.len().min(digest.len());
    raw[..n].copy_from_slice(&digest[..n]);
    Ok(raw)
}

// SCRAM-SHA-256 client exchange, no channel binding (gs2 = "n,,").
fn scram_exchange(conn: &mut Conn, password: &str) -> Result<(), String> {
    let scratch = mcx::MemoryContext::new("psql scram");
    let prep = saslprep::pg_saslprep(scratch.mcx(), password.as_bytes())
        .ok()
        .flatten()
        .map(|v| v.as_slice().to_vec())
        .unwrap_or_else(|| password.as_bytes().to_vec());

    let raw_nonce = random_nonce()?;
    let client_nonce = b64(&raw_nonce);
    let client_first_bare = format!("n=,r={client_nonce}");

    let mut body = Vec::new();
    body.extend_from_slice(scram_common::SCRAM_SHA_256_NAME.as_bytes());
    body.push(0);
    let initial = format!("n,,{client_first_bare}");
    body.extend_from_slice(&((initial.len() as u32).to_be_bytes()));
    body.extend_from_slice(initial.as_bytes());
    conn.send_raw(&msg(b'p', &body))?;

    let (t, mbody) = conn.read_message()?;
    if t == b'E' {
        return Err(error_text(&parse_diag(&mbody)));
    }
    if t != b'R' || be_i32(&mbody[0..4]) != 11 {
        return Err("expected SASL continue message from server".into());
    }
    let server_first = String::from_utf8_lossy(&mbody[4..]).into_owned();
    let fields: Vec<&str> = server_first.split(',').collect();
    let server_nonce = scram_attr(&fields, 'r')?.to_string();
    if !server_nonce.starts_with(&client_nonce) {
        return Err("invalid SCRAM response (nonce mismatch)".into());
    }
    let salt = scram_attr(&fields, 's').and_then(b64_decode)?;
    let iterations: i32 = match scram_attr(&fields, 'i').map(|s| s.parse::<i32>()) {
        Ok(Ok(v)) => v,
        _ => return Err("malformed SCRAM message (invalid iteration count)".into()),
    };

    let salted = scram_common::scram_salted_password(&prep, &salt, iterations)
        .map_err(|e| format!("SCRAM key derivation failed: {e:?}"))?;
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
    conn.send_raw(&msg(b'p', client_final.as_bytes()))?;

    let (t, mbody) = conn.read_message()?;
    if t == b'E' {
        return Err(error_text(&parse_diag(&mbody)));
    }
    if t != b'R' || be_i32(&mbody[0..4]) != 12 {
        return Err("expected SASL final message from server".into());
    }
    let server_final = String::from_utf8_lossy(&mbody[4..]).into_owned();
    let ffields: Vec<&str> = server_final.split(',').collect();
    let server_sig_b64 = scram_attr(&ffields, 'v')?.to_string();
    let server_key = scram_common::scram_server_key(&salted);
    let expected = b64(&pg_hmac::hmac_sha256(&server_key, auth_message.as_bytes()));
    if server_sig_b64 != expected {
        return Err("incorrect server signature in SCRAM exchange".into());
    }
    Ok(())
}
