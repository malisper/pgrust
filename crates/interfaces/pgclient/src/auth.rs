// Client-side auth ladder: trust, cleartext password, md5, SCRAM-SHA-256
// (no channel binding), then ParameterStatus/BackendKeyData collection up to
// ReadyForQuery.
//
// Every byte parsed here arrives from the SERVER, which dblink/postgres_fdw/
// walreceiver may point at an arbitrary host. A truncated or malformed
// message must therefore surface as a connection error, never a panic: all
// reads below are bounds-checked before slicing.
use scram_common::SCRAM_MAX_KEY_LEN;
use timingsafe_bcmp::timingsafe_bcmp;
use types_error::PgResult;

use crate::{be_i32, msg, parse_error_fields, PgConn};

// AuthRequest codes (src/include/libpq/protocol.h:74-87).
const AUTH_REQ_OK: i32 = 0;
const AUTH_REQ_PASSWORD: i32 = 3;
const AUTH_REQ_MD5: i32 = 5;
const AUTH_REQ_GSS: i32 = 7;
const AUTH_REQ_GSS_CONT: i32 = 8;
const AUTH_REQ_SSPI: i32 = 9;
const AUTH_REQ_SASL: i32 = 10;
const AUTH_REQ_SASL_CONT: i32 = 11;
const AUTH_REQ_SASL_FIN: i32 = 12;

/// fe-auth.c supported_sasl_mechs[]: the mechanisms require_auth can name
/// (pg_scram_mech, pg_oauth_mech), in table order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SaslMech {
    ScramSha256,
    OAuth,
}

const SUPPORTED_SASL_MECHS: [SaslMech; 2] = [SaslMech::ScramSha256, SaslMech::OAuth];

/// The connection's require_auth policy: libpq-int.h:513-519's
/// auth_required / allowed_auth_methods / allowed_sasl_mechs[2], filled by
/// fe-connect.c connectOptions2 (:1469-1723) and consulted by fe-auth.c
/// check_expected_areq (:903) and pg_SASL_init (:556). `require_auth` is
/// the option text (NULL in C when the option was never set); an explicit
/// empty value keeps the calloc'd defaults but still arms the checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthPolicy {
    require_auth: Option<String>,
    auth_required: bool,
    allowed_auth_methods: u32,
    allowed_sasl_mechs: [Option<SaslMech>; 2],
    channel_binding_required: bool,
}

impl AuthPolicy {
    /// connectOptions2's "parse and validate require_auth option"
    /// (fe-connect.c:1469-1723).
    pub(crate) fn parse(require_auth: Option<&str>) -> Result<AuthPolicy, String> {
        let mut pol = AuthPolicy {
            require_auth: require_auth.map(str::to_string),
            auth_required: false,
            allowed_auth_methods: 0,
            allowed_sasl_mechs: [None; 2],
            channel_binding_required: false,
        };
        let Some(s) = require_auth else {
            return Ok(pol);
        };
        if s.is_empty() {
            return Ok(pol);
        }
        // By default, start from an empty set of allowed methods and
        // mechanisms, and add to it.
        pol.auth_required = true;
        pol.allowed_auth_methods = 0;
        pol.allowed_sasl_mechs = [None; 2];
        let mut negated = false;
        // parse_comma_separated_list: fields are cut at every comma with no
        // trimming, so an empty field is an (invalid) empty method name.
        for (idx, part) in s.split(',').enumerate() {
            let first = idx == 0;
            let duplicate =
                || Err(format!("require_auth method \"{part}\" is specified more than once"));
            // Check for negation, e.g. '!password'. If one element is
            // negated, they all have to be.
            let method = if let Some(rest) = part.strip_prefix('!') {
                if first {
                    // Switch to a permissive set of allowed methods and
                    // mechanisms, and subtract from it.
                    pol.auth_required = false;
                    pol.allowed_auth_methods = u32::MAX;
                    pol.allowed_sasl_mechs = [Some(SUPPORTED_SASL_MECHS[0]), Some(SUPPORTED_SASL_MECHS[1])];
                } else if !negated {
                    return Err(format!(
                        "negative require_auth method \"{part}\" cannot be mixed with non-negative methods"
                    ));
                }
                negated = true;
                rest
            } else if negated {
                return Err(format!(
                    "require_auth method \"{part}\" cannot be mixed with negative methods"
                ));
            } else {
                part
            };

            let mut bits: u32 = 0;
            let mut mech: Option<SaslMech> = None;
            match method {
                // First group: methods that can be handled solely with the
                // authentication request codes.
                "password" => bits = 1 << AUTH_REQ_PASSWORD,
                "md5" => bits = 1 << AUTH_REQ_MD5,
                "gss" => bits = (1 << AUTH_REQ_GSS) | (1 << AUTH_REQ_GSS_CONT),
                "sspi" => bits = (1 << AUTH_REQ_SSPI) | (1 << AUTH_REQ_GSS_CONT),
                // Next group: SASL mechanisms, tracked separately since they
                // all share the same request codes.
                "scram-sha-256" => mech = Some(SaslMech::ScramSha256),
                "oauth" => mech = Some(SaslMech::OAuth),
                // Final group: meta-options. "none" lets the user explicitly
                // allow (or disallow) connections where the server sends no
                // authentication challenge, such as trust and cert auth.
                "none" => {
                    if negated {
                        if pol.auth_required {
                            return duplicate();
                        }
                        pol.auth_required = true;
                    } else {
                        if !pol.auth_required {
                            return duplicate();
                        }
                        pol.auth_required = false;
                    }
                    continue; // avoid the bitmask manipulation below
                }
                _ => return Err(format!("invalid require_auth value: \"{method}\"")),
            }

            if let Some(m) = mech {
                // Update the mechanism set only; the method bitmask is
                // updated for SASL further down.
                let slot = pol.allowed_sasl_mechs.iter().position(|x| *x == Some(m));
                if negated {
                    let Some(i) = slot else {
                        return duplicate();
                    };
                    pol.allowed_sasl_mechs[i] = None;
                } else {
                    if slot.is_some() {
                        return duplicate();
                    }
                    let Some(i) = pol.allowed_sasl_mechs.iter().position(Option::is_none) else {
                        return Err("internal error: no space in allowed_sasl_mechs".into());
                    };
                    pol.allowed_sasl_mechs[i] = Some(m);
                }
            } else if negated {
                if pol.allowed_auth_methods & bits == 0 {
                    return duplicate();
                }
                pol.allowed_auth_methods &= !bits;
            } else {
                if pol.allowed_auth_methods & bits == bits {
                    return duplicate();
                }
                pol.allowed_auth_methods |= bits;
            }
        }
        // Finally, allow SASL authentication requests if (and only if) any
        // mechanism is allowed: add the SASL bits in the standard case,
        // remove them in the negated case.
        let sasl_bits = (1 << AUTH_REQ_SASL) | (1 << AUTH_REQ_SASL_CONT) | (1 << AUTH_REQ_SASL_FIN);
        let allowed = pol.allowed_sasl_mechs.iter().any(Option::is_some);
        if !negated && allowed {
            pol.allowed_auth_methods |= sasl_bits;
        } else if negated && !allowed {
            pol.allowed_auth_methods &= !sasl_bits;
        }
        Ok(pol)
    }

    /// fe-auth.c check_expected_areq (:903), require_auth arm: reject every
    /// request the user did not allow, and demand a completed exchange
    /// before AuthenticationOk unless "none" was allowed.
    pub(crate) fn with_channel_binding_required(mut self, required: bool) -> Self {
        self.channel_binding_required = required;
        self
    }

    fn check_expected_areq(&self, areq: i32, client_finished_auth: bool) -> Result<(), String> {
        self.check_require_auth(areq, client_finished_auth)?;
        // fe-auth.c:1019-1044: with channel_binding=require never answer a
        // non-SASL request (it would leak the password), and never accept an
        // AuthenticationOk that was not channel-bound. This client has no TLS,
        // so no exchange is ever channel-bound.
        if self.channel_binding_required {
            match areq {
                AUTH_REQ_SASL | AUTH_REQ_SASL_CONT | AUTH_REQ_SASL_FIN => {}
                AUTH_REQ_OK => {
                    return Err("channel binding required, but server authenticated client without channel binding".into());
                }
                _ => {
                    return Err("channel binding required but not supported by server's authentication request".into());
                }
            }
        }
        Ok(())
    }

    /// pg_SASL_init's first check (fe-auth.c:446): SCRAM under
    /// channel_binding=require needs a TLS transport, which we never have.
    pub(crate) fn check_sasl_transport(&self) -> Result<(), String> {
        if self.channel_binding_required {
            return Err("channel binding required, but SSL not in use".into());
        }
        Ok(())
    }

    fn check_require_auth(&self, areq: i32, client_finished_auth: bool) -> Result<(), String> {
        let Some(require_auth) = &self.require_auth else {
            return Ok(());
        };
        let (ok, reason) = match areq {
            AUTH_REQ_OK => {
                if !self.auth_required || client_finished_auth {
                    (true, None)
                } else {
                    (false, Some("server did not complete authentication"))
                }
            }
            // Not the default arm, to avoid bit-shifting past the end of the
            // allowed_auth_methods mask on an unexpected AuthRequest.
            AUTH_REQ_PASSWORD | AUTH_REQ_MD5 | AUTH_REQ_GSS | AUTH_REQ_GSS_CONT | AUTH_REQ_SSPI
            | AUTH_REQ_SASL | AUTH_REQ_SASL_CONT | AUTH_REQ_SASL_FIN => {
                (self.allowed_auth_methods & (1 << areq) != 0, None)
            }
            _ => (false, None),
        };
        if ok {
            return Ok(());
        }
        let reason = reason.unwrap_or_else(|| auth_method_description(areq));
        Err(format!("authentication method requirement \"{require_auth}\" failed: {reason}"))
    }

    /// pg_SASL_init's "Make sure require_auth is satisfied" (fe-auth.c:556):
    /// the selected mechanism must be in the allowed list.
    fn check_sasl_mech(&self, mech: SaslMech, name: &str) -> Result<(), String> {
        let Some(require_auth) = &self.require_auth else {
            return Ok(());
        };
        if self.allowed_sasl_mechs.contains(&Some(mech)) {
            return Ok(());
        }
        Err(format!(
            "authentication method requirement \"{require_auth}\" failed: server requested {name} authentication"
        ))
    }
}

/// fe-auth.c auth_method_description (:864).
fn auth_method_description(areq: i32) -> &'static str {
    match areq {
        AUTH_REQ_PASSWORD => "server requested a cleartext password",
        AUTH_REQ_MD5 => "server requested a hashed password",
        AUTH_REQ_GSS | AUTH_REQ_GSS_CONT => "server requested GSSAPI authentication",
        AUTH_REQ_SSPI => "server requested SSPI authentication",
        AUTH_REQ_SASL | AUTH_REQ_SASL_CONT | AUTH_REQ_SASL_FIN => {
            "server requested SASL authentication"
        }
        _ => "server requested an unknown authentication type",
    }
}

/// SCRAM pass-through keys: libpq's scram_client_key_binary /
/// scram_server_key_binary, decoded from the base64 scram_client_key /
/// scram_server_key options by connectOptions2 (fe-connect.c:2018-2062).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScramKeys {
    client: Option<[u8; SCRAM_MAX_KEY_LEN]>,
    server: Option<[u8; SCRAM_MAX_KEY_LEN]>,
}

impl ScramKeys {
    pub(crate) fn parse(client: Option<&str>, server: Option<&str>) -> Result<ScramKeys, String> {
        Ok(ScramKeys {
            client: client.map(|s| decode_scram_key(s, "client")).transpose()?,
            server: server.map(|s| decode_scram_key(s, "server")).transpose()?,
        })
    }
}

fn decode_scram_key(encoded: &str, which: &str) -> Result<[u8; SCRAM_MAX_KEY_LEN], String> {
    let Some(bytes) = b64_decode_raw(encoded) else {
        return Err(format!("invalid SCRAM {which} key"));
    };
    if bytes.len() != SCRAM_MAX_KEY_LEN {
        return Err(format!("invalid SCRAM {which} key length: {}", bytes.len()));
    }
    let mut key = [0u8; SCRAM_MAX_KEY_LEN];
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// Everything the auth ladder needs besides the socket: the password (if
/// any), the pass-through keys, and the require_auth policy.
pub(crate) struct AuthOptions<'a> {
    pub(crate) password: Option<&'a str>,
    pub(crate) keys: ScramKeys,
    pub(crate) policy: AuthPolicy,
}

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
    auth: &AuthOptions<'_>,
) -> PgResult<Result<(), String>> {
    let password = auth.password;
    // libpq conn->client_finished_auth: set once our half of an exchange is
    // done (password sent, SCRAM server signature verified); AuthenticationOk
    // under require_auth demands it (fe-auth.c:955).
    let mut client_finished_auth = false;
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
                // pg_fe_sendauth: every request is vetted against
                // require_auth before it is answered (fe-auth.c:1072).
                if let Err(e) = auth.policy.check_expected_areq(authtype, client_finished_auth) {
                    return Ok(Err(e));
                }
                match authtype {
                    AUTH_REQ_OK => {}
                    AUTH_REQ_PASSWORD => {
                        conn.used_password = true;
                        let Some(pw) = password else {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        };
                        let mut b = pw.as_bytes().to_vec();
                        b.push(0);
                        if let Err(e) = conn.send_all(&msg(b'p', &b)) {
                            return Ok(Err(e));
                        }
                        // We expect no further authentication requests.
                        client_finished_auth = true;
                    }
                    AUTH_REQ_MD5 => {
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
                        client_finished_auth = true;
                    }
                    AUTH_REQ_SASL => {
                        // pg_SASL_init (fe-auth.c): SCRAM sets password_needed
                        // (PQconnectionUsedPassword) whether or not the
                        // exchange ends up using a pass-through key.
                        conn.used_password = true;
                        if let Err(e) = auth.policy.check_sasl_transport() {
                            return Ok(Err(e));
                        }
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
                        if let Err(e) = auth
                            .policy
                            .check_sasl_mech(SaslMech::ScramSha256, scram_common::SCRAM_SHA_256_NAME)
                        {
                            return Ok(Err(e));
                        }
                        // The password is only demanded when no pass-through
                        // client key was supplied (fe-auth.c:597
                        // "password_needed && !scram_client_key_binary").
                        if auth.keys.client.is_none() && password.is_none() {
                            return Ok(Err("fe_sendauth: no password supplied".into()));
                        }
                        if let Err(e) = scram_exchange(conn, password, &auth.keys, &auth.policy)? {
                            return Ok(Err(e));
                        }
                        // fe-auth-scram.c:286 (FE_SCRAM_FINISHED after the
                        // server signature verified).
                        client_finished_auth = true;
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

/// pg_b64_decode into a pg_b64_dec_len-sized buffer; None on the -1 arm.
fn b64_decode_raw(s: &str) -> Option<Vec<u8>> {
    let mut dst = vec![0u8; pg_b64::pg_b64_dec_len(s.len() as i32) as usize];
    let dstlen = dst.len() as i32;
    let n = pg_b64::pg_b64_decode(s.as_bytes(), s.len() as i32, &mut dst, dstlen);
    if n < 0 {
        return None;
    }
    dst.truncate(n as usize);
    Some(dst)
}

fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    b64_decode_raw(s).ok_or_else(|| "malformed base64 in SCRAM message".into())
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
    // upstream d93ef413174d (18.4): Apply timingsafe_bcmp() in authentication paths
    let n = client_nonce.len();
    if server_nonce.len() <= n
        || timingsafe_bcmp(&server_nonce.as_bytes()[..n], client_nonce.as_bytes()) != 0
    {
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

/// Mid-exchange 'R' message: pg_fe_sendauth vets its request code against
/// require_auth before the SASL continuation is consumed; anything but the
/// expected continue/final code is a protocol error.
fn expect_sasl_request(t: u8, mbody: &[u8], want: i32, policy: &AuthPolicy) -> Result<(), String> {
    let what = if want == AUTH_REQ_SASL_CONT { "continue" } else { "final" };
    if t == b'R' {
        if let Ok(code) = auth_req_code(mbody) {
            policy.check_expected_areq(code, false)?;
            if code == want {
                return Ok(());
            }
        }
    }
    Err(format!("expected SASL {what} message from server"))
}

// SCRAM-SHA-256 client exchange, no channel binding (gs2 = "n,,"). With
// pass-through keys (fe-auth-scram.c calculate_client_proof :783 /
// verify_server_signature :860) ClientKey / ServerKey are taken verbatim and
// SaltedPassword is never derived; a key that is missing falls back to the
// derivation from `state->SaltedPassword`, which is the calloc'd all-zero
// buffer when the client key was supplied and no password was needed.
fn scram_exchange(
    conn: &mut PgConn,
    password: Option<&str>,
    keys: &ScramKeys,
    policy: &AuthPolicy,
) -> PgResult<Result<(), String>> {
    // scram_init: SASLprep the password when there is one.
    let scratch = mcx::MemoryContext::new("pgclient scram");
    let prep: Vec<u8> = match password {
        Some(password) => saslprep::pg_saslprep(scratch.mcx(), password.as_bytes())
            .ok()
            .flatten()
            .map(|v| v.as_slice().to_vec())
            .unwrap_or_else(|| password.as_bytes().to_vec()),
        None => Vec::new(),
    };

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
    if let Err(e) = expect_sasl_request(t, &mbody, AUTH_REQ_SASL_CONT, policy) {
        return Ok(Err(e));
    }
    let server_first = String::from_utf8_lossy(&mbody[4..]).into_owned();
    let sf = match parse_server_first(&mbody[4..], &client_nonce) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };

    let mut salted = [0u8; SCRAM_MAX_KEY_LEN];
    let client_key = match keys.client {
        Some(k) => k,
        None => {
            salted = scram_common::scram_salted_password(&prep, &sf.salt, sf.iterations)?;
            scram_common::scram_client_key(&salted)
        }
    };
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
    if let Err(e) = expect_sasl_request(t, &mbody, AUTH_REQ_SASL_FIN, policy) {
        return Ok(Err(e));
    }
    let server_sig_b64 = match parse_server_final(&mbody[4..]) {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    let server_key = match keys.server {
        Some(k) => k,
        None => scram_common::scram_server_key(&salted),
    };
    let expected = b64(&pg_hmac::hmac_sha256(&server_key, auth_message.as_bytes()));
    // upstream d93ef413174d (18.4): Apply timingsafe_bcmp() in authentication paths
    if timingsafe_bcmp(server_sig_b64.as_bytes(), expected.as_bytes()) != 0 {
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

    // ---- require_auth policy (fe-connect.c:1469-1723, fe-auth.c:903) ----

    #[test]
    fn require_auth_unset_allows_everything() {
        let p = AuthPolicy::parse(None).unwrap();
        for areq in [0, 3, 5, 7, 8, 9, 10, 11, 12, 1, 99] {
            assert!(p.check_expected_areq(areq, false).is_ok(), "{areq}");
        }
        assert!(p.check_sasl_mech(SaslMech::ScramSha256, "SCRAM-SHA-256").is_ok());
    }

    // require_auth='' keeps the calloc'd defaults (nothing allowed, no
    // exchange required) but the checks are armed by the non-NULL string.
    #[test]
    fn require_auth_empty_arms_checks_with_defaults() {
        let p = AuthPolicy::parse(Some("")).unwrap();
        assert!(p.check_expected_areq(0, false).is_ok());
        assert_eq!(
            p.check_expected_areq(3, false).unwrap_err(),
            "authentication method requirement \"\" failed: server requested a cleartext password"
        );
        assert_eq!(
            p.check_sasl_mech(SaslMech::ScramSha256, "SCRAM-SHA-256").unwrap_err(),
            "authentication method requirement \"\" failed: server requested SCRAM-SHA-256 authentication"
        );
    }

    // dblink's pass-through form: only SASL requests, and a completed
    // exchange before AuthenticationOk.
    #[test]
    fn require_auth_scram_only() {
        let p = AuthPolicy::parse(Some("scram-sha-256")).unwrap();
        assert_eq!(
            p.check_expected_areq(0, false).unwrap_err(),
            "authentication method requirement \"scram-sha-256\" failed: server did not complete authentication"
        );
        assert!(p.check_expected_areq(0, true).is_ok());
        for areq in [10, 11, 12] {
            assert!(p.check_expected_areq(areq, false).is_ok(), "{areq}");
        }
        assert_eq!(
            p.check_expected_areq(5, false).unwrap_err(),
            "authentication method requirement \"scram-sha-256\" failed: server requested a hashed password"
        );
        assert_eq!(
            p.check_expected_areq(7, false).unwrap_err(),
            "authentication method requirement \"scram-sha-256\" failed: server requested GSSAPI authentication"
        );
        assert_eq!(
            p.check_expected_areq(9, false).unwrap_err(),
            "authentication method requirement \"scram-sha-256\" failed: server requested SSPI authentication"
        );
        assert_eq!(
            p.check_expected_areq(2, false).unwrap_err(),
            "authentication method requirement \"scram-sha-256\" failed: server requested an unknown authentication type"
        );
        assert!(p.check_sasl_mech(SaslMech::ScramSha256, "SCRAM-SHA-256").is_ok());
        assert_eq!(
            p.check_sasl_mech(SaslMech::OAuth, "OAUTHBEARER").unwrap_err(),
            "authentication method requirement \"scram-sha-256\" failed: server requested OAUTHBEARER authentication"
        );
    }

    #[test]
    fn require_auth_lists_and_none() {
        let p = AuthPolicy::parse(Some("password,md5")).unwrap();
        assert!(p.check_expected_areq(3, false).is_ok());
        assert!(p.check_expected_areq(5, false).is_ok());
        assert_eq!(
            p.check_expected_areq(10, false).unwrap_err(),
            "authentication method requirement \"password,md5\" failed: server requested SASL authentication"
        );
        // "none" allows an unchallenged AuthenticationOk.
        let p = AuthPolicy::parse(Some("none")).unwrap();
        assert!(p.check_expected_areq(0, false).is_ok());
        assert!(p.check_expected_areq(3, false).is_err());
        let p = AuthPolicy::parse(Some("scram-sha-256,none")).unwrap();
        assert!(p.check_expected_areq(0, false).is_ok());
        assert!(p.check_expected_areq(10, false).is_ok());
        // gss/sspi share the GSS_CONT bit.
        let p = AuthPolicy::parse(Some("gss")).unwrap();
        assert!(p.check_expected_areq(7, false).is_ok());
        assert!(p.check_expected_areq(8, false).is_ok());
        assert!(p.check_expected_areq(9, false).is_err());
    }

    #[test]
    fn require_auth_negated() {
        let p = AuthPolicy::parse(Some("!password")).unwrap();
        assert!(p.check_expected_areq(0, false).is_ok());
        assert!(p.check_expected_areq(3, false).is_err());
        assert!(p.check_expected_areq(5, false).is_ok());
        assert!(p.check_expected_areq(10, false).is_ok());
        assert!(p.check_sasl_mech(SaslMech::ScramSha256, "SCRAM-SHA-256").is_ok());
        // Negating every mechanism also drops the SASL request bits.
        let p = AuthPolicy::parse(Some("!scram-sha-256,!oauth")).unwrap();
        assert!(p.check_expected_areq(10, false).is_err());
        assert!(p.check_expected_areq(3, false).is_ok());
        // Negating one mechanism keeps SASL requests allowed.
        let p = AuthPolicy::parse(Some("!oauth")).unwrap();
        assert!(p.check_expected_areq(10, false).is_ok());
        assert!(p.check_sasl_mech(SaslMech::ScramSha256, "SCRAM-SHA-256").is_ok());
        assert!(p.check_sasl_mech(SaslMech::OAuth, "OAUTHBEARER").is_err());
        // "!none" requires an exchange.
        let p = AuthPolicy::parse(Some("!none")).unwrap();
        assert!(p.check_expected_areq(0, false).is_err());
        assert!(p.check_expected_areq(0, true).is_ok());
    }

    #[test]
    fn require_auth_diagnostics_match_libpq() {
        assert_eq!(
            AuthPolicy::parse(Some("bogus")).unwrap_err(),
            "invalid require_auth value: \"bogus\""
        );
        assert!(AuthPolicy::parse(Some("")).is_ok());
        assert_eq!(
            AuthPolicy::parse(Some("password,")).unwrap_err(),
            "invalid require_auth value: \"\""
        );
        assert_eq!(
            AuthPolicy::parse(Some("password,!md5")).unwrap_err(),
            "negative require_auth method \"!md5\" cannot be mixed with non-negative methods"
        );
        assert_eq!(
            AuthPolicy::parse(Some("!password,md5")).unwrap_err(),
            "require_auth method \"md5\" cannot be mixed with negative methods"
        );
        assert_eq!(
            AuthPolicy::parse(Some("md5,md5")).unwrap_err(),
            "require_auth method \"md5\" is specified more than once"
        );
        assert_eq!(
            AuthPolicy::parse(Some("!scram-sha-256,!scram-sha-256")).unwrap_err(),
            "require_auth method \"!scram-sha-256\" is specified more than once"
        );
        assert_eq!(
            AuthPolicy::parse(Some("none,none")).unwrap_err(),
            "require_auth method \"none\" is specified more than once"
        );
        assert_eq!(
            AuthPolicy::parse(Some("!none,!none")).unwrap_err(),
            "require_auth method \"!none\" is specified more than once"
        );
        // gss and sspi overlap on GSS_CONT only: not a duplicate.
        assert!(AuthPolicy::parse(Some("gss,sspi")).is_ok());
    }

    // ---- SCRAM pass-through keys (fe-connect.c:2018-2062) ----

    #[test]
    fn scram_keys_decode_and_validate() {
        let k = ScramKeys::parse(None, None).unwrap();
        assert_eq!(k, ScramKeys::default());
        let key = [0x5au8; SCRAM_MAX_KEY_LEN];
        let enc = b64(&key);
        let k = ScramKeys::parse(Some(enc.as_str()), Some(enc.as_str())).unwrap();
        assert_eq!(k.client, Some(key));
        assert_eq!(k.server, Some(key));
        assert_eq!(
            ScramKeys::parse(Some("!!!!"), None).unwrap_err(),
            "invalid SCRAM client key"
        );
        assert_eq!(
            ScramKeys::parse(None, Some("!!!!")).unwrap_err(),
            "invalid SCRAM server key"
        );
        assert_eq!(
            ScramKeys::parse(Some(b64(&[1u8; 16]).as_str()), None).unwrap_err(),
            "invalid SCRAM client key length: 16"
        );
        assert_eq!(
            ScramKeys::parse(None, Some("")).unwrap_err(),
            "invalid SCRAM server key length: 0"
        );
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
