use auth_sasl::{
    SaslMech, PG_MAX_SASL_MESSAGE_LENGTH, PG_SASL_EXCHANGE_CONTINUE, PG_SASL_EXCHANGE_FAILURE,
    PG_SASL_EXCHANGE_SUCCESS,
};
use pg_b64::{pg_b64_enc_len, pg_b64_encode};
use pg_hmac::{PgHmacCtx, Sha256};
use scram_common::{
    scram_h, SCRAM_MAX_KEY_LEN, SCRAM_RAW_NONCE_LEN, SCRAM_SHA_256_KEY_LEN, SCRAM_SHA_256_NAME,
    SCRAM_SHA_256_PLUS_NAME,
};
use stringinfo::StringInfo;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION, ERRCODE_PROTOCOL_VIOLATION, ERROR,
};
use types_startup::Port;

use crate::{b64dec, loc, mock_scram_secret, parse_scram_secret};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ScramPhase {
    Init,
    SaltSent,
    Finished,
}

#[cfg_attr(test, derive(Debug))]
pub struct ScramState {
    phase: ScramPhase,
    channel_binding_in_use: bool,
    key_length: usize,
    iterations: i32,
    salt: String,
    client_key: [u8; SCRAM_MAX_KEY_LEN],
    stored_key: [u8; SCRAM_MAX_KEY_LEN],
    server_key: [u8; SCRAM_MAX_KEY_LEN],
    cbind_flag: u8,
    client_first_message_bare: Vec<u8>,
    #[allow(dead_code)] // ignored, kept like C for debugging.
    client_username: Vec<u8>,
    client_nonce: Vec<u8>,
    client_final_message_without_proof: Vec<u8>,
    client_final_nonce: Vec<u8>,
    client_proof: [u8; SCRAM_MAX_KEY_LEN],
    server_first_message: Vec<u8>,
    server_nonce: Vec<u8>,
    doomed: bool,
    logdetail: Option<String>,
}

pub struct ScramMech;

impl SaslMech for ScramMech {
    type State = ScramState;

    fn max_message_length(&self) -> i32 {
        PG_MAX_SASL_MESSAGE_LENGTH
    }

    fn get_mechanisms(&self, port: &Port, buf: &mut StringInfo<'_>) -> PgResult<()> {
        if port.ssl_in_use {
            buf.append_str(SCRAM_SHA_256_PLUS_NAME)?;
            buf.append_byte(0)?;
        }
        buf.append_str(SCRAM_SHA_256_NAME)?;
        buf.append_byte(0)?;
        Ok(())
    }

    fn init(
        &self,
        port: &Port,
        selected_mech: &[u8],
        shadow_pass: Option<&str>,
    ) -> PgResult<ScramState> {
        scram_init(port, selected_mech, shadow_pass)
    }

    fn exchange(
        &self,
        state: &mut ScramState,
        port: &mut Port,
        input: Option<&[u8]>,
        logdetail: &mut Option<String>,
    ) -> PgResult<(i32, Option<Vec<u8>>)> {
        scram_exchange(state, port, input, logdetail)
    }
}

fn scram_init(port: &Port, selected_mech: &[u8], shadow_pass: Option<&str>) -> PgResult<ScramState> {
    let channel_binding_in_use = if selected_mech == SCRAM_SHA_256_PLUS_NAME.as_bytes()
        && port.ssl_in_use
    {
        true
    } else if selected_mech == SCRAM_SHA_256_NAME.as_bytes() {
        false
    } else {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("client selected an invalid SASL authentication mechanism")
            .finish(loc("scram_init"))?;
        unreachable!()
    };

    let user_name = port.user_name.as_deref().unwrap_or("");
    let mut logdetail = None;

    // C branches on get_password_type first; a secret classified SCRAM there
    // is exactly one parse_scram_secret accepts, so the "looked like a SCRAM
    // secret, but could not be parsed" LOG arm is unreachable and both non-
    // SCRAM shapes (MD5 secret, plaintext) take the logdetail arm.
    let parsed = match shadow_pass {
        Some(shadow_pass) => match parse_scram_secret(shadow_pass) {
            Some(p) => Some(p),
            None => {
                logdetail = Some(format!(
                    "User \"{user_name}\" does not have a valid SCRAM secret."
                ));
                None
            }
        },
        None => None,
    };

    let state = match parsed {
        Some(p) => ScramState {
            phase: ScramPhase::Init,
            channel_binding_in_use,
            key_length: p.key_length as usize,
            iterations: p.iterations,
            salt: p.salt,
            client_key: [0; SCRAM_MAX_KEY_LEN],
            stored_key: p.stored_key,
            server_key: p.server_key,
            cbind_flag: 0,
            client_first_message_bare: Vec::new(),
            client_username: Vec::new(),
            client_nonce: Vec::new(),
            client_final_message_without_proof: Vec::new(),
            client_final_nonce: Vec::new(),
            client_proof: [0; SCRAM_MAX_KEY_LEN],
            server_first_message: Vec::new(),
            server_nonce: Vec::new(),
            doomed: false,
            logdetail,
        },
        None => {
            let mock = mock_scram_secret(user_name)?;
            ScramState {
                phase: ScramPhase::Init,
                channel_binding_in_use,
                key_length: mock.key_length as usize,
                iterations: mock.iterations,
                salt: mock.salt,
                client_key: [0; SCRAM_MAX_KEY_LEN],
                stored_key: mock.stored_key,
                server_key: mock.server_key,
                cbind_flag: 0,
                client_first_message_bare: Vec::new(),
                client_username: Vec::new(),
                client_nonce: Vec::new(),
                client_final_message_without_proof: Vec::new(),
                client_final_nonce: Vec::new(),
                client_proof: [0; SCRAM_MAX_KEY_LEN],
                server_first_message: Vec::new(),
                server_nonce: Vec::new(),
                doomed: true,
                logdetail,
            }
        }
    };

    Ok(state)
}

fn scram_exchange(
    state: &mut ScramState,
    port: &mut Port,
    input: Option<&[u8]>,
    logdetail: &mut Option<String>,
) -> PgResult<(i32, Option<Vec<u8>>)> {
    let Some(input) = input else {
        debug_assert!(state.phase == ScramPhase::Init);
        return Ok((PG_SASL_EXCHANGE_CONTINUE, Some(Vec::new())));
    };

    if input.is_empty() {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("The message is empty.")
            .finish(loc("scram_exchange"))?;
    }
    // C: inputlen != strlen(input); an embedded NUL is the only mismatch.
    if input.contains(&0) {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("Message length does not match input length.")
            .finish(loc("scram_exchange"))?;
    }

    let mut output: Option<Vec<u8>> = None;
    let result;
    match state.phase {
        ScramPhase::Init => {
            read_client_first_message(state, port.ssl_in_use, input)?;
            output = Some(build_server_first_message(state)?);
            state.phase = ScramPhase::SaltSent;
            result = PG_SASL_EXCHANGE_CONTINUE;
        }
        ScramPhase::SaltSent => {
            read_client_final_message(state, input)?;

            if !verify_final_nonce(state) {
                elog::ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("invalid SCRAM response")
                    .errdetail("Nonce does not match.")
                    .finish(loc("scram_exchange"))?;
            }

            // Order is intentional: compute the client proof even when
            // doomed, to keep mock authentication timing-shaped like a
            // wrong password.
            if !verify_client_proof(state) || state.doomed {
                result = PG_SASL_EXCHANGE_FAILURE;
            } else {
                output = Some(build_server_final_message(state));
                result = PG_SASL_EXCHANGE_SUCCESS;
                state.phase = ScramPhase::Finished;
            }
        }
        ScramPhase::Finished => {
            elog::ereport(ERROR)
                .errmsg_internal("invalid SCRAM exchange state")
                .finish(loc("scram_exchange"))?;
            unreachable!()
        }
    }

    if result == PG_SASL_EXCHANGE_FAILURE && state.logdetail.is_some() {
        *logdetail = state.logdetail.clone();
    }

    if result == PG_SASL_EXCHANGE_SUCCESS && state.phase == ScramPhase::Finished {
        port.scram_client_key = state.client_key;
        port.scram_server_key = state.server_key;
        port.has_scram_keys = true;
    }

    Ok((result, output))
}

// C's parsers walk a NUL-terminated copy; end-of-slice reads as the NUL.
fn at(input: &[u8], pos: usize) -> u8 {
    input.get(pos).copied().unwrap_or(0)
}

fn read_attr_value<'a>(input: &'a [u8], pos: &mut usize, attr: u8) -> PgResult<&'a [u8]> {
    let c = at(input, *pos);
    if c != attr {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Expected attribute \"{}\" but found \"{}\".",
                attr as char,
                sanitize_char(c)
            ))
            .finish(loc("read_attr_value"))?;
    }
    *pos += 1;
    if at(input, *pos) != b'=' {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Expected character \"=\" for attribute \"{}\".",
                attr as char
            ))
            .finish(loc("read_attr_value"))?;
    }
    *pos += 1;

    let begin = *pos;
    let mut end = *pos;
    while end < input.len() && input[end] != b',' {
        end += 1;
    }
    *pos = if end < input.len() { end + 1 } else { end };
    Ok(&input[begin..end])
}

fn read_any_attr<'a>(input: &'a [u8], pos: &mut usize) -> PgResult<(u8, &'a [u8])> {
    let attr = at(input, *pos);
    if attr == 0 {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("Attribute expected, but found end of string.")
            .finish(loc("read_any_attr"))?;
    }
    if !attr.is_ascii_alphabetic() {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Attribute expected, but found invalid character \"{}\".",
                sanitize_char(attr)
            ))
            .finish(loc("read_any_attr"))?;
    }
    *pos += 1;
    if at(input, *pos) != b'=' {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Expected character \"=\" for attribute \"{}\".",
                attr as char
            ))
            .finish(loc("read_any_attr"))?;
    }
    *pos += 1;

    let begin = *pos;
    let mut end = *pos;
    while end < input.len() && input[end] != b',' {
        end += 1;
    }
    *pos = if end < input.len() { end + 1 } else { end };
    Ok((attr, &input[begin..end]))
}

fn is_scram_printable(p: &[u8]) -> bool {
    // RFC 5802 printable: %x21-2B / %x2D-7E (ASCII printables minus comma).
    p.iter().all(|&c| (0x21..=0x7e).contains(&c) && c != 0x2c)
}

fn sanitize_char(c: u8) -> String {
    if (0x21..=0x7e).contains(&c) {
        format!("'{}'", c as char)
    } else {
        format!("0x{c:02x}")
    }
}

fn sanitize_str(s: &[u8]) -> String {
    s.iter()
        .take(30)
        .map(|&c| if (0x21..=0x7e).contains(&c) { c as char } else { '?' })
        .collect()
}

fn comma_expected(found: u8, funcname: &'static str) -> PgResult<()> {
    elog::ereport(ERROR)
        .errcode(ERRCODE_PROTOCOL_VIOLATION)
        .errmsg("malformed SCRAM message")
        .errdetail(format!(
            "Comma expected, but found character \"{}\".",
            sanitize_char(found)
        ))
        .finish(loc(funcname))
}

fn read_client_first_message(
    state: &mut ScramState,
    ssl_in_use: bool,
    input: &[u8],
) -> PgResult<()> {
    let mut pos = 0usize;

    state.cbind_flag = at(input, 0);
    match at(input, 0) {
        b'n' => {
            // Client does not use channel binding.
            if state.channel_binding_in_use {
                elog::ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("malformed SCRAM message")
                    .errdetail("The client selected SCRAM-SHA-256-PLUS, but the SCRAM message does not include channel binding data.")
                    .finish(loc("read_client_first_message"))?;
            }
            pos += 1;
            if at(input, pos) != b',' {
                comma_expected(at(input, pos), "read_client_first_message")?;
            }
            pos += 1;
        }
        b'y' => {
            // Client supports channel binding and thinks the server does not.
            if state.channel_binding_in_use {
                elog::ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("malformed SCRAM message")
                    .errdetail("The client selected SCRAM-SHA-256-PLUS, but the SCRAM message does not include channel binding data.")
                    .finish(loc("read_client_first_message"))?;
            }
            // 28000: rejecting "y" under SSL thwarts binding-downgrade attacks.
            if ssl_in_use {
                elog::ereport(ERROR)
                    .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                    .errmsg("SCRAM channel binding negotiation error")
                    .errdetail("The client supports SCRAM channel binding but thinks the server does not.  However, this server does support channel binding.")
                    .finish(loc("read_client_first_message"))?;
            }
            pos += 1;
            if at(input, pos) != b',' {
                comma_expected(at(input, pos), "read_client_first_message")?;
            }
            pos += 1;
        }
        b'p' => {
            // Client requires channel binding, e.g. "p=tls-server-end-point".
            if !state.channel_binding_in_use {
                elog::ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("malformed SCRAM message")
                    .errdetail("The client selected SCRAM-SHA-256 without channel binding, but the SCRAM message includes channel binding data.")
                    .finish(loc("read_client_first_message"))?;
            }
            let channel_binding_type = read_attr_value(input, &mut pos, b'p')?;
            if channel_binding_type != b"tls-server-end-point" {
                elog::ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!(
                        "unsupported SCRAM channel-binding type \"{}\"",
                        sanitize_str(channel_binding_type)
                    ))
                    .finish(loc("read_client_first_message"))?;
            }
        }
        c => {
            elog::ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("malformed SCRAM message")
                .errdetail(format!(
                    "Unexpected channel-binding flag \"{}\".",
                    sanitize_char(c)
                ))
                .finish(loc("read_client_first_message"))?;
        }
    }

    if at(input, pos) == b'a' {
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("client uses authorization identity, but it is not supported")
            .finish(loc("read_client_first_message"))?;
    }
    if at(input, pos) != b',' {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail(format!(
                "Unexpected attribute \"{}\" in client-first-message.",
                sanitize_char(at(input, pos))
            ))
            .finish(loc("read_client_first_message"))?;
    }
    pos += 1;

    state.client_first_message_bare = input[pos..].to_vec();

    if at(input, pos) == b'm' {
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("client requires an unsupported SCRAM extension")
            .finish(loc("read_client_first_message"))?;
    }

    state.client_username = read_attr_value(input, &mut pos, b'n')?.to_vec();

    let client_nonce = read_attr_value(input, &mut pos, b'r')?;
    if !is_scram_printable(client_nonce) {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("non-printable characters in SCRAM nonce")
            .finish(loc("read_client_first_message"))?;
    }
    state.client_nonce = client_nonce.to_vec();

    while at(input, pos) != 0 {
        read_any_attr(input, &mut pos)?;
    }

    Ok(())
}

fn build_server_first_message(state: &mut ScramState) -> PgResult<Vec<u8>> {
    let mut raw_nonce = [0u8; SCRAM_RAW_NONCE_LEN];
    if !pg_strong_random::pg_strong_random(&mut raw_nonce) {
        elog::ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("could not generate random nonce")
            .finish(loc("build_server_first_message"))?;
    }

    let cap = pg_b64_enc_len(SCRAM_RAW_NONCE_LEN as i32);
    let mut encoded = vec![0u8; cap as usize];
    let n = pg_b64_encode(&raw_nonce, SCRAM_RAW_NONCE_LEN as i32, &mut encoded, cap);
    if n < 0 {
        elog::ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("could not encode random nonce")
            .finish(loc("build_server_first_message"))?;
    }
    encoded.truncate(n as usize);
    state.server_nonce = encoded;

    let mut msg = Vec::with_capacity(
        2 + state.client_nonce.len() + state.server_nonce.len() + 3 + state.salt.len() + 16,
    );
    msg.extend_from_slice(b"r=");
    msg.extend_from_slice(&state.client_nonce);
    msg.extend_from_slice(&state.server_nonce);
    msg.extend_from_slice(b",s=");
    msg.extend_from_slice(state.salt.as_bytes());
    msg.extend_from_slice(b",i=");
    msg.extend_from_slice(state.iterations.to_string().as_bytes());

    state.server_first_message = msg.clone();
    Ok(msg)
}

fn read_client_final_message(state: &mut ScramState, input: &[u8]) -> PgResult<()> {
    let mut pos = 0usize;

    let channel_binding = read_attr_value(input, &mut pos, b'c')?;
    if state.channel_binding_in_use {
        debug_assert!(state.cbind_flag == b'p');

        let cbind_data = be_secure_seams::be_tls_get_certificate_hash::call()?;
        if cbind_data.is_empty() {
            elog::ereport(ERROR)
                .errmsg_internal("could not get server certificate hash")
                .finish(loc("read_client_final_message"))?;
        }

        let mut cbind_input = b"p=tls-server-end-point,,".to_vec();
        cbind_input.extend_from_slice(&cbind_data);

        let cap = pg_b64_enc_len(cbind_input.len() as i32);
        let mut b64_message = vec![0u8; cap as usize];
        let n = pg_b64_encode(&cbind_input, cbind_input.len() as i32, &mut b64_message, cap);
        if n < 0 {
            elog::ereport(ERROR)
                .errmsg_internal("could not encode channel binding data")
                .finish(loc("read_client_final_message"))?;
        }
        b64_message.truncate(n as usize);

        if channel_binding != &b64_message[..] {
            elog::ereport(ERROR)
                .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                .errmsg("SCRAM channel binding check failed")
                .finish(loc("read_client_final_message"))?;
        }
    } else {
        // Expect "biws" ("n,," base64) or "eSws" ("y,,"), matching the flag
        // the client sent in the first message.
        if !((channel_binding == b"biws" && state.cbind_flag == b'n')
            || (channel_binding == b"eSws" && state.cbind_flag == b'y'))
        {
            elog::ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("unexpected SCRAM channel-binding attribute in client-final-message")
                .finish(loc("read_client_final_message"))?;
        }
    }

    state.client_final_nonce = read_attr_value(input, &mut pos, b'r')?.to_vec();

    // Ignore optional extensions: scan until the "p" attribute.
    let mut proof_pos;
    let value;
    loop {
        proof_pos = pos - 1;
        let (attr, v) = read_any_attr(input, &mut pos)?;
        if attr == b'p' {
            value = v;
            break;
        }
    }

    let decoded = b64dec(value);
    match decoded {
        Some(ref d) if d.len() == state.key_length => {
            state.client_proof[..state.key_length].copy_from_slice(d);
        }
        _ => {
            elog::ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("malformed SCRAM message")
                .errdetail("Malformed proof in client-final-message.")
                .finish(loc("read_client_final_message"))?;
        }
    }

    if at(input, pos) != 0 {
        elog::ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed SCRAM message")
            .errdetail("Garbage found at the end of client-final-message.")
            .finish(loc("read_client_final_message"))?;
    }

    state.client_final_message_without_proof = input[..proof_pos].to_vec();
    Ok(())
}

fn verify_final_nonce(state: &ScramState) -> bool {
    let client_len = state.client_nonce.len();
    let server_len = state.server_nonce.len();
    if state.client_final_nonce.len() != client_len + server_len {
        return false;
    }
    state.client_final_nonce[..client_len] == state.client_nonce[..]
        && state.client_final_nonce[client_len..] == state.server_nonce[..]
}

fn verify_client_proof(state: &mut ScramState) -> bool {
    let mut ctx = PgHmacCtx::<Sha256>::init(&state.stored_key[..state.key_length]);
    ctx.update(&state.client_first_message_bare);
    ctx.update(b",");
    ctx.update(&state.server_first_message);
    ctx.update(b",");
    ctx.update(&state.client_final_message_without_proof);
    let client_signature = ctx.finalize();

    for i in 0..state.key_length {
        state.client_key[i] = state.client_proof[i] ^ client_signature[i];
    }

    let client_stored_key = scram_h(&state.client_key);
    client_stored_key[..state.key_length] == state.stored_key[..state.key_length]
}

fn build_server_final_message(state: &ScramState) -> Vec<u8> {
    const { assert!(SCRAM_MAX_KEY_LEN == SCRAM_SHA_256_KEY_LEN) };
    let mut ctx = PgHmacCtx::<Sha256>::init(&state.server_key[..state.key_length]);
    ctx.update(&state.client_first_message_bare);
    ctx.update(b",");
    ctx.update(&state.server_first_message);
    ctx.update(b",");
    ctx.update(&state.client_final_message_without_proof);
    let server_signature = ctx.finalize();

    let cap = pg_b64_enc_len(state.key_length as i32);
    let mut encoded = vec![0u8; cap as usize];
    let n = pg_b64_encode(
        &server_signature[..state.key_length],
        state.key_length as i32,
        &mut encoded,
        cap,
    );
    assert!(n >= 0, "could not encode server signature");
    encoded.truncate(n as usize);

    let mut msg = Vec::with_capacity(2 + encoded.len());
    msg.extend_from_slice(b"v=");
    msg.extend_from_slice(&encoded);
    msg
}

#[cfg(test)]
impl ScramState {
    pub(crate) fn test_fields(
        &mut self,
    ) -> (
        &mut Vec<u8>,
        &mut Vec<u8>,
        &mut Vec<u8>,
        &mut [u8; SCRAM_MAX_KEY_LEN],
        &mut [u8; SCRAM_MAX_KEY_LEN],
    ) {
        (
            &mut self.client_first_message_bare,
            &mut self.server_first_message,
            &mut self.client_final_message_without_proof,
            &mut self.client_proof,
            &mut self.stored_key,
        )
    }

    pub(crate) fn doomed(&self) -> bool {
        self.doomed
    }

    pub(crate) fn salt(&self) -> &str {
        &self.salt
    }

    pub(crate) fn iterations(&self) -> i32 {
        self.iterations
    }
}

#[cfg(test)]
pub(crate) fn test_verify_client_proof(state: &mut ScramState) -> bool {
    verify_client_proof(state)
}

#[cfg(test)]
pub(crate) fn test_build_server_final_message(state: &ScramState) -> Vec<u8> {
    build_server_final_message(state)
}

#[cfg(test)]
pub(crate) fn test_scram_init(
    port: &Port,
    selected_mech: &[u8],
    shadow_pass: Option<&str>,
) -> PgResult<ScramState> {
    scram_init(port, selected_mech, shadow_pass)
}
