//! auth-oauth.c: server-side OAUTHBEARER SASL mechanism (RFC 7628) and the
//! validator dispatch. C dlopens validator modules; pgrust resolves the same
//! names against a builtin registry (no-dlopen carve §2, dfmgr precedent) —
//! an unregistered name is C's dlopen stat miss (58P01).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use pgsync::Mutex;

use auth_sasl::{
    SaslMech, PG_SASL_EXCHANGE_CONTINUE, PG_SASL_EXCHANGE_FAILURE, PG_SASL_EXCHANGE_SUCCESS,
};
use elog::ereport;
use stringinfo::StringInfo;
use types_error::{
    ErrorLocation, PgResult, COMMERROR, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_PROTOCOL_VIOLATION, ERRCODE_UNDEFINED_FILE, ERROR, FATAL, LOG, WARNING,
};
use types_startup::Port;

pub const OAUTHBEARER_NAME: &str = "OAUTHBEARER";

pub const PG_MAX_AUTH_TOKEN_LENGTH: i32 = 65535;

const KVSEP: u8 = 0x01;
const AUTH_KEY: &[u8] = b"auth";
const BEARER_SCHEME: &[u8] = b"Bearer ";

const STATUS_OK: i32 = 0;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/libpq/auth-oauth.c", line, func)
}

pub struct ValidatorModuleResult {
    pub authorized: bool,
    pub authn_id: Option<String>,
}

pub trait OAuthValidator: Sync {
    fn startup(&self, _sversion: i32) {}
    fn shutdown(&self) {}
    fn validate(
        &self,
        token: &str,
        role: &str,
        result: &mut ValidatorModuleResult,
    ) -> PgResult<bool>;
}

struct BuiltinValidatorEntry {
    name: &'static str,
    validator: &'static (dyn OAuthValidator + Sync),
}

static BUILTIN_VALIDATORS: Mutex<Vec<BuiltinValidatorEntry>> = Mutex::new(Vec::new());

pub fn register_builtin_validator(name: &'static str, validator: &'static (dyn OAuthValidator + Sync)) {
    let mut vals = BUILTIN_VALIDATORS.lock().unwrap();
    match vals.iter_mut().find(|e| e.name == name) {
        Some(existing) => existing.validator = validator,
        None => vals.push(BuiltinValidatorEntry { name, validator }),
    }
}

// load_validator_library (auth-oauth.c:737): registry miss is C's dlopen
// stat miss — `could not access file "<lib>"`, 58P01 (dfmgr taxonomy).
fn load_validator_library(libname: &str) -> PgResult<&'static (dyn OAuthValidator + Sync)> {
    debug_assert!(!libname.is_empty());
    let found = BUILTIN_VALIDATORS
        .lock()
        .unwrap()
        .iter()
        .find(|e| e.name == libname)
        .map(|e| e.validator);
    let Some(validator) = found else {
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FILE)
            .errmsg(format!(
                "could not access file \"{libname}\": No such file or directory"
            ))
            .finish(loc(751, "load_validator_library"))?;
        unreachable!()
    };
    validator.startup(types_core::fmgr::PG_VERSION_NUM);
    Ok(validator)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OauthState {
    Init,
    Error,
    Finished,
}

pub struct OauthCtx {
    state: OauthState,
    issuer: Option<String>,
    scope: Option<String>,
    validator: &'static (dyn OAuthValidator + Sync),
}

impl Drop for OauthCtx {
    // C runs shutdown_cb from a memory-context reset callback.
    fn drop(&mut self) {
        self.validator.shutdown();
    }
}

pub struct OAuthMech {
    // set_authn_id lives in the auth crate (would cycle auth -> auth_oauth
    // -> auth); injected like CheckSASLAuth's send_auth_request.
    pub set_authn_id: fn(&Port, &str) -> PgResult<()>,
}

impl SaslMech for OAuthMech {
    type State = OauthCtx;

    fn max_message_length(&self) -> i32 {
        PG_MAX_AUTH_TOKEN_LENGTH
    }

    fn get_mechanisms(&self, _port: &Port, buf: &mut StringInfo<'_>) -> PgResult<()> {
        // Only OAUTHBEARER is supported.
        buf.append_str(OAUTHBEARER_NAME)?;
        buf.append_byte(0)?;
        Ok(())
    }

    fn init(
        &self,
        port: &Port,
        selected_mech: &[u8],
        _shadow_pass: Option<&str>,
    ) -> PgResult<OauthCtx> {
        if selected_mech != OAUTHBEARER_NAME.as_bytes() {
            ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("client selected an invalid SASL authentication mechanism")
                .finish(loc(107, "oauth_init"))?;
        }

        let hba = port.hba.as_ref().expect("oauth_init: port->hba is NULL");
        let validator_name = hba
            .oauth_validator
            .as_deref()
            .expect("oauth_validator established by check_oauth_validator");

        Ok(OauthCtx {
            state: OauthState::Init,
            issuer: hba.oauth_issuer.clone(),
            scope: hba.oauth_scope.clone(),
            validator: load_validator_library(validator_name)?,
        })
    }

    fn exchange(
        &self,
        ctx: &mut OauthCtx,
        port: &mut Port,
        input: Option<&[u8]>,
        _logdetail: &mut Option<String>,
    ) -> PgResult<(i32, Option<Vec<u8>>)> {
        oauth_exchange(self, ctx, port, input)
    }
}

fn oauth_exchange(
    mech: &OAuthMech,
    ctx: &mut OauthCtx,
    port: &mut Port,
    input: Option<&[u8]>,
) -> PgResult<(i32, Option<Vec<u8>>)> {
    let func = "oauth_exchange";
    let malformed = |line: i32, detail: String| -> PgResult<()> {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed OAUTHBEARER message")
            .errdetail(detail)
            .finish(loc(line, func))
    };

    let Some(input) = input else {
        debug_assert!(ctx.state == OauthState::Init);
        return Ok((PG_SASL_EXCHANGE_CONTINUE, Some(Vec::new())));
    };

    if input.is_empty() {
        malformed(167, "The message is empty.".to_string())?;
    }
    // C compares inputlen to strlen(input): an interior NUL is a mismatch.
    if input.contains(&0) {
        malformed(172, "Message length does not match input length.".to_string())?;
    }

    match ctx.state {
        OauthState::Init => {} // handled below
        OauthState::Error => {
            if input != [KVSEP] {
                malformed(190, "Client did not send a kvsep response.".to_string())?;
            }
            ctx.state = OauthState::Finished;
            return Ok((PG_SASL_EXCHANGE_FAILURE, None));
        }
        OauthState::Finished => {
            ereport(ERROR)
                .errmsg_internal("invalid OAUTHBEARER exchange state")
                .finish(loc(200, func))?;
            return Ok((PG_SASL_EXCHANGE_FAILURE, None));
        }
    }

    let mut p = input;

    let cbind_flag = p[0];
    match cbind_flag {
        b'p' => {
            malformed(218, "The server does not support channel binding for OAuth, but the client message includes channel binding data.".to_string())?;
        }
        b'y' | b'n' => {
            p = &p[1..];
            if p.first() != Some(&b',') {
                malformed(
                    228,
                    format!(
                        "Comma expected, but found character \"{}\".",
                        sanitize_char(p.first().copied().unwrap_or(0))
                    ),
                )?;
            }
            p = &p[1..];
        }
        c => {
            malformed(
                237,
                format!("Unexpected channel-binding flag \"{}\".", sanitize_char(c)),
            )?;
        }
    }

    if p.first() == Some(&b'a') {
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("client uses authorization identity, but it is not supported")
            .finish(loc(248, func))?;
    }
    if p.first() != Some(&b',') {
        malformed(
            252,
            format!(
                "Unexpected attribute \"{}\" in client-first-message.",
                sanitize_char(p.first().copied().unwrap_or(0))
            ),
        )?;
    }
    p = &p[1..];

    if p.first() != Some(&KVSEP) {
        malformed(
            261,
            format!(
                "Key-value separator expected, but found character \"{}\".",
                sanitize_char(p.first().copied().unwrap_or(0))
            ),
        )?;
    }
    p = &p[1..];

    let (auth, rest) = parse_kvpairs_for_auth(p)?;
    let Some(auth) = auth else {
        malformed(270, "Message does not contain an auth value.".to_string())?;
        unreachable!()
    };

    if !rest.is_empty() {
        malformed(
            277,
            "Message contains additional data after the final terminator.".to_string(),
        )?;
    }

    if !validate(mech, ctx, port, auth)? {
        let output = generate_error_response(ctx)?;
        ctx.state = OauthState::Error;
        Ok((PG_SASL_EXCHANGE_CONTINUE, Some(output)))
    } else {
        ctx.state = OauthState::Finished;
        Ok((PG_SASL_EXCHANGE_SUCCESS, None))
    }
}

fn sanitize_char(c: u8) -> String {
    if (0x21..=0x7e).contains(&c) {
        format!("'{}'", c as char)
    } else {
        format!("0x{c:02x}")
    }
}

fn validate_kvpair(key: &[u8], val: &[u8]) -> PgResult<()> {
    let malformed = |line: i32, detail: &str| -> PgResult<()> {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed OAUTHBEARER message")
            .errdetail(detail)
            .finish(loc(line, "validate_kvpair"))
    };

    if key.is_empty() {
        malformed(340, "Message contains an empty key name.")?;
    }
    if !key.iter().all(u8::is_ascii_alphabetic) {
        malformed(347, "Message contains an invalid key name.")?;
    }

    for &c in val {
        let ok = (0x21..=0x7e).contains(&c) || matches!(c, b' ' | b'\t' | b'\r' | b'\n');
        if !ok {
            malformed(373, "Message contains an invalid value.")?;
        }
    }
    Ok(())
}

fn parse_kvpairs_for_auth(input: &[u8]) -> PgResult<(Option<&[u8]>, &[u8])> {
    let malformed = |line: i32, detail: &str| -> PgResult<()> {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed OAUTHBEARER message")
            .errdetail(detail)
            .finish(loc(line, "parse_kvpairs_for_auth"))
    };

    let mut pos = input;
    let mut auth: Option<&[u8]> = None;

    while !pos.is_empty() {
        let Some(end) = pos.iter().position(|&b| b == KVSEP) else {
            malformed(418, "Message contains an unterminated key/value pair.")?;
            unreachable!()
        };
        let pair = &pos[..end];

        if pair.is_empty() {
            return Ok((auth, &pos[end + 1..]));
        }

        let Some(sep) = pair.iter().position(|&b| b == b'=') else {
            malformed(436, "Message contains a key without a value.")?;
            unreachable!()
        };
        let (key, value) = (&pair[..sep], &pair[sep + 1..]);
        validate_kvpair(key, value)?;

        if key == AUTH_KEY {
            if auth.is_some() {
                malformed(450, "Message contains multiple auth values.")?;
            }
            auth = Some(value);
        }

        pos = &pos[end + 1..];
    }

    malformed(470, "Message did not contain a final terminator.")?;
    unreachable!()
}

fn generate_error_response(ctx: &OauthCtx) -> PgResult<Vec<u8>> {
    let (Some(issuer), Some(scope)) = (ctx.issuer.as_deref(), ctx.scope.as_deref()) else {
        ereport(FATAL)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("OAuth is not properly configured for this user")
            .errdetail_log("The issuer and scope parameters must be set in pg_hba.conf.")
            .finish(loc(497, "generate_error_response"))?;
        unreachable!()
    };

    let mut full_issuer = issuer.to_string();
    if !issuer.contains("/.well-known/") {
        full_issuer.push_str("/.well-known/openid-configuration");
    }

    let mut buf = String::new();
    buf.push_str("{ \"status\": \"invalid_token\", ");
    buf.push_str("\"openid-configuration\": ");
    escape_json(&mut buf, &full_issuer);
    buf.push_str(", \"scope\": ");
    escape_json(&mut buf, scope);
    buf.push_str(" }");

    Ok(buf.into_bytes())
}

fn escape_json(buf: &mut String, s: &str) {
    buf.push('"');
    for c in s.chars() {
        match c {
            '\u{8}' => buf.push_str("\\b"),
            '\u{c}' => buf.push_str("\\f"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            c if (c as u32) < 0x20 => buf.push_str(&format!("\\u{:04x}", c as u32)),
            c => buf.push(c),
        }
    }
    buf.push('"');
}

fn validate_token_format(header: &[u8]) -> PgResult<Option<&[u8]>> {
    let commerror = |line: i32, detail: &str| -> PgResult<()> {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("malformed OAuth bearer token")
            .errdetail_log(detail)
            .finish(loc(line, "validate_token_format"))
    };

    if header.is_empty() {
        return Ok(None);
    }

    if header.len() < BEARER_SCHEME.len()
        || !header[..BEARER_SCHEME.len()].eq_ignore_ascii_case(BEARER_SCHEME)
    {
        commerror(
            583,
            "Client response indicated a non-Bearer authentication scheme.",
        )?;
        return Ok(None);
    }

    let mut token = &header[BEARER_SCHEME.len()..];
    while token.first() == Some(&b' ') {
        token = &token[1..];
    }

    if token.is_empty() {
        commerror(600, "Bearer token is empty.")?;
        return Ok(None);
    }

    let mut span = token
        .iter()
        .position(|&c| !(c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')))
        .unwrap_or(token.len());
    while token.get(span) == Some(&b'=') {
        span += 1;
    }
    if span != token.len() {
        commerror(622, "Bearer token is not in the correct format.")?;
        return Ok(None);
    }

    Ok(Some(token))
}

fn validate(mech: &OAuthMech, ctx: &OauthCtx, port: &Port, auth: &[u8]) -> PgResult<bool> {
    let Some(token) = validate_token_format(auth)? else {
        return Ok(false);
    };
    // Token bytes are within the validated b64token set: always UTF-8.
    let token = std::str::from_utf8(token).expect("b64token is ASCII");

    let user_name = port.user_name.as_deref().unwrap_or("");
    let hba = port.hba.as_ref().expect("validate: port->hba is NULL");

    let mut ret = ValidatorModuleResult {
        authorized: false,
        authn_id: None,
    };
    match ctx.validator.validate(token, user_name, &mut ret) {
        Ok(true) => {}
        Ok(false) => {
            ereport(WARNING)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg("internal error in OAuth validator module")
                .finish(loc(663, "validate"))?;
            return Ok(false);
        }
        Err(e) => return Err(e),
    }

    if let Some(authn_id) = ret.authn_id.as_deref() {
        (mech.set_authn_id)(port, authn_id)?;
    }

    if !ret.authorized {
        ereport(LOG)
            .errmsg(format!(
                "OAuth bearer authentication failed for user \"{user_name}\""
            ))
            .errdetail_log("Validator failed to authorize the provided token.")
            .finish(loc(678, "validate"))?;
        return Ok(false);
    }

    if hba.oauth_skip_usermap {
        return Ok(true);
    }

    if ret.authn_id.as_deref().map_or(true, str::is_empty) {
        ereport(LOG)
            .errmsg(format!(
                "OAuth bearer authentication failed for user \"{user_name}\""
            ))
            .errdetail_log("Validator provided no identity.")
            .finish(loc(702, "validate"))?;
        return Ok(false);
    }

    let (authn_id, _) = miscinit::client_connection_info();
    let map_status = hba::check_usermap(
        hba.usermap.as_deref(),
        user_name,
        authn_id.expect("authn_id set above"),
        false,
    )?;
    Ok(map_status == STATUS_OK)
}

// Builtin deterministic test validator (C core ships none); reachable only
// when named by oauth_validator_libraries + the hba validator option.
// Tokens: "valid-<id>" authorized as <id>; "noauthz-<id>" denied as <id>;
// "noident" authorized w/o identity; "modulefail" module error; else denied.

pub const TEST_VALIDATOR_NAME: &str = "oauth_test_validator";

struct TestValidator;

impl OAuthValidator for TestValidator {
    fn validate(
        &self,
        token: &str,
        role: &str,
        result: &mut ValidatorModuleResult,
    ) -> PgResult<bool> {
        elog::elog(
            LOG,
            &format!("oauth_test_validator: token=\"{token}\", role=\"{role}\""),
        )?;

        if token == "modulefail" {
            return Ok(false);
        }
        if let Some(id) = token.strip_prefix("valid-") {
            result.authorized = true;
            result.authn_id = Some(id.to_string());
        } else if let Some(id) = token.strip_prefix("noauthz-") {
            result.authorized = false;
            result.authn_id = Some(id.to_string());
        } else if token == "noident" {
            result.authorized = true;
            result.authn_id = None;
        }
        Ok(true)
    }
}

static TEST_VALIDATOR: TestValidator = TestValidator;

pub fn init_seams() {
    register_builtin_validator(TEST_VALIDATOR_NAME, &TEST_VALIDATOR);
}

#[cfg(test)]
mod tests;
