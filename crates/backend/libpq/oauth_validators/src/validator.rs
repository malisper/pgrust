use std::time::Duration;

use auth_oauth::{OAuthValidator, ValidatorEnv, ValidatorModuleResult};
use elog::ereport;
use openssl::pkey::{PKey, Public};
use pg_clock::MonoStamp;
use pgsync::Mutex;
use types_error::{ErrorLocation, PgResult, LOG, WARNING};

use crate::http::{self, HttpOptions};
use crate::json::{self, b64_std_encode, Json};
use crate::jwt::{self, Jwk, Policy};

pub const JWT_VALIDATOR_NAME: &str = "jwt_validator";

pub struct JwtValidator;
pub static JWT_VALIDATOR: JwtValidator = JwtValidator;

// Refreshes triggered by an unknown `kid` are rate-limited so a flood of
// forged tokens cannot turn into a flood of JWKS fetches.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

struct Config {
    jwks_uri: String,
    audiences: Vec<String>,
    identity_claim: String,
    introspection_uri: String,
    client_id: String,
    client_secret: String,
    require_scopes: bool,
    clock_skew: i64,
    jwks_cache_ttl: Duration,
    http: HttpOptions,
}

fn config() -> Config {
    use guc_tables::backing as g;
    let s = |v: Option<String>| v.unwrap_or_default().trim().to_string();
    let audiences = s(g::jwt_validator_audience())
        .split(',')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_string)
        .collect();
    Config {
        jwks_uri: s(g::jwt_validator_jwks_uri()),
        audiences,
        identity_claim: s(g::jwt_validator_identity_claim()),
        introspection_uri: s(g::jwt_validator_introspection_uri()),
        client_id: g::jwt_validator_introspection_client_id().unwrap_or_default(),
        client_secret: g::jwt_validator_introspection_client_secret().unwrap_or_default(),
        require_scopes: g::jwt_validator_require_scopes(),
        clock_skew: i64::from(g::jwt_validator_clock_skew()),
        jwks_cache_ttl: Duration::from_secs(u64::try_from(g::jwt_validator_jwks_cache_ttl()).unwrap_or(0)),
        http: HttpOptions {
            allow_insecure_http: g::jwt_validator_allow_insecure_http(),
            ca_file: Some(s(g::jwt_validator_ca_file())).filter(|f| !f.is_empty()),
            timeout: Duration::from_secs(u64::try_from(g::jwt_validator_http_timeout()).unwrap_or(10)),
        },
    }
}

pub enum Failure {
    // The token is bad: authentication fails, reason goes to the log.
    Reject(String),
    // The validator itself could not do its job (config, network, IdP).
    Internal(String),
}

struct CachedJwks {
    uri: String,
    fetched_at: MonoStamp,
    keys: Vec<Jwk>,
}

struct CachedDiscovery {
    issuer: String,
    fetched_at: MonoStamp,
    jwks_uri: String,
}

static JWKS_CACHE: Mutex<Vec<CachedJwks>> = Mutex::new(Vec::new());
static DISCOVERY_CACHE: Mutex<Vec<CachedDiscovery>> = Mutex::new(Vec::new());

fn now_unix() -> i64 {
    pg_clock::wall_secs()
}

fn fetch_json(uri: &str, opts: &HttpOptions) -> Result<Json, String> {
    let url = http::parse_url(uri)?;
    let resp = http::get(&url, opts)?;
    if resp.status != 200 {
        return Err(format!("GET {uri} returned HTTP {}", resp.status));
    }
    let text = std::str::from_utf8(&resp.body).map_err(|_| format!("GET {uri}: body is not UTF-8"))?;
    json::parse(text).map_err(|e| format!("GET {uri}: {e}"))
}

fn resolve_jwks_uri(hba_issuer: &str, cfg: &Config) -> Result<String, String> {
    if !cfg.jwks_uri.is_empty() {
        return Ok(cfg.jwks_uri.clone());
    }
    {
        let cache = DISCOVERY_CACHE.lock().unwrap();
        if let Some(e) = cache.iter().find(|e| e.issuer == hba_issuer) {
            if Duration::from_nanos(e.fetched_at.elapsed_ns()) < cfg.jwks_cache_ttl {
                return Ok(e.jwks_uri.clone());
            }
        }
    }
    let url = jwt::discovery_url(hba_issuer);
    let doc = fetch_json(&url, &cfg.http)?;
    let jwks_uri = doc
        .get("jwks_uri")
        .and_then(Json::as_str)
        .ok_or_else(|| format!("discovery document at {url} has no \"jwks_uri\""))?
        .to_string();
    let mut cache = DISCOVERY_CACHE.lock().unwrap();
    cache.retain(|e| e.issuer != hba_issuer);
    cache.push(CachedDiscovery { issuer: hba_issuer.to_string(), fetched_at: MonoStamp::now(), jwks_uri: jwks_uri.clone() });
    Ok(jwks_uri)
}

fn refresh_jwks(uri: &str, opts: &HttpOptions) -> Result<(), String> {
    let url = http::parse_url(uri)?;
    let resp = http::get(&url, opts)?;
    if resp.status != 200 {
        return Err(format!("GET {uri} returned HTTP {}", resp.status));
    }
    let text = std::str::from_utf8(&resp.body).map_err(|_| format!("GET {uri}: body is not UTF-8"))?;
    let (keys, skipped) = jwt::parse_jwks(text).map_err(|e| format!("GET {uri}: {e}"))?;
    for why in skipped {
        let _ = ereport(LOG)
            .errmsg(format!("jwt_validator: skipping unusable key in JWKS {uri}"))
            .errdetail_log(why)
            .finish(ErrorLocation::new(file!(), line!() as i32, "refresh_jwks"));
    }
    if keys.is_empty() {
        return Err(format!("JWKS at {uri} contains no usable signing keys"));
    }
    let mut cache = JWKS_CACHE.lock().unwrap();
    cache.retain(|e| e.uri != uri);
    cache.push(CachedJwks { uri: uri.to_string(), fetched_at: MonoStamp::now(), keys });
    Ok(())
}

// (candidate keys, age of the cache entry they came from)
fn candidate_keys(uri: &str, jws: &jwt::Jws<'_>) -> Option<(Vec<PKey<Public>>, Duration)> {
    let cache = JWKS_CACHE.lock().unwrap();
    let entry = cache.iter().find(|e| e.uri == uri)?;
    let keys = jwt::candidate_keys(jws, &entry.keys).into_iter().map(|k| k.key.clone()).collect();
    Some((keys, Duration::from_nanos(entry.fetched_at.elapsed_ns())))
}

fn policy_scopes<'a>(env: ValidatorEnv<'a>, cfg: &Config) -> Vec<&'a str> {
    if !cfg.require_scopes {
        return Vec::new();
    }
    env.scope.unwrap_or("").split_ascii_whitespace().collect()
}

fn validate_jws(env: ValidatorEnv<'_>, hba_issuer: &str, token: &str, cfg: &Config) -> Result<String, Failure> {
    let jws = jwt::parse_jws(token).map_err(Failure::Reject)?;
    let uri = resolve_jwks_uri(hba_issuer, cfg).map_err(Failure::Internal)?;

    let mut keys = match candidate_keys(&uri, &jws) {
        Some((keys, age)) if age < cfg.jwks_cache_ttl => keys,
        _ => {
            refresh_jwks(&uri, &cfg.http).map_err(Failure::Internal)?;
            candidate_keys(&uri, &jws).map(|(k, _)| k).unwrap_or_default()
        }
    };
    if keys.is_empty() {
        // Key rotation: an unknown kid earns one fresh fetch.
        let stale = candidate_keys(&uri, &jws).is_some_and(|(_, age)| age >= MIN_REFRESH_INTERVAL);
        if stale {
            refresh_jwks(&uri, &cfg.http).map_err(Failure::Internal)?;
            keys = candidate_keys(&uri, &jws).map(|(k, _)| k).unwrap_or_default();
        }
    }
    if keys.is_empty() {
        return Err(Failure::Reject(match &jws.kid {
            Some(kid) => format!("no {} key with kid \"{kid}\" in JWKS {uri}", jws.alg.name()),
            None => format!("no {} key in JWKS {uri}", jws.alg.name()),
        }));
    }
    let mut verified = false;
    for key in &keys {
        if jwt::verify_signature(&jws, key).map_err(Failure::Internal)? {
            verified = true;
            break;
        }
    }
    if !verified {
        return Err(Failure::Reject(format!("{} signature verification failed", jws.alg.name())));
    }
    let scopes = policy_scopes(env, cfg);
    let policy = Policy {
        issuer: &jwt::issuer_identifier(hba_issuer),
        audiences: &cfg.audiences,
        required_scopes: &scopes,
        identity_claim: &cfg.identity_claim,
        clock_skew: cfg.clock_skew,
        now: now_unix(),
    };
    jwt::check_claims(&jws.claims, &policy, true).map_err(Failure::Reject)
}

fn form_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn introspect(env: ValidatorEnv<'_>, hba_issuer: &str, token: &str, cfg: &Config) -> Result<String, Failure> {
    if cfg.introspection_uri.is_empty() {
        return Err(Failure::Reject(
            "token is not a JWT and jwt_validator.introspection_uri is not set".into(),
        ));
    }
    let url = http::parse_url(&cfg.introspection_uri).map_err(Failure::Internal)?;
    let body = format!("token={}&token_type_hint=access_token", form_encode(token));
    let auth = (!cfg.client_id.is_empty())
        .then(|| b64_std_encode(format!("{}:{}", form_encode(&cfg.client_id), form_encode(&cfg.client_secret)).as_bytes()));
    let resp = http::post_form(&url, &body, auth.as_deref(), &cfg.http).map_err(Failure::Internal)?;
    if resp.status != 200 {
        return Err(Failure::Internal(format!(
            "introspection endpoint {} returned HTTP {}",
            cfg.introspection_uri, resp.status
        )));
    }
    let text = std::str::from_utf8(&resp.body)
        .map_err(|_| Failure::Internal("introspection response is not UTF-8".into()))?;
    let doc = json::parse(text).map_err(|e| Failure::Internal(format!("introspection response: {e}")))?;
    if !doc.is_obj() {
        return Err(Failure::Internal("introspection response is not a JSON object".into()));
    }
    if doc.get("active").and_then(Json::as_bool) != Some(true) {
        return Err(Failure::Reject("introspection reports the token as not active".into()));
    }
    let scopes = policy_scopes(env, cfg);
    let policy = Policy {
        issuer: &jwt::issuer_identifier(hba_issuer),
        audiences: &cfg.audiences,
        required_scopes: &scopes,
        identity_claim: &cfg.identity_claim,
        clock_skew: cfg.clock_skew,
        now: now_unix(),
    };
    jwt::check_claims(&doc, &policy, false).map_err(Failure::Reject)
}

pub fn validate_token(env: ValidatorEnv<'_>, token: &str) -> Result<String, Failure> {
    let cfg = config();
    if cfg.audiences.is_empty() {
        return Err(Failure::Internal("jwt_validator.audience is not set".into()));
    }
    if cfg.identity_claim.is_empty() {
        return Err(Failure::Internal("jwt_validator.identity_claim is empty".into()));
    }
    let hba_issuer = env.issuer.ok_or_else(|| Failure::Internal("HBA line has no issuer".into()))?;
    if jwt::looks_like_jws(token) {
        validate_jws(env, hba_issuer, token, &cfg)
    } else {
        introspect(env, hba_issuer, token, &cfg)
    }
}

impl OAuthValidator for JwtValidator {
    fn startup(&self, _sversion: i32) {
        if guc_tables::backing::jwt_validator_allow_insecure_http() {
            let _ = ereport(WARNING)
                .errmsg("jwt_validator.allow_insecure_http is on: JWKS and introspection traffic may travel unencrypted")
                .finish(ErrorLocation::new(file!(), line!() as i32, "startup"));
        }
    }

    fn validate(
        &self,
        env: ValidatorEnv<'_>,
        token: &str,
        role: &str,
        result: &mut ValidatorModuleResult,
    ) -> PgResult<bool> {
        match validate_token(env, token) {
            Ok(identity) => {
                result.authorized = true;
                result.authn_id = Some(identity);
                Ok(true)
            }
            Err(Failure::Reject(reason)) => {
                ereport(LOG)
                    .errmsg(format!("jwt_validator: rejected bearer token for role \"{role}\""))
                    .errdetail_log(reason)
                    .finish(ErrorLocation::new(file!(), line!() as i32, "validate"))?;
                result.authorized = false;
                result.authn_id = None;
                Ok(true)
            }
            Err(Failure::Internal(what)) => {
                ereport(LOG)
                    .errmsg(format!("jwt_validator: could not validate bearer token for role \"{role}\""))
                    .errdetail_log(what)
                    .finish(ErrorLocation::new(file!(), line!() as i32, "validate"))?;
                Ok(false)
            }
        }
    }
}
