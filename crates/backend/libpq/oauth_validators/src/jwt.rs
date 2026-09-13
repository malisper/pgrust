// JWS compact parsing (RFC 7515), JWK-to-key conversion (RFC 7517/7518) and
// the claim policy shared by the JWT and introspection paths (RFC 7519 §4,
// RFC 7662 §2.2).

use openssl::bn::BigNum;
use openssl::ec::{EcGroup, EcKey};
use openssl::ecdsa::EcdsaSig;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Public};
use openssl::rsa::{Padding, Rsa};
use openssl::sign::{RsaPssSaltlen, Verifier};

use crate::json::{b64url_decode, parse, Json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Alg {
    Rs256,
    Rs384,
    Rs512,
    Ps256,
    Ps384,
    Ps512,
    Es256,
    Es384,
}

impl Alg {
    pub fn from_name(name: &str) -> Option<Alg> {
        Some(match name {
            "RS256" => Alg::Rs256,
            "RS384" => Alg::Rs384,
            "RS512" => Alg::Rs512,
            "PS256" => Alg::Ps256,
            "PS384" => Alg::Ps384,
            "PS512" => Alg::Ps512,
            "ES256" => Alg::Es256,
            "ES384" => Alg::Es384,
            _ => return None,
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            Alg::Rs256 => "RS256",
            Alg::Rs384 => "RS384",
            Alg::Rs512 => "RS512",
            Alg::Ps256 => "PS256",
            Alg::Ps384 => "PS384",
            Alg::Ps512 => "PS512",
            Alg::Es256 => "ES256",
            Alg::Es384 => "ES384",
        }
    }
    fn digest(self) -> MessageDigest {
        match self {
            Alg::Rs256 | Alg::Ps256 | Alg::Es256 => MessageDigest::sha256(),
            Alg::Rs384 | Alg::Ps384 | Alg::Es384 => MessageDigest::sha384(),
            Alg::Rs512 | Alg::Ps512 => MessageDigest::sha512(),
        }
    }
    pub fn kty(self) -> &'static str {
        match self {
            Alg::Es256 | Alg::Es384 => "EC",
            _ => "RSA",
        }
    }
    fn ec_coord_len(self) -> usize {
        match self {
            Alg::Es256 => 32,
            Alg::Es384 => 48,
            _ => 0,
        }
    }
}

pub struct Jwk {
    pub kid: Option<String>,
    pub alg: Option<String>,
    pub kty: String,
    pub crv: Option<String>,
    pub key: PKey<Public>,
}

impl Jwk {
    pub fn usable_for(&self, alg: Alg) -> bool {
        if self.kty != alg.kty() {
            return false;
        }
        if let Some(a) = &self.alg {
            if a != alg.name() {
                return false;
            }
        }
        match alg {
            Alg::Es256 => self.crv.as_deref() == Some("P-256"),
            Alg::Es384 => self.crv.as_deref() == Some("P-384"),
            _ => true,
        }
    }
}

fn bn(obj: &Json, field: &str) -> Result<BigNum, String> {
    let s = obj
        .get(field)
        .and_then(Json::as_str)
        .ok_or_else(|| format!("JWK missing \"{field}\""))?;
    let bytes = b64url_decode(s).map_err(|e| format!("JWK \"{field}\": {e}"))?;
    BigNum::from_slice(&bytes).map_err(|e| format!("JWK \"{field}\": {e}"))
}

pub fn jwk_from_json(obj: &Json) -> Result<Jwk, String> {
    let kty = obj
        .get("kty")
        .and_then(Json::as_str)
        .ok_or("JWK missing \"kty\"")?
        .to_string();
    if let Some(u) = obj.get("use").and_then(Json::as_str) {
        if u != "sig" {
            return Err(format!("JWK \"use\" is \"{u}\", not \"sig\""));
        }
    }
    let crv = obj.get("crv").and_then(Json::as_str).map(str::to_string);
    let key = match kty.as_str() {
        "RSA" => {
            let rsa = Rsa::from_public_components(bn(obj, "n")?, bn(obj, "e")?)
                .map_err(|e| format!("bad RSA JWK: {e}"))?;
            PKey::from_rsa(rsa).map_err(|e| e.to_string())?
        }
        "EC" => {
            let nid = match crv.as_deref() {
                Some("P-256") => Nid::X9_62_PRIME256V1,
                Some("P-384") => Nid::SECP384R1,
                other => return Err(format!("unsupported EC curve {other:?}")),
            };
            let group = EcGroup::from_curve_name(nid).map_err(|e| e.to_string())?;
            let (x, y) = (bn(obj, "x")?, bn(obj, "y")?);
            let ec = EcKey::from_public_key_affine_coordinates(&group, &x, &y)
                .map_err(|e| format!("bad EC JWK: {e}"))?;
            ec.check_key().map_err(|e| format!("bad EC JWK: {e}"))?;
            PKey::from_ec_key(ec).map_err(|e| e.to_string())?
        }
        other => return Err(format!("unsupported JWK kty \"{other}\"")),
    };
    Ok(Jwk {
        kid: obj.get("kid").and_then(Json::as_str).map(str::to_string),
        alg: obj.get("alg").and_then(Json::as_str).map(str::to_string),
        kty,
        crv,
        key,
    })
}

// Unusable entries (encryption keys, unknown kty/crv) are skipped: a JWKS
// commonly mixes them with the signing keys.
pub fn parse_jwks(text: &str) -> Result<(Vec<Jwk>, Vec<String>), String> {
    let doc = parse(text)?;
    let keys = doc.get("keys").and_then(Json::as_arr).ok_or("JWKS has no \"keys\" array")?;
    let mut out = Vec::new();
    let mut skipped = Vec::new();
    for k in keys {
        match jwk_from_json(k) {
            Ok(jwk) => out.push(jwk),
            Err(e) => skipped.push(e),
        }
    }
    Ok((out, skipped))
}

#[derive(Debug)]
pub struct Jws<'a> {
    pub claims: Json,
    pub alg: Alg,
    pub kid: Option<String>,
    signing_input: &'a [u8],
    signature: Vec<u8>,
}

pub fn looks_like_jws(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    parts.len() == 3
        && !parts[0].is_empty()
        && b64url_decode(parts[0])
            .ok()
            .and_then(|h| String::from_utf8(h).ok())
            .and_then(|h| parse(&h).ok())
            .is_some_and(|h| h.get("alg").is_some())
}

pub fn parse_jws(token: &str) -> Result<Jws<'_>, String> {
    let mut parts = token.splitn(4, '.');
    let (h, p, s) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(h), Some(p), Some(s), None) => (h, p, s),
        _ => return Err("token is not a three-part JWS".into()),
    };
    let header_bytes = b64url_decode(h).map_err(|e| format!("JWS header: {e}"))?;
    let header = parse(std::str::from_utf8(&header_bytes).map_err(|_| "JWS header is not UTF-8")?)
        .map_err(|e| format!("JWS header: {e}"))?;
    if !header.is_obj() {
        return Err("JWS header is not an object".into());
    }
    let alg_name = header.get("alg").and_then(Json::as_str).ok_or("JWS header has no \"alg\"")?;
    let alg = Alg::from_name(alg_name).ok_or_else(|| format!("unsupported JWS algorithm \"{alg_name}\""))?;
    if let Some(typ) = header.get("typ").and_then(Json::as_str) {
        if !matches!(typ.to_ascii_lowercase().as_str(), "jwt" | "at+jwt" | "application/at+jwt") {
            return Err(format!("unsupported JWS type \"{typ}\""));
        }
    }
    if header.get("crit").is_some() {
        return Err("JWS \"crit\" header extensions are not supported".into());
    }
    let claims_bytes = b64url_decode(p).map_err(|e| format!("JWS payload: {e}"))?;
    let claims = parse(std::str::from_utf8(&claims_bytes).map_err(|_| "JWS payload is not UTF-8")?)
        .map_err(|e| format!("JWS payload: {e}"))?;
    if !claims.is_obj() {
        return Err("JWS payload is not an object".into());
    }
    let signature = b64url_decode(s).map_err(|e| format!("JWS signature: {e}"))?;
    if signature.is_empty() {
        return Err("JWS signature is empty".into());
    }
    Ok(Jws {
        kid: header.get("kid").and_then(Json::as_str).map(str::to_string),
        claims,
        alg,
        signing_input: &token.as_bytes()[..h.len() + 1 + p.len()],
        signature,
    })
}

pub fn verify_signature(jws: &Jws<'_>, key: &PKey<Public>) -> Result<bool, String> {
    let sig: Vec<u8> = match jws.alg {
        Alg::Es256 | Alg::Es384 => {
            let n = jws.alg.ec_coord_len();
            if jws.signature.len() != 2 * n {
                return Ok(false);
            }
            let r = BigNum::from_slice(&jws.signature[..n]).map_err(|e| e.to_string())?;
            let s = BigNum::from_slice(&jws.signature[n..]).map_err(|e| e.to_string())?;
            EcdsaSig::from_private_components(r, s)
                .and_then(|sig| sig.to_der())
                .map_err(|e| e.to_string())?
        }
        _ => jws.signature.clone(),
    };
    let mut v = Verifier::new(jws.alg.digest(), key).map_err(|e| e.to_string())?;
    if matches!(jws.alg, Alg::Ps256 | Alg::Ps384 | Alg::Ps512) {
        v.set_rsa_padding(Padding::PKCS1_PSS).map_err(|e| e.to_string())?;
        v.set_rsa_pss_saltlen(RsaPssSaltlen::DIGEST_LENGTH).map_err(|e| e.to_string())?;
    }
    v.update(jws.signing_input).map_err(|e| e.to_string())?;
    v.verify(&sig).map_err(|e| e.to_string())
}

// Which JWKS entries may sign this token: the `kid` match when the header
// names one, otherwise every key of the algorithm's family.
pub fn candidate_keys<'k>(jws: &Jws<'_>, keys: &'k [Jwk]) -> Vec<&'k Jwk> {
    keys.iter()
        .filter(|k| k.usable_for(jws.alg))
        .filter(|k| match &jws.kid {
            Some(kid) => k.kid.as_deref() == Some(kid),
            None => true,
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct Policy<'a> {
    pub issuer: &'a str,
    pub audiences: &'a [String],
    pub required_scopes: &'a [&'a str],
    pub identity_claim: &'a str,
    pub clock_skew: i64,
    pub now: i64,
}

// `strict` = JWT: iss/aud/exp are mandatory. Introspection responses carry
// them optionally (RFC 7662 §2.2) and are checked only where present.
pub fn check_claims(claims: &Json, policy: &Policy<'_>, strict: bool) -> Result<String, String> {
    match claims.get("iss").and_then(Json::as_str) {
        Some(iss) if iss == policy.issuer => {}
        Some(iss) => return Err(format!("issuer \"{iss}\" does not match \"{}\"", policy.issuer)),
        None if strict => return Err("token has no \"iss\" claim".into()),
        None => {}
    }
    match claims.get("aud") {
        Some(Json::Str(a)) => {
            if !policy.audiences.iter().any(|want| want == a) {
                return Err(format!("audience \"{a}\" is not accepted"));
            }
        }
        Some(Json::Arr(items)) => {
            let ok = items
                .iter()
                .filter_map(Json::as_str)
                .any(|a| policy.audiences.iter().any(|want| want == a));
            if !ok {
                return Err("none of the token's audiences is accepted".into());
            }
        }
        None | Some(Json::Null) if strict => return Err("token has no \"aud\" claim".into()),
        None | Some(Json::Null) => {}
        Some(_) => return Err("\"aud\" claim is not a string or array".into()),
    }
    let time_claim = |name: &str| -> Result<Option<i64>, String> {
        match claims.get(name) {
            None | Some(Json::Null) => Ok(None),
            Some(v) => v.as_i64().map(Some).ok_or_else(|| format!("\"{name}\" claim is not a number")),
        }
    };
    match time_claim("exp")? {
        Some(exp) if exp + policy.clock_skew <= policy.now => {
            return Err(format!("token expired at {exp} (now {})", policy.now));
        }
        Some(_) => {}
        None if strict => return Err("token has no \"exp\" claim".into()),
        None => {}
    }
    if let Some(nbf) = time_claim("nbf")? {
        if nbf - policy.clock_skew > policy.now {
            return Err(format!("token not valid before {nbf} (now {})", policy.now));
        }
    }
    if let Some(iat) = time_claim("iat")? {
        if iat - policy.clock_skew > policy.now {
            return Err(format!("token issued in the future at {iat} (now {})", policy.now));
        }
    }
    if !policy.required_scopes.is_empty() {
        let granted: Vec<String> = match (claims.get("scope"), claims.get("scp")) {
            (Some(Json::Str(s)), _) => s.split_ascii_whitespace().map(str::to_string).collect(),
            (Some(Json::Arr(a)), _) | (None, Some(Json::Arr(a))) => {
                a.iter().filter_map(Json::as_str).map(str::to_string).collect()
            }
            (None, Some(Json::Str(s))) => s.split_ascii_whitespace().map(str::to_string).collect(),
            (None, None) => return Err("token has no \"scope\" claim".into()),
            _ => return Err("\"scope\" claim is not a string or array".into()),
        };
        if let Some(missing) = policy.required_scopes.iter().find(|s| !granted.iter().any(|g| g == *s)) {
            return Err(format!("token lacks required scope \"{missing}\""));
        }
    }
    let identity = claims
        .get(policy.identity_claim)
        .and_then(Json::as_str)
        .filter(|s| !s.is_empty());
    let identity = match identity {
        Some(id) => id,
        None if !strict => claims
            .get("username")
            .and_then(Json::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("response has no \"{}\" or \"username\" field", policy.identity_claim))?,
        None => return Err(format!("token has no \"{}\" claim", policy.identity_claim)),
    };
    Ok(identity.to_string())
}

// RFC 8414 §3 / OIDC Discovery §4: the issuer identifier behind an HBA
// issuer that is spelled as a discovery URL.
pub fn issuer_identifier(hba_issuer: &str) -> String {
    let Some(i) = hba_issuer.find("/.well-known/") else {
        return hba_issuer.trim_end_matches('/').to_string();
    };
    let origin = &hba_issuer[..i];
    let rest = &hba_issuer[i + "/.well-known/".len()..];
    let suffix = rest
        .strip_prefix("openid-configuration")
        .or_else(|| rest.strip_prefix("oauth-authorization-server"))
        .unwrap_or("");
    format!("{origin}{}", suffix.trim_end_matches('/'))
}

pub fn discovery_url(hba_issuer: &str) -> String {
    if hba_issuer.contains("/.well-known/") {
        hba_issuer.to_string()
    } else {
        format!("{}/.well-known/openid-configuration", hba_issuer.trim_end_matches('/'))
    }
}
