use super::*;

fn kv_auth(input: &[u8]) -> Result<(Option<Vec<u8>>, Vec<u8>), String> {
    match parse_kvpairs_for_auth(input) {
        Ok((auth, rest)) => Ok((auth.map(<[u8]>::to_vec), rest.to_vec())),
        Err(e) => Err(e.detail().unwrap_or_default().to_string()),
    }
}

#[test]
fn kvpairs_extract_auth() {
    let (auth, rest) = kv_auth(b"auth=Bearer tok\x01host=example.com\x01\x01").unwrap();
    assert_eq!(auth.as_deref(), Some(&b"Bearer tok"[..]));
    assert!(rest.is_empty());

    // Unknown keys are ignored; empty pair terminates.
    let (auth, rest) = kv_auth(b"host=h\x01\x01trailing").unwrap();
    assert_eq!(auth, None);
    assert_eq!(rest, b"trailing");
}

#[test]
fn kvpairs_malformed() {
    assert_eq!(
        kv_auth(b"auth=Bearer tok\x01").err().unwrap(),
        "Message did not contain a final terminator."
    );
    assert_eq!(
        kv_auth(b"auth=Bearer tok").err().unwrap(),
        "Message contains an unterminated key/value pair."
    );
    assert_eq!(
        kv_auth(b"noequals\x01\x01").err().unwrap(),
        "Message contains a key without a value."
    );
    assert_eq!(
        kv_auth(b"auth=a\x01auth=b\x01\x01").err().unwrap(),
        "Message contains multiple auth values."
    );
    assert_eq!(
        kv_auth(b"au7th=a\x01\x01").err().unwrap(),
        "Message contains an invalid key name."
    );
    assert_eq!(
        kv_auth(b"=a\x01\x01").err().unwrap(),
        "Message contains an empty key name."
    );
    assert_eq!(
        kv_auth(b"auth=a\x7fb\x01\x01").err().unwrap(),
        "Message contains an invalid value."
    );
}

#[test]
fn sanitize_char_matches_c() {
    assert_eq!(sanitize_char(b'x'), "'x'");
    assert_eq!(sanitize_char(b'~'), "'~'");
    assert_eq!(sanitize_char(0x01), "0x01");
    assert_eq!(sanitize_char(b' '), "0x20");
    assert_eq!(sanitize_char(0x7f), "0x7f");
}

#[test]
fn error_response_json() {
    let ctx = OauthCtx {
        state: OauthState::Init,
        issuer: Some("https://issuer.example.com".to_string()),
        scope: Some("openid postgres".to_string()),
        validator: &TEST_VALIDATOR,
    };
    let out = generate_error_response(&ctx).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "{ \"status\": \"invalid_token\", \"openid-configuration\": \"https://issuer.example.com/.well-known/openid-configuration\", \"scope\": \"openid postgres\" }"
    );

    // An issuer already carrying /.well-known/ is used as-is.
    let ctx = OauthCtx {
        state: OauthState::Init,
        issuer: Some("https://i.example/.well-known/oauth-authorization-server".to_string()),
        scope: Some("openid".to_string()),
        validator: &TEST_VALIDATOR,
    };
    let out = generate_error_response(&ctx).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "{ \"status\": \"invalid_token\", \"openid-configuration\": \"https://i.example/.well-known/oauth-authorization-server\", \"scope\": \"openid\" }"
    );
}

#[test]
fn registry_miss_is_file_error() {
    let Err(err) = load_validator_library("no_such_validator") else {
        panic!("registry miss must error");
    };
    assert_eq!(
        err.message(),
        "could not access file \"no_such_validator\": No such file or directory"
    );
}

// Security posture guard (finding idx 93 / CWE-489): the forgeable test
// validator must exist ONLY in test builds. Its definition, its constant, and
// its registration in init_seams() are all #[cfg(test)]-gated, so a production
// binary (built without cfg(test)) contains no builtin validator at all and any
// validator name — including "oauth_test_validator" — misses the registry and
// raises 58P01, matching C's dlopen stat miss. This test can only run under
// cfg(test), where init_seams() DOES register it; it asserts the wiring stays
// consistent and that unrelated names still miss (the production parity).
#[test]
fn test_validator_gated_to_test_builds() {
    super::init_seams();
    // In this (test) build the gated validator resolves.
    assert!(load_validator_library(TEST_VALIDATOR_NAME).is_ok());
    // Any other name misses the registry exactly as every name would in a
    // production binary, where no builtin validator is registered.
    assert!(load_validator_library("some_production_validator").is_err());
}

#[test]
fn test_validator_grammar() {
    let v = &TEST_VALIDATOR;
    let mut r = ValidatorModuleResult { authorized: false, authn_id: None };
    assert!(v.validate("valid-alice", "alice", &mut r).unwrap());
    assert!(r.authorized);
    assert_eq!(r.authn_id.as_deref(), Some("alice"));

    let mut r = ValidatorModuleResult { authorized: false, authn_id: None };
    assert!(v.validate("noauthz-bob", "bob", &mut r).unwrap());
    assert!(!r.authorized);
    assert_eq!(r.authn_id.as_deref(), Some("bob"));

    let mut r = ValidatorModuleResult { authorized: false, authn_id: None };
    assert!(v.validate("noident", "bob", &mut r).unwrap());
    assert!(r.authorized);
    assert_eq!(r.authn_id, None);

    let mut r = ValidatorModuleResult { authorized: false, authn_id: None };
    assert!(!v.validate("modulefail", "bob", &mut r).unwrap());

    let mut r = ValidatorModuleResult { authorized: false, authn_id: None };
    assert!(v.validate("garbage", "bob", &mut r).unwrap());
    assert!(!r.authorized);
    assert_eq!(r.authn_id, None);
}
