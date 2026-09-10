//! In-tree OAuth bearer-token validators for the `oauth` HBA method. C
//! PostgreSQL dlopens validator modules named in oauth_validator_libraries
//! and ships none in the server; pgrust has no dlopen (no-dlopen carve,
//! docs/design/carve-ratifications.md §2), so the validators a deployment
//! can name are exactly the ones registered here.
//!
//! `jwt_validator` (always registered): verifies JWS access tokens against
//! the issuer's JWKS (RS256/384/512, PS256/384/512, ES256/384; issuer,
//! audience, exp/nbf/iat and scope checks) and falls back to RFC 7662
//! introspection for opaque tokens. Configured by the jwt_validator.* GUCs.
//!
//! `validator`, `fail_validator`, `oauth_test_validator` (feature
//! `test-validator` only): the C regression module's validators, which
//! authorize any token. They are gated by a cargo feature rather than a GUC
//! because a GUC — however loudly named — is reachable from a shipped binary
//! by anyone who can edit postgresql.conf or run ALTER SYSTEM, and turns a
//! configuration mistake into an authentication bypass; a feature-gated
//! binary cannot be talked into it at all, and the release build (main_main
//! default features) never contains the code. A test-built server still
//! warns on every validator startup so a mis-shipped binary is visible in
//! the log.

#[cfg(not(target_family = "wasm"))]
mod http;
mod json;
#[cfg(not(target_family = "wasm"))]
mod jwt;
#[cfg(not(target_family = "wasm"))]
mod validator;

#[cfg(feature = "test-validator")]
mod testmod;

#[cfg(not(target_family = "wasm"))]
pub use validator::JWT_VALIDATOR_NAME;

pub fn init_seams() {
    #[cfg(not(target_family = "wasm"))]
    auth_oauth::register_builtin_validator(validator::JWT_VALIDATOR_NAME, &validator::JWT_VALIDATOR);
    #[cfg(feature = "test-validator")]
    {
        auth_oauth::register_builtin_validator(testmod::VALIDATOR_NAME, &testmod::TEST_VALIDATOR);
        auth_oauth::register_builtin_validator(testmod::FAIL_VALIDATOR_NAME, &testmod::FAIL_VALIDATOR);
        auth_oauth::register_builtin_validator(testmod::PREFIX_VALIDATOR_NAME, &testmod::PREFIX_VALIDATOR);
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests;
