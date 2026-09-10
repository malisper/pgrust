// src/test/modules/oauth_validator (validator.c, fail_validator.c) plus the
// token-prefix validator scripts/oauth-auth-e2e.sh drives. Every one of these
// authorizes forgeable credentials; see the crate header for the gating.

use auth_oauth::{OAuthValidator, ValidatorEnv, ValidatorModuleResult};
use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERRCODE_INTERNAL_ERROR, FATAL, LOG, WARNING};

pub const VALIDATOR_NAME: &str = "validator";
pub const FAIL_VALIDATOR_NAME: &str = "fail_validator";
pub const PREFIX_VALIDATOR_NAME: &str = "oauth_test_validator";

fn warn_test_build(name: &str) {
    let _ = ereport(WARNING)
        .errmsg(format!("OAuth test validator \"{name}\" is compiled into this server"))
        .errdetail("It authorizes forged bearer tokens. This binary was built with the oauth-test-validator feature and must not serve production traffic.")
        .finish(ErrorLocation::new(file!(), line!() as i32, "warn_test_build"));
}

pub struct TestValidator;
pub static TEST_VALIDATOR: TestValidator = TestValidator;

impl OAuthValidator for TestValidator {
    fn startup(&self, sversion: i32) {
        warn_test_build(VALIDATOR_NAME);
        if sversion != auth_oauth::PG_VERSION_NUM {
            let _ = elog::elog(LOG, format!("oauth_validator: sversion set to {sversion}"));
        }
    }

    fn validate(
        &self,
        env: ValidatorEnv<'_>,
        token: &str,
        role: &str,
        result: &mut ValidatorModuleResult,
    ) -> PgResult<bool> {
        elog::elog(LOG, format!("oauth_validator: token=\"{token}\", role=\"{role}\""))?;
        elog::elog(
            LOG,
            format!(
                "oauth_validator: issuer=\"{}\", scope=\"{}\"",
                env.issuer.unwrap_or("(null)"),
                env.scope.unwrap_or("(null)")
            ),
        )?;
        result.authorized = guc_tables::backing::oauth_validator_authorize_tokens();
        result.authn_id = Some(guc_tables::backing::oauth_validator_authn_id().unwrap_or_else(|| role.to_string()));
        Ok(true)
    }
}

pub struct FailValidator;
pub static FAIL_VALIDATOR: FailValidator = FailValidator;

impl OAuthValidator for FailValidator {
    fn startup(&self, _sversion: i32) {
        warn_test_build(FAIL_VALIDATOR_NAME);
    }

    fn validate(
        &self,
        _env: ValidatorEnv<'_>,
        _token: &str,
        _role: &str,
        _result: &mut ValidatorModuleResult,
    ) -> PgResult<bool> {
        ereport(FATAL)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg("fail_validator: sentinel error")
            .finish(ErrorLocation::new(file!(), line!() as i32, "fail_token"))?;
        unreachable!()
    }
}

// "valid-<id>" authorized as <id>; "noauthz-<id>" denied as <id>; "noident"
// authorized without identity; "modulefail" module error; else denied.
pub struct PrefixValidator;
pub static PREFIX_VALIDATOR: PrefixValidator = PrefixValidator;

impl OAuthValidator for PrefixValidator {
    fn startup(&self, _sversion: i32) {
        warn_test_build(PREFIX_VALIDATOR_NAME);
    }

    fn validate(
        &self,
        _env: ValidatorEnv<'_>,
        token: &str,
        role: &str,
        result: &mut ValidatorModuleResult,
    ) -> PgResult<bool> {
        elog::elog(LOG, format!("oauth_test_validator: token=\"{token}\", role=\"{role}\""))?;
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
