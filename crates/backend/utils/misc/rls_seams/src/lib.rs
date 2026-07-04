use types_core::Oid;
use types_error::PgResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum CheckEnableRls {
    RlsNone = 0,
    RlsNoneEnv = 1,
    RlsEnabled = 2,
}

seam_core::seam!(
    pub fn check_enable_rls(relid: Oid, check_as_user: Oid, no_error: bool) -> PgResult<CheckEnableRls>
);
