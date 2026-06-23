//! Port of `src/backend/utils/misc/rls.c` — RLS-related utility functions.
//!
//! `check_enable_rls` is the decision core; `row_security_active` and
//! `row_security_active_name` are the two SQL-callable wrappers
//! (`PG_FUNCTION_ARGS` in C). The relcache flags, the BYPASSRLS/owner ACL
//! checks, the `InNoForceRLSOperation` test, the `row_security` GUC, and the
//! name resolution all live in other subsystems, reached through their owners'
//! `-seams` crates.

use acl_seams::has_bypassrls_privilege;
use lsyscache_seams::get_rel_name;
use syscache_seams::search_relation_rls_flags;
use miscinit_seams::{get_user_id, in_no_force_rls_operation};
use aclchk_seams::object_ownercheck;
use namespace_seams::range_var_get_relid_from_text;
use utils_error::ereport;
use guc_tables::vars;
use mcx::Mcx;
use types_core::{FirstNormalObjectId, Oid, RELATION_RELATION_ID, INVALID_OID};
use types_error::{PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR};
use types_storage::lock::NoLock;

/// `enum CheckEnableRlsResult` (`utils/rls.h`) — the result of
/// [`check_enable_rls`]. Canonically defined in `types_acl` (which is also the
/// seam-contract type); re-exported here so the two surfaces share one type.
pub use types_acl::CheckEnableRlsResult;

/// `row_security` GUC value.
fn row_security() -> bool {
    (vars::row_security.get().get)()
}

/// `check_enable_rls(relid, checkAsUser, noError)` — decide whether RLS applies
/// to a query against `relid`. Pass `InvalidOid` as `check_as_user` to check
/// the current user. `mcx` charges the transient `get_rel_name` copy used to
/// build the error message.
pub fn check_enable_rls(
    mcx: Mcx<'_>,
    relid: Oid,
    check_as_user: Oid,
    no_error: bool,
) -> PgResult<CheckEnableRlsResult> {
    let user_id = if OidIsValid(check_as_user) {
        check_as_user
    } else {
        get_user_id::call()
    };

    // Nothing to do for built-in relations.
    if relid < FirstNormalObjectId {
        return Ok(CheckEnableRlsResult::RlsNone);
    }

    // Fetch relation's relrowsecurity and relforcerowsecurity flags. A
    // `!HeapTupleIsValid` lookup returns RLS_NONE.
    let Some((relrowsecurity, relforcerowsecurity)) = search_relation_rls_flags::call(relid)?
    else {
        return Ok(CheckEnableRlsResult::RlsNone);
    };

    // Nothing to do if the relation does not have RLS.
    if !relrowsecurity {
        return Ok(CheckEnableRlsResult::RlsNone);
    }

    // BYPASSRLS users always bypass RLS (superusers always have BYPASSRLS).
    // Return RLS_NONE_ENV: this decision depends on the environment (user_id).
    if has_bypassrls_privilege::call(user_id)? {
        return Ok(CheckEnableRlsResult::RlsNoneEnv);
    }

    // Table owners generally bypass RLS, except if the table has been set to
    // FORCE ROW SECURITY, and this is not a referential integrity check.
    let amowner = object_ownercheck::call(RELATION_RELATION_ID, relid, user_id)?;
    if amowner {
        // If FORCE ROW LEVEL SECURITY is set we return RLS_ENABLED; otherwise,
        // or if we are in an InNoForceRLSOperation context, RLS_NONE_ENV.
        if !relforcerowsecurity || in_no_force_rls_operation::call() {
            return Ok(CheckEnableRlsResult::RlsNoneEnv);
        }
    }

    // We should apply RLS. However, the user may turn off the row_security GUC
    // to get a forced error instead.
    if !row_security() && !no_error {
        let relname = get_rel_name::call(mcx, relid)?;
        let relname = relname.as_ref().map(|s| s.as_str()).unwrap_or("");
        let mut builder = ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "query would be affected by row-level security policy for table \"{relname}\""
            ));
        if amowner {
            builder = builder.errhint(
                "To disable the policy for the table's owner, use ALTER TABLE NO FORCE ROW LEVEL SECURITY.",
            );
        }
        return Err(builder.into_error());
    }

    // RLS should be fully enabled for this relation.
    Ok(CheckEnableRlsResult::RlsEnabled)
}

/// `row_security_active(tableoid)` — SQL-callable test of whether RLS is active
/// for the current user on `tableoid`. `RLS_NONE_ENV` and `RLS_NONE` are the
/// same for this purpose.
pub fn row_security_active(mcx: Mcx<'_>, tableoid: Oid) -> PgResult<bool> {
    let rls_status = check_enable_rls(mcx, tableoid, INVALID_OID, true)?;
    Ok(rls_status == CheckEnableRlsResult::RlsEnabled)
}

/// `row_security_active_name(relname)` — same as [`row_security_active`] but
/// resolves a qualified relation name first.
pub fn row_security_active_name(mcx: Mcx<'_>, relation_name: &str) -> PgResult<bool> {
    // Look up table name. Can't lock it - we might not have privileges.
    // C: `RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(
    // tablename)), NoLock, false)` — missing_ok = false, so a non-existent
    // relation raises ERRCODE_UNDEFINED_TABLE.
    let tableoid =
        range_var_get_relid_from_text::call(mcx, relation_name, NoLock, false)?;
    row_security_active(mcx, tableoid)
}

/// `OidIsValid(objectId)` — `(objectId) != InvalidOid`.
#[inline]
fn OidIsValid(object_id: Oid) -> bool {
    object_id != INVALID_OID
}
