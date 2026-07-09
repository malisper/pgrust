#![allow(non_snake_case)]

use cache_syscache::cacheinfo::RELOID;
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use types_core::catalog::FirstNormalObjectId;
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

pub use rls_seams::CheckEnableRls;
use rls_seams::CheckEnableRls::{RlsEnabled, RlsNone, RlsNoneEnv};

const ANUM_PG_CLASS_RELROWSECURITY: i32 = 24;
const ANUM_PG_CLASS_RELFORCEROWSECURITY: i32 = 25;

pub fn check_enable_rls(relid: Oid, check_as_user: Oid, no_error: bool) -> PgResult<CheckEnableRls> {
    let user_id = if check_as_user != InvalidOid { check_as_user } else { miscinit::GetUserId() };

    if relid < FirstNormalObjectId {
        return Ok(RlsNone);
    }

    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(RlsNone);
    };
    let relrowsecurity =
        SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELROWSECURITY)?.as_bool();
    let relforcerowsecurity =
        SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELFORCEROWSECURITY)?.as_bool();
    ReleaseSysCache(tuple);

    if !relrowsecurity {
        return Ok(RlsNone);
    }

    if aclchk::has_bypassrls_privilege(user_id)? {
        return Ok(RlsNoneEnv);
    }

    // Owners bypass RLS unless FORCE ROW LEVEL SECURITY is set and this is
    // not a referential-integrity (InNoForceRLSOperation) context.
    let amowner = aclchk::object_ownercheck(types_core::RELATION_RELATION_ID, relid, user_id)?;
    if amowner && (!relforcerowsecurity || miscinit::InNoForceRLSOperation()) {
        return Ok(RlsNoneEnv);
    }

    if !guc_tables::backing::row_security() && !no_error {
        let relname = syscache_seams::pg_class_relname::call(relid)?
            .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
            .unwrap_or_default();
        let mut b = elog::ereport(ERROR).errcode(ERRCODE_INSUFFICIENT_PRIVILEGE).errmsg(format!(
            "query would be affected by row-level security policy for table \"{relname}\""
        ));
        if amowner {
            b = b.errhint(
                "To disable the policy for the table's owner, use ALTER TABLE NO FORCE ROW LEVEL SECURITY.",
            );
        }
        return Err(b.into_error().into());
    }

    Ok(RlsEnabled)
}

fn fc_row_security_active(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let tableoid = fcinfo.arg_oid(0);
    let status = check_enable_rls(tableoid, InvalidOid, true)?;
    Ok(Datum::from_bool(status == RlsEnabled))
}

fn fc_row_security_active_name(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let t = unsafe { fcinfo.arg_varlena_packed(0)? };
    let tablename = String::from_utf8_lossy(t.data()).into_owned();
    let names = varlena::textToQualifiedNameList(fcinfo.result_mcx(), &tablename)?;
    let parts: Vec<&str> = names.iter().map(String::as_str).collect();
    let tablerel = catalog_objectaddress::makeRangeVarFromParts(&parts)?;
    let tableoid = catalog_namespace::RangeVarGetRelid(&tablerel, types_rel::NoLock, false)?;
    let status = check_enable_rls(tableoid, InvalidOid, true)?;
    Ok(Datum::from_bool(status == RlsEnabled))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const RLS_BUILTINS: &[FmgrBuiltin] = &[
    b(3298, "row_security_active", 1, fc_row_security_active),
    b(3299, "row_security_active_name", 1, fc_row_security_active_name),
];

pub fn init_seams() {
    rls_seams::check_enable_rls::set(check_enable_rls);
}

#[cfg(test)]
mod tests {
    use super::*;

    // The owner/FORCE/bypassrls matrix reads live pg_class/pg_authid tuples;
    // without a bootable catcache harness those legs run in rls-e2e only.
    #[test]
    fn builtin_relids_bypass_rls() {
        assert_eq!(check_enable_rls(1259, 10, false).unwrap(), RlsNone);
        assert_eq!(check_enable_rls(16383, 10, true).unwrap(), RlsNone);
    }

    #[test]
    fn install_seams() {
        init_seams();
        assert!(rls_seams::check_enable_rls::is_installed());
        assert_eq!(rls_seams::check_enable_rls::call(1247, 10, false).unwrap(), RlsNone);
    }
}
