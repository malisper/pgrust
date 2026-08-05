#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use catalog::{ParameterAclOidIndexId, ParameterAclRelationId};
use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT};
use types_rel::{NoLock, RowExclusiveLock};

pub const Natts_pg_parameter_acl: usize = 3;
pub const Anum_pg_parameter_acl_oid: i32 = 1;
pub const Anum_pg_parameter_acl_parname: i32 = 2;
pub const Anum_pg_parameter_acl_paracl: i32 = 3;

#[track_caller]
#[cold]
fn undefined_err(parameter: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("parameter ACL \"{parameter}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

pub fn ParameterAclLookup(parameter: &str, missing_ok: bool) -> PgResult<Oid> {
    let parname = guc::convert_guc_name_for_parameter_acl(parameter);
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::PARAMETERACLNAME,
        Anum_pg_parameter_acl_oid,
        cache_syscache::SysCacheKey::Str(&parname),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(undefined_err(parameter));
    }
    Ok(oid)
}

// Only RowExclusiveLock is taken: the unique parname index, not a pre-probe,
// rejects concurrent duplicate inserts (matches C).
pub fn ParameterAclCreate<'mcx>(mcx: Mcx<'mcx>, parameter: &str) -> PgResult<Oid> {
    guc::check_GUC_name_for_parameter_acl(parameter)?;
    let parname = guc::convert_guc_name_for_parameter_acl(parameter);

    let rel = table::table_open(mcx, ParameterAclRelationId, RowExclusiveLock)?;
    let parameterId = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        ParameterAclOidIndexId,
        Anum_pg_parameter_acl_oid as AttrNumber,
    )?;

    let parname_text = varlena::cstring_to_text(mcx, parname.as_bytes())?;
    let mut values = [Datum::null(); Natts_pg_parameter_acl];
    let mut nulls = [false; Natts_pg_parameter_acl];
    values[(Anum_pg_parameter_acl_oid - 1) as usize] = Datum::from_oid(parameterId);
    values[(Anum_pg_parameter_acl_parname - 1) as usize] =
        Datum::from_usize(parname_text.as_bytes().as_ptr() as usize);
    nulls[(Anum_pg_parameter_acl_paracl - 1) as usize] = true;

    let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;

    rel.close(NoLock)?;
    Ok(parameterId)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_matches_pg_parameter_acl_h() {
        assert_eq!(Natts_pg_parameter_acl, 3);
        assert_eq!(Anum_pg_parameter_acl_oid, 1);
        assert_eq!(Anum_pg_parameter_acl_parname, 2);
        assert_eq!(Anum_pg_parameter_acl_paracl, 3);
        assert_eq!(ParameterAclRelationId, 6243);
        assert_eq!(ParameterAclOidIndexId, 6247);
        assert_eq!(catalog::ParameterAclParnameIndexId, 6246);
    }

    #[test]
    fn lookup_miss_error_shape() {
        let e = undefined_err("plperl.on_init");
        assert_eq!(e.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
        assert!(e.message().contains("parameter ACL \"plperl.on_init\" does not exist"));
    }
}
