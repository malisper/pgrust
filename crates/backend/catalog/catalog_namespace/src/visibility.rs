use cache_syscache::cacheinfo::{PROCOID, RELOID, TYPEOID};
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use mcx::MemoryContext;
use types_core::{Oid, PG_CATALOG_NAMESPACE};
use types_error::{PgError, PgResult};
use types_tuple::NameData;

use crate::lookup::FuncnameGetCandidates;
use crate::path::recomputeNamespacePath;
use crate::{base_path_len, base_path_nth, OidIsValid};

const ANUM_PG_CLASS_RELNAME: i32 = 2;
const ANUM_PG_CLASS_RELNAMESPACE: i32 = 3;
const ANUM_PG_TYPE_TYPNAME: i32 = 2;
const ANUM_PG_TYPE_TYPNAMESPACE: i32 = 3;
const ANUM_PG_PROC_PRONAME: i32 = 2;
const ANUM_PG_PROC_PRONAMESPACE: i32 = 3;
const ANUM_PG_PROC_PRONARGS: i32 = 17;
const ANUM_PG_PROC_PROARGTYPES: i32 = 20;

fn name_of(d: Datum) -> NameData {
    // SAFETY: the datum points at a NameData column's 64-byte buffer inside
    // the pinned tuple image, copied out before release.
    unsafe { *(d.as_usize() as *const NameData) }
}

fn name_str(name: &NameData) -> &str {
    core::str::from_utf8(name.name_str()).expect("catalog names are valid UTF-8")
}

fn path_contains(nsp: Oid) -> bool {
    (0..base_path_len()).any(|i| base_path_nth(i) == nsp)
}

#[cold]
#[inline(never)]
fn lookup_failed(kind: &str, oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for {kind} {oid}")))
}

pub fn RelationIsVisible(relid: Oid) -> PgResult<bool> {
    RelationIsVisibleExt(relid)?.ok_or_else(|| lookup_failed("relation", relid))
}

/// C `RelationIsVisibleExt`; `None` mirrors `*is_missing = true`.
pub fn RelationIsVisibleExt(relid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        return Ok(None);
    };
    let relnamespace = SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELNAMESPACE)?.as_oid();
    let relname = name_of(SysCacheGetAttrNotNull(RELOID, &tuple, ANUM_PG_CLASS_RELNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if relnamespace != PG_CATALOG_NAMESPACE && !path_contains(relnamespace) {
        return Ok(Some(false));
    }
    // In-path items can still be shadowed by an earlier same-name relation.
    let mut visible = false;
    for i in 0..base_path_len() {
        let namespace_id = base_path_nth(i);
        if namespace_id == relnamespace {
            visible = true;
            break;
        }
        if OidIsValid(lsyscache::get_relname_relid(name_str(&relname), namespace_id)?) {
            break;
        }
    }
    Ok(Some(visible))
}

pub fn TypeIsVisible(typid: Oid) -> PgResult<bool> {
    TypeIsVisibleExt(typid)?.ok_or_else(|| lookup_failed("type", typid))
}

/// C `TypeIsVisibleExt`; `None` mirrors `*is_missing = true`.
pub fn TypeIsVisibleExt(typid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(TYPEOID, SysCacheKey::Value(Datum::from_oid(typid)))? else {
        return Ok(None);
    };
    let typnamespace = SysCacheGetAttrNotNull(TYPEOID, &tuple, ANUM_PG_TYPE_TYPNAMESPACE)?.as_oid();
    let typname = name_of(SysCacheGetAttrNotNull(TYPEOID, &tuple, ANUM_PG_TYPE_TYPNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if typnamespace != PG_CATALOG_NAMESPACE && !path_contains(typnamespace) {
        return Ok(Some(false));
    }
    let mut visible = false;
    for i in 0..base_path_len() {
        let namespace_id = base_path_nth(i);
        if namespace_id == typnamespace {
            visible = true;
            break;
        }
        if OidIsValid(syscache_seams::lookup_pg_type_oid_by_name::call(
            name_str(&typname),
            namespace_id,
        )?) {
            break;
        }
    }
    Ok(Some(visible))
}

pub fn FunctionIsVisible(funcid: Oid) -> PgResult<bool> {
    FunctionIsVisibleExt(funcid)?.ok_or_else(|| lookup_failed("function", funcid))
}

/// C `FunctionIsVisibleExt`; `None` mirrors `*is_missing = true`.
pub fn FunctionIsVisibleExt(funcid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcid)))? else {
        return Ok(None);
    };
    let scratch = MemoryContext::new("FunctionIsVisible");
    let pronamespace = SysCacheGetAttrNotNull(PROCOID, &tuple, ANUM_PG_PROC_PRONAMESPACE)?.as_oid();
    let proname = name_of(SysCacheGetAttrNotNull(PROCOID, &tuple, ANUM_PG_PROC_PRONAME)?);
    let pronargs = SysCacheGetAttrNotNull(PROCOID, &tuple, ANUM_PG_PROC_PRONARGS)?.as_i16();
    let argv = SysCacheGetAttrNotNull(PROCOID, &tuple, ANUM_PG_PROC_PROARGTYPES)?;
    // SAFETY: proargtypes is a not-null plain-storage oidvector; values tail
    // follows the 24-byte header in place, dim1 == pronargs.
    let args = unsafe {
        let p = argv.as_usize() as *const array::oidvector;
        core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
    };
    let proargtypes = mcx::slice_in(scratch.mcx(), args)?;
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if pronamespace != PG_CATALOG_NAMESPACE && !path_contains(pronamespace) {
        return Ok(Some(false));
    }
    // Visible iff FuncnameGetCandidates resolves the unqualified name +
    // signature to this exact proc.
    let clist = FuncnameGetCandidates(scratch.mcx(), &[name_str(&proname)], pronargs, false, false)?;
    let mut visible = false;
    for cand in clist.iter() {
        if cand.args.as_slice() == proargtypes.as_slice() {
            visible = cand.oid == funcid;
            break;
        }
    }
    Ok(Some(visible))
}
