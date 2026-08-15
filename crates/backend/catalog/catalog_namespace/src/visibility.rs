use cache_syscache::cacheinfo::{
    CLAOID, COLLOID, CONVOID, OPEROID, OPFAMILYOID, PROCOID, RELOID, STATEXTOID, TSCONFIGOID,
    TSDICTOID, TSPARSERNAMENSP, TSPARSEROID, TSTEMPLATENAMENSP, TSTEMPLATEOID, TYPEOID,
};
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use mcx::MemoryContext;
use types_core::{Oid, PG_CATALOG_NAMESPACE};
use types_error::{PgError, PgResult};
use types_tuple::NameData;

use crate::lookup::{
    CollationGetCollid, ConversionGetConid, FuncnameGetCandidates, OpclassnameGetOpcid,
    OpernameGetOprid, OpfamilynameGetOpfid,
};
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
const ANUM_PG_OPERATOR_OPRNAME: i32 = 2;
const ANUM_PG_OPERATOR_OPRNAMESPACE: i32 = 3;
const ANUM_PG_OPERATOR_OPRLEFT: i32 = 8;
const ANUM_PG_OPERATOR_OPRRIGHT: i32 = 9;
const ANUM_PG_OPCLASS_OPCMETHOD: i32 = 2;
const ANUM_PG_OPCLASS_OPCNAME: i32 = 3;
const ANUM_PG_OPCLASS_OPCNAMESPACE: i32 = 4;
const ANUM_PG_STATISTIC_EXT_STXNAME: i32 = 3;
const ANUM_PG_STATISTIC_EXT_STXNAMESPACE: i32 = 4;
const ANUM_PG_OPFAMILY_OPFMETHOD: i32 = 2;
const ANUM_PG_OPFAMILY_OPFNAME: i32 = 3;
const ANUM_PG_OPFAMILY_OPFNAMESPACE: i32 = 4;

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

#[track_caller]
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
    let clist =
        FuncnameGetCandidates(scratch.mcx(), &[name_str(&proname)], pronargs, &[], false, false)?;
    let mut visible = false;
    for cand in clist.iter() {
        if cand.args.as_slice() == proargtypes.as_slice() {
            visible = cand.oid == funcid;
            break;
        }
    }
    Ok(Some(visible))
}

pub fn OperatorIsVisible(oprid: Oid) -> PgResult<bool> {
    OperatorIsVisibleExt(oprid)?.ok_or_else(|| lookup_failed("operator", oprid))
}

/// C `OperatorIsVisibleExt`; `None` mirrors `*is_missing = true`.
pub fn OperatorIsVisibleExt(oprid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(OPEROID, SysCacheKey::Value(Datum::from_oid(oprid)))? else {
        return Ok(None);
    };
    let oprnamespace =
        SysCacheGetAttrNotNull(OPEROID, &tuple, ANUM_PG_OPERATOR_OPRNAMESPACE)?.as_oid();
    let oprname = name_of(SysCacheGetAttrNotNull(OPEROID, &tuple, ANUM_PG_OPERATOR_OPRNAME)?);
    let oprleft = SysCacheGetAttrNotNull(OPEROID, &tuple, ANUM_PG_OPERATOR_OPRLEFT)?.as_oid();
    let oprright = SysCacheGetAttrNotNull(OPEROID, &tuple, ANUM_PG_OPERATOR_OPRRIGHT)?.as_oid();
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if oprnamespace != PG_CATALOG_NAMESPACE && !path_contains(oprnamespace) {
        return Ok(Some(false));
    }
    // In-path items can still be shadowed by an earlier same-name/same-args
    // operator; visible iff OpernameGetOprid resolves back to this one.
    Ok(Some(OpernameGetOprid(&[name_str(&oprname)], oprleft, oprright)? == oprid))
}

pub fn OpclassIsVisible(opcid: Oid) -> PgResult<bool> {
    OpclassIsVisibleExt(opcid)?.ok_or_else(|| lookup_failed("opclass", opcid))
}

/// C `OpclassIsVisibleExt`; `None` mirrors `*is_missing = true`.
pub fn OpclassIsVisibleExt(opcid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(CLAOID, SysCacheKey::Value(Datum::from_oid(opcid)))? else {
        return Ok(None);
    };
    let opcnamespace =
        SysCacheGetAttrNotNull(CLAOID, &tuple, ANUM_PG_OPCLASS_OPCNAMESPACE)?.as_oid();
    let opcname = name_of(SysCacheGetAttrNotNull(CLAOID, &tuple, ANUM_PG_OPCLASS_OPCNAME)?);
    let opcmethod = SysCacheGetAttrNotNull(CLAOID, &tuple, ANUM_PG_OPCLASS_OPCMETHOD)?.as_oid();
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if opcnamespace != PG_CATALOG_NAMESPACE && !path_contains(opcnamespace) {
        return Ok(Some(false));
    }
    Ok(Some(OpclassnameGetOpcid(opcmethod, name_str(&opcname))? == opcid))
}


// C StatisticsObjIsVisible; callers (ruleutils/objectaddress arms) not yet ported
// (unported-census 2026-08-05 lane 3).
#[allow(dead_code)]
pub fn StatisticsObjIsVisible(stxid: Oid) -> PgResult<bool> {
    StatisticsObjIsVisibleExt(stxid)?.ok_or_else(|| lookup_failed("statistics object", stxid))
}

/// C `StatisticsObjIsVisibleExt`; `None` mirrors `*is_missing = true`.
pub fn StatisticsObjIsVisibleExt(stxid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(STATEXTOID, SysCacheKey::Value(Datum::from_oid(stxid)))?
    else {
        return Ok(None);
    };
    let stxnamespace =
        SysCacheGetAttrNotNull(STATEXTOID, &tuple, ANUM_PG_STATISTIC_EXT_STXNAMESPACE)?.as_oid();
    let stxname =
        name_of(SysCacheGetAttrNotNull(STATEXTOID, &tuple, ANUM_PG_STATISTIC_EXT_STXNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if stxnamespace != PG_CATALOG_NAMESPACE && !path_contains(stxnamespace) {
        return Ok(Some(false));
    }
    // In-path objects can still be shadowed by a same-name statistics object
    // earlier in the path.
    for i in 0..base_path_len() {
        let namespace_id = base_path_nth(i);
        if namespace_id == crate::my_temp_namespace() {
            // namespace.c:2690: do not look in temp namespace.
            continue;
        }
        if namespace_id == stxnamespace {
            return Ok(Some(true));
        }
        if OidIsValid(syscache_seams::lookup_pg_statistic_ext_oid_by_name_nsp::call(
            name_str(&stxname),
            namespace_id,
        )?) {
            return Ok(Some(false));
        }
    }
    Ok(Some(false))
}

pub fn OpfamilyIsVisible(opfid: Oid) -> PgResult<bool> {
    OpfamilyIsVisibleExt(opfid)?.ok_or_else(|| lookup_failed("opfamily", opfid))
}

/// C `OpfamilyIsVisibleExt`; `None` mirrors `*is_missing = true`.
pub fn OpfamilyIsVisibleExt(opfid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(OPFAMILYOID, SysCacheKey::Value(Datum::from_oid(opfid)))?
    else {
        return Ok(None);
    };
    let opfnamespace =
        SysCacheGetAttrNotNull(OPFAMILYOID, &tuple, ANUM_PG_OPFAMILY_OPFNAMESPACE)?.as_oid();
    let opfname = name_of(SysCacheGetAttrNotNull(OPFAMILYOID, &tuple, ANUM_PG_OPFAMILY_OPFNAME)?);
    let opfmethod =
        SysCacheGetAttrNotNull(OPFAMILYOID, &tuple, ANUM_PG_OPFAMILY_OPFMETHOD)?.as_oid();
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if opfnamespace != PG_CATALOG_NAMESPACE && !path_contains(opfnamespace) {
        return Ok(Some(false));
    }
    Ok(Some(OpfamilynameGetOpfid(opfmethod, name_str(&opfname))? == opfid))
}

const ANUM_PG_COLLATION_COLLNAME: i32 = 2;
const ANUM_PG_COLLATION_COLLNAMESPACE: i32 = 3;
const ANUM_PG_CONVERSION_CONNAME: i32 = 2;
const ANUM_PG_CONVERSION_CONNAMESPACE: i32 = 3;
const ANUM_PG_TS_PARSER_PRSNAME: i32 = 2;
const ANUM_PG_TS_PARSER_PRSNAMESPACE: i32 = 3;
const ANUM_PG_TS_TEMPLATE_TMPLNAME: i32 = 2;
const ANUM_PG_TS_TEMPLATE_TMPLNAMESPACE: i32 = 3;
const ANUM_PG_TS_DICT_DICTNAME: i32 = 2;
const ANUM_PG_TS_DICT_DICTNAMESPACE: i32 = 3;
const ANUM_PG_TS_CONFIG_CFGNAME: i32 = 2;
const ANUM_PG_TS_CONFIG_CFGNAMESPACE: i32 = 3;

fn named_visible_in_path(obj_nsp: Oid, name: &str, cache_id: i32) -> PgResult<bool> {
    for i in 0..base_path_len() {
        let namespace_id = base_path_nth(i);
        if namespace_id == crate::my_temp_namespace() {
            continue;
        }
        if namespace_id == obj_nsp {
            return Ok(true);
        }
        if OidIsValid(cache_syscache::GetSysCacheOid(
            cache_id,
            1,
            SysCacheKey::Str(name),
            SysCacheKey::Value(Datum::from_oid(namespace_id)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?) {
            return Ok(false);
        }
    }
    Ok(false)
}

pub fn CollationIsVisible(collid: Oid) -> PgResult<bool> {
    CollationIsVisibleExt(collid)?.ok_or_else(|| lookup_failed("collation", collid))
}

pub fn CollationIsVisibleExt(collid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(COLLOID, SysCacheKey::Value(Datum::from_oid(collid)))? else {
        return Ok(None);
    };
    let collnamespace =
        SysCacheGetAttrNotNull(COLLOID, &tuple, ANUM_PG_COLLATION_COLLNAMESPACE)?.as_oid();
    let collname = name_of(SysCacheGetAttrNotNull(COLLOID, &tuple, ANUM_PG_COLLATION_COLLNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if collnamespace != PG_CATALOG_NAMESPACE && !path_contains(collnamespace) {
        return Ok(Some(false));
    }
    Ok(Some(CollationGetCollid(name_str(&collname))? == collid))
}

pub fn ConversionIsVisible(conid: Oid) -> PgResult<bool> {
    ConversionIsVisibleExt(conid)?.ok_or_else(|| lookup_failed("conversion", conid))
}

pub fn ConversionIsVisibleExt(conid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(CONVOID, SysCacheKey::Value(Datum::from_oid(conid)))? else {
        return Ok(None);
    };
    let connamespace =
        SysCacheGetAttrNotNull(CONVOID, &tuple, ANUM_PG_CONVERSION_CONNAMESPACE)?.as_oid();
    let conname = name_of(SysCacheGetAttrNotNull(CONVOID, &tuple, ANUM_PG_CONVERSION_CONNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if connamespace != PG_CATALOG_NAMESPACE && !path_contains(connamespace) {
        return Ok(Some(false));
    }
    Ok(Some(ConversionGetConid(name_str(&conname))? == conid))
}

pub fn TSParserIsVisible(prs_id: Oid) -> PgResult<bool> {
    TSParserIsVisibleExt(prs_id)?.ok_or_else(|| lookup_failed("text search parser", prs_id))
}

pub fn TSParserIsVisibleExt(prs_id: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(TSPARSEROID, SysCacheKey::Value(Datum::from_oid(prs_id)))?
    else {
        return Ok(None);
    };
    let prsnamespace =
        SysCacheGetAttrNotNull(TSPARSEROID, &tuple, ANUM_PG_TS_PARSER_PRSNAMESPACE)?.as_oid();
    let prsname = name_of(SysCacheGetAttrNotNull(TSPARSEROID, &tuple, ANUM_PG_TS_PARSER_PRSNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if prsnamespace != PG_CATALOG_NAMESPACE && !path_contains(prsnamespace) {
        return Ok(Some(false));
    }
    Ok(Some(named_visible_in_path(
        prsnamespace,
        name_str(&prsname),
        TSPARSERNAMENSP,
    )?))
}

pub fn TSDictionaryIsVisible(dict_id: Oid) -> PgResult<bool> {
    TSDictionaryIsVisibleExt(dict_id)?.ok_or_else(|| lookup_failed("text search dictionary", dict_id))
}

pub fn TSDictionaryIsVisibleExt(dict_id: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(TSDICTOID, SysCacheKey::Value(Datum::from_oid(dict_id)))?
    else {
        return Ok(None);
    };
    let dictnamespace =
        SysCacheGetAttrNotNull(TSDICTOID, &tuple, ANUM_PG_TS_DICT_DICTNAMESPACE)?.as_oid();
    let dictname = name_of(SysCacheGetAttrNotNull(TSDICTOID, &tuple, ANUM_PG_TS_DICT_DICTNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if dictnamespace != PG_CATALOG_NAMESPACE && !path_contains(dictnamespace) {
        return Ok(Some(false));
    }
    Ok(Some(named_visible_in_path(
        dictnamespace,
        name_str(&dictname),
        cache_syscache::cacheinfo::TSDICTNAMENSP,
    )?))
}

pub fn TSTemplateIsVisible(tmpl_id: Oid) -> PgResult<bool> {
    TSTemplateIsVisibleExt(tmpl_id)?.ok_or_else(|| lookup_failed("text search template", tmpl_id))
}

pub fn TSTemplateIsVisibleExt(tmpl_id: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(TSTEMPLATEOID, SysCacheKey::Value(Datum::from_oid(tmpl_id)))?
    else {
        return Ok(None);
    };
    let tmplnamespace =
        SysCacheGetAttrNotNull(TSTEMPLATEOID, &tuple, ANUM_PG_TS_TEMPLATE_TMPLNAMESPACE)?.as_oid();
    let tmplname =
        name_of(SysCacheGetAttrNotNull(TSTEMPLATEOID, &tuple, ANUM_PG_TS_TEMPLATE_TMPLNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if tmplnamespace != PG_CATALOG_NAMESPACE && !path_contains(tmplnamespace) {
        return Ok(Some(false));
    }
    Ok(Some(named_visible_in_path(
        tmplnamespace,
        name_str(&tmplname),
        TSTEMPLATENAMENSP,
    )?))
}

pub fn TSConfigIsVisible(cfgid: Oid) -> PgResult<bool> {
    TSConfigIsVisibleExt(cfgid)?.ok_or_else(|| lookup_failed("text search configuration", cfgid))
}

pub fn TSConfigIsVisibleExt(cfgid: Oid) -> PgResult<Option<bool>> {
    let Some(tuple) = SearchSysCache1(TSCONFIGOID, SysCacheKey::Value(Datum::from_oid(cfgid)))?
    else {
        return Ok(None);
    };
    let cfgnamespace =
        SysCacheGetAttrNotNull(TSCONFIGOID, &tuple, ANUM_PG_TS_CONFIG_CFGNAMESPACE)?.as_oid();
    let cfgname = name_of(SysCacheGetAttrNotNull(TSCONFIGOID, &tuple, ANUM_PG_TS_CONFIG_CFGNAME)?);
    ReleaseSysCache(tuple);

    recomputeNamespacePath()?;

    if cfgnamespace != PG_CATALOG_NAMESPACE && !path_contains(cfgnamespace) {
        return Ok(Some(false));
    }
    Ok(Some(named_visible_in_path(
        cfgnamespace,
        name_str(&cfgname),
        cache_syscache::cacheinfo::TSCONFIGNAMENSP,
    )?))
}
