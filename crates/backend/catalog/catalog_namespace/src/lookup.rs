use mcx::MemoryContext;
use rel_vocab::RangeVar;
use types_core::{InvalidOid, Oid, RELPERSISTENCE_TEMP};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_SCHEMA, ERRCODE_UNDEFINED_TABLE,
};
use types_rel::{NoLock, LOCKMODE};

use crate::path::recomputeNamespacePath;
use crate::{base_path_len, base_path_nth, my_temp_namespace, OidIsValid};

pub use namespace_seams::{RVR_MISSING_OK, RVR_NOWAIT, RVR_SKIP_LOCKED};

// parsenodes.h ObjectType, verified against REL_18_3.
const OBJECT_SCHEMA: i32 = 36;
const ACL_USAGE: u64 = 1 << 8;
const ACLCHECK_OK: i32 = 0;

pub type RangeVarGetRelidCallback<'a> =
    Option<&'a mut dyn FnMut(&RangeVar<'_>, Oid, Oid) -> PgResult<()>>;

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_schema(nspname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("schema \"{nspname}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_SCHEMA),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_relation(relation: &RangeVar<'_>) -> Box<PgError> {
    let msg = match relation.schemaname {
        Some(schema) => format!(
            "relation \"{}.{}\" does not exist",
            schema, relation.relname
        ),
        None => format!("relation \"{}\" does not exist", relation.relname),
    };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_TABLE))
}

#[track_caller]
#[cold]
#[inline(never)]
fn cross_database_reference(relation: &RangeVar<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "cross-database references are not implemented: \"{}.{}.{}\"",
            relation.catalogname.unwrap_or_default(),
            relation.schemaname.unwrap_or_default(),
            relation.relname
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn temp_table_schema_name() -> Box<PgError> {
    Box::new(
        PgError::error("temporary tables cannot specify a schema name")
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
    )
}

pub fn get_namespace_oid(nspname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::lookup_pg_namespace_oid_by_name::call(nspname)?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(undefined_schema(nspname));
    }
    Ok(oid)
}

pub fn LookupNamespaceNoError(nspname: &str) -> PgResult<Oid> {
    if nspname == "pg_temp" {
        if OidIsValid(my_temp_namespace()) {
            return Ok(my_temp_namespace());
        }
        // Lookups of existing objects never create the temp namespace.
        return Ok(InvalidOid);
    }
    get_namespace_oid(nspname, true)
}

pub fn LookupExplicitNamespace(nspname: &str, missing_ok: bool) -> PgResult<Oid> {
    if nspname == "pg_temp" {
        if OidIsValid(my_temp_namespace()) {
            return Ok(my_temp_namespace());
        }
        // Fall through: missing temp namespace means the object cannot exist.
    }

    let namespaceId = get_namespace_oid(nspname, missing_ok)?;
    if missing_ok && !OidIsValid(namespaceId) {
        return Ok(InvalidOid);
    }

    let aclresult = aclchk_seams::object_aclcheck::call(
        types_core::catalog::NAMESPACE_RELATION_ID,
        namespaceId,
        miscinit_seams::get_user_id::call(),
        ACL_USAGE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclchk_seams::aclcheck_error::call(aclresult, OBJECT_SCHEMA, nspname)?;
    }
    Ok(namespaceId)
}

pub fn LookupCreationNamespace(mcx: mcx::Mcx<'_>, nspname: &str) -> PgResult<Oid> {
    // pg_temp alias (namespace.c): initialize-if-needed and return the
    // session's temp namespace; no ACL check (it's ours by construction).
    // SET SCHEMA callers then fail C's CheckSetNamespace temp-schema arm.
    if nspname == "pg_temp" {
        return crate::temp::GetTempTableNamespace(mcx);
    }
    let namespaceId = get_namespace_oid(nspname, false)?;
    const ACL_CREATE: u64 = 1 << 9;
    let aclresult = aclchk_seams::object_aclcheck::call(
        types_core::catalog::NAMESPACE_RELATION_ID,
        namespaceId,
        miscinit_seams::get_user_id::call(),
        ACL_CREATE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclchk_seams::aclcheck_error::call(aclresult, OBJECT_SCHEMA, nspname)?;
    }
    Ok(namespaceId)
}

pub fn CheckSetNamespace(oldNspOid: Oid, nspOid: Oid) -> PgResult<()> {
    if crate::isAnyTempNamespace(nspOid)? || crate::isAnyTempNamespace(oldNspOid)? {
        return Err(Box::new(
            PgError::error("cannot move objects into or out of temporary schemas")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    const PG_TOAST_NAMESPACE: Oid = 99;
    if nspOid == PG_TOAST_NAMESPACE || oldNspOid == PG_TOAST_NAMESPACE {
        return Err(Box::new(
            PgError::error("cannot move objects into or out of TOAST schema")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(())
}

pub fn FindDefaultConversionProc(for_encoding: i32, to_encoding: i32) -> PgResult<Oid> {
    recomputeNamespacePath()?;

    for i in 0..base_path_len() {
        let namespaceId = base_path_nth(i);
        if namespaceId == my_temp_namespace() {
            continue;
        }
        let proc = pg_conversion::FindDefaultConversion(namespaceId, for_encoding, to_encoding)?;
        if OidIsValid(proc) {
            return Ok(proc);
        }
    }
    Ok(InvalidOid)
}

pub fn RelnameGetRelid(relname: &str) -> PgResult<Oid> {
    recomputeNamespacePath()?;

    for i in 0..base_path_len() {
        let relid = lsyscache::get_relname_relid(relname, base_path_nth(i))?;
        if OidIsValid(relid) {
            return Ok(relid);
        }
    }
    Ok(InvalidOid)
}

#[track_caller]
#[cold]
#[inline(never)]
fn improper_qualified_name(names: &[&str]) -> Box<PgError> {
    improper_qualified_name_joined(names.join("."))
}

#[track_caller]
#[cold]
#[inline(never)]
fn improper_qualified_name_joined(joined: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "improper qualified name (too many dotted names): {joined}"
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

// C takes a List of String nodes; callers here pass the extracted parts.
pub fn DeconstructQualifiedName<'a>(names: &[&'a str]) -> PgResult<(Option<&'a str>, &'a str)> {
    match names {
        [objname] => Ok((None, objname)),
        [schemaname, objname] => Ok((Some(schemaname), objname)),
        [catalogname, schemaname, objname] => {
            let dbname =
                dbcommands_seams::get_database_name::call(init_small::globals::MyDatabaseId())?;
            if dbname.as_deref() != Some(*catalogname) {
                return Err(Box::new(
                    PgError::error(format!(
                        "cross-database references are not implemented: {}",
                        names.join(".")
                    ))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            Ok((Some(schemaname), objname))
        }
        _ => Err(improper_qualified_name(names)),
    }
}

pub fn OpernameGetOprid(names: &[&str], oprleft: Oid, oprright: Oid) -> PgResult<Oid> {
    let (schemaname, opername) = DeconstructQualifiedName(names)?;

    if let Some(schemaname) = schemaname {
        let namespaceId = LookupExplicitNamespace(schemaname, true)?;
        if OidIsValid(namespaceId) {
            let result = syscache_seams::lookup_pg_operator_oid_exact::call(
                opername,
                oprleft,
                oprright,
                namespaceId,
            )?;
            if OidIsValid(result) {
                return Ok(result);
            }
        }
        return Ok(InvalidOid);
    }

    // Per-call scratch is fine here: callers sit behind parse_oper's OprCache
    // memo (C allocates the CatCList per call too).
    let scratch = MemoryContext::new("OpernameGetOprid");
    let candidates = syscache_seams::lookup_pg_operator_candidates::call(
        scratch.mcx(),
        opername,
        oprleft,
        oprright,
    )?;
    if candidates.is_empty() {
        return Ok(InvalidOid);
    }

    recomputeNamespacePath()?;
    let mtn = my_temp_namespace();
    for i in 0..base_path_len() {
        let namespaceId = base_path_nth(i);
        if namespaceId == mtn {
            continue;
        }
        for &(oid, oprnamespace) in candidates.iter() {
            if oprnamespace == namespaceId {
                return Ok(oid);
            }
        }
    }
    Ok(InvalidOid)
}

pub struct OperCandidate {
    pub oid: Oid,
    pub args: [Oid; 2],
}

// OpernameGetCandidates (namespace.c). C prepends onto a linked list; this
// returns that final head-first order (reverse of acceptance order).
pub fn OpernameGetCandidates<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    names: &[&str],
    oprkind: i8,
    missing_schema_ok: bool,
) -> PgResult<mcx::PgVec<'mcx, OperCandidate>> {
    let (schemaname, opername) = DeconstructQualifiedName(names)?;

    let namespace_id = match schemaname {
        Some(schemaname) => {
            let id = LookupExplicitNamespace(schemaname, missing_schema_ok)?;
            if missing_schema_ok && !OidIsValid(id) {
                return Ok(mcx::PgVec::new_in(mcx));
            }
            Some(id)
        }
        None => {
            recomputeNamespacePath()?;
            None
        }
    };

    let raw = syscache_seams::lookup_pg_operator_name_candidates::call(mcx, opername)?;
    let mut result: mcx::PgVec<'mcx, OperCandidate> = mcx::PgVec::new_in(mcx);
    let mut pathposes: mcx::PgVec<'mcx, usize> = mcx::PgVec::new_in(mcx);
    let mtn = my_temp_namespace();
    for cand in raw.iter() {
        if oprkind != 0 && cand.oprkind != oprkind {
            continue;
        }
        let mut pathpos = 0usize;
        match namespace_id {
            Some(id) => {
                if cand.oprnamespace != id {
                    continue;
                }
            }
            None => {
                let mut found = false;
                for i in 0..base_path_len() {
                    if cand.oprnamespace == base_path_nth(i) && cand.oprnamespace != mtn {
                        found = true;
                        break;
                    }
                    pathpos += 1;
                }
                if !found {
                    continue;
                }
                if let Some(prev) = result
                    .iter()
                    .position(|p| p.args == [cand.oprleft, cand.oprright])
                {
                    debug_assert_ne!(pathpos, pathposes[prev]);
                    if pathpos > pathposes[prev] {
                        continue;
                    }
                    pathposes[prev] = pathpos;
                    result[prev].oid = cand.oid;
                    continue;
                }
            }
        }
        result.push(OperCandidate {
            oid: cand.oid,
            args: [cand.oprleft, cand.oprright],
        });
        pathposes.push(pathpos);
    }
    result.reverse();
    Ok(result)
}

// is_encoding_supported_by_icu (encnames.c): the pg_enc2icu_tbl NULL slots
// are SQL_ASCII(0), EUC_JIS_2004(5), MULE_INTERNAL(7), LATIN10(17), WIN874(21).
pub fn is_encoding_supported_by_icu(encoding: i32) -> bool {
    (0..=34).contains(&encoding) && !matches!(encoding, 0 | 5 | 7 | 17 | 21)
}

// lookup_collation (namespace.c).
fn lookup_collation(collname: &str, collnamespace: Oid, encoding: i32) -> PgResult<Oid> {
    if let Some(row) = syscache_seams::lookup_pg_collation_by_name_enc_nsp::call(
        collname,
        encoding,
        collnamespace,
    )? {
        return Ok(row.oid);
    }
    let Some(row) =
        syscache_seams::lookup_pg_collation_by_name_enc_nsp::call(collname, -1, collnamespace)?
    else {
        return Ok(InvalidOid);
    };
    if row.collprovider == b'i' && !is_encoding_supported_by_icu(encoding) {
        return Ok(InvalidOid);
    }
    Ok(row.oid)
}

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_collation(collname: &[&str]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "collation \"{}\" for encoding \"{}\" does not exist",
            collname.join("."),
            mbutils_seams::get_database_encoding_name::call()
        ))
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
    )
}

// get_collation_oid over the raw name List; >3 parts flows to C's
// DeconstructQualifiedName 42601 instead of a length assert.
pub fn get_collation_oid_list(
    collname: &types_nodes::NodeList<'_>,
    missing_ok: bool,
) -> PgResult<Oid> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = collname.len();
    if nnames > 4 {
        let mut joined = String::new();
        for (i, n) in collname.iter().enumerate() {
            if i > 0 {
                joined.push('.');
            }
            joined.push_str(n.as_string().expect("collname cell").sval);
        }
        return Err(improper_qualified_name_joined(joined));
    }
    for (i, n) in collname.iter().enumerate() {
        names[i] = n.as_string().expect("collname cell").sval;
    }
    get_collation_oid(&names[..nnames], missing_ok)
}

pub fn get_collation_oid(collname: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let dbencoding = mbutils_seams::get_database_encoding::call();
    let (schemaname, collation_name) = DeconstructQualifiedName(collname)?;

    if let Some(schemaname) = schemaname {
        let namespace_id = LookupExplicitNamespace(schemaname, missing_ok)?;
        if missing_ok && !OidIsValid(namespace_id) {
            return Ok(InvalidOid);
        }
        let colloid = lookup_collation(collation_name, namespace_id, dbencoding)?;
        if OidIsValid(colloid) {
            return Ok(colloid);
        }
    } else {
        recomputeNamespacePath()?;
        let mtn = my_temp_namespace();
        for i in 0..base_path_len() {
            let namespace_id = base_path_nth(i);
            if namespace_id == mtn {
                continue;
            }
            let colloid = lookup_collation(collation_name, namespace_id, dbencoding)?;
            if OidIsValid(colloid) {
                return Ok(colloid);
            }
        }
    }

    if !missing_ok {
        return Err(undefined_collation(collname));
    }
    Ok(InvalidOid)
}

#[track_caller]
#[cold]
#[inline(never)]
fn undefined_ts_object(kind: &str, names: &[&str]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "text search {} \"{}\" does not exist",
            kind,
            names.join(".")
        ))
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
    )
}

pub fn get_ts_config_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let (schemaname, config_name) = DeconstructQualifiedName(names)?;
    let mut cfgoid = InvalidOid;
    if let Some(schemaname) = schemaname {
        let namespace_id = LookupExplicitNamespace(schemaname, missing_ok)?;
        if !(missing_ok && !OidIsValid(namespace_id)) {
            cfgoid = syscache_seams::lookup_pg_ts_config_oid_by_name_nsp::call(
                config_name,
                namespace_id,
            )?;
        }
    } else {
        recomputeNamespacePath()?;
        let mtn = my_temp_namespace();
        for i in 0..base_path_len() {
            let namespace_id = base_path_nth(i);
            if namespace_id == mtn {
                continue;
            }
            cfgoid = syscache_seams::lookup_pg_ts_config_oid_by_name_nsp::call(
                config_name,
                namespace_id,
            )?;
            if OidIsValid(cfgoid) {
                break;
            }
        }
    }
    if !OidIsValid(cfgoid) && !missing_ok {
        return Err(undefined_ts_object("configuration", names));
    }
    Ok(cfgoid)
}

fn get_ts_object_oid_cached(
    cache_id: i32,
    noun: &str,
    names: &[&str],
    missing_ok: bool,
) -> PgResult<Oid> {
    let (schemaname, objname) = DeconstructQualifiedName(names)?;
    let mut oid = InvalidOid;
    if let Some(schemaname) = schemaname {
        let namespace_id = LookupExplicitNamespace(schemaname, missing_ok)?;
        if !(missing_ok && !OidIsValid(namespace_id)) {
            oid = ts_cache_lookup(cache_id, objname, namespace_id)?;
        }
    } else {
        recomputeNamespacePath()?;
        let mtn = my_temp_namespace();
        for i in 0..base_path_len() {
            let namespace_id = base_path_nth(i);
            if namespace_id == mtn {
                continue;
            }
            oid = ts_cache_lookup(cache_id, objname, namespace_id)?;
            if OidIsValid(oid) {
                break;
            }
        }
    }
    if !OidIsValid(oid) && !missing_ok {
        return Err(undefined_ts_object(noun, names));
    }
    Ok(oid)
}

fn ts_cache_lookup(cache_id: i32, objname: &str, namespace_id: Oid) -> PgResult<Oid> {
    cache_syscache::GetSysCacheOid(
        cache_id,
        1,
        cache_syscache::SysCacheKey::Str(objname),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(namespace_id)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )
}

pub fn get_ts_parser_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    get_ts_object_oid_cached(
        cache_syscache::cacheinfo::TSPARSERNAMENSP,
        "parser",
        names,
        missing_ok,
    )
}

pub fn get_ts_template_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    get_ts_object_oid_cached(
        cache_syscache::cacheinfo::TSTEMPLATENAMENSP,
        "template",
        names,
        missing_ok,
    )
}

pub fn get_ts_dict_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let (schemaname, dict_name) = DeconstructQualifiedName(names)?;
    let mut dictoid = InvalidOid;
    if let Some(schemaname) = schemaname {
        let namespace_id = LookupExplicitNamespace(schemaname, missing_ok)?;
        if !(missing_ok && !OidIsValid(namespace_id)) {
            dictoid =
                syscache_seams::lookup_pg_ts_dict_oid_by_name_nsp::call(dict_name, namespace_id)?;
        }
    } else {
        recomputeNamespacePath()?;
        let mtn = my_temp_namespace();
        for i in 0..base_path_len() {
            let namespace_id = base_path_nth(i);
            if namespace_id == mtn {
                continue;
            }
            dictoid =
                syscache_seams::lookup_pg_ts_dict_oid_by_name_nsp::call(dict_name, namespace_id)?;
            if OidIsValid(dictoid) {
                break;
            }
        }
    }
    if !OidIsValid(dictoid) && !missing_ok {
        return Err(undefined_ts_object("dictionary", names));
    }
    Ok(dictoid)
}

pub fn get_conversion_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let (schemaname, conversion_name) = DeconstructQualifiedName(names)?;
    let mut conoid = InvalidOid;
    if let Some(schemaname) = schemaname {
        let namespace_id = LookupExplicitNamespace(schemaname, missing_ok)?;
        if !(missing_ok && !OidIsValid(namespace_id)) {
            conoid = syscache_seams::lookup_pg_conversion_oid_by_name_nsp::call(
                conversion_name,
                namespace_id,
            )?;
        }
    } else {
        recomputeNamespacePath()?;
        let mtn = my_temp_namespace();
        for i in 0..base_path_len() {
            let namespace_id = base_path_nth(i);
            if namespace_id == mtn {
                continue;
            }
            conoid = syscache_seams::lookup_pg_conversion_oid_by_name_nsp::call(
                conversion_name,
                namespace_id,
            )?;
            if OidIsValid(conoid) {
                break;
            }
        }
    }
    if !OidIsValid(conoid) && !missing_ok {
        return Err(Box::new(
            types_error::PgError::error(format!(
                "conversion \"{}\" does not exist",
                names.join(".")
            ))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(conoid)
}

// TypenameGetTypidExtended (namespace.c).
pub fn OpclassnameGetOpcid(amid: Oid, opcname: &str) -> PgResult<Oid> {
    recomputeNamespacePath()?;
    let mtn = my_temp_namespace();
    for i in 0..base_path_len() {
        let namespaceId = base_path_nth(i);
        if namespaceId == mtn {
            continue;
        }
        let opcid =
            syscache_seams::lookup_pg_opclass_oid_by_name::call(amid, opcname, namespaceId)?;
        if OidIsValid(opcid) {
            return Ok(opcid);
        }
    }
    Ok(InvalidOid)
}

pub fn OpfamilynameGetOpfid(amid: Oid, opfname: &str) -> PgResult<Oid> {
    recomputeNamespacePath()?;
    let mtn = my_temp_namespace();
    for i in 0..base_path_len() {
        let namespaceId = base_path_nth(i);
        if namespaceId == mtn {
            continue;
        }
        let opfid = syscache_seams::lookup_pg_opfamily_oid_exact::call(amid, opfname, namespaceId)?;
        if OidIsValid(opfid) {
            return Ok(opfid);
        }
    }
    Ok(InvalidOid)
}

pub fn TypenameGetTypidExtended(typname: &str, temp_ok: bool) -> PgResult<Oid> {
    recomputeNamespacePath()?;
    let mtn = my_temp_namespace();
    for i in 0..base_path_len() {
        let namespace_id = base_path_nth(i);
        if !temp_ok && namespace_id == mtn {
            continue;
        }
        let typid = syscache_seams::lookup_pg_type_oid_by_name::call(typname, namespace_id)?;
        if OidIsValid(typid) {
            return Ok(typid);
        }
    }
    Ok(InvalidOid)
}

// TypeIsVisible (namespace.c): first path entry owning the name decides.
pub fn TypeIsVisible(typid: Oid) -> PgResult<bool> {
    let Some(t) = syscache_seams::pg_type_domain_shape::call(typid)? else {
        return Err(Box::new(types_error::PgError::error(format!(
            "cache lookup failed for type {typid}"
        ))));
    };
    let typname = core::str::from_utf8(t.typname.name_str())
        .unwrap_or_else(|_| panic!("non-UTF-8 type name"));
    recomputeNamespacePath()?;
    for i in 0..base_path_len() {
        let namespace_id = base_path_nth(i);
        if namespace_id == t.typnamespace {
            return Ok(true);
        }
        if OidIsValid(syscache_seams::lookup_pg_type_oid_by_name::call(
            typname,
            namespace_id,
        )?) {
            return Ok(false);
        }
    }
    Ok(false)
}

pub fn RangeVarGetRelid(
    relation: &RangeVar<'_>,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Oid> {
    let flags = if missing_ok { RVR_MISSING_OK } else { 0 };
    RangeVarGetRelidExtended(relation, lockmode, flags, None)
}

pub fn RangeVarGetRelidExtended(
    relation: &RangeVar<'_>,
    lockmode: LOCKMODE,
    flags: u32,
    mut callback: RangeVarGetRelidCallback<'_>,
) -> PgResult<Oid> {
    let mut relId;
    let mut oldRelId = InvalidOid;
    let mut retry = false;
    let missing_ok = (flags & RVR_MISSING_OK) != 0;

    debug_assert!(!((flags & RVR_NOWAIT) != 0 && (flags & RVR_SKIP_LOCKED) != 0));

    if let Some(catalogname) = relation.catalogname {
        let dbname =
            dbcommands_seams::get_database_name::call(init_small::globals::MyDatabaseId())?;
        if dbname.as_deref() != Some(catalogname) {
            return Err(cross_database_reference(relation));
        }
    }

    // DDL can change a name lookup's answer; retry until the locked OID and
    // the resolved OID agree with no invalidations in between (C comment).
    loop {
        let inval_count = sinval::SharedInvalidMessageCounter();

        if relation.relpersistence == RELPERSISTENCE_TEMP {
            if !OidIsValid(my_temp_namespace()) {
                relId = InvalidOid;
            } else {
                if let Some(schemaname) = relation.schemaname {
                    let namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
                    if namespaceId != my_temp_namespace() {
                        return Err(temp_table_schema_name());
                    }
                }
                relId = lsyscache::get_relname_relid(relation.relname, my_temp_namespace())?;
            }
        } else if let Some(schemaname) = relation.schemaname {
            let namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
            if missing_ok && !OidIsValid(namespaceId) {
                relId = InvalidOid;
            } else {
                relId = lsyscache::get_relname_relid(relation.relname, namespaceId)?;
            }
        } else {
            relId = RelnameGetRelid(relation.relname)?;
        }

        if let Some(cb) = callback.as_deref_mut() {
            cb(relation, relId, oldRelId)?;
        }

        if lockmode == NoLock {
            break;
        }

        if retry {
            if relId == oldRelId {
                break;
            }
            if OidIsValid(oldRelId) {
                lmgr_seams::unlock_relation_oid::call(oldRelId, lockmode)?;
            }
        }

        if !OidIsValid(relId) {
            inval_seams::accept_invalidation_messages::call()?;
        } else if (flags & (RVR_NOWAIT | RVR_SKIP_LOCKED)) == 0 {
            lmgr_seams::lock_relation_oid::call(relId, lockmode)?;
        } else if !lmgr_seams::conditional_lock_relation_oid::call(relId, lockmode)? {
            if (flags & RVR_SKIP_LOCKED) != 0 {
                // C ereports DEBUG1 here; no elog-level channel below ERROR.
                return Ok(InvalidOid);
            }
            let msg = match relation.schemaname {
                Some(schema) => format!(
                    "could not obtain lock on relation \"{schema}.{}\"",
                    relation.relname
                ),
                None => format!("could not obtain lock on relation \"{}\"", relation.relname),
            };
            return Err(::elog::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_LOCK_NOT_AVAILABLE)
                .errmsg(msg)
                .into_error()
                .into());
        }

        if inval_count == sinval::SharedInvalidMessageCounter() {
            break;
        }

        retry = true;
        oldRelId = relId;
    }

    if !OidIsValid(relId) && !missing_ok {
        return Err(undefined_relation(relation));
    }
    Ok(relId)
}

pub struct FuncCandidate<'mcx> {
    pub oid: Oid,
    pub nargs: i16,
    pub nominal_nargs: i16,
    pub nvargs: i16,
    pub ndargs: i16,
    pub va_elem_type: Oid,
    pathpos: i32,
    // argnumbers[k] = proargtypes/proallargtypes index of the k'th call
    // argument; Some whenever argnames was non-empty (C's non-NULL array).
    pub argnumbers: Option<mcx::PgVec<'mcx, i32>>,
    pub args: mcx::PgVec<'mcx, Oid>,
}

const FUNC_MAX_ARGS: usize = 100;
const FUNC_PARAM_IN: i8 = b'i' as i8;
const FUNC_PARAM_INOUT: i8 = b'b' as i8;
const FUNC_PARAM_VARIADIC: i8 = b'v' as i8;

// MatchNamedCall (namespace.c): Ok(None) is C's `return false`.
fn MatchNamedCall<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    arrays: &syscache_seams::PgProcResultArraysShape<'mcx>,
    proc_pronargs: i16,
    pronargdefaults: i16,
    nargs: i16,
    argnames: &[&str],
    include_out_arguments: bool,
    pronargs: i16,
) -> PgResult<Option<mcx::PgVec<'mcx, i32>>> {
    debug_assert!(!argnames.is_empty());
    debug_assert!(nargs as usize >= argnames.len());
    debug_assert!(nargs <= pronargs);
    let numposargs = nargs as usize - argnames.len();

    let Some(p_argnames) = arrays.proargnames.as_ref() else {
        return Ok(None);
    };
    // get_func_arg_info (funcapi.c): all-args count comes from proallargtypes
    // when present, else proargtypes.
    let pronallargs = arrays
        .proallargtypes
        .as_ref()
        .map_or(proc_pronargs as usize, |v| v.len());
    let p_argmodes = arrays.proargmodes.as_ref();
    debug_assert!(if include_out_arguments {
        pronargs as usize == pronallargs
    } else {
        pronargs as usize <= pronallargs
    });
    let pronargs = pronargs as usize;

    let mut argnumbers: mcx::PgVec<'mcx, i32> = mcx::vec_with_capacity_in(mcx, pronargs)?;
    let mut arggiven = [false; FUNC_MAX_ARGS];

    for ap in 0..numposargs {
        argnumbers.push(ap as i32);
        arggiven[ap] = true;
    }

    for argname in argnames {
        let mut pp = 0usize;
        let mut found = false;
        for i in 0..pronallargs {
            if !include_out_arguments {
                if let Some(modes) = p_argmodes {
                    let m = modes[i];
                    if m != FUNC_PARAM_IN && m != FUNC_PARAM_INOUT && m != FUNC_PARAM_VARIADIC {
                        continue;
                    }
                }
            }
            if p_argnames[i].as_str() == *argname {
                if arggiven[pp] {
                    return Ok(None);
                }
                arggiven[pp] = true;
                argnumbers.push(pp as i32);
                found = true;
                break;
            }
            pp += 1;
        }
        if !found {
            return Ok(None);
        }
    }
    debug_assert_eq!(argnumbers.len(), nargs as usize);

    if (nargs as usize) < pronargs {
        let first_arg_with_default = pronargs as i32 - pronargdefaults as i32;
        for pp in numposargs..pronargs {
            if arggiven[pp] {
                continue;
            }
            if (pp as i32) < first_arg_with_default {
                return Ok(None);
            }
            argnumbers.push(pp as i32);
        }
    }
    debug_assert_eq!(argnumbers.len(), pronargs);

    Ok(Some(argnumbers))
}

// FuncnameGetCandidates (namespace.c) with argnames = NIL,
// include_out_arguments = false, missing_ok = false. An oid of InvalidOid
// marks C's ambiguous-set placeholder.
pub fn FuncnameGetCandidates<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    names: &[&str],
    nargs: i16,
    argnames: &[&str],
    expand_variadic: bool,
    expand_defaults: bool,
) -> PgResult<mcx::PgVec<'mcx, FuncCandidate<'mcx>>> {
    FuncnameGetCandidatesExtended(
        mcx,
        names,
        nargs,
        argnames,
        expand_variadic,
        expand_defaults,
        false,
        false,
    )
}

pub fn FuncnameGetCandidatesExtended<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    names: &[&str],
    nargs: i16,
    argnames: &[&str],
    expand_variadic: bool,
    expand_defaults: bool,
    include_out_arguments: bool,
    missing_ok: bool,
) -> PgResult<mcx::PgVec<'mcx, FuncCandidate<'mcx>>> {
    // nargs == -1: any arity, no variadic/default expansion (C convention).
    let (schemaname, funcname) = DeconstructQualifiedName(names)?;

    let namespace_id = match schemaname {
        Some(schemaname) => {
            let id = LookupExplicitNamespace(schemaname, missing_ok)?;
            if id == InvalidOid {
                return Ok(mcx::PgVec::new_in(mcx));
            }
            Some(id)
        }
        None => {
            recomputeNamespacePath()?;
            None
        }
    };

    let raw = syscache_seams::lookup_pg_proc_name_candidates::call(mcx, funcname)?;
    let mut result: mcx::PgVec<'mcx, FuncCandidate<'mcx>> = mcx::PgVec::new_in(mcx);
    let mut any_special = false;
    let mtn = my_temp_namespace();
    for cand in raw {
        let mut pronargs = cand.pronargs;
        let mut pathpos: i32 = 0;
        match namespace_id {
            Some(id) => {
                if cand.pronamespace != id {
                    continue;
                }
            }
            None => {
                let mut found = false;
                for i in 0..base_path_len() {
                    if cand.pronamespace == base_path_nth(i) && cand.pronamespace != mtn {
                        found = true;
                        break;
                    }
                    pathpos += 1;
                }
                if !found {
                    continue;
                }
            }
        }

        // C reads proallargtypes/proargmodes/proargnames off the tuple in
        // hand; the equivalent PROCOID re-probe cannot miss under the list.
        let arrays = if include_out_arguments || !argnames.is_empty() {
            Some(
                syscache_seams::pg_proc_result_arrays::call(mcx, cand.oid)?.unwrap_or_else(|| {
                    panic!(
                        "cache lookup failed for function {} (namespace.c)",
                        cand.oid
                    )
                }),
            )
        } else {
            None
        };

        let mut proargtypes: &[Oid] = cand.proargtypes.as_slice();
        if include_out_arguments {
            if let Some(all) = arrays.as_ref().and_then(|a| a.proallargtypes.as_ref()) {
                pronargs = all.len() as i16;
                debug_assert!(pronargs >= cand.pronargs);
                proargtypes = all.as_slice();
            }
        }

        let variadic;
        let va_elem_type;
        let use_defaults;
        let argnumbers;
        if !argnames.is_empty() {
            // Named/mixed notation cannot match a variadic function when
            // expand_variadic is on: the expanded parameters are nameless.
            if OidIsValid(cand.provariadic) && expand_variadic {
                continue;
            }
            va_elem_type = InvalidOid;
            variadic = false;
            debug_assert!(nargs >= 0);

            use_defaults = if pronargs > nargs && expand_defaults {
                if nargs + cand.pronargdefaults < pronargs {
                    continue;
                }
                true
            } else {
                false
            };

            if pronargs != nargs && !use_defaults {
                continue;
            }

            let Some(nums) = MatchNamedCall(
                mcx,
                arrays.as_ref().unwrap(),
                cand.pronargs,
                cand.pronargdefaults,
                nargs,
                argnames,
                include_out_arguments,
                pronargs,
            )?
            else {
                continue;
            };
            argnumbers = Some(nums);
            any_special = true;
        } else {
            // C considers variadic expansion only when pronargs <= nargs; an
            // undersupplied variadic candidate falls through to the arg-count
            // skip (e.g. rank() never sees the hypothetical-set aggregate 3986).
            let (v, vet) = if pronargs <= nargs && expand_variadic {
                (OidIsValid(cand.provariadic), cand.provariadic)
            } else {
                (false, InvalidOid)
            };
            variadic = v;
            va_elem_type = vet;
            any_special |= variadic;

            use_defaults = pronargs > nargs && expand_defaults && {
                if nargs + cand.pronargdefaults < pronargs {
                    continue;
                }
                any_special = true;
                true
            };

            if nargs >= 0 && pronargs != nargs && !variadic && !use_defaults {
                continue;
            }
            argnumbers = None;
        }

        let effective_nargs = pronargs.max(nargs);
        let mut args = mcx::vec_with_capacity_in(mcx, effective_nargs as usize)?;
        match &argnumbers {
            // C: re-order the argument types into the call's logical order.
            Some(nums) => {
                for j in 0..pronargs as usize {
                    args.push(proargtypes[nums[j] as usize]);
                }
            }
            None => {
                for &a in proargtypes.iter() {
                    args.push(a);
                }
            }
        }
        let nvargs = if variadic {
            // C: expand the variadic slot into N copies of the element type.
            args.truncate(pronargs as usize - 1);
            while args.len() < effective_nargs as usize {
                args.push(va_elem_type);
            }
            effective_nargs - pronargs + 1
        } else {
            0
        };
        let ndargs = if use_defaults { pronargs - nargs } else { 0 };
        let new = FuncCandidate {
            oid: cand.oid,
            nargs: effective_nargs,
            nominal_nargs: pronargs,
            nvargs,
            ndargs,
            va_elem_type: if variadic { va_elem_type } else { InvalidOid },
            pathpos,
            argnumbers,
            args,
        };

        // C ignores defaulted arguments when deciding what is a duplicate,
        // prefers the earlier-in-path then the non-variadic form, and marks
        // an undecidable pair ambiguous (oid = InvalidOid, new one dropped).
        if !result.is_empty() && (any_special || namespace_id.is_none()) {
            let cmp_nargs = (new.nargs - new.ndargs) as usize;
            let prev = result.iter().position(|p| {
                cmp_nargs == (p.nargs - p.ndargs) as usize
                    && new.args[..cmp_nargs] == p.args[..cmp_nargs]
            });
            if let Some(prev) = prev {
                let preference = if pathpos != result[prev].pathpos {
                    pathpos - result[prev].pathpos
                } else if variadic && result[prev].nvargs == 0 {
                    1
                } else if !variadic && result[prev].nvargs > 0 {
                    -1
                } else {
                    0
                };
                if preference > 0 {
                    continue;
                } else if preference < 0 {
                    result.remove(prev);
                } else {
                    result[prev].oid = InvalidOid;
                    continue;
                }
            }
        }
        result.push(new);
    }
    // C prepends; head-first order = reverse acceptance order.
    result.reverse();
    Ok(result)
}
