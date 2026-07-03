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

pub const RVR_MISSING_OK: u32 = 1 << 0;
pub const RVR_NOWAIT: u32 = 1 << 1;
pub const RVR_SKIP_LOCKED: u32 = 1 << 2;

// parsenodes.h ObjectType, verified against REL_18_3.
const OBJECT_SCHEMA: i32 = 36;
const ACL_USAGE: u64 = 1 << 8;
const ACLCHECK_OK: i32 = 0;

pub type RangeVarGetRelidCallback<'a> =
    Option<&'a mut dyn FnMut(&RangeVar<'_>, Oid, Oid) -> PgResult<()>>;

#[cold]
#[inline(never)]
fn undefined_schema(nspname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("schema \"{nspname}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_SCHEMA),
    )
}

#[cold]
#[inline(never)]
fn undefined_relation(relation: &RangeVar<'_>) -> Box<PgError> {
    let msg = match relation.schemaname {
        Some(schema) => format!("relation \"{}.{}\" does not exist", schema, relation.relname),
        None => format!("relation \"{}\" does not exist", relation.relname),
    };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_UNDEFINED_TABLE))
}

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

#[cold]
#[inline(never)]
fn improper_qualified_name(names: &[&str]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "improper qualified name (too many dotted names): {}",
            names.join(".")
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
                opername, oprleft, oprright, namespaceId,
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
        let dbname = dbcommands_seams::get_database_name::call(init_small::globals::MyDatabaseId())?;
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
        } else {
            // No ConditionalLockRelationOid consumer in-tree; the
            // parse/analyze spine never passes these flags.
            crate::deferred("RangeVarGetRelidExtended RVR_NOWAIT/RVR_SKIP_LOCKED");
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
    pub args: mcx::PgVec<'mcx, Oid>,
}

// FuncnameGetCandidates (namespace.c), exact-arity slice: candidates that C
// would only admit via variadic or default-argument expansion panic instead
// of being silently dropped.
pub fn FuncnameGetCandidates<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    names: &[&str],
    nargs: i16,
    expand_variadic: bool,
    expand_defaults: bool,
) -> PgResult<mcx::PgVec<'mcx, FuncCandidate<'mcx>>> {
    debug_assert!(nargs >= 0);
    let (schemaname, funcname) = DeconstructQualifiedName(names)?;

    let namespace_id = match schemaname {
        Some(schemaname) => {
            let id = LookupExplicitNamespace(schemaname, false)?;
            Some(id)
        }
        None => {
            recomputeNamespacePath()?;
            None
        }
    };

    let raw = syscache_seams::lookup_pg_proc_name_candidates::call(mcx, funcname)?;
    let mut result: mcx::PgVec<'mcx, FuncCandidate<'mcx>> = mcx::PgVec::new_in(mcx);
    for cand in raw {
        if OidIsValid(cand.provariadic) && expand_variadic && nargs >= cand.pronargs - 1 {
            panic!(
                "FuncnameGetCandidates (namespace.c): expand_variadic arm unported — \
                 candidate {} for \"{funcname}\" is variadic",
                cand.oid
            );
        }
        if cand.pronargdefaults > 0
            && expand_defaults
            && nargs >= cand.pronargs - cand.pronargdefaults
            && nargs < cand.pronargs
        {
            panic!(
                "FuncnameGetCandidates (namespace.c): expand_defaults arm unported — \
                 candidate {} for \"{funcname}\" needs default-argument expansion",
                cand.oid
            );
        }
        if cand.pronargs != nargs {
            continue;
        }
        let visible = match namespace_id {
            Some(id) => cand.pronamespace == id,
            None => {
                let mut pathpos = None;
                for i in 0..base_path_len() {
                    if base_path_nth(i) == cand.pronamespace {
                        pathpos = Some(i);
                        break;
                    }
                }
                pathpos.is_some()
            }
        };
        if !visible {
            continue;
        }
        if result.iter().any(|prev: &FuncCandidate<'mcx>| prev.args.as_slice() == cand.proargtypes.as_slice()) {
            panic!(
                "FuncnameGetCandidates (namespace.c): same-signature shadowing across \
                 schemas unported — duplicate candidate {} for \"{funcname}\"",
                cand.oid
            );
        }
        let mut args = mcx::vec_with_capacity_in(mcx, cand.proargtypes.len())?;
        for &a in cand.proargtypes.iter() {
            args.push(a);
        }
        result.push(FuncCandidate { oid: cand.oid, nargs: cand.pronargs, args });
    }
    Ok(result)
}
