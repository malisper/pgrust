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
        result.push(OperCandidate { oid: cand.oid, args: [cand.oprleft, cand.oprright] });
        pathposes.push(pathpos);
    }
    result.reverse();
    Ok(result)
}

// TypenameGetTypidExtended (namespace.c).
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
    pub nominal_nargs: i16,
    pub nvargs: i16,
    pub ndargs: i16,
    pub va_elem_type: Oid,
    pathpos: i32,
    pub args: mcx::PgVec<'mcx, Oid>,
}

// FuncnameGetCandidates (namespace.c), positional notation only (argnames and
// include_out_arguments have no in-tree callers; both stay on the loud path in
// parse_func). An oid of InvalidOid marks C's ambiguous-set placeholder.
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
    let mut any_special = false;
    let mtn = my_temp_namespace();
    for cand in raw {
        let pronargs = cand.pronargs;
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

        // C considers variadic expansion only when pronargs <= nargs; an
        // undersupplied variadic candidate falls through to the arg-count
        // skip (e.g. rank() never sees the hypothetical-set aggregate 3986).
        let (variadic, va_elem_type) = if pronargs <= nargs && expand_variadic {
            (OidIsValid(cand.provariadic), cand.provariadic)
        } else {
            (false, InvalidOid)
        };
        any_special |= variadic;

        let use_defaults = pronargs > nargs && expand_defaults && {
            if nargs + cand.pronargdefaults < pronargs {
                continue;
            }
            any_special = true;
            true
        };

        if pronargs != nargs && !variadic && !use_defaults {
            continue;
        }

        let effective_nargs = pronargs.max(nargs);
        let mut args = mcx::vec_with_capacity_in(mcx, effective_nargs as usize)?;
        for &a in cand.proargtypes.iter() {
            args.push(a);
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
