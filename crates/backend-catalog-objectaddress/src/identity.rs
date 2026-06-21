//! F3 — `getObjectIdentity[Parts]` and the per-class identity helpers
//! (objectaddress.c 4824-6130).
//!
//! Assembles the dotted-identity string and, for `getObjectIdentityParts`, the
//! C `List **objname` / `List **objargs` out-parameters (modeled as Rust
//! out-vectors). Per-class catalog projections and the cross-unit formatters
//! (`format_type_extended` / `format_procedure*` / `format_operator*` /
//! `quote_identifier` / `get_namespace_name_or_temp` / …) cross through their
//! owners' `-seams` crates (mirror-and-panic until each owner lands). Builds on
//! F0's class-id constants ([`crate::consts`]).

use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid};
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::{PgResult, ERROR};

use backend_utils_error::ereport;

use crate::consts::*;

// --- cross-unit seam aliases (mirror-and-panic until each owner lands) ---
use backend_utils_adt_format_type_seams as format_type;
use backend_utils_adt_misc2_seams as regproc;
use backend_utils_adt_ruleutils_seams as ruleutils;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_commands_dbcommands_seams as dbcommands;
use backend_commands_extension_seams as extension;
use backend_commands_tablespace_seams as tablespace;
use backend_commands_user_seams as user;
use backend_foreign_foreign_seams as foreign;
use backend_catalog_pg_largeobject_seams as pg_largeobject;

/// The C `List **objname` / `List **objargs` out-parameters of
/// `getObjectIdentityParts`, modeled as owned out-vectors of strings.
#[derive(Debug, Default)]
pub struct ObjectIdentityParts {
    /// `*objname` — the qualified-name components.
    pub objname: Vec<String>,
    /// `*objargs` — the argument-type components (empty when the C passes NULL
    /// or the object has no args).
    pub objargs: Vec<String>,
}

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `elog(ERROR, fmt, ...)` rendered as an owned internal error.
fn elog_error(msg: String) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

/// `getObjectIdentity(const ObjectAddress *object, bool missing_ok)`
/// (objectaddress.c 4824): the canonical dotted identity string, ignoring the
/// name/args breakdown. `Ok(None)` mirrors the C NULL for a vanished object
/// under `missing_ok`.
pub fn get_object_identity<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // return getObjectIdentityParts(object, NULL, NULL, missing_ok);
    match get_object_identity_parts_inner(mcx, object, false, missing_ok)? {
        Some((s, _)) => Ok(Some(s)),
        None => Ok(None),
    }
}

/// `getObjectIdentityParts(const ObjectAddress *object, List **objname, List
/// **objargs, bool missing_ok)` (objectaddress.c 4839; ~41 arms): the identity
/// string plus the name/args breakdown. The C out-params are returned in
/// [`ObjectIdentityParts`] alongside the identity string. `Ok(None)` mirrors
/// the C NULL for a vanished object under `missing_ok`.
pub fn get_object_identity_parts<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<(PgString<'mcx>, ObjectIdentityParts)>> {
    // The public wrapper always wants the parts breakdown (objname/objargs
    // non-NULL).
    get_object_identity_parts_inner(mcx, object, true, missing_ok)
}

/// The shared body of `getObjectIdentityParts`. `want_parts` mirrors whether
/// the C `objname`/`objargs` out-pointers are non-NULL (`PointerIsValid`); when
/// `false` the per-class arms skip building the name/args lists exactly as the
/// C `if (objname)` guards do.
fn get_object_identity_parts_inner<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    want_parts: bool,
    missing_ok: bool,
) -> PgResult<Option<(PgString<'mcx>, ObjectIdentityParts)>> {
    // StringInfoData buffer; initStringInfo(&buffer);
    let mut buffer = String::new();
    // Assert(PointerIsValid(objname) == PointerIsValid(objargs)); both are
    // initialized to NIL together (modeled by the default empty vectors).
    let mut parts = ObjectIdentityParts::default();

    let class_id = object.classId;

    if class_id == RelationRelationId {
        // Check for the attribute first, so as if it is missing we can skip the
        // entire relation description.
        let mut attr: Option<PgString<'mcx>> = None;
        if object.objectSubId != 0 {
            attr = lsyscache::get_attname::call(
                mcx,
                object.objectId,
                object.objectSubId as i16,
                missing_ok,
            )?;
            if missing_ok && attr.is_none() {
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
        }

        get_relation_identity(mcx, &mut buffer, object.objectId, want_parts, &mut parts, missing_ok)?;
        if want_parts && parts.objname.is_empty() {
            return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
        }

        if let Some(attr) = attr {
            let q = ruleutils::quote_identifier::call(mcx, attr.as_str())?;
            buffer.push('.');
            buffer.push_str(q.as_str());
            if want_parts {
                parts.objname.push(attr.as_str().to_string());
            }
        }
    } else if class_id == ProcedureRelationId {
        let flags = regproc_proc_force_qualify() | regproc_proc_invalid_as_null();
        let proname = regproc::format_procedure_extended::call(mcx, object.objectId, flags)?;
        match proname {
            None => return finish(mcx, buffer, want_parts, parts, missing_ok, class_id),
            Some(proname) => {
                buffer.push_str(proname.as_str());
                if want_parts {
                    if let Some((objname, objargs)) =
                        regproc::format_procedure_parts::call(mcx, object.objectId, missing_ok)?
                    {
                        parts.objname = pgvec_to_strings(&objname);
                        parts.objargs = pgvec_to_strings(&objargs);
                    }
                }
            }
        }
    } else if class_id == TypeRelationId {
        let flags = format_type::FORMAT_TYPE_INVALID_AS_NULL | format_type::FORMAT_TYPE_FORCE_QUALIFY;
        let typeout = format_type::format_type_extended::call(mcx, object.objectId, -1, flags)?;
        match typeout {
            None => return finish(mcx, buffer, want_parts, parts, missing_ok, class_id),
            Some(typeout) => {
                buffer.push_str(typeout.as_str());
                if want_parts {
                    parts.objname = vec![typeout.as_str().to_string()];
                }
            }
        }
    } else if class_id == CastRelationId {
        match crate::resolve::cast_source_target(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for cast {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((castsource, casttarget)) => {
                let src = format_type::format_type_be_qualified::call(mcx, castsource)?;
                let tgt = format_type::format_type_be_qualified::call(mcx, casttarget)?;
                buffer.push_str(&format!("({} AS {})", src.as_str(), tgt.as_str()));
                if want_parts {
                    parts.objname = vec![src.as_str().to_string()];
                    parts.objargs = vec![tgt.as_str().to_string()];
                }
            }
        }
    } else if class_id == CollationRelationId {
        match syscache::collation_namespace_and_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for collation {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(row) => {
                let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
                let q = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    row.name.as_str(),
                )?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname =
                        vec![schema.as_str().to_string(), row.name.as_str().to_string()];
                }
            }
        }
    } else if class_id == ConstraintRelationId {
        match syscache::constraint_identity::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for constraint {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((conname, conrelid, contypid)) => {
                let conname_q = ruleutils::quote_identifier::call(mcx, conname.as_str())?;
                if oid_is_valid(conrelid) {
                    buffer.push_str(&format!("{} on ", conname_q.as_str()));
                    get_relation_identity(
                        mcx, &mut buffer, conrelid, want_parts, &mut parts, false,
                    )?;
                    if want_parts {
                        parts.objname.push(conname.as_str().to_string());
                    }
                } else {
                    // Assert(OidIsValid(con->contypid));
                    let domain = ObjectAddress {
                        classId: TypeRelationId,
                        objectId: contypid,
                        objectSubId: 0,
                    };
                    let (dident, dparts) =
                        get_object_identity_parts_inner(mcx, &domain, want_parts, false)?
                            .ok_or_else(|| {
                                elog_error(format!(
                                    "cache lookup failed for constraint domain {contypid}"
                                ))
                            })?;
                    if want_parts {
                        parts = dparts;
                    }
                    buffer.push_str(&format!("{} on {}", conname_q.as_str(), dident.as_str()));
                    if want_parts {
                        parts.objargs.push(conname.as_str().to_string());
                    }
                }
            }
        }
    } else if class_id == ConversionRelationId {
        match syscache::conversion_namespace_and_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for conversion {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(row) => {
                let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
                let q = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    row.name.as_str(),
                )?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname =
                        vec![schema.as_str().to_string(), row.name.as_str().to_string()];
                }
            }
        }
    } else if class_id == AttrDefaultRelationId {
        let col = syscache::attr_default_column::call(object.objectId)?;
        match col {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for attrdef {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((adrelid, adnum)) => {
                let colobject = ObjectAddress {
                    classId: RelationRelationId,
                    objectId: adrelid,
                    objectSubId: adnum as i32,
                };
                let (cident, cparts) =
                    get_object_identity_parts_inner(mcx, &colobject, want_parts, false)?
                        .ok_or_else(|| {
                            elog_error(format!("could not find tuple for attrdef {}", object.objectId))
                        })?;
                if want_parts {
                    parts = cparts;
                }
                buffer.push_str(&format!("for {}", cident.as_str()));
            }
        }
    } else if class_id == LanguageRelationId {
        match syscache::language_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for language {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(lanname) => {
                let q = ruleutils::quote_identifier::call(mcx, lanname.as_str())?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname = vec![lanname.as_str().to_string()];
                }
            }
        }
    } else if class_id == LargeObjectRelationId {
        if !pg_largeobject::large_object_exists_with_snapshot::call(object.objectId, None)? {
            return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
        }
        buffer.push_str(&object.objectId.to_string());
        if want_parts {
            parts.objname = vec![object.objectId.to_string()];
        }
    } else if class_id == OperatorRelationId {
        let flags = regproc_operator_force_qualify() | regproc_operator_invalid_as_null();
        let oprname = regproc::format_operator_extended::call(mcx, object.objectId, flags)?;
        match oprname {
            None => return finish(mcx, buffer, want_parts, parts, missing_ok, class_id),
            Some(oprname) => {
                buffer.push_str(oprname.as_str());
                if want_parts {
                    if let Some((objname, objargs)) =
                        regproc::format_operator_parts::call(mcx, object.objectId, missing_ok)?
                    {
                        parts.objname = pgvec_to_strings(&objname);
                        parts.objargs = pgvec_to_strings(&objargs);
                    }
                }
            }
        }
    } else if class_id == OperatorClassRelationId {
        match syscache::opclass_namespace_method_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for opclass {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((opcnamespace, opcmethod, opcname)) => {
                let schema = get_namespace_name_or_temp_required(mcx, opcnamespace)?;
                let amname = lsyscache::get_am_name::call(mcx, opcmethod)?.ok_or_else(|| {
                    elog_error(format!("cache lookup failed for access method {opcmethod}"))
                })?;
                let q_qual = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    opcname.as_str(),
                )?;
                let q_am = ruleutils::quote_identifier::call(mcx, amname.as_str())?;
                buffer.push_str(&format!("{} USING {}", q_qual.as_str(), q_am.as_str()));
                if want_parts {
                    parts.objname = vec![
                        amname.as_str().to_string(),
                        schema.as_str().to_string(),
                        opcname.as_str().to_string(),
                    ];
                }
            }
        }
    } else if class_id == OperatorFamilyRelationId {
        get_op_family_identity(
            mcx, &mut buffer, object.objectId, want_parts, &mut parts, missing_ok,
        )?;
    } else if class_id == AccessMethodRelationId {
        match lsyscache::get_am_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for access method {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(amname) => {
                let q = ruleutils::quote_identifier::call(mcx, amname.as_str())?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname = vec![amname.as_str().to_string()];
                }
            }
        }
    } else if class_id == AccessMethodOperatorRelationId {
        match syscache::amop_identity::call(object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for amop entry {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((amopfamily, amoplefttype, amoprighttype, amopstrategy)) => {
                let mut opfam = String::new();
                get_op_family_identity(
                    mcx, &mut opfam, amopfamily, want_parts, &mut parts, false,
                )?;
                let ltype = format_type::format_type_be_qualified::call(mcx, amoplefttype)?;
                let rtype = format_type::format_type_be_qualified::call(mcx, amoprighttype)?;
                if want_parts {
                    parts.objname.push(amopstrategy.to_string());
                    parts.objargs =
                        vec![ltype.as_str().to_string(), rtype.as_str().to_string()];
                }
                buffer.push_str(&format!(
                    "operator {} ({}, {}) of {}",
                    amopstrategy,
                    ltype.as_str(),
                    rtype.as_str(),
                    opfam
                ));
            }
        }
    } else if class_id == AccessMethodProcedureRelationId {
        match syscache::amproc_identity::call(object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for amproc entry {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((amprocfamily, amproclefttype, amprocrighttype, amprocnum)) => {
                let mut opfam = String::new();
                get_op_family_identity(
                    mcx, &mut opfam, amprocfamily, want_parts, &mut parts, false,
                )?;
                let ltype = format_type::format_type_be_qualified::call(mcx, amproclefttype)?;
                let rtype = format_type::format_type_be_qualified::call(mcx, amprocrighttype)?;
                if want_parts {
                    parts.objname.push(amprocnum.to_string());
                    parts.objargs =
                        vec![ltype.as_str().to_string(), rtype.as_str().to_string()];
                }
                buffer.push_str(&format!(
                    "function {} ({}, {}) of {}",
                    amprocnum,
                    ltype.as_str(),
                    rtype.as_str(),
                    opfam
                ));
            }
        }
    } else if class_id == RewriteRelationId {
        match syscache::rewrite_name_evclass::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for rule {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((rulename, ev_class)) => {
                let q = ruleutils::quote_identifier::call(mcx, rulename.as_str())?;
                buffer.push_str(&format!("{} on ", q.as_str()));
                get_relation_identity(mcx, &mut buffer, ev_class, want_parts, &mut parts, false)?;
                if want_parts {
                    parts.objname.push(rulename.as_str().to_string());
                }
            }
        }
    } else if class_id == TriggerRelationId {
        match syscache::trigger_name_relid::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for trigger {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((tgname, tgrelid)) => {
                let q = ruleutils::quote_identifier::call(mcx, tgname.as_str())?;
                buffer.push_str(&format!("{} on ", q.as_str()));
                get_relation_identity(mcx, &mut buffer, tgrelid, want_parts, &mut parts, false)?;
                if want_parts {
                    parts.objname.push(tgname.as_str().to_string());
                }
            }
        }
    } else if class_id == NamespaceRelationId {
        match lsyscache::get_namespace_name_or_temp::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for namespace {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(nspname) => {
                let q = ruleutils::quote_identifier::call(mcx, nspname.as_str())?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname = vec![nspname.as_str().to_string()];
                }
            }
        }
    } else if class_id == StatisticExtRelationId {
        match syscache::statext_namespace_and_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for statistics object {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(row) => {
                let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
                let q = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    row.name.as_str(),
                )?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname =
                        vec![schema.as_str().to_string(), row.name.as_str().to_string()];
                }
            }
        }
    } else if class_id == TSParserRelationId {
        match syscache::ts_parser_namespace_and_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for text search parser {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(row) => {
                let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
                let q = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    row.name.as_str(),
                )?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname =
                        vec![schema.as_str().to_string(), row.name.as_str().to_string()];
                }
            }
        }
    } else if class_id == TSDictionaryRelationId {
        match syscache::ts_dict_namespace_and_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for text search dictionary {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(row) => {
                let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
                let q = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    row.name.as_str(),
                )?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname =
                        vec![schema.as_str().to_string(), row.name.as_str().to_string()];
                }
            }
        }
    } else if class_id == TSTemplateRelationId {
        match syscache::ts_template_namespace_and_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for text search template {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(row) => {
                let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
                let q = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    row.name.as_str(),
                )?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname =
                        vec![schema.as_str().to_string(), row.name.as_str().to_string()];
                }
            }
        }
    } else if class_id == TSConfigRelationId {
        match syscache::ts_config_namespace_and_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for text search configuration {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(row) => {
                let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
                let q = ruleutils::quote_qualified_identifier::call(
                    mcx,
                    Some(schema.as_str()),
                    row.name.as_str(),
                )?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname =
                        vec![schema.as_str().to_string(), row.name.as_str().to_string()];
                }
            }
        }
    } else if class_id == AuthIdRelationId {
        // GetUserNameFromId(objectId, missing_ok)
        let username = get_user_name_from_id_opt(mcx, object.objectId, missing_ok)?;
        match username {
            None => return finish(mcx, buffer, want_parts, parts, missing_ok, class_id),
            Some(username) => {
                if want_parts {
                    parts.objname = vec![username.as_str().to_string()];
                }
                let q = ruleutils::quote_identifier::call(mcx, username.as_str())?;
                buffer.push_str(q.as_str());
            }
        }
    } else if class_id == AuthMemRelationId {
        match syscache::auth_member_member_role::call(object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for pg_auth_members entry {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((member, roleid)) => {
                let member_name = user::get_user_name_from_id::call(mcx, member, false)?;
                let role_name = user::get_user_name_from_id::call(mcx, roleid, false)?;
                buffer.push_str(&format!(
                    "membership of role {} in role {}",
                    member_name.as_str(),
                    role_name.as_str()
                ));
            }
        }
    } else if class_id == DatabaseRelationId {
        match dbcommands::get_database_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for database {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(datname) => {
                if want_parts {
                    parts.objname = vec![datname.as_str().to_string()];
                }
                let q = ruleutils::quote_identifier::call(mcx, datname.as_str())?;
                buffer.push_str(q.as_str());
            }
        }
    } else if class_id == TableSpaceRelationId {
        match tablespace::get_tablespace_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for tablespace {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(tblspace) => {
                if want_parts {
                    parts.objname = vec![tblspace.as_str().to_string()];
                }
                let q = ruleutils::quote_identifier::call(mcx, tblspace.as_str())?;
                buffer.push_str(q.as_str());
            }
        }
    } else if class_id == ForeignDataWrapperRelationId {
        if let Some(fdw) =
            foreign::get_foreign_data_wrapper_extended::call(mcx, object.objectId, missing_ok)?
        {
            let q = ruleutils::quote_identifier::call(mcx, fdw.fdwname.as_str())?;
            buffer.push_str(q.as_str());
            if want_parts {
                parts.objname = vec![fdw.fdwname.as_str().to_string()];
            }
        }
    } else if class_id == ForeignServerRelationId {
        if let Some(srv) =
            foreign::get_foreign_server_extended::call(mcx, object.objectId, missing_ok)?
        {
            let q = ruleutils::quote_identifier::call(mcx, srv.servername.as_str())?;
            buffer.push_str(q.as_str());
            if want_parts {
                parts.objname = vec![srv.servername.as_str().to_string()];
            }
        }
    } else if class_id == UserMappingRelationId {
        match syscache::user_mapping_user_server::call(object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for user mapping {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((useid, umserver)) => {
                let srv = foreign::get_foreign_server::call(mcx, umserver)?;
                let usename: String = if oid_is_valid(useid) {
                    user::get_user_name_from_id::call(mcx, useid, false)?
                        .as_str()
                        .to_string()
                } else {
                    "public".to_string()
                };
                if want_parts {
                    parts.objname = vec![usename.clone()];
                    parts.objargs = vec![srv.servername.as_str().to_string()];
                }
                let q = ruleutils::quote_identifier::call(mcx, &usename)?;
                buffer.push_str(&format!("{} on server {}", q.as_str(), srv.servername.as_str()));
            }
        }
    } else if class_id == DefaultAclRelationId {
        match syscache::default_acl_identity::call(object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for default ACL {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((defaclrole, defaclnamespace, defaclobjtype)) => {
                let username = user::get_user_name_from_id::call(mcx, defaclrole, false)?;
                let q_user = ruleutils::quote_identifier::call(mcx, username.as_str())?;
                buffer.push_str(&format!("for role {}", q_user.as_str()));

                let schema: Option<PgString<'mcx>> = if oid_is_valid(defaclnamespace) {
                    let schema = get_namespace_name_or_temp_required(mcx, defaclnamespace)?;
                    let q_schema = ruleutils::quote_identifier::call(mcx, schema.as_str())?;
                    buffer.push_str(&format!(" in schema {}", q_schema.as_str()));
                    Some(schema)
                } else {
                    None
                };

                match defaclobjtype {
                    x if x == DEFACLOBJ_RELATION => buffer.push_str(" on tables"),
                    x if x == DEFACLOBJ_SEQUENCE => buffer.push_str(" on sequences"),
                    x if x == DEFACLOBJ_FUNCTION => buffer.push_str(" on functions"),
                    x if x == DEFACLOBJ_TYPE => buffer.push_str(" on types"),
                    x if x == DEFACLOBJ_NAMESPACE => buffer.push_str(" on schemas"),
                    x if x == DEFACLOBJ_LARGEOBJECT => buffer.push_str(" on large objects"),
                    _ => {}
                }

                if want_parts {
                    parts.objname = vec![username.as_str().to_string()];
                    if let Some(schema) = schema {
                        parts.objname.push(schema.as_str().to_string());
                    }
                    parts.objargs = vec![(defaclobjtype as u8 as char).to_string()];
                }
            }
        }
    } else if class_id == ExtensionRelationId {
        match extension::get_extension_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for extension {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(extname) => {
                let q = ruleutils::quote_identifier::call(mcx, extname.as_str())?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname = vec![extname.as_str().to_string()];
                }
            }
        }
    } else if class_id == EventTriggerRelationId {
        match syscache::event_trigger_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for event trigger {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(evtname) => {
                let q = ruleutils::quote_identifier::call(mcx, evtname.as_str())?;
                buffer.push_str(q.as_str());
                if want_parts {
                    parts.objname = vec![evtname.as_str().to_string()];
                }
            }
        }
    } else if class_id == ParameterAclRelationId {
        match syscache::parameter_acl_name::call(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for parameter ACL {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some(parname) => {
                buffer.push_str(parname.as_str());
                if want_parts {
                    parts.objname = vec![parname.as_str().to_string()];
                }
            }
        }
    } else if class_id == PolicyRelationId {
        // objectaddress.c owns pg_policy's by-oid projection (there is no
        // POLICYOID syscache); call the in-crate `policy_relid_name` directly
        // (it returns `(polrelid, polname)`).
        match crate::policy_lookup::policy_relid_name(mcx, object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for policy {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((polrelid, polname)) => {
                let q = ruleutils::quote_identifier::call(mcx, polname.as_str())?;
                buffer.push_str(&format!("{} on ", q.as_str()));
                get_relation_identity(mcx, &mut buffer, polrelid, want_parts, &mut parts, false)?;
                if want_parts {
                    parts.objname.push(polname.as_str().to_string());
                }
            }
        }
    } else if class_id == PublicationRelationId {
        if let Some(pubname) =
            lsyscache::get_publication_name::call(mcx, object.objectId, missing_ok)?
        {
            let q = ruleutils::quote_identifier::call(mcx, pubname.as_str())?;
            buffer.push_str(q.as_str());
            if want_parts {
                parts.objname = vec![pubname.as_str().to_string()];
            }
        }
    } else if class_id == PublicationNamespaceRelationId {
        match get_publication_schema_info(mcx, object, missing_ok)? {
            None => return finish(mcx, buffer, want_parts, parts, missing_ok, class_id),
            Some((pubname, nspname)) => {
                buffer.push_str(&format!(
                    "{} in publication {}",
                    nspname.as_str(),
                    pubname.as_str()
                ));
                if want_parts {
                    // C: `if (objargs) *objargs = list_make1(pubname);` —
                    // objargs is non-NULL exactly when objname is.
                    parts.objargs = vec![pubname.as_str().to_string()];
                    parts.objname = vec![nspname.as_str().to_string()];
                }
            }
        }
    } else if class_id == PublicationRelRelationId {
        match syscache::publication_rel_ids::call(object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "cache lookup failed for publication table {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((prpubid, prrelid)) => {
                let pubname = lsyscache::get_publication_name::call(mcx, prpubid, false)?
                    .ok_or_else(|| {
                        elog_error(format!("cache lookup failed for publication {prpubid}"))
                    })?;
                get_relation_identity(mcx, &mut buffer, prrelid, want_parts, &mut parts, false)?;
                buffer.push_str(&format!(" in publication {}", pubname.as_str()));
                if want_parts {
                    parts.objargs = vec![pubname.as_str().to_string()];
                }
            }
        }
    } else if class_id == SubscriptionRelationId {
        if let Some(subname) =
            lsyscache::get_subscription_name::call(mcx, object.objectId, missing_ok)?
        {
            let q = ruleutils::quote_identifier::call(mcx, subname.as_str())?;
            buffer.push_str(q.as_str());
            if want_parts {
                parts.objname = vec![subname.as_str().to_string()];
            }
        }
    } else if class_id == TransformRelationId {
        match syscache::transform_type_lang::call(object.objectId)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!(
                        "could not find tuple for transform {}",
                        object.objectId
                    )));
                }
                return finish(mcx, buffer, want_parts, parts, missing_ok, class_id);
            }
            Some((trftype, trflang)) => {
                let transform_type = format_type::format_type_be_qualified::call(mcx, trftype)?;
                let transform_lang =
                    syscache::language_name::call(mcx, trflang)?.ok_or_else(|| {
                        elog_error(format!("cache lookup failed for language {trflang}"))
                    })?;
                buffer.push_str(&format!(
                    "for {} language {}",
                    transform_type.as_str(),
                    transform_lang.as_str()
                ));
                if want_parts {
                    parts.objname = vec![transform_type.as_str().to_string()];
                    parts.objargs = vec![transform_lang.as_str().to_string()];
                }
            }
        }
    } else {
        // default: elog(ERROR, "unsupported object class: %u", object->classId);
        return Err(elog_error(format!("unsupported object class: {class_id}")));
    }

    finish(mcx, buffer, want_parts, parts, missing_ok, class_id)
}

/// The shared `getObjectIdentityParts` epilogue (objectaddress.c 6025-6047).
fn finish<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: String,
    want_parts: bool,
    parts: ObjectIdentityParts,
    missing_ok: bool,
    class_id: Oid,
) -> PgResult<Option<(PgString<'mcx>, ObjectIdentityParts)>> {
    if !missing_ok {
        // If a get_object_address() representation was requested, make sure we
        // are providing one. We don't check objargs.
        if want_parts && parts.objname.is_empty() {
            return Err(elog_error(format!(
                "requested object address for unsupported object class {class_id}: text result \"{buffer}\""
            )));
        }
    } else {
        // an empty buffer is equivalent to no object found
        if buffer.is_empty() {
            // Assert((objname is empty) && (objargs is empty));
            return Ok(None);
        }
    }

    Ok(Some((PgString::from_str_in(&buffer, mcx)?, parts)))
}

/// `getOpFamilyIdentity(StringInfo buffer, Oid opfid, List **object, bool
/// missing_ok)` (objectaddress.c 6053).
pub fn get_op_family_identity<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: &mut String,
    opfid: Oid,
    want_parts: bool,
    parts: &mut ObjectIdentityParts,
    missing_ok: bool,
) -> PgResult<()> {
    let (opfnamespace, opfmethod, opfname) =
        match syscache::opfamily_namespace_method_name::call(mcx, opfid)? {
            None => {
                if !missing_ok {
                    return Err(elog_error(format!("cache lookup failed for opfamily {opfid}")));
                }
                return Ok(());
            }
            Some(t) => t,
        };

    let amname = lsyscache::get_am_name::call(mcx, opfmethod)?
        .ok_or_else(|| elog_error(format!("cache lookup failed for access method {opfmethod}")))?;

    let schema = get_namespace_name_or_temp_required(mcx, opfnamespace)?;
    let q_qual = ruleutils::quote_qualified_identifier::call(
        mcx,
        Some(schema.as_str()),
        opfname.as_str(),
    )?;
    // Note: the C appends the *unquoted* amname here (`NameStr(amForm->amname)`).
    buffer.push_str(&format!("{} USING {}", q_qual.as_str(), amname.as_str()));

    if want_parts {
        parts.objname = vec![
            amname.as_str().to_string(),
            schema.as_str().to_string(),
            opfname.as_str().to_string(),
        ];
    }

    Ok(())
}

/// `getRelationIdentity(StringInfo buffer, Oid relid, List **object, bool
/// missing_ok)` (objectaddress.c 6097).
pub fn get_relation_identity<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: &mut String,
    relid: Oid,
    want_parts: bool,
    parts: &mut ObjectIdentityParts,
    missing_ok: bool,
) -> PgResult<()> {
    let row = match syscache::relation_namespace_and_name::call(mcx, relid)? {
        None => {
            if !missing_ok {
                return Err(elog_error(format!("cache lookup failed for relation {relid}")));
            }
            if want_parts {
                parts.objname = Vec::new();
            }
            return Ok(());
        }
        Some(r) => r,
    };

    let schema = get_namespace_name_or_temp_required(mcx, row.namespace)?;
    let q = ruleutils::quote_qualified_identifier::call(
        mcx,
        Some(schema.as_str()),
        row.name.as_str(),
    )?;
    buffer.push_str(q.as_str());
    if want_parts {
        parts.objname = vec![schema.as_str().to_string(), row.name.as_str().to_string()];
    }

    Ok(())
}

/// `getPublicationSchemaInfo(const ObjectAddress *object, bool missing_ok,
/// char **pubname, char **nspname)` (objectaddress.c 2864): a static helper
/// shared by the description/identity families. Returns `Some((pubname,
/// nspname))` (the C `true` with both out-params set) or `None` (the C
/// `false`).
pub(crate) fn get_publication_schema_info<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<(PgString<'mcx>, PgString<'mcx>)>> {
    let (pnpubid, pnnspid) = match syscache::publication_namespace_ids::call(object.objectId)? {
        None => {
            if !missing_ok {
                return Err(elog_error(format!(
                    "cache lookup failed for publication schema {}",
                    object.objectId
                )));
            }
            return Ok(None);
        }
        Some(t) => t,
    };

    let pubname = match lsyscache::get_publication_name::call(mcx, pnpubid, missing_ok)? {
        None => return Ok(None),
        Some(p) => p,
    };

    let nspname = match lsyscache::get_namespace_name::call(mcx, pnnspid)? {
        None => {
            if !missing_ok {
                return Err(elog_error(format!("cache lookup failed for schema {pnnspid}")));
            }
            return Ok(None);
        }
        Some(n) => n,
    };

    Ok(Some((pubname, nspname)))
}

/* ---------------------------------------------------------------------------
 * small local helpers
 * ------------------------------------------------------------------------- */

/// `get_namespace_name_or_temp(nspid)`, raising the catalog `elog(ERROR)` the
/// identity arms expect when a schema vanished (they pass the schema straight
/// into `quote_qualified_identifier`, so a NULL would be a C crash; the arms
/// only reach here on a present catalog row).
fn get_namespace_name_or_temp_required<'mcx>(
    mcx: Mcx<'mcx>,
    nspid: Oid,
) -> PgResult<PgString<'mcx>> {
    lsyscache::get_namespace_name_or_temp::call(mcx, nspid)?
        .ok_or_else(|| elog_error(format!("cache lookup failed for namespace {nspid}")))
}

/// `GetUserNameFromId(roleid, missing_ok)` → `Option` (the C returns NULL under
/// `noerr`).
fn get_user_name_from_id_opt<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    noerr: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    if noerr {
        // Probe existence first so a missing role yields the C NULL rather than
        // an error; the role-name lookup itself then cannot fail.
        match syscache::authid_rolname::call(mcx, roleid)? {
            None => Ok(None),
            Some(_) => Ok(Some(user::get_user_name_from_id::call(mcx, roleid, true)?)),
        }
    } else {
        Ok(Some(user::get_user_name_from_id::call(mcx, roleid, false)?))
    }
}

/// Copy a `PgVec<PgString>` (a `format_*_parts` out-list) into owned `String`s.
fn pgvec_to_strings(v: &mcx::PgVec<'_, PgString<'_>>) -> Vec<String> {
    v.iter().map(|s| s.as_str().to_string()).collect()
}

/* `FORMAT_PROC_*` / `FORMAT_OPERATOR_*` (utils/regproc.h) — the bits16 flag
 * masks the identity arms pass to the regproc formatters. Defined locally as
 * the misc2 owner's `-seams` crate exports only the formatter slots, not the
 * flag constants. */
#[inline]
fn regproc_proc_force_qualify() -> u16 {
    0x02
}
#[inline]
fn regproc_proc_invalid_as_null() -> u16 {
    0x01
}
#[inline]
fn regproc_operator_force_qualify() -> u16 {
    0x02
}
#[inline]
fn regproc_operator_invalid_as_null() -> u16 {
    0x01
}
