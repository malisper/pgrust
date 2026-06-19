//! F1 — `getObjectDescription` and friends (objectaddress.c 2912-4219).
//!
//! The ~41 catalog-class description arms assemble a human-readable
//! description into a `String` buffer (the C `StringInfo`), threading per-class
//! catalog reads through the owners' `-seams` crates (`format_type_extended` /
//! `format_procedure_extended` / `format_operator_extended` /
//! `*_namespace_and_name` / `quote_qualified_identifier` / …). Bodies are the
//! faithful C logic; this is what the F0 `get_object_description` seam install
//! routes to.

extern crate alloc;
use alloc::format;
use alloc::string::{String, ToString};

use mcx::{Mcx, PgString};
use types_core::{Oid, OidIsValid, InvalidOid};
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};

use crate::consts::*;
use types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
};

use backend_catalog_namespace_seams as namespace;
use backend_commands_dbcommands_seams as dbcommands;
use backend_commands_extension_seams as extension;
use backend_commands_tablespace_seams as tablespace;
use backend_foreign_foreign_seams as foreign;
use backend_utils_adt_format_type_seams as format_type;
use backend_utils_adt_regproc_seams as regproc;
use backend_utils_adt_ruleutils_seams as ruleutils;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_init_miscinit_seams as miscinit;
use backend_catalog_pg_largeobject_seams as largeobject;

/// `elog(ERROR, ...)` (the catalog-corruption / shouldn't-happen internal
/// errors of `getObjectDescription`).
fn elog_error(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `quote_qualified_identifier(nspname, NameStr(...))` then push onto `buf`.
/// `nspname == None` mirrors the C "visible in search path ⇒ unqualified".
fn push_qualified(
    mcx: Mcx<'_>,
    buf: &mut String,
    prefix: &str,
    nspname: Option<&str>,
    name: &str,
) -> PgResult<()> {
    let qualified = ruleutils::quote_qualified_identifier::call(mcx, nspname, name)?;
    buf.push_str(prefix);
    buf.push_str(qualified.as_str());
    Ok(())
}

/// `getObjectDescription(const ObjectAddress *object, bool missing_ok)`
/// (objectaddress.c 2912): a human-readable description of the object, palloc'd
/// in `mcx`. `Ok(None)` mirrors the C NULL (object vanished under `missing_ok`,
/// or an empty per-class buffer). This is the body the F0
/// `get_object_description` seam install routes to.
pub fn get_object_description<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    let mut buffer = String::new();

    match object.classId {
        RelationRelationId => {
            if object.objectSubId == 0 {
                get_relation_description(mcx, &mut buffer, object.objectId, missing_ok)?;
            } else {
                // column, not whole relation
                let attname = lsyscache::get_attname::call(
                    mcx,
                    object.objectId,
                    object.objectSubId as i16,
                    missing_ok,
                )?;
                let attname = match attname {
                    Some(a) => a,
                    None => return finish(mcx, buffer),
                };
                let mut rel = String::new();
                get_relation_description(mcx, &mut rel, object.objectId, missing_ok)?;
                // translator: second %s is, e.g., "table %s"
                buffer.push_str(&format!("column {} of {}", attname.as_str(), rel));
            }
        }

        ProcedureRelationId => {
            let proname = regproc::format_procedure_extended::call(
                mcx,
                object.objectId,
                FORMAT_PROC_INVALID_AS_NULL,
            )?;
            match proname {
                None => return finish(mcx, buffer),
                Some(proname) => buffer.push_str(&format!("function {}", proname.as_str())),
            }
        }

        TypeRelationId => {
            let typname = format_type::format_type_extended::call(
                mcx,
                object.objectId,
                -1,
                FORMAT_TYPE_INVALID_AS_NULL,
            )?;
            match typname {
                None => return finish(mcx, buffer),
                Some(typname) => buffer.push_str(&format!("type {}", typname.as_str())),
            }
        }

        CastRelationId => {
            match crate::resolve::cast_source_target(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for cast {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((castsource, casttarget)) => {
                    let src = format_type::format_type_be::call(mcx, castsource)?;
                    let tgt = format_type::format_type_be::call(mcx, casttarget)?;
                    buffer.push_str(&format!(
                        "cast from {} to {}",
                        src.as_str(),
                        tgt.as_str()
                    ));
                }
            }
        }

        CollationRelationId => {
            match syscache::collation_namespace_and_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for collation {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(coll) => {
                    let nspname = if namespace::collation_is_visible::call(mcx, object.objectId)? {
                        None
                    } else {
                        lsyscache::get_namespace_name::call(mcx, coll.namespace)?
                    };
                    push_qualified(
                        mcx,
                        &mut buffer,
                        "collation ",
                        nspname.as_deref(),
                        coll.name.as_str(),
                    )?;
                }
            }
        }

        ConstraintRelationId => {
            // SearchSysCache1(CONSTROID) projected to (conrelid, conname).
            match syscache::constraint_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for constraint {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(conname) => {
                    // The owning relation (`conrelid`) comes from the
                    // constraint-type/index projection's sibling read: a
                    // constraint with an owning relation prints "constraint %s
                    // on %s". The conrelid is fetched alongside the name via the
                    // constraint row projection.
                    let conrelid = constraint_conrelid(object.objectId)?;
                    if OidIsValid(conrelid) {
                        let mut rel = String::new();
                        get_relation_description(mcx, &mut rel, conrelid, false)?;
                        // translator: second %s is, e.g., "table %s"
                        buffer.push_str(&format!(
                            "constraint {} on {}",
                            conname.as_str(),
                            rel
                        ));
                    } else {
                        buffer.push_str(&format!("constraint {}", conname.as_str()));
                    }
                }
            }
        }

        ConversionRelationId => {
            match syscache::conversion_namespace_and_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for conversion {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(conv) => {
                    let nspname = if namespace::conversion_is_visible::call(mcx, object.objectId)? {
                        None
                    } else {
                        lsyscache::get_namespace_name::call(mcx, conv.namespace)?
                    };
                    push_qualified(
                        mcx,
                        &mut buffer,
                        "conversion ",
                        nspname.as_deref(),
                        conv.name.as_str(),
                    )?;
                }
            }
        }

        AttrDefaultRelationId => {
            let colobject = syscache::attr_default_column::call(object.objectId)?;
            match colobject {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for attrdef {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((adrelid, adnum)) => {
                    let col = ObjectAddress {
                        classId: RelationRelationId,
                        objectId: adrelid,
                        objectSubId: adnum as i32,
                    };
                    let desc = get_object_description(mcx, &col, false)?;
                    let desc = desc
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                    // translator: %s is typically "column %s of table %s"
                    buffer.push_str(&format!("default value for {}", desc));
                }
            }
        }

        LanguageRelationId => {
            let langname = syscache::language_name::call(mcx, object.objectId)?;
            if !missing_ok && langname.is_none() {
                return Err(elog_error(format!(
                    "cache lookup failed for language {}",
                    object.objectId
                )));
            }
            if let Some(langname) = langname {
                buffer.push_str(&format!("language {}", langname.as_str()));
            }
        }

        LargeObjectRelationId => {
            if !largeobject::large_object_exists_with_snapshot::call(object.objectId, None)? {
                return finish(mcx, buffer);
            }
            buffer.push_str(&format!("large object {}", object.objectId));
        }

        OperatorRelationId => {
            let oprname = regproc::format_operator_extended::call(
                mcx,
                object.objectId,
                FORMAT_OPERATOR_INVALID_AS_NULL,
            )?;
            match oprname {
                None => return finish(mcx, buffer),
                Some(oprname) => buffer.push_str(&format!("operator {}", oprname.as_str())),
            }
        }

        OperatorClassRelationId => {
            match syscache::opclass_namespace_method_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for opclass {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((opcnamespace, opcmethod, opcname)) => {
                    let amname = syscache::am_name::call(mcx, opcmethod)?.ok_or_else(|| {
                        elog_error(format!(
                            "cache lookup failed for access method {}",
                            opcmethod
                        ))
                    })?;
                    let nspname = if namespace::opclass_is_visible::call(mcx, object.objectId)? {
                        None
                    } else {
                        lsyscache::get_namespace_name::call(mcx, opcnamespace)?
                    };
                    let qualified = ruleutils::quote_qualified_identifier::call(
                        mcx,
                        nspname.as_deref(),
                        opcname.as_str(),
                    )?;
                    buffer.push_str(&format!(
                        "operator class {} for access method {}",
                        qualified.as_str(),
                        amname.as_str()
                    ));
                }
            }
        }

        OperatorFamilyRelationId => {
            get_op_family_description(mcx, &mut buffer, object.objectId, missing_ok)?;
        }

        AccessMethodRelationId => {
            match syscache::am_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for access method {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(amname) => buffer.push_str(&format!("access method {}", amname.as_str())),
            }
        }

        AccessMethodOperatorRelationId => {
            match syscache::amop_description_row::call(object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for amop entry {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(amop) => {
                    let mut opfam = String::new();
                    get_op_family_description(mcx, &mut opfam, amop.amopfamily, false)?;
                    // FORMAT_TYPE_ALLOW_INVALID so dangling type links don't fail.
                    let lefttype = format_type::format_type_extended::call(
                        mcx,
                        amop.amoplefttype,
                        -1,
                        FORMAT_TYPE_ALLOW_INVALID,
                    )?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                    let righttype = format_type::format_type_extended::call(
                        mcx,
                        amop.amoprighttype,
                        -1,
                        FORMAT_TYPE_ALLOW_INVALID,
                    )?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                    let opr = regproc::format_operator::call(mcx, amop.amopopr)?;
                    /*
                       translator: %d is the operator strategy (a number), the
                       first two %s's are data type names, the third %s is the
                       description of the operator family, and the last %s is
                       the textual form of the operator with arguments. */
                    buffer.push_str(&format!(
                        "operator {} ({}, {}) of {}: {}",
                        amop.amopstrategy, lefttype, righttype, opfam, opr.as_str()
                    ));
                }
            }
        }

        AccessMethodProcedureRelationId => {
            match syscache::amproc_description_row::call(object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for amproc entry {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(amproc) => {
                    let mut opfam = String::new();
                    get_op_family_description(mcx, &mut opfam, amproc.amprocfamily, false)?;
                    let lefttype = format_type::format_type_extended::call(
                        mcx,
                        amproc.amproclefttype,
                        -1,
                        FORMAT_TYPE_ALLOW_INVALID,
                    )?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                    let righttype = format_type::format_type_extended::call(
                        mcx,
                        amproc.amprocrighttype,
                        -1,
                        FORMAT_TYPE_ALLOW_INVALID,
                    )?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                    let proc = regproc::format_procedure::call(mcx, amproc.amproc)?;
                    /*
                       translator: %d is the function number, the first two
                       %s's are data type names, the third %s is the description
                       of the operator family, and the last %s is the textual
                       form of the function with arguments. */
                    buffer.push_str(&format!(
                        "function {} ({}, {}) of {}: {}",
                        amproc.amprocnum, lefttype, righttype, opfam, proc.as_str()
                    ));
                }
            }
        }

        RewriteRelationId => {
            match syscache::rewrite_class_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for rule {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((ev_class, rulename)) => {
                    let mut rel = String::new();
                    get_relation_description(mcx, &mut rel, ev_class, false)?;
                    // translator: second %s is, e.g., "table %s"
                    buffer.push_str(&format!("rule {} on {}", rulename.as_str(), rel));
                }
            }
        }

        TriggerRelationId => {
            match syscache::trigger_relid_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for trigger {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((tgrelid, tgname)) => {
                    let mut rel = String::new();
                    get_relation_description(mcx, &mut rel, tgrelid, false)?;
                    // translator: second %s is, e.g., "table %s"
                    buffer.push_str(&format!("trigger {} on {}", tgname.as_str(), rel));
                }
            }
        }

        NamespaceRelationId => {
            match lsyscache::get_namespace_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for namespace {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(nspname) => buffer.push_str(&format!("schema {}", nspname.as_str())),
            }
        }

        StatisticExtRelationId => {
            match syscache::statext_namespace_and_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for statistics object {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(stx) => {
                    let nspname =
                        if namespace::statistics_obj_is_visible::call(mcx, object.objectId)? {
                            None
                        } else {
                            lsyscache::get_namespace_name::call(mcx, stx.namespace)?
                        };
                    push_qualified(
                        mcx,
                        &mut buffer,
                        "statistics object ",
                        nspname.as_deref(),
                        stx.name.as_str(),
                    )?;
                }
            }
        }

        TSParserRelationId => {
            match syscache::ts_parser_namespace_and_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for text search parser {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(prs) => {
                    let nspname = if namespace::ts_parser_is_visible::call(mcx, object.objectId)? {
                        None
                    } else {
                        lsyscache::get_namespace_name::call(mcx, prs.namespace)?
                    };
                    push_qualified(
                        mcx,
                        &mut buffer,
                        "text search parser ",
                        nspname.as_deref(),
                        prs.name.as_str(),
                    )?;
                }
            }
        }

        TSDictionaryRelationId => {
            match syscache::ts_dict_namespace_and_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for text search dictionary {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(dict) => {
                    let nspname =
                        if namespace::ts_dictionary_is_visible::call(mcx, object.objectId)? {
                            None
                        } else {
                            lsyscache::get_namespace_name::call(mcx, dict.namespace)?
                        };
                    push_qualified(
                        mcx,
                        &mut buffer,
                        "text search dictionary ",
                        nspname.as_deref(),
                        dict.name.as_str(),
                    )?;
                }
            }
        }

        TSTemplateRelationId => {
            match syscache::ts_template_namespace_and_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for text search template {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(tmpl) => {
                    let nspname =
                        if namespace::ts_template_is_visible::call(mcx, object.objectId)? {
                            None
                        } else {
                            lsyscache::get_namespace_name::call(mcx, tmpl.namespace)?
                        };
                    push_qualified(
                        mcx,
                        &mut buffer,
                        "text search template ",
                        nspname.as_deref(),
                        tmpl.name.as_str(),
                    )?;
                }
            }
        }

        TSConfigRelationId => {
            match syscache::ts_config_namespace_and_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for text search configuration {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(cfg) => {
                    let nspname = if namespace::ts_config_is_visible::call(mcx, object.objectId)? {
                        None
                    } else {
                        lsyscache::get_namespace_name::call(mcx, cfg.namespace)?
                    };
                    push_qualified(
                        mcx,
                        &mut buffer,
                        "text search configuration ",
                        nspname.as_deref(),
                        cfg.name.as_str(),
                    )?;
                }
            }
        }

        AuthIdRelationId => {
            let username = miscinit::get_user_name_from_id::call(mcx, object.objectId, missing_ok)?;
            if let Some(username) = username {
                buffer.push_str(&format!("role {}", username.as_str()));
            }
        }

        AuthMemRelationId => {
            match syscache::auth_member_member_role::call(object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for role membership {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((member, roleid)) => {
                    let member_name = miscinit::get_user_name_from_id::call(mcx, member, false)?
                        .ok_or_else(|| {
                            elog_error(format!("role {} does not exist", member))
                        })?;
                    let role_name = miscinit::get_user_name_from_id::call(mcx, roleid, false)?
                        .ok_or_else(|| {
                            elog_error(format!("role {} does not exist", roleid))
                        })?;
                    buffer.push_str(&format!(
                        "membership of role {} in role {}",
                        member_name.as_str(),
                        role_name.as_str()
                    ));
                }
            }
        }

        DatabaseRelationId => {
            match dbcommands::get_database_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for database {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(datname) => buffer.push_str(&format!("database {}", datname.as_str())),
            }
        }

        TableSpaceRelationId => {
            match tablespace::get_tablespace_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for tablespace {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(tblspace) => {
                    buffer.push_str(&format!("tablespace {}", tblspace.as_str()))
                }
            }
        }

        ForeignDataWrapperRelationId => {
            if let Some(fdwname) =
                foreign::foreign_data_wrapper_name::call(mcx, object.objectId, missing_ok)?
            {
                buffer.push_str(&format!("foreign-data wrapper {}", fdwname.as_str()));
            }
        }

        ForeignServerRelationId => {
            if let Some(srvname) =
                foreign::foreign_server_name::call(mcx, object.objectId, missing_ok)?
            {
                buffer.push_str(&format!("server {}", srvname.as_str()));
            }
        }

        UserMappingRelationId => {
            match syscache::user_mapping_user_server::call(object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for user mapping {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((useid, umserver)) => {
                    let usename = if OidIsValid(useid) {
                        miscinit::get_user_name_from_id::call(mcx, useid, false)?
                            .map(|s| s.as_str().to_string())
                            .ok_or_else(|| {
                                elog_error(format!("role {} does not exist", useid))
                            })?
                    } else {
                        "public".to_string()
                    };
                    // GetForeignServer(umserver) — errors on a missing server.
                    let srvname = foreign::foreign_server_name::call(mcx, umserver, false)?
                        .ok_or_else(|| {
                            elog_error(format!(
                                "cache lookup failed for foreign server {}",
                                umserver
                            ))
                        })?;
                    buffer.push_str(&format!(
                        "user mapping for {} on server {}",
                        usename,
                        srvname.as_str()
                    ));
                }
            }
        }

        DefaultAclRelationId => {
            match syscache::default_acl_row::call(object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for default ACL {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(defacl) => {
                    let rolename =
                        miscinit::get_user_name_from_id::call(mcx, defacl.defaclrole, false)?
                            .map(|s| s.as_str().to_string())
                            .ok_or_else(|| {
                                elog_error(format!("role {} does not exist", defacl.defaclrole))
                            })?;
                    let nspname = if OidIsValid(defacl.defaclnamespace) {
                        lsyscache::get_namespace_name::call(mcx, defacl.defaclnamespace)?
                            .map(|s| s.as_str().to_string())
                    } else {
                        None
                    };
                    let msg = match defacl.defaclobjtype {
                        DEFACLOBJ_RELATION => match &nspname {
                            Some(nsp) => format!(
                                "default privileges on new relations belonging to role {} in schema {}",
                                rolename, nsp
                            ),
                            None => format!(
                                "default privileges on new relations belonging to role {}",
                                rolename
                            ),
                        },
                        DEFACLOBJ_SEQUENCE => match &nspname {
                            Some(nsp) => format!(
                                "default privileges on new sequences belonging to role {} in schema {}",
                                rolename, nsp
                            ),
                            None => format!(
                                "default privileges on new sequences belonging to role {}",
                                rolename
                            ),
                        },
                        DEFACLOBJ_FUNCTION => match &nspname {
                            Some(nsp) => format!(
                                "default privileges on new functions belonging to role {} in schema {}",
                                rolename, nsp
                            ),
                            None => format!(
                                "default privileges on new functions belonging to role {}",
                                rolename
                            ),
                        },
                        DEFACLOBJ_TYPE => match &nspname {
                            Some(nsp) => format!(
                                "default privileges on new types belonging to role {} in schema {}",
                                rolename, nsp
                            ),
                            None => format!(
                                "default privileges on new types belonging to role {}",
                                rolename
                            ),
                        },
                        DEFACLOBJ_NAMESPACE => {
                            // Assert(!nspname)
                            format!(
                                "default privileges on new schemas belonging to role {}",
                                rolename
                            )
                        }
                        DEFACLOBJ_LARGEOBJECT => {
                            // Assert(!nspname)
                            format!(
                                "default privileges on new large objects belonging to role {}",
                                rolename
                            )
                        }
                        _ => match &nspname {
                            // shouldn't get here
                            Some(nsp) => format!(
                                "default privileges belonging to role {} in schema {}",
                                rolename, nsp
                            ),
                            None => {
                                format!("default privileges belonging to role {}", rolename)
                            }
                        },
                    };
                    buffer.push_str(&msg);
                }
            }
        }

        ExtensionRelationId => {
            match extension::get_extension_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for extension {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(extname) => buffer.push_str(&format!("extension {}", extname.as_str())),
            }
        }

        EventTriggerRelationId => {
            match syscache::event_trigger_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for event trigger {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(evtname) => {
                    buffer.push_str(&format!("event trigger {}", evtname.as_str()))
                }
            }
        }

        ParameterAclRelationId => {
            match syscache::parameter_acl_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for parameter ACL {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some(parname) => buffer.push_str(&format!("parameter {}", parname.as_str())),
            }
        }

        PolicyRelationId => {
            match syscache::policy_relid_name::call(mcx, object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for policy {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((polrelid, polname)) => {
                    let mut rel = String::new();
                    get_relation_description(mcx, &mut rel, polrelid, false)?;
                    // translator: second %s is, e.g., "table %s"
                    buffer.push_str(&format!("policy {} on {}", polname.as_str(), rel));
                }
            }
        }

        PublicationRelationId => {
            if let Some(pubname) =
                get_publication_name(mcx, object.objectId, missing_ok)?
            {
                buffer.push_str(&format!("publication {}", pubname));
            }
        }

        PublicationNamespaceRelationId => {
            match get_publication_schema_info(mcx, object, missing_ok)? {
                None => return finish(mcx, buffer),
                Some((pubname, nspname)) => {
                    buffer.push_str(&format!(
                        "publication of schema {} in publication {}",
                        nspname, pubname
                    ));
                }
            }
        }

        PublicationRelRelationId => {
            match syscache::publication_rel_pub_rel::call(object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "cache lookup failed for publication table {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((prpubid, prrelid)) => {
                    let pubname = get_publication_name(mcx, prpubid, false)?.ok_or_else(|| {
                        elog_error(format!(
                            "cache lookup failed for publication {}",
                            prpubid
                        ))
                    })?;
                    let mut rel = String::new();
                    get_relation_description(mcx, &mut rel, prrelid, false)?;
                    // translator: first %s is, e.g., "table %s"
                    buffer.push_str(&format!(
                        "publication of {} in publication {}",
                        rel, pubname
                    ));
                }
            }
        }

        SubscriptionRelationId => {
            if let Some(subname) =
                get_subscription_name(mcx, object.objectId, missing_ok)?
            {
                buffer.push_str(&format!("subscription {}", subname));
            }
        }

        TransformRelationId => {
            match syscache::transform_type_lang::call(object.objectId)? {
                None => {
                    if !missing_ok {
                        return Err(elog_error(format!(
                            "could not find tuple for transform {}",
                            object.objectId
                        )));
                    }
                    return finish(mcx, buffer);
                }
                Some((trftype, trflang)) => {
                    let typname = format_type::format_type_be::call(mcx, trftype)?;
                    let langname = syscache::language_name::call(mcx, trflang)?.ok_or_else(|| {
                        elog_error(format!("cache lookup failed for language {}", trflang))
                    })?;
                    buffer.push_str(&format!(
                        "transform for {} language {}",
                        typname.as_str(),
                        langname.as_str()
                    ));
                }
            }
        }

        _ => {
            return Err(elog_error(format!(
                "unsupported object class: {}",
                object.classId
            )));
        }
    }

    // an empty buffer is equivalent to no object found
    finish(mcx, buffer)
}

/// `if (buffer.len == 0) return NULL; return buffer.data;` — render the buffer
/// into `mcx`, or `None` for the empty (vanished-object) case.
fn finish<'mcx>(mcx: Mcx<'mcx>, buffer: String) -> PgResult<Option<PgString<'mcx>>> {
    if buffer.is_empty() {
        Ok(None)
    } else {
        Ok(Some(PgString::from_str_in(&buffer, mcx)?))
    }
}

/// `getObjectDescriptionOids(Oid classid, Oid objid)` (objectaddress.c 4086):
/// the description for a bare (classid, objid) pair (objectSubId 0).
pub fn get_object_description_oids<'mcx>(
    mcx: Mcx<'mcx>,
    classid: Oid,
    objid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    let address = ObjectAddress {
        classId: classid,
        objectId: objid,
        objectSubId: 0,
    };
    get_object_description(mcx, &address, false)
}

/// `getRelationDescription(StringInfo buffer, Oid relid, bool missing_ok)`
/// (objectaddress.c 4103): append the relation/column-flavored description to
/// `buffer`.
pub fn get_relation_description<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: &mut String,
    relid: Oid,
    missing_ok: bool,
) -> PgResult<()> {
    // SearchSysCache1(RELOID) projected to (relnamespace, relname); relkind read
    // separately (the syscache projection that carries both name and relkind).
    let relname_info = syscache::relation_namespace_and_name::call(mcx, relid)?;
    let relname_info = match relname_info {
        Some(info) => info,
        None => {
            if !missing_ok {
                return Err(elog_error(format!(
                    "cache lookup failed for relation {}",
                    relid
                )));
            }
            return Ok(());
        }
    };
    let relkind = syscache::rel_relkind::call(relid)?.ok_or_else(|| {
        elog_error(format!("cache lookup failed for relation {}", relid))
    })?;

    // Qualify the name if not visible in search path
    let nspname = if namespace::relation_is_visible::call(mcx, relid)? {
        None
    } else {
        lsyscache::get_namespace_name::call(mcx, relname_info.namespace)?
    };

    let relname = ruleutils::quote_qualified_identifier::call(
        mcx,
        nspname.as_deref(),
        relname_info.name.as_str(),
    )?;
    let relname = relname.as_str();

    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => {
            buffer.push_str(&format!("table {}", relname))
        }
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => {
            buffer.push_str(&format!("index {}", relname))
        }
        RELKIND_SEQUENCE => buffer.push_str(&format!("sequence {}", relname)),
        RELKIND_TOASTVALUE => buffer.push_str(&format!("toast table {}", relname)),
        RELKIND_VIEW => buffer.push_str(&format!("view {}", relname)),
        RELKIND_MATVIEW => buffer.push_str(&format!("materialized view {}", relname)),
        RELKIND_COMPOSITE_TYPE => buffer.push_str(&format!("composite type {}", relname)),
        RELKIND_FOREIGN_TABLE => buffer.push_str(&format!("foreign table {}", relname)),
        // shouldn't get here
        _ => buffer.push_str(&format!("relation {}", relname)),
    }

    Ok(())
}

/// `getOpFamilyDescription(StringInfo buffer, Oid opfid, bool missing_ok)`
/// (objectaddress.c 4178): append the operator-family description to `buffer`.
pub fn get_op_family_description<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: &mut String,
    opfid: Oid,
    missing_ok: bool,
) -> PgResult<()> {
    let opf = match syscache::opfamily_namespace_method_name::call(mcx, opfid)? {
        Some(opf) => opf,
        None => {
            if !missing_ok {
                return Err(elog_error(format!(
                    "cache lookup failed for opfamily {}",
                    opfid
                )));
            }
            return Ok(());
        }
    };
    let (opfnamespace, opfmethod, opfname) = opf;

    let amname = syscache::am_name::call(mcx, opfmethod)?.ok_or_else(|| {
        elog_error(format!("cache lookup failed for access method {}", opfmethod))
    })?;

    // Qualify the name if not visible in search path
    let nspname = if namespace::opfamily_is_visible::call(mcx, opfid)? {
        None
    } else {
        lsyscache::get_namespace_name::call(mcx, opfnamespace)?
    };

    let qualified =
        ruleutils::quote_qualified_identifier::call(mcx, nspname.as_deref(), opfname.as_str())?;
    buffer.push_str(&format!(
        "operator family {} for access method {}",
        qualified.as_str(),
        amname.as_str()
    ));

    Ok(())
}

/// `getPublicationSchemaInfo(object, missing_ok, &pubname, &nspname)`
/// (objectaddress.c 2864): `(pubname, nspname)` for a publication-schema object,
/// or `None` (the C `false`). Shared across the description / identity families.
pub(crate) fn get_publication_schema_info<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<(String, String)>> {
    let row = syscache::publication_namespace_pub_nsp::call(object.objectId)?;
    let (pnpubid, pnnspid) = match row {
        Some(r) => r,
        None => {
            if !missing_ok {
                return Err(elog_error(format!(
                    "cache lookup failed for publication schema {}",
                    object.objectId
                )));
            }
            return Ok(None);
        }
    };

    let pubname = match get_publication_name(mcx, pnpubid, missing_ok)? {
        Some(p) => p,
        None => return Ok(None),
    };

    let nspname = match lsyscache::get_namespace_name::call(mcx, pnnspid)? {
        Some(n) => n.as_str().to_string(),
        None => {
            if !missing_ok {
                return Err(elog_error(format!(
                    "cache lookup failed for schema {}",
                    pnnspid
                )));
            }
            return Ok(None);
        }
    };

    Ok(Some((pubname, nspname)))
}

/// `get_publication_name(pubid, missing_ok)` (pg_publication.c): the publication
/// name, or `None` when absent and `missing_ok`.
fn get_publication_name<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    match syscache::get_publication_name_syscache::call(mcx, pubid)? {
        Some(name) => Ok(Some(name.as_str().to_string())),
        None => {
            if !missing_ok {
                return Err(elog_error(format!(
                    "cache lookup failed for publication {}",
                    pubid
                )));
            }
            Ok(None)
        }
    }
}

/// `get_subscription_name(subid, missing_ok)` (pg_subscription.c): the
/// subscription name, or `None` when absent and `missing_ok`.
fn get_subscription_name<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    match syscache::get_subscription_name_syscache::call(mcx, subid)? {
        Some(name) => Ok(Some(name.as_str().to_string())),
        None => {
            if !missing_ok {
                return Err(elog_error(format!(
                    "cache lookup failed for subscription {}",
                    subid
                )));
            }
            Ok(None)
        }
    }
}

/// The constraint's owning relation OID (`Form_pg_constraint.conrelid`) for the
/// constraint description arm. `InvalidOid` when the constraint is not
/// relation-scoped.
fn constraint_conrelid(conoid: Oid) -> PgResult<Oid> {
    // SearchSysCache1(CONSTROID) projected to Form_pg_constraint.conrelid.
    match syscache::constraint_relid::call(conoid)? {
        Some(relid) => Ok(relid),
        None => Ok(InvalidOid),
    }
}
