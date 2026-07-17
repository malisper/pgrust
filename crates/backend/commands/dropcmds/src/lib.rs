// dropcmds.c RemoveObjects over the objtypes get_object_address serves
// (TYPE/DOMAIN/SCHEMA/RULE/TRIGGER/EXTENSION/OPERATOR/OPCLASS/OPFAMILY).
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_core::InvalidOid;
use types_core::xact::XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
use types_error::{PgResult, NOTICE};
use types_nodes::parsenodes::{DropStmt, ObjectType, ObjectWithArgs};
use types_nodes::rawnodes::TypeName;
use types_nodes::{Node, NodeList};
use types_rel::AccessExclusiveLock;

const PROKIND_AGGREGATE: i8 = b'a' as i8;

fn notice(msg: String) -> PgResult<()> {
    elog_seams::ereport_msg::call(NOTICE, msg, None)
}

fn schema_does_not_exist_skipping(names: &NodeList<'_>) -> PgResult<Option<String>> {
    let rv = catalog_objectaddress::makeRangeVarFromNameList(names)?;
    if let Some(schemaname) = rv.schemaname {
        if catalog_namespace::LookupNamespaceNoError(schemaname)? == types_core::InvalidOid {
            return Ok(Some(format!(
                "schema \"{schemaname}\" does not exist, skipping"
            )));
        }
    }
    Ok(None)
}

fn schema_does_not_exist_skipping_parts(parts: &[&str]) -> PgResult<Option<String>> {
    if parts.len() >= 2 {
        let schemaname = parts[parts.len() - 2];
        if catalog_namespace::LookupNamespaceNoError(schemaname)? == types_core::InvalidOid {
            return Ok(Some(format!("schema \"{schemaname}\" does not exist, skipping")));
        }
    }
    Ok(None)
}

fn string_parts<'mcx>(names: &NodeList<'mcx>, upto: usize) -> Vec<&'mcx str> {
    names
        .iter()
        .take(upto)
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect()
}

fn owningrel_does_not_exist_skipping(names: &NodeList<'_>) -> PgResult<Option<String>> {
    let parent = string_parts(names, names.len() - 1);
    if let Some(msg) = schema_does_not_exist_skipping_parts(&parent)? {
        return Ok(Some(msg));
    }
    let rv = catalog_objectaddress::makeRangeVarFromParts(&parent)?;
    if !OidIsValid(catalog_namespace::RangeVarGetRelid(&rv, types_rel::NoLock, true)?) {
        return Ok(Some(format!(
            "relation \"{}\" does not exist, skipping",
            parent.join(".")
        )));
    }
    Ok(None)
}

fn type_does_not_exist_skipping(tn: &TypeName<'_>) -> PgResult<Option<String>> {
    if !OidIsValid(catalog_objectaddress::LookupTypeNameOid(tn, true)?) {
        if let Some(msg) = schema_does_not_exist_skipping(&tn.names)? {
            return Ok(Some(msg));
        }
        return Ok(Some(format!(
            "type \"{}\" does not exist, skipping",
            catalog_objectaddress::TypeNameToString(tn)
        )));
    }
    Ok(None)
}

// C skips NULL cells (oper_argtypes NONE sides).
fn type_in_list_does_not_exist_skipping(
    typenames: &types_nodes::OptNodeList<'_>,
) -> PgResult<Option<String>> {
    for n in typenames.iter() {
        let Some(n) = n else { continue };
        let tn = n.as_variant::<TypeName>().expect("objargs holds TypeName nodes");
        if let Some(msg) = type_does_not_exist_skipping(tn)? {
            return Ok(Some(msg));
        }
    }
    Ok(None)
}

fn does_not_exist_skipping(objtype: ObjectType, object: Node<'_>) -> PgResult<()> {
    let msg = match objtype {
        ObjectType::OBJECT_TYPE | ObjectType::OBJECT_DOMAIN => {
            let tn: &TypeName<'_> = object.as_type_name().expect("type object is a TypeName");
            match schema_does_not_exist_skipping(&tn.names)? {
                Some(msg) => msg,
                None => format!(
                    "type \"{}\" does not exist, skipping",
                    catalog_objectaddress::TypeNameToString(tn)
                ),
            }
        }
        ObjectType::OBJECT_SCHEMA => {
            let name = object.as_string().expect("schema name is a String node").sval;
            format!("schema \"{name}\" does not exist, skipping")
        }
        ObjectType::OBJECT_CONVERSION => {
            let names = object.as_list().expect("conversion object is a name list");
            match schema_does_not_exist_skipping(names)? {
                Some(msg) => msg,
                None => {
                    let parts = string_parts(names, names.len());
                    format!("conversion \"{}\" does not exist, skipping", parts.join("."))
                }
            }
        }
        ObjectType::OBJECT_LANGUAGE => {
            let name = object.as_string().expect("language name is a String node").sval;
            format!("language \"{name}\" does not exist, skipping")
        }
        ObjectType::OBJECT_EXTENSION => {
            let name = object.as_string().expect("extension name is a String node").sval;
            format!("extension \"{name}\" does not exist, skipping")
        }
        ObjectType::OBJECT_EVENT_TRIGGER => {
            let name = object.as_string().expect("event trigger name is a String node").sval;
            format!("event trigger \"{name}\" does not exist, skipping")
        }
        ObjectType::OBJECT_ACCESS_METHOD => {
            let name = object.as_string().expect("access method name is a String node").sval;
            format!("access method \"{name}\" does not exist, skipping")
        }
        ObjectType::OBJECT_PUBLICATION => {
            let name = object.as_string().expect("publication name is a String node").sval;
            format!("publication \"{name}\" does not exist, skipping")
        }
        ObjectType::OBJECT_SUBSCRIPTION => {
            let name = object.as_string().expect("subscription name is a String node").sval;
            format!("subscription \"{name}\" does not exist, skipping")
        }
        ObjectType::OBJECT_COLLATION
        | ObjectType::OBJECT_STATISTIC_EXT
        | ObjectType::OBJECT_TSPARSER
        | ObjectType::OBJECT_TSDICTIONARY
        | ObjectType::OBJECT_TSTEMPLATE
        | ObjectType::OBJECT_TSCONFIGURATION => {
            let names = object.as_list().expect("object is a name list");
            match schema_does_not_exist_skipping(names)? {
                Some(msg) => msg,
                None => {
                    let noun = match objtype {
                        ObjectType::OBJECT_COLLATION => "collation",
                        ObjectType::OBJECT_STATISTIC_EXT => "statistics object",
                        ObjectType::OBJECT_TSPARSER => "text search parser",
                        ObjectType::OBJECT_TSDICTIONARY => "text search dictionary",
                        ObjectType::OBJECT_TSTEMPLATE => "text search template",
                        _ => "text search configuration",
                    };
                    let parts = string_parts(names, names.len());
                    format!("{noun} \"{}\" does not exist, skipping", parts.join("."))
                }
            }
        }
        ObjectType::OBJECT_CAST => {
            let objlist = object.as_list().expect("cast object is a TypeName pair");
            let source = objlist
                .first()
                .and_then(|n| n.as_variant::<TypeName>())
                .expect("cast source TypeName");
            let target = objlist
                .last()
                .and_then(|n| n.as_variant::<TypeName>())
                .expect("cast target TypeName");
            let missing_source = type_does_not_exist_skipping(source)?;
            match missing_source {
                Some(msg) => msg,
                None => match type_does_not_exist_skipping(target)? {
                    Some(msg) => msg,
                    None => format!(
                        "cast from type {} to type {} does not exist, skipping",
                        catalog_objectaddress::TypeNameToString(source),
                        catalog_objectaddress::TypeNameToString(target)
                    ),
                },
            }
        }
        ObjectType::OBJECT_TRANSFORM => {
            let objlist = object.as_list().expect("transform object is a pair");
            let tn = objlist
                .first()
                .and_then(|n| n.as_variant::<TypeName>())
                .expect("transform type TypeName");
            let lang = objlist
                .last()
                .and_then(|n| n.as_string())
                .expect("transform language is a String node")
                .sval;
            match type_does_not_exist_skipping(tn)? {
                Some(msg) => msg,
                None => format!(
                    "transform for type {} language \"{lang}\" does not exist, skipping",
                    catalog_objectaddress::TypeNameToString(tn)
                ),
            }
        }
        ObjectType::OBJECT_POLICY => {
            let names = object.as_list().expect("relation-attached object is a name list");
            match owningrel_does_not_exist_skipping(names)? {
                Some(msg) => msg,
                None => {
                    let depname = names
                        .last()
                        .and_then(|n| n.as_string())
                        .expect("dependent object name is a String node")
                        .sval;
                    let parent = string_parts(names, names.len() - 1);
                    format!(
                        "policy \"{depname}\" for relation \"{}\" does not exist, skipping",
                        parent.join(".")
                    )
                }
            }
        }
        ObjectType::OBJECT_RULE | ObjectType::OBJECT_TRIGGER => {
            let names = object.as_list().expect("relation-attached object is a name list");
            match owningrel_does_not_exist_skipping(names)? {
                Some(msg) => msg,
                None => {
                    let depname = names
                        .last()
                        .and_then(|n| n.as_string())
                        .expect("dependent object name is a String node")
                        .sval;
                    let noun = if objtype == ObjectType::OBJECT_RULE { "rule" } else { "trigger" };
                    let parent = string_parts(names, names.len() - 1);
                    format!(
                        "{noun} \"{depname}\" for relation \"{}\" does not exist, skipping",
                        parent.join(".")
                    )
                }
            }
        }
        ObjectType::OBJECT_FUNCTION
        | ObjectType::OBJECT_PROCEDURE
        | ObjectType::OBJECT_ROUTINE
        | ObjectType::OBJECT_AGGREGATE => {
            let owa = object
                .as_variant::<ObjectWithArgs>()
                .expect("function object is an ObjectWithArgs");
            match schema_does_not_exist_skipping(&owa.objname)? {
                Some(msg) => msg,
                None => match type_in_list_does_not_exist_skipping(&owa.objargs)? {
                    Some(msg) => msg,
                    None => {
                        let noun = match objtype {
                            ObjectType::OBJECT_PROCEDURE => "procedure",
                            ObjectType::OBJECT_ROUTINE => "routine",
                            ObjectType::OBJECT_AGGREGATE => "aggregate",
                            _ => "function",
                        };
                        let mut args = String::new();
                        for (i, n) in owa.objargs.iter().enumerate() {
                            if i > 0 {
                                args.push(',');
                            }
                            args.push_str(&catalog_objectaddress::TypeNameToString(
                                n.expect("function objargs cell is non-NULL")
                                    .as_variant::<TypeName>()
                                    .expect("objargs holds TypeName nodes"),
                            ));
                        }
                        format!(
                            "{noun} {}({args}) does not exist, skipping",
                            catalog_objectaddress::NameListToString(&owa.objname)
                        )
                    }
                },
            }
        }
        ObjectType::OBJECT_OPERATOR => {
            let owa = object
                .as_variant::<ObjectWithArgs>()
                .expect("operator object is an ObjectWithArgs");
            match schema_does_not_exist_skipping(&owa.objname)? {
                Some(msg) => msg,
                None => match type_in_list_does_not_exist_skipping(&owa.objargs)? {
                    Some(msg) => msg,
                    None => format!(
                        "operator {} does not exist, skipping",
                        catalog_objectaddress::NameListToString(&owa.objname)
                    ),
                },
            }
        }
        ObjectType::OBJECT_OPCLASS | ObjectType::OBJECT_OPFAMILY => {
            let names = object.as_list().expect("opclass object is a name list");
            let amname = names
                .first()
                .and_then(|n| n.as_string())
                .expect("opclass access method name is a String node")
                .sval;
            let tail: Vec<&str> = string_parts(names, names.len())[1..].to_vec();
            match schema_does_not_exist_skipping_parts(&tail)? {
                Some(msg) => msg,
                None => {
                    let noun = if objtype == ObjectType::OBJECT_OPCLASS {
                        "operator class"
                    } else {
                        "operator family"
                    };
                    format!(
                        "{noun} \"{}\" does not exist for access method \"{amname}\", skipping",
                        tail.join(".")
                    )
                }
            }
        }
        // unported: does_not_exist_skipping remaining object-type arms
        _ => {
            return Err(Box::new(
                types_error::PgError::error(
                    "DROP ... IF EXISTS is not supported yet for this type of object",
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ))
        }
    };
    notice(msg)
}

pub fn RemoveObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    if matches!(stmt.removeType, ObjectType::OBJECT_FDW | ObjectType::OBJECT_FOREIGN_SERVER) {
        return remove_foreign_objects(mcx, stmt);
    }
    let mut objects = catalog_dependency::ObjectAddresses::new();

    for object in stmt.objects.iter() {
        let (address, relation) = catalog_objectaddress::get_object_address(
            mcx,
            stmt.removeType,
            object,
            AccessExclusiveLock,
            stmt.missing_ok,
        )?;

        if !OidIsValid(address.objectId) {
            debug_assert!(stmt.missing_ok);
            does_not_exist_skipping(stmt.removeType, object)?;
            continue;
        }

        if stmt.removeType == ObjectType::OBJECT_FUNCTION {
            // Historically DROP FUNCTION refuses aggregates.
            if lsyscache::get_func_prokind(address.objectId)? == PROKIND_AGGREGATE {
                let owa = object
                    .as_variant::<ObjectWithArgs>()
                    .expect("function object is an ObjectWithArgs");
                return Err(Box::new(
                    types_error::PgError::error(format!(
                        "\"{}\" is an aggregate function",
                        catalog_objectaddress::NameListToString(&owa.objname)
                    ))
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .with_hint("Use DROP AGGREGATE to drop aggregate functions."),
                ));
            }
        }

        let namespaceId = catalog_objectaddress::get_object_namespace(&address)?;
        if !OidIsValid(namespaceId)
            || !aclchk::object_ownercheck(
                types_core::NAMESPACE_RELATION_ID,
                namespaceId,
                miscinit::GetUserId(),
            )?
        {
            catalog_objectaddress::check_object_ownership(
                mcx,
                miscinit::GetUserId(),
                stmt.removeType,
                address,
                object,
                relation.as_ref(),
            )?;
        }

        if OidIsValid(namespaceId) && catalog_namespace::isTempNamespace(namespaceId) {
            xact::OrMyXactFlags(XACT_FLAGS_ACCESSEDTEMPNAMESPACE);
        }

        if let Some(rel) = relation {
            rel.close(types_rel::NoLock)?;
        }

        objects.add_exact_object_address(address);
    }

    catalog_dependency::performMultipleDeletions(mcx, &objects, stmt.behavior, 0)
}

// OBJECT_FDW / OBJECT_FOREIGN_SERVER leg: get_object_address name lookup +
// check_object_ownership + performMultipleDeletions.
fn remove_foreign_objects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    let is_fdw = stmt.removeType == ObjectType::OBJECT_FDW;
    let mut objects = catalog_dependency::ObjectAddresses::new();
    for cell in stmt.objects.iter() {
        let name = cell.as_string().expect("DROP FDW/SERVER object is a String").sval;
        let (class_id, oid, owner) = if is_fdw {
            let oid = foreigncmds::foreign::get_foreign_data_wrapper_oid(name, stmt.missing_ok)?;
            if oid == InvalidOid {
                elog_seams::ereport_msg::call(
                    NOTICE,
                    format!("foreign-data wrapper \"{name}\" does not exist, skipping"),
                    None,
                )?;
                continue;
            }
            let fdw = foreigncmds::foreign::GetForeignDataWrapper(mcx, oid)?;
            (types_core::FOREIGN_DATA_WRAPPER_RELATION_ID, oid, fdw.owner)
        } else {
            let oid = foreigncmds::foreign::get_foreign_server_oid(name, stmt.missing_ok)?;
            if oid == InvalidOid {
                elog_seams::ereport_msg::call(
                    NOTICE,
                    format!("server \"{name}\" does not exist, skipping"),
                    None,
                )?;
                continue;
            }
            let srv = foreigncmds::foreign::GetForeignServer(mcx, oid)?;
            (types_core::FOREIGN_SERVER_RELATION_ID, oid, srv.owner)
        };
        let roleid = miscinit::GetUserId();
        let owned = superuser::superuser_arg(roleid)? || adt_acl::has_privs_of_role(roleid, owner)?;
        if !owned {
            let objtype = if is_fdw {
                ObjectType::OBJECT_FDW
            } else {
                ObjectType::OBJECT_FOREIGN_SERVER
            };
            aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, objtype, name)?;
        }
        objects.add_exact_object_address(pg_depend::ObjectAddress::set(class_id, oid));
    }
    catalog_dependency::performMultipleDeletions(mcx, &objects, stmt.behavior, 0)?;
    Ok(())
}
