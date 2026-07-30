// objectaddress.c: get_object_address over the object classes with live DDL
// lanes (DROP matrix + COMMENT matrix unions), getObjectDescription/
// getObjectIdentity for the classes pg_depend can reach; every other
// objtype/class is a named panic.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod builtins;
mod description;
mod identity;
mod properties;
pub use properties::*;
pub use description::{getObjectDescription, getObjectIdentity};
pub use identity::{getObjectIdentityParts, getObjectTypeDescription, ObjectIdentity};

use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::primitive::OidIsValid;
use types_core::{
    InvalidOid, Oid, AUTH_ID_RELATION_ID, CONSTRAINT_RELATION_ID, DATABASE_RELATION_ID,
    EXTENSION_RELATION_ID, NAMESPACE_RELATION_ID, OPERATOR_CLASS_RELATION_ID,
    OPERATOR_FAMILY_RELATION_ID, OPERATOR_RELATION_ID, RELATION_RELATION_ID,
    TABLE_SPACE_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::parsenodes::{ObjectType, ObjectWithArgs};
use types_nodes::rawnodes::TypeName;
use types_nodes::{Node, NodeList};
use types_rel::{
    Relation, LOCKMODE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_VIEW,
};

pub use pg_depend::ObjectAddress;

pub const ProcedureRelationId: Oid = types_core::PROCEDURE_RELATION_ID;
pub const ConstraintRelationId: Oid = 2606;
pub const AttrDefaultRelationId: Oid = 2604;
pub const RewriteRelationId: Oid = 2618;
pub const TriggerRelationId: Oid = 2620;
pub const PolicyRelationId: Oid = 3256;
pub const EventTriggerRelationId: Oid = 3466;
pub const CollationRelationId: Oid = 3456;
pub const CastRelationId: Oid = 2605;
pub const AccessMethodRelationId: Oid = 2601;
pub const LargeObjectRelationId: Oid = 2613;
pub const TSParserRelationId: Oid = 3601;
pub const TSDictionaryRelationId: Oid = 3600;
pub const TSTemplateRelationId: Oid = 3764;
pub const TSConfigRelationId: Oid = 3602;
pub const AccessMethodOperatorRelationId: Oid = 2602;
pub const AccessMethodOperatorOidIndexId: Oid = 2756;
pub const AccessMethodProcedureRelationId: Oid = 2603;
pub const AccessMethodProcedureOidIndexId: Oid = 2757;
pub const AuthMemRelationId: Oid = 1261;
pub const AuthMemOidIndexId: Oid = 6303;
pub const DefaultAclRelationId: Oid = 826;
pub const DefaultAclOidIndexId: Oid = 828;
pub const ParameterAclRelationId: Oid = 6243;
pub const TransformRelationId: Oid = 3576;

pub fn init_seams() {
    objectaddress_seams::get_object_description::set(get_object_description_by_oids);
    objectaddress_seams::get_object_address::set(get_object_address_marshal);
    objectaddress_seams::check_object_ownership::set(check_object_ownership_marshal);
    fmgr_core::register_late_builtins(builtins::OBJECTADDRESS_BUILTINS);
}

fn get_object_address_marshal<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: Node<'mcx>,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<(objectaddress_seams::ObjectAddr, Option<Relation<'mcx>>)> {
    let (a, rel) = get_object_address(mcx, objtype, object, lockmode, missing_ok)?;
    Ok((
        objectaddress_seams::ObjectAddr {
            classId: a.classId,
            objectId: a.objectId,
            objectSubId: a.objectSubId,
        },
        rel,
    ))
}

fn check_object_ownership_marshal<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    objtype: ObjectType,
    address: objectaddress_seams::ObjectAddr,
    object: Node<'mcx>,
    relation: Option<&Relation<'mcx>>,
) -> PgResult<()> {
    check_object_ownership(
        mcx,
        roleid,
        objtype,
        ObjectAddress::sub_set(address.classId, address.objectId, address.objectSubId),
        object,
        relation,
    )
}

fn get_object_description_by_oids(
    mcx: Mcx<'_>,
    class_id: Oid,
    object_id: Oid,
    object_sub_id: i32,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    let object = ObjectAddress::sub_set(class_id, object_id, object_sub_id);
    getObjectDescription(mcx, &object, missing_ok)
}

pub const PublicationRelationId: Oid = 6104;
pub const PublicationRelRelationId: Oid = 6106;
pub const PublicationNamespaceRelationId: Oid = 6237;
pub const SubscriptionRelationId: Oid = 6100;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: objectaddress.c {what}")
}

#[track_caller]
#[cold]
fn err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

fn fill_range_var<'mcx>(parts: &[&'mcx str]) -> PgResult<RangeVar<'mcx>> {
    let mut rv = RangeVar {
        catalogname: None,
        schemaname: None,
        relname: "",
        inh: true,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    match parts {
        [r] => rv.relname = r,
        [s, r] => {
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        [c, s, r] => {
            rv.catalogname = Some(c);
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        _ => {
            return Err(err(
                ERRCODE_SYNTAX_ERROR,
                format!("improper relation name (too many dotted names): {}", parts.join(".")),
            ))
        }
    }
    Ok(rv)
}

pub fn makeRangeVarFromParts<'mcx>(parts: &[&'mcx str]) -> PgResult<RangeVar<'mcx>> {
    fill_range_var(parts)
}

pub fn makeRangeVarFromNameList<'mcx>(names: &NodeList<'mcx>) -> PgResult<RangeVar<'mcx>> {
    let parts: Vec<&'mcx str> = names
        .iter()
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect();
    fill_range_var(&parts)
}


pub fn NameListToString(names: &NodeList<'_>) -> String {
    let mut out = String::new();
    for (i, node) in names.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(node.as_string().expect("name component is a String node").sval);
    }
    out
}

// TypeNameToString/appendTypeNameToBuffer (parse_type.c); C renders %TYPE
// and [] decorations but never SETOF.
pub fn TypeNameToString(tn: &TypeName<'_>) -> String {
    let mut out = String::new();
    if tn.names.is_nil() {
        // Internally-generated TypeName: render the resolved OID (C's
        // format_type_be arm). Unreachable from grammar TypeNames.
        out.push_str(&format_type::format_type_be(tn.typeOid).expect("format_type_be"));
    } else {
        for (i, node) in tn.names.iter().enumerate() {
            if i > 0 {
                out.push('.');
            }
            out.push_str(node.as_string().expect("TypeName names").sval);
        }
    }
    if tn.pct_type {
        out.push_str("%TYPE");
    }
    if !tn.arrayBounds.is_nil() {
        out.push_str("[]");
    }
    out
}

// LookupTypeNameOid (parse_type.c), plain unparameterized names only.
pub fn LookupTypeNameOid(tn: &TypeName<'_>, missing_ok: bool) -> PgResult<Oid> {
    if tn.pct_type || tn.setof {
        unported("LookupTypeName %TYPE / SETOF");
    }
    if tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }
    let mut names: [&str; 3] = [""; 3];
    let nnames = tn.names.len();
    if nnames > 3 {
        // C DeconstructQualifiedName (namespace.c): catchable 42601, not a crash.
        return Err(err(
            ERRCODE_SYNTAX_ERROR,
            format!("improper qualified name (too many dotted names): {}", NameListToString(&tn.names)),
        ));
    }
    if nnames == 0 {
        // names == NIL is the pre-resolved-typeOid lane (parse_type.c);
        // grammar TypeNames always carry names.
        unported("pre-resolved TypeName.typeOid lane");
    }
    for (i, n) in tn.names.iter().enumerate() {
        names[i] = n.as_string().expect("TypeName names").sval;
    }
    let (schemaname, typname) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;
    let typoid = match schemaname {
        Some(schemaname) => {
            let namespace_id = catalog_namespace::LookupExplicitNamespace(schemaname, missing_ok)?;
            if namespace_id == InvalidOid {
                InvalidOid
            } else {
                syscache_seams::lookup_pg_type_oid_by_name::call(typname, namespace_id)?
            }
        }
        None => catalog_namespace::TypenameGetTypidExtended(typname, true)?,
    };
    let typoid = if typoid != InvalidOid && !tn.arrayBounds.is_nil() {
        syscache_seams::pg_type_typarray::call(typoid)?.unwrap_or(InvalidOid)
    } else {
        typoid
    };
    if typoid == InvalidOid && !missing_ok {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("type \"{}\" does not exist", TypeNameToString(tn)),
        ));
    }
    Ok(typoid)
}


fn get_relation_by_qualified_name<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &NodeList<'mcx>,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<(ObjectAddress, Option<Relation<'mcx>>)> {
    let mut address = ObjectAddress::set(RELATION_RELATION_ID, InvalidOid);
    let rv = makeRangeVarFromNameList(object)?;
    let Some(rel) = relation::relation_openrv_extended(mcx, &rv, lockmode, missing_ok)? else {
        return Ok((address, None));
    };
    let relkind = rel.rd_rel.relkind;
    let relname = rel.name().to_string();
    let wrong = |what: &str| -> Box<PgError> {
        err(ERRCODE_WRONG_OBJECT_TYPE, format!("\"{relname}\" is not {what}"))
    };
    match objtype {
        ObjectType::OBJECT_INDEX => {
            if relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX {
                return Err(wrong("an index"));
            }
        }
        ObjectType::OBJECT_SEQUENCE => {
            if relkind != RELKIND_SEQUENCE {
                return Err(wrong("a sequence"));
            }
        }
        ObjectType::OBJECT_TABLE => {
            if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
                return Err(wrong("a table"));
            }
        }
        ObjectType::OBJECT_VIEW => {
            if relkind != RELKIND_VIEW {
                return Err(wrong("a view"));
            }
        }
        ObjectType::OBJECT_MATVIEW => {
            if relkind != RELKIND_MATVIEW {
                return Err(wrong("a materialized view"));
            }
        }
        ObjectType::OBJECT_FOREIGN_TABLE => {
            if relkind != RELKIND_FOREIGN_TABLE {
                return Err(wrong("a foreign table"));
            }
        }
        other => panic!("unrecognized object type: {other:?}"),
    }
    address.objectId = rel.rd_id;
    Ok((address, Some(rel)))
}

fn get_object_address_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    object: &NodeList<'mcx>,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<(ObjectAddress, Option<Relation<'mcx>>)> {
    let nnames = object.len();
    if nnames < 2 {
        return Err(err(ERRCODE_SYNTAX_ERROR, "column name must be qualified".into()));
    }
    let parts: Vec<&'mcx str> = object
        .iter()
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect();
    let attname = parts[nnames - 1];
    let relparts = &parts[..nnames - 1];
    let relname_str = relparts.join(".");
    let rv = fill_range_var(relparts)?;
    // C: no missing_ok support for the relation itself here.
    let rel = relation::relation_openrv(mcx, &rv, lockmode)?;
    let reloid = rel.rd_id;
    let attnum = lsyscache::get_attnum(reloid, attname)?;
    if attnum == 0 {
        if !missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_COLUMN,
                format!("column \"{attname}\" of relation \"{relname_str}\" does not exist"),
            ));
        }
        let address = ObjectAddress::sub_set(RELATION_RELATION_ID, InvalidOid, 0);
        rel.close(lockmode)?;
        return Ok((address, None));
    }
    Ok((
        ObjectAddress::sub_set(RELATION_RELATION_ID, reloid, attnum as i32),
        Some(rel),
    ))
}


fn get_object_address_type(
    objtype: ObjectType,
    tn: &TypeName<'_>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let mut address = ObjectAddress::set(TYPE_RELATION_ID, InvalidOid);
    let typoid = LookupTypeNameOid(tn, missing_ok)?;
    if typoid == InvalidOid {
        debug_assert!(missing_ok);
        return Ok(address);
    }
    address.objectId = typoid;
    if objtype == ObjectType::OBJECT_DOMAIN {
        match syscache_seams::pg_type_typtype::call(typoid)? {
            Some(t) if t == b'd' as i8 => {}
            _ => {
                return Err(err(
                    ERRCODE_WRONG_OBJECT_TYPE,
                    format!("\"{}\" is not a domain", TypeNameToString(tn)),
                ))
            }
        }
    }
    Ok(address)
}



fn get_object_address_unqualified<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: Node<'_>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let name = object.as_string().expect("unqualified object name is a String node").sval;
    match objtype {
        ObjectType::OBJECT_SCHEMA => Ok(ObjectAddress::set(
            NAMESPACE_RELATION_ID,
            catalog_namespace::get_namespace_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_DATABASE => Ok(ObjectAddress::set(
            DATABASE_RELATION_ID,
            dbcommands::get_database_oid(mcx, name, missing_ok)?,
        )),
        ObjectType::OBJECT_EXTENSION => Ok(ObjectAddress::set(
            EXTENSION_RELATION_ID,
            extension::get_extension_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_TABLESPACE => Ok(ObjectAddress::set(
            TABLE_SPACE_RELATION_ID,
            commands_tablespace::get_tablespace_oid(mcx, name, missing_ok)?,
        )),
        ObjectType::OBJECT_ROLE => Ok(ObjectAddress::set(
            AUTH_ID_RELATION_ID,
            adt_acl::get_role_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_EVENT_TRIGGER => Ok(ObjectAddress::set(
            EventTriggerRelationId,
            get_event_trigger_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_PUBLICATION => Ok(ObjectAddress::set(
            PublicationRelationId,
            lsyscache::get_publication_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_SUBSCRIPTION => Ok(ObjectAddress::set(
            SubscriptionRelationId,
            lsyscache::get_subscription_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_ACCESS_METHOD => Ok(ObjectAddress::set(
            AccessMethodRelationId,
            commands_amcmds::get_am_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_PARAMETER_ACL => Ok(ObjectAddress::set(
            catalog::ParameterAclRelationId,
            pg_parameter_acl::ParameterAclLookup(name, missing_ok)?,
        )),
        ObjectType::OBJECT_LANGUAGE => Ok(ObjectAddress::set(
            proclang::LanguageRelationId,
            proclang::get_language_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_FDW => Ok(ObjectAddress::set(
            types_core::FOREIGN_DATA_WRAPPER_RELATION_ID,
            get_foreign_data_wrapper_oid(name, missing_ok)?,
        )),
        ObjectType::OBJECT_FOREIGN_SERVER => Ok(ObjectAddress::set(
            types_core::FOREIGN_SERVER_RELATION_ID,
            get_foreign_server_oid(name, missing_ok)?,
        )),
        other => unported(&format!("get_object_address_unqualified {other:?}")),
    }
}


// get_event_trigger_oid (event_trigger.c); hosted here because event_trigger
// depends on this crate for identity parts.
fn get_event_trigger_oid(trigname: &str, missing_ok: bool) -> PgResult<Oid> {
    const Anum_pg_event_trigger_oid: i32 = 1;
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::EVENTTRIGGERNAME,
        Anum_pg_event_trigger_oid,
        cache_syscache::SysCacheKey::Str(trigname),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("event trigger \"{trigname}\" does not exist"),
        ));
    }
    Ok(oid)
}

// get_foreign_data_wrapper_oid / get_foreign_server_oid (foreign.c); hosted
// here because foreigncmds depends on this crate.
fn get_foreign_data_wrapper_oid(fdwname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::FOREIGNDATAWRAPPERNAME,
        1,
        cache_syscache::SysCacheKey::Str(fdwname),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("foreign-data wrapper \"{fdwname}\" does not exist"),
        ));
    }
    Ok(oid)
}

fn get_foreign_server_oid(servername: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::FOREIGNSERVERNAME,
        1,
        cache_syscache::SysCacheKey::Str(servername),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("server \"{servername}\" does not exist"),
        ));
    }
    Ok(oid)
}

fn get_object_address_publication_rel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &NodeList<'mcx>,
    missing_ok: bool,
) -> PgResult<(ObjectAddress, Option<Relation<'mcx>>)> {
    let mut address = ObjectAddress::set(PublicationRelRelationId, InvalidOid);
    let relname = object
        .nth(0)
        .as_list()
        .expect("publication relation object leads with a name list");
    let rv = makeRangeVarFromNameList(&relname)?;
    let Some(relation) =
        relation::relation_openrv_extended(mcx, &rv, types_rel::AccessShareLock, missing_ok)?
    else {
        return Ok((address, None));
    };

    let pubname = object
        .nth(1)
        .as_string()
        .expect("publication relation object carries the publication name")
        .sval;
    let puboid = lsyscache::get_publication_oid(pubname, missing_ok)?;
    if !OidIsValid(puboid) {
        relation.close(types_rel::AccessShareLock)?;
        return Ok((address, None));
    }

    address.objectId = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::PUBLICATIONRELMAP,
        1,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(relation.rd_id)),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(puboid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(address.objectId) {
        if !missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                format!(
                    "publication relation \"{}\" in publication \"{pubname}\" does not exist",
                    relation.name()
                ),
            ));
        }
        relation.close(types_rel::AccessShareLock)?;
        return Ok((address, None));
    }
    Ok((address, Some(relation)))
}

fn get_object_address_publication_schema(
    object: &NodeList<'_>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let mut address = ObjectAddress::set(PublicationNamespaceRelationId, InvalidOid);
    let schemaname = object
        .nth(0)
        .as_string()
        .expect("publication schema object leads with the schema name")
        .sval;
    let pubname = object
        .nth(1)
        .as_string()
        .expect("publication schema object carries the publication name")
        .sval;

    let schemaid = catalog_namespace::get_namespace_oid(schemaname, missing_ok)?;
    if !OidIsValid(schemaid) {
        return Ok(address);
    }
    let puboid = lsyscache::get_publication_oid(pubname, missing_ok)?;
    if !OidIsValid(puboid) {
        return Ok(address);
    }

    address.objectId = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::PUBLICATIONNAMESPACEMAP,
        1,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(schemaid)),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(puboid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(address.objectId) && !missing_ok {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!(
                "publication schema \"{schemaname}\" in publication \"{pubname}\" does not exist"
            ),
        ));
    }
    Ok(address)
}

// get_object_address_relobject (objectaddress.c), OBJECT_RULE arm; the
// TRIGGER/POLICY/TABCONSTRAINT forms wait on their grammar lanes.

fn get_object_address_relobject<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &NodeList<'mcx>,
    missing_ok: bool,
) -> PgResult<(ObjectAddress, Option<Relation<'mcx>>)> {
    let nnames = object.len();
    let depname = object
        .last()
        .and_then(|n| n.as_string())
        .expect("dependent object name is a String node")
        .sval;
    if nnames < 2 {
        return Err(err(
            ERRCODE_SYNTAX_ERROR,
            "must specify relation and object name".into(),
        ));
    }
    let parts: Vec<&'mcx str> = object
        .iter()
        .take(nnames - 1)
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect();
    let rv = fill_range_var(&parts)?;
    let rel = table::table_openrv_extended(mcx, &rv, types_rel::AccessShareLock, missing_ok)?;
    let reloid = rel.as_ref().map(|r| r.rd_id).unwrap_or(InvalidOid);
    let (classId, objectId) = match objtype {
        ObjectType::OBJECT_RULE => (
            RewriteRelationId,
            match &rel {
                Some(_) => rewrite_define_seams::get_rewrite_oid::call(mcx, reloid, depname, missing_ok)?,
                None => InvalidOid,
            },
        ),
        ObjectType::OBJECT_TRIGGER => (
            TriggerRelationId,
            match &rel {
                Some(_) => trigger::get_trigger_oid(mcx, reloid, depname, missing_ok)?,
                None => InvalidOid,
            },
        ),
        ObjectType::OBJECT_TABCONSTRAINT => (
            CONSTRAINT_RELATION_ID,
            match &rel {
                Some(_) => {
                    pg_constraint::get_relation_constraint_oid(mcx, reloid, depname, missing_ok)?
                }
                None => InvalidOid,
            },
        ),
        ObjectType::OBJECT_POLICY => (
            PolicyRelationId,
            match &rel {
                Some(_) => get_relation_policy_oid(mcx, reloid, depname, missing_ok)?,
                None => InvalidOid,
            },
        ),
        other => panic!("unrecognized object type: {other:?}"),
    };
    let address = ObjectAddress::set(classId, objectId);
    if !OidIsValid(address.objectId) {
        if let Some(rel) = rel {
            rel.close(types_rel::AccessShareLock)?;
        }
        return Ok((address, None));
    }
    Ok((address, rel))
}


// get_object_address_opcf (objectaddress.c).
fn get_object_address_opcf<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &NodeList<'mcx>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let amname = object
        .first()
        .and_then(|n| n.as_string())
        .expect("opclass access method name is a String node")
        .sval;
    let amoid = opclasscmds_seams::get_index_am_oid::call(amname)?;
    let name = NodeList::from_slice(mcx, &object.as_slice()[1..])?;
    match objtype {
        ObjectType::OBJECT_OPCLASS => Ok(ObjectAddress::set(
            OPERATOR_CLASS_RELATION_ID,
            opclasscmds_seams::get_opclass_oid::call(amoid, &name, missing_ok)?,
        )),
        ObjectType::OBJECT_OPFAMILY => Ok(ObjectAddress::set(
            OPERATOR_FAMILY_RELATION_ID,
            opclasscmds_seams::get_opfamily_oid::call(amoid, &name, missing_ok)?,
        )),
        other => unported(&format!("get_object_address_opcf {other:?}")),
    }
}

// get_relation_policy_oid (policy.c), inlined: commands_policy would cycle
// through catalog_dependency back into this crate.
fn get_relation_policy_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    policy_name: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    const POLICY_POLRELID_POLNAME_INDEX_ID: Oid = 3258;
    let rel = table::table_open(mcx, PolicyRelationId, types_rel::AccessShareLock)?;
    let mut namebuf = [0u8; 64];
    let n = policy_name.len().min(63);
    namebuf[..n].copy_from_slice(&policy_name.as_bytes()[..n]);
    let mut keys = [types_scan::scankey::ScanKeyData::empty(), types_scan::scankey::ScanKeyData::empty()];
    keys[0].sk_attno = 3;
    keys[0].sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    keys[0].sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)?;
    keys[0].sk_argument = datum::Datum::from_oid(relid);
    keys[1].sk_attno = 2;
    keys[1].sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    keys[1].sk_collation = types_core::C_COLLATION_OID;
    keys[1].sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_NAMEEQ)?;
    keys[1].sk_argument = datum::Datum::from_usize(namebuf.as_ptr() as usize);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        POLICY_POLRELID_POLNAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let oid = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => description::getattr(tup, 1, rel.descr()).as_oid(),
        None => {
            if !missing_ok {
                let relname = lsyscache::relation::get_rel_name(mcx, relid)?
                    .map(|n| n.as_str().to_string())
                    .unwrap_or_default();
                return Err(err(
                    ERRCODE_UNDEFINED_OBJECT,
                    format!("policy \"{policy_name}\" for table \"{relname}\" does not exist"),
                ));
            }
            InvalidOid
        }
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(oid)
}

// get_object_address_attrdef (objectaddress.c). C additionally gates on
// tupdesc->constr; GetAttrDefaultOid returning InvalidOid is result-equal.
fn get_object_address_attrdef<'mcx>(
    mcx: Mcx<'mcx>,
    object: &NodeList<'mcx>,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<(ObjectAddress, Option<Relation<'mcx>>)> {
    let nnames = object.len();
    if nnames < 2 {
        return Err(err(ERRCODE_SYNTAX_ERROR, "column name must be qualified".into()));
    }
    let parts: Vec<&'mcx str> = object
        .iter()
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect();
    let attname = parts[nnames - 1];
    let relparts = &parts[..nnames - 1];
    let relname_str = relparts.join(".");
    let rv = fill_range_var(relparts)?;
    let rel = relation::relation_openrv(mcx, &rv, lockmode)?;
    let reloid = rel.rd_id;
    let attnum = lsyscache::get_attnum(reloid, attname)?;
    let defoid = if attnum != 0 {
        pg_attrdef::GetAttrDefaultOid(mcx, reloid, attnum)?
    } else {
        InvalidOid
    };
    if !OidIsValid(defoid) {
        if !missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_COLUMN,
                format!(
                    "default value for column \"{attname}\" of relation \"{relname_str}\" does not exist"
                ),
            ));
        }
        rel.close(lockmode)?;
        return Ok((ObjectAddress::set(AttrDefaultRelationId, InvalidOid), None));
    }
    Ok((ObjectAddress::set(AttrDefaultRelationId, defoid), Some(rel)))
}

// get_transform_oid (functioncmds.c), inlined: functioncmds depends on this
// crate.
fn get_transform_oid(
    type_id: Oid,
    langname: &str,
    lang_id: Oid,
    missing_ok: bool,
) -> PgResult<Oid> {
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::TRFTYPELANG,
        1,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(type_id)),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(lang_id)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!(
                "transform for type {} language \"{langname}\" does not exist",
                format_type::format_type_be(type_id)?
            ),
        ));
    }
    Ok(oid)
}

// get_object_address_usermapping (objectaddress.c).
fn get_object_address_usermapping(
    object: &NodeList<'_>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let username = object
        .first()
        .and_then(|n| n.as_string())
        .expect("user mapping user name is a String node")
        .sval;
    let servername = object
        .last()
        .and_then(|n| n.as_string())
        .expect("user mapping server name is a String node")
        .sval;
    let mut address = ObjectAddress::set(types_core::USER_MAPPING_RELATION_ID, InvalidOid);
    let userid = if username == "public" {
        InvalidOid
    } else {
        let oid = cache_syscache::GetSysCacheOid(
            cache_syscache::cacheinfo::AUTHNAME,
            1,
            cache_syscache::SysCacheKey::Str(username),
            cache_syscache::SysCacheKey::UNUSED,
            cache_syscache::SysCacheKey::UNUSED,
            cache_syscache::SysCacheKey::UNUSED,
        )?;
        if !OidIsValid(oid) {
            if !missing_ok {
                return Err(err(
                    ERRCODE_UNDEFINED_OBJECT,
                    format!(
                        "user mapping for user \"{username}\" on server \"{servername}\" does not exist"
                    ),
                ));
            }
            return Ok(address);
        }
        oid
    };
    let serverid = foreigncmds_seams::get_foreign_server_oid::call(servername, true)?;
    if !OidIsValid(serverid) {
        if !missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                format!("server \"{servername}\" does not exist"),
            ));
        }
        return Ok(address);
    }
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::USERMAPPINGUSERSERVER,
        1,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(userid)),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(serverid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) {
        if !missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                format!(
                    "user mapping for user \"{username}\" on server \"{servername}\" does not exist"
                ),
            ));
        }
        return Ok(address);
    }
    address.objectId = oid;
    Ok(address)
}

// get_object_address_defacl (objectaddress.c).
fn get_object_address_defacl(
    object: &NodeList<'_>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let cells = object.as_slice();
    let sval = |i: usize| {
        cells[i]
            .as_string()
            .expect("default ACL component is a String node")
            .sval
    };
    let username = sval(1);
    let schema = if cells.len() >= 3 { Some(sval(2)) } else { None };
    let objtype = sval(0).as_bytes().first().copied().unwrap_or(0);
    let objtype_str = match objtype {
        b'r' => "tables",
        b'S' => "sequences",
        b'f' => "functions",
        b'T' => "types",
        b'n' => "schemas",
        b'L' => "large objects",
        other => {
            return Err(Box::new(
                PgError::error(format!(
                    "unrecognized default ACL object type \"{}\"",
                    other as char
                ))
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                .with_hint(
                    "Valid object types are \"r\", \"S\", \"f\", \"T\", \"n\", \"L\".",
                ),
            ))
        }
    };
    let address = ObjectAddress::set(DefaultAclRelationId, InvalidOid);
    let not_found = || -> PgResult<ObjectAddress> {
        if !missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                match schema {
                    Some(s) => format!(
                        "default ACL for user \"{username}\" in schema \"{s}\" on {objtype_str} does not exist"
                    ),
                    None => format!(
                        "default ACL for user \"{username}\" on {objtype_str} does not exist"
                    ),
                },
            ));
        }
        Ok(address)
    };
    let userid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::AUTHNAME,
        1,
        cache_syscache::SysCacheKey::Str(username),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(userid) {
        return not_found();
    }
    let schemaid = match schema {
        Some(s) => {
            let oid = catalog_namespace::get_namespace_oid(s, true)?;
            if !OidIsValid(oid) {
                return not_found();
            }
            oid
        }
        None => InvalidOid,
    };
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::cacheinfo::DEFACLROLENSPOBJ,
        1,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(userid)),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(schemaid)),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_char(objtype as i8)),
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) {
        return not_found();
    }
    Ok(ObjectAddress::set(DefaultAclRelationId, oid))
}

// atoi(3) shape: optional sign + leading digits, 0 on no parse.
fn c_atoi(s: &str) -> i32 {
    let b = s.trim_start().as_bytes();
    let (sign, rest) = match b.first() {
        Some(b'-') => (-1i64, &b[1..]),
        Some(b'+') => (1i64, &b[1..]),
        _ => (1i64, b),
    };
    let mut v = 0i64;
    for &c in rest {
        if !c.is_ascii_digit() {
            break;
        }
        v = (v * 10 + (c - b'0') as i64).min(i32::MAX as i64);
    }
    (sign * v).clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

// get_object_address_opf_member (objectaddress.c).
fn get_object_address_opf_member<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &NodeList<'mcx>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let name_list = object
        .first()
        .and_then(|n| n.as_list())
        .expect("opfamily member leads with a name list");
    let args_list = object
        .last()
        .and_then(|n| n.as_list())
        .expect("opfamily member args are a list");
    let names = name_list.as_slice();
    let membernum = c_atoi(
        names
            .last()
            .and_then(|n| n.as_string())
            .expect("member number is a String node")
            .sval,
    );
    let copy = NodeList::from_slice(mcx, &names[..names.len() - 1])?;
    let famaddr = get_object_address_opcf(mcx, ObjectType::OBJECT_OPFAMILY, &copy, false)?;
    let mut typenames: [Option<&TypeName<'mcx>>; 2] = [None, None];
    let mut typeoids: [Oid; 2] = [InvalidOid, InvalidOid];
    for (i, cell) in args_list.iter().take(2).enumerate() {
        let tn = cell.as_type_name().expect("opfamily member arg is a TypeName");
        typenames[i] = Some(tn);
        typeoids[i] = get_object_address_type(ObjectType::OBJECT_TYPE, tn, missing_ok)?.objectId;
    }
    let (cacheid, class_id, noun) = match objtype {
        ObjectType::OBJECT_AMOP => (
            cache_syscache::cacheinfo::AMOPSTRATEGY,
            AccessMethodOperatorRelationId,
            "operator",
        ),
        ObjectType::OBJECT_AMPROC => (
            cache_syscache::cacheinfo::AMPROCNUM,
            AccessMethodProcedureRelationId,
            "function",
        ),
        other => panic!("unrecognized object type: {other:?}"),
    };
    let oid = cache_syscache::GetSysCacheOid(
        cacheid,
        1,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(famaddr.objectId)),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(typeoids[0])),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(typeoids[1])),
        cache_syscache::SysCacheKey::Value(datum::Datum::from_i16(membernum as i16)),
    )?;
    if !OidIsValid(oid) && !missing_ok {
        let famdesc = getObjectDescription(mcx, &famaddr, false)?.expect("missing_ok=false");
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!(
                "{noun} {membernum} ({}, {}) of {famdesc} does not exist",
                typenames[0].map(TypeNameToString).unwrap_or_default(),
                typenames[1].map(TypeNameToString).unwrap_or_default(),
            ),
        ));
    }
    Ok(ObjectAddress::set(class_id, oid))
}

// get_object_address (objectaddress.c). Returns the resolved address plus the
// open relation for relation-attached objects; caller closes it.

// oidparse (nodes/value.c): Integer directly; Float carries oids beyond
// int32 range (or non-numeric text) through oidin_subr.
fn oidparse(node: Node<'_>) -> PgResult<Oid> {
    if let Some(i) = node.as_integer() {
        return Ok(i.ival as Oid);
    }
    if let Some(f) = node.as_float() {
        let (v, _) = numutils::uint32in_subr(f.fval, false, "oid", None)?;
        return Ok(v);
    }
    panic!("unsupported node type in oidparse");
}

// open relation for relation-attached objects; caller closes it.
pub fn get_object_address<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: Node<'mcx>,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<(ObjectAddress, Option<Relation<'mcx>>)> {
    use ObjectType::*;
    debug_assert!(lockmode != types_rel::NoLock);
    let mut old_address = ObjectAddress::set(InvalidOid, InvalidOid);
    loop {
        let inval_count = sinval::SharedInvalidMessageCounter();
        let (address, relation) = match objtype {
            OBJECT_INDEX | OBJECT_SEQUENCE | OBJECT_TABLE | OBJECT_VIEW | OBJECT_MATVIEW
            | OBJECT_FOREIGN_TABLE => get_relation_by_qualified_name(
                mcx,
                objtype,
                object.as_list().expect("relation object is a name list"),
                lockmode,
                missing_ok,
            )?,
            OBJECT_ATTRIBUTE | OBJECT_COLUMN => get_object_address_attribute(
                mcx,
                object.as_list().expect("column object is a name list"),
                lockmode,
                missing_ok,
            )?,
            OBJECT_RULE | OBJECT_TRIGGER | OBJECT_TABCONSTRAINT | OBJECT_POLICY => {
                get_object_address_relobject(
                    mcx,
                    objtype,
                    object.as_list().expect("relation-attached object is a name list"),
                    missing_ok,
                )?
            }
            OBJECT_DOMCONSTRAINT => {
                let objlist = object.as_list().expect("domain constraint object is a list");
                let tn = objlist
                    .first()
                    .and_then(|n| n.as_type_name())
                    .expect("domain constraint leads with a TypeName");
                let constrname = objlist
                    .last()
                    .and_then(|n| n.as_string())
                    .expect("constraint name is a String node")
                    .sval;
                let domaddr = get_object_address_type(OBJECT_DOMAIN, tn, missing_ok)?;
                let conoid = pg_constraint::get_domain_constraint_oid(
                    mcx,
                    domaddr.objectId,
                    constrname,
                    missing_ok,
                )?;
                (ObjectAddress::set(CONSTRAINT_RELATION_ID, conoid), None)
            }
            OBJECT_DATABASE | OBJECT_EXTENSION | OBJECT_TABLESPACE | OBJECT_ROLE
            | OBJECT_SCHEMA | OBJECT_LANGUAGE | OBJECT_FDW | OBJECT_FOREIGN_SERVER
            | OBJECT_EVENT_TRIGGER | OBJECT_PARAMETER_ACL | OBJECT_ACCESS_METHOD
            | OBJECT_PUBLICATION | OBJECT_SUBSCRIPTION => {
                (get_object_address_unqualified(mcx, objtype, object, missing_ok)?, None)
            }
            OBJECT_TYPE | OBJECT_DOMAIN => {
                let tn = object.as_type_name().expect("type object is a TypeName");
                (get_object_address_type(objtype, tn, missing_ok)?, None)
            }
            OBJECT_PUBLICATION_NAMESPACE => (
                get_object_address_publication_schema(
                    &object.as_list().expect("publication schema object is a list"),
                    missing_ok,
                )?,
                None,
            ),
            OBJECT_PUBLICATION_REL => get_object_address_publication_rel(
                mcx,
                &object.as_list().expect("publication relation object is a list"),
                missing_ok,
            )?,
            OBJECT_AGGREGATE | OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE => {
                let owa = object
                    .as_variant::<ObjectWithArgs>()
                    .expect("function object is an ObjectWithArgs");
                (
                    ObjectAddress::set(
                        ProcedureRelationId,
                        parse_func::LookupFuncWithArgs(objtype, owa, missing_ok)?,
                    ),
                    None,
                )
            }
            OBJECT_OPERATOR => {
                let owa = object
                    .as_variant::<ObjectWithArgs>()
                    .expect("operator object is an ObjectWithArgs");
                let oid = parse_oper::LookupOperWithArgs(&owa.objname, &owa.objargs, missing_ok)?;
                (ObjectAddress::set(OPERATOR_RELATION_ID, oid), None)
            }
            OBJECT_COLLATION => {
                let names = object.as_list().expect("collation object is a name list");
                let oid = catalog_namespace::get_collation_oid_list(names, missing_ok)?;
                (ObjectAddress::set(CollationRelationId, oid), None)
            }
            OBJECT_CONVERSION => {
                let names = object.as_list().expect("conversion object is a name list");
                let parts: Vec<&str> = names
                    .iter()
                    .map(|n| {
                        n.as_string().expect("qualified name component is a String node").sval
                    })
                    .collect();
                let oid = catalog_namespace::get_conversion_oid(&parts, missing_ok)?;
                (ObjectAddress::set(pg_conversion::ConversionRelationId, oid), None)
            }
            OBJECT_OPCLASS | OBJECT_OPFAMILY => {
                let names = object.as_list().expect("opclass object is a name list");
                (get_object_address_opcf(mcx, objtype, names, missing_ok)?, None)
            }
            OBJECT_STATISTIC_EXT => {
                let names = object.as_list().expect("statistics object is a name list");
                let parts: Vec<&str> = names
                    .iter()
                    .map(|n| {
                        n.as_string().expect("qualified name component is a String node").sval
                    })
                    .collect();
                (
                    ObjectAddress::set(
                        statscmds::StatisticExtRelationId,
                        statscmds::get_statistics_object_oid(&parts, missing_ok)?,
                    ),
                    None,
                )
            }
            OBJECT_TSPARSER | OBJECT_TSDICTIONARY | OBJECT_TSTEMPLATE
            | OBJECT_TSCONFIGURATION => {
                let names = object.as_list().expect("text search object is a name list");
                let parts: Vec<&str> = names
                    .iter()
                    .map(|n| {
                        n.as_string().expect("qualified name component is a String node").sval
                    })
                    .collect();
                let (class_id, oid) = match objtype {
                    OBJECT_TSPARSER => (
                        TSParserRelationId,
                        catalog_namespace::get_ts_parser_oid(&parts, missing_ok)?,
                    ),
                    OBJECT_TSDICTIONARY => (
                        TSDictionaryRelationId,
                        catalog_namespace::get_ts_dict_oid(&parts, missing_ok)?,
                    ),
                    OBJECT_TSTEMPLATE => (
                        TSTemplateRelationId,
                        catalog_namespace::get_ts_template_oid(&parts, missing_ok)?,
                    ),
                    _ => (
                        TSConfigRelationId,
                        catalog_namespace::get_ts_config_oid(&parts, missing_ok)?,
                    ),
                };
                (ObjectAddress::set(class_id, oid), None)
            }
            OBJECT_LARGEOBJECT => {
                let loid = oidparse(object)?;
                if !pg_largeobject::LargeObjectExists(mcx, loid)? && !missing_ok {
                    return Err(err(
                        ERRCODE_UNDEFINED_OBJECT,
                        format!("large object {loid} does not exist"),
                    ));
                }
                (ObjectAddress::set(LargeObjectRelationId, loid), None)
            }
            OBJECT_CAST => {
                let objlist = object.as_list().expect("cast object is a TypeName pair");
                let source = objlist
                    .first()
                    .and_then(|n| n.as_type_name())
                    .expect("cast source TypeName");
                let target = objlist
                    .last()
                    .and_then(|n| n.as_type_name())
                    .expect("cast target TypeName");
                let sourcetypeid =
                    parse_utilcmd::LookupTypeNameOidExtended(mcx, source, missing_ok)?;
                let targettypeid =
                    parse_utilcmd::LookupTypeNameOidExtended(mcx, target, missing_ok)?;
                let oid = lsyscache::get_cast_oid(sourcetypeid, targettypeid, missing_ok)?;
                (ObjectAddress::set(CastRelationId, oid), None)
            }
            OBJECT_DEFAULT => get_object_address_attrdef(
                mcx,
                object.as_list().expect("default value object is a name list"),
                lockmode,
                missing_ok,
            )?,
            OBJECT_TRANSFORM => {
                let objlist = object.as_list().expect("transform object is a list");
                let tn = objlist
                    .first()
                    .and_then(|n| n.as_type_name())
                    .expect("transform type TypeName");
                let langname = objlist
                    .last()
                    .and_then(|n| n.as_string())
                    .expect("transform language is a String node")
                    .sval;
                let type_id = LookupTypeNameOid(tn, missing_ok)?;
                let lang_id = proclang::get_language_oid(langname, missing_ok)?;
                (
                    ObjectAddress::set(
                        TransformRelationId,
                        get_transform_oid(type_id, langname, lang_id, missing_ok)?,
                    ),
                    None,
                )
            }
            OBJECT_USER_MAPPING => (
                get_object_address_usermapping(
                    &object.as_list().expect("user mapping object is a list"),
                    missing_ok,
                )?,
                None,
            ),
            OBJECT_DEFACL => (
                get_object_address_defacl(
                    &object.as_list().expect("default ACL object is a list"),
                    missing_ok,
                )?,
                None,
            ),
            OBJECT_AMOP | OBJECT_AMPROC => (
                get_object_address_opf_member(
                    mcx,
                    objtype,
                    &object.as_list().expect("opfamily member object is a list"),
                    missing_ok,
                )?,
                None,
            ),
            #[allow(unreachable_patterns)]
            other => unported(&format!("get_object_address {other:?}")),
        };

        if !OidIsValid(address.objectId) {
            debug_assert!(missing_ok);
            return Ok((address, None));
        }

        if OidIsValid(old_address.classId) {
            if old_address == address {
                return Ok((address, relation));
            }
            if old_address.classId != RELATION_RELATION_ID {
                if catalog::IsSharedRelation(old_address.classId) {
                    lmgr::UnlockSharedObject(
                        old_address.classId,
                        old_address.objectId,
                        0,
                        lockmode,
                    )?;
                } else {
                    lmgr::UnlockDatabaseObject(
                        old_address.classId,
                        old_address.objectId,
                        0,
                        lockmode,
                    )?;
                }
            }
        }

        if address.classId != RELATION_RELATION_ID {
            if catalog::IsSharedRelation(address.classId) {
                lmgr::LockSharedObject(address.classId, address.objectId, 0, lockmode)?;
            } else {
                lmgr::LockDatabaseObject(address.classId, address.objectId, 0, lockmode)?;
            }
        }

        if inval_count == sinval::SharedInvalidMessageCounter() || relation.is_some() {
            return Ok((address, relation));
        }
        old_address = address;
    }
}

// get_object_namespace (objectaddress.c): ObjectProperty namespace column.
pub fn get_object_namespace(address: &ObjectAddress) -> PgResult<Oid> {
    // Publication sub-objects carry no ObjectProperty row; they never own a
    // namespace.
    if address.classId == PublicationRelRelationId
        || address.classId == PublicationNamespaceRelationId
    {
        return Ok(InvalidOid);
    }
    let property = get_object_property_data(address.classId);
    if property.attnum_namespace == 0 {
        return Ok(InvalidOid);
    }
    debug_assert!(property.oid_catcache_id != -1);
    syscache_oid_field(property.oid_catcache_id, address.objectId, property.attnum_namespace)
}

const Anum_pg_constraint_contypid: i32 = 10;

fn syscache_oid_field(cacheid: i32, objid: Oid, attnum: i32) -> PgResult<Oid> {
    let Some(tup) = cache_syscache::SearchSysCache1(
        cacheid,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(objid)),
    )?
    else {
        return Ok(InvalidOid);
    };
    let d = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, attnum)?;
    let oid = d.as_oid();
    cache_syscache::ReleaseSysCache(tup);
    Ok(oid)
}

// has_createrole_privilege (aclchk.c).
fn has_createrole_privilege(roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    const Anum_pg_authid_rolcreaterole: i32 = 5;
    match cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::AUTHOID,
        cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(roleid)),
    )? {
        Some(tuple) => {
            let result = cache_syscache::SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::AUTHOID,
                &tuple,
                Anum_pg_authid_rolcreaterole,
            )?
            .as_bool();
            cache_syscache::ReleaseSysCache(tuple);
            Ok(result)
        }
        None => Ok(false),
    }
}

#[track_caller]
#[cold]
fn permission_denied(attr_detail: String) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "permission denied")
            .with_detail(attr_detail)
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

// check_object_ownership (objectaddress.c); objtypes without an arm below
// are superuser-only until ported.
pub fn check_object_ownership<'mcx>(
    _mcx: Mcx<'mcx>,
    roleid: Oid,
    objtype: ObjectType,
    address: ObjectAddress,
    object: Node<'mcx>,
    relation: Option<&Relation<'mcx>>,
) -> PgResult<()> {
    match objtype {
        ObjectType::OBJECT_INDEX
        | ObjectType::OBJECT_SEQUENCE
        | ObjectType::OBJECT_TABLE
        | ObjectType::OBJECT_VIEW
        | ObjectType::OBJECT_MATVIEW
        | ObjectType::OBJECT_FOREIGN_TABLE
        | ObjectType::OBJECT_COLUMN
        | ObjectType::OBJECT_RULE
        | ObjectType::OBJECT_TRIGGER
        | ObjectType::OBJECT_POLICY
        | ObjectType::OBJECT_TABCONSTRAINT => {
            let relation = relation.expect("relation-scoped object carries its relation");
            if !aclchk::object_ownercheck(RELATION_RELATION_ID, relation.rd_id, roleid)? {
                aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, objtype, relation.name())?;
            }
        }
        ObjectType::OBJECT_AGGREGATE
        | ObjectType::OBJECT_FUNCTION
        | ObjectType::OBJECT_PROCEDURE
        | ObjectType::OBJECT_ROUTINE
        | ObjectType::OBJECT_OPERATOR => {
            if !aclchk::object_ownercheck(address.classId, address.objectId, roleid)? {
                let owa = object
                    .as_variant::<ObjectWithArgs>()
                    .expect("object is an ObjectWithArgs");
                aclchk::aclcheck_error(
                    aclchk::ACLCHECK_NOT_OWNER,
                    objtype,
                    &NameListToString(&owa.objname),
                )?;
            }
        }
        ObjectType::OBJECT_TYPE | ObjectType::OBJECT_DOMAIN | ObjectType::OBJECT_ATTRIBUTE => {
            if !aclchk::object_ownercheck(address.classId, address.objectId, roleid)? {
                aclcheck_error_type(aclchk::ACLCHECK_NOT_OWNER, address.objectId)?;
            }
        }
        ObjectType::OBJECT_DOMCONSTRAINT => {
            let contypid = syscache_oid_field(
                cache_syscache::cacheinfo::CONSTROID,
                address.objectId,
                Anum_pg_constraint_contypid,
            )?;
            if !OidIsValid(contypid) {
                return Err(Box::new(PgError::error(format!(
                    "constraint with OID {} does not exist",
                    address.objectId
                ))));
            }
            // Domain constraints fall back to the type ownership check.
            if !aclchk::object_ownercheck(TYPE_RELATION_ID, contypid, roleid)? {
                aclcheck_error_type(aclchk::ACLCHECK_NOT_OWNER, contypid)?;
            }
        }
        ObjectType::OBJECT_COLLATION
        | ObjectType::OBJECT_CONVERSION
        | ObjectType::OBJECT_OPCLASS
        | ObjectType::OBJECT_OPFAMILY
        | ObjectType::OBJECT_STATISTIC_EXT
        | ObjectType::OBJECT_TSDICTIONARY
        | ObjectType::OBJECT_TSCONFIGURATION => {
            if !aclchk::object_ownercheck(address.classId, address.objectId, roleid)? {
                let names = object.as_list().expect("object is a name list");
                aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, objtype, &NameListToString(names))?;
            }
        }
        ObjectType::OBJECT_LARGEOBJECT => {
            if !guc_tables::vars::lo_compat_privileges.read()
                && !aclchk::object_ownercheck_lo(_mcx, address.objectId, roleid)?
            {
                return Err(err(
                    types_error::ERRCODE_INSUFFICIENT_PRIVILEGE,
                    format!("must be owner of large object {}", address.objectId),
                ));
            }
        }
        ObjectType::OBJECT_CAST => {
            // Only the source/target type ownerships are checkable.
            let objlist = object.as_list().expect("cast object is a TypeName pair");
            let sourcetype = objlist
                .first()
                .and_then(|n| n.as_type_name())
                .expect("cast source TypeName");
            let targettype = objlist
                .last()
                .and_then(|n| n.as_type_name())
                .expect("cast target TypeName");
            let sourcetypeid = parse_utilcmd::LookupTypeNameOid(_mcx, sourcetype)?;
            let targettypeid = parse_utilcmd::LookupTypeNameOid(_mcx, targettype)?;
            if !aclchk::object_ownercheck(TYPE_RELATION_ID, sourcetypeid, roleid)?
                && !aclchk::object_ownercheck(TYPE_RELATION_ID, targettypeid, roleid)?
            {
                return Err(err(
                    types_error::ERRCODE_INSUFFICIENT_PRIVILEGE,
                    format!(
                        "must be owner of type {} or type {}",
                        format_type::format_type_be(sourcetypeid)?,
                        format_type::format_type_be(targettypeid)?
                    ),
                ));
            }
        }
        ObjectType::OBJECT_TRANSFORM => {
            let objlist = object.as_list().expect("transform object leads with a TypeName");
            let tn = objlist
                .first()
                .and_then(|n| n.as_type_name())
                .expect("transform type TypeName");
            let typeid = parse_utilcmd::LookupTypeNameOid(_mcx, tn)?;
            if !aclchk::object_ownercheck(TYPE_RELATION_ID, typeid, roleid)? {
                aclcheck_error_type(aclchk::ACLCHECK_NOT_OWNER, typeid)?;
            }
        }
        ObjectType::OBJECT_ROLE => {
            // Roles are "owned" by CREATEROLE holders with ADMIN on the role;
            // superuser roles only by superusers.
            if superuser::superuser_arg(address.objectId)? {
                if !superuser::superuser_arg(roleid)? {
                    return Err(Box::new(
                        PgError::error("permission denied")
                            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                            .with_detail("The current user must have the SUPERUSER attribute."),
                    ));
                }
            } else {
                if !aclchk::has_createrole_privilege(roleid)? {
                    return Err(Box::new(
                        PgError::error("permission denied")
                            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                            .with_detail("The current user must have the CREATEROLE attribute."),
                    ));
                }
                if !adt_acl::is_admin_of_role(roleid, address.objectId)? {
                    let rolename = miscinit::GetUserNameFromId(_mcx, address.objectId, true)?
                        .map(|n| n.to_string())
                        .unwrap_or_default();
                    return Err(Box::new(
                        PgError::error("permission denied")
                            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                            .with_detail(format!(
                                "The current user must have the ADMIN option on role \"{rolename}\"."
                            )),
                    ));
                }
            }
        }
        ObjectType::OBJECT_TSPARSER
        | ObjectType::OBJECT_TSTEMPLATE
        | ObjectType::OBJECT_ACCESS_METHOD
        | ObjectType::OBJECT_PARAMETER_ACL => {
            // Treated as owned by superusers.
            if !superuser::superuser_arg(roleid)? {
                return Err(err(
                    types_error::ERRCODE_INSUFFICIENT_PRIVILEGE,
                    "must be superuser".to_string(),
                ));
            }
        }
        ObjectType::OBJECT_DATABASE
        | ObjectType::OBJECT_EVENT_TRIGGER
        | ObjectType::OBJECT_EXTENSION
        | ObjectType::OBJECT_FDW
        | ObjectType::OBJECT_FOREIGN_SERVER
        | ObjectType::OBJECT_LANGUAGE
        | ObjectType::OBJECT_PUBLICATION
        | ObjectType::OBJECT_SCHEMA
        | ObjectType::OBJECT_SUBSCRIPTION
        | ObjectType::OBJECT_TABLESPACE => {
            if !aclchk::object_ownercheck(address.classId, address.objectId, roleid)? {
                let name = object.as_string().expect("object is a String node").sval;
                aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, objtype, name)?;
            }
        }
        ObjectType::OBJECT_TYPE | ObjectType::OBJECT_DOMAIN | ObjectType::OBJECT_ATTRIBUTE => {
            if !aclchk::object_ownercheck(address.classId, address.objectId, roleid)? {
                aclcheck_error_type(aclchk::ACLCHECK_NOT_OWNER, address.objectId)?;
            }
        }
        other => panic!("check_object_ownership: unsupported object type: {other:?}"),
    }
    Ok(())
}

// aclcheck_error_type (aclchk.c): arrays report their element type.
fn aclcheck_error_type(aclerr: i32, type_oid: Oid) -> PgResult<()> {
    let element_type = lsyscache::get_element_type(type_oid)?;
    let type_oid = if OidIsValid(element_type) { element_type } else { type_oid };
    aclchk::aclcheck_error(
        aclerr,
        ObjectType::OBJECT_TYPE,
        &format_type::format_type_be(type_oid)?,
    )
}
