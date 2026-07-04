// objectaddress.c: get_object_address over the object classes with live DDL
// lanes (DROP matrix + COMMENT matrix unions), getObjectDescription/
// getObjectIdentity for the classes pg_depend can reach; every other
// objtype/class is a named panic.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod description;
mod identity;
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

pub fn init_seams() {
    objectaddress_seams::get_object_description::set(get_object_description_by_oids);
    objectaddress_seams::get_object_address::set(get_object_address_marshal);
    objectaddress_seams::check_object_ownership::set(check_object_ownership_marshal);
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

#[cold]
fn err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

fn fill_range_var<'mcx>(parts: &[&'mcx str]) -> RangeVar<'mcx> {
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
        _ => panic!("improper relation name (too many dotted names)"),
    }
    rv
}

pub fn makeRangeVarFromParts<'mcx>(parts: &[&'mcx str]) -> RangeVar<'mcx> {
    fill_range_var(parts)
}

pub fn makeRangeVarFromNameList<'mcx>(names: &NodeList<'mcx>) -> RangeVar<'mcx> {
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

// TypeNameToString (parse_type.c), plain-names slice.
pub fn TypeNameToString(tn: &TypeName<'_>) -> String {
    if tn.pct_type || tn.setof || !tn.arrayBounds.is_nil() {
        unported("TypeNameToString %TYPE/SETOF/array rendering");
    }
    let mut out = String::new();
    for (i, node) in tn.names.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(node.as_string().expect("TypeName names").sval);
    }
    out
}

// LookupTypeNameOid (parse_type.c), plain unparameterized names only.
pub fn LookupTypeNameOid(tn: &TypeName<'_>, missing_ok: bool) -> PgResult<Oid> {
    if tn.pct_type || tn.setof {
        unported("LookupTypeName %TYPE / SETOF");
    }
    if !tn.arrayBounds.is_nil() {
        unported("LookupTypeName array bounds");
    }
    if tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }
    let mut names: [&str; 3] = [""; 3];
    let nnames = tn.names.len();
    if nnames == 0 || nnames > 3 {
        unported("improper TypeName names length");
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
    let rv = makeRangeVarFromNameList(object);
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
    let rv = fill_range_var(relparts);
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
    let rv = makeRangeVarFromNameList(&relname);
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
    let rv = fill_range_var(&parts);
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
        ObjectType::OBJECT_POLICY => {
            unported("get_object_address_relobject OBJECT_POLICY (rls lane)")
        }
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

// get_object_address (objectaddress.c). Returns the resolved address plus the
// open relation for relation-attached objects; caller closes it.

// oidparse (nodes/value.c): Integer directly; Float carries oids beyond
// int32 range as their decimal image.
fn oidparse(node: Node<'_>) -> Oid {
    if let Some(i) = node.as_integer() {
        return i.ival as Oid;
    }
    if let Some(f) = node.as_float() {
        return f.fval.parse::<Oid>().unwrap_or_else(|_| {
            panic!("invalid OID literal {:?}", f.fval)
        });
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
            OBJECT_OPCLASS | OBJECT_OPFAMILY => {
                let names = object.as_list().expect("opclass object is a name list");
                (get_object_address_opcf(mcx, objtype, names, missing_ok)?, None)
            }
            OBJECT_LARGEOBJECT => {
                let loid = oidparse(object);
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
                if missing_ok {
                    unported("OBJECT_CAST missing_ok lane");
                }
                let sourcetypeid = parse_utilcmd::LookupTypeNameOid(mcx, source)?;
                let targettypeid = parse_utilcmd::LookupTypeNameOid(mcx, target)?;
                let oid = lsyscache::get_cast_oid(sourcetypeid, targettypeid, missing_ok)?;
                (ObjectAddress::set(CastRelationId, oid), None)
            }
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

// get_object_namespace (objectaddress.c): ObjectProperty namespace column for
// the classes with live address lanes.
pub fn get_object_namespace(address: &ObjectAddress) -> PgResult<Oid> {
    match address.classId {
        RELATION_RELATION_ID => lsyscache::get_rel_namespace(address.objectId),
        TYPE_RELATION_ID => Ok(syscache_seams::pg_type_name_namespace::call(address.objectId)?
            .map(|(_, nsp)| nsp)
            .unwrap_or(InvalidOid)),
        NAMESPACE_RELATION_ID | DATABASE_RELATION_ID | AUTH_ID_RELATION_ID
        | RewriteRelationId | TriggerRelationId | EventTriggerRelationId
        | PublicationRelationId | PublicationRelRelationId
        | PublicationNamespaceRelationId | SubscriptionRelationId
        | AccessMethodRelationId => Ok(InvalidOid),
        ProcedureRelationId => Ok(syscache_seams::lookup_pg_proc_shape::call(address.objectId)?
            .map(|s| s.pronamespace)
            .unwrap_or(InvalidOid)),
        EXTENSION_RELATION_ID => extension::get_extension_schema(address.objectId),
        OPERATOR_RELATION_ID => syscache_oid_field(
            cache_syscache::cacheinfo::OPEROID,
            address.objectId,
            Anum_pg_operator_oprnamespace,
        ),
        OPERATOR_CLASS_RELATION_ID => syscache_oid_field(
            cache_syscache::cacheinfo::CLAOID,
            address.objectId,
            Anum_pg_opclass_opcnamespace,
        ),
        OPERATOR_FAMILY_RELATION_ID => syscache_oid_field(
            cache_syscache::cacheinfo::OPFAMILYOID,
            address.objectId,
            Anum_pg_opfamily_opfnamespace,
        ),
        other => unported(&format!("get_object_namespace class {other}")),
    }
}

const Anum_pg_operator_oprnamespace: i32 = 3;
const Anum_pg_opclass_opcnamespace: i32 = 4;
const Anum_pg_opfamily_opfnamespace: i32 = 4;

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
        _ => {
            if !superuser::superuser_arg(roleid)? {
                unported("check_object_ownership for non-superusers");
            }
        }
    }
    Ok(())
}
