// objectaddress.c, DROP-matrix slice: get_object_address over the object
// classes with live DDL lanes (relations, columns, types/domains, schemas),
// getObjectDescription/getObjectIdentity for the classes pg_depend can reach
// from those; every other objtype/class is a named panic.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod description;
pub use description::{getObjectDescription, getObjectIdentity};

use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::primitive::OidIsValid;
use types_core::{
    InvalidOid, Oid, AUTH_ID_RELATION_ID, DATABASE_RELATION_ID, EXTENSION_RELATION_ID,
    NAMESPACE_RELATION_ID, OPERATOR_CLASS_RELATION_ID, OPERATOR_FAMILY_RELATION_ID,
    OPERATOR_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
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

pub fn init_seams() {
    objectaddress_seams::get_object_description::set(get_object_description_by_oids);
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

pub const ProcedureRelationId: Oid = types_core::PROCEDURE_RELATION_ID;
pub const ConstraintRelationId: Oid = 2606;
pub const AttrDefaultRelationId: Oid = 2604;
pub const RewriteRelationId: Oid = 2618;
pub const TriggerRelationId: Oid = 2620;

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

fn get_object_address_unqualified(
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
        ObjectType::OBJECT_EXTENSION => Ok(ObjectAddress::set(
            EXTENSION_RELATION_ID,
            extension::get_extension_oid(name, missing_ok)?,
        )),
        other => unported(&format!("get_object_address_unqualified {other:?}")),
    }
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
    if nnames < 2 {
        return Err(err(
            ERRCODE_SYNTAX_ERROR,
            "must specify relation and object name".into(),
        ));
    }
    let depname = object
        .last()
        .and_then(|n| n.as_string())
        .expect("dependent object name is a String node")
        .sval;
    let relparts: Vec<&'mcx str> = object
        .iter()
        .take(nnames - 1)
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect();
    let rv = fill_range_var(&relparts);
    let reloid = catalog_namespace::RangeVarGetRelid(&rv, types_rel::AccessShareLock, missing_ok)?;
    if !OidIsValid(reloid) {
        debug_assert!(missing_ok);
        let class_id = match objtype {
            ObjectType::OBJECT_RULE => RewriteRelationId,
            ObjectType::OBJECT_TRIGGER => TriggerRelationId,
            other => unported(&format!("get_object_address_relobject {other:?}")),
        };
        return Ok((ObjectAddress::set(class_id, InvalidOid), None));
    }
    let relation = relation::relation_open(mcx, reloid, types_rel::NoLock)?;
    let address = match objtype {
        ObjectType::OBJECT_RULE => ObjectAddress::set(
            RewriteRelationId,
            rewrite_define_seams::get_rewrite_oid::call(mcx, reloid, depname, missing_ok)?,
        ),
        ObjectType::OBJECT_TRIGGER => ObjectAddress::set(
            TriggerRelationId,
            trigger::get_trigger_oid(mcx, reloid, depname, missing_ok)?,
        ),
        other => unported(&format!("get_object_address_relobject {other:?}")),
    };
    if !OidIsValid(address.objectId) {
        relation.close(types_rel::AccessShareLock)?;
        return Ok((address, None));
    }
    Ok((address, Some(relation)))
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
                object.as_list().expect("relation drop object is a name list"),
                lockmode,
                missing_ok,
            )?,
            OBJECT_ATTRIBUTE | OBJECT_COLUMN => get_object_address_attribute(
                mcx,
                object.as_list().expect("column object is a name list"),
                lockmode,
                missing_ok,
            )?,
            OBJECT_RULE | OBJECT_TRIGGER => get_object_address_relobject(
                mcx,
                objtype,
                object.as_list().expect("relation-attached object is a name list"),
                missing_ok,
            )?,
            OBJECT_TYPE | OBJECT_DOMAIN => {
                let tn = object.as_type_name().expect("type object is a TypeName");
                (get_object_address_type(objtype, tn, missing_ok)?, None)
            }
            OBJECT_SCHEMA | OBJECT_EXTENSION => {
                (get_object_address_unqualified(objtype, object, missing_ok)?, None)
            }
            OBJECT_OPERATOR => {
                let owa = object
                    .as_variant::<ObjectWithArgs>()
                    .expect("operator object is an ObjectWithArgs");
                (
                    ObjectAddress::set(
                        OPERATOR_RELATION_ID,
                        parse_oper::LookupOperWithArgs(&owa.objname, &owa.objargs, missing_ok)?,
                    ),
                    None,
                )
            }
            OBJECT_OPCLASS | OBJECT_OPFAMILY => (
                get_object_address_opcf(
                    mcx,
                    objtype,
                    object.as_list().expect("opclass object is a name list"),
                    missing_ok,
                )?,
                None,
            ),
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
        | RewriteRelationId | TriggerRelationId => Ok(InvalidOid),
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
