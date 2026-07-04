// objectaddress.c, COMMENT-matrix slice: get_object_address over the object
// classes with live lookup lanes; every other objtype is a named panic.
// INTERLOCK: drop-lane-2 (branch @ a1b8723e, unlanded) carries its own
// catalog_objectaddress — land as the union of arms.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::primitive::OidIsValid;
use types_core::{
    InvalidOid, Oid, AUTH_ID_RELATION_ID, CONSTRAINT_RELATION_ID, DATABASE_RELATION_ID,
    EXTENSION_RELATION_ID, NAMESPACE_RELATION_ID, OPERATOR_CLASS_RELATION_ID,
    OPERATOR_FAMILY_RELATION_ID, OPERATOR_RELATION_ID, PROCEDURE_RELATION_ID,
    RELATION_RELATION_ID, TABLE_SPACE_RELATION_ID, TYPE_RELATION_ID,
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

pub const RewriteRelationId: Oid = 2618;
pub const TriggerRelationId: Oid = 2620;
pub const CollationRelationId: Oid = 3456;
pub const CastRelationId: Oid = 2605;
pub const LargeObjectRelationId: Oid = 2613;
const PROKIND_PROCEDURE: i8 = b'p' as i8;
const PROKIND_AGGREGATE: i8 = b'a' as i8;

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

pub fn makeRangeVarFromNameList<'mcx>(names: &NodeList<'mcx>) -> RangeVar<'mcx> {
    let parts: Vec<&'mcx str> = names
        .iter()
        .map(|n| n.as_string().expect("qualified name component is a String node").sval)
        .collect();
    fill_range_var(&parts)
}

pub fn NameListToString(names: &[&str]) -> String {
    names.join(".")
}

fn TypeNameToString(tn: &TypeName<'_>) -> String {
    let parts: Vec<&str> = tn
        .names
        .iter()
        .map(|n| n.as_string().expect("TypeName names").sval)
        .collect();
    parts.join(".")
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
    let rv = fill_range_var(relparts);
    // C: no missing_ok support for the relation itself here.
    let rel = relation::relation_openrv(mcx, &rv, lockmode)?;
    let reloid = rel.rd_id;
    let attnum = lsyscache::get_attnum(reloid, attname)?;
    if attnum == 0 {
        if !missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_COLUMN,
                format!(
                    "column \"{attname}\" of relation \"{}\" does not exist",
                    NameListToString(relparts)
                ),
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
                Some(_) => rewrite_define::get_rewrite_oid(mcx, reloid, depname, missing_ok)?,
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

fn get_object_address_type<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    tn: &TypeName<'_>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    if missing_ok {
        unported("get_object_address_type missing_ok lane");
    }
    let typoid = parse_utilcmd::LookupTypeNameOid(mcx, tn)?;
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
    Ok(ObjectAddress::set(TYPE_RELATION_ID, typoid))
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
        other => unported(&format!("get_object_address_unqualified {other:?}")),
    }
}

fn get_object_address_opcf<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &NodeList<'mcx>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let amname = object
        .first()
        .and_then(|n| n.as_string())
        .expect("opclass object leads with the AM name")
        .sval;
    // C: no missing_ok support for the AM itself here.
    let amoid = amcmds::get_index_am_oid(amname, false)?;
    let mut rest = NodeList::nil();
    for n in object.iter().skip(1) {
        rest.lappend(mcx, n)?;
    }
    match objtype {
        ObjectType::OBJECT_OPCLASS => Ok(ObjectAddress::set(
            OPERATOR_CLASS_RELATION_ID,
            opclasscmds::get_opclass_oid(amoid, &rest, missing_ok)?,
        )),
        ObjectType::OBJECT_OPFAMILY => Ok(ObjectAddress::set(
            OPERATOR_FAMILY_RELATION_ID,
            opclasscmds::get_opfamily_oid(amoid, &rest, missing_ok)?,
        )),
        other => panic!("unrecognized object type: {other:?}"),
    }
}

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

// func_signature_string (parse_func.c): arg types re-resolve at error time —
// the failed lookup already proved they exist.
fn func_signature_string<'mcx>(mcx: Mcx<'mcx>, owa: &ObjectWithArgs<'_>) -> PgResult<String> {
    let parts: Vec<&str> = owa
        .objname
        .iter()
        .map(|n| n.as_string().expect("func name component").sval)
        .collect();
    let mut args = String::new();
    for (i, n) in owa.objargs.iter().enumerate() {
        if i > 0 {
            args.push_str(", ");
        }
        let tn = n.as_type_name().expect("objargs holds TypeName nodes");
        let oid = parse_utilcmd::LookupTypeNameOid(mcx, tn)?;
        args.push_str(&format_type::format_type_be(oid)?);
    }
    Ok(format!("{}({args})", NameListToString(&parts)))
}

fn get_object_address_func<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: Node<'mcx>,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let owa = object
        .as_variant::<ObjectWithArgs>()
        .expect("function object is an ObjectWithArgs");
    let oid =
        parse_func::LookupFuncWithArgs(&owa.objname, &owa.objargs, owa.args_unspecified, missing_ok)?;
    // C validates prokind inside LookupFuncWithArgs (parse_func.c); hosted
    // here because main's port carries no objtype parameter.
    if OidIsValid(oid) {
        let prokind = syscache_seams::lookup_pg_proc_shape::call(oid)?
            .map(|s| s.prokind)
            .unwrap_or_else(|| panic!("cache lookup failed for function {oid}"));
        match objtype {
            ObjectType::OBJECT_FUNCTION if prokind == PROKIND_PROCEDURE => {
                return Err(err(
                    ERRCODE_WRONG_OBJECT_TYPE,
                    format!("{} is not a function", func_signature_string(mcx, owa)?),
                ));
            }
            ObjectType::OBJECT_PROCEDURE if prokind != PROKIND_PROCEDURE => {
                return Err(err(
                    ERRCODE_WRONG_OBJECT_TYPE,
                    format!("{} is not a procedure", func_signature_string(mcx, owa)?),
                ));
            }
            ObjectType::OBJECT_AGGREGATE if prokind != PROKIND_AGGREGATE => {
                return Err(err(
                    ERRCODE_WRONG_OBJECT_TYPE,
                    format!(
                        "function {} is not an aggregate",
                        func_signature_string(mcx, owa)?
                    ),
                ));
            }
            _ => {}
        }
    }
    Ok(ObjectAddress::set(PROCEDURE_RELATION_ID, oid))
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
                let domaddr = get_object_address_type(mcx, OBJECT_DOMAIN, tn, missing_ok)?;
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
                (get_object_address_type(mcx, objtype, tn, missing_ok)?, None)
            }
            OBJECT_AGGREGATE | OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE => {
                (get_object_address_func(mcx, objtype, object, missing_ok)?, None)
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

// check_object_ownership (objectaddress.c): superuser fast path; the role
// ownership walks are the unported remainder.
pub fn check_object_ownership<'mcx>(
    _mcx: Mcx<'mcx>,
    roleid: Oid,
    _objtype: ObjectType,
    _address: ObjectAddress,
    _object: Node<'mcx>,
) -> PgResult<()> {
    if !superuser::superuser_arg(roleid)? {
        unported("check_object_ownership for non-superusers");
    }
    Ok(())
}

pub fn init_seams() {
    objectaddress_seams::get_object_address::set(get_object_address);
    objectaddress_seams::check_object_ownership::set(check_object_ownership);
}
