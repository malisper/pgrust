//! F0 keystone — the resolution engine: `get_object_address[_rv]`, the 13
//! `get_object_address_*` helpers, `check_object_ownership` / `object_ownercheck`,
//! `get_object_namespace`, the string↔objtype / relkind maps, and
//! `get_catalog_object_by_oid[_extended]` (objectaddress.c 923-2864, 6186).
//!
//! The resolution engine is ported faithfully from `objectaddress.c`. Every
//! catalog lookup the C switch performs routes to its owner via a seam; where
//! the owner is unported (the parser `LookupFunc/Oper/TypeName` callees, the
//! rewrite/trigger/policy rule-name lookups, `pg_attrdef`, `pg_extension`,
//! `pg_event_trigger`, `pg_parameter_acl`, `oidparse`) the seam is declared but
//! uninstalled, so that object-type arm is an honest mirror-and-panic until the
//! owner lands (the C behaviour: a `castNode`-validated parse node that cannot
//! be resolved is an `ereport`/`elog`, and the seam carries that failure
//! surface). The node demux uses the real [`Node`] accessors (`as_list` /
//! `as_string` / `as_typename` / `as_objectwithargs`); no invented node model.

use mcx::Mcx;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::parsenodes::ObjectType;
use types_nodes::parsenodes::{
    OBJECT_ACCESS_METHOD, OBJECT_AGGREGATE, OBJECT_AMOP, OBJECT_AMPROC, OBJECT_ATTRIBUTE,
    OBJECT_CAST, OBJECT_COLLATION, OBJECT_COLUMN, OBJECT_CONVERSION, OBJECT_DATABASE, OBJECT_DEFACL,
    OBJECT_DEFAULT, OBJECT_DOMAIN, OBJECT_DOMCONSTRAINT, OBJECT_EVENT_TRIGGER, OBJECT_EXTENSION,
    OBJECT_FDW, OBJECT_FOREIGN_SERVER, OBJECT_FOREIGN_TABLE, OBJECT_FUNCTION, OBJECT_INDEX,
    OBJECT_LANGUAGE, OBJECT_LARGEOBJECT, OBJECT_MATVIEW, OBJECT_OPCLASS, OBJECT_OPERATOR,
    OBJECT_OPFAMILY, OBJECT_PARAMETER_ACL, OBJECT_POLICY, OBJECT_PROCEDURE, OBJECT_PUBLICATION,
    OBJECT_PUBLICATION_NAMESPACE, OBJECT_PUBLICATION_REL, OBJECT_ROLE, OBJECT_ROUTINE, OBJECT_RULE,
    OBJECT_SCHEMA, OBJECT_SEQUENCE, OBJECT_STATISTIC_EXT, OBJECT_SUBSCRIPTION, OBJECT_TABCONSTRAINT,
    OBJECT_TABLE, OBJECT_TABLESPACE, OBJECT_TRANSFORM, OBJECT_TRIGGER, OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY, OBJECT_TSPARSER, OBJECT_TSTEMPLATE, OBJECT_TYPE, OBJECT_USER_MAPPING,
    OBJECT_VIEW,
};
use types_parsenodes::{Node, StringNode};
use types_rel::Relation;
use types_storage::lock::LOCKMODE;
use types_tuple::access::RangeVar;

use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_VIEW,
};
use types_tuple::backend_access_common_heaptuple::{Datum as TupleDatum, FormedTuple};

use backend_catalog_objectaddress_seams::ResolvedObjectAddress;

use crate::consts::*;

const INVALID_OID: Oid = 0;

/// `OidIsValid`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != INVALID_OID
}

/// Extract an `Oid` from a tuple column value (catalog OID columns are
/// pass-by-value).
fn tuplevalue_oid(val: &TupleDatum<'_>) -> Oid {
    match val {
        TupleDatum::ByVal(_) => val.as_oid(),
        TupleDatum::ByRef(_)
        | TupleDatum::Cstring(_)
        | TupleDatum::Composite(_)
        | TupleDatum::Expanded(_)
        | TupleDatum::Internal(_) => 0,
    }
}

/// `strVal(node)` — read the string value of a `String` node.
fn str_val<'a>(node: &'a Node) -> &'a str {
    match node.as_string() {
        Some(s) => s.sval.as_deref().unwrap_or(""),
        None => panic!("strVal: node is not a String"),
    }
}

/// `castNode(List, object)`.
fn cast_list<'a>(node: &'a Node) -> PgResult<&'a [Node]> {
    match node.as_list() {
        Some(l) => Ok(l),
        None => Err(PgError::error("castNode(List): node is not a List")),
    }
}

/// Render a slice of `String` nodes (a `List *` qualified name) as `&[&str]`.
fn name_list_strs<'a>(names: &'a [Node]) -> Vec<&'a str> {
    names.iter().map(str_val).collect()
}

/// `TypeNameToString(typeName)` — render a raw-parser `TypeName` node for an
/// error message. The C reads it in `CurrentMemoryContext`; the result is
/// copied into the owned `String` immediately, so a transient context is the
/// faithful stand-in for the seam's mcx-bearing signature.
fn type_name_to_string(tn: &types_parsenodes::TypeName) -> PgResult<String> {
    let cx = mcx::MemoryContext::new("TypeNameToString");
    let s = backend_parser_parse_type_seams::typename_to_string_node::call(cx.mcx(), tn)?;
    Ok(s.as_str().to_string())
}

/// `NameListToString(names)` — render a dotted name for error messages.
fn name_list_to_string(names: &[Node]) -> String {
    let mut out = String::new();
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(str_val(n));
    }
    out
}

/* ---------------------------------------------------------------------------
 * get_object_address + get_object_address_rv (the public resolution entry)
 * ------------------------------------------------------------------------- */

/// `get_object_address(ObjectType objtype, Node *object, Relation *relp,
/// LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 923).
pub fn get_object_address<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &Node,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    debug_assert!(lockmode != 0, "Some kind of lock must be taken");

    let mut old_address = ObjectAddress {
        classId: INVALID_OID,
        objectId: INVALID_OID,
        objectSubId: 0,
    };
    let mut have_old = false;

    loop {
        let mut address = ObjectAddress {
            classId: INVALID_OID,
            objectId: INVALID_OID,
            objectSubId: 0,
        };
        let mut relation: Option<Relation<'mcx>> = None;

        let inval_count = backend_storage_ipc_sinval_seams::shared_invalid_message_counter::call();

        match objtype {
            OBJECT_INDEX | OBJECT_SEQUENCE | OBJECT_TABLE | OBJECT_VIEW | OBJECT_MATVIEW
            | OBJECT_FOREIGN_TABLE => {
                let r = get_relation_by_qualified_name(mcx, objtype, object, lockmode, missing_ok)?;
                address = r.address;
                relation = r.relation;
            }
            OBJECT_ATTRIBUTE | OBJECT_COLUMN => {
                let r = get_object_address_attribute(mcx, objtype, object, lockmode, missing_ok)?;
                address = r.address;
                relation = r.relation;
            }
            OBJECT_DEFAULT => {
                let r = get_object_address_attrdef(mcx, objtype, object, lockmode, missing_ok)?;
                address = r.address;
                relation = r.relation;
            }
            OBJECT_RULE | OBJECT_TRIGGER | OBJECT_TABCONSTRAINT | OBJECT_POLICY => {
                let r = get_object_address_relobject(mcx, objtype, object, missing_ok)?;
                address = r.address;
                relation = r.relation;
            }
            OBJECT_DOMCONSTRAINT => {
                let objlist = cast_list(object)?;
                let domaddr = get_object_address_type(OBJECT_DOMAIN, &objlist[0], missing_ok)?;
                let constrname = str_val(&objlist[1]);
                address.classId = ConstraintRelationId;
                address.objectId =
                    backend_catalog_pg_constraint_seams::get_domain_constraint_oid::call(
                        mcx,
                        domaddr.objectId,
                        constrname,
                        missing_ok,
                    )?;
                address.objectSubId = 0;
            }
            OBJECT_DATABASE | OBJECT_EXTENSION | OBJECT_TABLESPACE | OBJECT_ROLE | OBJECT_SCHEMA
            | OBJECT_LANGUAGE | OBJECT_FDW | OBJECT_FOREIGN_SERVER | OBJECT_EVENT_TRIGGER
            | OBJECT_PARAMETER_ACL | OBJECT_ACCESS_METHOD | OBJECT_PUBLICATION
            | OBJECT_SUBSCRIPTION => {
                address = get_object_address_unqualified(objtype, object, missing_ok)?;
            }
            OBJECT_TYPE | OBJECT_DOMAIN => {
                address = get_object_address_type(objtype, object, missing_ok)?;
            }
            OBJECT_AGGREGATE | OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE => {
                let owa = object
                    .as_objectwithargs()
                    .ok_or_else(|| PgError::error("castNode(ObjectWithArgs)"))?;
                address.classId = ProcedureRelationId;
                address.objectId =
                    backend_parser_parse_func_seams::lookup_func_with_args_for_objtype::call(
                        objtype, owa, missing_ok,
                    )?;
                address.objectSubId = 0;
            }
            OBJECT_OPERATOR => {
                let owa = object
                    .as_objectwithargs()
                    .ok_or_else(|| PgError::error("castNode(ObjectWithArgs)"))?;
                address.classId = OperatorRelationId;
                address.objectId =
                    backend_parser_parse_oper_seams::lookup_oper_with_args_node::call(
                        owa, missing_ok,
                    )?;
                address.objectSubId = 0;
            }
            OBJECT_COLLATION => {
                let names = cast_list(object)?;
                address.classId = CollationRelationId;
                let strs = name_list_strs(names);
                address.objectId =
                    backend_catalog_namespace_seams::get_collation_oid::call(mcx, &strs, missing_ok)?;
                address.objectSubId = 0;
            }
            OBJECT_CONVERSION => {
                let names = cast_list(object)?;
                address.classId = ConversionRelationId;
                let strs = name_list_strs(names);
                address.objectId = backend_catalog_namespace_seams::get_conversion_oid::call(
                    mcx, &strs, missing_ok,
                )?;
                address.objectSubId = 0;
            }
            OBJECT_OPCLASS | OBJECT_OPFAMILY => {
                address = get_object_address_opcf(mcx, objtype, object, missing_ok)?;
            }
            OBJECT_AMOP | OBJECT_AMPROC => {
                address = get_object_address_opf_member(mcx, objtype, object, missing_ok)?;
            }
            OBJECT_LARGEOBJECT => {
                address.classId = LargeObjectRelationId;
                address.objectId = backend_utils_adt_oid_seams::oidparse::call(object)?;
                address.objectSubId = 0;
                if !backend_catalog_pg_largeobject_seams::large_object_exists::call(
                    address.objectId,
                )? && !missing_ok
                {
                    return Err(PgError::new(
                        ERROR,
                        format!("large object {} does not exist", address.objectId),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
                }
            }
            OBJECT_CAST => {
                let list = cast_list(object)?;
                let sourcetype = list[0]
                    .as_typename()
                    .ok_or_else(|| PgError::error("linitial_node(TypeName)"))?;
                let targettype = list[1]
                    .as_typename()
                    .ok_or_else(|| PgError::error("lsecond_node(TypeName)"))?;
                let sourcetypeid = backend_parser_parse_type_seams::lookup_type_name_oid::call(
                    sourcetype, missing_ok,
                )?;
                let targettypeid = backend_parser_parse_type_seams::lookup_type_name_oid::call(
                    targettype, missing_ok,
                )?;
                address.classId = CastRelationId;
                address.objectId = backend_utils_cache_lsyscache_seams::get_cast_oid::call(
                    sourcetypeid,
                    targettypeid,
                    missing_ok,
                )?;
                address.objectSubId = 0;
            }
            OBJECT_TRANSFORM => {
                let list = cast_list(object)?;
                let typename = list[0]
                    .as_typename()
                    .ok_or_else(|| PgError::error("linitial_node(TypeName)"))?;
                let langname = str_val(&list[1]);
                let type_id = backend_parser_parse_type_seams::lookup_type_name_oid::call(
                    typename, missing_ok,
                )?;
                let lang_id = backend_commands_proclang_seams::get_language_oid::call(
                    langname,
                    missing_ok,
                )?;
                address.classId = TransformRelationId;
                address.objectId = backend_commands_functioncmds_seams::get_transform_oid::call(
                    mcx, type_id, lang_id, missing_ok,
                )?;
                address.objectSubId = 0;
            }
            OBJECT_TSPARSER => {
                let names = cast_list(object)?;
                address.classId = TSParserRelationId;
                let strs = name_list_strs(names);
                address.objectId =
                    backend_catalog_namespace_seams::get_ts_parser_oid::call(mcx, &strs, missing_ok)?;
                address.objectSubId = 0;
            }
            OBJECT_TSDICTIONARY => {
                let names = cast_list(object)?;
                address.classId = TSDictionaryRelationId;
                let strs = name_list_strs(names);
                address.objectId =
                    backend_catalog_namespace_seams::get_ts_dict_oid::call(mcx, &strs, missing_ok)?;
                address.objectSubId = 0;
            }
            OBJECT_TSTEMPLATE => {
                let names = cast_list(object)?;
                address.classId = TSTemplateRelationId;
                let strs = name_list_strs(names);
                address.objectId = backend_catalog_namespace_seams::get_ts_template_oid::call(
                    mcx, &strs, missing_ok,
                )?;
                address.objectSubId = 0;
            }
            OBJECT_TSCONFIGURATION => {
                let names = cast_list(object)?;
                address.classId = TSConfigRelationId;
                let strs = name_list_strs(names);
                address.objectId =
                    backend_catalog_namespace_seams::get_ts_config_oid::call(&strs, missing_ok)?;
                address.objectSubId = 0;
            }
            OBJECT_USER_MAPPING => {
                address = get_object_address_usermapping(mcx, object, missing_ok)?;
            }
            OBJECT_PUBLICATION_NAMESPACE => {
                address = get_object_address_publication_schema(mcx, object, missing_ok)?;
            }
            OBJECT_PUBLICATION_REL => {
                let r = get_object_address_publication_rel(mcx, object, missing_ok)?;
                address = r.address;
                relation = r.relation;
            }
            OBJECT_DEFACL => {
                address = get_object_address_defacl(mcx, object, missing_ok)?;
            }
            OBJECT_STATISTIC_EXT => {
                let names = cast_list(object)?;
                address.classId = StatisticExtRelationId;
                let strs = name_list_strs(names);
                address.objectId =
                    backend_catalog_namespace_seams::get_statistics_object_oid::call(
                        mcx, &strs, missing_ok,
                    )?;
                address.objectSubId = 0;
            }
        }

        if !oid_is_valid(address.classId) {
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as u32
            )));
        }

        // If we could not find the supplied object, return without locking.
        if !oid_is_valid(address.objectId) {
            debug_assert!(missing_ok);
            return Ok(ResolvedObjectAddress { address, relation });
        }

        // If retrying, compare to last time.
        if have_old && oid_is_valid(old_address.classId) {
            if old_address.classId == address.classId
                && old_address.objectId == address.objectId
                && old_address.objectSubId == address.objectSubId
            {
                return Ok(ResolvedObjectAddress { address, relation });
            }
            if old_address.classId != RelationRelationId {
                if backend_catalog_catalog_seams::is_shared_relation::call(old_address.classId) {
                    backend_storage_lmgr_lmgr_seams::unlock_shared_object::call(
                        old_address.classId,
                        old_address.objectId,
                        0,
                        lockmode,
                    )?;
                } else {
                    backend_storage_lmgr_lmgr_seams::unlock_database_object::call(
                        old_address.classId,
                        old_address.objectId,
                        0,
                        lockmode,
                    )?;
                }
            }
        }

        // Lock non-relation objects now (transaction-scoped: keep the guard).
        if address.classId != RelationRelationId {
            if backend_catalog_catalog_seams::is_shared_relation::call(address.classId) {
                backend_storage_lmgr_lmgr_seams::lock_shared_object::call(
                    address.classId,
                    address.objectId,
                    0,
                    lockmode,
                )?
                .keep();
            } else {
                backend_storage_lmgr_lmgr_seams::lock_database_object::call(
                    address.classId,
                    address.objectId,
                    0,
                    lockmode,
                )?
                .keep();
            }
        }

        if inval_count == backend_storage_ipc_sinval_seams::shared_invalid_message_counter::call()
            || relation.is_some()
        {
            return Ok(ResolvedObjectAddress { address, relation });
        }
        old_address = address;
        have_old = true;
    }
}

/// `get_object_address_rv(ObjectType objtype, RangeVar *rel, List *object,
/// Relation *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c
/// 1225): prepend the RangeVar's name components to `object`, then resolve.
///
/// `rel` crosses as a real [`RangeVar`] value (the C `RangeVar *`); `object`
/// is the (possibly empty) `List *` of String nodes.
pub fn get_object_address_rv<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    rel: Option<&RangeVar>,
    object: &Node,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    if let Some(rv) = rel {
        let base = cast_list(object)?;
        let mk = |s: &str| Node::String(StringNode { sval: Some(s.to_string()) });
        // C builds the list with lcons (prepend) in catalog/schema/rel order so
        // the result reads catalogname.schemaname.relname.<object...>.
        let mut parts: Vec<Node> = Vec::new();
        if let Some(catalogname) = &rv.catalogname {
            parts.push(mk(catalogname));
        }
        if let Some(schemaname) = &rv.schemaname {
            parts.push(mk(schemaname));
        }
        parts.push(mk(&rv.relname));
        parts.extend(base.iter().cloned());
        let object = Node::List(parts);
        return get_object_address(mcx, objtype, &object, lockmode, missing_ok);
    }

    get_object_address(mcx, objtype, object, lockmode, missing_ok)
}

/* ---------------------------------------------------------------------------
 * The 13 get_object_address_* helpers (objectaddress.c 1247-1963)
 * ------------------------------------------------------------------------- */

/// `get_object_address_unqualified(ObjectType objtype, String *strval, bool
/// missing_ok)` (objectaddress.c 1247).
pub fn get_object_address_unqualified(
    objtype: ObjectType,
    strval: &Node,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let name = str_val(strval);
    let mut address = ObjectAddress {
        classId: INVALID_OID,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    match objtype {
        OBJECT_ACCESS_METHOD => {
            address.classId = AccessMethodRelationId;
            address.objectId = backend_commands_amcmds_seams::get_am_oid::call(name, missing_ok)?;
        }
        OBJECT_DATABASE => {
            address.classId = DatabaseRelationId;
            address.objectId = backend_commands_user_seams::get_database_oid::call(
                name.to_string(),
                missing_ok,
            )?;
        }
        OBJECT_EXTENSION => {
            address.classId = ExtensionRelationId;
            address.objectId =
                backend_commands_extension_seams::get_extension_oid::call(name, missing_ok)?;
        }
        OBJECT_TABLESPACE => {
            address.classId = TableSpaceRelationId;
            address.objectId =
                backend_commands_tablespace_seams::get_tablespace_oid::call(name, missing_ok)?;
        }
        OBJECT_ROLE => {
            address.classId = AuthIdRelationId;
            address.objectId =
                backend_commands_user_seams::get_role_oid::call(name.to_string(), missing_ok)?;
        }
        OBJECT_SCHEMA => {
            address.classId = NamespaceRelationId;
            address.objectId =
                backend_catalog_namespace_seams::get_namespace_oid::call(name, missing_ok)?;
        }
        OBJECT_LANGUAGE => {
            address.classId = LanguageRelationId;
            address.objectId = backend_commands_proclang_seams::get_language_oid::call(
                name,
                missing_ok,
            )?;
        }
        OBJECT_FDW => {
            address.classId = ForeignDataWrapperRelationId;
            address.objectId =
                backend_foreign_foreign_seams::get_foreign_data_wrapper_oid::call(name, missing_ok)?;
        }
        OBJECT_FOREIGN_SERVER => {
            address.classId = ForeignServerRelationId;
            address.objectId =
                backend_foreign_foreign_seams::get_foreign_server_oid::call(name, missing_ok)?;
        }
        OBJECT_EVENT_TRIGGER => {
            address.classId = EventTriggerRelationId;
            address.objectId =
                backend_commands_event_trigger_seams::get_event_trigger_oid::call(name, missing_ok)?;
        }
        OBJECT_PARAMETER_ACL => {
            address.classId = ParameterAclRelationId;
            address.objectId =
                backend_catalog_pg_parameter_acl_seams::parameter_acl_lookup::call(name, missing_ok)?;
        }
        OBJECT_PUBLICATION => {
            address.classId = PublicationRelationId;
            address.objectId =
                backend_utils_cache_lsyscache_seams::get_publication_oid::call(name, missing_ok)?;
        }
        OBJECT_SUBSCRIPTION => {
            address.classId = SubscriptionRelationId;
            address.objectId =
                backend_utils_cache_lsyscache_seams::get_subscription_oid::call(name, missing_ok)?;
        }
        _ => {
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as u32
            )));
        }
    }
    address.objectSubId = 0;
    Ok(address)
}

/// `get_relation_by_qualified_name(ObjectType objtype, List *object, Relation
/// *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 1338).
pub fn get_relation_by_qualified_name<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &Node,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    let mut address = ObjectAddress {
        classId: RelationRelationId,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let names = cast_list(object)?;
    let strs = name_list_strs(names);
    let rv = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&strs)?;
    let Some(relation) = backend_access_common_relation_seams::relation_openrv_extended::call(
        mcx, &rv, lockmode, missing_ok,
    )?
    else {
        return Ok(ResolvedObjectAddress {
            address,
            relation: None,
        });
    };

    let relkind = relation.rd_rel.relkind;
    match objtype {
        OBJECT_INDEX => {
            if relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX {
                return Err(wrong_object_type(relation.name(), "is not an index"));
            }
        }
        OBJECT_SEQUENCE => {
            if relkind != RELKIND_SEQUENCE {
                return Err(wrong_object_type(relation.name(), "is not a sequence"));
            }
        }
        OBJECT_TABLE => {
            if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
                return Err(wrong_object_type(relation.name(), "is not a table"));
            }
        }
        OBJECT_VIEW => {
            if relkind != RELKIND_VIEW {
                return Err(wrong_object_type(relation.name(), "is not a view"));
            }
        }
        OBJECT_MATVIEW => {
            if relkind != RELKIND_MATVIEW {
                return Err(wrong_object_type(
                    relation.name(),
                    "is not a materialized view",
                ));
            }
        }
        OBJECT_FOREIGN_TABLE => {
            if relkind != RELKIND_FOREIGN_TABLE {
                return Err(wrong_object_type(relation.name(), "is not a foreign table"));
            }
        }
        _ => {
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as u32
            )));
        }
    }

    address.objectId = relation.rd_id;
    Ok(ResolvedObjectAddress {
        address,
        relation: Some(relation),
    })
}

fn wrong_object_type(relname: &str, what: &str) -> PgError {
    PgError::new(ERROR, format!("\"{relname}\" {what}")).with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
}

/// `get_object_address_relobject(ObjectType objtype, List *object, Relation
/// *relp, bool missing_ok)` (objectaddress.c 1420).
pub fn get_object_address_relobject<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    object: &Node,
    missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    use types_storage::lock::AccessShareLock;

    let mut address = ObjectAddress {
        classId: INVALID_OID,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let object_list = cast_list(object)?;
    let depname = str_val(&object_list[object_list.len() - 1]);

    let nnames = object_list.len();
    if nnames < 2 {
        return Err(PgError::new(ERROR, "must specify relation and object name".to_string())
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }

    let relname: Vec<&str> = name_list_strs(&object_list[..nnames - 1]);
    let rv = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&relname)?;
    let relation = backend_access_common_relation_seams::relation_openrv_extended::call(
        mcx,
        &rv,
        AccessShareLock,
        missing_ok,
    )?;
    let reloid = relation.as_ref().map(|r| r.rd_id).unwrap_or(INVALID_OID);

    address.objectSubId = 0;
    match objtype {
        OBJECT_RULE => {
            address.classId = RewriteRelationId;
            address.objectId = if relation.is_some() {
                backend_rewrite_rewritesupport_seams::get_rewrite_oid::call(
                    reloid, depname, missing_ok,
                )?
            } else {
                INVALID_OID
            };
        }
        OBJECT_TRIGGER => {
            address.classId = TriggerRelationId;
            address.objectId = if relation.is_some() {
                backend_commands_trigger_seams::get_trigger_oid::call(reloid, depname, missing_ok)?
            } else {
                INVALID_OID
            };
        }
        OBJECT_TABCONSTRAINT => {
            address.classId = ConstraintRelationId;
            address.objectId = if relation.is_some() {
                backend_catalog_pg_constraint_seams::get_relation_constraint_oid::call(
                    mcx, reloid, depname, missing_ok,
                )?
            } else {
                INVALID_OID
            };
        }
        OBJECT_POLICY => {
            address.classId = PolicyRelationId;
            address.objectId = if relation.is_some() {
                backend_commands_policy_seams::get_relation_policy_oid::call(
                    reloid, depname, missing_ok,
                )?
            } else {
                INVALID_OID
            };
        }
        _ => {
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as u32
            )));
        }
    }

    // Avoid relcache leak when object not found.
    if !oid_is_valid(address.objectId) {
        if let Some(rel) = relation {
            rel.close(AccessShareLock)?;
        }
        return Ok(ResolvedObjectAddress {
            address,
            relation: None,
        });
    }

    Ok(ResolvedObjectAddress { address, relation })
}

/// `get_object_address_attribute(ObjectType objtype, List *object, Relation
/// *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 1499).
pub fn get_object_address_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    object: &Node,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    let object_list = cast_list(object)?;
    if object_list.len() < 2 {
        return Err(PgError::new(ERROR, "column name must be qualified".to_string())
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    let attname = str_val(&object_list[object_list.len() - 1]);
    let relname: Vec<&str> = name_list_strs(&object_list[..object_list.len() - 1]);

    // XXX no missing_ok support here (relation_openrv, not _extended).
    let rv = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&relname)?;
    let relation = backend_access_common_relation_seams::relation_openrv::call(mcx, &rv, lockmode)?;
    let reloid = relation.rd_id;

    let attnum = backend_utils_cache_lsyscache_seams::get_attnum::call(reloid, attname)?;
    if attnum == 0 {
        if !missing_ok {
            return Err(PgError::new(
                ERROR,
                format!(
                    "column \"{attname}\" of relation \"{}\" does not exist",
                    name_list_to_string(&object_list[..object_list.len() - 1])
                ),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
        }
        let address = ObjectAddress {
            classId: RelationRelationId,
            objectId: INVALID_OID,
            objectSubId: 0,
        };
        relation.close(lockmode)?;
        return Ok(ResolvedObjectAddress {
            address,
            relation: None,
        });
    }

    let address = ObjectAddress {
        classId: RelationRelationId,
        objectId: reloid,
        objectSubId: attnum as i32,
    };
    Ok(ResolvedObjectAddress {
        address,
        relation: Some(relation),
    })
}

/// `get_object_address_attrdef(ObjectType objtype, List *object, Relation
/// *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 1550).
pub fn get_object_address_attrdef<'mcx>(
    mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    object: &Node,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    let object_list = cast_list(object)?;
    if object_list.len() < 2 {
        return Err(PgError::new(ERROR, "column name must be qualified".to_string())
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    let attname = str_val(&object_list[object_list.len() - 1]);
    let relname: Vec<&str> = name_list_strs(&object_list[..object_list.len() - 1]);

    let rv = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&relname)?;
    let relation = backend_access_common_relation_seams::relation_openrv::call(mcx, &rv, lockmode)?;
    let reloid = relation.rd_id;

    let attnum = backend_utils_cache_lsyscache_seams::get_attnum::call(reloid, attname)?;
    let mut defoid = INVALID_OID;
    if attnum != 0 {
        defoid = backend_catalog_heap_seams::get_attr_default_oid::call(reloid, attnum)?;
    }
    if !oid_is_valid(defoid) {
        if !missing_ok {
            return Err(PgError::new(
                ERROR,
                format!(
                    "default value for column \"{attname}\" of relation \"{}\" does not exist",
                    name_list_to_string(&object_list[..object_list.len() - 1])
                ),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_COLUMN));
        }
        let address = ObjectAddress {
            classId: AttrDefaultRelationId,
            objectId: INVALID_OID,
            objectSubId: 0,
        };
        relation.close(lockmode)?;
        return Ok(ResolvedObjectAddress {
            address,
            relation: None,
        });
    }

    let address = ObjectAddress {
        classId: AttrDefaultRelationId,
        objectId: defoid,
        objectSubId: 0,
    };
    Ok(ResolvedObjectAddress {
        address,
        relation: Some(relation),
    })
}

/// `get_object_address_type(ObjectType objtype, TypeName *typename, bool
/// missing_ok)` (objectaddress.c 1608).
pub fn get_object_address_type(
    objtype: ObjectType,
    typename: &Node,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let mut address = ObjectAddress {
        classId: TypeRelationId,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let tn = typename
        .as_typename()
        .ok_or_else(|| PgError::error("castNode(TypeName)"))?;

    let typeoid = backend_parser_parse_type_seams::lookup_type_name_oid::call(tn, missing_ok)?;
    if !oid_is_valid(typeoid) {
        if !missing_ok {
            let tnstr = type_name_to_string(tn)?;
            return Err(PgError::new(ERROR, format!("type \"{tnstr}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
        return Ok(address);
    }
    address.objectId = typeoid;

    if objtype == OBJECT_DOMAIN {
        const TYPTYPE_DOMAIN: u8 = b'd';
        let typtype = backend_utils_cache_lsyscache_seams::get_typtype::call(typeoid)?;
        if typtype != TYPTYPE_DOMAIN {
            let tnstr = type_name_to_string(tn)?;
            return Err(PgError::new(ERROR, format!("\"{tnstr}\" is not a domain"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
        }
    }

    Ok(address)
}

/// `get_object_address_opcf(ObjectType objtype, List *object, bool
/// missing_ok)` (objectaddress.c 1647).
pub fn get_object_address_opcf(
    mcx: Mcx<'_>,
    objtype: ObjectType,
    object: &Node,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let object_list = cast_list(object)?;
    // XXX no missing_ok support here for the AM lookup.
    let amname = str_val(&object_list[0]);
    let amoid = backend_commands_amcmds_seams::get_index_am_oid::call(amname, false)?;
    let tail = name_list_strs(&object_list[1..]);

    let mut address = ObjectAddress {
        classId: INVALID_OID,
        objectId: INVALID_OID,
        objectSubId: 0,
    };
    match objtype {
        OBJECT_OPCLASS => {
            address.classId = OperatorClassRelationId;
            address.objectId = backend_commands_opclasscmds_seams::get_opclass_oid::call(
                mcx, amoid, &tail, missing_ok,
            )?;
        }
        OBJECT_OPFAMILY => {
            address.classId = OperatorFamilyRelationId;
            address.objectId = backend_commands_opclasscmds_seams::get_opfamily_oid::call(
                mcx, amoid, &tail, missing_ok,
            )?;
        }
        _ => {
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as u32
            )));
        }
    }
    address.objectSubId = 0;
    Ok(address)
}

/// `get_object_address_opf_member(ObjectType objtype, List *object, bool
/// missing_ok)` (objectaddress.c 1685).
pub fn get_object_address_opf_member(
    mcx: Mcx<'_>,
    objtype: ObjectType,
    object: &Node,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    use backend_utils_cache_syscache::{SearchSysCache4, SysCacheGetAttrNotNull};
    use backend_utils_cache_syscache::{AMOPSTRATEGY, AMPROCNUM};
    use types_cache::SysCacheKey;
    use types_datum::Datum as KeyDatum;

    let outer = cast_list(object)?;
    let first = cast_list(&outer[0])?;
    let membernum: i32 = str_val(&first[first.len() - 1]).parse().unwrap_or(0);
    let copy = Node::List(first[..first.len() - 1].to_vec());

    let famaddr = get_object_address_opcf(mcx, OBJECT_OPFAMILY, &copy, false)?;

    let typename_nodes = cast_list(&outer[1])?;
    let mut typeoids: [Oid; 2] = [INVALID_OID, INVALID_OID];
    let mut typenames: [Option<&Node>; 2] = [None, None];
    for (i, cell) in typename_nodes.iter().enumerate() {
        if i >= 2 {
            break;
        }
        typenames[i] = Some(cell);
        let typaddr = get_object_address_type(OBJECT_TYPE, cell, missing_ok)?;
        typeoids[i] = typaddr.objectId;
    }

    let mut address = ObjectAddress {
        classId: INVALID_OID,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let type_name_str = |idx: usize| -> PgResult<String> {
        match typenames[idx] {
            Some(n) => type_name_to_string(
                n.as_typename()
                    .ok_or_else(|| PgError::error("lfirst_node(TypeName)"))?,
            ),
            None => Ok(String::from("-")),
        }
    };

    match objtype {
        OBJECT_AMOP => {
            address.classId = AccessMethodOperatorRelationId;
            let tp = SearchSysCache4(
                mcx,
                AMOPSTRATEGY,
                SysCacheKey::Value(KeyDatum::from_oid(famaddr.objectId)),
                SysCacheKey::Value(KeyDatum::from_oid(typeoids[0])),
                SysCacheKey::Value(KeyDatum::from_oid(typeoids[1])),
                SysCacheKey::Value(KeyDatum::from_i16(membernum as i16)),
            )?;
            match tp {
                None => {
                    if !missing_ok {
                        return Err(PgError::new(
                            ERROR,
                            format!(
                                "operator {membernum} ({}, {}) of {} does not exist",
                                type_name_str(0)?,
                                type_name_str(1)?,
                                describe_family(mcx, &famaddr)?
                            ),
                        )
                        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
                    }
                }
                Some(tp) => {
                    let val = SysCacheGetAttrNotNull(mcx, AMOPSTRATEGY, &tp, 1)?;
                    address.objectId = tuplevalue_oid(&val);
                }
            }
        }
        OBJECT_AMPROC => {
            address.classId = AccessMethodProcedureRelationId;
            let tp = SearchSysCache4(
                mcx,
                AMPROCNUM,
                SysCacheKey::Value(KeyDatum::from_oid(famaddr.objectId)),
                SysCacheKey::Value(KeyDatum::from_oid(typeoids[0])),
                SysCacheKey::Value(KeyDatum::from_oid(typeoids[1])),
                SysCacheKey::Value(KeyDatum::from_i16(membernum as i16)),
            )?;
            match tp {
                None => {
                    if !missing_ok {
                        return Err(PgError::new(
                            ERROR,
                            format!(
                                "function {membernum} ({}, {}) of {} does not exist",
                                type_name_str(0)?,
                                type_name_str(1)?,
                                describe_family(mcx, &famaddr)?
                            ),
                        )
                        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
                    }
                }
                Some(tp) => {
                    let val = SysCacheGetAttrNotNull(mcx, AMPROCNUM, &tp, 1)?;
                    address.objectId = tuplevalue_oid(&val);
                }
            }
        }
        _ => {
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as u32
            )));
        }
    }

    Ok(address)
}

/// `getObjectDescription(&famaddr, false)` for the opf-member error messages.
fn describe_family<'mcx>(mcx: Mcx<'mcx>, famaddr: &ObjectAddress) -> PgResult<String> {
    let desc =
        backend_catalog_objectaddress_seams::get_object_description::call(mcx, famaddr, false)?;
    Ok(desc.map(|s| s.as_str().to_string()).unwrap_or_default())
}

/// `get_object_address_usermapping(List *object, bool missing_ok)`
/// (objectaddress.c 1797).
pub fn get_object_address_usermapping(
    mcx: Mcx<'_>,
    object: &Node,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    let mut address = ObjectAddress {
        classId: UserMappingRelationId,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let object_list = cast_list(object)?;
    let username = str_val(&object_list[0]);
    let servername = str_val(&object_list[1]);

    // C uses SearchSysCache(AUTHNAME) → Form_pg_authid.oid; get_role_oid is the
    // equivalent name→OID resolution (InvalidOid if PUBLIC).
    let userid = if username == "public" {
        INVALID_OID
    } else {
        let oid = backend_commands_user_seams::get_role_oid::call(username.to_string(), true)?;
        if !oid_is_valid(oid) {
            if !missing_ok {
                return Err(usermapping_missing(username, servername));
            }
            return Ok(address);
        }
        oid
    };

    let server =
        backend_foreign_foreign_seams::get_foreign_server_by_name::call(mcx, servername, true)?;
    let Some(server) = server else {
        if !missing_ok {
            return Err(PgError::new(
                ERROR,
                format!("server \"{servername}\" does not exist"),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
        return Ok(address);
    };

    let umoid = backend_foreign_foreign_seams::usermapping_oid::call(userid, server.serverid)?;
    if !oid_is_valid(umoid) {
        if !missing_ok {
            return Err(usermapping_missing(username, servername));
        }
        return Ok(address);
    }
    address.objectId = umoid;
    Ok(address)
}

fn usermapping_missing(username: &str, servername: &str) -> PgError {
    PgError::new(
        ERROR,
        format!("user mapping for user \"{username}\" on server \"{servername}\" does not exist"),
    )
    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
}

/// `get_object_address_publication_rel(List *object, Relation *relp, bool
/// missing_ok)` (objectaddress.c 1868).
pub fn get_object_address_publication_rel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &Node,
    missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    use backend_utils_cache_syscache::GetSysCacheOid;
    use backend_utils_cache_syscache::PUBLICATIONRELMAP;
    use types_cache::SysCacheKey;
    use types_datum::Datum as KeyDatum;
    use types_storage::lock::AccessShareLock;

    let mut address = ObjectAddress {
        classId: PublicationRelRelationId,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let object_list = cast_list(object)?;
    // linitial(object) is itself the relation-name List.
    let relname = name_list_strs(cast_list(&object_list[0])?);
    let rv = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&relname)?;
    let Some(relation) = backend_access_common_relation_seams::relation_openrv_extended::call(
        mcx,
        &rv,
        AccessShareLock,
        missing_ok,
    )?
    else {
        return Ok(ResolvedObjectAddress {
            address,
            relation: None,
        });
    };

    let pubname = str_val(&object_list[1]);
    let pub_oid =
        backend_utils_cache_lsyscache_seams::get_publication_oid::call(pubname, missing_ok)?;
    if !oid_is_valid(pub_oid) {
        relation.close(AccessShareLock)?;
        return Ok(ResolvedObjectAddress {
            address,
            relation: None,
        });
    }

    let relid = relation.rd_id;
    address.objectId = GetSysCacheOid(
        mcx,
        PUBLICATIONRELMAP,
        Anum_pg_publication_rel_oid,
        SysCacheKey::Value(KeyDatum::from_oid(relid)),
        SysCacheKey::Value(KeyDatum::from_oid(pub_oid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if !oid_is_valid(address.objectId) {
        if !missing_ok {
            let err = PgError::new(
                ERROR,
                format!(
                    "publication relation \"{}\" in publication \"{pubname}\" does not exist",
                    relation.name()
                ),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT);
            relation.close(AccessShareLock)?;
            return Err(err);
        }
        relation.close(AccessShareLock)?;
        return Ok(ResolvedObjectAddress {
            address,
            relation: None,
        });
    }

    Ok(ResolvedObjectAddress {
        address,
        relation: Some(relation),
    })
}

/// `get_object_address_publication_schema(List *object, bool missing_ok)`
/// (objectaddress.c 1921).
pub fn get_object_address_publication_schema(
    mcx: Mcx<'_>,
    object: &Node,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    use backend_utils_cache_syscache::GetSysCacheOid;
    use backend_utils_cache_syscache::PUBLICATIONNAMESPACEMAP;
    use types_cache::SysCacheKey;
    use types_datum::Datum as KeyDatum;

    let mut address = ObjectAddress {
        classId: PublicationNamespaceRelationId,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let object_list = cast_list(object)?;
    let schemaname = str_val(&object_list[0]);
    let pubname = str_val(&object_list[1]);

    let schemaid =
        backend_catalog_namespace_seams::get_namespace_oid::call(schemaname, missing_ok)?;
    if !oid_is_valid(schemaid) {
        return Ok(address);
    }

    let pub_oid =
        backend_utils_cache_lsyscache_seams::get_publication_oid::call(pubname, missing_ok)?;
    if !oid_is_valid(pub_oid) {
        return Ok(address);
    }

    address.objectId = GetSysCacheOid(
        mcx,
        PUBLICATIONNAMESPACEMAP,
        Anum_pg_publication_namespace_oid,
        SysCacheKey::Value(KeyDatum::from_oid(schemaid)),
        SysCacheKey::Value(KeyDatum::from_oid(pub_oid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if !oid_is_valid(address.objectId) && !missing_ok {
        return Err(PgError::new(
            ERROR,
            format!(
                "publication schema \"{schemaname}\" in publication \"{pubname}\" does not exist"
            ),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }

    Ok(address)
}

/// `get_object_address_defacl(List *object, bool missing_ok)`
/// (objectaddress.c 1963).
pub fn get_object_address_defacl(
    mcx: Mcx<'_>,
    object: &Node,
    missing_ok: bool,
) -> PgResult<ObjectAddress> {
    use backend_utils_cache_syscache::GetSysCacheOid;
    use backend_utils_cache_syscache::DEFACLROLENSPOBJ;
    use types_cache::SysCacheKey;
    use types_datum::Datum as KeyDatum;

    const DEFACLOBJ_RELATION: u8 = b'r';
    const DEFACLOBJ_SEQUENCE: u8 = b'S';
    const DEFACLOBJ_FUNCTION: u8 = b'f';
    const DEFACLOBJ_TYPE: u8 = b'T';
    const DEFACLOBJ_NAMESPACE: u8 = b'n';
    const DEFACLOBJ_LARGEOBJECT: u8 = b'L';

    let mut address = ObjectAddress {
        classId: DefaultAclRelationId,
        objectId: INVALID_OID,
        objectSubId: 0,
    };

    let object_list = cast_list(object)?;
    let username = str_val(&object_list[1]);
    let schema = if object_list.len() >= 3 {
        Some(str_val(&object_list[2]))
    } else {
        None
    };

    let objtype_str0 = str_val(&object_list[0]);
    let objtype = objtype_str0.as_bytes().first().copied().unwrap_or(0);
    let objtype_str = match objtype {
        DEFACLOBJ_RELATION => "tables",
        DEFACLOBJ_SEQUENCE => "sequences",
        DEFACLOBJ_FUNCTION => "functions",
        DEFACLOBJ_TYPE => "types",
        DEFACLOBJ_NAMESPACE => "schemas",
        DEFACLOBJ_LARGEOBJECT => "large objects",
        other => {
            return Err(PgError::new(
                ERROR,
                format!("unrecognized default ACL object type \"{}\"", other as char),
            )
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    };

    let userid = backend_commands_user_seams::get_role_oid::call(username.to_string(), true)?;
    if !oid_is_valid(userid) {
        return defacl_not_found(missing_ok, username, schema, objtype_str, address);
    }

    let schemaid = if let Some(schema) = schema {
        let sid = backend_catalog_namespace_seams::get_namespace_oid::call(schema, true)?;
        if !oid_is_valid(sid) {
            return defacl_not_found(missing_ok, username, Some(schema), objtype_str, address);
        }
        sid
    } else {
        INVALID_OID
    };

    address.objectId = GetSysCacheOid(
        mcx,
        DEFACLROLENSPOBJ,
        Anum_pg_default_acl_oid,
        SysCacheKey::Value(KeyDatum::from_oid(userid)),
        SysCacheKey::Value(KeyDatum::from_oid(schemaid)),
        SysCacheKey::Value(KeyDatum::from_char(objtype as i8)),
        SysCacheKey::UNUSED,
    )?;
    if !oid_is_valid(address.objectId) {
        return defacl_not_found(missing_ok, username, schema, objtype_str, address);
    }

    Ok(address)
}

fn defacl_not_found(
    missing_ok: bool,
    username: &str,
    schema: Option<&str>,
    objtype_str: &str,
    address: ObjectAddress,
) -> PgResult<ObjectAddress> {
    if !missing_ok {
        let msg = match schema {
            Some(schema) => format!(
                "default ACL for user \"{username}\" in schema \"{schema}\" on {objtype_str} does not exist"
            ),
            None => format!("default ACL for user \"{username}\" on {objtype_str} does not exist"),
        };
        return Err(PgError::new(ERROR, msg).with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(address)
}

/* ---------------------------------------------------------------------------
 * Ownership + namespace (objectaddress.c 2391-2608)
 * ------------------------------------------------------------------------- */

/// `check_object_ownership(Oid roleid, ObjectType objtype, ObjectAddress
/// address, Node *object, Relation relation)` (objectaddress.c 2391).
pub fn check_object_ownership<'mcx>(
    roleid: Oid,
    objtype: ObjectType,
    address: ObjectAddress,
    object: &Node,
    relation: Option<&Relation<'mcx>>,
) -> PgResult<()> {
    use types_acl::ACLCHECK_NOT_OWNER;

    match objtype {
        OBJECT_INDEX | OBJECT_SEQUENCE | OBJECT_TABLE | OBJECT_VIEW | OBJECT_MATVIEW
        | OBJECT_FOREIGN_TABLE | OBJECT_COLUMN | OBJECT_RULE | OBJECT_TRIGGER | OBJECT_POLICY
        | OBJECT_TABCONSTRAINT => {
            let rel = relation.expect("relation-member ownership check requires an open relation");
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                RelationRelationId,
                rel.rd_id,
                roleid,
            )? {
                return backend_catalog_aclchk_seams::aclcheck_error::call(
                    ACLCHECK_NOT_OWNER,
                    objtype,
                    Some(rel.name().to_string()),
                );
            }
        }
        OBJECT_TYPE | OBJECT_DOMAIN | OBJECT_ATTRIBUTE => {
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                address.classId,
                address.objectId,
                roleid,
            )? {
                return backend_catalog_aclchk_seams::aclcheck_error_type::call(
                    ACLCHECK_NOT_OWNER,
                    address.objectId,
                );
            }
        }
        OBJECT_DOMCONSTRAINT => {
            let contypid = match backend_catalog_pg_constraint_seams::constraint_type_oids::call(
                address.objectId,
            )? {
                Some((_conrelid, contypid, _oid)) => contypid,
                None => {
                    return Err(PgError::error(format!(
                        "constraint with OID {} does not exist",
                        address.objectId
                    )));
                }
            };
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                TypeRelationId,
                contypid,
                roleid,
            )? {
                return backend_catalog_aclchk_seams::aclcheck_error_type::call(
                    ACLCHECK_NOT_OWNER,
                    contypid,
                );
            }
        }
        OBJECT_AGGREGATE | OBJECT_FUNCTION | OBJECT_PROCEDURE | OBJECT_ROUTINE | OBJECT_OPERATOR => {
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                address.classId,
                address.objectId,
                roleid,
            )? {
                let owa = object
                    .as_objectwithargs()
                    .ok_or_else(|| PgError::error("castNode(ObjectWithArgs)"))?;
                let name = owa.objname.join(".");
                return backend_catalog_aclchk_seams::aclcheck_error::call(
                    ACLCHECK_NOT_OWNER,
                    objtype,
                    Some(name),
                );
            }
        }
        OBJECT_DATABASE | OBJECT_EVENT_TRIGGER | OBJECT_EXTENSION | OBJECT_FDW
        | OBJECT_FOREIGN_SERVER | OBJECT_LANGUAGE | OBJECT_PUBLICATION | OBJECT_SCHEMA
        | OBJECT_SUBSCRIPTION | OBJECT_TABLESPACE => {
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                address.classId,
                address.objectId,
                roleid,
            )? {
                return backend_catalog_aclchk_seams::aclcheck_error::call(
                    ACLCHECK_NOT_OWNER,
                    objtype,
                    Some(str_val(object).to_string()),
                );
            }
        }
        OBJECT_COLLATION | OBJECT_CONVERSION | OBJECT_OPCLASS | OBJECT_OPFAMILY
        | OBJECT_STATISTIC_EXT | OBJECT_TSDICTIONARY | OBJECT_TSCONFIGURATION => {
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                address.classId,
                address.objectId,
                roleid,
            )? {
                return backend_catalog_aclchk_seams::aclcheck_error::call(
                    ACLCHECK_NOT_OWNER,
                    objtype,
                    Some(name_list_to_string(cast_list(object)?)),
                );
            }
        }
        OBJECT_LARGEOBJECT => {
            // lo_compat_privileges: the GUC machinery is unported and defaults
            // to `false` (boot value) repo-wide (cf. inv_api.c port). With the
            // default, ownership is required.
            let lo_compat_privileges = false;
            if !lo_compat_privileges
                && !backend_catalog_aclchk_seams::object_ownercheck::call(
                    address.classId,
                    address.objectId,
                    roleid,
                )?
            {
                return Err(PgError::new(
                    ERROR,
                    format!("must be owner of large object {}", address.objectId),
                )
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
            }
        }
        OBJECT_CAST => {
            let list = cast_list(object)?;
            let sourcetype = list[0]
                .as_typename()
                .ok_or_else(|| PgError::error("linitial_node(TypeName)"))?;
            let targettype = list[1]
                .as_typename()
                .ok_or_else(|| PgError::error("lsecond_node(TypeName)"))?;
            let sourcetypeid =
                backend_parser_parse_type_seams::typename_type_id_node::call(sourcetype)?;
            let targettypeid =
                backend_parser_parse_type_seams::typename_type_id_node::call(targettype)?;
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                TypeRelationId,
                sourcetypeid,
                roleid,
            )? && !backend_catalog_aclchk_seams::object_ownercheck::call(
                TypeRelationId,
                targettypeid,
                roleid,
            )? {
                let src =
                    backend_utils_adt_format_type_seams::format_type_be_str::call(sourcetypeid)?;
                let tgt =
                    backend_utils_adt_format_type_seams::format_type_be_str::call(targettypeid)?;
                return Err(PgError::new(
                    ERROR,
                    format!("must be owner of type {src} or type {tgt}"),
                )
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
            }
        }
        OBJECT_TRANSFORM => {
            let list = cast_list(object)?;
            let typename = list[0]
                .as_typename()
                .ok_or_else(|| PgError::error("linitial_node(TypeName)"))?;
            let typeid = backend_parser_parse_type_seams::typename_type_id_node::call(typename)?;
            if !backend_catalog_aclchk_seams::object_ownercheck::call(
                TypeRelationId,
                typeid,
                roleid,
            )? {
                return backend_catalog_aclchk_seams::aclcheck_error_type::call(
                    ACLCHECK_NOT_OWNER,
                    typeid,
                );
            }
        }
        OBJECT_ROLE => {
            // Roles are "owned" by those with CREATEROLE + admin option;
            // superusers are owned only by superusers.
            if backend_commands_user_seams::superuser_arg::call(address.objectId)? {
                if !backend_commands_user_seams::superuser_arg::call(roleid)? {
                    return Err(PgError::new(ERROR, "permission denied".to_string())
                        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                        .with_detail("The current user must have the SUPERUSER attribute."));
                }
            } else {
                if !backend_commands_user_seams::has_createrole_privilege::call(roleid)? {
                    return Err(PgError::new(ERROR, "permission denied".to_string())
                        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                        .with_detail("The current user must have the CREATEROLE attribute."));
                }
                if !backend_commands_user_seams::is_admin_of_role::call(roleid, address.objectId)? {
                    // GetUserNameFromId(address.objectId, true) in a transient
                    // context — the name is copied into the detail string.
                    let cx = mcx::MemoryContext::new("check_object_ownership");
                    let target =
                        backend_commands_user_seams::get_user_name_from_id::call(
                            cx.mcx(),
                            address.objectId,
                            true,
                        )?
                        .as_str()
                        .to_string();
                    return Err(PgError::new(ERROR, "permission denied".to_string())
                        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                        .with_detail(format!(
                            "The current user must have the ADMIN option on role \"{target}\"."
                        )));
                }
            }
        }
        OBJECT_TSPARSER | OBJECT_TSTEMPLATE | OBJECT_ACCESS_METHOD | OBJECT_PARAMETER_ACL => {
            // Owned by superusers.
            if !backend_commands_user_seams::superuser_arg::call(roleid)? {
                return Err(PgError::new(ERROR, "must be superuser".to_string())
                    .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
            }
        }
        OBJECT_AMOP | OBJECT_AMPROC | OBJECT_DEFAULT | OBJECT_DEFACL
        | OBJECT_PUBLICATION_NAMESPACE | OBJECT_PUBLICATION_REL | OBJECT_USER_MAPPING => {
            return Err(PgError::error(format!(
                "unsupported object type: {}",
                objtype as u32
            )));
        }
    }
    Ok(())
}

/// `object_ownercheck(Oid classid, Oid objectid, Oid roleid)` (aclchk.c): the
/// catalog-class ownership probe. Routes to the aclchk owner's seam.
pub fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> PgResult<bool> {
    backend_catalog_aclchk_seams::object_ownercheck::call(classid, objectid, roleid)
}

/* ---------------------------------------------------------------------------
 * get_object_namespace (objectaddress.c 2573)
 * ------------------------------------------------------------------------- */

/// `get_object_namespace(const ObjectAddress *address)` (objectaddress.c
/// 2573).
pub fn get_object_namespace(address: &ObjectAddress) -> PgResult<Oid> {
    use backend_utils_cache_syscache::{SearchSysCache1, SysCacheGetAttrNotNull};
    use types_cache::SysCacheKey;
    use types_datum::Datum;

    let property = crate::properties::get_object_property_data(address.classId)?;
    if property.attnum_namespace == crate::consts::InvalidAttrNumber {
        return Ok(INVALID_OID);
    }

    let cache = property.oid_catcache_id;
    debug_assert!(cache != -1);

    let cx = mcx::MemoryContext::new("get_object_namespace");
    let mcx = cx.mcx();
    let tuple = SearchSysCache1(
        mcx,
        cache,
        SysCacheKey::Value(Datum::from_oid(address.objectId)),
    )?;
    let Some(tuple) = tuple else {
        return Err(PgError::error(format!(
            "cache lookup failed for cache {} oid {}",
            cache, address.objectId
        )));
    };
    let val = SysCacheGetAttrNotNull(mcx, cache, &tuple, property.attnum_namespace as i32)?;
    Ok(tuplevalue_oid(&val))
}

/* ---------------------------------------------------------------------------
 * string↔objtype + relkind mapping (objectaddress.c 2609, 6186)
 * ------------------------------------------------------------------------- */

/// `read_objtype_from_string(const char *objtype)` (objectaddress.c 2609).
pub fn read_objtype_from_string(objtype: &str) -> PgResult<i32> {
    for entry in crate::tables::OBJECT_TYPE_MAP {
        if entry.tm_name == objtype {
            return Ok(entry.tm_type);
        }
    }
    Err(PgError::error(format!(
        "unrecognized object type \"{objtype}\""
    )))
}

/// `get_relkind_objtype(char relkind)` (objectaddress.c 6186).
pub fn get_relkind_objtype(relkind: u8) -> ObjectType {
    use types_tuple::access::RELKIND_TOASTVALUE;
    match relkind {
        x if x == RELKIND_RELATION || x == RELKIND_PARTITIONED_TABLE => OBJECT_TABLE,
        x if x == RELKIND_INDEX || x == RELKIND_PARTITIONED_INDEX => OBJECT_INDEX,
        x if x == RELKIND_SEQUENCE => OBJECT_SEQUENCE,
        x if x == RELKIND_VIEW => OBJECT_VIEW,
        x if x == RELKIND_MATVIEW => OBJECT_MATVIEW,
        x if x == RELKIND_FOREIGN_TABLE => OBJECT_FOREIGN_TABLE,
        x if x == RELKIND_TOASTVALUE => OBJECT_TABLE,
        _ => OBJECT_TABLE,
    }
}

/* ---------------------------------------------------------------------------
 * get_catalog_object_by_oid[_extended] (objectaddress.c 2790-2862)
 * ------------------------------------------------------------------------- */

/// `get_catalog_object_by_oid(Relation catalog, AttrNumber oidcol, Oid
/// objectId)` (objectaddress.c 2790).
pub fn get_catalog_object_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    catalog: &Relation<'mcx>,
    oidcol: i16,
    object_id: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    get_catalog_object_by_oid_extended(mcx, catalog, oidcol, object_id, false)
}

/// The `(castsource, casttarget)` projection of the `CastRelationId` arm shared
/// by `getObjectDescription` (objectaddress.c 2969) and `getObjectIdentityParts`
/// (objectaddress.c 4927). Faithful to the C inline body:
///
/// ```c
/// castDesc = table_open(CastRelationId, AccessShareLock);
/// rcscan = systable_beginscan(castDesc, CastOidIndexId, true, NULL, 1, skey);
/// tup = systable_getnext(rcscan);
/// if (!HeapTupleIsValid(tup)) { ... return; }     // -> Ok(None)
/// castForm = (Form_pg_cast) GETSTRUCT(tup);
/// // castForm->castsource, castForm->casttarget
/// systable_endscan(rcscan);
/// table_close(castDesc, AccessShareLock);
/// ```
///
/// `castsource`/`casttarget` are `NOT NULL` columns, so they are read directly
/// (no null short-circuit). `Ok(None)` is the C `!HeapTupleIsValid(tup)` (the
/// caller raises its own "could not find tuple for cast" when `!missing_ok`).
pub fn cast_source_target<'mcx>(
    mcx: Mcx<'mcx>,
    castid: Oid,
) -> PgResult<Option<(Oid, Oid)>> {
    use types_storage::lock::AccessShareLock;

    let cast_desc = backend_access_common_relation_seams::relation_open::call(
        mcx,
        CastRelationId,
        AccessShareLock,
    )?;

    let tup = get_catalog_object_by_oid(mcx, &cast_desc, Anum_pg_cast_oid, castid)?;

    let Some(tup) = tup else {
        cast_desc.close(AccessShareLock)?;
        return Ok(None);
    };

    let castsource = cast_attr_oid(mcx, &tup, Anum_pg_cast_castsource, &cast_desc)?;
    let casttarget = cast_attr_oid(mcx, &tup, Anum_pg_cast_casttarget, &cast_desc)?;

    cast_desc.close(AccessShareLock)?;
    Ok(Some((castsource, casttarget)))
}

/// `heap_getattr(tup, attnum, RelationGetDescr(castDesc), &isnull)` for a
/// `NOT NULL` `pg_cast` oid column, returning the `Oid` value. Mirrors the
/// macro's `fastgetattr` -> `nocachegetattr` for `attnum > 0`.
fn cast_attr_oid<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &FormedTuple<'_>,
    attnum: i16,
    cast_desc: &Relation<'mcx>,
) -> PgResult<Oid> {
    let datum: TupleDatum<'mcx> = backend_access_common_heaptuple::nocachegetattr(
        mcx,
        &tup.tuple,
        attnum as i32,
        &cast_desc.rd_att,
        tup.data.as_slice(),
    )?;
    Ok(datum.as_oid())
}

/// `get_catalog_object_by_oid_extended(Relation catalog, AttrNumber oidcol,
/// Oid objectId, bool locktuple)` (objectaddress.c 2803).
pub fn get_catalog_object_by_oid_extended<'mcx>(
    mcx: Mcx<'mcx>,
    catalog: &Relation<'mcx>,
    oidcol: i16,
    object_id: Oid,
    locktuple: bool,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    backend_catalog_indexing_seams::get_catalog_object_by_oid::call(
        mcx, catalog, oidcol, object_id, locktuple,
    )
}
