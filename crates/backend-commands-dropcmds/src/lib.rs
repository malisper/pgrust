#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large error type shared across the whole tree, so boxing it
// would diverge from every sibling crate's `Result` shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/dropcmds.c` — handle various "DROP" operations.
//!
//! The generic DROP driver for object types that do not need the special
//! relation/index handling (DROP TYPE / DOMAIN / FUNCTION / AGGREGATE /
//! OPERATOR / COLLATION / CONVERSION / CAST / TRANSFORM / TRIGGER / RULE /
//! POLICY / TEXT SEARCH … and the other types in the C `switch`).
//!
//! dropcmds.c's own decision logic lives in-crate: the per-object resolve loop,
//! the `OidIsValid` skip test, the OBJECT_FUNCTION aggregate guard, the
//! `!OidIsValid(namespaceId) || !object_ownercheck(...)` ownership shortcut, the
//! temp-namespace flag, and the entire `does_not_exist_skipping` `switch` that
//! discovers *why* an object was not found (missing schema / missing owning
//! relation / missing datatype) so the skip-NOTICE blames the right thing.
//!
//! Genuine cross-subsystem callees cross seams into their owners
//! (`get_object_address`, `get_object_namespace`, `check_object_ownership`,
//! `object_ownercheck`, `get_func_prokind`, `isTempNamespace`,
//! `performMultipleDeletions`, `LookupTypeNameOid`, `TypeNameToString`,
//! `TypeNameListToString`); the name-resolution helpers `NameListToString`,
//! `LookupNamespaceNoError`, `makeRangeVarFromNameList`, `RangeVarGetRelid`
//! call directly into the (ported) `backend-catalog-namespace`.
//!
//! ## Function inventory (dropcmds.c, PostgreSQL 18.3 — 5 functions)
//!
//! * `RemoveObjects`                          — C 52-126
//! * `owningrel_does_not_exist_skipping`      — C 138-160 (static)
//! * `schema_does_not_exist_skipping`         — C 173-190 (static)
//! * `type_in_list_does_not_exist_skipping`   — C 205-232 (static)
//! * `does_not_exist_skipping`                — C 242-524 (static)

use backend_utils_error::ereport;
use mcx::{Mcx, MemoryContext};

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{Oid, OidIsValid};
use types_core::catalog::NAMESPACE_RELATION_ID;
use types_error::{ErrorLevel, ErrorLocation, PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_nodes::parsenodes::{
    ObjectType, OBJECT_ACCESS_METHOD, OBJECT_AGGREGATE, OBJECT_AMOP, OBJECT_AMPROC,
    OBJECT_ATTRIBUTE, OBJECT_CAST, OBJECT_COLLATION, OBJECT_COLUMN, OBJECT_CONVERSION,
    OBJECT_DATABASE, OBJECT_DEFACL, OBJECT_DEFAULT, OBJECT_DOMAIN, OBJECT_DOMCONSTRAINT,
    OBJECT_EVENT_TRIGGER, OBJECT_EXTENSION, OBJECT_FDW, OBJECT_FOREIGN_SERVER,
    OBJECT_FOREIGN_TABLE, OBJECT_FUNCTION, OBJECT_INDEX, OBJECT_LANGUAGE, OBJECT_LARGEOBJECT,
    OBJECT_MATVIEW, OBJECT_OPCLASS, OBJECT_OPERATOR, OBJECT_OPFAMILY, OBJECT_PARAMETER_ACL,
    OBJECT_POLICY, OBJECT_PROCEDURE, OBJECT_PUBLICATION, OBJECT_PUBLICATION_NAMESPACE,
    OBJECT_PUBLICATION_REL, OBJECT_ROLE, OBJECT_ROUTINE, OBJECT_RULE, OBJECT_SCHEMA,
    OBJECT_SEQUENCE, OBJECT_STATISTIC_EXT, OBJECT_SUBSCRIPTION, OBJECT_TABCONSTRAINT,
    OBJECT_TABLE, OBJECT_TABLESPACE, OBJECT_TRANSFORM, OBJECT_TRIGGER, OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY, OBJECT_TSPARSER, OBJECT_TSTEMPLATE, OBJECT_TYPE, OBJECT_USER_MAPPING,
    OBJECT_VIEW,
};
use types_parsenodes::{DropStmt, Node, ObjectWithArgs, StringNode, TypeName};
use types_storage::lock::{AccessExclusiveLock, NoLock};

use backend_catalog_aclchk_seams::object_ownercheck;
use backend_catalog_dependency_seams::perform_multiple_deletions;
use backend_catalog_namespace_seams::is_temp_namespace;
use backend_catalog_objectaddress_seams::{
    check_object_ownership, get_object_address, get_object_namespace, ResolvedObjectAddress,
};
use backend_parser_parse_type_seams::{
    lookup_type_name_oid, type_name_list_to_string, typename_to_string_node,
};
use backend_utils_cache_lsyscache_seams::get_func_prokind;
use backend_utils_init_miscinit_seams::get_user_id;
use backend_access_transam_xact_seams::set_xact_accessed_temp_namespace;

use backend_catalog_namespace::{
    makeRangeVarFromNameList, LookupNamespaceNoError, NameListToString, RangeVarGetRelid,
};

const NOTICE: ErrorLevel = types_error::error::NOTICE;
const ERROR: ErrorLevel = types_error::error::ERROR;

/// `NamespaceRelationId` (`catalog/pg_namespace.h`).
const NamespaceRelationId: Oid = NAMESPACE_RELATION_ID;

/// pg_proc.prokind value for an aggregate (`catalog/pg_proc.h`); `get_func_prokind`
/// returns the `char` as `u8`.
const PROKIND_AGGREGATE: u8 = b'a';

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/dropcmds.c", lineno, funcname)
}

/// `elog(ERROR, fmt, ...)` — the internal-error helper used by the
/// `does_not_exist_skipping` `switch` for object types handled elsewhere,
/// unused, or unrecognized. `elog` messages are not translatable.
fn elog_error(message: String) -> PgError {
    PgError::error(message)
}

/// Install this crate's inward seams.
pub fn init_seams() {
    backend_commands_dropcmds_seams::remove_objects::set(remove_objects);
    // ProcessUtilitySlow / ExecDropStmt dispatch target (utility.c) for the
    // general object-removal (`RemoveObjects`) leg — decode the rich `DropStmt`
    // node and run the ported `remove_objects` body.
    backend_tcop_utility_out_seams::remove_objects::set(remove_objects_seam);
}

/// Outward-seam adapter for `RemoveObjects(stmt)` (utility.c `ExecDropStmt`
/// default leg): decode the rich [`types_nodes::nodes::Node`] `DropStmt` into the
/// flat [`types_parsenodes::DropStmt`] the ported body consumes, then run it.
/// `mcx` is threaded for parity but `remove_objects` runs in its own context.
fn remove_objects_seam(_mcx: Mcx<'_>, stmt: &types_nodes::nodes::Node<'_>) -> PgResult<()> {
    let ds = match stmt.as_dropstmt() {
        Some(d) => d,
        None => return Err(PgError::error("remove_objects_seam: statement is not a DropStmt")),
    };

    let mut objects: Vec<Node> = Vec::with_capacity(ds.objects.len());
    for obj in ds.objects.iter() {
        objects.push(backend_parser_parse_type::rich_node_to_parse(obj)?);
    }

    let pn = DropStmt {
        objects,
        removeType: ds.removeType,
        behavior: ds.behavior,
        missing_ok: ds.missing_ok,
        concurrent: ds.concurrent,
    };
    remove_objects(&pn)
}

/// Drop one or more objects. dropcmds.c:52-126.
///
/// We don't currently handle all object types here. Relations, for example,
/// require special handling, because (for example) indexes have additional
/// locking requirements.
///
/// We look up all the objects first, and then delete them in a single
/// `performMultipleDeletions()` call. This avoids unnecessary DROP RESTRICT
/// errors if there are dependencies between them.
pub fn remove_objects(stmt: &DropStmt) -> PgResult<()> {
    // The DROP driver works in the current memory context (C
    // `CurrentMemoryContext`); a per-call context stands in here for the
    // transient catalog/name-resolution copies the resolution seams make.
    let ctx = MemoryContext::new("RemoveObjects");
    let mcx = ctx.mcx();

    // ObjectAddresses *objects; objects = new_object_addresses();
    let mut objects: Vec<ObjectAddress> = Vec::new();

    // foreach(cell1, stmt->objects)
    for object in &stmt.objects {
        /* Get an ObjectAddress for the object. */
        let ResolvedObjectAddress { address, relation } = get_object_address::call(
            mcx,
            stmt.removeType,
            object,
            AccessExclusiveLock,
            stmt.missing_ok,
        )?;

        /*
         * Issue NOTICE if supplied object was not found.  Note this is only
         * relevant in the missing_ok case, because otherwise
         * get_object_address would have thrown an error.
         */
        if !OidIsValid(address.objectId) {
            debug_assert!(stmt.missing_ok);
            does_not_exist_skipping(mcx, stmt.removeType, object)?;
            continue;
        }

        /*
         * Although COMMENT ON FUNCTION, SECURITY LABEL ON FUNCTION, etc. are
         * happy to operate on an aggregate as on any other function, we have
         * historically not allowed this for DROP FUNCTION.
         */
        if stmt.removeType == OBJECT_FUNCTION
            && get_func_prokind::call(address.objectId)? == PROKIND_AGGREGATE
        {
            let owa = as_objectwithargs(object);
            let funcname = NameListToString(mcx, &objname_namelist(owa))?;
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{}\" is an aggregate function", funcname.as_str()))
                .errhint("Use DROP AGGREGATE to drop aggregate functions.")
                .finish(here(94, "RemoveObjects"));
        }

        /* Check permissions. */
        // namespaceId = get_object_namespace(&address);
        let namespace_id = get_object_namespace::call(&address)?;
        if !OidIsValid(namespace_id)
            || !object_ownercheck::call(NamespaceRelationId, namespace_id, get_user_id::call())?
        {
            check_object_ownership::call(
                get_user_id::call(),
                stmt.removeType,
                address,
                object,
                relation.as_ref(),
            )?;
        }

        /*
         * Make note if a temporary namespace has been accessed in this
         * transaction.
         */
        if OidIsValid(namespace_id) && is_temp_namespace::call(namespace_id)? {
            set_xact_accessed_temp_namespace::call();
        }

        /* Release any relcache reference count, but keep lock until commit. */
        if let Some(rel) = relation {
            rel.close(NoLock)?;
        }

        // add_exact_object_address(&address, objects);
        objects.push(address);
    }

    /* Here we really delete them. */
    perform_multiple_deletions::call(&objects, stmt.behavior, 0)?;

    // free_object_addresses(objects) — the owned Vec drops here.
    Ok(())
}

/// `owningrel_does_not_exist_skipping` — subroutine for [`remove_objects`].
/// dropcmds.c:138-160.
///
/// After determining that a specification for a rule or trigger returns that
/// the specified object does not exist, test whether its owning relation, and
/// its schema, exist or not; if they do, return `Ok(None)` --- the trigger or
/// rule itself is missing instead. If the owning relation or its schema do not
/// exist, return `Ok(Some((msg, name)))`.
fn owningrel_does_not_exist_skipping<'mcx>(
    mcx: Mcx<'mcx>,
    object: &[Node],
) -> PgResult<Option<(&'static str, String)>> {
    // parent_object = list_copy_head(object, list_length(object) - 1);
    let parent_object = list_copy_head(object, list_length(object) - 1);

    if let Some(found) = schema_does_not_exist_skipping(mcx, &parent_object)? {
        return Ok(Some(found));
    }

    // parent_rel = makeRangeVarFromNameList(parent_object);
    let names = namelist_of_nodes(&parent_object)?;
    let parent_rel = makeRangeVarFromNameList(&names)?;

    // if (!OidIsValid(RangeVarGetRelid(parent_rel, NoLock, true)))
    if !OidIsValid(RangeVarGetRelid(mcx, &parent_rel, NoLock, true)?) {
        let msg = "relation \"%s\" does not exist, skipping";
        let name = NameListToString(mcx, &names)?.as_str().to_string();
        return Ok(Some((msg, name)));
    }

    Ok(None)
}

/// `schema_does_not_exist_skipping` — subroutine for [`remove_objects`].
/// dropcmds.c:173-190.
///
/// After determining that a specification for a schema-qualifiable object
/// refers to an object that does not exist, test whether the specified schema
/// exists or not. If no schema was specified, or if the schema does exist,
/// return `Ok(None)` -- the object itself is missing instead. If the specified
/// schema does not exist, return `Ok(Some((msg, schemaname)))`.
fn schema_does_not_exist_skipping<'mcx>(
    // `mcx` is the context the C `makeRangeVarFromNameList` allocates the
    // transient `RangeVar` in; the repo's `makeRangeVarFromNameList` returns
    // an owned `RangeVar`, so no charge crosses here — kept for call-site
    // symmetry with the other probes.
    _mcx: Mcx<'mcx>,
    object: &[Node],
) -> PgResult<Option<(&'static str, String)>> {
    // rel = makeRangeVarFromNameList(object);
    let names = namelist_of_nodes(object)?;
    let rel = makeRangeVarFromNameList(&names)?;

    // if (rel->schemaname != NULL && !OidIsValid(LookupNamespaceNoError(rel->schemaname)))
    if let Some(schemaname) = rel.schemaname {
        if !OidIsValid(LookupNamespaceNoError(&schemaname)?) {
            let msg = "schema \"%s\" does not exist, skipping";
            return Ok(Some((msg, schemaname)));
        }
    }

    Ok(None)
}

/// `type_in_list_does_not_exist_skipping` — subroutine for [`remove_objects`].
/// dropcmds.c:205-232.
///
/// After determining that a specification for a function, cast, aggregate or
/// operator returns that the specified object does not exist, test whether the
/// involved datatypes, and their schemas, exist or not; if they do, return
/// `Ok(None)`. If the datatypes or schemas do not exist, return
/// `Ok(Some((msg, name)))`.
///
/// First parameter is a list of `TypeName` nodes (the C iterates a `List *`,
/// where a cell may be NULL — the `if (typeName != NULL)` guard — for an
/// unspecified-argument cell).
fn type_in_list_does_not_exist_skipping<'mcx>(
    mcx: Mcx<'mcx>,
    typenames: &[Node],
) -> PgResult<Option<(&'static str, String)>> {
    // foreach(l, typenames)
    for type_name in typenames {
        // TypeName *typeName = lfirst_node(TypeName, l);
        // if (typeName != NULL)
        if let Some(type_name) = type_name.as_typename() {
            // if (!OidIsValid(LookupTypeNameOid(NULL, typeName, true)))
            if !OidIsValid(lookup_type_name_oid::call(type_name, true)?) {
                /* type doesn't exist, try to find why */
                // if (schema_does_not_exist_skipping(typeName->names, ...))
                if let Some(found) = schema_does_not_exist_skipping(mcx, &type_name.names)? {
                    return Ok(Some(found));
                }

                let msg = "type \"%s\" does not exist, skipping";
                let name = typename_to_string_node::call(mcx, type_name)?
                    .as_str()
                    .to_string();
                return Ok(Some((msg, name)));
            }
        }
    }

    Ok(None)
}

/// `does_not_exist_skipping` — subroutine for [`remove_objects`].
/// dropcmds.c:242-524.
///
/// Generate a NOTICE stating that the named object was not found, and is being
/// skipped. This is only relevant when "IF EXISTS" is used; otherwise,
/// `get_object_address()` in [`remove_objects`] would have thrown an ERROR.
pub fn does_not_exist_skipping(mcx: Mcx<'_>, objtype: ObjectType, object: &Node) -> PgResult<()> {
    // const char *msg = NULL; char *name = NULL; char *args = NULL;
    let mut msg: Option<&'static str> = None;
    let mut name: Option<String> = None;
    let mut args: Option<String> = None;

    match objtype {
        OBJECT_ACCESS_METHOD => {
            msg = Some("access method \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }
        OBJECT_TYPE | OBJECT_DOMAIN => {
            // TypeName *typ = castNode(TypeName, object);
            let typ = as_typename(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &typ.names)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("type \"%s\" does not exist, skipping");
                name = Some(typename_to_string_node::call(mcx, typ)?.as_str().to_string());
            }
        }
        OBJECT_COLLATION => {
            let list = as_list(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("collation \"%s\" does not exist, skipping");
                name = Some(name_list_to_string(mcx, list)?);
            }
        }
        OBJECT_CONVERSION => {
            let list = as_list(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("conversion \"%s\" does not exist, skipping");
                name = Some(name_list_to_string(mcx, list)?);
            }
        }
        OBJECT_SCHEMA => {
            msg = Some("schema \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }
        OBJECT_STATISTIC_EXT => {
            let list = as_list(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("statistics object \"%s\" does not exist, skipping");
                name = Some(name_list_to_string(mcx, list)?);
            }
        }
        OBJECT_TSPARSER => {
            let list = as_list(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("text search parser \"%s\" does not exist, skipping");
                name = Some(name_list_to_string(mcx, list)?);
            }
        }
        OBJECT_TSDICTIONARY => {
            let list = as_list(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("text search dictionary \"%s\" does not exist, skipping");
                name = Some(name_list_to_string(mcx, list)?);
            }
        }
        OBJECT_TSTEMPLATE => {
            let list = as_list(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("text search template \"%s\" does not exist, skipping");
                name = Some(name_list_to_string(mcx, list)?);
            }
        }
        OBJECT_TSCONFIGURATION => {
            let list = as_list(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("text search configuration \"%s\" does not exist, skipping");
                name = Some(name_list_to_string(mcx, list)?);
            }
        }
        OBJECT_EXTENSION => {
            msg = Some("extension \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }
        OBJECT_FUNCTION => {
            let owa = as_objectwithargs(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &objname_nodes(owa))? {
                msg = Some(m);
                name = Some(n);
            } else if let Some((m, n)) =
                type_in_list_does_not_exist_skipping(mcx, &owa.objargs)?
            {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("function %s(%s) does not exist, skipping");
                name = Some(name_list_to_string(mcx, &objname_nodes(owa))?);
                args = Some(type_name_list_to_string_objargs(mcx, owa)?);
            }
        }
        OBJECT_PROCEDURE => {
            let owa = as_objectwithargs(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &objname_nodes(owa))? {
                msg = Some(m);
                name = Some(n);
            } else if let Some((m, n)) =
                type_in_list_does_not_exist_skipping(mcx, &owa.objargs)?
            {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("procedure %s(%s) does not exist, skipping");
                name = Some(name_list_to_string(mcx, &objname_nodes(owa))?);
                args = Some(type_name_list_to_string_objargs(mcx, owa)?);
            }
        }
        OBJECT_ROUTINE => {
            let owa = as_objectwithargs(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &objname_nodes(owa))? {
                msg = Some(m);
                name = Some(n);
            } else if let Some((m, n)) =
                type_in_list_does_not_exist_skipping(mcx, &owa.objargs)?
            {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("routine %s(%s) does not exist, skipping");
                name = Some(name_list_to_string(mcx, &objname_nodes(owa))?);
                args = Some(type_name_list_to_string_objargs(mcx, owa)?);
            }
        }
        OBJECT_AGGREGATE => {
            let owa = as_objectwithargs(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &objname_nodes(owa))? {
                msg = Some(m);
                name = Some(n);
            } else if let Some((m, n)) =
                type_in_list_does_not_exist_skipping(mcx, &owa.objargs)?
            {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("aggregate %s(%s) does not exist, skipping");
                name = Some(name_list_to_string(mcx, &objname_nodes(owa))?);
                args = Some(type_name_list_to_string_objargs(mcx, owa)?);
            }
        }
        OBJECT_OPERATOR => {
            let owa = as_objectwithargs(object);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &objname_nodes(owa))? {
                msg = Some(m);
                name = Some(n);
            } else if let Some((m, n)) =
                type_in_list_does_not_exist_skipping(mcx, &owa.objargs)?
            {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("operator %s does not exist, skipping");
                name = Some(name_list_to_string(mcx, &objname_nodes(owa))?);
            }
        }
        OBJECT_LANGUAGE => {
            msg = Some("language \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }
        OBJECT_CAST => {
            // list_make1(linitial(castNode(List, object))) /
            // list_make1(lsecond(castNode(List, object)))
            let list = as_list(object);
            let source = linitial(list);
            let target = lsecond(list);
            let first = [source.clone()];
            let second = [target.clone()];
            if let Some((m, n)) = type_in_list_does_not_exist_skipping(mcx, &first)? {
                msg = Some(m);
                name = Some(n);
            } else if let Some((m, n)) = type_in_list_does_not_exist_skipping(mcx, &second)? {
                msg = Some(m);
                name = Some(n);
            } else {
                /* XXX quote or no quote? */
                msg = Some("cast from type %s to type %s does not exist, skipping");
                name = Some(
                    typename_to_string_node::call(mcx, node_as_typename(source))?
                        .as_str()
                        .to_string(),
                );
                args = Some(
                    typename_to_string_node::call(mcx, node_as_typename(target))?
                        .as_str()
                        .to_string(),
                );
            }
        }
        OBJECT_TRANSFORM => {
            // list_make1(linitial(castNode(List, object)))
            let list = as_list(object);
            let type_node = linitial(list);
            let first = [type_node.clone()];
            if let Some((m, n)) = type_in_list_does_not_exist_skipping(mcx, &first)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("transform for type %s language \"%s\" does not exist, skipping");
                name = Some(
                    typename_to_string_node::call(mcx, node_as_typename(type_node))?
                        .as_str()
                        .to_string(),
                );
                args = Some(node_str_val(lsecond(list)));
            }
        }
        OBJECT_TRIGGER => {
            let list = as_list(object);
            if let Some((m, n)) = owningrel_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("trigger \"%s\" for relation \"%s\" does not exist, skipping");
                name = Some(node_str_val(llast(list)));
                let head = list_copy_head(list, list_length(list) - 1);
                args = Some(name_list_to_string(mcx, &head)?);
            }
        }
        OBJECT_POLICY => {
            let list = as_list(object);
            if let Some((m, n)) = owningrel_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("policy \"%s\" for relation \"%s\" does not exist, skipping");
                name = Some(node_str_val(llast(list)));
                let head = list_copy_head(list, list_length(list) - 1);
                args = Some(name_list_to_string(mcx, &head)?);
            }
        }
        OBJECT_EVENT_TRIGGER => {
            msg = Some("event trigger \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }
        OBJECT_RULE => {
            let list = as_list(object);
            if let Some((m, n)) = owningrel_does_not_exist_skipping(mcx, list)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some("rule \"%s\" for relation \"%s\" does not exist, skipping");
                name = Some(node_str_val(llast(list)));
                let head = list_copy_head(list, list_length(list) - 1);
                args = Some(name_list_to_string(mcx, &head)?);
            }
        }
        OBJECT_FDW => {
            msg = Some("foreign-data wrapper \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }
        OBJECT_FOREIGN_SERVER => {
            msg = Some("server \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }
        OBJECT_OPCLASS => {
            // List *opcname = list_copy_tail(castNode(List, object), 1);
            let list = as_list(object);
            let opcname = list_copy_tail(list, 1);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &opcname)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some(
                    "operator class \"%s\" does not exist for access method \"%s\", skipping",
                );
                name = Some(name_list_to_string(mcx, &opcname)?);
                args = Some(node_str_val(linitial(list)));
            }
        }
        OBJECT_OPFAMILY => {
            let list = as_list(object);
            let opfname = list_copy_tail(list, 1);
            if let Some((m, n)) = schema_does_not_exist_skipping(mcx, &opfname)? {
                msg = Some(m);
                name = Some(n);
            } else {
                msg = Some(
                    "operator family \"%s\" does not exist for access method \"%s\", skipping",
                );
                name = Some(name_list_to_string(mcx, &opfname)?);
                args = Some(node_str_val(linitial(list)));
            }
        }
        OBJECT_PUBLICATION => {
            msg = Some("publication \"%s\" does not exist, skipping");
            name = Some(str_val(object));
        }

        OBJECT_COLUMN | OBJECT_DATABASE | OBJECT_FOREIGN_TABLE | OBJECT_INDEX | OBJECT_MATVIEW
        | OBJECT_ROLE | OBJECT_SEQUENCE | OBJECT_SUBSCRIPTION | OBJECT_TABLE
        | OBJECT_TABLESPACE | OBJECT_VIEW => {
            /*
             * These are handled elsewhere, so if someone gets here the code is
             * probably wrong or should be revisited.
             */
            return Err(elog_error(format!(
                "unsupported object type: {}",
                objtype as i32
            )));
        }

        OBJECT_AMOP | OBJECT_AMPROC | OBJECT_ATTRIBUTE | OBJECT_DEFAULT | OBJECT_DEFACL
        | OBJECT_DOMCONSTRAINT | OBJECT_LARGEOBJECT | OBJECT_PARAMETER_ACL
        | OBJECT_PUBLICATION_NAMESPACE | OBJECT_PUBLICATION_REL | OBJECT_TABCONSTRAINT
        | OBJECT_USER_MAPPING => {
            /* These are currently not used or needed. */
            return Err(elog_error(format!(
                "unsupported object type: {}",
                objtype as i32
            )));
        } // no default, to let compiler warn about missing case
    }

    // if (!msg) elog(ERROR, "unrecognized object type: %d", (int) objtype);
    let Some(msg) = msg else {
        return Err(elog_error(format!(
            "unrecognized object type: {}",
            objtype as i32
        )));
    };

    // The C builds the final NOTICE text with errmsg(msg, name[, args]).
    let name = name.unwrap_or_default();
    let text = match &args {
        // ereport(NOTICE, (errmsg(msg, name)));
        None => render_format(msg, &[&name]),
        // ereport(NOTICE, (errmsg(msg, name, args)));
        Some(args) => render_format(msg, &[&name, args]),
    };

    ereport(NOTICE)
        .errmsg(text)
        .finish(here(521, "does_not_exist_skipping"))
}

// ---------------------------------------------------------------------------
// Polymorphic-node accessors (the idiomatic equivalents of the C `castNode` /
// `strVal` / `linitial` reads against `Node *object`). Each asserts the
// variant the matching `ObjectType` branch guarantees, exactly as `castNode`
// reinterprets the node without a runtime tag check in a non-assert build.
// ---------------------------------------------------------------------------

/// `strVal(object)` — the string value of a `String` value node.
fn str_val(object: &Node) -> String {
    node_str_val(object)
}

/// `strVal(node)` for a `String` value node.
fn node_str_val(node: &Node) -> String {
    match node.as_string() {
        Some(StringNode { sval }) => sval.clone().unwrap_or_default(),
        None => unreachable!("Node::String expected for strVal()"),
    }
}

/// `castNode(TypeName, object)`.
fn as_typename(object: &Node) -> &TypeName {
    node_as_typename(object)
}

/// `castNode(TypeName, node)`.
fn node_as_typename(node: &Node) -> &TypeName {
    node.as_typename().expect("Node::TypeName expected")
}

/// `castNode(List, object)` — the cells of a `List` value node.
fn as_list(object: &Node) -> &[Node] {
    object.as_list().expect("Node::List expected")
}

/// `castNode(ObjectWithArgs, object)`.
fn as_objectwithargs(object: &Node) -> &ObjectWithArgs {
    object
        .as_objectwithargs()
        .expect("Node::ObjectWithArgs expected")
}

// ---------------------------------------------------------------------------
// List helpers (pg_list.h `list_length` / `list_copy_head` / `list_copy_tail`
// / `linitial` / `lsecond` / `llast`).
// ---------------------------------------------------------------------------

/// `list_length(list)` (`pg_list.h`).
fn list_length(list: &[Node]) -> i32 {
    list.len() as i32
}

/// `list_copy_head(list, len)` (nodes/list.c) — the first `len` elements.
fn list_copy_head(list: &[Node], len: i32) -> Vec<Node> {
    let len = len.max(0) as usize;
    list[..len.min(list.len())].to_vec()
}

/// `list_copy_tail(list, nskip)` (nodes/list.c) — all but the first `nskip`.
fn list_copy_tail(list: &[Node], nskip: i32) -> Vec<Node> {
    let nskip = nskip.max(0) as usize;
    list[nskip.min(list.len())..].to_vec()
}

/// `linitial(list)` — the first element.
fn linitial(list: &[Node]) -> &Node {
    &list[0]
}

/// `lsecond(list)` — the second element.
fn lsecond(list: &[Node]) -> &Node {
    &list[1]
}

/// `llast(list)` — the last element.
fn llast(list: &[Node]) -> &Node {
    &list[list.len() - 1]
}

// ---------------------------------------------------------------------------
// Name-list / type-name rendering adapters.
// ---------------------------------------------------------------------------

/// Project a `List`'s `String` cells to a `NameList` (`&[Option<String>]`)
/// for `makeRangeVarFromNameList` / `RangeVarGetRelid`.
fn namelist_of_nodes(cells: &[Node]) -> PgResult<Vec<Option<String>>> {
    Ok(cells
        .iter()
        .map(|cell| match cell.as_string() {
            Some(StringNode { sval }) => sval.clone(),
            None => unreachable!("Node::String expected in name list"),
        })
        .collect())
}

/// `NameListToString(list)` over a `List` of `String` nodes, returning an
/// owned `String` for the NOTICE text.
fn name_list_to_string(mcx: Mcx<'_>, cells: &[Node]) -> PgResult<String> {
    let names = namelist_of_nodes(cells)?;
    Ok(NameListToString(mcx, &names)?.as_str().to_string())
}

/// The `objname` of an `ObjectWithArgs` as a `NameList` (`&[Option<String>]`).
/// `objname` is already a `Vec<String>`.
fn objname_namelist(owa: &ObjectWithArgs) -> Vec<Option<String>> {
    owa.objname.iter().map(|s| Some(s.clone())).collect()
}

/// The `objname` of an `ObjectWithArgs` reconstructed as a `List` of `String`
/// nodes (the C `owa->objname` is a `List *` of `String`), for the
/// schema-probe and `NameListToString` helpers that take `&[Node]`.
fn objname_nodes(owa: &ObjectWithArgs) -> Vec<Node> {
    owa.objname
        .iter()
        .map(|s| {
            Node::String(StringNode {
                sval: Some(s.clone()),
            })
        })
        .collect()
}

/// `TypeNameListToString(owa->objargs)` — render the argument-type list. The
/// seam takes a slice of raw-parser `TypeName`; project the `objargs` `List`
/// of `TypeName` nodes to that slice (cells are `TypeName` nodes; a missing
/// `TypeName` cell renders as the default `TypeName`, matching the C list).
fn type_name_list_to_string_objargs(mcx: Mcx<'_>, owa: &ObjectWithArgs) -> PgResult<String> {
    let typenames: Vec<TypeName> = owa
        .objargs
        .iter()
        .map(|n| n.as_typename().cloned().unwrap_or_default())
        .collect();
    Ok(type_name_list_to_string::call(mcx, &typenames)?
        .as_str()
        .to_string())
}

// ---------------------------------------------------------------------------
// printf rendering — the fixed `%s` / `%%` formats dropcmds.c uses.
// ---------------------------------------------------------------------------

/// Minimal printf renderer for the fixed `%s` / `%%` formats dropcmds.c uses:
/// substitute each `%s` in order with the next argument, and `%%` with `%`.
/// (All dropcmds.c format strings contain only `%s` conversions.) The C
/// `errmsg(msg, …)` formats into the error builder's `StringInfo`; here the
/// builder takes the already-rendered owned `String`.
fn render_format(fmt: &str, subs: &[&str]) -> String {
    let mut out = String::with_capacity(fmt.len());
    let mut chars = fmt.chars().peekable();
    let mut next = subs.iter();
    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.peek() {
                Some('s') => {
                    chars.next();
                    if let Some(s) = next.next() {
                        out.push_str(s);
                    }
                }
                Some('%') => {
                    chars.next();
                    out.push('%');
                }
                _ => out.push('%'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests;
