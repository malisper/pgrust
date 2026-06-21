//! `DefineRelation` (tablecmds.c:764), `BuildDescForRelation` (1380),
//! `StoreCatalogInheritance` (3521), `findAttrByName` (3609), `storage_name`
//! (2460) and the helpers they use.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use backend_utils_error::ereport;
use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgVec};

use types_acl::{ACLCHECK_OK, ACL_CREATE, ACL_USAGE};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_core::AttrNumber;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_nodes::ddlnodes::{ConstrType, CreateStmt};
use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::primnodes::OnCommitAction;
use types_nodes::rawnodes::{ColumnDef, RangeVar, TypeName};
use types_tuple::access::{
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
use types_tuple::heaptuple::TupleDescData;

use types_catalog::pg_attribute::{AttributeRelationId, PgAttributeUpdateRow};
use types_rel::Relation;
use types_storage::lock::RowExclusiveLock;

use types_tuple::heaptuple::ATTNULLABLE_VALID;

use backend_access_common_relation::relation_open;
use backend_catalog_indexing_seams as indexing_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_cache_syscache::SearchSysCacheAttNum;
use backend_access_common_tupdesc::{
    populate_compact_attribute, CreateTemplateTupleDesc, TupleDescInitEntry,
    TupleDescInitEntryCollation,
};
use backend_access_transam_xact::CommandCounterIncrement;
use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_heap_seams::{heap_create_with_catalog, HeapCreateWithCatalogArgs};
use backend_catalog_namespace::{RangeVarGetAndCheckCreationNamespace, RangeVarGetRelid};
use backend_commands_tablespace_globals_seams as ts_globals_seam;
use backend_commands_tablespace_seams as ts_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{
    here, object_address_set, strlcpy_namedatalen, to_access_range_var, GLOBALTABLESPACE_OID,
    PG_INT16_MAX, RelationRelationId, TableSpaceRelationId, TypeRelationId,
};

/// `castNode(RangeVar, node)` for an owned `Node::RangeVar`.
fn as_range_var<'a, 'mcx>(node: &'a Node<'mcx>) -> &'a RangeVar<'mcx> {
    match node.as_rangevar() {
        Some(rv) => rv,
        None => unreachable!("Node::RangeVar expected"),
    }
}

/// `castNode(ColumnDef, node)` for an owned `Node::ColumnDef`.
fn as_column_def<'a, 'mcx>(node: &'a Node<'mcx>) -> &'a ColumnDef<'mcx> {
    match node.as_columndef() {
        Some(cd) => cd,
        None => unreachable!("Node::ColumnDef expected"),
    }
}

/// `castNode(TypeName, node)`.
fn as_typename<'a, 'mcx>(node: &'a Node<'mcx>) -> &'a TypeName<'mcx> {
    match node.as_typename() {
        Some(tn) => tn,
        None => unreachable!("Node::TypeName expected"),
    }
}

/// `HEAP_RELOPT_NAMESPACES` (access/reloptions.h) — `{ "toast", NULL }`. The
/// namespaces `transformRelOptions` accepts for a heap relation's `WITH (...)`.
const HEAP_RELOPT_NAMESPACES: &[&str] = &["toast"];

/// `def->arg` (a `nodes/value.h` value node) projected to the
/// `define.c` `DefElemArg` the reloptions `defGetString`/`defGetBoolean`
/// dispatch on — the same projection every DDL caller uses (cf.
/// `backend-commands-vacuum::defel_arg`). `None` mirrors `def->arg == NULL`.
pub(crate) fn defel_arg(
    def: &types_nodes::ddlnodes::DefElem<'_>,
) -> PgResult<Option<backend_commands_define_seams::DefElemArg>> {
    use backend_commands_define_seams::DefElemArg;
    let Some(node) = def.arg.as_deref() else {
        return Ok(None);
    };
    // Mirror `defGetString`'s node switch (define.c). A bare-identifier reloption
    // value such as `autovacuum_enabled = off` arrives from the grammar's
    // `def_arg: func_type` production as a `T_TypeName` (and a qualified name as
    // a `T_List`); both render to their textual form. The prior `_ => AStar`
    // catch-all collapsed those to `"*"`, so `WITH (autovacuum_enabled = off)`
    // failed with `invalid value for boolean option "...": *`.
    Ok(Some(match node.node_tag() {
        ntag::T_Integer => DefElemArg::Integer(node.expect_integer().ival as i64),
        ntag::T_Float => DefElemArg::Float(node.expect_float().fval.as_str().to_string()),
        ntag::T_Boolean => DefElemArg::Boolean(node.expect_boolean().boolval),
        ntag::T_String => DefElemArg::String(node.expect_string().sval.as_str().to_string()),
        // case T_TypeName: return TypeNameToString((TypeName *) def->arg);
        ntag::T_TypeName => DefElemArg::TypeName(type_name_to_string(node.expect_typename())?),
        // case T_List: return NameListToString((List *) def->arg);
        ntag::T_List => DefElemArg::List(name_list_to_string(node.expect_list())?),
        // case T_A_Star: return pstrdup("*");
        ntag::T_A_Star => DefElemArg::AStar,
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized node type: {}", other))
                .into_error())
        }
    }))
}

/// `TypeNameToString(typeName)` for the `defGetString` `T_TypeName` case
/// (parse_type.c). A reloptions `def->arg` `TypeName` is always a parsed
/// identifier carrying `names` (never an internal `typeOid`-only node), so the
/// `format_type_be` fallback is unreachable here.
fn type_name_to_string(tn: &TypeName<'_>) -> PgResult<String> {
    if tn.names.is_empty() {
        return Err(ereport(ERROR)
            .errmsg_internal(
                "reloption TypeName carries no name (internal typeOid-only form unsupported here)",
            )
            .into_error());
    }
    let mut out = String::new();
    for (i, name) in tn.names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    if tn.pct_type {
        out.push_str("%TYPE");
    }
    if !tn.arrayBounds.is_empty() {
        out.push_str("[]");
    }
    Ok(out)
}

/// `NameListToString(names)` for the `defGetString` `T_List` case (namespace.c).
fn name_list_to_string(names: &[NodePtr<'_>]) -> PgResult<String> {
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            ntag::T_A_Star => out.push('*'),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    Ok(out)
}

/// `transformRelOptions((Datum) 0, stmt->options, NULL, validnsps, true, false)`
/// then the per-relkind `view_reloptions` / `partitioned_table_reloptions` /
/// `heap_reloptions` validation (the `DefineRelation` reloptions block,
/// tablecmds.c:930-946). Returns the constructed `text[]` reloptions image as a
/// [`RelOptionsToken`](types_cluster::RelOptionsToken) — null (`is_null`) when
/// there were no options (the no-`WITH` case, `(Datum) 0`), else the array
/// varlena bytes the catalog owner stores in `pg_class.reloptions`.
///
/// This is the create-time specialisation of `transformRelOptions` where
/// `oldOptions` is always `(Datum) 0`, so the "copy unreplaced old options"
/// branch is dead and only the flatten-`defList` half runs. We build the
/// `name=value` element strings (skipping `WITH (oids=false)`, validating
/// namespaces against `HEAP_RELOPT_NAMESPACES`, rejecting `name` containing
/// `=`), assemble the on-disk `text[]` varlena via `build_text_array_nullable`,
/// then validate it by relkind exactly as C's switch does. `Err` carries the
/// transform/validation `ereport(ERROR)` surface.
pub(crate) fn transform_and_check_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: &[NodePtr<'mcx>],
    relkind: u8,
) -> PgResult<types_cluster::RelOptionsToken> {
    // transformRelOptions: build the new text[] from defList (oldOptions is
    // (Datum) 0 here, so there are no old options to copy).
    let mut astate: Vec<String> = Vec::new();

    for opt in options {
        let Some(def) = opt.as_defelem() else {
            // CreateStmt.options is a List of DefElem (gram.y); a non-DefElem
            // would be a parser invariant violation.
            unreachable!("CreateStmt.options element is not a DefElem");
        };
        let defname = def.defname.as_deref().unwrap_or("");

        // Error out if the namespace is not valid. A NULL namespace is always
        // valid. (validnsps = HEAP_RELOPT_NAMESPACES.)
        if let Some(defns) = def.defnamespace.as_deref() {
            let valid = HEAP_RELOPT_NAMESPACES.iter().any(|ns| *ns == defns);
            if !valid {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("unrecognized parameter namespace \"{defns}\""))
                    .finish(here("transformRelOptions"))
                    .map(|()| unreachable!());
            }
        }

        // ignore if not in the same namespace (namspace == NULL here, so skip
        // any qualified option — it belongs to the "toast" pass, not this one).
        if def.defnamespace.is_some() {
            continue;
        }

        // Flatten the DefElem into "name=value"; bare "name" means "name=true".
        let value: String = if def.arg.is_some() {
            backend_commands_define_seams::def_get_string::call(
                mcx,
                defname.to_string(),
                defel_arg(def)?,
            )?
            .as_str()
            .to_string()
        } else {
            "true".to_string()
        };

        // Insist that name not contain "=", else "a=b=c" is ambiguous.
        if defname.contains('=') {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "invalid option name \"{defname}\": must not contain \"=\""
                ))
                .finish(here("transformRelOptions"))
                .map(|()| unreachable!());
        }

        // acceptOidsOff: filter out WITH (oids=false); error on oids=true.
        if def.defnamespace.is_none() && defname == "oids" {
            if backend_commands_define_seams::def_get_boolean::call(
                defname.to_string(),
                defel_arg(def)?,
            )? {
                return ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("tables declared WITH OIDS are not supported")
                    .finish(here("transformRelOptions"))
                    .map(|()| unreachable!());
            }
            // skip over option, reloptions machinery doesn't know it
            continue;
        }

        astate.push(format!("{defname}={value}"));
    }

    // makeArrayResult / (Datum) 0: no elements means a NULL reloptions token.
    if astate.is_empty() {
        // Validate the empty set (heap/view/partitioned_table_reloptions with a
        // NULL `reloptions` Datum) — a no-op that just confirms emptiness is
        // acceptable for the relkind.
        validate_reloptions(mcx, relkind, None)?;
        return Ok(types_cluster::RelOptionsToken {
            is_null: true,
            bytes: Vec::new(),
        });
    }

    // Assemble the on-disk text[] varlena image (the C makeArrayResult).
    let elems: Vec<Option<&[u8]>> = astate.iter().map(|s| Some(s.as_bytes())).collect();
    let bytes: Vec<u8> =
        backend_utils_adt_arrayfuncs_seams::build_text_array_nullable::call(mcx, &elems)?
            .iter()
            .copied()
            .collect();

    // Validate by relkind (the C switch on relkind, validate=true).
    validate_reloptions(mcx, relkind, Some(&bytes))?;

    Ok(types_cluster::RelOptionsToken {
        is_null: false,
        bytes,
    })
}

/// The `switch (relkind)` validate block of the DefineRelation reloptions
/// handling (tablecmds.c:936-946): `view_reloptions` for views,
/// `partitioned_table_reloptions` for partitioned tables, else
/// `heap_reloptions`. All run with `validate = true`; the parsed struct is
/// discarded (C `(void) ...`), only the `ereport(ERROR)` matters.
fn validate_reloptions(mcx: Mcx<'_>, relkind: u8, reloptions: Option<&[u8]>) -> PgResult<()> {
    if relkind == RELKIND_VIEW {
        backend_access_common_reloptions::view_reloptions(mcx, reloptions, true)?;
    } else if relkind == RELKIND_PARTITIONED_TABLE {
        backend_access_common_reloptions::partitioned_table_reloptions(reloptions, true)?;
    } else {
        backend_access_common_reloptions::heap_reloptions(mcx, relkind, reloptions, true)?;
    }
    Ok(())
}

/// The CREATE TABLE TOAST-table follow-on (utility.c:1170-1190):
///
/// ```c
/// toast_options = transformRelOptions((Datum) 0, cstmt->options, "toast",
///                                     validnsps, true, false);
/// (void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
/// NewRelationCreateToastTable(address.objectId, toast_options);
/// ```
///
/// `transformRelOptions` is called here with `namspace = "toast"`, so only the
/// `WITH (toast.*)` qualified options participate (the unqualified heap options
/// were already consumed by the table's own `transform_and_check_reloptions`).
/// The toast namespace is stripped from each surviving element. The validated
/// `text[]` image is the `reloptions` Datum forwarded into
/// `NewRelationCreateToastTable`, which (via `CheckAndCreateToastTable` →
/// `create_toast_table` → `needs_toast_table`) decides whether the relation
/// actually needs a TOAST table and creates it if so. For a relation whose
/// columns are all fixed-width / non-toastable (e.g. `t(f1 int)`) this is the
/// early-return no-op and CREATE TABLE completes here.
pub fn create_toast_for_relation<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    options: &PgVec<'mcx, NodePtr<'mcx>>,
) -> PgResult<()> {
    // transformRelOptions((Datum) 0, options, "toast", validnsps, true, false):
    // build the toast text[] from the `toast.*`-qualified DefElems, stripping
    // the namespace. oldOptions is (Datum) 0 so there are no old options to
    // copy; only the flatten-defList half runs.
    let mut astate: Vec<String> = Vec::new();

    for opt in options.iter() {
        let Some(def) = opt.as_defelem() else {
            unreachable!("CreateStmt.options element is not a DefElem");
        };
        let defname = def.defname.as_deref().unwrap_or("");

        // Error out if the namespace is not valid. A NULL namespace is always
        // valid. (validnsps = HEAP_RELOPT_NAMESPACES.)
        if let Some(defns) = def.defnamespace.as_deref() {
            let valid = HEAP_RELOPT_NAMESPACES.iter().any(|ns| *ns == defns);
            if !valid {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("unrecognized parameter namespace \"{defns}\""))
                    .finish(here("transformRelOptions"))
                    .map(|()| unreachable!());
            }
        }

        // ignore if not in the same namespace (namspace == "toast" here, so
        // skip any option that is NOT in the toast namespace).
        match def.defnamespace.as_deref() {
            Some("toast") => {}
            _ => continue,
        }

        // Flatten the DefElem into "name=value"; bare "name" means "name=true".
        let value: String = if def.arg.is_some() {
            backend_commands_define_seams::def_get_string::call(
                mcx,
                defname.to_string(),
                defel_arg(def)?,
            )?
            .as_str()
            .to_string()
        } else {
            "true".to_string()
        };

        // Insist that name not contain "=", else "a=b=c" is ambiguous.
        if defname.contains('=') {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "invalid option name \"{defname}\": must not contain \"=\""
                ))
                .finish(here("transformRelOptions"))
                .map(|()| unreachable!());
        }

        astate.push(format!("{defname}={value}"));
    }

    // makeArrayResult / (Datum) 0: no elements means a NULL reloptions token.
    let toast_options = if astate.is_empty() {
        // (void) heap_reloptions(RELKIND_TOASTVALUE, (Datum) 0, true): validate
        // the empty set (a no-op confirming emptiness is acceptable).
        backend_access_common_reloptions::heap_reloptions(
            mcx,
            types_tuple::access::RELKIND_TOASTVALUE,
            None,
            true,
        )?;
        types_cluster::RelOptionsToken {
            is_null: true,
            bytes: Vec::new(),
        }
    } else {
        // Assemble the on-disk text[] varlena image (the C makeArrayResult).
        let elems: Vec<Option<&[u8]>> = astate.iter().map(|s| Some(s.as_bytes())).collect();
        let bytes: Vec<u8> =
            backend_utils_adt_arrayfuncs_seams::build_text_array_nullable::call(mcx, &elems)?
                .iter()
                .copied()
                .collect();

        // (void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true).
        backend_access_common_reloptions::heap_reloptions(
            mcx,
            types_tuple::access::RELKIND_TOASTVALUE,
            Some(&bytes),
            true,
        )?;

        types_cluster::RelOptionsToken {
            is_null: false,
            bytes,
        }
    };

    // NewRelationCreateToastTable(relid, toast_options).
    backend_catalog_toasting::NewRelationCreateToastTable(mcx, relid, toast_options)
}

/// `DefineRelation(stmt, relkind, ownerId, typaddress=NULL, queryString)`
/// (tablecmds.c:764). The CREATE TABLE / relation driver.
pub fn define_relation<'mcx>(
    mcx: Mcx<'mcx>,
    mut stmt: CreateStmt<'mcx>,
    mut relkind: u8,
    mut owner_id: Oid,
    query_string: Option<&str>,
) -> PgResult<ObjectAddress> {
    /*
     * Truncate relname to appropriate length (probably a waste of time, as
     * parser should have done this already).
     */
    let stmt_relation = stmt
        .relation
        .as_deref()
        .expect("CreateStmt.relation is NOT NULL");
    let relname =
        strlcpy_namedatalen(as_range_var(stmt_relation).relname.as_ref().map(|s| s.as_str()).unwrap_or(""));
    let relpersistence = as_range_var(stmt_relation).relpersistence as u8;

    /*
     * Check consistency of arguments
     */
    if stmt.oncommit != OnCommitAction::ONCOMMIT_NOOP && relpersistence != RELPERSISTENCE_TEMP {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("ON COMMIT can only be used on temporary tables")
            .finish(here("DefineRelation"))
            .map(|()| object_address_set(InvalidOid, InvalidOid));
    }

    let partitioned;
    if stmt.partspec.is_some() {
        if relkind != RELKIND_RELATION {
            return ereport(ERROR)
                .errmsg_internal(format!("unexpected relkind: {}", relkind as i32))
                .finish(here("DefineRelation"))
                .map(|()| object_address_set(InvalidOid, InvalidOid));
        }
        relkind = RELKIND_PARTITIONED_TABLE;
        partitioned = true;
    } else {
        partitioned = false;
    }

    if relkind == RELKIND_PARTITIONED_TABLE && relpersistence == RELPERSISTENCE_UNLOGGED {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("partitioned tables cannot be unlogged")
            .finish(here("DefineRelation"))
            .map(|()| object_address_set(InvalidOid, InvalidOid));
    }

    /*
     * Look up the namespace in which we are supposed to create the relation,
     * check permissions, lock it, and mark stmt->relation as
     * RELPERSISTENCE_TEMP if a temporary namespace is selected.
     */
    let mut access_rv = to_access_range_var(as_range_var(stmt_relation));
    let namespace_id = RangeVarGetAndCheckCreationNamespace(
        mcx,
        &mut access_rv,
        types_storage::lock::NoLock,
        None,
    )?;
    /* propagate the (possibly temp-promoted) persistence back to the node */
    let relpersistence = access_rv.relpersistence;
    if let Some(rv) = stmt.relation.as_deref_mut().and_then(|n| n.as_rangevar_mut()) {
        rv.relpersistence = relpersistence as i8;
    }

    /*
     * Security check: disallow creating temp tables from security-restricted
     * code.
     */
    if relpersistence == RELPERSISTENCE_TEMP && miscinit_seam::in_security_restricted_operation::call()
    {
        return ereport(ERROR)
            .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("cannot create temporary table within security-restricted operation")
            .finish(here("DefineRelation"))
            .map(|()| object_address_set(InvalidOid, InvalidOid));
    }

    /*
     * Determine the lockmode to use when scanning parents.
     */
    let parent_lockmode = if stmt.partbound.is_some() {
        types_storage::lock::AccessExclusiveLock
    } else {
        types_storage::lock::ShareUpdateExclusiveLock
    };

    /* Determine the list of OIDs of the parents. */
    let mut inherit_oids: Vec<Oid> = Vec::new();
    for rv_node in stmt.inhRelations.iter() {
        let rv = as_range_var(rv_node);
        let access_parent = to_access_range_var(rv);
        let parent_oid = RangeVarGetRelid(mcx, &access_parent, parent_lockmode, false)?;

        /* Reject duplications in the list of parents. */
        if inherit_oids.contains(&parent_oid) {
            let pname = lsyscache_seam::get_rel_name::call(mcx, parent_oid)?;
            return ereport(ERROR)
                .errcode(types_error::ERRCODE_DUPLICATE_TABLE)
                .errmsg(format!(
                    "relation \"{}\" would be inherited from more than once",
                    pname.as_ref().map(|s| s.as_str()).unwrap_or("")
                ))
                .finish(here("DefineRelation"))
                .map(|()| object_address_set(InvalidOid, InvalidOid));
        }
        inherit_oids.push(parent_oid);
    }

    /*
     * Select tablespace to use.
     */
    let mut tablespace_id: Oid;
    if let Some(tablespacename) = stmt.tablespacename.as_ref().map(|s| s.as_str()) {
        tablespace_id = ts_seam::get_tablespace_oid::call(tablespacename, false)?;

        if partitioned && tablespace_id == ts_globals_seam::MyDatabaseTableSpace::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot specify default tablespace for partitioned relations")
                .finish(here("DefineRelation"))
                .map(|()| object_address_set(InvalidOid, InvalidOid));
        }
    } else if stmt.partbound.is_some() {
        debug_assert_eq!(inherit_oids.len(), 1);
        tablespace_id = lsyscache_seam::get_rel_tablespace::call(inherit_oids[0])?;
    } else {
        tablespace_id = InvalidOid;
    }

    /* still nothing? use the default */
    if !OidIsValid(tablespace_id) {
        tablespace_id = seam::get_default_tablespace::call(relpersistence, partitioned)?;
    }

    /* Check permissions except when using database's default */
    if OidIsValid(tablespace_id)
        && tablespace_id != ts_globals_seam::MyDatabaseTableSpace::call()?
    {
        let aclresult = aclchk_seam::object_aclcheck::call(
            TableSpaceRelationId,
            tablespace_id,
            miscinit_seam::get_user_id::call(),
            ACL_CREATE,
        )?;
        if aclresult != ACLCHECK_OK {
            let tsname = ts_seam::get_tablespace_name::call(mcx, tablespace_id)?;
            aclchk_seam::aclcheck_error::call(
                aclresult,
                types_nodes::parsenodes::OBJECT_TABLESPACE,
                tsname.as_ref().map(|s| s.as_str().to_string()),
            )?;
        }
    }

    /* In all cases disallow placing user relations in pg_global */
    if tablespace_id == GLOBALTABLESPACE_OID {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("only shared relations can be placed in pg_global tablespace")
            .finish(here("DefineRelation"))
            .map(|()| object_address_set(InvalidOid, InvalidOid));
    }

    /* Identify user ID that will own the table */
    if !OidIsValid(owner_id) {
        owner_id = miscinit_seam::get_user_id::call();
    }

    /*
     * Parse and validate reloptions, if any.
     */
    let reloptions = seam::transform_and_check_reloptions::call(mcx, &stmt.options, relkind)?;

    /*
     * Resolve OF typename, if any.
     */
    let of_type_id;
    if let Some(of_typename) = stmt.ofTypename.as_deref() {
        of_type_id = seam::typename_type_id::call(mcx, as_typename(of_typename))?;

        let aclresult = aclchk_seam::object_aclcheck::call(
            TypeRelationId,
            of_type_id,
            miscinit_seam::get_user_id::call(),
            ACL_USAGE,
        )?;
        if aclresult != ACLCHECK_OK {
            aclchk_seam::aclcheck_error_type::call(aclresult, of_type_id)?;
        }
    } else {
        of_type_id = InvalidOid;
    }

    /*
     * Look up inheritance ancestors and generate relation schema, including
     * inherited attributes.  (stmt->tableElts is destructively modified by
     * MergeAttributes.)
     */
    let table_elts: PgVec<ColumnDef> = nodes_to_columndefs(mcx, &stmt.tableElts)?;
    let merged = seam::merge_attributes::call(
        mcx,
        table_elts,
        &inherit_oids,
        relpersistence,
        stmt.partbound.is_some(),
    )?;
    let table_elts = merged.columns;
    let old_constraints = merged.old_constraints;
    let old_notnulls = merged.old_notnulls;

    /*
     * Create a tuple descriptor from the relation schema.
     */
    let descriptor = build_desc_for_relation(mcx, &table_elts)?;

    /*
     * Find columns with default values and prepare for insertion of the
     * defaults.
     */
    let mut raw_defaults: Vec<(AttrNumber, NodePtr<'mcx>, i8)> = Vec::new();
    let mut cooked_defaults: Vec<NodePtr<'mcx>> = Vec::new();
    let mut attnum: AttrNumber = 0;

    for col_def in table_elts.iter() {
        attnum += 1;
        if let Some(raw_default) = col_def.raw_default.as_ref() {
            debug_assert!(col_def.cooked_default.is_none());
            let raw = alloc_in(mcx, (**raw_default).clone_in(mcx)?)?;
            raw_defaults.push((attnum, raw, col_def.generated));
        } else if let Some(cooked_default) = col_def.cooked_default.as_ref() {
            /*
             * Build a CookedConstraint node for the inherited/cooked default.
             * The owned model carries it as a `Node` for
             * heap_create_with_catalog's cooked_constraints list.
             */
            let cooked = alloc_in(mcx, (**cooked_default).clone_in(mcx)?)?;
            cooked_defaults.push(make_cooked_default(mcx, attnum, cooked)?);
        }
    }

    /*
     * Select access method to use.
     */
    let mut access_method_id = InvalidOid;
    if let Some(access_method) = stmt.accessMethod.as_ref().map(|s| s.as_str()) {
        debug_assert!(RELKIND_HAS_TABLE_AM(relkind) || relkind == RELKIND_PARTITIONED_TABLE);
        access_method_id = seam::get_table_am_oid::call(access_method, false)?;
    } else if RELKIND_HAS_TABLE_AM(relkind) || relkind == RELKIND_PARTITIONED_TABLE {
        if stmt.partbound.is_some() {
            debug_assert_eq!(inherit_oids.len(), 1);
            access_method_id = lsyscache_seam::get_rel_relam::call(inherit_oids[0])?;
        }

        if RELKIND_HAS_TABLE_AM(relkind) && !OidIsValid(access_method_id) {
            let default_am = seam::default_table_access_method::call(mcx)?;
            access_method_id = seam::get_table_am_oid::call(default_am.as_str(), false)?;
        }
    }

    /*
     * Create the relation. Inherited defaults and CHECK constraints are passed
     * in for immediate handling (C `list_concat(cookedDefaults, old_constraints)`),
     * stored by heap_create_with_catalog -> StoreConstraints.
     */
    let cooked_constraints = list_concat(cooked_defaults, old_constraints);
    let mut cooked_vec: PgVec<NodePtr> = vec_with_capacity_in(mcx, cooked_constraints.len())?;
    for c in cooked_constraints.into_iter() {
        cooked_vec.push(c);
    }
    let relation_id = heap_create_with_catalog::call(HeapCreateWithCatalogArgs {
        relname: relname.clone(),
        relnamespace: namespace_id,
        reltablespace: tablespace_id,
        relid: InvalidOid,
        reltypeid: InvalidOid,
        reloftypeid: of_type_id,
        ownerid: owner_id,
        accessmtd: access_method_id,
        tupdesc: descriptor,
        relkind,
        relpersistence,
        shared_relation: false,
        mapped_relation: false,
        oncommit: stmt.oncommit,
        reloptions,
        use_user_acl: true,
        allow_system_table_mods: ts_globals_seam::allowSystemTableMods::call()?,
        is_internal: false,
        relrewrite: InvalidOid,
        cooked_constraints: cooked_vec,
    })?;

    /*
     * Bump the command counter to make the newly-created relation tuple
     * visible for opening.
     */
    CommandCounterIncrement()?;

    /*
     * Open the new relation and acquire exclusive lock on it.
     */
    let rel = relation_open(mcx, relation_id, types_storage::lock::AccessExclusiveLock)?;

    /*
     * Now add any newly specified column default and generation expressions to
     * the new relation.
     */
    if !raw_defaults.is_empty() {
        seam::add_relation_new_constraints::call(
            mcx,
            &rel,
            &raw_defaults,
            &[],
            true,
            true,
            false,
            query_string,
        )?;
    }

    /* Make column generation expressions visible for use by partitioning. */
    CommandCounterIncrement()?;

    /* Process and store partition bound, if any. */
    if let Some(partbound) = stmt.partbound.as_deref() {
        /*
         * The partition-bound block (DefineRelation, tablecmds.c:1114-1201):
         * open + validate the parent, transformPartitionBound,
         * check_new_partition_bound, check_default_partition_contents, and
         * StorePartitionBound. `parentId = linitial_oid(inheritOids)`.
         */
        let parent_oid = inherit_oids[0];
        crate::partbound::define_relation_partbound(
            mcx,
            &rel,
            parent_oid,
            &relname,
            partbound,
            query_string,
        )?;
    }

    /* Store inheritance information for new rel. */
    store_catalog_inheritance(mcx, relation_id, &inherit_oids, stmt.partbound.is_some())?;

    /*
     * Process the partitioning specification (if any) and store the partition
     * key information into the catalog.
     */
    if partitioned {
        let partspec_node = stmt
            .partspec
            .as_deref()
            .expect("partitioned implies CreateStmt.partspec is set");
        let partspec = match partspec_node.as_partitionspec() {
            Some(ps) => ps,
            None => unreachable!(
                "CreateStmt.partspec is not a PartitionSpec: {}",
                partspec_node.node_tag()
            ),
        };
        crate::partition::define_relation_partspec(mcx, &rel, partspec, query_string)?;

        /* make it all visible */
        CommandCounterIncrement()?;
    }

    /*
     * If we're creating a partition, create now all the indexes, triggers, FKs
     * defined in the parent.
     */
    if stmt.partbound.is_some() {
        seam::define_relation_clone_partition_objects::call(mcx, relation_id, &inherit_oids)?;
    }

    /*
     * In C, `rel` is a long-lived Relation pointer whose `rd_rel`/`rd_att` are
     * rebuilt in place by relcache invalidation as the catalog is mutated
     * (StoreConstraints inside heap_create_with_catalog already stored the
     * inherited/LIKE cooked CHECK constraints and bumped relchecks; a partbound
     * flips `relispartition` to true).  Our owned `rel` was snapshotted by
     * relation_open() before any of those ran, so it still carries the
     * pre-catalog-write image.  AddRelationNewConstraints below reads
     * `rel->rd_att->constr->num_check` (numoldchecks) to compute the new
     * relchecks total, and `rel->rd_rel->relispartition` to decide whether a
     * merged CHECK constraint is non-local; with a stale image numoldchecks
     * would be 0 (clobbering the inherited count) and a partition constraint
     * would wrongly stay local.  Refresh the relation here (we already hold
     * AccessExclusiveLock) so the constraint merge sees the post-write catalog,
     * matching C's in-place rebuild.
     */
    let rel = {
        rel.close(types_storage::lock::NoLock)?;
        relation_open(mcx, relation_id, types_storage::lock::NoLock)?
    };

    /*
     * Now add any newly specified CHECK constraints to the new relation.
     */
    let mut connames: Vec<String> = Vec::new();
    if !stmt.constraints.is_empty() {
        let conlist =
            seam::add_relation_new_constraints::call(mcx, &rel, &[], &stmt.constraints, true, true, false, query_string)?;
        for cons in conlist.iter() {
            if let Some(name) = cooked_constraint_name(cons) {
                connames.push(name);
            }
        }
    }

    /*
     * Finally, merge the not-null constraints, create them, and set the
     * attnotnull flag on columns that don't yet have it.
     */
    let nncols = seam::add_relation_not_null_constraints::call(
        mcx,
        &rel,
        &stmt.nnconstraints,
        &old_notnulls,
        &connames,
    )?;
    for &attrnum in nncols.iter() {
        seam::set_attnotnull::call(mcx, &rel, attrnum, true, false)?;
    }

    let address = object_address_set(RelationRelationId, relation_id);

    /*
     * Clean up.  We keep lock on new relation.
     */
    rel.close(types_storage::lock::NoLock)?;

    Ok(address)
}

/// `create_ctas_internal(attrList, into)` (createas.c:81-145) — build the
/// destination relation for a CREATE TABLE AS / CREATE MATERIALIZED VIEW by
/// faking up a `CreateStmt`, calling `DefineRelation`, creating the TOAST table,
/// and (for a matview) installing the ON SELECT rule via `StoreViewQuery`.
///
/// Installed into `create_ctas_relation` because the createas unit cannot reach
/// `DefineRelation` (would cycle) and the whole sequence lands entirely in the
/// catalog with no createas-observable intermediate state.
pub fn create_ctas_relation<'mcx>(
    mcx: Mcx<'mcx>,
    into: types_nodes::ddlnodes::IntoClause<'mcx>,
    attr_list: PgVec<'mcx, NodePtr<'mcx>>,
    relkind: u8,
    is_matview: bool,
) -> PgResult<ObjectAddress> {
    /*
     * Create the target relation by faking up a CREATE TABLE parsetree and
     * passing it to DefineRelation. We own `into` by value, so move its fields
     * in; the `options` list is needed again for the TOAST step, so keep a
     * deep copy.
     */
    let mut options_copy: PgVec<'mcx, NodePtr<'mcx>> =
        vec_with_capacity_in(mcx, into.options.len())?;
    for opt in into.options.iter() {
        options_copy.push(alloc_in(mcx, opt.clone_in(mcx)?)?);
    }

    let create = CreateStmt {
        relation: into.rel,
        tableElts: attr_list,
        inhRelations: vec_with_capacity_in(mcx, 0)?,
        partbound: None,
        partspec: None,
        ofTypename: None,
        constraints: vec_with_capacity_in(mcx, 0)?,
        nnconstraints: vec_with_capacity_in(mcx, 0)?,
        options: into.options,
        oncommit: into.onCommit,
        tablespacename: into.tableSpaceName,
        accessMethod: into.accessMethod,
        if_not_exists: false,
    };
    let options = options_copy;

    /*
     * Create the relation.  (This will error out if there's an existing view,
     * so we don't need more code to complain if "replace" is false.)
     */
    let into_relation_addr = define_relation(mcx, create, relkind, InvalidOid, None)?;

    /*
     * If necessary, create a TOAST table for the target table.  Note that
     * NewRelationCreateToastTable ends with CommandCounterIncrement(), so that
     * the TOAST table will be visible for insertion.
     */
    CommandCounterIncrement()?;
    create_toast_for_relation(mcx, into_relation_addr.objectId, &options)?;

    /* Create the "view" part of a materialized view. */
    if is_matview {
        /* StoreViewQuery scribbles on tree, so make a copy */
        let view_query_node = into
            .viewQuery
            .as_deref()
            .expect("create_ctas_relation: matview into->viewQuery is NULL");
        let query = match view_query_node.as_query() {
            Some(q) => q.clone_in(mcx)?,
            None => panic!("create_ctas_relation: into->viewQuery is not a Query"),
        };
        backend_commands_view_seams::store_view_query::call(
            mcx,
            into_relation_addr.objectId,
            query,
            false,
        )?;
        CommandCounterIncrement()?;
    }

    Ok(into_relation_addr)
}

/// `set_attnotnull(wqueue, rel, attnum, is_valid, queue_validation)`
/// (tablecmds.c:8534) — set the `attnotnull` flag on a column of a relation.
///
/// Used by `DefineRelation` (and `ATExecSetNotNull`) to mark the columns that
/// a PRIMARY KEY / NOT NULL constraint covers. The `DefineRelation` caller
/// passes `wqueue == NULL`, `is_valid == true`, `queue_validation == false`,
/// so the ALTER-TABLE phase-3 validation-queue block (`ATGetQueueEntry`) is not
/// reached here.
///
/// On the not-yet-set path it writes `pg_attribute.attnotnull = true`, stamps
/// the live relcache entry's compact attribute `attnullability = ATTNULLABLE_VALID`
/// (the in-place descriptor poke C does through the relation pointer, expressed
/// via the `set_relcache_attnullability` relcache seam), and bumps the command
/// counter. `is_valid` is carried but unused by the PG 18.3 body (the stamp is
/// unconditionally `ATTNULLABLE_VALID`). If `attnotnull` was already set, it only
/// registers a relcache invalidation.
pub fn set_attnotnull<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    _is_valid: bool,
    _queue_validation: bool,
) -> PgResult<()> {
    // Assert(!queue_validation || wqueue): the only caller passes
    // queue_validation == false, so there is no wqueue to require.

    // CheckAlterTableIsSafe(rel);
    crate::at_phase::CheckAlterTableIsSafe(rel)?;

    // Exit quickly by testing attnotnull from the tupledesc's copy of the
    // attribute.
    // attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
    let attr = rel.rd_att.attr((attnum - 1) as usize);
    if attr.attisdropped {
        return Ok(());
    }

    if !attr.attnotnull {
        // attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
        let attr_rel = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

        // tuple = SearchSysCacheCopyAttNum(RelationGetRelid(rel), attnum);
        let tuple = match SearchSysCacheAttNum(mcx, rel.rd_id, attnum)? {
            Some(t) => t,
            None => {
                // elog(ERROR, "cache lookup failed for attribute %d of relation %u")
                return ereport(ERROR)
                    .errmsg(format!(
                        "cache lookup failed for attribute {} of relation {}",
                        attnum, rel.rd_id
                    ))
                    .finish(here("set_attnotnull"))
                    .map(|()| unreachable!());
            }
        };

        // thisatt = TupleDescCompactAttr(RelationGetDescr(rel), attnum - 1);
        // thisatt->attnullability = ATTNULLABLE_VALID;
        // Poke the live relcache entry's compact-attr nullability in place (the
        // cached descriptor mutation C performs through the relation pointer).
        relcache_seam::set_relcache_attnullability::call(rel.rd_id, attnum, ATTNULLABLE_VALID)?;

        // ((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull = true;
        // CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);
        let row = PgAttributeUpdateRow {
            attnotnull: Some(true),
            ..Default::default()
        };
        indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attr_rel, &tuple, &row)?;

        // The queue_validation block (NotNullImpliedByRelConstraints +
        // ATGetQueueEntry) is unreachable here: the DefineRelation caller passes
        // queue_validation == false (and no wqueue).

        // CommandCounterIncrement();
        CommandCounterIncrement()?;

        // table_close(attr_rel, RowExclusiveLock): RAII drop (lmgr lock is
        // transaction-scoped).
        drop(attr_rel);

        // heap_freetuple(tuple): owned by the mcx arena; dropped at scope end.
    } else {
        // CacheInvalidateRelcache(rel);
        backend_utils_cache_inval_seams::cache_invalidate_relcache::call(rel.rd_id)?;
    }

    Ok(())
}

/// `RELKIND_HAS_TABLE_AM(relkind)` (pg_class.h): does this relkind carry a table
/// access method in `pg_class.relam`? True for ordinary tables, toast tables,
/// and matviews. Sequences are *accessed* like heaps (relcache overwrites
/// `rd_amhandler` for them), but their `pg_class.relam` stays `InvalidOid`, so
/// they are deliberately excluded here — matching pg_class.h exactly.
fn RELKIND_HAS_TABLE_AM(relkind: u8) -> bool {
    relkind == RELKIND_RELATION
        || relkind == types_tuple::access::RELKIND_TOASTVALUE
        || relkind == types_tuple::access::RELKIND_MATVIEW
}

/// `BuildDescForRelation(const List *columns)` (tablecmds.c:1380).
pub fn build_desc_for_relation<'mcx>(
    mcx: Mcx<'mcx>,
    columns: &[ColumnDef<'mcx>],
) -> PgResult<TupleDescData<'mcx>> {
    /* allocate a new tuple descriptor */
    let natts = columns.len() as i32;
    let mut desc = CreateTemplateTupleDesc(mcx, natts)?;

    let mut attnum: AttrNumber = 0;

    for entry in columns.iter() {
        attnum += 1;

        let attname = entry.colname.as_ref().map(|s| s.as_str()).unwrap_or("");
        let type_name = entry
            .typeName
            .as_deref()
            .expect("ColumnDef.typeName is NOT NULL");
        let (atttypid, atttypmod) = seam::typename_type_id_and_mod::call(mcx, type_name)?;

        let aclresult = aclchk_seam::object_aclcheck::call(
            TypeRelationId,
            atttypid,
            miscinit_seam::get_user_id::call(),
            ACL_USAGE,
        )?;
        if aclresult != ACLCHECK_OK {
            aclchk_seam::aclcheck_error_type::call(aclresult, atttypid)?;
        }

        let attcollation = seam::get_column_def_collation::call(mcx, entry, atttypid)?;
        let attdim = type_name.arrayBounds.len() as i32;
        if attdim > PG_INT16_MAX {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("too many array dimensions")
                .into_error());
        }

        if type_name.setof {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!("column \"{attname}\" cannot be declared SETOF"))
                .into_error());
        }

        TupleDescInitEntry(&mut desc, attnum, Some(attname), atttypid, atttypmod, attdim)?;

        /* Override TupleDescInitEntry's settings as requested */
        TupleDescInitEntryCollation(&mut desc, attnum, attcollation)?;

        /* Fill in additional stuff not handled by TupleDescInitEntry */
        let att = desc.attr_mut((attnum - 1) as usize);
        att.attnotnull = entry.is_not_null;
        att.attislocal = entry.is_local;
        att.attinhcount = entry.inhcount;
        att.attidentity = entry.identity;
        att.attgenerated = entry.generated;
        let atttypid_for_compress = att.atttypid;
        att.attcompression = seam::get_attribute_compression::call(
            atttypid_for_compress,
            entry.compression.as_ref().map(|s| s.as_str()),
        )?;
        if entry.storage != 0 {
            desc.attr_mut((attnum - 1) as usize).attstorage = entry.storage;
        } else if let Some(storage_name) = entry.storage_name.as_ref().map(|s| s.as_str()) {
            let att = desc.attr_mut((attnum - 1) as usize);
            let typid = att.atttypid;
            att.attstorage = seam::get_attribute_storage::call(typid, storage_name)?;
        }

        populate_compact_attribute(&mut desc, (attnum - 1) as usize)?;
    }

    Ok(desc)
}

/// `StoreCatalogInheritance(relationId, supers, child_is_partition)`
/// (tablecmds.c:3521). The early `supers == NIL` return is handled here; the
/// pg_inherits write loop crosses the `store_catalog_inheritance_supers` seam.
fn store_catalog_inheritance<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    supers: &[Oid],
    child_is_partition: bool,
) -> PgResult<()> {
    /* sanity checks */
    debug_assert!(OidIsValid(relation_id));

    if supers.is_empty() {
        return Ok(());
    }

    seam::store_catalog_inheritance_supers::call(mcx, relation_id, supers, child_is_partition)
}

/// The pg_inherits write loop of `StoreCatalogInheritance` (tablecmds.c:3521),
/// installed as the `store_catalog_inheritance_supers` seam. `supers` is
/// non-empty (the early NIL return is handled by the in-owner wrapper).
pub fn store_catalog_inheritance_supers<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    supers: &[Oid],
    child_is_partition: bool,
) -> PgResult<()> {
    /*
     * Store INHERITS information in pg_inherits using direct ancestors only.
     * Also enter dependencies on the direct ancestors, and make sure they are
     * marked with relhassubclass = true.
     *
     * table_open(InheritsRelationId, RowExclusiveLock): the StoreSingleInheritance
     * owner takes its own lock per call, so the explicit open here only mirrors
     * the C lifetime; the RAII handle drops at scope end (lock is xact-scoped).
     */
    let inh_relation = relation_open(
        mcx,
        types_catalog::pg_inherits::InheritsRelationId,
        RowExclusiveLock,
    )?;

    let mut seq_number = 1i32;
    for &parent_oid in supers.iter() {
        store_catalog_inheritance1(mcx, relation_id, parent_oid, seq_number, child_is_partition)?;
        seq_number += 1;
    }

    drop(inh_relation);
    Ok(())
}

/// `StoreCatalogInheritance1` (tablecmds.c:3556): make catalog entries showing
/// `relationId` as an inheritance child of `parentOid`.
fn store_catalog_inheritance1<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    parent_oid: Oid,
    seq_number: i32,
    child_is_partition: bool,
) -> PgResult<()> {
    /* store the pg_inherits row */
    backend_catalog_pg_inherits::StoreSingleInheritance(relation_id, parent_oid, seq_number)?;

    /* Store a dependency too */
    let parentobject = object_address_set(RelationRelationId, parent_oid);
    let childobject = object_address_set(RelationRelationId, relation_id);
    backend_catalog_dependency_seams::record_dependency_on::call(
        childobject,
        parentobject,
        child_dependency_type(child_is_partition),
    )?;

    /*
     * Post creation hook of this inheritance (InvokeObjectPostAlterHookArg):
     * a no-op in this port (fires only when an extension installs an
     * object_access_hook, which never happens here).
     */

    /* Mark the parent as having subclasses. */
    crate::smallfns::set_relation_has_subclass(mcx, parent_oid, true)?;

    Ok(())
}

/// `child_dependency_type(child_is_partition)` (catalog/heap.c): partitions get
/// an AUTO dependency, regular inheritance children a NORMAL one.
fn child_dependency_type(
    child_is_partition: bool,
) -> types_catalog::catalog_dependency::DependencyType {
    if child_is_partition {
        types_catalog::catalog_dependency::DEPENDENCY_AUTO
    } else {
        types_catalog::catalog_dependency::DEPENDENCY_NORMAL
    }
}

/// `findAttrByName(attributeName, columns)` (tablecmds.c:3609): the 1-based
/// index of the matching column, or 0 if none.
#[allow(dead_code)]
pub fn findAttrByName(attribute_name: &str, columns: &[ColumnDef<'_>]) -> i32 {
    let mut i = 1;
    for col in columns.iter() {
        if col.colname.as_ref().map(|s| s.as_str()) == Some(attribute_name) {
            return i;
        }
        i += 1;
    }
    0
}

/// `storage_name(c)` (tablecmds.c:2460): the name of a typstorage/attstorage
/// enum value (used in F1+ ALTER error messages).
#[allow(dead_code)]
pub(crate) fn storage_name(c: i8) -> &'static str {
    use types_tuple::heaptuple::{
        TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL, TYPSTORAGE_MAIN, TYPSTORAGE_PLAIN,
    };
    match c {
        TYPSTORAGE_PLAIN => "PLAIN",
        TYPSTORAGE_EXTERNAL => "EXTERNAL",
        TYPSTORAGE_EXTENDED => "EXTENDED",
        TYPSTORAGE_MAIN => "MAIN",
        _ => "???",
    }
}

/// `GetAttributeCompression(atttypid, compression)` (tablecmds.c:22044):
/// resolve a column's `attcompression` from its compression name and type.
/// `None` / `"default"` selects the cluster default (`InvalidCompressionMethod`).
pub(crate) fn get_attribute_compression(
    atttypid: Oid,
    compression: Option<&str>,
) -> PgResult<i8> {
    use backend_access_common_toast_compression::{
        compression_name_to_method, INVALID_COMPRESSION_METHOD,
    };

    match compression {
        None => return Ok(INVALID_COMPRESSION_METHOD as i8),
        Some("default") => return Ok(INVALID_COMPRESSION_METHOD as i8),
        Some(_) => {}
    }
    let compression = compression.unwrap();

    /*
     * To specify a nondefault method, the column data type must be toastable.
     * (attstorage and attcompression are intentionally independent.)
     */
    if !type_is_toastable(atttypid)? {
        let typ = backend_utils_adt_format_type_seams::format_type_be_str::call(atttypid)?;
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("column data type {typ} does not support compression"))
            .finish(here("GetAttributeCompression"))
            .map(|()| unreachable!());
    }

    let cmethod = compression_name_to_method(compression.as_bytes())?;
    if cmethod == INVALID_COMPRESSION_METHOD {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid compression method \"{compression}\""))
            .finish(here("GetAttributeCompression"))
            .map(|()| unreachable!());
    }
    Ok(cmethod as i8)
}

/// `GetAttributeStorage(atttypid, storagemode)` (tablecmds.c:9152): map a
/// STORAGE mode keyword to its `attstorage` char, validating the mode is legal
/// for the column type. Used both by `DefineRelation` (column-level STORAGE in
/// CREATE TABLE) and `ATExecSetStorage` (ALTER COLUMN SET STORAGE).
pub(crate) fn get_attribute_storage(atttypid: Oid, storagemode: &str) -> PgResult<i8> {
    use types_tuple::heaptuple::{
        TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL, TYPSTORAGE_MAIN, TYPSTORAGE_PLAIN,
    };

    // pg_strcasecmp — case-insensitive ASCII comparison.
    let cstorage = if storagemode.eq_ignore_ascii_case("plain") {
        TYPSTORAGE_PLAIN
    } else if storagemode.eq_ignore_ascii_case("external") {
        TYPSTORAGE_EXTERNAL
    } else if storagemode.eq_ignore_ascii_case("extended") {
        TYPSTORAGE_EXTENDED
    } else if storagemode.eq_ignore_ascii_case("main") {
        TYPSTORAGE_MAIN
    } else if storagemode.eq_ignore_ascii_case("default") {
        lsyscache_seam::get_typstorage::call(atttypid)? as i8
    } else {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid storage type \"{storagemode}\""))
            .finish(here("GetAttributeStorage"))
            .map(|()| unreachable!());
    };

    // safety check: do not allow toasted storage modes unless column datatype
    // is TOAST-aware.
    if !(cstorage == TYPSTORAGE_PLAIN || type_is_toastable(atttypid)?) {
        let typ = backend_utils_adt_format_type_seams::format_type_be_str::call(atttypid)?;
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("column data type {typ} can only have storage PLAIN"))
            .finish(here("GetAttributeStorage"))
            .map(|()| unreachable!());
    }

    Ok(cstorage)
}

/// `TypeIsToastable(typid)` (catalog/pg_type.h) ==
/// `get_typstorage(typid) != TYPSTORAGE_PLAIN`.
fn type_is_toastable(typid: Oid) -> PgResult<bool> {
    let storage = lsyscache_seam::get_typstorage::call(typid)?;
    Ok(storage as i8 != types_tuple::heaptuple::TYPSTORAGE_PLAIN)
}

// ---------------------------------------------------------------------------
// Small node-construction / list helpers.
// ---------------------------------------------------------------------------

/// Project the owned `tableElts` node list (a list of `Node::ColumnDef`) into a
/// `PgVec<ColumnDef>` for `MergeAttributes`.
fn nodes_to_columndefs<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: &[NodePtr<'mcx>],
) -> PgResult<PgVec<'mcx, ColumnDef<'mcx>>> {
    let mut out: PgVec<ColumnDef> = vec_with_capacity_in(mcx, nodes.len())?;
    for n in nodes.iter() {
        out.push(as_column_def(n).clone_in(mcx)?);
    }
    Ok(out)
}

/// `palloc(sizeof(CookedConstraint))` + `contype = CONSTR_DEFAULT` for an
/// inherited (cooked) column default — carried as a `Node` for the
/// cooked-constraints list. The repo's owned model wraps the cooked expression
/// directly; the catalog owner consumes it.
fn make_cooked_default<'mcx>(
    mcx: Mcx<'mcx>,
    attnum: AttrNumber,
    expr: NodePtr<'mcx>,
) -> PgResult<NodePtr<'mcx>> {
    // Carry the cooked default as a CONSTR_DEFAULT `Constraint`, matching the
    // CookedConstraint that StoreConstraints consumes: `raw_expr` holds the
    // cooked default expression and `location` carries the attnum (the cooked
    // node's `attnum` field in C).
    let mut c = crate::mergeattr::empty_constraint(mcx, ConstrType::CONSTR_DEFAULT)?;
    c.raw_expr = Some(expr);
    c.location = attnum as i32;
    alloc_in(mcx, Node::mk_constraint(mcx, c)?)
}

/// `list_concat(a, b)` for two owned node lists.
fn list_concat<'mcx>(
    mut a: Vec<NodePtr<'mcx>>,
    b: PgVec<'mcx, NodePtr<'mcx>>,
) -> Vec<NodePtr<'mcx>> {
    for n in b.into_iter() {
        a.push(n);
    }
    a
}

/// `cons->name` of a `CookedConstraint` node returned by
/// `AddRelationNewConstraints`. The owned model carries the constraint name (if
/// any) in the node; extracted via the node's constraint-name accessor.
fn cooked_constraint_name(node: &Node<'_>) -> Option<String> {
    match node.as_constraint() {
        Some(c) => c.conname.as_ref().map(|s| s.as_str().to_string()),
        None => None,
    }
}
