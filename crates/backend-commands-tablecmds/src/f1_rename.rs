//! `backend/commands/tablecmds.c` — FAMILY F1 (RENAME / namespace / owner).
//!
//! Ported here:
//! - `RangeVarCallbackOwnsRelation` (tablecmds.c:19554) — the
//!   `RangeVarGetRelidExtended` ownership-check callback used by `AlterSequence`.
//! - the `DefineSequence` `DefineRelation(stmt, RELKIND_SEQUENCE, ...)` slice
//!   that `commands/sequence.c` crosses as `define_sequence_relation`
//!   (sequence.c:131).
//!
//! The RENAME-relation / RENAME-column drivers (`RenameRelation` /
//! `RenameRelationInternal` / `renameatt` / `renameatt_internal`) now live in
//! [`crate::rename`]: the writable `pg_class` / `pg_attribute` write carriers
//! (`types_cluster::PgClassForm.relname` via `catalog_tuple_update_pg_class`,
//! and `PgAttributeUpdateRow.attname` via `catalog_tuple_update_pg_attribute`)
//! make those fully expressible — the older "trimmed-`PgClassForm` carrier
//! keystone" stop no longer applies. `RenameConstraint` / CHANGE-OWNER /
//! SET-SCHEMA remain their declared `backend-commands-tablecmds-seams` panics
//! (`RenameConstraint` needs a `pg_constraint` contype/conindid/coninhcount/
//! connoinherit form-reader seam that does not exist yet).

#![allow(non_snake_case)]

use mcx::{alloc_in, Mcx};

use types_acl::ACLCHECK_NOT_OWNER;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::catalog::{BOOLOID, INT8OID};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR};
use types_nodes::ddlnodes::{CreateSeqStmt, CreateStmt};
use types_nodes::nodes::{Node, NodePtr};
use types_nodes::primnodes::OnCommitAction;
use types_tuple::access::RELKIND_SEQUENCE;

use backend_utils_error::ereport;

use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_objectaddress_seams as objaddr_seam;
use backend_commands_tablespace_globals_seams as ts_globals_seam;
use backend_nodes_core::makefuncs::make_column_def;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{here, RelationRelationId};

/// `RangeVarCallbackOwnsRelation(relation, relId, oldRelId, arg)`
/// (tablecmds.c:19554).
///
/// The `RangeVarGetRelidExtended` callback shared by `AlterSequence` (and
/// others): for a found relation, verify the current user owns it and that it
/// is not a system catalog (unless `allow_system_table_mods`). `relation` is
/// only read for `relation->relname` in the error messages, so the seam passes
/// the name alone; `relkind` / `relnamespace` (which the C reads off the
/// `SearchSysCache1(RELOID)` tuple) are re-derived via lsyscache, exactly as
/// the parallel `RangeVarCallbackForDropRelation` port does.
pub fn range_var_callback_owns_relation(
    relname: &str,
    rel_id: Oid,
    _old_rel_id: Oid,
) -> PgResult<()> {
    /* Nothing to do if the relation was not found. */
    if !OidIsValid(rel_id) {
        return Ok(());
    }

    /*
     * tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relId));
     * if (!HeapTupleIsValid(tuple)) elog(ERROR, "cache lookup failed ...");
     *
     * get_rel_relkind raises that same "cache lookup failed for relation %u"
     * on a concurrently-dropped relation, standing in for the SearchSysCache1
     * "should not happen" elog.
     */
    let relkind = lsyscache_seam::get_rel_relkind::call(rel_id)?;

    if !aclchk_seam::object_ownercheck::call(
        RelationRelationId,
        rel_id,
        miscinit_seam::get_user_id::call(),
    )? {
        aclchk_seam::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            objaddr_seam::get_relkind_objtype::call(relkind),
            Some(relname.to_string()),
        )?;
    }

    let relnamespace = lsyscache_seam::get_rel_namespace::call(rel_id)?;
    if !ts_globals_seam::allowSystemTableMods::call()?
        && seam::is_system_class_relid::call(rel_id, relkind, relnamespace)?
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{relname}\" is a system catalog"
            ))
            .finish(here("RangeVarCallbackOwnsRelation"));
    }

    Ok(())
}

/// The `DefineSequence` body slice that builds the `CreateStmt` for a
/// sequence's backing relation and runs `DefineRelation(stmt,
/// RELKIND_SEQUENCE, seq->ownerId, NULL, NULL)` (sequence.c:131).
///
/// `commands/sequence.c` owns the surrounding `init_params` /
/// `RangeVarGetAndCheckCreationNamespace` if-not-exists guard / `fill_seq_with_data`
/// / pg_sequence insert; this slice is exactly the three NOT NULL columns
/// (`last_value int8`, `log_cnt int8`, `is_called bool`) + `DefineRelation`,
/// returning the new sequence relation's [`ObjectAddress`]. Mirrors the C
/// `SEQ_COL_FIRSTCOL..SEQ_COL_LASTCOL` loop's `makeColumnDef` order.
pub fn define_sequence_relation<'mcx>(
    mcx: Mcx<'mcx>,
    seq: &CreateSeqStmt<'_>,
) -> PgResult<ObjectAddress> {
    /*
     * Create relation (and fill value[] and null[] for the tuple) --- the
     * value[]/null[] filling and pg_sequence insert stay in sequence.c; here
     * we build the column list DefineRelation needs.
     *
     * for (i = SEQ_COL_FIRSTCOL; i <= SEQ_COL_LASTCOL; i++) { coldef = ...;
     *     coldef->is_not_null = true; stmt->tableElts = lappend(..., coldef); }
     */
    let mut table_elts: mcx::PgVec<'mcx, NodePtr<'mcx>> = mcx::vec_with_capacity_in(mcx, 3)?;

    /* SEQ_COL_LASTVAL: makeColumnDef("last_value", INT8OID, -1, InvalidOid) */
    let mut last_value = make_column_def(mcx, "last_value", INT8OID, -1, InvalidOid)?;
    last_value.is_not_null = true;
    table_elts.push(alloc_in(mcx, Node::mk_column_def(mcx, last_value)?)?);

    /* SEQ_COL_LOG: makeColumnDef("log_cnt", INT8OID, -1, InvalidOid) */
    let mut log_cnt = make_column_def(mcx, "log_cnt", INT8OID, -1, InvalidOid)?;
    log_cnt.is_not_null = true;
    table_elts.push(alloc_in(mcx, Node::mk_column_def(mcx, log_cnt)?)?);

    /* SEQ_COL_CALLED: makeColumnDef("is_called", BOOLOID, -1, InvalidOid) */
    let mut is_called = make_column_def(mcx, "is_called", BOOLOID, -1, InvalidOid)?;
    is_called.is_not_null = true;
    table_elts.push(alloc_in(mcx, Node::mk_column_def(mcx, is_called)?)?);

    /*
     * stmt->relation = seq->sequence;
     * stmt->inhRelations = NIL; stmt->constraints = NIL; stmt->options = NIL;
     * stmt->oncommit = ONCOMMIT_NOOP; stmt->tablespacename = NULL;
     * stmt->if_not_exists = seq->if_not_exists;
     */
    let relation = match &seq.sequence {
        Some(rv) => Some(alloc_in(mcx, rv.clone_in(mcx)?)?),
        None => None,
    };

    let stmt = CreateStmt {
        relation,
        tableElts: table_elts,
        inhRelations: mcx::vec_with_capacity_in(mcx, 0)?,
        partbound: None,
        partspec: None,
        ofTypename: None,
        constraints: mcx::vec_with_capacity_in(mcx, 0)?,
        nnconstraints: mcx::vec_with_capacity_in(mcx, 0)?,
        options: mcx::vec_with_capacity_in(mcx, 0)?,
        oncommit: OnCommitAction::ONCOMMIT_NOOP,
        tablespacename: None,
        accessMethod: None,
        if_not_exists: seq.if_not_exists,
    };

    /* address = DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId, NULL, NULL); */
    seam::define_relation::call(mcx, stmt, RELKIND_SEQUENCE, seq.ownerId, None)
}
