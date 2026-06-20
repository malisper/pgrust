//! `backend/commands/tablecmds.c` — FAMILY F0: the relation create / drop /
//! truncate driver functions plus the ON COMMIT bookkeeping.
//!
//! Ported with the same branch order, error codes / messages / SQLSTATE, lock
//! levels, dependency recording, and command-counter bumps as PostgreSQL 18.3.
//!
//! F0 functions ported here:
//! - `DefineRelation` / `BuildDescForRelation` / `StoreCatalogInheritance` /
//!   `findAttrByName` / `storage_name` ([`create`]).
//! - `RemoveRelations` / `RangeVarCallbackForDropRelation` /
//!   `DropErrorMsgNonExistent` / `DropErrorMsgWrongType` ([`drop`] / [`helpers`]).
//! - `ExecuteTruncate` / `ExecuteTruncateGuts` / `truncate_check_rel` /
//!   `truncate_check_perms` / `truncate_check_activity` /
//!   `RangeVarCallbackForTruncate` ([`truncate`]).
//! - `CheckTableNotInUse` / `SetRelationHasSubclass` /
//!   `CheckRelationTableSpaceMove` / `SetRelationTableSpace` ([`smallfns`]).
//! - `register_on_commit_action` / `remove_on_commit_action` /
//!   `PreCommit_on_commit_actions` / `AtEOXact_on_commit_actions` /
//!   `AtEOSubXact_on_commit_actions` ([`oncommit`]).
//!
//! The genuine cross-subsystem externals whose owners are not yet ported cross
//! the [`backend_commands_tablecmds_seams`] outward seams (MergeAttributes /
//! AddRelation* / reloptions / access-method lookup / type-name + collation +
//! storage resolution / relcache reads / pg_class drop-info projection / heap
//! truncate machinery / owned-sequence reset / FDW truncate / trigger firing /
//! WAL truncate / snapshot mgmt / partition blocks / catalog-write bodies).

#![allow(non_snake_case)]

mod at_altertype;
mod at_attach;
mod at_attach_idx;
mod at_detach;
mod at_coladd;
mod at_coldrop;
mod at_column;
mod at_constraint;
mod at_dropvalidate;
mod at_fk;
mod at_owner;
mod at_phase;
mod create;
mod drop;
mod f1_rename;
mod helpers;
mod mergeattr;
mod oncommit;
mod partbound;
mod partition;
mod rename;
mod rename_schema;
mod smallfns;
mod truncate;

pub use at_phase::{
    AlterTable, AlterTableGetLockLevel, AlterTableInternal, AlterTableLookupRelation,
    AlterTableUtilityContext,
};
pub use create::{build_desc_for_relation, define_relation};

use backend_commands_tablecmds_seams as seam;

/// Install every F0-owned inward seam. (The outward seams declared in
/// `backend-commands-tablecmds-seams` are installed by their owners when they
/// land.)
pub fn init_seams() {
    seam::define_relation::set(create::define_relation);
    seam::build_desc_for_relation::set(create::build_desc_for_relation);
    // create_ctas_internal (createas.c): owned here because it calls
    // DefineRelation + StoreViewQuery (the latter across view-seams).
    backend_commands_createas_seams::create_ctas_relation::set(create::create_ctas_relation);
    // DefineRelation's reloptions block (transformRelOptions + per-relkind
    // validate, tablecmds.c:930-946). Declared as an outward seam from create.rs
    // but its body — the create-time reloptions transform — is F0-owned here.
    seam::transform_and_check_reloptions::set(create::transform_and_check_reloptions);
    seam::merge_attributes::set(mergeattr::merge_attributes);
    // StoreCatalogInheritance pg_inherits write loop (tablecmds.c:3521): the
    // CREATE TABLE ... INHERITS pg_inherits rows + dependencies + parent
    // relhassubclass marking.
    seam::store_catalog_inheritance_supers::set(create::store_catalog_inheritance_supers);
    seam::get_attribute_compression::set(create::get_attribute_compression);
    seam::get_attribute_storage::set(create::get_attribute_storage);
    // set_attnotnull (tablecmds.c:8534) — the PK/NOT-NULL-implied attnotnull
    // catalog poke, called from DefineRelation's not-null merge tail.
    seam::set_attnotnull::set(create::set_attnotnull);

    seam::remove_relations::set(drop::remove_relations);

    seam::execute_truncate::set(truncate::execute_truncate);

    seam::get_pg_class_drop_info::set(smallfns::get_pg_class_drop_info);
    seam::is_system_class_relid::set(smallfns::is_system_class_relid);
    seam::get_index_isvalid::set(smallfns::get_index_isvalid);
    seam::check_table_not_in_use::set(smallfns::check_table_not_in_use);
    seam::relation_is_other_temp::set(smallfns::relation_is_other_temp);
    seam::relation_is_logically_logged::set(smallfns::relation_is_logically_logged);
    seam::set_relation_has_subclass::set(smallfns::set_relation_has_subclass);
    seam::check_relation_tablespace_move::set(smallfns::check_relation_tablespace_move);
    seam::set_relation_tablespace::set(smallfns::set_relation_tablespace);
    seam::reset_rel_rewrite::set(at_column::ResetRelRewrite);

    // F1 (RENAME / namespace / owner).
    seam::range_var_callback_owns_relation::set(f1_rename::range_var_callback_owns_relation);
    seam::define_sequence_relation::set(f1_rename::define_sequence_relation);

    // ATExecChangeOwner (at_owner.rs): the ALTER TABLE ... OWNER TO body. The
    // direct AT_ChangeOwner phase calls it in-process (it has the live `mcx`);
    // this seam install serves the REASSIGN OWNED / DROP OWNED path that
    // pg_shdepend's shdepReassignOwned drives by OID (no caller-side `mcx`), so
    // the closure runs the body in a fresh MemoryContext.
    seam::at_exec_change_owner::set(|relation_oid, new_owner_id, recursing, lockmode| {
        let ctx = mcx::MemoryContext::new("ATExecChangeOwner");
        at_owner::ATExecChangeOwner(ctx.mcx(), relation_oid, new_owner_id, recursing, lockmode)
    });

    // RENAME drivers (rename.rs): ALTER ... RENAME COLUMN / RENAME TO. The
    // writable pg_class / pg_attribute write carriers (PgClassForm.relname /
    // PgAttributeUpdateRow.attname) make these fully expressible; the stale
    // "carrier keystone" stop in f1_rename's doc no longer applies.
    seam::renameatt::set(rename::renameatt);
    seam::RenameRelation::set(rename::RenameRelation);
    seam::rename_relation_internal::set(rename::RenameRelationInternal);

    // RENAME CONSTRAINT + SET SCHEMA execute-phase drivers (rename_schema.rs).
    seam::RenameConstraint::set(rename_schema::RenameConstraint);
    seam::AlterTableNamespace::set(rename_schema::AlterTableNamespace);

    seam::register_on_commit_action::set(oncommit::register_on_commit_action);
    seam::remove_on_commit_action::set(oncommit::remove_on_commit_action);
    seam::pre_commit_on_commit_actions::set(oncommit::pre_commit_on_commit_actions);
    seam::at_eoxact_on_commit_actions::set(oncommit::at_eoxact_on_commit_actions);
    seam::at_eosubxact_on_commit_actions::set(oncommit::at_eosubxact_on_commit_actions);

    // transformPartitionBound (parse_utilcmd.c) — the partition `FOR VALUES`
    // bound transform. Declared as an outward seam from the low-level
    // parse-utilcmd crate (relcache/partcache/planner-bound); the body lives in
    // `partbound` here, where the partition key + expression/coercion engine are
    // reachable. Used by parse-utilcmd's ATTACH PARTITION leg; the inline
    // DefineRelation path (create.rs) calls the body directly.
    backend_parser_parse_utilcmd_outward_seams::transformPartitionBound::set(
        partbound::transform_partition_bound_seam,
    );

    // The post-partbound clone block (DefineRelation, tablecmds.c:1258-1328):
    // clone the parent's indexes/triggers/FKs onto the new partition. No-op when
    // the parent has none; a precise error when it does (the cloners are
    // unported). Installed so CREATE TABLE PARTITION OF a bare parent runs
    // end-to-end.
    seam::define_relation_clone_partition_objects::set(
        partbound::define_relation_clone_partition_objects,
    );

    // --- ProcessUtility dispatch arms (utility.c TRUNCATE + DROP relations) ---
    backend_tcop_utility_out_seams::execute_truncate::set(execute_truncate_arm);
    backend_tcop_utility_out_seams::remove_relations::set(remove_relations_arm);

    // --- ProcessUtilitySlow CREATE TABLE spine (utility.c:1135-1190) ---
    // `DefineRelation(cstmt, RELKIND_RELATION, InvalidOid, NULL, queryString)`.
    backend_tcop_utility_out_seams::define_relation::set(create::define_relation);

    // `NewRelationCreateToastTable` follow-on (utility.c:1170-1190): parse +
    // validate toast reloptions then create the TOAST table if needed.
    backend_tcop_utility_out_seams::create_toast_for_relation::set(create::create_toast_for_relation);

    // --- ProcessUtilitySlow ALTER TABLE arm (utility.c:1270-1331) ---
    backend_tcop_utility_out_seams::alter_table_slow::set(alter_table_slow_arm);
}

use mcx::Mcx;
use types_core::primitive::{Oid, OidIsValid};
use types_error::PgResult;
use types_nodes::ddlnodes::AlterTableType::AT_DetachPartition;
use types_nodes::nodes::Node;

/// `case T_TruncateStmt: ExecuteTruncate(stmt)` (utility.c). The dispatch carries
/// the parse tree as `&Node`; extract the `TruncateStmt` variant and forward.
fn execute_truncate_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let Some(s) = stmt.as_truncatestmt() else {
        panic!("execute_truncate: parse tree is not a TruncateStmt");
    };
    truncate::execute_truncate(mcx, s)
}

/// `ExecDropStmt → RemoveRelations(stmt)` (utility.c) for the relation removeType
/// legs (TABLE/SEQUENCE/VIEW/MATVIEW/FOREIGN TABLE/INDEX).
fn remove_relations_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let Some(s) = stmt.as_dropstmt() else {
        panic!("remove_relations: parse tree is not a DropStmt");
    };
    drop::remove_relations(mcx, s)
}

/// `case T_AlterTableStmt:` (utility.c:1270-1331). The DETACH-CONCURRENTLY
/// transaction-block guard, `AlterTableGetLockLevel` + `AlterTableLookupRelation`,
/// the `EventTriggerAlterTableStart` / `EventTriggerAlterTableRelid` fence,
/// `AlterTable`, and the `EventTriggerAlterTableEnd` close (or the "does not
/// exist, skipping" NOTICE). The `pstmt`/`params`/`queryEnv` recursive-callback
/// fields of the C `AlterTableUtilityContext` are needed only by the
/// transform-and-recurse families that are themselves unported; this port's
/// context carries the portable `relid` + `queryString` subset.
fn alter_table_slow_arm<'mcx>(
    mcx: Mcx<'mcx>,
    _pstmt: &types_nodes::nodeindexscan::PlannedStmt<'mcx>,
    parsetree: &Node<'mcx>,
    query_string: &str,
    _params: types_nodes::portalcmds::ParamListInfo,
    is_top_level: bool,
) -> PgResult<()> {
    let Some(atstmt) = parsetree.as_altertablestmt() else {
        panic!("alter_table_slow: parse tree is not an AlterTableStmt");
    };

    // Disallow ALTER TABLE ... DETACH CONCURRENTLY in a transaction block.
    for cmd_node in atstmt.cmds.iter() {
        let Some(cmd) = cmd_node.as_altertablecmd() else {
            unreachable!("AlterTableStmt.cmds element is a Node::AlterTableCmd");
        };
        if cmd.subtype == AT_DetachPartition {
            if let Some(def) = cmd.def.as_ref() {
                if let Some(pc) = def.as_partitioncmd() {
                    if pc.concurrent {
                        backend_access_transam_xact::PreventInTransactionBlock(
                            is_top_level,
                            "ALTER TABLE ... DETACH CONCURRENTLY",
                        )?;
                    }
                }
            }
        }
    }

    // Figure out lock mode, and acquire lock (this also does basic permission
    // checks, via the lookup callback).
    let lockmode = at_phase::AlterTableGetLockLevel(&atstmt.cmds)?;
    let relid: Oid = at_phase::AlterTableLookupRelation(mcx, atstmt, lockmode)?;

    if OidIsValid(relid) {
        // Set up info needed for recursive callbacks ...
        let atcontext = at_phase::AlterTableUtilityContext {
            relid,
            query_string: Some(query_string),
        };

        // ... ensure we have an event trigger context ...
        backend_tcop_utility_out_seams::event_trigger_alter_table_start::call(parsetree);
        backend_tcop_utility_out_seams::event_trigger_alter_table_relid::call(relid);

        // ... and do it.
        at_phase::AlterTable(mcx, atstmt, lockmode, &atcontext)?;

        // done.
        backend_tcop_utility_out_seams::event_trigger_alter_table_end::call();
    } else {
        // relation "%s" does not exist, skipping
        let relname: String = atstmt
            .relation
            .as_ref()
            .and_then(|rv| match rv.as_rangevar() {
                Some(rv) => rv.relname.as_ref().map(|s| s.as_str().to_string()),
                None => None,
            })
            .unwrap_or_default();
        backend_utils_error::ereport(types_error::NOTICE)
            .errmsg(format!("relation \"{relname}\" does not exist, skipping"))
            .finish(helpers::here("alter_table_slow"))?;
    }
    Ok(())
}
