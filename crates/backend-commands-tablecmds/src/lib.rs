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

mod at_column;
mod at_phase;
mod create;
mod drop;
mod f1_rename;
mod helpers;
mod mergeattr;
mod oncommit;
mod smallfns;
mod truncate;

pub use at_phase::{
    AlterTable, AlterTableGetLockLevel, AlterTableInternal, AlterTableUtilityContext,
};

use backend_commands_tablecmds_seams as seam;

/// Install every F0-owned inward seam. (The outward seams declared in
/// `backend-commands-tablecmds-seams` are installed by their owners when they
/// land.)
pub fn init_seams() {
    seam::define_relation::set(create::define_relation);
    seam::build_desc_for_relation::set(create::build_desc_for_relation);
    // DefineRelation's reloptions block (transformRelOptions + per-relkind
    // validate, tablecmds.c:930-946). Declared as an outward seam from create.rs
    // but its body — the create-time reloptions transform — is F0-owned here.
    seam::transform_and_check_reloptions::set(create::transform_and_check_reloptions);
    seam::merge_attributes::set(mergeattr::merge_attributes);

    seam::remove_relations::set(drop::remove_relations);

    seam::execute_truncate::set(truncate::execute_truncate);

    seam::check_table_not_in_use::set(smallfns::check_table_not_in_use);
    seam::set_relation_has_subclass::set(smallfns::set_relation_has_subclass);
    seam::check_relation_tablespace_move::set(smallfns::check_relation_tablespace_move);
    seam::set_relation_tablespace::set(smallfns::set_relation_tablespace);

    // F1 (RENAME / namespace / owner) — the subset buildable without the
    // trimmed-`PgClassForm` carrier keystone (see `f1_rename`).
    seam::range_var_callback_owns_relation::set(f1_rename::range_var_callback_owns_relation);
    seam::define_sequence_relation::set(f1_rename::define_sequence_relation);

    seam::register_on_commit_action::set(oncommit::register_on_commit_action);
    seam::remove_on_commit_action::set(oncommit::remove_on_commit_action);
    seam::pre_commit_on_commit_actions::set(oncommit::pre_commit_on_commit_actions);
    seam::at_eoxact_on_commit_actions::set(oncommit::at_eoxact_on_commit_actions);
    seam::at_eosubxact_on_commit_actions::set(oncommit::at_eosubxact_on_commit_actions);

    // --- ProcessUtility dispatch arms (utility.c TRUNCATE + DROP relations) ---
    backend_tcop_utility_out_seams::execute_truncate::set(execute_truncate_arm);
    backend_tcop_utility_out_seams::remove_relations::set(remove_relations_arm);

    // --- ProcessUtilitySlow CREATE TABLE spine (utility.c:1135-1190) ---
    // `DefineRelation(cstmt, RELKIND_RELATION, InvalidOid, NULL, queryString)`.
    backend_tcop_utility_out_seams::define_relation::set(create::define_relation);
}

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::Node;

/// `case T_TruncateStmt: ExecuteTruncate(stmt)` (utility.c). The dispatch carries
/// the parse tree as `&Node`; extract the `TruncateStmt` variant and forward.
fn execute_truncate_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let Node::TruncateStmt(s) = stmt else {
        panic!("execute_truncate: parse tree is not a TruncateStmt");
    };
    truncate::execute_truncate(mcx, s)
}

/// `ExecDropStmt → RemoveRelations(stmt)` (utility.c) for the relation removeType
/// legs (TABLE/SEQUENCE/VIEW/MATVIEW/FOREIGN TABLE/INDEX).
fn remove_relations_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let Node::DropStmt(s) = stmt else {
        panic!("remove_relations: parse tree is not a DropStmt");
    };
    drop::remove_relations(mcx, s)
}
