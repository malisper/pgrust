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

mod create;
mod drop;
mod f1_rename;
mod helpers;
mod oncommit;
mod smallfns;
mod truncate;

use backend_commands_tablecmds_seams as seam;

/// Install every F0-owned inward seam. (The outward seams declared in
/// `backend-commands-tablecmds-seams` are installed by their owners when they
/// land.)
pub fn init_seams() {
    seam::define_relation::set(create::define_relation);
    seam::build_desc_for_relation::set(create::build_desc_for_relation);

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
}
