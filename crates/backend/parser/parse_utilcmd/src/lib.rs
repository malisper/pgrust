#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// The project-wide error contract is the un-boxed `PgResult` (large `Err`).
#![allow(clippy::result_large_err)]
// The bodies below mirror parse_utilcmd.c 1:1: index-based scans over parallel
// vectors and the explicit branch order matching the C `switch` arms are kept.
#![allow(clippy::needless_range_loop)]
#![allow(clippy::too_many_arguments)]

//! Port of `src/backend/parser/parse_utilcmd.c` (PostgreSQL 18.3) — parse
//! analysis for the utility commands: CREATE / ALTER TABLE, CREATE INDEX, CREATE
//! STATISTICS, CREATE RULE, CREATE SCHEMA, partition bounds, and LIKE / OF-type
//! expansion.
//!
//! `parse_utilcmd.c` has **no** file-static state — everything is threaded
//! through two stack context structs ([`core::CreateStmtContext`],
//! [`core::CreateSchemaStmtContext`]). In the owned `Node<'mcx>` model these hold
//! `PgVec<NodePtr>` accumulators instead of `List *`, and the node tree is the
//! unified [`nodes::nodes::Node`] enum (a NULL `Node *` becomes `None`).
//!
//! The node-independent building skeleton (the constraint bucketing, the
//! `[NOT] NULL` / DEFAULT / IDENTITY / GENERATED / PK / UNIQUE / CHECK / FK
//! distribution, the CREATE SCHEMA element split, the CREATE TABLE element
//! dispatch + output assembly, and the index-redundancy dedup) is ported 1:1.
//! The arms that resolve types / opclasses / collations from the catalog, open
//! relations through the relcache, generate sequences, or compute partition
//! bounds are routed through the per-owner seam crates: the outward seams in
//! [`parse_utilcmd_outward_seams`] (catalog/relcache leaves) and
//! the inward seams this crate owns and installs in [`init_seams`].

extern crate alloc;

pub mod core;
mod errpos;

pub mod alter;
pub mod cloned_index;
pub mod coltype;
pub mod column;
pub mod constraint;
pub mod fk_check_attrs;
pub mod index_constraint;
pub mod expand_like;
pub mod index_stats;
pub mod like;
pub mod partition;
pub mod schema;
pub mod serial;
pub mod toplevel;

// --- Shared core re-exports -------------------------------------------------
pub use core::{CreateSchemaStmtContext, CreateStmtContext};

// --- Public entry points (the `extern` set from parse_utilcmd.h) ------------
pub use toplevel::{
    transformAlterTableStmt, transformCreateStmt, transformIndexStmt, transformRuleStmt,
    transformStatsStmt,
};

pub use partition::transformPartitionBound;
pub use schema::transformCreateSchemaStmtElements;

/// Install the inward seams this crate owns. Must be called from the workspace
/// `init_all`/`init_seams` startup. Installs:
///   * `transformCreateSchemaStmtElements` (CREATE SCHEMA element reorder)
///   * `transformRuleStmt` (CREATE RULE parse analysis)
pub fn init_seams() {
    parse_utilcmd_seams::transformCreateSchemaStmtElements::set(
        transformCreateSchemaStmtElements,
    );
    parse_utilcmd_seams::transformRuleStmt::set(transformRuleStmt);
    parse_utilcmd_seams::generateClonedIndexStmt::set(
        cloned_index::generateClonedIndexStmt,
    );
    parse_utilcmd_seams::transformAlterTableStmt::set(transformAlterTableStmt);

    // The tcop/utility.c ProcessUtilitySlow CREATE TABLE / CREATE INDEX / CREATE
    // STATISTICS arms run these parse-analysis transforms through the utility
    // outward seams.
    utility_out_seams::transform_create_stmt::set(transformCreateStmt);
    utility_out_seams::transform_index_stmt::set(transformIndexStmt);
    utility_out_seams::transform_stats_stmt::set(transformStatsStmt);

    // The catalog-resident leaf of transformIndexConstraint (USING INDEX checks,
    // column resolution against cxt.columns / system attrs, PRIMARY-KEY-implied
    // not-null additions). Lives in index_constraint.rs; reached across the
    // outward seam from transformIndexConstraint in this same crate.
    parse_utilcmd_outward_seams::transformIndexConstraintCatalog::set(
        index_constraint::transform_index_constraint_catalog,
    );

    // The post-DefineRelation CREATE TABLE … (LIKE …) leg, run from
    // ProcessUtilitySlow once the child table exists (defaults / generated /
    // CHECK constraints / indexes / constraint comments).
    utility_out_seams::expand_table_like_clause::set(
        expand_like::expandTableLikeClause,
    );
}
