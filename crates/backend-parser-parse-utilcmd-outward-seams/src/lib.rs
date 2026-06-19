//! Outward dependency seams for `backend-parser-parse-utilcmd`
//! (`parser/parse_utilcmd.c`).
//!
//! These mirror the C signatures of callees whose owners are genuinely
//! catalog/relcache/partcache-bound and are not reachable from this crate. Each
//! seam panics by default; a future owner port installs the real body via
//! `::set(...)`. No `todo!()`/`unimplemented!()` — every unported leaf is a
//! loud declared seam-and-panic.
//!
//! `transformColumnType` and `quote_qualified_identifier` formerly lived here
//! but are now real bodies in the owning crate (their substrate —
//! `typenameType`/`LookupCollation`/`format_type_be` and `ruleutils` — is a
//! cycle-free direct dependency). The remaining seams bottom out on the live
//! relcache `Relation` carrier that `CreateStmtContext` deliberately omits
//! (and, for the rule/partition legs, analyze.c / planner substrate).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, PgBox, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::ParseState;

type NodeBox<'mcx> = PgBox<'mcx, Node<'mcx>>;

seam_core::seam!(
    /// `transformTableLikeClause(cxt, table_like_clause)` (parse_utilcmd.c):
    /// expand `LIKE <srctable>` into recreated column definitions by reading the
    /// source relation's `TupleDesc`, defaults, identity, storage, compression,
    /// comments and NOT NULL constraints through the relcache. Returns the new
    /// `(columns, nnconstraints, alist, likeclauses)` to fold into the context.
    /// Relcache/catalog/syscache-bound.
    pub fn transformTableLikeClause<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &ParseState<'mcx>,
        relation: NodeBox<'mcx>,
        table_like_clause: NodeBox<'mcx>,
        isforeign: bool,
    ) -> PgResult<(
        PgVec<'mcx, NodeBox<'mcx>>,
        PgVec<'mcx, NodeBox<'mcx>>,
        PgVec<'mcx, NodeBox<'mcx>>,
        PgVec<'mcx, NodeBox<'mcx>>,
    )>
);

seam_core::seam!(
    /// The catalog-resident leaf of `transformIndexConstraint` (parse_utilcmd.c):
    /// the ALTER TABLE ADD CONSTRAINT USING INDEX path (`get_relname_relid` /
    /// `index_open` / opclass+collation checks), the inherited-table column
    /// search (`table_openrv`), the WITHOUT OVERLAPS type check
    /// (`type_is_range`/`type_is_multirange`/`typenameTypeId`), and the
    /// `SystemAttributeByName` lookups. Given a built `IndexStmt`, the
    /// constraint, and the accumulator state, finishes building the index
    /// definition (mutating the `IndexStmt` and appending any
    /// PRIMARY-KEY-implied not-null constraints). Returns the finished
    /// `(IndexStmt, extra_nnconstraints)`.
    pub fn transformIndexConstraintCatalog<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &ParseState<'mcx>,
        constraint: NodeBox<'mcx>,
        index: NodeBox<'mcx>,
        relation: NodeBox<'mcx>,
        rel_oid: Oid,
        isalter: bool,
        columns: PgVec<'mcx, NodeBox<'mcx>>,
        inh_relations: PgVec<'mcx, NodeBox<'mcx>>,
    ) -> PgResult<(NodeBox<'mcx>, PgVec<'mcx, NodeBox<'mcx>>)>
);

seam_core::seam!(
    /// `transformAlterTableStmt(relid, stmt, queryString, &beforeStmts,
    /// &afterStmts)` (parse_utilcmd.c): parse analysis for ALTER TABLE — the
    /// per-subcommand relcache dispatch (`relation_open` / `RelationGetDescr` /
    /// `get_attnum` / `getIdentitySequence` / the USING-clause `transformExpr` /
    /// ALTER SEQUENCE generation). Returns `(stmt, beforeStmts, afterStmts)`.
    pub fn transformAlterTableStmt<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        stmt: NodeBox<'mcx>,
        query_string: &str,
    ) -> PgResult<(
        NodeBox<'mcx>,
        PgVec<'mcx, NodeBox<'mcx>>,
        PgVec<'mcx, NodeBox<'mcx>>,
    )>
);

seam_core::seam!(
    /// `transformPartitionBound(pstate, parent, spec)` (parse_utilcmd.c):
    /// transform a partition `FOR VALUES` bound against the parent's
    /// `PartitionKey` (`RelationGetPartitionKey`, `get_partition_*`,
    /// `transformExpr`/`coerce_to_target_type`/`evaluate_expr`). Relcache/
    /// partcache/planner-bound. Carries the bound `Node` in and out.
    pub fn transformPartitionBound<'mcx>(
        mcx: Mcx<'mcx>,
        pstate: &ParseState<'mcx>,
        parent_relid: Oid,
        spec: NodeBox<'mcx>,
    ) -> PgResult<NodeBox<'mcx>>
);

seam_core::seam!(
    /// The catalog/relcache leaf of `transformRuleStmt` (parse_utilcmd.c):
    /// `table_openrv(stmt->relation, AccessExclusiveLock)`, building the fake
    /// OLD/NEW range-table entries, running each action statement through
    /// analyze.c (`transformStmt`) and the WHERE qual through
    /// `transformWhereClause`, and validating OLD/NEW usage. Returns the
    /// analysed `(actions, where_clause)`.
    pub fn transformRuleStmtCatalog<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &types_nodes::ddlnodes::RuleStmt<'mcx>,
        query_string: &str,
    ) -> PgResult<(
        PgVec<'mcx, types_nodes::copy_query::Query<'mcx>>,
        Option<Node<'mcx>>,
    )>
);
