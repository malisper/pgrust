//! SHARED CORE for the `parse_utilcmd.c` port — the two stack context structs.
//!
//! `parse_utilcmd.c` has **no** file-static variables or global statics: all of
//! its mutable state is carried on the stack in two context structs that are
//! threaded by pointer through the CREATE / ALTER TABLE and CREATE SCHEMA
//! transformation subroutines. In the owned-tree idiom the `List *`
//! accumulators become `PgVec<NodePtr>` and the `Node *` / typed pointers become
//! the unified [`Node`] enum (a NULL pointer → `None`).
//!
//! `CreateStmtContext` is mutated in place by nearly every module — its
//! `columns` / `ckconstraints` / `nnconstraints` / `fkconstraints` /
//! `ixconstraints` / `likeclauses` / `blist` / `alist` / `pkey` fields are
//! accumulators — so it is borrowed `&mut` across module boundaries. It carries
//! the arena `Mcx` so subroutines can allocate new accumulator vectors / nodes.

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::ParseState;
use types_nodes::value::StringNode;

/// `NodePtr<'mcx>` — a boxed `Node` (the C `Node *`).
pub type NodePtr<'mcx> = PgBox<'mcx, Node<'mcx>>;

/// `makeString(str)` (`nodes/value.h`) — wrap a string in a `String` value node.
pub fn make_string<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<NodePtr<'mcx>> {
    mcx::alloc_in(
        mcx,
        Node::mk_string(mcx, StringNode {
            sval: PgString::from_str_in(s, mcx)?,
        })?,
    )
}

/// `CreateStmtContext` — state shared by `transformCreateStmt` /
/// `transformAlterTableStmt` and **all** of their subroutines (the C
/// `typedef struct { ... } CreateStmtContext`).
///
/// The accumulator fields are appended to in place by the various transform
/// subroutines, which is why this struct is passed as `&mut CreateStmtContext`
/// across module boundaries.
pub struct CreateStmtContext<'mcx> {
    /// arena allocator for accumulator vectors / new nodes.
    pub mcx: Mcx<'mcx>,
    /// overall parser state
    pub pstate: PgBox<'mcx, ParseState<'mcx>>,
    /// "CREATE [FOREIGN] TABLE" or "ALTER TABLE"
    pub stmtType: &'static str,
    /// relation to create (the `RangeVar` node)
    pub relation: Option<NodePtr<'mcx>>,
    /// opened/locked rel OID, if ALTER (`InvalidOid` for CREATE)
    pub rel_oid: Oid,
    /// relations to inherit from (list of RangeVar)
    pub inhRelations: PgVec<'mcx, NodePtr<'mcx>>,
    /// true if CREATE/ALTER FOREIGN TABLE
    pub isforeign: bool,
    /// true if altering existing table
    pub isalter: bool,
    /// `ColumnDef` items
    pub columns: PgVec<'mcx, NodePtr<'mcx>>,
    /// CHECK constraints
    pub ckconstraints: PgVec<'mcx, NodePtr<'mcx>>,
    /// NOT NULL constraints
    pub nnconstraints: PgVec<'mcx, NodePtr<'mcx>>,
    /// FOREIGN KEY constraints
    pub fkconstraints: PgVec<'mcx, NodePtr<'mcx>>,
    /// index-creating constraints
    pub ixconstraints: PgVec<'mcx, NodePtr<'mcx>>,
    /// LIKE clauses that need post-processing
    pub likeclauses: PgVec<'mcx, NodePtr<'mcx>>,
    /// "before list" of things to do before creating the table
    pub blist: PgVec<'mcx, NodePtr<'mcx>>,
    /// "after list" of things to do after creating the table
    pub alist: PgVec<'mcx, NodePtr<'mcx>>,
    /// PRIMARY KEY index, if any (an `IndexStmt` node)
    pub pkey: Option<NodePtr<'mcx>>,
    /// true if table is partitioned
    pub ispartitioned: bool,
    /// transformed FOR VALUES (a `PartitionBoundSpec` node)
    pub partbound: Option<NodePtr<'mcx>>,
    /// true if statement contains OF typename
    pub ofType: bool,
}

impl<'mcx> CreateStmtContext<'mcx> {
    /// Convenience: the bare relation name (for error messages), as C reads
    /// `cxt->relation->relname`.
    pub fn relname(&self) -> &str {
        match self.relation.as_deref().and_then(|n| n.as_rangevar()) {
            Some(rv) => rv.relname.as_ref().map_or("", PgString::as_str),
            None => "",
        }
    }
}

/// `CreateSchemaStmtContext` — state shared by
/// `transformCreateSchemaStmtElements` and `setSchemaName` (the C
/// `typedef struct { ... } CreateSchemaStmtContext`).  Local to
/// [`schema`](crate::schema).
pub struct CreateSchemaStmtContext<'mcx> {
    /// name of schema
    pub schemaname: Option<PgString<'mcx>>,
    /// CREATE SEQUENCE items
    pub sequences: PgVec<'mcx, NodePtr<'mcx>>,
    /// CREATE TABLE items
    pub tables: PgVec<'mcx, NodePtr<'mcx>>,
    /// CREATE VIEW items
    pub views: PgVec<'mcx, NodePtr<'mcx>>,
    /// CREATE INDEX items
    pub indexes: PgVec<'mcx, NodePtr<'mcx>>,
    /// CREATE TRIGGER items
    pub triggers: PgVec<'mcx, NodePtr<'mcx>>,
    /// GRANT items
    pub grants: PgVec<'mcx, NodePtr<'mcx>>,
}
