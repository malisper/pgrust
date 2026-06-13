//! Signature vocabulary for `backend/commands/matview.c` (REFRESH MATERIALIZED
//! VIEW + the CREATE MATERIALIZED VIEW populate path).
//!
//! This crate holds the parse-node (`RefreshMatViewStmt`), command-completion
//! (`QueryCompletion`/`CommandTag`), and matview-specific read-out value types
//! the matview driver branches on, plus the opaque handles for objects owned by
//! the not-yet-ported executor / planner / rewriter (`Query *`, `PlannedStmt *`,
//! `QueryDesc *`, `DestReceiver *`, `TupleTableSlot *`, `TupleDesc`). Those C
//! objects are created and consumed entirely inside seam calls into their owning
//! subsystems; matview never inspects their internals, so they stay opaque (the
//! semantic opacity C's `void`-free pointers carry through this driver), to be
//! replaced by the real node types when the executor knot lands.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::string::String;
use std::vec::Vec;

use types_core::primitive::Oid;
use types_tuple::access::RangeVar;

/// `ObjectAddress` (`catalog/objectaddress.h`), re-exported for matview's
/// command-entry-point return values.
pub use types_catalog::catalog_dependency::ObjectAddress;

// ---------------------------------------------------------------------------
// CommandTag / QueryCompletion (tcop/cmdtag.h)
// ---------------------------------------------------------------------------

/// `typedef enum CommandTag` (`tcop/cmdtag.h`, generated from
/// `tcop/cmdtaglist.h`), as a value-checked newtype over the enumerator index.
///
/// Only the two tags REFRESH MATERIALIZED VIEW uses are defined; the values are
/// the positional indices in `cmdtaglist.h` (verified against PostgreSQL 18.3
/// via the c2rust rendering: `CMDTAG_UNKNOWN` = 0,
/// `CMDTAG_REFRESH_MATERIALIZED_VIEW` = 169, `CMDTAG_SELECT` = 179). Extend as
/// more commands are ported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct CommandTag(pub i32);

impl CommandTag {
    /// `CMDTAG_UNKNOWN` (cmdtaglist.h line 27).
    pub const UNKNOWN: CommandTag = CommandTag(0);
    /// `CMDTAG_REFRESH_MATERIALIZED_VIEW` (cmdtaglist.h line 196; enum index 169).
    pub const REFRESH_MATERIALIZED_VIEW: CommandTag = CommandTag(169);
    /// `CMDTAG_SELECT` (cmdtaglist.h line 206; enum index 179).
    pub const SELECT: CommandTag = CommandTag(179);
}

/// `typedef struct QueryCompletion` (`tcop/cmdtag.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryCompletion {
    /// `CommandTag commandTag`.
    pub commandTag: CommandTag,
    /// `uint64 nprocessed`.
    pub nprocessed: u64,
}

impl QueryCompletion {
    /// `SetQueryCompletion(qc, commandTag, nprocessed)` (tcop/cmdtag.h).
    pub fn set(&mut self, command_tag: CommandTag, nprocessed: u64) {
        self.commandTag = command_tag;
        self.nprocessed = nprocessed;
    }
}

// ---------------------------------------------------------------------------
// RefreshMatViewStmt (nodes/parsenodes.h 4003-4009)
// ---------------------------------------------------------------------------

/// `typedef struct RefreshMatViewStmt` (`nodes/parsenodes.h`). The C `NodeTag`
/// header is carried by the node framework; the trimmed value here holds the
/// fields the matview entry point reads.
#[derive(Clone, Debug, PartialEq)]
pub struct RefreshMatViewStmt {
    /// `bool concurrent` — allow concurrent access?
    pub concurrent: bool,
    /// `bool skipData` — true for WITH NO DATA.
    pub skipData: bool,
    /// `RangeVar *relation` — relation to refresh (never NULL in a well-formed
    /// parse).
    pub relation: RangeVar,
}

// ---------------------------------------------------------------------------
// Opaque handles for objects owned by unported subsystems.
// ---------------------------------------------------------------------------

/// Opaque handle to a `Query *` (the matview's stored `dataQuery` and the single
/// rewritten query). `NULL` (`0`) is the C NULL `Query *`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryHandle(pub usize);

impl QueryHandle {
    pub const NULL: QueryHandle = QueryHandle(0);
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// Opaque handle to a `PlannedStmt *` produced by `pg_plan_query`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlannedStmtHandle(pub usize);

impl PlannedStmtHandle {
    pub const NULL: PlannedStmtHandle = PlannedStmtHandle(0);
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// Opaque handle to a `QueryDesc *`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryDescHandle(pub usize);

impl QueryDescHandle {
    pub const NULL: QueryDescHandle = QueryDescHandle(0);
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// Opaque handle to a `DestReceiver *` (the `DR_transientrel` the runtime
/// allocates and wires).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DestReceiverHandle(pub usize);

impl DestReceiverHandle {
    pub const NULL: DestReceiverHandle = DestReceiverHandle(0);
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// Opaque handle to a `TupleTableSlot *` the executor hands the
/// `transientrel_receive` callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TupleSlotHandle(pub usize);

impl TupleSlotHandle {
    pub const NULL: TupleSlotHandle = TupleSlotHandle(0);
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// Opaque handle to a `TupleDesc` the executor hands the
/// `transientrel_startup` callback (unused by C `transientrel_startup`, but part
/// of the `rStartup` vtable contract).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TupleDescHandle(pub usize);

impl TupleDescHandle {
    pub const NULL: TupleDescHandle = TupleDescHandle(0);
    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

// ---------------------------------------------------------------------------
// Read-out value bundles the in-crate logic branches on.
// ---------------------------------------------------------------------------

/// The relcache fields of the open matview that `RefreshMatViewByOid` branches
/// on, read once after `table_open`. Mirrors the `matviewRel->rd_rel->...` and
/// the rule inspection the C does inline (`rd_rules->rules[0]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatViewRelInfo {
    /// `matviewRel->rd_rel->relkind`.
    pub relkind: i8,
    /// `RelationIsPopulated(matviewRel)` (`relispopulated`).
    pub is_populated: bool,
    /// `matviewRel->rd_rel->relowner`.
    pub relowner: Oid,
    /// `matviewRel->rd_rel->relam`.
    pub relam: Oid,
    /// `matviewRel->rd_rel->reltablespace`.
    pub reltablespace: Oid,
    /// `matviewRel->rd_rel->relpersistence`.
    pub relpersistence: i8,
    /// `RelationGetRelationName(matviewRel)`.
    pub relname: String,
    /// `matviewRel->rd_rel->relhasrules`.
    pub relhasrules: bool,
    /// `matviewRel->rd_rules->numLocks` (`< 0` when `rd_rules` is NULL).
    pub num_rules: i32,
    /// `rule->event == CMD_SELECT` for the first rule.
    pub rule_is_select: bool,
    /// `rule->isInstead` for the first rule.
    pub rule_is_instead: bool,
    /// `list_length(rule->actions)` of the first rule.
    pub rule_actions_length: i32,
}

/// The `pg_index` relcache fields `is_usable_unique_index` inspects, read out of
/// an open index relcache handle. Mirrors `indexRel->rd_index`
/// (`indisunique`/`indimmediate`/`indisvalid`/`indnatts`/`indkey.values[i]`) plus
/// `RelationGetIndexPredicate(indexRel) == NIL`. The predicate logic itself
/// stays in the ported crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexUsabilityInfo {
    /// `indexStruct->indisunique`.
    pub indisunique: bool,
    /// `indexStruct->indimmediate`.
    pub indimmediate: bool,
    /// `indexStruct->indisvalid`.
    pub indisvalid: bool,
    /// `RelationGetIndexPredicate(indexRel) == NIL` (true == no predicate).
    pub pred_is_nil: bool,
    /// `indexStruct->indnatts`.
    pub indnatts: i16,
    /// `indexStruct->indkey.values[0 .. indnatts]` — the key column attnums.
    pub indkey: Vec<i16>,
}

/// One equality qual to add to the match-merge FULL JOIN ON clause, fully
/// resolved by the runtime from a usable unique index column. The in-crate loop
/// applies the de-dup (`opUsedForQual`) and emits the qual text via
/// `generate_operator_clause`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchMergeQual {
    /// `attnum` (1-based) of the matview column this qual references.
    pub attnum: i32,
    /// The equality operator OID (`get_opfamily_member_for_cmptype(...,
    /// COMPARE_EQ)`).
    pub op: Oid,
    /// `attr->atttypid` — the column's type (left and right type of the clause).
    pub attrtype: Oid,
    /// `quote_qualified_identifier("newdata", NameStr(attr->attname))`.
    pub leftop: String,
    /// `quote_qualified_identifier("mv", NameStr(attr->attname))`.
    pub rightop: String,
}

