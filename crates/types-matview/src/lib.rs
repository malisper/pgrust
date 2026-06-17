//! Signature vocabulary for `backend/commands/matview.c` (REFRESH MATERIALIZED
//! VIEW + the CREATE MATERIALIZED VIEW populate path).
//!
//! This crate holds the parse-node (`RefreshMatViewStmt`), command-completion
//! (`QueryCompletion`/`CommandTag`), and matview-specific read-out value types
//! the matview driver branches on, plus the opaque handles for objects owned by
//! the not-yet-ported executor / planner / rewriter (`Query *`, `PlannedStmt *`,
//! `QueryDesc *`). Those C
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

/// `typedef enum CommandTag` (`tcop/cmdtag.h`), the statement command-tag
/// enumerator carried by value. Canonically defined in `types_core` (shared
/// with the parser/plancache layers); the `UNKNOWN` /
/// `REFRESH_MATERIALIZED_VIEW` / `SELECT` associated constants live there.
pub use types_core::cmdtag::CommandTag;

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

// The `DR_transientrel` `DestReceiver` and the `TupleTableSlot`/`TupleDesc` the
// executor hands its callbacks are no longer modeled here: the receiver is owned
// in-crate by `backend-commands-matview` and registered into the
// `backend-tcop-dest` value-router (mirroring `createas.c`'s `DR_intorel`), so its
// callbacks take the real `types_nodes::tuptable::SlotData` /
// `types_tuple::heaptuple::TupleDescData` directly.

// ---------------------------------------------------------------------------
// Read-out value bundles the in-crate logic branches on.
// ---------------------------------------------------------------------------

/// The matview's rewrite-rule shape that `RefreshMatViewByOid` validates
/// (matview.c 211-262). The `rd_rel->...` fields are read off the real open
/// [`Relation`] in-crate; only the `rd_rules` rewrite-rule inspection lives here,
/// because `RelationData` does not model `rd_rules` (the RuleLock-carrier
/// keystone). The relcache owner reports this from the open matview handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatViewRuleInfo {
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

