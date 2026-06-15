//! Planner path-graph vocabulary (`nodes/pathnodes.h`), arena-shaped, trimmed to
//! what the join-path enumerator (`optimizer/path/joinpath.c`) and its sibling
//! optimizer crates consume.
//!
//! # Arena + handle model
//!
//! In C the planner is pure pointer manipulation: `RelOptInfo *`, `Path *`, and
//! `RestrictInfo *` are densely aliased — shared across many lists and
//! back-links and mutated by pointer identity (`add_path` rewrites a joinrel's
//! `pathlist`; the same `Path *` is both in `pathlist` and in
//! `cheapest_total_path`). An owned `Box`/`Vec` tree cannot represent that
//! sharing without `Rc`/`RefCell` (forbidden) or identity-breaking deep clones.
//!
//! So the four aliasing graph types live in per-query *arenas* owned by
//! [`PlannerInfo`]: a [`RelId`]/[`PathId`]/[`RinfoId`]/[`EcId`] is a `Copy` index
//! into the matching arena, and `root.rel(id)` / `root.path(id)` /
//! `root.rinfo(id)` recover the node. Identity is preserved — the same `PathId`
//! in a rel's `pathlist` and in its `cheapest_total_path` denotes one arena
//! slot. The arenas only grow within a planner run (the C planner never frees
//! mid-run), so a bare `u32` index never dangles.
//!
//! This is a distinct vocabulary from `types_nodes::pathnodes` (the executor's
//! owned capability tree consumed by execAmi): that one models a `Path *` as an
//! owned `PathNode` tree for `ExecMaterializesOutput`-style recursion; this one
//! models the planner's mutable shared graph. They are different views of
//! `Path` for different subsystems and intentionally coexist.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

extern crate alloc;

pub mod optimizer_plan;

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::primitive::{
    AttrNumber, BlockNumber, Cardinality, Cost, Index, Selectivity,
};
pub use types_core::primitive::Oid;
pub use types_core::fmgr::FmgrInfo;
pub use types_nodes::nodes::NodeTag;
pub use types_nodes::primnodes::Expr;
pub use types_hash::hsearch::HTAB;

/* ==========================================================================
 * Custom join-search private state + the planner's saved-context token.
 * ======================================================================== */

/// `void *join_search_private` (`pathnodes.h`) — the callback-supplied private
/// context a custom join-search hook (e.g. GEQO) stashes in
/// [`PlannerInfo::join_search_private`]. There is no PG struct: it is an opaque
/// `void *`. The GEQO port threads its real state explicitly and only ever
/// nulls this field, so the value carries nothing here.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinSearchPrivate {}

/// `MemoryContext` (`utils/palloc.h`) — an opaque handle to a memory context,
/// the analogue of the C `MemoryContextData *`. Used by the GEQO private temp
/// context seams to carry the saved "old" context across the planner boundary;
/// the value is opaque here (this repo has no ambient current context, so the
/// owning planner-memory unit defines its contents).
#[derive(Debug, Default)]
pub struct MemoryContextData {}

/// `MemoryContext` — `MemoryContextData *`.
pub type PathnodesMemoryContext = Option<Box<MemoryContextData>>;

/* ==========================================================================
 * Relids — a planner relation-id set (`Bitmapset *`).
 *
 * The empty set is `None` (the planner convention; relnode.c's seam docs
 * spell this out). The set algebra (`bms_*`) is owned by the not-yet-ported
 * nodes/bitmapset.c + relnode.c and is reached through the `relids_*` seams,
 * so this type is an opaque owned value here: a word-vector planner bitmapset,
 * planner-context-lived (the C `palloc`ed `Bitmapset`).
 * ======================================================================== */

/// `struct derives_hash *ec_derives_hash` (equivclass.c) — an optional
/// `ECDerivesKey` (equivclass.c) — the lookup key for [`DerivesHash`]: the
/// (canonicalised) pair of [`EmId`] handles plus the originating EC. The C key
/// orders the two EM pointers by address; here the equivalent canonical order
/// is by [`EmId`] arena index (see `fill_ec_derives_key`). A constant-bearing
/// derived clause stores only the non-constant EM in `em2`, with `em1 = None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ECDerivesKey {
    /// canonical lower EM (or `None` for the const-EM lookup case).
    pub em1: Option<EmId>,
    /// canonical higher EM (or the sole EM for the const case).
    pub em2: Option<EmId>,
    /// `EquivalenceClass *parent_ec` — the EC this derived clause is redundant
    /// with, if any (handle into `eq_classes`).
    pub parent_ec: Option<EcId>,
}

/// `ECDerivesEntry` (equivclass.c) — one open-addressing slot of a
/// [`DerivesHash`]: the simplehash status word, the key, and the cached
/// derived [`RinfoId`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ECDerivesEntry {
    /// simplehash status (`SH_STATUS_EMPTY`/`SH_STATUS_IN_USE`).
    pub status: u32,
    /// the (canonicalised) lookup key.
    pub key: ECDerivesKey,
    /// `RestrictInfo *rinfo` — the cached derived clause (handle into
    /// `rinfo_arena`).
    pub rinfo: Option<RinfoId>,
}

/// fast-lookup hash over an EquivalenceClass's derived RestrictInfos, holding
/// the same entries as `ec_derives_list`. It is a rebuildable cache
/// (`pg_node_attr(read_write_ignore)` in C): the consumer layer never inspects
/// it and a deep copy is meaningless. The owning equivclass unit (re)builds the
/// real table; it is modelled here as PostgreSQL's `simplehash` (open
/// addressing, linear probing, power-of-two sizing, 0.9 fill-factor grow), the
/// only observable behaviour being key→rinfo lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DerivesHash {
    /// number of allocated buckets (a power of two), or 0 if unbuilt.
    pub size: u64,
    /// `size - 1`, the bucket index mask.
    pub sizemask: u32,
    /// number of live entries.
    pub members: u32,
    /// resize once `members` reaches this (`size * fillfactor`).
    pub grow_threshold: u32,
    /// the bucket array.
    pub data: Vec<ECDerivesEntry>,
}

/// A clone-skipping wrapper for `subroot` — a shared `PlannerInfo *` in C
/// (`pg_node_attr` not copied; the planner never deep-copies a PlannerInfo).
/// [`RelOptInfo`] derives `Clone`, but `PlannerInfo` is not `Clone` (it owns the
/// non-cloneable [`HTAB`] join-rel hash). A clone of a `RelOptInfo` therefore
/// drops the sub-PlannerInfo (yields `None`), matching the C "pointer not
/// followed when copying" semantics; the consumer layer never reads `subroot`.
#[derive(Debug, Default)]
pub struct Subroot(pub Option<Box<PlannerInfo>>);

impl Clone for Subroot {
    fn clone(&self) -> Self {
        Subroot(None)
    }
}

impl core::ops::Deref for Subroot {
    type Target = Option<Box<PlannerInfo>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl core::ops::DerefMut for Subroot {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// `Bitmapset` (nodes/bitmapset.h), the planner relation-id-set value. The
/// `bms_*` operations live with their owning unit (reached via the `relids_*`
/// seams); this carries only the word storage so set-valued seams can hand back
/// fresh owned sets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bitmapset {
    /// `bitmapword words[]` — the bit storage.
    pub words: Vec<u64>,
}

/// `Relids` — a set of relation identifiers (`Bitmapset *`). Empty set = `None`.
pub type Relids = Option<Box<Bitmapset>>;

/* ==========================================================================
 * JoinType (nodes.h) — exact discriminant values.
 * ======================================================================== */

/// `JoinType` (nodes.h).
pub type JoinType = u32;
pub const JOIN_INNER: JoinType = 0;
pub const JOIN_LEFT: JoinType = 1;
pub const JOIN_FULL: JoinType = 2;
pub const JOIN_RIGHT: JoinType = 3;
pub const JOIN_SEMI: JoinType = 4;
pub const JOIN_ANTI: JoinType = 5;
pub const JOIN_RIGHT_SEMI: JoinType = 6;
pub const JOIN_RIGHT_ANTI: JoinType = 7;
pub const JOIN_UNIQUE_OUTER: JoinType = 8;
pub const JOIN_UNIQUE_INNER: JoinType = 9;

/// `RTEKind` (parsenodes.h) — the subset relnode-built rels carry.
pub type RTEKind = u32;
pub const RTE_RELATION: RTEKind = 0;

/// `RelOptKind` (pathnodes.h).
pub type RelOptKind = u32;
pub const RELOPT_BASEREL: RelOptKind = 0;
pub const RELOPT_JOINREL: RelOptKind = 1;
pub const RELOPT_OTHER_MEMBER_REL: RelOptKind = 2;
pub const RELOPT_OTHER_JOINREL: RelOptKind = 3;
pub const RELOPT_UPPER_REL: RelOptKind = 4;
pub const RELOPT_OTHER_UPPER_REL: RelOptKind = 5;

/// `CompareType` (`access/cmptype.h`) — used by `PathKey.pk_cmptype`.
pub type CompareType = i32;

/* ==========================================================================
 * QualCost (pathnodes.h)
 * ======================================================================== */

/// `QualCost` — startup + per-tuple cost of a clause.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct QualCost {
    pub startup: Cost,
    pub per_tuple: Cost,
}

/// `VolatileFunctionStatus` (pathnodes.h) — caches a node's
/// `contain_volatile_functions` result. `VOLATILITY_UNKNOWN` (the `Default`)
/// means "not yet computed". Modeled as the existing local `pub type X = u32` +
/// const convention; discriminant values match the C enum exactly.
pub type VolatileFunctionStatus = u32;
pub const VOLATILITY_UNKNOWN: VolatileFunctionStatus = 0;
pub const VOLATILITY_VOLATILE: VolatileFunctionStatus = 1;
pub const VOLATILITY_NOVOLATILE: VolatileFunctionStatus = 2;

/* ==========================================================================
 * PartitionSchemeData (pathnodes.h:612-628)
 *
 * If multiple relations are partitioned the same way they share one
 * `PartitionScheme`. It captures only the general partition-method properties
 * (strategy, column count, per-column type/collation/compare info), never the
 * specific bounds. `PlannerInfo::part_schemes` is the list of canonicalised
 * schemes; each `RelOptInfo::part_scheme` points at one (here: an owned shared
 * value, the planner never frees these mid-run).
 * ======================================================================== */

/// `PartitionSchemeData` (pathnodes.h) — the general partitioning properties
/// shared by like-partitioned relations. The per-column arrays
/// (`partopfamily`/`partopcintype`/`partcollation`/`parttyplen`/`parttypbyval`/
/// `partsupfunc`) all have `partnatts` entries.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionSchemeData {
    /// `char strategy` — partition strategy (LIST/RANGE/HASH).
    pub strategy: i8,
    /// `int16 partnatts` — number of partition attributes.
    pub partnatts: i16,
    /// `Oid *partopfamily` — OIDs of operator families.
    pub partopfamily: Vec<Oid>,
    /// `Oid *partopcintype` — OIDs of opclass declared input data types.
    pub partopcintype: Vec<Oid>,
    /// `Oid *partcollation` — OIDs of partitioning collations.
    pub partcollation: Vec<Oid>,
    /// `int16 *parttyplen` — cached partition key type lengths.
    pub parttyplen: Vec<i16>,
    /// `bool *parttypbyval` — cached partition key by-value flags.
    pub parttypbyval: Vec<bool>,
    /// `struct FmgrInfo *partsupfunc` — cached partition comparison functions.
    pub partsupfunc: Vec<FmgrInfo>,
}

/// `PartitionScheme` — `PartitionSchemeData *`; `None` if the rel isn't
/// partitioned.
pub type PartitionScheme = Option<Box<PartitionSchemeData>>;

/// `struct PartitionBoundInfoData` (partition/partbounds.h) — the specific
/// partition bounds of a partitioned relation. Opaque here: the bound algebra
/// lives with the partbounds unit, so this carries no fields at the consumer
/// layer (the analogue of the C `PartitionBoundInfoData *`, reached only by
/// presence in `RelOptInfo::boundinfo`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionBoundInfoData {}

/// `struct FdwRoutine` (foreign/fdwapi.h) — the FDW callback hook table for a
/// foreign table/join. Opaque here (the hooks are function pointers owned by the
/// FDW unit); presence in `RelOptInfo::fdwroutine` is what the planner tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FdwRoutine {}

/* ==========================================================================
 * Arena handles for the four aliasing planner graph types.
 *
 * Each is a `Copy` u32 index into a [`PlannerInfo`] arena; `Eq`/`Hash` so a
 * handle can key a set/map. There is no generation/ABA guard — the arena only
 * grows within a planner run (matching the C planner, which never frees
 * mid-run).
 * ======================================================================== */

/// Handle into [`PlannerInfo::rel_arena`] — the owned-tree analogue of a
/// `RelOptInfo *`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct RelId(pub u32);

/// Handle into [`PlannerInfo::path_arena`] — the analogue of a `Path *` (or any
/// path subtype; the arena element is [`PathNode`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct PathId(pub u32);

/// Handle into [`PlannerInfo::rinfo_arena`] — the analogue of a `RestrictInfo *`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct RinfoId(pub u32);

/// Handle into [`PlannerInfo::eq_classes`] — the analogue of an
/// `EquivalenceClass *`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct EcId(pub u32);

/// Handle for an expression `Node *` living in the optimizer/parse arena (a
/// `Var`/`PlaceHolderVar`/`OpExpr` arg/PathTarget expr). The join-path
/// enumerator only ever compares these by identity (the C `list_member`
/// pointer-equality on cache-key expressions) and passes them to the
/// node-walking seams (`contain_volatile_functions`/`pull_varnos`/…); it never
/// inspects the payload, so an opaque handle is the faithful model of the C
/// `Node *`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct NodeId(pub u32);

/// Handle into [`PlannerInfo::placeholder_list`] — the analogue of a
/// `PlaceHolderInfo *`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct PhInfoId(pub u32);

/// Handle into [`PlannerInfo::em_arena`] — the analogue of an
/// `EquivalenceMember *`. EMs are densely shared (an EC's `ec_members` /
/// `ec_childmembers` and each EM's `em_parent` all alias the same EM by
/// pointer); an [`EmId`] gives every shared member one identity-preserving home,
/// mirroring the [`RelId`]/[`PathId`]/[`RinfoId`]/[`EcId`] arena model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct EmId(pub u32);

impl RelId {
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}
impl PathId {
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}
impl RinfoId {
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}
impl EcId {
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

impl PhInfoId {
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

impl EmId {
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

impl NodeId {
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/* ==========================================================================
 * UpperRelationKind (pathnodes.h:69-81) — indexes PlannerInfo::upper_rels[] /
 * upper_targets[]. UPPERREL_FINAL must be last; it sizes the arrays.
 * ======================================================================== */

/// `UpperRelationKind` (pathnodes.h).
pub type UpperRelationKind = u32;
pub const UPPERREL_SETOP: UpperRelationKind = 0;
pub const UPPERREL_PARTIAL_GROUP_AGG: UpperRelationKind = 1;
pub const UPPERREL_GROUP_AGG: UpperRelationKind = 2;
pub const UPPERREL_WINDOW: UpperRelationKind = 3;
pub const UPPERREL_PARTIAL_DISTINCT: UpperRelationKind = 4;
pub const UPPERREL_DISTINCT: UpperRelationKind = 5;
pub const UPPERREL_ORDERED: UpperRelationKind = 6;
pub const UPPERREL_FINAL: UpperRelationKind = 7;
/// `UPPERREL_FINAL + 1` — the array length for `upper_rels`/`upper_targets`.
pub const NUM_UPPERREL_KINDS: usize = (UPPERREL_FINAL as usize) + 1;

/* ==========================================================================
 * Opaque handles for parser/global state the planner threads but does not
 * inspect at this consumer layer (the owning units define the payloads).
 * ======================================================================== */

/// `Query *parse` — the Query being planned. The planner threads it but the
/// join-path layer never inspects the parse tree directly here, so it is an
/// opaque handle into the parser's owned Query store, the analogue of the C
/// `Query *` (the unported parser/parse-analysis unit owns the value).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct QueryId(pub u32);

/// `RangeTblEntry *` — an entry of `simple_rte_array`. Opaque handle into the
/// parser's owned rangetable; the planner indexes it by RT index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RangeTblEntryId(pub u32);

/* ==========================================================================
 * PlannerGlobal (pathnodes.h:95-182) — global state for an entire planner
 * invocation, shared across all sub-Query levels. Trimmed to the scalar/handle
 * fields a consumer reads; the node-list payloads (subplans/finalrtable/…) are
 * owned by their producing units and carried as opaque `NodeId` handles.
 * ======================================================================== */

/// `PlannerGlobal` — global information for one planner run.
#[derive(Clone, Debug, Default)]
pub struct PlannerGlobal {
    /// `List *subplans` — Plans for SubPlan nodes (opaque node handles).
    pub subplans: Vec<NodeId>,
    /// `List *subpaths` — Paths the SubPlan Plans were made from.
    pub subpaths: Vec<NodeId>,
    /// `List *subroots` — PlannerInfos for SubPlan nodes.
    pub subroots: Vec<NodeId>,
    /// `Bitmapset *rewindPlanIDs`.
    pub rewind_plan_ids: Relids,
    /// `List *finalrtable`.
    pub finalrtable: Vec<NodeId>,
    /// `Bitmapset *allRelids`.
    pub all_relids: Relids,
    /// `Bitmapset *prunableRelids`.
    pub prunable_relids: Relids,
    /// `List *finalrteperminfos`.
    pub finalrteperminfos: Vec<NodeId>,
    /// `List *finalrowmarks`.
    pub finalrowmarks: Vec<NodeId>,
    /// `List *resultRelations`.
    pub result_relations: Vec<i32>,
    /// `List *appendRelations`.
    pub append_relations: Vec<NodeId>,
    /// `List *partPruneInfos`.
    pub part_prune_infos: Vec<NodeId>,
    /// `List *relationOids`.
    pub relation_oids: Vec<Oid>,
    /// `List *invalItems`.
    pub inval_items: Vec<NodeId>,
    /// `List *paramExecTypes`.
    pub param_exec_types: Vec<Oid>,
    /// `Index lastPHId`.
    pub last_ph_id: Index,
    /// `Index lastRowMarkId`.
    pub last_row_mark_id: Index,
    /// `int lastPlanNodeId`.
    pub last_plan_node_id: i32,
    pub transient_plan: bool,
    pub depends_on_role: bool,
    pub parallel_mode_ok: bool,
    pub parallel_mode_needed: bool,
    /// `char maxParallelHazard`.
    pub max_parallel_hazard: i8,
}

/* ==========================================================================
 * JoinDomain (pathnodes.h) — scope of EquivalenceClass deductions, referenced
 * by EquivalenceMember::em_jdomain.
 * ======================================================================== */

/// `JoinDomain` — the scope within which an EC deduction is valid.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinDomain {
    /// `Relids jd_relids` — the rels this domain spans.
    pub jd_relids: Relids,
}

/* ==========================================================================
 * AppendRelInfo (parsenodes.h) — relates an append-relation parent to one of
 * its children, used to translate parent Vars into child references. The C
 * `RelOptInfo`-adjacent planner code reads `child_relid`/`parent_relid` etc; the
 * `translated_vars` expressions are opaque expr-node handles into the optimizer
 * arena. `PlannerInfo::append_rel_array` carries one of these per child relid.
 * ======================================================================== */

/// `AppendRelInfo` — one parent/child append relationship (parsenodes.h).
#[derive(Clone, Debug, Default)]
pub struct AppendRelInfo {
    /// `Index parent_relid` — RT index of the append parent rel.
    pub parent_relid: Index,
    /// `Index child_relid` — RT index of the append child rel.
    pub child_relid: Index,
    /// `Oid parent_reltype` — OID of the parent's composite type (InvalidOid for
    /// a UNION-ALL appendrel).
    pub parent_reltype: Oid,
    /// `Oid child_reltype` — OID of the child's composite type (InvalidOid for a
    /// UNION-ALL appendrel).
    pub child_reltype: Oid,
    /// `List *translated_vars` — N'th element is the child column (a Var or
    /// expression) for the N'th parent column; opaque expr-node handles. A NULL
    /// element (dropped parent column) is `NodeId::default()` (0).
    pub translated_vars: Vec<NodeId>,
    /// `int num_child_cols` — length of `parent_colnos`.
    pub num_child_cols: i32,
    /// `AttrNumber *parent_colnos` — per child column, the 1-based parent column
    /// number (0 if dropped or absent in parent). `num_child_cols` entries.
    pub parent_colnos: Vec<i16>,
    /// `Oid parent_reloid` — OID of the parent relation (InvalidOid for UNION
    /// ALL); used only for error messages.
    pub parent_reloid: Oid,
}

/* ==========================================================================
 * IndexOptInfo (pathnodes.h:1137-1239) — per-index planning state, built by
 * plancat.c. This is the FULL planner producer type (distinct from the
 * trimmed executor-side IndexOptInfo in types_nodes::pathnodes). The
 * `indexkeys[]`/`canreturn[]` arrays have `ncolumns` entries; the
 * `indexcollations[]`/`opfamily[]`/`opcintype[]`/`sortopfamily[]`/
 * `reverse_sort[]`/`nulls_first[]` arrays have `nkeycolumns` entries.
 * ======================================================================== */

/// `IndexOptInfo` — per-index information for planning/optimization. Expression
/// columns (`indexprs`/`indpred`/`indextlist`) are carried as opaque `NodeId`
/// handles (the owning optimizer-arena crate holds the expression trees); the
/// AM cost-estimate function pointer (`amcostestimate`) is not modeled at this
/// lifetime-free consumer layer.
#[derive(Clone, Debug, Default)]
pub struct IndexOptInfo {
    /// `Oid indexoid` — OID of the index relation.
    pub indexoid: Oid,
    /// `Oid reltablespace` — tablespace of the index (not the table).
    pub reltablespace: Oid,
    /// `RelOptInfo *rel` — back-link to the index's table (handle into
    /// `rel_arena`).
    pub rel: Option<RelId>,
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    /// `int tree_height` — index tree height, or -1 if unknown.
    pub tree_height: i32,
    /// `int ncolumns` — number of columns in the index.
    pub ncolumns: i32,
    /// `int nkeycolumns` — number of key columns in the index.
    pub nkeycolumns: i32,
    /// `int *indexkeys` — table column numbers (0 = expression column).
    pub indexkeys: Vec<i32>,
    pub indexcollations: Vec<Oid>,
    pub opfamily: Vec<Oid>,
    pub opcintype: Vec<Oid>,
    /// `Oid *sortopfamily` — btree opfamilies if orderable; empty if partitioned.
    pub sortopfamily: Vec<Oid>,
    pub reverse_sort: Vec<bool>,
    pub nulls_first: Vec<bool>,
    /// `bool *canreturn` — which index cols can be returned in an index-only
    /// scan.
    pub canreturn: Vec<bool>,
    /// `Oid relam` — OID of the access method (pg_am).
    pub relam: Oid,
    /// `List *indexprs` — expressions for non-simple index columns (opaque expr
    /// node handles).
    pub indexprs: Vec<NodeId>,
    /// `List *indpred` — predicate if a partial index, else empty.
    pub indpred: Vec<NodeId>,
    /// `List *indextlist` — targetlist representing index columns.
    pub indextlist: Vec<NodeId>,
    /// `List *indrestrictinfo` — parent's baserestrictinfo less predicate-implied
    /// conditions (handles into `rinfo_arena`).
    pub indrestrictinfo: Vec<RinfoId>,
    /// `bool predOK` — true if the index predicate matches the query.
    #[allow(non_snake_case)]
    pub predOK: bool,
    pub unique: bool,
    pub nullsnotdistinct: bool,
    pub immediate: bool,
    pub hypothetical: bool,
    pub amcanorderbyop: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amhasgettuple: bool,
    pub amhasgetbitmap: bool,
    pub amcanparallel: bool,
    pub amcanmarkpos: bool,
}

/* ==========================================================================
 * PathKey / PathTarget / ParamPathInfo (pathnodes.h)
 * ======================================================================== */

/// `PathKey` — represents a sort ordering. Trimmed to the fields the path
/// enumerator and pathkey seams marshal; the generating `EquivalenceClass` is a
/// handle into `PlannerInfo::eq_classes`.
#[derive(Clone, Debug, PartialEq)]
pub struct PathKey {
    /// `EquivalenceClass *pk_eclass` — the value that is ordered.
    pub pk_eclass: Option<EcId>,
    pub pk_opfamily: Oid,
    pub pk_cmptype: CompareType,
    pub pk_nulls_first: bool,
}

/// `PathTarget` — the output columns a Path computes (pathnodes.h). `exprs` is
/// the targetlist as opaque expression-node handles (the owning optimizer arena
/// holds the `Expr` trees); `sortgrouprefs` is the per-column sort/group ref (0
/// if none); cost/width are the consumed scalars.
#[derive(Clone, Debug, Default)]
pub struct PathTarget {
    /// `List *exprs` — expressions to be computed (one per output column), as
    /// opaque expr-node handles into the optimizer arena.
    pub exprs: Vec<NodeId>,
    /// `Index *sortgrouprefs` — sort/group refs, or empty if none. One entry per
    /// `exprs` element (`exprs`-length array in C; 0 = "no ref").
    pub sortgrouprefs: Vec<u32>,
    /// `QualCost cost` — cost of evaluating the expressions.
    pub cost: QualCost,
    /// `int width` — estimated avg width of result tuples.
    pub width: i32,
    /// `VolatileFunctionStatus has_volatile_expr` — whether `exprs` contains any
    /// volatile functions. Modeled as the C enum value (`VOLATILITY_UNKNOWN` = 0
    /// until computed); see [`VolatileFunctionStatus`].
    pub has_volatile_expr: VolatileFunctionStatus,
}

/// `ParamPathInfo` — shared parameterization info for a set of paths. Trimmed to
/// the fields `PATH_REQ_OUTER` and the parameterized-path machinery read.
#[derive(Clone, Debug)]
pub struct ParamPathInfo {
    pub ppi_req_outer: Relids,
    pub ppi_rows: Cardinality,
    /// join clauses available from outer rels — handles into `rinfo_arena`.
    pub ppi_clauses: Vec<RinfoId>,
    /// set of rinfo_serials of the parameterization's clauses (the C
    /// `Bitmapset *ppi_serials`); read by the memoize inner_unique guard.
    pub ppi_serials: Relids,
}

/* ==========================================================================
 * Path-subtype supporting enums (nodes.h / access/sdir.h / pathnodes.h).
 *
 * These mirror the C enums the path constructors store on upper/scan paths.
 * They are modeled as the existing local `pub type X = i32/u32` + const
 * convention (cf. JoinType/RelOptKind above) so the crate stays self-contained,
 * no_std and lifetime-free; the discriminant values match the C enums exactly.
 * ======================================================================== */

/// `ScanDirection` (access/sdir.h) — used by `IndexPath.indexscandir`.
pub type ScanDirection = i32;
pub const BackwardScanDirection: ScanDirection = -1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const ForwardScanDirection: ScanDirection = 1;

/// `CmdType` (nodes.h) — used by `ModifyTablePath.operation`.
pub type CmdType = u32;
pub const CMD_UNKNOWN: CmdType = 0;
pub const CMD_SELECT: CmdType = 1;
pub const CMD_UPDATE: CmdType = 2;
pub const CMD_INSERT: CmdType = 3;
pub const CMD_DELETE: CmdType = 4;
pub const CMD_MERGE: CmdType = 5;
pub const CMD_UTILITY: CmdType = 6;
pub const CMD_NOTHING: CmdType = 7;

/// `AggStrategy` (nodes.h) — used by `AggPath`/`GroupingSetsPath`.
pub type AggStrategy = u32;
/// simple agg across all input rows.
pub const AGG_PLAIN: AggStrategy = 0;
/// grouped agg, input must be sorted.
pub const AGG_SORTED: AggStrategy = 1;
/// grouped agg, use internal hashtable.
pub const AGG_HASHED: AggStrategy = 2;
/// grouped agg, hash and sort both used.
pub const AGG_MIXED: AggStrategy = 3;

/// `AggSplit` (nodes.h) — bitmask of `AGGSPLITOP_*`; used by `AggPath`.
pub type AggSplit = u32;
/// substitute combinefn for transfn.
pub const AGGSPLITOP_COMBINE: AggSplit = 0x01;
/// skip finalfn, return state as-is.
pub const AGGSPLITOP_SKIPFINAL: AggSplit = 0x02;
/// apply serialfn to output.
pub const AGGSPLITOP_SERIALIZE: AggSplit = 0x04;
/// apply deserialfn to input.
pub const AGGSPLITOP_DESERIALIZE: AggSplit = 0x08;
/// Basic, non-split aggregation.
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
/// Initial phase of partial aggregation, with serialization.
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE;
/// Final phase of partial aggregation, with deserialization.
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE;

/// `SetOpCmd` (nodes.h) — used by `SetOpPath.cmd`.
pub type SetOpCmd = u32;
pub const SETOPCMD_INTERSECT: SetOpCmd = 0;
pub const SETOPCMD_INTERSECT_ALL: SetOpCmd = 1;
pub const SETOPCMD_EXCEPT: SetOpCmd = 2;
pub const SETOPCMD_EXCEPT_ALL: SetOpCmd = 3;

/// `SetOpStrategy` (nodes.h) — used by `SetOpPath.strategy`.
pub type SetOpStrategy = u32;
/// input must be sorted.
pub const SETOP_SORTED: SetOpStrategy = 0;
/// use internal hashtable.
pub const SETOP_HASHED: SetOpStrategy = 1;

/// `LimitOption` (nodes.h) — used by `LimitPath.limit_option`.
pub type LimitOption = u32;
/// FETCH FIRST... ONLY.
pub const LIMIT_OPTION_COUNT: LimitOption = 0;
/// FETCH FIRST... WITH TIES.
pub const LIMIT_OPTION_WITH_TIES: LimitOption = 1;

/// `UniquePathMethod` (pathnodes.h) — used by `UniquePath.umethod`.
pub type UniquePathMethod = u32;
/// input is known unique already.
pub const UNIQUE_PATH_NOOP: UniquePathMethod = 0;
/// use hashing.
pub const UNIQUE_PATH_HASH: UniquePathMethod = 1;
/// use sorting.
pub const UNIQUE_PATH_SORT: UniquePathMethod = 2;

/// `uint32 CUSTOMPATH_*` flags (extensible.h) — mask stored in `CustomPath.flags`.
pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: u32 = 0x0001;
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: u32 = 0x0002;
pub const CUSTOMPATH_SUPPORT_PROJECTION: u32 = 0x0004;

/* ==========================================================================
 * Path and its join subtypes (pathnodes.h)
 * ======================================================================== */

/// `Path` — base path node; first member of every path subtype. Trimmed to the
/// fields joinpath reads via `PathNode::base()`.
#[derive(Clone, Debug)]
pub struct Path {
    /// `NodeTag type` — the path node's own tag.
    pub type_: NodeTag,
    /// `NodeTag pathtype` — the plan-node type this path would produce.
    pub pathtype: NodeTag,
    /// the relation this path can build (handle into `rel_arena`). Non-`Option`,
    /// matching the non-null `RelOptInfo *parent` in C.
    pub parent: RelId,
    pub pathtarget: Option<Box<PathTarget>>,
    pub param_info: Option<Box<ParamPathInfo>>,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub parallel_workers: i32,
    pub rows: Cardinality,
    pub disabled_nodes: i32,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub pathkeys: Vec<PathKey>,
}

/// `JoinPath` — fields shared by all join paths.
#[derive(Clone, Debug)]
pub struct JoinPath {
    pub path: Path,
    pub jointype: JoinType,
    pub inner_unique: bool,
    /// the outer subpath (handle into `path_arena`).
    pub outerjoinpath: Option<PathId>,
    /// the inner subpath (handle into `path_arena`).
    pub innerjoinpath: Option<PathId>,
    /// RestrictInfos to apply to join — handles into `rinfo_arena`.
    pub joinrestrictinfo: Vec<RinfoId>,
}

/// `NestPath` — a nested-loop join.
#[derive(Clone, Debug)]
pub struct NestPath {
    pub jpath: JoinPath,
}

/// `MergePath` — a merge join.
#[derive(Clone, Debug)]
pub struct MergePath {
    pub jpath: JoinPath,
    /// join clauses to be used for merge — handles into `rinfo_arena`.
    pub path_mergeclauses: Vec<RinfoId>,
    pub outersortkeys: Vec<PathKey>,
    pub innersortkeys: Vec<PathKey>,
    pub outer_presorted_keys: i32,
    pub skip_mark_restore: bool,
    pub materialize_inner: bool,
}

/// `HashPath` — a hash join.
#[derive(Clone, Debug)]
pub struct HashPath {
    pub jpath: JoinPath,
    /// join clauses used for hashing — handles into `rinfo_arena`.
    pub path_hashclauses: Vec<RinfoId>,
    pub num_batches: i32,
    pub inner_rows_total: Cardinality,
}

/* --------------------------------------------------------------------------
 * Scan-path subtypes (pathnodes.h:1842-2047). Subpaths reference other paths
 * by [`PathId`] (handle into `path_arena`), mirroring the join variants'
 * `outerjoinpath`/`innerjoinpath`. Bare clause/expr lists are opaque [`NodeId`]
 * handles; RestrictInfo lists are [`RinfoId`] handles.
 * ------------------------------------------------------------------------ */

/// `IndexClause` — how one restriction is applied to a particular index.
#[derive(Clone, Debug)]
pub struct IndexClause {
    /// `RestrictInfo *rinfo` — original restriction or join clause (handle into
    /// `rinfo_arena`).
    pub rinfo: Option<RinfoId>,
    /// `List *indexquals` — indexqual(s) derived from it (handles into
    /// `rinfo_arena`).
    pub indexquals: Vec<RinfoId>,
    /// `bool lossy` — are indexquals a lossy version of the clause?
    pub lossy: bool,
    /// `AttrNumber indexcol` — index column the clause uses (zero-based).
    pub indexcol: AttrNumber,
    /// `List *indexcols` — multiple index columns, if a RowCompare.
    pub indexcols: Vec<AttrNumber>,
}

/// `IndexPath` — an index scan over a single index (regular or index-only).
#[derive(Clone, Debug)]
pub struct IndexPath {
    pub path: Path,
    /// `IndexOptInfo *indexinfo` — the index to be scanned.
    pub indexinfo: Option<Box<IndexOptInfo>>,
    pub indexclauses: Vec<IndexClause>,
    /// `List *indexorderbys` — ORDER BY expressions usable as ordering ops
    /// (bare expr node handles).
    pub indexorderbys: Vec<NodeId>,
    /// `List *indexorderbycols` — index column numbers for each orderby.
    pub indexorderbycols: Vec<i32>,
    pub indexscandir: ScanDirection,
    pub indextotalcost: Cost,
    pub indexselectivity: Selectivity,
}

/// `BitmapHeapPath` — heap scan driven by a TID bitmap.
#[derive(Clone, Debug)]
pub struct BitmapHeapPath {
    pub path: Path,
    /// `Path *bitmapqual` — IndexPath, BitmapAndPath, or BitmapOrPath (handle
    /// into `path_arena`).
    pub bitmapqual: Option<PathId>,
}

/// `BitmapAndPath` — a BitmapAnd plan node (only under a BitmapHeapPath).
#[derive(Clone, Debug)]
pub struct BitmapAndPath {
    pub path: Path,
    /// `List *bitmapquals` — IndexPaths and BitmapOrPaths (handles into
    /// `path_arena`).
    pub bitmapquals: Vec<PathId>,
    pub bitmapselectivity: Selectivity,
}

/// `BitmapOrPath` — a BitmapOr plan node (only under a BitmapHeapPath).
#[derive(Clone, Debug)]
pub struct BitmapOrPath {
    pub path: Path,
    /// `List *bitmapquals` — IndexPaths and BitmapAndPaths (handles into
    /// `path_arena`).
    pub bitmapquals: Vec<PathId>,
    pub bitmapselectivity: Selectivity,
}

/// `TidPath` — a scan by TID.
#[derive(Clone, Debug)]
pub struct TidPath {
    pub path: Path,
    /// `List *tidquals` — qual(s) involving CTID = something (bare expr handles).
    pub tidquals: Vec<NodeId>,
}

/// `TidRangePath` — a scan by a contiguous range of TIDs.
#[derive(Clone, Debug)]
pub struct TidRangePath {
    pub path: Path,
    /// `List *tidrangequals` — CTID relop pseudoconstant quals (bare expr
    /// handles).
    pub tidrangequals: Vec<NodeId>,
}

/// `SubqueryScanPath` — a scan of an unflattened subquery-in-FROM.
#[derive(Clone, Debug)]
pub struct SubqueryScanPath {
    pub path: Path,
    /// `Path *subpath` — path representing subquery execution (handle into
    /// `path_arena`).
    pub subpath: Option<PathId>,
}

/// `ForeignPath` — a scan of a foreign table/join/upper-relation.
#[derive(Clone, Debug)]
pub struct ForeignPath {
    pub path: Path,
    /// `Path *fdw_outerpath` — outer path for a foreign join (handle into
    /// `path_arena`).
    pub fdw_outerpath: Option<PathId>,
    /// `List *fdw_restrictinfo` — RestrictInfos to apply to a foreign join
    /// (handles into `rinfo_arena`).
    pub fdw_restrictinfo: Vec<RinfoId>,
    /// `List *fdw_private` — FDW private data (opaque node handles).
    pub fdw_private: Vec<NodeId>,
}

/// `CustomPath` — a scan/join supplied by an out-of-core extension. The
/// `methods` vtable is a function-pointer table owned by the extension and is
/// not modeled at this lifetime-free layer (presence/flags are what core reads).
#[derive(Clone, Debug)]
pub struct CustomPath {
    pub path: Path,
    /// `uint32 flags` — mask of `CUSTOMPATH_*` flags.
    pub flags: u32,
    /// `List *custom_paths` — child Path nodes, if any (handles into
    /// `path_arena`).
    pub custom_paths: Vec<PathId>,
    /// `List *custom_restrictinfo` — RestrictInfos to apply to a custom join
    /// (handles into `rinfo_arena`).
    pub custom_restrictinfo: Vec<RinfoId>,
    /// `List *custom_private` — extension private data (opaque node handles).
    pub custom_private: Vec<NodeId>,
}

/* --------------------------------------------------------------------------
 * Append / upper / misc path subtypes (pathnodes.h:2064-2547).
 * ------------------------------------------------------------------------ */

/// `AppendPath` — successive execution of several member plans.
#[derive(Clone, Debug)]
pub struct AppendPath {
    pub path: Path,
    /// `List *subpaths` — component Paths (handles into `path_arena`).
    pub subpaths: Vec<PathId>,
    /// `int first_partial_path` — index of first partial path in `subpaths`.
    pub first_partial_path: i32,
    /// `Cardinality limit_tuples` — hard limit on output tuples, or -1.
    pub limit_tuples: Cardinality,
}

/// `MergeAppendPath` — merge of sorted results from member plans.
#[derive(Clone, Debug)]
pub struct MergeAppendPath {
    pub path: Path,
    /// `List *subpaths` — component Paths (handles into `path_arena`).
    pub subpaths: Vec<PathId>,
    /// `Cardinality limit_tuples` — hard limit on output tuples, or -1.
    pub limit_tuples: Cardinality,
}

/// `GroupResultPath` — a Result node for a degenerate GROUP BY.
#[derive(Clone, Debug)]
pub struct GroupResultPath {
    pub path: Path,
    /// `List *quals` — bare clauses (not RestrictInfos), expr node handles.
    pub quals: Vec<NodeId>,
}

/// `MaterialPath` — a Material node caching its subpath's output.
#[derive(Clone, Debug)]
pub struct MaterialPath {
    pub path: Path,
    /// `Path *subpath` — handle into `path_arena`.
    pub subpath: Option<PathId>,
}

/// `MemoizePath` — a Memoize node caching tuples from a parameterized subpath.
#[derive(Clone, Debug)]
pub struct MemoizePath {
    pub path: Path,
    /// `Path *subpath` — outerpath to cache tuples from (handle into
    /// `path_arena`).
    pub subpath: Option<PathId>,
    /// `List *hash_operators` — OIDs of hash equality ops for cache keys.
    pub hash_operators: Vec<Oid>,
    /// `List *param_exprs` — expressions that are cache keys (expr handles).
    pub param_exprs: Vec<NodeId>,
    /// `bool singlerow` — mark the cache entry complete after the first record?
    pub singlerow: bool,
    /// `bool binary_mode` — compare cache keys bit-by-bit?
    pub binary_mode: bool,
    /// `Cardinality calls` — expected number of rescans.
    pub calls: Cardinality,
    /// `uint32 est_entries` — max entries expected to fit, or 0 if unknown.
    pub est_entries: u32,
}

/// `UniquePath` — elimination of distinct rows from its subpath.
#[derive(Clone, Debug)]
pub struct UniquePath {
    pub path: Path,
    /// `Path *subpath` — handle into `path_arena`.
    pub subpath: Option<PathId>,
    pub umethod: UniquePathMethod,
    /// `List *in_operators` — equality operators of the IN clause (OIDs).
    pub in_operators: Vec<Oid>,
    /// `List *uniq_exprs` — expressions to be made unique (expr handles).
    pub uniq_exprs: Vec<NodeId>,
}

/// `GatherPath` — runs copies of a plan in parallel and collects results.
#[derive(Clone, Debug)]
pub struct GatherPath {
    pub path: Path,
    /// `Path *subpath` — path for each worker (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `bool single_copy` — don't execute path more than once.
    pub single_copy: bool,
    /// `int num_workers` — number of workers sought to help.
    pub num_workers: i32,
}

/// `GatherMergePath` — parallel collect preserving common sort order.
#[derive(Clone, Debug)]
pub struct GatherMergePath {
    pub path: Path,
    /// `Path *subpath` — path for each worker (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `int num_workers` — number of workers sought to help.
    pub num_workers: i32,
}

/// `ProjectionPath` — a projection (targetlist computation) step.
#[derive(Clone, Debug)]
pub struct ProjectionPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `bool dummypp` — true if no separate Result is needed.
    pub dummypp: bool,
}

/// `ProjectSetPath` — evaluation of a tlist containing set-returning functions.
#[derive(Clone, Debug)]
pub struct ProjectSetPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
}

/// `SortPath` — an explicit sort step (keys are `path.pathkeys`).
#[derive(Clone, Debug)]
pub struct SortPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
}

/// `IncrementalSortPath` — an incremental sort step (leading keys presorted).
#[derive(Clone, Debug)]
pub struct IncrementalSortPath {
    pub spath: SortPath,
    /// `int nPresortedCols` — number of presorted columns.
    #[allow(non_snake_case)]
    pub nPresortedCols: i32,
}

/// `GroupPath` — grouping of presorted input.
#[derive(Clone, Debug)]
pub struct GroupPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `List *groupClause` — SortGroupClauses (opaque node handles).
    #[allow(non_snake_case)]
    pub groupClause: Vec<NodeId>,
    /// `List *qual` — HAVING quals, if any (bare expr node handles).
    pub qual: Vec<NodeId>,
}

/// `UpperUniquePath` — adjacent-duplicate removal in presorted input.
#[derive(Clone, Debug)]
pub struct UpperUniquePath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `int numkeys` — number of pathkey columns to compare.
    pub numkeys: i32,
}

/// `AggPath` — generic computation of aggregate functions.
#[derive(Clone, Debug)]
pub struct AggPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    pub aggstrategy: AggStrategy,
    pub aggsplit: AggSplit,
    /// `Cardinality numGroups` — estimated number of groups in input.
    #[allow(non_snake_case)]
    pub numGroups: Cardinality,
    /// `uint64 transitionSpace` — for pass-by-ref transition data.
    #[allow(non_snake_case)]
    pub transitionSpace: u64,
    /// `List *groupClause` — SortGroupClauses (opaque node handles).
    #[allow(non_snake_case)]
    pub groupClause: Vec<NodeId>,
    /// `List *qual` — HAVING quals, if any (bare expr node handles).
    pub qual: Vec<NodeId>,
}

/// `GroupingSetData` — one grouping set (pathnodes.h:2410).
#[derive(Clone, Debug, Default)]
pub struct GroupingSetData {
    /// `List *set` — grouping set as a list of sortgrouprefs.
    pub set: Vec<Index>,
    /// `Cardinality numGroups` — est. number of result groups.
    #[allow(non_snake_case)]
    pub numGroups: Cardinality,
}

/// `RollupData` — one rollup specification (pathnodes.h:2419).
#[derive(Clone, Debug, Default)]
pub struct RollupData {
    /// `List *groupClause` — applicable subset of parse->groupClause (handles).
    #[allow(non_snake_case)]
    pub groupClause: Vec<NodeId>,
    /// `List *gsets` — lists of integer indexes into `groupClause`.
    pub gsets: Vec<Vec<i32>>,
    /// `List *gsets_data` — GroupingSetData entries.
    pub gsets_data: Vec<GroupingSetData>,
    /// `Cardinality numGroups` — est. number of result groups.
    #[allow(non_snake_case)]
    pub numGroups: Cardinality,
    pub hashable: bool,
    pub is_hashed: bool,
}

/// `GroupingSetsPath` — a GROUPING SETS aggregation.
#[derive(Clone, Debug)]
pub struct GroupingSetsPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    pub aggstrategy: AggStrategy,
    /// `List *rollups` — RollupData entries.
    pub rollups: Vec<RollupData>,
    /// `List *qual` — HAVING quals, if any (bare expr node handles).
    pub qual: Vec<NodeId>,
    /// `uint64 transitionSpace` — for pass-by-ref transition data.
    #[allow(non_snake_case)]
    pub transitionSpace: u64,
}

/// `MinMaxAggInfo` — annotation for one MIN/MAX aggregate computed from an
/// index (pathnodes.h). The expression `target` / output `param` are bare expr
/// node handles; the per-agg sub-plan `path` is a handle into `path_arena`; the
/// modified sub-`root` is not carried at this consumer layer (the C field is
/// `read_write_ignore`).
#[derive(Clone, Debug, Default)]
pub struct MinMaxAggInfo {
    /// `Oid aggfnoid` — pg_proc OID of the aggregate.
    pub aggfnoid: Oid,
    /// `Oid aggsortop` — OID of its sort operator.
    pub aggsortop: Oid,
    /// `Expr *target` — expression we are aggregating on (expr handle).
    pub target: NodeId,
    /// `Path *path` — access path for the subquery (handle into `path_arena`).
    pub path: Option<PathId>,
    /// `Cost pathcost` — estimated cost to fetch the first row.
    pub pathcost: Cost,
    /// `Param *param` — param for the subplan's output (expr handle).
    pub param: NodeId,
}

/// `MinMaxAggPath` — computation of MIN/MAX aggregates from indexes.
#[derive(Clone, Debug)]
pub struct MinMaxAggPath {
    pub path: Path,
    /// `List *mmaggregates` — MinMaxAggInfo entries.
    pub mmaggregates: Vec<MinMaxAggInfo>,
    /// `List *quals` — HAVING quals, if any (bare expr node handles).
    pub quals: Vec<NodeId>,
}

/// `WindowAggPath` — generic computation of window functions.
#[derive(Clone, Debug)]
pub struct WindowAggPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `WindowClause *winclause` — the WindowClause we'll use (opaque node
    /// handle).
    pub winclause: NodeId,
    /// `List *qual` — lower-level WindowAgg runconditions (expr handles).
    pub qual: Vec<NodeId>,
    /// `List *runCondition` — OpExpr list to short-circuit execution (handles).
    #[allow(non_snake_case)]
    pub runCondition: Vec<NodeId>,
    /// `bool topwindow` — true only for the WindowAgg closest to the plan root.
    pub topwindow: bool,
}

/// `SetOpPath` — a set operation (INTERSECT or EXCEPT).
#[derive(Clone, Debug)]
pub struct SetOpPath {
    pub path: Path,
    /// `Path *leftpath` — left input source (handle into `path_arena`).
    pub leftpath: Option<PathId>,
    /// `Path *rightpath` — right input source (handle into `path_arena`).
    pub rightpath: Option<PathId>,
    pub cmd: SetOpCmd,
    pub strategy: SetOpStrategy,
    /// `List *groupList` — SortGroupClauses identifying target cols (handles).
    #[allow(non_snake_case)]
    pub groupList: Vec<NodeId>,
    /// `Cardinality numGroups` — estimated number of groups in the left input.
    #[allow(non_snake_case)]
    pub numGroups: Cardinality,
}

/// `RecursiveUnionPath` — a recursive UNION node.
#[derive(Clone, Debug)]
pub struct RecursiveUnionPath {
    pub path: Path,
    /// `Path *leftpath` — left input source (handle into `path_arena`).
    pub leftpath: Option<PathId>,
    /// `Path *rightpath` — right input source (handle into `path_arena`).
    pub rightpath: Option<PathId>,
    /// `List *distinctList` — SortGroupClauses identifying target cols (handles).
    #[allow(non_snake_case)]
    pub distinctList: Vec<NodeId>,
    /// `int wtParam` — ID of the Param representing the work table.
    #[allow(non_snake_case)]
    pub wtParam: i32,
    /// `Cardinality numGroups` — estimated number of groups in input.
    #[allow(non_snake_case)]
    pub numGroups: Cardinality,
}

/// `LockRowsPath` — acquiring row locks for SELECT FOR UPDATE/SHARE.
#[derive(Clone, Debug)]
pub struct LockRowsPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `List *rowMarks` — PlanRowMarks (opaque node handles).
    #[allow(non_snake_case)]
    pub rowMarks: Vec<NodeId>,
    /// `int epqParam` — ID of the Param for EvalPlanQual re-eval.
    #[allow(non_snake_case)]
    pub epqParam: i32,
}

/// `ModifyTablePath` — INSERT/UPDATE/DELETE/MERGE.
#[derive(Clone, Debug)]
pub struct ModifyTablePath {
    pub path: Path,
    /// `Path *subpath` — Path producing source data (handle into `path_arena`).
    pub subpath: Option<PathId>,
    pub operation: CmdType,
    /// `bool canSetTag` — do we set the command tag/es_processed?
    #[allow(non_snake_case)]
    pub canSetTag: bool,
    /// `Index nominalRelation` — parent RT index for use of EXPLAIN.
    #[allow(non_snake_case)]
    pub nominalRelation: Index,
    /// `Index rootRelation` — root RT index if partitioned/inherited.
    #[allow(non_snake_case)]
    pub rootRelation: Index,
    /// `bool partColsUpdated` — some part key in hierarchy updated?
    #[allow(non_snake_case)]
    pub partColsUpdated: bool,
    /// `List *resultRelations` — integer list of RT indexes.
    #[allow(non_snake_case)]
    pub resultRelations: Vec<i32>,
    /// `List *updateColnosLists` — per-target-table update_colnos lists.
    #[allow(non_snake_case)]
    pub updateColnosLists: Vec<Vec<AttrNumber>>,
    /// `List *withCheckOptionLists` — per-target-table WCO lists (node handles).
    #[allow(non_snake_case)]
    pub withCheckOptionLists: Vec<Vec<NodeId>>,
    /// `List *returningLists` — per-target-table RETURNING tlists (node handles).
    #[allow(non_snake_case)]
    pub returningLists: Vec<Vec<NodeId>>,
    /// `List *rowMarks` — PlanRowMarks (non-locking only; opaque node handles).
    #[allow(non_snake_case)]
    pub rowMarks: Vec<NodeId>,
    /// `OnConflictExpr *onconflict` — ON CONFLICT clause, or `None` (opaque
    /// node handle; analysis is deferred to createplan.c).
    pub onconflict: Option<NodeId>,
    /// `int epqParam` — ID of the Param for EvalPlanQual re-eval.
    #[allow(non_snake_case)]
    pub epqParam: i32,
    /// `List *mergeActionLists` — per-target-table MERGE action lists (handles).
    #[allow(non_snake_case)]
    pub mergeActionLists: Vec<Vec<NodeId>>,
    /// `List *mergeJoinConditions` — per-target-table MERGE join conditions.
    #[allow(non_snake_case)]
    pub mergeJoinConditions: Vec<Vec<NodeId>>,
}

/// `LimitPath` — applying LIMIT/OFFSET restrictions.
#[derive(Clone, Debug)]
pub struct LimitPath {
    pub path: Path,
    /// `Path *subpath` — input source (handle into `path_arena`).
    pub subpath: Option<PathId>,
    /// `Node *limitOffset` — OFFSET parameter, or `None` (expr node handle).
    #[allow(non_snake_case)]
    pub limitOffset: Option<NodeId>,
    /// `Node *limitCount` — COUNT parameter, or `None` (expr node handle).
    #[allow(non_snake_case)]
    pub limitCount: Option<NodeId>,
    #[allow(non_snake_case)]
    pub limitOption: LimitOption,
}

/// The polymorphic path-arena element — the owned-tree analogue of a `Path *`
/// that may point at any path subtype. The path constructors (`pathnode.c`,
/// reached via seams) mint the concrete variants; the enumerator reaches the
/// embedded base [`Path`] uniformly via [`PathNode::base`]. `#[non_exhaustive]`:
/// further path variants are added as the constructing units land.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PathNode {
    /// `T_Path` — a plain base/scan path (seqscan/samplescan/function/values/…).
    Path(Path),
    /// `T_IndexPath`.
    IndexPath(IndexPath),
    /// `T_BitmapHeapPath`.
    BitmapHeapPath(BitmapHeapPath),
    /// `T_BitmapAndPath`.
    BitmapAndPath(BitmapAndPath),
    /// `T_BitmapOrPath`.
    BitmapOrPath(BitmapOrPath),
    /// `T_TidPath`.
    TidPath(TidPath),
    /// `T_TidRangePath`.
    TidRangePath(TidRangePath),
    /// `T_SubqueryScanPath`.
    SubqueryScanPath(SubqueryScanPath),
    /// `T_ForeignPath`.
    ForeignPath(ForeignPath),
    /// `T_CustomPath`.
    CustomPath(CustomPath),
    /// `T_NestPath`.
    NestPath(NestPath),
    /// `T_MergePath`.
    MergePath(MergePath),
    /// `T_HashPath`.
    HashPath(HashPath),
    /// `T_AppendPath`.
    AppendPath(AppendPath),
    /// `T_MergeAppendPath`.
    MergeAppendPath(MergeAppendPath),
    /// `T_GroupResultPath`.
    GroupResultPath(GroupResultPath),
    /// `T_MaterialPath`.
    MaterialPath(MaterialPath),
    /// `T_MemoizePath`.
    MemoizePath(MemoizePath),
    /// `T_UniquePath`.
    UniquePath(UniquePath),
    /// `T_GatherPath`.
    GatherPath(GatherPath),
    /// `T_GatherMergePath`.
    GatherMergePath(GatherMergePath),
    /// `T_ProjectionPath`.
    ProjectionPath(ProjectionPath),
    /// `T_ProjectSetPath`.
    ProjectSetPath(ProjectSetPath),
    /// `T_SortPath`.
    SortPath(SortPath),
    /// `T_IncrementalSortPath`.
    IncrementalSortPath(IncrementalSortPath),
    /// `T_GroupPath`.
    GroupPath(GroupPath),
    /// `T_UpperUniquePath`.
    UpperUniquePath(UpperUniquePath),
    /// `T_AggPath`.
    AggPath(AggPath),
    /// `T_GroupingSetsPath`.
    GroupingSetsPath(GroupingSetsPath),
    /// `T_MinMaxAggPath`.
    MinMaxAggPath(MinMaxAggPath),
    /// `T_WindowAggPath`.
    WindowAggPath(WindowAggPath),
    /// `T_SetOpPath`.
    SetOpPath(SetOpPath),
    /// `T_RecursiveUnionPath`.
    RecursiveUnionPath(RecursiveUnionPath),
    /// `T_LockRowsPath`.
    LockRowsPath(LockRowsPath),
    /// `T_ModifyTablePath`.
    ModifyTablePath(ModifyTablePath),
    /// `T_LimitPath`.
    LimitPath(LimitPath),
}

impl PathNode {
    /// Recover the embedded base [`Path`] (the analogue of the C up-cast
    /// `(Path *) subtype`).
    pub fn base(&self) -> &Path {
        match self {
            PathNode::Path(p) => p,
            PathNode::IndexPath(p) => &p.path,
            PathNode::BitmapHeapPath(p) => &p.path,
            PathNode::BitmapAndPath(p) => &p.path,
            PathNode::BitmapOrPath(p) => &p.path,
            PathNode::TidPath(p) => &p.path,
            PathNode::TidRangePath(p) => &p.path,
            PathNode::SubqueryScanPath(p) => &p.path,
            PathNode::ForeignPath(p) => &p.path,
            PathNode::CustomPath(p) => &p.path,
            PathNode::NestPath(p) => &p.jpath.path,
            PathNode::MergePath(p) => &p.jpath.path,
            PathNode::HashPath(p) => &p.jpath.path,
            PathNode::AppendPath(p) => &p.path,
            PathNode::MergeAppendPath(p) => &p.path,
            PathNode::GroupResultPath(p) => &p.path,
            PathNode::MaterialPath(p) => &p.path,
            PathNode::MemoizePath(p) => &p.path,
            PathNode::UniquePath(p) => &p.path,
            PathNode::GatherPath(p) => &p.path,
            PathNode::GatherMergePath(p) => &p.path,
            PathNode::ProjectionPath(p) => &p.path,
            PathNode::ProjectSetPath(p) => &p.path,
            PathNode::SortPath(p) => &p.path,
            PathNode::IncrementalSortPath(p) => &p.spath.path,
            PathNode::GroupPath(p) => &p.path,
            PathNode::UpperUniquePath(p) => &p.path,
            PathNode::AggPath(p) => &p.path,
            PathNode::GroupingSetsPath(p) => &p.path,
            PathNode::MinMaxAggPath(p) => &p.path,
            PathNode::WindowAggPath(p) => &p.path,
            PathNode::SetOpPath(p) => &p.path,
            PathNode::RecursiveUnionPath(p) => &p.path,
            PathNode::LockRowsPath(p) => &p.path,
            PathNode::ModifyTablePath(p) => &p.path,
            PathNode::LimitPath(p) => &p.path,
        }
    }

    /// Recover the embedded base [`Path`] for mutation.
    pub fn base_mut(&mut self) -> &mut Path {
        match self {
            PathNode::Path(p) => p,
            PathNode::IndexPath(p) => &mut p.path,
            PathNode::BitmapHeapPath(p) => &mut p.path,
            PathNode::BitmapAndPath(p) => &mut p.path,
            PathNode::BitmapOrPath(p) => &mut p.path,
            PathNode::TidPath(p) => &mut p.path,
            PathNode::TidRangePath(p) => &mut p.path,
            PathNode::SubqueryScanPath(p) => &mut p.path,
            PathNode::ForeignPath(p) => &mut p.path,
            PathNode::CustomPath(p) => &mut p.path,
            PathNode::NestPath(p) => &mut p.jpath.path,
            PathNode::MergePath(p) => &mut p.jpath.path,
            PathNode::HashPath(p) => &mut p.jpath.path,
            PathNode::AppendPath(p) => &mut p.path,
            PathNode::MergeAppendPath(p) => &mut p.path,
            PathNode::GroupResultPath(p) => &mut p.path,
            PathNode::MaterialPath(p) => &mut p.path,
            PathNode::MemoizePath(p) => &mut p.path,
            PathNode::UniquePath(p) => &mut p.path,
            PathNode::GatherPath(p) => &mut p.path,
            PathNode::GatherMergePath(p) => &mut p.path,
            PathNode::ProjectionPath(p) => &mut p.path,
            PathNode::ProjectSetPath(p) => &mut p.path,
            PathNode::SortPath(p) => &mut p.path,
            PathNode::IncrementalSortPath(p) => &mut p.spath.path,
            PathNode::GroupPath(p) => &mut p.path,
            PathNode::UpperUniquePath(p) => &mut p.path,
            PathNode::AggPath(p) => &mut p.path,
            PathNode::GroupingSetsPath(p) => &mut p.path,
            PathNode::MinMaxAggPath(p) => &mut p.path,
            PathNode::WindowAggPath(p) => &mut p.path,
            PathNode::SetOpPath(p) => &mut p.path,
            PathNode::RecursiveUnionPath(p) => &mut p.path,
            PathNode::LockRowsPath(p) => &mut p.path,
            PathNode::ModifyTablePath(p) => &mut p.path,
            PathNode::LimitPath(p) => &mut p.path,
        }
    }
}

/* ==========================================================================
 * RestrictInfo (pathnodes.h)
 * ======================================================================== */

/// `RestrictInfo` — a WHERE/JOIN clause plus planner annotations. Trimmed to the
/// scalar/`Relids`/handle fields the join-path enumerator and its seams read;
/// the clause node payload (`clause`/`orclause`) is owned by the
/// optimizer-arena crate and reached only by `RinfoId` handle from here, so it
/// is not carried in this consumer-facing mirror.
#[derive(Clone, Debug)]
pub struct RestrictInfo {
    /// `Expr *clause` — the represented WHERE/JOIN clause, as an opaque
    /// expr-node handle into the optimizer arena.
    pub clause: NodeId,
    pub is_pushed_down: bool,
    pub can_join: bool,
    pub pseudoconstant: bool,
    /// `bool has_clone` — this clause has clones with extra `required_relids`.
    pub has_clone: bool,
    /// `bool is_clone` — this clause is a clone of another (outer-join id 3).
    pub is_clone: bool,
    /// `bool leakproof` — true if known to contain no leaked Vars.
    pub leakproof: bool,
    /// `VolatileFunctionStatus has_volatile` — volatility cache of the clause.
    pub has_volatile: VolatileFunctionStatus,
    /// `Index security_level` — security level of the clause.
    pub security_level: u32,
    /// `int num_base_rels` — number of base rels in `clause_relids`.
    pub num_base_rels: i32,
    pub clause_relids: Relids,
    pub required_relids: Relids,
    /// `Relids incompatible_relids` — relids above which the clause can't be
    /// evaluated.
    pub incompatible_relids: Relids,
    pub outer_relids: Relids,
    pub left_relids: Relids,
    pub right_relids: Relids,
    /// `Expr *orclause` — modified clause with RestrictInfos; `None` unless
    /// `clause` is an OR clause. Opaque expr-node handle.
    pub orclause: Option<NodeId>,
    /// per-clause serial (unique within a planner run); the memoize
    /// inner_unique guard tests membership in `ppi_serials`.
    pub rinfo_serial: i32,
    /// generating EquivalenceClass, if any — handle into `eq_classes`.
    pub parent_ec: Option<EcId>,
    /// `QualCost eval_cost` — eval cost of the clause; `-1` startup if not set.
    pub eval_cost: QualCost,
    /// `Selectivity norm_selec` — selectivity for JOIN_INNER semantics; -1 if
    /// not yet set.
    pub norm_selec: f64,
    /// `Selectivity outer_selec` — selectivity for outer-join semantics; -1 if
    /// not yet set.
    pub outer_selec: f64,
    /// opfamilies containing clause operator (OIDs), valid if mergejoinable.
    pub mergeopfamilies: Vec<Oid>,
    /// EquivalenceClass containing the left operand — handle into `eq_classes`.
    pub left_ec: Option<EcId>,
    /// EquivalenceClass containing the right operand — handle into `eq_classes`.
    pub right_ec: Option<EcId>,
    /// `EquivalenceMember *left_em` — EM for the lefthand; handle into `em_arena`.
    pub left_em: Option<EmId>,
    /// `EquivalenceMember *right_em` — EM for the righthand; handle into
    /// `em_arena`.
    pub right_em: Option<EmId>,
    /// `List *scansel_cache` — MergeScanSelCache structs. Not Nodes; the C code
    /// replaces them with NIL on copy, so this carries opaque handles and is
    /// reset (empty) by a clone-style rebuild.
    pub scansel_cache: Vec<NodeId>,
    pub outer_is_left: bool,
    pub hashjoinoperator: Oid,
    /// `Selectivity left_bucketsize` — avg bucketsize of the left side; -1 if not
    /// yet set.
    pub left_bucketsize: f64,
    /// `Selectivity right_bucketsize` — avg bucketsize of the right side.
    pub right_bucketsize: f64,
    /// `Selectivity left_mcvfreq` — left side's most-common-value frequency.
    pub left_mcvfreq: f64,
    /// `Selectivity right_mcvfreq` — right side's most-common-value frequency.
    pub right_mcvfreq: f64,
    /// hash equality operator for the "outer op inner" form (clause's left
    /// arg is the outer side) — `OpExpr` payload cached on the rinfo.
    pub left_hasheqoperator: Oid,
    /// hash equality operator for the "inner op outer" form.
    pub right_hasheqoperator: Oid,
}

/* ==========================================================================
 * EquivalenceClass (pathnodes.h) — only the fields the eclass-merge chase + the
 * redundancy predicate need. EquivalenceMembers / derive caches belong to the
 * equivclass.c crate and are reached through its seams.
 * ======================================================================== */

/// `EquivalenceClass` — a set of expressions known to be equal (pathnodes.h:
/// 1442-1467). The full planner producer fields; `ec_members`/`ec_childmembers`
/// hold [`EmId`] handles into [`PlannerInfo::em_arena`] (the EM pointer identity
/// the derive-hash and search routines key on survives as the arena index).
#[derive(Clone, Debug, Default)]
pub struct EquivalenceClass {
    /// `List *ec_opfamilies` — btree operator family OIDs.
    pub ec_opfamilies: Vec<Oid>,
    /// `Oid ec_collation` — collation, if the datatypes are collatable.
    pub ec_collation: Oid,
    /// `int ec_childmembers_size` — # elements in `ec_childmembers`.
    pub ec_childmembers_size: i32,
    /// `List *ec_members` — list of EquivalenceMembers (handles into
    /// `em_arena`).
    pub ec_members: Vec<EmId>,
    /// `List **ec_childmembers` — per-relid array of Lists of child members
    /// (handles into `em_arena`).
    pub ec_childmembers: Vec<Vec<EmId>>,
    /// `List *ec_sources` — generating RestrictInfos (handles into
    /// `rinfo_arena`).
    pub ec_sources: Vec<RinfoId>,
    /// `List *ec_derives_list` — derived RestrictInfos (handles into
    /// `rinfo_arena`).
    pub ec_derives_list: Vec<RinfoId>,
    /// `struct derives_hash *ec_derives_hash` — optional fast-lookup hash over
    /// the same RestrictInfos as `ec_derives_list`. Opaque, rebuildable cache;
    /// `None` until built.
    pub ec_derives_hash: Option<Box<DerivesHash>>,
    /// `Relids ec_relids` — all relids in `ec_members` (excluding child members).
    pub ec_relids: Relids,
    /// `bool ec_has_const` — any pseudoconstants in `ec_members`?
    pub ec_has_const: bool,
    /// `bool ec_has_volatile` — the (sole) member is a volatile expr?
    pub ec_has_volatile: bool,
    /// `bool ec_broken` — failed to generate needed clauses?
    pub ec_broken: bool,
    /// `Index ec_sortref` — originating sortclause label, or 0.
    pub ec_sortref: Index,
    /// `Index ec_min_security` — minimum security_level in `ec_sources`.
    pub ec_min_security: Index,
    /// `Index ec_max_security` — maximum security_level in `ec_sources`.
    pub ec_max_security: Index,
    /// `EquivalenceClass *ec_merged` — non-NULL if this EC was merged into
    /// another; the canonical EC is found by chasing this. A handle into
    /// `eq_classes`.
    pub ec_merged: Option<EcId>,
}

/// `EquivalenceMember` — one member expression of an [`EquivalenceClass`]
/// (pathnodes.h:1503-1517). Lives in [`PlannerInfo::em_arena`], reached by
/// [`EmId`].
#[derive(Clone, Debug, Default)]
pub struct EquivalenceMember {
    /// `Expr *em_expr` — the represented expression (opaque expr node handle).
    pub em_expr: NodeId,
    /// `Relids em_relids` — all relids appearing in `em_expr`.
    pub em_relids: Relids,
    /// `bool em_is_const` — expression is pseudoconstant?
    pub em_is_const: bool,
    /// `bool em_is_child` — derived version for a child relation?
    pub em_is_child: bool,
    /// `Oid em_datatype` — the "nominal type" used by the opfamily.
    pub em_datatype: Oid,
    /// `JoinDomain *em_jdomain` — join domain containing the source clause.
    pub em_jdomain: Option<Box<JoinDomain>>,
    /// `EquivalenceMember *em_parent` — for a child member, the corresponding EM
    /// for the top parent (handle into `em_arena`).
    pub em_parent: Option<EmId>,
}

/// `EquivalenceMemberIterator` (equivclass.c) — state for iterating an EC's
/// parent members (`ec_members`) followed by the child members
/// (`ec_childmembers[relid]`) for the requested `child_relids`. Resolved against
/// a [`PlannerInfo`] by `eclass_member_iterator_next`.
#[derive(Clone, Debug, Default)]
pub struct EquivalenceMemberIterator {
    /// `EquivalenceClass *ec` — the EC being iterated (handle into `eq_classes`).
    pub ec: Option<EcId>,
    /// `int current_relid` — the child relid most recently advanced to (-1 to
    /// start; the parent-member pass uses the initial `current_list`).
    pub current_relid: i32,
    /// `Relids child_relids` — the child relids whose `ec_childmembers` lists are
    /// to be walked (empty/`None` if the EC has no child members).
    pub child_relids: Relids,
    /// `ListCell *current_cell` — cursor into `current_list` (index, or `None`).
    pub current_cell: Option<usize>,
    /// `List *current_list` — the member list currently being walked (a copy of
    /// `ec_members` or one of the `ec_childmembers[relid]` lists).
    pub current_list: Vec<EmId>,
}

/// `ForeignKeyOptInfo` (pathnodes.h) — per-foreign-key planner bookkeeping,
/// trimmed to the fields `match_eclasses_to_foreign_key_col` reads/writes. The
/// per-column EC match results are stored back into `eclass`/`fk_eclass_member`.
#[derive(Clone, Debug, Default)]
pub struct ForeignKeyOptInfo {
    /// `Index con_relid` — RT index of the referencing (FK) table.
    pub con_relid: Index,
    /// `Index ref_relid` — RT index of the referenced (PK) table.
    pub ref_relid: Index,
    /// `int nkeys` — number of columns in the FK.
    pub nkeys: i32,
    /// `AttrNumber conkey[]` — cols in the FK table (zero-based access).
    pub conkey: Vec<AttrNumber>,
    /// `AttrNumber confkey[]` — cols in the referenced table.
    pub confkey: Vec<AttrNumber>,
    /// `Oid conpfeqop[]` — PK = FK operator OIDs.
    pub conpfeqop: Vec<Oid>,
    /// `EquivalenceClass *eclass[]` — matching EC for each column (or `None`).
    pub eclass: Vec<Option<EcId>>,
    /// `EquivalenceMember *fk_eclass_member[]` — the FK-table EM within that EC.
    pub fk_eclass_member: Vec<Option<EmId>>,
}

/// `OuterJoinClauseInfo` (pathnodes.h) — an outer-join clause set aside by
/// `distribute_qual_to_rels` for `reconsider_outer_join_clauses` to re-examine.
#[derive(Clone, Debug)]
pub struct OuterJoinClauseInfo {
    /// `RestrictInfo *rinfo` — the set-aside clause (handle into `rinfo_arena`).
    pub rinfo: RinfoId,
    /// `SpecialJoinInfo *sjinfo` — the outer join the clause came from.
    pub sjinfo: SpecialJoinInfo,
}

/* ==========================================================================
 * SpecialJoinInfo (pathnodes.h)
 * ======================================================================== */

/// `SpecialJoinInfo` — info about an outer/semi/anti join. Trimmed to the
/// `Relids`/`JoinType`/`ojrelid` fields the enumerator reads.
#[derive(Clone, Debug)]
pub struct SpecialJoinInfo {
    pub min_lefthand: Relids,
    pub min_righthand: Relids,
    pub syn_lefthand: Relids,
    pub syn_righthand: Relids,
    pub jointype: JoinType,
    pub ojrelid: Index,
    pub commute_above_l: Relids,
    pub commute_above_r: Relids,
    pub commute_below_l: Relids,
    pub commute_below_r: Relids,
    pub lhs_strict: bool,
    pub semi_can_btree: bool,
    pub semi_can_hash: bool,
    pub semi_operators: Vec<Oid>,
}

/* ==========================================================================
 * PlaceHolderInfo (pathnodes.h) — trimmed to the fields the memoize cache-key
 * analysis (`extract_lateral_vars_from_PHVs`) reads.
 * ======================================================================== */

/// `PlaceHolderInfo` — planner bookkeeping for a `PlaceHolderVar`. Trimmed to
/// the `Relids`/expr-handle fields the join-path enumerator reads; the full
/// node tree is owned by the placeholder.c crate.
#[derive(Clone, Debug, Default)]
pub struct PlaceHolderInfo {
    /// `Index phid` — ID for the PH (unique within a planner run).
    pub phid: Index,
    /// `ph_var->phexpr` — the represented expression (an expr `Node *`). The
    /// `ph_var` is a `PlaceHolderVar` tree; the join-path layer only reads its
    /// `phexpr`, so just that expr handle is carried.
    pub ph_var_phexpr: NodeId,
    /// lowest level we can evaluate the value at.
    pub ph_eval_at: Relids,
    /// relids of contained lateral refs, if any (NULL/empty if none).
    pub ph_lateral: Relids,
    /// `Relids ph_needed` — highest level the value is needed at.
    pub ph_needed: Relids,
    /// `int32 ph_width` — estimated attribute width.
    pub ph_width: i32,
}

/* ==========================================================================
 * RelOptInfo (pathnodes.h)
 * ======================================================================== */

/// `RelOptInfo` — per-relation planning state. Trimmed to the fields the
/// join-path enumerator reads/writes; node-payload fields (reltarget exprs,
/// indexlist, subroot, lateral vars, partition trees) belong to their owning
/// crates and are not carried in this consumer-facing mirror.
#[derive(Clone, Debug, Default)]
pub struct RelOptInfo {
    pub reloptkind: RelOptKind,
    pub relids: Relids,
    pub rows: Cardinality,
    pub consider_startup: bool,
    pub consider_param_startup: bool,
    pub consider_parallel: bool,
    pub reltarget: Option<Box<PathTarget>>,
    /// Path handles into `path_arena`.
    pub pathlist: Vec<PathId>,
    pub ppilist: Vec<ParamPathInfo>,
    /// partial Path handles into `path_arena`.
    pub partial_pathlist: Vec<PathId>,
    pub cheapest_startup_path: Option<PathId>,
    pub cheapest_total_path: Option<PathId>,
    pub cheapest_unique_path: Option<PathId>,
    pub cheapest_parameterized_paths: Vec<PathId>,
    pub direct_lateral_relids: Relids,
    pub lateral_relids: Relids,
    /// lateral references this rel must supply — expr `Node *` handles; the
    /// memoize cache-key analysis folds these into the cache keys.
    pub lateral_vars: Vec<NodeId>,
    pub relid: Index,
    pub reltablespace: Oid,
    pub rtekind: RTEKind,
    pub min_attr: AttrNumber,
    pub max_attr: AttrNumber,
    pub attr_widths: Vec<i32>,
    pub nulling_relids: Relids,
    pub lateral_referencers: Relids,
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    pub allvisfrac: f64,
    /// RestrictInfo handles into `rinfo_arena` (if base rel).
    pub baserestrictinfo: Vec<RinfoId>,
    pub baserestrictcost: QualCost,
    pub baserestrict_min_security: Index,
    /// RestrictInfo handles for join clauses involving this rel.
    pub joininfo: Vec<RinfoId>,
    pub has_eclass_joins: bool,
    pub consider_partitionwise_join: bool,
    pub serverid: Oid,
    pub userid: Oid,
    pub useridiscurrent: bool,
    /// immediate parent relation (handle into `rel_arena`).
    pub parent: Option<RelId>,
    /// topmost parent relation (handle into `rel_arena`).
    pub top_parent: Option<RelId>,
    pub top_parent_relids: Relids,
    pub rel_parallel_workers: i32,
    pub amflags: u32,
    pub has_fdwroutine: bool,

    /* ------------------------------------------------------------------
     * F0b producer-model fields (additive). The join-path consumer does
     * not read these; they are produced by relcache/plancat/initsplan/
     * partitionwise. Expression lists are opaque `NodeId` handles; node
     * payloads (FdwRoutine, PartitionBoundInfo) are opaque values owned by
     * their producing units.
     * ------------------------------------------------------------------ */
    /// `Relids *attr_needed` — array indexed [min_attr..max_attr]: the highest
    /// joinrel each attribute is needed in.
    pub attr_needed: Vec<Relids>,
    /// `Bitmapset *notnullattnums` — zero-based set of NOT NULL column attnums.
    pub notnullattnums: Relids,
    /// `List *indexlist` — IndexOptInfos for the relation's indexes.
    pub indexlist: Vec<IndexOptInfo>,
    /// `List *statlist` — StatisticExtInfos (opaque node handles).
    pub statlist: Vec<NodeId>,
    /// `Bitmapset *eclass_indexes` — indexes into PlannerInfo::eq_classes of ECs
    /// that mention this rel (filled after EC merging).
    pub eclass_indexes: Relids,
    /// `PlannerInfo *subroot` — PlannerInfo for a subquery rel (`None` if not a
    /// subquery). Carried in a clone-skipping [`Subroot`] wrapper so
    /// `RelOptInfo` can keep deriving `Clone` (a non-`Clone` `PlannerInfo`).
    pub subroot: Subroot,
    /// `List *subplan_params` — PlannerParamItems to pass to the subquery
    /// (opaque node handles).
    pub subplan_params: Vec<NodeId>,
    /// `struct FdwRoutine *fdwroutine` — FDW function hooks if a foreign table.
    /// Opaque value owned by the FDW unit; `None` if not foreign.
    pub fdwroutine: Option<Box<FdwRoutine>>,
    /// `void *fdw_private` — private FDW state (opaque node handle; 0 = NULL).
    pub fdw_private: NodeId,
    /// `List *unique_for_rels` — UniqueRelInfos: other-rel sets this rel is
    /// proven unique for (opaque node handles).
    pub unique_for_rels: Vec<NodeId>,
    /// `List *non_unique_for_rels` — Relid sets this rel was tried-and-failed to
    /// prove unique for.
    pub non_unique_for_rels: Vec<Relids>,
    /// `PartitionScheme part_scheme` — partitioning scheme of the relation.
    pub part_scheme: PartitionScheme,
    /// `int nparts` — number of partitions; -1 if not yet set (0 for a join rel
    /// means considered unpartitioned).
    pub nparts: i32,
    /// `struct PartitionBoundInfoData *boundinfo` — partition bounds. Opaque
    /// value owned by the partbounds unit; `None` if not set.
    pub boundinfo: Option<Box<PartitionBoundInfoData>>,
    /// `bool partbounds_merged` — true if bounds came from
    /// partition_bounds_merge().
    pub partbounds_merged: bool,
    /// `List *partition_qual` — partition constraint if not the root (opaque
    /// expr node handles).
    pub partition_qual: Vec<NodeId>,
    /// `struct RelOptInfo **part_rels` — RelOptInfos for each partition, in
    /// bound order (handles into `rel_arena`; `None` for pruned slots).
    pub part_rels: Vec<Option<RelId>>,
    /// `Bitmapset *live_parts` — indexes into `part_rels` for partitions that
    /// survived pruning.
    pub live_parts: Relids,
    /// `Relids all_partrels` — set of all partition relids.
    pub all_partrels: Relids,
    /// `List **partexprs` — non-nullable partition key expressions, one list per
    /// partitioning column (opaque expr node handles).
    pub partexprs: Vec<Vec<NodeId>>,
    /// `List **nullable_partexprs` — nullable partition key expressions, one
    /// list per partitioning column.
    pub nullable_partexprs: Vec<Vec<NodeId>>,
}

/* ==========================================================================
 * PlannerInfo (pathnodes.h) — the per-query planner state + the four arenas.
 * ======================================================================== */

/// `PlannerInfo` — per-query planner state. Trimmed to the fields the join-path
/// enumerator reads, plus the four arenas that own the aliasing graph types.
#[derive(Debug, Default)]
pub struct PlannerInfo {
    pub all_baserels: Relids,
    pub outer_join_rels: Relids,
    pub all_query_rels: Relids,
    /// list of SpecialJoinInfos.
    pub join_info_list: Vec<SpecialJoinInfo>,
    pub last_rinfo_serial: i32,
    /// true if any RTE is a LATERAL subquery (the C `hasLateralRTEs`); the
    /// memoize PHV scan early-outs when false.
    #[allow(non_snake_case)]
    pub hasLateralRTEs: bool,
    /// list of PlaceHolderInfos — handles into `ph_info_arena`.
    pub placeholder_list: Vec<PhInfoId>,

    /// `List *join_rel_list` — list of join-relation RelOptInfos. GEQO appends
    /// candidate joinrels here while building a tour and truncates back to the
    /// saved length afterward.
    pub join_rel_list: Vec<RelId>,
    /// `struct HTAB *join_rel_hash` — optional hashtable for faster lookup of
    /// join-relation RelOptInfos. GEQO nulls this for the duration of an
    /// evaluation so a fresh local hash is built and restores it afterward.
    pub join_rel_hash: Option<Box<HTAB>>,
    /// `List **join_rel_level` — lists of join-relation RelOptInfos at each
    /// level (`standard_join_search`); should be unused (empty) while GEQO runs.
    pub join_rel_level: Vec<Vec<RelId>>,
    /// `void *join_search_private` — private state for a custom join-search
    /// hook (GEQO stores its [`JoinSearchPrivate`] here in C; the port threads
    /// the state explicitly and only nulls this field).
    pub join_search_private: Option<Box<JoinSearchPrivate>>,

    /* ------------------------------------------------------------------
     * F0a producer-model fields (additive; pathnodes.h:216-586). The
     * join-path consumer does not read these — they are produced by
     * parse-analysis/initsplan/query_planner/grouping_planner/createplan.
     * Expression/clause lists are opaque `NodeId` handles; cross-arena rel
     * refs use `RelId`; parse/global state use the opaque `QueryId`/
     * `PlannerGlobal` conventions.
     * ------------------------------------------------------------------ */
    /// `Query *parse` — the Query being planned (opaque handle).
    pub parse: QueryId,
    /// `PlannerGlobal *glob` — global info for the current planner run.
    pub glob: Option<Box<PlannerGlobal>>,
    /// `Index query_level` — 1 at the outermost Query.
    pub query_level: Index,
    /// `List *plan_params` — PlannerParamItems this level exposes to a lower
    /// level (opaque node handles).
    pub plan_params: Vec<NodeId>,
    /// `Bitmapset *outer_params` — paramIds outer levels expose to this level.
    pub outer_params: Relids,
    /// `RelOptInfo **simple_rel_array` — per-RT-index slot array (handles into
    /// `rel_arena`; `None` where the RTE has no base rel).
    pub simple_rel_array: Vec<Option<RelId>>,
    /// `int simple_rel_array_size` — allocated size of the array.
    pub simple_rel_array_size: i32,
    /// `RangeTblEntry **simple_rte_array` — per-RT-index RTE handles.
    pub simple_rte_array: Vec<RangeTblEntryId>,
    /// `AppendRelInfo **append_rel_array` — per-child-relid AppendRelInfo
    /// (`None` = NULL slot). A real [`AppendRelInfo`] value: relnode reads
    /// `appinfo->child_relid`/`parent_relid` etc. directly.
    pub append_rel_array: Vec<Option<AppendRelInfo>>,
    /// `int join_cur_level` — index of the join level being extended.
    pub join_cur_level: i32,
    /// `List *init_plans` — init SubPlans for the query (opaque node handles).
    pub init_plans: Vec<NodeId>,
    /// `List *cte_plan_ids` — per-CTE-item subplan ID (or -1).
    pub cte_plan_ids: Vec<i32>,
    /// `List *multiexpr_params` — Lists of Params for MULTIEXPR outputs.
    pub multiexpr_params: Vec<Vec<NodeId>>,
    /// `List *join_domains` — JoinDomains used in the query (higher ones first).
    pub join_domains: Vec<JoinDomain>,
    /// `bool ec_merging_done` — set true once ECs are canonical.
    pub ec_merging_done: bool,
    /// `List *canon_pathkeys` — "canonical" PathKeys.
    pub canon_pathkeys: Vec<PathKey>,
    /// `List *left_join_clauses` — OuterJoinClauseInfos, nonnullable var on left.
    pub left_join_clauses: Vec<OuterJoinClauseInfo>,
    /// `List *right_join_clauses` — OuterJoinClauseInfos, nonnullable var on
    /// right.
    pub right_join_clauses: Vec<OuterJoinClauseInfo>,
    /// `List *full_join_clauses` — OuterJoinClauseInfos for full join clauses.
    pub full_join_clauses: Vec<OuterJoinClauseInfo>,
    /// `Relids all_result_relids` — set of all result relids.
    pub all_result_relids: Relids,
    /// `Relids leaf_result_relids` — set of all leaf result relids.
    pub leaf_result_relids: Relids,
    /// `List *append_rel_list` — AppendRelInfos (real values).
    pub append_rel_list: Vec<AppendRelInfo>,
    /// `List *row_identity_vars` — RowIdentityVarInfos (opaque node handles).
    pub row_identity_vars: Vec<NodeId>,
    /// `List *rowMarks` — PlanRowMarks (opaque node handles).
    #[allow(non_snake_case)]
    pub rowMarks: Vec<NodeId>,
    /// `PlaceHolderInfo **placeholder_array` — array indexed by phid (handles
    /// into `ph_info_arena`; `None` for empty slots).
    pub placeholder_array: Vec<Option<PhInfoId>>,
    /// `int placeholder_array_size` — allocated size of the array.
    pub placeholder_array_size: i32,
    /// `List *fkey_list` — ForeignKeyOptInfos (opaque node handles).
    pub fkey_list: Vec<NodeId>,
    /// `List *query_pathkeys` — desired pathkeys for query_planner().
    pub query_pathkeys: Vec<PathKey>,
    /// `List *group_pathkeys` — groupClause pathkeys, if any.
    pub group_pathkeys: Vec<PathKey>,
    /// `int num_groupby_pathkeys` — # of `group_pathkeys` belonging to GROUP BY.
    pub num_groupby_pathkeys: i32,
    /// `List *window_pathkeys` — pathkeys of the bottom window, if any.
    pub window_pathkeys: Vec<PathKey>,
    /// `List *distinct_pathkeys` — distinctClause pathkeys, if any.
    pub distinct_pathkeys: Vec<PathKey>,
    /// `List *sort_pathkeys` — sortClause pathkeys, if any.
    pub sort_pathkeys: Vec<PathKey>,
    /// `List *setop_pathkeys` — set operator pathkeys, if any.
    pub setop_pathkeys: Vec<PathKey>,
    /// `List *part_schemes` — canonicalised partition schemes used in the query.
    pub part_schemes: Vec<PartitionScheme>,
    /// `List *initial_rels` — RelOptInfos we are now trying to join (handles
    /// into `rel_arena`).
    pub initial_rels: Vec<RelId>,
    /// `List *upper_rels[UPPERREL_FINAL + 1]` — upper-rel RelOptInfos by kind
    /// (handles into `rel_arena`).
    pub upper_rels: [Vec<RelId>; NUM_UPPERREL_KINDS],
    /// `struct PathTarget *upper_targets[UPPERREL_FINAL + 1]` — result tlists for
    /// upper-stage processing, by kind.
    pub upper_targets: [Option<Box<PathTarget>>; NUM_UPPERREL_KINDS],
    /// `List *processed_groupClause` — fully-processed groupClause (opaque node
    /// handles).
    #[allow(non_snake_case)]
    pub processed_groupClause: Vec<NodeId>,
    /// `List *processed_distinctClause` — fully-processed distinctClause.
    #[allow(non_snake_case)]
    pub processed_distinctClause: Vec<NodeId>,
    /// `List *processed_tlist` — fully-processed targetlist (TargetEntrys).
    #[allow(non_snake_case)]
    pub processed_tlist: Vec<NodeId>,
    /// `List *update_colnos` — UPDATE target attribute numbers.
    pub update_colnos: Vec<AttrNumber>,
    /// `AttrNumber *grouping_map` — GroupingFunc fixup map (createplan/setrefs).
    pub grouping_map: Vec<AttrNumber>,
    /// `List *minmax_aggs` — MinMaxAggInfos (opaque node handles).
    pub minmax_aggs: Vec<NodeId>,
    /// `MemoryContext planner_cxt` — context holding this PlannerInfo.
    pub planner_cxt: PathnodesMemoryContext,
    /// `Cardinality total_table_pages` — # pages in all non-dummy tables.
    pub total_table_pages: Cardinality,
    /// `Selectivity tuple_fraction` — passed to query_planner.
    pub tuple_fraction: Selectivity,
    /// `Cardinality limit_tuples` — passed to query_planner.
    pub limit_tuples: Cardinality,
    /// `Index qual_security_level` — minimum security_level for quals (0 if no
    /// securityQuals).
    pub qual_security_level: Index,
    /// `bool hasJoinRTEs` — true if any RTE is RTE_JOIN kind.
    #[allow(non_snake_case)]
    pub hasJoinRTEs: bool,
    /// `bool hasHavingQual` — true if havingQual was non-null.
    #[allow(non_snake_case)]
    pub hasHavingQual: bool,
    /// `bool hasPseudoConstantQuals` — true if any RestrictInfo is pseudoconstant.
    #[allow(non_snake_case)]
    pub hasPseudoConstantQuals: bool,
    /// `bool hasAlternativeSubPlans` — true if we've made any.
    #[allow(non_snake_case)]
    pub hasAlternativeSubPlans: bool,
    /// `bool placeholdersFrozen` — true once no more PlaceHolderInfos may be
    /// added.
    pub placeholdersFrozen: bool,
    /// `bool hasRecursion` — true if planning a recursive WITH item.
    #[allow(non_snake_case)]
    pub hasRecursion: bool,
    /// `int group_rtindex` — RT index for the RTE_GROUP RTE, or 0 if none.
    pub group_rtindex: i32,
    /// `List *agginfos` — AggInfo structs (opaque node handles).
    pub agginfos: Vec<NodeId>,
    /// `List *aggtransinfos` — AggTransInfo structs (opaque node handles).
    pub aggtransinfos: Vec<NodeId>,
    /// `int numOrderedAggs` — # aggs with DISTINCT/ORDER BY/WITHIN GROUP.
    #[allow(non_snake_case)]
    pub numOrderedAggs: i32,
    /// `bool hasNonPartialAggs` — does any agg not support partial mode?
    #[allow(non_snake_case)]
    pub hasNonPartialAggs: bool,
    /// `bool hasNonSerialAggs` — is any partial agg non-serializable?
    #[allow(non_snake_case)]
    pub hasNonSerialAggs: bool,
    /// `int wt_param_id` — PARAM_EXEC ID for the work table (recursion only).
    pub wt_param_id: i32,
    /// `struct Path *non_recursive_path` — path for the non-recursive term
    /// (handle into `path_arena`).
    pub non_recursive_path: Option<PathId>,
    /// `Relids curOuterRels` — outer rels above the current node (createplan).
    #[allow(non_snake_case)]
    pub curOuterRels: Relids,
    /// `List *curOuterParams` — not-yet-assigned NestLoopParams (opaque node
    /// handles).
    #[allow(non_snake_case)]
    pub curOuterParams: Vec<NodeId>,
    /// `bool partColsUpdated` — does this query modify any partition key column?
    #[allow(non_snake_case)]
    pub partColsUpdated: bool,
    /// `List *partPruneInfos` — PartitionPruneInfos added in this query's plan
    /// (opaque node handles).
    #[allow(non_snake_case)]
    pub partPruneInfos: Vec<NodeId>,

    /* Arenas (owned-tree arena + handle model — not in the C struct). */
    /// Backing store for every [`RelOptInfo`]; a [`RelId`] indexes here.
    pub rel_arena: Vec<RelOptInfo>,
    /// Backing store for every [`PathNode`]; a [`PathId`] indexes here.
    pub path_arena: Vec<PathNode>,
    /// Backing store for every [`RestrictInfo`]; a [`RinfoId`] indexes here.
    pub rinfo_arena: Vec<RestrictInfo>,
    /// Backing store for every [`EquivalenceClass`]; an [`EcId`] indexes here.
    pub eq_classes: Vec<EquivalenceClass>,
    /// Backing store for every [`EquivalenceMember`]; an [`EmId`] indexes here.
    /// (The C planner has no single EM list; EMs alias from `ec_members`/
    /// `ec_childmembers`/`em_parent`, so the arena gives each one identity.)
    pub em_arena: Vec<EquivalenceMember>,
    /// Backing store for every [`PlaceHolderInfo`]; a [`PhInfoId`] indexes here.
    pub ph_info_arena: Vec<PlaceHolderInfo>,
    /// Backing store for every expression node carried by a [`NodeId`] — the
    /// owned-tree analogue of the C `Node *` expression payloads the planner
    /// interns (PathTarget `exprs`, RestrictInfo `clause`, index `indexprs`/
    /// `indpred`, lateral-var lists, etc.). A [`NodeId`] indexes here.
    ///
    /// The payload is the **lifetime-free** [`Expr`] (its child `SubPlan`s pin
    /// to `'static`), so the node store can be added without forcing an `'mcx`
    /// lifetime onto [`PlannerInfo`]; this mirrors the rationale at
    /// [`Expr`]'s definition and keeps the arena model identical to the
    /// `rel_arena`/`path_arena`/`rinfo_arena`/`em_arena` siblings.
    ///
    /// Node-walking owners (var.c / clauses.c via their seams) resolve a
    /// [`NodeId`] to `&Expr` through [`PlannerInfo::node`] and walk the tree;
    /// the join-path enumerator still only forwards/compares the opaque handle.
    pub node_arena: Vec<Expr>,
}

impl PlannerInfo {
    /// Resolve a [`RelId`] to its [`RelOptInfo`].
    #[inline]
    pub fn rel(&self, id: RelId) -> &RelOptInfo {
        &self.rel_arena[id.index()]
    }
    /// Resolve a [`RelId`] for mutation.
    #[inline]
    pub fn rel_mut(&mut self, id: RelId) -> &mut RelOptInfo {
        &mut self.rel_arena[id.index()]
    }
    /// Resolve a [`PathId`] to its [`PathNode`].
    #[inline]
    pub fn path(&self, id: PathId) -> &PathNode {
        &self.path_arena[id.index()]
    }
    /// Resolve a [`PathId`] for mutation.
    #[inline]
    pub fn path_mut(&mut self, id: PathId) -> &mut PathNode {
        &mut self.path_arena[id.index()]
    }
    /// Resolve a [`RinfoId`] to its [`RestrictInfo`].
    #[inline]
    pub fn rinfo(&self, id: RinfoId) -> &RestrictInfo {
        &self.rinfo_arena[id.index()]
    }
    /// Resolve a [`RinfoId`] for mutation.
    #[inline]
    pub fn rinfo_mut(&mut self, id: RinfoId) -> &mut RestrictInfo {
        &mut self.rinfo_arena[id.index()]
    }
    /// Resolve an [`EcId`] to its [`EquivalenceClass`].
    #[inline]
    pub fn ec(&self, id: EcId) -> &EquivalenceClass {
        &self.eq_classes[id.index()]
    }
    /// Resolve an [`EcId`] for mutation.
    #[inline]
    pub fn ec_mut(&mut self, id: EcId) -> &mut EquivalenceClass {
        &mut self.eq_classes[id.index()]
    }
    /// Follow the `ec_merged` union-find link (with no path compression, as in
    /// C, where `ec_merged` always points directly at the surviving EC) to the
    /// canonical [`EcId`]. equivclass.c never chains merges more than one level
    /// because a merge target is itself already canonical, but we chase to a
    /// fixpoint to be safe.
    #[inline]
    pub fn ec_canonical(&self, id: EcId) -> EcId {
        let mut cur = id;
        while let Some(next) = self.eq_classes[cur.index()].ec_merged {
            cur = next;
        }
        cur
    }
    /// Push an [`EquivalenceClass`] into the arena, returning its [`EcId`]. The
    /// arena index doubles as the C `list_nth(root->eq_classes, i)` position
    /// that `RelOptInfo::eclass_indexes` bitmaps reference.
    #[inline]
    pub fn alloc_ec(&mut self, ec: EquivalenceClass) -> EcId {
        let id = EcId(self.eq_classes.len() as u32);
        self.eq_classes.push(ec);
        id
    }
    /// Resolve a [`PhInfoId`] to its [`PlaceHolderInfo`].
    #[inline]
    pub fn phinfo(&self, id: PhInfoId) -> &PlaceHolderInfo {
        &self.ph_info_arena[id.index()]
    }
    /// Resolve an [`EmId`] to its [`EquivalenceMember`].
    #[inline]
    pub fn em(&self, id: EmId) -> &EquivalenceMember {
        &self.em_arena[id.index()]
    }
    /// Resolve an [`EmId`] for mutation.
    #[inline]
    pub fn em_mut(&mut self, id: EmId) -> &mut EquivalenceMember {
        &mut self.em_arena[id.index()]
    }
    /// Resolve a [`NodeId`] to its expression [`Expr`] — the deref behind the
    /// opaque `Node *` handle. Node-walking seam owners (var.c / clauses.c)
    /// call this to obtain `&Expr` and recurse.
    #[inline]
    pub fn node(&self, id: NodeId) -> &Expr {
        &self.node_arena[id.index()]
    }
    /// Resolve a [`NodeId`] for mutation.
    #[inline]
    pub fn node_mut(&mut self, id: NodeId) -> &mut Expr {
        &mut self.node_arena[id.index()]
    }

    /// Push a [`RelOptInfo`] into the arena, returning its [`RelId`].
    #[inline]
    pub fn alloc_rel(&mut self, rel: RelOptInfo) -> RelId {
        let id = RelId(self.rel_arena.len() as u32);
        self.rel_arena.push(rel);
        id
    }
    /// Push a [`PathNode`] into the arena, returning its [`PathId`].
    #[inline]
    pub fn alloc_path(&mut self, path: PathNode) -> PathId {
        let id = PathId(self.path_arena.len() as u32);
        self.path_arena.push(path);
        id
    }
    /// Push a [`RestrictInfo`] into the arena, returning its [`RinfoId`].
    #[inline]
    pub fn alloc_rinfo(&mut self, rinfo: RestrictInfo) -> RinfoId {
        let id = RinfoId(self.rinfo_arena.len() as u32);
        self.rinfo_arena.push(rinfo);
        id
    }
    /// Push an [`EquivalenceMember`] into the arena, returning its [`EmId`].
    #[inline]
    pub fn alloc_em(&mut self, em: EquivalenceMember) -> EmId {
        let id = EmId(self.em_arena.len() as u32);
        self.em_arena.push(em);
        id
    }
    /// Intern an [`Expr`] into the node store, returning its [`NodeId`] handle.
    /// The producer path: the planner (and the optimizer leaves as they
    /// construct PathTargets/RestrictInfos) call this to obtain the `NodeId`
    /// stored in the W0''-added `exprs`/`clause`/… fields, giving every such
    /// field a real backing node that the walking seams can dereference.
    #[inline]
    pub fn alloc_node(&mut self, node: Expr) -> NodeId {
        let id = NodeId(self.node_arena.len() as u32);
        self.node_arena.push(node);
        id
    }
}
