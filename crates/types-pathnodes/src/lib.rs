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

extern crate alloc;

pub mod optimizer_plan;

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::primitive::{
    AttrNumber, BlockNumber, Cardinality, Cost, Index,
};
pub use types_core::primitive::Oid;
pub use types_nodes::nodes::NodeTag;

/* ==========================================================================
 * Relids — a planner relation-id set (`Bitmapset *`).
 *
 * The empty set is `None` (the planner convention; relnode.c's seam docs
 * spell this out). The set algebra (`bms_*`) is owned by the not-yet-ported
 * nodes/bitmapset.c + relnode.c and is reached through the `relids_*` seams,
 * so this type is an opaque owned value here: a word-vector planner bitmapset,
 * planner-context-lived (the C `palloc`ed `Bitmapset`).
 * ======================================================================== */

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

/// `PathTarget` — the output columns a Path computes. Trimmed (the `exprs` node
/// list belongs to the owning crate; cost/width are the consumed scalars).
#[derive(Clone, Debug, Default)]
pub struct PathTarget {
    pub cost: QualCost,
    pub width: i32,
}

/// `ParamPathInfo` — shared parameterization info for a set of paths. Trimmed to
/// the fields `PATH_REQ_OUTER` and the parameterized-path machinery read.
#[derive(Clone, Debug)]
pub struct ParamPathInfo {
    pub ppi_req_outer: Relids,
    pub ppi_rows: Cardinality,
    /// join clauses available from outer rels — handles into `rinfo_arena`.
    pub ppi_clauses: Vec<RinfoId>,
}

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

/// The polymorphic path-arena element — the owned-tree analogue of a `Path *`
/// that may point at any path subtype. The path constructors (`pathnode.c`,
/// reached via seams) mint the concrete variants; the enumerator reaches the
/// embedded base [`Path`] uniformly via [`PathNode::base`]. `#[non_exhaustive]`:
/// further path variants are added as the constructing units land.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PathNode {
    /// `T_Path` — a plain base/scan path.
    Path(Path),
    /// `T_NestPath`.
    NestPath(NestPath),
    /// `T_MergePath`.
    MergePath(MergePath),
    /// `T_HashPath`.
    HashPath(HashPath),
}

impl PathNode {
    /// Recover the embedded base [`Path`] (the analogue of the C up-cast
    /// `(Path *) subtype`).
    pub fn base(&self) -> &Path {
        match self {
            PathNode::Path(p) => p,
            PathNode::NestPath(p) => &p.jpath.path,
            PathNode::MergePath(p) => &p.jpath.path,
            PathNode::HashPath(p) => &p.jpath.path,
        }
    }

    /// Recover the embedded base [`Path`] for mutation.
    pub fn base_mut(&mut self) -> &mut Path {
        match self {
            PathNode::Path(p) => p,
            PathNode::NestPath(p) => &mut p.jpath.path,
            PathNode::MergePath(p) => &mut p.jpath.path,
            PathNode::HashPath(p) => &mut p.jpath.path,
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
    pub is_pushed_down: bool,
    pub can_join: bool,
    pub pseudoconstant: bool,
    pub clause_relids: Relids,
    pub required_relids: Relids,
    pub outer_relids: Relids,
    pub left_relids: Relids,
    pub right_relids: Relids,
    /// generating EquivalenceClass, if any — handle into `eq_classes`.
    pub parent_ec: Option<EcId>,
    /// opfamilies containing clause operator (OIDs), valid if mergejoinable.
    pub mergeopfamilies: Vec<Oid>,
    /// EquivalenceClass containing the left operand — handle into `eq_classes`.
    pub left_ec: Option<EcId>,
    /// EquivalenceClass containing the right operand — handle into `eq_classes`.
    pub right_ec: Option<EcId>,
    pub outer_is_left: bool,
    pub hashjoinoperator: Oid,
}

/* ==========================================================================
 * EquivalenceClass (pathnodes.h) — only the fields the eclass-merge chase + the
 * redundancy predicate need. EquivalenceMembers / derive caches belong to the
 * equivclass.c crate and are reached through its seams.
 * ======================================================================== */

/// `EquivalenceClass` — a set of expressions known to be equal. Trimmed.
#[derive(Clone, Debug, Default)]
pub struct EquivalenceClass {
    /// `EquivalenceClass *ec_merged` — non-NULL if this EC was merged into
    /// another; the canonical EC is found by chasing this. A handle into
    /// `eq_classes`.
    pub ec_merged: Option<EcId>,
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

    /* Arenas (owned-tree arena + handle model — not in the C struct). */
    /// Backing store for every [`RelOptInfo`]; a [`RelId`] indexes here.
    pub rel_arena: Vec<RelOptInfo>,
    /// Backing store for every [`PathNode`]; a [`PathId`] indexes here.
    pub path_arena: Vec<PathNode>,
    /// Backing store for every [`RestrictInfo`]; a [`RinfoId`] indexes here.
    pub rinfo_arena: Vec<RestrictInfo>,
    /// Backing store for every [`EquivalenceClass`]; an [`EcId`] indexes here.
    pub eq_classes: Vec<EquivalenceClass>,
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
}
