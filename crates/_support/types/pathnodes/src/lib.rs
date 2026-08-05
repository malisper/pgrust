//! Planner path-graph vocabulary (nodes/pathnodes.h). C aliases
//! RelOptInfo/Path/RestrictInfo/EquivalenceClass/PathTarget by pointer inside
//! one bulk-freed planner context; here each lives in a [`PlannerInfo`] arena
//! and is shared by `Copy` u32 handle (fabled #295: PathTarget arena ids took
//! the planner 3.50x->2.52x; Rc there is refuted, fabled #401). Arenas only
//! grow within a planner run, so handles never dangle. Exceptions where C
//! mutates through a densely shared pointer built once: `&'mcx IndexOptInfo`
//! (forget-leaked into the planner arena) with `Cell`/`RefCell` on exactly
//! the C-mutated fields (fabled #356), and a borrowed `SpecialJoinInfo` in
//! `JoinPathExtraData` (fabled #401 refuted the by-value clone).
//!
//! types-nodes boundary (in-flight crate; no dep taken): expression/parse
//! payloads are opaque [`NodeId`]/[`QueryId`] handles. Deferred until
//! types_nodes commits its API: the Expr/SortGroupClause/WithCheckOption/
//! MergeAction/NestLoopParam/RowIdentityVar arena variants and accessors,
//! `PlaceHolderInfo.ph_var`, `RelOptInfo.fdwroutine`, the PlannerRun
//! query/RTE/subplan stores, and `PlannerGlobal`'s append_relations/
//! part_prune_infos/inval_items/partition_directory/bound_params fields.
//! `pathtype`/`type_` carry the raw `nodetags.h` discriminant
//! ([`NodeTagValue`], `types_nodes::NodeTag as u16`) until then.

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

extern crate alloc;

pub mod optimizer_plan;
pub mod relids;
pub mod run;
#[cfg(test)]
mod relids_differential_tests;

use core::cell::{Cell, RefCell};

pub use ::mcx::{Mcx, PgBox, PgString, PgVec};

use ::datum::datum::Datum;
use ::types_core::fmgr::FmgrInfo;
use ::types_core::primitive::{
    AttrNumber, BlockNumber, Cardinality, Cost, Index, Selectivity, Size,
};
pub use ::types_core::primitive::Oid;
pub use ::types_hash::hsearch::HTAB;

pub type NodeTagValue = u16;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinSearchPrivate {}

// One-word sets carry the word inline: one arena allocation, like C's
// single-palloc bms. Small(w) is exactly a len-1 word vec; representation is
// deterministic by word count, so slice-equality semantics are unchanged.
#[derive(Clone, Debug)]
pub enum Bitmapset<'mcx> {
    Small(u64),
    Big(PgVec<'mcx, u64>),
}

impl<'mcx> Bitmapset<'mcx> {
    #[inline]
    pub fn word_slice(&self) -> &[u64] {
        match self {
            Bitmapset::Small(w) => core::slice::from_ref(w),
            Bitmapset::Big(v) => v.as_slice(),
        }
    }
    #[inline]
    pub fn word_slice_mut(&mut self) -> &mut [u64] {
        match self {
            Bitmapset::Small(w) => core::slice::from_mut(w),
            Bitmapset::Big(v) => v.as_mut_slice(),
        }
    }
}

impl PartialEq for Bitmapset<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.word_slice() == other.word_slice()
    }
}
impl Eq for Bitmapset<'_> {}

/// `Bitmapset *`; the empty set is `None` (planner convention). Boxed
/// representation of record, kept for bisection behind `boxed_relids`.
#[cfg(feature = "boxed_relids")]
pub type Relids<'mcx> = Option<PgBox<'mcx, Bitmapset<'mcx>>>;

/// `Bitmapset *`, by value. `Empty` is the unset value (C's NULL / the old
/// `None`); `Small` carries a one-word set inline — zero allocation for every
/// set whose max member is < 64, i.e. all short-query shapes; `Big` stores
/// the multi-word words directly (one allocation, no box indirection).
///
/// Value identity is the word slice, verbatim: `Empty` is `[]`, `Small(w)`
/// is `[w]`, `Big(v)` is `v`. The relids_* helpers reproduce the boxed
/// representation's word slices bit-for-bit — including its non-canonical
/// values (allocated all-zero sets distinct from `Empty`, trailing zero
/// words preserved) — so every comparison, and therefore every plan, is
/// unchanged by construction. Pinned by relids_differential_tests.
#[cfg(not(feature = "boxed_relids"))]
#[derive(Clone, Debug)]
pub enum Relids<'mcx> {
    Empty,
    Small(u64),
    Big(PgVec<'mcx, u64>),
}

#[cfg(not(feature = "boxed_relids"))]
impl<'mcx> Relids<'mcx> {
    #[inline]
    pub fn word_slice(&self) -> &[u64] {
        match self {
            Relids::Empty => &[],
            Relids::Small(w) => core::slice::from_ref(w),
            Relids::Big(v) => v.as_slice(),
        }
    }
    #[inline]
    pub fn word_slice_mut(&mut self) -> &mut [u64] {
        match self {
            Relids::Empty => &mut [],
            Relids::Small(w) => core::slice::from_mut(w),
            Relids::Big(v) => v.as_mut_slice(),
        }
    }
}

#[cfg(not(feature = "boxed_relids"))]
impl Default for Relids<'_> {
    #[inline]
    fn default() -> Self {
        Relids::Empty
    }
}

// Same equality as the boxed repr: unset equals only unset (an allocated
// all-zero set is NOT unset); otherwise verbatim word-slice comparison.
#[cfg(not(feature = "boxed_relids"))]
impl PartialEq for Relids<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Relids::Empty, Relids::Empty) => true,
            (Relids::Empty, _) | (_, Relids::Empty) => false,
            _ => self.word_slice() == other.word_slice(),
        }
    }
}
#[cfg(not(feature = "boxed_relids"))]
impl Eq for Relids<'_> {}

#[cfg(not(feature = "boxed_relids"))]
mcx::forget_safe_enum!(Relids<'_> { Empty, Small(x), Big(x) });

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

pub type RTEKind = u32;
pub const RTE_RELATION: RTEKind = 0;
pub const RTE_SUBQUERY: RTEKind = 1;
pub const RTE_FUNCTION: RTEKind = 3;
pub const RTE_TABLEFUNC: RTEKind = 4;
pub const RTE_VALUES: RTEKind = 5;
pub const RTE_CTE: RTEKind = 6;
pub const RTE_NAMEDTUPLESTORE: RTEKind = 7;

pub type RelOptKind = u32;
pub const RELOPT_BASEREL: RelOptKind = 0;
pub const RELOPT_JOINREL: RelOptKind = 1;
pub const RELOPT_OTHER_MEMBER_REL: RelOptKind = 2;
pub const RELOPT_OTHER_JOINREL: RelOptKind = 3;
pub const RELOPT_UPPER_REL: RelOptKind = 4;
pub const RELOPT_OTHER_UPPER_REL: RelOptKind = 5;

// access/cmptype.h (values are wire-visible in pg_amop logic).
pub type CompareType = i32;
pub const COMPARE_INVALID: CompareType = 0;
pub const COMPARE_LT: CompareType = 1;
pub const COMPARE_LE: CompareType = 2;
pub const COMPARE_EQ: CompareType = 3;
pub const COMPARE_GE: CompareType = 4;
pub const COMPARE_GT: CompareType = 5;
pub const COMPARE_NE: CompareType = 6;
pub const COMPARE_OVERLAP: CompareType = 7;
pub const COMPARE_CONTAINED_BY: CompareType = 8;

pub type VolatileFunctionStatus = u32;
pub const VOLATILITY_UNKNOWN: VolatileFunctionStatus = 0;
pub const VOLATILITY_VOLATILE: VolatileFunctionStatus = 1;
pub const VOLATILITY_NOVOLATILE: VolatileFunctionStatus = 2;

pub type UpperRelationKind = u32;
pub const UPPERREL_SETOP: UpperRelationKind = 0;
pub const UPPERREL_PARTIAL_GROUP_AGG: UpperRelationKind = 1;
pub const UPPERREL_GROUP_AGG: UpperRelationKind = 2;
pub const UPPERREL_WINDOW: UpperRelationKind = 3;
pub const UPPERREL_PARTIAL_DISTINCT: UpperRelationKind = 4;
pub const UPPERREL_DISTINCT: UpperRelationKind = 5;
pub const UPPERREL_ORDERED: UpperRelationKind = 6;
pub const UPPERREL_FINAL: UpperRelationKind = 7;
pub const NUM_UPPERREL_KINDS: usize = (UPPERREL_FINAL as usize) + 1;

pub type ScanDirection = i32;
pub const BackwardScanDirection: ScanDirection = -1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const ForwardScanDirection: ScanDirection = 1;

pub type CmdType = u32;
pub const CMD_UNKNOWN: CmdType = 0;
pub const CMD_SELECT: CmdType = 1;
pub const CMD_UPDATE: CmdType = 2;
pub const CMD_INSERT: CmdType = 3;
pub const CMD_DELETE: CmdType = 4;
pub const CMD_MERGE: CmdType = 5;
pub const CMD_UTILITY: CmdType = 6;
pub const CMD_NOTHING: CmdType = 7;

pub type AggStrategy = u32;
pub const AGG_PLAIN: AggStrategy = 0;
pub const AGG_SORTED: AggStrategy = 1;
pub const AGG_HASHED: AggStrategy = 2;
pub const AGG_MIXED: AggStrategy = 3;

pub type AggSplit = u32;
pub const AGGSPLITOP_COMBINE: AggSplit = 0x01;
pub const AGGSPLITOP_SKIPFINAL: AggSplit = 0x02;
pub const AGGSPLITOP_SERIALIZE: AggSplit = 0x04;
pub const AGGSPLITOP_DESERIALIZE: AggSplit = 0x08;
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE;
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE;

pub type SetOpCmd = u32;
pub const SETOPCMD_INTERSECT: SetOpCmd = 0;
pub const SETOPCMD_INTERSECT_ALL: SetOpCmd = 1;
pub const SETOPCMD_EXCEPT: SetOpCmd = 2;
pub const SETOPCMD_EXCEPT_ALL: SetOpCmd = 3;

pub type SetOpStrategy = u32;
pub const SETOP_SORTED: SetOpStrategy = 0;
pub const SETOP_HASHED: SetOpStrategy = 1;

pub type LimitOption = u32;
pub const LIMIT_OPTION_COUNT: LimitOption = 0;
pub const LIMIT_OPTION_WITH_TIES: LimitOption = 1;

pub type UniquePathMethod = u32;
pub const UNIQUE_PATH_NOOP: UniquePathMethod = 0;
pub const UNIQUE_PATH_HASH: UniquePathMethod = 1;
pub const UNIQUE_PATH_SORT: UniquePathMethod = 2;

pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: u32 = 0x0001;
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: u32 = 0x0002;
pub const CUSTOMPATH_SUPPORT_PROJECTION: u32 = 0x0004;

macro_rules! arena_handle {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[repr(transparent)]
        pub struct $name(pub u32);
        impl $name {
            #[inline]
            pub fn index(self) -> usize {
                self.0 as usize
            }
        }
        const _: () = assert!(core::mem::size_of::<$name>() == 4);
    )+};
}

arena_handle!(RelId, PathId, PtId, RinfoId, EcId, EmId, PhInfoId, NodeId, PlanId);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct QueryId(pub u32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RangeTblEntryId {
    #[default]
    Invalid,
    Parse { query: QueryId, index: u32 },
    Flat(u32),
}

impl RangeTblEntryId {
    #[inline]
    pub fn flat_index(self) -> u32 {
        match self {
            RangeTblEntryId::Flat(i) => i,
            other => panic!("flat_index on non-Flat RangeTblEntryId: {other:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct PlanRowMarkId(pub u32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RtePermInfoId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinlistNode<'mcx> {
    Rel(i32),
    Sub(PgVec<'mcx, JoinlistNode<'mcx>>),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ECDerivesKey {
    pub em1: Option<EmId>,
    pub em2: Option<EmId>,
    pub parent_ec: Option<EcId>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ECDerivesEntry {
    pub status: u32,
    pub key: ECDerivesKey,
    pub rinfo: Option<RinfoId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivesHash<'mcx> {
    pub size: u64,
    pub sizemask: u32,
    pub members: u32,
    pub grow_threshold: u32,
    pub data: PgVec<'mcx, ECDerivesEntry>,
}

impl<'mcx> DerivesHash<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        DerivesHash {
            size: 0,
            sizemask: 0,
            members: 0,
            grow_threshold: 0,
            data: PgVec::new_in(mcx),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MergeScanSelCache {
    pub opfamily: Oid,
    pub collation: Oid,
    pub cmptype: CompareType,
    pub nulls_first: bool,
    pub leftstartsel: Selectivity,
    pub leftendsel: Selectivity,
    pub rightstartsel: Selectivity,
    pub rightendsel: Selectivity,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct QualCost {
    pub startup: Cost,
    pub per_tuple: Cost,
}

#[derive(Clone, Debug)]
pub struct PartitionSchemeData<'mcx> {
    pub strategy: i8,
    pub partnatts: i16,
    pub partopfamily: PgVec<'mcx, Oid>,
    pub partopcintype: PgVec<'mcx, Oid>,
    pub partcollation: PgVec<'mcx, Oid>,
    pub parttyplen: PgVec<'mcx, i16>,
    pub parttypbyval: PgVec<'mcx, bool>,
    pub partsupfunc: PgVec<'mcx, FmgrInfo>,
}

impl<'mcx> PartitionSchemeData<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PartitionSchemeData {
            strategy: 0,
            partnatts: 0,
            partopfamily: PgVec::new_in(mcx),
            partopcintype: PgVec::new_in(mcx),
            partcollation: PgVec::new_in(mcx),
            parttyplen: PgVec::new_in(mcx),
            parttypbyval: PgVec::new_in(mcx),
            partsupfunc: PgVec::new_in(mcx),
        }
    }
}

// `partsupfunc` compares by resolved fn_oid — the `find_partition_scheme`
// matching key; fn_addr/fn_expr are derived from it.
impl<'mcx> PartialEq for PartitionSchemeData<'mcx> {
    fn eq(&self, other: &Self) -> bool {
        self.strategy == other.strategy
            && self.partnatts == other.partnatts
            && self.partopfamily == other.partopfamily
            && self.partopcintype == other.partopcintype
            && self.partcollation == other.partcollation
            && self.parttyplen == other.parttyplen
            && self.parttypbyval == other.parttypbyval
            && self.partsupfunc.len() == other.partsupfunc.len()
            && self
                .partsupfunc
                .iter()
                .zip(other.partsupfunc.iter())
                .all(|(a, b)| a.fn_oid == b.fn_oid)
    }
}

pub type PartitionScheme<'mcx> = Option<PgBox<'mcx, PartitionSchemeData<'mcx>>>;

/// Raw datum image for `datumIsEqual`-only comparisons (partition bounds are
/// never compared through an operator at this layer).
///
/// ByVal carries the full 8-byte Datum word (SIZEOF_DATUM is pinned to 8 on
/// every target): a usize image on wasm32 truncated int8-class partition
/// bounds, so pruning/partitionwise comparisons ran on the low half only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatumImage<'mcx> {
    ByVal(u64),
    Bytes(PgVec<'mcx, u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionBoundInfoData<'mcx> {
    pub strategy: i8,
    pub ndatums: i32,
    pub nindexes: i32,
    pub null_index: i32,
    pub default_index: i32,
    pub indexes: PgVec<'mcx, i32>,
    pub datums: PgVec<'mcx, PgVec<'mcx, DatumImage<'mcx>>>,
    pub kind: Option<PgVec<'mcx, PgVec<'mcx, i8>>>,
    pub interleaved_parts: Relids<'mcx>,
}

impl<'mcx> PartitionBoundInfoData<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PartitionBoundInfoData {
            strategy: 0,
            ndatums: 0,
            nindexes: 0,
            null_index: -1,
            default_index: -1,
            indexes: PgVec::new_in(mcx),
            datums: PgVec::new_in(mcx),
            kind: None,
            interleaved_parts: relids::relids_empty(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinDomain<'mcx> {
    pub jd_relids: Relids<'mcx>,
}

#[derive(Clone, Debug)]
pub struct AppendRelInfo<'mcx> {
    pub parent_relid: Index,
    pub child_relid: Index,
    pub parent_reltype: Oid,
    pub child_reltype: Oid,
    /// `NodeId::default()` (0) is the NULL element (dropped parent column).
    pub translated_vars: PgVec<'mcx, NodeId>,
    pub num_child_cols: i32,
    pub parent_colnos: PgVec<'mcx, AttrNumber>,
    pub parent_reloid: Oid,
}

impl<'mcx> AppendRelInfo<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        AppendRelInfo {
            parent_relid: 0,
            child_relid: 0,
            parent_reltype: 0,
            child_reltype: 0,
            translated_vars: PgVec::new_in(mcx),
            num_child_cols: 0,
            parent_colnos: PgVec::new_in(mcx),
            parent_reloid: 0,
        }
    }
}

/// RowIdentityVarInfo (pathnodes.h): `rowidvar` is an interned Var with
/// varno = ROWID_VAR and varattno = its 1-based index in row_identity_vars.
#[derive(Clone, Debug)]
pub struct RowIdentityVarInfo<'mcx> {
    pub rowidvar: NodeId,
    pub rowidwidth: i32,
    pub rowidname: &'mcx str,
    pub rowidrels: Relids<'mcx>,
}

/// C builds each IndexOptInfo once (get_relation_info) and every consumer
/// shares the pointer; `tree_height`/`predOK`/`indrestrictinfo` are the only
/// fields C mutates through it afterwards (fabled #356).
#[derive(Debug)]
pub struct IndexOptInfo<'mcx> {
    pub indexoid: Oid,
    pub reltablespace: Oid,
    pub rel: Option<RelId>,
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    pub tree_height: Cell<i32>,
    pub ncolumns: i32,
    pub nkeycolumns: i32,
    pub indexkeys: PgVec<'mcx, i32>,
    pub indexcollations: PgVec<'mcx, Oid>,
    pub opfamily: PgVec<'mcx, Oid>,
    pub opcintype: PgVec<'mcx, Oid>,
    pub sortopfamily: PgVec<'mcx, Oid>,
    pub reverse_sort: PgVec<'mcx, bool>,
    pub nulls_first: PgVec<'mcx, bool>,
    pub canreturn: PgVec<'mcx, bool>,
    pub relam: Oid,
    pub indexprs: PgVec<'mcx, NodeId>,
    pub indpred: PgVec<'mcx, NodeId>,
    pub indextlist: PgVec<'mcx, NodeId>,
    pub indrestrictinfo: RefCell<PgVec<'mcx, RinfoId>>,
    pub predOK: Cell<bool>,
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
    /// GIN metapage stats captured at plancat time (C's gincostestimate
    /// reopens the index; rule 6 passes them down instead). None for
    /// non-GIN indexes.
    pub gin_stats: Option<GinIndexStats>,
}

/// GinStatsData mirror for the planner (avoids a pathnodes->gin dep).
#[derive(Clone, Copy, Debug, Default)]
pub struct GinIndexStats {
    pub pending_pages: BlockNumber,
    pub total_pages: BlockNumber,
    pub entry_pages: BlockNumber,
    pub data_pages: BlockNumber,
    pub entries: i64,
    pub version: i32,
}

impl<'mcx> IndexOptInfo<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        IndexOptInfo {
            indexoid: 0,
            reltablespace: 0,
            rel: None,
            pages: 0,
            tuples: 0.0,
            tree_height: Cell::new(-1),
            ncolumns: 0,
            nkeycolumns: 0,
            indexkeys: PgVec::new_in(mcx),
            indexcollations: PgVec::new_in(mcx),
            opfamily: PgVec::new_in(mcx),
            opcintype: PgVec::new_in(mcx),
            sortopfamily: PgVec::new_in(mcx),
            reverse_sort: PgVec::new_in(mcx),
            nulls_first: PgVec::new_in(mcx),
            canreturn: PgVec::new_in(mcx),
            relam: 0,
            indexprs: PgVec::new_in(mcx),
            indpred: PgVec::new_in(mcx),
            indextlist: PgVec::new_in(mcx),
            indrestrictinfo: RefCell::new(PgVec::new_in(mcx)),
            predOK: Cell::new(false),
            unique: false,
            nullsnotdistinct: false,
            immediate: false,
            hypothetical: false,
            amcanorderbyop: false,
            amoptionalkey: false,
            amsearcharray: false,
            amsearchnulls: false,
            amhasgettuple: false,
            amhasgetbitmap: false,
            amcanparallel: false,
            amcanmarkpos: false,
            gin_stats: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PathKey {
    pub pk_eclass: Option<EcId>,
    pub pk_opfamily: Oid,
    pub pk_cmptype: CompareType,
    pub pk_nulls_first: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupByOrdering<'mcx> {
    pub pathkeys: PgVec<'mcx, PathKey>,
    pub clauses: PgVec<'mcx, NodeId>,
}

impl<'mcx> GroupByOrdering<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        GroupByOrdering {
            pathkeys: PgVec::new_in(mcx),
            clauses: PgVec::new_in(mcx),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PathTarget<'mcx> {
    pub exprs: PgVec<'mcx, NodeId>,
    /// One entry per `exprs` element; 0 = no ref. Empty = C NULL array.
    pub sortgrouprefs: PgVec<'mcx, u32>,
    pub cost: QualCost,
    pub width: i32,
    pub has_volatile_expr: VolatileFunctionStatus,
}

impl<'mcx> PathTarget<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PathTarget {
            exprs: PgVec::new_in(mcx),
            sortgrouprefs: PgVec::new_in(mcx),
            cost: QualCost::default(),
            width: 0,
            has_volatile_expr: VOLATILITY_UNKNOWN,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParamPathInfo<'mcx> {
    pub ppi_req_outer: Relids<'mcx>,
    pub ppi_rows: Cardinality,
    pub ppi_clauses: PgVec<'mcx, RinfoId>,
    pub ppi_serials: Relids<'mcx>,
}

#[derive(Clone, Debug)]
pub struct Path<'mcx> {
    pub type_: NodeTagValue,
    pub pathtype: NodeTagValue,
    pub parent: RelId,
    pub pathtarget_id: Option<PtId>,
    pub param_info: Option<PgBox<'mcx, ParamPathInfo<'mcx>>>,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub parallel_workers: i32,
    pub rows: Cardinality,
    pub disabled_nodes: i32,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub pathkeys: PgVec<'mcx, PathKey>,
}

#[derive(Clone, Debug)]
pub struct JoinPath<'mcx> {
    pub path: Path<'mcx>,
    pub jointype: JoinType,
    pub inner_unique: bool,
    pub outerjoinpath: Option<PathId>,
    pub innerjoinpath: Option<PathId>,
    pub joinrestrictinfo: PgVec<'mcx, RinfoId>,
}

#[derive(Clone, Debug)]
pub struct NestPath<'mcx> {
    pub jpath: JoinPath<'mcx>,
}

#[derive(Clone, Debug)]
pub struct MergePath<'mcx> {
    pub jpath: JoinPath<'mcx>,
    pub path_mergeclauses: PgVec<'mcx, RinfoId>,
    pub outersortkeys: PgVec<'mcx, PathKey>,
    pub innersortkeys: PgVec<'mcx, PathKey>,
    pub outer_presorted_keys: i32,
    pub skip_mark_restore: bool,
    pub materialize_inner: bool,
}

#[derive(Clone, Debug)]
pub struct HashPath<'mcx> {
    pub jpath: JoinPath<'mcx>,
    pub path_hashclauses: PgVec<'mcx, RinfoId>,
    pub num_batches: i32,
    pub inner_rows_total: Cardinality,
}

#[derive(Clone, Debug)]
pub struct IndexClause<'mcx> {
    pub rinfo: Option<RinfoId>,
    pub indexquals: PgVec<'mcx, RinfoId>,
    pub lossy: bool,
    pub indexcol: AttrNumber,
    pub indexcols: PgVec<'mcx, AttrNumber>,
}

#[derive(Clone, Debug)]
pub struct IndexPath<'mcx> {
    pub path: Path<'mcx>,
    pub indexinfo: Option<&'mcx IndexOptInfo<'mcx>>,
    pub indexclauses: PgVec<'mcx, IndexClause<'mcx>>,
    pub indexorderbys: PgVec<'mcx, NodeId>,
    pub indexorderbycols: PgVec<'mcx, i32>,
    pub indexscandir: ScanDirection,
    pub indextotalcost: Cost,
    pub indexselectivity: Selectivity,
}

#[derive(Clone, Debug)]
pub struct BitmapHeapPath<'mcx> {
    pub path: Path<'mcx>,
    pub bitmapqual: Option<PathId>,
}

#[derive(Clone, Debug)]
pub struct BitmapAndPath<'mcx> {
    pub path: Path<'mcx>,
    pub bitmapquals: PgVec<'mcx, PathId>,
    pub bitmapselectivity: Selectivity,
}

#[derive(Clone, Debug)]
pub struct BitmapOrPath<'mcx> {
    pub path: Path<'mcx>,
    pub bitmapquals: PgVec<'mcx, PathId>,
    pub bitmapselectivity: Selectivity,
}

#[derive(Clone, Debug)]
pub struct TidPath<'mcx> {
    pub path: Path<'mcx>,
    pub tidquals: PgVec<'mcx, RinfoId>,
}

#[derive(Clone, Debug)]
pub struct TidRangePath<'mcx> {
    pub path: Path<'mcx>,
    pub tidrangequals: PgVec<'mcx, RinfoId>,
}

#[derive(Clone, Debug)]
pub struct SubqueryScanPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub subroot_subpath: Option<PathId>,
}

#[derive(Clone, Debug)]
pub struct ForeignPath<'mcx> {
    pub path: Path<'mcx>,
    pub fdw_outerpath: Option<PathId>,
    pub fdw_restrictinfo: PgVec<'mcx, RinfoId>,
    pub fdw_private: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct CustomPath<'mcx> {
    pub path: Path<'mcx>,
    pub flags: u32,
    pub custom_paths: PgVec<'mcx, PathId>,
    pub custom_restrictinfo: PgVec<'mcx, RinfoId>,
    pub custom_private: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct AppendPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpaths: PgVec<'mcx, PathId>,
    pub first_partial_path: i32,
    pub limit_tuples: Cardinality,
}

#[derive(Clone, Debug)]
pub struct MergeAppendPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpaths: PgVec<'mcx, PathId>,
    pub limit_tuples: Cardinality,
}

#[derive(Clone, Debug)]
pub struct GroupResultPath<'mcx> {
    pub path: Path<'mcx>,
    pub quals: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct MaterialPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
}

#[derive(Clone, Debug)]
pub struct MemoizePath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub hash_operators: PgVec<'mcx, Oid>,
    pub param_exprs: PgVec<'mcx, NodeId>,
    pub singlerow: bool,
    pub binary_mode: bool,
    pub calls: Cardinality,
    pub est_entries: u32,
}

#[derive(Clone, Debug)]
pub struct UniquePath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub umethod: UniquePathMethod,
    pub in_operators: PgVec<'mcx, Oid>,
    pub uniq_exprs: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct GatherPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub single_copy: bool,
    pub num_workers: i32,
}

#[derive(Clone, Debug)]
pub struct GatherMergePath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub num_workers: i32,
}

#[derive(Clone, Debug)]
pub struct ProjectionPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub dummypp: bool,
}

#[derive(Clone, Debug)]
pub struct ProjectSetPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
}

#[derive(Clone, Debug)]
pub struct SortPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
}

#[derive(Clone, Debug)]
pub struct IncrementalSortPath<'mcx> {
    pub spath: SortPath<'mcx>,
    pub nPresortedCols: i32,
}

#[derive(Clone, Debug)]
pub struct GroupPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub groupClause: PgVec<'mcx, NodeId>,
    pub qual: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct UpperUniquePath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub numkeys: i32,
}

#[derive(Clone, Debug)]
pub struct AggPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub aggstrategy: AggStrategy,
    pub aggsplit: AggSplit,
    pub numGroups: Cardinality,
    pub transitionSpace: u64,
    pub groupClause: PgVec<'mcx, NodeId>,
    pub qual: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct GroupingSetData<'mcx> {
    pub set: PgVec<'mcx, Index>,
    pub numGroups: Cardinality,
}

impl<'mcx> GroupingSetData<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        GroupingSetData {
            set: PgVec::new_in(mcx),
            numGroups: 0.0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RollupData<'mcx> {
    pub groupClause: PgVec<'mcx, NodeId>,
    pub gsets: PgVec<'mcx, PgVec<'mcx, i32>>,
    pub gsets_data: PgVec<'mcx, GroupingSetData<'mcx>>,
    pub numGroups: Cardinality,
    pub hashable: bool,
    pub is_hashed: bool,
}

impl<'mcx> RollupData<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        RollupData {
            groupClause: PgVec::new_in(mcx),
            gsets: PgVec::new_in(mcx),
            gsets_data: PgVec::new_in(mcx),
            numGroups: 0.0,
            hashable: false,
            is_hashed: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GroupingSetsPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub aggstrategy: AggStrategy,
    pub rollups: PgVec<'mcx, RollupData<'mcx>>,
    pub qual: PgVec<'mcx, NodeId>,
    pub transitionSpace: u64,
}

/// C MinMaxAggInfo (pathnodes.h): `subroot`/`path` live in
/// `PlannerRun::minmax_subroots[subroot_idx]`; `subroot_path` is a PathId in
/// THAT root's arena. `target` and `param` are NodeIds in the outer root.
#[derive(Clone, Copy, Debug, Default)]
pub struct MinMaxAggInfo {
    pub aggfnoid: Oid,
    pub aggsortop: Oid,
    pub target: NodeId,
    pub pathcost: Cost,
    pub param: NodeId,
    pub subroot_idx: Option<usize>,
    pub subroot_path: Option<PathId>,
}

#[derive(Clone, Debug)]
pub struct MinMaxAggPath<'mcx> {
    pub path: Path<'mcx>,
    pub mmaggregates: PgVec<'mcx, MinMaxAggInfo>,
    pub quals: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct WindowAggPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub winclause: NodeId,
    pub qual: PgVec<'mcx, NodeId>,
    pub runCondition: PgVec<'mcx, NodeId>,
    pub topwindow: bool,
}

#[derive(Clone, Debug)]
pub struct SetOpPath<'mcx> {
    pub path: Path<'mcx>,
    pub leftpath: Option<PathId>,
    pub rightpath: Option<PathId>,
    pub cmd: SetOpCmd,
    pub strategy: SetOpStrategy,
    pub groupList: PgVec<'mcx, NodeId>,
    pub numGroups: Cardinality,
}

#[derive(Clone, Debug)]
pub struct RecursiveUnionPath<'mcx> {
    pub path: Path<'mcx>,
    pub leftpath: Option<PathId>,
    pub rightpath: Option<PathId>,
    pub distinctList: PgVec<'mcx, NodeId>,
    pub wtParam: i32,
    pub numGroups: Cardinality,
}

#[derive(Clone, Debug)]
pub struct LockRowsPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub rowMarks: PgVec<'mcx, PlanRowMarkId>,
    pub epqParam: i32,
}

#[derive(Clone, Debug)]
pub struct ModifyTablePath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub operation: CmdType,
    pub canSetTag: bool,
    pub nominalRelation: Index,
    pub rootRelation: Index,
    pub partColsUpdated: bool,
    pub resultRelations: PgVec<'mcx, i32>,
    pub updateColnosLists: PgVec<'mcx, PgVec<'mcx, AttrNumber>>,
    pub withCheckOptionLists: PgVec<'mcx, PgVec<'mcx, NodeId>>,
    pub returningLists: PgVec<'mcx, PgVec<'mcx, NodeId>>,
    pub rowMarks: PgVec<'mcx, PlanRowMarkId>,
    pub onconflict: Option<NodeId>,
    pub epqParam: i32,
    pub mergeActionLists: PgVec<'mcx, PgVec<'mcx, NodeId>>,
    pub mergeJoinConditions: PgVec<'mcx, Option<NodeId>>,
}

#[derive(Clone, Debug)]
pub struct LimitPath<'mcx> {
    pub path: Path<'mcx>,
    pub subpath: Option<PathId>,
    pub limitOffset: Option<NodeId>,
    pub limitCount: Option<NodeId>,
    pub limitOption: LimitOption,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PathNode<'mcx> {
    Path(Path<'mcx>),
    IndexPath(IndexPath<'mcx>),
    BitmapHeapPath(BitmapHeapPath<'mcx>),
    BitmapAndPath(BitmapAndPath<'mcx>),
    BitmapOrPath(BitmapOrPath<'mcx>),
    TidPath(TidPath<'mcx>),
    TidRangePath(TidRangePath<'mcx>),
    SubqueryScanPath(SubqueryScanPath<'mcx>),
    ForeignPath(ForeignPath<'mcx>),
    CustomPath(CustomPath<'mcx>),
    NestPath(NestPath<'mcx>),
    MergePath(MergePath<'mcx>),
    HashPath(HashPath<'mcx>),
    AppendPath(AppendPath<'mcx>),
    MergeAppendPath(MergeAppendPath<'mcx>),
    GroupResultPath(GroupResultPath<'mcx>),
    MaterialPath(MaterialPath<'mcx>),
    MemoizePath(MemoizePath<'mcx>),
    UniquePath(UniquePath<'mcx>),
    GatherPath(GatherPath<'mcx>),
    GatherMergePath(GatherMergePath<'mcx>),
    ProjectionPath(ProjectionPath<'mcx>),
    ProjectSetPath(ProjectSetPath<'mcx>),
    SortPath(SortPath<'mcx>),
    IncrementalSortPath(IncrementalSortPath<'mcx>),
    GroupPath(GroupPath<'mcx>),
    UpperUniquePath(UpperUniquePath<'mcx>),
    AggPath(AggPath<'mcx>),
    GroupingSetsPath(GroupingSetsPath<'mcx>),
    MinMaxAggPath(MinMaxAggPath<'mcx>),
    WindowAggPath(WindowAggPath<'mcx>),
    SetOpPath(SetOpPath<'mcx>),
    RecursiveUnionPath(RecursiveUnionPath<'mcx>),
    LockRowsPath(LockRowsPath<'mcx>),
    ModifyTablePath(ModifyTablePath<'mcx>),
    LimitPath(LimitPath<'mcx>),
}

macro_rules! path_base {
    ($self:ident, $($amp:tt)+) => {
        match $self {
            PathNode::Path(p) => p,
            PathNode::IndexPath(p) => $($amp)+ p.path,
            PathNode::BitmapHeapPath(p) => $($amp)+ p.path,
            PathNode::BitmapAndPath(p) => $($amp)+ p.path,
            PathNode::BitmapOrPath(p) => $($amp)+ p.path,
            PathNode::TidPath(p) => $($amp)+ p.path,
            PathNode::TidRangePath(p) => $($amp)+ p.path,
            PathNode::SubqueryScanPath(p) => $($amp)+ p.path,
            PathNode::ForeignPath(p) => $($amp)+ p.path,
            PathNode::CustomPath(p) => $($amp)+ p.path,
            PathNode::NestPath(p) => $($amp)+ p.jpath.path,
            PathNode::MergePath(p) => $($amp)+ p.jpath.path,
            PathNode::HashPath(p) => $($amp)+ p.jpath.path,
            PathNode::AppendPath(p) => $($amp)+ p.path,
            PathNode::MergeAppendPath(p) => $($amp)+ p.path,
            PathNode::GroupResultPath(p) => $($amp)+ p.path,
            PathNode::MaterialPath(p) => $($amp)+ p.path,
            PathNode::MemoizePath(p) => $($amp)+ p.path,
            PathNode::UniquePath(p) => $($amp)+ p.path,
            PathNode::GatherPath(p) => $($amp)+ p.path,
            PathNode::GatherMergePath(p) => $($amp)+ p.path,
            PathNode::ProjectionPath(p) => $($amp)+ p.path,
            PathNode::ProjectSetPath(p) => $($amp)+ p.path,
            PathNode::SortPath(p) => $($amp)+ p.path,
            PathNode::IncrementalSortPath(p) => $($amp)+ p.spath.path,
            PathNode::GroupPath(p) => $($amp)+ p.path,
            PathNode::UpperUniquePath(p) => $($amp)+ p.path,
            PathNode::AggPath(p) => $($amp)+ p.path,
            PathNode::GroupingSetsPath(p) => $($amp)+ p.path,
            PathNode::MinMaxAggPath(p) => $($amp)+ p.path,
            PathNode::WindowAggPath(p) => $($amp)+ p.path,
            PathNode::SetOpPath(p) => $($amp)+ p.path,
            PathNode::RecursiveUnionPath(p) => $($amp)+ p.path,
            PathNode::LockRowsPath(p) => $($amp)+ p.path,
            PathNode::ModifyTablePath(p) => $($amp)+ p.path,
            PathNode::LimitPath(p) => $($amp)+ p.path,
        }
    };
}

impl<'mcx> PathNode<'mcx> {
    pub fn base(&self) -> &Path<'mcx> {
        path_base!(self, &)
    }

    pub fn base_mut(&mut self) -> &mut Path<'mcx> {
        path_base!(self, &mut)
    }
}

#[derive(Clone, Debug)]
pub struct RestrictInfo<'mcx> {
    pub clause: NodeId,
    pub is_pushed_down: bool,
    pub can_join: bool,
    pub pseudoconstant: bool,
    pub has_clone: bool,
    pub is_clone: bool,
    pub leakproof: bool,
    pub has_volatile: VolatileFunctionStatus,
    pub security_level: u32,
    pub num_base_rels: i32,
    pub clause_relids: Relids<'mcx>,
    pub required_relids: Relids<'mcx>,
    pub incompatible_relids: Relids<'mcx>,
    pub outer_relids: Relids<'mcx>,
    pub left_relids: Relids<'mcx>,
    pub right_relids: Relids<'mcx>,
    pub orclause: Option<NodeId>,
    pub rinfo_serial: i32,
    pub parent_ec: Option<EcId>,
    pub eval_cost: QualCost,
    pub norm_selec: f64,
    pub outer_selec: f64,
    pub mergeopfamilies: PgVec<'mcx, Oid>,
    pub left_ec: Option<EcId>,
    pub right_ec: Option<EcId>,
    pub left_em: Option<EmId>,
    pub right_em: Option<EmId>,
    /// C caches `mergejoinscansel` results here; leaving it unwritten cost
    /// fabled 53x on joinplan — the equivclass/costsize port must fill it.
    pub scansel_cache: PgVec<'mcx, MergeScanSelCache>,
    pub outer_is_left: bool,
    pub hashjoinoperator: Oid,
    pub left_bucketsize: f64,
    pub right_bucketsize: f64,
    pub left_mcvfreq: f64,
    pub right_mcvfreq: f64,
    pub left_hasheqoperator: Oid,
    pub right_hasheqoperator: Oid,
}

#[derive(Clone, Debug)]
pub struct EquivalenceClass<'mcx> {
    pub ec_opfamilies: PgVec<'mcx, Oid>,
    pub ec_collation: Oid,
    pub ec_childmembers_size: i32,
    pub ec_members: PgVec<'mcx, EmId>,
    pub ec_childmembers: PgVec<'mcx, PgVec<'mcx, EmId>>,
    pub ec_sources: PgVec<'mcx, RinfoId>,
    pub ec_derives_list: PgVec<'mcx, RinfoId>,
    pub ec_derives_hash: Option<mcx::PgFxHashMap<'mcx, ECDerivesKey, RinfoId>>,
    pub ec_relids: Relids<'mcx>,
    pub ec_has_const: bool,
    pub ec_has_volatile: bool,
    pub ec_broken: bool,
    pub ec_sortref: Index,
    pub ec_min_security: Index,
    pub ec_max_security: Index,
    pub ec_merged: Option<EcId>,
}

impl<'mcx> EquivalenceClass<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        EquivalenceClass {
            ec_opfamilies: PgVec::new_in(mcx),
            ec_collation: 0,
            ec_childmembers_size: 0,
            ec_members: PgVec::new_in(mcx),
            ec_childmembers: PgVec::new_in(mcx),
            ec_sources: PgVec::new_in(mcx),
            ec_derives_list: PgVec::new_in(mcx),
            ec_derives_hash: None,
            ec_relids: relids::relids_empty(),
            ec_has_const: false,
            ec_has_volatile: false,
            ec_broken: false,
            ec_sortref: 0,
            ec_min_security: 0,
            ec_max_security: 0,
            ec_merged: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct EquivalenceMember<'mcx> {
    pub em_expr: NodeId,
    pub em_relids: Relids<'mcx>,
    pub em_is_const: bool,
    pub em_is_child: bool,
    pub em_datatype: Oid,
    /// Index into PlannerInfo.join_domains; C's pointer identity.
    pub em_jdomain: usize,
    pub em_parent: Option<EmId>,
}

#[derive(Clone, Debug)]
pub struct EquivalenceMemberIterator<'mcx> {
    pub ec: Option<EcId>,
    pub current_relid: i32,
    pub child_relids: Relids<'mcx>,
    pub current_cell: Option<usize>,
    pub current_list: PgVec<'mcx, EmId>,
}

impl<'mcx> EquivalenceMemberIterator<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        EquivalenceMemberIterator {
            ec: None,
            current_relid: 0,
            child_relids: relids::relids_empty(),
            current_cell: None,
            current_list: PgVec::new_in(mcx),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ForeignKeyOptInfo<'mcx> {
    pub con_relid: Index,
    pub ref_relid: Index,
    pub nkeys: i32,
    pub conkey: PgVec<'mcx, AttrNumber>,
    pub confkey: PgVec<'mcx, AttrNumber>,
    pub conpfeqop: PgVec<'mcx, Oid>,
    pub nmatched_ec: i32,
    pub nconst_ec: i32,
    pub nmatched_rcols: i32,
    pub nmatched_ri: i32,
    pub eclass: PgVec<'mcx, Option<EcId>>,
    pub fk_eclass_member: PgVec<'mcx, Option<EmId>>,
    pub rinfos: PgVec<'mcx, PgVec<'mcx, RinfoId>>,
}

impl<'mcx> ForeignKeyOptInfo<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        ForeignKeyOptInfo {
            con_relid: 0,
            ref_relid: 0,
            nkeys: 0,
            conkey: PgVec::new_in(mcx),
            confkey: PgVec::new_in(mcx),
            conpfeqop: PgVec::new_in(mcx),
            nmatched_ec: 0,
            nconst_ec: 0,
            nmatched_rcols: 0,
            nmatched_ri: 0,
            eclass: PgVec::new_in(mcx),
            fk_eclass_member: PgVec::new_in(mcx),
            rinfos: PgVec::new_in(mcx),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StatisticExtInfo<'mcx> {
    pub stat_oid: Oid,
    pub inherit: bool,
    pub rel: Option<RelId>,
    pub kind: i8,
    pub keys: Relids<'mcx>,
    pub exprs: PgVec<'mcx, NodeId>,
}

impl<'mcx> StatisticExtInfo<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        StatisticExtInfo {
            stat_oid: 0,
            inherit: false,
            rel: None,
            kind: 0,
            keys: relids::relids_empty(),
            exprs: PgVec::new_in(mcx),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpecialJoinInfo<'mcx> {
    pub min_lefthand: Relids<'mcx>,
    pub min_righthand: Relids<'mcx>,
    pub syn_lefthand: Relids<'mcx>,
    pub syn_righthand: Relids<'mcx>,
    pub jointype: JoinType,
    pub ojrelid: Index,
    pub commute_above_l: Relids<'mcx>,
    pub commute_above_r: Relids<'mcx>,
    pub commute_below_l: Relids<'mcx>,
    pub commute_below_r: Relids<'mcx>,
    pub lhs_strict: bool,
    pub semi_can_btree: bool,
    pub semi_can_hash: bool,
    pub semi_operators: PgVec<'mcx, Oid>,
    pub semi_rhs_exprs: PgVec<'mcx, NodeId>,
}

#[derive(Clone, Debug)]
pub struct OuterJoinClauseInfo<'mcx> {
    pub rinfo: RinfoId,
    pub sjinfo: SpecialJoinInfo<'mcx>,
}

/// `PlaceHolderInfo` less `ph_var` (a `PlaceHolderVar`, deferred to the
/// types_nodes boundary); its decomposed reads are carried as
/// `ph_var_phexpr`/`ph_var_phrels`.
#[derive(Clone, Debug, Default)]
pub struct PlaceHolderInfo<'mcx> {
    pub phid: Index,
    pub ph_var_phexpr: NodeId,
    pub ph_var_phrels: Relids<'mcx>,
    pub ph_eval_at: Relids<'mcx>,
    pub ph_lateral: Relids<'mcx>,
    pub ph_needed: Relids<'mcx>,
    pub ph_width: i32,
}

#[derive(Clone, Debug)]
pub struct UniqueRelInfo<'mcx> {
    pub outerrelids: Relids<'mcx>,
    pub self_join: bool,
    pub extra_clauses: PgVec<'mcx, RinfoId>,
}

/// Clone-skipping `subroot` wrapper: C never deep-copies a PlannerInfo (the
/// pointer is `pg_node_attr` not-copied), so cloning a RelOptInfo yields
/// `None` here.
#[derive(Debug, Default)]
pub struct Subroot<'mcx>(pub Option<PgBox<'mcx, PlannerInfo<'mcx>>>);

impl<'mcx> Clone for Subroot<'mcx> {
    fn clone(&self) -> Self {
        Subroot(None)
    }
}

impl<'mcx> core::ops::Deref for Subroot<'mcx> {
    type Target = Option<PgBox<'mcx, PlannerInfo<'mcx>>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'mcx> core::ops::DerefMut for Subroot<'mcx> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'mcx> Subroot<'mcx> {
    #[inline]
    pub fn from_planner_info(mcx: Mcx<'mcx>, root: PlannerInfo<'mcx>) -> Self {
        Subroot(Some(::mcx::box_new_in(mcx, root)))
    }
}

// RelOptInfo.amflags bits (C: uint32 amflags in RelOptInfo; bit 0 matches
// C's AMFLAG_HAS_TID_RANGE, bits 1-2 are pgrust-only).
pub const AMFLAG_HAS_TID_RANGE: u32 = 1 << 0;
pub const AMFLAG_PGRCOLUMNAR: u32 = 1 << 1;
/// pgrust-only (q2box lane): every committed row group of the pgrcolumnar
/// part carries exact v7 zero/empty counts (RG_FLAG_ZEROCNT on all RGs) —
/// the executor's footer META answer can serve zero-count-qual COUNT
/// shapes (`col <> 0` / `col = 0`) without a scan. Plan-time consumers
/// (m5_suppress CbPlainAggFold keying) must treat a CLEAR bit as NOT
/// answerable (v<=6 parts, preserved-RG mixtures, footer-less rels).
pub const AMFLAG_PGRCOLUMNAR_ZEROCNT: u32 = 1 << 2;

#[derive(Clone, Debug)]
pub struct RelOptInfo<'mcx> {
    pub reloptkind: RelOptKind,
    pub relids: Relids<'mcx>,
    pub rows: Cardinality,
    pub consider_startup: bool,
    pub consider_param_startup: bool,
    pub consider_parallel: bool,
    pub pathtarget_id: Option<PtId>,
    pub pathlist: PgVec<'mcx, PathId>,
    pub ppilist: PgVec<'mcx, ParamPathInfo<'mcx>>,
    pub partial_pathlist: PgVec<'mcx, PathId>,
    pub cheapest_startup_path: Option<PathId>,
    pub cheapest_total_path: Option<PathId>,
    pub cheapest_unique_path: Option<PathId>,
    pub cheapest_parameterized_paths: PgVec<'mcx, PathId>,
    pub direct_lateral_relids: Relids<'mcx>,
    pub lateral_relids: Relids<'mcx>,
    pub lateral_vars: PgVec<'mcx, NodeId>,
    pub relid: Index,
    pub reltablespace: Oid,
    pub rtekind: RTEKind,
    pub min_attr: AttrNumber,
    pub max_attr: AttrNumber,
    pub attr_widths: PgVec<'mcx, i32>,
    pub nulling_relids: Relids<'mcx>,
    pub lateral_referencers: Relids<'mcx>,
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    pub allvisfrac: f64,
    pub baserestrictinfo: PgVec<'mcx, RinfoId>,
    pub baserestrictcost: QualCost,
    pub baserestrict_min_security: Index,
    pub joininfo: PgVec<'mcx, RinfoId>,
    pub has_eclass_joins: bool,
    pub consider_partitionwise_join: bool,
    pub serverid: Oid,
    pub userid: Oid,
    pub useridiscurrent: bool,
    pub parent: Option<RelId>,
    pub top_parent: Option<RelId>,
    pub top_parent_relids: Relids<'mcx>,
    pub rel_parallel_workers: i32,
    pub amflags: u32,
    // pgrcolumnar v5 footer sorted-asc columns (1-based attnos, ascending order)
    // usable as scan pathkeys; empty for every other relation.
    pub pgrcolumnar_sorted_attnos: PgVec<'mcx, i16>,
    // pgrcolumnar per-column on-disk chunk bytes (1-based attno = index + 1)
    // for column-fraction seqscan disk costing; empty for every other
    // relation (and when the part has no committed footer).
    pub pgrcolumnar_col_bytes: PgVec<'mcx, u64>,
    // pgrcolumnar ingest-time per-column NDV from the part footer (whole-stream
    // HLL; 1-based attno = index + 1, 0 = unknown) for group-key ndistinct
    // estimation on never-ANALYZEd tables; empty for every other relation
    // (and when the part has no committed footer).
    pub pgrcolumnar_col_ndv: PgVec<'mcx, u64>,
    // pgrcolumnar v7 per-column part-global stitch dict sizes (1-based attno
    // = index + 1, 0 = no stitch) for the SE-TOPNNI text sort-key plan-time
    // answerability probe; populated only while PGRUST_LANE_V2_TOPN_NONINT
    // is armed (default ON since the GL-TOPNNI-1 flip; empty for every
    // other relation, footer-less parts, and under the kill spelling).
    pub pgrcolumnar_stitch_gndv: PgVec<'mcx, u64>,
    pub fdwroutine: Option<types_nodes::FdwKind>,
    pub attr_needed: PgVec<'mcx, Relids<'mcx>>,
    pub notnullattnums: Relids<'mcx>,
    pub indexlist: PgVec<'mcx, &'mcx IndexOptInfo<'mcx>>,
    pub statlist: PgVec<'mcx, NodeId>,
    pub eclass_indexes: Relids<'mcx>,
    pub subroot: Subroot<'mcx>,
    /// Handle into the run's rel_subroots store (the live form of `subroot`;
    /// a PlannerRun-level pair can't live inside the rel arena).
    pub subroot_idx: Option<usize>,
    pub subplan_params: PgVec<'mcx, NodeId>,
    pub fdw_private: NodeId,
    /// C RelOptInfo.fdw_private's void* face for FDWs whose planner state
    /// exceeds value-node lists: an opaque pointer into the run arena, owned
    /// (allocated, cast, mutated) solely by the provider crate
    /// (att_stats_memo discipline; file_fdw keeps using `fdw_private`).
    pub fdw_state: Option<core::ptr::NonNull<()>>,
    pub unique_for_rels: PgVec<'mcx, UniqueRelInfo<'mcx>>,
    pub non_unique_for_rels: PgVec<'mcx, Relids<'mcx>>,
    pub part_scheme: PartitionScheme<'mcx>,
    pub nparts: i32,
    pub boundinfo: Option<PgBox<'mcx, PartitionBoundInfoData<'mcx>>>,
    pub partbounds_merged: bool,
    pub partition_qual: PgVec<'mcx, NodeId>,
    pub part_rels: PgVec<'mcx, Option<RelId>>,
    pub live_parts: Relids<'mcx>,
    pub all_partrels: Relids<'mcx>,
    pub partexprs: PgVec<'mcx, PgVec<'mcx, NodeId>>,
    pub nullable_partexprs: PgVec<'mcx, PgVec<'mcx, NodeId>>,
}

impl<'mcx> RelOptInfo<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        RelOptInfo {
            reloptkind: RELOPT_BASEREL,
            relids: relids::relids_empty(),
            rows: 0.0,
            consider_startup: false,
            consider_param_startup: false,
            consider_parallel: false,
            pathtarget_id: None,
            pathlist: PgVec::new_in(mcx),
            ppilist: PgVec::new_in(mcx),
            partial_pathlist: PgVec::new_in(mcx),
            cheapest_startup_path: None,
            cheapest_total_path: None,
            cheapest_unique_path: None,
            cheapest_parameterized_paths: PgVec::new_in(mcx),
            direct_lateral_relids: relids::relids_empty(),
            lateral_relids: relids::relids_empty(),
            lateral_vars: PgVec::new_in(mcx),
            relid: 0,
            reltablespace: 0,
            rtekind: RTE_RELATION,
            min_attr: 0,
            max_attr: 0,
            attr_widths: PgVec::new_in(mcx),
            nulling_relids: relids::relids_empty(),
            lateral_referencers: relids::relids_empty(),
            pages: 0,
            tuples: 0.0,
            allvisfrac: 0.0,
            baserestrictinfo: PgVec::new_in(mcx),
            baserestrictcost: QualCost::default(),
            baserestrict_min_security: 0,
            joininfo: PgVec::new_in(mcx),
            has_eclass_joins: false,
            consider_partitionwise_join: false,
            serverid: 0,
            userid: 0,
            useridiscurrent: false,
            parent: None,
            top_parent: None,
            top_parent_relids: relids::relids_empty(),
            rel_parallel_workers: 0,
            amflags: 0,
            pgrcolumnar_sorted_attnos: PgVec::new_in(mcx),
            pgrcolumnar_col_bytes: PgVec::new_in(mcx),
            pgrcolumnar_col_ndv: PgVec::new_in(mcx),
            pgrcolumnar_stitch_gndv: PgVec::new_in(mcx),
            fdwroutine: None,
            attr_needed: PgVec::new_in(mcx),
            notnullattnums: relids::relids_empty(),
            indexlist: PgVec::new_in(mcx),
            statlist: PgVec::new_in(mcx),
            eclass_indexes: relids::relids_empty(),
            subroot: Subroot::default(),
            subroot_idx: None,
            subplan_params: PgVec::new_in(mcx),
            fdw_private: NodeId::default(),
            fdw_state: None,
            unique_for_rels: PgVec::new_in(mcx),
            non_unique_for_rels: PgVec::new_in(mcx),
            part_scheme: None,
            nparts: 0,
            boundinfo: None,
            partbounds_merged: false,
            partition_qual: PgVec::new_in(mcx),
            part_rels: PgVec::new_in(mcx),
            live_parts: relids::relids_empty(),
            all_partrels: relids::relids_empty(),
            partexprs: PgVec::new_in(mcx),
            nullable_partexprs: PgVec::new_in(mcx),
        }
    }
}

#[derive(Debug)]
pub struct PlannerGlobal<'mcx> {
    /// `PlanId` handles into the run's subplan stores; a C `plan_id` is the
    /// 0-based handle + 1.
    pub subplans: PgVec<'mcx, PlanId>,
    pub subpaths: PgVec<'mcx, PlanId>,
    pub subroots: PgVec<'mcx, PlanId>,
    pub rewind_plan_ids: Relids<'mcx>,
    pub finalrtable: PgVec<'mcx, RangeTblEntryId>,
    pub all_relids: Relids<'mcx>,
    pub prunable_relids: Relids<'mcx>,
    pub finalrteperminfos: PgVec<'mcx, RtePermInfoId>,
    pub finalrowmarks: PgVec<'mcx, PlanRowMarkId>,
    pub result_relations: PgVec<'mcx, i32>,
    pub relation_oids: PgVec<'mcx, Oid>,
    pub param_exec_types: PgVec<'mcx, Oid>,
    pub last_ph_id: Index,
    pub last_row_mark_id: Index,
    pub last_plan_node_id: i32,
    pub transient_plan: bool,
    pub depends_on_role: bool,
    pub parallel_mode_ok: bool,
    pub parallel_mode_needed: bool,
    pub max_parallel_hazard: i8,
}

impl<'mcx> PlannerGlobal<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PlannerGlobal {
            subplans: PgVec::new_in(mcx),
            subpaths: PgVec::new_in(mcx),
            subroots: PgVec::new_in(mcx),
            rewind_plan_ids: relids::relids_empty(),
            finalrtable: PgVec::new_in(mcx),
            all_relids: relids::relids_empty(),
            prunable_relids: relids::relids_empty(),
            finalrteperminfos: PgVec::new_in(mcx),
            finalrowmarks: PgVec::new_in(mcx),
            result_relations: PgVec::new_in(mcx),
            relation_oids: PgVec::new_in(mcx),
            param_exec_types: PgVec::new_in(mcx),
            last_ph_id: 0,
            last_row_mark_id: 0,
            last_plan_node_id: 0,
            transient_plan: false,
            depends_on_role: false,
            parallel_mode_ok: false,
            parallel_mode_needed: false,
            max_parallel_hazard: 0,
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ArenaNode<'mcx> {
    /// Index-0 sentinel so `NodeId(0)` stays the NULL handle.
    Reserved,
    /// Expression payload boundary to types_nodes (discharges the deferral in
    /// the crate doc): C shares `Expr *` by pointer; `Node` is that pointer.
    Expr(::types_nodes::node_tree::Node<'mcx>),
    TargetEntry(TargetEntryNode<'mcx>),
    ForeignKey(ForeignKeyOptInfo<'mcx>),
    StatisticExt(StatisticExtInfo<'mcx>),
    AggInfo(AggInfo<'mcx>),
    AggTransInfo(AggTransInfo<'mcx>),
    PlannerParamItem(PlannerParamItem),
    MinMaxAggInfo(MinMaxAggInfo),
    WindowClause(WindowClauseNode<'mcx>),
}

#[derive(Debug)]
pub struct WindowClauseNode<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub partitionClause: PgVec<'mcx, NodeId>,
    pub orderClause: PgVec<'mcx, NodeId>,
    pub frameOptions: i32,
    pub startOffset: Option<NodeId>,
    pub endOffset: Option<NodeId>,
    pub startInRangeFunc: Oid,
    pub endInRangeFunc: Oid,
    pub inRangeColl: Oid,
    pub inRangeAsc: bool,
    pub inRangeNullsFirst: bool,
    pub winref: Index,
}

impl<'mcx> WindowClauseNode<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        WindowClauseNode {
            name: None,
            partitionClause: PgVec::new_in(mcx),
            orderClause: PgVec::new_in(mcx),
            frameOptions: 0,
            startOffset: None,
            endOffset: None,
            startInRangeFunc: 0,
            endInRangeFunc: 0,
            inRangeColl: 0,
            inRangeAsc: false,
            inRangeNullsFirst: false,
            winref: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PlannerParamItem {
    pub item: NodeId,
    pub paramId: i32,
}

#[derive(Debug)]
pub struct AggInfo<'mcx> {
    pub aggrefs: PgVec<'mcx, NodeId>,
    pub transno: i32,
    pub shareable: bool,
    pub finalfn_oid: Oid,
}

impl<'mcx> AggInfo<'mcx> {
    #[inline]
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        AggInfo {
            aggrefs: PgVec::new_in(mcx),
            transno: 0,
            shareable: false,
            finalfn_oid: 0,
        }
    }
}

#[derive(Debug)]
pub struct AggTransInfo<'mcx> {
    pub args: PgVec<'mcx, NodeId>,
    pub aggfilter: Option<NodeId>,
    pub transfn_oid: Oid,
    pub serialfn_oid: Oid,
    pub deserialfn_oid: Oid,
    pub combinefn_oid: Oid,
    pub aggtranstype: Oid,
    pub aggtranstypmod: i32,
    pub transtypeLen: i32,
    pub transtypeByVal: bool,
    pub aggtransspace: i32,
    pub initValue: Datum,
    pub initValueIsNull: bool,
    pub initValueImage: Option<PgVec<'mcx, u8>>,
}

impl<'mcx> AggTransInfo<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        AggTransInfo {
            args: PgVec::new_in(mcx),
            aggfilter: None,
            transfn_oid: 0,
            serialfn_oid: 0,
            deserialfn_oid: 0,
            combinefn_oid: 0,
            aggtranstype: 0,
            aggtranstypmod: -1,
            transtypeLen: 0,
            transtypeByVal: false,
            aggtransspace: 0,
            initValue: Datum::null(),
            initValueIsNull: true,
            initValueImage: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AggClauseCosts {
    pub transCost: QualCost,
    pub finalCost: QualCost,
    pub transitionSpace: Size,
}

#[derive(Debug, Default)]
pub struct TargetEntryNode<'mcx> {
    pub expr: NodeId,
    pub resno: AttrNumber,
    pub resname: Option<PgString<'mcx>>,
    pub ressortgroupref: Index,
    pub resorigtbl: Oid,
    pub resorigcol: AttrNumber,
    pub resjunk: bool,
}

macro_rules! arena_node_accessors {
    ($($get:ident, $get_mut:ident, $alloc:ident, $variant:ident => $ty:ty),+ $(,)?) => {
        impl<'mcx> PlannerInfo<'mcx> {$(
            #[inline]
            pub fn $get(&self, id: NodeId) -> &$ty {
                match &self.node_arena[id.index()] {
                    ArenaNode::$variant(v) => v,
                    _ => panic!(concat!("NodeId is not ", stringify!($variant))),
                }
            }
            #[inline]
            pub fn $get_mut(&mut self, id: NodeId) -> &mut $ty {
                match &mut self.node_arena[id.index()] {
                    ArenaNode::$variant(v) => v,
                    _ => panic!(concat!("NodeId is not ", stringify!($variant))),
                }
            }
            #[inline]
            pub fn $alloc(&mut self, v: $ty) -> NodeId {
                let id = self.reserve_node_id();
                self.node_arena.push(ArenaNode::$variant(v));
                id
            }
        )+}
    };
}

arena_node_accessors!(
    expr_node, expr_node_mut, alloc_expr_node, Expr => ::types_nodes::node_tree::Node<'mcx>,
    targetentry, targetentry_mut, alloc_targetentry, TargetEntry => TargetEntryNode<'mcx>,
    foreign_key, foreign_key_mut, alloc_foreign_key, ForeignKey => ForeignKeyOptInfo<'mcx>,
    statistic_ext, statistic_ext_mut, alloc_statistic_ext, StatisticExt => StatisticExtInfo<'mcx>,
    agg_info, agg_info_mut, alloc_agg_info, AggInfo => AggInfo<'mcx>,
    agg_trans_info, agg_trans_info_mut, alloc_agg_trans_info, AggTransInfo => AggTransInfo<'mcx>,
    planner_param_item, planner_param_item_mut, alloc_planner_param_item, PlannerParamItem => PlannerParamItem,
    minmax_agg_info, minmax_agg_info_mut, alloc_minmax_agg_info, MinMaxAggInfo => MinMaxAggInfo,
    windowclause, windowclause_mut, alloc_windowclause, WindowClause => WindowClauseNode<'mcx>,
);

#[derive(Debug, Clone, Copy)]
pub struct CteScanParam {
    pub rti: u32,
    pub plan_id: i32,
    pub cte_param: i32,
}

#[derive(Debug)]
pub struct PlannerInfo<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub parse: QueryId,
    // C root->parse->commandType; run.queries is interned lazily (post-
    // preprocess), so the level's command type is carried directly.
    pub command_type: ::types_nodes::nodes_enums::CmdType,
    pub glob: Option<PgBox<'mcx, PlannerGlobal<'mcx>>>,
    pub query_level: Index,
    pub parent_root: Option<PgBox<'mcx, PlannerInfo<'mcx>>>,
    pub plan_params: PgVec<'mcx, NodeId>,
    pub outer_params: Relids<'mcx>,
    pub simple_rel_array: PgVec<'mcx, Option<RelId>>,
    pub simple_rel_array_size: i32,
    pub simple_rte_array: PgVec<'mcx, RangeTblEntryId>,
    pub append_rel_array: PgVec<'mcx, Option<AppendRelInfo<'mcx>>>,
    pub all_baserels: Relids<'mcx>,
    pub outer_join_rels: Relids<'mcx>,
    pub all_query_rels: Relids<'mcx>,
    pub join_rel_list: PgVec<'mcx, RelId>,
    // C's join_rel_hash HTAB; keyed by the relids word slice, built lazily
    // by find_join_rel once join_rel_list outgrows the linear probe.
    pub join_rel_hash: Option<mcx::PgFxHashMap<'mcx, mcx::PgVec<'mcx, u64>, RelId>>,
    pub join_rel_level: PgVec<'mcx, PgVec<'mcx, RelId>>,
    pub join_cur_level: i32,
    pub init_plans: PgVec<'mcx, NodeId>,
    pub cte_plan_ids: PgVec<'mcx, i32>,
    /// Not in C: parse.cteList snapshot taken by SS_process_ctes. C's
    /// set_cte_pathlist reads cteroot->parse->cteList while the parent is
    /// still mid-preprocessing; here that parse is not yet sealed/interned.
    pub cte_list: ::types_nodes::list::NodeList<'mcx>,
    /// Not in C: (rti, plan_id, cte_param) resolved at set_cte_pathlist time,
    /// because the C parent_root chain is unavailable at createplan time
    /// (same reason as self_ref_wt_param).
    pub cte_scan_params: PgVec<'mcx, CteScanParam>,
    pub multiexpr_params: PgVec<'mcx, PgVec<'mcx, NodeId>>,
    pub join_domains: PgVec<'mcx, JoinDomain<'mcx>>,
    pub eq_classes: PgVec<'mcx, EquivalenceClass<'mcx>>,
    pub ec_merging_done: bool,
    pub canon_pathkeys: PgVec<'mcx, PathKey>,
    pub left_join_clauses: PgVec<'mcx, OuterJoinClauseInfo<'mcx>>,
    pub right_join_clauses: PgVec<'mcx, OuterJoinClauseInfo<'mcx>>,
    pub full_join_clauses: PgVec<'mcx, OuterJoinClauseInfo<'mcx>>,
    pub join_info_list: PgVec<'mcx, SpecialJoinInfo<'mcx>>,
    pub last_rinfo_serial: i32,
    pub all_result_relids: Relids<'mcx>,
    pub leaf_result_relids: Relids<'mcx>,
    pub append_rel_list: PgVec<'mcx, AppendRelInfo<'mcx>>,
    pub row_identity_vars: PgVec<'mcx, RowIdentityVarInfo<'mcx>>,
    pub rowMarks: PgVec<'mcx, PlanRowMarkId>,
    pub placeholder_list: PgVec<'mcx, PhInfoId>,
    pub placeholder_array: PgVec<'mcx, Option<PhInfoId>>,
    pub placeholder_array_size: i32,
    pub fkey_list: PgVec<'mcx, NodeId>,
    pub query_pathkeys: PgVec<'mcx, PathKey>,
    pub group_pathkeys: PgVec<'mcx, PathKey>,
    pub num_groupby_pathkeys: i32,
    pub window_pathkeys: PgVec<'mcx, PathKey>,
    pub distinct_pathkeys: PgVec<'mcx, PathKey>,
    pub sort_pathkeys: PgVec<'mcx, PathKey>,
    pub setop_pathkeys: PgVec<'mcx, PathKey>,
    pub part_schemes: PgVec<'mcx, PartitionScheme<'mcx>>,
    pub initial_rels: PgVec<'mcx, RelId>,
    pub upper_rels: [PgVec<'mcx, RelId>; NUM_UPPERREL_KINDS],
    pub upper_targets: [Option<PtId>; NUM_UPPERREL_KINDS],
    pub processed_groupClause: PgVec<'mcx, NodeId>,
    pub processed_distinctClause: PgVec<'mcx, NodeId>,
    pub processed_tlist: PgVec<'mcx, NodeId>,
    pub update_colnos: PgVec<'mcx, AttrNumber>,
    pub grouping_map: PgVec<'mcx, AttrNumber>,
    pub minmax_aggs: PgVec<'mcx, NodeId>,
    pub total_table_pages: Cardinality,
    pub tuple_fraction: Selectivity,
    pub limit_tuples: Cardinality,
    pub qual_security_level: Index,
    pub hasJoinRTEs: bool,
    pub hasLateralRTEs: bool,
    pub hasHavingQual: bool,
    pub hasPseudoConstantQuals: bool,
    pub hasAlternativeSubPlans: bool,
    pub placeholdersFrozen: bool,
    pub hasRecursion: bool,
    pub group_rtindex: i32,
    pub agginfos: PgVec<'mcx, NodeId>,
    pub aggtransinfos: PgVec<'mcx, NodeId>,
    pub numOrderedAggs: i32,
    pub hasNonPartialAggs: bool,
    pub hasNonSerialAggs: bool,
    pub wt_param_id: i32,
    pub non_recursive_path: Option<PathId>,
    pub non_recursive_rows: Option<f64>,
    /// Not in C: cteroot->wt_param_id resolved at set_worktable_pathlist time
    /// for this level's self-reference RTEs, because the C parent_root chain
    /// is unavailable at createplan time (rel_subroots swap detaches it).
    pub self_ref_wt_param: i32,
    pub curOuterRels: Relids<'mcx>,
    pub curOuterParams: PgVec<'mcx, NodeId>,
    pub partColsUpdated: bool,
    pub join_search_private: Option<PgBox<'mcx, JoinSearchPrivate>>,
    pub isAltSubplan: PgVec<'mcx, bool>,
    pub isUsedSubplan: PgVec<'mcx, bool>,
    // The backbone arenas: the whole aliasing node graph, bump-allocated in
    // the per-query planner context, reclaimed wholesale at the planner
    // boundary. Not in the C struct (C shares these by raw pointer).
    pub rel_arena: PgVec<'mcx, RelOptInfo<'mcx>>,
    pub path_arena: PgVec<'mcx, PathNode<'mcx>>,
    pub rinfo_arena: PgVec<'mcx, RestrictInfo<'mcx>>,
    pub em_arena: PgVec<'mcx, EquivalenceMember<'mcx>>,
    pub ph_info_arena: PgVec<'mcx, PlaceHolderInfo<'mcx>>,
    pub node_arena: PgVec<'mcx, ArenaNode<'mcx>>,
    pub pathtarget_arena: PgVec<'mcx, PathTarget<'mcx>>,
}

impl<'mcx> PlannerInfo<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PlannerInfo {
            mcx,
            parse: QueryId::default(),
            command_type: ::types_nodes::nodes_enums::CmdType::CMD_UNKNOWN,
            glob: None,
            query_level: 0,
            parent_root: None,
            plan_params: PgVec::new_in(mcx),
            outer_params: relids::relids_empty(),
            simple_rel_array: PgVec::new_in(mcx),
            simple_rel_array_size: 0,
            simple_rte_array: PgVec::new_in(mcx),
            append_rel_array: PgVec::new_in(mcx),
            all_baserels: relids::relids_empty(),
            outer_join_rels: relids::relids_empty(),
            all_query_rels: relids::relids_empty(),
            join_rel_list: PgVec::new_in(mcx),
            join_rel_hash: None,
            join_rel_level: PgVec::new_in(mcx),
            join_cur_level: 0,
            init_plans: PgVec::new_in(mcx),
            cte_plan_ids: PgVec::new_in(mcx),
            cte_list: ::types_nodes::list::NodeList::nil(),
            cte_scan_params: PgVec::new_in(mcx),
            multiexpr_params: PgVec::new_in(mcx),
            join_domains: PgVec::new_in(mcx),
            eq_classes: PgVec::new_in(mcx),
            ec_merging_done: false,
            canon_pathkeys: PgVec::new_in(mcx),
            left_join_clauses: PgVec::new_in(mcx),
            right_join_clauses: PgVec::new_in(mcx),
            full_join_clauses: PgVec::new_in(mcx),
            join_info_list: PgVec::new_in(mcx),
            last_rinfo_serial: 0,
            all_result_relids: relids::relids_empty(),
            leaf_result_relids: relids::relids_empty(),
            append_rel_list: PgVec::new_in(mcx),
            row_identity_vars: PgVec::new_in(mcx),
            rowMarks: PgVec::new_in(mcx),
            placeholder_list: PgVec::new_in(mcx),
            placeholder_array: PgVec::new_in(mcx),
            placeholder_array_size: 0,
            fkey_list: PgVec::new_in(mcx),
            query_pathkeys: PgVec::new_in(mcx),
            group_pathkeys: PgVec::new_in(mcx),
            num_groupby_pathkeys: 0,
            window_pathkeys: PgVec::new_in(mcx),
            distinct_pathkeys: PgVec::new_in(mcx),
            sort_pathkeys: PgVec::new_in(mcx),
            setop_pathkeys: PgVec::new_in(mcx),
            part_schemes: PgVec::new_in(mcx),
            initial_rels: PgVec::new_in(mcx),
            upper_rels: core::array::from_fn(|_| PgVec::new_in(mcx)),
            upper_targets: [None; NUM_UPPERREL_KINDS],
            processed_groupClause: PgVec::new_in(mcx),
            processed_distinctClause: PgVec::new_in(mcx),
            processed_tlist: PgVec::new_in(mcx),
            update_colnos: PgVec::new_in(mcx),
            grouping_map: PgVec::new_in(mcx),
            minmax_aggs: PgVec::new_in(mcx),
            total_table_pages: 0.0,
            tuple_fraction: 0.0,
            limit_tuples: 0.0,
            qual_security_level: 0,
            hasJoinRTEs: false,
            hasLateralRTEs: false,
            hasHavingQual: false,
            hasPseudoConstantQuals: false,
            hasAlternativeSubPlans: false,
            placeholdersFrozen: false,
            hasRecursion: false,
            group_rtindex: 0,
            agginfos: PgVec::new_in(mcx),
            aggtransinfos: PgVec::new_in(mcx),
            numOrderedAggs: 0,
            hasNonPartialAggs: false,
            hasNonSerialAggs: false,
            wt_param_id: 0,
            non_recursive_path: None,
            self_ref_wt_param: -1,
            non_recursive_rows: None,
            curOuterRels: relids::relids_empty(),
            curOuterParams: PgVec::new_in(mcx),
            partColsUpdated: false,
            join_search_private: None,
            isAltSubplan: PgVec::new_in(mcx),
            isUsedSubplan: PgVec::new_in(mcx),
            rel_arena: PgVec::new_in(mcx),
            path_arena: PgVec::new_in(mcx),
            rinfo_arena: PgVec::new_in(mcx),
            em_arena: PgVec::new_in(mcx),
            ph_info_arena: PgVec::new_in(mcx),
            node_arena: PgVec::new_in(mcx),
            pathtarget_arena: PgVec::new_in(mcx),
        }
    }

    pub fn make_minmax_subroot(&self) -> PlannerInfo<'mcx> {
        let mut sub = PlannerInfo::new(self.mcx);
        sub.command_type = self.command_type;
        sub.query_level = self.query_level + 1;
        sub.tuple_fraction = self.tuple_fraction;
        sub.limit_tuples = self.limit_tuples;
        sub.total_table_pages = self.total_table_pages;
        sub.qual_security_level = self.qual_security_level;
        sub.hasJoinRTEs = self.hasJoinRTEs;
        sub.hasLateralRTEs = self.hasLateralRTEs;
        sub.hasPseudoConstantQuals = self.hasPseudoConstantQuals;
        sub.placeholdersFrozen = self.placeholdersFrozen;
        sub.group_rtindex = self.group_rtindex;
        sub.wt_param_id = self.wt_param_id;
        sub.join_domains.push(JoinDomain::default());
        // C memcpy's the whole PlannerInfo and copyObject's append_rel_list
        // (planagg.c:338-354): a pulled-up UNION ALL target's appendrel
        // structure must survive into the subroot or its inh-subquery RTE
        // can't re-expand. translated_vars re-intern into the fresh arena
        // (C's Var-sublevel bump is a no-op: no uplevel Vars, asserted at
        // build_minmax_path).
        for ari in self.append_rel_list.iter() {
            let mut translated_vars: PgVec<'mcx, NodeId> = PgVec::new_in(self.mcx);
            for &id in ari.translated_vars.iter() {
                if id == NodeId::default() {
                    translated_vars.push(id);
                } else {
                    let node = *self.expr_node(id);
                    translated_vars.push(sub.alloc_expr_node(node));
                }
            }
            let mut parent_colnos: PgVec<'mcx, AttrNumber> = PgVec::new_in(self.mcx);
            for &c in ari.parent_colnos.iter() {
                parent_colnos.push(c);
            }
            sub.append_rel_list.push(AppendRelInfo {
                parent_relid: ari.parent_relid,
                child_relid: ari.child_relid,
                parent_reltype: ari.parent_reltype,
                child_reltype: ari.child_reltype,
                translated_vars,
                num_child_cols: ari.num_child_cols,
                parent_colnos,
                parent_reloid: ari.parent_reloid,
            });
        }
        sub
    }

    #[inline]
    pub fn rel(&self, id: RelId) -> &RelOptInfo<'mcx> {
        &self.rel_arena[id.index()]
    }

    #[inline]
    pub fn rel_mut(&mut self, id: RelId) -> &mut RelOptInfo<'mcx> {
        &mut self.rel_arena[id.index()]
    }

    #[inline]
    pub fn path(&self, id: PathId) -> &PathNode<'mcx> {
        &self.path_arena[id.index()]
    }

    #[inline]
    pub fn path_mut(&mut self, id: PathId) -> &mut PathNode<'mcx> {
        &mut self.path_arena[id.index()]
    }

    #[inline]
    pub fn pathtarget(&self, id: PtId) -> &PathTarget<'mcx> {
        &self.pathtarget_arena[id.index()]
    }

    #[inline]
    pub fn pathtarget_mut(&mut self, id: PtId) -> &mut PathTarget<'mcx> {
        &mut self.pathtarget_arena[id.index()]
    }

    #[inline]
    pub fn rel_reltarget(&self, rel: RelId) -> &PathTarget<'mcx> {
        self.pathtarget(self.rel(rel).pathtarget_id.unwrap())
    }

    #[inline]
    pub fn rel_reltarget_mut(&mut self, rel: RelId) -> &mut PathTarget<'mcx> {
        let id = self.rel(rel).pathtarget_id.unwrap();
        self.pathtarget_mut(id)
    }

    #[inline]
    pub fn path_pathtarget(&self, path: PathId) -> &PathTarget<'mcx> {
        self.pathtarget(self.path(path).base().pathtarget_id.unwrap())
    }

    #[inline]
    pub fn path_pathtarget_mut(&mut self, path: PathId) -> &mut PathTarget<'mcx> {
        let id = self.path(path).base().pathtarget_id.unwrap();
        self.pathtarget_mut(id)
    }

    #[inline]
    pub fn rinfo(&self, id: RinfoId) -> &RestrictInfo<'mcx> {
        &self.rinfo_arena[id.index()]
    }

    #[inline]
    pub fn rinfo_mut(&mut self, id: RinfoId) -> &mut RestrictInfo<'mcx> {
        &mut self.rinfo_arena[id.index()]
    }

    #[inline]
    pub fn ec(&self, id: EcId) -> &EquivalenceClass<'mcx> {
        &self.eq_classes[id.index()]
    }

    #[inline]
    pub fn ec_mut(&mut self, id: EcId) -> &mut EquivalenceClass<'mcx> {
        &mut self.eq_classes[id.index()]
    }

    /// Chase `ec_merged` links to the canonical (surviving) EC.
    #[inline]
    pub fn ec_canonical(&self, id: EcId) -> EcId {
        let mut cur = id;
        while let Some(next) = self.eq_classes[cur.index()].ec_merged {
            cur = next;
        }
        cur
    }

    #[inline]
    pub fn em(&self, id: EmId) -> &EquivalenceMember<'mcx> {
        &self.em_arena[id.index()]
    }

    #[inline]
    pub fn em_mut(&mut self, id: EmId) -> &mut EquivalenceMember<'mcx> {
        &mut self.em_arena[id.index()]
    }

    #[inline]
    pub fn phinfo(&self, id: PhInfoId) -> &PlaceHolderInfo<'mcx> {
        &self.ph_info_arena[id.index()]
    }

    #[inline]
    pub fn phinfo_mut(&mut self, id: PhInfoId) -> &mut PlaceHolderInfo<'mcx> {
        &mut self.ph_info_arena[id.index()]
    }

    #[inline]
    pub fn alloc_rel(&mut self, rel: RelOptInfo<'mcx>) -> RelId {
        let id = RelId(self.rel_arena.len() as u32);
        self.rel_arena.push(rel);
        id
    }

    #[inline]
    pub fn alloc_path(&mut self, path: PathNode<'mcx>) -> PathId {
        let id = PathId(self.path_arena.len() as u32);
        self.path_arena.push(path);
        id
    }

    #[inline]
    pub fn alloc_pathtarget(&mut self, target: PathTarget<'mcx>) -> PtId {
        let id = PtId(self.pathtarget_arena.len() as u32);
        self.pathtarget_arena.push(target);
        id
    }

    #[inline]
    pub fn alloc_rinfo(&mut self, rinfo: RestrictInfo<'mcx>) -> RinfoId {
        let id = RinfoId(self.rinfo_arena.len() as u32);
        self.rinfo_arena.push(rinfo);
        id
    }

    #[inline]
    pub fn alloc_ec(&mut self, ec: EquivalenceClass<'mcx>) -> EcId {
        let id = EcId(self.eq_classes.len() as u32);
        self.eq_classes.push(ec);
        id
    }

    #[inline]
    pub fn alloc_em(&mut self, em: EquivalenceMember<'mcx>) -> EmId {
        let id = EmId(self.em_arena.len() as u32);
        self.em_arena.push(em);
        id
    }

    #[inline]
    pub fn alloc_phinfo(&mut self, phinfo: PlaceHolderInfo<'mcx>) -> PhInfoId {
        let id = PhInfoId(self.ph_info_arena.len() as u32);
        self.ph_info_arena.push(phinfo);
        id
    }

    #[inline]
    fn reserve_node_id(&mut self) -> NodeId {
        if self.node_arena.is_empty() {
            self.node_arena.push(ArenaNode::Reserved);
        }
        NodeId(self.node_arena.len() as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

    #[test]
    fn c_enum_values_match_headers() {
        assert_eq!(JOIN_UNIQUE_INNER, 9);
        assert_eq!(RELOPT_OTHER_UPPER_REL, 5);
        assert_eq!(NUM_UPPERREL_KINDS, 8);
        assert_eq!(CMD_NOTHING, 7);
        assert_eq!(AGGSPLIT_INITIAL_SERIAL, 0x06);
        assert_eq!(AGGSPLIT_FINAL_DESERIAL, 0x09);
        assert_eq!(BackwardScanDirection, -1);
        assert_eq!(VOLATILITY_NOVOLATILE, 2);
    }

    fn empty_path<'mcx>(mcx: Mcx<'mcx>, parent: RelId, pt: Option<PtId>) -> Path<'mcx> {
        Path {
            type_: 0,
            pathtype: 0,
            parent,
            pathtarget_id: pt,
            param_info: None,
            parallel_aware: false,
            parallel_safe: false,
            parallel_workers: 0,
            rows: 0.0,
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            pathkeys: PgVec::new_in(mcx),
        }
    }

    #[test]
    fn pathtarget_arena_shares_one_identity() {
        let cx = MemoryContext::new("pathnodes_test");
        let mut root = PlannerInfo::new(cx.mcx());
        let pt = root.alloc_pathtarget(PathTarget::new(cx.mcx()));
        let mut rel = RelOptInfo::new(cx.mcx());
        rel.pathtarget_id = Some(pt);
        let rel_id = root.alloc_rel(rel);
        let path = root.alloc_path(PathNode::Path(empty_path(cx.mcx(), rel_id, Some(pt))));
        root.rel_mut(rel_id).pathlist.push(path);
        root.rel_mut(rel_id).cheapest_total_path = Some(path);

        root.pathtarget_mut(pt).width = 42;
        assert_eq!(root.rel_reltarget(rel_id).width, 42);
        assert_eq!(root.path_pathtarget(path).width, 42);
        assert_eq!(
            root.rel(rel_id).pathlist[0],
            root.rel(rel_id).cheapest_total_path.unwrap()
        );
    }

    #[test]
    fn indexoptinfo_shared_mutation_through_borrow() {
        let cx = MemoryContext::new("pathnodes_test");
        let index = &*mcx::forget_box_in(cx.mcx(), IndexOptInfo::new(cx.mcx())).unwrap();
        let alias = index;
        assert_eq!(index.tree_height.get(), -1);
        alias.tree_height.set(3);
        alias.predOK.set(true);
        alias.indrestrictinfo.borrow_mut().push(RinfoId(7));
        assert_eq!(index.tree_height.get(), 3);
        assert!(index.predOK.get());
        assert_eq!(index.indrestrictinfo.borrow()[0], RinfoId(7));
    }

    #[test]
    fn ec_canonical_chases_merged_links() {
        let cx = MemoryContext::new("pathnodes_test");
        let mut root = PlannerInfo::new(cx.mcx());
        let a = root.alloc_ec(EquivalenceClass::new(cx.mcx()));
        let b = root.alloc_ec(EquivalenceClass::new(cx.mcx()));
        let c = root.alloc_ec(EquivalenceClass::new(cx.mcx()));
        root.ec_mut(a).ec_merged = Some(b);
        root.ec_mut(b).ec_merged = Some(c);
        assert_eq!(root.ec_canonical(a), c);
        assert_eq!(root.ec_canonical(c), c);
    }

    #[test]
    fn node_arena_reserves_null_id_zero() {
        let cx = MemoryContext::new("pathnodes_test");
        let mut root = PlannerInfo::new(cx.mcx());
        let te = root.alloc_targetentry(TargetEntryNode {
            expr: NodeId(0),
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        });
        assert_ne!(te, NodeId::default());
        assert_eq!(root.targetentry(te).resno, 1);
        root.targetentry_mut(te).resjunk = true;
        assert!(root.targetentry(te).resjunk);
        let mm = root.alloc_minmax_agg_info(MinMaxAggInfo::default());
        assert_eq!(mm, NodeId(2));
        assert!(matches!(root.node_arena[0], ArenaNode::Reserved));
    }

    #[test]
    fn pathnode_base_reaches_nested_variants() {
        let cx = MemoryContext::new("pathnodes_test");
        let base = empty_path(cx.mcx(), RelId(0), None);
        let mut node = PathNode::HashPath(HashPath {
            jpath: JoinPath {
                path: base,
                jointype: JOIN_INNER,
                inner_unique: false,
                outerjoinpath: None,
                innerjoinpath: None,
                joinrestrictinfo: PgVec::new_in(cx.mcx()),
            },
            path_hashclauses: PgVec::new_in(cx.mcx()),
            num_batches: 0,
            inner_rows_total: 0.0,
        });
        node.base_mut().total_cost = 12.5;
        assert_eq!(node.base().total_cost, 12.5);
        let inc = PathNode::IncrementalSortPath(IncrementalSortPath {
            spath: SortPath {
                path: empty_path(cx.mcx(), RelId(1), None),
                subpath: None,
            },
            nPresortedCols: 2,
        });
        assert_eq!(inc.base().parent, RelId(1));
    }
}

// The planner never drops these: a PlannerRun is forgotten whole and its
// arenas die with the query context (C: one wholesale context reset). The
// census below makes a droppy-beyond-the-arena field a compile error.
mcx::forget_safe_nodrop!(
    RelId, PathId, PtId, RinfoId, EcId, EmId, PhInfoId, NodeId, PlanId,
    QueryId, RangeTblEntryId, PlanRowMarkId, RtePermInfoId, JoinSearchPrivate,
    ECDerivesKey, ECDerivesEntry, MergeScanSelCache, QualCost, PathKey,
    MinMaxAggInfo, PlannerParamItem, AggClauseCosts,
);

mcx::forget_safe_struct!(
    DerivesHash<'_> { size, sizemask, members, grow_threshold, data },
    PartitionBoundInfoData<'_> { strategy, ndatums, nindexes, null_index, default_index, indexes, datums, kind, interleaved_parts },
    JoinDomain<'_> { jd_relids },
    AppendRelInfo<'_> { parent_relid, child_relid, parent_reltype, child_reltype, translated_vars, num_child_cols, parent_colnos, parent_reloid },
    RowIdentityVarInfo<'_> { rowidvar, rowidwidth, rowidname, rowidrels },
    GinIndexStats { pending_pages, total_pages, entry_pages, data_pages, entries, version },
    IndexOptInfo<'_> { indexoid, reltablespace, rel, pages, tuples, tree_height, ncolumns, nkeycolumns, indexkeys, indexcollations, opfamily, opcintype, sortopfamily, reverse_sort, nulls_first, canreturn, relam, indexprs, indpred, indextlist, indrestrictinfo, predOK, unique, nullsnotdistinct, immediate, hypothetical, amcanorderbyop, amoptionalkey, amsearcharray, amsearchnulls, amhasgettuple, amhasgetbitmap, amcanparallel, amcanmarkpos, gin_stats },
    GroupByOrdering<'_> { pathkeys, clauses },
    PathTarget<'_> { exprs, sortgrouprefs, cost, width, has_volatile_expr },
    ParamPathInfo<'_> { ppi_req_outer, ppi_rows, ppi_clauses, ppi_serials },
    Path<'_> { type_, pathtype, parent, pathtarget_id, param_info, parallel_aware, parallel_safe, parallel_workers, rows, disabled_nodes, startup_cost, total_cost, pathkeys },
    JoinPath<'_> { path, jointype, inner_unique, outerjoinpath, innerjoinpath, joinrestrictinfo },
    NestPath<'_> { jpath },
    MergePath<'_> { jpath, path_mergeclauses, outersortkeys, innersortkeys, outer_presorted_keys, skip_mark_restore, materialize_inner },
    HashPath<'_> { jpath, path_hashclauses, num_batches, inner_rows_total },
    IndexClause<'_> { rinfo, indexquals, lossy, indexcol, indexcols },
    IndexPath<'_> { path, indexinfo, indexclauses, indexorderbys, indexorderbycols, indexscandir, indextotalcost, indexselectivity },
    BitmapHeapPath<'_> { path, bitmapqual },
    BitmapAndPath<'_> { path, bitmapquals, bitmapselectivity },
    BitmapOrPath<'_> { path, bitmapquals, bitmapselectivity },
    TidPath<'_> { path, tidquals },
    TidRangePath<'_> { path, tidrangequals },
    SubqueryScanPath<'_> { path, subpath, subroot_subpath },
    ForeignPath<'_> { path, fdw_outerpath, fdw_restrictinfo, fdw_private },
    CustomPath<'_> { path, flags, custom_paths, custom_restrictinfo, custom_private },
    AppendPath<'_> { path, subpaths, first_partial_path, limit_tuples },
    MergeAppendPath<'_> { path, subpaths, limit_tuples },
    GroupResultPath<'_> { path, quals },
    MaterialPath<'_> { path, subpath },
    MemoizePath<'_> { path, subpath, hash_operators, param_exprs, singlerow, binary_mode, calls, est_entries },
    UniquePath<'_> { path, subpath, umethod, in_operators, uniq_exprs },
    GatherPath<'_> { path, subpath, single_copy, num_workers },
    GatherMergePath<'_> { path, subpath, num_workers },
    ProjectionPath<'_> { path, subpath, dummypp },
    ProjectSetPath<'_> { path, subpath },
    SortPath<'_> { path, subpath },
    IncrementalSortPath<'_> { spath, nPresortedCols },
    GroupPath<'_> { path, subpath, groupClause, qual },
    UpperUniquePath<'_> { path, subpath, numkeys },
    AggPath<'_> { path, subpath, aggstrategy, aggsplit, numGroups, transitionSpace, groupClause, qual },
    GroupingSetData<'_> { set, numGroups },
    RollupData<'_> { groupClause, gsets, gsets_data, numGroups, hashable, is_hashed },
    GroupingSetsPath<'_> { path, subpath, aggstrategy, rollups, qual, transitionSpace },
    MinMaxAggPath<'_> { path, mmaggregates, quals },
    WindowAggPath<'_> { path, subpath, winclause, qual, runCondition, topwindow },
    SetOpPath<'_> { path, leftpath, rightpath, cmd, strategy, groupList, numGroups },
    RecursiveUnionPath<'_> { path, leftpath, rightpath, distinctList, wtParam, numGroups },
    LockRowsPath<'_> { path, subpath, rowMarks, epqParam },
    ModifyTablePath<'_> { path, subpath, operation, canSetTag, nominalRelation, rootRelation, partColsUpdated, resultRelations, updateColnosLists, withCheckOptionLists, returningLists, rowMarks, onconflict, epqParam, mergeActionLists, mergeJoinConditions },
    LimitPath<'_> { path, subpath, limitOffset, limitCount, limitOption },
    RestrictInfo<'_> { clause, is_pushed_down, can_join, pseudoconstant, has_clone, is_clone, leakproof, has_volatile, security_level, num_base_rels, clause_relids, required_relids, incompatible_relids, outer_relids, left_relids, right_relids, orclause, rinfo_serial, parent_ec, eval_cost, norm_selec, outer_selec, mergeopfamilies, left_ec, right_ec, left_em, right_em, scansel_cache, outer_is_left, hashjoinoperator, left_bucketsize, right_bucketsize, left_mcvfreq, right_mcvfreq, left_hasheqoperator, right_hasheqoperator },
    EquivalenceClass<'_> { ec_opfamilies, ec_collation, ec_childmembers_size, ec_members, ec_childmembers, ec_sources, ec_derives_list, ec_derives_hash, ec_relids, ec_has_const, ec_has_volatile, ec_broken, ec_sortref, ec_min_security, ec_max_security, ec_merged },
    EquivalenceMember<'_> { em_expr, em_relids, em_is_const, em_is_child, em_datatype, em_jdomain, em_parent },
    EquivalenceMemberIterator<'_> { ec, current_relid, child_relids, current_cell, current_list },
    ForeignKeyOptInfo<'_> { con_relid, ref_relid, nkeys, conkey, confkey, conpfeqop, nmatched_ec, nconst_ec, nmatched_rcols, nmatched_ri, eclass, fk_eclass_member, rinfos },
    StatisticExtInfo<'_> { stat_oid, inherit, rel, kind, keys, exprs },
    SpecialJoinInfo<'_> { min_lefthand, min_righthand, syn_lefthand, syn_righthand, jointype, ojrelid, commute_above_l, commute_above_r, commute_below_l, commute_below_r, lhs_strict, semi_can_btree, semi_can_hash, semi_operators, semi_rhs_exprs },
    OuterJoinClauseInfo<'_> { rinfo, sjinfo },
    PlaceHolderInfo<'_> { phid, ph_var_phexpr, ph_var_phrels, ph_eval_at, ph_lateral, ph_needed, ph_width },
    UniqueRelInfo<'_> { outerrelids, self_join, extra_clauses },
    RelOptInfo<'_> { reloptkind, relids, rows, consider_startup, consider_param_startup, consider_parallel, pathtarget_id, pathlist, ppilist, partial_pathlist, cheapest_startup_path, cheapest_total_path, cheapest_unique_path, cheapest_parameterized_paths, direct_lateral_relids, lateral_relids, lateral_vars, relid, reltablespace, rtekind, min_attr, max_attr, attr_widths, nulling_relids, lateral_referencers, pages, tuples, allvisfrac, baserestrictinfo, baserestrictcost, baserestrict_min_security, joininfo, has_eclass_joins, consider_partitionwise_join, serverid, userid, useridiscurrent, parent, top_parent, top_parent_relids, rel_parallel_workers, amflags, pgrcolumnar_sorted_attnos, pgrcolumnar_col_bytes, pgrcolumnar_col_ndv, pgrcolumnar_stitch_gndv, fdwroutine, attr_needed, notnullattnums, indexlist, statlist, eclass_indexes, subroot, subroot_idx, subplan_params, fdw_private, fdw_state, unique_for_rels, non_unique_for_rels, part_scheme, nparts, boundinfo, partbounds_merged, partition_qual, part_rels, live_parts, all_partrels, partexprs, nullable_partexprs },
    PlannerGlobal<'_> { subplans, subpaths, subroots, rewind_plan_ids, finalrtable, all_relids, prunable_relids, finalrteperminfos, finalrowmarks, result_relations, relation_oids, param_exec_types, last_ph_id, last_row_mark_id, last_plan_node_id, transient_plan, depends_on_role, parallel_mode_ok, parallel_mode_needed, max_parallel_hazard },
    WindowClauseNode<'_> { name, partitionClause, orderClause, frameOptions, startOffset, endOffset, startInRangeFunc, endInRangeFunc, inRangeColl, inRangeAsc, inRangeNullsFirst, winref },
    AggInfo<'_> { aggrefs, transno, shareable, finalfn_oid },
    AggTransInfo<'_> { args, aggfilter, transfn_oid, serialfn_oid, deserialfn_oid, combinefn_oid, aggtranstype, aggtranstypmod, transtypeLen, transtypeByVal, aggtransspace, initValue, initValueIsNull, initValueImage },
    TargetEntryNode<'_> { expr, resno, resname, ressortgroupref, resorigtbl, resorigcol, resjunk },
    PlannerInfo<'_> { mcx, parse, command_type, glob, query_level, parent_root, plan_params, outer_params, simple_rel_array, simple_rel_array_size, simple_rte_array, append_rel_array, all_baserels, outer_join_rels, all_query_rels, join_rel_list, join_rel_hash, join_rel_level, join_cur_level, init_plans, cte_plan_ids, cte_list, cte_scan_params, multiexpr_params, join_domains, eq_classes, ec_merging_done, canon_pathkeys, left_join_clauses, right_join_clauses, full_join_clauses, join_info_list, last_rinfo_serial, all_result_relids, leaf_result_relids, append_rel_list, row_identity_vars, rowMarks, placeholder_list, placeholder_array, placeholder_array_size, fkey_list, query_pathkeys, group_pathkeys, num_groupby_pathkeys, window_pathkeys, distinct_pathkeys, sort_pathkeys, setop_pathkeys, part_schemes, initial_rels, upper_rels, upper_targets, processed_groupClause, processed_distinctClause, processed_tlist, update_colnos, grouping_map, minmax_aggs, total_table_pages, tuple_fraction, limit_tuples, qual_security_level, hasJoinRTEs, hasLateralRTEs, hasHavingQual, hasPseudoConstantQuals, hasAlternativeSubPlans, placeholdersFrozen, hasRecursion, group_rtindex, agginfos, aggtransinfos, numOrderedAggs, hasNonPartialAggs, hasNonSerialAggs, wt_param_id, non_recursive_path, non_recursive_rows, self_ref_wt_param, curOuterRels, curOuterParams, partColsUpdated, join_search_private, isAltSubplan, isUsedSubplan, rel_arena, path_arena, rinfo_arena, em_arena, ph_info_arena, node_arena, pathtarget_arena },
);

// partsupfunc exempt: plain fmgr_info resolutions, fn_expr never set on the
// plancat path, so forgetting leaks nothing; revisit when partitionwise
// planning fills part_schemes.
mcx::forget_safe_struct!(
    PartitionSchemeData<'_> { strategy, partnatts, partopfamily, partopcintype,
        partcollation, parttyplen, parttypbyval; partsupfunc },
);

mcx::forget_safe_tuple!(Subroot<'_>(inner));

mcx::forget_safe_enum!(
    Bitmapset<'_> { Small(x), Big(x) },
    JoinlistNode<'_> { Rel(x), Sub(x) },
    DatumImage<'_> { ByVal(x), Bytes(x) },
    ArenaNode<'_> { Reserved, Expr(x), TargetEntry(x), ForeignKey(x),
        StatisticExt(x), AggInfo(x), AggTransInfo(x), PlannerParamItem(x),
        MinMaxAggInfo(x), WindowClause(x) },
    PathNode<'_> {
        Path(x),
        IndexPath(x),
        BitmapHeapPath(x),
        BitmapAndPath(x),
        BitmapOrPath(x),
        TidPath(x),
        TidRangePath(x),
        SubqueryScanPath(x),
        ForeignPath(x),
        CustomPath(x),
        NestPath(x),
        MergePath(x),
        HashPath(x),
        AppendPath(x),
        MergeAppendPath(x),
        GroupResultPath(x),
        MaterialPath(x),
        MemoizePath(x),
        UniquePath(x),
        GatherPath(x),
        GatherMergePath(x),
        ProjectionPath(x),
        ProjectSetPath(x),
        SortPath(x),
        IncrementalSortPath(x),
        GroupPath(x),
        UpperUniquePath(x),
        AggPath(x),
        GroupingSetsPath(x),
        MinMaxAggPath(x),
        WindowAggPath(x),
        SetOpPath(x),
        RecursiveUnionPath(x),
        LockRowsPath(x),
        ModifyTablePath(x),
        LimitPath(x),
    },
);

// selfuncs.h defaults (shared with the cost model).
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;

pub fn tag16(tag: ::types_nodes::NodeTag) -> u16 {
    tag as u16
}

// IS_OUTER_JOIN (nodes.h) over pathnodes' u32 JoinType.
pub fn is_outer_join(jointype: u32) -> bool {
    matches!(
        jointype,
        JOIN_LEFT
            | JOIN_FULL
            | JOIN_RIGHT
            | JOIN_ANTI
            | JOIN_RIGHT_ANTI
    )
}

// SemiAntiJoinFactors (pathnodes.h).
#[derive(Clone, Copy, Default)]
pub struct SemiAntiJoinFactors {
    pub outer_match_frac: f64,
    pub match_count: f64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PathKeysComparison {
    Equal,
    Better1,
    Better2,
    Different,
}

pub fn compare_pathkeys(keys1: &[PathKey], keys2: &[PathKey]) -> PathKeysComparison {
    for (k1, k2) in keys1.iter().zip(keys2.iter()) {
        if k1 != k2 {
            return PathKeysComparison::Different;
        }
    }
    match keys1.len().cmp(&keys2.len()) {
        core::cmp::Ordering::Greater => PathKeysComparison::Better1,
        core::cmp::Ordering::Less => PathKeysComparison::Better2,
        core::cmp::Ordering::Equal => PathKeysComparison::Equal,
    }
}

// Returns (contained, n leading matches).
pub fn pathkeys_count_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> (bool, usize) {
    let mut n = 0;
    for (k1, k2) in keys1.iter().zip(keys2.iter()) {
        if k1 != k2 {
            return (false, n);
        }
        n += 1;
    }
    (n == keys1.len(), n)
}

pub fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool {
    matches!(
        compare_pathkeys(keys1, keys2),
        PathKeysComparison::Equal | PathKeysComparison::Better2
    )
}

mcx::forget_safe_struct!(SemiAntiJoinFactors { outer_match_frac, match_count });
mcx::forget_safe_struct!(CteScanParam { rti, plan_id, cte_param });
