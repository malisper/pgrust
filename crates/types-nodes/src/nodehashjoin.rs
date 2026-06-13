//! Hash-join node vocabulary (nodes/plannodes.h, executor/execnodes.h,
//! executor/hashjoin.h), trimmed to what `nodeHashjoin.c` consumes.
//!
//! This module is additive (docs/types.md rule 4 + the node-knot crate): it adds
//! the `Join`/`JoinState` head shared by every join node, the `JoinType` enum,
//! the `HashJoin` plan node and `HashJoinState` execution state, the hash-table
//! handle the hash join navigates (`HashJoinTableData`, owned by nodeHash but
//! consumed here), and the parallel-hash barrier-phase constants.

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;

use crate::execnodes::{Opaque, PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::Node;

/// `T_HashJoin` (nodes/nodetags.h) — the plan-node tag for a HashJoin.
pub const T_HashJoin: crate::nodes::NodeTag = crate::nodes::NodeTag(371);
/// `T_HashJoinState` (nodes/nodetags.h) — the executor-state node tag.
pub const T_HashJoinState: crate::nodes::NodeTag = crate::nodes::NodeTag(437);

/// `JoinType` (nodes/nodes.h) — values verified against PostgreSQL 18.3
/// `src/include/nodes/nodes.h`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JoinType {
    /// `JOIN_INNER` — matching tuple pairs only.
    JOIN_INNER = 0,
    /// `JOIN_LEFT` — pairs + unmatched LHS tuples.
    JOIN_LEFT = 1,
    /// `JOIN_FULL` — pairs + unmatched LHS + unmatched RHS.
    JOIN_FULL = 2,
    /// `JOIN_RIGHT` — pairs + unmatched RHS tuples.
    JOIN_RIGHT = 3,
    /// `JOIN_SEMI` — 1 copy of each LHS row that has any match.
    JOIN_SEMI = 4,
    /// `JOIN_ANTI` — 1 copy of each LHS row that has no match.
    JOIN_ANTI = 5,
    /// `JOIN_RIGHT_SEMI` — 1 copy of each RHS row that has any match.
    JOIN_RIGHT_SEMI = 6,
    /// `JOIN_RIGHT_ANTI` — 1 copy of each RHS row that has no match.
    JOIN_RIGHT_ANTI = 7,
    /// `JOIN_UNIQUE_OUTER` — LHS path must be made unique.
    JOIN_UNIQUE_OUTER = 8,
    /// `JOIN_UNIQUE_INNER` — RHS path must be made unique.
    JOIN_UNIQUE_INNER = 9,
}

pub use JoinType::{
    JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI,
    JOIN_SEMI, JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER,
};

pub use crate::execexpr::ExprState;

/// `Join` plan node (plannodes.h) — the abstract base every join plan embeds:
///
/// ```c
/// typedef struct Join
/// {
///     Plan        plan;
///     JoinType    jointype;
///     bool        inner_unique;
///     List       *joinqual;       /* JOIN quals (in addition to plan.qual) */
/// } Join;
/// ```
#[derive(Debug)]
pub struct Join<'mcx> {
    /// `Plan plan` — its first field starts with the `NodeTag`.
    pub plan: Plan<'mcx>,
    /// `JoinType jointype`.
    pub jointype: JoinType,
    /// `bool inner_unique` — each outer tuple provably matches no more than one
    /// inner tuple.
    pub inner_unique: bool,
    /// `List *joinqual` — JOIN quals (in addition to `plan.qual`), a
    /// heterogeneous expression list. `None` is the C `NIL`.
    pub joinqual: Option<PgVec<'mcx, Node<'mcx>>>,
}

impl<'mcx> Join<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Join<'b>> {
        let joinqual = match &self.joinqual {
            Some(q) => {
                let mut out = mcx::vec_with_capacity_in(mcx, q.len())?;
                for n in q.iter() {
                    out.push(n.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(Join {
            plan: self.plan.clone_in(mcx)?,
            jointype: self.jointype,
            inner_unique: self.inner_unique,
            joinqual,
        })
    }
}

/// `HashJoin` plan node (plannodes.h):
///
/// ```c
/// typedef struct HashJoin
/// {
///     Join        join;
///     List       *hashclauses;
///     List       *hashoperators;
///     List       *hashcollations;
///     List       *hashkeys;
/// } HashJoin;
/// ```
#[derive(Debug)]
pub struct HashJoin<'mcx> {
    /// `Join join` — its first field (`plan`) starts with the `NodeTag`.
    pub join: Join<'mcx>,
    /// `List *hashclauses` — the hash-join clause expressions (`OpExpr` nodes),
    /// a heterogeneous expression list.
    pub hashclauses: Option<PgVec<'mcx, Node<'mcx>>>,
    /// `List *hashoperators` — list of the per-clause hash operator OIDs.
    pub hashoperators: PgVec<'mcx, Oid>,
    /// `List *hashcollations` — list of the per-clause input collation OIDs.
    pub hashcollations: PgVec<'mcx, Oid>,
    /// `List *hashkeys` — expressions hashed for outer-plan tuples (one per
    /// hash clause), a heterogeneous expression list.
    pub hashkeys: Option<PgVec<'mcx, Node<'mcx>>>,
}

impl<'mcx> HashJoin<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<HashJoin<'b>> {
        fn clone_node_list<'a, 'b>(
            mcx: Mcx<'b>,
            l: &Option<PgVec<'a, Node<'a>>>,
        ) -> PgResult<Option<PgVec<'b, Node<'b>>>> {
            match l {
                Some(v) => {
                    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
                    for n in v.iter() {
                        out.push(n.clone_in(mcx)?);
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        }
        let mut hashoperators = mcx::vec_with_capacity_in(mcx, self.hashoperators.len())?;
        for o in self.hashoperators.iter() {
            hashoperators.push(*o);
        }
        let mut hashcollations = mcx::vec_with_capacity_in(mcx, self.hashcollations.len())?;
        for o in self.hashcollations.iter() {
            hashcollations.push(*o);
        }
        Ok(HashJoin {
            join: self.join.clone_in(mcx)?,
            hashclauses: clone_node_list(mcx, &self.hashclauses)?,
            hashoperators,
            hashcollations,
            hashkeys: clone_node_list(mcx, &self.hashkeys)?,
        })
    }
}

/// `T_Hash` (nodes/nodetags.h) — the plan-node tag for the inner Hash node of a
/// hash join.
pub const T_Hash: crate::nodes::NodeTag = crate::nodes::NodeTag(370);

/// `Hash` plan node (plannodes.h) — the inner child of a `HashJoin`:
///
/// ```c
/// typedef struct Hash
/// {
///     Plan        plan;
///     List       *hashkeys;
///     Oid         skewTable;     /* outer join key's table OID, or InvalidOid */
///     AttrNumber  skewColumn;    /* outer join key's column #, or zero */
///     bool        skewInherit;   /* is outer join rel an inheritance tree? */
///     Cardinality rows_total;    /* estimate total rows if parallel_aware */
/// } Hash;
/// ```
///
/// Trimmed to the fields `nodeHashjoin.c` consumes (`skewTable`, read by
/// `ExecInitHashJoin` to gate skew-hashfunction setup); `hashkeys` is built by
/// the planner and consumed by nodeHash, the rest of the skew fields are
/// nodeHash-owned.
#[derive(Debug, Default)]
pub struct Hash<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `List *hashkeys` — hash keys for the hashjoin condition, a heterogeneous
    /// expression list (`None` = the C `NIL`).
    pub hashkeys: Option<PgVec<'mcx, Node<'mcx>>>,
    /// `Oid skewTable` — outer join key's table OID, or `InvalidOid`.
    pub skewTable: Oid,
    /// `AttrNumber skewColumn` — outer join key's column #, or zero.
    pub skewColumn: i16,
    /// `bool skewInherit` — is the outer join rel an inheritance tree?
    pub skewInherit: bool,
    /// `Cardinality rows_total` — estimate of total rows if `parallel_aware`.
    pub rows_total: f64,
}

impl<'mcx> Hash<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Hash<'b>> {
        let hashkeys = match &self.hashkeys {
            Some(v) => {
                let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
                for n in v.iter() {
                    out.push(n.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(Hash {
            plan: self.plan.clone_in(mcx)?,
            hashkeys,
            skewTable: self.skewTable,
            skewColumn: self.skewColumn,
            skewInherit: self.skewInherit,
            rows_total: self.rows_total,
        })
    }
}

/// `JoinState` head (execnodes.h) — the executor-state base every join state
/// node embeds:
///
/// ```c
/// typedef struct JoinState
/// {
///     PlanState   ps;
///     JoinType    jointype;
///     bool        single_match;
///     ExprState  *joinqual;
/// } JoinState;
/// ```
#[derive(Debug)]
pub struct JoinStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `JoinType jointype`.
    pub jointype: JoinType,
    /// `bool single_match` — true if we should skip to next outer tuple after
    /// finding one inner match.
    pub single_match: bool,
    /// `ExprState *joinqual` — JOIN quals (in addition to `ps.qual`).
    pub joinqual: Option<PgBox<'mcx, ExprState>>,
}

impl<'mcx> Default for JoinStateData<'mcx> {
    fn default() -> Self {
        JoinStateData {
            ps: PlanStateData::default(),
            jointype: JoinType::JOIN_INNER,
            single_match: false,
            joinqual: None,
        }
    }
}

/// `HashJoinState` (execnodes.h) — the per-node execution state of a hash join:
///
/// ```c
/// typedef struct HashJoinState
/// {
///     JoinState   js;             /* its first field is NodeTag */
///     ExprState  *hashclauses;
///     ExprState  *hj_OuterHash;
///     HashJoinTable hj_HashTable;
///     uint32      hj_CurHashValue;
///     int         hj_CurBucketNo;
///     int         hj_CurSkewBucketNo;
///     HashJoinTuple hj_CurTuple;
///     TupleTableSlot *hj_OuterTupleSlot;
///     TupleTableSlot *hj_HashTupleSlot;
///     TupleTableSlot *hj_NullOuterTupleSlot;
///     TupleTableSlot *hj_NullInnerTupleSlot;
///     TupleTableSlot *hj_FirstOuterTupleSlot;
///     int         hj_JoinState;
///     bool        hj_MatchedOuter;
///     bool        hj_OuterNotEmpty;
/// } HashJoinState;
/// ```
#[derive(Debug)]
pub struct HashJoinState<'mcx> {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData<'mcx>,
    /// `ExprState *hashclauses`.
    pub hashclauses: Option<PgBox<'mcx, ExprState>>,
    /// `ExprState *hj_OuterHash`.
    pub hj_OuterHash: Option<PgBox<'mcx, ExprState>>,
    /// `HashJoinTable hj_HashTable` — the hash table (built lazily; `None` until
    /// `HJ_BUILD_HASHTABLE`). Owned by nodeHash; the box is context-allocated.
    pub hj_HashTable: Option<PgBox<'mcx, HashJoinTableData<'mcx>>>,
    /// `uint32 hj_CurHashValue`.
    pub hj_CurHashValue: u32,
    /// `int hj_CurBucketNo`.
    pub hj_CurBucketNo: i32,
    /// `int hj_CurSkewBucketNo`.
    pub hj_CurSkewBucketNo: i32,
    /// `HashJoinTuple hj_CurTuple` — current tuple in the scanned bucket: an
    /// arena index into the hash table's tuple arena, or `None` for the C
    /// `NULL`.
    pub hj_CurTuple: Option<HashTupleIdx>,
    /// `TupleTableSlot *hj_OuterTupleSlot` — id into `es_tupleTable`.
    pub hj_OuterTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_HashTupleSlot` — id into `es_tupleTable` (the inner
    /// Hash node's result slot, adopted at init).
    pub hj_HashTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_NullOuterTupleSlot` — id into `es_tupleTable`.
    /// Presence is the C `HJ_FILL_INNER` predicate.
    pub hj_NullOuterTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_NullInnerTupleSlot` — id into `es_tupleTable`.
    /// Presence is the C `HJ_FILL_OUTER` predicate.
    pub hj_NullInnerTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *hj_FirstOuterTupleSlot` — id into `es_tupleTable`; set
    /// when the prefetched first outer tuple has not been consumed yet.
    pub hj_FirstOuterTupleSlot: Option<SlotId>,
    /// `int hj_JoinState` — the HJ state-machine state.
    pub hj_JoinState: i32,
    /// `bool hj_MatchedOuter`.
    pub hj_MatchedOuter: bool,
    /// `bool hj_OuterNotEmpty`.
    pub hj_OuterNotEmpty: bool,
}

impl<'mcx> Default for HashJoinState<'mcx> {
    fn default() -> Self {
        HashJoinState {
            js: JoinStateData::default(),
            hashclauses: None,
            hj_OuterHash: None,
            hj_HashTable: None,
            hj_CurHashValue: 0,
            hj_CurBucketNo: 0,
            hj_CurSkewBucketNo: -1,
            hj_CurTuple: None,
            hj_OuterTupleSlot: None,
            hj_HashTupleSlot: None,
            hj_NullOuterTupleSlot: None,
            hj_NullInnerTupleSlot: None,
            hj_FirstOuterTupleSlot: None,
            hj_JoinState: 0,
            hj_MatchedOuter: false,
            hj_OuterNotEmpty: false,
        }
    }
}

// ===========================================================================
// Hash-table vocabulary (executor/hashjoin.h). The full layout is owned by
// nodeHash; trimmed here to the fields nodeHashjoin.c reads/writes directly
// (the rest of the hash-table machinery is reached through nodeHash seams).
// ===========================================================================

/// `INVALID_SKEW_BUCKET_NO` (executor/hashjoin.h).
pub const INVALID_SKEW_BUCKET_NO: i32 = -1;

/// Index of a `HashJoinTupleData` inside the hash table's owned tuple arena.
/// C's `HashJoinTuple` (`HashJoinTupleData *`) bucket-chain head becomes a
/// stable arena index in the owned model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HashTupleIdx(pub usize);

/// `BufFile` (storage/buffile.h) — a buffered virtual temp file. Owned by the
/// buffile subsystem; the hash join only stores the handle and passes it back
/// to the buffile seams (create/seek/read/write/close), so it is a real opaque
/// resource handle here (the foreign-opacity case in docs/types.md rule 6).
#[derive(Debug, Default)]
pub struct BufFile(pub Opaque);

/// `SharedTuplestoreAccessor` (utils/sharedtuplestore.h) — a backend's handle
/// to a shared tuplestore partition. Owned by the sharedtuplestore subsystem;
/// opaque to the hash join.
#[derive(Debug, Default)]
pub struct SharedTuplestoreAccessor(pub Opaque);

/// `ParallelHashJoinState` (executor/hashjoin.h) — DSM-resident shared state
/// coordinating a Parallel Hash Join. Owned by nodeHash; opaque to the hash
/// join (it reaches the `distributor`/`build_barrier` through nodeHash seams).
#[derive(Debug, Default)]
pub struct ParallelHashJoinState(pub Opaque);

/// `ParallelHashJoinBatch` (executor/hashjoin.h) — shared per-batch state, used
/// only through the per-batch barrier; opaque here.
#[derive(Debug, Default)]
pub struct ParallelHashJoinBatch(pub Opaque);

/// `ParallelHashJoinBatchAccessor` (executor/hashjoin.h) — each backend's
/// per-batch accessor. Trimmed to the fields nodeHashjoin.c reads/writes
/// directly (`done`, `outer_eof`, the two `SharedTuplestoreAccessor *`, and the
/// shared per-batch state for the batch_barrier).
#[derive(Debug, Default)]
pub struct ParallelHashJoinBatchAccessor<'mcx> {
    /// `ParallelHashJoinBatch *shared` — pointer to shared state.
    pub shared: Option<PgBox<'mcx, ParallelHashJoinBatch>>,
    /// `bool outer_eof` — has this process hit end of batch?
    pub outer_eof: bool,
    /// `bool done` — flag to remember that a batch is done.
    pub done: bool,
    /// `SharedTuplestoreAccessor *inner_tuples`.
    pub inner_tuples: Option<PgBox<'mcx, SharedTuplestoreAccessor>>,
    /// `SharedTuplestoreAccessor *outer_tuples`.
    pub outer_tuples: Option<PgBox<'mcx, SharedTuplestoreAccessor>>,
}

/// `HashJoinTableData` (executor/hashjoin.h) — the per-hashjoin hash table,
/// trimmed to the fields `nodeHashjoin.c` accesses directly. The bucket arrays,
/// chunk allocation, skew hashing, and parallel coordination are driven through
/// nodeHash seams; this carries the batch bookkeeping the hash-join state
/// machine reads/writes itself.
#[derive(Debug)]
pub struct HashJoinTableData<'mcx> {
    /// `int nbatch` — number of batches.
    pub nbatch: i32,
    /// `int curbatch` — current batch #; 0 during 1st pass.
    pub curbatch: i32,
    /// `int nbatch_original` — nbatch when we started inner scan.
    pub nbatch_original: i32,
    /// `int nbatch_outstart` — nbatch when we started outer scan.
    pub nbatch_outstart: i32,
    /// `double totalTuples` — # tuples obtained from inner plan.
    pub totalTuples: f64,
    /// `bool skewEnabled` — are we using skew optimization?
    pub skewEnabled: bool,
    /// `int nSkewBuckets` — number of active skew buckets.
    pub nSkewBuckets: i32,
    /// `Size spaceUsedSkew` — skew hash table's current space usage.
    pub spaceUsedSkew: usize,
    /// `BufFile **innerBatchFile` — buffered virtual temp file per batch.
    /// Empty until allocated; entries are `None` for an unspilled batch.
    pub innerBatchFile: PgVec<'mcx, Option<PgBox<'mcx, BufFile>>>,
    /// `BufFile **outerBatchFile` — buffered virtual temp file per batch.
    pub outerBatchFile: PgVec<'mcx, Option<PgBox<'mcx, BufFile>>>,
    /// `ParallelHashJoinBatchAccessor *batches` — per-batch accessors (parallel
    /// hash join only). Empty in the parallel-oblivious case.
    pub batches: PgVec<'mcx, ParallelHashJoinBatchAccessor<'mcx>>,
    /// `ParallelHashJoinState *parallel_state` — DSM-resident shared state, or
    /// `None` for a parallel-oblivious hash join.
    pub parallel_state: Option<PgBox<'mcx, ParallelHashJoinState>>,
    /// `MemoryContext spillCxt` — context for spilling to temp files. Carried as
    /// the per-query allocator handle the spill files are charged to (C: a child
    /// context of `hashCxt`); used by `ExecHashJoinSaveTuple`.
    pub spillCxt: Mcx<'mcx>,
}

// ===========================================================================
// Barrier-phase constants (executor/hashjoin.h). The build_barrier phases
// coordinate the parallel build; the batch_barrier phases coordinate probing.
// ===========================================================================

/// `PHJ_BUILD_ELECT` — initial build phase.
pub const PHJ_BUILD_ELECT: i32 = 0;
/// `PHJ_BUILD_ALLOCATE` — one sets up the batches and table 0.
pub const PHJ_BUILD_ALLOCATE: i32 = 1;
/// `PHJ_BUILD_HASH_INNER` — all hash the inner rel.
pub const PHJ_BUILD_HASH_INNER: i32 = 2;
/// `PHJ_BUILD_HASH_OUTER` — (multi-batch only) all hash the outer.
pub const PHJ_BUILD_HASH_OUTER: i32 = 3;
/// `PHJ_BUILD_RUN` — building done, probing can begin.
pub const PHJ_BUILD_RUN: i32 = 4;
/// `PHJ_BUILD_FREE` — all work complete, one frees batches.
pub const PHJ_BUILD_FREE: i32 = 5;

/// `PHJ_BATCH_ELECT` — initial batch phase.
pub const PHJ_BATCH_ELECT: i32 = 0;
/// `PHJ_BATCH_ALLOCATE` — one allocates buckets.
pub const PHJ_BATCH_ALLOCATE: i32 = 1;
/// `PHJ_BATCH_LOAD` — all load the hash table from disk.
pub const PHJ_BATCH_LOAD: i32 = 2;
/// `PHJ_BATCH_PROBE` — all probe.
pub const PHJ_BATCH_PROBE: i32 = 3;
/// `PHJ_BATCH_SCAN` — one does the unmatched scan.
pub const PHJ_BATCH_SCAN: i32 = 4;
/// `PHJ_BATCH_FREE` — one frees memory.
pub const PHJ_BATCH_FREE: i32 = 5;
