//! Hash-join node vocabulary (nodes/plannodes.h, executor/execnodes.h,
//! executor/hashjoin.h), trimmed to what `nodeHashjoin.c` consumes.
//!
//! This module is additive (docs/types.md rule 4 + the node-knot crate): it adds
//! the `Join`/`JoinState` head shared by every join node, the `JoinType` enum,
//! the `HashJoin` plan node and `HashJoinState` execution state, the hash-table
//! handle the hash join navigates (`HashJoinTableData`, owned by nodeHash but
//! consumed here), and the parallel-hash barrier-phase constants.

use mcx::{Mcx, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

use crate::nodeindexscan::Plan;
use crate::nodes::Node;

/// `T_HashJoin` (nodes/nodetags.h) — the plan-node tag for a HashJoin.
pub const T_HashJoin: crate::nodes::NodeTag = crate::nodes::NodeTag(359);
/// `T_HashJoinState` (nodes/nodetags.h) — the executor-state node tag. This is
/// the single canonical value from `execstate_tags` (423); re-exported here so
/// the hash-join vocabulary reads as one module.
pub use crate::execstate_tags::T_HashJoinState;

// `JoinType`, the `Join` plan-node base and the `JoinState` head are the
// canonical shapes from the `jointype` module (main's single source of truth,
// shared with nodeMergejoin); re-exported here so the hash-join vocabulary
// reads as one module. `JOIN_RIGHT_SEMI` was added to that canonical enum.
pub use crate::execexpr::ExprState;
pub use crate::jointype::{
    Join, JoinStateData, JoinType, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT,
    JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI, JOIN_SEMI, JOIN_UNIQUE_INNER, JOIN_UNIQUE_OUTER,
};

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
                    let mut out = ::mcx::vec_with_capacity_in(mcx, v.len())?;
                    for n in v.iter() {
                        out.push(n.clone_in(mcx)?);
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        }
        let mut hashoperators = ::mcx::vec_with_capacity_in(mcx, self.hashoperators.len())?;
        for o in self.hashoperators.iter() {
            hashoperators.push(*o);
        }
        let mut hashcollations = ::mcx::vec_with_capacity_in(mcx, self.hashcollations.len())?;
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
                let mut out = ::mcx::vec_with_capacity_in(mcx, v.len())?;
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


// ===========================================================================
// Canonical hash-join / hash-table vocabulary lives in `crate::nodehash`.
// The opacity-introducing stand-ins that used to live here (a slim
// HashJoinTableData, ParallelHashJoinState(Opaque), ParallelHashJoinBatch,
// the trimmed accessor, BufFile/SharedTuplestoreAccessor, HashTupleIdx,
// INVALID_SKEW_BUCKET_NO, the PHJ_* phase consts) have been deleted; this
// module re-exports the real, C-faithful types so the seam crates and the
// nodeHashjoin consumer keep their `nodes::nodehashjoin::*` paths.
// ===========================================================================

pub use crate::nodehash::{
    BufFile, HashJoinState, HashJoinTableData, HashTupleIdx, ParallelHashJoinBatch,
    ParallelHashJoinBatchAccessor, ParallelHashJoinState, SharedTuplestoreAccessor,
    INVALID_SKEW_BUCKET_NO, PHJ_BATCH_ALLOCATE, PHJ_BATCH_ELECT, PHJ_BATCH_FREE, PHJ_BATCH_LOAD,
    PHJ_BATCH_PROBE, PHJ_BATCH_SCAN, PHJ_BUILD_ALLOCATE, PHJ_BUILD_ELECT, PHJ_BUILD_FREE,
    PHJ_BUILD_HASH_INNER, PHJ_BUILD_HASH_OUTER, PHJ_BUILD_RUN,
};
