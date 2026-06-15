//! `#[repr(C)]` ABI for `nodeHashjoin.c` (the hash-join executor node).
//!
//! The hash-join node is ported in-crate (`backend-executor-nodeHashjoin`), so
//! its state node `HashJoinState` is a complete, address-stable `#[repr(C)]`
//! struct laid out exactly like the C `HashJoinState` (execnodes.h), and the
//! `HashJoin` plan node it navigates is spelled out here too. The hash-table
//! machinery, the sibling `Hash`/`HashState` nodes, and the parallel-hash shared
//! structures live in [`crate::nodehash_abi`] (shared with `nodeHash.c`); this
//! module reuses them and adds only the hash-*join*-specific layouts plus the
//! barrier-phase constants from `executor/hashjoin.h`.
//!
//! The embedded `JoinState`/`PlanState` heads reuse the shared
//! [`crate::nodenestloop_abi::JoinStateData`] / [`crate::PlanStateData`] layouts,
//! and the leading `Plan` of the `HashJoin` plan node is the abstract
//! [`crate::nodenestloop_abi::Join`] base.

use core::ffi::c_int;

use crate::nodehash_abi::{HashJoinTable, HashJoinTuple};
use crate::nodenestloop_abi::{Join, JoinStateData};
use crate::{ExprState, List, PlanStateData, TupleTableSlot};

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
///
/// The leading `join.plan` is the abstract plan-node base, so a `*mut HashJoin`
/// is also a valid `Node *` / `Plan *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HashJoin {
    /// `Join join` — its first field (`plan`) starts with the `NodeTag`.
    pub join: Join,
    /// `List *hashclauses`.
    pub hashclauses: *mut List,
    /// `List *hashoperators`.
    pub hashoperators: *mut List,
    /// `List *hashcollations`.
    pub hashcollations: *mut List,
    /// `List *hashkeys` — expressions hashed for outer-plan tuples.
    pub hashkeys: *mut List,
}

/// `HashJoinState` (execnodes.h) — the per-node execution state of a hash join.
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
///
/// The leading [`JoinStateData`] head's first member is a `NodeTag`, so a
/// `*mut HashJoinStateData` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
pub struct HashJoinStateData {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData,
    /// `ExprState *hashclauses`.
    pub hashclauses: *mut ExprState,
    /// `ExprState *hj_OuterHash`.
    pub hj_OuterHash: *mut ExprState,
    /// `HashJoinTable hj_HashTable`.
    pub hj_HashTable: HashJoinTable,
    /// `uint32 hj_CurHashValue`.
    pub hj_CurHashValue: u32,
    /// `int hj_CurBucketNo`.
    pub hj_CurBucketNo: c_int,
    /// `int hj_CurSkewBucketNo`.
    pub hj_CurSkewBucketNo: c_int,
    /// `HashJoinTuple hj_CurTuple`.
    pub hj_CurTuple: HashJoinTuple,
    /// `TupleTableSlot *hj_OuterTupleSlot`.
    pub hj_OuterTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *hj_HashTupleSlot`.
    pub hj_HashTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *hj_NullOuterTupleSlot`.
    pub hj_NullOuterTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *hj_NullInnerTupleSlot`.
    pub hj_NullInnerTupleSlot: *mut TupleTableSlot,
    /// `TupleTableSlot *hj_FirstOuterTupleSlot`.
    pub hj_FirstOuterTupleSlot: *mut TupleTableSlot,
    /// `int hj_JoinState` — the HJ state-machine state.
    pub hj_JoinState: c_int,
    /// `bool hj_MatchedOuter`.
    pub hj_MatchedOuter: bool,
    /// `bool hj_OuterNotEmpty`.
    pub hj_OuterNotEmpty: bool,
}

// ===========================================================================
// Barrier-phase constants (executor/hashjoin.h). The build_barrier phases
// coordinate the parallel build; the batch_barrier phases coordinate probing.
// ===========================================================================

/// `PHJ_BUILD_ELECT` — initial build phase.
pub const PHJ_BUILD_ELECT: c_int = 0;
/// `PHJ_BUILD_ALLOCATE` — one sets up the batches and table 0.
pub const PHJ_BUILD_ALLOCATE: c_int = 1;
/// `PHJ_BUILD_HASH_INNER` — all hash the inner rel.
pub const PHJ_BUILD_HASH_INNER: c_int = 2;
/// `PHJ_BUILD_HASH_OUTER` — (multi-batch only) all hash the outer.
pub const PHJ_BUILD_HASH_OUTER: c_int = 3;
/// `PHJ_BUILD_RUN` — building done, probing can begin.
pub const PHJ_BUILD_RUN: c_int = 4;
/// `PHJ_BUILD_FREE` — all work complete, one frees batches.
pub const PHJ_BUILD_FREE: c_int = 5;

/// `PHJ_BATCH_ELECT` — initial batch phase.
pub const PHJ_BATCH_ELECT: c_int = 0;
/// `PHJ_BATCH_ALLOCATE` — one allocates buckets.
pub const PHJ_BATCH_ALLOCATE: c_int = 1;
/// `PHJ_BATCH_LOAD` — all load the hash table from disk.
pub const PHJ_BATCH_LOAD: c_int = 2;
/// `PHJ_BATCH_PROBE` — all probe.
pub const PHJ_BATCH_PROBE: c_int = 3;
/// `PHJ_BATCH_SCAN` — one does the unmatched scan.
pub const PHJ_BATCH_SCAN: c_int = 4;
/// `PHJ_BATCH_FREE` — one frees memory.
pub const PHJ_BATCH_FREE: c_int = 5;

// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut HashJoinStateData` can be navigated as the C `HashJoinState *`, and a
// `*mut HashJoin` as `Plan *`.
const _: () = {
    use crate::PlanNode;
    use core::mem::{offset_of, size_of};

    // HashJoinState: JoinState at offset 0 (so `&self` is a valid Node*/PlanState*).
    assert!(offset_of!(HashJoinStateData, js) == 0);
    assert!(offset_of!(JoinStateData, ps) == 0);
    assert!(offset_of!(PlanStateData, type_) == 0);
    // hashclauses follows the JoinState head.
    assert!(offset_of!(HashJoinStateData, hashclauses) == size_of::<JoinStateData>());

    // HashJoin: Join at offset 0; Join.plan at offset 0 (PlanNode base with NodeTag).
    assert!(offset_of!(HashJoin, join) == 0);
    assert!(offset_of!(Join, plan) == 0);
    assert!(offset_of!(PlanNode, type_) == 0);
    assert!(offset_of!(HashJoin, hashclauses) == size_of::<Join>());
};
