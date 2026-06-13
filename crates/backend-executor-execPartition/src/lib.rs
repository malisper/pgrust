//! `executor/execPartition.c` — support routines for partitioning.
//!
//! Two responsibilities, split across family modules:
//!   * tuple routing — `routing_setup` (build/teardown of the
//!     `PartitionTupleRouting`/`PartitionDispatch` structures) and
//!     `routing_find` (`ExecFindPartition` and the per-tuple key extraction +
//!     partition search), with `colnos` holding the UPDATE target-column
//!     remapping helpers.
//!   * run-time partition pruning — `pruning` (initial/exec pruning state
//!     setup and the `ExecFindMatchingSubPlans` evaluator).
//!
//! The `PartitionTupleRouting` and `PartitionDispatchData` structs are private
//! to this translation unit, so they live here (not in `types-nodes`).

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::primitive::AttrNumber;
use types_nodes::partition::{PartitionDescData, PartitionKeyData};
use types_nodes::{
    execexpr::ExprState, EcxtId, Opaque, ResultRelInfo, RriId, SlotId, TupleTableSlot,
};
use types_rel::Relation;
use types_tuple::attmap::AttrMap;

pub mod colnos;
pub mod pruning;
pub mod routing_find;
pub mod routing_setup;

/// `PARTITION_MAX_KEYS` (`pg_config_manual.h`): max columns in a partition key.
pub const PARTITION_MAX_KEYS: usize = 32;

/// `PARTITION_CACHED_FIND_THRESHOLD` (execPartition.c): number of consecutive
/// same-partition finds before `get_partition_for_tuple` switches from a binary
/// search to a cached last-found check. Must be above 0.
pub const PARTITION_CACHED_FIND_THRESHOLD: i32 = 16;

/// `PartitionDispatchData` (execPartition.c, private): per-partitioned-table
/// info needed to route a tuple to any of its partitions. Always encapsulated
/// in a [`PartitionTupleRouting`].
///
/// The C struct ends with a `int indexes[FLEXIBLE_ARRAY_MEMBER]` tail
/// (`partdesc->nparts` entries); here that is the owned `indexes` `PgVec`.
#[derive(Debug)]
pub struct PartitionDispatchData<'mcx> {
    /// `Relation reldesc` — relation descriptor of the table.
    pub reldesc: Option<Relation<'mcx>>,
    /// `PartitionKey key` — partition key information of the table.
    pub key: Option<mcx::PgBox<'mcx, PartitionKeyData<'mcx>>>,
    /// `List *keystate` — `ExprState`s for the partition-key expressions
    /// (`NIL` until first `FormPartitionKeyDatum`).
    pub keystate: PgVec<'mcx, mcx::PgBox<'mcx, ExprState>>,
    /// `PartitionDesc partdesc` — partition descriptor of the table.
    pub partdesc: Option<mcx::PgBox<'mcx, PartitionDescData<'mcx>>>,
    /// `TupleTableSlot *tupslot` — standalone slot for this table's tupdesc, or
    /// `None` if no tuple conversion from the parent is required.
    pub tupslot: Option<TupleTableSlot>,
    /// `AttrMap *tupmap` — parent→this-table rowtype map, or `None` if no
    /// conversion is required.
    pub tupmap: Option<mcx::PgBox<'mcx, AttrMap<'mcx>>>,
    /// `int indexes[FLEXIBLE_ARRAY_MEMBER]` — per-partition index into the
    /// `PartitionTupleRouting` `partitions` (leaf) or `partition_dispatch_info`
    /// (sub-partitioned) array; -1 if nothing allocated yet.
    pub indexes: PgVec<'mcx, i32>,
}

/// `PartitionDispatch` — owned alias (the C `PartitionDispatchData *`); in the
/// owned model a dispatch is addressed by its index into the routing's
/// `partition_dispatch_info` pool.
pub type PartitionDispatchId = usize;

/// `PartitionTupleRouting` (execPartition.c, private): everything required to
/// route a tuple inserted into a partitioned table to one of its leaf
/// partitions. Allocated in the per-query context (`memcxt`).
#[derive(Debug)]
pub struct PartitionTupleRouting<'mcx> {
    /// `Relation partition_root` — the partitioned table targeted by the
    /// command.
    pub partition_root: Option<Relation<'mcx>>,
    /// `PartitionDispatch *partition_dispatch_info` — one per partitioned table
    /// touched by routing; element 0 is always the target table.
    pub partition_dispatch_info: PgVec<'mcx, mcx::PgBox<'mcx, PartitionDispatchData<'mcx>>>,
    /// `ResultRelInfo **nonleaf_partitions` — fake `ResultRelInfo`s (ids into
    /// the EState pool) for nonleaf partitions, used to check the partition
    /// constraint; a `None` element is the C `NULL` (root level).
    pub nonleaf_partitions: PgVec<'mcx, Option<RriId>>,
    /// `int num_dispatch` — items stored in `partition_dispatch_info`.
    pub num_dispatch: i32,
    /// `int max_dispatch` — allocated size of `partition_dispatch_info`
    /// (tracked for 1:1 mirror of the C grow logic).
    pub max_dispatch: i32,
    /// `ResultRelInfo **partitions` — one per leaf partition touched by
    /// routing (ids into the EState pool); some borrowed from the owning
    /// `ModifyTableState`, the rest built here.
    pub partitions: PgVec<'mcx, RriId>,
    /// `bool *is_borrowed_rel` — parallel to `partitions`: whether the entry is
    /// borrowed from the owning `ModifyTableState` (do not close on cleanup).
    pub is_borrowed_rel: PgVec<'mcx, bool>,
    /// `int num_partitions` — items stored in `partitions`.
    pub num_partitions: i32,
    /// `int max_partitions` — allocated size of `partitions`.
    pub max_partitions: i32,
    /// `MemoryContext memcxt` — context used to allocate subsidiary structs.
    pub memcxt: Opaque,
}

/// Install this unit's seams. execPartition exposes no functions across a
/// dependency cycle (its consumers — nodeModifyTable, copyfrom, nodeAppend —
/// take a direct dependency), so there is nothing to install here yet; the
/// aggregator still calls it for uniformity.
pub fn init_seams() {}

// Re-export the family entry points so consumers depend on the crate root.
pub use pruning::{
    ExecDoInitialPruning, ExecFindMatchingSubPlans, ExecInitPartitionExecPruning,
};
pub use routing_find::ExecFindPartition;
pub use routing_setup::{ExecCleanupTupleRouting, ExecSetupPartitionTupleRouting};

// Anchor a few imports the family modules share so the root stays the single
// definition site; silenced until the body phase consumes them everywhere.
#[allow(dead_code)]
fn _vocab_anchor<'mcx>(
    _mcx: Mcx<'mcx>,
    _a: AttrNumber,
    _e: EcxtId,
    _s: SlotId,
    _r: &ResultRelInfo<'mcx>,
) {
}
