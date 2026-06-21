//! Aggregation node vocabulary (`nodes/plannodes.h`, `nodes/primnodes.h`,
//! `nodes/nodes.h`, `executor/nodeAgg.h`, `executor/execnodes.h`), trimmed to
//! the fields the `nodeAgg.c` port consumes.
//!
//! C aliases used cross-subsystem appear as the established handle/id shapes:
//!
//! - `TupleTableSlot *` → [`SlotId`] into the EState slot pool (as in
//!   `execnodes`);
//! - `ExprContext *` → an owned [`ExprContext`] (or, where C aliases one of
//!   the EState-owned contexts, an index into the owning array);
//! - `Tuplesortstate *` (utils/sort/tuplesort.c) → the real
//!   [`crate::nodesort::Tuplesortstate`] (carried in an owning `PgBox`);
//! - `TupleHashTable` / `TupleHashIterator` / `TupleHashEntry`
//!   (executor/execGrouping.c) → the real [`TupleHashTable`] /
//!   [`TupleHashIterator`] / [`TupleHashEntryData`] structs (execGrouping.c
//!   exposes the full `TupleHashTableData`/`TupleHashEntryData` definitions in
//!   execnodes.h, so these are real structs, not opaque handles).
//!
//! These collapse onto the owners' real types when those units land. `None`
//! collections are the C `NIL`/NULL array.

use mcx::{alloc_in, vec_with_capacity_in, Mcx, MemoryContext, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::FormedMinimalTuple;

use crate::bitmapset::Bitmapset;
use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;
use crate::primnodes::{Expr, TargetEntry};

/// `T_Agg` (nodes/nodetags.h, PostgreSQL 18.3 generated order: Group=364,
/// Agg=365, WindowAgg=366).
pub const T_Agg: NodeTag = NodeTag(365);

// ---------------------------------------------------------------------------
// AggStrategy / AggSplit (nodes/nodes.h)
// ---------------------------------------------------------------------------

/// `AggStrategy` (nodes/nodes.h).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum AggStrategy {
    /// Simple agg across all input rows.
    #[default]
    AggPlain = 0,
    /// Grouped agg, input must be sorted.
    AggSorted = 1,
    /// Grouped agg, use internal hashtable.
    AggHashed = 2,
    /// Grouped agg, hash and sort both used.
    AggMixed = 3,
}

pub const AGG_PLAIN: AggStrategy = AggStrategy::AggPlain;
pub const AGG_SORTED: AggStrategy = AggStrategy::AggSorted;
pub const AGG_HASHED: AggStrategy = AggStrategy::AggHashed;
pub const AGG_MIXED: AggStrategy = AggStrategy::AggMixed;

/// `AggSplit` (nodes/nodes.h) — a bitmask of the `AGGSPLITOP_*` bits.
pub type AggSplit = i32;

/// `AGGSPLITOP_COMBINE` — substitute combinefn for transfn.
pub const AGGSPLITOP_COMBINE: i32 = 0x01;
/// `AGGSPLITOP_SKIPFINAL` — skip finalfn, return state as-is.
pub const AGGSPLITOP_SKIPFINAL: i32 = 0x02;
/// `AGGSPLITOP_SERIALIZE` — apply serialfn to output.
pub const AGGSPLITOP_SERIALIZE: i32 = 0x04;
/// `AGGSPLITOP_DESERIALIZE` — apply deserialfn to input.
pub const AGGSPLITOP_DESERIALIZE: i32 = 0x08;

/// `AGGSPLIT_SIMPLE` — basic, non-split aggregation.
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
/// `AGGSPLIT_INITIAL_SERIAL` — initial partial phase, with serialization.
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE;
/// `AGGSPLIT_FINAL_DESERIAL` — final partial phase, with deserialization.
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE;

/// `DO_AGGSPLIT_COMBINE(as)`.
#[inline]
pub fn do_aggsplit_combine(a: AggSplit) -> bool {
    (a & AGGSPLITOP_COMBINE) != 0
}
/// `DO_AGGSPLIT_SKIPFINAL(as)`.
#[inline]
pub fn do_aggsplit_skipfinal(a: AggSplit) -> bool {
    (a & AGGSPLITOP_SKIPFINAL) != 0
}
/// `DO_AGGSPLIT_SERIALIZE(as)`.
#[inline]
pub fn do_aggsplit_serialize(a: AggSplit) -> bool {
    (a & AGGSPLITOP_SERIALIZE) != 0
}
/// `DO_AGGSPLIT_DESERIALIZE(as)`.
#[inline]
pub fn do_aggsplit_deserialize(a: AggSplit) -> bool {
    (a & AGGSPLITOP_DESERIALIZE) != 0
}

// ---------------------------------------------------------------------------
// Aggref (nodes/primnodes.h), Agg / Sort plan nodes (nodes/plannodes.h)
// ---------------------------------------------------------------------------

/// Trimmed `SortGroupClause` (nodes/parsenodes.h) the agg path reads for its
/// ORDER BY / DISTINCT sort keys.
#[derive(Clone, Copy, Debug, Default)]
pub struct SortGroupClauseAgg {
    /// `Index tleSortGroupRef` — reference into the targetlist.
    pub tle_sort_group_ref: Index,
    /// `Oid eqop` — the equality operator (`pg_operator` OID).
    pub eqop: Oid,
    /// `Oid sortop` — the ordering operator (`pg_operator` OID), or 0.
    pub sortop: Oid,
    /// `bool nulls_first`.
    pub nulls_first: bool,
}

/// `Aggref` (nodes/primnodes.h) — an aggregate-function call in an expression
/// tree, trimmed to the fields the agg executor consumes.
#[derive(Debug, Default)]
pub struct Aggref<'mcx> {
    /// `Oid aggfnoid` — pg_proc OID of the aggregate.
    pub aggfnoid: Oid,
    /// `Oid aggtype` — type of the aggregate's result.
    pub aggtype: Oid,
    /// `Oid aggcollid` — OID of collation of result.
    pub aggcollid: Oid,
    /// `Oid inputcollid` — OID of collation that the function should use.
    pub inputcollid: Oid,
    /// `Oid aggtranstype` — type of aggregate's transition (state) data.
    pub aggtranstype: Oid,
    /// `List *aggargtypes` — type Oids of direct and aggregated args.
    pub aggargtypes: Option<PgVec<'mcx, Oid>>,
    /// `List *aggdirectargs` — direct arguments, if an ordered-set agg.
    pub aggdirectargs: Option<PgVec<'mcx, PgBox<'mcx, Expr<'mcx>>>>,
    /// `List *args` — aggregated arguments and sort expressions (TargetEntry).
    pub args: Option<PgVec<'mcx, PgBox<'mcx, TargetEntry<'mcx>>>>,
    /// `List *aggorder` — ORDER BY (list of SortGroupClause).
    pub aggorder: Option<PgVec<'mcx, SortGroupClauseAgg>>,
    /// `List *aggdistinct` — DISTINCT (list of SortGroupClause).
    pub aggdistinct: Option<PgVec<'mcx, SortGroupClauseAgg>>,
    /// `Expr *aggfilter` — FILTER expression, if any.
    pub aggfilter: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `bool aggstar` — true if argument list was really `*`.
    pub aggstar: bool,
    /// `bool aggvariadic` — true if variadic arguments combined into array.
    pub aggvariadic: bool,
    /// `char aggkind` — aggregate kind (see pg_aggregate.h).
    pub aggkind: i8,
    /// `bool aggpresorted` — input already sorted on order/distinct cols.
    pub aggpresorted: bool,
    /// `Index agglevelsup` — > 0 if agg belongs to outer query.
    pub agglevelsup: Index,
    /// `AggSplit aggsplit` — expected agg-splitting mode of parent Agg.
    pub aggsplit: AggSplit,
    /// `int aggno` — unique ID within the Agg node.
    pub aggno: i32,
    /// `int aggtransno` — unique ID of transition state in the Agg.
    pub aggtransno: i32,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: i32,
}

impl<'mcx> Aggref<'mcx> {
    /// Build the executor-side `Aggref<'mcx>` from the lifetime-free
    /// expression-tree [`crate::primnodes::Aggref`] discovered during
    /// `ExecInitExprRec`.
    ///
    /// In C there is a single `Aggref` struct that both the expression tree and
    /// `aggstate->aggs` carry. The repo split it into the lifetime-free
    /// [`crate::primnodes::Aggref`] (in the `Expr` tree) and this executor-side
    /// `nodeagg::Aggref<'mcx>` (whose child lists are arena-allocated
    /// `PgVec`/`PgBox`). This is the faithful `copyObject`-shape bridge between
    /// them: it deep-copies the args / direct-args / filter into `mcx` and maps
    /// the order/distinct SortGroupClauses field-for-field.
    pub fn from_primnode(
        src: &crate::primnodes::Aggref<'mcx>,
        mcx: Mcx<'mcx>,
    ) -> PgResult<Aggref<'mcx>> {
        // List *aggargtypes (Oid list).
        let mut aggargtypes = vec_with_capacity_in(mcx, src.aggargtypes.len())?;
        for t in src.aggargtypes.iter() {
            aggargtypes.push(*t);
        }

        // List *aggdirectargs (plain Exprs).
        let mut aggdirectargs = vec_with_capacity_in(mcx, src.aggdirectargs.len())?;
        for e in src.aggdirectargs.iter() {
            aggdirectargs.push(alloc_in(mcx, e.clone_in(mcx)?)?);
        }

        // List *args (TargetEntry list).
        let mut args = vec_with_capacity_in(mcx, src.args.len())?;
        for tle in src.args.iter() {
            args.push(alloc_in(mcx, tle.clone_in(mcx)?)?);
        }

        // List *aggorder / List *aggdistinct (SortGroupClause lists).
        let map_sgc = |v: &[crate::rawnodes::SortGroupClause]| -> PgResult<PgVec<'mcx, SortGroupClauseAgg>> {
            let mut out = vec_with_capacity_in(mcx, v.len())?;
            for c in v.iter() {
                out.push(SortGroupClauseAgg {
                    tle_sort_group_ref: c.tleSortGroupRef,
                    eqop: c.eqop,
                    sortop: c.sortop,
                    nulls_first: c.nulls_first,
                });
            }
            Ok(out)
        };
        let aggorder = map_sgc(&src.aggorder)?;
        let aggdistinct = map_sgc(&src.aggdistinct)?;

        // Expr *aggfilter.
        let aggfilter = match src.aggfilter.as_ref() {
            Some(f) => Some(alloc_in(mcx, f.clone_in(mcx)?)?),
            None => None,
        };

        Ok(Aggref {
            aggfnoid: src.aggfnoid,
            aggtype: src.aggtype,
            aggcollid: src.aggcollid,
            inputcollid: src.inputcollid,
            aggtranstype: src.aggtranstype,
            aggargtypes: Some(aggargtypes),
            aggdirectargs: Some(aggdirectargs),
            args: Some(args),
            aggorder: Some(aggorder),
            aggdistinct: Some(aggdistinct),
            aggfilter,
            aggstar: src.aggstar,
            aggvariadic: src.aggvariadic,
            aggkind: src.aggkind,
            aggpresorted: src.aggpresorted,
            agglevelsup: src.agglevelsup,
            aggsplit: src.aggsplit,
            aggno: src.aggno,
            aggtransno: src.aggtransno,
            location: src.location,
        })
    }

    /// Deep copy of the executor-side `Aggref` into `mcx` (C: `copyObject`
    /// shape). Used for the `pertrans->aggref` / `peragg->aggref` back-references.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Aggref<'b>> {
        let clone_oids = |v: &Option<PgVec<'_, Oid>>| -> PgResult<Option<PgVec<'b, Oid>>> {
            match v {
                Some(v) => {
                    let mut out = vec_with_capacity_in(mcx, v.len())?;
                    for x in v.iter() {
                        out.push(*x);
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        };
        let clone_exprs =
            |v: &Option<PgVec<'_, PgBox<'_, Expr<'_>>>>| -> PgResult<Option<PgVec<'b, PgBox<'b, Expr<'b>>>>> {
                match v {
                    Some(v) => {
                        let mut out = vec_with_capacity_in(mcx, v.len())?;
                        for e in v.iter() {
                            out.push(alloc_in(mcx, e.clone_in(mcx)?)?);
                        }
                        Ok(Some(out))
                    }
                    None => Ok(None),
                }
            };
        let clone_tles = |v: &Option<PgVec<'_, PgBox<'_, TargetEntry<'_>>>>|
         -> PgResult<Option<PgVec<'b, PgBox<'b, TargetEntry<'b>>>>> {
            match v {
                Some(v) => {
                    let mut out = vec_with_capacity_in(mcx, v.len())?;
                    for tle in v.iter() {
                        out.push(alloc_in(mcx, tle.clone_in(mcx)?)?);
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        };
        let clone_sgc = |v: &Option<PgVec<'_, SortGroupClauseAgg>>|
         -> PgResult<Option<PgVec<'b, SortGroupClauseAgg>>> {
            match v {
                Some(v) => {
                    let mut out = vec_with_capacity_in(mcx, v.len())?;
                    for c in v.iter() {
                        out.push(*c);
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        };
        let aggfilter = match self.aggfilter.as_ref() {
            Some(f) => Some(alloc_in(mcx, f.clone_in(mcx)?)?),
            None => None,
        };
        Ok(Aggref {
            aggfnoid: self.aggfnoid,
            aggtype: self.aggtype,
            aggcollid: self.aggcollid,
            inputcollid: self.inputcollid,
            aggtranstype: self.aggtranstype,
            aggargtypes: clone_oids(&self.aggargtypes)?,
            aggdirectargs: clone_exprs(&self.aggdirectargs)?,
            args: clone_tles(&self.args)?,
            aggorder: clone_sgc(&self.aggorder)?,
            aggdistinct: clone_sgc(&self.aggdistinct)?,
            aggfilter,
            aggstar: self.aggstar,
            aggvariadic: self.aggvariadic,
            aggkind: self.aggkind,
            aggpresorted: self.aggpresorted,
            agglevelsup: self.agglevelsup,
            aggsplit: self.aggsplit,
            aggno: self.aggno,
            aggtransno: self.aggtransno,
            location: self.location,
        })
    }
}

/// `Agg` plan node (nodes/plannodes.h).
#[derive(Debug, Default)]
pub struct Agg<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `AggStrategy aggstrategy` — basic strategy.
    pub aggstrategy: AggStrategy,
    /// `AggSplit aggsplit` — agg-splitting mode.
    pub aggsplit: AggSplit,
    /// `int numCols` — number of grouping columns.
    pub num_cols: i32,
    /// `AttrNumber *grpColIdx` — their indexes in the target list.
    pub grp_col_idx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *grpOperators` — equality operators to compare with.
    pub grp_operators: Option<PgVec<'mcx, Oid>>,
    /// `Oid *grpCollations`.
    pub grp_collations: Option<PgVec<'mcx, Oid>>,
    /// `long numGroups` — estimated number of groups in input.
    pub num_groups: i64,
    /// `uint64 transitionSpace` — estimated size of the transition state.
    pub transition_space: u64,
    /// `Bitmapset *aggParams` — IDs of Params used in Aggref inputs.
    pub agg_params: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *groupingSets` — grouping sets to use (list of integer lists).
    pub grouping_sets: Option<PgVec<'mcx, PgVec<'mcx, i32>>>,
    /// `List *chain` — chained Agg/Sort nodes.
    pub chain: Option<PgVec<'mcx, PgBox<'mcx, Agg<'mcx>>>>,
}

impl Agg<'_> {
    /// Deep copy of the `Agg` plan node (and its `Plan` subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Agg<'b>> {
        let clone_oid_idx = |src: &Option<PgVec<'_, Oid>>| -> PgResult<Option<PgVec<'b, Oid>>> {
            match src {
                Some(v) => {
                    let mut out = vec_with_capacity_in(mcx, v.len())?;
                    for x in v.iter() {
                        out.push(*x);
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        };
        let grp_col_idx = match &self.grp_col_idx {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(*x);
                }
                Some(out)
            }
            None => None,
        };
        let grouping_sets = match &self.grouping_sets {
            Some(sets) => {
                let mut out = vec_with_capacity_in(mcx, sets.len())?;
                for set in sets.iter() {
                    let mut inner = vec_with_capacity_in(mcx, set.len())?;
                    for x in set.iter() {
                        inner.push(*x);
                    }
                    out.push(inner);
                }
                Some(out)
            }
            None => None,
        };
        let chain = match &self.chain {
            Some(list) => {
                let mut out = vec_with_capacity_in(mcx, list.len())?;
                for a in list.iter() {
                    out.push(alloc_in(mcx, a.clone_in(mcx)?)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(Agg {
            plan: self.plan.clone_in(mcx)?,
            aggstrategy: self.aggstrategy,
            aggsplit: self.aggsplit,
            num_cols: self.num_cols,
            grp_col_idx,
            grp_operators: clone_oid_idx(&self.grp_operators)?,
            grp_collations: clone_oid_idx(&self.grp_collations)?,
            num_groups: self.num_groups,
            transition_space: self.transition_space,
            agg_params: match &self.agg_params {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            grouping_sets,
            chain,
        })
    }
}

// ---------------------------------------------------------------------------
// Sibling-subsystem handles (collapse onto real types when owners land)
// ---------------------------------------------------------------------------

// `Tuplesortstate *` (utils/sort/tuplesort.c) is owned by `nodesort`; the agg
// sort fields hold the real [`crate::nodesort::Tuplesortstate`] (carried in an
// owning `PgBox`, the owned-model shape for a `Tuplesortstate *`).
//
// `LogicalTapeSet *` / `LogicalTape *` (utils/sort/logtape.c) are NOT vocabulary
// here: the hash-agg spill state (`AggStateData`, `HashAggSpill`, `HashAggBatch`)
// lives in the `backend-executor-nodeAgg` crate, which holds the real owned
// `LogicalTapeSet` value (from `backend-utils-sort-storage-seams`) directly. No
// handle/registry.

/// `TupleHashEntryData` (executor/execnodes.h) — one hash-table entry.
///
/// The real C struct, faithfully mirrored (opacity-inherited: C exposes the
/// full definition in execnodes.h, so this is a real struct, not a handle).
/// The MAXALIGNed "additional" space C carves out immediately before
/// `firstTuple` (`TupleHashEntryGetAdditional`) is owned by the execGrouping
/// hash table's `tablecxt`; the seam surfaces it as a `&mut [u8]` view rather
/// than embedding it here, matching the C pointer-arithmetic layout.
#[derive(Debug)]
pub struct TupleHashEntryData<'mcx> {
    /// `MinimalTuple firstTuple` — copy of first tuple in this group. Carried as
    /// the payload-bearing [`FormedMinimalTuple`] (header + user-data area);
    /// `None` is the C `NULL` (a freshly-inserted / find-only entry).
    pub firstTuple: Option<FormedMinimalTuple<'mcx>>,
    /// The MAXALIGNed "additional" bytes C carves out immediately before
    /// `firstTuple` in the same `tablecxt` allocation
    /// (`TupleHashEntryGetAdditional`); zeroed on a fresh insert, then read and
    /// written by the driving node (Agg's per-group transition values, SetOp's
    /// per-group counts) which the execGrouping seam lends as a `&mut [u8]`
    /// view. Empty for a `find`-only / not-yet-filled entry.
    pub additional: PgVec<'mcx, u8>,
    /// `uint32 status` — simplehash slot status.
    pub status: u32,
    /// `uint32 hash` — cached hash value.
    pub hash: u32,
}

impl<'mcx> TupleHashEntryData<'mcx> {
    /// A fresh empty bucket: `firstTuple = NULL`, no additional bytes, status
    /// `EMPTY`. (`PgVec` has no allocator-free `Default`, so empty buckets are
    /// built through this `mcx`-bound constructor.)
    pub fn empty(mcx: mcx::Mcx<'mcx>) -> Self {
        Self {
            firstTuple: None,
            additional: PgVec::new_in(mcx),
            status: TUPLEHASH_STATUS_EMPTY,
            hash: 0,
        }
    }
}

/// `tuplehash` simplehash slot status: empty bucket (`SH_STATUS_EMPTY`).
pub const TUPLEHASH_STATUS_EMPTY: u32 = 0x00;
/// `tuplehash` simplehash slot status: occupied bucket (`SH_STATUS_IN_USE`).
pub const TUPLEHASH_STATUS_IN_USE: u32 = 0x01;

/// `tuplehash_hash` (the `lib/simplehash.h` instantiation `execGrouping.c`
/// generates with `SH_PREFIX tuplehash`, also declared in execnodes.h so the
/// types are externally visible) — the open-addressing (Robin Hood) bucket
/// array backing a [`TupleHashTable`].
///
/// Opacity-inherited: `execnodes.h` exposes the generated `tuplehash_hash`
/// struct type, so this is a real struct (not a handle). The bucket array is a
/// `PgVec<TupleHashEntryData>` charged to the table's memory context; the
/// control fields mirror the simplehash header. `ctx`/`private_data` are not
/// carried — the owned execGrouping driver reaches the hash/equality callbacks
/// through its own parameters rather than the C `tb->private_data` aliasing.
#[derive(Debug)]
pub struct TuplehashHash<'mcx> {
    /// `uint64 size` — current number of buckets (a power of two).
    pub size: u64,
    /// `uint32 members` — number of occupied buckets.
    pub members: u32,
    /// `uint32 sizemask` — `size - 1`, for masking hashes to bucket indices.
    pub sizemask: u32,
    /// `uint32 grow_threshold` — `members` at which the table grows.
    pub grow_threshold: u32,
    /// `SH_ELEMENT_TYPE *data` — the bucket array.
    pub data: PgVec<'mcx, TupleHashEntryData<'mcx>>,
}

/// `TupleHashTableData` (executor/execnodes.h) — the all-in-memory tuple hash
/// table that `execGrouping.c` builds and probes for Agg/SetOp/Subplan/etc.
///
/// The real C struct, faithfully mirrored. `TupleHashTable` in C is
/// `TupleHashTableData *`; in the owned model the table is carried by value
/// (in a `Box`/owning field) and threaded through the execGrouping seams as
/// `&mut TupleHashTable`.
#[derive(Debug, Default)]
pub struct TupleHashTable<'mcx> {
    /// `tuplehash_hash *hashtab` — the underlying simplehash bucket array,
    /// built and owned by the execGrouping driver.
    pub hashtab: Option<PgBox<'mcx, TuplehashHash<'mcx>>>,
    /// `int numCols` — number of columns in the lookup key.
    pub numCols: i32,
    /// `AttrNumber *keyColIdx` — attr numbers of key columns.
    pub keyColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `ExprState *tab_hash_expr` — ExprState for hashing table datatype(s).
    pub tab_hash_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *tab_eq_func` — comparator for table datatype(s).
    pub tab_eq_func: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `Oid *tab_collations` — collations for hash and comparison.
    pub tab_collations: Option<PgVec<'mcx, Oid>>,
    /// `MemoryContext tablecxt` — memory context containing the table.
    pub tablecxt: Option<MemoryContext>,
    /// `MemoryContext tempcxt` — context for per-search function evaluations.
    pub tempcxt: Option<MemoryContext>,
    /// `Size additionalsize` — size of the per-entry additional data.
    pub additionalsize: usize,
    /// `TupleTableSlot *tableslot` — slot for referencing table entries (id
    /// into the EState slot pool).
    pub tableslot: Option<SlotId>,
    /// `TupleTableSlot *inputslot` — current input tuple's slot (transient).
    pub inputslot: Option<SlotId>,
    /// `ExprState *in_hash_expr` — ExprState for hashing input datatype(s)
    /// (transient).
    pub in_hash_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *cur_eq_func` — comparator for input vs. table (transient).
    pub cur_eq_func: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprContext *exprcontext` — expression context for the evaluations.
    pub exprcontext: Option<EcxtId>,

    // ------------------------------------------------------------------
    // Deferred-materialization holders (owned-model bridge)
    //
    // C's `BuildTupleHashTable` creates the standalone `tableslot`
    // (`MakeSingleTupleTableSlot`) and `exprcontext`
    // (`CreateStandaloneExprContext`) eagerly, with the parent EState in
    // scope. The execGrouping build seam carries no EState (the table is
    // returned to the caller before any search), so those standalone values
    // are stashed here and registered into the EState's slot/exprcontext
    // pools (yielding `tableslot`/`exprcontext` ids above) on the first
    // search call, which does thread the EState. Once registered the holders
    // are emptied. `inputslot` is allocated entirely lazily the same way.
    /// Standalone `ExprContext` from build, awaiting arena registration.
    pub pending_exprcontext: Option<crate::execnodes::ExprContext<'mcx>>,
    /// Standalone table slot from build, awaiting arena registration.
    pub pending_tableslot: Option<crate::tuptable::SlotData<'mcx>>,
}

/// `TupleHashIterator` (executor/execnodes.h) — iteration cursor over a
/// `TupleHashTable`. The real `tuplehash_iterator` (`lib/simplehash.h`
/// `SH_ITERATOR`) triple: the current/end bucket indices and the done flag.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TupleHashIterator {
    /// `uint32 cur` — current bucket index.
    pub cur: u32,
    /// `uint32 end` — bucket index at which iteration completes.
    pub end: u32,
    /// `bool done` — iteration has wrapped back to `end`.
    pub done: bool,
}


// ---------------------------------------------------------------------------
// HyperLogLog cardinality estimator state (lib/hyperloglog.h)
// ---------------------------------------------------------------------------

/// `hyperLogLogState` (`lib/hyperloglog.h`) — HyperLogLog approximate
/// cardinality estimator state.
///
/// The C struct is
///
/// ```text
/// typedef struct hyperLogLogState
/// {
///     uint8    registerWidth;  /* Register width in bits */
///     Size     nRegisters;     /* Number of registers */
///     double   alphaMM;        /* Gamma times (number of registers) ^ 2 */
///     uint8   *hashesArr;      /* Hashes of every element added */
///     Size     arrSize;        /* Size of hashesArr array */
/// } hyperLogLogState;
/// ```
///
/// This is *pure data* — the register array is an owned [`PgVec<u8>`] charged to
/// a [`mcx::MemoryContext`], the control fields are plain owned values, and
/// there is no raw pointer. The struct lives here in the foundational
/// `types-nodes` crate so that struct holders below the `backend-lib-*` layer
/// (e.g. [`HashAggSpill`]) can store the estimator *by value*, exactly as C
/// holds `hyperLogLogState` inline. The operations
/// (`initHyperLogLog`/`addHyperLogLog`/`estimateHyperLogLog`/`freeHyperLogLog`)
/// live in the higher `backend-lib-hyperloglog` crate and borrow this struct.
///
/// Fields are `pub` because the ops crate above must build and mutate them;
/// they carry the C field names (camelCase) so the 1:1 mapping is obvious.
#[derive(Debug)]
#[allow(non_snake_case)]
pub struct HyperLogLog<'mcx> {
    /// `uint8 registerWidth` — register width, in bits ("k").
    pub registerWidth: u8,
    /// `Size nRegisters` — number of registers.
    pub nRegisters: usize,
    /// `double alphaMM` — alpha * m ^ 2 (see `initHyperLogLog()`).
    pub alphaMM: f64,
    /// `uint8 *hashesArr` — owned register array of hashes (the C `hashesArr`).
    pub hashesArr: PgVec<'mcx, u8>,
    /// `Size arrSize` — size of `hashesArr`.
    pub arrSize: usize,
}

