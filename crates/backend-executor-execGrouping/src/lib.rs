//! `backend-executor-execGrouping` — executor utility routines for grouping,
//! hashing, and aggregation.
//!
//! Idiomatic owned-tree port of `src/backend/executor/execGrouping.c`
//! (PostgreSQL 18.3): `execTuplesMatchPrepare` / `execTuplesHashPrepare`, the
//! all-in-memory `TupleHashTable` build / lookup / find / reset / scan surface,
//! and the `tuplehash` simplehash specialization (see [`tuplehash`]).
//!
//! The `TupleHashTable` / `TupleHashEntryData` / `TuplehashHash` types are the
//! real owned structs in [`types_nodes::nodeagg`] (opacity-inherited: execnodes.h
//! exposes them). The hash/equality dispatch is the caller's compiled execExpr
//! `ExprState`s, evaluated through the execExpr/execTuples seams; the catalog /
//! fmgr lookups go through the lsyscache / fmgr seams.
//!
//! # Deferred slot/exprcontext materialization
//!
//! C's `BuildTupleHashTable` eagerly creates the standalone `tableslot`
//! (`MakeSingleTupleTableSlot`) and `exprcontext`
//! (`CreateStandaloneExprContext`) with the parent EState in scope. The
//! execGrouping build seam carries no EState (the table is handed to the caller
//! before any search), so those standalone values are stashed on the table
//! (`pending_*`) and registered into the EState's pools on the first search
//! call — which does thread the EState. `inputslot` is allocated the same way,
//! lazily, mirroring C where it is just assigned the caller's slot pointer.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]
// PgError is a large soft-error type returned via PgResult throughout the
// executor crates; the large-Err lint is a project-wide non-issue.
#![allow(clippy::result_large_err)]

pub mod tuplehash;

use common_hashfn::murmurhash32;
use mcx::{Mcx, MemoryContext, PgBox};
use tuplehash::{Iter, TuplehashOps};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::{EcxtId, Opaque, SlotId};
use types_nodes::planstate::PlanStateNode;
use types_nodes::nodeagg::{TupleHashEntryData, TupleHashIterator, TupleHashTable, TuplehashHash};
use types_nodes::tuptable::TupleSlotKind;
use types_nodes::EStateData;
use types_tuple::heaptuple::TupleDesc;

use backend_executor_execExpr_seams as execExpr;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_fmgr_fmgr_seams as fmgr;

mod parallel {
    pub use backend_access_transam_parallel::parallel_worker_number;
}
mod nodeHash {
    pub use backend_executor_nodeHash_seams::get_hash_memory_limit;
}

// ===========================================================================
// Iterator conversion
//
// The shared `TupleHashIterator` is the `tuplehash_iterator` triple directly,
// so the internal simplehash `Iter` round-trips field-for-field.
// ===========================================================================

fn pack_iter(it: Iter) -> TupleHashIterator {
    TupleHashIterator {
        cur: it.cur,
        end: it.end,
        done: it.done,
    }
}

fn unpack_iter(it: TupleHashIterator) -> Iter {
    Iter {
        cur: it.cur,
        end: it.end,
        done: it.done,
    }
}

// ===========================================================================
// Live hash/equal adapter (TupleHashTableHash_internal / TupleHashTableMatch)
// ===========================================================================

/// The table fields the simplehash callbacks reach through `tb->private_data`
/// in C. Constructed by destructuring the `TupleHashTable` so the `hashtab`
/// bucket array and these search fields are disjoint borrows. In the
/// all-in-memory path the C `in_hash_expr`/`cur_eq_func` transient fields are
/// just aliases of `tab_hash_expr`/`tab_eq_func`, so the adapter borrows the
/// `tab_*` states directly; `FindTupleHashEntry` would instead lend its
/// caller-supplied cross-type states (see `find_tuple_hash_entry`).
struct GroupingHashOps<'a, 'mcx> {
    /// `in_hash_expr` — ExprState for hashing the input.
    hash_expr: &'a mut ExprState<'mcx>,
    /// `cur_eq_func` — comparator for input vs. table.
    eq_func: &'a mut ExprState<'mcx>,
    /// `inputslot` — current input tuple's slot id.
    inputslot: Option<SlotId>,
    /// `tableslot` — slot for referencing table entries.
    tableslot: SlotId,
    /// `exprcontext` — expression context id for the evaluations.
    exprcontext: EcxtId,
}

impl<'a, 'mcx> GroupingHashOps<'a, 'mcx> {
    /// `TupleHashTableHash_internal(tb, NULL)` — hash the current input tuple
    /// (the `tuple == NULL` branch; the `tuple != NULL` branch never fires
    /// because `SH_STORE_HASH` keeps the hash in the entries).
    fn hash_internal(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        // econtext->ecxt_innertuple = hashtable->inputslot;
        estate.ecxt_mut(self.exprcontext).ecxt_innertuple = self.inputslot;

        // hashkey = DatumGetUInt32(ExecEvalExpr(hashtable->in_hash_expr,
        //                                       econtext, &isnull));
        let (datum, _isnull) = execExpr::exec_eval_expr_switch_context::call(
            self.hash_expr,
            self.exprcontext,
            estate,
        )?;
        let hashkey = datum.as_u32();

        // murmurhash32 the result for good perturbation.
        Ok(murmurhash32(hashkey))
    }

    /// `TupleHashTableMatch(tb, tuple1, tuple2)` == 0 — i.e. whether the entry's
    /// stored tuple and the input slot are NOT DISTINCT. `entry_index` is the
    /// table entry (`tuple1`); the input slot is `tuple2`.
    fn match_equal(
        &mut self,
        tb: &TuplehashHash<'mcx>,
        entry_index: usize,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool> {
        // slot1 = hashtable->tableslot; ExecStoreMinimalTuple(tuple1, slot1, false);
        let slot1 = self.tableslot;
        let mtup = tb.data[entry_index]
            .firstTuple
            .as_ref()
            .expect("match_equal: table entry has a firstTuple")
            .clone_in(estate.es_query_cxt)?;
        execTuples::exec_store_minimal_tuple::call(estate, mtup, slot1, false)?;

        // slot2 = hashtable->inputslot;
        let slot2 = self.inputslot.expect("match_equal: inputslot set");

        // For crosstype comparisons, the inputslot must be first.
        //   econtext->ecxt_innertuple = slot2;
        //   econtext->ecxt_outertuple = slot1;
        //   return !ExecQualAndReset(hashtable->cur_eq_func, econtext);
        {
            let ec = estate.ecxt_mut(self.exprcontext);
            ec.ecxt_innertuple = Some(slot2);
            ec.ecxt_outertuple = Some(slot1);
        }
        // C: return !ExecQualAndReset(...); 0 == match => SH_EQUAL is
        // (match == 0), i.e. "equal" is exactly ExecQualAndReset.
        execExpr::exec_qual_and_reset::call(self.eq_func, self.exprcontext, estate)
    }
}

impl<'a, 'mcx> TuplehashOps<'mcx> for GroupingHashOps<'a, 'mcx> {
    fn hash_key(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        self.hash_internal(estate)
    }

    fn equal(
        &mut self,
        tb: &TuplehashHash<'mcx>,
        a_index: usize,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool> {
        self.match_equal(tb, a_index, estate)
    }
}

// ===========================================================================
// Utility routines for grouping tuples together
// ===========================================================================

/// `execTuplesMatchPrepare(desc, numCols, keyColIdx, eqOperators, collations,
/// parent)` (execGrouping.c) — build the `ExprState` (usable with `ExecQual`)
/// returning whether an ExprContext's inner/outer tuples are NOT DISTINCT.
/// `numCols == 0` returns `None` (the C `NULL`).
pub fn exec_tuples_match_prepare<'mcx>(
    desc: TupleDesc<'mcx>,
    num_cols: i32,
    key_col_idx: &[AttrNumber],
    eq_operators: &[Oid],
    collations: &[Oid],
    _parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>> {
    if num_cols == 0 {
        return Ok(None);
    }

    let mcx = estate.es_query_cxt;
    let descdata = desc.as_deref().expect("exec_tuples_match_prepare: desc");

    // lookup equality functions
    let mut eq_functions: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    eq_functions
        .try_reserve(num_cols as usize)
        .map_err(|_| mcx.oom(num_cols as usize * core::mem::size_of::<Oid>()))?;
    for i in 0..num_cols as usize {
        eq_functions.push(lsyscache::get_opcode::call(eq_operators[i])?);
    }

    // build actual expression: ExecBuildGroupingEqual(desc, desc, NULL, NULL, ...)
    execExpr::exec_build_grouping_equal::call(
        mcx,
        descdata,
        descdata,
        TupleSlotKind::Virtual,
        TupleSlotKind::Virtual,
        num_cols,
        key_col_idx,
        eq_functions.as_slice(),
        collations,
    )
}

/// `execTuplesHashPrepare(numCols, eqOperators, &eqFuncOids, &hashFunctions)`
/// (execGrouping.c) — look up the equality-function OIDs and hash `FmgrInfo`s
/// for the grouping columns. Expects non-cross-type operators.
pub fn exec_tuples_hash_prepare<'mcx>(
    mcx: Mcx<'mcx>,
    num_cols: i32,
    eq_operators: &[Oid],
) -> PgResult<(mcx::PgVec<'mcx, Oid>, mcx::PgVec<'mcx, FmgrInfo>)> {
    let mut eq_func_oids: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    eq_func_oids
        .try_reserve(num_cols as usize)
        .map_err(|_| mcx.oom(num_cols as usize * core::mem::size_of::<Oid>()))?;
    let mut hash_functions: mcx::PgVec<'mcx, FmgrInfo> = mcx::PgVec::new_in(mcx);
    hash_functions
        .try_reserve(num_cols as usize)
        .map_err(|_| mcx.oom(num_cols as usize * core::mem::size_of::<FmgrInfo>()))?;

    for i in 0..num_cols as usize {
        let eq_opr = eq_operators[i];

        let eq_function = lsyscache::get_opcode::call(eq_opr)?;
        let (left_hash_function, right_hash_function) =
            match lsyscache::get_op_hash_functions::call(eq_opr)? {
                Some(funcs) => funcs,
                None => {
                    return Err(PgError::error(alloc::format!(
                        "could not find hash function for hash operator {eq_opr}"
                    )));
                }
            };
        // We're not supporting cross-type cases here.
        debug_assert_eq!(left_hash_function, right_hash_function);
        eq_func_oids.push(eq_function);
        hash_functions.push(fmgr::fmgr_info::call(mcx, right_hash_function)?);
    }

    Ok((eq_func_oids, hash_functions))
}

extern crate alloc;

// ===========================================================================
// Utility routines for all-in-memory hash tables
// ===========================================================================

/// `MAXALIGN(len)` against `MAXIMUM_ALIGNOF` (8 on supported platforms).
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `BuildTupleHashTable(parent, inputDesc, inputOps, numCols, keyColIdx,
/// eqfuncoids, hashfunctions, collations, nbuckets, additionalsize, metacxt,
/// tablecxt, tempcxt, use_variable_hash_iv)` (execGrouping.c) — construct an
/// empty [`TupleHashTable`].
pub fn build_tuple_hash_table<'mcx>(
    mcx: Mcx<'mcx>,
    _parent: Option<&mut PlanStateNode<'mcx>>,
    input_desc: TupleDesc<'mcx>,
    input_ops: TupleSlotKind,
    num_cols: i32,
    key_col_idx: &[AttrNumber],
    eqfuncoids: &[Oid],
    hashfunctions: &[FmgrInfo],
    collations: &[Oid],
    mut nbuckets: i64,
    mut additionalsize: usize,
    metacxt: &MemoryContext,
    tablecxt: &MemoryContext,
    tempcxt: &MemoryContext,
    use_variable_hash_iv: bool,
) -> PgResult<alloc::boxed::Box<TupleHashTable<'mcx>>> {
    let mut hash_iv: u32 = 0;

    debug_assert!(nbuckets > 0);
    additionalsize = maxalign(additionalsize);
    let entrysize = core::mem::size_of::<TupleHashEntryData>() + additionalsize;

    // Limit initial table size request to not more than hash_mem.
    let hash_mem_limit = nodeHash::get_hash_memory_limit::call()? / entrysize as u64;
    if nbuckets as u64 > hash_mem_limit {
        nbuckets = hash_mem_limit as i64;
    }

    // C: MemoryContextSwitchTo(metacxt); the bucket array lives in metacxt. In
    // the owned model the bucket Vec is charged via try_reserve to mcx.
    if use_variable_hash_iv {
        hash_iv = murmurhash32(parallel::parallel_worker_number() as u32);
    }

    let hashtab = tuplehash::create(mcx, nbuckets as u32)?;

    // Copy the input tuple descriptor just for safety (assume all input tuples
    // have equivalent descriptors): the table slot uses TTSOpsMinimalTuple.
    let descdata = input_desc.as_deref().expect("build_tuple_hash_table: inputDesc");
    let desc_copy = mcx::alloc_in(mcx, descdata.clone_in(mcx)?)?;
    let pending_tableslot = execTuples::make_single_tuple_table_slot::call(
        mcx,
        Some(desc_copy),
        TupleSlotKind::MinimalTuple,
    )?;

    // If the caller fails to make metacxt != tablecxt, allowing JIT would let
    // the generated functions outlive the query / be regenerated on reset; the
    // C prevents JIT by passing a NULL parent. JIT is not modeled here, so this
    // decision (and `parent`) is observationally a no-op.
    let _allow_jit = !core::ptr::eq(metacxt as *const _, tablecxt as *const _);

    // build hash ExprState for all columns
    let tab_hash_expr = execExpr::exec_build_hash32_from_attrs::call(
        mcx,
        descdata,
        input_ops,
        hashfunctions,
        collations,
        num_cols,
        key_col_idx,
        hash_iv,
    )?;

    // build comparator for all columns
    let tab_eq_func = execExpr::exec_build_grouping_equal::call(
        mcx,
        descdata,
        descdata,
        input_ops,
        TupleSlotKind::MinimalTuple,
        num_cols,
        key_col_idx,
        eqfuncoids,
        collations,
    )?;

    // CreateStandaloneExprContext(): an ExprContext for the evaluations.
    let pending_exprcontext = execUtils::create_standalone_expr_context::call(mcx)?;

    // keyColIdx / tab_collations live as long as the table; copy into mcx.
    let mut key_col_idx_v: mcx::PgVec<'mcx, AttrNumber> = mcx::PgVec::new_in(mcx);
    key_col_idx_v
        .try_reserve(key_col_idx.len())
        .map_err(|_| mcx.oom(key_col_idx.len() * core::mem::size_of::<AttrNumber>()))?;
    key_col_idx_v.extend_from_slice(key_col_idx);

    let mut collations_v: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    collations_v
        .try_reserve(collations.len())
        .map_err(|_| mcx.oom(collations.len() * core::mem::size_of::<Oid>()))?;
    collations_v.extend_from_slice(collations);

    let table = TupleHashTable {
        hashtab: Some(mcx::alloc_in(mcx, hashtab)?),
        numCols: num_cols,
        keyColIdx: Some(key_col_idx_v),
        tab_hash_expr: Some(tab_hash_expr),
        tab_eq_func,
        tab_collations: Some(collations_v),
        tablecxt: None,
        tempcxt: None,
        additionalsize,
        tableslot: None,
        inputslot: None,
        in_hash_expr: None,
        cur_eq_func: None,
        exprcontext: None,
        pending_exprcontext: Some(pending_exprcontext),
        pending_tableslot: Some(pending_tableslot),
    };
    // `tablecxt`/`tempcxt` are caller-owned contexts the table only aliases;
    // they are not consumed here (the contexts are kept and reset by the
    // driving node), so they are not stored — every allocation goes through
    // `mcx` and the caller resets its contexts directly.
    let _ = (tablecxt, tempcxt);

    Ok(alloc::boxed::Box::new(table))
}

/// Register the table's deferred standalone `exprcontext`/`tableslot` into the
/// EState's pools on first search use, and allocate the `inputslot` lazily.
/// Mirrors the eager creation C does inside `BuildTupleHashTable` with the
/// parent EState available.
fn ensure_materialized<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if hashtable.exprcontext.is_none() {
        let ec = hashtable
            .pending_exprcontext
            .take()
            .expect("ensure_materialized: pending_exprcontext set before first use");
        hashtable.exprcontext = Some(estate.add_expr_context(ec)?);
    }
    if hashtable.tableslot.is_none() {
        let slot = hashtable
            .pending_tableslot
            .take()
            .expect("ensure_materialized: pending_tableslot set before first use");
        hashtable.tableslot = Some(estate.push_slot_data(slot)?);
    }
    Ok(())
}

/// Point `hashtable.inputslot` at the caller's slot. C just assigns the
/// caller's `TupleTableSlot *`; here the input slot id is recorded directly
/// (both refer to the same EState pool slot).
fn set_input_slot(hashtable: &mut TupleHashTable<'_>, slot: SlotId) {
    hashtable.inputslot = Some(slot);
}

/// Destructure the table to obtain disjoint borrows of the `hashtab` bucket
/// array and a [`GroupingHashOps`] adapter over the `tab_*` expr states / slots
/// / exprcontext, then run `body`. The `hashtab` and the search fields are
/// distinct struct fields, so this split borrow is sound (C reaches them all
/// through `tb->private_data`; the owned model lends them disjointly).
fn with_search<'mcx, R>(
    hashtable: &mut TupleHashTable<'mcx>,
    estate: &mut EStateData<'mcx>,
    body: impl FnOnce(
        &mut TuplehashHash<'mcx>,
        &mut GroupingHashOps<'_, 'mcx>,
        &mut EStateData<'mcx>,
    ) -> PgResult<R>,
) -> PgResult<R> {
    let TupleHashTable {
        hashtab,
        tab_hash_expr,
        tab_eq_func,
        inputslot,
        tableslot,
        exprcontext,
        ..
    } = hashtable;
    let hashtab = hashtab.as_mut().expect("with_search: hashtab set");
    let mut ops = GroupingHashOps {
        hash_expr: tab_hash_expr.as_deref_mut().expect("with_search: tab_hash_expr set"),
        eq_func: tab_eq_func.as_deref_mut().expect("with_search: tab_eq_func set"),
        inputslot: *inputslot,
        tableslot: tableslot.expect("with_search: tableslot materialized"),
        exprcontext: exprcontext.expect("with_search: exprcontext materialized"),
    };
    body(hashtab, &mut ops, estate)
}

/// `ResetTupleHashTable(hashtable)` (execGrouping.c) — empty the table,
/// keeping its allocated structure.
pub fn reset_tuple_hash_table<'mcx>(hashtable: &mut TupleHashTable<'mcx>) -> PgResult<()> {
    let hashtab = hashtable
        .hashtab
        .as_mut()
        .expect("reset_tuple_hash_table: hashtab set");
    tuplehash::reset(hashtab);
    Ok(())
}

/// `TupleHashTableHash(hashtable, slot)` (execGrouping.c) — compute the hash
/// value for the grouping key in `slot`.
pub fn tuple_hash_table_hash<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    ensure_materialized(hashtable, estate)?;
    set_input_slot(hashtable, slot);
    // hashtable->in_hash_expr = hashtable->tab_hash_expr; run in tempcxt.
    with_search(hashtable, estate, |_hashtab, ops, estate| ops.hash_internal(estate))
}

/// `LookupTupleHashEntry(hashtable, slot, &isnew, &hash)` (execGrouping.c) —
/// find or create the entry for `slot`'s grouping key (always creating).
pub fn lookup_tuple_hash_entry<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
    f: &mut dyn FnMut(&mut TupleHashEntryData<'mcx>, &mut [u8]),
) -> PgResult<(bool, u32)> {
    ensure_materialized(hashtable, estate)?;
    set_input_slot(hashtable, slot);
    // hashtable->in_hash_expr = tab_hash_expr; cur_eq_func = tab_eq_func (the
    // transient fields alias the tab_* states in the all-in-memory path).

    // local_hash = TupleHashTableHash_internal(hashtable->hashtab, NULL);
    let local_hash = with_search(hashtable, estate, |_hashtab, ops, estate| {
        ops.hash_internal(estate)
    })?;

    let mut isnew = false;
    lookup_internal(hashtable, Some(&mut isnew), local_hash, slot, estate, &mut |e, a| {
        f(e, a)
    })?;
    Ok((isnew, local_hash))
}

/// `LookupTupleHashEntryHash(hashtable, slot, isnew, hash)` (execGrouping.c).
pub fn lookup_tuple_hash_entry_hash<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    slot: SlotId,
    hash: u32,
    create: bool,
    estate: &mut EStateData<'mcx>,
    f: &mut dyn FnMut(Option<(&mut TupleHashEntryData<'mcx>, &mut [u8])>),
) -> PgResult<bool> {
    ensure_materialized(hashtable, estate)?;
    set_input_slot(hashtable, slot);

    let mut isnew = false;
    if create {
        lookup_internal(hashtable, Some(&mut isnew), hash, slot, estate, &mut |e, a| {
            f(Some((e, a)));
        })?;
    } else {
        let hit = lookup_internal(hashtable, None, hash, slot, estate, &mut |e, a| {
            f(Some((e, a)));
        })?;
        if !hit {
            f(None);
        }
    }
    Ok(isnew)
}

/// `FindTupleHashEntry(hashtable, slot, eqcomp, hashexpr)` (execGrouping.c) —
/// probe (no creation) supporting cross-type comparison via caller-provided
/// equality (`eqcomp`) and hash (`hashexpr`) `ExprState`s. Returns whether a
/// match was found.
pub fn find_tuple_hash_entry<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    slot: SlotId,
    eqcomp: &Opaque,
    hashexpr: &Opaque,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    ensure_materialized(hashtable, estate)?;
    set_input_slot(hashtable, slot);

    // C: hashtable->in_hash_expr = hashexpr; hashtable->cur_eq_func = eqcomp;
    // these are the caller's (possibly cross-type) compiled ExprStates, held in
    // the SubPlanState's `lhs_hash_expr` / `cur_eq_comp` fields.
    //
    // Those fields are `Opaque` (`Box<dyn Any>`), which is `'static` and so
    // cannot carry an `ExprState<'mcx>`. The hashed-subplan setup that would
    // populate them (`build_hash_projections_and_exprs`) is itself an
    // unported-owner panic, so this path is unreachable in the current tree;
    // when that owner lands it must re-home these fields onto a real
    // `Option<PgBox<'mcx, ExprState<'mcx>>>` (the same shape as the table's
    // `tab_*` exprs) so the ExprStates can cross. Mirror PG and panic on the
    // genuinely-unmodeled `Opaque` → `ExprState<'mcx>` extraction rather than
    // emit an approximate probe.
    let _ = (eqcomp, hashexpr);
    panic!(
        "FindTupleHashEntry: the caller's cross-type eqcomp/hashexpr cross the \
         seam as `Opaque` (`'static` `Box<dyn Any>`), which cannot carry an \
         `ExprState<'mcx>`; the only driver (nodeSubplan hashed testexpr via \
         build_hash_projections_and_exprs) is itself an unported-owner panic. \
         Re-home SubPlanState.lhs_hash_expr / cur_eq_comp to \
         Option<PgBox<'mcx, ExprState<'mcx>>> when that owner lands."
    );
}

/// `InitTupleHashIterator(hashtable, &iter)` (execnodes.h macro).
pub fn init_tuple_hash_iterator<'mcx>(hashtable: &mut TupleHashTable<'mcx>) -> TupleHashIterator {
    let hashtab = hashtable
        .hashtab
        .as_ref()
        .expect("init_tuple_hash_iterator: hashtab set");
    pack_iter(tuplehash::start_iterate(hashtab))
}

/// `entry = ScanTupleHashTable(hashtable, &iter)` (execnodes.h macro). When an
/// entry is produced its `firstTuple` is stored into `hashtable->tableslot` and
/// the entry (and its additional bytes) is delivered to `f`.
pub fn scan_tuple_hash_table<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    iter: &mut TupleHashIterator,
    estate: &mut EStateData<'mcx>,
    f: &mut dyn FnMut(&mut TupleHashEntryData<'mcx>, &mut [u8]),
) -> PgResult<bool> {
    ensure_materialized(hashtable, estate)?;
    let mut it = unpack_iter(*iter);
    let idx = {
        let hashtab = hashtable
            .hashtab
            .as_ref()
            .expect("scan_tuple_hash_table: hashtab set");
        tuplehash::iterate(hashtab, &mut it)
    };
    *iter = pack_iter(it);

    let idx = match idx {
        Some(i) => i,
        None => return Ok(false),
    };

    // ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), tableslot, false).
    let tableslot = hashtable.tableslot.expect("scan_tuple_hash_table: tableslot");
    {
        let mtup = {
            let hashtab = hashtable.hashtab.as_ref().expect("hashtab set");
            hashtab.data[idx]
                .firstTuple
                .as_ref()
                .map(|ft| ft.clone_in(estate.es_query_cxt))
                .transpose()?
        };
        if let Some(mtup) = mtup {
            execTuples::exec_store_minimal_tuple::call(estate, mtup, tableslot, false)?;
        }
    }

    deliver_entry(hashtable, idx, f);
    Ok(true)
}

/// `TermTupleHashIterator(&iter)` (execnodes.h macro = no-op).
pub fn term_tuple_hash_iterator(_iter: &mut TupleHashIterator) {}

// ===========================================================================
// Internal helpers
// ===========================================================================

/// The allocation context for the table's owned data (recovered from the
/// `hashtab` bucket array's allocator).
fn hashtable_mcx<'mcx>(hashtable: &TupleHashTable<'mcx>) -> Mcx<'mcx> {
    *hashtable
        .hashtab
        .as_ref()
        .expect("hashtable_mcx: hashtab set")
        .data
        .allocator()
}

/// Deliver the entry at `idx` and a `&mut [u8]` view of its additional bytes to
/// `f`, temporarily moving the additional `PgVec` aside so both the entry and
/// the byte slice can be lent at once (the byte slice IS `entry.additional`,
/// which would otherwise alias the `&mut entry`).
fn deliver_entry<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    idx: usize,
    f: &mut dyn FnMut(&mut TupleHashEntryData<'mcx>, &mut [u8]),
) {
    let mcx = hashtable_mcx(hashtable);
    let hashtab = hashtable.hashtab.as_mut().expect("deliver_entry: hashtab set");
    let entry = &mut hashtab.data[idx];
    let mut additional = core::mem::replace(&mut entry.additional, mcx::PgVec::new_in(mcx));
    f(entry, additional.as_mut_slice());
    entry.additional = additional;
}

/// `LookupTupleHashEntry_internal(hashtable, slot, isnew, hash)` — shared
/// lookup/insert body. When `isnew` is `Some`, a new entry is created on a miss
/// (its additional data zeroed); when `None`, no entry is created and a miss
/// returns without invoking `f`. Returns whether an entry was delivered.
fn lookup_internal<'mcx>(
    hashtable: &mut TupleHashTable<'mcx>,
    isnew: Option<&mut bool>,
    hash: u32,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
    f: &mut dyn FnMut(&mut TupleHashEntryData<'mcx>, &mut [u8]),
) -> PgResult<bool> {
    let mcx = hashtable_mcx(hashtable);

    let (index, do_fill) = with_search(hashtable, estate, |hashtab, ops, estate| {
        if isnew.is_some() {
            let (index, found) = tuplehash::insert_hash(mcx, hashtab, ops, hash, estate)?;
            Ok((Some(index), !found))
        } else {
            Ok((tuplehash::lookup_hash(hashtab, ops, hash, estate)?, false))
        }
    })?;

    let index = match index {
        Some(i) => i,
        None => return Ok(false),
    };

    if let Some(isnew) = isnew {
        if do_fill {
            *isnew = true;
            // Copy the first tuple into the table context with additionalsize
            // extra bytes; zero the additional region.
            let addsize = hashtable.additionalsize;
            let first_tuple =
                execTuples::exec_copy_slot_minimal_tuple_extra::call(estate, slot, addsize)?;
            let hashtab = hashtable.hashtab.as_mut().expect("hashtab set");
            let entry = &mut hashtab.data[index];
            entry.firstTuple = Some(first_tuple);
            entry.additional.clear();
            entry
                .additional
                .try_reserve(addsize)
                .map_err(|_| mcx.oom(addsize))?;
            for _ in 0..addsize {
                entry.additional.push(0u8);
            }
        } else {
            *isnew = false;
        }
    }

    deliver_entry(hashtable, index, f);
    Ok(true)
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every `backend-executor-execGrouping-seams` implementation. Called
/// once from `seams-init`.
pub fn init_seams() {
    use backend_executor_execGrouping_seams as s;
    s::build_tuple_hash_table::set(build_tuple_hash_table);
    s::lookup_tuple_hash_entry::set(lookup_tuple_hash_entry);
    s::lookup_tuple_hash_entry_hash::set(lookup_tuple_hash_entry_hash);
    s::find_tuple_hash_entry::set(find_tuple_hash_entry);
    s::tuple_hash_table_hash::set(tuple_hash_table_hash);
    s::reset_tuple_hash_table::set(reset_tuple_hash_table);
    s::init_tuple_hash_iterator::set(init_tuple_hash_iterator);
    s::scan_tuple_hash_table::set(scan_tuple_hash_table);
    s::term_tuple_hash_iterator::set(term_tuple_hash_iterator);
    s::exec_tuples_hash_prepare::set(exec_tuples_hash_prepare);
    s::exec_tuples_match_prepare::set(exec_tuples_match_prepare);
}
