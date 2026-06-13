//! Seam declarations for the `backend-executor-execGrouping` unit
//! (`executor/execGrouping.c`): the `TupleHashTable` operations that
//! `nodeSubplan.c` drives over a `SubPlanState`'s `hashtable` / `hashnulls`.
//!
//! The two hash tables and their control memory contexts are owned by
//! execGrouping; here they are reached through the `SubPlanState` whose
//! `hashtable`/`hashnulls`/`hashtablecxt`/`hashtempcxt` opaque slots hold them.
//! Each operation names the table with [`HashTableKind`]. Operations that build
//! or probe allocate in the EState's contexts, so they take `&mut EStateData`
//! and are fallible on OOM / `ereport(ERROR)`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_nodes::execexpr::SubPlanState;
use types_nodes::EStateData;

/// Which of a `SubPlanState`'s two hash tables an operation targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashTableKind {
    /// `node->hashtable` — no-nulls subselect rows.
    Main,
    /// `node->hashnulls` — rows containing null(s).
    Nulls,
}

seam_core::seam!(
    /// Is the named `TupleHashTable` built (non-NULL)? (`node->hashtable != NULL`
    /// / `node->hashnulls != NULL`). Infallible.
    pub fn hash_table_present(node: &SubPlanState<'_>, which: HashTableKind) -> bool
);

seam_core::seam!(
    /// `ResetTupleHashTable(node->hash*)` (execGrouping.c): empty an existing
    /// table, keeping its allocated structure. Fallible (`ereport(ERROR)`).
    pub fn reset_tuple_hash_table<'mcx>(
        node: &mut SubPlanState<'mcx>,
        which: HashTableKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BuildTupleHashTable(node->parent, node->descRight, &TTSOpsVirtual,
    /// node->numCols, node->keyColIdx, node->tab_eq_funcoids,
    /// node->tab_hash_funcs, node->tab_collations, nbuckets, 0,
    /// node->planstate->state->es_query_cxt, node->hashtablecxt,
    /// node->hashtempcxt, false)` (execGrouping.c): construct the named table
    /// with `nbuckets` initial buckets, storing it into `node`. Allocates in
    /// the hashtable context; fallible.
    pub fn build_tuple_hash_table<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        which: HashTableKind,
        nbuckets: i64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `node->hashnulls = NULL` — discard the partly-null table (the
    /// `unknownEqFalse` branch). Infallible.
    pub fn clear_hashnulls(node: &mut SubPlanState<'_>)
);

seam_core::seam!(
    /// `MemoryContextReset(node->hashtablecxt)` (memutils): empty the context
    /// holding the hash tables. Infallible.
    pub fn reset_hashtablecxt(node: &mut SubPlanState<'_>)
);

seam_core::seam!(
    /// `MemoryContextReset(node->hashtempcxt)` (memutils): empty the
    /// hash-tables' temp context after each lookup. Infallible.
    pub fn reset_hashtempcxt(node: &mut SubPlanState<'_>)
);

seam_core::seam!(
    /// `LookupTupleHashEntry(node->hash*, slot, &isnew, NULL)` over the slot
    /// produced by `ExecProject(node->projRight)` (execGrouping.c): insert the
    /// current projected right-hand tuple into the named table, deduplicating.
    /// The slot to insert is the projRight result slot, read off `node` by the
    /// owner. Fallible (allocation / `ereport`).
    pub fn lookup_tuple_hash_entry<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        which: HashTableKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FindTupleHashEntry(node->hashtable, slot, node->cur_eq_comp,
    /// node->lhs_hash_expr) != NULL` (execGrouping.c): probe the main table for
    /// an exact match of the lefthand projection slot. Returns whether a match
    /// was found. Fallible (`ereport` inside the comparator/hash expr).
    pub fn find_tuple_hash_entry_main<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// Create the two hash-table memory contexts and the inner exprcontext
    /// (nodeSubplan.c:897-907): `sstate->hashtablecxt =
    /// AllocSetContextCreate(..., "Subplan HashTable Context", DEFAULT_SIZES)`,
    /// `sstate->hashtempcxt = AllocSetContextCreate(..., "Subplan HashTable Temp
    /// Context", SMALL_SIZES)`, and `sstate->innerecontext =
    /// CreateExprContext(estate)`. Stores them on `node`. Allocating; fallible.
    pub fn create_hash_contexts<'mcx>(
        node: &mut SubPlanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Allocate the `ncols`-sized control arrays on `node`
    /// (`keyColIdx`/`tab_eq_funcoids`/`tab_collations`/`tab_hash_funcs`/
    /// `cur_eq_funcs`, plus the transient `lhs_hash_funcs`/`cross_eq_funcoids`)
    /// and set `node->numCols = ncols` (nodeSubplan.c:942-950). Allocating;
    /// fallible.
    pub fn alloc_hash_control_arrays<'mcx>(
        node: &mut SubPlanState<'mcx>,
        ncols: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `hashtable->numCols` for the named table (execGrouping.c). Infallible.
    pub fn hash_table_num_cols(node: &SubPlanState<'_>, which: HashTableKind) -> i32
);

seam_core::seam!(
    /// `hashtable->keyColIdx[i]` for the named table (execGrouping.c).
    /// Infallible.
    pub fn hash_table_key_col_idx(
        node: &SubPlanState<'_>,
        which: HashTableKind,
        i: i32,
    ) -> types_core::AttrNumber
);

seam_core::seam!(
    /// `hashtable->tab_collations[i]` for the named table (execGrouping.c).
    /// Infallible.
    pub fn hash_table_collation(
        node: &SubPlanState<'_>,
        which: HashTableKind,
        i: i32,
    ) -> types_core::Oid
);

// The opaque iterator cursor lives behind the seam; callers only learn whether
// an entry was returned (and, for the scan step, the table's `tableslot` is
// stored by the owner).
seam_core::seam!(
    /// `InitTupleHashIterator(hashtable, &hashiter)` (execGrouping.c): start a
    /// full-table scan of the named table. The cursor is held inside the owner
    /// keyed by the node; one scan is active per node at a time (matches the C
    /// stack-local `hashiter`). Fallible only structurally (returns `()`).
    pub fn init_tuple_hash_iterator<'mcx>(
        node: &mut SubPlanState<'mcx>,
        which: HashTableKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `entry = ScanTupleHashTable(hashtable, &hashiter)` then
    /// `ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), hashtable->tableslot,
    /// false)` (execGrouping.c): advance the active iterator; if an entry is
    /// produced, store its tuple into the table's `tableslot` and return `true`.
    /// `false` means the scan is exhausted. Fallible (`ereport`).
    pub fn scan_tuple_hash_table<'mcx>(
        node: &mut SubPlanState<'mcx>,
        which: HashTableKind,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `TermTupleHashIterator(&hashiter)` (execGrouping.c): finish the active
    /// scan of the named table early (only needed on the early-return path of
    /// `findPartialMatch`). Infallible.
    pub fn term_tuple_hash_iterator(node: &mut SubPlanState<'_>, which: HashTableKind)
);

/// One read of the named hash table's `tableslot` attribute (`slot_getattr`):
/// its `Datum` plus is-null.
#[derive(Clone, Copy, Debug)]
pub struct SlotAttr {
    pub value: types_datum::Datum,
    pub isnull: bool,
}

seam_core::seam!(
    /// `slot_getattr(hashtable->tableslot, att, &isNull)` for the named table
    /// (`execTuplesUnequal` `slot2`). Fallible.
    pub fn tableslot_getattr<'mcx>(
        node: &mut SubPlanState<'mcx>,
        which: HashTableKind,
        att: types_core::AttrNumber,
    ) -> types_error::PgResult<SlotAttr>
);

seam_core::seam!(
    /// `MemoryContextReset(hashtable->tempcxt)` (`execTuplesUnequal` entry):
    /// reset the named table's temp/eval context so the per-call equality-fn
    /// evaluations have fresh short-term memory. Infallible.
    pub fn reset_hash_eval_context(node: &mut SubPlanState<'_>, which: HashTableKind)
);

seam_core::seam!(
    /// `FunctionCall2Coll(&node->cur_eq_funcs[i], collation, attr1, attr2)`
    /// (`execTuplesUnequal`): apply the cross-type LHS-vs-table equality
    /// function for column `i`, evaluated in the named table's temp context.
    /// Returns the boolean result as a `Datum`. Fallible (`ereport` from the
    /// comparison function).
    pub fn cur_eq_func_call2coll<'mcx>(
        node: &mut SubPlanState<'mcx>,
        which: HashTableKind,
        i: i32,
        collation: types_core::Oid,
        attr1: types_datum::Datum,
        attr2: types_datum::Datum,
    ) -> types_error::PgResult<types_datum::Datum>
);
