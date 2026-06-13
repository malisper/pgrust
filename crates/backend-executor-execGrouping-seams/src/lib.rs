//! Seam declarations for the `backend-executor-execGrouping` unit
//! (`executor/execGrouping.c`): the tuple-hash-table build/lookup/scan/reset
//! API plus the per-grouping-column hash/equality fmgr precompute
//! (`execTuplesHashPrepare`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The hash table itself
//! ([`types_execgrouping::TupleHashTable`]) and its entry handles
//! ([`types_execgrouping::TupleHashEntry`]) are owner-opaque; the per-group
//! `additional` payload — for a SetOp node a [`SetOpStatePerGroupData`] — is
//! read/written through the typed get/set seams below.

#![allow(non_snake_case)]

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_execgrouping::{TupleHashEntry, TupleHashIterator, TupleHashTable};
use types_nodes::execnodes::{EStateData, PlanStateData};
use types_nodes::nodesetop::SetOpStatePerGroupData;
use types_nodes::TupleSlotKind;
use types_nodes::TupleTableSlot;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `execTuplesHashPrepare(numCols, eqOperators, &eqFuncOids,
    /// &hashFunctions)` (execGrouping.c): look up the equality function OIDs
    /// and hash `FmgrInfo`s for the grouping columns. Returns the two
    /// `numCols`-long arrays (C's two out-params, `palloc`'d in the current
    /// context), allocated in `mcx`; fallible on OOM / catalog
    /// `ereport(ERROR)`.
    pub fn exec_tuples_hash_prepare<'mcx>(
        mcx: Mcx<'mcx>,
        num_cols: i32,
        eq_operators: &[Oid],
    ) -> PgResult<(PgVec<'mcx, Oid>, PgVec<'mcx, FmgrInfo>)>
);

seam_core::seam!(
    /// `BuildTupleHashTable(parent, inputDesc, inputOps, numCols, keyColIdx,
    /// eqfuncoids, hashfunctions, collations, nbuckets, additionalsize,
    /// metacxt, tablecxt, tempcxt, use_variable_hash_iv)` (execGrouping.c):
    /// build a tuple hash table with one entry per group, returning the
    /// handle. The set/agg caller fixes `additionalsize =
    /// sizeof(SetOpStatePerGroupData)`. The table is allocated in `metacxt`
    /// (the per-query context) with entries in `tablecxt`; fallible on OOM.
    pub fn build_tuple_hash_table<'mcx>(
        parent: &PlanStateData<'mcx>,
        input_desc: &TupleDescData<'mcx>,
        input_ops: Option<TupleSlotKind>,
        num_cols: i32,
        key_col_idx: &[AttrNumber],
        eqfuncoids: &[Oid],
        hashfunctions: &[FmgrInfo],
        collations: &[Oid],
        nbuckets: i64,
        additionalsize: usize,
        metacxt: Mcx<'mcx>,
        tablecxt: &MemoryContext,
        tempcxt: &MemoryContext,
        use_variable_hash_iv: bool,
    ) -> PgResult<TupleHashTable>
);

seam_core::seam!(
    /// `LookupTupleHashEntry(hashtable, slot, &isnew, NULL)` (execGrouping.c):
    /// find or (when `make_new` is true) create the hash entry for `slot`'s
    /// group. Returns `Some((entry, isnew))` — `isnew` true if the entry was
    /// just created — or `None` when `make_new` is false and no entry exists
    /// (the C `LookupTupleHashEntry(..., NULL, NULL)` returning `NULL`).
    /// Hashing runs the eq/hash fns (may `ereport(ERROR)`) and may allocate.
    pub fn lookup_tuple_hash_entry(
        hashtable: &mut TupleHashTable,
        slot: &TupleTableSlot,
        make_new: bool,
    ) -> PgResult<Option<(TupleHashEntry, bool)>>
);

seam_core::seam!(
    /// `TupleHashEntryGetAdditional(hashtable, entry)` (executor.h) read: the
    /// per-group `additional` payload for a SetOp node ([`SetOpStatePerGroupData`]).
    pub fn tuple_hash_entry_get_additional(
        hashtable: &TupleHashTable,
        entry: TupleHashEntry,
    ) -> PgResult<SetOpStatePerGroupData>
);

seam_core::seam!(
    /// `*TupleHashEntryGetAdditional(hashtable, entry) = pergroup` — write the
    /// per-group `additional` payload (the C stores into the in-place struct;
    /// the owned model writes the value back through the owner).
    pub fn tuple_hash_entry_set_additional(
        hashtable: &mut TupleHashTable,
        entry: TupleHashEntry,
        pergroup: SetOpStatePerGroupData,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), slot, false)`
    /// (executor.h/execTuples.c): store the entry's stored minimal tuple into
    /// `slot` without freeing it. Fallible on OOM.
    pub fn store_hash_entry_tuple<'mcx>(
        hashtable: &TupleHashTable,
        entry: TupleHashEntry,
        estate: &mut EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ResetTupleHashIterator(hashtable, iter)` (execnodes.h): freeze the
    /// table for iteration and reset `iter` to the start.
    pub fn reset_tuple_hash_iterator(
        hashtable: &mut TupleHashTable,
        iter: &mut TupleHashIterator,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ScanTupleHashTable(hashtable, iter)` (execnodes.h): return the next
    /// entry in iteration order, or `None` at the end (the C `NULL`).
    pub fn scan_tuple_hash_table(
        hashtable: &mut TupleHashTable,
        iter: &mut TupleHashIterator,
    ) -> PgResult<Option<TupleHashEntry>>
);

seam_core::seam!(
    /// `ResetTupleHashTable(hashtable)` (execGrouping.c): empty the table,
    /// keeping it usable.
    pub fn reset_tuple_hash_table(hashtable: &mut TupleHashTable) -> PgResult<()>
);
