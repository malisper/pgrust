//! Seam declarations for the `backend-executor-execGrouping` unit
//! (`executor/execGrouping.c`): the `TupleHashTable` build / prepare / lookup /
//! reset / scan surface that Agg / SetOp / Subplan / Memoize nodes drive.
//!
//! `execGrouping.c` is not yet ported; until it lands these calls panic loudly
//! (the `seam_core::seam!` default). The crossing type is the real
//! [`types_nodes::TupleHashTable`] / [`types_nodes::TupleHashEntryData`] struct
//! (`execnodes.h` exposes them publicly) per the opacity-inherited rule — never
//! an invented handle. Each seam mirrors its C counterpart's operand list,
//! taking the table as `&mut TupleHashTable` exactly as C takes
//! `TupleHashTable`.
//!
//! A hash entry is handed back through a `&mut dyn FnMut(...)` callback rather
//! than as a borrow, since a seam must never return a `&'static mut`. The
//! callback also surfaces the entry's MAXALIGNed "additional" space
//! (`TupleHashEntryGetAdditional`) as a `&mut [u8]` view — its bytes live in the
//! table's `tablecxt`, owned by execGrouping. The driving node (SetOp/Agg)
//! reads/writes its per-group struct directly in those bytes.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use mcx::{Mcx, MemoryContext};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_nodes::nodesetop::{TupleHashEntryData, TupleHashIterator, TupleHashTable};
use types_nodes::{EStateData, PlanStateNode, SlotId, TupleSlotKind};
use types_tuple::heaptuple::TupleDesc;

seam_core::seam!(
    /// `execTuplesHashPrepare(numCols, eqOperators, &eqFuncOids, &hashFunctions)`
    /// (execGrouping.c): look up the equality-function OIDs and hash `FmgrInfo`s
    /// for the `num_cols` grouping columns described by `eq_operators`. The C
    /// pallocs the two output arrays in `CurrentMemoryContext`; the owned model
    /// returns the two `num_cols`-long vectors (in `mcx`). Fallible (catalog
    /// lookups can `ereport`).
    pub fn exec_tuples_hash_prepare<'mcx>(
        mcx: Mcx<'mcx>,
        num_cols: i32,
        eq_operators: &[Oid],
    ) -> PgResult<(mcx::PgVec<'mcx, Oid>, mcx::PgVec<'mcx, FmgrInfo>)>
);

seam_core::seam!(
    /// `BuildTupleHashTable(parent, inputDesc, inputOps, numCols, keyColIdx,
    /// eqfuncoids, hashfunctions, collations, nbuckets, additionalsize,
    /// metacxt, tablecxt, tempcxt, use_variable_hash_iv)` (execGrouping.c):
    /// construct an empty grouping hash table. C pallocs the table and its
    /// bucket array into `metacxt` (entries land in `tablecxt`, temp work in
    /// `tempcxt`), so creation is fallible on OOM / `ereport`. `mcx` carries the
    /// caller's context for the table's owned spine; `parent` is the driving
    /// `PlanState` (used only for JIT/expr-compilation decisions). The returned
    /// table is owned by the caller (carried in a `Box`).
    ///
    /// `metacxt`/`tablecxt`/`tempcxt` are borrowed, not consumed: in C they are
    /// caller-owned `MemoryContext`s the table merely aliases (the caller — e.g.
    /// SetOp/Agg — keeps owning and resetting them).
    pub fn build_tuple_hash_table<'mcx>(
        mcx: Mcx<'mcx>,
        parent: Option<&mut PlanStateNode<'mcx>>,
        input_desc: TupleDesc<'mcx>,
        input_ops: TupleSlotKind,
        num_cols: i32,
        key_col_idx: &[AttrNumber],
        eqfuncoids: &[Oid],
        hashfunctions: &[FmgrInfo],
        collations: &[Oid],
        nbuckets: i64,
        additionalsize: usize,
        metacxt: &MemoryContext,
        tablecxt: &MemoryContext,
        tempcxt: &MemoryContext,
        use_variable_hash_iv: bool,
    ) -> PgResult<alloc::boxed::Box<TupleHashTable<'mcx>>>
);

seam_core::seam!(
    /// `LookupTupleHashEntry(hashtable, slot, &isnew, NULL)` (execGrouping.c):
    /// find or, when `create` is true, build the entry for `slot`'s grouping
    /// key. When found/created the entry (its `isnew` flag and its additional
    /// bytes) is delivered to `f` and `Some(isnew)` is returned; when `create`
    /// is false and no entry matches, `f` is not called and `None` is returned
    /// (the C `entry == NULL` miss). The `isnew` flag is passed to `f` so the
    /// caller can initialize a fresh entry's additional space (the C caller's
    /// `if (isnew) { ... }`, exactly where nodeSetOp zeroes the per-group
    /// counts). Allocating a new entry can `ereport` on OOM.
    pub fn lookup_tuple_hash_entry<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
        slot: SlotId,
        create: bool,
        estate: &mut EStateData<'mcx>,
        f: &mut dyn FnMut(bool, &mut TupleHashEntryData<'mcx>, &mut [u8]),
    ) -> PgResult<Option<bool>>
);

seam_core::seam!(
    /// `ResetTupleHashTable(hashtable)` (execGrouping.c): empty the table,
    /// keeping its allocated structure (used by `ExecReScanSetOp` when re-using
    /// a table). The caller also resets the table's `tablecxt`. Fallible.
    pub fn reset_tuple_hash_table<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ResetTupleHashIterator(hashtable, &iter)` / `InitTupleHashIterator`
    /// (execnodes.h macro = `tuplehash_start_iterate`): (re)start a sequential
    /// scan over the table's entries, returning the initial cursor.
    pub fn reset_tuple_hash_iterator<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
    ) -> TupleHashIterator
);

seam_core::seam!(
    /// `entry = ScanTupleHashTable(hashtable, &iter)` (execnodes.h macro =
    /// `tuplehash_iterate`): advance the iterator. When an entry is produced it
    /// is delivered (with its additional bytes) to `f` and `true` is returned;
    /// `false` means the scan is exhausted. The owner stores the entry's
    /// `firstTuple` into the result slot itself (`ExecStoreMinimalTuple` of
    /// `TupleHashEntryGetTuple(entry)`), reading the tuple via the entry handed
    /// to `f`. Fallible (`ereport`).
    pub fn scan_tuple_hash_table<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
        iter: &mut TupleHashIterator,
        estate: &mut EStateData<'mcx>,
        f: &mut dyn FnMut(&mut TupleHashEntryData<'mcx>, &mut [u8]),
    ) -> PgResult<bool>
);
