//! Seam declarations for the `backend-executor-execGrouping` unit
//! (`executor/execGrouping.c`): the `TupleHashTable` build / lookup / reset /
//! scan surface that Agg / SetOp / Subplan / Memoize nodes drive.
//!
//! # One canonical API over the real `TupleHashTable`
//!
//! `execGrouping.c`'s routines all operate on a `TupleHashTable` (a
//! `TupleHashTableData *`); `execnodes.h` exposes the full `TupleHashTableData`
//! / `TupleHashEntryData` definitions. By the opacity-inherited rule the
//! crossing type is therefore the real [`TupleHashTable`] /
//! [`TupleHashEntryData`] struct from [`types_nodes::nodeagg`] — never an
//! invented `usize` handle, and never reached into through some owning node.
//! Each seam mirrors its C counterpart's operand list, taking the table as
//! `&mut TupleHashTable` exactly as C takes `TupleHashTable`.
//!
//! Hash entries are handed back through a `&mut dyn FnMut(...)` callback rather
//! than as a borrow, since a seam must never return a `&'static mut`. The
//! callback also surfaces the entry's MAXALIGNed "additional" space
//! (`TupleHashEntryGetAdditional`) as a `&mut [u8]` view — its bytes live in
//! the table's `tablecxt`, owned by execGrouping.
//!
//! Build / lookup / scan allocate or evaluate expressions and so are fallible
//! (`PgResult`, mirroring the C `ereport(ERROR)` surface). The owning unit
//! installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, MemoryContext, PgBox};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::{PlanStateData, SlotId};
use types_nodes::nodeagg::{TupleHashEntryData, TupleHashIterator, TupleHashTable};
use types_nodes::planstate::PlanStateNode;
use types_nodes::{EStateData, TupleSlotKind};

seam_core::seam!(
    /// `TupleHashEntrySize()` (executor.h) — `sizeof(TupleHashEntryData)`, the
    /// fixed per-entry overhead the hash-agg memory estimate adds. Infallible.
    pub fn tuple_hash_entry_size() -> usize
);

seam_core::seam!(
    /// `execTuplesHashPrepare(numCols, eqOperators, &eqFuncOids, &hashFunctions)`
    /// (execGrouping.c): look up the equality-function OIDs and hash `FmgrInfo`s
    /// for the `num_cols` grouping columns described by `eq_operators`. The C
    /// pallocs the two output arrays in `CurrentMemoryContext`; the owned model
    /// returns the two `num_cols`-long vectors (in `mcx`). Fallible (catalog
    /// lookups can `ereport`). Driven by nodeSetOp/nodeAgg's hashed-mode init.
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
    /// `tempcxt`), so creation is fallible on OOM / `ereport`. `mcx` carries
    /// the caller's context for any transient allocation of the seam shim
    /// itself; `parent` is the driving `PlanState` (used only for JIT/expr
    /// compilation decisions, `None` ⇒ the C NULL parent). The `input_ops` /
    /// descriptor describe the input tuples; `key_col_idx`/`eqfuncoids`/
    /// `hashfunctions`/`collations` are the `num_cols`-long key descriptors.
    /// The returned table is owned by the caller (carried in a `Box`).
    ///
    /// `metacxt`/`tablecxt`/`tempcxt` are **borrowed**, not consumed: in C they
    /// are caller-owned `MemoryContext`s the table merely aliases (the table's
    /// bucket array lives in `metacxt`, entries in `tablecxt`, temp work in
    /// `tempcxt`; the caller — e.g. nodeSubplan/nodeAgg — keeps owning and
    /// resetting them). `mcx::MemoryContext` is move-only (it carries the
    /// allocation domain and resets on drop), so they cross the seam by `&`,
    /// matching C's pointer aliasing; the still-unported execGrouping owner
    /// records its own non-owning handle when it lands.
    pub fn build_tuple_hash_table<'mcx>(
        mcx: Mcx<'mcx>,
        parent: Option<&mut PlanStateNode<'mcx>>,
        input_desc: types_tuple::heaptuple::TupleDesc<'mcx>,
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
    ) -> PgResult<Box<TupleHashTable<'mcx>>>
);

seam_core::seam!(
    /// `LookupTupleHashEntry(hashtable, slot, &isnew, &hash)` (execGrouping.c):
    /// find or create the entry for `slot`'s grouping key, always creating a
    /// new entry when none matches. The entry (and its additional bytes) is
    /// delivered to `f`; `isnew` and the computed `hash` are returned.
    /// Allocating a new entry can `ereport` on OOM.
    pub fn lookup_tuple_hash_entry<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
        slot: SlotId,
        estate: &mut EStateData<'mcx>,
        f: &mut dyn FnMut(&mut TupleHashEntryData<'mcx>, &mut [u8]),
    ) -> PgResult<(bool, u32)>
);

seam_core::seam!(
    /// `LookupTupleHashEntryHash(hashtable, slot, isnew, hash)`
    /// (execGrouping.c): like [`lookup_tuple_hash_entry`] but with a
    /// caller-supplied precomputed `hash`. When `create` is false (the C passes
    /// `isnew == NULL`), no new entry is made and a miss yields `false` with
    /// the callback invoked `None`; otherwise an entry is found/created,
    /// delivered to `f`, and `isnew` is returned.
    pub fn lookup_tuple_hash_entry_hash<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
        slot: SlotId,
        hash: u32,
        create: bool,
        estate: &mut EStateData<'mcx>,
        f: &mut dyn FnMut(Option<(&mut TupleHashEntryData<'mcx>, &mut [u8])>),
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `FindTupleHashEntry(hashtable, slot, eqcomp, hashexpr)` (execGrouping.c):
    /// probe (no creation) for an entry matching `slot`, supporting cross-type
    /// comparison via the caller-provided equality (`eqcomp`) and hash
    /// (`hashexpr`) `ExprState`s. Returns whether a match was found. Fallible
    /// (the comparator/hash expr can `ereport`). The two `ExprState`s are the
    /// caller's compiled execExpr states (the driving node's `cur_eq_comp` /
    /// `lhs_hash_expr` fields, lent `&mut` for the duration of the probe).
    pub fn find_tuple_hash_entry<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
        slot: SlotId,
        eqcomp: &mut ExprState<'mcx>,
        hashexpr: &mut ExprState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `TupleHashTableHash(hashtable, slot)` (execGrouping.c): compute the hash
    /// value for the grouping key in `slot`. Fallible (`ereport` from the hash
    /// expr).
    pub fn tuple_hash_table_hash<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
        slot: SlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<u32>
);

seam_core::seam!(
    /// `ResetTupleHashTable(hashtable)` (execGrouping.c): empty the table,
    /// keeping its allocated structure (used when re-using a table across
    /// grouping-set phases / spilled batches). Note the caller must also reset
    /// the table's `tablecxt` to avoid leaks. Fallible.
    pub fn reset_tuple_hash_table<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InitTupleHashIterator(hashtable, &iter)` (execnodes.h macro =
    /// `tuplehash_start_iterate`): start a sequential scan over the table's
    /// entries, returning the initial cursor.
    pub fn init_tuple_hash_iterator<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
    ) -> TupleHashIterator
);

seam_core::seam!(
    /// `entry = ScanTupleHashTable(hashtable, &iter)` (execnodes.h macro =
    /// `tuplehash_iterate`): advance the iterator. When an entry is produced
    /// the owner stores its `firstTuple` into `hashtable->tableslot` (the
    /// `ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), tableslot, false)`
    /// the scan callers do) and delivers the entry (and its additional bytes)
    /// to `f`; the seam returns `true`. `false` means the scan is exhausted.
    /// Fallible (`ereport`).
    pub fn scan_tuple_hash_table<'mcx>(
        hashtable: &mut TupleHashTable<'mcx>,
        iter: &mut TupleHashIterator,
        estate: &mut EStateData<'mcx>,
        f: &mut dyn FnMut(&mut TupleHashEntryData<'mcx>, &mut [u8]),
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `TermTupleHashIterator(&iter)` (execnodes.h macro = no-op): finish a
    /// read/write scan early. Infallible.
    pub fn term_tuple_hash_iterator(iter: &mut TupleHashIterator)
);

seam_core::seam!(
    /// `execTuplesMatchPrepare(desc, numCols, keyColIdx, eqOperators,
    /// collations, parent)` (execGrouping.c): build the `ExprState` that tests
    /// two tuples of the given descriptor for equality on the named key
    /// columns (used for `LIMIT ... WITH TIES` peer detection, `DISTINCT`,
    /// etc.). A zero-column key compiles to `None` (the C `NULL` ExprState).
    /// The compiled state is allocated in the EState's per-query context
    /// (fallible on OOM); preparation can also `ereport(ERROR)`.
    pub fn exec_tuples_match_prepare<'mcx>(
        desc: types_tuple::heaptuple::TupleDesc<'mcx>,
        num_cols: i32,
        key_col_idx: &[AttrNumber],
        eq_operators: &[Oid],
        collations: &[Oid],
        parent: &mut PlanStateData<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<PgBox<'mcx, ExprState<'mcx>>>>
);
