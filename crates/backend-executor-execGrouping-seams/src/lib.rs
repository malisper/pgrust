//! Seam declarations for the `backend-executor-execGrouping` unit
//! (`executor/execGrouping.c`): the `TupleHashTable` build / lookup / reset /
//! scan surface the agg and setop nodes drive.
//!
//! `TupleHashTable` / `TupleHashEntry` cross the seam as the opaque handles
//! from `types_nodes::nodeagg` (the execGrouping owner names the concrete
//! `TupleHashTableData` / `TupleHashEntryData` when it lands). The owning unit
//! installs these from its `init_seams()`; until then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, MemoryContext};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_nodes::nodeagg::{TupleHashEntryHandle, TupleHashIterator, TupleHashTableHandle};
use types_nodes::{PlanStateNode, SlotId, TupleSlotKind};

seam_core::seam!(
    /// `BuildTupleHashTable(...)` (execGrouping.c): construct a hash table for
    /// grouping. C pallocs the table and its bucket array into `metacxt`
    /// (entries land in `tablecxt`, temp work in `tempcxt`), so creation is
    /// fallible on OOM; `mcx` carries the caller's context for any transient
    /// allocation of the seam shim itself. `key_col_idx`/`eqfuncoids`/
    /// `hashfunctions`/`collations` are the `numCols`-long key descriptors.
    pub fn build_tuple_hash_table<'mcx>(
        mcx: Mcx<'mcx>,
        parent: &mut PlanStateNode<'mcx>,
        input_ops: TupleSlotKind,
        num_cols: i32,
        key_col_idx: &[AttrNumber],
        eqfuncoids: &[Oid],
        hashfunctions: &[FmgrInfo],
        collations: &[Oid],
        nbuckets: i64,
        additionalsize: usize,
        metacxt: MemoryContext,
        tablecxt: MemoryContext,
        tempcxt: MemoryContext,
        use_variable_hash_iv: bool,
    ) -> PgResult<TupleHashTableHandle>
);

seam_core::seam!(
    /// `LookupTupleHashEntry(hashtable, slot, isnew, hash)` (execGrouping.c):
    /// find or create the hash entry for `slot`'s grouping key. Returns the
    /// entry, whether it was newly created (`isnew`), and the computed hash
    /// value. Allocating a new entry can ereport on OOM.
    pub fn lookup_tuple_hash_entry(
        hashtable: TupleHashTableHandle,
        slot: SlotId,
    ) -> PgResult<(TupleHashEntryHandle, bool, u32)>
);

seam_core::seam!(
    /// `LookupTupleHashEntryHash(hashtable, slot, isnew, hash)`
    /// (execGrouping.c): like [`lookup_tuple_hash_entry`] but with a
    /// caller-supplied precomputed `hash`. Returns the entry and `isnew`.
    pub fn lookup_tuple_hash_entry_hash(
        hashtable: TupleHashTableHandle,
        slot: SlotId,
        hash: u32,
    ) -> PgResult<(TupleHashEntryHandle, bool)>
);

seam_core::seam!(
    /// `ResetTupleHashTable(hashtable)` (execGrouping.c): empty the hash table
    /// (used when re-using a table across grouping-set phases).
    pub fn reset_tuple_hash_table(hashtable: TupleHashTableHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `InitTupleHashIterator(hashtable, &iter)` (execGrouping.h): start a
    /// sequential scan over the entries, returning the initial cursor.
    pub fn init_tuple_hash_iterator(hashtable: TupleHashTableHandle) -> TupleHashIterator
);

seam_core::seam!(
    /// `ScanTupleHashTable(hashtable, &iter)` (execGrouping.h): return the
    /// next entry under `iter` (advancing it), or `None` at end of scan.
    pub fn scan_tuple_hash_table(
        hashtable: TupleHashTableHandle,
        iter: &mut TupleHashIterator,
    ) -> Option<TupleHashEntryHandle>
);

seam_core::seam!(
    /// `TupleHashEntryGetTuple(entry)` (executor.h inline): the entry's
    /// `firstTuple` (a `MinimalTuple`). Returned as the entry's storing slot
    /// after the owner loads it; here the raw minimal-tuple bytes are surfaced
    /// as an owned image in `mcx`.
    pub fn tuple_hash_entry_get_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        entry: TupleHashEntryHandle,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `TupleHashEntryGetAdditional(hashtable, entry)` (executor.h inline):
    /// the maxaligned, zeroed additional space allocated alongside the entry
    /// (the agg per-group array lives here). The owner returns a mutable view
    /// via a callback shape — seams must not hand out `&'static mut`.
    pub fn tuple_hash_entry_get_additional(
        hashtable: TupleHashTableHandle,
        entry: TupleHashEntryHandle,
        f: &mut dyn FnMut(Option<&mut [u8]>),
    )
);
