//! Seam declarations for the `backend-executor-execTuples` unit
//! (`executor/execTuples.c`): slot creation and the slot-ops virtual calls
//! (`tuptable.h`'s `ExecClearTuple` / `ExecCopySlot` dispatch through the
//! `TupleTableSlotOps` tables that execTuples.c owns).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecInitResultTypeTL(planstate)` (execTuples.c): build the node's
    /// result tuple descriptor from its plan's target list
    /// (`ExecTypeFromTL`) and store it in `planstate.ps_ResultTupleDesc`.
    /// Allocates the descriptor in the per-query context (fallible on OOM).
    pub fn exec_init_result_type_tl<'mcx>(
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `scanstate->ss_ScanTupleSlot->tts_tupleDescriptor` (tuptable.h): the
    /// scan slot's tuple descriptor, copied into `mcx` (C reads the shared
    /// pointer). The slot payload model is not yet landed, so the owning unit
    /// installs this when the slot carries a descriptor.
    ///
    /// PROVISIONAL: re-sign when the slot payload model lands (the descriptor
    /// will then be lent, not copied).
    pub fn exec_scan_slot_descriptor<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        scanstate: &types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `slot_getallattrs(slot)` (tuptable.h, via execTuples.c's
    /// `slot_getsomeattrs`) plus the subsequent `slot->tts_values[i]` /
    /// `slot->tts_isnull[i]` reads: fully deconstruct the slot and return its
    /// per-attribute `(value, isnull)` arrays, copied into `mcx` (in C the
    /// arrays live in the slot itself). Deforming can detoast/allocate, so
    /// the call is fallible.
    ///
    /// PROVISIONAL: `TupleTableSlot` is currently trimmed to its header bits
    /// (no descriptor/values payload), so this contract cannot yet be
    /// implemented as promised. It must be re-signed when the slot payload
    /// model lands (same caveat as `exec_copy_slot`).
    pub fn slot_getallattrs<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: &types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<
        mcx::PgVec<'mcx, types_tuple::backend_access_common_heaptuple::DeformedColumn<'mcx>>,
    >
);

seam_core::seam!(
    /// `ExecInitResultTupleSlotTL(planstate, tts_ops)` (execTuples.c):
    /// initialize the node's result tuple type (from the plan's targetlist)
    /// and create its result slot in the `EState` slot pool, storing the id in
    /// `planstate.ps_ResultTupleSlot`. The slot is allocated in the pool's
    /// context (C: `MakeTupleTableSlot` pallocs in the query context), so the
    /// call is fallible on OOM.
    pub fn exec_init_result_tuple_slot_tl<'mcx>(
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(slot)` (tuptable.h): clear the slot's contents
    /// (`slot->tts_ops->clear`).
    pub fn exec_clear_tuple(slot: &mut types_nodes::TupleTableSlot) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `slot_getsomeattrs(slot, attnum)` then `(slot->tts_values[attnum-1],
    /// slot->tts_isnull[attnum-1])` (tuptable.h, via execTuples.c): ensure the
    /// first `attnum` columns are extracted and return the `(value, isnull)` of
    /// the 1-based `attnum`th. nodeSort's Datum sort reads attribute 1.
    /// Deforming can detoast/allocate, so the call is fallible.
    pub fn slot_getsomeattr(
        slot: &mut types_nodes::TupleTableSlot,
        attnum: i32,
    ) -> types_error::PgResult<(types_datum::Datum, bool)>
);

seam_core::seam!(
    /// `ExecClearTuple(slot); slot->tts_values[0] = val; slot->tts_isnull[0] =
    /// isnull; ExecStoreVirtualTuple(slot)` (tuptable.h, via execTuples.c):
    /// store a single-Datum virtual tuple in the (single-column) result slot.
    /// nodeSort's Datum-sort output path. Storing can allocate, fallible.
    pub fn exec_store_first_datum<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        val: types_datum::Datum,
        is_null: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCopySlot(dstslot, srcslot)` (tuptable.h): copy the source slot's
    /// tuple into the destination slot (`dstslot->tts_ops->copyslot`). The
    /// copy allocates in `mcx`, the destination slot's memory context (C:
    /// `slot->tts_mcxt`; the trimmed slot carries no payload yet, so the
    /// owned model passes the context explicitly). Fallible on OOM.
    ///
    /// PROVISIONAL: `TupleTableSlot` is currently trimmed to its header bits
    /// (no descriptor/values payload), so this contract cannot yet be
    /// implemented as promised. It must be re-signed when the first real
    /// tuple flow (the slot payload model) lands.
    pub fn exec_copy_slot<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        dstslot: &mut types_nodes::TupleTableSlot,
        srcslot: &types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecInitResultSlot(planstate, tts_ops)` (execTuples.c): create the
    /// node's result slot (from its already-set `ps_ResultTupleDesc`) in the
    /// EState slot pool, storing the id in `planstate.ps_ResultTupleSlot`.
    /// Fallible on OOM.
    pub fn exec_init_result_slot<'mcx>(
        planstate: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecInitScanTupleSlot(estate, scanstate, tupledesc, tts_ops)`
    /// (execTuples.c): create the scan slot in the EState slot pool, storing
    /// the id in `scanstate.ss_ScanTupleSlot` and the scan-ops fields in the
    /// node. The descriptor moves into the slot (the owned model passes an
    /// already-`'mcx` copy where C shares the pointer). Fallible on OOM.
    pub fn exec_init_scan_tuple_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        scanstate: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        tupledesc: types_tuple::heaptuple::TupleDesc<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecInitExtraTupleSlot(estate, tupledesc, tts_ops)` (execTuples.c):
    /// create a slot outside the standard per-node slots (trigger OLD/NEW,
    /// RETURNING, ...), returning its pool id (the C pointer). Fallible on
    /// OOM.
    pub fn exec_init_extra_tuple_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        tupledesc: types_tuple::heaptuple::TupleDesc<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `ExecSetSlotDescriptor(slot, tupdesc)` (execTuples.c): set (or reset)
    /// the slot's row descriptor, releasing any tuple stored in it. The slot
    /// is addressed by pool id; the descriptor moves in. Fallible on OOM
    /// (the C pallocs the per-column value arrays).
    pub fn exec_set_slot_descriptor<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        tupdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecStoreAllNullTuple(slot)` (tuptable.h/execTuples.c): store an
    /// all-NULL virtual tuple in the slot. Fallible on OOM.
    pub fn exec_store_all_null_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MakeSingleTupleTableSlot(tupdesc, tts_ops)` (execTuples.c): create a
    /// standalone slot (not tied to a tuple table) of the given slot class,
    /// fixed to `tupdesc`. The slot is allocated in `mcx` (C: palloc in
    /// `CurrentMemoryContext`), so the call is fallible on OOM.
    pub fn make_single_tuple_table_slot<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tupdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<types_nodes::TupleTableSlot>
);

seam_core::seam!(
    /// `ExecDropSingleTupleTableSlot(slot)` (execTuples.c): release a slot
    /// made with `MakeSingleTupleTableSlot`. Clearing the slot can release a
    /// buffer pin, whose bookkeeping can `elog(ERROR)`, carried on `Err`.
    pub fn exec_drop_single_tuple_table_slot(
        slot: types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `slot_getsysattr(slot, attnum, &isnull)` (tuptable.h/execTuples.c):
    /// fetch a system attribute of the slot's current tuple as
    /// `(datum, isnull)` (`slot->tts_ops->getsysattr` dispatch). A slot class
    /// without system attributes (e.g. virtual) is the C `elog(ERROR)`,
    /// carried on `Err`.
    pub fn slot_getsysattr(
        slot: &types_nodes::TupleTableSlot,
        attnum: types_core::AttrNumber,
    ) -> types_error::PgResult<(types_datum::Datum, bool)>
);

seam_core::seam!(
    /// `slot_getattr(slot, attnum, &isnull)` (tuptable.h): fetch a regular
    /// (positive) attribute `attnum` (1-based) of the slot's current tuple as
    /// `(datum, isnull)`, deforming up to `attnum` first via
    /// `slot_getsomeattrs`/`slot_getsomeattrs_int` (`slot->tts_ops->getsomeattrs`
    /// dispatch). The slot is borrowed mutably because deforming populates the
    /// slot's `tts_values`/`tts_isnull`/`tts_nvalid`; deforming can
    /// detoast/allocate, so the call is fallible. System (non-positive) attnums
    /// take the `slot_getsysattr` path instead and are never passed here.
    pub fn slot_getattr(
        slot: &mut types_nodes::TupleTableSlot,
        attnum: types_core::AttrNumber,
    ) -> types_error::PgResult<(types_datum::Datum, bool)>
);

seam_core::seam!(
    /// `ExecInitNullTupleSlot(estate, tupledesc, tts_ops)` (execTuples.c):
    /// create a slot in the EState slot pool and store an all-NULL virtual
    /// tuple of the given descriptor in it (the null-padding slot for outer
    /// joins), returning its pool id. The descriptor is moved in (the owned
    /// model passes an already-`'mcx` copy where C shares the child's
    /// `TupleDesc *`). The slot allocates in the pool's context, fallible on OOM.
    pub fn exec_init_null_tuple_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        tupledesc: types_tuple::heaptuple::TupleDesc<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

/// One read of a slot/tuple attribute: its `Datum` plus is-null.
#[derive(Clone, Copy, Debug)]
pub struct SlotAttr {
    pub value: types_datum::Datum,
    pub isnull: bool,
}

seam_core::seam!(
    /// `slot_getattr(slot, attnum, &isnull)` (tuptable.h): fetch a user
    /// attribute of the slot's current tuple as `(datum, isnull)`. The slot is
    /// addressed by its pool id; reads it out of the EState slot pool.
    /// Deforming can detoast/allocate, so fallible. (The `&mut TupleTableSlot`
    /// form above is the same C op reached through a borrowed slot; this
    /// pool-id form is the one the owned-EState executor nodes use.)
    pub fn slot_getattr_by_id<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        attnum: types_core::AttrNumber,
    ) -> types_error::PgResult<SlotAttr>
);

seam_core::seam!(
    /// `slot->tts_tupleDescriptor->natts` of the slot addressed by its pool id
    /// (`slotAllNulls`/`slotNoNulls` over a scan slot; the EXPR/MULTIEXPR
    /// per-column loops bound). Infallible.
    pub fn slot_natts(estate: &types_nodes::EStateData<'_>, slot: types_nodes::SlotId) -> i32
);

seam_core::seam!(
    /// `node->curTuple = ExecCopySlotHeapTuple(slot)` after
    /// `if (node->curTuple) heap_freetuple(node->curTuple)` (nodeSubplan.c):
    /// copy the slot's current tuple into the node's `curTuple` (freeing any
    /// previous copy). The copy is allocated in the per-query context; fallible
    /// on OOM.
    pub fn replace_cur_tuple_from_slot<'mcx>(
        node: &mut types_nodes::execexpr::SubPlanState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `heap_getattr(node->curTuple, attnum, tdesc, &isnull)` (htup_details.h)
    /// where `tdesc` is the producing slot's descriptor (nodeSubplan.c): read
    /// column `attnum` of the node's `curTuple`. The descriptor is the slot the
    /// tuple was copied from, addressed by its pool id. Fallible (detoast).
    pub fn cur_tuple_getattr<'mcx>(
        node: &types_nodes::execexpr::SubPlanState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        attnum: types_core::AttrNumber,
    ) -> types_error::PgResult<SlotAttr>
);

seam_core::seam!(
    /// `ExecGetResultType(planstate)` (execUtils.c/executor.h): the node's
    /// result tuple descriptor (`planstate->ps_ResultTupleSlot`'s descriptor),
    /// returned as a shared borrow of the planstate. `None` is the C `NULL`
    /// (descriptor not yet set).
    pub fn exec_get_result_type<'a, 'mcx>(
        planstate: &'a types_nodes::execnodes::PlanStateData<'mcx>,
    ) -> Option<&'a types_tuple::heaptuple::TupleDescData<'mcx>>
);

seam_core::seam!(
    /// `ExecGetResultSlotOps(planstate, &isfixed)` (execUtils.c): the slot-ops
    /// class of the node's result slot (`planstate->resultops`). Returns the
    /// owned `TupleSlotKind` token; the `isfixed` out-flag is not consumed by
    /// the merge-join caller (it passes `NULL`).
    pub fn exec_get_result_slot_ops<'mcx>(
        planstate: &types_nodes::execnodes::PlanStateData<'mcx>,
    ) -> types_nodes::TupleSlotKind
);

seam_core::seam!(
    /// `ExecTypeFromTL(targetList)` (execTuples.c): build a tuple descriptor
    /// from a target list (the planner's `indextlist` for an index-only scan),
    /// allocated in `mcx`. Fallible on OOM.
    pub fn exec_type_from_tl<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        target_list: &[types_nodes::primnodes::TargetEntry<'mcx>],
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `ExecAllocTableSlot(tupleTable, desc, tts_ops)` (execTuples.c): allocate
    /// a slot in the EState slot pool with the given descriptor and slot class,
    /// returning its pool id. The descriptor is the relation's
    /// `RelationGetDescr` copy; the slot class is `table_slot_callbacks`'s
    /// result. Fallible on OOM.
    pub fn exec_alloc_table_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        desc: types_tuple::heaptuple::TupleDesc<'mcx>,
        tts_ops: types_nodes::TupleSlotKind,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `ExecForceStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c):
    /// store a heap tuple in the slot regardless of the slot's native format
    /// (materializing it if the slot is not a heap-tuple slot). Targets the
    /// slot by pool id. Fallible on OOM.
    pub fn exec_force_store_heap_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        tuple: &types_tuple::heaptuple::HeapTupleData<'mcx>,
        should_free: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecForceStoreMinimalTuple(mtup, slot, shouldFree)` (execTuples.c):
    /// store a `MinimalTuple` into the slot (forcing it through the slot's ops),
    /// taking ownership when `should_free`. Fallible on OOM.
    pub fn exec_force_store_minimal_tuple<'mcx>(
        slot: types_nodes::SlotId,
        mtup: mcx::PgBox<'mcx, types_tuple::heaptuple::MinimalTupleData<'mcx>>,
        should_free: bool,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecStoreVirtualTuple(slot)` (tuptable.h/execTuples.c): mark the slot
    /// as holding a valid virtual tuple (its `tts_values`/`tts_isnull` arrays
    /// have already been filled, e.g. by `index_deform_tuple`). Targets the
    /// slot by pool id. Fallible only via the slot-ops `ereport(ERROR)` paths.
    pub fn exec_store_virtual_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Slot-payload op for `StoreIndexTuple`'s name-cstring fix-up
    /// (nodeIndexonlyscan.c): for each attribute number in `attnums` whose slot
    /// value is non-null, copy the cstring datum into a NAMEDATALEN-byte
    /// allocation in `per_tuple_ecxt` (the C `namestrcpy` zero-pad) and store
    /// the resulting `Name` datum back. The decision of *which* attnums is the
    /// node's owned logic; this seam performs only the slot-value read/write
    /// the slot owns. Fallible on OOM.
    pub fn pad_name_cstring_columns<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        per_tuple_ecxt: types_nodes::EcxtId,
        attnums: &[types_core::AttrNumber],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecFetchSlotMinimalTuple(slot, &shouldFree)` (execTuples.c): materialize
    /// the slot's contents as a `MinimalTuple` (copied into `mcx`), returning it
    /// and whether the caller must free it. Fallible on OOM.
    pub fn exec_fetch_slot_minimal_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: &mut types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<(
        mcx::PgBox<'mcx, types_tuple::heaptuple::MinimalTupleData<'mcx>>,
        bool,
    )>
);

seam_core::seam!(
    /// `ExecMaterializeSlot(slot)` (execTuples.c, via the slot ops): force the
    /// slot to materialize its own copy of the tuple's data (so later changes
    /// to the source storage cannot affect it). Fallible: materializing can
    /// `palloc` (OOM).
    pub fn exec_materialize_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `execute_attr_map_slot(attrMap, in_slot, out_slot)` (tupconvert.c):
    /// remap `in_slot`'s attributes through `attr_map` into `out_slot` and
    /// return `out_slot`. The conversion map is the one ExecGetChildToRootMap
    /// stored on the source `ResultRelInfo`'s `ri_ChildToRootMap`; the owner
    /// reads it from there. Fallible on `palloc` (OOM).
    pub fn execute_attr_map_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        in_slot: types_nodes::SlotId,
        out_slot: types_nodes::SlotId,
    ) -> types_error::PgResult<types_nodes::SlotId>
);
