//! Seam declarations for the `backend-executor-execTuples` unit
//! (`executor/execTuples.c`): slot creation and the slot-ops virtual calls
//! (`tuptable.h`'s `ExecClearTuple` / `ExecCopySlot` dispatch through the
//! `TupleTableSlotOps` tables that execTuples.c owns).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `BlessTupleDesc(tupdesc)` (execTuples.c): register a transient record
    /// type for an anonymous (RECORD, typmod < 0) descriptor so rowtype datums
    /// can be made with it, and return it. A non-anonymous descriptor passes
    /// through unchanged. Fallible on the `assign_record_type_typmod`
    /// `ereport(ERROR)`.
    pub fn bless_tuple_desc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tupdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

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
    /// `slot->tts_tupleDescriptor` for an arbitrary pool slot (tuptable.h): the
    /// tuple descriptor a slot was created with, addressed by its `EState`
    /// tuple-table pool [`SlotId`], copied into `mcx`. nodeAgg's
    /// `build_hash_table` reads `perhash->hashslot->tts_tupleDescriptor` to
    /// marshal `BuildTupleHashTable`.
    ///
    /// PROVISIONAL: re-sign when the slot payload model lands (the descriptor
    /// will then be lent, not copied).
    pub fn exec_slot_descriptor<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
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
    /// STANDALONE-SLOT FORM: for callers whose slot is NOT in the `EState`
    /// tuple-table pool (no `SlotId`) — `CopyOneRowTo`'s `table_slot_create`
    /// scan slot and logical replication's `logicalrep_write_tuple` slot. The
    /// slot is the unified payload-bearing [`SlotData`], so this runs the real
    /// deform. Pool-resident callers use [`slot_getallattrs_by_id`] instead.
    pub fn slot_getallattrs<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: &mut types_nodes::tuptable::SlotData<'mcx>,
    ) -> types_error::PgResult<
        mcx::PgVec<'mcx, types_tuple::backend_access_common_heaptuple::DeformedColumn<'mcx>>,
    >
);

seam_core::seam!(
    /// `slot_getallattrs(slot)` (tuptable.h) resolving a pool [`SlotId`] to its
    /// live payload-bearing [`SlotData`] first: fully deconstruct the slot and
    /// return its per-attribute `(value, isnull)` array, copied into `mcx` (in
    /// C the arrays live in the slot itself). Deforming can detoast/allocate, so
    /// fallible. This is the form pool-resident executor nodes use (they hold
    /// the slot's pool id); the header-only [`slot_getallattrs`] is the
    /// standalone-slot sibling.
    pub fn slot_getallattrs_by_id<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
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
    /// (`slot->tts_ops->clear`). The slot is addressed by its `EState`
    /// tuple-table pool id; the body resolves it to the live payload-bearing
    /// [`types_nodes::tuptable::SlotData`] and runs the real `clear` op (the
    /// header-only `&mut TupleTableSlot` projection carries no payload to
    /// clear). This is the form every executor node uses (they hold the slot's
    /// pool id); it is the same op as the `exec_clear_tuple_by_id` alias.
    pub fn exec_clear_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(slot)` (tuptable.h) resolving a pool [`SlotId`] to its
    /// live payload-bearing slot first — the form the `tuplestore_gettupleslot`
    /// "no tuple" path needs (the header-only [`exec_clear_tuple`] cannot reach
    /// the payload).
    pub fn exec_clear_tuple_by_id(
        estate: &mut types_nodes::EStateData<'_>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecResetTupleTable`'s per-slot processing (execTuples.c): given one
    /// live `es_tupleTable` slot, `ExecClearTuple(slot)` then
    /// `slot->tts_ops->release(slot)` then `ReleaseTupleDesc` the slot's
    /// descriptor (clearing it). The owned model carries the slot as the
    /// payload-bearing [`types_nodes::tuptable::SlotData`]; the `shouldFree`
    /// memory release of the slot/value-arrays is the pool drop in
    /// `ExecResetTupleTable` (no separate `pfree`), so it is not a parameter
    /// here. The descriptor release can run tupdesc-owner code, so fallible.
    pub fn exec_reset_one_slot(
        slot: &mut types_nodes::tuptable::SlotData<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `slot_getsomeattrs(slot, attnum)` then `(slot->tts_values[attnum-1],
    /// slot->tts_isnull[attnum-1])` (tuptable.h, via execTuples.c): ensure the
    /// first `attnum` columns are extracted and return the `(value, isnull)` of
    /// the 1-based `attnum`th. nodeSort's Datum sort reads attribute 1.
    /// Deforming can detoast/allocate, so the call is fallible. The returned
    /// value is the canonical [`types_tuple::backend_access_common_heaptuple::Datum`].
    /// The slot is addressed by its `EState` tuple-table pool [`SlotId`] (the
    /// payload-bearing [`SlotData`] is not reachable through the header-only
    /// `&mut TupleTableSlot` projection); a by-reference image is copied out into
    /// the per-query context.
    pub fn slot_getsomeattr<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        attnum: i32,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        bool,
    )>
);

seam_core::seam!(
    /// `ExecClearTuple(slot); slot->tts_values[0] = val; slot->tts_isnull[0] =
    /// isnull; ExecStoreVirtualTuple(slot)` (tuptable.h, via execTuples.c):
    /// store a single-Datum virtual tuple in the (single-column) result slot.
    /// nodeSort's Datum-sort output path. Storing can allocate, fallible.
    pub fn exec_store_first_datum<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        val: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        is_null: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCopySlot(dstslot, srcslot)` (tuptable.h): copy the source slot's
    /// tuple into the destination slot (`dstslot->tts_ops->copyslot`). Both
    /// slots are addressed by their `EState` tuple-table pool ids; the body
    /// resolves them to the live payload-bearing
    /// [`types_nodes::tuptable::SlotData`] pair and runs the real `copyslot`
    /// op. The copy allocates in the destination slot's memory context (C:
    /// `slot->tts_mcxt`; here the query context). Fallible on OOM.
    pub fn exec_copy_slot<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        dstslot: types_nodes::SlotId,
        srcslot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCopySlotMinimalTuple(slot)` (tuptable.h): produce a freshly-palloc'd
    /// `MinimalTuple` copy of the slot's current tuple (the C
    /// `slot->tts_ops->copy_minimal_tuple`), owned by the caller. The copy lands
    /// in `mcx` (C: `CurrentMemoryContext`). Fallible on OOM. The owned model
    /// returns the payload-bearing
    /// [`FormedMinimalTuple`](types_tuple::backend_access_common_heaptuple::FormedMinimalTuple)
    /// carrier (header + user-data area).
    pub fn exec_copy_slot_minimal_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>>
);

seam_core::seam!(
    /// `ExecCopySlotMinimalTupleExtra(slot, extra)` (tuptable.h inline): copy
    /// the slot's tuple into a freshly-`palloc`'d `MinimalTuple` reserving
    /// `extra` leading MAXALIGNed bytes (the `TupleHashEntryGetAdditional`
    /// space `LookupTupleHashEntry_internal` requests). The owned model carries
    /// the payload-bearing
    /// [`FormedMinimalTuple`](types_tuple::backend_access_common_heaptuple::FormedMinimalTuple);
    /// the `extra` bytes live alongside in the hash entry's `additional` field.
    /// Targets the slot by pool id (the execGrouping driver reads it off the
    /// EState arena). Fallible on OOM / the slot-ops `ereport(ERROR)` paths.
    pub fn exec_copy_slot_minimal_tuple_extra<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        extra: usize,
    ) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>>
);

seam_core::seam!(
    /// `ExecStoreMinimalTuple(mtup, slot, shouldFree)` (tuptable.h / execTuples.c):
    /// store the `MinimalTuple` into the slot (forcing it through the slot's
    /// minimal-tuple ops), the C returning the same slot. `should_free` records
    /// whether the slot owns and should later free the tuple. Fallible on OOM.
    /// The owned model carries the payload-bearing
    /// [`FormedMinimalTuple`](types_tuple::backend_access_common_heaptuple::FormedMinimalTuple).
    pub fn exec_store_minimal_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        mtup: types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
        slot: types_nodes::SlotId,
        should_free: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecStoreMinimalTuple(mtup, slot, shouldFree)` (tuptable.h / execTuples.c)
    /// over the payload-bearing [`SlotData`] held directly (no `EState`/`SlotId`
    /// pool) — the form `pquery.c`'s `RunFromStore` / the standalone tuplestore
    /// fetch needs. `mcx` is the slot's context. Fallible on OOM / wrong slot
    /// kind.
    pub fn exec_store_minimal_tuple_payload<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        mtup: types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
        slot: &mut types_nodes::tuptable::SlotData<'mcx>,
        should_free: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCleanTypeFromTL(targetList)` (execTuples.c): build a result tuple
    /// descriptor from `target_list`, omitting resjunk columns. `pquery.c`'s
    /// `PortalStart` `PORTAL_ONE_RETURNING` / `PORTAL_ONE_MOD_WITH` legs use it.
    /// Allocates the descriptor in `mcx`; fallible on OOM.
    pub fn exec_clean_type_from_tl<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        target_list: &[types_nodes::primnodes::TargetEntry<'mcx>],
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `ExecStoreBufferHeapTuple(tuple, slot, buffer)` (execTuples.c): store an
    /// on-disk heap tuple (still living in the pinned page `buffer`) into a
    /// `BufferHeapTupleTableSlot`, taking a pin on `buffer` (releasing any pin
    /// the slot previously held). This is the store the heap-scan layer
    /// (`heapam_handler.c`'s `heapam_scan_getnextslot` / `index_fetch_tuple`)
    /// performs on the payload-bearing slot the table-AM vtable hands it — the
    /// reason the vtable's slot parameter is the [`SlotData`] enum (the C
    /// `TupleTableSlot *` that actually points at a `BufferHeapTupleTableSlot`)
    /// rather than the header-only base. The slot is borrowed directly (the
    /// caller holds the `&mut SlotData` from the vtable callback, not an EState
    /// pool id). `Err` carries the "wrong type of slot" `elog(ERROR)`.
    pub fn exec_store_buffer_heap_tuple<'mcx>(
        tuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        slot: &mut types_nodes::tuptable::SlotData<'mcx>,
        buffer: types_storage::buf::Buffer,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecStorePinnedBufferHeapTuple(tuple, slot, buffer)` (execTuples.c):
    /// like [`exec_store_buffer_heap_tuple`] but *transfers* an existing pin on
    /// `buffer` into the slot instead of taking a fresh one — the store
    /// `heapam_handler.c`'s `heapam_fetch_row_version` performs on the
    /// `heap_fetch`-returned (already-pinned) `userbuf`. The slot crosses as the
    /// payload-bearing `&mut SlotData` (held directly by the table-AM vtable
    /// callback, not an EState pool id). `Err` carries the "wrong type of slot"
    /// `elog(ERROR)`.
    pub fn exec_store_pinned_buffer_heap_tuple<'mcx>(
        tuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        slot: &mut types_nodes::tuptable::SlotData<'mcx>,
        buffer: types_storage::buf::Buffer,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(slot)` (tuptable.h) over the payload-bearing [`SlotData`]
    /// the heap-scan vtable callback holds directly (the end-of-scan / no-match
    /// path of `heapam_scan_getnextslot` / `heap_getnextslot_tidrange`). Unlike
    /// the pool-id [`exec_clear_tuple`], the caller has the `&mut SlotData` from
    /// the vtable, not an `EState` `SlotId`. Runs the real `tts_ops->clear`.
    pub fn exec_clear_tuple_payload<'mcx>(
        slot: &mut types_nodes::tuptable::SlotData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecFetchSlotHeapTuple(slot, materialize, &shouldFree)` (execTuples.c)
    /// over the payload-bearing [`SlotData`] the table-AM DML vtable callbacks
    /// (`heapam_tuple_insert` / `heapam_tuple_update` / `heapam_tuple_lock`)
    /// hold directly. C's `heapam_handler.c` calls `ExecFetchSlotHeapTuple(slot,
    /// true, &shouldFree)` to obtain the heap tuple to pass to
    /// `heap_insert`/`heap_update`/`heap_lock_tuple`; the heap modify core is the
    /// unported owner of those wrappers, so this seam is the bridge it will call.
    /// Unlike the pool-id `exec_*` seams, the caller has the `&mut SlotData` from
    /// the vtable, not an `EState` `SlotId`. Returns `(tuple, should_free)` —
    /// `should_free` is `true` when a fresh tuple was built (virtual/minimal
    /// slots) and the caller must free it, `false` when the slot's stored tuple
    /// was returned (heap/buffer-heap slots). `Err` carries the materialize OOM.
    pub fn exec_fetch_slot_heap_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: &mut types_nodes::tuptable::SlotData<'mcx>,
        materialize: bool,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        bool,
    )>
);

seam_core::seam!(
    /// `ExecFetchSlotHeapTupleDatum(slot)` (execTuples.c): fetch the slot's
    /// current row as a composite/row `Datum` (`heap_copy_tuple_as_datum` over
    /// `ExecFetchSlotHeapTuple(slot, false)`). The slot is addressed by its
    /// EState tuple-table pool [`SlotId`]; the result is the canonical
    /// [`types_tuple::backend_access_common_heaptuple::Datum`] (a
    /// `Datum::Composite` carrying the row image in `mcx`). Used by the
    /// `ExecMakeFunctionResultSet` Materialize-mode drain to return a whole
    /// `funcReturnsTuple` row from `funcResultSlot`. Fallible on the materialize
    /// OOM / form path.
    pub fn exec_fetch_slot_heap_tuple_datum<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<
        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    >
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
    ) -> types_error::PgResult<types_nodes::tuptable::SlotData<'mcx>>
);

seam_core::seam!(
    /// `ExecDropSingleTupleTableSlot(slot)` (execTuples.c): release a slot
    /// made with `MakeSingleTupleTableSlot`. Clearing the slot can release a
    /// buffer pin, whose bookkeeping can `elog(ERROR)`, carried on `Err`.
    pub fn exec_drop_single_tuple_table_slot<'mcx>(
        slot: types_nodes::tuptable::SlotData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(slot); memcpy(slot->tts_values, values); memcpy(
    /// slot->tts_isnull, isnull); ExecStoreVirtualTuple(slot)` (tuptable.h):
    /// fill a virtual slot's per-column `tts_values`/`tts_isnull` payload with
    /// the given `values`/`isnull` (which must be `slot`'s descriptor's `natts`
    /// long) and mark it as holding a valid virtual tuple. The N-column analogue
    /// of `exec_store_single_datum_virtual`; used by Memoize's
    /// `prepare_probe_slot` to materialize the probe slot before
    /// `ExecCopySlotMinimalTuple`. Targets the slot by pool id. Fallible on OOM /
    /// the slot-ops `ereport(ERROR)` paths.
    pub fn store_virtual_values<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        values: &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
        isnull: &[bool],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecClearTuple(slot); index_deform_tuple(itup, itupdesc,
    /// slot->tts_values, slot->tts_isnull)` (nodeIndexonlyscan.c StoreIndexTuple,
    /// genam.c index_deform_tuple): clear the slot and write the deformed
    /// per-column `values`/`isnull` into its `tts_values`/`tts_isnull` payload —
    /// WITHOUT marking it as a stored virtual tuple. Unlike [`store_virtual_values`]
    /// this does NOT call `ExecStoreVirtualTuple`, because the IndexOnlyScan caller
    /// performs an intervening name-cstring fixup and a single trailing
    /// `ExecStoreVirtualTuple` (matching C). Targets the slot by pool id. Fallible
    /// on the slot-ops `ereport(ERROR)` paths.
    pub fn fill_virtual_values<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        values: &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
        isnull: &[bool],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecTypeFromExprList(exprList)` (execTuples.c): build a `TupleDesc` (a
    /// RECORD type, no column names) describing the result types of a bare list
    /// of expressions. Used to derive a Memoize node's `hashkeydesc` from its
    /// `param_exprs`. The descriptor is allocated in `mcx` (C: `palloc` in
    /// `CurrentMemoryContext`); fallible on OOM / `ereport(ERROR)` from a type
    /// lookup.
    pub fn exec_type_from_expr_list<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        expr_list: &[types_nodes::primnodes::Expr],
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `slot_getsysattr(slot, attnum, &isnull)` (tuptable.h/execTuples.c):
    /// fetch a system attribute of the slot's current tuple as
    /// `(datum, isnull)` (`slot->tts_ops->getsysattr` dispatch). A slot class
    /// without system attributes (e.g. virtual) is the C `elog(ERROR)`,
    /// carried on `Err`. The returned value is the canonical
    /// [`types_tuple::backend_access_common_heaptuple::Datum`]; a by-reference
    /// image is copied into `mcx`.
    ///
    /// STANDALONE-SLOT FORM: the only caller (`GetTupleTransactionInfo` in the
    /// logical-replication conflict path) holds a standalone payload-bearing
    /// [`SlotData`] outside any `EState` pool (no `SlotId`); this dispatches the
    /// real `tts_ops->getsysattr` against the slot's stored tuple.
    pub fn slot_getsysattr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: &mut types_nodes::tuptable::SlotData<'mcx>,
        attnum: types_core::AttrNumber,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        bool,
    )>
);

seam_core::seam!(
    /// `slot_getattr(slot, attnum, &isnull)` (tuptable.h): fetch a regular
    /// (positive) attribute `attnum` (1-based) of the slot's current tuple as
    /// `(datum, isnull)`, deforming up to `attnum` first via
    /// `slot_getsomeattrs`/`slot_getsomeattrs_int` (`slot->tts_ops->getsomeattrs`
    /// dispatch). The slot is borrowed mutably because deforming populates the
    /// slot's `tts_values`/`tts_isnull`/`tts_nvalid`; deforming can
    /// detoast/allocate, so the call is fallible. System (non-positive) attnums
    /// take the `slot_getsysattr` path instead and are never passed here. The
    /// returned value is the canonical
    /// [`types_tuple::backend_access_common_heaptuple::Datum`]; a by-reference
    /// image is copied into the per-query context. The slot is addressed by its
    /// `EState` tuple-table pool [`SlotId`] (the payload-bearing [`SlotData`] is
    /// not reachable through the header-only `&mut TupleTableSlot` projection);
    /// this is the same C op as [`slot_getattr_by_id`], returning the bare
    /// `(value, isnull)` pair the sort/compare callers destructure.
    pub fn slot_getattr<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        attnum: types_core::AttrNumber,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        bool,
    )>
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
///
/// Carries the canonical [`types_tuple::backend_access_common_heaptuple::Datum`]
/// value (no longer `Copy`: the by-reference arm owns a `PgVec`). Canonical
/// definition in `types-tuple`.
pub use types_tuple::backend_access_common_heaptuple::SlotAttr;

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
    ) -> types_error::PgResult<SlotAttr<'mcx>>
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
    ) -> types_error::PgResult<SlotAttr<'mcx>>
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
    /// `ExecCopySlotHeapTuple(slot)` (execTuples.c): make a palloc'd heap-tuple
    /// copy of the slot's current contents (in the EState per-query context).
    /// Targets the slot by pool id. The owned model returns the
    /// [`FormedTuple`](types_tuple::backend_access_common_heaptuple::FormedTuple)
    /// carrier (header + user-data area). Fallible on OOM.
    pub fn exec_copy_slot_heap_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>
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
    /// `ExecForceStoreHeapTuple(tuple, slot, shouldFree)` (execTuples.c) taking
    /// the full [`FormedTuple`](types_tuple::backend_access_common_heaptuple::FormedTuple)
    /// carrier (header + user-data area) by value — the correct contract for a
    /// copied tuple, since a bare `HeapTupleData` header does not carry the
    /// tuple's data bytes. Targets the slot by pool id. Fallible on OOM.
    pub fn exec_force_store_formed_heap_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        tuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        should_free: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecForceStoreMinimalTuple(mtup, slot, shouldFree)` (execTuples.c):
    /// store a `MinimalTuple` into the slot (forcing it through the slot's ops),
    /// taking ownership when `should_free`. Fallible on OOM.
    pub fn exec_force_store_minimal_tuple<'mcx>(
        slot: types_nodes::SlotId,
        mtup: types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
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
    /// and whether the caller must free it. Fallible on OOM. The slot is
    /// addressed by its EState-pool id (the owner reaches the payload-bearing
    /// `SlotData`); the owned model returns the payload-bearing
    /// [`FormedMinimalTuple`](types_tuple::backend_access_common_heaptuple::FormedMinimalTuple).
    pub fn exec_fetch_slot_minimal_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>,
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

// `ExecComputeStoredGenerated`'s per-attribute compute loop (nodeModifyTable.c)
// is implemented inline in `backend-executor-nodeModifyTable` (its C home),
// driving the slot-payload (`slot_getallattrs_by_id` / `store_virtual_values` /
// `exec_materialize_slot`), the `exec_eval_expr_switch_context` interpreter
// seam, and the `datum_copy_v` seam directly — so no `exec_store_generated_columns`
// seam is needed.

seam_core::seam!(
    /// `execute_attr_map_slot(attrMap, in_slot, out_slot)` (tupconvert.c) with
    /// an explicitly-supplied `attrMap` (rather than one read off a
    /// `ResultRelInfo` field). Used by callers that obtained the map directly
    /// (e.g. `ExecGetRootToChildMap`'s returned `AttrMap`): remap `in_slot`'s
    /// attributes through `attr_map` into `out_slot` and return `out_slot`.
    /// Fallible on `palloc` (OOM).
    pub fn execute_attr_map_slot_explicit<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        attr_map: &types_tuple::attmap::AttrMap<'mcx>,
        in_slot: types_nodes::SlotId,
        out_slot: types_nodes::SlotId,
    ) -> types_error::PgResult<types_nodes::SlotId>
);

seam_core::seam!(
    /// `ExecFetchSlotMinimalTuple(slot, &shouldFree)` (execTuples.c): the
    /// slot's contents as a `MinimalTuple` (`slot->tts_ops->get_minimal_tuple`
    /// or `copy_minimal_tuple`). The owned model always returns a copy, so the C
    /// `shouldFree` / `heap_free_minimal_tuple` bookkeeping is internal to the
    /// owner and does not cross the seam. The slot is addressed by its
    /// EState-pool id. This is the *boundary* form used by the shm_mq transport
    /// (tqueue): it returns the `MinimalTuple`'s contiguous C byte image — the
    /// flat blob (`t_len` first) `shm_mq_send` ships verbatim — allocated in
    /// `mcx`. The materialize / copy / serialize path allocates, so the call is
    /// fallible on OOM.
    pub fn exec_fetch_slot_minimal_tuple_copy<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

// ===========================================================================
// Standalone-slot forms (a slot made by `MakeSingleTupleTableSlot`, held as a
// payload-bearing `SlotData` outside `es_tupleTable`). The incremental-sort
// node's `group_pivot` / `transfer_tuple` slots are standalone, so these forms
// take `&mut SlotData` directly instead of an `es_tupleTable` `SlotId`.
// ===========================================================================

seam_core::seam!(
    /// `slot_getattr(slot, attnum, &isnull)` (tuptable.h) for a standalone
    /// [`SlotData`]: fetch a regular attribute as `(datum, isnull)`, deforming
    /// up to `attnum` first. Mutably borrows the slot (deforming populates
    /// `tts_values`/`tts_isnull`); detoast can allocate, so fallible. The
    /// pool-`SlotId` form is [`slot_getattr`].
    pub fn slot_getattr_standalone<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        slot: &mut types_nodes::SlotData<'mcx>,
        attnum: types_core::AttrNumber,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        bool,
    )>
);

seam_core::seam!(
    /// `ExecClearTuple(slot)` (tuptable.h) for a standalone [`SlotData`]: clear
    /// the slot's stored tuple (releasing any held resources). Clearing can
    /// release a buffer pin whose bookkeeping can `elog(ERROR)`, carried on
    /// `Err`. The pool-`SlotId` form is [`exec_clear_tuple`].
    pub fn exec_clear_tuple_standalone<'mcx>(
        slot: &mut types_nodes::SlotData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCopySlot(dstslot, srcslot)` (tuptable.h) with BOTH slots standalone
    /// [`SlotData`]s (the incremental-sort `ExecCopySlot(group_pivot,
    /// transfer_tuple)`): copy the source slot's tuple into the destination.
    /// Copies the tuple, fallible on OOM. The pool-`SlotId` form is
    /// [`exec_copy_slot`].
    pub fn exec_copy_slot_standalone<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        dstslot: &mut types_nodes::SlotData<'mcx>,
        srcslot: &mut types_nodes::SlotData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCopySlot(dstslot, srcslot)` (tuptable.h) where `dstslot` is a
    /// standalone [`SlotData`] and `srcslot` is an `es_tupleTable` pool slot
    /// (the incremental-sort `ExecCopySlot(group_pivot, outer_result_slot)`):
    /// copy the pool source slot's tuple into the standalone destination.
    /// Copies the tuple, fallible on OOM.
    pub fn exec_copy_pool_slot_into_standalone<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        dstslot: &mut types_nodes::SlotData<'mcx>,
        srcslot: types_nodes::SlotId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecTypeSetColNames(typeInfo, namesList)` (execTuples.c): apply the
    /// column aliases from a not-yet-blessed RECORD `TupleDesc`'s source RTE
    /// `eref->colnames` (a `List *` of `String` nodes) over the descriptor's
    /// non-dropped attributes, in order. `ExecEvalWholeRowVar`'s RECORD branch
    /// adopts the source range-table entry's column aliases this way. The
    /// `names` are the already-extracted alias strings (the C walks the
    /// `String` node list).
    pub fn exec_type_set_col_names<'mcx>(
        type_info: &mut types_tuple::heaptuple::TupleDescData<'mcx>,
        names: &[&str],
    ) -> types_error::PgResult<()>
);
