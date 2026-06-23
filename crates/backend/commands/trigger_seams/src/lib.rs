//! Seam declarations for the trigger manager (`commands/trigger.c`) and the
//! executor slot accessors `ri_triggers.c` invokes on the trigger manager's
//! `TupleTableSlot`s.
//!
//! `ri_triggers.c` is trigger glue: it reads its `TriggerData *` context and the
//! OLD/NEW slots the trigger manager hands it, and it reads attribute values out
//! of those slots. The slots and trigger descriptors are owned by the trigger
//! manager / executor (not yet ported), so each accessor is a seam keyed by the
//! foreign handle. The owning unit installs these from its `init_seams()`; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_tuple::heaptuple::Datum;
use ::types_ri_triggers::{TriggerDataRef, TriggerRef, TupleTableSlotRef};

seam_core::seam!(
    /// `get_trigger_oid(relid, trigname, missing_ok)` (trigger.c): the OID of
    /// the named trigger on relation `relid`, or `InvalidOid` with
    /// `missing_ok = true`. With `missing_ok = false` a miss raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`).
    pub fn get_trigger_oid(relid: Oid, trigname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `AfterTriggerBeginXact()` — initialize the deferred-trigger manager.
    pub fn after_trigger_begin_xact() -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerFireDeferred()` — fire all pending deferred triggers
    /// (user code; can `ereport(ERROR)`).
    pub fn after_trigger_fire_deferred() -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerBeginQuery()` (trigger.c) — start an after-trigger query
    /// level (bumps `afterTriggers.query_depth`). Called by the executor's
    /// `ExecutorStart` unless `EXEC_FLAG_SKIP_TRIGGERS`/`EXPLAIN_ONLY`.
    pub fn after_trigger_begin_query() -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerEndQuery(estate)` (trigger.c) — fire this query level's
    /// AFTER IMMEDIATE events, promote deferred ones, release the level's
    /// storage. Called by the executor's `ExecutorFinish` unless
    /// `EXEC_FLAG_SKIP_TRIGGERS`.
    pub fn after_trigger_end_query<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerEndXact(isCommit)` — shut down the deferred-trigger
    /// manager.
    pub fn after_trigger_end_xact(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerBeginSubXact()`.
    pub fn after_trigger_begin_sub_xact() -> PgResult<()>
);

seam_core::seam!(
    /// `AfterTriggerEndSubXact(isCommit)`.
    pub fn after_trigger_end_sub_xact(is_commit: bool) -> PgResult<()>
);

// ---- trigger-data accessors (commands/trigger.c) --------------------------

seam_core::seam!(
    /// `CALLED_AS_TRIGGER(fcinfo)` — is `fcinfo->context` a `TriggerData`?
    pub fn called_as_trigger(trigdata: TriggerDataRef) -> bool
);
seam_core::seam!(
    /// `trigdata->tg_event` — the `TriggerEvent` bitmask.
    pub fn tg_event(trigdata: TriggerDataRef) -> u32
);
seam_core::seam!(
    /// `RelationGetRelid(trigdata->tg_relation)` — the OID of the relation the
    /// trigger fired on. RI reads only identity/name/namespace/owner/relkind
    /// off the trigger relation; those are exposed individually below.
    pub fn tg_relation_oid(trigdata: TriggerDataRef) -> Oid
);
seam_core::seam!(
    /// `RelationGetRelationName(trigdata->tg_relation)` — raw server-encoded
    /// name bytes, copied into `mcx`.
    pub fn tg_relation_name<'mcx>(
        mcx: Mcx<'mcx>,
        trigdata: TriggerDataRef,
    ) -> PgResult<PgVec<'mcx, u8>>
);
seam_core::seam!(
    /// `RelationGetNamespace(trigdata->tg_relation)` — `relnamespace`.
    pub fn tg_relation_namespace(trigdata: TriggerDataRef) -> Oid
);
seam_core::seam!(
    /// `trigdata->tg_relation->rd_rel->relowner`.
    pub fn tg_relation_owner(trigdata: TriggerDataRef) -> Oid
);
seam_core::seam!(
    /// `trigdata->tg_relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE`.
    pub fn tg_relation_is_partitioned(trigdata: TriggerDataRef) -> bool
);
seam_core::seam!(
    /// `RIAttName(trigdata->tg_relation, attnum)`: the trigger relation's
    /// attribute name bytes, copied into `mcx`.
    pub fn tg_relation_att_name<'mcx>(
        mcx: Mcx<'mcx>,
        trigdata: TriggerDataRef,
        attnum: i16,
    ) -> PgResult<PgVec<'mcx, u8>>
);
seam_core::seam!(
    /// `RIAttType(trigdata->tg_relation, attnum)`.
    pub fn tg_relation_att_type(trigdata: TriggerDataRef, attnum: i16) -> Oid
);
seam_core::seam!(
    /// `RIAttCollation(trigdata->tg_relation, attnum)`.
    pub fn tg_relation_att_collation(trigdata: TriggerDataRef, attnum: i16) -> Oid
);
seam_core::seam!(
    /// `table_tuple_satisfies_snapshot(trigdata->tg_relation, slot,
    /// SnapshotSelf)`.
    pub fn tg_relation_tuple_satisfies_snapshot_self(
        trigdata: TriggerDataRef,
        slot: TupleTableSlotRef,
    ) -> PgResult<bool>
);
// ---------------------------------------------------------------------------
// Outward seams the BEFORE-ROW UPDATE/DELETE firing front calls to fetch and
// lock the OLD on-disk tuple (`GetTupleForTrigger`).  The trigger manager
// (`commands/trigger.c`) is BELOW the executor's tableam/slot machinery in the
// crate DAG, so these cross back up to the owning executor unit
// (`nodeModifyTable`), which installs them from its `init_seams()`.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ExecGetTriggerOldSlot(estate, relinfo)` (execUtils.c): the relInfo's
    /// reusable slot for a trigger's OLD tuple (lazily created against the
    /// relation's tuple descriptor). Returns its `SlotId`.
    pub fn exec_get_trigger_old_slot<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
    ) -> PgResult<nodes::SlotId>
);

seam_core::seam!(
    /// `ExecGetTriggerNewSlot(estate, relinfo)` (execUtils.c): the relInfo's
    /// reusable slot for a trigger's NEW tuple (lazily created against the
    /// relation's tuple descriptor). Returns its `SlotId`. Used by the
    /// cross-partition AFTER-UPDATE root-conversion path.
    pub fn exec_get_trigger_new_slot<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
    ) -> PgResult<nodes::SlotId>
);

seam_core::seam!(
    /// `ExecUpdateLockMode(estate, relinfo)` (execMain.c): the row-lock mode
    /// (`LockTupleExclusive` / `LockTupleNoKeyExclusive`) for a BEFORE-ROW
    /// UPDATE, chosen by whether any key column is updated.
    pub fn exec_update_lock_mode<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
    ) -> PgResult<types_tableam::tableam::LockTupleMode>
);

seam_core::seam!(
    /// `table_tuple_lock(rel, tid, estate->es_snapshot, oldslot,
    /// estate->es_output_cid, mode, LockWaitBlock, lockflags, &tmfd)`
    /// (tableam.h) for `GetTupleForTrigger`: lock the OLD on-disk tuple into
    /// `oldslot`, returning the lock outcome. `find_last_version` corresponds
    /// to `TUPLE_LOCK_FLAG_FIND_LAST_VERSION` (set unless the isolation level
    /// uses a transaction snapshot).
    pub fn get_tuple_for_trigger_lock<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
        tupleid: &::types_tuple::heaptuple::ItemPointerData,
        oldslot: nodes::SlotId,
        mode: types_tableam::tableam::LockTupleMode,
        find_last_version: bool,
        tmfd: &mut types_tableam::tableam::TM_FailureData,
    ) -> PgResult<types_tableam::tableam::TM_Result>
);

seam_core::seam!(
    /// `table_tuple_fetch_row_version(rel, tid, SnapshotAny, oldslot)`
    /// (tableam.h) for the no-EPQ leg of `GetTupleForTrigger`: fetch the row
    /// version identified by `tid` into `oldslot`. Returns `false` if absent.
    pub fn get_tuple_for_trigger_fetch<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
        tupleid: &::types_tuple::heaptuple::ItemPointerData,
        oldslot: nodes::SlotId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `trigdata->tg_trigger`.
    pub fn tg_trigger(trigdata: TriggerDataRef) -> TriggerRef
);
seam_core::seam!(
    /// `trigdata->tg_trigslot`.
    pub fn tg_trigslot(trigdata: TriggerDataRef) -> TupleTableSlotRef
);
seam_core::seam!(
    /// `trigdata->tg_newslot`.
    pub fn tg_newslot(trigdata: TriggerDataRef) -> TupleTableSlotRef
);
seam_core::seam!(
    /// `trigdata->tg_trigtuple` — the OLD/row-being-modified `HeapTuple` the
    /// trigger manager handed the trigger function, copied into `mcx`.
    /// `Ok(None)` mirrors a NULL `tg_trigtuple`.
    pub fn tg_trigtuple<'mcx>(
        mcx: Mcx<'mcx>,
        trigdata: TriggerDataRef,
    ) -> PgResult<Option<::types_tuple::heaptuple::HeapTupleData<'mcx>>>
);
seam_core::seam!(
    /// `trigdata->tg_newtuple` — the NEW `HeapTuple` (for an UPDATE) the trigger
    /// manager handed the trigger function, copied into `mcx`. `Ok(None)`
    /// mirrors a NULL `tg_newtuple`.
    pub fn tg_newtuple<'mcx>(
        mcx: Mcx<'mcx>,
        trigdata: TriggerDataRef,
    ) -> PgResult<Option<::types_tuple::heaptuple::HeapTupleData<'mcx>>>
);
seam_core::seam!(
    /// The fully-formed OLD/NEW tuple a row trigger is firing on, addressed by
    /// the `tg_trigslot`/`tg_newslot` marker — the `FormedTuple` (header + user
    /// data) the trigger manager materialized for this call. `Ok(None)` mirrors
    /// the C `TupIsNull(slot)` (no tuple in that slot). Used by
    /// `plpgsql_exec_trigger` to populate the NEW/OLD expanded records via
    /// `expanded_record_set_tuple`.
    pub fn tg_slot_formed_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        slot: TupleTableSlotRef,
    ) -> PgResult<Option<::types_tuple::heaptuple::FormedTuple<'mcx>>>
);

seam_core::seam!(
    /// `trigger->tgnargs` — the number of textual arguments declared for the
    /// trigger (`TG_NARGS` in PL/pgSQL). Resolves off the current `tg_trigger`.
    pub fn tg_nargs(trigdata: TriggerDataRef) -> i32
);
seam_core::seam!(
    /// `trigger->tgargs[i]` — the i-th textual trigger argument
    /// (`TG_ARGV[i]` in PL/pgSQL), copied into `mcx`. `Ok(None)` for an
    /// out-of-range index (the C `i >= tgnargs` case yields a NULL element).
    pub fn tg_argv<'mcx>(
        mcx: Mcx<'mcx>,
        trigdata: TriggerDataRef,
        i: i32,
    ) -> PgResult<Option<PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `trigger->tgconstraint`.
    pub fn trigger_constraint(trigger: TriggerRef) -> Oid
);
seam_core::seam!(
    /// `trigger->tgconstrrelid`.
    pub fn trigger_constrrelid(trigger: TriggerRef) -> Oid
);
seam_core::seam!(
    /// `trigger->tgname` — raw name bytes, copied into `mcx`.
    pub fn trigger_name<'mcx>(mcx: Mcx<'mcx>, trigger: TriggerRef) -> PgResult<PgVec<'mcx, u8>>
);
seam_core::seam!(
    /// `trigger->tgconstrindid` — the OID of the unique/exclusion index backing
    /// the constraint whose deferred recheck the trigger fires (read by
    /// `constraint.c`'s `unique_key_recheck` to `index_open` the index).
    pub fn trigger_constrindid(trigger: TriggerRef) -> Oid
);

// ---- the live trigger carriers `unique_key_recheck` (constraint.c) drives ----
//
// Unlike `ri_triggers.c` (which reads only scalars/attributes off the trigger
// relation), `commands/constraint.c`'s `unique_key_recheck` drives the table-AM
// and index-AM against the *live* `tg_relation` and the inserted/updated tuple's
// TID.  Those carriers are owned by the per-row AFTER-trigger firing substrate
// (`AfterTriggerExecute` re-resolves the `Relation` and materializes the OLD/NEW
// `TupleTableSlot`s), which is not yet ported — `AfterTriggerExecute` currently
// builds the `TriggerData` with `tg_relation`/`tg_trigslot`/`tg_newslot` left
// NULL and loud-panics on the per-row tuple fetch.  These owner-homed seams
// therefore have no producer yet; a call panics loudly (mirror-PG-and-panic).

seam_core::seam!(
    /// `trigdata->tg_relation` — the heap relation the trigger fired on, aliased
    /// into the caller's `mcx` (the C `Relation` pointer alias; the bumped
    /// refcount the alias represents is released when the value drops).  Driving
    /// the table-AM / `index_open` / `index_insert` against this relation is what
    /// distinguishes `unique_key_recheck` from the OID-only RI accessors above.
    pub fn tg_relation<'mcx>(
        mcx: Mcx<'mcx>,
        trigdata: TriggerDataRef,
    ) -> PgResult<rel::Relation<'mcx>>
);
seam_core::seam!(
    /// `slot->tts_tid` — the TID stored in one of the trigger manager's OLD/NEW
    /// `TupleTableSlot`s (the slot is owned by the firing substrate's `EState`,
    /// so the TID is resolved on the owner side, exactly like `slot_getattr`).
    pub fn slot_tid(
        slot: TupleTableSlotRef,
    ) -> ::types_tuple::heaptuple::ItemPointerData
);

// ---- slot value access (executor/execTuples.c, access/common) --------------

seam_core::seam!(
    /// `slot_attisnull(slot, attnum)`.
    pub fn slot_attisnull(slot: TupleTableSlotRef, attnum: i16) -> PgResult<bool>
);
seam_core::seam!(
    /// `slot_is_current_xact_tuple(slot)` (execTuples.c): is the slot's tuple
    /// from the current transaction? Can `ereport(ERROR)`, carried on `Err`.
    pub fn slot_is_current_xact_tuple(slot: TupleTableSlotRef) -> PgResult<bool>
);
seam_core::seam!(
    /// `slot_getattr(slot, attnum, &isnull)` returning `(datum, isnull)`. Can
    /// `ereport(ERROR)` deforming a tuple, carried on `Err`. The returned value
    /// is the canonical [`Datum`]; a by-reference image is copied into `mcx`
    /// (in C it points into the slot).
    pub fn slot_getattr<'mcx>(
        mcx: Mcx<'mcx>,
        slot: TupleTableSlotRef,
        attnum: i16,
    ) -> PgResult<(Datum<'mcx>, bool)>
);
seam_core::seam!(
    /// The `datum_image_eq(oldvalue, newvalue, attbyval, attlen)` test
    /// `ri_KeysEqual` uses for the PK side (consults the slot's
    /// `CompactAttribute` for `attbyval`/`attlen`).
    pub fn pk_datum_image_eq<'mcx>(
        slot: TupleTableSlotRef,
        attnum: i16,
        oldvalue: &Datum<'mcx>,
        newvalue: &Datum<'mcx>,
    ) -> bool
);

// ---- STATEMENT-trigger firing (commands/trigger.c), called by nodeModifyTable.c ----

seam_core::seam!(
    /// `ExecBSInsertTriggers(estate, relinfo)` (trigger.c): fire BEFORE
    /// STATEMENT INSERT triggers. Reads `relinfo->ri_TrigDesc` and, when
    /// `trig_insert_before_statement`, runs each before-statement trigger as
    /// user code (can `ereport(ERROR)`). `relinfo` addresses the target in
    /// `estate`'s result-rel pool.
    pub fn exec_bs_insert_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecBSUpdateTriggers(estate, relinfo)` (trigger.c): fire BEFORE
    /// STATEMENT UPDATE triggers (user code; can `ereport(ERROR)`).
    pub fn exec_bs_update_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecBSDeleteTriggers(estate, relinfo)` (trigger.c): fire BEFORE
    /// STATEMENT DELETE triggers (user code; can `ereport(ERROR)`).
    pub fn exec_bs_delete_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecASInsertTriggers(estate, relinfo, transition_capture)` (trigger.c):
    /// queue AFTER STATEMENT INSERT trigger events (with the statement-level
    /// transition capture, when present). `transition_capture` is owned by the
    /// caller's `ModifyTableState`; `None` is the C `NULL`.
    pub fn exec_as_insert_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
        transition_capture: Option<&mut nodes::modifytable::TransitionCaptureState>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecASUpdateTriggers(estate, relinfo, transition_capture)` (trigger.c):
    /// queue AFTER STATEMENT UPDATE trigger events.
    pub fn exec_as_update_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
        transition_capture: Option<&mut nodes::modifytable::TransitionCaptureState>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecASDeleteTriggers(estate, relinfo, transition_capture)` (trigger.c):
    /// queue AFTER STATEMENT DELETE trigger events.
    pub fn exec_as_delete_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
        transition_capture: Option<&mut nodes::modifytable::TransitionCaptureState>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `MakeTransitionCaptureState(trigdesc, relid, cmdType)` (trigger.c):
    /// build the `TransitionCaptureState` for `relinfo`'s trigger descriptor
    /// when transition tables are wanted for `cmd_type`, else the C `NULL`
    /// ([`None`]). The result is allocated in `mcx` (the C uses the per-query
    /// AfterTriggers `cxt`); `relinfo` names the root target in `estate`'s
    /// result-rel pool, from which the owner reads `ri_TrigDesc` and
    /// `RelationGetRelid(ri_RelationDesc)`.
    pub fn make_transition_capture_state<'mcx>(
        mcx: Mcx<'mcx>,
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
        cmd_type: nodes::nodes::CmdType,
    ) -> PgResult<Option<::mcx::PgBox<'mcx, nodes::modifytable::TransitionCaptureState>>>
);

// ---- ROW-trigger firing (commands/trigger.c), called by nodeModifyTable.c ----

seam_core::seam!(
    /// `ExecBRInsertTriggers(estate, relinfo, slot)` (trigger.c): fire BEFORE
    /// ROW INSERT triggers; returns `false` ("do nothing") to skip the insert.
    /// May replace the slot contents and `ereport(ERROR)` from user code.
    pub fn exec_br_insert_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        slot: nodes::SlotId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecIRInsertTriggers(estate, relinfo, slot)` (trigger.c): fire INSTEAD
    /// OF ROW INSERT triggers (on a view); returns `false` ("do nothing").
    pub fn exec_ir_insert_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        slot: nodes::SlotId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// Deposit the row a BEFORE/INSTEAD-OF row trigger function returned, just
    /// before the trigger-language handler returns its sentinel `Datum` from the
    /// fmgr call. `None` is the C `return NULL` ("do nothing"). The firing path
    /// (`ExecBRInsertTriggers`) takes it back within the same `ExecCallTriggerFunc`
    /// invocation. This is the owned analogue of C handing a `HeapTuple` pointer
    /// back as the fmgr `Datum` result (an opaque `usize` here cannot carry the
    /// arena pointer safely).
    pub fn set_before_trigger_result_tuple<'mcx>(
        tuple: Option<::types_tuple::heaptuple::FormedTuple<'mcx>>,
    )
);

seam_core::seam!(
    /// `return PointerGetDatum(rettuple)` where `rettuple == trigdata->tg_newtuple`
    /// — deposit the firing trigger's NEW row on the BEFORE-trigger return-tuple
    /// channel. Used by `suppress_redundant_updates_trigger` (trigfuncs.c) when
    /// the NEW row differs from OLD: the C function returns the unmodified NEW
    /// tuple so the UPDATE proceeds. Returns `false` when no NEW slot payload is
    /// installed (the analogue of a NULL `tg_newtuple` -> "do nothing").
    pub fn set_before_trigger_result_to_newtuple() -> bool
);

seam_core::seam!(
    /// `return PointerGetDatum(NULL)` — deposit the C "do nothing" sentinel on the
    /// BEFORE-trigger return-tuple channel (the firing path decodes it as no row
    /// change). Used by `suppress_redundant_updates_trigger` (trigfuncs.c) when
    /// the NEW row is byte-identical to OLD and the UPDATE is suppressed.
    pub fn set_before_trigger_result_do_nothing()
);

seam_core::seam!(
    /// `ExecARInsertTriggers(estate, relinfo, slot, recheckIndexes,
    /// transition_capture)` (trigger.c): queue AFTER ROW INSERT trigger events
    /// (and capture the NEW tuple for transition tables). The transition-capture
    /// state is owned by the caller's `ModifyTableState`; `None` is the C
    /// `NULL`.
    pub fn exec_ar_insert_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        slot: nodes::SlotId,
        recheck_indexes: &[::types_core::Oid],
        transition_capture: Option<&mut nodes::modifytable::TransitionCaptureState>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple,
    /// epqslot, tmresult, tmfd, is_merge_delete)` (trigger.c) — fire BEFORE ROW
    /// DELETE triggers, returning `false` when one of them turns the delete
    /// into a no-op. `relinfo` addresses the target in `estate`'s result-rel
    /// pool. Trigger functions are user code and can `ereport(ERROR)`.
    pub fn exec_br_delete_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        epqstate: &mut nodes::EPQState<'mcx>,
        relinfo: nodes::RriId,
        tupleid: Option<&::types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: Option<&::types_tuple::heaptuple::FormedTuple<'mcx>>,
        epqslot: Option<&mut Option<nodes::SlotId>>,
        tmresult: Option<&mut types_tableam::tableam::TM_Result>,
        tmfd: &mut types_tableam::tableam::TM_FailureData,
        is_merge_delete: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecARDeleteTriggers(estate, relinfo, tupleid, fdw_trigtuple,
    /// transition_capture, is_crosspart_update)` (trigger.c) — queue AFTER ROW
    /// DELETE triggers and capture the OLD row for transition tables.
    pub fn exec_ar_delete_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        relinfo: nodes::RriId,
        tupleid: Option<&::types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: Option<&::types_tuple::heaptuple::FormedTuple<'mcx>>,
        transition_capture: Option<&nodes::TransitionCaptureState>,
        is_crosspart_update: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// Whether the `ResultRelInfo`'s `ri_TrigDesc` has any non-cloned AFTER
    /// ROW UPDATE trigger whose function is an `RI_TRIGGER_PK` foreign-key
    /// trigger (nodeModifyTable.c's `ExecCrossPartitionUpdateForeignKey`
    /// inner loop). The trigger descriptor is owned by the relcache/trigger
    /// machinery; the owner walks `trigdesc->triggers[]`.
    pub fn has_noncloned_pk_fkey_trigger<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
    ) -> ::types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecBRUpdateTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple,
    /// newslot, tmresult, tmfd, is_merge_update)` (trigger.c): fire BEFORE ROW
    /// UPDATE triggers. Returns `false` when a trigger turns the update into a
    /// no-op; otherwise `true` (and `newslot` may have been replaced by a
    /// trigger). `tmresult`, when present, carries the trigger's `TM_Result`.
    pub fn exec_br_update_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        epqstate: &mut nodes::modifytable::EPQState<'mcx>,
        result_rel_info: nodes::RriId,
        tupleid: Option<&::types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: Option<::types_tuple::heaptuple::FormedTuple<'mcx>>,
        newslot: nodes::SlotId,
        tmresult: Option<&mut types_tableam::tableam::TM_Result>,
        tmfd: &mut types_tableam::tableam::TM_FailureData,
        is_merge_update: bool,
    ) -> ::types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecIRUpdateTriggers(estate, relinfo, trigtuple, newslot)` (trigger.c):
    /// fire INSTEAD OF ROW UPDATE triggers on a view. Returns `false` when the
    /// triggers ask for "do nothing"; otherwise `true`.
    pub fn exec_ir_update_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        trigtuple: Option<::types_tuple::heaptuple::FormedTuple<'mcx>>,
        newslot: nodes::SlotId,
    ) -> ::types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecIRDeleteTriggers(estate, relinfo, trigtuple)` (trigger.c): fire
    /// INSTEAD OF ROW DELETE triggers on a view. Returns `false` when the
    /// triggers ask for "do nothing"; otherwise `true`. User trigger code can
    /// `ereport(ERROR)`.
    pub fn exec_ir_delete_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        trigtuple: Option<::types_tuple::heaptuple::FormedTuple<'mcx>>,
    ) -> ::types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecARUpdateTriggers(estate, relinfo, src_partinfo, dst_partinfo,
    /// tupleid, fdw_trigtuple, newslot, recheckIndexes, transition_capture,
    /// is_crosspart_update)` (trigger.c): queue AFTER ROW UPDATE trigger
    /// events (and capture NEW/OLD tuples for transition tables). For a
    /// cross-partition update, `src_partinfo`/`dst_partinfo` name the source
    /// and destination partition rels; otherwise they equal `relinfo`. The
    /// transition-capture state is owned by the caller's `ModifyTableState`;
    /// `transition_capture` is lent in (the caller selects `mt_transition_capture`
    /// or `mt_oc_transition_capture`; `None` is the C `NULL`).
    pub fn exec_ar_update_triggers<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        src_partinfo: Option<nodes::RriId>,
        dst_partinfo: Option<nodes::RriId>,
        tupleid: Option<&::types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: Option<&::types_tuple::heaptuple::FormedTuple<'mcx>>,
        newslot: Option<nodes::SlotId>,
        recheck_indexes: &[::types_core::Oid],
        transition_capture: Option<&mut nodes::modifytable::TransitionCaptureState>,
        is_crosspart_update: bool,
    ) -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// `RemoveTriggerById(trigOid)` (commands/trigger.c): the per-class
    /// `OCLASS_TRIGGER` drop handler dependency.c's `doDeletion` invokes for a
    /// `pg_trigger` object. Removes the trigger's catalog row. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveTriggerById(trigOid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `renametrig(RenameStmt *stmt)` (trigger.c) — ALTER TRIGGER ... RENAME TO.
    pub fn renametrig<'mcx>(
        mcx: ::mcx::Mcx<'mcx>,
        stmt: &parsenodes::RenameStmt,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// The deferred-uniqueness-recheck `CreateTrigger` call that
    /// `index_constraint_create` (catalog/index.c) makes for a *deferrable*
    /// PRIMARY KEY / UNIQUE constraint. The C builds a fixed `CreateTrigStmt`
    /// (`unique_key_recheck`, AFTER INSERT OR UPDATE, row-level, deferrable,
    /// `initdeferred` per the constraint) and calls `CreateTrigger(stmt, NULL,
    /// relOid, InvalidOid, constraintOid, indexOid, InvalidOid, InvalidOid,
    /// NULL, is_internal=true, in_partition=false)`. This seam captures exactly
    /// those varying inputs; the trigger manager (owner) materialises the
    /// `CreateTrigStmt` and runs `CreateTrigger`. `is_primary` selects the
    /// trigger name (`PK_ConstraintTrigger` vs `Unique_ConstraintTrigger`).
    /// `Err` carries the trigger-creation `ereport(ERROR)`s.
    pub fn create_unique_key_recheck_trigger(
        rel_oid: Oid,
        constraint_oid: Oid,
        index_oid: Oid,
        is_primary: bool,
        initdeferred: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `validateForeignKeyConstraint(conname, rel, pkrel, pkindOid,
    /// constraintOid, hasperiod)` (tablecmds.c) — validate that all existing
    /// rows of the referencing relation `rel` satisfy the FK constraint, called
    /// from ALTER TABLE phase 3 (ADD CONSTRAINT validating + ALTER CONSTRAINT
    /// ENFORCED). Owned by the trigger manager because the fire-the-trigger
    /// fallback installs the current-trigger side-channel (a synthetic `Trigger`
    /// + per-row `TriggerData`) the RI procs read. It first tries the set-based
    /// `RI_Initial_Check` (single LEFT JOIN SPI query); if that cannot run
    /// (permissions/RLS) or `hasperiod` (temporal FK, no LEFT JOIN yet), it scans
    /// every row firing `RI_FKey_check_ins`. On a violation either leg raises the
    /// standard FK-violation `ereport(ERROR)` (`Err`). **Installed by
    /// `backend-commands-trigger`.**
    pub fn validate_foreign_key_constraint<'mcx>(
        mcx: Mcx<'mcx>,
        conname: &str,
        rel: &rel::Relation<'mcx>,
        pkrel: &rel::Relation<'mcx>,
        pkind_oid: Oid,
        constraint_oid: Oid,
        hasperiod: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `RI_PartitionRemove_Check(&trig, fk_rel, pk_rel)` (ri_triggers.c), as
    /// invoked from `ATDetachCheckNoForeignKeyRefs` (tablecmds.c) during DETACH
    /// PARTITION. Verifies that detaching the partition `pk_rel` (the referenced
    /// side of a parented FK identified by `constraint_oid`) does not orphan any
    /// row in the referencing table `fk_rel`. Owned by the trigger manager
    /// because it installs the current-trigger side-channel (a synthetic
    /// `Trigger` carrying the constraint identity — C's stack `Trigger trig =
    /// {0}`) the RI proc reads; the actual partition-overlap query lives in
    /// `backend-utils-adt-ri-triggers` and is reached via its own seam. On any
    /// referencing row that would be orphaned, raises the standard FK-violation
    /// `ereport(ERROR)` (`Err`). **Installed by `backend-commands-trigger`.**
    pub fn detach_partition_remove_check<'mcx>(
        mcx: Mcx<'mcx>,
        conname: &str,
        fk_rel: &rel::Relation<'mcx>,
        partition: &rel::Relation<'mcx>,
        pkind_oid: Oid,
        constraint_oid: Oid,
    ) -> PgResult<()>
);
