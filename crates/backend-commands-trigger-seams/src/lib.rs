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

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_ri_triggers::{TriggerDataRef, TriggerRef, TupleTableSlotRef};

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
    ) -> PgResult<Option<types_tuple::heaptuple::HeapTupleData<'mcx>>>
);
seam_core::seam!(
    /// `trigdata->tg_newtuple` — the NEW `HeapTuple` (for an UPDATE) the trigger
    /// manager handed the trigger function, copied into `mcx`. `Ok(None)`
    /// mirrors a NULL `tg_newtuple`.
    pub fn tg_newtuple<'mcx>(
        mcx: Mcx<'mcx>,
        trigdata: TriggerDataRef,
    ) -> PgResult<Option<types_tuple::heaptuple::HeapTupleData<'mcx>>>
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
    ) -> PgResult<types_rel::Relation<'mcx>>
);
seam_core::seam!(
    /// `slot->tts_tid` — the TID stored in one of the trigger manager's OLD/NEW
    /// `TupleTableSlot`s (the slot is owned by the firing substrate's `EState`,
    /// so the TID is resolved on the owner side, exactly like `slot_getattr`).
    pub fn slot_tid(
        slot: TupleTableSlotRef,
    ) -> types_tuple::heaptuple::ItemPointerData
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
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecBSUpdateTriggers(estate, relinfo)` (trigger.c): fire BEFORE
    /// STATEMENT UPDATE triggers (user code; can `ereport(ERROR)`).
    pub fn exec_bs_update_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecBSDeleteTriggers(estate, relinfo)` (trigger.c): fire BEFORE
    /// STATEMENT DELETE triggers (user code; can `ereport(ERROR)`).
    pub fn exec_bs_delete_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecASInsertTriggers(estate, relinfo, transition_capture)` (trigger.c):
    /// queue AFTER STATEMENT INSERT trigger events (with the statement-level
    /// transition capture, when present). `transition_capture` is owned by the
    /// caller's `ModifyTableState`; `None` is the C `NULL`.
    pub fn exec_as_insert_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
        transition_capture: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecASUpdateTriggers(estate, relinfo, transition_capture)` (trigger.c):
    /// queue AFTER STATEMENT UPDATE trigger events.
    pub fn exec_as_update_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
        transition_capture: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecASDeleteTriggers(estate, relinfo, transition_capture)` (trigger.c):
    /// queue AFTER STATEMENT DELETE trigger events.
    pub fn exec_as_delete_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
        transition_capture: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
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
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
        cmd_type: types_nodes::nodes::CmdType,
    ) -> PgResult<Option<mcx::PgBox<'mcx, types_nodes::modifytable::TransitionCaptureState>>>
);

// ---- ROW-trigger firing (commands/trigger.c), called by nodeModifyTable.c ----

seam_core::seam!(
    /// `ExecBRInsertTriggers(estate, relinfo, slot)` (trigger.c): fire BEFORE
    /// ROW INSERT triggers; returns `false` ("do nothing") to skip the insert.
    /// May replace the slot contents and `ereport(ERROR)` from user code.
    pub fn exec_br_insert_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecIRInsertTriggers(estate, relinfo, slot)` (trigger.c): fire INSTEAD
    /// OF ROW INSERT triggers (on a view); returns `false` ("do nothing").
    pub fn exec_ir_insert_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ExecARInsertTriggers(estate, relinfo, slot, recheckIndexes,
    /// transition_capture)` (trigger.c): queue AFTER ROW INSERT trigger events
    /// (and capture the NEW tuple for transition tables). The transition-capture
    /// state is owned by the caller's `ModifyTableState`; `None` is the C
    /// `NULL`.
    pub fn exec_ar_insert_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
        recheck_indexes: &[types_core::Oid],
        transition_capture: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple,
    /// epqslot, tmresult, tmfd, is_merge_delete)` (trigger.c) — fire BEFORE ROW
    /// DELETE triggers, returning `false` when one of them turns the delete
    /// into a no-op. `relinfo` addresses the target in `estate`'s result-rel
    /// pool. Trigger functions are user code and can `ereport(ERROR)`.
    pub fn exec_br_delete_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        epqstate: &mut types_nodes::EPQState<'mcx>,
        relinfo: types_nodes::RriId,
        tupleid: Option<&types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: Option<&types_tuple::heaptuple::HeapTupleData<'mcx>>,
        epqslot: Option<&mut Option<types_nodes::SlotId>>,
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
        estate: &mut types_nodes::EStateData<'mcx>,
        relinfo: types_nodes::RriId,
        tupleid: Option<&types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: Option<&types_tuple::heaptuple::HeapTupleData<'mcx>>,
        transition_capture: Option<&types_nodes::TransitionCaptureState>,
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
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecBRUpdateTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple,
    /// newslot, tmresult, tmfd, is_merge_update)` (trigger.c): fire BEFORE ROW
    /// UPDATE triggers. Returns `false` when a trigger turns the update into a
    /// no-op; otherwise `true` (and `newslot` may have been replaced by a
    /// trigger). `tmresult`, when present, carries the trigger's `TM_Result`.
    pub fn exec_br_update_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
        result_rel_info: types_nodes::RriId,
        tupleid: Option<&types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: types_tuple::heaptuple::HeapTuple<'mcx>,
        newslot: types_nodes::SlotId,
        tmresult: Option<&mut types_tableam::tableam::TM_Result>,
        tmfd: &mut types_tableam::tableam::TM_FailureData,
        is_merge_update: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecIRUpdateTriggers(estate, relinfo, trigtuple, newslot)` (trigger.c):
    /// fire INSTEAD OF ROW UPDATE triggers on a view. Returns `false` when the
    /// triggers ask for "do nothing"; otherwise `true`.
    pub fn exec_ir_update_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        trigtuple: types_tuple::heaptuple::HeapTuple<'mcx>,
        newslot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ExecIRDeleteTriggers(estate, relinfo, trigtuple)` (trigger.c): fire
    /// INSTEAD OF ROW DELETE triggers on a view. Returns `false` when the
    /// triggers ask for "do nothing"; otherwise `true`. User trigger code can
    /// `ereport(ERROR)`.
    pub fn exec_ir_delete_triggers<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        trigtuple: types_tuple::heaptuple::HeapTuple<'mcx>,
    ) -> types_error::PgResult<bool>
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
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        src_partinfo: Option<types_nodes::RriId>,
        dst_partinfo: Option<types_nodes::RriId>,
        tupleid: Option<&types_tuple::heaptuple::ItemPointerData>,
        fdw_trigtuple: Option<&types_tuple::heaptuple::HeapTupleData<'mcx>>,
        newslot: Option<types_nodes::SlotId>,
        recheck_indexes: &[types_core::Oid],
        transition_capture: Option<&mut types_nodes::modifytable::TransitionCaptureState>,
        is_crosspart_update: bool,
    ) -> types_error::PgResult<()>
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
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_parsenodes::RenameStmt,
    ) -> PgResult<types_catalog::catalog_dependency::ObjectAddress>
);
