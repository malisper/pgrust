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

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_ri_triggers::{TriggerDataRef, TriggerRef, TupleTableSlotRef};

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
    /// `ereport(ERROR)` deforming a tuple, carried on `Err`.
    pub fn slot_getattr(slot: TupleTableSlotRef, attnum: i16) -> PgResult<(Datum, bool)>
);
seam_core::seam!(
    /// The `datum_image_eq(oldvalue, newvalue, attbyval, attlen)` test
    /// `ri_KeysEqual` uses for the PK side (consults the slot's
    /// `CompactAttribute` for `attbyval`/`attlen`).
    pub fn pk_datum_image_eq(
        slot: TupleTableSlotRef,
        attnum: i16,
        oldvalue: Datum,
        newvalue: Datum,
    ) -> bool
);
