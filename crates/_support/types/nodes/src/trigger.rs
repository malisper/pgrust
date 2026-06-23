//! Trigger firing-context + after-trigger engine value types
//! (`commands/trigger.h` `TriggerData` / `TransitionCaptureState`, plus the
//! private after-trigger module state from `commands/trigger.c`).
//!
//! These are the F0 value types only — field-for-field vs C. The trigger firing
//! logic (`ExecBRInsertTriggers`, `AfterTriggerExecute`, …) is F1 and lands with
//! the `backend-commands-trigger` owner.
//!
//! The per-relation [`Trigger`]/[`TriggerDesc`] types live one layer down in the
//! [`types_trigger`] crate (mirroring C's separate `reltrigger.h`) so `rel.h`'s
//! `RelationData.rd_trigdesc` can name them without a cycle; this module re-uses
//! them for the firing context.

use mcx::{MemoryContext, PgBox, PgVec};
use ::types_core::primitive::Oid;
use ::types_core::xact::CommandId;
use ::types_tuple::heaptuple::{HeapTuple, ItemPointerData};

use crate::bitmapset::Bitmapset;
use crate::execnodes::SlotId;
use crate::funcapi::Tuplestorestate;
use crate::nodes::{CmdType, NodeTag};
use ::types_trigger::Trigger;

/// `TriggerEvent` (`commands/trigger.h`) — a `uint32` carrying the event-type +
/// timing + row/statement bits (see the `TRIGGER_EVENT_*` consts).
pub type TriggerEvent = u32;

/// `T_TriggerData` (nodetags.h) — the `NodeTag` of [`TriggerData`].
pub const T_TriggerData: NodeTag = NodeTag(442);

// --- TriggerEvent bit flags (commands/trigger.h) -----------------------------

/// `TRIGGER_EVENT_INSERT`.
pub const TRIGGER_EVENT_INSERT: TriggerEvent = 0x00000000;
/// `TRIGGER_EVENT_DELETE`.
pub const TRIGGER_EVENT_DELETE: TriggerEvent = 0x00000001;
/// `TRIGGER_EVENT_UPDATE`.
pub const TRIGGER_EVENT_UPDATE: TriggerEvent = 0x00000002;
/// `TRIGGER_EVENT_TRUNCATE`.
pub const TRIGGER_EVENT_TRUNCATE: TriggerEvent = 0x00000003;
/// `TRIGGER_EVENT_OPMASK`.
pub const TRIGGER_EVENT_OPMASK: TriggerEvent = 0x00000003;
/// `TRIGGER_EVENT_ROW`.
pub const TRIGGER_EVENT_ROW: TriggerEvent = 0x00000004;
/// `TRIGGER_EVENT_BEFORE`.
pub const TRIGGER_EVENT_BEFORE: TriggerEvent = 0x00000008;
/// `TRIGGER_EVENT_AFTER`.
pub const TRIGGER_EVENT_AFTER: TriggerEvent = 0x00000000;
/// `TRIGGER_EVENT_INSTEAD`.
pub const TRIGGER_EVENT_INSTEAD: TriggerEvent = 0x00000010;
/// `TRIGGER_EVENT_TIMINGMASK`.
pub const TRIGGER_EVENT_TIMINGMASK: TriggerEvent = 0x00000018;
/// `AFTER_TRIGGER_DEFERRABLE` — used only within trigger.c.
pub const AFTER_TRIGGER_DEFERRABLE: TriggerEvent = 0x00000020;
/// `AFTER_TRIGGER_INITDEFERRED` — used only within trigger.c.
pub const AFTER_TRIGGER_INITDEFERRED: TriggerEvent = 0x00000040;

/// `TriggerData` (`commands/trigger.h`) — the `fcinfo->context` for a trigger
/// function invocation: which event, on which relation, with which tuples.
///
/// `Relation tg_relation` is a pointer alias [`rel::Relation`] (`None` is
/// the C NULL); `HeapTuple` fields use the owned [`HeapTuple`] carrier;
/// `TupleTableSlot *` are [`SlotId`] into the EState slot table;
/// `Tuplestorestate *` are the owned transition-table stores; the
/// `const Bitmapset *tg_updatedcols` is the owned [`Bitmapset`] (`None` = NULL).
#[derive(Debug)]
pub struct TriggerData<'mcx> {
    /// `NodeTag type` — always [`T_TriggerData`].
    pub type_: NodeTag,
    /// `TriggerEvent tg_event`.
    pub tg_event: TriggerEvent,
    /// `Relation tg_relation`.
    pub tg_relation: Option<rel::Relation<'mcx>>,
    /// `HeapTuple tg_trigtuple` — the OLD tuple (DELETE/UPDATE), or NULL.
    pub tg_trigtuple: HeapTuple<'mcx>,
    /// `HeapTuple tg_newtuple` — the NEW tuple (UPDATE), or NULL.
    pub tg_newtuple: HeapTuple<'mcx>,
    /// `Trigger *tg_trigger` — the firing trigger.
    pub tg_trigger: Option<PgBox<'mcx, Trigger<'mcx>>>,
    /// `TupleTableSlot *tg_trigslot` — slot holding `tg_trigtuple`, or NULL.
    pub tg_trigslot: Option<SlotId>,
    /// `TupleTableSlot *tg_newslot` — slot holding `tg_newtuple`, or NULL.
    pub tg_newslot: Option<SlotId>,
    /// `Tuplestorestate *tg_oldtable` — OLD transition table, or NULL.
    pub tg_oldtable: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `Tuplestorestate *tg_newtable` — NEW transition table, or NULL.
    pub tg_newtable: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `const Bitmapset *tg_updatedcols` — columns updated (UPDATE), or NULL.
    pub tg_updatedcols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

/// `TransitionCaptureState` (`commands/trigger.h`) — per-caller state for
/// capturing OLD/NEW tuples into transition tables for one ModifyTable node (or
/// COPY). The `tcs_*_private` pointers reference shared [`AfterTriggersTableData`]
/// (private in trigger.c).
#[derive(Debug)]
pub struct TransitionCaptureState<'mcx> {
    /// `bool tcs_delete_old_table`.
    pub tcs_delete_old_table: bool,
    /// `bool tcs_update_old_table`.
    pub tcs_update_old_table: bool,
    /// `bool tcs_update_new_table`.
    pub tcs_update_new_table: bool,
    /// `bool tcs_insert_new_table`.
    pub tcs_insert_new_table: bool,
    /// `TupleTableSlot *tcs_original_insert_tuple` — original (parent-format)
    /// insert tuple to bypass child→parent conversion, or NULL.
    pub tcs_original_insert_tuple: Option<SlotId>,
    /// `struct AfterTriggersTableData *tcs_insert_private`.
    pub tcs_insert_private: Option<PgBox<'mcx, AfterTriggersTableData<'mcx>>>,
    /// `struct AfterTriggersTableData *tcs_update_private`.
    pub tcs_update_private: Option<PgBox<'mcx, AfterTriggersTableData<'mcx>>>,
    /// `struct AfterTriggersTableData *tcs_delete_private`.
    pub tcs_delete_private: Option<PgBox<'mcx, AfterTriggersTableData<'mcx>>>,
}

// =============================================================================
// After-trigger engine (private to commands/trigger.c)
// =============================================================================

/// `TriggerFlags` (`commands/trigger.c`) — a `uint32` of status bits + the
/// offset to the shared data (see the `AFTER_TRIGGER_*` consts).
pub type TriggerFlags = u32;

/// `AFTER_TRIGGER_OFFSET` — must be low-order bits.
pub const AFTER_TRIGGER_OFFSET: TriggerFlags = 0x07FFFFFF;
/// `AFTER_TRIGGER_DONE`.
pub const AFTER_TRIGGER_DONE: TriggerFlags = 0x80000000;
/// `AFTER_TRIGGER_IN_PROGRESS`.
pub const AFTER_TRIGGER_IN_PROGRESS: TriggerFlags = 0x40000000;
/// `AFTER_TRIGGER_FDW_REUSE`.
pub const AFTER_TRIGGER_FDW_REUSE: TriggerFlags = 0x00000000;
/// `AFTER_TRIGGER_FDW_FETCH`.
pub const AFTER_TRIGGER_FDW_FETCH: TriggerFlags = 0x20000000;
/// `AFTER_TRIGGER_1CTID`.
pub const AFTER_TRIGGER_1CTID: TriggerFlags = 0x10000000;
/// `AFTER_TRIGGER_2CTID`.
pub const AFTER_TRIGGER_2CTID: TriggerFlags = 0x30000000;
/// `AFTER_TRIGGER_CP_UPDATE`.
pub const AFTER_TRIGGER_CP_UPDATE: TriggerFlags = 0x08000000;
/// `AFTER_TRIGGER_TUP_BITS`.
pub const AFTER_TRIGGER_TUP_BITS: TriggerFlags = 0x38000000;

/// `SetConstraintTriggerData` (`commands/trigger.c`) — per-trigger SET CONSTRAINT
/// status.
#[derive(Clone, Copy, Debug)]
pub struct SetConstraintTriggerData {
    /// `Oid sct_tgoid`.
    pub sct_tgoid: Oid,
    /// `bool sct_tgisdeferred`.
    pub sct_tgisdeferred: bool,
}

/// `SetConstraintStateData` (`commands/trigger.c`) — SET CONSTRAINT
/// intra-transaction status. The C `trigstates[FLEXIBLE_ARRAY_MEMBER]` becomes an
/// owned [`PgVec`]; `numstates`/`numalloc` are retained field-for-field
/// (`numstates == trigstates.len()`; `numalloc == trigstates.capacity()`).
#[derive(Debug)]
pub struct SetConstraintStateData<'mcx> {
    /// `bool all_isset`.
    pub all_isset: bool,
    /// `bool all_isdeferred`.
    pub all_isdeferred: bool,
    /// `int numstates` — number of `trigstates[]` entries in use.
    pub numstates: i32,
    /// `int numalloc` — allocated size of `trigstates[]`.
    pub numalloc: i32,
    /// `SetConstraintTriggerData trigstates[FLEXIBLE_ARRAY_MEMBER]`.
    pub trigstates: PgVec<'mcx, SetConstraintTriggerData>,
}

/// `SetConstraintState` (`commands/trigger.c`) — `SetConstraintStateData *`.
pub type SetConstraintState<'mcx> = Option<PgBox<'mcx, SetConstraintStateData<'mcx>>>;

/// `AfterTriggerSharedData` (`commands/trigger.c`) — data shared across all
/// instances of similar events within a chunk.
#[derive(Debug)]
pub struct AfterTriggerSharedData<'mcx> {
    /// `TriggerEvent ats_event` — event type indicator.
    pub ats_event: TriggerEvent,
    /// `Oid ats_tgoid` — the trigger's ID.
    pub ats_tgoid: Oid,
    /// `Oid ats_relid` — the relation it's on.
    pub ats_relid: Oid,
    /// `Oid ats_rolid` — role to execute the trigger.
    pub ats_rolid: Oid,
    /// `CommandId ats_firing_id` — ID for firing cycle.
    pub ats_firing_id: CommandId,
    /// `struct AfterTriggersTableData *ats_table` — transition table access.
    pub ats_table: Option<PgBox<'mcx, AfterTriggersTableData<'mcx>>>,
    /// `Bitmapset *ats_modifiedcols` — modified columns.
    pub ats_modifiedcols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

/// `AfterTriggerSharedData *` (`commands/trigger.c`).
pub type AfterTriggerShared<'mcx> = Option<PgBox<'mcx, AfterTriggerSharedData<'mcx>>>;

/// `AfterTriggerEventData` (`commands/trigger.c`) — the per-event record:
/// status bits + offset to shared data, up to two tuple CTIDs, and optionally
/// the source/destination partition OIDs for a cross-partition update.
#[derive(Clone, Copy, Debug)]
pub struct AfterTriggerEventData {
    /// `TriggerFlags ate_flags` — status bits and offset to shared data.
    pub ate_flags: TriggerFlags,
    /// `ItemPointerData ate_ctid1` — inserted/deleted/old-updated tuple.
    pub ate_ctid1: ItemPointerData,
    /// `ItemPointerData ate_ctid2` — new updated tuple.
    pub ate_ctid2: ItemPointerData,
    /// `Oid ate_src_part` — source partition (cross-partition update).
    pub ate_src_part: Oid,
    /// `Oid ate_dst_part` — destination partition (cross-partition update).
    pub ate_dst_part: Oid,
}

/// `AfterTriggerEvent` (`commands/trigger.c`) — `AfterTriggerEventData *`.
pub type AfterTriggerEvent<'mcx> = Option<PgBox<'mcx, AfterTriggerEventData>>;

/// `AfterTriggerEventChunk` (`commands/trigger.c`) — a chunk of the expansible
/// event array. Event data records occupy `[CHUNK_DATA_START, freeptr)`; shared
/// records occupy `[endfree, endptr)`. The C `char *` cursors into the chunk's
/// own storage become byte offsets (`usize`) plus the owned backing buffer.
#[derive(Debug)]
pub struct AfterTriggerEventChunk<'mcx> {
    /// `struct AfterTriggerEventChunk *next` — list link.
    pub next: Option<PgBox<'mcx, AfterTriggerEventChunk<'mcx>>>,
    /// `char *freeptr` — start of free space in chunk (offset into `data`).
    pub freeptr: usize,
    /// `char *endfree` — end of free space in chunk (offset into `data`).
    pub endfree: usize,
    /// `char *endptr` — end of chunk (offset into `data`).
    pub endptr: usize,
    /// The chunk's event/shared storage ("event data follows here" in C).
    pub data: PgVec<'mcx, u8>,
}

/// `AfterTriggerEventList` (`commands/trigger.c`) — a list of event chunks. The
/// C `head`/`tail` `AfterTriggerEventChunk *` aliases and the `tailfree` cursor
/// are kept; the chunks themselves are owned through `head`'s `next` links.
#[derive(Debug, Default)]
pub struct AfterTriggerEventList {
    /// `AfterTriggerEventChunk *head` — present (`true`) iff the list is
    /// non-empty (the owned chunks live off [`AfterTriggersData`]'s storage).
    pub head: bool,
    /// `AfterTriggerEventChunk *tail`.
    pub tail: bool,
    /// `char *tailfree` — `freeptr` of the tail chunk (offset).
    pub tailfree: usize,
}

/// `AfterTriggersQueryData` (`commands/trigger.c`) — per-query-level data.
#[derive(Debug)]
pub struct AfterTriggersQueryData<'mcx> {
    /// `AfterTriggerEventList events` — events pending from this query.
    pub events: AfterTriggerEventList,
    /// `Tuplestorestate *fdw_tuplestore` — foreign tuples for said events.
    pub fdw_tuplestore: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `List *tables` — list of [`AfterTriggersTableData`].
    pub tables: PgVec<'mcx, AfterTriggersTableData<'mcx>>,
}

/// `AfterTriggersTransData` (`commands/trigger.c`) — per-subtransaction-level
/// data, used to reset state at subtransaction abort.
#[derive(Debug)]
pub struct AfterTriggersTransData<'mcx> {
    /// `SetConstraintState state` — saved S C state, or NULL if not yet saved.
    pub state: SetConstraintState<'mcx>,
    /// `AfterTriggerEventList events` — saved list pointer.
    pub events: AfterTriggerEventList,
    /// `int query_depth` — saved query_depth.
    pub query_depth: i32,
    /// `CommandId firing_counter` — saved firing_counter.
    pub firing_counter: CommandId,
}

/// `AfterTriggersTableData` (`commands/trigger.c`) — per-(relid, cmdType)
/// transition-table + statement-trigger bookkeeping.
#[derive(Debug)]
pub struct AfterTriggersTableData<'mcx> {
    /// `Oid relid` — target table's OID.
    pub relid: Oid,
    /// `CmdType cmdType` — event type (CMD_INSERT/UPDATE/DELETE).
    pub cmdType: CmdType,
    /// `bool closed` — true when no longer OK to add tuples.
    pub closed: bool,
    /// `bool before_trig_done` — did we already queue BS triggers?
    pub before_trig_done: bool,
    /// `bool after_trig_done` — did we already queue AS triggers?
    pub after_trig_done: bool,
    /// `AfterTriggerEventList after_trig_events` — saved list pointer.
    pub after_trig_events: AfterTriggerEventList,
    /// `Tuplestorestate *old_tuplestore` — OLD transition table, or NULL.
    pub old_tuplestore: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `Tuplestorestate *new_tuplestore` — NEW transition table, or NULL.
    pub new_tuplestore: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `TupleTableSlot *storeslot` — for converting to tuplestore's format.
    pub storeslot: Option<SlotId>,
}

/// `AfterTriggersData` (`commands/trigger.c`) — all per-transaction state for
/// the AFTER TRIGGERS module. The single C static `afterTriggers` is mirrored as
/// the [`AFTER_TRIGGERS`] thread-local.
#[derive(Debug)]
pub struct AfterTriggersData<'mcx> {
    /// `CommandId firing_counter` — next firing ID to assign.
    pub firing_counter: CommandId,
    /// `SetConstraintState state` — the active S C state.
    pub state: SetConstraintState<'mcx>,
    /// `AfterTriggerEventList events` — deferred-event list.
    pub events: AfterTriggerEventList,
    /// `MemoryContext event_cxt` — memory context for events, if any.
    pub event_cxt: Option<MemoryContext>,
    /// `AfterTriggersQueryData *query_stack` — per-query-level data array.
    pub query_stack: Option<PgVec<'mcx, AfterTriggersQueryData<'mcx>>>,
    /// `int query_depth` — current index in `query_stack` (-1 when empty).
    pub query_depth: i32,
    /// `int maxquerydepth` — allocated length of `query_stack`.
    pub maxquerydepth: i32,
    /// `AfterTriggersTransData *trans_stack` — per-subtransaction-level data.
    pub trans_stack: Option<PgVec<'mcx, AfterTriggersTransData<'mcx>>>,
    /// `int maxtransdepth` — allocated length of `trans_stack`.
    pub maxtransdepth: i32,
}

impl<'mcx> AfterTriggersData<'mcx> {
    /// The zero-initialized state of the C static `afterTriggers` before
    /// `AfterTriggerBeginXact` runs (C zeroes the BSS static; `query_depth`
    /// reaches -1 only once a transaction begins, but the unstarted static is
    /// all-zero — callers must `AfterTriggerBeginXact` before use).
    pub fn empty() -> Self {
        AfterTriggersData {
            firing_counter: 0,
            state: None,
            events: AfterTriggerEventList::default(),
            event_cxt: None,
            query_stack: None,
            query_depth: 0,
            maxquerydepth: 0,
            trans_stack: None,
            maxtransdepth: 0,
        }
    }
}

// `static AfterTriggersData afterTriggers` (`commands/trigger.c`) — the
// backend-global AFTER-trigger state. The C static is instantiated as a
// `thread_local!` in the F1 owner crate (`backend-commands-trigger`, which is a
// `std` crate, mirroring the xact.c statics convention of one `thread_local!`
// per backend-global). It is NOT instantiated here: `types-nodes` is `no_std`
// (the `thread_local!` macro requires `std`), and per the repo convention
// backend-global statics live with their owner, not in a leaf type crate. F0
// supplies the value type [`AfterTriggersData`] (with [`AfterTriggersData::empty`]
// for the zeroed BSS initial state); F1 declares
// `thread_local! { static AFTER_TRIGGERS: RefCell<AfterTriggersData<'static>> =
// RefCell::new(AfterTriggersData::empty()); }` and drives it.
