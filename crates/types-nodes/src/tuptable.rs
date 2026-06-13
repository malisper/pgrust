//! Tuple-table-slot payload model (executor/tuptable.h) — the runtime slot
//! data carrier that `backend-executor-execTuples` owns and mutates.
//!
//! # Why this lives alongside the trimmed [`crate::executor::TupleTableSlot`]
//!
//! [`crate::executor::TupleTableSlot`] is the *header* projection that 70+
//! ports already consume (`TTS_EMPTY`/`TTS_FIXED` tests, the `tts_ops`
//! identity token, `tts_tid`, `tts_tableOid`). C's real `TupleTableSlot`
//! additionally carries the per-attribute `tts_values`/`tts_isnull` arrays,
//! the `tts_nvalid` deform watermark, the `tts_tupleDescriptor`, and the
//! slot's own `tts_mcxt`. Those payload fields, and the four concrete
//! `Virtual/Heap/Minimal/BufferHeap` superstructures that downcast off the
//! base, land here as [`SlotData`] — the live slot type the executor stores
//! in `EState::es_tupleTable`.
//!
//! Mirroring C exactly (tuptable.h):
//!
//! * `VirtualTupleTableSlot { TupleTableSlot base; char *data; }`
//! * `HeapTupleTableSlot   { TupleTableSlot base; HeapTuple tuple; uint32 off;
//!    HeapTupleData tupdata; }`
//! * `BufferHeapTupleTableSlot { HeapTupleTableSlot base; Buffer buffer; }`
//! * `MinimalTupleTableSlot { TupleTableSlot base; HeapTuple tuple;
//!    MinimalTuple mintuple; HeapTupleData minhdr; uint32 off; }`
//!
//! The `tts_values`/`tts_isnull`/`tts_nvalid`/`tts_tupleDescriptor`/`tts_mcxt`
//! fields that C puts directly in `TupleTableSlot` are carried here in
//! [`SlotBase`] (paired with the trimmed header) so the existing header type
//! can stay unchanged for its current consumers. When the slot payload model
//! is fully assembled the header and [`SlotBase`] re-converge.
//!
//! SCAFFOLD: data carrier only. The per-kind dispatch callbacks
//! (clear/materialize/getsomeattrs/copyslot/store/…) live in
//! `backend-executor-execTuples`; this crate is `#![no_std]` and seam-free, so
//! only the owned structs + pure accessors live here.

use mcx::{Mcx, PgVec};
use types_core::primitive::{AttrNumber, Oid, Size};
use types_datum::Datum;
use types_error::PgResult;
use types_storage::buf::Buffer;
use types_tuple::heaptuple::{HeapTuple, HeapTupleData, ItemPointerData, MinimalTuple, TupleDesc};

use crate::executor::{TupleSlotKind, TupleTableSlot, TTS_FLAG_EMPTY};

/// `TTS_FLAG_SHOULDFREE` (tuptable.h) — true if we should `pfree` the slot's
/// stored tuple on clear.
pub const TTS_FLAG_SHOULDFREE: u16 = 1 << 0;
/// `TTS_FLAG_SLOW` (tuptable.h) — saved state for `slot_deform_heap_tuple`.
pub const TTS_FLAG_SLOW: u16 = 1 << 2;
/// `TTS_FLAG_FIXED` (tuptable.h) — fixed tuple descriptor.
pub const TTS_FLAG_FIXED: u16 = 1 << 4;

/// The payload C carries directly in `TupleTableSlot` but which the trimmed
/// [`TupleTableSlot`] header omits: the per-attribute value/null arrays, the
/// deform watermark, the slot's descriptor, and the memory context the slot
/// lives in (`tts_mcxt`). Paired with the header to form the full base slot.
///
/// SCAFFOLD: shape mirrors tuptable.h field-for-field; bodies that fill it land
/// in `backend-executor-execTuples`.
#[derive(Debug)]
pub struct SlotBase<'mcx> {
    /// `TupleTableSlot` header bits (the projection ports already share):
    /// `tts_flags`, `tts_ops`, `tts_tid`, `tts_tableOid`.
    pub header: TupleTableSlot,
    /// `AttrNumber tts_nvalid` — # of valid values in `tts_values`.
    pub tts_nvalid: AttrNumber,
    /// `TupleDesc tts_tupleDescriptor` — slot's tuple descriptor.
    pub tts_tupleDescriptor: TupleDesc<'mcx>,
    /// `Datum *tts_values` — current per-attribute values.
    pub tts_values: PgVec<'mcx, Datum>,
    /// `bool *tts_isnull` — current per-attribute isnull flags.
    pub tts_isnull: PgVec<'mcx, bool>,
}

impl<'mcx> SlotBase<'mcx> {
    /// `TTS_EMPTY(slot)`.
    pub fn is_empty(&self) -> bool {
        self.header.tts_flags & TTS_FLAG_EMPTY != 0
    }

    /// `TTS_SHOULDFREE(slot)`.
    pub fn should_free(&self) -> bool {
        self.header.tts_flags & TTS_FLAG_SHOULDFREE != 0
    }

    /// `TTS_FIXED(slot)`.
    pub fn is_fixed(&self) -> bool {
        self.header.tts_flags & TTS_FLAG_FIXED != 0
    }

    /// `slot->tts_flags |= TTS_EMPTY; slot->tts_flags &= ~TTS_SHOULDFREE;
    /// slot->tts_nvalid = 0;` (the `*_clear` callback tail).
    pub fn mark_empty(&mut self) {
        self.header.tts_flags |= TTS_FLAG_EMPTY;
        self.header.tts_flags &= !TTS_FLAG_SHOULDFREE;
        self.tts_nvalid = 0;
    }

    /// `slot->tts_flags &= ~TTS_EMPTY;`
    pub fn mark_not_empty(&mut self) {
        self.header.tts_flags &= !TTS_FLAG_EMPTY;
    }
}

/// `VirtualTupleTableSlot` (tuptable.h).
#[derive(Debug)]
pub struct VirtualTupleTableSlot<'mcx> {
    /// `TupleTableSlot base;`
    pub base: SlotBase<'mcx>,
    /// `char *data;` — data for materialized slots.
    pub data: PgVec<'mcx, u8>,
}

/// `HeapTupleTableSlot` (tuptable.h).
#[derive(Debug)]
pub struct HeapTupleTableSlot<'mcx> {
    /// `TupleTableSlot base;`
    pub base: SlotBase<'mcx>,
    /// `HeapTuple tuple;` — physical tuple.
    pub tuple: HeapTuple<'mcx>,
    /// `uint32 off;` — saved state for `slot_deform_heap_tuple`.
    pub off: u32,
    /// `HeapTupleData tupdata;` — optional workspace for storing tuple.
    pub tupdata: HeapTupleData<'mcx>,
}

/// `BufferHeapTupleTableSlot` (tuptable.h).
#[derive(Debug)]
pub struct BufferHeapTupleTableSlot<'mcx> {
    /// `HeapTupleTableSlot base;`
    pub base: HeapTupleTableSlot<'mcx>,
    /// `Buffer buffer;` — tuple's buffer, or `InvalidBuffer`.
    pub buffer: Buffer,
}

/// `MinimalTupleTableSlot` (tuptable.h).
#[derive(Debug)]
pub struct MinimalTupleTableSlot<'mcx> {
    /// `TupleTableSlot base;`
    pub base: SlotBase<'mcx>,
    /// `HeapTuple tuple;` — tuple wrapper (points at `minhdr`).
    pub tuple: HeapTuple<'mcx>,
    /// `MinimalTuple mintuple;` — minimal tuple, or NULL if none.
    pub mintuple: MinimalTuple<'mcx>,
    /// `HeapTupleData minhdr;` — workspace for minimal-tuple-only case.
    pub minhdr: HeapTupleData<'mcx>,
    /// `uint32 off;` — saved state for `slot_deform_heap_tuple`.
    pub off: u32,
}

/// A live tuple table slot: one of the four owned subtype structs (the
/// idiomatic replacement for a `TupleTableSlot *` that may actually point at a
/// `Virtual/Heap/Minimal/BufferHeap` superstructure). The execTuples callbacks
/// downcast through this enum exactly as the C casts the pointer.
#[derive(Debug)]
pub enum SlotData<'mcx> {
    Virtual(VirtualTupleTableSlot<'mcx>),
    Heap(HeapTupleTableSlot<'mcx>),
    Minimal(MinimalTupleTableSlot<'mcx>),
    BufferHeap(BufferHeapTupleTableSlot<'mcx>),
}

impl<'mcx> SlotData<'mcx> {
    /// `TTS_IS_*` classification (`slot->tts_ops` identity).
    pub fn kind(&self) -> TupleSlotKind {
        match self {
            SlotData::Virtual(_) => TupleSlotKind::Virtual,
            SlotData::Heap(_) => TupleSlotKind::HeapTuple,
            SlotData::Minimal(_) => TupleSlotKind::MinimalTuple,
            SlotData::BufferHeap(_) => TupleSlotKind::BufferHeapTuple,
        }
    }

    /// The shared [`SlotBase`] (`&slot->base` after the appropriate downcast;
    /// for the buffer slot this is `slot->base.base`).
    pub fn base(&self) -> &SlotBase<'mcx> {
        match self {
            SlotData::Virtual(s) => &s.base,
            SlotData::Heap(s) => &s.base,
            SlotData::Minimal(s) => &s.base,
            SlotData::BufferHeap(s) => &s.base.base,
        }
    }

    /// Mutable [`SlotBase`].
    pub fn base_mut(&mut self) -> &mut SlotBase<'mcx> {
        match self {
            SlotData::Virtual(s) => &mut s.base,
            SlotData::Heap(s) => &mut s.base,
            SlotData::Minimal(s) => &mut s.base,
            SlotData::BufferHeap(s) => &mut s.base.base,
        }
    }
}

/// `TupleTableSlotOps` (tuptable.h) — the per-kind callback table. In the
/// owned model the callbacks are dispatched through [`SlotData::kind`]; this
/// struct records the C metadata (`base_slot_size`, which optional callbacks
/// the kind implements) consumed by `MakeTupleTableSlot`/`ExecCopySlot`.
///
/// SCAFFOLD: the dispatch lives in `backend-executor-execTuples`; this carrier
/// records only the data those callbacks key off.
#[derive(Clone, Copy, Debug)]
pub struct TupleTableSlotOps {
    /// `size_t base_slot_size` — size of the concrete slot struct.
    pub base_slot_size: Size,
    /// Whether the kind implements `get_heap_tuple` (NULL for virtual/minimal).
    pub has_get_heap_tuple: bool,
    /// Whether the kind implements `get_minimal_tuple` (NULL for
    /// virtual/heap/buffer).
    pub has_get_minimal_tuple: bool,
}

/// `ItemPointerSetInvalid(&slot->tts_tid)` — block = `InvalidBlockNumber`,
/// offset = `InvalidOffsetNumber`.
pub fn item_pointer_invalid() -> ItemPointerData {
    ItemPointerData::default()
}

/// `AttInMetadata` (funcapi.h) — metadata for `BuildTupleFromCStrings`.
///
/// SCAFFOLD: shape mirrors funcapi.h; built by `TupleDescGetAttInMetadata`.
#[derive(Debug)]
pub struct AttInMetadata<'mcx> {
    /// `TupleDesc tupdesc` — full tuple descriptor.
    pub tupdesc: TupleDesc<'mcx>,
    /// `FmgrInfo *attinfuncs` — array of attribute type input function info.
    pub attinfuncs: PgVec<'mcx, types_core::fmgr::FmgrInfo>,
    /// `Oid *attioparams` — array of attribute type I/O parameter OIDs.
    pub attioparams: PgVec<'mcx, Oid>,
    /// `int32 *atttypmods` — array of attribute typmod values.
    pub atttypmods: PgVec<'mcx, i32>,
}

/// `TupOutputState` (executor.h) — the convenience-output context built by
/// `begin_tup_output_tupdesc`.
///
/// SCAFFOLD: shape mirrors executor.h; the `DestReceiver` is referenced by
/// handle (the owned dest-receiver model is the printtup owner's).
#[derive(Debug)]
pub struct TupOutputState<'mcx> {
    /// `TupleTableSlot *slot` — the slot rows are stored into.
    pub slot: SlotData<'mcx>,
    /// `DestReceiver *dest` — the receiver each row is sent to.
    pub dest: crate::parsestmt::DestReceiverHandle,
}

/// SCAFFOLD anchor so an otherwise data-only module exposes a fallible builder
/// signature shape; the real builders live in `backend-executor-execTuples`.
#[allow(dead_code)]
fn _scaffold_marker<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<()> {
    Ok(())
}
