//! Tuple-table-slot payload model (executor/tuptable.h) — the runtime slot
//! data carrier that `backend-executor-execTuples` owns and mutates.
//!
//! The unified [`TupleTableSlot`] (`types-slot`) carries both the header bits
//! and the per-attribute payload (`tts_values`/`tts_isnull`/`tts_nvalid`/
//! `tts_tupleDescriptor`), exactly as C does — there is no header-vs-payload
//! split. This module owns the four concrete `Virtual/Heap/Minimal/BufferHeap`
//! superstructures that embed that base, the [`SlotData`] enum that selects
//! among them (the idiomatic `TupleTableSlot *` that may point at any subtype),
//! and the sizing/ops metadata. [`SlotBase`] is a thin alias of the unified
//! base so the superstructures' `base` field keeps its name.
//!
//! Mirroring C exactly (tuptable.h); the body-bearing `HeapTuple`/`MinimalTuple`
//! fields are carried as the [`FormedTuple`]/[`FormedMinimalTuple`] carriers
//! (header + owned data-area bytes) the form/deform owner already produces:
//!
//! * `VirtualTupleTableSlot { TupleTableSlot base; char *data; }`
//! * `HeapTupleTableSlot   { TupleTableSlot base; HeapTuple tuple; uint32 off;
//!    HeapTupleData tupdata; }`
//! * `BufferHeapTupleTableSlot { HeapTupleTableSlot base; Buffer buffer; }`
//! * `MinimalTupleTableSlot { TupleTableSlot base; HeapTuple tuple;
//!    MinimalTuple mintuple; HeapTupleData minhdr; uint32 off; }`
//!
//! The per-kind dispatch callbacks (clear/materialize/getsomeattrs/copyslot/
//! store/…) live in `backend-executor-execTuples`; this crate carries only the
//! owned structs + pure accessors.

use mcx::{Mcx, PgVec};
use types_core::primitive::{Oid, Size};
use types_error::PgResult;
use types_storage::buf::Buffer;
use types_tuple::backend_access_common_heaptuple::{FormedMinimalTuple, FormedTuple};
use types_tuple::heaptuple::{HeapTupleData, ItemPointerData, TupleDesc};

use types_slot::{TupleSlotKind, TupleTableSlot};
// Re-export the slot flag constants from their owning crate so the executor's
// slot ops keep importing them from `tuptable`.
pub use types_slot::{TTS_FLAG_EMPTY, TTS_FLAG_FIXED, TTS_FLAG_SHOULDFREE, TTS_FLAG_SLOW};

/// `SlotBase<'mcx>` is now exactly the unified [`TupleTableSlot`] (tuptable.h):
/// the header bits and the per-attribute value/null payload (`tts_nvalid`,
/// `tts_tupleDescriptor`, `tts_values`, `tts_isnull`) live in one type, as in
/// C. Retained as an alias so the four superstructures' `base` field and the
/// executor's `slot.base()`/`slot.base_mut()` flows keep their names.
pub type SlotBase<'mcx> = TupleTableSlot<'mcx>;

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
    /// `HeapTuple tuple;` — physical tuple. Carried as the body-bearing
    /// [`FormedTuple`] (header `PgBox` + `data: PgVec<u8>` user-data bytes), the
    /// faithful idiomatic stand-in for C's `HeapTuple` pointing at a contiguous
    /// `HeapTupleHeaderData + (char*)tup + t_hoff` data area, which the
    /// header-only [`HeapTuple`](types_tuple::heaptuple::HeapTuple) lacks. `None`
    /// when no tuple is stored.
    pub tuple: Option<FormedTuple<'mcx>>,
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
    /// `HeapTuple tuple;` — tuple wrapper (points at `minhdr`). Carried as the
    /// body-bearing [`FormedTuple`] view over the minimal body (its `t_data`
    /// header sits `MINIMAL_TUPLE_OFFSET` before the body bytes), so
    /// `slot_deform`/`get_heap_tuple` see `(char*)tup + t_hoff` data. `None`
    /// until the wrapper is set up.
    pub tuple: Option<FormedTuple<'mcx>>,
    /// `MinimalTuple mintuple;` — minimal tuple, or NULL if none. Carried as the
    /// body-bearing [`FormedMinimalTuple`] (header `PgBox` + `data` body bytes)
    /// that `heap_form_minimal_tuple`/`heap_copy_minimal_tuple`/
    /// `minimal_tuple_from_heap_tuple` already return.
    pub mintuple: Option<FormedMinimalTuple<'mcx>>,
    /// `HeapTupleData minhdr;` — workspace for minimal-tuple-only case (the
    /// header `tuple` points into; `minhdr.t_data` is `MINIMAL_TUPLE_OFFSET`
    /// before the minimal body).
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
