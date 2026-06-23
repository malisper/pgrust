//! Family: slot payload model — the `TupleTableSlot` data carrier plus the
//! four payload subtypes and the flag/TID/sizing helpers (executor/tuptable.h,
//! execTuples.c `MakeTupleTableSlot` sizing).
//!
//! The owned types live in [`::nodes::tuptable`]; this module owns the
//! per-kind sizing and the make/empty helpers that operate on them.

use mcx::{vec_with_capacity_in, Mcx};
use ::types_core::primitive::Size;
use ::types_error::PgResult;
use ::nodes::tuptable::{
    BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotData,
    TupleTableSlotOps, VirtualTupleTableSlot, TTS_FLAG_FIXED,
};
use nodes::{TupleSlotKind, TupleTableSlot};
// The canonical value enum; `Datum` is its transitional alias.
use ::types_tuple::heaptuple::{Datum};
use ::types_tuple::heaptuple::{HeapTupleData, ItemPointerData, TupleDesc};

use crate::slot_ops_vtables;

/// `sizeof(VirtualTupleTableSlot)` (`MakeTupleTableSlot` sizing /
/// `TTSOpsVirtual.base_slot_size`).
pub fn virtual_slot_size() -> Size {
    core::mem::size_of::<VirtualTupleTableSlot>()
}

/// `sizeof(HeapTupleTableSlot)` (`TTSOpsHeapTuple.base_slot_size`).
pub fn heap_slot_size() -> Size {
    core::mem::size_of::<HeapTupleTableSlot>()
}

/// `sizeof(MinimalTupleTableSlot)` (`TTSOpsMinimalTuple.base_slot_size`).
pub fn minimal_slot_size() -> Size {
    core::mem::size_of::<MinimalTupleTableSlot>()
}

/// `sizeof(BufferHeapTupleTableSlot)` (`TTSOpsBufferHeapTuple.base_slot_size`).
pub fn buffer_slot_size() -> Size {
    core::mem::size_of::<BufferHeapTupleTableSlot>()
}

/// The `TupleTableSlotOps` metadata for a slot kind (the `&TTSOps*` singleton
/// the kind selects). Mirrors the four `const TupleTableSlotOps` tables in
/// execTuples.c: `base_slot_size` is the concrete struct size, and the
/// `get_heap_tuple` / `get_minimal_tuple` callback presence is exactly the
/// `NULL`/non-`NULL` pattern of those tables.
pub fn ops_for_kind(kind: TupleSlotKind) -> TupleTableSlotOps {
    match kind {
        // TTSOpsVirtual: get_heap_tuple = NULL, get_minimal_tuple = NULL.
        TupleSlotKind::Virtual => TupleTableSlotOps {
            base_slot_size: virtual_slot_size(),
            has_get_heap_tuple: false,
            has_get_minimal_tuple: false,
        },
        // TTSOpsHeapTuple: get_heap_tuple = tts_heap_get_heap_tuple,
        // get_minimal_tuple = NULL.
        TupleSlotKind::HeapTuple => TupleTableSlotOps {
            base_slot_size: heap_slot_size(),
            has_get_heap_tuple: true,
            has_get_minimal_tuple: false,
        },
        // TTSOpsMinimalTuple: get_heap_tuple = NULL,
        // get_minimal_tuple = tts_minimal_get_minimal_tuple.
        TupleSlotKind::MinimalTuple => TupleTableSlotOps {
            base_slot_size: minimal_slot_size(),
            has_get_heap_tuple: false,
            has_get_minimal_tuple: true,
        },
        // TTSOpsBufferHeapTuple: get_heap_tuple = tts_buffer_heap_get_heap_tuple,
        // get_minimal_tuple = NULL.
        TupleSlotKind::BufferHeapTuple => TupleTableSlotOps {
            base_slot_size: buffer_slot_size(),
            has_get_heap_tuple: true,
            has_get_minimal_tuple: false,
        },
    }
}

/// A zeroed `HeapTupleData` — the owned analog of the `palloc0`'d `tupdata` /
/// `minhdr` workspace that `MakeTupleTableSlot` leaves in a fresh slot.
fn zeroed_heap_tuple_data<'mcx>() -> HeapTupleData<'mcx> {
    HeapTupleData {
        t_len: 0,
        t_self: ItemPointerData::default(),
        t_tableOid: 0,
        t_data: None,
    }
}

/// `MakeTupleTableSlot(tupleDesc, tts_ops)` (execTuples.c): allocate a slot of
/// the given kind, fixed to `tupleDesc`, with its `tts_values`/`tts_isnull`
/// arrays sized from the descriptor. The slot allocates in `mcx`.
///
/// The C single-block `palloc0(allocsz)` (base struct + `MAXALIGN`ed Datum and
/// bool arrays) becomes the owned subtype struct plus two `mcx`-allocated
/// arrays: when `tupleDesc` is fixed they are sized to `natts` and zero-filled
/// (`palloc0`); when it is `NULL` they are empty (the slot type's `init`/store
/// callbacks size them on demand). `tts_mcxt = CurrentMemoryContext` is modeled
/// by allocating in `mcx`.
pub fn MakeTupleTableSlot<'mcx>(
    mcx: Mcx<'mcx>,
    tupleDesc: TupleDesc<'mcx>,
    kind: TupleSlotKind,
) -> PgResult<SlotData<'mcx>> {
    // The unified base: tts_flags |= TTS_FLAG_EMPTY (a fresh slot is empty),
    // plus TTS_FLAG_FIXED when a descriptor is supplied. `new_in` gives the
    // empty virtual default; we override tts_ops and fill the payload below.
    let mut base = TupleTableSlot::new_in(mcx);
    base.tts_ops = kind;

    if let Some(mut desc) = tupleDesc {
        base.tts_flags |= TTS_FLAG_FIXED;

        let natts = desc.natts as usize;
        // palloc0 of the Datum/bool arrays. A freshly-allocated slot has
        // tts_nvalid = 0, so the array contents are don't-care until deform
        // fills them; the zero word maps to `Datum::null()`.
        let mut values: ::mcx::PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, natts)?;
        let mut isnull: ::mcx::PgVec<'mcx, bool> = vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        isnull.resize(natts, false);
        base.tts_values = values;
        base.tts_isnull = isnull;

        // PinTupleDesc: bump the refcount of a refcounted descriptor
        // (`if (tupleDesc->tdrefcount >= 0) IncrTupleDescRefCount`). The
        // `IncrTupleDescRefCount` helper's own guard is exactly this gate, so
        // routing through the tupdesc owner is correct: a non-refcounted
        // (`tdrefcount < 0`) descriptor is left untouched.
        if desc.tdrefcount >= 0 {
            tupdesc::IncrTupleDescRefCount(&mut desc)?;
        }
        base.tts_tupleDescriptor = Some(desc);
    }

    // Build the concrete subtype, then run the slot-type-specific `init`
    // callback (`slot->tts_ops->init(slot)`), owned by the slot-ops vtable
    // family.
    let mut slot = match kind {
        TupleSlotKind::Virtual => SlotData::Virtual(VirtualTupleTableSlot {
            base,
            data: vec_with_capacity_in(mcx, 0)?,
        }),
        TupleSlotKind::HeapTuple => SlotData::Heap(HeapTupleTableSlot {
            base,
            tuple: None,
            off: 0,
            tupdata: zeroed_heap_tuple_data(),
        }),
        TupleSlotKind::MinimalTuple => SlotData::Minimal(MinimalTupleTableSlot {
            base,
            tuple: None,
            mintuple: None,
            minhdr: zeroed_heap_tuple_data(),
            off: 0,
        }),
        TupleSlotKind::BufferHeapTuple => SlotData::BufferHeap(BufferHeapTupleTableSlot {
            base: HeapTupleTableSlot {
                base,
                tuple: None,
                off: 0,
                tupdata: zeroed_heap_tuple_data(),
            },
            buffer: types_storage::buf::InvalidBuffer,
        }),
    };

    match &mut slot {
        SlotData::Virtual(s) => slot_ops_vtables::tts_virtual_init(s),
        SlotData::Heap(s) => slot_ops_vtables::tts_heap_init(s),
        SlotData::Minimal(s) => slot_ops_vtables::tts_minimal_init(s),
        SlotData::BufferHeap(s) => slot_ops_vtables::tts_buffer_heap_init(s),
    }

    Ok(slot)
}
