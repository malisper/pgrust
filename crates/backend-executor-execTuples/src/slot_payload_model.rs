//! Family: slot payload model — the `TupleTableSlot` data carrier plus the
//! four payload subtypes and the flag/TID/sizing helpers (executor/tuptable.h,
//! execTuples.c `MakeTupleTableSlot` sizing).
//!
//! The owned types live in [`types_nodes::tuptable`]; this module owns the
//! per-kind sizing and the make/empty helpers that operate on them.

use mcx::Mcx;
use types_core::primitive::Size;
use types_error::PgResult;
use types_nodes::tuptable::{SlotData, TupleTableSlotOps};
use types_nodes::TupleSlotKind;
use types_tuple::heaptuple::TupleDesc;

/// `sizeof(VirtualTupleTableSlot)` (`MakeTupleTableSlot` sizing).
pub fn virtual_slot_size() -> Size {
    todo!("execTuples.c TTSOpsVirtual.base_slot_size")
}

/// `sizeof(HeapTupleTableSlot)`.
pub fn heap_slot_size() -> Size {
    todo!("execTuples.c TTSOpsHeapTuple.base_slot_size")
}

/// `sizeof(MinimalTupleTableSlot)`.
pub fn minimal_slot_size() -> Size {
    todo!("execTuples.c TTSOpsMinimalTuple.base_slot_size")
}

/// `sizeof(BufferHeapTupleTableSlot)`.
pub fn buffer_slot_size() -> Size {
    todo!("execTuples.c TTSOpsBufferHeapTuple.base_slot_size")
}

/// The `TupleTableSlotOps` metadata for a slot kind (the `&TTSOps*` singleton
/// the kind selects).
pub fn ops_for_kind(_kind: TupleSlotKind) -> TupleTableSlotOps {
    todo!("execTuples.c TTSOpsVirtual / TTSOpsHeapTuple / TTSOpsMinimalTuple / TTSOpsBufferHeapTuple")
}

/// `MakeTupleTableSlot(tupleDesc, tts_ops)` (execTuples.c): allocate a slot of
/// the given kind, fixed to `tupleDesc`, with its `tts_values`/`tts_isnull`
/// arrays sized from the descriptor. The slot allocates in `mcx`.
pub fn MakeTupleTableSlot<'mcx>(
    _mcx: Mcx<'mcx>,
    _tupleDesc: TupleDesc<'mcx>,
    _kind: TupleSlotKind,
) -> PgResult<SlotData<'mcx>> {
    todo!("execTuples.c MakeTupleTableSlot")
}
