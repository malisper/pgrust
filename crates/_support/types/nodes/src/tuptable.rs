//! Tuple-table-slot payload model (executor/tuptable.h).
//!
//! The unified [`TupleTableSlot`] base AND the four concrete
//! `Virtual/Heap/Minimal/BufferHeap` superstructures + the [`SlotData`] enum
//! now all live together in the leaf crate `types-slot` (as `tuptable.h`
//! defines them together in C), so the table-AM vtable (`types-tableam`, which
//! `types-nodes` depends on) can name the payload-bearing [`SlotData`] its
//! slot-bearing callbacks hand to the AM. This module re-exports them so the
//! many existing `nodes::tuptable::*` / `nodes::*` imports keep
//! working unchanged, and additionally owns the funcapi/executor convenience
//! carriers ([`AttInMetadata`], [`TupOutputState`]) that reference
//! `types-nodes`-local types and therefore cannot move down.

use ::mcx::{Mcx, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

// Re-export the unified base, the four superstructures, the SlotData enum, the
// per-kind ops carrier, and the slot flag constants from their owning leaf
// crate so the executor's slot ops keep importing them from `tuptable`.
pub use ::types_slot::{
    item_pointer_invalid, BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot,
    SlotBase, SlotData, TupleSlotKind, TupleTableSlot, TupleTableSlotOps, VirtualTupleTableSlot,
    TTS_FLAG_EMPTY, TTS_FLAG_FIXED, TTS_FLAG_SHOULDFREE, TTS_FLAG_SLOW,
};

/// `AttInMetadata` (funcapi.h) — metadata for `BuildTupleFromCStrings`.
///
/// SCAFFOLD: shape mirrors funcapi.h; built by `TupleDescGetAttInMetadata`.
#[derive(Debug)]
pub struct AttInMetadata<'mcx> {
    /// `TupleDesc tupdesc` — full tuple descriptor.
    pub tupdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
    /// `FmgrInfo *attinfuncs` — array of attribute type input function info.
    pub attinfuncs: PgVec<'mcx, ::types_core::fmgr::FmgrInfo>,
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

/// SCAFFOLD anchor so an otherwise re-export module exposes a fallible builder
/// signature shape; the real builders live in `backend-executor-execTuples`.
#[allow(dead_code)]
fn _scaffold_marker<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<()> {
    Ok(())
}
