//! Family: slot-ops vtables — the per-kind `TupleTableSlotOps` callbacks for
//! the virtual/heap/minimal/buffer slot classes (execTuples.c
//! `tts_virtual_*` / `tts_heap_*` / `tts_minimal_*` / `tts_buffer_heap_*`),
//! plus the `Slot`-level dispatch that routes `slot->tts_ops->op(slot)` to the
//! right kind.
//!
//! Each callback takes a `&mut` to the concrete payload subtype (the analog of
//! the C downcast of `TupleTableSlot *`); allocating callbacks take `Mcx`.

use mcx::Mcx;
use types_core::primitive::AttrNumber;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::tuptable::{
    BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotData,
    VirtualTupleTableSlot,
};
use types_tuple::heaptuple::{HeapTuple, MinimalTuple};

// --- VirtualTupleTableSlot ops --------------------------------------------

pub fn tts_virtual_init(_slot: &mut VirtualTupleTableSlot) {
    todo!("execTuples.c tts_virtual_init")
}
pub fn tts_virtual_release(_slot: &mut VirtualTupleTableSlot) {
    todo!("execTuples.c tts_virtual_release")
}
pub fn tts_virtual_clear(_slot: &mut VirtualTupleTableSlot) {
    todo!("execTuples.c tts_virtual_clear")
}
pub fn tts_virtual_getsomeattrs(_slot: &mut VirtualTupleTableSlot, _natts: i32) -> PgResult<()> {
    todo!("execTuples.c tts_virtual_getsomeattrs")
}
pub fn tts_virtual_getsysattr(
    _slot: &VirtualTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum, bool)> {
    todo!("execTuples.c tts_virtual_getsysattr")
}
pub fn tts_virtual_is_current_xact_tuple(_slot: &VirtualTupleTableSlot) -> PgResult<bool> {
    todo!("execTuples.c tts_virtual_is_current_xact_tuple")
}
pub fn tts_virtual_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut VirtualTupleTableSlot<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c tts_virtual_materialize")
}
pub fn tts_virtual_copyslot<'mcx>(
    _mcx: Mcx<'mcx>,
    _dst: &mut SlotData<'mcx>,
    _src: &SlotData<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c tts_virtual_copyslot")
}
pub fn tts_virtual_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &VirtualTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c tts_virtual_copy_heap_tuple")
}
pub fn tts_virtual_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &VirtualTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    todo!("execTuples.c tts_virtual_copy_minimal_tuple")
}

// --- HeapTupleTableSlot ops -----------------------------------------------

pub fn tts_heap_init(_slot: &mut HeapTupleTableSlot) {
    todo!("execTuples.c tts_heap_init")
}
pub fn tts_heap_release(_slot: &mut HeapTupleTableSlot) {
    todo!("execTuples.c tts_heap_release")
}
pub fn tts_heap_clear(_slot: &mut HeapTupleTableSlot) {
    todo!("execTuples.c tts_heap_clear")
}
pub fn tts_heap_getsomeattrs(_slot: &mut HeapTupleTableSlot, _natts: i32) -> PgResult<()> {
    todo!("execTuples.c tts_heap_getsomeattrs")
}
pub fn tts_heap_getsysattr(
    _slot: &HeapTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum, bool)> {
    todo!("execTuples.c tts_heap_getsysattr")
}
pub fn tts_heap_is_current_xact_tuple(_slot: &HeapTupleTableSlot) -> PgResult<bool> {
    todo!("execTuples.c tts_heap_is_current_xact_tuple")
}
pub fn tts_heap_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c tts_heap_materialize")
}
pub fn tts_heap_get_heap_tuple<'mcx>(
    _slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c tts_heap_get_heap_tuple")
}
pub fn tts_heap_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c tts_heap_copy_heap_tuple")
}
pub fn tts_heap_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut HeapTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    todo!("execTuples.c tts_heap_copy_minimal_tuple")
}
pub fn tts_heap_store_tuple<'mcx>(
    _slot: &mut HeapTupleTableSlot<'mcx>,
    _tuple: HeapTuple<'mcx>,
    _should_free: bool,
) {
    todo!("execTuples.c tts_heap_store_tuple")
}

// --- MinimalTupleTableSlot ops --------------------------------------------

pub fn tts_minimal_init(_slot: &mut MinimalTupleTableSlot) {
    todo!("execTuples.c tts_minimal_init")
}
pub fn tts_minimal_release(_slot: &mut MinimalTupleTableSlot) {
    todo!("execTuples.c tts_minimal_release")
}
pub fn tts_minimal_clear(_slot: &mut MinimalTupleTableSlot) {
    todo!("execTuples.c tts_minimal_clear")
}
pub fn tts_minimal_getsomeattrs(_slot: &mut MinimalTupleTableSlot, _natts: i32) -> PgResult<()> {
    todo!("execTuples.c tts_minimal_getsomeattrs")
}
pub fn tts_minimal_getsysattr(
    _slot: &MinimalTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum, bool)> {
    todo!("execTuples.c tts_minimal_getsysattr")
}
pub fn tts_minimal_is_current_xact_tuple(_slot: &MinimalTupleTableSlot) -> PgResult<bool> {
    todo!("execTuples.c tts_minimal_is_current_xact_tuple")
}
pub fn tts_minimal_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c tts_minimal_materialize")
}
pub fn tts_minimal_get_minimal_tuple<'mcx>(
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    todo!("execTuples.c tts_minimal_get_minimal_tuple")
}
pub fn tts_minimal_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c tts_minimal_copy_heap_tuple")
}
pub fn tts_minimal_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut MinimalTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    todo!("execTuples.c tts_minimal_copy_minimal_tuple")
}
pub fn tts_minimal_store_tuple<'mcx>(
    _slot: &mut MinimalTupleTableSlot<'mcx>,
    _mtup: MinimalTuple<'mcx>,
    _should_free: bool,
) {
    todo!("execTuples.c tts_minimal_store_tuple")
}

// --- BufferHeapTupleTableSlot ops -----------------------------------------

pub fn tts_buffer_heap_init(_slot: &mut BufferHeapTupleTableSlot) {
    todo!("execTuples.c tts_buffer_heap_init")
}
pub fn tts_buffer_heap_release(_slot: &mut BufferHeapTupleTableSlot) {
    todo!("execTuples.c tts_buffer_heap_release")
}
pub fn tts_buffer_heap_clear(_slot: &mut BufferHeapTupleTableSlot) {
    todo!("execTuples.c tts_buffer_heap_clear")
}
pub fn tts_buffer_heap_getsomeattrs(
    _slot: &mut BufferHeapTupleTableSlot,
    _natts: i32,
) -> PgResult<()> {
    todo!("execTuples.c tts_buffer_heap_getsomeattrs")
}
pub fn tts_buffer_heap_getsysattr(
    _slot: &BufferHeapTupleTableSlot,
    _attnum: i32,
) -> PgResult<(Datum, bool)> {
    todo!("execTuples.c tts_buffer_heap_getsysattr")
}
pub fn tts_buffer_is_current_xact_tuple(_slot: &BufferHeapTupleTableSlot) -> PgResult<bool> {
    todo!("execTuples.c tts_buffer_is_current_xact_tuple")
}
pub fn tts_buffer_heap_materialize<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c tts_buffer_heap_materialize")
}
pub fn tts_buffer_heap_copyslot<'mcx>(
    _mcx: Mcx<'mcx>,
    _dst: &mut SlotData<'mcx>,
    _src: &SlotData<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c tts_buffer_heap_copyslot")
}
pub fn tts_buffer_heap_get_heap_tuple<'mcx>(
    _slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c tts_buffer_heap_get_heap_tuple")
}
pub fn tts_buffer_heap_copy_heap_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<HeapTuple<'mcx>> {
    todo!("execTuples.c tts_buffer_heap_copy_heap_tuple")
}
pub fn tts_buffer_heap_copy_minimal_tuple<'mcx>(
    _mcx: Mcx<'mcx>,
    _slot: &mut BufferHeapTupleTableSlot<'mcx>,
) -> PgResult<MinimalTuple<'mcx>> {
    todo!("execTuples.c tts_buffer_heap_copy_minimal_tuple")
}

// --- Slot-level dispatch (slot->tts_ops->op(slot)) ------------------------

/// `slot->tts_ops->clear(slot)` dispatch.
pub fn slot_clear(_slot: &mut SlotData) {
    todo!("execTuples.c slot->tts_ops->clear")
}
/// `slot->tts_ops->release(slot)` dispatch.
pub fn slot_release(_slot: &mut SlotData) {
    todo!("execTuples.c slot->tts_ops->release")
}
/// `slot->tts_ops->materialize(slot)` dispatch.
pub fn slot_materialize<'mcx>(_mcx: Mcx<'mcx>, _slot: &mut SlotData<'mcx>) -> PgResult<()> {
    todo!("execTuples.c slot->tts_ops->materialize")
}
/// `slot->tts_ops->getsomeattrs(slot, natts)` dispatch.
pub fn slot_ops_getsomeattrs(_slot: &mut SlotData, _natts: i32) -> PgResult<()> {
    todo!("execTuples.c slot->tts_ops->getsomeattrs")
}
/// `slot->tts_ops->getsysattr(slot, attnum, &isnull)` dispatch.
pub fn slot_getsysattr(_slot: &SlotData, _attnum: AttrNumber) -> PgResult<(Datum, bool)> {
    todo!("execTuples.c slot->tts_ops->getsysattr")
}
/// `slot->tts_ops->is_current_xact_tuple(slot)` dispatch.
pub fn slot_is_current_xact_tuple(_slot: &SlotData) -> PgResult<bool> {
    todo!("execTuples.c slot->tts_ops->is_current_xact_tuple")
}
/// `slot->tts_ops->copyslot(dst, src)` dispatch (invoked on the destination).
pub fn slot_copyslot<'mcx>(
    _mcx: Mcx<'mcx>,
    _dst: &mut SlotData<'mcx>,
    _src: &SlotData<'mcx>,
) -> PgResult<()> {
    todo!("execTuples.c dstslot->tts_ops->copyslot")
}
