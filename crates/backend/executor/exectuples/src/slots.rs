use alloc::boxed::Box;
use alloc::rc::Rc;
use core::alloc::Layout;
use core::ptr::NonNull;

use ::datum::Datum;
use ::heaptuple::{
    heap_copy_minimal_tuple, heap_copy_tuple_as_datum, heap_copytuple, heap_form_minimal_tuple,
    heap_form_minimal_tuple_planned, heap_form_tuple, heap_tuple_from_minimal_tuple,
    minimal_tuple_from_heap_tuple, HeapTuple, MinimalFormPlan, MinimalTuple,
};
use ::mcx::{vec_with_capacity_in, Allocator, Mcx, PgVec};
use ::types_core::{AttrNumber, Buffer, BufferIsValid, InvalidBuffer, TransactionId};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_slot::{
    BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotBase, SlotData,
    TupleSlotKind, VirtualTupleTableSlot, TTS_FLAG_EMPTY, TTS_FLAG_FIXED, TTS_FLAG_SHOULDFREE,
};
use ::types_tuple::tupmacs::{att_addlength_datum, att_nominal_alignby};
use ::types_tuple::varatt::varatt_is_external_expanded;
use ::types_tuple::{
    heap_deform_tuple, HeapTupleData, HeapTupleHeaderData, ItemPointerData, MinimalTupleData,
    TupleDescData, MAXIMUM_ALIGNOF,
};

use crate::deform::{slot_getallattrs, slot_getmissingattrs, TupleImage};

// unported: EOH_flatten_into (the expandeddatum unit owns it); clean
// feature error rather than a panic if an expanded datum ever lands here.
#[track_caller]
#[cold]
#[inline(never)]
fn expanded_datum_unported() -> Box<PgError> {
    Box::new(
        PgError::error("materializing an expanded datum is not yet implemented")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[cold]
#[inline(never)]
fn wrong_slot(kind: &'static str) -> ! {
    panic!("trying to store a {kind} tuple into wrong type of slot")
}

#[inline]
fn image_layout(size: usize) -> Layout {
    // SAFETY: MAXIMUM_ALIGNOF is a power of two; size was accepted by the
    // original heaptuple allocation.
    unsafe { Layout::from_size_align_unchecked(size, MAXIMUM_ALIGNOF) }
}

/// # Safety
/// The image was allocated by heaptuple in `mcx` with `alloc_size == t_len`
/// (the SHOULDFREE ownership invariant) and is not referenced afterwards.
unsafe fn free_heap_image(mcx: Mcx<'_>, t: &HeapTupleData<'_>) {
    unsafe {
        mcx.deallocate(
            NonNull::new_unchecked(t.header_ptr().cast_mut()),
            image_layout(t.t_len as usize),
        )
    }
}

/// # Safety
/// As [`free_heap_image`]: slot-owned minimal tuples are stored with
/// `extra == 0`, so the allocation starts at `p` and spans `t_len`.
unsafe fn free_minimal_image(mcx: Mcx<'_>, p: NonNull<MinimalTupleData>) {
    unsafe {
        let len = p.as_ref().t_len as usize;
        mcx.deallocate(p.cast(), image_layout(len));
    }
}

// Ownership transfer into the slot: leak the owner, keep the raw view; the
// SHOULDFREE flag records the free obligation (C's shouldFree bool).
fn forget_heap(t: HeapTuple<'_>) -> HeapTupleData<'_> {
    // free_heap_image frees by t_len: a t_len shrink desyncs the layout.
    debug_assert_eq!(t.alloc_size(), t.t_len);
    // SAFETY: same live image; the owner is forgotten so the view is unique.
    let view =
        unsafe { HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid) };
    core::mem::forget(t);
    view
}

fn forget_minimal(mut t: MinimalTuple<'_>) -> NonNull<MinimalTupleData> {
    debug_assert!(t.extra_mut().is_empty());
    // SAFETY: as_ptr is non-null and points at the initialized header.
    let p = unsafe { NonNull::new_unchecked(t.as_ptr().cast_mut().cast()) };
    core::mem::forget(t);
    p
}

/// # Safety
/// Aliases the same image read-only; caller must not free through both.
pub(crate) unsafe fn dup_heap_view<'mcx>(t: &HeapTupleData<'mcx>) -> HeapTupleData<'mcx> {
    unsafe { HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid) }
}

#[inline]
unsafe fn minimal_bytes<'a>(p: NonNull<MinimalTupleData>) -> &'a [u8] {
    // SAFETY: a stored minimal tuple is a live flat image of t_len bytes.
    unsafe { core::slice::from_raw_parts(p.as_ptr().cast::<u8>(), p.as_ref().t_len as usize) }
}

pub fn make_tuple_table_slot<'mcx>(
    mcx: Mcx<'mcx>,
    kind: TupleSlotKind,
    desc: Option<Rc<TupleDescData<'mcx>>>,
) -> SlotData<'mcx> {
    let mut base = SlotBase::new_in(mcx, kind);
    if let Some(d) = desc {
        base.tts_flags |= TTS_FLAG_FIXED;
        base.set_descriptor(mcx, d);
    }
    match kind {
        TupleSlotKind::Virtual => SlotData::Virtual(VirtualTupleTableSlot {
            base,
            data: PgVec::new_in(mcx),
        }),
        TupleSlotKind::HeapTuple => SlotData::Heap(HeapTupleTableSlot {
            base,
            tuple: None,
            off: 0,
            jit_deform: None,
        }),
        // C's minhdr wrapper (mslot->tuple = &mslot->minhdr) dissolves: the
        // minimal deform lane reads the minimal header directly.
        TupleSlotKind::MinimalTuple => SlotData::Minimal(MinimalTupleTableSlot {
            base,
            tuple: None,
            mintuple: None,
            off: 0,
        }),
        TupleSlotKind::BufferHeapTuple => SlotData::BufferHeap(BufferHeapTupleTableSlot {
            base: HeapTupleTableSlot {
                base,
                tuple: None,
                off: 0,
                jit_deform: None,
            },
            buffer: InvalidBuffer,
        }),
    }
}

pub fn exec_set_slot_descriptor<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    desc: Rc<TupleDescData<'mcx>>,
) {
    debug_assert!(!slot.base().is_fixed());
    exec_clear_tuple(slot, mcx);
    // A rebound descriptor orphans any deform-JIT arm (kernels are per-desc).
    match slot {
        SlotData::Heap(h) => h.jit_deform = None,
        SlotData::BufferHeap(b) => b.base.jit_deform = None,
        _ => {}
    }
    let base = slot.base_mut();
    base.tts_tupleDescriptor = None;
    base.set_descriptor(mcx, desc);
}

#[inline]
fn clear_virtual(v: &mut VirtualTupleTableSlot<'_>) {
    // C pfrees vslot->data when SHOULDFREE; kept as retained scratch for the
    // next materialize (rule 7) — the flag alone tracks materialization.
    v.base.mark_empty();
    v.base.tts_tid = ItemPointerData::invalid();
}

#[inline]
fn clear_heap<'mcx>(h: &mut HeapTupleTableSlot<'mcx>, mcx: Mcx<'mcx>) {
    if h.base.should_free() {
        // SAFETY: SHOULDFREE marks a slot-owned heaptuple image in mcx.
        unsafe { free_heap_image(mcx, h.tuple.as_ref().expect("SHOULDFREE without tuple")) };
    }
    h.base.mark_empty();
    h.base.tts_tid = ItemPointerData::invalid();
    h.off = 0;
    h.tuple = None;
}

#[inline]
fn clear_minimal<'mcx>(m: &mut MinimalTupleTableSlot<'mcx>, mcx: Mcx<'mcx>) {
    if m.base.should_free() {
        // SAFETY: SHOULDFREE marks a slot-owned minimal image in mcx.
        unsafe { free_minimal_image(mcx, m.mintuple.expect("SHOULDFREE without tuple")) };
    }
    m.base.mark_empty();
    m.base.tts_tid = ItemPointerData::invalid();
    m.off = 0;
    m.mintuple = None;
}

#[inline]
fn clear_buffer<'mcx>(b: &mut BufferHeapTupleTableSlot<'mcx>, mcx: Mcx<'mcx>) {
    if b.base.base.should_free() {
        debug_assert!(!BufferIsValid(b.buffer));
        // SAFETY: SHOULDFREE marks a slot-owned heaptuple image in mcx.
        unsafe {
            free_heap_image(
                mcx,
                b.base.tuple.as_ref().expect("SHOULDFREE without tuple"),
            )
        };
        b.base.base.tts_flags &= !TTS_FLAG_SHOULDFREE;
    }
    if BufferIsValid(b.buffer) {
        let _ = bufmgr_seams::release_buffer::call(b.buffer);
    }
    b.base.base.mark_empty();
    b.base.base.tts_tid = ItemPointerData::invalid();
    b.base.tuple = None;
    b.base.off = 0;
    b.buffer = InvalidBuffer;
}

/// `mcx` is the slot's owning context (C `tts_mcxt`) for every function here.
#[inline]
pub fn exec_clear_tuple<'mcx>(slot: &mut SlotData<'mcx>, mcx: Mcx<'mcx>) {
    match slot {
        SlotData::Virtual(v) => clear_virtual(v),
        SlotData::Heap(h) => clear_heap(h, mcx),
        SlotData::Minimal(m) => clear_minimal(m, mcx),
        SlotData::BufferHeap(b) => clear_buffer(b, mcx),
    }
}

/// Executor-skeleton park: drop a virtual slot's retained materialize
/// scratch. Materialize allocates it from the CALLER's context (dest
/// receivers pass their own), so it must not outlive the statement; parked
/// skeletons do.
pub fn exec_drop_slot_scratch<'mcx>(slot: &mut SlotData<'mcx>, mcx: Mcx<'mcx>) {
    if let SlotData::Virtual(v) = slot {
        if v.data.capacity() != 0 {
            debug_assert!(!v.base.should_free());
            v.data = PgVec::new_in(mcx);
        }
    }
}

#[inline]
pub fn exec_store_virtual_tuple(slot: &mut SlotData<'_>) {
    let base = slot.base_mut();
    debug_assert!(base.is_empty());
    let natts = base
        .tts_tupleDescriptor
        .as_ref()
        .expect("ExecStoreVirtualTuple without descriptor")
        .natts;
    base.mark_not_empty();
    base.tts_nvalid = natts as AttrNumber;
}

pub fn exec_store_all_null_tuple<'mcx>(slot: &mut SlotData<'mcx>, mcx: Mcx<'mcx>) {
    exec_clear_tuple(slot, mcx);
    let base = slot.base_mut();
    base.tts_values.fill(Datum::null());
    base.tts_isnull.fill(true);
    exec_store_virtual_tuple(slot);
}

#[inline]
fn store_heap<'mcx>(
    h: &mut HeapTupleTableSlot<'mcx>,
    mcx: Mcx<'mcx>,
    tuple: HeapTupleData<'mcx>,
    owned: bool,
) {
    clear_heap(h, mcx);
    h.base.tts_nvalid = 0;
    h.base.tts_tid = tuple.t_self;
    h.off = 0;
    h.tuple = Some(tuple);
    h.base.tts_flags &= !(TTS_FLAG_EMPTY | TTS_FLAG_SHOULDFREE);
    if owned {
        h.base.tts_flags |= TTS_FLAG_SHOULDFREE;
    }
}

/// C `ExecStoreHeapTuple(tuple, slot, shouldFree=false)`: the caller's image
/// must outlive the slot content (C's lower-slot-retains-ownership contract).
#[inline]
pub fn exec_store_heap_tuple<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    tuple: HeapTupleData<'mcx>,
) {
    let SlotData::Heap(h) = slot else {
        wrong_slot("heap")
    };
    debug_assert!(h.base.tts_tupleDescriptor.is_some());
    let table_oid = tuple.t_tableOid;
    store_heap(h, mcx, tuple, false);
    h.base.tts_tableOid = table_oid;
}

/// C `ExecStoreHeapTuple(tuple, slot, shouldFree=true)`.
#[inline]
pub fn exec_store_heap_tuple_owned<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    tuple: HeapTuple<'mcx>,
) {
    let SlotData::Heap(h) = slot else {
        wrong_slot("heap")
    };
    debug_assert!(h.base.tts_tupleDescriptor.is_some());
    let table_oid = tuple.t_tableOid;
    store_heap(h, mcx, forget_heap(tuple), true);
    h.base.tts_tableOid = table_oid;
}

#[inline]
fn store_minimal<'mcx>(
    m: &mut MinimalTupleTableSlot<'mcx>,
    mcx: Mcx<'mcx>,
    mtup: NonNull<MinimalTupleData>,
    owned: bool,
) {
    clear_minimal(m, mcx);
    m.base.tts_flags &= !TTS_FLAG_EMPTY;
    m.base.tts_nvalid = 0;
    m.off = 0;
    m.mintuple = Some(mtup);
    if owned {
        m.base.tts_flags |= TTS_FLAG_SHOULDFREE;
    }
}

/// C `ExecStoreMinimalTuple(mtup, slot, shouldFree=false)` over a raw image
/// pointer. No `&MinimalTupleData`-taking variant: such an arg retags
/// provenance down to the header — the later deform is UB (Miri-caught).
///
/// # Safety
/// `mtup` points to a live minimal-tuple image readable for `t_len` bytes for
/// as long as the slot holds it.
#[inline]
pub unsafe fn exec_store_minimal_tuple_ptr<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    mtup: NonNull<MinimalTupleData>,
) {
    let SlotData::Minimal(m) = slot else {
        wrong_slot("minimal")
    };
    debug_assert!(m.base.tts_tupleDescriptor.is_some());
    store_minimal(m, mcx, mtup, false);
}

/// C `ExecStoreMinimalTuple(mtup, slot, shouldFree=true)`.
#[inline]
pub fn exec_store_minimal_tuple_owned<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    mtup: MinimalTuple<'mcx>,
) {
    let SlotData::Minimal(m) = slot else {
        wrong_slot("minimal")
    };
    debug_assert!(m.base.tts_tupleDescriptor.is_some());
    store_minimal(m, mcx, forget_minimal(mtup), true);
}

// SHOULDFREE arm of the buffer-slot store: off the per-tuple scan path
// (a scan slot never owns its image between stores).
#[cold]
#[inline(never)]
fn store_buffer_free_owned<'mcx>(b: &mut BufferHeapTupleTableSlot<'mcx>, mcx: Mcx<'mcx>) {
    // SAFETY: SHOULDFREE marks a slot-owned heaptuple image in mcx.
    unsafe {
        free_heap_image(
            mcx,
            b.base.tuple.as_ref().expect("SHOULDFREE without tuple"),
        )
    };
    b.base.base.tts_flags &= !TTS_FLAG_SHOULDFREE;
}

// Buffer-change arm: runs once per page in a scan, not per tuple; outlined so
// the same-buffer fast path (C's 12-insn tts_buffer_heap_store_tuple hit)
// carries no seam-call frame.
#[inline(never)]
fn store_buffer_new_pin(b: &mut BufferHeapTupleTableSlot<'_>, buffer: Buffer, transfer_pin: bool) {
    if BufferIsValid(b.buffer) {
        let _ = bufmgr_seams::release_buffer::call(b.buffer);
    }
    b.buffer = buffer;
    if !transfer_pin {
        bufmgr_seams::incr_buffer_ref_count::call(buffer);
    }
}

// C tts_buffer_heap_store_tuple: the slot keeps its own pin on `buffer`
// (IncrBufferRefCount) unless the pin is transferred or already held.
// `transfer_pin` is constant at both callers and folds under inlining.
#[inline]
fn store_buffer<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    tuple: HeapTupleData<'mcx>,
    buffer: Buffer,
    transfer_pin: bool,
) {
    debug_assert!(BufferIsValid(buffer));
    let SlotData::BufferHeap(b) = slot else {
        wrong_slot("buffer heap")
    };
    if b.base.base.should_free() {
        store_buffer_free_owned(b, mcx);
    }

    b.base.base.tts_nvalid = 0;
    b.base.base.tts_tid = tuple.t_self;
    b.base.base.tts_tableOid = tuple.t_tableOid;
    b.base.tuple = Some(tuple);
    b.base.off = 0;
    b.base.base.tts_flags &= !TTS_FLAG_EMPTY;

    if buffer != b.buffer {
        store_buffer_new_pin(b, buffer, transfer_pin);
    } else if transfer_pin {
        // The slot already holds this pin; drop the transferred extra one.
        let _ = bufmgr_seams::release_buffer::call(buffer);
    }
}

/// C `ExecStoreBufferHeapTuple`: caller's pin stays caller's; the slot pins.
#[inline]
pub fn exec_store_buffer_heap_tuple<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    tuple: HeapTupleData<'mcx>,
    buffer: Buffer,
) {
    store_buffer(slot, mcx, tuple, buffer, false)
}

/// C `ExecStorePinnedBufferHeapTuple`: the caller's pin moves into the slot.
#[inline]
pub fn exec_store_pinned_buffer_heap_tuple<'mcx>(
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
    tuple: HeapTupleData<'mcx>,
    buffer: Buffer,
) {
    store_buffer(slot, mcx, tuple, buffer, true)
}

fn virtual_materialize<'mcx>(v: &mut VirtualTupleTableSlot<'mcx>, mcx: Mcx<'mcx>) -> PgResult<()> {
    if v.base.should_free() {
        return Ok(());
    }

    let VirtualTupleTableSlot { base, data } = v;
    let SlotBase {
        tts_tupleDescriptor,
        tts_values,
        tts_isnull,
        tts_flags,
        ..
    } = base;
    let desc = tts_tupleDescriptor
        .as_ref()
        .expect("materialize without descriptor");
    let natts = desc.natts as usize;
    // Pre-sliced by natts: the per-attribute bounds checks fold away.
    let atts = &desc.compact_attrs[..natts];
    let values = &mut tts_values[..natts];
    let isnull = &tts_isnull[..natts];

    let mut sz = 0usize;
    for (att, (&val, &null)) in atts.iter().zip(values.iter().zip(isnull)) {
        if att.attbyval || null {
            continue;
        }
        // SAFETY: a non-null by-ref column datum points at a live field image.
        unsafe {
            if att.attlen == -1 && varatt_is_external_expanded(val.as_usize() as *const u8) {
                return Err(expanded_datum_unported());
            }
            sz = att_nominal_alignby(sz, att.attalignby);
            sz = att_addlength_datum(sz, att.attlen as i32, val);
        }
    }

    if sz == 0 {
        return Ok(());
    }

    // Headroom so the copy base is MAXALIGN'd (C's palloc guarantees it).
    let need = sz + MAXIMUM_ALIGNOF;
    if data.len() < need {
        let mut buf = vec_with_capacity_in::<u8>(mcx, need)?;
        buf.resize(need, 0);
        *data = buf;
    }
    let start = data.as_ptr().align_offset(MAXIMUM_ALIGNOF);
    debug_assert!(start + sz <= data.len());
    let dst0 = unsafe { data.as_mut_ptr().add(start) };

    *tts_flags |= TTS_FLAG_SHOULDFREE;

    let mut off = 0usize;
    for (att, (val, &null)) in atts.iter().zip(values.iter_mut().zip(isnull)) {
        if att.attbyval || null {
            continue;
        }
        // SAFETY: off stays within [0, sz) by the size pass above; source is
        // the live field image the datum points at.
        unsafe {
            off = att_nominal_alignby(off, att.attalignby);
            let data_length = att_addlength_datum(0, att.attlen as i32, *val);
            core::ptr::copy_nonoverlapping(val.as_usize() as *const u8, dst0.add(off), data_length);
            *val = Datum::from_usize(dst0.add(off) as usize);
            off += data_length;
        }
    }

    Ok(())
}

fn heap_materialize<'mcx>(h: &mut HeapTupleTableSlot<'mcx>, mcx: Mcx<'mcx>) -> PgResult<()> {
    debug_assert!(!h.base.is_empty());
    if h.base.should_free() {
        return Ok(());
    }

    // Deform state resets: tts_values could point into the non-materialized
    // tuple (C comment).
    h.base.tts_nvalid = 0;
    h.off = 0;

    let new = match &h.tuple {
        None => {
            let desc = h
                .base
                .tts_tupleDescriptor
                .as_ref()
                .expect("materialize without descriptor");
            heap_form_tuple(mcx, desc, &h.base.tts_values, &h.base.tts_isnull)?
        }
        Some(t) => heap_copytuple(mcx, t)?,
    };
    h.tuple = Some(forget_heap(new));
    h.base.tts_flags |= TTS_FLAG_SHOULDFREE;
    Ok(())
}

fn minimal_materialize<'mcx>(m: &mut MinimalTupleTableSlot<'mcx>, mcx: Mcx<'mcx>) -> PgResult<()> {
    debug_assert!(!m.base.is_empty());
    if m.base.should_free() {
        return Ok(());
    }

    m.base.tts_nvalid = 0;
    m.off = 0;

    let new = match m.mintuple {
        None => {
            let desc = m
                .base
                .tts_tupleDescriptor
                .as_ref()
                .expect("materialize without descriptor");
            heap_form_minimal_tuple(mcx, desc, &m.base.tts_values, &m.base.tts_isnull, 0)?
        }
        // SAFETY: stored mintuple is live; copied before the old reference drops.
        Some(p) => heap_copy_minimal_tuple(mcx, unsafe { minimal_bytes(p) }, 0)?,
    };
    m.mintuple = Some(forget_minimal(new));
    m.base.tts_flags |= TTS_FLAG_SHOULDFREE;
    Ok(())
}

fn buffer_materialize<'mcx>(
    b: &mut BufferHeapTupleTableSlot<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    debug_assert!(!b.base.base.is_empty());
    if b.base.base.should_free() {
        return Ok(());
    }

    b.base.off = 0;
    b.base.base.tts_nvalid = 0;

    match &b.base.tuple {
        None => {
            let desc = b
                .base
                .base
                .tts_tupleDescriptor
                .as_ref()
                .expect("materialize without descriptor");
            let new = heap_form_tuple(mcx, desc, &b.base.base.tts_values, &b.base.base.tts_isnull)?;
            b.base.tuple = Some(forget_heap(new));
        }
        Some(t) => {
            let new = heap_copytuple(mcx, t)?;
            if BufferIsValid(b.buffer) {
                let _ = bufmgr_seams::release_buffer::call(b.buffer);
            }
            b.buffer = InvalidBuffer;
            b.base.tuple = Some(forget_heap(new));
        }
    }

    b.base.base.tts_flags |= TTS_FLAG_SHOULDFREE;
    Ok(())
}

pub fn exec_materialize_slot<'mcx>(slot: &mut SlotData<'mcx>, mcx: Mcx<'mcx>) -> PgResult<()> {
    match slot {
        SlotData::Virtual(v) => virtual_materialize(v, mcx),
        SlotData::Heap(h) => heap_materialize(h, mcx),
        SlotData::Minimal(m) => minimal_materialize(m, mcx),
        SlotData::BufferHeap(b) => buffer_materialize(b, mcx),
    }
}

/// C `ExecCopySlotHeapTuple` (+ the materialize-into-slot fallback the per-kind
/// callbacks perform in `slot_mcx`). Result is allocated in `out_mcx`.
pub fn exec_copy_slot_heap_tuple<'mcx, 'out>(
    slot: &mut SlotData<'mcx>,
    slot_mcx: Mcx<'mcx>,
    out_mcx: Mcx<'out>,
) -> PgResult<HeapTuple<'out>> {
    debug_assert!(!slot.base().is_empty());
    match slot {
        SlotData::Virtual(v) => {
            let desc = v
                .base
                .tts_tupleDescriptor
                .as_ref()
                .expect("copy without descriptor");
            heap_form_tuple(out_mcx, desc, &v.base.tts_values, &v.base.tts_isnull)
        }
        SlotData::Heap(h) => {
            if h.tuple.is_none() {
                heap_materialize(h, slot_mcx)?;
            }
            heap_copytuple(
                out_mcx,
                h.tuple.as_ref().expect("materialize left no tuple"),
            )
        }
        SlotData::BufferHeap(b) => {
            if b.base.tuple.is_none() {
                buffer_materialize(b, slot_mcx)?;
            }
            heap_copytuple(
                out_mcx,
                b.base.tuple.as_ref().expect("materialize left no tuple"),
            )
        }
        SlotData::Minimal(m) => {
            if m.mintuple.is_none() {
                minimal_materialize(m, slot_mcx)?;
            }
            let p = m.mintuple.expect("materialize left no tuple");
            // SAFETY: stored mintuple is live for the duration of the copy.
            heap_tuple_from_minimal_tuple(out_mcx, unsafe { minimal_bytes(p) })
        }
    }
}

/// C `ExecCopySlotMinimalTuple` / `ExecCopySlotMinimalTupleExtra`.
pub fn exec_copy_slot_minimal_tuple<'mcx, 'out>(
    slot: &mut SlotData<'mcx>,
    slot_mcx: Mcx<'mcx>,
    out_mcx: Mcx<'out>,
    extra: usize,
) -> PgResult<MinimalTuple<'out>> {
    debug_assert!(!slot.base().is_empty());
    match slot {
        SlotData::Virtual(v) => {
            let desc = v
                .base
                .tts_tupleDescriptor
                .as_ref()
                .expect("copy without descriptor");
            heap_form_minimal_tuple(out_mcx, desc, &v.base.tts_values, &v.base.tts_isnull, extra)
        }
        SlotData::Heap(h) => {
            if h.tuple.is_none() {
                heap_materialize(h, slot_mcx)?;
            }
            minimal_tuple_from_heap_tuple(
                out_mcx,
                h.tuple.as_ref().expect("materialize left no tuple"),
                extra,
            )
        }
        SlotData::BufferHeap(b) => {
            if b.base.tuple.is_none() {
                buffer_materialize(b, slot_mcx)?;
            }
            minimal_tuple_from_heap_tuple(
                out_mcx,
                b.base.tuple.as_ref().expect("materialize left no tuple"),
                extra,
            )
        }
        SlotData::Minimal(m) => {
            if m.mintuple.is_none() {
                minimal_materialize(m, slot_mcx)?;
            }
            let p = m.mintuple.expect("materialize left no tuple");
            // SAFETY: stored mintuple is live for the duration of the copy.
            heap_copy_minimal_tuple(out_mcx, unsafe { minimal_bytes(p) }, extra)
        }
    }
}

/// [`exec_copy_slot_minimal_tuple`] with the caller's resolve-once
/// [`MinimalFormPlan`] on the no-null virtual-slot arm; bytes identical.
#[inline]
pub fn exec_copy_slot_minimal_tuple_planned<'mcx, 'out>(
    slot: &mut SlotData<'mcx>,
    slot_mcx: Mcx<'mcx>,
    out_mcx: Mcx<'out>,
    extra: usize,
    plan: &MinimalFormPlan,
) -> PgResult<MinimalTuple<'out>> {
    if let SlotData::Virtual(v) = slot {
        debug_assert_eq!(
            plan.natts(),
            v.base.tts_tupleDescriptor.as_ref().expect("copy without descriptor").natts as usize
        );
        if !v.base.tts_isnull[..plan.natts()].contains(&true) {
            return heap_form_minimal_tuple_planned(out_mcx, plan, &v.base.tts_values, extra);
        }
    }
    exec_copy_slot_minimal_tuple(slot, slot_mcx, out_mcx, extra)
}

/// C returns (tuple, *shouldFree); the variants carry the ownership instead.
pub enum FetchedHeapTuple<'a, 'mcx, 'out> {
    Slot(&'a HeapTupleData<'mcx>),
    Copied(HeapTuple<'out>),
}

pub enum FetchedMinimalTuple<'a, 'out> {
    /// Full-image-provenance pointer, live for `'a`; a `&MinimalTupleData` here would shrink provenance to the header (UB on deform).
    Slot(NonNull<MinimalTupleData>, core::marker::PhantomData<&'a MinimalTupleData>),
    Copied(MinimalTuple<'out>),
}

pub fn exec_fetch_slot_heap_tuple<'a, 'mcx, 'out>(
    slot: &'a mut SlotData<'mcx>,
    materialize: bool,
    slot_mcx: Mcx<'mcx>,
    out_mcx: Mcx<'out>,
) -> PgResult<FetchedHeapTuple<'a, 'mcx, 'out>> {
    debug_assert!(!slot.base().is_empty());
    if materialize {
        exec_materialize_slot(slot, slot_mcx)?;
    }
    match slot {
        SlotData::Heap(h) => {
            if h.tuple.is_none() {
                heap_materialize(h, slot_mcx)?;
            }
            Ok(FetchedHeapTuple::Slot(
                h.tuple.as_ref().expect("materialize left no tuple"),
            ))
        }
        SlotData::BufferHeap(b) => {
            if b.base.tuple.is_none() {
                buffer_materialize(b, slot_mcx)?;
            }
            Ok(FetchedHeapTuple::Slot(
                b.base.tuple.as_ref().expect("materialize left no tuple"),
            ))
        }
        other => Ok(FetchedHeapTuple::Copied(exec_copy_slot_heap_tuple(
            other, slot_mcx, out_mcx,
        )?)),
    }
}

pub fn exec_fetch_slot_minimal_tuple<'a, 'mcx, 'out>(
    slot: &'a mut SlotData<'mcx>,
    slot_mcx: Mcx<'mcx>,
    out_mcx: Mcx<'out>,
) -> PgResult<FetchedMinimalTuple<'a, 'out>> {
    debug_assert!(!slot.base().is_empty());
    match slot {
        SlotData::Minimal(m) => {
            if m.mintuple.is_none() {
                minimal_materialize(m, slot_mcx)?;
            }
            let p = m.mintuple.expect("materialize left no tuple");
            Ok(FetchedMinimalTuple::Slot(p, core::marker::PhantomData))
        }
        other => Ok(FetchedMinimalTuple::Copied(exec_copy_slot_minimal_tuple(
            other, slot_mcx, out_mcx, 0,
        )?)),
    }
}

pub fn exec_fetch_slot_heap_tuple_datum<'mcx>(
    slot: &mut SlotData<'mcx>,
    slot_mcx: Mcx<'mcx>,
    out_mcx: Mcx<'_>,
) -> PgResult<Datum> {
    let desc = slot
        .base()
        .tts_tupleDescriptor
        .clone()
        .expect("fetch without descriptor");
    let fetched = exec_fetch_slot_heap_tuple(slot, false, slot_mcx, out_mcx)?;
    let tup: &HeapTupleData<'_> = match &fetched {
        FetchedHeapTuple::Slot(t) => t,
        FetchedHeapTuple::Copied(t) => t,
    };
    heap_copy_tuple_as_datum(out_mcx, tup, &desc)
}

/// C `ExecCopySlot`: dispatch on the DESTINATION slot kind.
pub fn exec_copy_slot<'mcx, 'src>(
    dst: &mut SlotData<'mcx>,
    src: &mut SlotData<'src>,
    dst_mcx: Mcx<'mcx>,
    src_mcx: Mcx<'src>,
) -> PgResult<()> {
    debug_assert!(!src.base().is_empty());
    debug_assert_eq!(
        dst.base().tts_tupleDescriptor.as_ref().map(|d| d.natts),
        src.base().tts_tupleDescriptor.as_ref().map(|d| d.natts)
    );

    match dst {
        SlotData::Virtual(_) => {
            exec_clear_tuple(dst, dst_mcx);
            slot_getallattrs(src);
            let sb = src.base();
            let natts = sb
                .tts_tupleDescriptor
                .as_ref()
                .expect("copyslot without descriptor")
                .natts as usize;
            let db = dst.base_mut();
            db.tts_values[..natts].copy_from_slice(&sb.tts_values[..natts]);
            db.tts_isnull[..natts].copy_from_slice(&sb.tts_isnull[..natts]);
            db.tts_nvalid = natts as AttrNumber;
            db.tts_flags &= !TTS_FLAG_EMPTY;
            exec_materialize_slot(dst, dst_mcx)
        }
        SlotData::Heap(_) => {
            let tuple = exec_copy_slot_heap_tuple(src, src_mcx, dst_mcx)?;
            exec_store_heap_tuple_owned(dst, dst_mcx, tuple);
            Ok(())
        }
        SlotData::Minimal(_) => {
            let mtup = exec_copy_slot_minimal_tuple(src, src_mcx, dst_mcx, 0)?;
            exec_store_minimal_tuple_owned(dst, dst_mcx, mtup);
            Ok(())
        }
        SlotData::BufferHeap(_) => {
            // C tts_buffer_heap_copyslot: a same-kind unmaterialized source
            // shares its buffer pin; only the HeapTupleData header is copied.
            if let SlotData::BufferHeap(s) = src {
                if !s.base.base.should_free() {
                    if let Some(t) = s.base.tuple.as_ref() {
                        debug_assert!(BufferIsValid(s.buffer));
                        // SAFETY: the image lives in the pinned buffer page,
                        // not src's mcx; store_buffer takes dst's own pin, so
                        // the page outlives dst's view (C memcpys the header
                        // for the same reason).
                        let tuple = unsafe {
                            HeapTupleData::from_raw_parts(
                                t.header_ptr(),
                                t.t_len,
                                t.t_self,
                                t.t_tableOid,
                            )
                        };
                        store_buffer(dst, dst_mcx, tuple, s.buffer, false);
                        return Ok(());
                    }
                }
            }
            exec_clear_tuple(dst, dst_mcx);
            let tuple = exec_copy_slot_heap_tuple(src, src_mcx, dst_mcx)?;
            let SlotData::BufferHeap(bd) = dst else {
                unreachable!()
            };
            bd.base.base.tts_flags &= !TTS_FLAG_EMPTY;
            bd.base.tuple = Some(forget_heap(tuple));
            bd.base.base.tts_flags |= TTS_FLAG_SHOULDFREE;
            Ok(())
        }
    }
}

fn force_store_deformed<'mcx>(slot: &mut SlotData<'mcx>, img: TupleImage) {
    let base = slot.base_mut();
    let natts = base
        .tts_tupleDescriptor
        .as_ref()
        .expect("force store without descriptor")
        .natts;
    let mut off = 0u32;
    crate::deform::slot_deform_heap_tuple(base, img, &mut off, natts);
    if (base.tts_nvalid as i32) < natts {
        slot_getmissingattrs(base, base.tts_nvalid as i32, natts);
    }
    base.tts_nvalid = 0;
    exec_store_virtual_tuple(slot);
}

/// C `ExecForceStoreHeapTuple(tuple, slot, shouldFree=false)`.
pub fn exec_force_store_heap_tuple<'mcx>(
    tuple: HeapTupleData<'mcx>,
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    match slot {
        SlotData::Heap(_) => {
            exec_store_heap_tuple(slot, mcx, tuple);
            Ok(())
        }
        SlotData::BufferHeap(_) => {
            exec_clear_tuple(slot, mcx);
            let copy = heap_copytuple(mcx, &tuple)?;
            let SlotData::BufferHeap(b) = slot else {
                unreachable!()
            };
            b.base.base.tts_flags &= !TTS_FLAG_EMPTY;
            b.base.tuple = Some(forget_heap(copy));
            b.base.base.tts_flags |= TTS_FLAG_SHOULDFREE;
            Ok(())
        }
        _ => {
            exec_clear_tuple(slot, mcx);
            force_store_deformed(slot, TupleImage::from_heap(&tuple));
            Ok(())
        }
    }
}

/// C `ExecForceStoreHeapTuple(tuple, slot, shouldFree=true)`.
pub fn exec_force_store_heap_tuple_owned<'mcx>(
    tuple: HeapTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    match slot {
        SlotData::Heap(_) => {
            exec_store_heap_tuple_owned(slot, mcx, tuple);
            Ok(())
        }
        SlotData::BufferHeap(_) => {
            // SAFETY: read-only alias for the copy; the owner frees on drop below.
            exec_force_store_heap_tuple(unsafe { dup_heap_view(&tuple) }, slot, mcx)
        }
        _ => {
            exec_clear_tuple(slot, mcx);
            force_store_deformed(slot, TupleImage::from_heap(&tuple));
            exec_materialize_slot(slot, mcx)
        }
    }
}

/// C `ExecForceStoreMinimalTuple(mtup, slot, shouldFree=true)`. No
/// shouldFree=false borrowing variant exists (header-shrunk provenance, as
/// `exec_store_minimal_tuple_ptr`); add a raw-pointer one when a caller lands.
pub fn exec_force_store_minimal_tuple_owned<'mcx>(
    mtup: MinimalTuple<'mcx>,
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    if matches!(slot, SlotData::Minimal(_)) {
        exec_store_minimal_tuple_owned(slot, mcx, mtup);
        Ok(())
    } else {
        exec_clear_tuple(slot, mcx);
        // SAFETY: mtup is live for the deform below; freed on drop after.
        let img = unsafe {
            TupleImage::from_minimal(NonNull::new_unchecked(
                mtup.as_ptr().cast_mut().cast::<MinimalTupleData>(),
            ))
        };
        force_store_deformed(slot, img);
        exec_materialize_slot(slot, mcx)
    }
}

/// # Safety
/// `data` is a valid composite-type datum: a live, complete
/// `HeapTupleHeader` image readable for its datum length.
pub unsafe fn exec_store_heap_tuple_datum<'mcx>(
    data: Datum,
    slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
) {
    let td = data.as_usize() as *const HeapTupleHeaderData;
    // SAFETY: caller contract.
    let tuple =
        unsafe { HeapTupleData::from_raw_parts(td.cast(), (*td).datum_length(), (*td).t_ctid, 0) };

    exec_clear_tuple(slot, mcx);
    let base = slot.base_mut();
    let SlotBase {
        tts_tupleDescriptor,
        tts_values,
        tts_isnull,
        ..
    } = base;
    let desc = tts_tupleDescriptor
        .as_ref()
        .expect("ExecStoreHeapTupleDatum without descriptor");
    heap_deform_tuple(&tuple, desc, tts_values, tts_isnull);
    exec_store_virtual_tuple(slot);
}

#[cold]
#[inline(never)]
fn no_xact_info(msg: &'static str) -> alloc::boxed::Box<PgError> {
    alloc::boxed::Box::new(PgError::error(msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// C `slot_is_current_xact_tuple`; the xact probe
/// (`TransactionIdIsCurrentTransactionId`) is passed in, not read ambiently.
pub fn slot_is_current_xact_tuple(
    slot: &SlotData<'_>,
    is_current_xact: impl FnOnce(TransactionId) -> bool,
) -> PgResult<bool> {
    debug_assert!(!slot.base().is_empty());
    let tuple = match slot {
        SlotData::Heap(h) => h.tuple.as_ref(),
        SlotData::BufferHeap(b) => b.base.tuple.as_ref(),
        SlotData::Virtual(_) | SlotData::Minimal(_) => {
            return Err(no_xact_info(
                "don't have transaction information for this type of tuple",
            ))
        }
    };
    match tuple {
        Some(t) => Ok(is_current_xact(t.t_data().xmin_raw())),
        None => Err(no_xact_info("don't have a storage tuple in this context")),
    }
}

// execute_attr_map_slot (tupconvert.c): attmap[out-1] = in attno, 0 = NULL.
pub fn execute_attr_map_slot<'mcx>(
    attmap: &[i16],
    in_slot: &mut SlotData<'mcx>,
    out_slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
) {
    debug_assert!(in_slot.base().tts_tupleDescriptor.is_some());
    debug_assert!(out_slot.base().tts_tupleDescriptor.is_some());

    let outnatts = attmap.len();
    slot_getallattrs(in_slot);
    exec_clear_tuple(out_slot, mcx);

    for i in 0..outnatts {
        let j = attmap[i] - 1;
        let (value, isnull) = if j < 0 {
            (Datum::null(), true)
        } else {
            let base = in_slot.base();
            (base.tts_values[j as usize], base.tts_isnull[j as usize])
        };
        let out_base = out_slot.base_mut();
        out_base.tts_values[i] = value;
        out_base.tts_isnull[i] = isnull;
    }

    exec_store_virtual_tuple(out_slot);
}
