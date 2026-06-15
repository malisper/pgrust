//! Tuple-table-slot vocabulary (executor/tuptable.h).
//!
//! This is the unified `TupleTableSlot` (tuptable.h): one slot type carrying
//! both the header bits every port consumes (`tts_flags`/`tts_tid`/`tts_ops`/
//! `tts_tableOid`) AND the per-attribute payload C puts directly in the struct
//! (`tts_nvalid`/`tts_tupleDescriptor`/`tts_values`/`tts_isnull`). The four
//! concrete `Virtual/Heap/Minimal/BufferHeap` superstructures that embed this
//! base, the `SlotData` enum, and the per-kind dispatch callbacks live in the
//! slot-owning units (`types-nodes::tuptable` for the structs;
//! `backend-executor-execTuples` for the ops). Mirrors C exactly: there is one
//! `TupleTableSlot` and the per-kind behaviour is selected by `tts_ops`.
#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::primitive::{AttrNumber, Oid, Size};
use types_error::PgResult;
use types_storage::buf::Buffer;
use types_tuple::backend_access_common_heaptuple::{Datum, FormedMinimalTuple, FormedTuple};
use types_tuple::heaptuple::{HeapTupleData, ItemPointerData, TupleDesc};

// `EXEC_FLAG_*` (executor.h) — the eflags bits passed down ExecutorStart /
// ExecInitNode. Shared vocabulary: every executor node unit and the tuplestore
// owner consume the same bits.

/// `EXEC_FLAG_EXPLAIN_ONLY` (executor.h) — EXPLAIN, no ANALYZE.
pub const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
/// `EXEC_FLAG_EXPLAIN_GENERIC` (executor.h) — EXPLAIN (GENERIC_PLAN).
pub const EXEC_FLAG_EXPLAIN_GENERIC: i32 = 0x0002;
/// `EXEC_FLAG_REWIND` (executor.h) — need efficient rescan.
pub const EXEC_FLAG_REWIND: i32 = 0x0004;
/// `EXEC_FLAG_BACKWARD` (executor.h) — need backward scan.
pub const EXEC_FLAG_BACKWARD: i32 = 0x0008;
/// `EXEC_FLAG_MARK` (executor.h) — need mark/restore.
pub const EXEC_FLAG_MARK: i32 = 0x0010;
/// `EXEC_FLAG_SKIP_TRIGGERS` (executor.h) — skip AfterTrigger setup.
pub const EXEC_FLAG_SKIP_TRIGGERS: i32 = 0x0020;
/// `EXEC_FLAG_WITH_NO_DATA` (executor.h) — REFRESH ... WITH NO DATA.
pub const EXEC_FLAG_WITH_NO_DATA: i32 = 0x0040;

/// `TTS_FLAG_SHOULDFREE` (tuptable.h) — true if we should `pfree` the slot's
/// stored tuple on clear.
pub const TTS_FLAG_SHOULDFREE: u16 = 1 << 0;
/// `TTS_FLAG_EMPTY` (tuptable.h) — true = slot is empty.
pub const TTS_FLAG_EMPTY: u16 = 1 << 1;
/// `TTS_FLAG_SLOW` (tuptable.h) — saved state for `slot_deform_heap_tuple`.
pub const TTS_FLAG_SLOW: u16 = 1 << 2;
/// `TTS_FLAG_FIXED` (tuptable.h) — true = the slot's tuple descriptor and
/// memory layout are fixed for its lifetime.
pub const TTS_FLAG_FIXED: u16 = 1 << 4;

/// `TupleTableSlot` (tuptable.h) — the unified slot: header bits plus the
/// per-attribute value/null payload C carries directly in the struct.
///
/// C additionally embeds the per-kind callback table pointer (`tts_ops`); here
/// the kind is recorded as the [`TupleSlotKind`] identity token and the
/// callbacks are dispatched off it by `backend-executor-execTuples`. The
/// slot's memory context (`tts_mcxt`) is modeled by the `'mcx` arena the
/// payload vectors allocate in.
#[derive(Debug)]
pub struct TupleTableSlot<'mcx> {
    /// `uint16 tts_flags` — `TTS_FLAG_*` boolean states of this slot.
    pub tts_flags: u16,
    /// `AttrNumber tts_nvalid` — # of valid values in `tts_values`.
    pub tts_nvalid: AttrNumber,
    /// `const TupleTableSlotOps *const tts_ops` — slot implementation
    /// identity (the owned token for the `&TTSOps*` singleton pointer).
    pub tts_ops: TupleSlotKind,
    /// `TupleDesc tts_tupleDescriptor` — slot's tuple descriptor.
    pub tts_tupleDescriptor: TupleDesc<'mcx>,
    /// `Datum *tts_values` — current per-attribute values. Each element is a
    /// [`Datum`] (`ByVal(word)` / `ByRef(bytes)`), faithfully modelling C's
    /// `Datum *` where a by-reference `Datum` is a pointer into owned bytes.
    pub tts_values: PgVec<'mcx, Datum<'mcx>>,
    /// `bool *tts_isnull` — current per-attribute isnull flags.
    pub tts_isnull: PgVec<'mcx, bool>,
    /// `ItemPointerData tts_tid` — TID of the tuple stored in the slot (the
    /// row's `ctid`; valid only when the slot holds a physical tuple).
    pub tts_tid: ItemPointerData,
    /// `Oid tts_tableOid` — table OID this row came from (`tableoid` system
    /// column); `InvalidOid` when unset.
    pub tts_tableOid: Oid,
}

impl<'mcx> TupleTableSlot<'mcx> {
    /// A header-only slot built in the given `mcx`: empty payload arrays and no
    /// descriptor — exactly the state a freshly-allocated virtual slot is in
    /// before any tuple is stored or descriptor assigned. `MakeTupleTableSlot`
    /// sets `TTS_EMPTY`; virtual is the default implementation.
    pub fn new_in(mcx: mcx::Mcx<'mcx>) -> Self {
        TupleTableSlot {
            tts_flags: TTS_FLAG_EMPTY,
            tts_nvalid: 0,
            tts_ops: TupleSlotKind::Virtual,
            tts_tupleDescriptor: None,
            tts_values: PgVec::new_in(mcx),
            tts_isnull: PgVec::new_in(mcx),
            tts_tid: ItemPointerData::default(),
            tts_tableOid: 0,
        }
    }

    /// `TTS_EMPTY(slot)` — the slot contains no tuple.
    pub fn is_empty(&self) -> bool {
        self.tts_flags & TTS_FLAG_EMPTY != 0
    }

    /// `TTS_SHOULDFREE(slot)`.
    pub fn should_free(&self) -> bool {
        self.tts_flags & TTS_FLAG_SHOULDFREE != 0
    }

    /// `TTS_FIXED(slot)` — the slot's descriptor/layout is fixed.
    pub fn is_fixed(&self) -> bool {
        self.tts_flags & TTS_FLAG_FIXED != 0
    }

    /// `slot->tts_flags |= TTS_EMPTY; slot->tts_flags &= ~TTS_SHOULDFREE;
    /// slot->tts_nvalid = 0;` (the `*_clear` callback tail).
    pub fn mark_empty(&mut self) {
        self.tts_flags |= TTS_FLAG_EMPTY;
        self.tts_flags &= !TTS_FLAG_SHOULDFREE;
        self.tts_nvalid = 0;
    }

    /// `slot->tts_flags &= ~TTS_EMPTY;`
    pub fn mark_not_empty(&mut self) {
        self.tts_flags &= !TTS_FLAG_EMPTY;
    }
}

/// The `&TTSOps*` singleton identity (tuptable.h / execTuples.c): C code
/// selects a slot implementation by passing one of the four `TupleTableSlotOps`
/// singletons (`&TTSOpsVirtual`, `&TTSOpsHeapTuple`, `&TTSOpsMinimalTuple`,
/// `&TTSOpsBufferHeapTuple`) and only ever observes pointer identity. The owned
/// model carries that identity as this token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TupleSlotKind {
    Virtual,
    HeapTuple,
    MinimalTuple,
    BufferHeapTuple,
}

/// `Buffer` re-export so the buffer-heap slot superstructure (below) names the
/// same type via this crate.
pub type SlotBuffer = Buffer;

// ===========================================================================
// Tuple-table-slot payload model (executor/tuptable.h) — the four concrete
// `Virtual/Heap/Minimal/BufferHeap` superstructures that embed the unified
// [`TupleTableSlot`] base, plus the [`SlotData`] enum (the idiomatic
// `TupleTableSlot *` that may point at any subtype). These live in the same
// leaf crate as the slot base (as `tuptable.h` defines them together in C), so
// the table-AM vtable (`types-tableam`) can name the payload-bearing
// [`SlotData`] its slot-bearing callbacks (`scan_getnextslot` /
// `index_fetch_tuple` / `tuple_fetch_row_version` / `tuple_insert` / …) hand to
// the AM — exactly as C passes a `TupleTableSlot *` that actually points at a
// `BufferHeapTupleTableSlot`. The per-kind dispatch callbacks (clear/
// materialize/getsomeattrs/copyslot/store/…) live in
// `backend-executor-execTuples`; this crate carries only the owned structs +
// pure accessors. `types-nodes::tuptable` re-exports all of these so existing
// `types_nodes::tuptable::SlotData` / `types_nodes::SlotData` imports keep
// working.
//
// Mirroring C exactly (tuptable.h); the body-bearing `HeapTuple`/`MinimalTuple`
// fields are carried as the [`FormedTuple`]/[`FormedMinimalTuple`] carriers
// (header + owned data-area bytes) the form/deform owner already produces:
//
// * `VirtualTupleTableSlot { TupleTableSlot base; char *data; }`
// * `HeapTupleTableSlot   { TupleTableSlot base; HeapTuple tuple; uint32 off;
//    HeapTupleData tupdata; }`
// * `BufferHeapTupleTableSlot { HeapTupleTableSlot base; Buffer buffer; }`
// * `MinimalTupleTableSlot { TupleTableSlot base; HeapTuple tuple;
//    MinimalTuple mintuple; HeapTupleData minhdr; uint32 off; }`

/// `SlotBase<'mcx>` is exactly the unified [`TupleTableSlot`] (tuptable.h): the
/// header bits and the per-attribute value/null payload (`tts_nvalid`,
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
    /// header-only `HeapTuple` lacks. `None` when no tuple is stored.
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

/// SCAFFOLD anchor so an otherwise data-only module exposes a fallible builder
/// signature shape; the real builders live in `backend-executor-execTuples`.
#[allow(dead_code)]
fn _scaffold_marker<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<()> {
    Ok(())
}
