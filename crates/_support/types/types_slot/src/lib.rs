#![no_std]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::rc::Rc;
use core::ptr::NonNull;

use ::datum::Datum;
use ::mcx::{vec_from_elem_in, Mcx, PgVec};
use ::types_core::{AttrNumber, Buffer, Oid};
use ::types_tuple::{HeapTupleData, ItemPointerData, MinimalTupleData, TupleDescData};

pub const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
pub const EXEC_FLAG_EXPLAIN_GENERIC: i32 = 0x0002;
pub const EXEC_FLAG_REWIND: i32 = 0x0004;
pub const EXEC_FLAG_BACKWARD: i32 = 0x0008;
pub const EXEC_FLAG_MARK: i32 = 0x0010;
pub const EXEC_FLAG_SKIP_TRIGGERS: i32 = 0x0020;
pub const EXEC_FLAG_WITH_NO_DATA: i32 = 0x0040;
/// pgrust-only (no C counterpart): EXPLAIN (ENGINE) — arm per-node engine
/// attribution capture. High bit, clear of C's execnodes.h flag space
/// (0x0001..0x0040 above). Next free pgrust eflag is 0x0002_0000 (reserved,
/// unassigned — integration-contract R-KNOBS registry).
pub const EXEC_FLAG_ENGINE_REPORT: i32 = 0x0001_0000;

pub const TTS_FLAG_EMPTY: u16 = 1 << 1;
pub const TTS_FLAG_SHOULDFREE: u16 = 1 << 2;
pub const TTS_FLAG_SLOW: u16 = 1 << 3;
pub const TTS_FLAG_FIXED: u16 = 1 << 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TupleSlotKind {
    Virtual,
    HeapTuple,
    MinimalTuple,
    BufferHeapTuple,
}

impl TupleSlotKind {
    #[inline]
    pub const fn has_get_heap_tuple(self) -> bool {
        matches!(self, TupleSlotKind::HeapTuple | TupleSlotKind::BufferHeapTuple)
    }

    #[inline]
    pub const fn has_get_minimal_tuple(self) -> bool {
        matches!(self, TupleSlotKind::MinimalTuple)
    }
}

// Invariant: tts_values.len() == tts_isnull.len() == descriptor natts and
// 0 <= tts_nvalid <= len; the getattr lane's unchecked indexing rests on it.
#[derive(Debug)]
pub struct TupleTableSlot<'mcx> {
    pub tts_flags: u16,
    pub tts_nvalid: AttrNumber,
    pub tts_ops: TupleSlotKind,
    pub tts_tupleDescriptor: Option<Rc<TupleDescData<'mcx>>>,
    pub tts_values: PgVec<'mcx, Datum>,
    pub tts_isnull: PgVec<'mcx, bool>,
    pub tts_tid: ItemPointerData,
    pub tts_tableOid: Oid,
}

const _: () = assert!(core::mem::size_of::<TupleTableSlot<'_>>() <= 128);

impl<'mcx> TupleTableSlot<'mcx> {
    pub fn new_in(mcx: Mcx<'mcx>, kind: TupleSlotKind) -> Self {
        TupleTableSlot {
            tts_flags: TTS_FLAG_EMPTY,
            tts_nvalid: 0,
            tts_ops: kind,
            tts_tupleDescriptor: None,
            tts_values: PgVec::new_in(mcx),
            tts_isnull: PgVec::new_in(mcx),
            tts_tid: ItemPointerData::invalid(),
            tts_tableOid: 0,
        }
    }

    pub fn set_descriptor(&mut self, mcx: Mcx<'mcx>, desc: Rc<TupleDescData<'mcx>>) {
        debug_assert!(self.is_empty());
        let natts = desc.natts as usize;
        self.tts_values = vec_from_elem_in(mcx, Datum::null(), natts);
        self.tts_isnull = vec_from_elem_in(mcx, true, natts);
        self.tts_tupleDescriptor = Some(desc);
        self.tts_nvalid = 0;
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tts_flags & TTS_FLAG_EMPTY != 0
    }

    #[inline]
    pub fn should_free(&self) -> bool {
        self.tts_flags & TTS_FLAG_SHOULDFREE != 0
    }

    #[inline]
    pub fn is_slow(&self) -> bool {
        self.tts_flags & TTS_FLAG_SLOW != 0
    }

    #[inline]
    pub fn is_fixed(&self) -> bool {
        self.tts_flags & TTS_FLAG_FIXED != 0
    }

    #[inline]
    pub fn mark_empty(&mut self) {
        self.tts_flags |= TTS_FLAG_EMPTY;
        self.tts_flags &= !TTS_FLAG_SHOULDFREE;
        self.tts_nvalid = 0;
    }

    #[inline]
    pub fn mark_not_empty(&mut self) {
        self.tts_flags &= !TTS_FLAG_EMPTY;
    }

    // getsomeattrs_int is C's extern slot_getsomeattrs_int (the per-kind
    // deform), monomorphized to a direct call; postcondition tts_nvalid >= attnum.
    #[inline]
    pub fn slot_getsomeattrs(&mut self, attnum: i32, getsomeattrs_int: impl FnOnce(&mut Self, i32)) {
        if (self.tts_nvalid as i32) < attnum {
            getsomeattrs_int(self, attnum);
            debug_assert!(self.tts_nvalid as i32 >= attnum);
            debug_assert!(
                (attnum as usize) <= self.tts_values.len()
                    && self.tts_values.len() == self.tts_isnull.len()
            );
        }
    }

    #[inline]
    pub fn slot_getallattrs(&mut self, getsomeattrs_int: impl FnOnce(&mut Self, i32)) {
        let natts = self
            .tts_tupleDescriptor
            .as_ref()
            .expect("slot_getallattrs without descriptor")
            .natts;
        self.slot_getsomeattrs(natts, getsomeattrs_int);
    }

    #[inline]
    pub fn slot_getattr(
        &mut self,
        attnum: i32,
        isnull: &mut bool,
        getsomeattrs_int: impl FnOnce(&mut Self, i32),
    ) -> Datum {
        debug_assert!(attnum > 0);
        self.slot_getsomeattrs(attnum, getsomeattrs_int);
        let i = (attnum - 1) as usize;
        // SAFETY: attnum <= tts_nvalid <= len by the struct invariant plus the
        // getsomeattrs_int postcondition (debug-asserted above).
        unsafe {
            *isnull = *self.tts_isnull.get_unchecked(i);
            *self.tts_values.get_unchecked(i)
        }
    }

    #[inline]
    pub fn slot_attisnull(
        &mut self,
        attnum: i32,
        getsomeattrs_int: impl FnOnce(&mut Self, i32),
    ) -> bool {
        debug_assert!(attnum > 0);
        self.slot_getsomeattrs(attnum, getsomeattrs_int);
        // SAFETY: as in slot_getattr.
        unsafe { *self.tts_isnull.get_unchecked((attnum - 1) as usize) }
    }
}

pub type SlotBase<'mcx> = TupleTableSlot<'mcx>;

#[repr(C)]
pub struct VirtualTupleTableSlot<'mcx> {
    pub base: SlotBase<'mcx>,
    pub data: PgVec<'mcx, u8>,
}

// C's HeapTuple pointer + inline tupdata workspace collapse into one by-value
// HeapTupleData.
#[repr(C)]
pub struct HeapTupleTableSlot<'mcx> {
    pub base: SlotBase<'mcx>,
    pub tuple: Option<HeapTupleData<'mcx>>,
    pub off: u32,
    // Deform-JIT arm (docs/optimizations/jit-deform.md): set only by scan
    // nodes that passed the row gate against this slot's relation; the fresh
    // (nvalid == 0) null-free deform runs the kernel, everything else keeps
    // the interpreted walk.
    pub jit_deform: Option<alloc::rc::Rc<jit_deform::DeformKernel>>,
}

#[repr(C)]
pub struct BufferHeapTupleTableSlot<'mcx> {
    pub base: HeapTupleTableSlot<'mcx>,
    // While not InvalidBuffer the slot holds a pin on that page (SHOULDFREE
    // must then be unset — the tuple points into the buffer).
    pub buffer: Buffer,
}

#[repr(C)]
pub struct MinimalTupleTableSlot<'mcx> {
    pub base: SlotBase<'mcx>,
    // The wrapper header sits MINIMAL_TUPLE_OFFSET before the minimal body so
    // deform sees a regular physical tuple.
    pub tuple: Option<HeapTupleData<'mcx>>,
    // Valid until the slot is cleared or overwritten; owned iff SHOULDFREE.
    pub mintuple: Option<NonNull<MinimalTupleData>>,
    pub off: u32,
}

// repr(C, u8) + base-first repr(C) payloads: every variant's SlotBase sits at
// one fixed offset (C's "TupleTableSlot is the first member" inheritance), so
// base()/base_mut() compile to a constant GEP -- no per-access variant probe
// (exectuples record's justified 1.4x generic-getattr class closes on this).
#[repr(C, u8)]
pub enum SlotData<'mcx> {
    Virtual(VirtualTupleTableSlot<'mcx>),
    Heap(HeapTupleTableSlot<'mcx>),
    Minimal(MinimalTupleTableSlot<'mcx>),
    BufferHeap(BufferHeapTupleTableSlot<'mcx>),
}

impl<'mcx> SlotData<'mcx> {
    #[inline]
    pub fn kind(&self) -> TupleSlotKind {
        match self {
            SlotData::Virtual(_) => TupleSlotKind::Virtual,
            SlotData::Heap(_) => TupleSlotKind::HeapTuple,
            SlotData::Minimal(_) => TupleSlotKind::MinimalTuple,
            SlotData::BufferHeap(_) => TupleSlotKind::BufferHeapTuple,
        }
    }

    #[inline]
    pub fn base(&self) -> &SlotBase<'mcx> {
        match self {
            SlotData::Virtual(s) => &s.base,
            SlotData::Heap(s) => &s.base,
            SlotData::Minimal(s) => &s.base,
            SlotData::BufferHeap(s) => &s.base.base,
        }
    }

    #[inline]
    pub fn base_mut(&mut self) -> &mut SlotBase<'mcx> {
        match self {
            SlotData::Virtual(s) => &mut s.base,
            SlotData::Heap(s) => &mut s.base,
            SlotData::Minimal(s) => &mut s.base,
            SlotData::BufferHeap(s) => &mut s.base.base,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;
    use ::types_tuple::FormData_pg_attribute;

    fn desc_with_natts<'mcx>(mcx: Mcx<'mcx>, natts: i32) -> Rc<TupleDescData<'mcx>> {
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        for i in 0..natts {
            let att = FormData_pg_attribute {
                attnum: (i + 1) as i16,
                attlen: 4,
                attbyval: true,
                attalign: ::types_tuple::TYPALIGN_INT,
                attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
                ..Default::default()
            };
            compact.push(::types_tuple::CompactAttribute::populate_from(&att));
            attrs.push(att);
        }
        Rc::new(TupleDescData {
            natts,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    #[test]
    fn flags_match_tuptable_h() {
        assert_eq!(TTS_FLAG_EMPTY, 1 << 1);
        assert_eq!(TTS_FLAG_SHOULDFREE, 1 << 2);
        assert_eq!(TTS_FLAG_SLOW, 1 << 3);
        assert_eq!(TTS_FLAG_FIXED, 1 << 4);

        assert_eq!(EXEC_FLAG_EXPLAIN_ONLY, 0x0001);
        assert_eq!(EXEC_FLAG_EXPLAIN_GENERIC, 0x0002);
        assert_eq!(EXEC_FLAG_REWIND, 0x0004);
        assert_eq!(EXEC_FLAG_BACKWARD, 0x0008);
        assert_eq!(EXEC_FLAG_MARK, 0x0010);
        assert_eq!(EXEC_FLAG_SKIP_TRIGGERS, 0x0020);
        assert_eq!(EXEC_FLAG_WITH_NO_DATA, 0x0040);
    }

    #[test]
    fn fresh_slot_and_clear_semantics() {
        let ctx = MemoryContext::new("test");
        let mut slot = TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::Virtual);
        assert!(slot.is_empty());
        assert!(!slot.should_free());
        assert_eq!(slot.tts_nvalid, 0);
        assert!(slot.tts_tupleDescriptor.is_none());

        slot.set_descriptor(ctx.mcx(), desc_with_natts(ctx.mcx(), 3));
        assert_eq!(slot.tts_values.len(), 3);
        assert_eq!(slot.tts_isnull.len(), 3);

        slot.mark_not_empty();
        slot.tts_flags |= TTS_FLAG_SHOULDFREE;
        slot.tts_nvalid = 3;
        assert!(!slot.is_empty());
        slot.mark_empty();
        assert!(slot.is_empty());
        assert!(!slot.should_free());
        assert_eq!(slot.tts_nvalid, 0);
    }

    #[test]
    fn getattr_hit_path_skips_deform_and_miss_path_calls_it() {
        let ctx = MemoryContext::new("test");
        let mut slot = TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::Virtual);
        slot.set_descriptor(ctx.mcx(), desc_with_natts(ctx.mcx(), 3));
        slot.tts_values[0] = Datum::from_i32(11);
        slot.tts_values[1] = Datum::from_i32(22);
        slot.tts_isnull[0] = false;
        slot.tts_isnull[1] = true;
        slot.tts_nvalid = 2;
        slot.mark_not_empty();

        let mut isnull = false;
        let d = slot.slot_getattr(1, &mut isnull, |_, _| panic!("hit path must not deform"));
        assert_eq!(d.as_i32(), 11);
        assert!(!isnull);
        assert!(slot.slot_attisnull(2, |_, _| panic!("hit path must not deform")));

        let d = slot.slot_getattr(3, &mut isnull, |s, upto| {
            assert_eq!(upto, 3);
            s.tts_values[2] = Datum::from_i32(33);
            s.tts_isnull[2] = false;
            s.tts_nvalid = 3;
        });
        assert_eq!(d.as_i32(), 33);
        assert!(!isnull);

        slot.tts_nvalid = 0;
        slot.slot_getallattrs(|s, upto| {
            assert_eq!(upto, 3);
            s.tts_nvalid = 3;
        });
        assert_eq!(slot.tts_nvalid, 3);
    }

    #[test]
    fn slotdata_downcasts_and_kind_capabilities() {
        let ctx = MemoryContext::new("test");
        let mut slot = SlotData::Virtual(VirtualTupleTableSlot {
            base: TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::Virtual),
            data: PgVec::new_in(ctx.mcx()),
        });
        assert_eq!(slot.kind(), TupleSlotKind::Virtual);
        assert!(slot.base().is_empty());
        slot.base_mut().mark_not_empty();
        assert!(!slot.base().is_empty());

        let buf = SlotData::BufferHeap(BufferHeapTupleTableSlot {
            base: HeapTupleTableSlot {
                base: TupleTableSlot::new_in(ctx.mcx(), TupleSlotKind::BufferHeapTuple),
                tuple: None,
                off: 0,
                jit_deform: None,
            },
            buffer: 0,
        });
        assert_eq!(buf.kind(), TupleSlotKind::BufferHeapTuple);
        assert!(buf.base().is_empty());

        assert!(TupleSlotKind::HeapTuple.has_get_heap_tuple());
        assert!(TupleSlotKind::BufferHeapTuple.has_get_heap_tuple());
        assert!(!TupleSlotKind::Virtual.has_get_heap_tuple());
        assert!(!TupleSlotKind::MinimalTuple.has_get_heap_tuple());
        assert!(TupleSlotKind::MinimalTuple.has_get_minimal_tuple());
        assert!(!TupleSlotKind::HeapTuple.has_get_minimal_tuple());
    }
}
