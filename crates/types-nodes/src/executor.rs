//! Tuple-table-slot vocabulary (executor/tuptable.h), trimmed.
//!
//! Ports so far consume only slot emptiness (`TTS_EMPTY`, the `TupIsNull`
//! test) and the slot-ops singleton identity token; the slot payload model
//! grows when the slot-owning units (execTuples and friends) land.

/// `TTS_FLAG_EMPTY` (tuptable.h) — true = slot is empty.
pub const TTS_FLAG_EMPTY: u16 = 1 << 1;

/// `TupleTableSlot` (tuptable.h), trimmed to the shared header bits ports
/// consume.
#[derive(Clone, Debug)]
pub struct TupleTableSlot {
    /// `uint16 tts_flags` — `TTS_FLAG_*` boolean states of this slot.
    pub tts_flags: u16,
}

impl Default for TupleTableSlot {
    /// A freshly made slot is empty (`MakeTupleTableSlot` sets `TTS_EMPTY`).
    fn default() -> Self {
        TupleTableSlot {
            tts_flags: TTS_FLAG_EMPTY,
        }
    }
}

impl TupleTableSlot {
    /// `TTS_EMPTY(slot)` — the slot contains no tuple.
    pub const fn is_empty(&self) -> bool {
        self.tts_flags & TTS_FLAG_EMPTY != 0
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
