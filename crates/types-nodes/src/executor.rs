//! Tuple-table-slot vocabulary (executor/tuptable.h), trimmed.
//!
//! Ports so far consume slot emptiness (`TTS_EMPTY`, the `TupIsNull` test),
//! fixedness (`TTS_FIXED`), and the slot-ops singleton identity token; the
//! slot payload model grows when the slot-owning units (execTuples and
//! friends) land.

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

/// `TTS_FLAG_EMPTY` (tuptable.h) — true = slot is empty.
pub const TTS_FLAG_EMPTY: u16 = 1 << 1;
/// `TTS_FLAG_FIXED` (tuptable.h) — true = the slot's tuple descriptor and
/// memory layout are fixed for its lifetime.
pub const TTS_FLAG_FIXED: u16 = 1 << 4;

/// `TupleTableSlot` (tuptable.h), trimmed to the shared header bits ports
/// consume.
#[derive(Clone, Debug)]
pub struct TupleTableSlot {
    /// `uint16 tts_flags` — `TTS_FLAG_*` boolean states of this slot.
    pub tts_flags: u16,
    /// `ItemPointerData tts_tid` — TID of the tuple stored in the slot (the
    /// row's `ctid`; valid only when the slot holds a physical tuple). The
    /// TID-scan `TidRecheck` reads this to confirm a tuple's identity.
    pub tts_tid: types_tuple::heaptuple::ItemPointerData,
    /// `const TupleTableSlotOps *const tts_ops` — slot implementation
    /// identity (the owned token for the `&TTSOps*` singleton pointer).
    pub tts_ops: TupleSlotKind,
    /// `Oid tts_tableOid` — table OID this row came from (the value reported
    /// by the `tableoid` system column). `InvalidOid` when unset.
    pub tts_tableOid: types_core::primitive::Oid,
}

impl Default for TupleTableSlot {
    /// A freshly made slot is empty (`MakeTupleTableSlot` sets `TTS_EMPTY`);
    /// virtual is the default implementation.
    fn default() -> Self {
        TupleTableSlot {
            tts_flags: TTS_FLAG_EMPTY,
            tts_tid: types_tuple::heaptuple::ItemPointerData::default(),
            tts_ops: TupleSlotKind::Virtual,
            tts_tableOid: 0,
        }
    }
}

impl TupleTableSlot {
    /// `TTS_EMPTY(slot)` — the slot contains no tuple.
    pub const fn is_empty(&self) -> bool {
        self.tts_flags & TTS_FLAG_EMPTY != 0
    }

    /// `TTS_FIXED(slot)` — the slot's descriptor/layout is fixed.
    pub const fn is_fixed(&self) -> bool {
        self.tts_flags & TTS_FLAG_FIXED != 0
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
