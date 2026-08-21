//! AB-7.3 — the per-batch guard-flag word + demotion protocol (v4 delta).
//!
//! v4 generalizes v3's single threaded overflow bool to a per-batch
//! GUARD-FLAG WORD (u32 bitset), one bit per AD-1 guard class. Bit
//! assignments are FROZEN APPEND-ONLY by the ABI; adding a bit is an ABI
//! amendment, never a local edit. Any set bit DEMOTES the batch to the
//! interpreted twin (Amendment 1) with byte/error identity through the
//! demotion; demotions are first-class census events (PC-6.3).
//!
//! PERF guards (TinyInputFloor class) are a DISTINCT vocabulary: they
//! elect, they never set guard bits.

/// Checked-kernel wrap/overflow verdict (the v3 threaded bool, now bit 0).
pub const GUARD_OVERFLOW: u32 = 1 << 0;
/// Collation licensing failed for a code-compare fast path (AD-1).
pub const GUARD_COLLATION: u32 = 1 << 1;
/// Encoding licensing failed (`STATSF_ALL_ASCII`-class witnesses absent).
pub const GUARD_ENCODING: u32 = 1 << 2;
/// AB-2.2 dict epoch tag mismatch: the lane's `DictEpochKey` is stale.
pub const GUARD_EPOCH: u32 = 1 << 3;
/// AP-4/OD-9: the per-batch vectorized max-code check failed vs dict len.
pub const GUARD_CODE_BOUND: u32 = 1 << 4;
/// Numeric domain guard (AD-1 numeric class).
pub const GUARD_NUMERIC_DOMAIN: u32 = 1 << 5;
// bits 6..31 reserved — append-only by ABI amendment.

/// The per-batch guard-flag word (AB-7.3). Producers OR bits in; the
/// batching layer reads exactly once per batch at the demotion decision.
/// A flagged batch NEVER uses its wrapped/fast-path output — it replays
/// through the per-row reference drive (AB-7.2, Ruling 1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GuardWord(pub u32);

impl GuardWord {
    #[inline]
    pub const fn clear() -> GuardWord {
        GuardWord(0)
    }

    /// OR a guard class in (cheap detection is the kernel's only duty).
    #[inline]
    pub fn raise(&mut self, class_bit: u32) {
        self.0 |= class_bit;
    }

    /// TRUE iff the batch must demote to the interpreted twin.
    #[inline]
    pub const fn demotes(self) -> bool {
        self.0 != 0
    }

    /// TRUE iff `class_bit` is raised.
    #[inline]
    pub const fn has(self, class_bit: u32) -> bool {
        self.0 & class_bit != 0
    }
}
