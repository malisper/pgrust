use std::sync::atomic::{AtomicU32, Ordering};

use crate::storage::smgr::{ForkNumber, RelFileLocator, BLCKSZ};

pub const PAGE_SIZE: usize = BLCKSZ;
pub type Page = [u8; PAGE_SIZE];

pub type ClientId = u32;
pub type BufferId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BufferTag {
    pub rel: RelFileLocator,
    pub fork: ForkNumber,
    pub block: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOp {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingIo {
    pub buffer_id: BufferId,
    pub op: IoOp,
    pub tag: BufferTag,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestPageResult {
    Hit { buffer_id: BufferId },
    ReadIssued { buffer_id: BufferId },
    WaitingOnRead { buffer_id: BufferId },
    AllBuffersPinned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushResult {
    WriteIssued,
    AlreadyClean,
    InProgress,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferStateView {
    pub tag: Option<BufferTag>,
    pub valid: bool,
    pub dirty: bool,
    pub io_in_progress: bool,
    pub io_error: bool,
    pub pin_count: usize,
    pub usage_count: u8,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BufferUsageStats {
    pub shared_hit: u64,
    pub shared_read: u64,
    pub shared_written: u64,
}

// --- Atomic buffer state bit layout ---
// Matches PostgreSQL's pg_atomic_uint32 state in BufferDesc.
//
//   Bits  0-13  (14 bits): pin_count      max 16383
//   Bits 14-17  ( 4 bits): usage_count    range 0-15
//   Bit  18:               BM_VALID
//   Bit  19:               BM_DIRTY
//   Bit  20:               BM_IO_IN_PROGRESS
//   Bit  21:               BM_IO_ERROR

const PIN_COUNT_MASK: u32 = 0x0000_3FFF; // bits 0-13
const USAGE_COUNT_SHIFT: u32 = 14;
const USAGE_COUNT_MASK: u32 = 0x0003_C000; // bits 14-17
const BM_VALID: u32 = 1 << 18;
const BM_DIRTY: u32 = 1 << 19;
const BM_IO_IN_PROGRESS: u32 = 1 << 20;
const BM_IO_ERROR: u32 = 1 << 21;

/// Atomic buffer frame state. All metadata (pin count, usage count, flags)
/// packed into a single u32 for lock-free access.
pub struct BufferState(AtomicU32);

impl BufferState {
    pub fn new() -> Self {
        Self(AtomicU32::new(0))
    }

    pub fn load(&self) -> u32 {
        self.0.load(Ordering::Acquire)
    }

    pub fn store(&self, val: u32) {
        self.0.store(val, Ordering::Release);
    }

    // --- Field accessors (snapshot from a loaded value) ---

    pub fn pin_count(&self) -> u32 {
        self.load() & PIN_COUNT_MASK
    }

    pub fn usage_count(&self) -> u8 {
        ((self.load() & USAGE_COUNT_MASK) >> USAGE_COUNT_SHIFT) as u8
    }

    pub fn is_valid(&self) -> bool {
        self.load() & BM_VALID != 0
    }

    pub fn is_dirty(&self) -> bool {
        self.load() & BM_DIRTY != 0
    }

    pub fn is_io_in_progress(&self) -> bool {
        self.load() & BM_IO_IN_PROGRESS != 0
    }

    pub fn is_io_error(&self) -> bool {
        self.load() & BM_IO_ERROR != 0
    }

    // --- Atomic pin operations ---

    /// Atomically increment pin count. Returns previous state.
    pub fn increment_pin(&self) -> u32 {
        // pin_count is in bits 0-13, so fetch_add(1) increments it directly.
        self.0.fetch_add(1, Ordering::AcqRel)
    }

    /// Atomically decrement pin count. Returns previous state.
    pub fn decrement_pin(&self) -> u32 {
        self.0.fetch_sub(1, Ordering::AcqRel)
    }

    // --- Atomic flag operations ---

    pub fn set_flag(&self, flag: u32) {
        self.0.fetch_or(flag, Ordering::Release);
    }

    pub fn clear_flag(&self, flag: u32) {
        self.0.fetch_and(!flag, Ordering::Release);
    }

    pub fn set_valid(&self) { self.set_flag(BM_VALID); }
    pub fn clear_valid(&self) { self.clear_flag(BM_VALID); }
    pub fn set_dirty(&self) { self.set_flag(BM_DIRTY); }
    pub fn clear_dirty(&self) { self.clear_flag(BM_DIRTY); }
    pub fn set_io_in_progress(&self) { self.set_flag(BM_IO_IN_PROGRESS); }
    pub fn clear_io_in_progress(&self) { self.clear_flag(BM_IO_IN_PROGRESS); }
    pub fn set_io_error(&self) { self.set_flag(BM_IO_ERROR); }
    pub fn clear_io_error(&self) { self.clear_flag(BM_IO_ERROR); }

    // --- Combined operations ---

    /// Atomically increment pin count AND bump usage count (if below max) in
    /// a single CAS, matching PostgreSQL's PinBuffer. Returns the old state.
    pub fn pin_and_bump_usage(&self, max_usage: u8) -> u32 {
        loop {
            let old = self.load();
            let mut new = old + 1; // increment pin_count (bits 0-13)
            let usage = ((new & USAGE_COUNT_MASK) >> USAGE_COUNT_SHIFT) as u8;
            if usage < max_usage {
                new += 1 << USAGE_COUNT_SHIFT; // increment usage_count
            }
            if self.0.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return old;
            }
        }
    }

    // --- Usage count (CAS loops) ---

    /// Increment usage count if below max. Returns true if incremented.
    pub fn increment_usage(&self, max: u8) -> bool {
        loop {
            let old = self.load();
            let usage = ((old & USAGE_COUNT_MASK) >> USAGE_COUNT_SHIFT) as u8;
            if usage >= max {
                return false;
            }
            let new = (old & !USAGE_COUNT_MASK) | (((usage + 1) as u32) << USAGE_COUNT_SHIFT);
            if self.0.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return true;
            }
        }
    }

    /// Decrement usage count. Returns true if it was > 0 and decremented.
    pub fn decrement_usage(&self) -> bool {
        loop {
            let old = self.load();
            let usage = ((old & USAGE_COUNT_MASK) >> USAGE_COUNT_SHIFT) as u8;
            if usage == 0 {
                return false;
            }
            let new = (old & !USAGE_COUNT_MASK) | (((usage - 1) as u32) << USAGE_COUNT_SHIFT);
            if self.0.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return true;
            }
        }
    }

    /// Set usage count to a specific value.
    pub fn set_usage_count(&self, count: u8) {
        loop {
            let old = self.load();
            let new = (old & !USAGE_COUNT_MASK) | ((count as u32) << USAGE_COUNT_SHIFT);
            if self.0.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return;
            }
        }
    }

    // --- Bulk operations for eviction/init ---

    /// Reset state for a newly allocated buffer: sets pin_count=1, usage_count=1,
    /// io_in_progress=true, clears everything else.
    pub fn init_for_io(&self) {
        let val = 1 // pin_count = 1
            | (1u32 << USAGE_COUNT_SHIFT) // usage_count = 1
            | BM_IO_IN_PROGRESS;
        self.store(val);
    }

    /// Clear all pin tracking (used during eviction reset).
    pub fn clear_pins(&self) {
        loop {
            let old = self.load();
            let new = old & !PIN_COUNT_MASK;
            if self.0.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return;
            }
        }
    }

    /// Build a BufferStateView from current state and a tag.
    pub fn to_view(&self, tag: Option<BufferTag>) -> BufferStateView {
        let raw = self.load();
        BufferStateView {
            tag,
            valid: raw & BM_VALID != 0,
            dirty: raw & BM_DIRTY != 0,
            io_in_progress: raw & BM_IO_IN_PROGRESS != 0,
            io_error: raw & BM_IO_ERROR != 0,
            pin_count: (raw & PIN_COUNT_MASK) as usize,
            usage_count: ((raw & USAGE_COUNT_MASK) >> USAGE_COUNT_SHIFT) as u8,
        }
    }

    /// Atomically set io_in_progress for flush, only if valid && dirty && !io_in_progress.
    /// Returns Ok(()) if set, Err with the reason otherwise.
    pub fn try_start_flush(&self) -> Result<(), FlushResult> {
        loop {
            let old = self.load();
            if old & BM_IO_IN_PROGRESS != 0 { return Err(FlushResult::InProgress); }
            if old & BM_VALID == 0 { return Err(FlushResult::Invalid); }
            if old & BM_DIRTY == 0 { return Err(FlushResult::AlreadyClean); }
            let new = (old | BM_IO_IN_PROGRESS) & !BM_IO_ERROR;
            if self.0.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return Ok(());
            }
        }
    }
}

impl std::fmt::Debug for BufferState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let raw = self.load();
        f.debug_struct("BufferState")
            .field("pin_count", &(raw & PIN_COUNT_MASK))
            .field("usage_count", &((raw & USAGE_COUNT_MASK) >> USAGE_COUNT_SHIFT))
            .field("valid", &(raw & BM_VALID != 0))
            .field("dirty", &(raw & BM_DIRTY != 0))
            .field("io_in_progress", &(raw & BM_IO_IN_PROGRESS != 0))
            .field("io_error", &(raw & BM_IO_ERROR != 0))
            .finish()
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;

    #[test]
    fn pin_count_increment_decrement() {
        let s = BufferState::new();
        assert_eq!(s.pin_count(), 0);
        s.increment_pin();
        assert_eq!(s.pin_count(), 1);
        s.increment_pin();
        assert_eq!(s.pin_count(), 2);
        s.decrement_pin();
        assert_eq!(s.pin_count(), 1);
        s.decrement_pin();
        assert_eq!(s.pin_count(), 0);
    }

    #[test]
    fn pin_count_does_not_affect_other_fields() {
        let s = BufferState::new();
        s.set_valid();
        s.set_dirty();
        s.increment_pin();
        s.increment_pin();
        assert!(s.is_valid());
        assert!(s.is_dirty());
        assert_eq!(s.pin_count(), 2);
        s.decrement_pin();
        assert!(s.is_valid());
        assert!(s.is_dirty());
        assert_eq!(s.pin_count(), 1);
    }

    #[test]
    fn usage_count_increment_decrement() {
        let s = BufferState::new();
        assert_eq!(s.usage_count(), 0);
        assert!(s.increment_usage(5));
        assert_eq!(s.usage_count(), 1);
        assert!(s.increment_usage(5));
        assert_eq!(s.usage_count(), 2);
        assert!(s.decrement_usage());
        assert_eq!(s.usage_count(), 1);
        assert!(s.decrement_usage());
        assert_eq!(s.usage_count(), 0);
        assert!(!s.decrement_usage()); // already 0
    }

    #[test]
    fn usage_count_respects_max() {
        let s = BufferState::new();
        assert!(s.increment_usage(2));
        assert!(s.increment_usage(2));
        assert!(!s.increment_usage(2)); // at max
        assert_eq!(s.usage_count(), 2);
    }

    #[test]
    fn usage_count_does_not_affect_pin_count() {
        let s = BufferState::new();
        s.increment_pin();
        s.increment_pin();
        s.increment_pin();
        s.increment_usage(5);
        s.increment_usage(5);
        assert_eq!(s.pin_count(), 3);
        assert_eq!(s.usage_count(), 2);
    }

    #[test]
    fn flags_independent() {
        let s = BufferState::new();
        assert!(!s.is_valid());
        assert!(!s.is_dirty());
        assert!(!s.is_io_in_progress());
        assert!(!s.is_io_error());

        s.set_valid();
        assert!(s.is_valid());
        assert!(!s.is_dirty());

        s.set_dirty();
        assert!(s.is_valid());
        assert!(s.is_dirty());

        s.clear_valid();
        assert!(!s.is_valid());
        assert!(s.is_dirty());

        s.set_io_in_progress();
        s.set_io_error();
        assert!(s.is_io_in_progress());
        assert!(s.is_io_error());
        assert!(s.is_dirty());
    }

    #[test]
    fn init_for_io_sets_correct_state() {
        let s = BufferState::new();
        s.set_valid();
        s.set_dirty();
        s.increment_pin();
        s.increment_pin();

        s.init_for_io();
        assert_eq!(s.pin_count(), 1);
        assert_eq!(s.usage_count(), 1);
        assert!(s.is_io_in_progress());
        assert!(!s.is_valid());
        assert!(!s.is_dirty());
        assert!(!s.is_io_error());
    }

    #[test]
    fn try_start_flush_succeeds_when_valid_dirty() {
        let s = BufferState::new();
        s.set_valid();
        s.set_dirty();
        assert!(s.try_start_flush().is_ok());
        assert!(s.is_io_in_progress());
    }

    #[test]
    fn try_start_flush_fails_when_not_dirty() {
        let s = BufferState::new();
        s.set_valid();
        assert_eq!(s.try_start_flush(), Err(FlushResult::AlreadyClean));
    }

    #[test]
    fn try_start_flush_fails_when_io_in_progress() {
        let s = BufferState::new();
        s.set_valid();
        s.set_dirty();
        s.set_io_in_progress();
        assert_eq!(s.try_start_flush(), Err(FlushResult::InProgress));
    }

    #[test]
    fn to_view_roundtrips() {
        let s = BufferState::new();
        s.increment_pin();
        s.increment_pin();
        s.increment_usage(5);
        s.set_valid();
        s.set_dirty();

        let view = s.to_view(None);
        assert_eq!(view.pin_count, 2);
        assert_eq!(view.usage_count, 1);
        assert!(view.valid);
        assert!(view.dirty);
        assert!(!view.io_in_progress);
        assert!(!view.io_error);
        assert_eq!(view.tag, None);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnknownBuffer,
    WrongIoOp,
    NoIoInProgress,
    BufferPinned,
    InvalidBuffer,
    NotDirty,
    Storage(String),
    Wal(String),
}
