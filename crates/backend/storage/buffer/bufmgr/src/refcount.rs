//! Per-backend private pin tracking — `PrivateRefCountEntry` and the
//! `ReservePrivateRefCountEntry` / `NewPrivateRefCountEntry` /
//! `GetPrivateRefCountEntry` / `ForgetPrivateRefCountEntry` lifecycle from
//! bufmgr.c.
//!
//! BACKEND-LOCAL: a process-local map, never placed in shmem and never touched
//! by another backend. The shared refcount in `BufferDesc.state` is only moved
//! when an entry here crosses 0<->1.
//!
//! The C implementation keeps entries in a fixed-size array
//! (`PrivateRefCountArray`, `REFCOUNT_ARRAY_ENTRIES` slots) with an overflow
//! hash table (`PrivateRefCountHash`); the array-vs-hash split is purely an
//! allocation optimisation (avoid `hash_search` while a spinlock is held — see
//! bufmgr.c:195-213) with NO externally observable behaviour. This collapses
//! both onto a single backend-local map: a `buffer` key with its `refcount`
//! value is the entry.

use core::cell::RefCell;
use std::collections::HashMap;

use types_core::primitive::Buffer;

/// `PrivateRefCountEntry` (bufmgr.c) — the per-backend pin record for one
/// buffer. Crate-local: this repo has no shared `PrivateRefCountEntry` type, and
/// the entry is strictly bufmgr-internal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateRefCountEntry {
    /// `Buffer buffer` — the 1-based shared-buffer handle.
    pub buffer: Buffer,
    /// `int32 refcount` — this backend's local pin count for the buffer.
    pub refcount: u32,
}

/// The backend-local private pin map (keyed by 0-based buf_id).
#[derive(Default)]
pub struct PrivateRefCount {
    counts: RefCell<HashMap<i32, u32>>,
}

// The lifecycle methods are the F1b pin/unpin API; they compile now but are
// first consumed once `PinBuffer`/`UnpinBuffer` land.
#[allow(dead_code)]
impl PrivateRefCount {
    /// `GetPrivateRefCount(buffer)` — this backend's (local) pin count.
    pub fn get(&self, buf_id: i32) -> u32 {
        self.counts.borrow().get(&buf_id).copied().unwrap_or(0)
    }

    /// Increment and return the new count.
    pub fn incr(&self, buf_id: i32) -> u32 {
        let mut map = self.counts.borrow_mut();
        let entry = map.entry(buf_id).or_insert(0);
        *entry += 1;
        *entry
    }

    /// Decrement and return the new count, removing the slot when it reaches 0.
    pub fn decr(&self, buf_id: i32) -> u32 {
        let mut map = self.counts.borrow_mut();
        let entry = map.entry(buf_id).or_insert(0);
        debug_assert!(*entry > 0, "UnpinBuffer of an unpinned buffer");
        *entry -= 1;
        let new = *entry;
        if new == 0 {
            map.remove(&buf_id);
        }
        new
    }

    /// `memset(&PrivateRefCountArray, 0, ...)` — empty the table
    /// (`InitBufferManagerAccess`).
    pub fn clear(&self) {
        self.counts.borrow_mut().clear();
    }

    /// Iterate every present `(buf_id, refcount)` entry — the
    /// `CheckForBufferLeaks` scan.
    pub fn for_each_present(&self, mut f: impl FnMut(i32, u32)) {
        for (&buf_id, &count) in self.counts.borrow().iter() {
            f(buf_id, count);
        }
    }

    /// `ReservePrivateRefCountEntry` (bufmgr.c) — ensure room for one more
    /// entry. The map-backed substrate never overflows, so this is a no-op
    /// (faithful: the C array/hash spill is invisible to every caller).
    pub fn ReservePrivateRefCountEntry(&self) {}

    /// `NewPrivateRefCountEntry(buffer)` — fill a reserved entry for `buffer`
    /// with `refcount = 0`. The buffer must not already have an entry.
    pub fn NewPrivateRefCountEntry(&self, buffer: Buffer) -> PrivateRefCountEntry {
        let buf_id = buffer - 1;
        let mut map = self.counts.borrow_mut();
        debug_assert!(
            !map.contains_key(&buf_id),
            "NewPrivateRefCountEntry: buffer already has a refcount entry"
        );
        map.insert(buf_id, 0);
        PrivateRefCountEntry {
            buffer,
            refcount: 0,
        }
    }

    /// `GetPrivateRefCountEntry(buffer, do_move)` — the entry for `buffer`, or
    /// `None`. The C `do_move` array-promotion is a no-op here (no array/hash
    /// distinction).
    pub fn GetPrivateRefCountEntry(
        &self,
        buffer: Buffer,
        _do_move: bool,
    ) -> Option<PrivateRefCountEntry> {
        debug_assert!(buffer > 0, "GetPrivateRefCountEntry: BufferIsValid");
        let buf_id = buffer - 1;
        self.counts
            .borrow()
            .get(&buf_id)
            .copied()
            .map(|refcount| PrivateRefCountEntry { buffer, refcount })
    }

    /// `ForgetPrivateRefCountEntry(ref)` — release tracking for a buffer this
    /// backend no longer pins (`refcount == 0`).
    pub fn ForgetPrivateRefCountEntry(&self, entry: PrivateRefCountEntry) {
        debug_assert_eq!(
            entry.refcount, 0,
            "ForgetPrivateRefCountEntry: refcount must be 0"
        );
        let buf_id = entry.buffer - 1;
        self.counts.borrow_mut().remove(&buf_id);
    }

    /// Number of distinct buffers this backend currently tracks (test helper).
    pub fn len(&self) -> usize {
        self.counts.borrow().len()
    }

    /// Whether this backend tracks no pins (test helper).
    pub fn is_empty(&self) -> bool {
        self.counts.borrow().is_empty()
    }
}
