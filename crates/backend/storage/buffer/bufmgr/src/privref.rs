use core::cell::UnsafeCell;
use core::mem::ManuallyDrop;

use mcx::{MemoryContext, PgHashMap};
use types_core::{Buffer, InvalidBuffer};

pub const REFCOUNT_ARRAY_ENTRIES: usize = 8;

#[derive(Clone, Copy)]
pub struct PrivateRefCountEntry {
    pub buffer: Buffer,
    pub refcount: i32,
}

struct PrivRef {
    entries: [PrivateRefCountEntry; REFCOUNT_ARRAY_ENTRIES],
    reserved: i32,
    clock: u32,
    overflowed: u32,
    overflow: Option<ManuallyDrop<PgHashMap<'static, Buffer, i32>>>,
}

const EMPTY: PrivateRefCountEntry = PrivateRefCountEntry {
    buffer: InvalidBuffer,
    refcount: 0,
};

thread_local! {
    static PRIV: UnsafeCell<PrivRef> = const {
        UnsafeCell::new(PrivRef {
            entries: [EMPTY; REFCOUNT_ARRAY_ENTRIES],
            reserved: -1,
            clock: 0,
            overflowed: 0,
            overflow: None,
        })
    };
}

#[inline(always)]
fn with<R>(f: impl FnOnce(&mut PrivRef) -> R) -> R {
    PRIV.with(|p| {
        // SAFETY: one backend = one thread and no callee below re-enters this
        // module, so the &mut is unique for the closure's extent.
        f(unsafe { &mut *p.get() })
    })
}

#[cold]
fn make_overflow_map() -> ManuallyDrop<PgHashMap<'static, Buffer, i32>> {
    let cx: &'static MemoryContext = ::mcx::session_root("PrivateRefCount");
    ManuallyDrop::new(PgHashMap::with_hasher_in(Default::default(), cx.mcx()))
}

fn reserve_slot(p: &mut PrivRef) {
    if p.reserved >= 0 {
        return;
    }
    for (i, e) in p.entries.iter().enumerate() {
        if e.buffer == InvalidBuffer {
            p.reserved = i as i32;
            return;
        }
    }
    let victim = (p.clock as usize) % REFCOUNT_ARRAY_ENTRIES;
    p.clock = p.clock.wrapping_add(1);
    let evicted = p.entries[victim];
    debug_assert!(evicted.buffer != InvalidBuffer);
    let map = p.overflow.get_or_insert_with(make_overflow_map);
    let prev = map.insert(evicted.buffer, evicted.refcount);
    debug_assert!(prev.is_none());
    p.entries[victim] = EMPTY;
    p.overflowed += 1;
    p.reserved = victim as i32;
}

/// ReservePrivateRefCountEntry (bufmgr.c): guarantee one free array slot so
/// the entry fill after a spinlock/CAS section never allocates.
#[inline(always)]
pub fn ReservePrivateRefCountEntry() {
    with(reserve_slot);
}

/// `NewPrivateRefCountEntry` + `refcount++` (bufmgr.c), WITHOUT the lookup:
/// fill the reserved array slot with a **fresh** entry at refcount 1 even if
/// this buffer already has one.
///
/// C does not search here either, and that is load-bearing rather than an
/// optimisation. `PinBuffer_Locked` adds a shared refcount **unconditionally**
/// — it fuses the bump into the header unlock, so it cannot first consult a
/// private entry the way `PinBuffer` does — therefore its private entry must
/// be a *second, independently droppable* one. Merging into an existing entry
/// instead pairs two shared bumps with a single shared drop, because the
/// shared refcount is released only on the transition to zero: the buffer then
/// keeps a shared pin forever, never becomes replaceable again, and
/// `InvalidateBuffer` spins on it without bound.
///
/// `GetPrivateRefCount` reports the first matching entry, so it reads 1 rather
/// than 2 while both are live — which is also what C reports, for the same
/// reason.
#[inline(always)]
pub(crate) fn new_pin_entry(buffer: Buffer) {
    debug_assert!(buffer != InvalidBuffer);
    with(|p| {
        let slot = p.reserved;
        // Hard assert, as in track_pin: C's counterpart is
        // `Assert(ReservedRefCountEntry != NULL)`, but a missing reservation
        // here would silently overwrite entry 0 and drop a live pin.
        assert!(slot >= 0, "no reserved private refcount entry");
        p.reserved = -1;
        p.entries[slot as usize] = PrivateRefCountEntry {
            buffer,
            refcount: 1,
        };
    })
}

/// GetPrivateRefCountEntry(do_move=true) + NewPrivateRefCountEntry + refcount++
/// fused to one TLS access; returns the pre-increment refcount.
#[inline(always)]
pub(crate) fn track_pin(buffer: Buffer) -> i32 {
    debug_assert!(buffer != InvalidBuffer);
    with(|p| {
        for e in p.entries.iter_mut() {
            if e.buffer == buffer {
                let old = e.refcount;
                e.refcount += 1;
                return old;
            }
        }
        if p.overflowed > 0 {
            let in_map = p.overflow.as_mut().and_then(|m| m.remove(&buffer));
            if let Some(rc) = in_map {
                // do_move: promote to the array (C reserves inside the move).
                p.overflowed -= 1;
                reserve_slot(p);
                let slot = p.reserved;
                p.reserved = -1;
                p.entries[slot as usize] = PrivateRefCountEntry {
                    buffer,
                    refcount: rc + 1,
                };
                return rc;
            }
        }
        let slot = p.reserved;
        assert!(slot >= 0, "no reserved private refcount entry");
        p.reserved = -1;
        p.entries[slot as usize] = PrivateRefCountEntry {
            buffer,
            refcount: 1,
        };
        0
    })
}

/// refcount--; at zero, ForgetPrivateRefCountEntry (the array slot becomes the
/// reserved entry so pin→unpin→pin never searches); true = drop the shared ref.
#[inline(always)]
pub(crate) fn track_unpin(buffer: Buffer) -> bool {
    with(|p| {
        for (i, e) in p.entries.iter_mut().enumerate() {
            if e.buffer == buffer {
                debug_assert!(e.refcount > 0);
                e.refcount -= 1;
                if e.refcount == 0 {
                    *e = EMPTY;
                    if p.reserved < 0 {
                        p.reserved = i as i32;
                    }
                    return true;
                }
                return false;
            }
        }
        let map = p.overflow.as_mut().expect("buffer is not pinned");
        let rc = map.get_mut(&buffer).expect("buffer is not pinned");
        debug_assert!(*rc > 0);
        *rc -= 1;
        if *rc == 0 {
            map.remove(&buffer);
            p.overflowed -= 1;
            true
        } else {
            false
        }
    })
}

#[inline]
pub fn GetPrivateRefCount(buffer: Buffer) -> i32 {
    debug_assert!(buffer != InvalidBuffer);
    with(|p| {
        for e in p.entries.iter() {
            if e.buffer == buffer {
                return e.refcount;
            }
        }
        if p.overflowed == 0 {
            return 0;
        }
        p.overflow
            .as_ref()
            .and_then(|m| m.get(&buffer).copied())
            .unwrap_or(0)
    })
}

pub(crate) fn track_incr(buffer: Buffer) {
    with(|p| {
        for e in p.entries.iter_mut() {
            if e.buffer == buffer {
                e.refcount += 1;
                return;
            }
        }
        if p.overflowed > 0 {
            let in_map = p.overflow.as_mut().and_then(|m| m.remove(&buffer));
            if let Some(rc) = in_map {
                p.overflowed -= 1;
                reserve_slot(p);
                let slot = p.reserved;
                p.reserved = -1;
                p.entries[slot as usize] = PrivateRefCountEntry {
                    buffer,
                    refcount: rc + 1,
                };
                return;
            }
        }
        panic!("IncrBufferRefCount: buffer {buffer} is not pinned");
    });
}

pub(crate) fn for_each_held(mut f: impl FnMut(Buffer, i32)) {
    with(|p| {
        for e in p.entries.iter() {
            if e.buffer != InvalidBuffer {
                f(e.buffer, e.refcount);
            }
        }
        if let Some(map) = p.overflow.as_ref() {
            for (b, rc) in map.iter() {
                f(*b, *rc);
            }
        }
    })
}

pub(crate) fn overflow_count() -> u32 {
    with(|p| p.overflowed)
}

pub(crate) fn overflowed_count() -> u32 {
    with(|p| p.overflowed)
}

// Diagnostic for pinned-where-C-expects-none panics: every live private pin.
pub fn debug_all_private_pins() -> Vec<(Buffer, i32)> {
    with(|p| {
        let mut out: Vec<(Buffer, i32)> = p
            .entries
            .iter()
            .filter(|e| e.buffer != InvalidBuffer && e.refcount != 0)
            .map(|e| (e.buffer, e.refcount))
            .collect();
        if let Some(m) = p.overflow.as_ref() {
            if p.overflowed > 0 {
                out.extend(m.iter().filter(|(_, &rc)| rc != 0).map(|(&b, &rc)| (b, rc)));
            }
        }
        out
    })
}
