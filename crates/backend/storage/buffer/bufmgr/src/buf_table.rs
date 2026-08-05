use lwlock::{main_lock, LWLock, BUFFER_MAPPING_LWLOCK_OFFSET, NUM_BUFFER_PARTITIONS};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::buf::buftag;

// Dense open-addressed replacement for C's dynahash buffer table (van de Meent
// RFC, upstream-unshipped-ideas #2): 24-byte contiguous entries, linear probe,
// ~1 pointer chase per warm lookup vs dynahash's 4. Stalled upstream on C's
// non-resizable shared memory; ours is process-local and grows per partition
// under the exclusive partition lock. External behavior (tag equality, insert
// collision return, delete error surface, partition-lock contract) is C-exact.
#[repr(C)]
struct BufferLookupEnt {
    key: buftag,
    id: i32,
}

const _: () = assert!(core::mem::size_of::<BufferLookupEnt>() == 24);

// key.blockNum == InvalidBlockNumber marks an empty slot; BufTableInsert's
// blockNum assert is load-bearing for this sentinel (C asserts it too).
const EMPTY_BLOCK: u32 = types_core::InvalidBlockNumber;

struct Partition {
    entries: *mut BufferLookupEnt,
    mask: u32,
    count: u32,
    grow_at: u32,
}

// SAFETY(Sync): partition contents are read under the shared partition LWLock
// and mutated (including grow/realloc) only under the exclusive partition
// LWLock — the same serialization contract the dynahash table relied on.
// Published once at startup, then plain loads (C global).
static PARTITIONS: core::sync::atomic::AtomicPtr<Partition> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

#[inline]
fn partitions() -> *mut Partition {
    let p = PARTITIONS.load(core::sync::atomic::Ordering::Relaxed);
    if p.is_null() {
        table_uninit();
    }
    p
}

#[cold]
#[inline(never)]
fn table_uninit() -> ! {
    panic!("bufmgr: InitBufTable (buf_table.c) not called")
}

#[cold]
#[inline(never)]
fn table_oom(nbytes: usize) -> ! {
    panic!("bufmgr: buffer lookup table allocation failed ({nbytes} bytes)")
}

fn alloc_entries(cap: usize) -> *mut BufferLookupEnt {
    let layout = core::alloc::Layout::array::<BufferLookupEnt>(cap)
        .unwrap()
        .align_to(64)
        .unwrap();
    // SAFETY: layout is non-zero; entries are plain-old-data.
    let p = unsafe { std::alloc::alloc(layout) } as *mut BufferLookupEnt;
    if p.is_null() {
        table_oom(layout.size());
    }
    for i in 0..cap {
        // SAFETY: i < cap, freshly allocated.
        unsafe { (*p.add(i)).key.blockNum = EMPTY_BLOCK };
    }
    p
}

fn free_entries(p: *mut BufferLookupEnt, cap: usize) {
    let layout = core::alloc::Layout::array::<BufferLookupEnt>(cap)
        .unwrap()
        .align_to(64)
        .unwrap();
    // SAFETY: p came from alloc_entries(cap) with this exact layout.
    unsafe { std::alloc::dealloc(p as *mut u8, layout) };
}

/// InitBufTable (buf_table.c): size = NBuffers + NUM_BUFFER_PARTITIONS.
pub fn InitBufTable(size: i32) -> PgResult<()> {
    let base = main_lock(BUFFER_MAPPING_LWLOCK_OFFSET as usize) as *const LWLock
        as *mut lwlock::LWLockPadded;
    PARTITION_BASE.store(base, core::sync::atomic::Ordering::Release);

    let per_part = (size as usize / NUM_BUFFER_PARTITIONS as usize) + 1;
    let cap = (per_part * 2).next_power_of_two().max(16);
    let nparts = NUM_BUFFER_PARTITIONS as usize;
    let layout = core::alloc::Layout::array::<Partition>(nparts).unwrap();
    // SAFETY: non-zero layout; fields initialized below before publication.
    let parts = unsafe { std::alloc::alloc(layout) } as *mut Partition;
    if parts.is_null() {
        table_oom(layout.size());
    }
    for i in 0..nparts {
        // SAFETY: i < nparts, freshly allocated.
        unsafe {
            core::ptr::write(
                parts.add(i),
                Partition {
                    entries: alloc_entries(cap),
                    mask: (cap - 1) as u32,
                    count: 0,
                    grow_at: (cap - cap / 4) as u32,
                },
            );
        }
    }
    assert!(
        PARTITIONS
            .compare_exchange(
                core::ptr::null_mut(),
                parts,
                core::sync::atomic::Ordering::Release,
                core::sync::atomic::Ordering::Relaxed
            )
            .is_ok(),
        "bufmgr: buffer lookup table initialized twice"
    );
    Ok(())
}

// FxHash-family mix over the 20 tag bytes (rule 9: FxHash for internal
// tables); replaces dynahash's hash_bytes. The value is internal-only — it
// feeds partition selection and slot placement, nothing persists it.
const FX_K: u64 = 0x517c_c1b7_2722_0a95;

#[inline]
pub fn BufTableHashCode(tag: &buftag) -> u32 {
    // SAFETY: buftag is repr(C), 20 bytes, no padding (size-asserted in
    // types_storage); reads are within the object.
    let (a, b, c) = unsafe {
        let p = tag as *const buftag as *const u8;
        (
            core::ptr::read_unaligned(p as *const u64),
            core::ptr::read_unaligned(p.add(8) as *const u64),
            core::ptr::read_unaligned(p.add(16) as *const u32) as u64,
        )
    };
    let mut h = a.wrapping_mul(FX_K);
    h = (h.rotate_left(5) ^ b).wrapping_mul(FX_K);
    h = (h.rotate_left(5) ^ c).wrapping_mul(FX_K);
    (h ^ (h >> 32)) as u32
}

// C indexes the bare MainLWLockArray global; cache the partition slice base at
// init so the hit path is load+index, not an OnceLock re-check per lookup.
static PARTITION_BASE: core::sync::atomic::AtomicPtr<lwlock::LWLockPadded> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

#[inline]
pub fn BufMappingPartitionLock(hashcode: u32) -> &'static LWLock {
    let base = PARTITION_BASE.load(core::sync::atomic::Ordering::Relaxed);
    if base.is_null() {
        return main_lock(
            (BUFFER_MAPPING_LWLOCK_OFFSET as u32 + hashcode % NUM_BUFFER_PARTITIONS as u32)
                as usize,
        );
    }
    // SAFETY: base points at MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET..],
    // NUM_BUFFER_PARTITIONS entries, process lifetime; index is in range.
    unsafe { &(*base.add((hashcode % NUM_BUFFER_PARTITIONS as u32) as usize)).lock }
}

#[inline]
fn partition_for(hashcode: u32) -> *mut Partition {
    // Same partition selection as the lock above — the lock covers exactly
    // this partition's slots.
    // SAFETY: index < NUM_BUFFER_PARTITIONS, table published at init.
    unsafe { partitions().add((hashcode % NUM_BUFFER_PARTITIONS as u32) as usize) }
}

// Slot placement uses the hash bits above the 7 partition bits so a
// partition's entries don't collide on their shared low bits.
#[inline]
fn ideal_slot(hashcode: u32, mask: u32) -> u32 {
    (hashcode >> 7) & mask
}

/// Crash-cycle reset. Postmaster-only choreography (children dead), so no
/// locks are taken — and unlike dynahash there are no freelist spinlocks a
/// crashed backend could have died holding.
pub(crate) fn BufTableResetAfterCrash() {
    let parts = partitions();
    for i in 0..NUM_BUFFER_PARTITIONS as usize {
        // SAFETY: single-threaded crash choreography; i in range.
        unsafe {
            let part = &mut *parts.add(i);
            for s in 0..=part.mask as usize {
                (*part.entries.add(s)).key.blockNum = EMPTY_BLOCK;
            }
            part.count = 0;
        }
    }
}

/// Caller holds the partition lock (shared or better).
pub fn BufTableLookup(tag: &buftag, hashcode: u32) -> PgResult<i32> {
    // SAFETY: shared partition lock excludes writers to this partition.
    unsafe {
        let part = &*partition_for(hashcode);
        let mask = part.mask;
        let mut slot = ideal_slot(hashcode, mask);
        loop {
            let ent = &*part.entries.add(slot as usize);
            if ent.key.blockNum == EMPTY_BLOCK {
                return Ok(-1);
            }
            if ent.key == *tag {
                return Ok(ent.id);
            }
            slot = (slot + 1) & mask;
        }
    }
}

/// -1 on success, existing id on collision; partition lock held exclusively.
pub fn BufTableInsert(tag: &buftag, hashcode: u32, buf_id: i32) -> PgResult<i32> {
    debug_assert!(buf_id >= 0);
    debug_assert!(tag.blockNum != types_core::InvalidBlockNumber);
    // SAFETY: exclusive partition lock — sole accessor of this partition.
    unsafe {
        let part = &mut *partition_for(hashcode);
        if part.count + 1 > part.grow_at {
            grow(part);
        }
        let mask = part.mask;
        let mut slot = ideal_slot(hashcode, mask);
        loop {
            let ent = &mut *part.entries.add(slot as usize);
            if ent.key.blockNum == EMPTY_BLOCK {
                ent.key = *tag;
                ent.id = buf_id;
                part.count += 1;
                return Ok(-1);
            }
            if ent.key == *tag {
                return Ok(ent.id);
            }
            slot = (slot + 1) & mask;
        }
    }
}

#[cold]
#[inline(never)]
unsafe fn grow(part: &mut Partition) {
    let old_cap = part.mask as usize + 1;
    let new_cap = old_cap * 2;
    let new_entries = alloc_entries(new_cap);
    let new_mask = (new_cap - 1) as u32;
    for s in 0..old_cap {
        let ent = &*part.entries.add(s);
        if ent.key.blockNum == EMPTY_BLOCK {
            continue;
        }
        let mut slot = ideal_slot(BufTableHashCode(&ent.key), new_mask);
        loop {
            let dst = &mut *new_entries.add(slot as usize);
            if dst.key.blockNum == EMPTY_BLOCK {
                dst.key = ent.key;
                dst.id = ent.id;
                break;
            }
            slot = (slot + 1) & new_mask;
        }
    }
    free_entries(part.entries, old_cap);
    part.entries = new_entries;
    part.mask = new_mask;
    part.grow_at = (new_cap - new_cap / 4) as u32;
}

/// Caller holds the partition lock exclusively.
pub fn BufTableDelete(tag: &buftag, hashcode: u32) -> PgResult<()> {
    // SAFETY: exclusive partition lock — sole accessor of this partition.
    unsafe {
        let part = &mut *partition_for(hashcode);
        let mask = part.mask;
        let mut slot = ideal_slot(hashcode, mask);
        loop {
            let ent = &*part.entries.add(slot as usize);
            if ent.key.blockNum == EMPTY_BLOCK {
                return Err(Box::new(
                    types_error::PgError::new(ERROR, "shared buffer hash table corrupted")
                        .with_error_location(ErrorLocation::new(file!(), line!() as i32, "BufTableDelete")),
                ));
            }
            if ent.key == *tag {
                break;
            }
            slot = (slot + 1) & mask;
        }
        // Backward-shift deletion (linear probing has no tombstones): pull
        // forward any entry whose ideal slot lies cyclically at-or-before the
        // hole, preserving every probe chain.
        let mut hole = slot;
        let mut j = slot;
        loop {
            j = (j + 1) & mask;
            let ent = &*part.entries.add(j as usize);
            if ent.key.blockNum == EMPTY_BLOCK {
                break;
            }
            let ideal = ideal_slot(BufTableHashCode(&ent.key), mask);
            if (j.wrapping_sub(ideal) & mask) >= (j.wrapping_sub(hole) & mask) {
                *part.entries.add(hole as usize) = core::ptr::read(part.entries.add(j as usize));
                hole = j;
            }
        }
        (*part.entries.add(hole as usize)).key.blockNum = EMPTY_BLOCK;
        part.count -= 1;
        Ok(())
    }
}
