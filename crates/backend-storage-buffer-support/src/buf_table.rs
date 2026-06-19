//! `buf_table.c` — the shared buffer lookup hash table.
//!
//! In PostgreSQL `SharedBufHash` is a partitioned dynahash in the shared-memory
//! segment, protected by the `BufferMappingLock` partition LWLocks. It is
//! modeled here as a fixed-capacity open-addressing table of [`Slot`]s
//! (capacity = `2 * (NBuffers + NUM_BUFFER_PARTITIONS)`, headroom over the
//! dynahash `max_size` so probe chains stay short). The hashing
//! (`BufTableHashCode`) and the lookup/insert/delete semantics are the verbatim
//! algorithm.
//!
//! The slot array AND the live-entry counter live in the `MAP_SHARED`
//! shared-memory segment, carved through the `ShmemInitStruct` seam: the
//! returned base address is the same in every forked backend, so the
//! `BufferTag -> buf_id` mapping one backend installs is visible to all others
//! — the real `SharedBufHash` posture. Without this, a backend that caches a
//! catalog page would record the mapping only in its own process heap and the
//! page would be invisible to every other connection.
//!
//! The routines do NO locking of their own: the caller must hold the
//! appropriate `BufferMappingLock` partition lock, exactly as `buf_table.c`
//! requires.

use types_core::Size;
use types_error::{PgError, PgResult};
use types_storage::buf::buftag;
use types_storage::NUM_BUFFER_PARTITIONS;

/// `get_hash_value(SharedBufHash, tagPtr)` (BufTableHashCode) — the hash code of
/// a buffer tag. The five fields are serialized in declaration order (matching
/// the in-memory layout of `BufferTag`) and hashed with `hash_bytes`; equal tags
/// hash equally.
pub fn buf_table_hash_code(tag: &buftag) -> u32 {
    let mut bytes = [0u8; 20];
    bytes[0..4].copy_from_slice(&tag.spcOid.to_ne_bytes());
    bytes[4..8].copy_from_slice(&tag.dbOid.to_ne_bytes());
    bytes[8..12].copy_from_slice(&tag.relNumber.to_ne_bytes());
    bytes[12..16].copy_from_slice(&(tag.forkNum as i32).to_ne_bytes());
    bytes[16..20].copy_from_slice(&tag.blockNum.to_ne_bytes());
    common_hashfn::hash_bytes(&bytes)
}

/// `BufTableHashPartition(hashcode)` (buf_internals.h) — the partition (and thus
/// the `BufferMappingLock` index) a hash code maps to.
pub fn buf_table_hash_partition(hashcode: u32) -> u32 {
    hashcode % (NUM_BUFFER_PARTITIONS as u32)
}

/// `EMPTY_SLOT` — sentinel `id` marking an unoccupied slot. Valid buffer ids
/// are `>= 0`; `-1` is reserved by `BufTableInsert`/`Lookup` for "not in table",
/// so `-2` is free as the empty marker.
const EMPTY_SLOT: i32 = -2;

/// One slot of the lookup table — shmem-resident, `repr(C)`. Mirrors dynahash's
/// `BufferLookupEnt { BufferTag key; int id; }`, with `id == EMPTY_SLOT`
/// denoting an empty slot (so the array can be zero-checked / scanned without an
/// out-of-band `Option` discriminant that would not be coherent cross-process).
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct Slot {
    key: buftag,
    id: i32,
}

impl Slot {
    fn empty() -> Self {
        Self {
            key: buftag::default(),
            id: EMPTY_SLOT,
        }
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.id == EMPTY_SLOT
    }
}

/// The shmem-resident header for the lookup table — the live-entry count,
/// reached under the partition `BufferMappingLock`s exactly as the slot array
/// is. (`repr(C)`; placed immediately before the slot array in C's dynahash,
/// here in its own field of the carved region.)
#[repr(C)]
struct BufTableHeader {
    nentries: usize,
}

/// `SharedBufHash` — the buffer-mapping lookup hash, modeled as an
/// open-addressing table whose slot array AND live-entry counter live in the
/// `MAP_SHARED` segment (carved through the `ShmemInitStruct` seam). The struct
/// itself is a process-local *view*: it holds raw base pointers into the shared
/// region, identical in every forked backend, so the mapping is shared.
pub struct BufTable {
    /// Base pointer to the shmem-resident `BufTableHeader` (the entry counter).
    header: *mut BufTableHeader,
    /// Base pointer to the shmem-resident slot array (`capacity` `Slot`s).
    slots: *mut Slot,
    capacity: usize,
}

// SAFETY: the slot array + header live in the shared segment for the server's
// life; cross-backend access is serialized by the caller-held BufferMappingLock
// partition locks (the buf_table.c contract). The view is published `'static`.
unsafe impl Send for BufTable {}
unsafe impl Sync for BufTable {}

impl BufTable {
    /// `InitBufTable(size)` — place the lookup table in shared memory. `size` is
    /// the dynahash `max_size` (`NBuffers + NUM_BUFFER_PARTITIONS`). Honors the
    /// `found` flag like C: on first creation zero the slot array + counter; on
    /// attach reuse the already-initialized shared bytes.
    pub fn InitBufTable(size: i32) -> PgResult<Self> {
        let capacity = Self::table_capacity(size);
        let header_bytes = core::mem::size_of::<BufTableHeader>();
        let slots_bytes = capacity
            .checked_mul(core::mem::size_of::<Slot>())
            .ok_or_else(|| PgError::error("buffer lookup table size overflow"))?;
        let bytes = header_bytes + slots_bytes;
        // ShmemInitStruct("Shared Buffer Lookup Table", bytes, &found).
        let (addr, found) = backend_storage_ipc_shmem_seams::shmem_init_struct::call(
            "Shared Buffer Lookup Table",
            bytes,
        )?;

        let (header, slots) = if !addr.is_null() {
            // header at the region start, the slot array right after it
            // (MAXALIGNed: BufTableHeader is a single `usize`, so a `Slot` array
            // immediately following is already aligned).
            let header = addr.cast::<BufTableHeader>();
            // SAFETY: the region holds header_bytes + slots_bytes; advancing by
            // size_of::<BufTableHeader>() lands at the slot array, which is
            // aligned for `Slot` (its alignment is 4, <= header's 8).
            let slots = unsafe { addr.add(header_bytes) }.cast::<Slot>();
            (header, slots)
        } else {
            // No real segment (test / standalone): leak a zeroed heap region,
            // matching the bufmgr fallback. The `!found` init path below fills
            // it in place.
            let h_layout = core::alloc::Layout::new::<BufTableHeader>();
            // SAFETY: non-zero layout.
            let header = unsafe { std::alloc::alloc_zeroed(h_layout) }.cast::<BufTableHeader>();
            let s_layout = core::alloc::Layout::array::<Slot>(capacity.max(1))
                .map_err(|_| PgError::error("buffer lookup table layout"))?;
            // SAFETY: non-zero layout.
            let slots = unsafe { std::alloc::alloc_zeroed(s_layout) }.cast::<Slot>();
            assert!(
                !header.is_null() && !slots.is_null(),
                "out of memory (buffer lookup table fallback)"
            );
            (header, slots)
        };

        let table = Self {
            header,
            slots,
            capacity,
        };
        if !found {
            // Zero/empty the table on first creation (dynahash "zero on first
            // creation"): every slot empty, zero live entries.
            for i in 0..capacity {
                // SAFETY: i < capacity; the region holds `capacity` Slots.
                unsafe { *table.slots.add(i) = Slot::empty() };
            }
            // SAFETY: header points at the live region.
            unsafe { (*table.header).nentries = 0 };
        }
        Ok(table)
    }

    #[inline]
    fn slot(&self, idx: usize) -> &Slot {
        debug_assert!(idx < self.capacity);
        // SAFETY: idx < capacity; caller holds the partition lock.
        unsafe { &*self.slots.add(idx) }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn slot_mut(&self, idx: usize) -> &mut Slot {
        debug_assert!(idx < self.capacity);
        // SAFETY: idx < capacity; caller holds the exclusive partition lock.
        unsafe { &mut *self.slots.add(idx) }
    }

    #[inline]
    fn nentries(&self) -> usize {
        // SAFETY: header is live; caller holds the partition lock.
        unsafe { (*self.header).nentries }
    }

    #[inline]
    fn set_nentries(&self, v: usize) {
        // SAFETY: header is live; caller holds the exclusive partition lock.
        unsafe { (*self.header).nentries = v };
    }

    fn table_capacity(size: i32) -> usize {
        // Open addressing wants headroom to keep probe chains short; round the
        // requested max_size up to twice its size (the dynahash never holds more
        // than `size` live entries, so this never fills).
        (size.max(1) as usize) * 2
    }

    fn shmem_bytes(capacity: usize) -> Size {
        // size_of(BufTableHeader) + capacity * size_of(Slot) — the actual bytes
        // carved from the shared segment.
        core::mem::size_of::<BufTableHeader>() + capacity * core::mem::size_of::<Slot>()
    }

    /// `BufTableHashCode`.
    pub fn hash_code(&self, tag: &buftag) -> u32 {
        buf_table_hash_code(tag)
    }

    fn probe_start(&self, hashcode: u32) -> usize {
        (hashcode as usize) % self.capacity
    }

    /// `BufTableLookup` — return the buffer id, or -1 if not present. Caller must
    /// hold at least share lock on the tag's `BufferMappingLock`.
    pub fn lookup(&self, tag: &buftag, hashcode: u32) -> i32 {
        let start = self.probe_start(hashcode);
        for step in 0..self.capacity {
            let idx = (start + step) % self.capacity;
            let slot = self.slot(idx);
            if slot.is_empty() {
                return -1;
            }
            if &slot.key == tag {
                return slot.id;
            }
        }
        -1
    }

    /// `BufTableInsert` — insert `tag -> buf_id`, unless an entry already exists
    /// for that tag. Returns -1 on successful insertion; if a conflicting entry
    /// exists already returns the buffer ID in that entry. Caller must hold
    /// exclusive lock on the tag's `BufferMappingLock`.
    pub fn insert(&self, tag: buftag, hashcode: u32, buf_id: i32) -> PgResult<i32> {
        debug_assert!(buf_id >= 0); // -1 is reserved for not-in-table
        let start = self.probe_start(hashcode);
        let mut free_slot: Option<usize> = None;
        for step in 0..self.capacity {
            let idx = (start + step) % self.capacity;
            let slot = self.slot(idx);
            if slot.is_empty() {
                free_slot = Some(idx);
                break;
            }
            if slot.key == tag {
                return Ok(slot.id);
            }
        }
        let idx = free_slot
            .ok_or_else(|| PgError::error("out of shared memory (buffer lookup table is full)"))?;
        *self.slot_mut(idx) = Slot { key: tag, id: buf_id };
        self.set_nentries(self.nentries() + 1);
        Ok(-1)
    }

    /// `BufTableDelete` — delete the hashtable entry for `tag` (which must
    /// exist); mirrors C's `elog(ERROR, "shared buffer hash table corrupted")`
    /// when absent. Caller must hold exclusive lock on the tag's
    /// `BufferMappingLock`.
    pub fn delete(&self, tag: &buftag, hashcode: u32) -> PgResult<()> {
        let start = self.probe_start(hashcode);
        let mut found_idx = None;
        for step in 0..self.capacity {
            let idx = (start + step) % self.capacity;
            let slot = self.slot(idx);
            if slot.is_empty() {
                break;
            }
            if &slot.key == tag {
                found_idx = Some(idx);
                break;
            }
        }
        let Some(idx) = found_idx else {
            return Err(PgError::error("shared buffer hash table corrupted"));
        };
        *self.slot_mut(idx) = Slot::empty();
        self.set_nentries(self.nentries() - 1);
        // Standard open-addressing tombstone repair: collect the rest of the
        // probe chain so no live entry is left unreachable, then re-insert.
        let mut chain: alloc::vec::Vec<Slot> = alloc::vec::Vec::new();
        let mut next = (idx + 1) % self.capacity;
        while !self.slot(next).is_empty() {
            chain.push(*self.slot(next));
            *self.slot_mut(next) = Slot::empty();
            self.set_nentries(self.nentries() - 1);
            next = (next + 1) % self.capacity;
        }
        // Re-insert every entry pulled off the chain.
        for ent in chain {
            let code = buf_table_hash_code(&ent.key);
            self.insert(ent.key, code, ent.id)?;
        }
        Ok(())
    }

    /// Live entry count.
    pub fn len(&self) -> usize {
        self.nentries()
    }

    /// Whether the table holds no live entries.
    pub fn is_empty(&self) -> bool {
        self.nentries() == 0
    }

    /// The `BufferMappingLock` partition tranche name, for callers that need
    /// `GetNamedLWLockTranche`.
    pub fn buffer_mapping_tranche() -> &'static str {
        "BufferMapping"
    }
}

/// `BufTableShmemSize(size)` — estimate the shared-memory footprint of the
/// lookup hash. Mirrors `hash_estimate_size(size + NUM_BUFFER_PARTITIONS,
/// sizeof(BufferLookupEnt))`; here it is the open-addressing footprint.
pub fn BufTableShmemSize(size: i32) -> Size {
    let capacity = BufTable::table_capacity(size);
    BufTable::shmem_bytes(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::install_test_seams;
    use types_core::primitive::ForkNumber;

    fn tag(rel: u32, blk: u32) -> buftag {
        buftag {
            spcOid: 1663,
            dbOid: 5,
            relNumber: rel,
            forkNum: ForkNumber::MAIN_FORKNUM,
            blockNum: blk,
        }
    }

    #[test]
    fn insert_then_lookup() {
        let _g = install_test_seams();
        let t = BufTable::InitBufTable(16).unwrap();
        let c = buf_table_hash_code(&tag(1, 0));
        assert_eq!(t.lookup(&tag(1, 0), c), -1);
        assert_eq!(t.insert(tag(1, 0), c, 42).unwrap(), -1);
        assert_eq!(t.lookup(&tag(1, 0), c), 42);
        // Re-insert of same tag returns the existing id, no overwrite.
        assert_eq!(t.insert(tag(1, 0), c, 99).unwrap(), 42);
        assert_eq!(t.lookup(&tag(1, 0), c), 42);
    }

    #[test]
    fn delete_missing_is_error() {
        let _g = install_test_seams();
        let t = BufTable::InitBufTable(8).unwrap();
        let c = buf_table_hash_code(&tag(7, 7));
        assert!(t.delete(&tag(7, 7), c).is_err());
        t.insert(tag(7, 7), c, 1).unwrap();
        assert!(t.delete(&tag(7, 7), c).is_ok());
        assert!(t.is_empty());
    }

    #[test]
    fn delete_repairs_probe_chain() {
        let _g = install_test_seams();
        let t = BufTable::InitBufTable(16).unwrap();
        for blk in 0..8 {
            let c = buf_table_hash_code(&tag(3, blk));
            t.insert(tag(3, blk), c, blk as i32).unwrap();
        }
        let c2 = buf_table_hash_code(&tag(3, 2));
        t.delete(&tag(3, 2), c2).unwrap();
        assert_eq!(t.lookup(&tag(3, 2), c2), -1);
        for blk in [0u32, 1, 3, 4, 5, 6, 7] {
            let c = buf_table_hash_code(&tag(3, blk));
            assert_eq!(t.lookup(&tag(3, blk), c), blk as i32);
        }
    }

    #[test]
    fn equal_tags_hash_equally() {
        assert_eq!(
            buf_table_hash_code(&tag(3, 9)),
            buf_table_hash_code(&tag(3, 9))
        );
    }

    #[test]
    fn partition_in_range() {
        for h in [0u32, 1, 127, 128, 1_000_000, u32::MAX] {
            assert!(buf_table_hash_partition(h) < NUM_BUFFER_PARTITIONS as u32);
        }
    }
}
