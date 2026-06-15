//! `buf_table.c` — the shared buffer lookup hash table.
//!
//! In PostgreSQL `SharedBufHash` is a partitioned dynahash in the shared-memory
//! segment, protected by the `BufferMappingLock` partition LWLocks. It is
//! modeled here as a fixed-capacity open-addressing table of [`LocalEnt`] slots
//! (capacity = `2 * (NBuffers + NUM_BUFFER_PARTITIONS)`, headroom over the
//! dynahash `max_size` so probe chains stay short). The hashing
//! (`BufTableHashCode`) and the lookup/insert/delete semantics are the verbatim
//! algorithm; the substrate — *where* the table lives — is signaled through the
//! `ShmemInitStruct` seam.
//!
//! The routines do NO locking of their own: the caller must hold the
//! appropriate `BufferMappingLock` partition lock, exactly as `buf_table.c`
//! requires.

use std::cell::{Cell, RefCell};

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

/// One occupied slot of the lookup table. Mirrors dynahash's
/// `BufferLookupEnt { BufferTag key; int id; }`.
#[derive(Clone, Copy, Debug)]
struct LocalEnt {
    key: buftag,
    id: i32,
}

/// `SharedBufHash` — the buffer-mapping lookup hash, modeled as an
/// open-addressing table. The underlying shared-memory allocation is signaled
/// through the `ShmemInitStruct` seam (which returns whether the table already
/// existed, so `InitBufTable` runs the "zero on first creation" path exactly
/// once).
pub struct BufTable {
    /// Fixed-capacity open-addressing slots; `None` = empty.
    slots: RefCell<alloc::vec::Vec<Option<LocalEnt>>>,
    /// Live entry count.
    nentries: Cell<usize>,
    capacity: usize,
}

impl BufTable {
    /// `InitBufTable(size)` — place the lookup table in shared memory. `size` is
    /// the dynahash `max_size` (`NBuffers + NUM_BUFFER_PARTITIONS`). Honors the
    /// `found` flag like C: on first creation build/zero the table; on attach
    /// the shared table already exists (modeled here by building the empty
    /// table, since the in-crate handle does not alias another backend's
    /// segment in this substrate).
    pub fn InitBufTable(size: i32) -> PgResult<Self> {
        let capacity = Self::table_capacity(size);
        let bytes = Self::shmem_bytes(capacity);
        // ShmemInitStruct("Shared Buffer Lookup Table", bytes, &found).
        let (_addr, _found) =
            backend_storage_ipc_shmem_seams::shmem_init_struct::call("Shared Buffer Lookup Table", bytes)?;
        // Zero/empty the table on creation (the dynahash "zero on first
        // creation" path).
        let mut slots = alloc::vec::Vec::new();
        slots
            .try_reserve(capacity)
            .map_err(|_| PgError::error("out of shared memory (buffer lookup table)"))?;
        slots.resize(capacity, None);
        Ok(Self {
            slots: RefCell::new(slots),
            nentries: Cell::new(0),
            capacity,
        })
    }

    fn table_capacity(size: i32) -> usize {
        // Open addressing wants headroom to keep probe chains short; round the
        // requested max_size up to twice its size (the dynahash never holds more
        // than `size` live entries, so this never fills).
        (size.max(1) as usize) * 2
    }

    fn shmem_bytes(capacity: usize) -> Size {
        // size_of(header) + capacity * size_of(BufferLookupEnt); the entry is a
        // BufferTag (20 bytes) + an int id, padded to 24 — mirror dynahash's
        // footprint estimate (the exact value only matters for the seam's
        // parity).
        16 + capacity * 24
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
        let slots = self.slots.borrow();
        let start = self.probe_start(hashcode);
        for step in 0..self.capacity {
            let idx = (start + step) % self.capacity;
            match &slots[idx] {
                None => return -1,
                Some(ent) if &ent.key == tag => return ent.id,
                Some(_) => {}
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
        let mut slots = self.slots.borrow_mut();
        let start = self.probe_start(hashcode);
        let mut free_slot: Option<usize> = None;
        for step in 0..self.capacity {
            let idx = (start + step) % self.capacity;
            match &slots[idx] {
                None => {
                    free_slot = Some(idx);
                    break;
                }
                Some(ent) if ent.key == tag => return Ok(ent.id),
                Some(_) => {}
            }
        }
        let idx = free_slot
            .ok_or_else(|| PgError::error("out of shared memory (buffer lookup table is full)"))?;
        slots[idx] = Some(LocalEnt { key: tag, id: buf_id });
        self.nentries.set(self.nentries.get() + 1);
        Ok(-1)
    }

    /// `BufTableDelete` — delete the hashtable entry for `tag` (which must
    /// exist); mirrors C's `elog(ERROR, "shared buffer hash table corrupted")`
    /// when absent. Caller must hold exclusive lock on the tag's
    /// `BufferMappingLock`.
    pub fn delete(&self, tag: &buftag, hashcode: u32) -> PgResult<()> {
        let start = self.probe_start(hashcode);
        let removed;
        {
            let mut slots = self.slots.borrow_mut();
            let mut found_idx = None;
            for step in 0..self.capacity {
                let idx = (start + step) % self.capacity;
                match &slots[idx] {
                    None => break,
                    Some(ent) if &ent.key == tag => {
                        found_idx = Some(idx);
                        break;
                    }
                    Some(_) => {}
                }
            }
            let Some(idx) = found_idx else {
                return Err(PgError::error("shared buffer hash table corrupted"));
            };
            slots[idx] = None;
            self.nentries.set(self.nentries.get() - 1);
            // Standard open-addressing tombstone repair: collect the rest of the
            // probe chain so no live entry is left unreachable, then re-insert.
            let mut chain: alloc::vec::Vec<LocalEnt> = alloc::vec::Vec::new();
            let mut next = (idx + 1) % self.capacity;
            while let Some(ent) = slots[next] {
                chain.push(ent);
                slots[next] = None;
                self.nentries.set(self.nentries.get() - 1);
                next = (next + 1) % self.capacity;
            }
            removed = chain;
        }
        // Re-insert every entry pulled off the chain.
        for ent in removed {
            let code = buf_table_hash_code(&ent.key);
            self.insert(ent.key, code, ent.id)?;
        }
        Ok(())
    }

    /// Live entry count.
    pub fn len(&self) -> usize {
        self.nentries.get()
    }

    /// Whether the table holds no live entries.
    pub fn is_empty(&self) -> bool {
        self.nentries.get() == 0
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
