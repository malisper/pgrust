//! `buf_table.c` — the shared buffer lookup hash table.
//!
//! In PostgreSQL `SharedBufHash` is a partitioned dynahash in the shared-memory
//! segment, protected by the `BufferMappingLock` partition LWLocks. It is
//! modeled here as `NUM_BUFFER_PARTITIONS` independent fixed-capacity
//! open-addressing sub-tables of [`Slot`]s — one per partition. The hashing
//! (`BufTableHashCode`) and the lookup/insert/delete semantics are the verbatim
//! algorithm.
//!
//! ## Why genuinely-partitioned sub-tables (not one flat array)
//!
//! C's `SharedBufHash` is a *partitioned* dynahash: each of the
//! `NUM_BUFFER_PARTITIONS` partitions owns an independent set of bucket chains
//! and freelists, so two backends operating on tags in different partitions —
//! each holding only its own `BufferMappingLock` partition lock — never touch
//! the same memory. An earlier version of this port modeled the table as a
//! *single* flat open-addressing array. That is INCORRECT under partitioned
//! locking: open-addressing probe chains (and `delete`'s tombstone-repair
//! re-insertion) span arbitrary slots, so a `delete` in partition A and an
//! `insert` in partition B — held under *different* locks — could mutate the
//! same slots concurrently, corrupting the shared array. Under heavy parallel
//! contention (e.g. a parallel bitmap heap scan with small `shared_buffers`)
//! this surfaced as `shared buffer hash table corrupted`, wrong buffer/block
//! mappings, and assertion failures in forked workers. Splitting into one
//! sub-table per partition restores C's invariant: a tag's hash code chooses
//! its partition AND confines its entire probe chain to that partition's own
//! slot region, so the caller-held partition lock fully serializes every access
//! to that region and no operation crosses a partition boundary.
//!
//! The slot regions AND the per-partition live-entry counters live in the
//! `MAP_SHARED` shared-memory segment, carved through the `ShmemInitStruct`
//! seam: the returned base address is the same in every forked backend, so the
//! `BufferTag -> buf_id` mapping one backend installs is visible to all others
//! — the real `SharedBufHash` posture. Without this, a backend that caches a
//! catalog page would record the mapping only in its own process heap and the
//! page would be invisible to every other connection.
//!
//! The routines do NO locking of their own: the caller must hold the
//! appropriate `BufferMappingLock` partition lock, exactly as `buf_table.c`
//! requires.

use ::types_core::Size;
use ::types_error::{PgError, PgResult};
use ::types_storage::buf::buftag;
use ::types_storage::NUM_BUFFER_PARTITIONS;

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
    hashfn::hash_bytes(&bytes)
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

/// The shmem-resident header for the lookup table — one live-entry count per
/// partition, reached under that partition's `BufferMappingLock` exactly as the
/// partition's slot region is. (`repr(C)`; placed at the region start, the
/// per-partition slot regions follow.)
#[repr(C)]
struct BufTableHeader {
    /// `nentries[partition]` — live entries in each partition's sub-table.
    nentries: [usize; NUM_BUFFER_PARTITIONS as usize],
}

/// `SharedBufHash` — the buffer-mapping lookup hash, modeled as
/// `NUM_BUFFER_PARTITIONS` independent open-addressing sub-tables whose slot
/// regions AND per-partition live-entry counters live in the `MAP_SHARED`
/// segment (carved through the `ShmemInitStruct` seam). The struct itself is a
/// process-local *view*: it holds raw base pointers into the shared region,
/// identical in every forked backend, so the mapping is shared.
///
/// A tag's hash code selects its partition (`hashcode % NUM_BUFFER_PARTITIONS`)
/// AND — within that partition's contiguous slot region of `cap_per_part`
/// slots — its probe start. Every probe/insert/delete/tombstone-repair for a
/// tag stays inside its own partition region, so the caller-held
/// `BufferMappingLock` partition lock fully serializes all access to that
/// region (C's partitioned-dynahash invariant).
pub struct BufTable {
    /// Base pointer to the shmem-resident `BufTableHeader` (per-partition entry
    /// counters).
    header: *mut BufTableHeader,
    /// Base pointer to the shmem-resident slot array. Logically
    /// `NUM_BUFFER_PARTITIONS` contiguous regions of `cap_per_part` `Slot`s; the
    /// region for partition `p` starts at slot index `p * cap_per_part`.
    slots: *mut Slot,
    /// Slots per partition region.
    cap_per_part: usize,
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
        let cap_per_part = Self::cap_per_part(size);
        let total_slots = cap_per_part
            .checked_mul(NUM_BUFFER_PARTITIONS as usize)
            .ok_or_else(|| PgError::error("buffer lookup table size overflow"))?;
        let header_bytes = core::mem::size_of::<BufTableHeader>();
        let slots_bytes = total_slots
            .checked_mul(core::mem::size_of::<Slot>())
            .ok_or_else(|| PgError::error("buffer lookup table size overflow"))?;
        let bytes = header_bytes + slots_bytes;
        // ShmemInitStruct("Shared Buffer Lookup Table", bytes, &found).
        let (addr, found) = ipc_shmem_seams::shmem_init_struct::call(
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
            let s_layout = core::alloc::Layout::array::<Slot>(total_slots.max(1))
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
            cap_per_part,
        };
        if !found {
            // Zero/empty the table on first creation (dynahash "zero on first
            // creation"): every slot empty, zero live entries per partition.
            for i in 0..total_slots {
                // SAFETY: i < total_slots; the region holds `total_slots` Slots.
                unsafe { *table.slots.add(i) = Slot::empty() };
            }
            // SAFETY: header points at the live region.
            unsafe {
                (*table.header).nentries = [0; NUM_BUFFER_PARTITIONS as usize];
            }
        }
        Ok(table)
    }

    /// Absolute slot index of `local` within partition `p`'s region.
    #[inline]
    fn abs_idx(&self, partition: usize, local: usize) -> usize {
        debug_assert!(local < self.cap_per_part);
        partition * self.cap_per_part + local
    }

    #[inline]
    fn slot(&self, idx: usize) -> &Slot {
        // SAFETY: idx < total_slots; caller holds the partition lock.
        unsafe { &*self.slots.add(idx) }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn slot_mut(&self, idx: usize) -> &mut Slot {
        // SAFETY: idx < total_slots; caller holds the exclusive partition lock.
        unsafe { &mut *self.slots.add(idx) }
    }

    #[inline]
    fn nentries(&self, partition: usize) -> usize {
        // SAFETY: header is live; caller holds the partition lock.
        unsafe { (*self.header).nentries[partition] }
    }

    #[inline]
    fn set_nentries(&self, partition: usize, v: usize) {
        // SAFETY: header is live; caller holds the exclusive partition lock.
        unsafe {
            (*self.header).nentries[partition] = v;
        }
    }

    /// Per-partition slot capacity. The dynahash never holds more than
    /// `size = NBuffers + NUM_BUFFER_PARTITIONS` live entries cluster-wide; with
    /// uniform hashing each partition's expected load is `size /
    /// NUM_BUFFER_PARTITIONS`. We give each partition region 4x that expected
    /// load plus a small constant of headroom so open-addressing probe chains
    /// stay short and statistical skew across partitions never fills a region
    /// (matching dynahash's own reliance on uniform key distribution).
    fn cap_per_part(size: i32) -> usize {
        let total = (size.max(1) as usize).next_multiple_of(NUM_BUFFER_PARTITIONS as usize);
        let per = total / (NUM_BUFFER_PARTITIONS as usize);
        (per * 4).max(8)
    }

    fn shmem_bytes(cap_per_part: usize) -> Size {
        // size_of(BufTableHeader) + NUM_BUFFER_PARTITIONS * cap_per_part *
        // size_of(Slot) — the actual bytes carved from the shared segment.
        core::mem::size_of::<BufTableHeader>()
            + (NUM_BUFFER_PARTITIONS as usize) * cap_per_part * core::mem::size_of::<Slot>()
    }

    /// `BufTableHashCode`.
    pub fn hash_code(&self, tag: &buftag) -> u32 {
        buf_table_hash_code(tag)
    }

    /// The partition (= `BufferMappingLock` index) a hash code maps to.
    #[inline]
    fn partition_of(&self, hashcode: u32) -> usize {
        (hashcode % (NUM_BUFFER_PARTITIONS as u32)) as usize
    }

    /// Probe start *within the partition region* — a local index in
    /// `[0, cap_per_part)`. Uses the high bits of the hash so it is independent
    /// of the low bits that already picked the partition.
    #[inline]
    fn probe_start_local(&self, hashcode: u32) -> usize {
        // Divide out the partition selector so distinct tags in the same
        // partition spread across the region.
        ((hashcode / (NUM_BUFFER_PARTITIONS as u32)) as usize) % self.cap_per_part
    }

    /// `BufTableLookup` — return the buffer id, or -1 if not present. Caller must
    /// hold at least share lock on the tag's `BufferMappingLock`.
    pub fn lookup(&self, tag: &buftag, hashcode: u32) -> i32 {
        let partition = self.partition_of(hashcode);
        let start = self.probe_start_local(hashcode);
        for step in 0..self.cap_per_part {
            let local = (start + step) % self.cap_per_part;
            let slot = self.slot(self.abs_idx(partition, local));
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
        let partition = self.partition_of(hashcode);
        let start = self.probe_start_local(hashcode);
        let mut free_local: Option<usize> = None;
        for step in 0..self.cap_per_part {
            let local = (start + step) % self.cap_per_part;
            let slot = self.slot(self.abs_idx(partition, local));
            if slot.is_empty() {
                free_local = Some(local);
                break;
            }
            if slot.key == tag {
                return Ok(slot.id);
            }
        }
        let local = free_local
            .ok_or_else(|| PgError::error("out of shared memory (buffer lookup table is full)"))?;
        *self.slot_mut(self.abs_idx(partition, local)) = Slot { key: tag, id: buf_id };
        self.set_nentries(partition, self.nentries(partition) + 1);
        Ok(-1)
    }

    /// `BufTableDelete` — delete the hashtable entry for `tag` (which must
    /// exist); mirrors C's `elog(ERROR, "shared buffer hash table corrupted")`
    /// when absent. Caller must hold exclusive lock on the tag's
    /// `BufferMappingLock`.
    pub fn delete(&self, tag: &buftag, hashcode: u32) -> PgResult<()> {
        let partition = self.partition_of(hashcode);
        let start = self.probe_start_local(hashcode);
        let mut found_local = None;
        for step in 0..self.cap_per_part {
            let local = (start + step) % self.cap_per_part;
            let slot = self.slot(self.abs_idx(partition, local));
            if slot.is_empty() {
                break;
            }
            if &slot.key == tag {
                found_local = Some(local);
                break;
            }
        }
        let Some(local) = found_local else {
            return Err(PgError::error("shared buffer hash table corrupted"));
        };
        let idx = self.abs_idx(partition, local);
        *self.slot_mut(idx) = Slot::empty();
        self.set_nentries(partition, self.nentries(partition) - 1);
        // Standard open-addressing tombstone repair, confined to this
        // partition's region: collect the rest of the probe chain so no live
        // entry is left unreachable, then re-insert. Every chain entry hashes to
        // this same partition (they share the partition region), so re-insert
        // via `insert` lands them back here under the same caller-held lock.
        let mut chain: alloc::vec::Vec<Slot> = alloc::vec::Vec::new();
        let mut next_local = (local + 1) % self.cap_per_part;
        loop {
            let next_idx = self.abs_idx(partition, next_local);
            if self.slot(next_idx).is_empty() {
                break;
            }
            chain.push(*self.slot(next_idx));
            *self.slot_mut(next_idx) = Slot::empty();
            self.set_nentries(partition, self.nentries(partition) - 1);
            next_local = (next_local + 1) % self.cap_per_part;
        }
        // Re-insert every entry pulled off the chain.
        for ent in chain {
            let code = buf_table_hash_code(&ent.key);
            debug_assert_eq!(self.partition_of(code), partition);
            self.insert(ent.key, code, ent.id)?;
        }
        Ok(())
    }

    /// Live entry count (summed across all partitions). NB: a faithful caller
    /// holds at most one partition lock, so this aggregate is only racy-exact;
    /// it exists for tests/diagnostics, not for the locked hot path.
    pub fn len(&self) -> usize {
        (0..NUM_BUFFER_PARTITIONS as usize)
            .map(|p| self.nentries(p))
            .sum()
    }

    /// Whether the table holds no live entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    let cap_per_part = BufTable::cap_per_part(size);
    BufTable::shmem_bytes(cap_per_part)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::install_test_seams;
    use ::types_core::primitive::ForkNumber;

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

    /// A tag's entire probe chain lives in its own partition's slot region, so
    /// an entry installed in one partition never lands in another partition's
    /// slots. This is the invariant that makes per-partition `BufferMappingLock`
    /// locking sound: two backends mutating different partitions touch disjoint
    /// memory. We assert it by checking that after inserting many tags, every
    /// occupied absolute slot index falls inside the region of the partition the
    /// slot's key hashes to.
    #[test]
    fn entries_stay_within_their_partition_region() {
        let _g = install_test_seams();
        let size = 256;
        let t = BufTable::InitBufTable(size).unwrap();
        let cap = BufTable::cap_per_part(size);
        // Insert a spread of tags covering many partitions.
        for rel in 0..50u32 {
            for blk in 0..20u32 {
                let tg = tag(rel, blk);
                let c = buf_table_hash_code(&tg);
                // Re-inserts of distinct tags; ignore the (never-hit) full error.
                let _ = t.insert(tg, c, (rel * 100 + blk) as i32);
            }
        }
        let total = cap * (NUM_BUFFER_PARTITIONS as usize);
        for idx in 0..total {
            let slot = t.slot(idx);
            if slot.is_empty() {
                continue;
            }
            let owning_partition = idx / cap;
            let code = buf_table_hash_code(&slot.key);
            assert_eq!(
                t.partition_of(code),
                owning_partition,
                "slot {idx} holds a key belonging to a different partition"
            );
        }
    }

    /// Inserts and deletes across many partitions interleave without disturbing
    /// each other (the cross-partition-corruption regression). After a churn of
    /// inserts then deletes of half the keys, every surviving key still resolves
    /// to its buffer id and every deleted key is absent.
    #[test]
    fn cross_partition_churn_is_consistent() {
        let _g = install_test_seams();
        let t = BufTable::InitBufTable(512).unwrap();
        let n = 400u32;
        for i in 0..n {
            let tg = tag(i, i.wrapping_mul(7));
            let c = buf_table_hash_code(&tg);
            assert_eq!(t.insert(tg, c, i as i32).unwrap(), -1);
        }
        // Delete every even key.
        for i in (0..n).step_by(2) {
            let tg = tag(i, i.wrapping_mul(7));
            let c = buf_table_hash_code(&tg);
            t.delete(&tg, c).unwrap();
        }
        for i in 0..n {
            let tg = tag(i, i.wrapping_mul(7));
            let c = buf_table_hash_code(&tg);
            if i % 2 == 0 {
                assert_eq!(t.lookup(&tg, c), -1, "deleted key {i} still present");
            } else {
                assert_eq!(t.lookup(&tg, c), i as i32, "surviving key {i} lost");
            }
        }
    }
}
