//! dshash rendered thread-native: one backend = one thread, so the dsa layer
//! disappears. Items and bucket arrays are stable global-allocator heap blocks
//! (shared cross-thread state lives outside mcx arenas — AGENTS.md rule 3);
//! a raw item address stands in for dsa_pointer + dsa_get_address, and the
//! shared table reference itself is C's dshash_table_handle (attach/detach
//! dissolve). Locking, partition layout, growth rule, and scan semantics keep
//! C's observable behavior exactly.

use core::cell::UnsafeCell;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
use core::ptr;
use core::sync::atomic::{AtomicPtr, AtomicU32, Ordering::Relaxed};

use init_small::globals;
use lwlock::{
    LWLock, LWLockAcquire, LWLockHeldByMeInMode, LWLockPadded, LWLockRelease, LWLockMode,
    LW_EXCLUSIVE, LW_SHARED,
};
use types_error::PgResult;

pub const DSHASH_NUM_PARTITIONS_LOG2: u32 = 7;
pub const DSHASH_NUM_PARTITIONS: usize = 1 << DSHASH_NUM_PARTITIONS_LOG2;

pub type DshashHash = u32;

// C's dshash_parameters: hash/compare/copy resolved once at create/attach,
// monomorphized here instead of fn pointers; `&self` carries C's `arg`.
pub trait DshashParams: Send + Sync {
    type Key: ?Sized;
    type Entry: Send + Sync;

    fn hash(&self, key: &Self::Key) -> DshashHash;
    fn keys_equal(&self, a: &Self::Key, b: &Self::Key) -> bool;
    fn entry_key<'e>(&self, entry: &'e Self::Entry) -> &'e Self::Key;
    // C copies only the key into the fresh item (copy_function) and the caller
    // initializes the rest under the exclusive lock; constructing the whole
    // entry is the safe superset of that contract.
    fn new_entry(&self, key: &Self::Key) -> Self::Entry;
}

struct Item<E> {
    next: *mut Item<E>,
    hash: DshashHash,
    entry: E,
}

struct Partition {
    lock: LWLock,
    // Mutated only under this partition's exclusive lock (dshash_partition.count).
    count: UnsafeCell<usize>,
}

pub struct DshashTable<P: DshashParams> {
    params: P,
    partitions: [Partition; DSHASH_NUM_PARTITIONS],
    // Written only with ALL partition locks held; read while holding any one.
    // The lock acquire orders the reads, so Relaxed matches C's plain loads.
    size_log2: AtomicU32,
    buckets: AtomicPtr<AtomicPtr<Item<P::Entry>>>,
}

// SAFETY: partition counts are touched only under that partition's exclusive
// LWLock; bucket heads and the bucket-array/size pair follow the C locking
// protocol above; entries are bounded Send + Sync by the trait.
unsafe impl<P: DshashParams> Send for DshashTable<P> {}
unsafe impl<P: DshashParams> Sync for DshashTable<P> {}

fn num_splits(size_log2: u32) -> u32 {
    size_log2 - DSHASH_NUM_PARTITIONS_LOG2
}

fn num_buckets(size_log2: u32) -> usize {
    1 << size_log2
}

fn buckets_per_partition(size_log2: u32) -> usize {
    1 << num_splits(size_log2)
}

fn max_count_per_partition(size_log2: u32) -> usize {
    buckets_per_partition(size_log2) / 2 + buckets_per_partition(size_log2) / 4
}

fn partition_for_hash(hash: DshashHash) -> usize {
    (hash >> (32 - DSHASH_NUM_PARTITIONS_LOG2)) as usize
}

fn bucket_index_for_hash(hash: DshashHash, size_log2: u32) -> usize {
    (hash >> (32 - size_log2)) as usize
}

fn partition_for_bucket_index(bucket_idx: usize, size_log2: u32) -> usize {
    bucket_idx >> num_splits(size_log2)
}

fn alloc_bucket_array<E>(n: usize) -> *mut AtomicPtr<Item<E>> {
    let v: Box<[AtomicPtr<Item<E>>]> = (0..n).map(|_| AtomicPtr::new(ptr::null_mut())).collect();
    Box::into_raw(v) as *mut AtomicPtr<Item<E>>
}

unsafe fn free_bucket_array<E>(p: *mut AtomicPtr<Item<E>>, n: usize) {
    drop(Box::from_raw(ptr::slice_from_raw_parts_mut(p, n)));
}

impl<P: DshashParams> DshashTable<P> {
    pub fn create(params: P, tranche_id: i32) -> Self {
        Self {
            params,
            partitions: core::array::from_fn(|_| Partition {
                lock: LWLockPadded::new_unlocked(tranche_id).lock,
                count: UnsafeCell::new(0),
            }),
            size_log2: AtomicU32::new(DSHASH_NUM_PARTITIONS_LOG2),
            buckets: AtomicPtr::new(alloc_bucket_array::<P::Entry>(DSHASH_NUM_PARTITIONS)),
        }
    }

    pub fn params(&self) -> &P {
        &self.params
    }

    fn lock(&self, partition: usize) -> &LWLock {
        &self.partitions[partition].lock
    }

    // Caller must hold at least one partition lock (interlocks resize).
    unsafe fn view(&self) -> (&[AtomicPtr<Item<P::Entry>>], u32) {
        let size_log2 = self.size_log2.load(Relaxed);
        let p = self.buckets.load(Relaxed);
        (
            core::slice::from_raw_parts(p, num_buckets(size_log2)),
            size_log2,
        )
    }

    unsafe fn bucket_for_hash(&self, hash: DshashHash) -> &AtomicPtr<Item<P::Entry>> {
        let (buckets, size_log2) = self.view();
        &buckets[bucket_index_for_hash(hash, size_log2)]
    }

    fn assert_no_partition_locks(&self) {
        #[cfg(debug_assertions)]
        for p in &self.partitions {
            debug_assert!(!lwlock::LWLockHeldByMe(&p.lock));
        }
    }

    unsafe fn find_in_bucket(
        &self,
        key: &P::Key,
        head: &AtomicPtr<Item<P::Entry>>,
    ) -> *mut Item<P::Entry> {
        let mut item = head.load(Relaxed);
        while !item.is_null() {
            if self
                .params
                .keys_equal(key, self.params.entry_key(&(*item).entry))
            {
                return item;
            }
            item = (*item).next;
        }
        ptr::null_mut()
    }

    fn find_impl(
        &self,
        key: &P::Key,
        mode: LWLockMode,
    ) -> PgResult<Option<(*mut Item<P::Entry>, usize)>> {
        let hash = self.params.hash(key);
        let partition = partition_for_hash(hash);
        self.assert_no_partition_locks();

        LWLockAcquire(self.lock(partition), mode, globals::MyProcNumber())?;
        // SAFETY: partition lock held; bucket heads of this partition are ours
        // to read in `mode`.
        let item = unsafe { self.find_in_bucket(key, self.bucket_for_hash(hash)) };
        if item.is_null() {
            LWLockRelease(self.lock(partition))?;
            Ok(None)
        } else {
            Ok(Some((item, partition)))
        }
    }

    pub fn find_shared(&self, key: &P::Key) -> PgResult<Option<DshashEntryShared<'_, P>>> {
        Ok(self.find_impl(key, LW_SHARED)?.map(|(item, partition)| {
            DshashEntryShared {
                table: self,
                item,
                partition,
                _not_send: PhantomData,
            }
        }))
    }

    pub fn find_exclusive(&self, key: &P::Key) -> PgResult<Option<DshashEntry<'_, P>>> {
        Ok(self
            .find_impl(key, LW_EXCLUSIVE)?
            .map(|(item, partition)| DshashEntry {
                table: self,
                item,
                partition,
                _not_send: PhantomData,
            }))
    }

    pub fn find_or_insert(&self, key: &P::Key) -> PgResult<(DshashEntry<'_, P>, bool)> {
        let hash = self.params.hash(key);
        let partition_index = partition_for_hash(hash);
        let partition = &self.partitions[partition_index];
        self.assert_no_partition_locks();

        loop {
            LWLockAcquire(&partition.lock, LW_EXCLUSIVE, globals::MyProcNumber())?;
            // SAFETY: exclusive partition lock held: bucket heads and count of
            // this partition are ours to mutate.
            unsafe {
                let (buckets, size_log2) = self.view();
                let bucket = &buckets[bucket_index_for_hash(hash, size_log2)];
                let item = self.find_in_bucket(key, bucket);
                if !item.is_null() {
                    return Ok((
                        DshashEntry {
                            table: self,
                            item,
                            partition: partition_index,
                            _not_send: PhantomData,
                        },
                        true,
                    ));
                }

                if *partition.count.get() > max_count_per_partition(size_log2) {
                    // Resize reacquires all locks in order; give ours up first.
                    LWLockRelease(&partition.lock)?;
                    self.resize(size_log2 + 1)?;
                    continue;
                }

                let item = Box::into_raw(Box::new(Item {
                    next: bucket.load(Relaxed),
                    hash,
                    entry: self.params.new_entry(key),
                }));
                bucket.store(item, Relaxed);
                *partition.count.get() += 1;
                return Ok((
                    DshashEntry {
                        table: self,
                        item,
                        partition: partition_index,
                        _not_send: PhantomData,
                    },
                    false,
                ));
            }
        }
    }

    pub fn delete_key(&self, key: &P::Key) -> PgResult<bool> {
        self.assert_no_partition_locks();
        let hash = self.params.hash(key);
        let partition = partition_for_hash(hash);

        LWLockAcquire(self.lock(partition), LW_EXCLUSIVE, globals::MyProcNumber())?;
        // SAFETY: exclusive partition lock held.
        let found = unsafe {
            let head = self.bucket_for_hash(hash);
            if self.delete_key_from_bucket(key, head) {
                let count = self.partitions[partition].count.get();
                debug_assert!(*count > 0);
                *count -= 1;
                true
            } else {
                false
            }
        };
        LWLockRelease(self.lock(partition))?;
        Ok(found)
    }

    unsafe fn delete_key_from_bucket(
        &self,
        key: &P::Key,
        head: &AtomicPtr<Item<P::Entry>>,
    ) -> bool {
        let mut prev: *mut Item<P::Entry> = ptr::null_mut();
        let mut cur = head.load(Relaxed);
        while !cur.is_null() {
            if self
                .params
                .keys_equal(key, self.params.entry_key(&(*cur).entry))
            {
                let next = (*cur).next;
                if prev.is_null() {
                    head.store(next, Relaxed);
                } else {
                    (*prev).next = next;
                }
                drop(Box::from_raw(cur));
                return true;
            }
            prev = cur;
            cur = (*cur).next;
        }
        false
    }

    // Caller holds the item's partition lock exclusively.
    unsafe fn delete_item(&self, item: *mut Item<P::Entry>) {
        let hash = (*item).hash;
        let partition = partition_for_hash(hash);
        debug_assert!(LWLockHeldByMeInMode(self.lock(partition), LW_EXCLUSIVE));

        let head = self.bucket_for_hash(hash);
        let mut prev: *mut Item<P::Entry> = ptr::null_mut();
        let mut cur = head.load(Relaxed);
        while !cur.is_null() {
            if cur == item {
                let next = (*cur).next;
                if prev.is_null() {
                    head.store(next, Relaxed);
                } else {
                    (*prev).next = next;
                }
                drop(Box::from_raw(cur));
                let count = self.partitions[partition].count.get();
                debug_assert!(*count > 0);
                *count -= 1;
                return;
            }
            prev = cur;
            cur = (*cur).next;
        }
        debug_assert!(false, "dshash: locked item not found in its bucket");
    }

    fn resize(&self, new_size_log2: u32) -> PgResult<()> {
        let proc = globals::MyProcNumber();
        for i in 0..DSHASH_NUM_PARTITIONS {
            debug_assert!(!lwlock::LWLockHeldByMe(self.lock(i)));
            if let Err(e) = LWLockAcquire(self.lock(i), LW_EXCLUSIVE, proc) {
                for j in 0..i {
                    let _ = LWLockRelease(self.lock(j));
                }
                return Err(e);
            }
            if i == 0 && self.size_log2.load(Relaxed) >= new_size_log2 {
                // Another backend already grew the table.
                LWLockRelease(self.lock(0))?;
                return Ok(());
            }
        }
        debug_assert!(new_size_log2 == self.size_log2.load(Relaxed) + 1);

        let new_n = num_buckets(new_size_log2);
        let new_buckets = alloc_bucket_array::<P::Entry>(new_n);
        // SAFETY: all partition locks held: sole access to every bucket and to
        // the buckets/size_log2 pair.
        unsafe {
            let (old, old_size_log2) = self.view();
            let new = core::slice::from_raw_parts(new_buckets, new_n);
            for head in old {
                let mut item = head.load(Relaxed);
                while !item.is_null() {
                    let next = (*item).next;
                    let slot = &new[bucket_index_for_hash((*item).hash, new_size_log2)];
                    (*item).next = slot.load(Relaxed);
                    slot.store(item, Relaxed);
                    item = next;
                }
            }
            let old_p = self.buckets.load(Relaxed);
            self.buckets.store(new_buckets, Relaxed);
            self.size_log2.store(new_size_log2, Relaxed);
            free_bucket_array(old_p, num_buckets(old_size_log2));
        }

        for i in 0..DSHASH_NUM_PARTITIONS {
            LWLockRelease(self.lock(i))?;
        }
        Ok(())
    }

    pub fn seq_scan(&self, exclusive: bool) -> DshashSeqScan<'_, P> {
        DshashSeqScan {
            table: self,
            curbucket: 0,
            nbuckets: 0,
            curitem: ptr::null_mut(),
            pnextitem: ptr::null_mut(),
            curpartition: -1,
            exclusive,
            buckets: ptr::null(),
            size_log2: 0,
            _not_send: PhantomData,
        }
    }
}

impl<P: DshashParams> Drop for DshashTable<P> {
    // dshash_destroy; `&mut self` is C's "no other backend attached" precondition.
    fn drop(&mut self) {
        let size_log2 = *self.size_log2.get_mut();
        let n = num_buckets(size_log2);
        let buckets = *self.buckets.get_mut();
        // SAFETY: exclusive ownership; frees every item chain then the array.
        unsafe {
            for i in 0..n {
                let mut item = (*buckets.add(i)).load(Relaxed);
                while !item.is_null() {
                    let next = (*item).next;
                    drop(Box::from_raw(item));
                    item = next;
                }
            }
            free_bucket_array(buckets, n);
        }
    }
}

// dshash_find(exclusive=false) result: entry readable while the shared
// partition lock is held; drop = dshash_release_lock.
pub struct DshashEntryShared<'t, P: DshashParams> {
    table: &'t DshashTable<P>,
    item: *mut Item<P::Entry>,
    partition: usize,
    _not_send: PhantomData<*mut ()>,
}

impl<P: DshashParams> Deref for DshashEntryShared<'_, P> {
    type Target = P::Entry;
    fn deref(&self) -> &P::Entry {
        // SAFETY: shared partition lock held for the guard's lifetime.
        unsafe { &(*self.item).entry }
    }
}

impl<P: DshashParams> Drop for DshashEntryShared<'_, P> {
    fn drop(&mut self) {
        LWLockRelease(self.table.lock(self.partition)).expect("dshash: release shared lock");
    }
}

// dshash_find(exclusive=true) / dshash_find_or_insert result; drop =
// dshash_release_lock, delete(self) = dshash_delete_entry.
pub struct DshashEntry<'t, P: DshashParams> {
    table: &'t DshashTable<P>,
    item: *mut Item<P::Entry>,
    partition: usize,
    _not_send: PhantomData<*mut ()>,
}

impl<P: DshashParams> DshashEntry<'_, P> {
    pub fn delete(self) {
        // SAFETY: exclusive partition lock held; item was obtained under it.
        unsafe { self.table.delete_item(self.item) };
        // Drop releases the partition lock, as dshash_delete_entry does.
    }
}

impl<P: DshashParams> Deref for DshashEntry<'_, P> {
    type Target = P::Entry;
    fn deref(&self) -> &P::Entry {
        // SAFETY: exclusive partition lock held for the guard's lifetime.
        unsafe { &(*self.item).entry }
    }
}

impl<P: DshashParams> DerefMut for DshashEntry<'_, P> {
    fn deref_mut(&mut self) -> &mut P::Entry {
        // SAFETY: exclusive partition lock held for the guard's lifetime.
        unsafe { &mut (*self.item).entry }
    }
}

impl<P: DshashParams> Drop for DshashEntry<'_, P> {
    fn drop(&mut self) {
        LWLockRelease(self.table.lock(self.partition)).expect("dshash: release exclusive lock");
    }
}

// dshash_seq_status; drop = dshash_seq_term. Holds one partition lock from the
// first next() until term, so the table cannot resize mid-scan (C invariant).
pub struct DshashSeqScan<'t, P: DshashParams> {
    table: &'t DshashTable<P>,
    curbucket: usize,
    nbuckets: usize,
    curitem: *mut Item<P::Entry>,
    pnextitem: *mut Item<P::Entry>,
    curpartition: isize,
    exclusive: bool,
    buckets: *const AtomicPtr<Item<P::Entry>>,
    size_log2: u32,
    _not_send: PhantomData<*mut ()>,
}

impl<P: DshashParams> DshashSeqScan<'_, P> {
    fn mode(&self) -> LWLockMode {
        if self.exclusive {
            LW_EXCLUSIVE
        } else {
            LW_SHARED
        }
    }

    unsafe fn bucket(&self, i: usize) -> &AtomicPtr<Item<P::Entry>> {
        &*self.buckets.add(i)
    }

    fn next_item(&mut self) -> PgResult<Option<*mut Item<P::Entry>>> {
        let mut next = if self.curpartition == -1 {
            debug_assert!(self.curbucket == 0);
            self.table.assert_no_partition_locks();
            self.curpartition = 0;
            LWLockAcquire(self.table.lock(0), self.mode(), globals::MyProcNumber())?;
            // SAFETY: lock held; snapshot is stable until term (resize blocked).
            let (buckets, size_log2) = unsafe { self.table.view() };
            self.buckets = buckets.as_ptr();
            self.size_log2 = size_log2;
            self.nbuckets = num_buckets(size_log2);
            unsafe { self.bucket(0).load(Relaxed) }
        } else {
            self.pnextitem
        };

        debug_assert!(LWLockHeldByMeInMode(
            self.table.lock(self.curpartition as usize),
            self.mode()
        ));

        while next.is_null() {
            self.curbucket += 1;
            if self.curbucket >= self.nbuckets {
                return Ok(None);
            }

            let next_partition = partition_for_bucket_index(self.curbucket, self.size_log2);
            if self.curpartition != next_partition as isize {
                // Lock the next partition before releasing the current one so
                // resize can never start mid-scan; same order as resize().
                LWLockAcquire(
                    self.table.lock(next_partition),
                    self.mode(),
                    globals::MyProcNumber(),
                )?;
                LWLockRelease(self.table.lock(self.curpartition as usize))?;
                self.curpartition = next_partition as isize;
            }

            // SAFETY: partition lock held; snapshot valid for the whole scan.
            next = unsafe { self.bucket(self.curbucket).load(Relaxed) };
        }

        self.curitem = next;
        // The caller may delete_current(); remember the next item now.
        self.pnextitem = unsafe { (*next).next };
        Ok(Some(next))
    }

    pub fn next(&mut self) -> PgResult<Option<&P::Entry>> {
        // SAFETY: item stays valid while our partition lock is held; the
        // returned borrow ends before the next scan call can free it.
        Ok(self.next_item()?.map(|item| unsafe { &(*item).entry }))
    }

    pub fn next_mut(&mut self) -> PgResult<Option<&mut P::Entry>> {
        assert!(self.exclusive);
        // SAFETY: exclusive partition lock held, as above.
        Ok(self.next_item()?.map(|item| unsafe { &mut (*item).entry }))
    }

    pub fn delete_current(&mut self) {
        assert!(self.exclusive);
        debug_assert!(!self.curitem.is_null());
        // SAFETY: exclusive lock on curitem's partition is held by this scan.
        unsafe { self.table.delete_item(self.curitem) };
        self.curitem = ptr::null_mut();
    }
}

impl<P: DshashParams> Drop for DshashSeqScan<'_, P> {
    fn drop(&mut self) {
        if self.curpartition >= 0 {
            LWLockRelease(self.table.lock(self.curpartition as usize))
                .expect("dshash: release scan lock");
        }
    }
}

pub fn dshash_memhash(v: &[u8]) -> DshashHash {
    hashfn::tag_hash(v, v.len())
}

pub fn dshash_strhash(v: &[u8], key_size: usize) -> DshashHash {
    hashfn::string_hash(v, key_size)
}

#[cfg(test)]
mod tests;
