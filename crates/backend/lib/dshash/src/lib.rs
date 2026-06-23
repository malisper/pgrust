//! Port of `backend/lib/dshash.c` — concurrent hash tables backed by dynamic
//! shared-memory areas.
//!
//! This is an open hashing hash table with a linked list at each bucket,
//! supporting geometric growth (resize only grows). Concurrency uses a fixed
//! `DSHASH_NUM_PARTITIONS = 128` set of independently-locked partitions; a
//! bucket maps to one partition, so find/insert/iterate normally take one lock.
//! A resize briefly takes all partition locks.
//!
//! ## Shared-memory substrate exception
//!
//! Like the C, the head [`DshashTableControl`] object — including its 128
//! `LWLock`+`count` partitions — and every item and bucket array live **inside
//! the DSA area**, addressed by `dsa_pointer`s and reached with
//! `dsa_get_address`, which resolves a backend-local address (`void *`) the code
//! reads/writes through. This crate keeps that residency exactly: the control
//! and items are crate-local `#[repr(C)]` mirrors of `dshash.c`'s file-private
//! structs, read/written through the resolved backend-local address. This is the
//! same blessed `*mut`/`*const` shared-memory substrate exception the ported
//! `dsa.c` and the in-segment `LWLock` take. The per-backend `dshash_table` is
//! backend-local state, `palloc`ed (here `Box`ed) and named by the raw
//! `*mut DshashTable` the seam contract carries.
//!
//! The DSA substrate (`dsa_allocate_extended`/`dsa_free`/`dsa_get_address`) is
//! reached through the DSA owner's seams; the partition `LWLock`s through the
//! lwlock owner's seams. Both panic loudly until their owners land.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;

use lwlock_seams as lwlock;
use ::init_small_seams::my_proc_number;
use dsa_seams as dsa;
use hashfn_seams as hashfn;
use types_core::{ProcNumber, Size};
use types_error::{PgError, PgResult};
use execparallel::{
    DsaPointer, DSA_ALLOC_HUGE, DSA_ALLOC_NO_OOM, DSA_ALLOC_ZERO, INVALID_DSA_POINTER,
};
use ::types_error::ERRCODE_OUT_OF_MEMORY;
use types_storage::{
    dshash_table_handle, DsaArea, DshashKeyKind, DshashParameters, LWLock, LWLockMode, LW_EXCLUSIVE,
    LW_SHARED,
};

pub use ::dshash_seams::DshashEntryGuard;

// ===========================================================================
// File-private constants (top of dshash.c).
// ===========================================================================

/// `DSHASH_NUM_PARTITIONS_LOG2` — set to match `NUM_BUFFER_PARTITIONS`.
const DSHASH_NUM_PARTITIONS_LOG2: u32 = 7;
/// `DSHASH_NUM_PARTITIONS` — `1 << DSHASH_NUM_PARTITIONS_LOG2`.
const DSHASH_NUM_PARTITIONS: usize = 1 << DSHASH_NUM_PARTITIONS_LOG2;
/// `DSHASH_MAGIC` — a magic value used to identify our hash tables.
const DSHASH_MAGIC: u32 = 0x75ff_6a20;

/// `dshash_hash` (`lib/dshash.h`) — the type for hash values.
type dshash_hash = u32;

/// Bits in a `dshash_hash` (`sizeof(dshash_hash) * CHAR_BIT`).
const HASH_BITS: u32 = (core::mem::size_of::<dshash_hash>() * 8) as u32;

// ===========================================================================
// In-DSA structs (the blessed `#[repr(C)]` shared-memory substrate mirror).
// ===========================================================================

/// `struct dshash_table_item` (dshash.c:44) — wraps the user's entry in an
/// envelope with the next-item pointer and the cached hash. The user's entry
/// object follows at `ENTRY_FROM_ITEM(item)`.
#[repr(C)]
struct dshash_table_item {
    /// `dsa_pointer next` — the next item in the same bucket.
    next: DsaPointer,
    /// `dshash_hash hash` — the hashed key, to avoid recomputing it.
    hash: dshash_hash,
}

/// `struct dshash_partition` (dshash.c:73) — per-partition tracking: the lock
/// protecting all of the partition's buckets and the item count for load-factor
/// bookkeeping.
#[repr(C)]
struct dshash_partition {
    /// `LWLock lock` — protects all buckets in this partition.
    lock: LWLock,
    /// `size_t count` — number of items in this partition's buckets.
    count: Size,
}

/// `struct dshash_table_control` (dshash.c:83) — the head object, stored in the
/// DSA area. Read under any one partition lock; the `size_log2`/`buckets`
/// members are written only with all partition locks held.
#[repr(C)]
struct dshash_table_control {
    /// `dshash_table_handle handle`.
    handle: dshash_table_handle,
    /// `uint32 magic`.
    magic: u32,
    /// `dshash_partition partitions[DSHASH_NUM_PARTITIONS]`.
    partitions: [dshash_partition; DSHASH_NUM_PARTITIONS],
    /// `int lwlock_tranche_id`.
    lwlock_tranche_id: i32,
    /// `size_t size_log2` — log2(number of buckets).
    size_log2: Size,
    /// `dsa_pointer buckets` — current bucket array.
    buckets: DsaPointer,
}

/// `struct dshash_table` (dshash.c:103) — per-backend state, allocated in
/// backend-local memory. The opaque `*mut DshashTable` the seam contract carries
/// points at one of these.
struct DshashTableState {
    /// `dsa_area *area` — backing dynamic shared memory area.
    area: *mut DsaArea,
    /// `dshash_parameters params`.
    params: DshashParameters,
    /// `dshash_table_control *control` — control object in DSM (a resolved
    /// backend-local address).
    control: *mut dshash_table_control,
    /// `dsa_pointer *buckets` — current bucket pointers in DSM (a resolved
    /// backend-local address to the start of the `dsa_pointer` array).
    buckets: *mut DsaPointer,
    /// `size_t size_log2` — backend-local cache of log2(number of buckets).
    size_log2: Size,
}

/// `dshash_table` (`lib/dshash.h`) — the opaque backend-local handle the public
/// API and the seam contract carry; it points at a [`DshashTableState`].
pub use ::types_storage::DshashTable;

/// `*mut DshashTable` -> `&mut DshashTableState`.
///
/// SAFETY: `t` was produced by [`dshash_create`]/[`dshash_attach`] and is live.
#[inline]
unsafe fn state<'a>(t: *mut DshashTable) -> &'a mut DshashTableState {
    &mut *(t as *mut DshashTableState)
}

// ===========================================================================
// Index-math macros, transcribed as functions.
// ===========================================================================

/// `MAXALIGN(value)` (`MAXIMUM_ALIGNOF == 8` on the supported 64-bit builds).
const fn maxalign(value: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (value + MAXIMUM_ALIGNOF - 1) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MAXALIGN(sizeof(dshash_table_item))` — the offset from an item to its entry.
const fn item_header_size() -> usize {
    maxalign(core::mem::size_of::<dshash_table_item>())
}

/// `NUM_SPLITS(size_log2)` — number of resize operations there have been.
const fn num_splits(size_log2: Size) -> Size {
    size_log2 - DSHASH_NUM_PARTITIONS_LOG2 as Size
}

/// `NUM_BUCKETS(size_log2)`.
const fn num_buckets(size_log2: Size) -> Size {
    1usize << size_log2
}

/// `BUCKETS_PER_PARTITION(size_log2)`.
const fn buckets_per_partition(size_log2: Size) -> Size {
    1usize << num_splits(size_log2)
}

/// `MAX_COUNT_PER_PARTITION(hash_table)` — half + quarter = 75% load factor.
const fn max_count_per_partition(size_log2: Size) -> Size {
    buckets_per_partition(size_log2) / 2 + buckets_per_partition(size_log2) / 4
}

/// `PARTITION_FOR_HASH(hash)` — choose partition from the highest-order bits.
const fn partition_for_hash(hash: dshash_hash) -> Size {
    (hash >> (HASH_BITS - DSHASH_NUM_PARTITIONS_LOG2)) as Size
}

/// `BUCKET_INDEX_FOR_HASH_AND_SIZE(hash, size_log2)`.
const fn bucket_index_for_hash_and_size(hash: dshash_hash, size_log2: Size) -> Size {
    (hash >> (HASH_BITS - size_log2 as u32)) as Size
}

/// `BUCKET_INDEX_FOR_PARTITION(partition, size_log2)`.
const fn bucket_index_for_partition(partition: Size, size_log2: Size) -> Size {
    partition << num_splits(size_log2)
}

/// `PARTITION_FOR_BUCKET_INDEX(bucket_idx, size_log2)`.
const fn partition_for_bucket_index(bucket_idx: Size, size_log2: Size) -> Size {
    bucket_idx >> num_splits(size_log2)
}

// ===========================================================================
// DSA-substrate + lwlock helpers (thin wrappers over the owner seams).
// ===========================================================================

/// `dsa_allocate_extended(area, size, flags)`.
fn dsa_allocate_extended(area: *mut DsaArea, size: Size, flags: i32) -> PgResult<DsaPointer> {
    dsa::dsa_allocate_extended::call(area, size, flags)
}

/// `dsa_allocate(area, size)` == `dsa_allocate_extended(area, size, 0)`.
fn dsa_allocate(area: *mut DsaArea, size: Size) -> PgResult<DsaPointer> {
    dsa::dsa_allocate_extended::call(area, size, 0)
}

/// `dsa_free(area, dp)`.
fn dsa_free(area: *mut DsaArea, dp: DsaPointer) -> PgResult<()> {
    dsa::dsa_free_ptr::call(area, dp)
}

/// `dsa_get_address(area, dp)` — the backend-local address for `dp`.
fn dsa_get_address(area: *mut DsaArea, dp: DsaPointer) -> PgResult<u64> {
    dsa::dsa_get_address_ptr::call(area, dp)
}

/// `DsaPointerIsValid(dp)`.
fn dsa_pointer_is_valid(dp: DsaPointer) -> bool {
    dp != INVALID_DSA_POINTER
}

/// `MyProcNumber` (the C ambient per-backend global the lwlock owner needs).
fn proc_number() -> ProcNumber {
    my_proc_number::call()
}

/// The lock mode for an `exclusive` flag.
fn lock_mode(exclusive: bool) -> LWLockMode {
    if exclusive {
        LW_EXCLUSIVE
    } else {
        LW_SHARED
    }
}

/// `LWLockAcquire(PARTITION_LOCK(hash_table, i), mode)`.
///
/// The lock is held across the return (find/find_or_insert hand it to the
/// caller, the seq scan walks partitions holding one) and released later by
/// recomputing the lock from the control, exactly as C does — so the
/// guard is defused here and [`unlock_partition`] does the release. C's
/// `LWLockReleaseAll` abort backstop is the process-wide one, not a per-call
/// guard.
fn lock_partition(t: &DshashTableState, partition: Size, mode: LWLockMode) -> PgResult<()> {
    let lock = partition_lock(t, partition);
    let guard = lwlock::lwlock_acquire::call(lock, mode, proc_number())?;
    core::mem::forget(guard);
    Ok(())
}

/// `LWLockRelease(PARTITION_LOCK(hash_table, i))`.
fn unlock_partition(t: &DshashTableState, partition: Size) -> PgResult<()> {
    let lock = partition_lock(t, partition);
    lwlock::lwlock_release::call(lock)
}

/// `PARTITION_LOCK(hash_table, i)` — `&hash_table->control->partitions[i].lock`.
fn partition_lock(t: &DshashTableState, partition: Size) -> &'static LWLock {
    // SAFETY: `control` is a resolved backend-local address into the DSA segment
    // (the blessed shared-memory substrate exception); the partition index is in
    // range by construction of every caller. The lock lives for the table's
    // lifetime in shared memory, so the `'static` borrow is sound for the held
    // duration.
    unsafe { &(*t.control).partitions[partition].lock }
}

// ===========================================================================
// Control-object + item field access (through the resolved addresses).
// ===========================================================================

/// `&*hash_table->control`.
#[inline]
fn control(t: &DshashTableState) -> &dshash_table_control {
    // SAFETY: resolved backend-local address; see `partition_lock`.
    unsafe { &*t.control }
}

/// `&mut *hash_table->control`.
#[inline]
#[allow(clippy::mut_from_ref)]
fn control_mut(t: &DshashTableState) -> &mut dshash_table_control {
    // SAFETY: resolved backend-local address held under the partition lock(s)
    // the caller is required to hold for any write.
    unsafe { &mut *t.control }
}

/// `dsa_get_address` of an item pointer, as `&dshash_table_item`.
#[inline]
fn item_at(area: *mut DsaArea, item_pointer: DsaPointer) -> PgResult<*mut dshash_table_item> {
    Ok(dsa_get_address(area, item_pointer)? as usize as *mut dshash_table_item)
}

/// `ENTRY_FROM_ITEM(item)` — the entry's backend-local address.
#[inline]
fn entry_from_item(item: *mut dshash_table_item) -> *mut u8 {
    (item as usize + item_header_size()) as *mut u8
}

/// `ITEM_FROM_ENTRY(entry)` — the item address holding `entry`.
#[inline]
fn item_from_entry(entry: *mut u8) -> *mut dshash_table_item {
    (entry as usize - item_header_size()) as *mut dshash_table_item
}

// ===========================================================================
// dshash_create / attach / detach / destroy.
// ===========================================================================

/// `dshash_create` — create a new hash table backed by the given DSA area.
pub fn dshash_create(
    area: *mut DsaArea,
    params: &DshashParameters,
) -> PgResult<*mut DshashTable> {
    // Allocate the backend-local object representing the hash table.
    let mut hash_table = Box::new(DshashTableState {
        area,
        params: *params,
        control: core::ptr::null_mut(),
        buckets: core::ptr::null_mut(),
        size_log2: 0,
    });

    // Allocate the control object in shared memory.
    let control_dp = dsa_allocate(area, core::mem::size_of::<dshash_table_control>())?;

    // Set up the local and shared hash table structs.
    hash_table.control = dsa_get_address(area, control_dp)? as usize as *mut dshash_table_control;
    {
        let c = control_mut(&hash_table);
        c.handle = control_dp;
        c.magic = DSHASH_MAGIC;
        c.lwlock_tranche_id = params.tranche_id;
    }

    // Set up the array of lock partitions.
    {
        let tranche_id = control(&hash_table).lwlock_tranche_id;
        for i in 0..DSHASH_NUM_PARTITIONS {
            let c = control_mut(&hash_table);
            lwlock::lwlock_initialize::call(&mut c.partitions[i].lock, tranche_id);
            c.partitions[i].count = 0;
        }
    }

    // Set up the initial array of buckets. Our initial size is the same as the
    // number of partitions.
    control_mut(&hash_table).size_log2 = DSHASH_NUM_PARTITIONS_LOG2 as Size;
    let buckets_dp = dsa_allocate_extended(
        area,
        core::mem::size_of::<DsaPointer>() * DSHASH_NUM_PARTITIONS,
        DSA_ALLOC_NO_OOM | DSA_ALLOC_ZERO,
    )?;
    if !dsa_pointer_is_valid(buckets_dp) {
        dsa_free(area, control_dp)?;
        return Err(PgError::error("out of memory")
            .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
            .with_detail(format!(
                "Failed on DSA request of size {}.",
                core::mem::size_of::<DsaPointer>() * DSHASH_NUM_PARTITIONS
            )));
    }
    control_mut(&hash_table).buckets = buckets_dp;
    hash_table.buckets = dsa_get_address(area, buckets_dp)? as usize as *mut DsaPointer;
    hash_table.size_log2 = control(&hash_table).size_log2;

    Ok(Box::into_raw(hash_table) as *mut DshashTable)
}

/// `dshash_attach` — attach to an existing hash table using a handle.
pub fn dshash_attach(
    area: *mut DsaArea,
    params: &DshashParameters,
    handle: dshash_table_handle,
) -> PgResult<*mut DshashTable> {
    // Find the control object in shared memory.
    let control = handle;

    let mut hash_table = Box::new(DshashTableState {
        area,
        params: *params,
        control: dsa_get_address(area, control)? as usize as *mut dshash_table_control,
        // These will later be set by ensure_valid_bucket_pointers(), under a
        // partition lock interlocking against concurrent resizing.
        buckets: core::ptr::null_mut(),
        size_log2: 0,
    });
    debug_assert_eq!(control_magic(&hash_table), DSHASH_MAGIC);

    let _ = &mut hash_table; // buckets/size_log2 already NULL/0.
    Ok(Box::into_raw(hash_table) as *mut DshashTable)
}

/// `hash_table->control->magic`.
#[inline]
fn control_magic(t: &DshashTableState) -> u32 {
    control(t).magic
}

/// `dshash_detach` — free backend-local resources. The table continues to exist
/// until explicitly destroyed.
pub fn dshash_detach(hash_table: *mut DshashTable) {
    // The hash table may have been destroyed. Just free local memory
    // (`pfree(hash_table)`).
    // SAFETY: `hash_table` was produced by `Box::into_raw` in create/attach.
    drop(unsafe { Box::from_raw(hash_table as *mut DshashTableState) });
}

/// `dshash_destroy` — destroy a hash table, returning all memory to the area.
/// The caller must be certain no other backend will access it.
pub fn dshash_destroy(hash_table: *mut DshashTable) -> PgResult<()> {
    // SAFETY: `hash_table` was produced by `Box::into_raw`.
    let mut boxed = unsafe { Box::from_raw(hash_table as *mut DshashTableState) };
    let t = &mut *boxed;

    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);
    ensure_valid_bucket_pointers(t)?;

    // Free all the entries.
    let size = num_buckets(t.size_log2);
    let area = t.area;
    for i in 0..size {
        let mut item_pointer = bucket_get(t, i);
        while dsa_pointer_is_valid(item_pointer) {
            let item = item_at(area, item_pointer)?;
            // SAFETY: `item` is a resolved address in the DSA segment.
            let next_item_pointer = unsafe { (*item).next };
            dsa_free(area, item_pointer)?;
            item_pointer = next_item_pointer;
        }
    }

    // Vandalize the control block to catch use-after-destroy.
    control_mut(t).magic = 0;

    // Free the active table and control object.
    let control_buckets = control(t).buckets;
    let control_handle = control(t).handle;
    dsa_free(area, control_buckets)?;
    dsa_free(area, control_handle)?;
    // pfree(hash_table) — drop the backend-local object.
    drop(boxed);
    Ok(())
}

/// `dshash_get_hash_table_handle` — a handle other processes can use to attach.
pub fn dshash_get_hash_table_handle(hash_table: *mut DshashTable) -> dshash_table_handle {
    // SAFETY: `hash_table` is a live table from create/attach.
    let t = unsafe { state(hash_table) };
    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);
    control(t).handle
}

// ===========================================================================
// dshash_find / find_or_insert / delete_key / delete_entry / release_lock.
// ===========================================================================

/// `dshash_find` — look up an entry. Returns the entry pointer (locked) if
/// found, else `None`. The lock mode is shared or exclusive per `exclusive`.
pub fn dshash_find(
    hash_table: *mut DshashTable,
    key: &[u8],
    exclusive: bool,
) -> PgResult<Option<*mut u8>> {
    // SAFETY: live table from create/attach.
    let t = unsafe { state(hash_table) };

    let hash = hash_key(t, key);
    let partition = partition_for_hash(hash);

    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);

    lock_partition(t, partition, lock_mode(exclusive))?;
    ensure_valid_bucket_pointers(t)?;

    // Search the active bucket.
    let bucket_head = bucket_for_hash(t, hash);
    let item = find_in_bucket(t, key, bucket_head)?;

    match item {
        None => {
            // Not found.
            unlock_partition(t, partition)?;
            Ok(None)
        }
        Some(item) => {
            // The caller will free the lock by calling dshash_release_lock.
            Ok(Some(entry_from_item(item)))
        }
    }
}

/// `dshash_find_or_insert` — returns an exclusively locked entry. `found` is set
/// to whether the key already existed; on absence a zeroed entry with the key
/// copied in is created.
pub fn dshash_find_or_insert(
    hash_table: *mut DshashTable,
    key: &[u8],
    found: &mut bool,
) -> PgResult<*mut u8> {
    // SAFETY: live table from create/attach.
    let t = unsafe { state(hash_table) };

    let hash = hash_key(t, key);
    let partition_index = partition_for_hash(hash);

    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);

    // restart:
    loop {
        lock_partition(t, partition_index, LW_EXCLUSIVE)?;
        ensure_valid_bucket_pointers(t)?;

        // Search the active bucket.
        let bucket_head = bucket_for_hash(t, hash);
        if let Some(item) = find_in_bucket(t, key, bucket_head)? {
            *found = true;
            return Ok(entry_from_item(item));
        }

        *found = false;

        // Check if we are getting too full.
        let count = control(t).partitions[partition_index].count;
        if count > max_count_per_partition(t.size_log2) {
            // Load factor > 0.75. Give up our lock first, because resizing
            // reacquires all locks in order to avoid deadlocks.
            unlock_partition(t, partition_index)?;
            resize(t, t.size_log2 + 1)?;
            continue; // goto restart
        }

        // Finally we can try to insert the new item.
        let bucket_index = bucket_index_for_hash_and_size(hash, t.size_log2);
        let item = insert_into_bucket(t, key, bucket_index)?;
        // SAFETY: `item` is a resolved address in the DSA segment.
        unsafe { (*item).hash = hash };
        // Adjust per-lock-partition counter for load factor knowledge.
        control_mut(t).partitions[partition_index].count += 1;

        return Ok(entry_from_item(item));
    }
}

/// `dshash_delete_key` — remove an entry by key. Returns true if removed.
pub fn dshash_delete_key(hash_table: *mut DshashTable, key: &[u8]) -> PgResult<bool> {
    // SAFETY: live table from create/attach.
    let t = unsafe { state(hash_table) };

    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);

    let hash = hash_key(t, key);
    let partition = partition_for_hash(hash);

    lock_partition(t, partition, LW_EXCLUSIVE)?;
    ensure_valid_bucket_pointers(t)?;

    let bucket_index = bucket_index_for_hash_and_size(hash, t.size_log2);
    let found = if delete_key_from_bucket(t, key, BucketSlot::Array(bucket_index))? {
        debug_assert!(control(t).partitions[partition].count > 0);
        control_mut(t).partitions[partition].count -= 1;
        true
    } else {
        false
    };

    unlock_partition(t, partition)?;
    Ok(found)
}

/// `dshash_delete_entry` — remove an already-exclusively-locked entry (obtained
/// via find / find_or_insert). Releases the lock like `dshash_release_lock`.
pub fn dshash_delete_entry(hash_table: *mut DshashTable, entry: *mut u8) -> PgResult<()> {
    // SAFETY: live table from create/attach.
    let t = unsafe { state(hash_table) };

    let item = item_from_entry(entry);
    // SAFETY: `item` is a resolved address in the DSA segment.
    let hash = unsafe { (*item).hash };
    let partition = partition_for_hash(hash);

    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);

    delete_item(t, item)?;
    unlock_partition(t, partition)
}

/// `dshash_release_lock` — unlock an entry locked by find / find_or_insert.
pub fn dshash_release_lock(hash_table: *mut DshashTable, entry: *mut u8) -> PgResult<()> {
    // SAFETY: live table from create/attach.
    let t = unsafe { state(hash_table) };

    let item = item_from_entry(entry);
    // SAFETY: `item` is a resolved address in the DSA segment.
    let hash = unsafe { (*item).hash };
    let partition_index = partition_for_hash(hash);

    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);

    unlock_partition(t, partition_index)
}

// ===========================================================================
// Convenience hash, compare, and copy functions.
// ===========================================================================

/// `dshash_memcmp` — a compare function that forwards to `memcmp`.
pub fn dshash_memcmp(a: &[u8], b: &[u8], size: Size) -> i32 {
    memcmp(a, b, size)
}

/// `dshash_memhash` — a hash function that forwards to `tag_hash`.
pub fn dshash_memhash(v: &[u8], size: Size) -> dshash_hash {
    hashfn::tag_hash::call(v, size)
}

/// `dshash_memcpy` — a copy function that forwards to `memcpy`.
pub fn dshash_memcpy(dest: &mut [u8], src: &[u8], size: Size) {
    dest[..size].copy_from_slice(&src[..size]);
}

/// `dshash_strcmp` — a compare function that forwards to `strcmp`.
pub fn dshash_strcmp(a: &[u8], b: &[u8], size: Size) -> i32 {
    debug_assert!(cstr_len(a) < size);
    debug_assert!(cstr_len(b) < size);
    strcmp(a, b)
}

/// `dshash_strhash` — a hash function that forwards to `string_hash`.
pub fn dshash_strhash(v: &[u8], size: Size) -> dshash_hash {
    debug_assert!(cstr_len(v) < size);
    hashfn::string_hash::call(v, size)
}

/// `dshash_strcpy` — a copy function that forwards to `strcpy`.
pub fn dshash_strcpy(dest: &mut [u8], src: &[u8], size: Size) {
    debug_assert!(cstr_len(src) < size);
    let len = cstr_len(src) + 1; // include the NUL terminator
    dest[..len].copy_from_slice(&src[..len]);
}

/// `memcmp(a, b, n)`, returning C-style sign over the first `n` bytes.
fn memcmp(a: &[u8], b: &[u8], n: usize) -> i32 {
    for i in 0..n {
        if a[i] != b[i] {
            return a[i] as i32 - b[i] as i32;
        }
    }
    0
}

/// `strcmp(a, b)` over NUL-terminated byte buffers.
fn strcmp(a: &[u8], b: &[u8]) -> i32 {
    let mut i = 0;
    loop {
        let ca = a[i];
        let cb = b[i];
        if ca != cb {
            return ca as i32 - cb as i32;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

/// `strlen` of a NUL-terminated byte buffer (capped at the slice length).
fn cstr_len(s: &[u8]) -> usize {
    s.iter().position(|&b| b == 0).unwrap_or(s.len())
}

// ===========================================================================
// Sequential scan (dshash_seq_*).
// ===========================================================================

/// `struct dshash_seq_status` (dshash.h) — sequential scan state.
pub struct DshashSeqStatus {
    /// `dshash_table *hash_table`.
    hash_table: *mut DshashTable,
    /// `int curbucket` — bucket number we are at.
    curbucket: i32,
    /// `int nbuckets` — total number of buckets in the dshash.
    nbuckets: i32,
    /// `dshash_table_item *curitem` — item we are currently at (a resolved
    /// address; null == none).
    curitem: *mut dshash_table_item,
    /// `dsa_pointer pnextitem` — dsa-pointer to the next item.
    pnextitem: DsaPointer,
    /// `int curpartition` — partition number we are at.
    curpartition: i32,
    /// `bool exclusive` — locking mode.
    exclusive: bool,
}

/// `dshash_seq_init` — initialize a sequential scan. Returned elements may be
/// deleted mid-scan with [`dshash_delete_current`] if `exclusive`.
pub fn dshash_seq_init(hash_table: *mut DshashTable, exclusive: bool) -> DshashSeqStatus {
    DshashSeqStatus {
        hash_table,
        curbucket: 0,
        nbuckets: 0,
        curitem: core::ptr::null_mut(),
        pnextitem: INVALID_DSA_POINTER,
        curpartition: -1,
        exclusive,
    }
}

/// `dshash_seq_next` — return the next element (locked), or `None` when all have
/// been returned. Locks are released by the next `dshash_seq_next` / `_term`.
pub fn dshash_seq_next(status: &mut DshashSeqStatus) -> PgResult<Option<*mut u8>> {
    // SAFETY: live table from create/attach.
    let t = unsafe { state(status.hash_table) };

    let mut next_item_pointer;
    if status.curpartition == -1 {
        debug_assert_eq!(status.curbucket, 0);

        status.curpartition = 0;
        lock_partition(t, status.curpartition as Size, lock_mode(status.exclusive))?;
        ensure_valid_bucket_pointers(t)?;

        status.nbuckets = num_buckets(control(t).size_log2) as i32;
        next_item_pointer = bucket_get(t, status.curbucket as Size);
    } else {
        next_item_pointer = status.pnextitem;
    }

    // Move to the next bucket if we finished the current bucket.
    while !dsa_pointer_is_valid(next_item_pointer) {
        status.curbucket += 1;
        if status.curbucket >= status.nbuckets {
            // all buckets have been scanned. finish.
            return Ok(None);
        }

        // Check if move to the next partition.
        let next_partition =
            partition_for_bucket_index(status.curbucket as Size, t.size_log2) as i32;
        if status.curpartition != next_partition {
            // Lock the next partition then release the current (not the reverse)
            // to avoid concurrent resizing; same lock order as resize().
            lock_partition(t, next_partition as Size, lock_mode(status.exclusive))?;
            unlock_partition(t, status.curpartition as Size)?;
            status.curpartition = next_partition;
        }

        next_item_pointer = bucket_get(t, status.curbucket as Size);
    }

    status.curitem = item_at(t.area, next_item_pointer)?;

    // The caller may delete the item. Store the next item in case of deletion.
    // SAFETY: `curitem` is a resolved address in the DSA segment.
    status.pnextitem = unsafe { (*status.curitem).next };

    Ok(Some(entry_from_item(status.curitem)))
}

/// `dshash_seq_term` — terminate the scan and release all locks.
pub fn dshash_seq_term(status: &mut DshashSeqStatus) -> PgResult<()> {
    if status.curpartition >= 0 {
        // SAFETY: live table from create/attach.
        let t = unsafe { state(status.hash_table) };
        unlock_partition(t, status.curpartition as Size)?;
    }
    Ok(())
}

/// `dshash_delete_current` — remove the current entry of the seq scan.
pub fn dshash_delete_current(status: &mut DshashSeqStatus) -> PgResult<()> {
    // SAFETY: live table from create/attach.
    let t = unsafe { state(status.hash_table) };
    let item = status.curitem;

    debug_assert!(status.exclusive);
    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);

    delete_item(t, item)
}

// ===========================================================================
// dshash_dump.
// ===========================================================================

/// `dshash_dump` — render debugging information about the hash table. The C
/// version `fprintf`s to stderr; here we build the same text and return it. The
/// caller must hold no partition locks; acquires all partition locks in shared
/// mode, faithfully to C.
pub fn dshash_dump(hash_table: *mut DshashTable) -> PgResult<alloc::string::String> {
    use core::fmt::Write;

    // SAFETY: live table from create/attach.
    let t = unsafe { state(hash_table) };

    debug_assert_eq!(control_magic(t), DSHASH_MAGIC);

    for i in 0..DSHASH_NUM_PARTITIONS {
        lock_partition(t, i, LW_SHARED)?;
    }

    ensure_valid_bucket_pointers(t)?;

    let mut out = alloc::string::String::new();
    let _ = writeln!(out, "hash table size = {}", 1usize << t.size_log2);
    for i in 0..DSHASH_NUM_PARTITIONS {
        let count = control(t).partitions[i].count;
        let begin = bucket_index_for_partition(i, t.size_log2);
        let end = bucket_index_for_partition(i + 1, t.size_log2);

        let _ = writeln!(out, "  partition {i}");
        let _ = writeln!(out, "    active buckets (key count = {count})");

        for j in begin..end {
            let mut count = 0usize;
            let mut bucket = bucket_get(t, j);
            while dsa_pointer_is_valid(bucket) {
                let item = item_at(t.area, bucket)?;
                // SAFETY: resolved address in the DSA segment.
                bucket = unsafe { (*item).next };
                count += 1;
            }
            let _ = writeln!(out, "      bucket {j} (key count = {count})");
        }
    }

    for i in 0..DSHASH_NUM_PARTITIONS {
        unlock_partition(t, i)?;
    }

    Ok(out)
}

// ===========================================================================
// File-private helpers.
// ===========================================================================

/// `delete_item` — delete a locked item to which we have a pointer.
fn delete_item(t: &mut DshashTableState, item: *mut dshash_table_item) -> PgResult<()> {
    // SAFETY: resolved address in the DSA segment.
    let hash = unsafe { (*item).hash };
    let partition = partition_for_hash(hash);

    let bucket_index = bucket_index_for_hash_and_size(hash, t.size_log2);
    if delete_item_from_bucket(t, item, BucketSlot::Array(bucket_index))? {
        debug_assert!(control(t).partitions[partition].count > 0);
        control_mut(t).partitions[partition].count -= 1;
    } else {
        debug_assert!(false, "delete_item: item not found in its bucket");
    }
    Ok(())
}

/// `resize` — grow the hash table to the requested number of buckets (double a
/// previously observed size). Must be called without any partition lock held.
fn resize(t: &mut DshashTableState, new_size_log2: Size) -> PgResult<()> {
    let new_size = 1usize << new_size_log2;
    let area = t.area;

    // Acquire the locks for all lock partitions. Expensive, but rare.
    for i in 0..DSHASH_NUM_PARTITIONS {
        lock_partition(t, i, LW_EXCLUSIVE)?;
        if i == 0 && control(t).size_log2 >= new_size_log2 {
            // Another backend already increased the size; return early.
            unlock_partition(t, 0)?;
            return Ok(());
        }
    }

    debug_assert_eq!(new_size_log2, control(t).size_log2 + 1);

    // Allocate the space for the new table.
    let new_buckets_shared = dsa_allocate_extended(
        area,
        core::mem::size_of::<DsaPointer>() * new_size,
        DSA_ALLOC_HUGE | DSA_ALLOC_ZERO,
    )?;
    let new_buckets = dsa_get_address(area, new_buckets_shared)? as usize as *mut DsaPointer;

    // Reinsert all items into the new bucket array.
    let size = 1usize << control(t).size_log2;
    for i in 0..size {
        let mut item_pointer = bucket_get(t, i);
        while dsa_pointer_is_valid(item_pointer) {
            let item = item_at(area, item_pointer)?;
            // SAFETY: resolved address in the DSA segment.
            let (next_item_pointer, hash) = unsafe { ((*item).next, (*item).hash) };
            let new_index = bucket_index_for_hash_and_size(hash, new_size_log2);
            // SAFETY: `new_buckets` is a resolved array of `new_size` slots.
            let slot = unsafe { new_buckets.add(new_index) };
            insert_item_into_bucket(item_pointer, item, slot);
            item_pointer = next_item_pointer;
        }
    }

    // Swap the hash table into place and free the old one.
    let old_buckets = control(t).buckets;
    control_mut(t).buckets = new_buckets_shared;
    control_mut(t).size_log2 = new_size_log2;
    t.buckets = new_buckets;
    t.size_log2 = new_size_log2;
    dsa_free(area, old_buckets)?;

    // Release all the locks.
    for i in 0..DSHASH_NUM_PARTITIONS {
        unlock_partition(t, i)?;
    }
    Ok(())
}

/// `ensure_valid_bucket_pointers` — refresh backend-local bucket pointers. The
/// caller must hold one partition lock (prevents a concurrent `resize`).
fn ensure_valid_bucket_pointers(t: &mut DshashTableState) -> PgResult<()> {
    let control_size_log2 = control(t).size_log2;
    if t.size_log2 != control_size_log2 {
        let buckets_dp = control(t).buckets;
        t.buckets = dsa_get_address(t.area, buckets_dp)? as usize as *mut DsaPointer;
        t.size_log2 = control_size_log2;
    }
    Ok(())
}

/// `find_in_bucket` — scan a locked bucket for a match. `item_pointer` is the
/// head of the bucket; returns the matching item's address, or `None`.
fn find_in_bucket(
    t: &DshashTableState,
    key: &[u8],
    mut item_pointer: DsaPointer,
) -> PgResult<Option<*mut dshash_table_item>> {
    let area = t.area;
    while dsa_pointer_is_valid(item_pointer) {
        let item = item_at(area, item_pointer)?;
        if equal_keys(t, key, entry_from_item(item)) {
            return Ok(Some(item));
        }
        // SAFETY: resolved address in the DSA segment.
        item_pointer = unsafe { (*item).next };
    }
    Ok(None)
}

/// `insert_item_into_bucket` — prepend an already-allocated item into a bucket.
/// `bucket` is the slot (`dsa_pointer *`) to prepend at.
fn insert_item_into_bucket(
    item_pointer: DsaPointer,
    item: *mut dshash_table_item,
    bucket: *mut DsaPointer,
) {
    // SAFETY: `item` and `bucket` are resolved addresses in the DSA segment.
    unsafe {
        (*item).next = *bucket;
        *bucket = item_pointer;
    }
}

/// `insert_into_bucket` — allocate an entry with the given key and insert it
/// into the active bucket at `bucket_index`. Returns the new item's address.
fn insert_into_bucket(
    t: &DshashTableState,
    key: &[u8],
    bucket_index: Size,
) -> PgResult<*mut dshash_table_item> {
    let area = t.area;
    let item_pointer = dsa_allocate(area, t.params.entry_size + item_header_size())?;
    let item = item_at(area, item_pointer)?;
    copy_key(t, entry_from_item(item), key);
    // SAFETY: `bucket_index` is in range of the active bucket array.
    let slot = unsafe { t.buckets.add(bucket_index) };
    insert_item_into_bucket(item_pointer, item, slot);
    Ok(item)
}

/// Models the C `dsa_pointer *bucket_head` lvalue: either the active bucket-array
/// slot at `index`, or the `next` field of a list item.
enum BucketSlot {
    /// `&hash_table->buckets[index]`.
    Array(Size),
    /// `&item->next`.
    ItemNext(*mut dshash_table_item),
}

impl BucketSlot {
    /// `*bucket_head`.
    fn read(&self, t: &DshashTableState) -> DsaPointer {
        // SAFETY: resolved addresses in the DSA segment / active bucket array.
        unsafe {
            match *self {
                BucketSlot::Array(index) => *t.buckets.add(index),
                BucketSlot::ItemNext(item) => (*item).next,
            }
        }
    }

    /// `*bucket_head = value`.
    fn write(&self, t: &DshashTableState, value: DsaPointer) {
        // SAFETY: resolved addresses in the DSA segment / active bucket array.
        unsafe {
            match *self {
                BucketSlot::Array(index) => *t.buckets.add(index) = value,
                BucketSlot::ItemNext(item) => (*item).next = value,
            }
        }
    }
}

/// `delete_key_from_bucket` — search a bucket for a matching key and delete it.
fn delete_key_from_bucket(
    t: &DshashTableState,
    key: &[u8],
    mut bucket_head: BucketSlot,
) -> PgResult<bool> {
    let area = t.area;
    loop {
        let cur = bucket_head.read(t);
        if !dsa_pointer_is_valid(cur) {
            return Ok(false);
        }
        let item = item_at(area, cur)?;
        if equal_keys(t, key, entry_from_item(item)) {
            // SAFETY: resolved address in the DSA segment.
            let next = unsafe { (*item).next };
            dsa_free(area, cur)?;
            bucket_head.write(t, next);
            return Ok(true);
        }
        bucket_head = BucketSlot::ItemNext(item);
    }
}

/// `delete_item_from_bucket` — delete the specified item from its bucket.
fn delete_item_from_bucket(
    t: &DshashTableState,
    item: *mut dshash_table_item,
    mut bucket_head: BucketSlot,
) -> PgResult<bool> {
    let area = t.area;
    loop {
        let bucket_item_ptr = bucket_head.read(t);
        if !dsa_pointer_is_valid(bucket_item_ptr) {
            return Ok(false);
        }
        let bucket_item = item_at(area, bucket_item_ptr)?;
        if bucket_item == item {
            // SAFETY: resolved address in the DSA segment.
            let next = unsafe { (*item).next };
            dsa_free(area, bucket_item_ptr)?;
            bucket_head.write(t, next);
            return Ok(true);
        }
        bucket_head = BucketSlot::ItemNext(bucket_item);
    }
}

/// `hash_key` — compute the hash value for a key, dispatching on the key kind.
fn hash_key(t: &DshashTableState, key: &[u8]) -> dshash_hash {
    match t.params.key_kind {
        DshashKeyKind::String => dshash_strhash(key, t.params.key_size),
        DshashKeyKind::Binary => dshash_memhash(key, t.params.key_size),
        // The record table's `arg` is the table's DSA area (typcache.c
        // `srtr_record_table_params` passes `area`); the owner resolves the
        // `SharedRecordTableKey` to a TupleDesc and runs `hashRowType`.
        DshashKeyKind::Record => {
            typcache_seams::shared_record_key_hash::call(t.area, key)
        }
    }
}

/// `equal_keys` — check whether two keys compare equal. `a` is the lookup key;
/// `b` is the entry address (`ENTRY_FROM_ITEM`), of which the leading `key_size`
/// bytes are read.
fn equal_keys(t: &DshashTableState, a: &[u8], b: *mut u8) -> bool {
    let key_size = t.params.key_size;
    // SAFETY: `b` is a resolved entry address with at least `key_size` bytes.
    let b = unsafe { core::slice::from_raw_parts(b, key_size) };
    match t.params.key_kind {
        DshashKeyKind::String => dshash_strcmp(a, b, key_size) == 0,
        DshashKeyKind::Binary => dshash_memcmp(a, b, key_size) == 0,
        // `arg = area`; the owner resolves each `SharedRecordTableKey` to a
        // TupleDesc and runs `equalRowTypes`. C returns 0 on equal, so we
        // compare the owner's bool directly.
        DshashKeyKind::Record => {
            typcache_seams::shared_record_key_compare::call(t.area, a, b)
        }
    }
}

/// `copy_key` — copy a key into the freshly allocated entry at `dest`
/// (`ENTRY_FROM_ITEM`).
fn copy_key(t: &DshashTableState, dest: *mut u8, src: &[u8]) {
    let key_size = t.params.key_size;
    // SAFETY: `dest` is a freshly allocated entry address with `entry_size`
    // (>= `key_size`) bytes.
    let dest = unsafe { core::slice::from_raw_parts_mut(dest, key_size) };
    match t.params.key_kind {
        DshashKeyKind::String => dshash_strcpy(dest, src, key_size),
        DshashKeyKind::Binary => dshash_memcpy(dest, src, key_size),
        // The record table's copy_function is `dshash_memcpy` (typcache.c
        // `srtr_record_table_params`): the `SharedRecordTableKey` bytes are
        // copied verbatim into the new entry.
        DshashKeyKind::Record => dshash_memcpy(dest, src, key_size),
    }
}

/// `BUCKET_FOR_HASH(hash_table, hash)` — head of the active bucket for `hash`.
fn bucket_for_hash(t: &DshashTableState, hash: dshash_hash) -> DsaPointer {
    let index = bucket_index_for_hash_and_size(hash, t.size_log2);
    bucket_get(t, index)
}

/// `hash_table->buckets[index]` — read a slot of the active bucket array.
fn bucket_get(t: &DshashTableState, index: Size) -> DsaPointer {
    // SAFETY: `index` is in range of the active bucket array, a resolved address.
    unsafe { *t.buckets.add(index) }
}

// ===========================================================================
// Seam wiring.
// ===========================================================================

/// Install every seam in `backend-lib-dshash-seams`.
pub fn init_seams() {
    use dshash_seams as seam;

    seam::dshash_create::set(|area, params| dshash_create(area, &params));
    seam::dshash_attach::set(|area, params, handle| dshash_attach(area, &params, handle));
    seam::dshash_get_hash_table_handle::set(dshash_get_hash_table_handle);
    seam::dshash_find_or_insert::set(|t, key| {
        let mut found = false;
        let entry = dshash_find_or_insert(t, key, &mut found)?;
        Ok(DshashEntryGuard::new(t, entry, found))
    });
    seam::dshash_find::set(|t, key, exclusive| {
        Ok(dshash_find(t, key, exclusive)?.map(|entry| DshashEntryGuard::new(t, entry, true)))
    });
    seam::dshash_delete_key::set(dshash_delete_key);
    seam::dshash_release_lock::set(|t, entry| {
        // The C `dshash_release_lock` cannot fail in the no-error path; the
        // guard's release/drop is the call site. Surface any release error as a
        // panic at this infallible seam boundary.
        if let Err(e) = dshash_release_lock(t, entry) {
            panic!("dshash_release_lock: {}", e.message());
        }
    });
}

#[cfg(test)]
mod tests;
