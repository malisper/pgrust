use dynahash::{get_hash_value, hash_create, hash_search_with_hash_value};
use lwlock::{main_lock, LWLock, BUFFER_MAPPING_LWLOCK_OFFSET, NUM_BUFFER_PARTITIONS};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_hash::hsearch::{
    HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_ENTER, HASH_FIND, HASH_FIXED_SIZE, HASH_PARTITION,
    HASH_REMOVE, HASH_SHARED_MEM, HTAB,
};
use types_storage::buf::buftag;

#[repr(C)]
struct BufferLookupEnt {
    key: buftag,
    id: i32,
}

// SAFETY(Sync): dynahash partitioned tables serialize bucket access via the
// caller's partition LWLock and internal freelist spinlocks (C's shared-HTAB
// contract). Published once at startup, then plain loads (C global).
static SHARED_BUF_HASH: core::sync::atomic::AtomicPtr<HTAB> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

#[inline]
fn htab() -> *mut HTAB {
    let h = SHARED_BUF_HASH.load(core::sync::atomic::Ordering::Relaxed);
    if h.is_null() {
        htab_uninit();
    }
    h
}

#[cold]
#[inline(never)]
fn htab_uninit() -> ! {
    panic!("bufmgr: InitBufTable (buf_table.c) not called")
}

/// InitBufTable (buf_table.c): size = NBuffers + NUM_BUFFER_PARTITIONS.
pub fn InitBufTable(size: i32) -> PgResult<()> {
    let base = main_lock(BUFFER_MAPPING_LWLOCK_OFFSET as usize) as *const LWLock
        as *mut lwlock::LWLockPadded;
    PARTITION_BASE.store(base, core::sync::atomic::Ordering::Release);
    let info = HASHCTL {
        num_partitions: NUM_BUFFER_PARTITIONS as i64,
        ssize: 0,
        dsize: 0,
        max_dsize: 0,
        keysize: core::mem::size_of::<buftag>(),
        entrysize: core::mem::size_of::<BufferLookupEnt>(),
        hash: None,
        match_: None,
        keycopy: None,
        alloc: None,
        hcxt: core::ptr::null_mut(),
        hctl: core::ptr::null_mut(),
    };
    // C grows shared-table elements from shmem; FIXED_SIZE preallocates the
    // same capacity so no thread allocates through the table context.
    let h = hash_create(
        "Shared Buffer Lookup Table",
        size as i64,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_SHARED_MEM | HASH_FIXED_SIZE,
    )?;
    assert!(
        SHARED_BUF_HASH
            .compare_exchange(
                core::ptr::null_mut(),
                h,
                core::sync::atomic::Ordering::Release,
                core::sync::atomic::Ordering::Relaxed
            )
            .is_ok(),
        "bufmgr: buffer lookup table initialized twice"
    );
    Ok(())
}

#[inline]
pub fn BufTableHashCode(tag: &buftag) -> u32 {
    get_hash_value(htab(), tag as *const buftag as *const u8)
}

// M2 swizzling decision site: this partition-locked hash probe (plus the pin
// CAS behind it) is exactly what pointer swizzling replaces on warm hits —
// parent-held swizzled child pointers validated by version, zero atomics
// (docs/beat-postgres.md §7, docs/strategy.md lever 8).
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

/// Crash-cycle reset: empty the table in place, no partition locks (exclusive
/// postmaster access); deleting the returned entry mid-scan is dynahash-legal.
pub(crate) fn BufTableResetAfterCrash() {
    use dynahash::{hash_seq_init, hash_seq_search};
    use types_hash::hsearch::HASH_SEQ_STATUS;

    let h = htab();
    let mut status = HASH_SEQ_STATUS::new();
    hash_seq_init(&mut status, h).expect("BufTableResetAfterCrash: hash_seq_init");
    loop {
        let entry = hash_seq_search(&mut status).expect("BufTableResetAfterCrash: hash_seq_search");
        if entry.is_null() {
            break;
        }
        // SAFETY: live BufferLookupEnt returned by the scan.
        let tag = unsafe { (*(entry as *const BufferLookupEnt)).key };
        let hashcode = BufTableHashCode(&tag);
        BufTableDelete(&tag, hashcode).expect("BufTableResetAfterCrash: delete");
    }
}

/// Caller holds the partition lock (shared or better).
pub fn BufTableLookup(tag: &buftag, hashcode: u32) -> PgResult<i32> {
    let entry = hash_search_with_hash_value(
        htab(),
        tag as *const buftag as *const u8,
        hashcode,
        HASH_FIND,
        None,
    )?;
    if entry.is_null() {
        return Ok(-1);
    }
    // SAFETY: dynahash returned a live BufferLookupEnt for this key.
    Ok(unsafe { (*(entry as *const BufferLookupEnt)).id })
}

/// -1 on success, existing id on collision; partition lock held exclusively.
pub fn BufTableInsert(tag: &buftag, hashcode: u32, buf_id: i32) -> PgResult<i32> {
    debug_assert!(buf_id >= 0);
    debug_assert!(tag.blockNum != types_core::InvalidBlockNumber);
    let mut found = false;
    let entry = hash_search_with_hash_value(
        htab(),
        tag as *const buftag as *const u8,
        hashcode,
        HASH_ENTER,
        Some(&mut found),
    )?;
    let ent = entry as *mut BufferLookupEnt;
    if found {
        // SAFETY: live entry (dynahash contract).
        return Ok(unsafe { (*ent).id });
    }
    // SAFETY: fresh entry, key already copied in; we own the payload until the
    // partition lock drops.
    unsafe { (*ent).id = buf_id };
    Ok(-1)
}

/// Caller holds the partition lock exclusively.
pub fn BufTableDelete(tag: &buftag, hashcode: u32) -> PgResult<()> {
    let entry = hash_search_with_hash_value(
        htab(),
        tag as *const buftag as *const u8,
        hashcode,
        HASH_REMOVE,
        None,
    )?;
    if entry.is_null() {
        return Err(Box::new(
            types_error::PgError::new(ERROR, "shared buffer hash table corrupted")
                .with_error_location(ErrorLocation::new("buf_table.c", 0, "BufTableDelete")),
        ));
    }
    Ok(())
}
