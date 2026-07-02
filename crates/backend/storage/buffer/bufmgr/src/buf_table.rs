use std::sync::OnceLock;

use dynahash::{get_hash_value, hash_create, hash_search_with_hash_value, HTAB};
use lwlock::{main_lock, LWLock, BUFFER_MAPPING_LWLOCK_OFFSET, NUM_BUFFER_PARTITIONS};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_hash::hsearch::{
    HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_ENTER, HASH_FIND, HASH_PARTITION, HASH_REMOVE,
};
use types_storage::buf::buftag;

#[repr(C)]
struct BufferLookupEnt {
    key: buftag,
    id: i32,
}

struct SharedHash(*mut HTAB);

// SAFETY: dynahash partitioned tables serialize bucket access via the caller's
// partition LWLock and internal freelist spinlocks (C's shared-HTAB contract).
unsafe impl Sync for SharedHash {}
unsafe impl Send for SharedHash {}

static SHARED_BUF_HASH: OnceLock<SharedHash> = OnceLock::new();

#[inline]
fn htab() -> *mut HTAB {
    SHARED_BUF_HASH
        .get()
        .expect("bufmgr: InitBufTable (buf_table.c) not called")
        .0
}

/// InitBufTable (buf_table.c): size = NBuffers + NUM_BUFFER_PARTITIONS.
pub fn InitBufTable(size: i32) -> PgResult<()> {
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
    let h = hash_create(
        "Shared Buffer Lookup Table",
        size as i64,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_PARTITION,
    )?;
    SHARED_BUF_HASH
        .set(SharedHash(h))
        .unwrap_or_else(|_| panic!("bufmgr: buffer lookup table initialized twice"));
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
#[inline]
pub fn BufMappingPartitionLock(hashcode: u32) -> &'static LWLock {
    main_lock(
        (BUFFER_MAPPING_LWLOCK_OFFSET as u32 + hashcode % NUM_BUFFER_PARTITIONS as u32) as usize,
    )
}

/// BufTableLookup (buf_table.c): caller holds the partition lock (shared+).
pub fn BufTableLookup(tag: &buftag, hashcode: u32) -> PgResult<i32> {
    let (entry, found) = hash_search_with_hash_value(
        htab(),
        tag as *const buftag as *const u8,
        hashcode,
        HASH_FIND,
    )?;
    if !found {
        return Ok(-1);
    }
    // SAFETY: dynahash returned a live BufferLookupEnt for this key.
    Ok(unsafe { (*(entry as *const BufferLookupEnt)).id })
}

/// BufTableInsert (buf_table.c): -1 on success, existing id on collision.
/// Caller holds the partition lock exclusively.
pub fn BufTableInsert(tag: &buftag, hashcode: u32, buf_id: i32) -> PgResult<i32> {
    debug_assert!(buf_id >= 0);
    debug_assert!(tag.blockNum != types_core::InvalidBlockNumber);
    let (entry, found) = hash_search_with_hash_value(
        htab(),
        tag as *const buftag as *const u8,
        hashcode,
        HASH_ENTER,
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

/// BufTableDelete (buf_table.c): caller holds the partition lock exclusively.
pub fn BufTableDelete(tag: &buftag, hashcode: u32) -> PgResult<()> {
    let (_, found) = hash_search_with_hash_value(
        htab(),
        tag as *const buftag as *const u8,
        hashcode,
        HASH_REMOVE,
    )?;
    if !found {
        return Err(Box::new(
            types_error::PgError::new(ERROR, "shared buffer hash table corrupted".into())
                .with_error_location(ErrorLocation::new("buf_table.c", 0, "BufTableDelete")),
        ));
    }
    Ok(())
}
