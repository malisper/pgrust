#![allow(non_snake_case)]
#![allow(clippy::missing_safety_doc)]

use core::alloc::Layout;
use core::cell::UnsafeCell;
use core::mem::size_of;
use core::ptr;
use core::ptr::NonNull;
use core::sync::atomic::{AtomicI32, Ordering};

use ::elog::elog;
use ::mcx::{Allocator, MemoryContext};
use ::types_core::Size;
use ::types_error::{PgError, ERRCODE_OUT_OF_MEMORY, WARNING};
use ::types_error::PgResult;
use ::types_hash::hsearch::{
    HashCompareFunc, HashValueFunc, HASHACTION, HASHCTL, HASHELEMENT, HASHHDR, HASHSEGMENT, HTAB,
    HASH_ALLOC, HASH_ATTACH, HASH_BLOBS, HASH_COMPARE, HASH_CONTEXT, HASH_DIRSIZE, HASH_ELEM,
    HASH_ENTER,
    HASH_ENTER_NULL, HASH_FIND, HASH_FIXED_SIZE, HASH_FUNCTION, HASH_KEYCOPY, HASH_PARTITION,
    HASH_REMOVE, HASH_SEGMENT, HASH_SEQ_STATUS, HASH_SHARED_MEM, HASH_STRINGS, DEF_DIRSIZE,
    DEF_SEGSIZE, DEF_SEGSIZE_SHIFT, NO_MAX_DSIZE, NUM_FREELISTS,
};

const MAX_SEQ_SCANS: usize = 100;
const MAXALIGN_SIZE: usize = 8;

#[inline]
const fn MAXALIGN(value: usize) -> usize {
    (value + (MAXALIGN_SIZE - 1)) & !(MAXALIGN_SIZE - 1)
}

#[inline]
unsafe fn IS_PARTITIONED(hctl: *const HASHHDR) -> bool {
    (*hctl).num_partitions != 0
}

#[inline]
unsafe fn FREELIST_IDX(hctl: *const HASHHDR, hashcode: u32) -> usize {
    if IS_PARTITIONED(hctl) {
        (hashcode as usize) % NUM_FREELISTS
    } else {
        0
    }
}

#[inline]
unsafe fn ELEMENTKEY(helem: *mut HASHELEMENT) -> *mut u8 {
    (helem as *mut u8).add(MAXALIGN(size_of::<HASHELEMENT>()))
}

#[inline]
unsafe fn ELEMENT_FROM_KEY(key: *mut u8) -> *mut HASHELEMENT {
    key.sub(MAXALIGN(size_of::<HASHELEMENT>())) as *mut HASHELEMENT
}

#[inline]
fn MOD(x: i64, y: i64) -> i64 {
    x & (y - 1)
}

// s_lock.h shape: one TAS on the uncontended path, and contended acquires go
// through C's perform_spin_delay backoff (s_lock.c:97 `s_lock`, reached from
// S_LOCK at s_lock.h:665).
//
// These guard the freeList[i].mutex of SHARED, PARTITIONED dynahash tables --
// the heavyweight lock manager's LOCK and PROCLOCK hashes and the two SSI
// predicate-lock hashes -- so they are on the path of every relation lock
// acquire that misses the fastpath, and every release. The previous unbounded
// busy-spin had no SPIN_DELAY, no sleep, and no NUM_DELAYS valve, so it also
// lost C's `PANIC: stuck spinlock detected`: a leaked or corrupted freelist
// mutex hung the backend silently instead of failing loudly.
//
// This is the same shape, and the same mistake, that already cost this tree a
// measured regression at transam_xlog/src/ctl.rs:10-13 -- whose comment records
// that an unbounded busy-spin collapsed the multi-client write gate once
// clients exceeded vCPU. That site is the model followed here (a non-Spinlock
// lock word driving the same seam).
#[inline]
unsafe fn SpinLockInit(m: *mut AtomicI32) {
    (*m).store(0, Ordering::Relaxed);
}

#[inline]
unsafe fn SpinLockAcquire(m: *mut AtomicI32) {
    if (*m).swap(1, Ordering::Acquire) != 0 {
        SpinLockAcquireContended(m);
    }
}

#[cold]
#[inline(never)]
unsafe fn SpinLockAcquireContended(m: *mut AtomicI32) {
    let mut delay =
        s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "dynahash freeList");
    loop {
        // C's TAS_SPIN: an unlocked read before each TAS, so a spinning waiter
        // does not keep the cache line exclusive.
        if (*m).load(Ordering::Relaxed) == 0 && (*m).swap(1, Ordering::Acquire) == 0 {
            break;
        }
        s_lock_seams::perform_spin_delay::call(&mut delay);
    }
    s_lock_seams::finish_spin_delay::call(&delay);
}

#[inline]
unsafe fn SpinLockRelease(m: *mut AtomicI32) {
    (*m).store(0, Ordering::Release);
}

// hcxt holds a leaked Box<MemoryContext> (C stores the table's private
// AllocSet context); hash_destroy reclaims it, freeing every table allocation.
#[inline]
unsafe fn table_context<'a>(hashp: *mut HTAB) -> &'a MemoryContext {
    &*((*hashp).hcxt as *const MemoryContext)
}

fn context_alloc(cx: &MemoryContext, size: Size) -> *mut u8 {
    let Ok(layout) = Layout::from_size_align(size, MAXALIGN_SIZE) else {
        return ptr::null_mut();
    };
    match cx.mcx().allocate(layout) {
        Ok(p) => p.cast::<u8>().as_ptr(),
        Err(_) => ptr::null_mut(),
    }
}

unsafe fn hash_alloc(hashp: *mut HTAB, size: Size) -> *mut u8 {
    match (*hashp).alloc {
        Some(alloc) => alloc(size),
        None => context_alloc(table_context(hashp), size),
    }
}

const LO: u64 = 0x0101_0101_0101_0101;
const HI: u64 = 0x8080_8080_8080_8080;

#[inline]
fn zero_byte_mask(v: u64) -> u64 {
    v.wrapping_sub(LO) & !v & HI
}

// strncmp(key1, key2, keysize-1), 8 bytes at a time; C links the vectorized
// libc strncmp and a byte loop loses the ns gate. Little-endian byte order
// assumed (trailing_zeros/8 = first byte index).
fn string_compare(key1: &[u8], key2: &[u8], keysize: Size) -> i32 {
    let n = keysize - 1;
    let mut i = 0usize;
    while i + 8 <= n {
        let a = u64::from_le_bytes(key1[i..i + 8].try_into().unwrap());
        let b = u64::from_le_bytes(key2[i..i + 8].try_into().unwrap());
        let diff = a ^ b;
        if diff != 0 {
            let dpos = (diff.trailing_zeros() / 8) as usize;
            let zeros = zero_byte_mask(a);
            if zeros != 0 && ((zeros.trailing_zeros() / 8) as usize) < dpos {
                return 0;
            }
            return key1[i + dpos] as i32 - key2[i + dpos] as i32;
        }
        if zero_byte_mask(a) != 0 {
            return 0;
        }
        i += 8;
    }
    while i < n {
        let (a, b) = (key1[i], key2[i]);
        if a != b || a == 0 {
            return a as i32 - b as i32;
        }
        i += 1;
    }
    0
}

// C's string_hash = strlen (vectorized libc) + hash_bytes; hashfn's
// string_hash scans bytewise, which loses the ns gate here, so the length
// scan is word-wise locally and the hashing stays in hashfn.
fn dyna_string_hash(key: &[u8], keysize: Size) -> u32 {
    let limit = keysize - 1;
    let mut len = limit;
    let mut i = 0usize;
    while i + 8 <= limit {
        let v = u64::from_le_bytes(key[i..i + 8].try_into().unwrap());
        let zeros = zero_byte_mask(v);
        if zeros != 0 {
            len = i + (zeros.trailing_zeros() / 8) as usize;
            return hashfn::hash_bytes(&key[..len]);
        }
        i += 8;
    }
    while i < limit {
        if key[i] == 0 {
            len = i;
            break;
        }
        i += 1;
    }
    hashfn::hash_bytes(&key[..len])
}

// C's default match is memcmp; dynahash only tests == 0, so equality (one
// bcmp) replaces the three-way compare (divergence: nonzero result is 1).
fn blob_compare(key1: &[u8], key2: &[u8], keysize: Size) -> i32 {
    debug_assert!(key1.len() >= keysize && key2.len() >= keysize);
    // SAFETY: hsearch contract — both key buffers span keysize bytes.
    unsafe {
        (core::slice::from_raw_parts(key1.as_ptr(), keysize)
            != core::slice::from_raw_parts(key2.as_ptr(), keysize)) as i32
    }
}

// strlcpy(dst, src, keysize) shape, 8 bytes at a time (C links libc strlcpy;
// a byte loop loses the instruction gate). May store src bytes past the NUL
// within the same 8-byte word — invisible to strncmp/strlen-bounded readers.
fn strlcpy_key(dst: &mut [u8], src: &[u8], keysize: Size) {
    if keysize == 0 {
        return;
    }
    let limit = keysize - 1;
    let mut i = 0usize;
    while i + 8 <= limit {
        let v = u64::from_le_bytes(src[i..i + 8].try_into().unwrap());
        dst[i..i + 8].copy_from_slice(&v.to_le_bytes());
        if zero_byte_mask(v) != 0 {
            return;
        }
        i += 8;
    }
    while i < limit {
        let c = src[i];
        dst[i] = c;
        if c == 0 {
            return;
        }
        i += 1;
    }
    dst[i] = 0;
}

fn mem_copy(dst: &mut [u8], src: &[u8], keysize: Size) {
    debug_assert!(dst.len() >= keysize && src.len() >= keysize);
    // SAFETY: hsearch contract — both buffers span keysize bytes; the entry
    // key never overlaps the probe key.
    unsafe { ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), keysize) };
}

fn uint32_hash(key: &[u8], _keysize: Size) -> u32 {
    hashfn::uint32_hash(u32::from_ne_bytes([key[0], key[1], key[2], key[3]]))
}

pub fn hash_create(tabname: &str, nelem: i64, info: &HASHCTL, flags: i32) -> PgResult<*mut HTAB> {
    debug_assert!(flags & HASH_ELEM != 0);
    debug_assert!(info.keysize > 0);
    debug_assert!(info.entrysize >= info.keysize);

    if flags & HASH_SHARED_MEM != 0 && flags & HASH_FIXED_SIZE == 0 {
        // One process = one address space, so a shared table lives on the
        // ordinary heap — but growth allocates through the table's private
        // (single-threaded) MemoryContext, so only fully preallocated shared
        // tables are thread-safe under the partition-lock protocol.
        panic!("dynahash: HASH_SHARED_MEM requires HASH_FIXED_SIZE (table \"{tabname}\")");
    }
    if flags & HASH_ATTACH != 0 {
        // Attach re-finds an existing table by name via the shmem index; that
        // layer (ShmemInitHash) owns attach semantics in this port.
        panic!("dynahash: HASH_ATTACH not supported (table \"{tabname}\")");
    }

    let context = if flags & HASH_CONTEXT != 0 {
        // SAFETY: contract — info.hcxt is a live mcx::MemoryContext used as the
        // accounting parent. Divergence: parent reset does NOT free the table;
        // hash_destroy is always required.
        unsafe { (*(info.hcxt as *const MemoryContext)).new_child("dynahash") }
    } else {
        MemoryContext::new("dynahash")
    };
    let cxp = Box::into_raw(Box::new(context));

    match unsafe { hash_create_in(tabname, nelem, info, flags, cxp) } {
        Ok(hashp) => Ok(hashp),
        Err(e) => {
            unsafe { drop(Box::from_raw(cxp)) };
            Err(e)
        }
    }
}

unsafe fn hash_create_in(
    tabname: &str,
    nelem: i64,
    info: &HASHCTL,
    flags: i32,
    cxp: *mut MemoryContext,
) -> PgResult<*mut HTAB> {
    let cx = &*cxp;
    let hashp = context_alloc(cx, size_of::<HTAB>() + tabname.len() + 1) as *mut HTAB;
    if hashp.is_null() {
        return Err(oom_error(false));
    }
    ptr::write(hashp, htab_zeroed());
    let namep = (hashp as *mut u8).add(size_of::<HTAB>());
    ptr::copy_nonoverlapping(tabname.as_ptr(), namep, tabname.len());
    *namep.add(tabname.len()) = 0;
    (*hashp).tabname = namep;
    cx.set_ident(Some(tabname));

    // C keys the match/keycopy defaults off `hashp->hash == string_hash`;
    // Rust fn-item comparison is unreliable, so carry the choice as a bool.
    let mut is_string_hash = false;
    if flags & HASH_FUNCTION != 0 {
        debug_assert!(flags & (HASH_BLOBS | HASH_STRINGS) == 0);
        (*hashp).hash = info.hash;
    } else if flags & HASH_BLOBS != 0 {
        debug_assert!(flags & HASH_STRINGS == 0);
        if info.keysize == size_of::<u32>() {
            (*hashp).hash = Some(uint32_hash as HashValueFunc);
        } else {
            (*hashp).hash = Some(hashfn::tag_hash as HashValueFunc);
        }
    } else {
        debug_assert!(flags & HASH_STRINGS != 0);
        debug_assert!(info.keysize > 8);
        (*hashp).hash = Some(dyna_string_hash as HashValueFunc);
        is_string_hash = true;
    }

    if flags & HASH_COMPARE != 0 {
        (*hashp).match_ = info.match_;
    } else if is_string_hash {
        (*hashp).match_ = Some(string_compare as HashCompareFunc);
    } else {
        (*hashp).match_ = Some(blob_compare as HashCompareFunc);
    }

    if flags & HASH_KEYCOPY != 0 {
        (*hashp).keycopy = info.keycopy;
    } else if is_string_hash {
        (*hashp).keycopy = Some(strlcpy_key);
    } else {
        (*hashp).keycopy = Some(mem_copy);
    }

    if flags & HASH_ALLOC != 0 {
        (*hashp).alloc = info.alloc;
    } else {
        (*hashp).alloc = None;
    }

    (*hashp).hctl = ptr::null_mut();
    (*hashp).dir = ptr::null_mut();
    (*hashp).hcxt = cxp as *mut u8;
    (*hashp).isshared = flags & HASH_SHARED_MEM != 0;

    let hdr = hash_alloc(hashp, size_of::<HASHHDR>());
    if hdr.is_null() {
        return Err(oom_error(false));
    }
    (*hashp).hctl = hdr as *mut HASHHDR;

    (*hashp).frozen = false;

    hdefault(hashp);

    let hctl = (*hashp).hctl;

    if flags & HASH_PARTITION != 0 {
        debug_assert!(flags & HASH_SHARED_MEM != 0);
        debug_assert!(info.num_partitions == next_pow2_int(info.num_partitions) as i64);
        (*hctl).num_partitions = info.num_partitions;
    }

    if flags & HASH_SEGMENT != 0 {
        (*hctl).ssize = info.ssize;
        (*hctl).sshift = my_log2(info.ssize);
        debug_assert!(info.ssize == 1i64 << (*hctl).sshift);
    }

    if flags & HASH_DIRSIZE != 0 {
        (*hctl).max_dsize = info.max_dsize;
        (*hctl).dsize = info.dsize;
    }

    (*hctl).keysize = info.keysize;
    (*hctl).entrysize = info.entrysize;

    (*hashp).keysize = (*hctl).keysize;
    (*hashp).ssize = (*hctl).ssize;
    (*hashp).sshift = (*hctl).sshift;

    if !init_htab(hashp, nelem) {
        return Err(Box::new(PgError::error(format!(
            "failed to initialize hash table \"{tabname}\""
        ))));
    }

    if (flags & HASH_SHARED_MEM != 0) || nelem < (*hctl).nelem_alloc as i64 {
        let freelist_partitions = if IS_PARTITIONED(hctl) { NUM_FREELISTS as i32 } else { 1 };
        let nelem_i = nelem as i32;
        let mut nelem_alloc = nelem_i / freelist_partitions;
        if nelem_alloc <= 0 {
            nelem_alloc = 1;
        }
        let nelem_alloc_first = if nelem_alloc * freelist_partitions < nelem_i {
            nelem_i - nelem_alloc * (freelist_partitions - 1)
        } else {
            nelem_alloc
        };

        for i in 0..freelist_partitions {
            let temp = if i == 0 { nelem_alloc_first } else { nelem_alloc };
            if !element_alloc(hashp, temp, i as usize) {
                return Err(oom_error(false));
            }
        }
    }

    if flags & HASH_FIXED_SIZE != 0 {
        (*hashp).isfixed = true;
    }

    Ok(hashp)
}

fn htab_zeroed() -> HTAB {
    HTAB {
        hctl: ptr::null_mut(),
        dir: ptr::null_mut(),
        hash: None,
        match_: None,
        keycopy: None,
        alloc: None,
        hcxt: ptr::null_mut(),
        tabname: ptr::null_mut(),
        isshared: false,
        isfixed: false,
        frozen: false,
        keysize: 0,
        ssize: 0,
        sshift: 0,
    }
}

unsafe fn hdefault(hashp: *mut HTAB) {
    let hctl = (*hashp).hctl;
    ptr::write_bytes(hctl as *mut u8, 0, size_of::<HASHHDR>());

    (*hctl).dsize = DEF_DIRSIZE;
    (*hctl).nsegs = 0;
    (*hctl).num_partitions = 0;
    (*hctl).max_dsize = NO_MAX_DSIZE;
    (*hctl).ssize = DEF_SEGSIZE;
    (*hctl).sshift = DEF_SEGSIZE_SHIFT;
}

fn choose_nelem_alloc(entrysize: Size) -> i32 {
    let element_size = MAXALIGN(size_of::<HASHELEMENT>()) + MAXALIGN(entrysize);
    let mut alloc_size: usize = 32 * 4;
    let mut nelem_alloc: i32;
    loop {
        alloc_size <<= 1;
        nelem_alloc = (alloc_size / element_size) as i32;
        if nelem_alloc >= 32 {
            break;
        }
    }
    nelem_alloc
}

unsafe fn init_htab(hashp: *mut HTAB, nelem: i64) -> bool {
    let hctl = (*hashp).hctl;

    if IS_PARTITIONED(hctl) {
        for i in 0..NUM_FREELISTS {
            SpinLockInit(&mut (*hctl).freeList[i].mutex);
        }
    }

    let mut nbuckets = next_pow2_int(nelem);

    while (nbuckets as i64) < (*hctl).num_partitions {
        nbuckets <<= 1;
    }

    (*hctl).max_bucket = (nbuckets - 1) as u32;
    (*hctl).low_mask = (nbuckets - 1) as u32;
    (*hctl).high_mask = ((nbuckets << 1) - 1) as u32;

    let mut nsegs = (nbuckets - 1) as i64 / (*hctl).ssize + 1;
    nsegs = next_pow2_int(nsegs) as i64;

    if nsegs > (*hctl).dsize {
        if (*hashp).dir.is_null() {
            (*hctl).dsize = nsegs;
        } else {
            return false;
        }
    }

    if (*hashp).dir.is_null() {
        let bytes = (*hctl).dsize as usize * size_of::<HASHSEGMENT>();
        let dir = hash_alloc(hashp, bytes);
        if dir.is_null() {
            return false;
        }
        (*hashp).dir = dir as *mut HASHSEGMENT;
    }

    let mut segp = (*hashp).dir;
    while (*hctl).nsegs < nsegs {
        let seg = seg_alloc(hashp);
        if seg.is_null() {
            return false;
        }
        *segp = seg;
        (*hctl).nsegs += 1;
        segp = segp.add(1);
    }

    (*hctl).nelem_alloc = choose_nelem_alloc((*hctl).entrysize);

    true
}

pub fn hash_estimate_size(num_entries: i64, entrysize: Size) -> Size {
    let n_buckets = next_pow2_long(num_entries);
    let n_segments = next_pow2_long((n_buckets - 1) / DEF_SEGSIZE + 1);
    let mut n_dir_entries = DEF_DIRSIZE;
    while n_dir_entries < n_segments {
        n_dir_entries <<= 1;
    }

    let mut size = MAXALIGN(size_of::<HASHHDR>());
    size += n_dir_entries as usize * size_of::<HASHSEGMENT>();
    size += n_segments as usize * MAXALIGN(DEF_SEGSIZE as usize * size_of::<*mut HASHELEMENT>());
    let element_alloc_cnt = choose_nelem_alloc(entrysize) as i64;
    let n_element_allocs = (num_entries - 1) / element_alloc_cnt + 1;
    let element_size = MAXALIGN(size_of::<HASHELEMENT>()) + MAXALIGN(entrysize);
    size += (n_element_allocs * element_alloc_cnt) as usize * element_size;

    size
}

pub fn hash_select_dirsize(num_entries: i64) -> i64 {
    let n_buckets = next_pow2_long(num_entries);
    let n_segments = next_pow2_long((n_buckets - 1) / DEF_SEGSIZE + 1);
    let mut n_dir_entries = DEF_DIRSIZE;
    while n_dir_entries < n_segments {
        n_dir_entries <<= 1;
    }
    n_dir_entries
}

pub fn hash_get_shared_size(info: &HASHCTL, flags: i32) -> Size {
    debug_assert!(flags & HASH_DIRSIZE != 0);
    debug_assert!(info.dsize == info.max_dsize);
    size_of::<HASHHDR>() + info.dsize as usize * size_of::<HASHSEGMENT>()
}

pub fn hash_destroy(hashp: *mut HTAB) {
    if hashp.is_null() {
        return;
    }
    unsafe {
        debug_assert!((*hashp).alloc.is_none());
        debug_assert!(!(*hashp).hcxt.is_null());
        drop(Box::from_raw((*hashp).hcxt as *mut MemoryContext));
    }
}

/// Crash-cycle reset in place (notes/crash-restart-design.md): unlinks every
/// live entry back onto the freelists and re-arms freelist counts/spinlocks,
/// restoring the post-create boot image without reallocating. Only for fully
/// preallocated shared tables — bucket geometry never changed since create.
///
/// # Safety
/// Caller must have exclusive access to the table (crash choreography: every
/// child is dead, only the postmaster thread runs).
pub unsafe fn hash_reset_after_crash(hashp: *mut HTAB) {
    let hctl = (*hashp).hctl;
    assert!((*hashp).isshared && (*hashp).isfixed);
    for bucket in 0..=(*hctl).max_bucket as i64 {
        let segp = *(*hashp).dir.offset((bucket >> (*hashp).sshift) as isize);
        let slot = segp.offset(MOD(bucket, (*hashp).ssize) as isize);
        let mut el = *slot;
        while !el.is_null() {
            let next = (*el).link;
            let idx = FREELIST_IDX(hctl, (*el).hashvalue);
            (*el).link = (*hctl).freeList[idx].freeList;
            (*hctl).freeList[idx].freeList = el;
            el = next;
        }
        *slot = ptr::null_mut();
    }
    let nlists = if IS_PARTITIONED(hctl) { NUM_FREELISTS } else { 1 };
    for i in 0..nlists {
        (*hctl).freeList[i].nentries = 0;
        SpinLockInit(&mut (*hctl).freeList[i].mutex);
    }
}

pub fn get_hash_value(hashp: *mut HTAB, key_ptr: *const u8) -> u32 {
    unsafe { do_hash(hashp, key_ptr) }
}

#[inline]
unsafe fn do_hash(hashp: *const HTAB, key_ptr: *const u8) -> u32 {
    let keysize = (*hashp).keysize;
    // SAFETY: hash is installed by hash_create; key buffers span keysize bytes.
    let f = (*hashp).hash.unwrap_unchecked();
    f(core::slice::from_raw_parts(key_ptr, keysize), keysize)
}

#[inline]
unsafe fn calc_bucket(hctl: *const HASHHDR, hash_val: u32) -> u32 {
    let mut bucket = hash_val & (*hctl).high_mask;
    if bucket > (*hctl).max_bucket {
        bucket &= (*hctl).low_mask;
    }
    bucket
}

pub fn hash_search(
    hashp: *mut HTAB,
    key_ptr: *const u8,
    action: HASHACTION,
    found_ptr: Option<&mut bool>,
) -> PgResult<*mut u8> {
    let hashvalue = unsafe { do_hash(hashp, key_ptr) };
    hash_search_with_hash_value(hashp, key_ptr, hashvalue, action, found_ptr)
}

pub fn hash_search_with_hash_value(
    hashp: *mut HTAB,
    key_ptr: *const u8,
    hashvalue: u32,
    action: HASHACTION,
    found_ptr: Option<&mut bool>,
) -> PgResult<*mut u8> {
    unsafe {
        let hctl = (*hashp).hctl;
        let freelist_idx = FREELIST_IDX(hctl, hashvalue);

        if (action == HASH_ENTER || action == HASH_ENTER_NULL)
            && (*hctl).freeList[0].nentries > (*hctl).max_bucket as i64
            && !IS_PARTITIONED(hctl)
            && !(*hashp).frozen
            && !has_seq_scans(hashp)
        {
            let _ = expand_table(hashp);
        }

        let (mut prev_bucket_ptr, _) = hash_initial_lookup(hashp, hashvalue);
        let mut curr_bucket = *prev_bucket_ptr;

        // SAFETY: match_ is installed by hash_create.
        let match_ = (*hashp).match_.unwrap_unchecked();
        let keysize = (*hashp).keysize;

        while !curr_bucket.is_null() {
            if (*curr_bucket).hashvalue == hashvalue
                && match_(
                    core::slice::from_raw_parts(ELEMENTKEY(curr_bucket), keysize),
                    core::slice::from_raw_parts(key_ptr, keysize),
                    keysize,
                ) == 0
            {
                break;
            }
            prev_bucket_ptr = &mut (*curr_bucket).link;
            curr_bucket = *prev_bucket_ptr;
        }

        let found = !curr_bucket.is_null();
        if let Some(f) = found_ptr {
            *f = found;
        }

        match action {
            HASH_FIND => {
                if found {
                    Ok(ELEMENTKEY(curr_bucket))
                } else {
                    Ok(ptr::null_mut())
                }
            }
            HASH_REMOVE => {
                if found {
                    if IS_PARTITIONED(hctl) {
                        SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
                    }
                    debug_assert!((*hctl).freeList[freelist_idx].nentries > 0);
                    (*hctl).freeList[freelist_idx].nentries -= 1;
                    *prev_bucket_ptr = (*curr_bucket).link;
                    (*curr_bucket).link = (*hctl).freeList[freelist_idx].freeList;
                    (*hctl).freeList[freelist_idx].freeList = curr_bucket;
                    if IS_PARTITIONED(hctl) {
                        SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
                    }
                    // C contract: dangling-but-stable pointer (now on freelist).
                    Ok(ELEMENTKEY(curr_bucket))
                } else {
                    Ok(ptr::null_mut())
                }
            }
            HASH_ENTER | HASH_ENTER_NULL => {
                if found {
                    return Ok(ELEMENTKEY(curr_bucket));
                }
                if (*hashp).frozen {
                    return Err(frozen_error("insert into", hashp));
                }

                let new_bucket = get_hash_entry(hashp, freelist_idx);
                if new_bucket.is_null() {
                    if action == HASH_ENTER_NULL {
                        return Ok(ptr::null_mut());
                    }
                    return Err(oom_error((*hashp).isshared));
                }

                *prev_bucket_ptr = new_bucket;
                (*new_bucket).link = ptr::null_mut();
                (*new_bucket).hashvalue = hashvalue;
                // SAFETY: keycopy is installed by hash_create.
                let keycopy = (*hashp).keycopy.unwrap_unchecked();
                keycopy(
                    core::slice::from_raw_parts_mut(ELEMENTKEY(new_bucket), keysize),
                    core::slice::from_raw_parts(key_ptr, keysize),
                    keysize,
                );

                Ok(ELEMENTKEY(new_bucket))
            }
        }
    }
}

pub fn hash_update_hash_key(
    hashp: *mut HTAB,
    existing_entry: *mut u8,
    new_key_ptr: *const u8,
) -> PgResult<bool> {
    unsafe {
        let existing_element = ELEMENT_FROM_KEY(existing_entry);

        if (*hashp).frozen {
            return Err(frozen_error("update in", hashp));
        }

        let (mut prev_bucket_ptr, bucket) =
            hash_initial_lookup(hashp, (*existing_element).hashvalue);
        let mut curr_bucket = *prev_bucket_ptr;
        while !curr_bucket.is_null() {
            if curr_bucket == existing_element {
                break;
            }
            prev_bucket_ptr = &mut (*curr_bucket).link;
            curr_bucket = *prev_bucket_ptr;
        }
        if curr_bucket.is_null() {
            return Err(Box::new(PgError::error(format!(
                "hash_update_hash_key argument is not in hashtable \"{}\"",
                tabname_str(hashp)
            ))));
        }
        let old_prev_ptr = prev_bucket_ptr;

        let newhashvalue = do_hash(hashp, new_key_ptr);
        let (mut prev_bucket_ptr, newbucket) = hash_initial_lookup(hashp, newhashvalue);
        let mut curr_bucket = *prev_bucket_ptr;

        // SAFETY: match_/keycopy installed by hash_create.
        let match_ = (*hashp).match_.unwrap_unchecked();
        let keysize = (*hashp).keysize;

        while !curr_bucket.is_null() {
            if (*curr_bucket).hashvalue == newhashvalue
                && match_(
                    core::slice::from_raw_parts(ELEMENTKEY(curr_bucket), keysize),
                    core::slice::from_raw_parts(new_key_ptr, keysize),
                    keysize,
                ) == 0
            {
                break;
            }
            prev_bucket_ptr = &mut (*curr_bucket).link;
            curr_bucket = *prev_bucket_ptr;
        }

        if !curr_bucket.is_null() {
            return Ok(false);
        }

        let curr = existing_element;

        // Same bucket: leave chain links alone (unlink+relink would corrupt
        // the list when curr is the chain tail).
        if bucket != newbucket {
            *old_prev_ptr = (*curr).link;
            *prev_bucket_ptr = curr;
            (*curr).link = ptr::null_mut();
        }

        // DIVERGENCE from C (which leaves nentries keyed by the insert-time
        // hashvalue): move the accounting to the new hashvalue's freelist, so
        // a later HASH_REMOVE under the new key never underflows a freelist
        // C only avoids by slack (lock.c 2PC proclock reassignment hits it).
        let hctl = (*hashp).hctl;
        let old_idx = FREELIST_IDX(hctl, (*curr).hashvalue);
        let new_idx = FREELIST_IDX(hctl, newhashvalue);
        if old_idx != new_idx {
            let (lo, hi) = (old_idx.min(new_idx), old_idx.max(new_idx));
            SpinLockAcquire(&mut (*hctl).freeList[lo].mutex);
            SpinLockAcquire(&mut (*hctl).freeList[hi].mutex);
            debug_assert!((*hctl).freeList[old_idx].nentries > 0);
            (*hctl).freeList[old_idx].nentries -= 1;
            (*hctl).freeList[new_idx].nentries += 1;
            SpinLockRelease(&mut (*hctl).freeList[hi].mutex);
            SpinLockRelease(&mut (*hctl).freeList[lo].mutex);
        }

        (*curr).hashvalue = newhashvalue;
        let keycopy = (*hashp).keycopy.unwrap_unchecked();
        keycopy(
            core::slice::from_raw_parts_mut(ELEMENTKEY(curr), keysize),
            core::slice::from_raw_parts(new_key_ptr, keysize),
            keysize,
        );

        Ok(true)
    }
}

unsafe fn get_hash_entry(hashp: *mut HTAB, freelist_idx: usize) -> *mut HASHELEMENT {
    let hctl = (*hashp).hctl;
    let mut new_element: *mut HASHELEMENT;

    loop {
        if IS_PARTITIONED(hctl) {
            SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
        }

        new_element = (*hctl).freeList[freelist_idx].freeList;

        if !new_element.is_null() {
            break;
        }

        if IS_PARTITIONED(hctl) {
            SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
        }

        if !element_alloc(hashp, (*hctl).nelem_alloc, freelist_idx) {
            if !IS_PARTITIONED(hctl) {
                return ptr::null_mut();
            }

            let mut borrow_from_idx = freelist_idx;
            loop {
                borrow_from_idx = (borrow_from_idx + 1) % NUM_FREELISTS;
                if borrow_from_idx == freelist_idx {
                    break;
                }

                SpinLockAcquire(&mut (*hctl).freeList[borrow_from_idx].mutex);
                new_element = (*hctl).freeList[borrow_from_idx].freeList;

                if !new_element.is_null() {
                    (*hctl).freeList[borrow_from_idx].freeList = (*new_element).link;
                    SpinLockRelease(&mut (*hctl).freeList[borrow_from_idx].mutex);

                    // Count the borrowed element in its hashcode's freelist.
                    SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
                    (*hctl).freeList[freelist_idx].nentries += 1;
                    SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);

                    return new_element;
                }

                SpinLockRelease(&mut (*hctl).freeList[borrow_from_idx].mutex);
            }

            return ptr::null_mut();
        }
    }

    (*hctl).freeList[freelist_idx].freeList = (*new_element).link;
    (*hctl).freeList[freelist_idx].nentries += 1;

    if IS_PARTITIONED(hctl) {
        SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
    }

    new_element
}

pub fn hash_get_num_entries(hashp: *mut HTAB) -> i64 {
    unsafe {
        let hctl = (*hashp).hctl;
        let mut sum = (*hctl).freeList[0].nentries;
        if IS_PARTITIONED(hctl) {
            for i in 1..NUM_FREELISTS {
                sum += (*hctl).freeList[i].nentries;
            }
        }
        sum
    }
}

pub fn hash_seq_init(status: &mut HASH_SEQ_STATUS, hashp: *mut HTAB) -> PgResult<()> {
    status.hashp = hashp;
    status.curBucket = 0;
    status.curEntry = ptr::null_mut();
    status.hasHashvalue = false;
    unsafe {
        if !(*hashp).frozen {
            register_seq_scan(hashp)?;
        }
    }
    Ok(())
}

pub fn hash_seq_init_with_hash_value(
    status: &mut HASH_SEQ_STATUS,
    hashp: *mut HTAB,
    hashvalue: u32,
) -> PgResult<()> {
    hash_seq_init(status, hashp)?;
    status.hasHashvalue = true;
    status.hashvalue = hashvalue;
    unsafe {
        let (bucket_ptr, bucket) = hash_initial_lookup(hashp, hashvalue);
        status.curBucket = bucket;
        status.curEntry = *bucket_ptr;
    }
    Ok(())
}

pub fn hash_seq_search(status: &mut HASH_SEQ_STATUS) -> PgResult<*mut u8> {
    unsafe {
        if status.hasHashvalue {
            loop {
                let cur_elem = status.curEntry;
                if cur_elem.is_null() {
                    break;
                }
                status.curEntry = (*cur_elem).link;
                if status.hashvalue != (*cur_elem).hashvalue {
                    continue;
                }
                return Ok(ELEMENTKEY(cur_elem));
            }
            hash_seq_term_inner(status.hashp)?;
            return Ok(ptr::null_mut());
        }

        let cur_elem0 = status.curEntry;
        if !cur_elem0.is_null() {
            status.curEntry = (*cur_elem0).link;
            if status.curEntry.is_null() {
                status.curBucket += 1;
            }
            return Ok(ELEMENTKEY(cur_elem0));
        }

        let mut cur_bucket = status.curBucket;
        let hashp = status.hashp;
        let hctl = (*hashp).hctl;
        let ssize = (*hashp).ssize;
        let sshift = (*hashp).sshift;
        let max_bucket = (*hctl).max_bucket;

        if cur_bucket > max_bucket {
            hash_seq_term_inner(hashp)?;
            return Ok(ptr::null_mut());
        }

        let mut segment_num = (cur_bucket >> sshift) as i64;
        let mut segment_ndx = MOD(cur_bucket as i64, ssize);

        let mut segp = *(*hashp).dir.offset(segment_num as isize);

        let mut cur_elem;
        loop {
            cur_elem = *segp.offset(segment_ndx as isize);
            if !cur_elem.is_null() {
                break;
            }
            cur_bucket += 1;
            if cur_bucket > max_bucket {
                status.curBucket = cur_bucket;
                hash_seq_term_inner(hashp)?;
                return Ok(ptr::null_mut());
            }
            segment_ndx += 1;
            if segment_ndx >= ssize {
                segment_num += 1;
                segment_ndx = 0;
                segp = *(*hashp).dir.offset(segment_num as isize);
            }
        }

        status.curEntry = (*cur_elem).link;
        if status.curEntry.is_null() {
            cur_bucket += 1;
        }
        status.curBucket = cur_bucket;
        Ok(ELEMENTKEY(cur_elem))
    }
}

pub fn hash_seq_term(status: &mut HASH_SEQ_STATUS) -> PgResult<()> {
    unsafe { hash_seq_term_inner(status.hashp) }
}

unsafe fn hash_seq_term_inner(hashp: *mut HTAB) -> PgResult<()> {
    if !(*hashp).frozen {
        deregister_seq_scan(hashp)?;
    }
    Ok(())
}

pub fn hash_freeze(hashp: *mut HTAB) -> PgResult<()> {
    unsafe {
        if (*hashp).isshared {
            return Err(Box::new(PgError::error(format!(
                "cannot freeze shared hashtable \"{}\"",
                tabname_str(hashp)
            ))));
        }
        if !(*hashp).frozen && has_seq_scans(hashp) {
            return Err(Box::new(PgError::error(format!(
                "cannot freeze hashtable \"{}\" because it has active scans",
                tabname_str(hashp)
            ))));
        }
        (*hashp).frozen = true;
    }
    Ok(())
}

#[inline(never)]
unsafe fn expand_table(hashp: *mut HTAB) -> bool {
    let hctl = (*hashp).hctl;
    debug_assert!(!IS_PARTITIONED(hctl));

    let new_bucket = (*hctl).max_bucket as i64 + 1;
    let new_segnum = new_bucket >> (*hashp).sshift;
    let new_segndx = MOD(new_bucket, (*hashp).ssize);

    if new_segnum >= (*hctl).nsegs {
        if new_segnum >= (*hctl).dsize && !dir_realloc(hashp) {
            return false;
        }
        let seg = seg_alloc(hashp);
        if seg.is_null() {
            return false;
        }
        *(*hashp).dir.offset(new_segnum as isize) = seg;
        (*hctl).nsegs += 1;
    }

    (*hctl).max_bucket += 1;

    // Old bucket must be computed BEFORE the mask adjustment below.
    let old_bucket = new_bucket & (*hctl).low_mask as i64;

    if new_bucket as u32 > (*hctl).high_mask {
        (*hctl).low_mask = (*hctl).high_mask;
        (*hctl).high_mask = new_bucket as u32 | (*hctl).low_mask;
    }

    let old_segnum = old_bucket >> (*hashp).sshift;
    let old_segndx = MOD(old_bucket, (*hashp).ssize);

    let old_seg = *(*hashp).dir.offset(old_segnum as isize);
    let new_seg = *(*hashp).dir.offset(new_segnum as isize);

    let mut oldlink: *mut *mut HASHELEMENT = old_seg.offset(old_segndx as isize);
    let mut newlink: *mut *mut HASHELEMENT = new_seg.offset(new_segndx as isize);

    let mut curr_element = *oldlink;
    while !curr_element.is_null() {
        let next_element = (*curr_element).link;
        if calc_bucket(hctl, (*curr_element).hashvalue) as i64 == old_bucket {
            *oldlink = curr_element;
            oldlink = &mut (*curr_element).link;
        } else {
            *newlink = curr_element;
            newlink = &mut (*curr_element).link;
        }
        curr_element = next_element;
    }
    *oldlink = ptr::null_mut();
    *newlink = ptr::null_mut();

    true
}

unsafe fn dir_realloc(hashp: *mut HTAB) -> bool {
    let hctl = (*hashp).hctl;
    if (*hctl).max_dsize != NO_MAX_DSIZE {
        return false;
    }

    let new_dsize = (*hctl).dsize << 1;
    let old_dirsize = (*hctl).dsize as usize * size_of::<HASHSEGMENT>();
    let new_dirsize = new_dsize as usize * size_of::<HASHSEGMENT>();

    let old_p = (*hashp).dir;
    let p = hash_alloc(hashp, new_dirsize);
    if p.is_null() {
        return false;
    }
    let new_p = p as *mut HASHSEGMENT;
    ptr::copy_nonoverlapping(old_p as *const u8, new_p as *mut u8, old_dirsize);
    ptr::write_bytes((new_p as *mut u8).add(old_dirsize), 0, new_dirsize - old_dirsize);
    (*hashp).dir = new_p;
    (*hctl).dsize = new_dsize;

    if (*hashp).alloc.is_none() {
        // SAFETY: old_p came from this context with this exact layout (pfree).
        table_context(hashp).mcx().deallocate(
            NonNull::new_unchecked(old_p as *mut u8),
            Layout::from_size_align_unchecked(old_dirsize, MAXALIGN_SIZE),
        );
    }
    true
}

unsafe fn seg_alloc(hashp: *mut HTAB) -> HASHSEGMENT {
    let bytes = size_of::<*mut HASHELEMENT>() * (*hashp).ssize as usize;
    let segp = hash_alloc(hashp, bytes);
    if segp.is_null() {
        return ptr::null_mut();
    }
    ptr::write_bytes(segp, 0, bytes);
    segp as HASHSEGMENT
}

unsafe fn element_alloc(hashp: *mut HTAB, nelem: i32, freelist_idx: usize) -> bool {
    let hctl = (*hashp).hctl;

    if (*hashp).isfixed {
        return false;
    }

    let element_size = MAXALIGN(size_of::<HASHELEMENT>()) + MAXALIGN((*hctl).entrysize);

    let first_element = hash_alloc(hashp, nelem as usize * element_size);
    if first_element.is_null() {
        return false;
    }

    let mut prev_element: *mut HASHELEMENT = ptr::null_mut();
    let mut tmp = first_element;
    for _ in 0..nelem {
        let el = tmp as *mut HASHELEMENT;
        (*el).link = prev_element;
        prev_element = el;
        tmp = tmp.add(element_size);
    }
    let first = first_element as *mut HASHELEMENT;

    if IS_PARTITIONED(hctl) {
        SpinLockAcquire(&mut (*hctl).freeList[freelist_idx].mutex);
    }

    (*first).link = (*hctl).freeList[freelist_idx].freeList;
    (*hctl).freeList[freelist_idx].freeList = prev_element;

    if IS_PARTITIONED(hctl) {
        SpinLockRelease(&mut (*hctl).freeList[freelist_idx].mutex);
    }

    true
}

#[inline]
unsafe fn hash_initial_lookup(hashp: *mut HTAB, hashvalue: u32) -> (*mut *mut HASHELEMENT, u32) {
    let hctl = (*hashp).hctl;
    let bucket = calc_bucket(hctl, hashvalue);

    let segment_num = (bucket >> (*hashp).sshift) as i64;
    let segment_ndx = MOD(bucket as i64, (*hashp).ssize);

    let segp = *(*hashp).dir.offset(segment_num as isize);
    if segp.is_null() {
        hash_corrupted(hashp);
    }
    (segp.offset(segment_ndx as isize), bucket)
}

#[cold]
#[inline(never)]
unsafe fn hash_corrupted(hashp: *mut HTAB) -> ! {
    // C: elog(PANIC) shared / elog(FATAL) local; both end the world here too.
    panic!("hash table \"{}\" corrupted", tabname_str(hashp));
}

unsafe fn tabname_str(hashp: *const HTAB) -> std::borrow::Cow<'static, str> {
    let p = (*hashp).tabname;
    if p.is_null() {
        return std::borrow::Cow::Borrowed("");
    }
    let cstr = core::ffi::CStr::from_ptr(p as *const core::ffi::c_char);
    std::borrow::Cow::Owned(cstr.to_string_lossy().into_owned())
}

#[track_caller]
#[cold]
#[inline(never)]
fn oom_error(shared: bool) -> Box<PgError> {
    let msg = if shared { "out of shared memory" } else { "out of memory" };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_OUT_OF_MEMORY))
}

#[track_caller]
#[cold]
#[inline(never)]
fn frozen_error(op: &str, hashp: *mut HTAB) -> Box<PgError> {
    unsafe {
        Box::new(PgError::error(format!(
            "cannot {op} frozen hashtable \"{}\"",
            tabname_str(hashp)
        )))
    }
}

pub fn my_log2(num: i64) -> i32 {
    let num = num.min(i64::MAX / 2);
    pg_ceil_log2_64(num)
}

fn pg_ceil_log2_64(num: i64) -> i32 {
    if num <= 1 {
        return 0;
    }
    let v = (num - 1) as u64;
    (64 - v.leading_zeros()) as i32
}

fn next_pow2_long(num: i64) -> i64 {
    1i64 << my_log2(num)
}

fn next_pow2_int(num: i64) -> i32 {
    let num = num.min(i32::MAX as i64 / 2);
    1i32 << my_log2(num)
}

struct SeqScanState {
    tables: [usize; MAX_SEQ_SCANS],
    level: [i32; MAX_SEQ_SCANS],
    n: usize,
}

const _: () = assert!(!core::mem::needs_drop::<SeqScanState>());

thread_local! {
    static SEQ_SCANS: UnsafeCell<SeqScanState> = const {
        UnsafeCell::new(SeqScanState {
            tables: [0; MAX_SEQ_SCANS],
            level: [0; MAX_SEQ_SCANS],
            n: 0,
        })
    };
}

#[inline]
fn with_seq_scans<R>(f: impl FnOnce(&mut SeqScanState) -> R) -> R {
    // SAFETY: leaf accessors, never re-entered (rule 10 single-entry pattern).
    SEQ_SCANS.with(|c| f(unsafe { &mut *c.get() }))
}

unsafe fn register_seq_scan(hashp: *mut HTAB) -> PgResult<()> {
    let level = xact_seams::get_current_transaction_nest_level::call();
    with_seq_scans(|s| {
        if s.n >= MAX_SEQ_SCANS {
            return Err(Box::new(PgError::error(format!(
                "too many active hash_seq_search scans, cannot start one on \"{}\"",
                tabname_str(hashp)
            ))));
        }
        s.tables[s.n] = hashp as usize;
        s.level[s.n] = level;
        s.n += 1;
        Ok(())
    })
}

unsafe fn deregister_seq_scan(hashp: *mut HTAB) -> PgResult<()> {
    with_seq_scans(|s| {
        let target = hashp as usize;
        for i in (0..s.n).rev() {
            if s.tables[i] == target {
                s.tables[i] = s.tables[s.n - 1];
                s.level[i] = s.level[s.n - 1];
                s.n -= 1;
                return Ok(());
            }
        }
        Err(Box::new(PgError::error(format!(
            "no hash_seq_search scan for hash table \"{}\"",
            tabname_str(hashp)
        ))))
    })
}

#[inline(never)]
unsafe fn has_seq_scans(hashp: *mut HTAB) -> bool {
    let target = hashp as usize;
    with_seq_scans(|s| s.tables[..s.n].contains(&target))
}

pub fn AtEOXact_HashTables(is_commit: bool) {
    let (leaked, nleaked) = with_seq_scans(|s| {
        let snapshot = (s.tables, if is_commit { s.n } else { 0 });
        s.n = 0;
        snapshot
    });
    for p in &leaked[..nleaked] {
        let _ = elog(
            WARNING,
            format!("leaked hash_seq_search scan for hash table {p:#x}"),
        );
    }
}

pub fn AtEOSubXact_HashTables(is_commit: bool, nest_depth: i32) {
    let mut leaked = [0usize; MAX_SEQ_SCANS];
    let mut nleaked = 0usize;
    with_seq_scans(|s| {
        let mut i = s.n;
        while i > 0 {
            i -= 1;
            if s.level[i] >= nest_depth {
                if is_commit {
                    leaked[nleaked] = s.tables[i];
                    nleaked += 1;
                }
                s.tables[i] = s.tables[s.n - 1];
                s.level[i] = s.level[s.n - 1];
                s.n -= 1;
            }
        }
    });
    for p in &leaked[..nleaked] {
        let _ = elog(
            WARNING,
            format!("leaked hash_seq_search scan for hash table {p:#x}"),
        );
    }
}

#[cfg(test)]
mod tests;
