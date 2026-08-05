//! radixtree_diff: differential fuzz driver — shipped Rust
//! crates/backend/lib/radixtree vs vendored PostgreSQL 18.3 (Stamp-18.3,
//! upstream sha 62d6c7d3df) C template src/include/lib/radixtree.h
//! (csrc/pg_radixtree_io.c; verbatim, two instantiations).
//!
//! OP-SEQUENCE-DRIVER shape (the libfam_diff pattern): input bytes decode
//! into a bounded op sequence applied to BOTH the Rust tree and the
//! verbatim-vendored C tree; observable state is compared after every op.
//! Input layout: [selector][payload]; selector % 3 picks the arm; selector
//! bit 7 picks `create` vs `create_in` on the local arms (C's single
//! RT_CREATE maps to both — they differ only in which context owns the
//! leaves). Short reads end the sequence — every input length is valid.
//!
//! Arms:
//!   0: RadixTree<u64>       <-> C rtf_* (RT_VALUE_TYPE = uint64, fixed,
//!      always embedded; test_radixtree.c's TestValueType shape)
//!   1: RadixTree<RtvVal>    <-> C rtv_* (RT_VARLEN_VALUE_SIZE +
//!      RT_RUNTIME_EMBEDDABLE_VALUE; tidstore's BlocktableEntry shape:
//!      first byte's low bit doubles as the embedded pointer tag)
//!   2: SharedRadixTree<RtvVal> <-> C rtv_* — the thread-native stand-in
//!      for C's RT_SHMEM flavor, driven under the C lock discipline
//!      (lock_exclusive for set/delete, lock_share for find/iterate).
//!      Varlen values so the shared store's LEAF alloc/free paths run
//!      (tidstore, the shipped consumer, is exactly shared+varlen; a
//!      fixed-u64 shared arm never allocates a leaf — fleet lcov of the
//!      10M run at be0165bd64 proved free_recurse's leaf free uncovered).
//!
//! Comparison planes (all arms):
//!   - set: "found" verdict (bool)
//!   - find: presence + full value image bytes (fixed: u64 bits; varlen:
//!     header+payload image, length included)
//!   - find-and-overwrite (RT_FIND's documented mutation channel <-> Rust
//!     find_mut / find_ptr write): presence verdict, then the image plane
//!     re-checks the write
//!   - delete: "found" verdict
//!   - num_keys after EVERY op (C side: harness window into ctl->num_keys)
//!   - full iteration: (key, value-image) stream, order included, after an
//!     explicit iterate op and again before every free/recreate
//!   - no-panic / no-UB (ASan on both sides' allocations)
//!
//! DOMAIN CARVES (documented driver fences; the fence models the C caller
//! contract, never pgrust behavior):
//!   - RT_MEMORY_USAGE / memory_usage(): EXECUTED on both sides every
//!     mem-usage op; VALUES not compared — C reports memory-context block
//!     accounting (shimmed as tracked payload bytes), Rust reports
//!     arena_footprint; both are allocator-layout non-surfaces (intset
//!     memory_usage precedent). Both must be nonzero (a live tree always
//!     holds at least its root node) — that much IS asserted.
//!   - C RT_SHMEM flavor: never instantiated (ranking-cell carve). Arm 2
//!     compares the Rust stand-in against the non-shmem template, whose
//!     tree-shape code is byte-identical between flavors.
//!   - varlen payload length <= 100 bytes (size bound only, both sides
//!     identically; embedded/leaf boundary at image size 8 sits well
//!     inside).
//!   - per-exec budgets (identical on both sides, decided by the driver
//!     before touching either tree): <= 384 ops, <= 24576 live-key sets,
//!     <= 1<<17 iterated pairs, <= 6 free/recreate cycles.
//!
//! ERROR PLANE: lib/radixtree.h raises no elog/ereport (asserts only), and
//! the Rust port's only PgResult error is allocator OOM — there is no
//! reachable error surface to compare; Rust set() Err aborts the exec via
//! expect (no-panic plane).

#![allow(dead_code)]

use core::mem::size_of;
use core::ptr::NonNull;

use mcx::MemoryContext;
use radixtree::{RadixTree, RtValue, SharedRadixTree};

extern "C" {
    fn pg_diff_rt_env_reset();
    fn pg_diff_rt_handles_reset();

    fn pg_diff_rtf_create();
    fn pg_diff_rtf_free();
    fn pg_diff_rtf_set(key: u64, val: u64) -> i32;
    fn pg_diff_rtf_find(key: u64, out: *mut u64) -> i32;
    fn pg_diff_rtf_find_set(key: u64, newval: u64) -> i32;
    fn pg_diff_rtf_delete(key: u64) -> i32;
    fn pg_diff_rtf_iter_begin();
    fn pg_diff_rtf_iter_next(key: *mut u64, val: *mut u64) -> i32;
    fn pg_diff_rtf_iter_end();
    fn pg_diff_rtf_memory_usage() -> u64;
    fn pg_diff_rtf_num_keys() -> i64;

    fn pg_diff_rtv_create();
    fn pg_diff_rtv_free();
    fn pg_diff_rtv_set(key: u64, payload: *const u8, len: i32) -> i32;
    fn pg_diff_rtv_find(key: u64, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_rtv_delete(key: u64) -> i32;
    fn pg_diff_rtv_iter_begin();
    fn pg_diff_rtv_iter_next(key: *mut u64, out: *mut u8, outlen: *mut i32) -> i32;
    fn pg_diff_rtv_iter_end();
    fn pg_diff_rtv_memory_usage() -> u64;
    fn pg_diff_rtv_num_keys() -> i64;
}

/// Mirror of the oracle's pg_diff_rtv_val header (csrc/pg_radixtree_io.c):
/// byte-identical image layout on both sides.
#[repr(C)]
#[derive(Clone, Copy)]
struct RtvVal {
    flags: u8, // bit 0 = embedded tag (set iff image size <= 8)
    len: u8,   // payload byte count
}

const RTV_HDR: usize = size_of::<RtvVal>();
const RTV_MAX_LEN: usize = 100;
const RTV_MAX_SIZE: usize = RTV_HDR + RTV_MAX_LEN;

// SAFETY: header prefix of a varlen image; value_size covers header +
// payload; when the image fits a child pointer slot the constructor sets
// flags bit 0 (the RT_RUNTIME_EMBEDDABLE_VALUE tag contract).
unsafe impl RtValue for RtvVal {
    const VARLEN: bool = true;
    const RUNTIME_EMBEDDABLE: bool = true;

    fn value_size(&self) -> usize {
        RTV_HDR + self.len as usize
    }
}

/// Full-size varlen image staging buffer (both sides build the identical
/// image bytes; 8-aligned like C's palloc'd staging).
#[repr(C, align(8))]
struct RtvImage {
    hdr: RtvVal,
    data: [u8; RTV_MAX_LEN],
}

impl RtvImage {
    fn new(payload: &[u8]) -> RtvImage {
        assert!(payload.len() <= RTV_MAX_LEN);
        let size = RTV_HDR + payload.len();
        let mut img = RtvImage {
            hdr: RtvVal {
                flags: if size <= size_of::<usize>() { 1 } else { 0 },
                len: payload.len() as u8,
            },
            data: [0; RTV_MAX_LEN],
        };
        img.data[..payload.len()].copy_from_slice(payload);
        img
    }
}

/// Little-endian byte cursor; a short read ends the op sequence.
struct R<'a> {
    d: &'a [u8],
    i: usize,
}

impl<'a> R<'a> {
    fn u8(&mut self) -> Option<u8> {
        let v = *self.d.get(self.i)?;
        self.i += 1;
        Some(v)
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.d.get(self.i..self.i + n)?;
        self.i += n;
        Some(s)
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// Compact structured key: control byte gives width (1..=8 low bytes)
    /// and an optional byte-granular left shift, so short encodings reach
    /// every tree level (the "poor man's path compression" means level
    /// count tracks the highest set byte).
    fn key(&mut self) -> Option<u64> {
        let ctl = self.u8()?;
        let nb = ((ctl & 7) + 1) as usize;
        let bytes = self.take(nb)?;
        let mut k = 0u64;
        for (i, &b) in bytes.iter().enumerate() {
            k |= (b as u64) << (8 * i);
        }
        if ctl & 8 != 0 {
            k <<= 8 * ((ctl >> 4) & 7);
        }
        Some(k)
    }
}

/// Per-exec budgets (identical for both sides by construction: the driver
/// decides from its own bookkeeping before touching either tree).
struct Budget {
    sets: u32,
    iter_pairs: u32,
    recreates: u32,
}

const MAX_OPS: u32 = 384;
const MAX_SETS: u32 = 24_576;
const MAX_ITER_PAIRS: u32 = 1 << 17;
const MAX_RECREATES: u32 = 6;

pub fn radixtree_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    unsafe {
        pg_diff_rt_env_reset();
        pg_diff_rt_handles_reset();
    }
    let mut r = R { d: payload, i: 0 };
    let in_ctx = sel & 0x80 != 0;
    match sel % 3 {
        0 => fixed_arm(&mut r, in_ctx),
        1 => varlen_arm(&mut r, in_ctx),
        _ => shared_arm(&mut r),
    }
}

fn fixed_val(r: &mut R, key: u64) -> Option<u64> {
    // mode bit: full 8 fuzzer-chosen bytes, or key-derived (cheap encoding;
    // the seed corpus carries the single-byte-difference witness pairs).
    let m = r.u8()?;
    if m & 1 != 0 {
        r.u64()
    } else {
        Some(key.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (m as u64))
    }
}

// ---- arm 0: RadixTree<u64> vs C rtf_* ----

fn fixed_arm(r: &mut R, in_ctx: bool) {
    let cx = MemoryContext::new("radixtree_fuzz");
    let mk = |cx: &MemoryContext| -> RadixTree<u64> {
        if in_ctx {
            RadixTree::create_in(cx.new_child("radixtree_fuzz_leaves"))
        } else {
            RadixTree::create(cx)
        }
        .expect("radixtree create")
    };
    let mut rust = mk(&cx);
    unsafe { pg_diff_rtf_create() };
    let mut b = Budget { sets: 0, iter_pairs: 0, recreates: 0 };

    for _ in 0..MAX_OPS {
        let Some(op) = r.u8() else { break };
        match op % 9 {
            0 => {
                if b.sets >= MAX_SETS {
                    continue;
                }
                b.sets += 1;
                let Some(key) = r.key() else { break };
                let Some(val) = fixed_val(r, key) else { break };
                let rfound = rust.set(key, &val).expect("rust set");
                let cfound = unsafe { pg_diff_rtf_set(key, val) } != 0;
                assert_eq!(rfound, cfound, "set found verdict, key={key:#x}");
            }
            1 => {
                let Some(key) = r.key() else { break };
                let rv = rust.find(key).copied();
                let mut cvv = 0u64;
                let cp = unsafe { pg_diff_rtf_find(key, &mut cvv) } != 0;
                assert_eq!(rv.is_some(), cp, "find presence, key={key:#x}");
                if let Some(rvv) = rv {
                    assert_eq!(rvv, cvv, "find value, key={key:#x}");
                }
            }
            2 => {
                let Some(key) = r.key() else { break };
                let rd = rust.delete(key);
                let cd = unsafe { pg_diff_rtf_delete(key) } != 0;
                assert_eq!(rd, cd, "delete verdict, key={key:#x}");
            }
            3 => {
                // find-and-overwrite: C writes through RT_FIND's pointer,
                // Rust through find_mut.
                let Some(key) = r.key() else { break };
                let Some(nv) = fixed_val(r, key) else { break };
                let rp = match rust.find_mut(key) {
                    Some(vr) => {
                        *vr = nv;
                        true
                    }
                    None => false,
                };
                let cp = unsafe { pg_diff_rtf_find_set(key, nv) } != 0;
                assert_eq!(rp, cp, "find_set presence, key={key:#x}");
            }
            4 => fixed_iterate_compare(&rust, &mut b),
            5 => {
                let rm = rust.memory_usage();
                let cm = unsafe { pg_diff_rtf_memory_usage() };
                // CARVE: allocator-layout non-surface; execute both, only
                // liveness asserted (a tree always holds its root node).
                assert!(rm > 0 && cm > 0, "memory_usage liveness");
            }
            6 => {
                if b.recreates >= MAX_RECREATES {
                    continue;
                }
                b.recreates += 1;
                fixed_iterate_compare(&rust, &mut b);
                rust = mk(&cx);
                unsafe {
                    pg_diff_rtf_free();
                    pg_diff_rtf_create();
                }
            }
            7 => {
                // dense ascending run: the only cheap way to grow node48/
                // node256 and deep-level splits.
                let Some(base) = r.key() else { break };
                let Some(n) = r.u8() else { break };
                let Some(stride_sel) = r.u8() else { break };
                let stride = match stride_sel % 4 {
                    0 => 1u64,
                    1 => 2,
                    2 => 0x100,
                    _ => 0x1_0000_0000,
                };
                for i in 0..n as u64 {
                    if b.sets >= MAX_SETS {
                        break;
                    }
                    b.sets += 1;
                    let key = base.wrapping_add(i.wrapping_mul(stride));
                    let val = key ^ 0x5555_5555_5555_5555;
                    let rfound = rust.set(key, &val).expect("rust set");
                    let cfound = unsafe { pg_diff_rtf_set(key, val) } != 0;
                    assert_eq!(rfound, cfound, "dense set verdict, key={key:#x}");
                }
            }
            _ => {
                // dense delete run (shrink paths node256->48->16->4)
                let Some(base) = r.key() else { break };
                let Some(n) = r.u8() else { break };
                for i in 0..n as u64 {
                    let key = base.wrapping_add(i);
                    let rd = rust.delete(key);
                    let cd = unsafe { pg_diff_rtf_delete(key) } != 0;
                    assert_eq!(rd, cd, "dense delete verdict, key={key:#x}");
                }
            }
        }
        let rn = rust.num_keys();
        let cn = unsafe { pg_diff_rtf_num_keys() };
        assert_eq!(rn, cn, "num_keys after op {op}");
    }
    fixed_iterate_compare(&rust, &mut b);
    drop(rust);
    unsafe { pg_diff_rtf_free() };
}

fn fixed_iterate_compare(rust: &RadixTree<u64>, b: &mut Budget) {
    if b.iter_pairs >= MAX_ITER_PAIRS {
        return;
    }
    let mut it = rust.begin_iterate();
    unsafe { pg_diff_rtf_iter_begin() };
    loop {
        let rn = it.next();
        let mut ck = 0u64;
        let mut cv = 0u64;
        let cn = unsafe { pg_diff_rtf_iter_next(&mut ck, &mut cv) } != 0;
        match rn {
            Some((rk, rv)) => {
                assert!(cn, "iterate: Rust yielded {rk:#x}, C exhausted");
                assert_eq!(rk, ck, "iterate key");
                assert_eq!(*rv, cv, "iterate value, key={rk:#x}");
            }
            None => {
                assert!(!cn, "iterate: C yielded {ck:#x}, Rust exhausted");
                break;
            }
        }
        b.iter_pairs += 1;
        if b.iter_pairs >= MAX_ITER_PAIRS {
            // identical early stop on both sides (budget fence)
            break;
        }
    }
    unsafe { pg_diff_rtf_iter_end() };
}

// ---- arm 1: RadixTree<RtvVal> (varlen, runtime-embeddable) vs C rtv_* ----

/// Read the full image bytes behind a found varlen value.
unsafe fn rtv_image(p: NonNull<RtvVal>) -> Vec<u8> {
    let hdr = *p.as_ptr();
    let size = RTV_HDR + hdr.len as usize;
    let mut out = vec![0u8; size];
    core::ptr::copy_nonoverlapping(p.as_ptr().cast::<u8>(), out.as_mut_ptr(), size);
    out
}

fn varlen_arm(r: &mut R, in_ctx: bool) {
    let cx = MemoryContext::new("radixtree_fuzz");
    let mk = |cx: &MemoryContext| -> RadixTree<RtvVal> {
        if in_ctx {
            RadixTree::create_in(cx.new_child("radixtree_fuzz_leaves"))
        } else {
            RadixTree::create(cx)
        }
        .expect("radixtree create")
    };
    let mut rust = mk(&cx);
    unsafe { pg_diff_rtv_create() };
    let mut b = Budget { sets: 0, iter_pairs: 0, recreates: 0 };
    let mut cbuf = [0u8; RTV_MAX_SIZE];

    for _ in 0..MAX_OPS {
        let Some(op) = r.u8() else { break };
        match op % 8 {
            0 => {
                if b.sets >= MAX_SETS {
                    continue;
                }
                b.sets += 1;
                let Some(key) = r.key() else { break };
                let Some(lb) = r.u8() else { break };
                let len = (lb as usize) % (RTV_MAX_LEN + 1);
                let Some(payload) = r.take(len) else { break };
                let img = RtvImage::new(payload);
                // SAFETY: img is a live staging buffer covering value_size.
                let rfound = unsafe {
                    rust.set_ptr(key, (&img as *const RtvImage).cast::<RtvVal>())
                }
                .expect("rust set");
                let cfound =
                    unsafe { pg_diff_rtv_set(key, payload.as_ptr(), len as i32) } != 0;
                assert_eq!(rfound, cfound, "rtv set verdict, key={key:#x}");
            }
            1 => {
                let Some(key) = r.key() else { break };
                let rp = rust.find_ptr(key);
                let mut clen = 0i32;
                let cp =
                    unsafe { pg_diff_rtv_find(key, cbuf.as_mut_ptr(), &mut clen) } != 0;
                assert_eq!(rp.is_some(), cp, "rtv find presence, key={key:#x}");
                if let Some(p) = rp {
                    // SAFETY: pointer from find_ptr covers the whole image.
                    let rimg = unsafe { rtv_image(p) };
                    assert_eq!(
                        rimg.as_slice(),
                        &cbuf[..clen as usize],
                        "rtv find image, key={key:#x}"
                    );
                }
            }
            2 => {
                let Some(key) = r.key() else { break };
                let rd = rust.delete(key);
                let cd = unsafe { pg_diff_rtv_delete(key) } != 0;
                assert_eq!(rd, cd, "rtv delete verdict, key={key:#x}");
            }
            3 => varlen_iterate_compare(&rust, &mut b, &mut cbuf),
            4 => {
                let rm = rust.memory_usage();
                let cm = unsafe { pg_diff_rtv_memory_usage() };
                assert!(rm > 0 && cm > 0, "rtv memory_usage liveness");
            }
            5 => {
                if b.recreates >= MAX_RECREATES {
                    continue;
                }
                b.recreates += 1;
                varlen_iterate_compare(&rust, &mut b, &mut cbuf);
                rust = mk(&cx);
                unsafe {
                    pg_diff_rtv_free();
                    pg_diff_rtv_create();
                }
            }
            6 => {
                // dense run with per-key length cycling across the
                // embedded/leaf boundary (image sizes 2..=10 around the
                // <=8 embed fence)
                let Some(base) = r.key() else { break };
                let Some(n) = r.u8() else { break };
                for i in 0..n as u64 {
                    if b.sets >= MAX_SETS {
                        break;
                    }
                    b.sets += 1;
                    let key = base.wrapping_add(i);
                    let len = (i % 9) as usize; // 0..=8 payload -> 2..=10 image
                    let payload: Vec<u8> =
                        (0..len).map(|j| (key as u8).wrapping_add(j as u8)).collect();
                    let img = RtvImage::new(&payload);
                    // SAFETY: as above.
                    let rfound = unsafe {
                        rust.set_ptr(key, (&img as *const RtvImage).cast::<RtvVal>())
                    }
                    .expect("rust set");
                    let cfound = unsafe {
                        pg_diff_rtv_set(key, payload.as_ptr(), len as i32)
                    } != 0;
                    assert_eq!(rfound, cfound, "rtv dense set verdict, key={key:#x}");
                }
            }
            _ => {
                let Some(base) = r.key() else { break };
                let Some(n) = r.u8() else { break };
                for i in 0..n as u64 {
                    let key = base.wrapping_add(i);
                    let rd = rust.delete(key);
                    let cd = unsafe { pg_diff_rtv_delete(key) } != 0;
                    assert_eq!(rd, cd, "rtv dense delete verdict, key={key:#x}");
                }
            }
        }
        let rn = rust.num_keys();
        let cn = unsafe { pg_diff_rtv_num_keys() };
        assert_eq!(rn, cn, "rtv num_keys after op {op}");
    }
    varlen_iterate_compare(&rust, &mut b, &mut cbuf);
    drop(rust);
    unsafe { pg_diff_rtv_free() };
}

fn varlen_iterate_compare(rust: &RadixTree<RtvVal>, b: &mut Budget, cbuf: &mut [u8]) {
    if b.iter_pairs >= MAX_ITER_PAIRS {
        return;
    }
    let mut it = rust.begin_iterate();
    unsafe { pg_diff_rtv_iter_begin() };
    loop {
        let rn = it.next_ptr();
        let mut ck = 0u64;
        let mut clen = 0i32;
        let cn = unsafe { pg_diff_rtv_iter_next(&mut ck, cbuf.as_mut_ptr(), &mut clen) }
            != 0;
        match rn {
            Some((rk, rp)) => {
                assert!(cn, "rtv iterate: Rust yielded {rk:#x}, C exhausted");
                assert_eq!(rk, ck, "rtv iterate key");
                // SAFETY: pointer from next_ptr covers the whole image.
                let rimg = unsafe { rtv_image(rp) };
                assert_eq!(
                    rimg.as_slice(),
                    &cbuf[..clen as usize],
                    "rtv iterate image, key={rk:#x}"
                );
            }
            None => {
                assert!(!cn, "rtv iterate: C yielded {ck:#x}, Rust exhausted");
                break;
            }
        }
        b.iter_pairs += 1;
        if b.iter_pairs >= MAX_ITER_PAIRS {
            break;
        }
    }
    unsafe { pg_diff_rtv_iter_end() };
}

// ---- arm 2: SharedRadixTree<RtvVal> vs C rtv_* (C lock discipline) ----

fn shared_arm(r: &mut R) {
    let mut rust: SharedRadixTree<RtvVal> =
        SharedRadixTree::create().expect("shared create");
    unsafe { pg_diff_rtv_create() };
    let mut b = Budget { sets: 0, iter_pairs: 0, recreates: 0 };
    let mut cbuf = [0u8; RTV_MAX_SIZE];

    for _ in 0..MAX_OPS {
        let Some(op) = r.u8() else { break };
        match op % 8 {
            0 => {
                if b.sets >= MAX_SETS {
                    continue;
                }
                b.sets += 1;
                let Some(key) = r.key() else { break };
                let Some(lb) = r.u8() else { break };
                let len = (lb as usize) % (RTV_MAX_LEN + 1);
                let Some(payload) = r.take(len) else { break };
                let img = RtvImage::new(payload);
                // SAFETY: img is a live staging buffer covering value_size.
                let rfound = unsafe {
                    rust.lock_exclusive()
                        .set_ptr(key, (&img as *const RtvImage).cast::<RtvVal>())
                }
                .expect("rust set");
                let cfound =
                    unsafe { pg_diff_rtv_set(key, payload.as_ptr(), len as i32) } != 0;
                assert_eq!(rfound, cfound, "shared set verdict, key={key:#x}");
            }
            1 => {
                let Some(key) = r.key() else { break };
                let g = rust.lock_share();
                let rp = g.find_ptr(key);
                let mut clen = 0i32;
                let cp =
                    unsafe { pg_diff_rtv_find(key, cbuf.as_mut_ptr(), &mut clen) } != 0;
                assert_eq!(rp.is_some(), cp, "shared find presence, key={key:#x}");
                if let Some(p) = rp {
                    // SAFETY: pointer from find_ptr covers the whole image;
                    // the share guard is live for the read.
                    let rimg = unsafe { rtv_image(p) };
                    assert_eq!(
                        rimg.as_slice(),
                        &cbuf[..clen as usize],
                        "shared find image, key={key:#x}"
                    );
                }
            }
            2 => {
                let Some(key) = r.key() else { break };
                let rd = rust.lock_exclusive().delete(key);
                let cd = unsafe { pg_diff_rtv_delete(key) } != 0;
                assert_eq!(rd, cd, "shared delete verdict, key={key:#x}");
            }
            3 => shared_iterate_compare(&rust, &mut b, &mut cbuf),
            4 => {
                let rm = rust.memory_usage();
                let cm = unsafe { pg_diff_rtv_memory_usage() };
                assert!(rm > 0 && cm > 0, "shared memory_usage liveness");
            }
            5 => {
                if b.recreates >= MAX_RECREATES {
                    continue;
                }
                b.recreates += 1;
                shared_iterate_compare(&rust, &mut b, &mut cbuf);
                // drop with live keys = C RT_FREE's RT_FREE_RECURSE walk
                // (node + leaf frees) on the recursive-free store
                rust = SharedRadixTree::create().expect("shared create");
                unsafe {
                    pg_diff_rtv_free();
                    pg_diff_rtv_create();
                }
            }
            6 => {
                let Some(base) = r.key() else { break };
                let Some(n) = r.u8() else { break };
                let mut g = rust.lock_exclusive();
                for i in 0..n as u64 {
                    if b.sets >= MAX_SETS {
                        break;
                    }
                    b.sets += 1;
                    let key = base.wrapping_add(i);
                    let len = (i % 13) as usize; // 0..=12 payload straddles the embed fence
                    let payload: Vec<u8> =
                        (0..len).map(|j| (key as u8).wrapping_add(j as u8)).collect();
                    let img = RtvImage::new(&payload);
                    // SAFETY: as above.
                    let rfound = unsafe {
                        g.set_ptr(key, (&img as *const RtvImage).cast::<RtvVal>())
                    }
                    .expect("rust set");
                    let cfound = unsafe {
                        pg_diff_rtv_set(key, payload.as_ptr(), len as i32)
                    } != 0;
                    assert_eq!(rfound, cfound, "shared dense set verdict, key={key:#x}");
                }
            }
            _ => {
                let Some(base) = r.key() else { break };
                let Some(n) = r.u8() else { break };
                let mut g = rust.lock_exclusive();
                for i in 0..n as u64 {
                    let key = base.wrapping_add(i);
                    let rd = g.delete(key);
                    let cd = unsafe { pg_diff_rtv_delete(key) } != 0;
                    assert_eq!(rd, cd, "shared dense delete verdict, key={key:#x}");
                }
            }
        }
        let rn = rust.lock_share().num_keys();
        let cn = unsafe { pg_diff_rtv_num_keys() };
        assert_eq!(rn, cn, "shared num_keys after op {op}");
    }
    shared_iterate_compare(&rust, &mut b, &mut cbuf);
    // final drop happens with whatever keys remain live — the recursive
    // free path (nodes AND single-value leaves) runs every exec
    drop(rust);
    unsafe { pg_diff_rtv_free() };
}

fn shared_iterate_compare(rust: &SharedRadixTree<RtvVal>, b: &mut Budget, cbuf: &mut [u8]) {
    if b.iter_pairs >= MAX_ITER_PAIRS {
        return;
    }
    let g = rust.lock_share();
    let mut it = g.begin_iterate();
    unsafe { pg_diff_rtv_iter_begin() };
    loop {
        let rn = it.next_ptr();
        let mut ck = 0u64;
        let mut clen = 0i32;
        let cn = unsafe { pg_diff_rtv_iter_next(&mut ck, cbuf.as_mut_ptr(), &mut clen) }
            != 0;
        match rn {
            Some((rk, rp)) => {
                assert!(cn, "shared iterate: Rust yielded {rk:#x}, C exhausted");
                assert_eq!(rk, ck, "shared iterate key");
                // SAFETY: pointer from next_ptr covers the whole image.
                let rimg = unsafe { rtv_image(rp) };
                assert_eq!(
                    rimg.as_slice(),
                    &cbuf[..clen as usize],
                    "shared iterate image, key={rk:#x}"
                );
            }
            None => {
                assert!(!cn, "shared iterate: C yielded {ck:#x}, Rust exhausted");
                break;
            }
        }
        b.iter_pairs += 1;
        if b.iter_pairs >= MAX_ITER_PAIRS {
            break;
        }
    }
    unsafe { pg_diff_rtv_iter_end() };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic replay helper: run one input end-to-end. With
    /// RT_EMIT_CORPUS=<dir> set, also bank the input as a seed (the
    /// directed witnesses below ARE the seed corpus — boundary seeds by
    /// construction, per the exec-floors-never-witness-boundaries law).
    fn run(bytes: &[u8]) {
        if let Ok(dir) = std::env::var("RT_EMIT_CORPUS") {
            use std::io::Write;
            let mut h = std::collections::hash_map::DefaultHasher::new();
            std::hash::Hash::hash(bytes, &mut h);
            let name = format!("{}/seed-{:016x}", dir, std::hash::Hasher::finish(&h));
            let mut f = std::fs::File::create(name).unwrap();
            f.write_all(bytes).unwrap();
        }
        radixtree_diff(bytes);
    }

    /// Local harness soak: 50k structured-random inputs (stable-build
    /// smoke of every plane; the 10M floor runs on the fleet).
    #[test]
    #[ignore = "run explicitly: cargo test -- --ignored radixtree soak"]
    fn local_soak_50k() {
        let mut rng = crate::radixtree_diff::tests::SplitMix64(0x5eed_cafe);
        for i in 0..50_000u64 {
            let len = 1 + (rng.next() % 200) as usize;
            let mut inp = Vec::with_capacity(len);
            for _ in 0..len {
                inp.push(rng.next() as u8);
            }
            // steer the selector across arms deterministically
            inp[0] = (i % 6) as u8 | (((i / 6) % 2) as u8) << 7;
            radixtree_diff(&inp);
        }
    }

    pub(super) struct SplitMix64(pub u64);

    impl SplitMix64 {
        pub fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }

    #[test]
    fn smoke_all_arms_empty() {
        for sel in 0..=255u8 {
            run(&[sel]);
        }
    }

    /// Single-field-difference WITNESS pairs (seeding obligation): keys
    /// differing in exactly one chunk byte per level, values differing in
    /// exactly one byte — each must round-trip through set/find/iterate
    /// with the difference observed on both sides.
    #[test]
    fn witness_single_chunk_and_value_byte() {
        for level in 0..8u8 {
            for sel in [0u8, 2u8] {
                // set base key, set key-with-one-chunk-bumped, find both,
                // iterate. key spec: ctl = 8 | (level<<4), 1 byte.
                let mut inp = vec![sel];
                for kb in [0x41u8, 0x42u8] {
                    inp.push(0); // op: set
                    inp.push(8 | (level << 4)); // ctl: 1 byte, shifted
                    inp.push(kb);
                    inp.push(1); // val mode: explicit
                    inp.extend_from_slice(&(0xDEAD_0000u64 + kb as u64).to_le_bytes());
                }
                for kb in [0x41u8, 0x42u8, 0x43u8] {
                    inp.push(1); // op: find
                    inp.push(8 | (level << 4));
                    inp.push(kb);
                }
                inp.push(4); // op: iterate (arm0) / mem (arm2: %8==4 -> mem)
                run(&inp);
            }
        }
    }

    /// Value byte-shift witnesses for the varlen arm: same key, images
    /// differing in exactly one payload byte / one length step across the
    /// embedded/leaf boundary (image sizes 7, 8, 9).
    #[test]
    fn witness_varlen_boundary() {
        for len in [5u8, 6, 7, 8, 20] {
            for tweak in 0..2u8 {
                let mut inp = vec![1u8]; // varlen arm
                inp.push(0); // set
                inp.push(0); // ctl: 1-byte key
                inp.push(0x33);
                inp.push(len);
                for j in 0..len {
                    inp.push(if j == 2 && tweak == 1 { 0xFF } else { j });
                }
                inp.push(1); // find
                inp.push(0);
                inp.push(0x33);
                inp.push(3); // iterate
                run(&inp);
            }
        }
    }

    /// Shared-arm leaf lifecycle witness: non-embeddable images (leaves)
    /// live at drop time, so the recursive-free store's node AND leaf free
    /// paths run (the tidstore shape; closes the fleet-lcov line-1567 gap).
    #[test]
    fn witness_shared_leaf_drop() {
        let mut inp = vec![2u8]; // shared arm
        for kb in [0x10u8, 0x20, 0x30] {
            inp.push(0); // set
            inp.push(0); // ctl: 1-byte key
            inp.push(kb);
            inp.push(20); // 20-byte payload -> 22-byte image -> LEAF
            for j in 0..20u8 {
                inp.push(kb.wrapping_add(j));
            }
        }
        inp.push(3); // iterate (image plane over leaves)
        inp.push(5); // recreate: drops the tree with 3 live leaves
        // repopulate one embedded + one leaf, then end-of-exec drop
        inp.push(0);
        inp.push(0);
        inp.push(0x44);
        inp.push(2); // embedded (4-byte image)
        inp.push(1);
        inp.push(2);
        inp.push(0);
        inp.push(0);
        inp.push(0x55);
        inp.push(30); // leaf
        for j in 0..30u8 {
            inp.push(j);
        }
        run(&inp);
    }

    /// Node-kind ladder: dense runs grow 4 -> 16 -> 48 -> 256, then dense
    /// deletes shrink back; iterate after each phase.
    #[test]
    fn node_kind_ladder() {
        for sel in [0u8, 1u8, 2u8] {
            let mut inp = vec![sel];
            let (dense, ddel, iter) = match sel % 3 {
                0 => (7u8, 8u8, 4u8),
                1 => (6, 7, 3),
                _ => (6, 7, 3),
            };
            for n in [3u8, 14, 40, 200, 255] {
                inp.push(dense);
                inp.push(0); // ctl: 1-byte base key
                inp.push(0);
                inp.push(n);
                if sel % 3 == 0 {
                    inp.push(0); // stride sel -> 1
                }
                inp.push(iter);
            }
            inp.push(ddel);
            inp.push(0);
            inp.push(0);
            inp.push(255);
            inp.push(iter);
            run(&inp);
        }
    }

    /// Out-of-range miss witnesses: delete/find with keys ABOVE the tree's
    /// current max_val (the injected delete-verdict flip on the max_val
    /// fence survived the other directed tests — this one kills it).
    #[test]
    fn witness_miss_above_max_val() {
        for sel in [0u8, 1, 2] {
            let mut inp = vec![sel];
            // one small set so max_val stays at the 1-byte level
            inp.push(0); // set
            inp.push(0); // ctl: 1-byte key
            inp.push(0x07);
            if sel % 3 == 1 {
                inp.push(4); // varlen payload len 4
                inp.extend_from_slice(&[9, 8, 7, 6]);
            } else {
                inp.push(0); // key-derived value
            }
            // delete + find far above max_val (deep keys, tree height 1)
            for kb in [1u8, 0xFF] {
                inp.push(2); // delete
                inp.push(8 | (7 << 4)); // 1 byte shifted to the top level
                inp.push(kb);
                inp.push(1); // find
                inp.push(8 | (7 << 4));
                inp.push(kb);
            }
            run(&inp);
        }
    }

    /// INT boundaries as key seeds: 0, u32::MAX +/- 1, u64::MAX (top of the
    /// key space = deepest tree).
    #[test]
    fn boundary_keys() {
        for sel in [0u8, 1, 2] {
            let mut inp = vec![sel];
            for key in [0u64, 1, 0xFF, 0x100, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX - 1, u64::MAX] {
                inp.push(0); // set
                inp.push(7); // ctl: 8 raw bytes
                inp.extend_from_slice(&key.to_le_bytes());
                if sel % 3 != 0 {
                    inp.push(9); // varlen: 9-byte payload (leaf)
                    inp.extend_from_slice(&key.to_le_bytes());
                    inp.push(0xEE);
                } else {
                    inp.push(1); // explicit value
                    inp.extend_from_slice(&key.wrapping_mul(3).to_le_bytes());
                }
            }
            inp.push(if sel % 3 == 0 { 4 } else { 3 }); // iterate
            for key in [0u64, u32::MAX as u64, u64::MAX] {
                inp.push(2); // delete
                inp.push(7);
                inp.extend_from_slice(&key.to_le_bytes());
            }
            run(&inp);
        }
    }
}
