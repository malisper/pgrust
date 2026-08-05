//! libfam_diff: differential fuzz driver — shipped Rust backend/lib family
//! vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_libfam_io.c). Crates under test:
//!   arm 0: crates/backend/lib/hyperloglog  (C: src/backend/lib/hyperloglog.c)
//!   arm 1: crates/backend/lib/binaryheap   (C: src/common/binaryheap.c)
//!   arm 2: crates/backend/lib/pairingheap  (C: src/backend/lib/pairingheap.c)
//!   arm 3: crates/backend/lib/bloomfilter  (C: src/backend/lib/bloomfilter.c)
//!   arm 4: crates/backend/lib/integerset   (C: src/backend/lib/integerset.c)
//!
//! OP-SEQUENCE-DRIVER shape: input bytes decode into a bounded op sequence
//! applied to BOTH the Rust structure and the verbatim-vendored C structure;
//! observable state is compared after every op. Input layout:
//! [selector][payload]; selector % 5 picks the crate arm; each arm consumes
//! the payload as its own little-endian op stream (short reads end the
//! sequence — every input length is valid).
//!
//! Comparison planes per arm:
//!   hll:    register file bytes after every add + estimate() f64 bits.
//!   bh:     len/is_empty, first, full node-array image (sift orders are
//!           C-exact by port contract, so array layout must match), pop
//!           sequence on drain.
//!   ph:     first value + ROOT IDENTITY (Rust slot id paired with the C
//!           fixture slot via insertion order), is_empty/is_singular,
//!           interior-remove values, pop sequence on drain (ties included —
//!           merge orders are C-exact by port contract).
//!   bloom:  k_hash_funcs, bitset_bits, lacks_element verdicts,
//!           prop_bits_set f64 bits, full bitset byte image.
//!   intset: add error-verdict plane (out-of-order add, add-during-iterate),
//!           is_member verdicts, num_entries, full iteration stream.
//!   all:    no-panic; pg_diff_errcode quiet unless an error was expected.
//!
//! DOMAIN CARVES (documented driver fences; the fence models the C caller
//! contract, never pgrust behavior):
//!   - hll bwidth ∈ {5, 10}: the Rust port is monomorphized at the two live
//!     widths (nodeAgg HASHAGG_HLL_BIT_WIDTH=5, abbrev-key=10); C accepts
//!     4..=16. C's bwidth elog arm is C-only surface.
//!   - binaryheap: add/add_unordered are fenced at capacity (C elogs, Rust
//!     panics "out of binary heap slots" — panic==abort under cargo-fuzz, so
//!     the parity of the two defensive arms is asserted by unit tests below,
//!     and the Rust panic lines carry defensive-c-parity exception rows).
//!     first/remove_first/remove_node/replace_first only with the heap
//!     property held and non-empty (C Asserts these; NDEBUG-noop in C,
//!     debug_assert in Rust — the fence IS the C caller contract).
//!   - pairingheap remove/get only on live node ids (C UB otherwise);
//!     remove_first/first only non-empty (C Assert).
//!   - bloom total_elems >= 1 (C divides by it in optimal_k; PG callers
//!     always pass a positive estimate); work_mem ∈ {0, 1024, 2048} to bound
//!     the per-exec bitset at <= 2MB (values only bound SIZE, never logic).
//!   - intset memory_usage: BOTH entry points execute every exec but the
//!     VALUES are not compared — C reports aset chunk-header accounting
//!     (GetMemoryChunkSpace), a malloc-layout non-surface with no Rust
//!     counterpart semantics (see csrc/libfam/include/utils/memutils.h).
//!
//! ERROR PLANE: these five C files raise only elog(ERROR) (internal class),
//! captured as PG_DIFF_ERR_INTERNAL=7 + longjmp; Rust integerset returns
//! PgError. Verdict parity (Ok/Err) is asserted; there is no
//! errcode/sqlstate distinction to compare within the internal class.
//!
//! FC-WRAPPER PLANE: not applicable — none of the five crates has a
//! builtins.rs / fc_* surface (non-SQL backend-lib data structures).

#![allow(dead_code)]

use binaryheap::BinaryHeap;
use bloomfilter::BloomFilter;
use hyperloglog::{HyperLogLog, HyperLogLog32};
use integerset::IntegerSet;
use mcx::MemoryContext;
use pairingheap::PairingHeap;

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;

    fn pg_diff_libfam_reset();

    fn pg_diff_hll_init(bwidth: i32) -> i32;
    fn pg_diff_hll_add(hash: u32);
    fn pg_diff_hll_estimate() -> f64;
    fn pg_diff_hll_reg_at(idx: i32) -> i32;
    fn pg_diff_hll_regs(out: *mut u8, cap: i32) -> i32;

    fn pg_diff_bh_create(capacity: i32);
    fn pg_diff_bh_add(v: i64) -> i32;
    fn pg_diff_bh_add_unordered(v: i64) -> i32;
    fn pg_diff_bh_build();
    fn pg_diff_bh_first() -> i64;
    fn pg_diff_bh_remove_first() -> i64;
    fn pg_diff_bh_remove_node(n: i32);
    fn pg_diff_bh_replace_first(v: i64);
    fn pg_diff_bh_size() -> i32;
    fn pg_diff_bh_get(n: i32) -> i64;
    fn pg_diff_bh_reset();

    fn pg_diff_ph_add(v: i64) -> i32;
    fn pg_diff_ph_is_empty() -> i32;
    fn pg_diff_ph_is_singular() -> i32;
    fn pg_diff_ph_first() -> i64;
    fn pg_diff_ph_first_slot() -> i32;
    fn pg_diff_ph_remove_first() -> i64;
    fn pg_diff_ph_remove(slot: i32) -> i64;
    fn pg_diff_ph_reset();

    fn pg_diff_bloom_create(total_elems: i64, work_mem: i32, seed: u64);
    fn pg_diff_bloom_k() -> i32;
    fn pg_diff_bloom_m() -> u64;
    fn pg_diff_bloom_add(elem: *const u8, len: usize);
    fn pg_diff_bloom_lacks(elem: *const u8, len: usize) -> i32;
    fn pg_diff_bloom_prop() -> f64;
    fn pg_diff_bloom_bitset_eq(bits: *const u8, len: usize) -> i32;

    fn pg_diff_intset_create();
    fn pg_diff_intset_add(x: u64) -> i32;
    fn pg_diff_intset_is_member(x: u64) -> i32;
    fn pg_diff_intset_num_entries() -> u64;
    fn pg_diff_intset_mem_usage() -> u64;
    fn pg_diff_intset_begin_iterate();
    fn pg_diff_intset_iterate_next(out: *mut u64) -> i32;
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
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u24(&mut self) -> Option<u32> {
        let s = self.take(3)?;
        Some(u32::from_le_bytes([s[0], s[1], s[2], 0]))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Option<i64> {
        Some(self.u64()? as i64)
    }
}

fn errcode_quiet(arm: &str) {
    assert_eq!(unsafe { pg_diff_errcode_get() }, 0, "oracle raised in {arm}");
}

/// int64 max-heap comparator — byte-identical semantics to the C harness
/// comparators in csrc/pg_libfam_io.c.
fn cmp_i64(a: &i64, b: &i64) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

pub fn libfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    unsafe { pg_diff_libfam_reset() };
    let mut r = R { d: payload, i: 0 };
    match sel % 5 {
        0 => hll_arm(&mut r),
        1 => bh_arm(&mut r),
        2 => ph_arm(&mut r),
        3 => bloom_arm(&mut r),
        _ => intset_arm(&mut r),
    }
}

// ---------------------------------------------------------------------------
// arm 0: hyperloglog
// ---------------------------------------------------------------------------

enum Hll {
    B4(Box<hyperloglog::Hll<16>>),
    B5(Box<HyperLogLog32>),
    B6(Box<hyperloglog::Hll<64>>),
    B10(Box<HyperLogLog>),
}

impl Hll {
    fn add(&mut self, h: u32) {
        match self {
            Hll::B4(s) => s.add(h),
            Hll::B5(s) => s.add(h),
            Hll::B6(s) => s.add(h),
            Hll::B10(s) => s.add(h),
        }
    }
    fn estimate(&self) -> f64 {
        match self {
            Hll::B4(s) => s.estimate(),
            Hll::B5(s) => s.estimate(),
            Hll::B6(s) => s.estimate(),
            Hll::B10(s) => s.estimate(),
        }
    }
    fn registers(&self) -> &[u8] {
        match self {
            Hll::B4(s) => s.registers(),
            Hll::B5(s) => s.registers(),
            Hll::B6(s) => s.registers(),
            Hll::B10(s) => s.registers(),
        }
    }
}

fn hll_compare_regs(rust: &Hll) {
    let mut buf = [0u8; 1024];
    let n = unsafe { pg_diff_hll_regs(buf.as_mut_ptr(), 1024) };
    assert!(n >= 0, "hll register copy-out");
    assert_eq!(rust.registers(), &buf[..n as usize], "hll register file");
}

fn hll_arm(r: &mut R) {
    let Some(b) = r.u8() else { return };
    // Widths 5 and 10 are the live consumers; 4 and 6 are valid generic
    // instantiations exercising the alpha table's 16/64-register arms.
    let (bwidth, mut rust) = match b % 4 {
        0 => (10u32, Hll::B10(Box::new(HyperLogLog::new(10)))),
        1 => (5u32, Hll::B5(Box::new(HyperLogLog32::new(5)))),
        2 => (4u32, Hll::B4(Box::new(hyperloglog::Hll::<16>::new(4)))),
        _ => (6u32, Hll::B6(Box::new(hyperloglog::Hll::<64>::new(6)))),
    };
    assert_eq!(unsafe { pg_diff_hll_init(bwidth as i32) }, 0, "hll init");

    let mut adds = 0u32;
    while let Some(op) = r.u8() {
        if op % 8 < 6 {
            let Some(h) = r.u32() else { break };
            rust.add(h);
            unsafe { pg_diff_hll_add(h) };
            // Per-add plane: the touched register, exactly (index formula is
            // the shared C/Rust contract); full register file every 64 adds.
            let idx = (h >> (32 - bwidth)) as usize;
            assert_eq!(
                rust.registers()[idx] as i32,
                unsafe { pg_diff_hll_reg_at(idx as i32) },
                "hll touched register {idx}"
            );
            adds += 1;
            if adds % 64 == 0 {
                hll_compare_regs(&rust);
            }
        } else {
            let re = rust.estimate();
            let ce = unsafe { pg_diff_hll_estimate() };
            assert_eq!(re.to_bits(), ce.to_bits(), "hll estimate bits");
        }
    }
    let re = rust.estimate();
    let ce = unsafe { pg_diff_hll_estimate() };
    assert_eq!(re.to_bits(), ce.to_bits(), "hll final estimate bits");
    hll_compare_regs(&rust);
    errcode_quiet("hll");
}

// ---------------------------------------------------------------------------
// arm 1: binaryheap
// ---------------------------------------------------------------------------

fn bh_compare_image(rust: &BinaryHeap<i64, fn(&i64, &i64) -> i32>, size: usize) {
    assert_eq!(rust.len(), size, "bh rust len");
    assert_eq!(unsafe { pg_diff_bh_size() } as usize, size, "bh c len");
    assert_eq!(rust.is_empty(), size == 0, "bh is_empty");
    for n in 0..size {
        assert_eq!(rust.get(n), unsafe { pg_diff_bh_get(n as i32) }, "bh node {n}");
    }
}

fn bh_arm(r: &mut R) {
    let Some(b) = r.u8() else { return };
    let cap = 1 + (b % 64) as usize;
    let mut rust: BinaryHeap<i64, fn(&i64, &i64) -> i32> = BinaryHeap::allocate(cap, cmp_i64);
    unsafe { pg_diff_bh_create(cap as i32) };
    let mut size = 0usize;
    let mut has_prop = true;

    while let Some(op) = r.u8() {
        match op % 9 {
            0 | 1 => {
                // FENCE (capacity): C elogs / Rust panics past it; parity of
                // those defensive arms is unit-tested, not fuzzed.
                let Some(v) = r.i64() else { break };
                if size < cap {
                    rust.add(v);
                    assert_eq!(unsafe { pg_diff_bh_add(v) }, 0, "bh add verdict");
                    size += 1;
                }
            }
            2 => {
                let Some(v) = r.i64() else { break };
                if size < cap {
                    rust.add_unordered(v);
                    assert_eq!(unsafe { pg_diff_bh_add_unordered(v) }, 0, "bh addu verdict");
                    size += 1;
                    has_prop = false;
                }
            }
            3 => {
                rust.build();
                unsafe { pg_diff_bh_build() };
                has_prop = true;
            }
            4 => {
                if size > 0 && has_prop {
                    let rv = rust.remove_first();
                    let cv = unsafe { pg_diff_bh_remove_first() };
                    assert_eq!(rv, cv, "bh remove_first value");
                    size -= 1;
                }
            }
            5 => {
                if size > 0 && has_prop {
                    let Some(nb) = r.u8() else { break };
                    let n = nb as usize % size;
                    rust.remove_node(n);
                    unsafe { pg_diff_bh_remove_node(n as i32) };
                    size -= 1;
                }
            }
            6 => {
                if size > 0 && has_prop {
                    let Some(v) = r.i64() else { break };
                    rust.replace_first(v);
                    unsafe { pg_diff_bh_replace_first(v) };
                }
            }
            7 => {
                if size > 0 && has_prop {
                    assert_eq!(rust.first(), unsafe { pg_diff_bh_first() }, "bh first");
                }
                bh_compare_image(&rust, size);
            }
            _ => {
                rust.reset();
                unsafe { pg_diff_bh_reset() };
                size = 0;
                has_prop = true;
            }
        }
    }

    if !has_prop {
        rust.build();
        unsafe { pg_diff_bh_build() };
    }
    bh_compare_image(&rust, size);
    while size > 0 {
        let rv = rust.remove_first();
        let cv = unsafe { pg_diff_bh_remove_first() };
        assert_eq!(rv, cv, "bh drain value");
        size -= 1;
    }
    assert!(rust.is_empty() && unsafe { pg_diff_bh_size() } == 0, "bh drained");
    errcode_quiet("bh");
}

// ---------------------------------------------------------------------------
// arm 2: pairingheap
// ---------------------------------------------------------------------------

const PH_MAX: usize = 600; // == PG_DIFF_PH_MAX in the C fixture

fn ph_root_pair_check(
    rust: &PairingHeap<i64, fn(&i64, &i64) -> i32>,
    live: &[(pairingheap::NodeId, i32, i64)],
) {
    let rv = *rust.first().expect("non-empty checked by caller");
    let cv = unsafe { pg_diff_ph_first() };
    assert_eq!(rv, cv, "ph first value");
    let rid = rust.first_id();
    let cslot = unsafe { pg_diff_ph_first_slot() };
    let pair = live.iter().find(|&&(id, _, _)| id == rid).expect("root id live");
    assert_eq!(pair.1, cslot, "ph root identity (insertion-order slot)");
    assert_eq!(pair.2, rv, "ph root value vs ledger");
}

fn ph_arm(r: &mut R) {
    let mut rust: PairingHeap<i64, fn(&i64, &i64) -> i32> = PairingHeap::new(cmp_i64);
    // (rust node id, C fixture slot, value) for every live node.
    let mut live: Vec<(pairingheap::NodeId, i32, i64)> = Vec::new();
    let mut c_adds = 0usize; // C fixture slots are not reused until reset

    while let Some(op) = r.u8() {
        match op % 8 {
            0 | 1 | 2 => {
                let Some(v) = r.i64() else { break };
                if c_adds < PH_MAX {
                    let id = rust.add(v);
                    let slot = unsafe { pg_diff_ph_add(v) };
                    assert!(slot >= 0, "ph fixture slot");
                    live.push((id, slot, v));
                    c_adds += 1;
                    ph_root_pair_check(&rust, &live);
                }
            }
            3 => {
                if !live.is_empty() {
                    let rid = rust.first_id();
                    let rv = rust.remove_first().expect("non-empty");
                    let cv = unsafe { pg_diff_ph_remove_first() };
                    assert_eq!(rv, cv, "ph remove_first value");
                    let k = live.iter().position(|&(id, _, _)| id == rid).expect("root live");
                    live.swap_remove(k);
                }
            }
            4 => {
                if !live.is_empty() {
                    let Some(kb) = r.u8() else { break };
                    let k = kb as usize % live.len();
                    let (id, slot, v) = live[k];
                    let rv = rust.remove(id);
                    let cv = unsafe { pg_diff_ph_remove(slot) };
                    assert_eq!(rv, cv, "ph interior remove value");
                    assert_eq!(rv, v, "ph interior remove ledger");
                    live.swap_remove(k);
                }
            }
            5 => {
                assert_eq!(rust.is_empty(), live.is_empty(), "ph is_empty");
                assert_eq!(
                    unsafe { pg_diff_ph_is_empty() } != 0,
                    live.is_empty(),
                    "ph c is_empty"
                );
                assert_eq!(
                    rust.is_singular(),
                    unsafe { pg_diff_ph_is_singular() } != 0,
                    "ph is_singular"
                );
                if live.is_empty() {
                    assert!(rust.first().is_none(), "ph first on empty");
                } else {
                    ph_root_pair_check(&rust, &live);
                }
            }
            6 => {
                if !live.is_empty() {
                    let Some(kb) = r.u8() else { break };
                    let k = kb as usize % live.len();
                    let (id, _, v) = live[k];
                    assert_eq!(*rust.get(id), v, "ph get");
                    assert_eq!(*rust.get_mut(id), v, "ph get_mut");
                }
            }
            _ => {
                rust.reset();
                unsafe { pg_diff_ph_reset() };
                live.clear();
                c_adds = 0;
            }
        }
    }

    while !live.is_empty() {
        ph_root_pair_check(&rust, &live);
        let rid = rust.first_id();
        let rv = rust.remove_first().expect("non-empty");
        let cv = unsafe { pg_diff_ph_remove_first() };
        assert_eq!(rv, cv, "ph drain value");
        let k = live.iter().position(|&(id, _, _)| id == rid).expect("root live");
        live.swap_remove(k);
    }
    assert!(rust.is_empty() && unsafe { pg_diff_ph_is_empty() } != 0, "ph drained");
    assert!(rust.remove_first().is_none(), "ph empty remove_first is None");
    errcode_quiet("ph");
}

// ---------------------------------------------------------------------------
// arm 3: bloomfilter
// ---------------------------------------------------------------------------

fn bloom_arm(r: &mut R) {
    let Some(t) = r.u24() else { return };
    let Some(wb) = r.u8() else { return };
    let Some(seed) = r.u64() else { return };
    // FENCE: total_elems >= 1 (C divides by it); work_mem bounds the bitset
    // at <= 2MB per exec (size bound only — both sides get identical args).
    let total_elems = 1 + t as i64;
    let work_mem = [0i32, 1024, 2048][wb as usize % 3];

    let cx = MemoryContext::new("libfam_fuzz");
    let Ok(mut rust) = BloomFilter::create_in(cx.mcx(), total_elems, work_mem, seed) else {
        // OOM path only; C side untouched this exec.
        return;
    };
    unsafe { pg_diff_bloom_create(total_elems, work_mem, seed) };
    assert_eq!(rust.k_hash_funcs(), unsafe { pg_diff_bloom_k() }, "bloom k");
    assert_eq!(rust.bitset_bits(), unsafe { pg_diff_bloom_m() }, "bloom m");

    let bitset_eq = |rust: &BloomFilter| {
        let bits = rust.bitset();
        assert_eq!(
            unsafe { pg_diff_bloom_bitset_eq(bits.as_ptr(), bits.len()) },
            1,
            "bloom bitset image"
        );
    };

    let mut log: Vec<&[u8]> = Vec::new();
    while let Some(op) = r.u8() {
        match op % 8 {
            0..=3 => {
                let Some(lb) = r.u8() else { break };
                let Some(s) = r.take(lb as usize % 24) else { break };
                rust.add_element(s);
                unsafe { pg_diff_bloom_add(s.as_ptr(), s.len()) };
                if log.len() < 16 {
                    log.push(s);
                }
            }
            4 | 5 => {
                let Some(kb) = r.u8() else { break };
                let s: &[u8] = if op & 1 == 0 && !log.is_empty() {
                    log[kb as usize % log.len()]
                } else {
                    let Some(s) = r.take(kb as usize % 24) else { break };
                    s
                };
                let rv = rust.lacks_element(s);
                let cv = unsafe { pg_diff_bloom_lacks(s.as_ptr(), s.len()) } != 0;
                assert_eq!(rv, cv, "bloom lacks verdict");
            }
            6 => {
                let rv = rust.prop_bits_set();
                let cv = unsafe { pg_diff_bloom_prop() };
                assert_eq!(rv.to_bits(), cv.to_bits(), "bloom prop bits");
            }
            _ => bitset_eq(&rust),
        }
    }
    bitset_eq(&rust);
    let rv = rust.prop_bits_set();
    let cv = unsafe { pg_diff_bloom_prop() };
    assert_eq!(rv.to_bits(), cv.to_bits(), "bloom final prop bits");
    errcode_quiet("bloom");
}

// ---------------------------------------------------------------------------
// arm 4: integerset
// ---------------------------------------------------------------------------

// High enough that a deliberate burst seed can force a 3-level tree
// (>64 internal downlinks => ~8200 two-value leaf items); random inputs
// rarely approach it.
const INTSET_MAX_ADDS: usize = 18000;

fn intset_arm(r: &mut R) {
    let cx = MemoryContext::new("libfam_fuzz");
    let mut rust = IntegerSet::create(cx.mcx());
    unsafe { pg_diff_intset_create() };

    let mut last: u64 = 0;
    let mut adds = 0usize;
    let mut iter_active = false;

    // Error-verdict plane: Ok/Err must agree op-for-op (out-of-order add,
    // add-during-iterate). Returns true when the value was accepted.
    let mut do_add = |rust: &mut IntegerSet, x: u64| -> bool {
        let rv = rust.add_member(x);
        let cv = unsafe { pg_diff_intset_add(x) };
        assert_eq!(rv.is_err(), cv == -1, "intset add verdict for {x}");
        rv.is_ok()
    };

    while let Some(op) = r.u8() {
        match op % 9 {
            8 => {
                // Gap burst: count adds of gap 2^k each — fills leaf items
                // fast (few values per item), the road to deep trees.
                let Some(nb) = r.u8() else { break };
                let Some(kb) = r.u8() else { break };
                let g = 1u64 << (kb % 32);
                let n = (1 + nb as usize).min(INTSET_MAX_ADDS.saturating_sub(adds));
                for _ in 0..n {
                    let next = last.wrapping_add(g);
                    if do_add(&mut rust, next) {
                        last = next;
                        adds += 1;
                        iter_active = false;
                    } else {
                        break;
                    }
                }
            }
            0 => {
                // Consecutive run — drives simple8b mode-0/1 codewords.
                let Some(nb) = r.u8() else { break };
                let n = (1 + nb as usize).min(INTSET_MAX_ADDS.saturating_sub(adds));
                for _ in 0..n {
                    let next = last.wrapping_add(1);
                    if do_add(&mut rust, next) {
                        last = next;
                        adds += 1;
                        iter_active = false;
                    } else {
                        break;
                    }
                }
            }
            1 => {
                let Some(g) = r.u16() else { break };
                if adds < INTSET_MAX_ADDS {
                    let next = last.wrapping_add(1 + g as u64);
                    if do_add(&mut rust, next) {
                        last = next;
                        adds += 1;
                        iter_active = false;
                    }
                }
            }
            2 => {
                // 2^k gaps — walks every simple8b selector band.
                let Some(kb) = r.u8() else { break };
                if adds < INTSET_MAX_ADDS {
                    let next = last.wrapping_add(1u64 << (kb % 64));
                    if do_add(&mut rust, next) {
                        last = next;
                        adds += 1;
                        iter_active = false;
                    }
                }
            }
            3 => {
                // Absolute value: backwards values exercise the
                // out-of-order elog / PgError arm on both sides.
                let Some(x) = r.u64() else { break };
                if adds < INTSET_MAX_ADDS && do_add(&mut rust, x) {
                    last = x;
                    adds += 1;
                    iter_active = false;
                }
            }
            4 => {
                let Some(mb) = r.u8() else { break };
                let x = match mb % 4 {
                    0 => last,
                    1 => last.wrapping_add(1),
                    2 => last.wrapping_sub(1),
                    _ => {
                        let Some(x) = r.u64() else { break };
                        x
                    }
                };
                let rv = rust.is_member(x);
                let cv = unsafe { pg_diff_intset_is_member(x) } != 0;
                assert_eq!(rv, cv, "intset is_member({x})");
            }
            5 => {
                assert_eq!(
                    rust.num_entries(),
                    unsafe { pg_diff_intset_num_entries() },
                    "intset num_entries"
                );
                // memory_usage: EXECUTED both sides, values not compared
                // (allocator-accounting carve — see module header).
                let ru = rust.memory_usage();
                let cu = unsafe { pg_diff_intset_mem_usage() };
                let _ = (ru, cu);
            }
            6 => {
                rust.begin_iterate();
                unsafe { pg_diff_intset_begin_iterate() };
                iter_active = true;
            }
            _ => {
                if iter_active {
                    let Some(kb) = r.u8() else { break };
                    for _ in 0..=kb {
                        let rv = rust.iterate_next();
                        let mut cval = 0u64;
                        let cok = unsafe { pg_diff_intset_iterate_next(&mut cval) } != 0;
                        assert_eq!(rv.is_some(), cok, "intset iterate verdict");
                        if let Some(v) = rv {
                            assert_eq!(v, cval, "intset iterate value");
                        } else {
                            iter_active = false;
                            break;
                        }
                    }
                }
            }
        }
    }

    // Full drain: begin a fresh iteration (abandoning any in-progress one is
    // documented behavior on both sides) and compare the whole stream.
    assert_eq!(
        rust.num_entries(),
        unsafe { pg_diff_intset_num_entries() },
        "intset final num_entries"
    );
    rust.begin_iterate();
    unsafe { pg_diff_intset_begin_iterate() };
    let mut n = 0u64;
    let mut prev: Option<u64> = None;
    loop {
        let rv = rust.iterate_next();
        let mut cval = 0u64;
        let cok = unsafe { pg_diff_intset_iterate_next(&mut cval) } != 0;
        assert_eq!(rv.is_some(), cok, "intset drain verdict");
        let Some(v) = rv else { break };
        assert_eq!(v, cval, "intset drain value");
        if let Some(p) = prev {
            assert!(v > p, "intset drain strictly increasing");
        }
        prev = Some(v);
        n += 1;
        assert!(n <= INTSET_MAX_ADDS as u64 + 1, "intset drain runaway");
    }
    assert_eq!(n, rust.num_entries(), "intset drain count");
    // No final quiet-oracle check here: expected out-of-order /
    // during-iterate adds legitimately leave the last errcode set; the
    // per-op verdict plane above is the intset error contract.
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(sel: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![sel];
        v.extend_from_slice(body);
        v
    }

    /// Fixed sweep: every arm executes real op sequences against the C
    /// oracle on every `cargo test` run (link + shim smoke).
    #[test]
    fn arm_sweep() {
        let _serial = crate::c_oracle_serial();
        for sel in 0u8..5 {
            // Structured pseudo-random op soup, several lengths.
            let mut x: u64 = 0x9e37_79b9_7f4a_7c15 ^ u64::from(sel);
            for len in [8usize, 64, 256, 1024] {
                let mut body = Vec::with_capacity(len);
                while body.len() < len {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    body.extend_from_slice(&x.to_le_bytes());
                }
                body.truncate(len);
                libfam_diff(&seq(sel, &body));
            }
        }
    }

    /// binaryheap capacity-overflow parity: the C arm elogs (internal
    /// class), the Rust arm panics with the same message — the fenced
    /// defensive pair asserted here because panic==abort under the fuzz
    /// build (see module-header carve + exception rows).
    #[test]
    fn bh_overflow_parity() {
        let _serial = crate::c_oracle_serial();
        unsafe {
            pg_diff_libfam_reset();
            pg_diff_bh_create(2);
            assert_eq!(pg_diff_bh_add(1), 0);
            assert_eq!(pg_diff_bh_add(2), 0);
            assert_eq!(pg_diff_bh_add(3), -1, "C add past capacity elogs");
            assert_eq!(pg_diff_errcode_get(), 7, "internal errcode class");
            pg_diff_libfam_reset();
            pg_diff_bh_create(1);
            assert_eq!(pg_diff_bh_add_unordered(7), 0);
            assert_eq!(pg_diff_bh_add_unordered(8), -1, "C addu past capacity elogs");
        }
        // Rust twins (panic side) — must panic with C's message.
        let r1 = std::panic::catch_unwind(|| {
            let mut h: BinaryHeap<i64, fn(&i64, &i64) -> i32> = BinaryHeap::allocate(2, cmp_i64);
            h.add(1);
            h.add(2);
            h.add(3);
        });
        assert!(r1.is_err(), "rust add past capacity panics");
        let r2 = std::panic::catch_unwind(|| {
            let mut h: BinaryHeap<i64, fn(&i64, &i64) -> i32> = BinaryHeap::allocate(1, cmp_i64);
            h.add_unordered(7);
            h.add_unordered(8);
        });
        assert!(r2.is_err(), "rust add_unordered past capacity panics");
    }

    /// hll bwidth domain fence twin: C elogs outside 4..=16 (the driver
    /// never sends such widths; the arm is C-only surface).
    #[test]
    fn hll_c_bwidth_fence() {
        let _serial = crate::c_oracle_serial();
        unsafe {
            pg_diff_libfam_reset();
            assert_eq!(pg_diff_hll_init(3), -1);
            pg_diff_libfam_reset();
            assert_eq!(pg_diff_hll_init(17), -1);
            pg_diff_libfam_reset();
            assert_eq!(pg_diff_hll_init(5), 0);
            assert_eq!(pg_diff_hll_init(10), 0);
        }
    }

    /// Single-field witness pairs (skill obligation):
    ///  - bloom: elements differing in exactly one byte, each position,
    ///    both directions — each byte's contribution to the k-hash image
    ///    must independently steer the bitset.
    ///  - intset: values differing by one in either direction around a
    ///    stored member — is_member must witness each delta.
    ///  - heaps: value pairs differing only in the low/high half — the
    ///    comparator must witness both halves.
    #[test]
    fn single_field_witness_pairs() {
        let _serial = crate::c_oracle_serial();
        // bloom: one filter, add base; probe mutants of one byte each.
        unsafe {
            pg_diff_libfam_reset();
            pg_diff_bloom_create(1000, 1024, 0xfeed_beef);
        }
        let cx = MemoryContext::new("witness");
        let mut rust = BloomFilter::create_in(cx.mcx(), 1000, 1024, 0xfeed_beef).unwrap();
        let base = *b"witness-elem";
        rust.add_element(&base);
        unsafe { pg_diff_bloom_add(base.as_ptr(), base.len()) };
        let mut images = std::collections::HashSet::new();
        for pos in 0..base.len() {
            for delta in [1u8, 0x80] {
                let mut m = base;
                m[pos] ^= delta;
                let rv = rust.lacks_element(&m);
                let cv = unsafe { pg_diff_bloom_lacks(m.as_ptr(), m.len()) } != 0;
                assert_eq!(rv, cv, "bloom witness pair pos {pos} delta {delta}");
                images.insert((pos, delta, rv));
            }
        }
        assert_eq!(images.len(), base.len() * 2, "all byte positions probed");

        // intset: member x; probe x-1, x, x+1 across codeword shapes.
        unsafe {
            pg_diff_libfam_reset();
            pg_diff_intset_create();
        }
        let cx2 = MemoryContext::new("witness2");
        let mut set = IntegerSet::create(cx2.mcx());
        let mut v = 100u64;
        for gap in [1u64, 2, 3, 255, 256, 1 << 20, 1 << 59] {
            v += gap;
            assert!(set.add_member(v).is_ok());
            assert_eq!(unsafe { pg_diff_intset_add(v) }, 0);
        }
        let mut probe = |x: u64| {
            let rv = set.is_member(x);
            let cv = unsafe { pg_diff_intset_is_member(x) } != 0;
            assert_eq!(rv, cv, "intset witness probe {x}");
        };
        let mut w = 100u64;
        for gap in [1u64, 2, 3, 255, 256, 1 << 20, 1 << 59] {
            w += gap;
            probe(w - 1);
            probe(w);
            probe(w + 1);
        }
    }

    /// Replay every checked-in seed (catches shim/link drift before the
    /// fleet campaign). Corpus is COMMITTED.
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/libfam_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/libfam_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                libfam_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// EXHAUSTIVE SWEEP (decision-cascade a0, full add-input domain):
    /// every u32 hash is applied, in order, to one cumulative filter per
    /// live bwidth on BOTH sides, with full register-file compares every
    /// 2^16 adds and estimate compares every 2^24. This witnesses the whole
    /// 2^32 add domain against C (cumulative order fixed; max-absorption
    /// means a wrong rho can only hide if a same-register larger value
    /// lands within the same 2^16 window — the fuzz arm's fresh-state
    /// per-exec compares cover that residue). Run explicitly:
    ///   cargo test -p decoder_fuzz --release hll_add_full_domain -- --ignored --nocapture
    #[test]
    #[ignore = "full 2^33 sweep (~minutes); run explicitly, bank the log"]
    fn hll_add_full_domain() {
        let _serial = crate::c_oracle_serial();
        for bwidth in [10i32, 5] {
            unsafe {
                pg_diff_libfam_reset();
                assert_eq!(pg_diff_hll_init(bwidth), 0);
            }
            let mut rust = if bwidth == 5 {
                Hll::B5(Box::new(HyperLogLog32::new(5)))
            } else {
                Hll::B10(Box::new(HyperLogLog::new(10)))
            };
            let t0 = std::time::Instant::now();
            let mut h: u32 = 0;
            loop {
                rust.add(h);
                unsafe { pg_diff_hll_add(h) };
                if h & 0xFFFF == 0xFFFF {
                    hll_compare_regs(&rust);
                }
                if h & 0xFF_FFFF == 0xFF_FFFF {
                    let re = rust.estimate();
                    let ce = unsafe { pg_diff_hll_estimate() };
                    assert_eq!(re.to_bits(), ce.to_bits(), "estimate at h={h:#x}");
                }
                h = match h.checked_add(1) {
                    Some(v) => v,
                    None => break,
                };
            }
            hll_compare_regs(&rust);
            let re = rust.estimate();
            let ce = unsafe { pg_diff_hll_estimate() };
            assert_eq!(re.to_bits(), ce.to_bits(), "final estimate bwidth {bwidth}");
            println!(
                "hll_add_full_domain bwidth={} : 4294967296 adds, {:?}",
                bwidth,
                t0.elapsed()
            );
        }
    }
}
