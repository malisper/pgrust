// Parity fuzz — the merge-blocking emitter evidence (Miri cannot run
// generated code): kernel vs interpreter byte-compare, all kind classes.
use std::cell::Cell;
use std::rc::Rc;

use datum::Datum;
use mcx::{MemoryContext, PgVec};
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, ItemPointerData, TupleDescData,
    ATTNULLABLE_UNRESTRICTED,
};

use super::{fixed_prefix, install};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn attr(attlen: i16, attbyval: bool, attalignby: u8, dropped: bool) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen,
        attbyval,
        attispackable: attlen == -1,
        atthasmissing: false,
        attisdropped: dropped,
        attgenerated: false,
        attnullability: ATTNULLABLE_UNRESTRICTED,
        attalignby,
    }
}

fn random_schema(r: &mut Rng) -> Vec<CompactAttribute> {
    let natts = 1 + r.below(32) as usize;
    (0..natts)
        .map(|_| match r.below(10) {
            0 => attr(1, true, 1, false),
            1 => attr(2, true, 2, false),
            2 => attr(4, true, 4, false),
            3 => attr(8, true, 8, false),
            4 => attr(12, false, 4, false),
            5 => attr(16, false, 8, false),
            6 => attr(64, false, 1, false),
            7 => attr(-1, false, 4, false),
            8 => attr(-2, false, 1, false),
            _ => attr(4, true, 4, true),
        })
        .collect()
}

fn leaked_desc(atts: &[CompactAttribute]) -> Rc<TupleDescData<'static>> {
    let mcx = Box::leak(Box::new(MemoryContext::new("jit-fuzz"))).mcx();
    let mut compact: PgVec<'static, CompactAttribute> = PgVec::new_in(mcx);
    let mut attrs: PgVec<'static, FormData_pg_attribute> = PgVec::new_in(mcx);
    for a in atts {
        compact.push(a.clone());
        attrs.push(FormData_pg_attribute::default());
    }
    Rc::new(TupleDescData {
        natts: atts.len() as i32,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

// heap_fill_tuple layout: fixed attrs aligned; short varlenas + toast
// pointers packed; 4B varlenas aligned over ZEROED padding; cstrings packed.
fn build_row(
    r: &mut Rng,
    atts: &[CompactAttribute],
    buf: &mut Vec<u8>,
) -> (u32, Vec<bool>, bool) {
    let natts = atts.len();
    let row_nullable = r.below(3) == 0;
    let nulls: Vec<bool> = (0..natts)
        .map(|_| row_nullable && r.below(4) == 0)
        .collect();
    let hasnulls = nulls.iter().any(|&b| b);
    let bitmap_len = if hasnulls { natts.div_ceil(8) } else { 0 };
    let hoff = (23 + bitmap_len).next_multiple_of(8);

    buf.clear();
    buf.resize(hoff, 0);
    buf[18..20].copy_from_slice(&(natts as u16).to_ne_bytes());
    if hasnulls {
        buf[20..22].copy_from_slice(&types_tuple::HEAP_HASNULL.to_ne_bytes());
        for (i, &isnull) in nulls.iter().enumerate() {
            if !isnull {
                buf[23 + i / 8] |= 1 << (i % 8);
            }
        }
    }
    buf[22] = hoff as u8;

    for (i, a) in atts.iter().enumerate() {
        if nulls[i] {
            continue;
        }
        if a.attlen > 0 {
            let pad = buf.len().next_multiple_of(a.attalignby.max(1) as usize) - buf.len();
            buf.resize(buf.len() + pad, 0);
            for _ in 0..a.attlen {
                let b = r.next() as u8;
                buf.push(b);
            }
        } else if a.attlen == -1 {
            match r.below(3) {
                0 => {
                    let total = 1 + r.below(20) as usize;
                    buf.push(((total as u8) << 1) | 0x01);
                    for _ in 0..total - 1 {
                        buf.push(r.next() as u8);
                    }
                }
                1 => {
                    let pad = buf.len().next_multiple_of(a.attalignby as usize) - buf.len();
                    buf.resize(buf.len() + pad, 0);
                    let total = 4 + r.below(32) as usize;
                    buf.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
                    for _ in 0..total - 4 {
                        buf.push(r.next() as u8);
                    }
                }
                _ => {
                    buf.push(0x01);
                    buf.push(types_tuple::varatt::VARTAG_ONDISK);
                    for _ in 0..16 {
                        buf.push(r.next() as u8);
                    }
                }
            }
        } else {
            debug_assert!(a.attlen == -2);
            for _ in 0..r.below(12) {
                buf.push(1 + (r.next() as u8) % 250);
            }
            buf.push(0);
        }
    }
    (buf.len() as u32, nulls, hasnulls)
}

#[test]
fn fuzz_parity_vs_interpreter() {
    let mut r = Rng(0x9E3779B97F4A7C15);
    let mut kernels_run = 0u32;
    let mut nullfree_rows = 0u32;
    let mut refused = 0u32;
    for _ in 0..400 {
        let atts = random_schema(&mut r);
        let desc = leaked_desc(&atts);
        let prefix = fixed_prefix(&atts);
        assert!(install(&desc, prefix + 1).is_none());
        if prefix == 0 {
            refused += 1;
            continue;
        }
        let mut ncols_set = vec![1usize, prefix, 1 + r.below(prefix as u64) as usize];
        ncols_set.dedup();
        for ncols in ncols_set {
            let Some(k) = install(&desc, ncols) else {
                refused += 1;
                continue;
            };
            assert!(k.matches(&desc));
            kernels_run += 1;
            let natts = atts.len();
            let mut buf = Vec::with_capacity(1024);
            for _ in 0..8 {
                let (len, _nulls, hasnulls) = build_row(&mut r, &atts, &mut buf);
                // Contract: hasnulls rows never reach a kernel.
                if hasnulls {
                    continue;
                }
                nullfree_rows += 1;
                let mut image = vec![0u64; (len as usize).div_ceil(8)];
                bytemuck_copy(&mut image, &buf);
                let tup = unsafe {
                    HeapTupleData::from_raw_parts(
                        image.as_ptr().cast(),
                        len,
                        ItemPointerData::new(0, 1),
                        1259,
                    )
                };
                let mut iv = vec![Datum::null(); natts];
                let mut inl = vec![true; natts];
                types_tuple::heap_deform_tuple(&tup, &desc, &mut iv, &mut inl);

                let mut jv = vec![Datum::null(); natts.max(ncols)];
                let mut jn = vec![0xAAu8; natts.max(ncols)];
                unsafe { k.row(tup.getstruct(), jv.as_mut_ptr(), jn.as_mut_ptr()) };
                for c in 0..ncols {
                    assert_eq!(jv[c].as_usize(), iv[c].as_usize(), "row value col {c}");
                    assert_eq!(jn[c], 0, "row isnull col {c}");
                    assert!(!inl[c]);
                }
                for c in ncols..jn.len() {
                    assert_eq!(jn[c], 0xAA, "isnull overrun at {c}");
                }

                const STRIDE_CELLS: usize = 61;
                let mut sv = vec![Datum::null(); ncols * STRIDE_CELLS];
                unsafe { k.soa(tup.getstruct(), sv.as_mut_ptr(), STRIDE_CELLS * 8) };
                for c in 0..ncols {
                    assert_eq!(sv[c * STRIDE_CELLS].as_usize(), iv[c].as_usize(), "soa col {c}");
                }
                assert_eq!(k.end_off() as usize, prefix_end(&atts, ncols));
            }
        }
    }
    assert!(kernels_run > 300, "kernels_run = {kernels_run}");
    assert!(nullfree_rows > 1500, "nullfree_rows = {nullfree_rows}");
    assert!(refused > 20, "refused = {refused}");
}

fn prefix_end(atts: &[CompactAttribute], ncols: usize) -> usize {
    let mut off = 0usize;
    for a in &atts[..ncols] {
        off = off.next_multiple_of(a.attalignby.max(1) as usize) + a.attlen as usize;
    }
    off
}

fn bytemuck_copy(dst: &mut [u64], src: &[u8]) {
    // SAFETY: dst covers >= src.len() bytes (sized by the caller).
    unsafe {
        core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr().cast::<u8>(), src.len())
    };
}

// Install/drop far past the 512KiB cap: only whole-chunk reuse can pass.
#[test]
fn arena_reuses_chunks() {
    let atts: Vec<CompactAttribute> =
        (0..8).map(|i| attr(if i % 2 == 0 { 4 } else { 8 }, true, if i % 2 == 0 { 4 } else { 8 }, false)).collect();
    let desc = leaked_desc(&atts);
    for _ in 0..20_000 {
        let k = install(&desc, 8).expect("arena must reuse retired chunks");
        drop(k);
    }
    let held: Vec<_> = (0..100_000).map_while(|_| install(&desc, 8)).collect();
    assert!(held.len() >= 100, "arena unusably small: {}", held.len());
    assert!(held.len() < 100_000, "arena cap did not fail open");
}
