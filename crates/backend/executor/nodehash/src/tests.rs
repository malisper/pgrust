use super::*;

use ::datum::Datum;
use ::mcx::MemoryContext;

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    let ctx = MemoryContext::new("nodehash-test");
    f(ctx.mcx())
}

#[test]
fn probe_bloom_no_false_negatives() {
    with_mcx(|mcx| {
        let mut bf = ProbeBloom::new_in(mcx, 10_000.0);
        let mut h: u32 = 0x9e37_79b9;
        let mut inserted = Vec::new();
        for _ in 0..10_000 {
            h = h.wrapping_mul(0x0019_660d).wrapping_add(0x3c6e_f35f);
            bf.insert(h);
            inserted.push(h);
        }
        for h in inserted {
            assert!(bf.test(h));
        }
    });
}

#[test]
fn probe_bloom_rejects_and_density() {
    with_mcx(|mcx| {
        let mut bf = ProbeBloom::new_in(mcx, 1_000.0);
        for v in 0..1_000i32 {
            bf.insert(::hashfn::hash_bytes_uint32(v as u32));
        }
        assert!(bf.density() <= 0.25);
        let misses = (100_000..110_000i32)
            .filter(|v| !bf.test(::hashfn::hash_bytes_uint32(*v as u32)))
            .count();
        assert!(misses > 9_000, "filter admits too much: {misses} misses of 10000");
        let full = ProbeBloom {
            words: ::mcx::vec_from_elem_in(mcx, u64::MAX, 64),
            wmask: 63,
        };
        assert!(full.density() > 0.25);
    });
}

#[test]
fn sel_hash32_low32_matches_scalar_semantics() {
    with_mcx(|mcx| {
        let mut bf = ProbeBloom::new_in(mcx, 64.0);
        for v in [7i32, -3, 0, 123_456] {
            bf.insert(::hashfn::hash_bytes_uint32(v as u32));
        }
        let values: Vec<Datum> = (-8..120i32).map(Datum::from_i32).collect();
        let mut isnull = vec![false; values.len()];
        isnull[3] = true;
        let mut sel = [0u64; 4];
        bf.sel_hash32_low32(&values, &isnull, &mut sel);
        for (i, v) in (-8..120i32).enumerate() {
            let expect = if isnull[i] {
                bf.test(0)
            } else {
                bf.test(::hashfn::hash_bytes_uint32(v as u32))
            };
            let got = sel[i / 64] & (1u64 << (i % 64)) != 0;
            assert_eq!(got, expect, "row {i} value {v}");
        }
    });
}
