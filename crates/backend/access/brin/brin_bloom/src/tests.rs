use super::*;

fn init(mcx: Mcx<'_>, ndistinct: i32, fpr: f64) -> Datum {
    bloom_init(mcx, ndistinct, fpr).unwrap()
}

#[test]
fn filter_size_matches_c_shape() {
    let (nbytes, nbits, nhashes) = bloom_filter_size(3724, 0.01);
    assert_eq!(nbits % 8, 0);
    assert_eq!(nbytes * 8, nbits as usize);
    // k = round(ln2 * m / n) with m sized for p=0.01 lands on -log2(p) ~= 7.
    assert_eq!(nhashes, 7);
    let (nbytes16, _, _) = bloom_filter_size(16, 0.01);
    assert!(nbytes16 < nbytes);
}

#[test]
fn add_then_contains() {
    let ctx = mcx::MemoryContext::new("t");
    let f = init(ctx.mcx(), 128, 0.01);
    let filter = unsafe { filter_bytes_mut(f) };
    for v in 0..100u32 {
        let updated = bloom_add_value(filter, v.wrapping_mul(0x9e3779b9));
        assert!(updated || v > 0);
    }
    for v in 0..100u32 {
        assert!(bloom_contains_value(filter, v.wrapping_mul(0x9e3779b9)));
    }
    let mut misses = 0;
    for v in 1000..1100u32 {
        if !bloom_contains_value(filter, v.wrapping_mul(0x517cc1b7)) {
            misses += 1;
        }
    }
    assert!(misses > 90, "false positive rate implausibly high: {misses}");
}

#[test]
fn nbits_set_tracks_popcount() {
    let ctx = mcx::MemoryContext::new("t");
    let f = init(ctx.mcx(), 64, 0.01);
    let filter = unsafe { filter_bytes_mut(f) };
    for v in 0..50u32 {
        bloom_add_value(filter, v.wrapping_mul(0x9e3779b9));
    }
    let popcount: u32 = filter[OFF_DATA..].iter().map(|b| b.count_ones()).sum();
    assert_eq!(read_u32(filter, OFF_NBITS_SET), popcount);
}

#[test]
fn union_ors_bitmaps() {
    let ctx = mcx::MemoryContext::new("t");
    let fa = init(ctx.mcx(), 64, 0.01);
    let fb = init(ctx.mcx(), 64, 0.01);
    let a = unsafe { filter_bytes_mut(fa) };
    let b = unsafe { filter_bytes_mut(fb) };
    for v in 0..30u32 {
        bloom_add_value(a, v.wrapping_mul(3));
    }
    for v in 30..60u32 {
        bloom_add_value(b, v.wrapping_mul(3));
    }
    for i in 0..a.len() - OFF_DATA {
        a[OFF_DATA + i] |= b[OFF_DATA + i];
    }
    for v in 0..60u32 {
        assert!(bloom_contains_value(a, v.wrapping_mul(3)));
    }
}
