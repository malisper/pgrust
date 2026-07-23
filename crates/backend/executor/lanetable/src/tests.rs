use super::*;
use std::collections::HashMap;

fn states_i64(p: *mut u8) -> *mut i64 {
    p.cast()
}

/// The miri fallback must be bit-exact with the hardware instruction, or the
/// UB gate would hash (and bucket) differently than production runs.
#[test]
#[cfg(all(target_arch = "aarch64", not(miri)))]
fn software_crc32cx_parity() {
    if !crc_supported() {
        return;
    }
    let mut x: u64 = 0x243F_6A88_85A3_08D3;
    let mut crc: u32 = 0;
    for _ in 0..4096 {
        x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(0xB5AD_4ECE_DA1C_E2A9);
        assert_eq!(crc32cx(crc, x), crc32cx_sw(crc, x), "data={x:#x} crc={crc:#x}");
        crc = crc32cx(crc, x);
    }
}

/// Small-N walk of every unsafe path for the miri UB gate (tuplesort's
/// `miri_scale_unsafe_paths` pattern — the full suite runs production-N
/// workloads that are TOO-SLOW under interpretation). Covers: the config
/// matrix (both hash kinds x both entry layouts) with growth; multi-chunk
/// row storage (bases[c > 0] addressing, miri F8); a pre-split two-level
/// table (capacity_hint > TWO_LEVEL_THRESHOLD); the hoisted batch and fold
/// drivers (row_ptr_raw); bytes and i128 key reprs; reset/reuse (the
/// clear() base re-capture). NOT covered here (native-only, needs >100k
/// members): two-level CONVERSION and salted probes past
/// SALT_DISABLE_MAX_ENTRIES.
#[test]
fn miri_scale_unsafe_paths() {
    // (a) config matrix, growth from 64 slots, per-row probe + read-back.
    int_roundtrip(600, 96, false);

    // (b) multi-chunk rows: a 4096-byte state makes stride 513 words, so
    // rows_per_chunk clamps to 64 and ~200 groups span 4 chunks.
    let mut t = LaneAggTable::new(KeyRepr::Int, 4096, 64);
    for i in 0..600i64 {
        let k = i % 200;
        let pr = t.probe_int(k, t.hash_key_int(k as u64));
        // SAFETY: zeroed 4096-byte states.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert_eq!(t.len(), 200);
    for i in 0..t.nrows() {
        // SAFETY: live row.
        assert_eq!(unsafe { *states_i64(t.row_states(i)) }, 3);
    }
    // reset() exercises RowStore::clear's zero + base re-capture (F8).
    t.reset();
    assert_eq!(t.len(), 0);
    let pr = t.probe_int(7, t.hash_key_int(7));
    assert!(pr.is_new);
    // SAFETY: reset re-zeroes retained chunks.
    assert_eq!(unsafe { *states_i64(pr.states) }, 0);

    // (c) born-two-level table: bucketed probe/insert without the (too slow
    // under miri) conversion crossing.
    let mut t2 = LaneAggTable::new(KeyRepr::Int, 16, TWO_LEVEL_THRESHOLD + 1);
    assert!(t2.is_two_level());
    for i in 0..300i64 {
        let k = i % 100;
        let pr = t2.probe_int(k, t2.hash_key_int(k as u64));
        // SAFETY: zeroed 16-byte states.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert_eq!(t2.len(), 100);

    // (d) hoisted batch driver (row_ptr_raw), all three prefetch modes.
    let keys: Vec<i64> = (0..1500)
        .map(|i| ((i as u64 * 48271 % 300).wrapping_mul(0x9E37_79B9_7F4A_7C15)) as i64)
        .collect();
    let mut results: Vec<Vec<(i64, i64)>> = Vec::new();
    for mode in [PrefetchMode::None, PrefetchMode::PreTouch, PrefetchMode::Adaptive] {
        let mut tb = LaneAggTable::new(KeyRepr::Int, 16, 64);
        let (mut hashes, mut out, mut new_out) = (Vec::new(), Vec::new(), Vec::new());
        for chunk in keys.chunks(512) {
            out.clear();
            new_out.clear();
            tb.probe_int_batch(chunk, mode, &mut hashes, &mut out, &mut new_out);
            for &s in out.iter() {
                // SAFETY: state pointers live for the table's life.
                unsafe { *states_i64(s) += 1 };
            }
        }
        let mut rows: Vec<(i64, i64)> = (0..tb.nrows())
            // SAFETY: live rows.
            .map(|i| (tb.row_key_int(i).unwrap(), unsafe { *states_i64(tb.row_states(i)) }))
            .collect();
        rows.sort();
        results.push(rows);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[1], results[2]);

    // (e) fused fold driver over the config matrix.
    for hash in [HashKind::Fmix, HashKind::Crc] {
        for layout in [EntryLayout::Salt8, EntryLayout::Inline16] {
            let mut tf = LaneAggTable::with_config(KeyRepr::Int, 16, 64, hash, layout);
            let mut news = 0usize;
            tf.probe_fold_int(&keys, |s, _i, is_new| {
                if is_new {
                    news += 1;
                }
                // SAFETY: zeroed 16-byte states.
                unsafe { *states_i64(s) += 1 };
            });
            assert_eq!(news, 300);
            assert_eq!(tf.len(), 300);
        }
    }

    // (f) bytes keys: short (packed) and long (arena) forms + read-back.
    let mut tby = LaneAggTable::new(KeyRepr::Bytes, 8, 16);
    let corpus: Vec<Vec<u8>> = (0..300)
        .map(|i| {
            let s =
                format!("k-{}{}", i % 70, if i % 3 == 0 { "-with-a-long-suffix" } else { "" });
            s.into_bytes()
        })
        .collect();
    let mut reference: HashMap<Vec<u8>, i64> = HashMap::new();
    for k in &corpus {
        let pr = tby.probe_bytes(k, tby.hash_key_bytes(k));
        // SAFETY: zeroed 8-byte states.
        unsafe { *states_i64(pr.states) += 1 };
        *reference.entry(k.clone()).or_insert(0) += 1;
    }
    assert_eq!(tby.len(), reference.len());
    let mut scratch = [0u8; 8];
    for i in 0..tby.nrows() {
        let k = tby.row_key_bytes(i, &mut scratch).unwrap().to_vec();
        // SAFETY: live row.
        assert_eq!(unsafe { *states_i64(tby.row_states(i)) }, reference[&k]);
    }

    // (g) i128 keys across word boundaries.
    let mut t128 = LaneAggTable::new(KeyRepr::Int128, 8, 16);
    for i in 0..300u64 {
        let k = [i % 60, (i % 60) << 1];
        let pr = t128.probe_i128(k, t128.hash_key_i128(k));
        // SAFETY: zeroed 8-byte states.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert_eq!(t128.len(), 60);
}

/// Reference-checked int build: fold (sum, count) per key across the salt
/// enable threshold, growth, and (optionally) two-level conversion — over
/// the full (hash kind × entry layout) config matrix.
fn int_roundtrip(n: usize, card: u64, expect_two_level: bool) {
    for hash in [HashKind::Fmix, HashKind::Crc] {
        for layout in [EntryLayout::Salt8, EntryLayout::Inline16] {
            int_roundtrip_cfg(n, card, expect_two_level, hash, layout);
        }
    }
}

fn int_roundtrip_cfg(
    n: usize,
    card: u64,
    expect_two_level: bool,
    hash: HashKind,
    layout: EntryLayout,
) {
    let mut t = LaneAggTable::with_config(KeyRepr::Int, 16, 64, hash, layout);
    let mut reference: HashMap<i64, (i64, i64)> = HashMap::new();
    for i in 0..n {
        // Multiplicative spread (the bench rig's own domain reduction).
        let k = ((i as u64 % card).wrapping_mul(0x9E37_79B9_7F4A_7C15)) as i64;
        let pr = t.probe_int(k, t.hash_key_int(k as u64));
        // SAFETY: 16 state bytes, zero-initialized at birth.
        unsafe {
            let s = states_i64(pr.states);
            if pr.is_new {
                assert_eq!((*s, *s.add(1)), (0, 0), "new states must be zeroed");
            }
            *s = (*s).wrapping_add(k);
            *s.add(1) += 1;
        }
        let e = reference.entry(k).or_insert((0, 0));
        e.0 = e.0.wrapping_add(k);
        e.1 += 1;
    }
    assert_eq!(t.len(), reference.len());
    assert_eq!(t.is_two_level(), expect_two_level);
    // Read-back: every row exactly once, values matching the reference.
    let mut seen = 0usize;
    for i in 0..t.nrows() {
        let k = t.row_key_int(i).expect("no NULL group in this test");
        let s = states_i64(t.row_states(i));
        // SAFETY: live row states.
        let (sum, cnt) = unsafe { (*s, *s.add(1)) };
        assert_eq!(reference[&k], (sum, cnt), "key {k}");
        seen += 1;
    }
    assert_eq!(seen, reference.len());
}

#[test]
fn int_small_salt_disabled() {
    int_roundtrip(50_000, 1_000, false);
}

#[test]
fn int_across_salt_enable_threshold() {
    // Cardinality crosses SALT_DISABLE_MAX_ENTRIES (8192) mid-build: entries
    // born saltless-CHECKED but salt-STORED must stay findable afterward.
    int_roundtrip(80_000, 40_000, false);
}

#[test]
fn int_two_level_conversion() {
    int_roundtrip(600_000, 300_000, true);
}

#[test]
fn int_negative_and_extreme_keys() {
    let mut t = LaneAggTable::new(KeyRepr::Int, 8, 4);
    for k in [i64::MIN, -1, 0, 1, i64::MAX, i64::MIN, 0] {
        let pr = t.probe_int(k, t.hash_key_int(k as u64));
        // SAFETY: 8 zeroed state bytes.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert_eq!(t.len(), 5);
    let mut got: Vec<(i64, i64)> = (0..t.nrows())
        .map(|i| {
            // SAFETY: live row.
            (t.row_key_int(i).unwrap(), unsafe { *states_i64(t.row_states(i)) })
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![(i64::MIN, 2), (-1, 1), (0, 2), (1, 1), (i64::MAX, 1)]);
}

#[test]
fn null_group_out_of_band() {
    let mut t = LaneAggTable::new(KeyRepr::Int, 8, 4);
    let a = t.probe_null();
    assert!(a.is_new);
    // SAFETY: zeroed states.
    unsafe { *states_i64(a.states) += 7 };
    let pr = t.probe_int(42, t.hash_key_int(42));
    // SAFETY: zeroed states.
    unsafe { *states_i64(pr.states) += 1 };
    let b = t.probe_null();
    assert!(!b.is_new);
    assert_eq!(a.states, b.states);
    assert_eq!(t.len(), 2);
    let keys: Vec<Option<i64>> = (0..t.nrows()).map(|i| t.row_key_int(i)).collect();
    assert!(keys.contains(&None) && keys.contains(&Some(42)));
}

#[test]
fn batch_modes_agree() {
    let n = 200_000usize;
    let card = 50_000u64;
    let keys: Vec<i64> = (0..n)
        .map(|i| ((i as u64 * 48271 % card).wrapping_mul(0x9E37_79B9_7F4A_7C15)) as i64)
        .collect();
    let mut results: Vec<Vec<(i64, i64, i64)>> = Vec::new();
    for mode in [PrefetchMode::None, PrefetchMode::PreTouch, PrefetchMode::Adaptive] {
        let mut t = LaneAggTable::new(KeyRepr::Int, 16, 64);
        let (mut hashes, mut out, mut new_out) = (Vec::new(), Vec::new(), Vec::new());
        for chunk in keys.chunks(1024) {
            out.clear();
            new_out.clear();
            t.probe_int_batch(chunk, mode, &mut hashes, &mut out, &mut new_out);
            assert_eq!(out.len(), chunk.len());
            for (j, &s) in out.iter().enumerate() {
                // SAFETY: state pointers returned live for the table's life.
                unsafe {
                    *states_i64(s) = (*states_i64(s)).wrapping_add(chunk[j]);
                    *states_i64(s).add(1) += 1;
                }
            }
        }
        let mut rows: Vec<(i64, i64, i64)> = (0..t.nrows())
            .map(|i| {
                let s = states_i64(t.row_states(i));
                // SAFETY: live rows.
                unsafe { (t.row_key_int(i).unwrap(), *s, *s.add(1)) }
            })
            .collect();
        rows.sort();
        results.push(rows);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0], results[2]);
}

/// GL-ALPHA1 batched-install parity pin: `probe_int_batch_install` must be
/// byte-identical to the incumbent `probe_int_batch` — same `new_out`
/// sequence PER CHUNK (create order), same row insertion order (unsorted
/// key walk), same fold results — across layouts, hash kinds, dup-heavy
/// and all-distinct streams, growth, and two-level conversion (small hint),
/// including trailing partial chunks.
#[test]
fn batch_install_matches_incumbent() {
    for (card, n) in [(50_000u64, 200_000usize), (150_000, 150_000), (777, 10_001)] {
        let keys: Vec<i64> = (0..n)
            .map(|i| ((i as u64 * 48271 % card).wrapping_mul(0x9E37_79B9_7F4A_7C15)) as i64)
            .collect();
        for layout in [EntryLayout::Salt8, EntryLayout::Inline16] {
            for hash in [HashKind::Fmix, HashKind::Crc] {
                let mut a = LaneAggTable::with_config(KeyRepr::Int, 16, 64, hash, layout);
                let mut b = LaneAggTable::with_config(KeyRepr::Int, 16, 64, hash, layout);
                let (mut ha, mut oa, mut na) = (Vec::new(), Vec::new(), Vec::new());
                let (mut hb, mut ob, mut nb) = (Vec::new(), Vec::new(), Vec::new());
                for chunk in keys.chunks(1024) {
                    oa.clear();
                    na.clear();
                    a.probe_int_batch(chunk, PrefetchMode::Adaptive, &mut ha, &mut oa, &mut na);
                    b.probe_int_batch_install(chunk, &mut hb, &mut ob, &mut nb);
                    assert_eq!(na, nb, "create order (new_out) diverged");
                    assert_eq!(oa.len(), ob.len());
                    for (j, (&sa, &sb)) in oa.iter().zip(ob.iter()).enumerate() {
                        assert!(!sb.is_null(), "install left a null state");
                        // SAFETY: state pointers live for the tables' lives.
                        unsafe {
                            *states_i64(sa) = (*states_i64(sa)).wrapping_add(chunk[j]);
                            *states_i64(sa).add(1) += 1;
                            *states_i64(sb) = (*states_i64(sb)).wrapping_add(chunk[j]);
                            *states_i64(sb).add(1) += 1;
                        }
                    }
                }
                assert_eq!(a.nrows(), b.nrows(), "row count diverged");
                for i in 0..a.nrows() {
                    // Row INSERTION ORDER must match exactly (unsorted).
                    assert_eq!(a.row_key_int(i), b.row_key_int(i), "row order diverged at {i}");
                    let (sa, sb) = (states_i64(a.row_states(i)), states_i64(b.row_states(i)));
                    // SAFETY: live rows.
                    unsafe {
                        assert_eq!((*sa, *sa.add(1)), (*sb, *sb.add(1)), "fold state diverged");
                    }
                }
            }
        }
    }
}

#[test]
fn bytes_short_and_long() {
    let mut t = LaneAggTable::new(KeyRepr::Bytes, 8, 16);
    let corpus: Vec<Vec<u8>> = (0..3000)
        .map(|i| {
            let s = format!("key-{}{}", i % 700, if i % 3 == 0 { "-with-a-long-suffix" } else { "" });
            s.into_bytes()
        })
        .collect();
    let mut reference: HashMap<Vec<u8>, i64> = HashMap::new();
    for k in &corpus {
        let pr = t.probe_bytes(k, t.hash_key_bytes(k));
        // SAFETY: zeroed 8-byte states.
        unsafe { *states_i64(pr.states) += 1 };
        *reference.entry(k.clone()).or_insert(0) += 1;
    }
    assert_eq!(t.len(), reference.len());
    let mut scratch = [0u8; 8];
    for i in 0..t.nrows() {
        let k = t.row_key_bytes(i, &mut scratch).unwrap().to_vec();
        // SAFETY: live row.
        let c = unsafe { *states_i64(t.row_states(i)) };
        assert_eq!(reference[&k], c, "key {:?}", String::from_utf8_lossy(&k));
    }
}

#[test]
fn bytes_prefix_lengths_distinct() {
    // "a", "aa" … packed-word keys of different lengths must be distinct
    // groups; empty key packs to 0 and is NOT the null group.
    let mut t = LaneAggTable::new(KeyRepr::Bytes, 8, 4);
    for k in ["", "a", "aa", "aaa", "aaaaaaaa", "aaaaaaaaa", ""] {
        let pr = t.probe_bytes(k.as_bytes(), t.hash_key_bytes(k.as_bytes()));
        // SAFETY: zeroed states.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert_eq!(t.len(), 6);
    let mut scratch = [0u8; 8];
    let mut seen: Vec<(Vec<u8>, i64)> = (0..t.nrows())
        .map(|i| {
            let k = t.row_key_bytes(i, &mut scratch).unwrap().to_vec();
            // SAFETY: live row.
            (k, unsafe { *states_i64(t.row_states(i)) })
        })
        .collect();
    seen.sort();
    assert_eq!(seen[0], (b"".to_vec(), 2));
}

/// Arena projection (re-charter inc-0): drive the long-key arena through
/// multiple reservation events under BOTH hint regimes (well-hinted and
/// wildly-exceeded hint = doubling fallback) and verify contents/offsets/
/// group identity are untouched — the reserve is capacity-only.
#[test]
fn bytes_arena_reserve_preserves_contents() {
    for hint in [64usize, 40_000] {
        let mut t = LaneAggTable::new(KeyRepr::Bytes, 8, hint);
        let card = 20_000usize;
        let mut reference: HashMap<Vec<u8>, i64> = HashMap::new();
        for i in 0..(card * 2) {
            // Mixed inline/long keys, long lengths varied 9..~60 B so the
            // arena grows unevenly (projection sees a moving average).
            let j = i % card;
            let k = if j % 4 == 0 {
                format!("s{:06}", j)
            } else {
                format!("long-key-{:06}-{}", j, "x".repeat(j % 48))
            }
            .into_bytes();
            let pr = t.probe_bytes(&k, t.hash_key_bytes(&k));
            // SAFETY: zeroed 8-byte states.
            unsafe { *states_i64(pr.states) += 1 };
            *reference.entry(k).or_insert(0) += 1;
        }
        assert_eq!(t.len(), reference.len(), "hint {hint}");
        let mut scratch = [0u8; 8];
        for i in 0..t.nrows() {
            let k = t.row_key_bytes(i, &mut scratch).unwrap().to_vec();
            // SAFETY: live row.
            let c = unsafe { *states_i64(t.row_states(i)) };
            assert_eq!(reference[&k], c, "hint {hint} key {:?}", String::from_utf8_lossy(&k));
        }
        // Capacity is accounted (mem_used) and must at least hold the arena.
        assert!(t.mem_used() > 0);
    }
}

#[test]
fn bytes_two_level_conversion() {
    let mut t = LaneAggTable::new(KeyRepr::Bytes, 8, 64);
    let card = 150_000usize;
    for i in 0..(card * 2) {
        let k = format!("k{:07}", i % card);
        let pr = t.probe_bytes(k.as_bytes(), t.hash_key_bytes(k.as_bytes()));
        // SAFETY: zeroed states.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert!(t.is_two_level());
    assert_eq!(t.len(), card);
    let mut scratch = [0u8; 8];
    for i in 0..t.nrows() {
        // SAFETY: live row.
        let c = unsafe { *states_i64(t.row_states(i)) };
        assert_eq!(c, 2, "row {i} key {:?}", t.row_key_bytes(i, &mut scratch));
    }
}

#[test]
fn reset_reuses() {
    let mut t = LaneAggTable::new(KeyRepr::Int, 8, 4);
    for k in 0..10_000i64 {
        t.probe_int(k, t.hash_key_int(k as u64));
    }
    t.probe_null();
    t.reset();
    assert_eq!(t.len(), 0);
    assert_eq!(t.nrows(), 0);
    let pr = t.probe_int(5, t.hash_key_int(5));
    assert!(pr.is_new);
    // SAFETY: reset re-zeroes retained chunks.
    assert_eq!(unsafe { *states_i64(pr.states) }, 0);
    assert_eq!(t.len(), 1);
}

#[test]
fn pack8_len_recovery() {
    assert_eq!(packed_len(pack8(b"")), 0);
    assert_eq!(packed_len(pack8(b"a")), 1);
    assert_eq!(packed_len(pack8(b"abcdefgh")), 8);
    assert_eq!(packed_len(pack8(b"abc")), 3);
}

#[test]
fn mem_used_monotone() {
    let mut t = LaneAggTable::new(KeyRepr::Int, 16, 4);
    let m0 = t.mem_used();
    for k in 0..100_000i64 {
        t.probe_int(k, t.hash_key_int(k as u64));
    }
    assert!(t.mem_used() > m0);
    // Sanity: accounted memory covers at least entries + rows actually held.
    assert!(t.mem_used() >= t.nrows() * (8 + 16));
}

#[test]
fn bytes_crc_hash_roundtrip() {
    // Same corpus as bytes_short_and_long, explicit Crc hash (falls back to
    // Fmix off-aarch64 — the test is then a duplicate, still valid).
    let mut t = LaneAggTable::with_config(KeyRepr::Bytes, 8, 16, HashKind::Crc, EntryLayout::Salt8);
    let corpus: Vec<Vec<u8>> = (0..3000)
        .map(|i| {
            let s = format!("key-{}{}", i % 700, if i % 3 == 0 { "-with-a-long-suffix" } else { "" });
            s.into_bytes()
        })
        .collect();
    let mut reference: HashMap<Vec<u8>, i64> = HashMap::new();
    for k in &corpus {
        let pr = t.probe_bytes(k, t.hash_key_bytes(k));
        // SAFETY: zeroed 8-byte states.
        unsafe { *states_i64(pr.states) += 1 };
        *reference.entry(k.clone()).or_insert(0) += 1;
    }
    assert_eq!(t.len(), reference.len());
    let mut scratch = [0u8; 8];
    for i in 0..t.nrows() {
        let k = t.row_key_bytes(i, &mut scratch).unwrap().to_vec();
        // SAFETY: live row.
        let c = unsafe { *states_i64(t.row_states(i)) };
        assert_eq!(reference[&k], c);
    }
}

#[test]
fn inline_layout_reset_reuses() {
    let mut t =
        LaneAggTable::with_config(KeyRepr::Int, 8, 4, HashKind::Crc, EntryLayout::Inline16);
    for k in 0..10_000i64 {
        t.probe_int(k, t.hash_key_int(k as u64));
    }
    assert_eq!(t.len(), 10_000);
    t.reset();
    assert_eq!(t.len(), 0);
    let pr = t.probe_int(5, t.hash_key_int(5));
    assert!(pr.is_new);
    assert_eq!(t.len(), 1);
}

#[test]
fn probe_fold_matches_per_row() {
    // The fused hoisted-locals driver must agree with per-row probe_int
    // across the config matrix, growth, and two-level conversion.
    let n = 300_000usize;
    let card = 140_000u64; // crosses TWO_LEVEL_THRESHOLD
    let keys: Vec<i64> = (0..n)
        .map(|i| ((i as u64 * 48271 % card).wrapping_mul(0x9E37_79B9_7F4A_7C15)) as i64)
        .collect();
    for hash in [HashKind::Fmix, HashKind::Crc] {
        for layout in [EntryLayout::Salt8, EntryLayout::Inline16] {
            let mut a = LaneAggTable::with_config(KeyRepr::Int, 16, 64, hash, layout);
            let mut new_seen = 0usize;
            a.probe_fold_int(&keys, |s, i, is_new| {
                if is_new {
                    new_seen += 1;
                }
                // SAFETY: zeroed 16-byte states.
                unsafe {
                    let p = states_i64(s);
                    *p = (*p).wrapping_add(keys[i as usize]);
                    *p.add(1) += 1;
                }
            });
            assert_eq!(new_seen, card as usize);
            assert!(a.is_two_level());
            let mut b = LaneAggTable::with_config(KeyRepr::Int, 16, 64, hash, layout);
            for &k in &keys {
                let pr = b.probe_int(k, b.hash_key_int(k as u64));
                // SAFETY: zeroed 16-byte states.
                unsafe {
                    let p = states_i64(pr.states);
                    *p = (*p).wrapping_add(k);
                    *p.add(1) += 1;
                }
            }
            let dump = |t: &LaneAggTable| -> Vec<(i64, i64, i64)> {
                let mut v: Vec<_> = (0..t.nrows())
                    .map(|i| {
                        let s = states_i64(t.row_states(i));
                        // SAFETY: live rows.
                        unsafe { (t.row_key_int(i).unwrap(), *s, *s.add(1)) }
                    })
                    .collect();
                v.sort();
                v
            };
            assert_eq!(dump(&a), dump(&b), "hash={hash:?} layout={layout:?}");
        }
    }
}

/// Reference-checked Int128 build: fold (sum, count) per 2-word key across
/// the salt threshold, growth, and (optionally) two-level conversion — both
/// hash kinds (Int128 is Salt8-only by construction).
fn i128_roundtrip(n: usize, card: u64, expect_two_level: bool) {
    for hash in [HashKind::Fmix, HashKind::Crc] {
        let mut t = LaneAggTable::with_config(KeyRepr::Int128, 16, 64, hash, EntryLayout::Salt8);
        let mut reference: HashMap<[u64; 2], (i64, i64)> = HashMap::new();
        for i in 0..n {
            let r = (i as u64 % card).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            // Both words carry key material (lo/hi split of a 96-bit-ish
            // composite — the packed multi-key shape).
            let k = [r ^ 0xDEAD_BEEF, r >> 7];
            let pr = t.probe_i128(k, t.hash_key_i128(k));
            // SAFETY: 16 zeroed state bytes.
            unsafe {
                let s = states_i64(pr.states);
                if pr.is_new {
                    assert_eq!((*s, *s.add(1)), (0, 0), "new states must be zeroed");
                }
                *s = (*s).wrapping_add(r as i64);
                *s.add(1) += 1;
            }
            let e = reference.entry(k).or_insert((0, 0));
            e.0 = e.0.wrapping_add(r as i64);
            e.1 += 1;
        }
        assert_eq!(t.len(), reference.len());
        assert_eq!(t.is_two_level(), expect_two_level);
        let mut seen = 0usize;
        for i in 0..t.nrows() {
            let k = t.row_key_i128(i).expect("no NULL group in this test");
            let s = states_i64(t.row_states(i));
            // SAFETY: live row states.
            let (sum, cnt) = unsafe { (*s, *s.add(1)) };
            assert_eq!(reference[&k], (sum, cnt), "key {k:?}");
            seen += 1;
        }
        assert_eq!(seen, reference.len());
    }
}

#[test]
fn i128_small_salt_disabled() {
    i128_roundtrip(50_000, 1_000, false);
}

#[test]
fn i128_across_salt_enable_threshold() {
    i128_roundtrip(80_000, 40_000, false);
}

#[test]
fn i128_two_level_conversion() {
    i128_roundtrip(600_000, 300_000, true);
}

#[test]
fn i128_word_boundaries_distinct() {
    // Keys differing in exactly one word (including hi-word-only diffs) must
    // stay distinct groups; extreme words exercise the full compare.
    let mut t = LaneAggTable::new(KeyRepr::Int128, 8, 4);
    let keys = [
        [0u64, 0u64],
        [1, 0],
        [0, 1],
        [u64::MAX, 0],
        [0, u64::MAX],
        [u64::MAX, u64::MAX],
        [0, 0],
        [0, 1],
    ];
    for k in keys {
        let pr = t.probe_i128(k, t.hash_key_i128(k));
        // SAFETY: 8 zeroed state bytes.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert_eq!(t.len(), 6);
    let mut got: Vec<([u64; 2], i64)> = (0..t.nrows())
        .map(|i| {
            // SAFETY: live row.
            (t.row_key_i128(i).unwrap(), unsafe { *states_i64(t.row_states(i)) })
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![
            ([0, 0], 2),
            ([0, 1], 2),
            ([0, u64::MAX], 1),
            ([1, 0], 1),
            ([u64::MAX, 0], 1),
            ([u64::MAX, u64::MAX], 1),
        ]
    );
}

#[test]
fn i128_batch_modes_agree_with_per_row() {
    // probe_i128_batch (all prefetch modes) and probe_fold_i128 must agree
    // with per-row probe_i128 across growth + two-level conversion.
    let n = 300_000usize;
    let card = 140_000u64; // crosses TWO_LEVEL_THRESHOLD
    let keys: Vec<[u64; 2]> = (0..n)
        .map(|i| {
            let r = (i as u64 * 48271 % card).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            [r, r >> 13]
        })
        .collect();
    for hash in [HashKind::Fmix, HashKind::Crc] {
        let dump = |t: &LaneAggTable| -> Vec<([u64; 2], i64, i64)> {
            let mut v: Vec<_> = (0..t.nrows())
                .map(|i| {
                    let s = states_i64(t.row_states(i));
                    // SAFETY: live rows.
                    unsafe { (t.row_key_i128(i).unwrap(), *s, *s.add(1)) }
                })
                .collect();
            v.sort();
            v
        };
        let mut per_row = LaneAggTable::with_config(KeyRepr::Int128, 16, 64, hash, EntryLayout::Salt8);
        for &k in &keys {
            let pr = per_row.probe_i128(k, per_row.hash_key_i128(k));
            // SAFETY: zeroed 16-byte states.
            unsafe {
                let p = states_i64(pr.states);
                *p = (*p).wrapping_add(k[0] as i64);
                *p.add(1) += 1;
            }
        }
        assert!(per_row.is_two_level());
        for mode in [PrefetchMode::None, PrefetchMode::PreTouch, PrefetchMode::Adaptive] {
            let mut t =
                LaneAggTable::with_config(KeyRepr::Int128, 16, 64, hash, EntryLayout::Salt8);
            let mut hashes = Vec::new();
            let mut out = Vec::new();
            let mut new_out = Vec::new();
            // Feed in table-growing sub-batches so prefetch engages mid-way.
            for chunk in keys.chunks(65_536) {
                out.clear();
                new_out.clear();
                t.probe_i128_batch(chunk, mode, &mut hashes, &mut out, &mut new_out);
                assert_eq!(out.len(), chunk.len());
                for (j, &s) in out.iter().enumerate() {
                    // SAFETY: probe-returned live states.
                    unsafe {
                        let p = states_i64(s);
                        *p = (*p).wrapping_add(chunk[j][0] as i64);
                        *p.add(1) += 1;
                    }
                }
            }
            assert_eq!(dump(&t), dump(&per_row), "hash={hash:?} mode={mode:?}");
        }
    }
}

#[test]
fn i128_reset_reuses() {
    let mut t = LaneAggTable::new(KeyRepr::Int128, 8, 4);
    for i in 0..10_000u64 {
        let k = [i, i ^ 0xF0F0];
        let pr = t.probe_i128(k, t.hash_key_i128(k));
        // SAFETY: zeroed states.
        unsafe { *states_i64(pr.states) += 1 };
    }
    assert_eq!(t.len(), 10_000);
    t.reset();
    assert_eq!(t.len(), 0);
    let pr = t.probe_i128([5, 6], t.hash_key_i128([5, 6]));
    assert!(pr.is_new);
    assert_eq!(t.len(), 1);
}

// GL-CONCMEM-1: the process-ledger charge balances across the table's
// whole lifecycle — growth (entry rehashes), two-level conversion, long-key
// arena growth, reset, Drop. Delta-based (other tests run concurrently on
// the same process-global counter) with a generous concurrent-noise
// allowance on the intermediate bound; the FINAL bound is the load-bearing
// one — a leaked charge is permanent and survives noise.
#[test]
fn ledger_charge_balances_across_lifecycle() {
    const NOISE: usize = 16 << 20;
    let base = mcx::global_footprint::bytes();
    {
        let mut t = LaneAggTable::new(KeyRepr::Int, 16, 0);
        for i in 0..200_000i64 {
            let pr = t.probe_int(i, t.hash_key_int(i as u64));
            // SAFETY: zeroed states.
            unsafe { *states_i64(pr.states) += 1 };
        }
        let held = mcx::global_footprint::bytes();
        assert!(
            held + NOISE >= base + t.mem_used() / 2,
            "growth did not charge the ledger (held {held}, base {base}, table {})",
            t.mem_used()
        );
        t.reset();
        // Bytes-key table: arena + projection path.
        let mut tb = LaneAggTable::new(KeyRepr::Bytes, 8, 1024);
        for i in 0..50_000u64 {
            let key = format!("a-rather-long-grouping-key-{i}-padded-well-past-eight");
            let b = key.as_bytes();
            let pr = tb.probe_bytes(b, tb.hash_key_bytes(b));
            // SAFETY: zeroed states.
            unsafe { *states_i64(pr.states) += 1 };
        }
        assert!(tb.mem_used() > 1 << 20, "bytes table premise");
    }
    // Both tables dropped: every charge must have unwound. Concurrent
    // tests hold their own live charges — retry until their windows pass
    // (a LEAKED charge is permanent and never converges, so the retry
    // cannot mask the failure class this test exists for).
    let mut ok = false;
    for _ in 0..50 {
        let after = mcx::global_footprint::bytes();
        if after <= base + NOISE && base <= after + NOISE {
            ok = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let after = mcx::global_footprint::bytes();
    assert!(ok, "ledger did not balance after Drop: base {base}, after {after}");
}
