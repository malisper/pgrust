use super::*;
use std::collections::HashMap;

fn states_i64(p: *mut u8) -> *mut i64 {
    p.cast()
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
