//! D-STATS (seal-fusion charter §4, ruled): the BATCHED fold kernels behind
//! [`crate::builder::ColumnMeta`]'s span face. The incumbent per-value walk
//! (`observe_value_at`) pays class dispatch + key-derivation branch soup +
//! sketch feeding PER VALUE (~22 ns/value of fold shell alone; the stats
//! plane totals 59.8% of `seal_part`). These kernels pay the dispatch once
//! per SPAN and run the folds as flat passes over the already-materialized
//! datum array — the SEAL-FUSION `part_ptrs` win the charter names.
//!
//! ## The byte-neutrality law (charter conditions, carried verbatim)
//!
//! The sealed bytes are pure functions of fold RESULTS, so batching is
//! byte-invisible iff:
//! - (a) reordering is confined to order-INSENSITIVE accumulators: min/max,
//!   the i128 SUM (integer — `SumKind` has no float variant, so regrouping
//!   is exact), zero/nonnull counts, bloom bit-OR, HLL max-registers, the
//!   DistAcc multiset. The split-lane SUM below regroups integer addition
//!   only.
//! - (b) order-SENSITIVE facts keep row order: `first_key`/`last_key`, the
//!   asc/desc/ties transition chain (folded over adjacent pairs of the
//!   nonnull key sequence IN ROW ORDER, carry-in from the previous span),
//!   and the PSMA keybuf (extended in row order).
//! - (c) `meta_hash128` stays bit-identical — the same function over the
//!   same canonical bytes; batching only stages its OUTPUT in arrays.
//! - (d) the distribution finish path is untouched (`sketch.rs` — one
//!   `finish_distribution`, both producers).
//!
//! Kill switch: `PGRUST_SEAL_BATCH_FOLD=0|off` (read by the builder at
//! construction — the `populate_on` class) restores the incumbent
//! per-value path, which stays in `builder.rs` UNTOUCHED as the control
//! arm. The equivalence battery (`tests/builder_tests.rs`) pins
//! batched == per-value on adversarial corpora; the seeded-divergence
//! tooth below proves the battery can fail.
//!
//! ## Dict fold-from-codes (the #971/#983 DICT-DEDUP successor move)
//!
//! Dict codes are byte-rank ranks (code order == entry byte order — the
//! order certificate the dict build re-proves at emit), and the DICT-DEDUP
//! feed hands this builder the byte-rank entry list, so every per-value
//! fact is a pure function of the CODE through once-per-stream tables:
//! prefix keys, byte/char lengths, and the `meta_hash128` pair are computed
//! once per ENTRY (D values), and the per-row fold becomes integer table
//! lookups over the codes the seal already materialized. Bloom/HLL inserts
//! collapse to once per DISTINCT code per granule — bit-identical to the
//! per-row inserts because both accumulators are idempotent (bit-OR /
//! max-register). Counts that carry multiplicity (`nonnull`, `byte_len_sum`,
//! the keybuf, the sortedness chain) stay per-row lookups in row order.

use crate::bloom::{bloom_insert_hashed, BLOOM_K_DEFAULT};
use crate::builder::{utf8_char_count, CurGranule};
use crate::format::abi::{datum_canonical_bytes, EncodeInput};
use crate::format::class::StorageClass;
use crate::hash::meta_hash128;
use crate::key::{
    bool_key, f32_key_from_datum, f64_key_from_datum, interval_cmp_key, memcmp_fixed_exact_key,
    memcmp_fixed_prefix_key, memcmp_var_prefix_key, numeric_pack_at_scale, packed_numeric_key,
    signed_word_key, timetz_utc_key, unsigned_flip_key, unsigned_small_key, KeyDerivation,
    NumericPack,
};
use crate::profile::{LenStats, MetaProfile, SumKind, ZeroKind};
use crate::sketch::DistAcc;

/// How many DistAcc slot prefetches run ahead of the scalar inserts.
const DIST_PREFETCH_AHEAD: usize = 8;

/// SUM split-lane chunk bound: per chunk, the low-32 unsigned lane sums at
/// most `4096 * (2^32 - 1) < 2^44` and the high-32 lane at most
/// `4096 * 2^31 < 2^43` in magnitude — both far inside u64/i64.
const SUM_CHUNK: usize = 4096;

/// Reusable span scratch (owned by the builder; allocation-free spans after
/// warmup).
#[derive(Debug, Default)]
pub(crate) struct BatchScratch {
    /// Densified valid datum words (word classes).
    vals: Vec<u64>,
    /// Granule-relative rows of the densified values.
    rows: Vec<u32>,
    /// Derived keys of the key-carrying subsequence (row order).
    keys: Vec<i64>,
    /// Rows aligned with `keys`.
    krows: Vec<u32>,
    /// Precomputed hash lanes of the span's canonical values (row order).
    h1: Vec<u64>,
    h2: Vec<u64>,
    /// Byte classes: canonical (addr, len) of each valid readable value —
    /// addresses point into writer-owned images (the `datum_canonical_bytes`
    /// caller contract), captured as usize so the scratch stays `Send`.
    ptrs: Vec<(usize, u32)>,
    /// Rows aligned with `ptrs`.
    prow: Vec<u32>,
    /// Dict-codes fold: densified codes of valid rows.
    codes: Vec<u32>,
    /// Dict-codes fold: distinct-in-granule bitmap + its reset list.
    seen: Vec<u64>,
    seen_list: Vec<u32>,
}

/// Once-per-stream lookup tables for the dict fold-from-codes face, built
/// from the DICT-DEDUP feed's byte-rank entry list (index == global code).
#[derive(Debug)]
pub(crate) struct DictFoldTables {
    /// Per-code derived key (`MemcmpVarPrefix` profiles); `None` when the
    /// profile derives no keys (non-C collations).
    keys: Option<Vec<i64>>,
    blen: Vec<u32>,
    /// Per-code UTF-8 char count (BytesAndChars profiles only, else empty).
    clen: Vec<u32>,
    /// Per-code `meta_hash128` (hash-carrying profiles only, else empty).
    h1: Vec<u64>,
    h2: Vec<u64>,
    hashes: bool,
    chars: bool,
}

#[cfg(test)]
pub(crate) mod seed {
    //! The born-RED tooth: when armed, the batched hash lane SKIPS the
    //! span's last HLL update — a deliberate accumulator-coverage leak.
    //! The equivalence battery must go RED with this armed and GREEN with
    //! it disarmed; that failure is the proof the battery has teeth.
    //! THREAD-LOCAL so the armed test cannot poison concurrently running
    //! suites (builders fold on the driving thread).
    use std::cell::Cell;
    thread_local! {
        pub static SKIP_LAST_HLL: Cell<bool> = const { Cell::new(false) };
    }
    pub fn armed() -> bool {
        SKIP_LAST_HLL.with(|c| c.get())
    }
    pub fn arm(on: bool) {
        SKIP_LAST_HLL.with(|c| c.set(on));
    }
}

/// The batched span fold — the exact fold set of `observe_value_at` over
/// rows `[first, first+n)` of `input`, restructured into per-lane passes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn observe_rows_batched(
    profile: &MetaProfile,
    psma_populate: bool,
    cur: &mut CurGranule,
    dist: Option<&mut DistAcc>,
    s: &mut BatchScratch,
    base: u32,
    input: &EncodeInput<'_>,
    first: u32,
    n: u32,
) {
    match profile.class {
        StorageClass::ByvalWord { .. }
        | StorageClass::F32
        | StorageClass::F64
        | StorageClass::Bool => observe_words(profile, psma_populate, cur, dist, s, base, input, first, n),
        StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim => {
            observe_bytes(profile, psma_populate, cur, dist, s, base, input, first, n)
        }
    }
}

// ---------------------------------------------------------------------------
// word classes
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn observe_words(
    profile: &MetaProfile,
    psma_populate: bool,
    cur: &mut CurGranule,
    dist: Option<&mut DistAcc>,
    s: &mut BatchScratch,
    base: u32,
    input: &EncodeInput<'_>,
    first: u32,
    n: u32,
) {
    // Densify: valid datums (+ their granule-relative rows when the PSMA
    // keybuf will want them).
    let need_rows = psma_populate && profile.key != KeyDerivation::None;
    s.vals.clear();
    s.rows.clear();
    match input.validity {
        None => {
            s.vals
                .extend_from_slice(&input.datums[first as usize..(first + n) as usize]);
            if need_rows {
                s.rows.extend(base + first..base + first + n);
            }
        }
        Some(_) => {
            for r in first..first + n {
                if input.valid(r) {
                    s.vals.push(input.datums[r as usize]);
                    if need_rows {
                        s.rows.push(base + r);
                    }
                }
            }
        }
    }
    cur.nonnull += s.vals.len() as u32;
    if s.vals.is_empty() {
        return;
    }

    // Keys: derive per word (each word derivation is total), then the
    // shared chain fold.
    if profile.key != KeyDerivation::None {
        s.keys.clear();
        match profile.key {
            KeyDerivation::SignedWord => {
                s.keys.extend(s.vals.iter().map(|&w| signed_word_key(w).raw()));
            }
            KeyDerivation::UnsignedWordSmall => {
                s.keys
                    .extend(s.vals.iter().map(|&w| unsigned_small_key(w).raw()));
            }
            KeyDerivation::UnsignedWordFlip => {
                s.keys
                    .extend(s.vals.iter().map(|&w| unsigned_flip_key(w).raw()));
            }
            KeyDerivation::Bool => {
                s.keys.extend(s.vals.iter().map(|&w| bool_key(w).raw()));
            }
            KeyDerivation::Float32 => {
                s.keys
                    .extend(s.vals.iter().map(|&w| f32_key_from_datum(w).raw()));
            }
            KeyDerivation::Float64 => {
                s.keys
                    .extend(s.vals.iter().map(|&w| f64_key_from_datum(w).raw()));
            }
            // `MetaProfile::derive` never pairs a byte derivation with a
            // word class; degrade (not panic) if a foreign profile appears.
            _ => {
                debug_assert!(false, "byte key derivation on a word class");
                cur.poisoned = true;
            }
        }
        if !s.keys.is_empty() {
            fold_key_chain(cur, &s.keys, &s.rows, need_rows);
        }
    }

    // SUM (integer regroup — exact under any association).
    match profile.sum {
        SumKind::None => {}
        SumKind::SignedWord => {
            for chunk in s.vals.chunks(SUM_CHUNK) {
                let (mut lo, mut hi) = (0u64, 0i64);
                for &w in chunk {
                    lo += w & 0xFFFF_FFFF;
                    hi += (w as i64) >> 32;
                }
                cur.sum += ((hi as i128) << 32) + lo as i128;
            }
        }
        SumKind::UnsignedWord => {
            for chunk in s.vals.chunks(SUM_CHUNK) {
                let (mut lo, mut hi) = (0u64, 0u64);
                for &w in chunk {
                    lo += w & 0xFFFF_FFFF;
                    hi += w >> 32;
                }
                cur.sum += (((hi as u128) << 32) + lo as u128) as i128;
            }
        }
        SumKind::BoolTrues => {
            cur.sum += s.vals.iter().filter(|&&w| w != 0).count() as i128;
        }
        SumKind::PackedI64 => {
            debug_assert!(false, "PackedI64 sum on a word class");
        }
    }

    // Zero counts.
    match profile.zero {
        ZeroKind::None | ZeroKind::PackedI64 => {}
        ZeroKind::Word | ZeroKind::BoolFalse => {
            cur.zero += s.vals.iter().filter(|&&w| w == 0).count() as u64;
        }
        ZeroKind::Float => {
            let is_f32 = matches!(profile.class, StorageClass::F32);
            cur.zero += s
                .vals
                .iter()
                .filter(|&&w| {
                    if is_f32 {
                        f32::from_bits(w as u32) == 0.0
                    } else {
                        f64::from_bits(w) == 0.0
                    }
                })
                .count() as u64;
        }
    }

    // Hash lane: the §18.1 canonical shape per class, `meta_hash128` staged
    // into arrays (bit-identical hash), sketches fed from the arrays.
    if profile.hashes_values() {
        s.h1.clear();
        s.h2.clear();
        match profile.class {
            StorageClass::ByvalWord { width, .. } => {
                for &w in &s.vals {
                    let b = w.to_le_bytes();
                    let (h1, h2) = meta_hash128(&b[..width as usize]);
                    s.h1.push(h1);
                    s.h2.push(h2);
                }
            }
            StorageClass::F32 => {
                for &w in &s.vals {
                    let b = w.to_le_bytes();
                    let (h1, h2) = meta_hash128(&b[..4]);
                    s.h1.push(h1);
                    s.h2.push(h2);
                }
            }
            StorageClass::F64 => {
                for &w in &s.vals {
                    let (h1, h2) = meta_hash128(&w.to_le_bytes());
                    s.h1.push(h1);
                    s.h2.push(h2);
                }
            }
            StorageClass::Bool => {
                for &w in &s.vals {
                    let (h1, h2) = meta_hash128(&[(w != 0) as u8]);
                    s.h1.push(h1);
                    s.h2.push(h2);
                }
            }
            _ => unreachable!("word path"),
        }
        feed_hll_bloom(profile, cur, &s.h1, &s.h2);
        if let Some(d) = dist {
            let mut tmp = [0u8; 8];
            for i in 0..s.vals.len() {
                if i + DIST_PREFETCH_AHEAD < s.h1.len() {
                    d.prefetch(s.h1[i + DIST_PREFETCH_AHEAD]);
                }
                let w = s.vals[i];
                let canon: &[u8] = match profile.class {
                    StorageClass::ByvalWord { width, .. } => {
                        tmp = w.to_le_bytes();
                        &tmp[..width as usize]
                    }
                    StorageClass::F32 => {
                        tmp = w.to_le_bytes();
                        &tmp[..4]
                    }
                    StorageClass::F64 => {
                        tmp = w.to_le_bytes();
                        &tmp[..8]
                    }
                    StorageClass::Bool => {
                        tmp[0] = (w != 0) as u8;
                        &tmp[..1]
                    }
                    _ => unreachable!("word path"),
                };
                d.observe_hashed(s.h1[i], s.h2[i], canon);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// byte classes (Fixed / VarlenaVerbatim)
// ---------------------------------------------------------------------------

/// Rehydrate one captured canonical span. SAFETY: every `(addr, len)` in
/// `BatchScratch::ptrs` was minted THIS span from a live
/// `datum_canonical_bytes` slice (the pointer arms return views into the
/// writer-owned image, untouched by the 8-byte scratch), and those images
/// outlive the seal — the same caller contract the incumbent walk and
/// `verify_roundtrip` ride. The unconstrained lifetime never escapes this
/// module: callers consume the slice within the span passes.
fn slice_of<'x>(p: (usize, u32)) -> &'x [u8] {
    unsafe { core::slice::from_raw_parts(p.0 as *const u8, p.1 as usize) }
}

#[allow(clippy::too_many_arguments)]
fn observe_bytes(
    profile: &MetaProfile,
    psma_populate: bool,
    cur: &mut CurGranule,
    dist: Option<&mut DistAcc>,
    s: &mut BatchScratch,
    base: u32,
    input: &EncodeInput<'_>,
    first: u32,
    n: u32,
) {
    // Densify + canonicalize ONCE: capture (addr, len) of each valid
    // value's canonical bytes. The pointer arms of `datum_canonical_bytes`
    // never touch the 8-byte scratch — the returned slice aims into the
    // writer-owned image, which outlives the span (the same caller
    // contract the incumbent walk and `verify_roundtrip` ride).
    s.ptrs.clear();
    s.prow.clear();
    let mut tmp = [0u8; 8];
    for r in first..first + n {
        if !input.valid(r) {
            continue;
        }
        cur.nonnull += 1;
        match unsafe { datum_canonical_bytes(profile.class, input.datums[r as usize], &mut tmp) } {
            Ok(b) => {
                s.ptrs.push((b.as_ptr() as usize, b.len().min(u32::MAX as usize) as u32));
                s.prow.push(base + r);
            }
            Err(_) => {
                // Unreadable image: degrade, never guess (incumbent arm's
                // exact behavior — the value is counted nonnull, poisons
                // the grain, and participates in nothing else).
                debug_assert!(false, "unreadable datum image at meta build");
                cur.poisoned = true;
            }
        }
    }
    if s.ptrs.is_empty() {
        return;
    }

    // Keys (+ the PackedNumeric sum/zero coupling — pack ONCE per value).
    match profile.key {
        KeyDerivation::None => {}
        KeyDerivation::MemcmpFixed { len } => {
            s.keys.clear();
            s.krows.clear();
            for (i, &p) in s.ptrs.iter().enumerate() {
                let b = slice_of(p);
                if b.len() != len as usize {
                    cur.poisoned = true;
                    continue;
                }
                let raw = if len <= 8 {
                    memcmp_fixed_exact_key(b).raw()
                } else {
                    memcmp_fixed_prefix_key(b).raw()
                };
                s.keys.push(raw);
                s.krows.push(s.prow[i]);
            }
            fold_key_chain(cur, &s.keys, &s.krows, psma_populate);
        }
        KeyDerivation::MemcmpVarPrefix => {
            s.keys.clear();
            for &p in s.ptrs.iter() {
                s.keys.push(memcmp_var_prefix_key(slice_of(p)).raw());
            }
            fold_key_chain(cur, &s.keys, &s.prow, psma_populate);
        }
        KeyDerivation::IntervalCmpSat => {
            s.keys.clear();
            s.krows.clear();
            for (i, &p) in s.ptrs.iter().enumerate() {
                match interval_cmp_key(slice_of(p)) {
                    Some(k) => {
                        s.keys.push(k.raw());
                        s.krows.push(s.prow[i]);
                    }
                    None => cur.poisoned = true,
                }
            }
            fold_key_chain(cur, &s.keys, &s.krows, psma_populate);
        }
        KeyDerivation::TimetzUtc => {
            s.keys.clear();
            s.krows.clear();
            for (i, &p) in s.ptrs.iter().enumerate() {
                match timetz_utc_key(slice_of(p)) {
                    Some(k) => {
                        s.keys.push(k.raw());
                        s.krows.push(s.prow[i]);
                    }
                    None => cur.poisoned = true,
                }
            }
            fold_key_chain(cur, &s.keys, &s.krows, psma_populate);
        }
        KeyDerivation::PackedNumeric { scale } => {
            // Key + SUM + zero ride ONE pack per value; a pack failure
            // poisons the grain and contributes to none of the three (the
            // incumbent arm's validity coupling, exactly).
            s.keys.clear();
            s.krows.clear();
            for (i, &p) in s.ptrs.iter().enumerate() {
                match numeric_pack_at_scale(slice_of(p), scale) {
                    NumericPack::Packed(v) => {
                        s.keys.push(packed_numeric_key(v).raw());
                        s.krows.push(s.prow[i]);
                        cur.sum += v as i128;
                        if v == 0 {
                            cur.zero += 1;
                        }
                    }
                    _ => cur.poisoned = true,
                }
            }
            fold_key_chain(cur, &s.keys, &s.krows, psma_populate);
        }
        // `MetaProfile::derive` never pairs a word derivation with a byte
        // class; degrade (not panic) if a foreign profile appears.
        _ => {
            debug_assert!(false, "word key derivation on a byte class");
            cur.poisoned = true;
        }
    }

    // Length stats (varlena payload units; BOTH units for text — #80).
    if profile.len_stats != LenStats::None {
        let chars = profile.len_stats == LenStats::BytesAndChars;
        for &p in s.ptrs.iter() {
            let blen = p.1;
            cur.blen_min = cur.blen_min.min(blen);
            cur.blen_max = cur.blen_max.max(blen);
            cur.blen_sum += blen as u64;
            if chars {
                let clen = utf8_char_count(slice_of(p));
                cur.clen_min = cur.clen_min.min(clen);
                cur.clen_max = cur.clen_max.max(clen);
                if clen != blen {
                    cur.ascii = false;
                }
            }
        }
        cur.have_len = true;
    }

    // Hash lane.
    if profile.hashes_values() {
        s.h1.clear();
        s.h2.clear();
        for &p in s.ptrs.iter() {
            let (h1, h2) = meta_hash128(slice_of(p));
            s.h1.push(h1);
            s.h2.push(h2);
        }
        feed_hll_bloom(profile, cur, &s.h1, &s.h2);
        if let Some(d) = dist {
            for i in 0..s.ptrs.len() {
                if i + DIST_PREFETCH_AHEAD < s.h1.len() {
                    d.prefetch(s.h1[i + DIST_PREFETCH_AHEAD]);
                }
                d.observe_hashed(s.h1[i], s.h2[i], slice_of(s.ptrs[i]));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// shared folds
// ---------------------------------------------------------------------------

/// The order-sensitive key chain over one span's derived-key subsequence
/// (row order; `keys[i]` at granule-relative row `rows[i]`): first/last
/// key, the asc/desc/ties transition fold (carry-in from `cur.last_key`),
/// min/max, and the PSMA keybuf extension. Exactly `observe_value_at`'s
/// per-key updates, chained.
fn fold_key_chain(cur: &mut CurGranule, keys: &[i64], rows: &[u32], stage_psma: bool) {
    if keys.is_empty() {
        return;
    }
    if stage_psma {
        debug_assert_eq!(keys.len(), rows.len(), "keybuf staging misaligned");
        cur.keybuf
            .extend(rows.iter().zip(keys.iter()).map(|(&r, &k)| (r as u16, k)));
    }
    let (mut asc, mut desc, mut ties) = (cur.asc, cur.desc, cur.ties);
    if cur.have_key {
        let (prev, k0) = (cur.last_key, keys[0]);
        asc &= k0 >= prev;
        desc &= k0 <= prev;
        ties |= k0 == prev;
    } else {
        cur.first_key = keys[0];
        cur.have_key = true;
    }
    // Adjacent-pair transition fold: branchless, auto-vectorizable.
    for i in 1..keys.len() {
        let (a, b) = (keys[i - 1], keys[i]);
        asc &= b >= a;
        desc &= b <= a;
        ties |= b == a;
    }
    let (mut kmin, mut kmax) = (cur.kmin, cur.kmax);
    for &k in keys {
        kmin = kmin.min(k);
        kmax = kmax.max(k);
    }
    cur.asc = asc;
    cur.desc = desc;
    cur.ties = ties;
    cur.kmin = kmin;
    cur.kmax = kmax;
    cur.last_key = keys[keys.len() - 1];
}

/// Feed HLL + the staged bloom block from precomputed hash arrays. Both
/// accumulators are idempotent + commutative, so array order is free; the
/// arrays are row order anyway.
fn feed_hll_bloom(profile: &MetaProfile, cur: &mut CurGranule, h1: &[u64], h2: &[u64]) {
    if profile.ndv {
        #[cfg(test)]
        let skip_last = seed::armed();
        #[cfg(not(test))]
        let skip_last = false;
        let upto = if skip_last { h1.len().saturating_sub(1) } else { h1.len() };
        for &h in &h1[..upto] {
            cur.hll.observe_hash(h);
        }
    }
    if let Some(block) = cur.bloom.as_deref_mut() {
        for i in 0..h1.len() {
            bloom_insert_hashed(block, BLOOM_K_DEFAULT, h1[i], h2[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// dict fold-from-codes
// ---------------------------------------------------------------------------

/// Build the once-per-stream code tables from the DICT-DEDUP feed's
/// byte-rank entry list (index == global code; strictly byte-ascending —
/// the dict build's contractual order certificate).
pub(crate) fn build_dict_tables(
    profile: &MetaProfile,
    entries: &[(Vec<u8>, u64)],
) -> DictFoldTables {
    let keys = match profile.key {
        KeyDerivation::MemcmpVarPrefix => Some(
            entries
                .iter()
                .map(|(b, _)| memcmp_var_prefix_key(b).raw())
                .collect(),
        ),
        _ => None,
    };
    let chars = profile.len_stats == LenStats::BytesAndChars;
    let hashes = profile.hashes_values();
    let (mut h1, mut h2) = (Vec::new(), Vec::new());
    if hashes {
        h1.reserve(entries.len());
        h2.reserve(entries.len());
        for (b, _) in entries {
            let (a, c) = meta_hash128(b);
            h1.push(a);
            h2.push(c);
        }
    }
    DictFoldTables {
        keys,
        blen: entries
            .iter()
            .map(|(b, _)| b.len().min(u32::MAX as usize) as u32)
            .collect(),
        clen: if chars {
            entries.iter().map(|(b, _)| utf8_char_count(b)).collect()
        } else {
            Vec::new()
        },
        h1,
        h2,
        hashes,
        chars,
    }
}

/// Fold one dict-stream granule FROM ITS CODES (`input.datums` = global
/// codes, row-dense, null slots carrying 0 — skipped via validity). Every
/// fact matches the hydrated-value walk over the same granule:
/// - keys/lengths are table lookups per row IN ROW ORDER (order-sensitive
///   facts + multiplicity sums preserved);
/// - bloom/HLL insert once per DISTINCT code (idempotent accumulators —
///   identical bits/registers to per-row inserts);
/// - the DistAcc is NOT fed here: the codes face is gated on the feed
///   being staged, which disarmed the accumulator (DICT-DEDUP).
pub(crate) fn observe_codes_granule(
    profile: &MetaProfile,
    psma_populate: bool,
    cur: &mut CurGranule,
    t: &DictFoldTables,
    s: &mut BatchScratch,
    base: u32,
    input: &EncodeInput<'_>,
) {
    // Densify valid (row, code).
    s.codes.clear();
    s.rows.clear();
    for r in 0..input.rows {
        if input.valid(r) {
            let c = input.datums[r as usize];
            debug_assert!((c as usize) < t.blen.len(), "code out of dict range");
            s.codes.push(c as u32);
            s.rows.push(base + r);
        }
    }
    cur.nonnull += s.codes.len() as u32;
    if s.codes.is_empty() {
        return;
    }

    // Keys through the code table (row order — the chain fold is shared
    // with the value walks).
    if let Some(kt) = &t.keys {
        s.keys.clear();
        for &c in &s.codes {
            s.keys.push(kt[c as usize]);
        }
        fold_key_chain(cur, &s.keys, &s.rows, psma_populate);
    }

    // Length stats by lookup (sum carries multiplicity per row; min/max
    // idempotent either way).
    if profile.len_stats != LenStats::None {
        for &c in &s.codes {
            let blen = t.blen[c as usize];
            cur.blen_min = cur.blen_min.min(blen);
            cur.blen_max = cur.blen_max.max(blen);
            cur.blen_sum += blen as u64;
            if t.chars {
                let clen = t.clen[c as usize];
                cur.clen_min = cur.clen_min.min(clen);
                cur.clen_max = cur.clen_max.max(clen);
                if clen != blen {
                    cur.ascii = false;
                }
            }
        }
        cur.have_len = true;
    }

    // Bloom/HLL once per distinct code in the granule.
    if t.hashes {
        let want_words = t.blen.len().div_ceil(64);
        if s.seen.len() < want_words {
            s.seen.resize(want_words, 0);
        }
        s.seen_list.clear();
        for &c in &s.codes {
            let (w, b) = ((c / 64) as usize, c % 64);
            if s.seen[w] >> b & 1 == 0 {
                s.seen[w] |= 1 << b;
                s.seen_list.push(c);
            }
        }
        #[cfg(test)]
        let skip_last = seed::armed();
        #[cfg(not(test))]
        let skip_last = false;
        for (i, &c) in s.seen_list.iter().enumerate() {
            if profile.ndv && !(skip_last && i + 1 == s.seen_list.len()) {
                cur.hll.observe_hash(t.h1[c as usize]);
            }
            if let Some(block) = cur.bloom.as_deref_mut() {
                bloom_insert_hashed(block, BLOOM_K_DEFAULT, t.h1[c as usize], t.h2[c as usize]);
            }
        }
        // Reset via the list (granule-local distinctness).
        for &c in &s.seen_list {
            s.seen[(c / 64) as usize] &= !(1u64 << (c % 64));
        }
    }
}
