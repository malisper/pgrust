//! [`ColumnMeta`] — the [`ColumnMetaBuilder`] implementation (spec §19.7:
//! "M3-E builds, M3-D drives"). One instance per column per part; the seal
//! driver calls `observe_granule` (one or more times per granule, granules
//! in order), `seal_granule(g)` after each granule, `seal_band(b)` after
//! each band's granules, `seal_part()` once, then `aux_sections()`.
//!
//! Laws carried:
//! - **Determinism** (spec §1): every output is a pure function of the
//!   observed values — no clocks, no addresses, no iteration-order
//!   dependence.
//! - **Two-witness null law** (spec §6.6): `nonnull` here is the
//!   stats-side witness; the writer cross-checks it against bitmap
//!   popcounts (M3-D's born-RED seeded-skew test drives both).
//! - **Degrade, never guess**: a value that defeats its derivation
//!   (numeric that does not rescale exactly, malformed fixed image) poisons
//!   the GRAIN to `key_kind = Absent` — and, for the packed lane, the
//!   sum/zero validity rides the same witness (`profile.rs` coupling).
//!   Nothing panics on data; released asserts guard only DRIVER protocol
//!   (out-of-order seals — code bugs, not data).
//! - **Coarse sortedness law**: coarse keys claim Ascending/Descending
//!   only from STRICTLY monotone key sequences (a tie can hide an
//!   inversion under the prefix) and never claim Constant — the sortedness
//!   analog of the coarse-key law, enforced at build so downstream
//!   consumers (bloom arming here; ordering consumers later) inherit it.
//! - Text length stats carry BOTH units (the #80 lesson): bytes AND UTF-8
//!   chars, per the profile.
//!
//! The one `unsafe` site is the datum-image read, riding the SAME caller
//! contract as `format::abi::verify_roundtrip` (EncodeInput datums are
//! writer-built and obey the pointer-class contract).

use crate::bloom::{
    bloom_armed, bloom_insert_hashed, BLOOM_BYTES_PER_GRANULE_DEFAULT, BLOOM_K_DEFAULT,
};
use crate::format::abi::{datum_canonical_bytes, ColumnMetaBuilder, EncodeInput};
use crate::format::geom::{GRANULES_PER_BAND, GRANULE_ROWS};
use crate::format::meta::{KeyKind, Sortedness, StatsRecord, STATSF_ALL_ASCII, STATSF_COMPUTED};
use crate::format::part::SectionKind;
use crate::format::wire::{put_bytes, put_u32};
use crate::hash::meta_hash128;
use crate::key::{
    bool_key, f32_key_from_datum, f64_key_from_datum, interval_cmp_key, memcmp_fixed_exact_key,
    memcmp_fixed_prefix_key, memcmp_var_prefix_key, numeric_pack_at_scale, packed_numeric_key,
    signed_word_key, timetz_utc_key, unsigned_flip_key, unsigned_small_key, DerivedKey,
    KeyDerivation, NumericPack,
};
use crate::ndv::Hll;
use crate::profile::{LenStats, MetaProfile, SumKind, ZeroKind};
use crate::psma::{psma_index, psma_shift, PsmaAcc};

/// Per-value observation extracted from one datum (the class view).
#[derive(Clone, Copy)]
enum ValueView<'a> {
    Word(u64),
    Bytes(&'a [u8]),
}

/// The in-flight granule accumulator. Fields are `pub(crate)` for the
/// D-STATS batched kernels ([`crate::batch`]), which fold into the SAME
/// accumulator the per-value walk uses — one state, two fold shells.
pub(crate) struct CurGranule {
    pub(crate) granule: u32,
    pub(crate) rows: u32,
    pub(crate) nonnull: u32,
    /// A value defeated its derivation (or its image was unreadable):
    /// keys + coupled aggregates degrade to Absent at seal.
    pub(crate) poisoned: bool,
    pub(crate) kmin: i64,
    pub(crate) kmax: i64,
    pub(crate) first_key: i64,
    pub(crate) last_key: i64,
    pub(crate) have_key: bool,
    /// Non-strict monotonicity over the nonnull key sequence.
    pub(crate) asc: bool,
    pub(crate) desc: bool,
    pub(crate) ties: bool,
    pub(crate) sum: i128,
    pub(crate) zero: u64,
    pub(crate) blen_min: u32,
    pub(crate) blen_max: u32,
    pub(crate) blen_sum: u64,
    pub(crate) clen_min: u32,
    pub(crate) clen_max: u32,
    pub(crate) have_len: bool,
    /// P-2 all-ASCII witness accumulator: true while every observed
    /// BytesAndChars value has char count == byte count (pure ASCII).
    /// Vacuously true over zero observations (char folds over an empty
    /// span are exact).
    pub(crate) ascii: bool,
    pub(crate) hll: Hll,
    /// (granule-relative row, raw key) — PSMA build input at seal.
    pub(crate) keybuf: Vec<(u16, i64)>,
    /// Staged bloom block (kept only if the arming policy says so at seal).
    pub(crate) bloom: Option<Vec<u8>>,
}

impl CurGranule {
    fn new(granule: u32, profile: &MetaProfile, bloom_populate: bool) -> CurGranule {
        CurGranule {
            granule,
            rows: 0,
            nonnull: 0,
            poisoned: false,
            kmin: i64::MAX,
            kmax: i64::MIN,
            first_key: 0,
            last_key: 0,
            have_key: false,
            asc: true,
            desc: true,
            ties: false,
            sum: 0,
            zero: 0,
            blen_min: u32::MAX,
            blen_max: 0,
            blen_sum: 0,
            clen_min: u32::MAX,
            clen_max: 0,
            have_len: false,
            ascii: true,
            hll: Hll::default(),
            keybuf: Vec::new(),
            bloom: if profile.eq_bloomable && bloom_populate {
                Some(vec![0u8; BLOOM_BYTES_PER_GRANULE_DEFAULT as usize])
            } else {
                None
            },
        }
    }
}

/// One sealed granule's carry-forward state (record + merge inputs).
struct SealedGranule {
    rec: StatsRecord,
    /// First/last raw keys of the nonnull sequence (valid iff the record's
    /// key kind is not Absent) — band/part boundary sortedness inputs.
    first_key: i64,
    last_key: i64,
    hll: Hll,
    psma: Option<Vec<u8>>,
    bloom: Option<Vec<u8>>,
}

/// The per-column meta builder (see module doc).
pub struct ColumnMeta {
    profile: MetaProfile,
    cur: Option<CurGranule>,
    granules: Vec<SealedGranule>,
    band_records: Vec<StatsRecord>,
    /// The next granule ordinal `seal_granule` will accept — the driver
    /// protocol tooth that survives the two-protocol widening below.
    next_seal: u32,
    part_sealed: bool,
    /// ST-1 (OD-2): the part-grain distribution accumulator — armed iff
    /// `profile.ndv` (value hashing sound ⇒ value identity sound; the
    /// collation gate applies automatically). DISTACC-KILL: an
    /// open-addressing table over the `meta_hash128` this builder already
    /// computes — byte order is recovered once at finalize, so no
    /// hash-order leak into sidecar bytes (see `sketch.rs`).
    dist: Option<crate::sketch::DistAcc>,
    /// DICT-DEDUP: the externally maintained counted distinct set (the
    /// dict election's byte-rank-sorted entries + exact counts). When the
    /// driver hands it over (`set_distribution_feed`, pre-observation),
    /// `dist` is DISARMED — the per-value accumulation this column would
    /// have paid is exactly the double-accounting the feed removes — and
    /// `distribution()` serves through the same finish path the
    /// accumulator uses (`sketch::distribution_from_sorted_counts`).
    dist_feed: Option<Vec<(Vec<u8>, u64)>>,
    /// OD-10/OD-11 populate kill switches (the M3 psma/bloom A/B apparatus,
    /// SEAL-FUSION measurement-honesty fix): an OFF arm must skip the
    /// COMPUTE — the per-value bloom inserts, the PSMA key staging and
    /// block build — not just the section append the seal driver already
    /// suppresses, or the A/B measures byte attribution while claiming to
    /// price populate CPU. Default ON; production never sets these; the
    /// env read mirrors the seal driver's own parsing.
    psma_populate: bool,
    bloom_populate: bool,
    /// SEAL-FUSION (#20): the part-grain merged HLL, cached at `seal_part`
    /// so `aux_sections` never re-merges the granule registers.
    part_hll: Option<Hll>,
    /// SEAL-SPEED-2 fold fusion: the in-flight granule's row base for the
    /// streaming face (`begin_granule_rows` latches the rows already
    /// observed into this granule; `observe_row(row)` folds at
    /// `stream_base + row` — exactly the classic walk's `base + r`).
    stream_base: u32,
    /// D-STATS batched-fold switch (`PGRUST_SEAL_BATCH_FOLD`, the
    /// `populate_on` parse — default ON; `=0|off` restores the per-value
    /// incumbent walk below, which stays as the control arm). A perf dial,
    /// never a bytes dial: the equivalence battery + the rig dirshas pin
    /// both shells output-identical.
    batch_fold: bool,
    /// D-STATS: reusable span scratch for the batched kernels.
    scratch: crate::batch::BatchScratch,
    /// D-STATS dict fold-from-codes: once-per-stream code tables, built
    /// lazily from the DICT-DEDUP feed on the first codes-face granule.
    dict_tables: Option<Box<crate::batch::DictFoldTables>>,
}

/// The OD-10/OD-11 populate-switch parse (the seal driver's own: unset or
/// anything but "0"/"off" = populate).
fn populate_on(name: &str) -> bool {
    !matches!(std::env::var(name).as_deref(), Ok("0") | Ok("off"))
}

impl ColumnMeta {
    pub fn new(profile: MetaProfile) -> ColumnMeta {
        ColumnMeta::with_batch_fold(profile, populate_on("PGRUST_SEAL_BATCH_FOLD"))
    }

    /// Arm-explicit constructor (the `DistAcc::with_hash_arm` class): the
    /// equivalence battery drives both fold shells side by side without
    /// touching process env. Production always enters through
    /// [`ColumnMeta::new`].
    pub fn with_batch_fold(profile: MetaProfile, batch_fold: bool) -> ColumnMeta {
        ColumnMeta {
            dist: if profile.ndv {
                Some(crate::sketch::DistAcc::default())
            } else {
                None
            },
            dist_feed: None,
            profile,
            cur: None,
            granules: Vec::new(),
            band_records: Vec::new(),
            next_seal: 0,
            part_sealed: false,
            psma_populate: populate_on("PGRUST_PGRC2_PSMA_POPULATE"),
            bloom_populate: populate_on("PGRUST_PGRC2_BLOOM_POPULATE"),
            part_hll: None,
            stream_base: 0,
            batch_fold,
            scratch: crate::batch::BatchScratch::default(),
            dict_tables: None,
        }
    }

    pub fn profile(&self) -> &MetaProfile {
        &self.profile
    }

    /// Derive one value's zone key (mirrors `lower.rs` — the SAME
    /// transforms, from stored values instead of constants; floats do NOT
    /// abstain on NaN here: the canonical order key absorbs NaN exactly,
    /// and the lowering-side contains_nan gate is a separate law).
    fn derive_key_at(profile: &MetaProfile, view: &ValueView<'_>) -> DerivedKey {
        match (profile.key, view) {
            (KeyDerivation::None, _) => DerivedKey::Absent,
            (KeyDerivation::SignedWord, ValueView::Word(w)) => {
                DerivedKey::Exact(signed_word_key(*w))
            }
            (KeyDerivation::UnsignedWordSmall, ValueView::Word(w)) => {
                DerivedKey::Exact(unsigned_small_key(*w))
            }
            (KeyDerivation::UnsignedWordFlip, ValueView::Word(w)) => {
                DerivedKey::Exact(unsigned_flip_key(*w))
            }
            (KeyDerivation::Bool, ValueView::Word(w)) => DerivedKey::Exact(bool_key(*w)),
            (KeyDerivation::Float32, ValueView::Word(w)) => {
                DerivedKey::Exact(f32_key_from_datum(*w))
            }
            (KeyDerivation::Float64, ValueView::Word(w)) => {
                DerivedKey::Exact(f64_key_from_datum(*w))
            }
            (KeyDerivation::MemcmpFixed { len }, ValueView::Bytes(b)) => {
                if b.len() != len as usize {
                    DerivedKey::Absent
                } else if len <= 8 {
                    DerivedKey::Exact(memcmp_fixed_exact_key(b))
                } else {
                    DerivedKey::Coarse(memcmp_fixed_prefix_key(b))
                }
            }
            (KeyDerivation::MemcmpVarPrefix, ValueView::Bytes(b)) => {
                DerivedKey::Coarse(memcmp_var_prefix_key(b))
            }
            (KeyDerivation::PackedNumeric { scale }, ValueView::Bytes(b)) => {
                match numeric_pack_at_scale(b, scale) {
                    NumericPack::Packed(p) => DerivedKey::Exact(packed_numeric_key(p)),
                    _ => DerivedKey::Absent,
                }
            }
            (KeyDerivation::IntervalCmpSat, ValueView::Bytes(b)) => match interval_cmp_key(b) {
                Some(k) => DerivedKey::Coarse(k),
                None => DerivedKey::Absent,
            },
            (KeyDerivation::TimetzUtc, ValueView::Bytes(b)) => match timetz_utc_key(b) {
                Some(k) => DerivedKey::Coarse(k),
                None => DerivedKey::Absent,
            },
            // A word derivation on a byte view (or vice versa) cannot be
            // built through `MetaProfile::derive`.
            _ => DerivedKey::Absent,
        }
    }

    fn observe_value_at(
        profile: &MetaProfile,
        psma_populate: bool,
        cur: &mut CurGranule,
        dist: Option<&mut crate::sketch::DistAcc>,
        row_in_granule: u32,
        view: ValueView<'_>,
    ) {
        // The packed-numeric lane packs ONCE per value; key + sum + zero all
        // ride the result (and its failure poisons all three together —
        // the validity coupling).
        let packed: Option<NumericPack> = match (profile.key, view) {
            (KeyDerivation::PackedNumeric { scale }, ValueView::Bytes(b)) => {
                Some(numeric_pack_at_scale(b, scale))
            }
            _ => None,
        };
        // Zone key.
        if profile.key != KeyDerivation::None {
            let derived = match packed {
                Some(NumericPack::Packed(p)) => DerivedKey::Exact(packed_numeric_key(p)),
                Some(_) => DerivedKey::Absent,
                None => ColumnMeta::derive_key_at(profile, &view),
            };
            match derived.typed() {
                Some(k) => {
                    let raw = k.raw();
                    if cur.have_key {
                        if raw < cur.last_key {
                            cur.asc = false;
                        }
                        if raw > cur.last_key {
                            cur.desc = false;
                        }
                        if raw == cur.last_key {
                            cur.ties = true;
                        }
                    } else {
                        cur.first_key = raw;
                        cur.have_key = true;
                    }
                    cur.last_key = raw;
                    cur.kmin = cur.kmin.min(raw);
                    cur.kmax = cur.kmax.max(raw);
                    // PSMA staging (the finalize-time block build's input):
                    // skipped under the OD-10 populate-off arm so the A/B
                    // prices the COMPUTE, not just the section bytes.
                    if psma_populate {
                        cur.keybuf.push((row_in_granule as u16, raw));
                    }
                }
                None => {
                    cur.poisoned = true;
                }
            }
        }
        // SUM / zero-count.
        match profile.sum {
            SumKind::None => {}
            SumKind::SignedWord => {
                if let ValueView::Word(w) = view {
                    cur.sum += w as i64 as i128;
                }
            }
            SumKind::UnsignedWord => {
                if let ValueView::Word(w) = view {
                    cur.sum += w as i128;
                }
            }
            SumKind::BoolTrues => {
                if let ValueView::Word(w) = view {
                    cur.sum += (w != 0) as i128;
                }
            }
            SumKind::PackedI64 => {
                if let Some(NumericPack::Packed(p)) = packed {
                    cur.sum += p as i128;
                    if p == 0 {
                        cur.zero += 1;
                    }
                }
                // Pack failure already poisoned via the key path.
            }
        }
        match profile.zero {
            ZeroKind::None | ZeroKind::PackedI64 => {} // packed handled with the sum
            ZeroKind::Word => {
                if let ValueView::Word(w) = view {
                    if w == 0 {
                        cur.zero += 1;
                    }
                }
            }
            ZeroKind::Float => {
                if let ValueView::Word(w) = view {
                    let is_zero = match profile.class {
                        crate::format::class::StorageClass::F32 => f32::from_bits(w as u32) == 0.0,
                        _ => f64::from_bits(w) == 0.0,
                    };
                    if is_zero {
                        cur.zero += 1;
                    }
                }
            }
            ZeroKind::BoolFalse => {
                if let ValueView::Word(w) = view {
                    if w == 0 {
                        cur.zero += 1;
                    }
                }
            }
        }
        // Length stats (varlena payload units; BOTH units for text).
        if profile.len_stats != LenStats::None {
            if let ValueView::Bytes(b) = &view {
                let blen = b.len().min(u32::MAX as usize) as u32;
                cur.blen_min = cur.blen_min.min(blen);
                cur.blen_max = cur.blen_max.max(blen);
                cur.blen_sum += blen as u64;
                if profile.len_stats == LenStats::BytesAndChars {
                    let clen = utf8_char_count(b);
                    cur.clen_min = cur.clen_min.min(clen);
                    cur.clen_max = cur.clen_max.max(clen);
                    // P-2 witness: ASCII iff chars == bytes (free — both
                    // counts are already in hand; no extra scan).
                    if clen as usize != b.len() {
                        cur.ascii = false;
                    }
                }
                cur.have_len = true;
            }
        }
        // Equality machinery: hash once, feed bloom + NDV.
        if profile.hashes_values() {
            let mut scratch = [0u8; 8];
            let canonical: &[u8] = match &view {
                ValueView::Bytes(b) => b,
                ValueView::Word(w) => {
                    // The §18.1 canonical shape for word classes.
                    match profile.class {
                        crate::format::class::StorageClass::ByvalWord { width, .. } => {
                            scratch = w.to_le_bytes();
                            &scratch[..width as usize]
                        }
                        crate::format::class::StorageClass::F32 => {
                            scratch = w.to_le_bytes();
                            &scratch[..4]
                        }
                        crate::format::class::StorageClass::F64 => {
                            scratch = w.to_le_bytes();
                            &scratch[..8]
                        }
                        crate::format::class::StorageClass::Bool => {
                            scratch[0] = (*w != 0) as u8;
                            &scratch[..1]
                        }
                        _ => unreachable!("word view on a pointer class"),
                    }
                }
            };
            let (h1, h2) = meta_hash128(canonical);
            if profile.ndv {
                cur.hll.observe_hash(h1);
            }
            if let Some(block) = cur.bloom.as_deref_mut() {
                bloom_insert_hashed(block, BLOOM_K_DEFAULT, h1, h2);
            }
            if let Some(d) = dist {
                // DISTACC-KILL: feed the hash already in hand — the
                // accumulator's probe rides it and never re-hashes.
                d.observe_hashed(h1, h2, canonical);
            }
        }
    }

    /// Merge sealed-granule records into one coarser-grain record.
    /// `granules` is the contributing slice IN ORDER.
    fn merge(&self, granules: &[SealedGranule]) -> StatsRecord {
        self.merge2(granules).0
    }

    /// [`ColumnMeta::merge`] returning the merged HLL as well (SEAL-FUSION
    /// #20: `seal_part` caches the part-grain merge so `aux_sections` never
    /// re-merges the same registers).
    fn merge2(&self, granules: &[SealedGranule]) -> (StatsRecord, Option<Hll>) {
        let profile = &self.profile;
        let mut rec = StatsRecord::absent();
        // The computed-stats witness (#598): this record was produced by
        // the REAL builder, so its profile-computed aggregates hold
        // computed values (uncomputed fields still read as absent through
        // the profile + key-kind coupling below). Stand-in records carry
        // flags == 0 and the answer face declines them.
        rec.flags |= STATSF_COMPUTED;
        // nonnull (saturating at the frozen u32 field — parts beyond 2^32
        // nonnull rows cannot witness exact counts and lose AllPass-grade
        // verdicts at that grain; conservative).
        let nonnull_sum: u64 = granules.iter().map(|g| g.rec.nonnull as u64).sum();
        rec.nonnull = nonnull_sum.min(u32::MAX as u64) as u32;
        // Contributing = granules with at least one nonnull value.
        let contributing: Vec<&SealedGranule> =
            granules.iter().filter(|g| g.rec.nonnull > 0).collect();
        // Keys: every contributing granule must carry the derivation's
        // kind; anything else (a poisoned granule) degrades the grain.
        let want_kind = profile.key.kind();
        let keys_ok = want_kind != KeyKind::Absent
            && !contributing.is_empty()
            && contributing
                .iter()
                .all(|g| g.rec.key_kind == want_kind.as_u8());
        if keys_ok {
            rec.key_kind = want_kind.as_u8();
            rec.min_key = contributing
                .iter()
                .map(|g| g.rec.min_key)
                .min()
                .expect("nonempty");
            rec.max_key = contributing
                .iter()
                .map(|g| g.rec.max_key)
                .max()
                .expect("nonempty");
        }
        // Sums / zero counts (PackedI64 validity rides the key witness).
        let sums_valid = match profile.sum {
            SumKind::None => false,
            SumKind::PackedI64 => keys_ok,
            _ => true,
        };
        if sums_valid {
            rec.sum_i128 = granules.iter().map(|g| g.rec.sum_i128).sum();
        }
        let zeros_valid = match profile.zero {
            ZeroKind::None => false,
            ZeroKind::PackedI64 => keys_ok,
            _ => true,
        };
        if zeros_valid {
            rec.zero_count = granules.iter().map(|g| g.rec.zero_count).sum();
        }
        // Length stats: merge over contributing granules only (empty
        // granules carry zeroed fields that must not pull mins down).
        if profile.len_stats != LenStats::None && !contributing.is_empty() {
            rec.byte_len_min = contributing
                .iter()
                .map(|g| g.rec.byte_len_min)
                .min()
                .expect("nonempty");
            rec.byte_len_max = contributing
                .iter()
                .map(|g| g.rec.byte_len_max)
                .max()
                .expect("nonempty");
            rec.byte_len_sum = granules.iter().map(|g| g.rec.byte_len_sum).sum();
            if profile.len_stats == LenStats::BytesAndChars {
                rec.char_len_min = contributing
                    .iter()
                    .map(|g| g.rec.char_len_min)
                    .min()
                    .expect("nonempty");
                rec.char_len_max = contributing
                    .iter()
                    .map(|g| g.rec.char_len_max)
                    .max()
                    .expect("nonempty");
            }
        }
        // P-2 all-ASCII witness at the coarser grain: carried iff every
        // contributing finer record carries it (P-1 grain-vector law; empty
        // granules mint the vacuous bit at finalize, so ALL-quantified).
        if profile.len_stats == LenStats::BytesAndChars
            && !granules.is_empty()
            && granules.iter().all(|g| g.rec.flags & STATSF_ALL_ASCII != 0)
        {
            rec.flags |= STATSF_ALL_ASCII;
        }
        // NDV: mergeable registers (max-per-register).
        let mut merged_hll: Option<Hll> = None;
        if profile.ndv && !granules.is_empty() {
            let mut hll = Hll::default();
            for g in granules {
                hll.merge(&g.hll);
            }
            rec.ndv_est = hll.estimate();
            merged_hll = Some(hll);
        }
        // Sortedness: chain contributing granules; boundaries must be
        // ordered — non-strictly for exact keys, STRICTLY for coarse (the
        // coarse sortedness law); Constant is exact-only.
        rec.sortedness = self
            .merge_sortedness(&contributing, want_kind, keys_ok)
            .as_u8();
        (rec, merged_hll)
    }

    fn merge_sortedness(
        &self,
        contributing: &[&SealedGranule],
        kind: KeyKind,
        keys_ok: bool,
    ) -> Sortedness {
        if !keys_ok || contributing.is_empty() {
            return Sortedness::Unknown;
        }
        let coarse = kind == KeyKind::Coarse;
        let s = |g: &SealedGranule| {
            Sortedness::from_u8(g.rec.sortedness).unwrap_or(Sortedness::Unknown)
        };
        // Constant: exact-only, all granules constant on the same key.
        if !coarse
            && contributing.iter().all(|g| s(g) == Sortedness::Constant)
            && contributing
                .windows(2)
                .all(|w| w[0].last_key == w[1].first_key)
        {
            return Sortedness::Constant;
        }
        let asc_ok = contributing
            .iter()
            .all(|g| matches!(s(g), Sortedness::Ascending | Sortedness::Constant))
            && contributing.windows(2).all(|w| {
                if coarse {
                    w[0].last_key < w[1].first_key
                } else {
                    w[0].last_key <= w[1].first_key
                }
            });
        if asc_ok {
            return Sortedness::Ascending;
        }
        let desc_ok = contributing
            .iter()
            .all(|g| matches!(s(g), Sortedness::Descending | Sortedness::Constant))
            && contributing.windows(2).all(|w| {
                if coarse {
                    w[0].last_key > w[1].first_key
                } else {
                    w[0].last_key >= w[1].first_key
                }
            });
        if desc_ok {
            return Sortedness::Descending;
        }
        Sortedness::Unknown
    }
}

pub(crate) fn utf8_char_count(b: &[u8]) -> u32 {
    b.iter().filter(|&&x| (x & 0xC0) != 0x80).count() as u32
}

impl ColumnMetaBuilder for ColumnMeta {
    /// The classic granule walk — since the SEAL-SPEED-2 fold fusion this
    /// DELEGATES to the streaming face (one implementation, no drift): the
    /// fold sequence per row is identical whether the driver streams rows
    /// from inside an encoder's emit loop or hands a whole granule here.
    fn observe_granule(&mut self, input: &EncodeInput<'_>, granule: u32) {
        assert_eq!(
            input.class, self.profile.class,
            "EncodeInput class disagrees with the profile (driver protocol)"
        );
        self.begin_granule_rows(granule);
        self.observe_rows(input, 0, input.rows);
        self.end_granule_rows(input.rows);
    }

    // ---- SEAL-SPEED-2 fold fusion: the streaming per-row face ------------

    fn row_observe_supported(&self) -> bool {
        true
    }

    fn begin_granule_rows(&mut self, granule: u32) {
        assert!(
            !self.part_sealed,
            "observe after seal_part (driver protocol)"
        );
        // Granule ordering: continue the in-flight granule or start the
        // next one in order (RELEASED asserts — driver protocol, not data).
        let cur_state = match self.cur.take() {
            Some(c) if c.granule == granule => c,
            // The driver advanced to the next granule WITHOUT an
            // interleaved `seal_granule`. That is M3-D's actual protocol:
            // `seal_part` observes every granule during the encode pass and
            // seals them all afterwards, so the in-flight granule must be
            // closed here. Both protocols are accepted; what is still
            // refused is a skipped or out-of-order granule.
            Some(c) => {
                assert_eq!(granule, c.granule + 1, "observe out of granule order");
                self.finalize(c);
                CurGranule::new(granule, &self.profile, self.bloom_populate)
            }
            None => {
                assert_eq!(
                    granule,
                    self.granules.len() as u32,
                    "observe skipped a granule"
                );
                CurGranule::new(granule, &self.profile, self.bloom_populate)
            }
        };
        self.stream_base = cur_state.rows;
        self.cur = Some(cur_state);
    }

    fn observe_rows(&mut self, input: &EncodeInput<'_>, first: u32, n: u32) {
        // One take/put per SPAN (frame grain): the fold loop below runs on
        // a LOCAL accumulator exactly as the pre-fusion walk did — tight,
        // monomorphic, no per-row Option/indirection tax.
        let mut cur = self.cur.take().expect("begin_granule_rows first");
        let mut self_dist = self.dist.take();
        let base = self.stream_base;
        // D-STATS: the batched fold shell (default arm). The per-value
        // walk below is the incumbent, kept UNTOUCHED as the
        // `PGRUST_SEAL_BATCH_FOLD=0` control arm — both shells fold into
        // the same accumulator and are pinned output-identical by the
        // equivalence battery.
        if self.batch_fold {
            crate::batch::observe_rows_batched(
                &self.profile,
                self.psma_populate,
                &mut cur,
                self_dist.as_mut(),
                &mut self.scratch,
                base,
                input,
                first,
                n,
            );
            self.dist = self_dist;
            self.cur = Some(cur);
            return;
        }
        for r in first..first + n {
            if !input.valid(r) {
                continue;
            }
            cur.nonnull += 1;
            let datum = input.datums[r as usize];
            let mut scratch = [0u8; 8];
            let view = match self.profile.class {
                crate::format::class::StorageClass::ByvalWord { .. }
                | crate::format::class::StorageClass::F32
                | crate::format::class::StorageClass::F64
                | crate::format::class::StorageClass::Bool => ValueView::Word(datum),
                crate::format::class::StorageClass::Fixed { .. }
                | crate::format::class::StorageClass::VarlenaVerbatim => {
                    // SAFETY: EncodeInput datums obey the pointer-class
                    // contract by construction (the seal driver built them
                    // — the same contract `abi::verify_roundtrip`
                    // documents). Byval classes never reach this arm.
                    match unsafe {
                        datum_canonical_bytes(self.profile.class, datum, &mut scratch)
                    } {
                        Ok(bytes) => ValueView::Bytes(bytes),
                        Err(_) => {
                            // Unreadable image: degrade, never guess (and
                            // never panic on data).
                            debug_assert!(false, "unreadable datum image at meta build");
                            cur.poisoned = true;
                            continue;
                        }
                    }
                }
            };
            ColumnMeta::observe_value_at(
                &self.profile,
                self.psma_populate,
                &mut cur,
                self_dist.as_mut(),
                base + r,
                view,
            );
        }
        self.dist = self_dist;
        self.cur = Some(cur);
    }

    fn end_granule_rows(&mut self, rows: u32) {
        let cur = self.cur.as_mut().expect("begin_granule_rows first");
        cur.rows += rows;
        assert!(cur.rows <= GRANULE_ROWS, "granule overfilled (driver protocol)");
    }

    fn seal_granule(&mut self, granule: u32) -> StatsRecord {
        assert_eq!(granule, self.next_seal, "seal_granule out of order");
        self.next_seal += 1;
        // Close the in-flight granule, whichever protocol the driver used:
        // interleaved, `cur` IS this granule; observe-all-then-seal-all,
        // `cur` is the LAST observed granule and this is the first seal.
        if let Some(c) = self.cur.take() {
            self.finalize(c);
        }
        // Granules the driver never observed (an all-null tail, or a
        // stats-only harness) seal empty rather than inventing values.
        while granule as usize >= self.granules.len() {
            let g = self.granules.len() as u32;
            self.finalize(CurGranule::new(g, &self.profile, self.bloom_populate));
        }
        self.granules[granule as usize].rec
    }

    fn seal_band(&mut self, band: u32) -> StatsRecord {
        assert!(self.cur.is_none(), "seal_band with an open granule");
        assert_eq!(
            band,
            self.band_records.len() as u32,
            "seal_band out of order"
        );
        let lo = (band * GRANULES_PER_BAND) as usize;
        let hi = ((band + 1) * GRANULES_PER_BAND) as usize;
        assert!(
            self.granules.len() > lo,
            "seal_band before its granules sealed"
        );
        let hi = hi.min(self.granules.len());
        let rec = self.merge(&self.granules[lo..hi]);
        self.band_records.push(rec);
        rec
    }

    fn seal_part(&mut self) -> StatsRecord {
        assert!(!self.part_sealed, "seal_part twice");
        assert!(self.cur.is_none(), "seal_part with an open granule");
        assert_eq!(
            self.band_records.len(),
            self.granules.len().div_ceil(GRANULES_PER_BAND as usize),
            "seal_part before all bands sealed"
        );
        self.part_sealed = true;
        let (rec, hll) = self.merge2(&self.granules);
        // SEAL-FUSION (#20): keep the part-grain merge for aux_sections.
        self.part_hll = hll;
        rec
    }

    fn aux_sections(&mut self) -> Vec<(SectionKind, Vec<u8>)> {
        assert!(self.part_sealed, "aux_sections before seal_part");
        let n = self.granules.len();
        let bitmap_len = n.div_ceil(8);
        let mut out = Vec::new();
        // Psma (spec §8.2): [armed bitmap][blocks in granule order] and
        // Bloom (spec §8.3): BloomHdr + [armed bitmap][blocks] — built in
        // ONE walk over the granule list (SEAL-FUSION #19: the bitmaps are
        // fixed-offset prefixes, so bits and blocks land in the same pass;
        // section bytes unchanged).
        let mut psma_body = vec![0u8; bitmap_len];
        let mut psma_any = false;
        let mut bloom_body = Vec::new();
        put_u32(&mut bloom_body, BLOOM_K_DEFAULT);
        put_u32(&mut bloom_body, BLOOM_BYTES_PER_GRANULE_DEFAULT);
        let bloom_bitmap_at = bloom_body.len();
        bloom_body.resize(bloom_bitmap_at + bitmap_len, 0);
        let mut bloom_any = false;
        // Blocks trail the bitmaps; stage them per kind in the same walk.
        let mut psma_blocks: Vec<u8> = Vec::new();
        let mut bloom_blocks: Vec<u8> = Vec::new();
        for (i, g) in self.granules.iter().enumerate() {
            if let Some(block) = &g.psma {
                psma_any = true;
                psma_body[i / 8] |= 1 << (i % 8);
                put_bytes(&mut psma_blocks, block);
            }
            if let Some(block) = &g.bloom {
                bloom_any = true;
                bloom_body[bloom_bitmap_at + i / 8] |= 1 << (i % 8);
                put_bytes(&mut bloom_blocks, block);
            }
        }
        if psma_any {
            psma_body.extend_from_slice(&psma_blocks);
            out.push((SectionKind::Psma, psma_body));
        }
        if bloom_any {
            bloom_body.extend_from_slice(&bloom_blocks);
            out.push((SectionKind::Bloom, bloom_body));
        }
        // NdvRegisters (spec §8.4): the part-grain mergeable form — the
        // merge `seal_part` already did (SEAL-FUSION #20), never redone.
        if self.profile.ndv && !self.granules.is_empty() {
            let hll = self
                .part_hll
                .clone()
                .expect("seal_part cached the part-grain HLL (ndv profile)");
            let mut body = Vec::new();
            hll.encode_section(&mut body);
            out.push((SectionKind::NdvRegisters, body));
        }
        out
    }

    /// ST-1 (OD-2): the part-grain distribution sketch (armed iff
    /// `profile.ndv` — the same soundness gate as value hashing; a
    /// nondeterministic collation computes none and the fold declines
    /// honestly).
    fn distribution(&mut self) -> Option<crate::format::sidecar::ColDistribution> {
        assert!(self.part_sealed, "distribution before seal_part");
        // DICT-DEDUP: the feed serves through the SAME finish path the
        // accumulator would have used (sketch.rs — one implementation, no
        // drift); `set_distribution_feed` disarmed `dist`, so exactly one
        // of the two arms exists.
        if let Some(feed) = &self.dist_feed {
            debug_assert!(self.dist.is_none(), "feed and accumulator both live");
            return Some(crate::sketch::distribution_from_sorted_counts(feed));
        }
        self.dist.as_ref().map(|d| d.finalize())
    }

    /// pgrc2.1 §2.2 (trait doc carries the contract): the feed arm is the
    /// dict election's COMPLETE counted distinct set (long entries
    /// included) — its length is the exact NDV; the accumulator arm is
    /// exact only with zero long-value residue.
    fn distinct_exact(&self) -> Option<u64> {
        assert!(self.part_sealed, "distinct_exact before seal_part");
        if let Some(feed) = &self.dist_feed {
            return Some(feed.len() as u64);
        }
        self.dist.as_ref().and_then(|d| d.distinct_exact())
    }

    /// DICT-DEDUP (trait doc carries the contract): accept the dict
    /// election's counted distinct set and DISARM the accumulator — the
    /// distinct set is maintained once, in the dict build; this builder
    /// becomes its second consumer. Accepted only where the accumulator
    /// was armed (`profile.ndv` — value identity sound; the collation
    /// gate applies automatically): an ndv-unsound profile keeps
    /// declining `distribution()`, feed or no feed.
    fn set_distribution_feed(&mut self, entries: Vec<(Vec<u8>, u64)>) {
        // Driver protocol (RELEASED asserts — code bugs, not data): the
        // feed replaces accumulation, so it must precede ALL observation.
        assert!(
            self.cur.is_none() && self.granules.is_empty() && !self.part_sealed,
            "set_distribution_feed after observation (driver protocol)"
        );
        assert!(self.dist_feed.is_none(), "set_distribution_feed twice");
        if self.dist.is_some() {
            self.dist = None;
            self.dist_feed = Some(entries);
        }
    }

    /// D-STATS dict fold-from-codes: available iff the batched shell is on
    /// AND the DICT-DEDUP feed was accepted (the feed's byte-rank entry
    /// list IS the code→bytes table — index == global code — and its
    /// acceptance already disarmed the accumulator, so no per-row DistAcc
    /// feeding is owed), on the dict-electable profile shape (varlena,
    /// no sums/zeros, prefix-or-no keys). Everything else — the D2
    /// count-free inherit path, the `PGRUST_PGRC2_DICT_DIST_FEED=0`
    /// control arm, the `PGRUST_SEAL_BATCH_FOLD=0` control arm, foreign
    /// profiles — declines, and the driver keeps the classic hydrated
    /// value walk.
    fn dict_code_observe_supported(&self) -> bool {
        self.batch_fold
            && self.dist_feed.is_some()
            && matches!(
                self.profile.class,
                crate::format::class::StorageClass::VarlenaVerbatim
            )
            && self.profile.sum == SumKind::None
            && self.profile.zero == ZeroKind::None
            && matches!(
                self.profile.key,
                KeyDerivation::None | KeyDerivation::MemcmpVarPrefix
            )
    }

    fn observe_granule_dict_codes(&mut self, input: &EncodeInput<'_>, granule: u32) {
        assert!(
            self.dict_code_observe_supported(),
            "codes face on an unsupported builder (driver protocol)"
        );
        assert_eq!(
            input.class, self.profile.class,
            "EncodeInput class disagrees with the profile (driver protocol)"
        );
        if self.dict_tables.is_none() {
            let feed = self.dist_feed.as_ref().expect("supported implies feed");
            self.dict_tables = Some(Box::new(crate::batch::build_dict_tables(
                &self.profile,
                feed,
            )));
        }
        // The observe_granule protocol exactly (ordering asserts + both
        // driver protocols), with the codes fold as the body.
        self.begin_granule_rows(granule);
        let mut cur = self.cur.take().expect("begin_granule_rows set cur");
        crate::batch::observe_codes_granule(
            &self.profile,
            self.psma_populate,
            &mut cur,
            self.dict_tables.as_ref().expect("just built"),
            &mut self.scratch,
            self.stream_base,
            input,
        );
        self.cur = Some(cur);
        self.end_granule_rows(input.rows);
    }
}

impl ColumnMeta {
    /// Close one granule: compute its `StatsRecord` + aux blocks and append
    /// them in granule order. The ONE place a granule's statistics are
    /// finalized — reached from `observe_granule` (protocol 2) and from
    /// `seal_granule` (protocol 1), so the two cannot diverge.
    fn finalize(&mut self, cur: CurGranule) {
        assert_eq!(
            cur.granule,
            self.granules.len() as u32,
            "granule finalized out of order"
        );
        let profile = &self.profile;
        let mut rec = StatsRecord::absent();
        // The computed-stats witness (#598) — see `merge`; minted at BOTH
        // record-producing sites so every grain carries it.
        rec.flags |= STATSF_COMPUTED;
        rec.nonnull = cur.nonnull;
        // Keys + the degrade rule.
        let keys_ok = profile.key.kind() != KeyKind::Absent
            && !cur.poisoned
            && cur.have_key
            && cur.nonnull > 0;
        if keys_ok {
            rec.key_kind = profile.key.kind().as_u8();
            rec.min_key = cur.kmin;
            rec.max_key = cur.kmax;
        }
        // Sortedness (coarse: strict-only claims, never Constant).
        rec.sortedness = if !keys_ok {
            Sortedness::Unknown
        } else if profile.key.kind() == KeyKind::Exact {
            if cur.kmin == cur.kmax {
                Sortedness::Constant
            } else if cur.asc {
                Sortedness::Ascending
            } else if cur.desc {
                Sortedness::Descending
            } else {
                Sortedness::Unknown
            }
        } else if cur.ties {
            Sortedness::Unknown
        } else if cur.asc {
            Sortedness::Ascending
        } else if cur.desc {
            Sortedness::Descending
        } else {
            Sortedness::Unknown
        }
        .as_u8();
        // Sums / zeros (PackedI64 rides the key witness).
        let sums_valid = match profile.sum {
            SumKind::None => false,
            SumKind::PackedI64 => keys_ok,
            _ => true,
        };
        if sums_valid {
            rec.sum_i128 = cur.sum;
        }
        let zeros_valid = match profile.zero {
            ZeroKind::None => false,
            ZeroKind::PackedI64 => keys_ok,
            _ => true,
        };
        if zeros_valid {
            rec.zero_count = cur.zero;
        }
        // Length stats.
        if profile.len_stats != LenStats::None && cur.have_len {
            rec.byte_len_min = cur.blen_min;
            rec.byte_len_max = cur.blen_max;
            rec.byte_len_sum = cur.blen_sum;
            if profile.len_stats == LenStats::BytesAndChars {
                rec.char_len_min = cur.clen_min;
                rec.char_len_max = cur.clen_max;
            }
        }
        // P-2 all-ASCII witness (v4 delta, ledger FT-2): minted at the
        // granule grain iff no observed value falsified it; empty granules
        // are vacuously ASCII. Char-unit folds are licensed only under it.
        if profile.len_stats == LenStats::BytesAndChars && cur.ascii {
            rec.flags |= STATSF_ALL_ASCII;
        }
        // NDV estimate.
        if profile.ndv {
            rec.ndv_est = cur.hll.estimate();
        }
        // PSMA: armed iff keys exist and the granule spans more than one
        // key (a constant granule prunes by zone alone). The OD-10
        // populate-off arm skipped the key staging, so the block build is
        // skipped with it (the A/B's compute half).
        let psma = if self.psma_populate && keys_ok && cur.kmin < cur.kmax {
            let shift = psma_shift(cur.kmin, cur.kmax);
            let mut acc = PsmaAcc::default();
            for &(row, key) in &cur.keybuf {
                acc.observe(psma_index(cur.kmin, shift, key), row as u32);
            }
            let mut block = Vec::new();
            acc.encode_into(&mut block);
            Some(block)
        } else {
            None
        };
        // Bloom: the charter arming policy (unclustered ∧ NDV floor); the
        // eq-soundness leg is the profile's.
        let sortedness = Sortedness::from_u8(rec.sortedness).expect("just written");
        let bloom = match cur.bloom {
            Some(block) if bloom_armed(sortedness, rec.ndv_est) => Some(block),
            _ => None,
        };
        self.granules.push(SealedGranule {
            rec,
            first_key: cur.first_key,
            last_key: cur.last_key,
            hll: cur.hll,
            psma,
            bloom,
        });
    }
}
