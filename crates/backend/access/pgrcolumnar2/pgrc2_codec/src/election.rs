//! Election machinery + the stream encode driver (spec §19.6; charter §1
//! "analyze-then-elect" carried verbatim: exact chunk stats, deterministic
//! election, ≥10%-win gates, incompressible-guard framing, round-trip
//! verify at encode — the election quadruple's moving side).
//!
//! Laws:
//! - **Input-decidable**: every election here is a pure function of the
//!   stream's granule inputs (+ catalog facts the writer supplies). Same
//!   input ⇒ same election ⇒ same bytes (the byte-identical-parts law
//!   depends on it).
//! - **Refusal demotes, never normalizes** (charter §3): every refusal is a
//!   typed [`Demotion`] carrying its reason; the demotion target is always
//!   VERBATIM (byte-exact by construction).
//! - **≥10% gate** ([`wins_by_ten_percent`]): a fast layout must win by a
//!   tenth of the baseline's bytes or the baseline ships (the gate is LAW;
//!   S4 ledger). The **incompressible guard** is the same comparison
//!   against the raw image — a candidate that cannot beat raw is never
//!   stored.
//! - **Round-trip verify at encode** ([`encode_stream`] with
//!   `verify = true`): every elected granule decodes back and compares
//!   canonical bytes before the section is publishable
//!   (`abi::verify_roundtrip`, the quadruple's fixed leg); dict-code
//!   streams verify code equality through `decode_codes` (their datum
//!   currency IS the code).
//! - The FFOR tier is offered ONLY when the caller asks for the fused
//!   posture (S4: FFOR wins fused, loses flat — the election prices it,
//!   witnessed constants in the M3 §9 ledger).
//! - The dict NDV cap is a caller parameter, not a constant: the §9 ledger
//!   marks it RE-MEASURE under global codes before any number ships.

use crate::{alpc, arraydual, bytefor, deltafor, ffor, packednum, wrapper};
use pgrc2_format::abi::{
    verify_roundtrip, CodecRegistry, EncodeInput, GranuleEncoder, KernelCtx, KernelKey,
};
use pgrc2_format::class::StorageClass;
use pgrc2_format::enc::{EncodingId, Wrapper};
use pgrc2_format::geom::{FRAME_VALUES, GRANULE_ROWS};
use pgrc2_format::part::{
    OverflowSink, StreamCloseout, StreamSectionHdr, StreamSectionWriter, STREAMF_SIGNED,
};
use pgrc2_format::verbatim::encode_validity_bitmap;
use pgrc2_format::{FormatError, FormatResult};

/// The standing ≥10%-win discipline (LAW, not a number): engage only on
/// `candidate × 10 ≤ baseline × 9`.
pub fn wins_by_ten_percent(candidate_bytes: usize, baseline_bytes: usize) -> bool {
    (candidate_bytes as u128) * 10 <= (baseline_bytes as u128) * 9
}

/// The WRAPPER-layer gate (LAW, O-CMP-4(a), ruled 2026-08-10): the wrapper
/// uniquely adds decode CPU on every cold read, so it engages only on
/// `candidate × 5 ≤ baseline × 4` (a ≥20% win). Encodings keep
/// [`wins_by_ten_percent`] — the two gates are deliberately distinct laws.
pub fn wins_by_twenty_percent(candidate_bytes: usize, baseline_bytes: usize) -> bool {
    (candidate_bytes as u128) * 5 <= (baseline_bytes as u128) * 4
}

/// Why an election refused (typed; the refusal-to-elect path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Demotion {
    /// The candidate did not clear the ≥10% gate (incl. the incompressible
    /// guard — raw is the floor baseline).
    BelowWinGate,
    /// numeric: NaN/±Inf present (vendored `Special`).
    NumericSpecial,
    /// numeric: dscale varies across the chunk (uniform-dscale rule).
    NumericMixedDscale,
    /// numeric: mantissa budget / non-canonical image.
    NumericOverflow,
    /// arrays: shape outside the 1-D/lbound-1/no-null/byval predicate.
    ArrayShape,
    /// dict: NDV above the caller's cap.
    NdvAboveCap,
    /// [`elect_float`] called on a non-float storage class (caller
    /// contract). F32 no longer demotes here: SB-5 completed the family
    /// with the native f32 ALP arm (`alpc.rs` module doc).
    FloatClassUnsupported,
    /// The stream is all-null/empty: nothing to elect on (verbatim ships).
    NoValues,
}

/// One stream's elected outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Election {
    Elected {
        encoding: EncodingId,
        /// The §6.3 width byte (BYTE_FOR delta width, PACKED_NUMERIC
        /// mantissa width, DICT_CODES max code width; else 0).
        width: u8,
        /// PACKED_NUMERIC: elected scale; else 0.
        aux32: u32,
        /// Exact candidate payload bytes (witness fact).
        candidate_bytes: usize,
        /// Exact baseline (verbatim) payload bytes.
        baseline_bytes: usize,
    },
    Demoted {
        reason: Demotion,
        baseline_bytes: usize,
    },
}

// ---------------------------------------------------------------------------
// class elections
// ---------------------------------------------------------------------------

/// Verbatim payload bytes for word classes (the baseline arm).
fn word_baseline(granules: &[EncodeInput<'_>], stride: usize) -> usize {
    granules.iter().map(|g| g.rows as usize * stride).sum()
}

fn any_values(granules: &[EncodeInput<'_>]) -> bool {
    granules.iter().any(|g| (0..g.rows).any(|r| g.valid(r)))
}

/// Constancy scan for word classes (datums compare directly). Pointer
/// classes run their own scans (const election for them is the writer's
/// canonical-bytes pass; word classes are the hot case).
fn word_const(granules: &[EncodeInput<'_>]) -> Option<u64> {
    let mut seen: Option<u64> = None;
    for g in granules {
        for r in 0..g.rows {
            if !g.valid(r) {
                continue;
            }
            let d = g.datums[r as usize];
            match seen {
                None => seen = Some(d),
                Some(s) if s != d => return None,
                _ => {}
            }
        }
    }
    seen
}

/// Elect an int-family (byval word) stream. `fused` unlocks the FFOR tier;
/// DELTA_FOR competes as a cold size arm only when `cold` is set (the
/// writer's temperature call — S4 struck it from hot tiers).
///
/// Thin wrapper over [`elect_int_carry`] (the SEAL-FUSION fused analyzer);
/// kept for stats-only callers and the property suites — the election
/// outcome is identical by construction.
pub fn elect_int(
    granules: &[EncodeInput<'_>],
    byval_width: u8,
    signed: bool,
    fused: bool,
    cold: bool,
) -> Election {
    elect_int_carry(granules, byval_width, signed, fused, cold).0
}

/// Per-frame reduction facts computed ONCE by the fused int analyzer
/// ([`elect_int_carry`]) and carried into encode, so the encoders never
/// re-reduce the same in-cache datums (SEAL-FUSION: walks #3/#4/#5 vs #9).
///
/// Frame ordinal = row-dense part offset / [`FRAME_VALUES`]. This mapping is
/// exact at EVERY SB-10 ladder grain because every ladder grain is a
/// multiple of `FRAME_VALUES` (geom.rs compile-time law): the election's
/// default-grain slicing and the seal's elected-grain slicing put frame
/// boundaries at the same absolute rows, tail frame included.
#[derive(Debug, Clone, Copy)]
pub struct IntFrameFact {
    /// Frame min in the elected domain (the BYTE_FOR ref / FFOR base);
    /// 0 for an all-null frame — exactly `frame_min`/`frame_base_bits`.
    pub refw: u64,
    /// max − min in the elected domain (wrapping for signed); 0 all-null.
    pub range: u64,
    /// DELTA_FOR per-frame zigzag width (computed only under the cold
    /// posture; 1 otherwise — matching `deltafor::frame_width`'s floor).
    pub df_width: u8,
    /// DELTA_FOR frame first value (first non-null; 0 all-null).
    pub df_first: i64,
    /// Any valid value in the frame.
    pub any: bool,
}

/// The fused int analyzer's carried output: one fact per frame of the
/// row-dense part.
#[derive(Debug, Clone, Default)]
pub struct IntCarry {
    pub frames: Vec<IntFrameFact>,
}

/// The fused int election (SEAL-FUSION): ONE pass over the granule datums
/// computes what previously took 3–5 separate walks — the any-valid probe,
/// the constancy scan, BYTE_FOR's per-frame ranges, FFOR's per-frame
/// base+bits, and (cold) DELTA_FOR's per-frame zigzag widths. The election
/// OUTCOME is identical to the unfused arms by construction (pinned by
/// `tests/electiontests.rs::fused_analyzer_matches_reference`); the
/// per-frame facts are returned for the seal to carry into encode.
pub fn elect_int_carry(
    granules: &[EncodeInput<'_>],
    byval_width: u8,
    signed: bool,
    fused: bool,
    cold: bool,
) -> (Election, IntCarry) {
    let baseline = word_baseline(granules, byval_width as usize);
    let mut carry = IntCarry {
        frames: Vec::with_capacity(
            granules
                .iter()
                .map(|g| (g.rows as usize).div_ceil(FRAME_VALUES as usize))
                .sum(),
        ),
    };
    let mut any_part = false;
    // Constancy over raw datum words (word_const semantics: valid rows only).
    let mut seen: Option<u64> = None;
    let mut constant = true;
    let mut bf_width: u8 = 1;
    let mut ffor_bytes_acc = 0usize;
    let mut df_bytes_acc = 0usize;
    for g in granules {
        let rows = g.rows as usize;
        let mut f0 = 0usize;
        while f0 < rows {
            let n = (rows - f0).min(FRAME_VALUES as usize);
            let mut any = false;
            let (mut min_s, mut max_s) = (i64::MAX, i64::MIN);
            let (mut min_u, mut max_u) = (u64::MAX, u64::MIN);
            // DELTA_FOR chain state (cold only): prev = frame's first valid
            // value; the first valid row's delta is zigzag(0) — exactly
            // `deltafor::frame_width`.
            let mut df_first: i64 = 0;
            let mut df_prev: i64 = 0;
            let mut df_have_first = false;
            let mut df_w: u8 = 1;
            for r in f0..f0 + n {
                if !g.valid(r as u32) {
                    continue;
                }
                let d = g.datums[r];
                any = true;
                match seen {
                    None => seen = Some(d),
                    Some(s) if s != d => constant = false,
                    _ => {}
                }
                if signed {
                    min_s = min_s.min(d as i64);
                    max_s = max_s.max(d as i64);
                } else {
                    min_u = min_u.min(d);
                    max_u = max_u.max(d);
                }
                if cold {
                    let v = d as i64;
                    if !df_have_first {
                        df_first = v;
                        df_prev = v;
                        df_have_first = true;
                    }
                    let z = deltafor::zigzag(v.wrapping_sub(df_prev));
                    let need = deltafor::width_for(z);
                    if need > df_w {
                        df_w = need;
                    }
                    df_prev = v;
                }
            }
            any_part |= any;
            let (refw, range) = if any {
                if signed {
                    (
                        min_s as u64,
                        (max_s as u64).wrapping_sub(min_s as u64),
                    )
                } else {
                    (min_u, max_u - min_u)
                }
            } else {
                (0, 0)
            };
            if any {
                let w = bytefor::width_for_range(range);
                if w > bf_width {
                    bf_width = w;
                }
            }
            // FFOR arm accumulation (`ffor::granule_payload_bytes` shape):
            // all-null frames price bits 0 exactly as `frame_base_bits`.
            if fused {
                let bw = if any { ffor::bits_for(range) } else { 0 };
                ffor_bytes_acc += ffor::FRAME_HDR_LEN + alp::bitpack::packed_words(bw) * 8;
            }
            if cold {
                df_bytes_acc += deltafor::FRAME_HDR_LEN + n * df_w as usize;
            }
            carry.frames.push(IntFrameFact {
                refw,
                range,
                df_width: df_w,
                df_first,
                any,
            });
            f0 += n;
        }
    }
    if !any_part {
        return (
            Election::Demoted {
                reason: Demotion::NoValues,
                baseline_bytes: baseline,
            },
            carry,
        );
    }
    if constant {
        return (
            Election::Elected {
                encoding: EncodingId::Const,
                width: byval_width,
                aux32: 0,
                candidate_bytes: 16, // one extent record
                baseline_bytes: baseline,
            },
            carry,
        );
    }
    // BYTE_FOR arm (primary): stream width = max frame width.
    let bf_bytes: usize = granules
        .iter()
        .map(|g| bytefor::payload_bytes(g.rows, bf_width))
        .sum();
    let ffor_bytes: Option<usize> = fused.then_some(ffor_bytes_acc);
    let df_bytes: Option<usize> = cold.then_some(df_bytes_acc);
    // Deterministic ranking: smallest exact size wins; ties prefer
    // BYTE_FOR (primary), then FFOR, then DELTA_FOR.
    let mut best = (EncodingId::ByteFor, bf_width, bf_bytes);
    if let Some(fb) = ffor_bytes {
        if fb < best.2 {
            best = (EncodingId::FforInterleave, 0, fb);
        }
    }
    if let Some(db) = df_bytes {
        if db < best.2 {
            best = (EncodingId::DeltaFor, 0, db);
        }
    }
    let e = if wins_by_ten_percent(best.2, baseline) {
        Election::Elected {
            encoding: best.0,
            width: best.1,
            aux32: 0,
            candidate_bytes: best.2,
            baseline_bytes: baseline,
        }
    } else {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            baseline_bytes: baseline,
        }
    };
    (e, carry)
}

// ---------------------------------------------------------------------------
// SEAL-SPEED-2 D3: sample-based election (BtrBlocks §3.1-3.2 lineage)
// ---------------------------------------------------------------------------

/// Deterministic sample ordinals over `count` units: ~1% as evenly strided
/// whole units, at least [`SAMPLE_MIN_RUNS`], the first unit always
/// included. A PURE FUNCTION of `count` — deterministic strides, never RNG
/// (the dirsha law: the sample must be a pure function of the input).
/// Units are whole contiguous runs (frames / granules) because contiguous
/// runs are load-bearing for run-length- and range-shaped estimators
/// (BtrBlocks measured random single tuples as the worst strategy).
pub fn sample_ords(count: usize) -> Vec<usize> {
    const SAMPLE_MIN_RUNS: usize = 8;
    if count <= SAMPLE_MIN_RUNS {
        return (0..count).collect();
    }
    let runs = count.div_ceil(100).max(SAMPLE_MIN_RUNS);
    // Strictly increasing (count >= runs ⇒ consecutive ordinals differ by
    // >= floor(count/runs) >= 1): whole distinct units.
    (0..runs).map(|i| i * count / runs).collect()
}

/// Which election families run sampled (per-family adoption is the D3
/// charter: a family whose A/B regresses keeps full census). Families not
/// listed here (numeric: the shared-scale fit is a correctness proof over
/// EVERY value; bool: the census is one trivial byte-scan; dict: D2's
/// inherit owns the parquet path and the TSV build is needed when elected)
/// have no sampled arm by design.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SampleFamilies {
    pub int: bool,
    pub float: bool,
    pub fsst: bool,
}

impl SampleFamilies {
    pub const NONE: SampleFamilies = SampleFamilies { int: false, float: false, fsst: false };
    pub const ALL: SampleFamilies = SampleFamilies { int: true, float: true, fsst: true };

    /// Parse the `PGRUST_SEAL_SAMPLE_ELECT` vocabulary: `0`/unset = census
    /// (the exact/control arm), `1` = all sampled families, else a comma
    /// list of family names (`int,float,fsst`).
    pub fn parse(v: &str) -> SampleFamilies {
        let v = v.trim();
        match v {
            "" | "0" => SampleFamilies::NONE,
            "1" => SampleFamilies::ALL,
            _ => {
                let mut f = SampleFamilies::NONE;
                for tok in v.split(',') {
                    match tok.trim() {
                        "int" => f.int = true,
                        "float" => f.float = true,
                        "fsst" => f.fsst = true,
                        _ => {}
                    }
                }
                f
            }
        }
    }
}

/// The sampled int election (D3, v2 after the smoke regret attribution):
/// the full facts pass computes min/max + constancy per frame EXACTLY (the
/// carry the encode needs anyway), which prices BYTE_FOR and FFOR exactly
/// for free — closed forms over the per-frame ranges. The SAMPLE's only
/// job is the one genuinely row-grain extra arm: the cold posture's
/// DELTA_FOR zigzag chain. A ~1% whole-frame sample estimates DELTA_FOR;
/// only when that estimate lands within [`DF_VERIFY_MARGIN`] of the best
/// exact arm does the full chain run (and then the election is EXACTLY the
/// census's). Divergence class vs census: a skipped DELTA_FOR arm that
/// would actually have won — a missed-win regret, bounded by the margin;
/// every emitted election is exact-sized and exact-gated (width is never
/// an estimate — the correctness law).
pub fn elect_int_sampled_carry(
    granules: &[EncodeInput<'_>],
    byval_width: u8,
    signed: bool,
    fused: bool,
    cold: bool,
) -> (Election, IntCarry) {
    let baseline = word_baseline(granules, byval_width as usize);
    // Frame geometry: (granule ordinal, f0, n) per frame, row-dense order.
    let mut frames: Vec<(usize, usize, usize)> = Vec::new();
    for (g, gi) in granules.iter().enumerate() {
        let rows = gi.rows as usize;
        let mut f0 = 0usize;
        while f0 < rows {
            let n = (rows - f0).min(FRAME_VALUES as usize);
            frames.push((g, f0, n));
            f0 += n;
        }
    }
    let total_frames = frames.len();
    if total_frames == 0 {
        return (
            Election::Demoted { reason: Demotion::NoValues, baseline_bytes: baseline },
            IntCarry { frames: Vec::new() },
        );
    }

    // ---- full facts pass: min/max + constancy (EXACT; no DF chain) --------
    let mut carry = IntCarry { frames: Vec::with_capacity(total_frames) };
    let mut any_part = false;
    let mut seen: Option<u64> = None;
    let mut constant = true;
    let mut bf_width: u8 = 1;
    let mut ffor_bytes_acc = 0usize;
    for &(g, f0, n) in &frames {
        let gi = &granules[g];
        let mut any = false;
        let (mut min_s, mut max_s) = (i64::MAX, i64::MIN);
        let (mut min_u, mut max_u) = (u64::MAX, u64::MIN);
        for r in f0..f0 + n {
            if !gi.valid(r as u32) {
                continue;
            }
            let d = gi.datums[r];
            any = true;
            match seen {
                None => seen = Some(d),
                Some(s) if s != d => constant = false,
                _ => {}
            }
            if signed {
                min_s = min_s.min(d as i64);
                max_s = max_s.max(d as i64);
            } else {
                min_u = min_u.min(d);
                max_u = max_u.max(d);
            }
        }
        any_part |= any;
        let (refw, range) = if any {
            if signed {
                (min_s as u64, (max_s as u64).wrapping_sub(min_s as u64))
            } else {
                (min_u, max_u - min_u)
            }
        } else {
            (0, 0)
        };
        if any {
            let w = bytefor::width_for_range(range);
            if w > bf_width {
                bf_width = w;
            }
        }
        if fused {
            let bw = if any { ffor::bits_for(range) } else { 0 };
            ffor_bytes_acc += ffor::FRAME_HDR_LEN + alp::bitpack::packed_words(bw) * 8;
        }
        carry.frames.push(IntFrameFact { refw, range, df_width: 1, df_first: 0, any });
    }
    if !any_part {
        return (
            Election::Demoted { reason: Demotion::NoValues, baseline_bytes: baseline },
            carry,
        );
    }
    if constant {
        return (
            Election::Elected {
                encoding: EncodingId::Const,
                width: byval_width,
                aux32: 0,
                candidate_bytes: 16,
                baseline_bytes: baseline,
            },
            carry,
        );
    }
    let bf_bytes: usize = granules
        .iter()
        .map(|g| bytefor::payload_bytes(g.rows, bf_width))
        .sum();
    let mut best = (EncodingId::ByteFor, bf_width, bf_bytes);
    if fused && ffor_bytes_acc < best.2 {
        best = (EncodingId::FforInterleave, 0, ffor_bytes_acc);
    }

    // ---- DELTA_FOR: sampled pre-filter, exact verify inside the margin ----
    if cold {
        let ords = sample_ords(total_frames);
        let mut s_df_payload = 0u64;
        let mut s_rows = 0u64;
        for &fo in &ords {
            let (g, f0, n) = frames[fo];
            let gi = &granules[g];
            let mut df_prev: i64 = 0;
            let mut df_have = false;
            let mut df_w: u8 = 1;
            for r in f0..f0 + n {
                if !gi.valid(r as u32) {
                    continue;
                }
                let v = gi.datums[r] as i64;
                if !df_have {
                    df_prev = v;
                    df_have = true;
                }
                let z = deltafor::zigzag(v.wrapping_sub(df_prev));
                let need = deltafor::width_for(z);
                if need > df_w {
                    df_w = need;
                }
                df_prev = v;
            }
            s_df_payload += (n * df_w as usize) as u64;
            s_rows += n as u64;
        }
        let total_rows: u64 = granules.iter().map(|g| g.rows as u64).sum();
        let df_est = if s_rows == 0 {
            u64::MAX
        } else {
            (deltafor::FRAME_HDR_LEN * total_frames) as u64
                + ((s_df_payload as u128) * (total_rows as u128) / (s_rows as u128)) as u64
        };
        // Run the exact chain when the estimate is competitive:
        // df_est <= best * DF_VERIFY_MARGIN (integer law: 4*df_est <= 5*best).
        if (df_est as u128) * 4 <= (best.2 as u128) * 5 {
            let mut df_bytes_acc = 0usize;
            for (fi, &(g, f0, n)) in frames.iter().enumerate() {
                let gi = &granules[g];
                let mut df_first: i64 = 0;
                let mut df_prev: i64 = 0;
                let mut df_have_first = false;
                let mut df_w: u8 = 1;
                for r in f0..f0 + n {
                    if !gi.valid(r as u32) {
                        continue;
                    }
                    let v = gi.datums[r] as i64;
                    if !df_have_first {
                        df_first = v;
                        df_prev = v;
                        df_have_first = true;
                    }
                    let z = deltafor::zigzag(v.wrapping_sub(df_prev));
                    let need = deltafor::width_for(z);
                    if need > df_w {
                        df_w = need;
                    }
                    df_prev = v;
                }
                df_bytes_acc += deltafor::FRAME_HDR_LEN + n * df_w as usize;
                carry.frames[fi].df_width = df_w;
                carry.frames[fi].df_first = df_first;
            }
            if df_bytes_acc < best.2 {
                best = (EncodingId::DeltaFor, 0, df_bytes_acc);
            }
        }
    }

    let e = if wins_by_ten_percent(best.2, baseline) {
        Election::Elected {
            encoding: best.0,
            width: best.1,
            aux32: 0,
            candidate_bytes: best.2,
            baseline_bytes: baseline,
        }
    } else {
        Election::Demoted { reason: Demotion::BelowWinGate, baseline_bytes: baseline }
    };
    (e, carry)
}

/// The DELTA_FOR verify margin (D3): the exact chain runs whenever the
/// sampled estimate is within 25% of the best exact arm — 4·est ≤ 5·best.
/// Inside the margin the election is EXACTLY the census's; outside it,
/// DELTA_FOR is skipped (the walk saving) and a true-DF-win there is the
/// bounded missed-win regret class.
pub const DF_VERIFY_MARGIN: (u32, u32) = (5, 4);

/// Elect a float stream. F64 runs the vendored exact-size ALP election
/// (classic/RD/raw per granule inside the stream); F32 runs the SB-5
/// native f32 arm (`alp::granule32` — classic/raw per granule, same
/// bit-exactness laws, always stamped ALP since no RD arm exists at that
/// width). Non-float classes demote typed (caller contract).
pub fn elect_float(granules: &[EncodeInput<'_>], class: StorageClass) -> Election {
    elect_float_carry(granules, class).0
}

/// The ALP frames the float election ALREADY encoded, carried to the seal
/// so encode never re-runs the identical scheme search (SEAL-FUSION: walk
/// #6 vs #9f). One self-describing vendored frame per ELECTION granule.
///
/// Scope law: ALP is granule-framed and the vendored first-stage sampling
/// is scoped to the input slice, so carried frames are byte-valid ONLY when
/// the seal encodes at the same granule slicing the election judged — i.e.
/// the DEFAULT grain (the election always slices at default). The seal's
/// `make_at` enforces this; non-default grains re-encode exactly as before.
#[derive(Debug, Clone, Default)]
pub struct FloatCarry {
    /// One frame per election granule, granule order.
    pub frames: Vec<Vec<u8>>,
}

/// The float election with carried frames (SEAL-FUSION). The election
/// outcome is identical to [`elect_float`]'s analyze-only arm by the
/// vendored contract (`analyze` is "always exactly `encode`'s report");
/// the frames it had to build to price exactly are RETURNED instead of
/// discarded. `None` carry on demotion (frames dropped) and on the
/// CONST/NoValues short-circuits.
///
/// Memory price (documented, SEAL-FUSION): the carried frames are ≈ the
/// encoded stream payload (the elected candidate_bytes), held from election
/// to the end of the stream's seal — a transient second copy of bytes the
/// band sections hold anyway. A DEMOTED float stream materializes frames
/// (≈ raw column bytes, since near-raw frames lost) and drops them at
/// return — the CPU is the same walk `analyze` already paid; only the
/// transient allocation is new.
pub fn elect_float_carry(
    granules: &[EncodeInput<'_>],
    class: StorageClass,
) -> (Election, Option<FloatCarry>) {
    let stride = match class {
        StorageClass::F64 => 8,
        StorageClass::F32 => 4,
        _ => {
            return (
                Election::Demoted {
                    reason: Demotion::FloatClassUnsupported,
                    baseline_bytes: 0,
                },
                None,
            )
        }
    };
    let baseline = word_baseline(granules, stride);
    if !any_values(granules) {
        return (
            Election::Demoted {
                reason: Demotion::NoValues,
                baseline_bytes: baseline,
            },
            None,
        );
    }
    if word_const(granules).is_some() {
        return (
            Election::Elected {
                encoding: EncodingId::Const,
                width: 0,
                aux32: 0,
                candidate_bytes: 16,
                baseline_bytes: baseline,
            },
            None,
        );
    }
    let mut frame_bytes = 0usize;
    let mut rd_granules = 0usize;
    let mut ngranules = 0usize;
    let mut frames: Vec<Vec<u8>> = Vec::with_capacity(granules.len());
    for g in granules {
        if class == StorageClass::F32 {
            let mut enc = alpc::encode_granule_f32_carry(g);
            frame_bytes += enc.report.frame_bytes;
            ngranules += enc.report.ngranules;
            debug_assert_eq!(enc.frames.len(), 1, "one vendored frame per granule");
            frames.push(std::mem::take(&mut enc.frames[0]));
        } else {
            let mut enc = alpc::encode_granule_carry(g);
            frame_bytes += enc.report.frame_bytes;
            rd_granules += enc.report.granules_using(alp::Scheme::AlpRd);
            ngranules += enc.report.ngranules;
            debug_assert_eq!(enc.frames.len(), 1, "one vendored frame per granule");
            frames.push(std::mem::take(&mut enc.frames[0]));
        }
    }
    if wins_by_ten_percent(frame_bytes, baseline) {
        // Stamp ALP_RD when RD dominates (stats-visible; decode-identical).
        // F32 never counts RD granules, so it always stamps ALP.
        let encoding = if rd_granules * 2 > ngranules {
            EncodingId::AlpRd
        } else {
            EncodingId::Alp
        };
        (
            Election::Elected {
                encoding,
                width: 0,
                aux32: 0,
                candidate_bytes: frame_bytes,
                baseline_bytes: baseline,
            },
            Some(FloatCarry { frames }),
        )
    } else {
        (
            Election::Demoted {
                reason: Demotion::BelowWinGate,
                baseline_bytes: baseline,
            },
            None,
        )
    }
}

/// Elect a bool stream: CONST or the 1-bit bitmap (which clears the gate
/// against the 1-byte verbatim baseline for any non-trivial granule).
pub fn elect_bool(granules: &[EncodeInput<'_>]) -> Election {
    let baseline = word_baseline(granules, 1);
    if !any_values(granules) {
        return Election::Demoted {
            reason: Demotion::NoValues,
            baseline_bytes: baseline,
        };
    }
    if word_const(granules).is_some() {
        return Election::Elected {
            encoding: EncodingId::Const,
            width: 1,
            aux32: 0,
            candidate_bytes: 16,
            baseline_bytes: baseline,
        };
    }
    let bytes: usize = granules.iter().map(|g| (g.rows as usize).div_ceil(8)).sum();
    if wins_by_ten_percent(bytes, baseline) {
        Election::Elected {
            encoding: EncodingId::BoolBitmap,
            width: 1,
            aux32: 0,
            candidate_bytes: bytes,
            baseline_bytes: baseline,
        }
    } else {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            baseline_bytes: baseline,
        }
    }
}

/// Elect PACKED_NUMERIC for a numeric varlena stream: shared-scale fit over
/// EVERY value (vendored uniform-dscale rule — never rounds), mantissa
/// width from the BYTE_FOR arm. `baseline_bytes` is the verbatim varlena
/// payload the writer computed (slot tables + entries).
pub fn elect_numeric(
    granules: &[EncodeInput<'_>],
    baseline_bytes: usize,
) -> FormatResult<Election> {
    let mut scale: Option<i32> = None;
    for g in granules {
        if let Some(s) = packednum::elect_scale(g)? {
            scale = Some(s);
            break;
        } else if any_values(core::slice::from_ref(g)) {
            // First valid value was special.
            return Ok(Election::Demoted {
                reason: Demotion::NumericSpecial,
                baseline_bytes,
            });
        }
    }
    let Some(scale) = scale else {
        return Ok(Election::Demoted {
            reason: Demotion::NoValues,
            baseline_bytes,
        });
    };
    // Fit every value; compute the mantissa width arm as we go.
    let mut width: u8 = 1;
    let mut mant_bytes = 0usize;
    let mut mantissas: Vec<u64> = Vec::with_capacity(GRANULE_ROWS as usize);
    for g in granules {
        mantissas.clear();
        for r in 0..g.rows {
            let m = if g.valid(r) {
                match packednum::mantissa_at(g.datums[r as usize], scale)? {
                    Some(m) => m,
                    None => {
                        // Distinguish the refusal class for the witness.
                        // SAFETY: EncodeInput pointer-class contract.
                        let payload =
                            unsafe { crate::section::varlena_payload(g.datums[r as usize])? };
                        let num = adt_numeric::Num::from_payload(payload);
                        let reason = if num.is_special() {
                            Demotion::NumericSpecial
                        } else if num.dscale() != scale {
                            Demotion::NumericMixedDscale
                        } else {
                            Demotion::NumericOverflow
                        };
                        return Ok(Election::Demoted {
                            reason,
                            baseline_bytes,
                        });
                    }
                }
            } else {
                0
            };
            mantissas.push(m as u64);
        }
        let gin = EncodeInput {
            class: StorageClass::ByvalWord {
                width: 8,
                signed: true,
            },
            rows: g.rows,
            datums: &mantissas,
            validity: g.validity,
        };
        width = width.max(bytefor::granule_min_width(&gin, true));
    }
    for g in granules {
        mant_bytes += 8 + bytefor::payload_bytes(g.rows, width); // + scale hdr
    }
    if wins_by_ten_percent(mant_bytes, baseline_bytes) {
        Ok(Election::Elected {
            encoding: EncodingId::PackedNumeric,
            width,
            aux32: scale as u32,
            candidate_bytes: mant_bytes,
            baseline_bytes,
        })
    } else {
        Ok(Election::Demoted {
            reason: Demotion::BelowWinGate,
            baseline_bytes,
        })
    }
}

/// The sampled float election (D3): a ~1% whole-GRANULE sample (ALP frames
/// are granule-scoped, so granules are the contiguous-run unit) is encoded
/// through the vendored kernels and its byte ratio gates ENTRY into the
/// full pricing — a stream whose sample cannot clear the ≥10% law demotes
/// without encoding the other ~99% (the census encodes EVERYTHING to price,
/// so demoted float columns are where the walk savings live). Survivors run
/// the census arm in full (sampled granules' frames reused, the rest
/// encoded) and the FINAL gate is exact — a sampled float election never
/// elects anything the census would not; its only divergence is a missed
/// election near the gate (regret, bounded by the D3 gate).
pub fn elect_float_sampled_carry(
    granules: &[EncodeInput<'_>],
    class: StorageClass,
) -> (Election, Option<FloatCarry>) {
    let stride = match class {
        StorageClass::F64 => 8,
        StorageClass::F32 => 4,
        _ => {
            return (
                Election::Demoted {
                    reason: Demotion::FloatClassUnsupported,
                    baseline_bytes: 0,
                },
                None,
            )
        }
    };
    let baseline = word_baseline(granules, stride);
    if !any_values(granules) {
        return (
            Election::Demoted { reason: Demotion::NoValues, baseline_bytes: baseline },
            None,
        );
    }
    if word_const(granules).is_some() {
        return (
            Election::Elected {
                encoding: EncodingId::Const,
                width: 0,
                aux32: 0,
                candidate_bytes: 16,
                baseline_bytes: baseline,
            },
            None,
        );
    }
    // ---- sample gate: encode ~1% of granules, ratio-scale, pre-gate -------
    let ords = sample_ords(granules.len());
    let mut s_frames: Vec<Option<Vec<u8>>> = vec![None; granules.len()];
    let mut s_bytes = 0usize;
    let mut s_rows = 0u64;
    for &g in &ords {
        let gi = &granules[g];
        if class == StorageClass::F32 {
            let mut enc = alpc::encode_granule_f32_carry(gi);
            s_bytes += enc.report.frame_bytes;
            debug_assert_eq!(enc.frames.len(), 1, "one vendored frame per granule");
            s_frames[g] = Some(std::mem::take(&mut enc.frames[0]));
        } else {
            let mut enc = alpc::encode_granule_carry(gi);
            s_bytes += enc.report.frame_bytes;
            debug_assert_eq!(enc.frames.len(), 1, "one vendored frame per granule");
            s_frames[g] = Some(std::mem::take(&mut enc.frames[0]));
        }
        s_rows += gi.rows as u64;
    }
    let total_rows: u64 = granules.iter().map(|g| g.rows as u64).sum();
    let est = if s_rows == 0 {
        usize::MAX
    } else {
        ((s_bytes as u128) * (total_rows as u128) / (s_rows as u128)) as usize
    };
    if !wins_by_ten_percent(est, baseline) {
        return (
            Election::Demoted { reason: Demotion::BelowWinGate, baseline_bytes: baseline },
            None,
        );
    }
    // ---- survivor: the census arm in full — exact bytes, exact final gate.
    // (The sampled granules re-encode here: reusing their frames would need
    // the per-granule scheme census the report carries, and threading it
    // was judged not worth the ~1% double work on survivors.)
    drop(s_frames);
    let mut frame_bytes = 0usize;
    let mut rd_granules = 0usize;
    let mut ngranules = 0usize;
    let mut frames: Vec<Vec<u8>> = Vec::with_capacity(granules.len());
    for gi in granules.iter() {
        if class == StorageClass::F32 {
            let mut enc = alpc::encode_granule_f32_carry(gi);
            frame_bytes += enc.report.frame_bytes;
            ngranules += enc.report.ngranules;
            debug_assert_eq!(enc.frames.len(), 1, "one vendored frame per granule");
            frames.push(std::mem::take(&mut enc.frames[0]));
        } else {
            let mut enc = alpc::encode_granule_carry(gi);
            frame_bytes += enc.report.frame_bytes;
            rd_granules += enc.report.granules_using(alp::Scheme::AlpRd);
            ngranules += enc.report.ngranules;
            debug_assert_eq!(enc.frames.len(), 1, "one vendored frame per granule");
            frames.push(std::mem::take(&mut enc.frames[0]));
        }
    }
    if wins_by_ten_percent(frame_bytes, baseline) {
        let encoding = if rd_granules * 2 > ngranules {
            EncodingId::AlpRd
        } else {
            EncodingId::Alp
        };
        (
            Election::Elected {
                encoding,
                width: 0,
                aux32: 0,
                candidate_bytes: frame_bytes,
                baseline_bytes: baseline,
            },
            Some(FloatCarry { frames }),
        )
    } else {
        (
            Election::Demoted { reason: Demotion::BelowWinGate, baseline_bytes: baseline },
            None,
        )
    }
}

/// Dict-election inputs the writer assembles from its byte-rank-sorted
/// global dictionary build (M3-D owns the build; this function owns the
/// DECISION).
#[derive(Debug, Clone, Copy)]
pub struct DictArm {
    pub rows: u64,
    pub ndv: u64,
    /// Caller-measured cap (§9 ledger: RE-MEASURE before a number ships).
    pub ndv_cap: u64,
    /// Exact dict bytes: index entries + payload entries.
    pub dict_bytes: usize,
    /// Exact code-stream bytes at the per-granule base+width packing.
    pub code_stream_bytes: usize,
    /// Verbatim varlena baseline bytes.
    pub baseline_bytes: usize,
}

/// The text/bytea dict decision: DICT_CODES iff NDV is under the cap AND
/// dict + codes clear the gate; otherwise the stream falls to the loser
/// path — where the writer may offer the SB-4 FSST arm
/// ([`elect_text_fsst`]) before the RawText/verbatim default (possibly
/// wrapper-elected via [`elect_stream_wrapper`]).
/// pgrc2.1 FLIP 1.2 measurement dial (`PGRUST_DICT_GATE=off|0`): disable
/// the ≥10%-savings gate for TEXT DICT ONLY, so every structurally
/// dict-able text part (cap-admitted, no oversize values, non-empty) seals
/// dict-encoded regardless of byte inflation. NEVER a production posture —
/// this is the A/B arm that prices what kernel-uniformity (no plain-path
/// fallbacks anywhere) costs in bank bytes on the NDV≈rows residue parts.
/// Default (unset) keeps the standing gate LAW exactly.
fn dict_gate_disabled() -> bool {
    matches!(
        std::env::var("PGRUST_DICT_GATE").as_deref(),
        Ok("0") | Ok("off")
    )
}

pub fn elect_text_dict(arm: DictArm) -> Election {
    if arm.ndv > arm.ndv_cap {
        return Election::Demoted {
            reason: Demotion::NdvAboveCap,
            baseline_bytes: arm.baseline_bytes,
        };
    }
    let candidate = arm.dict_bytes + arm.code_stream_bytes;
    if wins_by_ten_percent(candidate, arm.baseline_bytes) || dict_gate_disabled() {
        Election::Elected {
            encoding: EncodingId::DictCodes,
            width: 0,
            aux32: 0,
            candidate_bytes: candidate,
            baseline_bytes: arm.baseline_bytes,
        }
    } else {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            baseline_bytes: arm.baseline_bytes,
        }
    }
}

/// FSST-election inputs the writer assembles from its per-(column,part)
/// symbol-table build + exact trial pricing (`fsst::FsstSymbolTable`
/// owns the build/pricing; this function owns the DECISION — the
/// [`DictArm`]/[`elect_text_dict`] split, mirrored).
#[derive(Debug, Clone, Copy)]
pub struct FsstArm {
    pub rows: u64,
    /// Exact serialized symbol-table bytes across every extent copy
    /// (OD-5: the table rides each extent's section header bytes).
    pub table_bytes: usize,
    /// Bare compressed code bytes over EVERY non-null value (the
    /// trial-encode total; no framing).
    pub code_bytes: usize,
    /// Exact fsst frame/section framing: per-value slot entries +
    /// per-frame slot-end/frame-table entries + section headers.
    pub framing_bytes: usize,
    /// Raw payload bytes of the same non-null values (the code-grain
    /// incompressible-guard operand).
    pub value_bytes: usize,
    /// Verbatim varlena baseline bytes.
    pub baseline_bytes: usize,
}

/// The SB-4 FSST decision for a dict-loser text stream — TWO gates, both
/// exact-bytes and input-decidable:
///
/// 1. **Code-grain incompressible guard**: `table + code_bytes` must beat
///    the raw `value_bytes` by the standing ≥10% law. FSST frames carry
///    no per-value varlena headers, so a total-bytes comparison against
///    the verbatim baseline hands FSST a ~4-bytes/value representation
///    win on ANY input — uniform-random tokens priced ~12% "smaller" that
///    way. The win must come from COMPRESSION, never from framing
///    asymmetry: incompressible data demotes here.
/// 2. The standing ≥10% total-bytes gate against the verbatim baseline
///    (framing included) — the same law every encoding arm answers to.
pub fn elect_text_fsst(arm: FsstArm) -> Election {
    if !wins_by_ten_percent(arm.table_bytes + arm.code_bytes, arm.value_bytes) {
        return Election::Demoted {
            reason: Demotion::BelowWinGate,
            baseline_bytes: arm.baseline_bytes,
        };
    }
    let candidate = arm.table_bytes + arm.code_bytes + arm.framing_bytes;
    if wins_by_ten_percent(candidate, arm.baseline_bytes) {
        Election::Elected {
            encoding: EncodingId::Fsst,
            width: 0,
            aux32: 0,
            candidate_bytes: candidate,
            baseline_bytes: arm.baseline_bytes,
        }
    } else {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            baseline_bytes: arm.baseline_bytes,
        }
    }
}

/// Wrapper election over a finished unwrapped section (O-CMP-3(a): offered
/// on every election, disk-only semantics): price BOTH implemented arms
/// exactly, keep the smaller image iff it clears the wrapper-layer ≥20% law
/// ([`wins_by_twenty_percent`], O-CMP-4(a)) against the unwrapped section
/// bytes. Deterministic ranking: smallest exact size wins; ties prefer LZ4
/// (the lower wrapper id — arm order below is part of the contract). An arm
/// this build cannot encode ([`wrapper::wrapper_available`]) is never
/// priced — a build without the codec never elects it.
pub fn elect_stream_wrapper(
    section: &[u8],
    granule_payload_ends: &[u32],
) -> FormatResult<Option<Wrapper>> {
    let mut best: Option<(Wrapper, usize)> = None;
    for arm in [Wrapper::Lz4, Wrapper::Zstd] {
        if !wrapper::wrapper_available(arm) {
            continue;
        }
        let wrapped = wrapper::wrapped_len(section, granule_payload_ends, arm)?;
        if !wins_by_twenty_percent(wrapped, section.len()) {
            continue;
        }
        let better = match best {
            None => true,
            Some((_, len)) => wrapped < len,
        };
        if better {
            best = Some((arm, wrapped));
        }
    }
    Ok(best.map(|(arm, _)| arm))
}

/// Array election: per-granule split under the catalog facts; ANY granule
/// refusing demotes the stream (per-part permanence — mixed layouts within
/// one stream do not exist).
pub fn elect_array(
    granules: &[EncodeInput<'_>],
    facts: arraydual::ArrayElemFacts,
    baseline_bytes: usize,
) -> FormatResult<Result<Vec<arraydual::ArraySplit>, Demotion>> {
    let mut splits = Vec::with_capacity(granules.len());
    for g in granules {
        match arraydual::elect_array_split(g, facts)? {
            Some(s) => splits.push(s),
            None => return Ok(Err(Demotion::ArrayShape)),
        }
    }
    let _ = baseline_bytes;
    Ok(Ok(splits))
}

// ---------------------------------------------------------------------------
// the stream encode driver (encode → finish → verify)
// ---------------------------------------------------------------------------

/// A built stream section + the facts the seal driver stamps into the
/// stream entry / extent record.
pub struct StreamBuild {
    pub closeout: StreamCloseout,
    pub key: KernelKey,
    /// Per-granule payload end offsets (payload-relative) — the wrapper
    /// assembly input.
    pub granule_payload_ends: Vec<u32>,
    /// Byte range of the section within the caller's buffer.
    pub section_range: core::ops::Range<usize>,
    /// Byte range of this stream's overflow entries within the caller's
    /// overflow buffer (empty when none).
    pub overflow_range: core::ops::Range<usize>,
}

/// Encode a whole stream extent through one [`GranuleEncoder`], then (when
/// `verify` — production seals ALWAYS verify; the flag exists so corrupt-
/// suite fixtures can build intentionally broken sections) round-trip every
/// granule through the registry's decode face before reporting success.
///
/// `granules[g]` is granule `g`'s input; `child` streams get a gcount
/// table. Verification decodes from the FINISHED section — the same bytes
/// a reader will see.
#[allow(clippy::too_many_arguments)]
pub fn encode_stream(
    buf: &mut Vec<u8>,
    ovf_buf: &mut Vec<u8>,
    enc: &mut dyn GranuleEncoder,
    granules: &[EncodeInput<'_>],
    child: bool,
    registry: &CodecRegistry,
    verify: bool,
    fixed_len: u32,
    signed: bool,
) -> FormatResult<StreamBuild> {
    let key = enc.key();
    let section_start = buf.len();
    let ovf_start = ovf_buf.len();
    let mut w = StreamSectionWriter::begin(buf, key.encoding, key.width, Wrapper::None)?;
    let mut ovf = OverflowSink::new(ovf_buf);
    let mut ends = Vec::with_capacity(granules.len());
    for g in granules {
        enc.encode_granule(g, &mut w, &mut ovf)?;
        ends.push(w.payload_off());
    }
    enc.finish_stream(&mut w)?;
    if let Some(last) = ends.last_mut() {
        // CONST closes out in finish_stream; fold trailing bytes into the
        // final granule's block for wrapper purposes.
        *last = w.payload_off();
    }
    let closeout = w.finish(child)?;
    let section_range = section_start..buf.len();
    let overflow_range = ovf_start..ovf_buf.len();

    if verify {
        let section = &buf[section_range.clone()];
        let hdr = StreamSectionHdr::decode(section)?;
        let frame_table = hdr.frame_table(section)?;
        let vt = registry.resolve(key)?;
        let is_dict = key.encoding == EncodingId::DictCodes.as_u16();
        let mut datum_scratch = vec![0u64; GRANULE_ROWS as usize];
        // Arena scratch sized for the worst granule (verbatim images +
        // per-value alignment). Dict streams skip the sizing pass entirely:
        // their datum currency is the CODE, never a pointer — dereferencing
        // it would be exactly the class/currency confusion this branch
        // exists to avoid.
        let arena_len = if is_dict {
            64
        } else {
            worst_granule_arena(granules, fixed_len)
        };
        let mut arena_scratch = vec![0u8; arena_len];
        let mut vbits: Vec<u8> = Vec::new();
        for (g, input) in granules.iter().enumerate() {
            vbits.clear();
            let validity_bytes = match input.validity {
                None => None,
                Some(_) => {
                    encode_validity_bitmap(input.validity, input.rows, &mut vbits);
                    Some(vbits.as_slice())
                }
            };
            let ctx = KernelCtx {
                key,
                flags: if signed { STREAMF_SIGNED } else { 0 },
                fixed_len,
                bytes: section,
                frame_table: frame_table.as_deref(),
                granule: g as u32,
                granule_in_extent: g as u32,
                rows: input.rows,
                values: input.rows,
                validity_bytes,
                overflow: if overflow_range.is_empty() {
                    None
                } else {
                    Some(&ovf_buf[overflow_range.clone()])
                },
                dict: None,
            };
            if is_dict {
                verify_codes(vt.decode_codes, &ctx, input)?;
            } else {
                verify_roundtrip(vt, &ctx, input, &mut datum_scratch, &mut arena_scratch)?;
            }
        }
    }
    Ok(StreamBuild {
        closeout,
        key,
        granule_payload_ends: ends,
        section_range,
        overflow_range,
    })
}

/// Dict-code streams verify code equality (their datum currency IS the
/// global code; value materialization is the dictionary's own round-trip,
/// proven by the writer's dict build suite).
fn verify_codes(
    face: pgrc2_format::abi::DecodeCodesFn,
    ctx: &KernelCtx<'_>,
    input: &EncodeInput<'_>,
) -> FormatResult<()> {
    let mut codes = vec![0u32; input.rows as usize];
    let n = face(ctx, &mut codes)?;
    if n != input.rows {
        return Err(FormatError::EncodeContract {
            detail: "round-trip code count",
        });
    }
    for r in 0..input.rows {
        if !input.valid(r) {
            continue;
        }
        if codes[r as usize] as u64 != input.datums[r as usize] {
            return Err(FormatError::EncodeContract {
                detail: "round-trip code mismatch",
            });
        }
    }
    Ok(())
}

fn worst_granule_arena(granules: &[EncodeInput<'_>], fixed_len: u32) -> usize {
    let mut worst = 64usize;
    for g in granules {
        // Null slots still cost arena on pointer classes (decoders emit the
        // canonical zero-length placeholder entries): one aligned slot each.
        let mut need = g.rows as usize * 16;
        for r in 0..g.rows {
            if !g.valid(r) {
                continue;
            }
            need += match g.class {
                StorageClass::Fixed { len } => (len as usize).div_ceil(8) * 8,
                StorageClass::VarlenaVerbatim => {
                    // SAFETY: EncodeInput pointer-class contract; sizing
                    // pass only.
                    let payload = unsafe {
                        crate::section::varlena_payload(g.datums[r as usize])
                            .map(|p| p.len())
                            .unwrap_or(0)
                    };
                    (payload + 4).div_ceil(8) * 8 + 8
                }
                _ => 0,
            };
        }
        worst = worst.max(need + fixed_len as usize + 64);
    }
    worst
}
