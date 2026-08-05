//! tsrank_diff: differential fuzz driver — shipped Rust `adt_tsrank` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_tsrank_io.c + verbatim csrc/tsvec/tsrank.c). Crate under test:
//! crates/backend/utils/adt/tsrank.
//!
//! Comparison planes: f32 result compared BIT-EXACTLY (both sides do the
//! identical mixed float/double expression trees; `exp`/`ln` go to the same
//! host libm — any ULP difference IS a divergence and gets recorded first,
//! never pre-carved as "platform"), error verdict, errcode/sqlstate CLASS,
//! and the fc-wrapper plane (adt_tsrank::builtins::fc_* with a real float4[]
//! varlena arg) against the same C wrapper output. The rank core is also
//! called directly for the non-weights variants (core == wrapper pin).
//!
//! Inputs: tsvector text parsed by the SHIPPED Rust parser only (parser
//! equivalence is tsvector_core_diff's job); the resulting IMAGE is handed
//! to both sides byte-identically. tsquery images come from tsq_gen (valcrc
//! = 0; unused by the rank kernels). The float4[] weights argument is a
//! REAL 1-D array varlena image built here and handed to both sides
//! byte-identically (C getWeights vs Rust arg_weights read the same bytes);
//! array-shape error arms (ndim != 1, too-short, null bitmap) are generated
//! deliberately.
//!
//! Input layout: [sel][wmode][wbytes 16][shape][method 4][u16 vlen][vtext][qbytes]
//!   sel % 8    variant: 0..3 ts_rank_{wttf,wtt,ttf,tt}, 4..7 ts_rankcd_{..}
//!   wmode      weight regimes: 0 = raw f32 bits, 1 = quantized table
//!              (negatives -> defaults, >1.0 error, 0.0 -> rank_cd 1/0 = inf,
//!              NaN -> default, subnormals), 2 = exact defaults, else mixed
//!   shape      array-shape arm (w* variants): bits 0..1: 0 = well-formed
//!              [4], 1 = ndim=2 error, 2 = nitems=3 too-short error,
//!              3 = null bitmap error; bit 2: extra 5th element (legal)
//!   method     i32; & 0x80 on byte 0 -> masked to 0..0x3f flag space
//!
//! SKIPPED rows: none — all 8 ledger functions (oids 3703-3710) are arms.

use datum::{Datum, NullableDatum};
use mcx::MemoryContext;
use types_error::{
    PgError, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR,
};
use types_fmgr::{LocalFcinfo, PGFunction};

use adt_tsrank::builtins as fcb;
use adt_tsrank::rank::{calc_rank, DEFAULT_WEIGHTS, DEF_NORM_METHOD};
use adt_tsrank::rank_cd::calc_rank_cd;
use adt_tsvector_core::io::tsvector_in_core;
use adt_tsvector_core::layout::TsVec;
use adt_tsvector_core::query::TsQueryRef;

use crate::tsq_gen::gen_tsquery_payload;

extern "C" {
    fn pg_diff_ts_rank(
        variant: i32,
        wpayload: *const u8,
        wplen: i32,
        vimg: *const u8,
        vlen: i32,
        qimg: *const u8,
        qlen: i32,
        method: i32,
        res_bits: *mut u32,
    ) -> i32;
    fn pg_diff_errcode_get() -> i32;
}

const MAX_TEXT: usize = 2048;
const FLOAT4OID: u32 = 700;

/// C-side errcode class constants (csrc/tsvec/postgres.h).
fn err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_SYNTAX_ERROR {
        1
    } else if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        2
    } else if e.sqlstate == ERRCODE_NULL_VALUE_NOT_ALLOWED {
        3
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        5
    } else if e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR {
        8
    } else {
        99
    }
}

/// fc-wrapper invocation (tsvector_core_diff.rs pattern).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [NullableDatum; N],
) -> types_error::PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args = args;
    f(None, &mut fcinfo)
}

/// Inline varlena image (4B uncompressed header + payload) for fc args.
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 4) as u32;
    #[cfg(target_endian = "little")]
    let word = len << 2;
    #[cfg(target_endian = "big")]
    let word = len & 0x3FFF_FFFF;
    let mut img = Vec::with_capacity(payload.len() + 4);
    img.extend_from_slice(&word.to_ne_bytes());
    img.extend_from_slice(payload);
    img
}

fn varlena_datum(img: &[u8]) -> NullableDatum {
    NullableDatum::value(Datum::from_usize(img.as_ptr() as usize))
}

/// Weight-regime table for wmode 1: the shapes getWeights branches on.
const WTABLE: &[f32] = &[
    -1.0,
    -0.0, // negative zero: (v >= 0) is TRUE for -0.0 -> kept, not default
    0.0,  // rank_cd invws 1/0 = inf
    0.1,
    0.2,
    0.4,
    0.5,
    1.0,
    1.0000001, // > 1.0 -> "weight out of range"
    2.0,
    f32::NAN,      // NaN >= 0 false -> default
    f32::INFINITY, // > 1.0 -> error
    f32::NEG_INFINITY,
    1e-40, // subnormal
    0.999_999_9,
    -0.5,
];

fn gen_weights(wmode: u8, wbytes: &[u8; 16]) -> [f32; 4] {
    let mut ws = [0f32; 4];
    for (i, w) in ws.iter_mut().enumerate() {
        let raw: [u8; 4] = wbytes[i * 4..i * 4 + 4].try_into().unwrap();
        *w = match wmode % 4 {
            0 => f32::from_ne_bytes(raw),
            1 => WTABLE[raw[0] as usize % WTABLE.len()],
            2 => DEFAULT_WEIGHTS[i],
            _ => {
                if raw[0] & 1 == 0 {
                    WTABLE[raw[1] as usize % WTABLE.len()]
                } else {
                    f32::from_ne_bytes(raw)
                }
            }
        };
    }
    ws
}

/// Build a float4[] varlena PAYLOAD (bytes after vl_len_) per the `shape`
/// arm (module header): 0 = well-formed, 1 = ndim=2, 2 = too-short,
/// 3 = null bitmap; +4 = five elements.
fn build_weights_payload(shape: u8, ws: &[f32; 4], extra: f32) -> Vec<u8> {
    let mut p = Vec::with_capacity(64);
    let arm = shape & 3;
    let n_extra = shape & 4 != 0;
    let nitems: i32 = match arm {
        2 => 3,
        _ => {
            if n_extra {
                5
            } else {
                4
            }
        }
    };
    let ndim: i32 = if arm == 1 { 2 } else { 1 };
    // arm 3 = a null bit CLEARED (error on both sides); shape bit 3 = null
    // bitmap PRESENT with all bits set (no nulls) — legal wire shape that
    // exercises the dataoffset!=0 read path in getWeights (builtins.rs:43).
    let hasnull = arm == 3 || shape & 8 != 0;

    // header after vl_len_: ndim, dataoffset, elemtype
    p.extend_from_slice(&ndim.to_ne_bytes());
    let dataoffset: i32 = if hasnull {
        // ARR_OVERHEAD_WITHNULLS(1, nitems) = MAXALIGN(24 + (n+7)/8)
        (24 + (nitems + 7) / 8 + 7) & !7
    } else {
        0
    };
    p.extend_from_slice(&dataoffset.to_ne_bytes());
    p.extend_from_slice(&FLOAT4OID.to_ne_bytes());
    // dims + lbounds (ndim of each)
    if ndim == 2 {
        // 2 x 2 grid so nitems stays 4; still an ndim error on both sides
        p.extend_from_slice(&2i32.to_ne_bytes());
        p.extend_from_slice(&2i32.to_ne_bytes());
        p.extend_from_slice(&1i32.to_ne_bytes());
        p.extend_from_slice(&1i32.to_ne_bytes());
    } else {
        p.extend_from_slice(&nitems.to_ne_bytes());
        p.extend_from_slice(&1i32.to_ne_bytes());
    }
    if hasnull {
        let mut bitmap = vec![0xFFu8; ((nitems as usize) + 7) / 8];
        if arm == 3 {
            // one null bit cleared (element 2 null) -> "must not contain nulls"
            bitmap[0] &= !(1 << 2);
        }
        p.extend_from_slice(&bitmap);
    }
    // pad to the data offset (payload offsets = image offsets minus vl_len_)
    let data_at = if dataoffset != 0 {
        dataoffset as usize - 4
    } else {
        // ARR_OVERHEAD_NONULLS(ndim) - 4
        (16 + 8 * ndim as usize + 7) & !7
    };
    while p.len() < data_at {
        p.push(0);
    }
    for i in 0..nitems as usize {
        let v = if i < 4 { ws[i] } else { extra };
        p.extend_from_slice(&v.to_ne_bytes());
    }
    p
}

// KNOWN-DIVERGENCE-2 carve RETIRED 2026-07-31: adjudicated pgrust-bug and
// FIXED — sort_and_uniq_items now uses the verbatim pg_qsort port
// (crates/backend/utils/adt/tsrank/src/qsort.rs), so the same-lexeme tie
// SURVIVOR matches C's qsort_arg exactly. Docker postgres:18.3 ground truth
// (2026-07-31): ts_rank('aab:1 u:2', 'u|v|w|x|y|z|aa:*|aa') = 0.008684673
// vs ...'|aa|aa:*') = 0.017369347 — tie order is REAL PG behavior, not an
// oracle artifact. Flagged-tie queries are back on the strict f32 plane.

/// UTF-8 + NUL-free gate, then parse with the shipped Rust parser.
fn parse_tsvector(m: mcx::Mcx<'_>, text: &[u8]) -> Option<Vec<u8>> {
    if text.len() > MAX_TEXT || text.contains(&0) {
        return None;
    }
    std::str::from_utf8(text).ok()?;
    match tsvector_in_core(m, text, None) {
        Ok(Some(img)) => Some(img[4..].to_vec()),
        _ => None,
    }
}

fn is_w_variant(variant: u8) -> bool {
    variant % 4 <= 1
}

fn is_f_variant(variant: u8) -> bool {
    variant % 4 == 0 || variant % 4 == 2
}

pub fn tsrank_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    // The rank_cd cover walks reach the TS_execute CHECK_FOR_INTERRUPTS
    // calls (tsvector_core::execute); shared no-op install, first-wins.
    crate::install_check_for_interrupts_seam_once();
    if data.len() < 23 {
        return;
    }
    let variant = data[0] % 8;
    let wmode = data[1];
    let wbytes: [u8; 16] = data[2..18].try_into().unwrap();
    let shape = data[18];
    let method = {
        let raw: [u8; 4] = data[19..23].try_into().unwrap();
        if raw[0] & 0x80 != 0 {
            (raw[1] & 0x3f) as i32
        } else {
            i32::from_ne_bytes(raw)
        }
    };
    let rest = &data[23..];
    if rest.len() < 2 {
        return;
    }
    let vlen = u16::from_le_bytes([rest[0], rest[1]]) as usize;
    let rest = &rest[2..];
    if vlen > rest.len() {
        return;
    }
    let (vtext, qbytes) = rest.split_at(vlen);

    let cx = MemoryContext::new("tsrank_fuzz");
    let m = cx.mcx();
    let Some(vpayload) = parse_tsvector(m, vtext) else { return };
    let qpayload = gen_tsquery_payload(qbytes);
    let ws = gen_weights(wmode, &wbytes);
    let wpayload = build_weights_payload(shape, &ws, 0.3);

    // C side.
    let mut cbits = 0u32;
    let crc = unsafe {
        pg_diff_ts_rank(
            variant as i32,
            wpayload.as_ptr(),
            wpayload.len() as i32,
            vpayload.as_ptr(),
            vpayload.len() as i32,
            qpayload.as_ptr(),
            qpayload.len() as i32,
            method,
            &mut cbits,
        )
    };
    let cclass = unsafe { pg_diff_errcode_get() };

    // Rust fc-wrapper side (primary plane).
    let wimg = varlena_image(&wpayload);
    let vimg = varlena_image(&vpayload);
    let qimg = varlena_image(&qpayload);
    let f: PGFunction = match variant {
        0 => fcb::fc_ts_rank_wttf,
        1 => fcb::fc_ts_rank_wtt,
        2 => fcb::fc_ts_rank_ttf,
        3 => fcb::fc_ts_rank_tt,
        4 => fcb::fc_ts_rankcd_wttf,
        5 => fcb::fc_ts_rankcd_wtt,
        6 => fcb::fc_ts_rankcd_ttf,
        _ => fcb::fc_ts_rankcd_tt,
    };
    let rres = match variant {
        0 | 4 => fc_call::<4>(
            f,
            m,
            [
                varlena_datum(&wimg),
                varlena_datum(&vimg),
                varlena_datum(&qimg),
                NullableDatum::value(Datum::from_i32(method)),
            ],
        ),
        1 | 5 => fc_call::<3>(
            f,
            m,
            [varlena_datum(&wimg), varlena_datum(&vimg), varlena_datum(&qimg)],
        ),
        2 | 6 => fc_call::<3>(
            f,
            m,
            [
                varlena_datum(&vimg),
                varlena_datum(&qimg),
                NullableDatum::value(Datum::from_i32(method)),
            ],
        ),
        _ => fc_call::<2>(f, m, [varlena_datum(&vimg), varlena_datum(&qimg)]),
    };

    match (&rres, crc) {
        (Ok(d), 0) => {
            let rbits = d.as_usize() as u32;
            assert_eq!(
                rbits,
                cbits,
                "ts_rank variant {variant} f32 divergence: rust {:e} ({rbits:#010x}) vs C {:e} \
                 ({cbits:#010x}) on v={:?} method={method} w={ws:?} shape={shape} q={qpayload:02x?}",
                f32::from_bits(rbits),
                f32::from_bits(cbits),
                String::from_utf8_lossy(vtext),
            );

            // Core == wrapper pin for the non-weights variants.
            let eff_method = if is_f_variant(variant) { method } else { DEF_NORM_METHOD };
            if !is_w_variant(variant) {
                let core = if variant < 4 {
                    calc_rank(
                        m,
                        &DEFAULT_WEIGHTS,
                        TsVec { payload: &vpayload },
                        TsQueryRef { payload: &qpayload },
                        eff_method,
                    )
                } else {
                    calc_rank_cd(
                        m,
                        &DEFAULT_WEIGHTS,
                        TsVec { payload: &vpayload },
                        TsQueryRef { payload: &qpayload },
                        eff_method,
                    )
                };
                let core = core.expect("rank core errored where fc wrapper succeeded");
                assert_eq!(core.to_bits(), rbits, "rank core != fc wrapper (variant {variant})");
            }
        }
        (Err(e), 1) => {
            assert_eq!(
                err_class(e),
                cclass,
                "ts_rank variant {variant} errcode class divergence: rust {:?} vs C {cclass} \
                 (shape={shape} w={ws:?})",
                e.sqlstate,
            );
        }
        _ => panic!(
            "ts_rank variant {variant} VERDICT divergence: rust {:?} vs C rc {crc} class {cclass} \
             (shape={shape} w={ws:?} method={method} v={:?} q={qpayload:02x?})",
            rres.as_ref().map(|_| "ok").map_err(|e| e.sqlstate),
            String::from_utf8_lossy(vtext),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(variant: u8, wmode: u8, shape: u8, method: i32, vtext: &[u8], qseed: &[u8]) {
        let mut v = vec![variant, wmode];
        v.extend_from_slice(&[0x20u8; 16]);
        v.push(shape);
        v.extend_from_slice(&method.to_ne_bytes());
        v.extend_from_slice(&(vtext.len() as u16).to_le_bytes());
        v.extend_from_slice(vtext);
        v.extend_from_slice(qseed);
        tsrank_diff(&v);
    }

    const V: &[u8] = b"cat:1A dog:2,5B fish:3 abc:16383";

    #[test]
    fn smoke_all_variants() {
        for variant in 0..8 {
            for qseed in 0..24u8 {
                run(variant, 2, 0, 0, V, &[qseed, qseed ^ 0x5a, 7, 3, qseed]);
            }
        }
    }

    #[test]
    fn smoke_methods() {
        for method in 0..64 {
            run(2, 2, 0, method, V, &[0x81, 3, 9, 1]);
            run(6, 2, 0, method, V, &[0x81, 3, 9, 1]);
        }
    }

    /// Filtered-run regression (2026-08-01): the rank_cd cover walks reach
    /// the TS_execute CHECK_FOR_INTERRUPTS calls restored by 229915b8d7;
    /// this target relied on other modules installing the seam first, so a
    /// filtered run (the fleet fuzz-binary posture) panicked "seam not
    /// installed". Re-exec with ONLY smoke_methods selected so no
    /// benefactor module can mask a dropped install.
    #[test]
    fn smoke_methods_survives_filtered_run() {
        let exe = std::env::current_exe().unwrap();
        let out = std::process::Command::new(&exe)
            .args(["--exact", "tsrank_diff::tests::smoke_methods"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "filtered smoke_methods failed (check_for_interrupts seam install dropped?):\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn smoke_weight_regimes() {
        for wmode in 0..4 {
            for b in [0u8, 1, 2, 8, 10, 11, 0xff] {
                let mut v = vec![0u8, wmode];
                v.extend_from_slice(&[b; 16]);
                v.push(0);
                v.extend_from_slice(&0i32.to_ne_bytes());
                v.extend_from_slice(&(V.len() as u16).to_le_bytes());
                v.extend_from_slice(V);
                v.extend_from_slice(&[0x81, 3]);
                tsrank_diff(&v);
                v[0] = 4; // rankcd too
                tsrank_diff(&v);
            }
        }
    }

    #[test]
    fn smoke_array_shape_errors() {
        for shape in 0..8 {
            run(0, 2, shape, 0, V, &[0x81, 3]);
            run(5, 2, shape, 0, V, &[0x81, 3]);
        }
    }

    #[test]
    fn smoke_empty_and_posnull() {
        run(3, 2, 0, 0, b"a b c", &[0x81, 3]); // no positions anywhere
        run(7, 2, 0, 0, b"a b c", &[0x81, 3]); // rankcd: get_docrep -> None
        run(3, 2, 0, 0, b"a:1 b c:2", &[0x81, 3]); // POSNULL mixed
        run(3, 2, 0, 0, V, &[31]); // empty tsquery
        run(7, 2, 0, 0, V, &[31]);
    }
}
