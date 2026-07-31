//! numericfam: differential fuzz drivers — shipped adt_numeric vs verbatim
//! vendored PostgreSQL 18.3 C (upstream sha 62d6c7d3df, the WHOLE numeric.c
//! `#include`d into csrc/numericfam/pg_numeric_oracle.c).
//!
//! Two targets share this module and the one oracle entry point:
//!   - `numeric_io_diff`  — text/wire input languages: numeric_in (str ×
//!     typmod), numeric_out, numeric() typmod application, numeric_recv
//!     (raw wire × typmod), numeric_send round-trip, numerictypmodin,
//!     numeric_out_sci.
//!   - `numeric_ops_diff` — value-domain ops over operands ADMITTED THROUGH
//!     BOTH SIDES' numeric_recv (the wire grammar is the operand language;
//!     admission itself is a compared plane): arithmetic, comparison,
//!     rounding, math (sqrt/exp/ln/log/pow/fac/gcd/lcm), conversions,
//!     hashing, width_bucket, in_range, the pure aggregate transfns
//!     (int8_sum / int8_avg / int2+int4_avg_accum(+inv)), and the
//!     pgrust-only keypack/fast_cmp kernels checked against C numeric_cmp
//!     as an order oracle (pack(a) ⋚ pack(b) must match cmp(a,b)) plus
//!     unpack round-trip identity.
//!
//! Comparison planes (message text out of scope): exact result varlena
//! image bytes (Rust NumericImage::as_bytes() == C palloc'd varlena) /
//! cstring bytes / scalar bits; error-verdict; sqlstate (both sides use the
//! MAKE_SQLSTATE i32 encoding — compared as raw ints). SQL NULL results
//! (scale/min_scale of specials, int8_avg count==0, int8_sum null lattice)
//! are a distinct verdict (C rc -2).
//!
//! Scope carves (documented, both sides skip identically):
//!   - numeric_in inputs restricted to valid UTF-8 without NUL: the shipped
//!     entry takes &str (fmgr layer does lossy conversion, a non-surface —
//!     the grammar is ASCII; non-UTF-8 bytes only reach reject arms).
//!   - Math-op operand size caps (cost fences, not behavior carves): the
//!     skip predicate is evaluated on driver-side fields BEFORE either side
//!     runs, so no behavior is ever compared asymmetrically.
//!   - numeric_support / generate_series / random_numeric: planner-nodes /
//!     SRF / PRNG carves (see phase1-routes.tsv).

use datum::{Datum, NullableDatum};
use types_error::PgResult;
use types_fmgr::{LocalFcinfo, PGFunction};

use adt_numeric::builtins as nb;
use adt_numeric::{Num, NumericImage};

extern "C" {
    fn pg_diff_num_call(
        op: i32,
        a: *const u8,
        alen: i32,
        b: *const u8,
        blen: i32,
        c: *const u8,
        clen: i32,
        i64arg: i64,
        i32arg: i32,
        f64arg: f64,
        out: *mut u8,
        outcap: i32,
        outlen: *mut i32,
        scalar: *mut u64,
    ) -> i32;
}

// Op codes — MUST match the enum in csrc/numericfam/pg_numeric_oracle.c.
const OP_IN: i32 = 1;
const OP_OUT: i32 = 2;
const OP_APPLY_TYPMOD: i32 = 3;
const OP_RECV: i32 = 4;
const OP_SEND: i32 = 5;
const OP_TYPMODIN: i32 = 6;
const OP_OUT_SCI: i32 = 7;
const OP_ADD: i32 = 10;
const OP_SUB: i32 = 11;
const OP_MUL: i32 = 12;
const OP_DIV: i32 = 13;
const OP_DIV_TRUNC: i32 = 14;
const OP_MOD: i32 = 15;
const OP_MIN: i32 = 16;
const OP_MAX: i32 = 17;
const OP_GCD: i32 = 18;
const OP_LCM: i32 = 19;
const OP_CMP: i32 = 20;
const OP_EQ: i32 = 21;
const OP_NE: i32 = 22;
const OP_GT: i32 = 23;
const OP_GE: i32 = 24;
const OP_LT: i32 = 25;
const OP_LE: i32 = 26;
const OP_ABS: i32 = 30;
const OP_UMINUS: i32 = 31;
const OP_UPLUS: i32 = 32;
const OP_SIGN: i32 = 33;
const OP_ROUND: i32 = 34;
const OP_TRUNC: i32 = 35;
const OP_CEIL: i32 = 36;
const OP_FLOOR: i32 = 37;
const OP_INC: i32 = 38;
const OP_SCALE: i32 = 39;
const OP_MIN_SCALE: i32 = 40;
const OP_TRIM_SCALE: i32 = 41;
const OP_SQRT: i32 = 50;
const OP_EXP: i32 = 51;
const OP_LN: i32 = 52;
const OP_LOG: i32 = 53;
const OP_POWER: i32 = 54;
const OP_FAC: i32 = 55;
const OP_WIDTH_BUCKET: i32 = 56;
const OP_IN_RANGE: i32 = 57;
const OP_TO_INT2: i32 = 60;
const OP_TO_INT4: i32 = 61;
const OP_TO_INT8: i32 = 62;
const OP_TO_FLOAT4: i32 = 63;
const OP_TO_FLOAT8: i32 = 64;
const OP_FROM_INT2: i32 = 65;
const OP_FROM_INT4: i32 = 66;
const OP_FROM_INT8: i32 = 67;
const OP_FROM_FLOAT4: i32 = 68;
const OP_FROM_FLOAT8: i32 = 69;
const OP_HASH: i32 = 70;
const OP_HASH_EXT: i32 = 71;
const OP_INT8_SUM: i32 = 80;
const OP_INT8_AVG: i32 = 81;
const OP_INT2_AVG_ACCUM: i32 = 82;
const OP_INT4_AVG_ACCUM: i32 = 83;
const OP_INT2_AVG_ACCUM_INV: i32 = 84;
const OP_INT4_AVG_ACCUM_INV: i32 = 85;

// numeric_out of a max-weight wire operand is ~148KB of digits (weight
// 32767 × 4 chars + dscale); 1 MiB bounds every reachable result image.
const OUTCAP: usize = 1 << 20;

/// C oracle verdict: Ok(bytes, scalar) / SQL NULL / error(sqlstate).
enum CRes {
    Ok(Vec<u8>, u64),
    Null,
    Err(i32),
}

fn c_call(
    op: i32,
    a: Option<&[u8]>,
    b: Option<&[u8]>,
    c: Option<&[u8]>,
    i64arg: i64,
    i32arg: i32,
    f64arg: f64,
) -> CRes {
    let mut out = vec![0u8; OUTCAP];
    let mut outlen: i32 = 0;
    let mut scalar: u64 = 0;
    let sl = |o: Option<&[u8]>| -> (*const u8, i32) {
        match o {
            Some(s) => (s.as_ptr(), s.len() as i32),
            None => (core::ptr::null(), -1),
        }
    };
    let (ap, al) = sl(a);
    let (bp, bl) = sl(b);
    let (cp, cl) = sl(c);
    // SAFETY: buffers live across the call; the oracle aborts (never
    // overflows) past outcap.
    let rc = unsafe {
        pg_diff_num_call(
            op, ap, al, bp, bl, cp, cl, i64arg, i32arg, f64arg,
            out.as_mut_ptr(), OUTCAP as i32, &mut outlen, &mut scalar,
        )
    };
    match rc {
        0 => {
            out.truncate(outlen as usize);
            CRes::Ok(out, scalar)
        }
        -2 => CRes::Null,
        e => CRes::Err(e),
    }
}

// ---------------------------------------------------------------------------
// comparators
// ---------------------------------------------------------------------------

fn sqlstate_of(e: &types_error::PgError) -> i32 {
    e.sqlstate.0
}

/// Rust image result vs C varlena result (whole-image byte parity).
fn chk_img(name: &str, cres: CRes, rres: PgResult<NumericImage>, dbg: &dyn Fn() -> String) {
    match (cres, rres) {
        (CRes::Ok(cimg, _), Ok(img)) => assert!(
            cimg == img.as_bytes(),
            "{name} IMAGE DIVERGENCE {}: C={cimg:02x?} Rust={:02x?}",
            dbg(),
            img.as_bytes()
        ),
        (CRes::Err(ce), Err(re)) => assert!(
            ce == sqlstate_of(&re),
            "{name} ERRCODE DIVERGENCE {}: C={ce:#x} Rust={:#x} ({})",
            dbg(),
            sqlstate_of(&re),
            re.message
        ),
        (CRes::Ok(..), Err(re)) => {
            panic!("{name} VERDICT DIVERGENCE {}: C=Ok Rust=Err({})", dbg(), re.message)
        }
        (CRes::Err(ce), Ok(_)) => {
            panic!("{name} VERDICT DIVERGENCE {}: C=Err({ce:#x}) Rust=Ok", dbg())
        }
        (CRes::Null, _) => panic!("{name} unexpected C NULL {}", dbg()),
    }
}

/// Rust scalar result vs C scalar result.
fn chk_scalar<T: PartialEq + core::fmt::Debug>(
    name: &str,
    cres: CRes,
    cconv: &dyn Fn(u64) -> T,
    rres: PgResult<T>,
    dbg: &dyn Fn() -> String,
) {
    match (cres, rres) {
        (CRes::Ok(_, s), Ok(v)) => {
            let cv = cconv(s);
            assert!(cv == v, "{name} VALUE DIVERGENCE {}: C={cv:?} Rust={v:?}", dbg());
        }
        (CRes::Err(ce), Err(re)) => assert!(
            ce == sqlstate_of(&re),
            "{name} ERRCODE DIVERGENCE {}: C={ce:#x} Rust={:#x} ({})",
            dbg(),
            sqlstate_of(&re),
            re.message
        ),
        (CRes::Ok(..), Err(re)) => {
            panic!("{name} VERDICT DIVERGENCE {}: C=Ok Rust=Err({})", dbg(), re.message)
        }
        (CRes::Err(ce), Ok(v)) => {
            panic!("{name} VERDICT DIVERGENCE {}: C=Err({ce:#x}) Rust=Ok({v:?})", dbg())
        }
        (CRes::Null, _) => panic!("{name} unexpected C NULL {}", dbg()),
    }
}

// ---------------------------------------------------------------------------
// fc-wrapper plumbing (for the arms whose NULL lattice lives in builtins.rs)
// ---------------------------------------------------------------------------

fn fc_call<const N: usize>(
    f: PGFunction,
    mcx: Option<mcx::Mcx<'_>>,
    args: [NullableDatum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    if let Some(m) = mcx {
        // SAFETY: the arming context outlives this single call.
        unsafe { fcinfo.set_result_mcx(m) };
    }
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = a;
    }
    let r = f(None, &mut fcinfo);
    let isnull = fcinfo.isnull;
    (r, isnull)
}

/// Read a full varlena image (header included) back from an fc result datum.
///
/// # Safety
/// `d` points at a live 4-byte-header varlena in the armed arena.
unsafe fn varlena_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract.
    unsafe {
        let hdr = u32::from_ne_bytes([*p, *p.add(1), *p.add(2), *p.add(3)]);
        core::slice::from_raw_parts(p, (hdr >> 2) as usize)
    }
}

fn chk_fc_varlena(
    name: &str,
    cres: CRes,
    rres: (PgResult<Datum>, bool),
    dbg: &dyn Fn() -> String,
) {
    match (cres, rres) {
        (CRes::Ok(cimg, _), (Ok(d), false)) => {
            // SAFETY: img result datums point at live varlenas in the armed mcx.
            let rimg = unsafe { varlena_image(d) };
            assert!(
                cimg == rimg,
                "{name} IMAGE DIVERGENCE {}: C={cimg:02x?} Rust={rimg:02x?}",
                dbg()
            );
        }
        (CRes::Null, (Ok(_), true)) => {}
        (CRes::Err(ce), (Err(re), _)) => assert!(
            ce == sqlstate_of(&re),
            "{name} ERRCODE DIVERGENCE {}: C={ce:#x} Rust={:#x} ({})",
            dbg(),
            sqlstate_of(&re),
            re.message
        ),
        (c, (r, isnull)) => {
            let cv = match c {
                CRes::Ok(..) => "Ok".to_string(),
                CRes::Null => "NULL".to_string(),
                CRes::Err(e) => format!("Err({e:#x})"),
            };
            let rv = match (&r, isnull) {
                (Ok(_), true) => "NULL".to_string(),
                (Ok(_), false) => "Ok".to_string(),
                (Err(e), _) => format!("Err({:#x})", sqlstate_of(e)),
            };
            panic!("{name} VERDICT DIVERGENCE {}: C={cv} Rust={rv}", dbg());
        }
    }
}

// ---------------------------------------------------------------------------
// operand admission: the wire grammar is the operand language
// ---------------------------------------------------------------------------

/// Structured wire assembly: [nd|flags][weight:i16][signsel][dscale:u16 BE]
/// [digits: nd × u16 BE]. Returns (wire bytes, nd, weight) or None if the
/// input is too short.
fn take_wire(bytes: &mut &[u8]) -> Option<(Vec<u8>, usize, i16)> {
    let (&b0, rest) = bytes.split_first()?;
    let nd = (b0 & 0x1F) as usize;
    if rest.len() < 5 + 2 * nd {
        return None;
    }
    let (hdr, rest) = rest.split_at(5 + 2 * nd);
    *bytes = rest;
    let weight = i16::from_be_bytes([hdr[0], hdr[1]]);
    let sign: u16 = match hdr[2] % 8 {
        0 => 0x0000,            /* NUMERIC_POS */
        1 => 0x4000,            /* NUMERIC_NEG */
        2 => 0xC000,            /* NUMERIC_NAN */
        3 => 0xD000,            /* NUMERIC_PINF */
        4 => 0xF000,            /* NUMERIC_NINF */
        _ => u16::from_be_bytes([hdr[2], hdr[3]]), /* raw: invalid-sign arm */
    };
    let dscale = u16::from_be_bytes([hdr[3], hdr[4]]);
    let mut wire = Vec::with_capacity(8 + 2 * nd);
    wire.extend_from_slice(&(nd as u16).to_be_bytes());
    wire.extend_from_slice(&weight.to_be_bytes());
    wire.extend_from_slice(&sign.to_be_bytes());
    wire.extend_from_slice(&dscale.to_be_bytes());
    wire.extend_from_slice(&hdr[5..5 + 2 * nd]);
    Some((wire, nd, weight))
}

/// Run one wire image through BOTH sides' numeric_recv (typmod -1), compare
/// the admission verdict, and return the admitted image (Rust image bytes ==
/// C image bytes, asserted) with its (nd, weight) shape.
fn admit(wire: &[u8], nd: usize, weight: i16) -> Option<NumericImage> {
    let cres = c_call(OP_RECV, Some(wire), None, None, 0, -1, 0.0);
    let cx = mcx::MemoryContext::new("numericfam");
    let rres = (|| {
        let mut v = ::stringinfo::StringInfo::with_capacity_in(cx.mcx(), wire.len() + 1)?;
        v.append_bytes(wire)?;
        adt_numeric::numeric_recv(&mut v, -1)
    })();
    let dbg = || format!("recv wire={wire:02x?} nd={nd} weight={weight}");
    match (&cres, &rres) {
        (CRes::Ok(cimg, _), Ok(img)) => {
            assert!(
                cimg == img.as_bytes(),
                "numeric_recv IMAGE DIVERGENCE {}: C={cimg:02x?} Rust={:02x?}",
                dbg(),
                img.as_bytes()
            );
        }
        (CRes::Err(ce), Err(re)) => {
            assert!(
                *ce == sqlstate_of(re),
                "numeric_recv ERRCODE DIVERGENCE {}: C={ce:#x} Rust={:#x} ({})",
                dbg(),
                sqlstate_of(re),
                re.message
            );
            return None;
        }
        _ => {
            let cv = match &cres {
                CRes::Ok(..) => "Ok",
                CRes::Null => "NULL",
                CRes::Err(_) => "Err",
            };
            panic!(
                "numeric_recv VERDICT DIVERGENCE {}: C={cv} Rust={}",
                dbg(),
                match &rres {
                    Ok(_) => "Ok".to_string(),
                    Err(e) => format!("Err({:#x} {})", sqlstate_of(e), e.message),
                }
            );
        }
    }
    rres.ok()
}

fn take_operand(bytes: &mut &[u8]) -> Option<(NumericImage, usize, i16)> {
    let (wire, nd, weight) = take_wire(bytes)?;
    admit(&wire, nd, weight).map(|img| (img, nd, weight))
}

fn take_i64(bytes: &mut &[u8]) -> Option<i64> {
    if bytes.len() < 8 {
        return None;
    }
    let (h, rest) = bytes.split_at(8);
    *bytes = rest;
    Some(i64::from_be_bytes(h.try_into().unwrap()))
}

fn take_i32(bytes: &mut &[u8]) -> Option<i32> {
    if bytes.len() < 4 {
        return None;
    }
    let (h, rest) = bytes.split_at(4);
    *bytes = rest;
    Some(i32::from_be_bytes(h.try_into().unwrap()))
}

/// Cost fence for the iterative math kernels (driver-side, symmetric).
fn math_sized(nd: usize, weight: i16) -> bool {
    nd <= 6 && (-24..=24).contains(&weight)
}

// ---------------------------------------------------------------------------
// numeric_io_diff
// ---------------------------------------------------------------------------

pub fn numeric_io_diff(data: &[u8]) {
    let Some((&sel, mut rest)) = data.split_first() else {
        return;
    };
    match sel % 4 {
        // ---- numeric_in (text × typmod), then out + send on success ----
        0 => {
            let Some(typmod) = take_i32(&mut rest) else { return };
            if rest.len() > 256 || rest.contains(&0) {
                return;
            }
            let Ok(s) = core::str::from_utf8(rest) else {
                return; /* &str entry carve, see header */
            };
            let cres = c_call(OP_IN, Some(rest), None, None, 0, typmod, 0.0);
            let rres = adt_numeric::numeric_in(s, typmod, None).map(|o| o.expect("no escontext"));
            let dbg = || format!("in s={s:?} typmod={typmod}");
            let img = match (&cres, &rres) {
                (CRes::Ok(cimg, _), Ok(img)) => {
                    assert!(
                        cimg == img.as_bytes(),
                        "numeric_in IMAGE DIVERGENCE {}: C={cimg:02x?} Rust={:02x?}",
                        dbg(),
                        img.as_bytes()
                    );
                    img
                }
                (CRes::Err(ce), Err(re)) => {
                    assert!(
                        *ce == sqlstate_of(re),
                        "numeric_in ERRCODE DIVERGENCE {}: C={ce:#x} Rust={:#x} ({})",
                        dbg(),
                        sqlstate_of(re),
                        re.message
                    );
                    return;
                }
                _ => panic!(
                    "numeric_in VERDICT DIVERGENCE {}: C={} Rust={}",
                    dbg(),
                    match &cres {
                        CRes::Ok(..) => "Ok".to_string(),
                        CRes::Null => "NULL".to_string(),
                        CRes::Err(e) => format!("Err({e:#x})"),
                    },
                    match &rres {
                        Ok(_) => "Ok".to_string(),
                        Err(e) => format!("Err({:#x} {})", sqlstate_of(e), e.message),
                    }
                ),
            };
            // numeric_out round-trip
            let cres = c_call(OP_OUT, Some(img.as_bytes()), None, None, 0, 0, 0.0);
            let mut out = Vec::new();
            adt_numeric::numeric_out_into(img.num(), &mut out);
            match cres {
                CRes::Ok(cout, _) => assert!(
                    cout == out,
                    "numeric_out DIVERGENCE {}: C={:?} Rust={:?}",
                    dbg(),
                    String::from_utf8_lossy(&cout),
                    String::from_utf8_lossy(&out)
                ),
                _ => panic!("numeric_out unexpected C verdict {}", dbg()),
            }
            // numeric_send round-trip
            let cres = c_call(OP_SEND, Some(img.as_bytes()), None, None, 0, 0, 0.0);
            let cx = mcx::MemoryContext::new("numericfam");
            let rbytes: Vec<u8> = match adt_numeric::numeric_send(cx.mcx(), img.num()) {
                Ok(rb) => rb.as_bytes().to_vec(),
                Err(e) => panic!(
                    "numeric_send VERDICT DIVERGENCE {}: Rust Err({:#x})",
                    dbg(),
                    sqlstate_of(&e)
                ),
            };
            match cres {
                CRes::Ok(cb, _) => assert!(
                    cb == rbytes,
                    "numeric_send DIVERGENCE {}: C={cb:02x?} Rust={rbytes:02x?}",
                    dbg()
                ),
                _ => panic!("numeric_send VERDICT DIVERGENCE {}: C err, Rust Ok", dbg()),
            }
        }
        // ---- numeric_recv (raw wire × typmod) — UNSTRUCTURED bytes ----
        1 => {
            let Some(typmod) = take_i32(&mut rest) else { return };
            if rest.len() > 512 {
                return;
            }
            let cres = c_call(OP_RECV, Some(rest), None, None, 0, typmod, 0.0);
            let cx = mcx::MemoryContext::new("numericfam");
            let rres = (|| {
                let mut v =
                    ::stringinfo::StringInfo::with_capacity_in(cx.mcx(), rest.len() + 1)?;
                v.append_bytes(rest)?;
                adt_numeric::numeric_recv(&mut v, typmod)
            })();
            chk_img("numeric_recv/raw", cres, rres, &|| {
                format!("wire={rest:02x?} typmod={typmod}")
            });
        }
        // ---- numeric() typmod application over an admitted operand ----
        2 => {
            let Some(typmod) = take_i32(&mut rest) else { return };
            let Some((img, nd, weight)) = take_operand(&mut rest) else {
                return;
            };
            let cres = c_call(OP_APPLY_TYPMOD, Some(img.as_bytes()), None, None, 0, typmod, 0.0);
            let rres = adt_numeric::numeric_apply_typmod(img.num(), typmod);
            chk_img("numeric(typmod)", cres, rres, &|| {
                format!("img={:02x?} typmod={typmod} nd={nd} w={weight}", img.as_bytes())
            });
        }
        // ---- numerictypmodin + numeric_out_sci ----
        _ => {
            let Some(&n) = rest.first() else { return };
            rest = &rest[1..];
            let n = (n % 4) as usize;
            let mut tl = Vec::with_capacity(n);
            for _ in 0..n {
                let Some(v) = take_i32(&mut rest) else { return };
                tl.push(v);
            }
            let mut le = Vec::with_capacity(4 * n);
            for v in &tl {
                le.extend_from_slice(&v.to_le_bytes());
            }
            let cres = c_call(OP_TYPMODIN, Some(&le), None, None, 0, 0, 0.0);
            let rres = adt_numeric::numerictypmodin_core(&tl);
            chk_scalar("numerictypmodin", cres, &|s| s as u32 as i32, rres, &|| {
                format!("tl={tl:?}")
            });

            // out_sci over an admitted operand with a bounded rscale
            let Some(rscale) = take_i32(&mut rest) else { return };
            let rscale = rscale.rem_euclid(2005) - 1002;
            let Some((img, ..)) = take_operand(&mut rest) else {
                return;
            };
            let cres = c_call(OP_OUT_SCI, Some(img.as_bytes()), None, None, 0, rscale, 0.0);
            let mut out = Vec::new();
            adt_numeric::numeric_out_sci(img.num(), rscale, &mut out);
            match cres {
                CRes::Ok(cout, _) => assert!(
                    cout == out,
                    "numeric_out_sci DIVERGENCE img={:02x?} rscale={rscale}: C={:?} Rust={:?}",
                    img.as_bytes(),
                    String::from_utf8_lossy(&cout),
                    String::from_utf8_lossy(&out)
                ),
                CRes::Err(_) => { /* out_sci allocs can't fail; unreachable */ }
                CRes::Null => unreachable!(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// numeric_ops_diff
// ---------------------------------------------------------------------------

pub fn numeric_ops_diff(data: &[u8]) {
    let Some((&sel, mut rest)) = data.split_first() else {
        return;
    };
    let r = &mut rest;

    macro_rules! binop_img {
        ($name:literal, $op:expr, $core:path) => {{
            let Some((a, ..)) = take_operand(r) else { return };
            let Some((b, ..)) = take_operand(r) else { return };
            let cres = c_call($op, Some(a.as_bytes()), Some(b.as_bytes()), None, 0, 0, 0.0);
            let rres = $core(a.num(), b.num());
            chk_img($name, cres, rres, &|| {
                format!("a={:02x?} b={:02x?}", a.as_bytes(), b.as_bytes())
            });
        }};
    }
    macro_rules! mathop2_img {
        ($name:literal, $op:expr, $core:path) => {{
            let Some((a, nda, wa)) = take_operand(r) else { return };
            let Some((b, ndb, wb)) = take_operand(r) else { return };
            if !math_sized(nda, wa) || !math_sized(ndb, wb) {
                return;
            }
            let cres = c_call($op, Some(a.as_bytes()), Some(b.as_bytes()), None, 0, 0, 0.0);
            let rres = $core(a.num(), b.num());
            chk_img($name, cres, rres, &|| {
                format!("a={:02x?} b={:02x?}", a.as_bytes(), b.as_bytes())
            });
        }};
    }
    macro_rules! unop_img {
        ($name:literal, $op:expr, |$a:ident| $body:expr) => {{
            let Some(($a, ..)) = take_operand(r) else { return };
            let cres = c_call($op, Some($a.as_bytes()), None, None, 0, 0, 0.0);
            let rres = $body;
            chk_img($name, cres, rres, &|| format!("a={:02x?}", $a.as_bytes()));
        }};
    }

    match sel {
        0 => binop_img!("numeric_add", OP_ADD, adt_numeric::numeric_add_common),
        1 => binop_img!("numeric_sub", OP_SUB, adt_numeric::numeric_sub_common),
        2 => binop_img!("numeric_mul", OP_MUL, adt_numeric::numeric_mul_common),
        3 => binop_img!("numeric_div", OP_DIV, adt_numeric::numeric_div_common),
        4 => binop_img!("numeric_div_trunc", OP_DIV_TRUNC, adt_numeric::numeric_div_trunc_common),
        5 => binop_img!("numeric_mod", OP_MOD, adt_numeric::numeric_mod_common),
        6 | 7 => {
            // min/max: winning-DATUM identity (C returns one of its inputs)
            let Some((a, ..)) = take_operand(r) else { return };
            let Some((b, ..)) = take_operand(r) else { return };
            let (op, name, want_a) = if sel == 6 {
                (OP_MIN, "numeric_smaller", adt_numeric::cmp_numerics(a.num(), b.num()) < 0)
            } else {
                (OP_MAX, "numeric_larger", adt_numeric::cmp_numerics(a.num(), b.num()) > 0)
            };
            let rimg = if want_a { &a } else { &b };
            let cres = c_call(op, Some(a.as_bytes()), Some(b.as_bytes()), None, 0, 0, 0.0);
            match cres {
                CRes::Ok(cimg, _) => assert!(
                    cimg == rimg.as_bytes(),
                    "{name} DIVERGENCE a={:02x?} b={:02x?}: C={cimg:02x?} Rust={:02x?}",
                    a.as_bytes(),
                    b.as_bytes(),
                    rimg.as_bytes()
                ),
                _ => panic!("{name}: unexpected C verdict"),
            }
        }
        8 => mathop2_img!("numeric_gcd", OP_GCD, adt_numeric::numeric_gcd_common),
        9 => mathop2_img!("numeric_lcm", OP_LCM, adt_numeric::numeric_lcm_common),
        10 => {
            let Some((a, ..)) = take_operand(r) else { return };
            let Some((b, ..)) = take_operand(r) else { return };
            let cres = c_call(OP_CMP, Some(a.as_bytes()), Some(b.as_bytes()), None, 0, 0, 0.0);
            chk_scalar(
                "numeric_cmp",
                cres,
                &|s| s as u32 as i32,
                Ok(adt_numeric::cmp_numerics(a.num(), b.num())),
                &|| format!("a={:02x?} b={:02x?}", a.as_bytes(), b.as_bytes()),
            );
            // bool family rides the same operands (cheap, full lattice)
            for (op, name, rv) in [
                (OP_EQ, "eq", adt_numeric::numeric_eq(a.num(), b.num())),
                (OP_NE, "ne", adt_numeric::numeric_ne(a.num(), b.num())),
                (OP_GT, "gt", adt_numeric::numeric_gt(a.num(), b.num())),
                (OP_GE, "ge", adt_numeric::numeric_ge(a.num(), b.num())),
                (OP_LT, "lt", adt_numeric::numeric_lt(a.num(), b.num())),
                (OP_LE, "le", adt_numeric::numeric_le(a.num(), b.num())),
            ] {
                let cres = c_call(op, Some(a.as_bytes()), Some(b.as_bytes()), None, 0, 0, 0.0);
                chk_scalar(name, cres, &|s| s != 0, Ok(rv), &|| {
                    format!("a={:02x?} b={:02x?}", a.as_bytes(), b.as_bytes())
                });
            }
        }
        11 => unop_img!("numeric_abs", OP_ABS, |a| Ok(adt_numeric::numeric_abs(a.num()))),
        12 => unop_img!("numeric_uminus", OP_UMINUS, |a| Ok(adt_numeric::numeric_uminus(a.num()))),
        13 => unop_img!("numeric_uplus", OP_UPLUS, |a| Ok(adt_numeric::numeric_uplus(a.num()))),
        14 => unop_img!("numeric_sign", OP_SIGN, |a| adt_numeric::numeric_sign(a.num())),
        15 | 16 => {
            let Some(scale) = take_i32(r) else { return };
            let Some((a, ..)) = take_operand(r) else { return };
            let (op, name) = if sel == 15 { (OP_ROUND, "numeric_round") } else { (OP_TRUNC, "numeric_trunc") };
            let cres = c_call(op, Some(a.as_bytes()), None, None, 0, scale, 0.0);
            let rres = if sel == 15 {
                adt_numeric::numeric_round_common(a.num(), scale)
            } else {
                adt_numeric::numeric_trunc_common(a.num(), scale)
            };
            chk_img(name, cres, rres, &|| {
                format!("a={:02x?} scale={scale}", a.as_bytes())
            });
        }
        17 => unop_img!("numeric_ceil", OP_CEIL, |a| adt_numeric::numeric_ceil(a.num())),
        18 => unop_img!("numeric_floor", OP_FLOOR, |a| adt_numeric::numeric_floor(a.num())),
        19 => unop_img!("numeric_inc", OP_INC, |a| adt_numeric::numeric_inc(a.num())),
        20 | 21 => {
            // scale / min_scale: fc-level (SQL NULL arm on specials)
            let Some((a, ..)) = take_operand(r) else { return };
            let (op, name, f): (i32, &str, PGFunction) = if sel == 20 {
                (OP_SCALE, "numeric_scale", nb::fc_numeric_scale)
            } else {
                (OP_MIN_SCALE, "numeric_min_scale", nb::fc_numeric_min_scale)
            };
            let cres = c_call(op, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            let cx = mcx::MemoryContext::new("numericfam");
            let (rres, isnull) = fc_call(
                f,
                Some(cx.mcx()),
                [NullableDatum::value(Datum::from_usize(a.as_bytes().as_ptr() as usize))],
            );
            match (cres, rres, isnull) {
                (CRes::Ok(_, s), Ok(d), false) => assert!(
                    s as u32 as i32 == d.as_i32(),
                    "{name} VALUE DIVERGENCE a={:02x?}: C={} Rust={}",
                    a.as_bytes(),
                    s as u32 as i32,
                    d.as_i32()
                ),
                (CRes::Null, Ok(_), true) => {}
                (c, rr, isn) => panic!(
                    "{name} VERDICT DIVERGENCE a={:02x?}: C={} Rust={}",
                    a.as_bytes(),
                    match c {
                        CRes::Ok(..) => "Ok".to_string(),
                        CRes::Null => "NULL".to_string(),
                        CRes::Err(e) => format!("Err({e:#x})"),
                    },
                    match (&rr, isn) {
                        (Ok(_), true) => "NULL".to_string(),
                        (Ok(_), false) => "Ok".to_string(),
                        (Err(e), _) => format!("Err({:#x})", sqlstate_of(e)),
                    }
                ),
            }
        }
        22 => unop_img!("numeric_trim_scale", OP_TRIM_SCALE, |a| adt_numeric::numeric_trim_scale(
            a.num()
        )),
        23 => {
            let Some((a, nd, w)) = take_operand(r) else { return };
            if nd > 16 {
                return;
            }
            let _ = w;
            let cres = c_call(OP_SQRT, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            chk_img("numeric_sqrt", cres, adt_numeric::numeric_sqrt(a.num()), &|| {
                format!("a={:02x?}", a.as_bytes())
            });
        }
        24 => {
            let Some((a, nd, w)) = take_operand(r) else { return };
            if !math_sized(nd, w) {
                return;
            }
            let cres = c_call(OP_EXP, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            chk_img("numeric_exp", cres, adt_numeric::numeric_exp(a.num()), &|| {
                format!("a={:02x?}", a.as_bytes())
            });
        }
        25 => {
            let Some((a, nd, w)) = take_operand(r) else { return };
            if nd > 16 {
                return;
            }
            let _ = w;
            let cres = c_call(OP_LN, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            chk_img("numeric_ln", cres, adt_numeric::numeric_ln(a.num()), &|| {
                format!("a={:02x?}", a.as_bytes())
            });
        }
        26 => mathop2_img!("numeric_log", OP_LOG, adt_numeric::numeric_log),
        27 => mathop2_img!("numeric_power", OP_POWER, adt_numeric::numeric_power),
        28 => {
            let Some(n) = take_i64(r) else { return };
            let n = n.rem_euclid(600) - 50; /* negative/zero error cells + up to 550! */
            let cres = c_call(OP_FAC, None, None, None, n, 0, 0.0);
            chk_img("factorial", cres, adt_numeric::numeric_fac(n), &|| format!("n={n}"));
        }
        29 => {
            let Some(count) = take_i32(r) else { return };
            let Some((op, ..)) = take_operand(r) else { return };
            let Some((b1, ..)) = take_operand(r) else { return };
            let Some((b2, ..)) = take_operand(r) else { return };
            let cres = c_call(
                OP_WIDTH_BUCKET,
                Some(op.as_bytes()),
                Some(b1.as_bytes()),
                Some(b2.as_bytes()),
                0,
                count,
                0.0,
            );
            let rres = adt_numeric::width_bucket_numeric(op.num(), b1.num(), b2.num(), count);
            chk_scalar("width_bucket", cres, &|s| s as u32 as i32, rres, &|| {
                format!(
                    "op={:02x?} b1={:02x?} b2={:02x?} count={count}",
                    op.as_bytes(),
                    b1.as_bytes(),
                    b2.as_bytes()
                )
            });
        }
        30 => {
            let Some(&flags) = r.first() else { return };
            *r = &r[1..];
            let Some((val, ..)) = take_operand(r) else { return };
            let Some((base, ..)) = take_operand(r) else { return };
            let Some((offset, ..)) = take_operand(r) else { return };
            let (sub, less) = (flags & 1 != 0, flags & 2 != 0);
            let cres = c_call(
                OP_IN_RANGE,
                Some(val.as_bytes()),
                Some(base.as_bytes()),
                Some(offset.as_bytes()),
                0,
                (flags & 3) as i32,
                0.0,
            );
            let rres =
                adt_numeric::in_range_numeric_numeric(val.num(), base.num(), offset.num(), sub, less);
            chk_scalar("in_range", cres, &|s| s != 0, rres, &|| {
                format!(
                    "val={:02x?} base={:02x?} off={:02x?} sub={sub} less={less}",
                    val.as_bytes(),
                    base.as_bytes(),
                    offset.as_bytes()
                )
            });
        }
        31..=33 => {
            // numeric -> int2/int4/int8
            let Some((a, ..)) = take_operand(r) else { return };
            let (op, name) = match sel {
                31 => (OP_TO_INT2, "numeric_int2"),
                32 => (OP_TO_INT4, "numeric_int4"),
                _ => (OP_TO_INT8, "numeric_int8"),
            };
            let cres = c_call(op, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            let dbg = || format!("a={:02x?}", a.as_bytes());
            match sel {
                31 => chk_scalar(name, cres, &|s| s as u16 as i16 as i64, adt_numeric::numeric_int2(a.num()).map(|v| v as i64), &dbg),
                32 => chk_scalar(name, cres, &|s| s as u32 as i32 as i64, adt_numeric::numeric_int4(a.num()).map(|v| v as i64), &dbg),
                _ => chk_scalar(name, cres, &|s| s as i64, adt_numeric::numeric_int8(a.num()), &dbg),
            }
        }
        34 => {
            let Some((a, ..)) = take_operand(r) else { return };
            let cres = c_call(OP_TO_FLOAT4, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            chk_scalar(
                "numeric_float4",
                cres,
                &|s| (s as u32),
                adt_numeric::numeric_float4(a.num()).map(|v| v.to_bits()),
                &|| format!("a={:02x?}", a.as_bytes()),
            );
        }
        35 => {
            let Some((a, ..)) = take_operand(r) else { return };
            let cres = c_call(OP_TO_FLOAT8, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            chk_scalar(
                "numeric_float8",
                cres,
                &|s| s,
                adt_numeric::numeric_float8(a.num()).map(|v| v.to_bits()),
                &|| format!("a={:02x?}", a.as_bytes()),
            );
        }
        36..=38 => {
            let Some(v) = take_i64(r) else { return };
            let (op, name, rres) = match sel {
                36 => (OP_FROM_INT2, "int2_numeric", adt_numeric::int2_numeric(v as i16)),
                37 => (OP_FROM_INT4, "int4_numeric", adt_numeric::int4_numeric(v as i32)),
                _ => (OP_FROM_INT8, "int8_numeric", adt_numeric::int8_numeric(v)),
            };
            let cres = c_call(op, None, None, None, v, 0, 0.0);
            chk_img(name, cres, Ok(rres), &|| format!("v={v}"));
        }
        39 | 40 => {
            let Some(bits) = take_i64(r) else { return };
            if sel == 39 {
                let f = f32::from_bits(bits as u32);
                let cres = c_call(OP_FROM_FLOAT4, None, None, None, 0, 0, f as f64);
                chk_img("float4_numeric", cres, adt_numeric::float4_numeric(f), &|| {
                    format!("f={f:?} bits={:#x}", bits as u32)
                });
            } else {
                let f = f64::from_bits(bits as u64);
                let cres = c_call(OP_FROM_FLOAT8, None, None, None, 0, 0, f);
                chk_img("float8_numeric", cres, adt_numeric::float8_numeric(f), &|| {
                    format!("f={f:?} bits={bits:#x}")
                });
            }
        }
        41 => {
            let Some((a, ..)) = take_operand(r) else { return };
            let cres = c_call(OP_HASH, Some(a.as_bytes()), None, None, 0, 0, 0.0);
            chk_scalar(
                "hash_numeric",
                cres,
                &|s| s as u32,
                Ok(adt_numeric::ops::hash_numeric(a.num())),
                &|| format!("a={:02x?}", a.as_bytes()),
            );
        }
        42 => {
            let Some(seed) = take_i64(r) else { return };
            let Some((a, ..)) = take_operand(r) else { return };
            let cres = c_call(OP_HASH_EXT, Some(a.as_bytes()), None, None, seed, 0, 0.0);
            chk_scalar(
                "hash_numeric_extended",
                cres,
                &|s| s,
                Ok(adt_numeric::ops::hash_numeric_extended(a.num(), seed as u64)),
                &|| format!("a={:02x?} seed={seed}", a.as_bytes()),
            );
        }
        43 => {
            // int8_sum: full null lattice at fc level
            let Some(&flags) = r.first() else { return };
            *r = &r[1..];
            let Some(v) = take_i64(r) else { return };
            let state_null = flags & 1 != 0;
            let val_null = flags & 2 != 0;
            let state = if state_null {
                None
            } else {
                match take_operand(r) {
                    Some((img, ..)) => Some(img),
                    None => return,
                }
            };
            let cres = c_call(
                OP_INT8_SUM,
                state.as_ref().map(|s| s.as_bytes()),
                None,
                None,
                v,
                (flags & 3) as i32,
                0.0,
            );
            let cx = mcx::MemoryContext::new("numericfam");
            let arg0 = match &state {
                Some(img) => {
                    NullableDatum::value(Datum::from_usize(img.as_bytes().as_ptr() as usize))
                }
                None => NullableDatum::null(),
            };
            let arg1 = if val_null {
                NullableDatum::null()
            } else {
                NullableDatum::value(Datum::from_i64(v))
            };
            let rres = fc_call(nb::fc_int8_sum, Some(cx.mcx()), [arg0, arg1]);
            // C keeps a datum identity for the state-passthrough arm; the
            // image plane still compares byte-equal in every arm.
            chk_fc_varlena("int8_sum", cres, rres, &|| {
                format!("state_null={state_null} val_null={val_null} v={v}")
            });
        }
        44 => {
            // int8_avg over a synthesized {count,sum} transarray
            let Some(count) = take_i64(r) else { return };
            let Some(sum) = take_i64(r) else { return };
            let arr = int8_transarray_image(count, sum);
            let cres = c_call(OP_INT8_AVG, Some(&arr), None, None, 0, 0, 0.0);
            let cx = mcx::MemoryContext::new("numericfam");
            let rres = fc_call(
                nb::fc_int8_avg,
                Some(cx.mcx()),
                [NullableDatum::value(Datum::from_usize(arr.as_ptr() as usize))],
            );
            chk_fc_varlena("int8_avg", cres, rres, &|| format!("count={count} sum={sum}"));
        }
        45..=48 => {
            // int2/int4_avg_accum(+inv) over a synthesized transarray
            let Some(count) = take_i64(r) else { return };
            let Some(sum) = take_i64(r) else { return };
            let Some(v) = take_i64(r) else { return };
            let arr = int8_transarray_image(count, sum);
            let (op, name, f): (i32, &str, PGFunction) = match sel {
                45 => (OP_INT2_AVG_ACCUM, "int2_avg_accum", nb::fc_int2_avg_accum),
                46 => (OP_INT4_AVG_ACCUM, "int4_avg_accum", nb::fc_int4_avg_accum),
                47 => (OP_INT2_AVG_ACCUM_INV, "int2_avg_accum_inv", nb::fc_int2_avg_accum_inv),
                _ => (OP_INT4_AVG_ACCUM_INV, "int4_avg_accum_inv", nb::fc_int4_avg_accum_inv),
            };
            let val = if sel == 45 || sel == 47 { v as i16 as i64 } else { v as i32 as i64 };
            let cres = c_call(op, Some(&arr), None, None, val, 0, 0.0);
            let cx = mcx::MemoryContext::new("numericfam");
            // copy arm (no agg context): C copies too (AggCheckCallContext
            // is stubbed unreachable — C's copy branch is the compiled one).
            let rarr = arr.clone();
            let arg1 = if sel == 45 || sel == 47 {
                NullableDatum::value(Datum::from_i16(val as i16))
            } else {
                NullableDatum::value(Datum::from_i32(val as i32))
            };
            let rres = fc_call(
                f,
                Some(cx.mcx()),
                [NullableDatum::value(Datum::from_usize(rarr.as_ptr() as usize)), arg1],
            );
            chk_fc_varlena(name, cres, rres, &|| {
                format!("count={count} sum={sum} val={val}")
            });
        }
        49 => {
            // pgrust-only keypack: byte-identical round-trip (the pack-side
            // contract) + C numeric_eq as the EQUALITY ORACLE
            // (pack(a)==pack(b) ⇔ eq(a,b) whenever both pack).
            let Some((a, ..)) = take_operand(r) else { return };
            let Some((b, ..)) = take_operand(r) else { return };
            const MANT_MAX: u64 = (1u64 << 55) - 1; /* nodeagg width-8 bound */
            for img in [&a, &b] {
                // out-of-contract budget must be fenced, never wrap (clamp
                // added after this target found the m-as-i64 wrap)
                if let Some(key) = adt_numeric::numeric_key_pack(img.num(), u64::MAX) {
                    let back = adt_numeric::numeric_key_unpack(key)
                        .expect("numeric_key_unpack of a packed key");
                    assert!(
                        back.as_bytes() == img.as_bytes(),
                        "numeric_key_pack(u64::MAX) BYTE-ROUNDTRIP failure img={:02x?} -> {:02x?}",
                        img.as_bytes(),
                        back.as_bytes()
                    );
                }
                if let Some(key) = adt_numeric::numeric_key_pack(img.num(), MANT_MAX) {
                    let back = adt_numeric::numeric_key_unpack(key)
                        .expect("numeric_key_unpack of a packed key");
                    assert!(
                        back.as_bytes() == img.as_bytes(),
                        "numeric_key_pack BYTE-ROUNDTRIP failure img={:02x?} -> {:02x?}",
                        img.as_bytes(),
                        back.as_bytes()
                    );
                }
            }
            if let (Some(ka), Some(kb)) = (
                adt_numeric::numeric_key_pack(a.num(), MANT_MAX),
                adt_numeric::numeric_key_pack(b.num(), MANT_MAX),
            ) {
                let cres = c_call(OP_EQ, Some(a.as_bytes()), Some(b.as_bytes()), None, 0, 0, 0.0);
                let ceq = match cres {
                    CRes::Ok(_, s) => s != 0,
                    _ => panic!("numeric_eq: unexpected C verdict"),
                };
                assert!(
                    (ka == kb) == ceq,
                    "numeric_key_pack EQ-ORACLE DIVERGENCE a={:02x?} b={:02x?}: C eq={ceq} keys eq={}",
                    a.as_bytes(),
                    b.as_bytes(),
                    ka == kb
                );
            }
            // numeric_fast_cmp vs the same oracle
            let cres = c_call(OP_CMP, Some(a.as_bytes()), Some(b.as_bytes()), None, 0, 0, 0.0);
            if let CRes::Ok(_, s) = cres {
                let ccmp = s as u32 as i32;
                let rcmp = adt_numeric::sortsupport::numeric_fast_cmp(a.payload(), b.payload());
                assert!(
                    ccmp == rcmp,
                    "numeric_fast_cmp DIVERGENCE a={:02x?} b={:02x?}: C={ccmp} Rust={rcmp}",
                    a.as_bytes(),
                    b.as_bytes()
                );
            }
        }
        _ => {
            // remaining selector space re-rolls the dense arms
            let mut d2 = Vec::with_capacity(data.len());
            d2.push(sel % 50);
            d2.extend_from_slice(rest);
            if d2.len() < data.len() || sel >= 50 {
                numeric_ops_diff(&d2);
            }
        }
    }
}

/// The _int8 {count,sum} transarray varlena image (1-D, no nulls,
/// elemtype INT8OID) both sides consume — layout matches C's
/// construct_array output for the int aggregate transtypes.
fn int8_transarray_image(count: i64, sum: i64) -> Vec<u8> {
    const INT8OID: u32 = 20;
    let mut v = Vec::with_capacity(40);
    v.extend_from_slice(&(40u32 << 2).to_ne_bytes()); /* vl_len_ */
    v.extend_from_slice(&1i32.to_ne_bytes()); /* ndim */
    v.extend_from_slice(&0i32.to_ne_bytes()); /* dataoffset (no nulls) */
    v.extend_from_slice(&INT8OID.to_ne_bytes()); /* elemtype */
    v.extend_from_slice(&2i32.to_ne_bytes()); /* dims[0] */
    v.extend_from_slice(&1i32.to_ne_bytes()); /* lbound[0] */
    v.extend_from_slice(&count.to_ne_bytes());
    v.extend_from_slice(&sum.to_ne_bytes());
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_in(s: &str, typmod: i32) {
        let mut d = vec![0u8];
        d.extend_from_slice(&typmod.to_be_bytes());
        d.extend_from_slice(s.as_bytes());
        numeric_io_diff(&d);
    }

    fn operand(signsel: u8, weight: i16, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut v = vec![digits.len() as u8];
        v.extend_from_slice(&weight.to_be_bytes());
        v.push(signsel);
        v.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    fn ops(sel: u8, parts: &[&[u8]]) {
        let mut d = vec![sel];
        for p in parts {
            d.extend_from_slice(p);
        }
        numeric_ops_diff(&d);
    }

    #[test]
    fn smoke_io_in_out_send() {
        for s in [
            "123.45", "0", "-0.001", "NaN", "Infinity", "-inf", "1e10", "1e-100",
            "  42  ", "0x1f", "0b101", "0o17", "1_000_000", "abc", "1..2", "9e99999",
            "1e1000", "-9999999999999999999999999.999999",
        ] {
            io_in(s, -1);
        }
        // typmod-constrained
        io_in("123.456", ((10 << 16) | 3) + 4 + 0x10000 * 0); // arbitrary packed typmod
        io_in("123.456", (7 << 16) | (3 & 0xffff)); // precision 7 scale ~3 region
    }

    #[test]
    fn smoke_io_recv_raw() {
        // sel 1, typmod -1, then raw wire bytes: valid 1-digit numeric
        let mut d = vec![1u8];
        d.extend_from_slice(&(-1i32).to_be_bytes());
        for w in [0u16, 0, 0x0000, 0, 42] {
            d.extend_from_slice(&w.to_be_bytes());
        }
        // fix ndigits=1
        d[5] = 0;
        d[6] = 1;
        numeric_io_diff(&d);
        // truncated wire (protocol violation both sides)
        numeric_io_diff(&[1, 255, 255, 255, 255, 0, 1, 0]);
        // invalid sign
        let mut d = vec![1u8];
        d.extend_from_slice(&(-1i32).to_be_bytes());
        for w in [1u16, 0, 0x1234, 3, 42] {
            d.extend_from_slice(&w.to_be_bytes());
        }
        numeric_io_diff(&d);
    }

    #[test]
    fn smoke_ops_arith_cmp_math() {
        let a = operand(0, 1, 2, &[123, 4500]);
        let b = operand(1, 0, 0, &[7]);
        let nan = operand(2, 0, 0, &[]);
        let pinf = operand(3, 0, 0, &[]);
        for sel in [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] {
            ops(sel, &[&a, &b]);
            ops(sel, &[&nan, &b]);
            ops(sel, &[&pinf, &nan]);
        }
        for sel in [11u8, 12, 13, 14, 17, 18, 19, 20, 21, 22, 23, 24, 25] {
            ops(sel, &[&a]);
            ops(sel, &[&nan]);
        }
        // round/trunc take a leading i32 scale
        for sel in [15u8, 16] {
            let mut d = vec![sel];
            d.extend_from_slice(&1i32.to_be_bytes());
            d.extend_from_slice(&a);
            numeric_ops_diff(&d);
        }
        // log/power (b positive)
        let two = operand(0, 0, 0, &[2]);
        ops(26, &[&a, &two]);
        ops(27, &[&two, &two]);
        ops(26, &[&b, &two]); // log of negative -> error both sides
        // div by zero
        let zero = operand(0, 0, 0, &[]);
        ops(3, &[&a, &zero]);
        ops(5, &[&a, &zero]);
    }

    #[test]
    fn smoke_ops_conv_hash_agg() {
        let a = operand(0, 1, 2, &[123, 4500]);
        let big = operand(0, 10, 0, &[9999, 9999, 9999]);
        for sel in [31u8, 32, 33, 34, 35, 41] {
            ops(sel, &[&a]);
            ops(sel, &[&big]);
        }
        for sel in [36u8, 37, 38] {
            let mut d = vec![sel];
            d.extend_from_slice(&(-123456789i64).to_be_bytes());
            numeric_ops_diff(&d);
        }
        // float -> numeric
        for (sel, bits) in [(39u8, (1.5f32).to_bits() as u64), (40, (1.5f64).to_bits())] {
            let mut d = vec![sel];
            d.extend_from_slice(&(bits as i64).to_be_bytes());
            numeric_ops_diff(&d);
        }
        // hash_ext with seed
        let mut d = vec![42u8];
        d.extend_from_slice(&99i64.to_be_bytes());
        d.extend_from_slice(&a);
        numeric_ops_diff(&d);
        // factorial
        let mut d = vec![28u8];
        d.extend_from_slice(&100i64.to_be_bytes());
        numeric_ops_diff(&d);
        // width_bucket
        let mut d = vec![29u8];
        d.extend_from_slice(&5i32.to_be_bytes());
        d.extend_from_slice(&a);
        d.extend_from_slice(&operand(0, 0, 0, &[1]));
        d.extend_from_slice(&operand(0, 1, 0, &[2]));
        numeric_ops_diff(&d);
        // in_range
        let mut d = vec![30u8, 3];
        d.extend_from_slice(&a);
        d.extend_from_slice(&operand(0, 0, 0, &[5]));
        d.extend_from_slice(&operand(0, 0, 0, &[2]));
        numeric_ops_diff(&d);
        // int8_sum null lattice
        for flags in 0u8..4 {
            let mut d = vec![43u8, flags];
            d.extend_from_slice(&77i64.to_be_bytes());
            d.extend_from_slice(&a);
            numeric_ops_diff(&d);
        }
        // int8_avg + avg_accum family
        for sel in 44u8..=48 {
            let mut d = vec![sel];
            d.extend_from_slice(&3i64.to_be_bytes());
            d.extend_from_slice(&1000i64.to_be_bytes());
            d.extend_from_slice(&7i64.to_be_bytes());
            numeric_ops_diff(&d);
        }
        // int8_avg count=0 (NULL both sides)
        let mut d = vec![44u8];
        d.extend_from_slice(&0i64.to_be_bytes());
        d.extend_from_slice(&0i64.to_be_bytes());
        numeric_ops_diff(&d);
        // keypack arm
        ops(49, &[&a, &a]);
        ops(49, &[&a, &operand(0, 0, 0, &[7])]);
        // typmodin + out_sci (io target sel 3)
        let mut d = vec![3u8, 2];
        d.extend_from_slice(&10i32.to_be_bytes());
        d.extend_from_slice(&3i32.to_be_bytes());
        d.extend_from_slice(&2i32.to_be_bytes()); // rscale
        d.extend_from_slice(&a);
        numeric_io_diff(&d);
    }
}
