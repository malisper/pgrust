//! quote_diff: differential fuzz driver — shipped Rust `adt_quote` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_quote_io.c). Crate under test: crates/backend/utils/adt/quote.
//!
//! Comparison planes: value bytes, error-verdict (both sides infallible on
//! the compute path — no vendored body can ereport; Rust `Err` = allocation
//! failure only, asserted unreachable at fuzz sizes), and the fc-wrapper
//! plane (wrapper == core over a native LocalFcinfo frame).
//!
//! Input layout: [selector][payload]; selector % 3 picks the arm:
//!   0 quote_ident  (oid 1282, C: quote.c -> ruleutils.c quote_identifier)
//!     payload = [guc byte][ident bytes]; guc&1 drives quote_all_identifiers
//!     on BOTH sides (GUC as input plane, not a carve). NUL-free (PG text
//!     invariant; C side is cstring), len cap 1024.
//!   1 quote_literal  (oid 1283, C: quote.c) — payload = raw text bytes,
//!     embedded NUL in-domain (byte-oriented core), len cap 1024.
//!   2 quote_nullable  (oid 1289, C: quote.c) — payload = [flag][text bytes];
//!     flag&1 = SQL NULL arm (payload ignored, both sides yield "NULL").
//!
//! SKIPPED rows: none — the crate has exactly the three catalog functions
//! (1285/1290 are SQL-language variants, out of the crate). Non-catalog core
//! `quote_identifier`/`QuotedIdent` is exercised by arm 0 through both the
//! plain and quoted return shapes.

use core::ffi::c_char;
use std::ffi::CString;

use datum::{Datum, NullableDatum};
use types_error::PgResult;
use types_fmgr::{LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    fn pg_diff_quote_ident(ident: *const c_char, quote_all: i32, out: *mut u8) -> usize;
    fn pg_diff_quote_literal(src: *const u8, len: usize, dst: *mut u8) -> usize;
    fn pg_diff_quote_nullable(isnull: i32, src: *const u8, len: usize, dst: *mut u8) -> usize;
}

const MAX_LEN: usize = 1024;

// GUC seam: the fuzz binary has no server, so the harness owns the
// quote_all_identifiers accessor (same pattern as the crate's own tests).
// AtomicBool storage; the C oracle side takes the bit per-call, so both
// sides read the identical value each exec.
fn install_guc() {
    use std::cell::Cell;
    use std::sync::Once;
    thread_local! {
        // Thread-local like the C oracle's TLS bool, so parallel test
        // threads can't race each other's GUC bit.
        static QAI: Cell<bool> = const { Cell::new(false) };
    }
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::vars::quote_all_identifiers.install_if_absent(guc_tables::GucVarAccessors {
            get: || QAI.with(Cell::get),
            set: |v| QAI.with(|c| c.set(v)),
        });
    });
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani; verbatim from uuid_diff.rs).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over the given (possibly-null) args.
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [NullableDatum; N],
) -> PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args = args;
    f(None, &mut fcinfo)
}

/// Varlena result readback (text payload bytes).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// text/varlena arg construction: inline 4B-uncompressed header + body
/// (the shipped set_varsize_4b_word encoding; body is capped by MAX_LEN so
/// the length always fits). Verbatim pattern from name_diff.rs.
fn text_image(body: &[u8]) -> Vec<u8> {
    let len = (body.len() + 4) as u32;
    #[cfg(target_endian = "little")]
    let word = len << 2;
    #[cfg(target_endian = "big")]
    let word = len & 0x3FFF_FFFF;
    let mut img = Vec::with_capacity(body.len() + 4);
    img.extend_from_slice(&word.to_ne_bytes());
    img.extend_from_slice(body);
    img
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn quote_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 3 {
        0 => quote_ident_diff(payload),
        1 => quote_literal_diff(payload),
        _ => quote_nullable_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: quote_ident (oid 1282; C: quote.c wrapper over ruleutils.c
// quote_identifier). GUC quote_all_identifiers is byte 0 of the payload.
// ---------------------------------------------------------------------------

fn quote_ident_diff(payload: &[u8]) {
    let Some((&guc, ident)) = payload.split_first() else {
        return;
    };
    if ident.len() > MAX_LEN || ident.contains(&0) {
        // PG text never contains NUL (rejected at input); C oracle is a
        // cstring core, so NUL-bearing idents are out of the shared domain.
        return;
    }
    let quote_all = (guc & 1) != 0;

    // C oracle.
    let cs = CString::new(ident).unwrap();
    let mut cbuf = vec![0u8; ident.len() * 2 + 3];
    let clen =
        unsafe { pg_diff_quote_ident(cs.as_ptr(), quote_all as i32, cbuf.as_mut_ptr()) };
    let cval = &cbuf[..clen];

    // Shipped Rust: GUC set to the same value (input plane, both sides).
    install_guc();
    guc_tables::vars::quote_all_identifiers.write(quote_all);
    let cx = mcx::MemoryContext::new("quote_fuzz");
    let m = cx.mcx();

    // Core shape 1: quote_identifier (Plain-vs-Quoted enum core).
    let qi = adt_quote::quote_identifier(m, ident).expect("quote_identifier alloc at fuzz sizes");
    assert!(
        qi.as_bytes() == cval,
        "quote_identifier DIVERGENCE input={:?} quote_all={quote_all}: C={:?} Rust={:?}",
        String::from_utf8_lossy(ident),
        String::from_utf8_lossy(cval),
        String::from_utf8_lossy(qi.as_bytes())
    );

    // Core shape 2: quote_ident (text-result core the fc wrapper ships).
    let vt = adt_quote::quote_ident(m, ident).expect("quote_ident alloc at fuzz sizes");
    assert!(
        vt.data() == cval,
        "quote_ident DIVERGENCE input={:?} quote_all={quote_all}",
        String::from_utf8_lossy(ident)
    );

    // fc-wrapper plane.
    let img = text_image(ident);
    let din = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let d = fc_call::<1>(adt_quote::builtins::fc_quote_ident, m, [din])
        .expect("fc_quote_ident alloc at fuzz sizes");
    assert!(
        read_varlena_data(d) == cval,
        "fc_quote_ident vs core DIVERGENCE input={:?} quote_all={quote_all}",
        String::from_utf8_lossy(ident)
    );
}

// ---------------------------------------------------------------------------
// Arm: quote_literal (oid 1283; C source: quote.c). Byte-oriented; embedded
// NUL is in-domain.
// ---------------------------------------------------------------------------

fn quote_literal_diff(payload: &[u8]) {
    if payload.len() > MAX_LEN {
        return;
    }

    // C oracle.
    let mut cbuf = vec![0u8; payload.len() * 2 + 3];
    let clen = unsafe { pg_diff_quote_literal(payload.as_ptr(), payload.len(), cbuf.as_mut_ptr()) };
    let cval = &cbuf[..clen];

    // Shipped Rust core.
    let cx = mcx::MemoryContext::new("quote_fuzz");
    let m = cx.mcx();
    let vt = adt_quote::quote_literal(m, payload).expect("quote_literal alloc at fuzz sizes");
    assert!(
        vt.data() == cval,
        "quote_literal DIVERGENCE input={:?}: C={:?} Rust={:?}",
        payload,
        String::from_utf8_lossy(cval),
        String::from_utf8_lossy(vt.data())
    );

    // fc-wrapper plane.
    let img = text_image(payload);
    let din = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let d = fc_call::<1>(adt_quote::builtins::fc_quote_literal, m, [din])
        .expect("fc_quote_literal alloc at fuzz sizes");
    assert!(
        read_varlena_data(d) == cval,
        "fc_quote_literal vs core DIVERGENCE input={payload:?}"
    );
}

// ---------------------------------------------------------------------------
// Arm: quote_nullable (oid 1289; C source: quote.c). Non-strict: flag&1
// exercises the SQL NULL -> "NULL" arm through the fc wrapper (the only
// place that arm exists on the Rust side too).
// ---------------------------------------------------------------------------

fn quote_nullable_diff(payload: &[u8]) {
    let Some((&flag, body)) = payload.split_first() else {
        return;
    };
    if body.len() > MAX_LEN {
        return;
    }
    let isnull = (flag & 1) != 0;

    // C oracle.
    let mut cbuf = vec![0u8; body.len() * 2 + 4];
    let clen = unsafe {
        pg_diff_quote_nullable(isnull as i32, body.as_ptr(), body.len(), cbuf.as_mut_ptr())
    };
    let cval = &cbuf[..clen];

    // fc-wrapper: the shipped quote_nullable IS the wrapper (null dispatch +
    // quote_literal core), so the wrapper call is the primary Rust side here.
    let cx = mcx::MemoryContext::new("quote_fuzz");
    let m = cx.mcx();
    let img = text_image(body);
    let din = if isnull {
        NullableDatum::null()
    } else {
        NullableDatum::value(Datum::from_usize(img.as_ptr() as usize))
    };
    let d = fc_call::<1>(adt_quote::builtins::fc_quote_nullable, m, [din])
        .expect("fc_quote_nullable alloc at fuzz sizes");
    assert!(
        read_varlena_data(d) == cval,
        "fc_quote_nullable DIVERGENCE isnull={isnull} input={:?}: C={:?} Rust={:?}",
        body,
        String::from_utf8_lossy(cval),
        String::from_utf8_lossy(read_varlena_data(d))
    );
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/quote_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/quote_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                quote_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Per-arm smoke: known-answer checks through the full diff drivers
    /// (any C-vs-Rust disagreement asserts inside).
    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // ident: safe (plain), keyword (quoted), embedded quote, guc-forced.
        quote_diff(b"\x00\x00abc_1");
        quote_diff(b"\x00\x00select");
        quote_diff(b"\x00\x00a\"b");
        quote_diff(b"\x00\x00Abc");
        quote_diff(b"\x00\x01abc_1"); // quote_all_identifiers = on
        quote_diff(b"\x00\x00");      // empty ident -> \"\"
        // literal: plain, quote-doubling, backslash (E-prefix), NUL byte.
        quote_diff(b"\x01hello");
        quote_diff(b"\x01it's");
        quote_diff(b"\x01a\\b");
        quote_diff(b"\x01a\x00b");
        quote_diff(b"\x01");          // empty literal -> ''
        // nullable: null arm and value arm.
        quote_diff(b"\x02\x01ignored");
        quote_diff(b"\x02\x00it's");
    }

    /// Keyword-category boundary: unreserved keyword stays plain, reserved
    /// gets quoted (two-sided through the diff driver; the assert text names
    /// the arm if the C table and Rust table ever drift).
    #[test]
    fn keyword_boundary() {
        let _serial = crate::c_oracle_serial();
        // "abort" is UNRESERVED, "all" is RESERVED in 18.3 kwlist.h.
        quote_diff(b"\x00\x00abort");
        quote_diff(b"\x00\x00all");
        quote_diff(b"\x00\x00between"); // col_name keyword -> quoted
    }
}
