//! VENDOR-ARRAYRECV — differential fuzzer for the binary array receive path.
//!
//! `array_recv` (arrayfuncs.c @ 62d6c7d3df, REL_18_3) is the richest single
//! length-field surface in the array codec: an attacker-controlled binary
//! image whose header is a run of big-endian length/count words —
//!
//! ```text
//!   ndim(i32) flags(i32) element_type(oid=i32)
//!   [ dim[i](i32) lBound[i](i32) ] * ndim
//!   [ itemlen(i32) payload[itemlen] ] * nitems     (itemlen == -1 => NULL)
//! ```
//!
//! — every one of which is validated (or must be) before a single element
//! byte is trusted. EDGE2 flagged it as "the richest single length-field
//! surface" and DEFERRED it; this lane vendors it verbatim (see
//! `csrc/pg_arrayfuncs_io.c`, `pg_afx_array_recv` / `pg_afx_ReadArrayBinary`)
//! and fires the length-field edge bank against the pgrust port
//! (`arrayfuncs::array_recv`).
//!
//! BAR: pgrust accept-or-reject IDENTICAL to verbatim C, byte-for-byte on the
//! resulting image and errcode-class-identical on reject. The classic
//! array-recv CVE shape — a missing overflow check on `ndim * dims` (product
//! overflow) or on `dim + lBound` (subscript overflow) that lets a malformed
//! image drive an allocation-size overflow, panic, OOB read, or assert where
//! C cleanly rejects via `ArrayGetNItems`/`ArrayCheckBounds` — is HIGH.
//!
//! The C oracle abstracts each real SQLSTATE into a small errcode class (see
//! the `#define ERRCODE_*` block in the C file); [`class_of`] maps the pgrust
//! `PgError` sqlstate to the same classes. Message text is out of scope.
//!
//! Element codec: only the two element types with a self-contained recv proc
//! are driven — int4 (`elemsel 0`, `adt_int::int4recv`, fixed 4-byte) and
//! text (`elemsel 1`, remaining-bytes-into-varlena). int4 is the primary arm:
//! its fixed width makes `itemlen` mismatches (leftover bytes / short reads)
//! trip the exact "improper binary format" / protocol-violation edges.

use datum::Datum;
use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use stringinfo::StringInfo;
use types_core::{Oid, INT4OID, TEXTOID};
use types_error::{
    PgError, PgResult, SqlState, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_PROTOCOL_VIOLATION,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use arrayfuncs::{array_recv, ArrayIoMeta};

extern "C" {
    fn pg_diff_array_recv(
        elemsel: i32,
        wire: *const u8,
        wire_len: usize,
        typmod: i32,
        out_img: *mut *const u8,
        out_len: *mut usize,
    ) -> i32;
}

// ---------------------------------------------------------------------------
// Element codecs (must be byte-identical to the C shim's ReceiveFunctionCall).
// ---------------------------------------------------------------------------

/// text recv: copy the remaining element bytes into a plain 4-byte-header
/// varlena — identical layout to the C `pg_afx_make_text` shim.
fn build_varlena<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum> {
    let total = ::datum::VARHDRSZ + payload.len();
    let mut img = vec_with_capacity_in(mcx, total)?;
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    img.extend_from_slice(payload);
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    Ok(d)
}

fn fc_mytextrecv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut StringInfo<'_>) };
    let n = buf.len() - buf.cursor;
    let bytes = ::pqformat::pq_getmsgbytes(buf, n)?.to_vec();
    build_varlena(fcinfo.result_mcx(), &bytes)
}

fn meta_int4() -> ArrayIoMeta {
    ArrayIoMeta {
        element_type: INT4OID,
        typlen: 4,
        typbyval: true,
        typalign: b'i',
        typdelim: b',',
        typioparam: INT4OID,
    }
}
fn meta_text() -> ArrayIoMeta {
    ArrayIoMeta {
        element_type: TEXTOID,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        typdelim: b',',
        typioparam: TEXTOID,
    }
}

fn meta_for(elemsel: i32) -> ArrayIoMeta {
    if elemsel == 0 {
        meta_int4()
    } else {
        meta_text()
    }
}
fn recv_proc(elemsel: i32) -> FmgrInfo {
    if elemsel == 0 {
        // adt_int::int4recv (oid 2406), strict.
        FmgrInfo::new(adt_int::builtins::fc_int4recv, 2406, 1, true, false)
    } else {
        FmgrInfo::new(fc_mytextrecv, 46, 1, true, false)
    }
}

/// Ensure the pg_type typcache seam is populated so the 42804 element-type
/// mismatch path (which renders type names via `format_type_extended`) does
/// not fault when this driver runs on its own. Tolerant of another lane's
/// oracle having installed the seam first (double-install panics; caught).
fn init_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = std::panic::catch_unwind(|| {
            ::syscache_seams::lookup_pg_type_typcache_shape::set(|_typid| Ok(None));
        });
    });
}

/// Map the pgrust PgError sqlstate to the C oracle's wire-lane errcode class.
fn class_of(e: &PgError) -> i32 {
    let ss: SqlState = e.sqlstate();
    if ss == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        2
    } else if ss == ERRCODE_INVALID_BINARY_REPRESENTATION {
        10
    } else if ss == ERRCODE_DATATYPE_MISMATCH {
        11
    } else if ss == ERRCODE_PROTOCOL_VIOLATION {
        12
    } else {
        0 // unmapped: always a divergence against the oracle's classes
    }
}

// ---------------------------------------------------------------------------
// The differential comparison.
// ---------------------------------------------------------------------------

/// Verdict of one decode: `Ok(image bytes)` or `Err(errcode class)`.
type Verdict = Result<std::vec::Vec<u8>, i32>;

fn c_decode(elemsel: i32, wire: &[u8]) -> Verdict {
    let mut img: *const u8 = core::ptr::null();
    let mut len: usize = 0;
    let rc = unsafe {
        pg_diff_array_recv(elemsel, wire.as_ptr(), wire.len(), -1, &mut img, &mut len)
    };
    if rc != 0 {
        return Err(rc);
    }
    // Copy eagerly: the arena memory is valid only until the next pg_diff_* call.
    let bytes = unsafe { core::slice::from_raw_parts(img, len) }.to_vec();
    Ok(bytes)
}

fn rust_decode(mcx: Mcx<'_>, elemsel: i32, wire: &[u8]) -> Verdict {
    let meta = meta_for(elemsel);
    let mut proc = recv_proc(elemsel);
    let mut buf = match StringInfo::with_capacity_in(mcx, wire.len() + 1) {
        Ok(b) => b,
        Err(e) => return Err(class_of(&e)),
    };
    if let Err(e) = buf.append_bytes(wire) {
        return Err(class_of(&e));
    }
    match array_recv(mcx, &mut buf, &meta, &mut proc, -1) {
        Ok(v) => Ok(v[..].to_vec()),
        Err(e) => Err(class_of(&e)),
    }
}

/// Compare the two verdicts. `Ok(())` on agreement; `Err(reason)` names the
/// divergence. Split out from [`run_case`] so the detection-control test can
/// assert the comparator actually flags a planted divergence.
fn compare(elemsel: i32, wire: &[u8], c: &Verdict, r: &Verdict) -> Result<(), String> {
    match (c, r) {
        (Ok(ci), Ok(ri)) => {
            if ci != ri {
                return Err(format!(
                    "array_recv IMAGE DIVERGENCE elemsel={elemsel} wire={wire:02x?}\n  C  ({} bytes)={ci:02x?}\n  Rust({} bytes)={ri:02x?}",
                    ci.len(),
                    ri.len()
                ));
            }
            Ok(())
        }
        (Err(cc), Err(rc)) => {
            if cc != rc {
                return Err(format!(
                    "array_recv ERRCLASS DIVERGENCE elemsel={elemsel} wire={wire:02x?}: C class {cc} vs Rust class {rc}"
                ));
            }
            Ok(())
        }
        (Ok(ci), Err(rc)) => Err(format!(
            "array_recv VERDICT DIVERGENCE elemsel={elemsel} wire={wire:02x?}: C accepted ({} bytes) but Rust rejected (class {rc})",
            ci.len()
        )),
        (Err(cc), Ok(ri)) => Err(format!(
            "array_recv VERDICT DIVERGENCE elemsel={elemsel} wire={wire:02x?}: C rejected (class {cc}) but Rust accepted ({} bytes)",
            ri.len()
        )),
    }
}

/// Decode `wire` under both implementations and panic on any divergence.
/// The panic is the campaign's detection signal (verdict); a pgrust panic /
/// OOB / assert inside `array_recv` is caught by the process ASan/abort.
fn run_case(mcx: Mcx<'_>, elemsel: i32, wire: &[u8]) {
    let c = c_decode(elemsel, wire);
    let r = rust_decode(mcx, elemsel, wire);
    if let Err(reason) = compare(elemsel, wire, &c, &r) {
        panic!("{reason}");
    }
}

// ---------------------------------------------------------------------------
// Wire-image builder.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Wire {
    b: std::vec::Vec<u8>,
}
impl Wire {
    fn new() -> Self {
        Wire::default()
    }
    fn i32(mut self, v: i32) -> Self {
        self.b.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn u32(mut self, v: u32) -> Self {
        self.b.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn raw(mut self, v: &[u8]) -> Self {
        self.b.extend_from_slice(v);
        self
    }
    fn done(self) -> std::vec::Vec<u8> {
        self.b
    }
}

/// Header for `ndim` dims: ndim, flags, element_type, then (dim,lb) pairs.
fn header(ndim: i32, flags: i32, elemtype: u32, dims: &[(i32, i32)]) -> Wire {
    let mut w = Wire::new().i32(ndim).i32(flags).u32(elemtype);
    for &(d, l) in dims {
        w = w.i32(d).i32(l);
    }
    w
}

/// A single int4 element (itemlen = 4, payload = big-endian value).
fn int4_elem(w: Wire, v: i32) -> Wire {
    w.i32(4).i32(v)
}

// ---------------------------------------------------------------------------
// Edge bank — the enumerated length/dimension/count corner cases.
// ---------------------------------------------------------------------------

const MAX_ARRAY_SIZE: i32 = (1 << 28) - 1; // MaxAllocSize/2-ish cap used by ArrayGetNItems

/// Every hand-built length-field edge case. Each entry is `(elemsel, wire)`.
fn edge_bank() -> std::vec::Vec<(i32, std::vec::Vec<u8>)> {
    let mut v: std::vec::Vec<(i32, std::vec::Vec<u8>)> = std::vec::Vec::new();
    let int4 = INT4OID; // 23
    let text = TEXTOID; // 25

    // ---- ndim edge words (INT_MIN/MAX/negative/huge/0) ----
    for &nd in &[
        i32::MIN,
        i32::MIN + 1,
        -1,
        0,
        1,
        6,      // MAXDIM
        7,      // MAXDIM + 1
        1000,
        i32::MAX,
    ] {
        // ndim alone, and ndim followed by a plausible 1-D body.
        v.push((0, Wire::new().i32(nd).done()));
        let body = header(nd, 0, int4, &[(1, 1)]);
        v.push((0, int4_elem(body, 42).done()));
    }

    // ---- flags edge words ----
    for &fl in &[0, 1, 2, -1, i32::MIN, i32::MAX] {
        let body = header(1, fl, int4, &[(1, 1)]);
        v.push((0, int4_elem(body, 7).done()));
    }

    // ---- element_type mismatch (both-builtin => 42804; non-builtin => carry) ----
    for &et in &[int4, text, 0u32, 16u32, 9999u32, 10000u32, 20000u32, u32::MAX] {
        let body = header(1, 0, et, &[(1, 1)]);
        v.push((0, int4_elem(body, 5).done()));
    }

    // ---- dims / lbound overflow (dim*lbound wrap, dim+lb subscript overflow) ----
    // 1-D: dim = INT_MAX, lBound = INT_MAX -> dim+lb overflow (ArrayCheckBounds).
    v.push((0, header(1, 0, int4, &[(i32::MAX, i32::MAX)]).done()));
    // 1-D: dim = INT_MAX, lBound = 2 -> subscript overflow.
    v.push((0, header(1, 0, int4, &[(i32::MAX, 2)]).done()));
    // 1-D negative dim -> ArrayGetNItems "negative dimension" (54000).
    v.push((0, header(1, 0, int4, &[(-1, 1)]).done()));
    v.push((0, header(1, 0, int4, &[(i32::MIN, 1)]).done()));
    // 2-D product overflow: 65536 * 65536 = 2^32 wraps int32.
    v.push((0, header(2, 0, int4, &[(65536, 1), (65536, 1)]).done()));
    // 2-D product overflow: big * big.
    v.push((0, header(2, 0, int4, &[(i32::MAX, 1), (2, 1)]).done()));
    // huge single dim beyond MaxArraySize -> "array size exceeds the maximum".
    v.push((0, header(1, 0, int4, &[(MAX_ARRAY_SIZE + 1, 1)]).done()));
    v.push((0, header(1, 0, int4, &[(i32::MAX, 1)]).done()));
    // lBound extremes with small dim.
    for &lb in &[i32::MIN, i32::MAX, i32::MAX - 1, 0, -1] {
        let body = header(1, 0, int4, &[(1, lb)]);
        v.push((0, int4_elem(body, 1).done()));
    }

    // ---- element-count vs payload mismatch ----
    // Claims dim=3 but supplies only one element -> itemlen read underflows.
    v.push((0, int4_elem(header(1, 0, int4, &[(3, 1)]), 9).done()));
    // Claims dim=1 but supplies two elements -> trailing bytes ignored (accept).
    v.push((0, int4_elem(int4_elem(header(1, 0, int4, &[(1, 1)]), 1), 2).done()));
    // dim=[2] 1-D with exactly two elements (accept, well-formed).
    v.push((0, int4_elem(int4_elem(header(1, 0, int4, &[(2, 1)]), 1), 2).done()));

    // ---- itemlen edge words ----
    for &il in &[-2, -1, 0, 1, 3, 4, 5, 8, i32::MAX, i32::MIN] {
        // 1-D, one element, header claims dim=1; itemlen is the fuzzed word.
        let w = header(1, 0, int4, &[(1, 1)]).i32(il).raw(b"\x00\x00\x00\x2a");
        v.push((0, w.done()));
    }

    // ---- NULL-bitmap edges (itemlen == -1) ----
    // Single NULL.
    v.push((0, header(1, 0, int4, &[(1, 1)]).i32(-1).done()));
    // Mixed NULL / non-NULL across a byte boundary (dim=9).
    {
        let mut w = header(1, 0, int4, &[(9, 1)]);
        for i in 0..9i32 {
            if i % 3 == 0 {
                w = w.i32(-1); // NULL
            } else {
                w = int4_elem(w, i);
            }
        }
        v.push((0, w.done()));
    }
    // All-NULL.
    {
        let mut w = header(1, 0, int4, &[(4, 1)]);
        for _ in 0..4 {
            w = w.i32(-1);
        }
        v.push((0, w.done()));
    }

    // ---- empty / truncated payloads ----
    v.push((0, std::vec::Vec::new())); // empty message
    for n in 1..20usize {
        // A well-formed 1-D single-int4 image truncated at every prefix length.
        let full = int4_elem(header(1, 0, int4, &[(1, 1)]), 77).done();
        if n < full.len() {
            v.push((0, full[..n].to_vec()));
        }
    }
    // Header claims a big dim but the element payload is truncated mid-value.
    v.push((0, header(1, 0, int4, &[(1, 1)]).i32(4).raw(b"\x00\x00").done()));

    // ---- ndim == 0 (empty array; must accept and match construct_empty_array) ----
    v.push((0, Wire::new().i32(0).i32(0).u32(int4).done()));
    v.push((1, Wire::new().i32(0).i32(0).u32(text).done()));

    // ---- text arm: variable-length element / leftover-byte / empty-elem ----
    // Well-formed 1-D text array {"ab"}.
    v.push((1, header(1, 0, text, &[(1, 1)]).i32(2).raw(b"ab").done()));
    // Zero-length text element.
    v.push((1, header(1, 0, text, &[(1, 1)]).i32(0).done()));
    // itemlen exceeds remaining -> reject.
    v.push((1, header(1, 0, text, &[(1, 1)]).i32(100).raw(b"ab").done()));
    // 2-element text with a NULL.
    v.push((
        1,
        header(1, 0, text, &[(2, 1)])
            .i32(1)
            .raw(b"x")
            .i32(-1)
            .done(),
    ));

    v
}

// ---------------------------------------------------------------------------
// Structured sweep — a broad cartesian product driving the header fields.
// ---------------------------------------------------------------------------

/// Yields tens of thousands of structured wire images. Every image is a
/// header with fuzzed ndim/flags/dims/lbounds followed by a fuzzed run of
/// element words, so both the reject paths (overflow, bad flags, short data)
/// and the accept paths (well-formed small arrays) are exercised.
fn structured_sweep(mut run: impl FnMut(i32, &[u8])) -> usize {
    let ndims = [0i32, 1, 2, 3, 4, 6, 7];
    let dimset = [
        0i32,
        1,
        2,
        3,
        4,
        -1,
        255,
        65536,
        i32::MAX,
        i32::MIN,
        MAX_ARRAY_SIZE + 1,
    ];
    let lbset = [1i32, 0, -1, 2, i32::MAX, i32::MIN, i32::MAX - 1];
    let flagset = [0i32, 1, 2, -1];
    let itemlens = [4i32, -1, 0, 3, 5, 8, i32::MAX, -2];
    let mut n = 0usize;

    for &elemsel in &[0i32, 1] {
        let etype = if elemsel == 0 { INT4OID } else { TEXTOID };
        for &nd in &ndims {
            for &d0 in &dimset {
                for &l0 in &lbset {
                    for &fl in &flagset {
                        for &il in &itemlens {
                            // Build a header. For ndim>=1 the first dim/lb are
                            // the fuzzed words; any further dims are pinned to
                            // 1 so the product stays small unless d0 is huge.
                            let mut dims: std::vec::Vec<(i32, i32)> = std::vec::Vec::new();
                            for i in 0..nd.clamp(0, 6) as usize {
                                if i == 0 {
                                    dims.push((d0, l0));
                                } else {
                                    dims.push((1, 1));
                                }
                            }
                            let mut w = header(nd, fl, etype, &dims);
                            // Append up to a few element words: itemlen then a
                            // 4-byte payload (enough for int4; text reads it as
                            // its bytes). This makes itemlen-vs-remaining and
                            // leftover-byte edges reachable.
                            for k in 0..3 {
                                w = w.i32(il).i32(0x0102_0300 + k);
                            }
                            run(elemsel, &w.done());
                            n += 1;
                        }
                    }
                }
            }
        }
    }
    n
}

// ---------------------------------------------------------------------------
// Fuzz entry.
// ---------------------------------------------------------------------------

/// libFuzzer / Antithesis entry. The first byte selects the element type; the
/// remainder is fed VERBATIM as the wire image — the most direct mapping of
/// fuzzer bytes onto the length-field surface — and also once through the
/// structured builder as a header seed.
pub fn arrayrecv_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial();
    init_seams();
    let Some((&sel, wire)) = data.split_first() else {
        return;
    };
    let elemsel = (sel % 2) as i32;
    let ctx = MemoryContext::new_bump("arrayrecv_diff");
    let mcx = ctx.mcx();

    // 1. Raw bytes as a wire image.
    run_case(mcx, elemsel, wire);

    // 2. Interpret the leading bytes as header field words, so a fuzzer that
    //    has not yet learned the frame still reaches the dims/count checks.
    if wire.len() >= 4 {
        let rd = |o: usize| -> i32 {
            let mut b = [0u8; 4];
            for (i, s) in b.iter_mut().enumerate() {
                *s = wire.get(o + i).copied().unwrap_or(0);
            }
            i32::from_be_bytes(b)
        };
        let nd = rd(0);
        let d0 = rd(4);
        let l0 = rd(8);
        let etype = if elemsel == 0 { INT4OID } else { TEXTOID };
        let w = int4_elem(header(nd, 0, etype, &[(d0, l0)]), 1);
        run_case(mcx, elemsel, &w.done());
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// DETECTION CONTROL: prove the comparator flags a planted divergence.
    /// A verdict pair that disagrees MUST yield `Err`; an agreeing pair MUST
    /// yield `Ok`. Guards against a vacuous (always-green) oracle.
    #[test]
    fn detection_control_catches_planted_divergence() {
        let wire = &[0u8, 0, 0, 1];
        // Verdict mismatch: C accepts, Rust rejects.
        let planted_verdict = compare(0, wire, &Ok(vec![1, 2, 3]), &Err(10));
        assert!(planted_verdict.is_err(), "verdict divergence not caught");
        // Errclass mismatch.
        let planted_class = compare(0, wire, &Err(2), &Err(10));
        assert!(planted_class.is_err(), "errclass divergence not caught");
        // Image mismatch.
        let planted_img = compare(0, wire, &Ok(vec![1, 2, 3]), &Ok(vec![1, 2, 4]));
        assert!(planted_img.is_err(), "image divergence not caught");
        // Agreement must NOT be flagged.
        assert!(compare(0, wire, &Err(10), &Err(10)).is_ok());
        assert!(compare(0, wire, &Ok(vec![9, 9]), &Ok(vec![9, 9])).is_ok());
    }

    /// EXECUTION WITNESS: prove BOTH implementations actually ran on a
    /// canonical malformed image (INT_MIN ndim). Both must reject with the
    /// invalid-binary-representation class (10 / 22P03), and the shared
    /// comparison must agree — a vacuous pass would leave one side silent.
    #[test]
    fn execution_witness_both_sides_reject_int_min_ndim() {
        let _serial = crate::c_oracle_serial();
        init_seams();
        let ctx = MemoryContext::new_bump("witness");
        let mcx = ctx.mcx();
        let wire = Wire::new().i32(i32::MIN).done();
        let c = c_decode(0, &wire);
        let r = rust_decode(mcx, 0, &wire);
        assert_eq!(c, Err(10), "C oracle did not reject INT_MIN ndim as 22P03");
        assert_eq!(r, Err(10), "pgrust did not reject INT_MIN ndim as 22P03");
        compare(0, &wire, &c, &r).expect("witness case must agree");
    }

    /// A well-formed 1-D int4 array must be ACCEPTED by both, with a
    /// byte-identical image (proves the accept path, not just rejects).
    #[test]
    fn well_formed_int4_roundtrip_agrees() {
        let _serial = crate::c_oracle_serial();
        init_seams();
        let ctx = MemoryContext::new_bump("wf");
        let mcx = ctx.mcx();
        let w = int4_elem(int4_elem(int4_elem(header(1, 0, INT4OID, &[(3, 1)]), 10), 20), 30);
        let wire = w.done();
        let c = c_decode(0, &wire);
        let r = rust_decode(mcx, 0, &wire);
        assert!(matches!(c, Ok(_)), "C rejected a well-formed array: {c:?}");
        compare(0, &wire, &c, &r).expect("well-formed image must agree");
    }

    /// Fire the entire hand-built length-field edge bank.
    #[test]
    fn edge_bank_fires_clean() {
        let _serial = crate::c_oracle_serial();
        init_seams();
        let bank = edge_bank();
        assert!(bank.len() >= 60, "edge bank too small: {}", bank.len());
        for (elemsel, wire) in &bank {
            let ctx = MemoryContext::new_bump("edge");
            run_case(ctx.mcx(), *elemsel, wire);
        }
    }

    /// Fire the broad structured sweep (tens of thousands of images).
    #[test]
    fn structured_sweep_fires_clean() {
        let _serial = crate::c_oracle_serial();
        init_seams();
        let n = structured_sweep(|elemsel, wire| {
            let ctx = MemoryContext::new_bump("sweep");
            run_case(ctx.mcx(), elemsel, wire);
        });
        assert!(n >= 20_000, "structured sweep too small: {n}");
    }

    /// The fuzz entry itself must survive arbitrary bytes without a
    /// non-divergence panic (link/shim smoke).
    #[test]
    fn fuzz_entry_smoke() {
        let _serial = crate::c_oracle_serial();
        let payloads: [&[u8]; 6] = [
            &[],
            &[0x00],
            &[0x00, 0xff, 0xff, 0xff, 0xff],
            &[0x01, 0x80, 0, 0, 0, 0, 0, 0, 1],
            &[0xff; 64],
            &[0x00, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 42],
        ];
        for p in payloads {
            arrayrecv_diff(p);
        }
    }
}
