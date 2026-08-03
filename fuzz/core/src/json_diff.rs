//! json_diff: differential fuzz driver — shipped Rust `adt_json` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_json_io.c + csrc/jsonfam/jsonapi.c + csrc/jsonfam/stringinfo.c,
//! all VERBATIM TUs). Crate under test: crates/backend/utils/adt/json.
//!
//! Comparison planes (float_in_diff conventions): value bytes, error-verdict,
//! errcode/sqlstate (exact MAKE_SQLSTATE int), SQL-NULL-ness, plus the
//! fc-wrapper plane (wrapper == core over a native LocalFcinfo frame) and the
//! soft-error (ErrorSaveContext) plane on json_in.
//!
//! Environment pins (identical on both sides, documented in pg_json_io.c):
//!   - server encoding pinned to UTF8 (SetDatabaseEncoding(PG_UTF8) per exec;
//!     C oracle GetDatabaseEncoding shim returns PG_UTF8). The non-UTF8
//!     pg_unicode_to_server arm is a recorded encoding-carve.
//!   - text/cstring payloads are NUL-free (SQL text/cstring invariant;
//!     inputs with interior NUL are outside the SQL-reachable domain).
//!     escape_json bytes keep NUL in-domain (byte-oriented core).
//!   - input length capped (MAX_LEN) so neither side's stack guard fires.
//!
//! Input layout: [selector][payload]; selector % 14 picks the arm:
//!    0 json_in (321): payload = json text; hard, soft (escontext) and fc
//!      planes.
//!    1 json_typeof (3968): payload = json text.
//!    2 json_array_length (3956): payload = json text.
//!    3 json_strip_nulls (3261): payload = [flag][json]; flag&1 =
//!      strip_in_arrays.
//!    4 json_validate (IS JSON core; no pg_proc oid): payload =
//!      [flag][json]; flag&1 = check_unique_keys, flag&2 = throw_error.
//!    5/6 json_object_field(_text) (3947/3948): [klen][key][json].
//!    7/8 json_array_element(_text) (3949/3950): [i32 LE idx][json]
//!      (negative idx exercises json_count_array_elements).
//!    9/10 json_extract_path(_text) (3951/3953): [npath%5] then npath x
//!      [elen][bytes], rest = json. Exercises get_path_all + path_index.
//!    11 json_object (3202): [ndim%4][d0%5][d1%5][d2%3] + elements
//!      ([flag][len][bytes] each; flag&1 = SQL NULL element).
//!    12 json_object_two_arg (3203): [nkdim%2+flags][kcount%6][vcount%6] +
//!      elements for keys then values.
//!    13 escape_json (escape_json_with_len; helper, no oid): raw bytes,
//!      NUL in-domain.
//!
//! SKIPPED rows (excepted in phase1-routes.tsv, never fuzzed here):
//!   - aggs.rs transfns/finalfns (aggcontext-lived state; agg-state carve),
//!   - srfs.rs SRFs (funcapi/tupdesc/typcache machinery; SRF-engine carve),
//!   - tojson.rs catalog/fmgr paths (json_categorize_type, output-function
//!     seams; catalog carve) incl. to_json/array_to_json/row_to_json/
//!     json_build_object/array non-noargs,
//!   - json_recv/json_send (pqformat wire plumbing; 322/324/3199/3201 are
//!     proved in the ledger).

use datum::{Datum, NullableDatum};
use stringinfo::StringInfo;
use types_error::{PgResult, SoftErrorContext, SqlState};
use types_fmgr::{LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    fn pg_diff_json_in(s: *const u8, len: usize, out: *mut *const u8, outlen: *mut usize) -> i32;
    fn pg_diff_json_typeof(
        s: *const u8,
        len: usize,
        out: *mut *const u8,
        outlen: *mut usize,
    ) -> i32;
    fn pg_diff_json_array_length(s: *const u8, len: usize, count: *mut i32) -> i32;
    fn pg_diff_json_strip_nulls(
        s: *const u8,
        len: usize,
        strip_in_arrays: i32,
        out: *mut *const u8,
        outlen: *mut usize,
    ) -> i32;
    fn pg_diff_json_validate(
        s: *const u8,
        len: usize,
        check_unique: i32,
        throw_error: i32,
        ok: *mut i32,
    ) -> i32;
    fn pg_diff_json_get_field(
        s: *const u8,
        len: usize,
        key: *const u8,
        keylen: usize,
        as_text: i32,
        out: *mut *const u8,
        outlen: *mut usize,
        isnull: *mut i32,
    ) -> i32;
    fn pg_diff_json_get_element(
        s: *const u8,
        len: usize,
        idx: i32,
        as_text: i32,
        out: *mut *const u8,
        outlen: *mut usize,
        isnull: *mut i32,
    ) -> i32;
    fn pg_diff_json_get_path(
        s: *const u8,
        len: usize,
        npath: i32,
        elems: *const *const u8,
        elemlens: *const usize,
        elemnulls: *const u8,
        as_text: i32,
        out: *mut *const u8,
        outlen: *mut usize,
        isnull: *mut i32,
    ) -> i32;
    fn pg_diff_json_object(
        ndim: i32,
        dims: *const i32,
        nelems: i32,
        elems: *const *const u8,
        lens: *const usize,
        nulls: *const u8,
        out: *mut *const u8,
        outlen: *mut usize,
    ) -> i32;
    fn pg_diff_json_object_two_arg(
        nkdim: i32,
        kdims: *const i32,
        nkelems: i32,
        kelems: *const *const u8,
        klens: *const usize,
        knulls: *const u8,
        nvdim: i32,
        vdims: *const i32,
        nvelems: i32,
        velems: *const *const u8,
        vlens: *const usize,
        vnulls: *const u8,
        out: *mut *const u8,
        outlen: *mut usize,
    ) -> i32;
    fn pg_diff_escape_json(
        s: *const u8,
        len: usize,
        out: *mut *const u8,
        outlen: *mut usize,
    ) -> i32;
}

const MAX_LEN: usize = 1024;
const PG_UTF8: i32 = 6;

/// Pin the Rust-side server encoding to UTF8 (thread-local Cell; the C
/// oracle side is compile-time pinned).
fn pin_utf8() {
    let _ = mbutils::SetDatabaseEncoding(PG_UTF8);
    // fc plane detoasts its text/array args; all images here are inline.
    // Seams are process-global set-once: install exactly once.
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // catch_unwind tolerates another lane's harness installing the
        // detoast seam first (double-install panics; all lanes share one
        // test binary — same convention as arrayfuncs_diff::init_seams).
        let _ = std::panic::catch_unwind(detoast::init_seams);
        let _ = std::panic::catch_unwind(adt_json::init_seams); // shipped no-op registration hook
    });
}

/// Arm the Rust-side recursion guard on the CALLING thread and pin
/// max_stack_depth to the server default 2048 kB (vendor guc.c:1613-1635
/// clamps the startup raise at 2048; nodesfam_diff::rearm_stack_bases is the
/// precedent, pinning BOTH sides). The C oracle arms its own base per entry
/// (PG_JSONFAM_ENTRY, csrc/pg_json_io.c) at the same 2048 kB — without this
/// the plane is one-sided: `stack_depth_core`'s base defaults to 0 (inert),
/// so adt_json's check_stack_depth lines never fire while the oracle's do.
/// In-domain this is verdict-neutral (MAX_LEN = 1024 => nesting <= 512 =>
/// ~49 kB, far under 2048 kB on both sides); the guard exists for corpus
/// replays, raised -max_len runs, and direct entry calls. Per-thread because
/// both bases are thread-locals; deep tests re-arm on their own big-stack
/// thread (see the deep pin below).
fn arm_stack_guards() {
    const SERVER_DEFAULT_KB: i32 = 2048;
    if stack_depth::max_stack_depth() != SERVER_DEFAULT_KB {
        stack_depth::set_max_stack_depth(SERVER_DEFAULT_KB);
        stack_depth::assign_max_stack_depth(SERVER_DEFAULT_KB);
    }
    let _ = stack_depth::set_stack_base();
}

// ---------------------------------------------------------------------------
// C-result plumbing
// ---------------------------------------------------------------------------

/// C oracle outcome: Ok(bytes) | SQL NULL | errcode.
enum COut<'a> {
    Val(&'a [u8]),
    Null,
    Err(i32),
}

fn sqlstate_i32(e: &types_error::PgError) -> i32 {
    let SqlState(v) = e.sqlstate();
    v
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx; verbatim pattern
// from uuid_diff.rs / quote_diff.rs).
// ---------------------------------------------------------------------------

fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [NullableDatum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.args = args;
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// Inline 4B-header text image (verbatim pattern from name_diff.rs).
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

/// A flat PG array image of text elements (elemtype text, typalign 'i',
/// typlen -1), matching the backend's ArrayType layout: used for the Rust
/// core/fc planes of json_object / json_object_two_arg / extract_path.
fn text_array_image(ndim: usize, dims: &[i32], elems: &[Option<&[u8]>]) -> Vec<u8> {
    let has_nulls = elems.iter().any(|e| e.is_none());
    let nitems = elems.len();
    let mut img: Vec<u8> = Vec::new();
    img.extend_from_slice(&[0u8; 4]); // vl_len_ patched last
    img.extend_from_slice(&(ndim as i32).to_ne_bytes());
    // dataoffset: 0 when no nulls, else the full header size incl. bitmap.
    let hdr_no_bitmap = 4 + 4 + 4 + 4 + 8 * ndim;
    let dataoffset: i32 = if has_nulls {
        (hdr_no_bitmap + nitems.div_ceil(8)).next_multiple_of(4) as i32
    } else {
        0
    };
    img.extend_from_slice(&dataoffset.to_ne_bytes());
    img.extend_from_slice(&25i32.to_ne_bytes()); // TEXTOID
    for d in 0..ndim {
        img.extend_from_slice(&dims[d].to_ne_bytes());
    }
    for _ in 0..ndim {
        img.extend_from_slice(&1i32.to_ne_bytes()); // lower bounds
    }
    if has_nulls {
        let mut bitmap = vec![0u8; nitems.div_ceil(8)];
        for (i, e) in elems.iter().enumerate() {
            if e.is_some() {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        img.extend_from_slice(&bitmap);
        while img.len() % 4 != 0 {
            img.push(0);
        }
    }
    for e in elems.iter() {
        if let Some(body) = e {
            while img.len() % 4 != 0 {
                img.push(0);
            }
            img.extend_from_slice(&text_image(body));
        }
    }
    let total = img.len() as u32;
    let word = total << 2;
    img[..4].copy_from_slice(&word.to_ne_bytes());
    img
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn json_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    pin_utf8();
    arm_stack_guards();
    match sel % 14 {
        0 => json_in_diff(payload),
        1 => json_typeof_diff(payload),
        2 => json_array_length_diff(payload),
        3 => json_strip_nulls_diff(payload),
        4 => json_validate_diff(payload),
        5 => json_get_field_diff(payload, false),
        6 => json_get_field_diff(payload, true),
        7 => json_get_element_diff(payload, false),
        8 => json_get_element_diff(payload, true),
        9 => json_get_path_diff(payload, false),
        10 => json_get_path_diff(payload, true),
        11 => json_object_diff(payload),
        12 => json_object_two_arg_diff(payload),
        _ => escape_json_diff(payload),
    }
}

fn text_ok(b: &[u8]) -> bool {
    b.len() <= MAX_LEN && !b.contains(&0)
}

/// SQL-reachability gate for the json-typed-argument arms (1-3, 5-10):
/// a `json` value always passed json_in validation, so unvalidated text is
/// outside the SQL-reachable domain (C keeps defensive elog arms there,
/// Rust keeps panics; neither is comparable nor reachable). NOTE the
/// validation lane (need_escapes=false) checks only \u hex FORMAT — lone
/// surrogates and \u0000 VALIDATE fine and keep the de-escape lanes'
/// error paths (code-point-zero, surrogate pairing) fully reachable here.
fn valid_json(b: &[u8]) -> bool {
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    (unsafe { pg_diff_json_in(b.as_ptr(), b.len(), &mut out, &mut outlen) }) == 0
}

// ---------------------------------------------------------------------------
// Arm 0: json_in (321; C json.c json_in over the verbatim jsonapi parser).
// ---------------------------------------------------------------------------

fn json_in_diff(payload: &[u8]) {
    if !text_ok(payload) {
        return;
    }

    // C oracle.
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let crc = unsafe { pg_diff_json_in(payload.as_ptr(), payload.len(), &mut out, &mut outlen) };
    let c = if crc == 0 {
        // SAFETY: arena bytes live until the next pg_diff_* call.
        COut::Val(unsafe { core::slice::from_raw_parts(out, outlen) })
    } else {
        COut::Err(crc)
    };

    // Accessor plane: JsonLex::input() is the C-parity input view.
    let lex = adt_json::jsonapi::JsonLex::new(payload, PG_UTF8);
    assert!(lex.input() == payload, "JsonLex::input identity");

    // Shipped Rust core, hard-error lane.
    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let r = adt_json::json_in(m, payload, None);
    match (&c, &r) {
        (COut::Val(cv), Ok(Some(v))) => assert!(
            v.data() == *cv,
            "json_in value DIVERGENCE input={:?}",
            String::from_utf8_lossy(payload)
        ),
        (COut::Err(ce), Err(e)) => assert!(
            sqlstate_i32(e) == *ce,
            "json_in sqlstate DIVERGENCE input={:?} C={ce:#x} Rust={:#x}",
            String::from_utf8_lossy(payload),
            sqlstate_i32(e)
        ),
        _ => panic!(
            "json_in verdict DIVERGENCE input={:?} C_ok={} Rust={:?}",
            String::from_utf8_lossy(payload),
            crc == 0,
            r.as_ref().map(|_| ()).map_err(|e| sqlstate_i32(e))
        ),
    }

    // Soft-error (ErrorSaveContext) plane: same verdict + sqlstate, absorbed.
    let mut soft = SoftErrorContext::new(true);
    let rs = adt_json::json_in(m, payload, Some(&mut soft));
    match &c {
        COut::Val(_) => {
            assert!(
                matches!(rs, Ok(Some(_))) && !soft.error_occurred(),
                "json_in soft-plane spurious error input={:?}",
                String::from_utf8_lossy(payload)
            );
        }
        COut::Err(ce) => {
            assert!(
                matches!(rs, Ok(None)) && soft.error_occurred(),
                "json_in soft-plane verdict DIVERGENCE input={:?}",
                String::from_utf8_lossy(payload)
            );
            let se = soft.error().expect("details_wanted saves the error");
            assert!(
                sqlstate_i32(se) == *ce,
                "json_in soft-plane sqlstate DIVERGENCE input={:?}",
                String::from_utf8_lossy(payload)
            );
        }
        COut::Null => unreachable!(),
    }

    // fc-wrapper plane (cstring arg).
    let mut cstr = payload.to_vec();
    cstr.push(0);
    let din = NullableDatum::value(Datum::from_usize(cstr.as_ptr() as usize));
    let (fr, _isnull) = fc_call::<1>(adt_json::builtins::fc_json_in, m, [din]);
    match (&c, &fr) {
        (COut::Val(cv), Ok(d)) => assert!(
            read_varlena_data(*d) == *cv,
            "fc_json_in vs C DIVERGENCE input={:?}",
            String::from_utf8_lossy(payload)
        ),
        (COut::Err(ce), Err(e)) => assert!(sqlstate_i32(e) == *ce, "fc_json_in sqlstate"),
        _ => panic!(
            "fc_json_in verdict DIVERGENCE input={:?}",
            String::from_utf8_lossy(payload)
        ),
    }

    // fc-wrapper soft-error plane: an armed ErrorSaveNode absorbs the parse
    // failure into a SQL NULL, sqlstate preserved (input_function_call_safe
    // shape).
    let mut esn = types_fmgr::ErrorSaveNode::new(true);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: cx outlives this call.
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.context = esn.fm_node_ptr();
    fcinfo.args = [NullableDatum::value(Datum::from_usize(cstr.as_ptr() as usize))];
    let frs = adt_json::builtins::fc_json_in(None, &mut fcinfo);
    match &c {
        COut::Val(cv) => {
            let d = frs.expect("fc_json_in soft-plane spurious error");
            assert!(!fcinfo.isnull && read_varlena_data(d) == *cv, "fc_json_in soft value");
            assert!(!esn.ctx.error_occurred(), "fc_json_in soft spurious save");
        }
        COut::Err(ce) => {
            // Absorbed: Ok(null Datum) + error saved in the node (the
            // input_function_call_safe caller keys off error_occurred).
            assert!(frs.is_ok(), "fc_json_in soft verdict");
            let se = esn.ctx.error().expect("soft error saved");
            assert!(sqlstate_i32(se) == *ce, "fc_json_in soft sqlstate");
        }
        COut::Null => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Arm 1: json_typeof (3968; C jsonfuncs.c json_typeof).
// ---------------------------------------------------------------------------

fn json_typeof_diff(payload: &[u8]) {
    if !text_ok(payload) || !valid_json(payload) {
        return;
    }
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let crc =
        unsafe { pg_diff_json_typeof(payload.as_ptr(), payload.len(), &mut out, &mut outlen) };

    let r = adt_json::funcs::json_typeof(payload);
    match (crc, &r) {
        (0, Ok(t)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(
                t.as_bytes() == cv,
                "json_typeof value DIVERGENCE input={:?}",
                String::from_utf8_lossy(payload)
            );
        }
        (ce, Err(e)) if ce != 0 => assert!(
            sqlstate_i32(e) == ce,
            "json_typeof sqlstate DIVERGENCE input={:?}",
            String::from_utf8_lossy(payload)
        ),
        _ => panic!(
            "json_typeof verdict DIVERGENCE input={:?} C={crc:#x}",
            String::from_utf8_lossy(payload)
        ),
    }

    // fc plane.
    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let img = text_image(payload);
    let din = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let (fr, _) = fc_call::<1>(adt_json::builtins::fc_json_typeof, m, [din]);
    match (crc, &fr) {
        (0, Ok(d)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(read_varlena_data(*d) == cv, "fc_json_typeof vs C");
        }
        (ce, Err(e)) if ce != 0 => assert!(sqlstate_i32(e) == ce, "fc_json_typeof sqlstate"),
        _ => panic!("fc_json_typeof verdict DIVERGENCE"),
    }
}

// ---------------------------------------------------------------------------
// Arm 2: json_array_length (3956).
// ---------------------------------------------------------------------------

fn json_array_length_diff(payload: &[u8]) {
    if !text_ok(payload) || !valid_json(payload) {
        return;
    }
    let mut count: i32 = 0;
    let crc = unsafe { pg_diff_json_array_length(payload.as_ptr(), payload.len(), &mut count) };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let r = adt_json::funcs::json_array_length(m, payload);
    match (crc, &r) {
        (0, Ok(n)) => assert!(
            *n == count,
            "json_array_length value DIVERGENCE input={:?} C={count} Rust={n}",
            String::from_utf8_lossy(payload)
        ),
        (ce, Err(e)) if ce != 0 => assert!(
            sqlstate_i32(e) == ce,
            "json_array_length sqlstate DIVERGENCE input={:?} C={ce:#x} Rust={:#x}",
            String::from_utf8_lossy(payload),
            sqlstate_i32(e)
        ),
        _ => panic!(
            "json_array_length verdict DIVERGENCE input={:?} C={crc:#x} Rust_ok={}",
            String::from_utf8_lossy(payload),
            r.is_ok()
        ),
    }

    // fc plane.
    let img = text_image(payload);
    let din = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let (fr, _) = fc_call::<1>(adt_json::builtins::fc_json_array_length, m, [din]);
    match (crc, &fr) {
        (0, Ok(d)) => assert!(d.as_i32() == count, "fc_json_array_length vs C"),
        (ce, Err(e)) if ce != 0 => assert!(sqlstate_i32(e) == ce, "fc_json_array_length sqlstate"),
        _ => panic!("fc_json_array_length verdict DIVERGENCE"),
    }
}

// ---------------------------------------------------------------------------
// Arm 3: json_strip_nulls (3261), both strip_in_arrays modes.
// ---------------------------------------------------------------------------

fn json_strip_nulls_diff(payload: &[u8]) {
    let Some((&flag, json)) = payload.split_first() else {
        return;
    };
    if !text_ok(json) || !valid_json(json) {
        return;
    }
    let strip_in_arrays = flag & 1 != 0;

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let crc = unsafe {
        pg_diff_json_strip_nulls(
            json.as_ptr(),
            json.len(),
            strip_in_arrays as i32,
            &mut out,
            &mut outlen,
        )
    };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let r = adt_json::funcs::json_strip_nulls(m, json, strip_in_arrays);
    match (crc, &r) {
        (0, Ok(v)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(
                v.data() == cv,
                "json_strip_nulls value DIVERGENCE strip={strip_in_arrays} input={:?} C={:?} Rust={:?}",
                String::from_utf8_lossy(json),
                String::from_utf8_lossy(cv),
                String::from_utf8_lossy(v.data())
            );
        }
        (ce, Err(e)) if ce != 0 => assert!(
            sqlstate_i32(e) == ce,
            "json_strip_nulls sqlstate DIVERGENCE input={:?}",
            String::from_utf8_lossy(json)
        ),
        _ => panic!(
            "json_strip_nulls verdict DIVERGENCE input={:?} C={crc:#x}",
            String::from_utf8_lossy(json)
        ),
    }

    // fc plane: 2-arg form, plus the 1-arg dispatch (strip=false) when
    // flag bit1 is set and the modes agree.
    let img = text_image(json);
    if flag & 2 != 0 && !strip_in_arrays {
        let d0 = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
        let (fr1, _) = fc_call::<1>(adt_json::builtins::fc_json_strip_nulls, m, [d0]);
        match (crc, &fr1) {
            (0, Ok(d)) => {
                let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
                assert!(read_varlena_data(*d) == cv, "fc_json_strip_nulls(1) vs C");
            }
            (ce, Err(e)) if ce != 0 => {
                assert!(sqlstate_i32(e) == ce, "fc_json_strip_nulls(1) sqlstate")
            }
            _ => panic!("fc_json_strip_nulls(1) verdict DIVERGENCE"),
        }
    }
    let d0 = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let d1 = NullableDatum::value(Datum::from_bool(strip_in_arrays));
    let (fr, _) = fc_call::<2>(adt_json::builtins::fc_json_strip_nulls, m, [d0, d1]);
    match (crc, &fr) {
        (0, Ok(d)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(read_varlena_data(*d) == cv, "fc_json_strip_nulls vs C");
        }
        (ce, Err(e)) if ce != 0 => assert!(sqlstate_i32(e) == ce, "fc_json_strip_nulls sqlstate"),
        _ => panic!("fc_json_strip_nulls verdict DIVERGENCE"),
    }
}

// ---------------------------------------------------------------------------
// Arm 4: json_validate (IS JSON core; modes = check_unique x throw_error).
// ---------------------------------------------------------------------------

fn json_validate_diff(payload: &[u8]) {
    let Some((&flag, json)) = payload.split_first() else {
        return;
    };
    if !text_ok(json) {
        return;
    }
    let check_unique = flag & 1 != 0;
    let throw_error = flag & 2 != 0;

    let mut ok: i32 = 0;
    let crc = unsafe {
        pg_diff_json_validate(
            json.as_ptr(),
            json.len(),
            check_unique as i32,
            throw_error as i32,
            &mut ok,
        )
    };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let r = adt_json::funcs::json_validate(m, json, check_unique, throw_error);
    match (crc, &r) {
        (0, Ok(b)) => assert!(
            *b == (ok != 0),
            "json_validate value DIVERGENCE unique={check_unique} input={:?} C={ok} Rust={b}",
            String::from_utf8_lossy(json)
        ),
        (ce, Err(e)) if ce != 0 => assert!(
            sqlstate_i32(e) == ce,
            "json_validate sqlstate DIVERGENCE input={:?} C={ce:#x} Rust={:#x}",
            String::from_utf8_lossy(json),
            sqlstate_i32(e)
        ),
        _ => panic!(
            "json_validate verdict DIVERGENCE unique={check_unique} throw={throw_error} input={:?} C={crc:#x}",
            String::from_utf8_lossy(json)
        ),
    }
}

// ---------------------------------------------------------------------------
// Arms 5-8: json_object_field(_text) / json_array_element(_text) via
// get_worker (3947-3950).
// ---------------------------------------------------------------------------

fn json_get_field_diff(payload: &[u8], as_text: bool) {
    let Some((&klen, rest)) = payload.split_first() else {
        return;
    };
    let klen = klen as usize;
    if rest.len() < klen {
        return;
    }
    let (key, json) = rest.split_at(klen);
    if !text_ok(json) || !text_ok(key) || !valid_json(json) {
        return;
    }

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let mut isnull: i32 = 0;
    let crc = unsafe {
        pg_diff_json_get_field(
            json.as_ptr(),
            json.len(),
            key.as_ptr(),
            key.len(),
            as_text as i32,
            &mut out,
            &mut outlen,
            &mut isnull,
        )
    };
    let c = if crc != 0 {
        COut::Err(crc)
    } else if isnull != 0 {
        COut::Null
    } else {
        COut::Val(unsafe { core::slice::from_raw_parts(out, outlen) })
    };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let names = [key];
    let r = adt_json::getpath::get_worker(m, json, Some(&names), None, 1, as_text);
    check_getter("json_object_field", as_text, json, &c, &r);

    // fc plane.
    let jimg = text_image(json);
    let kimg = text_image(key);
    let d0 = NullableDatum::value(Datum::from_usize(jimg.as_ptr() as usize));
    let d1 = NullableDatum::value(Datum::from_usize(kimg.as_ptr() as usize));
    let f: PGFunction = if as_text {
        adt_json::builtins::fc_json_object_field_text
    } else {
        adt_json::builtins::fc_json_object_field
    };
    let (fr, fnull) = fc_call::<2>(f, m, [d0, d1]);
    check_getter_fc("fc_json_object_field", json, &c, &fr, fnull);
}

fn json_get_element_diff(payload: &[u8], as_text: bool) {
    if payload.len() < 4 {
        return;
    }
    let (ib, json) = payload.split_at(4);
    let idx = i32::from_le_bytes(ib.try_into().unwrap());
    if !text_ok(json) || !valid_json(json) {
        return;
    }

    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let mut isnull: i32 = 0;
    let crc = unsafe {
        pg_diff_json_get_element(
            json.as_ptr(),
            json.len(),
            idx,
            as_text as i32,
            &mut out,
            &mut outlen,
            &mut isnull,
        )
    };
    let c = if crc != 0 {
        COut::Err(crc)
    } else if isnull != 0 {
        COut::Null
    } else {
        COut::Val(unsafe { core::slice::from_raw_parts(out, outlen) })
    };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let mut indexes = [idx];
    let r = adt_json::getpath::get_worker(m, json, None, Some(&mut indexes), 1, as_text);
    check_getter("json_array_element", as_text, json, &c, &r);

    // fc plane.
    let jimg = text_image(json);
    let d0 = NullableDatum::value(Datum::from_usize(jimg.as_ptr() as usize));
    let d1 = NullableDatum::value(Datum::from_i32(idx));
    let f: PGFunction = if as_text {
        adt_json::builtins::fc_json_array_element_text
    } else {
        adt_json::builtins::fc_json_array_element
    };
    let (fr, fnull) = fc_call::<2>(f, m, [d0, d1]);
    check_getter_fc("fc_json_array_element", json, &c, &fr, fnull);
}

fn json_get_path_diff(payload: &[u8], as_text: bool) {
    let Some((&np, mut rest)) = payload.split_first() else {
        return;
    };
    let npath = (np % 5) as usize;
    // Option = SQL NULL path element (raw length byte >= 200); exercises
    // get_path_all's array_contains_nulls early-NULL arm.
    let mut elems: Vec<Option<&[u8]>> = Vec::with_capacity(npath);
    for _ in 0..npath {
        let Some((&el, r2)) = rest.split_first() else {
            return;
        };
        if el >= 200 {
            elems.push(None);
            rest = r2;
            continue;
        }
        let el = (el % 33) as usize;
        if r2.len() < el {
            return;
        }
        let (e, r3) = r2.split_at(el);
        if !text_ok(e) {
            return;
        }
        elems.push(Some(e));
        rest = r3;
    }
    let json = rest;
    if !text_ok(json) || !valid_json(json) {
        return;
    }

    let ptrs: Vec<*const u8> = elems
        .iter()
        .map(|e| e.map_or(core::ptr::null(), |b| b.as_ptr()))
        .collect();
    let lens: Vec<usize> = elems.iter().map(|e| e.map_or(0, |b| b.len())).collect();
    let nullflags: Vec<u8> = elems.iter().map(|e| u8::from(e.is_none())).collect();
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let mut isnull: i32 = 0;
    let crc = unsafe {
        pg_diff_json_get_path(
            json.as_ptr(),
            json.len(),
            npath as i32,
            ptrs.as_ptr(),
            lens.as_ptr(),
            nullflags.as_ptr(),
            as_text as i32,
            &mut out,
            &mut outlen,
            &mut isnull,
        )
    };
    let c = if crc != 0 {
        COut::Err(crc)
    } else if isnull != 0 {
        COut::Null
    } else {
        COut::Val(unsafe { core::slice::from_raw_parts(out, outlen) })
    };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    if !elems.iter().any(|e| e.is_none()) {
        // Rust core: get_worker with names + path_index-derived indexes (the
        // shipped get_path_all decomposition past the null check).
        let flat: Vec<&[u8]> = elems.iter().map(|e| e.unwrap()).collect();
        let mut indexes: Vec<i32> = flat
            .iter()
            .map(|e| adt_json::getpath::path_index(e))
            .collect();
        let r = adt_json::getpath::get_worker(
            m,
            json,
            Some(&flat),
            Some(&mut indexes),
            npath,
            as_text,
        );
        check_getter("json_extract_path", as_text, json, &c, &r);
    }

    // fc plane: real text[] path array image (incl. the null-element arm).
    let opt_elems: Vec<Option<&[u8]>> = elems.clone();
    let dims = [npath as i32, 0];
    let ndim = usize::from(npath > 0);
    let pimg = text_array_image(ndim, &dims, &opt_elems);
    let jimg = text_image(json);
    let d0 = NullableDatum::value(Datum::from_usize(jimg.as_ptr() as usize));
    let d1 = NullableDatum::value(Datum::from_usize(pimg.as_ptr() as usize));
    let f: PGFunction = if as_text {
        adt_json::builtins::fc_json_extract_path_text
    } else {
        adt_json::builtins::fc_json_extract_path
    };
    let (fr, fnull) = fc_call::<2>(f, m, [d0, d1]);
    check_getter_fc("fc_json_extract_path", json, &c, &fr, fnull);
}

fn check_getter(
    name: &str,
    as_text: bool,
    json: &[u8],
    c: &COut<'_>,
    r: &PgResult<Option<datum::Varlena<'_>>>,
) {
    match (c, r) {
        (COut::Val(cv), Ok(Some(v))) => assert!(
            v.data() == *cv,
            "{name} value DIVERGENCE as_text={as_text} input={:?} C={:?} Rust={:?}",
            String::from_utf8_lossy(json),
            String::from_utf8_lossy(cv),
            String::from_utf8_lossy(v.data())
        ),
        (COut::Null, Ok(None)) => {}
        (COut::Err(ce), Err(e)) => assert!(
            sqlstate_i32(e) == *ce,
            "{name} sqlstate DIVERGENCE input={:?} C={ce:#x} Rust={:#x}",
            String::from_utf8_lossy(json),
            sqlstate_i32(e)
        ),
        _ => panic!(
            "{name} verdict DIVERGENCE as_text={as_text} input={:?}",
            String::from_utf8_lossy(json)
        ),
    }
}

fn check_getter_fc(
    name: &str,
    json: &[u8],
    c: &COut<'_>,
    fr: &PgResult<Datum>,
    fnull: bool,
) {
    match (c, fr) {
        (COut::Val(cv), Ok(d)) => {
            assert!(!fnull, "{name} spurious SQL NULL");
            assert!(
                read_varlena_data(*d) == *cv,
                "{name} vs C value DIVERGENCE input={:?}",
                String::from_utf8_lossy(json)
            );
        }
        (COut::Null, Ok(_)) => assert!(fnull, "{name} missing SQL NULL"),
        (COut::Err(ce), Err(e)) => assert!(sqlstate_i32(e) == *ce, "{name} sqlstate"),
        _ => panic!(
            "{name} verdict DIVERGENCE input={:?}",
            String::from_utf8_lossy(json)
        ),
    }
}

// ---------------------------------------------------------------------------
// Arms 11/12: json_object (3202) / json_object_two_arg (3203). The C side
// receives pre-deconstructed elements (environment seam); the Rust side gets
// a real flat text[] image.
// ---------------------------------------------------------------------------

/// Parse `[flag][len][bytes]` element encodings; None = SQL NULL element.
fn parse_elems<'a>(mut rest: &'a [u8], count: usize) -> Option<(Vec<Option<&'a [u8]>>, &'a [u8])> {
    let mut elems = Vec::with_capacity(count);
    for _ in 0..count {
        let (&flag, r2) = rest.split_first()?;
        if flag & 1 != 0 {
            elems.push(None);
            rest = r2;
            continue;
        }
        let (&el, r3) = r2.split_first()?;
        let el = (el % 33) as usize;
        if r3.len() < el {
            return None;
        }
        let (e, r4) = r3.split_at(el);
        if !text_ok(e) {
            return None;
        }
        elems.push(Some(e));
        rest = r4;
    }
    Some((elems, rest))
}

fn celems(elems: &[Option<&[u8]>]) -> (Vec<*const u8>, Vec<usize>, Vec<u8>) {
    let mut p: Vec<*const u8> = Vec::with_capacity(elems.len());
    let mut l: Vec<usize> = Vec::with_capacity(elems.len());
    let mut n: Vec<u8> = Vec::with_capacity(elems.len());
    for e in elems {
        match e {
            Some(b) => {
                p.push(b.as_ptr());
                l.push(b.len());
                n.push(0);
            }
            None => {
                p.push(core::ptr::null());
                l.push(0);
                n.push(1);
            }
        }
    }
    (p, l, n)
}

fn json_object_diff(payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let ndim = (payload[0] % 4) as usize;
    let d = [
        (payload[1] % 5) as i32,
        (payload[2] % 5) as i32,
        (payload[3] % 3) as i32,
    ];
    let dims: Vec<i32> = d[..ndim].to_vec();
    let count: usize = dims.iter().map(|&x| x as usize).product::<usize>() * usize::from(ndim > 0);
    let Some((elems, _rest)) = parse_elems(&payload[4..], count) else {
        return;
    };

    let (p, l, n) = celems(&elems);
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let crc = unsafe {
        pg_diff_json_object(
            ndim as i32,
            dims.as_ptr(),
            count as i32,
            p.as_ptr(),
            l.as_ptr(),
            n.as_ptr(),
            &mut out,
            &mut outlen,
        )
    };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let img = text_array_image(ndim, &dims, &elems);
    // SAFETY: the image vec outlives the call; json_object reads it in-place.
    let arr: &[u8] = unsafe { core::slice::from_raw_parts(img.as_ptr(), img.len()) };
    let r = adt_json::tojson::json_object(m, arr);
    match (crc, &r) {
        (0, Ok(v)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(
                v.data() == cv,
                "json_object value DIVERGENCE ndim={ndim} dims={dims:?} C={:?} Rust={:?}",
                String::from_utf8_lossy(cv),
                String::from_utf8_lossy(v.data())
            );
        }
        (ce, Err(e)) if ce != 0 => assert!(
            sqlstate_i32(e) == ce,
            "json_object sqlstate DIVERGENCE ndim={ndim} dims={dims:?} C={ce:#x} Rust={:#x}",
            sqlstate_i32(e)
        ),
        _ => panic!(
            "json_object verdict DIVERGENCE ndim={ndim} dims={dims:?} C={crc:#x} Rust_ok={}",
            r.is_ok()
        ),
    }

    // fc plane.
    let d0 = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
    let (fr, _) = fc_call::<1>(adt_json::builtins::fc_json_object, m, [d0]);
    match (crc, &fr) {
        (0, Ok(dd)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(read_varlena_data(*dd) == cv, "fc_json_object vs C");
        }
        (ce, Err(e)) if ce != 0 => assert!(sqlstate_i32(e) == ce, "fc_json_object sqlstate"),
        _ => panic!("fc_json_object verdict DIVERGENCE"),
    }
}

fn json_object_two_arg_diff(payload: &[u8]) {
    if payload.len() < 3 {
        return;
    }
    let nkdim = (payload[0] % 2) as usize;
    let nvdim = ((payload[0] >> 1) % 2) as usize;
    let kcount = if nkdim == 0 { 0 } else { (payload[1] % 6) as usize };
    let vcount = if nvdim == 0 { 0 } else { (payload[2] % 6) as usize };
    let Some((kelems, rest)) = parse_elems(&payload[3..], kcount) else {
        return;
    };
    let Some((velems, _rest)) = parse_elems(rest, vcount) else {
        return;
    };

    let kdims = [kcount as i32, 0];
    let vdims = [vcount as i32, 0];
    let (kp, kl, kn) = celems(&kelems);
    let (vp, vl, vn) = celems(&velems);
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let crc = unsafe {
        pg_diff_json_object_two_arg(
            nkdim as i32,
            kdims.as_ptr(),
            kcount as i32,
            kp.as_ptr(),
            kl.as_ptr(),
            kn.as_ptr(),
            nvdim as i32,
            vdims.as_ptr(),
            vcount as i32,
            vp.as_ptr(),
            vl.as_ptr(),
            vn.as_ptr(),
            &mut out,
            &mut outlen,
        )
    };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let kimg = text_array_image(nkdim, &kdims, &kelems);
    let vimg = text_array_image(nvdim, &vdims, &velems);
    let r = adt_json::tojson::json_object_two_arg(m, &kimg, &vimg);
    match (crc, &r) {
        (0, Ok(v)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(
                v.data() == cv,
                "json_object_two_arg value DIVERGENCE C={:?} Rust={:?}",
                String::from_utf8_lossy(cv),
                String::from_utf8_lossy(v.data())
            );
        }
        (ce, Err(e)) if ce != 0 => assert!(
            sqlstate_i32(e) == ce,
            "json_object_two_arg sqlstate DIVERGENCE C={ce:#x} Rust={:#x}",
            sqlstate_i32(e)
        ),
        _ => panic!(
            "json_object_two_arg verdict DIVERGENCE nkdim={nkdim} nvdim={nvdim} \
             kcount={kcount} vcount={vcount} C={crc:#x} Rust_ok={}",
            r.is_ok()
        ),
    }

    // fc plane.
    let d0 = NullableDatum::value(Datum::from_usize(kimg.as_ptr() as usize));
    let d1 = NullableDatum::value(Datum::from_usize(vimg.as_ptr() as usize));
    let (fr, _) = fc_call::<2>(adt_json::builtins::fc_json_object_two_arg, m, [d0, d1]);
    match (crc, &fr) {
        (0, Ok(dd)) => {
            let cv = unsafe { core::slice::from_raw_parts(out, outlen) };
            assert!(read_varlena_data(*dd) == cv, "fc_json_object_two_arg vs C");
        }
        (ce, Err(e)) if ce != 0 => {
            assert!(sqlstate_i32(e) == ce, "fc_json_object_two_arg sqlstate")
        }
        _ => panic!("fc_json_object_two_arg verdict DIVERGENCE"),
    }
}

// ---------------------------------------------------------------------------
// Arm 13: escape_json (C escape_json_with_len; NUL in-domain).
// ---------------------------------------------------------------------------

fn escape_json_diff(payload: &[u8]) {
    if payload.len() > MAX_LEN {
        return;
    }
    let mut out: *const u8 = core::ptr::null();
    let mut outlen: usize = 0;
    let crc =
        unsafe { pg_diff_escape_json(payload.as_ptr(), payload.len(), &mut out, &mut outlen) };
    assert!(crc == 0, "escape_json C oracle cannot fail");
    let cv = unsafe { core::slice::from_raw_parts(out, outlen) };

    let cx = mcx::MemoryContext::new("json_fuzz");
    let m = cx.mcx();
    let mut buf = StringInfo::new_in(m).expect("stringinfo alloc");
    adt_json::escape_json(&mut buf, payload).expect("escape_json alloc at fuzz sizes");
    assert!(
        buf.as_bytes() == cv,
        "escape_json value DIVERGENCE input={payload:?} C={:?} Rust={:?}",
        String::from_utf8_lossy(cv),
        String::from_utf8_lossy(buf.as_bytes())
    );
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(depth: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(depth * 2);
        v.extend(std::iter::repeat(b'[').take(depth));
        v.extend(std::iter::repeat(b']').take(depth));
        v
    }

    fn json_in_rc(src: &[u8]) -> i32 {
        let mut out: *const u8 = std::ptr::null();
        let mut outlen: usize = 0;
        unsafe { pg_diff_json_in(src.as_ptr(), src.len(), &mut out, &mut outlen) }
    }

    /// SHIM-CONTRACT PIN (task #131, rework of the refuted 515fffe6d6a).
    ///
    /// The oracle's `check_stack_depth` must reproduce stack_depth.c's
    /// contract at the EFFECTIVE server default: a BYTE budget of 2048 kB
    /// (vendor guc.c:1613-1635 raises the 100 kB boot value to
    /// min((rlimit-512kB)/1024, 2048) kB at startup; guc_tables.c:2615-2618
    /// says so in so many words), measured from a base armed at entry,
    /// raising the catchable 54001. Three wrong shims are each rejected by a
    /// dedicated arm:
    ///
    ///  * frame counter at 100000 (the pre-census shim): DEAD — needs
    ///    ~9.1 MiB of stack to fire; nesting 60000 returns 0 under it, so
    ///    the deep arm fails;
    ///  * byte budget at 100 kB (the REFUTED first fix): 20x tighter than a
    ///    real backend — fires 54001 at nesting 4000 (~0.4 MiB) where PG
    ///    parses fine, so the mid-depth arm fails;
    ///  * no guard at all: nesting 60000 rides the stack down, and on this
    ///    test's 16 MiB thread nesting 200000 (~19 MiB unguarded) SIGBUSes.
    ///
    /// The deep arms run on a dedicated 16 MiB thread (nodesfam precedent):
    /// a 2048 kB budget can never fire on a default 2 MiB libtest thread —
    /// physical exhaustion comes first — and the guard caps consumption at
    /// ~2 MiB + slop, so 16 MiB is safe headroom for every arm.
    #[test]
    fn shim_check_stack_depth_is_pg_byte_budget_2048kb() {
        // In-domain (<= MAX_LEN): must parse on any thread, exactly as
        // before the fix — the guard is not allowed to change any verdict
        // json_diff can actually reach (nesting <= 512 => ~49 kB).
        {
            let _serial = crate::c_oracle_serial();
            for depth in [1usize, 8, 256, MAX_LEN / 2] {
                assert_eq!(
                    json_in_rc(&nested(depth)),
                    0,
                    "in-domain nesting {depth} must stay accepted"
                );
            }
        }

        std::thread::Builder::new()
            .name("json_stack_guard_deep".into())
            .stack_size(16 << 20)
            .spawn(|| {
                let _serial = crate::c_oracle_serial();
                let too_complex = types_error::ERRCODE_STATEMENT_TOO_COMPLEX.0;

                // Mid-depth: ~0.4 MiB of oracle stack. A real backend
                // (max_stack_depth 2048 kB) parses this; the refuted 100 kB
                // bound raised 54001 here. MUST stay accepted.
                assert_eq!(
                    json_in_rc(&nested(4_000)),
                    0,
                    "nesting 4000 is within PG's 2048 kB default: a shim that \
                     rejects it fires where a real server does not \
                     (the refuted 100 kB bound)"
                );

                // Past 2048 kB: PG raises the catchable 54001; the oracle
                // must too — not a success (dead frame counter) and not a
                // crash (no guard).
                for depth in [60_000usize, 200_000] {
                    assert_eq!(
                        json_in_rc(&nested(depth)),
                        too_complex,
                        "nesting {depth} exceeds max_stack_depth (2048 kB): \
                         the shim owes PG's catchable 54001"
                    );
                }

                // Two-sidedness: armed the same way (2048 kB, base at this
                // frame), the shipped Rust side raises the SAME 54001 on the
                // same shape — the refuted fix left this side inert.
                arm_stack_guards();
                let cx = mcx::MemoryContext::new("json_stack_guard_deep");
                let m = cx.mcx();
                match adt_json::json_in(m, &nested(60_000), None) {
                    Err(e) => assert_eq!(
                        sqlstate_i32(&e),
                        too_complex,
                        "Rust json_in must raise 54001 past the budget"
                    ),
                    Ok(_) => panic!(
                        "Rust json_in accepted nesting 60000 with the guard \
                         armed at 2048 kB — one-sided stack-depth plane"
                    ),
                }
                assert!(
                    adt_json::json_in(m, &nested(MAX_LEN / 2), None).is_ok(),
                    "in-domain nesting must stay accepted on the Rust side"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/json_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/json_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                json_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// a0 EXHAUSTIVE-DIFF (Michael 2026-07-31): the \uXXXX escape domain.
    /// Phase 1: ALL 2^16 single escapes through json_in (validation lane)
    /// and ->> de-escape (json_object_field_text). Phase 2: ALL escape
    /// pairs whose behavior is state-dependent — every high surrogate
    /// (0xD800..=0xDBFF) x every second escape (2^16) = 67,108,864 cases —
    /// through the de-escape lane. Pairs with a non-high-surrogate first
    /// escape reduce to phase 1 (hi_surrogate state is -1; the lone-low /
    /// zero / conversion verdicts fire per-escape), and a low-surrogate
    /// first escape fails before the second is read, so this union is
    /// TOTAL over the 1- and 2-escape domain. Run explicitly (release):
    ///   cargo test --release -p decoder_fuzz \
    ///     json_diff::tests::exhaustive_unicode_escape_domain -- \
    ///     --ignored --nocapture
    #[test]
    #[ignore = "a0 exhaustive sweep: run explicitly in release"]
    fn exhaustive_unicode_escape_domain() {
        let _serial = crate::c_oracle_serial();
        // NO outer c_oracle_serial here: phase 2's scoped workers call the
        // guarded driver entries (json_get_field_diff/json_in_diff take
        // oracle_serial at entry since the 2026-08-02 rework), and an outer
        // guard held across the spawn would deadlock them. Entry-level
        // serialization also means phase 2 now runs lock-stepped rather
        // than truly parallel — correctness of the shared C oracle state
        // outranks sweep wall-time (this is an explicit --ignored run).
        const HEX: &[u8; 16] = b"0123456789abcdef";
        fn esc(cp: u32, out: &mut Vec<u8>) {
            out.extend_from_slice(b"\\u");
            out.push(HEX[((cp >> 12) & 0xf) as usize]);
            out.push(HEX[((cp >> 8) & 0xf) as usize]);
            out.push(HEX[((cp >> 4) & 0xf) as usize]);
            out.push(HEX[(cp & 0xf) as usize]);
        }
        pin_utf8();
        // Phase 1: singles (json_in + de-escape planes).
        for cp in 0..=0xFFFFu32 {
            let mut j = Vec::with_capacity(24);
            j.extend_from_slice(b"{\"a\":\"");
            esc(cp, &mut j);
            j.extend_from_slice(b"\"}");
            let mut p = vec![1u8, b'a'];
            p.extend_from_slice(&j);
            json_get_field_diff(&p, true);
            json_in_diff(&j);
        }
        println!("phase 1 (65536 singles) done");
        // Phase 2: high surrogate x all second escapes, sharded across
        // threads (C oracle state is thread-local).
        let nthreads = std::thread::available_parallelism().map_or(4, |n| n.get());
        let counter = std::sync::atomic::AtomicU32::new(0xD800);
        std::thread::scope(|sc| {
            for _ in 0..nthreads {
                sc.spawn(|| {
                    pin_utf8();
                    loop {
                        let hi = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if hi > 0xDBFF {
                            break;
                        }
                        for lo in 0..=0xFFFFu32 {
                            let mut j = Vec::with_capacity(32);
                            j.extend_from_slice(b"{\"a\":\"");
                            esc(hi, &mut j);
                            esc(lo, &mut j);
                            j.extend_from_slice(b"\"}");
                            let mut p = vec![1u8, b'a'];
                            p.extend_from_slice(&j);
                            json_get_field_diff(&p, true);
                        }
                    }
                });
            }
        });
        println!("phase 2 (1024 x 65536 pairs) done: total 67,174,400 cases");
    }

    fn arm(sel: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![sel];
        v.extend_from_slice(body);
        v
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // 0 json_in: valid + invalid + unicode escapes + surrogate pairs.
        json_diff(&arm(0, "{\"a\": [1,2,null], \"b\":\"xé😀\"}".as_bytes()));
        json_diff(&arm(0, b"{bad"));
        json_diff(&arm(0, br#""\ud83d""#));
        json_diff(&arm(0, br#""\u0000""#));
        json_diff(&arm(0, b"  [1e5, -0.5, 1E+2] "));
        // 1 typeof over every scalar kind.
        for j in [
            &b"{}"[..], b"[]", b"\"s\"", b"1.5", b"true", b"false", b"null", b"nul",
        ] {
            json_diff(&arm(1, j));
        }
        // 2 array_length: array / object / scalar / bad.
        for j in [&b"[1,2,3]"[..], b"{}", b"7", b"["] {
            json_diff(&arm(2, j));
        }
        // 3 strip_nulls both modes.
        json_diff(&arm(3, b"\x00{\"a\":null,\"b\":[null,1]}"));
        json_diff(&arm(3, b"\x01{\"a\":null,\"b\":[null,1]}"));
        // 4 validate: dup keys, all four mode combos.
        for f in 0..4u8 {
            let mut p = vec![f];
            p.extend_from_slice(br#"{"a":1,"a":2}"#);
            json_diff(&arm(4, &p));
            let mut p = vec![f];
            p.extend_from_slice(b"{bad");
            json_diff(&arm(4, &p));
        }
        // 5/6 object_field: hit + miss + de-escape.
        let mut p = vec![1u8];
        p.extend_from_slice(b"b");
        p.extend_from_slice("{\"a\":1,\"b\":\"x😀\",\"c\":null}".as_bytes());
        json_diff(&arm(5, &p));
        json_diff(&arm(6, &p));
        // 7/8 array_element incl. negative index.
        for idx in [0i32, 2, -1, -5, i32::MIN] {
            let mut p = idx.to_le_bytes().to_vec();
            p.extend_from_slice("[10,\"aé\",null]".as_bytes());
            json_diff(&arm(7, &p));
            json_diff(&arm(8, &p));
        }
        // 5/6 de-escape unicode error paths on VALID json (lone surrogate,
        // \u0000): validation passes, ->> de-escape errors.
        for j in [
            &br#"{"a":"\ud83d"}"#[..],
            br#"{"a":"\u0000"}"#,
            br#"{"a":"\ude00\ud83d"}"#,
            br#"{"a":"\ud83dx"}"#,
        ] {
            let mut p = vec![1u8];
            p.extend_from_slice(b"a");
            p.extend_from_slice(j);
            json_diff(&arm(6, &p));
            json_diff(&arm(5, &p));
        }
        // 9/10 extract_path: name+index mix, numeric-string path element.
        let mut p = vec![2u8];
        p.extend_from_slice(&[1u8]);
        p.extend_from_slice(b"a");
        p.extend_from_slice(&[1u8]);
        p.extend_from_slice(b"1");
        p.extend_from_slice(br#"{"a":[10,20],"1":"x"}"#);
        json_diff(&arm(9, &p));
        json_diff(&arm(10, &p));
        // 11 json_object: ndim 0/1/2 + odd count + null key + null value.
        json_diff(&arm(11, &[0u8, 0, 0, 0]));
        json_diff(&arm(11, &[1u8, 2, 0, 0, 0, 1, b'k', 0, 1, b'v']));
        json_diff(&arm(11, &[1u8, 3, 0, 0, 0, 1, b'k', 0, 1, b'v', 0, 0])); // odd
        json_diff(&arm(11, &[1u8, 2, 0, 0, 1, 0, 1, b'v'])); // null key
        json_diff(&arm(11, &[1u8, 2, 0, 0, 0, 1, b'k', 1])); // null value
        json_diff(&arm(11, &[2u8, 2, 2, 0, 0, 1, b'a', 0, 1, b'b', 0, 1, b'c', 0, 1, b'd']));
        json_diff(&arm(11, &[3u8, 1, 1, 1, 0, 1, b'x'])); // wrong ndims
        // 12 two_arg: match, mismatch, zero-dim.
        json_diff(&arm(12, &[3u8, 1, 1, 0, 1, b'k', 0, 1, b'v']));
        json_diff(&arm(12, &[3u8, 2, 1, 0, 1, b'a', 0, 1, b'b', 0, 1, b'v']));
        json_diff(&arm(12, &[0u8, 0, 0]));
        // 13 escape_json incl. NUL + controls + quotes.
        json_diff(&arm(13, b"plain"));
        json_diff(&arm(13, b"a\"b\\c\x01\x00\x1f\ttail"));
    }
}
