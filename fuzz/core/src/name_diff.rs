//! name_diff: differential fuzz driver for adt/name — SHIPPED Rust
//! (`crates/backend/utils/adt/name`) vs verbatim vendored PostgreSQL 18.3 C
//! (csrc/pg_name_io.c; name.c/varlena.c/mbutils.c/wchar.c, upstream sha
//! 62d6c7d3df). Comparator planes: value bytes (full 64-byte NameData
//! images / exact i32 cmp magnitudes / bool results), error-vs-no-error,
//! and errcode class. Message text is out of scope. Any mismatch panics —
//! libFuzzer minimizes that into the divergence reproducer.
//!
//! Encoding posture (documented decision): the driver pins the database
//! encoding to UTF8 (PostgreSQL's default) on both sides — shipped Rust
//! `namein` delegates truncation to the `pg_mbcliplen` seam, production
//! installs `mbutils::pg_mbcliplen` (encoding-dispatched), and the vendored
//! C oracle runs the verbatim `pg_encoding_mbcliplen` with `pg_utf_mblen`.
//! So the 63-byte truncation arms exercise the REAL multibyte
//! char-boundary clip, not a single-byte simplification. (The single-byte
//! SQL_ASCII arm, min(len,limit) for NUL-free input, is pinned by the
//! proofs/name-ascii Kani harnesses instead.) The client encoding stays at
//! its SQL_ASCII default on both sides, so namesend conversion is the
//! identity and namerecv performs PG's mandatory no-conversion VALIDATION
//! of the wire bytes against the database encoding (pg_any_to_server:
//! 22021 on invalid UTF8; verifier vendored verbatim in the oracle).
//!
//! Scope carve (the name crate's carve of record): C-locale core only.
//! Every comparison runs under C_COLLATION_OID; the varstr_cmp locale path
//! (collation-dependent) is out of scope on both sides.
//!
//! fc-wrapper plane: every selector arm ALSO routes its input through the
//! crate's builtins.rs fmgr wrappers (fc_*) on a native LocalFcinfo frame
//! (the proofs/uuid wrapper-level pattern, run natively with a real
//! mcx::MemoryContext), asserting wrapper ≡ core (Datum value / result
//! bytes / error verdict+class). C parity keeps riding the core-vs-C
//! comparison; the fc plane pins the wrapper glue (arg decode, result
//! encode, scratch/mcx conventions). hashname/hashnameextended execute at
//! wrapper grain with in-harness oracles only (the seed-0 identity
//! `hash_bytes_extended(k,0) as u32 == hash_bytes(k)` plus determinism);
//! their hash-value C parity is owned by proofs/hash (`hashfn` is not a
//! fuzz/core dependency).
//!
//! Skipped, with reasons:
//!  - current_user/session_user/current_schema(s) and their fc_* wrappers
//!    (fc_current_user, fc_session_user, fc_current_schema,
//!    fc_current_schemas): excluded(state) — syscache/catalog state, not
//!    reachable purely.
//!  - btnamesortsupport: SortSupport plumbing (varstr_sortsupport), no
//!    pure entry point in the shipped crate and no fc_* wrapper in
//!    builtins.rs.

use std::ffi::{c_char, CStr, CString};
use std::sync::Once;

use datum::{Datum, NullableDatum};
use name::builtins as nb;
use types_core::C_COLLATION_OID;
use types_error::{PgError, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_NAME_TOO_LONG};
use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction, PackedVarlena};

extern "C" {
    fn pg_diff_namein(s: *const c_char, result: *mut u8) -> i32;
    fn pg_diff_nameout(name: *const u8, out: *mut u8) -> i32;
    fn pg_diff_namerecv(payload: *const u8, nbytes: i32, result: *mut u8) -> i32;
    fn pg_diff_nameeq(a: *const u8, b: *const u8) -> i32;
    fn pg_diff_namene(a: *const u8, b: *const u8) -> i32;
    fn pg_diff_namelt(a: *const u8, b: *const u8) -> i32;
    fn pg_diff_namele(a: *const u8, b: *const u8) -> i32;
    fn pg_diff_namegt(a: *const u8, b: *const u8) -> i32;
    fn pg_diff_namege(a: *const u8, b: *const u8) -> i32;
    fn pg_diff_btnamecmp(a: *const u8, b: *const u8) -> i32;
    fn pg_diff_namestrcpy(name: *mut u8, s: *const c_char);
    fn pg_diff_namestrcmp(name: *const u8, s: *const c_char) -> i32;
    fn pg_diff_nameconcatoid(nam: *const u8, oid: u32, result: *mut u8) -> i32;
    fn pg_diff_btnametextcmp(a: *const u8, t: *const u8, tl: i32) -> i32;
    fn pg_diff_bttextnamecmp(t: *const u8, tl: i32, a: *const u8) -> i32;
    fn pg_diff_nameeqtext(a: *const u8, t: *const u8, tl: i32) -> i32;
    fn pg_diff_namenetext(a: *const u8, t: *const u8, tl: i32) -> i32;
    fn pg_diff_namelttext(a: *const u8, t: *const u8, tl: i32) -> i32;
    fn pg_diff_nameletext(a: *const u8, t: *const u8, tl: i32) -> i32;
    fn pg_diff_namegetext(a: *const u8, t: *const u8, tl: i32) -> i32;
    fn pg_diff_namegttext(a: *const u8, t: *const u8, tl: i32) -> i32;
    fn pg_diff_texteqname(t: *const u8, tl: i32, a: *const u8) -> i32;
    fn pg_diff_textnename(t: *const u8, tl: i32, a: *const u8) -> i32;
    fn pg_diff_textltname(t: *const u8, tl: i32, a: *const u8) -> i32;
    fn pg_diff_textlename(t: *const u8, tl: i32, a: *const u8) -> i32;
    fn pg_diff_textgename(t: *const u8, tl: i32, a: *const u8) -> i32;
    fn pg_diff_textgtname(t: *const u8, tl: i32, a: *const u8) -> i32;
    fn pg_diff_text_name(s: *const u8, len: i32, result: *mut u8) -> i32;
    // _Thread_local errcode accessor (same pattern as diff.rs / pg_float_io.c).
    fn pg_diff_name_errcode_get() -> i32;
}

/// Oracle error classes (csrc/pg_name_io.c).
const C_ERR_NAME_TOO_LONG: i32 = 1; /* 42622 */
const C_ERR_NOT_IN_REPERTOIRE: i32 = 2; /* 22021 invalid byte sequence */

fn c_errcode() -> i32 {
    unsafe { pg_diff_name_errcode_get() }
}

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_NAME_TOO_LONG {
        C_ERR_NAME_TOO_LONG
    } else if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        C_ERR_NOT_IN_REPERTOIRE
    } else {
        99
    }
}

/// Cap driver payloads so the truncation arms are exercised constantly but
/// libFuzzer doesn't waste budget on megabyte inputs.
const MAX_TEXT: usize = 300;

// The shipped namein truncation goes through the pg_mbcliplen SEAM and
// namesend goes through the pg_server_to_client seam; install the REAL
// mbutils implementations (what production boot installs) once per process,
// and pin the thread's database encoding to UTF8 (thread-local Cell in
// mbutils; client encoding stays at its SQL_ASCII default => identity
// conversion). init_seams() is set-once per seam and panics on double
// install, so if another differential module in this crate ever installs
// mbutils seams first the catch_unwind keeps us alive; the encoding pin
// below is what this driver actually depends on.
fn setup() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        let _ = std::panic::catch_unwind(mbutils::init_seams);
    });
    std::thread_local! {
        static ENC_PINNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    ENC_PINNED.with(|c| {
        if !c.get() {
            mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("UTF8 is a valid be-encoding");
            c.set(true);
        }
    });
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing: one native fmgr call frame per wrapper
// invocation (deterministic; result allocations ride the caller's arena and
// die with it, no hot-loop leaks).
// ---------------------------------------------------------------------------

/// One native fmgr call: N by-value/by-ref arg Datums, optional armed result
/// mcx, explicit collation. Wrappers under test never return SQL NULL.
fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    coll: u32,
    mcx: Option<mcx::Mcx<'_>>,
    args: [Datum; N],
) -> types_error::PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(coll);
    if let Some(m) = mcx {
        // SAFETY: the arming context outlives this single call.
        unsafe { fcinfo.set_result_mcx(m) };
    }
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(flinfo, &mut fcinfo);
    if r.is_ok() {
        assert!(!fcinfo.isnull, "fc wrapper returned SQL NULL unexpectedly");
    }
    r
}

/// NAME arg convention: fixed 64-byte by-ref pointer.
fn name_datum(block: &[u8; 64]) -> Datum {
    Datum::from_usize(block.as_ptr() as usize)
}

/// text/varlena arg construction: inline 4B-uncompressed header + body
/// (the shipped set_varsize_4b_word encoding; body is capped by MAX_TEXT so
/// the length always fits).
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

/// By-ref NAME result readback (fc results are 64-byte blocks).
fn read_name(d: Datum) -> [u8; 64] {
    // SAFETY: fc name results point at live 64-byte blocks (fn_extra scratch
    // or byref_result copies) read before their owner drops.
    unsafe { *(d.as_usize() as *const [u8; 64]) }
}

/// Varlena result readback (text/bytea payload bytes).
fn read_varlena_data<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc varlena results are live inline images in the armed arena,
    // read before the arena drops.
    unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data()
}

/// Infallible bool-returning wrapper vs the already-C-checked core verdict.
fn fc_expect_bool(fname: &str, f: PGFunction, coll: u32, args: [Datum; 2], want: bool) {
    let d = fc_call(f, None, coll, None, args)
        .unwrap_or_else(|e| panic!("{fname} wrapper errored: {}", e.message));
    assert!(
        d.as_bool() == want,
        "{fname} fc-wrapper DIVERGENCE: wrapper={} core={want}",
        d.as_bool()
    );
}

// ---------------------------------------------------------------------------
// Input layout: [selector][payload]; selector % 6 picks the arm:
//   0 = namein cstring arm (NUL-free text; + nameout, namestrcmp, namestrcpy)
//   1 = name cmp family over two raw 64-byte blocks (C collation)
//   2 = name-vs-text cross ops: [nlen][name bytes][text bytes]
//   3 = nameconcatoid: [u32 oid le][name source bytes]
//   4 = namerecv/namesend/nameout payload paths (via real pqformat)
//   5 = text_name explicit-length arm (embedded NULs are data)
// ---------------------------------------------------------------------------

pub fn name_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    setup();
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 6 {
        0 => namein_arm(payload),
        1 => cmp_arm(payload),
        2 => nametext_arm(payload),
        3 => concatoid_arm(payload),
        4 => sendrecv_arm(payload),
        _ => text_name_arm(payload),
    }
}

/// namein over a cstring (NUL-free bytes, like real PG's datatype-input
/// path), plus nameout / namestrcmp / namestrcpy over the result.
fn namein_arm(payload: &[u8]) {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return;
    }
    let cs = CString::new(payload).unwrap();

    let mut cimg = [0u8; 64];
    let clen = unsafe { pg_diff_namein(cs.as_ptr(), cimg.as_mut_ptr()) };
    let r = name::namein(payload);
    assert!(
        r.data == cimg,
        "namein DIVERGENCE input={payload:?}: C(len={clen})={:?} Rust={:?}",
        &cimg[..],
        &r.data[..]
    );

    // nameout (cstring image: strlen prefix + NUL).
    let mut cout = [0u8; 65];
    let colen = unsafe { pg_diff_nameout(cimg.as_ptr(), cout.as_mut_ptr()) } as usize;
    let mut rbuf = Vec::new();
    name::nameout_into(&r, &mut rbuf);
    assert!(
        rbuf.len() == colen + 1 && rbuf[..colen] == cout[..colen] && rbuf[colen] == 0,
        "nameout DIVERGENCE input={payload:?}: C={:?} Rust={:?}",
        &cout[..colen],
        &rbuf[..]
    );

    // namestrcmp(name, original cstring) — C strncmp raw-difference plane.
    let cv = unsafe { pg_diff_namestrcmp(cimg.as_ptr(), cs.as_ptr()) };
    let rv = name::namestrcmp(Some(&r), Some(payload));
    assert!(
        cv == rv,
        "namestrcmp DIVERGENCE input={payload:?}: C={cv} Rust={rv}"
    );

    // namestrcpy (shipped entry takes &str: valid-UTF-8 inputs only).
    if let Ok(s) = std::str::from_utf8(payload) {
        let mut nd = name::namein(b"");
        name::namestrcpy(&mut nd, s);
        let mut cimg2 = [0u8; 64];
        unsafe { pg_diff_namestrcpy(cimg2.as_mut_ptr(), cs.as_ptr()) };
        assert!(
            nd.data == cimg2,
            "namestrcpy DIVERGENCE input={s:?}: C={:?} Rust={:?}",
            &cimg2[..],
            &nd.data[..]
        );
    }

    // ---- fc-wrapper plane (wrapper ≡ core) ----
    // namestrcmp NULL lattice: C name.c namestrcmp's defensive NULL arms,
    // oracled against the transcribed C semantics (0 / -1 / 1) plus the
    // Some/Some arm against the C strncmp shim result already checked above
    // for this input's parsed name.
    assert!(name::namestrcmp(None, None) == 0);
    assert!(name::namestrcmp(None, Some(payload)) == -1);
    assert!(name::namestrcmp(Some(&r), None) == 1);
    let nlen = r.data.iter().position(|&b| b == 0).unwrap_or(64);
    let selfcmp = name::namestrcmp(Some(&r), Some(&r.data[..nlen]));
    assert!(selfcmp == 0, "namestrcmp self-compare nonzero");

    // fc_namein: cstring arg; result rides the resolved FmgrInfo's scratch.
    let mut fl = FmgrInfo::unresolved();
    let din = fc_call(
        nb::fc_namein,
        Some(&mut fl),
        0,
        None,
        [Datum::from_usize(cs.as_ptr() as usize)],
    )
    .expect("fc_namein is infallible");
    assert!(
        read_name(din) == r.data,
        "fc_namein vs core DIVERGENCE input={payload:?}"
    );
    // Second call through the SAME resolved FmgrInfo: the retained-scratch
    // reuse arm (fn_extra already set — the varlena textin precedent).
    let din2 = fc_call(
        nb::fc_namein,
        Some(&mut fl),
        0,
        None,
        [Datum::from_usize(cs.as_ptr() as usize)],
    )
    .expect("fc_namein is infallible");
    assert!(
        read_name(din2) == r.data,
        "fc_namein scratch-reuse vs core DIVERGENCE input={payload:?}"
    );

    // fc_nameout: cstring result in the thread-local scratch.
    let dout = fc_call(nb::fc_nameout, None, 0, None, [name_datum(&r.data)])
        .expect("fc_nameout is infallible");
    // SAFETY: fc_nameout returns the live NUL-terminated TLS scratch.
    let out = unsafe { CStr::from_ptr(dout.as_usize() as *const c_char) };
    assert!(
        out.to_bytes_with_nul() == &rbuf[..],
        "fc_nameout vs core DIVERGENCE input={payload:?}"
    );

    // fc_name_text: NAME -> text (cstring_to_text of the NUL-stopped bytes).
    let cx = mcx::MemoryContext::new("name_fuzz_fc");
    let dtext = fc_call(nb::fc_name_text, None, 0, Some(cx.mcx()), [name_datum(&r.data)])
        .expect("fc_name_text allocation");
    assert!(
        read_varlena_data(dtext) == r.name_str(),
        "fc_name_text vs core DIVERGENCE input={payload:?}"
    );

    // fc_hashname / fc_hashnameextended: in-harness oracles only (seed-0
    // low-word identity + determinism); hash-value C parity is proofs/hash's.
    let h = fc_call(nb::fc_hashname, None, 0, None, [name_datum(&r.data)])
        .expect("fc_hashname is infallible")
        .as_u32();
    let h0 = fc_call(
        nb::fc_hashnameextended,
        None,
        0,
        None,
        [name_datum(&r.data), Datum::from_i64(0)],
    )
    .expect("fc_hashnameextended is infallible")
    .as_u64();
    assert!(
        h0 as u32 == h,
        "hashname/hashnameextended seed-0 identity broke input={payload:?}: h={h:#x} ext0={h0:#x}"
    );
    let mut sb = [0u8; 8];
    for (i, &x) in payload.iter().take(8).enumerate() {
        sb[i] = x;
    }
    let seed = Datum::from_u64(u64::from_le_bytes(sb));
    let h1 = fc_call(nb::fc_hashnameextended, None, 0, None, [name_datum(&r.data), seed])
        .expect("fc_hashnameextended is infallible")
        .as_u64();
    let h2 = fc_call(nb::fc_hashnameextended, None, 0, None, [name_datum(&r.data), seed])
        .expect("fc_hashnameextended is infallible")
        .as_u64();
    assert!(h1 == h2, "fc_hashnameextended nondeterministic input={payload:?}");
}

/// namelt/le/gt/ge/eq/ne + btnamecmp over two raw 64-byte name blocks under
/// C collation (the carve of record). Blocks are NOT forced to carry a NUL:
/// C strncmp(_,_,NAMEDATALEN) and the shipped word-at-a-time strncmp_name
/// are both total over the full 64 bytes, and the differential holds them
/// to identical results including the raw first-mismatch magnitude.
fn cmp_arm(payload: &[u8]) {
    if payload.len() < 128 {
        return;
    }
    let mut a = [0u8; 64];
    a.copy_from_slice(&payload[..64]);
    let mut b = [0u8; 64];
    b.copy_from_slice(&payload[64..128]);
    let mut nda = name::namein(b"");
    nda.data = a;
    let mut ndb = name::namein(b"");
    ndb.data = b;
    let coll = C_COLLATION_OID;

    let cases: [(&str, bool, i32); 6] = unsafe {
        [
            ("nameeq", name::nameeq(&nda, &ndb, coll).unwrap(), pg_diff_nameeq(a.as_ptr(), b.as_ptr())),
            ("namene", name::namene(&nda, &ndb, coll).unwrap(), pg_diff_namene(a.as_ptr(), b.as_ptr())),
            ("namelt", name::namelt(&nda, &ndb, coll).unwrap(), pg_diff_namelt(a.as_ptr(), b.as_ptr())),
            ("namele", name::namele(&nda, &ndb, coll).unwrap(), pg_diff_namele(a.as_ptr(), b.as_ptr())),
            ("namegt", name::namegt(&nda, &ndb, coll).unwrap(), pg_diff_namegt(a.as_ptr(), b.as_ptr())),
            ("namege", name::namege(&nda, &ndb, coll).unwrap(), pg_diff_namege(a.as_ptr(), b.as_ptr())),
        ]
    };
    for (fname, rres, cres) in cases {
        assert!(
            rres == (cres != 0),
            "{fname} DIVERGENCE a={a:?} b={b:?}: C={cres} Rust={rres}"
        );
    }
    let cres = unsafe { pg_diff_btnamecmp(a.as_ptr(), b.as_ptr()) };
    let rres = name::btnamecmp(&nda, &ndb, coll).unwrap();
    assert!(
        cres == rres,
        "btnamecmp DIVERGENCE a={a:?} b={b:?}: C={cres} Rust={rres}"
    );

    // ---- fc-wrapper plane (wrapper ≡ core, same collation) ----
    let (da, db) = (name_datum(&a), name_datum(&b));
    let fc_bools: [(&str, PGFunction); 6] = [
        ("fc_nameeq", nb::fc_nameeq),
        ("fc_namene", nb::fc_namene),
        ("fc_namelt", nb::fc_namelt),
        ("fc_namele", nb::fc_namele),
        ("fc_namegt", nb::fc_namegt),
        ("fc_namege", nb::fc_namege),
    ];
    for ((fname, f), (_, core, _)) in fc_bools.iter().zip(cases.iter()) {
        fc_expect_bool(fname, *f, coll, [da, db], *core);
    }
    let d = fc_call(nb::fc_btnamecmp, None, coll, None, [da, db]).expect("fc_btnamecmp");
    assert!(
        d.as_i32() == rres,
        "fc_btnamecmp fc-wrapper DIVERGENCE a={a:?} b={b:?}: wrapper={} core={rres}",
        d.as_i32()
    );

    // namestrcmp against b's NUL-truncated prefix as the C string.
    let bprefix = b.iter().position(|&x| x == 0).map_or(&b[..], |i| &b[..i]);
    let cs = CString::new(bprefix).unwrap();
    let cv = unsafe { pg_diff_namestrcmp(a.as_ptr(), cs.as_ptr()) };
    let rv = name::namestrcmp(Some(&nda), Some(bprefix));
    assert!(
        cv == rv,
        "namestrcmp DIVERGENCE a={a:?} str={bprefix:?}: C={cv} Rust={rv}"
    );
}

/// nameeqtext family + texteqname family + both cmp entry points, C
/// collation. Layout: [nlen % 64][name source bytes][text bytes]; the name
/// is namein-constructed (real names always are), the text is raw bytes.
fn nametext_arm(payload: &[u8]) {
    let Some((&nlen, rest)) = payload.split_first() else {
        return;
    };
    let nlen = (nlen as usize) % 64;
    if rest.len() < nlen || rest.len() - nlen > MAX_TEXT {
        return;
    }
    let nd = name::namein(&rest[..nlen]);
    let text = &rest[nlen..];
    let coll = C_COLLATION_OID;
    let (np, tp, tl) = (nd.data.as_ptr(), text.as_ptr(), text.len() as i32);

    let bool_cases: [(&str, bool, i32); 12] = unsafe {
        [
            ("nameeqtext", name::nameeqtext(&nd, text, coll).unwrap(), pg_diff_nameeqtext(np, tp, tl)),
            ("namenetext", name::namenetext(&nd, text, coll).unwrap(), pg_diff_namenetext(np, tp, tl)),
            ("namelttext", name::namelttext(&nd, text, coll).unwrap(), pg_diff_namelttext(np, tp, tl)),
            ("nameletext", name::nameletext(&nd, text, coll).unwrap(), pg_diff_nameletext(np, tp, tl)),
            ("namegetext", name::namegetext(&nd, text, coll).unwrap(), pg_diff_namegetext(np, tp, tl)),
            ("namegttext", name::namegttext(&nd, text, coll).unwrap(), pg_diff_namegttext(np, tp, tl)),
            ("texteqname", name::texteqname(text, &nd, coll).unwrap(), pg_diff_texteqname(tp, tl, np)),
            ("textnename", name::textnename(text, &nd, coll).unwrap(), pg_diff_textnename(tp, tl, np)),
            ("textltname", name::textltname(text, &nd, coll).unwrap(), pg_diff_textltname(tp, tl, np)),
            ("textlename", name::textlename(text, &nd, coll).unwrap(), pg_diff_textlename(tp, tl, np)),
            ("textgename", name::textgename(text, &nd, coll).unwrap(), pg_diff_textgename(tp, tl, np)),
            ("textgtname", name::textgtname(text, &nd, coll).unwrap(), pg_diff_textgtname(tp, tl, np)),
        ]
    };
    for (fname, rres, cres) in bool_cases {
        assert!(
            rres == (cres != 0),
            "{fname} DIVERGENCE name={:?} text={text:?}: C={cres} Rust={rres}",
            nd.name_str()
        );
    }
    let cres = unsafe { pg_diff_btnametextcmp(np, tp, tl) };
    let rres = name::btnametextcmp(&nd, text, coll).unwrap();
    assert!(
        cres == rres,
        "btnametextcmp DIVERGENCE name={:?} text={text:?}: C={cres} Rust={rres}",
        nd.name_str()
    );
    let cres = unsafe { pg_diff_bttextnamecmp(tp, tl, np) };
    let rres = name::bttextnamecmp(text, &nd, coll).unwrap();
    assert!(
        cres == rres,
        "bttextnamecmp DIVERGENCE text={text:?} name={:?}: C={cres} Rust={rres}",
        nd.name_str()
    );

    // ---- fc-wrapper plane (wrapper ≡ core, same collation) ----
    // Text arg: inline 4B-header varlena over the same bytes; NAME arg: the
    // same namein-built 64-byte block by pointer.
    let timg = text_image(text);
    let (dn, dt) = (name_datum(&nd.data), Datum::from_usize(timg.as_ptr() as usize));
    let fc_bools: [(&str, PGFunction, [Datum; 2]); 12] = [
        ("fc_nameeqtext", nb::fc_nameeqtext, [dn, dt]),
        ("fc_namenetext", nb::fc_namenetext, [dn, dt]),
        ("fc_namelttext", nb::fc_namelttext, [dn, dt]),
        ("fc_nameletext", nb::fc_nameletext, [dn, dt]),
        ("fc_namegetext", nb::fc_namegetext, [dn, dt]),
        ("fc_namegttext", nb::fc_namegttext, [dn, dt]),
        ("fc_texteqname", nb::fc_texteqname, [dt, dn]),
        ("fc_textnename", nb::fc_textnename, [dt, dn]),
        ("fc_textltname", nb::fc_textltname, [dt, dn]),
        ("fc_textlename", nb::fc_textlename, [dt, dn]),
        ("fc_textgename", nb::fc_textgename, [dt, dn]),
        ("fc_textgtname", nb::fc_textgtname, [dt, dn]),
    ];
    for ((fname, f, args), (_, core, _)) in fc_bools.iter().zip(bool_cases.iter()) {
        fc_expect_bool(fname, *f, coll, *args, *core);
    }
    let d = fc_call(nb::fc_btnametextcmp, None, coll, None, [dn, dt]).expect("fc_btnametextcmp");
    let core = name::btnametextcmp(&nd, text, coll).unwrap();
    assert!(
        d.as_i32() == core,
        "fc_btnametextcmp fc-wrapper DIVERGENCE text={text:?}: wrapper={} core={core}",
        d.as_i32()
    );
    let d = fc_call(nb::fc_bttextnamecmp, None, coll, None, [dt, dn]).expect("fc_bttextnamecmp");
    assert!(
        d.as_i32() == rres,
        "fc_bttextnamecmp fc-wrapper DIVERGENCE text={text:?}: wrapper={} core={rres}",
        d.as_i32()
    );
}

/// nameconcatoid: `_{oid}` suffix, truncating the NAME part (mbcliplen'd)
/// never the suffix. Layout: [u32 oid le][name source bytes].
fn concatoid_arm(payload: &[u8]) {
    if payload.len() < 4 || payload.len() - 4 > MAX_TEXT {
        return;
    }
    let oid = u32::from_le_bytes(payload[..4].try_into().unwrap());
    let src = &payload[4..];
    let nd = name::namein(src);
    let r = name::nameconcatoid(&nd, oid);

    let mut cnam = [0u8; 64];
    let mut cres = [0u8; 64];
    unsafe {
        // Same explicit-length namein core the Rust side used (src may
        // contain NULs; both sides then work from the strlen prefix).
        pg_diff_text_name(src.as_ptr(), src.len() as i32, cnam.as_mut_ptr());
        pg_diff_nameconcatoid(cnam.as_ptr(), oid, cres.as_mut_ptr());
    }
    assert!(
        nd.data == cnam,
        "nameconcatoid(namein) DIVERGENCE src={src:?}: C={:?} Rust={:?}",
        &cnam[..],
        &nd.data[..]
    );
    assert!(
        r.data == cres,
        "nameconcatoid DIVERGENCE name={:?} oid={oid}: C={:?} Rust={:?}",
        nd.name_str(),
        &cres[..],
        &r.data[..]
    );

    // ---- fc-wrapper plane (wrapper ≡ core) ----
    let cx = mcx::MemoryContext::new("name_fuzz_fc");
    let d = fc_call(
        nb::fc_nameconcatoid,
        None,
        0,
        Some(cx.mcx()),
        [name_datum(&nd.data), Datum::from_oid(oid)],
    )
    .expect("fc_nameconcatoid allocation");
    assert!(
        read_name(d) == r.data,
        "fc_nameconcatoid vs core DIVERGENCE name={:?} oid={oid}",
        nd.name_str()
    );
}

/// namerecv (wire payload of any length incl. the >=64 ereport arm, through
/// the real pqformat/StringInfo machinery) and namesend/nameout (payload =
/// strlen prefix; identity conversion, see module doc).
fn sendrecv_arm(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let cx = mcx::MemoryContext::new("name_fuzz");
    let mcx = cx.mcx();

    // recv plane
    let mut si = match stringinfo::StringInfo::new_in(mcx) {
        Ok(s) => s,
        Err(_) => return,
    };
    if si.append_bytes(payload).is_err() {
        return;
    }
    let mut cimg = [0u8; 64];
    let cres = unsafe { pg_diff_namerecv(payload.as_ptr(), payload.len() as i32, cimg.as_mut_ptr()) };
    let cerr = c_errcode();
    match name::namerecv(mcx, &mut si) {
        Ok(nd) => assert!(
            cres >= 0 && cerr == 0 && nd.data == cimg,
            "namerecv DIVERGENCE payload={payload:?}: C=(res {cres}, err {cerr}, {:?}) Rust=Ok({:?})",
            &cimg[..],
            &nd.data[..]
        ),
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cres == -1 && cerr == rerr,
                "namerecv DIVERGENCE payload={payload:?}: C=(res {cres}, err {cerr}) Rust=Err({rerr} {})",
                e.message
            );
        }
    }

    // fc_namerecv over its own StringInfo image of the same wire payload
    // (wrapper ≡ C-checked outcome ≡ core).
    let mut si2 = match stringinfo::StringInfo::new_in(mcx) {
        Ok(s) => s,
        Err(_) => return,
    };
    if si2.append_bytes(payload).is_err() {
        return;
    }
    let dsi = Datum::from_usize(core::ptr::from_mut(&mut si2) as usize);
    match fc_call(nb::fc_namerecv, None, 0, Some(mcx), [dsi]) {
        Ok(d) => assert!(
            cres >= 0 && cerr == 0 && read_name(d) == cimg,
            "fc_namerecv DIVERGENCE payload={payload:?}: C=(res {cres}, err {cerr})"
        ),
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cres == -1 && cerr == rerr,
                "fc_namerecv DIVERGENCE payload={payload:?}: C=(res {cres}, err {cerr}) wrapper=Err({rerr} {})",
                e.message
            );
        }
    }

    // send/out plane over a namein-built name (cstring contract: NUL-free).
    if !payload.contains(&0) {
        let nd = name::namein(payload);
        let mut cout = [0u8; 65];
        let clen = unsafe { pg_diff_nameout(nd.data.as_ptr(), cout.as_mut_ptr()) } as usize;
        let bytea = name::namesend(mcx, &nd).expect("namesend is infallible on identity encoding");
        assert!(
            bytea.data() == &cout[..clen],
            "namesend DIVERGENCE input={payload:?}: C={:?} Rust={:?}",
            &cout[..clen],
            bytea.data()
        );
        let ov = name::nameout(mcx, &nd).expect("nameout allocation");
        assert!(
            &ov[..] == &cout[..clen],
            "nameout(mcx) DIVERGENCE input={payload:?}: C={:?} Rust={:?}",
            &cout[..clen],
            &ov[..]
        );

        // fc_namesend (wrapper ≡ core bytea image).
        let d = fc_call(nb::fc_namesend, None, 0, Some(mcx), [name_datum(&nd.data)])
            .expect("fc_namesend is infallible on identity encoding");
        assert!(
            read_varlena_data(d) == bytea.data(),
            "fc_namesend vs core DIVERGENCE input={payload:?}"
        );
    }
}

/// text_name's clip core: namein over an explicit-length payload — embedded
/// NULs are data (C text_name never strlen-walks), and pg_mbcliplen itself
/// NUL-stops during truncation. Differential over arbitrary bytes.
fn text_name_arm(payload: &[u8]) {
    if payload.len() > MAX_TEXT {
        return;
    }
    let r = name::namein(payload);
    let mut cimg = [0u8; 64];
    let clen = unsafe { pg_diff_text_name(payload.as_ptr(), payload.len() as i32, cimg.as_mut_ptr()) };
    assert!(
        r.data == cimg,
        "text_name DIVERGENCE input={payload:?}: C(len={clen})={:?} Rust={:?}",
        &cimg[..],
        &r.data[..]
    );

    // ---- fc-wrapper plane: fc_text_name over the same explicit-length
    // bytes as a real text varlena (wrapper ≡ core) ----
    let cx = mcx::MemoryContext::new("name_fuzz_fc");
    let timg = text_image(payload);
    let d = fc_call(
        nb::fc_text_name,
        None,
        0,
        Some(cx.mcx()),
        [Datum::from_usize(timg.as_ptr() as usize)],
    )
    .expect("fc_text_name allocation");
    assert!(
        read_name(d) == r.data,
        "fc_text_name vs core DIVERGENCE input={payload:?}"
    );
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke: replay the seed grid through every arm so
// `cargo test` exercises the C link + comparators without cargo-fuzz.
// gen-style corpus written to ../corpus/name_diff mirrors these shapes.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(sel: u8, payload: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(payload);
        name_diff(&d);
    }

    /// regress-style identifier corpus: short, exactly-63, >63 truncating,
    /// empty, high-bit bytes, multibyte boundaries.
    fn ident_corpus() -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"pg_class".to_vec(),
            b"PG_Class".to_vec(),
            b"_x1".to_vec(),
            b"a b\tc".to_vec(),
            vec![b'a'; 62],
            vec![b'a'; 63],
            vec![b'a'; 64],
            vec![b'x'; 100],
            vec![b'z'; 300],
            vec![0x80; 70],
            vec![0xff; 70],
            vec![0xf8; 70], // invalid UTF-8 lead: mblen 1 on both sides
        ];
        // 2-byte char straddling the 63-byte boundary: 31*"é" = 62 bytes,
        // then one more clips to 62 not 63.
        v.push("é".repeat(40).into_bytes());
        v.push("é".repeat(31).into_bytes());
        // 3- and 4-byte chars near the boundary.
        v.push("\u{20ac}".repeat(25).into_bytes()); // 75 bytes of 3-byte €
        v.push("\u{1f409}".repeat(20).into_bytes()); // 80 bytes of 4-byte 🐉
        let mut mixed = vec![b'q'; 61];
        mixed.extend_from_slice("é".as_bytes()); // 63 exactly, boundary-clean
        v.push(mixed);
        let mut mixed = vec![b'q'; 62];
        mixed.extend_from_slice("é".as_bytes()); // char straddles 63
        v.push(mixed);
        // truncated multibyte at end of a short (non-clipping) input
        v.push(vec![0xc3]);
        v.push(vec![0xe2, 0x82]);
        v
    }

    #[test]
    fn namein_arm_corpus() {
        let _serial = crate::c_oracle_serial();
        for id in ident_corpus() {
            drive(0, &id);
            drive(4, &id); // send/recv over the same shapes
            drive(5, &id); // text_name explicit-length
        }
    }

    #[test]
    fn recv_length_edges_and_error_plane() {
        let _serial = crate::c_oracle_serial();
        for n in [0usize, 1, 62, 63, 64, 65, 100, 300] {
            drive(4, &vec![b'n'; n]); // >=64 → 42622 on both sides
        }
        // embedded NUL is data on the wire
        drive(4, b"ab\0cd");
        let mut long = vec![b'a'; 63];
        long[10] = 0;
        drive(4, &long);
    }

    #[test]
    fn cmp_arm_corpus() {
        let _serial = crate::c_oracle_serial();
        let mk = |a: &[u8], b: &[u8]| {
            let mut p = [0u8; 128];
            p[..a.len().min(64)].copy_from_slice(&a[..a.len().min(64)]);
            p[64..64 + b.len().min(64)].copy_from_slice(&b[..b.len().min(64)]);
            p
        };
        drive(1, &mk(b"abc", b"abd"));
        drive(1, &mk(b"abc", b"abc"));
        drive(1, &mk(b"ab", b"abc")); // prefix: NUL vs 'c' raw magnitude
        drive(1, &mk(b"a", b"c")); // raw magnitude -2 (C 18.3 ground truth)
        drive(1, &mk(b"a\xff", b"az")); // unsigned compare
        drive(1, &mk(b"", b"x"));
        drive(1, &mk(&[b'a'; 64], &[b'a'; 64])); // no NUL anywhere
        drive(1, &mk(&[b'a'; 64], &[b'a'; 63])); // NUL only at b[63]
        let mut garbage = mk(b"same", b"same");
        garbage[10] = 0xde; // past-NUL garbage is irrelevant on both sides
        garbage[64 + 10] = 0xad;
        drive(1, &garbage);
        // word-at-a-time edges: mismatch and NUL in the same 8-byte word
        drive(1, &mk(b"aaaaaaa", b"aaaaaaaz"));
        drive(1, &mk(b"aaaaaaaa", b"aaaaaaab"));
    }

    #[test]
    fn nametext_arm_corpus() {
        let _serial = crate::c_oracle_serial();
        let mk = |name: &[u8], text: &[u8]| {
            let mut p = vec![name.len() as u8];
            p.extend_from_slice(name);
            p.extend_from_slice(text);
            p
        };
        drive(2, &mk(b"alpha", b"alpha"));
        drive(2, &mk(b"alpha", b"alphab")); // length tie-break => ±1
        drive(2, &mk(b"alphab", b"alpha"));
        drive(2, &mk(b"a", b"c")); // raw memcmp magnitude
        drive(2, &mk(b"", b""));
        drive(2, &mk(b"", b"x"));
        drive(2, &mk(b"x", b""));
        drive(2, &mk(b"nul", b"nu\0l")); // NUL inside text is data
        drive(2, &mk(b"a\xffz", b"a\xefz"));
        drive(2, &mk(&[b'n'; 63], &[b'n'; 300])); // name-len cap vs long text
    }

    #[test]
    fn concatoid_arm_corpus() {
        let _serial = crate::c_oracle_serial();
        let mk = |oid: u32, name: &[u8]| {
            let mut p = oid.to_le_bytes().to_vec();
            p.extend_from_slice(name);
            p
        };
        drive(3, &mk(0, b"f"));
        drive(3, &mk(1234, b"func"));
        drive(3, &mk(u32::MAX, &[b'n'; 63])); // max suffix, max name → clip
        drive(3, &mk(9, &[b'n'; 63]));
        drive(3, &mk(u32::MAX, b""));
        drive(3, &mk(4096, "é".repeat(40).as_bytes())); // multibyte clip vs suffix
        let mut boundary = vec![b'q'; 50];
        boundary.extend_from_slice("é".as_bytes()); // char at the clip edge
        drive(3, &mk(123456789, &boundary));
    }

    /// namestrcmp NULL-argument lattice (not expressible through the byte
    /// driver): direct dispatch-grain check against the C oracle.
    #[test]
    fn namestrcmp_null_lattice() {
        let _serial = crate::c_oracle_serial();
        setup();
        assert_eq!(name::namestrcmp(None, None), unsafe {
            pg_diff_namestrcmp(std::ptr::null(), std::ptr::null())
        });
        let n = name::namein(b"x");
        let cs = CString::new("x").unwrap();
        assert_eq!(name::namestrcmp(None, Some(b"x")), unsafe {
            pg_diff_namestrcmp(std::ptr::null(), cs.as_ptr())
        });
        assert_eq!(name::namestrcmp(Some(&n), None), unsafe {
            pg_diff_namestrcmp(n.data.as_ptr(), std::ptr::null())
        });
        assert_eq!(name::namestrcmp(Some(&n), Some(b"x")), unsafe {
            pg_diff_namestrcmp(n.data.as_ptr(), cs.as_ptr())
        });
    }

    /// Ground-truth pins from C 18.3 (fnconf batch-1 cmp-magnitude family):
    /// the raw strncmp magnitude is SQL-visible through btnamecmp.
    #[test]
    fn btnamecmp_magnitude_pins() {
        let _serial = crate::c_oracle_serial();
        setup();
        let a = name::namein(b"a");
        let c = name::namein(b"c");
        assert_eq!(name::btnamecmp(&a, &c, C_COLLATION_OID).unwrap(), -2);
        assert_eq!(unsafe { pg_diff_btnamecmp(a.data.as_ptr(), c.data.as_ptr()) }, -2);
        let ab = name::namein(b"ab");
        let abc = name::namein(b"abc");
        assert_eq!(name::btnamecmp(&ab, &abc, C_COLLATION_OID).unwrap(), -99);
        assert_eq!(
            unsafe { pg_diff_btnamecmp(ab.data.as_ptr(), abc.data.as_ptr()) },
            -99
        );
    }

    /// Multibyte truncation pin: 40 "é" (80 bytes) clips to 62 bytes on a
    /// char boundary under UTF8 on BOTH sides (not 63).
    #[test]
    fn utf8_clip_lands_on_char_boundary() {
        let _serial = crate::c_oracle_serial();
        setup();
        let s = "é".repeat(40);
        let r = name::namein(s.as_bytes());
        assert_eq!(r.name_str().len(), 62);
        let cs = CString::new(s.as_bytes()).unwrap();
        let mut cimg = [0u8; 64];
        let clen = unsafe { pg_diff_namein(cs.as_ptr(), cimg.as_mut_ptr()) };
        assert_eq!(clen, 62);
        assert_eq!(r.data, cimg);
    }

    /// fc-wrapper plane smoke: every new fc arm executes over seeds — arm 0
    /// (fc_namein/out/name_text/hashname/hashnameextended), arm 1 (the six
    /// fc name cmps + fc_btnamecmp), arm 2 (the twelve name↔text fc ops +
    /// both fc cmp entry points), arm 3 (fc_nameconcatoid), arm 4
    /// (fc_namerecv Ok + Err + fc_namesend), arm 5 (fc_text_name).
    #[test]
    fn fc_wrapper_plane_smoke() {
        let _serial = crate::c_oracle_serial();
        for id in ident_corpus() {
            drive(0, &id);
            drive(4, &id);
            drive(5, &id);
        }
        // cmp arm needs two 64-byte blocks
        let mut p = [0u8; 128];
        p[..3].copy_from_slice(b"abc");
        p[64..67].copy_from_slice(b"abd");
        drive(1, &p);
        drive(1, &[0x5a; 128]); // no NUL anywhere
        // name-vs-text arm
        let mut q = vec![5u8];
        q.extend_from_slice(b"alpha");
        q.extend_from_slice(b"alphab");
        drive(2, &q);
        drive(2, &[0]); // empty name, empty text
        // concatoid arm
        let mut c = 123456789u32.to_le_bytes().to_vec();
        c.extend_from_slice(&[b'n'; 63]);
        drive(3, &c);
        // recv error plane (>=64 wire payload -> 42622 through the wrapper too)
        drive(4, &vec![b'e'; 80]);
        drive(4, &[0xf8; 10]); // invalid UTF-8 -> 22021 through the wrapper too
    }

    /// Fuzz-shaped byte soup through every selector: the whole driver must
    /// be panic-free on arbitrary input (asserts fire only on divergence).
    #[test]
    fn selector_soup() {
        let _serial = crate::c_oracle_serial();
        for sel in 0u8..12 {
            for len in [0usize, 1, 3, 63, 64, 65, 128, 129, 200] {
                let payload: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(37).wrapping_add(sel)).collect();
                drive(sel, &payload);
            }
        }
        name_diff(&[]);
    }
}
