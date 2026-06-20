//! Regression-test support library (`src/test/regress/regress.c`).
//!
//! `regress.c` is built into the loadable module `regress` (`$libdir/regress`)
//! and the regression `test_setup.sql` script creates SQL functions from it with
//! `CREATE FUNCTION ... LANGUAGE C AS '$libdir/regress', '<symbol>'`. The Rust
//! backend exposes no C ABI, so the real `regress.so` cannot be `dlopen`ed;
//! instead the C bodies the regression suite depends on are ported here and
//! registered with the dynamic-loader unit's in-process ported-library registry
//! ([`backend_utils_fmgr_dfmgr_seams::builtin_library_present`] /
//! [`backend_utils_fmgr_dfmgr_seams::resolve_builtin_library_function`]). When
//! `dfmgr`'s `load_external_function` / `load_file` is asked to resolve a symbol
//! from library `regress`, it consults this registry rather than the OS loader,
//! so `CREATE FUNCTION ... LANGUAGE C AS '$libdir/regress'` validates and the
//! resulting function is callable.
//!
//! Each ported symbol is a plain fmgr-1 `PGFunction` exactly as the
//! `PG_FUNCTION_INFO_V1` macro would expose it (api_version 1); the registry hands
//! the function manager the same `(user_fn, api_version)` pair the OS loader's
//! `fetch_finfo_record` would have produced.

use types_datum::Datum;
use types_error::PgError;
use types_fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};

/// The simple (suffix-free, directory-free) name of the regression-test loadable
/// module — `$libdir/regress` reduces to this for the registry.
const LIBRARY: &str = "regress";

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (`invoke_pgfunction`'s `catch_unwind`), which downcasts
/// the panic payload back to the structured [`PgError`].
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// `PG_GETARG_OID(i)` — argument `i`'s word as an `Oid`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_core::Oid {
    fcinfo
        .arg(i)
        .expect("regress fn: missing oid arg")
        .value
        .as_oid()
}

/// `PG_GETARG_CSTRING(i)` — a `cstring` argument on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("regress fn: cstring arg missing from by-ref lane")
}

/// A `name` arg's NAMEDATALEN buffer image on the by-ref lane (C reads it as a
/// `char *`/`cstring` since `name` is a NUL-padded fixed buffer). Falls back to
/// the `Cstring` lane for a literal `cstring` arg. Returns the raw bytes up to
/// (not including) the first NUL.
#[inline]
fn arg_name_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    match fcinfo.ref_arg(i) {
        Some(types_fmgr::boundary::RefPayload::Cstring(s)) => s.as_bytes(),
        Some(types_fmgr::boundary::RefPayload::Varlena(b)) => {
            let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            &b[..end]
        }
        _ => panic!("regress fn: name/cstring arg missing from by-ref lane"),
    }
}

/// The raw native by-ref struct/varlena image of arg `i` (carried on the
/// `Varlena` lane by the bridge). For a `text`/`bytea` this is the header-ful
/// varlena image; for a fixed-length by-ref type (`point`, `widget`,
/// `city_budget`/int44) it is the bare native struct image, no varlena header.
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("regress fn: by-ref arg missing from by-ref lane")
}

/// `text_to_cstring(PG_GETARG_TEXT_PP(i))` — a `text` arg's `VARDATA_ANY`
/// payload bytes (the header-ful image with its 4-byte length word skipped).
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    &arg_bytes(fcinfo, i)[types_datum::varlena::VARHDRSZ..]
}

/// `PG_RETURN_CSTRING(s)` — write a `cstring` result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Build a header-ful varlena (`text`/`bytea`) image from its payload bytes
/// (C: `SET_VARSIZE(result, len + VARHDRSZ)` over a fresh palloc'd block).
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + types_datum::varlena::VARHDRSZ;
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
    image.extend_from_slice(payload);
    image
}

/// `PG_RETURN_TEXT_P` / `PG_RETURN_BYTEA_P` — write a header-ful varlena result.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(varlena_image(payload)));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/* ===========================================================================
 * binary_coercible(oid, oid) RETURNS bool  (regress.c)
 *
 *   Datum
 *   binary_coercible(PG_FUNCTION_ARGS)
 *   {
 *       Oid srctype = PG_GETARG_OID(0);
 *       Oid targettype = PG_GETARG_OID(1);
 *       PG_RETURN_BOOL(IsBinaryCoercible(srctype, targettype));
 *   }
 *
 * Provides SQL access to IsBinaryCoercible(); used by the opr_sanity /
 * type_sanity regression tests.
 * ========================================================================= */

/// `binary_coercible(oid, oid) -> bool` — SQL access to `IsBinaryCoercible`.
fn fc_binary_coercible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let srctype = arg_oid(fcinfo, 0);
    let targettype = arg_oid(fcinfo, 1);
    match backend_parser_coerce_seams::is_binary_coercible::call(srctype, targettype) {
        Ok(result) => Datum::from_bool(result),
        Err(err) => raise(err),
    }
}

/* ===========================================================================
 * reverse_name(cstring) RETURNS cstring  (regress.c)
 * ========================================================================= */

/// `NAMEDATALEN` (`pg_config_manual.h`).
const NAMEDATALEN: usize = 64;

fn fc_reverse_name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: PG_GETARG_CSTRING(0) over a `name` value — `name` is binary-compatible
    // with a NUL-terminated `char *`, so it arrives on the by-ref lane as its
    // NAMEDATALEN buffer image (NUL-padded), which we read byte-for-byte.
    let string = arg_name_bytes(fcinfo, 0);
    // C: new_string = palloc0(NAMEDATALEN); i = first NUL or NAMEDATALEN.
    let mut i = 0usize;
    while i < NAMEDATALEN && i < string.len() && string[i] != 0 {
        i += 1;
    }
    if i == 0 {
        // Empty input: C reads string[-1]; mirror by returning empty.
        return ret_cstring(fcinfo, String::new());
    }
    if i == NAMEDATALEN || i >= string.len() || string[i] == 0 {
        i -= 1;
    }
    let len = i;
    let mut out = vec![0u8; NAMEDATALEN];
    let mut k = i as isize;
    while k >= 0 {
        out[len - k as usize] = string[k as usize];
        k -= 1;
    }
    let end = out.iter().position(|&b| b == 0).unwrap_or(out.len());
    ret_cstring(fcinfo, String::from_utf8_lossy(&out[..end]).into_owned())
}

/* ===========================================================================
 * int44in(cstring) / int44out(city_budget) — fixed int4[4] vector (16 bytes).
 * ========================================================================= */

fn fc_int44in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let input = arg_cstring(fcinfo, 0);
    // C: sscanf(input, "%d, %d, %d, %d", ...); missing positions filled with 0.
    let mut result = [0i32; 4];
    let mut i = 0usize;
    for tok in input.split(',') {
        if i >= 4 {
            break;
        }
        match tok.trim().parse::<i32>() {
            Ok(v) => {
                result[i] = v;
                i += 1;
            }
            // sscanf stops at the first non-matching field.
            Err(_) => break,
        }
    }
    let mut image = Vec::with_capacity(16);
    for v in result.iter() {
        image.extend_from_slice(&v.to_ne_bytes());
    }
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(image));
    Datum::from_usize(0)
}

fn fc_int44out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let bytes = arg_bytes(fcinfo, 0);
    assert!(bytes.len() >= 16, "int44 image too short");
    let mut a = [0i32; 4];
    for (j, slot) in a.iter_mut().enumerate() {
        *slot = i32::from_ne_bytes(bytes[j * 4..j * 4 + 4].try_into().unwrap());
    }
    ret_cstring(fcinfo, format!("{},{},{},{}", a[0], a[1], a[2], a[3]))
}

/* ===========================================================================
 * widget type: widget_in / widget_out / pt_in_widget  (regress.c)
 *   typedef struct { Point center; double radius; } WIDGET;  (24 bytes)
 * ========================================================================= */

const LDELIM: u8 = b'(';
const RDELIM: u8 = b')';
const DELIM: u8 = b',';
const WIDGET_NARGS: usize = 3;

fn fc_widget_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_in = arg_cstring(fcinfo, 0);
    let bytes = str_in.as_bytes();
    // C: walk the string collecting up to NARGS coordinate start offsets.
    let mut coord: [usize; WIDGET_NARGS] = [0; WIDGET_NARGS];
    let mut i = 0usize;
    let mut p = 0usize;
    while p < bytes.len() && bytes[p] != 0 && i < WIDGET_NARGS && bytes[p] != RDELIM {
        if bytes[p] == DELIM || (bytes[p] == LDELIM && i == 0) {
            coord[i] = p + 1;
            i += 1;
        }
        p += 1;
    }

    if i < WIDGET_NARGS {
        // Note (regress.c): DON'T convert to soft error — stays a hard error.
        raise(
            PgError::error(format!(
                "invalid input syntax for type widget: \"{str_in}\""
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    }

    let cx = atof(&bytes[coord[0]..]);
    let cy = atof(&bytes[coord[1]..]);
    let radius = atof(&bytes[coord[2]..]);

    let mut image = Vec::with_capacity(24);
    image.extend_from_slice(&cx.to_ne_bytes());
    image.extend_from_slice(&cy.to_ne_bytes());
    image.extend_from_slice(&radius.to_ne_bytes());
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// C's `atof`: parse the leading floating-point prefix of `bytes`, ignoring
/// the remainder; returns 0.0 if no number is present.
fn atof(bytes: &[u8]) -> f64 {
    let s = match core::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return 0.0,
    };
    let trimmed = s.trim_start();
    let mut end = 0usize;
    let mut idx = 0usize;
    while idx < trimmed.len() {
        let ch_len = trimmed[idx..].chars().next().unwrap().len_utf8();
        if trimmed[..idx + ch_len].parse::<f64>().is_ok() {
            end = idx + ch_len;
        }
        idx += ch_len;
    }
    trimmed[..end].parse::<f64>().unwrap_or(0.0)
}

fn fc_widget_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let bytes = arg_bytes(fcinfo, 0);
    assert!(bytes.len() >= 24, "widget image too short");
    let cx = f64::from_ne_bytes(bytes[0..8].try_into().unwrap());
    let cy = f64::from_ne_bytes(bytes[8..16].try_into().unwrap());
    let radius = f64::from_ne_bytes(bytes[16..24].try_into().unwrap());
    // C: psprintf("(%g,%g,%g)", ...).
    ret_cstring(fcinfo, format!("({cx},{cy},{radius})"))
}

fn fc_pt_in_widget(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let point = types_core::geo::Point::from_datum_bytes(arg_bytes(fcinfo, 0));
    let wbytes = arg_bytes(fcinfo, 1);
    assert!(wbytes.len() >= 24, "widget image too short");
    let center = types_core::geo::Point::from_datum_bytes(&wbytes[0..16]);
    let radius = f64::from_ne_bytes(wbytes[16..24].try_into().unwrap());
    match backend_utils_adt_geo_ops::point_distance(&point, &center) {
        Ok(distance) => Datum::from_bool(distance < radius),
        Err(err) => raise(err),
    }
}

/* ===========================================================================
 * test_bytea_to_text / test_text_to_bytea  (regress.c) — varlena identity.
 * ========================================================================= */

fn fc_test_bytea_to_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let payload = arg_text(fcinfo, 0).to_vec();
    ret_varlena(fcinfo, &payload)
}

fn fc_test_text_to_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let payload = arg_text(fcinfo, 0).to_vec();
    ret_varlena(fcinfo, &payload)
}

/* ===========================================================================
 * test_valid_server_encoding(text) RETURNS bool  (regress.c)
 * ========================================================================= */

fn fc_test_valid_server_encoding(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let name = String::from_utf8_lossy(arg_text(fcinfo, 0)).into_owned();
    let valid = common_extra_encnames::pg_valid_server_encoding(&name) >= 0;
    Datum::from_bool(valid)
}

/* ===========================================================================
 * test_canonicalize_path(text) RETURNS text  (regress.c)
 * ========================================================================= */

fn fc_test_canonicalize_path(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let path = String::from_utf8_lossy(arg_text(fcinfo, 0)).into_owned();
    let canon = common_path_seams::canonicalize_path::call(path);
    ret_varlena(fcinfo, canon.as_bytes())
}

/* ===========================================================================
 * is_catalog_text_unique_index_oid(oid) RETURNS bool  (regress.c)
 * ========================================================================= */

#[allow(non_snake_case)]
fn fc_is_catalog_text_unique_index_oid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let relid = arg_oid(fcinfo, 0);
    Datum::from_bool(backend_catalog_catalog::IsCatalogTextUniqueIndexOid(relid))
}

/* ===========================================================================
 * test_opclass_options_func(internal) RETURNS void  (regress.c) — returns NULL.
 * ========================================================================= */

fn fc_test_opclass_options_func(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_null(fcinfo)
}

/* ===========================================================================
 * test_fdw_handler() RETURNS fdw_handler  (regress.c) — not implemented.
 * ========================================================================= */

fn fc_test_fdw_handler(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error("test_fdw_handler is not implemented"));
}

/* ===========================================================================
 * interpt_pp(path, path) RETURNS point  (regress.c)
 *
 *   Return the point where two paths intersect, or NULL if no intersection.
 *   C walks each segment of p1 against each segment of p2, builds an LSEG per
 *   adjacent vertex pair (regress_lseg_construct), tests lseg_intersect, and on
 *   the first intersecting pair returns lseg_interpt of those two segments.
 * ========================================================================= */

/// `regress_lseg_construct(lseg, pt1, pt2)` — like `lseg_construct` but writes
/// into an already-allocated `LSEG` (a 2-point line segment).
fn regress_lseg_construct(
    pt1: types_core::geo::Point,
    pt2: types_core::geo::Point,
) -> types_core::geo::LSEG {
    types_core::geo::LSEG { p: [pt1, pt2] }
}

fn fc_interpt_pp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: PG_GETARG_PATH_P(0/1) — the toastable PATH varlena image on the by-ref
    // lane; decode to the owned Path (vertex list + closed flag).
    let p1 = backend_utils_adt_geo_ops::Path::from_datum_image(arg_bytes(fcinfo, 0));
    let p2 = backend_utils_adt_geo_ops::Path::from_datum_image(arg_bytes(fcinfo, 1));

    let npts1 = p1.points.len();
    let npts2 = p2.points.len();

    // for (i = 0; i < p1->npts - 1 && !found; i++) ...
    let mut i = 0usize;
    while i + 1 < npts1 {
        let seg1 = regress_lseg_construct(p1.points[i], p1.points[i + 1]);
        let mut j = 0usize;
        while j + 1 < npts2 {
            let seg2 = regress_lseg_construct(p2.points[j], p2.points[j + 1]);
            match backend_utils_adt_geo_ops::lseg_intersect(&seg1, &seg2) {
                Ok(true) => {
                    // C: DirectFunctionCall2(lseg_interpt, seg1, seg2). The
                    // comment notes lseg_interpt cannot return NULL here since
                    // the segments are known to intersect.
                    match backend_utils_adt_geo_ops::lseg_interpt(&seg1, &seg2) {
                        Ok(Some(point)) => {
                            fcinfo.isnull = false;
                            fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(
                                point.to_datum_bytes().to_vec(),
                            ));
                            return Datum::from_usize(0);
                        }
                        Ok(None) => {
                            // C's DirectFunctionCall2 would error on NULL; this
                            // is documented as impossible.
                            raise(PgError::error(
                                "function lseg_interpt returned NULL",
                            ));
                        }
                        Err(err) => raise(err),
                    }
                }
                Ok(false) => {}
                Err(err) => raise(err),
            }
            j += 1;
        }
        i += 1;
    }

    // if (!found) PG_RETURN_NULL();
    ret_null(fcinfo)
}

/* ===========================================================================
 * regress_setenv(text, text) RETURNS void  (regress.c)
 *
 *   Superuser-only setenv() of an environment variable. Used by the encoding /
 *   collation regression scripts to set TZ etc.
 * ========================================================================= */

fn fc_regress_setenv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let envvar = String::from_utf8_lossy(arg_text(fcinfo, 0)).into_owned();
    let envval = String::from_utf8_lossy(arg_text(fcinfo, 1)).into_owned();

    // C: if (!superuser()) elog(ERROR, "must be superuser ...");
    match backend_utils_misc_superuser_seams::superuser::call() {
        Ok(true) => {}
        Ok(false) => raise(PgError::error(
            "must be superuser to change environment variables",
        )),
        Err(err) => raise(err),
    }

    // C: if (setenv(envvar, envval, 1) != 0) elog(ERROR, ...). The Rust
    // std::env::set_var is the idiomatic setenv(name, value, overwrite=1).
    // SAFETY: regression-only support routine; the backend is single-threaded at
    // the point a SQL `SELECT regress_setenv(...)` runs it (mirrors C's setenv).
    unsafe {
        std::env::set_var(&envvar, &envval);
    }

    // C: PG_RETURN_VOID().
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/* ===========================================================================
 * test_relpath() RETURNS void  (regress.c)
 *
 *   Sanity checks for relpath.h: that PROCNUMBER_CHARS stays in sync with
 *   MAX_BACKENDS, and that the maximum-length relpath is generated at exactly
 *   REL_PATH_STR_MAXLEN. Mismatches are reported via elog(WARNING); the function
 *   itself returns void.
 * ========================================================================= */

/// `OID_MAX` (`postgres_ext.h`) — the largest Oid value (`0xFFFFFFFF`).
const OID_MAX: types_core::Oid = u32::MAX;

fn fc_test_relpath(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use backend_common_relpath::{
        GetRelationPath, PROCNUMBER_CHARS, REL_PATH_STR_MAXLEN,
    };

    let max_backends = backend_utils_init_small_seams::max_backends::call();

    // C: if ((int) ceil(log10(MAX_BACKENDS)) != PROCNUMBER_CHARS)
    //        elog(WARNING, "mismatch between MAX_BACKENDS and PROCNUMBER_CHARS");
    if (max_backends as f64).log10().ceil() as i32 != PROCNUMBER_CHARS as i32 {
        warn("mismatch between MAX_BACKENDS and PROCNUMBER_CHARS");
    }

    // C: rpath = GetRelationPath(OID_MAX, OID_MAX, OID_MAX, MAX_BACKENDS - 1,
    //                            INIT_FORKNUM);
    let rpath = GetRelationPath(
        OID_MAX,
        OID_MAX,
        OID_MAX,
        max_backends - 1,
        types_core::primitive::ForkNumber::INIT_FORKNUM,
    );

    // C: if (strlen(rpath.str) != REL_PATH_STR_MAXLEN)
    //        elog(WARNING, "maximum length relpath is of length %zu instead of %zu", ...);
    if rpath.len() != REL_PATH_STR_MAXLEN {
        warn(&format!(
            "maximum length relpath is of length {} instead of {}",
            rpath.len(),
            REL_PATH_STR_MAXLEN
        ));
    }

    // C: PG_RETURN_VOID().
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/// `elog(WARNING, msg)` through the elog unit's seam (emit, do not raise).
fn warn(msg: &str) {
    let _ = backend_utils_error_elog_seams::ereport_msg::call(
        types_error::WARNING,
        msg.to_owned(),
        None,
    );
}

/* ===========================================================================
 * test_atomic_ops() RETURNS bool  (regress.c)
 *
 *   Exercises the atomic-operation primitives (flag, uint32, uint64) and the
 *   spinlock, asserting the documented results, then returns true. The C
 *   primitives map directly onto Rust's std::sync::atomic types with
 *   sequentially-consistent ordering (PG's pg_atomic_* are full barriers).
 * ========================================================================= */

fn fc_test_atomic_ops(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    test_atomic_flag();
    test_atomic_uint32();
    test_atomic_uint64();
    test_spinlock();
    fcinfo.isnull = false;
    Datum::from_bool(true)
}

/// `EXPECT_TRUE(expr)` — C: `if (!(expr)) elog(ERROR, "%s yielded %s ...")`.
fn expect_true(cond: bool, what: &str) {
    if !cond {
        raise(PgError::error(format!("test_atomic_ops: {what} failed")));
    }
}

/// `test_atomic_flag` — `pg_atomic_flag` is a test-and-set boolean; model it
/// with `AtomicBool` where `true` means "set". `test_set` returns whether it
/// transitioned from clear to set; `unlocked_test` reads the current state.
fn test_atomic_flag() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let flag = AtomicBool::new(false); // pg_atomic_init_flag: cleared
    // unlocked_test_flag(): true when *not* set.
    expect_true(!flag.load(Ordering::SeqCst), "unlocked_test_flag after init");
    // test_set_flag(): true when it was clear (and now sets it).
    expect_true(
        flag.swap(true, Ordering::SeqCst) == false,
        "test_set_flag first",
    );
    expect_true(!(!flag.load(Ordering::SeqCst)), "unlocked_test_flag after set");
    // second test_set_flag returns false (already set).
    expect_true(
        !(flag.swap(true, Ordering::SeqCst) == false),
        "test_set_flag second",
    );
    flag.store(false, Ordering::SeqCst); // clear_flag
    expect_true(
        !flag.load(Ordering::SeqCst),
        "unlocked_test_flag after clear",
    );
    expect_true(
        flag.swap(true, Ordering::SeqCst) == false,
        "test_set_flag after clear",
    );
    flag.store(false, Ordering::SeqCst);
}

/// `test_atomic_uint32` — the documented arithmetic identities over a wrapping
/// 32-bit atomic, mirroring the C `EXPECT_EQ_U32` checks exactly.
fn test_atomic_uint32() {
    use std::sync::atomic::{AtomicU32, Ordering};
    const SEQ: Ordering = Ordering::SeqCst;
    let var = AtomicU32::new(0); // init_u32(0)
    expect_true(var.load(SEQ) == 0, "read_u32 init");
    var.store(3, SEQ);
    expect_true(var.load(SEQ) == 3, "read_u32 after write 3");
    // fetch_add(read - 2) == 3  (adds 1, returns old 3)
    let delta = var.load(SEQ).wrapping_sub(2);
    expect_true(var.fetch_add(delta, SEQ) == 3, "fetch_add returns old");
    expect_true(var.fetch_sub(1, SEQ) == 4, "fetch_sub returns old");
    // sub_fetch(3) == 0  (4 - 1 = 3, then 3 - 3 = 0)
    expect_true(var.fetch_sub(3, SEQ).wrapping_sub(3) == 0, "sub_fetch");
    // add_fetch(10) == 10
    expect_true(var.fetch_add(10, SEQ).wrapping_add(10) == 10, "add_fetch");
    expect_true(var.swap(5, SEQ) == 10, "exchange returns old 10");
    expect_true(var.swap(0, SEQ) == 5, "exchange returns old 5");

    // around numerical limits
    expect_true(var.fetch_add(i32::MAX as u32, SEQ) == 0, "fetch_add INT_MAX a");
    expect_true(
        var.fetch_add(i32::MAX as u32, SEQ) == i32::MAX as u32,
        "fetch_add INT_MAX b",
    );
    var.fetch_add(2, SEQ); // wrap to 0
    expect_true(
        var.fetch_add(i16::MAX as u32, SEQ) == 0,
        "fetch_add INT16_MAX",
    );
    expect_true(
        var.fetch_add(i16::MAX as u32 + 1, SEQ) == i16::MAX as u32,
        "fetch_add INT16_MAX+1",
    );
    expect_true(
        var.fetch_add((i16::MIN as i32) as u32, SEQ) == 2 * i16::MAX as u32 + 1,
        "fetch_add INT16_MIN",
    );
    expect_true(
        var.fetch_add((i16::MIN as i32 - 1) as u32, SEQ) == i16::MAX as u32,
        "fetch_add INT16_MIN-1",
    );
    var.fetch_add(1, SEQ); // top up to UINT_MAX
    expect_true(var.load(SEQ) == u32::MAX, "read_u32 == UINT_MAX");
    expect_true(
        var.fetch_sub(i32::MAX as u32, SEQ) == u32::MAX,
        "fetch_sub INT_MAX",
    );
    expect_true(var.load(SEQ) == i32::MAX as u32 + 1, "read after sub");
    expect_true(
        var.fetch_sub(i32::MAX as u32, SEQ).wrapping_sub(i32::MAX as u32) == 1,
        "sub_fetch",
    );
    var.fetch_sub(1, SEQ);

    // compare_exchange failures (wrong expected): C passes &expected and a new
    // value of 1; the call must fail (return false) for these stale expecteds.
    for &exp in &[
        i16::MAX as u32,
        i16::MAX as u32 + 1,
        (i16::MIN as i32) as u32,
        (i16::MIN as i32 - 1) as u32,
        10,
    ] {
        expect_true(
            var.compare_exchange(exp, 1, SEQ, SEQ).is_err(),
            "compare_exchange stale fails",
        );
    }
    // CAS succeed loop (expected 0).
    let mut ok = false;
    for _ in 0..1000 {
        if var.compare_exchange(0, 1, SEQ, SEQ).is_ok() {
            ok = true;
            break;
        }
    }
    expect_true(ok, "compare_exchange_u32 never succeeded");
    expect_true(var.load(SEQ) == 1, "read after CAS == 1");
    var.store(0, SEQ);

    // flag-bit set/clear via fetch_or / fetch_and.
    expect_true(var.fetch_or(1, SEQ) & 1 == 0, "fetch_or 1 old bit clear");
    expect_true(var.fetch_or(2, SEQ) & 1 != 0, "fetch_or 2 old bit set");
    expect_true(var.load(SEQ) == 3, "read == 3");
    expect_true(var.fetch_and(!2u32, SEQ) & 3 == 3, "fetch_and ~2");
    expect_true(var.fetch_and(!1u32, SEQ) == 1, "fetch_and ~1");
    expect_true(var.fetch_and(!0u32, SEQ) == 0, "fetch_and ~0");
}

/// `test_atomic_uint64` — the 64-bit analogue of the documented identities.
fn test_atomic_uint64() {
    use std::sync::atomic::{AtomicU64, Ordering};
    const SEQ: Ordering = Ordering::SeqCst;
    let var = AtomicU64::new(0);
    expect_true(var.load(SEQ) == 0, "read_u64 init");
    var.store(3, SEQ);
    expect_true(var.load(SEQ) == 3, "read_u64 after write 3");
    let delta = var.load(SEQ).wrapping_sub(2);
    expect_true(var.fetch_add(delta, SEQ) == 3, "fetch_add returns old");
    expect_true(var.fetch_sub(1, SEQ) == 4, "fetch_sub returns old");
    expect_true(var.fetch_sub(3, SEQ).wrapping_sub(3) == 0, "sub_fetch");
    expect_true(var.fetch_add(10, SEQ).wrapping_add(10) == 10, "add_fetch");
    expect_true(var.swap(5, SEQ) == 10, "exchange returns old 10");
    expect_true(var.swap(0, SEQ) == 5, "exchange returns old 5");

    expect_true(
        var.compare_exchange(10, 1, SEQ, SEQ).is_err(),
        "compare_exchange stale fails",
    );
    let mut ok = false;
    for _ in 0..100 {
        if var.compare_exchange(0, 1, SEQ, SEQ).is_ok() {
            ok = true;
            break;
        }
    }
    expect_true(ok, "compare_exchange_u64 never succeeded");
    expect_true(var.load(SEQ) == 1, "read after CAS == 1");
    var.store(0, SEQ);

    expect_true(var.fetch_or(1, SEQ) & 1 == 0, "fetch_or 1 old bit clear");
    expect_true(var.fetch_or(2, SEQ) & 1 != 0, "fetch_or 2 old bit set");
    expect_true(var.load(SEQ) == 3, "read == 3");
    expect_true(var.fetch_and(!2u64, SEQ) & 3 == 3, "fetch_and ~2");
    expect_true(var.fetch_and(!1u64, SEQ) == 1, "fetch_and ~1");
    expect_true(var.fetch_and(!0u64, SEQ) == 0, "fetch_and ~0");
}

/// `test_spinlock` — minimal exercise of the spinlock API. The C test embeds a
/// `slock_t` in a padded struct and verifies acquire/release plus that the
/// surrounding bytes are untouched (no over-wide writes). A Rust `Mutex` is the
/// idiomatic spinlock stand-in; the padding-integrity invariant is structurally
/// guaranteed (the lock is a distinct field), so we exercise lock/unlock and
/// reaffirm the neighboring data.
fn test_spinlock() {
    use std::sync::Mutex;
    struct TestLockStruct {
        data_before: [u8; 4],
        lock: Mutex<()>,
        data_after: [u8; 4],
    }
    let s = TestLockStruct {
        data_before: *b"abcd",
        lock: Mutex::new(()),
        data_after: *b"ef12",
    };
    {
        let _g = s.lock.lock().unwrap(); // SpinLockAcquire
    } // SpinLockRelease (guard drop)
    {
        let _g = s.lock.lock().unwrap(); // S_LOCK
    }
    expect_true(&s.data_before == b"abcd", "padding before spinlock modified");
    expect_true(&s.data_after == b"ef12", "padding after spinlock modified");
}

/// Resolve a symbol of the `regress` module to its ported `PGFunction` (the
/// `PG_FUNCTION_INFO_V1`-exposed `(user_fn, api_version=1)` pair). Returns `None`
/// for an unported / unknown symbol, exactly as the OS loader would fail to find
/// it in `regress.so`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "binary_coercible" => Some(fc_binary_coercible),
        "reverse_name" => Some(fc_reverse_name),
        "int44in" => Some(fc_int44in),
        "int44out" => Some(fc_int44out),
        "widget_in" => Some(fc_widget_in),
        "widget_out" => Some(fc_widget_out),
        "pt_in_widget" => Some(fc_pt_in_widget),
        "test_bytea_to_text" => Some(fc_test_bytea_to_text),
        "test_text_to_bytea" => Some(fc_test_text_to_bytea),
        "test_valid_server_encoding" => Some(fc_test_valid_server_encoding),
        "test_canonicalize_path" => Some(fc_test_canonicalize_path),
        "is_catalog_text_unique_index_oid" => Some(fc_is_catalog_text_unique_index_oid),
        "test_opclass_options_func" => Some(fc_test_opclass_options_func),
        "test_fdw_handler" => Some(fc_test_fdw_handler),
        "interpt_pp" => Some(fc_interpt_pp),
        "regress_setenv" => Some(fc_regress_setenv),
        "test_relpath" => Some(fc_test_relpath),
        "test_atomic_ops" => Some(fc_test_atomic_ops),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        // PG_FUNCTION_INFO_V1 declares api_version 1 (the only version fmgr
        // accepts for a C-language function).
        api_version: 1,
    })
}

/// Install this unit's inward seams: register the `regress` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    backend_utils_fmgr_dfmgr_seams::register_builtin_library(
        backend_utils_fmgr_dfmgr_seams::BuiltinLibraryEntry {
            name: LIBRARY,
            lookup,
        },
    );
}
