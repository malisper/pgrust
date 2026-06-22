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
use types_error::{PgError, PgResult};
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

/// `PG_GETARG_INT32(i)` — argument `i`'s word as an `int4`.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("regress fn: missing int4 arg")
        .value
        .as_i32()
}

/// `PG_GETARG_BOOL(i)` — argument `i`'s word as a `bool`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo
        .arg(i)
        .expect("regress fn: missing bool arg")
        .value
        .as_bool()
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

/// Return a fixed-length-by-reference `name` value. A `name` is a NAMEDATALEN
/// (64) byte NUL-padded buffer, and (unlike a varlena) it carries no length
/// header, so it crosses the by-ref boundary as its RAW buffer image on the
/// `Varlena` lane (the raw-buffer name convention — see
/// `byref-name-fmgr-lane-raw-buffer-convention`). `image` MUST be exactly
/// NAMEDATALEN bytes (C's `palloc0(NAMEDATALEN)`), so the downstream
/// fixed-length-by-ref `fill_val` (which copies `attlen` = 64 bytes verbatim)
/// reads a full buffer rather than slicing past a short cstring.
#[inline]
fn ret_name(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    debug_assert_eq!(image.len(), NAMEDATALEN);
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(image));
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
        // Empty input: C reads string[-1]; mirror by returning the empty
        // (all-NUL) NAMEDATALEN buffer that `palloc0` produced.
        return ret_name(fcinfo, vec![0u8; NAMEDATALEN]);
    }
    if i == NAMEDATALEN || i >= string.len() || string[i] == 0 {
        i -= 1;
    }
    let len = i;
    // C: new_string = palloc0(NAMEDATALEN); the reversed bytes are written into
    // this zero-filled NAMEDATALEN buffer, which is returned IN FULL — `name` is
    // a fixed-length-by-reference type, so the whole 64-byte NUL-padded image is
    // the value (NOT a NUL-trimmed cstring).
    let mut out = vec![0u8; NAMEDATALEN];
    let mut k = i as isize;
    while k >= 0 {
        out[len - k as usize] = string[k as usize];
        k -= 1;
    }
    ret_name(fcinfo, out)
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

    // C: regress.c uses the *compile-time* MAX_BACKENDS constant (postmaster.h
    // / procnumber.h: `(1U << 18) - 1`), NOT the runtime GetMaxBackends() value.
    // The whole point of the test is to assert the static relpath.h sizing
    // constants (PROCNUMBER_CHARS / REL_PATH_STR_MAXLEN) stay in sync with that
    // constant, so it must be the constant here too.
    const MAX_BACKENDS: u32 = types_storage::storage::MAX_BACKENDS;

    // C: if ((int) ceil(log10(MAX_BACKENDS)) != PROCNUMBER_CHARS)
    //        elog(WARNING, "mismatch between MAX_BACKENDS and PROCNUMBER_CHARS");
    if (MAX_BACKENDS as f64).log10().ceil() as i32 != PROCNUMBER_CHARS as i32 {
        warn("mismatch between MAX_BACKENDS and PROCNUMBER_CHARS");
    }

    // C: rpath = GetRelationPath(OID_MAX, OID_MAX, OID_MAX, MAX_BACKENDS - 1,
    //                            INIT_FORKNUM);
    let rpath = GetRelationPath(
        OID_MAX,
        OID_MAX,
        OID_MAX,
        (MAX_BACKENDS - 1) as i32,
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

/* ===========================================================================
 * Encoding-infrastructure test functions  (regress.c)
 *
 * These back the regression scripts `conversion.sql` and `encoding.sql`, which
 * create them with `CREATE FUNCTION ... LANGUAGE C AS '$libdir/regress', '...'`.
 * The encoding substrate (`pg_char_to_encoding`, `pg_encoding_mblen`,
 * `pg_encoding_mb2wchar_with_len`, `pg_do_encoding_conversion_buf`, ...) is
 * ported in common-extra-encnames-fgram / common-wchar / backend-utils-mb-mbutils;
 * the array / composite plumbing in backend-utils-adt-arrayfuncs /
 * backend-utils-fmgr-funcapi. A fresh scratch `MemoryContext` stands in for the
 * fmgr call's `CurrentMemoryContext`; result bytes are copied out of it onto the
 * by-ref lane before it drops.
 * ========================================================================= */

use mcx::MemoryContext;

/// `_PG_LAST_ENCODING_` (`mb/pg_wchar.h`) as a plain count.
const PG_LAST_ENCODING: i32 = common_extra_encnames::_PG_LAST_ENCODING_;

/// `pg_char_to_encoding(name)` over a NUL-terminated byte image (a `name`/`text`
/// payload). Returns the encoding id, or `-1` for an unknown name.
fn char_to_encoding(name: &[u8]) -> i32 {
    let end = name.iter().position(|&c| c == 0).unwrap_or(name.len());
    common_extra_encnames::pg_char_to_encoding(&String::from_utf8_lossy(&name[..end]))
}

/* ---------------------------------------------------------------------------
 * test_enc_setup() RETURNS void  (regress.c)
 *
 *   One-time sanity of pg_encoding_set_invalid(): for every multibyte encoding,
 *   the "official invalid string" must have length 2, mblen 2, and verify as a
 *   wholly-invalid prefix (valid prefix length 0) both standalone and with
 *   trailing data. Each mismatch is an elog(WARNING); the function returns void.
 * ------------------------------------------------------------------------- */

fn fc_test_enc_setup(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    for i in 0..PG_LAST_ENCODING {
        // if (pg_encoding_max_length(i) == 1) continue;
        if common_wchar::pg_encoding_max_length(i) == 1 {
            continue;
        }

        // char buf[2]; pg_encoding_set_invalid(i, buf);
        let mut buf = [0u8; 2];
        common_wchar::pg_encoding_set_invalid(i, &mut buf);

        // len = strnlen(buf, 2);
        let len = buf.iter().position(|&c| c == 0).unwrap_or(2) as i32;
        let name = common_extra_encnames::pg_encoding_to_char(i);
        if len != 2 {
            warn(&format!(
                "official invalid string for encoding \"{name}\" has length {len}"
            ));
        }

        // mblen = pg_encoding_mblen(i, buf);
        let mblen = common_wchar::pg_encoding_mblen(i, &buf).unwrap_or(0);
        if mblen != 2 {
            warn(&format!(
                "official invalid string for encoding \"{name}\" has mblen {mblen}"
            ));
        }

        // valid = pg_encoding_verifymbstr(i, buf, len);
        let valid = common_wchar::pg_encoding_verifymbstr(i, &buf[..len as usize]);
        if valid != 0 {
            warn(&format!(
                "official invalid string for encoding \"{name}\" has valid prefix of length {valid}"
            ));
        }

        // valid = pg_encoding_verifymbstr(i, buf, 1);
        let valid = common_wchar::pg_encoding_verifymbstr(i, &buf[..1]);
        if valid != 0 {
            warn(&format!(
                "first byte of official invalid string for encoding \"{name}\" has valid prefix of length {valid}"
            ));
        }

        // bigbuf[16] = "  ..  "; bigbuf[0..2] = buf[0..2];
        let mut bigbuf = [b' '; 16];
        bigbuf[0] = buf[0];
        bigbuf[1] = buf[1];
        let valid = common_wchar::pg_encoding_verifymbstr(i, &bigbuf);
        if valid != 0 {
            warn(&format!(
                "trailing data changed official invalid string for encoding \"{name}\" to have valid prefix of length {valid}"
            ));
        }
    }

    // PG_RETURN_VOID().
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/* ---------------------------------------------------------------------------
 * test_enc_conversion(bytea, name, name, bool) RETURNS record  (regress.c)
 *
 *   Convert `string` from `src_enc` to `dest_enc`. Returns a 2-column record
 *   (int4 converted-byte count, bytea converted string). When src == dest, just
 *   verify the source; with noError, a truncated result is returned. Otherwise
 *   look up the default conversion proc and run it through
 *   pg_do_encoding_conversion_buf.
 * ------------------------------------------------------------------------- */

/// `MAX_CONVERSION_GROWTH` (`mb/pg_wchar.h`).
const MAX_CONVERSION_GROWTH: usize = 4;
/// `MaxAllocSize` (`utils/memutils.h`) — `0x3fffffff` (1 GB - 1).
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

fn fc_test_enc_conversion(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use types_tuple::heaptuple::{BYTEAOID, INT4OID};

    // bytea string = PG_GETARG_BYTEA_PP(0); src = VARDATA_ANY, srclen = EXHDR.
    let src = arg_text(fcinfo, 0).to_vec();
    let srclen = src.len();

    let src_name = arg_name_bytes(fcinfo, 1).to_vec();
    let dest_name = arg_name_bytes(fcinfo, 2).to_vec();
    let no_error = arg_bool(fcinfo, 3);

    let src_encoding = char_to_encoding(&src_name);
    let dest_encoding = char_to_encoding(&dest_name);

    if src_encoding < 0 {
        raise(
            PgError::error(format!(
                "invalid source encoding name \"{}\"",
                String::from_utf8_lossy(&src_name)
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }
    if dest_encoding < 0 {
        raise(
            PgError::error(format!(
                "invalid destination encoding name \"{}\"",
                String::from_utf8_lossy(&dest_name)
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    let convertedbytes: i32;
    // The converted payload bytes (no varlena header).
    let retval: Vec<u8>;

    if src_encoding == dest_encoding {
        // just check that the source string is valid.
        let oklen = common_wchar::pg_encoding_verifymbstr(src_encoding, &src);
        if oklen as usize == srclen {
            convertedbytes = oklen;
            retval = src.clone();
        } else if !no_error {
            // report_invalid_encoding(src_encoding, src + oklen, srclen - oklen).
            match backend_utils_mb_mbutils_seams::report_invalid_encoding::call(
                src_encoding,
                &src[oklen as usize..],
            ) {
                Ok(()) => unreachable!("report_invalid_encoding returned Ok"),
                Err(err) => raise(err),
            }
        } else {
            // Truncate to the valid prefix.
            debug_assert!((oklen as usize) < srclen);
            convertedbytes = oklen;
            retval = src[..oklen as usize].to_vec();
        }
    } else {
        // proc = FindDefaultConversionProc(src_encoding, dest_encoding).
        let proc = match backend_utils_mb_mbutils_seams::find_default_conversion_proc::call(
            src_encoding,
            dest_encoding,
        ) {
            Ok(proc) => proc,
            Err(err) => raise(err),
        };
        if proc == types_core::primitive::Oid::from(0u32) {
            raise(
                PgError::error(format!(
                    "default conversion function for encoding \"{}\" to \"{}\" does not exist",
                    common_extra_encnames::pg_encoding_to_char(src_encoding),
                    common_extra_encnames::pg_encoding_to_char(dest_encoding),
                ))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_FUNCTION),
            );
        }

        if srclen >= MAX_ALLOC_SIZE / MAX_CONVERSION_GROWTH {
            raise(
                PgError::error("out of memory")
                    .with_detail(format!(
                        "String of {srclen} bytes is too long for encoding conversion."
                    ))
                    .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }

        let dstsize = srclen * MAX_CONVERSION_GROWTH + 1;
        let m = MemoryContext::new("test_enc_conversion scratch");
        let (consumed, dst) = match backend_utils_mb_mbutils_seams::pg_do_encoding_conversion_buf::call(
            m.mcx(),
            proc,
            src_encoding,
            dest_encoding,
            &src,
            dstsize as i32,
            no_error,
        ) {
            Ok(pair) => pair,
            Err(err) => raise(err),
        };
        convertedbytes = consumed;
        // C: dstlen = strlen(dst). The seam returns the converted bytes without
        // the trailing NUL, but a NUL inside the output truncates strlen.
        let dstlen = dst
            .as_slice()
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(dst.len());
        retval = dst.as_slice()[..dstlen].to_vec();
    }

    // Build the 2-column record (int4 convertedbytes, bytea retval) and lower it
    // onto the by-ref Composite lane (C: heap_form_tuple + HeapTupleGetDatum).
    let m = MemoryContext::new("test_enc_conversion record");
    let image = build_conv_record(m.mcx(), &[INT4OID, BYTEAOID], convertedbytes, &retval);
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Composite(image));
    Datum::from_usize(0)
}

/// `record_from_values` over (int4, bytea), returning the self-describing
/// composite-Datum byte image (copied out of `mcx`).
fn build_conv_record(
    mcx: mcx::Mcx<'_>,
    coltypes: &[types_core::primitive::Oid],
    convertedbytes: i32,
    retval: &[u8],
) -> Vec<u8> {
    use types_tuple::backend_access_common_heaptuple::Datum as TDatum;
    let bytea_image = varlena_image(retval);
    let bytea_datum = match TDatum::from_byref_bytes_in(mcx, &bytea_image) {
        Ok(d) => d,
        Err(err) => raise(err),
    };
    let values = [TDatum::from_i32(convertedbytes), bytea_datum];
    let nulls = [false, false];
    let datum =
        match backend_utils_fmgr_funcapi_seams::record_from_values::call(mcx, coltypes, &values, &nulls)
        {
            Ok(d) => d,
            Err(err) => raise(err),
        };
    datum.as_varlena_bytes().into_owned()
}

/* ---------------------------------------------------------------------------
 * test_mblen_func(bytea, bytea, bytea, int4) RETURNS int4  (regress.c)
 *
 *   Call one of the pg_mblen_* leading-character-length helpers (selected by the
 *   first text arg) at `offset` into the third arg's bytes. `pg_encoding_mblen`
 *   uses the explicit encoding named by the second arg; the rest use the
 *   database encoding.
 * ------------------------------------------------------------------------- */

fn fc_test_mblen_func(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let func = String::from_utf8_lossy(arg_text(fcinfo, 0)).into_owned();
    let encoding = arg_text(fcinfo, 1).to_vec();
    let data = arg_text(fcinfo, 2);
    let size = data.len();
    let offset = arg_int32(fcinfo, 3) as usize;

    // The byte windows C forms: data + offset, and the [data+offset, data+size).
    let at_offset = &data[offset.min(size)..];

    let result: i32 = match func.as_str() {
        "pg_mblen_unbounded" => backend_utils_mb_mbutils::pg_mblen_unbounded(at_offset),
        "pg_mblen_cstr" => match backend_utils_mb_mbutils::pg_mblen_cstr(at_offset) {
            Ok(n) => n,
            Err(err) => raise(err),
        },
        "pg_mblen_with_len" => {
            match backend_utils_mb_mbutils::pg_mblen_with_len(at_offset, (size - offset) as i32) {
                Ok(n) => n,
                Err(err) => raise(err),
            }
        }
        "pg_mblen_range" => match backend_utils_mb_mbutils::pg_mblen_range(at_offset) {
            Ok(n) => n,
            Err(err) => raise(err),
        },
        "pg_encoding_mblen" => {
            let enc = char_to_encoding(&encoding);
            common_wchar::pg_encoding_mblen(enc, at_offset).unwrap_or(0)
        }
        _ => raise(PgError::error("unknown function")),
    };

    fcinfo.isnull = false;
    Datum::from_i32(result)
}

/* ---------------------------------------------------------------------------
 * test_text_to_wchars(bytea, text) RETURNS int4[]  (regress.c)
 *
 *   Convert `string` (in the named encoding) to its pg_wchar code points and
 *   return them as an int4[] array.
 * ------------------------------------------------------------------------- */

fn fc_test_text_to_wchars(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let encoding_name = arg_text(fcinfo, 0).to_vec();
    let data = arg_text(fcinfo, 1).to_vec();
    let size = data.len();

    let encoding = char_to_encoding(&encoding_name);
    if encoding < 0 {
        raise(PgError::error(format!(
            "unknown encoding name: {}",
            String::from_utf8_lossy(&encoding_name)
        )));
    }

    let m = MemoryContext::new("test_text_to_wchars scratch");
    let datums: Vec<Datum> = if size > 0 {
        let wchars = match backend_utils_mb_mbutils::pg_encoding_mb2wchar_with_len(
            m.mcx(),
            encoding,
            &data,
            size as i32,
        ) {
            Ok(w) => w,
            Err(err) => raise(err),
        };
        // C asserts wlen <= size and wchars[wlen] == 0; the wchar count == wlen.
        // Each code point is a UInt32GetDatum element of an int4[] array.
        wchars
            .as_slice()
            .iter()
            .map(|&w| Datum::from_i32(w as i32))
            .collect()
    } else {
        Vec::new()
    };

    // construct_array_builtin(datums, wlen, INT4OID) → array varlena image.
    let array_v = match backend_utils_adt_arrayfuncs::construct::construct_array_builtin_v(
        m.mcx(),
        &datums,
        types_tuple::heaptuple::INT4OID,
    ) {
        Ok(v) => v,
        Err(err) => raise(err),
    };
    let image = array_v.as_varlena_bytes().into_owned();
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/* ---------------------------------------------------------------------------
 * test_wchars_to_text(bytea, int4[]) RETURNS text  (regress.c)
 *
 *   Inverse of test_text_to_wchars: take an int4[] of pg_wchar code points and
 *   encode them as text in the named encoding.
 * ------------------------------------------------------------------------- */

fn fc_test_wchars_to_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let encoding_name = arg_text(fcinfo, 0).to_vec();
    // PG_GETARG_ARRAYTYPE_P(1): the header-ful int4[] varlena image.
    let array = arg_bytes(fcinfo, 1).to_vec();

    let encoding = char_to_encoding(&encoding_name);
    if encoding < 0 {
        raise(PgError::error(format!(
            "unknown encoding name: {}",
            String::from_utf8_lossy(&encoding_name)
        )));
    }

    let m = MemoryContext::new("test_wchars_to_text scratch");
    // deconstruct_array_builtin(array, INT4OID) → (Datum, isnull) pairs.
    let elems = match backend_utils_adt_arrayfuncs::construct::deconstruct_array_builtin(
        m.mcx(),
        &array,
        types_tuple::heaptuple::INT4OID,
    ) {
        Ok(v) => v,
        Err(err) => raise(err),
    };

    let bytes: Vec<u8> = if !elems.is_empty() {
        let mut wchars: Vec<types_wchar::wchar::PgWChar> = Vec::with_capacity(elems.len());
        for (datum, isnull) in elems.iter() {
            if *isnull {
                raise(PgError::error("unexpected NULL in array"));
            }
            wchars.push(datum.as_i32() as types_wchar::wchar::PgWChar);
        }
        let wlen = wchars.len() as i32;
        match backend_utils_mb_mbutils::pg_encoding_wchar2mb_with_len(
            m.mcx(),
            encoding,
            &wchars,
            wlen,
        ) {
            Ok(mb) => mb.as_slice().to_vec(),
            Err(err) => raise(err),
        }
    } else {
        Vec::new()
    };

    ret_varlena(fcinfo, &bytes)
}

/* ---------------------------------------------------------------------------
 * get_environ() RETURNS text[]  (regress.c)
 *
 *   Return the process environment as a text[] of "VAR=value" strings.
 * ------------------------------------------------------------------------- */

fn fc_get_environ(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // for (char **s = environ; *s; s++) ... CStringGetTextDatum(environ[i]).
    let env: Vec<String> = std::env::vars()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    let refs: Vec<&str> = env.iter().map(|s| s.as_str()).collect();

    // construct_array_builtin(env, nvals, TEXTOID) → text[] varlena image.
    let m = MemoryContext::new("get_environ scratch");
    let image = match backend_utils_adt_arrayfuncs::construct::construct_text_array_bytes_str(m.mcx(), &refs) {
        Ok(v) => v.as_slice().to_vec(),
        Err(err) => raise(err),
    };
    fcinfo.isnull = false;
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/* ===========================================================================
 * overpaid(emp) RETURNS bool  (regress.c)
 *
 *   Datum
 *   overpaid(PG_FUNCTION_ARGS)
 *   {
 *       HeapTupleHeader tuple = PG_GETARG_HEAPTUPLEHEADER(0);
 *       bool        isnull;
 *       int32       salary;
 *
 *       salary = DatumGetInt32(GetAttributeByName(tuple, "salary", &isnull));
 *       if (isnull)
 *           PG_RETURN_NULL();
 *       PG_RETURN_BOOL(salary > 699);
 *   }
 * ========================================================================= */

fn fc_overpaid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // PG_GETARG_HEAPTUPLEHEADER(0): the composite/record Datum arrives on the
    // by-ref Composite lane as the flat HeapTupleHeader Datum image.
    let image = match fcinfo.ref_arg(0).and_then(|p| p.as_composite()) {
        Some(b) => b.to_vec(),
        None => panic!("overpaid: composite arg missing from by-ref lane"),
    };
    let m = MemoryContext::new("overpaid scratch");
    let tuple = match types_tuple::FormedTuple::from_datum_image(m.mcx(), &image) {
        Ok(t) => t,
        Err(err) => raise(err),
    };
    // salary = DatumGetInt32(GetAttributeByName(tuple, "salary", &isnull));
    let (value, isnull) =
        match backend_executor_execUtils::GetAttributeByName(m.mcx(), Some(&tuple), "salary") {
            Ok(pair) => pair,
            Err(err) => raise(err),
        };
    if isnull {
        return ret_null(fcinfo);
    }
    let salary = value.as_i32();
    fcinfo.isnull = false;
    Datum::from_bool(salary > 699)
}

/* ===========================================================================
 * trigger_return_old() RETURNS trigger  (regress.c)
 *
 *   Datum
 *   trigger_return_old(PG_FUNCTION_ARGS)
 *   {
 *       TriggerData *trigdata = (TriggerData *) fcinfo->context;
 *       HeapTuple   tuple;
 *
 *       if (!CALLED_AS_TRIGGER(fcinfo))
 *           elog(ERROR, "trigger_return_old: not fired by trigger manager");
 *
 *       tuple = trigdata->tg_trigtuple;
 *       return PointerGetDatum(tuple);
 *   }
 *
 * Returns the OLD/unmodified row the trigger manager handed in, so a BEFORE
 * trigger leaves the operation's tuple unchanged.  The rich `TriggerData` is
 * read off the `CURRENT_TRIGGER_DATA` thread-local side-channel the trigger
 * manager installs around the call (`fcinfo->context` carries only the
 * `T_TriggerData` demux tag); the returned tuple is lowered onto the by-ref
 * Composite lane the trigger firing path reads its result tuple from.
 * ========================================================================= */

fn fc_trigger_return_old(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // if (!CALLED_AS_TRIGGER(fcinfo)) elog(ERROR, "... not fired by trigger manager");
    // CALLED_AS_TRIGGER == `fcinfo->context` is a TriggerData node; the trigger
    // manager installs the rich TriggerData on the per-call side-channel exactly
    // when that demux tag is stamped, so its presence is the same predicate.
    let called_as_trigger =
        backend_commands_trigger::firing::with_current_trigger_data(|td| td.is_some());
    if !called_as_trigger {
        raise(PgError::error(
            "trigger_return_old: not fired by trigger manager",
        ));
    }

    // tuple = trigdata->tg_trigtuple; return PointerGetDatum(tuple).
    //
    // A registry-loaded C trigger function returns its result row through the
    // BEFORE-trigger return-tuple channel (the fmgr-returned Datum is the ignored
    // sentinel), so deposit the OLD tuple there.  A NULL tg_trigtuple mirrors C's
    // PointerGetDatum(NULL) ("do nothing"): leave the channel as the DoNothing /
    // empty default, which the firing path decodes as no row change.
    backend_commands_trigger::firing::set_before_trigger_result_to_trigtuple();

    // The fmgr result is the trigger sentinel; isnull must stay false (the
    // trigger protocol forbids a SQL-NULL result flag).
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/* ===========================================================================
 * wait_pid(int4) RETURNS void  (regress.c)
 *
 *   Datum
 *   wait_pid(PG_FUNCTION_ARGS)
 *   {
 *       int pid = PG_GETARG_INT32(0);
 *       if (!superuser())
 *           elog(ERROR, "must be superuser to check PID liveness");
 *       while (kill(pid, 0) == 0)
 *       {
 *           CHECK_FOR_INTERRUPTS();
 *           pg_usleep(50000);
 *       }
 *       if (errno != ESRCH)
 *           elog(ERROR, "could not check PID %d liveness: %m", pid);
 *       PG_RETURN_VOID();
 *   }
 *
 * Blocks until the given PID exits.  Used by the TAP/isolation harness, not the
 * core SQL schedule, but declared via `CREATE FUNCTION ... 'wait_pid'`.
 * ========================================================================= */

fn fc_wait_pid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let pid = arg_int32(fcinfo, 0);

    // if (!superuser()) elog(ERROR, "must be superuser to check PID liveness");
    match backend_utils_misc_superuser_seams::superuser::call() {
        Ok(true) => {}
        Ok(false) => raise(PgError::error("must be superuser to check PID liveness")),
        Err(err) => raise(err),
    }

    // while (kill(pid, 0) == 0) { CHECK_FOR_INTERRUPTS(); pg_usleep(50000); }
    loop {
        // SAFETY: kill(pid, 0) only probes for the process's existence /
        // signalability; signal 0 sends nothing.  errno is read immediately
        // after, before any other libc call.
        let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if rc != 0 {
            // Loop ends; classify the failure below.
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            // if (errno != ESRCH) elog(ERROR, "could not check PID %d liveness: %m", pid);
            if errno != libc::ESRCH {
                raise(PgError::error(format!(
                    "could not check PID {pid} liveness: {}",
                    std::io::Error::from_raw_os_error(errno)
                )));
            }
            break;
        }
        // CHECK_FOR_INTERRUPTS();
        if let Err(err) =
            backend_access_transam_parallel_rt_seams::check_for_interrupts::call()
        {
            raise(err);
        }
        // pg_usleep(50000);
        port_pgsleep::pg_usleep(50000);
    }

    // PG_RETURN_VOID().
    fcinfo.isnull = false;
    Datum::from_usize(0)
}

/* ===========================================================================
 * make_tuple_indirect(record) RETURNS record  (regress.c)
 *
 * Rewrites each not-null, toastable varlena attribute of the input record into
 * an *indirect* TOAST pointer (`VARTAG_INDIRECT`) that points at a copy of the
 * datum living in `TopTransactionContext`, then returns the rebuilt
 * HeapTupleHeader *without* flattening the indirect pointers (so the
 * indirect-toast machinery can be exercised downstream).
 *
 * Port note — the indirect-TOAST-pointer substrate exists in this codebase but
 * is keyed differently from C.  C's `varatt_indirect.pointer` is a raw
 * `struct varlena *` into `TopTransactionContext`, embedded in the returned
 * tuple's bytes and followed verbatim by the detoast `indirect_pointer`
 * dereference.  Here a composite Datum crosses the fmgr boundary as a
 * *serialized byte image* (`RefPayload::Composite`), so an embedded process
 * address would not survive the copy.  Instead the target bytes are stashed in
 * the per-backend `INDIRECT_TARGETS` registry (the `TopTransactionContext`
 * stand-in — see `backend-access-common-toast-internals-seams`) and a stable
 * `u64` *token* is embedded in the `varatt_indirect` payload slot; the
 * `indirect_pointer` seam (installed by `backend-access-common-toast-internals`)
 * resolves the token back to those bytes.  Everything else mirrors regress.c
 * line-for-line: deform the record, rewrite each not-null toastable varlena
 * into a `VARTAG_INDIRECT` external datum (fully detoasting an on-disk-external
 * value first, so the target "still lives later"), re-form, and return the
 * `HeapTupleHeader` image *without* flattening the indirect pointers.
 * ========================================================================= */

/// `VARTAG_INDIRECT` (varatt.h).
const VARTAG_INDIRECT: u8 = 1;
/// `VARHDRSZ_EXTERNAL` (varatt.h): 1-byte length tag (`0x01`) + 1-byte vartag.
const VARHDRSZ_EXTERNAL: usize = 2;
/// `TYPSTORAGE_PLAIN` (pg_type.h): the `attstorage` value for a non-toastable
/// fixed-storage column.
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;

/// `VARATT_IS_1B_E(PTR)` / `VARATT_IS_EXTERNAL(PTR)` (varatt.h, little-endian):
/// a `0x01` length byte marks an external (TOAST-pointer) varlena.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    !b.is_empty() && b[0] == 0x01
}

/// `VARATT_IS_EXTERNAL_INDIRECT(PTR)`: external form, `va_tag == VARTAG_INDIRECT`.
#[inline]
fn varatt_is_external_indirect(b: &[u8]) -> bool {
    varatt_is_external(b) && b.len() >= 2 && b[1] == VARTAG_INDIRECT
}

/// `VARATT_IS_EXTERNAL_ONDISK(PTR)`: external form, `va_tag == VARTAG_ONDISK` (18).
#[inline]
fn varatt_is_external_ondisk(b: &[u8]) -> bool {
    varatt_is_external(b) && b.len() >= 2 && b[1] == 18
}

fn fc_make_tuple_indirect(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match make_tuple_indirect_impl(fcinfo) {
        Ok(d) => d,
        Err(err) => raise(err),
    }
}

fn make_tuple_indirect_impl(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> Result<Datum, PgError> {
    use backend_access_common_heaptuple::{heap_deform_tuple, heap_form_tuple, Datum as HtDatum};

    // HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
    //
    // The composite record arrives either on the dedicated `Composite` lane (a
    // SQL-level `make_tuple_indirect(tab)` call) or on the generic `Varlena`
    // lane (a plpgsql `NEW := make_tuple_indirect(NEW)` trigger assignment,
    // which lowers the RECORD variable to its flat HeapTupleHeader varlena
    // image) — both are the same physical block C's `DatumGetHeapTupleHeader`
    // reads, so accept either via `as_any_varlena`.
    let image = match fcinfo.ref_arg(0).and_then(|p| p.as_byref_image()) {
        Some(b) => b.to_vec(),
        None => {
            return Err(PgError::error(
                "make_tuple_indirect: composite arg missing from by-ref lane",
            ))
        }
    };

    let m = MemoryContext::new("make_tuple_indirect");
    let mcx = m.mcx();

    // Build the temporary HeapTuple control structure from the Datum image.
    let formed = types_tuple::FormedTuple::from_datum_image(mcx, &image)?;
    let header = formed.tuple.t_data.as_ref().ok_or_else(|| {
        PgError::error("make_tuple_indirect: record has no header")
    })?;

    // tupType = HeapTupleHeaderGetTypeId(rec); tupTypmod = HeapTupleHeaderGetTypMod(rec);
    let tup_type = types_tuple::heaptuple::HeapTupleHeaderGetTypeId(header);
    let tup_typmod = types_tuple::heaptuple::HeapTupleHeaderGetTypMod(header);

    // tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod); ncolumns = tupdesc->natts;
    let tupdesc = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
        mcx, tup_type, tup_typmod,
    )?;
    let ncolumns = tupdesc.natts as usize;

    // heap_deform_tuple(&tuple, tupdesc, values, nulls);
    let cols = heap_deform_tuple(mcx, &formed.tuple, &tupdesc, &formed.data)?;
    let mut values: Vec<HtDatum> = Vec::with_capacity(ncolumns);
    let mut nulls: Vec<bool> = Vec::with_capacity(ncolumns);
    for (val, isnull) in cols.iter() {
        values.push(val.clone_in(mcx)?);
        nulls.push(*isnull);
    }
    while values.len() < ncolumns {
        values.push(HtDatum::null());
        nulls.push(true);
    }

    // for (i = 0; i < ncolumns; i++) — rewrite each toastable varlena to indirect.
    for i in 0..ncolumns {
        let atti = &tupdesc.attrs[i];

        // only work on existing, not-null varlenas
        if atti.attisdropped
            || nulls[i]
            || atti.attlen != -1
            || atti.attstorage == TYPSTORAGE_PLAIN
        {
            continue;
        }

        // attr = (struct varlena *) DatumGetPointer(values[i]);
        let attr = values[i].as_ref_bytes().to_vec();

        // don't recursively indirect
        if varatt_is_external_indirect(&attr) {
            continue;
        }

        // copy datum, so it still lives later
        let target: Vec<u8> = if varatt_is_external_ondisk(&attr) {
            // attr = detoast_external_attr(attr);
            backend_access_common_detoast::detoast_external_attr(mcx, &attr)?.to_vec()
        } else {
            // palloc0(VARSIZE_ANY(oldattr)); memcpy(attr, oldattr, VARSIZE_ANY(oldattr));
            let sz = backend_access_common_heaptuple::varsize_any(&attr);
            attr[..sz.min(attr.len())].to_vec()
        };

        // build indirection Datum:
        //   new_attr = palloc0(INDIRECT_POINTER_SIZE);
        //   redirect_pointer.pointer = attr;
        //   SET_VARTAG_EXTERNAL(new_attr, VARTAG_INDIRECT);
        //   memcpy(VARDATA_EXTERNAL(new_attr), &redirect_pointer, sizeof(redirect_pointer));
        //
        // The `varatt_indirect.pointer` raw address becomes a stable registry
        // token (the `TopTransactionContext`-lived copy stand-in).
        let token = backend_access_common_toast_internals_seams::register_indirect_target(&target);
        let mut new_attr = Vec::with_capacity(VARHDRSZ_EXTERNAL + 8);
        new_attr.push(0x01u8); // SET_VARTAG_EXTERNAL length byte
        new_attr.push(VARTAG_INDIRECT); // va_tag
        new_attr.extend_from_slice(&token.to_ne_bytes()); // varatt_indirect payload

        // values[i] = PointerGetDatum(new_attr);
        values[i] = HtDatum::from_byref_bytes_in(mcx, &new_attr)?;
    }

    // newtup = heap_form_tuple(tupdesc, values, nulls);
    let newtup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)
        .map_err(|e| PgError::error(format!("make_tuple_indirect: heap_form_tuple: {e:?}")))?;

    // We intentionally return the HeapTupleHeader image as-is (no flattening),
    // so the indirect toast pointers survive in the returned composite for the
    // downstream indirect-toast machinery to exercise.
    let out_image = newtup.to_datum_image();
    fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Composite(out_image));
    fcinfo.isnull = false;
    Ok(Datum::from_usize(0))
}

/* ===========================================================================
 * test_support_func(internal) RETURNS internal  (regress.c)
 *
 * A planner-support function: receives a `SupportRequestSelectivity` /
 * `SupportRequestCost` / `SupportRequestRows` request node by `internal`
 * pointer, fills in its estimate fields in place, and returns it.
 *
 * The port decomposes planner-support into kernel function tables
 * (`backend-optimizer-util-clauses` `support_cost`/`support_rows`, and the
 * `SupportRequestSelectivity` leg in selfuncs) rather than passing a mutable
 * `SupportRequest*` `Node` across the fmgr `internal` lane. Because this is a
 * *dynamically-OID'd* (user-created C-language) support function — its
 * `prosupport` OID is assigned at `CREATE FUNCTION` time, so it cannot be keyed
 * by a fixed builtin OID like the in-tree support functions — its kernels are
 * registered by the `prosrc` symbol `"test_support_func"` in [`init_seams`].
 * The dispatch (`get_function_rows`/`add_function_cost` in plancat; the
 * `SupportRequestSelectivity` leg in selfuncs) resolves the support OID's
 * `prosrc` symbol and routes here, the faithful counterpart of fmgr running
 * `OidFunctionCall1(prosupport, &req)` over a C-language function resolved by
 * its `prosrc`.
 *
 * `fc_test_support_func` itself is never invoked through the one-shot fmgr
 * boundary (no `SupportRequest*` carrier crosses it); the symbol resolves so
 * `CREATE FUNCTION test_support_func(internal)` validates.
 * ========================================================================= */

fn fc_test_support_func(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error(
        "test_support_func is not invoked through the fmgr `internal` lane; its \
         SupportRequest{Selectivity,Cost,Rows} legs are routed by `prosrc` symbol \
         through the decomposed planner-support registries",
    ));
}

/// `test_support_func`'s `SupportRequestRows` leg (regress.c): assume the target
/// is `generate_series_int4`; if `req->node` is a `FuncExpr` whose first two
/// arguments are non-NULL `Const`s, `req->rows = val2 - val1 + 1`. Otherwise
/// decline.
fn test_support_func_rows(
    _funcid: types_core::Oid,
    node: &types_nodes::primnodes::Expr,
) -> PgResult<Option<f64>> {
    // if (req->node && IsA(req->node, FuncExpr))  /* be paranoid */
    let Some(fexpr) = node.as_funcexpr() else {
        return Ok(None);
    };
    let args = &fexpr.args;
    if args.len() < 2 {
        return Ok(None);
    }
    // arg1 = linitial(args); arg2 = lsecond(args);
    let (Some(a1), Some(a2)) = (args[0].as_const(), args[1].as_const()) else {
        return Ok(None);
    };
    if a1.constisnull || a2.constisnull {
        return Ok(None);
    }
    // val1 = DatumGetInt32(arg1->constvalue); val2 = DatumGetInt32(arg2->constvalue);
    let val1 = a1.constvalue.as_i32();
    let val2 = a2.constvalue.as_i32();
    // req->rows = val2 - val1 + 1;
    Ok(Some((val2 - val1 + 1) as f64))
}

/// `test_support_func`'s `SupportRequestCost` leg (regress.c): a generic
/// estimate — `startup = 0`, `per_tuple = 2 * cpu_operator_cost`.
fn test_support_func_cost(
    _funcid: types_core::Oid,
    _node: Option<&types_nodes::primnodes::Expr>,
) -> PgResult<Option<(f64, f64)>> {
    let cpu_operator_cost = backend_optimizer_path_costsize_seams::cpu_operator_cost::call();
    Ok(Some((0.0, 2.0 * cpu_operator_cost)))
}

/// Resolve a symbol of the `regress` module to its ported `PGFunction` (the
/// `PG_FUNCTION_INFO_V1`-exposed `(user_fn, api_version=1)` pair). Returns `None`
/// for an unported / unknown symbol, exactly as the OS loader would fail to find
/// it in `regress.so`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "binary_coercible" => Some(fc_binary_coercible),
        "overpaid" => Some(fc_overpaid),
        "trigger_return_old" => Some(fc_trigger_return_old),
        "wait_pid" => Some(fc_wait_pid),
        "make_tuple_indirect" => Some(fc_make_tuple_indirect),
        "test_support_func" => Some(fc_test_support_func),
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
        "test_enc_setup" => Some(fc_test_enc_setup),
        "test_enc_conversion" => Some(fc_test_enc_conversion),
        "test_mblen_func" => Some(fc_test_mblen_func),
        "test_text_to_wchars" => Some(fc_test_text_to_wchars),
        "test_wchars_to_text" => Some(fc_test_wchars_to_text),
        "get_environ" => Some(fc_get_environ),
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
            pg_init: None,
        },
    );

    // `test_support_func` (regress.c) is a user-created C-language planner
    // support function: its `prosupport` OID is assigned at `CREATE FUNCTION`
    // time and so cannot be keyed by a fixed builtin OID. Register its
    // `SupportRequestRows`/`SupportRequestCost` kernels under its `prosrc` symbol
    // so the decomposed planner-support dispatch can route to them by symbol
    // (the `SupportRequestSelectivity` leg is handled in selfuncs by the same
    // symbol resolution). Mirrors fmgr's by-`prosrc` C-language resolution.
    backend_optimizer_util_clauses::support_rows::register_support_rows_by_symbol(
        "test_support_func",
        test_support_func_rows,
    );
    backend_optimizer_util_clauses::support_cost::register_support_cost_by_symbol(
        "test_support_func",
        test_support_func_cost,
    );
}
