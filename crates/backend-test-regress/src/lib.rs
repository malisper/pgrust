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
