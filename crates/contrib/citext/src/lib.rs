#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of the PostgreSQL `citext` contrib module
//! (`contrib/citext/citext.c`) — the case-insensitive `citext` text type.
//!
//! The `citext` *type* itself piggybacks on `text`'s I/O (`textin`/`textout`/
//! `textrecv`/`textsend`) and is created entirely in SQL by the install script
//! (`citext--1.4.sql` + the 1.5..1.8 updates). The only code that must be
//! ported from C is the comparison/hash/aggregate `LANGUAGE C` functions:
//! these compare two `citext` values case-insensitively by lowercasing each
//! operand (via `str_tolower`, always under `DEFAULT_COLLATION_OID` so the
//! behavior is collation-independent) and then comparing.
//!
//! Registration mirrors `pg_prewarm`: the SQL emitted by `citext--*.sql`
//! (`CREATE FUNCTION citext_eq(...) LANGUAGE C AS 'MODULE_PATHNAME','citext_eq'`)
//! resolves `$libdir/citext` through the dynamic-loader unit's ported-library
//! registry rather than the OS loader (the Rust backend exposes no C ABI).

extern crate alloc;

use ::datum::Datum;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::mcx::{Mcx, MemoryContext};
use ::types_core::primitive::Oid;
use ::types_error::{PgError, PgResult};

/// The simple (suffix-free, directory-free) name of the loadable module —
/// `$libdir/citext` reduces to this for the registry.
const LIBRARY: &str = "citext";

/// `DEFAULT_COLLATION_OID` (catalog/pg_collation_d.h). citext does all its
/// `str_tolower` calls with this, NOT the input collation, so equality/hashing
/// are not collation-dependent (see the long comment in `citextcmp`).
const DEFAULT_COLLATION_OID: Oid = 100;

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (`invoke_pgfunction`'s `catch_unwind`), which downcasts
/// the panic payload back to the structured [`PgError`] (mirrors `pg_prewarm`
/// and `backend-test-regress`).
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// `PG_GETARG_TEXT_PP(i)` — the `VARDATA_ANY` payload bytes of `text`/`citext`
/// argument `i`. Header-form-agnostic (`VARDATA_ANY`): skip ONE byte for a
/// short (low-bit-set, non-external) 1-byte header, else the 4-byte header.
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("citext fn: text/citext arg missing from by-ref lane");
    match image.first() {
        // VARATT_IS_1B && !VARATT_IS_1B_E: short 1-byte header (skip 1 byte).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        // 4-byte uncompressed header (skip VARHDRSZ).
        Some(_) if image.len() >= ::datum::varlena::VARHDRSZ => {
            &image[::datum::varlena::VARHDRSZ..]
        }
        _ => &[],
    }
}

/// `PG_GET_COLLATION()` — the collation oid for this call.
#[inline]
fn collation(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo.fncollation
}

/// Build a header-ful varlena (`text`/`citext`) image from its payload bytes
/// (C: `SET_VARSIZE(result, len + VARHDRSZ)` over a fresh palloc'd block).
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + ::datum::varlena::VARHDRSZ;
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    image.extend_from_slice(payload);
    image
}

/// `PG_RETURN_TEXT_P` — write a header-ful varlena result.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(::fmgr::boundary::RefPayload::Varlena(varlena_image(payload)));
    Datum::from_usize(0)
}

/// `str_tolower(buff, len, DEFAULT_COLLATION_OID)` — the deterministic
/// case-fold citext uses everywhere. Allocates the folded bytes in `mcx`.
fn lower<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<alloc::vec::Vec<u8>> {
    let folded = formatting_seams::str_tolower::call(mcx, buff, DEFAULT_COLLATION_OID)?;
    Ok(folded.as_slice().to_vec())
}

/// `citextcmp(left, right, collid)` (citext.c) — internal collation-aware
/// 3-way comparison: lowercase both operands under DEFAULT_COLLATION_OID, then
/// `varstr_cmp` the folded strings under the *call* collation.
fn citextcmp(left: &[u8], right: &[u8], collid: Oid) -> PgResult<i32> {
    let scratch = MemoryContext::new("citext cmp");
    let mcx = scratch.mcx();
    let lcstr = lower(mcx, left)?;
    let rcstr = lower(mcx, right)?;
    varlena_seams::varstr_cmp::call(&lcstr, &rcstr, collid)
}

/// `internal_citext_pattern_cmp(left, right, collid)` (citext.c) — character
/// (byte) comparison of the lowercased operands, with a length tiebreak.
fn internal_citext_pattern_cmp(left: &[u8], right: &[u8]) -> PgResult<i32> {
    let scratch = MemoryContext::new("citext pattern cmp");
    let mcx = scratch.mcx();
    let lcstr = lower(mcx, left)?;
    let rcstr = lower(mcx, right)?;

    let llen = lcstr.len();
    let rlen = rcstr.len();
    let result = lcstr[..llen.min(rlen)].cmp(&rcstr[..llen.min(rlen)]);
    Ok(match result {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Greater => 1,
        core::cmp::Ordering::Equal => match llen.cmp(&rlen) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Greater => 1,
            core::cmp::Ordering::Equal => 0,
        },
    })
}

// ===========================================================================
// Indexing functions
// ===========================================================================

/// `citext_cmp(citext, citext) RETURNS int4`.
fn fc_citext_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let collid = collation(fcinfo);
    let left = arg_text(fcinfo, 0);
    let right = arg_text(fcinfo, 1);
    match citextcmp(left, right, collid) {
        Ok(r) => Datum::from_i32(r),
        Err(e) => raise(e),
    }
}

/// `citext_pattern_cmp(citext, citext) RETURNS int4`.
fn fc_citext_pattern_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let left = arg_text(fcinfo, 0);
    let right = arg_text(fcinfo, 1);
    match internal_citext_pattern_cmp(left, right) {
        Ok(r) => Datum::from_i32(r),
        Err(e) => raise(e),
    }
}

/// `citext_hash(citext) RETURNS int4` — `hash_any(lower(txt))`.
fn fc_citext_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let scratch = MemoryContext::new("citext hash");
    let txt = arg_text(fcinfo, 0);
    match lower(scratch.mcx(), txt) {
        Ok(str) => Datum::from_i32(hashfn::hash_bytes(&str) as i32),
        Err(e) => raise(e),
    }
}

/// `citext_hash_extended(citext, int8) RETURNS int8`.
fn fc_citext_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let scratch = MemoryContext::new("citext hash extended");
    let txt = arg_text(fcinfo, 0);
    let seed = fcinfo
        .arg(1)
        .expect("citext_hash_extended: missing int8 seed")
        .value
        .as_i64() as u64;
    match lower(scratch.mcx(), txt) {
        Ok(str) => Datum::from_i64(hashfn::hash_bytes_extended(&str, seed) as i64),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// Operator functions
// ===========================================================================

/// `citext_eq`/`citext_ne`: lowercase both, then a bitwise compare (equality
/// only — no collation needed, matching C's `strcmp` on the folded strings).
fn citext_eq_impl(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<bool> {
    let scratch = MemoryContext::new("citext eq");
    let mcx = scratch.mcx();
    let lcstr = lower(mcx, arg_text(fcinfo, 0))?;
    let rcstr = lower(mcx, arg_text(fcinfo, 1))?;
    Ok(lcstr == rcstr)
}

fn fc_citext_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match citext_eq_impl(fcinfo) {
        Ok(b) => Datum::from_bool(b),
        Err(e) => raise(e),
    }
}

fn fc_citext_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match citext_eq_impl(fcinfo) {
        Ok(b) => Datum::from_bool(!b),
        Err(e) => raise(e),
    }
}

/// Shared body for the four ordering operators driven by `citextcmp`.
fn citext_cmp_op(
    fcinfo: &mut FunctionCallInfoBaseData,
    pred: fn(i32) -> bool,
) -> Datum {
    let collid = collation(fcinfo);
    let left = arg_text(fcinfo, 0);
    let right = arg_text(fcinfo, 1);
    match citextcmp(left, right, collid) {
        Ok(r) => Datum::from_bool(pred(r)),
        Err(e) => raise(e),
    }
}

fn fc_citext_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_cmp_op(fcinfo, |r| r < 0)
}
fn fc_citext_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_cmp_op(fcinfo, |r| r <= 0)
}
fn fc_citext_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_cmp_op(fcinfo, |r| r > 0)
}
fn fc_citext_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_cmp_op(fcinfo, |r| r >= 0)
}

/// Shared body for the four pattern ordering operators driven by
/// `internal_citext_pattern_cmp`.
fn citext_pattern_cmp_op(
    fcinfo: &mut FunctionCallInfoBaseData,
    pred: fn(i32) -> bool,
) -> Datum {
    let left = arg_text(fcinfo, 0);
    let right = arg_text(fcinfo, 1);
    match internal_citext_pattern_cmp(left, right) {
        Ok(r) => Datum::from_bool(pred(r)),
        Err(e) => raise(e),
    }
}

fn fc_citext_pattern_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_pattern_cmp_op(fcinfo, |r| r < 0)
}
fn fc_citext_pattern_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_pattern_cmp_op(fcinfo, |r| r <= 0)
}
fn fc_citext_pattern_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_pattern_cmp_op(fcinfo, |r| r > 0)
}
fn fc_citext_pattern_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_pattern_cmp_op(fcinfo, |r| r >= 0)
}

// ===========================================================================
// Aggregate transition functions
// ===========================================================================

/// `citext_smaller`/`citext_larger` — return whichever operand sorts
/// smaller/larger, as the verbatim input text (citext.c `PG_RETURN_TEXT_P`).
fn citext_minmax(fcinfo: &mut FunctionCallInfoBaseData, want_smaller: bool) -> Datum {
    let collid = collation(fcinfo);
    let left = arg_text(fcinfo, 0).to_vec();
    let right = arg_text(fcinfo, 1).to_vec();
    let cmp = match citextcmp(&left, &right, collid) {
        Ok(r) => r,
        Err(e) => raise(e),
    };
    let take_left = if want_smaller { cmp < 0 } else { cmp > 0 };
    let result = if take_left { &left } else { &right };
    ret_text(fcinfo, result)
}

fn fc_citext_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_minmax(fcinfo, true)
}
fn fc_citext_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    citext_minmax(fcinfo, false)
}

// ===========================================================================
// Builtin-library registration
// ===========================================================================

/// Resolve a symbol of the `citext` module to its ported `PGFunction`. Returns
/// `None` for an unknown symbol, exactly as the OS loader would fail to find it
/// in `citext.so`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "citext_eq" => Some(fc_citext_eq),
        "citext_ne" => Some(fc_citext_ne),
        "citext_lt" => Some(fc_citext_lt),
        "citext_le" => Some(fc_citext_le),
        "citext_gt" => Some(fc_citext_gt),
        "citext_ge" => Some(fc_citext_ge),
        "citext_cmp" => Some(fc_citext_cmp),
        "citext_hash" => Some(fc_citext_hash),
        "citext_hash_extended" => Some(fc_citext_hash_extended),
        "citext_pattern_lt" => Some(fc_citext_pattern_lt),
        "citext_pattern_le" => Some(fc_citext_pattern_le),
        "citext_pattern_gt" => Some(fc_citext_pattern_gt),
        "citext_pattern_ge" => Some(fc_citext_pattern_ge),
        "citext_pattern_cmp" => Some(fc_citext_pattern_cmp),
        "citext_smaller" => Some(fc_citext_smaller),
        "citext_larger" => Some(fc_citext_larger),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        // PG_FUNCTION_INFO_V1 declares api_version 1.
        api_version: 1,
    })
}

/// Install this unit's inward seams: register the `citext` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    dfmgr_seams::register_builtin_library(dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // citext.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}
