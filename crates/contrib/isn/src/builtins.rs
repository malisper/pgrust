//! isn.c's C-language functions, dispatched through the `dfmgr` builtin-library
//! registry (the SQL script resolves `$libdir/isn`). The comparison, btree, and
//! hash functions are `LANGUAGE internal` over int8 and never reach here.

use datum::Datum;
use mcx::Mcx;
use types_error::PgResult;
use types_fmgr::{cstring_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use crate::{ean2isn, ean2string, string2ean, Ean13, IsnType, MAXEAN13LEN};

const LIBRARY: &str = "isn";

/// C's `g_weak` global: the `isn.weak` GUC, read from the placeholder store.
fn g_weak() -> bool {
    match guc::GetConfigOption("isn.weak", true, false) {
        Ok(Some(s)) => adt_bool::parse_bool(&s).unwrap_or(false),
        _ => false,
    }
}

fn cstr_out(mcx: Mcx<'_>, buf: &[u8]) -> PgResult<Datum> {
    let end = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    let mut out = mcx::vec_with_capacity_in(mcx, end + 1)?;
    out.extend_from_slice(&buf[..end]);
    out.push(0);
    Ok(cstring_result(out))
}

fn input(fcinfo: &mut Fcinfo, accept: IsnType) -> PgResult<Datum> {
    // SAFETY: strict arg 0 is a non-null cstring; the escontext (if any) is a
    // live ErrorSaveNode armed for this call.
    let str = unsafe { fcinfo.arg_cstring(0) };
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(match string2ean(str.to_bytes(), esc, accept, g_weak())? {
        // C's PG_RETURN_NULL when string2ean soft-fails; the caller checks
        // error_occurred first, so the value is inspected only on success.
        Some(v) => Datum::from_i64(v as i64),
        None => Datum::null(),
    })
}

fn output(fcinfo: &mut Fcinfo, short_type: bool) -> PgResult<Datum> {
    let val = fcinfo.arg_i64(0) as Ean13;
    let mut buf = [0u8; MAXEAN13LEN + 1];
    ean2string(val, &mut buf, short_type)?;
    cstr_out(fcinfo.result_mcx(), &buf)
}

fn cast(fcinfo: &mut Fcinfo, accept: IsnType) -> PgResult<Datum> {
    let val = fcinfo.arg_i64(0) as Ean13;
    Ok(Datum::from_i64(ean2isn(val, accept)? as i64))
}

fn fc_ean13_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    input(fcinfo, IsnType::Ean13)
}
fn fc_isbn_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    input(fcinfo, IsnType::Isbn)
}
fn fc_ismn_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    input(fcinfo, IsnType::Ismn)
}
fn fc_issn_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    input(fcinfo, IsnType::Issn)
}
fn fc_upc_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    input(fcinfo, IsnType::Upc)
}

fn fc_ean13_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    output(fcinfo, false)
}
fn fc_isn_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    output(fcinfo, true)
}

fn fc_isbn_cast_from_ean13(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    cast(fcinfo, IsnType::Isbn)
}
fn fc_ismn_cast_from_ean13(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    cast(fcinfo, IsnType::Ismn)
}
fn fc_issn_cast_from_ean13(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    cast(fcinfo, IsnType::Issn)
}
fn fc_upc_cast_from_ean13(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    cast(fcinfo, IsnType::Upc)
}

/// `is_valid`: false when the "invalid-check-digit-on-input" flag (bit 0) is set.
fn fc_is_valid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let val = fcinfo.arg_i64(0) as Ean13;
    Ok(Datum::from_bool((val & 1) == 0))
}

/// `make_valid`: clear the invalid-check-digit flag.
fn fc_make_valid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let val = fcinfo.arg_i64(0) as Ean13;
    Ok(Datum::from_i64((val & !1u64) as i64))
}

/// `accept_weak_input` (SQL `isn_weak(boolean)`): flip the isn.weak GUC.
fn fc_accept_weak_input(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let newvalue = fcinfo.arg_bool(0);
    guc::SetConfigOption(
        "isn.weak",
        Some(if newvalue { "on" } else { "off" }),
        types_guc::GucContext::PGC_USERSET,
        types_guc::GucSource::PGC_S_SESSION,
    )?;
    Ok(Datum::from_bool(g_weak()))
}

/// `weak_input_status` (SQL `isn_weak()`): read the isn.weak GUC.
fn fc_weak_input_status(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(g_weak()))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "ean13_in" => fc_ean13_in,
        "ean13_out" => fc_ean13_out,
        "isbn_in" => fc_isbn_in,
        "ismn_in" => fc_ismn_in,
        "issn_in" => fc_issn_in,
        "upc_in" => fc_upc_in,
        "isn_out" => fc_isn_out,
        "isbn_cast_from_ean13" => fc_isbn_cast_from_ean13,
        "ismn_cast_from_ean13" => fc_ismn_cast_from_ean13,
        "issn_cast_from_ean13" => fc_issn_cast_from_ean13,
        "upc_cast_from_ean13" => fc_upc_cast_from_ean13,
        "is_valid" => fc_is_valid,
        "make_valid" => fc_make_valid,
        "accept_weak_input" => fc_accept_weak_input,
        "weak_input_status" => fc_weak_input_status,
        _ => return None,
    })
}

/// Install this unit's inward seam: register the `isn` module with the
/// dynamic-loader's builtin-library registry.
pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // isn.c's _PG_init only defines the isn.weak GUC (placeholder store)
        // and validates the tables under assertions; nothing to run here.
        pg_init: None,
    });
}
