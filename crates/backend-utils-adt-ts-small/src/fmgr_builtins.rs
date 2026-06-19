//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `tsquery_rewrite.c` functions of this unit — the `ts_rewrite` family.
//!
//! A `tsquery` value is its flat **header-ful** varlena image (the cores read /
//! produce the `HDRSIZETQ`-headed image), so `tsquery` args/results cross
//! VERBATIM on the by-ref lane — no header strip, no re-frame. The `text`
//! sub-query argument of `tsquery_rewrite_query` (oid 3685) crosses header-ful;
//! its `VARDATA_ANY` payload (after the 4-byte length word) is the rewrite SPI
//! query string.
//!
//! This crate is `#![no_std]`; the value-core families are `no_std`/`alloc`.
//! The fmgr registration layer needs `std` for the `panic_any` ereport path, so
//! it pulls in `extern crate std` (see `lib.rs`) and uses `alloc::` for
//! `String`/`Vec`.
//!
//! NOT registered here: nothing else in this unit is SQL-callable
//! (`tsquery_util.c` / `tsquery_cleanup.c` are internal toolkits with no
//! `pg_proc` row).

use alloc::string::String;
use alloc::string::ToString;
use alloc::vec::Vec;

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

const VARHDRSZ: usize = 4;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `tsquery` arg's full header-ful varlena image on the by-ref lane (read
/// verbatim — the value cores consume the `HDRSIZETQ`-headed image).
#[inline]
fn arg_tsquery<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ts_rewrite fn: by-ref tsquery arg missing from by-ref lane")
}

/// `VARDATA_ANY` of a header-ful `text` arg: the payload bytes after the
/// (4-byte uncompressed) length header, as a `&str` (the SPI query text).
#[inline]
fn arg_text_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = arg_tsquery(fcinfo, i);
    let payload = if image.len() >= VARHDRSZ { &image[VARHDRSZ..] } else { &[][..] };
    core::str::from_utf8(payload).expect("ts_rewrite: text arg not valid UTF-8")
}

/// Set a header-ful `tsquery` varlena result on the by-ref lane verbatim (the
/// core already produced the full `HDRSIZETQ`-headed image).
#[inline]
fn ret_varlena_image(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("ts_rewrite fmgr scratch")
}

fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `tsquery_rewrite(tsquery query, tsquery target, tsquery substitute)` (oid 3684).
fn fc_tsquery_rewrite(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let query = arg_tsquery(fcinfo, 0);
    let ex = arg_tsquery(fcinfo, 1);
    let subst = arg_tsquery(fcinfo, 2);
    let image = ok(crate::rewrite::tsquery_rewrite(m.mcx(), query, ex, subst));
    ret_varlena_image(fcinfo, image)
}

/// `tsquery_rewrite_query(tsquery query, text spi_query)` (oid 3685). The `text`
/// arg is the SPI query string whose two `tsquery` columns drive each rewrite.
fn fc_tsquery_rewrite_query(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let query = arg_tsquery(fcinfo, 0);
    let buf = arg_text_str(fcinfo, 1);
    let image = ok(crate::rewrite::tsquery_rewrite_query(m.mcx(), query, buf));
    ret_varlena_image(fcinfo, image)
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register the `ts_rewrite` builtins (C: their `fmgr_builtins[]` rows). Called
/// from this crate's `init_seams()`. OIDs/nargs from `pg_proc.dat`; both rows
/// are `proisstrict => 't'` and not retset.
pub fn register_ts_small_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3684, "tsquery_rewrite", 3, fc_tsquery_rewrite),
        builtin(3685, "tsquery_rewrite_query", 2, fc_tsquery_rewrite_query),
    ]);
}
