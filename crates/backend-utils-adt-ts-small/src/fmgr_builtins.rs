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
//! The fmgr adapters return `PgResult<Datum>` (the Result-native fmgr shape),
//! threading any `ereport(ERROR)` back through the dispatch `?` path; they use
//! `alloc::` for `String`/`Vec`.
//!
//! NOT registered here: nothing else in this unit is SQL-callable
//! (`tsquery_util.c` / `tsquery_cleanup.c` are internal toolkits with no
//! `pg_proc` row).

use alloc::string::ToString;
use alloc::vec::Vec;

use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

const VARHDRSZ: usize = 4;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `tsquery` arg's full header-ful varlena image on the by-ref lane. The value
/// cores read the size word at the FIXED offset 4 and `QueryItem`s at `HDRSIZETQ`
/// (8), so a 4-byte-header base is required (`DatumGetTSQuery` ==
/// `PG_DETOAST_DATUM` un-packs short->4B). Under `SHORT_VARLENA_PACKING` a small
/// heap-stored tsquery can be short, so un-pack here before the fixed-offset
/// struct decode. `arg_text_str` (the `text`-payload reader that also goes
/// through this) then sees a 4-byte header and strips it correctly. With the flag
/// OFF the un-pack branch is never taken (behavior-preserving). The short case
/// leaks a `'static` un-packed buffer (see the tsquery-core note for the
/// rationale; zero leak while OFF).
#[inline]
fn arg_tsquery<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("ts_rewrite fn: by-ref tsquery arg missing from by-ref lane");
    if image.first().is_some_and(|&b| b != 0x01 && (b & 0x01) == 0x01) {
        const VARHDRSZ_SHORT: usize = 1;
        let data_size = ((image[0] >> 1) & 0x7f) as usize - VARHDRSZ_SHORT;
        let new_size = data_size + VARHDRSZ;
        let mut out = Vec::with_capacity(new_size);
        out.extend_from_slice(&((new_size as u32) << 2).to_ne_bytes());
        out.extend_from_slice(&image[VARHDRSZ_SHORT..VARHDRSZ_SHORT + data_size]);
        Vec::leak(out)
    } else {
        image
    }
}

/// `VARDATA_ANY` of a header-ful `text` arg: the payload bytes after the
/// (4-byte uncompressed) length header, as a `&str` (the SPI query text).
#[inline]
fn arg_text_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = arg_tsquery(fcinfo, i);
    // VARDATA_ANY: a small stored value arrives short-headed once
    // SHORT_VARLENA_PACKING is on; skip ONE byte for a short header, else VARHDRSZ.
    let payload = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[][..],
    };
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

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `tsquery_rewrite(tsquery query, tsquery target, tsquery substitute)` (oid 3684).
fn fc_tsquery_rewrite(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let query = arg_tsquery(fcinfo, 0);
    let ex = arg_tsquery(fcinfo, 1);
    let subst = arg_tsquery(fcinfo, 2);
    let image = crate::rewrite::tsquery_rewrite(m.mcx(), query, ex, subst)?;
    Ok(ret_varlena_image(fcinfo, image))
}

/// `tsquery_rewrite_query(tsquery query, text spi_query)` (oid 3685). The `text`
/// arg is the SPI query string whose two `tsquery` columns drive each rewrite.
fn fc_tsquery_rewrite_query(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let m = scratch_mcx();
    let query = arg_tsquery(fcinfo, 0);
    let buf = arg_text_str(fcinfo, 1);
    let image = crate::rewrite::tsquery_rewrite_query(m.mcx(), query, buf)?;
    Ok(ret_varlena_image(fcinfo, image))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the `ts_rewrite` builtins (C: their `fmgr_builtins[]` rows). Called
/// from this crate's `init_seams()`. OIDs/nargs from `pg_proc.dat`; both rows
/// are `proisstrict => 't'` and not retset.
pub fn register_ts_small_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(3684, "tsquery_rewrite", 3, fc_tsquery_rewrite),
        builtin(3685, "tsquery_rewrite_query", 2, fc_tsquery_rewrite_query),
    ]);
}
