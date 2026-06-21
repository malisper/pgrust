//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `tsgistidx.c` I/O functions `gtsvectorin` (oid 3646) and `gtsvectorout`
//! (oid 3647) — the `gtsvector` type's input/output procedures.
//!
//! These two are the only `tsgistidx.c` entry points expressible on the scalar/
//! by-ref fmgr boundary: `gtsvectorin(cstring) -> gtsvector` (always errors —
//! there is no need to support input of gtsvectors) and `gtsvectorout(gtsvector)
//! -> cstring` (renders the `SignTSVector` key as a human-readable summary).
//! [`register_tsgistidx_builtins`] registers both rows into the fmgr-core
//! builtin table (C: their `fmgr_builtins[]` rows) so by-OID / by-name dispatch
//! and `fmgr_info` resolve them — without which `gtsvectorout(...)` / a cast to
//! text of a `gtsvector` errors `internal function "gtsvectorout" is not in
//! internal lookup table`.
//!
//! The remaining `tsgistidx.c` entry points (`gtsvector_compress` /
//! `_decompress` / `_consistent` / `_union` / `_same` / `_penalty` /
//! `_picksplit` / `_options`) are GiST opclass support procedures dispatched by
//! the GiST AM through the typed by-OID opclass dispatch, not the fmgr frame;
//! they are not registered here.

use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_tsearch::tsgistidx::SignTsVector;

/// `VARHDRSZ` — the uncompressed varlena length-word size, in bytes.
const VARHDRSZ: usize = 4;

/// Decode a `gtsvector` arg off the by-ref lane: the value arrives as its full
/// header-ful `SignTSVector` varlena image (size word at offset 4); strip the
/// 4-byte varlena header and parse the `flag` + `data[]` body.
fn arg_gtsvector(fcinfo: &FunctionCallInfoBaseData, i: usize) -> SignTsVector {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("gtsvector fn: by-ref gtsvector arg missing from by-ref lane");
    let body = if image.len() >= VARHDRSZ {
        &image[VARHDRSZ..]
    } else {
        &image[..]
    };
    SignTsVector::from_image(body).expect("gtsvectorout: corrupt gtsvector key image")
}

/// `gtsvectorin(cstring) -> gtsvector` (tsgistidx.c:88) — always errors
/// (`cannot accept a value of type gtsvector`). Mirrors the C body, which never
/// reads its argument.
fn fc_gtsvectorin(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    crate::gtsvectorin()?;
    // Unreachable: gtsvectorin always returns Err above.
    Ok(Datum::null())
}

/// `gtsvectorout(gtsvector) -> cstring` (tsgistidx.c:99) — render the key as
/// text. The result crosses the by-ref lane as a `cstring`.
fn fc_gtsvectorout(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let key = arg_gtsvector(fcinfo, 0);
    let out = crate::gtsvectorout(&key);
    fcinfo.set_ref_result(RefPayload::Cstring(out));
    Ok(Datum::from_usize(0))
}

fn builtin(foid: u32, name: &str, nargs: i16, native: PgFnNative) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            // Both `gtsvectorin`/`gtsvectorout` are `proisstrict => 't'` (the
            // default) and not proretset in pg_proc.dat.
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the SQL-callable `tsgistidx.c` I/O builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs /
/// nargs / strict / retset transcribed exactly from `pg_proc.dat`.
pub fn register_tsgistidx_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(3646, "gtsvectorin", 1, fc_gtsvectorin),
        builtin(3647, "gtsvectorout", 1, fc_gtsvectorout),
    ]);
}
