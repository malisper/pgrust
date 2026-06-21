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
//! The `gtsvector_options` (oid 3434) GiST opclass-options support procedure IS
//! registered here: `index_opclass_options` resolves it by OID and invokes it
//! through fmgr, passing the `local_relopts` on the `internal` lane. The
//! remaining `tsgistidx.c` opclass support procedures (`gtsvector_compress` /
//! `_decompress` / `_consistent` / `_union` / `_same` / `_penalty` /
//! `_picksplit`) are dispatched by the GiST AM through the typed by-OID opclass
//! dispatch, not the fmgr frame; they are not registered here.

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

/// `gtsvector_options(internal) -> void` (tsgistidx.c:799, oid 3434) — the GiST
/// `tsvector_ops` opclass-options support procedure: register the `siglen`
/// opclass option on the `local_relopts` it receives on the fmgr `internal`
/// lane. `PG_RETURN_VOID()`.
fn fc_gtsvector_options(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let relopts = fcinfo
        .ref_arg_mut(0)
        .and_then(|p| p.as_internal_mut::<types_reloptions::local_relopts>())
        .expect("gtsvector_options: args[0] is not a local_relopts internal arg");
    crate::gtsvector_options(relopts);
    Ok(Datum::null())
}

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset: false,
            func: None,
        },
        native,
    )
}

/// Register the SQL-callable `tsgistidx.c` I/O builtins plus the
/// `gtsvector_options` opclass-options support procedure (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs /
/// nargs / strict / retset transcribed exactly from `pg_proc.dat`.
pub fn register_tsgistidx_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // `gtsvectorin`/`gtsvectorout` are `proisstrict => 't'` (the default).
        builtin(3646, "gtsvectorin", 1, true, fc_gtsvectorin),
        builtin(3647, "gtsvectorout", 1, true, fc_gtsvectorout),
        // `gtsvector_options` is `proisstrict => 'f'`.
        builtin(3434, "gtsvector_options", 1, false, fc_gtsvector_options),
    ]);
}
