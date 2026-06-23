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
//! through fmgr, passing the `local_relopts` on the `internal` lane.
//!
//! The remaining `tsgistidx.c` opclass support procedures (`gtsvector_compress`
//! = 3648 / `_decompress` = 3649 / `_picksplit` = 3650 / `_union` = 3651 /
//! `_same` = 3652 / `_penalty` = 3653 / `_consistent` = 3654 + the obsolete
//! `gtsvector_consistent_oldsig` = 3790) are `prolang => internal` procs. Like
//! every other GiST opclass (box/point in `backend-access-gist-proc`, inet in
//! `network_gist.c`, range in `rangetypes_gist.c`), the GiST AM resolves each by
//! OID via `index_getprocinfo` → `fmgr_info`, which — for an `internal`-language
//! proc — looks the `prosrc` name up in the fmgr builtin table
//! (`fmgr_lookupByName`) and errors `internal function "gtsvector_consistent" is
//! not in internal lookup table` when the row is absent. So every gtsvector GiST
//! support proc MUST have its `fmgr_builtins[]` row registered for `CREATE INDEX
//! ... USING gist (... tsvector_ops)` and opclass validation to resolve it —
//! exactly C's table. [`register_tsgistidx_builtins`] registers all of them
//! (matching `register_inet_gist_builtins`'s precedent).
//!
//! As with the inet/range opclasses, the faithful *invocation* of these support
//! procs is the GiST core's typed by-OID dispatch
//! (`backend-access-gist-dispatch-seams`), which reads `FmgrInfo::fn_oid` and
//! never the `fn_addr`. The `func` adapter installed in the builtin row is the
//! fmgr-frame entry the owned GiST path never enters; it raises a clear error
//! naming the dispatch seam if a future `FunctionCallNColl` ever reaches it.

use ::datum::Datum;
use ::types_error::{PgError, PgResult};
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use ::tsearch::tsgistidx::SignTsVector;

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
    let body = vardata_any(image);
    SignTsVector::from_image(body).expect("gtsvectorout: corrupt gtsvector key image")
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`.
/// A small stored value arrives short-headed once `SHORT_VARLENA_PACKING` is on; a
/// fixed 4-byte strip would drop three payload bytes. No-op while off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
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

/// The shared fmgr-frame entry point for every gtsvector GiST opclass support
/// proc (`gtsvector_consistent` / `_union` / `_compress` / `_decompress` /
/// `_penalty` / `_picksplit` / `_same`). In the owned model the GiST access
/// method invokes these procs through the typed by-OID dispatch
/// (`backend-access-gist-dispatch-seams` → `backend-access-gist-proc`), reading
/// `FmgrInfo::fn_oid` — never `fn_addr`. This frame entry therefore is not
/// reached on the owned GiST path; it exists so the `fmgr_builtins[]` row carries
/// a non-`None` callable (matching C's table), and raises a clear error if a
/// future fmgr-frame call site is added, pointing at the dispatch seam to use.
fn fc_gtsvector_support_via_dispatch(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let foid = fcinfo.flinfo.as_ref().map(|fi| fi.fn_oid).unwrap_or(0);
    Err(PgError::error(format!(
        "gtsvector GiST support function (OID {foid}) must be invoked through the \
         typed GiST opclass dispatch (backend-access-gist-dispatch-seams / \
         backend-access-gist-proc), not the fmgr frame; the owned GiST access \
         method dispatches these by FmgrInfo.fn_oid"
    )))
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
    fmgr_core::register_builtins_native([
        // `gtsvectorin`/`gtsvectorout` are `proisstrict => 't'` (the default).
        builtin(3646, "gtsvectorin", 1, true, fc_gtsvectorin),
        builtin(3647, "gtsvectorout", 1, true, fc_gtsvectorout),
        // `gtsvector_options` is `proisstrict => 'f'`.
        builtin(3434, "gtsvector_options", 1, false, fc_gtsvector_options),
        // The GiST `tsvector_ops` opclass support procedures (all
        // `proisstrict => 't'` — the default — and not retset). OIDs / nargs
        // transcribed from `pg_proc.dat`; the bodies live in this crate
        // (`crate::gtsvector_*`) and are invoked through the GiST core's typed
        // by-OID dispatch, so the row's `func` adapter is the never-entered
        // dispatch-frame stub.
        builtin(3648, "gtsvector_compress", 1, true, fc_gtsvector_support_via_dispatch),
        builtin(3649, "gtsvector_decompress", 1, true, fc_gtsvector_support_via_dispatch),
        builtin(3650, "gtsvector_picksplit", 2, true, fc_gtsvector_support_via_dispatch),
        builtin(3651, "gtsvector_union", 2, true, fc_gtsvector_support_via_dispatch),
        builtin(3652, "gtsvector_same", 3, true, fc_gtsvector_support_via_dispatch),
        builtin(3653, "gtsvector_penalty", 3, true, fc_gtsvector_support_via_dispatch),
        builtin(3654, "gtsvector_consistent", 5, true, fc_gtsvector_support_via_dispatch),
        // `gtsvector_consistent_oldsig` (obsolete signature, prosrc differs).
        builtin(3790, "gtsvector_consistent_oldsig", 5, true, fc_gtsvector_support_via_dispatch),
    ]);
}
