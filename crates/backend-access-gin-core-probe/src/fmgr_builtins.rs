//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the GIN
//! `anyarray_ops` opclass support procedures ported in this crate
//! (`ginarrayproc.c`): `ginarrayextract` / `ginarrayextract_2args` /
//! `ginqueryarrayextract` / `ginarrayconsistent` / `ginarraytriconsistent`.
//!
//! These are `prolang => internal` opclass support procedures whose rows carry
//! `internal`-language out-parameters (`*nentries`, `**nullFlags`,
//! `*searchMode`, `*recheck`). In C they are real `fmgr_builtins[]` rows whose
//! `fn_addr` the GIN access method invokes through `FunctionCallNColl`. The port
//! replaced that fmgr-frame call with a typed, by-OID dispatch through the
//! `ginutil-seams` / `core-probe-seams` GIN dispatch seams (installed by this
//! crate's `init_seams`): `initGinState` resolves each support proc into a
//! `GinState` `FmgrInfo` slot, and the scan/build machinery dispatches on
//! `FmgrInfo::fn_oid` (see [`crate::dispatch`]), never on `fn_addr`.
//!
//! `initGinState` builds each slot with `index_getprocinfo` → `fmgr_info`,
//! which — for an `internal`-language proc — looks the prosrc name up in the
//! fmgr builtin table (`fmgr_lookupByName`) and errors (`internal function
//! "ginarrayextract" is not in internal lookup table`) when it is absent. So
//! every GIN array support proc MUST have its `fmgr_builtins[]` row registered
//! for `CREATE INDEX ... USING gin` (which extracts entries) and any GIN scan to
//! resolve it — exactly C's table.
//!
//! Because the port's faithful invocation of these procs IS the by-OID typed
//! dispatch (the `fn_addr` is structurally never reached through the fmgr
//! frame), the `func` adapter here is an entry point the port never enters; if a
//! future C-faithful `FunctionCallNColl` ever reaches it, it raises a clear
//! `ereport(ERROR)` naming the dispatch seam to route through. The real bodies
//! are in [`crate::ginarrayproc`] and reached through [`crate::dispatch`].
//!
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`
//! (every row is `proisstrict => 't'` and not `proretset`).

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// The shared fmgr-frame entry point for every GIN `anyarray_ops` support proc.
/// In the owned model the GIN access method invokes these procs through the
/// typed by-OID dispatch ([`crate::dispatch`]), reading `FmgrInfo::fn_oid` —
/// never `fn_addr`. This frame entry therefore is never reached on any port
/// path; it exists so the `fmgr_builtins[]` row carries a non-`None` callable
/// (matching C's table). It raises a clear error if a future fmgr-frame call
/// site is added, pointing at the dispatch seam to use instead.
fn fc_gin_support_via_dispatch(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let foid = fcinfo
        .flinfo
        .as_ref()
        .map(|fi| fi.fn_oid)
        .unwrap_or(0);
    Err(types_error::PgError::error(format!(
        "GIN anyarray_ops support function (OID {foid}) must be invoked through \
         the typed opclass dispatch (gin_extract_value / gin_extract_query / \
         gin_consistent_call_{{bool,tri}} seams), not the fmgr frame; the owned \
         GIN access method dispatches these by FmgrInfo.fn_oid"
    )))
}

fn builtin(foid: u32, name: &str, nargs: i16) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            // Every ginarrayproc.c support proc is proisstrict => 't' and not
            // proretset in pg_proc.dat.
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        fc_gin_support_via_dispatch,
    )
}

/// Register the `fmgr_builtins[]` rows for the GIN `anyarray_ops` opclass support
/// procedures owned by this crate (C: their `fmgr_builtins[]` rows). Called from
/// this crate's `init_seams()`. Resolving these rows is what lets
/// `index_getprocinfo` → `fmgr_info` build the `GinState` `FmgrInfo` slots
/// (without which `CREATE INDEX ... USING gin` errors `internal function
/// "ginarrayextract" is not in internal lookup table`). OIDs / nargs from
/// `pg_proc.dat`.
pub fn register_gin_array_proc_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // ---- anyarray_ops ----
        builtin(2743, "ginarrayextract", 3),
        // OID 3076: pg_proc.dat `proname => 'ginarrayextract'` but
        // `prosrc => 'ginarrayextract_2args'` — fmgr resolves by prosrc name.
        builtin(3076, "ginarrayextract_2args", 2),
        builtin(2774, "ginqueryarrayextract", 7),
        builtin(2744, "ginarrayconsistent", 8),
        builtin(3920, "ginarraytriconsistent", 7),
    ]);
}
