//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the GiST geometric
//! opclass support procedures ported in this crate (`gistproc.c`): the box,
//! point, polygon and circle `consistent` / `union` / `compress` / `penalty` /
//! `picksplit` / `same` / `distance` / `fetch` / `sortsupport` methods.
//!
//! These functions are `prolang => internal` opclass support procedures (every
//! row carries internal-language args — `GISTENTRY *`, the `internal` query, the
//! `GIST_SPLITVEC *` etc.). In C they are real `fmgr_builtins[]` rows whose
//! `fn_addr` the GiST access method invokes through `FunctionCallNColl`. The
//! port replaced that fmgr-frame call with a typed, by-OID dispatch through
//! `backend-access-gist-dispatch-seams` (installed by this crate's
//! `init_seams`): `initGISTstate` resolves each support proc into a `GISTSTATE`
//! `FmgrInfo` slot, and the scan/insert/vacuum machinery dispatches on
//! `FmgrInfo::fn_oid` (e.g. `gist_scan.rs`'s `gist_consistent::call(proc_oid,
//! ..)`), never on `fn_addr`.
//!
//! `initGISTstate` builds each slot with `index_getprocinfo` → `fmgr_info`,
//! which — for an `internal`-language proc — looks the prosrc name up in the
//! fmgr builtin table (`fmgr_lookupByName`) and errors
//! (`internal function "..." is not in internal lookup table`) when it is
//! absent. So every GiST support proc MUST have its `fmgr_builtins[]` row
//! registered for `CREATE INDEX ... USING gist` (and any opclass validation /
//! `fmgr_isbuiltin` fast path) to resolve it — exactly C's table.
//!
//! Because the port's faithful invocation of these procs IS the by-OID typed
//! dispatch (the `fn_addr` is structurally never reached through the fmgr
//! frame: the AM reads `fn_oid` and calls the dispatch seam), the `func`
//! adapter here is the fmgr-frame entry point that the port never enters. If it
//! ever is entered (a future C-faithful `FunctionCallNColl` on a GiST support
//! proc's `FmgrInfo`), it raises a clear `ereport(ERROR)` naming the typed
//! dispatch seam to route through — it does NOT silently fabricate a `GISTENTRY`
//! (which the fmgr frame does not carry in the owned model). The real bodies are
//! ported in [`crate`] and reached through the dispatch.
//!
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`
//! (every row is `proisstrict => 't'` — the default — and not retset).

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// The shared fmgr-frame entry point for every GiST opclass support proc. In
/// the owned model the GiST access method invokes these procs through the typed
/// by-OID dispatch (`backend-access-gist-dispatch-seams`), reading
/// `FmgrInfo::fn_oid` — never `fn_addr`. This frame entry therefore is never
/// reached on any port path; it exists so the `fmgr_builtins[]` row carries a
/// non-`None` callable (matching C's table, where `fn_addr` is the real C
/// function). It raises a clear error if a future fmgr-frame call site is added,
/// pointing at the dispatch seam to use instead.
fn fc_gist_support_via_dispatch(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let foid = fcinfo
        .flinfo
        .as_ref()
        .map(|fi| fi.fn_oid)
        .unwrap_or(0);
    Err(types_error::PgError::error(format!(
        "GiST support function (OID {foid}) must be invoked through the typed \
         opclass dispatch (backend-access-gist-dispatch-seams), not the fmgr \
         frame; the owned GiST access method dispatches these by FmgrInfo.fn_oid"
    )))
}

fn builtin(foid: u32, name: &str, nargs: i16) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            // Every gistproc.c support proc is proisstrict => 't' (the default)
            // and not proretset in pg_proc.dat.
            strict: true,
            retset: false,
            func: None,
        },
        fc_gist_support_via_dispatch,
    )
}

/// Register the `fmgr_builtins[]` rows for every GiST box/point/polygon/circle
/// opclass support procedure owned by this crate (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. Resolving these rows is what
/// lets `index_getprocinfo` → `fmgr_info` build the `GISTSTATE` `FmgrInfo` slots
/// (without which `CREATE INDEX ... USING gist` errors `internal function "..."
/// is not in internal lookup table`). OIDs / nargs from `pg_proc.dat`.
pub fn register_gist_proc_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // ---- box opclass ----
        builtin(2578, "gist_box_consistent", 5),
        builtin(2581, "gist_box_penalty", 3),
        builtin(2582, "gist_box_picksplit", 2),
        builtin(2583, "gist_box_union", 2),
        builtin(2584, "gist_box_same", 3),
        builtin(3998, "gist_box_distance", 5),
        // ---- point opclass ----
        builtin(1030, "gist_point_compress", 1),
        builtin(2179, "gist_point_consistent", 5),
        builtin(3064, "gist_point_distance", 5),
        builtin(3282, "gist_point_fetch", 1),
        builtin(3435, "gist_point_sortsupport", 1),
        // ---- polygon opclass ----
        builtin(2585, "gist_poly_consistent", 5),
        builtin(2586, "gist_poly_compress", 1),
        builtin(3288, "gist_poly_distance", 5),
        // ---- circle opclass ----
        builtin(2591, "gist_circle_consistent", 5),
        builtin(2592, "gist_circle_compress", 1),
        builtin(3280, "gist_circle_distance", 5),
    ]);
}
