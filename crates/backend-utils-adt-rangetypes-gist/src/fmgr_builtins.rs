//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the range /
//! multirange GiST opclass support procedures ported in this crate
//! (`rangetypes_gist.c`): the `range_ops` `consistent` / `union` / `penalty` /
//! `picksplit` / `same` methods and the `multirange_ops`
//! `consistent` / `compress` methods.
//!
//! These functions are `prolang => internal` opclass support procedures (every
//! row carries internal-language args — `GISTENTRY *`, the `internal` query, the
//! `GIST_SPLITVEC *` etc.). In C they are real `fmgr_builtins[]` rows whose
//! `fn_addr` the GiST access method invokes through `FunctionCallNColl`. The
//! port replaced that fmgr-frame call with a typed, by-OID dispatch through
//! `backend-access-gist-dispatch-seams` (whose box/point/inet/range arms are
//! installed by `backend-access-gist-proc`): `initGISTstate` resolves each
//! support proc into a `GISTSTATE` `FmgrInfo` slot, and the scan/insert/vacuum
//! machinery dispatches on `FmgrInfo::fn_oid` (e.g. `gist_scan.rs`'s
//! `gist_consistent::call(proc_oid, ..)`), never on `fn_addr`.
//!
//! `initGISTstate` builds each slot with `index_getprocinfo` → `fmgr_info`,
//! which — for an `internal`-language proc — looks the prosrc name up in the
//! fmgr builtin table (`fmgr_lookupByName`) and errors
//! (`internal function "..." is not in internal lookup table`) when it is
//! absent. So every range/multirange GiST support proc MUST have its
//! `fmgr_builtins[]` row registered for `CREATE INDEX ... USING gist` over a
//! range/multirange column (and any opclass validation / `fmgr_isbuiltin` fast
//! path) to resolve it — exactly C's table.
//!
//! Because the port's faithful invocation of these procs IS the by-OID typed
//! dispatch (the `fn_addr` is structurally never reached through the fmgr
//! frame: the AM reads `fn_oid` and calls the dispatch seam), the `func`
//! adapter here is the fmgr-frame entry point that the port never enters. If it
//! ever is entered (a future C-faithful `FunctionCallNColl` on a range GiST
//! support proc's `FmgrInfo`), it raises a clear `ereport(ERROR)` naming the
//! typed dispatch seam to route through — it does NOT silently fabricate a
//! `GISTENTRY` (which the fmgr frame does not carry in the owned model). The
//! real bodies are ported in [`crate`] and reached through the dispatch.
//!
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`
//! (every row is `proisstrict => 't'` — the default — and not retset).

use types_datum::datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// The shared fmgr-frame entry point for every range/multirange GiST opclass
/// support proc. In the owned model the GiST access method invokes these procs
/// through the typed by-OID dispatch (`backend-access-gist-dispatch-seams`),
/// reading `FmgrInfo::fn_oid` — never `fn_addr`. This frame entry therefore is
/// never reached on any port path; it exists so the `fmgr_builtins[]` row
/// carries a non-`None` callable (matching C's table, where `fn_addr` is the
/// real C function). It raises a clear error if a future fmgr-frame call site is
/// added, pointing at the dispatch seam to use instead.
fn fc_gist_support_via_dispatch(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let foid = fcinfo
        .flinfo
        .as_ref()
        .map(|fi| fi.fn_oid)
        .unwrap_or(0);
    std::panic::panic_any(types_error::PgError::error(format!(
        "range/multirange GiST support function (OID {foid}) must be invoked \
         through the typed opclass dispatch (backend-access-gist-dispatch-seams), \
         not the fmgr frame; the owned GiST access method dispatches these by \
         FmgrInfo.fn_oid"
    )));
}

fn builtin(foid: u32, name: &str, nargs: i16) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        // Every rangetypes_gist.c support proc is proisstrict => 't' (the
        // default) and not proretset in pg_proc.dat.
        strict: true,
        retset: false,
        func: Some(fc_gist_support_via_dispatch),
    }
}

/// Register the `fmgr_builtins[]` rows for every range/multirange GiST opclass
/// support procedure owned by this crate (C: their `fmgr_builtins[]` rows).
/// Called from this crate's [`crate::init_seams`]. Resolving these rows is what
/// lets `index_getprocinfo` → `fmgr_info` build the `GISTSTATE` `FmgrInfo` slots
/// (without which `CREATE INDEX ... USING gist` over a range column errors
/// `internal function "..." is not in internal lookup table`). OIDs / nargs from
/// `pg_proc.dat`.
///
/// NOTE: the `range_ops` opclass has no `GIST_COMPRESS_PROC` (range GiST keys
/// are stored as plain `RangeType *`, never compressed/decompressed), so no
/// `range_gist_compress` row exists. `multirange_ops` carries both a `compress`
/// (6156, which collapses a multirange to its union range) and a `consistent`
/// (6154).
pub fn register_rangetypes_gist_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- range_ops opclass ----
        builtin(3875, "range_gist_consistent", 5),
        builtin(3876, "range_gist_union", 2),
        builtin(3879, "range_gist_penalty", 3),
        builtin(3880, "range_gist_picksplit", 2),
        builtin(3881, "range_gist_same", 3),
        // `range_sortsupport` (the GiST sorted-build comparator installer).
        // Bodied (`range_fast_cmp`) and routed through the GiST sortsupport
        // dispatch; lives in `rangetypes.c` but is a `range_ops` GiST support
        // proc, so its `fmgr_builtins[]` row is registered here next to the
        // opclass it serves.
        builtin(6391, "range_sortsupport", 1),
        // ---- multirange_ops opclass ----
        builtin(6154, "multirange_gist_consistent", 5),
        builtin(6156, "multirange_gist_compress", 1),
    ]);
}
