//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `jsonb_gin.c` support functions whose argument/result types are expressible
//! at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_jsonb_gin_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`). OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! Scope: `gin_compare_jsonb` (oid 3480) is registered with its real fmgr-frame
//! adapter (two `text` keys → `int32`, expressible on the by-ref lane). The
//! eight other jsonb GIN support procs (`gin_extract_jsonb`/
//! `gin_extract_jsonb_query`/`gin_consistent_jsonb`/`gin_triconsistent_jsonb`
//! and the `_path` family) take the GIN dispatch out-parameter pointers
//! (`int32 *nentries`, `bool *recheck`, `Datum **extra_data`, `Pointer
//! **extra_data`, the `bool[]`/`GinTernaryValue[]` check vectors) which are not
//! expressible on the scalar/by-ref fmgr call frame; they reach their value
//! cores through [`backend_utils_adt_jsonb_gin_seams`] (the GIN by-OID
//! support-proc dispatcher), not the fmgr-frame `func`.
//!
//! Nonetheless every one of these eight procs MUST have its `fmgr_builtins[]`
//! row registered, exactly as in C: `initGinState` builds each opclass support
//! slot via `index_getprocinfo` → `fmgr_info`, which — for an `internal`-language
//! proc — looks the prosrc name up in the fmgr builtin table (`fmgr_lookupByName`)
//! and errors (`internal function "..." is not in internal lookup table`) when it
//! is absent. Without these rows `CREATE INDEX ... USING gin (j)` /
//! `(j jsonb_path_ops)` fails before any scan. So we register all eight as
//! placeholder rows whose `func` is a clear dispatch-error frame entry (the port
//! never reaches it: the GIN access method dispatches these by `FmgrInfo.fn_oid`
//! through the typed seam, never through `fn_addr`) — the identical pattern used
//! by `backend-access-gist-proc` for the GiST opclass support procs.

use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text` arg's `VARDATA_ANY` payload bytes. C: `PG_GETARG_TEXT_PP(i)` then
/// `VARDATA_ANY` (`gin_compare_jsonb` calls `bttextcmp`, which reads `VARDATA_ANY`
/// of each arg). The by-ref lane carries the RAW varlena image (header included,
/// `as_varlena()` does not strip it), so strip the header here: a short (1-byte,
/// low-bit-set) header skips ONE byte, an ordinary 4-byte header skips `VARHDRSZ`.
/// The two GIN keys this compares are a STORED entry key (`make_text_key` -> small
/// text, short-packed when stored as the GIN entry tuple once
/// `SHORT_VARLENA_PACKING` is on) and a fresh query key (4-byte); comparing the
/// raw images would compare differing headers and never match identical payloads
/// (= the jsonb_ops GIN `@>`/`?` returns 0 rows). No-op while the flag is off
/// (both keys are 4-byte). Mirrors `backend-utils-adt-varlena`'s
/// `arg_bytes`/`vardata_any_slice` used by `fc_bttextcmp`.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("gin_compare_jsonb: text arg missing from by-ref lane");
    const VARHDRSZ: usize = 4;
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// C: `gin_compare_jsonb(PG_FUNCTION_ARGS)`. Two `text` GIN keys → `int32`
/// comparison result (always under the C collation, i.e. a plain unsigned byte
/// compare).
fn fc_gin_compare_jsonb(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_text(fcinfo, 0);
    let b = arg_text(fcinfo, 1);
    Ok(ret_i32(crate::gin_compare_jsonb(a, b)))
}

/// The shared fmgr-frame entry point for the jsonb GIN extract/consistent/
/// triconsistent support procs. In the owned model the GIN access method invokes
/// these procs through the typed by-OID dispatch
/// ([`backend_utils_adt_jsonb_gin_seams`]), reading `FmgrInfo::fn_oid` — never
/// `fn_addr`. This frame entry is therefore never reached on any port path; it
/// exists so the `fmgr_builtins[]` row carries a non-`None` callable (matching
/// C's table, where `fn_addr` is the real C function). It raises a clear error if
/// a future fmgr-frame call site is added, pointing at the dispatch seam.
fn fc_jsonb_gin_via_dispatch(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let foid = fcinfo.flinfo.as_ref().map(|fi| fi.fn_oid).unwrap_or(0);
    Err(types_error::PgError::error(alloc::format!(
        "jsonb GIN support function (OID {foid}) must be invoked through the typed \
         opclass dispatch (backend-utils-adt-jsonb-gin-seams), not the fmgr frame; \
         the owned GIN access method dispatches these by FmgrInfo.fn_oid"
    )))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: alloc::string::ToString::to_string(name),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the `jsonb_gin.c` builtins (C: their `fmgr_builtins[]` rows). Called
/// from this crate's [`crate::init_seams`]. OIDs / nargs / strict / retset
/// transcribed from `pg_proc.dat`. `gin_compare_jsonb` (3480: `proargtypes =>
/// 'text text'`, `prorettype => 'int4'`) gets its real fmgr-frame adapter; the
/// eight GIN extract/consistent/triconsistent procs (jsonb_ops + jsonb_path_ops)
/// get the dispatch-error frame entry — they are invoked by-OID through the typed
/// seam, but their `fmgr_builtins[]` rows must exist so `fmgr_info` resolves them
/// during `CREATE INDEX ... USING gin`. All are `proisstrict => 't'` (the
/// `BKI_DEFAULT`) and not proretset.
pub fn register_jsonb_gin_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(3480, "gin_compare_jsonb", 2, true, false, fc_gin_compare_jsonb),
        // ---- jsonb_ops support procs ----
        builtin(3482, "gin_extract_jsonb", 3, true, false, fc_jsonb_gin_via_dispatch),
        builtin(3483, "gin_extract_jsonb_query", 7, true, false, fc_jsonb_gin_via_dispatch),
        builtin(3484, "gin_consistent_jsonb", 8, true, false, fc_jsonb_gin_via_dispatch),
        builtin(3488, "gin_triconsistent_jsonb", 7, true, false, fc_jsonb_gin_via_dispatch),
        // ---- jsonb_path_ops support procs ----
        builtin(3485, "gin_extract_jsonb_path", 3, true, false, fc_jsonb_gin_via_dispatch),
        builtin(3486, "gin_extract_jsonb_query_path", 7, true, false, fc_jsonb_gin_via_dispatch),
        builtin(3487, "gin_consistent_jsonb_path", 8, true, false, fc_jsonb_gin_via_dispatch),
        builtin(3489, "gin_triconsistent_jsonb_path", 7, true, false, fc_jsonb_gin_via_dispatch),
    ]);
}
