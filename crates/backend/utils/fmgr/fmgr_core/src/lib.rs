#![no_std]
#![allow(non_upper_case_globals)]

// A function's address is not an identity: builtin / wrapper identity reads
// the resolution record (FmgrInfo::fn_kind / fn_body), never a pointer compare.
#![deny(unpredictable_function_pointer_comparisons)]

extern crate alloc;
// thread_local! for the per-backend CFuncHash (AGENTS.md rule 10).
extern crate std;

pub mod canonical;
pub mod ported;
#[cfg(test)]
mod tests;

use alloc::boxed::Box;
use alloc::format;

use ::datum::Datum;
use ::fmgr::{FmgrBuiltin, FmgrInfo, FnKind, FunctionCallInfoBaseData, TRACK_FUNC_ALL};
use ::types_core::{primitive::InvalidOid, Oid, TransactionId};
use ::types_error::PgResult;
use ::types_tuple::ItemPointerData;

pub use ::fmgr::{
    direct_input_function_call_safe, input_function_call, input_function_call_safe,
    receive_function_call, send_function_call, ErrorSaveNode,
};
pub use ::fmgr::{
    direct_function_call1_coll, direct_function_call1_coll_in, direct_function_call2_coll,
    direct_function_call2_coll_in, direct_function_call3_coll, direct_function_call3_coll_in,
    function_call0_coll, function_call1_coll, function_call1_coll_in, function_call2_coll,
    function_call2_coll_in, function_call3_coll, function_call3_coll_in, function_call4_coll,
    function_call5_coll, function_call6_coll, function_call7_coll, function_call8_coll,
    function_call9_coll,
};
pub use canonical::{CANONICAL, CANONICAL_LAST_BUILTIN_OID};

pub fn init_seams() {
    fmgr_seams::fmgr_info::set(fmgr_info);
    fmgr_seams::fmgr_info_not_ported_name::set(fmgr_info_not_ported_name);
    fmgr_seams::internal_builtin_oid::set(internal_builtin_oid);
}

/// pgrust-only (no C analogue): `Some(builtin name)` iff `flinfo`'s resolved
/// entry point is the not-ported stub — i.e. invoking it can only raise the
/// clean feature-not-supported error. The stub's call-time late-table
/// dispatch is re-checked here so the answer tracks what invocation would
/// actually do. Lets eager resolvers (index-AM support-proc loading) reject
/// an unported dependency at resolution time instead of deep inside an
/// operation (e.g. a GiST page split long after CREATE INDEX succeeded).
pub fn fmgr_info_not_ported_name(flinfo: &FmgrInfo) -> Option<&'static str> {
    // The resolution's own record (`FnKind::NotPorted`), never `fn_addr ==
    // builtin_not_ported`: a fn-item cast is not one address (Rust gives a
    // function no unique address; Miri salts every fresh cast).
    if flinfo.fn_kind == FnKind::NotPorted && late_builtin(flinfo.fn_oid).is_none() {
        Some(fmgr_isbuiltin(flinfo.fn_oid).map_or("?", |b| b.name))
    } else {
        None
    }
}

/// C: `InvalidOidBuiltinMapping` (fmgrtab.h).
pub const INVALID_OID_BUILTIN_MAPPING: u16 = u16::MAX;

pub const FMGR_NBUILTINS: usize = CANONICAL.len();
pub const FMGR_LAST_BUILTIN_OID: Oid = CANONICAL_LAST_BUILTIN_OID;
pub const FMGR_OID_INDEX_SIZE: usize = FMGR_LAST_BUILTIN_OID as usize + 1;

/// C: `fmgr_builtin_oid_index[]` — dense OID -> table-row map, `N == last+1`.
pub struct BuiltinOidIndex<const N: usize>([u16; N]);

impl<const N: usize> BuiltinOidIndex<N> {
    pub const fn build(entries: &[FmgrBuiltin]) -> Self {
        assert!(entries.len() < INVALID_OID_BUILTIN_MAPPING as usize);
        let mut map = [INVALID_OID_BUILTIN_MAPPING; N];
        let mut i = 0;
        let mut prev = 0u32;
        while i < entries.len() {
            let oid = entries[i].foid;
            assert!(i == 0 || oid > prev, "entries must be strictly OID-ascending");
            assert!((oid as usize) < N, "entry OID exceeds index span");
            prev = oid;
            map[oid as usize] = i as u16;
            i += 1;
        }
        Self(map)
    }

    /// C: `fmgr_isbuiltin` — bounds test + one u16 load + one row borrow.
    #[inline]
    pub fn lookup<'a>(&self, entries: &'a [FmgrBuiltin], id: Oid) -> Option<&'a FmgrBuiltin> {
        if id as usize >= N {
            return None;
        }
        let i = self.0[id as usize];
        if i == INVALID_OID_BUILTIN_MAPPING {
            return None;
        }
        // SAFETY: `build` wrote only indices < entries.len() for this table.
        Some(unsafe { entries.get_unchecked(i as usize) })
    }
}

// Builtin tables from crates that sit above fmgr_core in the crate graph
// (adt_acl needs syscache). Consulted only where the entry would otherwise
// panic as unported; fn metadata still comes from the canonical row.
const MAX_LATE_TABLES: usize = 64;
static LATE_TABLE_PTR: [core::sync::atomic::AtomicPtr<FmgrBuiltin>; MAX_LATE_TABLES] =
    [const { core::sync::atomic::AtomicPtr::new(core::ptr::null_mut()) }; MAX_LATE_TABLES];
static LATE_TABLE_LEN: [core::sync::atomic::AtomicUsize; MAX_LATE_TABLES] =
    [const { core::sync::atomic::AtomicUsize::new(0) }; MAX_LATE_TABLES];

pub fn register_late_builtins(table: &'static [FmgrBuiltin]) {
    use core::sync::atomic::Ordering;
    for i in 0..MAX_LATE_TABLES {
        if LATE_TABLE_PTR[i]
            .compare_exchange(
                core::ptr::null_mut(),
                table.as_ptr() as *mut FmgrBuiltin,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            LATE_TABLE_LEN[i].store(table.len(), Ordering::Release);
            return;
        }
    }
    panic!("fmgr: too many late builtin tables");
}

#[cold]
fn late_builtin(oid: Oid) -> Option<&'static FmgrBuiltin> {
    use core::sync::atomic::Ordering;
    for i in 0..MAX_LATE_TABLES {
        let p = LATE_TABLE_PTR[i].load(Ordering::Acquire);
        if p.is_null() {
            return None;
        }
        let len = LATE_TABLE_LEN[i].load(Ordering::Acquire);
        if len == 0 {
            continue;
        }
        // SAFETY: registered as &'static [FmgrBuiltin] with this length.
        let table = unsafe { core::slice::from_raw_parts(p, len) };
        if let Some(b) = table.iter().find(|b| b.foid == oid) {
            return Some(b);
        }
    }
    None
}

fn builtin_not_ported(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let oid = flinfo.as_ref().map_or(InvalidOid, |f| f.fn_oid);
    if let Some(b) = late_builtin(oid) {
        return (b.func)(flinfo, fcinfo);
    }
    // unported: the canonical row exists but no port registered; raise a
    // clean feature error instead of panicking (the fn is user-callable).
    Err(builtin_not_ported_err(oid))
}

#[cold]
#[inline(never)]
fn builtin_not_ported_err(oid: Oid) -> Box<::types_error::PgError> {
    let name = fmgr_isbuiltin(oid).map_or("?", |b| b.name);
    Box::new(
        ::types_error::PgError::error(format!(
            "function {name} (OID {oid}) is not yet implemented"
        ))
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

const fn build_builtins() -> [FmgrBuiltin; FMGR_NBUILTINS] {
    let mut t = [FmgrBuiltin {
        foid: InvalidOid,
        name: "",
        nargs: 0,
        strict: false,
        retset: false,
        func: builtin_not_ported,
    }; FMGR_NBUILTINS];
    let mut i = 0;
    while i < FMGR_NBUILTINS {
        let (foid, name, nargs, strict, retset) = CANONICAL[i];
        t[i] = FmgrBuiltin {
            foid,
            name,
            nargs,
            strict,
            retset,
            func: builtin_not_ported,
        };
        i += 1;
    }
    let mut p = 0;
    let mut prev = 0u32;
    while p < ported::PORTED.len() {
        let (oid, func) = ported::PORTED[p];
        assert!(p == 0 || oid > prev, "PORTED must be strictly OID-ascending");
        prev = oid;
        let mut lo = 0;
        let mut hi = FMGR_NBUILTINS;
        let mut hit = false;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if t[mid].foid == oid {
                t[mid].func = func;
                hit = true;
                break;
            } else if t[mid].foid < oid {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        assert!(hit, "PORTED OID missing from the canonical table");
        p += 1;
    }
    t
}

/// Row `i` of `BUILTINS` kept the `builtin_not_ported` stub: no `PORTED`
/// entry named its oid. The same walk as `build_builtins`, so the bit and the
/// row's body are one fact of the generated tables; every "is this row the
/// stub?" question reads the bit — comparing function addresses is unreliable.
const fn build_not_ported() -> [bool; FMGR_NBUILTINS] {
    let mut stub = [true; FMGR_NBUILTINS];
    let mut p = 0;
    while p < ported::PORTED.len() {
        let (oid, _) = ported::PORTED[p];
        let mut lo = 0;
        let mut hi = FMGR_NBUILTINS;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if BUILTINS[mid].foid == oid {
                stub[mid] = false;
                break;
            } else if BUILTINS[mid].foid < oid {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        p += 1;
    }
    stub
}

const BUILTINS: [FmgrBuiltin; FMGR_NBUILTINS] = build_builtins();
const OID_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = BuiltinOidIndex::build(&BUILTINS);
// A static, not a const: a const array is materialized at every use.
static NOT_PORTED_ROW: [bool; FMGR_NBUILTINS] = build_not_ported();


/// C: `fmgr_builtins[]`.
pub static FMGR_BUILTINS: [FmgrBuiltin; FMGR_NBUILTINS] = BUILTINS;
pub static FMGR_BUILTIN_OID_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = OID_INDEX;

/// The row's index in `FMGR_BUILTINS` when `b` borrows from it (a data
/// address: unique and stable, unlike a function's); `None` for a late,
/// extra or native-C table row.
#[inline]
fn canonical_row_index(b: &FmgrBuiltin) -> Option<usize> {
    let p = b as *const FmgrBuiltin;
    if FMGR_BUILTINS.as_ptr_range().contains(&p) {
        // SAFETY: `p` is in the array's range, so both pointers derive from the
        // same allocation and the offset is a whole number of rows.
        Some(unsafe { p.offset_from(FMGR_BUILTINS.as_ptr()) } as usize)
    } else {
        None
    }
}

/// `b`'s body is the not-ported stub: a canonical row without a `PORTED`
/// entry. By the generated bit, never by comparing `b.func` to a fresh cast.
#[inline]
pub fn row_is_stub(b: &FmgrBuiltin) -> bool {
    canonical_row_index(b).is_some_and(|i| NOT_PORTED_ROW[i])
}

// Overlay for builtin tables whose crates would cycle into fmgr_core via
// cache_syscache -> catcache -> indexam -> nbtree (regproc/acl/ruleutils…),
// plus obj/col_description (prolang=sql in C, hosted natively — result-
// equivalent: C's inline_function rejects their table-reading bodies, so
// treating them as non-inlinable internal fns matches C's plan shape).
// INVARIANT (set-once): written by install_extra_builtins during
// single-threaded startup, before any lookup, never again.
static EXTRA_PTR: core::sync::atomic::AtomicPtr<()> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());
static EXTRA_LEN: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);

pub fn install_extra_builtins(tables: &'static [&'static [FmgrBuiltin]]) {
    for t in tables {
        for b in *t {
            // Live = a ported canonical row, or an already-installed extra
            // row (never the stub's own row) — by the table's bit, not `x.func`.
            let live = fmgr_isbuiltin(b.foid).is_some_and(|x| !row_is_stub(x));
            assert!(!live, "extra builtin {} collides with a live row", b.foid);
        }
    }
    let prev = EXTRA_PTR.swap(
        tables.as_ptr() as *mut (),
        core::sync::atomic::Ordering::Relaxed,
    );
    assert!(prev.is_null(), "extra builtins installed twice");
    EXTRA_LEN.store(tables.len(), core::sync::atomic::Ordering::Relaxed);
}

#[cold]
#[inline(never)]
fn extra_builtin(id: Oid) -> Option<&'static FmgrBuiltin> {
    let ptr = EXTRA_PTR.load(core::sync::atomic::Ordering::Relaxed);
    if ptr.is_null() {
        return None;
    }
    let len = EXTRA_LEN.load(core::sync::atomic::Ordering::Relaxed);
    // SAFETY: set-once invariant above — (ptr,len) is the installed slice.
    let tables = unsafe {
        core::slice::from_raw_parts(ptr as *const &'static [FmgrBuiltin], len)
    };
    tables
        .iter()
        .find_map(|t| t.iter().find(|b| b.foid == id))
}

#[inline]
/// Test support: assert each row's metadata matches its canonical pg_proc row.
pub fn assert_rows_match_canonical(rows: &[FmgrBuiltin]) {
    for r in rows {
        let i = CANONICAL
            .binary_search_by_key(&r.foid, |c| c.0)
            .unwrap_or_else(|_| panic!("OID {} not in the canonical table", r.foid));
        let c = &CANONICAL[i];
        assert_eq!(r.name, c.1, "name mismatch for OID {}", r.foid);
        assert_eq!(r.nargs, c.2, "nargs mismatch for {} ({})", c.1, r.foid);
        assert_eq!(r.strict, c.3, "strict mismatch for {} ({})", c.1, r.foid);
        assert_eq!(r.retset, c.4, "retset mismatch for {} ({})", c.1, r.foid);
    }
}

pub fn fmgr_isbuiltin(id: Oid) -> Option<&'static FmgrBuiltin> {
    match FMGR_BUILTIN_OID_INDEX.lookup(&FMGR_BUILTINS, id) {
        Some(b) if row_is_stub(b) => extra_builtin(id).or(Some(b)),
        Some(b) => Some(b),
        None => extra_builtin(id),
    }
}

// Oid-ascending per table (asserted in tests; binary-searched here).
const THIN_TABLES: &[&[::fmgr::ThinBuiltin]] = &[
    ::adt_int::builtins::INT_THIN,
    ::adt_int8::builtins::INT8_THIN,
];

/// Thin-ABI twin for a resolved carrier. The resolution record + `nargs`
/// referee the row: a carrier whose body is not the builtin table's row for
/// its oid (a hand-installed `FmgrInfo::new`, a wrapper, a diverging
/// resolution path), or a call site whose arity differs from the
/// registration, falls back to the PGFunction ABI. Identity is
/// `is_builtin_body(foid)`, never `e.func == fn_addr` (two casts of one body
/// need not compare equal).
pub fn fmgr_thin_builtin(flinfo: &FmgrInfo, nargs: i16) -> Option<::fmgr::PGFunctionThin> {
    for t in THIN_TABLES {
        if let Ok(i) = t.binary_search_by_key(&flinfo.fn_oid, |e| e.foid) {
            let e = &t[i];
            return (flinfo.is_builtin_body(e.foid) && e.nargs == nargs).then_some(e.thin);
        }
    }
    None
}

/// C: `fmgr_lookupByName` — linear, validator/alias resolution only (cold).
/// Extended to search the installed extra tables after the canonical table
/// so pgrust-native internal names (e.g. pgrust_lane_coverage) resolve for
/// CREATE FUNCTION ... LANGUAGE internal and the fmgr_info pg_proc arm —
/// cold paths only. C behavior is unchanged for every canonical name (the
/// canonical table is searched first and C has no extra names).
pub fn fmgr_lookup_by_name(name: &str) -> Option<&'static FmgrBuiltin> {
    FMGR_BUILTINS
        .iter()
        .find(|b| b.name == name)
        .or_else(|| extra_builtin_by_name(name))
}

/// The fmgr_builtins row a pg_proc row dispatches to: fmgr_info_cxt_security's
/// LANGUAGE internal arm without the call (fmgr.c:236-247, fmgr_lookupByName
/// on prosrc), so a `CREATE FUNCTION ... AS 'bthandler' LANGUAGE internal`
/// alias IS bthandler. None when there is no such pg_proc row or the function
/// is of any other language (LANGUAGE c goes through dfmgr — the no-dlopen
/// carve, docs/design/carve-ratifications.md §2 — and SQL/PL through their
/// handlers); a null prosrc cannot happen (pg_proc.prosrc is NOT NULL).
pub fn internal_builtin_of(funcid: Oid) -> PgResult<Option<&'static FmgrBuiltin>> {
    let Some(row) = syscache_seams::lookup_pg_proc_fmgr::call(funcid)? else {
        return Ok(None);
    };
    if row.prolang != INTERNAL_LANGUAGE_ID {
        return Ok(None);
    }
    let cx = ::mcx::MemoryContext::new("fmgr_info prosrc");
    let prosrc = syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), funcid)?
        .unwrap_or_else(|| panic!("fmgr: null prosrc for function {funcid}"));
    Ok(fmgr_lookup_by_name(prosrc.as_str()))
}

// fmgr_seams::internal_builtin_oid: the builtin's own pg_proc oid.
fn internal_builtin_oid(funcid: Oid) -> PgResult<Option<Oid>> {
    Ok(internal_builtin_of(funcid)?.map(|b| b.foid))
}

// A user-created internal-language fn (new oid) must resolve through the
// canonical entry's oid: the stub's late lookup keys on flinfo.fn_oid, which
// is the new oid, so late and extra ports are resolved here instead.
// Returns (body, resolution kind, the builtin row's oid): `Builtin` under the
// canonical row's oid (a user alias keeps its new `fn_oid`), `NotPorted` for
// a stub row with no late/extra port.
fn internal_fn_addr(prosrc: &str) -> Option<(::fmgr::PGFunction, FnKind, Oid)> {
    let fbp = fmgr_lookup_by_name(prosrc)?;
    if row_is_stub(fbp) {
        return Some(match late_builtin(fbp.foid).or_else(|| extra_builtin(fbp.foid)) {
            Some(b) => (b.func, FnKind::Builtin, fbp.foid),
            None => (fbp.func, FnKind::NotPorted, fbp.foid),
        });
    }
    Some((fbp.func, FnKind::Builtin, fbp.foid))
}

#[cold]
#[inline(never)]
fn extra_builtin_by_name(name: &str) -> Option<&'static FmgrBuiltin> {
    let ptr = EXTRA_PTR.load(core::sync::atomic::Ordering::Relaxed);
    if ptr.is_null() {
        return None;
    }
    let len = EXTRA_LEN.load(core::sync::atomic::Ordering::Relaxed);
    // SAFETY: set-once invariant (install_extra_builtins) — (ptr,len) is the
    // installed slice.
    let tables = unsafe {
        core::slice::from_raw_parts(ptr as *const &'static [FmgrBuiltin], len)
    };
    tables
        .iter()
        .find_map(|t| t.iter().find(|b| b.name == name))
}

/// C: `fmgr_internal_function` (`fmgr_internal_validator`'s lookup glue).
pub fn fmgr_internal_function(proname: &str) -> Oid {
    match fmgr_lookup_by_name(proname) {
        Some(fbp) => fbp.foid,
        None => InvalidOid,
    }
}

/// The builtin fast path's FmgrInfo fill (C: fmgr_info_cxt_security's fbp arm).
/// Late-registered builtins resolve to their real entry point here so
/// flinfo-less invocations (sortsupport shims) don't dead-end in the stub.
#[inline]
pub fn fmgr_info_from_builtin_into(fbp: &FmgrBuiltin, function_id: Oid, finfo: &mut FmgrInfo) {
    let (fbp, kind) = if row_is_stub(fbp) {
        match late_builtin(function_id) {
            Some(late) => (late, FnKind::Builtin),
            None => (fbp, FnKind::NotPorted),
        }
    } else if canonical_row_index(fbp).is_some()
        || [extra_builtin(fbp.foid), late_builtin(fbp.foid), native_clang_builtin(fbp.foid)]
            .into_iter().flatten().any(|row| core::ptr::eq(row, fbp))
    {
        (fbp, FnKind::Builtin)
    } else {
        (fbp, FnKind::Direct)
    };
    finfo.set_fn_addr(fbp.func);
    finfo.set_resolution(kind, if kind == FnKind::Direct { InvalidOid } else { fbp.foid });
    finfo.fn_nargs = fbp.nargs;
    finfo.fn_strict = fbp.strict;
    finfo.fn_retset = fbp.retset;
    finfo.fn_stats = TRACK_FUNC_ALL;
    finfo.fn_extra = None;
    finfo.fn_expr = None;
    finfo.fn_oid = function_id;
}

#[inline]
pub fn fmgr_info_from_builtin(fbp: &FmgrBuiltin, function_id: Oid) -> FmgrInfo {
    let mut finfo = FmgrInfo::unresolved();
    fmgr_info_from_builtin_into(fbp, function_id, &mut finfo);
    finfo
}

/// C: `fmgr_info`/`fmgr_info_cxt` (fn_mcxt dropped: fn_extra owns its storage).
/// Field-wise fill of the caller's carrier, like C — a by-value return spills
/// the 56B carrier through an sret and its droppy slots stop folding (bench).
/// Non-builtin OIDs resolve through pg_proc (syscache seam): prolang internal
/// dispatches by prosrc name, prolang sql through the registered SQL-language
/// handler (resolved once here, never per row); languages whose handler is
/// not registered error cleanly (0A000; no-dlopen carve,
/// docs/design/carve-ratifications.md §2). In-core C-language functions
/// (C's fmgr_info_C_lang dlopen
/// leg) resolve from NATIVE_CLANG instead of pg_proc — their FmgrBuiltin rows
/// carry the pg_proc metadata.
#[inline]
pub fn fmgr_info_into(function_id: Oid, finfo: &mut FmgrInfo) -> PgResult<()> {
    fmgr_info_into_security(function_id, finfo, false)
}

// fmgr_info_cxt_security's ignore_security knob: true bypasses the
// fmgr_security_definer interposition (the wrapper's own inner lookup).
fn fmgr_info_into_security(
    function_id: Oid,
    finfo: &mut FmgrInfo,
    ignore_security: bool,
) -> PgResult<()> {
    match fmgr_isbuiltin(function_id) {
        Some(fbp) => {
            fmgr_info_from_builtin_into(fbp, function_id, finfo);
            Ok(())
        }
        None => match native_clang_builtin(function_id) {
            Some(fbp) => {
                fmgr_info_from_builtin_into(fbp, function_id, finfo);
                Ok(())
            }
            None => fmgr_info_pg_proc(function_id, finfo, ignore_security),
        },
    }
}

pub const INTERNAL_LANGUAGE_ID: Oid = 12;
pub const C_LANGUAGE_ID: Oid = 13;
pub const SQL_LANGUAGE_ID: Oid = 14;

static SQL_HANDLER: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);

pub fn register_sql_language_handler(handler: ::fmgr::PGFunction) {
    SQL_HANDLER.store(handler as usize, core::sync::atomic::Ordering::Release);
}

// PL handler entry points are C-language extension functions; the dlopen leg
// is replaced by name-keyed registration (closed set; no-dlopen carve,
// docs/design/carve-ratifications.md §2).
static PLPGSQL_CALL_HANDLER: core::sync::atomic::AtomicUsize =
    core::sync::atomic::AtomicUsize::new(0);
static PLPGSQL_INLINE_HANDLER: core::sync::atomic::AtomicUsize =
    core::sync::atomic::AtomicUsize::new(0);
static PLPGSQL_VALIDATOR: core::sync::atomic::AtomicUsize =
    core::sync::atomic::AtomicUsize::new(0);

pub fn register_plpgsql_handlers(
    call_handler: ::fmgr::PGFunction,
    inline_handler: ::fmgr::PGFunction,
    validator: ::fmgr::PGFunction,
) {
    PLPGSQL_CALL_HANDLER.store(call_handler as usize, core::sync::atomic::Ordering::Release);
    PLPGSQL_INLINE_HANDLER.store(inline_handler as usize, core::sync::atomic::Ordering::Release);
    PLPGSQL_VALIDATOR.store(validator as usize, core::sync::atomic::Ordering::Release);
}

fn registered_c_lang_fn(prosrc: &str) -> Option<::fmgr::PGFunction> {
    let slot = match prosrc {
        "plpgsql_call_handler" => &PLPGSQL_CALL_HANDLER,
        "plpgsql_inline_handler" => &PLPGSQL_INLINE_HANDLER,
        "plpgsql_validator" => &PLPGSQL_VALIDATOR,
        _ => return None,
    };
    let h = slot.load(core::sync::atomic::Ordering::Acquire);
    if h == 0 {
        return None;
    }
    // SAFETY: written only by register_plpgsql_handlers from valid PGFunctions.
    Some(unsafe { core::mem::transmute::<usize, ::fmgr::PGFunction>(h) })
}

// fmgr.c CFuncHash: the resolved address of each external C function, keyed
// by pg_proc OID and validated by the tuple's xmin/TID (lookup_C_func), so
// dfmgr's path resolution runs once per function per session.
struct CFuncHashTabEntry {
    fn_xmin: TransactionId,
    fn_tid: ItemPointerData,
    user_fn: ::fmgr::PGFunction,
}

type CFuncHash = ::mcx::PgHashMap<'static, Oid, CFuncHashTabEntry>;

std::thread_local! {
    static CFUNC_HASH: core::cell::RefCell<Option<core::mem::ManuallyDrop<CFuncHash>>> =
        const { core::cell::RefCell::new(None) };
}

fn lookup_c_func(
    fn_oid: Oid,
    xmin: TransactionId,
    tid: ItemPointerData,
) -> Option<::fmgr::PGFunction> {
    CFUNC_HASH.with(|cell| {
        let slot = cell.borrow();
        let entry = slot.as_ref()?.get(&fn_oid)?;
        (entry.fn_xmin == xmin && entry.fn_tid == tid).then_some(entry.user_fn)
    })
}

fn record_c_func(
    fn_oid: Oid,
    xmin: TransactionId,
    tid: ItemPointerData,
    user_fn: ::fmgr::PGFunction,
) {
    CFUNC_HASH.with(|cell| {
        let mut slot = cell.borrow_mut();
        let table = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("CFuncHash").mcx();
            ::mcx::register_session_cleanup(Box::new(|| {
                CFUNC_HASH.with(|cell| {
                    if let Some(table) = cell.borrow_mut().take() {
                        drop(core::mem::ManuallyDrop::into_inner(table));
                    }
                });
            }));
            core::mem::ManuallyDrop::new(CFuncHash::with_capacity_in(100, mcx))
        });
        table.insert(fn_oid, CFuncHashTabEntry { fn_xmin: xmin, fn_tid: tid, user_fn });
    });
}

/// Whether `prosrc` names a registered in-tree PL entry point. DDL fences
/// (CREATE LANGUAGE) consult this so a handler that could never dispatch is
/// refused at creation with a clean 0A000 instead of failing at call time
/// (no-dlopen carve, docs/design/carve-ratifications.md §2).
pub fn has_registered_c_lang_handler(prosrc: &str) -> bool {
    registered_c_lang_fn(prosrc).is_some()
}

// fmgr.c:428 elog(ERROR, "cache lookup failed for language %u", language).
// elog's default SQLSTATE at ERROR is XX000 (ERRCODE_INTERNAL_ERROR), so no
// explicit sqlstate; the error is catchable and never aborts the backend.
#[track_caller]
#[cold]
#[inline(never)]
pub(crate) fn language_lookup_failed(language: Oid) -> alloc::boxed::Box<::types_error::PgError> {
    alloc::boxed::Box::new(::types_error::PgError::error(alloc::format!(
        "cache lookup failed for language {language}"
    )))
}

// fmgr.c:663 elog(ERROR, "cache lookup failed for function %u") inside
// fmgr_security_definer -- same catchable XX000.
#[track_caller]
#[cold]
#[inline(never)]
pub(crate) fn function_lookup_failed(
    function_id: Oid,
) -> alloc::boxed::Box<::types_error::PgError> {
    alloc::boxed::Box::new(::types_error::PgError::error(alloc::format!(
        "cache lookup failed for function {function_id}"
    )))
}

#[cold]
#[inline(never)]
fn fmgr_info_pg_proc(
    function_id: Oid,
    finfo: &mut FmgrInfo,
    ignore_security: bool,
) -> PgResult<()> {
    use ::types_error::{PgError, ERRCODE_UNDEFINED_FUNCTION};
    let Some(row) = syscache_seams::lookup_pg_proc_fmgr::call(function_id)? else {
        return Err(alloc::boxed::Box::new(PgError::error(alloc::format!(
            "cache lookup failed for function {function_id}"
        ))));
    };
    // fmgr_info_cxt_security: prosecdef or non-null proconfig routes through
    // the fmgr_security_definer handler (FmgrHookIsNeeded: no hook surface).
    if !ignore_security && (row.prosecdef || !row.proconfig_isnull) {
        finfo.set_fn_addr(fmgr_security_definer);
        // The wrapper's identity is this write (C: `fn_addr == fmgr_security_definer`).
        finfo.set_resolution(FnKind::SecurityDefiner, InvalidOid);
        finfo.fn_nargs = row.pronargs;
        finfo.fn_strict = row.proisstrict;
        finfo.fn_retset = row.proretset;
        finfo.fn_stats = TRACK_FUNC_ALL;
        finfo.fn_extra = None;
        finfo.fn_expr = None;
        finfo.fn_oid = function_id;
        return Ok(());
    }
    // (body, resolution kind, builtin row oid): the language arms are
    // `FnKind::Language`; the internal arm is the canonical row's body.
    let (fn_addr, kind, body) = match row.prolang {
        INTERNAL_LANGUAGE_ID => {
            let cx = ::mcx::MemoryContext::new("fmgr_info prosrc");
            let prosrc = syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), function_id)?
                .unwrap_or_else(|| panic!("fmgr: null prosrc for function {function_id}"));
            match internal_fn_addr(&prosrc) {
                Some(resolved) => resolved,
                None => {
                    return Err(alloc::boxed::Box::new(
                        PgError::error(alloc::format!(
                            "internal function \"{}\" is not in internal lookup table",
                            prosrc.as_str()
                        ))
                        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
                    ))
                }
            }
        }
        SQL_LANGUAGE_ID => {
            let h = SQL_HANDLER.load(core::sync::atomic::Ordering::Acquire);
            if h == 0 {
                panic!("fmgr: SQL-language handler not registered (function {function_id})");
            }
            // SAFETY: written only by register_sql_language_handler from a
            // valid PGFunction.
            (unsafe { core::mem::transmute::<usize, ::fmgr::PGFunction>(h) }, FnKind::Language, InvalidOid)
        }
        C_LANGUAGE_ID => match lookup_c_func(function_id, row.xmin, row.tid) {
            Some(f) => (f, FnKind::Language, InvalidOid),
            None => {
                // fmgr_info_C_lang (fmgr.c:349-365): prosrc is the link
                // symbol, probin the library; load_external_function always
                // goes through internal_load_library (dfmgr.c:118-121), so a
                // library's first use in the session runs its _PG_init and
                // links it into file_list (dfmgr.c:297-306) — the registered
                // PL entry points and dict_snowball are dfmgr libraries like
                // every other C-language probin (no-dlopen carve,
                // docs/design/carve-ratifications.md §2).
                let cx = ::mcx::MemoryContext::new("fmgr_info prosrc");
                let prosrc = syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), function_id)?
                    .unwrap_or_else(|| panic!("fmgr: null prosrc for function {function_id}"));
                let probin = syscache_seams::lookup_pg_proc_probin::call(cx.mcx(), function_id)?
                    .unwrap_or_else(|| panic!("fmgr: null probin for C function {function_id}"));
                let user_fn = ::dfmgr::load_external_function(&probin, &prosrc, true)?
                    .expect("signal_not_found=true returned no function");
                record_c_func(function_id, row.xmin, row.tid, user_fn);
                (user_fn, FnKind::Language, InvalidOid)
            }
        },
        lang => {
            // fmgr_info_other_lang (fmgr.c:418-441): look up the language's
            // call handler and adopt its entry point through a recursive
            // fmgr_info_cxt_security(lanplcallfoid, ..., ignore_security =
            // true) (:435-437) — the handler is a C-language function, so
            // its library loads (and records) exactly like any other.
            // fmgr.c:426-428 elog(ERROR, "cache lookup failed for language
            // %u", language) -- catchable, SQLSTATE XX000 (elog's default at
            // ERROR); it unwinds the transaction, never the backend.
            let Some(langrow) = syscache_seams::lookup_pg_language_fmgr::call(lang)? else {
                return Err(language_lookup_failed(lang));
            };
            let mut plfinfo = FmgrInfo::unresolved();
            fmgr_info_pg_proc(langrow.lanplcallfoid, &mut plfinfo, true)?;
            (plfinfo.fn_addr(), FnKind::Language, InvalidOid)
        }
    };
    finfo.set_fn_addr(fn_addr);
    finfo.set_resolution(kind, body);

    finfo.fn_nargs = row.pronargs;
    finfo.fn_strict = row.proisstrict;
    finfo.fn_retset = row.proretset;
    finfo.fn_stats = match row.prolang {
        INTERNAL_LANGUAGE_ID => TRACK_FUNC_ALL,
        C_LANGUAGE_ID | SQL_LANGUAGE_ID => ::fmgr::TRACK_FUNC_PL,
        _ => ::fmgr::TRACK_FUNC_OFF,
    };
    finfo.fn_extra = None;
    finfo.fn_expr = None;
    finfo.fn_oid = function_id;
    Ok(())
}

// fmgr_security_definer_cache (fmgr.c:611): configHandles dropped — the
// registry lookup inside set_config_option replaces the handle cache.
struct SecurityDefinerCache {
    flinfo: FmgrInfo,
    // Userid to switch to; InvalidOid = proconfig-only wrapping.
    userid: Oid,
    proconfig: Option<alloc::vec::Vec<alloc::string::String>>,
}

/// fmgr_security_definer (fmgr.c:632): SECURITY DEFINER / proconfig call
/// handler. GUC and userid state need no unwinding on error — the ensuing
/// xact or subxact abort restores both (fmgr.c:717); C's PG_TRY only relinks
/// fcinfo->flinfo, which does not exist in this ABI (flinfo travels as a
/// parameter). Divergence: pgstat function tracking inside the wrapper
/// (fmgr.c:733/741) is not wired — fmgr_core has no pgstat edge.
pub fn fmgr_security_definer(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("fmgr_security_definer requires flinfo");
    if flinfo.fn_extra.is_none() {
        let mut inner = FmgrInfo::unresolved();
        fmgr_info_into_security(flinfo.fn_oid, &mut inner, true)?;
        inner.fn_expr = flinfo.fn_expr;
        // fmgr.c:661-664 elog(ERROR, "cache lookup failed for function %u",
        // fcinfo->flinfo->fn_oid) inside fmgr_security_definer -- catchable
        // XX000, not an abort.
        let row = syscache_seams::lookup_pg_proc_secdef::call(flinfo.fn_oid)?
            .ok_or_else(|| function_lookup_failed(flinfo.fn_oid))?;
        let userid = if row.prosecdef { row.proowner } else { InvalidOid };
        flinfo.fn_extra = Some(::fmgr::FnExtra::new(SecurityDefinerCache {
            flinfo: inner,
            userid,
            proconfig: row.proconfig,
        }));
    }
    let cache = flinfo
        .fn_extra
        .as_mut()
        .expect("fn_extra filled above")
        .downcast_mut::<SecurityDefinerCache>();

    let (save_userid, save_sec_context) = miscinit_seams::get_user_id_and_sec_context::call();
    let save_nestlevel = if cache.proconfig.is_some() {
        guc_seams::new_guc_nest_level::call()
    } else {
        0
    };
    if cache.userid != InvalidOid {
        miscinit_seams::set_user_id_and_sec_context::call(
            cache.userid,
            save_sec_context | ::types_core::SECURITY_LOCAL_USERID_CHANGE,
        );
    }
    if let Some(cfg) = &cache.proconfig {
        guc_seams::process_guc_array_secdef::call(cfg)?;
    }

    let result = cache.flinfo.invoke(fcinfo)?;

    if cache.proconfig.is_some() {
        guc_seams::at_eoxact_guc::call(true, save_nestlevel)?;
    }
    if cache.userid != InvalidOid {
        miscinit_seams::set_user_id_and_sec_context::call(save_userid, save_sec_context);
    }
    Ok(result)
}

#[inline]
pub fn fmgr_info(function_id: Oid) -> PgResult<FmgrInfo> {
    let mut finfo = FmgrInfo::unresolved();
    fmgr_info_into(function_id, &mut finfo)?;
    Ok(finfo)
}

#[cold]
fn native_clang_builtin(function_id: Oid) -> Option<&'static FmgrBuiltin> {
    ::conv::conv_builtin(function_id)
}

pub fn oid_function_call0_coll(function_id: Oid, collation: Oid) -> PgResult<Datum> {
    let mut flinfo = FmgrInfo::unresolved();
    fmgr_info_into(function_id, &mut flinfo)?;
    function_call0_coll(&mut flinfo, collation)
}

macro_rules! define_oid_calls {
    ($($oname:ident $cname:ident ($($arg:ident),+);)*) => {$(
        pub fn $oname(
            function_id: Oid,
            collation: Oid,
            $($arg: Datum,)+
        ) -> PgResult<Datum> {
            let mut flinfo = FmgrInfo::unresolved();
            fmgr_info_into(function_id, &mut flinfo)?;
            ::fmgr::$cname(&mut flinfo, collation, $($arg,)+)
        }
    )*};
}

define_oid_calls! {
    oid_function_call1_coll function_call1_coll (a1);
    oid_function_call2_coll function_call2_coll (a1, a2);
    oid_function_call3_coll function_call3_coll (a1, a2, a3);
    oid_function_call4_coll function_call4_coll (a1, a2, a3, a4);
    oid_function_call5_coll function_call5_coll (a1, a2, a3, a4, a5);
    oid_function_call6_coll function_call6_coll (a1, a2, a3, a4, a5, a6);
    oid_function_call7_coll function_call7_coll (a1, a2, a3, a4, a5, a6, a7);
    oid_function_call8_coll function_call8_coll (a1, a2, a3, a4, a5, a6, a7, a8);
    oid_function_call9_coll function_call9_coll (a1, a2, a3, a4, a5, a6, a7, a8, a9);
}
