#![no_std]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod canonical;
pub mod ported;
#[cfg(test)]
mod tests;

use alloc::boxed::Box;
use alloc::format;

use ::datum::Datum;
use ::fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData, TRACK_FUNC_ALL};
use ::types_core::{primitive::InvalidOid, Oid};
use ::types_error::PgResult;

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

const BUILTINS: [FmgrBuiltin; FMGR_NBUILTINS] = build_builtins();
const OID_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = BuiltinOidIndex::build(&BUILTINS);

/// C: `fmgr_builtins[]`.
pub static FMGR_BUILTINS: [FmgrBuiltin; FMGR_NBUILTINS] = BUILTINS;
pub static FMGR_BUILTIN_OID_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = OID_INDEX;

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
            let live = fmgr_isbuiltin(b.foid)
                .is_some_and(|x| x.func as usize != builtin_not_ported as usize);
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
pub fn fmgr_isbuiltin(id: Oid) -> Option<&'static FmgrBuiltin> {
    match FMGR_BUILTIN_OID_INDEX.lookup(&FMGR_BUILTINS, id) {
        Some(b) if b.func as usize == builtin_not_ported as usize => {
            extra_builtin(id).or(Some(b))
        }
        Some(b) => Some(b),
        None => extra_builtin(id),
    }
}

// Oid-ascending per table (asserted in tests; binary-searched here).
const THIN_TABLES: &[&[::fmgr::ThinBuiltin]] = &[
    ::adt_int::builtins::INT_THIN,
    ::adt_int8::builtins::INT8_THIN,
];

/// Thin-ABI twin for a resolved carrier. `fn_addr` + `nargs` referee the
/// row: an oid reused by a diverging resolution path, or a call site whose
/// arity differs from the registration, falls back to the PGFunction ABI.
pub fn fmgr_thin_builtin(flinfo: &FmgrInfo, nargs: i16) -> Option<::fmgr::PGFunctionThin> {
    for t in THIN_TABLES {
        if let Ok(i) = t.binary_search_by_key(&flinfo.fn_oid, |e| e.foid) {
            let e = &t[i];
            return (e.func as usize == flinfo.fn_addr as usize && e.nargs == nargs)
                .then_some(e.thin);
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
    let fbp = if fbp.func as usize == builtin_not_ported as usize {
        late_builtin(function_id).unwrap_or(fbp)
    } else {
        fbp
    };
    finfo.fn_addr = fbp.func;
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
/// handler (resolved once here, never per row); other languages panic
/// loudly. In-core C-language functions (C's fmgr_info_C_lang dlopen
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
// is replaced by name-keyed registration (closed set).
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
        finfo.fn_addr = fmgr_security_definer;
        finfo.fn_nargs = row.pronargs;
        finfo.fn_strict = row.proisstrict;
        finfo.fn_retset = row.proretset;
        finfo.fn_stats = TRACK_FUNC_ALL;
        finfo.fn_extra = None;
        finfo.fn_expr = None;
        finfo.fn_oid = function_id;
        return Ok(());
    }
    let fn_addr = match row.prolang {
        INTERNAL_LANGUAGE_ID => {
            let cx = ::mcx::MemoryContext::new("fmgr_info prosrc");
            let prosrc = syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), function_id)?
                .unwrap_or_else(|| panic!("fmgr: null prosrc for function {function_id}"));
            match fmgr_lookup_by_name(&prosrc) {
                // A user-created internal-language fn (new oid) must resolve
                // through the canonical entry's oid; the stub's late lookup
                // keys on flinfo.fn_oid, which is the new oid.
                Some(fbp) if fbp.func as usize == builtin_not_ported as usize => {
                    late_builtin(fbp.foid).map_or(fbp.func, |b| b.func)
                }
                Some(fbp) => fbp.func,
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
            unsafe { core::mem::transmute::<usize, ::fmgr::PGFunction>(h) }
        }
        C_LANGUAGE_ID => {
            // fmgr_info_C_lang: the dlopen'd symbol is the prosrc name;
            // resolve against the registered PL entry points, then the
            // shipped native-library tables, then the in-process
            // ported-library registry keyed by probin (dfmgr).
            let cx = ::mcx::MemoryContext::new("fmgr_info prosrc");
            let prosrc = syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), function_id)?
                .unwrap_or_else(|| panic!("fmgr: null prosrc for function {function_id}"));
            match registered_c_lang_fn(&prosrc).or_else(|| {
                ::dict_snowball::builtins::SNOWBALL_CLANG
                    .iter()
                    .find(|(name, _, _)| *name == prosrc.as_str())
                    .map(|&(_, _, func)| func)
            }) {
                Some(f) => f,
                None => {
                    let probin =
                        syscache_seams::lookup_pg_proc_probin::call(cx.mcx(), function_id)?
                            .unwrap_or_else(|| {
                                panic!("fmgr: null probin for C function {function_id}")
                            });
                    ::dfmgr::load_external_function(&probin, &prosrc, true)?
                        .expect("signal_not_found=true returned no function")
                }
            }
        }
        lang => {
            // fmgr_info_other_lang: adopt the language call handler's entry
            // point (the handler is a C-language function; dispatch by its
            // prosrc name).
            let Some(langrow) = syscache_seams::lookup_pg_language_fmgr::call(lang)? else {
                panic!("fmgr: cache lookup failed for language {lang} (function {function_id})");
            };
            let cx = ::mcx::MemoryContext::new("fmgr_info prosrc");
            let hsrc =
                syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), langrow.lanplcallfoid)?
                    .unwrap_or_else(|| {
                        panic!(
                            "fmgr: null prosrc for call handler {}",
                            langrow.lanplcallfoid
                        )
                    });
            match registered_c_lang_fn(&hsrc) {
                Some(f) => f,
                None => panic!(
                    "fmgr: language handler \"{}\" not ported (function {function_id})",
                    hsrc.as_str()
                ),
            }
        }
    };
    finfo.fn_addr = fn_addr;
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
        let row = syscache_seams::lookup_pg_proc_secdef::call(flinfo.fn_oid)?
            .unwrap_or_else(|| panic!("cache lookup failed for function {}", flinfo.fn_oid));
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
