// pl_handler.c + pl_comp.c's plpgsql_compile / do_compile + pl_exec.c's
// plpgsql_exec_function/plpgsql_exec_trigger shells. DO blocks and VARIADIC
// parameters are named louds. GUC-backed compile options
// (plpgsql.variable_conflict, ...) read their C-source defaults; SET on the
// unregistered custom GUCs is loud at the GUC layer.
use std::collections::HashMap;

type FxHashMap<K, V> = HashMap<K, V, rustc_hash::FxBuildHasher>;
use std::rc::Rc;

use datum::Datum;
use fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use mcx::{Mcx, PgString, PgVec};
use types_core::{Oid, OidIsValid};
use types_error::{PgResult, ERROR};

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, PROCOID};

use crate::ast::*;
use crate::comp::CompState;
use crate::exec::{Estate, RC_OK, RC_RETURN};
use crate::gram::Parser;
use crate::scanner::PlScanner;

const ANUM_PG_PROC_PRONAME: i32 = 2;
const ANUM_PG_PROC_PROKIND: i32 = 10;
const ANUM_PG_PROC_PRORETSET: i32 = 14;
const ANUM_PG_PROC_PROVOLATILE: i32 = 15;
const ANUM_PG_PROC_PRONARGS: i32 = 17;
const ANUM_PG_PROC_PRORETTYPE: i32 = 19;
const ANUM_PG_PROC_PROARGTYPES: i32 = 20;
const ANUM_PG_PROC_PROALLARGTYPES: i32 = 21;
const ANUM_PG_PROC_PROARGMODES: i32 = 22;
const ANUM_PG_PROC_PROARGNAMES: i32 = 23;
const ANUM_PG_PROC_PROSRC: i32 = 26;

const BOOLOID: Oid = 16;
const TEXTOID: Oid = 25;
const VOIDOID: Oid = 2278;
const RECORDOID: Oid = 2249;
const TRIGGEROID: Oid = 2279;
const EVENT_TRIGGEROID: Oid = 3838;
const TYPTYPE_PSEUDO: i8 = b'p' as i8;
const PROVOLATILE_VOLATILE: i8 = b'v' as i8;

fn is_polymorphic(t: Oid) -> bool {
    // IsPolymorphicType (pg_type.h); excludes ANYOID as C does.
    matches!(
        t,
        2277 /* anyarray */
            | 2283 /* anyelement */
            | 2776 /* anynonarray */
            | 3500 /* anyenum */
            | 3831 /* anyrange */
            | 4537 /* anymultirange */
            | 4538 /* anycompatiblemultirange */
            | 5077 /* anycompatible */
            | 5078 /* anycompatiblearray */
            | 5079 /* anycompatiblenonarray */
            | 5080 /* anycompatiblerange */
    )
}

// use_count lives behind an Rc shared with each in-flight invocation, so a
// mid-call cache eviction can neither free storage under the invocation nor
// have the decrement land on a different entry (funccache.c use_count).
#[derive(Clone)]
struct FuncCacheEntry {
    func: Rc<PlFunction>,
    use_count: Rc<core::cell::Cell<u32>>,
}

std::thread_local! {
    // Keyed by (fn_oid, input_collation, is_trigger, trigger oid,
    // is_event_trigger, resolved input argtypes): funccache.c
    // compute_function_hashkey — per-trigger entries allow different relation
    // rowtypes per usage; the argtypes component separates polymorphic/RECORD
    // instantiations and stays empty for signatures that cannot vary.
    static FUNC_CACHE: core::cell::RefCell<FxHashMap<(Oid, Oid, bool, Oid, bool, Vec<Oid>), FuncCacheEntry>> =
        core::cell::RefCell::new(FxHashMap::default());
}

#[derive(Clone, Copy)]
enum CallKind {
    Function,
    Trigger(Oid),
    EventTrigger,
}

pub fn init_seams() {
    fmgr_core::register_plpgsql_handlers(
        plpgsql_call_handler,
        plpgsql_inline_handler,
        plpgsql_validator,
    );
    // plpgsql _PG_init (pl_handler.c) runs at LOAD: MarkGUCPrefixReserved
    // after defining its custom GUCs. Those GUC definitions are unported
    // (compile options read C-source defaults), so only the reservation runs.
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: "plpgsql",
        lookup: |_| None,
        pg_init: Some(|| {
            guc::MarkGUCPrefixReserved("plpgsql");
            Ok(())
        }),
    });
}

// plpgsql_compile (pl_comp.c) with funccache.c's xmin/tid staleness rule.
fn plpgsql_compile(
    fn_oid: Oid,
    fn_collation: Oid,
    for_validator: bool,
    kind: CallKind,
    call_expr: Option<types_core::fmgr::FnExprErased>,
) -> PgResult<FuncCacheEntry> {
    let (cur_xmin, cur_tid, key_argtypes) =
        proc_call_stamp(fn_oid, call_expr, for_validator)?;
    // C hashkey carries isTrigger AND isEventTrigger (funccache.c) — the same
    // OID called in different contexts compiles separately.
    let (is_trigger, trig_oid, is_event_trigger) = match kind {
        CallKind::Function => (false, types_core::InvalidOid, false),
        CallKind::Trigger(oid) => (true, oid, false),
        CallKind::EventTrigger => (false, types_core::InvalidOid, true),
    };
    let key = (fn_oid, fn_collation, is_trigger, trig_oid, is_event_trigger, key_argtypes);
    let cached = FUNC_CACHE.with(|c| c.borrow().get(&key).cloned());
    if let Some(entry) = cached {
        if entry.func.fn_xmin == cur_xmin && entry.func.fn_tid == cur_tid {
            return Ok(entry);
        }
        FUNC_CACHE.with(|c| {
            c.borrow_mut().remove(&key);
        });
        // delete_function (funccache.c:433): free subsidiary storage only
        // when no invocation is in flight; otherwise the old definition runs
        // to completion and the entry leaks, as in C.
        if entry.use_count.get() == 0 {
            crate::exec::free_function_plans(&entry.func.expr_ids);
        }
    }

    let func = Rc::new(do_compile(
        fn_oid,
        fn_collation,
        cur_xmin,
        cur_tid,
        for_validator,
        is_trigger,
        is_event_trigger,
        call_expr,
    )?);
    let entry = FuncCacheEntry { func, use_count: Rc::new(core::cell::Cell::new(0)) };
    // Validator compiles are cached too (funccache.c cached_function_compile
    // has no validator carve-out): the CREATE-time compile is the one the
    // first call reuses, so compile-time messages fire once, at CREATE.
    FUNC_CACHE.with(|c| {
        c.borrow_mut().insert(key, entry.clone());
    });
    Ok(entry)
}

// Tuple stamp + the hashkey argtypes component of funccache.c
// compute_function_hashkey: input argtypes resolved through the call
// expression when the signature can vary per call, empty otherwise.
fn proc_call_stamp(
    fn_oid: Oid,
    call_expr: Option<types_core::fmgr::FnExprErased>,
    for_validator: bool,
) -> PgResult<(u32, (u32, u16), Vec<Oid>)> {
    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(fn_oid)))? else {
        return Err(crate::exec::exec_err(
            types_error::ERRCODE_UNDEFINED_FUNCTION,
            format!("cache lookup failed for function {fn_oid}"),
        ));
    };
    let t = tup.tuple();
    let xmin = t.t_data().xmin_raw();
    let tid = (
        ((t.t_self.ip_blkid.bi_hi as u32) << 16) | t.t_self.ip_blkid.bi_lo as u32,
        t.t_self.ip_posid,
    );
    drop(t);
    let stamp_result = (|| -> PgResult<Vec<Oid>> {
        let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
        // SAFETY: proargtypes is a not-null plain-storage oidvector; the
        // values tail follows the 24-byte header in place, dim1 long.
        let args = unsafe {
            let p = argv.as_usize() as *const array::oidvector;
            core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
        };
        if !args.iter().any(|&t| {
            is_polymorphic(t) || t == RECORDOID || t == types_core::RECORDARRAYOID
        }) {
            return Ok(Vec::new());
        }
        let mut argtypes = args.to_vec();
        let (proname_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONAME)?;
        // SAFETY: NameData attr from the live pinned syscache tuple — 64
        // NUL-padded bytes.
        let nbytes =
            unsafe { core::slice::from_raw_parts(proname_d.as_usize() as *const u8, 64) };
        let nlen = nbytes.iter().position(|&b| b == 0).unwrap_or(64);
        let proname =
            core::str::from_utf8(&nbytes[..nlen]).expect("proname is server-encoding text");
        funcapi::cfunc_resolve_polymorphic_argtypes(
            &mut argtypes,
            &[],
            call_expr,
            for_validator,
            proname,
        )?;
        Ok(argtypes)
    })();
    ReleaseSysCache(tup);
    Ok((xmin, tid, stamp_result?))
}

struct ProcInfo {
    proname: String,
    prosrc: String,
    argtypes: Vec<Oid>,
    /// One mode per argtypes entry; empty when proargmodes is null (all IN).
    argmodes: Vec<i8>,
    argnames: Vec<String>,
    rettype: Oid,
    retset: bool,
    prokind: i8,
    readonly: bool,
}

const PROARGMODE_IN: i8 = b'i' as i8;
const PROARGMODE_OUT: i8 = b'o' as i8;
const PROARGMODE_INOUT: i8 = b'b' as i8;
const PROARGMODE_VARIADIC: i8 = b'v' as i8;
const PROARGMODE_TABLE: i8 = b't' as i8;
const PROKIND_FUNCTION: i8 = b'f' as i8;
const PROKIND_PROCEDURE: i8 = b'p' as i8;

fn read_proc_row(fn_oid: Oid) -> PgResult<ProcInfo> {
    let cx = mcx::MemoryContext::new("plpgsql compile proc row");
    let mcx = cx.mcx();
    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(fn_oid)))? else {
        return Err(crate::exec::exec_err(
            types_error::ERRCODE_UNDEFINED_FUNCTION,
            format!("cache lookup failed for function {fn_oid}"),
        ));
    };
    let (rettype_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRORETTYPE)?;
    let (provolatile, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROVOLATILE)?;
    let (proretset, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRORETSET)?;
    let (prokind_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROKIND)?;
    let (pronargs, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONARGS)?;
    let nargs = pronargs.as_i16() as usize;
    // get_func_arg_info (funcapi.c): proallargtypes supersedes proargtypes
    // when present; proargmodes rides along.
    let (allarg_d, allarg_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROALLARGTYPES)?;
    let (modes_d, modes_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGMODES)?;
    let (argtypes, argmodes): (Vec<Oid>, Vec<i8>) = if !allarg_null {
        let img = varlena_bytes(mcx, allarg_d)?;
        let elems = datum::array_build::deconstruct_array_image(mcx, &img, 4, true, b'i')?;
        let types: Vec<Oid> = elems.iter().map(|d| d.as_oid()).collect();
        assert!(!modes_null, "proallargtypes without proargmodes (function {fn_oid})");
        let mimg = varlena_bytes(mcx, modes_d)?;
        let melems = datum::array_build::deconstruct_array_image(mcx, &mimg, 1, true, b'c')?;
        let modes: Vec<i8> = melems.iter().map(|d| d.as_i8()).collect();
        assert_eq!(types.len(), modes.len(), "proargmodes length mismatch (function {fn_oid})");
        (types, modes)
    } else {
        let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
        let argtypes_pg = read_oidvector_attr(mcx, argv)?;
        (argtypes_pg.iter().copied().collect(), Vec::new())
    };
    let numargs = argtypes.len();
    let _ = nargs;
    let (prosrc_d, prosrc_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!prosrc_null, "null prosrc for function {fn_oid}");
    let prosrc = varlena_str(mcx, prosrc_d)?;
    let (proname_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONAME)?;
    let proname = name_str(mcx, proname_d)?;
    let (argnames_d, argnames_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGNAMES)?;
    let argnames = read_argnames_attr(mcx, argnames_d, argnames_null, numargs)?;
    let info = ProcInfo {
        proname: proname.as_str().to_string(),
        prosrc: prosrc.as_str().to_string(),
        argtypes,
        argmodes,
        argnames: argnames.iter().map(|s| s.as_str().to_string()).collect(),
        rettype: rettype_d.as_oid(),
        retset: proretset.as_bool(),
        prokind: prokind_d.as_i8(),
        readonly: provolatile.as_i8() != PROVOLATILE_VOLATILE,
    };
    ReleaseSysCache(tup);
    Ok(info)
}

fn name_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: NameData attr from a live syscache tuple — 64 NUL-padded bytes.
    let bytes = unsafe { core::slice::from_raw_parts(p, 64) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    let s = core::str::from_utf8(&bytes[..len]).expect("proname is server-encoding text");
    PgString::from_str_in(s, mcx)
}

fn varlena_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum from a live syscache tuple; the
    // image spans its header-declared size (external / short / 4B forms).
    let src = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            2 + types_tuple::varatt::vartag_size(*p.add(1))
        } else if b0 & 0x01 != 0 {
            (b0 as usize >> 1) & 0x7F
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    let img = detoast::detoast_attr(mcx, src)?;
    let s = core::str::from_utf8(&img[4..]).expect("text column is server-encoding text");
    PgString::from_str_in(s, mcx)
}

fn read_oidvector_attr<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, Oid>> {
    // SAFETY: proargtypes is a not-null plain-storage oidvector; the values
    // tail follows the 24-byte header in place, dim1 elements long.
    let args = unsafe {
        let p = d.as_usize() as *const array::oidvector;
        core::slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
    };
    let mut argtypes = mcx::vec_with_capacity_in(mcx, args.len())?;
    argtypes.extend_from_slice(args);
    Ok(argtypes)
}

fn read_argnames_attr<'mcx>(
    mcx: Mcx<'mcx>,
    d: Datum,
    isnull: bool,
    nargs: usize,
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut out: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve_exact(nargs).map_err(|_| mcx.oom(nargs))?;
    if isnull {
        for _ in 0..nargs {
            out.push(PgString::from_str_in("", mcx)?);
        }
        return Ok(out);
    }
    let img = varlena_bytes(mcx, d)?;
    let elems = datum::array_build::deconstruct_array_image(mcx, &img, -1, false, b'i')?;
    assert!(elems.len() >= nargs, "proargnames shorter than pronargs");
    for e in elems.iter().take(nargs) {
        out.push(varlena_str(mcx, *e)?);
    }
    Ok(out)
}

fn varlena_bytes<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: as varlena_str — image spans its header-declared size.
    let src = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            2 + types_tuple::varatt::vartag_size(*p.add(1))
        } else if b0 & 0x01 != 0 {
            (b0 as usize >> 1) & 0x7F
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    detoast::detoast_attr(mcx, src)
}

// do_compile / plpgsql_compile_callback (pl_comp.c).
#[allow(clippy::too_many_arguments)]
fn do_compile(
    fn_oid: Oid,
    fn_collation: Oid,
    fn_xmin: u32,
    fn_tid: (u32, u16),
    for_validator: bool,
    is_dml_trigger: bool,
    is_event_trigger: bool,
    call_expr: Option<types_core::fmgr::FnExprErased>,
) -> PgResult<PlFunction> {
    let mut proc = read_proc_row(fn_oid)?;
    // plpgsql_compile_error_callback covers header processing too: pre-parse
    // errors report "near line 1" (scanner initialized, nothing consumed).
    let hdr_ctx = |e: Box<types_error::PgError>| {
        attach_compile_context(e, &proc.proname, 1, for_validator, &proc.prosrc)
    };

    // C rejects these in the plain branch's pseudotype check (pl_comp.c:388);
    // hoisted here with the same message and errcode.
    if (!is_dml_trigger && proc.rettype == TRIGGEROID)
        || (!is_event_trigger && proc.rettype == EVENT_TRIGGEROID)
    {
        return Err(hdr_ctx(crate::exec::exec_err(
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            "trigger functions can only be called as triggers".to_string(),
        )));
    }
    let mut comp = CompState::new();
    comp.print_strict_params = print_strict_params_guc()?;
    // Only promote extra warnings and errors at CREATE FUNCTION time
    // (pl_comp.c:249-250).
    if for_validator {
        comp.extra_warnings = plpgsql_extra_checks("plpgsql.extra_warnings")?;
        comp.extra_errors = plpgsql_extra_checks("plpgsql.extra_errors")?;
    }
    // Outermost level: named after the function; holds params and FOUND.
    comp.ns_push_label(Some(&proc.proname), crate::gram::LABEL_BLOCK);

    let mut fn_argvarnos = Vec::with_capacity(proc.argtypes.len());
    let mut fn_arg_is_input = Vec::with_capacity(proc.argtypes.len());
    // fn_signature is format_procedure(fn_oid) (pl_comp.c:240) — declared
    // pg_proc types, not the resolved instantiation.
    let mut declared_argtypes: Vec<Oid> = Vec::new();
    let mut out_param_varno: Dno = -1;
    let mut new_varno: Dno = -1;
    let mut old_varno: Dno = -1;
    let mut rettypeid;
    let fn_retistuple;
    let fn_retisdomain;
    let fn_rettyplen;
    let fn_retbyval;
    let fn_is_trigger;

    if is_dml_trigger {
        if !proc.argtypes.is_empty() {
            return Err(hdr_ctx(Box::new(
                elog::ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("trigger functions cannot have declared arguments")
                    .errhint(
                        "The arguments of the trigger can be accessed through TG_NARGS and TG_ARGV instead.",
                    )
                    .into_error(),
            )));
        }
        rettypeid = types_core::InvalidOid;
        fn_retistuple = true;
        fn_retisdomain = false;
        fn_rettyplen = -1i16;
        fn_retbyval = false;

        new_varno = comp.build_rec("new", 0, true);
        old_varno = comp.build_rec("old", 0, true);

        const NAMEOID: Oid = 19;
        const TEXTOID: Oid = 25;
        const OIDOID: Oid = 26;
        const INT4OID: Oid = 23;
        const TEXTARRAYOID: Oid = 1009;
        let tg_vars: &[(&str, Oid, Oid, i32)] = &[
            ("tg_name", NAMEOID, fn_collation, PROMISE_TG_NAME),
            ("tg_when", TEXTOID, fn_collation, PROMISE_TG_WHEN),
            ("tg_level", TEXTOID, fn_collation, PROMISE_TG_LEVEL),
            ("tg_op", TEXTOID, fn_collation, PROMISE_TG_OP),
            ("tg_relid", OIDOID, types_core::InvalidOid, PROMISE_TG_RELID),
            ("tg_relname", NAMEOID, fn_collation, PROMISE_TG_TABLE_NAME),
            ("tg_table_name", NAMEOID, fn_collation, PROMISE_TG_TABLE_NAME),
            ("tg_table_schema", NAMEOID, fn_collation, PROMISE_TG_TABLE_SCHEMA),
            ("tg_nargs", INT4OID, types_core::InvalidOid, PROMISE_TG_NARGS),
            ("tg_argv", TEXTARRAYOID, fn_collation, PROMISE_TG_ARGV),
        ];
        for &(name, typoid, coll, promise) in tg_vars {
            let dno = comp.build_variable(
                name,
                0,
                CompState::build_datatype(typoid, -1, coll)?,
                true,
            )?;
            if let PlDatum::Var(v) = &mut comp.datums[dno as usize] {
                v.promise = promise;
            }
        }
        fn_is_trigger = FnTrigger::DmlTrigger;
    } else if is_event_trigger {
        if !proc.argtypes.is_empty() {
            return Err(hdr_ctx(crate::exec::exec_err(
                types_error::ERRCODE_INVALID_FUNCTION_DEFINITION,
                "event trigger functions cannot have declared arguments".to_string(),
            )));
        }
        rettypeid = VOIDOID;
        fn_retbyval = false;
        fn_retistuple = true;
        fn_retisdomain = false;
        fn_rettyplen = 0;
        // C makes tg_event/tg_tag lazy PROMISE vars; no promise machinery
        // here — the exec path materializes them eagerly at entry.
        let tg_event_varno = comp.build_variable(
            "tg_event",
            0,
            CompState::build_datatype(TEXTOID, -1, fn_collation)?,
            true,
        )?;
        let tg_tag_varno = comp.build_variable(
            "tg_tag",
            0,
            CompState::build_datatype(TEXTOID, -1, fn_collation)?,
            true,
        )?;
        fn_is_trigger = FnTrigger::EventTrigger { tg_event_varno, tg_tag_varno };
    } else {
        const PROARGMODE_IN: i8 = b'i' as i8;
        const PROARGMODE_OUT: i8 = b'o' as i8;
        const PROARGMODE_INOUT: i8 = b'b' as i8;
        const PROARGMODE_VARIADIC: i8 = b'v' as i8;
        const PROARGMODE_TABLE: i8 = b't' as i8;
        declared_argtypes = proc.argtypes.clone();
        funcapi::cfunc_resolve_polymorphic_argtypes(
            &mut proc.argtypes,
            &proc.argmodes,
            call_expr,
            for_validator,
            &proc.proname,
        )
        .map_err(|e| hdr_ctx(e))?;

        let mut out_arg_variables: Vec<Dno> = Vec::new();
        for (i, &argtypeid) in proc.argtypes.iter().enumerate() {
            // A VARIADIC parameter needs nothing special here: proargtypes
            // carries the array type and the caller packs the actuals; C's
            // only variadic arm in do_compile is the is-input test below.
            let argmode = proc.argmodes.get(i).copied().unwrap_or(PROARGMODE_IN);
            let buf = format!("${}", i + 1);
            let argdtype = CompState::build_datatype(argtypeid, -1, fn_collation)?;
            if argdtype.ttype == TypeKind::Pseudo {
                return Err(hdr_ctx(crate::exec::exec_err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "PL/pgSQL functions cannot accept type {}",
                        format_type::format_type_be(argtypeid)?
                    ),
                )));
            }
            let argname = &proc.argnames[i];
            let refname = if !argname.is_empty() { argname.as_str() } else { buf.as_str() };
            let dno = comp.build_variable(refname, 0, argdtype, false)?;
            fn_argvarnos.push(dno);
            fn_arg_is_input.push(matches!(
                argmode,
                PROARGMODE_IN | PROARGMODE_INOUT | PROARGMODE_VARIADIC
            ));
            if matches!(argmode, PROARGMODE_OUT | PROARGMODE_INOUT | PROARGMODE_TABLE) {
                out_arg_variables.push(dno);
            }
            add_parameter_name(&mut comp, dno, &buf).map_err(|e| hdr_ctx(e))?;
            if !argname.is_empty() {
                add_parameter_name(&mut comp, dno, argname).map_err(|e| hdr_ctx(e))?;
            }
        }

        let num_out_args = out_arg_variables.len();
        // out_param_varno: one OUT param is itself; several build a row.
        if out_arg_variables.len() > 1
            || (out_arg_variables.len() == 1 && proc.prokind == b'p' as i8)
        {
            out_param_varno = comp.build_row("(unnamed row)", -1, out_arg_variables);
        } else if out_arg_variables.len() == 1 {
            out_param_varno = out_arg_variables[0];
        }

        // Return type checks; polymorphic returns resolve through the call
        // expression, or the int4 family in validation mode (pl_comp.c:398).
        rettypeid = proc.rettype;
        if is_polymorphic(rettypeid) {
            const ANYARRAYOID: Oid = 2277;
            const ANYRANGEOID: Oid = 3831;
            const ANYMULTIRANGEOID: Oid = 4537;
            const ANYCOMPATIBLEARRAYOID: Oid = 5078;
            const ANYCOMPATIBLERANGEOID: Oid = 5080;
            const INT4ARRAYOID: Oid = 1007;
            const INT4RANGEOID: Oid = 3904;
            const INT4MULTIRANGEOID: Oid = 4451;
            if for_validator {
                rettypeid = match rettypeid {
                    ANYARRAYOID | ANYCOMPATIBLEARRAYOID => INT4ARRAYOID,
                    ANYRANGEOID | ANYCOMPATIBLERANGEOID => INT4RANGEOID,
                    ANYMULTIRANGEOID => INT4MULTIRANGEOID,
                    _ => types_core::INT4OID,
                };
            } else {
                rettypeid = call_expr
                    .as_ref()
                    .map_or(types_core::InvalidOid, funcapi::erased_call_expr_rettype);
                if !OidIsValid(rettypeid) {
                    return Err(hdr_ctx(crate::exec::exec_err(
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        format!(
                            "could not determine actual return type for polymorphic function \"{}\"",
                            proc.proname
                        ),
                    )));
                }
            }
        }
        let rettyptype = lsyscache::typ::get_typtype(rettypeid)?;
        if rettyptype == TYPTYPE_PSEUDO && rettypeid != VOIDOID && rettypeid != RECORDOID {
            return Err(hdr_ctx(crate::exec::exec_err(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "PL/pgSQL functions cannot return type {}",
                    format_type::format_type_be(rettypeid)?
                ),
            )));
        }
        fn_retistuple = lsyscache::typ::type_is_rowtype(rettypeid)? || rettypeid == RECORDOID;
        fn_retisdomain = rettyptype == b'd' as i8;
        let (l, bv) = lsyscache::typ::get_typlenbyval(rettypeid)?;
        fn_rettyplen = l;
        fn_retbyval = bv;
        // $0 references the resolved return type, only for polymorphic
        // returns not delivered through OUT params (pl_comp.c:469).
        if is_polymorphic(proc.rettype) && num_out_args == 0 {
            comp.build_variable(
                "$0",
                0,
                CompState::build_datatype(rettypeid, -1, fn_collation)?,
                true,
            )?;
        }
        fn_is_trigger = FnTrigger::NotTrigger;
    }

    let found_varno = comp.build_variable(
        "found",
        0,
        CompState::build_datatype(BOOLOID, -1, types_core::InvalidOid)?,
        true,
    )?;

    // Parse the body.
    let scan_cx = mcx::MemoryContext::new("plpgsql parse");
    let body_bytes = proc.prosrc.as_bytes();
    let scanbuf = mcx::slice_borrow_in(scan_cx.mcx(), body_bytes)?;
    let scanner = PlScanner::new(scan_cx.mcx(), scanbuf);
    let mut parser = Parser {
        sc: scanner,
        comp: &mut comp,
        check_syntax: for_validator,
        fn_rettype: rettypeid,
        fn_retset: proc.retset,
        fn_prokind: proc.prokind,
        fn_input_collation: fn_collation,
        fn_is_trigger: is_dml_trigger,
        out_param_varno,
        scratch: scan_cx.mcx(),
        last_endtoken_loc: -1,
    };
    // function_parse_error_transpose runs for every elevel via the
    // validator's error-context callback (pg_proc.c:1004); warnings emitted
    // mid-parse (shadowing, scanner escape warnings) transpose here since
    // they never reach attach_compile_context.
    let _emit_guard = if for_validator {
        let cb_prosrc = proc.prosrc.clone();
        Some(EmitCbGuard(elog::push_emit_context_callback(Box::new(move |e| {
            pg_proc::function_parse_error_transpose(e, &cb_prosrc);
        }))))
    } else {
        None
    };
    let parse_result = parser.parse_function_body();
    let latest_line = parser.sc.latest_lineno();
    let mut action = parse_result.map_err(|e| {
        attach_compile_context(e, &proc.proname, latest_line, for_validator, &proc.prosrc)
    })?;
    drop(_emit_guard);

    // pl_comp.c:691-693: OUT params / VOID / SETOF may fall off the end.
    if out_param_varno >= 0 || rettypeid == VOIDOID || proc.retset {
        add_dummy_return(&mut action, out_param_varno, &mut comp.nstatements);
    }

    // format_procedure covers input args only (proargtypes).
    let sig_argtypes: Vec<Oid> = declared_argtypes
        .iter()
        .zip(fn_arg_is_input.iter().chain(std::iter::repeat(&true)))
        .filter_map(|(&t, &is_in)| is_in.then_some(t))
        .collect();
    // pl_comp.c: fn_signature = format_procedure(fn_oid) — schema-qualified
    // when the function is not visible on the (possibly restricted) search
    // path; SRO error contexts depend on the qualification.
    let fn_signature = {
        let sig_cx = mcx::MemoryContext::new("plpgsql fn_signature");
        adt_regproc::format_procedure(sig_cx.mcx(), fn_oid)?
    };
    Ok(PlFunction {
        fn_signature,
        fn_oid,
        fn_xmin,
        fn_tid,
        fn_input_collation: fn_collation,
        fn_rettype: rettypeid,
        fn_rettyplen,
        fn_retbyval,
        fn_retistuple,
        fn_retisdomain,
        fn_retset: proc.retset,
        fn_readonly: proc.readonly,
        fn_prokind: proc.prokind,
        fn_nargs: sig_argtypes.len() as i16,
        fn_argvarnos,
        fn_arg_is_input,
        fn_is_trigger,
        new_varno,
        old_varno,
        found_varno,
        out_param_varno,
        datums: std::mem::take(&mut comp.datums),
        ns: std::mem::take(&mut comp.ns),
        action,
        resolve_option: comp.resolve_option,
        print_strict_params: comp.print_strict_params,
        nstatements: comp.nstatements,
        expr_ids: std::mem::take(&mut comp.expr_ids),
    })
}

// add_dummy_return (pl_comp.c): wrap labeled/EXCEPTION outer blocks so the
// appended RETURN sits outside them.
fn add_dummy_return(action: &mut PlBlock, out_param_varno: Dno, nstatements: &mut u32) {
    if action.exceptions.is_some() || action.label.is_some() {
        *nstatements += 1;
        let inner = std::mem::replace(
            action,
            PlBlock {
                lineno: 0,
                label: None,
                body: Vec::new(),
                initvarnos: Vec::new(),
                exceptions: None,
            },
        );
        action.body.push(PlStmt::Block(inner));
    }
    if !matches!(action.body.last(), Some(PlStmt::Return { .. })) {
        *nstatements += 1;
        action.body.push(PlStmt::Return { lineno: 0, expr: None, retvarno: out_param_varno });
    }
}

// PROCPERF P2: the runtime extra-check levels and the print_strict_params /
// check_asserts flags are consulted per statement execution (too_many_rows,
// pl_exec.c:4217), per row move (strict_multi_assignment) and per ASSERT —
// measured 16K Ir/call of by-name GetConfigOption on TPROC-C NEWORD. C binds
// these GUCs to process globals via DefineCustom*Variable hooks
// (pl_handler.c:61-149) and reads them for free; this port carries them as
// SET-created placeholders with no hook lane, so the PARSED values are
// snapshotted per backend thread, keyed by the GUC store's mutation counter
// (the guc::layers cache pattern: every value mutation — SET / RESET / xact
// revert / reload / session bind — goes through with_store_mut, which bumps
// the counter). NOT session state in itself: a session rebinding onto this
// thread mutates the thread's store and thereby invalidates the snapshot.
#[derive(Clone, Copy)]
struct PlGucValues {
    mutations: u64,
    extra_errors: u32,
    extra_warnings: u32,
    print_strict_params: bool,
    check_asserts: bool,
}

thread_local! {
    static PL_GUC_VALUES: core::cell::Cell<Option<PlGucValues>> =
        const { core::cell::Cell::new(None) };
}

// Kill switch: PGRUST_PLPGSQL_GUC_SNAPSHOT=0 restores the per-read by-name
// GetConfigOption behavior (the snapshot is then rebuilt on every read).
// Latched once per process.
fn guc_snapshot_disabled() -> bool {
    static DISABLED: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *DISABLED.get_or_init(|| {
        matches!(std::env::var("PGRUST_PLPGSQL_GUC_SNAPSHOT").as_deref(), Ok("0") | Ok("off"))
    })
}

fn pl_guc_values() -> PgResult<PlGucValues> {
    let mutations = guc::store::store_mutation_count();
    if !guc_snapshot_disabled() {
        if let Some(v) = PL_GUC_VALUES.with(core::cell::Cell::get) {
            if v.mutations == mutations {
                return Ok(v);
            }
        }
    }
    let v = PlGucValues {
        mutations,
        extra_errors: plpgsql_extra_checks("plpgsql.extra_errors")?,
        extra_warnings: plpgsql_extra_checks("plpgsql.extra_warnings")?,
        print_strict_params: print_strict_params_uncached()?,
        check_asserts: check_asserts_uncached()?,
    };
    PL_GUC_VALUES.with(|c| c.set(Some(v)));
    Ok(v)
}

// plpgsql_check_asserts: DefineCustomBoolVariable is the extension-GUC lane;
// the SET-created placeholder carries the session value and an unset name
// means C's default (true). (Moved here from exec_stmt_assert; parse
// semantics byte-preserved.)
fn check_asserts_uncached() -> PgResult<bool> {
    Ok(guc::GetConfigOption("plpgsql.check_asserts", true, false)?
        .map_or(true, |v| !matches!(v.as_str(), "off" | "false" | "no" | "0" | "f" | "n")))
}

pub(crate) fn check_asserts_enabled() -> PgResult<bool> {
    Ok(pl_guc_values()?.check_asserts)
}

// plpgsql_extra_checks_check_hook (pl_handler.c:61-104) applied to the
// SET-created placeholder at compile time; an unset name is C's default
// "none". Invalid tokens were accepted by the placeholder SET, so they
// contribute nothing here instead of failing.
fn plpgsql_extra_checks(guc_name: &str) -> PgResult<u32> {
    let Some(v) = guc::GetConfigOption(guc_name, true, false)? else {
        return Ok(0);
    };
    let v = v.trim();
    if v.eq_ignore_ascii_case("all") {
        return Ok(crate::comp::XCHECK_ALL);
    }
    if v.eq_ignore_ascii_case("none") {
        return Ok(0);
    }
    let mut checks = 0u32;
    for tok in v.split(',') {
        let tok = tok.trim();
        if tok.eq_ignore_ascii_case("shadowed_variables") {
            checks |= crate::comp::XCHECK_SHADOWVAR;
        } else if tok.eq_ignore_ascii_case("too_many_rows") {
            checks |= crate::comp::XCHECK_TOOMANYROWS;
        } else if tok.eq_ignore_ascii_case("strict_multi_assignment") {
            checks |= crate::comp::XCHECK_STRICTMULTIASSIGNMENT;
        }
    }
    Ok(checks)
}

// The runtime extra-check level for one PLPGSQL_XCHECK bit: extra_errors
// wins over extra_warnings (pl_exec.c:4217-4220, 7196-7202); None means the
// check is off. Reads the mutation-keyed snapshot (PROCPERF P2, above).
pub(crate) fn extra_checks_level(mask: u32) -> PgResult<Option<types_error::ErrorLevel>> {
    let v = pl_guc_values()?;
    if v.extra_errors & mask != 0 {
        return Ok(Some(ERROR));
    }
    if v.extra_warnings & mask != 0 {
        return Ok(Some(types_error::WARNING));
    }
    Ok(None)
}

// plpgsql_print_strict_params: bool GUC via the placeholder lane
// (check_asserts precedent); C's default is false.
fn print_strict_params_uncached() -> PgResult<bool> {
    Ok(guc::GetConfigOption("plpgsql.print_strict_params", true, false)?.is_some_and(|v| {
        matches!(v.trim().to_ascii_lowercase().as_str(), "on" | "true" | "yes" | "1" | "t" | "y")
    }))
}

fn print_strict_params_guc() -> PgResult<bool> {
    Ok(pl_guc_values()?.print_strict_params)
}

struct EmitCbGuard(u64);

impl Drop for EmitCbGuard {
    fn drop(&mut self) {
        elog::pop_emit_context_callback(self.0);
    }
}

#[cold]
fn attach_compile_context(
    mut e: Box<types_error::PgError>,
    fname: &str,
    line: i32,
    for_validator: bool,
    prosrc: &str,
) -> Box<types_error::PgError> {
    // plpgsql_compile_error_callback: at validation the cursor transposes
    // onto the CREATE statement (no context line); otherwise a context line.
    if for_validator && pg_proc::function_parse_error_transpose(&mut e, prosrc) {
        return e;
    }
    if e.context.is_none() {
        e.context = Some(format!(
            "compilation of PL/pgSQL function \"{fname}\" near line {line}"
        ));
    }
    e
}

// add_parameter_name (pl_comp.c); itemtype NSTYPE_VAR for scalar params,
// NSTYPE_REC for composite/record params (pl_comp.c:341-349).
fn add_parameter_name(comp: &mut CompState, dno: Dno, name: &str) -> PgResult<()> {
    if comp.ns_lookup(comp.ns_top, true, name, None, None).is_some() {
        return Err(crate::exec::exec_err(
            types_error::ERRCODE_INVALID_FUNCTION_DEFINITION,
            format!("parameter name \"{name}\" used more than once"),
        ));
    }
    let itemtype = match &comp.datums[dno as usize] {
        PlDatum::Var(_) => NsType::Var,
        PlDatum::Rec(_) => NsType::Rec,
        _ => unreachable!("parameter datum is Var or Rec (pl_comp.c:341-349)"),
    };
    comp.ns_additem(itemtype, dno, name);
    Ok(())
}

// plpgsql_call_handler (pl_handler.c), function + DML trigger arms.
fn plpgsql_call_handler(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let fn_oid = flinfo.as_ref().map(|f| f.fn_oid).expect("plpgsql_call_handler needs flinfo");

    // CALLED_AS_TRIGGER / CALLED_AS_EVENT_TRIGGER demux on the context tag.
    let ctx_tag = fcinfo.context.map(|p| {
        // SAFETY: a set fcinfo.context points at a live tag-discriminated
        // FmNode for the duration of the call (fmgr contract).
        unsafe { p.as_ref().tag }
    });
    let evtrigdata: Option<&event_trigger::EventTriggerData> =
        if ctx_tag == Some(event_trigger::T_EVENT_TRIGGER_DATA) {
            // SAFETY: T_EventTriggerData tag — the firing side installed an
            // EventTriggerData whose first field is this FmNode; it outlives
            // the call.
            Some(unsafe {
                fcinfo.context.unwrap().cast::<event_trigger::EventTriggerData>().as_ref()
            })
        } else {
            None
        };

    // SAFETY: fcinfo.context, when set, is ExecuteCallStmt's live CallContext
    // armed for this call, with no `&mut` formed during it.
    let nonatomic = ctx_tag == Some(fmgr::T_CALL_CONTEXT)
        && unsafe { fcinfo.call_context() }.is_some_and(|cc| !cc.atomic);

    let rc = spi::SPI_connect_ext(if nonatomic { spi::SPI_OPT_NONATOMIC } else { 0 })?;
    assert_eq!(rc, spi::SPI_OK_CONNECT, "SPI_connect failed");

    let outcome = (|| -> PgResult<Datum> {
        // SAFETY: a T_TriggerData-tagged fcinfo context is the executor's
        // live TriggerData for this call (ExecCallTriggerFunc).
        let trigdata: Option<&types_trigger_call::TriggerData<'_, '_>> =
            unsafe { types_trigger_call::trigger_data_from_fcinfo(fcinfo) };
        let kind = match (trigdata, &evtrigdata) {
            (Some(td), None) => CallKind::Trigger(td.tg_trigger.tgoid),
            (None, Some(_)) => CallKind::EventTrigger,
            _ => CallKind::Function,
        };
        let call_expr = flinfo.as_ref().and_then(|f| f.fn_expr);
        let entry = plpgsql_compile(fn_oid, fcinfo.fncollation, false, kind, call_expr)?;
        entry.use_count.set(entry.use_count.get() + 1);
        let r = match (entry.func.fn_is_trigger, trigdata, evtrigdata) {
            (FnTrigger::DmlTrigger, Some(td), None) => {
                plpgsql_exec_trigger(&entry.func, td, fcinfo)
            }
            (FnTrigger::EventTrigger { .. }, None, Some(td)) => {
                plpgsql_exec_event_trigger(&entry.func, td).map(|()| {
                    fcinfo.isnull = true;
                    Datum::null()
                })
            }
            (FnTrigger::NotTrigger, None, None) => {
                plpgsql_exec_function(&entry.func, flinfo.as_deref(), fcinfo, !nonatomic)
            }
            _ => panic!(
                "plpgsql_call_handler: call context does not match the compiled \
                 trigger-ness (function {fn_oid})"
            ),
        };
        entry.use_count.set(entry.use_count.get() - 1);
        r
    })();

    let result = outcome?;
    let rc = spi::SPI_finish()?;
    assert_eq!(rc, spi::SPI_OK_FINISH, "SPI_finish failed");
    Ok(result)
}

fn plpgsql_inline_handler(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // SAFETY: ExecuteDoStmt passes a live InlineCodeBlock for this call.
    let cb: &types_nodes::parsenodes::InlineCodeBlock =
        unsafe { &*(fcinfo.args[0].value.as_usize() as *const _) };

    let rc = spi::SPI_connect_ext(if cb.atomic { 0 } else { spi::SPI_OPT_NONATOMIC })?;
    assert_eq!(rc, spi::SPI_OK_CONNECT, "SPI_connect failed");

    let func = compile_inline(cb.source_text)?;
    let mut fake = fmgr::LocalFcinfo::<0>::fresh(types_core::InvalidOid);
    let result = plpgsql_exec_function(&func, None, &mut fake, cb.atomic);
    crate::exec::free_function_plans(&func.expr_ids);
    result?;

    let rc = spi::SPI_finish()?;
    assert_eq!(rc, spi::SPI_OK_FINISH, "SPI_finish failed");
    Ok(Datum::null())
}

// plpgsql_compile_inline (pl_comp.c): uncached, VOID, no args.
fn compile_inline(src: &str) -> PgResult<PlFunction> {
    let func_name = "inline_code_block";
    let mut comp = CompState::new();
    // print_strict_params follows the GUC even inline (pl_comp.c:790); the
    // extra checks stay 0 (pl_comp.c:796-797).
    comp.print_strict_params = print_strict_params_guc()?;
    comp.ns_push_label(Some(func_name), crate::gram::LABEL_BLOCK);
    let found_varno = comp.build_variable(
        "found",
        0,
        CompState::build_datatype(BOOLOID, -1, types_core::InvalidOid)?,
        true,
    )?;

    let scan_cx = mcx::MemoryContext::new("plpgsql inline parse");
    let scanbuf = mcx::slice_borrow_in(scan_cx.mcx(), src.as_bytes())?;
    let scanner = PlScanner::new(scan_cx.mcx(), scanbuf);
    let mut parser = Parser {
        sc: scanner,
        comp: &mut comp,
        check_syntax: guc_check_function_bodies(),
        fn_rettype: VOIDOID,
        fn_retset: false,
        fn_prokind: PROKIND_FUNCTION,
        fn_input_collation: types_core::InvalidOid,
        fn_is_trigger: false,
        out_param_varno: -1,
        scratch: scan_cx.mcx(),
        last_endtoken_loc: -1,
    };
    let parse_result = parser.parse_function_body();
    let latest_line = parser.sc.latest_lineno();
    // C's inline callback always tries the position transpose (cbarg
    // .proc_source is set unconditionally for inline blocks).
    let action = parse_result
        .map_err(|e| attach_compile_context(e, func_name, latest_line, true, src))?;

    Ok(PlFunction {
        fn_signature: func_name.to_string(),
        fn_oid: types_core::InvalidOid,
        fn_xmin: 0,
        fn_tid: (0, 0),
        fn_input_collation: types_core::InvalidOid,
        fn_rettype: VOIDOID,
        fn_rettyplen: 4,
        fn_retbyval: true,
        fn_retistuple: false,
        fn_retisdomain: false,
        fn_retset: false,
        fn_readonly: false,
        fn_prokind: PROKIND_FUNCTION,
        fn_nargs: 0,
        fn_argvarnos: Vec::new(),
        fn_arg_is_input: Vec::new(),
        fn_is_trigger: FnTrigger::NotTrigger,
        new_varno: -1,
        old_varno: -1,
        found_varno,
        out_param_varno: -1,
        datums: std::mem::take(&mut comp.datums),
        ns: std::mem::take(&mut comp.ns),
        action,
        resolve_option: comp.resolve_option,
        print_strict_params: comp.print_strict_params,
        nstatements: comp.nstatements,
        expr_ids: std::mem::take(&mut comp.expr_ids),
    })
}

// plpgsql_validator (pl_handler.c).
fn plpgsql_validator(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let funcoid = fcinfo.args[0].value.as_oid();
    let _ = flinfo;

    // CheckFunctionValidatorAccess: object_aclcheck EXECUTE — superuser
    // fast path per repo convention (sql validator precedent).

    let info = read_proc_row(funcoid)?;
    // Pseudotype result disallowed except TRIGGER, EVTTRIGGER, RECORD, VOID,
    // or polymorphic; pseudotype args except RECORD or polymorphic.
    let functyptype = lsyscache::typ::get_typtype(info.rettype)?;
    let mut is_dml_trigger = false;
    if functyptype == TYPTYPE_PSEUDO
        && info.rettype != RECORDOID
        && info.rettype != VOIDOID
        && !is_polymorphic(info.rettype)
    {
        if info.rettype == TRIGGEROID {
            is_dml_trigger = true;
        } else if info.rettype != EVENT_TRIGGEROID {
            return Err(crate::exec::exec_err(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "PL/pgSQL functions cannot return type {}",
                    format_type::format_type_be(info.rettype)?
                ),
            ));
        }
    }
    for &t in &info.argtypes {
        if lsyscache::typ::get_typtype(t)? == TYPTYPE_PSEUDO
            && t != RECORDOID
            && !is_polymorphic(t)
        {
            return Err(crate::exec::exec_err(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "PL/pgSQL functions cannot accept type {}",
                    format_type::format_type_be(t)?
                ),
            ));
        }
    }
    let kind = if is_dml_trigger {
        CallKind::Trigger(types_core::InvalidOid)
    } else if info.rettype == EVENT_TRIGGEROID {
        CallKind::EventTrigger
    } else {
        CallKind::Function
    };

    if guc_check_function_bodies() {
        let rc = spi::SPI_connect_ext(0)?;
        assert_eq!(rc, spi::SPI_OK_CONNECT, "SPI_connect failed");
        let r = plpgsql_compile(funcoid, types_core::InvalidOid, true, kind, None);
        let _ = spi::SPI_finish()?;
        r?;
    }
    Ok(Datum::null())
}

fn guc_check_function_bodies() -> bool {
    guc_tables::backing::check_function_bodies()
}

// plpgsql_exec_function (pl_exec.c).
fn plpgsql_exec_function(
    func: &PlFunction,
    flinfo: Option<&FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
    atomic: bool,
) -> PgResult<Datum> {
    let mut estate = Estate::new(func, func.fn_readonly, atomic);
    if let Some(rsi) = fcinfo.rsinfo_mut() {
        estate.rsi = Some(crate::exec::RsiSnapshot {
            allowed_modes: rsi.allowedModes,
            expected_desc: rsi.expectedDesc,
        });
    }
    let _frame = crate::exec::FrameGuard::push_pl(&estate);

    // Store call arguments into the argument variables; OUT-only args have
    // no fcinfo slot and stay NULL.
    estate.frame.text.set(Some("while storing call arguments into local variables"));
    let mut argi = 0usize;
    for (i, &dno) in func.fn_argvarnos.iter().enumerate() {
        if !func.fn_arg_is_input.get(i).copied().unwrap_or(true) {
            continue;
        }
        let (value, isnull) = {
            let arg = &fcinfo.args[argi];
            (arg.value, arg.isnull)
        };
        argi += 1;
        match &func.datums[dno as usize] {
            // Argument datums live in the caller's context for the call's
            // duration; no copy (C behaves identically for IN args).
            PlDatum::Var(_) => estate.set_var(dno, value, isnull),
            PlDatum::Rec(_) => {
                if isnull {
                    estate.datums[dno as usize] = crate::exec::DatumVal::Rec(None);
                } else {
                    match estate.exec_assign_value(dno, value, false, RECORDOID, -1) {
                        Ok(()) => {}
                        Err(e) => return Err(attach_exec_context(e, &estate)),
                    }
                }
            }
            _ => panic!("plpgsql: argument datum is not a Var or Rec"),
        }
    }

    // C sets FOUND=false at function entry (pl_exec.c:623).
    estate.frame.text.set(Some("during function entry"));
    estate.set_var(func.found_varno, Datum::from_bool(false), false);
    estate.frame.text.set(None);

    let outcome = (|| -> PgResult<i32> {
        let rc = estate.exec_toplevel_block(&func.action)?;
        Ok(rc)
    })();

    let rc = match outcome {
        Ok(rc) => rc,
        Err(e) => return Err(attach_exec_context(e, &estate)),
    };

    if rc != RC_RETURN {
        debug_assert_eq!(rc, RC_OK);
        if func.fn_rettype == VOIDOID {
            // C's compiled-in dummy RETURN hits the void hack (pl_exec.c:3303-
            // 3314): functions return a non-null VOID datum; procedures null.
            if func.fn_prokind != PROKIND_PROCEDURE {
                fcinfo.isnull = false;
                return Ok(Datum::from_usize(0));
            }
            fcinfo.isnull = true;
            return Ok(Datum::null());
        }
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(types_error::ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT)
                .errmsg("control reached end of function without RETURN")
                .errcontext_msg(format!("PL/pgSQL function {}", func.fn_signature))
                .into_error(),
        ));
    }

    estate.frame.text.set(Some("while casting return value to function's return type"));

    if func.fn_retset {
        // pl_exec.c:651-680: hand the tuplestore to the caller's rsinfo.
        let Some(store) = estate.take_tuple_store() else {
            // Empty set: signal materialize mode with an empty store (the
            // executor's setResult-NULL leg also yields zero rows).
            let Some(rsi) = fcinfo.rsinfo_mut() else {
                return Err(crate::exec::exec_err(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "set-valued function called in context that cannot accept a set"
                        .to_string(),
                ));
            };
            rsi.returnMode = fmgr::SetFunctionReturnMode::Materialize;
            fcinfo.isnull = true;
            return Ok(Datum::null());
        };
        let rsi = fcinfo.rsinfo_mut().expect("tuple store implies rsinfo");
        rsi.returnMode = fmgr::SetFunctionReturnMode::Materialize;
        rsi.setResult = Some(Box::new(store));
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }

    if func.fn_retistuple && !estate.retisnull {
        let out = coerce_function_result_tuple(&mut estate, func, flinfo, fcinfo);
        return match out {
            Ok(d) => {
                fcinfo.isnull = false;
                Ok(d)
            }
            Err(e) => Err(attach_exec_context(e, &estate)),
        };
    }

    // Scalar case: cast the return value to the function's return type and
    // copy it out of SPI/estate memory (it must survive SPI_finish and
    // estate drop).
    let mut isnull = estate.retisnull;
    let mut retval = estate.retval;
    if isnull && func.fn_retisdomain {
        // NULL into a domain return type still checks constraints.
        retval = match estate.exec_cast_value(
            retval,
            &mut isnull,
            estate.rettype,
            -1,
            func.fn_rettype,
            -1,
        ) {
            Ok(v) => v,
            Err(e) => return Err(attach_exec_context(e, &estate)),
        };
    } else if !isnull {
        let rt = if OidIsValid(estate.rettype) { estate.rettype } else { func.fn_rettype };
        retval = match estate.exec_cast_value(retval, &mut isnull, rt, -1, func.fn_rettype, -1) {
            Ok(v) => v,
            Err(e) => return Err(attach_exec_context(e, &estate)),
        };
    }
    fcinfo.isnull = isnull;
    if isnull || func.fn_retbyval {
        return Ok(retval);
    }
    // SAFETY: retval is a live by-ref datum of the return type's typlen
    // discipline; copied into the caller-armed result context.
    let out = unsafe {
        execexpr::agg_datum_copy(fcinfo.result_mcx(), retval, func.fn_rettyplen)?
    };
    Ok(out)
}

// plpgsql_exec_event_trigger (pl_exec.c). No return value is delivered.
fn plpgsql_exec_event_trigger(
    func: &PlFunction,
    trigdata: &event_trigger::EventTriggerData,
) -> PgResult<()> {
    let FnTrigger::EventTrigger { tg_event_varno, tg_tag_varno } = func.fn_is_trigger else {
        panic!("plpgsql_exec_event_trigger: function is not an event trigger");
    };
    let mut estate = Estate::new(func, func.fn_readonly, true);

    let _frame = crate::exec::FrameGuard::push_pl(&estate);
    // Divergence from C: tg_event/tg_tag are eager, not PROMISE-lazy.
    estate.frame.text.set(Some("during initialization of execution state"));
    if let Err(e) = estate.assign_text_var(tg_event_varno, trigdata.event) {
        return Err(attach_exec_context(e, &estate));
    }
    let tagname = cmdtag::GetCommandTagName(trigdata.tag);
    if let Err(e) = estate.assign_text_var(tg_tag_varno, tagname) {
        return Err(attach_exec_context(e, &estate));
    }
    estate.frame.text.set(None);

    let rc = match estate.exec_toplevel_block(&func.action) {
        Ok(rc) => rc,
        Err(e) => return Err(attach_exec_context(e, &estate)),
    };
    // C appends an implicit RETURN at compile (pl_comp.c), so falling off the
    // end is success; this port accepts RC_OK directly (void-fn precedent).
    debug_assert!(rc == RC_RETURN || rc == RC_OK);
    Ok(())
}

// coerce_function_result_tuple (pl_exec.c:824) + the get_call_result_type
// dispatch of plpgsql_exec_function's retistuple arm.
fn coerce_function_result_tuple(
    estate: &mut Estate<'_>,
    func: &PlFunction,
    flinfo: Option<&FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    use funcapi::TypeFuncClass;

    // Source row: a returned record variable, or a composite Datum.
    let rv: crate::exec::RecValue = match estate.ret_rec.take() {
        Some(rv) => rv,
        None => {
            let retval = estate.retval;
            let (desc, src, values, nulls) = estate.deconstruct_composite(retval)?;
            crate::exec::RecValue { desc, values, nulls, src_desc: Some(src), empty: false }
        }
    };

    let cx = mcx::MemoryContext::new("plpgsql result-type resolution");
    let resolved = match flinfo {
        Some(fl) => funcapi::get_call_result_type(cx.mcx(), fl, None)?,
        None => funcapi::ResolvedResultType {
            class: if func.fn_rettype == RECORDOID {
                TypeFuncClass::Record
            } else {
                TypeFuncClass::Composite
            },
            result_type_id: func.fn_rettype,
            result_tuple_desc: None,
        },
    };

    let out_mcx = fcinfo.result_mcx();
    match resolved.class {
        TypeFuncClass::Composite | TypeFuncClass::CompositeDomain => {
            if resolved.class == TypeFuncClass::CompositeDomain {
                panic!(
                    "plpgsql coerce_function_result_tuple: domain-over-composite \
                     return (domain_check) unported — unit backend-pl-plpgsql-exec"
                );
            }
            let expected = resolved
                .result_tuple_desc
                .as_ref()
                .expect("composite result carries a tupdesc");
            let dst = crate::exec::RecDesc::from_tupdesc(expected);
            let (values, nulls) = crate::exec::convert_values_by_position(
                &rv.desc,
                &rv.values,
                &rv.nulls,
                &dst,
                "returned record type does not match expected record type",
            )?;
            let mut td = tupdesc::CreateTupleDescCopy(out_mcx, expected)?;
            td.tdtypeid = resolved.result_type_id;
            if td.tdtypeid == RECORDOID {
                // C BlessTupleDesc in internal_get_result_type: OUT-param
                // rowtypes are anonymous records.
                if td.tdtypmod < 0 {
                    typcache::assign_record_type_typmod(&mut td)?;
                }
            } else {
                td.tdtypmod = -1;
            }
            let tup = heaptuple::heap_form_tuple(out_mcx, &td, &values, &nulls)?;
            let img = tup.header_ptr();
            core::mem::forget(tup);
            Ok(Datum::from_usize(img as usize))
        }
        _ => {
            // Generic RECORD caller: pass the row back with a blessed typmod.
            let src = rv.src_desc.clone().expect("RecValue carries its source tupdesc");
            let mut td = tupdesc::CreateTupleDescCopy(out_mcx, &src)?;
            td.tdtypeid = RECORDOID;
            if td.tdtypmod < 0 {
                typcache::assign_record_type_typmod(&mut td)?;
            }
            let tup = heaptuple::heap_form_tuple(out_mcx, &td, &rv.values, &rv.nulls)?;
            let img = tup.header_ptr();
            core::mem::forget(tup);
            Ok(Datum::from_usize(img as usize))
        }
    }
}

use crate::exec::attach_frame_context_at_exit as attach_exec_context;

const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
const TYPALIGN_INT: u8 = b'i';

fn recdesc_from_tupdesc(td: &types_tuple::TupleDescData<'_>) -> (crate::exec::RecDesc, Vec<bool>) {
    let natts = td.attrs.len();
    let mut d = crate::exec::RecDesc {
        names: Vec::with_capacity(natts),
        types: Vec::with_capacity(natts),
        typmods: Vec::with_capacity(natts),
        typlens: Vec::with_capacity(natts),
        typbyvals: Vec::with_capacity(natts),
        dropped: Vec::with_capacity(natts),
    };
    let mut generated = Vec::with_capacity(natts);
    for a in td.attrs.iter() {
        d.names.push(String::from_utf8_lossy(a.attname.name_str()).to_ascii_lowercase());
        d.types.push(a.atttypid);
        d.typmods.push(a.atttypmod);
        d.typlens.push(a.attlen);
        d.typbyvals.push(a.attbyval);
        d.dropped.push(a.attisdropped);
        generated.push(a.attgenerated == ATTRIBUTE_GENERATED_STORED);
    }
    (d, generated)
}

// name datum (NAMEOID): 64-byte NUL-padded image in `mcx`.
fn name_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    let mut v: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, 64)?;
    let b = s.as_bytes();
    let n = b.len().min(63);
    mcx::vec_append_bytes(&mut v, &b[..n])?;
    v.resize(64, 0);
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    Ok(d)
}

fn text_datum(mcx: Mcx<'_>, s: &[u8]) -> PgResult<Datum> {
    Ok(fmgr::varlena_result(varlena::cstring_to_text(mcx, s)?))
}

// plpgsql_fulfill_promise, fulfilled eagerly at trigger entry (each TG_* is
// computed at most once per call in C too; eager cost is the only divergence).
fn fulfill_trigger_promises(
    estate: &mut Estate<'_>,
    func: &PlFunction,
    trigdata: &types_trigger_call::TriggerData<'_, '_>,
) -> PgResult<()> {
    use types_trigger::*;
    let mcx = estate.datum_mcx();
    let ev = trigdata.tg_event;
    let rel = trigdata.tg_relation;
    let trig = trigdata.tg_trigger;
    for d in &func.datums {
        let PlDatum::Var(v) = d else { continue };
        if v.promise == PROMISE_NONE {
            continue;
        }
        let (value, isnull) = match v.promise {
            PROMISE_TG_NAME => (name_datum(mcx, trig.tgname.as_str())?, false),
            PROMISE_TG_WHEN => {
                let s = if ev & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_BEFORE {
                    "BEFORE"
                } else if ev & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_AFTER {
                    "AFTER"
                } else if ev & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_INSTEAD {
                    "INSTEAD OF"
                } else {
                    panic!("unrecognized trigger execution time: not BEFORE, AFTER, or INSTEAD OF")
                };
                (text_datum(mcx, s.as_bytes())?, false)
            }
            PROMISE_TG_LEVEL => {
                let s = if TRIGGER_FIRED_FOR_ROW(ev) { "ROW" } else { "STATEMENT" };
                (text_datum(mcx, s.as_bytes())?, false)
            }
            PROMISE_TG_OP => {
                let s = if TRIGGER_FIRED_BY_INSERT(ev) {
                    "INSERT"
                } else if TRIGGER_FIRED_BY_UPDATE(ev) {
                    "UPDATE"
                } else if TRIGGER_FIRED_BY_DELETE(ev) {
                    "DELETE"
                } else if ev & TRIGGER_EVENT_OPMASK == TRIGGER_EVENT_TRUNCATE {
                    "TRUNCATE"
                } else {
                    panic!("unrecognized trigger action: not INSERT, DELETE, UPDATE, or TRUNCATE")
                };
                (text_datum(mcx, s.as_bytes())?, false)
            }
            PROMISE_TG_RELID => (Datum::from_oid(rel.rd_id), false),
            PROMISE_TG_TABLE_NAME => {
                let name = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
                (name_datum(mcx, &name)?, false)
            }
            PROMISE_TG_TABLE_SCHEMA => {
                let nsp = lsyscache::misc::get_namespace_name(mcx, rel.rd_rel.relnamespace)?
                    .unwrap_or_else(|| {
                        panic!(
                            "cache lookup failed for namespace {}",
                            rel.rd_rel.relnamespace
                        )
                    });
                (name_datum(mcx, nsp.as_str())?, false)
            }
            PROMISE_TG_NARGS => (Datum::from_i16(trig.tgnargs), false),
            PROMISE_TG_ARGV => {
                if trig.tgnargs > 0 {
                    let mut elems = Vec::with_capacity(trig.tgargs.len());
                    for a in trig.tgargs.iter() {
                        elems.push(text_datum(mcx, a.as_str().as_bytes())?);
                    }
                    // tg_argv[] subscripts start at zero, so lbs = [0].
                    let img = arrayfuncs::construct_md_array(
                        mcx,
                        &elems,
                        None,
                        1,
                        &[elems.len() as i32],
                        &[0],
                        25, // TEXTOID
                        -1,
                        false,
                        TYPALIGN_INT,
                    )?;
                    let d = Datum::from_usize(img.as_ptr() as usize);
                    core::mem::forget(img);
                    (d, false)
                } else {
                    (Datum::null(), true)
                }
            }
            other => panic!("unrecognized promise type: {other}"),
        };
        estate.set_var(v.dno, value, isnull);
    }
    Ok(())
}

fn bind_trigger_tuple(
    estate: &Estate<'_>,
    rv: &mut crate::exec::RecValue,
    tuple: Option<core::ptr::NonNull<types_tuple::HeapTupleData<'_>>>,
    tupdesc: &types_tuple::TupleDescData<'_>,
) -> PgResult<()> {
    let Some(t) = tuple else {
        panic!("plpgsql_exec_trigger: expected trigger tuple is missing");
    };
    // SAFETY: the executor's TriggerData tuples are live for the call.
    let t = unsafe { t.as_ref() };
    let natts = rv.desc.types.len();
    types_tuple::heap_deform_tuple(t, tupdesc, &mut rv.values, &mut rv.nulls);
    for i in 0..natts {
        if !rv.desc.dropped[i] {
            rv.values[i] = estate.copy_to_datum_ctx(
                rv.values[i],
                rv.nulls[i],
                rv.desc.typlens[i],
                rv.desc.typbyvals[i],
            )?;
        }
    }
    Ok(())
}

// build_attrmap_by_position (attmap.c) over RecDesc shapes, with the map
// applied in place of execute_attr_map_tuple.
fn map_returned_row(
    rv: &crate::exec::RecValue,
    out: &crate::exec::RecDesc,
) -> PgResult<(Vec<Datum>, Vec<bool>)> {
    const MSG: &str = "returned row structure does not match the structure of the triggering table";
    #[cold]
    fn mismatch(detail: String) -> Box<types_error::PgError> {
        Box::new(
            elog::ereport(ERROR)
                .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(MSG)
                .errdetail(detail)
                .into_error(),
        )
    }
    let n = out.types.len();
    let mut values = vec![Datum::null(); n];
    let mut nulls = vec![true; n];
    let mut j = 0usize;
    let mut nincols = 0;
    let mut noutcols = 0;
    for i in 0..n {
        if out.dropped[i] {
            continue;
        }
        noutcols += 1;
        while j < rv.desc.types.len() {
            if rv.desc.dropped[j] {
                j += 1;
                continue;
            }
            nincols += 1;
            if out.types[i] != rv.desc.types[j]
                || (out.typmods[i] != rv.desc.typmods[j] && out.typmods[i] >= 0)
            {
                return Err(mismatch(format!(
                    "Returned type {} does not match expected type {} in column \"{}\" (position {}).",
                    format_type::format_type_with_typemod(rv.desc.types[j], rv.desc.typmods[j])?,
                    format_type::format_type_with_typemod(out.types[i], out.typmods[i])?,
                    out.names[i],
                    noutcols
                )));
            }
            values[i] = rv.values[j];
            nulls[i] = rv.nulls[j];
            j += 1;
            break;
        }
    }
    let extra = rv.desc.types[j..].iter().enumerate().filter(|&(k, _)| !rv.desc.dropped[j + k]).count();
    if extra > 0 || nincols != noutcols {
        return Err(mismatch(format!(
            "Number of returned columns ({}) does not match expected column count ({}).",
            nincols + extra,
            noutcols
        )));
    }
    Ok((values, nulls))
}

// plpgsql_exec_trigger (pl_exec.c).
fn plpgsql_exec_trigger(
    func: &PlFunction,
    trigdata: &types_trigger_call::TriggerData<'_, '_>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    use types_trigger::*;
    assert!(
        matches!(func.fn_is_trigger, FnTrigger::DmlTrigger),
        "plpgsql_exec_trigger on a non-trigger function"
    );
    let mut estate = Estate::new(func, func.fn_readonly, true);
    let _frame = crate::exec::FrameGuard::push_pl(&estate);
    estate.frame.text.set(Some("during initialization of execution state"));

    let rel = trigdata.tg_relation;
    let tupdesc = rel.rd_att.clone();
    let (desc, generated) = recdesc_from_tupdesc(&tupdesc);
    let natts = desc.types.len();
    let src_desc =
        Rc::new(tupdesc::CreateTupleDescCopy(estate.datum_mcx(), &tupdesc)?);
    // C makes empty expanded records for BOTH variables (pl_exec.c:966-984):
    // unsupplied tuples read as NULL, whole-record use is SQL NULL.
    let empty_rv = crate::exec::RecValue {
        desc: desc.clone(),
        values: vec![Datum::null(); natts],
        nulls: vec![true; natts],
        src_desc: Some(src_desc),
        empty: true,
    };
    let mut new_rv = empty_rv.clone();
    let mut old_rv = empty_rv;

    let ev = trigdata.tg_event;
    if !TRIGGER_FIRED_FOR_ROW(ev) {
        // Per-statement triggers don't use OLD/NEW variables.
    } else if TRIGGER_FIRED_BY_INSERT(ev) {
        bind_trigger_tuple(&estate, &mut new_rv, trigdata.tg_trigtuple, &tupdesc)?;
        new_rv.empty = false;
    } else if TRIGGER_FIRED_BY_UPDATE(ev) {
        bind_trigger_tuple(&estate, &mut new_rv, trigdata.tg_newtuple, &tupdesc)?;
        bind_trigger_tuple(&estate, &mut old_rv, trigdata.tg_trigtuple, &tupdesc)?;
        new_rv.empty = false;
        old_rv.empty = false;
        // BEFORE UPDATE: stored generated columns are not computed yet, so
        // NEW carries them as NULL (pl_exec.c:1005-1023).
        if ev & TRIGGER_EVENT_TIMINGMASK == TRIGGER_EVENT_BEFORE {
            for (i, g) in generated.iter().enumerate() {
                if *g {
                    new_rv.values[i] = Datum::null();
                    new_rv.nulls[i] = true;
                }
            }
        }
    } else if TRIGGER_FIRED_BY_DELETE(ev) {
        bind_trigger_tuple(&estate, &mut old_rv, trigdata.tg_trigtuple, &tupdesc)?;
        old_rv.empty = false;
    } else {
        panic!("unrecognized trigger action: not INSERT, DELETE, or UPDATE");
    }
    estate.datums[func.new_varno as usize] = crate::exec::DatumVal::Rec(Some(new_rv));
    estate.datums[func.old_varno as usize] = crate::exec::DatumVal::Rec(Some(old_rv));

    if trigdata.tg_trigger.tgoldtable.is_some() || trigdata.tg_trigger.tgnewtable.is_some() {
        let rc = spi::SPI_register_trigger_data(trigdata)?;
        assert_eq!(rc, spi::SPI_OK_TD_REGISTER, "SPI_register_trigger_data failed");
    }

    fulfill_trigger_promises(&mut estate, func, trigdata)?;

    estate.frame.text.set(Some("during function entry"));
    estate.set_var(func.found_varno, Datum::from_bool(false), false);
    estate.frame.text.set(None);

    let rc = match estate.exec_toplevel_block(&func.action) {
        Ok(rc) => rc,
        Err(e) => return Err(attach_exec_context(e, &estate)),
    };
    if rc != RC_RETURN {
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(types_error::ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT)
                .errmsg("control reached end of trigger procedure without RETURN")
                .errcontext_msg(format!("PL/pgSQL function {}", func.fn_signature))
                .into_error(),
        ));
    }

    estate.frame.text.set(Some("during function exit"));
    fcinfo.isnull = false;
    if estate.retisnull || !TRIGGER_FIRED_FOR_ROW(ev) {
        return Ok(Datum::null());
    }
    let rv = match estate.ret_rec.take() {
        Some(rv) => rv,
        None => {
            // Composite Datum returned by expression: deconstruct it.
            let retval = estate.retval;
            let (d, s, values, nulls) = match estate.deconstruct_composite(retval) {
                Ok(x) => x,
                Err(e) => return Err(attach_exec_context(e, &estate)),
            };
            crate::exec::RecValue { desc: d, values, nulls, src_desc: Some(s), empty: false }
        }
    };
    // build_attrmap_by_position + execute_attr_map_tuple: map the returned
    // row onto the relation rowtype (typmod-aware position walk).
    let (values, nulls) = match map_returned_row(&rv, &desc) {
        Ok(vn) => vn,
        Err(e) => return Err(attach_exec_context(e, &estate)),
    };

    let out_mcx = fcinfo.result_mcx();
    let tup = heaptuple::heap_form_tuple(out_mcx, &tupdesc, &values, &nulls)?;
    let (img, t_len) = (tup.header_ptr(), tup.t_len);
    core::mem::forget(tup);
    // SAFETY: img is a live heap-tuple image of t_len bytes in out_mcx,
    // leaked into the arena above.
    let htd = unsafe {
        types_tuple::HeapTupleData::from_raw_parts(
            img,
            t_len,
            types_tuple::ItemPointerData::invalid(),
            rel.rd_id,
        )
    };
    let boxed = mcx::alloc_in(out_mcx, htd)?;
    let p = mcx::leak_in(boxed) as *mut types_tuple::HeapTupleData<'_>;
    Ok(Datum::from_usize(p as usize))
}
