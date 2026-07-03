// pl_handler.c + pl_comp.c's plpgsql_compile / do_compile (phase-1 subset)
// + pl_exec.c's plpgsql_exec_function shell. Triggers, DO blocks, procedures
// and polymorphic signatures are named louds. GUC-backed compile options
// (plpgsql.variable_conflict, ...) read their C-source defaults; SET on the
// unregistered custom GUCs is loud at the GUC layer.
use std::collections::HashMap;
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
const ANUM_PG_PROC_PROKIND: i32 = 12;
const ANUM_PG_PROC_PRORETSET: i32 = 14;
const ANUM_PG_PROC_PROVOLATILE: i32 = 15;
const ANUM_PG_PROC_PRONARGS: i32 = 17;
const ANUM_PG_PROC_PRORETTYPE: i32 = 19;
const ANUM_PG_PROC_PROARGTYPES: i32 = 20;
const ANUM_PG_PROC_PROARGMODES: i32 = 22;
const ANUM_PG_PROC_PROARGNAMES: i32 = 23;
const ANUM_PG_PROC_PROSRC: i32 = 26;

const BOOLOID: Oid = 16;
const VOIDOID: Oid = 2278;
const RECORDOID: Oid = 2249;
const TRIGGEROID: Oid = 2279;
const EVENT_TRIGGEROID: Oid = 3838;
const TYPTYPE_PSEUDO: i8 = b'p' as i8;
const PROKIND_FUNCTION: i8 = b'f' as i8;
const PROVOLATILE_VOLATILE: i8 = b'v' as i8;
const PROARGMODE_IN: u8 = b'i';

fn is_polymorphic(t: Oid) -> bool {
    // pseudotypes.dat polymorphic set.
    matches!(
        t,
        2276 /* any */
            | 2277 /* anyarray */
            | 2283 /* anyelement */
            | 2776 /* anynonarray */
            | 3500 /* anyenum */
            | 3831 /* anyrange */
            | 4537 /* anycompatible */
            | 4538 /* anycompatiblearray */
            | 4539 /* anycompatiblenonarray */
            | 4540 /* anycompatiblerange */
            | 4642 /* anymultirange */
            | 4643 /* anycompatiblemultirange */
    )
}

struct FuncCacheEntry {
    func: Rc<PlFunction>,
    use_count: u32,
}

std::thread_local! {
    static FUNC_CACHE: core::cell::RefCell<HashMap<Oid, FuncCacheEntry>> =
        core::cell::RefCell::new(HashMap::new());
}

pub fn init_seams() {
    fmgr_core::register_plpgsql_handlers(
        plpgsql_call_handler,
        plpgsql_inline_handler,
        plpgsql_validator,
    );
}

// plpgsql_compile (pl_comp.c) with funccache.c's xmin/tid staleness rule.
fn plpgsql_compile(fn_oid: Oid, fn_collation: Oid, for_validator: bool) -> PgResult<Rc<PlFunction>> {
    let (cur_xmin, cur_tid) = proc_row_stamp(fn_oid)?;
    let cached = FUNC_CACHE.with(|c| {
        c.borrow().get(&fn_oid).map(|e| (e.func.clone(), e.use_count))
    });
    if let Some((func, _)) = cached {
        if func.fn_xmin == cur_xmin && func.fn_tid == cur_tid {
            return Ok(func);
        }
        FUNC_CACHE.with(|c| {
            if let Some(e) = c.borrow_mut().remove(&fn_oid) {
                crate::exec::free_function_plans(&e.func.expr_ids);
            }
        });
    }

    let func = Rc::new(do_compile(fn_oid, fn_collation, cur_xmin, cur_tid, for_validator)?);
    if !for_validator {
        FUNC_CACHE.with(|c| {
            c.borrow_mut().insert(fn_oid, FuncCacheEntry { func: func.clone(), use_count: 0 })
        });
    }
    Ok(func)
}

fn proc_row_stamp(fn_oid: Oid) -> PgResult<(u32, (u32, u16))> {
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
    ReleaseSysCache(tup);
    Ok((xmin, tid))
}

struct ProcInfo {
    proname: String,
    prosrc: String,
    argtypes: Vec<Oid>,
    argnames: Vec<String>,
    rettype: Oid,
    retset: bool,
    prokind: i8,
    readonly: bool,
}

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
    let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
    let argtypes_pg = read_oidvector_attr(mcx, argv)?;
    let (_, modes_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGMODES)?;
    if !modes_null {
        panic!("plpgsql_compile: OUT/INOUT/VARIADIC/TABLE parameters unported (function {fn_oid})");
    }
    let (prosrc_d, prosrc_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!prosrc_null, "null prosrc for function {fn_oid}");
    let prosrc = varlena_str(mcx, prosrc_d)?;
    let (proname_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONAME)?;
    let proname = name_str(mcx, proname_d)?;
    let (argnames_d, argnames_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGNAMES)?;
    let argnames = read_argnames_attr(mcx, argnames_d, argnames_null, nargs)?;
    let info = ProcInfo {
        proname: proname.as_str().to_string(),
        prosrc: prosrc.as_str().to_string(),
        argtypes: argtypes_pg.iter().copied().collect(),
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
fn do_compile(
    fn_oid: Oid,
    fn_collation: Oid,
    fn_xmin: u32,
    fn_tid: (u32, u16),
    for_validator: bool,
) -> PgResult<PlFunction> {
    let proc = read_proc_row(fn_oid)?;

    if proc.prokind != PROKIND_FUNCTION {
        panic!("plpgsql_compile: procedures (CALL) unported (function {fn_oid})");
    }
    if proc.rettype == TRIGGEROID || proc.rettype == EVENT_TRIGGEROID {
        panic!("plpgsql_compile: trigger functions unported (function {fn_oid})");
    }
    if is_polymorphic(proc.rettype) || proc.argtypes.iter().any(|&t| is_polymorphic(t)) {
        panic!("plpgsql_compile: polymorphic signatures unported (function {fn_oid})");
    }

    let mut comp = CompState::new();
    // Outermost level: named after the function; holds params and FOUND.
    comp.ns_push_label(Some(&proc.proname), crate::gram::LABEL_BLOCK);

    let mut fn_argvarnos = Vec::with_capacity(proc.argtypes.len());
    for (i, &argtypeid) in proc.argtypes.iter().enumerate() {
        let buf = format!("${}", i + 1);
        let argdtype = CompState::build_datatype(argtypeid, -1, fn_collation)?;
        if argdtype.ttype == TypeKind::Pseudo {
            return Err(crate::exec::exec_err(
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "PL/pgSQL functions cannot accept type {}",
                    format_type::format_type_be(argtypeid)?
                ),
            ));
        }
        if lsyscache::typ::type_is_rowtype(argtypeid)? || argtypeid == RECORDOID {
            panic!(
                "plpgsql_compile: composite/record arguments unported \
                 (function {fn_oid}, arg {})",
                i + 1
            );
        }
        let argname = &proc.argnames[i];
        let refname = if !argname.is_empty() { argname.as_str() } else { buf.as_str() };
        let dno = comp.build_variable(refname, 0, argdtype, false)?;
        fn_argvarnos.push(dno);
        add_parameter_name(&mut comp, dno, &buf)?;
        if !argname.is_empty() {
            add_parameter_name(&mut comp, dno, argname)?;
        }
    }

    // Return type checks.
    let rettypeid = proc.rettype;
    let rettyptype = lsyscache::typ::get_typtype(rettypeid)?;
    if rettyptype == TYPTYPE_PSEUDO && rettypeid != VOIDOID && rettypeid != RECORDOID {
        return Err(crate::exec::exec_err(
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "PL/pgSQL functions cannot return type {}",
                format_type::format_type_be(rettypeid)?
            ),
        ));
    }
    let fn_retistuple = lsyscache::typ::type_is_rowtype(rettypeid)?;
    if fn_retistuple || rettypeid == RECORDOID {
        panic!("plpgsql_compile: composite/record return types unported (function {fn_oid})");
    }
    if proc.retset {
        panic!("plpgsql_compile: SETOF return unported (function {fn_oid})");
    }
    let fn_retisdomain = rettyptype == b'd' as i8;
    let (fn_rettyplen, fn_retbyval) = lsyscache::typ::get_typlenbyval(rettypeid)?;

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
        scratch: scan_cx.mcx(),
    };
    let parse_result = parser.parse_function_body();
    let latest_line = parser.sc.latest_lineno();
    let action =
        parse_result.map_err(|e| attach_compile_context(e, &proc.proname, latest_line))?;

    let fn_signature = format_signature(&proc.proname, &proc.argtypes)?;
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
        fn_nargs: proc.argtypes.len() as i16,
        fn_argvarnos,
        found_varno,
        datums: std::mem::take(&mut comp.datums),
        ns: std::mem::take(&mut comp.ns),
        action,
        resolve_option: comp.resolve_option,
        print_strict_params: comp.print_strict_params,
        nstatements: comp.nstatements,
        expr_ids: std::mem::take(&mut comp.expr_ids),
    })
}

#[cold]
fn attach_compile_context(
    mut e: Box<types_error::PgError>,
    fname: &str,
    line: i32,
) -> Box<types_error::PgError> {
    // plpgsql_compile_error_callback.
    if e.context.is_none() {
        e.context = Some(format!(
            "compilation of PL/pgSQL function \"{fname}\" near line {line}"
        ));
    }
    e
}

// add_parameter_name (pl_comp.c).
fn add_parameter_name(comp: &mut CompState, dno: Dno, name: &str) -> PgResult<()> {
    if comp.ns_lookup(comp.ns_top, true, name, None, None).is_some() {
        return Err(crate::exec::exec_err(
            types_error::ERRCODE_DUPLICATE_OBJECT,
            format!("parameter name \"{name}\" used more than once"),
        ));
    }
    comp.ns_additem(NsType::Var, dno, name);
    Ok(())
}

// format_procedure minus schema qualification (createfn precedent).
fn format_signature(name: &str, argtypes: &[Oid]) -> PgResult<String> {
    let mut s = String::from(name);
    s.push('(');
    for (i, &t) in argtypes.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format_type::format_type_be(t)?);
    }
    s.push(')');
    Ok(s)
}

// plpgsql_call_handler (pl_handler.c), non-trigger arm.
fn plpgsql_call_handler(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let fn_oid = flinfo.as_ref().map(|f| f.fn_oid).expect("plpgsql_call_handler needs flinfo");

    let nonatomic = false; // procedures/DO are unported; function calls are atomic

    let rc = spi::SPI_connect_ext(if nonatomic { spi::SPI_OPT_NONATOMIC } else { 0 })?;
    assert_eq!(rc, spi::SPI_OK_CONNECT, "SPI_connect failed");

    let outcome = (|| -> PgResult<Datum> {
        let func = plpgsql_compile(fn_oid, fcinfo.fncollation, false)?;
        FUNC_CACHE.with(|c| {
            if let Some(e) = c.borrow_mut().get_mut(&fn_oid) {
                e.use_count += 1;
            }
        });
        let r = plpgsql_exec_function(&func, fcinfo);
        FUNC_CACHE.with(|c| {
            if let Some(e) = c.borrow_mut().get_mut(&fn_oid) {
                e.use_count -= 1;
            }
        });
        r
    })();

    let result = outcome?;
    let rc = spi::SPI_finish()?;
    assert_eq!(rc, spi::SPI_OK_FINISH, "SPI_finish failed");
    Ok(result)
}

fn plpgsql_inline_handler(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    panic!("plpgsql_inline_handler (pl_handler.c): DO blocks unported — unit backend-pl-plpgsql-handler");
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
    if is_polymorphic(info.rettype) || info.argtypes.iter().any(|&t| is_polymorphic(t)) {
        panic!("plpgsql_validator: polymorphic signatures unported (function {funcoid})");
    }
    if info.rettype == TRIGGEROID || info.rettype == EVENT_TRIGGEROID {
        panic!("plpgsql_validator: trigger functions unported (function {funcoid})");
    }

    if guc_check_function_bodies() {
        let rc = spi::SPI_connect_ext(0)?;
        assert_eq!(rc, spi::SPI_OK_CONNECT, "SPI_connect failed");
        let r = plpgsql_compile(funcoid, types_core::InvalidOid, true);
        let _ = spi::SPI_finish()?;
        r?;
    }
    Ok(Datum::null())
}

fn guc_check_function_bodies() -> bool {
    guc_tables::backing::check_function_bodies()
}

// plpgsql_exec_function (pl_exec.c), scalar non-SETOF arm.
fn plpgsql_exec_function(
    func: &PlFunction,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let mut estate = Estate::new(func, func.fn_readonly, true);

    // Store call arguments into the argument variables.
    estate.err_text = Some("while storing call arguments into local variables");
    for (i, &dno) in func.fn_argvarnos.iter().enumerate() {
        let arg = &fcinfo.args[i];
        let ty = match &func.datums[dno as usize] {
            PlDatum::Var(v) => &v.datatype,
            _ => panic!("plpgsql: argument datum is not a Var"),
        };
        // Argument datums live in the caller's context for the call's
        // duration; no copy (C behaves identically for IN args).
        let _ = ty;
        estate.set_var(dno, arg.value, arg.isnull);
    }
    estate.err_text = None;

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

    // Cast the return value to the function's return type and copy it out
    // of SPI/estate memory (it must survive SPI_finish and estate drop).
    estate.err_text = Some("while casting return value to function's return type");
    let mut isnull = estate.retisnull;
    let mut retval = estate.retval;
    if !isnull || func.fn_rettype != VOIDOID {
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

#[cold]
fn attach_exec_context(mut e: Box<types_error::PgError>, estate: &Estate<'_>) -> Box<types_error::PgError> {
    let sig = &estate.func.fn_signature;
    let line = if let Some(t) = estate.err_text {
        match estate.err_stmt {
            Some((lineno, _)) if lineno > 0 => {
                format!("PL/pgSQL function {sig} line {lineno} {t}")
            }
            _ => format!("PL/pgSQL function {sig} {t}"),
        }
    } else if let Some((lineno, typename)) = estate.err_stmt {
        if lineno > 0 {
            format!("PL/pgSQL function {sig} line {lineno} at {typename}")
        } else {
            format!("PL/pgSQL function {sig}")
        }
    } else {
        format!("PL/pgSQL function {sig}")
    };
    match e.context.take() {
        Some(prev) => e.context = Some(format!("{prev}\n{line}")),
        None => e.context = Some(line),
    }
    e.plpgsql_context_attached = true;
    e
}
