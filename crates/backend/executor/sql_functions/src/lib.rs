// executor/functions.c — SQL-language function execution (18.3 shape:
// funccache-backed SQLFunctionHashEntry, lazy per-query plansources, eslist
// state machine, lazy eval, SETOF materialize + value-per-call).
// DIVERGENCES: rows for the result statement always route through a
// tuplestore (C's DR_sqlfunction keeps scalar rows in the junkfilter slot);
// ShutdownSQLFunction rides rsinfo.srf_shutdown (planted on suspension,
// fired by the SRF node at ExecEnd/ReScan — the ShutdownExprContext moments);
// cleanup on error is eager instead of resowner-driven.
#![allow(non_snake_case)]

mod cache;
mod inline_fn;
mod retval;
mod srf_inline;

use std::rc::Rc;

use datum::Datum;
use elog::ereport;
use fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData};
use mcx::{bind, Mcx, McxOwned, MemoryContext, PgString, PgVec};
use types_core::catalog::VOIDOID;
use types_core::Oid;
use types_dest::CommandDest;
use types_error::{
    PgError, PgResult, SqlState, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_UNDEFINED_FUNCTION, ERROR,
};
use fmgr::rsinfo::{
    ExprDoneCond, SetFunctionReturnMode, SFRM_Materialize, SFRM_Materialize_Preferred,
    SFRM_Materialize_Random, SFRM_ValuePerCall,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::{Node, NodeList, NodeTag};
use types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
use types_portal::{
    CachedPlanHandle, ParamListHandle, QueryDescHandle, QueryEnvHandle, TuplestoreHandle,
};
use types_scan::sdir::ForwardScanDirection;
use types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_SKIP_TRIGGERS};
use types_tuple::TupleDescData;

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, PROCOID};

pub use retval::check_sql_stmt_retval;

pub(crate) const ANUM_PG_PROC_PRONAME: i32 = 2;
pub(crate) const ANUM_PG_PROC_PROLANG: i32 = 5;
pub(crate) const ANUM_PG_PROC_PROKIND: i32 = 10;
pub(crate) const ANUM_PG_PROC_PROISSTRICT: i32 = 13;
pub(crate) const ANUM_PG_PROC_PRORETSET: i32 = 14;
pub(crate) const ANUM_PG_PROC_PROVOLATILE: i32 = 15;
pub(crate) const ANUM_PG_PROC_PRORETTYPE: i32 = 19;
pub(crate) const ANUM_PG_PROC_PROARGTYPES: i32 = 20;
pub(crate) const ANUM_PG_PROC_PROARGMODES: i32 = 22;
pub(crate) const ANUM_PG_PROC_PROARGNAMES: i32 = 23;
pub(crate) const ANUM_PG_PROC_PROSRC: i32 = 26;
pub(crate) const ANUM_PG_PROC_PROSQLBODY: i32 = 28;

pub fn init_seams() {
    fmgr_core::register_sql_language_handler(fmgr_sql);
    fmgr_core::register_late_builtins(FUNCTIONS_BUILTINS);
    sql_functions_seams::sqlfunction_receive::set(sqlfunction_receive);
    inline_fn::init_seams();
    srf_inline::init_seams();
}

const fn vb(foid: Oid, name: &'static str, func: fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 1, strict: true, retset: false, func }
}

static FUNCTIONS_BUILTINS: &[FmgrBuiltin] = &[
    vb(2246, "fmgr_internal_validator", fc_fmgr_internal_validator),
    vb(2248, "fmgr_sql_validator", fc_fmgr_sql_validator),
];

#[cold]
pub(crate) fn efn(code: SqlState, msg: String) -> Box<PgError> {
    ereport(ERROR).errcode(code).errmsg(msg).into_error().into()
}

#[cold]
pub(crate) fn lookup_failed(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for function {fn_oid}")))
}

pub(crate) fn name_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: NameData attr from a live syscache tuple — 64 NUL-padded bytes.
    let bytes = unsafe { core::slice::from_raw_parts(p, 64) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    let s = core::str::from_utf8(&bytes[..len]).expect("proname is server-encoding text");
    PgString::from_str_in(s, mcx)
}

pub(crate) fn varlena_bytes<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
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
    detoast::detoast_attr(mcx, src)
}

pub(crate) fn varlena_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    let img = varlena_bytes(mcx, d)?;
    let s = core::str::from_utf8(&img[4..]).expect("text column is server-encoding text");
    PgString::from_str_in(s, mcx)
}

pub(crate) fn read_oidvector_attr<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, Oid>> {
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

pub(crate) fn is_polymorphic(typid: Oid) -> bool {
    use types_core::catalog::{
        ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID,
        ANYCOMPATIBLENONARRAYOID, ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID,
        ANYENUMOID, ANYMULTIRANGEOID, ANYNONARRAYOID, ANYOID, ANYRANGEOID,
    };
    matches!(
        typid,
        ANYOID
            | ANYELEMENTOID
            | ANYARRAYOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

pub(crate) fn clone_query<'mcx>(q: &Query<'mcx>) -> Query<'mcx> {
    const { assert!(!core::mem::needs_drop::<Query<'static>>()) };
    // SAFETY: bitwise duplicate of an arena node struct; interior refs stay
    // shared read-only source material.
    unsafe { core::ptr::read(q as *const Query<'mcx>) }
}

pub(crate) fn query_command_tag(ct: CmdType) -> types_core::CommandTag {
    match ct {
        CmdType::CMD_SELECT => types_portal::CMDTAG_SELECT,
        CmdType::CMD_INSERT => types_portal::CMDTAG_INSERT,
        CmdType::CMD_UPDATE => types_portal::CMDTAG_UPDATE,
        CmdType::CMD_DELETE => types_portal::CMDTAG_DELETE,
        CmdType::CMD_MERGE => types_portal::CMDTAG_MERGE,
        _ => types_portal::CMDTAG_UNKNOWN,
    }
}

// sql_compile_error_callback (functions.c:1894).
#[cold]
pub(crate) fn startup_error_context(e: Box<PgError>, fname: &str, src: &str) -> Box<PgError> {
    let mut err = transpose_position(*e, src);
    err.add_context_line(format!("SQL function \"{fname}\" during startup"));
    Box::new(err)
}

// sql_function_parse_error_callback (pg_proc.c:1000): a positioned error is
// transposed onto the original CREATE FUNCTION text (or demoted to an
// internal-query report); only position-less errors get the context line.
#[track_caller]
#[cold]
fn validator_error_context(e: Box<PgError>, fname: &str, src: &str) -> Box<PgError> {
    let mut err = *e;
    if !catalog_seams::function_parse_error_transpose::call(&mut err, src) {
        err.add_context_line(format!("SQL function \"{fname}\""));
    }
    Box::new(err)
}

// sql_exec_error_callback (functions.c:1928).
#[track_caller]
#[cold]
fn exec_error_context(
    e: Box<PgError>,
    fname: &str,
    src: &str,
    error_query_index: usize,
) -> Box<PgError> {
    let mut err = transpose_position(*e, src);
    if error_query_index > 0 {
        err.add_context_line(format!("SQL function \"{fname}\" statement {error_query_index}"));
    } else {
        err.add_context_line(format!("SQL function \"{fname}\" during startup"));
    }
    Box::new(err)
}

fn transpose_position(err: PgError, src: &str) -> PgError {
    if let Some(pos) = err.cursor_position() {
        if pos > 0 && err.internal_query().is_none() {
            return err
                .with_cursor_position(0)
                .with_internal_position(pos)
                .with_internal_query(src.to_string());
        }
    }
    err
}

// datumCopy (datum.c) for by-ref values leaving a tuplestore image.
fn datum_copy_out<'mcx>(mcx: Mcx<'mcx>, value: Datum, typlen: i16) -> PgResult<Datum> {
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    // SAFETY: by-ref datum into a live tuplestore image; size per its header.
    let src = unsafe {
        let size = match typlen {
            -1 => {
                let b0 = *p;
                if b0 == 0x01 {
                    2 + types_tuple::varatt::vartag_size(*p.add(1))
                } else if b0 & 0x01 != 0 {
                    (b0 as usize >> 1) & 0x7F
                } else {
                    (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
                }
            }
            -2 => {
                let mut n = 0usize;
                while *p.add(n) != 0 {
                    n += 1;
                }
                n + 1
            }
            l => {
                debug_assert!(l > 0);
                l as usize
            }
        };
        core::slice::from_raw_parts(p, size)
    };
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, src.len())?;
    out.extend_from_slice(src);
    let slice = mcx::vec_borrow_in(mcx, out)?;
    Ok(Datum::from_usize(slice.as_ptr() as usize))
}

fn check_body_utility_node(u: Node<'_>) -> PgResult<()> {
    match u.node_tag() {
        NodeTag::T_CopyStmt => {
            let c = u.as_copy_stmt().expect("tag-checked");
            if c.filename.is_none() {
                return Err(efn(
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                    "cannot COPY to/from client in an SQL function".into(),
                ));
            }
        }
        NodeTag::T_TransactionStmt => {
            let name = cmdtag::GetCommandTagName(utility_seams::create_command_tag::call(u));
            return Err(efn(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!("{name} is not allowed in an SQL function"),
            ));
        }
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExecStatus {
    Start,
    Run,
    Done,
}

struct ExecState<'mcx> {
    status: ExecStatus,
    sets_result: bool,
    lazy_eval: bool,
    stmt_idx: usize,
    qd: QueryDescHandle,
    snapshot: Option<snapmgr::Snapshot>,
    dest: Option<tcop_dest::DestReceiver<'mcx>>,
}

struct JunkState<'mcx> {
    clean_desc: Rc<TupleDescData<'mcx>>,
    // 1-based source resnos per output column; 0 = NULL (dropped column).
    clean_map: &'mcx [i16],
    slot: SlotData<'mcx>,
}

#[derive(Clone, Copy)]
struct EntryFacts {
    rettype: Oid,
    typlen: i16,
    typbyval: bool,
    returns_set: bool,
    returns_tuple: bool,
    readonly_func: bool,
    num_queries: usize,
}

struct SqlFcacheState<'mcx> {
    entry: Option<Rc<cache::SqlFnEntry>>,
    params_buf: PgVec<'mcx, ParamExternData>,
    params_h: ParamListHandle,
    tstore: TuplestoreHandle,
    row_store: TuplestoreHandle,
    junk: Option<JunkState<'mcx>>,
    jf_generation: i32,
    cplan: CachedPlanHandle,
    next_query_index: usize,
    error_query_index: usize,
    // std Vec justified: ExecState is droppy (Rc snapshot + dest receiver).
    eslist: Vec<ExecState<'mcx>>,
    lazy_eval_ok: bool,
    lazy_eval: bool,
    random_access: bool,
    active: bool,
}

bind!(SqlFcacheTy => SqlFcacheState<'mcx>);

struct SqlFcacheGuard(McxOwned<SqlFcacheTy>);

impl Drop for SqlFcacheGuard {
    fn drop(&mut self) {
        self.0.with_mut(|s| release_execution_state(s));
    }
}

pub fn shutdown_sql_srf(flinfo: &mut FmgrInfo) -> PgResult<()> {
    match flinfo.fn_extra_mut::<SqlFcacheGuard>() {
        Some(guard) => guard.0.with_mut(|s| shutdown_sql_function(s)),
        None => Ok(()),
    }
}

// ShutdownSQLFunction (functions.c:1967): clean teardown of a suspended
// lazy-eval execution. Drop stays the abort path (release_query_desc).
fn shutdown_sql_function(s: &mut SqlFcacheState<'_>) -> PgResult<()> {
    let readonly =
        s.entry.as_ref().map_or(true, |e| e.owned.with(|es| es.readonly_func));
    for i in 0..s.eslist.len() {
        if s.eslist[i].status != ExecStatus::Run {
            continue;
        }
        let mut pushed = false;
        if !readonly {
            let snap = s.eslist[i].snapshot.clone().expect("running es has a snapshot");
            snapmgr::PushActiveSnapshot(&snap)?;
            pushed = true;
        }
        let r = postquel_end(s, i);
        if pushed {
            let popped = snapmgr::PopActiveSnapshot();
            if r.is_ok() {
                popped?;
            }
        }
        r?;
    }
    release_execution_state(s);
    Ok(())
}

fn release_execution_state(s: &mut SqlFcacheState<'_>) {
    for es in s.eslist.drain(..) {
        if !es.qd.is_null() {
            execmain_seams::release_query_desc::call(es.qd);
        }
    }
    if !s.cplan.is_null() {
        plancache::ReleaseCachedPlan(s.cplan);
        s.cplan = CachedPlanHandle::default();
    }
    if !s.tstore.is_null() {
        tuplestore::hold::end(s.tstore);
        s.tstore = TuplestoreHandle::NULL;
    }
    if !s.row_store.is_null() {
        tuplestore::hold::end(s.row_store);
        s.row_store = TuplestoreHandle::NULL;
    }
    if !s.params_h.is_null() {
        types_portal::params::free(s.params_h);
        s.params_h = ParamListHandle::NULL;
    }
}

// DR_sqlfunction receiveSlot (functions.c:2644), tuplestore-backed for both
// the accumulate and keep-first legs.
fn sqlfunction_receive<'mcx>(
    state: &mut sql_functions_seams::SqlFunctionDestState<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if state.only_first && state.received {
        return Ok(true);
    }
    exectuples::slot_getallattrs(slot);
    state.values.clear();
    state.isnull.clear();
    let b = slot.base();
    for &src in state.clean_map.iter() {
        if src == 0 {
            state.values.push(Datum::null());
            state.isnull.push(true);
        } else {
            state.values.push(b.tts_values[(src - 1) as usize]);
            state.isnull.push(b.tts_isnull[(src - 1) as usize]);
        }
    }
    tuplestore::hold::putvalues(state.tstore, &state.clean_desc, &state.values, &state.isnull)?;
    state.received = true;
    Ok(true)
}

pub fn fmgr_sql(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("fmgr_sql: called without flinfo");
    // C's guard for runaway SQL-function recursion sits in the per-level
    // parse/plan/exec entries; checked here once per call instead.
    stack_depth::check_stack_depth()?;

    let (random_access, lazy_eval_ok) = if flinfo.fn_retset {
        let Some(rsi) = fcinfo.rsinfo_mut() else {
            return Err(set_context_error());
        };
        if rsi.allowedModes & SFRM_ValuePerCall == 0 || rsi.allowedModes & SFRM_Materialize == 0 {
            return Err(set_context_error());
        }
        (
            rsi.allowedModes & SFRM_Materialize_Random != 0,
            rsi.allowedModes & SFRM_Materialize_Preferred == 0,
        )
    } else {
        (false, true)
    };

    if flinfo.fn_extra_ref::<SqlFcacheGuard>().is_none() {
        let owned =
            McxOwned::<SqlFcacheTy>::try_new(MemoryContext::new("SQL function cache"), |mcx| {
                Ok(SqlFcacheState {
                    entry: None,
                    params_buf: PgVec::new_in(mcx),
                    params_h: ParamListHandle::NULL,
                    tstore: TuplestoreHandle::NULL,
                    row_store: TuplestoreHandle::NULL,
                    junk: None,
                    jf_generation: -1,
                    cplan: CachedPlanHandle::default(),
                    next_query_index: 0,
                    error_query_index: 0,
                    eslist: Vec::new(),
                    lazy_eval_ok: false,
                    lazy_eval: false,
                    random_access: false,
                    active: false,
                })
            })?;
        flinfo.set_fn_extra(SqlFcacheGuard(owned));
    }

    let fncollation = fcinfo.fncollation;
    let nargs = fcinfo.nargs();
    assert!(nargs <= cache::MAX_SQL_FN_ARGS, "fmgr_sql: >FUNC_MAX_ARGS arguments");
    let mut arg_vals = [datum::NullableDatum::null(); cache::MAX_SQL_FN_ARGS];
    for i in 0..nargs {
        arg_vals[i] = datum::NullableDatum { value: fcinfo.arg(i), isnull: fcinfo.argisnull(i) };
    }

    // init_sql_fcache (functions.c:535): reset error debris, resume a
    // suspended set-returning execution, or (re)bind entry and params.
    let resuming = {
        let guard = flinfo.fn_extra_mut::<SqlFcacheGuard>().expect("set above");
        guard.0.with_mut(|s| {
            if s.active {
                release_execution_state(s);
                s.active = false;
            }
            !s.eslist.is_empty()
        })
    };
    if !resuming {
        let expected_desc: Option<&TupleDescData<'_>> = fcinfo.rsinfo_mut().and_then(|rsi| {
            rsi.expectedDesc.map(|p| {
                // SAFETY: fmNodePtr contract — the executor armed
                // expectedDesc with a live tupdesc for this call.
                unsafe { p.cast::<TupleDescData<'_>>().as_ref() }
            })
        });
        let e = cache::cached_sql_function(flinfo, fncollation, expected_desc)?;
        let guard = flinfo.fn_extra_mut::<SqlFcacheGuard>().expect("set above");
        guard.0.with_mut_mcx(|mcx, s| {
            let changed = match &s.entry {
                Some(old) => !Rc::ptr_eq(old, &e),
                None => true,
            };
            if changed {
                s.junk = None;
                s.jf_generation = -1;
            }
            s.entry = Some(e.clone());
            e.owned.with(|es| -> PgResult<()> {
                assert_eq!(nargs, es.argtypes.len(), "fmgr_sql: argument count mismatch");
                if s.params_buf.len() != nargs {
                    if !s.params_h.is_null() {
                        types_portal::params::free(s.params_h);
                        s.params_h = ParamListHandle::NULL;
                    }
                    s.params_buf.clear();
                    s.params_buf.try_reserve_exact(nargs.max(1)).map_err(|_| mcx.oom(nargs))?;
                    for (i, &t) in es.argtypes.iter().enumerate() {
                        s.params_buf.push(ParamExternData {
                            value: arg_vals[i].value,
                            isnull: arg_vals[i].isnull,
                            pflags: PARAM_FLAG_CONST,
                            ptype: t,
                        });
                    }
                } else {
                    for i in 0..nargs {
                        s.params_buf[i].value = arg_vals[i].value;
                        s.params_buf[i].isnull = arg_vals[i].isnull;
                        s.params_buf[i].ptype = es.argtypes[i];
                    }
                }
                Ok(())
            })?;
            if !s.params_buf.is_empty() && s.params_h.is_null() {
                // C functions.c: paramLI->paramFetch = sql_fn_param_fetch —
                // a hooked PL-owned list (params.c BuildParamLogString bails
                // on it; auto_explain's Query Parameters suppression).
                // SAFETY: freed in release_execution_state before the buffer
                // is next reallocated.
                s.params_h = unsafe { types_portal::params::register_hooked(&s.params_buf) };
            }
            s.lazy_eval_ok = lazy_eval_ok;
            s.lazy_eval = false;
            s.next_query_index = 0;
            s.error_query_index = 0;
            Ok(())
        })?;
    }

    let guard = flinfo.fn_extra_ref::<SqlFcacheGuard>().expect("set above");
    let entry = guard.0.with(|s| s.entry.clone()).expect("entry bound above");
    let facts = entry.owned.with(|es| EntryFacts {
        rettype: es.rettype,
        typlen: es.typlen,
        typbyval: es.typbyval,
        returns_set: es.returns_set,
        returns_tuple: es.returns_tuple.get(),
        readonly_func: es.readonly_func,
        num_queries: es.num_queries,
    });

    let result_mcx = fcinfo.result_mcx();
    let guard = flinfo.fn_extra_mut::<SqlFcacheGuard>().expect("set above");
    let run = guard.0.with_mut_mcx(|mcx, state| {
        state.active = true;
        state.random_access = random_access;
        let r = execute_function(mcx, state, &entry, facts, result_mcx);
        if r.is_err() {
            release_execution_state(state);
        }
        state.active = false;
        r
    });
    let outcome = match run {
        Ok(o) => o,
        Err(e) => {
            let (fname, src) = entry.owned.with(|es| (es.fname.to_string(), es.src.to_string()));
            let eqi = guard.0.with(|s| s.error_query_index);
            return Err(exec_error_context(e, &fname, &src, eqi));
        }
    };

    match outcome {
        FnOutcome::Value(v, isnull) => {
            if isnull {
                Ok(fcinfo.return_null())
            } else {
                Ok(v)
            }
        }
        FnOutcome::LazyRow(v, isnull) => {
            let rsi = fcinfo.rsinfo_mut().expect("checked at entry");
            rsi.isDone = ExprDoneCond::ExprMultipleResult;
            rsi.srf_shutdown = Some(shutdown_sql_srf);
            if isnull {
                Ok(fcinfo.return_null())
            } else {
                Ok(v)
            }
        }
        FnOutcome::LazyEnd => {
            let rsi = fcinfo.rsinfo_mut().expect("checked at entry");
            rsi.isDone = ExprDoneCond::ExprEndResult;
            Ok(fcinfo.return_null())
        }
        FnOutcome::Materialized => {
            let (ts, set_desc) = guard.0.with_mut(|s| {
                let h = s.tstore;
                s.tstore = TuplestoreHandle::NULL;
                // C: rsi->setDesc from junkFilter->jf_cleanTupType; the
                // fn_extra fcache outlives the call, so a borrow suffices.
                let d = s
                    .junk
                    .as_ref()
                    .map(|j| core::ptr::NonNull::from(&*j.clean_desc).cast::<core::ffi::c_void>());
                (h, d)
            });
            let rsi = fcinfo.rsinfo_mut().expect("checked at entry");
            rsi.returnMode = SetFunctionReturnMode::Materialize;
            rsi.setDesc = set_desc;
            if !ts.is_null() {
                let store = tuplestore::hold::take(ts).expect("live materialize store");
                rsi.setResult = Some(Box::new(store));
            }
            Ok(fcinfo.return_null())
        }
    }
}

#[track_caller]
#[cold]
fn set_context_error() -> Box<PgError> {
    efn(
        ERRCODE_FEATURE_NOT_SUPPORTED,
        "set-valued function called in context that cannot accept a set".into(),
    )
}

enum FnOutcome {
    Value(Datum, bool),
    LazyRow(Datum, bool),
    LazyEnd,
    Materialized,
}

// fmgr_sql main loop (functions.c:1640-1890).
fn execute_function<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut SqlFcacheState<'mcx>,
    entry: &Rc<cache::SqlFnEntry>,
    facts: EntryFacts,
    result_mcx: Mcx<'_>,
) -> PgResult<FnOutcome> {
    let mut es_idx: Option<usize> = None;
    loop {
        if let Some(i) = state.eslist.iter().position(|e| e.status != ExecStatus::Done) {
            es_idx = Some(i);
            break;
        }
        if !init_execution_state(mcx, state, entry, facts)? {
            break;
        }
    }
    // returnsTuple settles once the last query is prepared.
    let returns_tuple = entry.owned.with(|s| s.returns_tuple.get());
    let facts = EntryFacts { returns_tuple, ..facts };

    let mut pushed_snapshot = false;
    let run = (|| -> PgResult<Option<usize>> {
        let mut cur = es_idx;
        while let Some(i) = cur {
            if state.eslist[i].status == ExecStatus::Start {
                if !facts.readonly_func {
                    xact::CommandCounterIncrement()?;
                    if !pushed_snapshot {
                        let snap = snapmgr::GetTransactionSnapshot()?;
                        snapmgr::PushActiveSnapshot(&snap)?;
                        pushed_snapshot = true;
                    } else {
                        snapmgr::UpdateActiveSnapshotCommandId()?;
                    }
                }
                postquel_start(mcx, state, i, facts)?;
            } else if !facts.readonly_func && !pushed_snapshot {
                let snap = state.eslist[i].snapshot.clone().expect("running es has a snapshot");
                snapmgr::PushActiveSnapshot(&snap)?;
                pushed_snapshot = true;
            }

            let completed = postquel_getnext(mcx, state, i)?;

            if completed || !facts.returns_set {
                postquel_end(state, i)?;
            }
            if state.eslist[i].status != ExecStatus::Done {
                return Ok(Some(i));
            }
            if i + 1 < state.eslist.len() {
                cur = Some(i + 1);
                continue;
            }
            cur = None;
            loop {
                if pushed_snapshot {
                    snapmgr::PopActiveSnapshot()?;
                    pushed_snapshot = false;
                }
                if !init_execution_state(mcx, state, entry, facts)? {
                    break;
                }
                if !state.eslist.is_empty() {
                    cur = Some(0);
                    break;
                }
            }
        }
        Ok(None)
    })();
    let suspended = match run {
        Ok(s) => s,
        Err(e) => {
            if pushed_snapshot {
                let _ = snapmgr::PopActiveSnapshot();
            }
            return Err(e);
        }
    };
    if pushed_snapshot {
        snapmgr::PopActiveSnapshot()?;
    }

    // returnsTuple settles only when the LAST query is prepared; multi-
    // statement bodies prepare lazily inside the run loop above, so the
    // pre-loop snapshot can be stale (record result took the scalar
    // copy-out path and dereferenced a by-value datum).
    let returns_tuple = entry.owned.with(|s| s.returns_tuple.get());
    let facts = EntryFacts { returns_tuple, ..facts };

    if facts.returns_set {
        if let Some(i) = suspended {
            assert!(state.eslist[i].lazy_eval, "suspension implies lazy eval");
            let (v, isnull) = take_single_result(mcx, state, facts, result_mcx)?;
            return Ok(FnOutcome::LazyRow(v, isnull));
        }
        finish_execution(state);
        if state.lazy_eval {
            return Ok(FnOutcome::LazyEnd);
        }
        assert!(
            !state.tstore.is_null() || facts.rettype == VOIDOID,
            "materialize mode without a tuplestore"
        );
        return Ok(FnOutcome::Materialized);
    }

    debug_assert!(suspended.is_none());
    let result = if state.junk.is_some() {
        take_single_result(mcx, state, facts, result_mcx)?
    } else {
        assert_eq!(facts.rettype, VOIDOID, "no junkfilter implies VOID result");
        (Datum::null(), true)
    };
    finish_execution(state);
    Ok(FnOutcome::Value(result.0, result.1))
}

fn finish_execution(state: &mut SqlFcacheState<'_>) {
    state.eslist.clear();
    if !state.cplan.is_null() {
        plancache::ReleaseCachedPlan(state.cplan);
        state.cplan = CachedPlanHandle::default();
    }
    if !state.params_h.is_null() {
        types_portal::params::free(state.params_h);
        state.params_h = ParamListHandle::NULL;
    }
}

// postquel_get_single_result (functions.c:1535) over the row tuplestore.
fn take_single_result<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut SqlFcacheState<'mcx>,
    facts: EntryFacts,
    result_mcx: Mcx<'_>,
) -> PgResult<(Datum, bool)> {
    let row_store = state.row_store;
    if row_store.is_null() {
        return Ok((Datum::null(), true));
    }
    let junk = state.junk.as_mut().expect("result extraction requires a junkfilter");
    let got = tuplestore::hold::with_store(row_store, |st| {
        st.gettupleslot(true, false, &mut junk.slot, mcx)
    })?;
    if !got {
        return Ok((Datum::null(), true));
    }
    let result = if facts.returns_tuple {
        (exectuples::exec_fetch_slot_heap_tuple_datum(&mut junk.slot, mcx, result_mcx)?, false)
    } else {
        let mut isnull = false;
        let v = exectuples::slot_getattr(&mut junk.slot, 1, &mut isnull);
        if isnull || facts.typbyval {
            (v, isnull)
        } else {
            (datum_copy_out(result_mcx, v, facts.typlen)?, false)
        }
    };
    exectuples::exec_clear_tuple(&mut junk.slot, mcx);
    tuplestore::hold::with_store(row_store, |st| st.clear());
    Ok(result)
}

// init_execution_state (functions.c:652).
fn init_execution_state<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut SqlFcacheState<'mcx>,
    entry: &Rc<cache::SqlFnEntry>,
    facts: EntryFacts,
) -> PgResult<bool> {
    if !state.cplan.is_null() {
        plancache::ReleaseCachedPlan(state.cplan);
        state.cplan = CachedPlanHandle::default();
    }
    for es in state.eslist.drain(..) {
        if !es.qd.is_null() {
            execmain_seams::release_query_desc::call(es.qd);
        }
    }

    let prepared = entry.owned.with(|s| s.plansources.borrow().len());
    if state.next_query_index >= prepared {
        if state.next_query_index >= facts.num_queries {
            return Ok(false);
        }
        state.error_query_index += 1;
        cache::prepare_next_query(entry)?;
    } else {
        state.error_query_index += 1;
    }
    let psrc = cache::query_plansource(entry, state.next_query_index);
    state.next_query_index += 1;

    state.cplan = plancache::GetCachedPlan(psrc, state.params_h, None, QueryEnvHandle::NULL)?;
    let stmt_list = plancache::CachedPlanStmtList(state.cplan);

    let mut lasttages: Option<usize> = None;
    for (idx, stmt) in stmt_list.iter().enumerate() {
        if let Some(u) = stmt.utilityStmt {
            check_body_utility_node(u)?;
        }
        if facts.readonly_func && !utility::CommandIsReadOnly(stmt) {
            let tag = match stmt.utilityStmt {
                Some(u) => utility_seams::create_command_tag::call(u),
                None => query_command_tag(stmt.commandType),
            };
            let name = cmdtag::GetCommandTagName(tag);
            return Err(efn(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!("{name} is not allowed in a non-volatile function"),
            ));
        }
        state.eslist.push(ExecState {
            status: ExecStatus::Start,
            sets_result: false,
            lazy_eval: false,
            stmt_idx: idx,
            qd: QueryDescHandle::NULL,
            snapshot: None,
            dest: None,
        });
        if stmt.canSetTag {
            lasttages = Some(idx);
        }
    }

    if state.next_query_index < facts.num_queries {
        return Ok(true);
    }

    let returns_tuple = entry.owned.with(|s| s.returns_tuple.get());

    if facts.rettype != VOIDOID
        && (state.junk.is_none()
            || state.jf_generation != plancache::CachedPlanGeneration(state.cplan))
    {
        build_junk_state(mcx, state, entry, psrc, returns_tuple)?;
        state.jf_generation = plancache::CachedPlanGeneration(state.cplan);
    }

    if facts.returns_set && !returns_tuple && lsyscache::typ::type_is_rowtype(facts.rettype)? {
        state.lazy_eval_ok = true;
    }

    if let Some(li) = lasttages {
        if state.junk.is_some() {
            let stmt = &stmt_list[li];
            state.eslist[li].sets_result = true;
            if state.lazy_eval_ok
                && stmt.commandType == CmdType::CMD_SELECT
                && !stmt.hasModifyingCTE
            {
                state.lazy_eval = true;
                state.eslist[li].lazy_eval = true;
            }
        }
    }
    Ok(true)
}

fn get_sql_fn_result_tlist(
    query_list: &'static [Query<'static>],
) -> Option<&'static NodeList<'static>> {
    let mut parse: Option<&'static Query<'static>> = None;
    for q in query_list {
        if q.canSetTag {
            parse = Some(q);
        }
    }
    match parse {
        Some(q) if q.commandType == CmdType::CMD_SELECT => Some(&q.targetList),
        Some(q)
            if matches!(
                q.commandType,
                CmdType::CMD_INSERT
                    | CmdType::CMD_UPDATE
                    | CmdType::CMD_DELETE
                    | CmdType::CMD_MERGE
            ) && !q.returningList.is_nil() =>
        {
            Some(&q.returningList)
        }
        _ => None,
    }
}

// ExecInitJunkFilter / ExecInitJunkFilterConversion legs of
// init_execution_state (functions.c:786-855).
fn build_junk_state<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut SqlFcacheState<'mcx>,
    entry: &Rc<cache::SqlFnEntry>,
    psrc: plancache::CachedPlanSourceHandle,
    returns_tuple: bool,
) -> PgResult<()> {
    let query_list = plancache::SourceQueryList(psrc);
    let tlist =
        get_sql_fn_result_tlist(query_list).expect("retval check guaranteed a result tlist");

    let rettupdesc: Option<Rc<TupleDescData<'static>>> = entry.owned.with(|s| {
        s.rettupdesc.clone().map(|d| {
            // SAFETY: the desc lives in the entry arena; the fcache junk
            // state also holds the entry Rc, so the entry outlives it.
            unsafe { core::mem::transmute::<Rc<TupleDescData<'_>>, Rc<TupleDescData<'static>>>(d) }
        })
    });

    let (clean_desc, clean_map): (Rc<TupleDescData<'mcx>>, PgVec<'mcx, i16>) =
        if returns_tuple && rettupdesc.is_some() {
            let rd = rettupdesc.expect("checked");
            let mut map: PgVec<'mcx, i16> = PgVec::new_in(mcx);
            map.try_reserve_exact(rd.natts as usize).map_err(|_| mcx.oom(rd.natts as usize))?;
            let mut nonjunk =
                tlist.iter().filter(|n| n.as_target_entry().is_some_and(|t| !t.resjunk));
            for a in rd.attrs.iter() {
                if a.attisdropped {
                    map.push(0);
                } else {
                    let tle = nonjunk
                        .next()
                        .and_then(|n| n.as_target_entry())
                        .expect("retval check matched tlist to rettupdesc");
                    map.push(tle.resno);
                }
            }
            let mut d = tupdesc::CreateTupleDescCopy(mcx, &rd)?;
            if d.tdtypeid == types_core::catalog::RECORDOID && d.tdtypmod < 0 {
                typcache_seams::assign_record_type_typmod::call(&mut d)?;
            }
            (Rc::new(d), map)
        } else {
            let src_desc = execscan::exec_clean_type_from_tl(mcx, tlist)?;
            let mut map: PgVec<'mcx, i16> = PgVec::new_in(mcx);
            let n = src_desc.natts as usize;
            map.try_reserve_exact(n).map_err(|_| mcx.oom(n))?;
            for tle_node in tlist.iter() {
                let tle = tle_node.as_target_entry().expect("tlist holds TargetEntries");
                if !tle.resjunk {
                    map.push(tle.resno);
                }
            }
            debug_assert_eq!(map.len(), n);
            let desc = if returns_tuple {
                let mut d = match Rc::try_unwrap(src_desc) {
                    Ok(d) => d,
                    Err(shared) => tupdesc::CreateTupleDescCopy(mcx, &shared)?,
                };
                if d.tdtypeid == types_core::catalog::RECORDOID && d.tdtypmod < 0 {
                    typcache_seams::assign_record_type_typmod::call(&mut d)?;
                }
                Rc::new(d)
            } else {
                src_desc
            };
            (desc, map)
        };

    let slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(clean_desc.clone()));
    state.junk =
        Some(JunkState { clean_desc, clean_map: mcx::vec_borrow_in(mcx, clean_map)?, slot });
    Ok(())
}

fn current_query_string(state: &SqlFcacheState<'_>) -> &'static str {
    let e = state.entry.as_ref().expect("bound");
    let psrc = e.owned.with(|s| s.plansources.borrow()[state.next_query_index - 1]);
    plancache::CachedPlanQueryString(psrc)
}

// postquel_start (functions.c:1275).
fn postquel_start<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut SqlFcacheState<'mcx>,
    i: usize,
    facts: EntryFacts,
) -> PgResult<()> {
    let stmt_list = plancache::CachedPlanStmtList(state.cplan);
    let stmt = &stmt_list[state.eslist[i].stmt_idx];
    let sets_result = state.eslist[i].sets_result;
    let lazy = state.eslist[i].lazy_eval;

    let dest: tcop_dest::DestReceiver<'mcx> = if sets_result {
        let materialize = facts.returns_set && !lazy;
        let target = if materialize {
            if state.tstore.is_null() {
                let work_mem = init_small::globals::work_mem();
                state.tstore = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(
                    state.random_access,
                    false,
                    work_mem,
                ));
            }
            state.tstore
        } else {
            if state.row_store.is_null() {
                let work_mem = init_small::globals::work_mem();
                state.row_store = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(
                    false, false, work_mem,
                ));
            } else {
                tuplestore::hold::with_store(state.row_store, |st| st.clear());
            }
            state.row_store
        };
        let junk = state.junk.as_ref().expect("setsResult requires junkfilter");
        let n = junk.clean_map.len();
        tcop_dest::DestReceiver::SqlFunction(sql_functions_seams::SqlFunctionDestState {
            tstore: target,
            clean_desc: junk.clean_desc.clone(),
            clean_map: junk.clean_map,
            only_first: !facts.returns_set,
            received: false,
            values: mcx::vec_with_capacity_in(mcx, n)?,
            isnull: mcx::vec_with_capacity_in(mcx, n)?,
        })
    } else {
        tcop_dest::CreateDestReceiver(CommandDest::None)
    };

    let query_string = current_query_string(state);
    let snap = snapmgr::ActiveSnapshotSet().then(snapmgr::GetActiveSnapshot);
    let qd = execmain_seams::create_query_desc::call(
        stmt,
        query_string,
        snap.clone(),
        None,
        dest.mydest(),
        state.params_h,
        QueryEnvHandle::NULL,
        0,
    )?;
    let es = &mut state.eslist[i];
    es.qd = qd;
    es.snapshot = snap;
    es.dest = Some(dest);
    if stmt.commandType != CmdType::CMD_UTILITY {
        let eflags = if lazy { EXEC_FLAG_SKIP_TRIGGERS } else { 0 };
        execmain_seams::executor_start::call(qd, eflags)?;
    }
    es.status = ExecStatus::Run;
    Ok(())
}

// postquel_getnext (functions.c:1399).
fn postquel_getnext<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut SqlFcacheState<'mcx>,
    i: usize,
) -> PgResult<bool> {
    let stmt_list = plancache::CachedPlanStmtList(state.cplan);
    let stmt = &stmt_list[state.eslist[i].stmt_idx];
    if stmt.commandType == CmdType::CMD_UTILITY {
        let query_string = current_query_string(state);
        let mut qc = types_portal::QueryCompletion::default();
        cmdtag::InitializeQueryCompletion(&mut qc);
        let params_h = state.params_h;
        let es = &mut state.eslist[i];
        let dest = es.dest.as_mut().expect("started es has a dest");
        utility_seams::process_utility::call(
            mcx,
            stmt,
            query_string,
            true,
            utility_seams::ProcessUtilityContext::PROCESS_UTILITY_QUERY,
            params_h,
            QueryEnvHandle::NULL,
            dest,
            Some(&mut qc),
        )?;
        return Ok(true);
    }
    let es = &mut state.eslist[i];
    let count: u64 = if es.lazy_eval { 1 } else { 0 };
    let qd = es.qd;
    let dest = es.dest.as_mut().expect("started es has a dest");
    execmain_seams::executor_run::call(qd, ForwardScanDirection, count, dest)?;
    Ok(count == 0 || execmain_seams::query_desc_es_processed::call(qd) == 0)
}

// postquel_end (functions.c:1440).
fn postquel_end(state: &mut SqlFcacheState<'_>, i: usize) -> PgResult<()> {
    let stmt_list = plancache::CachedPlanStmtList(state.cplan);
    let is_utility = stmt_list[state.eslist[i].stmt_idx].commandType == CmdType::CMD_UTILITY;
    let es = &mut state.eslist[i];
    es.status = ExecStatus::Done;
    let qd = es.qd;
    es.qd = QueryDescHandle::NULL;
    es.dest = None;
    es.snapshot = None;
    if !is_utility {
        let fin = execmain_seams::executor_finish::call(qd)
            .and_then(|_| execmain_seams::executor_end::call(qd));
        if let Err(e) = fin {
            execmain_seams::release_query_desc::call(qd);
            return Err(e);
        }
    }
    execmain_seams::free_query_desc::call(qd);
    Ok(())
}

fn read_prosrc_any<'mcx>(mcx: Mcx<'mcx>, funcoid: Oid) -> PgResult<PgString<'mcx>> {
    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcoid)))? else {
        return Err(lookup_failed(funcoid));
    };
    let (d, isnull) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!isnull, "null prosrc for function {funcoid}");
    let s = varlena_str(mcx, d)?;
    ReleaseSysCache(tup);
    Ok(s)
}

fn fc_fmgr_internal_validator(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let funcoid = fcinfo.arg(0).as_oid();
    // C ignores check_function_bodies here: the name won't appear later.
    let cx = MemoryContext::new("fmgr_internal_validator");
    let prosrc = read_prosrc_any(cx.mcx(), funcoid)?;
    if fmgr_core::fmgr_internal_function(&prosrc) == types_core::InvalidOid {
        return Err(efn(
            ERRCODE_UNDEFINED_FUNCTION,
            format!("there is no built-in function named \"{}\"", prosrc.as_str()),
        ));
    }
    Ok(Datum::null())
}

// fmgr_sql_validator (pg_proc.c:820). DIVERGENCE:
// CheckFunctionValidatorAccess is unported.
fn fc_fmgr_sql_validator(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    use types_core::catalog::RECORDOID;
    let funcoid = fcinfo.arg(0).as_oid();
    let cx = MemoryContext::new("fmgr_sql_validator");
    let mcx = cx.mcx();

    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(funcoid)))? else {
        return Err(lookup_failed(funcoid));
    };
    let (rettype_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRORETTYPE)?;
    let rettype = rettype_d.as_oid();
    let (prokind_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROKIND)?;
    let prokind = prokind_d.as_i8();
    let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
    let argtypes = read_oidvector_attr(mcx, argv)?;
    let (sqlbody_d, sqlbody_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSQLBODY)?;
    let (prosrc_d, prosrc_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!prosrc_null, "null prosrc for function {funcoid}");
    let prosrc = varlena_str(mcx, prosrc_d)?;
    let prosqlbody = if sqlbody_null { None } else { Some(varlena_str(mcx, sqlbody_d)?) };
    let (proname_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONAME)?;
    let proname = name_str(mcx, proname_d)?;
    let (names_d, names_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGNAMES)?;
    let (modes_d, modes_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGMODES)?;
    let argnames =
        cache::read_input_argnames(mcx, names_d, names_null, modes_d, modes_null, argtypes.len())?;
    ReleaseSysCache(tup);

    if lsyscache::typ::get_typtype(rettype)? == b'p' as i8
        && rettype != RECORDOID
        && rettype != VOIDOID
        && !is_polymorphic(rettype)
    {
        return Err(efn(
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            format!("SQL functions cannot return type {}", format_type::format_type_be(rettype)?),
        ));
    }
    let mut haspolyarg = false;
    for &t in argtypes.iter() {
        if lsyscache::typ::get_typtype(t)? == b'p' as i8 {
            if is_polymorphic(t) {
                haspolyarg = true;
            } else {
                return Err(efn(
                    ERRCODE_INVALID_FUNCTION_DEFINITION,
                    format!(
                        "SQL functions cannot have arguments of type {}",
                        format_type::format_type_be(t)?
                    ),
                ));
            }
        }
    }

    if guc_tables::vars::check_function_bodies.read() {
        let r = (|| -> PgResult<()> {
            let rettupdesc = if is_polymorphic(rettype) || haspolyarg {
                None
            } else {
                funcapi::get_func_result_type(mcx, funcoid)?.result_tuple_desc
            };
            if let Some(body) = prosqlbody.as_ref() {
                let queries = cache::sqlbody_queries(mcx, body)?;
                let n = queries.len();
                let mut last_list: Option<PgVec<'_, Query<'_>>> = None;
                for (qi, q) in queries.into_iter().enumerate() {
                    let list = if q.commandType == CmdType::CMD_UTILITY {
                        let mut v: PgVec<'_, Query<'_>> = mcx::vec_with_capacity_in(mcx, 1)?;
                        v.push(q);
                        v
                    } else {
                        rewrite_handler_seams::acquire_rewrite_locks::call(mcx, &q, true, false)?;
                        rewrite_handler_seams::query_rewrite::call(mcx, q)?
                    };
                    for lq in list.iter() {
                        cache::check_sql_fn_statement(lq)?;
                    }
                    if qi == n - 1 {
                        last_list = Some(list);
                    }
                }
                match last_list {
                    Some(mut last) => {
                        retval::check_sql_stmt_retval(
                            mcx,
                            &mut last,
                            rettype,
                            rettupdesc.as_ref(),
                            prokind,
                            false,
                        )?;
                    }
                    None if rettype == VOIDOID => {}
                    None => return Err(retval::retval_mismatch_final_stmt(rettype)),
                }
                return Ok(());
            }
            let raw_list = parser_seams::raw_parser::call(
                mcx,
                &prosrc,
                parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
            )?;
            if haspolyarg {
                return Ok(());
            }
            let mut name_refs: PgVec<'_, &str> = PgVec::new_in(mcx);
            name_refs.try_reserve_exact(argnames.len()).map_err(|_| mcx.oom(argnames.len()))?;
            for n in argnames.iter() {
                name_refs.push(n.as_str());
            }
            let n = raw_list.len();
            let mut last_list: Option<PgVec<'_, Query<'_>>> = None;
            for (i, raw) in raw_list.iter().enumerate() {
                let query = analyze_seams::parse_analyze_sql_fn::call(
                    mcx,
                    raw,
                    &prosrc,
                    proname.as_str(),
                    &argtypes,
                    &name_refs,
                    types_core::InvalidOid,
                    QueryEnvHandle::NULL,
                )?;
                let list = if query.commandType == CmdType::CMD_UTILITY {
                    let mut v: PgVec<'_, Query<'_>> = mcx::vec_with_capacity_in(mcx, 1)?;
                    v.push(query);
                    v
                } else {
                    rewrite_handler_seams::query_rewrite::call(mcx, query)?
                };
                for lq in list.iter() {
                    cache::check_sql_fn_statement(lq)?;
                }
                if i == n - 1 {
                    last_list = Some(list);
                }
            }
            match last_list {
                Some(mut last) => {
                    retval::check_sql_stmt_retval(
                        mcx,
                        &mut last,
                        rettype,
                        rettupdesc.as_ref(),
                        prokind,
                        false,
                    )?;
                }
                None if rettype == VOIDOID => {}
                None => return Err(retval::retval_mismatch_final_stmt(rettype)),
            }
            Ok(())
        })();
        r.map_err(|e| validator_error_context(e, proname.as_str(), prosrc.as_str()))?;
    }
    Ok(Datum::null())
}
