// executor/functions.c — SQL-language function execution.
// Scope: scalar non-SETOF functions; SETOF, composite/RECORD results,
// polymorphic signatures, prosqlbody and named-parameter references are loud.
// DIVERGENCE: the per-backend funccache.c hash (C 18.3) is replaced by a
// per-FmgrInfo fn_extra cache; plans are plancache-backed so invalidation
// replans, but the compiled body is rebuilt per FmgrInfo, as in pre-18 C.
#![allow(non_snake_case)]

mod retval;

use datum::Datum;
use elog::ereport;
use fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData};
use mcx::{bind, Mcx, McxOwned, MemoryContext, PgString, PgVec};
use types_core::catalog::VOIDOID;
use types_core::Oid;
use types_dest::CommandDest;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_UNDEFINED_FUNCTION, ERROR,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::NodeTag;
use types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
use types_portal::{ParamListHandle, QueryEnvHandle, TuplestoreHandle, CURSOR_OPT_PARALLEL_OK};
use types_scan::sdir::ForwardScanDirection;
use types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_SKIP_TRIGGERS};

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, PROCOID};

pub use retval::check_sql_stmt_retval;

const ANUM_PG_PROC_PROLANG: i32 = 5;
const ANUM_PG_PROC_PRORETSET: i32 = 14;
const ANUM_PG_PROC_PROVOLATILE: i32 = 15;
const ANUM_PG_PROC_PRONARGS: i32 = 17;
const ANUM_PG_PROC_PRORETTYPE: i32 = 19;
const ANUM_PG_PROC_PROARGTYPES: i32 = 20;
const ANUM_PG_PROC_PROARGMODES: i32 = 22;
const ANUM_PG_PROC_PROSRC: i32 = 26;
const ANUM_PG_PROC_PROSQLBODY: i32 = 28;

pub fn init_seams() {
    fmgr_core::register_sql_language_handler(fmgr_sql);
    fmgr_core::register_late_builtins(FUNCTIONS_BUILTINS);
}

const fn vb(foid: Oid, name: &'static str, func: fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 1, strict: true, retset: false, func }
}

static FUNCTIONS_BUILTINS: &[FmgrBuiltin] = &[
    vb(2246, "fmgr_internal_validator", fc_fmgr_internal_validator),
    vb(2248, "fmgr_sql_validator", fc_fmgr_sql_validator),
];

struct SqlFcacheState<'mcx> {
    sources: PgVec<'mcx, plancache::CachedPlanSourceHandle>,
    argtypes: PgVec<'mcx, Oid>,
    params_buf: PgVec<'mcx, ParamExternData>,
    rettype: Oid,
    typlen: i16,
    typbyval: bool,
    readonly: bool,
    returns_void: bool,
    tstore: TuplestoreHandle,
    slot: Option<SlotData<'mcx>>,
}

bind!(SqlFcacheTy => SqlFcacheState<'mcx>);

// Guard owner for the plancache + tuplestore handles held by the cache
// (RAII concentrated in the owner outside the arenas).
struct SqlFcacheGuard(McxOwned<SqlFcacheTy>);

impl Drop for SqlFcacheGuard {
    fn drop(&mut self) {
        self.0.with(|s| {
            for &h in s.sources.iter() {
                plancache::DropCachedPlan(h);
            }
            if !s.tstore.is_null() {
                tuplestore::hold::end(s.tstore);
            }
        });
    }
}

#[cold]
fn efn(code: types_error::SqlState, msg: String) -> Box<PgError> {
    ereport(ERROR).errcode(code).errmsg(msg).into_error().into()
}

#[cold]
fn lookup_failed(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for function {fn_oid}")))
}

struct ProcRow<'mcx> {
    prosrc: PgString<'mcx>,
    argtypes: PgVec<'mcx, Oid>,
    rettype: Oid,
    provolatile: i8,
    proretset: bool,
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

fn read_proc_row<'mcx>(mcx: Mcx<'mcx>, fn_oid: Oid) -> PgResult<ProcRow<'mcx>> {
    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(fn_oid)))? else {
        return Err(lookup_failed(fn_oid));
    };
    let (prolang, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROLANG)?;
    assert_eq!(
        prolang.as_oid(),
        fmgr_core::SQL_LANGUAGE_ID,
        "fmgr_sql: not a SQL-language function"
    );
    let (rettype_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRORETTYPE)?;
    let rettype = rettype_d.as_oid();
    let (provolatile, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROVOLATILE)?;
    let (proretset, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRORETSET)?;
    let (pronargs, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONARGS)?;
    let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
    let argtypes = read_oidvector_attr(mcx, argv)?;
    assert_eq!(argtypes.len(), pronargs.as_i16() as usize);
    if is_polymorphic(rettype) || argtypes.iter().any(|&t| is_polymorphic(t)) {
        panic!("fmgr_sql: polymorphic SQL function signatures unported (function {fn_oid})");
    }
    let (_, modes_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGMODES)?;
    if !modes_null {
        panic!("fmgr_sql: OUT/INOUT/VARIADIC/TABLE parameters unported (function {fn_oid})");
    }
    let (_, sqlbody_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSQLBODY)?;
    if !sqlbody_null {
        panic!("fmgr_sql: prosqlbody (BEGIN ATOMIC bodies) unported (function {fn_oid})");
    }
    let (prosrc_d, prosrc_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!prosrc_null, "null prosrc for function {fn_oid}");
    let prosrc = varlena_str(mcx, prosrc_d)?;
    ReleaseSysCache(tup);
    Ok(ProcRow {
        prosrc,
        argtypes,
        rettype,
        provolatile: provolatile.as_i8(),
        proretset: proretset.as_bool(),
    })
}

fn is_polymorphic(typid: Oid) -> bool {
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

fn analyze_and_rewrite(
    qmcx: Mcx<'static>,
    raw: &types_nodes::rawnodes::RawStmt<'static>,
    src: &str,
    argtypes: &[Oid],
) -> PgResult<PgVec<'static, Query<'static>>> {
    let query = analyze_seams::parse_analyze_fixedparams::call(
        qmcx,
        raw,
        src,
        argtypes,
        QueryEnvHandle::NULL,
    )?;
    if query.commandType == CmdType::CMD_UTILITY {
        let mut v = PgVec::new_in(qmcx);
        v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
        v.push(query);
        Ok(v)
    } else {
        rewrite_handler_seams::query_rewrite::call(qmcx, query)
    }
}

// C init_sql_fcache + prepare_next_query, eager: the whole body is analyzed
// and completed into plancache sources up front. DIVERGENCE from C 18.3's
// lazy per-statement prepare — intra-body DDL whose later statements depend
// on it fails at analyze time instead of working.
fn build_sources<'mcx>(
    mcx: Mcx<'mcx>,
    prosrc: &str,
    argtypes: &[Oid],
    rettype: Oid,
) -> PgResult<PgVec<'mcx, plancache::CachedPlanSourceHandle>> {
    let scratch = MemoryContext::new("fmgr_sql parse");
    let raw_list = parser_seams::raw_parser::call(
        scratch.mcx(),
        prosrc,
        parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
    )?;
    let n = raw_list.len();
    let mut sources: PgVec<'mcx, plancache::CachedPlanSourceHandle> = PgVec::new_in(mcx);
    sources.try_reserve_exact(n.max(1)).map_err(|_| mcx.oom(n))?;
    let outcome = (|| -> PgResult<()> {
        for (i, raw) in raw_list.iter().enumerate() {
            let stmt = raw.stmt.expect("RawStmt has a stmt");
            let tag = utility_seams::create_command_tag::call(stmt);
            let psrc = plancache::CreateCachedPlan(Some(raw), prosrc, tag)?;
            sources.push(psrc);
            let qmcx = plancache::SourceQueryMcx(psrc);
            let src = plancache::CachedPlanQueryString(psrc);
            let reparsed = parser_seams::raw_parser::call(
                qmcx,
                src,
                parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
            )?;
            let raw2 = reparsed.get(i).expect("re-parse reproduces the statement");
            let mut query_list = analyze_and_rewrite(qmcx, raw2, src, argtypes)?;
            for q in query_list.iter() {
                check_body_utility_query(q)?;
            }
            if i == n - 1 {
                retval::check_sql_stmt_retval(qmcx, &mut query_list, rettype)?;
            }
            plancache::CompleteCachedPlan(psrc, query_list, argtypes, CURSOR_OPT_PARALLEL_OK, false)?;
        }
        Ok(())
    })();
    if let Err(e) = outcome {
        for psrc in sources.iter() {
            plancache::DropCachedPlan(*psrc);
        }
        return Err(e);
    }
    if n == 0 && rettype != VOIDOID {
        return Err(retval::retval_mismatch_final_stmt(rettype));
    }
    Ok(sources)
}


fn copy_result_desc<'mcx>(
    mcx: Mcx<'mcx>,
    src: &types_tuple::TupleDescData<'_>,
) -> PgResult<std::rc::Rc<types_tuple::TupleDescData<'mcx>>> {
    let n = src.attrs.len();
    let mut attrs: PgVec<'mcx, types_tuple::FormData_pg_attribute> = PgVec::new_in(mcx);
    attrs.try_reserve_exact(n).map_err(|_| mcx.oom(n))?;
    for a in src.attrs.iter() {
        attrs.push(*a);
    }
    let mut compact: PgVec<'mcx, types_tuple::CompactAttribute> = PgVec::new_in(mcx);
    compact.try_reserve_exact(n).map_err(|_| mcx.oom(n))?;
    for c in src.compact_attrs.iter() {
        compact.push(c.clone());
    }
    Ok(std::rc::Rc::new(types_tuple::TupleDescData {
        natts: src.natts,
        tdtypeid: src.tdtypeid,
        tdtypmod: src.tdtypmod,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }))
}

fn check_body_utility_query(q: &Query<'_>) -> PgResult<()> {
    if q.commandType != CmdType::CMD_UTILITY {
        return Ok(());
    }
    let Some(u) = q.utilityStmt else { return Ok(()) };
    check_body_utility_node(u)
}

fn check_body_utility_node(u: types_nodes::Node<'_>) -> PgResult<()> {
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

fn build_fcache(fn_oid: Oid, fn_retset: bool) -> PgResult<SqlFcacheGuard> {
    let owned = McxOwned::<SqlFcacheTy>::try_new(MemoryContext::new("SQL function cache"), |mcx| {
        let row = read_proc_row(mcx, fn_oid)?;
        if row.proretset || fn_retset {
            panic!("fmgr_sql: SETOF SQL functions unported (function {fn_oid})");
        }
        let sources = build_sources(mcx, &row.prosrc, &row.argtypes, row.rettype)?;
        let (typlen, typbyval) = if row.rettype == VOIDOID {
            (4, true)
        } else {
            lsyscache::typ::get_typlenbyval(row.rettype)?
        };
        let nargs = row.argtypes.len();
        let mut params_buf = PgVec::new_in(mcx);
        params_buf.try_reserve_exact(nargs.max(1)).map_err(|_| mcx.oom(nargs))?;
        for &t in row.argtypes.iter() {
            params_buf.push(ParamExternData {
                value: Datum::null(),
                isnull: true,
                pflags: PARAM_FLAG_CONST,
                ptype: t,
            });
        }
        Ok(SqlFcacheState {
            sources,
            argtypes: row.argtypes,
            params_buf,
            rettype: row.rettype,
            typlen,
            typbyval,
            readonly: row.provolatile != b'v' as i8,
            returns_void: row.rettype == VOIDOID,
            tstore: TuplestoreHandle::NULL,
            slot: None,
        })
    })?;
    Ok(SqlFcacheGuard(owned))
}

// datumCopy (datum.c) for values leaving the tuplestore image: fixed, cstring
// and all varlena header forms (toast pointers copied as-is, like C).
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

const MAX_SQL_FN_ARGS: usize = 16;

pub fn fmgr_sql(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("fmgr_sql: called without flinfo");
    if flinfo.fn_retset {
        panic!("fmgr_sql: SETOF SQL functions unported (function {})", flinfo.fn_oid);
    }
    if !flinfo.has_fn_extra() {
        let guard = build_fcache(flinfo.fn_oid, flinfo.fn_retset)?;
        flinfo.set_fn_extra(guard);
    }
    let nargs = fcinfo.nargs();
    assert!(nargs <= MAX_SQL_FN_ARGS, "fmgr_sql: >{MAX_SQL_FN_ARGS} arguments unported");
    let mut arg_vals = [datum::NullableDatum::null(); MAX_SQL_FN_ARGS];
    for i in 0..nargs {
        arg_vals[i] = datum::NullableDatum { value: fcinfo.arg(i), isnull: fcinfo.argisnull(i) };
    }
    let typbyval = flinfo
        .fn_extra_ref::<SqlFcacheGuard>()
        .expect("set above")
        .0
        .with(|s| s.typbyval);
    let result_mcx = (!typbyval).then(|| fcinfo.result_mcx());
    let guard = flinfo.fn_extra_mut::<SqlFcacheGuard>().expect("set above");
    let (value, isnull) = guard.0.with_mut_mcx(|mcx, state| {
        assert_eq!(nargs, state.argtypes.len(), "fmgr_sql: argument count mismatch");
        for i in 0..nargs {
            state.params_buf[i].value = arg_vals[i].value;
            state.params_buf[i].isnull = arg_vals[i].isnull;
        }
        execute_body(mcx, state, result_mcx)
    })?;
    if isnull {
        return Ok(fcinfo.return_null());
    }
    Ok(value)
}

fn execute_body<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut SqlFcacheState<'mcx>,
    result_mcx: Option<Mcx<'_>>,
) -> PgResult<(Datum, bool)> {
    let params_h = if state.params_buf.is_empty() {
        ParamListHandle::NULL
    } else {
        // SAFETY: freed below, before the buffer is next mutated.
        unsafe { types_portal::params::register(&state.params_buf) }
    };
    if state.tstore.is_null() && !state.returns_void {
        let work_mem = init_small::globals::work_mem();
        state.tstore =
            tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(false, false, work_mem));
    }
    let mut pushed = false;
    let nsources = state.sources.len();
    let outcome = (|| -> PgResult<()> {
        for (si, &psrc) in state.sources.iter().enumerate() {
            let is_last_source = si == nsources - 1;
            let cplan = plancache::GetCachedPlan(psrc, params_h, None, QueryEnvHandle::NULL)?;
            let run = run_source(mcx, state, psrc, cplan, params_h, is_last_source, &mut pushed);
            plancache::ReleaseCachedPlan(cplan);
            run?;
            // C pops at original-query boundaries so each list gets a fresh snap.
            if pushed {
                snapmgr::PopActiveSnapshot()?;
                pushed = false;
            }
        }
        Ok(())
    })();
    if pushed {
        let popped = snapmgr::PopActiveSnapshot();
        if outcome.is_ok() {
            popped?;
        }
    }
    if !params_h.is_null() {
        types_portal::params::free(params_h);
    }
    outcome?;
    if state.returns_void {
        return Ok((Datum::null(), true));
    }
    if state.slot.is_none() {
        let last = *state.sources.last().expect("nonempty body");
        let desc = plancache::CachedPlanResultDesc(last)
            .expect("retval check guaranteed a result tupdesc");
        // C CreateTupleDescCopy into the fcache context: the source's desc
        // storage dies with the plancache entry; the cached slot must not
        // outlive-borrow it.
        let desc = copy_result_desc(mcx, &desc)?;
        state.slot =
            Some(exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc)));
    }
    let tstore = state.tstore;
    let slot = state.slot.as_mut().expect("just set");
    let mut got: Option<(Datum, bool)> = None;
    loop {
        let more =
            tuplestore::hold::with_store(tstore, |st| st.gettupleslot(true, false, slot, mcx))?;
        if !more {
            break;
        }
        let mut isnull = false;
        let v = exectuples::slot_getattr(slot, 1, &mut isnull);
        got = Some((v, isnull));
    }
    let result = match got {
        Some((v, false)) if state.typbyval => (v, false),
        Some((v, false)) => (
            datum_copy_out(result_mcx.expect("armed for by-ref results"), v, state.typlen)?,
            false,
        ),
        Some((_, true)) | None => (Datum::null(), true),
    };
    exectuples::exec_clear_tuple(slot, mcx);
    tuplestore::hold::with_store(tstore, |st| st.clear());
    Ok(result)
}

fn run_source<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SqlFcacheState<'mcx>,
    psrc: plancache::CachedPlanSourceHandle,
    cplan: types_portal::CachedPlanHandle,
    params_h: ParamListHandle,
    is_last_source: bool,
    pushed: &mut bool,
) -> PgResult<()> {
    let stmt_list = plancache::CachedPlanStmtList(cplan);
    let query_string = plancache::CachedPlanQueryString(psrc);
    let last_tag = stmt_list.iter().rposition(|s| s.canSetTag);
    for (ti, stmt) in stmt_list.iter().enumerate() {
        if let Some(u) = stmt.utilityStmt {
            check_body_utility_node(u)?;
        }
        if state.readonly && !utility::CommandIsReadOnly(stmt) {
            let name = cmdtag::GetCommandTagName(command_tag_of(stmt));
            return Err(efn(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!("{name} is not allowed in a non-volatile function"),
            ));
        }
        if !state.readonly {
            xact::CommandCounterIncrement()?;
            if !*pushed {
                let snap = snapmgr::GetTransactionSnapshot()?;
                snapmgr::PushActiveSnapshot(&snap)?;
                *pushed = true;
            } else {
                snapmgr::UpdateActiveSnapshotCommandId()?;
            }
        }
        let sets_result = is_last_source && Some(ti) == last_tag && !state.returns_void;
        if stmt.utilityStmt.is_some() {
            let mut qc = types_portal::QueryCompletion::default();
            cmdtag::InitializeQueryCompletion(&mut qc);
            let mut dest = tcop_dest::CreateDestReceiver(CommandDest::None);
            // C frees a per-utility subcontext; here the fn-cache mcx holds
            // these allocations until the cache is released (bounded).
            utility_seams::process_utility::call(
                mcx,
                stmt,
                query_string,
                true,
                utility_seams::ProcessUtilityContext::PROCESS_UTILITY_QUERY,
                params_h,
                QueryEnvHandle::NULL,
                &mut dest,
                Some(&mut qc),
            )?;
            continue;
        }
        let mut dest = if sets_result {
            tuplestore::hold::with_store(state.tstore, |st| st.clear());
            let mut d = tcop_dest::CreateDestReceiver(CommandDest::Tuplestore);
            tcop_dest::SetTuplestoreDestReceiverParams(&mut d, state.tstore, false);
            d
        } else {
            tcop_dest::CreateDestReceiver(CommandDest::None)
        };
        let lazy =
            sets_result && stmt.commandType == CmdType::CMD_SELECT && !stmt.hasModifyingCTE;
        let snap = snapmgr::ActiveSnapshotSet().then(snapmgr::GetActiveSnapshot);
        let qd = execmain_seams::create_query_desc::call(
            stmt,
            query_string,
            snap,
            None,
            dest.mydest(),
            params_h,
            QueryEnvHandle::NULL,
            0,
        )?;
        let eflags = if lazy { EXEC_FLAG_SKIP_TRIGGERS } else { 0 };
        let count = if lazy { 1 } else { 0 };
        let r = (|| -> PgResult<()> {
            execmain_seams::executor_start::call(qd, eflags)?;
            execmain_seams::executor_run::call(qd, ForwardScanDirection, count, &mut dest)?;
            execmain_seams::executor_finish::call(qd)?;
            execmain_seams::executor_end::call(qd)
        })();
        match r {
            Ok(()) => execmain_seams::free_query_desc::call(qd),
            Err(e) => {
                execmain_seams::release_query_desc::call(qd);
                return Err(e);
            }
        }
    }
    Ok(())
}

fn command_tag_of(stmt: &types_nodes::plannodes::PlannedStmt<'_>) -> types_core::CommandTag {
    match stmt.utilityStmt {
        Some(u) => utility_seams::create_command_tag::call(u),
        None => match stmt.commandType {
            CmdType::CMD_SELECT => types_portal::CMDTAG_SELECT,
            CmdType::CMD_INSERT => types_portal::CMDTAG_INSERT,
            CmdType::CMD_UPDATE => types_portal::CMDTAG_UPDATE,
            CmdType::CMD_DELETE => types_portal::CMDTAG_DELETE,
            CmdType::CMD_MERGE => types_portal::CMDTAG_MERGE,
            _ => types_portal::CMDTAG_UNKNOWN,
        },
    }
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
    if !guc_tables::vars::check_function_bodies.read() {
        return Ok(Datum::null());
    }
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

// fmgr_sql_validator (pg_proc.c). DIVERGENCES: CheckFunctionValidatorAccess
// and the sql_function_parse_error_callback traceback are unported;
// polymorphic arguments are loud (execution is loud for them anyway).
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
    let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
    let argtypes = read_oidvector_attr(mcx, argv)?;
    let (_, sqlbody_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSQLBODY)?;
    let (prosrc_d, prosrc_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!prosrc_null, "null prosrc for function {funcoid}");
    let prosrc = varlena_str(mcx, prosrc_d)?;
    ReleaseSysCache(tup);

    if lsyscache::typ::get_typtype(rettype)? == b'p' as i8
        && rettype != RECORDOID
        && rettype != VOIDOID
        && !is_polymorphic(rettype)
    {
        return Err(efn(
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            format!(
                "SQL functions cannot return type {}",
                format_type::format_type_be(rettype)?
            ),
        ));
    }
    for &t in argtypes.iter() {
        if lsyscache::typ::get_typtype(t)? == b'p' as i8 {
            if is_polymorphic(t) {
                panic!(
                    "fmgr_sql_validator: polymorphic SQL function arguments unported \
                     (function {funcoid})"
                );
            }
            return Err(efn(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                format!(
                    "SQL functions cannot have arguments of type {}",
                    format_type::format_type_be(t)?
                ),
            ));
        }
    }

    if guc_tables::vars::check_function_bodies.read() {
        if !sqlbody_null {
            panic!(
                "fmgr_sql_validator: prosqlbody (BEGIN ATOMIC bodies) unported \
                 (function {funcoid})"
            );
        }
        let raw_list = parser_seams::raw_parser::call(
            mcx,
            &prosrc,
            parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
        )?;
        let n = raw_list.len();
        let mut last_list: Option<PgVec<'_, Query<'_>>> = None;
        for (i, raw) in raw_list.iter().enumerate() {
            let query = analyze_seams::parse_analyze_fixedparams::call(
                mcx,
                raw,
                &prosrc,
                &argtypes,
                QueryEnvHandle::NULL,
            )?;
            let list = if query.commandType == CmdType::CMD_UTILITY {
                let mut v = PgVec::new_in(mcx);
                v.try_reserve_exact(1).map_err(|_| mcx.oom(1))?;
                v.push(query);
                v
            } else {
                rewrite_handler_seams::query_rewrite::call(mcx, query)?
            };
            if i == n - 1 {
                last_list = Some(list);
            }
        }
        let mut last = match last_list {
            Some(l) => l,
            None if rettype == VOIDOID => PgVec::new_in(mcx),
            None => return Err(retval::retval_mismatch_final_stmt(rettype)),
        };
        retval::check_sql_stmt_retval(mcx, &mut last, rettype)?;
    }
    Ok(Datum::null())
}
