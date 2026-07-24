// funccache.c (SQL slice) + sql_compile_callback/prepare_next_query
// (functions.c). DIVERGENCE: RECORD results resolved from an expectedDesc
// bypass the map (C hashes the resolved tupdesc identity into the key).
use core::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};

use datum::Datum;
use mcx::{bind, Mcx, McxOwned, MemoryContext, PgString, PgVec};
use rustc_hash::FxHashMap;
use types_core::catalog::VOIDOID;
use types_core::Oid;
use elog::ereport;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::{Node, NodeTag};
use types_portal::{QueryEnvHandle, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_PARALLEL_OK};
use types_tuple::TupleDescData;

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, PROCOID};

use crate::{
    efn, is_polymorphic, lookup_failed, name_str, read_oidvector_attr, varlena_bytes, varlena_str,
    ANUM_PG_PROC_PROARGMODES, ANUM_PG_PROC_PROARGNAMES, ANUM_PG_PROC_PROARGTYPES,
    ANUM_PG_PROC_PROKIND, ANUM_PG_PROC_PRONAME, ANUM_PG_PROC_PRORETSET, ANUM_PG_PROC_PROSQLBODY,
    ANUM_PG_PROC_PROSRC, ANUM_PG_PROC_PROVOLATILE,
};

pub(crate) const MAX_SQL_FN_ARGS: usize = types_core::FUNC_MAX_ARGS;

pub(crate) struct SqlFnEntryState<'mcx> {
    pub fname: PgString<'mcx>,
    pub src: PgString<'mcx>,
    pub sqlbody: Option<PgString<'mcx>>,
    pub argtypes: PgVec<'mcx, Oid>,
    pub argnames: PgVec<'mcx, PgString<'mcx>>,
    pub input_collation: Oid,
    pub rettype: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub returns_set: bool,
    pub returns_tuple: Cell<bool>,
    pub readonly_func: bool,
    pub prokind: i8,
    pub rettupdesc: Option<Rc<TupleDescData<'mcx>>>,
    pub num_queries: usize,
    pub plansources: RefCell<PgVec<'mcx, plancache::CachedPlanSourceHandle>>,
}

bind!(pub(crate) SqlFnEntryTy => SqlFnEntryState<'mcx>);

pub(crate) struct SqlFnEntry {
    pub owned: McxOwned<SqlFnEntryTy>,
    stamp: (u32, (u32, u16)),
}

impl Drop for SqlFnEntry {
    fn drop(&mut self) {
        self.owned.with(|s| {
            for &h in s.plansources.borrow().iter() {
                // The hooks installed on the source resolve their owner
                // through this map; the source dies with the entry, so the
                // registration must die with it too (C's parserSetupArg
                // pointer is likewise only valid while the entry lives).
                SQLFN_SOURCE_OWNER.with_borrow_mut(|m| m.remove(&h.0));
                plancache::DropCachedPlan(h);
            }
        });
    }
}

#[derive(Clone, Copy)]
struct FnKey {
    fn_oid: Oid,
    collation: Oid,
    argtypes: [Oid; MAX_SQL_FN_ARGS],
    nargs: u8,
}

// Hash/eq the live argtype prefix only: the array is FUNC_MAX_ARGS wide and
// keyed per call.
impl PartialEq for FnKey {
    fn eq(&self, other: &Self) -> bool {
        self.fn_oid == other.fn_oid
            && self.collation == other.collation
            && self.nargs == other.nargs
            && self.argtypes[..self.nargs as usize] == other.argtypes[..other.nargs as usize]
    }
}

impl Eq for FnKey {}

impl core::hash::Hash for FnKey {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.fn_oid.hash(state);
        self.collation.hash(state);
        self.nargs.hash(state);
        self.argtypes[..self.nargs as usize].hash(state);
    }
}

thread_local! {
    // std map justified: funccache.c is itself an open-ended backend hash.
    // ManuallyDrop: entry Drops reach the plancache/tuplestore TLS
    // registries, whose teardown order at thread exit is unspecified; the
    // map leaks with the backend, exactly like C's CacheMemoryContext.
    static SQL_FN_CACHE: RefCell<core::mem::ManuallyDrop<FxHashMap<FnKey, Rc<SqlFnEntry>>>> =
        RefCell::new(core::mem::ManuallyDrop::new(FxHashMap::default()));

    // Owner of each SQL-function CachedPlanSource, keyed by handle. This is
    // our safe spelling of C's `void *parserSetupArg` / `postRewriteArg`: C
    // hands plancache raw pointers into the hash entry (func->pinfo, func),
    // and plancache hands them back when it re-analyzes. Weak, so a
    // registration never keeps an evicted entry alive; unregistered by
    // SqlFnEntry::drop alongside the DropCachedPlan it pairs with.
    static SQLFN_SOURCE_OWNER: RefCell<core::mem::ManuallyDrop<FxHashMap<u64, Weak<SqlFnEntry>>>> =
        RefCell::new(core::mem::ManuallyDrop::new(FxHashMap::default()));
}

fn proc_row_stamp(fn_oid: Oid) -> PgResult<(u32, (u32, u16))> {
    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(fn_oid)))? else {
        return Err(lookup_failed(fn_oid));
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

struct ProcRow<'mcx> {
    proname: PgString<'mcx>,
    prosrc: PgString<'mcx>,
    prosqlbody: Option<PgString<'mcx>>,
    argtypes: PgVec<'mcx, Oid>,
    argnames: PgVec<'mcx, PgString<'mcx>>,
    provolatile: i8,
    prokind: i8,
    proretset: bool,
}

// proargnames filtered to input args per get_func_input_arg_names
// (funcapi.c): with proargmodes, only i/b/v entries are parameter names.
pub(crate) fn read_input_argnames<'mcx>(
    mcx: Mcx<'mcx>,
    names_d: Datum,
    names_null: bool,
    modes_d: Datum,
    modes_null: bool,
    nargs: usize,
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut out: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    out.try_reserve_exact(nargs).map_err(|_| mcx.oom(nargs))?;
    if names_null {
        for _ in 0..nargs {
            out.push(PgString::from_str_in("", mcx)?);
        }
        return Ok(out);
    }
    let scratch = MemoryContext::new("sqlfn argnames");
    let smcx = scratch.mcx();
    let img = varlena_bytes(smcx, names_d)?;
    // elems borrow img: every read below copies before img drops.
    let elems = datum::array_build::deconstruct_array_image(smcx, &img, -1, false, b'i')?;
    let modes: Option<PgVec<'_, Datum>> = if modes_null {
        None
    } else {
        let mimg = varlena_bytes(smcx, modes_d)?;
        Some(datum::array_build::deconstruct_array_image(smcx, &mimg, 1, true, b'c')?)
    };
    for (i, &e) in elems.iter().enumerate() {
        if let Some(m) = &modes {
            let mode = m[i].as_i8() as u8;
            if !matches!(mode, b'i' | b'b' | b'v') {
                continue;
            }
        }
        if out.len() == nargs {
            break;
        }
        out.push(varlena_str(mcx, e)?);
    }
    while out.len() < nargs {
        out.push(PgString::from_str_in("", mcx)?);
    }
    Ok(out)
}

fn read_proc_row<'mcx>(mcx: Mcx<'mcx>, fn_oid: Oid) -> PgResult<ProcRow<'mcx>> {
    let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(fn_oid)))? else {
        return Err(lookup_failed(fn_oid));
    };
    let (prolang, _) = SysCacheGetAttr(PROCOID, &tup, crate::ANUM_PG_PROC_PROLANG)?;
    assert_eq!(prolang.as_oid(), fmgr_core::SQL_LANGUAGE_ID, "fmgr_sql: not a SQL function");
    let (proname_d, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRONAME)?;
    let proname = name_str(mcx, proname_d)?;
    let (provolatile, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROVOLATILE)?;
    let (prokind, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROKIND)?;
    let (proretset, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PRORETSET)?;
    let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
    let argtypes = read_oidvector_attr(mcx, argv)?;
    let (names_d, names_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGNAMES)?;
    let (modes_d, modes_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGMODES)?;
    let argnames =
        read_input_argnames(mcx, names_d, names_null, modes_d, modes_null, argtypes.len())?;
    let (prosrc_d, prosrc_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSRC)?;
    assert!(!prosrc_null, "null prosrc for function {fn_oid}");
    let prosrc = varlena_str(mcx, prosrc_d)?;
    let (sqlbody_d, sqlbody_null) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROSQLBODY)?;
    let prosqlbody = if sqlbody_null { None } else { Some(varlena_str(mcx, sqlbody_d)?) };
    ReleaseSysCache(tup);
    Ok(ProcRow {
        proname,
        prosrc,
        prosqlbody,
        argtypes,
        argnames,
        provolatile: provolatile.as_i8(),
        prokind: prokind.as_i8(),
        proretset: proretset.as_bool(),
    })
}

// prosqlbody unwrap (sql_compile_callback, functions.c:1152-1164): a List
// whose first element is the query list, a List of queries, or one Query.
pub(crate) fn sqlbody_queries<'mcx>(mcx: Mcx<'mcx>, body: &str) -> PgResult<Vec<Query<'mcx>>> {
    let n = readfuncs::stringToNode(mcx, body)?;
    let mut out = Vec::new();
    match n.as_list() {
        Some(outer) => {
            if outer.is_nil() {
                return Ok(out);
            }
            let first = outer.nth(0);
            if first.node_tag() == NodeTag::T_List {
                for q in first.as_list().expect("tag-checked").iter() {
                    out.push(read_query(q));
                }
            } else {
                for q in outer.iter() {
                    out.push(read_query(q));
                }
            }
        }
        None => out.push(read_query(n)),
    }
    Ok(out)
}

fn read_query<'mcx>(n: Node<'mcx>) -> Query<'mcx> {
    let q = n.as_query().expect("prosqlbody holds analyzed Query nodes");
    Query { ..clone_query(q) }
}

fn clone_query<'mcx>(q: &Query<'mcx>) -> Query<'mcx> {
    // Query is not Copy; field-wise move of shared interior refs (the node
    // tree itself stays shared and is treated as read-only source material).
    unsafe { core::ptr::read(q as *const Query<'mcx>) }
}

fn resolve_argtypes(
    declared: &[Oid],
    flinfo: &fmgr::FmgrInfo,
) -> PgResult<[Oid; MAX_SQL_FN_ARGS]> {
    let mut out = [types_core::InvalidOid; MAX_SQL_FN_ARGS];
    for (i, &t) in declared.iter().enumerate() {
        out[i] = if is_polymorphic(t) {
            let r = funcapi::get_fn_expr_argtype(Some(flinfo), i);
            if r == types_core::InvalidOid {
                return Err(efn(
                    types_error::ERRCODE_DATATYPE_MISMATCH,
                    format!(
                        "could not determine actual type of argument declared {}",
                        format_type::format_type_be(t)?
                    ),
                ));
            }
            r
        } else {
            t
        };
    }
    Ok(out)
}

fn rettupdesc_is_current(entry: &Rc<SqlFnEntry>) -> PgResult<bool> {
    entry.owned.with(|s| -> PgResult<bool> {
        let Some(d) = s.rettupdesc.as_ref() else { return Ok(true) };
        if d.tdtypeid == types_core::catalog::RECORDOID {
            return Ok(true);
        }
        let scratch = MemoryContext::new("sqlfn revalidate");
        let fresh =
            typcache_seams::lookup_rowtype_tupdesc_copy::call(scratch.mcx(), d.tdtypeid, -1)?;
        if fresh.natts != d.natts {
            return Ok(false);
        }
        for i in 0..d.natts as usize {
            let (a, b) = (d.attr(i), fresh.attr(i));
            if a.attname.name_str() != b.attname.name_str()
                || a.atttypid != b.atttypid
                || a.atttypmod != b.atttypmod
                || a.attisdropped != b.attisdropped
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

pub(crate) fn cached_sql_function(
    flinfo: &fmgr::FmgrInfo,
    input_collation: Oid,
    expected_desc: Option<&TupleDescData<'_>>,
) -> PgResult<Rc<SqlFnEntry>> {
    let fn_oid = flinfo.fn_oid;
    let stamp = proc_row_stamp(fn_oid)?;

    let scratch = MemoryContext::new("sqlfn key");
    let (declared, nargs) = {
        let Some(tup) = SearchSysCache1(PROCOID, SysCacheKey::Value(Datum::from_oid(fn_oid)))?
        else {
            return Err(lookup_failed(fn_oid));
        };
        let (argv, _) = SysCacheGetAttr(PROCOID, &tup, ANUM_PG_PROC_PROARGTYPES)?;
        let a = read_oidvector_attr(scratch.mcx(), argv)?;
        ReleaseSysCache(tup);
        let n = a.len();
        assert!(n <= MAX_SQL_FN_ARGS, "fmgr_sql: >{MAX_SQL_FN_ARGS} arguments (FUNC_MAX_ARGS)");
        (a, n)
    };
    let argtypes = resolve_argtypes(&declared, flinfo)?;
    let key = FnKey { fn_oid, collation: input_collation, argtypes, nargs: nargs as u8 };

    if let Some(hit) = SQL_FN_CACHE.with(|c| c.borrow().get(&key).cloned()) {
        // The pg_proc stamp misses ALTERs of a composite rettype's relation
        // (C rebuilds the fcache with the plan, so it re-resolves there);
        // revalidate the resolved rettupdesc against the current rowtype.
        if hit.stamp == stamp && rettupdesc_is_current(&hit)? {
            return Ok(hit);
        }
        SQL_FN_CACHE.with(|c| {
            c.borrow_mut().remove(&key);
        });
    }

    let entry = Rc::new(compile_entry(
        fn_oid,
        stamp,
        &argtypes[..nargs],
        input_collation,
        flinfo,
        expected_desc,
    )?);
    // A RECORD rettype resolves from the CALLING context (expectedDesc /
    // coldeflist), so the entry is context-dependent and must never be
    // shared across call sites — C re-resolves per fn_extra fcache.
    let cacheable = entry.owned.with(|s| s.rettype != types_core::catalog::RECORDOID);
    if cacheable {
        SQL_FN_CACHE.with(|c| {
            c.borrow_mut().insert(key, entry.clone());
        });
    }
    Ok(entry)
}

fn compile_entry(
    fn_oid: Oid,
    stamp: (u32, (u32, u16)),
    argtypes: &[Oid],
    input_collation: Oid,
    flinfo: &fmgr::FmgrInfo,
    expected_desc: Option<&TupleDescData<'_>>,
) -> PgResult<SqlFnEntry> {
    let owned = McxOwned::<SqlFnEntryTy>::try_new(MemoryContext::new("SQL function"), |mcx| {
        let row = read_proc_row(mcx, fn_oid)?;
        let fname_s = row.proname.as_str().to_string();
        let src_s = row.prosrc.as_str().to_string();
        let r = (|| -> PgResult<SqlFnEntryState<'_>> {
            let resolved = funcapi::get_call_result_type(mcx, flinfo, expected_desc)?;
            let rettype = resolved.result_type_id;
            let rettupdesc = resolved.result_tuple_desc.map(Rc::new);
            let (typlen, typbyval) = lsyscache::typ::get_typlenbyval(rettype)?;
            let scratch = MemoryContext::new("sqlfn count");
            let num_queries = match row.prosqlbody.as_ref() {
                Some(body) => {
                    let qs = sqlbody_queries(scratch.mcx(), body.as_str())?;
                    qs.len()
                }
                None => {
                    let raws = parser_seams::raw_parser::call(
                        scratch.mcx(),
                        row.prosrc.as_str(),
                        parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
                    )?;
                    raws.len()
                }
            };
            if num_queries == 0 && rettype != VOIDOID {
                return Err(crate::retval::retval_mismatch_final_stmt(rettype));
            }
            let mut at: PgVec<'_, Oid> = PgVec::new_in(mcx);
            at.try_reserve_exact(argtypes.len().max(1)).map_err(|_| mcx.oom(1))?;
            at.extend_from_slice(argtypes);
            Ok(SqlFnEntryState {
                fname: row.proname,
                src: row.prosrc,
                sqlbody: row.prosqlbody,
                argtypes: at,
                argnames: row.argnames,
                input_collation,
                rettype,
                typlen,
                typbyval,
                returns_set: row.proretset,
                returns_tuple: Cell::new(false),
                readonly_func: row.provolatile != b'v' as i8,
                prokind: row.prokind,
                rettupdesc,
                num_queries,
                plansources: RefCell::new(PgVec::new_in(mcx)),
            })
        })();
        r.map_err(|e| crate::startup_error_context(e, &fname_s, &src_s))
    })?;
    Ok(SqlFnEntry { owned, stamp })
}

// check_sql_fn_statement (functions.c:2051): CALL runs through the generic
// utility lane in postquel_getnext; only OUT-argument procedures are barred.
pub(crate) fn check_sql_fn_statement(q: &Query<'_>) -> PgResult<()> {
    if q.commandType == CmdType::CMD_UTILITY {
        if let Some(u) = q.utilityStmt {
            if u.node_tag() == NodeTag::T_CallStmt {
                let stmt = u.as_call_stmt().expect("tag-checked");
                if stmt.outargs.len() != 0 {
                    return Err(efn(
                        ERRCODE_FEATURE_NOT_SUPPORTED,
                        "calling procedures with output arguments is not supported in SQL functions"
                            .into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

// prepare_next_query (functions.c:899): one CachedPlanSource per original
// query, built lazily. The source is re-derived per index (raw re-parse or
// prosqlbody re-read) so the entry keeps no mutable parse trees.
pub(crate) fn prepare_next_query(entry: &Rc<SqlFnEntry>) -> PgResult<()> {
    let owner = Rc::downgrade(entry);
    entry.owned.with(|s| {
        let qindex = s.plansources.borrow().len();
        assert!(qindex < s.num_queries, "prepare_next_query past end");
        let psrc = build_query_plansource(&owner, s, qindex)?;
        s.plansources.borrow_mut().push(psrc);
        Ok(())
    })
}

/// The CachedPlanSource for one of the function's queries (C's
/// `func->plansource_list` slot). No owner-side revalidation happens here: the
/// source revalidates itself inside GetCachedPlan, through the parserSetup /
/// postRewrite hooks installed when it was created — exactly as in C, where
/// fmgr_sql reads plansource_list and calls GetCachedPlan directly.
pub(crate) fn query_plansource(
    entry: &SqlFnEntry,
    qindex: usize,
) -> plancache::CachedPlanSourceHandle {
    entry.owned.with(|s| s.plansources.borrow()[qindex])
}

fn build_query_plansource(
    owner: &Weak<SqlFnEntry>,
    s: &SqlFnEntryState<'_>,
    qindex: usize,
) -> PgResult<plancache::CachedPlanSourceHandle> {
    {
        let islast = qindex + 1 >= s.num_queries;

        let psrc;
        if let Some(body) = s.sqlbody.as_ref() {
            let scratch = MemoryContext::new("sqlfn tag");
            psrc = {
                let qs = sqlbody_queries(scratch.mcx(), body.as_str())?;
                let tagq = &qs[qindex];
                let q0 = match tagq.utilityStmt {
                    Some(u) => utility_seams::create_command_tag::call(u),
                    None => crate::query_command_tag(tagq.commandType),
                };
                plancache::CreateCachedPlanForQuery(tagq, s.src.as_str(), q0)?
            };
            let build = (|| -> PgResult<()> {
                let qmcx = plancache::SourceQueryMcx(psrc);
                let queries = sqlbody_queries(qmcx, body.as_str())?;
                let query = queries.into_iter().nth(qindex).expect("counted at compile");
                let mut query_list: PgVec<'static, Query<'static>> =
                    if query.commandType == CmdType::CMD_UTILITY {
                        let mut v = PgVec::new_in(qmcx);
                        v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
                        v.push(query);
                        v
                    } else {
                        rewrite_handler_seams::acquire_rewrite_locks::call(
                            qmcx, &query, true, false,
                        )?;
                        rewrite_handler_seams::query_rewrite::call(qmcx, query)?
                    };
                for q in query_list.iter() {
                    check_sql_fn_statement(q)?;
                }
                if islast {
                    let rt = crate::retval::check_sql_stmt_retval(
                        qmcx,
                        &mut query_list,
                        s.rettype,
                        s.rettupdesc.as_deref(),
                        s.prokind,
                        false,
                    )?;
                    s.returns_tuple.set(rt);
                }
                plancache::CompleteCachedPlan(
                    psrc,
                    query_list,
                    &s.argtypes,
                    CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_NO_SCROLL,
                    false,
                )?;
                install_sqlfn_hooks(owner, psrc, qindex, false);
                plancache::SaveCachedPlan(psrc)?;
                Ok(())
            })();
            if let Err(e) = build {
                unregister_sqlfn_source(psrc);
                plancache::DropCachedPlan(psrc);
                return Err(e);
            }
        } else {
            let scratch = MemoryContext::new("sqlfn parse");
            let raw_list = parser_seams::raw_parser::call(
                scratch.mcx(),
                s.src.as_str(),
                parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
            )?;
            let raw = raw_list.get(qindex).expect("counted at compile");
            let stmt = raw.stmt.expect("RawStmt has a stmt");
            let tag = utility_seams::create_command_tag::call(stmt);
            psrc = plancache::CreateCachedPlan(Some(raw), s.src.as_str(), tag)?;
            let build = (|| -> PgResult<()> {
                let qmcx = plancache::SourceQueryMcx(psrc);
                let src = plancache::CachedPlanQueryString(psrc);
                // Retained-tree copy, not a re-parse: a second lex re-emits
                // scanner warnings C doesn't.
                let raw2 = plancache::CachedPlanRawParseTreeCopy(qmcx, psrc)?
                    .expect("created with a raw tree");
                let mut name_refs: PgVec<'_, &str> = PgVec::new_in(qmcx);
                name_refs
                    .try_reserve_exact(s.argnames.len())
                    .map_err(|_| qmcx.oom(s.argnames.len()))?;
                for n in s.argnames.iter() {
                    name_refs.push(n.as_str());
                }
                let query = analyze_seams::parse_analyze_sql_fn::call(
                    qmcx,
                    raw2,
                    src,
                    s.fname.as_str(),
                    &s.argtypes,
                    &name_refs,
                    s.input_collation,
                    QueryEnvHandle::NULL,
                )?;
                let mut query_list: PgVec<'static, Query<'static>> =
                    if query.commandType == CmdType::CMD_UTILITY {
                        let mut v = PgVec::new_in(qmcx);
                        v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
                        v.push(query);
                        v
                    } else {
                        rewrite_handler_seams::query_rewrite::call(qmcx, query)?
                    };
                for q in query_list.iter() {
                    check_sql_fn_statement(q)?;
                }
                if islast {
                    let rt = crate::retval::check_sql_stmt_retval(
                        qmcx,
                        &mut query_list,
                        s.rettype,
                        s.rettupdesc.as_deref(),
                        s.prokind,
                        false,
                    )?;
                    s.returns_tuple.set(rt);
                }
                plancache::CompleteCachedPlan(
                    psrc,
                    query_list,
                    &s.argtypes,
                    CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_NO_SCROLL,
                    false,
                )?;
                install_sqlfn_hooks(owner, psrc, qindex, true);
                plancache::SaveCachedPlan(psrc)?;
                Ok(())
            })();
            if let Err(e) = build {
                unregister_sqlfn_source(psrc);
                plancache::DropCachedPlan(psrc);
                return Err(e);
            }
        }
        Ok(psrc)
    }
}

// C prepare_next_query's hook installation (functions.c:981-1000): the
// parserSetup/parserSetupArg pair CompleteCachedPlan takes, then
// SetPostRewriteHook. Both are what let plancache rebuild the source itself
// when an invalidation lands, instead of the owner having to notice.
//
// `raw_source` distinguishes C's two source shapes. C passes
// sql_fn_parser_setup to CompleteCachedPlan for both, but only the raw-tree
// arm of RevalidateCachedQuery ever consults it — the analyzed-tree
// (prosqlbody) arm just re-rewrites — so installing it there would be dead.
// The postRewrite hook is installed for both, as in C.
fn install_sqlfn_hooks(
    owner: &Weak<SqlFnEntry>,
    psrc: plancache::CachedPlanSourceHandle,
    qindex: usize,
    raw_source: bool,
) {
    SQLFN_SOURCE_OWNER.with_borrow_mut(|m| m.insert(psrc.0, owner.clone()));
    if raw_source {
        plancache::SetCachedPlanReanalyze(psrc, reanalyze_sql_fn, qindex as i32);
    }
    plancache::SetCachedPlanPostRewrite(psrc, sql_postrewrite_callback, qindex as i32);
}

fn unregister_sqlfn_source(psrc: plancache::CachedPlanSourceHandle) {
    SQLFN_SOURCE_OWNER.with_borrow_mut(|m| m.remove(&psrc.0));
}

// The hash entry that owns `h` — C dereferences the pinfo/func pointer it
// stashed in the source; we look the owner up by handle. A live source always
// has a live owner (SqlFnEntry::drop drops its sources), so the miss arm is an
// internal-consistency error, not a user-reachable one.
fn sqlfn_source_owner(h: plancache::CachedPlanSourceHandle) -> PgResult<Rc<SqlFnEntry>> {
    match SQLFN_SOURCE_OWNER.with_borrow(|m| m.get(&h.0).and_then(Weak::upgrade)) {
        Some(e) => Ok(e),
        None => Err(efn(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "SQL function cached plan re-analysis: owning cache entry is gone".to_string(),
        )),
    }
}

// RevalidateCachedQuery's parserSetup arm for a SQL function (C plancache.c:
// 800-809 dispatching to functions.c's sql_fn_parser_setup via
// pg_analyze_and_rewrite_withcb): the retained raw parse tree is re-analyzed
// under the function's own parameter hooks, then rewritten. The statement
// checks and the last-query result munging are NOT done here — C does them in
// the postRewrite hook, which also covers the prosqlbody arm.
fn reanalyze_sql_fn(
    h: plancache::CachedPlanSourceHandle,
    qmcx: Mcx<'static>,
    raw: &'static types_nodes::rawnodes::RawStmt<'static>,
    query_string: &'static str,
    _param_types: &'static [Oid],
    query_env: QueryEnvHandle,
    _arg: i32,
) -> PgResult<PgVec<'static, Query<'static>>> {
    let entry = sqlfn_source_owner(h)?;
    entry.owned.with(|s| {
        let mut name_refs: PgVec<'_, &str> = PgVec::new_in(qmcx);
        name_refs.try_reserve_exact(s.argnames.len()).map_err(|_| qmcx.oom(s.argnames.len()))?;
        for n in s.argnames.iter() {
            name_refs.push(n.as_str());
        }
        let query = analyze_seams::parse_analyze_sql_fn::call(
            qmcx,
            raw,
            query_string,
            s.fname.as_str(),
            &s.argtypes,
            &name_refs,
            s.input_collation,
            query_env,
        )?;
        if query.commandType == CmdType::CMD_UTILITY {
            let mut v = PgVec::new_in(qmcx);
            v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
            v.push(query);
            Ok(v)
        } else {
            rewrite_handler_seams::query_rewrite::call(qmcx, query)
        }
    })
}

// C sql_postrewrite_callback (functions.c:1242-1272). Runs on the freshly
// rewritten query list of EITHER re-analysis arm, before the source adopts it:
// re-check the statement kinds, and for the function's last query redo what
// check_sql_stmt_retval did to the targetlist at creation time (it injects the
// result coercions, so skipping it would leave the cached plan returning the
// wrong type). returnsTuple must not have changed: C is cautious here because
// the junkfilter built from it is not rebuilt by this path.
fn sql_postrewrite_callback(
    h: plancache::CachedPlanSourceHandle,
    qmcx: Mcx<'static>,
    query_list: &mut PgVec<'static, Query<'static>>,
    arg: i32,
) -> PgResult<()> {
    let entry = sqlfn_source_owner(h)?;
    entry.owned.with(|s| {
        for q in query_list.iter() {
            check_sql_fn_statement(q)?;
        }
        // C's postRewriteArg is the hash entry for the last statement and NULL
        // otherwise; the query index carries the same one bit of information.
        let islast = (arg as usize) + 1 >= s.num_queries;
        if islast {
            let returns_tuple = crate::retval::check_sql_stmt_retval(
                qmcx,
                query_list,
                s.rettype,
                s.rettupdesc.as_deref(),
                s.prokind,
                false,
            )?;
            if returns_tuple != s.returns_tuple.get() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cached plan must not change result type")
                    .into_error()
                    .with_funcname("sql_postrewrite_callback")
                    .into());
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datum::{array_build::construct_array_image, set_varsize_4b, VARHDRSZ};

    const TEXTOID: Oid = 25;
    const CHAROID: Oid = 18;

    fn text_image<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgVec<'mcx, u8> {
        let mut img: PgVec<'_, u8> = PgVec::new_in(mcx);
        img.extend_from_slice(&set_varsize_4b(VARHDRSZ + s.len()));
        img.extend_from_slice(s.as_bytes());
        img
    }

    // get_func_input_arg_names: with proargmodes, only i/b/v entries are
    // parameter names — o/t entries are skipped, not blanked.
    #[test]
    fn input_argnames_filter_out_and_table_modes() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let names = ["a", "sum", "b", "cols"];
        let imgs: Vec<PgVec<'_, u8>> = names.iter().map(|s| text_image(mcx, s)).collect();
        let elems: Vec<Datum> =
            imgs.iter().map(|i| Datum::from_usize(i.as_ptr() as usize)).collect();
        let names_img =
            construct_array_image(mcx, &elems, TEXTOID, -1, false, b'i').expect("names array");
        // modes {i, o, b, t}: inputs are a (i) and b (b).
        let modes: Vec<Datum> =
            [b'i', b'o', b'b', b't'].iter().map(|&m| Datum::from_char(m as i8)).collect();
        let modes_img =
            construct_array_image(mcx, &modes, CHAROID, 1, true, b'c').expect("modes array");
        let out = read_input_argnames(
            mcx,
            Datum::from_usize(names_img.as_ptr() as usize),
            false,
            Datum::from_usize(modes_img.as_ptr() as usize),
            false,
            2,
        )
        .expect("read_input_argnames");
        let got: Vec<&str> = out.iter().map(|s| s.as_str()).collect();
        assert_eq!(got, ["a", "b"]);
    }

    // Without proargmodes every listed name is an input name (all-IN
    // signatures store no modes array); short lists pad with "".
    #[test]
    fn input_argnames_no_modes_pads_short_list() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let imgs = [text_image(mcx, "x")];
        let elems = [Datum::from_usize(imgs[0].as_ptr() as usize)];
        let names_img =
            construct_array_image(mcx, &elems, TEXTOID, -1, false, b'i').expect("names array");
        let out = read_input_argnames(
            mcx,
            Datum::from_usize(names_img.as_ptr() as usize),
            false,
            Datum::null(),
            true,
            2,
        )
        .expect("read_input_argnames");
        let got: Vec<&str> = out.iter().map(|s| s.as_str()).collect();
        assert_eq!(got, ["x", ""]);
    }

    // VARIADIC ("v") mode entries are input parameters (the inline path now
    // reads rows with non-null proargmodes; the panic seam is retired).
    #[test]
    fn input_argnames_variadic_is_input() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        let names = ["fmt", "rest"];
        let imgs: Vec<PgVec<'_, u8>> = names.iter().map(|s| text_image(mcx, s)).collect();
        let elems: Vec<Datum> =
            imgs.iter().map(|i| Datum::from_usize(i.as_ptr() as usize)).collect();
        let names_img =
            construct_array_image(mcx, &elems, TEXTOID, -1, false, b'i').expect("names array");
        let modes: Vec<Datum> =
            [b'i', b'v'].iter().map(|&m| Datum::from_char(m as i8)).collect();
        let modes_img =
            construct_array_image(mcx, &modes, CHAROID, 1, true, b'c').expect("modes array");
        let out = read_input_argnames(
            mcx,
            Datum::from_usize(names_img.as_ptr() as usize),
            false,
            Datum::from_usize(modes_img.as_ptr() as usize),
            false,
            2,
        )
        .expect("read_input_argnames");
        let got: Vec<&str> = out.iter().map(|s| s.as_str()).collect();
        assert_eq!(got, ["fmt", "rest"]);
    }
}

#[cfg(test)]
mod fnkey_tests {
    use super::*;

    fn key(nargs: u8, types: &[Oid]) -> FnKey {
        let mut argtypes = [types_core::InvalidOid; MAX_SQL_FN_ARGS];
        argtypes[..types.len()].copy_from_slice(types);
        FnKey { fn_oid: 16384, collation: 100, argtypes, nargs }
    }

    fn fxhash(k: &FnKey) -> u64 {
        use core::hash::{Hash, Hasher};
        let mut h = rustc_hash::FxHasher::default();
        k.hash(&mut h);
        h.finish()
    }

    #[test]
    fn fnkey_matches_func_max_args_and_hashes_live_prefix() {
        assert_eq!(MAX_SQL_FN_ARGS, 100);
        let t20: Vec<Oid> = (1..=20u32).map(|i| 20 + i).collect();
        let a = key(20, &t20);
        let mut b = key(20, &t20);
        b.argtypes[20] = 9999;
        assert!(a == b);
        assert_eq!(fxhash(&a), fxhash(&b));

        let mut c = key(20, &t20);
        c.argtypes[19] = 9999;
        assert!(a != c);
        assert!(a != key(19, &t20[..19]));

        let t100: Vec<Oid> = (1..=100u32).collect();
        let full = key(100, &t100);
        assert_eq!(full.argtypes[99], 100);
        assert!(full != a);
    }
}

#[cfg(test)]
mod callstmt_tests {
    use super::*;

    fn call_query<'m>(mcx: Mcx<'m>, outargs: types_nodes::NodeList<'m>) -> Query<'m> {
        let node = types_nodes::Node::mk(
            mcx,
            types_nodes::rawnodes::CallStmt { funccall: None, funcexpr: None, outargs },
        )
        .unwrap();
        let mut q = Query::default();
        q.commandType = CmdType::CMD_UTILITY;
        q.utilityStmt = Some(node);
        q
    }

    #[test]
    fn call_without_outargs_passes_with_outargs_errors() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        assert!(check_sql_fn_statement(&call_query(mcx, types_nodes::NodeList::nil())).is_ok());

        let mut outargs = types_nodes::NodeList::nil();
        let dummy = types_nodes::Node::mk(mcx, types_nodes::Integer { ival: 1 }).unwrap();
        outargs.lappend(mcx, dummy).unwrap();
        let err = check_sql_fn_statement(&call_query(mcx, outargs)).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("output arguments is not supported in SQL functions"), "{msg}");
    }
}
