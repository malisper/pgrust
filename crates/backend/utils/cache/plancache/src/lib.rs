// plancache.c. Sources/plans live in per-entry leaked MemoryContexts, reclaimed
// on drop; registry handles are generation-checked (C's dangling pointer made
// loud). CachedPlan.refcount IS C's refcount. Divergences (each loud or
// vacuously equal until its lane lands): raw parse trees are not retained
// (classification bits captured at create; the invalidated-replan arm panics),
// query-side (source) invalItems are not collected — the generic plan's
// invalItems (recorded by setrefs) carry the function dependency and drive
// PlanCacheObjectCallback's generic-plan arm; source invalidation would force
// re-analysis (the replan arm), RLS fields are constant-false.
#![allow(non_snake_case)]

use core::cell::RefCell;
use std::rc::Rc;

use cache_syscache::cacheinfo::{
    AMOPOPID, FOREIGNDATAWRAPPEROID, FOREIGNSERVEROID, NAMESPACEOID, OPEROID, PROCOID, TYPEOID,
};
use catalog_namespace::SearchPathMatcher;
use datum::Datum;
use elog::ereport;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::xact::{InvalidTransactionId, TransactionIdIsValid};
use types_core::TransactionId;
use types_core::{CommandTag, InvalidOid, Oid};
use types_error::{PgResult, ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::plannodes::PlannedStmt;
use types_nodes::rawnodes::RawStmt;
use types_portal::{
    CachedPlanHandle, ParamListHandle, PortalStrategy, QueryEnvHandle, CURSOR_OPT_CUSTOM_PLAN,
    CURSOR_OPT_GENERIC_PLAN,
};
use types_resowner::ResourceOwner;
use types_tuple::TupleDescData;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CachedPlanSourceHandle(pub u64);

struct CachedPlanSource {
    handle_gen: u32,
    query_string: &'static str,
    commandTag: CommandTag,
    param_types: &'static [Oid],
    cursor_options: i32,
    fixed_result: bool,
    requires_reval: bool,
    requires_snapshot: bool,
    is_xact_exit_stmt: bool,
    query_list: &'static [Query<'static>],
    relation_oids: &'static [Oid],
    search_path: Option<SearchPathMatcher<'static>>,
    result_desc: Option<Rc<TupleDescData<'static>>>,
    gplan: Option<CachedPlanHandle>,
    is_complete: bool,
    is_saved: bool,
    is_valid: bool,
    // Dropped source whose arenas must outlive it: custom plans share
    // query-arena subnodes (C copies; we defer the free to last plan release).
    dead: bool,
    generation: i32,
    generic_cost: f64,
    total_custom_cost: f64,
    num_generic_plans: i64,
    num_custom_plans: i64,
    source_ctx: *mut MemoryContext,
    query_ctx: *mut MemoryContext,
}

struct CachedPlan {
    handle_gen: u32,
    source: CachedPlanSourceHandle,
    stmt_list: &'static [PlannedStmt<'static>],
    plan_role_id: Oid,
    depends_on_role: bool,
    saved_xmin: TransactionId,
    refcount: i32,
    generation: i32,
    is_saved: bool,
    is_valid: bool,
    plan_ctx: *mut MemoryContext,
}

// Registry vectors are bare std Vec: const-init TLS slot maps, never arena
// data (pquery::stmt_list precedent).
struct PlanCache {
    sources: Vec<Option<CachedPlanSource>>,
    source_free: Vec<u32>,
    plans: Vec<Option<CachedPlan>>,
    plan_free: Vec<u32>,
    saved_plan_list: Vec<CachedPlanSourceHandle>,
    handle_gen: u32,
}

thread_local! {
    static CACHE: RefCell<PlanCache> = const {
        RefCell::new(PlanCache {
            sources: Vec::new(),
            source_free: Vec::new(),
            plans: Vec::new(),
            plan_free: Vec::new(),
            saved_plan_list: Vec::new(),
            handle_gen: 0,
        })
    };
}

fn encode(idx: u32, generation: u32) -> u64 {
    (u64::from(generation) << 32) | u64::from(idx + 1)
}

fn decode(h: u64) -> (usize, u32) {
    (((h as u32) - 1) as usize, (h >> 32) as u32)
}

// Callbacks flip flags only and never call out, so one borrow per entry point
// is safe (fabled's ResetPlanCache double-borrow incident); helpers that lock,
// plan, or probe catalogs run outside any borrow.
fn with_cache<R>(f: impl FnOnce(&mut PlanCache) -> R) -> R {
    CACHE.with(|c| f(&mut c.borrow_mut()))
}

fn source_mut(pc: &mut PlanCache, h: CachedPlanSourceHandle) -> &mut CachedPlanSource {
    let (idx, generation) = decode(h.0);
    match pc.sources.get_mut(idx).and_then(Option::as_mut) {
        Some(s) if s.handle_gen == generation => s,
        _ => panic!("plancache: stale CachedPlanSourceHandle {h:?} (dropped)"),
    }
}

fn with_source<R>(h: CachedPlanSourceHandle, f: impl FnOnce(&mut CachedPlanSource) -> R) -> R {
    with_cache(|pc| f(source_mut(pc, h)))
}

fn plan_mut(pc: &mut PlanCache, h: CachedPlanHandle) -> &mut CachedPlan {
    let (idx, generation) = decode(h.0);
    match pc.plans.get_mut(idx).and_then(Option::as_mut) {
        Some(p) if p.handle_gen == generation => p,
        _ => panic!("plancache: stale CachedPlanHandle {h:?} (released)"),
    }
}

fn with_plan<R>(h: CachedPlanHandle, f: impl FnOnce(&mut CachedPlan) -> R) -> R {
    with_cache(|pc| f(plan_mut(pc, h)))
}

fn leak_ctx(name: &'static str) -> *mut MemoryContext {
    Box::into_raw(Box::new(MemoryContext::new(name)))
}

fn ctx_mcx(ctx: *mut MemoryContext) -> Mcx<'static> {
    // SAFETY: ctx came from leak_ctx and is reclaimed only after its owning
    // registry slot (the only path to it) is removed.
    unsafe { (*ctx).mcx() }
}

fn reclaim_ctx(ctx: *mut MemoryContext) {
    // SAFETY: leak_ctx provenance; caller removed every reference first.
    drop(unsafe { Box::from_raw(ctx) });
}

pub fn init_seams() {
    plancache_portal_seams::init_plan_cache::set(InitPlanCache);
    plancache_portal_seams::release_cached_plan::set(ReleaseCachedPlan);
    // C: plancache.c owns `int plan_cache_mode = PLAN_CACHE_MODE_AUTO`.
    thread_local! {
        static PLAN_CACHE_MODE: core::cell::Cell<i32> =
            const { core::cell::Cell::new(guc_tables::consts::PLAN_CACHE_MODE_AUTO) };
    }
    // installed() guard: test fixtures shim this slot before init_seams.
    {
        guc_tables::vars::plan_cache_mode.install_if_absent(guc_tables::GucVarAccessors {
            get: || PLAN_CACHE_MODE.with(core::cell::Cell::get),
            set: |v| PLAN_CACHE_MODE.with(|c| c.set(v)),
        });
    }
}

pub fn InitPlanCache() -> PgResult<()> {
    let zero = Datum::from_oid(InvalidOid);
    inval::invalidate::CacheRegisterRelcacheCallback(PlanCacheRelCallback, zero)?;
    inval::invalidate::CacheRegisterSyscacheCallback(PROCOID, PlanCacheObjectCallback, zero)?;
    inval::invalidate::CacheRegisterSyscacheCallback(TYPEOID, PlanCacheObjectCallback, zero)?;
    inval::invalidate::CacheRegisterSyscacheCallback(NAMESPACEOID, PlanCacheSysCallback, zero)?;
    inval::invalidate::CacheRegisterSyscacheCallback(OPEROID, PlanCacheSysCallback, zero)?;
    inval::invalidate::CacheRegisterSyscacheCallback(AMOPOPID, PlanCacheSysCallback, zero)?;
    inval::invalidate::CacheRegisterSyscacheCallback(FOREIGNSERVEROID, PlanCacheSysCallback, zero)?;
    inval::invalidate::CacheRegisterSyscacheCallback(
        FOREIGNDATAWRAPPEROID,
        PlanCacheSysCallback,
        zero,
    )?;
    Ok(())
}

pub fn CreateCachedPlan(
    raw_parse_tree: Option<&RawStmt<'_>>,
    query_string: &str,
    commandTag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    let (requires_reval, requires_snapshot, is_xact_exit_stmt) = match raw_parse_tree {
        Some(raw) => (
            parser_analyze::stmt_requires_parse_analysis(raw),
            parser_analyze::analyze_requires_snapshot(raw),
            is_transaction_exit_stmt(raw),
        ),
        None => (false, false, false),
    };

    let source_ctx = leak_ctx("CachedPlanSource");
    let query_ctx = leak_ctx("CachedPlanQuery");
    let mcx = ctx_mcx(source_ctx);
    let qs = mcx::slice_borrow_in(mcx, query_string.as_bytes())?;
    let query_string: &'static str = core::str::from_utf8(qs).expect("query_string is UTF-8");

    Ok(with_cache(|pc| {
        pc.handle_gen = pc.handle_gen.wrapping_add(1);
        let source = CachedPlanSource {
            handle_gen: pc.handle_gen,
            query_string,
            commandTag,
            param_types: &[],
            cursor_options: 0,
            fixed_result: false,
            requires_reval,
            requires_snapshot,
            is_xact_exit_stmt,
            query_list: &[],
            relation_oids: &[],
            search_path: None,
            result_desc: None,
            gplan: None,
            is_complete: false,
            is_saved: false,
            is_valid: false,
            dead: false,
            generation: 0,
            generic_cost: -1.0,
            total_custom_cost: 0.0,
            num_generic_plans: 0,
            num_custom_plans: 0,
            source_ctx,
            query_ctx,
        };
        let idx = match pc.source_free.pop() {
            Some(i) => {
                pc.sources[i as usize] = Some(source);
                i
            }
            None => {
                pc.sources.push(Some(source));
                (pc.sources.len() - 1) as u32
            }
        };
        CachedPlanSourceHandle(encode(idx, pc.handle_gen))
    }))
}

/// C CompleteCachedPlan's `querytree_context`: analysis output must be
/// allocated with this Mcx so the plansource owns it with zero copies.
pub fn SourceQueryMcx(h: CachedPlanSourceHandle) -> Mcx<'static> {
    ctx_mcx(with_source(h, |src| src.query_ctx))
}

pub fn CompleteCachedPlan(
    h: CachedPlanSourceHandle,
    query_list: PgVec<'static, Query<'static>>,
    param_types: &[Oid],
    cursor_options: i32,
    fixed_result: bool,
) -> PgResult<()> {
    let (source_ctx, query_ctx, requires_reval) = with_source(h, |src| {
        assert!(!src.is_complete, "CompleteCachedPlan: already complete");
        (src.source_ctx, src.query_ctx, src.requires_reval)
    });
    let source_mcx = ctx_mcx(source_ctx);
    let query_mcx = ctx_mcx(query_ctx);

    let query_list: &'static [Query<'static>] = mcx::vec_borrow_in(query_mcx, query_list)?;

    let (relation_oids, search_path) = if requires_reval {
        let mut oids: PgVec<'static, Oid> = PgVec::new_in(query_mcx);
        for q in query_list {
            extract_query_relation_deps(q, &mut oids)?;
        }
        (
            mcx::vec_borrow_in(query_mcx, oids)?,
            Some(catalog_namespace::GetSearchPathMatcher(query_mcx)?),
        )
    } else {
        (&[] as &[Oid], None)
    };

    let result_desc = plan_cache_compute_result_desc(source_mcx, query_list)?;
    let param_types: &'static [Oid] = mcx::slice_borrow_in(source_mcx, param_types)?;

    with_source(h, |src| {
        src.query_list = query_list;
        src.relation_oids = relation_oids;
        src.search_path = search_path;
        src.result_desc = result_desc;
        src.param_types = param_types;
        src.cursor_options = cursor_options;
        src.fixed_result = fixed_result;
        src.is_complete = true;
        src.is_valid = true;
    });
    Ok(())
}

pub fn SaveCachedPlan(h: CachedPlanSourceHandle) -> PgResult<()> {
    ReleaseGenericPlan(h);
    with_cache(|pc| {
        let src = source_mut(pc, h);
        assert!(src.is_complete && !src.is_saved, "SaveCachedPlan: bad order");
        src.is_saved = true;
        pc.saved_plan_list.push(h);
    });
    Ok(())
}

pub fn DropCachedPlan(h: CachedPlanSourceHandle) {
    with_cache(|pc| {
        let src = source_mut(pc, h);
        if src.is_saved {
            src.is_saved = false;
            pc.saved_plan_list.retain(|&s| s != h);
        }
    });
    ReleaseGenericPlan(h);
    let ctxs = with_cache(|pc| {
        // A surviving refcounted plan (pipelined portal) tombstones the
        // source; ReleaseCachedPlan of the last survivor frees it.
        if pc.plans.iter().flatten().any(|p| p.source == h) {
            source_mut(pc, h).dead = true;
            return None;
        }
        let (idx, _) = decode(h.0);
        let src = pc.sources[idx].take().expect("checked by source_mut");
        pc.source_free.push(idx as u32);
        Some((src.source_ctx, src.query_ctx))
    });
    if let Some((source_ctx, query_ctx)) = ctxs {
        reclaim_ctx(query_ctx);
        reclaim_ctx(source_ctx);
    }
}

fn ReleaseGenericPlan(h: CachedPlanSourceHandle) {
    let gplan = with_source(h, |src| src.gplan.take());
    if let Some(plan) = gplan {
        ReleaseCachedPlan(plan);
    }
}

pub fn ReleaseCachedPlan(cplan: CachedPlanHandle) {
    let freed = with_cache(|pc| {
        let plan = plan_mut(pc, cplan);
        assert!(plan.refcount > 0, "ReleaseCachedPlan: refcount underflow");
        plan.refcount -= 1;
        if plan.refcount == 0 {
            let (idx, _) = decode(cplan.0);
            let plan = pc.plans[idx].take().expect("checked by plan_mut");
            pc.plan_free.push(idx as u32);
            let mut ctxs = vec![plan.plan_ctx];
            // Last survivor of a dead source reclaims the tombstone.
            let (sidx, sgen) = decode(plan.source.0);
            let src_dead = pc.sources.get(sidx).and_then(Option::as_ref).is_some_and(|s| {
                s.handle_gen == sgen && s.dead
            });
            if src_dead && !pc.plans.iter().flatten().any(|p| p.source == plan.source) {
                let src = pc.sources[sidx].take().expect("checked above");
                pc.source_free.push(sidx as u32);
                ctxs.push(src.query_ctx);
                ctxs.push(src.source_ctx);
            }
            ctxs
        } else {
            Vec::new()
        }
    });
    for ctx in freed {
        reclaim_ctx(ctx);
    }
}

pub fn CachedPlanIsValid(h: CachedPlanSourceHandle) -> bool {
    with_source(h, |src| src.is_valid)
}

// C CachedPlanIsSimplyValid (plancache.c) minus the resowner arm: true only
// while `cplan` is the source's current generic plan, both are valid, and the
// captured search_path still matches the current environment. Caller must
// hold a refcount on `cplan`.
pub fn CachedPlanIsSimplyValid(
    h: CachedPlanSourceHandle,
    cplan: CachedPlanHandle,
) -> PgResult<bool> {
    let ok = with_cache(|pc| {
        let (src_valid, gplan) = {
            let src = source_mut(pc, h);
            (src.is_valid, src.gplan)
        };
        src_valid && gplan == Some(cplan) && plan_mut(pc, cplan).is_valid
    });
    if !ok {
        return Ok(false);
    }
    // Matcher taken out of the registry: SearchPathMatchesCurrentEnvironment
    // probes catalogs and must run outside the cache borrow.
    let mut matcher = with_source(h, |src| src.search_path.take());
    let matches = match matcher.as_mut() {
        Some(m) => catalog_namespace::SearchPathMatchesCurrentEnvironment(m),
        None => panic!("CachedPlanIsSimplyValid: valid revalidatable source lost its search_path"),
    };
    with_source(h, |src| src.search_path = matcher);
    matches
}

thread_local! {
    // Monotonic count of plancache invalidation events; consumers holding
    // uncached coercion expressions (plpgsql cast cache) rebuild on any bump
    // (conservative stand-in for C's CachedExpression is_valid).
    static INVAL_COUNTER: core::cell::Cell<u64> = const { core::cell::Cell::new(0) };
}

pub fn PlanCacheInvalCounter() -> u64 {
    INVAL_COUNTER.with(core::cell::Cell::get)
}

fn bump_inval_counter() {
    INVAL_COUNTER.with(|c| c.set(c.get().wrapping_add(1)));
}

/// Valid while the caller holds a refcount on `cplan` (C: cplan->stmt_list).
pub fn CachedPlanStmtList(cplan: CachedPlanHandle) -> &'static [PlannedStmt<'static>] {
    with_plan(cplan, |plan| {
        assert!(plan.refcount > 0, "CachedPlanStmtList: caller holds no refcount");
        plan.stmt_list
    })
}

pub fn GetCachedPlan(
    h: CachedPlanSourceHandle,
    boundParams: ParamListHandle,
    owner: Option<ResourceOwner>,
    queryEnv: QueryEnvHandle,
) -> PgResult<CachedPlanHandle> {
    let is_saved = with_source(h, |src| {
        assert!(src.is_complete, "GetCachedPlan: incomplete plansource");
        src.is_saved
    });
    if let Some(owner) = owner {
        if !is_saved {
            return Err(ereport(ERROR)
                .errmsg("cannot apply ResourceOwner to non-saved cached plan")
                .into_error()
                .into());
        }
        panic!(
            "GetCachedPlan (plancache.c): ResourceOwner-tracked plan refs ({owner:?}) \
             are the EXPLAIN EXECUTE/SPI lane"
        );
    }

    RevalidateCachedQuery(h, queryEnv)?;

    let mut customplan = choose_custom_plan(h, boundParams);
    let mut plan: Option<CachedPlanHandle> = None;

    if !customplan {
        if CheckCachedPlan(h)? {
            plan = with_source(h, |src| src.gplan);
            debug_assert!(plan.is_some());
        } else {
            let built = BuildCachedPlan(h, ParamListHandle::NULL, queryEnv)?;
            ReleaseGenericPlan(h);
            with_cache(|pc| {
                let cost = cached_plan_cost(plan_mut(pc, built).stmt_list, false);
                let p = plan_mut(pc, built);
                p.refcount += 1;
                p.is_saved = is_saved;
                let src = source_mut(pc, h);
                src.gplan = Some(built);
                src.generic_cost = cost;
            });
            plan = Some(built);
            // C wart: re-choose with the now-known generic cost; a losing
            // generic plan is kept but not executed.
            customplan = choose_custom_plan(h, boundParams);
        }
    }

    if customplan {
        let built = BuildCachedPlan(h, boundParams, queryEnv)?;
        with_cache(|pc| {
            let cost = cached_plan_cost(plan_mut(pc, built).stmt_list, true);
            let src = source_mut(pc, h);
            src.total_custom_cost += cost;
            src.num_custom_plans += 1;
        });
        plan = Some(built);
    }

    let plan = plan.expect("GetCachedPlan: no plan chosen");
    with_cache(|pc| {
        if !customplan {
            source_mut(pc, h).num_generic_plans += 1;
        }
        let p = plan_mut(pc, plan);
        p.refcount += 1;
        if customplan && is_saved {
            p.is_saved = true;
        }
    });
    Ok(plan)
}

fn RevalidateCachedQuery(h: CachedPlanSourceHandle, _queryEnv: QueryEnvHandle) -> PgResult<()> {
    // One borrow per phase; the catalog probe and lock calls run outside.
    let (requires_reval, is_valid, mut matcher) = with_source(h, |src| {
        let take = src.requires_reval && src.is_valid;
        (
            src.requires_reval,
            src.is_valid,
            if take { src.search_path.take() } else { None },
        )
    });
    if !requires_reval {
        debug_assert!(is_valid);
        return Ok(());
    }

    if is_valid {
        let matches = match matcher.as_mut() {
            Some(m) => catalog_namespace::SearchPathMatchesCurrentEnvironment(m)?,
            None => panic!("RevalidateCachedQuery: valid revalidatable source lost its search_path"),
        };
        with_cache(|pc| {
            let src = source_mut(pc, h);
            src.search_path = matcher;
            if !matches {
                if let Some(gplan) = invalidate_source_entry(src) {
                    plan_mut(pc, gplan).is_valid = false;
                }
            }
        });
    }

    if let Some(query_list) = with_source(h, |src| src.is_valid.then_some(src.query_list)) {
        AcquirePlannerLocks(query_list, true)?;
        if with_source(h, |src| src.is_valid) {
            return Ok(());
        }
        AcquirePlannerLocks(query_list, false)?;
    }

    panic!(
        "RevalidateCachedQuery (plancache.c): plan for {:?} was invalidated; re-analysis \
         needs a retained raw parse tree (analyze-rewrite hooks lane)",
        with_source(h, |src| src.commandTag)
    );
}

fn CheckCachedPlan(h: CachedPlanSourceHandle) -> PgResult<bool> {
    let user = miscinit::GetUserId();
    let Some((gplan, mut is_valid, stmt_list)) = with_cache(|pc| {
        let src = source_mut(pc, h);
        debug_assert!(src.is_valid);
        let gplan = src.gplan?;
        let plan = plan_mut(pc, gplan);
        debug_assert!(plan.refcount > 0);
        if plan.is_valid && plan.depends_on_role && plan.plan_role_id != user {
            plan.is_valid = false;
        }
        Some((gplan, plan.is_valid, plan.stmt_list))
    }) else {
        return Ok(false);
    };

    if is_valid {
        AcquireExecutorLocks(stmt_list, true)?;
        is_valid = with_cache(|pc| {
            let plan = plan_mut(pc, gplan);
            if plan.is_valid
                && TransactionIdIsValid(plan.saved_xmin)
                && plan.saved_xmin != snapmgr::TransactionXmin()
            {
                plan.is_valid = false;
            }
            plan.is_valid
        });
        if is_valid {
            return Ok(true);
        }
        AcquireExecutorLocks(stmt_list, false)?;
    }

    ReleaseGenericPlan(h);
    Ok(false)
}

fn BuildCachedPlan(
    h: CachedPlanSourceHandle,
    boundParams: ParamListHandle,
    _queryEnv: QueryEnvHandle,
) -> PgResult<CachedPlanHandle> {
    let (query_list, query_string, cursor_options, requires_snapshot, is_valid) =
        with_source(h, |src| {
            (
                src.query_list,
                src.query_string,
                src.cursor_options,
                src.requires_snapshot,
                src.is_valid,
            )
        });
    if !is_valid {
        panic!(
            "BuildCachedPlan (plancache.c): invalidated while building (sinval-reset race); \
             re-revalidation needs the analyze-rewrite hooks lane"
        );
    }

    let plan_ctx = leak_ctx("CachedPlan");
    let result = build_stmt_list(
        ctx_mcx(plan_ctx),
        query_list,
        query_string,
        cursor_options,
        requires_snapshot,
        boundParams,
    );
    let stmt_list = match result {
        Ok(list) => list,
        Err(e) => {
            reclaim_ctx(plan_ctx);
            return Err(e);
        }
    };

    let mut depends_on_role = false;
    let mut is_transient = false;
    for stmt in stmt_list {
        if stmt.commandType == CmdType::CMD_UTILITY {
            continue;
        }
        is_transient |= stmt.transientPlan;
        depends_on_role |= stmt.dependsOnRole;
    }
    let saved_xmin = if is_transient {
        snapmgr::TransactionXmin()
    } else {
        InvalidTransactionId
    };
    let plan_role_id = miscinit::GetUserId();

    Ok(with_cache(|pc| {
        pc.handle_gen = pc.handle_gen.wrapping_add(1);
        let src = source_mut(pc, h);
        src.generation += 1;
        let generation = src.generation;
        let plan = CachedPlan {
            handle_gen: pc.handle_gen,
            source: h,
            stmt_list,
            plan_role_id,
            depends_on_role,
            saved_xmin,
            refcount: 0,
            generation,
            is_saved: false,
            is_valid: true,
            plan_ctx,
        };
        let idx = match pc.plan_free.pop() {
            Some(i) => {
                pc.plans[i as usize] = Some(plan);
                i
            }
            None => {
                pc.plans.push(Some(plan));
                (pc.plans.len() - 1) as u32
            }
        };
        CachedPlanHandle(encode(idx, pc.handle_gen))
    }))
}

fn build_stmt_list(
    mcx: Mcx<'static>,
    query_list: &[Query<'static>],
    query_string: &'static str,
    cursor_options: i32,
    requires_snapshot: bool,
    boundParams: ParamListHandle,
) -> PgResult<&'static [PlannedStmt<'static>]> {
    let mut snapshot_set = false;
    if !snapmgr::ActiveSnapshotSet() && requires_snapshot {
        let snap: snapmgr::Snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
        snapshot_set = true;
    }

    let plan = |mcx: Mcx<'static>| -> PgResult<PgVec<'static, PlannedStmt<'static>>> {
        let mut stmts: PgVec<'static, PlannedStmt<'static>> = PgVec::new_in(mcx);
        stmts
            .try_reserve_exact(query_list.len())
            .map_err(|_| mcx.oom(query_list.len()))?;
        for q in query_list {
            if q.commandType == CmdType::CMD_UTILITY {
                stmts.push(PlannedStmt {
                    commandType: CmdType::CMD_UTILITY,
                    canSetTag: q.canSetTag,
                    utilityStmt: q.utilityStmt,
                    stmt_location: q.stmt_location,
                    stmt_len: q.stmt_len,
                    queryId: q.queryId,
                    ..PlannedStmt::default()
                });
            } else {
                let input = clone_query_in(mcx, q)?;
                stmts.push(planner_seams::planner::call(
                    mcx,
                    input,
                    query_string,
                    cursor_options,
                    boundParams,
                )?);
            }
        }
        Ok(stmts)
    };
    let result = plan(mcx);

    if snapshot_set {
        snapmgr::PopActiveSnapshot()?;
    }

    mcx::vec_borrow_in(mcx, result?)
}

// copyObject(query_list) analog (BuildCachedPlan): top-level structs and list
// cell arrays are copied so the planner never scribbles on cached cells;
// subnodes stay shared — safe Rust denies the planner in-place edits of them.
fn clone_query_in(mcx: Mcx<'static>, q: &Query<'static>) -> PgResult<Query<'static>> {
    let Query {
        commandType,
        querySource,
        queryId,
        canSetTag,
        utilityStmt,
        resultRelation,
        hasAggs,
        hasWindowFuncs,
        hasTargetSRFs,
        hasSubLinks,
        hasDistinctOn,
        hasRecursive,
        hasModifyingCTE,
        hasForUpdate,
        hasRowSecurity,
        hasGroupRTE,
        isReturn,
        ref cteList,
        ref rtable,
        ref rteperminfos,
        jointree,
        ref mergeActionList,
        mergeTargetRelation,
        mergeJoinCondition,
        ref targetList,
        r#override,
        onConflict,
        returningOldAlias,
        returningNewAlias,
        ref returningList,
        ref groupClause,
        groupDistinct,
        ref groupingSets,
        havingQual,
        ref windowClause,
        ref distinctClause,
        ref sortClause,
        limitOffset,
        limitCount,
        limitOption,
        ref rowMarks,
        setOperations,
        ref constraintDeps,
        ref withCheckOptions,
        stmt_location,
        stmt_len,
    } = *q;
    Ok(Query {
        commandType,
        querySource,
        queryId,
        canSetTag,
        utilityStmt,
        resultRelation,
        hasAggs,
        hasWindowFuncs,
        hasTargetSRFs,
        hasSubLinks,
        hasDistinctOn,
        hasRecursive,
        hasModifyingCTE,
        hasForUpdate,
        hasRowSecurity,
        hasGroupRTE,
        isReturn,
        cteList: cteList.clone_in(mcx)?,
        rtable: rtable.clone_in(mcx)?,
        rteperminfos: rteperminfos.clone_in(mcx)?,
        jointree,
        mergeActionList: mergeActionList.clone_in(mcx)?,
        mergeTargetRelation,
        mergeJoinCondition,
        targetList: targetList.clone_in(mcx)?,
        r#override,
        onConflict,
        returningOldAlias,
        returningNewAlias,
        returningList: returningList.clone_in(mcx)?,
        groupClause: groupClause.clone_in(mcx)?,
        groupDistinct,
        groupingSets: groupingSets.clone_in(mcx)?,
        havingQual,
        windowClause: windowClause.clone_in(mcx)?,
        distinctClause: distinctClause.clone_in(mcx)?,
        sortClause: sortClause.clone_in(mcx)?,
        limitOffset,
        limitCount,
        limitOption,
        rowMarks: rowMarks.clone_in(mcx)?,
        setOperations,
        constraintDeps: constraintDeps.clone_in(mcx)?,
        withCheckOptions: withCheckOptions.clone_in(mcx)?,
        stmt_location,
        stmt_len,
    })
}

fn choose_custom_plan(h: CachedPlanSourceHandle, boundParams: ParamListHandle) -> bool {
    if boundParams.is_null() {
        return false;
    }
    with_source(h, |src| {
        if !src.requires_reval {
            return false;
        }
        let mode = guc_tables::vars::plan_cache_mode.read();
        if mode == guc_tables::consts::PLAN_CACHE_MODE_FORCE_GENERIC_PLAN {
            return false;
        }
        if mode == guc_tables::consts::PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN {
            return true;
        }
        if src.cursor_options & CURSOR_OPT_GENERIC_PLAN != 0 {
            return false;
        }
        if src.cursor_options & CURSOR_OPT_CUSTOM_PLAN != 0 {
            return true;
        }
        if src.num_custom_plans < 5 {
            return true;
        }
        let avg_custom_cost = src.total_custom_cost / src.num_custom_plans as f64;
        // generic_cost == -1 (not yet known) also prefers generic, as in C.
        if src.generic_cost < avg_custom_cost {
            return false;
        }
        true
    })
}

fn cached_plan_cost(stmt_list: &[PlannedStmt<'_>], include_planner: bool) -> f64 {
    let mut result = 0.0;
    for stmt in stmt_list {
        if stmt.commandType == CmdType::CMD_UTILITY {
            continue;
        }
        if let Some(tree) = stmt.planTree {
            result += tree.as_plan().expect("planTree is a plan node").total_cost;
        }
        if include_planner {
            // C's crude planning-effort charge: 1000 * cpu_operator_cost per
            // rangetable entry plus one.
            let nrelations = stmt.rtable.len() as f64;
            result += 1000.0 * guc_tables::vars::cpu_operator_cost.read() * (nrelations + 1.0);
        }
    }
    result
}

fn AcquirePlannerLocks(query_list: &[Query<'static>], acquire: bool) -> PgResult<()> {
    for query in query_list {
        if query.commandType == CmdType::CMD_UTILITY {
            panic!(
                "AcquirePlannerLocks (plancache.c): UtilityContainsQuery probe is the \
                 EXPLAIN/CTAS/DECLARE lane"
            );
        }
        ScanQueryForLocks(query, acquire)?;
    }
    Ok(())
}

fn AcquireExecutorLocks(stmt_list: &[PlannedStmt<'static>], acquire: bool) -> PgResult<()> {
    for stmt in stmt_list {
        if stmt.commandType == CmdType::CMD_UTILITY {
            panic!(
                "AcquireExecutorLocks (plancache.c): UtilityContainsQuery probe is the \
                 EXPLAIN/CTAS/DECLARE lane"
            );
        }
        for rte_node in stmt.rtable.iter() {
            let rte = rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");
            let lockable = rte.rtekind == RTEKind::RTE_RELATION
                || (rte.rtekind == RTEKind::RTE_SUBQUERY && rte.relid != InvalidOid);
            if !lockable {
                continue;
            }
            lock_relation(rte, acquire)?;
        }
    }
    Ok(())
}

fn lock_relation(rte: &RangeTblEntry<'_>, acquire: bool) -> PgResult<()> {
    if acquire {
        lmgr::LockRelationOid(rte.relid, rte.rellockmode)
    } else {
        lmgr::UnlockRelationOid(rte.relid, rte.rellockmode)
    }
}

fn ScanQueryForLocks(query: &Query<'static>, acquire: bool) -> PgResult<()> {
    debug_assert!(query.commandType != CmdType::CMD_UTILITY);
    for rte_node in query.rtable.iter() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");
        match rte.rtekind {
            RTEKind::RTE_RELATION => lock_relation(rte, acquire)?,
            RTEKind::RTE_SUBQUERY => {
                if rte.relid != InvalidOid {
                    lock_relation(rte, acquire)?;
                }
                ScanQueryForLocks(rte.subquery.expect("RTE_SUBQUERY has subquery"), acquire)?;
            }
            _ => {}
        }
    }
    for cte_node in &query.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let ctequery = cte.ctequery.and_then(|n| n.as_query()).expect("analyzed CTE query");
        ScanQueryForLocks(ctequery, acquire)?;
    }
    if query.hasSubLinks {
        walk_sublink_queries(query, &mut |sub| ScanQueryForLocks(sub, acquire))?;
    }
    Ok(())
}

// The sublink leg C reaches via query_tree_walker: visit every SubLink's
// sub-Query in the query's expression-bearing fields (rtable/CTE subqueries
// are handled by the callers' own loops).
fn walk_sublink_queries<F>(query: &Query<'static>, f: &mut F) -> PgResult<()>
where
    F: FnMut(&Query<'static>) -> PgResult<()>,
{
    struct W<'a, F> {
        f: &'a mut F,
    }
    impl<F> nodes_core::NodeWalker<'static> for W<'_, F>
    where
        F: FnMut(&Query<'static>) -> PgResult<()>,
    {
        fn visit(&mut self, node: types_nodes::Node<'static>) -> PgResult<bool> {
            if let Some(sl) = node.as_sub_link() {
                (self.f)(sl.subselect.as_query().expect("analyzed sublink sub-select"))?;
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    fn walk_jt<F>(node: types_nodes::Node<'static>, w: &mut W<'_, F>) -> PgResult<()>
    where
        F: FnMut(&Query<'static>) -> PgResult<()>,
    {
        use nodes_core::NodeWalker as _;
        match node.node_tag() {
            types_nodes::NodeTag::T_RangeTblRef => {}
            types_nodes::NodeTag::T_FromExpr => {
                let fe = node.as_from_expr().expect("FromExpr");
                for child in &fe.fromlist {
                    walk_jt(child, w)?;
                }
                if let Some(q) = fe.quals {
                    w.visit(q)?;
                }
            }
            types_nodes::NodeTag::T_JoinExpr => {
                let j = node.as_join_expr().expect("JoinExpr");
                walk_jt(j.larg, w)?;
                walk_jt(j.rarg, w)?;
                if let Some(q) = j.quals {
                    w.visit(q)?;
                }
            }
            other => panic!("walk_sublink_queries (plancache.c): {other:?} jointree arm"),
        }
        Ok(())
    }
    use nodes_core::NodeWalker as _;
    let mut w = W { f };
    for te in &query.targetList {
        w.visit(te)?;
    }
    for te in &query.returningList {
        w.visit(te)?;
    }
    if let Some(jt) = query.jointree {
        for item in &jt.fromlist {
            walk_jt(item, &mut w)?;
        }
        if let Some(q) = jt.quals {
            w.visit(q)?;
        }
    }
    if let Some(h) = query.havingQual {
        w.visit(h)?;
    }
    if let Some(n) = query.limitOffset {
        w.visit(n)?;
    }
    if let Some(n) = query.limitCount {
        w.visit(n)?;
    }
    Ok(())
}

// extract_query_dependencies (setrefs.c), relation half: the function/inval
// half is compiled out repo-wide (record_plan_function_dependency panics).
fn extract_query_relation_deps(
    query: &Query<'static>,
    out: &mut PgVec<'static, Oid>,
) -> PgResult<()> {
    for cte_node in &query.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let ctequery = cte.ctequery.and_then(|n| n.as_query()).expect("analyzed CTE query");
        extract_query_relation_deps(ctequery, out)?;
    }
    if query.hasSubLinks {
        walk_sublink_queries(query, &mut |sub| extract_query_relation_deps(sub, out))?;
    }
    for rte_node in query.rtable.iter() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                out.try_reserve(1).map_err(|_| mcx_oom(out))?;
                out.push(rte.relid);
            }
            RTEKind::RTE_SUBQUERY => {
                if rte.relid != InvalidOid {
                    out.try_reserve(1).map_err(|_| mcx_oom(out))?;
                    out.push(rte.relid);
                }
                extract_query_relation_deps(
                    rte.subquery.expect("RTE_SUBQUERY has subquery"),
                    out,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn mcx_oom(v: &PgVec<'static, Oid>) -> Box<types_error::PgError> {
    Box::new(v.allocator().oom(core::mem::size_of::<Oid>()))
}

// ChoosePortalStrategy (pquery.c), Query flavor, feeding
// PlanCacheComputeResultDesc.
fn choose_portal_strategy_queries(query_list: &[Query<'static>]) -> PortalStrategy {
    use PortalStrategy::*;
    if query_list.len() == 1 {
        let query = &query_list[0];
        if query.canSetTag {
            if query.commandType == CmdType::CMD_SELECT {
                if query.hasModifyingCTE {
                    return PORTAL_ONE_MOD_WITH;
                }
                return PORTAL_ONE_SELECT;
            }
            if query.commandType == CmdType::CMD_UTILITY {
                let u = query.utilityStmt.expect("CMD_UTILITY query has utilityStmt");
                if utility_seams::utility_returns_tuples::call(u) {
                    return PORTAL_UTIL_SELECT;
                }
                return PORTAL_MULTI_QUERY;
            }
        }
    }

    let mut n_set_tag = 0i32;
    for query in query_list {
        if query.canSetTag {
            n_set_tag += 1;
            if n_set_tag > 1 {
                return PORTAL_MULTI_QUERY;
            }
            if query.commandType == CmdType::CMD_UTILITY || query.returningList.is_nil() {
                return PORTAL_MULTI_QUERY;
            }
        }
    }
    if n_set_tag == 1 {
        return PORTAL_ONE_RETURNING;
    }
    PORTAL_MULTI_QUERY
}

fn plan_cache_compute_result_desc(
    mcx: Mcx<'static>,
    query_list: &[Query<'static>],
) -> PgResult<Option<Rc<TupleDescData<'static>>>> {
    use PortalStrategy::*;
    match choose_portal_strategy_queries(query_list) {
        PORTAL_ONE_SELECT | PORTAL_ONE_MOD_WITH => Ok(Some(execscan::exec_clean_type_from_tl(
            mcx,
            &query_list[0].targetList,
        )?)),
        PORTAL_ONE_RETURNING => {
            let query = query_list
                .iter()
                .find(|q| q.canSetTag)
                .expect("ONE_RETURNING has a canSetTag query");
            Ok(Some(execscan::exec_clean_type_from_tl(mcx, &query.returningList)?))
        }
        PORTAL_UTIL_SELECT => {
            let u = query_list[0].utilityStmt.expect("CMD_UTILITY query has utilityStmt");
            utility_seams::utility_tuple_descriptor::call(u)
        }
        PORTAL_MULTI_QUERY => Ok(None),
    }
}

// IsTransactionExitStmt (postgres.c), captured at create because the raw
// parse tree is not retained.
fn is_transaction_exit_stmt(raw: &RawStmt<'_>) -> bool {
    use types_nodes::parsenodes::TransactionStmtKind::*;
    match raw.stmt.and_then(|node| node.as_transaction_stmt()) {
        Some(stmt) => matches!(
            stmt.kind,
            TRANS_STMT_COMMIT | TRANS_STMT_PREPARE | TRANS_STMT_ROLLBACK | TRANS_STMT_ROLLBACK_TO
        ),
        None => false,
    }
}

/// C CachedPlanGetTargetList: the primary query's targetlist, projected to the
/// row-description fields (exec_describe_statement_message's only consumer).
pub fn CachedPlanGetTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    h: CachedPlanSourceHandle,
    queryEnv: QueryEnvHandle,
) -> PgResult<PgVec<'mcx, pquery_seams::TargetEntrySummary>> {
    let mut out: PgVec<'mcx, pquery_seams::TargetEntrySummary> = PgVec::new_in(mcx);
    if with_source(h, |src| src.result_desc.is_none()) {
        return Ok(out);
    }

    RevalidateCachedQuery(h, queryEnv)?;

    let query_list = with_source(h, |src| src.query_list);
    let primary = query_list
        .iter()
        .find(|q| q.canSetTag)
        .expect("QueryListGetPrimaryStmt: fixed-result source has a canSetTag query");
    if primary.commandType == CmdType::CMD_UTILITY {
        panic!(
            "FetchStatementTargetList (pquery.c): utility statement targetlist \
             (FETCH/EXECUTE recursion) is the portalcmds lane"
        );
    }
    out.try_reserve(primary.targetList.len())
        .map_err(|_| mcx.oom(primary.targetList.len()))?;
    for node in primary.targetList.iter() {
        let tle = node.as_target_entry().expect("targetlist entry is a TargetEntry");
        out.push(pquery_seams::TargetEntrySummary {
            resjunk: tle.resjunk,
            resorigtbl: tle.resorigtbl,
            resorigcol: tle.resorigcol,
        });
    }
    Ok(out)
}

pub fn CachedPlanParamTypes(h: CachedPlanSourceHandle) -> &'static [Oid] {
    with_source(h, |src| src.param_types)
}

/// Valid while the caller holds the source handle (C: plansource->query_list).
pub fn CachedPlanQueryList(h: CachedPlanSourceHandle) -> &'static [Query<'static>] {
    with_source(h, |src| src.query_list)
}

pub fn CachedPlanRequiresSnapshot(h: CachedPlanSourceHandle) -> bool {
    with_source(h, |src| src.requires_snapshot)
}

pub fn CachedPlanIsTransactionExitStmt(h: CachedPlanSourceHandle) -> bool {
    with_source(h, |src| src.is_xact_exit_stmt)
}

/// (num_generic_plans, num_custom_plans) — the plan-cache-hit probe.
pub fn CachedPlanCounts(h: CachedPlanSourceHandle) -> (i64, i64) {
    with_source(h, |src| (src.num_generic_plans, src.num_custom_plans))
}

pub fn CachedPlanResultDesc(h: CachedPlanSourceHandle) -> Option<Rc<TupleDescData<'static>>> {
    with_source(h, |src| src.result_desc.clone())
}

pub fn CachedPlanCommandTag(h: CachedPlanSourceHandle) -> CommandTag {
    with_source(h, |src| src.commandTag)
}

pub fn CachedPlanQueryString(h: CachedPlanSourceHandle) -> &'static str {
    with_source(h, |src| src.query_string)
}

pub fn CachedPlanNumParams(h: CachedPlanSourceHandle) -> usize {
    with_source(h, |src| src.param_types.len())
}

pub fn CachedPlanFixedResult(h: CachedPlanSourceHandle) -> bool {
    with_source(h, |src| src.fixed_result)
}

#[derive(Clone, Copy)]
pub struct SourceExecInfo {
    pub fixed_result: bool,
    pub num_params: usize,
    pub query_string: &'static str,
    pub commandTag: CommandTag,
}

/// ExecuteQuery's per-EXECUTE field reads in one registry borrow (C reads
/// plansource fields directly).
pub fn CachedPlanSourceExecInfo(h: CachedPlanSourceHandle) -> SourceExecInfo {
    with_source(h, |src| SourceExecInfo {
        fixed_result: src.fixed_result,
        num_params: src.param_types.len(),
        query_string: src.query_string,
        commandTag: src.commandTag,
    })
}

pub fn CachedPlanGeneration(cplan: CachedPlanHandle) -> i32 {
    with_plan(cplan, |plan| plan.generation)
}

fn invalidate_source_entry(src: &mut CachedPlanSource) -> Option<CachedPlanHandle> {
    src.is_valid = false;
    src.gplan
}

fn invalidate_source(h: CachedPlanSourceHandle) {
    with_cache(|pc| {
        if let Some(gplan) = invalidate_source_entry(source_mut(pc, h)) {
            plan_mut(pc, gplan).is_valid = false;
        }
    });
}

pub fn PlanCacheRelCallback(_arg: Datum, relid: Oid) {
    bump_inval_counter();
    with_cache(|pc| {
        for i in 0..pc.saved_plan_list.len() {
            let h = pc.saved_plan_list[i];
            let (hit, gplan) = {
                let src = source_mut(pc, h);
                if !src.is_valid || !src.requires_reval {
                    continue;
                }
                let hit = if relid == InvalidOid {
                    !src.relation_oids.is_empty()
                } else {
                    src.relation_oids.contains(&relid)
                };
                if hit {
                    src.is_valid = false;
                }
                (hit, src.gplan)
            };
            let Some(gplan) = gplan else { continue };
            let plan = plan_mut(pc, gplan);
            if hit {
                plan.is_valid = false;
                continue;
            }
            // The generic plan can have more dependencies than the querytree.
            if plan.is_valid {
                let plan_hit = plan.stmt_list.iter().any(|stmt| {
                    stmt.commandType != CmdType::CMD_UTILITY
                        && if relid == InvalidOid {
                            !stmt.relationOids.is_nil()
                        } else {
                            stmt.relationOids.as_slice().contains(&relid)
                        }
                });
                if plan_hit {
                    plan.is_valid = false;
                }
            }
        }
    });
    // cached_expression_list is provably empty: GetCachedExpression defers loud.
}

pub fn PlanCacheObjectCallback(_arg: Datum, cacheid: i32, hashvalue: u32) {
    bump_inval_counter();
    with_cache(|pc| {
        for i in 0..pc.saved_plan_list.len() {
            let h = pc.saved_plan_list[i];
            let gplan = {
                let src = source_mut(pc, h);
                if !src.is_valid || !src.requires_reval {
                    continue;
                }
                // Source-side invalItems are uncollected: matching them would
                // invalidate the querytree, forcing re-analysis (the replan arm
                // needing the retained raw tree). The generic-plan scan below
                // re-plans the analyzed querytree, covering function redefinition.
                src.gplan
            };
            let Some(gplan) = gplan else { continue };
            let plan = plan_mut(pc, gplan);
            if plan.is_valid && stmt_list_matches_inval(plan.stmt_list, cacheid, hashvalue) {
                plan.is_valid = false;
            }
        }
    });
    // cached_expression_list is provably empty: GetCachedExpression defers loud.
}

// hashvalue == 0 matches every entry of the cache (C's cacheid-wide inval).
fn stmt_list_matches_inval(stmt_list: &[PlannedStmt<'_>], cacheid: i32, hashvalue: u32) -> bool {
    stmt_list.iter().any(|stmt| {
        stmt.commandType != CmdType::CMD_UTILITY
            && stmt.invalItems.iter().any(|node| {
                let item =
                    node.as_plan_inval_item().expect("invalItems holds PlanInvalItem");
                item.cacheId == cacheid && (hashvalue == 0 || item.hashValue == hashvalue)
            })
    })
}

pub fn PlanCacheSysCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    ResetPlanCache();
}

pub fn ResetPlanCache() {
    bump_inval_counter();
    with_cache(|pc| {
        for i in 0..pc.saved_plan_list.len() {
            let h = pc.saved_plan_list[i];
            let src = source_mut(pc, h);
            // Never invalidate what a new parse/plan cycle can't change —
            // particularly ROLLBACK (C bug #5269).
            if !src.is_valid || !src.requires_reval {
                continue;
            }
            if let Some(gplan) = invalidate_source_entry(src) {
                plan_mut(pc, gplan).is_valid = false;
            }
        }
    });
}

pub fn GetCachedExpression() -> ! {
    panic!("GetCachedExpression (plancache.c) deferred: cached expressions unported");
}
