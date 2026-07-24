// plancache.c. Sources/plans live in per-entry leaked MemoryContexts, reclaimed
// on drop; registry handles are generation-checked (C's dangling pointer made
// loud). CachedPlan.refcount IS C's refcount. The raw parse tree is retained on
// the source (C's copyObject into source_context) and the invalidated-replan
// arm re-analyzes a per-attempt copy of it: through the installed ReanalyzeFn
// when the owner's analysis needs parse hooks (C's parserSetup), else through
// the fixedparams default (C plancache.c:793-814). Query-side (source)
// invalItems come from extract_query_deps (extract_query_dependencies) and
// invalidate the source, forcing re-analysis; the generic plan's own
// invalItems (recorded by setrefs) drive PlanCacheObjectCallback's
// generic-plan arm. Divergence: RLS fields are constant-false.
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

const REGCLASSOID: Oid = 2205;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CachedPlanSourceHandle(pub u64);

/// Analyze-and-rewrite `raw` (a per-attempt copy of the retained raw parse
/// tree, allocated in `qmcx`; analysis may scribble on it) into `qmcx`;
/// `arg` is C's parserSetupArg. Install only when analysis needs parse hooks
/// the fixedparams default cannot carry (C's parserSetup != NULL arm).
/// `h` is our safe spelling of the rest of C's parserSetupArg: the owner
/// resolves its own per-source state from the handle.
pub type ReanalyzeFn = fn(
    h: CachedPlanSourceHandle,
    qmcx: Mcx<'static>,
    raw: &'static RawStmt<'static>,
    query_string: &'static str,
    param_types: &'static [Oid],
    query_env: QueryEnvHandle,
    arg: i32,
) -> PgResult<PgVec<'static, Query<'static>>>;

pub fn SetCachedPlanReanalyze(h: CachedPlanSourceHandle, f: ReanalyzeFn, arg: i32) {
    with_source(h, |src| src.reanalyze = Some((f, arg)));
}

/// C's PostRewriteHook (plancache.h / SetPostRewriteHook): runs on the
/// transient rewritten query list produced by the re-analysis arm, before the
/// source adopts it — the owner's chance to re-apply whatever it did to the
/// list at CompleteCachedPlan time (SQL functions re-run their statement
/// checks and re-insert the result coercions). May rewrite the list in place,
/// exactly as C's `List *querytree_list` hook does. `arg` is C's
/// postRewriteArg, `h` identifies the source (see [`ReanalyzeFn`]).
pub type PostRewriteFn = fn(
    h: CachedPlanSourceHandle,
    qmcx: Mcx<'static>,
    query_list: &mut PgVec<'static, Query<'static>>,
    arg: i32,
) -> PgResult<()>;

/// C SetPostRewriteHook (plancache.c:504-512). Must be called before the
/// source is saved: the hook is part of how the source rebuilds itself.
pub fn SetCachedPlanPostRewrite(h: CachedPlanSourceHandle, f: PostRewriteFn, arg: i32) {
    with_source(h, |src| src.post_rewrite = Some((f, arg)));
}

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
    // Lives in source_ctx (C: copyObject into source_context); handed out
    // only as copies because analysis scribbles on its input.
    raw_parse_tree: Option<&'static RawStmt<'static>>,
    // C's analyzed_parse_tree: the pre-rewrite Query a CreateCachedPlanForQuery
    // source was built from, also copied into source_ctx. Exactly one of the
    // two trees is retained (an empty statement retains neither).
    analyzed_parse_tree: Option<&'static Query<'static>>,
    reanalyze: Option<(ReanalyzeFn, i32)>,
    post_rewrite: Option<(PostRewriteFn, i32)>,
    query_list: &'static [Query<'static>],
    relation_oids: &'static [Oid],
    // extract_query_dependencies' invalItems (cacheId, hashValue): a matching
    // sinval invalidates the source, forcing re-analysis of the raw tree.
    inval_items: &'static [(i32, u32)],
    search_path: Option<SearchPathMatcher<'static>>,
    result_desc: Option<Rc<TupleDescData<'static>>>,
    gplan: Option<CachedPlanHandle>,
    depends_on_rls: bool,
    rewrite_role_id: Oid,
    rewrite_row_security: bool,
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
    // TRUE once ReleaseAllCachedPlansAtExit reclaimed the registry. C never
    // unpins at process death — CacheMemoryContext just dies with the process
    // and outstanding refcounts are abandoned. The thread model reclaims the
    // estate in an on_proc_exit callback, which runs BEFORE the session
    // teardown phases (launch_backend: run_deferred_exit_callbacks, then
    // mcxt_stats::run_session_teardown), so late pin holders — the parked
    // executor skeleton (execmain, State phase) and parked portal shells
    // (portalmem, Portals phase) — still release their handles afterwards.
    // Those releases are C's abandoned refcounts: no-ops, never staleness.
    torn_down: bool,
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
            torn_down: false,
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
    plancache_portal_seams::incr_cached_plan::set(IncrCachedPlan);
    plancache_portal_seams::is_source_generic_plan::set(CachedPlanIsSourceGeneric);
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

// Backend-exit reclaim of every surviving plancache context. C frees these by
// process death (plancache.c allocates under CacheMemoryContext); a
// thread-per-connection backend must free them itself or every prepared
// statement's CachedPlanSource/CachedPlanQuery/CachedPlan contexts leak into
// the postmaster's heap for the server's lifetime. Runs as an on_proc_exit
// callback: portals and transactions are already torn down
// (shutdown_postgres_cb), so the registry structs are the last owners; their
// arena-tied fields (search_path, result_desc) drop before their arenas are
// reclaimed.
fn ReleaseAllCachedPlansAtExit(_code: i32, _arg: usize) {
    let ctxs = with_cache(|pc| {
        let mut ctxs = Vec::new();
        for slot in pc.plans.iter_mut() {
            if let Some(plan) = slot.take() {
                ctxs.push(plan.plan_ctx);
            }
        }
        for slot in pc.sources.iter_mut() {
            if let Some(src) = slot.take() {
                ctxs.push(src.query_ctx);
                ctxs.push(src.source_ctx);
            }
        }
        pc.plans.clear();
        pc.plan_free.clear();
        pc.sources.clear();
        pc.source_free.clear();
        pc.saved_plan_list.clear();
        pc.torn_down = true;
        ctxs
    });
    for ctx in ctxs {
        reclaim_ctx(ctx);
    }
}

pub fn InitPlanCache() -> PgResult<()> {
    // A fresh session on a reused thread starts with a live registry.
    with_cache(|pc| pc.torn_down = false);
    // installed() guard: unit-test rigs call InitPlanCache without ipc.
    if ipc_seams::on_proc_exit::is_installed() {
        ipc_seams::on_proc_exit::call(ReleaseAllCachedPlansAtExit, 0);
    }
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
    let flags = match raw_parse_tree {
        Some(raw) => (
            parser_analyze::stmt_requires_parse_analysis(raw),
            parser_analyze::analyze_requires_snapshot(raw),
            is_transaction_exit_stmt(raw),
        ),
        None => (false, false, false),
    };
    create_cached_plan_flags(raw_parse_tree, None, flags, query_string, commandTag)
}

// CreateCachedPlanForQuery (plancache.c:255-277): entry created from an
// analyzed Query (SQL-function prosqlbody); revalidation/snapshot flags come
// from query_requires_rewrite_plan, not the raw-tree probe. C copies the tree
// into the source context (`plansource->analyzed_parse_tree =
// copyObject(...)`) because that copy is what the re-rewrite arm of
// RevalidateCachedQuery works from.
pub fn CreateCachedPlanForQuery(
    analyzed: &Query<'_>,
    query_string: &str,
    commandTag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    let reval = parser_analyze::query_requires_rewrite_plan(analyzed);
    create_cached_plan_flags(None, Some(analyzed), (reval, reval, false), query_string, commandTag)
}

fn create_cached_plan_flags(
    raw_parse_tree: Option<&RawStmt<'_>>,
    analyzed_parse_tree: Option<&Query<'_>>,
    (requires_reval, requires_snapshot, is_xact_exit_stmt): (bool, bool, bool),
    query_string: &str,
    commandTag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    let source_ctx = leak_ctx("CachedPlanSource");
    let query_ctx = leak_ctx("CachedPlanQuery");
    let mcx = ctx_mcx(source_ctx);
    let qs = mcx::slice_borrow_in(mcx, query_string.as_bytes())?;
    let query_string: &'static str = core::str::from_utf8(qs).expect("query_string is UTF-8");
    let mut bail = |e| {
        reclaim_ctx(query_ctx);
        reclaim_ctx(source_ctx);
        e
    };
    let raw_parse_tree = match raw_parse_tree {
        Some(raw) => match copy_raw_stmt_in(mcx, raw) {
            Ok(r) => Some(r),
            Err(e) => return Err(bail(e)),
        },
        None => None,
    };
    let analyzed_parse_tree = match analyzed_parse_tree {
        Some(q) => match copyfuncs::copy_query(mcx, q).and_then(|c| mcx::alloc_leak_in(mcx, c)) {
            Ok(q) => Some(&*q),
            Err(e) => return Err(bail(e)),
        },
        None => None,
    };

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
            raw_parse_tree,
            analyzed_parse_tree,
            reanalyze: None,
            post_rewrite: None,
            query_list: &[],
            relation_oids: &[],
            inval_items: &[],
            search_path: None,
            result_desc: None,
            gplan: None,
            depends_on_rls: false,
            rewrite_role_id: InvalidOid,
            rewrite_row_security: false,
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

    let (relation_oids, inval_items, search_path) = if requires_reval {
        let mut oids: PgVec<'static, Oid> = PgVec::new_in(query_mcx);
        let mut items: PgVec<'static, (i32, u32)> = PgVec::new_in(query_mcx);
        for q in query_list {
            extract_query_deps(q, &mut oids, &mut items)?;
        }
        (
            mcx::vec_borrow_in(query_mcx, oids)?,
            mcx::vec_borrow_in(query_mcx, items)?,
            Some(catalog_namespace::GetSearchPathMatcher(query_mcx)?),
        )
    } else {
        (&[] as &[Oid], &[] as &[(i32, u32)], None)
    };

    let result_desc = plan_cache_compute_result_desc(source_mcx, query_list)?;
    let param_types: &'static [Oid] = mcx::slice_borrow_in(source_mcx, param_types)?;

    let depends_on_rls = query_list.iter().any(|q| q.hasRowSecurity);
    let rewrite_role_id = miscinit::GetUserId();
    let rewrite_row_security = guc_tables::backing::row_security();
    with_source(h, |src| {
        src.query_list = query_list;
        src.depends_on_rls = depends_on_rls;
        src.rewrite_role_id = rewrite_role_id;
        src.rewrite_row_security = rewrite_row_security;
        src.relation_oids = relation_oids;
        src.inval_items = inval_items;
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
    // Post-exit-reclaim drops (e.g. a cache teardown running after the
    // on_proc_exit ceremony) are C's process-death abandonment: no-ops.
    if with_cache(|pc| pc.torn_down) {
        return;
    }
    if plancache_portal_seams::discard_parked_portal::is_installed() {
        plancache_portal_seams::discard_parked_portal::call(types_portal::PlanSourceHandle(h.0));
    }
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
        // A release arriving after ReleaseAllCachedPlansAtExit reclaimed the
        // registry (parked executor skeleton / parked portal shells, whose
        // session-teardown cleanups run after the exit-callback ceremony) is
        // an abandoned refcount in C's process model: drop it silently.
        if pc.torn_down {
            return Vec::new();
        }
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

// Extra refcount on a live plan; pairs with ReleaseCachedPlan.
pub fn IncrCachedPlan(cplan: CachedPlanHandle) {
    with_cache(|pc| {
        let plan = plan_mut(pc, cplan);
        assert!(plan.refcount > 0, "IncrCachedPlan: plan already freed");
        plan.refcount += 1;
    });
}

// True iff cplan is its source's current generic plan — the only plan a
// later GetCachedPlan can return unchanged; one-shot custom plans never
// recur. Caller must hold a refcount on cplan.
pub fn CachedPlanIsSourceGeneric(cplan: CachedPlanHandle) -> bool {
    with_cache(|pc| {
        let src = plan_mut(pc, cplan).source;
        let (idx, gen) = decode(src.0);
        pc.sources
            .get(idx)
            .and_then(Option::as_ref)
            .is_some_and(|s| s.handle_gen == gen && s.gplan == Some(cplan))
    })
}

pub fn CachedPlanIsValid(h: CachedPlanSourceHandle) -> bool {
    with_source(h, |src| src.is_valid)
}

// Source-validity probe for owners that can rebuild from retained SQL (the
// plpgsql SPI-plan path): C revalidates in place from the raw tree.
pub fn CachedPlanSourceIsValid(h: CachedPlanSourceHandle) -> bool {
    with_cache(|pc| source_mut(pc, h).is_valid)
}

// RevalidateCachedQuery's pre-lock screen (invalidation, search_path mismatch,
// RLS environment change) as a pure probe, for owners whose analysis needs
// parse hooks a ReanalyzeFn cannot carry (plpgsql exprs, SQL-function fcache).
pub fn CachedPlanSourceRequiresReanalysis(h: CachedPlanSourceHandle) -> PgResult<bool> {
    let (requires_reval, is_valid, depends_on_rls, role, row_sec) = with_source(h, |src| {
        (
            src.requires_reval,
            src.is_valid,
            src.depends_on_rls,
            src.rewrite_role_id,
            src.rewrite_row_security,
        )
    });
    if !requires_reval {
        return Ok(false);
    }
    if !is_valid {
        return Ok(true);
    }
    let mut matcher = with_source(h, |src| src.search_path.take());
    let matches = match matcher.as_mut() {
        Some(m) => catalog_namespace::SearchPathMatchesCurrentEnvironment(m),
        None => panic!(
            "CachedPlanSourceRequiresReanalysis: valid revalidatable source lost its search_path"
        ),
    };
    with_source(h, |src| src.search_path = matcher);
    if !matches? {
        return Ok(true);
    }
    Ok(depends_on_rls
        && (role != miscinit::GetUserId() || row_sec != guc_tables::backing::row_security()))
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

/// Valid while the source stays saved and valid (C: plansource->query_list;
/// callers must have just revalidated via GetCachedPlan).
pub fn SourceQueryList(h: CachedPlanSourceHandle) -> &'static [Query<'static>] {
    with_source(h, |src| src.query_list)
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

    RevalidateCachedQuery(h, queryEnv, true)?;

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

fn RevalidateCachedQuery(
    h: CachedPlanSourceHandle,
    queryEnv: QueryEnvHandle,
    release_generic: bool,
) -> PgResult<()> {
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

    // The rewrite had an RLS dependency: redo it if the role or the
    // row_security setting changed (C plancache.c:714).
    if with_source(h, |src| src.is_valid && src.depends_on_rls)
        && with_source(h, |src| {
            src.rewrite_role_id != miscinit::GetUserId()
                || src.rewrite_row_security != guc_tables::backing::row_security()
        })
    {
        invalidate_source(h);
    }

    if let Some(query_list) = with_source(h, |src| src.is_valid.then_some(src.query_list)) {
        AcquirePlannerLocks(query_list, true)?;
        if with_source(h, |src| src.is_valid) {
            return Ok(());
        }
        AcquirePlannerLocks(query_list, false)?;
    }

    let (reanalyze, post_rewrite, raw_parse_tree, analyzed_parse_tree) = with_source(h, |src| {
        (src.reanalyze, src.post_rewrite, src.raw_parse_tree, src.analyzed_parse_tree)
    });

    // An analysis error below leaves the source invalid with empty lists
    // (retried on the next fetch), exactly as C's longjmp.
    if release_generic {
        ReleaseGenericPlan(h);
    }
    let (old_qctx, query_string, param_types, fixed_result, source_ctx) = with_cache(|pc| {
        let src = source_mut(pc, h);
        src.is_valid = false;
        src.query_list = &[];
        src.relation_oids = &[];
        src.inval_items = &[];
        src.search_path = None;
        (src.query_ctx, src.query_string, src.param_types, src.fixed_result, src.source_ctx)
    });

    // Re-analysis probes catalogs: C pushes a transaction snapshot if none set.
    let mut snapshot_set = false;
    if !snapmgr::ActiveSnapshotSet() {
        let snap: snapmgr::Snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
        snapshot_set = true;
    }
    let new_qctx = leak_ctx("CachedPlanQuery");
    let qmcx = ctx_mcx(new_qctx);
    // The three source shapes of C plancache.c:790-830, in C's order. Both
    // retained-tree arms work from a per-attempt copy (C's copyObject):
    // analysis and the rewriter both scribble on their input, and a failed
    // attempt must not poison the retained original.
    let analyzed = match (raw_parse_tree, analyzed_parse_tree) {
        (Some(raw), _) => copy_raw_stmt_in(qmcx, raw).and_then(|raw| match reanalyze {
            Some((f, arg)) => f(h, qmcx, raw, query_string, param_types, queryEnv, arg),
            None => reanalyze_fixedparams(qmcx, raw, query_string, param_types, queryEnv),
        }),
        (None, Some(q)) => rewrite_analyzed(qmcx, q),
        // Empty query: C leaves tlist NIL. Unreachable in practice, since an
        // empty statement never requires revalidation.
        (None, None) => Ok(PgVec::new_in(qmcx)),
    }
    // C plancache.c:834-836, before the snapshot is popped: the postRewrite
    // hook may probe catalogs (SQL functions re-run check_sql_stmt_retval).
    .and_then(|mut query_list| match post_rewrite {
        Some((f, arg)) => f(h, qmcx, &mut query_list, arg).map(|()| query_list),
        None => Ok(query_list),
    });
    if snapshot_set {
        if let Err(e) = snapmgr::PopActiveSnapshot() {
            drop(analyzed);
            reclaim_ctx(new_qctx);
            return Err(e);
        }
    }
    let rebuilt = analyzed.and_then(|query_list| {
        let query_list: &'static [Query<'static>] = mcx::vec_borrow_in(qmcx, query_list)?;
        let mut oids: PgVec<'static, Oid> = PgVec::new_in(qmcx);
        let mut items: PgVec<'static, (i32, u32)> = PgVec::new_in(qmcx);
        for q in query_list {
            extract_query_deps(q, &mut oids, &mut items)?;
        }
        let relation_oids = mcx::vec_borrow_in(qmcx, oids)?;
        let inval_items = mcx::vec_borrow_in(qmcx, items)?;
        let search_path = catalog_namespace::GetSearchPathMatcher(qmcx)?;
        let result_desc = plan_cache_compute_result_desc(ctx_mcx(source_ctx), query_list)?;
        Ok((query_list, relation_oids, inval_items, search_path, result_desc))
    });
    let (query_list, relation_oids, inval_items, search_path, result_desc) = match rebuilt {
        Ok(r) => r,
        Err(e) => {
            reclaim_ctx(new_qctx);
            return Err(e);
        }
    };

    let old_desc = with_source(h, |src| src.result_desc.clone());
    let desc_equal = match (&old_desc, &result_desc) {
        (None, None) => true,
        (Some(o), Some(n)) => tupdesc::equalRowTypes(o, n),
        _ => false,
    };
    if !desc_equal && fixed_result {
        // search_path's schemas PgVec lives in new_qctx: it must be dead
        // before the arena is reclaimed or its Drop scribbles a freed slab.
        drop(search_path);
        reclaim_ctx(new_qctx);
        // routine=RevalidateCachedQuery is load-bearing: pgjdbc and
        // postgres.js key their transparent re-prepare-after-DDL on this
        // exact R field (C's __func__ at the equivalent report site).
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cached plan must not change result type")
            .into_error()
            .with_funcname("RevalidateCachedQuery")
            .into());
    }

    let depends_on_rls = query_list.iter().any(|q| q.hasRowSecurity);
    let rewrite_role_id = miscinit::GetUserId();
    let rewrite_row_security = guc_tables::backing::row_security();
    with_cache(|pc| {
        let src = source_mut(pc, h);
        src.query_ctx = new_qctx;
        src.query_list = query_list;
        src.relation_oids = relation_oids;
        src.inval_items = inval_items;
        src.search_path = Some(search_path);
        if !desc_equal {
            src.result_desc = result_desc;
        }
        src.depends_on_rls = depends_on_rls;
        src.rewrite_role_id = rewrite_role_id;
        src.rewrite_row_security = rewrite_row_security;
        src.is_valid = true;
    });
    // Custom plans share query-arena subnodes (see `dead`): the old arena is
    // reclaimed only when no live plan can still reference it.
    if with_cache(|pc| !pc.plans.iter().flatten().any(|p| p.source == h)) {
        reclaim_ctx(old_qctx);
    }
    Ok(())
}

// copyObject(raw_parse_tree): the RawStmt shell plus its full statement tree.
fn copy_raw_stmt_in<'mcx>(mcx: Mcx<'mcx>, raw: &RawStmt<'_>) -> PgResult<&'mcx RawStmt<'mcx>> {
    let stmt = match raw.stmt {
        Some(s) => Some(copyfuncs::copy_object(mcx, s)?),
        None => None,
    };
    mcx::alloc_leak_in(
        mcx,
        RawStmt { stmt, stmt_location: raw.stmt_location, stmt_len: raw.stmt_len },
    )
}

/// Copy of the source's retained raw parse tree into `mcx`; None for sources
/// created without one (empty statements, CreateCachedPlanForQuery). Copies
/// only — analysis scribbles on its input (C reads plansource->raw_parse_tree
/// and copyObjects before use).
pub fn CachedPlanRawParseTreeCopy<'mcx>(
    mcx: Mcx<'mcx>,
    h: CachedPlanSourceHandle,
) -> PgResult<Option<&'mcx RawStmt<'mcx>>> {
    match with_source(h, |src| src.raw_parse_tree) {
        Some(raw) => Ok(Some(copy_raw_stmt_in(mcx, raw)?)),
        None => Ok(None),
    }
}

// The analyzed_parse_tree revalidation arm (C plancache.c:816-827): the
// source was built from a Query that is already parse-analyzed, so only the
// rewrite is redone — preceded by the lock acquisition the rewriter needs,
// which is what makes this arm safe to plan from afterwards.
fn rewrite_analyzed(
    qmcx: Mcx<'static>,
    analyzed: &Query<'static>,
) -> PgResult<PgVec<'static, Query<'static>>> {
    // The rewriter scribbles on its input, so copy (C's copyObject).
    let copied: Query<'static> = copyfuncs::copy_query(qmcx, analyzed)?;
    rewrite_handler_seams::acquire_rewrite_locks::call(qmcx, &copied, true, false)?;
    // pg_rewrite_query's own utility arm (a utility Query has an empty rtable,
    // so the lock walk above is a no-op for it, exactly as in C).
    if copied.commandType == CmdType::CMD_UTILITY {
        let mut v = PgVec::new_in(qmcx);
        v.try_reserve_exact(1).map_err(|_| qmcx.oom(1))?;
        v.push(copied);
        return Ok(v);
    }
    rewrite_handler_seams::query_rewrite::call(qmcx, copied)
}

// pg_analyze_and_rewrite_fixedparams (tcop/postgres.c) via the seams, the
// parserSetup == NULL revalidation arm (C plancache.c:807-814).
fn reanalyze_fixedparams(
    qmcx: Mcx<'static>,
    raw: &RawStmt<'static>,
    query_string: &'static str,
    param_types: &'static [Oid],
    query_env: QueryEnvHandle,
) -> PgResult<PgVec<'static, Query<'static>>> {
    let query = analyze_seams::parse_analyze_fixedparams::call(
        qmcx,
        raw,
        query_string,
        param_types,
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
    queryEnv: QueryEnvHandle,
) -> PgResult<CachedPlanHandle> {
    // Invalidated since GetCachedPlan's revalidation (sinval-reset race):
    // re-analyze, without releasing the generic plan the caller may hold (C
    // plancache.c BuildCachedPlan's release_generic = false).
    if !with_source(h, |src| src.is_valid) {
        RevalidateCachedQuery(h, queryEnv, false)?;
    }
    let (query_list, query_string, cursor_options, requires_snapshot) = with_source(h, |src| {
        (src.query_list, src.query_string, src.cursor_options, src.requires_snapshot)
    });

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

    let mut depends_on_role = with_source(h, |src| src.depends_on_rls);
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
                    queryId: ::types_nodes::SyncCell::new(q.queryId),
                    ..PlannedStmt::default()
                });
            } else {
                let input = mcx::leak_in(mcx::alloc_in(mcx, copy_query_in(mcx, q)?)?);
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

// BuildCachedPlan's copyObject boundary: the planner writes plan-arena
// pointers into its input tree; a subtree shared with the cached query_list
// dangles once that plan's arena is reclaimed, so the copy must share no
// storage with the source.
fn copy_query_in(mcx: Mcx<'static>, q: &Query<'static>) -> PgResult<Query<'static>> {
    copyfuncs::copy_query(mcx, q)
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
            let contained = query
                .utilityStmt
                .and_then(|s| utility_seams::utility_contains_query::call(s));
            if let Some(q) = contained {
                ScanQueryForLocks(q.as_query().expect("contained Query"), acquire)?;
            }
            continue;
        }
        ScanQueryForLocks(query, acquire)?;
    }
    Ok(())
}

fn AcquireExecutorLocks(stmt_list: &[PlannedStmt<'static>], acquire: bool) -> PgResult<()> {
    for stmt in stmt_list {
        if stmt.commandType == CmdType::CMD_UTILITY {
            // C: unplanned Query inside EXPLAIN/DECLARE still needs its locks.
            let contained = stmt
                .utilityStmt
                .and_then(|s| utility_seams::utility_contains_query::call(s));
            if let Some(q) = contained {
                ScanQueryForLocks(q.as_query().expect("contained Query"), acquire)?;
            }
            continue;
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
    walk_query_expr_nodes(query, &mut |node| {
        if let Some(sl) = node.as_sub_link() {
            f(sl.subselect.as_query().expect("analyzed sublink sub-select"))?;
        }
        Ok(())
    })
}

// query_tree_walker's expression leg: the callback sees every expression node
// in the query's expression-bearing fields (rtable/CTE subqueries are the
// callers' own loops).
fn walk_query_expr_nodes<F>(query: &Query<'static>, f: &mut F) -> PgResult<()>
where
    F: FnMut(types_nodes::Node<'static>) -> PgResult<()>,
{
    struct W<'a, F> {
        f: &'a mut F,
    }
    impl<F> nodes_core::NodeWalker<'static> for W<'_, F>
    where
        F: FnMut(types_nodes::Node<'static>) -> PgResult<()>,
    {
        fn visit(&mut self, node: types_nodes::Node<'static>) -> PgResult<bool> {
            (self.f)(node)?;
            nodes_core::expression_tree_walker(node, self)
        }
    }
    fn walk_jt<F>(node: types_nodes::Node<'static>, w: &mut W<'_, F>) -> PgResult<()>
    where
        F: FnMut(types_nodes::Node<'static>) -> PgResult<()>,
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
            other => panic!("walk_query_expr_nodes (plancache.c): {other:?} jointree arm"),
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

// FirstUnpinnedObjectId: built-ins below it are immutable, untracked
// (record_plan_function_dependency, setrefs.c).
const FIRST_UNPINNED_OBJECT_ID: Oid = 12000;

fn syscache_oid_hash(cache_id: i32, oid: Oid) -> PgResult<u32> {
    cache_syscache::GetSysCacheHashValue(
        cache_id,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(oid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )
}

fn push_proc_dep(items: &mut PgVec<'static, (i32, u32)>, funcid: Oid) -> PgResult<()> {
    if funcid < FIRST_UNPINNED_OBJECT_ID {
        return Ok(());
    }
    let hash = syscache_oid_hash(PROCOID, funcid)?;
    items
        .try_reserve(1)
        .map_err(|_| Box::new(items.allocator().oom(core::mem::size_of::<(i32, u32)>())))?;
    items.push((PROCOID, hash));
    Ok(())
}

// extract_query_dependencies (setrefs.c): relation OIDs + the fix_expr_common
// function/type invalItems.
fn extract_query_deps(
    query: &Query<'static>,
    out: &mut PgVec<'static, Oid>,
    items: &mut PgVec<'static, (i32, u32)>,
) -> PgResult<()> {
    for cte_node in &query.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        let ctequery = cte.ctequery.and_then(|n| n.as_query()).expect("analyzed CTE query");
        extract_query_deps(ctequery, out, items)?;
    }
    // extract_query_dependencies_walker runs fix_expr_common on every node:
    // a regclass Const is a relation dependency (ISREGCLASSCONST accepts
    // OIDOID because oideq-style folding coerces regclass Consts to it);
    // function-calling nodes are PROCOID invalItems, CoerceToDomain a TYPEOID
    // one.
    walk_query_expr_nodes(query, &mut |node| {
        if let Some(c) = node.as_const() {
            if (c.consttype == REGCLASSOID || c.consttype == types_core::OIDOID)
                && !c.constisnull
            {
                out.try_reserve(1).map_err(|_| mcx_oom(out))?;
                out.push(c.constvalue.as_u32());
            }
        } else if let Some(sl) = node.as_sub_link() {
            extract_query_deps(
                sl.subselect.as_query().expect("analyzed sublink sub-select"),
                out,
                items,
            )?;
        } else if let Some(f) = node.as_func_expr() {
            push_proc_dep(items, f.funcid)?;
        } else if let Some(o) = node.as_op_expr() {
            push_proc_dep(items, o.opfuncid)?;
        } else if let Some(o) = node.as_distinct_expr() {
            push_proc_dep(items, o.opfuncid)?;
        } else if let Some(o) = node.as_null_if_expr() {
            push_proc_dep(items, o.opfuncid)?;
        } else if let Some(sa) = node.as_scalar_array_op_expr() {
            push_proc_dep(items, sa.opfuncid)?;
        } else if let Some(a) = node.as_aggref() {
            push_proc_dep(items, a.aggfnoid)?;
        } else if let Some(w) = node.as_window_func() {
            push_proc_dep(items, w.winfnoid)?;
        } else if let Some(cd) = node.as_coerce_to_domain() {
            if cd.resulttype >= FIRST_UNPINNED_OBJECT_ID {
                let hash = syscache_oid_hash(TYPEOID, cd.resulttype)?;
                items.try_reserve(1).map_err(|_| {
                    Box::new(items.allocator().oom(core::mem::size_of::<(i32, u32)>()))
                })?;
                items.push((TYPEOID, hash));
            }
        }
        Ok(())
    })?;
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
                extract_query_deps(
                    rte.subquery.expect("RTE_SUBQUERY has subquery"),
                    out,
                    items,
                )?;
            }
            // A transition-table RTE carries the base relation's OID
            // (addRangeTableEntryForENR: rte->relid = enrmd->reliddesc) so
            // ALTERing that table invalidates dependent plans
            // (setrefs.c:3720-3724).
            RTEKind::RTE_NAMEDTUPLESTORE => {
                if rte.relid != InvalidOid {
                    out.try_reserve(1).map_err(|_| mcx_oom(out))?;
                    out.push(rte.relid);
                }
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

// IsTransactionExitStmt (postgres.c), captured at create (C recomputes from
// the retained raw tree on each read; the answer never changes).
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

    RevalidateCachedQuery(h, queryEnv, true)?;

    let query_list = with_source(h, |src| src.query_list);
    let primary = query_list
        .iter()
        .find(|q| q.canSetTag)
        .expect("QueryListGetPrimaryStmt: fixed-result source has a canSetTag query");
    if primary.commandType == CmdType::CMD_UTILITY {
        // FETCH/EXECUTE recursion needs pquery's portal registry and
        // prepare's statement registry; plancache can't depend on either
        // without a cycle back through this function, so it's a seam.
        return pquery_seams::fetch_utility_statement_target_list::call(mcx, primary.utilityStmt);
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
                // Source-side invalItems (extract_query_dependencies):
                // invalidate the querytree, forcing re-analysis of the
                // retained raw tree (plancache.c PlanCacheObjectCallback).
                if src.inval_items.iter().any(|&(cid, hv)| {
                    cid == cacheid && (hashvalue == 0 || hv == hashvalue)
                }) {
                    if let Some(gplan) = invalidate_source_entry(src) {
                        plan_mut(pc, gplan).is_valid = false;
                    }
                    continue;
                }
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
