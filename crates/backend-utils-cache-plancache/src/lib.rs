//! `utils/cache/plancache.c` — plan cache management (prepared-statement /
//! cached-plan lifecycle), idiomatic port of PostgreSQL 18.3.
//!
//! The plancache *algorithm* lives here: the backend-global saved-source list
//! (`saved_plan_list`) and cached-expression list (`cached_expression_list`),
//! the `CachedPlan` reference counting, the generic/custom plan cost
//! accounting (`choose_custom_plan`/`cached_plan_cost`), the result-tupdesc
//! bookkeeping, and the full control flow (branch order, conditions, loops,
//! assertions, error messages) of every revalidation / invalidation / build
//! routine. Only calls into *other* subsystems cross a seam.
//!
//! ## Modeling the shared-mutable cache graph
//!
//! C shares the same `CachedPlan *` simultaneously between `plansource->gplan`,
//! the caller's returned pointer, and a `ResourceOwner`, and the backend-global
//! lists hold `CachedPlanSource *`s that invalidation callbacks mutate in place
//! while callers still hold them. That shape is modeled with `Rc<RefCell<…>>`
//! interned in a [`thread_local!`] registry keyed by a stable `u64` handle; the
//! public entry points take/return those handles exactly as the C entry points
//! take/return `CachedPlanSource *` / `CachedPlan *`. The C reference count
//! (`CachedPlan.refcount`) stays authoritative; the registry `Rc` only keeps
//! the data reachable until the refcount and all links drop, at which point the
//! entry is removed (`MemoryContextDelete` crosses the mctx seam for the real
//! storage).
//!
//! The saved-source / cached-expression lists are a backend-lifetime
//! thread-local global, so their owned `String`/`Vec` fields use plain owned
//! collections (`docs/mctx-design.md` decision 5), not `Mcx`-parameterized
//! ones.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use types_core::primitive::{Oid, INVALID_OID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_plancache::{
    AnalyzedQueryHandle, CommandTag, CtxId, ExprHandle, InvalItemKey, ParamListInfoHandle,
    ParserSetupHandle, PlannedStmtListHandle, PortalStrategy, PostRewriteHandle,
    QueryEnvHandle, QueryHandle, QueryListHandle, RawStmtHandle, ResourceOwnerHandle,
    SearchPathMatcherHandle, SysCacheId, TargetListHandle, TupleDescHandle, CACHEDEXPR_MAGIC,
    CACHEDPLANSOURCE_MAGIC, CACHEDPLAN_MAGIC, CURSOR_OPT_CUSTOM_PLAN,
    CURSOR_OPT_GENERIC_PLAN, FIRST_NORMAL_TRANSACTION_ID, PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN,
    PLAN_CACHE_MODE_FORCE_GENERIC_PLAN, RTE_RELATION, RTE_SUBQUERY,
};

use backend_access_common_tupdesc_pc_seams as tupdesc_seams;
use backend_catalog_namespace_pc_seams as namespace_seams;
use backend_nodes_copyfuncs_pc_seams as node_seams;
use backend_optimizer_plan_planner_pc_seams as planner_seams;
use backend_parser_analyze_pc_seams as analyze_seams;
use backend_rewrite_rewriteHandler_pc_seams as rewrite_seams;
use backend_storage_lmgr_lmgr_pc_seams as lmgr_seams;
use backend_tcop_pquery_pc_seams as pquery_seams;
use backend_tcop_utility_pc_seams as utility_seams;
use backend_utils_cache_inval_pc_seams as inval_seams;
use backend_utils_cache_syscache_pc_seams as syscache_seams;
use backend_utils_misc_backendstate_pc_seams as backend_seams;
use backend_utils_mmgr_mcxt_pc_seams as mcxt_seams;
use backend_utils_resowner_pc_seams as resowner_seams;
use backend_utils_time_snapmgr_pc_seams as snapmgr_seams;

#[cfg(test)]
mod tests;

/* ==========================================================================
 * Public handle types — opaque externally-owned identities (the plancache
 * registry ids; C `CachedPlanSource *` / `CachedPlan *` / `CachedExpression *`).
 * ======================================================================== */

/// `CachedPlanSource *` — opaque identity of a cached plan source.  `0` is NULL.
pub type CachedPlanSourceHandle = u64;
/// `CachedPlan *` — opaque identity of a cached plan.  `0` is NULL.
pub type CachedPlanHandle = u64;
/// `CachedExpression *` — opaque identity of a cached expression.  `0` is NULL.
pub type CachedExpressionHandle = u64;

const NULL_HANDLE: u64 = 0;

/* ==========================================================================
 * In-crate state owned by plancache (mirrors plancache.c's allocations).
 * ======================================================================== */

/// `struct CachedPlanSource`. Querytree/plan handles and search-path/result-
/// desc tokens point at storage owned by sibling subsystems (reached via seam).
struct CachedPlanSourceData {
    magic: i32,
    raw_parse_tree: RawStmtHandle,
    analyzed_parse_tree: AnalyzedQueryHandle,
    query_string: String,
    command_tag: CommandTag,
    param_types: Vec<Oid>,
    num_params: i32,
    parser_setup: ParserSetupHandle,
    post_rewrite: PostRewriteHandle,
    cursor_options: i32,
    fixed_result: bool,
    result_desc: TupleDescHandle,
    context: CtxId,
    query_list: QueryListHandle,
    relation_oids: Vec<Oid>,
    inval_items: Vec<InvalItemKey>,
    search_path: SearchPathMatcherHandle,
    query_context: Option<CtxId>,
    rewrite_role_id: Oid,
    rewrite_row_security: bool,
    depends_on_rls: bool,
    gplan: CachedPlanHandle,
    is_oneshot: bool,
    is_complete: bool,
    is_saved: bool,
    is_valid: bool,
    generation: i32,
    generic_cost: f64,
    total_custom_cost: f64,
    num_custom_plans: i64,
    num_generic_plans: i64,
}

/// `struct CachedPlan`. `generation` is written but, as in C, never read by
/// plancache itself.
#[allow(dead_code)]
struct CachedPlanData {
    magic: i32,
    stmt_list: PlannedStmtListHandle,
    is_oneshot: bool,
    is_saved: bool,
    is_valid: bool,
    plan_role_id: Oid,
    depends_on_role: bool,
    saved_xmin: u32,
    generation: i32,
    refcount: i32,
    context: CtxId,
}

/// `struct CachedExpression`. `expr` holds the planned-expression handle.
#[allow(dead_code)]
struct CachedExpressionData {
    magic: i32,
    expr: ExprHandle,
    is_valid: bool,
    relation_oids: Vec<Oid>,
    inval_items: Vec<InvalItemKey>,
    context: CtxId,
}

type SourceRc = Rc<RefCell<CachedPlanSourceData>>;
type PlanRc = Rc<RefCell<CachedPlanData>>;
type ExprRc = Rc<RefCell<CachedExpressionData>>;

/// Backend-global plancache state (plancache.c file scope), per-backend.
struct PlanCacheState {
    next_handle: u64,
    sources: HashMap<u64, SourceRc>,
    plans: HashMap<u64, PlanRc>,
    expressions: HashMap<u64, ExprRc>,
    /// `static dlist_head saved_plan_list`, in insertion order (handles).
    saved_plan_list: Vec<u64>,
    /// `static dlist_head cached_expression_list` — handles, in order.
    cached_expression_list: Vec<u64>,
}

impl PlanCacheState {
    fn new() -> Self {
        PlanCacheState {
            next_handle: 1,
            sources: HashMap::new(),
            plans: HashMap::new(),
            expressions: HashMap::new(),
            saved_plan_list: Vec::new(),
            cached_expression_list: Vec::new(),
        }
    }

    fn alloc_handle(&mut self) -> u64 {
        let h = self.next_handle;
        self.next_handle += 1;
        h
    }
}

thread_local! {
    static STATE: RefCell<PlanCacheState> = RefCell::new(PlanCacheState::new());
}

fn with_state<R>(f: impl FnOnce(&mut PlanCacheState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

fn get_source(h: u64) -> SourceRc {
    with_state(|s| {
        s.sources
            .get(&h)
            .cloned()
            .expect("plancache: dangling CachedPlanSource handle")
    })
}

fn get_plan(h: u64) -> PlanRc {
    with_state(|s| {
        s.plans
            .get(&h)
            .cloned()
            .expect("plancache: dangling CachedPlan handle")
    })
}

fn get_expr(h: u64) -> ExprRc {
    with_state(|s| {
        s.expressions
            .get(&h)
            .cloned()
            .expect("plancache: dangling CachedExpression handle")
    })
}

/* ==========================================================================
 * Small inline helpers from transam.h / c.h.
 * ======================================================================== */

/// `TransactionIdIsValid(xid)` — `(xid) != InvalidTransactionId`.
#[inline]
fn transaction_id_is_valid(xid: u32) -> bool {
    xid != 0
}

/// `TransactionIdEquals(a, b)`.
#[inline]
fn transaction_id_equals(a: u32, b: u32) -> bool {
    a == b
}

/// `TransactionIdIsNormal(xid)` — `(xid) >= FirstNormalTransactionId`.
#[inline]
fn transaction_id_is_normal(xid: u32) -> bool {
    xid >= FIRST_NORMAL_TRANSACTION_ID
}

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != INVALID_OID
}

/// `elog(ERROR, ...)` — internal error.
fn elog_error(msg: &str) -> PgError {
    PgError::new(ERROR, msg.to_string())
}

/* ==========================================================================
 * InitPlanCache
 * ======================================================================== */

/// `InitPlanCache(void)` — initialize module during InitPostgres.
pub fn InitPlanCache() -> PgResult<()> {
    inval_seams::register_relcache_callback::call(plan_cache_rel_callback)?;
    let proc_id = syscache_seams::syscache_id::call(SysCacheId::ProcOid)?;
    inval_seams::register_syscache_callback::call(proc_id, plan_cache_object_callback)?;
    let type_id = syscache_seams::syscache_id::call(SysCacheId::TypeOid)?;
    inval_seams::register_syscache_callback::call(type_id, plan_cache_object_callback)?;
    let ns_id = syscache_seams::syscache_id::call(SysCacheId::NamespaceOid)?;
    inval_seams::register_syscache_callback::call(ns_id, plan_cache_sys_callback)?;
    let oper_id = syscache_seams::syscache_id::call(SysCacheId::OperOid)?;
    inval_seams::register_syscache_callback::call(oper_id, plan_cache_sys_callback)?;
    let amop_id = syscache_seams::syscache_id::call(SysCacheId::AmOpOpId)?;
    inval_seams::register_syscache_callback::call(amop_id, plan_cache_sys_callback)?;
    let fs_id = syscache_seams::syscache_id::call(SysCacheId::ForeignServerOid)?;
    inval_seams::register_syscache_callback::call(fs_id, plan_cache_sys_callback)?;
    let fdw_id = syscache_seams::syscache_id::call(SysCacheId::ForeignDataWrapperOid)?;
    inval_seams::register_syscache_callback::call(fdw_id, plan_cache_sys_callback)?;
    Ok(())
}

/* ==========================================================================
 * CreateCachedPlan / CreateCachedPlanForQuery / CreateOneShotCachedPlan
 * ======================================================================== */

fn new_source(
    context: CtxId,
    is_oneshot: bool,
    command_tag: CommandTag,
    query_string: String,
) -> CachedPlanSourceData {
    CachedPlanSourceData {
        magic: CACHEDPLANSOURCE_MAGIC,
        raw_parse_tree: RawStmtHandle::NULL,
        analyzed_parse_tree: AnalyzedQueryHandle::NULL,
        query_string,
        command_tag,
        param_types: Vec::new(),
        num_params: 0,
        parser_setup: ParserSetupHandle::NONE,
        post_rewrite: PostRewriteHandle::NONE,
        cursor_options: 0,
        fixed_result: false,
        result_desc: TupleDescHandle::NULL,
        context,
        query_list: QueryListHandle::NIL,
        relation_oids: Vec::new(),
        inval_items: Vec::new(),
        search_path: SearchPathMatcherHandle::NULL,
        query_context: None,
        rewrite_role_id: INVALID_OID,
        rewrite_row_security: false,
        depends_on_rls: false,
        gplan: NULL_HANDLE,
        is_oneshot,
        is_complete: false,
        is_saved: false,
        is_valid: false,
        generation: 0,
        generic_cost: -1.0,
        total_custom_cost: 0.0,
        num_custom_plans: 0,
        num_generic_plans: 0,
    }
}

/// `CreateCachedPlan(raw_parse_tree, query_string, commandTag)`.
pub fn CreateCachedPlan(
    raw_parse_tree: RawStmtHandle,
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    let current = mcxt_seams::current_memory_context::call()?;
    let source_context =
        mcxt_seams::alloc_set_context_create_small::call(current, "CachedPlanSource")?;
    let oldcxt = mcxt_seams::memory_context_switch_to::call(source_context)?;

    // raw_parse_tree = copyObject(raw_parse_tree) — NULL copies to NULL.
    let raw_copy = node_seams::copy_raw_stmt::call(raw_parse_tree)?;
    let qstr = query_string.to_string();
    mcxt_seams::memory_context_set_identifier::call(source_context, &qstr)?;

    let mut data = new_source(source_context, false, command_tag, qstr);
    data.raw_parse_tree = raw_copy;

    mcxt_seams::memory_context_switch_to::call(oldcxt)?;

    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.sources.insert(h, Rc::new(RefCell::new(data)));
        h
    });
    Ok(handle)
}

/// `CreateCachedPlanForQuery(analyzed_parse_tree, query_string, commandTag)`.
pub fn CreateCachedPlanForQuery(
    analyzed_parse_tree: AnalyzedQueryHandle,
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    let plansource = CreateCachedPlan(RawStmtHandle::NULL, query_string, command_tag)?;
    let src = get_source(plansource);
    let context = src.borrow().context;
    let oldcxt = mcxt_seams::memory_context_switch_to::call(context)?;
    let copy = node_seams::copy_analyzed_query::call(analyzed_parse_tree)?;
    src.borrow_mut().analyzed_parse_tree = copy;
    mcxt_seams::memory_context_switch_to::call(oldcxt)?;
    Ok(plansource)
}

/// `CreateOneShotCachedPlan(raw_parse_tree, query_string, commandTag)`.
pub fn CreateOneShotCachedPlan(
    raw_parse_tree: RawStmtHandle,
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    // palloc0 in CurrentMemoryContext; raw tree NOT copied.
    let current = mcxt_seams::current_memory_context::call()?;
    let mut data = new_source(current, true, command_tag, query_string.to_string());
    data.raw_parse_tree = raw_parse_tree;

    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.sources.insert(h, Rc::new(RefCell::new(data)));
        h
    });
    Ok(handle)
}

/* ==========================================================================
 * CompleteCachedPlan
 * ======================================================================== */

/// `CompleteCachedPlan(...)`.
#[allow(clippy::too_many_arguments)]
pub fn CompleteCachedPlan(
    plansource: CachedPlanSourceHandle,
    mut querytree_list: QueryListHandle,
    querytree_context: Option<CtxId>,
    param_types: &[Oid],
    num_params: i32,
    parser_setup: ParserSetupHandle,
    cursor_options: i32,
    fixed_result: bool,
) -> PgResult<()> {
    let src = get_source(plansource);

    let (source_context, is_oneshot) = {
        let p = src.borrow();
        debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
        debug_assert!(!p.is_complete);
        (p.context, p.is_oneshot)
    };

    let oldcxt = mcxt_seams::current_memory_context::call()?;

    let querytree_context: CtxId = if is_oneshot {
        mcxt_seams::current_memory_context::call()?
    } else if let Some(qcxt) = querytree_context {
        mcxt_seams::memory_context_set_parent::call(qcxt, source_context)?;
        mcxt_seams::memory_context_switch_to::call(qcxt)?;
        qcxt
    } else {
        let qcxt =
            mcxt_seams::alloc_set_context_create_small::call(source_context, "CachedPlanQuery")?;
        mcxt_seams::memory_context_switch_to::call(qcxt)?;
        querytree_list = node_seams::copy_query_list::call(querytree_list)?;
        qcxt
    };

    src.borrow_mut().query_context = Some(querytree_context);
    src.borrow_mut().query_list = querytree_list;

    if !is_oneshot && StmtPlanRequiresRevalidation(&src)? {
        let deps = node_seams::extract_query_dependencies::call(querytree_list)?;
        let role = backend_seams::get_user_id::call()?;
        let rsec = backend_seams::row_security::call()?;
        let sp = namespace_seams::get_search_path_matcher::call(querytree_context)?;
        let mut p = src.borrow_mut();
        p.relation_oids = deps.relation_oids;
        p.inval_items = deps.inval_items;
        p.depends_on_rls = deps.depends_on_rls;
        p.rewrite_role_id = role;
        p.rewrite_row_security = rsec;
        p.search_path = sp;
    }

    mcxt_seams::memory_context_switch_to::call(source_context)?;

    {
        let mut p = src.borrow_mut();
        if num_params > 0 {
            p.param_types = param_types.to_vec();
        } else {
            p.param_types = Vec::new();
        }
        p.num_params = num_params;
        p.parser_setup = parser_setup;
        p.cursor_options = cursor_options;
        p.fixed_result = fixed_result;
    }

    let result_desc = PlanCacheComputeResultDesc(querytree_list)?;
    src.borrow_mut().result_desc = result_desc;

    mcxt_seams::memory_context_switch_to::call(oldcxt)?;

    let mut p = src.borrow_mut();
    p.is_complete = true;
    p.is_valid = true;
    Ok(())
}

/* ==========================================================================
 * SetPostRewriteHook
 * ======================================================================== */

/// `SetPostRewriteHook(plansource, postRewrite, postRewriteArg)`.
pub fn SetPostRewriteHook(
    plansource: CachedPlanSourceHandle,
    post_rewrite: PostRewriteHandle,
) -> PgResult<()> {
    let src = get_source(plansource);
    let mut p = src.borrow_mut();
    debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
    p.post_rewrite = post_rewrite;
    Ok(())
}

/* ==========================================================================
 * SaveCachedPlan
 * ======================================================================== */

/// `SaveCachedPlan(plansource)`.
pub fn SaveCachedPlan(plansource: CachedPlanSourceHandle) -> PgResult<()> {
    let src = get_source(plansource);
    {
        let p = src.borrow();
        debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
        debug_assert!(p.is_complete);
        debug_assert!(!p.is_saved);
        if p.is_oneshot {
            return Err(elog_error("cannot save one-shot cached plan"));
        }
    }

    ReleaseGenericPlan(plansource)?;

    let context = src.borrow().context;
    let cache_cxt = mcxt_seams::cache_memory_context::call()?;
    mcxt_seams::memory_context_set_parent::call(context, cache_cxt)?;

    with_state(|s| s.saved_plan_list.push(plansource));
    src.borrow_mut().is_saved = true;
    Ok(())
}

/* ==========================================================================
 * DropCachedPlan
 * ======================================================================== */

/// `DropCachedPlan(plansource)`.
pub fn DropCachedPlan(plansource: CachedPlanSourceHandle) -> PgResult<()> {
    let src = get_source(plansource);
    let (is_saved, is_oneshot, context) = {
        let p = src.borrow();
        debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
        (p.is_saved, p.is_oneshot, p.context)
    };

    if is_saved {
        with_state(|s| s.saved_plan_list.retain(|&h| h != plansource));
        src.borrow_mut().is_saved = false;
    }

    ReleaseGenericPlan(plansource)?;

    src.borrow_mut().magic = 0;

    if !is_oneshot {
        mcxt_seams::memory_context_delete::call(context)?;
    }

    with_state(|s| {
        s.sources.remove(&plansource);
    });
    Ok(())
}

/* ==========================================================================
 * ReleaseGenericPlan (static)
 * ======================================================================== */

/// `ReleaseGenericPlan(plansource)` — release a source's generic plan, if any.
fn ReleaseGenericPlan(plansource: CachedPlanSourceHandle) -> PgResult<()> {
    let src = get_source(plansource);
    let gplan = src.borrow().gplan;
    if gplan != NULL_HANDLE {
        debug_assert_eq!(get_plan(gplan).borrow().magic, CACHEDPLAN_MAGIC);
        src.borrow_mut().gplan = NULL_HANDLE;
        ReleaseCachedPlan(gplan, ResourceOwnerHandle::NULL)?;
    }
    Ok(())
}

/* ==========================================================================
 * StmtPlanRequiresRevalidation / BuildingPlanRequiresSnapshot (static)
 * ======================================================================== */

/// `StmtPlanRequiresRevalidation(plansource)`.
fn StmtPlanRequiresRevalidation(src: &SourceRc) -> PgResult<bool> {
    let p = src.borrow();
    if !p.raw_parse_tree.is_null() {
        analyze_seams::stmt_requires_parse_analysis::call(p.raw_parse_tree)
    } else if !p.analyzed_parse_tree.is_null() {
        analyze_seams::query_requires_rewrite_plan::call(p.analyzed_parse_tree)
    } else {
        // empty query never needs revalidation
        Ok(false)
    }
}

/// `BuildingPlanRequiresSnapshot(plansource)`.
fn BuildingPlanRequiresSnapshot(src: &SourceRc) -> PgResult<bool> {
    let p = src.borrow();
    if !p.raw_parse_tree.is_null() {
        analyze_seams::analyze_requires_snapshot::call(p.raw_parse_tree)
    } else if !p.analyzed_parse_tree.is_null() {
        analyze_seams::query_requires_rewrite_plan::call(p.analyzed_parse_tree)
    } else {
        Ok(false)
    }
}

/* ==========================================================================
 * RevalidateCachedQuery (static)
 * ======================================================================== */

/// `RevalidateCachedQuery(plansource, queryEnv)`.
fn RevalidateCachedQuery(
    plansource: CachedPlanSourceHandle,
    query_env: QueryEnvHandle,
) -> PgResult<QueryListHandle> {
    let src = get_source(plansource);
    let nil = QueryListHandle::NIL;

    if src.borrow().is_oneshot || !StmtPlanRequiresRevalidation(&src)? {
        debug_assert!(src.borrow().is_valid);
        return Ok(nil);
    }

    if src.borrow().is_valid {
        let sp = src.borrow().search_path;
        debug_assert!(!sp.is_null());
        if !namespace_seams::search_path_matches_current_environment::call(sp)? {
            src.borrow_mut().is_valid = false;
            let gplan = src.borrow().gplan;
            if gplan != NULL_HANDLE {
                get_plan(gplan).borrow_mut().is_valid = false;
            }
        }
    }

    if src.borrow().is_valid && src.borrow().depends_on_rls {
        let (role, rsec) = {
            let p = src.borrow();
            (p.rewrite_role_id, p.rewrite_row_security)
        };
        if role != backend_seams::get_user_id::call()?
            || rsec != backend_seams::row_security::call()?
        {
            src.borrow_mut().is_valid = false;
        }
    }

    if src.borrow().is_valid {
        let qlist = src.borrow().query_list;
        AcquirePlannerLocks(qlist, true)?;

        if src.borrow().is_valid {
            return Ok(nil);
        }

        // Oops, the race case happened.  Release useless locks.
        AcquirePlannerLocks(qlist, false)?;
    }

    {
        let mut p = src.borrow_mut();
        p.is_valid = false;
        p.query_list = QueryListHandle::NIL;
        p.relation_oids = Vec::new();
        p.inval_items = Vec::new();
        p.search_path = SearchPathMatcherHandle::NULL;
    }

    if let Some(qcxt) = src.borrow().query_context {
        src.borrow_mut().query_context = None;
        mcxt_seams::memory_context_delete::call(qcxt)?;
    }

    ReleaseGenericPlan(plansource)?;

    debug_assert!(src.borrow().is_complete);

    let mut snapshot_set = false;
    if !snapmgr_seams::active_snapshot_set::call()? {
        snapmgr_seams::push_active_snapshot_transaction::call()?;
        snapshot_set = true;
    }

    let tlist: QueryListHandle;
    // rawtree = copyObject(plansource->raw_parse_tree).
    let (raw, analyzed) = {
        let p = src.borrow();
        (p.raw_parse_tree, p.analyzed_parse_tree)
    };
    if !raw.is_null() {
        let rawtree = node_seams::copy_raw_stmt::call(raw)?;
        let (qstr, psetup, params) = {
            let p = src.borrow();
            (p.query_string.clone(), p.parser_setup, p.param_types.clone())
        };
        if psetup.is_some() {
            tlist =
                analyze_seams::analyze_and_rewrite_withcb::call(rawtree, &qstr, psetup, query_env)?;
        } else {
            tlist = analyze_seams::analyze_and_rewrite_fixedparams::call(
                rawtree, &qstr, &params, query_env,
            )?;
        }
    } else if !analyzed.is_null() {
        let analyzed_tree = node_seams::copy_analyzed_query::call(analyzed)?;
        rewrite_seams::acquire_rewrite_locks::call(analyzed_tree)?;
        tlist = rewrite_seams::rewrite_query::call(analyzed_tree)?;
    } else {
        tlist = QueryListHandle::NIL;
    }

    let post = src.borrow().post_rewrite;
    if post.is_some() {
        rewrite_seams::invoke_post_rewrite::call(post, tlist)?;
    }

    if snapshot_set {
        snapmgr_seams::pop_active_snapshot::call()?;
    }

    // Check or update the result tupdesc.
    let mut result_desc = PlanCacheComputeResultDesc(tlist)?;
    let existing_desc = src.borrow().result_desc;
    if result_desc.is_null() && existing_desc.is_null() {
        // OK, doesn't return tuples.
    } else if result_desc.is_null()
        || existing_desc.is_null()
        || !tupdesc_seams::equal_row_types::call(result_desc, existing_desc)?
    {
        if src.borrow().fixed_result {
            return Err(PgError::new(ERROR, "cached plan must not change result type".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        let context = src.borrow().context;
        let oldcxt = mcxt_seams::memory_context_switch_to::call(context)?;
        if !result_desc.is_null() {
            result_desc = tupdesc_seams::create_tuple_desc_copy::call(result_desc)?;
        }
        if !existing_desc.is_null() {
            tupdesc_seams::free_tuple_desc::call(existing_desc)?;
        }
        src.borrow_mut().result_desc = result_desc;
        mcxt_seams::memory_context_switch_to::call(oldcxt)?;
    }

    // Allocate new query_context and copy the completed querytree into it.
    let current = mcxt_seams::current_memory_context::call()?;
    let querytree_context =
        mcxt_seams::alloc_set_context_create_small::call(current, "CachedPlanQuery")?;
    let oldcxt = mcxt_seams::memory_context_switch_to::call(querytree_context)?;

    let qlist = node_seams::copy_query_list::call(tlist)?;

    let deps = node_seams::extract_query_dependencies::call(qlist)?;
    let role = backend_seams::get_user_id::call()?;
    let rsec = backend_seams::row_security::call()?;
    let sp = namespace_seams::get_search_path_matcher::call(querytree_context)?;
    {
        let mut p = src.borrow_mut();
        p.relation_oids = deps.relation_oids;
        p.inval_items = deps.inval_items;
        p.depends_on_rls = deps.depends_on_rls;
        p.rewrite_role_id = role;
        p.rewrite_row_security = rsec;
        p.search_path = sp;
    }

    mcxt_seams::memory_context_switch_to::call(oldcxt)?;

    let context = src.borrow().context;
    mcxt_seams::memory_context_set_parent::call(querytree_context, context)?;

    {
        let mut p = src.borrow_mut();
        p.query_context = Some(querytree_context);
        p.query_list = qlist;
        // Note: we do not reset generic_cost or total_custom_cost.
        p.is_valid = true;
    }

    Ok(tlist)
}

/* ==========================================================================
 * CheckCachedPlan (static)
 * ======================================================================== */

/// `CheckCachedPlan(plansource)`.
fn CheckCachedPlan(plansource: CachedPlanSourceHandle) -> PgResult<bool> {
    let src = get_source(plansource);
    let plan_h = src.borrow().gplan;

    debug_assert!(src.borrow().is_valid);

    if plan_h == NULL_HANDLE {
        return Ok(false);
    }

    let plan = get_plan(plan_h);
    debug_assert_eq!(plan.borrow().magic, CACHEDPLAN_MAGIC);
    debug_assert!(!plan.borrow().is_oneshot);

    if plan.borrow().is_valid
        && plan.borrow().depends_on_role
        && plan.borrow().plan_role_id != backend_seams::get_user_id::call()?
    {
        plan.borrow_mut().is_valid = false;
    }

    if plan.borrow().is_valid {
        debug_assert!(plan.borrow().refcount > 0);

        let stmt_list = plan.borrow().stmt_list;
        AcquireExecutorLocks(stmt_list, true)?;

        let saved_xmin = plan.borrow().saved_xmin;
        if plan.borrow().is_valid
            && transaction_id_is_valid(saved_xmin)
            && !transaction_id_equals(saved_xmin, snapmgr_seams::transaction_xmin::call()?)
        {
            plan.borrow_mut().is_valid = false;
        }

        if plan.borrow().is_valid {
            return Ok(true);
        }

        AcquireExecutorLocks(stmt_list, false)?;
    }

    ReleaseGenericPlan(plansource)?;

    Ok(false)
}

/* ==========================================================================
 * BuildCachedPlan (static)
 * ======================================================================== */

/// `BuildCachedPlan(plansource, qlist, boundParams, queryEnv)`.
fn BuildCachedPlan(
    plansource: CachedPlanSourceHandle,
    mut qlist: QueryListHandle,
    bound_params: ParamListInfoHandle,
    query_env: QueryEnvHandle,
) -> PgResult<CachedPlanHandle> {
    let src = get_source(plansource);
    let oldcxt = mcxt_seams::current_memory_context::call()?;

    if !src.borrow().is_valid {
        qlist = RevalidateCachedQuery(plansource, query_env)?;
    }

    if qlist.is_nil() {
        let (is_oneshot, query_list) = {
            let p = src.borrow();
            (p.is_oneshot, p.query_list)
        };
        if !is_oneshot {
            qlist = node_seams::copy_query_list::call(query_list)?;
        } else {
            qlist = query_list;
        }
    }

    let mut snapshot_set = false;
    if !snapmgr_seams::active_snapshot_set::call()? && BuildingPlanRequiresSnapshot(&src)? {
        snapmgr_seams::push_active_snapshot_transaction::call()?;
        snapshot_set = true;
    }

    let (qstr, cursor_options) = {
        let p = src.borrow();
        (p.query_string.clone(), p.cursor_options)
    };
    let mut plist = planner_seams::plan_queries::call(qlist, &qstr, cursor_options, bound_params)?;

    if snapshot_set {
        snapmgr_seams::pop_active_snapshot::call()?;
    }

    let is_oneshot = src.borrow().is_oneshot;
    let plan_context: CtxId;
    if !is_oneshot {
        let current = mcxt_seams::current_memory_context::call()?;
        plan_context = mcxt_seams::alloc_set_context_create_small::call(current, "CachedPlan")?;
        let qstr2 = src.borrow().query_string.clone();
        mcxt_seams::memory_context_copy_and_set_identifier::call(plan_context, &qstr2)?;

        mcxt_seams::memory_context_switch_to::call(plan_context)?;
        plist = node_seams::copy_plan_list::call(plist)?;
    } else {
        plan_context = mcxt_seams::current_memory_context::call()?;
    }

    let plan_role_id = backend_seams::get_user_id::call()?;
    let mut depends_on_role = src.borrow().depends_on_rls;
    let mut is_transient = false;
    for stmt in node_seams::plan_list_elements::call(plist)? {
        if planner_seams::pstmt_command_type_is_utility::call(stmt)? {
            continue; // Ignore utility statements.
        }
        if planner_seams::pstmt_transient_plan::call(stmt)? {
            is_transient = true;
        }
        if planner_seams::pstmt_depends_on_role::call(stmt)? {
            depends_on_role = true;
        }
    }

    let saved_xmin = if is_transient {
        let txmin = snapmgr_seams::transaction_xmin::call()?;
        debug_assert!(transaction_id_is_normal(txmin));
        txmin
    } else {
        0 // InvalidTransactionId
    };

    let generation = {
        let mut p = src.borrow_mut();
        p.generation += 1;
        p.generation
    };

    let plan = CachedPlanData {
        magic: CACHEDPLAN_MAGIC,
        stmt_list: plist,
        is_oneshot,
        is_saved: false,
        is_valid: true,
        plan_role_id,
        depends_on_role,
        saved_xmin,
        generation,
        refcount: 0,
        context: plan_context,
    };

    mcxt_seams::memory_context_switch_to::call(oldcxt)?;

    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.plans.insert(h, Rc::new(RefCell::new(plan)));
        h
    });
    Ok(handle)
}

/* ==========================================================================
 * choose_custom_plan (static)
 * ======================================================================== */

/// `choose_custom_plan(plansource, boundParams)`.
fn choose_custom_plan(
    plansource: CachedPlanSourceHandle,
    bound_params: ParamListInfoHandle,
) -> PgResult<bool> {
    let src = get_source(plansource);

    if src.borrow().is_oneshot {
        return Ok(true);
    }

    if bound_params.is_null() {
        return Ok(false);
    }
    if !StmtPlanRequiresRevalidation(&src)? {
        return Ok(false);
    }

    let mode = backend_seams::plan_cache_mode::call()?;
    if mode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN {
        return Ok(false);
    }
    if mode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN {
        return Ok(true);
    }

    let cursor_options = src.borrow().cursor_options;
    if cursor_options & CURSOR_OPT_GENERIC_PLAN != 0 {
        return Ok(false);
    }
    if cursor_options & CURSOR_OPT_CUSTOM_PLAN != 0 {
        return Ok(true);
    }

    let p = src.borrow();
    if p.num_custom_plans < 5 {
        return Ok(true);
    }

    let avg_custom_cost = p.total_custom_cost / p.num_custom_plans as f64;

    if p.generic_cost < avg_custom_cost {
        return Ok(false);
    }

    Ok(true)
}

/* ==========================================================================
 * cached_plan_cost (static)
 * ======================================================================== */

/// `cached_plan_cost(plan, include_planner)`.
fn cached_plan_cost(plan: CachedPlanHandle, include_planner: bool) -> PgResult<f64> {
    let mut result: f64 = 0.0;
    let stmt_list = get_plan(plan).borrow().stmt_list;

    for stmt in node_seams::plan_list_elements::call(stmt_list)? {
        if planner_seams::pstmt_command_type_is_utility::call(stmt)? {
            continue; // Ignore utility statements.
        }

        result += planner_seams::pstmt_plantree_total_cost::call(stmt)?;

        if include_planner {
            let nrelations = planner_seams::pstmt_rtable_length::call(stmt)?;
            let cpu_operator_cost = planner_seams::cpu_operator_cost::call()?;
            result += 1000.0 * cpu_operator_cost * (nrelations as f64 + 1.0);
        }
    }

    Ok(result)
}

/* ==========================================================================
 * GetCachedPlan
 * ======================================================================== */

/// `GetCachedPlan(plansource, boundParams, owner, queryEnv)`.
pub fn GetCachedPlan(
    plansource: CachedPlanSourceHandle,
    bound_params: ParamListInfoHandle,
    owner: ResourceOwnerHandle,
    query_env: QueryEnvHandle,
) -> PgResult<CachedPlanHandle> {
    let src = get_source(plansource);
    let mut plan: CachedPlanHandle = NULL_HANDLE;

    debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);
    debug_assert!(src.borrow().is_complete);
    if !owner.is_null() && !src.borrow().is_saved {
        return Err(elog_error(
            "cannot apply ResourceOwner to non-saved cached plan",
        ));
    }

    let mut qlist = RevalidateCachedQuery(plansource, query_env)?;

    let mut customplan = choose_custom_plan(plansource, bound_params)?;

    if !customplan {
        if CheckCachedPlan(plansource)? {
            plan = src.borrow().gplan;
            debug_assert_eq!(get_plan(plan).borrow().magic, CACHEDPLAN_MAGIC);
        } else {
            plan = BuildCachedPlan(plansource, qlist, ParamListInfoHandle::NULL, query_env)?;
            ReleaseGenericPlan(plansource)?;
            src.borrow_mut().gplan = plan;
            get_plan(plan).borrow_mut().refcount += 1;
            let (is_saved, context) = {
                let p = src.borrow();
                (p.is_saved, p.context)
            };
            let plan_context = get_plan(plan).borrow().context;
            if is_saved {
                let cache_cxt = mcxt_seams::cache_memory_context::call()?;
                mcxt_seams::memory_context_set_parent::call(plan_context, cache_cxt)?;
                get_plan(plan).borrow_mut().is_saved = true;
            } else {
                let parent = mcxt_seams::memory_context_get_parent::call(context)?;
                mcxt_seams::memory_context_set_parent::call(plan_context, parent)?;
            }
            let cost = cached_plan_cost(plan, false)?;
            src.borrow_mut().generic_cost = cost;

            customplan = choose_custom_plan(plansource, bound_params)?;

            qlist = QueryListHandle::NIL;
        }
    }

    if customplan {
        plan = BuildCachedPlan(plansource, qlist, bound_params, query_env)?;
        let cost = cached_plan_cost(plan, true)?;
        let mut p = src.borrow_mut();
        p.total_custom_cost += cost;
        p.num_custom_plans += 1;
    } else {
        src.borrow_mut().num_generic_plans += 1;
    }

    debug_assert!(plan != NULL_HANDLE);

    if !owner.is_null() {
        resowner_seams::resource_owner_enlarge::call(owner)?;
    }
    get_plan(plan).borrow_mut().refcount += 1;
    if !owner.is_null() {
        resowner_seams::resource_owner_remember_plan::call(owner, plan)?;
    }

    if customplan && src.borrow().is_saved {
        let cache_cxt = mcxt_seams::cache_memory_context::call()?;
        let plan_context = get_plan(plan).borrow().context;
        mcxt_seams::memory_context_set_parent::call(plan_context, cache_cxt)?;
        get_plan(plan).borrow_mut().is_saved = true;
    }

    Ok(plan)
}

/// Borrowable access to `CachedPlan.stmt_list` for callers that hold only a
/// `CachedPlan` (the executor / portal consumers).
pub fn CachedPlanStmtList(plan: CachedPlanHandle) -> PgResult<PlannedStmtListHandle> {
    let plan = get_plan(plan);
    debug_assert_eq!(plan.borrow().magic, CACHEDPLAN_MAGIC);
    let stmt_list = plan.borrow().stmt_list;
    Ok(stmt_list)
}

/* ==========================================================================
 * ReleaseCachedPlan
 * ======================================================================== */

/// `ReleaseCachedPlan(plan, owner)`.
pub fn ReleaseCachedPlan(plan: CachedPlanHandle, owner: ResourceOwnerHandle) -> PgResult<()> {
    let p = get_plan(plan);
    debug_assert_eq!(p.borrow().magic, CACHEDPLAN_MAGIC);
    if !owner.is_null() {
        debug_assert!(p.borrow().is_saved);
        resowner_seams::resource_owner_forget_plan::call(owner, plan)?;
    }
    debug_assert!(p.borrow().refcount > 0);
    p.borrow_mut().refcount -= 1;
    if p.borrow().refcount == 0 {
        p.borrow_mut().magic = 0;

        let (is_oneshot, context) = {
            let pl = p.borrow();
            (pl.is_oneshot, pl.context)
        };
        if !is_oneshot {
            mcxt_seams::memory_context_delete::call(context)?;
        }
        with_state(|s| {
            s.plans.remove(&plan);
        });
    }
    Ok(())
}

/* ==========================================================================
 * CachedPlanAllowsSimpleValidityCheck
 * ======================================================================== */

/// `CachedPlanAllowsSimpleValidityCheck(plansource, plan, owner)`.
pub fn CachedPlanAllowsSimpleValidityCheck(
    plansource: CachedPlanSourceHandle,
    plan: CachedPlanHandle,
    owner: ResourceOwnerHandle,
) -> PgResult<bool> {
    let src = get_source(plansource);
    let pl = get_plan(plan);

    debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);
    debug_assert_eq!(pl.borrow().magic, CACHEDPLAN_MAGIC);
    debug_assert!(pl.borrow().is_valid);
    debug_assert_eq!(plan, src.borrow().gplan);
    debug_assert!(!src.borrow().search_path.is_null());

    if src.borrow().is_oneshot {
        return Ok(false);
    }
    debug_assert!(!pl.borrow().is_oneshot);

    if src.borrow().depends_on_rls {
        return Ok(false);
    }
    if pl.borrow().depends_on_role {
        return Ok(false);
    }
    if transaction_id_is_valid(pl.borrow().saved_xmin) {
        return Ok(false);
    }

    // Reject if AcquirePlannerLocks would have anything to do.
    let query_list = src.borrow().query_list;
    for query in node_seams::query_list_elements::call(query_list)? {
        if analyze_seams::query_command_type_is_utility::call(query)? {
            return Ok(false);
        }
        if analyze_seams::query_has_rtable::call(query)?
            || analyze_seams::query_has_cte_list::call(query)?
            || analyze_seams::query_has_sublinks::call(query)?
        {
            return Ok(false);
        }
    }

    // Reject if AcquireExecutorLocks would have anything to do.
    let stmt_list = pl.borrow().stmt_list;
    for stmt in node_seams::plan_list_elements::call(stmt_list)? {
        if planner_seams::pstmt_command_type_is_utility::call(stmt)? {
            return Ok(false);
        }
        // grovel through the rtable for an RTE_RELATION.
        for rte in planner_seams::pstmt_rtable_fields::call(stmt)? {
            if rte.rtekind == RTE_RELATION {
                return Ok(false);
            }
        }
    }

    // Okay, it's simple.  Bump refcount if requested.
    if !owner.is_null() {
        resowner_seams::resource_owner_enlarge::call(owner)?;
        pl.borrow_mut().refcount += 1;
        resowner_seams::resource_owner_remember_plan::call(owner, plan)?;
    }

    Ok(true)
}

/* ==========================================================================
 * CachedPlanIsSimplyValid
 * ======================================================================== */

/// `CachedPlanIsSimplyValid(plansource, plan, owner)`.
pub fn CachedPlanIsSimplyValid(
    plansource: CachedPlanSourceHandle,
    plan: CachedPlanHandle,
    owner: ResourceOwnerHandle,
) -> PgResult<bool> {
    let src = get_source(plansource);

    debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);

    // Careful: "plan" might be a dangling handle; don't deref until verified.
    let plan_valid = plan != NULL_HANDLE
        && with_state(|s| s.plans.contains_key(&plan))
        && get_plan(plan).borrow().is_valid;
    if !src.borrow().is_valid || plan == NULL_HANDLE || plan != src.borrow().gplan || !plan_valid {
        return Ok(false);
    }

    let pl = get_plan(plan);
    debug_assert_eq!(pl.borrow().magic, CACHEDPLAN_MAGIC);

    let sp = src.borrow().search_path;
    debug_assert!(!sp.is_null());
    if !namespace_seams::search_path_matches_current_environment::call(sp)? {
        return Ok(false);
    }

    if !owner.is_null() {
        resowner_seams::resource_owner_enlarge::call(owner)?;
        pl.borrow_mut().refcount += 1;
        resowner_seams::resource_owner_remember_plan::call(owner, plan)?;
    }

    Ok(true)
}

/* ==========================================================================
 * CachedPlanSetParentContext
 * ======================================================================== */

/// `CachedPlanSetParentContext(plansource, newcontext)`.
pub fn CachedPlanSetParentContext(
    plansource: CachedPlanSourceHandle,
    newcontext: CtxId,
) -> PgResult<()> {
    let src = get_source(plansource);

    {
        let p = src.borrow();
        debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
        debug_assert!(p.is_complete);

        if p.is_saved {
            return Err(elog_error(
                "cannot move a saved cached plan to another context",
            ));
        }
        if p.is_oneshot {
            return Err(elog_error(
                "cannot move a one-shot cached plan to another context",
            ));
        }
    }

    let context = src.borrow().context;
    mcxt_seams::memory_context_set_parent::call(context, newcontext)?;

    let gplan = src.borrow().gplan;
    if gplan != NULL_HANDLE {
        debug_assert_eq!(get_plan(gplan).borrow().magic, CACHEDPLAN_MAGIC);
        let plan_context = get_plan(gplan).borrow().context;
        mcxt_seams::memory_context_set_parent::call(plan_context, newcontext)?;
    }
    Ok(())
}

/* ==========================================================================
 * CopyCachedPlan
 * ======================================================================== */

/// `CopyCachedPlan(plansource)`.
pub fn CopyCachedPlan(plansource: CachedPlanSourceHandle) -> PgResult<CachedPlanSourceHandle> {
    let src = get_source(plansource);
    {
        let p = src.borrow();
        debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
        debug_assert!(p.is_complete);
        if p.is_oneshot {
            return Err(elog_error("cannot copy a one-shot cached plan"));
        }
    }

    let current = mcxt_seams::current_memory_context::call()?;
    let source_context =
        mcxt_seams::alloc_set_context_create_small::call(current, "CachedPlanSource")?;
    let oldcxt = mcxt_seams::memory_context_switch_to::call(source_context)?;

    let (
        raw_copy,
        analyzed_copy,
        qstr,
        command_tag,
        param_types,
        num_params,
        parser_setup,
        post_rewrite,
        cursor_options,
        fixed_result,
        result_desc_copy,
    ) = {
        let p = src.borrow();
        let raw_copy = node_seams::copy_raw_stmt::call(p.raw_parse_tree)?;
        let analyzed_copy = node_seams::copy_analyzed_query::call(p.analyzed_parse_tree)?;
        let result_desc_copy = if !p.result_desc.is_null() {
            tupdesc_seams::create_tuple_desc_copy::call(p.result_desc)?
        } else {
            TupleDescHandle::NULL
        };
        (
            raw_copy,
            analyzed_copy,
            p.query_string.clone(),
            p.command_tag,
            p.param_types.clone(),
            p.num_params,
            p.parser_setup,
            p.post_rewrite,
            p.cursor_options,
            p.fixed_result,
            result_desc_copy,
        )
    };
    mcxt_seams::memory_context_set_identifier::call(source_context, &qstr)?;

    let querytree_context =
        mcxt_seams::alloc_set_context_create_small::call(source_context, "CachedPlanQuery")?;
    mcxt_seams::memory_context_switch_to::call(querytree_context)?;

    let (
        query_list_copy,
        relation_oids,
        inval_items,
        search_path_copy,
        is_valid,
        generation,
        generic_cost,
        total_custom_cost,
        num_generic_plans,
        num_custom_plans,
        rewrite_role_id,
        rewrite_row_security,
        depends_on_rls,
    ) = {
        let p = src.borrow();
        let query_list_copy = node_seams::copy_query_list::call(p.query_list)?;
        let search_path_copy = if !p.search_path.is_null() {
            namespace_seams::copy_search_path_matcher::call(p.search_path)?
        } else {
            SearchPathMatcherHandle::NULL
        };
        (
            query_list_copy,
            p.relation_oids.clone(),
            p.inval_items.clone(),
            search_path_copy,
            p.is_valid,
            p.generation,
            p.generic_cost,
            p.total_custom_cost,
            p.num_generic_plans,
            p.num_custom_plans,
            p.rewrite_role_id,
            p.rewrite_row_security,
            p.depends_on_rls,
        )
    };

    let newdata = CachedPlanSourceData {
        magic: CACHEDPLANSOURCE_MAGIC,
        raw_parse_tree: raw_copy,
        analyzed_parse_tree: analyzed_copy,
        query_string: qstr,
        command_tag,
        param_types,
        num_params,
        parser_setup,
        post_rewrite,
        cursor_options,
        fixed_result,
        result_desc: result_desc_copy,
        context: source_context,
        query_list: query_list_copy,
        relation_oids,
        inval_items,
        search_path: search_path_copy,
        query_context: Some(querytree_context),
        rewrite_role_id,
        rewrite_row_security,
        depends_on_rls,
        gplan: NULL_HANDLE,
        is_oneshot: false,
        is_complete: true,
        is_saved: false,
        is_valid,
        generation,
        generic_cost,
        total_custom_cost,
        num_custom_plans,
        num_generic_plans,
    };

    mcxt_seams::memory_context_switch_to::call(oldcxt)?;

    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.sources.insert(h, Rc::new(RefCell::new(newdata)));
        h
    });
    Ok(handle)
}

/* ==========================================================================
 * CachedPlanIsValid
 * ======================================================================== */

/// `CachedPlanIsValid(plansource)`.
pub fn CachedPlanIsValid(plansource: CachedPlanSourceHandle) -> PgResult<bool> {
    let src = get_source(plansource);
    let p = src.borrow();
    debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
    Ok(p.is_valid)
}

/* ==========================================================================
 * CachedPlanGetTargetList
 * ======================================================================== */

/// `CachedPlanGetTargetList(plansource, queryEnv)`.
pub fn CachedPlanGetTargetList(
    plansource: CachedPlanSourceHandle,
    query_env: QueryEnvHandle,
) -> PgResult<TargetListHandle> {
    let src = get_source(plansource);

    debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);
    debug_assert!(src.borrow().is_complete);

    if src.borrow().result_desc.is_null() {
        return Ok(TargetListHandle::NIL);
    }

    RevalidateCachedQuery(plansource, query_env)?;

    let query_list = src.borrow().query_list;
    let pstmt = QueryListGetPrimaryStmt(query_list)?;

    pquery_seams::fetch_statement_target_list::call(pstmt)
}

/* ==========================================================================
 * GetCachedExpression
 * ======================================================================== */

/// `GetCachedExpression(expr)`.
pub fn GetCachedExpression(expr: ExprHandle) -> PgResult<CachedExpressionHandle> {
    let (planned_expr, relation_oids, inval_items) =
        node_seams::expression_planner_with_deps::call(expr)?;

    let current = mcxt_seams::current_memory_context::call()?;
    let cexpr_context =
        mcxt_seams::alloc_set_context_create_small::call(current, "CachedExpression")?;
    let oldcxt = mcxt_seams::memory_context_switch_to::call(cexpr_context)?;

    let expr_copy = node_seams::copy_expr::call(planned_expr)?;
    let data = CachedExpressionData {
        magic: CACHEDEXPR_MAGIC,
        expr: expr_copy,
        is_valid: true,
        relation_oids,
        inval_items,
        context: cexpr_context,
    };

    mcxt_seams::memory_context_switch_to::call(oldcxt)?;

    let cache_cxt = mcxt_seams::cache_memory_context::call()?;
    mcxt_seams::memory_context_set_parent::call(cexpr_context, cache_cxt)?;

    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.expressions.insert(h, Rc::new(RefCell::new(data)));
        s.cached_expression_list.push(h);
        h
    });
    Ok(handle)
}

/* ==========================================================================
 * FreeCachedExpression
 * ======================================================================== */

/// `FreeCachedExpression(cexpr)`.
pub fn FreeCachedExpression(cexpr: CachedExpressionHandle) -> PgResult<()> {
    let ce = get_expr(cexpr);
    debug_assert_eq!(ce.borrow().magic, CACHEDEXPR_MAGIC);
    with_state(|s| s.cached_expression_list.retain(|&h| h != cexpr));
    let context = ce.borrow().context;
    mcxt_seams::memory_context_delete::call(context)?;
    with_state(|s| {
        s.expressions.remove(&cexpr);
    });
    Ok(())
}

/* ==========================================================================
 * QueryListGetPrimaryStmt (static)
 * ======================================================================== */

/// `QueryListGetPrimaryStmt(stmts)`.
fn QueryListGetPrimaryStmt(stmts: QueryListHandle) -> PgResult<QueryHandle> {
    for stmt in node_seams::query_list_elements::call(stmts)? {
        if analyze_seams::query_can_set_tag::call(stmt)? {
            return Ok(stmt);
        }
    }
    Ok(QueryHandle::NULL)
}

/* ==========================================================================
 * AcquireExecutorLocks (static)
 * ======================================================================== */

/// `AcquireExecutorLocks(stmt_list, acquire)`.
fn AcquireExecutorLocks(stmt_list: PlannedStmtListHandle, acquire: bool) -> PgResult<()> {
    for stmt in node_seams::plan_list_elements::call(stmt_list)? {
        if planner_seams::pstmt_command_type_is_utility::call(stmt)? {
            // Ignore utility statements, except those that contain a query.
            let utility = planner_seams::pstmt_utility_stmt::call(stmt)?;
            let query = utility_seams::utility_contains_query::call(utility)?;
            if !query.is_null() {
                ScanQueryForLocks(query, acquire)?;
            }
            continue;
        }

        for rte in planner_seams::pstmt_rtable_fields::call(stmt)? {
            if !(rte.rtekind == RTE_RELATION
                || (rte.rtekind == RTE_SUBQUERY && oid_is_valid(rte.relid)))
            {
                continue;
            }
            if acquire {
                lmgr_seams::lock_relation_oid::call(rte.relid, rte.rellockmode)?;
            } else {
                lmgr_seams::unlock_relation_oid::call(rte.relid, rte.rellockmode)?;
            }
        }
    }
    Ok(())
}

/* ==========================================================================
 * AcquirePlannerLocks (static)
 * ======================================================================== */

/// `AcquirePlannerLocks(stmt_list, acquire)`.
fn AcquirePlannerLocks(stmt_list: QueryListHandle, acquire: bool) -> PgResult<()> {
    for query in node_seams::query_list_elements::call(stmt_list)? {
        if analyze_seams::query_command_type_is_utility::call(query)? {
            // Ignore utility statements, unless they contain a Query.
            let utility = analyze_seams::query_utility_stmt::call(query)?;
            let inner = utility_seams::utility_contains_query::call(utility)?;
            if !inner.is_null() {
                ScanQueryForLocks(inner, acquire)?;
            }
            continue;
        }
        ScanQueryForLocks(query, acquire)?;
    }
    Ok(())
}

/* ==========================================================================
 * ScanQueryForLocks (static)
 * ======================================================================== */

/// `ScanQueryForLocks(parsetree, acquire)`.
fn ScanQueryForLocks(parsetree: QueryHandle, acquire: bool) -> PgResult<()> {
    debug_assert!(!analyze_seams::query_command_type_is_utility::call(parsetree)?);

    // First, process RTEs of the current query level.
    for rte in analyze_seams::query_rtable_fields::call(parsetree)? {
        if rte.rtekind == RTE_RELATION {
            if acquire {
                lmgr_seams::lock_relation_oid::call(rte.relid, rte.rellockmode)?;
            } else {
                lmgr_seams::unlock_relation_oid::call(rte.relid, rte.rellockmode)?;
            }
        } else if rte.rtekind == RTE_SUBQUERY {
            // If this was a view, must lock/unlock the view.
            if oid_is_valid(rte.relid) {
                if acquire {
                    lmgr_seams::lock_relation_oid::call(rte.relid, rte.rellockmode)?;
                } else {
                    lmgr_seams::unlock_relation_oid::call(rte.relid, rte.rellockmode)?;
                }
            }
            // Recurse into subquery-in-FROM.
            ScanQueryForLocks(rte.subquery, acquire)?;
        }
        // else: ignore other types of RTEs.
    }

    // Recurse into subquery-in-WITH.
    for ctequery in analyze_seams::query_cte_queries::call(parsetree)? {
        ScanQueryForLocks(ctequery, acquire)?;
    }

    // Recurse into sublink subqueries, too (rtable + cteList already done).
    if analyze_seams::query_has_sublinks::call(parsetree)? {
        for sub in ScanQueryWalker(parsetree)? {
            ScanQueryForLocks(sub, acquire)?;
        }
    }
    Ok(())
}

/* ==========================================================================
 * ScanQueryWalker (static)
 * ======================================================================== */

/// `ScanQueryWalker` — find sublink subqueries for `ScanQueryForLocks`.
fn ScanQueryWalker(parsetree: QueryHandle) -> PgResult<Vec<QueryHandle>> {
    analyze_seams::walk_query_sublinks_for_locks::call(parsetree)
}

/* ==========================================================================
 * PlanCacheComputeResultDesc (static)
 * ======================================================================== */

/// `PlanCacheComputeResultDesc(stmt_list)`.
fn PlanCacheComputeResultDesc(stmt_list: QueryListHandle) -> PgResult<TupleDescHandle> {
    match pquery_seams::choose_portal_strategy::call(stmt_list)? {
        PortalStrategy::OneSelect | PortalStrategy::OneModWith => {
            let query = first_query(stmt_list)?;
            let tl = analyze_seams::query_target_list::call(query)?;
            pquery_seams::exec_clean_type_from_tl::call(tl)
        }
        PortalStrategy::OneReturning => {
            let query = QueryListGetPrimaryStmt(stmt_list)?;
            let returning = analyze_seams::query_returning_list::call(query)?;
            pquery_seams::exec_clean_type_from_tl::call(returning)
        }
        PortalStrategy::UtilSelect => {
            let query = first_query(stmt_list)?;
            let util = analyze_seams::query_utility_stmt::call(query)?;
            debug_assert!(!util.is_null());
            utility_seams::utility_tuple_descriptor::call(util)
        }
        PortalStrategy::MultiQuery => Ok(TupleDescHandle::NULL),
    }
}

/// `linitial_node(Query, stmt_list)`.
fn first_query(stmt_list: QueryListHandle) -> PgResult<QueryHandle> {
    let elems = node_seams::query_list_elements::call(stmt_list)?;
    Ok(elems.into_iter().next().unwrap_or(QueryHandle::NULL))
}

/* ==========================================================================
 * PlanCacheRelCallback (static)
 * ======================================================================== */

/// `PlanCacheRelCallback(arg, relid)` — relcache inval callback.
fn plan_cache_rel_callback(relid: Oid) {
    // A registered C callback cannot return a Result; in C an error here would
    // ereport(ERROR). The project convention swallows at this boundary.
    let _ = plan_cache_rel_callback_impl(relid);
}

fn plan_cache_rel_callback_impl(relid: Oid) -> PgResult<()> {
    let sources = with_state(|s| s.saved_plan_list.clone());
    for h in sources {
        let src = match with_state(|s| s.sources.get(&h).cloned()) {
            Some(s) => s,
            None => continue,
        };
        debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);

        if !src.borrow().is_valid {
            continue;
        }

        if !StmtPlanRequiresRevalidation(&src)? {
            continue;
        }

        let hit = if relid == INVALID_OID {
            !src.borrow().relation_oids.is_empty()
        } else {
            node_seams::list_member_oid::call(&src.borrow().relation_oids, relid)?
        };
        if hit {
            src.borrow_mut().is_valid = false;
            let gplan = src.borrow().gplan;
            if gplan != NULL_HANDLE {
                get_plan(gplan).borrow_mut().is_valid = false;
            }
        }

        let gplan = src.borrow().gplan;
        if gplan != NULL_HANDLE && get_plan(gplan).borrow().is_valid {
            let stmt_list = get_plan(gplan).borrow().stmt_list;
            for stmt in node_seams::plan_list_elements::call(stmt_list)? {
                if planner_seams::pstmt_command_type_is_utility::call(stmt)? {
                    continue;
                }
                let oids = planner_seams::pstmt_relation_oids::call(stmt)?;
                let stmt_hit = if relid == INVALID_OID {
                    !oids.is_empty()
                } else {
                    node_seams::list_member_oid::call(&oids, relid)?
                };
                if stmt_hit {
                    get_plan(gplan).borrow_mut().is_valid = false;
                    break;
                }
            }
        }
    }

    // Likewise check cached expressions.
    let exprs = with_state(|s| s.cached_expression_list.clone());
    for h in exprs {
        let ce = match with_state(|s| s.expressions.get(&h).cloned()) {
            Some(c) => c,
            None => continue,
        };
        debug_assert_eq!(ce.borrow().magic, CACHEDEXPR_MAGIC);

        if !ce.borrow().is_valid {
            continue;
        }

        let hit = if relid == INVALID_OID {
            !ce.borrow().relation_oids.is_empty()
        } else {
            node_seams::list_member_oid::call(&ce.borrow().relation_oids, relid)?
        };
        if hit {
            ce.borrow_mut().is_valid = false;
        }
    }
    Ok(())
}

/* ==========================================================================
 * PlanCacheObjectCallback (static)
 * ======================================================================== */

/// `PlanCacheObjectCallback(arg, cacheid, hashvalue)` — PROCOID/TYPEOID inval.
fn plan_cache_object_callback(cacheid: i32, hashvalue: u32) {
    let _ = plan_cache_object_callback_impl(cacheid, hashvalue);
}

fn inval_matches(item: &InvalItemKey, cacheid: i32, hashvalue: u32) -> bool {
    item.cache_id == cacheid && (hashvalue == 0 || item.hash_value == hashvalue)
}

fn plan_cache_object_callback_impl(cacheid: i32, hashvalue: u32) -> PgResult<()> {
    let sources = with_state(|s| s.saved_plan_list.clone());
    for h in sources {
        let src = match with_state(|s| s.sources.get(&h).cloned()) {
            Some(s) => s,
            None => continue,
        };
        debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);

        if !src.borrow().is_valid {
            continue;
        }
        if !StmtPlanRequiresRevalidation(&src)? {
            continue;
        }

        let items = src.borrow().inval_items.clone();
        for item in &items {
            if item.cache_id != cacheid {
                continue;
            }
            if hashvalue == 0 || item.hash_value == hashvalue {
                src.borrow_mut().is_valid = false;
                let gplan = src.borrow().gplan;
                if gplan != NULL_HANDLE {
                    get_plan(gplan).borrow_mut().is_valid = false;
                }
                break;
            }
        }

        let gplan = src.borrow().gplan;
        if gplan != NULL_HANDLE && get_plan(gplan).borrow().is_valid {
            let stmt_list = get_plan(gplan).borrow().stmt_list;
            'stmt_scan: for stmt in node_seams::plan_list_elements::call(stmt_list)? {
                if planner_seams::pstmt_command_type_is_utility::call(stmt)? {
                    continue;
                }
                for item in planner_seams::pstmt_inval_items::call(stmt)? {
                    if inval_matches(&item, cacheid, hashvalue) {
                        get_plan(gplan).borrow_mut().is_valid = false;
                        break;
                    }
                }
                if !get_plan(gplan).borrow().is_valid {
                    break 'stmt_scan;
                }
            }
        }
    }

    // Likewise check cached expressions.
    let exprs = with_state(|s| s.cached_expression_list.clone());
    for h in exprs {
        let ce = match with_state(|s| s.expressions.get(&h).cloned()) {
            Some(c) => c,
            None => continue,
        };
        debug_assert_eq!(ce.borrow().magic, CACHEDEXPR_MAGIC);

        if !ce.borrow().is_valid {
            continue;
        }

        let items = ce.borrow().inval_items.clone();
        for item in &items {
            if item.cache_id != cacheid {
                continue;
            }
            if hashvalue == 0 || item.hash_value == hashvalue {
                ce.borrow_mut().is_valid = false;
                break;
            }
        }
    }
    Ok(())
}

/* ==========================================================================
 * PlanCacheSysCallback (static)
 * ======================================================================== */

/// `PlanCacheSysCallback(arg, cacheid, hashvalue)` — just invalidate everything.
fn plan_cache_sys_callback(_cacheid: i32, _hashvalue: u32) {
    let _ = ResetPlanCache();
}

/* ==========================================================================
 * ResetPlanCache
 * ======================================================================== */

/// `ResetPlanCache(void)` — invalidate all cached plans.
pub fn ResetPlanCache() -> PgResult<()> {
    let sources = with_state(|s| s.saved_plan_list.clone());
    for h in sources {
        let src = match with_state(|s| s.sources.get(&h).cloned()) {
            Some(s) => s,
            None => continue,
        };
        debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);

        if !src.borrow().is_valid {
            continue;
        }

        // We *must not* mark transaction control statements as invalid.
        if !StmtPlanRequiresRevalidation(&src)? {
            continue;
        }

        src.borrow_mut().is_valid = false;
        let gplan = src.borrow().gplan;
        if gplan != NULL_HANDLE {
            get_plan(gplan).borrow_mut().is_valid = false;
        }
    }

    let exprs = with_state(|s| s.cached_expression_list.clone());
    for h in exprs {
        let ce = match with_state(|s| s.expressions.get(&h).cloned()) {
            Some(c) => c,
            None => continue,
        };
        debug_assert_eq!(ce.borrow().magic, CACHEDEXPR_MAGIC);
        ce.borrow_mut().is_valid = false;
    }
    Ok(())
}

/* ==========================================================================
 * ReleaseAllPlanCacheRefsInOwner
 * ======================================================================== */

/// `ReleaseAllPlanCacheRefsInOwner(owner)`.
pub fn ReleaseAllPlanCacheRefsInOwner(owner: ResourceOwnerHandle) -> PgResult<()> {
    // ResourceOwnerReleaseAllOfKind returns the plan ids still held by `owner`;
    // each re-enters ResOwnerReleaseCachedPlan -> ReleaseCachedPlan(plan, NULL).
    let plans = resowner_seams::resource_owner_release_all_plan_refs::call(owner)?;
    for plan in plans {
        ResOwnerReleaseCachedPlan(plan)?;
    }
    Ok(())
}

/* ==========================================================================
 * ResOwnerReleaseCachedPlan (static) — ResourceOwner release callback.
 * ======================================================================== */

/// `ResOwnerReleaseCachedPlan(res)` — `ReleaseCachedPlan((CachedPlan *) res, NULL)`.
pub fn ResOwnerReleaseCachedPlan(plan: CachedPlanHandle) -> PgResult<()> {
    ReleaseCachedPlan(plan, ResourceOwnerHandle::NULL)
}

/* ==========================================================================
 * Seam installation.  plancache has no INWARD seams (no other crate calls it
 * across a cycle yet), so init_seams() installs nothing.  It exists so the
 * aggregator can call it uniformly.
 * ======================================================================== */

/// Install plancache's own seams. `init_plan_cache` is consumed across the
/// postinit bring-up cycle via `plancache-seams`, so it is installed here
/// (assemble/seam-wiring-guard pure-wiring fix).
pub fn init_seams() {
    backend_utils_cache_plancache_seams::init_plan_cache::set(InitPlanCache);

    // `plan_cache_mode` (plancache.c:138 `int plan_cache_mode`) is a USERSET enum
    // GUC owned by this unit (guc_tables.c:5372 binds `&plan_cache_mode`).  C reads
    // it straight from the GUC slot in choose_custom_plan(); mirror that by reading
    // the guc-tables slot (GucEnumVar::read() -> i32).
    backend_seams::plan_cache_mode::set(|| {
        Ok(backend_utils_misc_guc_tables::vars::plan_cache_mode.read())
    });
}
