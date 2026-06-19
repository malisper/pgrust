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
//! ## Modeling the shared-mutable cache graph (identity vs storage)
//!
//! C shares the same `CachedPlan *` simultaneously between `plansource->gplan`,
//! the caller's returned pointer, and a `ResourceOwner`, and the backend-global
//! lists hold `CachedPlanSource *`s that invalidation callbacks mutate in place
//! while callers still hold them. That *identity* shape is modeled with
//! `Rc<RefCell<…>>` interned in a [`thread_local!`] registry keyed by a stable
//! `u64` handle; the public entry points take/return those handles exactly as
//! the C entry points take/return `CachedPlanSource *` / `CachedPlan *`. The C
//! reference count (`CachedPlan.refcount`) stays authoritative; the registry
//! `Rc` only keeps the data reachable until the refcount and all links drop.
//!
//! ## De-handled data: owned value nodes in private MemoryContexts (F0)
//!
//! Where C allocates the querytree / plan / search-path / result-tupdesc data
//! in a `MemoryContext` subsidiary to the `CachedPlanSource`/`CachedPlan`, the
//! Rust model OWNS those values directly inside the struct, backed by a
//! struct-private [`mcx::MemoryContext`] (the portalmem pattern,
//! `docs/mctx-design.md`): each value is produced/copied via `clone_in(ctx.mcx())`
//! and its borrow extended to the field's `'static` marker. That is sound
//! because each value field is declared *before* the `MemoryContext` field that
//! backs it, so the value is dropped (freeing its `Global`-heap storage) before
//! the arena it is accounted to. `CreateCachedPlan`'s C trick of building under
//! the caller's transient context and reparenting later is collapsed to: build
//! the value, drop on error — Rust's drop on `?` gives the same "disappears on
//! error" guarantee.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use mcx::{Mcx, MemoryContext, PgVec};
use types_core::primitive::{Oid, INVALID_OID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_namespace::namespace::SearchPathMatcher;
use types_nodes::copy_query::{Query, CURSOR_OPT_PARALLEL_OK};
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::{ntag, CmdType, Node};
use types_nodes::params::ParamListInfo;
use types_nodes::parsestmt::{
    CachedPlanHandle as SeamPlanHandle, CachedPlanSourceHandle as SeamSourceHandle, CommandTag,
    RawStmt, ResourceOwnerHandle,
};
use types_nodes::primnodes::Expr;
use types_nodes::queryenvironment::QueryEnvironment;
use types_plancache::{
    InvalItemKey, SysCacheId, CACHEDEXPR_MAGIC, CACHEDPLANSOURCE_MAGIC,
    CACHEDPLAN_MAGIC, CURSOR_OPT_CUSTOM_PLAN, CURSOR_OPT_GENERIC_PLAN,
    FIRST_NORMAL_TRANSACTION_ID, PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN,
    PLAN_CACHE_MODE_FORCE_GENERIC_PLAN,
};
use types_tuple::heaptuple::TupleDescData;

// Value-producer seams (the de-handled pipeline).
use backend_catalog_namespace_seams as namespace_seams;
use backend_executor_execTuples_seams as exectuples_seams;
use backend_optimizer_plan_planner_pc_seams as planner_pc_seams;
use backend_optimizer_plan_setrefs_seams as setrefs_seams;
use backend_parser_analyze_seams as analyze_seams;
use backend_rewrite_rewritehandler_seams as rewrite_seams;
use backend_tcop_postgres_seams as postgres_seams;
use backend_tcop_pquery_seams as pquery_seams;
use backend_tcop_utility_seams as utility_seams;

// Seams that stay handle/scalar-shaped (no value content crosses them).
use backend_access_common_tupdesc_seams as tupdesc_seams;
use backend_storage_lmgr_lmgr_pc_seams as lmgr_seams;
use backend_utils_cache_inval_pc_seams as inval_seams;
use backend_utils_cache_syscache_pc_seams as syscache_seams;
use backend_utils_misc_backendstate_pc_seams as backend_seams;
use backend_utils_resowner_pc_seams as resowner_seams;
use backend_utils_time_snapmgr_pc_seams as snapmgr_seams;

use backend_utils_cache_plancache_seams as inward;
use backend_utils_cache_plancache_portal_seams as portal_inward;

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
 *
 * The value fields OWN their nodes in the struct-private `MemoryContext`s
 * (`context` for permanent source data; `query_context` for the rewritten
 * querytree). DROP ORDER: every value field is declared BEFORE the context
 * that backs it, so values are dropped first (portalmem `'static` pattern).
 * ======================================================================== */

/// `struct CachedPlanSource`.
struct CachedPlanSourceData {
    magic: i32,
    /// `RawStmt *raw_parse_tree` — owned in `context` (NULL == `None`).
    raw_parse_tree: Option<RawStmt<'static>>,
    /// `Query *analyzed_parse_tree` — owned in `context` (NULL == `None`).
    analyzed_parse_tree: Option<Query<'static>>,
    query_string: String,
    command_tag: CommandTag,
    param_types: Vec<Oid>,
    num_params: i32,
    /// `bool has_parser_setup` — C `parserSetup != NULL`. The owned model has
    /// no value parser-setup hook (only reached for PREPARE-with-$n / SPI; see
    /// map B2), so revalidation of such a source is an unsupported-feature path.
    has_parser_setup: bool,
    /// `void (*postRewrite)` — present? The owned model has no value post-rewrite
    /// hook producer yet; presence is tracked, invocation is the B2 path.
    has_post_rewrite: bool,
    cursor_options: i32,
    fixed_result: bool,
    /// `TupleDesc resultDesc` — owned in `context` (NULL == `None`).
    result_desc: Option<TupleDescData<'static>>,
    /// `List *query_list` — the rewritten `Query` list, owned in `query_context`.
    query_list: Vec<Query<'static>>,
    relation_oids: Vec<Oid>,
    inval_items: Vec<InvalItemKey>,
    /// `SearchPathMatcher *search_path` — owned in `query_context`.
    search_path: Option<SearchPathMatcher<'static>>,
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
    /// `MemoryContext query_context` — backs `query_list`/`search_path`. Declared
    /// AFTER those value fields so it drops last. `None` when not yet built.
    query_context: Option<MemoryContext>,
    /// `MemoryContext context` — the source's permanent context, backs
    /// `raw_parse_tree`/`analyzed_parse_tree`/`result_desc`/`query_string`.
    /// Declared LAST so it drops after every value field above. For one-shot
    /// sources there is no dedicated context (data lives in the caller's
    /// context in C); we still own a context here for storage, faithful in
    /// behavior (dropped with the struct).
    context: MemoryContext,
}

/// `struct CachedPlan`. `generation` is written but, as in C, never read by
/// plancache itself.
#[allow(dead_code)]
struct CachedPlanData {
    magic: i32,
    /// `List *stmt_list` — owned `PlannedStmt`s in this plan's private `context`.
    stmt_list: Vec<PlannedStmt<'static>>,
    is_oneshot: bool,
    is_saved: bool,
    is_valid: bool,
    plan_role_id: Oid,
    depends_on_role: bool,
    saved_xmin: u32,
    generation: i32,
    refcount: i32,
    /// `MemoryContext context` — declared AFTER `stmt_list` so it drops last.
    context: MemoryContext,
}

/// `struct CachedExpression`. `expr` holds the planned expression.
#[allow(dead_code)]
struct CachedExpressionData {
    magic: i32,
    /// `Node *expr` — owned planned expression, allocated into `context` below
    /// (see `GetCachedExpression`). Its `PgBox`/`PgVec` children live in that
    /// context, so `context` must outlive `expr`.
    expr: Expr,
    is_valid: bool,
    relation_oids: Vec<Oid>,
    inval_items: Vec<InvalItemKey>,
    /// `MemoryContext context` — backs `expr`'s node children; declared AFTER
    /// `expr` so it drops last (the node's allocator must outlive the node).
    context: MemoryContext,
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
 * `'static` clone helpers — the portalmem pattern.  Each clones a value into
 * the named `MemoryContext` and extends the borrow to the context-lived
 * `'static` marker. SOUND because the destination context is owned by the same
 * struct and dropped AFTER the value field (see the field-order notes above).
 * ======================================================================== */

/// `MemoryContextSwitchTo(ctx); copyObject(query)` — clone `query` into `ctx`.
fn clone_query_into(ctx: &MemoryContext, query: &Query<'_>) -> PgResult<Query<'static>> {
    let copied = query.clone_in(ctx.mcx())?;
    // SAFETY: `copied` lives in `ctx` (real owned heap freed by its own
    // PgBox/PgVec Drop). `ctx` is dropped after the value field it backs.
    Ok(unsafe { core::mem::transmute::<Query<'_>, Query<'static>>(copied) })
}

/// Clone a `Query` list into `ctx`.
fn clone_query_list_into(
    ctx: &MemoryContext,
    list: &[Query<'_>],
) -> PgResult<Vec<Query<'static>>> {
    let mut out = Vec::with_capacity(list.len());
    for q in list {
        out.push(clone_query_into(ctx, q)?);
    }
    Ok(out)
}

/// Clone a `PlannedStmt` list into `ctx`.
fn clone_plan_list_into(
    ctx: &MemoryContext,
    list: &[PlannedStmt<'_>],
) -> PgResult<Vec<PlannedStmt<'static>>> {
    let mut out = Vec::with_capacity(list.len());
    for s in list {
        let copied = s.clone_in(ctx.mcx())?;
        out.push(unsafe { core::mem::transmute::<PlannedStmt<'_>, PlannedStmt<'static>>(copied) });
    }
    Ok(out)
}

/// Clone a `RawStmt` into `ctx`.
fn clone_raw_into(ctx: &MemoryContext, raw: &RawStmt<'_>) -> PgResult<RawStmt<'static>> {
    let copied = raw.clone_in(ctx.mcx())?;
    Ok(unsafe { core::mem::transmute::<RawStmt<'_>, RawStmt<'static>>(copied) })
}

/// Clone a `TupleDescData` into `ctx` (C `CreateTupleDescCopy`).
fn clone_tupdesc_into(
    ctx: &MemoryContext,
    td: &TupleDescData<'_>,
) -> PgResult<TupleDescData<'static>> {
    let copied = td.clone_in(ctx.mcx())?;
    Ok(unsafe { core::mem::transmute::<TupleDescData<'_>, TupleDescData<'static>>(copied) })
}

/// Clone a `SearchPathMatcher` into `ctx` (the `query_context` it is saved in).
fn clone_search_path_into(
    ctx: &MemoryContext,
    sp: &SearchPathMatcher<'_>,
) -> PgResult<SearchPathMatcher<'static>> {
    let mut schemas: PgVec<'_, Oid> = mcx::vec_with_capacity_in(ctx.mcx(), sp.schemas.len())?;
    for &o in sp.schemas.iter() {
        schemas.push(o);
    }
    let schemas = unsafe { core::mem::transmute::<PgVec<'_, Oid>, PgVec<'static, Oid>>(schemas) };
    Ok(SearchPathMatcher {
        schemas,
        addCatalog: sp.addCatalog,
        addTemp: sp.addTemp,
        generation: sp.generation,
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

/// `(plannedstmt->commandType == CMD_UTILITY)`.
#[inline]
fn pstmt_is_utility(stmt: &PlannedStmt<'_>) -> bool {
    stmt.commandType == CmdType::CMD_UTILITY
}

/// `(query->commandType == CMD_UTILITY)`.
#[inline]
fn query_is_utility(q: &Query<'_>) -> bool {
    q.commandType == CmdType::CMD_UTILITY
}

/// `elog(ERROR, ...)` — internal error.
fn elog_error(msg: &str) -> PgError {
    PgError::new(ERROR, msg.to_string())
}

/// `list_member_oid(list, oid)`.
#[inline]
fn list_member_oid(list: &[Oid], oid: Oid) -> bool {
    list.contains(&oid)
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
    context: MemoryContext,
    is_oneshot: bool,
    command_tag: CommandTag,
    query_string: String,
) -> CachedPlanSourceData {
    CachedPlanSourceData {
        magic: CACHEDPLANSOURCE_MAGIC,
        raw_parse_tree: None,
        analyzed_parse_tree: None,
        query_string,
        command_tag,
        param_types: Vec::new(),
        num_params: 0,
        has_parser_setup: false,
        has_post_rewrite: false,
        cursor_options: 0,
        fixed_result: false,
        result_desc: None,
        query_list: Vec::new(),
        relation_oids: Vec::new(),
        inval_items: Vec::new(),
        search_path: None,
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
        query_context: None,
        context,
    }
}

/// `CreateCachedPlan(raw_parse_tree, query_string, commandTag)`.
///
/// The de-handle takes the raw parse tree by reference and copies it into the
/// source's private `context` (C `copyObject(raw_parse_tree)`). A NULL raw tree
/// (empty query) is the `None`-typed companion entry `create_cached_plan_empty`.
pub fn CreateCachedPlan(
    raw_parse_tree: &RawStmt<'_>,
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    // AllocSetContextCreate(CurrentMemoryContext, "CachedPlanSource", ...).
    let context = MemoryContext::new("CachedPlanSource");

    // Build the source and intern it FIRST, so its `context` reaches its final,
    // stable heap address (inside the `Rc<RefCell<..>>`) before anything is
    // cloned into it. Cloning into a stack-local context and then *moving* that
    // context into the struct (and again into the `Rc`/HashMap) would invalidate
    // every `Mcx(&context)` borrow the clone captured — a use-after-free on drop.
    let data = new_source(context, false, command_tag, query_string.to_string());
    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.sources.insert(h, Rc::new(RefCell::new(data)));
        h
    });

    // raw_parse_tree = copyObject(raw_parse_tree) — into the now-stable context.
    let src = get_source(handle);
    let raw_copy = {
        let p = src.borrow();
        clone_raw_into(&p.context, raw_parse_tree)?
    };
    src.borrow_mut().raw_parse_tree = Some(raw_copy);
    Ok(handle)
}

/// `CreateCachedPlan(NULL, query_string, commandTag)` — the empty-query case.
pub fn CreateCachedPlanEmpty(
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    let context = MemoryContext::new("CachedPlanSource");
    let data = new_source(context, false, command_tag, query_string.to_string());
    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.sources.insert(h, Rc::new(RefCell::new(data)));
        h
    });
    Ok(handle)
}

/// `CreateCachedPlanForQuery(analyzed_parse_tree, query_string, commandTag)`.
pub fn CreateCachedPlanForQuery(
    analyzed_parse_tree: &Query<'_>,
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    let plansource = CreateCachedPlanEmpty(query_string, command_tag)?;
    let src = get_source(plansource);
    let copy = {
        let p = src.borrow();
        clone_query_into(&p.context, analyzed_parse_tree)?
    };
    src.borrow_mut().analyzed_parse_tree = Some(copy);
    Ok(plansource)
}

/// `CreateOneShotCachedPlan(raw_parse_tree, query_string, commandTag)`.
///
/// In C the raw tree is NOT copied (it lives in the caller's context). The owned
/// model still copies it into the source's context (it has nowhere else to own
/// it from); behavior is identical — the data disappears with the source.
pub fn CreateOneShotCachedPlan(
    raw_parse_tree: &RawStmt<'_>,
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<CachedPlanSourceHandle> {
    // Intern the source FIRST so its `context` is at its final stable address
    // before cloning into it (see CreateCachedPlan — a move-after-clone dangles
    // every captured `Mcx(&context)` borrow, a use-after-free on drop).
    let context = MemoryContext::new("CachedPlanSource");
    let data = new_source(context, true, command_tag, query_string.to_string());
    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.sources.insert(h, Rc::new(RefCell::new(data)));
        h
    });

    let src = get_source(handle);
    let raw_copy = {
        let p = src.borrow();
        clone_raw_into(&p.context, raw_parse_tree)?
    };
    src.borrow_mut().raw_parse_tree = Some(raw_copy);
    Ok(handle)
}

/* ==========================================================================
 * CompleteCachedPlan
 * ======================================================================== */

/// `CompleteCachedPlan(plansource, querytree_list, querytree_context, ...)`.
///
/// `querytree_list` is the rewritten `Query` list (a slice of `Node::Query`).
/// The de-handle always owns the querytree in a freshly created `query_context`
/// (the C `querytree_context == NULL` path — callers in this model never hand
/// in a pre-built context).
#[allow(clippy::too_many_arguments)]
pub fn CompleteCachedPlan(
    plansource: CachedPlanSourceHandle,
    querytree_list: &[Query<'_>],
    param_types: &[Oid],
    num_params: i32,
    has_parser_setup: bool,
    cursor_options: i32,
    fixed_result: bool,
) -> PgResult<()> {
    let src = get_source(plansource);

    let is_oneshot = {
        let p = src.borrow();
        debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
        debug_assert!(!p.is_complete);
        p.is_oneshot
    };

    // querytree_context: a fresh child for non-one-shot; for one-shot the data
    // lives directly in `context`. Install it into the interned source FIRST so
    // it is at its final stable heap address before cloning into it — cloning
    // into a stack-local context then moving it into `p.query_context` would
    // dangle every captured `Mcx(&context)` borrow (use-after-free on drop).
    if !is_oneshot {
        src.borrow_mut().query_context = Some(MemoryContext::new("CachedPlanQuery"));
    }

    // Copy the querytree list into the now-stable owning context.
    let owned_qlist = {
        let p = src.borrow();
        let ctx = p.query_context.as_ref().unwrap_or(&p.context);
        clone_query_list_into(ctx, querytree_list)?
    };

    src.borrow_mut().query_list = owned_qlist;

    if !is_oneshot && StmtPlanRequiresRevalidation(&src)? {
        // extract_query_dependencies over the owned query list.
        let deps = {
            let p = src.borrow();
            extract_deps(&p.query_list)?
        };
        let role = backend_seams::get_user_id::call()?;
        let rsec = backend_seams::row_security::call()?;
        let sp = get_search_path_into(&src)?;
        let mut p = src.borrow_mut();
        p.relation_oids = deps.0;
        p.inval_items = deps.1;
        p.depends_on_rls = deps.2;
        p.rewrite_role_id = role;
        p.rewrite_row_security = rsec;
        p.search_path = Some(sp);
    }

    {
        let mut p = src.borrow_mut();
        if num_params > 0 {
            p.param_types = param_types.to_vec();
        } else {
            p.param_types = Vec::new();
        }
        p.num_params = num_params;
        p.has_parser_setup = has_parser_setup;
        p.cursor_options = cursor_options;
        p.fixed_result = fixed_result;
    }

    // result_desc = PlanCacheComputeResultDesc(querytree_list), owned in context.
    let result_desc = {
        let p = src.borrow();
        compute_result_desc_into(&p.context, &p.query_list)?
    };
    src.borrow_mut().result_desc = result_desc;

    let mut p = src.borrow_mut();
    p.is_complete = true;
    p.is_valid = true;
    Ok(())
}

/* ==========================================================================
 * SaveCachedPlan
 * ======================================================================== */

/// `SaveCachedPlan(plansource)`.
///
/// In C this reparents the source's context under `CacheMemoryContext` so it
/// lives indefinitely. In the owned model the source already outlives the call
/// (it is in the registry); "saving" only adds it to `saved_plan_list` and
/// flips `is_saved` so the invalidation callbacks scan it. The context lifetime
/// is governed by the struct (dropped on `DropCachedPlan`).
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
    let is_saved = {
        let p = src.borrow();
        debug_assert_eq!(p.magic, CACHEDPLANSOURCE_MAGIC);
        p.is_saved
    };

    if is_saved {
        with_state(|s| s.saved_plan_list.retain(|&h| h != plansource));
        src.borrow_mut().is_saved = false;
    }

    ReleaseGenericPlan(plansource)?;

    src.borrow_mut().magic = 0;

    // MemoryContextDelete(plansource->context) — happens when the struct (and
    // its owned `context`/`query_context`) drops as we remove it from the
    // registry below (the last `Rc` reference).
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
    if let Some(raw) = &p.raw_parse_tree {
        analyze_seams::stmt_requires_parse_analysis_value::call(raw)
    } else if let Some(analyzed) = &p.analyzed_parse_tree {
        analyze_seams::query_requires_rewrite_plan_value::call(analyzed)
    } else {
        // empty query never needs revalidation
        Ok(false)
    }
}

/// `BuildingPlanRequiresSnapshot(plansource)`.
fn BuildingPlanRequiresSnapshot(src: &SourceRc) -> PgResult<bool> {
    let p = src.borrow();
    if let Some(raw) = &p.raw_parse_tree {
        analyze_seams::analyze_requires_snapshot_value::call(raw)
    } else if let Some(analyzed) = &p.analyzed_parse_tree {
        analyze_seams::query_requires_rewrite_plan_value::call(analyzed)
    } else {
        Ok(false)
    }
}

/* ==========================================================================
 * Dependency / search-path / result-desc value helpers.
 * ======================================================================== */

/// `extract_query_dependencies((Node *) qlist, &relationOids, &invalItems,
/// &dependsOnRLS)` over the owned `Query` list.
fn extract_deps(qlist: &[Query<'_>]) -> PgResult<(Vec<Oid>, Vec<InvalItemKey>, bool)> {
    // The value producer wants `&[Query<'mcx>]` for some `'mcx`; our list is
    // `Query<'static>`. Run it in a scratch context so the produced
    // dependencies (plain Oid/(i32,u32) scalars) are independent of any arena.
    let scratch = MemoryContext::new("plancache_extract_deps");
    let deps = setrefs_seams::extract_query_dependencies_value::call(scratch.mcx(), qlist)?;
    let inval_items = deps
        .inval_items
        .into_iter()
        .map(|(cache_id, hash_value)| InvalItemKey {
            cache_id,
            hash_value,
        })
        .collect();
    Ok((deps.relation_oids, inval_items, deps.depends_on_rls))
}

/// `GetSearchPathMatcher(querytree_context)` — fetch the current search path and
/// own it in the source's `query_context` (falls back to `context`).
fn get_search_path_into(src: &SourceRc) -> PgResult<SearchPathMatcher<'static>> {
    let scratch = MemoryContext::new("plancache_search_path");
    let sp = namespace_seams::get_search_path_matcher_value::call(scratch.mcx())?;
    let p = src.borrow();
    let ctx = p.query_context.as_ref().unwrap_or(&p.context);
    clone_search_path_into(ctx, &sp)
}

/// `PlanCacheComputeResultDesc(stmt_list)` with the descriptor owned in `ctx`.
fn compute_result_desc_into(
    ctx: &MemoryContext,
    stmt_list: &[Query<'_>],
) -> PgResult<Option<TupleDescData<'static>>> {
    PlanCacheComputeResultDesc(ctx, stmt_list)
}

/* ==========================================================================
 * RevalidateCachedQuery (static)
 * ======================================================================== */

/// `RevalidateCachedQuery(plansource, queryEnv)`. Returns the transient
/// re-analyzed/rewritten query list (empty when no re-analysis happened), to
/// save `BuildCachedPlan` a copy step.
fn RevalidateCachedQuery(
    plansource: CachedPlanSourceHandle,
    query_env: Option<&QueryEnvironment<'_>>,
) -> PgResult<Vec<Query<'static>>> {
    let src = get_source(plansource);

    if src.borrow().is_oneshot || !StmtPlanRequiresRevalidation(&src)? {
        debug_assert!(src.borrow().is_valid);
        return Ok(Vec::new());
    }

    if src.borrow().is_valid {
        let matches = {
            let mut p = src.borrow_mut();
            debug_assert!(p.search_path.is_some());
            let scratch = MemoryContext::new("plancache_spmatch");
            let sp = p.search_path.as_mut().unwrap();
            // SAFETY: the value seam takes &mut SearchPathMatcher<'mcx>; our
            // stored matcher is `'static`. Reborrow to the scratch lifetime for
            // the call (it only reads/updates scalar fields).
            let sp_ref: &mut SearchPathMatcher<'_> = unsafe {
                core::mem::transmute::<&mut SearchPathMatcher<'static>, &mut SearchPathMatcher<'_>>(
                    sp,
                )
            };
            namespace_seams::search_path_matches_current_environment_value::call(
                scratch.mcx(),
                sp_ref,
            )?
        };
        if !matches {
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
        AcquirePlannerLocks(&src, true)?;

        if src.borrow().is_valid {
            return Ok(Vec::new());
        }

        // Oops, the race case happened.  Release useless locks.
        AcquirePlannerLocks(&src, false)?;
    }

    {
        let mut p = src.borrow_mut();
        p.is_valid = false;
        p.query_list = Vec::new();
        p.relation_oids = Vec::new();
        p.inval_items = Vec::new();
        p.search_path = None;
    }

    // Free the query_context (drop the owned MemoryContext).
    src.borrow_mut().query_context = None;

    ReleaseGenericPlan(plansource)?;

    debug_assert!(src.borrow().is_complete);

    let mut snapshot_set = false;
    if !snapmgr_seams::active_snapshot_set::call()? {
        snapmgr_seams::push_active_snapshot_transaction::call()?;
        snapshot_set = true;
    }

    // Re-do parse analysis (if needed) and rule rewriting. The produced `tlist`
    // is built in a transient context held for the rest of this routine (C uses
    // the caller's CurrentMemoryContext); we copy what we keep into the
    // permanent `query_context`. C returns `tlist` to save BuildCachedPlan a
    // copy; we instead always return NIL and let BuildCachedPlan copy from the
    // stored `query_list` (behaviorally identical — `tlist` is purely a copy
    // optimization, plancache.c:662-664).
    let transient = MemoryContext::new("CachedPlanRevalidate");
    let tlist: PgVec<'_, Query<'_>> = {
        let has_raw = src.borrow().raw_parse_tree.is_some();
        let has_analyzed = src.borrow().analyzed_parse_tree.is_some();
        if has_raw {
            let has_setup = src.borrow().has_parser_setup;
            if has_setup {
                // parserSetup branch (PREPARE-with-$n / SPI). No value
                // parse_analyze_withcb producer exists (map B2); precise panic.
                let _ = query_env;
                panic!(
                    "plancache RevalidateCachedQuery: parserSetup (parse_analyze_withcb) \
                     value path not yet ported (PREPARE-with-$n / SPI only; map B2)"
                );
            } else {
                let p = src.borrow();
                let raw = p.raw_parse_tree.as_ref().unwrap();
                let rawtree = raw.clone_in(transient.mcx())?;
                // analyze_and_rewrite_fixedparams: parse-analyze + rewrite.
                analyze_seams::pg_analyze_and_rewrite_fixedparams_params::call(
                    transient.mcx(),
                    &rawtree,
                    p.query_string.as_str(),
                    p.param_types.as_slice(),
                )?
            }
        } else if has_analyzed {
            let p = src.borrow();
            let analyzed = p.analyzed_parse_tree.as_ref().unwrap();
            let analyzed_tree = analyzed.clone_in(transient.mcx())?;
            // AcquireRewriteLocks(analyzed_tree, true, false) then rewrite.
            let locked = rewrite_seams::acquire_rewrite_locks::call(
                transient.mcx(),
                analyzed_tree,
                true,
                false,
            )?;
            rewrite_seams::query_rewrite_canonical::call(transient.mcx(), locked)?
        } else {
            mcx::vec_with_capacity_in(transient.mcx(), 0)?
        }
    };

    // Apply post-rewrite callback if there is one (B2: no value producer).
    if src.borrow().has_post_rewrite {
        panic!(
            "plancache RevalidateCachedQuery: postRewrite hook value path not yet ported (map B2)"
        );
    }

    if snapshot_set {
        snapmgr_seams::pop_active_snapshot::call()?;
    }

    // Check or update the result tupdesc. Compute into a transient context held
    // alive through the comparison; copy into the permanent context only if it
    // actually changed.
    let desc_scratch = MemoryContext::new("plancache_newdesc");
    {
        // `new_desc` is charged to `desc_scratch`; it MUST drop before
        // `desc_scratch` is freed (otherwise its TupleDesc uncharges a
        // already-freed context). Scope it so it drops at the end of this block,
        // ahead of `drop(desc_scratch)`.
        let new_desc = PlanCacheComputeResultDesc(&desc_scratch, tlist.as_slice())?;
        let had_existing = src.borrow().result_desc.is_some();
        let equal = match (&new_desc, src.borrow().result_desc.as_ref()) {
            (Some(a), Some(b)) => tupdesc_seams::equal_row_types::call(a, b),
            _ => false,
        };
        if new_desc.is_none() && !had_existing {
            // OK, doesn't return tuples.
        } else if new_desc.is_none() || !had_existing || !equal {
            if src.borrow().fixed_result {
                return Err(PgError::new(
                    ERROR,
                    "cached plan must not change result type".to_string(),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            // Copy the new descriptor into the source's permanent context; drop
            // the old one (FreeTupleDesc).
            let owned = match &new_desc {
                Some(td) => {
                    let p = src.borrow();
                    Some(clone_tupdesc_into(&p.context, td)?)
                }
                None => None,
            };
            src.borrow_mut().result_desc = owned;
        }
    }
    drop(desc_scratch);

    // Install the new query_context into the interned source FIRST so it is at
    // its final stable heap address before cloning into it — cloning into a
    // stack-local context then moving it into `p.query_context` dangles every
    // captured `Mcx(&context)` borrow (use-after-free on drop).
    src.borrow_mut().query_context = Some(MemoryContext::new("CachedPlanQuery"));
    let qlist = {
        let p = src.borrow();
        let qctx = p.query_context.as_ref().expect("just set");
        clone_query_list_into(qctx, tlist.as_slice())?
    };

    let deps = extract_deps(&qlist)?;
    let role = backend_seams::get_user_id::call()?;
    let rsec = backend_seams::row_security::call()?;
    let scratch = MemoryContext::new("plancache_sp");
    let sp_val = namespace_seams::get_search_path_matcher_value::call(scratch.mcx())?;
    let sp = {
        let p = src.borrow();
        let qctx = p.query_context.as_ref().expect("just set");
        clone_search_path_into(qctx, &sp_val)?
    };

    {
        let mut p = src.borrow_mut();
        p.relation_oids = deps.0;
        p.inval_items = deps.1;
        p.depends_on_rls = deps.2;
        p.rewrite_role_id = role;
        p.rewrite_row_security = rsec;
        p.search_path = Some(sp);
        p.query_list = qlist;
        // Note: we do not reset generic_cost or total_custom_cost.
        p.is_valid = true;
    }

    // `transient` (and `tlist` borrowing it) drop here in reverse declaration
    // order — `tlist` first, then `transient`.
    Ok(Vec::new())
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

        AcquireExecutorLocks(&plan, true)?;

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

        AcquireExecutorLocks(&plan, false)?;
    }

    ReleaseGenericPlan(plansource)?;

    Ok(false)
}

/* ==========================================================================
 * BuildCachedPlan (static)
 * ======================================================================== */

/// `BuildCachedPlan(plansource, qlist, boundParams, queryEnv)`.
///
/// `qlist_in` is the transient query list returned by RevalidateCachedQuery; in
/// the owned model it is always empty (RevalidateCachedQuery returns NIL), so we
/// copy from the stored `query_list`.
fn BuildCachedPlan(
    plansource: CachedPlanSourceHandle,
    qlist_in: &[Query<'_>],
    bound_params: ParamListInfo,
    query_env: Option<&QueryEnvironment<'_>>,
) -> PgResult<CachedPlanHandle> {
    let src = get_source(plansource);

    let mut revalidated: Vec<Query<'static>> = Vec::new();
    let use_revalidated;
    if !src.borrow().is_valid {
        revalidated = RevalidateCachedQuery(plansource, query_env)?;
        use_revalidated = !revalidated.is_empty();
    } else {
        use_revalidated = !qlist_in.is_empty();
    }

    // Build the query list to plan, copied into a transient context.
    let transient = MemoryContext::new("CachedPlanWork");
    let qlist: Vec<Query<'static>> = if use_revalidated {
        // qlist_in / revalidated already live (transient or caller); copy.
        if !qlist_in.is_empty() {
            clone_query_list_into(&transient, qlist_in)?
        } else {
            clone_query_list_into(&transient, revalidated.as_slice())?
        }
    } else {
        let is_oneshot = src.borrow().is_oneshot;
        let p = src.borrow();
        if !is_oneshot {
            clone_query_list_into(&transient, p.query_list.as_slice())?
        } else {
            // One-shot: C consumes query_list in place; we copy (behaviorally
            // identical — the one-shot source is dropped right after).
            clone_query_list_into(&transient, p.query_list.as_slice())?
        }
    };

    let mut snapshot_set = false;
    if !snapmgr_seams::active_snapshot_set::call()? && BuildingPlanRequiresSnapshot(&src)? {
        snapmgr_seams::push_active_snapshot_transaction::call()?;
        snapshot_set = true;
    }

    let (qstr, cursor_options) = {
        let p = src.borrow();
        (p.query_string.clone(), p.cursor_options)
    };

    // pg_plan_queries(qlist, query_string, cursor_options, boundParams). The
    // generic-plan spine passes NULL boundParams; custom-plan substitution
    // passes the bound value param list through to the planner so the planner
    // can fold `$n` consts (`ParamListInfo` is now a real shared value, not an
    // opaque handle).
    //
    // Intern the CachedPlan with an empty stmt_list FIRST so its `context`
    // reaches its final stable heap address inside the `Rc`, THEN clone the
    // planned stmts into that now-stable context. Cloning into a stack-local
    // `plan_context` and then moving it into `CachedPlanData`/the `Rc` would
    // dangle every captured `Mcx(&context)` borrow (use-after-free on drop).
    let is_oneshot = src.borrow().is_oneshot;
    let plan_role_id = backend_seams::get_user_id::call()?;
    let generation = {
        let mut p = src.borrow_mut();
        p.generation += 1;
        p.generation
    };
    let plan_handle = with_state(|s| {
        let h = s.alloc_handle();
        s.plans.insert(
            h,
            Rc::new(RefCell::new(CachedPlanData {
                magic: CACHEDPLAN_MAGIC,
                stmt_list: Vec::new(),
                is_oneshot,
                is_saved: false,
                is_valid: true,
                plan_role_id,
                depends_on_role: false,
                saved_xmin: 0,
                generation,
                refcount: 0,
                context: MemoryContext::new("CachedPlan"),
            })),
        );
        h
    });
    let plan_rc = with_state(|s| s.plans.get(&plan_handle).cloned().expect("just inserted"));

    let plist: Vec<PlannedStmt<'static>> = {
        let plan_p = plan_rc.borrow();
        let planned = postgres_seams::pg_plan_queries_value::call(
            plan_p.context.mcx(),
            qlist.as_slice(),
            qstr.as_str(),
            cursor_options,
            bound_params.as_deref(),
        )?;
        // Own the planned stmts in the now-stable plan context.
        clone_plan_list_into(&plan_p.context, planned.as_slice())?
    };

    // qlist's nodes were allocated in `transient`; drop them before that arena
    // is freed so their deallocators uncharge against a live context.
    drop(qlist);

    if snapshot_set {
        snapmgr_seams::pop_active_snapshot::call()?;
    }

    let mut depends_on_role = src.borrow().depends_on_rls;
    let mut is_transient = false;
    for stmt in plist.iter() {
        if pstmt_is_utility(stmt) {
            continue; // Ignore utility statements.
        }
        if stmt.transientPlan {
            is_transient = true;
        }
        if stmt.dependsOnRole {
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

    // Fill in the remaining fields on the already-interned plan (its `context`
    // and the `stmt_list` cloned into it are at their final stable addresses).
    {
        let mut plan_p = plan_rc.borrow_mut();
        plan_p.stmt_list = plist;
        plan_p.depends_on_role = depends_on_role;
        plan_p.saved_xmin = saved_xmin;
    }

    drop(transient);

    Ok(plan_handle)
}

/* ==========================================================================
 * choose_custom_plan (static)
 * ======================================================================== */

/// `choose_custom_plan(plansource, boundParams)`.
fn choose_custom_plan(
    plansource: CachedPlanSourceHandle,
    bound_params: &ParamListInfo,
) -> PgResult<bool> {
    let src = get_source(plansource);

    if src.borrow().is_oneshot {
        return Ok(true);
    }

    if bound_params.is_none() {
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
    let pl = get_plan(plan);
    let p = pl.borrow();

    for stmt in p.stmt_list.iter() {
        if pstmt_is_utility(stmt) {
            continue; // Ignore utility statements.
        }

        result += stmt.plan_total_cost();

        if include_planner {
            let nrelations = stmt.rtable.as_ref().map(|r| r.len()).unwrap_or(0);
            let cpu_operator_cost = planner_pc_seams::cpu_operator_cost::call()?;
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
    bound_params: ParamListInfo,
    owner: ResourceOwnerHandle,
    query_env: Option<&QueryEnvironment<'_>>,
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

    let qlist = RevalidateCachedQuery(plansource, query_env)?;
    let mut have_qlist = !qlist.is_empty();

    let mut customplan = choose_custom_plan(plansource, &bound_params)?;

    if !customplan {
        if CheckCachedPlan(plansource)? {
            plan = src.borrow().gplan;
            debug_assert_eq!(get_plan(plan).borrow().magic, CACHEDPLAN_MAGIC);
        } else {
            let qslice: &[Query<'_>] = if have_qlist { qlist.as_slice() } else { &[] };
            plan = BuildCachedPlan(plansource, qslice, None, query_env)?;
            ReleaseGenericPlan(plansource)?;
            src.borrow_mut().gplan = plan;
            get_plan(plan).borrow_mut().refcount += 1;
            // is_saved propagation (context reparenting is a no-op in the owned
            // model — the plan's context is owned by the struct).
            let is_saved = src.borrow().is_saved;
            if is_saved {
                get_plan(plan).borrow_mut().is_saved = true;
            }
            let cost = cached_plan_cost(plan, false)?;
            src.borrow_mut().generic_cost = cost;

            customplan = choose_custom_plan(plansource, &bound_params)?;

            have_qlist = false;
        }
    }

    if customplan {
        let qslice: &[Query<'_>] = if have_qlist { qlist.as_slice() } else { &[] };
        plan = BuildCachedPlan(plansource, qslice, bound_params, query_env)?;
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
        get_plan(plan).borrow_mut().is_saved = true;
    }

    Ok(plan)
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
        // MemoryContextDelete(plan->context) — happens when the struct (owning
        // its `context`) drops as the last Rc is removed from the registry.
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
    debug_assert!(src.borrow().search_path.is_some());

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
    {
        let p = src.borrow();
        for query in p.query_list.iter() {
            if query_is_utility(query) {
                return Ok(false);
            }
            if !query.rtable.is_empty()
                || !query.cteList.is_empty()
                || query.hasSubLinks
            {
                return Ok(false);
            }
        }
    }

    // Reject if AcquireExecutorLocks would have anything to do.
    {
        let p = pl.borrow();
        for stmt in p.stmt_list.iter() {
            if pstmt_is_utility(stmt) {
                return Ok(false);
            }
            // grovel through the rtable for an RTE_RELATION.
            if let Some(rtable) = &stmt.rtable {
                for rte in rtable.iter() {
                    if rte.rtekind == RTEKind_relation() {
                        return Ok(false);
                    }
                }
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

    {
        let mut p = src.borrow_mut();
        debug_assert!(p.search_path.is_some());
        let scratch = MemoryContext::new("plancache_spmatch2");
        let sp = p.search_path.as_mut().unwrap();
        let sp_ref: &mut SearchPathMatcher<'_> = unsafe {
            core::mem::transmute::<&mut SearchPathMatcher<'static>, &mut SearchPathMatcher<'_>>(sp)
        };
        if !namespace_seams::search_path_matches_current_environment_value::call(
            scratch.mcx(),
            sp_ref,
        )? {
            return Ok(false);
        }
    }

    if !owner.is_null() {
        resowner_seams::resource_owner_enlarge::call(owner)?;
        pl.borrow_mut().refcount += 1;
        resowner_seams::resource_owner_remember_plan::call(owner, plan)?;
    }

    Ok(true)
}

/// `RTE_RELATION` as the model's `RTEKind`.
#[inline]
fn RTEKind_relation() -> types_nodes::parsenodes::RTEKind {
    types_nodes::parsenodes::RTEKind::RTE_RELATION
}

/// `RTE_SUBQUERY` as the model's `RTEKind`.
#[inline]
fn RTEKind_subquery() -> types_nodes::parsenodes::RTEKind {
    types_nodes::parsenodes::RTEKind::RTE_SUBQUERY
}

/* ==========================================================================
 * CachedPlanSetParentContext / CopyCachedPlan
 *
 * In the owned model the source's storage context is owned by the struct (not
 * reparentable). These two entry points are not on the SELECT/EXECUTE spine and
 * have no value-model meaning for the context-reparenting half; they retain
 * their guards + the data-copy half.
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

    // Intern the destination source (with its two fresh contexts) FIRST so the
    // contexts reach their final stable heap addresses inside the `Rc`, THEN
    // clone each tree into the now-stable contexts. Cloning into stack-local
    // contexts and moving them into `newdata`/the `Rc` would dangle every
    // captured `Mcx(&context)` borrow (use-after-free on drop).
    let newdata = {
        let p = src.borrow();
        CachedPlanSourceData {
            magic: CACHEDPLANSOURCE_MAGIC,
            raw_parse_tree: None,
            analyzed_parse_tree: None,
            query_string: p.query_string.clone(),
            command_tag: p.command_tag,
            param_types: p.param_types.clone(),
            num_params: p.num_params,
            has_parser_setup: p.has_parser_setup,
            has_post_rewrite: p.has_post_rewrite,
            cursor_options: p.cursor_options,
            fixed_result: p.fixed_result,
            result_desc: None,
            query_list: Vec::new(),
            relation_oids: p.relation_oids.clone(),
            inval_items: p.inval_items.clone(),
            search_path: None,
            rewrite_role_id: p.rewrite_role_id,
            rewrite_row_security: p.rewrite_row_security,
            depends_on_rls: p.depends_on_rls,
            gplan: NULL_HANDLE,
            is_oneshot: false,
            is_complete: true,
            is_saved: false,
            is_valid: p.is_valid,
            generation: p.generation,
            generic_cost: p.generic_cost,
            total_custom_cost: p.total_custom_cost,
            num_custom_plans: p.num_custom_plans,
            num_generic_plans: p.num_generic_plans,
            query_context: Some(MemoryContext::new("CachedPlanQuery")),
            context: MemoryContext::new("CachedPlanSource"),
        }
    };

    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.sources.insert(h, Rc::new(RefCell::new(newdata)));
        h
    });

    // Now clone every tree into the destination's stable contexts.
    {
        let p = src.borrow();
        let dst = get_source(handle);
        let d = dst.borrow();
        let raw_copy = match &p.raw_parse_tree {
            Some(r) => Some(clone_raw_into(&d.context, r)?),
            None => None,
        };
        let analyzed_copy = match &p.analyzed_parse_tree {
            Some(q) => Some(clone_query_into(&d.context, q)?),
            None => None,
        };
        let result_desc_copy = match &p.result_desc {
            Some(td) => Some(clone_tupdesc_into(&d.context, td)?),
            None => None,
        };
        let qctx = d.query_context.as_ref().expect("just set");
        let query_list_copy = clone_query_list_into(qctx, p.query_list.as_slice())?;
        let search_path_copy = match &p.search_path {
            Some(sp) => Some(clone_search_path_into(qctx, sp)?),
            None => None,
        };
        drop(d);
        let mut d = dst.borrow_mut();
        d.raw_parse_tree = raw_copy;
        d.analyzed_parse_tree = analyzed_copy;
        d.result_desc = result_desc_copy;
        d.query_list = query_list_copy;
        d.search_path = search_path_copy;
    }

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

/// `CachedPlanGetTargetList(plansource, queryEnv)` — the primary query's
/// cleaned target list as owned `TargetEntry` nodes, allocated in `mcx`.
pub fn CachedPlanGetTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    plansource: CachedPlanSourceHandle,
    query_env: Option<&QueryEnvironment<'_>>,
) -> PgResult<PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>> {
    let src = get_source(plansource);

    debug_assert_eq!(src.borrow().magic, CACHEDPLANSOURCE_MAGIC);
    debug_assert!(src.borrow().is_complete);

    if src.borrow().result_desc.is_none() {
        return mcx::vec_with_capacity_in(mcx, 0);
    }

    RevalidateCachedQuery(plansource, query_env)?;

    // QueryListGetPrimaryStmt(plansource->query_list)->targetList, cleaned.
    let p = src.borrow();
    let primary = query_list_get_primary_stmt(&p.query_list);
    match primary {
        Some(q) => {
            let mut out = mcx::vec_with_capacity_in(mcx, q.targetList.len())?;
            for te in q.targetList.iter() {
                out.push(te.clone_in(mcx)?);
            }
            Ok(out)
        }
        None => mcx::vec_with_capacity_in(mcx, 0),
    }
}

/* ==========================================================================
 * GetCachedExpression / FreeCachedExpression
 * ======================================================================== */

/// `GetCachedExpression(expr)`.
pub fn GetCachedExpression(expr: Expr) -> PgResult<CachedExpressionHandle> {
    // expression_planner_with_deps(expr, &relationOids, &invalItems).
    //
    // The planned `Expr` is cached at backend lifetime (in `s.expressions` /
    // `cached_expression_list`) and evaluated on later calls, so it MUST outlive
    // this function. A planned `Expr` tree can embed context-allocated
    // `mcx::PgBox`/`PgVec` children (const-folded sub-expressions the planner
    // builds into the passed `Mcx`), so planning into a transient context freed
    // on return would dangle them — a later evaluation or the cache drop would
    // double-free through a NULL `Mcx` (SIGSEGV), the parser-coerce crash class.
    // C plans into `cexpr->context`; we mirror that here by creating the
    // per-CachedExpression `context` FIRST and planning into IT, so the node's
    // children live exactly as long as the `CachedExpressionData` that owns them
    // (`context` is declared after `expr` in the struct, so it drops last).
    let context = MemoryContext::new("CachedExpression");
    let (planned_expr, relation_oids, inval_items) =
        planner_pc_seams::expression_planner_with_deps_value::call(context.mcx(), expr)?;

    let data = CachedExpressionData {
        magic: CACHEDEXPR_MAGIC,
        expr: planned_expr,
        is_valid: true,
        relation_oids,
        inval_items,
        context,
    };

    let handle = with_state(|s| {
        let h = s.alloc_handle();
        s.expressions.insert(h, Rc::new(RefCell::new(data)));
        s.cached_expression_list.push(h);
        h
    });
    Ok(handle)
}

/// `FreeCachedExpression(cexpr)`.
pub fn FreeCachedExpression(cexpr: CachedExpressionHandle) -> PgResult<()> {
    let ce = get_expr(cexpr);
    debug_assert_eq!(ce.borrow().magic, CACHEDEXPR_MAGIC);
    with_state(|s| s.cached_expression_list.retain(|&h| h != cexpr));
    // MemoryContextDelete(cexpr->context) — happens when the struct drops below.
    with_state(|s| {
        s.expressions.remove(&cexpr);
    });
    Ok(())
}

/* ==========================================================================
 * QueryListGetPrimaryStmt (static)
 * ======================================================================== */

/// `QueryListGetPrimaryStmt(stmts)` — the first `canSetTag` query, if any.
fn query_list_get_primary_stmt<'a>(stmts: &'a [Query<'_>]) -> Option<&'a Query<'a>> {
    // SAFETY: lifetime-narrowing of the inner 'mcx to the borrow 'a; we only
    // read the returned reference within the borrow scope.
    stmts.iter().find(|q| q.canSetTag).map(|q| unsafe {
        core::mem::transmute::<&Query<'_>, &Query<'a>>(q)
    })
}

/* ==========================================================================
 * AcquireExecutorLocks (static)
 * ======================================================================== */

/// `AcquireExecutorLocks(stmt_list, acquire)`.
fn AcquireExecutorLocks(plan: &PlanRc, acquire: bool) -> PgResult<()> {
    let p = plan.borrow();
    for stmt in p.stmt_list.iter() {
        if pstmt_is_utility(stmt) {
            // Ignore utility statements, except those that contain a query.
            if let Some(util) = &stmt.utilityStmt {
                if let Some(q) = utility_contains_query_value(util) {
                    ScanQueryForLocks(q, acquire)?;
                }
            }
            continue;
        }

        if let Some(rtable) = &stmt.rtable {
            for rte in rtable.iter() {
                if !(rte.rtekind == RTEKind_relation()
                    || (rte.rtekind == RTEKind_subquery() && oid_is_valid(rte.relid)))
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
    }
    Ok(())
}

/* ==========================================================================
 * AcquirePlannerLocks (static)
 * ======================================================================== */

/// `AcquirePlannerLocks(stmt_list, acquire)`.
fn AcquirePlannerLocks(src: &SourceRc, acquire: bool) -> PgResult<()> {
    let p = src.borrow();
    for query in p.query_list.iter() {
        if query_is_utility(query) {
            // Ignore utility statements, unless they contain a Query.
            if let Some(util) = &query.utilityStmt {
                if let Some(inner) = utility_contains_query_value(util) {
                    ScanQueryForLocks(inner, acquire)?;
                }
            }
            continue;
        }
        ScanQueryForLocks(query, acquire)?;
    }
    Ok(())
}

/// `UtilityContainsQuery(parsetree)` (utility.c:2178) over the owned utility
/// `Node` — the embedded `Query` of an EXPLAIN / CTAS / DECLARE CURSOR, drilling
/// through nested utility-`Query` wrappers to a non-utility `Query`. Returns
/// `None` for utilities that do not contain a query.
fn utility_contains_query_value<'a>(util: &'a Node<'_>) -> Option<&'a Query<'a>> {
    let qry: Option<&Node<'_>> = match util.node_tag() {
        ntag::T_DeclareCursorStmt => util.expect_declarecursorstmt().query.as_deref(),
        ntag::T_ExplainStmt => util.expect_explainstmt().query.as_deref(),
        ntag::T_CreateTableAsStmt => util.expect_createtableasstmt().query.as_deref(),
        _ => return None,
    };
    match qry {
        Some(node) => match node.as_query() {
            Some(q) => {
                if q.commandType == CmdType::CMD_UTILITY {
                    match q.utilityStmt.as_deref() {
                        Some(inner) => utility_contains_query_value(inner),
                        None => None,
                    }
                } else {
                    Some(unsafe { core::mem::transmute::<&Query<'_>, &Query<'a>>(q) })
                }
            }
            None => None,
        },
        None => None,
    }
}

/* ==========================================================================
 * ScanQueryForLocks / ScanQueryWalker (static)
 * ======================================================================== */

/// `ScanQueryForLocks(parsetree, acquire)`.
fn ScanQueryForLocks(parsetree: &Query<'_>, acquire: bool) -> PgResult<()> {
    debug_assert!(!query_is_utility(parsetree));

    // First, process RTEs of the current query level.
    for rte in parsetree.rtable.iter() {
        if rte.rtekind == RTEKind_relation() {
            if acquire {
                lmgr_seams::lock_relation_oid::call(rte.relid, rte.rellockmode)?;
            } else {
                lmgr_seams::unlock_relation_oid::call(rte.relid, rte.rellockmode)?;
            }
        } else if rte.rtekind == RTEKind_subquery() {
            // If this was a view, must lock/unlock the view.
            if oid_is_valid(rte.relid) {
                if acquire {
                    lmgr_seams::lock_relation_oid::call(rte.relid, rte.rellockmode)?;
                } else {
                    lmgr_seams::unlock_relation_oid::call(rte.relid, rte.rellockmode)?;
                }
            }
            // Recurse into subquery-in-FROM.
            if let Some(subq) = &rte.subquery {
                ScanQueryForLocks(subq, acquire)?;
            }
        }
        // else: ignore other types of RTEs.
    }

    // Recurse into subquery-in-WITH.
    for cte in parsetree.cteList.iter() {
        // cte is a CommonTableExpr Node; its ctequery is the embedded Query.
        if let Some(q) = cte_query(cte) {
            ScanQueryForLocks(q, acquire)?;
        }
    }

    // Recurse into sublink subqueries, too (rtable + cteList already done).
    if parsetree.hasSubLinks {
        scan_query_sublinks(parsetree, acquire)?;
    }
    Ok(())
}

/// `castNode(Query, cte->ctequery)` — the embedded `Query` of a
/// `CommonTableExpr` node (post-analysis).
fn cte_query<'a>(cte: &'a Node<'_>) -> Option<&'a Query<'a>> {
    if let Some(c) = cte.as_commontableexpr() {
        c.ctequery
            .as_deref()
            .and_then(|n| n.as_query())
            .map(|q| unsafe { core::mem::transmute::<&Query<'_>, &Query<'a>>(q) })
    } else {
        None
    }
}

/// `query_tree_walker(parsetree, ScanQueryWalker, &acquire,
/// QTW_IGNORE_RC_SUBQUERIES)` — descend expression sublinks, recursing
/// `ScanQueryForLocks` into each `SubLink.subselect`. We collect the sublink
/// subqueries with a closure walker (the value `query_tree_walker`), then
/// recurse — equivalent to C's in-walker recursion (order is irrelevant for
/// lock acquisition).
fn scan_query_sublinks(parsetree: &Query<'_>, acquire: bool) -> PgResult<()> {
    // Collect raw pointers to the SubLink subselects so the immutable borrow of
    // `parsetree` taken by the walker ends before we recurse. Lifetime-erased to
    // `*const ()` because the HRTB walker node lifetime differs from `subs`.
    let mut subs: Vec<*const ()> = Vec::new();
    {
        let mut walker = |node: &Node<'_>| -> bool {
            if let Some(sl) = node.as_sublink() {
                // castNode(Query, sub->subselect).
                if let Some(q) = sl.subselect.as_deref().and_then(|n| n.as_query()) {
                    subs.push(q as *const Query<'_> as *const ());
                }
            }
            // expression_tree_walker recursion is handled by query_tree_walker
            // itself; do NOT recurse into Query nodes (ScanQueryForLocks does).
            false
        };
        backend_nodes_core::node_walker::query_tree_walker(
            parsetree,
            &mut walker,
            backend_nodes_core::node_walker::QTW_IGNORE_RT_SUBQUERIES
                | backend_nodes_core::node_walker::QTW_IGNORE_CTE_SUBQUERIES,
        );
    }
    for sub in subs {
        // SAFETY: `sub` points into `parsetree`'s owned tree, alive for this
        // call; no mutation happens between collection and use.
        let q: &Query<'_> = unsafe { &*(sub as *const Query<'_>) };
        ScanQueryForLocks(q, acquire)?;
    }
    Ok(())
}

/* ==========================================================================
 * PlanCacheComputeResultDesc (static)
 * ======================================================================== */

/// `PlanCacheComputeResultDesc(stmt_list)` — over the owned `Query` list,
/// returning an owned `TupleDescData` allocated in `dest`. `None` == no result
/// tuples. The descriptor is `'static`-marked but lives in `dest`, which the
/// caller MUST keep alive at least as long as it uses the result.
fn PlanCacheComputeResultDesc(
    dest: &MemoryContext,
    stmt_list: &[Query<'_>],
) -> PgResult<Option<TupleDescData<'static>>> {
    use types_portal::PortalStrategy as PStrat;
    let strategy = pquery_seams::choose_portal_strategy_queries::call(stmt_list)?;
    let td: types_tuple::heaptuple::TupleDesc<'_> = match strategy {
        PStrat::PORTAL_ONE_SELECT | PStrat::PORTAL_ONE_MOD_WITH => match stmt_list.first() {
            Some(q) => {
                exectuples_seams::exec_clean_type_from_tl::call(dest.mcx(), q.targetList.as_slice())?
            }
            None => None,
        },
        PStrat::PORTAL_ONE_RETURNING => match query_list_get_primary_stmt(stmt_list) {
            Some(q) => exectuples_seams::exec_clean_type_from_tl::call(
                dest.mcx(),
                q.returningList.as_slice(),
            )?,
            None => None,
        },
        PStrat::PORTAL_UTIL_SELECT => {
            match stmt_list.first().and_then(|q| q.utilityStmt.as_deref()) {
                Some(util) => utility_seams::utility_tuple_descriptor::call(dest.mcx(), util)?,
                None => None,
            }
        }
        PStrat::PORTAL_MULTI_QUERY => None,
    };
    // The descriptor lives in `dest`; clone the boxed payload into a value and
    // extend the borrow to the `'static` marker (sound while `dest` is alive —
    // the caller's contract).
    match td {
        Some(boxed) => {
            let value = boxed.clone_in(dest.mcx())?;
            Ok(Some(unsafe {
                core::mem::transmute::<TupleDescData<'_>, TupleDescData<'static>>(value)
            }))
        }
        None => Ok(None),
    }
}

/* ==========================================================================
 * PlanCacheRelCallback (static)
 * ======================================================================== */

/// `PlanCacheRelCallback(arg, relid)` — relcache inval callback.
fn plan_cache_rel_callback(relid: Oid) {
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
            list_member_oid(&src.borrow().relation_oids, relid)
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
            let pl = get_plan(gplan);
            let mut invalidate = false;
            {
                let p = pl.borrow();
                'stmt_scan: for stmt in p.stmt_list.iter() {
                    if pstmt_is_utility(stmt) {
                        continue;
                    }
                    let empty: &[Oid] = &[];
                    let oids: &[Oid] = stmt.relationOids.as_deref().unwrap_or(empty);
                    let stmt_hit = if relid == INVALID_OID {
                        !oids.is_empty()
                    } else {
                        list_member_oid(oids, relid)
                    };
                    if stmt_hit {
                        invalidate = true;
                        break 'stmt_scan;
                    }
                }
            }
            if invalidate {
                pl.borrow_mut().is_valid = false;
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
            list_member_oid(&ce.borrow().relation_oids, relid)
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
            let pl = get_plan(gplan);
            let mut invalidate = false;
            {
                let p = pl.borrow();
                'stmt_scan: for stmt in p.stmt_list.iter() {
                    if pstmt_is_utility(stmt) {
                        continue;
                    }
                    if let Some(inval) = &stmt.invalItems {
                        for pii in inval.iter() {
                            let key = InvalItemKey {
                                cache_id: pii.cacheId,
                                hash_value: pii.hashValue,
                            };
                            if inval_matches(&key, cacheid, hashvalue) {
                                invalidate = true;
                                break 'stmt_scan;
                            }
                        }
                    }
                }
            }
            if invalidate {
                pl.borrow_mut().is_valid = false;
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
 * ReleaseAllPlanCacheRefsInOwner / ResOwnerReleaseCachedPlan
 * ======================================================================== */

/// `ReleaseAllPlanCacheRefsInOwner(owner)`.
pub fn ReleaseAllPlanCacheRefsInOwner(owner: ResourceOwnerHandle) -> PgResult<()> {
    let plans = resowner_seams::resource_owner_release_all_plan_refs::call(owner)?;
    for plan in plans {
        ResOwnerReleaseCachedPlan(plan)?;
    }
    Ok(())
}

/// `ResOwnerReleaseCachedPlan(res)` — `ReleaseCachedPlan((CachedPlan *) res, NULL)`.
pub fn ResOwnerReleaseCachedPlan(plan: CachedPlanHandle) -> PgResult<()> {
    ReleaseCachedPlan(plan, ResourceOwnerHandle::NULL)
}

/* ==========================================================================
 * INWARD seam bodies — the value-shaped public API the prepare/portal
 * consumers call (backend-utils-cache-plancache-seams).
 * ======================================================================== */

/// `create_cached_plan(mcx, raw_stmt, query_string, command_tag)`.
fn seam_create_cached_plan<'mcx>(
    _mcx: Mcx<'mcx>,
    raw_stmt: &RawStmt<'mcx>,
    query_string: &str,
    command_tag: CommandTag,
) -> PgResult<SeamSourceHandle> {
    Ok(SeamSourceHandle(CreateCachedPlan(
        raw_stmt,
        query_string,
        command_tag,
    )?))
}

/// `complete_cached_plan(mcx, plansource, query_list, arg_types)` — the
/// fixed-cursor / fixed-result PREPARE convenience form
/// (`CompleteCachedPlan(..., CURSOR_OPT_PARALLEL_OK, true)`).
fn seam_complete_cached_plan<'mcx>(
    _mcx: Mcx<'mcx>,
    plansource: SeamSourceHandle,
    query_list: &[Node<'mcx>],
    arg_types: &[Oid],
) -> PgResult<()> {
    // Each node is a Node::Query — project to &Query, copying into a scratch
    // context so `CompleteCachedPlan` gets an owned `&[Query]`.
    let scratch = MemoryContext::new("plancache_complete_in");
    let mut owned: Vec<Query<'_>> = Vec::with_capacity(query_list.len());
    for n in query_list {
        match n.as_query() {
            Some(q) => owned.push(q.clone_in(scratch.mcx())?),
            None => {
                return Err(elog_error(
                    "complete_cached_plan: querytree list element is not a Query",
                ))
            }
        }
    }
    let num_params = arg_types.len() as i32;
    CompleteCachedPlan(
        plansource.0,
        owned.as_slice(),
        arg_types,
        num_params,
        /* has_parser_setup */ false,
        CURSOR_OPT_PARALLEL_OK,
        /* fixed_result */ true,
    )
}

/// `save_cached_plan(plansource)`.
fn seam_save_cached_plan(plansource: SeamSourceHandle) -> PgResult<()> {
    SaveCachedPlan(plansource.0)
}

/// `drop_cached_plan(plansource)`.
fn seam_drop_cached_plan(plansource: SeamSourceHandle) -> PgResult<()> {
    DropCachedPlan(plansource.0)
}

/// `get_cached_plan(plansource, bound_params, owner, query_env)`.
fn seam_get_cached_plan<'mcx>(
    plansource: SeamSourceHandle,
    bound_params: ParamListInfo,
    owner: ResourceOwnerHandle,
    query_env: Option<&QueryEnvironment<'mcx>>,
) -> PgResult<SeamPlanHandle> {
    Ok(SeamPlanHandle(GetCachedPlan(
        plansource.0,
        bound_params,
        owner,
        query_env,
    )?))
}

/// `release_cached_plan(cplan, owner)`.
fn seam_release_cached_plan(cplan: SeamPlanHandle, owner: ResourceOwnerHandle) -> PgResult<()> {
    ReleaseCachedPlan(cplan.0, owner)
}

/// Portal-release seam: `PortalReleaseCachedPlan` calls
/// `ReleaseCachedPlan(portal->cplan, NULL)` (portalmem.c:314). With a NULL
/// owner `ReleaseCachedPlan` only drops the refcount (the sole fallible path —
/// `resource_owner_forget_plan` — is skipped), so this is infallible.
fn seam_portal_release_cached_plan(plan: types_portal::CachedPlanHandle) {
    ReleaseCachedPlan(plan.0, ResourceOwnerHandle::NULL)
        .expect("ReleaseCachedPlan(plan, NULL) cannot fail");
}

/// `cached_plan_get_target_list(mcx, plansource)` — owned `Node` list.
fn seam_cached_plan_get_target_list<'mcx>(
    mcx: Mcx<'mcx>,
    plansource: SeamSourceHandle,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let tl = CachedPlanGetTargetList(mcx, plansource.0, None)?;
    let mut out = mcx::vec_with_capacity_in(mcx, tl.len())?;
    for te in tl.into_iter() {
        out.push(Node::mk_target_entry(mcx, te));
    }
    Ok(out)
}

/// `plansource_fixed_result(plansource)`.
fn seam_plansource_fixed_result(plansource: SeamSourceHandle) -> PgResult<bool> {
    Ok(get_source(plansource.0).borrow().fixed_result)
}

/// `plansource_num_params(plansource)`.
fn seam_plansource_num_params(plansource: SeamSourceHandle) -> PgResult<i32> {
    Ok(get_source(plansource.0).borrow().num_params)
}

/// `plansource_param_types(mcx, plansource)`.
fn seam_plansource_param_types<'mcx>(
    mcx: Mcx<'mcx>,
    plansource: SeamSourceHandle,
) -> PgResult<PgVec<'mcx, Oid>> {
    let src = get_source(plansource.0);
    let p = src.borrow();
    let mut out = mcx::vec_with_capacity_in(mcx, p.param_types.len())?;
    for &o in p.param_types.iter() {
        out.push(o);
    }
    Ok(out)
}

/// `plansource_query_string(mcx, plansource)`.
fn seam_plansource_query_string<'mcx>(
    mcx: Mcx<'mcx>,
    plansource: SeamSourceHandle,
) -> PgResult<mcx::PgString<'mcx>> {
    let src = get_source(plansource.0);
    let p = src.borrow();
    mcx::PgString::from_str_in(p.query_string.as_str(), mcx)
}

/// `plansource_command_tag(plansource)`.
fn seam_plansource_command_tag(plansource: SeamSourceHandle) -> PgResult<CommandTag> {
    Ok(get_source(plansource.0).borrow().command_tag)
}

/// `plansource_result_desc(mcx, plansource)`.
fn seam_plansource_result_desc<'mcx>(
    mcx: Mcx<'mcx>,
    plansource: SeamSourceHandle,
) -> PgResult<Option<TupleDescData<'mcx>>> {
    let src = get_source(plansource.0);
    let p = src.borrow();
    match &p.result_desc {
        Some(td) => Ok(Some(td.clone_in(mcx)?)),
        None => Ok(None),
    }
}

/// `plansource_has_result_desc(plansource)` — `resultDesc != NULL`, no copy.
fn seam_plansource_has_result_desc(plansource: SeamSourceHandle) -> PgResult<bool> {
    Ok(get_source(plansource.0).borrow().result_desc.is_some())
}

/// `plansource_num_generic_plans(plansource)`.
fn seam_plansource_num_generic_plans(plansource: SeamSourceHandle) -> PgResult<i64> {
    Ok(get_source(plansource.0).borrow().num_generic_plans)
}

/// `plansource_num_custom_plans(plansource)`.
fn seam_plansource_num_custom_plans(plansource: SeamSourceHandle) -> PgResult<i64> {
    Ok(get_source(plansource.0).borrow().num_custom_plans)
}

/// `cached_plan_stmt_list(mcx, cplan)` — owned `PlannedStmt`s in `mcx`.
fn seam_cached_plan_stmt_list<'mcx>(
    mcx: Mcx<'mcx>,
    cplan: SeamPlanHandle,
) -> PgResult<PgVec<'mcx, PlannedStmt<'mcx>>> {
    let pl = get_plan(cplan.0);
    let p = pl.borrow();
    debug_assert_eq!(p.magic, CACHEDPLAN_MAGIC);
    let mut out = mcx::vec_with_capacity_in(mcx, p.stmt_list.len())?;
    for s in p.stmt_list.iter() {
        out.push(s.clone_in(mcx)?);
    }
    Ok(out)
}

/* ==========================================================================
 * Seam installation.
 * ======================================================================== */

/// Install plancache's INWARD value seams plus `init_plan_cache` / the
/// `plan_cache_mode` GUC reader.
pub fn init_seams() {
    inward::init_plan_cache::set(InitPlanCache);

    inward::create_cached_plan::set(seam_create_cached_plan);
    inward::complete_cached_plan::set(seam_complete_cached_plan);
    inward::save_cached_plan::set(seam_save_cached_plan);
    inward::drop_cached_plan::set(seam_drop_cached_plan);
    inward::get_cached_plan::set(seam_get_cached_plan);
    inward::release_cached_plan::set(seam_release_cached_plan);
    portal_inward::release_cached_plan::set(seam_portal_release_cached_plan);
    inward::cached_plan_get_target_list::set(seam_cached_plan_get_target_list);
    inward::plansource_fixed_result::set(seam_plansource_fixed_result);
    inward::plansource_num_params::set(seam_plansource_num_params);
    inward::plansource_param_types::set(seam_plansource_param_types);
    inward::plansource_query_string::set(seam_plansource_query_string);
    inward::plansource_command_tag::set(seam_plansource_command_tag);
    inward::plansource_result_desc::set(seam_plansource_result_desc);
    inward::plansource_has_result_desc::set(seam_plansource_has_result_desc);
    inward::plansource_num_generic_plans::set(seam_plansource_num_generic_plans);
    inward::plansource_num_custom_plans::set(seam_plansource_num_custom_plans);
    inward::cached_plan_stmt_list::set(seam_cached_plan_stmt_list);

    // `plan_cache_mode` (plancache.c:138 `int plan_cache_mode`) — USERSET enum
    // GUC owned by this unit. Install the guc-table slot accessors over the
    // backing cell so the GUC engine can read/write it, then read the slot.
    backend_utils_misc_guc_tables::vars::plan_cache_mode.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: || PLAN_CACHE_MODE.with(core::cell::Cell::get),
            set: |v| PLAN_CACHE_MODE.with(|c| c.set(v)),
        },
    );
    backend_seams::plan_cache_mode::set(|| {
        Ok(backend_utils_misc_guc_tables::vars::plan_cache_mode.read())
    });
}

std::thread_local! {
    /// `int plan_cache_mode = PLAN_CACHE_MODE_AUTO` (plancache.c:138) — backing
    /// store for the guc-table slot; PGC_USERSET, boot value 0 (AUTO).
    static PLAN_CACHE_MODE: core::cell::Cell<i32> = const { core::cell::Cell::new(0) };
}
