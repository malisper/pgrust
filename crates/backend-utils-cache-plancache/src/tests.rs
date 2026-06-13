//! Unit tests for the plancache port.
//!
//! These install minimal mocks for the cross-subsystem seams (pure
//! bookkeeping stand-ins for the memory-context / node / planner / namespace
//! surfaces) and exercise the in-crate algorithm: the create/complete
//! lifecycle, save + invalidation, reset, and the one-shot/saved error paths.

use std::cell::RefCell;
use std::sync::Once;

use types_plancache::{
    CtxId, PortalStrategy, QueryListHandle, RawStmtHandle, SearchPathMatcherHandle,
    SysCacheId, TupleDescHandle,
};

use super::*;

use backend_nodes_copyfuncs_pc_seams::QueryDependencies;

struct MockState {
    next_ctx: u64,
    user_id: Oid,
    requires_reval: bool,
    search_path_matches: bool,
}

thread_local! {
    static MOCK: RefCell<MockState> = RefCell::new(MockState {
        next_ctx: 100,
        user_id: 10,
        requires_reval: true,
        search_path_matches: true,
    });
}

fn mock<R>(f: impl FnOnce(&mut MockState) -> R) -> R {
    MOCK.with(|m| f(&mut m.borrow_mut()))
}

fn next_ctx() -> CtxId {
    CtxId(mock(|m| {
        m.next_ctx += 1;
        m.next_ctx
    }))
}

static INSTALL: Once = Once::new();

fn install_seams() {
    INSTALL.call_once(|| {
        mcxt_seams::current_memory_context::set(|| Ok(CtxId(1)));
        mcxt_seams::cache_memory_context::set(|| Ok(CtxId(2)));
        mcxt_seams::alloc_set_context_create_small::set(|_p, _n| Ok(next_ctx()));
        mcxt_seams::memory_context_switch_to::set(|_c| Ok(CtxId(1)));
        mcxt_seams::memory_context_set_parent::set(|_c, _p| Ok(()));
        mcxt_seams::memory_context_get_parent::set(|_c| Ok(CtxId(1)));
        mcxt_seams::memory_context_delete::set(|_c| Ok(()));
        mcxt_seams::memory_context_set_identifier::set(|_c, _i| Ok(()));
        mcxt_seams::memory_context_copy_and_set_identifier::set(|_c, _i| Ok(()));

        node_seams::copy_raw_stmt::set(|r| Ok(r));
        node_seams::copy_analyzed_query::set(|q| Ok(q));
        node_seams::copy_query_list::set(|l| Ok(l));
        node_seams::extract_query_dependencies::set(|_l| Ok(QueryDependencies::default()));

        analyze_seams::stmt_requires_parse_analysis::set(|_r| {
            Ok(mock(|m| m.requires_reval))
        });
        analyze_seams::query_requires_rewrite_plan::set(|_q| Ok(mock(|m| m.requires_reval)));

        backend_seams::get_user_id::set(|| Ok(mock(|m| m.user_id)));
        backend_seams::row_security::set(|| Ok(false));

        namespace_seams::get_search_path_matcher::set(|_c| Ok(SearchPathMatcherHandle(7)));
        namespace_seams::search_path_matches_current_environment::set(|_m| {
            Ok(mock(|m| m.search_path_matches))
        });

        pquery_seams::choose_portal_strategy::set(|_l| Ok(PortalStrategy::MultiQuery));

        inval_seams::register_relcache_callback::set(|_f| Ok(()));
        inval_seams::register_syscache_callback::set(|_c, _f| Ok(()));
        syscache_seams::syscache_id::set(|which| {
            Ok(match which {
                SysCacheId::ProcOid => 1,
                SysCacheId::TypeOid => 2,
                SysCacheId::NamespaceOid => 3,
                SysCacheId::OperOid => 4,
                SysCacheId::AmOpOpId => 5,
                SysCacheId::ForeignServerOid => 6,
                SysCacheId::ForeignDataWrapperOid => 7,
            })
        });
    });
}

#[test]
fn init_plan_cache_registers_callbacks() {
    install_seams();
    InitPlanCache().unwrap();
}

#[test]
fn create_complete_save_drop_lifecycle() {
    install_seams();
    // A query that "requires revalidation" so dependency extraction runs.
    mock(|m| m.requires_reval = true);

    let src = CreateCachedPlan(RawStmtHandle(1), "SELECT 1", CommandTag(0)).unwrap();
    assert!(!CachedPlanIsValid(src).unwrap());

    CompleteCachedPlan(
        src,
        QueryListHandle(11),
        None,
        &[],
        0,
        ParserSetupHandle::NONE,
        0,
        false,
    )
    .unwrap();
    assert!(CachedPlanIsValid(src).unwrap());

    SaveCachedPlan(src).unwrap();
    // Saved sources are checked by ResetPlanCache.
    ResetPlanCache().unwrap();
    assert!(!CachedPlanIsValid(src).unwrap());

    DropCachedPlan(src).unwrap();
}

#[test]
fn oneshot_cannot_be_saved_or_copied() {
    install_seams();
    let src = CreateOneShotCachedPlan(RawStmtHandle(2), "SELECT 2", CommandTag(0)).unwrap();
    CompleteCachedPlan(
        src,
        QueryListHandle(0),
        None,
        &[],
        0,
        ParserSetupHandle::NONE,
        0,
        false,
    )
    .unwrap();
    assert!(SaveCachedPlan(src).is_err());
    assert!(CopyCachedPlan(src).is_err());
    DropCachedPlan(src).unwrap();
}

#[test]
fn empty_query_needs_no_revalidation() {
    install_seams();
    // raw NULL, analyzed NULL => StmtPlanRequiresRevalidation == false.
    let src = CreateCachedPlan(RawStmtHandle::NULL, "", CommandTag(0)).unwrap();
    CompleteCachedPlan(
        src,
        QueryListHandle(0),
        None,
        &[],
        0,
        ParserSetupHandle::NONE,
        0,
        false,
    )
    .unwrap();
    assert!(CachedPlanIsValid(src).unwrap());
    // ResetPlanCache leaves an empty-query (no-revalidation) source valid.
    SaveCachedPlan(src).unwrap();
    ResetPlanCache().unwrap();
    assert!(CachedPlanIsValid(src).unwrap());
    DropCachedPlan(src).unwrap();
}

#[test]
fn result_desc_present_helper() {
    install_seams();
    let src = CreateCachedPlan(RawStmtHandle::NULL, "", CommandTag(0)).unwrap();
    CompleteCachedPlan(
        src,
        QueryListHandle(0),
        None,
        &[],
        0,
        ParserSetupHandle::NONE,
        0,
        false,
    )
    .unwrap();
    // MultiQuery strategy => NULL result desc.
    let _ = TupleDescHandle::NULL;
    DropCachedPlan(src).unwrap();
}
