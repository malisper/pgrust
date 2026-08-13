//! D3.4 idle passivation (docs/design/connection-scaling.md).
//!
//! After `idle_passivate_timeout` seconds idle outside a transaction, a
//! backend drops its L1 caches and returns retained allocator memory; the
//! first query after passivation rebuilds through the shared L2 (Arc adopt,
//! µs per entry) instead of catalog scans.
//!
//! # Trigger mechanics
//!
//! The backend's idle block is the FeBeWaitSet wait inside `secure_read`
//! (be_secure), entered from `ReadCommand` with an infinite timeout. Rather
//! than threading a timeout parameter down that stack, passivation rides the
//! existing timeout engine exactly like IDLE_STATS_UPDATE_TIMEOUT: the
//! ready-for-query path arms a one-shot IDLE_PASSIVATE_TIMEOUT when the
//! session goes truly idle; the shared timer thread sets MyLatch; the wait
//! loop wakes on WL_LATCH_SET and runs ProcessClientReadInterrupt →
//! ProcessInterrupts, whose arm calls [`IdlePassivate`] once (the timer is
//! one-shot and only re-armed by the next ready-for-query) and the backend
//! goes right back to waiting.
//!
//! # What passivation drops (and what survives)
//!
//! - plancache: generic plan bodies of saved sources; the sources themselves
//!   (prepared-statement identity, query text, parse trees) survive and
//!   replan on next execution — C's behavior after any plancache inval.
//! - catcache: full ResetCatalogCaches — every unpinned tuple and CatCList
//!   freed (pinned ones survive marked dead; at idle-not-in-txn there should
//!   be none).
//! - relcache: every unpinned, un-nailed, no-subxact-state entry plus the
//!   derived side caches (rules/RLS/indexattr/statext/fkey/deform-JIT) and
//!   the L2 mirror registry. Nailed entries survive (their bulk aliases L2
//!   cores anyway since the init-file routing).
//! - typcache and the other small per-session caches: kept (census: <8KB
//!   combined; their entries are woven into fn_extra/plan state and are not
//!   safely droppable from here).
//! - mcx: per-thread retained pools (parked aset keeper blocks, Acct nodes,
//!   children-vecs) freed, then the allocator release hook (mi_collect)
//!   returns freed-but-retained segments. In-context aset freelist retention
//!   (the CacheMemoryContext high-water) is NOT released — that would need
//!   per-chunk block back-pointers on the hot dealloc path (see
//!   mcx::passivate_trim).
//! - thread stack madvise: deliberately SKIPPED for the PoC — releasing
//!   dirtied stack pages under a live thread is platform-specific
//!   (MADV_FREE vs MADV_DONTNEED semantics differ between macOS and Linux,
//!   and both race the guard-page/red-zone layout Rust's runtime assumes);
//!   the win is real (~stack high-water per idle conn) but belongs to a
//!   dedicated, carefully-gated pass.

use types_error::PgResult;

/// Effective passivation delay in seconds; 0 = off. GUC
/// `idle_passivate_timeout` (PGC_SIGHUP), with a PGRUST_IDLE_PASSIVATE_SECS
/// env override for harnesses (PoC knob precedent: PGRUST_RELCACHE_CAP).
pub(crate) fn idle_passivate_secs() -> i32 {
    static ENV: std::sync::OnceLock<Option<i32>> = std::sync::OnceLock::new();
    if let Some(v) = *ENV.get_or_init(|| {
        std::env::var("PGRUST_IDLE_PASSIVATE_SECS").ok().and_then(|v| v.trim().parse().ok())
    }) {
        return v;
    }
    guc_tables::backing::idle_passivate_timeout()
}

/// Passivate this backend: see the module doc. Caller (the ProcessInterrupts
/// arm) guarantees DoingCommandRead && !IsTransactionOrTransactionBlock.
#[cold]
#[inline(never)]
pub(crate) fn IdlePassivate() -> PgResult<()> {
    debug_assert!(!xact::IsTransactionOrTransactionBlock());
    let plans = plancache::ReleaseIdleGenericPlans();
    catcache::ResetCatalogCaches()?;
    let rels = relcache::PassivateRelationCache()?;
    // Wave-4 arena diet: the operator-lookup memo rebuilds from catcache on
    // next use (same rebuild-through-L2 contract as the caches above).
    parse_oper::PassivateOprCache();
    let trimmed = mcx::passivate_trim();
    // Wave-4 stack discipline: after every cache drop (the drops themselves
    // dip the stack), release the dead dirty stack pages below the current
    // frame. Safety argument + platform gating: stack_mem module doc.
    let stack = crate::stack_mem::release_idle_stack();
    elog::elog(
        types_error::DEBUG1,
        format!(
            "idle passivation: dropped {rels} relcache entries, {plans} generic plans, \
             reset catcache, allocator trim{}, stack release {stack} bytes",
            if trimmed { "" } else { " (no release hook)" }
        ),
    )?;
    Ok(())
}
