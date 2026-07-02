// plancache.c, M1 surface: InitPlanCache's inval wiring plus the callback set
// it registers. saved_plan_list/cached_expression_list are provably empty —
// their only writers (CreateCachedPlan/GetCachedExpression) defer loud — so
// each callback IS C's vacuous empty-dlist walk, not a stub. The callbacks
// hold no borrows and touch no TLS state (inval re-enters them mid-
// invalidation; fabled's ResetPlanCache double-borrow incident).
#![allow(non_snake_case)]

use cache_syscache::cacheinfo::{
    AMOPOPID, FOREIGNDATAWRAPPEROID, FOREIGNSERVEROID, NAMESPACEOID, OPEROID, PROCOID, TYPEOID,
};
use datum::Datum;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_portal::CachedPlanHandle;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    plancache_portal_seams::init_plan_cache::set(InitPlanCache);
    plancache_portal_seams::release_cached_plan::set(ReleaseCachedPlan);
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
    inval::invalidate::CacheRegisterSyscacheCallback(FOREIGNDATAWRAPPEROID, PlanCacheSysCallback, zero)?;
    Ok(())
}

fn PlanCacheRelCallback(_arg: Datum, _relid: Oid) {}

fn PlanCacheObjectCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {}

fn PlanCacheSysCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    ResetPlanCache();
}

pub fn ResetPlanCache() {}

// ReleaseCachedPlan(plan, NULL): a live CachedPlan cannot exist while its
// builders defer, so any handle reaching here is corruption — refuse loudly.
pub fn ReleaseCachedPlan(cplan: CachedPlanHandle) {
    panic!(
        "ReleaseCachedPlan (plancache.c): live CachedPlan handle {} but \
         CreateCachedPlan/GetCachedPlan are deferred (plancache unported)",
        cplan.0
    );
}

pub fn CreateCachedPlan() -> ! {
    panic!("CreateCachedPlan (plancache.c) deferred: prepared statements/plan caching unported");
}

pub fn CompleteCachedPlan() -> ! {
    panic!("CompleteCachedPlan (plancache.c) deferred: prepared statements/plan caching unported");
}

pub fn SaveCachedPlan() -> ! {
    panic!("SaveCachedPlan (plancache.c) deferred: prepared statements/plan caching unported");
}

pub fn DropCachedPlan() -> ! {
    panic!("DropCachedPlan (plancache.c) deferred: prepared statements/plan caching unported");
}

pub fn GetCachedPlan() -> ! {
    panic!("GetCachedPlan (plancache.c) deferred: prepared statements/plan caching unported");
}

pub fn GetCachedExpression() -> ! {
    panic!("GetCachedExpression (plancache.c) deferred: cached expressions unported");
}
