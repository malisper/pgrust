use super::*;

#[test]
fn init_plan_cache_registers_and_callbacks_fire_vacuously() {
    InitPlanCache().unwrap();
    for cacheid in [
        PROCOID,
        TYPEOID,
        NAMESPACEOID,
        OPEROID,
        AMOPOPID,
        FOREIGNSERVEROID,
        FOREIGNDATAWRAPPEROID,
    ] {
        inval::invalidate::CallSyscacheCallbacks(cacheid, 0).unwrap();
        inval::invalidate::CallSyscacheCallbacks(cacheid, 0xDEAD_BEEF).unwrap();
    }
    PlanCacheRelCallback(Datum::from_oid(InvalidOid), InvalidOid);
    PlanCacheRelCallback(Datum::from_oid(InvalidOid), 1259);
    ResetPlanCache();
}

#[test]
fn seams_route_to_this_crate() {
    init_seams();
    plancache_portal_seams::init_plan_cache::call().unwrap();
    assert!(plancache_portal_seams::release_cached_plan::is_installed());
}

#[test]
#[should_panic(expected = "CreateCachedPlan/GetCachedPlan are deferred")]
fn release_cached_plan_refuses_live_handle() {
    ReleaseCachedPlan(CachedPlanHandle(7));
}

#[test]
#[should_panic(expected = "GetCachedPlan (plancache.c) deferred")]
fn get_cached_plan_is_deferred() {
    GetCachedPlan();
}

#[test]
#[should_panic(expected = "CreateCachedPlan (plancache.c) deferred")]
fn create_cached_plan_is_deferred() {
    CreateCachedPlan();
}

// syscache_ids.h parity for the ids InitPlanCache registers.
#[test]
fn cache_ids_match_c_headers() {
    assert_eq!(PROCOID, 47);
    assert_eq!(TYPEOID, 82);
    assert_eq!(NAMESPACEOID, 38);
    assert_eq!(OPEROID, 40);
    assert_eq!(AMOPOPID, 3);
    assert_eq!(FOREIGNSERVEROID, 32);
    assert_eq!(FOREIGNDATAWRAPPEROID, 30);
}
