use super::*;

fn seed_cache(roleid: Oid, is_super: bool) {
    LAST_ROLEID.set(roleid);
    LAST_ROLEID_IS_SUPER.set(is_super);
}

#[test]
fn bootstrap_superuser_escape_hatch_without_postmaster() {
    assert!(!init_small::globals::IsUnderPostmaster());
    assert!(superuser_arg(BOOTSTRAP_SUPERUSERID).unwrap());
    // C returns before touching the one-entry cache on this path.
    assert_eq!(LAST_ROLEID.get(), InvalidOid);
}

#[test]
fn one_entry_cache_hit_skips_lookup_and_callback_invalidates() {
    seed_cache(42, true);
    assert!(superuser_arg(42).unwrap());
    seed_cache(42, false);
    assert!(!superuser_arg(42).unwrap());

    RoleidCallback(Datum::null(), AUTHOID, 0);
    assert_eq!(LAST_ROLEID.get(), InvalidOid);
    // Post-invalidation, a non-bootstrap roleid must go back to pg_authid;
    // with no booted catcache that is a loud stop, never a stale answer.
    let probe = std::panic::catch_unwind(|| superuser_arg(42));
    assert!(probe.is_err() || probe.unwrap().is_err());
}

#[test]
fn install_seams() {
    init_seams();
    assert!(superuser_seams::superuser::is_installed());
    assert!(superuser_seams::superuser_arg::is_installed());
    seed_cache(77, true);
    assert!(superuser_seams::superuser_arg::call(77).unwrap());
}
