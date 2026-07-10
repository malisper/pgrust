use std::collections::HashSet;
use std::sync::Once;

use types_core::{InvalidOid, BOOTSTRAP_SUPERUSERID};
use types_error::{PgError, ERRCODE_QUERY_CANCELED, ERROR};
use types_guc::{GucContext::PGC_USERSET, GucSource::PGC_S_SESSION};

use super::*;

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "on" | "yes" | "1" => Some(true),
        "false" | "off" | "no" | "0" => Some(false),
        _ => None,
    }
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        guc::init_seams();
        xact_seams::is_in_parallel_mode::set(|| false);
        scalar_seams::parse_bool::set(parse_bool);
        aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
        mbutils_seams::get_database_encoding::set(|| 6);
        timestamp_seams::get_current_timestamp::set(|| 42);
    });
    guc::store::initialize_guc_options().unwrap();
    init_small::globals::SetMyDatabaseId(42);
    init_small::globals::SetMyDatabaseTableSpace(1663);
    init_small::globals::SetInterruptPending(false);
    init_small::globals::SetQueryCancelPending(false);
    init_small::globals::SetProcDiePending(false);
    init_small::globals::SetInterruptHoldoffCount(0);
    init_small::globals::SetQueryCancelHoldoffCount(0);
    init_small::globals::SetCritSectionCount(0);
    set_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
}

fn identity(user: Oid) -> miscinit::SessionIdentityState {
    miscinit::SessionIdentityState {
        authenticated_user_id: user,
        session_user_id: user,
        outer_user_id: user,
        current_user_id: user,
        system_user: Some("trust:test"),
        session_user_is_superuser: user == BOOTSTRAP_SUPERUSERID,
        security_restriction_context: if user == 23 {
            types_core::SECURITY_NOFORCE_RLS
        } else {
            0
        },
        set_role_is_active: false,
    }
}

fn set_state(user: Oid, work_mem: i32, temp: (Oid, Oid)) {
    miscinit::ReplaceSessionIdentityState(identity(user));
    catalog_namespace::ReplaceTempNamespaceState(temp.0, temp.1);
    guc::ResetAllOptions();
    guc::SetConfigOption(
        "work_mem",
        Some(&work_mem.to_string()),
        PGC_USERSET,
        PGC_S_SESSION,
    )
    .unwrap();
    miscinit::ReplaceSessionIdentityState(identity(user));
}

fn install_context(context: &SessionContext) {
    miscinit::ReplaceSessionIdentityState(context.identity);
    catalog_namespace::ReplaceTempNamespaceState(
        context.temp_namespace.0,
        context.temp_namespace.1,
    );
    guc::ResetAllOptions();
    guc::store::apply_captured_session_gucs(&context.gucs).unwrap();
    miscinit::ReplaceSessionIdentityState(context.identity);
}

fn assert_state(user: Oid, work_mem: i32, temp: (Oid, Oid)) {
    assert_eq!(miscinit::CaptureSessionIdentityState(), identity(user));
    assert_eq!(init_small::globals::work_mem(), work_mem);
    assert_eq!(catalog_namespace::GetTempNamespaceState(), temp);
}

fn contexts() -> (SessionContext, SessionContext, SessionContext) {
    set_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
    let base = SessionContext::capture();
    set_state(22, 8192, (2200, 2201));
    let a = SessionContext::capture();
    set_state(23, 16384, (2300, 2301));
    let b = SessionContext::capture();
    install_context(&base);
    (base, a, b)
}

#[test]
fn manifest_is_unique_and_phase0_actions_are_explicit() {
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    for member in SESSION_ENVELOPE_MANIFEST {
        assert!(
            ids.insert(member.id),
            "duplicate manifest id: {:?}",
            member.id
        );
        assert!(
            names.insert(member.name),
            "duplicate manifest name: {}",
            member.name
        );
        if member.phase0 == Phase0Action::Refuse {
            assert!(
                member.blocker.is_some(),
                "refusal without blocker: {}",
                member.name
            );
        }
    }
    assert_eq!(ids.len(), 21);
    assert!(ids.contains(&EnvelopeMemberId::GucStore));
    assert!(ids.contains(&EnvelopeMemberId::SnapshotState));
    assert!(ids.contains(&EnvelopeMemberId::ResourceOwners));
    assert!(ids.contains(&EnvelopeMemberId::ErrorStack));
}

#[test]
fn nested_bind_restores_roots_and_scalars_in_lifo_order() {
    std::thread::spawn(|| {
        setup();
        let (_base, a, b) = contexts();
        let mut drains = 0;

        let outer = bind_session_envelope_with(&a, || {
            drains += 1;
            Ok(())
        })
        .unwrap();
        assert_state(22, 8192, (2200, 2201));

        let inner = bind_session_envelope_with(&b, || {
            drains += 1;
            Ok(())
        })
        .unwrap();
        assert_state(23, 16384, (2300, 2301));
        drop(inner);
        assert_state(22, 8192, (2200, 2201));
        drop(outer);
        assert_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
        assert_eq!(drains, 2);
    })
    .join()
    .unwrap();
}

#[test]
fn panic_and_cancel_paths_restore_without_clearing_cancel() {
    std::thread::spawn(|| {
        setup();
        let (_base, a, _) = contexts();

        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _binding = bind_session_envelope_with(&a, || Ok(())).unwrap();
            assert_state(22, 8192, (2200, 2201));
            panic!("task panic");
        }));
        assert!(panic_result.is_err());
        assert_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));

        let binding = bind_session_envelope_with(&a, || Ok(())).unwrap();
        init_small::globals::SetQueryCancelPending(true);
        let cancelled: PgResult<()> = Err(PgError::new(ERROR, "cancelled")
            .with_sqlstate(ERRCODE_QUERY_CANCELED)
            .into());
        binding.finish().unwrap();
        assert!(cancelled.is_err());
        assert_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
        assert!(init_small::globals::QueryCancelPending());
        init_small::globals::SetQueryCancelPending(false);
    })
    .join()
    .unwrap();
}

#[test]
fn cross_database_and_unimplemented_transaction_state_are_refused_before_drain() {
    std::thread::spawn(|| {
        setup();
        let (_base, mut target, _) = contexts();
        let mut drains = 0;
        target.database_id = 43;
        let error = bind_session_envelope_with(&target, || {
            drains += 1;
            Ok(())
        })
        .err()
        .expect("cross-database bind must fail");
        assert!(error.message().contains("cross-database"));
        assert_eq!(drains, 0);
        assert_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));

        target.database_id = 42;
        target.xact_nest_level = 1;
        target.transaction_active = true;
        let error = bind_session_envelope_with(&target, || {
            drains += 1;
            Ok(())
        })
        .err()
        .expect("transaction-bearing bind must fail");
        assert!(error.message().contains("transaction/snapshot root"));
        assert_eq!(drains, 0);
    })
    .join()
    .unwrap();
}

#[test]
fn dirty_error_resource_holdoff_and_pending_cancel_boundaries_refuse() {
    std::thread::spawn(|| {
        setup();
        let (_base, target, _) = contexts();

        let callback = elog::push_emit_context_callback(Box::new(|_| {}));
        let error = bind_session_envelope_with(&target, || Ok(()))
            .err()
            .expect("dirty error state");
        assert!(error.message().contains("error or callback"));
        elog::pop_emit_context_callback(callback);

        let owner = resowner::ResourceOwnerCreate(
            types_resowner::ResourceOwner::NULL,
            "session envelope dirty-boundary test",
        )
        .unwrap();
        let error = bind_session_envelope_with(&target, || Ok(()))
            .err()
            .expect("dirty resource state");
        assert!(error.message().contains("resource-owner"));
        resowner::ResourceOwnerDelete(owner);

        init_small::globals::SetCritSectionCount(1);
        let error = bind_session_envelope_with(&target, || Ok(()))
            .err()
            .expect("dirty holdoff state");
        assert!(error.message().contains("holdoff"));
        init_small::globals::SetCritSectionCount(0);

        init_small::globals::SetQueryCancelPending(true);
        let error = bind_session_envelope_with(&target, || Ok(()))
            .err()
            .expect("pending cancellation");
        assert!(error.message().contains("cancellation is pending"));
        init_small::globals::SetQueryCancelPending(false);
    })
    .join()
    .unwrap();
}
