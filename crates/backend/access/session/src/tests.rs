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
        init_small::init_seams();
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
    guc::store::replace_exact_guc_state(&context.gucs);
    catalog_namespace::ReplaceSessionNamespaceState(&context.namespace);
    miscinit::ReplaceSessionIdentityState(context.identity);
    CURRENT_SESSION.set(context.session_exists);
}

fn assert_state(user: Oid, work_mem: i32, temp: (Oid, Oid)) {
    assert_eq!(miscinit::CaptureSessionIdentityState(), identity(user));
    assert_eq!(init_small::globals::work_mem(), work_mem);
    assert_eq!(catalog_namespace::GetTempNamespaceState(), temp);
}

fn contexts() -> (SessionContext, SessionContext, SessionContext) {
    set_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
    let base = SessionContext::capture();
    InitializeSession().unwrap();
    set_state(22, 8192, (2200, 2201));
    let a = SessionContext::capture();
    set_state(23, 16384, (2300, 2301));
    let b = SessionContext::capture();
    install_context(&base);
    (base, a, b)
}

#[test]
fn manifest_is_unique_exhaustive_and_phase0_actions_are_explicit() {
    let expected: HashSet<_> = [
        EnvelopeMemberId::DatabaseIdentity,
        EnvelopeMemberId::DatabasePaths,
        EnvelopeMemberId::ProcessIdentity,
        EnvelopeMemberId::SessionLifecycle,
        EnvelopeMemberId::UserIdentity,
        EnvelopeMemberId::TempNamespace,
        EnvelopeMemberId::SearchPath,
        EnvelopeMemberId::SnapshotState,
        EnvelopeMemberId::TransactionState,
        EnvelopeMemberId::GucStore,
        EnvelopeMemberId::GucFlatBackings,
        EnvelopeMemberId::GucNesting,
        EnvelopeMemberId::ResourceOwnerCells,
        EnvelopeMemberId::ResourceOwnerArena,
        EnvelopeMemberId::ErrorStack,
        EnvelopeMemberId::ErrorCallbacks,
        EnvelopeMemberId::InterruptPending,
        EnvelopeMemberId::InterruptHoldoffs,
        EnvelopeMemberId::Catcache,
        EnvelopeMemberId::Relcache,
        EnvelopeMemberId::Typcache,
        EnvelopeMemberId::Plancache,
        EnvelopeMemberId::InvalidationCallbacks,
        EnvelopeMemberId::InvalidationMessages,
        EnvelopeMemberId::PendingInvalidations,
        EnvelopeMemberId::SyscacheArrays,
        EnvelopeMemberId::Relmapper,
        EnvelopeMemberId::Partcache,
        EnvelopeMemberId::TsCache,
        EnvelopeMemberId::EventCache,
    ]
    .into_iter()
    .collect();
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
        assert!(
            !member.declaration.is_empty(),
            "unlocated TLS member: {}",
            member.name
        );
        match member.phase0 {
            Phase0Action::CaptureApply => assert_eq!(member.kind, EnvelopeBindKind::SwapRoot),
            Phase0Action::RestoreScalar | Phase0Action::RequireSameDatabase => {
                assert_eq!(member.kind, EnvelopeBindKind::ScalarRestore)
            }
            Phase0Action::Drain => assert_eq!(member.kind, EnvelopeBindKind::DrainSameDatabase),
            Phase0Action::CheckEmpty => assert_eq!(member.kind, EnvelopeBindKind::MustBeEmpty),
            Phase0Action::Refuse => assert!(
                member.blocker.is_some(),
                "refusal without blocker: {}",
                member.name
            ),
        }
    }
    assert_eq!(ids, expected);
}

#[test]
fn tls_source_census_and_session_surface_are_pinned() {
    fn count_tree(path: &std::path::Path) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .map(|path| {
                if path.is_dir() {
                    count_tree(&path)
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    std::fs::read_to_string(path)
                        .unwrap()
                        .lines()
                        .filter(|line| {
                            let line = line.trim_start();
                            line.starts_with("thread_local!")
                                || line.starts_with("std::thread_local!")
                                || line.starts_with("::std::thread_local!")
                        })
                        .count()
                } else {
                    0
                }
            })
            .sum()
    }

    let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .unwrap();
    // Baseline 463, re-pinned at the m0-harvest onto lane-executor-v2
    // (fc6ded2c7): the donor lineage's 451/453/455 pins counted the
    // cb-compose-v9.3 tree; this tree carries lane-executor-v2's own
    // non-session TLS population. The two binder-era sources the donor
    // classified stay classified here and are deliberately NOT
    // SESSION_ENVELOPE_MANIFEST members:
    //   1. parallel/src/query_task_guard.rs QUERY_TASK_FAULT — a
    //      #[cfg(debug_assertions)] fault-injection selector for the query-task
    //      binder fault matrix. It is compiled out of release builds, so it can
    //      never affect production byte-identity; it is one-shot per fire and
    //      carries no session state. It is thread-local (not the former global
    //      Mutex) so each helper thread injects independently.
    //   2. parallel/tests/substrate_e2e.rs TEST_RECORD_REGISTRY — pure test
    //      harness state, not product session state.
    // 464, re-pinned at m0-integration (the M0 lane merge): lane C's Waiter
    // adds the ONE new source this tree carries —
    //   3. storage/ipc/waiter/src/lib.rs CURRENT (global::WaiterGuard) — the
    //      per-THREAD parking slot of the structured wait primitive
    //      (parallelism-redesign §2.6). Deliberately non-session TLS: the
    //      slot belongs to the OS thread for its lifetime (poison-on-owner-
    //      death frees it at thread exit) and carries no session or task
    //      state; task-identity hygiene is the waker-token reissue at the
    //      wretain warm-claim boundary (reissue_current_token via the rekey
    //      seam), NOT envelope capture/restore — an envelope must never
    //      touch another task's parking slot, and a parked thread's slot
    //      routes wakes correctly across session rebinds by construction
    //      (handles go stale by token, not by TLS swap).
    // 465, re-pinned at m1-uring (M1 lane C): one new source —
    //   4. executor/runtime/src/io.rs (WORKER_RT / PERMIT_HELD /
    //      IN_IO_SECTION, one thread_local! block) — the pool worker
    //      loop's §2.8/§2.9 bookkeeping: which Runtime this worker thread
    //      serves and whether it currently holds an execution permit /
    //      sits inside a declared blocking section (the io_permit seam
    //      impls read it). Deliberately non-session TLS: runtime workers
    //      are EXECUTORS, not sessions (redesign §2.1) — the state
    //      belongs to the pool thread for the worker loop's lifetime,
    //      carries no session or task identity, and is set/cleared only
    //      by the loop itself (worker_enter/worker_exit); an envelope
    //      bind/unbind must never touch another thread's permit
    //      accounting.
    assert_eq!(count_tree(crates), 465, "TLS census changed; classify the delta in SESSION_ENVELOPE_MANIFEST or document it as non-session TLS");
    let session_sources = [
        ("backend/access/session/src/lib.rs", 1),
        ("backend/utils/init/init_small/src/globals.rs", 4),
        ("backend/utils/init/miscinit/src/userid.rs", 1),
        ("backend/catalog/catalog_namespace/src/lib.rs", 1),
        ("backend/catalog/catalog_namespace/src/path.rs", 2),
        ("backend/utils/time/snapmgr/src/lib.rs", 2),
        ("backend/access/transam/xact/src/state.rs", 2),
        ("backend/access/transam/xact/src/engine.rs", 2),
        ("backend/access/transam/xact/src/lib.rs", 1),
        ("backend/utils/misc/guc/src/store.rs", 1),
        ("backend/utils/misc/guc/src/lib.rs", 1),
        ("backend/utils/misc/guc_tables/src/session.rs", 5),
        ("backend/utils/resowner/resowner/src/lib.rs", 1),
        ("backend/utils/error/elog/src/stack.rs", 4),
        ("backend/storage/lmgr/lmgr_proc/src/lib.rs", 1),
        ("backend/utils/cache/catcache/src/lib.rs", 2),
        ("backend/utils/cache/catcache/src/graph.rs", 1),
        ("backend/utils/cache/relcache/src/lib.rs", 1),
        ("backend/utils/cache/typcache/src/lib.rs", 1),
        ("backend/utils/cache/plancache/src/lib.rs", 3),
        ("backend/utils/cache/inval/src/lib.rs", 2),
        ("backend/utils/cache/cache_syscache/src/lib.rs", 1),
        ("backend/utils/cache/relmapper/src/lib.rs", 1),
        ("backend/utils/cache/partcache/src/lib.rs", 1),
        ("backend/utils/cache/ts_cache/src/lib.rs", 1),
        ("backend/utils/cache/cache_evtcache/src/lib.rs", 1),
    ];
    for (path, expected) in session_sources {
        let source = std::fs::read_to_string(crates.join(path)).unwrap();
        let actual = source
            .lines()
            .filter(|line| {
                let line = line.trim_start();
                line.starts_with("thread_local!")
                    || line.starts_with("std::thread_local!")
                    || line.starts_with("::std::thread_local!")
            })
            .count();
        assert_eq!(
            actual, expected,
            "session TLS declarations changed in {path}"
        );
    }
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
        assert!(CurrentSessionExists());
        assert!(!SessionEnvelopeBoundaryClean());
        assert_eq!(
            catalog_namespace::CaptureSessionNamespaceState(),
            a.namespace
        );

        let inner = bind_session_envelope_with(&b, || {
            drains += 1;
            Ok(())
        })
        .unwrap();
        assert_state(23, 16384, (2300, 2301));
        assert_eq!(
            catalog_namespace::CaptureSessionNamespaceState(),
            b.namespace
        );
        drop(inner);
        assert_state(22, 8192, (2200, 2201));
        assert_eq!(
            catalog_namespace::CaptureSessionNamespaceState(),
            a.namespace
        );
        drop(outer);
        assert_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
        assert!(!CurrentSessionExists());
        assert!(SessionEnvelopeBoundaryClean());
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
        let (base, mut target, _) = contexts();
        let mut drains = 0;
        let error = bind_session_envelope_with(&base, || {
            drains += 1;
            Ok(())
        })
        .err()
        .expect("uninitialized target session must fail");
        assert!(error.message().contains("initialized target session"));
        assert_eq!(drains, 0);

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

        target.xact_nest_level = 0;
        target.transaction_active = false;
        target.guc_nest_level = 1;
        let error = bind_session_envelope_with(&target, || {
            drains += 1;
            Ok(())
        })
        .err()
        .expect("nested GUC target must fail");
        assert!(error.message().contains("SET LOCAL"));
        assert_eq!(drains, 0);

        target.guc_nest_level = 0;
        target.pending_invalidations = true;
        let error = bind_session_envelope_with(&target, || {
            drains += 1;
            Ok(())
        })
        .err()
        .expect("uncommitted invalidations must fail");
        assert!(error.message().contains("uncommitted invalidations"));
        assert_eq!(drains, 0);

        target.pending_invalidations = false;
        target.data_dir = Some("/other-cluster");
        let error = bind_session_envelope_with(&target, || {
            drains += 1;
            Ok(())
        })
        .err()
        .expect("path mismatch must fail");
        assert!(error.message().contains("path identity"));
        assert_eq!(drains, 0);
    })
    .join()
    .unwrap();
}

#[test]
fn drain_failure_and_dirty_exit_restore_without_partial_binding() {
    std::thread::spawn(|| {
        setup();
        let (_base, target, _) = contexts();
        let error = bind_session_envelope_with(&target, || {
            Err(PgError::new(ERROR, "invalidation drain failed").into())
        })
        .err()
        .expect("drain failure must refuse binding");
        assert!(error.message().contains("drain failed"));
        assert_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
        assert_eq!(ENVELOPE_DEPTH.get(), 0);

        let binding = bind_session_envelope_with(&target, || Ok(())).unwrap();
        init_small::globals::SetCritSectionCount(1);
        let error = binding.finish().expect_err("dirty exit must fail");
        assert!(error.message().contains("holdoff"));
        assert_state(BOOTSTRAP_SUPERUSERID, 4096, (InvalidOid, InvalidOid));
        assert_eq!(ENVELOPE_DEPTH.get(), 0);
        init_small::globals::SetCritSectionCount(0);
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
