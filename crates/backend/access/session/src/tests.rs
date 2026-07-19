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
    // 466, re-pinned at m2-agg-sink (M1 scan pipelines + M2 aggregation
    // sink merged): the two runtime engagement arms each add a per-helper
    // executor slot —
    //   4. executor/execmain/src/lanev2/runtime_scan.rs WORKER_EXEC
    //   5. executor/execmain/src/lanev2/runtime_agg.rs WORKER_EXEC
    //      — each holds the BOUND HELPER's thread-local QueryDesc handle for
    //      one engagement drive (built inside the query-task binding, torn
    //      down before unbind on every path, stale-checked at rebuild).
    //      Deliberately non-session TLS: the slot exists only between
    //      POST_TASK_PARK entry and exit on a parallel helper thread; no
    //      session survives across it and the binder owns all session state
    //      movement (envelope capture/restore must never see a mid-drive
    //      executor).
    // 467, re-pinned at m2-integration (agg + distinct sinks merged): the
    // third runtime engagement arm adds its per-helper executor slot —
    //   6. executor/execmain/src/lanev2/runtime_distinct.rs WORKER_EXEC —
    //      same class and same argument as 4/5 (bound-helper drive slot,
    //      built inside the query-task binding, torn down before unbind on
    //      every path, non-session TLS). Conductor note: the distinct lane's
    //      own 464 pin was stale for its tree (its fleet unit sweeps did not
    //      run this crate's suite); the merged pin re-counts all three arms.
    // 468, re-pinned at chaos-battery (m2-integration + m1-uring merged):
    // lane C's io_uring pool-worker slot joins the three engagement arms —
    //   7. executor/runtime/src/io.rs (WORKER_RT / PERMIT_HELD /
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
    // 469, re-pinned at train-12 (m2-integration x train-11 base composed):
    // the guc-snapshots lane (train-11 car 2) added one block its own
    // battery never counted (its unit sweeps did not run this crate's
    // suite — the same stale-pin class as the distinct lane's 464) —
    //   8. utils/misc/guc/src/layers.rs (SESSION_BASE + query-pin
    //      statement-window cache, one thread_local! block) — the typed
    //      base snapshot this thread last adopted (its started-with GUC
    //      values) plus a mutation-counter-keyed cache for the query pin.
    //      Deliberately non-session TLS in the envelope sense: the base is
    //      installed at child bring-up / worker BIND (the binder owns the
    //      movement, exactly like the WORKER_EXEC slots 4-6) and advanced
    //      only by the thread's own ProcessConfigFile pass; the pin cache
    //      is derived state keyed on the session store's mutation counter
    //      (stale entries can never be adopted). Envelope capture/restore
    //      moves the session GUC STORE; the layered snapshots follow it
    //      through the bind path by construction (guc-snapshots lane
    //      design, kill switches PGRUST_NO_GUC_BASE/_BIND).
    // 470, re-pinned at train-12 (m3-hashjoin merged): the fourth runtime
    // engagement arm adds its per-helper executor slot —
    //   9. executor/execmain/src/lanev2/runtime_hashjoin.rs
    //      (HJ_WORKER_EXEC + HJ_PAYLOAD, one thread_local! block) — the
    //      bound helper's drive-scoped QueryDesc handle plus the frozen
    //      join table the run_morsel bodies read; same class and same
    //      argument as WORKER_EXEC slots 4-6 (built inside the query-task
    //      binding, torn down before unbind on every path, non-session
    //      TLS — the binder owns all session state movement).
    // 471, re-pinned at train-12 (m3-sort merged): the fifth runtime
    // engagement arm adds its per-helper executor slot —
    //   10. executor/execmain/src/lanev2/runtime_sort.rs WORKER_EXEC —
    //      identical class and argument as slots 4-6/9 (bound-helper
    //      drive slot inside the query-task binding, torn down before
    //      unbind on every path, non-session TLS).
    // 473, re-pinned at runtime-ceremony2 (lazy first-touch bind + sticky
    // session-affine binding, notes/runtime-ceremony2.md) —
    //   11. access/transam/parallel/src/query_task_guard.rs (STICKY +
    //      ACTIVE_DEFERRED, one thread_local! block) — the standing gang
    //      worker's KEYED session-bind retention (parked, disarmed guard;
    //      evicted by the binder before any foreign-session bind) and the
    //      mid-drive bound-guard slot of the deferred first-touch binding.
    //      Non-session TLS in the census sense with one sanctioned twist:
    //      the sticky slot deliberately RETAINS binder-owned session state
    //      between same-session engagements — the envelope's exception is
    //      SessionEnvelopeBoundaryIssueForRetainedBind (this crate), and
    //      the binder still owns ALL session-state movement (bind/resume/
    //      evict/park run only inside DeferredQueryTaskBinding). wpool /
    //      launched helpers never use the slot (sticky_allowed=false);
    //      envelope bind/unbind never touches another thread's slot.
    //      (runtime_scan.rs's LAZY_CTX rides the existing WORKER_EXEC
    //      block — same drive-scoped class as slots 4-6.)
    //   12. access/transam/parallel/src/standing.rs DEFERRED_VIS — the
    //      standing serve's visibility-deferral latch (Armed by
    //      serve_ticket, consumed at the first-touch bind, reset in the
    //      serve tail): pure worker-loop bookkeeping, no session identity,
    //      same argument as the io.rs pool-worker block (slot 7).
    // 474, re-pinned at train-14 (conductor debt fix): three train-13 cars
    // landed AFTER ceremony2's 473 re-pin and this suite never ran at the
    // train-13 merged tip (its battery's TEST_CRATES was empty), so the pin
    // went stale by net +1 — three additions minus two migrations
    // (transam_xlog/src/write.rs's walwriter slot moved into the auxjob
    // layer; bufmgr/src/bgwriter_sync.rs's block moved into the bgwriter
    // job). The additions, all classified non-session:
    //   13. executor/runtime/src/blocking.rs PERMIT_SEM (m35-spill inc-1) —
    //      non-null exactly while a PermitThreadReg for the pool worker
    //      thread lives (the spill blocking-section facade reads it): pool
    //      thread bookkeeping with no session or task identity, created and
    //      cleared only by the worker loop — same argument as the io.rs
    //      pool-worker block (slot 7).
    //   14. postmaster/auxjob/src/lib.rs THREAD_CHILD_INITED (bgjobs
    //      identity-seat layer) — once-per-thread aux-child init latch
    //      (InitPostmasterChild/BaseInit halves) shared across all aux jobs
    //      hosted by the thread: aux daemons are not sessions; the latch
    //      never crosses threads and carries no session state.
    //   15. access/heap/vacuumlazy/src/morsels.rs WORKER_CX (vacuum-morsels)
    //      — the vacuum SCAN task set's drive-scoped worker context pointer,
    //      set for one run_morsel drive and cleared on every exit path —
    //      same drive-scoped class and argument as WORKER_EXEC slots 4-6.
    // Train-14's own cargo (q5/q22/q14/topn) adds ZERO sources — the
    // per-file census at the train-13 tip and the train-14 tip is identical.
    // (Merge reconciliation, train-14 car 6: the m35-spill-joins lane
    // independently re-pinned 474 attributing the whole drift to morsels.rs
    // WORKER_CX; this block's net decomposition subsumes it — one pin kept.
    // m35 inc-4/5's join-batch spill code itself adds no TLS source.
    // Merge reconciliation, m5-integration-r2: the m5-liveness lane's own
    // 474 re-pin attributed the whole train-13 drift to morsels.rs
    // WORKER_CX alone — train-14's fuller +3/−2 decomposition above
    // subsumes it, same precedent as the m35 pin; one pin kept.)
    // 475, re-pinned at band-2b (runtime plain-distinct sink):
    //   16. executor/execmain/src/lanev2/runtime_plaindistinct.rs
    //      WORKER_EXEC — the plain exact-DISTINCT sink helper's drive-scoped
    //      worker executor slot (built inside the query-task binding, torn
    //      down on every drive exit path) — same drive-scoped class and
    //      argument as WORKER_EXEC slots 4-6.
    // 476, re-pinned at train-18 (q28-sorted-arm ordered-grouped runtime sink):
    //   17. executor/execmain/src/lanev2/runtime_agg_sorted.rs
    //      WORKER_EXEC — the ordered-grouped (sorted-agg) sink's drive-scoped
    //      worker executor slot (QueryDescHandle + fold keys/spec, built
    //      inside the query-task binding, torn down on every drive exit
    //      path incl. mark_self_errored) — same drive-scoped class and
    //      argument as WORKER_EXEC slots 4-6 and the band-2b slot 16.
    // 477, re-pinned at m5-boarding (M5-0/1 router merged onto train-19;
    //   this slot was first pinned as 475/slot-16 at m5-integration on the
    //   train-13/16 bases, renumbered here over train-18/19's two sink
    //   slots above):
    //   18. executor/execmain/src/lanev2/router.rs DUMP (the
    //      DumpOnThreadExit guard armed by arm_dump_on_thread_exit) — the
    //      M5-1 telemetry dump-on-exit hook: a drop guard whose only act
    //      is writing the process-global router counters to
    //      m5-router-stats.<pid>.tsv when the backend thread exits, and
    //      only when PGRUST_LANE_V2_STATS is armed. Pure telemetry
    //      bookkeeping, no session identity, no state movement — the
    //      stats.rs dump-on-exit discipline; same argument as the worker
    //      pool-loop block (slot 7).
    // 478, parallel-copy lane (+1, renumbered to slot 19 over m5-boarding's
    //   router DUMP slot 18 at the train-20 merge):
    //   19. commands/copy/src/parallel.rs WORKER_CX (morsel-parallel COPY)
    //      — the COPY chunk task set's drive-scoped worker context pointer
    //      (parse state + chunk encoder plan), set for one drive_pinned
    //      frame and cleared before the frame drops — the EXACT class and
    //      argument as slot 15 (vacuumlazy morsels.rs WORKER_CX): full-
    //      identity parallel helpers, no cross-thread access, no retained
    //      session state.
    // 479, simplecache lane (fix/plpgsql-simple-cache):
    //   20. pl/plpgsql/src/exec.rs SIMPLE_EXIT_RELEASE — one-shot Cell<bool>
    //      recording that this backend thread registered its on_proc_exit
    //      release of function-lifetime simple-expression plan pins
    //      (release_simple_states_at_exit; the TLS-destructor-order law).
    //      Pure per-thread registration bookkeeping: no session identity,
    //      no state movement, never reset — the registered callback (and
    //      the flag's meaning) live exactly as long as the backend thread,
    //      same class as the router DUMP guard (slot 18).
    // +14 recovery slots (t26 car-10 re-board; renumbered after the simplecache slot): ALL
    // one class — C per-PROCESS function-statics of the replication/
    // recovery machinery become per-THREAD TLS on the thread model, owned
    // by DEDICATED background threads (startup, walreceiver, walsender,
    // logical apply/tablesync workers, slotsync) that never host a swapped
    // session; no envelope capture/restore applies. Deliberately
    // non-session TLS, no SESSION_ENVELOPE_MANIFEST rows:
    //   21. transam/xlogrecovery/src/targets.rs (x2) — recovery-target
    //      bookkeeping of the startup thread.
    //   22. transam/xlogrecovery/src/lib.rs (+1) — startup-thread replay
    //      state beside the existing slot.
    //   23. replication/logical/relation/src/lib.rs — apply-worker
    //      relation-map cache.
    //   24. replication/logical/worker/src/lib.rs — apply-worker state
    //      (worker.c per-process statics).
    //   25. replication/logical/worker/src/tablesync.rs — tablesync-worker
    //      state.
    //   26. replication/origin/src/lib.rs — session_replication_origin
    //      analog on the apply thread.
    //   27. replication/slot/src/lib.rs (+1) — per-thread acquired-slot
    //      pointer (MyReplicationSlot analog).
    //   28. replication/slotsync/src/lib.rs — slotsync-worker state.
    //   29. replication/syncrep/src/lib.rs — walsender syncrep queue state.
    //   30. replication/walreceiver/src/lib.rs — walreceiver-thread state.
    //   31. replication/walsender/src/logical_stream.rs — per-walsender
    //      logical-stream state (incl. the WalFlushPacing analog of C's
    //      function-static).
    //   32. storage/ipc/procarray/src/known_assigned.rs — startup-thread
    //      KnownAssignedXids bookkeeping.
    //   33. contrib/pgoutput/src/lib.rs — pgoutput per-decoder context on
    //      the walsender thread.
    // 494, re-pinned at dst/p1-vfs-integrated (DST-P1 WS-C simulated VFS;
    // renumbered to slot 34 over the t26 simplecache+recovery slots at the
    // train-27 merge):
    //   34. storage/file/vfs/src/sim.rs SIM — the deterministic simulated
    //      filesystem's state cell (one simulated universe per harness
    //      thread). The entire sim.rs module is `cfg(pgrust_sim)`-gated —
    //      ABSENT from product codegen (integration-record TLS census:
    //      fd thread_local counts identical to main; vfs product code adds
    //      zero TLS). DST test infrastructure only: no session identity,
    //      no state movement, never compiled into a shipped binary.
    // 496, spi-compile-residual lane (renumbered 35/36 over the t26+DST slots at the train-27 merge)
    // original header: 481, spi-compile-residual lane (fix/spi-compile-residual, PROCPERF P2):
    //   35. executor/execexpr/src/compile.rs COMPILE_ECONOMY — Cell<bool>
    //      compile-cost-policy window armed by standard_executor_start over
    //      InitPlan of cost-gated-cheap statements and RAII-restored
    //      (EconomyWindow) before the start seam returns; it never spans a
    //      statement boundary, carries no session state, and only chooses
    //      whether ready_expr runs its per-row-payoff passes — never a
    //      result byte. Same transient-window class as execexpr's jit
    //      session collector.
    //   36. pl/plpgsql/src/handler.rs PL_GUC_VALUES — Cell<Option<..>>
    //      derived cache of the parsed plpgsql.* GUC values keyed by the
    //      GUC store's per-thread mutation counter (store_mutation_count;
    //      the guc::layers cache-key pattern). Deliberately non-session
    //      TLS: it caches nothing a session owns — it memoizes a pure
    //      function of THIS thread's GUC store, and any session
    //      bind/unbind/SET/RESET/xact-revert mutates that store through
    //      with_store_mut, which bumps the key and invalidates the entry.
    // 500, train-28 merge (the DST t28-set + provider-seam + wasm/t28-set
    // cars meet the census; renumbered 37-40 over the t27 slots):
    //   37. _support/pgsync/src/sim/sched.rs — the permit scheduler's
    //      per-thread slot (vpid binding/current pick state). The whole
    //      sim module is `cfg(pgrust_sim)`-gated — ABSENT from product
    //      codegen; DST test infrastructure only (slot-34 sim.rs class).
    //   38. backend/libpq/pqcomm_simnet/src/imp.rs — sim-net transport
    //      provider's per-thread duplex state. Crate compiles EMPTY on
    //      native by design (cfg pgrust_sim) — slot-34 class, never in a
    //      shipped binary.
    //   39. backend/libpq/pqcomm_stdio/src/lib.rs STATE — the stdio
    //      transport provider's noblock bit: the stdio twin of
    //      pqcomm::socket's CLIENT_STATE (already-classified transport
    //      connection state). One session per process in stdio-wire mode
    //      by construction; no session identity, no state movement.
    //   40. backend/tcop/postgres/src/switches.rs USER_D_OPTION — the
    //      userDoption analog (postgres.c:106): -D switch storage consumed
    //      by SelectConfigFiles at single-user/stdio-wire boot. Boot-time
    //      argv plumbing on the main thread; dead after startup.
    // 505, train-29 merge (fix/ddl-churn-rss FPBUDGET-1 session-cleanup
    // registry meets the census; all five are that car's machinery):
    //   41. utils/mmgr/mcxt_stats/src/lib.rs SESSION_CLEANUPS — THE
    //      session-cleanup registry itself: the per-thread LIFO of teardown
    //      closures run_session_teardown drains at ProcExitThread (C's
    //      on_proc_exit table analog). Deliberately non-session TLS: it
    //      holds cleanup CODE for this thread's current session estate,
    //      never session state — binding a different session re-registers
    //      through the same idempotent flags below; the drain empties it.
    //   42. executor/execmain/src/execmain.rs TEARDOWN_REGISTERED —
    //      once-per-thread registration guard for the parked exec-ctx
    //      skeleton's cleanup. A bool latch, no session identity.
    //   43. libpq/pqcomm/src/lib.rs REGISTERED — once-per-thread
    //      registration guard for the send-buffer cleanup. Same class.
    //   44. utils/init/postinit/src/lib.rs FUNDAMENTALS_REGISTERED —
    //      once-per-thread registration guard for xact/resowner/globals
    //      teardown. Same class.
    //   45. main/main_main/src/bin/postgres.rs (alloc_track IN_HOOK +
    //      TRACKED, one block) — debug_assertions-only allocation-tracker
    //      reentrancy/thread-filter bits (PGRUST_ALLOC_TRACK diagnostics);
    //      absent from dist codegen, never session state.
    assert_eq!(count_tree(crates), 505, "TLS census changed; classify the delta in SESSION_ENVELOPE_MANIFEST or document it as non-session TLS");
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
