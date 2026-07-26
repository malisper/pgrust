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
    // Train-14's own cargo (int-key distinct, qualed text min/max, near-unique text top-n, topn) adds ZERO sources — the
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
    // 476, re-pinned at train-18 (the ordered-grouped sorted-agg runtime sink):
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
    // 506, WS-B grant-donation thread_local (single-executor merge, renumbered
    // to slot 46 over the t30 fold):
    //   46. executor/runtime/src/sched.rs LEDGER_GRANT — per-worker
    //      admission bookkeeping holding (scheduler, slot) while the thread
    //      is inside a ledger-JOINED run_task (set by run_task_admitted,
    //      cleared by GrantCtx drop — unwind-safe). Consulted by declared-
    //      blocking-section entry points (io.rs permit seams, blocking.rs
    //      facade) so the width grant is donated and retaken alongside the
    //      execution permit. Deliberately non-session TLS: empty on non-
    //      worker threads and knob-OFF, plain per-thread state owned by the
    //      pool-loop worker (same pattern as blocking.rs's PERMIT_SEM /
    //      slot 13).
    // t30 fold deltas (509 = 506 + 3):
    //   47. contrib/auto_explain/src/hooks.rs — per-backend ExecutorRun
    //      nesting depth (C auto_explain.c static nesting_level becomes
    //      per-THREAD state); plain executor-hook bookkeeping owned by the
    //      backend thread, no session identity, no cross-thread access.
    //   48. transam/xlogrecovery/tests/sim_crash_sweep.rs — DST sim-corpus
    //      test-rig TLS (integration test only, absent from product
    //      codegen; same class as the loom/e2e rig statics).
    //   49. replication/logical/reorderbuffer/src/tests.rs (+2, the
    //      second from the checkxid-alive car) — cfg(test) rig TLS beside
    //      the existing slot; never in dist.
    //   50. access/heap/visibilitymap/src/tests.rs — cfg(test) rig TLS
    //      (standby-logical car's catalog-cleanup-flag tests); never in
    //      dist.
    // t31 fold deltas (517 = 511 + 6), all C-parity per-backend statics
    // from the contrib wave (one backend = one thread, rule 10):
    //   51. commands/explain/src/state.rs — EXPLAIN extension-option state
    //      (the pg_overexplain hook surface; C's static becomes per-thread
    //      hook bookkeeping; no session identity).
    //   52. utils/error/elog/src/sink.rs — debug_query_string (C's global;
    //      the dblink car's current_query() fix): per-backend CURRENT
    //      STATEMENT text, set/cleared at tcop's dispatch points;
    //      statement-scoped, no session identity.
    //   53. contrib/dblink/src/registry.rs — C dblink.c remoteConnHash:
    //      per-backend named-connection registry (session-lifetime state
    //      owned by the backend thread; close/cleanup per the lane's
    //      lifecycle audit).
    //   54. contrib/pg_overexplain/src/lib.rs — per-backend option state
    //      (C static parity beside 51).
    //   55. contrib/postgres_fdw/src/connection.rs — C connection.c
    //      ConnectionHash: per-backend FDW connection cache (same class
    //      as 53).
    //   56. contrib/postgres_fdw/src/shippable.rs — C shippable.c
    //      ShippableCacheHash: per-backend memo, syscache-inval driven,
    //      no session identity.
    // t32 fold delta (518 = 517 + 1):
    //   57. tcop/postgres/src/sim_net.rs — SENT_LOG (the sim-converge
    //      sent-log artifact dump): per-wire-client-thread transcript
    //      alignment log. Whole module is cfg(pgrust_sim) at the mod decl
    //      — ABSENT from product codegen (same class as 48); source census
    //      sees the text. No session identity.
    // night/row-emit-funnel delta (519 = 518 + 1):
    //   58. executor/execmain/src/lanev2/runtime_passthrough.rs
    //      WORKER_EXEC_PT — per-runtime-worker executor state for the
    //      parallel row-emit funnel (World-B): a fresh QueryDesc over the
    //      leader-arena pstmt + this worker's RowEmitSink, built on the
    //      worker's first claimed morsel and released on drive teardown.
    //      Plain per-thread state owned by the pool-loop worker thread —
    //      the same non-session class as slot 46's LEDGER_GRANT and
    //      blocking.rs's PERMIT_SEM (slot 13). No session identity, no
    //      cross-thread access.
    // night/fix-tz-abbrevs delta (t36 composed: 518 = 519 - 1): REMOVED
    //   timezone/pgtz/src/lib.rs TIMEZONE_CACHE — the session-arena
    //   pg_tzset cache was the pg_timezone_abbrevs use-after-free
    //   (localtime/clock.rs:32 panic): `&'static PgTz` escapes the thread
    //   via the process-shared DynamicZoneAbbrev table, so the cache is now
    //   a process-permanent static (C dynahash parity), not TLS.
    // m2-inc2 fold delta (520 = 518 + 2) — PGPROC-leasing pool workers,
    // both deliberately NON-SESSION TLS:
    //   59. access/transam/parallel/src/standing.rs — ON_POOL_SERVE: a
    //      bool marking "this thread is inside a pool-db serve_ticket"
    //      (arm drivers read it to disable sticky retention on pool
    //      threads). Engagement-scoped RAII flag on the executor THREAD;
    //      no session identity, never captured/restored by an envelope
    //      (the binder guard it influences owns the session state).
    //   60. postmaster/launch_backend/src/lib.rs — POOL_IDENT: the pool
    //      thread's leased-identity state (None/Ready(fence-epoch)/
    //      Poisoned), thread-lifetime bring-up bookkeeping exactly like a
    //      gang thread's implicit state; identity is PGPROC/proc-number
    //      (shared memory), not session state — the per-engagement session
    //      view is bound/unbound by the query-task binder.
    // night/cost-model-steps12 delta (521 = 520 + 1):
    //   61. optimizer/plan/planner/src/m5_suppress.rs cost_shadow
    //      LAST_SAMPLE — the step-2 cost-shadow EXPLAIN sample slot:
    //      derived plan-time observability (the last covered
    //      classification's whitelist-vs-model verdict pair), written only
    //      while PGRUST_M5_COST_EXPLAIN is armed (default OFF — slot never
    //      touched otherwise), cleared at every standard_planner entry and
    //      taken (cleared) by EXPLAIN right after planning. Statement-
    //      scoped scratch on the backend thread — same non-session class
    //      as slot 52's debug_query_string. No session identity; never
    //      read across a session boundary.
    // m2-inc3 rung-2 delta (522 = 521 + 1), deliberately NON-SESSION TLS:
    //   62. executor/execmain/src/execmain.rs — SERIAL_LEASE_DEPTH: the
    //      lease-only measurement vehicle's top-level-ExecutorRun depth
    //      guard (PGRUST_RUNTIME_SERIAL_LEASE, default OFF). Pure frame
    //      counter, unwound by construction with the executor frames;
    //      no session identity, never captured/restored by an envelope.
    // night/nlidx-arm delta (523 = 522 + 1), deliberately NON-SESSION TLS:
    //   63. executor/execmain/src/lanev2/runtime_nlindex.rs — WORKER_NLEXEC:
    //      the NL-inner-index arm's per-worker private executor slot (the
    //      worker's own QueryDescHandle + errored flag for teardown-path
    //      selection). Engagement-scoped scratch on the gang thread —
    //      installed at worker bind, taken at detach; identity is the
    //      engagement, never the session; never captured/restored by an
    //      envelope.
    // m4.1 fold delta (524 = 523 + 1) — vacuum driver swap:
    //   64. commands/vacuumparallel/src/lib.rs — POOL_CX: the index-pass
    //      participant's per-DRIVE worker context pointer (opened
    //      relations + strategy on the drive frame), the vacuumlazy
    //      WORKER_CX / runtime_scan WORKER_EXEC precedent exactly: exists
    //      only between the drive frame's publish and clear on ONE thread,
    //      torn down before unbind on every path; no session identity
    //      (the binder owns all session state movement).
    // night/m41-w-gap delta (525 = 524 + 1), deliberately NON-SESSION TLS:
    //   65. access/heap/vacuumlazy/src/lib.rs — phase_trace ACC: the GL-M41-2
    //      per-phase vacuum clock accumulator (ns/calls/WAL per phase),
    //      trace-gated instrumentation scratch on the driving thread —
    //      reset at vacuum entry, read at the trace emit; no session
    //      identity, never captured/restored by an envelope.
    // m2-inc3 rung-3 delta (526 = 525 + 1), deliberately NON-SESSION TLS:
    //   66. executor/runtime/src/lib.rs — SESSION_RESIDUE: the pool
    //      worker's parked-sticky-retention HINT (bool) feeding the
    //      scheduler's unbound-work eviction gate and the affinity
    //      tiebreak. Pure scheduling advisory on the executor THREAD; the
    //      session state it hints at lives in the binder layer's sticky
    //      slot (query_task_guard STICKY, already row 57-class), which
    //      owns capture/restore — the hint itself carries no identity.
    // GL-SLEASE-1 flip (count UNCHANGED at 522 — the census counts
    // thread_local! source sites and the new cell rides row 61's block):
    //   62b. executor/execmain/src/execmain.rs — SERIAL_LEASE_HELD joined
    //      the SERIAL_LEASE_DEPTH block: the runtime whose permit the
    //      current top-level serial lease holds (feeds the engagement-
    //      yield release/re-acquire). Pure permit bookkeeping, the same
    //      executor-frame lifecycle and non-session classification as
    //      row 61.
    // m4.2 fold delta (527 = 526 + 1) — parallel btree build on the pool:
    //   67. access/nbtree/nbtsort/src/pool.rs — BT_CX: the build-scan
    //      relations + IndexInfo + form scratch on the drive frame), the
    //      vacuumparallel POOL_CX / vacuumlazy WORKER_CX precedent
    //      exactly: exists only between the drive frame's publish and
    //      clear on ONE thread, torn down before unbind on every path; no
    //      session identity (the binder owns all session state movement).
        // POOL-QOS interactive tier delta (+1), deliberately NON-SESSION TLS:
    //   access/transam/parallel/src/standing.rs — the serve-yield block
    //      {YIELD_DETACHED, CURRENT_SERVE_BOARD, YIELD_PENDING}: pool-
    //      serve-scoped protocol bookkeeping (the DetachGuard suppression
    //      mark, the current serve's board Arc for the yield grant, the
    //      granted-yield pending mark). Engagement-scoped RAII state on
    //      the executor THREAD (pool_serve installs/clears); no session
    //      identity, never captured/restored by an envelope — the
    //      ON_POOL_SERVE classification exactly.
    // 528 -> 527 (Phase-5 D1, t42): the pardistinct GM-hybrid deletion
    // removed the PdWorkerSink worker-fragment thread_local with the
    // executor paths it served (lanev2.rs pardistinct region) — a
    // deletion-explained movement, not an unclassified source.
    // GL-MEMWATCH-1 delta (529 = 527 + 2), both deliberately NON-SESSION TLS:
    //   68. tcop/postgres/src/simple_query.rs — HOG_CTX: the memory
    //      watchdog e2e's per-thread cache of the "WatchdogTestHog"
    //      session_root pointer (developer GUC, default-off). The context
    //      estate itself is owned by the session-teardown Roots phase
    //      (session_root registration); the TLS is a lookup cache on the
    //      backend thread that dies with the thread — no session identity,
    //      never captured/restored by an envelope.
    //   69. utils/mmgr/mcxt_stats/src/lib.rs — IN_OOM_DUMP: reentry guard
    //      flag for the allocation-failure context dump (C parity for
    //      aset.c's MemoryContextStats-on-OOM). Pure per-thread scratch;
    //      no session identity    // night/mcx-pool-stripe delta (531 = 529 + 2, composed at t43 over the memwatch pair), deliberately NON-SESSION TLS:
    //   70. _support/mcx/src/lib.rs — tls_pools ACCT/CHILD_VECS: the
    //      per-thread context-pool free lists (recycled AcctInner blocks +
    //      children-Vec capacities) replacing the global spin-locked pools
    //      on the context create/destroy path (the @high-backend-count
    //      contention riser). Pure allocator-block caches on the owning
    //      thread — entries are raw memory, no session identity, never
    //      captured/restored by an envelope; TLS Drop hands blocks back to
    //      Global at thread exit (the FPBUDGET leak law), kill switch
    //      PGRUST_MCX_POOL_STRIPE=0 restores the globals.
    //   71. _support/mcx/src/aset.rs — KEEPER_TLS: the per-thread parked
    //      keeper-block lists (C context_freelists parity — C's freelist is
    //      per-process = per-backend, so per-thread IS the C shape). Same
    //      classification and teardown story as row 70.
    // W2a inc-2 delta (532 = 531 + 1, composed at t43), deliberately NON-SESSION TLS:
    //   72. access/heap/heapam/src/dml.rs — PARALLEL_WRITE_PERMITS: the
    //      RAII depth counter behind ParallelWriteGuard — the block-run
    //      write sink's carve through heap_prepare_insert's real
    //      is_parallel_worker refusal (the CTAS-dop4 postmortem's
    //      release-reachable tripwire). Armed strictly around the sink's
    //      own write calls on the worker THREAD and zero on every
    //      statement boundary by RAII; no session identity, never
    //      captured/restored by an envelope.
    // GL-INERT-FIXES-1 delta (533 = 532 + 1, composed at t44), GUC-backed non-envelope TLS:
    //   73. utils/adt/arrayfuncs/src/lib.rs — ARRAY_NULLS: C's Array_nulls
    //      backing cell behind the GUC var slot (default true). Session
    //      identity is owned by the GUC store (snapshot/restore writes
    //      through the installed accessors, incl. replace_exact_guc_state
    //      above) — the parse_expr TRANSFORM_NULL_EQUALS classification
    //      exactly; no envelope row.
    // logdec-port delta (534 = 533 + 1):
    //   74. replication/logical/reorderbuffer/src/tests.rs (+1 beside row
    //      49's pair) — STATS_FLUSHES: cfg(test) rig TLS counting spill
    //      stats-flush hook invocations; never in dist.
    // logdec-port streaming delta (535 = 534 + 1), deliberately NON-SESSION
    // TLS:
    //   75. replication/logical/worker/src/stream_apply.rs — the serial
    //      streamed-apply spool state (C worker.c statics
    //      in_streamed_transaction/stream_xid/stream_fd/subxact_data plus
    //      MyLogicalRepWorker->stream_fileset): per-WORKER state owned by
    //      the apply bgworker thread for the life of the worker; no session
    //      identity, never captured/restored by an envelope (one backend
    //      worker = one thread, rule 10).
    // GL-AIO-1 delta (537 = 535 + 3 - 1, composed at t44), all NON-SESSION TLS
    // (+3 sites, -1 deleted with the aio_config crate):
    //   76. storage/aio/aio_core/src/lib.rs — MY_BACKEND (C
    //      pgaio_my_backend): this THREAD's aio backend-slot binding,
    //      armed by pgaio_init_backend (BaseInit/BaseInitRetained; pool
    //      and gang executors attach at bring-up with their own BgWorker
    //      procnos), torn down by pgaio_shutdown via before_shmem_exit
    //      (park teardown consumes it, so retained threads re-arm clean).
    //      IOs issued during a pool serve are owned by the WORKER's slot,
    //      as in C where the issuing process owns the IO regardless of
    //      session; never captured/restored by an envelope.
    //   77. storage/aio/aio_core/src/method_worker.rs — MY_IO_WORKER_ID:
    //      the io-worker thread's registry slot ordinal (B_IO_WORKER
    //      kind only); released by pgaio_worker_die at proc_exit.
    //   78. same file — EXECUTED_IOS: per-worker executed-IO counter
    //      (the read-path e2e witness). Pure thread-local diagnostics.
    //   (-1: aio_config's MY_BACKEND_ATTACHED site left with the crate,
    //      absorbed into row 76's real slot binding.)
    // GL-STMTTASK-1 delta (538 = 537 + 1, composed at t44), deliberately NON-SESSION TLS:
    //   79. tcop/postgres_seams/src/lib.rs — stmt_task_arm ARMED: the
    //      statement-scoped protocol arm flag (exec_simple_query arms one
    //      statement's top-level portal run; the executor's statement-task
    //      hook consumes it on first entry; RAII-disarmed at statement
    //      end). Pure per-statement routing advisory on the SESSION thread
    //      — carries no session identity, never set on workers, never
    //      captured/restored by an envelope (an engagement rebuilds
    //      nothing from it; consume-once semantics make leakage across
    //      statements structurally impossible).
    // GL-STMTTASK-2 delta (539 = 538 + 1), deliberately NON-SESSION TLS:
    //   80. executor/execmain/src/lanev2/stmt_task.rs — SESSION_FUNNEL: the
    //      session thread's persistent statement-task row funnel (change 1,
    //      standing-engagement reuse — one ring + wake hook created on the
    //      first armed statement, reset per statement). A pure TRANSPORT
    //      cache on the session thread: rows never survive a statement, the
    //      wake hook keys the thread's own proc latch, and the funnel
    //      carries no session identity — never captured/restored by an
    //      envelope (a fresh thread simply rebuilds one on first use); dies
    //      with the thread (Arc drop, heap only).
    // GL-STMTTASK-2 quantum-yield delta (541 = 539 + 2), deliberately
    // NON-SESSION TLS:
    //   81. tcop/postgres_seams/src/lib.rs — stmt_yield ARMED: the
    //      CHECK_FOR_INTERRUPTS-side span flag (set only for one
    //      statement-task execution span, RAII-restored by the executor's
    //      span guard). Pure per-span routing advisory; carries no session
    //      identity; never captured/restored by an envelope.
    //   82. executor/execmain/src/lanev2/stmt_task.rs — YIELD_GOV: the
    //      armed span's governor state (runtime handle + quantum stamp) —
    //      scheduling bookkeeping unwound with the executor frames by the
    //      span guard; no session identity, dies with the thread.
    //   83. lanev2/stmt_task.rs — the yield machinery's thread_local
    //      blocks: MY_YIELD_SLOT + YIELD_RT (one block, replacing the
    //      clock-read governor's YIELD_GOV block in the same census slot)
    //      and the debug-enforcement TICKS counter block
    //      (cfg(debug_assertions) throttle scratch) — pure scheduling
    //      bookkeeping, no session identity, die with the thread
    //      (541 -> 542 net).
    // RC/t44-fixes NET-ZERO delta (542 -> 542): two offsetting movements,
    // both classified here rather than netted silently.
    //   -1  storage/lmgr/lock/src/fastpath.rs — the GL-LOCKCACHE-1 fix
    //      (per-group fast-path use counts keyed by PGPROC, not thread)
    //      DELETES that file's thread_local block by construction: the
    //      counts were the defect, because proc identity is leased across
    //      pool threads and a migrated statement read stale/zero counts.
    //   +1  access/table/tableam/src/lib.rs, slot 84 below.
    // copyerr-teardown delta, deliberately NON-SESSION TLS:
    //   84. access/table/tableam/src/lib.rs — CB_EOXACT_REGISTERED: one-shot
    //      Cell<bool> recording that this backend thread registered the
    //      cbstore ingest-writer purge as a xact callback (the errored-COPY
    //      teardown fix: abandoned TLS writers must drop at transaction end,
    //      inside the arena lifetime, never in the thread's TLS destructor).
    //      Pure per-thread registration bookkeeping, the slot-20
    //      SIMPLE_EXIT_RELEASE class exactly: no session identity, no state
    //      movement, never reset — the registration (and the flag's meaning)
    //      live as long as the backend thread. The purge it registers is
    //      thread-correct wherever it fires: it clears the EXECUTING
    //      thread's writer map, and any writer present there at a
    //      transaction end is abandoned by construction (statement-end
    //      flush removed every published one).
    // GL-VACGUARD-1 delta (RC: 542 -> 543 net, see the pair above), deliberately NON-SESSION TLS:
    //   85. access/heap/heapam/src/freeze.rs — FREEZE_MEMBER_SCRATCH: the
    //      reset-per-call bump context for FreezeMultiXactId's multixact
    //      member arrays, standing in for C's palloc into
    //      CurrentMemoryContext (C imposes no cap on member counts; the
    //      fixed 256-element stack array it replaces was enforced by a
    //      reachable release `assert!`). Same class and same rationale as
    //      this crate's own KEY_TEST_SCRATCH: there is no per-tuple mcx
    //      reachable that deep in the freeze path. Reset at entry to every
    //      call and never read across one, so it carries no state at all
    //      between calls, let alone session identity — pure per-thread
    //      scratch that dies with the thread, never captured or restored by
    //      an envelope.
    // GL-SHMSEAM-1 delta (+1), TEST-ONLY and NOT session state — not a
    // SESSION_ENVELOPE_MANIFEST member:
    //   utils/init/miscinit/src/tests.rs — EMITTED, the datadir-first-contact
    //   tests' capture buffer for what `elog::set_emit_log_hook` reported.
    //   Per-thread because the emit hook itself is per-thread and the tests that
    //   install it run concurrently on separate harness threads, so a
    //   process-global buffer would let one test read another's FATAL. Spelled as
    //   ONE `thread_local!` block. `cfg(test)` only, therefore absent from every
    //   shipped profile; counted only because the census counter is textual.
    //   Non-session on the substance too: scratch output for the duration of one
    //   call, no session identity, nothing an envelope could capture or restore.
    //   The new row touches no file on the `session_sources` tripwire below,
    //   which is correct: that list covers SESSION TLS files and
    //   miscinit/src/tests.rs is not one.
    // NOTE for whoever merges this to main: the same one declaration lands on a
    // DIFFERENT absolute total there (562 -> 563 at t56). Re-derive the pin at
    // the tip it will be enforced against; do not carry this number across.
    assert_eq!(count_tree(crates), 544, "TLS census changed; classify the delta in SESSION_ENVELOPE_MANIFEST or document it as non-session TLS");
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
