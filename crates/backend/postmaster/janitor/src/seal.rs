//! `pgrust_seal_template(text)` — janitor-executed one-call template
//! sealing. Both halves of the protocol live here (the mint.rs convention):
//!
//! - **Backend side** (`request_seal`, called from builtins.rs on the
//!   calling backend's thread, AFTER the owner-or-superuser and
//!   already-a-template checks): post the idempotent seal request, wake
//!   the janitor, park on the backend's own latch until it resolves.
//! - **Janitor side** (`seal_pass`, a janitor tick step): drive each
//!   request through the manual recipe's exact ordering — validate the
//!   target, VACUUM (FREEZE, ANALYZE) it through a one-shot internal
//!   session, then flip `IS_TEMPLATE true ALLOW_CONNECTIONS false` — with
//!   per-entry containment (a failing seal fails its caller, never the
//!   janitor loop).
//!
//! WHY THE JANITOR: the manual recipe is three statements whose ORDER is
//! load-bearing (freeze before seal — the sealed-template flush-mark skip's
//! wraparound caveat assumes freeze-at-seal; see mint_batch_body) and whose
//! middle step cannot run from a SQL function (VACUUM refuses transaction
//! blocks) nor comfortably from a session inside the target (ALTER DATABASE
//! ... ALLOW_CONNECTIONS false refuses the current database). The janitor
//! already owns internal sessions and serializes every lifecycle mutation,
//! so it can run the whole sequence correctly on the caller's behalf.
//!
//! MECHANISM for the vacuum (the prewarm.rs precedent, same rationale): a
//! session is bound to its home database for life, so the janitor itself
//! cannot vacuum the target — a DYNAMIC one-shot background worker connects
//! to the target by oid, runs the whole-database VACUUM (FREEZE, ANALYZE)
//! through the internal `commands_vacuum::vacuum` entry (the maint.rs call
//! shape with a nil relation list = every vacuumable relation, and WITHOUT
//! SKIP_DATABASE_STATS so datfrozenxid advances — the freeze witness), and
//! reports back through the registry. The seal is a small state machine
//! across ticks (Pending -> Vacuuming -> VacuumDone -> flip -> Done), so a
//! long vacuum never stalls minting or reaping.
//!
//! C-parity: pgrust-only surface (like every janitor builtin). The flip
//! goes through the PUBLIC `dbcommands::AlterDatabase` entry (the
//! pool::handout_one precedent — internally callable, its only
//! PreventInTransactionBlock arm is SET TABLESPACE's); no C-shaped entry
//! changes.

use elog::{elog as log_report, ereport};
use mcx::Mcx;
use tableam_vocab::{
    VacOptValue, VacuumParams, VACOPT_ANALYZE, VACOPT_PROCESS_MAIN, VACOPT_PROCESS_TOAST,
    VACOPT_VACUUM,
};
use types_core::{InvalidOid, Oid, ProcNumber};
use types_error::{
    PgError, PgResult, ERRCODE_CANNOT_CONNECT_NOW, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
    ERRCODE_UNDEFINED_DATABASE, ERRCODE_WRONG_OBJECT_TYPE, ERROR, LOG,
};
use types_nodes::parsenodes::AlterDatabaseStmt;
use types_nodes::NodeList;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::registry::{self, PostSeal, SealStatus, SealWork};

/// Hard cap on how long the calling backend parks waiting for its seal.
/// The dominant term is the whole-database VACUUM (FREEZE, ANALYZE) —
/// seconds on test-sized templates, but a big seeded template is legal —
/// so this is generous where the mint wait (60s) is tight; sealing happens
/// once per schema change, never per test.
const SEAL_WAIT_TIMEOUT_NS: u64 = 300 * 1_000_000_000;

/// Vacuum-worker report deadline (registry SealWork::TimedOut): inside the
/// caller's wait budget so the waiter gets the janitor's saved timeout
/// error rather than its own coarser one.
const SEAL_VACUUM_DEADLINE_NS: u64 = 240 * 1_000_000_000;

/// Waiter tick + wait tag: the wait_for_mint conventions (advisory latch,
/// re-poll on timeout, CHECK_FOR_INTERRUPTS every wake).
const WAIT_TICK_MS: i64 = 250;
const PG_WAIT_EXTENSION: u32 = 0x0700_0000;

// ---------------------------------------------------------------------------
// Backend side.
// ---------------------------------------------------------------------------

/// Post the seal request and park until the janitor resolves it. `datname`
/// is the RESOLVED catalog name (builtins.rs resolves + permission-checks
/// first, the pin rationale). Errors are ERROR-level (they abort the
/// caller's statement, not its connection — unlike the mint path's FATALs,
/// which kill a connection attempt by design), with the mint taxonomy's
/// sqlstates: 57P03 for the janitor's own operational states, the janitor's
/// saved error otherwise.
pub(crate) fn request_seal(datname: &str) -> PgResult<()> {
    let my_procno: ProcNumber =
        lmgr_proc::MyProc().expect("pgrust_seal_template before InitProcess");
    match registry::post_seal(datname, my_procno) {
        PostSeal::JanitorAbsent => Err(ereport(ERROR)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg(format!(
                "cannot seal database \"{datname}\": the pgrust ephemeral-db janitor is not \
                 running"
            ))
            .errhint(
                "The janitor may still be starting up, or it has been disabled after an \
                 unrecoverable error (see the server log); retrying shortly is safe."
                    .to_string(),
            )
            .into_error()
            .into()),
        PostSeal::TableFull => Err(ereport(ERROR)
            .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
            .errmsg(format!(
                "cannot seal database \"{datname}\": the seal request table is full ({} entries)",
                registry::MAX_SEALS
            ))
            .errhint("Wait for an in-flight pgrust_seal_template() call to finish.".to_string())
            .into_error()
            .into()),
        PostSeal::Posted(gen) | PostSeal::Joined(gen) => {
            registry::wake_janitor();
            wait_for_seal(datname, gen, my_procno)
        }
    }
}

/// Park on our own latch until the seal resolves (the wait_for_mint
/// choreography: drop-guard deregistration on every exit path, spurious-
/// tolerant wakes, belt-and-suspenders janitor liveness checks, deadline).
fn wait_for_seal(datname: &str, gen: u64, procno: ProcNumber) -> PgResult<()> {
    struct WaiterGuard {
        gen: u64,
        procno: ProcNumber,
    }
    impl Drop for WaiterGuard {
        fn drop(&mut self) {
            registry::remove_seal_waiter(self.gen, self.procno);
        }
    }
    let _guard = WaiterGuard { gen, procno };

    let deadline = pg_clock::mono_ns() + SEAL_WAIT_TIMEOUT_NS;
    loop {
        match registry::seal_status(gen) {
            SealStatus::Done => return Ok(()),
            SealStatus::Failed(e) => {
                // The janitor's saved errdata, re-raised at ERROR on the
                // caller (same sqlstate/message on every joined waiter).
                let mut b = ereport(ERROR).errcode(e.sqlstate()).errmsg(format!(
                    "sealing database \"{datname}\" failed: {}",
                    e.message()
                ));
                if let Some(d) = e.detail() {
                    b = b.errdetail(d.to_string());
                }
                if let Some(h) = e.hint() {
                    b = b.errhint(h.to_string());
                }
                return Err(b.into_error().into());
            }
            SealStatus::Gone => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg(format!(
                        "seal request for database \"{datname}\" was retired before completion"
                    ))
                    .into_error()
                    .into())
            }
            SealStatus::InProgress => {}
        }

        if !registry::janitor_present() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                .errmsg(format!(
                    "cannot seal database \"{datname}\": the pgrust ephemeral-db janitor went \
                     away while the request was pending"
                ))
                .into_error()
                .into());
        }

        if pg_clock::mono_ns() >= deadline {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                .errmsg(format!(
                    "timed out waiting for database \"{datname}\" to be sealed ({}s)",
                    SEAL_WAIT_TIMEOUT_NS / 1_000_000_000
                ))
                .errhint(
                    "The seal runs a whole-database VACUUM (FREEZE, ANALYZE); check the \
                     server log for contained janitor errors."
                        .to_string(),
                )
                .into_error()
                .into());
        }

        let rc = latch::WaitLatch(
            init_small::globals::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            WAIT_TICK_MS,
            PG_WAIT_EXTENSION,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = init_small::globals::MyLatch() {
                latch::ResetLatch(l);
            }
        }
        postgres_seams::check_for_interrupts::call()?;
    }
}

// ---------------------------------------------------------------------------
// Janitor side.
// ---------------------------------------------------------------------------

/// One seal service pass, a janitor tick step (main_loop places it after
/// mint servicing: a template sealed this tick is then visible to the same
/// tick's replenish probe). Per-entry errors are contained via the
/// service_serial choreography — the SAVED error fans out to the entry's
/// waiters and the loop survives; FATAL-class errors propagate.
pub(crate) fn seal_pass() -> PgResult<()> {
    for w in registry::seal_work(pg_clock::mono_ns()) {
        match w {
            SealWork::Validate { gen, name } => {
                if let Err(e) = validate_and_launch(gen, &name) {
                    let saved: Box<PgError> = Box::new((*e).clone());
                    crate::main_loop::contain(e, &format!("sealing database \"{name}\""))?;
                    let waiters = registry::complete_seal(gen, Err(saved));
                    crate::mint::wake_waiters(&waiters);
                }
            }
            SealWork::Flip { gen, name, oid } => match flip_one(&name, oid) {
                Ok(()) => {
                    let waiters = registry::complete_seal(gen, Ok(()));
                    let _ = log_report(
                        LOG,
                        format!(
                            "pgrust ephemeral-db janitor: sealed template \"{name}\" \
                             (VACUUM FREEZE + ANALYZE done; IS_TEMPLATE true \
                             ALLOW_CONNECTIONS false; {} waiter(s))",
                            waiters.len()
                        ),
                    );
                    crate::mint::wake_waiters(&waiters);
                }
                Err(e) => {
                    let saved: Box<PgError> = Box::new((*e).clone());
                    crate::main_loop::contain(e, &format!("sealing database \"{name}\""))?;
                    let waiters = registry::complete_seal(gen, Err(saved));
                    crate::mint::wake_waiters(&waiters);
                }
            },
            SealWork::VacuumFailed { gen, name, err } => {
                crate::mint::report_contained_refusal(&err, &name);
                let waiters = registry::complete_seal(gen, Err(err));
                crate::mint::wake_waiters(&waiters);
            }
            SealWork::TimedOut { gen, name } => {
                let e: Box<PgError> = ereport(ERROR)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg(format!(
                        "the seal vacuum worker for database \"{name}\" did not report within \
                         {}s",
                        SEAL_VACUUM_DEADLINE_NS / 1_000_000_000
                    ))
                    .into_error()
                    .into();
                crate::mint::report_contained_refusal(&e, &name);
                let waiters = registry::complete_seal(gen, Err(e));
                crate::mint::wake_waiters(&waiters);
            }
        }
    }
    Ok(())
}

/// The two seal-specific refusal shapes (shared by the janitor validation
/// and — message-wise — the builtin's fast-fail check).
pub(crate) fn seal_target_missing_error(name: &str) -> Box<PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_DATABASE)
        .errmsg(format!("database \"{name}\" does not exist"))
        .into_error()
        .into()
}

pub(crate) fn already_template_error(name: &str) -> Box<PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!("database \"{name}\" is already a template"))
        .errhint(
            "pgrust_seal_template() seals a WRITABLE database; a sealed template needs no \
             re-seal."
                .to_string(),
        )
        .into_error()
        .into()
}

/// Pending entry: validate the target under a private transaction, then
/// launch the one-shot vacuum worker. On Err the transaction is left for
/// the caller's contain() to abort (the mint_one convention).
fn validate_and_launch(gen: u64, name: &str) -> PgResult<()> {
    let cx = mcx::MemoryContext::new("pgrust janitor seal validate");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();
    let Some(db) = pg_database::get_database_tuple_by_name(mcx, name)? else {
        return Err(seal_target_missing_error(name));
    };
    // Observation-site discipline (the preflight/mint_one/handout clear
    // sites): this probe OBSERVES the target in a non-template state —
    // for a former template unsealed manually and now being re-sealed,
    // any flush mark/relcount surviving under this oid predates a
    // writable window and must not outlive the observation.
    registry::clear_template_flushed(db.oid);
    if db.datistemplate {
        return Err(already_template_error(name));
    }
    let oid = db.oid;
    xact::CommitTransactionCommand()?;

    // Stage Vacuuming BEFORE registering the worker: the worker resolves
    // its target by gen (`seal_vacuum_target` answers only for Vacuuming
    // entries), and it may run before this thread returns from the
    // registration call.
    let deadline = pg_clock::mono_ns() + SEAL_VACUUM_DEADLINE_NS;
    if !registry::begin_seal_vacuum(gen, oid, deadline) {
        // Retired under us (waiter died and GC ran? — cannot for
        // non-terminal states, so this is defensive): nothing to drive.
        return Ok(());
    }
    let bgw = bgworker::BackgroundWorker {
        bgw_name: format!("pgrust template seal: {name}"),
        bgw_type: "pgrust template seal".to_string(),
        bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS
            | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
        bgw_start_time: bgworker::BgWorkerStartTime::RecoveryFinished,
        bgw_restart_time: bgworker::BGW_NEVER_RESTART,
        bgw_main: seal_vacuum_main,
        bgw_main_arg: gen,
        bgw_extra: [0; bgworker::BGW_EXTRALEN],
        bgw_notify_pid: 0,
    };
    match bgworker::RegisterDynamicBackgroundWorker(bgw) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => {
            // No free worker slot (transient): back to Pending, retried
            // next tick (the prewarm requeue convention).
            registry::requeue_seal(gen);
            Ok(())
        }
        Err(e) => {
            registry::requeue_seal(gen);
            Err(e)
        }
    }
}

/// VacuumDone(Ok) entry: flip IS_TEMPLATE true ALLOW_CONNECTIONS false
/// through the public AlterDatabase entry, in the janitor's home session.
fn flip_one(name: &str, oid: Oid) -> PgResult<()> {
    let cx = mcx::MemoryContext::new("pgrust janitor seal flip");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();
    // Identity re-check under the flip transaction: the target can be
    // dropped — or dropped and re-created under a new oid, earning none of
    // this seal's vacuum work — while the vacuum ran.
    let Some(db) = pg_database::get_database_tuple_by_name(mcx, name)? else {
        return Err(seal_target_missing_error(name));
    };
    if db.oid != oid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!(
                "database \"{name}\" was dropped and re-created while being sealed"
            ))
            .errhint("Call pgrust_seal_template() again.".to_string())
            .into_error()
            .into());
    }
    let stmt = build_alterdb_seal_stmt(mcx, name)?;
    // is_top_level=false is safe: the only PreventInTransactionBlock arm in
    // AlterDatabase is SET TABLESPACE's (the handout_one precedent). A
    // target that was concurrently sealed by hand just re-applies the same
    // flags (idempotent).
    dbcommands::AlterDatabase(mcx, &stmt, false)?;
    xact::CommitTransactionCommand()?;

    // Seal bookkeeping, indistinguishable from a manual seal: a freshly
    // sealed template carries NO flush mark and NO cached relcount (its
    // buffers were dirtied moments ago by the vacuum) — the first batch
    // mint pays the FLUSH_ALL pre-checkpoint once and marks it, exactly as
    // after a manual ALTER. The pg_database update is shared-catalog churn
    // (maint.rs accounting).
    registry::clear_template_flushed(oid);
    registry::note_catalog_churn(1);
    Ok(())
}

/// Programmatic `ALTER DATABASE <name> WITH IS_TEMPLATE true
/// ALLOW_CONNECTIONS false` (the build_alterdb_allowconn_stmt shape).
fn build_alterdb_seal_stmt<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<AlterDatabaseStmt<'mcx>> {
    let mut options = NodeList::make1(mcx, crate::mint::def(mcx, "is_template", "true")?)?;
    options.lappend(mcx, crate::mint::def(mcx, "allow_connections", "false")?)?;
    Ok(AlterDatabaseStmt {
        dbname: Some(crate::mint::str_in(mcx, name)?),
        options,
    })
}

// ---------------------------------------------------------------------------
// The one-shot vacuum worker.
// ---------------------------------------------------------------------------

/// The vacuum worker body (bgw_main; arg = the seal entry's gen): resolve
/// the target oid from the registry, connect by oid, run the whole-database
/// VACUUM (FREEZE, ANALYZE), report the outcome. The report guard runs on
/// EVERY exit path — a FATAL unwind out of the connect must not strand the
/// entry until its deadline — and wakes the janitor so the flip does not
/// wait out a full tick.
pub(crate) fn seal_vacuum_main(main_arg: u64) -> PgResult<()> {
    let gen = main_arg;
    struct Report {
        gen: u64,
        done: bool,
    }
    impl Drop for Report {
        fn drop(&mut self) {
            if !self.done {
                // Unwind path: the specific error already went to this
                // worker's log through the bgworker harness; the entry
                // gets the generic cause. finish_seal_vacuum is a no-op
                // unless the entry is still Vacuuming.
                registry::finish_seal_vacuum(
                    self.gen,
                    Err(Box::new(
                        PgError::error(
                            "the seal vacuum worker exited before completing".to_string(),
                        )
                        .with_sqlstate(ERRCODE_CANNOT_CONNECT_NOW),
                    )),
                );
            }
            registry::wake_janitor();
        }
    }
    let mut guard = Report { gen, done: false };

    bgworker::BackgroundWorkerUnblockSignals();
    let Some(oid) = registry::seal_vacuum_target(gen) else {
        // Retired/timed out before we started: nothing to do (the guard's
        // report no-ops the same way).
        guard.done = true;
        return Ok(());
    };
    let t0 = pg_clock::mono_ns();
    let r = connect_and_freeze(oid);
    guard.done = true;
    let outcome = match &r {
        Ok(()) => Ok(()),
        Err(e) => Err(Box::new((**e).clone())),
    };
    registry::finish_seal_vacuum(gen, outcome);
    if r.is_ok() {
        let ms = pg_clock::mono_ns().saturating_sub(t0) as f64 / 1e6;
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: seal vacuum (FREEZE, ANALYZE) of database oid \
                 {oid} completed in {ms:.1} ms"
            ),
        );
    }
    // The guard's Drop wakes the janitor on both arms.
    r
}

/// Connect to the target and run VACUUM (FREEZE, ANALYZE) over every
/// vacuumable relation (nil relation list = get_all_vacuum_rels, the
/// interactive whole-database shape). WITHOUT SKIP_DATABASE_STATS —
/// vac_update_datfrozenxid must run so the seal parks the anti-wraparound
/// trigger ~2^31 xids out (the flush-mark skip's freeze-at-seal premise).
fn connect_and_freeze(oid: Oid) -> PgResult<()> {
    // BYPASS_ALLOWCONN: sealing is legal on a database whose owner already
    // set ALLOW_CONNECTIONS false by hand.
    bgworker::BackgroundWorkerInitializeConnectionByOid(
        oid,
        InvalidOid,
        bgworker::BGWORKER_BYPASS_ALLOWCONN,
    )?;
    let cx = mcx::MemoryContext::new("pgrust template seal vacuum");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();
    let rels = NodeList::nil();
    let params = VacuumParams {
        options: VACOPT_VACUUM | VACOPT_ANALYZE | VACOPT_PROCESS_MAIN | VACOPT_PROCESS_TOAST,
        // All four zero = the FREEZE spelling (vac_cmd's own expansion).
        freeze_min_age: 0,
        freeze_table_age: 0,
        multixact_freeze_min_age: 0,
        multixact_freeze_table_age: 0,
        is_wraparound: false,
        log_min_duration: -1,
        index_cleanup: VacOptValue::Unspecified,
        truncate: VacOptValue::Unspecified,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: guc_tables::vars::vacuum_max_eager_freeze_failure_rate
            .read(),
        nworkers: -1,
    };
    // The maint.rs calling convention: vacuum() commits the entry
    // transaction itself, runs each relation in its own transaction, and
    // leaves a fresh transaction open for this commit.
    commands_vacuum::vacuum(mcx, &rels, &params, None, true)?;
    xact::CommitTransactionCommand()?;
    Ok(())
}
