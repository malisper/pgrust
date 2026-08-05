//! The pgrust ephemeral-database janitor (docs/design/test-views.md, D1).
//!
//! pgrust-only product code — no C counterpart. One background worker,
//! statically registered when `pgrust.ephemeral_db_prefix` is non-empty at
//! postmaster start (the logical-replication-launcher registration
//! precedent), that owns the lifecycle of databases named under the prefix:
//!
//! - **Reap on idle**: a prefix-matching, non-template, unpinned database at
//!   zero backends (procarray ground truth) continuously for
//!   `pgrust.ephemeral_db_grace` is dropped. Reaps are batched per cycle
//!   through `dbcommands::dropdb_skip_checkpoint`, with ONE immediate
//!   checkpoint per cycle that dropped anything (main_loop.rs for the
//!   deferral safety analysis pointer).
//! - **Startup sweep**: the first loop iteration drops every prefix-matching
//!   non-template (and unpinned — pins are restart-lossy, so only pins taken
//!   in the start-to-first-iteration reconnect window exist) database,
//!   UNCONDITIONALLY. THE PREFIX IS THE CONTRACT (ruling 2026-08-05):
//!   arming the janitor hands it the whole matching namespace — there is no
//!   adoption guard, no PAUSED state, and no marker file.
//! - **Pin/unpin/seal**: builtins.rs, reserved-oid `LANGUAGE internal`
//!   builtins whose pg_proc rows every database carries (bootstrap.rs
//!   backfills them — no install step); pin state is process-global memory
//!   only (registry.rs), lost on restart BY DESIGN.
//!
//! Containment contract: the loop catches every `PgResult` error from its
//! own work (drop failures included) and keeps running; an unrecoverable
//! error exits FATAL-cleanly (status 1) with a loud disabled-until-restart
//! log. `BGW_NEVER_RESTART` supplies the no-respawn half. A panic inside a
//! ported crate's critical section still crash-restarts the cluster per the
//! standing panic-fatality ruling — deliberately outside this containment
//! scope. NOTE: a cluster crash-restart also FORGETS the registration
//! (`ResetBackgroundWorkerCrashTimes` drops NEVER_RESTART workers, matching
//! C), so the janitor is gone until a full postmaster restart in that case
//! too — "disabled until restart" includes crash cycles.
//!
//! pgrust-only code discipline: ported C-parity crates are touched only at
//! public entry points (`dropdb`, `CountDBBackends`, catalog scans) plus the
//! sanctioned additive extensions: `dropdb_skip_checkpoint` (batched
//! reaps), `createdb_skip_checkpoints` (batched mints), and the
//! mint-strategy addendum's `createdb_deferred_file_copy` /
//! `log_file_copy_record` / `count_swept_relations` /
//! `forget_walog_database` (parallel batch copies + the per-template
//! wal_log strategy pick).
#![allow(non_snake_case)]

pub mod bootstrap;
pub mod builtins;
mod dbscan;
pub mod grammar;
mod main_loop;
mod maint;
pub mod mint;
mod prewarm;
// The parallel batch-copy helper is LIVE-ONLY: its workers issue raw
// path-based syscalls (std::fs / libc), which are foreign to the
// thread-local SimVfs; under pgrust_sim the batch mints copy serially
// through fd::copydir exactly as before (mint.rs cfg-gates the phases).
#[cfg(not(pgrust_sim))]
mod parcopy;
pub mod pool;
pub mod reap;
pub mod registry;
pub mod seal;

pub use builtins::JANITOR_BUILTINS;

/// The janitor's home database: a full DB-connected session is required for
/// `dropdb` (relcache phase 3), and `postgres` exists on every C-initdb'd
/// datadir this port boots. Must stay outside any sane ephemeral prefix; if
/// it IS inside the prefix, the own-database guard in the reap predicate
/// still protects it. If the database is missing (renamed/dropped by an
/// operator) the connect FATALs and the janitor is disabled until restart —
/// the loud log is the only witness, documented here on purpose.
pub const JANITOR_HOME_DB: &str = "postgres";

/// `pgrust.ephemeral_db_prefix` (PGC_POSTMASTER, default `''` = feature
/// off). Reads the process-global backing cell, set during config load.
pub fn ephemeral_db_prefix() -> String {
    guc_tables::vars::pgrust_ephemeral_db_prefix
        .read()
        .unwrap_or_default()
}

/// `pgrust.ephemeral_db_grace` in seconds (PGC_SIGHUP, default 15). The
/// janitor re-reads this every tick after running the SIGHUP reload idiom.
pub fn ephemeral_db_grace_secs() -> i32 {
    guc_tables::vars::pgrust_ephemeral_db_grace.read()
}

/// `pgrust.ephemeral_db_mint_roles` (PGC_SIGHUP, default `''` = minting
/// disabled). Read at mint time, never cached (spec: SIGHUP-tunable).
pub fn ephemeral_db_mint_roles() -> String {
    guc_tables::vars::pgrust_ephemeral_db_mint_roles
        .read()
        .unwrap_or_default()
}

/// `pgrust.ephemeral_db_max_per_role` (PGC_SIGHUP, default 0 = unlimited).
pub fn ephemeral_db_max_per_role() -> i32 {
    guc_tables::vars::pgrust_ephemeral_db_max_per_role.read()
}

/// `pgrust.ephemeral_db_pool_size` (PGC_SIGHUP, default 0 = warm pool off).
/// The janitor re-reads it every tick after its reload idiom (pool.rs).
pub fn ephemeral_db_pool_size() -> i32 {
    guc_tables::vars::pgrust_ephemeral_db_pool_size.read()
}

/// `pgrust.ephemeral_db_wal_log_threshold` (PGC_SIGHUP, default -1 = never
/// wal_log). Swept-relation count at or above which a mint's CREATE
/// DATABASE picks STRATEGY wal_log (zero checkpoints) over file_copy.
/// Read at every strategy pick (mint.rs), never cached.
pub fn ephemeral_db_wal_log_threshold() -> i32 {
    guc_tables::vars::pgrust_ephemeral_db_wal_log_threshold.read()
}

/// `pgrust.ephemeral_db_prewarm` (PGC_SIGHUP, default true). Whether the
/// janitor launches a one-shot prewarm worker against every database it
/// mints (prewarm.rs). Read at each enqueue/dispatch site, never cached.
pub fn ephemeral_db_prewarm() -> bool {
    guc_tables::vars::pgrust_ephemeral_db_prewarm.read()
}

/// Static bgworker registration (ApplyLauncherRegister precedent): called by
/// PostmasterMain through `janitor_seams::janitor_register`, after config
/// load and strictly before `BackgroundWorkerShmemInit`. Gated on the prefix
/// GUC; `BGW_NEVER_RESTART` because a janitor that failed once must stay
/// down until an operator restarts the server (containment contract).
pub fn JanitorRegister() {
    let prefix = ephemeral_db_prefix();
    if prefix.is_empty() {
        return;
    }
    let max = types_core::fmgr::NAMEDATALEN as usize - 1;
    if prefix.len() > max {
        // No database name can ever match: registering would be a silent
        // no-op janitor. Refuse loudly instead.
        let _ = elog::elog(
            types_error::WARNING,
            format!(
                "pgrust.ephemeral_db_prefix is longer than a database name \
                 ({max} bytes); ephemeral-database janitor not started"
            ),
        );
        return;
    }
    let bgw = bgworker::BackgroundWorker {
        bgw_name: "pgrust ephemeral-db janitor".to_string(),
        bgw_type: "pgrust ephemeral-db janitor".to_string(),
        bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
        // Earliest legal start for a DB-connected worker; also implements
        // spec item 3's "cannot run at literal postmaster start".
        bgw_start_time: bgworker::BgWorkerStartTime::RecoveryFinished,
        bgw_restart_time: bgworker::BGW_NEVER_RESTART,
        bgw_main: main_loop::janitor_bgw_main,
        bgw_main_arg: 0,
        bgw_extra: [0; bgworker::BGW_EXTRALEN],
        bgw_notify_pid: 0,
    };
    bgworker::RegisterBackgroundWorker(&bgw);
}

pub fn init_seams() {
    janitor_seams::janitor_register::set(JanitorRegister);
    // D2 mint-on-connect: the InitPostgres lookup-miss hook (uninstalled =
    // stock behavior; the postinit guard checks is_installed()).
    janitor_seams::ephemeral_db_mint_on_connect::set(mint::mint_on_connect);
    // Bootstrap-builtin backfill: PostgresMain calls through the seam after
    // InitPostgres so the three janitor builtins exist in every database a
    // session can reach — no install step (bootstrap.rs).
    janitor_seams::builtin_backfill::set(bootstrap::backfill_builtin_rows);
    guc_tables::hooks::check_pgrust_ephemeral_db_mint_roles
        .install(mint::check_ephemeral_db_mint_roles);
}
