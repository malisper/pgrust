//! Seam declarations for the `backend-postmaster-postmaster` unit
//! (`src/backend/postmaster/postmaster.c`). The owning unit installs these
//! from its `init_seams()`; until then a call panics loudly.


seam_core::seam!(
    /// `PostmasterMain(argc, argv)` (`postmaster.c`): the postmaster's main
    /// entry, reached from `main()` for the `DISPATCH_POSTMASTER` case. It
    /// never returns — it runs the server until shutdown and then `proc_exit`s.
    pub fn postmaster_main(argv: &[&str]) -> !
);

seam_core::seam!(
    /// `ClosePostmasterPorts(am_syslogger)` (`postmaster.c`): in a child
    /// process, close the postmaster's listen sockets and other
    /// postmaster-only file descriptors.
    pub fn close_postmaster_ports(am_syslogger: bool)
);

seam_core::seam!(
    /// `PostmasterMarkPIDForWorkerNotify(pid)` (`postmaster.c`): scan the
    /// postmaster's active-child list for `pid`, set `bkend->bgworker_notify`,
    /// and return whether the PID was found. Called from the postmaster while
    /// reconciling new worker slots.
    pub fn postmaster_mark_pid_for_worker_notify(pid: i32) -> bool
);

seam_core::seam!(
    /// `kill(pid, SIGUSR1)` from the postmaster to a child it tracks — used by
    /// bgworker.c to wake a worker's `bgw_notify_pid` backend on a state
    /// change. The owner (postmaster.c, which forks the children) performs the
    /// actual `kill`.
    pub fn signal_child_sigusr1(pid: i32)
);

seam_core::seam!(
    /// `kill(pid, SIGTERM)` from the postmaster to a running worker, used to
    /// terminate a worker marked for shutdown.
    pub fn signal_child_sigterm(pid: i32)
);

seam_core::seam!(
    /// `MemoryContextDelete(PostmasterContext); PostmasterContext = NULL`
    /// (`postmaster.c` owns `PostmasterContext`): a freshly-forked child
    /// releases the postmaster's working context after copying its startup
    /// data out of it.
    pub fn delete_postmaster_context()
);

seam_core::seam!(
    /// `PreAuthDelay` (`postmaster.c` GUC) — seconds to sleep before
    /// authentication, a debugging aid. Pure read of backend-local GUC state.
    pub fn pre_auth_delay() -> i32
);

seam_core::seam!(
    /// `AuthenticationTimeout` (`postmaster.c` GUC) — seconds to wait for the
    /// startup packet / authentication exchange. Pure read.
    pub fn authentication_timeout() -> i32
);

seam_core::seam!(
    /// `log_hostname` (`postmaster.c` GUC) — whether to log/resolve the
    /// client's host name (vs. numeric address). Pure read.
    pub fn log_hostname() -> bool
);

// --- backend-utils-init-postinit consumers (postmaster.c) ---

seam_core::seam!(
    /// `ClientAuthInProgress` (postmaster.c global): read the flag.
    pub fn client_auth_in_progress() -> bool
);

seam_core::seam!(
    /// `ClientAuthInProgress = value` (postmaster.c global): set the flag that
    /// limits log-message visibility during authentication.
    pub fn set_client_auth_in_progress(value: bool)
);

// --- backend-storage-ipc-pmsignal consumers (postmaster death watch) ---

seam_core::seam!(
    /// `read(postmaster_alive_fds[POSTMASTER_FD_WATCH], &c, 1)`
    /// (`PostmasterIsAliveInternal`, pmsignal.c). `postmaster_alive_fds` is the
    /// death-watch pipe set up by postmaster.c (which owns those fds), so the
    /// raw non-blocking `read` lives behind this seam. Returns `(rc, errno)`:
    /// `rc < 0` with `errno == EAGAIN/EWOULDBLOCK` means the postmaster is still
    /// alive; `rc == 0` means EOF (postmaster gone); `rc > 0` is unexpected data.
    pub fn read_postmaster_death_watch() -> (isize, i32)
);

seam_core::seam!(
    /// `postmaster_alive_fds[POSTMASTER_FD_WATCH]` (postmaster.c) — the raw
    /// read-end fd of the postmaster death-watch pipe. `AddWaitEventToSet`'s
    /// `WL_POSTMASTER_DEATH` arm registers this fd with epoll/poll (Linux only;
    /// the kqueue path watches `PostmasterPid` via `EVFILT_PROC` instead).
    pub fn postmaster_death_watch_fd() -> i32
);

seam_core::seam!(
    /// Request a signal on parent (postmaster) death:
    /// `prctl(PR_SET_PDEATHSIG, signum)` / `procctl(PROC_PDEATHSIG_CTL, &signum)`
    /// (`PostmasterDeathSignalInit`, pmsignal.c). The platform mechanism is the
    /// OS boundary; the owner performs it. `Err` carries the C
    /// `elog(ERROR, "could not request parent death signal: %m")`.
    pub fn request_parent_death_signal(signum: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MarkPostmasterChildWalSender()` (pmsignal.c) — set the
    /// `PM_CHILD_WALSENDER` slot flag so the postmaster lets this child outlive
    /// bgwriter and kills it last (it must stream remaining WAL at shutdown).
    pub fn mark_postmaster_child_wal_sender()
);

seam_core::seam!(
    /// `SendPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE)` (pmsignal.c) —
    /// nudge the postmaster's state machine after marking ourselves a WAL
    /// sender.
    pub fn send_postmaster_signal_advance_state_machine()
);

// ---------------------------------------------------------------------------
// Caller-side seams fronting unported external dependencies of PostmasterMain
// and its deferred processors.
//
// These follow the established `backend_tcop_postgres_seams::
// local_process_control_file` pattern: the caller (postmaster.c) declares a
// seam fronting a function whose owner unit is not yet ported. They are
// installed by their owner when it lands; until then a call panics loudly,
// which is the sanctioned seam-and-panic (NOT a silent stub).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `load_hba()` (`libpq/hba.c`) — load `pg_hba.conf`. Returns whether the
    /// load succeeded. Owner: `backend-libpq-hba` (unported entry).
    pub fn load_hba() -> bool
);

seam_core::seam!(
    /// `load_ident()` (`libpq/hba.c`) — load `pg_ident.conf`. Returns whether
    /// the load succeeded. Owner: `backend-libpq-hba` (unported entry).
    pub fn load_ident() -> bool
);

seam_core::seam!(
    /// `HbaFileName` (`guc_tables.c`) — the configured `pg_hba.conf` path, for
    /// the FATAL message when `load_hba()` fails.
    pub fn hba_file_name() -> String
);

seam_core::seam!(
    /// `IdentFileName` (`guc_tables.c`) — the configured `pg_ident.conf` path.
    pub fn ident_file_name() -> String
);

seam_core::seam!(
    /// `autovac_init()` (`postmaster/autovacuum.c`) — emit a WARNING if
    /// `autovacuum=off` but the stats collector is also off. No process start.
    pub fn autovac_init()
);

seam_core::seam!(
    /// `RemovePromoteSignalFiles()` (`access/transam/xlog.c`) — remove the
    /// standby-promotion signal files at postmaster startup.
    pub fn remove_promote_signal_files()
);

seam_core::seam!(
    /// `RemoveLogrotateSignalFiles()` (`postmaster/syslogger.c`) — remove the
    /// logrotate signal file.
    pub fn remove_logrotate_signal_files()
);

seam_core::seam!(
    /// `unlink(LOG_METAINFO_DATAFILE)` (`postmaster/syslogger.c`) — remove the
    /// outdated current-log-filenames metafile.
    pub fn remove_log_metainfo_datafile()
);

seam_core::seam!(
    /// `RemovePgTempFiles()` (`storage/file/fd.c`) — remove old temporary files
    /// at startup (and after a crash if configured).
    pub fn remove_pg_temp_files()
);

seam_core::seam!(
    /// `RecheckDataDirLockFile()` (`utils/init/miscinit.c`) — verify that
    /// `postmaster.pid` hasn't been removed/overwritten. Returns whether the
    /// lock file is still valid.
    pub fn recheck_data_dir_lock_file() -> bool
);

seam_core::seam!(
    /// `TouchSocketLockFiles()` (`utils/init/miscinit.c`) — `utime()` the socket
    /// lock files so /tmp cleaners don't remove them.
    pub fn touch_socket_lock_files()
);

seam_core::seam!(
    /// `CheckPromoteSignal()` (`access/transam/xlog.c`) — whether a promote
    /// signal file exists.
    pub fn check_promote_signal() -> bool
);

seam_core::seam!(
    /// `LocalProcessControlFile(reset)` (`access/transam/xlog.c`) — read the
    /// control file into postmaster-local memory (also called on crash restart).
    pub fn local_process_control_file(reset: bool)
);

seam_core::seam!(
    /// `ShmemExit(code)` driver call on crash-restart reinitialization
    /// (`storage/ipc/ipc.c` `shmem_exit`).
    pub fn shmem_exit(code: i32)
);

// ---------------------------------------------------------------------------
// Background-worker registry access (fronting the un-widened bgworker carrier).
//
// postmaster.c iterates `BackgroundWorkerList` (a `dlist_head` owned by
// bgworker.c) and reads/writes per-entry fields. The merged `bgworker` crate
// keeps that list private and exposes only index-keyed mutators
// (ForgetBackgroundWorker/ReportBackgroundWorker{PID,Exit}). The postmaster's
// scheduling logic (maybe_start_bgworkers / DetermineSleepTime / CleanupBackend)
// reaches the list + the `RegisteredBgWorker` fields through these seams; the
// bgworker crate installs them when its carrier is widened (K2). A default
// cluster registers no bgworkers, so the snapshot is empty and these are never
// reached on the SELECT-1 happy path.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// Snapshot the registration indices of `BackgroundWorkerList`
    /// (`dlist_foreach`/`dlist_foreach_modify` order). Each `u32` is an
    /// `rw_index` usable with the index-keyed bgworker mutators and the
    /// `rw_*` accessor seams below.
    pub fn background_worker_list() -> Vec<u32>
);

seam_core::seam!(
    /// `rw->rw_pid` (postmaster.c reads it to skip already-running workers).
    pub fn rw_pid(rw_index: u32) -> i32
);

seam_core::seam!(
    /// `rw->rw_crashed_at`.
    pub fn rw_crashed_at(rw_index: u32) -> i64
);

seam_core::seam!(
    /// `rw->rw_terminate`.
    pub fn rw_terminate(rw_index: u32) -> bool
);

seam_core::seam!(
    /// `rw->rw_worker.bgw_restart_time`.
    pub fn rw_bgw_restart_time(rw_index: u32) -> i32
);

seam_core::seam!(
    /// `rw->rw_worker.bgw_start_time` (the `BgWorkerStartTime` enum as i32).
    pub fn rw_bgw_start_time(rw_index: u32) -> i32
);

seam_core::seam!(
    /// `rw->rw_worker.bgw_notify_pid`.
    pub fn rw_bgw_notify_pid(rw_index: u32) -> i32
);

seam_core::seam!(
    /// `rw->rw_worker.bgw_name` (for the start log line).
    pub fn rw_bgw_name(rw_index: u32) -> String
);

seam_core::seam!(
    /// `rw->rw_worker.bgw_type` (for the CleanupBackend procname).
    pub fn rw_bgw_type(rw_index: u32) -> String
);

seam_core::seam!(
    /// `rw->rw_pid = pid`.
    pub fn rw_set_pid(rw_index: u32, pid: i32)
);

seam_core::seam!(
    /// `rw->rw_crashed_at = ts`.
    pub fn rw_set_crashed_at(rw_index: u32, ts: i64)
);

seam_core::seam!(
    /// `rw->rw_terminate = value`.
    pub fn rw_set_terminate(rw_index: u32, value: bool)
);

// ---------------------------------------------------------------------------
// GUC / policy reads used by the postmaster's launch decisions and ServerLoop.
//
// Each fronts a GUC variable or a tiny policy predicate owned by another unit
// (guc tables, xlog, autovacuum, pgarch, slotsync, aio). They are pure reads;
// the owner installs them when it lands.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `PostPortNumber` (guc) — the TCP/unix port number.
    pub fn post_port_number() -> u16
);

seam_core::seam!(
    /// `ListenAddresses` (guc) — the comma-separated TCP listen address list,
    /// or `None` if unset.
    pub fn listen_addresses() -> Option<String>
);

seam_core::seam!(
    /// `Unix_socket_directories` (guc) — the comma-separated unix-socket
    /// directory list, or `None` if unset.
    pub fn unix_socket_directories() -> Option<String>
);

seam_core::seam!(
    /// `max_connections` (guc) — passed to `ListenServerPort` for the listen
    /// backlog computation.
    pub fn max_connections() -> i32
);

seam_core::seam!(
    /// `Logging_collector` (guc) — whether the syslogger subprocess runs.
    pub fn logging_collector() -> bool
);

seam_core::seam!(
    /// `send_abort_for_kill` (guc) — SIGABRT instead of SIGKILL for recalcitrant
    /// children.
    pub fn send_abort_for_kill() -> bool
);

seam_core::seam!(
    /// `send_abort_for_crash` (guc) — SIGABRT instead of SIGQUIT on crash.
    pub fn send_abort_for_crash() -> bool
);

seam_core::seam!(
    /// `restart_after_crash` (guc).
    pub fn restart_after_crash() -> bool
);

seam_core::seam!(
    /// `remove_temp_files_after_crash` (guc).
    pub fn remove_temp_files_after_crash() -> bool
);

seam_core::seam!(
    /// `EnableSSL` (guc).
    pub fn enable_ssl() -> bool
);

seam_core::seam!(
    /// `IsBinaryUpgrade` (miscinit global).
    pub fn is_binary_upgrade() -> bool
);

seam_core::seam!(
    /// `AutoVacuumingActive()` (autovacuum.c).
    pub fn autovacuuming_active() -> bool
);

seam_core::seam!(
    /// `XLogArchivingActive()` (xlog.c).
    pub fn xlog_archiving_active() -> bool
);

seam_core::seam!(
    /// `XLogArchivingAlways()` (xlog.c).
    pub fn xlog_archiving_always() -> bool
);

seam_core::seam!(
    /// `PgArchCanRestart()` (pgarch.c).
    pub fn pgarch_can_restart() -> bool
);

seam_core::seam!(
    /// `sync_replication_slots` (guc).
    pub fn sync_replication_slots() -> bool
);

seam_core::seam!(
    /// `ValidateSlotSyncParams(elevel)` (slotsync.c).
    pub fn validate_slot_sync_params(elevel: i32) -> bool
);

seam_core::seam!(
    /// `SlotSyncWorkerCanRestart()` (slotsync.c).
    pub fn slot_sync_worker_can_restart() -> bool
);

seam_core::seam!(
    /// `summarize_wal` (guc).
    pub fn summarize_wal() -> bool
);

seam_core::seam!(
    /// `EnableHotStandby` (guc).
    pub fn enable_hot_standby() -> bool
);

seam_core::seam!(
    /// `io_method == IOMETHOD_WORKER` (`pgaio_workers_enabled` predicate).
    pub fn pgaio_workers_enabled() -> bool
);

seam_core::seam!(
    /// `io_workers` (guc) — the configured number of IO worker processes.
    pub fn io_workers() -> i32
);

seam_core::seam!(
    /// `pgstat_get_crashed_backend_activity(pid, buffer, buflen)`
    /// (`utils/activity/backend_status.c`) — the current activity string of a
    /// crashed backend, for the death-log detail. `None` if unavailable.
    pub fn pgstat_get_crashed_backend_activity(pid: i32) -> Option<String>
);

seam_core::seam!(
    /// `pg_strsignal(signum)` (`port/strsignal.c`) — human-readable signal name.
    pub fn pg_strsignal(signum: i32) -> String
);

seam_core::seam!(
    /// `process_config_file(PGC_SIGHUP)` (guc-file) — re-read configuration on
    /// SIGHUP.
    pub fn process_config_file_sighup()
);

seam_core::seam!(
    /// `CheckLogrotateSignal()` (syslogger.c) — whether a logrotate signal file
    /// exists.
    pub fn check_logrotate_signal() -> bool
);

seam_core::seam!(
    /// `BackgroundWorkerStateChange(allow_new_workers)` (bgworker.c).
    pub fn background_worker_state_change(allow_new_workers: bool)
);

seam_core::seam!(
    /// `ForgetUnstartedBackgroundWorkers()` (bgworker.c).
    pub fn forget_unstarted_background_workers()
);

seam_core::seam!(
    /// `ForgetBackgroundWorker(rw_index)` (bgworker.c).
    pub fn forget_background_worker(rw_index: u32)
);

seam_core::seam!(
    /// `ResetBackgroundWorkerCrashTimes()` (bgworker.c).
    pub fn reset_background_worker_crash_times()
);

seam_core::seam!(
    /// `BackgroundWorkerStopNotifications(pid)` (bgworker.c) — cancel pending
    /// worker-state-change notifications targeted at a dying backend.
    pub fn background_worker_stop_notifications(pid: i32)
);

seam_core::seam!(
    /// `ReportBackgroundWorkerPID(rw_index)` (bgworker.c).
    pub fn report_background_worker_pid(rw_index: u32)
);

seam_core::seam!(
    /// `ReportBackgroundWorkerExit(rw_index)` (bgworker.c).
    pub fn report_background_worker_exit(rw_index: u32)
);

seam_core::seam!(
    /// `AutoVacWorkerFailed()` (autovacuum.c) — record that an av worker fork
    /// failed so the launcher backs off.
    pub fn autovac_worker_failed()
);

seam_core::seam!(
    /// `SysLogger_Start(syslogger_slot)` (syslogger.c) — fork the syslogger.
    /// Returns the child pid (0 on failure).
    pub fn syslogger_start(child_slot: i32) -> i32
);

seam_core::seam!(
    /// `assign_syslogger_pmchild()` — allocate the syslogger's PMChild slot.
    /// Returns the assigned `child_slot`, or `None` if none was available.
    pub fn assign_syslogger_slot() -> Option<i32>
);

seam_core::seam!(
    /// `set_reachedConsistency(value)` (xlogrecovery.c) — postmaster-local flag
    /// poke when recovery reaches consistency.
    pub fn set_reached_consistency(value: bool)
);

seam_core::seam!(
    /// `CreateOptsFile(argc, argv, fullprogname)` (postmaster.c, but the file
    /// write is OS-coupled) — record postmaster options to `postmaster.opts`.
    /// Returns success.
    pub fn create_opts_file(argv: Vec<String>) -> bool
);

seam_core::seam!(
    /// `external_pid_file` write at startup, if the GUC is set. No-op if unset.
    pub fn maybe_write_external_pid_file()
);

seam_core::seam!(
    /// `InitProcessLocalLatch()` (miscinit.c) — set up the postmaster's
    /// process-local `MyLatch`.
    pub fn init_process_local_latch()
);

seam_core::seam!(
    /// `pqsignal()` install of the postmaster's full signal-handler set, plus
    /// `pqinitmask`/`sigprocmask` block-then-unblock and
    /// `InitializeWaitEventSupport()`. The handler function pointers are this
    /// crate's `handle_pm_*` functions, registered by the owner's signal
    /// machinery (which marshals the C `pqsignal`). Bundled because the whole
    /// block is the postmaster's one-time signal setup.
    pub fn install_postmaster_signal_handlers()
);

seam_core::seam!(
    /// `whereToSendOutput = DestNone` (elog.c) — stop sending log to stderr once
    /// the postmaster is fully launched.
    pub fn finalize_where_to_send_output()
);

seam_core::seam!(
    /// `MyProcPid` (globals.c) — the postmaster's own pid (for the
    /// data-directory listen lines / opts file).
    pub fn my_proc_pid() -> i32
);
