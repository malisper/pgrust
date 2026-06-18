//! Port of the `backend-utils-init-small` unit:
//! `src/backend/utils/init/globals.c` (the backend-global variable
//! declarations) and `src/backend/utils/init/usercontext.c` (run code as a
//! different database user).

#![allow(non_snake_case)]

pub mod globals;
pub mod usercontext;

pub use usercontext::{RestoreUserContext, SwitchToUntrustedUser};

/// `MyCancelKey[..MyCancelKeyLength]` (globals.c): copy the backend's cancel
/// key bytes into `mcx`. `Err` carries OOM from the copy. Mirrors the C use in
/// `ProcSignalInit(MyProcNumber, &MyCancelKey, MyCancelKeyLength)`.
pub fn my_cancel_key<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> types_error::PgResult<mcx::PgVec<'mcx, u8>> {
    let key = globals::MyCancelKey();
    let len = globals::MyCancelKeyLength() as usize;
    mcx::slice_in(mcx, &key[..len])
}

/// `InitProcessGlobals()` (`postmaster/postmaster.c:1932`) -- set
/// `MyStartTime[stamp]` and the per-process random seed. Called early in the
/// postmaster and every backend.
///
/// The real owner (postmaster.c) is not yet ported as a crate; the seam is
/// homed here (next to the backend-global setters it touches), so the body
/// lives here.
pub fn InitProcessGlobals() -> types_error::PgResult<()> {
    let my_start_timestamp = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
    globals::SetMyStartTimestamp(my_start_timestamp);
    globals::SetMyStartTime(
        backend_utils_adt_timestamp_seams::timestamptz_to_time_t::call(my_start_timestamp),
    );

    // Set a different global seed in every process. C: prefer high-quality OS
    // random bits (`pg_prng_strong_seed`); on failure, fall back to a seed
    // derived from the PID and start timestamp. The CSPRNG (`pg_strong_random`)
    // owner may not be ported yet; when its seam is uninstalled we go straight
    // to C's documented fallback rather than panicking.
    let strong_ok = if port_pg_strong_random_seams::pg_strong_random::is_installed() {
        let mut bytes = [0u8; 16];
        if port_pg_strong_random_seams::pg_strong_random::call(&mut bytes) {
            let s0 = u64::from_ne_bytes(bytes[0..8].try_into().unwrap());
            let s1 = u64::from_ne_bytes(bytes[8..16].try_into().unwrap());
            pg_prng::global_prng(|state| {
                *state = pg_prng::PgPrng::from_raw(s0, s1);
                // pg_prng_seed_check: replace an all-zero state with a fallback.
                state.ensure_seeded()
            })
        } else {
            false
        }
    } else {
        false
    };

    if !strong_ok {
        // Since PIDs and timestamps tend to change more in their least
        // significant bits, shift the timestamp left to allow a larger total
        // number of seeds in a given time period, and also mix in higher bits.
        let pid = globals::MyProcPid() as u64;
        let ts = my_start_timestamp as u64;
        let rseed = pid ^ (ts << 12) ^ (ts >> 20);
        pg_prng::global_prng(|state| state.seed(rseed));
    }

    Ok(())
}

/// Install this unit's seams (`backend-utils-init-small-seams`).
pub fn init_seams() {
    backend_utils_init_small_seams::init_process_globals::set(InitProcessGlobals);
    backend_utils_init_small_seams::work_mem::set(globals::work_mem);
    backend_utils_init_small_seams::max_worker_processes::set(globals::max_worker_processes);
    backend_utils_init_small_seams::max_parallel_workers::set(globals::max_parallel_workers);
    backend_utils_init_small_seams::fast_path_lock_groups_per_backend::set(
        globals::fast_path_lock_groups_per_backend,
    );
    backend_utils_init_small_seams::set_fast_path_lock_groups_per_backend::set(
        globals::set_fast_path_lock_groups_per_backend,
    );
    backend_utils_init_small_seams::my_proc_pid::set(globals::MyProcPid);
    backend_utils_init_small_seams::my_proc_number::set(globals::MyProcNumber);
    backend_utils_init_small_seams::set_my_proc_number::set(globals::SetMyProcNumber);
    backend_utils_init_small_seams::my_cancel_key::set(my_cancel_key);
    backend_utils_init_small_seams::is_under_postmaster::set(globals::IsUnderPostmaster);
    backend_utils_init_small_seams::postmaster_pid::set(globals::PostmasterPid);
    backend_utils_init_small_seams::my_pm_child_slot::set(globals::MyPMChildSlot);
    backend_utils_init_small_seams::max_backends::set(globals::MaxBackends);
    backend_utils_init_small_seams::with_my_proc_port::set(globals::with_my_proc_port_seam);
    backend_utils_init_small_seams::set_my_proc_port::set(|port| {
        globals::SetMyProcPort(Some(Box::new(port)))
    });
    backend_utils_init_small_seams::set_client_connection_lost::set(globals::SetClientConnectionLost);
    backend_utils_init_small_seams::set_interrupt_pending::set(globals::SetInterruptPending);
    backend_utils_init_small_seams::interrupt_pending::set(globals::InterruptPending);
    backend_utils_init_small_seams::set_proc_die_pending::set(globals::SetProcDiePending);
    backend_utils_init_small_seams::proc_die_pending::set(globals::ProcDiePending);
    backend_utils_init_small_seams::set_query_cancel_pending::set(globals::SetQueryCancelPending);
    backend_utils_init_small_seams::query_cancel_pending::set(globals::QueryCancelPending);
    backend_utils_init_small_seams::set_interrupt_holdoff_count::set(globals::SetInterruptHoldoffCount);
    backend_utils_init_small_seams::interrupt_holdoff_count::set(globals::InterruptHoldoffCount);
    backend_utils_init_small_seams::hold_interrupts::set(globals::HoldInterrupts);
    backend_utils_init_small_seams::resume_interrupts::set(globals::ResumeInterrupts);
    backend_utils_init_small_seams::set_my_backend_type::set(globals::SetMyBackendType);
    backend_utils_init_small_seams::nbuffers::set(globals::NBuffers);
    backend_utils_init_small_seams::nloc_buffer::set(globals::NLocBuffer);
    backend_utils_init_small_seams::my_database_id::set(globals::MyDatabaseId);
    // collationcmds.c re-declares `MyDatabaseId` in its own seam crate.
    backend_commands_collationcmds_seams::my_database_id::set(globals::MyDatabaseId);
    backend_utils_init_small_seams::my_database_table_space::set(globals::MyDatabaseTableSpace);
    backend_utils_init_small_seams::my_start_timestamp::set(globals::MyStartTimestamp);
    backend_utils_init_small_seams::is_postmaster_environment::set(globals::IsPostmasterEnvironment);
    backend_utils_init_small_seams::set_my_pm_child_slot::set(globals::SetMyPMChildSlot);
    backend_utils_init_small_seams::set_my_client_socket::set(|cs| globals::SetMyClientSocket(Some(cs)));
    backend_utils_init_small_seams::my_client_socket::set(globals::MyClientSocket);
    backend_utils_init_small_seams::start_critical_section::set(globals::StartCriticalSection);
    backend_utils_init_small_seams::end_critical_section::set(globals::EndCriticalSection);
    backend_utils_init_small_seams::exit_on_any_error::set(globals::ExitOnAnyError);
    backend_utils_init_small_seams::set_exit_on_any_error::set(globals::SetExitOnAnyError);
    backend_utils_init_small_seams::my_backend_type::set(globals::MyBackendType);
    // Pure-wiring installs (assemble/seam-wiring-guard): owner globals match.
    backend_utils_init_small_seams::max_connections::set(globals::MaxConnections);
    backend_utils_init_small_seams::set_max_backends::set(globals::SetMaxBackends);
    backend_utils_init_small_seams::is_binary_upgrade::set(globals::IsBinaryUpgrade);
    // The postmaster's LaunchMissingBackgroundProcesses gates the autovacuum
    // launcher on `IsBinaryUpgrade`; it reads the same global through
    // `backend-postmaster-postmaster-seams`.
    backend_postmaster_postmaster_seams::is_binary_upgrade::set(globals::IsBinaryUpgrade);
    // Catalog-creation (pg_type / pg_enum OID preselection), tablecmds TRUNCATE,
    // and the tablespace ambient-globals bundle read the same flag.
    backend_catalog_binary_upgrade_seams::is_binary_upgrade::set(globals::IsBinaryUpgrade);
    backend_commands_tablecmds_seams::is_binary_upgrade::set(|| Ok(globals::IsBinaryUpgrade()));
    backend_commands_tablespace_globals_seams::IsBinaryUpgrade::set(|| {
        Ok(globals::IsBinaryUpgrade())
    });
    // The tablespace ambient-globals bundle reads the same per-backend session
    // globals init-small owns; forward them to the real globals.
    backend_commands_tablespace_globals_seams::MyDatabaseId::set(|| Ok(globals::MyDatabaseId()));
    backend_commands_tablespace_globals_seams::MyDatabaseTableSpace::set(|| {
        Ok(globals::MyDatabaseTableSpace())
    });
    backend_utils_init_small_seams::set_my_database_id::set(globals::SetMyDatabaseId);
    backend_utils_init_small_seams::set_my_database_table_space::set(globals::SetMyDatabaseTableSpace);
    backend_utils_init_small_seams::set_my_database_has_login_event_triggers::set(
        globals::SetMyDatabaseHasLoginEventTriggers,
    );
    backend_utils_init_small_seams::has_my_proc_port::set(globals::MyProcPortIsSet);
    // Contract-reconcile (init-small owns the real impl): GUC-backed integer
    // globals + per-connection `Port` field copies consumed by postinit and
    // bgworker.
    backend_utils_init_small_seams::post_auth_delay::set(globals::post_auth_delay);
    backend_utils_init_small_seams::reserved_connections::set(globals::reserved_connections);
    backend_utils_init_small_seams::superuser_reserved_connections::set(
        globals::superuser_reserved_connections,
    );
    backend_utils_init_small_seams::my_proc_port_user_name::set(globals::my_proc_port_user_name);
    backend_utils_init_small_seams::my_proc_port_database_name::set(
        globals::my_proc_port_database_name,
    );
    backend_utils_init_small_seams::my_proc_port_application_name::set(
        globals::my_proc_port_application_name,
    );
    backend_utils_init_small_seams::my_proc_port_cmdline_options::set(
        globals::my_proc_port_cmdline_options,
    );
    backend_utils_init_small_seams::my_proc_port_guc_options::set(
        globals::my_proc_port_guc_options,
    );
    backend_utils_init_small_seams::data_dir::set(globals::DataDir);
    // GUC-backed integer globals whose C definitions live in their owning units
    // (`max_prepared_xacts` in twophase.c, `autovacuum_worker_slots` in
    // autovacuum.c); the single store is the GUC-table slot, read here.
    backend_utils_init_small_seams::max_prepared_xacts::set(globals::max_prepared_xacts);
    backend_utils_init_small_seams::autovacuum_worker_slots::set(
        globals::autovacuum_worker_slots,
    );
    // `DatabasePath` read, reconciled to the seam's `PgResult<String>` contract.
    backend_utils_init_small_seams::database_path::set(globals::database_path_seam);

    // `PgStartTime = GetCurrentTimestamp();` — the `globals.c` global lives here,
    // so the standalone-boot seam declared in `backend-tcop-postgres-seams` is
    // installed by its owner (globals.c) pointing at the real setter.
    backend_tcop_postgres_seams::set_pg_start_time::set(globals::SetPgStartTime);

    // GUC variable accessors (`conf->variable`) for the globals.c integers backed
    // by GUC settings. The GUC machinery seeds these from boot_val during
    // InitializeGUCOptions and the shmem-sizing path reads them.
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
    vars::NBuffers.install(GucVarAccessors {
        get: globals::NBuffers,
        set: globals::SetNBuffers,
    });
    // SLRU buffer-size GUCs whose `conf->variable` backing storage lives in
    // globals.c (mirrored in `globals`); read at shmem-sizing time by
    // CLOGShmemSize/MultiXactShmemSize/AsyncShmemSize.
    vars::transaction_buffers.install(GucVarAccessors {
        get: globals::transaction_buffers,
        set: globals::set_transaction_buffers,
    });
    vars::multixact_member_buffers.install(GucVarAccessors {
        get: globals::multixact_member_buffers,
        set: globals::set_multixact_member_buffers,
    });
    vars::multixact_offset_buffers.install(GucVarAccessors {
        get: globals::multixact_offset_buffers,
        set: globals::set_multixact_offset_buffers,
    });
    vars::notify_buffers.install(GucVarAccessors {
        get: globals::notify_buffers,
        set: globals::set_notify_buffers,
    });
    // Remaining `guc_tables.c` integer/real/enum GUCs whose `conf->variable`
    // backing storage lives in globals.c (mirrored in `globals`). Each entry
    // reads/writes its own GUC-table slot — none of these are ControlFile
    // fields. `shared_buffers` is intentionally absent: its `conf->variable`
    // is `&NBuffers`, already installed above via `vars::NBuffers`.
    // `max_connections` is likewise absent: it has no `vars::` slot, only its
    // globals.c-seam accessor (installed above).
    // `serializable_buffers` is intentionally absent here: although its C
    // `conf->variable` is the `globals.c` int, the `serializable_buffers` GUC
    // accessor is already installed by the predicate.c owner crate
    // (backend-storage-lmgr-predicate), which reads it at shmem-sizing time
    // (PredicateLockShmemSize). Installing it here too would double-install the
    // same slot. Single owner = predicate.
    // `commit_timestamp_buffers` is intentionally absent here: like
    // `serializable_buffers`, its C `conf->variable` is the `globals.c` int, but
    // the GUC accessor is already installed by its consuming SLRU owner crate
    // (backend-access-transam-commit-ts), which reads it at shmem-sizing time
    // (CommitTsShmemBuffers). Single owner = commit-ts.
    vars::VacuumBufferUsageLimit.install(GucVarAccessors {
        get: globals::VacuumBufferUsageLimit,
        set: globals::SetVacuumBufferUsageLimit,
    });
    vars::VacuumCostPageHit.install(GucVarAccessors {
        get: globals::VacuumCostPageHit,
        set: globals::SetVacuumCostPageHit,
    });
    vars::VacuumCostPageMiss.install(GucVarAccessors {
        get: globals::VacuumCostPageMiss,
        set: globals::SetVacuumCostPageMiss,
    });
    vars::VacuumCostPageDirty.install(GucVarAccessors {
        get: globals::VacuumCostPageDirty,
        set: globals::SetVacuumCostPageDirty,
    });
    vars::VacuumCostLimit.install(GucVarAccessors {
        get: globals::VacuumCostLimit,
        set: globals::SetVacuumCostLimit,
    });
    vars::VacuumCostDelay.install(GucVarAccessors {
        get: globals::VacuumCostDelay,
        set: globals::SetVacuumCostDelay,
    });
    vars::work_mem.install(GucVarAccessors {
        get: globals::work_mem,
        set: globals::set_work_mem,
    });
    vars::maintenance_work_mem.install(GucVarAccessors {
        get: globals::maintenance_work_mem,
        set: globals::set_maintenance_work_mem,
    });
    vars::hash_mem_multiplier.install(GucVarAccessors {
        get: globals::hash_mem_multiplier,
        set: globals::set_hash_mem_multiplier,
    });
    // `int MaxConnections` (globals.c) — the `max_connections` GUC's
    // `conf->variable`. Read by xlog's control-file consistency check
    // (control_funcs.c) at end-of-recovery, among others.
    vars::MaxConnections.install(GucVarAccessors {
        get: globals::MaxConnections,
        set: globals::SetMaxConnections,
    });
    vars::max_worker_processes.install(GucVarAccessors {
        get: globals::max_worker_processes,
        set: globals::set_max_worker_processes,
    });
    vars::max_parallel_workers.install(GucVarAccessors {
        get: globals::max_parallel_workers,
        set: globals::set_max_parallel_workers,
    });
    vars::max_parallel_maintenance_workers.install(GucVarAccessors {
        get: globals::max_parallel_maintenance_workers,
        set: globals::set_max_parallel_maintenance_workers,
    });
    vars::data_directory_mode.install(GucVarAccessors {
        get: globals::data_directory_mode,
        set: globals::set_data_directory_mode,
    });
    // `IntervalStyle` is a `config_enum` whose `conf->variable` is the i32
    // `IntervalStyle` global; the enum slot carries i32 accessors.
    vars::IntervalStyle.install(GucVarAccessors {
        get: globals::IntervalStyle,
        set: globals::SetIntervalStyle,
    });
    // `bool enableFsync` (globals.c) — the `fsync` GUC's `conf->variable`.
    // Read on the WAL-write/fsync path (xlog.c `get_sync_bit`/`issue_xlog_fsync`,
    // also fd.c/bufmgr.c/sync.c); the bgwriter/walwriter/checkpointer children
    // reach it via `XLogFileInit` -> `get_sync_bit` -> `enable_fsync`.
    vars::enableFsync.install(GucVarAccessors {
        get: globals::enableFsync,
        set: globals::set_enableFsync,
    });
    // `bool allowSystemTableMods = false;` (globals.c) — the
    // `allow_system_table_mods` GUC's `conf->variable`. Read by the DDL paths
    // (tablecmds.c, policy.c, rewriteDefine.c, ...) via the ts-globals seam.
    vars::allowSystemTableMods.install(GucVarAccessors {
        get: globals::allowSystemTableMods,
        set: globals::set_allowSystemTableMods,
    });

    // --- lazy-vacuum driver process-global reads (vacuumlazy.c). MyDatabaseId
    //     and MyBackendType are globals.c globals this crate owns;
    //     AmAutoVacuumWorkerProcess() == (MyBackendType == B_AUTOVAC_WORKER).
    //     The seams home in vacuumlazy-seams. ---
    {
        use backend_access_heap_vacuumlazy_seams as vx;
        vx::my_database_id::set(|| Ok(globals::MyDatabaseId()));
        vx::am_autovacuum_worker_process::set(|| {
            Ok(globals::MyBackendType()
                == types_core::init::BackendType::AutovacWorker)
        });
    }
}

#[cfg(test)]
mod tests;
