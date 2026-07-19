//! postinit.c — InitPostgres and the per-backend init sequence. The C call
//! order is preserved call-for-call; landed units are direct deps, unported
//! owners are loud seams, so a boot failure names its missing unit.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use elog::ereport;
use mcx::Mcx;
use types_core::init::BackendType;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_core::xact::XACT_READ_COMMITTED;
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG3, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_TOO_MANY_CONNECTIONS, ERRCODE_UNDEFINED_DATABASE, ERRCODE_UNDEFINED_OBJECT, ERROR,
    FATAL, LOG, WARNING,
};
use types_guc::{GucContext, GucSource};
use types_storage::lock::{AccessShareLock, RowExclusiveLock, USER_LOCKMETHOD};

// wasm32: the wasi libc crate exposes no LC_*/SIG* names. LC values are
// musl's (the numbering the linked wasi-libc and pg_locale's wasm arm use);
// SIG values are the thread-signal emulation's Linux-numbered space.
#[cfg(not(target_family = "wasm"))]
use libc::{LC_COLLATE, LC_CTYPE, SIGINT, SIGTERM};
#[cfg(target_family = "wasm")]
const LC_CTYPE: i32 = 0;
#[cfg(target_family = "wasm")]
const LC_COLLATE: i32 = 3;
#[cfg(target_family = "wasm")]
const SIGINT: i32 = 2;
#[cfg(target_family = "wasm")]
const SIGTERM: i32 = 15;

#[cfg(test)]
mod tests;

pub const INIT_PG_LOAD_SESSION_LIBS: u32 = 0x0001;
pub const INIT_PG_OVERRIDE_ALLOW_CONNS: u32 = 0x0002;
pub const INIT_PG_OVERRIDE_ROLE_LOGIN: u32 = 0x0004;

pub const MAX_BACKENDS: i32 = (1 << 18) - 1;
pub const FP_LOCK_GROUPS_PER_BACKEND_MAX: i32 = 1024;
pub const FP_LOCK_SLOTS_PER_GROUP: i32 = 16;

pub const TEMPLATE1_DB_OID: Oid = 1;
pub const DEFAULTTABLESPACE_OID: Oid = 1663;
pub const DB_ROLE_SETTING_RELATION_ID: Oid = 2964;
pub const ROLE_PG_USE_RESERVED_CONNECTIONS: Oid = 4550;
/// ACL_CONNECT (nodes/parsenodes.h); AclMode is uint64.
pub const ACL_CONNECT: u64 = 1 << 11;
const ACLCHECK_OK: i32 = 0;
/// DATCONNLIMIT_INVALID_DB (catalog/pg_database.h); database_is_invalid_form
/// (dbcommands.c) folds to this comparison — inlined, dbcommands unported.
const DATCONNLIMIT_INVALID_DB: i32 = -2;
const NAMEDATALEN: usize = 64;

const SRC: &str = "src/backend/utils/init/postinit.c";

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

fn am_walsender() -> bool {
    walsender_seams::am_walsender()
}
fn am_db_walsender() -> bool {
    walsender_seams::am_db_walsender()
}

fn GetDatabaseTuple<'mcx>(
    mcx: Mcx<'mcx>,
    dbname: &str,
) -> PgResult<Option<pg_database_seams::PgDatabaseForm<'mcx>>> {
    pg_database_seams::get_database_tuple_by_name::call(mcx, dbname)
}

fn GetDatabaseTupleByOid<'mcx>(
    mcx: Mcx<'mcx>,
    dboid: Oid,
) -> PgResult<Option<pg_database_seams::PgDatabaseForm<'mcx>>> {
    pg_database_seams::get_database_tuple_by_oid::call(mcx, dboid)
}

fn PerformAuthentication() -> PgResult<()> {
    elog::config::set_client_auth_in_progress(true);

    backend_startup::conn_timing::set_auth_start(timestamp_seams::get_current_timestamp::call());

    let auth_timeout = guc_tables::vars::AuthenticationTimeout.read();
    timeout_seams::enable_timeout_after::call(
        timeout_seams::STATEMENT_TIMEOUT,
        auth_timeout * 1000,
    )?;

    ps_status_seams::set_ps_display::call("authentication");
    auth_seams::client_authentication::call()?;

    timeout_seams::disable_timeout::call(timeout_seams::STATEMENT_TIMEOUT, false)?;

    backend_startup::conn_timing::set_auth_end(timestamp_seams::get_current_timestamp::call());

    if backend_startup::log_connections::get() & backend_startup::LOG_CONNECTION_AUTHORIZATION != 0
    {
        let logmsg = build_auth_logmsg();
        ereport(LOG).errmsg_internal(logmsg).finish(loc(309, "PerformAuthentication"))?;
    }

    ps_status_seams::set_ps_display::call("startup");

    elog::config::set_client_auth_in_progress(false);
    Ok(())
}

fn build_auth_logmsg() -> String {
    init_small::globals::WithMyProcPort(|port| {
        let mut s = String::new();
        if am_walsender() {
            s.push_str("replication connection authorized: user=");
        } else {
            s.push_str("connection authorized: user=");
        }
        s.push_str(port.user_name.as_deref().unwrap_or(""));
        if !am_walsender() {
            s.push_str(" database=");
            s.push_str(port.database_name.as_deref().unwrap_or(""));
        }
        if let Some(app) = port.application_name.as_deref() {
            s.push_str(" application_name=");
            s.push_str(app);
        }
        if port.ssl_in_use {
            s.push_str(&format!(
                " SSL enabled (protocol={}, cipher={}, bits={})",
                be_secure::be_tls_get_version().unwrap_or_default(),
                be_secure::be_tls_get_cipher().unwrap_or_default(),
                be_secure::be_tls_get_cipher_bits()
            ));
        }
        // ENABLE_GSS fragment: not this build.
        s
    })
}

fn CheckMyDatabase(
    mcx: Mcx<'_>,
    name: &str,
    am_superuser: bool,
    override_allow_connections: bool,
) -> PgResult<()> {
    let my_database_id = init_small::globals::MyDatabaseId();

    let Some(dbform) = pg_database_seams::search_database_syscache::call(mcx, my_database_id)?
    else {
        return ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for database {my_database_id}"))
            .finish(loc(335, "CheckMyDatabase"));
    };

    if name != dbform.datname.as_str() {
        return ereport(FATAL)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{name}\" has disappeared from pg_database"))
            .errdetail(format!(
                "Database OID {} now seems to belong to \"{}\".",
                my_database_id,
                dbform.datname.as_str()
            ))
            .finish(loc(340, "CheckMyDatabase"));
    }

    if init_small::globals::IsUnderPostmaster() {
        if !dbform.datallowconn && !override_allow_connections {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "database \"{name}\" is not currently accepting connections"
                ))
                .finish(loc(362, "CheckMyDatabase"));
        }

        if !am_superuser
            && !override_allow_connections
            && aclchk_seams::object_aclcheck::call(
                types_core::catalog::DATABASE_RELATION_ID,
                my_database_id,
                miscinit::GetUserId(),
                ACL_CONNECT,
            )? != ACLCHECK_OK
        {
            return ereport(FATAL)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!("permission denied for database \"{name}\""))
                .errdetail("User does not have CONNECT privilege.")
                .finish(loc(375, "CheckMyDatabase"));
        }

        if dbform.datconnlimit >= 0
            && miscinit::GetMyBackendType() == BackendType::Backend
            && !am_superuser
            && procarray_seams::count_db_connections::call(my_database_id)? > dbform.datconnlimit
        {
            return ereport(FATAL)
                .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
                .errmsg(format!("too many connections for database \"{name}\""))
                .finish(loc(396, "CheckMyDatabase"));
        }
    }

    mbutils_seams::set_database_encoding::call(dbform.encoding)?;
    guc::SetConfigOption(
        "server_encoding",
        Some(mbutils_seams::get_database_encoding_name::call()),
        GucContext::PGC_INTERNAL,
        GucSource::PGC_S_DYNAMIC_DEFAULT,
    )?;
    guc::SetConfigOption(
        "client_encoding",
        Some(mbutils_seams::get_database_encoding_name::call()),
        GucContext::PGC_BACKEND,
        GucSource::PGC_S_DYNAMIC_DEFAULT,
    )?;

    let collate = dbform.datcollate.as_str();
    let ctype = dbform.datctype.as_str();

    if pg_locale_seams::pg_perm_setlocale::call(mcx, LC_COLLATE, collate)?.is_none() {
        return ereport(FATAL)
            .errmsg("database locale is incompatible with operating system")
            .errdetail(format!(
                "The database was initialized with LC_COLLATE \"{collate}\",  which is not recognized by setlocale()."
            ))
            .errhint("Recreate the database with another locale or install the missing locale.")
            .finish(loc(421, "CheckMyDatabase"));
    }

    if pg_locale_seams::pg_perm_setlocale::call(mcx, LC_CTYPE, ctype)?.is_none() {
        return ereport(FATAL)
            .errmsg("database locale is incompatible with operating system")
            .errdetail(format!(
                "The database was initialized with LC_CTYPE \"{ctype}\",  which is not recognized by setlocale()."
            ))
            .errhint("Recreate the database with another locale or install the missing locale.")
            .finish(loc(428, "CheckMyDatabase"));
    }

    if ctype == "C" || ctype == "POSIX" {
        pg_locale_seams::set_database_ctype_is_c::call(true);
    }

    pg_locale_seams::init_database_collation::call()?;

    if let Some(collversion) = dbform.datcollversion.as_ref() {
        let collversionstr = collversion.as_str();
        let locale = if dbform.datlocprovider == pg_database_seams::COLLPROVIDER_LIBC {
            collate
        } else {
            match dbform.datlocale.as_ref() {
                Some(l) => l.as_str(),
                None => {
                    return ereport(ERROR)
                        .errmsg_internal("unexpected null datlocale in pg_database tuple")
                        .finish(loc(459, "CheckMyDatabase"));
                }
            }
        };

        match pg_locale_seams::get_collation_actual_version::call(
            mcx,
            dbform.datlocprovider,
            locale,
        )? {
            None => {
                ereport(WARNING)
                    .errmsg_internal(format!(
                        "database \"{name}\" has no actual collation version, but a version was recorded"
                    ))
                    .finish(loc(466, "CheckMyDatabase"))?;
            }
            Some(actual) if actual.as_str() != collversionstr => {
                ereport(WARNING)
                    .errmsg(format!("database \"{name}\" has a collation version mismatch"))
                    .errdetail(format!(
                        "The database was created using collation version {collversionstr}, but the operating system provides version {}.",
                        actual.as_str()
                    ))
                    .errhint(format!(
                        "Rebuild all objects in this database that use the default collation and run ALTER DATABASE {} REFRESH COLLATION VERSION, or build PostgreSQL with the right library version.",
                        quote_identifier(name)
                    ))
                    .finish(loc(470, "CheckMyDatabase"))?;
            }
            Some(_) => {}
        }
    }

    Ok(())
}

// quote_identifier (ruleutils.c) reduced to the quote-when-not-plain rendering
// this WARNING hint needs; the keyword-aware owner supersedes it when
// ruleutils lands.
fn quote_identifier(ident: &str) -> String {
    let plain = !ident.is_empty()
        && ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        ident.to_string()
    } else {
        let mut s = String::with_capacity(ident.len() + 2);
        s.push('"');
        for c in ident.chars() {
            if c == '"' {
                s.push('"');
            }
            s.push(c);
        }
        s.push('"');
        s
    }
}

fn c_isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

pub fn pg_split_opts(argv: &mut Vec<String>, optstr: &str) {
    let bytes = optstr.as_bytes();
    let mut i = 0usize;
    let mut s = String::new();

    while i < bytes.len() {
        let mut last_was_escape = false;
        s.clear();

        while i < bytes.len() && c_isspace(bytes[i]) {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        while i < bytes.len() {
            let c = bytes[i];
            if c_isspace(c) && !last_was_escape {
                break;
            }
            if !last_was_escape && c == b'\\' {
                last_was_escape = true;
            } else {
                last_was_escape = false;
                s.push(c as char);
            }
            i += 1;
        }

        argv.push(s.clone());
    }
}

pub fn InitializeMaxBackends() -> PgResult<()> {
    debug_assert_eq!(init_small::globals::MaxBackends(), 0);

    let max_connections = init_small::globals::MaxConnections();
    let av_worker_slots = guc_tables::vars::autovacuum_worker_slots.read();
    let max_worker_processes = init_small::globals::max_worker_processes();
    let max_wal_senders = guc_tables::vars::max_wal_senders.read();

    // M2 pool-binding: the standing runtime executor gang's boot-reserved
    // PGPROCs (parallel::standing; 0 unless PGRUST_RUNTIME=1 — set by the
    // postmaster before this runs). They widen the bgworker freelist
    // segment in InitProcGlobal without touching the max_worker_processes
    // GUC, so registry/parallel-class budgets are unaffected.
    let runtime_gang = init_small::globals::RuntimeGangProcs();

    let max_backends = max_connections
        + av_worker_slots
        + max_worker_processes
        + max_wal_senders
        + runtime_gang
        + types_storage::storage::NUM_SPECIAL_WORKER_PROCS;
    init_small::globals::SetMaxBackends(max_backends);

    if max_backends > MAX_BACKENDS {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("too many server processes configured")
            .errdetail(format!(
                "\"max_connections\" ({max_connections}) plus \"autovacuum_worker_slots\" ({av_worker_slots}) plus \"max_worker_processes\" ({max_worker_processes}) plus \"max_wal_senders\" ({max_wal_senders}) must be less than {}.",
                MAX_BACKENDS - (types_storage::storage::NUM_SPECIAL_WORKER_PROCS - 1)
            ))
            .finish(loc(564, "InitializeMaxBackends"));
    }
    Ok(())
}

/// InitializeFastPathLocks. C stores a global; here the computed group count
/// is returned and threaded into lmgr_proc::ProcGlobalConfig at sizing time
/// (PROC_HDR.fpLockGroupsPerBackend is its storage — repo rule: parameters,
/// not ambient-global seams).
pub fn InitializeFastPathLocks() -> i32 {
    let max_locks_per_xact = guc_tables::vars::max_locks_per_xact.read();
    let groups = ((max_locks_per_xact as u32).next_power_of_two() as i32
        / FP_LOCK_SLOTS_PER_GROUP)
        .clamp(1, FP_LOCK_GROUPS_PER_BACKEND_MAX);
    debug_assert_eq!(groups, (groups as u32).next_power_of_two() as i32);
    groups
}

pub fn BaseInit() -> PgResult<()> {
    debug_assert!(lmgr_proc::MyProc().is_some());

    if init_small::wretain::warm_claim() {
        return BaseInitRetained();
    }

    elog::DebugFileOpen()?;

    fd::InitFileAccess();

    pgstat_seams::pgstat_initialize::call()?;

    aio_seams::pgaio_init_backend::call();

    sync_seams::init_sync::call()?;
    smgr::smgrinit()?;
    bufmgr::InitBufferManagerAccess();

    fd::InitTemporaryFileAccess()?;

    xloginsert_seams::init_xlog_insert::call()?;

    lock::InitLockManagerAccess();

    slot_seams::replication_slot_initialize::call()?;
    Ok(())
}

/// Retention claim (wretain): BaseInit for a thread whose per-thread state
/// survived a park. Exit callbacks (consumed by the park teardown) are
/// re-armed and per-task baselines reset; once-per-thread constructions
/// (VFD cache, sync/xloginsert scratch creation asserts) are skipped.
fn BaseInitRetained() -> PgResult<()> {
    pgstat_seams::pgstat_reattach_retained_backend::call()?;
    aio_seams::pgaio_init_backend::call();
    fd::ReattachRetainedFileAccess()?;
    slot_seams::replication_slot_initialize::call()?;
    Ok(())
}

/// InitPostgres. The C call order below is the deliverable — do not reorder.
// Launch-path phase timestamp, PGRUST_GATHER_TRACE-gated (duplicated from
// parallel::gtrace — this crate sits below parallel in the dep graph).
fn gtrace(phase: &str) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if !*ON.get_or_init(|| std::env::var_os("PGRUST_GATHER_TRACE").is_some()) {
        return;
    }
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    eprintln!("GTRACE {phase} w=? t_us={t}");
}

pub fn InitPostgres(
    mcx: Mcx<'_>,
    in_dbname: Option<&str>,
    mut dboid: Oid,
    username: Option<&str>,
    useroid: Oid,
    flags: u32,
    out_dbname: Option<&mut String>,
) -> PgResult<()> {
    let bootstrap = miscinit::IsBootstrapProcessingMode();
    let am_superuser: bool;
    let mut dbname = String::new();

    ereport(DEBUG3).errmsg_internal("InitPostgres").finish(loc(723, "InitPostgres"))?;

    // Session-memory teardown (FPBUDGET-1): the fundamentals — transaction
    // contexts, resource-owner arena, Port copy — freed at clean task end.
    // Registered before any catalog access so cache teardowns (registered
    // lazily during boot lookups) drain FIRST in the LIFO order. Once per
    // thread: a wretain standby re-enters InitPostgres per claim but parks
    // without draining, and must not stack duplicates.
    {
        use std::cell::Cell;
        thread_local! {
            static FUNDAMENTALS_REGISTERED: Cell<bool> = const { Cell::new(false) };
        }
        if !FUNDAMENTALS_REGISTERED.replace(true) {
            ::mcx::register_session_cleanup(Box::new(|| {
                xact::session_mem_teardown();
                resowner::session_mem_teardown();
                init_small::globals::SessionMemTeardownPort();
            }));
        }
    }

    gtrace("p.enter");
    lmgr_proc::InitProcessPhase2()?;
    gtrace("p.proc2");

    backend_status_seams::pgstat_beinit::call()?;

    if !bootstrap {
        backend_status_seams::pgstat_bestart_initial::call()?;
        // INJECTION_POINT("init-pre-auth"): compiled out (non-assert build).
    }

    gtrace("p.beinit");
    let warm_claim = init_small::wretain::warm_claim();
    if warm_claim {
        // Retention claim: the sinval slot survived the park. Its exit
        // callback was already re-armed by bgworker::run_worker_body,
        // immediately after ReattachRetainedProc — it MUST never re-arm
        // later than ProcKill's registration, or a task failure between the
        // two exits through a drain that frees the PGPROC while the sinval
        // slot stays claimed ("sinval slot for backend N is already in use
        // by process M" on every later claimant of the procno — the standing
        // chaos flake). DDL committed while parked is drained (not nuked)
        // once the startup transaction exists below.
        debug_assert!(sinval::RetainedSlotIsCurrent());
    } else {
        sinval::SharedInvalBackendInit(false)?;
    }

    // Test-only fault injection (default-off, dead unless the env var is set
    // at server start): PGRUST_TEST_CONNECT_FAIL_AFTER_SINVAL=<n> fails the
    // first n BgWorker connects RIGHT AFTER the sinval slot claim — the
    // half-connected geometry (slot claimed, connect failed) whose survivors
    // used to self-poison standing gang workers ("sinval slot for backend N
    // is already in use by process <own pid>" on every retry). The standing
    // battery (scripts/sinval-slot-e2e.sh) pins the fix: a failed gang
    // connect is now thread-fatal and the exit drain releases the claim.
    test_connect_fail_after_sinval()?;

    let cancel_key = init_small::globals::MyCancelKey();
    let cancel_key_len = init_small::globals::MyCancelKeyLength() as usize;
    procsignal::ProcSignalInit(&cancel_key[..cancel_key_len])?;

    if !bootstrap {
        timeout_seams::register_timeout::call(
            timeout_seams::DEADLOCK_TIMEOUT,
            lmgr_proc::CheckDeadLockAlert,
        );
        timeout_seams::register_timeout::call(
            timeout_seams::STATEMENT_TIMEOUT,
            StatementTimeoutHandler,
        );
        timeout_seams::register_timeout::call(timeout_seams::LOCK_TIMEOUT, LockTimeoutHandler);
        timeout_seams::register_timeout::call(
            timeout_seams::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            IdleInTransactionSessionTimeoutHandler,
        );
        timeout_seams::register_timeout::call(
            timeout_seams::TRANSACTION_TIMEOUT,
            TransactionTimeoutHandler,
        );
        timeout_seams::register_timeout::call(
            timeout_seams::IDLE_SESSION_TIMEOUT,
            IdleSessionTimeoutHandler,
        );
        timeout_seams::register_timeout::call(
            timeout_seams::CLIENT_CONNECTION_CHECK_TIMEOUT,
            ClientCheckTimeoutHandler,
        );
        timeout_seams::register_timeout::call(
            timeout_seams::IDLE_STATS_UPDATE_TIMEOUT,
            IdleStatsUpdateTimeoutHandler,
        );
    }

    if !init_small::globals::IsUnderPostmaster() {
        resowner::CreateAuxProcessResourceOwner()?;

        transam_xlog_seams::startup_xlog::call()?;
        resowner::ReleaseAuxProcessResources(true)?;
        resowner::SetCurrentResourceOwner(types_resowner::ResourceOwner::NULL);

        ipc_seams::before_shmem_exit::call(pgstat_before_server_shutdown_cb, datum_null())?;
        ipc_seams::before_shmem_exit::call(shutdown_xlog_cb, datum_null())?;
    }

    gtrace("p.timeouts");
    if !warm_claim {
        relcache::RelationCacheInitialize();
        cache_syscache::InitCatalogCache()?;
        plancache_portal_seams::init_plan_cache::call()?;
        gtrace("p.catcache");

        portalmem::EnablePortalManager();

        relcache::RelationCacheInitializePhase2()?;
        gtrace("p.relcache2");
    }

    ipc_seams::before_shmem_exit::call(shutdown_postgres_cb, datum_null())?;

    if miscinit::GetMyBackendType() == BackendType::AutovacLauncher {
        backend_status_seams::pgstat_bestart_final::call()?;
        return Ok(());
    }

    if !bootstrap {
        xact::SetCurrentStatementStartTimestamp();
        xact::StartTransactionCommand()?;

        xact::SetXactIsoLevel(XACT_READ_COMMITTED);
    }
    gtrace("p.txn");

    if warm_claim {
        // Barrier work emitted while parked (procsignal slot was released):
        // the only barrier kind is SMGRRELEASE; apply it before touching any
        // retained smgr state.
        if procsignal::SharedBarrierGeneration() != init_small::wretain::parked_barrier_gen() {
            smgr_seams::process_barrier_smgr_release::call()?;
        }
        // Drain the parked slot's accumulated invalidations before the first
        // catalog access below (pg_authid/pg_database reads must observe all
        // DDL committed before this claim). A queue overflow while parked
        // surfaces as resetState -> full InvalidateSystemCaches here.
        inval_seams::accept_invalidation_messages::call()?;
        gtrace("p.retained.drain");
    }

    let backend_type = miscinit::GetMyBackendType();
    if bootstrap
        || backend_type == BackendType::AutovacWorker
        || backend_type == BackendType::SlotsyncWorker
    {
        miscinit::InitializeSessionUserIdStandalone()?;
        am_superuser = true;
    } else if !init_small::globals::IsUnderPostmaster() {
        miscinit::InitializeSessionUserIdStandalone()?;
        am_superuser = true;
        if !ThereIsAtLeastOneRole(mcx)? {
            ereport(WARNING)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg("no roles are defined in this database system")
                .errhint(format!(
                    "You should immediately run CREATE USER \"{}\" SUPERUSER;.",
                    username.unwrap_or("postgres")
                ))
                .finish(loc(876, "InitPostgres"))?;
        }
    } else if backend_type == BackendType::BgWorker {
        if username.is_none() && !OidIsValid(useroid) {
            miscinit::InitializeSessionUserIdStandalone()?;
            am_superuser = true;
        } else {
            miscinit_seams::initialize_session_user_id::call(
                username,
                useroid,
                (flags & INIT_PG_OVERRIDE_ROLE_LOGIN) != 0,
            )?;
            am_superuser = superuser_seams::superuser::call()?;
        }
    } else {
        debug_assert!(init_small::globals::HaveMyProcPort());
        PerformAuthentication()?;
        miscinit_seams::initialize_session_user_id::call(username, useroid, false)?;
        let (authn_id, auth_method) = miscinit::client_connection_info();
        if let Some(authn_id) = authn_id {
            miscinit::InitializeSystemUser(authn_id, hba_seams::hba_authname::call(auth_method));
        }
        am_superuser = superuser_seams::superuser::call()?;
    }

    gtrace("p.auth");
    if init_small::globals::HaveMyProcPort() {
        debug_assert!(!bootstrap);
        backend_status_seams::pgstat_bestart_security::call()?;
    }

    if init_small::globals::IsBinaryUpgrade() && !am_superuser {
        return ereport(FATAL)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to connect in binary upgrade mode")
            .finish(loc(922, "InitPostgres"));
    }

    let su_reserved = guc_tables::vars::SuperuserReservedConnections.read();
    let reserved = guc_tables::vars::ReservedConnections.read();
    if miscinit::GetMyBackendType() == BackendType::Backend
        && !am_superuser
        && (su_reserved + reserved) > 0
    {
        let (have, nfree) = lmgr_proc::HaveNFreeProcs(su_reserved + reserved);
        if !have {
            if nfree < su_reserved {
                return ereport(FATAL)
                    .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
                    .errmsg("remaining connection slots are reserved for roles with the SUPERUSER attribute")
                    .finish(loc(942, "InitPostgres"));
            }
            if !acl_seams::has_privs_of_role::call(
                miscinit::GetUserId(),
                ROLE_PG_USE_RESERVED_CONNECTIONS,
            )? {
                return ereport(FATAL)
                    .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
                    .errmsg("remaining connection slots are reserved for roles with privileges of the \"pg_use_reserved_connections\" role")
                    .finish(loc(948, "InitPostgres"));
            }
        }
    }

    if am_walsender() {
        debug_assert!(!bootstrap);
        if !miscinit_seams::has_rolreplication::call(miscinit::GetUserId())? {
            return ereport(FATAL)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to start WAL sender")
                .errdetail("Only roles with the REPLICATION attribute may start a WAL sender process.")
                .finish(loc(960, "InitPostgres"));
        }
    }

    if am_walsender() && !am_db_walsender() {
        if init_small::globals::HaveMyProcPort() {
            process_startup_options(am_superuser)?;
        }
        apply_post_auth_delay();
        mbutils_seams::initialize_client_encoding::call()?;
        backend_status_seams::pgstat_bestart_final::call()?;
        xact::CommitTransactionCommand()?;
        return Ok(());
    }

    if bootstrap {
        dboid = TEMPLATE1_DB_OID;
        init_small::globals::SetMyDatabaseTableSpace(DEFAULTTABLESPACE_OID);
    } else if let Some(in_dbname) = in_dbname {
        let Some(dbform) = GetDatabaseTuple(mcx, in_dbname)? else {
            return ereport(FATAL)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{in_dbname}\" does not exist"))
                .finish(loc(1014, "InitPostgres"));
        };
        dboid = dbform.oid;
    } else if !OidIsValid(dboid) {
        if !bootstrap {
            backend_status_seams::pgstat_bestart_final::call()?;
            xact::CommitTransactionCommand()?;
        }
        return Ok(());
    }

    // Writer's lock on the database: serializes against DROP DATABASE; held
    // to end of this startup transaction, and we advertise the database in
    // the ProcArray before releasing (CountOtherDBBackends ordering).
    if !bootstrap {
        lmgr::LockSharedObject(
            types_core::catalog::DATABASE_RELATION_ID,
            dboid,
            0,
            RowExclusiveLock,
        )?;
    }

    if !bootstrap {
        let tuple = GetDatabaseTupleByOid(mcx, dboid)?;

        let name_mismatch = match (&tuple, in_dbname) {
            (Some(df), Some(req)) => df.datname.as_str() != req,
            _ => false,
        };
        if tuple.is_none() || name_mismatch {
            return match in_dbname {
                Some(req) => ereport(FATAL)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database \"{req}\" does not exist"))
                    .errdetail("It seems to have just been dropped or renamed.")
                    .finish(loc(1078, "InitPostgres")),
                None => ereport(FATAL)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database {dboid} does not exist"))
                    .finish(loc(1083, "InitPostgres")),
            };
        }

        let datform = tuple.unwrap();
        dbname = strlcpy_name(datform.datname.as_str());

        if datform.datconnlimit == DATCONNLIMIT_INVALID_DB {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("cannot connect to invalid database \"{dbname}\""))
                .errhint("Use DROP DATABASE to drop invalid databases.")
                .finish(loc(1092, "InitPostgres"));
        }

        init_small::globals::SetMyDatabaseTableSpace(datform.dattablespace);
        init_small::globals::SetMyDatabaseHasLoginEventTriggers(datform.dathasloginevt);
        if let Some(out) = out_dbname {
            out.clear();
            out.push_str(&dbname);
        }
    }

    gtrace("p.dblookup");
    init_small::globals::SetMyDatabaseId(dboid);

    // MyProc->databaseId: plain atomic store, no lock (C relies on the
    // database lock for searchers of this database's ID).
    let procno = lmgr_proc::MyProc().expect("InitPostgres before InitProcess");
    lmgr_proc::ProcGlobal().allProcs[procno as usize]
        .databaseId
        .store(dboid, std::sync::atomic::Ordering::Relaxed);

    // The catalog snapshot taken while reading pg_authid/pg_database predates
    // MyDatabaseId, so unshared-catalog sinval wasn't honored: drop it.
    snapmgr::InvalidateCatalogSnapshot();

    let fullpath = relpath_seams::get_database_path::call(
        mcx,
        init_small::globals::MyDatabaseId(),
        init_small::globals::MyDatabaseTableSpace(),
    )?;

    if !bootstrap {
        match std::fs::metadata(fullpath.as_str()) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return ereport(FATAL)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database \"{dbname}\" does not exist"))
                    .errdetail(format!(
                        "The database subdirectory \"{}\" is missing.",
                        fullpath.as_str()
                    ))
                    .finish(loc(1151, "InitPostgres"));
            }
            Err(e) => {
                return ereport(FATAL)
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not access directory \"{}\": {e}",
                        fullpath.as_str()
                    ))
                    .finish(loc(1158, "InitPostgres"));
            }
        }

        miscinit::ValidatePgVersion(fullpath.as_str())?;
    }

    if warm_claim {
        // Retained thread: the path was set on the first claim (set-once
        // global) and the db is pinned.
        debug_assert_eq!(init_small::globals::DatabasePath(), Some(fullpath.as_str()));
    } else {
        miscinit::SetDatabasePath(fullpath.as_str());
    }

    gtrace("p.dbpath");
    if warm_claim {
        // Retained caches were built against this database (wpool dispatch
        // pins standbys by db) and are drained-valid as of the accept above.
        let retained = init_small::wretain::retained_db();
        if retained != init_small::globals::MyDatabaseId() {
            panic!(
                "retained worker claimed for database {} but caches are for {retained}",
                init_small::globals::MyDatabaseId()
            );
        }
    } else {
        relcache::RelationCacheInitializePhase3()?;
    }
    gtrace("p.relcache3");

    acl_seams::initialize_acl::call()?;

    if !bootstrap {
        CheckMyDatabase(mcx, &dbname, am_superuser, (flags & INIT_PG_OVERRIDE_ALLOW_CONNS) != 0)?;
    }

    gtrace("p.checkdb");
    if init_small::globals::HaveMyProcPort() {
        process_startup_options(am_superuser)?;
    }

    // A thread-native parallel worker about to take a §3.4 session bind skips
    // the pg_db_role_setting scan: the leader's captured GUC state already
    // carries those effects (sources PGC_S_DATABASE/USER/DATABASE_USER/
    // GLOBAL) and the bind applies them verbatim, so rerunning here only
    // recomputes state the bind is about to overwrite.
    let takes_session_bind = guc::store::session_guc_bind_enabled()
        && parallel_seams::initializing_parallel_worker::is_installed()
        && parallel_seams::initializing_parallel_worker::call();
    if !takes_session_bind {
        process_settings(mcx, init_small::globals::MyDatabaseId(), miscinit::GetSessionUserId())?;
    }
    gtrace("p.settings");

    apply_post_auth_delay();

    namespace_seams::initialize_search_path::call()?;
    gtrace("p.searchpath");

    if !warm_claim {
        mbutils_seams::initialize_client_encoding::call()?;
    }

    session_seams::initialize_session::call()?;

    if (flags & INIT_PG_LOAD_SESSION_LIBS) != 0 {
        miscinit_seams::process_session_preload_libraries::call()?;
    }

    if !bootstrap {
        backend_status_seams::pgstat_bestart_final::call()?;
    }

    if !bootstrap {
        xact::CommitTransactionCommand()?;
    }
    gtrace("p.done");

    Ok(())
}

/// See the call site in InitPostgres: fails the first n BgWorker connects
/// just after SharedInvalBackendInit claimed the slot. Resolved once per
/// process; a plain Err, exactly the class an organic ProcSignalInit /
/// startup-transaction failure raises there.
fn test_connect_fail_after_sinval() -> PgResult<()> {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    static BUDGET: std::sync::OnceLock<Option<AtomicUsize>> = std::sync::OnceLock::new();
    let Some(budget) = BUDGET.get_or_init(|| {
        std::env::var("PGRUST_TEST_CONNECT_FAIL_AFTER_SINVAL")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .map(AtomicUsize::new)
    }) else {
        return Ok(());
    };
    if miscinit::GetMyBackendType() != BackendType::BgWorker
        || !init_small::globals::IsUnderPostmaster()
    {
        return Ok(());
    }
    if budget
        .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
        .is_ok()
    {
        return Err(Box::new(PgError::new(
            ERROR,
            "pgrust: test connect-fail-after-sinval injection",
        )));
    }
    Ok(())
}

fn apply_post_auth_delay() {
    let post_auth_delay = guc_tables::vars::PostAuthDelay.read();
    if post_auth_delay > 0 {
        std::thread::sleep(std::time::Duration::from_secs(post_auth_delay as u64));
    }
}

fn process_startup_options(am_superuser: bool) -> PgResult<()> {
    let gucctx = if am_superuser {
        GucContext::PGC_SU_BACKEND
    } else {
        GucContext::PGC_BACKEND
    };

    let (cmdline_options, guc_options) = init_small::globals::WithMyProcPort(|port| {
        (port.cmdline_options.clone(), port.guc_options.clone())
    });

    if let Some(cmdline_options) = cmdline_options {
        let maxac = 2 + (cmdline_options.len() + 1) / 2;
        let mut av: Vec<String> = Vec::with_capacity(maxac);
        av.push("postgres".to_string());
        pg_split_opts(&mut av, &cmdline_options);
        debug_assert!(av.len() < maxac);

        postgres_seams::process_postgres_switches::call(&av, gucctx as i32 as u8)?;
    }

    let mut it = guc_options.iter();
    while let Some(name) = it.next() {
        let value = it.next().expect("guc_options must be name/value pairs");
        guc::SetConfigOption(name, Some(value), gucctx, GucSource::PGC_S_CLIENT)?;
    }

    Ok(())
}

fn process_settings(mcx: Mcx<'_>, databaseid: Oid, roleid: Oid) -> PgResult<()> {
    if !init_small::globals::IsUnderPostmaster() {
        return Ok(());
    }

    let relsetting = table::table_open(mcx, DB_ROLE_SETTING_RELATION_ID, AccessShareLock)?;

    let snapshot = snapmgr::GetCatalogSnapshot(DB_ROLE_SETTING_RELATION_ID)?;
    let snapshot = snapmgr::RegisterSnapshot(Some(&snapshot))?.expect("registered snapshot");

    pg_db_role_setting_seams::apply_setting::call(
        &snapshot,
        databaseid,
        roleid,
        &relsetting,
        GucSource::PGC_S_DATABASE_USER,
    )?;
    pg_db_role_setting_seams::apply_setting::call(
        &snapshot,
        InvalidOid,
        roleid,
        &relsetting,
        GucSource::PGC_S_USER,
    )?;
    pg_db_role_setting_seams::apply_setting::call(
        &snapshot,
        databaseid,
        InvalidOid,
        &relsetting,
        GucSource::PGC_S_DATABASE,
    )?;
    pg_db_role_setting_seams::apply_setting::call(
        &snapshot,
        InvalidOid,
        InvalidOid,
        &relsetting,
        GucSource::PGC_S_GLOBAL,
    )?;

    snapmgr::UnregisterSnapshot(Some(&snapshot));
    relsetting.close(AccessShareLock)
}

pub fn ShutdownPostgres(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    xact::AbortOutOfAnyTransaction()?;
    lock::LockReleaseAll(USER_LOCKMETHOD.into(), true)
}

fn shutdown_postgres_cb(code: i32, arg: datum::Datum) -> PgResult<()> {
    ShutdownPostgres(code, arg)
}

fn shutdown_xlog_cb(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    // ShutdownXLOG's entry arm (xlog.c:6740): the shutdown checkpoint's
    // buffer pins need the aux-process resource owner. C reinstates it
    // inside ShutdownXLOG; hosted at this standalone-only callback here
    // (registered solely on the !IsUnderPostmaster path — the checkpointer's
    // ShutdownXLOG call site already runs under its own aux owner).
    debug_assert!(!resowner::AuxProcessResourceOwner().is_null());
    debug_assert!(
        resowner::CurrentResourceOwner().is_null()
            || resowner::CurrentResourceOwner() == resowner::AuxProcessResourceOwner()
    );
    resowner::SetCurrentResourceOwner(resowner::AuxProcessResourceOwner());
    transam_xlog_seams::shutdown_xlog::call()
}

fn pgstat_before_server_shutdown_cb(code: i32, _arg: datum::Datum) -> PgResult<()> {
    pgstat_seams::pgstat_before_server_shutdown::call(code)
}

fn datum_null() -> datum::Datum {
    datum::Datum::from_usize(0)
}

/// C self-signals SIGINT (SIGTERM during authentication_timeout use) to enter
/// StatementCancelHandler/die; SendThreadSignal is kill(MyProcPid, sig)'s
/// thread rendering, routing through the same installed dispositions. The
/// kill(-MyProcPid) process-group leg has no thread analog (no children).
pub fn StatementTimeoutHandler() {
    let sig = if elog::config::client_auth_in_progress() {
        SIGTERM
    } else {
        SIGINT
    };
    procsignal::SendThreadSignal(init_small::globals::MyProcPid(), sig);
}

pub fn LockTimeoutHandler() {
    procsignal::SendThreadSignal(init_small::globals::MyProcPid(), SIGINT);
}

fn set_latch_on_my_latch() {
    if let Some(l) = init_small::globals::MyLatch() {
        latch::SetLatch(l);
    }
}

pub fn TransactionTimeoutHandler() {
    init_small::globals::SetTransactionTimeoutPending(true);
    init_small::globals::SetInterruptPending(true);
    set_latch_on_my_latch();
}

pub fn IdleInTransactionSessionTimeoutHandler() {
    init_small::globals::SetIdleInTransactionSessionTimeoutPending(true);
    init_small::globals::SetInterruptPending(true);
    set_latch_on_my_latch();
}

pub fn IdleSessionTimeoutHandler() {
    init_small::globals::SetIdleSessionTimeoutPending(true);
    init_small::globals::SetInterruptPending(true);
    set_latch_on_my_latch();
}

pub fn IdleStatsUpdateTimeoutHandler() {
    init_small::globals::SetIdleStatsUpdateTimeoutPending(true);
    init_small::globals::SetInterruptPending(true);
    set_latch_on_my_latch();
}

pub fn ClientCheckTimeoutHandler() {
    init_small::globals::SetCheckClientConnectionPending(true);
    init_small::globals::SetInterruptPending(true);
    set_latch_on_my_latch();
}

/// ThereIsAtLeastOneRole: table_beginscan_catalog(pg_authid) expressed as the
/// keyless no-index systable scan (identical shape: forced heap scan, catalog
/// snapshot, allow_sync=false).
fn ThereIsAtLeastOneRole(mcx: Mcx<'_>) -> PgResult<bool> {
    let pg_authid_rel =
        table::table_open(mcx, types_core::catalog::AUTH_ID_RELATION_ID, AccessShareLock)?;

    let mut scan = genam::systable_beginscan(mcx, &pg_authid_rel, InvalidOid, false, None, &[])?;
    let result = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;

    pg_authid_rel.close(AccessShareLock)?;
    Ok(result)
}

fn strlcpy_name(src: &str) -> String {
    if src.len() <= NAMEDATALEN - 1 {
        src.to_string()
    } else {
        String::from_utf8_lossy(&src.as_bytes()[..NAMEDATALEN - 1]).into_owned()
    }
}
