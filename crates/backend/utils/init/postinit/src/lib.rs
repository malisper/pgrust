//! Port of `src/backend/utils/init/postinit.c` — backend startup utilities.
//!
//! postinit.c is the orchestration of POSTGRES backend initialization. Its OWN
//! logic lives here: the branching that distinguishes bootstrap / autovacuum
//! launcher & worker / physical & logical walsender / background-worker /
//! normal-multiuser startup, the connection-limit and reserved-slot
//! arithmetic, the option-string splitter, the `MaxBackends` and
//! `FastPathLockGroupsPerBackend` computations, the timeout-handler bodies, and
//! the locale recheck control flow. Every leaf call into another subsystem is
//! routed through that owner's `-seams` crate (panicking until the owner
//! lands) or, where acyclic, a direct dependency.

#![allow(non_snake_case)]

use ::utils_error::ereport;
use mcx::{Mcx, MemoryContext, PgString};
use ::types_catalog::pg_database::{FormPgDatabase, COLLPROVIDER_LIBC};
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgError, PgResult, DEBUG3, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_TOO_MANY_CONNECTIONS,
    ERRCODE_UNDEFINED_DATABASE, ERRCODE_UNDEFINED_OBJECT, ERROR, FATAL, LOG, WARNING,
};
use types_guc::{GucContext, GucSource};
use types_timeout::{TimeoutHandlerProc, TimeoutId};

/// C `bits32` (`c.h`) — the `flags` word of `InitPostgres`.
#[allow(non_camel_case_types)]
pub type bits32 = u32;

/// The timeout-id variant the `more2-seams` (timeout.c enable/disable) decls
/// use lives in `types-core`; the `timeout-seams` register decls use
/// `types-timeout`. Bridge `STATEMENT_TIMEOUT` for the auth timeout.
use ::types_core::TimeoutId as CoreTimeoutId;

const SRC: &str = "src/backend/utils/init/postinit.c";

#[inline]
fn loc(lineno: i32, func: &str) -> ::types_error::ErrorLocation {
    ::types_error::ErrorLocation::new(SRC, lineno, func)
}

// ---------------------------------------------------------------------------
// Constants (verified against PostgreSQL 18.3 generated headers).
// ---------------------------------------------------------------------------

/// `Template1DbOid` (pg_database_d.h).
pub const TEMPLATE1_DB_OID: Oid = ::types_catalog::catalog::TEMPLATE1_DB_OID;
/// `DEFAULTTABLESPACE_OID` (pg_tablespace_d.h).
pub const DEFAULTTABLESPACE_OID: Oid = ::types_catalog::catalog::DEFAULTTABLESPACE_OID;
/// `ROLE_PG_USE_RESERVED_CONNECTIONS` (pg_authid_d.h).
pub const ROLE_PG_USE_RESERVED_CONNECTIONS: Oid =
    ::types_catalog::catalog::ROLE_PG_USE_RESERVED_CONNECTIONS;

/// `INIT_PG_LOAD_SESSION_LIBS` (miscadmin.h).
pub const INIT_PG_LOAD_SESSION_LIBS: bits32 = 0x0001;
/// `INIT_PG_OVERRIDE_ALLOW_CONNS` (miscadmin.h).
pub const INIT_PG_OVERRIDE_ALLOW_CONNS: bits32 = 0x0002;
/// `INIT_PG_OVERRIDE_ROLE_LOGIN` (miscadmin.h).
pub const INIT_PG_OVERRIDE_ROLE_LOGIN: bits32 = 0x0004;

/// `MAX_BACKENDS_BITS` / `MAX_BACKENDS` (procnumber.h).
pub const MAX_BACKENDS_BITS: u32 = 18;
pub const MAX_BACKENDS: i32 = ((1u32 << MAX_BACKENDS_BITS) - 1) as i32;

/// `NUM_SPECIAL_WORKER_PROCS` (proc.h).
pub const NUM_SPECIAL_WORKER_PROCS: i32 = 2;

/// `FP_LOCK_GROUPS_PER_BACKEND_MAX` / `FP_LOCK_SLOTS_PER_GROUP` (proc.h).
pub const FP_LOCK_GROUPS_PER_BACKEND_MAX: i32 = 1024;
pub const FP_LOCK_SLOTS_PER_GROUP: i32 = 16;

/// `NAMEDATALEN` (pg_config_manual.h).
const NAMEDATALEN: usize = 64;

// ---------------------------------------------------------------------------
// Pure helpers ported from PostgreSQL headers.
// ---------------------------------------------------------------------------

/// `pg_nextpower2_32(num)` (port/pg_bitutils.h): the smallest power of two >=
/// `num`. `num` must be in `(0, PG_UINT32_MAX/2 + 1]`.
#[inline]
pub fn pg_nextpower2_32(num: u32) -> u32 {
    if (num & (num - 1)) == 0 {
        return num;
    }
    let leftmost = 31 - num.leading_zeros();
    1u32 << (leftmost + 1)
}

#[inline]
fn pg_min(a: i32, b: i32) -> i32 {
    if a < b { a } else { b }
}

#[inline]
fn pg_max(a: i32, b: i32) -> i32 {
    if a > b { a } else { b }
}

/// `isspace((unsigned char) c)` — C/POSIX classification used by pg_split_opts.
#[inline]
fn c_isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

// ===========================================================================
// *** InitPostgres support ***
// ===========================================================================

/// `GetDatabaseTuple` — fetch the pg_database row for a database by name.
///
/// C opens pg_database, scans by `datname`, copies the tuple before releasing
/// the buffer, and closes. The owning catalog-read seam performs the
/// table_open/scan/heap_copytuple/close and returns the decoded row.
fn GetDatabaseTuple<'mcx>(mcx: Mcx<'mcx>, dbname: &str) -> PgResult<Option<FormPgDatabase<'mcx>>> {
    pg_database_seams::get_database_tuple_by_name::call(mcx, dbname)
}

/// `GetDatabaseTupleByOid` — as above, but search by database OID.
fn GetDatabaseTupleByOid<'mcx>(mcx: Mcx<'mcx>, dboid: Oid) -> PgResult<Option<FormPgDatabase<'mcx>>> {
    pg_database_seams::get_database_tuple_by_oid::call(mcx, dboid)
}

/// `PerformAuthentication` — authenticate a remote client.
///
/// Returns nothing; will not return at all on failure (the auth seam raises
/// FATAL). The `EXEC_BACKEND` load_hba()/load_ident() block is `#ifdef`-ed out
/// in the repo's non-EXEC_BACKEND target.
fn PerformAuthentication(mcx: Mcx<'_>) -> PgResult<()> {
    // This should be set already, but let's make sure.
    postmaster_seams::set_client_auth_in_progress::call(true);

    // (EXEC_BACKEND: load_hba()/load_ident() — not compiled in this build.)

    // Capture authentication start time for logging.
    let auth_start = timestamp_seams::get_current_timestamp::call();
    backend_startup_seams::set_conn_timing_auth_start::call(auth_start);

    // Set up a timeout in case a buggy or malicious client fails to respond
    // during authentication. Since we're inside a transaction and might do
    // database access, we have to use the statement_timeout infrastructure.
    let auth_timeout_ms = auth_seams::authentication_timeout::call() * 1000;
    more2_seams::enable_timeout_after::call(
        CoreTimeoutId::STATEMENT_TIMEOUT,
        auth_timeout_ms,
    )?;

    // Now perform authentication exchange.
    more_seams::set_ps_display::call("authentication");
    auth_seams::client_authentication::call()?; // might not return

    // Done with authentication. Disable the timeout, and log if needed.
    more2_seams::disable_timeout::call(CoreTimeoutId::STATEMENT_TIMEOUT, false)?;

    // Capture authentication end time for logging.
    let auth_end = timestamp_seams::get_current_timestamp::call();
    backend_startup_seams::set_conn_timing_auth_end::call(auth_end);

    if auth_seams::log_connection_authorization::call() {
        // Assemble the message verbatim, mirroring the C StringInfoData logmsg
        // built in CurrentMemoryContext and pfree'd after the report.
        let logmsg = build_auth_logmsg(mcx)?;
        let _ = ereport(LOG)
            .errmsg_internal(logmsg.as_str())
            .finish(loc(309, "PerformAuthentication"));
    }

    more_seams::set_ps_display::call("startup");

    postmaster_seams::set_client_auth_in_progress::call(false);

    Ok(())
}

/// Assemble the `PerformAuthentication` log message into an `mcx` string (the
/// C `StringInfoData logmsg`).
fn build_auth_logmsg(mcx: Mcx<'_>) -> PgResult<PgString<'_>> {
    let mut logmsg = PgString::new_in(mcx);

    let am_walsender = walsender_seams::am_walsender::call();
    let user = init_small_seams::my_proc_port_user_name::call(mcx)?;
    if am_walsender {
        logmsg.try_push_str("replication connection authorized: user=")?;
    } else {
        logmsg.try_push_str("connection authorized: user=")?;
    }
    logmsg.try_push_str(user.as_str())?;

    if !am_walsender {
        let db = init_small_seams::my_proc_port_database_name::call(mcx)?;
        logmsg.try_push_str(" database=")?;
        logmsg.try_push_str(db.as_str())?;
    }

    if let Some(app) = init_small_seams::my_proc_port_application_name::call(mcx)? {
        logmsg.try_push_str(" application_name=")?;
        logmsg.try_push_str(app.as_str())?;
    }

    // #ifdef USE_SSL (compiled in this build): append the SSL fragment when
    // the connection is using TLS. The TLS version/cipher/bits accessors read
    // be-secure's SSL state (seamed); the `port->ssl_in_use` branch and the
    // format assembly are postinit's own logic, mirroring C 283-287.
    //
    // (#ifdef ENABLE_GSS is not compiled in this build, so the GSS fragment at
    // C 288-307 is omitted.)
    let mut ssl_frag: Option<(PgString<'_>, PgString<'_>, i32)> = None;
    let mut ssl_err: Option<::types_error::PgError> = None;
    init_small_seams::with_my_proc_port::call(&mut |port| {
        if let Some(port) = port {
            if port.ssl_in_use {
                let r = (|| {
                    let version = be_secure_seams::be_tls_get_version::call(mcx, port)?;
                    let cipher = be_secure_seams::be_tls_get_cipher::call(mcx, port)?;
                    let bits = be_secure_seams::be_tls_get_cipher_bits::call(port);
                    Ok((version, cipher, bits))
                })();
                match r {
                    Ok(v) => ssl_frag = Some(v),
                    Err(e) => ssl_err = Some(e),
                }
            }
        }
    });
    if let Some(e) = ssl_err {
        return Err(e);
    }
    if let Some((version, cipher, bits)) = ssl_frag {
        logmsg.try_push_str(" SSL enabled (protocol=")?;
        logmsg.try_push_str(version.as_str())?;
        logmsg.try_push_str(", cipher=")?;
        logmsg.try_push_str(cipher.as_str())?;
        logmsg.try_push_str(", bits=")?;
        logmsg.try_push_str(&bits.to_string())?;
        logmsg.try_push_str(")")?;
    }

    Ok(logmsg)
}

/// `CheckMyDatabase` — fetch information from the pg_database entry for our DB.
fn CheckMyDatabase(
    mcx: Mcx<'_>,
    name: &str,
    am_superuser: bool,
    override_allow_connections: bool,
) -> PgResult<()> {
    let my_database_id = init_small_seams::my_database_id::call();

    // Fetch our pg_database row normally, via syscache.
    let dbform = match pg_database_seams::search_database_syscache::call(
        mcx,
        my_database_id,
    )? {
        Some(f) => f,
        None => {
            return ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for database {my_database_id}"))
                .finish(loc(335, "CheckMyDatabase"));
        }
    };

    // This recheck is strictly paranoia.
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

    // Check permissions to connect to the database. Not enforced in standalone.
    if init_small_seams::is_under_postmaster::call() {
        // Check that the database is currently allowing connections.
        if !dbform.datallowconn && !override_allow_connections {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "database \"{name}\" is not currently accepting connections"
                ))
                .finish(loc(362, "CheckMyDatabase"));
        }

        // Check privilege to connect to the database. C evaluates
        // object_aclcheck(..., GetUserId(), ...) only after the first two
        // short-circuiting conditions; preserve that ordering.
        if !am_superuser && !override_allow_connections {
            let userid = miscinit_seams::get_user_id::call();
            // object_aclcheck(DatabaseRelationId, MyDatabaseId, userid, ACL_CONNECT) != ACLCHECK_OK
            let res = aclchk_seams::object_aclcheck::call(
                ::types_catalog::catalog::DATABASE_RELATION_ID,
                my_database_id,
                userid,
                types_acl::acl::ACL_CONNECT,
            )?;
            if res != types_acl::acl::AclResult::AclcheckOk {
                return ereport(FATAL)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!("permission denied for database \"{name}\""))
                    .errdetail("User does not have CONNECT privilege.")
                    .finish(loc(375, "CheckMyDatabase"));
            }
        }

        // Check connection limit for this database. Enforced only for regular
        // backends, since other process types have their own PGPROC pools.
        if dbform.datconnlimit >= 0
            && lmgr_proc_seams::am_regular_backend_process::call()
            && !am_superuser
            && procarray_seams::count_db_connections::call(my_database_id)?
                > dbform.datconnlimit
        {
            return ereport(FATAL)
                .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
                .errmsg(format!("too many connections for database \"{name}\""))
                .finish(loc(396, "CheckMyDatabase"));
        }
    }

    // OK, we're golden. Save the encoding info out of the pg_database tuple.
    mbutils_seams::set_database_encoding::call(dbform.encoding)?;
    // Record it as a GUC internal option, too.
    let encname = mbutils_seams::get_database_encoding_name::call();
    guc_seams::set_config_option::call(
        "server_encoding",
        encname,
        GucContext::PGC_INTERNAL,
        GucSource::PGC_S_DYNAMIC_DEFAULT,
    )?;
    // If we have no other source of client_encoding, use server encoding.
    let encname = mbutils_seams::get_database_encoding_name::call();
    guc_seams::set_config_option::call(
        "client_encoding",
        encname,
        GucContext::PGC_BACKEND,
        GucSource::PGC_S_DYNAMIC_DEFAULT,
    )?;

    // assign locale variables
    let collate = dbform.datcollate.as_str();
    let ctype = dbform.datctype.as_str();

    if pg_locale_seams::pg_perm_setlocale::call(
        mcx,
        pg_locale_seams::LcCategory::LcCollate,
        collate,
    )?
    .is_none()
    {
        return ereport(FATAL)
            .errmsg("database locale is incompatible with operating system")
            .errdetail(format!(
                "The database was initialized with LC_COLLATE \"{collate}\", \
                 which is not recognized by setlocale()."
            ))
            .errhint("Recreate the database with another locale or install the missing locale.")
            .finish(loc(421, "CheckMyDatabase"));
    }

    if pg_locale_seams::pg_perm_setlocale::call(
        mcx,
        pg_locale_seams::LcCategory::LcCtype,
        ctype,
    )?
    .is_none()
    {
        return ereport(FATAL)
            .errmsg("database locale is incompatible with operating system")
            .errdetail(format!(
                "The database was initialized with LC_CTYPE \"{ctype}\", \
                 which is not recognized by setlocale()."
            ))
            .errhint("Recreate the database with another locale or install the missing locale.")
            .finish(loc(428, "CheckMyDatabase"));
    }

    if ctype == "C" || ctype == "POSIX" {
        pg_locale_seams::set_database_ctype_is_c::call(true);
    }

    pg_locale_seams::init_database_collation::call()?;

    // Check collation version. Warn (don't error) so we never block connecting.
    if let Some(collversion) = dbform.datcollversion.as_ref() {
        let collversionstr = collversion.as_str();
        let locale_owned;
        let locale: &str = if dbform.datlocprovider == COLLPROVIDER_LIBC {
            collate
        } else {
            // SysCacheGetAttrNotNull(Anum_pg_database_datlocale)
            match dbform.datlocale.as_ref() {
                Some(l) => {
                    locale_owned = l;
                    locale_owned.as_str()
                }
                None => {
                    return ereport(ERROR)
                        .errmsg_internal(
                            "unexpected null value in cached tuple for catalog pg_database column datlocale",
                        )
                        .finish(loc(459, "CheckMyDatabase"));
                }
            }
        };

        let actual = pg_locale_seams::get_collation_actual_version::call(
            mcx,
            dbform.datlocprovider,
            locale,
        )?;
        match actual {
            None => {
                // should not happen
                let _ = ereport(WARNING)
                    .errmsg_internal(format!(
                        "database \"{name}\" has no actual collation version, but a version was recorded"
                    ))
                    .finish(loc(466, "CheckMyDatabase"));
            }
            Some(actual_versionstr) if actual_versionstr.as_str() != collversionstr => {
                let quoted =
                    ruleutils_seams::quote_identifier::call(mcx, name)?;
                let _ = ereport(WARNING)
                    .errmsg(format!("database \"{name}\" has a collation version mismatch"))
                    .errdetail(format!(
                        "The database was created using collation version {collversionstr}, \
                         but the operating system provides version {}.",
                        actual_versionstr.as_str()
                    ))
                    .errhint(format!(
                        "Rebuild all objects in this database that use the default collation and run \
                         ALTER DATABASE {} REFRESH COLLATION VERSION, \
                         or build PostgreSQL with the right library version.",
                        quoted.as_str()
                    ))
                    .finish(loc(470, "CheckMyDatabase"));
            }
            Some(_) => {}
        }
    }

    // ReleaseSysCache(tup) — the decoded form is owned in mcx; nothing to do.
    Ok(())
}

/// `pg_split_opts` — split a string of options and append it to an argv array.
///
/// Some option values can contain spaces, allowed via backslash escaping, with
/// `\\` representing a literal backslash. `argv` is a growable vector and
/// `argcp` the running count, kept in lock-step with the C `argv[(*argcp)++]`
/// store. The transient per-option working buffer is an `mcx` string (the C
/// `StringInfoData s`).
pub fn pg_split_opts(
    mcx: Mcx<'_>,
    argv: &mut Vec<String>,
    argcp: &mut usize,
    optstr: &str,
) -> PgResult<()> {
    let bytes = optstr.as_bytes();
    let mut i = 0usize;
    let n = bytes.len();

    // initStringInfo(&s)
    let mut s = PgString::new_in(mcx);

    while i < n {
        let mut last_was_escape = false;

        // resetStringInfo(&s)
        s.clear();

        // skip over leading space
        while i < n && c_isspace(bytes[i]) {
            i += 1;
        }

        // if (*optstr == '\0') break;
        if i >= n {
            break;
        }

        // Parse a single option, stopping at the first space, unless escaped.
        while i < n {
            let c = bytes[i];
            if c_isspace(c) && !last_was_escape {
                break;
            }

            if !last_was_escape && c == b'\\' {
                last_was_escape = true;
            } else {
                last_was_escape = false;
                // appendStringInfoChar(&s, *optstr)
                s.try_push(c as char)?;
            }

            i += 1;
        }

        // now store the option in the next argv[] position (pstrdup analog)
        argv.try_reserve(1).map_err(reserve_failed)?;
        argv.push(s.as_str().to_string());
        *argcp += 1;
    }

    Ok(())
}

/// `InitializeMaxBackends` — initialize MaxBackends from config options.
pub fn InitializeMaxBackends() -> PgResult<()> {
    debug_assert_eq!(
        init_small_seams::max_backends::call(),
        0,
        "MaxBackends must be 0"
    );

    // Note that this does not include "auxiliary" processes.
    let max_connections = init_small_seams::max_connections::call();
    let av_worker_slots = autovacuum_seams::autovacuum_worker_slots::call();
    let max_worker_processes = init_small_seams::max_worker_processes::call();
    let max_wal_senders = walsender_seams::max_wal_senders::call();

    let max_backends =
        max_connections + av_worker_slots + max_worker_processes + max_wal_senders + NUM_SPECIAL_WORKER_PROCS;
    init_small_seams::set_max_backends::call(max_backends);

    if max_backends > MAX_BACKENDS {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("too many server processes configured")
            .errdetail(format!(
                "\"max_connections\" ({max_connections}) plus \"autovacuum_worker_slots\" \
                 ({av_worker_slots}) plus \"max_worker_processes\" ({max_worker_processes}) plus \
                 \"max_wal_senders\" ({max_wal_senders}) must be less than {}.",
                MAX_BACKENDS - (NUM_SPECIAL_WORKER_PROCS - 1)
            ))
            .finish(loc(564, "InitializeMaxBackends"));
    }

    Ok(())
}

/// `InitializeFastPathLocks` — initialize the number of fast-path lock slots in
/// PGPROC.
pub fn InitializeFastPathLocks() -> PgResult<()> {
    debug_assert_eq!(
        init_small_seams::fast_path_lock_groups_per_backend::call(),
        0,
        "FastPathLockGroupsPerBackend must be 0"
    );

    // Based on max_locks_per_transaction, figure out the power-of-two value,
    // capped at FP_LOCK_GROUPS_PER_BACKEND_MAX and at least 1.
    let max_locks_per_xact = lock_seams::max_locks_per_xact::call();
    let value = pg_max(
        pg_min(
            (pg_nextpower2_32(max_locks_per_xact as u32) as i32) / FP_LOCK_SLOTS_PER_GROUP,
            FP_LOCK_GROUPS_PER_BACKEND_MAX,
        ),
        1,
    );
    init_small_seams::set_fast_path_lock_groups_per_backend::call(value);

    debug_assert_eq!(
        value,
        pg_nextpower2_32(value as u32) as i32,
        "FastPathLockGroupsPerBackend must be a power of two"
    );

    Ok(())
}

/// `BaseInit` — early initialization of a backend (standalone or under
/// postmaster). Happens even before InitPostgres; also called by auxiliary
/// processes that may not call InitPostgres at all.
pub fn BaseInit() -> PgResult<()> {
    // Initialize our input/output/debugging file descriptors.
    ::utils_error::DebugFileOpen()?;

    // Initialize file access. Done early so other subsystems can access files.
    fd_seams::init_file_access::call()?;

    // Initialize statistics reporting. This needs to happen early to ensure that
    // pgstat's shutdown callback runs after the shutdown callbacks of all
    // subsystems that can produce stats.
    pgstat_seams::pgstat_initialize::call()?;

    // Initialize AIO before infrastructure that might need to execute AIO.
    aio_seams_2::pgaio_init_backend::call()?;

    // Do local initialization of storage and buffer managers.
    // `InitSync()` creates the pending-operations table iff this process tracks
    // sync requests: `!IsUnderPostmaster || AmCheckpointerProcess()` (sync.c).
    let create_pending_ops = !init_small_seams::is_under_postmaster::call()
        || init_small_seams::my_backend_type::call()
            == ::types_core::init::BackendType::Checkpointer;
    sync_seams::init_sync::call(create_pending_ops);
    smgr_seams::smgrinit::call()?;
    bufmgr_seams::init_buffer_manager_access::call()?;

    // Initialize temporary file access after pgstat, so that the temporary file
    // shutdown hook can report temporary file statistics.
    fd_seams::init_temporary_file_access::call()?;

    // Initialize local buffers for WAL record construction.
    xloginsert_seams::init_xlog_insert::call()?;

    // Initialize lock manager's local structs.
    lock_seams::init_lock_manager_access::call()?;

    // Initialize replication slots after pgstat. The exit hook might need to
    // drop ephemeral slots, which in turn triggers stats reporting.
    slot_seams::replication_slot_initialize::call()?;

    Ok(())
}

/// `InitPostgres` — initialize POSTGRES. See the C header comment for the
/// meaning of the parameters. `out_dbname` is the optional NAMEDATALEN output
/// buffer: when `Some`, the resolved database name is written into it.
pub fn InitPostgres(
    mcx: Mcx<'_>,
    in_dbname: Option<&str>,
    mut dboid: Oid,
    username: Option<&str>,
    useroid: Oid,
    flags: bits32,
    out_dbname: Option<&mut String>,
) -> PgResult<()> {
    let bootstrap = miscinit_seams::is_bootstrap_processing_mode::call();
    let am_superuser: bool;
    let mut dbname = String::new();

    let _ = ereport(DEBUG3)
        .errmsg_internal("InitPostgres")
        .finish(loc(723, "InitPostgres"));

    // Add my PGPROC struct to the ProcArray. Once done, I am visible to others.
    lmgr_proc_seams::init_process_phase2::call(mcx)?;

    // Initialize status reporting.
    status_seams::pgstat_beinit::call()?;

    // And initialize an entry in the PgBackendStatus array.
    if !bootstrap {
        status_seams::pgstat_bestart_initial::call()?;
        // INJECTION_POINT("init-pre-auth") — injection points are not compiled.
    }

    // Initialize my entry in the shared-invalidation manager's array.
    sinval_seams::shared_inval_backend_init::call(false)?;

    // ProcSignalInit(MyCancelKey, MyCancelKeyLength). The ported owner reads
    // MyProcNumber/MyProcPid explicitly; the cancel-key bytes come from the
    // globals.c owner (MyCancelKey/MyCancelKeyLength globals).
    let cancel_key = init_small_seams::my_cancel_key::call(mcx)?;
    procsignal::ProcSignalInit(
        init_small_seams::my_proc_number::call(),
        init_small_seams::my_proc_pid::call(),
        cancel_key.as_slice(),
    )?;

    // Also set up timeout handlers needed for backend operation. Needed in every
    // case except bootstrap.
    if !bootstrap {
        timeout_seams::register_timeout::call(
            TimeoutId::DEADLOCK_TIMEOUT,
            check_dead_lock_alert_cb as TimeoutHandlerProc,
        );
        timeout_seams::register_timeout::call(
            TimeoutId::STATEMENT_TIMEOUT,
            StatementTimeoutHandler as TimeoutHandlerProc,
        );
        timeout_seams::register_timeout::call(
            TimeoutId::LOCK_TIMEOUT,
            LockTimeoutHandler as TimeoutHandlerProc,
        );
        timeout_seams::register_timeout::call(
            TimeoutId::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            IdleInTransactionSessionTimeoutHandler as TimeoutHandlerProc,
        );
        timeout_seams::register_timeout::call(
            TimeoutId::TRANSACTION_TIMEOUT,
            TransactionTimeoutHandler as TimeoutHandlerProc,
        );
        timeout_seams::register_timeout::call(
            TimeoutId::IDLE_SESSION_TIMEOUT,
            IdleSessionTimeoutHandler as TimeoutHandlerProc,
        );
        timeout_seams::register_timeout::call(
            TimeoutId::CLIENT_CONNECTION_CHECK_TIMEOUT,
            ClientCheckTimeoutHandler as TimeoutHandlerProc,
        );
        timeout_seams::register_timeout::call(
            TimeoutId::IDLE_STATS_UPDATE_TIMEOUT,
            IdleStatsUpdateTimeoutHandler as TimeoutHandlerProc,
        );
    }

    // If this is a bootstrap process or a standalone backend, start up the XLOG
    // machinery. Otherwise the startup process / checkpointer handle it.
    if !init_small_seams::is_under_postmaster::call() {
        resowner_seams::create_aux_process_resource_owner::call()?;

        transam_xlog::StartupXLOG()?;
        // Release (and warn about) any buffer pins leaked in StartupXLOG.
        resowner_seams::release_aux_process_resources::call(true)?;
        // Reset CurrentResourceOwner to nothing for the moment.
        resowner_seams::reset_current_resource_owner::call();

        // Use before_shmem_exit() so that ShutdownXLOG() can rely on DSM segments.
        dsm_core_seams::before_shmem_exit::call(
            pgstat_before_server_shutdown_cb,
            types_tuple::Datum::null(),
        )?;
        dsm_core_seams::before_shmem_exit::call(
            shutdown_xlog_cb,
            types_tuple::Datum::null(),
        )?;
    }

    // Initialize the relation cache and the system catalog caches. No catalog
    // access happens here; we only set up the hashtable structure.
    relcache_seams::relation_cache_initialize::call()?;
    cache_syscache::InitCatalogCache()?;
    plancache_seams::init_plan_cache::call()?;

    // Initialize portal manager.
    portalmem_seams::enable_portal_manager::call()?;

    // Load relcache entries for the shared system catalogs.
    relcache_seams::relation_cache_initialize_phase2::call()?;

    // Set up process-exit callback to do pre-shutdown cleanup.
    dsm_core_seams::before_shmem_exit::call(
        shutdown_postgres_cb,
        types_tuple::Datum::null(),
    )?;

    // The autovacuum launcher is done here.
    if autovacuum_seams::am_autovacuum_launcher_process::call() {
        status_seams::pgstat_bestart_final::call()?;
        return Ok(());
    }

    // Start a new transaction here before first access to db.
    if !bootstrap {
        // statement_timestamp must be set for timeouts to work correctly.
        transam_xact_seams::set_current_statement_start_timestamp::call();
        transam_xact_seams::start_transaction_command::call()?;

        // transaction_isolation will have been set to the default by the above.
        // If serializable and in hot standby, lower to read committed.
        transam_xact_seams::set_xact_iso_level_read_committed::call();
    }

    // Perform client authentication if necessary, then figure out our postgres
    // user ID, and see if we are a superuser.
    if bootstrap
        || autovacuum_seams::am_autovacuum_worker_process::call()
        || miscinit_seams::am_logical_slot_sync_worker_process::call()
    {
        miscinit_seams::initialize_session_user_id_standalone::call()?;
        am_superuser = true;
    } else if !init_small_seams::is_under_postmaster::call() {
        miscinit_seams::initialize_session_user_id_standalone::call()?;
        am_superuser = true;
        if !ThereIsAtLeastOneRole(mcx)? {
            let _ = ereport(WARNING)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg("no roles are defined in this database system")
                .errhint(format!(
                    "You should immediately run CREATE USER \"{}\" SUPERUSER;.",
                    username.unwrap_or("postgres")
                ))
                .finish(loc(876, "InitPostgres"));
        }
    } else if init_small_seams::my_backend_type::call()
        == ::types_core::init::BackendType::BgWorker
    {
        if username.is_none() && !OidIsValid(useroid) {
            miscinit_seams::initialize_session_user_id_standalone::call()?;
            am_superuser = true;
        } else {
            miscinit_seams::initialize_session_user_id::call(
                mcx,
                username,
                useroid,
                (flags & INIT_PG_OVERRIDE_ROLE_LOGIN) != 0,
            )?;
            am_superuser = superuser_seams::superuser::call()?;
        }
    } else {
        // normal multiuser case
        PerformAuthentication(mcx)?;
        miscinit_seams::initialize_session_user_id::call(mcx, username, useroid, false)?;
        // ensure that auth_method is actually valid, aka authn_id is not NULL
        if let Some(authn_id) = auth_seams::client_authn_id::call(mcx)? {
            let auth_method = hba_seams::hba_authname::call(mcx)?;
            miscinit_seams::initialize_system_user::call(
                authn_id.as_str(),
                auth_method.as_str(),
            );
        }
        am_superuser = superuser_seams::superuser::call()?;
    }

    // Report any SSL/GSS details for the session.
    if init_small_seams::has_my_proc_port::call() {
        debug_assert!(!bootstrap);
        status_seams::pgstat_bestart_security::call()?;
    }

    // Binary upgrades only allow super-user connections.
    if init_small_seams::is_binary_upgrade::call() && !am_superuser {
        return ereport(FATAL)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to connect in binary upgrade mode")
            .finish(loc(922, "InitPostgres"));
    }

    // The last few regular connection slots are reserved for superusers and
    // roles with privileges of pg_use_reserved_connections.
    let su_reserved = init_small_seams::superuser_reserved_connections::call();
    let reserved_conns = init_small_seams::reserved_connections::call();
    if lmgr_proc_seams::am_regular_backend_process::call()
        && !am_superuser
        && (su_reserved + reserved_conns) > 0
    {
        let mut nfree = 0;
        let have = lmgr_proc_seams::have_n_free_procs::call(
            su_reserved + reserved_conns,
            &mut nfree,
        );
        if !have {
            if nfree < su_reserved {
                return ereport(FATAL)
                    .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
                    .errmsg(
                        "remaining connection slots are reserved for roles with the SUPERUSER attribute",
                    )
                    .finish(loc(942, "InitPostgres"));
            }

            let userid = miscinit_seams::get_user_id::call();
            if !acl_seams::has_privs_of_role::call(
                userid,
                ROLE_PG_USE_RESERVED_CONNECTIONS,
            )? {
                return ereport(FATAL)
                    .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
                    .errmsg(
                        "remaining connection slots are reserved for roles with privileges of the \"pg_use_reserved_connections\" role",
                    )
                    .finish(loc(948, "InitPostgres"));
            }
        }
    }

    // Check replication permissions needed for walsender processes.
    if walsender_seams::am_walsender::call() {
        debug_assert!(!bootstrap);

        let userid = miscinit_seams::get_user_id::call();
        if !miscinit_seams::has_rolreplication::call(mcx, userid)? {
            return ereport(FATAL)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to start WAL sender")
                .errdetail(
                    "Only roles with the REPLICATION attribute may start a WAL sender process.",
                )
                .finish(loc(960, "InitPostgres"));
        }
    }

    // If this is a plain walsender only supporting physical replication, we
    // don't want to connect to any particular database.
    if walsender_seams::am_walsender::call()
        && !walsender_seams::am_db_walsender::call()
    {
        // process any options passed in the startup packet
        if init_small_seams::has_my_proc_port::call() {
            process_startup_options(mcx, am_superuser)?;
        }

        // Apply PostAuthDelay as soon as we've read all options.
        let post_auth_delay = postgres_seams::post_auth_delay::call();
        if post_auth_delay > 0 {
            pgsleep_seams::pg_usleep::call(post_auth_delay as i64 * 1_000_000);
        }

        // initialize client encoding
        mbutils_seams::initialize_client_encoding::call()?;

        // fill in the remainder of this entry in the PgBackendStatus array
        status_seams::pgstat_bestart_final::call()?;

        // close the transaction we started above
        transam_xact_seams::commit_transaction_command::call()?;

        return Ok(());
    }

    // Set up the global variables holding database id and default tablespace.
    if bootstrap {
        dboid = TEMPLATE1_DB_OID;
        init_small_seams::set_my_database_table_space::call(DEFAULTTABLESPACE_OID);
    } else if let Some(in_dbname) = in_dbname {
        let dbform = match GetDatabaseTuple(mcx, in_dbname)? {
            Some(f) => f,
            None => {
                return ereport(FATAL)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database \"{in_dbname}\" does not exist"))
                    .finish(loc(1014, "InitPostgres"));
            }
        };
        dboid = dbform.oid;
    } else if !OidIsValid(dboid) {
        // A background worker not bound to any particular database is done now.
        if !bootstrap {
            status_seams::pgstat_bestart_final::call()?;
            transam_xact_seams::commit_transaction_command::call()?;
        }
        return Ok(());
    }

    // Now take a writer's lock on the database we are connecting to.
    if !bootstrap {
        // LockSharedObject(DatabaseRelationId, dboid, 0, RowExclusiveLock)
        lmgr_seams::lock_shared_object::call(
            ::types_catalog::catalog::DATABASE_RELATION_ID,
            dboid,
            0,
            types_storage::lock::RowExclusiveLock,
        )?;
    }

    // Recheck pg_database to make sure the target database hasn't gone away.
    if !bootstrap {
        let tuple = GetDatabaseTupleByOid(mcx, dboid)?;

        let name_mismatch = match (tuple.as_ref(), in_dbname) {
            (Some(df), Some(req)) => df.datname.as_str() != req,
            _ => false,
        };
        if tuple.is_none() || name_mismatch {
            if let Some(req) = in_dbname {
                return ereport(FATAL)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database \"{req}\" does not exist"))
                    .errdetail("It seems to have just been dropped or renamed.")
                    .finish(loc(1078, "InitPostgres"));
            } else {
                return ereport(FATAL)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database {dboid} does not exist"))
                    .finish(loc(1083, "InitPostgres"));
            }
        }

        let datform = tuple.unwrap();
        // strlcpy(dbname, NameStr(datform->datname), sizeof(dbname))
        dbname = strlcpy_name(datform.datname.as_str());

        if dbcommands_seams::database_is_invalid_form::call(
            datform.datconnlimit,
        ) {
            return ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("cannot connect to invalid database \"{dbname}\""))
                .errhint("Use DROP DATABASE to drop invalid databases.")
                .finish(loc(1092, "InitPostgres"));
        }

        init_small_seams::set_my_database_table_space::call(datform.dattablespace);
        init_small_seams::set_my_database_has_login_event_triggers::call(
            datform.dathasloginevt,
        );
        // pass the database name back to the caller
        if let Some(out) = out_dbname {
            *out = dbname.clone();
        }
    }

    // Now we are certain to be connected to a database and can set MyDatabaseId.
    init_small_seams::set_my_database_id::call(dboid);

    // Now we can mark our PGPROC entry with the database ID.
    lmgr_proc_seams::set_my_proc_database_id::call(dboid);

    // We established a catalog snapshot while reading pg_authid and/or
    // pg_database; assume it's no good anymore.
    snapmgr_seams::invalidate_catalog_snapshot::call();

    // Now we should be able to access the database directory safely.
    let my_database_id = init_small_seams::my_database_id::call();
    let my_database_tablespace = init_small_seams::my_database_table_space::call();
    let fullpath =
        backend_common_relpath_seams::get_database_path::call(mcx, my_database_id, my_database_tablespace)?;

    if !bootstrap {
        match fd_seams::access_f_ok::call(fullpath.as_str())? {
            fd_seams::AccessResult::Ok => {}
            fd_seams::AccessResult::NoEnt => {
                return ereport(FATAL)
                    .errcode(ERRCODE_UNDEFINED_DATABASE)
                    .errmsg(format!("database \"{dbname}\" does not exist"))
                    .errdetail(format!(
                        "The database subdirectory \"{}\" is missing.",
                        fullpath.as_str()
                    ))
                    .finish(loc(1151, "InitPostgres"));
            }
            fd_seams::AccessResult::Other(errno) => {
                return ereport(FATAL)
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not access directory \"{}\": {}",
                        fullpath.as_str(),
                        os_error_string(errno)
                    ))
                    .finish(loc(1158, "InitPostgres"));
            }
        }

        miscinit_seams::validate_pg_version::call(fullpath.as_str())?;
    }

    miscinit_seams::set_database_path_once::call(fullpath.as_str());

    // It's now possible to do real access to the system catalogs.
    relcache_seams::relation_cache_initialize_phase3::call()?;

    // set up ACL framework (so CheckMyDatabase can check permissions)
    acl_seams::initialize_acl::call()?;

    // Re-read the pg_database row, check permissions, set up GUC settings.
    if !bootstrap {
        CheckMyDatabase(
            mcx,
            &dbname,
            am_superuser,
            (flags & INIT_PG_OVERRIDE_ALLOW_CONNS) != 0,
        )?;
    }

    // Now process any command-line switches and additional GUC settings.
    if init_small_seams::has_my_proc_port::call() {
        process_startup_options(mcx, am_superuser)?;
    }

    // Process pg_db_role_setting options.
    let my_database_id = init_small_seams::my_database_id::call();
    let session_user_id = miscinit_seams::get_session_user_id::call();
    process_settings(mcx, my_database_id, session_user_id)?;

    // Apply PostAuthDelay as soon as we've read all options.
    let post_auth_delay = postgres_seams::post_auth_delay::call();
    if post_auth_delay > 0 {
        pgsleep_seams::pg_usleep::call(post_auth_delay as i64 * 1_000_000);
    }

    // Initialize various default states.

    // set default namespace search path
    catalog_namespace::InitializeSearchPath()?;

    // initialize client encoding
    mbutils_seams::initialize_client_encoding::call()?;

    // Initialize this backend's session state.
    session_seams::initialize_session::call()?;

    // If this is an interactive session, load any preloaded libraries.
    if (flags & INIT_PG_LOAD_SESSION_LIBS) != 0 {
        miscinit_seams::process_session_preload_libraries::call(mcx)?;
    }

    // fill in the remainder of this entry in the PgBackendStatus array
    if !bootstrap {
        status_seams::pgstat_bestart_final::call()?;
    }

    // close the transaction we started above
    if !bootstrap {
        transam_xact_seams::commit_transaction_command::call()?;
    }

    Ok(())
}

/// `process_startup_options` — process command-line switches and additional GUC
/// settings passed in the startup packet.
fn process_startup_options(mcx: Mcx<'_>, am_superuser: bool) -> PgResult<()> {
    let gucctx = if am_superuser {
        GucContext::PGC_SU_BACKEND
    } else {
        GucContext::PGC_BACKEND
    };

    // Read the Port fields the C reads (port->cmdline_options, port->guc_options)
    // through init-small's MyProcPort accessor.
    let cmdline_options = init_small_seams::my_proc_port_cmdline_options::call(mcx)?;
    let guc_options = init_small_seams::my_proc_port_guc_options::call(mcx)?;

    // First process any command-line switches included in the startup packet.
    if let Some(cmdline_options) = cmdline_options {
        let cmdline_options = cmdline_options.as_str();
        // Max possible number of args is (strlen + 1) / 2; see pg_split_opts().
        let maxac = 2 + (cmdline_options.len() + 1) / 2;

        let mut av: Vec<String> = Vec::new();
        av.try_reserve(maxac).map_err(reserve_failed)?;
        let mut ac: usize = 0;

        av.push("postgres".to_string());
        ac += 1;

        pg_split_opts(mcx, &mut av, &mut ac, cmdline_options)?;

        debug_assert!(ac < maxac);

        postgres_seams::process_postgres_switches::call(&av[..ac], gucctx)?;
    }

    // Process any additional GUC variable settings passed in the startup packet.
    // These are handled exactly like command-line variables.
    let mut it = guc_options.iter();
    while let Some(name) = it.next() {
        // The startup-packet GUC options arrive as (name, value) pairs, so the
        // value is always present (mirrors C's `lnext` walk in postinit.c).
        let value = it.next().expect("guc_options must contain name/value pairs");
        guc_seams::set_config_option::call(
            name.as_str(),
            value.as_str(),
            gucctx,
            GucSource::PGC_S_CLIENT,
        )?;
    }

    Ok(())
}

/// `process_settings` — load GUC settings from pg_db_role_setting.
///
/// Try specific settings for the database/role combination, as well as general
/// for this database and for this user.
fn process_settings(mcx: Mcx<'_>, databaseid: Oid, roleid: Oid) -> PgResult<()> {
    if !init_small_seams::is_under_postmaster::call() {
        return Ok(());
    }

    // table_open(DbRoleSettingRelationId, AccessShareLock),
    // RegisterSnapshot(GetCatalogSnapshot(DbRoleSettingRelationId)), the four
    // ApplySetting() calls in scope order, UnregisterSnapshot, and table_close
    // — the relsetting/snapshot/scan machinery is owned by the seam.
    pg_db_role_setting_seams::apply_db_role_settings::call(mcx, databaseid, roleid)
}

/// `ShutdownPostgres` — backend-shutdown callback. Cleanup that must happen
/// before the supporting modules begin to nail their doors shut.
pub fn ShutdownPostgres(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    // Make sure we've killed any active transaction.
    transam_xact_seams::abort_out_of_any_transaction::call()?;

    // User locks are not released by transaction end; release them explicitly.
    // LockReleaseAll(USER_LOCKMETHOD, true)
    lock_seams::lock_release_all_user::call()?;

    Ok(())
}

/// `before_shmem_exit(ShutdownPostgres, 0)` callback shape.
fn shutdown_postgres_cb(code: i32, arg: types_tuple::Datum<'static>) -> PgResult<()> {
    ShutdownPostgres(code, arg)
}

/// `before_shmem_exit(ShutdownXLOG, 0)` — delegate to xlog.c's exit callback.
///
/// Both the `before_shmem_exit` seam and the xlog `ShutdownXLOG` seam now carry
/// the canonical unified `types_tuple::Datum<'static>`, so the arg forwards
/// directly.
fn shutdown_xlog_cb(code: i32, arg: types_tuple::Datum<'static>) -> PgResult<()> {
    transam_xlog::ShutdownXLOG(code, arg);
    Ok(())
}

/// `before_shmem_exit(pgstat_before_server_shutdown, 0)` — delegate to pgstat.
///
/// Both the `before_shmem_exit` seam and the pgstat seam now carry the canonical
/// unified `types_tuple::Datum<'static>`, so the arg forwards directly.
fn pgstat_before_server_shutdown_cb(code: i32, arg: types_tuple::Datum<'static>) -> PgResult<()> {
    pgstat_seams::pgstat_before_server_shutdown::call(code, arg)
}

// ---------------------------------------------------------------------------
// Timeout handlers. Registered as `fn()` (TimeoutHandlerProc). They run in the
// timeout-manager context and just set flags / signal; the C bodies set
// `volatile sig_atomic_t` pending flags and SetLatch(MyLatch), or kill().
// ---------------------------------------------------------------------------

/// `StatementTimeoutHandler` — STATEMENT_TIMEOUT handler: trigger a
/// query-cancel interrupt.
pub fn StatementTimeoutHandler() {
    let mut sig = libc::SIGINT;

    // During authentication the timeout is used to deal with
    // authentication_timeout — we want to quit in response to such timeouts.
    if postmaster_seams::client_auth_in_progress::call() {
        sig = libc::SIGTERM;
    }

    // HAVE_SETSID: signal the whole process group, then self.
    let pid = init_small_seams::my_proc_pid::call();
    kill_pgrp_and_self(pid, sig);
}

/// `LockTimeoutHandler` — LOCK_TIMEOUT handler: trigger a query-cancel interrupt.
pub fn LockTimeoutHandler() {
    let pid = init_small_seams::my_proc_pid::call();
    kill_pgrp_and_self(pid, libc::SIGINT);
}

/// `TransactionTimeoutHandler`.
pub fn TransactionTimeoutHandler() {
    postgres_seams::set_transaction_timeout_pending::call(true);
    init_small_seams::set_interrupt_pending::call(true);
    latch_seams::set_latch_my_latch::call();
}

/// `IdleInTransactionSessionTimeoutHandler`.
pub fn IdleInTransactionSessionTimeoutHandler() {
    postgres_seams::set_idle_in_transaction_session_timeout_pending::call(true);
    init_small_seams::set_interrupt_pending::call(true);
    latch_seams::set_latch_my_latch::call();
}

/// `IdleSessionTimeoutHandler`.
pub fn IdleSessionTimeoutHandler() {
    postgres_seams::set_idle_session_timeout_pending::call(true);
    init_small_seams::set_interrupt_pending::call(true);
    latch_seams::set_latch_my_latch::call();
}

/// `IdleStatsUpdateTimeoutHandler`.
pub fn IdleStatsUpdateTimeoutHandler() {
    postgres_seams::set_idle_stats_update_timeout_pending::call(true);
    init_small_seams::set_interrupt_pending::call(true);
    latch_seams::set_latch_my_latch::call();
}

/// `ClientCheckTimeoutHandler`.
pub fn ClientCheckTimeoutHandler() {
    postgres_seams::set_check_client_connection_pending::call(true);
    init_small_seams::set_interrupt_pending::call(true);
    latch_seams::set_latch_my_latch::call();
}

/// `RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert)` — proc.c owns the
/// deadlock-check handler; register a `fn()` that delegates to it.
fn check_dead_lock_alert_cb() {
    lmgr_proc_seams::check_dead_lock_alert::call();
}

/// `ThereIsAtLeastOneRole(void)` (postinit.c): returns true if at least one
/// role is defined.
///
/// ```c
/// pg_authid_rel = table_open(AuthIdRelationId, AccessShareLock);
/// scan = table_beginscan_catalog(pg_authid_rel, 0, NULL);
/// result = (heap_getnext(scan, ForwardScanDirection) != NULL);
/// table_endscan(scan);
/// table_close(pg_authid_rel, AccessShareLock);
/// return result;
/// ```
///
/// The catalog heap scan is expressed through `systable_beginscan` with
/// `index_ok = false` and no keys: that opens no index and runs
/// `table_beginscan_strat(..., allow_sync = false)`, exactly what
/// `table_beginscan_catalog(rel, 0, NULL)` does. `heap_getnext != NULL` is the
/// first `systable_getnext` returning `Some`.
fn ThereIsAtLeastOneRole(mcx: Mcx<'_>) -> PgResult<bool> {
    // pg_authid_rel = table_open(AuthIdRelationId, AccessShareLock);
    let pg_authid_rel = table::table_open(
        mcx,
        ::types_catalog::catalog::AUTH_ID_RELATION_ID,
        types_storage::lock::AccessShareLock,
    )?;

    // scan = table_beginscan_catalog(pg_authid_rel, 0, NULL);
    let mut scan = genam_seams::systable_beginscan::call(
        &pg_authid_rel,
        InvalidOid,
        false,
        None,
        &[],
    )?;

    // result = (heap_getnext(scan, ForwardScanDirection) != NULL);
    let row_mcx = MemoryContext::new("ThereIsAtLeastOneRole row");
    let result =
        genam_seams::systable_getnext::call(row_mcx.mcx(), scan.desc_mut())?
            .is_some();

    // table_endscan(scan);
    scan.end()?;

    // table_close(pg_authid_rel, AccessShareLock);
    pg_authid_rel.close(types_storage::lock::AccessShareLock)?;

    Ok(result)
}

// ---------------------------------------------------------------------------
// Small helpers used by the orchestration.
// ---------------------------------------------------------------------------

/// `#ifdef HAVE_SETSID kill(-MyProcPid, sig); #endif kill(MyProcPid, sig);`
fn kill_pgrp_and_self(pid: i32, sig: i32) {
    // HAVE_SETSID: try to signal the whole process group, then self.
    unsafe {
        libc::kill(-pid, sig);
        libc::kill(pid, sig);
    }
}

/// `strlcpy(dbname, src, NAMEDATALEN)` — copy at most NAMEDATALEN-1 bytes.
fn strlcpy_name(src: &str) -> String {
    if src.len() <= NAMEDATALEN - 1 {
        src.to_string()
    } else {
        // strlcpy is byte-oriented; a NameData is ASCII/UTF-8 already.
        String::from_utf8_lossy(&src.as_bytes()[..NAMEDATALEN - 1]).into_owned()
    }
}

/// Render an errno into the `%m`-style string `errmsg("... %m")` would produce.
fn os_error_string(errno: i32) -> String {
    std::io::Error::from_raw_os_error(errno).to_string()
}

/// Map a `TryReserveError` onto `ERRCODE_OUT_OF_MEMORY` (recoverable), rather
/// than aborting the process.
fn reserve_failed(_: std::collections::TryReserveError) -> PgError {
    PgError::new(ERROR, "out of memory").with_sqlstate(::types_error::ERRCODE_OUT_OF_MEMORY)
}

/// Install this crate's seam implementations.
///
/// `backend-bootstrap-bootstrap` calls back into postinit's
/// `InitializeMaxBackends`/`InitializeFastPathLocks`/`BaseInit`/`InitPostgres`
/// (a real dependency cycle: bootstrap.c's `BootstrapModeMain` drives these
/// per-backend init steps). Those four entry points are declared in
/// `backend-utils-init-postinit-seams` and installed here.
pub fn init_seams() {
    postinit_seams::initialize_max_backends::set(InitializeMaxBackends);
    postinit_seams::initialize_fast_path_locks::set(|| {
        // C InitializeFastPathLocks is void; the port returns PgResult only to
        // carry the debug asserts. The seam contract is infallible, matching C,
        // so a panic here would only fire on the impossible already-initialized
        // assertion. Unwrap to honor the infallible signature.
        InitializeFastPathLocks().expect("InitializeFastPathLocks is infallible");
    });
    postinit_seams::base_init::set(BaseInit);
    postinit_seams::init_postgres_bootstrap::set(|mcx| {
        // C: InitPostgres(NULL, InvalidOid, NULL, InvalidOid, 0, NULL).
        InitPostgres(mcx, None, InvalidOid, None, InvalidOid, 0, None)
    });
    postinit_seams::init_postgres_by_name::set(
        |dbname, username, init_flags| {
            // C: InitPostgres(dbname, InvalidOid, username, InvalidOid,
            // init_flags, NULL) — the background-worker by-name attach.
            // Unlike the bootstrap seam, the consumer
            // (BackgroundWorkerInitializeConnection) carries no Mcx, matching
            // C where InitPostgres allocates through TopMemoryContext rather
            // than a passed-in context; supply that handle here.
            InitPostgres(
                top_memory_context(),
                dbname,
                InvalidOid,
                username,
                InvalidOid,
                init_flags,
                None,
            )
        },
    );
    postinit_seams::init_postgres_by_oid::set(|dboid, useroid, init_flags| {
        // C: InitPostgres(NULL, dboid, NULL, useroid, init_flags, NULL) — the
        // background-worker by-OID attach. See init_postgres_by_name for why
        // TopMemoryContext is sourced here.
        InitPostgres(
            top_memory_context(),
            None,
            dboid,
            None,
            useroid,
            init_flags,
            None,
        )
    });
}

thread_local! {
    /// The per-backend `TopMemoryContext`-equivalent handle. In C,
    /// `InitPostgres` allocates in `TopMemoryContext` (process-lifetime), which
    /// is established before the background worker's connection-attach path
    /// runs. The by-name/by-OID seams declare no `Mcx` argument (mirroring the
    /// C signature, where the context is implicit), so the owner supplies the
    /// process-lifetime handle here. Leaked once per backend so the resulting
    /// `Mcx<'static>` lives for the whole process, exactly like C's
    /// `TopMemoryContext`.
    static TOP_MEMORY_CONTEXT: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("TopMemoryContext")));
}

/// The process-lifetime `Mcx<'static>` standing in for C's `TopMemoryContext`,
/// used by the `init_postgres_by_name`/`init_postgres_by_oid` seam delegates.
fn top_memory_context() -> Mcx<'static> {
    TOP_MEMORY_CONTEXT.with(|ctx| ctx.mcx())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nextpower2_matches_c() {
        // pg_nextpower2_32: smallest power of two >= num (num already-pow2 stays).
        assert_eq!(pg_nextpower2_32(1), 1);
        assert_eq!(pg_nextpower2_32(2), 2);
        assert_eq!(pg_nextpower2_32(3), 4);
        assert_eq!(pg_nextpower2_32(5), 8);
        assert_eq!(pg_nextpower2_32(64), 64);
        assert_eq!(pg_nextpower2_32(65), 128);
        assert_eq!(pg_nextpower2_32(1023), 1024);
        assert_eq!(pg_nextpower2_32(1024), 1024);
    }

    #[test]
    fn fastpath_default_is_four_groups() {
        // max_locks_per_transaction default 64 => 4 groups
        // (pg_nextpower2_32(64)/16 = 4), capped at FP_LOCK_GROUPS_PER_BACKEND_MAX.
        let value = pg_max(
            pg_min(
                (pg_nextpower2_32(64) as i32) / FP_LOCK_SLOTS_PER_GROUP,
                FP_LOCK_GROUPS_PER_BACKEND_MAX,
            ),
            1,
        );
        assert_eq!(value, 4);
        assert_eq!(value, pg_nextpower2_32(value as u32) as i32);
    }

    #[test]
    fn isspace_matches_c_locale() {
        for c in [b' ', b'\t', b'\n', 0x0b, 0x0c, b'\r'] {
            assert!(c_isspace(c));
        }
        for c in [b'a', b'0', b'\\', 0u8, 0x1f] {
            assert!(!c_isspace(c));
        }
    }

    #[test]
    fn constants_match_headers() {
        assert_eq!(TEMPLATE1_DB_OID, 1);
        assert_eq!(DEFAULTTABLESPACE_OID, 1663);
        assert_eq!(ROLE_PG_USE_RESERVED_CONNECTIONS, 4550);
        assert_eq!(MAX_BACKENDS, (1 << 18) - 1);
        assert_eq!(NUM_SPECIAL_WORKER_PROCS, 2);
        assert_eq!(FP_LOCK_GROUPS_PER_BACKEND_MAX, 1024);
        assert_eq!(FP_LOCK_SLOTS_PER_GROUP, 16);
        assert_eq!(INIT_PG_LOAD_SESSION_LIBS, 0x0001);
        assert_eq!(INIT_PG_OVERRIDE_ALLOW_CONNS, 0x0002);
        assert_eq!(INIT_PG_OVERRIDE_ROLE_LOGIN, 0x0004);
    }

    #[test]
    fn strlcpy_truncates_to_namedatalen() {
        let s = "short";
        assert_eq!(strlcpy_name(s), "short");
        let long: String = std::iter::repeat('x').take(100).collect();
        assert_eq!(strlcpy_name(&long).len(), NAMEDATALEN - 1);
    }
}
