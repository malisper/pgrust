//! `backend_startup.c` — backend startup code
//! (`src/backend/tcop/backend_startup.c`, PostgreSQL 18.3).
//!
//! The postmaster-child entry point: `BackendMain` initializes the connection,
//! reads and acts on the startup packet (the SSL/GSS negotiation state machine
//! and cancel-request handling included), rejects the connection if the
//! database state forbids it, builds the ps title, and hands off to
//! `PostgresMain`.
//!
//! Every subsystem crossing goes through the owning crate — directly where
//! acyclic, otherwise through the owner's `-seams` crate (which panics until
//! the owner lands). The logic owned by `backend_startup.c` lives here in full.

#![allow(clippy::result_large_err)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::too_many_arguments)]

use backend_utils_error::ereport;
use mcx::{MemoryContext, Mcx};
use types_core::{init::BackendType, ProtocolVersion, TimestampTz};
use types_error::{
    ErrorLocation, PgResult, COMMERROR, ERRCODE_CANNOT_CONNECT_NOW, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_PROTOCOL_VIOLATION, ERRCODE_TOO_MANY_CONNECTIONS, FATAL, LOG, WARNING,
};
use types_net::{Port, SockError};
use types_startup::{BackendStartupData, CacState, StartupData};
use types_timeout::TimeoutId;

mod globals;

pub use globals::{conn_timing, log_connections, trace_connection_negotiation};

// ===========================================================================
//  Constants — must match the C headers exactly.
// ===========================================================================

/// `STATUS_OK` (`c.h`).
pub const STATUS_OK: i32 = 0;
/// `STATUS_ERROR` (`c.h`).
pub const STATUS_ERROR: i32 = -1;

/// `MAX_STARTUP_PACKET_LENGTH` (`pqcomm.h`).
pub const MAX_STARTUP_PACKET_LENGTH: i32 = 10000;

/// `NAMEDATALEN` (`pg_config_manual.h`).
pub const NAMEDATALEN: usize = 64;

/// `PGPROC_MAX_CACHED_SUBXIDS` (`storage/proc.h`).
pub const PGPROC_MAX_CACHED_SUBXIDS: i32 = 64;

/// `PG_PROTOCOL(m, n)` (`pqcomm.h`).
pub const fn pg_protocol(m: u32, n: u32) -> ProtocolVersion {
    (m << 16) | n
}
/// `PG_PROTOCOL_MAJOR(v)` (`pqcomm.h`).
pub const fn pg_protocol_major(v: ProtocolVersion) -> u32 {
    v >> 16
}
/// `PG_PROTOCOL_MINOR(v)` (`pqcomm.h`).
pub const fn pg_protocol_minor(v: ProtocolVersion) -> u32 {
    v & 0x0000_ffff
}

/// `PG_PROTOCOL_EARLIEST` = `PG_PROTOCOL(3, 0)` (`pqcomm.h`).
pub const PG_PROTOCOL_EARLIEST: ProtocolVersion = pg_protocol(3, 0);
/// `PG_PROTOCOL_LATEST` = `PG_PROTOCOL(3, 2)` (`pqcomm.h`).
pub const PG_PROTOCOL_LATEST: ProtocolVersion = pg_protocol(3, 2);

/// `CANCEL_REQUEST_CODE` = `PG_PROTOCOL(1234, 5678)` (`pqcomm.h`).
pub const CANCEL_REQUEST_CODE: ProtocolVersion = pg_protocol(1234, 5678);
/// `NEGOTIATE_SSL_CODE` = `PG_PROTOCOL(1234, 5679)` (`pqcomm.h`).
pub const NEGOTIATE_SSL_CODE: ProtocolVersion = pg_protocol(1234, 5679);
/// `NEGOTIATE_GSS_CODE` = `PG_PROTOCOL(1234, 5680)` (`pqcomm.h`).
pub const NEGOTIATE_GSS_CODE: ProtocolVersion = pg_protocol(1234, 5680);

/// `sizeof(ProtocolVersion)` (== 4).
const SIZEOF_PROTOCOL_VERSION: i32 = core::mem::size_of::<ProtocolVersion>() as i32;

/// `PqMsg_NegotiateProtocolVersion` (`libpq/protocol.h`) == `'v'`.
const PQMSG_NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';

/// `offsetof(CancelRequestPacket, cancelAuthCode)` (`pqcomm.h`): a 4-byte
/// `cancelRequestCode` followed by a 4-byte `backendPID`.
const CANCEL_AUTH_CODE_OFFSET: usize = 8;

// ---------------------------------------------------------------------------
//  log_connections aspect flags (backend_startup.h LogConnectionOption).
// ---------------------------------------------------------------------------

/// `LOG_CONNECTION_RECEIPT = (1 << 0)`.
pub const LOG_CONNECTION_RECEIPT: u32 = 1 << 0;
/// `LOG_CONNECTION_AUTHENTICATION = (1 << 1)`.
pub const LOG_CONNECTION_AUTHENTICATION: u32 = 1 << 1;
/// `LOG_CONNECTION_AUTHORIZATION = (1 << 2)`.
pub const LOG_CONNECTION_AUTHORIZATION: u32 = 1 << 2;
/// `LOG_CONNECTION_SETUP_DURATIONS = (1 << 3)`.
pub const LOG_CONNECTION_SETUP_DURATIONS: u32 = 1 << 3;
/// `LOG_CONNECTION_ON` (RECEIPT | AUTHENTICATION | AUTHORIZATION).
pub const LOG_CONNECTION_ON: u32 =
    LOG_CONNECTION_RECEIPT | LOG_CONNECTION_AUTHENTICATION | LOG_CONNECTION_AUTHORIZATION;
/// `LOG_CONNECTION_ALL` (the above plus SETUP_DURATIONS).
pub const LOG_CONNECTION_ALL: u32 = LOG_CONNECTION_RECEIPT
    | LOG_CONNECTION_AUTHENTICATION
    | LOG_CONNECTION_AUTHORIZATION
    | LOG_CONNECTION_SETUP_DURATIONS;

/// `EOF` sentinel returned by `pq_peekbyte` / used by `pq_getbytes`.
const EOF: i32 = -1;
/// `EINTR` (errno).
const EINTR: i32 = libc::EINTR;
/// `NI_NUMERICHOST` (`netdb.h`).
const NI_NUMERICHOST: i32 = 1;
/// `NI_NUMERICSERV` (`netdb.h`).
const NI_NUMERICSERV: i32 = 8;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("backend_startup.c", line, func)
}

// ===========================================================================
//  BackendMain (line 76)
// ===========================================================================

/// `BackendMain(startup_data, startup_data_len)` (backend_startup.c:76-125) —
/// entry point for a new backend process. Never returns: it ends in the
/// `PostgresMain` hand-off (or `proc_exit` on a rejected/cancel packet inside
/// `BackendInitialize`).
///
/// The `EXEC_BACKEND`-only SSL reinitialization block is not ported (this repo
/// targets the fork model). The backend's long-lived context (C's
/// `TopMemoryContext`, into which `pq_init` and the startup-packet strings are
/// allocated) is created here and threaded through the call tree as `Mcx`.
pub fn backend_main(startup_data: &StartupData) -> ! {
    // const BackendStartupData *bsdata = startup_data;
    // Assert(startup_data_len == sizeof(BackendStartupData));
    let bsdata: BackendStartupData = match startup_data {
        StartupData::Backend(b) => *b,
        StartupData::None | StartupData::BgWorker(_) => {
            unreachable!("BackendMain requires a BackendStartupData payload (StartupData::Backend)")
        }
    };

    // Transfer the launch timings the postmaster recorded into conn_timing.
    conn_timing::set_socket_create(bsdata.socket_created);
    conn_timing::set_fork_start(bsdata.fork_started);

    // The backend's long-lived allocation context.
    let top = MemoryContext::new("TopMemoryContext");
    let mcx = top.mcx();

    // Run the rest of backend startup + the main loop under a top-level unwind
    // backstop. `PostgresMain` has its own catch that turns a setup-phase panic
    // (e.g. `InitPostgres` cache-init on a `\c`-reconnect) into a reported FATAL,
    // but the steps *before* it here — `backend_initialize` (auth / Port setup)
    // and `InitProcess` — plus any path that re-panics during PostgresMain are
    // not covered by it. A bare panic escaping this `-> !` frame unwinds to the
    // process top and exits 101, which the postmaster's `CleanupBackend`
    // classifies as a crash (status != 0 && != 1) → `HandleChildCrash` → crash
    // recovery; pgrust's `StartupXLOG` cannot replay a crashed datadir, so the
    // postmaster then wedges forever in "the database system is in recovery
    // mode", refusing every new connection (the file-level TIMEOUT hang). Catch
    // the escape and `proc_exit(1)` cleanly — a normal FATAL disconnect that
    // never provokes crash recovery, mirroring C where any unrecoverable backend
    // error is `ereport(FATAL)` (clean exit), never an abnormal termination.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // BackendInitialize(MyClientSocket, bsdata->canAcceptConnections);
        let client_sock = backend_utils_init_small_seams::my_client_socket::call()
            .expect("MyClientSocket must be set before BackendMain (postmaster_child_launch)");
        backend_initialize(mcx, client_sock, bsdata.can_accept_connections);

        // InitProcess(): create a per-backend PGPROC in shared memory.
        backend_storage_lmgr_proc_seams::init_process::call();

        // MemoryContextSwitchTo(TopMemoryContext): explicit-Mcx threading;
        // nothing to switch.

        // PostgresMain(MyProcPort->database_name, MyProcPort->user_name);
        let (dbname, username) = read_proc_port_names();
        backend_tcop_postgres_seams::postgres_main::call(
            dbname.as_deref(),
            username.as_deref(),
        )
    }));
    // The closure body diverges (PostgresMain is `-> !`), so `Ok` is
    // unreachable; only an escaped panic reaches here.
    match outcome {
        Ok(never) => never,
        Err(_payload) => {
            let my_pid = backend_utils_init_small_seams::my_proc_pid::call();
            backend_storage_ipc_dsm_core_seams::proc_exit::call(1, my_pid)
        }
    }
}

/// `PostgresMain(MyProcPort->database_name, MyProcPort->user_name)` argument
/// read — pulls the two names off the `Port` held in `MyProcPort`.
fn read_proc_port_names() -> (Option<String>, Option<String>) {
    let mut out = (None, None);
    backend_utils_init_small_seams::with_my_proc_port::call(&mut |port| {
        if let Some(p) = port {
            out = (p.database_name.clone(), p.user_name.clone());
        }
    });
    out
}

// ===========================================================================
//  BackendInitialize (line 141)
// ===========================================================================

/// `BackendInitialize(client_sock, cac)` (backend_startup.c:141-392) —
/// initialize an interactive (postmaster-child) backend process and collect
/// the client's startup packet. Will not return at all on failure (`proc_exit`
/// or `ereport(FATAL)`).
///
/// C returns `void` and terminates on failure; the explicit-error model raises
/// `Err` internally, and this wrapper drives the `FATAL` report cycle (which
/// `proc_exit`s) to honor the C "does not return on failure" contract.
fn backend_initialize(mcx: Mcx<'_>, client_sock: types_net::ClientSocket, cac: CacState) {
    if let Err(e) = backend_initialize_inner(mcx, client_sock, cac) {
        // ThrowErrorData on FATAL/ERROR runs the report cycle; FATAL proc_exits
        // and never returns.
        let _ = backend_utils_error::ThrowErrorData(e);
        let my_pid = backend_utils_init_small_seams::my_proc_pid::call();
        backend_storage_ipc_dsm_core_seams::proc_exit::call(1, my_pid);
    }
}

fn backend_initialize_inner(
    mcx: Mcx<'_>,
    client_sock: types_net::ClientSocket,
    cac: CacState,
) -> PgResult<()> {
    // ReserveExternalFD(): tell fd.c about the long-lived client FD.
    backend_storage_file_seams::reserve_external_fd::call();

    // if (PreAuthDelay > 0) pg_usleep(PreAuthDelay * 1000000L);
    let pre_auth_delay = backend_postmaster_postmaster_seams::pre_auth_delay::call();
    if pre_auth_delay > 0 {
        port_pgsleep_seams::pg_usleep::call(pre_auth_delay as i64 * 1_000_000);
    }

    // ClientAuthInProgress = true;
    backend_utils_error::config::set_client_auth_in_progress(true);

    // port = MyProcPort = pq_init(client_sock);
    //
    // pq_init allocates the Port, applies TCP options, initializes the message
    // buffers, registers a socket on_proc_exit, and builds FeBeWaitSet (reading
    // C's MyLatch, passed explicitly here per the no-ambient-global rule). It
    // returns the Port; we install it as MyProcPort, then read/mutate it
    // through `with_my_proc_port`.
    let my_latch = backend_storage_ipc_latch_seams::my_latch::call();
    let port = backend_libpq_pqcomm_seams::pq_init::call(&client_sock, my_latch)?;
    backend_utils_init_small_seams::set_my_proc_port::call(port);

    // whereToSendOutput = DestRemote; (now safe to ereport to client)
    backend_utils_error::config::set_where_to_send_output(types_dest::CommandDest::Remote);

    // port->remote_host = ""; port->remote_port = "";
    with_proc_port(|port| {
        port.remote_host = Some(String::new());
        port.remote_port = Some(String::new());
    });

    // pqsignal(SIGTERM, process_startup_packet_die);
    arm_startup_packet_signals();

    // InitializeTimeouts(); establishes SIGALRM handler.
    backend_utils_misc_timeout_seams::initialize_timeouts::call();

    // sigprocmask(SIG_SETMASK, &StartupBlockSig, NULL);
    let masks = backend_libpq_pqsignal::signal_masks();
    set_signal_mask(masks.startup_block_sig());

    // pg_getnameinfo_all(...) into remote_host / remote_port.
    let log_hostname = backend_postmaster_postmaster_seams::log_hostname::call();
    let (raddr, getname_flags) = with_proc_port(|port| {
        // (log_hostname ? 0 : NI_NUMERICHOST) | NI_NUMERICSERV
        let flags = (if log_hostname { 0 } else { NI_NUMERICHOST }) | NI_NUMERICSERV;
        (port.raddr, flags)
    });
    let mut remote_host = String::new();
    let mut remote_port = String::new();
    let ret = common_ip_seams::pg_getnameinfo_all::call(
        &raddr,
        Some(&mut remote_host),
        Some(&mut remote_port),
        getname_flags,
    );
    if ret != 0 {
        let gai = gai_strerror(ret);
        ereport(WARNING)
            .errmsg_internal(format!("pg_getnameinfo_all() failed: {gai}"))
            .finish(loc(211, "BackendInitialize"))?;
    }

    // Save remote_host / remote_port into the Port (MemoryContextStrdup).
    {
        let rh = remote_host.clone();
        let rp = remote_port.clone();
        with_proc_port(|port| {
            port.remote_host = Some(rh.clone());
            port.remote_port = Some(rp.clone());
        });
    }

    // if (log_connections & LOG_CONNECTION_RECEIPT) { ... }
    if log_connections::get() & LOG_CONNECTION_RECEIPT != 0 {
        if !remote_port.is_empty() {
            ereport(LOG)
                .errmsg(format!(
                    "connection received: host={remote_host} port={remote_port}"
                ))
                .finish(loc(226, "BackendInitialize"))?;
        } else {
            ereport(LOG)
                .errmsg(format!("connection received: host={remote_host}"))
                .finish(loc(231, "BackendInitialize"))?;
        }
    }

    // USE_INJECTION_POINTS block (backend-initialize / -v2-error): the
    // injection-point subsystem is compiled out by default; not ported.

    // Reverse-lookup save: keep remote_hostname only if it isn't a numeric
    // IPv4/IPv6 literal.
    if log_hostname
        && ret == 0
        && strspn(&remote_host, b"0123456789.") < remote_host.len()
        && strspn(&remote_host, b"0123456789ABCDEFabcdef:") < remote_host.len()
    {
        let rh = remote_host.clone();
        with_proc_port(|port| port.remote_hostname = Some(rh.clone()));
    }

    // RegisterTimeout(STARTUP_PACKET_TIMEOUT, StartupPacketTimeoutHandler);
    backend_utils_misc_timeout_seams::register_timeout::call(
        TimeoutId::STARTUP_PACKET_TIMEOUT,
        startup_packet_timeout_handler,
    );
    // enable_timeout_after(STARTUP_PACKET_TIMEOUT, AuthenticationTimeout * 1000);
    let auth_timeout = backend_postmaster_postmaster_seams::authentication_timeout::call();
    backend_utils_misc_timeout_seams::enable_timeout_after::call(
        TimeoutId::STARTUP_PACKET_TIMEOUT,
        auth_timeout * 1000,
    )?;

    // status = ProcessSSLStartup(port);
    let mut status = process_ssl_startup()?;

    // if (status == STATUS_OK) status = ProcessStartupPacket(port, false, false);
    if status == STATUS_OK {
        status = process_startup_packet(mcx, false, false)?;
    }

    // Reject connection due to database state, if applicable.
    if status == STATUS_OK {
        reject_for_cac_state(cac)?;
    }

    // disable_timeout(STARTUP_PACKET_TIMEOUT, false);
    backend_utils_misc_timeout_seams::disable_timeout::call(
        TimeoutId::STARTUP_PACKET_TIMEOUT,
        false,
    );
    // sigprocmask(SIG_SETMASK, &BlockSig, NULL);
    let masks = backend_libpq_pqsignal::signal_masks();
    set_signal_mask(masks.block_sig());

    // check_on_shmem_exit_lists_are_empty();
    backend_storage_ipc_dsm_core_seams::check_on_shmem_exit_lists_are_empty::call()?;

    // if (status != STATUS_OK) proc_exit(0);
    if status != STATUS_OK {
        let my_pid = backend_utils_init_small_seams::my_proc_pid::call();
        backend_storage_ipc_dsm_core_seams::proc_exit::call(0, my_pid);
    }

    // Build the ps title now that we have user/database names.
    build_ps_title();

    Ok(())
}

/// The CAC-state acceptance switch (backend_startup.c:304-347). Every
/// non-`CAC_OK` state is an `ereport(FATAL)` (returned as `Err`).
fn reject_for_cac_state(cac: CacState) -> PgResult<()> {
    let err = match cac {
        CacState::Startup => ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg("the database system is starting up")
            .into_error(),
        CacState::NotHotStandby => {
            let enable_hot_standby = backend_access_transam_xlog_seams::enable_hot_standby::call();
            if !enable_hot_standby {
                ereport(FATAL)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg("the database system is not accepting connections")
                    .errdetail("Hot standby mode is disabled.")
                    .into_error()
            } else if backend_access_transam_xlogrecovery_seams::reached_consistency::call() {
                ereport(FATAL)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg("the database system is not yet accepting connections")
                    .errdetail("Recovery snapshot is not yet ready for hot standby.")
                    .errhint(format!(
                        "To enable hot standby, close write transactions with more than {} subtransactions on the primary server.",
                        PGPROC_MAX_CACHED_SUBXIDS
                    ))
                    .into_error()
            } else {
                ereport(FATAL)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg("the database system is not yet accepting connections")
                    .errdetail("Consistent recovery state has not been yet reached.")
                    .into_error()
            }
        }
        CacState::Shutdown => ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg("the database system is shutting down")
            .into_error(),
        CacState::Recovery => ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg("the database system is in recovery mode")
            .into_error(),
        CacState::TooMany => ereport(FATAL)
            .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
            .errmsg("sorry, too many clients already")
            .into_error(),
        CacState::Ok => return Ok(()),
    };
    Err(err)
}

/// `initStringInfo(&ps_data); ...; init_ps_display(ps_data.data); set_ps_display("initializing");`
/// (backend_startup.c:378-391). `appendStringInfo` is infallible string growth
/// here (an owned `String`), so this builds without an `Mcx`.
fn build_ps_title() {
    let mut ps_data = String::new();
    let am_walsender = backend_replication_walsender_seams::am_walsender::call();
    if am_walsender {
        let desc = backend_utils_init_miscinit_seams::get_backend_type_desc::call(
            BackendType::WalSender,
        );
        ps_data.push_str(desc);
        ps_data.push(' ');
    }
    with_proc_port(|port| {
        if let Some(u) = &port.user_name {
            ps_data.push_str(u);
        }
        ps_data.push(' ');
        if port.database_name.as_deref().map(|d| !d.is_empty()).unwrap_or(false) {
            ps_data.push_str(port.database_name.as_deref().unwrap());
            ps_data.push(' ');
        }
        if let Some(h) = &port.remote_host {
            ps_data.push_str(h);
        }
        if port.remote_port.as_deref().map(|p| !p.is_empty()).unwrap_or(false) {
            ps_data.push('(');
            ps_data.push_str(port.remote_port.as_deref().unwrap());
            ps_data.push(')');
        }
    });

    backend_utils_misc_more_seams::init_ps_display::call(Some(&ps_data));
    backend_utils_misc_ps_status_seams::set_ps_display::call("initializing".to_string());
}

// ===========================================================================
//  ProcessSSLStartup (line 401)
// ===========================================================================

/// `ProcessSSLStartup(port)` (backend_startup.c:401-471) — check for a direct
/// SSL connection without consuming non-SSL bytes.
///
/// The non-`USE_SSL` build (this repo's current target) takes the reject path
/// once a `0x16` first byte is seen; the SSL handshake itself is owned by
/// be-secure and routed through its seam for the `USE_SSL` arms.
fn process_ssl_startup() -> PgResult<i32> {
    // Assert(!port->ssl_in_use);
    // pq_startmsgread(); firstbyte = pq_peekbyte(); pq_endmsgread();
    backend_libpq_pqcomm_seams::pq_startmsgread::call()?;
    let firstbyte = backend_libpq_pqcomm_seams::pq_peekbyte::call()?;
    backend_libpq_pqcomm_seams::pq_endmsgread::call();

    // if (firstbyte == EOF) return STATUS_ERROR;
    if firstbyte == EOF {
        return Ok(STATUS_ERROR);
    }

    // if (firstbyte != 0x16) return STATUS_OK;
    if firstbyte != 0x16 {
        return Ok(STATUS_OK);
    }

    // First byte indicates a standard SSL handshake message.
    if backend_libpq_be_secure_seams::ssl_supported::call() {
        // if (!LoadedSSL || port->laddr.addr.ss_family == AF_UNIX) goto reject;
        if ssl_negotiation_disabled() {
            return reject_direct_ssl();
        }
        // if (secure_open_server(port) == -1) goto reject;
        let opened = with_proc_port(backend_libpq_be_secure_seams::secure_open_server::call);
        if opened == -1 {
            return reject_direct_ssl();
        }
        // Assert(port->ssl_in_use);
        // if (!port->alpn_used) { COMMERROR; goto reject; }
        let alpn_used = with_proc_port(|port| port.alpn_used);
        if !alpn_used {
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("received direct SSL connection request without ALPN protocol negotiation extension")
                .finish(loc(453, "ProcessSSLStartup"))?;
            return reject_direct_ssl();
        }
        // if (Trace_connection_negotiation) ereport(LOG, "direct SSL connection accepted");
        if trace_connection_negotiation::get() {
            ereport(LOG)
                .errmsg("direct SSL connection accepted")
                .finish(loc(459, "ProcessSSLStartup"))?;
        }
        Ok(STATUS_OK)
    } else {
        // SSL not supported by this build: goto reject.
        reject_direct_ssl()
    }
}

/// The `reject:` label of `ProcessSSLStartup`.
fn reject_direct_ssl() -> PgResult<i32> {
    if trace_connection_negotiation::get() {
        ereport(LOG)
            .errmsg("direct SSL connection rejected")
            .finish(loc(468, "ProcessSSLStartup"))?;
    }
    Ok(STATUS_ERROR)
}

// ===========================================================================
//  ProcessStartupPacket (line 492)
// ===========================================================================

/// `ProcessStartupPacket(port, ssl_done, gss_done)` (backend_startup.c:492-867)
/// — read the client's startup packet and act on it. Returns `Ok(STATUS_OK)` /
/// `Ok(STATUS_ERROR)`, or `Err` carrying an `ereport(FATAL)` (sent to the
/// client). The SSL/GSS negotiation recursion is a real recursive call. `mcx`
/// is the backend's long-lived context (C's TopMemoryContext) into which the
/// option strings and the negotiate-version message are allocated.
fn process_startup_packet(mcx: Mcx<'_>, ssl_done: bool, gss_done: bool) -> PgResult<i32> {
    // pq_startmsgread();
    backend_libpq_pqcomm_seams::pq_startmsgread::call()?;

    // Read the 4-byte big-endian length word one-then-three bytes.
    let mut len_bytes = [0u8; 4];

    // if (pq_getbytes(&len, 1) == EOF)  /* no data at all */
    match read_bytes(mcx, 1)? {
        Some(b) => len_bytes[0] = b[0],
        None => return Ok(STATUS_ERROR),
    }

    // if (pq_getbytes(((char *) &len) + 1, 3) == EOF)  /* partial */
    match read_bytes(mcx, 3)? {
        Some(b) => len_bytes[1..4].copy_from_slice(&b[..3]),
        None => {
            if !ssl_done && !gss_done {
                ereport(COMMERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("incomplete startup packet")
                    .finish(loc(528, "ProcessStartupPacket"))?;
            }
            return Ok(STATUS_ERROR);
        }
    }

    // len = pg_ntoh32(len); len -= 4;
    let mut len: i32 = i32::from_be_bytes(len_bytes);
    len -= 4;

    // if (len < (int32) sizeof(ProtocolVersion) || len > MAX_STARTUP_PACKET_LENGTH)
    if len < SIZEOF_PROTOCOL_VERSION || len > MAX_STARTUP_PACKET_LENGTH {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid length of startup packet")
            .finish(loc(540, "ProcessStartupPacket"))?;
        return Ok(STATUS_ERROR);
    }

    // buf = palloc(len + 1); buf[len] = '\0';
    // if (pq_getbytes(buf, len) == EOF)
    //
    // The owned buffer carries the `len` real bytes; the implicit trailing NUL
    // is the slice boundary — the v3 scan never reads past `len`.
    let buf: mcx::PgVec<'_, u8> = match read_bytes(mcx, len as usize)? {
        Some(b) => b,
        None => {
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("incomplete startup packet")
                .finish(loc(556, "ProcessStartupPacket"))?;
            return Ok(STATUS_ERROR);
        }
    };
    // pq_endmsgread();
    backend_libpq_pqcomm_seams::pq_endmsgread::call();

    // port->proto = proto = pg_ntoh32(*((ProtocolVersion *) buf));
    let proto: ProtocolVersion = read_be_u32(&buf, 0);
    with_proc_port(|port| port.proto = proto);

    // if (proto == CANCEL_REQUEST_CODE)
    if proto == CANCEL_REQUEST_CODE {
        // ProcessCancelRequestPacket(port, buf, len);
        process_cancel_request_packet(&buf, len)?;
        // Not really an error, but we don't want to proceed further.
        return Ok(STATUS_ERROR);
    }

    // if (proto == NEGOTIATE_SSL_CODE && !ssl_done)
    if proto == NEGOTIATE_SSL_CODE && !ssl_done {
        let ssl_ok: u8 = if backend_libpq_be_secure_seams::ssl_supported::call() {
            // if (!LoadedSSL || port->laddr.addr.ss_family == AF_UNIX || port->ssl_in_use)
            if ssl_negotiation_disabled() || with_proc_port(|p| p.ssl_in_use) {
                b'N'
            } else {
                b'S'
            }
        } else {
            b'N'
        };

        if trace_connection_negotiation::get() {
            if ssl_ok == b'S' {
                ereport(LOG)
                    .errmsg("SSLRequest accepted")
                    .finish(loc(597, "ProcessStartupPacket"))?;
            } else {
                ereport(LOG)
                    .errmsg("SSLRequest rejected")
                    .finish(loc(600, "ProcessStartupPacket"))?;
            }
        }

        // while (secure_write(port, &SSLok, 1) != 1) { EINTR retry; else COMMERROR }
        if !write_negotiation_byte(ssl_ok, "SSL")? {
            return Ok(STATUS_ERROR);
        }

        // if (SSLok == 'S' && secure_open_server(port) == -1) return STATUS_ERROR;
        if backend_libpq_be_secure_seams::ssl_supported::call()
            && ssl_ok == b'S'
            && with_proc_port(backend_libpq_be_secure_seams::secure_open_server::call) == -1
        {
            return Ok(STATUS_ERROR);
        }

        // if (pq_buffer_remaining_data() > 0) ereport(FATAL, ...);
        if backend_libpq_pqcomm_seams::pq_buffer_remaining_data::call() > 0 {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("received unencrypted data after SSL request")
                .errdetail("This could be either a client-software bug or evidence of an attempted man-in-the-middle attack.")
                .into_error());
        }

        // return ProcessStartupPacket(port, true, SSLok == 'S');
        return process_startup_packet(mcx, true, ssl_ok == b'S');
    } else if proto == NEGOTIATE_GSS_CODE && !gss_done {
        // char GSSok = 'N';  #ifdef ENABLE_GSS if (laddr != AF_UNIX) GSSok = 'G';
        let gss_ok: u8 = if backend_libpq_be_secure_seams::gss_supported::call()
            && !gss_negotiation_disabled()
        {
            b'G'
        } else {
            b'N'
        };

        if trace_connection_negotiation::get() {
            if gss_ok == b'G' {
                ereport(LOG)
                    .errmsg("GSSENCRequest accepted")
                    .finish(loc(651, "ProcessStartupPacket"))?;
            } else {
                ereport(LOG)
                    .errmsg("GSSENCRequest rejected")
                    .finish(loc(654, "ProcessStartupPacket"))?;
            }
        }

        if !write_negotiation_byte(gss_ok, "GSSAPI")? {
            return Ok(STATUS_ERROR);
        }

        // if (GSSok == 'G' && secure_open_gssapi(port) == -1) return STATUS_ERROR;
        if backend_libpq_be_secure_seams::gss_supported::call()
            && gss_ok == b'G'
            && with_proc_port(backend_libpq_be_secure_seams::secure_open_gssapi::call) == -1
        {
            return Ok(STATUS_ERROR);
        }

        if backend_libpq_pqcomm_seams::pq_buffer_remaining_data::call() > 0 {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("received unencrypted data after GSSAPI encryption request")
                .errdetail("This could be either a client-software bug or evidence of an attempted man-in-the-middle attack.")
                .into_error());
        }

        // return ProcessStartupPacket(port, GSSok == 'G', true);
        return process_startup_packet(mcx, gss_ok == b'G', true);
    }

    /* Could add additional special packet types here */

    // FrontendProtocol = Min(proto, PG_PROTOCOL_LATEST);
    backend_utils_error::config::set_frontend_protocol(min_u32(proto, PG_PROTOCOL_LATEST));

    // Check that the major protocol version is in range.
    if pg_protocol_major(proto) < pg_protocol_major(PG_PROTOCOL_EARLIEST)
        || pg_protocol_major(proto) > pg_protocol_major(PG_PROTOCOL_LATEST)
    {
        return Err(ereport(FATAL)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "unsupported frontend protocol {}.{}: server supports {}.0 to {}.{}",
                pg_protocol_major(proto),
                pg_protocol_minor(proto),
                pg_protocol_major(PG_PROTOCOL_EARLIEST),
                pg_protocol_major(PG_PROTOCOL_LATEST),
                pg_protocol_minor(PG_PROTOCOL_LATEST),
            ))
            .into_error());
    }

    // oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    // (`mcx` is the long-lived context; owned strings below live in the Port.)

    // Handle protocol version 3 startup packet.
    let mut unrecognized_protocol_options: Vec<String> = Vec::new();
    {
        // int32 offset = sizeof(ProtocolVersion);
        let mut offset: i32 = SIZEOF_PROTOCOL_VERSION;

        // port->guc_options = NIL;
        with_proc_port(|port| port.guc_options.clear());

        // while (offset < len)
        while offset < len {
            // char *nameptr = buf + offset;
            // if (*nameptr == '\0') break;  /* found packet terminator */
            if buf[offset as usize] == 0 {
                break;
            }
            // valoffset = offset + strlen(nameptr) + 1;
            let name_off = offset as usize;
            let name_len = cstr_len(&buf, name_off);
            let valoffset: i32 = offset + name_len as i32 + 1;
            // if (valoffset >= len) break;  /* missing value, complain below */
            if valoffset >= len {
                break;
            }
            // valptr = buf + valoffset;
            let val_off = valoffset as usize;
            let val_len = cstr_len(&buf, val_off);

            if cstr_eq(&buf, name_off, b"database") {
                let v = cstr_str(&buf, val_off, val_len);
                with_proc_port(|port| port.database_name = Some(v.clone()));
            } else if cstr_eq(&buf, name_off, b"user") {
                let v = cstr_str(&buf, val_off, val_len);
                with_proc_port(|port| port.user_name = Some(v.clone()));
            } else if cstr_eq(&buf, name_off, b"options") {
                let v = cstr_str(&buf, val_off, val_len);
                with_proc_port(|port| port.cmdline_options = Some(v.clone()));
            } else if cstr_eq(&buf, name_off, b"replication") {
                // replication is a hybrid: boolean or the string 'database'.
                let valstr = cstr_str(&buf, val_off, val_len);
                if cstr_eq(&buf, val_off, b"database") {
                    backend_replication_walsender_seams::set_am_walsender::call(true);
                    backend_replication_walsender_seams::set_am_db_walsender::call(true);
                } else {
                    // else if (!parse_bool(valptr, &am_walsender))
                    match backend_utils_adt_scalar_seams::parse_bool::call(&valstr) {
                        Some(b) => {
                            backend_replication_walsender_seams::set_am_walsender::call(b)
                        }
                        None => {
                            return Err(ereport(FATAL)
                                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                                .errmsg(format!(
                                    "invalid value for parameter \"{}\": \"{}\"",
                                    "replication", valstr
                                ))
                                .errhint(
                                    "Valid values are: \"false\", 0, \"true\", 1, \"database\".",
                                )
                                .into_error());
                        }
                    }
                }
            } else if cstr_starts_with(&buf, name_off, b"_pq_.") {
                // _pq_. options are reserved for protocol-level options; none
                // are defined at present.
                unrecognized_protocol_options.push(cstr_str(&buf, name_off, name_len));
            } else {
                // Generic GUC option: name then value.
                let nm = cstr_str(&buf, name_off, name_len);
                let vl = cstr_str(&buf, val_off, val_len);
                {
                    let nm = nm.clone();
                    let vl = vl.clone();
                    with_proc_port(|port| {
                        port.guc_options.push(nm.clone());
                        port.guc_options.push(vl.clone());
                    });
                }

                // Copy application_name to port if we come across it.
                if cstr_eq(&buf, name_off, b"application_name") {
                    // pg_clean_ascii(valptr, 0): replace non-ASCII with "\xXX",
                    // allocating the cleaned copy in `mcx`. The Port stores an
                    // owned String, so we materialize the PgString result.
                    let cleaned = common_string_seams::pg_clean_ascii::call(mcx, &vl, 0)?;
                    let cleaned = cleaned.as_str().to_string();
                    with_proc_port(|port| port.application_name = Some(cleaned.clone()));
                }
            }
            // offset = valoffset + strlen(valptr) + 1;
            offset = valoffset + val_len as i32 + 1;
        }

        // if (offset != len - 1) ereport(FATAL, "invalid startup packet layout ...");
        if offset != len - 1 {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("invalid startup packet layout: expected terminator as last byte")
                .into_error());
        }

        // if (PG_PROTOCOL_MINOR(proto) > PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST) ||
        //     unrecognized_protocol_options != NIL)
        if pg_protocol_minor(proto) > pg_protocol_minor(PG_PROTOCOL_LATEST)
            || !unrecognized_protocol_options.is_empty()
        {
            send_negotiate_protocol_version(mcx, &unrecognized_protocol_options)?;
        }
    }

    // Check a user name was given.
    let user_empty = with_proc_port(|port| port.user_name.as_deref().unwrap_or("").is_empty());
    if user_empty {
        return Err(ereport(FATAL)
            .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            .errmsg("no PostgreSQL user name specified in startup packet")
            .into_error());
    }

    // The database defaults to the user name; then truncate both to NAMEDATALEN.
    with_proc_port(|port| {
        if port.database_name.as_deref().unwrap_or("").is_empty() {
            port.database_name = port.user_name.clone();
        }
        truncate_namedatalen(&mut port.database_name);
        truncate_namedatalen(&mut port.user_name);
    });

    // if (am_walsender) MyBackendType = B_WAL_SENDER; else MyBackendType = B_BACKEND;
    let am_walsender = backend_replication_walsender_seams::am_walsender::call();
    if am_walsender {
        backend_utils_init_small_seams::set_my_backend_type::call(BackendType::WalSender);
    } else {
        backend_utils_init_small_seams::set_my_backend_type::call(BackendType::Backend);
    }

    // Normal (non-db) walsenders are not connected to a particular database.
    if am_walsender && !backend_replication_walsender_seams::am_db_walsender::call() {
        with_proc_port(|port| port.database_name = Some(String::new()));
    }

    // MemoryContextSwitchTo(oldcontext);
    Ok(STATUS_OK)
}

/// The `while (secure_write(port, &byte, 1) != 1) { if (EINTR) continue; …
/// COMMERROR }` negotiation-byte loop.
///
/// Returns `Ok(true)` once the byte is written, `Ok(false)` on a hard socket
/// error (the caller returns `STATUS_ERROR`; the COMMERROR has been raised),
/// or propagates a raised `Err` from the transport's interrupt processing.
fn write_negotiation_byte(byte: u8, which: &str) -> PgResult<bool> {
    let buf = [byte];
    loop {
        let result =
            with_proc_port(|port| backend_libpq_be_secure_seams::secure_write::call(port, &buf));
        // The errno the failing write left behind; threaded into the report so
        // `errcode_for_socket_access()` picks the SQLSTATE off it and `%m`
        // expands against it (C: ambient `errno` set by secure_write).
        let saved_errno = match result? {
            Ok(1) => return Ok(true),
            // A 1-byte secure_write returns 1 on success; any other Ok is the
            // never-in-practice 0. Treat as the C `!= 1` failure path. No errno
            // is associated, so fall through to the ambient-errno report.
            Ok(_) => None,
            Err(SockError::Errno(e)) if e == EINTR => continue, // if interrupted, retry
            // errcode_for_socket_access() + "failed to send %s negotiation
            // response: %m" — COMMERROR, then close.
            Err(SockError::Errno(e)) => Some(e),
            Err(_) => None,
        };
        let mut builder = ereport(COMMERROR);
        if let Some(e) = saved_errno {
            builder = builder.with_saved_errno(e);
        }
        builder
            .errcode_for_socket_access()
            .errmsg(format!("failed to send {which} negotiation response: %m"))
            .finish(loc(607, "ProcessStartupPacket"))?;
        return Ok(false);
    }
}

// ===========================================================================
//  ProcessCancelRequestPacket (line 875)
// ===========================================================================

/// `ProcessCancelRequestPacket(port, pkt, pktlen)` (backend_startup.c:875-898)
/// — the client sent a cancel request. Validate the length and forward to the
/// cancel-key subsystem. Nothing is sent back to the client.
fn process_cancel_request_packet(pkt: &[u8], pktlen: i32) -> PgResult<()> {
    // if (pktlen < offsetof(CancelRequestPacket, cancelAuthCode))
    if (pktlen as usize) < CANCEL_AUTH_CODE_OFFSET {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid length of cancel request packet")
            .finish(loc(884, "ProcessCancelRequestPacket"))?;
        return Ok(());
    }
    // len = pktlen - offsetof(CancelRequestPacket, cancelAuthCode);
    let len = pktlen as usize - CANCEL_AUTH_CODE_OFFSET;
    // if (len == 0 || len > 256)
    if len == 0 || len > 256 {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid length of cancel key in cancel request packet")
            .finish(loc(892, "ProcessCancelRequestPacket"))?;
        return Ok(());
    }

    // canc = (CancelRequestPacket *) pkt;
    // SendCancelRequest(pg_ntoh32(canc->backendPID), canc->cancelAuthCode, len);
    let backend_pid = read_be_u32(pkt, 4) as i32;
    let cancel_auth_code = &pkt[CANCEL_AUTH_CODE_OFFSET..CANCEL_AUTH_CODE_OFFSET + len];
    backend_storage_ipc_procsignal::SendCancelRequest(backend_pid, cancel_auth_code);
    Ok(())
}

// ===========================================================================
//  SendNegotiateProtocolVersion (line 917)
// ===========================================================================

/// `SendNegotiateProtocolVersion(unrecognized_protocol_options)`
/// (backend_startup.c:917-930) — send a `PqMsg_NegotiateProtocolVersion`
/// listing `FrontendProtocol` and the unrecognized options. The buffer is
/// allocated in `mcx` (C: `initStringInfo` in CurrentMemoryContext).
fn send_negotiate_protocol_version(
    mcx: Mcx<'_>,
    unrecognized_protocol_options: &[String],
) -> PgResult<()> {
    // pq_beginmessage(&buf, PqMsg_NegotiateProtocolVersion);
    let mut buf =
        backend_libpq_pqformat::pq_beginmessage(mcx, PQMSG_NEGOTIATE_PROTOCOL_VERSION)?;
    // pq_sendint32(&buf, FrontendProtocol);
    let frontend_protocol = backend_utils_error::config::frontend_protocol();
    backend_libpq_pqformat::pq_sendint32(&mut buf, frontend_protocol)?;
    // pq_sendint32(&buf, list_length(unrecognized_protocol_options));
    backend_libpq_pqformat::pq_sendint32(&mut buf, unrecognized_protocol_options.len() as u32)?;
    // foreach(lc, unrecognized_protocol_options) pq_sendstring(&buf, lfirst(lc));
    for opt in unrecognized_protocol_options {
        // pq_sendstring sends the bytes plus the trailing NUL.
        backend_libpq_pqformat::pq_sendstring(&mut buf, opt.as_bytes())?;
    }
    // pq_endmessage(&buf);
    backend_libpq_pqformat::pq_endmessage(buf)?;
    // no need to flush, some other message will follow
    Ok(())
}

// ===========================================================================
//  process_startup_packet_die / StartupPacketTimeoutHandler (lines 947/957)
// ===========================================================================

/// `process_startup_packet_die(SIGNAL_ARGS)` (backend_startup.c:947-950) —
/// SIGTERM while processing the startup packet: `_exit(1)`. Running before
/// shared memory is touched, so no atexit handlers run.
pub fn process_startup_packet_die() -> ! {
    // C calls `_exit(1)` directly: this runs in a signal handler before shared
    // memory is touched, so atexit/on_exit and Rust destructors must NOT run.
    // `libc::_exit` mirrors C exactly (immediate, skips all exit handlers);
    // `std::process::exit` would run them and is unsafe here.
    unsafe { libc::_exit(1) }
}

/// `StartupPacketTimeoutHandler()` (backend_startup.c:957-960) — timeout while
/// processing the startup packet: `_exit(1)`.
pub fn startup_packet_timeout_handler() {
    // Same as process_startup_packet_die: `_exit(1)` from a signal handler,
    // skipping atexit/on_exit handlers and Rust destructors.
    unsafe { libc::_exit(1) }
}

// ===========================================================================
//  validate_log_connections_options (line 976)
// ===========================================================================

/// `config_enum_entry` analogue: a `(name, val)` pair (the `hidden` field is
/// unused by these tables, as in C where these literals omit it).
struct LogConnOption {
    name: &'static str,
    val: u32,
}

/// `validate_log_connections_options(elemlist, *flags)`
/// (backend_startup.c:976-1061) — validate the listified `log_connections`
/// input, returning the resolved flags on `Ok`, or on `Err` the verbatim
/// `GUC_check_errdetail` text the C records before returning `false`.
pub fn validate_log_connections_options(elemlist: &[String]) -> Result<u32, String> {
    // static const struct config_enum_entry compat_options[] = { … };
    const COMPAT_OPTIONS: [LogConnOption; 8] = [
        LogConnOption { name: "off", val: 0 },
        LogConnOption { name: "false", val: 0 },
        LogConnOption { name: "no", val: 0 },
        LogConnOption { name: "0", val: 0 },
        LogConnOption { name: "on", val: LOG_CONNECTION_ON },
        LogConnOption { name: "true", val: LOG_CONNECTION_ON },
        LogConnOption { name: "yes", val: LOG_CONNECTION_ON },
        LogConnOption { name: "1", val: LOG_CONNECTION_ON },
    ];

    // *flags = 0;
    let mut flags: u32 = 0;

    // if (list_length(elemlist) == 0) return true;
    if elemlist.is_empty() {
        return Ok(flags);
    }

    // item = linitial(elemlist);
    let item = &elemlist[0];

    for option in COMPAT_OPTIONS.iter() {
        // if (pg_strcasecmp(item, option.name) != 0) continue;
        if pg_strcasecmp(item, option.name) != 0 {
            continue;
        }
        // if (list_length(elemlist) > 1) { GUC_check_errdetail(...); return false; }
        if elemlist.len() > 1 {
            return Err(format!(
                "Cannot specify log_connections option \"{item}\" in a list with other options."
            ));
        }
        // *flags = option.val; return true;
        return Ok(option.val);
    }

    // Now check the aspect options. The empty string was already handled.
    const OPTIONS: [LogConnOption; 5] = [
        LogConnOption { name: "receipt", val: LOG_CONNECTION_RECEIPT },
        LogConnOption { name: "authentication", val: LOG_CONNECTION_AUTHENTICATION },
        LogConnOption { name: "authorization", val: LOG_CONNECTION_AUTHORIZATION },
        LogConnOption { name: "setup_durations", val: LOG_CONNECTION_SETUP_DURATIONS },
        LogConnOption { name: "all", val: LOG_CONNECTION_ALL },
    ];

    // foreach(l, elemlist)
    'outer: for item in elemlist.iter() {
        for option in OPTIONS.iter() {
            if pg_strcasecmp(item, option.name) == 0 {
                flags |= option.val;
                continue 'outer; // goto next;
            }
        }
        // GUC_check_errdetail("Invalid option \"%s\".", item); return false;
        return Err(format!("Invalid option \"{item}\"."));
    }

    // return true;
    Ok(flags)
}

// ===========================================================================
//  check_log_connections / assign_log_connections (lines 1068/1112)
// ===========================================================================

/// `check_log_connections(newval, extra, source)` (backend_startup.c:1068-1106)
/// — the `log_connections` GUC check hook. `mcx` is the current allocation
/// context (`SplitIdentifierString` allocates there). Returns
/// `Ok(Ok(flags))` on success (the value the assign hook stores in `*extra`),
/// `Ok(Err(detail))` on a validation failure (C `return false`, carrying the
/// `GUC_check_errdetail` text the GUC machinery records), or `Err` for an
/// allocation `ereport(ERROR)`.
pub fn check_log_connections(mcx: Mcx<'_>, newval: &str) -> PgResult<Result<u32, String>> {
    // rawstring = pstrdup(*newval);
    // if (!SplitIdentifierString(rawstring, ',', &elemlist)) { ... return false; }
    let elemlist =
        match backend_utils_adt_varlena_seams::split_identifier_string::call(mcx, newval, ',')? {
            Some(list) => list,
            None => {
                // GUC_check_errdetail("Invalid list syntax in parameter \"%s\".", ...);
                return Ok(Err(format!(
                    "Invalid list syntax in parameter \"{}\".",
                    "log_connections"
                )));
            }
        };

    // Validation logic is all in the helper.
    let elems: Vec<String> = elemlist.iter().map(|s| s.as_str().to_string()).collect();
    Ok(validate_log_connections_options(&elems))
}

/// `assign_log_connections(newval, extra)` (backend_startup.c:1112-1115) —
/// the `log_connections` GUC assign hook: `log_connections = *((int *) extra)`.
pub fn assign_log_connections(extra: u32) {
    log_connections::set(extra);
}

// ---------------------------------------------------------------------------
// GUC hook-slot adapters (the `GucStringCheckFn`/`GucStringAssignFn` shapes the
// guc-tables hook slots expect). They wrap the owned-model helpers above,
// threading the parsed `u32` flag mask through the `*extra` carrier exactly as C
// passes `void *extra` from `check_log_connections` to `assign_log_connections`.
// ---------------------------------------------------------------------------

/// The `check_log_connections` GUC check-hook slot adapter
/// (`GucStringCheckFn`). Parses `*newval` via [`check_log_connections`], records
/// the resulting flag mask into `*extra`, and reports the C `false` (with the
/// `GUC_check_errdetail` text) on a syntax/keyword error.
fn check_log_connections_slot(
    newval: &mut Option<String>,
    extra: &mut Option<backend_utils_misc_guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    let raw = newval.clone().unwrap_or_default();
    let scratch = mcx::MemoryContext::new("check_log_connections");
    match check_log_connections(scratch.mcx(), &raw)? {
        Ok(flags) => {
            // Save the flags in *extra, for use by the assign function.
            *extra = Some(Box::new(flags));
            Ok(true)
        }
        Err(detail) => {
            backend_utils_misc_guc_seams::guc_check_errdetail::call(detail);
            Ok(false)
        }
    }
}

/// The `assign_log_connections` GUC assign-hook slot adapter
/// (`GucStringAssignFn`): `log_connections = *((int *) extra)`.
fn assign_log_connections_slot(
    _newval: Option<&str>,
    extra: Option<&backend_utils_misc_guc_tables::GucHookExtra>,
) {
    if let Some(extra) = extra {
        if let Some(flags) = extra.downcast_ref::<u32>() {
            assign_log_connections(*flags);
        }
    }
}

// ===========================================================================
//  init_seams
// ===========================================================================

/// Install every seam this crate owns (`backend-tcop-backend-startup-seams`).
pub fn init_seams() {
    backend_tcop_backend_startup_seams::backend_main::set(backend_main);
    backend_tcop_backend_startup_seams::set_conn_timing_child::set(set_conn_timing_child);
    backend_tcop_backend_startup_seams::set_conn_timing_auth_start::set(globals::conn_timing::set_auth_start);
    backend_tcop_backend_startup_seams::set_conn_timing_auth_end::set(globals::conn_timing::set_auth_end);

    // GUC variable accessors (`conf->variable`) for the two GUC variables this
    // unit owns (backend_startup.c:46-47, guc_tables.c:1253/4991). The GUC
    // machinery seeds them from boot_val during InitializeGUCOptions and reads
    // them through these accessors.
    //
    //   `bool Trace_connection_negotiation` (DEVELOPER_OPTIONS) — the GUC slot
    //   IS the variable; read directly at pre-auth handshake time.
    //
    //   `char *log_connections_string` (LOGGING_WHAT, GUC_LIST_INPUT) — the raw
    //   string the GUC machinery owns; `check_log_connections`/
    //   `assign_log_connections` parse it into the separate `log_connections`
    //   int flag mask (the latter is set by the assign hook, not a GUC slot).
    use backend_utils_misc_guc_tables::{hooks, vars, GucVarAccessors};
    vars::Trace_connection_negotiation.install(GucVarAccessors {
        get: globals::trace_connection_negotiation::get,
        set: globals::trace_connection_negotiation::set,
    });
    vars::log_connections_string.install(GucVarAccessors {
        get: globals::log_connections_string::get,
        set: globals::log_connections_string::set,
    });

    // The `log_connections` GUC's check/assign hook slots (guc_tables.c:875).
    // The GUC machinery fires these when `log_connections` is set (e.g. from
    // postgresql.conf); without them the slot panics "used before its owning unit
    // installed it" the moment a non-default `log_connections` value is applied.
    hooks::check_log_connections.install(check_log_connections_slot);
    hooks::assign_log_connections.install(assign_log_connections_slot);

    // `log_connections & LOG_CONNECTION_AUTHENTICATION` — auth.c reads this
    // bitmask to decide whether to emit the per-method "connection
    // authenticated" line. The `log_connections` aspect-flag mask is owned by
    // this unit (backend_startup.c, parsed by check/assign_log_connections), so
    // the read for auth.c is installed here.
    backend_libpq_auth_seams::log_connection_authentication::set(|| {
        log_connections::get() & LOG_CONNECTION_AUTHENTICATION != 0
    });
}

/// `set_conn_timing_child` (the inward seam): transfer launch timings into the
/// `conn_timing` global in the freshly forked child.
fn set_conn_timing_child(
    socket_create: TimestampTz,
    fork_start: TimestampTz,
    fork_end: TimestampTz,
) {
    conn_timing::set_socket_create(socket_create);
    conn_timing::set_fork_start(fork_start);
    conn_timing::set_fork_end(fork_end);
}

// ===========================================================================
//  Small helpers / glue.
// ===========================================================================

/// Run `f` with mutable access to `MyProcPort`, panicking if there is no
/// connection (within `BackendInitialize`/`ProcessStartupPacket` there always
/// is — `pq_init` set it).
fn with_proc_port<R>(f: impl FnOnce(&mut Port) -> R) -> R {
    let mut f = Some(f);
    let mut out: Option<R> = None;
    backend_utils_init_small_seams::with_my_proc_port::call(&mut |port| {
        let port = port.expect("MyProcPort must be set during backend startup (pq_init ran)");
        let f = f.take().expect("with_my_proc_port invoked the callback more than once");
        out = Some(f(port));
    });
    out.expect("with_my_proc_port did not invoke the callback")
}

/// `pq_getbytes(buf, n)` over the owned-buffer seam: `Ok(Some(bytes))` for the
/// `n` bytes read, `Ok(None)` for the C `EOF` return. The buffer is allocated
/// by the pqcomm owner in `mcx`.
fn read_bytes(mcx: Mcx<'_>, n: usize) -> PgResult<Option<mcx::PgVec<'_, u8>>> {
    backend_libpq_pqcomm_seams::pq_getbytes::call(mcx, n)
}

/// `gai_strerror(code)` — render a getaddrinfo error code via the libc
/// function (the same text C's `gai_strerror` produces).
fn gai_strerror(code: i32) -> String {
    // SAFETY: gai_strerror returns a pointer to a static, NUL-terminated C
    // string for any input; it is never NULL.
    let ptr = unsafe { libc::gai_strerror(code) };
    if ptr.is_null() {
        return format!("EAI error {code}");
    }
    // SAFETY: `ptr` is a valid NUL-terminated C string owned by libc.
    unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

/// `pqsignal(SIGTERM, process_startup_packet_die)`.
fn arm_startup_packet_signals() {
    port_pqsignal_seams::pqsignal::call(
        libc::SIGTERM,
        types_signal::SigHandler::Handler(process_startup_packet_die_signal),
    );
}

/// C signal-handler trampoline for `process_startup_packet_die`.
fn process_startup_packet_die_signal(_signo: i32) {
    process_startup_packet_die()
}

/// `sigprocmask(SIG_SETMASK, mask, NULL)` — replace the process signal mask.
/// The mask itself (`BlockSig`/`StartupBlockSig`) is owned by the
/// pqsignal unit; the syscall is a plain libc call.
fn set_signal_mask(mask: &libc::sigset_t) {
    // SAFETY: `mask` is a valid, initialized sigset_t produced by the
    // pqsignal owner; the NULL old-mask out-pointer is the C call shape.
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, mask, core::ptr::null_mut());
    }
}

/// `port->laddr.addr.ss_family == AF_UNIX || !LoadedSSL || port->ssl_in_use`
/// reduced to the SSL-negotiation-disabled predicate the owner answers.
fn ssl_negotiation_disabled() -> bool {
    with_proc_port(backend_libpq_be_secure_seams::ssl_negotiation_disabled::call)
}

/// `port->laddr.addr.ss_family == AF_UNIX` (the GSS-on-Unix-socket guard).
fn gss_negotiation_disabled() -> bool {
    with_proc_port(backend_libpq_be_secure_seams::gss_negotiation_disabled::call)
}

/// `*((ProtocolVersion *) (buf + off))` then `pg_ntoh32`.
#[inline]
fn read_be_u32(buf: &[u8], off: usize) -> u32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[off..off + 4]);
    u32::from_be_bytes(bytes)
}

/// `Min(a, b)` for `uint32`.
#[inline]
const fn min_u32(a: u32, b: u32) -> u32 {
    if a < b {
        a
    } else {
        b
    }
}

/// `strlen(buf + off)` — length of the NUL-terminated C string at `off`.
#[inline]
fn cstr_len(buf: &[u8], off: usize) -> usize {
    let mut n = 0usize;
    while off + n < buf.len() && buf[off + n] != 0 {
        n += 1;
    }
    n
}

/// `strcmp(buf + off, lit) == 0` — compare the C string at `off` against an
/// ASCII byte literal (no trailing NUL in `lit`).
#[inline]
fn cstr_eq(buf: &[u8], off: usize, lit: &[u8]) -> bool {
    for (i, &b) in lit.iter().enumerate() {
        if buf.get(off + i).copied().unwrap_or(0) != b {
            return false;
        }
    }
    // The byte after the literal must be the terminating NUL (or buffer end).
    buf.get(off + lit.len()).copied().unwrap_or(0) == 0
}

/// `strncmp(buf + off, prefix, prefix.len()) == 0`.
#[inline]
fn cstr_starts_with(buf: &[u8], off: usize, prefix: &[u8]) -> bool {
    for (i, &b) in prefix.iter().enumerate() {
        if buf.get(off + i).copied().unwrap_or(0) != b {
            return false;
        }
    }
    true
}

/// `pstrdup(buf + off)` — render the `len`-byte C string at `off`.
#[inline]
fn cstr_str(buf: &[u8], off: usize, len: usize) -> String {
    String::from_utf8_lossy(&buf[off..off + len]).into_owned()
}

/// `if (strlen(s) >= NAMEDATALEN) s[NAMEDATALEN - 1] = '\0';`.
#[inline]
fn truncate_namedatalen(name: &mut Option<String>) {
    if let Some(s) = name {
        if s.len() >= NAMEDATALEN {
            s.truncate(NAMEDATALEN - 1);
        }
    }
}

/// `strspn(s, charset)` — length of the initial segment of `s` consisting
/// entirely of bytes in `charset`.
fn strspn(s: &str, charset: &[u8]) -> usize {
    let bytes = s.as_bytes();
    let mut n = 0;
    while n < bytes.len() && charset.contains(&bytes[n]) {
        n += 1;
    }
    n
}

/// `pg_strcasecmp(a, b)` — ASCII case-insensitive comparison returning the sign
/// of the first differing (lowercased) byte.
fn pg_strcasecmp(a: &str, b: &str) -> i32 {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    let mut i = 0usize;
    loop {
        let ca = ab.get(i).copied().unwrap_or(0);
        let cb = bb.get(i).copied().unwrap_or(0);
        let la = ascii_tolower(ca);
        let lb = ascii_tolower(cb);
        if la != lb {
            return la as i32 - lb as i32;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

#[inline]
fn ascii_tolower(c: u8) -> u8 {
    if c.is_ascii_uppercase() {
        c + 32
    } else {
        c
    }
}

#[cfg(test)]
mod tests;
