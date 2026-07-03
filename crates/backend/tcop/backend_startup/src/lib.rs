//! backend_startup.c: the postmaster-child backend entry point. BackendMain
//! initializes the connection, collects and acts on the startup packet
//! (cancel requests and SSL/GSS negotiation included), rejects on database
//! state, builds the ps title, and hands off to PostgresMain.
//!
//! Build shape matches a `!USE_SSL && !ENABLE_GSS` C build: direct-SSL and
//! negotiation requests take the reject/'N' arms. The auth surface
//! (ClientAuthentication and everything behind it) is not this unit; it stays
//! behind loud seams in InitPostgres. Thread model: the per-child SIGTERM /
//! SIGQUIT process handlers of the fork build are postmaster-signal design
//! (process-wide under threads); the per-thread signal masks ARE installed.

#![allow(clippy::result_large_err)]

use elog::ereport;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::{ProtocolVersion, STATUS_ERROR, STATUS_OK};
use types_dest::CommandDest;
use types_error::{
    ErrorLocation, PgResult, COMMERROR, ERRCODE_CANNOT_CONNECT_NOW, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION, ERRCODE_PROTOCOL_VIOLATION,
    ERRCODE_TOO_MANY_CONNECTIONS, FATAL, LOG, WARNING,
};
use types_startup::{
    CacState, ClientSocket, StartupData, CANCEL_REQUEST_OFFSET_AUTH_CODE,
    CANCEL_REQUEST_OFFSET_BACKEND_PID,
};

mod globals;
#[cfg(test)]
mod tests;

pub use globals::{conn_timing, log_connections, trace_connection_negotiation};

pub const MAX_STARTUP_PACKET_LENGTH: i32 = 10000;
pub const NAMEDATALEN: usize = 64;
pub const PGPROC_MAX_CACHED_SUBXIDS: i32 = 64;

pub const fn pg_protocol(m: u32, n: u32) -> ProtocolVersion {
    (m << 16) | n
}
pub const fn pg_protocol_major(v: ProtocolVersion) -> u32 {
    v >> 16
}
pub const fn pg_protocol_minor(v: ProtocolVersion) -> u32 {
    v & 0x0000_ffff
}

pub const PG_PROTOCOL_EARLIEST: ProtocolVersion = pg_protocol(3, 0);
pub const PG_PROTOCOL_LATEST: ProtocolVersion = pg_protocol(3, 2);
pub const CANCEL_REQUEST_CODE: ProtocolVersion = pg_protocol(1234, 5678);
pub const NEGOTIATE_SSL_CODE: ProtocolVersion = pg_protocol(1234, 5679);
pub const NEGOTIATE_GSS_CODE: ProtocolVersion = pg_protocol(1234, 5680);

const SIZEOF_PROTOCOL_VERSION: i32 = 4;
const PQMSG_NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';

pub const LOG_CONNECTION_RECEIPT: u32 = 1 << 0;
pub const LOG_CONNECTION_AUTHENTICATION: u32 = 1 << 1;
pub const LOG_CONNECTION_AUTHORIZATION: u32 = 1 << 2;
pub const LOG_CONNECTION_SETUP_DURATIONS: u32 = 1 << 3;
pub const LOG_CONNECTION_ON: u32 =
    LOG_CONNECTION_RECEIPT | LOG_CONNECTION_AUTHENTICATION | LOG_CONNECTION_AUTHORIZATION;
pub const LOG_CONNECTION_ALL: u32 = LOG_CONNECTION_ON | LOG_CONNECTION_SETUP_DURATIONS;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/tcop/backend_startup.c", line, func)
}

// BackendMain (backend_startup.c) — never returns.
pub fn backend_main(startup_data: &StartupData) -> ! {
    let StartupData::Backend(bsdata) = *startup_data else {
        unreachable!("BackendMain requires StartupData::Backend")
    };
    let client_sock = init_small::globals::MyClientSocket()
        .expect("MyClientSocket must be set before BackendMain");

    let top = MemoryContext::new("TopMemoryContext");
    let result = (|| -> PgResult<()> {
        backend_initialize(top.mcx(), &client_sock, bsdata.can_accept_connections)?;
        lmgr_proc::InitProcess(miscinit::GetMyBackendType())?;
        Ok(())
    })();

    let my_pid = init_small::globals::MyProcPid();
    if result.is_err() {
        // The FATAL was reported by the elog machinery; C's proc_exit(1).
        ipc_seams::proc_exit::call(1, my_pid)
    }

    let (dbname, username) = init_small::globals::WithMyProcPort(|port| {
        (
            port.database_name.clone().unwrap_or_default(),
            port.user_name.clone().unwrap_or_default(),
        )
    });
    postgres_seams::postgres_main::call(&dbname, &username)
}

// BackendInitialize: C terminates on failure; here Err after the report ran.
fn backend_initialize(mcx: Mcx<'_>, client_sock: &ClientSocket, cac: CacState) -> PgResult<()> {
    fd::ReserveExternalFD()?;

    let pre_auth_delay = guc_tables::vars::PreAuthDelay.read();
    if pre_auth_delay > 0 {
        std::thread::sleep(std::time::Duration::from_secs(pre_auth_delay as u64));
    }

    elog::config::set_client_auth_in_progress(true);

    let port = pqcomm_seams::pq_init::call(client_sock)?;
    init_small::globals::SetMyProcPort(port);

    elog::config::set_where_to_send_output(CommandDest::Remote);

    // pqsignal(SIGTERM, process_startup_packet_die): process-wide handler in
    // the fork build; the thread rendering (a per-backend termination request
    // channel) is postmaster-signal design. The per-thread mask below is real.
    timeout_seams::initialize_timeouts::call();
    libpq_pqsignal::block_startup_signals();

    let log_hostname = guc_tables::vars::log_hostname.read();
    let raddr = init_small::globals::WithMyProcPort(|p| p.raddr);
    let flags = (if log_hostname { 0 } else { libc::NI_NUMERICHOST }) | libc::NI_NUMERICSERV;
    let mut remote_host = String::new();
    let mut remote_port = String::new();
    let ret =
        ip::pg_getnameinfo_all(&raddr, Some(&mut remote_host), Some(&mut remote_port), flags);
    if ret != 0 {
        ereport(WARNING)
            .errmsg_internal(format!("pg_getnameinfo_all() failed: {}", gai_strerror(ret)))
            .finish(loc(211, "BackendInitialize"))?;
    }

    init_small::globals::WithMyProcPort(|p| {
        p.remote_host = remote_host.clone();
        p.remote_port = remote_port.clone();
    });

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

    if log_hostname
        && ret == 0
        && strspn(&remote_host, b"0123456789.") < remote_host.len()
        && strspn(&remote_host, b"0123456789ABCDEFabcdef:") < remote_host.len()
    {
        let rh = remote_host.clone();
        init_small::globals::WithMyProcPort(|p| p.remote_hostname = Some(rh.clone()));
    }

    timeout_seams::register_timeout::call(
        timeout_seams::STARTUP_PACKET_TIMEOUT,
        startup_packet_timeout_handler,
    );
    let auth_timeout = guc_tables::vars::AuthenticationTimeout.read();
    timeout_seams::enable_timeout_after::call(
        timeout_seams::STARTUP_PACKET_TIMEOUT,
        auth_timeout * 1000,
    )?;

    let mut status = process_ssl_startup()?;
    if status == STATUS_OK {
        status = process_startup_packet(mcx, false, false)?;
    }
    if status == STATUS_OK {
        reject_for_cac_state(cac)?;
    }

    timeout_seams::disable_timeout::call(timeout_seams::STARTUP_PACKET_TIMEOUT, false)?;
    libpq_pqsignal::block_signals();

    ipc_seams::check_on_shmem_exit_lists_are_empty::call()?;

    if status != STATUS_OK {
        ipc_seams::proc_exit::call(0, init_small::globals::MyProcPid())
    }

    build_ps_title();
    Ok(())
}

fn reject_for_cac_state(cac: CacState) -> PgResult<()> {
    let f = loc(307, "BackendInitialize");
    match cac {
        CacState::Ok => Ok(()),
        CacState::Startup => ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg("the database system is starting up")
            .finish(f),
        CacState::NotHotStandby => {
            if !guc_tables::vars::EnableHotStandby.read() {
                ereport(FATAL)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg("the database system is not accepting connections")
                    .errdetail("Hot standby mode is disabled.")
                    .finish(f)
            } else if xlogrecovery_seams::reached_consistency::call() {
                ereport(FATAL)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg("the database system is not yet accepting connections")
                    .errdetail("Recovery snapshot is not yet ready for hot standby.")
                    .errhint(format!(
                        "To enable hot standby, close write transactions with more than {PGPROC_MAX_CACHED_SUBXIDS} subtransactions on the primary server."
                    ))
                    .finish(f)
            } else {
                ereport(FATAL)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg("the database system is not yet accepting connections")
                    .errdetail("Consistent recovery state has not been yet reached.")
                    .finish(f)
            }
        }
        CacState::Shutdown => ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg("the database system is shutting down")
            .finish(f),
        CacState::Recovery => ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg("the database system is in recovery mode")
            .finish(f),
        CacState::TooMany => ereport(FATAL)
            .errcode(ERRCODE_TOO_MANY_CONNECTIONS)
            .errmsg("sorry, too many clients already")
            .finish(f),
    }
}

fn build_ps_title() {
    // am_walsender is always false here: the replication startup option
    // panics until the walsender unit lands (see process_startup_packet).
    let ps_data = init_small::globals::WithMyProcPort(|port| {
        let mut s = String::new();
        s.push_str(port.user_name.as_deref().unwrap_or(""));
        s.push(' ');
        if let Some(db) = port.database_name.as_deref() {
            if !db.is_empty() {
                s.push_str(db);
                s.push(' ');
            }
        }
        s.push_str(&port.remote_host);
        if !port.remote_port.is_empty() {
            s.push('(');
            s.push_str(&port.remote_port);
            s.push(')');
        }
        s
    });
    ps_status_seams::init_ps_display::call(Some(&ps_data));
    ps_status_seams::set_ps_display::call("initializing");
}

// ProcessSSLStartup: without USE_SSL, 0x16 lands on the C `reject:` label.
fn process_ssl_startup() -> PgResult<i32> {
    pqcomm::pq_startmsgread()?;
    let firstbyte = pqcomm::pq_peekbyte()?;
    pqcomm::pq_endmsgread();

    if firstbyte == pqcomm::EOF {
        return Ok(STATUS_ERROR);
    }
    if firstbyte != 0x16 {
        return Ok(STATUS_OK);
    }
    if trace_connection_negotiation::get() {
        ereport(LOG)
            .errmsg("direct SSL connection rejected")
            .finish(loc(468, "ProcessSSLStartup"))?;
    }
    Ok(STATUS_ERROR)
}

// ProcessStartupPacket: the SSL/GSS recursion is real, depth-bounded as in C.
fn process_startup_packet(mcx: Mcx<'_>, ssl_done: bool, gss_done: bool) -> PgResult<i32> {
    pqcomm::pq_startmsgread()?;

    let mut len_bytes = [0u8; 4];
    if pqcomm::pq_getbytes(&mut len_bytes[..1])? == pqcomm::EOF {
        // No data at all: don't clutter the log.
        return Ok(STATUS_ERROR);
    }
    if pqcomm::pq_getbytes(&mut len_bytes[1..4])? == pqcomm::EOF {
        if !ssl_done && !gss_done {
            ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("incomplete startup packet")
                .finish(loc(528, "ProcessStartupPacket"))?;
        }
        return Ok(STATUS_ERROR);
    }

    let len = i32::from_be_bytes(len_bytes) - 4;
    if len < SIZEOF_PROTOCOL_VERSION || len > MAX_STARTUP_PACKET_LENGTH {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid length of startup packet")
            .finish(loc(540, "ProcessStartupPacket"))?;
        return Ok(STATUS_ERROR);
    }

    let mut buf: PgVec<'_, u8> = zeroed_vec(mcx, len as usize)?;
    if pqcomm::pq_getbytes(&mut buf)? == pqcomm::EOF {
        ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("incomplete startup packet")
            .finish(loc(556, "ProcessStartupPacket"))?;
        return Ok(STATUS_ERROR);
    }
    pqcomm::pq_endmsgread();

    let proto: ProtocolVersion = read_be_u32(&buf, 0);
    init_small::globals::WithMyProcPort(|p| p.proto = proto);

    if proto == CANCEL_REQUEST_CODE {
        process_cancel_request_packet(&buf, len)?;
        // Not really an error, but we don't want to proceed further.
        return Ok(STATUS_ERROR);
    }

    if proto == NEGOTIATE_SSL_CODE && !ssl_done {
        // No USE_SSL in this build: SSLok = 'N'.
        if trace_connection_negotiation::get() {
            ereport(LOG)
                .errmsg("SSLRequest rejected")
                .finish(loc(600, "ProcessStartupPacket"))?;
        }
        if !write_negotiation_byte(b'N', "SSL")? {
            return Ok(STATUS_ERROR);
        }
        if pqcomm::pq_buffer_remaining_data() > 0 {
            return ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("received unencrypted data after SSL request")
                .errdetail("This could be either a client-software bug or evidence of an attempted man-in-the-middle attack.")
                .finish(loc(627, "ProcessStartupPacket"))
                .map(|()| STATUS_ERROR);
        }
        return process_startup_packet(mcx, true, false);
    } else if proto == NEGOTIATE_GSS_CODE && !gss_done {
        // No ENABLE_GSS in this build: GSSok = 'N'.
        if trace_connection_negotiation::get() {
            ereport(LOG)
                .errmsg("GSSENCRequest rejected")
                .finish(loc(654, "ProcessStartupPacket"))?;
        }
        if !write_negotiation_byte(b'N', "GSSAPI")? {
            return Ok(STATUS_ERROR);
        }
        if pqcomm::pq_buffer_remaining_data() > 0 {
            return ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("received unencrypted data after GSSAPI encryption request")
                .errdetail("This could be either a client-software bug or evidence of an attempted man-in-the-middle attack.")
                .finish(loc(681, "ProcessStartupPacket"))
                .map(|()| STATUS_ERROR);
        }
        return process_startup_packet(mcx, false, true);
    }

    init_small::globals::SetFrontendProtocol(proto.min(PG_PROTOCOL_LATEST));

    if pg_protocol_major(proto) < pg_protocol_major(PG_PROTOCOL_EARLIEST)
        || pg_protocol_major(proto) > pg_protocol_major(PG_PROTOCOL_LATEST)
    {
        return ereport(FATAL)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "unsupported frontend protocol {}.{}: server supports {}.0 to {}.{}",
                pg_protocol_major(proto),
                pg_protocol_minor(proto),
                pg_protocol_major(PG_PROTOCOL_EARLIEST),
                pg_protocol_major(PG_PROTOCOL_LATEST),
                pg_protocol_minor(PG_PROTOCOL_LATEST),
            ))
            .finish(loc(707, "ProcessStartupPacket"))
            .map(|()| STATUS_ERROR);
    }

    let mut unrecognized_protocol_options: Vec<String> = Vec::new();
    {
        let mut offset = SIZEOF_PROTOCOL_VERSION as usize;
        let len = len as usize;

        init_small::globals::WithMyProcPort(|p| p.guc_options.clear());

        while offset < len {
            if buf[offset] == 0 {
                break; // found packet terminator
            }
            let name_len = cstr_len(&buf, offset);
            let valoffset = offset + name_len + 1;
            if valoffset >= len {
                break; // missing value, will complain below
            }
            let val_len = cstr_len(&buf, valoffset);
            let name = bytes_str(&buf[offset..offset + name_len]);
            let val = bytes_str(&buf[valoffset..valoffset + val_len]);

            match name.as_str() {
                "database" => {
                    let v = val.clone();
                    init_small::globals::WithMyProcPort(|p| p.database_name = Some(v.clone()));
                }
                "user" => {
                    let v = val.clone();
                    init_small::globals::WithMyProcPort(|p| p.user_name = Some(v.clone()));
                }
                "options" => {
                    let v = val.clone();
                    init_small::globals::WithMyProcPort(|p| p.cmdline_options = Some(v.clone()));
                }
                "replication" => {
                    // am_walsender/am_db_walsender are walsender.c globals;
                    // walsender startup cannot proceed until that unit lands.
                    let is_walsender = val == "database"
                        || match scalar_seams::parse_bool::call(&val) {
                            Some(b) => b,
                            None => {
                                return ereport(FATAL)
                                    .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                                    .errmsg(format!(
                                        "invalid value for parameter \"replication\": \"{val}\""
                                    ))
                                    .errhint("Valid values are: \"false\", 0, \"true\", 1, \"database\".")
                                    .finish(loc(767, "ProcessStartupPacket"))
                                    .map(|()| STATUS_ERROR);
                            }
                        };
                    if is_walsender {
                        panic!(
                            "backend_startup: replication={val} needs am_walsender \
                             (backend-replication-walsender unported)"
                        );
                    }
                }
                _ if name.starts_with("_pq_.") => {
                    unrecognized_protocol_options.push(name.clone());
                }
                _ => {
                    {
                        let n = name.clone();
                        let v = val.clone();
                        init_small::globals::WithMyProcPort(|p| {
                            p.guc_options.push(n.clone());
                            p.guc_options.push(v.clone());
                        });
                    }
                    if name == "application_name" {
                        let cleaned = string_seams::pg_clean_ascii::call(&val, 0)
                            .expect("pg_clean_ascii with alloc_flags=0 cannot fail");
                        init_small::globals::WithMyProcPort(|p| {
                            p.application_name = Some(cleaned.clone())
                        });
                    }
                }
            }
            offset = valoffset + val_len + 1;
        }

        if offset != len - 1 {
            return ereport(FATAL)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("invalid startup packet layout: expected terminator as last byte")
                .finish(loc(811, "ProcessStartupPacket"))
                .map(|()| STATUS_ERROR);
        }

        if pg_protocol_minor(proto) > pg_protocol_minor(PG_PROTOCOL_LATEST)
            || !unrecognized_protocol_options.is_empty()
        {
            send_negotiate_protocol_version(mcx, &unrecognized_protocol_options)?;
        }
    }

    let user_empty =
        init_small::globals::WithMyProcPort(|p| p.user_name.as_deref().unwrap_or("").is_empty());
    if user_empty {
        return ereport(FATAL)
            .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            .errmsg("no PostgreSQL user name specified in startup packet")
            .finish(loc(828, "ProcessStartupPacket"))
            .map(|()| STATUS_ERROR);
    }

    init_small::globals::WithMyProcPort(|p| {
        if p.database_name.as_deref().unwrap_or("").is_empty() {
            p.database_name = p.user_name.clone();
        }
        truncate_namedatalen(&mut p.database_name);
        truncate_namedatalen(&mut p.user_name);
    });

    // if (am_walsender) MyBackendType = B_WAL_SENDER — unreachable, see above.
    miscinit::SetMyBackendType(types_core::BackendType::Backend);

    Ok(STATUS_OK)
}

// Ok(false) = hard socket error, COMMERROR already raised.
fn write_negotiation_byte(byte: u8, which: &str) -> PgResult<bool> {
    let buf = [byte];
    loop {
        let saved_errno = match be_secure_seams::secure_write::call(&buf)? {
            Ok(1) => return Ok(true),
            Ok(_) => None,
            Err(e) if e == libc::EINTR => continue,
            Err(e) => Some(e),
        };
        let mut builder = ereport(COMMERROR);
        if let Some(e) = saved_errno {
            builder = builder.with_saved_errno(e);
        }
        builder
            .errcode_for_socket_access()
            .errmsg(format!("failed to send {which} negotiation response: %m"))
            .finish(loc(609, "ProcessStartupPacket"))?;
        return Ok(false);
    }
}

fn process_cancel_request_packet(pkt: &[u8], pktlen: i32) -> PgResult<()> {
    if (pktlen as usize) < CANCEL_REQUEST_OFFSET_AUTH_CODE {
        return ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid length of cancel request packet")
            .finish(loc(884, "ProcessCancelRequestPacket"));
    }
    let len = pktlen as usize - CANCEL_REQUEST_OFFSET_AUTH_CODE;
    if len == 0 || len > 256 {
        return ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid length of cancel key in cancel request packet")
            .finish(loc(892, "ProcessCancelRequestPacket"));
    }

    let backend_pid = read_be_u32(pkt, CANCEL_REQUEST_OFFSET_BACKEND_PID) as i32;
    procsignal::SendCancelRequest(
        backend_pid,
        &pkt[CANCEL_REQUEST_OFFSET_AUTH_CODE..CANCEL_REQUEST_OFFSET_AUTH_CODE + len],
    );
    Ok(())
}

fn send_negotiate_protocol_version(
    mcx: Mcx<'_>,
    unrecognized_protocol_options: &[String],
) -> PgResult<()> {
    let mut buf = pqformat::pq_beginmessage(mcx, PQMSG_NEGOTIATE_PROTOCOL_VERSION)?;
    pqformat::pq_sendint32(&mut buf, init_small::globals::FrontendProtocol())?;
    pqformat::pq_sendint32(&mut buf, unrecognized_protocol_options.len() as u32)?;
    for opt in unrecognized_protocol_options {
        pqformat::pq_sendstring(&mut buf, opt.as_bytes())?;
    }
    pqformat::pq_endmessage(buf)?;
    // no need to flush, some other message will follow
    Ok(())
}

// C `_exit(1)`s the child; a thread must never `_exit` the shared process,
// so the rendering is a thread-fatal panic (shared memory untouched here —
// the C precondition for skipping cleanup).
pub fn startup_packet_timeout_handler() {
    panic!("terminating backend thread: startup packet timeout (C _exit(1))");
}

pub fn process_startup_packet_die() {
    panic!("terminating backend thread: SIGTERM during startup packet (C _exit(1))");
}

// Err carries the GUC_check_errdetail text.
pub fn validate_log_connections_options(elemlist: &[String]) -> Result<u32, String> {
    const COMPAT_OPTIONS: [(&str, u32); 8] = [
        ("off", 0),
        ("false", 0),
        ("no", 0),
        ("0", 0),
        ("on", LOG_CONNECTION_ON),
        ("true", LOG_CONNECTION_ON),
        ("yes", LOG_CONNECTION_ON),
        ("1", LOG_CONNECTION_ON),
    ];

    if elemlist.is_empty() {
        return Ok(0);
    }

    let item = &elemlist[0];
    for (name, val) in COMPAT_OPTIONS {
        if !item.eq_ignore_ascii_case(name) {
            continue;
        }
        if elemlist.len() > 1 {
            return Err(format!(
                "Cannot specify log_connections option \"{item}\" in a list with other options."
            ));
        }
        return Ok(val);
    }

    const OPTIONS: [(&str, u32); 5] = [
        ("receipt", LOG_CONNECTION_RECEIPT),
        ("authentication", LOG_CONNECTION_AUTHENTICATION),
        ("authorization", LOG_CONNECTION_AUTHORIZATION),
        ("setup_durations", LOG_CONNECTION_SETUP_DURATIONS),
        ("all", LOG_CONNECTION_ALL),
    ];

    let mut flags = 0u32;
    'outer: for item in elemlist {
        for (name, val) in OPTIONS {
            if item.eq_ignore_ascii_case(name) {
                flags |= val;
                continue 'outer;
            }
        }
        return Err(format!("Invalid option \"{item}\"."));
    }
    Ok(flags)
}

pub fn check_log_connections(mcx: Mcx<'_>, newval: &str) -> PgResult<Result<u32, String>> {
    // mbutils is ported; the fallback covers early-boot callers that run
    // before init_seams installs get_database_encoding (same SQL_ASCII
    // default C reports pre-initialization).
    let encoding = if mbutils_seams::get_database_encoding::is_installed() {
        mbutils_seams::get_database_encoding::call()
    } else {
        wchar::PG_SQL_ASCII
    };
    let Some(elemlist) = varlena::split_identifier_string(mcx, newval, b',', encoding)? else {
        return Ok(Err(format!(
            "Invalid list syntax in parameter \"{}\".",
            "log_connections"
        )));
    };
    Ok(validate_log_connections_options(&elemlist))
}

pub fn assign_log_connections(extra: u32) {
    log_connections::set(extra);
}

fn check_log_connections_hook(
    newval: &mut Option<String>,
    extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    let raw = newval.clone().unwrap_or_default();
    let scratch = MemoryContext::new("check_log_connections");
    match check_log_connections(scratch.mcx(), &raw)? {
        Ok(flags) => {
            *extra = Some(Box::new(flags));
            Ok(true)
        }
        Err(detail) => {
            guc_seams::guc_check_errdetail::call(detail);
            Ok(false)
        }
    }
}

fn assign_log_connections_hook(_newval: Option<&str>, extra: Option<&guc_tables::GucHookExtra>) {
    if let Some(flags) = extra.and_then(|e| e.downcast_ref::<u32>()) {
        assign_log_connections(*flags);
    }
}

fn zeroed_vec(mcx: Mcx<'_>, len: usize) -> PgResult<PgVec<'_, u8>> {
    let mut v = mcx::vec_with_capacity_in(mcx, len)?;
    // SAFETY: capacity >= len from the reserve; bytes zero-initialized before
    // set_len.
    unsafe {
        core::ptr::write_bytes(v.as_mut_ptr(), 0, len);
        v.set_len(len);
    }
    Ok(v)
}

fn read_be_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(buf[off..off + 4].try_into().unwrap())
}

fn cstr_len(buf: &[u8], off: usize) -> usize {
    buf[off..].iter().position(|&b| b == 0).unwrap_or(buf.len() - off)
}

fn bytes_str(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn strspn(s: &str, accept: &[u8]) -> usize {
    s.bytes().take_while(|b| accept.contains(b)).count()
}

fn truncate_namedatalen(name: &mut Option<String>) {
    if let Some(n) = name {
        if n.len() >= NAMEDATALEN {
            let mut cut = NAMEDATALEN - 1;
            while !n.is_char_boundary(cut) {
                cut -= 1;
            }
            n.truncate(cut);
        }
    }
}

fn gai_strerror(errcode: i32) -> String {
    // SAFETY: gai_strerror returns a pointer to a static NUL-terminated
    // message for any error code.
    unsafe {
        std::ffi::CStr::from_ptr(libc::gai_strerror(errcode))
            .to_string_lossy()
            .into_owned()
    }
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::Trace_connection_negotiation.install(GucVarAccessors {
        get: trace_connection_negotiation::get,
        set: trace_connection_negotiation::set,
    });
    guc_tables::vars::log_connections_string.install(GucVarAccessors {
        get: globals::log_connections_string::get,
        set: globals::log_connections_string::set,
    });
    guc_tables::hooks::check_log_connections.install(check_log_connections_hook);
    guc_tables::hooks::assign_log_connections.install(assign_log_connections_hook);
}
