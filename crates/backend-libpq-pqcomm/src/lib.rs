//! Port of `src/backend/libpq/pqcomm.c` — communication functions between the
//! frontend and the backend: the listen/accept socket plumbing, the
//! per-backend send/receive buffers, the protocol-message framing, and the
//! TCP keepalive knobs.
//!
//! Mapping notes (vs the C):
//!
//! - The file-static buffers/flags (`PqSendBuffer`, `PqRecvBuffer`,
//!   `PqCommBusy`, ...) are per-backend state and live in `thread_local!`
//!   (AGENTS.md "Backend-global state"). The send buffer is a `Vec<u8>` whose
//!   length is `PqSendBufferSize`; it is process-lifetime
//!   (`TopMemoryContext` in C), so it uses the global allocator with
//!   `try_reserve` for the fallible allocating steps, like the elog port.
//! - `MyProcPort` is owned by globals.c (unported); every touch goes through
//!   the `backend_utils_init_small_seams::with_my_proc_port` callback seam.
//! - `ereport(ERROR/FATAL)` paths surface as `Err(PgError)` per the repo's
//!   PgResult divergence; COMMERROR logs and continues, exactly as in C.
//! - Functions whose C body can reach `socket_set_nonblocking` (which
//!   `ereport(ERROR)`s on a NULL `MyProcPort`) or `secure_read`/`secure_write`
//!   (whose wait loops can raise through interrupt processing) return
//!   `PgResult`; socket trouble itself is the C `EOF` (-1) in `Ok`.
//! - Windows-only code (`pq_setkeepaliveswin32`, the WIN32 SO_SNDBUF tuning,
//!   `SIO_KEEPALIVE_VALS`) is not ported; `TCP_USER_TIMEOUT` is Linux-only,
//!   so other targets take the C `#else` ("not supported") arms.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::ffi::CString;

use backend_utils_error::ereport;
use types_core::{pgsocket, PGINVALID_SOCKET, STATUS_ERROR, STATUS_OK};
use types_datum::Datum;
use types_error::{
    ErrorLocation, PgResult, COMMERROR, ERRCODE_CONNECTION_DOES_NOT_EXIST,
    ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_PROTOCOL_VIOLATION, ERROR,
    FATAL, LOG,
};
use types_net::{AddrInfoHint, ClientSocket, PgAddrInfo, Port, SockAddr};
use types_storage::waiteventset::{
    WaitEvent, WaitEventSetHandle, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_SOCKET_CLOSED,
    WL_SOCKET_WRITEABLE,
};

pub mod config;

#[cfg(test)]
mod tests;

/// C `EOF` (`<stdio.h>`): the trouble sentinel of the get/flush routines.
pub const EOF: i32 = -1;

pub const PQ_SEND_BUFFER_SIZE: usize = 8192;
pub const PQ_RECV_BUFFER_SIZE: usize = 8192;

/// `FeBeWaitSetSocketPos` / `FeBeWaitSetLatchPos` / `FeBeWaitSetNEvents`
/// (libpq/libpq.h).
pub const FeBeWaitSetSocketPos: i32 = 0;
pub const FeBeWaitSetLatchPos: i32 = 1;
pub const FeBeWaitSetNEvents: usize = 3;

// ---------------------------------------------------------------------------
// errno access (the C code communicates with secure_read/secure_write through
// the process errno, and so do the installed seam implementations).
// ---------------------------------------------------------------------------

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno_location() -> *mut i32 {
    unsafe { libc::__error() }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn errno_location() -> *mut i32 {
    unsafe { libc::__errno_location() }
}

fn errno() -> i32 {
    unsafe { *errno_location() }
}

fn set_errno(v: i32) {
    unsafe { *errno_location() = v }
}

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("pqcomm.c", 0, funcname)
}

// ---------------------------------------------------------------------------
// The file-static state.
// ---------------------------------------------------------------------------

struct PqCommState {
    /// `PqSendBuffer` + `PqSendBufferSize`: `send_buffer.len()` is the
    /// allocated size (usually 8k, enlarged by `pq_putmessage_noblock`).
    send_buffer: Vec<u8>,
    send_buffer_size: usize,
    /// `PqSendPointer` — next index to store a byte.
    send_pointer: usize,
    /// `PqSendStart` — next index to send a byte.
    send_start: usize,

    /// `PqRecvBuffer` (fixed size).
    recv_buffer: [u8; PQ_RECV_BUFFER_SIZE],
    /// `PqRecvPointer` — next index to read a byte.
    recv_pointer: i32,
    /// `PqRecvLength` — end of available data.
    recv_length: i32,

    /// `PqCommBusy` — busy sending data to the client.
    comm_busy: bool,
    /// `PqCommReadingMsg` — in the middle of reading a message.
    comm_reading_msg: bool,
}

impl PqCommState {
    const fn new() -> Self {
        PqCommState {
            send_buffer: Vec::new(),
            send_buffer_size: 0,
            send_pointer: 0,
            send_start: 0,
            recv_buffer: [0; PQ_RECV_BUFFER_SIZE],
            recv_pointer: 0,
            recv_length: 0,
            comm_busy: false,
            comm_reading_msg: false,
        }
    }
}

thread_local! {
    static PQ: RefCell<PqCommState> = const { RefCell::new(PqCommState::new()) };
    /// internal_flush_buffer's `static int last_reported_send_errno`.
    static LAST_REPORTED_SEND_ERRNO: Cell<i32> = const { Cell::new(0) };
    /// `static List *sock_paths` — Unix socket file paths for maintenance.
    static SOCK_PATHS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    /// `WaitEventSet *FeBeWaitSet`.
    static FE_BE_WAIT_SET: Cell<Option<WaitEventSetHandle>> = const { Cell::new(None) };
}

/// `FeBeWaitSet` — the backend's socket/latch/postmaster-death wait set,
/// created by [`pq_init`].
pub fn fe_be_wait_set() -> Option<WaitEventSetHandle> {
    FE_BE_WAIT_SET.with(Cell::get)
}

fn comm_busy() -> bool {
    PQ.with(|s| s.borrow().comm_busy)
}

fn set_comm_busy(v: bool) {
    PQ.with(|s| s.borrow_mut().comm_busy = v);
}

// ---------------------------------------------------------------------------
// sockaddr helpers over the owned `SockAddr` byte buffer.
// ---------------------------------------------------------------------------

fn sockaddr_storage_from(sa: &SockAddr) -> libc::sockaddr_storage {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let n = (sa.salen as usize)
        .min(std::mem::size_of::<libc::sockaddr_storage>())
        .min(sa.addr.len());
    unsafe {
        std::ptr::copy_nonoverlapping(sa.addr.as_ptr(), (&mut ss as *mut libc::sockaddr_storage).cast::<u8>(), n);
    }
    ss
}

/// `addr.ss_family` of an owned `SockAddr`.
fn sockaddr_family(sa: &SockAddr) -> i32 {
    sockaddr_storage_from(sa).ss_family as i32
}

// ---------------------------------------------------------------------------
// MyProcPort access (globals.c, via seam) and the secure_* transport.
// ---------------------------------------------------------------------------

fn with_my_proc_port(f: &mut dyn FnMut(Option<&mut Port>)) {
    backend_utils_init_small_seams::with_my_proc_port::call(f);
}

fn secure_read_my_port(buf: &mut [u8]) -> PgResult<isize> {
    let mut res: Option<PgResult<isize>> = None;
    with_my_proc_port(&mut |port| {
        if let Some(port) = port {
            res = Some(backend_libpq_be_secure_seams::secure_read::call(port, buf));
        }
    });
    // The preceding socket_set_nonblocking() already raised the no-client-
    // connection ERROR; reaching here without a Port is the C NULL deref.
    res.expect("pqcomm: secure_read with no client connection (MyProcPort is NULL)")
}

fn secure_write_my_port(buf: &[u8]) -> PgResult<isize> {
    let mut res: Option<PgResult<isize>> = None;
    with_my_proc_port(&mut |port| {
        if let Some(port) = port {
            res = Some(backend_libpq_be_secure_seams::secure_write::call(port, buf));
        }
    });
    res.expect("pqcomm: secure_write with no client connection (MyProcPort is NULL)")
}

// ---------------------------------------------------------------------------
// pq_init - initialize libpq at backend startup
// ---------------------------------------------------------------------------

/// `pq_init(client_sock)` — allocate and fill the connection `Port` (the
/// caller stores it as `MyProcPort`), apply TCP options, initialize the
/// message buffers, register the exit hook, switch the socket to non-blocking
/// mode, and build `FeBeWaitSet`.
pub fn pq_init(client_sock: &ClientSocket) -> PgResult<Port> {
    // allocate the Port struct and copy the ClientSocket contents to it
    let mut port = Port::zeroed();
    port.sock = client_sock.sock;
    let salen = (client_sock.raddr.salen as usize).min(port.raddr.addr.len());
    port.raddr.addr[..salen].copy_from_slice(&client_sock.raddr.addr[..salen]);
    port.raddr.salen = client_sock.raddr.salen;

    // fill in the server (local) address
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut slen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    if unsafe {
        libc::getsockname(
            port.sock,
            (&mut ss as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
            &mut slen,
        )
    } < 0
    {
        let e = errno();
        ereport(FATAL)
            .with_saved_errno(e)
            .errmsg("getsockname() failed: %m")
            .finish(loc("pq_init"))?;
    }
    let n = (slen as usize).min(port.laddr.addr.len());
    port.laddr.addr[..n].copy_from_slice(unsafe {
        std::slice::from_raw_parts((&ss as *const libc::sockaddr_storage).cast::<u8>(), n)
    });
    port.laddr.salen = slen;

    // select NODELAY and KEEPALIVE options if it's a TCP connection
    if sockaddr_family(&port.laddr) != libc::AF_UNIX {
        let on: libc::c_int = 1;
        if unsafe {
            libc::setsockopt(
                port.sock,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                (&on as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        } < 0
        {
            let e = errno();
            ereport(FATAL)
                .with_saved_errno(e)
                .errmsg("setsockopt(TCP_NODELAY) failed: %m")
                .finish(loc("pq_init"))?;
        }
        let on: libc::c_int = 1;
        if unsafe {
            libc::setsockopt(
                port.sock,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                (&on as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        } < 0
        {
            let e = errno();
            ereport(FATAL)
                .with_saved_errno(e)
                .errmsg("setsockopt(SO_KEEPALIVE) failed: %m")
                .finish(loc("pq_init"))?;
        }

        // Also apply the current keepalive parameters. If we fail to set a
        // parameter, don't error out, because these aren't universally
        // supported (the show hooks report the kernel truth anyway).
        let _ = pq_setkeepalivesidle(config::tcp_keepalives_idle(), Some(&mut port));
        let _ = pq_setkeepalivesinterval(config::tcp_keepalives_interval(), Some(&mut port));
        let _ = pq_setkeepalivescount(config::tcp_keepalives_count(), Some(&mut port));
        let _ = pq_settcpusertimeout(config::tcp_user_timeout(), Some(&mut port));
    }

    // initialize state variables
    PQ.with(|s| -> PgResult<()> {
        let mut st = s.borrow_mut();
        st.send_buffer = Vec::new();
        if st.send_buffer.try_reserve_exact(PQ_SEND_BUFFER_SIZE).is_err() {
            return Err(out_of_memory_error(PQ_SEND_BUFFER_SIZE, "pq_init"));
        }
        st.send_buffer.resize(PQ_SEND_BUFFER_SIZE, 0);
        st.send_buffer_size = PQ_SEND_BUFFER_SIZE;
        st.send_pointer = 0;
        st.send_start = 0;
        st.recv_pointer = 0;
        st.recv_length = 0;
        st.comm_busy = false;
        st.comm_reading_msg = false;
        Ok(())
    })?;

    // set up process-exit hook to close the socket
    backend_storage_ipc_seams::on_proc_exit::call(socket_close, Datum::from_usize(0));

    // In backends (as soon as forked) we operate the underlying socket in
    // nonblocking mode and use latches to implement blocking semantics if
    // needed. That allows us to provide safely interruptible reads and
    // writes.
    if !port_noblock_seams::pg_set_noblock::call(port.sock) {
        let e = errno();
        ereport(FATAL)
            .with_saved_errno(e)
            .errmsg("could not set socket to nonblocking mode: %m")
            .finish(loc("pq_init"))?;
    }

    // Don't give the socket to any subprograms we execute.
    if unsafe { libc::fcntl(port.sock, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        let e = errno();
        ereport(FATAL)
            .with_saved_errno(e)
            .errmsg_internal("fcntl(F_SETFD) failed on socket: %m")
            .finish(loc("pq_init"))?;
    }

    let set = backend_storage_ipc_waiteventset_seams::create_wait_event_set::call(
        FeBeWaitSetNEvents as i32,
    )?;
    let socket_pos = backend_storage_ipc_waiteventset_seams::add_wait_event_to_set::call(
        set,
        WL_SOCKET_WRITEABLE,
        port.sock,
        false,
    )?;
    let latch_pos = backend_storage_ipc_waiteventset_seams::add_wait_event_to_set::call(
        set,
        WL_LATCH_SET,
        PGINVALID_SOCKET,
        true,
    )?;
    backend_storage_ipc_waiteventset_seams::add_wait_event_to_set::call(
        set,
        WL_POSTMASTER_DEATH,
        PGINVALID_SOCKET,
        false,
    )?;
    FE_BE_WAIT_SET.with(|c| c.set(Some(set)));

    // The event positions match the order we added them.
    debug_assert_eq!(socket_pos, FeBeWaitSetSocketPos);
    debug_assert_eq!(latch_pos, FeBeWaitSetLatchPos);

    Ok(port)
}

fn out_of_memory_error(size: usize, funcname: &str) -> types_error::PgError {
    match ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .errdetail(format!("Failed on request of size {}.", size))
        .finish(loc(funcname))
    {
        Err(e) => e,
        Ok(()) => unreachable!("ERROR-level report returned Ok"),
    }
}

// ---------------------------------------------------------------------------
// socket_comm_reset / socket_close
// ---------------------------------------------------------------------------

/// `socket_comm_reset` — reset libpq during error recovery. Does NOT throw
/// away pending data; only resets the busy flag.
fn socket_comm_reset() {
    set_comm_busy(false);
}

/// `socket_close(code, arg)` — shutdown libpq at backend exit (the
/// `on_proc_exit` callback registered by [`pq_init`]). Must be safe to run at
/// any instant.
fn socket_close(_code: i32, _arg: Datum) {
    // Nothing to do in a standalone backend, where MyProcPort is NULL.
    with_my_proc_port(&mut |port| {
        if let Some(port) = port {
            // (ENABLE_GSS is not defined in this build: no GSSAPI shutdown.)

            // Cleanly shut down SSL layer.
            backend_libpq_be_secure_seams::secure_close::call(port);

            // Leave the socket open until the process dies, so clients can
            // wait for transport-level closure; just prevent further I/O.
            port.sock = PGINVALID_SOCKET;
        }
    });
}

// ---------------------------------------------------------------------------
// Postmaster socket functions: ListenServerPort / AcceptConnection /
// TouchSocketFiles / RemoveSocketFiles.
// ---------------------------------------------------------------------------

/// `sizeof(((struct sockaddr_un *) NULL)->sun_path)`.
fn unixsock_path_buflen() -> usize {
    let su: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    su.sun_path.len()
}

/// `ListenServerPort` — open a "listening" port to accept connections.
///
/// `family` is `AF_UNIX` or `AF_UNSPEC`. Opened sockets are appended to
/// `listen_sockets` (C's `ListenSockets[]` + `*NumListenSockets`);
/// `max_listen` is C's `MaxListen`. Returns `STATUS_OK` / `STATUS_ERROR`.
pub fn ListenServerPort(
    family: i32,
    host_name: Option<&str>,
    port_number: u16,
    unix_socket_dir: Option<&str>,
    listen_sockets: &mut Vec<pgsocket>,
    max_listen: usize,
) -> PgResult<i32> {
    let hint = AddrInfoHint {
        flags: libc::AI_PASSIVE,
        family,
        socktype: libc::SOCK_STREAM,
    };

    let mut unix_socket_path = String::new();
    let service: String;
    if family == libc::AF_UNIX {
        // Create unixSocketPath from portNumber and unixSocketDir and lock
        // that file path. (UNIXSOCK_PATH asserts a non-empty sockdir.)
        let dir = unix_socket_dir.expect("ListenServerPort: AF_UNIX requires unixSocketDir");
        debug_assert!(!dir.is_empty());
        unix_socket_path = format!("{}/.s.PGSQL.{}", dir, port_number);
        if unix_socket_path.len() >= unixsock_path_buflen() {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "Unix-domain socket path \"{}\" is too long (maximum {} bytes)",
                    unix_socket_path,
                    unixsock_path_buflen() - 1
                ))
                .finish(loc("ListenServerPort"));
            return Ok(STATUS_ERROR);
        }
        if Lock_AF_UNIX(dir, &unix_socket_path)? != STATUS_OK {
            return Ok(STATUS_ERROR);
        }
        service = unix_socket_path.clone();
    } else {
        service = format!("{}", port_number);
    }

    let mut addrs: Vec<PgAddrInfo> = Vec::new();
    let ret = common_ip_seams::pg_getaddrinfo_all::call(host_name, Some(&service), &hint, &mut addrs);
    if ret != 0 || addrs.is_empty() {
        let gai = gai_strerror_string(ret);
        let _ = match host_name {
            Some(host_name) => ereport(LOG).errmsg(format!(
                "could not translate host name \"{}\", service \"{}\" to address: {}",
                host_name, service, gai
            )),
            None => ereport(LOG).errmsg(format!(
                "could not translate service \"{}\" to address: {}",
                service, gai
            )),
        }
        .finish(loc("ListenServerPort"));
        return Ok(STATUS_ERROR);
    }

    let mut added = 0usize;
    for addr in &addrs {
        if family != libc::AF_UNIX && addr.family == libc::AF_UNIX {
            // Only set up a unix domain socket when they really asked for it.
            // The service/port is different in that case.
            continue;
        }

        // See if there is still room to add 1 more socket.
        if listen_sockets.len() == max_listen {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "could not bind to all requested addresses: MAXLISTEN ({}) exceeded",
                    max_listen
                ))
                .finish(loc("ListenServerPort"));
            break;
        }

        // set up address family name for log messages
        let family_desc: String = match addr.family {
            x if x == libc::AF_INET => "IPv4".to_owned(),
            x if x == libc::AF_INET6 => "IPv6".to_owned(),
            x if x == libc::AF_UNIX => "Unix".to_owned(),
            other => format!("unrecognized address family {}", other),
        };

        // set up text form of address for log messages
        let addr_desc: String = if addr.family == libc::AF_UNIX {
            unix_socket_path.clone()
        } else {
            let mut node = String::new();
            common_ip_seams::pg_getnameinfo_all::call(
                &addr.addr,
                Some(&mut node),
                None,
                libc::NI_NUMERICHOST,
            );
            node
        };

        let fd = unsafe { libc::socket(addr.family, libc::SOCK_STREAM, 0) };
        if fd == PGINVALID_SOCKET {
            let e = errno();
            let _ = ereport(LOG)
                .with_saved_errno(e)
                .errcode_for_socket_access()
                .errmsg(format!(
                    "could not create {} socket for address \"{}\": %m",
                    family_desc, addr_desc
                ))
                .finish(loc("ListenServerPort"));
            continue;
        }

        // Don't give the listen socket to any subprograms we execute.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            let e = errno();
            ereport(FATAL)
                .with_saved_errno(e)
                .errmsg_internal("fcntl(F_SETFD) failed on socket: %m")
                .finish(loc("ListenServerPort"))?;
        }

        // Without the SO_REUSEADDR flag, a new postmaster can't be started
        // right away after a stop or crash.
        let one: libc::c_int = 1;
        if addr.family != libc::AF_UNIX {
            if unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_REUSEADDR,
                    (&one as *const libc::c_int).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            } == -1
            {
                let e = errno();
                let _ = ereport(LOG)
                    .with_saved_errno(e)
                    .errcode_for_socket_access()
                    .errmsg(format!(
                        "setsockopt(SO_REUSEADDR) failed for {} address \"{}\": %m",
                        family_desc, addr_desc
                    ))
                    .finish(loc("ListenServerPort"));
                unsafe { libc::close(fd) };
                continue;
            }
        }

        if addr.family == libc::AF_INET6 {
            if unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_V6ONLY,
                    (&one as *const libc::c_int).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            } == -1
            {
                let e = errno();
                let _ = ereport(LOG)
                    .with_saved_errno(e)
                    .errcode_for_socket_access()
                    .errmsg(format!(
                        "setsockopt(IPV6_V6ONLY) failed for {} address \"{}\": %m",
                        family_desc, addr_desc
                    ))
                    .finish(loc("ListenServerPort"));
                unsafe { libc::close(fd) };
                continue;
            }
        }

        let ss = sockaddr_storage_from(&addr.addr);
        let err = unsafe {
            libc::bind(
                fd,
                (&ss as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
                addr.addr.salen as libc::socklen_t,
            )
        };
        if err < 0 {
            let saved_errno = errno();
            let mut b = ereport(LOG)
                .with_saved_errno(saved_errno)
                .errcode_for_socket_access()
                .errmsg(format!(
                    "could not bind {} address \"{}\": %m",
                    family_desc, addr_desc
                ));
            if saved_errno == libc::EADDRINUSE {
                b = if addr.family == libc::AF_UNIX {
                    b.errhint(format!(
                        "Is another postmaster already running on port {}?",
                        port_number
                    ))
                } else {
                    b.errhint(format!(
                        "Is another postmaster already running on port {}? If not, wait a few seconds and retry.",
                        port_number
                    ))
                };
            }
            let _ = b.finish(loc("ListenServerPort"));
            unsafe { libc::close(fd) };
            continue;
        }

        if addr.family == libc::AF_UNIX {
            if Setup_AF_UNIX(&service)? != STATUS_OK {
                unsafe { libc::close(fd) };
                break;
            }
        }

        // Select appropriate accept-queue length limit: similar to the
        // maximum number of child processes the postmaster will permit.
        let maxconn = backend_utils_init_small_seams::max_connections::call() * 2;

        let err = unsafe { libc::listen(fd, maxconn) };
        if err < 0 {
            let e = errno();
            let _ = ereport(LOG)
                .with_saved_errno(e)
                .errcode_for_socket_access()
                .errmsg(format!(
                    "could not listen on {} address \"{}\": %m",
                    family_desc, addr_desc
                ))
                .finish(loc("ListenServerPort"));
            unsafe { libc::close(fd) };
            continue;
        }

        let _ = if addr.family == libc::AF_UNIX {
            ereport(LOG).errmsg(format!("listening on Unix socket \"{}\"", addr_desc))
        } else {
            ereport(LOG).errmsg(format!(
                "listening on {} address \"{}\", port {}",
                family_desc, addr_desc, port_number
            ))
        }
        .finish(loc("ListenServerPort"));

        listen_sockets.push(fd);
        added += 1;
    }

    if added == 0 {
        return Ok(STATUS_ERROR);
    }
    Ok(STATUS_OK)
}

/// `Lock_AF_UNIX` — grab the socket lock file and remember the path.
fn Lock_AF_UNIX(unix_socket_dir: &str, unix_socket_path: &str) -> PgResult<i32> {
    // no lock file for abstract sockets
    if unix_socket_path.starts_with('@') {
        return Ok(STATUS_OK);
    }

    // Grab an interlock file associated with the socket file; with it held we
    // can safely delete any pre-existing socket file to avoid failure at
    // bind() time.
    backend_utils_init_miscinit_seams::create_socket_lock_file::call(
        unix_socket_path,
        true,
        unix_socket_dir,
    )?;

    let c = CString::new(unix_socket_path).expect("socket path contains NUL");
    unsafe { libc::unlink(c.as_ptr()) };

    // Remember socket file pathnames for later maintenance.
    SOCK_PATHS.with(|p| -> PgResult<()> {
        let mut paths = p.borrow_mut();
        if paths.try_reserve(1).is_err() {
            return Err(out_of_memory_error(unix_socket_path.len(), "Lock_AF_UNIX"));
        }
        paths.push(unix_socket_path.to_owned());
        Ok(())
    })?;

    Ok(STATUS_OK)
}

/// C `strtoul(s, &endptr, 10)` with the `*endptr == '\0'` full-consumption
/// test: `Some(value)` only when the whole string is a (possibly signed,
/// whitespace-prefixed) decimal number.
fn parse_strtoul_full(s: &str) -> Option<u64> {
    let bytes = s.as_bytes();
    let mut i = 0;
    // C-locale isspace(): space, \t, \n, \v, \f, \r.
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    let mut negative = false;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        negative = bytes[i] == b'-';
        i += 1;
    }
    let start = i;
    let mut value: u64 = 0;
    let mut overflowed = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        let (mul, o1) = value.overflowing_mul(10);
        let (add, o2) = mul.overflowing_add(u64::from(bytes[i] - b'0'));
        overflowed = overflowed || o1 || o2;
        value = add;
        i += 1;
    }
    if i == start || i != bytes.len() {
        return None;
    }
    if overflowed {
        // strtoul clamps to ULONG_MAX (and sets ERANGE) on overflow; the C
        // caller ignores errno and uses the clamped value.
        return Some(u64::MAX);
    }
    Some(if negative { value.wrapping_neg() } else { value })
}

/// `Setup_AF_UNIX` — configure unix socket ownership/permissions.
fn Setup_AF_UNIX(sock_path: &str) -> PgResult<i32> {
    // no file system permissions for abstract sockets
    if sock_path.starts_with('@') {
        return Ok(STATUS_OK);
    }

    let path_c = CString::new(sock_path).expect("socket path contains NUL");

    // Fix socket ownership/permission if requested. Must happen before
    // listen() to avoid a window where unwanted connections could get
    // accepted.
    let group = config::unix_socket_group();
    if !group.is_empty() {
        let gid: libc::gid_t = if let Some(val) = parse_strtoul_full(&group) {
            // numeric group id
            val as libc::gid_t
        } else {
            // convert group name to id
            let group_c = CString::new(group.as_str()).expect("group name contains NUL");
            let gr = unsafe { libc::getgrnam(group_c.as_ptr()) };
            if gr.is_null() {
                let _ = ereport(LOG)
                    .errmsg(format!("group \"{}\" does not exist", group))
                    .finish(loc("Setup_AF_UNIX"));
                return Ok(STATUS_ERROR);
            }
            unsafe { (*gr).gr_gid }
        };
        if unsafe { libc::chown(path_c.as_ptr(), libc::uid_t::MAX /* (uid_t) -1 */, gid) } == -1 {
            let e = errno();
            let _ = ereport(LOG)
                .with_saved_errno(e)
                .errcode_for_file_access()
                .errmsg(format!("could not set group of file \"{}\": %m", sock_path))
                .finish(loc("Setup_AF_UNIX"));
            return Ok(STATUS_ERROR);
        }
    }

    if unsafe { libc::chmod(path_c.as_ptr(), config::unix_socket_permissions() as libc::mode_t) }
        == -1
    {
        let e = errno();
        let _ = ereport(LOG)
            .with_saved_errno(e)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not set permissions of file \"{}\": %m",
                sock_path
            ))
            .finish(loc("Setup_AF_UNIX"));
        return Ok(STATUS_ERROR);
    }
    Ok(STATUS_OK)
}

fn gai_strerror_string(err: i32) -> String {
    let p = unsafe { libc::gai_strerror(err) };
    if p.is_null() {
        return format!("getaddrinfo error {}", err);
    }
    unsafe { std::ffi::CStr::from_ptr(p) }
        .to_string_lossy()
        .into_owned()
}

/// `AcceptConnection` — accept a new connection on `server_fd`, filling
/// `client_sock`. Returns `STATUS_OK` / `STATUS_ERROR`.
pub fn AcceptConnection(server_fd: pgsocket, client_sock: &mut ClientSocket) -> i32 {
    // accept connection and fill in the client (remote) address
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut slen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    client_sock.raddr.salen = client_sock.raddr.addr.len() as u32;
    let fd = unsafe {
        libc::accept(
            server_fd,
            (&mut ss as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
            &mut slen,
        )
    };
    if fd == PGINVALID_SOCKET {
        let e = errno();
        client_sock.sock = PGINVALID_SOCKET;
        let _ = ereport(LOG)
            .with_saved_errno(e)
            .errcode_for_socket_access()
            .errmsg("could not accept new connection: %m")
            .finish(loc("AcceptConnection"));

        // If accept() fails then postmaster.c will still see the server
        // socket as read-ready, and will immediately try again. To avoid
        // uselessly sucking lots of CPU, delay a bit before trying again.
        port_pgsleep_seams::pg_usleep::call(100000); // wait 0.1 sec
        return STATUS_ERROR;
    }
    let n = (slen as usize).min(client_sock.raddr.addr.len());
    client_sock.raddr.addr[..n].copy_from_slice(unsafe {
        std::slice::from_raw_parts((&ss as *const libc::sockaddr_storage).cast::<u8>(), n)
    });
    client_sock.raddr.salen = slen;
    client_sock.sock = fd;

    STATUS_OK
}

/// `TouchSocketFiles` — mark socket files as recently accessed, protecting
/// them from /tmp-directory cleaners.
pub fn TouchSocketFiles() {
    SOCK_PATHS.with(|p| {
        for sock_path in p.borrow().iter() {
            // Ignore errors; there's no point in complaining
            if let Ok(c) = CString::new(sock_path.as_str()) {
                unsafe { libc::utime(c.as_ptr(), std::ptr::null()) };
            }
        }
    });
}

/// `RemoveSocketFiles` — unlink socket files at postmaster shutdown.
pub fn RemoveSocketFiles() {
    SOCK_PATHS.with(|p| {
        let mut paths = p.borrow_mut();
        for sock_path in paths.iter() {
            // Ignore any error.
            if let Ok(c) = CString::new(sock_path.as_str()) {
                unsafe { libc::unlink(c.as_ptr()) };
            }
        }
        paths.clear();
    });
}

// ---------------------------------------------------------------------------
// Low-level I/O routines.
// ---------------------------------------------------------------------------

/// `socket_set_nonblocking` — set `MyProcPort->noblock`; `ereport(ERROR)` if
/// there is no client connection.
fn socket_set_nonblocking(nonblocking: bool) -> PgResult<()> {
    let mut have_port = false;
    with_my_proc_port(&mut |port| {
        if let Some(port) = port {
            port.noblock = nonblocking;
            have_port = true;
        }
    });
    if !have_port {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST)
            .errmsg("there is no client connection")
            .finish(loc("socket_set_nonblocking"))?;
    }
    Ok(())
}

/// `pq_recvbuf` — load some bytes into the input buffer. `Ok(0)` if OK,
/// `Ok(EOF)` if trouble.
fn pq_recvbuf() -> PgResult<i32> {
    PQ.with(|s| {
        let mut st = s.borrow_mut();
        if st.recv_pointer > 0 {
            if st.recv_length > st.recv_pointer {
                // still some unread data, left-justify it in the buffer
                let p = st.recv_pointer as usize;
                let l = st.recv_length as usize;
                st.recv_buffer.copy_within(p..l, 0);
                st.recv_length -= st.recv_pointer;
                st.recv_pointer = 0;
            } else {
                st.recv_length = 0;
                st.recv_pointer = 0;
            }
        }
    });

    // Ensure that we're in blocking mode
    socket_set_nonblocking(false)?;

    // Can fill buffer from PqRecvLength and upwards
    loop {
        let start = PQ.with(|s| s.borrow().recv_length) as usize;
        let mut scratch = [0u8; PQ_RECV_BUFFER_SIZE];

        set_errno(0);
        let r = secure_read_my_port(&mut scratch[..PQ_RECV_BUFFER_SIZE - start])?;

        if r < 0 {
            let e = errno();
            if e == libc::EINTR {
                continue; // Ok if interrupted
            }

            // Careful: an ereport() that tries to write to the client would
            // cause recursion to here; this message must go *only* to the
            // postmaster log (COMMERROR). If errno is zero, assume it's EOF
            // and let the caller complain.
            if e != 0 {
                let _ = ereport(COMMERROR)
                    .with_saved_errno(e)
                    .errcode_for_socket_access()
                    .errmsg("could not receive data from client: %m")
                    .finish(loc("pq_recvbuf"));
            }
            return Ok(EOF);
        }
        if r == 0 {
            // EOF detected. The ultimate caller logs it.
            return Ok(EOF);
        }
        // r contains number of bytes read, so just incr length
        PQ.with(|s| {
            let mut st = s.borrow_mut();
            let r = r as usize;
            st.recv_buffer[start..start + r].copy_from_slice(&scratch[..r]);
            st.recv_length += r as i32;
        });
        return Ok(0);
    }
}

/// `pq_getbyte` — get a single byte from connection, or return `EOF`.
pub fn pq_getbyte() -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    while PQ.with(|s| {
        let st = s.borrow();
        st.recv_pointer >= st.recv_length
    }) {
        if pq_recvbuf()? != 0 {
            return Ok(EOF); // Failed to recv data
        }
    }
    Ok(PQ.with(|s| {
        let mut st = s.borrow_mut();
        let b = st.recv_buffer[st.recv_pointer as usize];
        st.recv_pointer += 1;
        b as i32
    }))
}

/// `pq_peekbyte` — peek at next byte from connection without advancing.
pub fn pq_peekbyte() -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    while PQ.with(|s| {
        let st = s.borrow();
        st.recv_pointer >= st.recv_length
    }) {
        if pq_recvbuf()? != 0 {
            return Ok(EOF);
        }
    }
    Ok(PQ.with(|s| {
        let st = s.borrow();
        st.recv_buffer[st.recv_pointer as usize] as i32
    }))
}

/// `pq_getbyte_if_available` — get a single byte if available without
/// blocking. Stores the byte in `*c`; `Ok(1)` if read, `Ok(0)` if no data
/// available, `Ok(EOF)` if trouble.
pub fn pq_getbyte_if_available(c: &mut u8) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    let buffered = PQ.with(|s| {
        let mut st = s.borrow_mut();
        if st.recv_pointer < st.recv_length {
            let b = st.recv_buffer[st.recv_pointer as usize];
            st.recv_pointer += 1;
            Some(b)
        } else {
            None
        }
    });
    if let Some(b) = buffered {
        *c = b;
        return Ok(1);
    }

    // Put the socket into non-blocking mode
    socket_set_nonblocking(true)?;

    set_errno(0);
    let mut buf = [0u8; 1];
    let mut r = secure_read_my_port(&mut buf)? as i32;
    if r < 0 {
        // Ok if no data available without blocking or interrupted (though
        // EINTR really shouldn't happen with a non-blocking socket). Report
        // other errors.
        let e = errno();
        if e == libc::EAGAIN || e == libc::EWOULDBLOCK || e == libc::EINTR {
            r = 0;
        } else {
            // Careful: server-log-only message (recursion hazard); errno 0 is
            // treated as EOF, caller complains.
            if e != 0 {
                let _ = ereport(COMMERROR)
                    .with_saved_errno(e)
                    .errcode_for_socket_access()
                    .errmsg("could not receive data from client: %m")
                    .finish(loc("pq_getbyte_if_available"));
            }
            r = EOF;
        }
    } else if r == 0 {
        // EOF detected
        r = EOF;
    } else {
        *c = buf[0];
    }

    Ok(r)
}

/// `pq_getbytes` — get a known number of bytes from connection into `b`.
/// `Ok(0)` if OK, `Ok(EOF)` if trouble.
pub fn pq_getbytes(b: &mut [u8]) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    let mut off = 0usize;
    let mut len = b.len();
    while len > 0 {
        while PQ.with(|s| {
            let st = s.borrow();
            st.recv_pointer >= st.recv_length
        }) {
            if pq_recvbuf()? != 0 {
                return Ok(EOF);
            }
        }
        PQ.with(|s| {
            let mut st = s.borrow_mut();
            let mut amount = (st.recv_length - st.recv_pointer) as usize;
            if amount > len {
                amount = len;
            }
            let p = st.recv_pointer as usize;
            b[off..off + amount].copy_from_slice(&st.recv_buffer[p..p + amount]);
            st.recv_pointer += amount as i32;
            off += amount;
            len -= amount;
        });
    }
    Ok(0)
}

/// `pq_discardbytes` — throw away a known number of bytes (resynchronize
/// after read errors). `Ok(0)` if OK, `Ok(EOF)` if trouble.
fn pq_discardbytes(mut len: usize) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    while len > 0 {
        while PQ.with(|s| {
            let st = s.borrow();
            st.recv_pointer >= st.recv_length
        }) {
            if pq_recvbuf()? != 0 {
                return Ok(EOF);
            }
        }
        PQ.with(|s| {
            let mut st = s.borrow_mut();
            let mut amount = (st.recv_length - st.recv_pointer) as usize;
            if amount > len {
                amount = len;
            }
            st.recv_pointer += amount as i32;
            len -= amount;
        });
    }
    Ok(0)
}

/// `pq_buffer_remaining_data` — number of bytes already in the receive
/// buffer; does *not* read more data.
pub fn pq_buffer_remaining_data() -> isize {
    PQ.with(|s| {
        let st = s.borrow();
        debug_assert!(st.recv_length >= st.recv_pointer);
        (st.recv_length - st.recv_pointer) as isize
    })
}

/// `pq_startmsgread` — begin reading a message from the client. Must be
/// called before any of the `pq_get*` functions. `ereport(FATAL)` (the
/// non-returning `Err`/proc_exit path) on lost protocol sync.
pub fn pq_startmsgread() -> PgResult<()> {
    // There shouldn't be a read active already, but check just to be sure.
    if pq_is_reading_msg() {
        ereport(FATAL)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("terminating connection because protocol synchronization was lost")
            .finish(loc("pq_startmsgread"))?;
    }
    PQ.with(|s| s.borrow_mut().comm_reading_msg = true);
    Ok(())
}

/// `pq_endmsgread` — finish reading message.
pub fn pq_endmsgread() {
    debug_assert!(pq_is_reading_msg());
    PQ.with(|s| s.borrow_mut().comm_reading_msg = false);
}

/// `pq_is_reading_msg` — are we currently reading a message?
pub fn pq_is_reading_msg() -> bool {
    PQ.with(|s| s.borrow().comm_reading_msg)
}

/// `enlargeStringInfo` over the owned message buffer: reserve room for
/// `needed` more bytes, with stringinfo's limit check and the OOM ERROR.
fn enlarge_message_buffer(s: &mut Vec<u8>, needed: usize, funcname: &str) -> PgResult<()> {
    /// `MaxAllocSize` (`memutils.h`) — StringInfo's growth ceiling.
    const MAX_ALLOC_SIZE: usize = 0x3fffffff;
    if needed >= MAX_ALLOC_SIZE - s.len() {
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("out of memory")
            .errdetail(format!(
                "Cannot enlarge string buffer containing {} bytes by {} more bytes.",
                s.len(),
                needed
            ))
            .finish(loc(funcname))?;
    }
    if s.try_reserve(needed).is_err() {
        return Err(out_of_memory_error(needed, funcname));
    }
    Ok(())
}

/// `pq_getmessage` — get a message with length word from connection. Only the
/// message body is placed in `s` (an expansible buffer the caller owns); the
/// length word is removed. `maxlen` is the upper limit on the length we are
/// willing to accept; the connection is aborted (`Ok(EOF)`) past it. `Ok(0)`
/// if OK.
///
/// The C `PG_TRY`/`PG_CATCH` around `enlargeStringInfo` (discard the body to
/// stay in sync, clear the reading flag, re-throw) is the `Err` path here.
pub fn pq_getmessage(s: &mut Vec<u8>, maxlen: i32) -> PgResult<i32> {
    debug_assert!(pq_is_reading_msg());

    s.clear();

    // Read message length word
    let mut lenbuf = [0u8; 4];
    if pq_getbytes(&mut lenbuf)? == EOF {
        let _ = ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("unexpected EOF within message length word")
            .finish(loc("pq_getmessage"));
        return Ok(EOF);
    }

    let mut len = i32::from_be_bytes(lenbuf);

    if len < 4 || len > maxlen {
        let _ = ereport(COMMERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid message length")
            .finish(loc("pq_getmessage"));
        return Ok(EOF);
    }

    len -= 4; // discount length itself

    if len > 0 {
        let len = len as usize;

        // Allocate space for message. If we run out of room (ridiculously
        // large message), we will ERROR, but we want to discard the message
        // body first so as not to lose communication sync.
        if let Err(oom) = enlarge_message_buffer(s, len, "pq_getmessage") {
            // An error raised inside the catch block (here: from
            // pq_discardbytes) propagates immediately in C, skipping the rest
            // of the block — hence the `?`.
            if pq_discardbytes(len)? == EOF {
                let _ = ereport(COMMERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("incomplete message from client")
                    .finish(loc("pq_getmessage"));
            }
            // we discarded the rest of the message so we're back in sync.
            PQ.with(|st| st.borrow_mut().comm_reading_msg = false);
            return Err(oom); // PG_RE_THROW
        }

        // And grab the message
        s.resize(len, 0); // capacity reserved above; cannot fail
        if pq_getbytes(&mut s[..])? == EOF {
            s.clear(); // C leaves s->len == 0 on this path
            let _ = ereport(COMMERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("incomplete message from client")
                .finish(loc("pq_getmessage"));
            return Ok(EOF);
        }
        // (The C trailing NUL is StringInfo convention; Vec is
        // length-delimited.)
    }

    // finished reading the message.
    PQ.with(|st| st.borrow_mut().comm_reading_msg = false);

    Ok(0)
}

// ---------------------------------------------------------------------------
// Send side.
// ---------------------------------------------------------------------------

/// `internal_putbytes` — buffer (or directly send) outgoing bytes. `Ok(0)` if
/// OK, `Ok(EOF)` if trouble.
fn internal_putbytes(b: &[u8]) -> PgResult<i32> {
    let mut off = 0usize;
    let mut len = b.len();

    while len > 0 {
        // If buffer is full, then flush it out
        let (pointer, size) = PQ.with(|s| {
            let st = s.borrow();
            (st.send_pointer, st.send_buffer_size)
        });
        if pointer >= size {
            socket_set_nonblocking(false)?;
            if internal_flush()? != 0 {
                return Ok(EOF);
            }
        }

        // If the buffer is empty and data length is larger than the buffer
        // size, send it without buffering. Otherwise, copy as much data as
        // possible into the buffer.
        let (pointer, size, start) = PQ.with(|s| {
            let st = s.borrow();
            (st.send_pointer, st.send_buffer_size, st.send_start)
        });
        if len >= size && start == pointer {
            let mut fstart = 0usize;
            let mut fend = len;
            socket_set_nonblocking(false)?;
            if internal_flush_buffer(&b[off..off + len], &mut fstart, &mut fend)? != 0 {
                return Ok(EOF);
            }
            // C passes `&len` as the end cursor: on full success it is reset
            // to 0 and the loop exits; a would-block partial send leaves it
            // unchanged (unreachable in blocking mode).
            len = fend;
        } else {
            PQ.with(|s| {
                let mut st = s.borrow_mut();
                let mut amount = st.send_buffer_size - st.send_pointer;
                if amount > len {
                    amount = len;
                }
                let p = st.send_pointer;
                st.send_buffer[p..p + amount].copy_from_slice(&b[off..off + amount]);
                st.send_pointer += amount;
                off += amount;
                len -= amount;
            });
        }
    }

    Ok(0)
}

/// `socket_flush` — flush pending output. `Ok(0)` if OK, `Ok(EOF)` if
/// trouble. No-op on a reentrant call.
fn socket_flush() -> PgResult<i32> {
    // No-op if reentrant call
    if comm_busy() {
        return Ok(0);
    }
    set_comm_busy(true);
    let res = (|| {
        socket_set_nonblocking(false)?;
        internal_flush()
    })();
    // On Err (the C longjmp) PqCommBusy stays true until pq_comm_reset, as in
    // C.
    if res.is_ok() {
        set_comm_busy(false);
    }
    res
}

/// `internal_flush` — flush the send buffer.
fn internal_flush() -> PgResult<i32> {
    // Move the buffer out for the duration of the (possibly reentrant-ish)
    // transport call; cursors are written back afterwards, also on Err.
    let (buf, mut start, mut end) = PQ.with(|s| {
        let mut st = s.borrow_mut();
        (
            std::mem::take(&mut st.send_buffer),
            st.send_start,
            st.send_pointer,
        )
    });
    let res = internal_flush_buffer(&buf, &mut start, &mut end);
    PQ.with(|s| {
        let mut st = s.borrow_mut();
        st.send_buffer = buf;
        st.send_start = start;
        st.send_pointer = end;
    });
    res
}

/// `internal_flush_buffer` — flush the given buffer content between `*start`
/// and `*end`. `Ok(0)` if OK (everything sent, or would-block in non-blocking
/// mode), `Ok(EOF)` if trouble.
fn internal_flush_buffer(buf: &[u8], start: &mut usize, end: &mut usize) -> PgResult<i32> {
    while *start < *end {
        let r = secure_write_my_port(&buf[*start..*end])?;

        if r <= 0 {
            let e = errno();
            if e == libc::EINTR {
                continue; // Ok if we were interrupted
            }

            // Ok if no data writable without blocking, and the socket is in
            // non-blocking mode.
            if e == libc::EAGAIN || e == libc::EWOULDBLOCK {
                return Ok(0);
            }

            // Careful: server-log-only message (a client write would recurse
            // here). If a client disconnects mid-output we might come through
            // here many times before a safe abort point, so suppress
            // duplicate log messages.
            if e != LAST_REPORTED_SEND_ERRNO.with(Cell::get) {
                LAST_REPORTED_SEND_ERRNO.with(|c| c.set(e));
                let _ = ereport(COMMERROR)
                    .with_saved_errno(e)
                    .errcode_for_socket_access()
                    .errmsg("could not send data to client: %m")
                    .finish(loc("internal_flush_buffer"));
            }

            // Drop the buffered data anyway so that processing can continue,
            // and flag the next CHECK_FOR_INTERRUPTS to terminate the
            // connection.
            *start = 0;
            *end = 0;
            backend_utils_init_small_seams::set_client_connection_lost::call(true);
            backend_utils_init_small_seams::set_interrupt_pending::call(true);
            return Ok(EOF);
        }

        LAST_REPORTED_SEND_ERRNO.with(|c| c.set(0)); // reset after any successful send
        *start += r as usize;
    }

    *start = 0;
    *end = 0;
    Ok(0)
}

/// `socket_flush_if_writable` — flush pending output if writable without
/// blocking. `Ok(0)` if OK, `Ok(EOF)` if trouble.
fn socket_flush_if_writable() -> PgResult<i32> {
    // Quick exit if nothing to do
    let (pointer, start) = PQ.with(|s| {
        let st = s.borrow();
        (st.send_pointer, st.send_start)
    });
    if pointer == start {
        return Ok(0);
    }

    // No-op if reentrant call
    if comm_busy() {
        return Ok(0);
    }

    // Temporarily put the socket into non-blocking mode
    socket_set_nonblocking(true)?;

    set_comm_busy(true);
    let res = internal_flush();
    if res.is_ok() {
        set_comm_busy(false);
    }
    res
}

/// `socket_is_send_pending` — is there any pending data in the output buffer?
fn socket_is_send_pending() -> bool {
    PQ.with(|s| {
        let st = s.borrow();
        st.send_start < st.send_pointer
    })
}

// ---------------------------------------------------------------------------
// Message-level I/O.
// ---------------------------------------------------------------------------

/// `socket_putmessage` — send a normal message (suppressed while busy, e.g.
/// in COPY OUT mode / quickdie). A length word equal to `len + 4` is inserted
/// after the type byte. `Ok(0)` if OK, `Ok(EOF)` if trouble.
fn socket_putmessage(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    debug_assert!(msgtype != 0);

    if comm_busy() {
        return Ok(0);
    }
    set_comm_busy(true);
    let res = (|| -> PgResult<i32> {
        if internal_putbytes(&[msgtype])? != 0 {
            return Ok(EOF);
        }
        let n32 = ((s.len() + 4) as u32).to_be_bytes();
        if internal_putbytes(&n32)? != 0 {
            return Ok(EOF);
        }
        if internal_putbytes(s)? != 0 {
            return Ok(EOF);
        }
        Ok(0)
    })();
    // Both the success and the `goto fail` paths clear the busy flag; only
    // the C longjmp (Err) leaves it set.
    if res.is_ok() {
        set_comm_busy(false);
    }
    res
}

/// `socket_putmessage_noblock` — like `pq_putmessage`, but never blocks: the
/// output buffer is enlarged (`repalloc`) if the message doesn't fit.
fn socket_putmessage_noblock(msgtype: u8, s: &[u8]) -> PgResult<()> {
    // Ensure we have enough space in the output buffer for the message header
    // as well as the message itself.
    let required = PQ.with(|st| st.borrow().send_pointer) + 1 + 4 + s.len();
    if required > PQ.with(|st| st.borrow().send_buffer_size) {
        PQ.with(|st| -> PgResult<()> {
            let mut st = st.borrow_mut();
            let grow = required - st.send_buffer.len();
            if st.send_buffer.try_reserve_exact(grow).is_err() {
                return Err(out_of_memory_error(required, "socket_putmessage_noblock"));
            }
            st.send_buffer.resize(required, 0);
            st.send_buffer_size = required;
            Ok(())
        })?;
    }
    let res = pq_putmessage(msgtype, s)?;
    debug_assert_eq!(res, 0, "should not fail when the message fits in buffer");
    Ok(())
}

/// `pq_putmessage_v2` — send a message in (no-longer-supported) protocol
/// version 2 framing: type byte then raw body, no length word. Kept only so
/// the "unsupported protocol version" courtesy error can reach a v2 client.
/// Suppressed while busy. `Ok(0)` if OK, `Ok(EOF)` if trouble.
pub fn pq_putmessage_v2(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    debug_assert!(msgtype != 0);

    if comm_busy() {
        return Ok(0);
    }
    set_comm_busy(true);
    let res = (|| -> PgResult<i32> {
        if internal_putbytes(&[msgtype])? != 0 {
            return Ok(EOF);
        }
        if internal_putbytes(s)? != 0 {
            return Ok(EOF);
        }
        Ok(0)
    })();
    if res.is_ok() {
        set_comm_busy(false);
    }
    res
}

// ---------------------------------------------------------------------------
// The PQcommMethods dispatch table (libpq/libpq.h). pqmq.c swaps in its
// shm_mq-backed methods for background workers.
// ---------------------------------------------------------------------------

/// `PQcommMethods` (libpq/libpq.h) — the pluggable comm-method table.
pub struct PQcommMethods {
    pub comm_reset: fn(),
    pub flush: fn() -> PgResult<i32>,
    pub flush_if_writable: fn() -> PgResult<i32>,
    pub is_send_pending: fn() -> bool,
    pub putmessage: fn(u8, &[u8]) -> PgResult<i32>,
    pub putmessage_noblock: fn(u8, &[u8]) -> PgResult<()>,
}

/// `PqCommSocketMethods` — the regular socket-backed methods.
pub static PQ_COMM_SOCKET_METHODS: PQcommMethods = PQcommMethods {
    comm_reset: socket_comm_reset,
    flush: socket_flush,
    flush_if_writable: socket_flush_if_writable,
    is_send_pending: socket_is_send_pending,
    putmessage: socket_putmessage,
    putmessage_noblock: socket_putmessage_noblock,
};

thread_local! {
    /// `const PQcommMethods *PqCommMethods = &PqCommSocketMethods`.
    static PQ_COMM_METHODS: Cell<&'static PQcommMethods> =
        const { Cell::new(&PQ_COMM_SOCKET_METHODS) };
}

/// Redirect the comm methods (pqmq.c's `pq_redirect_to_shm_mq` /
/// `pq_set_parallel_leader` machinery installs its own table).
pub fn set_pq_comm_methods(methods: &'static PQcommMethods) {
    PQ_COMM_METHODS.with(|c| c.set(methods));
}

/// `pq_comm_reset()` (`PqCommMethods->comm_reset`).
pub fn pq_comm_reset() {
    (PQ_COMM_METHODS.with(Cell::get).comm_reset)()
}

/// `pq_flush()` (`PqCommMethods->flush`).
pub fn pq_flush() -> PgResult<i32> {
    (PQ_COMM_METHODS.with(Cell::get).flush)()
}

/// `pq_flush_if_writable()` (`PqCommMethods->flush_if_writable`).
pub fn pq_flush_if_writable() -> PgResult<i32> {
    (PQ_COMM_METHODS.with(Cell::get).flush_if_writable)()
}

/// `pq_is_send_pending()` (`PqCommMethods->is_send_pending`).
pub fn pq_is_send_pending() -> bool {
    (PQ_COMM_METHODS.with(Cell::get).is_send_pending)()
}

/// `pq_putmessage(msgtype, s, len)` (`PqCommMethods->putmessage`).
pub fn pq_putmessage(msgtype: u8, s: &[u8]) -> PgResult<i32> {
    (PQ_COMM_METHODS.with(Cell::get).putmessage)(msgtype, s)
}

/// `pq_putmessage_noblock(msgtype, s, len)`
/// (`PqCommMethods->putmessage_noblock`).
pub fn pq_putmessage_noblock(msgtype: u8, s: &[u8]) -> PgResult<()> {
    (PQ_COMM_METHODS.with(Cell::get).putmessage_noblock)(msgtype, s)
}

// ---------------------------------------------------------------------------
// TCP keepalive support.
// ---------------------------------------------------------------------------

#[cfg(any(target_os = "macos", target_os = "ios"))]
const PG_TCP_KEEPALIVE_IDLE: libc::c_int = libc::TCP_KEEPALIVE;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const PG_TCP_KEEPALIVE_IDLE_STR: &str = "TCP_KEEPALIVE";
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const PG_TCP_KEEPALIVE_IDLE: libc::c_int = libc::TCP_KEEPIDLE;
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const PG_TCP_KEEPALIVE_IDLE_STR: &str = "TCP_KEEPIDLE";

fn getsockopt_int(sock: pgsocket, level: libc::c_int, optname: libc::c_int) -> Result<i32, ()> {
    let mut val: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            sock,
            level,
            optname,
            (&mut val as *mut libc::c_int).cast(),
            &mut size,
        )
    } < 0
    {
        Err(())
    } else {
        Ok(val)
    }
}

fn setsockopt_int(
    sock: pgsocket,
    level: libc::c_int,
    optname: libc::c_int,
    val: i32,
) -> Result<(), ()> {
    let val: libc::c_int = val;
    if unsafe {
        libc::setsockopt(
            sock,
            level,
            optname,
            (&val as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    } < 0
    {
        Err(())
    } else {
        Ok(())
    }
}

fn log_sockopt_failure(call: &str, optname: &str, funcname: &str) {
    let e = errno();
    let _ = ereport(LOG)
        .with_saved_errno(e)
        .errmsg(format!("{}({}) failed: %m", call, optname))
        .finish(loc(funcname));
}

/// `pq_getkeepalivesidle`.
pub fn pq_getkeepalivesidle(port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return 0 };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return 0;
    }

    if port.keepalives_idle != 0 {
        return port.keepalives_idle;
    }

    if port.default_keepalives_idle == 0 {
        match getsockopt_int(port.sock, libc::IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE) {
            Ok(v) => port.default_keepalives_idle = v,
            Err(()) => {
                log_sockopt_failure("getsockopt", PG_TCP_KEEPALIVE_IDLE_STR, "pq_getkeepalivesidle");
                port.default_keepalives_idle = -1; // don't know
            }
        }
    }

    port.default_keepalives_idle
}

/// `pq_setkeepalivesidle`.
pub fn pq_setkeepalivesidle(idle: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if idle == port.keepalives_idle {
        return STATUS_OK;
    }

    if port.default_keepalives_idle <= 0 {
        if pq_getkeepalivesidle(Some(port)) < 0 {
            if idle == 0 {
                return STATUS_OK; // default is set but unknown
            }
            return STATUS_ERROR;
        }
    }

    let mut idle = idle;
    if idle == 0 {
        idle = port.default_keepalives_idle;
    }

    if setsockopt_int(port.sock, libc::IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE, idle).is_err() {
        log_sockopt_failure("setsockopt", PG_TCP_KEEPALIVE_IDLE_STR, "pq_setkeepalivesidle");
        return STATUS_ERROR;
    }

    port.keepalives_idle = idle;
    STATUS_OK
}

/// `pq_getkeepalivesinterval`.
pub fn pq_getkeepalivesinterval(port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return 0 };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return 0;
    }

    if port.keepalives_interval != 0 {
        return port.keepalives_interval;
    }

    if port.default_keepalives_interval == 0 {
        match getsockopt_int(port.sock, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL) {
            Ok(v) => port.default_keepalives_interval = v,
            Err(()) => {
                log_sockopt_failure("getsockopt", "TCP_KEEPINTVL", "pq_getkeepalivesinterval");
                port.default_keepalives_interval = -1;
            }
        }
    }

    port.default_keepalives_interval
}

/// `pq_setkeepalivesinterval`.
pub fn pq_setkeepalivesinterval(interval: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if interval == port.keepalives_interval {
        return STATUS_OK;
    }

    if port.default_keepalives_interval <= 0 {
        if pq_getkeepalivesinterval(Some(port)) < 0 {
            if interval == 0 {
                return STATUS_OK;
            }
            return STATUS_ERROR;
        }
    }

    let mut interval = interval;
    if interval == 0 {
        interval = port.default_keepalives_interval;
    }

    if setsockopt_int(port.sock, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, interval).is_err() {
        log_sockopt_failure("setsockopt", "TCP_KEEPINTVL", "pq_setkeepalivesinterval");
        return STATUS_ERROR;
    }

    port.keepalives_interval = interval;
    STATUS_OK
}

/// `pq_getkeepalivescount`.
pub fn pq_getkeepalivescount(port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return 0 };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return 0;
    }

    if port.keepalives_count != 0 {
        return port.keepalives_count;
    }

    if port.default_keepalives_count == 0 {
        match getsockopt_int(port.sock, libc::IPPROTO_TCP, libc::TCP_KEEPCNT) {
            Ok(v) => port.default_keepalives_count = v,
            Err(()) => {
                log_sockopt_failure("getsockopt", "TCP_KEEPCNT", "pq_getkeepalivescount");
                port.default_keepalives_count = -1;
            }
        }
    }

    port.default_keepalives_count
}

/// `pq_setkeepalivescount`.
pub fn pq_setkeepalivescount(count: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if count == port.keepalives_count {
        return STATUS_OK;
    }

    if port.default_keepalives_count <= 0 {
        if pq_getkeepalivescount(Some(port)) < 0 {
            if count == 0 {
                return STATUS_OK;
            }
            return STATUS_ERROR;
        }
    }

    let mut count = count;
    if count == 0 {
        count = port.default_keepalives_count;
    }

    if setsockopt_int(port.sock, libc::IPPROTO_TCP, libc::TCP_KEEPCNT, count).is_err() {
        log_sockopt_failure("setsockopt", "TCP_KEEPCNT", "pq_setkeepalivescount");
        return STATUS_ERROR;
    }

    port.keepalives_count = count;
    STATUS_OK
}

/// `pq_gettcpusertimeout` (`TCP_USER_TIMEOUT` build, i.e. Linux).
#[cfg(target_os = "linux")]
pub fn pq_gettcpusertimeout(port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return 0 };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return 0;
    }

    if port.tcp_user_timeout != 0 {
        return port.tcp_user_timeout;
    }

    if port.default_tcp_user_timeout == 0 {
        match getsockopt_int(port.sock, libc::IPPROTO_TCP, libc::TCP_USER_TIMEOUT) {
            Ok(v) => port.default_tcp_user_timeout = v,
            Err(()) => {
                log_sockopt_failure("getsockopt", "TCP_USER_TIMEOUT", "pq_gettcpusertimeout");
                port.default_tcp_user_timeout = -1;
            }
        }
    }

    port.default_tcp_user_timeout
}

/// `pq_gettcpusertimeout` (no `TCP_USER_TIMEOUT` on this platform).
#[cfg(not(target_os = "linux"))]
pub fn pq_gettcpusertimeout(_port: Option<&mut Port>) -> i32 {
    0
}

/// `pq_settcpusertimeout` (`TCP_USER_TIMEOUT` build, i.e. Linux).
#[cfg(target_os = "linux")]
pub fn pq_settcpusertimeout(timeout: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if timeout == port.tcp_user_timeout {
        return STATUS_OK;
    }

    if port.default_tcp_user_timeout <= 0 {
        if pq_gettcpusertimeout(Some(port)) < 0 {
            if timeout == 0 {
                return STATUS_OK;
            }
            return STATUS_ERROR;
        }
    }

    let mut timeout = timeout;
    if timeout == 0 {
        timeout = port.default_tcp_user_timeout;
    }

    if setsockopt_int(port.sock, libc::IPPROTO_TCP, libc::TCP_USER_TIMEOUT, timeout).is_err() {
        log_sockopt_failure("setsockopt", "TCP_USER_TIMEOUT", "pq_settcpusertimeout");
        return STATUS_ERROR;
    }

    port.tcp_user_timeout = timeout;
    STATUS_OK
}

/// `pq_settcpusertimeout` (no `TCP_USER_TIMEOUT` on this platform).
#[cfg(not(target_os = "linux"))]
pub fn pq_settcpusertimeout(timeout: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }
    if timeout != 0 {
        let _ = ereport(LOG)
            .errmsg("setsockopt(TCP_USER_TIMEOUT) not supported")
            .finish(loc("pq_settcpusertimeout"));
        return STATUS_ERROR;
    }
    STATUS_OK
}

// ---------------------------------------------------------------------------
// GUC assign/show hooks for the keepalive parameters. The kernel API provides
// no way to test a value without setting it, so the assignment happens on
// demand and the show hooks retrieve the kernel value rather than trusting
// GUC's copy.
// ---------------------------------------------------------------------------

/// GUC assign_hook for `tcp_keepalives_idle`.
pub fn assign_tcp_keepalives_idle(newval: i32) {
    with_my_proc_port(&mut |port| {
        let _ = pq_setkeepalivesidle(newval, port);
    });
}

/// GUC show_hook for `tcp_keepalives_idle`.
pub fn show_tcp_keepalives_idle() -> String {
    let mut v = 0;
    with_my_proc_port(&mut |port| {
        v = pq_getkeepalivesidle(port);
    });
    v.to_string()
}

/// GUC assign_hook for `tcp_keepalives_interval`.
pub fn assign_tcp_keepalives_interval(newval: i32) {
    with_my_proc_port(&mut |port| {
        let _ = pq_setkeepalivesinterval(newval, port);
    });
}

/// GUC show_hook for `tcp_keepalives_interval`.
pub fn show_tcp_keepalives_interval() -> String {
    let mut v = 0;
    with_my_proc_port(&mut |port| {
        v = pq_getkeepalivesinterval(port);
    });
    v.to_string()
}

/// GUC assign_hook for `tcp_keepalives_count`.
pub fn assign_tcp_keepalives_count(newval: i32) {
    with_my_proc_port(&mut |port| {
        let _ = pq_setkeepalivescount(newval, port);
    });
}

/// GUC show_hook for `tcp_keepalives_count`.
pub fn show_tcp_keepalives_count() -> String {
    let mut v = 0;
    with_my_proc_port(&mut |port| {
        v = pq_getkeepalivescount(port);
    });
    v.to_string()
}

/// GUC assign_hook for `tcp_user_timeout`.
pub fn assign_tcp_user_timeout(newval: i32) {
    with_my_proc_port(&mut |port| {
        let _ = pq_settcpusertimeout(newval, port);
    });
}

/// GUC show_hook for `tcp_user_timeout`.
pub fn show_tcp_user_timeout() -> String {
    let mut v = 0;
    with_my_proc_port(&mut |port| {
        v = pq_gettcpusertimeout(port);
    });
    v.to_string()
}

// ---------------------------------------------------------------------------
// pq_check_connection
// ---------------------------------------------------------------------------

/// `pq_check_connection` — is the client still connected? (Polls
/// `FeBeWaitSet` for `WL_SOCKET_CLOSED`.)
pub fn pq_check_connection() -> PgResult<bool> {
    let set = fe_be_wait_set().expect("pq_check_connection: FeBeWaitSet not created");

    // It's OK to modify the socket event filter without restoring, because
    // all FeBeWaitSet socket wait sites do the same.
    backend_storage_ipc_waiteventset_seams::modify_wait_event::call(
        set,
        FeBeWaitSetSocketPos,
        WL_SOCKET_CLOSED,
    )?;

    'retry: loop {
        let mut events = [WaitEvent::default(); FeBeWaitSetNEvents];
        let rc = backend_storage_ipc_waiteventset_seams::wait_event_set_wait::call(
            set,
            0,
            &mut events,
            0,
        )?;
        for event in events.iter().take(rc.max(0) as usize) {
            if event.events & WL_SOCKET_CLOSED != 0 {
                return Ok(false);
            }
            if event.events & WL_LATCH_SET != 0 {
                // A latch event might be preventing other events from being
                // reported. Reset it and poll again. (No code expects latches
                // to survive across CHECK_FOR_INTERRUPTS().)
                backend_storage_ipc_latch_seams::reset_latch_my_latch::call();
                continue 'retry;
            }
        }
        return Ok(true);
    }
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this crate's seams (declared in `backend-libpq-pqcomm-seams`).
pub fn init_seams() {
    backend_libpq_pqcomm_seams::pq_putmessage::set(pq_putmessage);
    backend_libpq_pqcomm_seams::pq_putmessage_v2::set(pq_putmessage_v2);
    backend_libpq_pqcomm_seams::pq_flush::set(pq_flush);
}
