// pqcomm.c socket half: pq_init wiring, socket_close, listen/accept,
// TCP keepalive knobs.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::ffi::CString;

use elog::ereport;
use ip::{sockaddr_family, AddrInfoHint, PgAddrInfo};
use types_core::{pgsocket, PGINVALID_SOCKET, STATUS_ERROR, STATUS_OK};
use types_error::{ErrorLocation, PgResult, FATAL, LOG};
use types_startup::{ClientSocket, Port};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WaitEventSetHandle, WL_LATCH_SET, WL_SOCKET_WRITEABLE};

use init_small::globals as g;

pub const FeBeWaitSetSocketPos: i32 = 0;
pub const FeBeWaitSetLatchPos: i32 = 1;
pub const FeBeWaitSetNEvents: i32 = 3;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("pqcomm.c", 0, funcname)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn errno() -> i32 {
    unsafe { *libc::__error() }
}

#[cfg(not(any(target_os = "macos", target_os = "ios")))]
fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

thread_local! {
    // WaitEventSet *FeBeWaitSet; never freed, as C (lives for the backend).
    static FE_BE_WAIT_SET: Cell<Option<WaitEventSetHandle>> = const { Cell::new(None) };
    // static List *sock_paths (postmaster-thread state).
    static SOCK_PATHS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

// GUC storage declared in pqcomm.c; boot values from guc_tables.c.
mod cfg {
    use std::cell::{Cell, RefCell};

    thread_local! {
        pub static TCP_KEEPALIVES_IDLE: Cell<i32> = const { Cell::new(0) };
        pub static TCP_KEEPALIVES_INTERVAL: Cell<i32> = const { Cell::new(0) };
        pub static TCP_KEEPALIVES_COUNT: Cell<i32> = const { Cell::new(0) };
        pub static TCP_USER_TIMEOUT: Cell<i32> = const { Cell::new(0) };
        pub static UNIX_SOCKET_PERMISSIONS: Cell<i32> = const { Cell::new(0o777) };
        pub static UNIX_SOCKET_GROUP: RefCell<String> = const { RefCell::new(String::new()) };
    }
}

pub fn unix_socket_group() -> String {
    cfg::UNIX_SOCKET_GROUP.with(|s| s.borrow().clone())
}

pub fn unix_socket_permissions() -> i32 {
    cfg::UNIX_SOCKET_PERMISSIONS.get()
}

fn setsockopt_int(sock: pgsocket, level: i32, optname: i32, val: i32) -> Result<(), ()> {
    let val: libc::c_int = val;
    // SAFETY: val outlives the call; optlen matches.
    let rc = unsafe {
        libc::setsockopt(
            sock,
            level,
            optname,
            std::ptr::from_ref(&val).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        Err(())
    } else {
        Ok(())
    }
}

fn getsockopt_int(sock: pgsocket, level: i32, optname: i32) -> Result<i32, ()> {
    let mut val: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: out-pointers sized by `size`.
    let rc = unsafe {
        libc::getsockopt(
            sock,
            level,
            optname,
            std::ptr::from_mut(&mut val).cast(),
            &mut size,
        )
    };
    if rc < 0 {
        Err(())
    } else {
        Ok(val)
    }
}

pub fn pq_init(client_sock: &ClientSocket) -> PgResult<Port> {
    let mut port = Port::new(client_sock);

    port.laddr.salen = port.laddr.addr.len() as u32;
    // SAFETY: laddr.addr is sockaddr_storage-sized; salen is in/out.
    if unsafe {
        libc::getsockname(
            port.sock,
            port.laddr.addr.as_mut_ptr().cast::<libc::sockaddr>(),
            &mut port.laddr.salen,
        )
    } < 0
    {
        ereport(FATAL)
            .with_saved_errno(errno())
            .errmsg("getsockname() failed: %m")
            .finish(loc("pq_init"))?;
    }

    if sockaddr_family(&port.laddr) != libc::AF_UNIX {
        if setsockopt_int(port.sock, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1).is_err() {
            ereport(FATAL)
                .with_saved_errno(errno())
                .errmsg("setsockopt(TCP_NODELAY) failed: %m")
                .finish(loc("pq_init"))?;
        }
        if setsockopt_int(port.sock, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1).is_err() {
            ereport(FATAL)
                .with_saved_errno(errno())
                .errmsg("setsockopt(SO_KEEPALIVE) failed: %m")
                .finish(loc("pq_init"))?;
        }

        // Keepalive GUC failures don't error out (not universally supported).
        let _ = pq_setkeepalivesidle(cfg::TCP_KEEPALIVES_IDLE.get(), Some(&mut port));
        let _ = pq_setkeepalivesinterval(cfg::TCP_KEEPALIVES_INTERVAL.get(), Some(&mut port));
        let _ = pq_setkeepalivescount(cfg::TCP_KEEPALIVES_COUNT.get(), Some(&mut port));
        let _ = pq_settcpusertimeout(cfg::TCP_USER_TIMEOUT.get(), Some(&mut port));
    }

    crate::pq_init_buffers()?;

    ipc_seams::on_proc_exit::call(socket_close, 0);

    // The socket runs in nonblocking mode from here on; latches provide the
    // blocking semantics (safely interruptible reads/writes). Inlined
    // pg_set_noblock (port/noblock.c): F_GETFL | O_NONBLOCK.
    let flags = unsafe { libc::fcntl(port.sock, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(port.sock, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        ereport(FATAL)
            .with_saved_errno(errno())
            .errmsg("could not set socket to nonblocking mode: %m")
            .finish(loc("pq_init"))?;
    }

    if unsafe { libc::fcntl(port.sock, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        ereport(FATAL)
            .with_saved_errno(errno())
            .errmsg_internal("fcntl(F_SETFD) failed on socket: %m")
            .finish(loc("pq_init"))?;
    }

    let set = waiteventset_seams::create_wait_event_set::call(FeBeWaitSetNEvents)?;
    let socket_pos = waiteventset_seams::add_wait_event_to_set::call(
        set,
        WL_SOCKET_WRITEABLE,
        port.sock,
        None,
        None,
    )?;
    let latch = g::MyLatch().expect("pq_init: MyLatch is not set");
    let latch_pos = waiteventset_seams::add_wait_event_to_set::call(
        set,
        WL_LATCH_SET,
        PGINVALID_SOCKET,
        Some(latch),
        None,
    )?;
    // C adds WL_POSTMASTER_DEATH third; the threaded waiteventset has none
    // (postmaster exit takes the whole process down).
    FE_BE_WAIT_SET.set(Some(set));

    debug_assert_eq!(socket_pos, FeBeWaitSetSocketPos);
    debug_assert_eq!(latch_pos, FeBeWaitSetLatchPos);

    Ok(port)
}

// on_proc_exit hook: stop I/O but leave the fd open until process death.
fn socket_close(_code: i32, _arg: usize) {
    if g::HaveMyProcPort() {
        g::WithMyProcPort(|port| {
            assert!(
                !port.ssl_in_use,
                "socket_close: secure_close TLS arm is unported"
            );
            port.sock = PGINVALID_SOCKET;
        });
    }
}

fn set_port_noblock(noblock: bool) -> bool {
    if !g::HaveMyProcPort() {
        return false;
    }
    g::WithMyProcPort(|port| port.noblock = noblock);
    true
}

pub fn pq_modify_fe_be_wait_set_latch(latch: LatchHandle) -> PgResult<()> {
    match FE_BE_WAIT_SET.get() {
        Some(set) => waiteventset_seams::modify_wait_event::call(
            set,
            FeBeWaitSetLatchPos,
            WL_LATCH_SET,
            Some(latch),
        ),
        None => Ok(()),
    }
}

pub fn pq_modify_fe_be_wait_set_socket(events: u32) -> PgResult<()> {
    let set = FE_BE_WAIT_SET.get().expect("FeBeWaitSet not created");
    waiteventset_seams::modify_wait_event::call(set, FeBeWaitSetSocketPos, events, None)
}

/// One-event `WaitEventSetWait(FeBeWaitSet, ...)`; returns the fired event's
/// wakeup bits (0 on timeout).
pub fn pq_wait_event_set_wait_fe_be(timeout: i64, wait_event_info: u32) -> PgResult<u32> {
    let set = FE_BE_WAIT_SET.get().expect("FeBeWaitSet not created");
    let event = waiteventset_seams::wait_event_set_wait_one::call(set, timeout, wait_event_info)?;
    Ok(event.map_or(0, |e| e.events))
}

fn sun_path_buflen() -> usize {
    // SAFETY: plain-old-data zero pattern.
    let su: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    su.sun_path.len()
}

fn gai_strerror_string(err: i32) -> String {
    // SAFETY: gai_strerror returns a static NUL-terminated message.
    unsafe { std::ffi::CStr::from_ptr(libc::gai_strerror(err)) }
        .to_string_lossy()
        .into_owned()
}

/// Open a listen socket; opened fds are appended to `listen_sockets`
/// (C `ListenSockets[]`/`*NumListenSockets`, `max_listen` = MaxListen).
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
        let dir = unix_socket_dir.expect("ListenServerPort: AF_UNIX requires unixSocketDir");
        debug_assert!(!dir.is_empty());
        unix_socket_path = format!("{}/.s.PGSQL.{}", dir, port_number);
        if unix_socket_path.len() >= sun_path_buflen() {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "Unix-domain socket path \"{}\" is too long (maximum {} bytes)",
                    unix_socket_path,
                    sun_path_buflen() - 1
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
    let ret = ip::pg_getaddrinfo_all(host_name, Some(&service), &hint, &mut addrs);
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
        // Unix sockets only when asked for (the service/port differs then).
        if family != libc::AF_UNIX && addr.family == libc::AF_UNIX {
            continue;
        }

        if listen_sockets.len() == max_listen {
            let _ = ereport(LOG)
                .errmsg(format!(
                    "could not bind to all requested addresses: MAXLISTEN ({}) exceeded",
                    max_listen
                ))
                .finish(loc("ListenServerPort"));
            break;
        }

        let family_desc: String = match addr.family {
            x if x == libc::AF_INET => "IPv4".to_owned(),
            x if x == libc::AF_INET6 => "IPv6".to_owned(),
            x if x == libc::AF_UNIX => "Unix".to_owned(),
            other => format!("unrecognized address family {}", other),
        };
        let addr_desc: String = if addr.family == libc::AF_UNIX {
            unix_socket_path.clone()
        } else {
            let mut node = String::new();
            ip::pg_getnameinfo_all(&addr.addr, Some(&mut node), None, libc::NI_NUMERICHOST);
            node
        };

        // SAFETY: plain socket(2).
        let fd = unsafe { libc::socket(addr.family, libc::SOCK_STREAM, 0) };
        if fd == PGINVALID_SOCKET {
            let _ = ereport(LOG)
                .with_saved_errno(errno())
                .errcode_for_socket_access()
                .errmsg(format!(
                    "could not create {} socket for address \"{}\": %m",
                    family_desc, addr_desc
                ))
                .finish(loc("ListenServerPort"));
            continue;
        }

        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            ereport(FATAL)
                .with_saved_errno(errno())
                .errmsg_internal("fcntl(F_SETFD) failed on socket: %m")
                .finish(loc("ListenServerPort"))?;
        }

        // Without SO_REUSEADDR a new postmaster can't start right away after
        // a stop or crash.
        if addr.family != libc::AF_UNIX
            && setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, 1).is_err()
        {
            let _ = ereport(LOG)
                .with_saved_errno(errno())
                .errcode_for_socket_access()
                .errmsg(format!(
                    "setsockopt(SO_REUSEADDR) failed for {} address \"{}\": %m",
                    family_desc, addr_desc
                ))
                .finish(loc("ListenServerPort"));
            unsafe { libc::close(fd) };
            continue;
        }

        if addr.family == libc::AF_INET6
            && setsockopt_int(fd, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY, 1).is_err()
        {
            let _ = ereport(LOG)
                .with_saved_errno(errno())
                .errcode_for_socket_access()
                .errmsg(format!(
                    "setsockopt(IPV6_V6ONLY) failed for {} address \"{}\": %m",
                    family_desc, addr_desc
                ))
                .finish(loc("ListenServerPort"));
            unsafe { libc::close(fd) };
            continue;
        }

        // SAFETY: addr.addr holds salen valid sockaddr bytes.
        let err = unsafe {
            libc::bind(
                fd,
                addr.addr.addr.as_ptr().cast::<libc::sockaddr>(),
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

        if addr.family == libc::AF_UNIX && Setup_AF_UNIX(&service)? != STATUS_OK {
            unsafe { libc::close(fd) };
            break;
        }

        // Accept-queue length: similar to the maximum number of children the
        // postmaster will permit.
        let maxconn = g::MaxConnections() * 2;

        if unsafe { libc::listen(fd, maxconn) } < 0 {
            let _ = ereport(LOG)
                .with_saved_errno(errno())
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

fn Lock_AF_UNIX(unix_socket_dir: &str, unix_socket_path: &str) -> PgResult<i32> {
    // No lock file for abstract sockets.
    if unix_socket_path.starts_with('@') {
        return Ok(STATUS_OK);
    }

    miscinit_seams::create_socket_lock_file::call(unix_socket_path, true, unix_socket_dir)?;

    // Interlock held: delete any pre-existing socket file before bind().
    let c = CString::new(unix_socket_path).expect("socket path contains NUL");
    unsafe { libc::unlink(c.as_ptr()) };

    SOCK_PATHS.with(|p| p.borrow_mut().push(unix_socket_path.to_owned()));

    Ok(STATUS_OK)
}

// C strtoul(s, &endptr, 10) with the `*endptr == '\0'` full-consumption test.
fn parse_strtoul_full(s: &str) -> Option<u64> {
    let bytes = s.as_bytes();
    let mut i = 0;
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
        // strtoul clamps to ULONG_MAX on overflow; the C caller ignores ERANGE.
        return Some(u64::MAX);
    }
    Some(if negative {
        value.wrapping_neg()
    } else {
        value
    })
}

// Fix socket ownership/permission before listen(), closing the window where
// unwanted connections could get accepted.
fn Setup_AF_UNIX(sock_path: &str) -> PgResult<i32> {
    // No file system permissions for abstract sockets.
    if sock_path.starts_with('@') {
        return Ok(STATUS_OK);
    }

    let path_c = CString::new(sock_path).expect("socket path contains NUL");

    let group = unix_socket_group();
    if !group.is_empty() {
        let gid: libc::gid_t = if let Some(val) = parse_strtoul_full(&group) {
            val as libc::gid_t
        } else {
            let group_c = CString::new(group.as_str()).expect("group name contains NUL");
            // SAFETY: NUL-terminated name; result checked for NULL before use.
            let gr = unsafe { libc::getgrnam(group_c.as_ptr()) };
            if gr.is_null() {
                let _ = ereport(LOG)
                    .errmsg(format!("group \"{}\" does not exist", group))
                    .finish(loc("Setup_AF_UNIX"));
                return Ok(STATUS_ERROR);
            }
            unsafe { (*gr).gr_gid }
        };
        // uid_t::MAX is C's (uid_t) -1 "don't change owner".
        if unsafe { libc::chown(path_c.as_ptr(), libc::uid_t::MAX, gid) } == -1 {
            let _ = ereport(LOG)
                .with_saved_errno(errno())
                .errcode_for_file_access()
                .errmsg(format!("could not set group of file \"{}\": %m", sock_path))
                .finish(loc("Setup_AF_UNIX"));
            return Ok(STATUS_ERROR);
        }
    }

    if unsafe { libc::chmod(path_c.as_ptr(), unix_socket_permissions() as libc::mode_t) } == -1 {
        let _ = ereport(LOG)
            .with_saved_errno(errno())
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

pub fn AcceptConnection(server_fd: pgsocket, client_sock: &mut ClientSocket) -> i32 {
    client_sock.raddr.salen = client_sock.raddr.addr.len() as u32;
    // SAFETY: raddr.addr is sockaddr_storage-sized; salen is in/out.
    let fd = unsafe {
        libc::accept(
            server_fd,
            client_sock.raddr.addr.as_mut_ptr().cast::<libc::sockaddr>(),
            &mut client_sock.raddr.salen,
        )
    };
    if fd == PGINVALID_SOCKET {
        client_sock.sock = PGINVALID_SOCKET;
        let _ = ereport(LOG)
            .with_saved_errno(errno())
            .errcode_for_socket_access()
            .errmsg("could not accept new connection: %m")
            .finish(loc("AcceptConnection"));

        // The postmaster retries immediately on read-ready; delay a bit.
        std::thread::sleep(std::time::Duration::from_micros(100000));
        return STATUS_ERROR;
    }
    client_sock.sock = fd;

    STATUS_OK
}

/// Mark socket files recently accessed, protecting them from /tmp cleaners.
pub fn TouchSocketFiles() {
    SOCK_PATHS.with(|p| {
        for sock_path in p.borrow().iter() {
            if let Ok(c) = CString::new(sock_path.as_str()) {
                // Errors ignored; NULL utimbuf sets times to now.
                unsafe { libc::utime(c.as_ptr(), std::ptr::null()) };
            }
        }
    });
}

pub fn RemoveSocketFiles() {
    SOCK_PATHS.with(|p| {
        let mut paths = p.borrow_mut();
        for sock_path in paths.iter() {
            if let Ok(c) = CString::new(sock_path.as_str()) {
                unsafe { libc::unlink(c.as_ptr()) };
            }
        }
        paths.clear();
    });
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
const PG_TCP_KEEPALIVE_IDLE: i32 = libc::TCP_KEEPALIVE;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const PG_TCP_KEEPALIVE_IDLE_STR: &str = "TCP_KEEPALIVE";
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const PG_TCP_KEEPALIVE_IDLE: i32 = libc::TCP_KEEPIDLE;
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const PG_TCP_KEEPALIVE_IDLE_STR: &str = "TCP_KEEPIDLE";

fn log_sockopt_failure(call: &str, optname: &str, funcname: &'static str) {
    let _ = ereport(LOG)
        .with_saved_errno(errno())
        .errmsg(format!("{}({}) failed: %m", call, optname))
        .finish(loc(funcname));
}

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
                log_sockopt_failure(
                    "getsockopt",
                    PG_TCP_KEEPALIVE_IDLE_STR,
                    "pq_getkeepalivesidle",
                );
                port.default_keepalives_idle = -1;
            }
        }
    }

    port.default_keepalives_idle
}

pub fn pq_setkeepalivesidle(idle: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if idle == port.keepalives_idle {
        return STATUS_OK;
    }

    if port.default_keepalives_idle <= 0 && pq_getkeepalivesidle(Some(port)) < 0 {
        if idle == 0 {
            return STATUS_OK; // default is set but unknown
        }
        return STATUS_ERROR;
    }

    let mut idle = idle;
    if idle == 0 {
        idle = port.default_keepalives_idle;
    }

    if setsockopt_int(port.sock, libc::IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE, idle).is_err() {
        log_sockopt_failure(
            "setsockopt",
            PG_TCP_KEEPALIVE_IDLE_STR,
            "pq_setkeepalivesidle",
        );
        return STATUS_ERROR;
    }

    port.keepalives_idle = idle;
    STATUS_OK
}

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

pub fn pq_setkeepalivesinterval(interval: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if interval == port.keepalives_interval {
        return STATUS_OK;
    }

    if port.default_keepalives_interval <= 0 && pq_getkeepalivesinterval(Some(port)) < 0 {
        if interval == 0 {
            return STATUS_OK;
        }
        return STATUS_ERROR;
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

pub fn pq_setkeepalivescount(count: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if count == port.keepalives_count {
        return STATUS_OK;
    }

    if port.default_keepalives_count <= 0 && pq_getkeepalivescount(Some(port)) < 0 {
        if count == 0 {
            return STATUS_OK;
        }
        return STATUS_ERROR;
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

// Non-Linux: no TCP_USER_TIMEOUT (the C #else arms).
#[cfg(not(target_os = "linux"))]
pub fn pq_gettcpusertimeout(_port: Option<&mut Port>) -> i32 {
    0
}

#[cfg(target_os = "linux")]
pub fn pq_settcpusertimeout(timeout: i32, port: Option<&mut Port>) -> i32 {
    let Some(port) = port else { return STATUS_OK };
    if sockaddr_family(&port.laddr) == libc::AF_UNIX {
        return STATUS_OK;
    }

    if timeout == port.tcp_user_timeout {
        return STATUS_OK;
    }

    if port.default_tcp_user_timeout <= 0 && pq_gettcpusertimeout(Some(port)) < 0 {
        if timeout == 0 {
            return STATUS_OK;
        }
        return STATUS_ERROR;
    }

    let mut timeout = timeout;
    if timeout == 0 {
        timeout = port.default_tcp_user_timeout;
    }

    if setsockopt_int(
        port.sock,
        libc::IPPROTO_TCP,
        libc::TCP_USER_TIMEOUT,
        timeout,
    )
    .is_err()
    {
        log_sockopt_failure("setsockopt", "TCP_USER_TIMEOUT", "pq_settcpusertimeout");
        return STATUS_ERROR;
    }

    port.tcp_user_timeout = timeout;
    STATUS_OK
}

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

fn with_my_proc_port_opt<R>(f: impl FnOnce(Option<&mut Port>) -> R) -> R {
    if g::HaveMyProcPort() {
        g::WithMyProcPort(|port| f(Some(port)))
    } else {
        f(None)
    }
}

// The kernel API can't test a keepalive value without setting it, so GUC
// assignment happens on demand and show reads back the kernel truth.
fn assign_tcp_keepalives_idle(newval: i32) {
    cfg::TCP_KEEPALIVES_IDLE.set(newval);
    with_my_proc_port_opt(|port| {
        let _ = pq_setkeepalivesidle(newval, port);
    });
}

fn show_tcp_keepalives_idle() -> String {
    with_my_proc_port_opt(pq_getkeepalivesidle).to_string()
}

fn assign_tcp_keepalives_interval(newval: i32) {
    cfg::TCP_KEEPALIVES_INTERVAL.set(newval);
    with_my_proc_port_opt(|port| {
        let _ = pq_setkeepalivesinterval(newval, port);
    });
}

fn show_tcp_keepalives_interval() -> String {
    with_my_proc_port_opt(pq_getkeepalivesinterval).to_string()
}

fn assign_tcp_keepalives_count(newval: i32) {
    cfg::TCP_KEEPALIVES_COUNT.set(newval);
    with_my_proc_port_opt(|port| {
        let _ = pq_setkeepalivescount(newval, port);
    });
}

fn show_tcp_keepalives_count() -> String {
    with_my_proc_port_opt(pq_getkeepalivescount).to_string()
}

fn assign_tcp_user_timeout(newval: i32) {
    cfg::TCP_USER_TIMEOUT.set(newval);
    with_my_proc_port_opt(|port| {
        let _ = pq_settcpusertimeout(newval, port);
    });
}

fn show_tcp_user_timeout() -> String {
    with_my_proc_port_opt(pq_gettcpusertimeout).to_string()
}

/// Install the socket-half seams and this file's GUC slots. Kept apart from
/// [`crate::init_seams`]: test binaries that stub the transport install that
/// one alone.
pub fn init_socket_seams() {
    use guc_tables::{hooks, vars, GucVarAccessors};

    pqcomm_seams::pq_init::set(pq_init);
    pqcomm_seams::modify_fe_be_wait_set_latch::set(pq_modify_fe_be_wait_set_latch);
    be_secure_seams::set_port_noblock::set(set_port_noblock);

    vars::tcp_keepalives_idle.install(GucVarAccessors {
        get: || cfg::TCP_KEEPALIVES_IDLE.get(),
        set: |v| cfg::TCP_KEEPALIVES_IDLE.set(v),
    });
    vars::tcp_keepalives_interval.install(GucVarAccessors {
        get: || cfg::TCP_KEEPALIVES_INTERVAL.get(),
        set: |v| cfg::TCP_KEEPALIVES_INTERVAL.set(v),
    });
    vars::tcp_keepalives_count.install(GucVarAccessors {
        get: || cfg::TCP_KEEPALIVES_COUNT.get(),
        set: |v| cfg::TCP_KEEPALIVES_COUNT.set(v),
    });
    vars::tcp_user_timeout.install(GucVarAccessors {
        get: || cfg::TCP_USER_TIMEOUT.get(),
        set: |v| cfg::TCP_USER_TIMEOUT.set(v),
    });
    vars::Unix_socket_permissions.install(GucVarAccessors {
        get: || cfg::UNIX_SOCKET_PERMISSIONS.get(),
        set: |v| cfg::UNIX_SOCKET_PERMISSIONS.set(v),
    });
    // Boots to "" and GUC string storage never goes back to NULL after.
    vars::Unix_socket_group.install(GucVarAccessors {
        get: || Some(unix_socket_group()),
        set: |v| cfg::UNIX_SOCKET_GROUP.with(|s| *s.borrow_mut() = v.unwrap_or_default()),
    });

    hooks::assign_tcp_keepalives_idle.install(|v, _extra| assign_tcp_keepalives_idle(v));
    hooks::assign_tcp_keepalives_interval.install(|v, _extra| assign_tcp_keepalives_interval(v));
    hooks::assign_tcp_keepalives_count.install(|v, _extra| assign_tcp_keepalives_count(v));
    hooks::assign_tcp_user_timeout.install(|v, _extra| assign_tcp_user_timeout(v));
    hooks::show_tcp_keepalives_idle.install(show_tcp_keepalives_idle);
    hooks::show_tcp_keepalives_interval.install(show_tcp_keepalives_interval);
    hooks::show_tcp_keepalives_count.install(show_tcp_keepalives_count);
    hooks::show_tcp_user_timeout.install(show_tcp_user_timeout);
}
