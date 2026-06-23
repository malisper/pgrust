//! `ident_inet` (`auth.c:1670`) + `interpret_ident_response` (`auth.c:1589`) —
//! Ident (RFC 1413) authentication.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use backend_utils_error::ereport;
use types_error::{PgResult, LOG};
use types_net::{AddrInfoHint, PgAddrInfo, Port, SockAddr};

use crate::seams;
use crate::{here, port_user_name, set_authn_id, STATUS_ERROR};

/// Max size of username ident server can return (per RFC 1413).
const IDENT_USERNAME_MAX: usize = 512;
/// Standard TCP port number for Ident service. Assigned by IANA.
const IDENT_PORT: u16 = 113;

/// `pg_isblank(c)` (hba.h): space, tab, or carriage return.
fn pg_isblank(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r'
}

/// `interpret_ident_response(ident_response, ident_user)` (`auth.c:1590`).
/// Parse the response from the ident server. Returns the username on success.
fn interpret_ident_response(ident_response: &[u8]) -> Option<String> {
    // Treat as a C string up to the first NUL (the C reads a NUL-terminated
    // buffer); the caller already NUL-terminated at the recv length.
    let resp_end = ident_response.iter().position(|&b| b == 0).unwrap_or(ident_response.len());
    let s = &ident_response[..resp_end];

    if s.len() < 2 {
        return None;
    }
    if s[s.len() - 2] != b'\r' {
        return None;
    }

    let mut cursor = 0usize;
    let at = |c: usize| -> u8 { *s.get(c).unwrap_or(&0) };

    // skip port field
    while at(cursor) != b':' && at(cursor) != b'\r' {
        cursor += 1;
    }
    if at(cursor) != b':' {
        return None;
    }
    cursor += 1; // Go over colon
    while pg_isblank(at(cursor)) {
        cursor += 1;
    }

    // response_type[80]
    let mut response_type: Vec<u8> = Vec::new();
    while at(cursor) != b':'
        && at(cursor) != b'\r'
        && !pg_isblank(at(cursor))
        && response_type.len() < 80 - 1
    {
        response_type.push(at(cursor));
        cursor += 1;
    }
    while pg_isblank(at(cursor)) {
        cursor += 1;
    }
    if response_type != b"USERID" {
        return None;
    }
    if at(cursor) != b':' {
        return None;
    }
    cursor += 1; // Go over colon
    // skip operating-system field
    while at(cursor) != b':' && at(cursor) != b'\r' {
        cursor += 1;
    }
    if at(cursor) != b':' {
        return None;
    }
    cursor += 1; // Go over colon
    while pg_isblank(at(cursor)) {
        cursor += 1;
    }

    let mut ident_user: Vec<u8> = Vec::new();
    while at(cursor) != b'\r' && ident_user.len() < IDENT_USERNAME_MAX {
        ident_user.push(at(cursor));
        cursor += 1;
    }

    Some(String::from_utf8_lossy(&ident_user).into_owned())
}

/// Address-family of a stored `SockAddr`'s `sockaddr_storage` (`ss_family`).
fn ss_family(sa: &SockAddr) -> i32 {
    // sa_family_t is the first field of sockaddr_storage.
    let fam = unsafe {
        let p = sa.addr.as_ptr() as *const libc::sockaddr_storage;
        (*p).ss_family
    };
    fam as i32
}

/// `ident_inet(port)` (`auth.c:1671`).
pub fn ident_inet(port: &Port) -> PgResult<i32> {
    let remote_addr = crate::raddr_sockaddr(port);
    let local_addr = SockAddr { addr: port.laddr.addr, salen: port.laddr.salen };

    // Convert addresses to text first.
    let (mut remote_addr_s, mut remote_port) = (String::new(), String::new());
    common_ip::pg_getnameinfo_all(
        &remote_addr,
        Some(&mut remote_addr_s),
        Some(&mut remote_port),
        libc::NI_NUMERICHOST | libc::NI_NUMERICSERV,
    );
    let (mut local_addr_s, mut local_port) = (String::new(), String::new());
    common_ip::pg_getnameinfo_all(
        &local_addr,
        Some(&mut local_addr_s),
        Some(&mut local_port),
        libc::NI_NUMERICHOST | libc::NI_NUMERICSERV,
    );

    let ident_port = format!("{IDENT_PORT}");

    // Resolve the ident server (remote host, port 113).
    let mut ident_serv: Vec<PgAddrInfo> = Vec::new();
    let hints = AddrInfoHint {
        flags: libc::AI_NUMERICHOST,
        family: ss_family(&remote_addr),
        socktype: libc::SOCK_STREAM,
    };
    let rc = common_ip::pg_getaddrinfo_all(
        Some(&remote_addr_s),
        Some(&ident_port),
        &hints,
        &mut ident_serv,
    );
    if rc != 0 || ident_serv.is_empty() {
        return Ok(STATUS_ERROR);
    }

    // Resolve the local address to bind from.
    let mut la: Vec<PgAddrInfo> = Vec::new();
    let hints = AddrInfoHint {
        flags: libc::AI_NUMERICHOST,
        family: ss_family(&local_addr),
        socktype: libc::SOCK_STREAM,
    };
    let rc = common_ip::pg_getaddrinfo_all(Some(&local_addr_s), None, &hints, &mut la);
    if rc != 0 || la.is_empty() {
        return Ok(STATUS_ERROR);
    }

    let serv = &ident_serv[0];
    let local = &la[0];

    let sock = unsafe { libc::socket(serv.family, serv.socktype, serv.protocol) };
    if sock < 0 {
        ereport(LOG)
            .errmsg(format!(
                "could not create socket for Ident connection: {}",
                std::io::Error::last_os_error()
            ))
            .finish(here("ident_inet"))?;
        return Ok(STATUS_ERROR);
    }
    let _guard = SockGuard(sock);

    let rc = unsafe {
        libc::bind(
            sock,
            local.addr.addr.as_ptr() as *const libc::sockaddr,
            local.addr.salen as libc::socklen_t,
        )
    };
    if rc != 0 {
        ereport(LOG)
            .errmsg(format!(
                "could not bind to local address \"{local_addr_s}\": {}",
                std::io::Error::last_os_error()
            ))
            .finish(here("ident_inet"))?;
        return Ok(STATUS_ERROR);
    }

    let rc = unsafe {
        libc::connect(
            sock,
            serv.addr.addr.as_ptr() as *const libc::sockaddr,
            serv.addr.salen as libc::socklen_t,
        )
    };
    if rc != 0 {
        ereport(LOG)
            .errmsg(format!(
                "could not connect to Ident server at address \"{remote_addr_s}\", port {IDENT_PORT}: {}",
                std::io::Error::last_os_error()
            ))
            .finish(here("ident_inet"))?;
        return Ok(STATUS_ERROR);
    }

    // ident_query = "<remote_port>,<local_port>\r\n"
    let ident_query = format!("{remote_port},{local_port}\r\n");
    let query_bytes = ident_query.as_bytes();

    // loop in case send is interrupted
    let sent = loop {
        crate::check_interrupts()?;
        let r = unsafe {
            libc::send(sock, query_bytes.as_ptr() as *const libc::c_void, query_bytes.len(), 0)
        };
        if r < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        break r;
    };
    if sent < 0 {
        ereport(LOG)
            .errmsg(format!(
                "could not send query to Ident server at address \"{remote_addr_s}\", port {IDENT_PORT}: {}",
                std::io::Error::last_os_error()
            ))
            .finish(here("ident_inet"))?;
        return Ok(STATUS_ERROR);
    }

    // ident_response[80 + IDENT_USERNAME_MAX]
    let mut ident_response = vec![0u8; 80 + IDENT_USERNAME_MAX];
    let recvd = loop {
        crate::check_interrupts()?;
        let r = unsafe {
            libc::recv(
                sock,
                ident_response.as_mut_ptr() as *mut libc::c_void,
                ident_response.len() - 1,
                0,
            )
        };
        if r < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        break r;
    };
    if recvd < 0 {
        ereport(LOG)
            .errmsg(format!(
                "could not receive response from Ident server at address \"{remote_addr_s}\", port {IDENT_PORT}: {}",
                std::io::Error::last_os_error()
            ))
            .finish(here("ident_inet"))?;
        return Ok(STATUS_ERROR);
    }

    ident_response[recvd as usize] = 0; // '\0'
    let ident_user = interpret_ident_response(&ident_response);
    if ident_user.is_none() {
        ereport(LOG)
            .errmsg(format!(
                "invalidly formatted response from Ident server: \"{}\"",
                String::from_utf8_lossy(&ident_response[..recvd as usize])
            ))
            .finish(here("ident_inet"))?;
    }

    // _guard drops the socket here (closesocket).
    drop(_guard);

    match ident_user {
        Some(ident_user) => {
            set_authn_id(port, &ident_user)?;
            let usermap = port.hba.as_ref().expect("ident_inet: port->hba is NULL").usermap.clone();
            seams::check_usermap::call(usermap, port_user_name(port), ident_user, false)
        }
        None => Ok(STATUS_ERROR),
    }
}

/// RAII socket close (`closesocket(sock_fd)` in the C `ident_inet_done` label).
struct SockGuard(libc::c_int);
impl Drop for SockGuard {
    fn drop(&mut self) {
        if self.0 >= 0 {
            unsafe { libc::close(self.0) };
            self.0 = -1;
        }
    }
}
