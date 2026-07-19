//! auth.c: ClientAuthentication and the per-method handlers. Live end-to-end:
//! trust, reject / implicit-reject (SQLSTATE 28000 exact), peer, ident
//! (RFC 1413 ident_inet), cert (CheckCertAuth, hostssl-only), and the
//! password family — password / md5 / scram-sha-256 via CheckPWChallengeAuth +
//! CheckSASLAuth. Loud: radius / oauth. gss / sspi / pam / bsd / ldap never
//! reach dispatch — hba rejects those methods in this build.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

#[cfg(test)]
mod tests;

use elog::{elog, ereport};
use hba::pg_isblank;
use mcx::MemoryContext;
use types_core::init::{
    uaBSD, uaCert, uaGSS, uaIdent, uaImplicitReject, uaLDAP, uaMD5, uaOAuth, uaPAM, uaPassword,
    uaPeer, uaRADIUS, uaReject, uaSCRAM, uaTrust, UserAuth,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG5, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION,
    ERRCODE_INVALID_PASSWORD, ERRCODE_PROTOCOL_VIOLATION, ERROR, FATAL, LOG,
};
use types_startup::{clientCertDN, clientCertFull, clientCertOff, Port};

pub const STATUS_OK: i32 = 0;
pub const STATUS_ERROR: i32 = -1;
pub const STATUS_EOF: i32 = -2;

pub type AuthRequest = u32;
pub const AUTH_REQ_OK: AuthRequest = 0;
pub const AUTH_REQ_PASSWORD: AuthRequest = 3;
pub const AUTH_REQ_MD5: AuthRequest = 5;
pub const AUTH_REQ_GSS: AuthRequest = 7;
pub const AUTH_REQ_SSPI: AuthRequest = 9;
pub use auth_sasl::{AUTH_REQ_SASL, AUTH_REQ_SASL_CONT, AUTH_REQ_SASL_FIN};

pub const PqMsg_AuthenticationRequest: u8 = b'R';
pub const PqMsg_PasswordMessage: u8 = b'p';

pub const PG_MAX_AUTH_TOKEN_LENGTH: i32 = 65535;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/libpq/auth.c", line, func)
}

pub fn auth_failed(port: &Port, status: i32, logdetail: Option<&str>) -> PgResult<()> {
    if status == STATUS_EOF {
        ipc_seams::proc_exit::call(0, init_small::globals::MyProcPid());
    }

    let hba = port.hba.as_ref().expect("auth_failed: port->hba is NULL");

    let mut errcode_return = ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION;
    let errstr: &str = match hba.auth_method {
        m if m == uaReject || m == uaImplicitReject => {
            "authentication failed for user \"%s\": host rejected"
        }
        m if m == uaTrust => "\"trust\" authentication failed for user \"%s\"",
        m if m == uaIdent => "Ident authentication failed for user \"%s\"",
        m if m == uaPeer => "Peer authentication failed for user \"%s\"",
        m if m == uaPassword || m == uaMD5 || m == uaSCRAM => {
            // We use it to indicate if a .pgpass password failed.
            errcode_return = ERRCODE_INVALID_PASSWORD;
            "password authentication failed for user \"%s\""
        }
        m if m == uaGSS => "GSSAPI authentication failed for user \"%s\"",
        m if m == types_core::init::uaSSPI => "SSPI authentication failed for user \"%s\"",
        m if m == uaPAM => "PAM authentication failed for user \"%s\"",
        m if m == uaBSD => "BSD authentication failed for user \"%s\"",
        m if m == uaLDAP => "LDAP authentication failed for user \"%s\"",
        m if m == uaCert => "certificate authentication failed for user \"%s\"",
        m if m == uaRADIUS => "RADIUS authentication failed for user \"%s\"",
        m if m == uaOAuth => "OAuth bearer authentication failed for user \"%s\"",
        _ => "authentication failed for user \"%s\": invalid authentication method",
    };

    let cdetail = format!(
        "Connection matched file \"{}\" line {}: \"{}\"",
        hba.sourcefile, hba.linenumber, hba.rawline
    );
    let logdetail = match logdetail {
        Some(ld) => format!("{ld}\n{cdetail}"),
        None => cdetail,
    };

    ereport(FATAL)
        .errcode(errcode_return)
        .errmsg(errstr.replacen("%s", port.user_name.as_deref().unwrap_or(""), 1))
        .errdetail_log(logdetail)
        .finish(loc(316, "auth_failed"))
}

fn CheckCertAuth(port: &Port) -> PgResult<i32> {
    let hba = port.hba.as_ref().expect("CheckCertAuth: port->hba is NULL");
    let user_name = port.user_name.as_deref().unwrap_or("");

    let peer_username = if hba.clientcertname == clientCertDN {
        port.peer_dn.as_deref()
    } else {
        port.peer_cn.as_deref()
    };

    let Some(peer_username) = peer_username.filter(|u| !u.is_empty()) else {
        ereport(LOG)
            .errmsg(format!(
                "certificate authentication failed for user \"{user_name}\": client certificate contains no user name"
            ))
            .finish(loc(2710, "CheckCertAuth"))?;
        return Ok(STATUS_ERROR);
    };

    if hba.auth_method == uaCert {
        let Some(peer_dn) = port.peer_dn.as_deref() else {
            ereport(LOG)
                .errmsg(format!(
                    "certificate authentication failed for user \"{user_name}\": unable to retrieve subject DN"
                ))
                .finish(loc(2730, "CheckCertAuth"))?;
            return Ok(STATUS_ERROR);
        };
        set_authn_id(port, peer_dn)?;
    }

    let status_check_usermap =
        hba::check_usermap(hba.usermap.as_deref(), user_name, peer_username, false)?;
    if status_check_usermap != STATUS_OK
        && hba.clientcert == clientCertFull
        && hba.auth_method != uaCert
    {
        let what = if hba.clientcertname == clientCertDN {
            "DN mismatch"
        } else {
            "CN mismatch"
        };
        ereport(LOG)
            .errmsg(format!(
                "certificate validation (clientcert=verify-full) failed for user \"{user_name}\": {what}"
            ))
            .finish(loc(2755, "CheckCertAuth"))?;
    }
    Ok(status_check_usermap)
}

pub fn set_authn_id(port: &Port, id: &str) -> PgResult<()> {
    let (authn_id, _) = miscinit::client_connection_info();
    if let Some(previous) = authn_id {
        ereport(FATAL)
            .errmsg("authentication identifier set more than once")
            .errdetail_log(format!(
                "previous identifier: \"{previous}\"; new identifier: \"{id}\""
            ))
            .finish(loc(356, "set_authn_id"))?;
    }

    let auth_method = port_auth_method(port);
    miscinit::set_client_connection_info(Some(id), auth_method);

    if backend_startup::log_connections::get() & backend_startup::LOG_CONNECTION_AUTHENTICATION
        != 0
    {
        let hba = port.hba.as_ref().expect("port->hba is NULL");
        ereport(LOG)
            .errmsg(format!(
                "connection authenticated: identity=\"{id}\" method={} ({}:{})",
                hba::hba_authname(auth_method),
                hba.sourcefile,
                hba.linenumber,
            ))
            .finish(loc(371, "set_authn_id"))?;
    }
    Ok(())
}

pub fn ClientAuthentication(port: &mut Port) -> PgResult<()> {
    let mut logdetail: Option<String> = None;

    hba::hba_getauthmethod(port)?;

    postgres_seams::check_for_interrupts::call()?;

    let clientcert = port
        .hba
        .as_ref()
        .expect("ClientAuthentication: port->hba is NULL")
        .clientcert;
    if clientcert != clientCertOff {
        if !be_secure::secure_loaded_verify_locations() {
            return ereport(FATAL)
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg("client certificates can only be checked if a root certificate store is available")
                .finish(loc(404, "ClientAuthentication"));
        }
        if !port.peer_cert_valid {
            return ereport(FATAL)
                .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                .errmsg("connection requires a valid client certificate")
                .finish(loc(414, "ClientAuthentication"));
        }
    }

    let auth_method = port_auth_method(port);

    let status: i32 = match auth_method {
        m if m == uaReject => return reject_arm(port, true),
        m if m == uaImplicitReject => return reject_arm(port, false),
        m if m == uaPeer => auth_peer(port)?,
        m if m == uaIdent => ident_inet(port)?,
        m if m == uaMD5 || m == uaSCRAM => CheckPWChallengeAuth(port, &mut logdetail)?,
        m if m == uaPassword => CheckPasswordAuth(port, &mut logdetail)?,
        m if m == uaRADIUS => deferred_arm("radius", "CheckRADIUSAuth"),
        m if m == uaOAuth => deferred_arm("oauth", "CheckSASLAuth(oauth)"),
        m if m == uaCert || m == uaTrust => STATUS_OK,
        // gss/sspi/pam/bsd/ldap: hba rejects these methods in this build.
        m => unreachable!("ClientAuthentication: unreachable auth method {m}"),
    };

    let status = if (status == STATUS_OK && clientcert == clientCertFull) || auth_method == uaCert
    {
        CheckCertAuth(port)?
    } else {
        status
    };

    if backend_startup::log_connections::get() & backend_startup::LOG_CONNECTION_AUTHENTICATION
        != 0
        && status == STATUS_OK
        && miscinit::client_connection_info().0.is_none()
    {
        // set_authn_id never ran (e.g. trust): log the connection here.
        let hba = port.hba.as_ref().expect("port->hba is NULL");
        ereport(LOG)
            .errmsg(format!(
                "connection authenticated: user=\"{}\" method={} ({}:{})",
                port.user_name.as_deref().unwrap_or(""),
                hba::hba_authname(auth_method),
                hba.sourcefile,
                hba.linenumber,
            ))
            .finish(loc(659, "ClientAuthentication"))?;
    }

    // ClientAuthentication_hook: NULL in this build (no auth module loaded).

    if status == STATUS_OK {
        sendAuthRequest(port, AUTH_REQ_OK, &[])?;
    } else {
        auth_failed(port, status, logdetail.as_deref())?;
    }

    Ok(())
}

#[cold]
fn deferred_arm(method: &str, what: &str) -> i32 {
    panic!("ClientAuthentication: \"{method}\" arm deferred — {what} unported");
}

pub const IDENT_USERNAME_MAX: usize = 512;
#[cfg(not(target_family = "wasm"))]
const IDENT_PORT: u16 = 113;

// interpret_ident_response (auth.c:1590): parse the reply to an RFC 1413
// Ident query. A normal "USERID" response yields Some(user name); anything
// else is None. Byte-walk with a NUL-past-end cursor, matching C's string
// scan; C's out-of-string scans (only reachable when blank-skipping consumes
// the final CR) become bounded failure arms here.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
fn interpret_ident_response(ident_response: &[u8]) -> Option<String> {
    // C reads a NUL-terminated buffer; stop at the first NUL like strlen.
    let nul = ident_response
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(ident_response.len());
    let resp = &ident_response[..nul];
    let len = resp.len();
    let at = |i: usize| resp.get(i).copied().unwrap_or(0);

    // Ident's response, in the telnet tradition, should end in crlf (\r\n).
    if len < 2 || resp[len - 2] != b'\r' {
        return None;
    }

    let mut cursor = 0usize;
    while at(cursor) != b':' && at(cursor) != b'\r' {
        cursor += 1; // skip port field
    }
    if at(cursor) != b':' {
        return None;
    }

    // We're positioned to colon before response type field.
    cursor += 1; // Go over colon
    while pg_isblank(at(cursor)) {
        cursor += 1; // skip blanks
    }
    let mut response_type: Vec<u8> = Vec::with_capacity(79);
    while at(cursor) != b':'
        && at(cursor) != b'\r'
        && !pg_isblank(at(cursor))
        && response_type.len() < 79
    {
        if cursor >= len {
            return None;
        }
        response_type.push(at(cursor));
        cursor += 1;
    }
    while pg_isblank(at(cursor)) {
        cursor += 1; // skip blanks
    }
    if response_type != b"USERID" {
        return None;
    }

    // It's a USERID response.  Good.  "cursor" should be pointing to the
    // colon that precedes the operating system type.
    if at(cursor) != b':' {
        return None;
    }
    cursor += 1; // Go over colon
    // Skip over operating system field.
    while at(cursor) != b':' && at(cursor) != b'\r' {
        if cursor >= len {
            return None;
        }
        cursor += 1;
    }
    if at(cursor) != b':' {
        return None;
    }
    cursor += 1; // Go over colon
    while pg_isblank(at(cursor)) {
        cursor += 1; // skip blanks
    }
    // Rest of line is user name.  Copy it over.
    let mut ident_user: Vec<u8> = Vec::new();
    while cursor < len && at(cursor) != b'\r' && ident_user.len() < IDENT_USERNAME_MAX {
        ident_user.push(at(cursor));
        cursor += 1;
    }
    Some(String::from_utf8_lossy(&ident_user).into_owned())
}

// Closes the Ident socket on every exit path (C's ident_inet_done label).
#[cfg(not(target_family = "wasm"))]
struct SocketGuard(i32);
#[cfg(not(target_family = "wasm"))]
impl Drop for SocketGuard {
    fn drop(&mut self) {
        // SAFETY: the guard owns the fd; closed exactly once.
        unsafe { libc::close(self.0) };
    }
}

// wasm32: WASI has no resolver and no TCP sockets; pg_getaddrinfo_all always
// fails there, which is C's silent "we don't expect this to happen" arm.
#[cfg(target_family = "wasm")]
fn ident_inet(_port: &Port) -> PgResult<i32> {
    Ok(STATUS_ERROR)
}

// Talk to the ident server on "remote_addr" and find out who owns the tcp
// connection to "local_addr". If the username is successfully retrieved,
// check the usermap.
#[cfg(not(target_family = "wasm"))]
fn ident_inet(port: &Port) -> PgResult<i32> {
    use ip::{pg_getaddrinfo_all, pg_getnameinfo_all, AddrInfoHint, PgAddrInfo};

    // Might look a little weird to first convert it to text and then back to
    // sockaddr, but it's protocol independent.
    let mut remote_addr_s = String::new();
    let mut remote_port = String::new();
    let mut local_addr_s = String::new();
    let mut local_port = String::new();
    pg_getnameinfo_all(
        &port.raddr,
        Some(&mut remote_addr_s),
        Some(&mut remote_port),
        ip::sys::NI_NUMERICHOST | ip::sys::NI_NUMERICSERV,
    );
    pg_getnameinfo_all(
        &port.laddr,
        Some(&mut local_addr_s),
        Some(&mut local_port),
        ip::sys::NI_NUMERICHOST | ip::sys::NI_NUMERICSERV,
    );

    let ident_port = IDENT_PORT.to_string();
    let hints = AddrInfoHint {
        flags: ip::sys::AI_NUMERICHOST,
        family: ip::sockaddr_family(&port.raddr),
        socktype: ip::sys::SOCK_STREAM,
    };
    let mut ident_serv: Vec<PgAddrInfo> = Vec::new();
    let rc = pg_getaddrinfo_all(Some(&remote_addr_s), Some(&ident_port), &hints, &mut ident_serv);
    if rc != 0 || ident_serv.is_empty() {
        return Ok(STATUS_ERROR); // we don't expect this to happen
    }

    let hints = AddrInfoHint {
        flags: ip::sys::AI_NUMERICHOST,
        family: ip::sockaddr_family(&port.laddr),
        socktype: ip::sys::SOCK_STREAM,
    };
    let mut la: Vec<PgAddrInfo> = Vec::new();
    let rc = pg_getaddrinfo_all(Some(&local_addr_s), None, &hints, &mut la);
    if rc != 0 || la.is_empty() {
        return Ok(STATUS_ERROR); // we don't expect this to happen
    }

    let serv = ident_serv[0];
    // SAFETY: family/socktype/protocol come straight from the resolver.
    let sock_fd = unsafe { libc::socket(serv.family, serv.socktype, serv.protocol) };
    if sock_fd < 0 {
        let errnum = elog::errno::current_errno();
        ereport(LOG)
            .with_saved_errno(errnum)
            .errcode_for_socket_access()
            .errmsg("could not create socket for Ident connection: %m")
            .finish(loc(1742, "ident_inet"))?;
        return Ok(STATUS_ERROR);
    }
    let _guard = SocketGuard(sock_fd);

    let ident_user = ident_inet_exchange(
        sock_fd,
        &la[0],
        &serv,
        &remote_addr_s,
        &local_addr_s,
        &remote_port,
        &local_port,
        &ident_port,
    )?;

    if let Some(ident_user) = ident_user {
        // Success!  Store the identity, then check the usermap. Note that
        // setting the authenticated identity is done before checking the
        // usermap, because at this point authentication has succeeded.
        set_authn_id(port, &ident_user)?;
        let hba = port.hba.as_ref().expect("ident_inet: port->hba is NULL");
        return hba::check_usermap(
            hba.usermap.as_deref(),
            port.user_name.as_deref().unwrap_or(""),
            &ident_user,
            false,
        );
    }
    Ok(STATUS_ERROR)
}

// The bind/connect/query/response body of C's ident_inet; the caller holds
// the SocketGuard so every `?`/early return closes the socket.
#[cfg(not(target_family = "wasm"))]
#[allow(clippy::too_many_arguments)]
fn ident_inet_exchange(
    sock_fd: i32,
    la: &ip::PgAddrInfo,
    ident_serv: &ip::PgAddrInfo,
    remote_addr_s: &str,
    local_addr_s: &str,
    remote_port: &str,
    local_port: &str,
    ident_port: &str,
) -> PgResult<Option<String>> {
    // Bind to the address which the client originally contacted, otherwise
    // the ident server won't be able to match up the right connection. This
    // is necessary if the PostgreSQL server is running on an IP alias.
    // SAFETY: la.addr holds salen valid sockaddr bytes.
    let rc = unsafe { libc::bind(sock_fd, la.addr.addr.as_ptr().cast(), la.addr.salen) };
    if rc != 0 {
        let errnum = elog::errno::current_errno();
        ereport(LOG)
            .with_saved_errno(errnum)
            .errcode_for_socket_access()
            .errmsg(format!(
                "could not bind to local address \"{local_addr_s}\": %m"
            ))
            .finish(loc(1755, "ident_inet"))?;
        return Ok(None);
    }

    // SAFETY: ident_serv.addr holds salen valid sockaddr bytes.
    let rc = unsafe {
        libc::connect(
            sock_fd,
            ident_serv.addr.addr.as_ptr().cast(),
            ident_serv.addr.salen,
        )
    };
    if rc != 0 {
        let errnum = elog::errno::current_errno();
        ereport(LOG)
            .with_saved_errno(errnum)
            .errcode_for_socket_access()
            .errmsg(format!(
                "could not connect to Ident server at address \"{remote_addr_s}\", port {ident_port}: %m"
            ))
            .finish(loc(1767, "ident_inet"))?;
        return Ok(None);
    }

    // The query we send to the Ident server.
    let ident_query = format!("{remote_port},{local_port}\r\n");

    // loop in case send is interrupted
    let (rc, errnum) = loop {
        postgres_seams::check_for_interrupts::call()?;
        // SAFETY: the query buffer is valid for its length.
        let rc = unsafe { libc::send(sock_fd, ident_query.as_ptr().cast(), ident_query.len(), 0) };
        let errnum = if rc < 0 { elog::errno::current_errno() } else { 0 };
        if rc < 0 && errnum == libc::EINTR {
            continue;
        }
        break (rc, errnum);
    };
    if rc < 0 {
        ereport(LOG)
            .with_saved_errno(errnum)
            .errcode_for_socket_access()
            .errmsg(format!(
                "could not send query to Ident server at address \"{remote_addr_s}\", port {ident_port}: %m"
            ))
            .finish(loc(1789, "ident_inet"))?;
        return Ok(None);
    }

    let mut ident_response = [0u8; 80 + IDENT_USERNAME_MAX];
    let (rc, errnum) = loop {
        postgres_seams::check_for_interrupts::call()?;
        // SAFETY: the buffer is valid for len-1 bytes (C keeps a NUL slot).
        let rc = unsafe {
            libc::recv(
                sock_fd,
                ident_response.as_mut_ptr().cast(),
                ident_response.len() - 1,
                0,
            )
        };
        let errnum = if rc < 0 { elog::errno::current_errno() } else { 0 };
        if rc < 0 && errnum == libc::EINTR {
            continue;
        }
        break (rc, errnum);
    };
    if rc < 0 {
        ereport(LOG)
            .with_saved_errno(errnum)
            .errcode_for_socket_access()
            .errmsg(format!(
                "could not receive response from Ident server at address \"{remote_addr_s}\", port {ident_port}: %m"
            ))
            .finish(loc(1806, "ident_inet"))?;
        return Ok(None);
    }

    let response = &ident_response[..rc as usize];
    let ident_user = interpret_ident_response(response);
    if ident_user.is_none() {
        // C prints the NUL-terminated buffer with %s.
        let printable = &response[..response.iter().position(|&b| b == 0).unwrap_or(response.len())];
        ereport(LOG)
            .errmsg(format!(
                "invalidly formatted response from Ident server: \"{}\"",
                String::from_utf8_lossy(printable)
            ))
            .finish(loc(1817, "ident_inet"))?;
    }
    Ok(ident_user)
}

// wasm32: no peer sockets and no credential surface on WASI; ENOSYS routes
// auth_peer to C's "peer authentication is not supported on this platform".
#[cfg(target_family = "wasm")]
fn getpeereid(_sock: i32) -> Result<(libc::uid_t, libc::gid_t), i32> {
    Err(libc::ENOSYS)
}

#[cfg(not(target_family = "wasm"))]
fn getpeereid(sock: i32) -> Result<(libc::uid_t, libc::gid_t), i32> {
    #[cfg(target_os = "linux")]
    {
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                sock,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::addr_of_mut!(cred).cast(),
                &mut len,
            )
        };
        if rc != 0 || len as usize != std::mem::size_of::<libc::ucred>() {
            return Err(elog::errno::current_errno());
        }
        Ok((cred.uid, cred.gid))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let (mut uid, mut gid) = (0, 0);
        if unsafe { libc::getpeereid(sock, &mut uid, &mut gid) } != 0 {
            return Err(elog::errno::current_errno());
        }
        Ok((uid, gid))
    }
}

// wasm32: getpeereid always ENOSYSes above (no passwd db exists either);
// the twin stops at the same "not supported" report as the native ENOSYS arm.
#[cfg(target_family = "wasm")]
fn auth_peer(port: &Port) -> PgResult<i32> {
    let errnum = getpeereid(port.sock).expect_err("wasm getpeereid always fails");
    debug_assert_eq!(errnum, libc::ENOSYS);
    ereport(LOG)
        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg("peer authentication is not supported on this platform")
        .finish(loc(1874, "auth_peer"))?;
    Ok(STATUS_ERROR)
}

#[cfg(not(target_family = "wasm"))]
fn auth_peer(port: &Port) -> PgResult<i32> {
    let (uid, _gid) = match getpeereid(port.sock) {
        Ok(v) => v,
        Err(errnum) => {
            if errnum == libc::ENOSYS {
                ereport(LOG)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("peer authentication is not supported on this platform")
                    .finish(loc(1874, "auth_peer"))?;
            } else {
                ereport(LOG)
                    .with_saved_errno(errnum)
                    .errcode_for_socket_access()
                    .errmsg("could not get peer credentials: %m")
                    .finish(loc(1878, "auth_peer"))?;
            }
            return Ok(STATUS_ERROR);
        }
    };

    let mut pwbuf: libc::passwd = unsafe { std::mem::zeroed() };
    let mut buf = [0u8; 1024];
    let mut pw: *mut libc::passwd = std::ptr::null_mut();
    let rc = unsafe {
        libc::getpwuid_r(uid, &mut pwbuf, buf.as_mut_ptr().cast(), buf.len(), &mut pw)
    };
    if rc != 0 {
        ereport(LOG)
            .with_saved_errno(rc)
            .errmsg(format!("could not look up local user ID {}: %m", uid as i64))
            .finish(loc(1888, "auth_peer"))?;
        return Ok(STATUS_ERROR);
    }
    if pw.is_null() {
        ereport(LOG)
            .errmsg(format!("local user with ID {} does not exist", uid as i64))
            .finish(loc(1894, "auth_peer"))?;
        return Ok(STATUS_ERROR);
    }

    // Copy out of the getpwuid_r buffer before check_usermap; this is our
    // authenticated identity, set before checking the map so the log
    // reflects that authentication succeeded.
    let pw_name = unsafe { std::ffi::CStr::from_ptr(pwbuf.pw_name) }
        .to_string_lossy()
        .into_owned();
    set_authn_id(port, &pw_name)?;

    let hba = port.hba.as_ref().expect("auth_peer: port->hba is NULL");
    let (authn_id, _) = miscinit::client_connection_info();
    hba::check_usermap(
        hba.usermap.as_deref(),
        port.user_name.as_deref().unwrap_or(""),
        authn_id.expect("authn_id set by set_authn_id"),
        false,
    )
}

fn reject_arm(port: &Port, explicit_reject: bool) -> PgResult<()> {
    let mut hostinfo = String::new();
    ip::pg_getnameinfo_all(&port.raddr, Some(&mut hostinfo), None, ip::sys::NI_NUMERICHOST);

    let encryption_state = if port.ssl_in_use {
        "SSL encryption"
    } else {
        "no encryption"
    };

    let user_name = port.user_name.as_deref().unwrap_or("");
    let database_name = port.database_name.as_deref().unwrap_or("");
    // Physical walsenders get the replication wording, as in C's check_db
    // "replication" keyword matching.
    let am_physical_walsender =
        walsender_seams::am_walsender() && !walsender_seams::am_db_walsender();

    let mut builder = if explicit_reject {
        if am_physical_walsender {
            ereport(FATAL)
                .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                .errmsg(format!(
                    "pg_hba.conf rejects replication connection for host \"{hostinfo}\", user \"{user_name}\", {encryption_state}"
                ))
        } else {
            ereport(FATAL)
                .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                .errmsg(format!(
                    "pg_hba.conf rejects connection for host \"{hostinfo}\", user \"{user_name}\", database \"{database_name}\", {encryption_state}"
                ))
        }
    } else if am_physical_walsender {
        ereport(FATAL)
            .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            .errmsg(format!(
                "no pg_hba.conf entry for replication connection from host \"{hostinfo}\", user \"{user_name}\", {encryption_state}"
            ))
    } else {
        ereport(FATAL)
            .errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            .errmsg(format!(
                "no pg_hba.conf entry for host \"{hostinfo}\", user \"{user_name}\", database \"{database_name}\", {encryption_state}"
            ))
    };

    // HOSTNAME_LOOKUP_DETAIL applies only to the implicit-reject path.
    if !explicit_reject {
        if let Some(detail) = hostname_lookup_detail(port) {
            builder = builder.errdetail_log(detail);
        }
    }

    builder.finish(loc(
        if explicit_reject { 463 } else { 530 },
        "ClientAuthentication",
    ))
}

fn hostname_lookup_detail(port: &Port) -> Option<String> {
    let resolv = port.remote_hostname_resolv;
    match port.remote_hostname.as_deref() {
        Some(h) => match resolv {
            1 => Some(format!(
                "Client IP address resolved to \"{h}\", forward lookup matches."
            )),
            0 => Some(format!(
                "Client IP address resolved to \"{h}\", forward lookup not checked."
            )),
            -1 => Some(format!(
                "Client IP address resolved to \"{h}\", forward lookup does not match."
            )),
            -2 => Some(format!(
                "Could not translate client host name \"{h}\" to IP address: {}.",
                gai_strerror(port.remote_hostname_errcode)
            )),
            _ => None,
        },
        None => {
            if resolv == -2 {
                Some(format!(
                    "Could not resolve client IP address to a host name: {}.",
                    gai_strerror(port.remote_hostname_errcode)
                ))
            } else {
                None
            }
        }
    }
}

pub fn sendAuthRequest(_port: &Port, areq: AuthRequest, extradata: &[u8]) -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()?;

    let scratch = MemoryContext::new("sendAuthRequest");
    let mut buf = pqformat::pq_beginmessage(scratch.mcx(), PqMsg_AuthenticationRequest)?;
    pqformat::pq_sendint32(&mut buf, areq)?;
    if !extradata.is_empty() {
        pqformat::pq_sendbytes(&mut buf, extradata)?;
    }
    pqformat::pq_endmessage(buf)?;

    // AUTH_REQ_OK / AUTH_REQ_SASL_FIN need not be sent until ready for queries.
    if areq != AUTH_REQ_OK && areq != AUTH_REQ_SASL_FIN {
        pqcomm::pq_flush()?;
    }

    postgres_seams::check_for_interrupts::call()?;
    Ok(())
}

pub fn recv_password_packet(_port: &Port) -> PgResult<Option<String>> {
    pqcomm::pq_startmsgread()?;

    let mtype = pqcomm::pq_getbyte()?;
    if mtype != PqMsg_PasswordMessage as i32 {
        // EOF (-1) is silent; any other type is a protocol violation.
        if mtype != -1 {
            ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "expected password response, got message type {mtype}"
                ))
                .finish(loc(722, "recv_password_packet"))?;
        }
        return Ok(None);
    }

    let scratch = MemoryContext::new("recv_password_packet");
    let mut buf = stringinfo::StringInfo::new_in(scratch.mcx())?;
    if pqcomm::pq_getmessage(&mut buf, PG_MAX_AUTH_TOKEN_LENGTH)? != 0 {
        return Ok(None); // EOF; pq_getmessage already logged
    }

    // Packet length must agree with the contained NUL-terminated string.
    let data = buf.as_bytes();
    let strlen = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    if strlen + 1 != data.len() {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid password packet size")
            .finish(loc(741, "recv_password_packet"))?;
    }

    if data.len() == 1 {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PASSWORD)
            .errmsg("empty password returned by client")
            .finish(loc(752, "recv_password_packet"))?;
    }

    // Do not echo password to logs, for security.
    elog(DEBUG5, "received password packet")?;

    Ok(Some(String::from_utf8_lossy(&data[..strlen]).into_owned()))
}

fn CheckPasswordAuth(port: &mut Port, logdetail: &mut Option<String>) -> PgResult<i32> {
    sendAuthRequest(port, AUTH_REQ_PASSWORD, &[])?;

    let Some(passwd) = recv_password_packet(port)? else {
        return Ok(STATUS_EOF); // client wouldn't send password
    };

    let user_name = port.user_name.clone().unwrap_or_default();
    let result = match crypt::get_role_password(&user_name, logdetail)? {
        Some(shadow_pass) => {
            let scratch = MemoryContext::new("CheckPasswordAuth");
            crypt::plain_crypt_verify(scratch.mcx(), &user_name, &shadow_pass, &passwd, logdetail)?
        }
        None => STATUS_ERROR,
    };

    if result == STATUS_OK {
        set_authn_id(port, &user_name)?;
    }
    Ok(result)
}

fn CheckPWChallengeAuth(port: &mut Port, logdetail: &mut Option<String>) -> PgResult<i32> {
    let auth_method = port_auth_method(port);
    debug_assert!(auth_method == uaSCRAM || auth_method == uaMD5);

    let user_name = port.user_name.clone().unwrap_or_default();
    let shadow_pass = crypt::get_role_password(&user_name, logdetail)?;

    // No password (or no such user, or expired): still go through the
    // motions with a doomed exchange, choosing md5 vs scram by
    // password_encryption so the mock "blends in".
    let pwtype = match &shadow_pass {
        None => crypt::PasswordType::from_guc(guc_tables::vars::Password_encryption.read()),
        Some(sp) => crypt::get_password_type(sp),
    };

    // uaMD5 with a SCRAM secret runs SCRAM; uaSCRAM with an MD5 secret runs
    // SCRAM and fails (doomed).
    let auth_result = if auth_method == uaMD5 && pwtype == crypt::PasswordType::Md5 {
        CheckMD5Auth(port, shadow_pass.as_deref(), logdetail)?
    } else {
        auth_sasl::CheckSASLAuth(
            &auth_scram::ScramMech,
            port,
            shadow_pass.as_deref(),
            logdetail,
            sendAuthRequest,
        )?
    };

    // If get_role_password() returned error, authentication better not
    // have succeeded.
    debug_assert!(shadow_pass.is_some() || auth_result != STATUS_OK);

    if auth_result == STATUS_OK {
        set_authn_id(port, &user_name)?;
    }
    Ok(auth_result)
}

fn CheckMD5Auth(
    port: &Port,
    shadow_pass: Option<&str>,
    logdetail: &mut Option<String>,
) -> PgResult<i32> {
    let mut md5_salt = [0u8; 4];
    if !pg_strong_random::pg_strong_random(&mut md5_salt) {
        ereport(LOG)
            .errmsg("could not generate random MD5 salt")
            .finish(loc(890, "CheckMD5Auth"))?;
        return Ok(STATUS_ERROR);
    }

    sendAuthRequest(port, AUTH_REQ_MD5, &md5_salt)?;

    let Some(passwd) = recv_password_packet(port)? else {
        return Ok(STATUS_EOF); // client wouldn't send password
    };

    match shadow_pass {
        Some(shadow_pass) => crypt::md5_crypt_verify(
            port.user_name.as_deref().unwrap_or(""),
            shadow_pass,
            &passwd,
            &md5_salt,
            logdetail,
        ),
        None => Ok(STATUS_ERROR),
    }
}

pub(crate) fn port_auth_method(port: &Port) -> UserAuth {
    port.hba.as_ref().expect("port->hba is NULL").auth_method
}

fn gai_strerror(errcode: i32) -> String {
    // SAFETY: gai_strerror returns a static NUL-terminated C string.
    unsafe {
        let p = ip::sys::gai_strerror(errcode);
        if p.is_null() {
            return String::new();
        }
        std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

pub fn init_seams() {
    auth_seams::client_authentication::set(client_authentication_entry);
}

fn client_authentication_entry() -> PgResult<()> {
    init_small::globals::WithMyProcPort(ClientAuthentication)
}
