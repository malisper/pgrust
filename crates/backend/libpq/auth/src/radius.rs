//! `CheckRADIUSAuth` (`auth.c:2845`) + `radius_add_attribute` (`auth.c:2818`) +
//! `PerformRadiusTransaction` (`auth.c:2939`) — RADIUS authentication.

use ::utils_error::{elog, ereport};
use ::types_error::{PgResult, LOG, WARNING};
use ::net::{AddrInfoHint, PgAddrInfo, Port};

use crate::seams;
use crate::{here, port_user_name, recv_password_packet, sendAuthRequest, set_authn_id};
use crate::{AUTH_REQ_PASSWORD, STATUS_EOF, STATUS_ERROR, STATUS_OK};

const RADIUS_VECTOR_LENGTH: usize = 16;
const RADIUS_HEADER_LENGTH: usize = 20;
const RADIUS_MAX_PASSWORD_LENGTH: usize = 128;
const RADIUS_BUFFER_SIZE: usize = 1024;

// RADIUS packet types.
const RADIUS_ACCESS_REQUEST: u8 = 1;
const RADIUS_ACCESS_ACCEPT: u8 = 2;
const RADIUS_ACCESS_REJECT: u8 = 3;

// RADIUS attributes.
const RADIUS_USER_NAME: u8 = 1;
const RADIUS_PASSWORD: u8 = 2;
const RADIUS_SERVICE_TYPE: u8 = 6;
const RADIUS_NAS_IDENTIFIER: u8 = 32;

// RADIUS service types.
const RADIUS_AUTHENTICATE_ONLY: u32 = 8;

// Seconds to wait for a RADIUS response.
const RADIUS_TIMEOUT: i64 = 3;

/// In-memory RADIUS packet. The C uses a `struct radius_packet` whose tail is a
/// `pad[RADIUS_BUFFER_SIZE - RADIUS_VECTOR_LENGTH]` raw region into which
/// attributes are appended by byte offset. We mirror that with a fixed
/// `RADIUS_BUFFER_SIZE` byte buffer and a logical `length`, then read/write the
/// header fields by offset exactly as the C casts do (code@0, id@1,
/// length@2..4 big-endian, vector@4..20).
struct RadiusPacket {
    buf: [u8; RADIUS_BUFFER_SIZE],
    /// Logical length used for `radius_add_attribute` bookkeeping (host order).
    length: usize,
}

impl RadiusPacket {
    fn new() -> Self {
        RadiusPacket { buf: [0; RADIUS_BUFFER_SIZE], length: 0 }
    }
    fn code(&self) -> u8 {
        self.buf[0]
    }
    fn set_code(&mut self, c: u8) {
        self.buf[0] = c;
    }
    fn id(&self) -> u8 {
        self.buf[1]
    }
    fn set_id(&mut self, i: u8) {
        self.buf[1] = i;
    }
    /// Network-order length field at offset 2 (`pg_ntoh16(packet->length)`).
    fn length_field(&self) -> u16 {
        u16::from_be_bytes([self.buf[2], self.buf[3]])
    }
    fn set_length_field(&mut self, v: u16) {
        let b = v.to_be_bytes();
        self.buf[2] = b[0];
        self.buf[3] = b[1];
    }
    /// `packet->vector` (offset 4, 16 bytes).
    fn vector(&self) -> [u8; RADIUS_VECTOR_LENGTH] {
        let mut v = [0u8; RADIUS_VECTOR_LENGTH];
        v.copy_from_slice(&self.buf[4..4 + RADIUS_VECTOR_LENGTH]);
        v
    }
    fn vector_mut(&mut self) -> &mut [u8] {
        &mut self.buf[4..4 + RADIUS_VECTOR_LENGTH]
    }
}

/// `radius_add_attribute(packet, type, data, len)` (`auth.c:2818`).
fn radius_add_attribute(packet: &mut RadiusPacket, atype: u8, data: &[u8]) {
    let len = data.len();
    if packet.length + len > RADIUS_BUFFER_SIZE {
        let _ = elog(
            WARNING,
            &format!(
                "adding attribute code {atype} with length {len} to radius packet would create oversize packet, ignoring"
            ),
        );
        return;
    }
    let off = packet.length;
    packet.buf[off] = atype;
    packet.buf[off + 1] = (len + 2) as u8; // total size includes type and length
    packet.buf[off + 2..off + 2 + len].copy_from_slice(data);
    packet.length += len + 2;
}

/// `CheckRADIUSAuth(port)` (`auth.c:2845`).
pub fn CheckRADIUSAuth(port: &Port) -> PgResult<i32> {
    let hba = port.hba.as_ref().expect("CheckRADIUSAuth: port->hba is NULL");

    if hba.radiusservers.is_empty() {
        ereport(LOG).errmsg("RADIUS server not specified").finish(here("CheckRADIUSAuth"))?;
        return Ok(STATUS_ERROR);
    }
    if hba.radiussecrets.is_empty() {
        ereport(LOG).errmsg("RADIUS secret not specified").finish(here("CheckRADIUSAuth"))?;
        return Ok(STATUS_ERROR);
    }

    sendAuthRequest(port, AUTH_REQ_PASSWORD, &[])?;
    let passwd = match recv_password_packet(port)? {
        Some(p) => p,
        None => return Ok(STATUS_EOF),
    };

    if passwd.len() > RADIUS_MAX_PASSWORD_LENGTH {
        ereport(LOG)
            .errmsg(format!(
                "RADIUS authentication does not support passwords longer than {RADIUS_MAX_PASSWORD_LENGTH} characters"
            ))
            .finish(here("CheckRADIUSAuth"))?;
        return Ok(STATUS_ERROR);
    }

    // Per-server iteration; secrets/ports/identifiers lists advance in step,
    // but only when they have more than one element (mirroring the C
    // list_length > 1 guards: a single value applies to every server).
    let user_name = port_user_name(port);
    for (i, server) in hba.radiusservers.iter().enumerate() {
        let secret = list_pick(&hba.radiussecrets, i);
        let portstr = list_pick_opt(&hba.radiusports, i);
        let identifier = list_pick_opt(&hba.radiusidentifiers, i);

        let ret = PerformRadiusTransaction(
            server,
            secret.expect("RADIUS: secrets list is non-empty"),
            portstr,
            identifier,
            &user_name,
            &passwd,
        )?;

        if ret == STATUS_OK {
            set_authn_id(port, &user_name)?;
            return Ok(STATUS_OK);
        } else if ret == STATUS_EOF {
            // Specific reject from this server: stop trying.
            return Ok(STATUS_ERROR);
        }
    }

    Ok(STATUS_ERROR)
}

/// Mirror C's `list_length(list) > 1 ? lnext(...) : same` stepping: a
/// single-element list applies to every server; a multi-element list advances
/// per server (clamped to the last element, matching the C cursor never running
/// past the list end since radiusservers governs the loop count).
fn list_pick(list: &[String], i: usize) -> Option<&str> {
    if list.len() > 1 {
        list.get(i).map(String::as_str)
    } else {
        list.first().map(String::as_str)
    }
}
fn list_pick_opt(list: &[String], i: usize) -> Option<&str> {
    if list.is_empty() {
        None
    } else if list.len() > 1 {
        list.get(i).map(String::as_str)
    } else {
        list.first().map(String::as_str)
    }
}

/// `PerformRadiusTransaction(...)` (`auth.c:2939`).
#[allow(clippy::too_many_arguments)]
fn PerformRadiusTransaction(
    server: &str,
    secret: &str,
    portstr: Option<&str>,
    identifier: Option<&str>,
    user_name: &str,
    passwd: &str,
) -> PgResult<i32> {
    let portstr = portstr.unwrap_or("1812");
    let identifier = identifier.unwrap_or("postgresql");

    let service = RADIUS_AUTHENTICATE_ONLY.to_be_bytes(); // pg_hton32

    let port_num: u16 = portstr.parse().unwrap_or(0); // atoi(portstr)

    // pg_getaddrinfo_all(server, portstr, &hint(SOCK_DGRAM, AF_UNSPEC), ...)
    let mut serveraddrs: Vec<PgAddrInfo> = Vec::new();
    let hint = AddrInfoHint { flags: 0, family: libc::AF_UNSPEC, socktype: libc::SOCK_DGRAM };
    let r = ip::pg_getaddrinfo_all(Some(server), Some(portstr), &hint, &mut serveraddrs);
    if r != 0 || serveraddrs.is_empty() {
        ereport(LOG)
            .errmsg(format!("could not translate RADIUS server name \"{server}\" to address: {}", crate::gai_strerror(r)))
            .finish(here("PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }
    let serveraddr = serveraddrs[0];

    // Construct RADIUS packet.
    let mut packet = RadiusPacket::new();
    packet.set_code(RADIUS_ACCESS_REQUEST);
    packet.length = RADIUS_HEADER_LENGTH;
    if !crate::pg_strong_random(packet.vector_mut()) {
        ereport(LOG)
            .errmsg("could not generate random encryption vector")
            .finish(here("PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }
    let request_authenticator = packet.vector();
    packet.set_id(request_authenticator[0]);
    radius_add_attribute(&mut packet, RADIUS_SERVICE_TYPE, &service);
    radius_add_attribute(&mut packet, RADIUS_USER_NAME, user_name.as_bytes());
    radius_add_attribute(&mut packet, RADIUS_NAS_IDENTIFIER, identifier.as_bytes());

    // RADIUS password encryption (RFC 2865 §5.2):
    //   c(1) = p(1) XOR MD5(secret + Request Authenticator)
    //   c(i) = p(i) XOR MD5(secret + c(i-1))
    let pw_bytes = passwd.as_bytes();
    let encryptedpasswordlen =
        pw_bytes.len().div_ceil(RADIUS_VECTOR_LENGTH) * RADIUS_VECTOR_LENGTH;
    let mut encryptedpassword = vec![0u8; encryptedpasswordlen];

    let mut md5trailer: Vec<u8> = request_authenticator.to_vec();
    let mut i = 0usize;
    while i < encryptedpasswordlen {
        // cryptvector = secret || md5trailer
        let mut cryptvector = Vec::with_capacity(secret.len() + RADIUS_VECTOR_LENGTH);
        cryptvector.extend_from_slice(secret.as_bytes());
        cryptvector.extend_from_slice(&md5trailer);

        let digest = match seams::pg_md5_binary::call(cryptvector)? {
            Ok(d) => d,
            Err(errstr) => {
                ereport(LOG)
                    .errmsg(format!("could not perform MD5 encryption of password: {errstr}"))
                    .finish(here("PerformRadiusTransaction"))?;
                return Ok(STATUS_ERROR);
            }
        };
        encryptedpassword[i..i + RADIUS_VECTOR_LENGTH].copy_from_slice(&digest);

        for j in i..i + RADIUS_VECTOR_LENGTH {
            let p = if j < pw_bytes.len() { pw_bytes[j] } else { 0 };
            encryptedpassword[j] ^= p;
        }
        // Next block's MD5 trailer is this block's ciphertext.
        md5trailer = encryptedpassword[i..i + RADIUS_VECTOR_LENGTH].to_vec();
        i += RADIUS_VECTOR_LENGTH;
    }

    radius_add_attribute(&mut packet, RADIUS_PASSWORD, &encryptedpassword);

    // Finalize header length field (network order).
    let packetlength = packet.length;
    packet.set_length_field(packetlength as u16);

    // UDP socket bound to in6addr_any of the server's family.
    let sock = unsafe { libc::socket(serveraddr.family, libc::SOCK_DGRAM, 0) };
    if sock < 0 {
        ereport(LOG)
            .errmsg(format!("could not create RADIUS socket: {}", std::io::Error::last_os_error()))
            .finish(here("PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }
    let _guard = SockGuard(sock);

    // bind to wildcard local address of the right family.
    let bind_rc = unsafe {
        if serveraddr.family == libc::AF_INET6 {
            let mut la: libc::sockaddr_in6 = std::mem::zeroed();
            la.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            libc::bind(
                sock,
                &la as *const _ as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        } else {
            let mut la: libc::sockaddr_in = std::mem::zeroed();
            la.sin_family = libc::AF_INET as libc::sa_family_t;
            libc::bind(
                sock,
                &la as *const _ as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
    };
    if bind_rc != 0 {
        ereport(LOG)
            .errmsg(format!("could not bind local RADIUS socket: {}", std::io::Error::last_os_error()))
            .finish(here("PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }

    // sendto(serveraddr).
    let sent = unsafe {
        libc::sendto(
            sock,
            packet.buf.as_ptr() as *const libc::c_void,
            packetlength,
            0,
            serveraddr.addr.addr.as_ptr() as *const libc::sockaddr,
            serveraddr.addr.salen as libc::socklen_t,
        )
    };
    if sent < 0 {
        ereport(LOG)
            .errmsg(format!("could not send RADIUS packet: {}", std::io::Error::last_os_error()))
            .finish(here("PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }

    // Absolute deadline (so invalid packets don't reset the timer).
    let endtime = now_micros() + RADIUS_TIMEOUT * 1_000_000;

    loop {
        let now = now_micros();
        let timeoutval = endtime - now;
        if timeoutval <= 0 {
            ereport(LOG)
                .errmsg(format!("timeout waiting for RADIUS response from {server}"))
                .finish(here("PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }

        let mut tv = libc::timeval {
            tv_sec: (timeoutval / 1_000_000) as libc::time_t,
            tv_usec: (timeoutval % 1_000_000) as libc::suseconds_t,
        };
        let mut fdset: libc::fd_set = unsafe { std::mem::zeroed() };
        unsafe {
            libc::FD_ZERO(&mut fdset);
            libc::FD_SET(sock, &mut fdset);
        }

        let r = unsafe {
            libc::select(sock + 1, &mut fdset, std::ptr::null_mut(), std::ptr::null_mut(), &mut tv)
        };
        if r < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            ereport(LOG)
                .errmsg(format!("could not check status on RADIUS socket: {}", std::io::Error::last_os_error()))
                .finish(here("PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }
        if r == 0 {
            ereport(LOG)
                .errmsg(format!("timeout waiting for RADIUS response from {server}"))
                .finish(here("PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }

        let mut receivepacket = RadiusPacket::new();
        let mut remoteaddr: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
        let mut addrsize = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        let received = unsafe {
            libc::recvfrom(
                sock,
                receivepacket.buf.as_mut_ptr() as *mut libc::c_void,
                RADIUS_BUFFER_SIZE,
                0,
                &mut remoteaddr as *mut _ as *mut libc::sockaddr,
                &mut addrsize,
            )
        };
        if received < 0 {
            ereport(LOG)
                .errmsg(format!("could not read RADIUS response: {}", std::io::Error::last_os_error()))
                .finish(here("PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }
        let received = received as usize;

        // Only accept responses from the port we sent to.
        if remoteaddr.sin6_port != port_num.to_be() {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} was sent from incorrect port: {}",
                    u16::from_be(remoteaddr.sin6_port)
                ))
                .finish(here("PerformRadiusTransaction"))?;
            continue;
        }

        if received < RADIUS_HEADER_LENGTH {
            ereport(LOG)
                .errmsg(format!("RADIUS response from {server} too short: {received}"))
                .finish(here("PerformRadiusTransaction"))?;
            continue;
        }
        if received != receivepacket.length_field() as usize {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} has corrupt length: {} (actual length {received})",
                    receivepacket.length_field()
                ))
                .finish(here("PerformRadiusTransaction"))?;
            continue;
        }
        if packet.id() != receivepacket.id() {
            ereport(LOG)
                .errmsg(format!("RADIUS response from {server} is to a different request: {} (should be {})", receivepacket.id(), packet.id()))
                .finish(here("PerformRadiusTransaction"))?;
            continue;
        }

        // Verify the response authenticator:
        //   MD5(Code+ID+Length + RequestAuthenticator + Attributes + Secret)
        let mut cryptvector = Vec::with_capacity(received + secret.len());
        cryptvector.extend_from_slice(&receivepacket.buf[0..4]); // code+id+length
        cryptvector.extend_from_slice(&request_authenticator); // request authenticator
        if received > RADIUS_HEADER_LENGTH {
            cryptvector.extend_from_slice(&receivepacket.buf[RADIUS_HEADER_LENGTH..received]);
        }
        cryptvector.extend_from_slice(secret.as_bytes());

        let digest = match seams::pg_md5_binary::call(cryptvector)? {
            Ok(d) => d,
            Err(errstr) => {
                ereport(LOG)
                    .errmsg(format!("could not perform MD5 encryption of received packet: {errstr}"))
                    .finish(here("PerformRadiusTransaction"))?;
                continue;
            }
        };

        if receivepacket.vector() != digest {
            ereport(LOG)
                .errmsg(format!("RADIUS response from {server} has incorrect MD5 signature"))
                .finish(here("PerformRadiusTransaction"))?;
            continue;
        }

        match receivepacket.code() {
            RADIUS_ACCESS_ACCEPT => return Ok(STATUS_OK),
            RADIUS_ACCESS_REJECT => return Ok(STATUS_EOF),
            other => {
                ereport(LOG)
                    .errmsg(format!(
                        "RADIUS response from {server} has invalid code ({other}) for user \"{user_name}\""
                    ))
                    .finish(here("PerformRadiusTransaction"))?;
                continue;
            }
        }
    }
}

/// `gettimeofday()` as microseconds since the epoch.
fn now_micros() -> i64 {
    let mut tv: libc::timeval = unsafe { std::mem::zeroed() };
    unsafe { libc::gettimeofday(&mut tv, std::ptr::null_mut()) };
    tv.tv_sec as i64 * 1_000_000 + tv.tv_usec as i64
}

/// RAII socket close (`closesocket(sock)`).
struct SockGuard(libc::c_int);
impl Drop for SockGuard {
    fn drop(&mut self) {
        if self.0 >= 0 {
            unsafe { libc::close(self.0) };
            self.0 = -1;
        }
    }
}
