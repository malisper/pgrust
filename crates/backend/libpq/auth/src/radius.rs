//! auth.c RADIUS arms: CheckRADIUSAuth + PerformRadiusTransaction (RFC 2865).
//! C implements the protocol by hand over a UDP socket; so does this port.
//! One divergence: the receive wait uses poll(2) instead of select(2) — same
//! EINTR/timeout arms, no FD_SETSIZE ceiling.

use elog::{elog, ereport};
use types_error::{PgResult, LOG, WARNING};
use types_startup::Port;

use crate::{
    loc, recv_password_packet, sendAuthRequest, set_authn_id, AUTH_REQ_PASSWORD, STATUS_EOF,
    STATUS_ERROR, STATUS_OK,
};

const RADIUS_VECTOR_LENGTH: usize = 16;
const RADIUS_HEADER_LENGTH: usize = 20;
const RADIUS_MAX_PASSWORD_LENGTH: usize = 128;
// Maximum size of a RADIUS packet we will create or accept.
const RADIUS_BUFFER_SIZE: usize = 1024;

const RADIUS_ACCESS_ACCEPT: u8 = 2;
const RADIUS_ACCESS_REJECT: u8 = 3;
const RADIUS_ACCESS_REQUEST: u8 = 1;

const RADIUS_USER_NAME: u8 = 1;
const RADIUS_PASSWORD: u8 = 2;
const RADIUS_SERVICE_TYPE: u8 = 6;
const RADIUS_NAS_IDENTIFIER: u8 = 32;

const RADIUS_AUTHENTICATE_ONLY: u32 = 8;

const RADIUS_TIMEOUT: i64 = 3;

struct RadiusPacket {
    buf: [u8; RADIUS_BUFFER_SIZE],
    length: usize,
}

impl RadiusPacket {
    fn new() -> Self {
        Self {
            buf: [0; RADIUS_BUFFER_SIZE],
            length: RADIUS_HEADER_LENGTH,
        }
    }
    fn code(&self) -> u8 {
        self.buf[0]
    }
    fn id(&self) -> u8 {
        self.buf[1]
    }
    fn wire_length(&self) -> u16 {
        u16::from_be_bytes([self.buf[2], self.buf[3]])
    }
    fn vector(&self) -> &[u8] {
        &self.buf[4..4 + RADIUS_VECTOR_LENGTH]
    }
}

// radius_add_attribute (auth.c:3018). The C length check omits the 2-byte
// attribute header exactly as written; kept faithful.
fn radius_add_attribute(packet: &mut RadiusPacket, typ: u8, data: &[u8]) -> PgResult<()> {
    let len = data.len();
    if packet.length + len > RADIUS_BUFFER_SIZE {
        elog(
            WARNING,
            format!(
                "adding attribute code {typ} with length {len} to radius packet would create oversize packet, ignoring"
            ),
        )?;
        return Ok(());
    }
    let at = packet.length;
    packet.buf[at] = typ;
    packet.buf[at + 1] = (len + 2) as u8;
    packet.buf[at + 2..at + 2 + len].copy_from_slice(data);
    packet.length += len + 2;
    Ok(())
}

pub(crate) fn CheckRADIUSAuth(port: &mut Port) -> PgResult<i32> {
    let hba = port
        .hba
        .as_ref()
        .expect("CheckRADIUSAuth: port->hba is NULL")
        .clone();

    if hba.radiusservers.is_empty() {
        ereport(LOG)
            .errmsg("RADIUS server not specified")
            .finish(loc(2859, "CheckRADIUSAuth"))?;
        return Ok(STATUS_ERROR);
    }
    if hba.radiussecrets.is_empty() {
        ereport(LOG)
            .errmsg("RADIUS secret not specified")
            .finish(loc(2866, "CheckRADIUSAuth"))?;
        return Ok(STATUS_ERROR);
    }

    sendAuthRequest(port, AUTH_REQ_PASSWORD, &[])?;
    let Some(passwd) = recv_password_packet(port)? else {
        return Ok(STATUS_EOF); // client wouldn't send password
    };

    if passwd.len() > RADIUS_MAX_PASSWORD_LENGTH {
        ereport(LOG)
            .errmsg(format!(
                "RADIUS authentication does not support passwords longer than {RADIUS_MAX_PASSWORD_LENGTH} characters"
            ))
            .finish(loc(2880, "CheckRADIUSAuth"))?;
        return Ok(STATUS_ERROR);
    }

    let user_name = port.user_name.clone().unwrap_or_default();

    // Loop over and try each server in order. secrets/ports/identifiers have
    // length 1 (same everywhere), the servers' length, or — for ports and
    // identifiers — 0 (use the default); hba parse validated the lengths.
    for (i, server) in hba.radiusservers.iter().enumerate() {
        let pick = |list: &[String]| -> Option<String> {
            match list.len() {
                0 => None,
                1 => Some(list[0].clone()),
                _ => Some(list[i].clone()),
            }
        };
        let secret = pick(&hba.radiussecrets).expect("radiussecrets validated non-empty");
        let radiusport = pick(&hba.radiusports);
        let identifier = pick(&hba.radiusidentifiers);

        let ret = PerformRadiusTransaction(
            server,
            &secret,
            radiusport.as_deref(),
            identifier.as_deref(),
            &user_name,
            &passwd,
        )?;

        // STATUS_OK = Login OK; STATUS_ERROR = Login not OK, but try next
        // server; STATUS_EOF = Login not OK, and don't try next server.
        if ret == STATUS_OK {
            set_authn_id(port, &user_name)?;
            return Ok(STATUS_OK);
        } else if ret == STATUS_EOF {
            return Ok(STATUS_ERROR);
        }
    }

    // No servers left to try, so give up.
    Ok(STATUS_ERROR)
}

#[allow(non_snake_case)]
fn PerformRadiusTransaction(
    server: &str,
    secret: &str,
    portstr: Option<&str>,
    identifier: Option<&str>,
    user_name: &str,
    passwd: &str,
) -> PgResult<i32> {
    use ip::{pg_getaddrinfo_all, AddrInfoHint, PgAddrInfo};

    let portstr = portstr.unwrap_or("1812");
    let identifier = identifier.unwrap_or("postgresql");
    // C atoi: digits prefix, 0 on garbage.
    let port: i32 = portstr
        .trim_start()
        .bytes()
        .take_while(|b| b.is_ascii_digit())
        .fold(0i32, |a, b| a.saturating_mul(10).saturating_add((b - b'0') as i32));

    let hint = AddrInfoHint {
        flags: 0,
        family: ip::sys::AF_UNSPEC,
        socktype: libc::SOCK_DGRAM,
    };
    let mut serveraddrs: Vec<PgAddrInfo> = Vec::new();
    let r = pg_getaddrinfo_all(Some(server), Some(portstr), &hint, &mut serveraddrs);
    if r != 0 || serveraddrs.is_empty() {
        ereport(LOG)
            .errmsg(format!(
                "could not translate RADIUS server name \"{server}\" to address: {}",
                crate::gai_strerror(r)
            ))
            .finish(loc(2986, "PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }
    let serveraddr = serveraddrs[0];

    let mut packet = RadiusPacket::new();
    packet.buf[0] = RADIUS_ACCESS_REQUEST;
    let mut vector = [0u8; RADIUS_VECTOR_LENGTH];
    if !pg_strong_random::pg_strong_random(&mut vector) {
        ereport(LOG)
            .errmsg("could not generate random encryption vector")
            .finish(loc(2999, "PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }
    packet.buf[4..4 + RADIUS_VECTOR_LENGTH].copy_from_slice(&vector);
    packet.buf[1] = vector[0];
    radius_add_attribute(
        &mut packet,
        RADIUS_SERVICE_TYPE,
        &RADIUS_AUTHENTICATE_ONLY.to_be_bytes(),
    )?;
    radius_add_attribute(&mut packet, RADIUS_USER_NAME, user_name.as_bytes())?;
    radius_add_attribute(&mut packet, RADIUS_NAS_IDENTIFIER, identifier.as_bytes())?;

    let encryptedpasswordlen = passwd.len().div_ceil(RADIUS_VECTOR_LENGTH) * RADIUS_VECTOR_LENGTH;
    let mut encryptedpassword = [0u8; RADIUS_MAX_PASSWORD_LENGTH];
    let pwbytes = passwd.as_bytes();
    let mut md5trailer = vector;
    let mut i = 0;
    while i < encryptedpasswordlen {
        let mut cryptvector = Vec::with_capacity(secret.len() + RADIUS_VECTOR_LENGTH);
        cryptvector.extend_from_slice(secret.as_bytes());
        cryptvector.extend_from_slice(&md5trailer);
        let digest = pg_md5::pg_md5_binary(&cryptvector);
        for j in i..i + RADIUS_VECTOR_LENGTH {
            let p = if j < pwbytes.len() { pwbytes[j] } else { 0 };
            encryptedpassword[j] = p ^ digest[j - i];
        }
        md5trailer.copy_from_slice(&encryptedpassword[i..i + RADIUS_VECTOR_LENGTH]);
        i += RADIUS_VECTOR_LENGTH;
    }
    radius_add_attribute(
        &mut packet,
        RADIUS_PASSWORD,
        &encryptedpassword[..encryptedpasswordlen],
    )?;

    let packetlength = packet.length;
    packet.buf[2..4].copy_from_slice(&(packetlength as u16).to_be_bytes());

    // SAFETY: family from the resolver; fd owned by the guard below.
    let sock = unsafe { libc::socket(serveraddr.family, libc::SOCK_DGRAM, 0) };
    if sock < 0 {
        let errnum = elog::errno::current_errno();
        ereport(LOG)
            .with_saved_errno(errnum)
            .errmsg("could not create RADIUS socket: %m")
            .finish(loc(3060, "PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }
    let _guard = crate::SocketGuard(sock);

    // Bind the matching-family wildcard address (C zero-fills a
    // sockaddr_in6 and stamps the server's family on it).
    let mut localaddr: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
    let addrsize;
    #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd"))]
    {
        localaddr.sin6_family = serveraddr.family as u8;
        localaddr.sin6_len = if serveraddr.family == libc::AF_INET6 {
            std::mem::size_of::<libc::sockaddr_in6>() as u8
        } else {
            std::mem::size_of::<libc::sockaddr_in>() as u8
        };
    }
    #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd")))]
    {
        localaddr.sin6_family = serveraddr.family as libc::sa_family_t;
    }
    if serveraddr.family == libc::AF_INET6 {
        addrsize = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
    } else {
        addrsize = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    }
    // SAFETY: localaddr is a zeroed wildcard of the right family/size.
    let rc = unsafe {
        libc::bind(
            sock,
            &localaddr as *const libc::sockaddr_in6 as *const libc::sockaddr,
            addrsize,
        )
    };
    if rc != 0 {
        let errnum = elog::errno::current_errno();
        ereport(LOG)
            .with_saved_errno(errnum)
            .errmsg("could not bind local RADIUS socket: %m")
            .finish(loc(3077, "PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }

    // SAFETY: buf/len describe the constructed packet; addr comes from the
    // resolver with its stored salen.
    let sent = unsafe {
        libc::sendto(
            sock,
            packet.buf.as_ptr().cast(),
            packetlength,
            0,
            serveraddr.addr.addr.as_ptr() as *const libc::sockaddr,
            serveraddr.addr.salen as libc::socklen_t,
        )
    };
    if sent < 0 {
        let errnum = elog::errno::current_errno();
        ereport(LOG)
            .with_saved_errno(errnum)
            .errmsg("could not send RADIUS packet: %m")
            .finish(loc(3086, "PerformRadiusTransaction"))?;
        return Ok(STATUS_ERROR);
    }

    // pg_clock: the DST-visible monotonic source (dst-and-wasm.md #2.2).
    let deadline_ns = pg_clock::mono_ns() + (RADIUS_TIMEOUT as u64) * 1_000_000_000;

    loop {
        let remaining = std::time::Duration::from_nanos(
            deadline_ns.saturating_sub(pg_clock::mono_ns()),
        );
        if remaining.is_zero() {
            ereport(LOG)
                .errmsg(format!("timeout waiting for RADIUS response from {server}"))
                .finish(loc(3125, "PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }

        let mut pfd = libc::pollfd {
            fd: sock,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: pfd describes our owned fd.
        let r = unsafe { libc::poll(&mut pfd, 1, remaining.as_millis().max(1) as i32) };
        if r < 0 {
            let errnum = elog::errno::current_errno();
            if errnum == libc::EINTR {
                continue;
            }
            ereport(LOG)
                .with_saved_errno(errnum)
                .errmsg("could not check status on RADIUS socket: %m")
                .finish(loc(3144, "PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }
        if r == 0 {
            ereport(LOG)
                .errmsg(format!("timeout waiting for RADIUS response from {server}"))
                .finish(loc(3151, "PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }

        let mut receive = RadiusPacket::new();
        let mut remoteaddr: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
        let mut raddrsize = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        // SAFETY: buffer and sockaddr storage sized as passed.
        let packetlength = unsafe {
            libc::recvfrom(
                sock,
                receive.buf.as_mut_ptr().cast(),
                RADIUS_BUFFER_SIZE,
                0,
                &mut remoteaddr as *mut libc::sockaddr_in6 as *mut libc::sockaddr,
                &mut raddrsize,
            )
        };
        if packetlength < 0 {
            let errnum = elog::errno::current_errno();
            ereport(LOG)
                .with_saved_errno(errnum)
                .errmsg("could not read RADIUS response: %m")
                .finish(loc(3173, "PerformRadiusTransaction"))?;
            return Ok(STATUS_ERROR);
        }
        let packetlength = packetlength as usize;

        // C reads sin6_port regardless of family: the port field aliases
        // across sockaddr_in/in6.
        let remote_port = u16::from_be(remoteaddr.sin6_port);
        if remote_port != port as u16 {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} was sent from incorrect port: {remote_port}"
                ))
                .finish(loc(3182, "PerformRadiusTransaction"))?;
            continue;
        }

        if packetlength < RADIUS_HEADER_LENGTH {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} too short: {packetlength}"
                ))
                .finish(loc(3190, "PerformRadiusTransaction"))?;
            continue;
        }

        if packetlength != receive.wire_length() as usize {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} has corrupt length: {} (actual length {packetlength})",
                    receive.wire_length()
                ))
                .finish(loc(3198, "PerformRadiusTransaction"))?;
            continue;
        }

        if packet.id() != receive.id() {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} is to a different request: {} (should be {})",
                    receive.id(),
                    packet.id()
                ))
                .finish(loc(3206, "PerformRadiusTransaction"))?;
            continue;
        }

        let mut cryptvector = Vec::with_capacity(packetlength + secret.len());
        cryptvector.extend_from_slice(&receive.buf[..4]); // code+id+length
        cryptvector.extend_from_slice(packet.vector()); // request authenticator
        if packetlength > RADIUS_HEADER_LENGTH {
            cryptvector.extend_from_slice(&receive.buf[RADIUS_HEADER_LENGTH..packetlength]);
        }
        cryptvector.extend_from_slice(secret.as_bytes());
        let digest = pg_md5::pg_md5_binary(&cryptvector);

        if receive.vector() != digest {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} has incorrect MD5 signature"
                ))
                .finish(loc(3244, "PerformRadiusTransaction"))?;
            continue;
        }

        if receive.code() == RADIUS_ACCESS_ACCEPT {
            return Ok(STATUS_OK);
        } else if receive.code() == RADIUS_ACCESS_REJECT {
            return Ok(STATUS_EOF);
        } else {
            ereport(LOG)
                .errmsg(format!(
                    "RADIUS response from {server} has invalid code ({}) for user \"{user_name}\"",
                    receive.code()
                ))
                .finish(loc(3259, "PerformRadiusTransaction"))?;
            continue;
        }
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod radius_tests {
    use super::*;

    #[test]
    fn attribute_layout_matches_rfc2865() {
        let mut p = RadiusPacket::new();
        radius_add_attribute(&mut p, RADIUS_USER_NAME, b"alice").unwrap();
        assert_eq!(p.length, RADIUS_HEADER_LENGTH + 7);
        assert_eq!(p.buf[RADIUS_HEADER_LENGTH], RADIUS_USER_NAME);
        assert_eq!(p.buf[RADIUS_HEADER_LENGTH + 1], 7);
        assert_eq!(&p.buf[RADIUS_HEADER_LENGTH + 2..RADIUS_HEADER_LENGTH + 7], b"alice");
    }

    #[test]
    fn oversize_attribute_is_skipped() {
        let mut p = RadiusPacket::new();
        let big = vec![0u8; RADIUS_BUFFER_SIZE];
        radius_add_attribute(&mut p, RADIUS_USER_NAME, &big).unwrap();
        assert_eq!(p.length, RADIUS_HEADER_LENGTH);
    }

    // RFC 2865 §5.2 hide operation round-trip: XOR with the same digest
    // chain decrypts.
    #[test]
    fn password_obfuscation_roundtrip() {
        let secret = b"radsecret";
        let vector = [7u8; RADIUS_VECTOR_LENGTH];
        let passwd = b"a-password-longer-than-16-bytes";
        let padded_len = passwd.len().div_ceil(16) * 16;

        let mut enc = vec![0u8; padded_len];
        let mut trailer: Vec<u8> = vector.to_vec();
        let mut i = 0;
        while i < padded_len {
            let mut cv = secret.to_vec();
            cv.extend_from_slice(&trailer);
            let digest = pg_md5::pg_md5_binary(&cv);
            for j in i..i + 16 {
                let p = if j < passwd.len() { passwd[j] } else { 0 };
                enc[j] = p ^ digest[j - i];
            }
            trailer = enc[i..i + 16].to_vec();
            i += 16;
        }

        // Decrypt the way a RADIUS server would.
        let mut dec = vec![0u8; padded_len];
        let mut trailer: Vec<u8> = vector.to_vec();
        let mut i = 0;
        while i < padded_len {
            let mut cv = secret.to_vec();
            cv.extend_from_slice(&trailer);
            let digest = pg_md5::pg_md5_binary(&cv);
            for j in i..i + 16 {
                dec[j] = enc[j] ^ digest[j - i];
            }
            trailer = enc[i..i + 16].to_vec();
            i += 16;
        }
        assert_eq!(&dec[..passwd.len()], passwd);
        assert!(dec[passwd.len()..].iter().all(|&b| b == 0));
    }
}
