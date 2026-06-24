//! `auth_peer` (`auth.c:1855`) — peer authentication using `getpeereid`.

use ::utils_error::ereport;
use ::types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, LOG};
use ::net::Port;

use crate::{here, port_user_name, set_authn_id, STATUS_ERROR};
use crate::seams;

/// `auth_peer(port)` (`auth.c:1856`). Authenticate by the OS uid on the other
/// end of a local socket, then run the ident usermap.
pub fn auth_peer(port: &Port) -> PgResult<i32> {
    let mut uid: libc::uid_t = 0;
    #[allow(unused_variables)]
    let mut gid: libc::gid_t = 0;

    // C auth.c gates this on the platform credential primitive:
    //   #if defined(SO_PEERCRED)            -> getsockopt(SO_PEERCRED, ucred)   (Linux)
    //   #elif defined(LOCAL_PEERCRED)/HAVE_GETPEEREID -> getpeereid()          (*BSD/macOS)
    // On Linux the Rust libc crate does not expose getpeereid; use SO_PEERCRED.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let rc = {
        let mut peercred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut so_len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let r = unsafe {
            libc::getsockopt(
                port.sock,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut peercred as *mut libc::ucred).cast::<libc::c_void>(),
                &mut so_len,
            )
        };
        if r == 0 && so_len as usize == std::mem::size_of::<libc::ucred>() {
            uid = peercred.uid;
            gid = peercred.gid;
            0
        } else {
            -1
        }
    };

    // getpeereid(port->sock, &uid, &gid)
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let rc = unsafe { libc::getpeereid(port.sock, &mut uid, &mut gid) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOSYS) {
            ereport(LOG)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("peer authentication is not supported on this platform")
                .finish(here("auth_peer"))?;
        } else {
            ereport(LOG)
                .errmsg(format!("could not get peer credentials: {err}"))
                .finish(here("auth_peer"))?;
        }
        return Ok(STATUS_ERROR);
    }

    // getpwuid_r(uid, &pwbuf, buf, sizeof buf, &pw)
    let mut pwbuf: libc::passwd = unsafe { std::mem::zeroed() };
    // NB: getpwuid_r's buffer is `*mut c_char`; c_char is i8 on x86_64 and on
    // aarch64-apple-darwin, but UNSIGNED (u8) on aarch64-linux. Use c_char so
    // the buffer element type matches the FFI pointer on every target.
    let mut buf = [0 as libc::c_char; 1024];
    let mut result: *mut libc::passwd = std::ptr::null_mut();
    let rc = unsafe {
        libc::getpwuid_r(uid, &mut pwbuf, buf.as_mut_ptr(), buf.len(), &mut result)
    };
    if rc != 0 {
        let err = std::io::Error::from_raw_os_error(rc);
        ereport(LOG)
            .errmsg(format!("could not look up local user ID {}: {err}", uid as i64))
            .finish(here("auth_peer"))?;
        return Ok(STATUS_ERROR);
    } else if result.is_null() {
        ereport(LOG)
            .errmsg(format!("local user with ID {} does not exist", uid as i64))
            .finish(here("auth_peer"))?;
        return Ok(STATUS_ERROR);
    }

    // pw->pw_name
    let pw_name = unsafe {
        std::ffi::CStr::from_ptr((*result).pw_name).to_string_lossy().into_owned()
    };

    set_authn_id(port, &pw_name)?;

    // check_usermap(port->hba->usermap, port->user_name,
    //               MyClientConnectionInfo.authn_id, false)
    let authn_id = miscinit::client_connection_info()
        .authn_id
        .unwrap_or_default();
    let usermap = port.hba.as_ref().expect("auth_peer: port->hba is NULL").usermap.clone();
    seams::check_usermap::call(usermap, port_user_name(port), authn_id, false)
}
