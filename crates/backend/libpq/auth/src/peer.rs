//! `auth_peer` (`auth.c:1855`) — peer authentication using `getpeereid`.

use utils_error::ereport;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, LOG};
use net::Port;

use crate::{here, port_user_name, set_authn_id, STATUS_ERROR};
use crate::seams;

/// `auth_peer(port)` (`auth.c:1856`). Authenticate by the OS uid on the other
/// end of a local socket, then run the ident usermap.
pub fn auth_peer(port: &Port) -> PgResult<i32> {
    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;

    // getpeereid(port->sock, &uid, &gid)
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
    let mut buf = [0i8; 1024];
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
