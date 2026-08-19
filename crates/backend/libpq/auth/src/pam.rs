//! auth.c PAM arm: CheckPAMAuth + pam_passwd_conv_proc over the dlopened
//! system libpam (pam_ffi). C's static pam_passwd / pam_port_cludge /
//! pam_no_password cludges become thread-locals (backends are threads here).
//! A PgResult error raised inside the conversation (C longjmps out of the
//! PAM library) is saved and re-raised after pam_authenticate returns.

use core::ffi::{c_char, c_int, c_void};
use std::cell::RefCell;
use std::ffi::CString;

use elog::ereport;
use types_error::{PgError, PgResult, LOG, WARNING};
use types_startup::{ctLocal, Port};

use crate::pam_ffi::{
    self, pam_conv, pam_message, pam_response, PAM_CONV, PAM_CONV_ERR, PAM_ERROR_MSG,
    PAM_MAX_NUM_MSG, PAM_PROMPT_ECHO_OFF, PAM_RHOST, PAM_SUCCESS, PAM_TEXT_INFO, PAM_USER,
};
use crate::{loc, sendAuthRequest, set_authn_id, AUTH_REQ_PASSWORD, STATUS_EOF, STATUS_ERROR, STATUS_OK};

const PGSQL_PAM_SERVICE: &str = "postgresql";

struct PamState {
    // C: pam_passwd (Solaris appdata workaround twin).
    passwd: Option<CString>,
    // C: pam_port_cludge. Set for the duration of CheckPAMAuth only.
    port: *const Port,
    // C: pam_no_password.
    no_password: bool,
    // A PgResult error raised inside the conversation, re-raised by
    // CheckPAMAuth (C's ereport(ERROR) longjmp analog).
    saved_err: Option<Box<PgError>>,
}

thread_local! {
    static PAM_STATE: RefCell<PamState> = const {
        RefCell::new(PamState { passwd: None, port: core::ptr::null(), no_password: false, saved_err: None })
    };
}

// C pam_passwd_conv_proc (auth.c:1928). Called from inside libpam; must not
// unwind across the FFI boundary.
pub(crate) unsafe extern "C" fn pam_passwd_conv_proc(
    num_msg: c_int,
    msg: *mut *const pam_message,
    resp: *mut *mut pam_response,
    appdata_ptr: *mut c_void,
) -> c_int {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: libpam passes valid msg/resp pointers for num_msg entries.
        unsafe { conv_body(num_msg, msg, resp, appdata_ptr) }
    }));
    r.unwrap_or(PAM_CONV_ERR)
}

// strdup twin over libc::malloc (PAM frees responses with free()).
unsafe fn c_strdup(s: &str) -> *mut c_char {
    let p = libc::malloc(s.len() + 1) as *mut u8;
    if p.is_null() {
        return core::ptr::null_mut();
    }
    core::ptr::copy_nonoverlapping(s.as_ptr(), p, s.len());
    *p.add(s.len()) = 0;
    p as *mut c_char
}

// C's `goto fail` arm: free whatever we allocated.
unsafe fn conv_fail(reply: *mut pam_response, num_msg: c_int) -> c_int {
    for i in 0..num_msg as usize {
        libc::free((*reply.add(i)).resp as *mut c_void);
    }
    libc::free(reply as *mut c_void);
    PAM_CONV_ERR
}

unsafe fn conv_body(
    num_msg: c_int,
    msg: *mut *const pam_message,
    resp: *mut *mut pam_response,
    appdata_ptr: *mut c_void,
) -> c_int {
    let mut passwd: String = if appdata_ptr.is_null() {
        // Solaris 2.6 workaround twin: fall back to the thread-local.
        PAM_STATE.with(|s| {
            s.borrow().passwd.as_ref().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default()
        })
    } else {
        std::ffi::CStr::from_ptr(appdata_ptr as *const c_char)
            .to_string_lossy()
            .into_owned()
    };

    *resp = core::ptr::null_mut(); // in case of error exit

    if num_msg <= 0 || num_msg > PAM_MAX_NUM_MSG {
        return PAM_CONV_ERR;
    }

    let reply =
        libc::calloc(num_msg as usize, core::mem::size_of::<pam_response>()) as *mut pam_response;
    if reply.is_null() {
        let _ = ereport(LOG)
            .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
            .errmsg("out of memory")
            .finish(loc(1959, "pam_passwd_conv_proc"));
        return PAM_CONV_ERR;
    }

    for i in 0..num_msg as usize {
        let m: *const pam_message = *msg.add(i);
        let style = (*m).msg_style;
        let slot = &mut *reply.add(i);
        match style {
            s if s == PAM_PROMPT_ECHO_OFF => {
                if passwd.is_empty() {
                    let port: &Port = &*PAM_STATE.with(|s| s.borrow().port);
                    if let Err(e) = sendAuthRequest(port, AUTH_REQ_PASSWORD, &[]) {
                        PAM_STATE.with(|s| s.borrow_mut().saved_err = Some(e));
                        return conv_fail(reply, num_msg);
                    }
                    match crate::recv_password_packet(port) {
                        Err(e) => {
                            PAM_STATE.with(|s| s.borrow_mut().saved_err = Some(e));
                            return conv_fail(reply, num_msg);
                        }
                        Ok(None) => {
                            // Client didn't want to send a password:
                            // intentionally log nothing, here or above.
                            PAM_STATE.with(|s| s.borrow_mut().no_password = true);
                            return conv_fail(reply, num_msg);
                        }
                        Ok(Some(p)) => passwd = p,
                    }
                }
                slot.resp = c_strdup(&passwd);
                if slot.resp.is_null() {
                    return conv_fail(reply, num_msg);
                }
                slot.resp_retcode = PAM_SUCCESS;
            }
            s if s == PAM_ERROR_MSG || s == PAM_TEXT_INFO => {
                if style == PAM_ERROR_MSG {
                    let text = if (*m).msg.is_null() {
                        String::new()
                    } else {
                        std::ffi::CStr::from_ptr((*m).msg).to_string_lossy().into_owned()
                    };
                    let _ = ereport(LOG)
                        .errmsg(format!("error from underlying PAM layer: {text}"))
                        .finish(loc(1994, "pam_passwd_conv_proc"));
                }
                slot.resp = c_strdup("");
                if slot.resp.is_null() {
                    return conv_fail(reply, num_msg);
                }
                slot.resp_retcode = PAM_SUCCESS;
            }
            other => {
                let text = if (*m).msg.is_null() {
                    "(none)".to_string()
                } else {
                    std::ffi::CStr::from_ptr((*m).msg).to_string_lossy().into_owned()
                };
                let _ = ereport(LOG)
                    .errmsg(format!("unsupported PAM conversation {other}/\"{text}\""))
                    .finish(loc(2005, "pam_passwd_conv_proc"));
                return conv_fail(reply, num_msg);
            }
        }
    }

    *resp = reply;
    PAM_SUCCESS
}

fn clear_state() -> (bool, Option<Box<PgError>>) {
    PAM_STATE.with(|s| {
        let mut st = s.borrow_mut();
        st.passwd = None;
        st.port = core::ptr::null();
        let no_password = st.no_password;
        (no_password, st.saved_err.take())
    })
}

// C CheckPAMAuth (auth.c:2029). `password` is always "" from dispatch: the
// conversation fetches the real one from the client.
pub(crate) fn CheckPAMAuth(port: &Port, user: &str, password: &str) -> PgResult<i32> {
    let api = match pam_ffi::try_pam() {
        Ok(api) => api,
        Err(e) => {
            // A USE_PAM C build links libpam at build time; the runtime
            // dlopen miss degrades to the authenticator-creation failure.
            ereport(LOG)
                .errmsg(format!("could not create PAM authenticator: {e}"))
                .finish(loc(2062, "CheckPAMAuth"))?;
            return Ok(STATUS_ERROR);
        }
    };

    let password_c = CString::new(password).unwrap_or_default();
    PAM_STATE.with(|s| {
        let mut st = s.borrow_mut();
        st.passwd = Some(password_c.clone());
        st.port = port;
        st.no_password = false;
        st.saved_err = None;
    });

    let conv = pam_conv {
        conv: pam_passwd_conv_proc,
        appdata_ptr: password_c.as_ptr() as *mut c_void,
    };

    let hba = port.hba.as_ref().expect("CheckPAMAuth: port->hba is NULL");
    let service = match hba.pamservice.as_deref() {
        Some(s) if !s.is_empty() => s,
        _ => PGSQL_PAM_SERVICE,
    };
    let service_c = CString::new(service).unwrap_or_default();
    let user_c = CString::new(user).unwrap_or_default();

    let mut pamh: *mut pam_ffi::pam_handle_t = core::ptr::null_mut();
    // SAFETY: service/user are live NUL-terminated strings; conv outlives
    // every pam_* call below; pamh is released via pam_end on success paths.
    let retval = unsafe { (api.pam_start)(service_c.as_ptr(), c"pgsql@".as_ptr(), &conv, &mut pamh) };
    if retval != PAM_SUCCESS {
        ereport(LOG)
            .errmsg(format!(
                "could not create PAM authenticator: {}",
                pam_ffi::pam_strerror_str(api, pamh, retval)
            ))
            .finish(loc(2062, "CheckPAMAuth"))?;
        clear_state();
        return Ok(STATUS_ERROR);
    }

    // SAFETY: pamh is a live handle; user_c is NUL-terminated.
    let retval = unsafe { (api.pam_set_item)(pamh, PAM_USER, user_c.as_ptr() as *const c_void) };
    if retval != PAM_SUCCESS {
        ereport(LOG)
            .errmsg(format!(
                "pam_set_item(PAM_USER) failed: {}",
                pam_ffi::pam_strerror_str(api, pamh, retval)
            ))
            .finish(loc(2073, "CheckPAMAuth"))?;
        clear_state();
        return Ok(STATUS_ERROR);
    }

    if hba.conntype != ctLocal {
        let flags = if hba.pam_use_hostname {
            0
        } else {
            ip::sys::NI_NUMERICHOST | ip::sys::NI_NUMERICSERV
        };
        let mut hostinfo = String::new();
        let rc = ip::pg_getnameinfo_all(&port.raddr, Some(&mut hostinfo), None, flags);
        if rc != 0 {
            ereport(WARNING)
                .errmsg_internal(format!(
                    "pg_getnameinfo_all() failed: {}",
                    crate::gai_strerror(rc)
                ))
                .finish(loc(2095, "CheckPAMAuth"))?;
            // C returns without pam_end here too.
            clear_state();
            return Ok(STATUS_ERROR);
        }
        let hostinfo_c = CString::new(hostinfo).unwrap_or_default();
        // SAFETY: pamh live; hostinfo_c NUL-terminated.
        let retval =
            unsafe { (api.pam_set_item)(pamh, PAM_RHOST, hostinfo_c.as_ptr() as *const c_void) };
        if retval != PAM_SUCCESS {
            ereport(LOG)
                .errmsg(format!(
                    "pam_set_item(PAM_RHOST) failed: {}",
                    pam_ffi::pam_strerror_str(api, pamh, retval)
                ))
                .finish(loc(2105, "CheckPAMAuth"))?;
            clear_state();
            return Ok(STATUS_ERROR);
        }
    }

    // SAFETY: pamh live; conv outlives the handle's use.
    let retval = unsafe {
        (api.pam_set_item)(pamh, PAM_CONV, &conv as *const pam_conv as *const c_void)
    };
    if retval != PAM_SUCCESS {
        ereport(LOG)
            .errmsg(format!(
                "pam_set_item(PAM_CONV) failed: {}",
                pam_ffi::pam_strerror_str(api, pamh, retval)
            ))
            .finish(loc(2117, "CheckPAMAuth"))?;
        clear_state();
        return Ok(STATUS_ERROR);
    }

    // SAFETY: pamh live; the conversation runs on this thread.
    let retval = unsafe { (api.pam_authenticate)(pamh, 0) };
    if retval != PAM_SUCCESS {
        let (no_password, saved) = clear_state();
        if let Some(e) = saved {
            // C's ereport(ERROR) inside the conversation longjmps out of
            // libpam; the saved-error rendering re-raises it here.
            return Err(e);
        }
        // If pam_passwd_conv_proc saw EOF, don't log anything.
        if !no_password {
            ereport(LOG)
                .errmsg(format!(
                    "pam_authenticate failed: {}",
                    pam_ffi::pam_strerror_str(api, pamh, retval)
                ))
                .finish(loc(2130, "CheckPAMAuth"))?;
        }
        return Ok(if no_password { STATUS_EOF } else { STATUS_ERROR });
    }

    // SAFETY: pamh live.
    let retval = unsafe { (api.pam_acct_mgmt)(pamh, 0) };
    if retval != PAM_SUCCESS {
        let (no_password, saved) = clear_state();
        if let Some(e) = saved {
            return Err(e);
        }
        if !no_password {
            ereport(LOG)
                .errmsg(format!(
                    "pam_acct_mgmt failed: {}",
                    pam_ffi::pam_strerror_str(api, pamh, retval)
                ))
                .finish(loc(2143, "CheckPAMAuth"))?;
        }
        return Ok(if no_password { STATUS_EOF } else { STATUS_ERROR });
    }

    // SAFETY: pamh live; released exactly once here.
    let retval = unsafe { (api.pam_end)(pamh, retval) };
    if retval != PAM_SUCCESS {
        ereport(LOG)
            .errmsg(format!(
                "could not release PAM authenticator: {}",
                pam_ffi::pam_strerror_str(api, core::ptr::null_mut(), retval)
            ))
            .finish(loc(2154, "CheckPAMAuth"))?;
    }

    clear_state();

    if retval == PAM_SUCCESS {
        set_authn_id(port, user)?;
        Ok(STATUS_OK)
    } else {
        Ok(STATUS_ERROR)
    }
}
