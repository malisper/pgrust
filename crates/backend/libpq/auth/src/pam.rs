//! auth.c PAM arm: CheckPAMAuth + pam_passwd_conv_proc over the dlopened
//! system libpam (pam_ffi). C's static pam_passwd / pam_port_cludge /
//! pam_no_password cludges become thread-locals (backends are threads here).
//! A PgResult error raised inside the conversation (C longjmps out of the
//! PAM library) is saved and re-raised unconditionally once control returns
//! from the pam_* call that drove the conversation — even when the PAM stack
//! reports PAM_SUCCESS — matching C, where the ereport(ERROR) longjmps out of
//! libpam and never lets a successful return code hide the error.

use core::ffi::{c_char, c_int, c_void};
use std::cell::RefCell;
use std::ffi::CString;

use elog::ereport;
use pgsync::Mutex;
use types_error::{PgError, PgResult, LOG, WARNING};
use types_startup::{ctLocal, Port};

use crate::pam_ffi::{
    self, pam_conv, pam_message, pam_response, PAM_CONV, PAM_CONV_ERR, PAM_ERROR_MSG,
    PAM_MAX_NUM_MSG, PAM_PROMPT_ECHO_OFF, PAM_RHOST, PAM_SUCCESS, PAM_TEXT_INFO, PAM_USER,
};
use crate::{loc, sendAuthRequest, set_authn_id, AUTH_REQ_PASSWORD, STATUS_EOF, STATUS_ERROR, STATUS_OK};

const PGSQL_PAM_SERVICE: &str = "postgresql";

// Serializes the entire PAM transaction (pam_start .. pam_end, including the
// blocking client conversation) across the whole process.
//
// Upstream PostgreSQL forks a dedicated backend process per connection, so at
// most one PAM transaction ever executes per address space and libpam's lack
// of a thread-safety guarantee never matters. This port runs backends as
// std::thread threads in a single process (see launch_backend), so without
// serialization two client connections can drive the dlopened, non-reentrant
// system libpam and its site-configured module stack concurrently in one
// address space. libpam and common modules (pam_unix's unix_chkpwd SIGCHLD
// save/restore, pam_ldap/pam_radius global config/session state, non-reentrant
// libc) assume the single-threaded/forked caller every traditional PAM
// consumer provides; interleaved execution yields cross-auth confusion,
// torn/global state, or memory corruption that faults the shared process.
//
// Holding this mutex for the full transaction restores the "one PAM invocation
// per process at a time" invariant that upstream gets for free. The Rust-side
// conversation state (PAM_STATE, appdata_ptr) is already per-thread/per-call;
// this guards the C library and its modules, which are not.
static PAM_LOCK: Mutex<()> = Mutex::new(());

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
    // unwind-ok: c-callback — called from inside libpam; must not unwind
    // across the FFI boundary.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: libpam passes valid msg/resp pointers for num_msg entries.
        unsafe { conv_body(num_msg, msg, resp, appdata_ptr) }
    }));
    r.unwrap_or(PAM_CONV_ERR)
}

// strdup twin over libc::malloc (PAM frees responses with free()).
unsafe fn c_strdup(s: &[u8]) -> *mut c_char {
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
    // Raw password bytes (C char*): no decoding, the client encoding is not
    // known during authentication.
    let mut passwd: Vec<u8> = if appdata_ptr.is_null() {
        // Solaris 2.6 workaround twin: fall back to the thread-local.
        PAM_STATE.with(|s| {
            s.borrow().passwd.as_ref().map(|p| p.to_bytes().to_vec()).unwrap_or_default()
        })
    } else {
        std::ffi::CStr::from_ptr(appdata_ptr as *const c_char)
            .to_bytes()
            .to_vec()
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
                slot.resp = c_strdup(b"");
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

fn clear_state() -> bool {
    PAM_STATE.with(|s| {
        let mut st = s.borrow_mut();
        st.passwd = None;
        st.port = core::ptr::null();
        st.saved_err = None;
        st.no_password
    })
}

// Take any error the conversation captured. C's ereport(ERROR) inside the
// conversation longjmps straight out of libpam, so a captured error must be
// re-raised regardless of the PAM stack's return code — including PAM_SUCCESS.
// Kept separate from clear_state so it can be consulted between pam_* calls
// (before pam_acct_mgmt) without tearing down `port`/`passwd`, which a later
// conversation turn may still need.
fn take_saved_err() -> Option<Box<PgError>> {
    PAM_STATE.with(|s| s.borrow_mut().saved_err.take())
}

// RAII release of the PAM handle allocated by pam_start.
//
// Upstream C's CheckPAMAuth (auth.c:2149) only calls pam_end on the success
// path and leaks `pamh` on every failure/error return, relying on the
// per-connection backend *process* exiting to reclaim libpam's heap state.
// pgrust runs each backend as a std::thread thread in one long-lived process
// (see PAM_LOCK), so that per-failure leak would accumulate for the life of
// the process — a remote unauthenticated attacker can drive it with repeated
// failed logins. This guard makes the release unmissable: pam_end runs exactly
// once on every path that reaches past a successful pam_start — success, every
// error return, and the `?` early returns out of ereport().finish().
struct PamHandle {
    api: &'static pam_ffi::PamApi,
    pamh: *mut pam_ffi::pam_handle_t,
    // Final status handed to pam_end; kept current with the last pam_* call so
    // libpam modules see the actual outcome, matching pam_end(pamh, retval).
    status: c_int,
}

impl PamHandle {
    // Release explicitly and hand back pam_end's own return value so the
    // success path can log / act on a release failure. Disarms Drop so the
    // handle is never released twice.
    fn end(mut self) -> c_int {
        let pamh = core::mem::replace(&mut self.pamh, core::ptr::null_mut());
        // SAFETY: pamh is the live handle from pam_start, released once here;
        // the null we swapped in makes Drop a no-op.
        unsafe { (self.api.pam_end)(pamh, self.status) }
    }
}

impl Drop for PamHandle {
    fn drop(&mut self) {
        if !self.pamh.is_null() {
            // SAFETY: live handle from pam_start not yet released (end() nulls
            // it, so this runs at most once). Drops while PAM_LOCK is still
            // held (declared after `_pam_guard`), keeping pam_end serialized.
            unsafe {
                (self.api.pam_end)(self.pamh, self.status);
            }
        }
    }
}

// C CheckPAMAuth (auth.c:2029). `password` is always "" from dispatch: the
// conversation fetches the real one from the client.
pub(crate) fn CheckPAMAuth(port: &Port, user: &str, password: &[u8]) -> PgResult<i32> {
    // The conversation would read the client's PasswordMessage while the
    // process-global PAM lock is held, letting one idle peer stall every
    // PAM login; fetch it up front instead (same wire exchange).
    let prefetched: Vec<u8>;
    let password: &[u8] = if password.is_empty() {
        sendAuthRequest(port, AUTH_REQ_PASSWORD, &[])?;
        match crate::recv_password_packet(port)? {
            // Client didn't want to send a password: log nothing.
            None => return Ok(STATUS_EOF),
            Some(p) => {
                prefetched = p;
                &prefetched
            }
        }
    } else {
        password
    };
    // Serialize the whole transaction: at most one thread may drive the
    // non-reentrant system libpam / module stack at a time (see PAM_LOCK).
    // Recover from poisoning — the conversation callback catches unwinds, but
    // a panic elsewhere while the lock is held must not wedge all future PAM
    // logins; the guarded data is only (), so there is no torn Rust state.
    let _pam_guard = PAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());

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
    // every pam_* call below; pamh is released via `pamh_guard` (pam_end) on
    // every path once pam_start succeeds.
    let retval = unsafe { (api.pam_start)(service_c.as_ptr(), c"pgsql@".as_ptr(), &conv, &mut pamh) };
    if retval != PAM_SUCCESS {
        ereport(LOG)
            .errmsg(format!(
                "could not create PAM authenticator: {}",
                pam_ffi::pam_strerror_str(api, pamh, retval)
            ))
            .finish(loc(2062, "CheckPAMAuth"))?;
        clear_state();
        // pam_start failed: no handle was allocated, nothing to release.
        return Ok(STATUS_ERROR);
    }

    // pam_start succeeded: from here every exit must release `pamh` exactly
    // once. This guard does so on drop (all error/`?`/return paths) unless the
    // success path consumes it via `.end()`. See PamHandle above.
    let mut pamh_guard = PamHandle { api, pamh, status: retval };

    // SAFETY: pamh is a live handle; user_c is NUL-terminated.
    let retval = unsafe { (api.pam_set_item)(pamh, PAM_USER, user_c.as_ptr() as *const c_void) };
    pamh_guard.status = retval;
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
        pamh_guard.status = retval;
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
    pamh_guard.status = retval;
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
    pamh_guard.status = retval;
    // C's ereport(ERROR) inside the conversation longjmps straight out of
    // libpam, so a captured error must propagate regardless of retval — even
    // when the module stack reports PAM_SUCCESS. Consult saved_err before
    // interpreting the return code so a captured error is never dropped on the
    // success path.
    if let Some(e) = take_saved_err() {
        clear_state();
        return Err(e);
    }
    if retval != PAM_SUCCESS {
        let no_password = clear_state();
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
    pamh_guard.status = retval;
    // Same unconditional re-raise as after pam_authenticate: a conversation
    // driven by pam_acct_mgmt may have captured an ERROR/FATAL that must
    // propagate even if the stack returned PAM_SUCCESS.
    if let Some(e) = take_saved_err() {
        clear_state();
        return Err(e);
    }
    if retval != PAM_SUCCESS {
        let no_password = clear_state();
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

    // Success path: release the handle explicitly (status is pam_acct_mgmt's
    // PAM_SUCCESS retval, matching C's pam_end(pamh, retval)) and consume the
    // guard so it is not released a second time on drop.
    let retval = pamh_guard.end();
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

#[cfg(test)]
mod tests {
    use super::PAM_LOCK;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    // The critical section CheckPAMAuth guards (pam_start .. pam_end) must run
    // one at a time per process. This mirrors that section's use of PAM_LOCK
    // and asserts that no two threads are ever inside it simultaneously.
    #[test]
    fn pam_lock_serializes_transactions() {
        let inside = Arc::new(AtomicBool::new(false));
        let overlaps = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let inside = Arc::clone(&inside);
                let overlaps = Arc::clone(&overlaps);
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        // Same acquisition CheckPAMAuth uses.
                        let _guard = PAM_LOCK.lock().unwrap_or_else(|e| e.into_inner());
                        // If serialization holds, `inside` is false on entry.
                        if inside.swap(true, Ordering::SeqCst) {
                            overlaps.fetch_add(1, Ordering::SeqCst);
                        }
                        // Widen the window to make any overlap observable.
                        std::thread::yield_now();
                        inside.store(false, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            overlaps.load(Ordering::SeqCst),
            0,
            "concurrent PAM transactions overlapped under PAM_LOCK"
        );
    }
}
