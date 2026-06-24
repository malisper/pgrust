//! `libpam` FFI bindings + the `CheckPAMAuth` flow (auth.c, USE_PAM).

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

// ---------------------------------------------------------------------------
// PAM constants (`<security/pam_appl.h>` / `<security/_pam_types.h>`).
//
// Item types (PAM_USER/PAM_CONV/PAM_RHOST) and message styles
// (PAM_PROMPT_ECHO_OFF/PAM_ERROR_MSG/PAM_TEXT_INFO) share the same numeric
// values across Linux-PAM and OpenPAM (macOS/BSD). PAM_SUCCESS is 0 everywhere.
// PAM_CONV_ERR differs: Linux-PAM = 19, OpenPAM = 6 — selected by target_os.
// ---------------------------------------------------------------------------

const PAM_SUCCESS: c_int = 0;

const PAM_USER: c_int = 2;
const PAM_CONV: c_int = 5;
const PAM_RHOST: c_int = 4;

const PAM_PROMPT_ECHO_OFF: c_int = 1;
const PAM_ERROR_MSG: c_int = 3;
const PAM_TEXT_INFO: c_int = 4;

const PAM_MAX_NUM_MSG: c_int = 32;

#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd"))]
const PAM_CONV_ERR: c_int = 6;
#[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd")))]
const PAM_CONV_ERR: c_int = 19;

// ---------------------------------------------------------------------------
// PAM structs. Field order matches Linux-PAM and OpenPAM:
//   struct pam_message  { int msg_style; const char *msg; }
//   struct pam_response { char *resp;    int resp_retcode; }
//   struct pam_conv     { int (*conv)(...); void *appdata_ptr; }
// `pam_handle_t` is opaque.
// ---------------------------------------------------------------------------

#[repr(C)]
struct pam_message {
    msg_style: c_int,
    msg: *const c_char,
}

#[repr(C)]
struct pam_response {
    resp: *mut c_char,
    resp_retcode: c_int,
}

type ConvFn = extern "C" fn(
    num_msg: c_int,
    msg: *const *const pam_message,
    resp: *mut *mut pam_response,
    appdata_ptr: *mut c_void,
) -> c_int;

#[repr(C)]
struct pam_conv {
    conv: ConvFn,
    appdata_ptr: *mut c_void,
}

#[allow(non_camel_case_types)]
type pam_handle_t = c_void;

extern "C" {
    fn pam_start(
        service_name: *const c_char,
        user: *const c_char,
        pam_conversation: *const pam_conv,
        pamh: *mut *mut pam_handle_t,
    ) -> c_int;
    fn pam_set_item(pamh: *mut pam_handle_t, item_type: c_int, item: *const c_void) -> c_int;
    fn pam_authenticate(pamh: *mut pam_handle_t, flags: c_int) -> c_int;
    fn pam_acct_mgmt(pamh: *mut pam_handle_t, flags: c_int) -> c_int;
    fn pam_end(pamh: *mut pam_handle_t, pam_status: c_int) -> c_int;
    fn pam_strerror(pamh: *mut pam_handle_t, errnum: c_int) -> *const c_char;
}

/// `PamOutcome` — see crate docs.
pub enum PamOutcome {
    Ok,
    Error,
    Eof,
}

// ---------------------------------------------------------------------------
// Conversation context passed through `appdata_ptr`.
//
// Mirrors the C statics `pam_passwd` (current password to hand to PAM),
// `pam_port_cludge` (here: the `request_password` closure that does
// sendAuthRequest + recv_password_packet), and `pam_no_password` (set when the
// client refused a password).
// ---------------------------------------------------------------------------

struct PamConvCtx<'a> {
    /// Current password (initially the `password` argument; empty in the C
    /// `CheckPAMAuth(port, user, "")` call, which triggers the prompt path).
    passwd: String,
    /// Whether the client refused to send a password (C `pam_no_password`).
    no_password: bool,
    /// Fetch a password from the client: sendAuthRequest(AUTH_REQ_PASSWORD) +
    /// recv_password_packet. `None` = client sent no password (EOF).
    request_password: &'a mut dyn FnMut() -> Option<String>,
    /// Accumulated log message text for any `PAM_ERROR_MSG` seen (the C
    /// `ereport(LOG, errmsg("error from underlying PAM layer: %s"))`).
    error_log: Vec<String>,
}

/// `pam_passwd_conv_proc` (auth.c:1928).
extern "C" fn pam_passwd_conv_proc(
    num_msg: c_int,
    msg: *const *const pam_message,
    resp: *mut *mut pam_response,
    appdata_ptr: *mut c_void,
) -> c_int {
    // SAFETY: appdata_ptr is the `*mut PamConvCtx` we installed in pam_conv.
    let ctx = unsafe { &mut *(appdata_ptr as *mut PamConvCtx) };

    // *resp = NULL in case of error exit.
    unsafe { *resp = ptr::null_mut() };

    if num_msg <= 0 || num_msg > PAM_MAX_NUM_MSG {
        return PAM_CONV_ERR;
    }

    // PAM frees this memory in pam_end(); allocate with libc::calloc so PAM's
    // free() matches (auth.c explicitly avoids palloc here).
    let n = num_msg as usize;
    let reply =
        unsafe { libc::calloc(n, std::mem::size_of::<pam_response>()) as *mut pam_response };
    if reply.is_null() {
        return PAM_CONV_ERR;
    }

    for i in 0..n {
        // msg is an array of `*const pam_message` (Linux-PAM / OpenPAM layout).
        let m = unsafe { &**msg.add(i) };
        let reply_i = unsafe { &mut *reply.add(i) };

        match m.msg_style {
            PAM_PROMPT_ECHO_OFF => {
                if ctx.passwd.is_empty() {
                    // Password wasn't passed to PAM — ask the client.
                    match (ctx.request_password)() {
                        Some(p) => ctx.passwd = p,
                        None => {
                            // Client refused; no logging (matches C).
                            ctx.no_password = true;
                            unsafe { conv_fail(reply, n) };
                            return PAM_CONV_ERR;
                        }
                    }
                }
                match c_strdup(&ctx.passwd) {
                    Some(p) => {
                        reply_i.resp = p;
                        reply_i.resp_retcode = PAM_SUCCESS;
                    }
                    None => {
                        unsafe { conv_fail(reply, n) };
                        return PAM_CONV_ERR;
                    }
                }
            }
            PAM_ERROR_MSG | PAM_TEXT_INFO => {
                if m.msg_style == PAM_ERROR_MSG && !m.msg.is_null() {
                    // C: ereport(LOG, "error from underlying PAM layer: %s").
                    let s = unsafe { CStr::from_ptr(m.msg) }.to_string_lossy().into_owned();
                    ctx.error_log.push(s);
                }
                // We don't bother logging TEXT_INFO messages.
                match c_strdup("") {
                    Some(p) => {
                        reply_i.resp = p;
                        reply_i.resp_retcode = PAM_SUCCESS;
                    }
                    None => {
                        unsafe { conv_fail(reply, n) };
                        return PAM_CONV_ERR;
                    }
                }
            }
            _ => {
                // Unsupported conversation style — recorded for the caller's log.
                let style = m.msg_style;
                let text = if m.msg.is_null() {
                    "(none)".to_string()
                } else {
                    unsafe { CStr::from_ptr(m.msg) }.to_string_lossy().into_owned()
                };
                ctx.error_log.push(format!("unsupported PAM conversation {style}/\"{text}\""));
                unsafe { conv_fail(reply, n) };
                return PAM_CONV_ERR;
            }
        }
    }

    unsafe { *resp = reply };
    PAM_SUCCESS
}

/// `fail:` cleanup in pam_passwd_conv_proc (auth.c): free all `num_msg`
/// response slots (calloc-zeroed slots have NULL `resp`, freed harmlessly),
/// then the array — matching C's `for (i=0;i<num_msg;i++) free(reply[i].resp)`.
unsafe fn conv_fail(reply: *mut pam_response, num_msg: usize) {
    for j in 0..num_msg {
        let r = &mut *reply.add(j);
        if !r.resp.is_null() {
            libc::free(r.resp as *mut c_void);
        }
    }
    libc::free(reply as *mut c_void);
}

/// `strdup` via libc so PAM's `free()` is the matching deallocator.
fn c_strdup(s: &str) -> Option<*mut c_char> {
    let c = CString::new(s).ok()?;
    let bytes = c.as_bytes_with_nul();
    // SAFETY: allocate len bytes and copy the NUL-terminated string.
    unsafe {
        let p = libc::malloc(bytes.len()) as *mut c_char;
        if p.is_null() {
            return None;
        }
        ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, bytes.len());
        Some(p)
    }
}

fn strerror(pamh: *mut pam_handle_t, code: c_int) -> String {
    // SAFETY: pam_strerror returns a static/owned-by-PAM C string (or NULL).
    unsafe {
        let p = pam_strerror(pamh, code);
        if p.is_null() {
            return String::new();
        }
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

/// `CheckPAMAuth(port, user, password)` (auth.c:2029). Returns the outcome plus
/// any LOG lines the caller should emit (each is an `ereport(LOG)` in C).
///
/// `request_password` performs `sendAuthRequest(AUTH_REQ_PASSWORD)` +
/// `recv_password_packet` and is invoked at most once, from inside the PAM
/// conversation, only when `initial_password` is empty (always, for the
/// server-driven `CheckPAMAuth(port, user, "")` call site).
pub fn check_pam_auth(
    service: &str,
    user: &str,
    rhost: Option<&str>,
    initial_password: &str,
    request_password: &mut dyn FnMut() -> Option<String>,
) -> Result<(PamOutcome, Vec<String>), String> {
    let mut logs: Vec<String> = Vec::new();

    let mut ctx = PamConvCtx {
        passwd: initial_password.to_string(),
        no_password: false,
        request_password,
        error_log: Vec::new(),
    };

    let conv = pam_conv {
        conv: pam_passwd_conv_proc,
        appdata_ptr: &mut ctx as *mut PamConvCtx as *mut c_void,
    };

    let c_service = CString::new(service).map_err(|_| "invalid PAM service name".to_string())?;
    let c_user = CString::new(user).map_err(|_| "invalid PAM user name".to_string())?;

    let mut pamh: *mut pam_handle_t = ptr::null_mut();

    // pam_start(service, "pgsql@", &conv, &pamh)
    let retval = unsafe {
        pam_start(c_service.as_ptr(), c_pgsql_at().as_ptr(), &conv, &mut pamh)
    };
    if retval != PAM_SUCCESS {
        logs.push(format!("could not create PAM authenticator: {}", strerror(pamh, retval)));
        return Ok((PamOutcome::Error, logs));
    }

    macro_rules! fail_error {
        ($msg:expr) => {{
            logs.push($msg);
            unsafe { pam_end(pamh, retval) };
            return Ok((PamOutcome::Error, logs));
        }};
    }

    let retval = unsafe { pam_set_item(pamh, PAM_USER, c_user.as_ptr() as *const c_void) };
    if retval != PAM_SUCCESS {
        fail_error!(format!("pam_set_item(PAM_USER) failed: {}", strerror(pamh, retval)));
    }

    if let Some(host) = rhost {
        let c_host = CString::new(host).map_err(|_| "invalid PAM rhost".to_string())?;
        let retval = unsafe { pam_set_item(pamh, PAM_RHOST, c_host.as_ptr() as *const c_void) };
        if retval != PAM_SUCCESS {
            fail_error!(format!("pam_set_item(PAM_RHOST) failed: {}", strerror(pamh, retval)));
        }
    }

    let retval = unsafe { pam_set_item(pamh, PAM_CONV, &conv as *const pam_conv as *const c_void) };
    if retval != PAM_SUCCESS {
        fail_error!(format!("pam_set_item(PAM_CONV) failed: {}", strerror(pamh, retval)));
    }

    let retval = unsafe { pam_authenticate(pamh, 0) };
    // Drain any conversation error logs accumulated so far.
    logs.append(&mut ctx.error_log);
    if retval != PAM_SUCCESS {
        if !ctx.no_password {
            logs.push(format!("pam_authenticate failed: {}", strerror(pamh, retval)));
        }
        let outcome = if ctx.no_password { PamOutcome::Eof } else { PamOutcome::Error };
        unsafe { pam_end(pamh, retval) };
        return Ok((outcome, logs));
    }

    let retval = unsafe { pam_acct_mgmt(pamh, 0) };
    logs.append(&mut ctx.error_log);
    if retval != PAM_SUCCESS {
        if !ctx.no_password {
            logs.push(format!("pam_acct_mgmt failed: {}", strerror(pamh, retval)));
        }
        let outcome = if ctx.no_password { PamOutcome::Eof } else { PamOutcome::Error };
        unsafe { pam_end(pamh, retval) };
        return Ok((outcome, logs));
    }

    let end_rc = unsafe { pam_end(pamh, retval) };
    if end_rc != PAM_SUCCESS {
        logs.push(format!("could not release PAM authenticator: {}", strerror(ptr::null_mut(), end_rc)));
    }

    // C returns STATUS_OK iff the final pam_end retval (== PAM_SUCCESS from
    // pam_acct_mgmt) is PAM_SUCCESS.
    if end_rc == PAM_SUCCESS {
        Ok((PamOutcome::Ok, logs))
    } else {
        Ok((PamOutcome::Error, logs))
    }
}

/// The constant `"pgsql@"` user-prompt string C passes as pam_start's 2nd arg.
fn c_pgsql_at() -> CString {
    CString::new("pgsql@").unwrap()
}
