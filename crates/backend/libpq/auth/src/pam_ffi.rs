//! dlopen/dlsym binding to the system libpam, resolved once per process
//! (pg_locale icu_ffi precedent). No build-time PAM dependency. Constants
//! and struct layouts follow OpenPAM (macOS/BSD) and Linux-PAM; the two
//! agree on everything used here except PAM_CONV_ERR.

#![allow(non_camel_case_types)]

use core::ffi::{c_char, c_int, c_void};
use std::sync::OnceLock;

pub type pam_handle_t = c_void;

#[repr(C)]
pub struct pam_message {
    pub msg_style: c_int,
    pub msg: *const c_char,
}

#[repr(C)]
pub struct pam_response {
    pub resp: *mut c_char,
    pub resp_retcode: c_int,
}

pub type PamConvFn = unsafe extern "C" fn(
    num_msg: c_int,
    msg: *mut *const pam_message,
    resp: *mut *mut pam_response,
    appdata_ptr: *mut c_void,
) -> c_int;

#[repr(C)]
pub struct pam_conv {
    pub conv: PamConvFn,
    pub appdata_ptr: *mut c_void,
}

pub const PAM_SUCCESS: c_int = 0;
pub const PAM_MAX_NUM_MSG: c_int = 32;

// Message styles (identical in OpenPAM and Linux-PAM).
pub const PAM_PROMPT_ECHO_OFF: c_int = 1;
pub const PAM_ERROR_MSG: c_int = 3;
pub const PAM_TEXT_INFO: c_int = 4;

// Item types (identical in OpenPAM and Linux-PAM).
pub const PAM_USER: c_int = 2;
pub const PAM_RHOST: c_int = 4;
pub const PAM_CONV: c_int = 5;

#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd"))]
pub const PAM_CONV_ERR: c_int = 6;
#[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd")))]
pub const PAM_CONV_ERR: c_int = 19;

pub struct PamApi {
    pub pam_start: unsafe extern "C" fn(
        service: *const c_char,
        user: *const c_char,
        conv: *const pam_conv,
        pamh: *mut *mut pam_handle_t,
    ) -> c_int,
    pub pam_set_item:
        unsafe extern "C" fn(pamh: *mut pam_handle_t, item_type: c_int, item: *const c_void) -> c_int,
    pub pam_authenticate: unsafe extern "C" fn(pamh: *mut pam_handle_t, flags: c_int) -> c_int,
    pub pam_acct_mgmt: unsafe extern "C" fn(pamh: *mut pam_handle_t, flags: c_int) -> c_int,
    pub pam_end: unsafe extern "C" fn(pamh: *mut pam_handle_t, status: c_int) -> c_int,
    pub pam_strerror:
        unsafe extern "C" fn(pamh: *mut pam_handle_t, errnum: c_int) -> *const c_char,
}

pub fn pam_strerror_str(api: &PamApi, pamh: *mut pam_handle_t, errnum: c_int) -> String {
    // SAFETY: pam_strerror returns a static NUL-terminated string.
    unsafe {
        let p = (api.pam_strerror)(pamh, errnum);
        if p.is_null() {
            return format!("PAM error {errnum}");
        }
        core::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

static PAM: OnceLock<Result<&'static PamApi, String>> = OnceLock::new();

/// Non-panicking probe: Err = libpam not loadable here. The pam auth arm
/// degrades to a clean LOG + auth failure, never a panic on a
/// client-reachable path.
pub fn try_pam() -> Result<&'static PamApi, &'static String> {
    PAM.get_or_init(load).as_ref().copied()
}

#[cfg(test)]
pub(crate) fn install_mock_for_tests(api: PamApi) -> &'static PamApi {
    PAM.get_or_init(|| Ok(Box::leak(Box::new(api))))
        .as_ref()
        .copied()
        .expect("mock install raced a failed real load")
}

fn load() -> Result<&'static PamApi, String> {
    let handle = open_lib()?;
    let api = resolve_all(handle)?;
    Ok(Box::leak(Box::new(api)))
}

#[cfg(target_family = "wasm")]
fn open_lib() -> Result<*mut c_void, String> {
    Err("PAM is not supported on wasm32-wasip1 (no dynamic loading)".to_string())
}

#[cfg(not(target_family = "wasm"))]
fn open_lib() -> Result<*mut c_void, String> {
    // macOS resolves libpam.dylib from the dyld shared cache; Linux ships
    // a versioned soname.
    #[cfg(target_os = "macos")]
    let names: &[&str] = &["libpam.dylib", "libpam.2.dylib", "libpam.1.dylib"];
    #[cfg(not(target_os = "macos"))]
    let names: &[&str] = &["libpam.so.0", "libpam.so"];
    for name in names {
        let cname = format!("{name}\0");
        // SAFETY: cname is NUL-terminated.
        let h = unsafe { libc::dlopen(cname.as_ptr() as *const c_char, libc::RTLD_NOW) };
        if !h.is_null() {
            return Ok(h);
        }
    }
    Err(format!("could not dlopen {}", names.join(" / ")))
}

// wasm32: unreachable — open_lib never yields a handle (no dlopen on WASI).
#[cfg(target_family = "wasm")]
pub(crate) fn dlsym(_handle: *mut c_void, _name: &str) -> *mut c_void {
    core::ptr::null_mut()
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn dlsym(handle: *mut c_void, name: &str) -> *mut c_void {
    let cname = format!("{name}\0");
    // SAFETY: cname is NUL-terminated; handle is a live dlopen handle.
    unsafe { libc::dlsym(handle, cname.as_ptr() as *const c_char) }
}

fn resolve_all(handle: *mut c_void) -> Result<PamApi, String> {
    macro_rules! resolve {
        ($name:ident) => {{
            let p = dlsym(handle, stringify!($name));
            if p.is_null() {
                return Err(format!("libpam symbol {} not found", stringify!($name)));
            }
            // SAFETY: transmuting a non-null dlsym result to the C signature
            // declared for this PAM entry point (stable public C API).
            unsafe { core::mem::transmute(p) }
        }};
    }
    Ok(PamApi {
        pam_start: resolve!(pam_start),
        pam_set_item: resolve!(pam_set_item),
        pam_authenticate: resolve!(pam_authenticate),
        pam_acct_mgmt: resolve!(pam_acct_mgmt),
        pam_end: resolve!(pam_end),
        pam_strerror: resolve!(pam_strerror),
    })
}
