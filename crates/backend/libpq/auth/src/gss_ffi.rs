//! dlopen/dlsym binding to the system GSSAPI library, resolved once per
//! process (pg_locale icu_ffi precedent). macOS: GSS.framework (Heimdal);
//! Linux: MIT libgssapi_krb5 (or Heimdal libgssapi). gss_store_cred_into is
//! an MIT extension Heimdal lacks, so it resolves optionally — it is only
//! reached when gss_accept_delegation is on and the client delegates.

#![allow(non_camel_case_types)]

use core::ffi::{c_char, c_int, c_void};
use std::sync::OnceLock;

pub type OM_uint32 = u32;
pub type gss_ctx_id_t = *mut c_void;
pub type gss_cred_id_t = *mut c_void;
pub type gss_name_t = *mut c_void;
pub type gss_OID = *mut c_void;
pub type gss_OID_set = *mut c_void;
pub type gss_channel_bindings_t = *mut c_void;
pub type gss_cred_usage_t = c_int;

#[repr(C)]
pub struct gss_buffer_desc {
    pub length: usize,
    pub value: *mut c_void,
}

impl gss_buffer_desc {
    pub const fn empty() -> Self {
        Self { length: 0, value: core::ptr::null_mut() }
    }
}

#[repr(C)]
pub struct gss_key_value_element_desc {
    pub key: *const c_char,
    pub value: *const c_char,
}

#[repr(C)]
pub struct gss_key_value_set_desc {
    pub count: OM_uint32,
    pub elements: *mut gss_key_value_element_desc,
}

pub const GSS_S_COMPLETE: OM_uint32 = 0;
pub const GSS_S_CONTINUE_NEEDED: OM_uint32 = 1; // supplementary bit 0
pub const GSS_C_GSS_CODE: c_int = 1;
pub const GSS_C_MECH_CODE: c_int = 2;
pub const GSS_C_DELEG_FLAG: OM_uint32 = 1;
pub const GSS_C_INITIATE: gss_cred_usage_t = 1;
pub const GSS_C_ACCEPT: gss_cred_usage_t = 2;

pub struct GssApi {
    pub gss_accept_sec_context: unsafe extern "C" fn(
        minor_status: *mut OM_uint32,
        context_handle: *mut gss_ctx_id_t,
        acceptor_cred_handle: gss_cred_id_t,
        input_token_buffer: *mut gss_buffer_desc,
        input_chan_bindings: gss_channel_bindings_t,
        src_name: *mut gss_name_t,
        mech_type: *mut gss_OID,
        output_token: *mut gss_buffer_desc,
        ret_flags: *mut OM_uint32,
        time_rec: *mut OM_uint32,
        delegated_cred_handle: *mut gss_cred_id_t,
    ) -> OM_uint32,
    pub gss_display_name: unsafe extern "C" fn(
        minor_status: *mut OM_uint32,
        input_name: gss_name_t,
        output_name_buffer: *mut gss_buffer_desc,
        output_name_type: *mut gss_OID,
    ) -> OM_uint32,
    pub gss_display_status: unsafe extern "C" fn(
        minor_status: *mut OM_uint32,
        status_value: OM_uint32,
        status_type: c_int,
        mech_type: gss_OID,
        message_context: *mut OM_uint32,
        status_string: *mut gss_buffer_desc,
    ) -> OM_uint32,
    pub gss_release_buffer:
        unsafe extern "C" fn(minor_status: *mut OM_uint32, buffer: *mut gss_buffer_desc) -> OM_uint32,
    pub gss_release_cred:
        unsafe extern "C" fn(minor_status: *mut OM_uint32, cred_handle: *mut gss_cred_id_t) -> OM_uint32,
    pub gss_release_name:
        unsafe extern "C" fn(minor_status: *mut OM_uint32, name: *mut gss_name_t) -> OM_uint32,
    pub gss_delete_sec_context: unsafe extern "C" fn(
        minor_status: *mut OM_uint32,
        context_handle: *mut gss_ctx_id_t,
        output_token: *mut gss_buffer_desc,
    ) -> OM_uint32,
    // MIT credential-store extension; None on Heimdal (macOS GSS.framework).
    // Lets the acceptor keytab be selected per credential (thread-safe) via a
    // {"keytab": path} cred store instead of the process-global KRB5_KTNAME
    // environment variable (which is racy in a threaded, single-process
    // server). Falls back to the env variable when this symbol is absent.
    pub gss_acquire_cred_from: Option<
        unsafe extern "C" fn(
            minor_status: *mut OM_uint32,
            desired_name: gss_name_t,
            time_req: OM_uint32,
            desired_mechs: gss_OID_set,
            cred_usage: gss_cred_usage_t,
            cred_store: *const gss_key_value_set_desc,
            output_cred_handle: *mut gss_cred_id_t,
            actual_mechs: *mut gss_OID_set,
            time_rec: *mut OM_uint32,
        ) -> OM_uint32,
    >,
    // MIT credential-store extension; None on Heimdal (macOS GSS.framework).
    pub gss_store_cred_into: Option<
        unsafe extern "C" fn(
            minor_status: *mut OM_uint32,
            input_cred_handle: gss_cred_id_t,
            input_usage: gss_cred_usage_t,
            desired_mech: gss_OID,
            overwrite_cred: OM_uint32,
            default_cred: OM_uint32,
            cred_store: *const gss_key_value_set_desc,
            elements_stored: *mut gss_OID_set,
            cred_usage_stored: *mut gss_cred_usage_t,
        ) -> OM_uint32,
    >,
}

static GSS: OnceLock<Result<&'static GssApi, String>> = OnceLock::new();

/// Non-panicking probe: Err = no loadable GSSAPI library here. The gss auth
/// arm degrades to a clean COMMERROR + auth failure, never a panic on a
/// client-reachable path.
pub fn try_gss() -> Result<&'static GssApi, &'static String> {
    GSS.get_or_init(load).as_ref().copied()
}

fn load() -> Result<&'static GssApi, String> {
    let handle = open_lib()?;
    let api = resolve_all(handle)?;
    Ok(Box::leak(Box::new(api)))
}

#[cfg(target_family = "wasm")]
fn open_lib() -> Result<*mut c_void, String> {
    Err("GSSAPI is not supported on wasm32-wasip1 (no dynamic loading)".to_string())
}

#[cfg(all(not(target_family = "wasm"), target_os = "macos"))]
fn open_lib() -> Result<*mut c_void, String> {
    for name in [
        "/System/Library/Frameworks/GSS.framework/GSS",
        "libgssapi_krb5.dylib",
        "/opt/homebrew/opt/krb5/lib/libgssapi_krb5.dylib",
        "/usr/local/opt/krb5/lib/libgssapi_krb5.dylib",
    ] {
        let cname = format!("{name}\0");
        // SAFETY: cname is NUL-terminated.
        let h = unsafe { libc::dlopen(cname.as_ptr() as *const c_char, libc::RTLD_NOW) };
        if !h.is_null() {
            return Ok(h);
        }
    }
    Err("could not dlopen GSS.framework or libgssapi_krb5".to_string())
}

#[cfg(all(not(target_family = "wasm"), not(target_os = "macos")))]
fn open_lib() -> Result<*mut c_void, String> {
    for name in ["libgssapi_krb5.so.2", "libgssapi_krb5.so", "libgssapi.so.3", "libgssapi.so"] {
        let cname = format!("{name}\0");
        // SAFETY: cname is NUL-terminated.
        let h = unsafe { libc::dlopen(cname.as_ptr() as *const c_char, libc::RTLD_NOW) };
        if !h.is_null() {
            return Ok(h);
        }
    }
    Err("could not dlopen libgssapi_krb5.so(.2) or libgssapi.so(.3)".to_string())
}

fn resolve_all(handle: *mut c_void) -> Result<GssApi, String> {
    use crate::pam_ffi::dlsym as sym;
    macro_rules! resolve {
        ($name:ident) => {{
            let p = sym(handle, stringify!($name));
            if p.is_null() {
                return Err(format!("GSSAPI symbol {} not found", stringify!($name)));
            }
            // SAFETY: transmuting a non-null dlsym result to the C signature
            // declared for this GSSAPI entry point (stable public C API).
            unsafe { core::mem::transmute(p) }
        }};
    }
    macro_rules! resolve_opt {
        ($name:literal) => {{
            let p = sym(handle, $name);
            if p.is_null() {
                None
            } else {
                // SAFETY: as in resolve! above.
                Some(unsafe { core::mem::transmute(p) })
            }
        }};
    }
    let acquire_cred_from = resolve_opt!("gss_acquire_cred_from");
    let store_cred_into = resolve_opt!("gss_store_cred_into");
    Ok(GssApi {
        gss_accept_sec_context: resolve!(gss_accept_sec_context),
        gss_display_name: resolve!(gss_display_name),
        gss_display_status: resolve!(gss_display_status),
        gss_release_buffer: resolve!(gss_release_buffer),
        gss_release_cred: resolve!(gss_release_cred),
        gss_release_name: resolve!(gss_release_name),
        gss_delete_sec_context: resolve!(gss_delete_sec_context),
        gss_acquire_cred_from: acquire_cred_from,
        gss_store_cred_into: store_cred_into,
    })
}
