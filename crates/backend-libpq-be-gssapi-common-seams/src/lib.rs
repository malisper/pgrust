//! Seam declarations for the `backend-libpq-be-gssapi-common` unit
//! (`libpq/be-gssapi-common.c`).
//!
//! The functions ported in `backend-libpq-be-gssapi-common`
//! (`pg_GSS_error`, `pg_store_delegated_credential`) call into the system
//! MIT/Heimdal GSSAPI library (`gssapi.h` / `libgssapi_krb5`), which is a
//! genuinely-EXTERNAL C library and is NOT linked into this build. Every
//! `gss_*` call and the OS `setenv` are routed through the seams declared
//! here.
//!
//! No Rust crate OWNS these `gss_*` externs — they are the system krb5 stack.
//! Until a krb5 binding layer lands and installs them, a call panics loudly
//! (mirror-PG-and-panic for an absent external dependency). The owning unit's
//! `init_seams()` therefore installs NONE of these; they are deliberately
//! left as loud-panic latent seams.
//!
//! Idiomatic vs the C ABI: the opaque `gss_cred_id_t` handle is a plain `u64`
//! token; `OM_uint32` is `u32`; the `gss_buffer_desc` (`value`/`length`)
//! output of `gss_display_status` is returned as an owned `Vec<u8>` of exactly
//! `length` bytes (the C immediately copies out then `gss_release_buffer`s, so
//! the seam folds the release into the return). The `(major, minor)` status
//! pair is returned as a `(u32, u32)` tuple instead of an out-parameter.

seam_core::seam!(
    /// `OM_uint32 gss_display_status(OM_uint32 *minor_status, OM_uint32 status_value,
    /// int status_type, const gss_OID mech_type, OM_uint32 *message_context,
    /// gss_buffer_t status_string)`.
    ///
    /// Fetch one human-readable fragment for `status_value` of kind
    /// `status_type` (`GSS_C_GSS_CODE` / `GSS_C_MECH_CODE`), advancing the
    /// message context. `mech_type` is always `GSS_C_NO_OID` here.
    ///
    /// Returns `(next_message_context, fragment)` where `fragment` is `Some`
    /// of the released `status_string` bytes (`value[..length]`) iff the call
    /// returned `GSS_S_COMPLETE`, and `None` otherwise (the caller breaks its
    /// loop). The seam also performs the matching `gss_release_buffer`.
    ///
    /// Arguments: `(status_value, status_type, message_context)`.
    pub fn gss_display_status(
        status_value: u32,
        status_type: i32,
        message_context: u32,
    ) -> (u32, Option<Vec<u8>>)
);

seam_core::seam!(
    /// `OM_uint32 gss_store_cred_into(OM_uint32 *minor_status,
    /// gss_cred_id_t input_cred_handle, gss_cred_usage_t cred_usage,
    /// const gss_OID desired_mech, OM_uint32 overwrite_cred,
    /// OM_uint32 default_cred, gss_const_key_value_set_t cred_store,
    /// gss_OID_set *elements_stored, gss_cred_usage_t *cred_usage_stored)`.
    ///
    /// Store `cred` into the credential cache. The C always passes
    /// `GSS_C_INITIATE`, `GSS_C_NULL_OID` (store all), `overwrite = true`,
    /// `default = true`, and the single-element `{"ccache" -> "MEMORY:"}`
    /// credential store, so that fixed call shape is implied by the seam.
    ///
    /// Returns `(major, minor)` status. `major == GSS_S_COMPLETE` (0) on
    /// success. Argument: the `gss_cred_id_t` handle (`u64` token).
    pub fn gss_store_cred_into(cred: u64) -> (u32, u32)
);

seam_core::seam!(
    /// `OM_uint32 gss_release_cred(OM_uint32 *minor_status,
    /// gss_cred_id_t *cred_handle)`.
    ///
    /// Release the credential handle. Returns `(major, minor)` status.
    /// Argument: the `gss_cred_id_t` handle (`u64` token).
    pub fn gss_release_cred(cred: u64) -> (u32, u32)
);

seam_core::seam!(
    /// `int setenv(const char *name, const char *value, int overwrite)`
    /// (POSIX libc) — set the `KRB5CCNAME` process environment variable so
    /// later `gss_acquire_cred` calls find the stored delegated credentials.
    /// Returns `0` on success, `-1` on error.
    pub fn setenv(name: &str, value: &str, overwrite: i32) -> i32
);
