//! Seam declarations for `libpq/auth.c` — client authentication.
//!
//! Two groups live here:
//!
//!   * The four seams `backend-utils-init-postinit` already consumes
//!     (`client_authentication`, `authentication_timeout`,
//!     `log_connection_authorization`, `client_authn_id`). The owning crate
//!     `backend-libpq-auth` installs these from its `init_seams()`.
//!
//!   * The dependencies `auth.c` reaches that are owned by *unported* units —
//!     `hba.c` (`hba_getauthmethod`, `check_usermap`, `hba_authname_of`),
//!     `crypt.c` (`get_role_password`, `md5_crypt_verify`), the SCRAM/OAuth
//!     SASL mechanisms (`auth-scram.c`/`auth-oauth.c`), `common/md5.c`
//!     (`pg_md5_binary`), the GUC reads (`password_encryption`,
//!     `log_connection_authentication`), TLS (`secure_loaded_verify_locations`),
//!     and the external-library methods (GSS/SSPI/PAM/BSD/LDAP). These are
//!     *consumed* by `backend-libpq-auth` and *installed by their own owners*
//!     when those land; until then a call panics loudly, which is correct.

use ::mcx::{Mcx, PgString};
use ::types_core::UserAuth;
use ::types_error::PgResult;

// ----------------------------------------------------------------------------
// Inward: installed by backend-libpq-auth, consumed by postinit.
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `ClientAuthentication(MyProcPort)` (auth.c): perform the authentication
    /// exchange against the connected client. Will not return on failure (the
    /// C `ereport(FATAL)`), carried on `Err`. Uses the ambient `MyProcPort`
    /// inside the owner, so no port argument crosses.
    pub fn client_authentication() -> PgResult<()>
);

seam_core::seam!(
    /// `AuthenticationTimeout` (auth.c GUC): the seconds a client has to
    /// complete authentication.
    pub fn authentication_timeout() -> i32
);

seam_core::seam!(
    /// `log_connections & LOG_CONNECTION_AUTHORIZATION` (auth.c read of the
    /// `log_connections` GUC): should the authorized-connection line be logged?
    pub fn log_connection_authorization() -> bool
);

seam_core::seam!(
    /// `MyClientConnectionInfo.authn_id` (auth.c): the authenticated identity
    /// string, or `None` when authentication did not establish one. Copied
    /// into `mcx`.
    pub fn client_authn_id<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgString<'mcx>>>
);

// ----------------------------------------------------------------------------
// Outward to unported owner (hba.c). Installed by hba.c when it lands.
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `hba_getauthmethod(port)` (hba.c): resolve the matched `pg_hba.conf`
    /// line and store it in `port->hba`. C passes the live `Port *` straight
    /// down from `ClientAuthentication` (auth.c:390), so the caller's `&mut Port`
    /// is threaded through here rather than re-read from the ambient
    /// `MyProcPort` — the caller already holds the port taken out of the
    /// `MyProcPort` cell, and a re-entrant cell read would observe it as unset.
    pub fn hba_getauthmethod(port: &mut net::Port) -> PgResult<()>
);

seam_core::seam!(
    /// `check_usermap(usermap_name, pg_role, auth_user, case_insensitive)`
    /// (hba.c): map an external identity to a PostgreSQL role per
    /// `pg_ident.conf`. Returns `STATUS_OK`/`STATUS_ERROR`.
    pub fn check_usermap(
        usermap_name: Option<String>,
        pg_role: String,
        auth_user: String,
        case_insensitive: bool,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `hba_authname(method)` (hba.c): the printable name of the given
    /// authentication method (the `UserAuthName[]` table). Result copied into
    /// `mcx`.
    pub fn hba_authname_of<'mcx>(mcx: Mcx<'mcx>, method: UserAuth) -> PgResult<PgString<'mcx>>
);

// ----------------------------------------------------------------------------
// Outward to unported owner (crypt.c). Installed by crypt.c when it lands.
// `plain_crypt_verify` and `get_password_type` are already declared by
// `backend-commands-user-seams`; auth re-uses those rather than redeclaring.
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `get_role_password(role, &logdetail)` (crypt.c): fetch the stored secret
    /// for a role. `Ok((Some(secret), _))` on success; `Ok((None, logdetail))`
    /// when the role has no usable password (the C NULL return plus
    /// `*logdetail`).
    pub fn get_role_password(role: String) -> PgResult<(Option<String>, Option<String>)>
);

seam_core::seam!(
    /// `md5_crypt_verify(role, shadow_pass, client_pass, md5_salt, &logdetail)`
    /// (crypt.c): verify an MD5 challenge response. Returns
    /// `(status, logdetail)`.
    pub fn md5_crypt_verify(
        role: String,
        shadow_pass: String,
        client_pass: String,
        md5_salt: Vec<u8>,
    ) -> PgResult<(i32, Option<String>)>
);

// ----------------------------------------------------------------------------
// Outward to unported owners: SASL mechanisms (auth-scram.c / auth-oauth.c).
// CheckSASLAuth's mechanism vtable (get_mechanisms/init/exchange) crosses as a
// single per-mechanism driver call. Installed by the mechanism owner.
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `CheckSASLAuth(&pg_be_scram_mech, port, shadow_pass, &logdetail)`
    /// (auth-sasl.c + auth-scram.c): run the SCRAM-SHA-256 SASL exchange.
    /// Reads ambient `MyProcPort`. Returns `(status, logdetail)`.
    pub fn check_scram_sasl_auth(
        shadow_pass: Option<String>,
    ) -> PgResult<(i32, Option<String>)>
);

seam_core::seam!(
    /// `CheckSASLAuth(&pg_be_oauth_mech, port, NULL, NULL)` (auth-sasl.c +
    /// auth-oauth.c): run the OAuth bearer SASL exchange. Reads ambient
    /// `MyProcPort`. Returns the status.
    pub fn check_oauth_sasl_auth() -> PgResult<i32>
);

// ----------------------------------------------------------------------------
// Outward to unported owner (common/md5.c). Installed by the md5 owner.
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `pg_md5_binary(buff, len, dest, &errstr)` (common/md5.c): compute the
    /// 16-byte raw MD5 digest of `buff`. `Ok(Ok(digest))` on success;
    /// `Ok(Err(errstr))` with the OpenSSL error string on failure.
    pub fn pg_md5_binary(buff: Vec<u8>) -> PgResult<Result<[u8; 16], String>>
);

// ----------------------------------------------------------------------------
// Outward to unported owners: TLS/cert (be-secure.c) and GUC reads (guc.c).
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `secure_loaded_verify_locations()` (be-secure.c): has a root certificate
    /// store been loaded?
    pub fn secure_loaded_verify_locations() -> bool
);

seam_core::seam!(
    /// `Password_encryption` (guc.c GUC): the password type new passwords are
    /// hashed as (`PASSWORD_TYPE_MD5` or `PASSWORD_TYPE_SCRAM_SHA_256`),
    /// expressed as the `PasswordType` discriminant value.
    pub fn password_encryption() -> i32
);

seam_core::seam!(
    /// `log_connections & LOG_CONNECTION_AUTHENTICATION` (guc.c GUC read):
    /// should the per-method "connection authenticated" line be logged?
    pub fn log_connection_authentication() -> bool
);

// ----------------------------------------------------------------------------
// Outward: the ClientAuthentication_hook plugin point (auth.c global).
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `(*ClientAuthentication_hook)(port, status)` (auth.c): invoke the
    /// optional plugin hook with the resolved status. No-op when no plugin is
    /// loaded. Reads ambient `MyProcPort` inside the owner.
    pub fn client_authentication_hook(status: i32) -> PgResult<()>
);

// ----------------------------------------------------------------------------
// Outward to unported external-library auth methods. Each Assert(false)s in C
// when the library is not compiled in; here a call panics naming the method.
// Installed by the respective external-lib owner when it lands.
// ----------------------------------------------------------------------------

seam_core::seam!(
    /// `pg_GSS_recvauth(port)` / `pg_GSS_checkauth(port)` (auth.c, ENABLE_GSS):
    /// GSSAPI/Kerberos authentication. Reads ambient `MyProcPort`.
    pub fn check_gss_auth() -> PgResult<i32>
);

seam_core::seam!(
    /// `pg_SSPI_recvauth(port)` (auth.c, ENABLE_SSPI): Windows SSPI
    /// authentication. Reads ambient `MyProcPort`.
    pub fn check_sspi_auth() -> PgResult<i32>
);

seam_core::seam!(
    /// `CheckPAMAuth(port, user, "")` (auth.c, USE_PAM): PAM authentication.
    /// Reads ambient `MyProcPort`.
    pub fn check_pam_auth() -> PgResult<i32>
);

seam_core::seam!(
    /// `CheckBSDAuth(port, user)` (auth.c, USE_BSD_AUTH): BSD authentication.
    /// Reads ambient `MyProcPort`.
    pub fn check_bsd_auth() -> PgResult<i32>
);

seam_core::seam!(
    /// `CheckLDAPAuth(port)` (auth.c, USE_LDAP): LDAP authentication. Reads
    /// ambient `MyProcPort`.
    pub fn check_ldap_auth() -> PgResult<i32>
);
