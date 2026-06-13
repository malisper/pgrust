//! Seam declarations for `libpq/auth.c` — client authentication.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_error::PgResult;

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
