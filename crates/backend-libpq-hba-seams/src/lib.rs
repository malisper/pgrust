//! Seam declarations for `libpq/hba.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_error::PgResult;

seam_core::seam!(
    /// `hba_authname(MyClientConnectionInfo.auth_method)` (hba.c): the printable
    /// name of the authentication method that authenticated this connection.
    /// hba.c owns the `UserAuthName[]` table and reads the ambient
    /// `MyClientConnectionInfo.auth_method`. Result copied into `mcx`.
    pub fn hba_authname<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgString<'mcx>>
);
