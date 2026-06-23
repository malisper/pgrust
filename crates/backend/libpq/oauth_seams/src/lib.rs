//! Seam declarations for `libpq/auth-oauth.c` as consumed by the
//! `pg_hba.conf` parser (`libpq/hba.c`).
//!
//! `hba.c`'s `parse_hba_line` calls `check_oauth_validator(hbaline, elevel,
//! err_msg)` while finalizing an `oauth` HBA line: it confirms a validator
//! library is configured and permitted. That machinery is owned by
//! `auth-oauth.c`, which is not ported. The call crosses this seam; until the
//! OAuth owner lands and installs it, a call panics loudly (which is correct —
//! it is only reachable for a configured `oauth` line).

use types_error::PgResult;
use net::HbaLine;

seam_core::seam!(
    /// `bool check_oauth_validator(const HbaLine *hbaline, int elevel, char
    /// **err_msg)` (auth-oauth.c): ensure the OAuth validator library named on
    /// the HBA line is set and permitted by `oauth_validator_libraries`.
    /// Returns `(ok, err_msg)`: `ok == false` means the line is rejected, with
    /// the C `*err_msg` carried in the second element.
    pub fn check_oauth_validator(
        hbaline: HbaLine,
        elevel: i32,
    ) -> PgResult<(bool, Option<String>)>
);
