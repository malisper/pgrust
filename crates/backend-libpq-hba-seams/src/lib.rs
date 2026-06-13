//! Seam declarations for the genuinely-unported `libpq/hba.c` authentication
//! file parser, as consumed by `hbafuncs.c`'s SQL views.
//!
//! `hbafuncs.c` (`pg_hba_file_rules` / `pg_ident_file_mappings`) is a thin
//! materialized-SRF wrapper that reads the parsed `HbaLine` / `IdentLine` token
//! lists and formats one tuplestore row per line. Every structure it touches —
//! `open_auth_file`, `tokenize_auth_file`, `parse_hba_line`, `parse_ident_line`,
//! `HbaLine`, `IdentLine`, `AuthToken`, `hba_authname`, `get_hba_options` — is
//! owned by hba.c, which is not ported. The whole view fill therefore crosses
//! as one seam against the prepared `ReturnSetInfo` (`setResult`/`setDesc`),
//! which is what `pg_ls_dir_files` does for the dir walk. The owning unit
//! installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.

use types_error::PgResult;
use types_nodes::funcapi::ReturnSetInfo;

seam_core::seam!(
    /// `fill_hba_view(rsi->setResult, rsi->setDesc)` (hbafuncs.c): read
    /// `pg_hba.conf` and push one row per line into the materialized
    /// tuplestore already prepared by `InitMaterializedSRF`. `Err` carries the
    /// `could not open pg_hba.conf` `ereport(ERROR)` and OOM.
    pub fn fill_hba_view<'mcx>(rsinfo: &mut ReturnSetInfo<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `fill_ident_view(rsi->setResult, rsi->setDesc)` (hbafuncs.c): read
    /// `pg_ident.conf` and push one row per mapping into the prepared
    /// tuplestore. `Err` carries the `could not open pg_ident.conf`
    /// `ereport(ERROR)` and OOM.
    pub fn fill_ident_view<'mcx>(rsinfo: &mut ReturnSetInfo<'mcx>) -> PgResult<()>
);
