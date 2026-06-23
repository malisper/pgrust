//! plancache's reads of assorted backend state: the `GetUserId()` /
//! `row_security` RLS environment (`utils/init/miscinit.c`, `utils/misc/rls.c`)
//! and the `plan_cache_mode` GUC (`utils/misc/guc_tables.c`). The owning units
//! install these; until then a call panics loudly.

use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `GetUserId()`.
    pub fn get_user_id() -> PgResult<Oid>
);

seam_core::seam!(
    /// The `row_security` GUC value.
    pub fn row_security() -> PgResult<bool>
);

seam_core::seam!(
    /// The `plan_cache_mode` GUC value.
    pub fn plan_cache_mode() -> PgResult<i32>
);
