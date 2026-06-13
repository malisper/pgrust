//! Seam declarations for `catalog/pg_db_role_setting.c` (`ApplySetting`), as
//! orchestrated by postinit.c's `process_settings`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// postinit.c `process_settings`: table_open(DbRoleSettingRelationId,
    /// AccessShareLock) + RegisterSnapshot(GetCatalogSnapshot(...)) + the four
    /// `ApplySetting()` calls in scope order (DATABASE_USER, USER, DATABASE,
    /// GLOBAL) + UnregisterSnapshot + table_close. The relsetting relation,
    /// snapshot, and catalog scan are owned by `pg_db_role_setting.c`; this
    /// batched call applies all matching GUC settings for the
    /// database/role pair. `Err` carries `ApplySetting`'s `ereport(ERROR)`
    /// surface (GUC value parsing/permission errors).
    pub fn apply_db_role_settings(
        mcx: Mcx<'_>,
        databaseid: Oid,
        roleid: Oid,
    ) -> PgResult<()>
);
