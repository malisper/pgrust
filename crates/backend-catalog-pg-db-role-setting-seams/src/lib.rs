//! Seam declarations for `catalog/pg_db_role_setting.c`.
//!
//! `AlterSetting` / `DropSetting` / `ApplySetting` are ported directly in the
//! owning crate (they scan the catalog through the real genam `systable_*`
//! iterator); the only seam here is the batched `process_settings` entry point
//! `backend-utils-init-postinit` consumes — it can't depend on the owner
//! directly (the owner is above postinit in the catalog/init layering), so the
//! orchestration crosses this seam.

#![allow(clippy::result_large_err)]

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// postinit.c `process_settings`, batched: the
    /// `table_open(DbRoleSettingRelationId, AccessShareLock)` +
    /// `RegisterSnapshot(GetCatalogSnapshot(...))` prologue, the four
    /// `ApplySetting()` calls in scope order (DATABASE_USER, USER, DATABASE,
    /// GLOBAL), and the `UnregisterSnapshot` + `table_close` epilogue. Installed
    /// by this unit's `init_seams()` (wired to `process_db_role_settings`), it is
    /// the real entry point consumed by `backend-utils-init-postinit`'s
    /// `process_settings`. `Err` carries `ApplySetting`'s `ereport(ERROR)`
    /// surface (GUC value parsing / permission errors).
    pub fn apply_db_role_settings(mcx: Mcx<'_>, databaseid: Oid, roleid: Oid) -> PgResult<()>
);
