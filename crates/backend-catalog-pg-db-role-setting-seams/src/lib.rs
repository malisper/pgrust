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
use types_nodes::nodes::Node;

seam_core::seam!(
    /// `AlterSetting(databaseid, roleid, setstmt)` (pg_db_role_setting.c),
    /// reached from `AlterDatabaseSet` (dbcommands.c) for `ALTER DATABASE name
    /// SET ...`. The `setstmt` is the canonical `VariableSetStmt` parse node (an
    /// arm of [`Node`]); the owning unit converts it to the
    /// `pg_db_role_setting` owner's `types_parsenodes::VariableSetStmt` model
    /// (the two parse-node models — the `'mcx` arena layer and the owned-`String`
    /// layer — meet only here) and runs the catalog read-modify-write. `Err`
    /// carries `AlterSetting`'s GUC-parse / catalog-mutation `ereport(ERROR)`
    /// surface.
    pub fn alter_database_setting<'mcx, 's>(
        mcx: Mcx<'mcx>,
        databaseid: Oid,
        roleid: Oid,
        setstmt: &Node<'s>,
    ) -> PgResult<()>
);

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
