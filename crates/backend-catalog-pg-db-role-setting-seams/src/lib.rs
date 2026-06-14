//! Seam declarations for `catalog/pg_db_role_setting.c` — the relcache- and
//! genam-bound operations over the `pg_db_role_setting` relation, owned by this
//! unit but installed only once the relcache + genam access for that relation
//! lands (until then a call panics loudly: mirror-and-panic).
//!
//! The `setconfig text[]` column crosses as its decoded `Vec<String>` form (the
//! repo-wide GUC-array convention shared with `functioncmds`'s `guc_array_*`).
//! The open relation + scan are owned behind the opaque [`SettingScan`] handle
//! (the C `Relation rel` / `SysScanDesc scan`; the relcache is not ported), and
//! the per-`AlterSetting` lookup result crosses as [`AlterLookup`].

#![allow(clippy::result_large_err)]

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_guc::guc::GucSource;

/// Opaque handle for the `pg_db_role_setting` relation opened (and, for
/// `AlterSetting`, the keyed `SysScanDesc` positioned on the matching tuple) by
/// the relcache/genam owner. Mirrors the C `Relation rel` + `SysScanDesc scan`,
/// which this unit holds across the find → mutate → finish sequence; opaque
/// because the relcache is not ported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SettingScan(pub u64);

/// Result of [`alter_find`]: the open relation/scan handle plus the existing
/// row's decoded `setconfig`, if any.
///
/// `tuple` mirrors `tuple = systable_getnext(scan)` followed by the
/// `heap_getattr(... setconfig ...)` decode: `None` when no row matched
/// (`!HeapTupleIsValid`), `Some(None)` when the row exists but `setconfig` is
/// SQL NULL (`isnull`), `Some(Some(arr))` for a non-NULL `text[]`.
#[derive(Clone, Debug)]
pub struct AlterLookup {
    /// The open relation + positioned scan, owned for the rest of `AlterSetting`.
    pub scan: SettingScan,
    /// The matching row's `setconfig`, decoded — see the type doc.
    pub tuple: Option<Option<Vec<String>>>,
}

seam_core::seam!(
    /// `AlterSetting` prologue (pg_db_role_setting.c:32-47, 69-70/112-114):
    /// `table_open(DbRoleSettingRelationId, RowExclusiveLock)`, the two-key
    /// `systable_beginscan` on `DbRoleSettingDatidRolidIndexId`
    /// (`setdatabase = databaseid`, `setrole = roleid`), `systable_getnext`, and
    /// the `heap_getattr` decode of `setconfig`. The open relation + scan stay
    /// owned behind the returned [`SettingScan`] for the matching mutate and the
    /// final [`alter_finish`]. `Err` carries the scan-setup error surface.
    pub fn alter_find(databaseid: Oid, roleid: Oid) -> PgResult<AlterLookup>
);

seam_core::seam!(
    /// `heap_modify_tuple(setconfig := new_array) + CatalogTupleUpdate(rel,
    /// &tuple->t_self, newtuple)` (pg_db_role_setting.c:77-91 / 99-129) for the
    /// row found by [`alter_find`], behind `scan`. `Err` carries the catalog
    /// update / index-maintenance error surface.
    pub fn update_setconfig(scan: SettingScan, new_array: Vec<String>) -> PgResult<()>
);

seam_core::seam!(
    /// `CatalogTupleDelete(rel, &tuple->t_self)` (pg_db_role_setting.c:94 / 132)
    /// for the row found by [`alter_find`], behind `scan`. `Err` carries the
    /// catalog delete error surface.
    pub fn delete_found_tuple(scan: SettingScan) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_form_tuple(values, nulls) + CatalogTupleInsert(rel, newtuple)`
    /// (pg_db_role_setting.c:137-152) — a fresh row with `setdatabase =
    /// databaseid`, `setrole = roleid`, `setconfig = array`, into the relation
    /// opened by [`alter_find`] (behind `scan`). `Err` carries the catalog
    /// insert / index-maintenance error surface.
    pub fn insert_setting(
        scan: SettingScan,
        databaseid: Oid,
        roleid: Oid,
        array: Vec<String>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterSetting` epilogue (pg_db_role_setting.c:155-161):
    /// `InvokeObjectPostAlterHookArg(DbRoleSettingRelationId, databaseid, 0,
    /// roleid, false)`, `systable_endscan(scan)`, and `table_close(rel,
    /// NoLock)` (keep the lock till commit). `Err` carries the post-alter-hook
    /// error surface.
    pub fn alter_finish(
        mcx: Mcx<'_>,
        scan: SettingScan,
        databaseid: Oid,
        roleid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `DropSetting` (pg_db_role_setting.c:178-206): `table_open(...,
    /// RowExclusiveLock)`, the `numkeys` key set selected from the
    /// `has_databaseid` / `has_roleid` flags (`OidIsValid` decided by the
    /// caller), the `table_beginscan_catalog` + `heap_getnext` loop deleting
    /// every matching tuple via `CatalogTupleDelete`, then `table_close(...,
    /// RowExclusiveLock)`. `Err` carries the scan / delete error surface.
    pub fn drop_settings(
        has_databaseid: bool,
        databaseid: Oid,
        has_roleid: bool,
        roleid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ApplySetting` body (pg_db_role_setting.c:227-260): the two-key
    /// `systable_beginscan` on `DbRoleSettingDatidRolidIndexId` over the open
    /// `relsetting` (behind `scan`) under its registered snapshot, then for each
    /// matching tuple the `setconfig` `heap_getattr` decode + non-NULL guard +
    /// `ProcessGUCArray(a, PGC_SUSET, source, GUC_ACTION_SET)`. `Err` carries
    /// `ProcessGUCArray`'s GUC value-parse / permission error surface.
    pub fn apply_setting(
        scan: SettingScan,
        databaseid: Oid,
        roleid: Oid,
        source: GucSource,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `process_settings` prologue (postinit.c:1317-1320):
    /// `table_open(DbRoleSettingRelationId, AccessShareLock)` +
    /// `RegisterSnapshot(GetCatalogSnapshot(DbRoleSettingRelationId))`,
    /// returning the open relation + registered snapshot behind a
    /// [`SettingScan`] for the four `ApplySetting` calls. `Err` carries the
    /// relation-open / snapshot error surface.
    pub fn apply_open(mcx: Mcx<'_>) -> PgResult<SettingScan>
);

seam_core::seam!(
    /// `process_settings` epilogue (postinit.c:1328-1329):
    /// `UnregisterSnapshot(snapshot)` + `table_close(relsetting,
    /// AccessShareLock)` for the handle from [`apply_open`].
    pub fn apply_close(scan: SettingScan) -> PgResult<()>
);

seam_core::seam!(
    /// postinit.c `process_settings`, batched: the [`apply_open`] prologue, the
    /// four `ApplySetting()` calls in scope order (DATABASE_USER, USER,
    /// DATABASE, GLOBAL), and the [`apply_close`] epilogue. Installed by this
    /// unit's `init_seams()` (wired to `process_db_role_settings`), it is the
    /// real entry point consumed by `backend-utils-init-postinit`'s
    /// `process_settings`. `Err` carries `ApplySetting`'s `ereport(ERROR)`
    /// surface (GUC value parsing / permission errors).
    pub fn apply_db_role_settings(
        mcx: Mcx<'_>,
        databaseid: Oid,
        roleid: Oid,
    ) -> PgResult<()>
);
