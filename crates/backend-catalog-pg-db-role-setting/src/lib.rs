#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `types_error::PgResult`
// (== `Result<_, PgError>`), the project-wide error contract; we accept the
// large-`Err` lint crate-wide.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/catalog/pg_db_role_setting.c` — manipulation of the
//! `pg_db_role_setting` relation (per-database / per-role GUC settings).
//!
//! The three public routines [`AlterSetting`], [`DropSetting`] and
//! [`ApplySetting`] carry their full decision logic here. The genuine externals
//! — the relcache-bound `table_open` / `systable_*` / tableam scans over
//! `pg_db_role_setting` keyed by `(setdatabase, setrole)`, the `heap_getattr`
//! decode of the `setconfig text[]` column, `heap_modify_tuple` /
//! `heap_form_tuple` plus `CatalogTupleUpdate` / `CatalogTupleDelete` /
//! `CatalogTupleInsert`, and `GUCArrayReset` / `ProcessGUCArray` — cross owner
//! seams. The relation and scan are owned behind the opaque [`SettingScan`]
//! handle (the relcache is not ported), exactly as the C owns `Relation rel`
//! and `SysScanDesc scan`; the `setconfig` array crosses as its decoded
//! `Vec<String>` form (the repo-wide GUC-array convention).

use mcx::Mcx;
use types_core::primitive::{Oid, OidIsValid};
use types_error::PgResult;
use types_guc::guc::GucSource;
use types_parsenodes::{VariableSetKind, VariableSetStmt};

use backend_catalog_pg_db_role_setting_seams as seam;
use backend_commands_functioncmds_seams as guc;

/* ===========================================================================
 * AlterSetting (pg_db_role_setting.c:23-162)
 * ========================================================================= */

/// `AlterSetting(Oid databaseid, Oid roleid, VariableSetStmt *setstmt)`.
///
/// Adds, updates or removes a `pg_db_role_setting` row for the given
/// `(databaseid, roleid)` according to `setstmt`.
pub fn AlterSetting(
    mcx: Mcx<'_>,
    databaseid: Oid,
    roleid: Oid,
    setstmt: &VariableSetStmt,
) -> PgResult<()> {
    // char *valuestr = ExtractSetVariableArgs(setstmt);
    let valuestr =
        backend_utils_misc_guc_funcs_seams::extract_set_variable_args::call(setstmt.clone())?;

    // The SET subcommand kind + the variable name are read straight from the
    // owned node, as the C reads `setstmt->kind` / `setstmt->name`. A `None`
    // name yields an empty string (matching `setstmt->name == NULL`, which would
    // make the `GUCArray*` ops never match).
    let kind = setstmt.kind;
    let name: &str = setstmt.name.as_deref().unwrap_or("");

    // rel = table_open(DbRoleSettingRelationId, RowExclusiveLock);
    // ScanKeyInit(setdatabase = databaseid, setrole = roleid);
    // scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true, NULL, 2, scankey);
    // tuple = systable_getnext(scan);  -- plus the setconfig heap_getattr decode.
    let lookup = seam::alter_find::call(databaseid, roleid)?;
    let scan = lookup.scan;

    /*
     * There are three cases:
     *
     * - in RESET ALL, request GUC to reset the settings array and update the
     * catalog if there's anything left, delete it otherwise
     *
     * - in other commands, if there's a tuple in pg_db_role_setting, update it;
     * if it ends up empty, delete it
     *
     * - otherwise, insert a new pg_db_role_setting tuple, but only if the
     * command is not RESET
     */
    if kind == VariableSetKind::ResetAll {
        // if (HeapTupleIsValid(tuple))
        if let Some(setconfig) = lookup.tuple {
            // datum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig, ...);
            // new = isnull ? NULL : GUCArrayReset(DatumGetArrayTypeP(datum));
            let new = match setconfig {
                Some(a) => guc::guc_array_reset::call(a)?,
                None => None,
            };

            // if (new) { heap_modify_tuple + CatalogTupleUpdate } else CatalogTupleDelete
            if let Some(new) = new {
                seam::update_setconfig::call(scan, new)?;
            } else {
                seam::delete_found_tuple::call(scan)?;
            }
        }
    } else if let Some(setconfig) = lookup.tuple {
        // a = isnull ? NULL : DatumGetArrayTypeP(datum);
        let a0 = setconfig;

        /* Update (valuestr is NULL in RESET cases) */
        // if (valuestr) a = GUCArrayAdd(a, setstmt->name, valuestr);
        // else          a = GUCArrayDelete(a, setstmt->name);
        let a = match &valuestr {
            Some(v) => Some(guc::guc_array_add::call(a0, name.to_string(), v.clone())?),
            None => guc::guc_array_delete::call(a0, name.to_string())?,
        };

        // if (a) { heap_modify_tuple + CatalogTupleUpdate } else CatalogTupleDelete
        if let Some(a) = a {
            seam::update_setconfig::call(scan, a)?;
        } else {
            seam::delete_found_tuple::call(scan)?;
        }
    } else if let Some(v) = &valuestr {
        /* non-null valuestr means it's not RESET, so insert a new tuple */
        // a = GUCArrayAdd(NULL, setstmt->name, valuestr);
        let a = guc::guc_array_add::call(None, name.to_string(), v.clone())?;
        // values[setdatabase] = databaseid; values[setrole] = roleid;
        // values[setconfig] = a; heap_form_tuple + CatalogTupleInsert.
        seam::insert_setting::call(scan, databaseid, roleid, a)?;
    }

    // InvokeObjectPostAlterHookArg(DbRoleSettingRelationId, databaseid, 0, roleid, false);
    // systable_endscan(scan); table_close(rel, NoLock);
    seam::alter_finish::call(mcx, scan, databaseid, roleid)
}

/* ===========================================================================
 * DropSetting (pg_db_role_setting.c:169-207)
 * ========================================================================= */

/// `DropSetting(Oid databaseid, Oid roleid)`.
///
/// Drop some settings from the catalog — for a particular database, or for a
/// particular role (it is possible to do both, but current uses do not).
pub fn DropSetting(databaseid: Oid, roleid: Oid) -> PgResult<()> {
    /*
     * The C builds `numkeys` scan keys from whichever of `databaseid` /
     * `roleid` is a valid OID (`OidIsValid`), then deletes every matching tuple.
     * That valid-OID key selection is the decision carried here; the keyed
     * `table_beginscan_catalog` + `heap_getnext` loop + per-tuple
     * `CatalogTupleDelete` over the open relation is the genuine external.
     * Current callers always pass at least one valid OID.
     */
    debug_assert!(OidIsValid(databaseid) || OidIsValid(roleid));
    seam::drop_settings::call(OidIsValid(databaseid), databaseid, OidIsValid(roleid), roleid)
}

/* ===========================================================================
 * ApplySetting (pg_db_role_setting.c:219-261)
 * ========================================================================= */

/// `ApplySetting(Snapshot snapshot, Oid databaseid, Oid roleid, Relation
/// relsetting, GucSource source)`.
///
/// Scan `pg_db_role_setting` for applicable settings, and load them on the
/// current process. `relsetting` is `pg_db_role_setting`, already opened and
/// locked; here it and `snapshot` are owned behind the [`SettingScan`] handle.
///
/// Note: only the exact `databaseid` / `roleid` combination is considered; this
/// is normally called more than once, with `InvalidOid` for either `databaseid`
/// or `roleid` — the precedence logic driven by [`process_db_role_settings`].
pub fn ApplySetting(
    relsetting: seam::SettingScan,
    databaseid: Oid,
    roleid: Oid,
    source: GucSource,
) -> PgResult<()> {
    /*
     * The keyed systable scan, the per-tuple `setconfig` `heap_getattr` decode +
     * non-NULL guard, and `ProcessGUCArray(a, PGC_SUSET, source, GUC_ACTION_SET)`
     * for every non-NULL array are performed by the seam (they are all
     * Datum/genam/guc externals); the `if (!isnull)` skip is honored there. We
     * process all options at SUSET level (the right to insert an option was
     * checked at insert time).
     */
    seam::apply_setting::call(relsetting, databaseid, roleid, source)
}

/// `process_settings` (`utils/init/postinit.c:1309-1330`) — load GUC settings
/// from `pg_db_role_setting` for the database/role combination, trying the
/// specific combination, then general for this database and for this user.
///
/// This orchestration is homed here (the `pg_db_role_setting` relation is owned
/// by this unit): `table_open(DbRoleSettingRelationId, AccessShareLock)`,
/// register the catalog snapshot, the four `ApplySetting` calls in scope order
/// (later settings are ignored if set earlier), then unregister + close. The
/// `IsUnderPostmaster` guard is applied by the postinit caller.
///
/// Installed as the `apply_db_role_settings` seam consumed by
/// `process_settings` in `backend-utils-init-postinit`.
pub fn process_db_role_settings(mcx: Mcx<'_>, databaseid: Oid, roleid: Oid) -> PgResult<()> {
    use types_core::primitive::INVALID_OID;
    use types_guc::guc::GucSource::{
        PGC_S_DATABASE, PGC_S_DATABASE_USER, PGC_S_GLOBAL, PGC_S_USER,
    };

    // relsetting = table_open(DbRoleSettingRelationId, AccessShareLock);
    // snapshot = RegisterSnapshot(GetCatalogSnapshot(DbRoleSettingRelationId));
    let relsetting = seam::apply_open::call(mcx)?;

    // Later settings are ignored if set earlier.
    ApplySetting(relsetting, databaseid, roleid, PGC_S_DATABASE_USER)?;
    ApplySetting(relsetting, INVALID_OID, roleid, PGC_S_USER)?;
    ApplySetting(relsetting, databaseid, INVALID_OID, PGC_S_DATABASE)?;
    ApplySetting(relsetting, INVALID_OID, INVALID_OID, PGC_S_GLOBAL)?;

    // UnregisterSnapshot(snapshot); table_close(relsetting, AccessShareLock);
    seam::apply_close::call(relsetting)
}

/// Install this crate's seams. The high-level relation-bound ops are owned by
/// this unit; `apply_db_role_settings` (consumed by postinit's
/// `process_settings`) is wired to [`process_db_role_settings`]. The
/// `SettingScan`-bound ops (`alter_find` / `update_setconfig` /
/// `delete_found_tuple` / `insert_setting` / `alter_finish` / `drop_settings` /
/// `apply_setting` / `apply_open` / `apply_close`) depend on the unported
/// relcache + genam access for `pg_db_role_setting`; they stay declared but
/// unset and panic on call until that access lands (mirror-and-panic).
pub fn init_seams() {
    seam::apply_db_role_settings::set(process_db_role_settings);
}

#[cfg(test)]
mod tests;
