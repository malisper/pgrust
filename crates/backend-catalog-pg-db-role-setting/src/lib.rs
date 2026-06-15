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
//! [`ApplySetting`] carry their full decision logic here, scanning the catalog
//! through the real `systable_*` genam iterator (the same boundary
//! `pg_depend` uses). The genuine cross-owner externals are:
//!
//! * `table_open` / `table_close` — the table-AM relation open (`OpenRelation`
//!   guard), already ported.
//! * `ScanKeyInit` + the `systable_beginscan` / `systable_getnext` /
//!   `systable_endscan` genam iterator over the real
//!   [`SysScanDescData`][types_scan::genam::SysScanDescData] — installed by the
//!   `backend-access-index-genam` owner.
//! * the `setconfig text[]` `heap_getattr` decode and the
//!   `heap_modify_tuple` / `heap_form_tuple` + `CatalogTuple{Update,Insert,
//!   Delete}` catalog mutators — owned by the indexing/catalog-form layer
//!   (`backend-catalog-indexing-seams`); they decode/form the `setconfig`
//!   `text[]` (carried as the repo-wide `Vec<String>` GUC-array form) and panic
//!   until that owner lands (the mirror-and-panic frontier).
//! * `GUCArrayReset`/`GUCArrayAdd`/`GUCArrayDelete`/`ProcessGUCArray` — guc.c.
//!
//! `DropSetting`'s `table_beginscan_catalog` + `heap_getnext` loop is the
//! genam heap-scan path: `systable_beginscan(rel, InvalidOid, index_ok = false,
//! NULL, keys)` forces the catalog heap scan (`table_beginscan_strat` with
//! `allow_sync = false`), exactly what `table_beginscan_catalog` does.

use mcx::{Mcx, MemoryContext};
use types_catalog::catalog::{DB_ROLE_SETTING_DATID_ROLID_INDEX_ID, DB_ROLE_SETTING_RELATION_ID};
use types_catalog::pg_db_role_setting::{
    Anum_pg_db_role_setting_setdatabase, Anum_pg_db_role_setting_setrole,
};
use types_core::fmgr::F_OIDEQ;
use types_core::primitive::{AttrNumber, Oid, OidIsValid};
use types_error::PgResult;
use types_guc::guc::GucSource;
use types_parsenodes::{VariableSetKind, VariableSetStmt};
use types_rel::Relation;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::SnapshotData;
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table as table;
use backend_catalog_indexing_seams as indexing;
use backend_catalog_objectaccess_seams as objectaccess;
use backend_utils_misc_guc_funcs_seams as guc_funcs;
use backend_utils_misc_guc_seams as guc;
use backend_utils_time_snapmgr_seams as snapmgr;

/// `table_open(DbRoleSettingRelationId, lockmode)`. The opened relation crosses
/// as the `OpenRelation` guard; its `Drop` is the error-path `table_close`, the
/// success path closes explicitly (`table_close(rel, lockmode)`).
fn open_db_role_setting(mcx: Mcx<'_>, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    table::table_open(mcx, DB_ROLE_SETTING_RELATION_ID, lockmode)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`. The eager fmgr resolution crosses the fmgr seam
/// (panics until fmgr lands, exactly where C does the lookup).
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

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
    let valuestr = guc_funcs::extract_set_variable_args::call(setstmt.clone())?;

    /* Get the old tuple, if any. */

    // rel = table_open(DbRoleSettingRelationId, RowExclusiveLock);
    let rel = open_db_role_setting(mcx, RowExclusiveLock)?;

    // ScanKeyInit(setdatabase = databaseid); ScanKeyInit(setrole = roleid);
    let scankey = [
        oid_key(Anum_pg_db_role_setting_setdatabase, databaseid)?,
        oid_key(Anum_pg_db_role_setting_setrole, roleid)?,
    ];

    // scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true,
    //                           NULL, 2, scankey);
    let mut scan = genam::systable_beginscan::call(
        &rel,
        DB_ROLE_SETTING_DATID_ROLID_INDEX_ID,
        true,
        None,
        &scankey,
    )?;

    // tuple = systable_getnext(scan);
    let row_mcx = MemoryContext::new("AlterSetting row");
    let tuple = genam::systable_getnext::call(row_mcx.mcx(), scan.desc_mut())?;

    // The found row's heap TID (`tuple->t_self`, for the update/delete legs) and
    // its decoded setconfig: `None` row == `!HeapTupleIsValid`; the inner
    // `Option<Vec<String>>` mirrors the `heap_getattr(setconfig) + isnull`
    // decode (`None` == SQL NULL setconfig).
    let found: Option<(ItemPointerData, Option<Vec<String>>)> = match &tuple {
        Some(tup) => {
            let setconfig = indexing::decode_db_role_setting_setconfig::call(&rel, tup)?;
            Some((tup.tuple.t_self, setconfig))
        }
        None => None,
    };

    let name: &str = setstmt.name.as_deref().unwrap_or("");

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
    if setstmt.kind == VariableSetKind::ResetAll {
        // if (HeapTupleIsValid(tuple))
        if let Some((tid, setconfig)) = found {
            // datum = heap_getattr(... setconfig ...); new = isnull ? NULL :
            //   GUCArrayReset(DatumGetArrayTypeP(datum));
            let new = match setconfig {
                Some(a) => guc::guc_array_reset::call(a)?,
                None => None,
            };

            // if (new) { heap_modify_tuple + CatalogTupleUpdate } else
            //   CatalogTupleDelete
            if let Some(new) = new {
                indexing::catalog_tuple_update_pg_db_role_setting::call(&rel, tid, new)?;
            } else {
                indexing::catalog_tuple_delete::call(&rel, tid)?;
            }
        }
    } else if let Some((tid, setconfig)) = found {
        // a = isnull ? NULL : DatumGetArrayTypeP(datum);
        let a0 = setconfig;

        /* Update (valuestr is NULL in RESET cases) */
        // if (valuestr) a = GUCArrayAdd(a, setstmt->name, valuestr);
        // else          a = GUCArrayDelete(a, setstmt->name);
        let a = match &valuestr {
            Some(v) => Some(guc::guc_array_add::call(a0, name.to_string(), v.clone())?),
            None => guc::guc_array_delete::call(a0, name.to_string())?,
        };

        // if (a) { heap_modify_tuple + CatalogTupleUpdate } else
        //   CatalogTupleDelete
        if let Some(a) = a {
            indexing::catalog_tuple_update_pg_db_role_setting::call(&rel, tid, a)?;
        } else {
            indexing::catalog_tuple_delete::call(&rel, tid)?;
        }
    } else if let Some(v) = &valuestr {
        /* non-null valuestr means it's not RESET, so insert a new tuple */
        // a = GUCArrayAdd(NULL, setstmt->name, valuestr);
        let a = guc::guc_array_add::call(None, name.to_string(), v.clone())?;
        // values[setdatabase] = databaseid; values[setrole] = roleid;
        // values[setconfig] = a; heap_form_tuple + CatalogTupleInsert.
        indexing::catalog_tuple_insert_pg_db_role_setting::call(&rel, databaseid, roleid, a)?;
    }

    // InvokeObjectPostAlterHookArg(DbRoleSettingRelationId, databaseid, 0,
    //                              roleid, false);
    if objectaccess::object_access_hook_present::call() {
        objectaccess::invoke_object_post_alter_hook_arg::call(
            DB_ROLE_SETTING_RELATION_ID,
            databaseid,
            0,
            roleid,
            false,
        )?;
    }

    // systable_endscan(scan);
    scan.end()?;

    /* Close pg_db_role_setting, but keep lock till commit */
    // table_close(rel, NoLock);
    rel.close(NoLock)
}

/* ===========================================================================
 * DropSetting (pg_db_role_setting.c:169-207)
 * ========================================================================= */

/// `DropSetting(Oid databaseid, Oid roleid)`.
///
/// Drop some settings from the catalog — for a particular database, or for a
/// particular role.  (It is of course possible to do both too, but it doesn't
/// make sense for current uses.)
pub fn DropSetting(mcx: Mcx<'_>, databaseid: Oid, roleid: Oid) -> PgResult<()> {
    // relsetting = table_open(DbRoleSettingRelationId, RowExclusiveLock);
    let relsetting = open_db_role_setting(mcx, RowExclusiveLock)?;

    // Build `numkeys` keys from whichever OID is valid, in C order.
    let mut keys: Vec<ScanKeyData<'_>> = Vec::new();
    if OidIsValid(databaseid) {
        keys.push(oid_key(Anum_pg_db_role_setting_setdatabase, databaseid)?);
    }
    if OidIsValid(roleid) {
        keys.push(oid_key(Anum_pg_db_role_setting_setrole, roleid)?);
    }

    /*
     * scan = table_beginscan_catalog(relsetting, numkeys, keys);
     *
     * The catalog heap scan: `systable_beginscan` with `index_ok = false`
     * opens no index and runs `table_beginscan_strat(..., allow_sync = false)`,
     * which is exactly what `table_beginscan_catalog` does. (The index OID is
     * unused on this path; pass the relation's index id for documentation.)
     */
    let mut scan = genam::systable_beginscan::call(
        &relsetting,
        DB_ROLE_SETTING_DATID_ROLID_INDEX_ID,
        false,
        None,
        &keys,
    )?;

    // while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    //   CatalogTupleDelete(relsetting, &tup->t_self);
    loop {
        let row_mcx = MemoryContext::new("DropSetting row");
        let Some(tup) = genam::systable_getnext::call(row_mcx.mcx(), scan.desc_mut())? else {
            break;
        };
        indexing::catalog_tuple_delete::call(&relsetting, tup.tuple.t_self)?;
    }

    // table_endscan(scan);
    scan.end()?;

    // table_close(relsetting, RowExclusiveLock);
    relsetting.close(RowExclusiveLock)
}

/* ===========================================================================
 * ApplySetting (pg_db_role_setting.c:219-261)
 * ========================================================================= */

/// `ApplySetting(Snapshot snapshot, Oid databaseid, Oid roleid, Relation
/// relsetting, GucSource source)`.
///
/// Scan `pg_db_role_setting` for applicable settings, and load them on the
/// current process. `relsetting` is `pg_db_role_setting`, already opened and
/// locked.
///
/// Note: only the exact `databaseid` / `roleid` combination is considered; this
/// is normally called more than once, with `InvalidOid` for either `databaseid`
/// or `roleid` — the precedence logic driven by [`process_db_role_settings`].
pub fn ApplySetting(
    snapshot: &SnapshotData,
    databaseid: Oid,
    roleid: Oid,
    relsetting: &Relation<'_>,
    source: GucSource,
) -> PgResult<()> {
    // ScanKeyInit(setdatabase = databaseid); ScanKeyInit(setrole = roleid);
    let keys = [
        oid_key(Anum_pg_db_role_setting_setdatabase, databaseid)?,
        oid_key(Anum_pg_db_role_setting_setrole, roleid)?,
    ];

    // scan = systable_beginscan(relsetting, DbRoleSettingDatidRolidIndexId,
    //                           true, snapshot, 2, keys);
    let mut scan = genam::systable_beginscan::call(
        relsetting,
        DB_ROLE_SETTING_DATID_ROLID_INDEX_ID,
        true,
        Some(snapshot),
        &keys,
    )?;

    // while (HeapTupleIsValid(tup = systable_getnext(scan)))
    loop {
        let row_mcx = MemoryContext::new("ApplySetting row");
        let Some(tup) = genam::systable_getnext::call(row_mcx.mcx(), scan.desc_mut())? else {
            break;
        };

        // datum = heap_getattr(... setconfig ...); if (!isnull) { a =
        //   DatumGetArrayTypeP(datum); ProcessGUCArray(a, ...); }
        if let Some(a) = indexing::decode_db_role_setting_setconfig::call(relsetting, &tup)? {
            /*
             * We process all the options at SUSET level.  We assume that the
             * right to insert an option into pg_db_role_setting was checked
             * when it was inserted.
             */
            // ProcessGUCArray(a, PGC_SUSET, source, GUC_ACTION_SET);
            guc::process_guc_array::call(a, source)?;
        }
    }

    // systable_endscan(scan);
    scan.end()
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
    use types_guc::guc::GucSource::{PGC_S_DATABASE, PGC_S_DATABASE_USER, PGC_S_GLOBAL, PGC_S_USER};

    // relsetting = table_open(DbRoleSettingRelationId, AccessShareLock);
    let relsetting = open_db_role_setting(mcx, AccessShareLock)?;

    // snapshot = RegisterSnapshot(GetCatalogSnapshot(DbRoleSettingRelationId));
    let snapshot = snapmgr::register_snapshot::call(snapmgr::get_catalog_snapshot::call(
        DB_ROLE_SETTING_RELATION_ID,
    )?)?;

    // Later settings are ignored if set earlier.
    ApplySetting(&snapshot, databaseid, roleid, &relsetting, PGC_S_DATABASE_USER)?;
    ApplySetting(&snapshot, INVALID_OID, roleid, &relsetting, PGC_S_USER)?;
    ApplySetting(&snapshot, databaseid, INVALID_OID, &relsetting, PGC_S_DATABASE)?;
    ApplySetting(&snapshot, INVALID_OID, INVALID_OID, &relsetting, PGC_S_GLOBAL)?;

    // UnregisterSnapshot(snapshot);
    snapmgr::unregister_snapshot::call(snapshot)?;
    // table_close(relsetting, AccessShareLock);
    relsetting.close(AccessShareLock)
}

/// Install this crate's seams. `process_db_role_settings` is wired to the
/// `apply_db_role_settings` seam consumed by postinit's `process_settings`.
/// This crate consumes (does not own) the genam `systable_*`, the
/// indexing/catalog-form, the guc, and the snapmgr seams — their owners install
/// them.
pub fn init_seams() {
    use backend_catalog_pg_db_role_setting_seams as seam;
    seam::apply_db_role_settings::set(process_db_role_settings);
}

#[cfg(test)]
mod tests;
