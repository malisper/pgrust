#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `::types_error::PgResult`
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
//!   [`SysScanDescData`][::types_scan::genam::SysScanDescData] — installed by the
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

use ::mcx::{Mcx, MemoryContext};
use ::types_catalog::catalog::{DB_ROLE_SETTING_DATID_ROLID_INDEX_ID, DB_ROLE_SETTING_RELATION_ID};
use ::types_catalog::pg_db_role_setting::{
    Anum_pg_db_role_setting_setdatabase, Anum_pg_db_role_setting_setrole,
};
use ::types_core::fmgr::F_OIDEQ;
use ::types_core::primitive::{AttrNumber, Oid, OidIsValid};
use ::types_error::PgResult;
use ::types_guc::guc::GucSource;
use ::parsenodes::{VariableSetKind, VariableSetStmt};
use ::rel::Relation;
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::snapshot::SnapshotData;
use ::types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::ItemPointerData;

use ::scankey::ScanKeyInit;
use genam_seams as genam;
use table as table;
use indexing_seams as indexing;
use objectaccess_seams as objectaccess;
use guc_funcs_seams as guc_funcs;
use guc_seams as guc;
use snapmgr_seams as snapmgr;

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
    use ::types_core::primitive::INVALID_OID;
    use ::types_guc::guc::GucSource::{PGC_S_DATABASE, PGC_S_DATABASE_USER, PGC_S_GLOBAL, PGC_S_USER};

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
    snapmgr::unregister_snapshot::call(snapshot);
    // table_close(relsetting, AccessShareLock);
    relsetting.close(AccessShareLock)
}

/// Install this crate's seams. `process_db_role_settings` is wired to the
/// `apply_db_role_settings` seam consumed by postinit's `process_settings`.
/// This crate consumes (does not own) the genam `systable_*`, the
/// indexing/catalog-form, the guc, and the snapmgr seams — their owners install
/// them.
/* ===========================================================================
 * alter_database_setting seam — the AlterDatabaseSet (dbcommands.c) boundary.
 *
 * `AlterDatabaseSet` hands the canonical `'mcx` arena `VariableSetStmt` (an arm
 * of `::nodes::nodes::Node`); `AlterSetting` consumes the owner's
 * owned-`String` `::parsenodes::VariableSetStmt`. The two parse-node models
 * meet only here. We convert the arena node into the owned form and run the
 * catalog read-modify-write.
 * ========================================================================= */

/// Convert one arena `VariableSetStmt` `args` element to the owned value node.
///
/// In the grammar each member is an `A_Const` (its `val` a value node), or an
/// `A_Const` within a `TypeCast` (the `SET TIME ZONE INTERVAL` case). The owned
/// model — mirroring `ExtractSetVariableArgs`/`flatten_set_variable_args` —
/// carries the `A_Const` value node directly (`Node::Integer`/`Float`/`String`/
/// `Boolean`/`BitString`). We unwrap `A_Const.val` to the bare value node and
/// re-home it onto the owned `::parsenodes::Node`.
fn arena_arg_to_owned(
    arg: &::nodes::nodes::Node<'_>,
) -> PgResult<::parsenodes::Node> {
    use ::nodes::nodes::ntag;
    use ::nodes::nodes::Node as ANode;
    use parsenodes as pn;

    // Unwrap an `A_Const` to its inner value node (a bare value node is taken
    // as-is). A `TypeCast`-wrapped `A_Const` (`SET TIME ZONE INTERVAL`) is not
    // representable in the owned value-node model — the same `ConstInterval`
    // leg `flatten_set_variable_args` handles via `interval_in`/`interval_out`;
    // that coercion is out of this converter's scope, so it errors like the
    // owned `flatten_set_variable_args` unrecognized-node path.
    let val: &ANode = match arg.node_tag() {
        ntag::T_A_Const => match arg.expect_a_const().val.as_deref() {
            Some(v) => v,
            // `isnull` A_Const (SQL NULL constant) — not a SET-value shape.
            None => return Err(unrecognized_arg(arg)),
        },
        _ => arg,
    };

    match val.node_tag() {
        ntag::T_Integer => Ok(pn::Node::Integer(pn::Integer {
            ival: val.expect_integer().ival,
        })),
        ntag::T_Float => Ok(pn::Node::Float(pn::Float {
            fval: Some(val.expect_float().fval.as_str().to_string()),
        })),
        ntag::T_String => Ok(pn::Node::String(pn::StringNode {
            sval: Some(val.expect_string().sval.as_str().to_string()),
        })),
        ntag::T_Boolean => Ok(pn::Node::Boolean(pn::Boolean {
            boolval: val.expect_boolean().boolval,
        })),
        ntag::T_BitString => Ok(pn::Node::BitString(pn::BitString {
            bsval: Some(val.expect_bitstring().bsval.as_str().to_string()),
        })),
        _ => Err(unrecognized_arg(val)),
    }
}

/// C: `elog(ERROR, "unrecognized node type: %d", nodeTag(arg))` from
/// `flatten_set_variable_args` for an arg shape the value-node model can't carry.
fn unrecognized_arg(node: &::nodes::nodes::Node<'_>) -> ::types_error::PgError {
    utils_error::ereport(::types_error::ERROR)
        .errmsg_internal(format!("unrecognized node type: {}", node.node_tag().0))
        .into_error()
}

/// `alter_database_setting` seam body: convert the arena `VariableSetStmt` to
/// the owned model and run `AlterSetting`.
fn alter_database_setting<'mcx, 's>(
    mcx: Mcx<'mcx>,
    databaseid: Oid,
    roleid: Oid,
    setstmt: &::nodes::nodes::Node<'s>,
) -> PgResult<()> {
    use ::nodes::ddlnodes::VariableSetKind as AKind;

    let v = match setstmt.node_tag() {
        ::nodes::nodes::ntag::T_VariableSetStmt => setstmt.expect_variablesetstmt(),
        _ => return Err(unrecognized_arg(setstmt)),
    };

    let kind = match v.kind {
        AKind::VAR_SET_VALUE => VariableSetKind::SetValue,
        AKind::VAR_SET_DEFAULT => VariableSetKind::SetDefault,
        AKind::VAR_SET_CURRENT => VariableSetKind::SetCurrent,
        AKind::VAR_SET_MULTI => VariableSetKind::SetMulti,
        AKind::VAR_RESET => VariableSetKind::Reset,
        AKind::VAR_RESET_ALL => VariableSetKind::ResetAll,
    };

    let mut args: Vec<::parsenodes::Node> = Vec::with_capacity(v.args.len());
    for a in v.args.iter() {
        args.push(arena_arg_to_owned(a)?);
    }

    let owned = VariableSetStmt {
        kind,
        name: v.name.as_ref().map(|s| s.as_str().to_string()),
        args,
        is_local: v.is_local,
        location: v.location,
    };

    AlterSetting(mcx, databaseid, roleid, &owned)
}

pub fn init_seams() {
    use pg_db_role_setting_seams as seam;
    seam::apply_db_role_settings::set(process_db_role_settings);
    seam::alter_database_setting::set(alter_database_setting);

    // user.c DROP ROLE: `DropSetting(InvalidOid, roleid)` removes the role's
    // per-database GUC settings.
    user_seams::drop_setting::set(|databaseid, roleid| {
        let ctx = ::mcx::MemoryContext::new("DropSetting");
        DropSetting(ctx.mcx(), databaseid, roleid)
    });

    // user.c ALTER ROLE ... SET: `AlterSetting(databaseid, roleid, setstmt)`.
    // The `VariableSetStmt` arrives already in the owned `parsenodes`
    // model (carried opaquely as `::parsenodes::Node::VariableSetStmt`).
    user_seams::alter_setting::set(|databaseid, roleid, setstmt| {
        let ctx = ::mcx::MemoryContext::new("AlterSetting");
        let setstmt = setstmt.ok_or_else(|| {
            utils_error::ereport(::types_error::ERROR)
                .errmsg_internal("AlterSetting: missing VariableSetStmt".to_string())
                .into_error()
        })?;
        let v = match &setstmt {
            ::parsenodes::Node::VariableSetStmt(v) => v,
            other => {
                return Err(utils_error::ereport(::types_error::ERROR)
                    .errmsg_internal(format!(
                        "unrecognized node type: {}",
                        other.node_tag_name()
                    ))
                    .into_error())
            }
        };
        AlterSetting(ctx.mcx(), databaseid, roleid, v)
    });
}

#[cfg(test)]
mod tests;
