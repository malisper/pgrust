#![allow(non_upper_case_globals)]

use std::rc::Rc;

use datum::Datum;
use mcx::MemoryContext;
use types_core::catalog::C_COLLATION_OID;
use types_core::fmgr::F_OIDEQ;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::PgResult;
use types_guc::GucSource;
use types_rel::{Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::SnapshotData;

const DbRoleSettingRelationId: Oid = 2964;
const DbRoleSettingDatidRolidIndexId: Oid = 2965;
const Anum_pg_db_role_setting_setdatabase: i32 = 1;
const Anum_pg_db_role_setting_setrole: i32 = 2;
const Anum_pg_db_role_setting_setconfig: i32 = 3;

pub fn init_seams() {
    pg_db_role_setting_seams::apply_setting::set(ApplySetting);
}

// DropSetting (pg_db_role_setting.c): C uses a keyed catalog heapscan, so the
// index is passed with indexOK=false.
#[allow(non_snake_case)]
pub fn DropSetting<'mcx>(mcx: Mcx<'mcx>, databaseid: Oid, roleid: Oid) -> PgResult<()> {
    let relsetting = table::table_open(mcx, DbRoleSettingRelationId, RowExclusiveLock)?;

    let mut keys: [ScanKeyData; 2] = [ScanKeyData::empty(), ScanKeyData::empty()];
    let mut numkeys = 0;
    if databaseid != InvalidOid {
        keys[numkeys] = oid_key(Anum_pg_db_role_setting_setdatabase, databaseid);
        numkeys += 1;
    }
    if roleid != InvalidOid {
        keys[numkeys] = oid_key(Anum_pg_db_role_setting_setrole, roleid);
        numkeys += 1;
    }

    let mut scan = genam::systable_beginscan(
        mcx,
        &relsetting,
        DbRoleSettingDatidRolidIndexId,
        false,
        None,
        &keys[..numkeys],
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&relsetting, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;

    relsetting.close(RowExclusiveLock)
}

fn oid_key(attno: i32, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info({F_OIDEQ}) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

/// ApplySetting (pg_db_role_setting.c).
#[allow(non_snake_case)]
pub fn ApplySetting(
    snapshot: &Rc<SnapshotData<'static>>,
    databaseid: Oid,
    roleid: Oid,
    relsetting: &Relation<'_>,
    _source: GucSource,
) -> PgResult<()> {
    let cx = MemoryContext::new("ApplySetting");
    let mcx = cx.mcx();
    let keys = [
        oid_key(Anum_pg_db_role_setting_setdatabase, databaseid),
        oid_key(Anum_pg_db_role_setting_setrole, roleid),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        relsetting,
        DbRoleSettingDatidRolidIndexId,
        true,
        Some(Rc::clone(snapshot)),
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: pg_db_role_setting row under its relation's descriptor.
        let _ = unsafe {
            types_tuple::heap_getattr(
                tup,
                Anum_pg_db_role_setting_setconfig,
                relsetting.descr(),
                &mut isnull,
            )
        };
        if !isnull {
            panic!(
                "pg_db_role_setting: setconfig set for (db {databaseid}, role {roleid}): \
                 ProcessGUCArray unported (text[] deconstruct lane)"
            );
        }
    }
    genam::systable_endscan(mcx, scan)?;
    Ok(())
}
