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

const Natts_pg_db_role_setting: usize = 3;
const TEXTOID: Oid = 25;

// setconfig text[] image -> owned "name=value" entries.
fn setconfig_entries(mcx: Mcx<'_>, d: Datum) -> PgResult<Vec<String>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum addresses in-tuple bytes; the
    // length is read from its own header before slicing.
    let raw = unsafe {
        let len = types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, len)
    };
    let image = detoast::detoast_attr(mcx, raw)?;
    let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(mcx, &image, TEXTOID, true)?;
    let mut out = Vec::with_capacity(elems.len());
    for (e, isnull) in elems.iter().zip(nulls.iter()) {
        if *isnull {
            continue;
        }
        let ep = e.as_usize() as *const u8;
        // SAFETY: by-ref text element datum inside the detoasted image.
        let text = unsafe {
            core::slice::from_raw_parts(ep, types_tuple::varatt::varsize_any(ep))
        };
        let payload = varlena::open_image(mcx, text)?;
        out.push(String::from_utf8_lossy(payload.as_bytes()).into_owned());
    }
    Ok(out)
}

fn entries_to_text_array<'mcx>(
    mcx: Mcx<'mcx>,
    entries: &[String],
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let mut datums = Vec::with_capacity(entries.len());
    let mut _keep_alive = Vec::with_capacity(entries.len());
    for entry in entries {
        let t = varlena::cstring_to_text(mcx, entry.as_bytes())?;
        datums.push(Datum::from_usize(t.as_bytes().as_ptr() as usize));
        _keep_alive.push(t);
    }
    arrayfuncs::construct_array(
        mcx,
        &datums,
        TEXTOID,
        -1,
        false,
        arrayfuncs::foundation::TYPALIGN_INT,
    )
}

/// AlterSetting (pg_db_role_setting.c).
#[allow(non_snake_case)]
pub fn AlterSetting<'mcx>(
    mcx: Mcx<'mcx>,
    databaseid: Oid,
    roleid: Oid,
    setstmt: &types_nodes::parsenodes::VariableSetStmt<'_>,
) -> PgResult<()> {
    use types_nodes::parsenodes::VariableSetKind;

    let valuestr = guc_funcs::ExtractSetVariableArgs(setstmt)?;

    let rel = table::table_open(mcx, DbRoleSettingRelationId, RowExclusiveLock)?;
    let keys = [
        oid_key(Anum_pg_db_role_setting_setdatabase, databaseid),
        oid_key(Anum_pg_db_role_setting_setrole, roleid),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        DbRoleSettingDatidRolidIndexId,
        true,
        None,
        &keys,
    )?;
    let tuple = genam::systable_getnext(mcx, &mut scan)?;

    let old_config = |tup: &types_tuple::HeapTupleData<'_>| -> PgResult<Option<Vec<String>>> {
        let mut isnull = false;
        // SAFETY: pg_db_role_setting row under its relation's descriptor.
        let d = unsafe {
            types_tuple::heap_getattr(
                tup,
                Anum_pg_db_role_setting_setconfig,
                rel.descr(),
                &mut isnull,
            )
        };
        if isnull {
            Ok(None)
        } else {
            Ok(Some(setconfig_entries(mcx, d)?))
        }
    };

    let replace_setconfig = |tup: &types_tuple::HeapTupleData<'_>,
                             new: Option<&[String]>|
     -> PgResult<()> {
        match new {
            Some(entries) => {
                let a = entries_to_text_array(mcx, entries)?;
                let mut values = [Datum::null(); Natts_pg_db_role_setting];
                let mut isnull = [false; Natts_pg_db_role_setting];
                let mut replace = [false; Natts_pg_db_role_setting];
                values[Anum_pg_db_role_setting_setconfig as usize - 1] =
                    Datum::from_usize(a.as_ptr() as usize);
                replace[Anum_pg_db_role_setting_setconfig as usize - 1] = true;
                let mut newtuple = heaptuple::heap_modify_tuple(
                    mcx,
                    tup,
                    rel.descr(),
                    &values,
                    &isnull,
                    &replace,
                )?;
                catalog_indexing::CatalogTupleUpdate(mcx, &rel, &tup.t_self, &mut newtuple)?;
            }
            None => {
                catalog_indexing::CatalogTupleDelete(&rel, &tup.t_self)?;
            }
        }
        Ok(())
    };

    if setstmt.kind == VariableSetKind::VAR_RESET_ALL {
        if let Some(tup) = tuple {
            let new = match old_config(tup)? {
                Some(old) => guc::GUCArrayReset(&old)?,
                None => None,
            };
            replace_setconfig(tup, new.as_deref())?;
        }
    } else if let Some(tup) = tuple {
        let name = setstmt.name.unwrap_or("");
        let old = old_config(tup)?.unwrap_or_default();
        let new = match valuestr.as_deref() {
            Some(v) => Some(guc::GUCArrayAdd(&old, name, v)?),
            None => guc::GUCArrayDelete(&old, name)?,
        };
        replace_setconfig(tup, new.as_deref())?;
    } else if let Some(v) = valuestr.as_deref() {
        let name = setstmt.name.unwrap_or("");
        let a = guc::GUCArrayAdd(&[], name, v)?;
        let img = entries_to_text_array(mcx, &a)?;
        let values = [
            Datum::from_oid(databaseid),
            Datum::from_oid(roleid),
            Datum::from_usize(img.as_ptr() as usize),
        ];
        let nulls = [false; Natts_pg_db_role_setting];
        let mut newtuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut newtuple)?;
    }

    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::NoLock)
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
    source: GucSource,
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
        let d = unsafe {
            types_tuple::heap_getattr(
                tup,
                Anum_pg_db_role_setting_setconfig,
                relsetting.descr(),
                &mut isnull,
            )
        };
        if !isnull {
            let entries = setconfig_entries(mcx, d)?;
            // All options apply at SUSET: the insert into pg_db_role_setting
            // already carried the permission check.
            guc::ProcessGUCArray(
                &entries,
                types_guc::GucContext::PGC_SUSET,
                source,
                guc::GUC_ACTION_SET,
            )?;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    Ok(())
}
