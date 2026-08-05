use std::rc::Rc;

use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, ForkNumber, InvalidOid, Oid, ProcNumber, RelFileNumber, F_OIDEQ,
    INVALID_PROC_NUMBER,
};
use types_error::PgResult;
use types_rel::Relation;
use types_scan::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::RelFileLocator;

pub const ClassOidIndexId: Oid = 2662;
pub const Anum_pg_class_oid: i32 = 1;

const RELPERSISTENCE_PERMANENT: u8 = b'p';
const RELPERSISTENCE_UNLOGGED: u8 = b'u';
const RELPERSISTENCE_TEMP: u8 = b't';

fn oid_eq_key(attno: AttrNumber, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

// SnapshotAny probe: uncommitted rows must count as collisions.
pub fn GetNewOidWithIndex<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    indexId: Oid,
    oidcolumn: AttrNumber,
) -> PgResult<Oid> {
    debug_assert!(crate::IsSystemRelation(relation.data_rc()));
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return varsup::GetNewObjectId();
    }
    let snapshot = Rc::new(SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_ANY));
    loop {
        let new_oid = varsup::GetNewObjectId()?;
        let key = [oid_eq_key(oidcolumn, new_oid)];
        let mut scan =
            genam::systable_beginscan(mcx, relation, indexId, true, Some(Rc::clone(&snapshot)), &key)?;
        let collides = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        if !collides {
            return Ok(new_oid);
        }
    }
}

pub fn GetNewRelFileNumber<'mcx>(
    mcx: Mcx<'mcx>,
    reltablespace: Oid,
    pg_class: Option<&Relation<'mcx>>,
    relpersistence: u8,
) -> PgResult<RelFileNumber> {
    let proc_number: ProcNumber = match relpersistence {
        RELPERSISTENCE_TEMP => init_small::globals::MyProcNumber(),
        RELPERSISTENCE_UNLOGGED | RELPERSISTENCE_PERMANENT => INVALID_PROC_NUMBER,
        _ => panic!("invalid relpersistence: {relpersistence}"),
    };

    let spc_oid = if reltablespace != InvalidOid {
        reltablespace
    } else {
        init_small::globals::MyDatabaseTableSpace()
    };
    let db_oid = if spc_oid == relpath::GLOBALTABLESPACE_OID {
        InvalidOid
    } else {
        init_small::globals::MyDatabaseId()
    };

    loop {
        let rel_number: RelFileNumber = match pg_class {
            Some(rel) => {
                GetNewOidWithIndex(mcx, rel, ClassOidIndexId, Anum_pg_class_oid as AttrNumber)?
            }
            None => varsup::GetNewObjectId()?,
        };
        let locator = RelFileLocator::new(spc_oid, db_oid, rel_number);
        // access(F_OK) relative to the datadir cwd, as C's relpath probe.
        let rpath = relpath::GetRelationPath(locator, proc_number, ForkNumber::MAIN_FORKNUM);
        if !std::path::Path::new(&rpath).exists() {
            return Ok(rel_number);
        }
    }
}
