#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::mem::ManuallyDrop;

use datum::Datum;
use mcx::{MemoryContext, PgFxHashMap};
use types_core::{InvalidOid, Oid, RelFileNumber, F_OIDEQ, RELPERSISTENCE_TEMP};
use types_error::{PgError, PgResult};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::AccessShareLock;
use types_tuple::{HeapTupleData, TupleDescData};

const RelationRelationId: Oid = 1259;
const ClassTblspcRelfilenodeIndexId: Oid = 3455;
const GLOBALTABLESPACE_OID: Oid = 1664;
const Anum_pg_class_oid: i32 = 1;
const Anum_pg_class_relfilenode: i32 = 8;
const Anum_pg_class_reltablespace: i32 = 9;
const Anum_pg_class_relpersistence: i32 = 17;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct RelfilenumberMapKey {
    reltablespace: Oid,
    relfilenumber: RelFileNumber,
}

struct RelfilenumberMap {
    skey: [ScanKeyData; 2],
    // Value is pg_class.oid; InvalidOid is a negative cache entry.
    hash: PgFxHashMap<'static, RelfilenumberMapKey, Oid>,
}

thread_local! {
    static MAP: RefCell<Option<ManuallyDrop<RelfilenumberMap>>> = const { RefCell::new(None) };
}

fn RelfilenumberMapInvalidateCallback(_arg: Datum, relid: Oid) {
    MAP.with(|cell| {
        let mut slot = cell.borrow_mut();
        let map = slot.as_mut().expect("RelfilenumberMapHash != NULL");
        map.hash.retain(|_, entry_relid| {
            !(relid == InvalidOid
                || *entry_relid == InvalidOid
                || *entry_relid == relid)
        });
    });
}

fn InitializeRelfilenumberMap() -> PgResult<()> {
    let mut skey = [ScanKeyData::empty(), ScanKeyData::empty()];
    for entry in &mut skey {
        entry.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)?;
        entry.sk_strategy = BTEqualStrategyNumber;
        entry.sk_subtype = InvalidOid;
        entry.sk_collation = InvalidOid;
    }
    skey[0].sk_attno = Anum_pg_class_reltablespace as i16;
    skey[1].sk_attno = Anum_pg_class_relfilenode as i16;

    // Installed only after skey resolution: an fmgr error must not leave a
    // partially initialized map.
    let mcx = ::mcx::session_root("RelfilenumberMap cache").mcx();
    let map = RelfilenumberMap {
        skey,
        hash: PgFxHashMap::with_hasher_in(Default::default(), mcx),
    };
    MAP.with(|cell| *cell.borrow_mut() = Some(ManuallyDrop::new(map)));

    inval::invalidate::CacheRegisterRelcacheCallback(
        RelfilenumberMapInvalidateCallback,
        Datum::null(),
    )
}

fn getattr(tup: &HeapTupleData<'_>, attnum: i32, desc: &TupleDescData<'_>) -> Datum {
    let mut isnull = false;
    // SAFETY: fixed-layout leading pg_class columns under pg_class's own
    // descriptor; never null.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

/// Temp relations can share a relfilenumber with permanent or other backends'
/// temp relations; they are skipped. Returns InvalidOid when nothing matches.
pub fn RelidByRelfilenumber(
    mut reltablespace: Oid,
    relfilenumber: RelFileNumber,
) -> PgResult<Oid> {
    if MAP.with(|cell| cell.borrow().is_none()) {
        InitializeRelfilenumberMap()?;
    }

    // pg_class stores 0 when the value is actually MyDatabaseTableSpace.
    if reltablespace == init_small::globals::MyDatabaseTableSpace() {
        reltablespace = 0;
    }
    let key = RelfilenumberMapKey { reltablespace, relfilenumber };

    let cached =
        MAP.with(|cell| cell.borrow().as_ref().and_then(|m| m.hash.get(&key).copied()));
    if let Some(relid) = cached {
        return Ok(relid);
    }

    let mut relid = InvalidOid;

    if reltablespace == GLOBALTABLESPACE_OID {
        relid = relmapper::RelationMapFilenumberToOid(relfilenumber, true);
    } else {
        let mut skey = MAP
            .with(|cell| cell.borrow().as_ref().expect("map initialized").skey.clone());
        skey[0].sk_argument = Datum::from_oid(reltablespace);
        skey[1].sk_argument = Datum::from_oid(relfilenumber);

        let cx = MemoryContext::new("RelidByRelfilenumber");
        let mcx = cx.mcx();
        let relation = table::table_open(mcx, RelationRelationId, AccessShareLock)?;
        let mut scandesc = genam::systable_beginscan(
            mcx,
            &relation,
            ClassTblspcRelfilenodeIndexId,
            true,
            None,
            &skey,
        )?;

        let mut found = false;
        while let Some(ntp) = genam::systable_getnext(mcx, &mut scandesc)? {
            let desc = relation.descr();
            if getattr(ntp, Anum_pg_class_relpersistence, desc).as_u8()
                == RELPERSISTENCE_TEMP
            {
                continue;
            }
            if found {
                return Err(PgError::error(format!(
                    "unexpected duplicate for tablespace {reltablespace}, relfilenumber {relfilenumber}"
                ))
                .into());
            }
            found = true;
            debug_assert_eq!(
                getattr(ntp, Anum_pg_class_reltablespace, desc).as_oid(),
                reltablespace
            );
            debug_assert_eq!(
                getattr(ntp, Anum_pg_class_relfilenode, desc).as_oid(),
                relfilenumber
            );
            relid = getattr(ntp, Anum_pg_class_oid, desc).as_oid();
        }

        genam::systable_endscan(mcx, scandesc)?;
        table::table_close(relation, AccessShareLock)?;

        if !found {
            relid = relmapper::RelationMapFilenumberToOid(relfilenumber, false);
        }
    }

    // Enter the entry only now: opening pg_class can run invalidations that
    // would have deleted an earlier insert.
    MAP.with(|cell| -> PgResult<()> {
        let mut slot = cell.borrow_mut();
        let map = slot.as_mut().expect("map initialized");
        if map.hash.insert(key, relid).is_some() {
            return Err(PgError::error("corrupted hashtable").into());
        }
        Ok(())
    })?;

    Ok(relid)
}

pub fn init_seams() {
    relfilenumbermap_seams::relid_by_relfilenumber::set(RelidByRelfilenumber);
}

#[cfg(test)]
mod tests;
