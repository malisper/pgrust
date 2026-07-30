use mcx::MemoryContext;
use relcache::schemapg::CLASS_OID_INDEX_ID;
use relcache_build_seams::ScannedPgClass;
use types_core::{InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, FATAL};
use types_rel::{AccessShareLock, FormData_pg_class};
use types_tuple::{HeapTupleData, TupleDescData};

use crate::{getattr, name_from, oid_key, req};

const Anum_pg_class_oid: i32 = 1;
const Anum_pg_class_reloptions: i32 = 33;

#[track_caller]
#[cold]
#[inline(never)]
fn no_database() -> Box<PgError> {
    Box::new(PgError::new(
        FATAL,
        "cannot read pg_class without having selected a database",
    ))
}

// ScanPgRelation (relcache.c). force_non_historic toggles the non-historic
// catalog snapshot in C; historic snapshots (logical decoding) are unported,
// so the default catalog snapshot is already the non-historic one.
pub(crate) fn scan_pg_relation(
    target_rel_id: Oid,
    index_ok: bool,
    _force_non_historic: bool,
) -> PgResult<Option<ScannedPgClass>> {
    if init_small::globals::MyDatabaseId() == InvalidOid {
        return Err(no_database());
    }
    let cx = MemoryContext::new("ScanPgRelation");
    let mcx = cx.mcx();
    let rel = table::table_open(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_class_oid, target_rel_id)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        CLASS_OID_INDEX_ID,
        index_ok && relcache::criticalRelcachesBuilt(),
        None,
        &keys,
    )?;
    let decoded = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => Some(decode(mcx, rel.descr(), tup, target_rel_id)?),
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(decoded)
}

pub(crate) fn decode(
    scratch: mcx::Mcx<'_>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    relid: Oid,
) -> PgResult<ScannedPgClass> {
    let form = FormData_pg_class {
        relname: name_from(req(td, tup, 2)?),
        relnamespace: req(td, tup, 3)?.as_oid(),
        reltype: req(td, tup, 4)?.as_oid(),
        relowner: req(td, tup, 6)?.as_oid(),
        relam: req(td, tup, 7)?.as_oid(),
        relfilenode: req(td, tup, 8)?.as_oid(),
        reltablespace: req(td, tup, 9)?.as_oid(),
        relpages: req(td, tup, 10)?.as_i32(),
        reltuples: req(td, tup, 11)?.as_f32(),
        relallvisible: req(td, tup, 12)?.as_i32(),
        reltoastrelid: req(td, tup, 14)?.as_oid(),
        relhasindex: req(td, tup, 15)?.as_bool(),
        relisshared: req(td, tup, 16)?.as_bool(),
        relpersistence: req(td, tup, 17)?.as_u8(),
        relkind: req(td, tup, 18)?.as_u8(),
        relhassubclass: req(td, tup, 23)?.as_bool(),
        relrowsecurity: req(td, tup, 24)?.as_bool(),
        relispopulated: req(td, tup, 26)?.as_bool(),
        relreplident: req(td, tup, 27)?.as_u8(),
        relispartition: req(td, tup, 28)?.as_bool(),
        relfrozenxid: req(td, tup, 30)?.as_transaction_id(),
        relminmxid: req(td, tup, 31)?.as_transaction_id(),
    };
    let _ = relid;
    let (opts_datum, opts_null) = getattr(td, tup, Anum_pg_class_reloptions);
    let options = reloptions::extractRelOptions(
        scratch,
        form.relkind,
        form.relam,
        if opts_null { None } else { Some(opts_datum) },
    )?;
    let relchecks = req(td, tup, 20)?.as_i16();
    let relhasrules = req(td, tup, 21)?.as_bool();
    let relhastriggers = req(td, tup, 22)?.as_bool();
    Ok(ScannedPgClass { form, relchecks, relhastriggers, relhasrules, options })
}
