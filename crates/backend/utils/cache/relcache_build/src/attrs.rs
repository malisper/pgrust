use std::rc::Rc;

use mcx::{Mcx, MemoryContext, PgVec};
use relcache::schemapg::ATTRIBUTE_RELID_NUM_INDEX_ID;
use types_core::fmgr::F_INT2GT;
use types_core::{ATTRIBUTE_RELATION_ID, InvalidOid, Oid, RECORDOID};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{AccessShareLock, FormData_pg_class};
use types_scan::scankey::BTGreaterStrategyNumber;
use types_tuple::{FormData_pg_attribute, HeapTupleData, TupleConstr, TupleDescData};

use crate::{name_from, oid_key, req, scan_key};

const Anum_pg_attribute_attrelid: i32 = 1;
const Anum_pg_attribute_attnum: i32 = 5;
const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

// RelationBuildTupleDesc (relcache.c). C cross-checks attnum <= relnatts and
// counts down from it; the trimmed rd_rel drops relnatts, so natts = max
// scanned attnum and gaps take C's missing-attribute ERROR.
pub(crate) fn relation_build_tuple_desc(
    mcx: Mcx<'static>,
    relid: Oid,
    form: &FormData_pg_class,
) -> PgResult<Rc<TupleDescData<'static>>> {
    let cx = MemoryContext::new("RelationBuildTupleDesc");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, ATTRIBUTE_RELATION_ID, AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_attribute_attrelid, relid),
        scan_key(
            Anum_pg_attribute_attnum,
            BTGreaterStrategyNumber,
            F_INT2GT,
            datum::Datum::from_i16(0),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        smcx,
        &rel,
        ATTRIBUTE_RELID_NUM_INDEX_ID,
        relcache::criticalRelcachesBuilt(),
        None,
        &keys,
    )?;
    let mut rows: PgVec<'_, FormData_pg_attribute> = PgVec::new_in(smcx);
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        rows.push(decode(rel.descr(), tup, relid)?);
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;

    let natts = rows.iter().map(|a| a.attnum).max().unwrap_or(0) as usize;
    let mut slots: PgVec<'_, FormData_pg_attribute> = mcx::vec_with_capacity_in(smcx, natts)?;
    slots.resize(natts, FormData_pg_attribute::default());
    for a in rows.iter() {
        slots[a.attnum as usize - 1] = *a;
    }
    let missing = slots.iter().filter(|a| a.attnum == 0).count();
    if missing != 0 {
        return Err(missing_attributes(missing, relid));
    }

    let mut has_not_null = false;
    let mut has_generated_stored = false;
    let mut has_generated_virtual = false;
    for a in slots.iter() {
        has_not_null |= a.attnotnull;
        has_generated_stored |= a.attgenerated == ATTRIBUTE_GENERATED_STORED;
        has_generated_virtual |= a.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL;
        if a.atthasdef {
            panic!(
                "relcache_build: attribute {} of relation {relid} has a default: \
                 AttrDefaultFetch unported (pg_attrdef unit)",
                a.attnum
            );
        }
        if a.atthasmissing {
            panic!(
                "relcache_build: attribute {} of relation {relid} has a missing value: \
                 attrmiss decode unported (array_get_element lane)",
                a.attnum
            );
        }
    }

    let mut td = tupdesc::CreateTupleDesc(mcx, &slots)?;
    td.tdtypeid = if form.reltype != InvalidOid { form.reltype } else { RECORDOID };
    td.tdtypmod = -1;
    td.tdrefcount = 1;
    if natts > 0 {
        td.compact_attrs[0].attcacheoff.set(0);
    }

    if has_not_null || has_generated_stored || has_generated_virtual {
        let is_catalog = catalog_seams::is_catalog_relation_oid::call(relid);
        // C also enters here on relchecks > 0; the trimmed rd_rel drops
        // relchecks, so CHECK constraints stay invisible until the
        // pg_constraint fetch unit (and the form field) land.
        if !is_catalog && has_not_null {
            panic!(
                "relcache_build: non-catalog relation {relid} has NOT NULL columns: \
                 CheckNNConstraintFetch unported (pg_constraint unit)"
            );
        }
        td.constr = Some(mcx::box_new_in(
            mcx,
            TupleConstr {
                defval: PgVec::new_in(mcx),
                check: PgVec::new_in(mcx),
                missing: PgVec::new_in(mcx),
                num_defval: 0,
                num_check: 0,
                has_not_null,
                has_generated_stored,
                has_generated_virtual,
            },
        ));
    }

    Ok(Rc::new(td))
}

pub(crate) fn decode(
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    relid: Oid,
) -> PgResult<FormData_pg_attribute> {
    let a = FormData_pg_attribute {
        attrelid: req(td, tup, 1)?.as_oid(),
        attname: name_from(req(td, tup, 2)?),
        atttypid: req(td, tup, 3)?.as_oid(),
        attlen: req(td, tup, 4)?.as_i16(),
        attnum: req(td, tup, 5)?.as_i16(),
        atttypmod: req(td, tup, 6)?.as_i32(),
        attndims: req(td, tup, 7)?.as_i16(),
        attbyval: req(td, tup, 8)?.as_bool(),
        attalign: req(td, tup, 9)?.as_i8(),
        attstorage: req(td, tup, 10)?.as_i8(),
        attcompression: req(td, tup, 11)?.as_i8(),
        attnotnull: req(td, tup, 12)?.as_bool(),
        atthasdef: req(td, tup, 13)?.as_bool(),
        atthasmissing: req(td, tup, 14)?.as_bool(),
        attidentity: req(td, tup, 15)?.as_i8(),
        attgenerated: req(td, tup, 16)?.as_i8(),
        attisdropped: req(td, tup, 17)?.as_bool(),
        attislocal: req(td, tup, 18)?.as_bool(),
        attinhcount: req(td, tup, 19)?.as_i16(),
        attcollation: req(td, tup, 20)?.as_oid(),
    };
    if a.attnum <= 0 {
        return Err(invalid_attnum(a.attnum, relid));
    }
    Ok(a)
}

#[cold]
#[inline(never)]
fn invalid_attnum(attnum: i16, relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "invalid attribute number {attnum} for relation OID {relid}"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[cold]
#[inline(never)]
fn missing_attributes(n: usize, relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "pg_attribute catalog is missing {n} attribute(s) for relation OID {relid}"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}
