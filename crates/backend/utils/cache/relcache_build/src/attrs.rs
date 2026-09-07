use std::rc::Rc;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgString, PgVec};
use relcache::schemapg::ATTRIBUTE_RELID_NUM_INDEX_ID;
use types_core::fmgr::F_INT2GT;
use types_core::{
    ATTRIBUTE_RELATION_ID, ATTR_DEFAULT_INDEX_ID, ATTR_DEFAULT_RELATION_ID,
    CONSTRAINT_RELATION_ID, CONSTRAINT_RELID_TYPID_NAME_INDEX_ID, InvalidOid, Oid, RECORDOID,
};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_core::{AttrNumber, INDEX_MAX_KEYS};
use types_rel::{AccessShareLock, FormData_pg_class, ForeignKeyCacheInfo};
use types_scan::scankey::BTGreaterStrategyNumber;
use types_tuple::{
    AttrDefault, ConstrCheck, FormData_pg_attribute, HeapTupleData, TupleConstr, TupleDescData,
    ATTNULLABLE_INVALID, ATTNULLABLE_UNKNOWN, ATTNULLABLE_VALID,
};

use crate::{getattr, name_from, oid_key, req, scan_key};

const Anum_pg_attribute_attrelid: i32 = 1;
const Anum_pg_attribute_attnum: i32 = 5;
const Anum_pg_attrdef_adrelid: i32 = 2;
const Anum_pg_attrdef_adnum: i32 = 3;
const Anum_pg_attrdef_adbin: i32 = 4;
const Anum_pg_constraint_oid: i32 = 1;
const Anum_pg_constraint_conname: i32 = 2;
const Anum_pg_constraint_contype: i32 = 4;
const Anum_pg_constraint_conenforced: i32 = 7;
const Anum_pg_constraint_convalidated: i32 = 8;
const Anum_pg_constraint_conrelid: i32 = 9;
const Anum_pg_constraint_confrelid: i32 = 13;
const Anum_pg_constraint_connoinherit: i32 = 19;
const Anum_pg_constraint_conkey: i32 = 21;
const Anum_pg_constraint_confkey: i32 = 22;
const Anum_pg_constraint_conpfeqop: i32 = 23;
const Anum_pg_constraint_conbin: i32 = 28;
const CONSTRAINT_CHECK: i8 = b'c' as i8;
const CONSTRAINT_NOTNULL: i8 = b'n' as i8;
const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

// RelationBuildTupleDesc (relcache.c:530-670). The descriptor has exactly
// relnatts slots: a row outside 1..=relnatts is C's "invalid attribute number"
// ERROR (:563), and every slot the scan leaves empty -- a missing trailing row
// included -- is counted into C's "pg_attribute catalog is missing %d
// attribute(s)" ERROR (:666), instead of silently building a narrower
// descriptor than the heap tuples carry.
pub(crate) fn relation_build_tuple_desc(
    mcx: Mcx<'static>,
    relid: Oid,
    form: &FormData_pg_class,
    relnatts: i16,
    relchecks: i16,
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
    let mut missing_pairs: PgVec<'static, (i16, Datum)> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let a = decode(rel.descr(), tup, relid)?;
        if a.attnum <= 0 || a.attnum > relnatts {
            return Err(invalid_attribute_number(a.attnum, form));
        }
        if a.atthasmissing {
            if let Some(v) = attr_missing_fetch(mcx, smcx, rel.descr(), tup, &a)? {
                missing_pairs.push((a.attnum, v));
            }
        }
        rows.push(a);
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;

    let natts = relnatts.max(0) as usize;
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
    let mut ndef = 0usize;
    for a in slots.iter() {
        has_not_null |= a.attnotnull;
        has_generated_stored |= a.attgenerated == ATTRIBUTE_GENERATED_STORED;
        has_generated_virtual |= a.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL;
        if a.atthasdef {
            ndef += 1;
        }
    }
    let has_missing = !missing_pairs.is_empty();

    let mut td = tupdesc::CreateTupleDesc(mcx, &slots)?;
    td.tdtypeid = if form.reltype != InvalidOid { form.reltype } else { RECORDOID };
    td.tdtypmod = -1;
    td.tdrefcount = 1;
    if natts > 0 {
        td.compact_attrs[0].attcacheoff.set(0);
    }

    if has_not_null
        || has_generated_stored
        || has_generated_virtual
        || ndef > 0
        || relchecks > 0
        || has_missing
    {
        let is_catalog = catalog_seams::is_catalog_relation_oid::call(relid);
        let relname = String::from_utf8_lossy(form.relname.name_str());
        let defval = if ndef > 0 {
            attr_default_fetch(mcx, smcx, relid, ndef, &relname)?
        } else {
            PgVec::new_in(mcx)
        };
        let check = if relchecks > 0 || (!is_catalog && has_not_null) {
            check_nn_constraint_fetch(mcx, smcx, relid, relchecks, &relname, &mut td)?
        } else {
            PgVec::new_in(mcx)
        };
        if !is_catalog {
            for i in 0..td.natts as usize {
                let attr = &mut td.compact_attrs[i];
                if attr.attnullability == ATTNULLABLE_UNKNOWN {
                    attr.attnullability = ATTNULLABLE_VALID;
                } else {
                    debug_assert!(
                        attr.attnullability == ATTNULLABLE_INVALID
                            || attr.attnullability
                                == types_tuple::ATTNULLABLE_UNRESTRICTED
                    );
                }
            }
        }
        let mut missing: PgVec<'static, types_tuple::AttrMissing> = PgVec::new_in(mcx);
        if has_missing {
            missing
                .try_reserve_exact(natts)
                .map_err(|_| Box::new(mcx.oom(natts * core::mem::size_of::<types_tuple::AttrMissing>())))?;
            missing.resize(
                natts,
                types_tuple::AttrMissing { am_present: false, am_value: Datum::null() },
            );
            for &(attnum, v) in missing_pairs.iter() {
                missing[attnum as usize - 1] =
                    types_tuple::AttrMissing { am_present: true, am_value: v };
            }
        }
        td.constr = Some(mcx::box_new_in(
            mcx,
            TupleConstr {
                num_defval: defval.len() as u16,
                num_check: check.len() as u16,
                relchecks,
                defval,
                check,
                missing,
                has_not_null,
                has_generated_stored,
                has_generated_virtual,
            },
        ));
    }

    Ok(Rc::new(td))
}

// TextDatumGetCString over a possibly packed/toasted pg_node_tree column.
pub(crate) fn text_str<'mcx>(mcx: Mcx<'mcx>, scratch: Mcx<'_>, d: Datum) -> PgResult<PgString<'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: d comes off a not-null text column: a live varlena image
    // readable through its varsize_any extent.
    let image =
        unsafe { std::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(scratch, image)?;
    let s = core::str::from_utf8(payload.as_bytes())
        .unwrap_or_else(|_| panic!("non-UTF-8 pg_node_tree text"));
    PgString::from_str_in(s, mcx)
}

// AttrDefaultFetch (relcache.c:4496-4558): an extra row, a null adbin and a
// short count are each C's elog(WARNING) (:4530/:4539/:4557); the
// descriptor is built from whatever was found.
fn attr_default_fetch(
    mcx: Mcx<'static>,
    smcx: Mcx<'_>,
    relid: Oid,
    ndef: usize,
    relname: &str,
) -> PgResult<PgVec<'static, AttrDefault<'static>>> {
    let mut defval: PgVec<'static, AttrDefault<'static>> = PgVec::new_in(mcx);
    defval
        .try_reserve_exact(ndef)
        .map_err(|_| Box::new(mcx.oom(ndef * core::mem::size_of::<AttrDefault<'_>>())))?;
    let rel = table::table_open(smcx, ATTR_DEFAULT_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_attrdef_adrelid, relid)];
    let mut scan = genam::systable_beginscan(
        smcx,
        &rel,
        ATTR_DEFAULT_INDEX_ID,
        relcache::criticalRelcachesBuilt(),
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let adnum = req(rel.descr(), tup, Anum_pg_attrdef_adnum)?.as_i16();
        if defval.len() >= ndef {
            elog::elog(
                types_error::WARNING,
                format!(
                    "unexpected pg_attrdef record found for attribute {adnum} of relation \"{relname}\""
                ),
            )?;
            break;
        }
        let (val, isnull) = getattr(rel.descr(), tup, Anum_pg_attrdef_adbin);
        if isnull {
            elog::elog(
                types_error::WARNING,
                format!("null adbin for attribute {adnum} of relation \"{relname}\""),
            )?;
            continue;
        }
        defval.push(AttrDefault { adnum, adbin: Some(text_str(mcx, smcx, val)?) });
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    if defval.len() != ndef {
        elog::elog(
            types_error::WARNING,
            format!(
                "{} pg_attrdef record(s) missing for relation \"{relname}\"",
                ndef - defval.len()
            ),
        )?;
    }
    defval.sort_unstable_by_key(|d| d.adnum);
    Ok(defval)
}

// CheckNNConstraintFetch (relcache.c:4606-4700): 'c' rows fill the check
// array; invalid 'n' rows mark their column ATTNULLABLE_INVALID. An extra
// CHECK row, a null conbin and a short count are each C's elog(WARNING)
// (:4658/:4674/:4691).
fn check_nn_constraint_fetch(
    mcx: Mcx<'static>,
    smcx: Mcx<'_>,
    relid: Oid,
    ncheck: i16,
    relname: &str,
    td: &mut TupleDescData<'static>,
) -> PgResult<PgVec<'static, ConstrCheck<'static>>> {
    let mut check: PgVec<'static, ConstrCheck<'static>> = PgVec::new_in(mcx);
    check
        .try_reserve_exact(ncheck.max(0) as usize)
        .map_err(|_| Box::new(mcx.oom(ncheck.max(0) as usize)))?;
    let rel = table::table_open(smcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_constraint_conrelid, relid)];
    let mut scan = genam::systable_beginscan(
        smcx,
        &rel,
        CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        relcache::criticalRelcachesBuilt(),
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let contype = req(rel.descr(), tup, Anum_pg_constraint_contype)?.as_i8();
        if contype == CONSTRAINT_NOTNULL {
            if !req(rel.descr(), tup, Anum_pg_constraint_convalidated)?.as_bool() {
                let attnum = extract_not_null_column(smcx, rel.descr(), tup)?;
                td.compact_attrs[attnum as usize - 1].attnullability = ATTNULLABLE_INVALID;
            }
            continue;
        }
        if contype != CONSTRAINT_CHECK {
            continue;
        }
        if check.len() >= ncheck.max(0) as usize {
            elog::elog(
                types_error::WARNING,
                format!("unexpected pg_constraint record found for relation \"{relname}\""),
            )?;
            break;
        }
        let (val, isnull) = getattr(rel.descr(), tup, Anum_pg_constraint_conbin);
        if isnull {
            elog::elog(
                types_error::WARNING,
                format!("null conbin for relation \"{relname}\""),
            )?;
            continue;
        }
        let name_bytes = name_from(tup, req(rel.descr(), tup, Anum_pg_constraint_conname)?);
        let ccname = PgString::from_str_in(
            core::str::from_utf8(name_bytes.name_str()).expect("conname UTF-8"),
            mcx,
        )?;
        check.push(ConstrCheck {
            ccname: Some(ccname),
            ccbin: Some(text_str(mcx, smcx, val)?),
            ccenforced: req(rel.descr(), tup, Anum_pg_constraint_conenforced)?.as_bool(),
            ccvalid: req(rel.descr(), tup, Anum_pg_constraint_convalidated)?.as_bool(),
            ccnoinherit: req(rel.descr(), tup, Anum_pg_constraint_connoinherit)?.as_bool(),
        });
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    if check.len() as i32 != ncheck as i32 {
        elog::elog(
            types_error::WARNING,
            format!(
                "{} pg_constraint record(s) missing for relation \"{relname}\"",
                ncheck as i32 - check.len() as i32
            ),
        )?;
    }
    check.sort_unstable_by(|a, b| {
        a.ccname.as_ref().map(|s| s.as_str()).cmp(&b.ccname.as_ref().map(|s| s.as_str()))
    });
    Ok(check)
}

// RelationGetFKeyList's scan half (relcache.c): contype='f' rows on conrelid,
// arrays decoded per DeconstructFkConstraintRow (pg_constraint.c), scan order.
pub(crate) fn scan_pg_constraint_fkeys<'mcx>(
    mcx: Mcx<'mcx>,
    conrelid: Oid,
) -> PgResult<PgVec<'mcx, ForeignKeyCacheInfo>> {
    let cx = MemoryContext::new("RelationGetFKeyList");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_constraint_conrelid, conrelid)];
    let mut scan = genam::systable_beginscan(
        smcx,
        &rel,
        CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let mut out: PgVec<'mcx, ForeignKeyCacheInfo> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        if req(rel.descr(), tup, Anum_pg_constraint_contype)?.as_i8() != CONSTRAINT_FOREIGN {
            continue;
        }
        let td = rel.descr();
        // DeconstructFkConstraintRow (pg_constraint.c:1540-1600): every arm is
        // elog(ERROR) -- a catchable XX000 -- never an assertion.
        let conkey_img = fk_array_image(smcx, req(td, tup, Anum_pg_constraint_conkey)?)?;
        let (ndim, hasnull, elemtype, dim0) = array_header(&conkey_img);
        if ndim != 1 || hasnull || elemtype != INT2OID {
            return Err(constraint_decode_error("conkey is not a 1-D smallint array"));
        }
        if dim0 <= 0 || dim0 > INDEX_MAX_KEYS as i32 {
            return Err(constraint_decode_error(&format!(
                "foreign key constraint cannot have {dim0} columns"
            )));
        }
        let nkeys = dim0 as usize;
        let conkey = datum::array_build::deconstruct_array_image(smcx, &conkey_img, 2, true, b's')?;
        let confkey_img = fk_array_image(smcx, req(td, tup, Anum_pg_constraint_confkey)?)?;
        let (ndim, hasnull, elemtype, dim0) = array_header(&confkey_img);
        if ndim != 1 || dim0 as usize != nkeys || hasnull || elemtype != INT2OID {
            return Err(constraint_decode_error("confkey is not a 1-D smallint array"));
        }
        let confkey =
            datum::array_build::deconstruct_array_image(smcx, &confkey_img, 2, true, b's')?;
        let conpfeqop_img = fk_array_image(smcx, req(td, tup, Anum_pg_constraint_conpfeqop)?)?;
        let (ndim, hasnull, elemtype, dim0) = array_header(&conpfeqop_img);
        if ndim != 1 || dim0 as usize != nkeys || hasnull || elemtype != OIDOID {
            return Err(constraint_decode_error("conpfeqop is not a 1-D Oid array"));
        }
        let conpfeqop =
            datum::array_build::deconstruct_array_image(smcx, &conpfeqop_img, 4, true, b'i')?;
        debug_assert!(conkey.len() == nkeys && confkey.len() == nkeys && conpfeqop.len() == nkeys);
        let mut info = ForeignKeyCacheInfo {
            conoid: req(td, tup, Anum_pg_constraint_oid)?.as_oid(),
            conrelid: req(td, tup, Anum_pg_constraint_conrelid)?.as_oid(),
            confrelid: req(td, tup, Anum_pg_constraint_confrelid)?.as_oid(),
            conenforced: req(td, tup, Anum_pg_constraint_conenforced)?.as_bool(),
            nkeys: nkeys as i32,
            conkey: [0 as AttrNumber; INDEX_MAX_KEYS as usize],
            confkey: [0 as AttrNumber; INDEX_MAX_KEYS as usize],
            conpfeqop: [InvalidOid; INDEX_MAX_KEYS as usize],
        };
        for i in 0..nkeys {
            info.conkey[i] = conkey[i].as_i16();
            info.confkey[i] = confkey[i].as_i16();
            info.conpfeqop[i] = conpfeqop[i].as_oid();
        }
        out.push(info);
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(out)
}

const INT2OID: Oid = 21;
const OIDOID: Oid = 26;

// DatumGetArrayTypeP over a not-null pg_constraint array column: the
// detoasted 4B-header image (the disk image may be packed).
fn fk_array_image<'s>(smcx: Mcx<'s>, d: Datum) -> PgResult<PgVec<'s, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: not-null array column: live varlena image through its extent.
    let image = unsafe { std::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(smcx, image)?;
    let body = payload.as_bytes();
    let total = body.len() + 4;
    let mut full: PgVec<'s, u8> = mcx::vec_with_capacity_in(smcx, total)?;
    mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
    mcx::vec_append_bytes(&mut full, body)?;
    Ok(full)
}

// (ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DIMS[0]) of a 4B-header array
// image; fields past the image read as 0 so a truncated image fails the
// caller's 1-D check instead of reading out of bounds.
fn array_header(full: &[u8]) -> (i32, bool, Oid, i32) {
    let rd = |off: usize| -> i32 {
        full.get(off..off + 4).map_or(0, |b| i32::from_ne_bytes(b.try_into().unwrap()))
    };
    (rd(4), rd(8) != 0, rd(12) as Oid, rd(16))
}

// pg_constraint.c's decode arms: elog(ERROR, ...) -> XX000, catchable.
#[track_caller]
#[cold]
#[inline(never)]
fn constraint_decode_error(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_INTERNAL_ERROR))
}

// extractNotNullColumn (pg_constraint.c): conkey[0] of a not-null row.
fn extract_not_null_column(
    smcx: Mcx<'_>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
) -> PgResult<i16> {
    let full = fk_array_image(smcx, req(td, tup, Anum_pg_constraint_conkey)?)?;
    // pg_constraint.c:716-720: elog(ERROR, "conkey is not a 1-D smallint
    // array") -- catchable XX000 (the relcache build runs inside parse
    // analysis, so C's parser error-position context applies to it).
    let (ndim, hasnull, elemtype, dim0) = array_header(&full);
    if ndim != 1 || hasnull || elemtype != INT2OID || dim0 != 1 {
        return Err(constraint_decode_error("conkey is not a 1-D smallint array"));
    }
    let elems = datum::array_build::deconstruct_array_image(smcx, &full, 2, true, b's')?;
    Ok(elems[0].as_i16())
}

// relcache.c attrmiss arm: unwrap the 1-element attmissingval array; by-ref
// values are datumCopy'd into the cache context (the tuple dies with smcx).
fn attr_missing_fetch(
    mcx: Mcx<'static>,
    smcx: Mcx<'_>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    a: &FormData_pg_attribute,
) -> PgResult<Option<Datum>> {
    const Anum_pg_attribute_attmissingval: i32 = 25;
    let (val, isnull) = getattr(td, tup, Anum_pg_attribute_attmissingval);
    if isnull {
        return Ok(None);
    }
    let p = val.as_usize() as *const u8;
    // SAFETY: not-null anyarray column: live varlena image through its extent.
    let image = unsafe { std::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(smcx, image)?;
    let body = payload.as_bytes();
    let total = body.len() + 4;
    let mut full: PgVec<'_, u8> = mcx::vec_with_capacity_in(smcx, total)?;
    mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
    mcx::vec_append_bytes(&mut full, body)?;
    let elems = datum::array_build::deconstruct_array_image(
        smcx,
        &full,
        a.attlen,
        a.attbyval,
        a.attalign as u8,
    )?;
    assert!(elems.len() == 1, "attmissingval with {} entries", elems.len());
    let v = elems[0];
    if a.attbyval {
        return Ok(Some(v));
    }
    let src = v.as_usize() as *const u8;
    let len = if a.attlen > 0 {
        a.attlen as usize
    } else {
        debug_assert!(a.attlen == -1);
        // SAFETY: element datum points into `full`, a live varlena image.
        unsafe { types_tuple::varatt::varsize_any(src) }
    };
    // SAFETY: `len` bytes readable at src per the array image layout.
    let bytes = unsafe { std::slice::from_raw_parts(src, len) };
    let copy = mcx::slice_borrow_in(mcx, bytes)?;
    Ok(Some(Datum::from_usize(copy.as_ptr() as usize)))
}

pub(crate) fn decode(
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    relid: Oid,
) -> PgResult<FormData_pg_attribute> {
    let a = FormData_pg_attribute {
        attrelid: req(td, tup, 1)?.as_oid(),
        attname: name_from(tup, req(td, tup, 2)?),
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

#[track_caller]
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

// relcache.c:563 elog(ERROR, "invalid attribute number %d for relation \"%s\"").
#[track_caller]
#[cold]
#[inline(never)]
fn invalid_attribute_number(attnum: i16, form: &FormData_pg_class) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "invalid attribute number {attnum} for relation \"{}\"",
            String::from_utf8_lossy(form.relname.name_str())
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[track_caller]
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
