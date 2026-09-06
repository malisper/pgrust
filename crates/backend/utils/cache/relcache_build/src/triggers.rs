// RelationBuildTriggers (trigger.c): pg_trigger scan in tgname order via
// TriggerRelidNameIndexId, decoded into a types_trigger::TriggerDesc.
use mcx::{Mcx, PgString, PgVec};
use types_core::fmgr::F_OIDEQ;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::AccessShareLock;
use types_trigger::{Trigger, TriggerDesc, TRIGGER_TYPE_ROW, TRIGGER_TYPE_STATEMENT};
use types_trigger::{
    TRIGGER_TYPE_AFTER, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT,
    TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_TIMING_MASK, TRIGGER_TYPE_TRUNCATE,
    TRIGGER_TYPE_UPDATE,
};

use crate::{getattr, req};
use mcx::MemoryContext;
use types_tuple::HeapTupleData;

const TRIGGER_RELATION_ID: Oid = 2620;
const TRIGGER_RELID_NAME_INDEX_ID: Oid = 2701;

const Anum_pg_trigger_oid: i32 = 1;
const Anum_pg_trigger_tgrelid: i32 = 2;
const Anum_pg_trigger_tgparentid: i32 = 3;
const Anum_pg_trigger_tgname: i32 = 4;
const Anum_pg_trigger_tgfoid: i32 = 5;
const Anum_pg_trigger_tgtype: i32 = 6;
const Anum_pg_trigger_tgenabled: i32 = 7;
const Anum_pg_trigger_tgisinternal: i32 = 8;
const Anum_pg_trigger_tgconstrrelid: i32 = 9;
const Anum_pg_trigger_tgconstrindid: i32 = 10;
const Anum_pg_trigger_tgconstraint: i32 = 11;
const Anum_pg_trigger_tgdeferrable: i32 = 12;
const Anum_pg_trigger_tginitdeferred: i32 = 13;
const Anum_pg_trigger_tgnargs: i32 = 14;
const Anum_pg_trigger_tgattr: i32 = 15;
const Anum_pg_trigger_tgargs: i32 = 16;
const Anum_pg_trigger_tgqual: i32 = 17;
const Anum_pg_trigger_tgoldtable: i32 = 18;
const Anum_pg_trigger_tgnewtable: i32 = 19;

fn trigger_type_matches(tgtype: i16, level: i16, timing: i16, event: i16) -> bool {
    tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | event)
        == level | timing | event
}

fn name_datum_str(tup: &HeapTupleData<'_>, d: datum::Datum) -> String {
    // Bound the fixed NameData read by the containing tuple image via the shared
    // helper (see crate::name_from). SQL_ASCII trigger names may be non-UTF-8;
    // match C's opaque NameData bytes with a lossy copy instead of panicking.
    let name = crate::name_from(tup, d);
    String::from_utf8_lossy(name.name_str()).into_owned()
}

pub(crate) fn build_trigger_desc(
    mcx: Mcx<'static>,
    relid: Oid,
    relname: &str,
) -> PgResult<Option<TriggerDesc<'static>>> {
    let cx = MemoryContext::new("RelationBuildTriggers");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, TRIGGER_RELATION_ID, AccessShareLock)?;
    let keys = [crate::scan_key(
        Anum_pg_trigger_tgrelid,
        types_scan::scankey::BTEqualStrategyNumber,
        F_OIDEQ,
        datum::Datum::from_oid(relid),
    )];
    let mut scan =
        genam::systable_beginscan(smcx, &rel, TRIGGER_RELID_NAME_INDEX_ID, true, None, &keys)?;
    let mut triggers: PgVec<'static, Trigger<'static>> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let td = rel.descr();
        let tgnargs = req(td, tup, Anum_pg_trigger_tgnargs)?.as_i16();
        let tgtype = req(td, tup, Anum_pg_trigger_tgtype)?.as_i16();
        let (attr_d, attr_null) = getattr(td, tup, Anum_pg_trigger_tgattr);
        if attr_null {
            return Err(corrupt(relname, "tgattr"));
        }
        // int2vector image: 24-byte 1-D array header, dim1 at offset 16.
        let (tgnattr, tgattr) = {
            // SAFETY: tgattr is an inline int2vector image (never toasted).
            let hdr = attr_d.as_usize() as *const u8;
            let dim1 = unsafe { core::ptr::read_unaligned(hdr.add(16) as *const i32) };
            let mut v: PgVec<'static, i16> = mcx::vec_with_capacity_in(mcx, dim1 as usize)?;
            for i in 0..dim1 as usize {
                v.push(unsafe {
                    core::ptr::read_unaligned(hdr.add(24 + 2 * i) as *const i16)
                });
            }
            (dim1 as i16, v)
        };
        let mut tgargs: PgVec<'static, PgString<'static>> = PgVec::new_in(mcx);
        if tgnargs > 0 {
            let (args_d, args_null) = getattr(td, tup, Anum_pg_trigger_tgargs);
            if args_null {
                return Err(corrupt(relname, "tgargs"));
            }
            let args_image = detoast_image(mcx, args_d)?;
            let bytes = &args_image[datum::varlena::VARHDRSZ..];
            let mut p = 0usize;
            for _ in 0..tgnargs {
                let end = bytes[p..]
                    .iter()
                    .position(|&b| b == 0)
                    .map(|e| p + e)
                    .unwrap_or(bytes.len());
                let s = core::str::from_utf8(&bytes[p..end]).expect("non-UTF-8 tgargs");
                tgargs.push(PgString::from_str_in(s, mcx)?);
                p = end + 1;
            }
        }
        let (qual_d, qual_null) = getattr(td, tup, Anum_pg_trigger_tgqual);
        let tgqual = if qual_null {
            None
        } else {
            let qual_image = detoast_image(mcx, qual_d)?;
            let bytes = &qual_image[datum::varlena::VARHDRSZ..];
            Some(PgString::from_str_in(
                core::str::from_utf8(bytes).expect("non-UTF-8 tgqual"),
                mcx,
            )?)
        };
        let (old_d, old_null) = getattr(td, tup, Anum_pg_trigger_tgoldtable);
        let (new_d, new_null) = getattr(td, tup, Anum_pg_trigger_tgnewtable);
        triggers.push(Trigger {
            tgoid: req(td, tup, Anum_pg_trigger_oid)?.as_oid(),
            tgname: PgString::from_str_in(
                &name_datum_str(tup, req(td, tup, Anum_pg_trigger_tgname)?),
                mcx,
            )?,
            tgfoid: req(td, tup, Anum_pg_trigger_tgfoid)?.as_oid(),
            tgtype,
            tgenabled: req(td, tup, Anum_pg_trigger_tgenabled)?.as_i8(),
            tgisinternal: req(td, tup, Anum_pg_trigger_tgisinternal)?.as_bool(),
            tgisclone: req(td, tup, Anum_pg_trigger_tgparentid)?.as_oid() != 0,
            tgconstrrelid: req(td, tup, Anum_pg_trigger_tgconstrrelid)?.as_oid(),
            tgconstrindid: req(td, tup, Anum_pg_trigger_tgconstrindid)?.as_oid(),
            tgconstraint: req(td, tup, Anum_pg_trigger_tgconstraint)?.as_oid(),
            tgdeferrable: req(td, tup, Anum_pg_trigger_tgdeferrable)?.as_bool(),
            tginitdeferred: req(td, tup, Anum_pg_trigger_tginitdeferred)?.as_bool(),
            tgnargs,
            tgnattr,
            tgattr,
            tgargs,
            tgqual,
            tgoldtable: if old_null {
                None
            } else {
                Some(PgString::from_str_in(&name_datum_str(tup, old_d), mcx)?)
            },
            tgnewtable: if new_null {
                None
            } else {
                Some(PgString::from_str_in(&name_datum_str(tup, new_d), mcx)?)
            },
        });
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;

    if triggers.is_empty() {
        return Ok(None);
    }
    let mut desc = TriggerDesc {
        triggers,
        trig_insert_before_row: false,
        trig_insert_after_row: false,
        trig_insert_instead_row: false,
        trig_insert_before_statement: false,
        trig_insert_after_statement: false,
        trig_update_before_row: false,
        trig_update_after_row: false,
        trig_update_instead_row: false,
        trig_update_before_statement: false,
        trig_update_after_statement: false,
        trig_delete_before_row: false,
        trig_delete_after_row: false,
        trig_delete_instead_row: false,
        trig_delete_before_statement: false,
        trig_delete_after_statement: false,
        trig_truncate_before_statement: false,
        trig_truncate_after_statement: false,
        trig_insert_new_table: false,
        trig_update_old_table: false,
        trig_update_new_table: false,
        trig_delete_old_table: false,
    };
    for i in 0..desc.triggers.len() {
        let t = desc.triggers[i].tgtype;
        // SetTriggerFlags (trigger.c).
        let m = |level, timing, event| trigger_type_matches(t, level, timing, event);
        let has_old = desc.triggers[i].tgoldtable.is_some();
        let has_new = desc.triggers[i].tgnewtable.is_some();
        desc.trig_insert_new_table |= t & TRIGGER_TYPE_INSERT != 0 && has_new;
        desc.trig_update_old_table |= t & TRIGGER_TYPE_UPDATE != 0 && has_old;
        desc.trig_update_new_table |= t & TRIGGER_TYPE_UPDATE != 0 && has_new;
        desc.trig_delete_old_table |= t & TRIGGER_TYPE_DELETE != 0 && has_old;
        desc.trig_insert_before_row |= m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
        desc.trig_insert_after_row |= m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
        desc.trig_insert_instead_row |=
            m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT);
        desc.trig_insert_before_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
        desc.trig_insert_after_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
        desc.trig_update_before_row |= m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
        desc.trig_update_after_row |= m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
        desc.trig_update_instead_row |=
            m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_UPDATE);
        desc.trig_update_before_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
        desc.trig_update_after_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
        desc.trig_delete_before_row |= m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
        desc.trig_delete_after_row |= m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
        desc.trig_delete_instead_row |=
            m(TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_DELETE);
        desc.trig_delete_before_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
        desc.trig_delete_after_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
        desc.trig_truncate_before_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_TRUNCATE);
        desc.trig_truncate_after_statement |=
            m(TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_TRUNCATE);
    }
    Ok(Some(desc))
}

// Detoast a pg_trigger varlena (tgargs/tgqual) and return its payload. The
// catalog writer TOASTs these for tuples over the toast threshold (compressed
// inline or out-of-line), so a raw header read is not enough — parity with C's
// DatumGetByteaPP/pg_detoast_datum. Returns the bytes after the varlena header.
fn detoast_image<'mcx>(mcx: Mcx<'mcx>, d: datum::Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum; length is taken from its own header
    // (short 1B, external 1B-tag, or long 4B) before slicing.
    let raw = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            detoast::varsize_any(core::slice::from_raw_parts(p, 2))
        } else if b0 & 0x01 != 0 {
            ((b0 >> 1) & 0x7F) as usize
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    // Fully-detoasted, decompressed image (with 4B varlena header); payload is
    // image[VARHDRSZ..]. Parity with C's DatumGetByteaPP/pg_detoast_datum.
    detoast::detoast_attr(mcx, raw)
}

// C trigger.c:1936/1950: elog(ERROR, "<field> is null in trigger for relation
// \"%s\"", RelationGetRelationName(relation)) -- the quoted relation name.
#[track_caller]
#[cold]
#[inline(never)]
pub(crate) fn corrupt(relname: &str, field: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{field} is null in trigger for relation \"{relname}\""))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}
