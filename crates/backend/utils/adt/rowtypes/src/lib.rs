//! rowtypes.c: record I/O (text + binary), the record comparison family
//! (typcache-proc and byte-image), and record hashing.
#![no_std]
extern crate alloc;

use alloc::vec::Vec;

use ::datum::Datum;
use ::mcx::{vec_from_elem_in, vec_with_capacity_in, PgVec};
use ::types_core::{InvalidOid, Oid};
use ::types_error::PgResult;
use ::types_fmgr::{
    cstring_result, function_call1_coll, function_call1_coll_in, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use ::types_tuple::{
    HeapTupleData, HeapTupleHeaderData, ItemPointerData, SizeofHeapTupleHeader,
};

struct ColumnIOData {
    column_type: Oid,
    proc: FmgrInfo,
}

// C RecordIOData fn_extra memo: per-column out procs, keyed by rowtype.
struct RecordIOData {
    record_type: Oid,
    record_typmod: i32,
    // std Vec justified: rides FmgrInfo.fn_extra, same
    // open-set slot the C fn_mcxt allocation fills.
    columns: Vec<Option<ColumnIOData>>,
}

pub fn fc_record_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 is a non-null composite datum (strict fn).
    let p = unsafe { fcinfo.arg_ptr(0) };
    // SAFETY: a live varlena-headed composite image.
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: `total` readable bytes at p, per the datum contract.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let rec = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    debug_assert!(rec.len() >= SizeofHeapTupleHeader);
    // SAFETY: detoasted composite image; header prefix is in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
    let tup_type = hdr.type_id();
    let tup_typmod = hdr.typmod();
    let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    // SAFETY: MAXALIGN'd detoasted image of datum_length() == rec.len() bytes.
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ItemPointerData::invalid(),
            InvalidOid,
        )
    };

    let flinfo = flinfo.expect("record_out: NULL flinfo");
    let refresh = match flinfo.fn_extra_ref::<RecordIOData>() {
        Some(x) => x.record_type != tup_type || x.record_typmod != tup_typmod,
        None => true,
    };
    if refresh {
        let mut columns = Vec::with_capacity(ncolumns);
        columns.resize_with(ncolumns, || None);
        flinfo.set_fn_extra(RecordIOData {
            record_type: tup_type,
            record_typmod: tup_typmod,
            columns,
        });
    }

    let mut values: PgVec<'_, Datum> = vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'_, bool> = vec_from_elem_in(mcx, true, ncolumns);
    ::types_tuple::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut nulls);

    let mut buf: PgVec<'_, u8> = vec_with_capacity_in(mcx, 64)?;
    buf.push(b'(');
    let mut need_comma = false;
    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        if att.attisdropped {
            continue;
        }
        if need_comma {
            ::mcx::vec_append_bytes(&mut buf, b",")?;
        }
        need_comma = true;
        if nulls[i] {
            continue;
        }
        let column_type = att.atttypid;
        let my_extra = flinfo.fn_extra_mut::<RecordIOData>().unwrap();
        let stale = match &my_extra.columns[i] {
            Some(c) => c.column_type != column_type,
            None => true,
        };
        if stale {
            let (typiofunc, _typisvarlena) = ::lsyscache::getTypeOutputInfo(column_type)?;
            let proc = ::fmgr_seams::fmgr_info::call(typiofunc)?;
            my_extra.columns[i] = Some(ColumnIOData { column_type, proc });
        }
        let proc =
            &mut flinfo.fn_extra_mut::<RecordIOData>().unwrap().columns[i].as_mut().unwrap().proc;
        let d = function_call1_coll_in(proc, InvalidOid, mcx, values[i])?;
        let value = cstring_bytes(d);
        let nq = value.is_empty()
            || value.iter().any(|&ch| {
                ch == b'"'
                    || ch == b'\\'
                    || ch == b'('
                    || ch == b')'
                    || ch == b','
                    || ch.is_ascii_whitespace()
                    || ch == 0x0b
            });
        let extra = 2 * value.len() + 2;
        buf.try_reserve(extra).map_err(|_| mcx.oom(extra))?;
        if nq {
            buf.push(b'"');
        }
        for &ch in value {
            if ch == b'"' || ch == b'\\' {
                buf.push(ch);
            }
            buf.push(ch);
        }
        if nq {
            buf.push(b'"');
        }
    }
    ::mcx::vec_append_bytes(&mut buf, b")\0")?;
    Ok(cstring_result(buf))
}

#[inline]
fn cstring_bytes<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let mut n = 0usize;
    // SAFETY: p is a NUL-terminated cstring returned by an output function.
    unsafe {
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p, n)
    }
}

struct ColumnHashData {
    column_type: Oid,
    proc: FmgrInfo,
}

// C RecordCompareData memo for hash_record{_extended}, keyed by rowtype.
struct RecordHashData {
    record_type: Oid,
    record_typmod: i32,
    extended: bool,
    columns: Vec<Option<ColumnHashData>>,
}

fn hash_record_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    seed: Option<Datum>,
) -> PgResult<u64> {
    ::stack_depth::check_stack_depth()?;
    let extended = seed.is_some();
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 is a non-null composite datum (strict fn).
    let p = unsafe { fcinfo.arg_ptr(0) };
    // SAFETY: a live varlena-headed composite image.
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: `total` readable bytes at p, per the datum contract.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let rec = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    // SAFETY: detoasted composite image; header prefix is in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
    let tup_type = hdr.type_id();
    let tup_typmod = hdr.typmod();
    let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    // SAFETY: MAXALIGN'd detoasted image of datum_length() == rec.len() bytes.
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ItemPointerData::invalid(),
            InvalidOid,
        )
    };

    let flinfo = flinfo.expect("hash_record: NULL flinfo");
    let refresh = match flinfo.fn_extra_ref::<RecordHashData>() {
        Some(x) => {
            x.record_type != tup_type || x.record_typmod != tup_typmod || x.extended != extended
        }
        None => true,
    };
    if refresh {
        let mut columns = Vec::with_capacity(ncolumns);
        columns.resize_with(ncolumns, || None);
        flinfo.set_fn_extra(RecordHashData {
            record_type: tup_type,
            record_typmod: tup_typmod,
            extended,
            columns,
        });
    }

    let mut values: PgVec<'_, Datum> = vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'_, bool> = vec_from_elem_in(mcx, true, ncolumns);
    ::types_tuple::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut nulls);

    let mut result: u64 = 0;
    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        if att.attisdropped {
            continue;
        }
        let column_type = att.atttypid;
        let my_extra = flinfo.fn_extra_mut::<RecordHashData>().unwrap();
        let stale = match &my_extra.columns[i] {
            Some(c) => c.column_type != column_type,
            None => true,
        };
        if stale {
            let flags = if extended {
                ::typcache::TYPECACHE_HASH_EXTENDED_PROC_FINFO
            } else {
                ::typcache::TYPECACHE_HASH_PROC_FINFO
            };
            let tc = ::typcache::lookup_type_cache(column_type, flags)?;
            let proc = if extended {
                tc.hash_extended_proc_finfo().clone()
            } else {
                tc.hash_proc_finfo().clone()
            };
            if proc.fn_oid == InvalidOid {
                let kind = if extended { "an extended hash" } else { "a hash" };
                let name = ::format_type::format_type_be(column_type)?;
                return Err(alloc::boxed::Box::new(
                    ::types_error::PgError::error(alloc::format!(
                        "could not identify {kind} function for type {name}"
                    ))
                    .with_sqlstate(::types_error::ERRCODE_UNDEFINED_FUNCTION),
                ));
            }
            my_extra.columns[i] = Some(ColumnHashData { column_type, proc });
        }
        let element_hash: u64 = if nulls[i] {
            0
        } else {
            let proc = &mut flinfo.fn_extra_mut::<RecordHashData>().unwrap().columns[i]
                .as_mut()
                .unwrap()
                .proc;
            match seed {
                Some(s) => {
                    ::types_fmgr::function_call2_coll(proc, att.attcollation, values[i], s)?
                        .as_u64()
                }
                None => {
                    function_call1_coll(proc, att.attcollation, values[i])?.as_u32() as u64
                }
            }
        };
        result = (result << 5).wrapping_sub(result).wrapping_add(element_hash);
    }
    Ok(result)
}

pub fn fc_hash_record(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let h = hash_record_common(flinfo, fcinfo, None)?;
    Ok(Datum::from_u32(h as u32))
}

pub fn fc_hash_record_extended(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let seed = fcinfo.arg(1);
    let h = hash_record_common(flinfo, fcinfo, Some(seed))?;
    Ok(Datum::from_u64(h))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const ROWTYPES_BUILTINS: &[FmgrBuiltin] = &[
    b(2290, "record_in", 3, fc_record_in),
    b(2402, "record_recv", 3, fc_record_recv),
    b(2403, "record_send", 1, fc_record_send),
    b(2291, "record_out", 1, fc_record_out),
    b(2981, "record_eq", 2, fc_record_eq),
    b(2982, "record_ne", 2, fc_record_ne),
    b(2983, "record_lt", 2, fc_record_lt),
    b(2984, "record_gt", 2, fc_record_gt),
    b(2985, "record_le", 2, fc_record_le),
    b(2986, "record_ge", 2, fc_record_ge),
    b(2987, "btrecordcmp", 2, fc_btrecordcmp),
    b(3181, "record_image_eq", 2, fc_record_image_eq),
    b(3182, "record_image_ne", 2, fc_record_image_ne),
    b(3183, "record_image_lt", 2, fc_record_image_lt),
    b(3184, "record_image_gt", 2, fc_record_image_gt),
    b(3185, "record_image_le", 2, fc_record_image_le),
    b(3186, "record_image_ge", 2, fc_record_image_ge),
    b(3187, "btrecordimagecmp", 2, fc_btrecordimagecmp),
    b(6192, "hash_record", 1, fc_hash_record),
    b(6193, "hash_record_extended", 2, fc_hash_record_extended),
    b(6375, "record_larger", 2, fc_record_larger),
    b(6376, "record_smaller", 2, fc_record_smaller),
];

use alloc::rc::Rc;

struct DeformedRec<'mcx> {
    tup_type: Oid,
    tup_typmod: i32,
    tupdesc: types_tuple::TupleDescData<'mcx>,
    values: PgVec<'mcx, Datum>,
    nulls: PgVec<'mcx, bool>,
    // By-ref values point into the detoasted image; dropping it early is a
    // use-after-free the free-list mcxs recycle immediately (sort comparator
    // bug class).
    _rec: PgVec<'mcx, u8>,
}

fn deform_record<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    fcinfo: &Fcinfo,
    argno: usize,
) -> PgResult<DeformedRec<'mcx>> {
    // SAFETY: catalog arg is a non-null composite datum (strict fn).
    let p = unsafe { fcinfo.arg_ptr(argno) };
    // SAFETY: a live varlena-headed composite image.
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: `total` readable bytes at p, per the datum contract.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let rec = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    debug_assert!(rec.len() >= SizeofHeapTupleHeader);
    // SAFETY: detoasted composite image; header prefix is in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
    let tup_type = hdr.type_id();
    let tup_typmod = hdr.typmod();
    let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;
    // SAFETY: MAXALIGN'd detoasted image of datum_length() == rec.len() bytes.
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ItemPointerData::invalid(),
            InvalidOid,
        )
    };
    let mut values: PgVec<'mcx, Datum> = vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'mcx, bool> = vec_from_elem_in(mcx, true, ncolumns);
    ::types_tuple::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut nulls);
    Ok(DeformedRec { tup_type, tup_typmod, tupdesc, values, nulls, _rec: rec })
}

// C RecordCompareData fn_extra memo: per-logical-column typcache entries.
struct RecordCompareData {
    record1_type: Oid,
    record1_typmod: i32,
    record2_type: Oid,
    record2_typmod: i32,
    // std Vec justified: rides FmgrInfo.fn_extra, the same
    // open-set slot the C fn_mcxt allocation fills.
    columns: Vec<Option<Rc<::typcache::TypeCacheEntry>>>,
}

fn compare_memo<'a>(
    flinfo: &'a mut FmgrInfo,
    r1: &DeformedRec<'_>,
    r2: &DeformedRec<'_>,
) -> &'a mut RecordCompareData {
    let ncols = r1.values.len().max(r2.values.len());
    let refresh = match flinfo.fn_extra_ref::<RecordCompareData>() {
        Some(x) => x.columns.len() < ncols,
        None => true,
    };
    if refresh {
        let mut columns = Vec::with_capacity(ncols);
        columns.resize_with(ncols, || None);
        flinfo.set_fn_extra(RecordCompareData {
            record1_type: InvalidOid,
            record1_typmod: 0,
            record2_type: InvalidOid,
            record2_typmod: 0,
            columns,
        });
    }
    let m = flinfo.fn_extra_mut::<RecordCompareData>().unwrap();
    if m.record1_type != r1.tup_type
        || m.record1_typmod != r1.tup_typmod
        || m.record2_type != r2.tup_type
        || m.record2_typmod != r2.tup_typmod
    {
        for c in &mut m.columns {
            *c = None;
        }
        m.record1_type = r1.tup_type;
        m.record1_typmod = r1.tup_typmod;
        m.record2_type = r2.tup_type;
        m.record2_typmod = r2.tup_typmod;
    }
    m
}

#[cold]
fn dissimilar_columns(t1: Oid, t2: Oid, j: usize) -> alloc::boxed::Box<::types_error::PgError> {
    let n1 = ::format_type::format_type_be(t1).unwrap_or_default();
    let n2 = ::format_type::format_type_be(t2).unwrap_or_default();
    alloc::boxed::Box::new(
        ::types_error::PgError::error(alloc::format!(
            "cannot compare dissimilar column types {n1} and {n2} at record column {}",
            j + 1
        ))
        .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

#[cold]
fn column_count_mismatch() -> alloc::boxed::Box<::types_error::PgError> {
    alloc::boxed::Box::new(
        ::types_error::PgError::error(
            "cannot compare record types with different numbers of columns",
        )
        .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

#[cold]
fn no_support_fn(kind: &str, typ: Oid) -> alloc::boxed::Box<::types_error::PgError> {
    let n = ::format_type::format_type_be(typ).unwrap_or_default();
    alloc::boxed::Box::new(
        ::types_error::PgError::error(alloc::format!(
            "could not identify {kind} for type {n}"
        ))
        .with_sqlstate(::types_error::ERRCODE_UNDEFINED_FUNCTION),
    )
}

fn record_cmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<i32> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    let r1 = deform_record(mcx, fcinfo, 0)?;
    let r2 = deform_record(mcx, fcinfo, 1)?;
    let flinfo = flinfo.expect("record_cmp: NULL flinfo");
    compare_memo(flinfo, &r1, &r2);

    let (n1, n2) = (r1.values.len(), r2.values.len());
    let (mut i1, mut i2, mut j) = (0usize, 0usize, 0usize);
    let mut result = 0i32;
    while i1 < n1 || i2 < n2 {
        if i1 < n1 && r1.tupdesc.attrs[i1].attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < n2 && r2.tupdesc.attrs[i2].attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= n1 || i2 >= n2 {
            break;
        }
        let att1 = &r1.tupdesc.attrs[i1];
        let att2 = &r2.tupdesc.attrs[i2];
        if att1.atttypid != att2.atttypid {
            return Err(dissimilar_columns(att1.atttypid, att2.atttypid, j));
        }
        let collation =
            if att1.attcollation == att2.attcollation { att1.attcollation } else { InvalidOid };

        let m = flinfo.fn_extra_mut::<RecordCompareData>().unwrap();
        let stale = match &m.columns[j] {
            Some(e) => e.type_id != att1.atttypid,
            None => true,
        };
        if stale {
            let e = ::typcache::lookup_type_cache(
                att1.atttypid,
                ::typcache::TYPECACHE_CMP_PROC_FINFO,
            )?;
            if e.cmp_proc_finfo().fn_oid == InvalidOid {
                return Err(no_support_fn("a comparison function", e.type_id));
            }
            flinfo.fn_extra_mut::<RecordCompareData>().unwrap().columns[j] = Some(e);
        }

        if !r1.nulls[i1] || !r2.nulls[i2] {
            if r1.nulls[i1] {
                result = 1;
                break;
            }
            if r2.nulls[i2] {
                result = -1;
                break;
            }
            let e = flinfo.fn_extra_ref::<RecordCompareData>().unwrap().columns[j]
                .clone()
                .unwrap();
            let mut finfo = e.cmp_proc_finfo();
            let d = ::types_fmgr::function_call2_coll_in(
                &mut finfo,
                collation,
                mcx,
                r1.values[i1],
                r2.values[i2],
            )?;
            let cmpresult = d.as_i32();
            if cmpresult < 0 {
                result = -1;
                break;
            }
            if cmpresult > 0 {
                result = 1;
                break;
            }
        }
        i1 += 1;
        i2 += 1;
        j += 1;
    }
    if result == 0 && (i1 != n1 || i2 != n2) {
        return Err(column_count_mismatch());
    }
    Ok(result)
}

pub fn fc_record_eq(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    let r1 = deform_record(mcx, fcinfo, 0)?;
    let r2 = deform_record(mcx, fcinfo, 1)?;
    let flinfo = flinfo.expect("record_eq: NULL flinfo");
    compare_memo(flinfo, &r1, &r2);

    let (n1, n2) = (r1.values.len(), r2.values.len());
    let (mut i1, mut i2, mut j) = (0usize, 0usize, 0usize);
    let mut result = true;
    while i1 < n1 || i2 < n2 {
        if i1 < n1 && r1.tupdesc.attrs[i1].attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < n2 && r2.tupdesc.attrs[i2].attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= n1 || i2 >= n2 {
            break;
        }
        let att1 = &r1.tupdesc.attrs[i1];
        let att2 = &r2.tupdesc.attrs[i2];
        if att1.atttypid != att2.atttypid {
            return Err(dissimilar_columns(att1.atttypid, att2.atttypid, j));
        }
        let collation =
            if att1.attcollation == att2.attcollation { att1.attcollation } else { InvalidOid };

        let m = flinfo.fn_extra_mut::<RecordCompareData>().unwrap();
        let stale = match &m.columns[j] {
            Some(e) => e.type_id != att1.atttypid,
            None => true,
        };
        if stale {
            let e = ::typcache::lookup_type_cache(
                att1.atttypid,
                ::typcache::TYPECACHE_EQ_OPR_FINFO,
            )?;
            if e.eq_opr_finfo().fn_oid == InvalidOid {
                return Err(no_support_fn("an equality operator", e.type_id));
            }
            flinfo.fn_extra_mut::<RecordCompareData>().unwrap().columns[j] = Some(e);
        }

        if !r1.nulls[i1] || !r2.nulls[i2] {
            if r1.nulls[i1] || r2.nulls[i2] {
                result = false;
                break;
            }
            let e = flinfo.fn_extra_ref::<RecordCompareData>().unwrap().columns[j]
                .clone()
                .unwrap();
            let mut finfo = e.eq_opr_finfo();
            let d = ::types_fmgr::function_call2_coll_in(
                &mut finfo,
                collation,
                mcx,
                r1.values[i1],
                r2.values[i2],
            )?;
            if !d.as_bool() {
                result = false;
                break;
            }
        }
        i1 += 1;
        i2 += 1;
        j += 1;
    }
    if result && (i1 != n1 || i2 != n2) {
        return Err(column_count_mismatch());
    }
    Ok(Datum::from_bool(result))
}

pub fn fc_record_ne(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!fc_record_eq(flinfo, fcinfo)?.as_bool()))
}

pub fn fc_record_lt(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(record_cmp(flinfo, fcinfo)? < 0))
}

pub fn fc_record_gt(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(record_cmp(flinfo, fcinfo)? > 0))
}

pub fn fc_record_le(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(record_cmp(flinfo, fcinfo)? <= 0))
}

pub fn fc_record_ge(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(record_cmp(flinfo, fcinfo)? >= 0))
}

pub fn fc_btrecordcmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(record_cmp(flinfo, fcinfo)?))
}

pub fn fc_record_larger(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let c = record_cmp(flinfo, fcinfo)?;
    Ok(fcinfo.arg(if c > 0 { 0 } else { 1 }))
}

pub fn fc_record_smaller(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let c = record_cmp(flinfo, fcinfo)?;
    Ok(fcinfo.arg(if c < 0 { 0 } else { 1 }))
}

// record_image_cmp/btrecordimagecmp (rowtypes.c). Byval columns compare as
// full Datum words, varlena as raw payloads with shorter-sorts-first — C's
// exact image order. C memoizes RecordCompareData in fn_extra; the image
// comparison reads only tupdesc attbyval/attlen, so there is nothing to
// memoize here.
pub fn fc_btrecordimagecmp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    let r1 = deform_record(mcx, fcinfo, 0)?;
    let r2 = deform_record(mcx, fcinfo, 1)?;
    let (n1, n2) = (r1.values.len(), r2.values.len());

    let mut result: i32 = 0;
    let (mut i1, mut i2, mut j) = (0usize, 0usize, 0usize);
    while i1 < n1 || i2 < n2 {
        if i1 < n1 && r1.tupdesc.attrs[i1].attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < n2 && r2.tupdesc.attrs[i2].attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= n1 || i2 >= n2 {
            break;
        }
        let att1 = &r1.tupdesc.attrs[i1];
        let att2 = &r2.tupdesc.attrs[i2];
        if att1.atttypid != att2.atttypid {
            return Err(dissimilar_columns(att1.atttypid, att2.atttypid, j));
        }
        if !r1.nulls[i1] || !r2.nulls[i2] {
            if r1.nulls[i1] {
                result = 1;
                break;
            }
            if r2.nulls[i2] {
                result = -1;
                break;
            }
            let cmp =
                datum_image_cmp(mcx, r1.values[i1], r2.values[i2], att1.attbyval, att1.attlen)?;
            if cmp != 0 {
                result = cmp;
                break;
            }
        }
        i1 += 1;
        i2 += 1;
        j += 1;
    }

    if result == 0 && (i1 != n1 || i2 != n2) {
        return Err(column_count_mismatch());
    }
    Ok(Datum::from_i32(result))
}

fn datum_image_cmp<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    a: Datum,
    b: Datum,
    attbyval: bool,
    attlen: i16,
) -> PgResult<i32> {
    if attbyval {
        let (x, y) = (a.as_usize() as u64, b.as_usize() as u64);
        return Ok(if x == y {
            0
        } else if x < y {
            -1
        } else {
            1
        });
    }
    let pa = a.as_usize() as *const u8;
    let pb = b.as_usize() as *const u8;
    if attlen > 0 {
        // SAFETY: by-ref fixed-length datums of attlen bytes.
        let (sa, sb) = unsafe {
            (
                core::slice::from_raw_parts(pa, attlen as usize),
                core::slice::from_raw_parts(pb, attlen as usize),
            )
        };
        return Ok(match sa.cmp(sb) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        });
    }
    if attlen == -1 {
        // SAFETY: live varlena datums; detoast_attr bounds the payloads.
        let (ra, rb) = unsafe {
            let ta = ::types_tuple::varatt::varsize_any(pa);
            let tb = ::types_tuple::varatt::varsize_any(pb);
            (
                ::detoast_seams::detoast_attr::call(mcx, core::slice::from_raw_parts(pa, ta))?,
                ::detoast_seams::detoast_attr::call(mcx, core::slice::from_raw_parts(pb, tb))?,
            )
        };
        let (da, db) = (varlena_payload(&ra), varlena_payload(&rb));
        let n = core::cmp::min(da.len(), db.len());
        let cmp = da[..n].cmp(&db[..n]);
        return Ok(match cmp {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Greater => 1,
            core::cmp::Ordering::Equal => {
                if da.len() == db.len() {
                    0
                } else if da.len() < db.len() {
                    -1
                } else {
                    1
                }
            }
        });
    }
    debug_assert!(attlen == -2);
    Ok(match cstring_bytes(a).cmp(cstring_bytes(b)) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    })
}

// record_image_eq (rowtypes.c:1595) with C's datum_image_eq semantics.
pub fn fc_record_image_eq(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    let r1 = deform_record(mcx, fcinfo, 0)?;
    let r2 = deform_record(mcx, fcinfo, 1)?;
    let (n1, n2) = (r1.values.len(), r2.values.len());

    let mut result = true;
    let (mut i1, mut i2, mut j) = (0usize, 0usize, 0usize);
    while i1 < n1 || i2 < n2 {
        if i1 < n1 && r1.tupdesc.attrs[i1].attisdropped {
            i1 += 1;
            continue;
        }
        if i2 < n2 && r2.tupdesc.attrs[i2].attisdropped {
            i2 += 1;
            continue;
        }
        if i1 >= n1 || i2 >= n2 {
            break;
        }
        let att1 = &r1.tupdesc.attrs[i1];
        let att2 = &r2.tupdesc.attrs[i2];
        if att1.atttypid != att2.atttypid {
            return Err(dissimilar_columns(att1.atttypid, att2.atttypid, j));
        }
        if !r1.nulls[i1] || !r2.nulls[i2] {
            if r1.nulls[i1] || r2.nulls[i2] {
                result = false;
                break;
            }
            result =
                datum_image_eq(mcx, r1.values[i1], r2.values[i2], att1.attbyval, att1.attlen)?;
            if !result {
                break;
            }
        }
        i1 += 1;
        i2 += 1;
        j += 1;
    }

    if result && (i1 != n1 || i2 != n2) {
        return Err(column_count_mismatch());
    }
    Ok(Datum::from_bool(result))
}

// datum_image_eq (datum.c): binary-image equality; varlena payloads compare
// post-detoast, header form ignored.
fn datum_image_eq<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    a: Datum,
    b: Datum,
    attbyval: bool,
    attlen: i16,
) -> PgResult<bool> {
    if attbyval {
        let (x, y) = (a.as_usize() as u64, b.as_usize() as u64);
        return Ok(match attlen {
            1 => x as u8 == y as u8,
            2 => x as u16 == y as u16,
            4 => x as u32 == y as u32,
            _ => x == y,
        });
    }
    let pa = a.as_usize() as *const u8;
    let pb = b.as_usize() as *const u8;
    if attlen > 0 {
        // SAFETY: by-ref fixed-length datums of attlen bytes.
        return Ok(unsafe {
            core::slice::from_raw_parts(pa, attlen as usize)
                == core::slice::from_raw_parts(pb, attlen as usize)
        });
    }
    if attlen == -1 {
        // SAFETY: live varlena datums; detoast_attr bounds the payloads.
        let (ra, rb) = unsafe {
            let ta = ::types_tuple::varatt::varsize_any(pa);
            let tb = ::types_tuple::varatt::varsize_any(pb);
            (
                ::detoast_seams::detoast_attr::call(mcx, core::slice::from_raw_parts(pa, ta))?,
                ::detoast_seams::detoast_attr::call(mcx, core::slice::from_raw_parts(pb, tb))?,
            )
        };
        return Ok(varlena_payload(&ra) == varlena_payload(&rb));
    }
    debug_assert!(attlen == -2);
    Ok(cstring_bytes(a) == cstring_bytes(b))
}

pub fn fc_record_image_ne(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(!fc_record_image_eq(flinfo, fcinfo)?.as_bool()))
}

pub fn fc_record_image_lt(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(fc_btrecordimagecmp(flinfo, fcinfo)?.as_i32() < 0))
}

pub fn fc_record_image_gt(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(fc_btrecordimagecmp(flinfo, fcinfo)?.as_i32() > 0))
}

pub fn fc_record_image_le(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(fc_btrecordimagecmp(flinfo, fcinfo)?.as_i32() <= 0))
}

pub fn fc_record_image_ge(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(fc_btrecordimagecmp(flinfo, fcinfo)?.as_i32() >= 0))
}

fn varlena_payload(rec: &[u8]) -> &[u8] {
    if rec[0] & 0x01 != 0 {
        &rec[1..]
    } else {
        &rec[4..]
    }
}

// C RecordIOData input side: per-column in procs + typioparam.
struct ColumnInData {
    column_type: Oid,
    typioparam: Oid,
    proc: FmgrInfo,
}

struct RecordInData {
    record_type: Oid,
    record_typmod: i32,
    // std Vec justified: rides FmgrInfo.fn_extra.
    columns: Vec<Option<ColumnInData>>,
}

#[cold]
fn malformed_record<'a>(
    escontext: Option<&mut ::types_fmgr::ErrorSaveNode>,
    string: &str,
    detail: &str,
) -> PgResult<Option<Datum>> {
    let err = ::types_error::PgError::error(alloc::format!(
        "malformed record literal: \"{string}\""
    ))
    .with_detail(detail)
    .with_sqlstate(::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
    match escontext {
        Some(node) => {
            if node.ctx.details_wanted() {
                node.ctx.save(err);
            } else {
                node.ctx.mark_error_occurred();
            }
            Ok(None)
        }
        None => Err(alloc::boxed::Box::new(err)),
    }
}

pub fn fc_record_in(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 of record_in is a non-null cstring.
    let string = unsafe { fcinfo.arg_cstring(0) };
    let tup_type = fcinfo.arg(1).as_oid();
    let tup_typmod = fcinfo.arg(2).as_i32();
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let mut escontext = unsafe { fcinfo.error_save_node() };

    if tup_type == types_core::catalog::RECORDOID && tup_typmod < 0 {
        let err = ::types_error::PgError::error(
            "input of anonymous composite types is not implemented",
        )
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        return match escontext {
            Some(node) => {
                if node.ctx.details_wanted() {
                    node.ctx.save(err);
                } else {
                    node.ctx.mark_error_occurred();
                }
                Ok(Datum::null())
            }
            None => Err(alloc::boxed::Box::new(err)),
        };
    }

    let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    let flinfo = flinfo.expect("record_in: NULL flinfo");
    let refresh = match flinfo.fn_extra_ref::<RecordInData>() {
        Some(x) => {
            x.record_type != tup_type
                || x.record_typmod != tup_typmod
                || x.columns.len() != ncolumns
        }
        None => true,
    };
    if refresh {
        let mut columns = Vec::with_capacity(ncolumns);
        columns.resize_with(ncolumns, || None);
        flinfo.set_fn_extra(RecordInData {
            record_type: tup_type,
            record_typmod: tup_typmod,
            columns,
        });
    }

    let bytes = string.to_bytes();
    let sdisplay = alloc::string::String::from_utf8_lossy(bytes);
    let mut pos = 0usize;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if pos >= bytes.len() || bytes[pos] != b'(' {
        return Ok(malformed_record(escontext, &sdisplay, "Missing left parenthesis.")?
            .unwrap_or(Datum::null()));
    }
    pos += 1;

    let mut values: PgVec<'_, Datum> = vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'_, bool> = vec_from_elem_in(mcx, true, ncolumns);
    let mut buf: Vec<u8> = Vec::new();
    let mut need_comma = false;

    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        if att.attisdropped {
            values[i] = Datum::null();
            nulls[i] = true;
            continue;
        }
        if need_comma {
            if pos < bytes.len() && bytes[pos] == b',' {
                pos += 1;
            } else {
                return Ok(malformed_record(escontext, &sdisplay, "Too few columns.")?
                    .unwrap_or(Datum::null()));
            }
        }
        let column_data: Option<&[u8]>;
        if pos < bytes.len() && (bytes[pos] == b',' || bytes[pos] == b')') {
            column_data = None;
            nulls[i] = true;
        } else {
            let mut inquote = false;
            buf.clear();
            loop {
                if pos >= bytes.len() {
                    return Ok(malformed_record(
                        escontext,
                        &sdisplay,
                        "Unexpected end of input.",
                    )?
                    .unwrap_or(Datum::null()));
                }
                let ch = bytes[pos];
                if !inquote && (ch == b',' || ch == b')') {
                    break;
                }
                pos += 1;
                if ch == b'\\' {
                    if pos >= bytes.len() {
                        return Ok(malformed_record(
                            escontext,
                            &sdisplay,
                            "Unexpected end of input.",
                        )?
                        .unwrap_or(Datum::null()));
                    }
                    buf.push(bytes[pos]);
                    pos += 1;
                } else if ch == b'"' {
                    if !inquote {
                        inquote = true;
                    } else if pos < bytes.len() && bytes[pos] == b'"' {
                        buf.push(b'"');
                        pos += 1;
                    } else {
                        inquote = false;
                    }
                } else {
                    buf.push(ch);
                }
            }
            column_data = Some(&buf);
            nulls[i] = false;
        }

        let column_type = att.atttypid;
        let my = flinfo.fn_extra_mut::<RecordInData>().unwrap();
        let stale = match &my.columns[i] {
            Some(c) => c.column_type != column_type,
            None => true,
        };
        if stale {
            let (typiofunc, typioparam) = ::lsyscache::getTypeInputInfo(column_type)?;
            let proc = ::fmgr_seams::fmgr_info::call(typiofunc)?;
            my.columns[i] = Some(ColumnInData { column_type, typioparam, proc });
        }
        let col = flinfo.fn_extra_mut::<RecordInData>().unwrap().columns[i].as_mut().unwrap();
        // The de-quoted bytes need a NUL for the cstring-taking in proc.
        let cstr_storage;
        let cstr: Option<&core::ffi::CStr> = match column_data {
            Some(d) => {
                let mut v = Vec::with_capacity(d.len() + 1);
                v.extend_from_slice(d);
                v.push(0);
                cstr_storage = v;
                Some(
                    core::ffi::CStr::from_bytes_with_nul(&cstr_storage)
                        .map_err(|_| embedded_nul())?,
                )
            }
            None => None,
        };
        let mut out = Datum::null();
        let ok = ::types_fmgr::input_function_call_safe(
            &mut col.proc,
            cstr,
            col.typioparam,
            att.atttypmod,
            mcx,
            escontext.as_deref_mut(),
            &mut out,
        )?;
        if !ok {
            return Ok(Datum::null());
        }
        values[i] = out;
        need_comma = true;
    }

    if pos >= bytes.len() || bytes[pos] != b')' {
        return Ok(malformed_record(escontext, &sdisplay, "Too many columns.")?
            .unwrap_or(Datum::null()));
    }
    pos += 1;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if pos < bytes.len() {
        return Ok(malformed_record(escontext, &sdisplay, "Junk after right parenthesis.")?
            .unwrap_or(Datum::null()));
    }

    let tuple = ::heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    let d = Datum::from_usize(tuple.image().as_ptr() as usize);
    core::mem::forget(tuple);
    Ok(d)
}

#[cold]
fn embedded_nul() -> alloc::boxed::Box<::types_error::PgError> {
    alloc::boxed::Box::new(
        ::types_error::PgError::error("invalid byte sequence in record literal")
            .with_sqlstate(::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

struct ColumnRecvData {
    column_type: Oid,
    typioparam: Oid,
    proc: FmgrInfo,
}

struct RecordRecvData {
    record_type: Oid,
    record_typmod: i32,
    // std Vec justified: rides FmgrInfo.fn_extra.
    columns: Vec<Option<ColumnRecvData>>,
}

pub fn fc_record_recv(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    // SAFETY: arg 0 of a recv function is a live &mut StringInfo pointer.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut ::stringinfo::StringInfo<'_>) };
    let tup_type = fcinfo.arg(1).as_oid();
    let tup_typmod = fcinfo.arg(2).as_i32();

    if tup_type == types_core::catalog::RECORDOID && tup_typmod < 0 {
        return Err(alloc::boxed::Box::new(
            ::types_error::PgError::error(
                "input of anonymous composite types is not implemented",
            )
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    let flinfo = flinfo.expect("record_recv: NULL flinfo");
    let refresh = match flinfo.fn_extra_ref::<RecordRecvData>() {
        Some(x) => {
            x.record_type != tup_type
                || x.record_typmod != tup_typmod
                || x.columns.len() != ncolumns
        }
        None => true,
    };
    if refresh {
        let mut columns = Vec::with_capacity(ncolumns);
        columns.resize_with(ncolumns, || None);
        flinfo.set_fn_extra(RecordRecvData {
            record_type: tup_type,
            record_typmod: tup_typmod,
            columns,
        });
    }

    let mut values: PgVec<'_, Datum> = vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'_, bool> = vec_from_elem_in(mcx, true, ncolumns);

    let usercols = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    let validcols =
        (0..ncolumns).filter(|&i| !tupdesc.attrs[i].attisdropped).count() as i32;
    if usercols != validcols {
        return Err(alloc::boxed::Box::new(
            ::types_error::PgError::error(alloc::format!(
                "wrong number of columns: {usercols}, expected {validcols}"
            ))
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
        ));
    }

    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        if att.attisdropped {
            values[i] = Datum::null();
            nulls[i] = true;
            continue;
        }
        let column_type = att.atttypid;
        let coltypoid = ::pqformat::pq_getmsgint(buf, 4)? as Oid;
        const FIRST_GENBKI: Oid = types_core::catalog::FirstGenbkiObjectId;
        if coltypoid != column_type && coltypoid < FIRST_GENBKI && column_type < FIRST_GENBKI {
            let n1 = ::format_type::format_type_be(coltypoid).unwrap_or_default();
            let n2 = ::format_type::format_type_be(column_type).unwrap_or_default();
            return Err(alloc::boxed::Box::new(
                ::types_error::PgError::error(alloc::format!(
                    "binary data has type {coltypoid} ({n1}) instead of expected \
                     {column_type} ({n2}) in record column {}",
                    i + 1
                ))
                .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
            ));
        }
        let itemlen = ::pqformat::pq_getmsgint(buf, 4)? as i32;
        if itemlen < -1 || itemlen > (buf.len() - buf.cursor) as i32 {
            return Err(alloc::boxed::Box::new(
                ::types_error::PgError::error("insufficient data left in message")
                    .with_sqlstate(::types_error::ERRCODE_INVALID_BINARY_REPRESENTATION),
            ));
        }

        let my = flinfo.fn_extra_mut::<RecordRecvData>().unwrap();
        let stale = match &my.columns[i] {
            Some(c) => c.column_type != column_type,
            None => true,
        };
        if stale {
            let (typiofunc, typioparam) = ::lsyscache::getTypeBinaryInputInfo(column_type)?;
            let proc = ::fmgr_seams::fmgr_info::call(typiofunc)?;
            my.columns[i] = Some(ColumnRecvData { column_type, typioparam, proc });
        }
        let col = flinfo.fn_extra_mut::<RecordRecvData>().unwrap().columns[i].as_mut().unwrap();

        if itemlen == -1 {
            values[i] =
                ::types_fmgr::receive_function_call(&mut col.proc, None, col.typioparam, att.atttypmod, mcx)?;
            nulls[i] = true;
            continue;
        }
        let mut item_buf = ::stringinfo::StringInfo::with_capacity_in(mcx, itemlen as usize + 1)?;
        {
            let slice = ::pqformat::pq_getmsgbytes(buf, itemlen as usize)?;
            item_buf.append_bytes(slice)?;
        }
        values[i] = ::types_fmgr::receive_function_call(
            &mut col.proc,
            Some(&mut item_buf),
            col.typioparam,
            att.atttypmod,
            mcx,
        )?;
        nulls[i] = false;
        if item_buf.cursor != itemlen as usize {
            return Err(alloc::boxed::Box::new(
                ::types_error::PgError::error(alloc::format!(
                    "improper binary format in record column {}",
                    i + 1
                ))
                .with_sqlstate(::types_error::ERRCODE_INVALID_BINARY_REPRESENTATION),
            ));
        }
    }

    let tuple = ::heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    let d = Datum::from_usize(tuple.image().as_ptr() as usize);
    core::mem::forget(tuple);
    Ok(d)
}

pub fn fc_record_send(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ::stack_depth::check_stack_depth()?;
    let mcx = fcinfo.result_mcx();
    let r = deform_record(mcx, fcinfo, 0)?;
    let ncolumns = r.values.len();

    let flinfo = flinfo.expect("record_send: NULL flinfo");
    let refresh = match flinfo.fn_extra_ref::<RecordRecvData>() {
        Some(x) => {
            x.record_type != r.tup_type
                || x.record_typmod != r.tup_typmod
                || x.columns.len() != ncolumns
        }
        None => true,
    };
    if refresh {
        let mut columns = Vec::with_capacity(ncolumns);
        columns.resize_with(ncolumns, || None);
        flinfo.set_fn_extra(RecordRecvData {
            record_type: r.tup_type,
            record_typmod: r.tup_typmod,
            columns,
        });
    }

    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    let validcols =
        (0..ncolumns).filter(|&i| !r.tupdesc.attrs[i].attisdropped).count() as u32;
    ::pqformat::pq_sendint32(&mut buf, validcols)?;

    for i in 0..ncolumns {
        let att = &r.tupdesc.attrs[i];
        if att.attisdropped {
            continue;
        }
        let column_type = att.atttypid;
        ::pqformat::pq_sendint32(&mut buf, column_type)?;
        if r.nulls[i] {
            ::pqformat::pq_sendint32(&mut buf, (-1i32) as u32)?;
            continue;
        }
        let my = flinfo.fn_extra_mut::<RecordRecvData>().unwrap();
        let stale = match &my.columns[i] {
            Some(c) => c.column_type != column_type,
            None => true,
        };
        if stale {
            let (typiofunc, _typisvarlena) = ::lsyscache::getTypeBinaryOutputInfo(column_type)?;
            let proc = ::fmgr_seams::fmgr_info::call(typiofunc)?;
            my.columns[i] =
                Some(ColumnRecvData { column_type, typioparam: InvalidOid, proc });
        }
        let col = flinfo.fn_extra_mut::<RecordRecvData>().unwrap().columns[i].as_mut().unwrap();
        let d = ::types_fmgr::send_function_call(&mut col.proc, r.values[i], mcx)?;
        let p = d.as_usize() as *const u8;
        // SAFETY: send returns a live 4B-header bytea.
        let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
        const VARHDRSZ: usize = 4;
        // SAFETY: `total` readable bytes at p.
        let payload = unsafe { core::slice::from_raw_parts(p.add(VARHDRSZ), total - VARHDRSZ) };
        ::pqformat::pq_sendint32(&mut buf, (total - VARHDRSZ) as u32)?;
        ::pqformat::pq_sendbytes(&mut buf, payload)?;
    }
    Ok(::types_fmgr::varlena_result(::pqformat::pq_endtypsend(buf)))
}

#[cfg(test)]
mod image_cmp_tests {
    use super::*;
    extern crate std;
    use ::mcx::MemoryContext;

    // datum_image_cmp/datum_image_eq underlie record_image_{ne,lt,gt,le,ge};
    // C's byval branch compares the full stored word (rowtypes.c datum.c
    // datum_image_eq/datum_image_cmp).
    #[test]
    fn byval_int4_cmp_and_eq() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let a = Datum::from_i32(5);
        let b = Datum::from_i32(7);
        assert_eq!(datum_image_cmp(mcx, a, a, true, 4).unwrap(), 0);
        assert_eq!(datum_image_cmp(mcx, a, b, true, 4).unwrap(), -1);
        assert_eq!(datum_image_cmp(mcx, b, a, true, 4).unwrap(), 1);
        assert!(datum_image_eq(mcx, a, a, true, 4).unwrap());
        assert!(!datum_image_eq(mcx, a, b, true, 4).unwrap());
    }

    #[test]
    fn byval_masks_to_attlen() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        // Same low byte, differing high bytes: attlen=1 must ignore them
        // (C's DatumGetChar/Int16/Int32 masking in datum_image_eq).
        let a = Datum::from_u32(0x0100_0007);
        let b = Datum::from_u32(0x0200_0007);
        assert!(datum_image_eq(mcx, a, b, true, 1).unwrap());
        assert!(!datum_image_eq(mcx, a, b, true, 4).unwrap());
    }

    #[test]
    fn image_ne_lt_gt_le_ge_are_cmp_derived() {
        // record_image_ne/lt/gt/le/ge (tid.c-style thin wrappers) reduce to
        // sign checks on record_image_cmp; verified directly on the shared
        // datum_image_cmp core rather than the full record fmgr path.
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let lo = Datum::from_i32(1);
        let hi = Datum::from_i32(2);
        let cmp_lt = datum_image_cmp(mcx, lo, hi, true, 4).unwrap();
        let cmp_gt = datum_image_cmp(mcx, hi, lo, true, 4).unwrap();
        let cmp_eq = datum_image_cmp(mcx, lo, lo, true, 4).unwrap();
        assert!(cmp_lt < 0 && !(cmp_lt <= 0 && cmp_lt >= 0));
        assert!(cmp_gt > 0);
        assert_eq!(cmp_eq, 0);
        assert!(!datum_image_eq(mcx, lo, hi, true, 4).unwrap());
    }
}
