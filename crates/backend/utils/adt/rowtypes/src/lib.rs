//! rowtypes.c, output slice: record_out. record_in/recv/send and the
//! record comparison family stay loud through the canonical fmgr table.
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
    // std Vec justified: rides FmgrInfo.fn_extra (Box<dyn Any>), same
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
                return Err(alloc::boxed::Box::new(
                    ::types_error::PgError::error(alloc::format!(
                        "could not identify a hash function for type {column_type}"
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
    b(2291, "record_out", 1, fc_record_out),
    b(6192, "hash_record", 1, fc_hash_record),
    b(6193, "hash_record_extended", 2, fc_hash_record_extended),
];
