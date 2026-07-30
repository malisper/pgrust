//! hstore_from_record / hstore_populate_record (hstore_io.c), on the
//! rowtypes record_out/record_in per-column IO idiom (fn_extra caches the
//! resolved column IO procs across calls).

use datum::Datum;
use mcx::{vec_from_elem_in, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use types_fmgr::{function_call1_coll_in, input_function_call, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::{HeapTupleData, HeapTupleHeaderData, ItemPointerData};

use crate::repr::{build_hstore, find_key, unique_pairs, HstoreView, Pair};
use crate::{check_key_len, check_val_len, ret_null};

const RECORDOID: Oid = 2249;

struct ColumnIOData {
    column_type: Oid,
    typioparam: Oid,
    proc: FmgrInfo,
}

struct RecordIOData {
    record_type: Oid,
    record_typmod: i32,
    columns: Vec<Option<ColumnIOData>>,
}

fn type_is_rowtype(typid: Oid) -> PgResult<bool> {
    if typid == RECORDOID {
        return Ok(true);
    }
    Ok(lsyscache::typ::get_typtype(typid)? == b'c' as i8)
}

#[track_caller]
#[cold]
fn not_rowtype() -> Box<PgError> {
    Box::new(
        PgError::error("first argument must be a rowtype")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
    )
}

// PG_GETARG_HEAPTUPLEHEADER(i): detoasted composite image + control tuple.
struct RecArg<'m> {
    rec: &'m [u8],
    tup_type: Oid,
    tup_typmod: i32,
}

fn composite_arg<'m>(mcx: mcx::Mcx<'m>, fcinfo: &Fcinfo, i: usize) -> PgResult<RecArg<'m>> {
    // SAFETY: caller checked arg i non-null; a live varlena-headed composite.
    let p = unsafe { fcinfo.arg_ptr(i) };
    let total = unsafe { types_tuple::varatt::varsize_any(p) };
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let rec: &'m [u8] = detoast_seams::detoast_attr::call(mcx, raw)?.leak();
    // SAFETY: detoasted composite image; header prefix in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
    Ok(RecArg { rec, tup_type: hdr.type_id(), tup_typmod: hdr.typmod() })
}

fn control_tuple(rec: &[u8]) -> HeapTupleData<'_> {
    // SAFETY: MAXALIGN'd detoasted image of datum_length() bytes.
    let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
    unsafe {
        HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ItemPointerData::invalid(),
            InvalidOid,
        )
    }
}

fn refresh_extra(flinfo: &mut FmgrInfo, tup_type: Oid, tup_typmod: i32, ncolumns: usize) {
    let refresh = match flinfo.fn_extra_ref::<RecordIOData>() {
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
        flinfo.set_fn_extra(RecordIOData { record_type: tup_type, record_typmod: tup_typmod, columns });
    }
}

pub fn fc_hstore_from_record(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_from_record: resolved FmgrInfo required");
    let mcx = fcinfo.result_mcx();
    let [a] = fcinfo.args_n::<1>();
    let a_isnull = a.isnull;

    let (tup_type, tup_typmod, rec): (Oid, i32, Option<RecArg<'_>>) = if a_isnull {
        let argtype = funcapi::get_fn_expr_argtype(Some(flinfo), 0);
        (argtype, -1, None)
    } else {
        let ra = composite_arg(mcx, fcinfo, 0)?;
        (ra.tup_type, ra.tup_typmod, Some(ra))
    };

    let tupdesc = typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;
    refresh_extra(flinfo, tup_type, tup_typmod, ncolumns);

    let mut values: PgVec<'_, Datum> = vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'_, bool> = vec_from_elem_in(mcx, true, ncolumns);
    if let Some(ra) = &rec {
        let tuple = control_tuple(ra.rec);
        types_tuple::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut nulls);
    }

    let mut pairs: Vec<Pair> = Vec::with_capacity(ncolumns);
    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        if att.attisdropped {
            continue;
        }
        let key = att.attname.name_str().to_vec();
        check_key_len(key.len())?;
        let val = if nulls[i] {
            None
        } else {
            let column_type = att.atttypid;
            let my = flinfo.fn_extra_mut::<RecordIOData>().unwrap();
            let stale = match &my.columns[i] {
                Some(c) => c.column_type != column_type,
                None => true,
            };
            if stale {
                let (typiofunc, _varlena) = lsyscache::getTypeOutputInfo(column_type)?;
                let proc = fmgr_seams::fmgr_info::call(typiofunc)?;
                my.columns[i] = Some(ColumnIOData { column_type, typioparam: InvalidOid, proc });
            }
            let proc = &mut flinfo.fn_extra_mut::<RecordIOData>().unwrap().columns[i]
                .as_mut()
                .unwrap()
                .proc;
            let d = function_call1_coll_in(proc, InvalidOid, mcx, values[i])?;
            let v = cstring_bytes(d).to_vec();
            check_val_len(v.len())?;
            Some(v)
        };
        pairs.push(Pair { key, val, needfree: false });
    }
    let pairs = unique_pairs(pairs);
    crate::ret_hstore_pub(fcinfo, &build_hstore(&pairs))
}

pub fn fc_hstore_populate_record(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_populate_record: resolved FmgrInfo required");
    let mcx = fcinfo.result_mcx();
    let argtype = funcapi::get_fn_expr_argtype(Some(flinfo), 0);
    if !type_is_rowtype(argtype)? {
        return Err(not_rowtype());
    }
    let [a, b] = fcinfo.args_n::<2>();
    let (a_isnull, b_isnull) = (a.isnull, b.isnull);

    let (tup_type, tup_typmod, rec): (Oid, i32, Option<RecArg<'_>>) = if a_isnull {
        if b_isnull {
            return Ok(ret_null(fcinfo));
        }
        (argtype, -1, None)
    } else {
        let ra = composite_arg(mcx, fcinfo, 0)?;
        if b_isnull {
            // C: PG_RETURN_POINTER(rec) — the detoasted image.
            return Ok(Datum::from_usize(ra.rec.as_ptr() as usize));
        }
        (ra.tup_type, ra.tup_typmod, Some(ra))
    };

    // SAFETY: checked non-null above.
    let hs = unsafe { crate::arg_hstore(fcinfo, 1)? };
    if hs.count() == 0 {
        if let Some(ra) = &rec {
            return Ok(Datum::from_usize(ra.rec.as_ptr() as usize));
        }
    }

    let tupdesc = typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;
    refresh_extra(flinfo, tup_type, tup_typmod, ncolumns);

    let mut values: PgVec<'_, Datum> = vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: PgVec<'_, bool> = vec_from_elem_in(mcx, true, ncolumns);
    if let Some(ra) = &rec {
        let tuple = control_tuple(ra.rec);
        types_tuple::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut nulls);
    }

    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        if att.attisdropped {
            nulls[i] = true;
            continue;
        }
        let idx = find_key(&hs, None, att.attname.name_str());
        // A missing key keeps the existing value for a non-null record; for a
        // null record every unpopulated field still runs the input function
        // (domain null checks).
        if idx.is_none() && rec.is_some() {
            continue;
        }
        let column_type = att.atttypid;
        let my = flinfo.fn_extra_mut::<RecordIOData>().unwrap();
        let stale = match &my.columns[i] {
            Some(c) => c.column_type != column_type,
            None => true,
        };
        if stale {
            let (typiofunc, typioparam) = lsyscache::getTypeInputInfo(column_type)?;
            let proc = fmgr_seams::fmgr_info::call(typiofunc)?;
            my.columns[i] = Some(ColumnIOData { column_type, typioparam, proc });
        }
        let col = flinfo.fn_extra_mut::<RecordIOData>().unwrap().columns[i].as_mut().unwrap();

        let cstr_storage;
        let cstr: Option<&core::ffi::CStr> = match idx {
            Some(idx) if !hs.val_isnull(idx) => {
                let mut v = Vec::with_capacity(hs.vallen(idx) + 1);
                v.extend_from_slice(hs.val(idx));
                v.push(0);
                cstr_storage = v;
                Some(
                    core::ffi::CStr::from_bytes_with_nul(&cstr_storage)
                        .map_err(|_| embedded_nul())?,
                )
            }
            _ => None,
        };
        let isnull = cstr.is_none();
        values[i] = input_function_call(&mut col.proc, cstr, col.typioparam, att.atttypmod, mcx)?;
        nulls[i] = isnull;
    }

    let tuple = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    let d = Datum::from_usize(tuple.image().as_ptr() as usize);
    core::mem::forget(tuple);

    // Domain over composite: validate constraints on the base value.
    if argtype != tupdesc.tdtypeid {
        adt_domains::domain_check(d, false, argtype)?;
    }
    Ok(d)
}

#[track_caller]
#[cold]
fn embedded_nul() -> Box<PgError> {
    Box::new(
        PgError::error("invalid byte sequence in hstore value")
            .with_sqlstate(types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

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
