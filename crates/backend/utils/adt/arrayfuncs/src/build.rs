use alloc::boxed::Box;

use ::datum::array_build::{ArrayBuildState, ArrayBuildStateAny, ArrayBuildStateArr};
use ::datum::{Bytea, Datum, VARHDRSZ};
use ::mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use ::types_fmgr::{receive_function_call, send_function_call, FmgrInfo};

use crate::construct::{construct_empty_array, construct_md_array, write_dims_lbounds, write_header};
use crate::element::array_bitmap_copy;
use crate::foundation::{
    arr_data_offset, arr_nullbitmap_off, arr_overhead_nonulls, arr_overhead_withnulls, arr_size,
    read_dims_lbounds, varsize_any, ARRAYTYPE_HDRSZ, MAXDIM,
};

// initArrayResult: allocate a build state in the caller-owned context `mcx`
// (the C private subcontext is the caller's child bump arena — deferred.md).
// element storage triple resolved via lsyscache get_typlenbyvalalign.
pub fn init_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    element_type: Oid,
    private_cxt: bool,
) -> PgResult<ArrayBuildState<'mcx>> {
    let mut astate = ArrayBuildState::new(mcx, element_type, private_cxt)?;
    let (typlen, typbyval, typalign) = ::lsyscache::get_typlenbyvalalign(element_type)?;
    astate.typlen = typlen;
    astate.typbyval = typbyval;
    astate.typalign = typalign as u8;
    Ok(astate)
}

// accumArrayResult: append one Datum, copying pass-by-ref payloads into the
// build context (datumCopy/detoast) so the caller's input is never damaged.
pub fn accum_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<ArrayBuildState<'mcx>>,
    dvalue: Datum,
    disnull: bool,
    element_type: Oid,
) -> PgResult<ArrayBuildState<'mcx>> {
    let mut astate = match astate {
        Some(a) => {
            debug_assert_eq!(a.element_type, element_type);
            a
        }
        None => init_array_result(mcx, element_type, true)?,
    };

    let stored = if !disnull && !astate.typbyval {
        let p = dvalue.as_usize() as *const u8;
        let n = if astate.typlen == -1 {
            varsize_any(p)
        } else {
            astate.typlen as usize
        };
        // SAFETY: by-ref datum points at n live bytes.
        let bytes = unsafe { core::slice::from_raw_parts(p, n) };
        // C accumArrayResult detoasts varlena elements before the copy
        // (PG_DETOAST_DATUM_COPY, arrayfuncs.c:5392-5402) so the build state
        // never holds a toast pointer the finalfn would pack. We detoast
        // external and compressed images here but keep short headers as-is
        // (C expands them too): construct_md_array expands shorts at
        // finalize, so the final array image is byte-identical to C's while
        // the build state stores the smaller short copy.
        if astate.typlen == -1 && (bytes[0] == 0x01 || (bytes[0] & 0x03) == 0x02) {
            let flat = ::detoast_seams::detoast_attr::call(mcx, bytes)?;
            astate.copy_byref(&flat)?
        } else {
            astate.copy_byref(bytes)?
        }
    } else {
        dvalue
    };

    astate.dvalues.push(stored);
    astate.dnulls.push(disnull);
    astate.nelems += 1;
    Ok(astate)
}

// makeArrayResult: 1-D final result (empty array if no elements accumulated).
pub fn make_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &ArrayBuildState<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let ndims = if astate.nelems > 0 { 1 } else { 0 };
    let dims = [astate.nelems];
    let lbs = [1i32];
    make_md_array_result(mcx, astate, ndims, &dims, &lbs)
}

// datumCopy into `st`'s context honoring typbyval/typlen. Flat inputs only:
// agg build states hold detoasted copies and recv outputs.
fn datum_copy_into(st: &ArrayBuildState<'_>, d: Datum) -> PgResult<Datum> {
    if st.typbyval {
        return Ok(d);
    }
    let p = d.as_usize() as *const u8;
    let n = match st.typlen {
        -1 => varsize_any(p),
        -2 => {
            let mut n = 0usize;
            // SAFETY: cstring datum is NUL-terminated.
            unsafe {
                while *p.add(n) != 0 {
                    n += 1;
                }
            }
            n + 1
        }
        l if l > 0 => l as usize,
        l => panic!("datum_copy_into: unsupported typlen {l}"),
    };
    // SAFETY: by-ref datum points at n live bytes.
    st.copy_byref(unsafe { core::slice::from_raw_parts(p, n) })
}

// array_agg_combine NULL-state1 arm: clone state2 into the agg context.
// C initArrayResultWithSize re-derives the typlen/typbyval/typalign triple
// from the catalog; state2 carries the identical values.
pub fn array_agg_combine_clone<'mcx>(
    mcx: Mcx<'mcx>,
    s2: &ArrayBuildState<'_>,
) -> PgResult<ArrayBuildState<'mcx>> {
    let mut s1 = ArrayBuildState::new(mcx, s2.element_type, false)?;
    s1.typlen = s2.typlen;
    s1.typbyval = s2.typbyval;
    s1.typalign = s2.typalign;
    for i in 0..s2.nelems as usize {
        let v = if !s2.dnulls[i] { datum_copy_into(&s1, s2.dvalues[i])? } else { Datum::null() };
        s1.dvalues.push(v);
        s1.dnulls.push(s2.dnulls[i]);
    }
    s1.nelems = s2.nelems;
    Ok(s1)
}

// array_agg_combine append arm (state2.nelems > 0 checked by the caller).
pub fn array_agg_combine_append(
    s1: &mut ArrayBuildState<'_>,
    s2: &ArrayBuildState<'_>,
) -> PgResult<()> {
    debug_assert_eq!(s1.element_type, s2.element_type);
    for i in 0..s2.nelems as usize {
        let v = if !s2.dnulls[i] { datum_copy_into(s1, s2.dvalues[i])? } else { Datum::null() };
        s1.dvalues.push(v);
        s1.dnulls.push(s2.dnulls[i]);
    }
    s1.nelems += s2.nelems;
    Ok(())
}

// array_agg_serialize wire image (field order/widths are byte-law).
// `typsend` is required iff the element type is by-ref.
pub fn array_agg_serialize_state<'mcx>(
    mcx: Mcx<'mcx>,
    st: &ArrayBuildState<'_>,
    typsend: Option<&mut FmgrInfo>,
) -> PgResult<Bytea<'mcx>> {
    let n = st.nelems as usize;
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, st.element_type as u32)?;
    ::pqformat::pq_sendint64(&mut buf, st.nelems as i64 as u64)?;
    ::pqformat::pq_sendint16(&mut buf, st.typlen as u16)?;
    ::pqformat::pq_sendbyte(&mut buf, st.typbyval as u8)?;
    ::pqformat::pq_sendbyte(&mut buf, st.typalign)?;
    // dnulls raw (sizeof(bool) * nelems).
    // SAFETY: bool is one 0/1 byte; dnulls holds n live elements.
    let dnulls = unsafe { core::slice::from_raw_parts(st.dnulls.as_ptr().cast::<u8>(), n) };
    ::pqformat::pq_sendbytes(&mut buf, dnulls)?;
    if st.typbyval {
        // By agreement with array_agg_deserialize, byval Datums go as-is,
        // null slots included.
        // SAFETY: Datum is a plain 8-byte word; dvalues holds n live elements.
        let dv = unsafe { core::slice::from_raw_parts(st.dvalues.as_ptr().cast::<u8>(), n * 8) };
        ::pqformat::pq_sendbytes(&mut buf, dv)?;
    } else {
        let proc = typsend.expect("array_agg_serialize: by-ref element type needs typsend");
        for i in 0..n {
            if st.dnulls[i] {
                continue;
            }
            let d = send_function_call(proc, st.dvalues[i], mcx)?;
            let p = d.as_usize() as *const u8;
            let total = varsize_any(p);
            ::pqformat::pq_sendint32(&mut buf, (total - VARHDRSZ) as u32)?;
            // SAFETY: send fns return a live 4B-header bytea of `total` bytes.
            let payload =
                unsafe { core::slice::from_raw_parts(p.add(VARHDRSZ), total - VARHDRSZ) };
            ::pqformat::pq_sendbytes(&mut buf, payload)?;
        }
    }
    Ok(::pqformat::pq_endtypsend(buf))
}

// array_agg_deserialize; `recv` = (typreceive, typioparam), required iff the
// wire typbyval flag is false.
pub fn array_agg_deserialize_state<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
    recv: Option<(&mut FmgrInfo, Oid)>,
) -> PgResult<ArrayBuildState<'mcx>> {
    let mut buf = ::stringinfo::StringInfo::with_capacity_in(mcx, payload.len() + 1)?;
    buf.append_bytes(payload)?;
    let element_type = ::pqformat::pq_getmsgint(&mut buf, 4)? as Oid;
    let nelems = ::pqformat::pq_getmsgint64(&mut buf)?;
    // C initArrayResultWithSize's catalog lookup is skipped: the wire carries
    // the typlen/typbyval/typalign triple it would return.
    let mut result = ArrayBuildState::new(mcx, element_type, false)?;
    result.nelems = nelems as i32;
    result.typlen = ::pqformat::pq_getmsgint(&mut buf, 2)? as i16;
    result.typbyval = ::pqformat::pq_getmsgbyte(&mut buf)? != 0;
    result.typalign = ::pqformat::pq_getmsgbyte(&mut buf)? as u8;
    let n = result.nelems as usize;
    {
        let raw = ::pqformat::pq_getmsgbytes(&mut buf, n)?;
        for &b in raw {
            result.dnulls.push(b != 0);
        }
    }
    if result.typbyval {
        let raw = ::pqformat::pq_getmsgbytes(&mut buf, n * 8)?;
        for c in raw.chunks_exact(8) {
            result.dvalues.push(Datum::from_u64(u64::from_ne_bytes(c.try_into().unwrap())));
        }
    } else {
        let (proc, typioparam) =
            recv.expect("array_agg_deserialize: by-ref element type needs typreceive");
        for i in 0..n {
            if result.dnulls[i] {
                result.dvalues.push(Datum::null());
                continue;
            }
            let itemlen = ::pqformat::pq_getmsgint(&mut buf, 4)? as i32;
            if itemlen < 0 || itemlen as usize > buf.len() - buf.cursor {
                return Err(Box::new(
                    PgError::error("insufficient data left in message")
                        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
                ));
            }
            let mut elem_buf =
                ::stringinfo::StringInfo::with_capacity_in(mcx, itemlen as usize + 1)?;
            {
                let slice = ::pqformat::pq_getmsgbytes(&mut buf, itemlen as usize)?;
                elem_buf.append_bytes(slice)?;
            }
            let v = receive_function_call(proc, Some(&mut elem_buf), typioparam, -1, mcx)?;
            result.dvalues.push(v);
        }
    }
    ::pqformat::pq_getmsgend(&buf)?;
    Ok(result)
}

// initArrayResultArr: element type looked up (true-array check) when the
// caller passes InvalidOid.
pub fn init_array_result_arr<'mcx>(
    mcx: Mcx<'mcx>,
    array_type: Oid,
    element_type: Oid,
) -> PgResult<ArrayBuildStateArr<'mcx>> {
    let element_type = if element_type != 0 {
        element_type
    } else {
        let et = ::lsyscache::get_element_type(array_type)?;
        if et == 0 {
            return Err(Box::new(
                PgError::error(alloc::format!(
                    "data type {} is not an array type",
                    ::format_type::format_type_be(array_type)?
                ))
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
            ));
        }
        et
    };
    Ok(ArrayBuildStateArr {
        mcx,
        data: PgVec::new_in(mcx),
        nullbitmap: None,
        abytes: 0,
        aitems: 0,
        nbytes: 0,
        nitems: 0,
        ndims: 0,
        dims: [0; MAXDIM],
        lbs: [0; MAXDIM],
        array_type,
        element_type,
        private_cxt: false,
    })
}

#[track_caller]
#[cold]
fn diff_dimensionality() -> Box<PgError> {
    Box::new(
        PgError::error("cannot accumulate arrays of different dimensionality")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

// pg_bitutils.h pg_nextpower2_32; valid for num in [1, 2^31].
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0 && num <= 0x8000_0000);
    num.next_power_of_two()
}

// accumArrayResultArr: append one sub-array's data (input detoasted per C's
// DatumGetArrayTypeP). abytes/aitems track C's growth formulas — they are
// wire fields of array_agg_array_serialize even though PgVec owns capacity.
pub fn accum_array_result_arr<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<ArrayBuildStateArr<'mcx>>,
    dvalue: Datum,
    disnull: bool,
    array_type: Oid,
) -> PgResult<ArrayBuildStateArr<'mcx>> {
    if disnull {
        return Err(Box::new(
            PgError::error("cannot accumulate null arrays")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    }
    let p = dvalue.as_usize() as *const u8;
    // SAFETY: non-null by-ref array datum readable through its varlena header.
    let raw = unsafe { core::slice::from_raw_parts(p, varsize_any(p)) };
    let detoasted;
    let arg: &[u8] = if raw[0] & 0x03 != 0 {
        detoasted = ::detoast_seams::detoast_attr::call(mcx, raw)?;
        &detoasted
    } else {
        raw
    };

    let mut st = match astate {
        Some(s) => {
            debug_assert_eq!(s.array_type, array_type);
            s
        }
        None => init_array_result_arr(mcx, array_type, 0)?,
    };

    let (ndims, dims, lbs) = read_dims_lbounds(arg);
    let nitems = ::arrayutils::array_get_n_items(ndims, &dims)?;
    let data_off = arr_data_offset(arg);
    let ndatabytes = arr_size(arg) - data_off;

    // nitems can overflow before the allocation limit trips when
    // accumulating many all-NULL arrays (CVE-2026-6473, C 67dd624).
    let newnitems = st.nitems.checked_add(nitems).unwrap_or(i32::MAX);
    if newnitems as i64 > ::arrayutils::MAX_ARRAY_SIZE {
        return Err(Box::new(
            PgError::error(alloc::format!(
                "array size exceeds the maximum allowed ({})",
                ::arrayutils::MAX_ARRAY_SIZE
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }

    if st.ndims == 0 {
        if ndims == 0 {
            return Err(Box::new(
                PgError::error("cannot accumulate empty arrays")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
            ));
        }
        if ndims as usize + 1 > MAXDIM {
            return Err(Box::new(
                PgError::error(alloc::format!(
                    "number of array dimensions ({}) exceeds the maximum allowed ({MAXDIM})",
                    ndims + 1
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ));
        }
        st.ndims = ndims + 1;
        st.dims[0] = 0;
        st.dims[1..=ndims as usize].copy_from_slice(&dims[..ndims as usize]);
        st.lbs[0] = 1;
        st.lbs[1..=ndims as usize].copy_from_slice(&lbs[..ndims as usize]);
        st.abytes = pg_nextpower2_32(core::cmp::max(1024, ndatabytes as i32 + 1) as u32) as i32;
    } else {
        if st.ndims != ndims + 1 {
            return Err(diff_dimensionality());
        }
        for i in 0..ndims as usize {
            if st.dims[i + 1] != dims[i] || st.lbs[i + 1] != lbs[i] {
                return Err(diff_dimensionality());
            }
        }
        if st.nbytes + ndatabytes as i32 > st.abytes {
            st.abytes = core::cmp::max(st.abytes * 2, st.nbytes + ndatabytes as i32);
        }
    }

    vec_append_bytes(&mut st.data, &arg[data_off..data_off + ndatabytes])?;
    st.nbytes += ndatabytes as i32;

    let arg_bitmap = arr_nullbitmap_off(arg);
    if st.nullbitmap.is_some() || arg_bitmap.is_some() {
        match st.nullbitmap.as_mut() {
            None => {
                // First input with nulls: prior inputs marked all-non-null.
                st.aitems = pg_nextpower2_32(core::cmp::max(256, newnitems + 1) as u32) as i32;
                let need = (st.aitems as usize + 7) / 8;
                let mut bm: PgVec<u8> = vec_with_capacity_in(mcx, need)?;
                bm.resize(need, 0);
                array_bitmap_copy(&mut bm, 0, 0, None, 0, st.nitems);
                st.nullbitmap = Some(bm);
            }
            Some(bm) => {
                if newnitems > st.aitems {
                    st.aitems = core::cmp::max(st.aitems * 2, newnitems);
                    bm.resize((st.aitems as usize + 7) / 8, 0);
                }
            }
        }
        let bm = st.nullbitmap.as_mut().unwrap();
        array_bitmap_copy(bm, 0, st.nitems, arg_bitmap.map(|b| (arg, b)), 0, nitems);
    }

    st.nitems = newnitems;
    st.dims[0] += 1;
    Ok(st)
}

// makeArrayResultArr: N+1-D final result (empty array if nothing accumulated).
pub fn make_array_result_arr<'mcx>(
    mcx: Mcx<'mcx>,
    st: &ArrayBuildStateArr<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    if st.ndims == 0 {
        return construct_empty_array(mcx, st.element_type);
    }
    ::arrayutils::array_get_n_items(st.ndims, &st.dims)?;
    ::arrayutils::array_check_bounds(st.ndims, &st.dims, &st.lbs)?;

    let mut nbytes = st.nbytes as usize;
    let dataoffset = if st.nullbitmap.is_some() {
        let d = arr_overhead_withnulls(st.ndims, st.nitems);
        nbytes += d;
        d as i32
    } else {
        nbytes += arr_overhead_nonulls(st.ndims);
        0
    };

    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, nbytes)?;
    out.resize(nbytes, 0);
    write_header(&mut out, nbytes, st.ndims, dataoffset, st.element_type);
    write_dims_lbounds(&mut out, st.ndims, &st.dims, &st.lbs);
    let dst = arr_data_offset(&out);
    out[dst..dst + st.nbytes as usize].copy_from_slice(&st.data[..st.nbytes as usize]);
    if let Some(bm) = st.nullbitmap.as_deref() {
        let bo = ARRAYTYPE_HDRSZ + 2 * 4 * st.ndims as usize;
        array_bitmap_copy(&mut out, bo, 0, Some((bm, 0)), 0, st.nitems);
    }
    Ok(out)
}

// initArrayResultAny: get_array_type (not get_element_type) picks the lane —
// int2vector/oidvector satisfy both and must accumulate as scalars, matching
// get_promoted_array_type.
pub fn init_array_result_any<'mcx>(
    mcx: Mcx<'mcx>,
    input_type: Oid,
    private_cxt: bool,
) -> PgResult<ArrayBuildStateAny<'mcx>> {
    if ::lsyscache::get_array_type(input_type)? == 0 {
        Ok(ArrayBuildStateAny {
            scalarstate: None,
            arraystate: Some(init_array_result_arr(mcx, input_type, 0)?),
        })
    } else {
        Ok(ArrayBuildStateAny {
            scalarstate: Some(init_array_result(mcx, input_type, private_cxt)?),
            arraystate: None,
        })
    }
}

pub fn accum_array_result_any<'mcx>(
    mcx: Mcx<'mcx>,
    astate: Option<ArrayBuildStateAny<'mcx>>,
    dvalue: Datum,
    disnull: bool,
    input_type: Oid,
) -> PgResult<ArrayBuildStateAny<'mcx>> {
    let mut st = match astate {
        Some(s) => s,
        None => init_array_result_any(mcx, input_type, true)?,
    };
    if st.scalarstate.is_some() {
        st.scalarstate =
            Some(accum_array_result(mcx, st.scalarstate.take(), dvalue, disnull, input_type)?);
    } else {
        st.arraystate =
            Some(accum_array_result_arr(mcx, st.arraystate.take(), dvalue, disnull, input_type)?);
    }
    Ok(st)
}

pub fn make_array_result_any<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &ArrayBuildStateAny<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    match (&astate.scalarstate, &astate.arraystate) {
        (Some(s), _) => make_array_result(mcx, s),
        (None, Some(a)) => make_array_result_arr(mcx, a),
        (None, None) => unreachable!("ArrayBuildStateAny has no sub-state"),
    }
}

pub fn make_md_array_result<'mcx>(
    mcx: Mcx<'mcx>,
    astate: &ArrayBuildState<'mcx>,
    ndims: i32,
    dims: &[i32],
    lbs: &[i32],
) -> PgResult<PgVec<'mcx, u8>> {
    construct_md_array(
        mcx,
        astate.dvalues.as_slice(),
        Some(astate.dnulls.as_slice()),
        ndims,
        dims,
        lbs,
        astate.element_type,
        astate.typlen as i32,
        astate.typbyval,
        astate.typalign,
    )
}
