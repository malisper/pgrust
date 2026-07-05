use alloc::boxed::Box;

use ::datum::array_build::ArrayBuildState;
use ::datum::{Bytea, Datum, VARHDRSZ};
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_BINARY_REPRESENTATION};
use ::types_fmgr::{receive_function_call, send_function_call, FmgrInfo};

use crate::construct::construct_md_array;
use crate::foundation::varsize_any;

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
        // C accumArrayResult PG_DETOAST_DATUMs varlena elements before copy.
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
