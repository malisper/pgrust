use ::datum::Datum;
use ::fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_storage::bufpage::MaxHeapTuplesPerPage;

const USECS_PER_SEC: i64 = 1_000_000;
const USECS_PER_DAY: i64 = 86_400_000_000;
const PGSQL_AF_INET: u8 = 2;

#[inline]
fn ret_f64(v: f64) -> PgResult<Datum> {
    Ok(Datum::from_f64(v))
}

// SAFETY: arg i is a live by-ref image of at least N bytes.
unsafe fn arg_fixed<const N: usize>(fcinfo: &Fcinfo, i: usize) -> [u8; N] {
    let p = fcinfo.arg(i).as_usize() as *const u8;
    let mut out = [0u8; N];
    core::ptr::copy_nonoverlapping(p, out.as_mut_ptr(), N);
    out
}

// PG_GETARG_*_PP payload: compressed/short forms detoast into the armed
// result mcx (stored BRIN bounds keep the heap value's form verbatim).
// SAFETY: arg i is a live varlena image.
unsafe fn arg_varlena_payload<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    Ok(fcinfo.arg_varlena_packed(i)?.data())
}

fn fc_dist_float4(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a1 = f32::from_bits(fcinfo.arg(0).as_u32());
    let a2 = f32::from_bits(fcinfo.arg(1).as_u32());
    if a1.is_nan() && a2.is_nan() {
        return ret_f64(0.0);
    }
    if a1.is_nan() || a2.is_nan() {
        return ret_f64(f64::INFINITY);
    }
    debug_assert!(a1 <= a2);
    ret_f64(a2 as f64 - a1 as f64)
}

fn fc_dist_float8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a1 = fcinfo.arg(0).as_f64();
    let a2 = fcinfo.arg(1).as_f64();
    if a1.is_nan() && a2.is_nan() {
        return ret_f64(0.0);
    }
    if a1.is_nan() || a2.is_nan() {
        return ret_f64(f64::INFINITY);
    }
    debug_assert!(a1 <= a2);
    ret_f64(a2 - a1)
}

fn fc_dist_int2(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ret_f64(fcinfo.arg(1).as_i16() as f64 - fcinfo.arg(0).as_i16() as f64)
}

fn fc_dist_int4(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ret_f64(fcinfo.arg(1).as_i32() as f64 - fcinfo.arg(0).as_i32() as f64)
}

fn fc_dist_int8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ret_f64(fcinfo.arg(1).as_i64() as f64 - fcinfo.arg(0).as_i64() as f64)
}

fn fc_dist_tid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: tid args are live 6-byte ItemPointerData images.
    let (a, b) = unsafe { (arg_fixed::<6>(fcinfo, 0), arg_fixed::<6>(fcinfo, 1)) };
    // C computes block*MaxHeapTuplesPerPage+off in uint32 (wraps) before the
    // double conversion.
    let tid_map = |t: [u8; 6]| -> f64 {
        let bi_hi = u16::from_ne_bytes([t[0], t[1]]) as u32;
        let bi_lo = u16::from_ne_bytes([t[2], t[3]]) as u32;
        let posid = u16::from_ne_bytes([t[4], t[5]]) as u32;
        ((bi_hi << 16 | bi_lo)
            .wrapping_mul(MaxHeapTuplesPerPage as u32)
            .wrapping_add(posid)) as f64
    };
    ret_f64(tid_map(b) - tid_map(a))
}

// C's PG_GETARG_NUMERIC: a 1B-short image (stored BRIN bounds keep the heap
// value's packed form, so digits land misaligned) expands into the result mcx.
// SAFETY: arg i is a live inline numeric varlena image.
unsafe fn arg_numeric_payload<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    // SAFETY: forwarded caller contract.
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    if v.is_short() {
        v.data_expanded(fcinfo.result_mcx())
    } else {
        Ok(v.data())
    }
}

fn fc_dist_numeric(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: numeric args are live inline varlena images.
    let (p1, p2) = unsafe { (arg_numeric_payload(fcinfo, 0)?, arg_numeric_payload(fcinfo, 1)?) };
    let a1 = ::adt_numeric::Num::from_payload(p1);
    let a2 = ::adt_numeric::Num::from_payload(p2);
    let d = ::adt_numeric::ops::numeric_sub_common(a2, a1)?;
    ret_f64(::adt_numeric::ops::numeric_float8(d.num())?)
}

fn fc_dist_uuid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: uuid args are live 16-byte images.
    let (u1, u2) = unsafe { (arg_fixed::<16>(fcinfo, 0), arg_fixed::<16>(fcinfo, 1)) };
    let mut delta = 0f64;
    for i in (0..16).rev() {
        delta += (u2[i] as i32 - u1[i] as i32) as f64;
        delta /= 256.0;
    }
    debug_assert!(delta >= 0.0);
    ret_f64(delta)
}

fn fc_dist_date(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ret_f64(fcinfo.arg(1).as_i32() as f64 - fcinfo.arg(0).as_i32() as f64)
}

fn fc_dist_time(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ret_f64((fcinfo.arg(1).as_i64() - fcinfo.arg(0).as_i64()) as f64)
}

fn fc_dist_timetz(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: timetz args are live 12-byte {time: i64, zone: i32} images.
    let (a, b) = unsafe { (arg_fixed::<12>(fcinfo, 0), arg_fixed::<12>(fcinfo, 1)) };
    let parse = |t: [u8; 12]| {
        (
            i64::from_ne_bytes(t[0..8].try_into().unwrap()),
            i32::from_ne_bytes(t[8..12].try_into().unwrap()),
        )
    };
    let (ta, tb) = (parse(a), parse(b));
    ret_f64(((tb.0 - ta.0) + (tb.1 as i64 - ta.1 as i64) * USECS_PER_SEC) as f64)
}

fn fc_dist_timestamp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ret_f64(fcinfo.arg(1).as_i64() as f64 - fcinfo.arg(0).as_i64() as f64)
}

fn fc_dist_interval(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: interval args are live 16-byte {time: i64, day: i32, month: i32}.
    let (a, b) = unsafe { (arg_fixed::<16>(fcinfo, 0), arg_fixed::<16>(fcinfo, 1)) };
    let parse = |t: [u8; 16]| {
        (
            i64::from_ne_bytes(t[0..8].try_into().unwrap()),
            i32::from_ne_bytes(t[8..12].try_into().unwrap()),
            i32::from_ne_bytes(t[12..16].try_into().unwrap()),
        )
    };
    let (ia, ib) = (parse(a), parse(b));
    let dayfraction = (ib.0 % USECS_PER_DAY) - (ia.0 % USECS_PER_DAY);
    let mut days = (ib.0 / USECS_PER_DAY) - (ia.0 / USECS_PER_DAY);
    days += ib.1 as i64 - ia.1 as i64;
    days += (ib.2 as i64 - ia.2 as i64) * 30;
    ret_f64(days as f64 + dayfraction as f64 / USECS_PER_DAY as f64)
}

fn fc_dist_pg_lsn(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ret_f64(fcinfo.arg(1).as_u64().wrapping_sub(fcinfo.arg(0).as_u64()) as f64)
}

fn fc_dist_macaddr(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: macaddr args are live 6-byte images.
    let (a, b) = unsafe { (arg_fixed::<6>(fcinfo, 0), arg_fixed::<6>(fcinfo, 1)) };
    let mut delta = 0f64;
    for i in (0..6).rev() {
        delta += b[i] as f64 - a[i] as f64;
        delta /= 256.0;
    }
    debug_assert!(delta >= 0.0);
    ret_f64(delta)
}

fn fc_dist_macaddr8(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: macaddr8 args are live 8-byte images.
    let (a, b) = unsafe { (arg_fixed::<8>(fcinfo, 0), arg_fixed::<8>(fcinfo, 1)) };
    let mut delta = 0f64;
    for i in (0..8).rev() {
        delta += b[i] as f64 - a[i] as f64;
        delta /= 256.0;
    }
    debug_assert!(delta >= 0.0);
    ret_f64(delta)
}

fn fc_dist_inet(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: inet args are live inline varlena images.
    let (pa, pb) = unsafe { (arg_varlena_payload(fcinfo, 0)?, arg_varlena_payload(fcinfo, 1)?) };
    // inet_struct: family u8, bits u8, ipaddr [u8; 4|16].
    let (fam_a, bits_a) = (pa[0], pa[1] as i32);
    let (fam_b, bits_b) = (pb[0], pb[1] as i32);
    if fam_a != fam_b {
        return ret_f64(1.0);
    }
    let len = if fam_a == PGSQL_AF_INET { 4 } else { 16 };
    let mut addra = [0u8; 16];
    let mut addrb = [0u8; 16];
    addra[..len].copy_from_slice(&pa[2..2 + len]);
    addrb[..len].copy_from_slice(&pb[2..2 + len]);

    for i in 0..len {
        let nbits = (bits_a - (i as i32 * 8)).max(0);
        if nbits < 8 {
            addra[i] &= (0xFFu32 << (8 - nbits)) as u8;
        }
        let nbits = (bits_b - (i as i32 * 8)).max(0);
        if nbits < 8 {
            addrb[i] &= (0xFFu32 << (8 - nbits)) as u8;
        }
    }

    let mut delta = 0f64;
    for i in (0..len).rev() {
        delta += addrb[i] as f64 - addra[i] as f64;
        delta /= 256.0;
    }
    debug_assert!((0.0..=1.0).contains(&delta));
    ret_f64(delta)
}

#[cold]
#[inline(never)]
fn cannot_accept() -> PgError {
    PgError::error("cannot accept a value of type brin_minmax_multi_summary")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

fn fc_summary_in(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(cannot_accept().into())
}

fn fc_summary_recv(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(cannot_accept().into())
}

// brin_minmax_multi_summary_out: "{nranges: N  nvalues: N  maxvalues: N
// [ ranges: {..}][ values: {..}]}" over the deserialized summary.
fn fc_summary_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    // PG_DETOAST_DATUM: rebuild the 4-byte-header image (stored values may
    // use a short header).
    // SAFETY: STRICT varlena arg.
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? };
    let mut image = Vec::with_capacity(payload.data().len() + 4);
    image.extend_from_slice(&(((payload.data().len() + 4) as u32) << 2).to_ne_bytes());
    image.extend_from_slice(payload.data());

    let hdr = crate::ranges::read_serialized_header(&image);
    let (outfunc, _isvarlena) = ::lsyscache::getTypeOutputInfo(hdr.typid)?;
    let mut out_fn = ::fmgr_core::fmgr_info(outfunc)?;
    let ranges = crate::ranges::brin_range_deserialize(mcx, hdr.maxvalues, &image)?;

    let mut s = format!(
        "{{nranges: {}  nvalues: {}  maxvalues: {}",
        ranges.nranges, ranges.nvalues, ranges.maxvalues
    );

    let call_out = |out_fn: &mut FmgrInfo, val: Datum| -> PgResult<String> {
        let d = ::fmgr::function_call1_coll_in(out_fn, ::types_core::InvalidOid, mcx, val)?;
        // SAFETY: output functions return NUL-terminated cstrings.
        let cs = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
        Ok(String::from_utf8_lossy(cs.to_bytes()).into_owned())
    };
    let text_elem = |txt: &str| -> Vec<u8> {
        let mut img = Vec::with_capacity(txt.len() + 4);
        img.extend_from_slice(&(((txt.len() + 4) as u32) << 2).to_ne_bytes());
        img.extend_from_slice(txt.as_bytes());
        img
    };
    let anyarray_text = |astate: &::datum::array_build::ArrayBuildState<'_>| -> PgResult<String> {
        let (typoutput, _) = ::lsyscache::getTypeOutputInfo(::types_core::ANYARRAYOID)?;
        let mut arr_out = ::fmgr_core::fmgr_info(typoutput)?;
        let img = ::arrayfuncs::build::make_array_result(mcx, astate)?;
        let val = Datum::from_usize(img.as_ptr() as usize);
        let d = ::fmgr::function_call1_coll_in(&mut arr_out, ::types_core::InvalidOid, mcx, val)?;
        // SAFETY: array_out returns a NUL-terminated cstring.
        let cs = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
        let s = String::from_utf8_lossy(cs.to_bytes()).into_owned();
        core::mem::forget(img); // keep the mcx image alive for the call above
        Ok(s)
    };

    let mut idx = 0usize;
    let mut astate: Option<::datum::array_build::ArrayBuildState<'_>> = None;
    for _ in 0..ranges.nranges {
        let a = call_out(&mut out_fn, ranges.values[idx])?;
        let b = call_out(&mut out_fn, ranges.values[idx + 1])?;
        idx += 2;
        let elem = text_elem(&format!("{a} ... {b}"));
        astate = Some(::arrayfuncs::build::accum_array_result(
            mcx,
            astate,
            Datum::from_usize(elem.as_ptr() as usize),
            false,
            ::types_core::TEXTOID,
        )?);
    }
    if ranges.nranges > 0 {
        s.push_str(&format!(" ranges: {}", anyarray_text(astate.as_ref().expect("nranges > 0"))?));
    }

    let mut astate: Option<::datum::array_build::ArrayBuildState<'_>> = None;
    for _ in 0..ranges.nvalues {
        let a = call_out(&mut out_fn, ranges.values[idx])?;
        idx += 1;
        let elem = text_elem(&a);
        astate = Some(::arrayfuncs::build::accum_array_result(
            mcx,
            astate,
            Datum::from_usize(elem.as_ptr() as usize),
            false,
            ::types_core::TEXTOID,
        )?);
    }
    if ranges.nvalues > 0 {
        s.push_str(&format!(" values: {}", anyarray_text(astate.as_ref().expect("nvalues > 0"))?));
    }

    s.push('}');

    let mut out: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, s.len() + 1)
        .map_err(|_| mcx.oom(s.len() + 1))?;
    out.extend_from_slice(s.as_bytes());
    out.push(0);
    Ok(::fmgr::cstring_result(out))
}

// brin_minmax_multi.c brin_minmax_multi_summary_send: `return byteasend(fcinfo);`
// — the summary is serialized as a bytea, so binary output is plain byteasend
// (varlena.c). Mirrors the bloom sibling's fc_summary_send.
fn fc_summary_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    varlena::builtins::fc_byteasend(None, fcinfo)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: ::fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

fn fc_options(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: index_opclass_options passes &mut LocalRelopts as arg 0 (C
    // PG_GETARG_POINTER protocol).
    let relopts = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut reloptions::LocalRelopts) };
    relopts.init(crate::MINMAXMULTIOPTIONS_SIZE);
    relopts.add_int(
        "values_per_range",
        crate::MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE,
        8,
        256,
        crate::MINMAXMULTIOPTIONS_VPR_OFF,
    );
    Ok(Datum::from_usize(0))
}

pub const MINMAX_MULTI_BUILTINS: &[FmgrBuiltin] = &[
    b(4620, "brin_minmax_multi_options", 1, fc_options),
    b(4621, "brin_minmax_multi_distance_int2", 2, fc_dist_int2),
    b(4622, "brin_minmax_multi_distance_int4", 2, fc_dist_int4),
    b(4623, "brin_minmax_multi_distance_int8", 2, fc_dist_int8),
    b(4624, "brin_minmax_multi_distance_float4", 2, fc_dist_float4),
    b(4625, "brin_minmax_multi_distance_float8", 2, fc_dist_float8),
    b(4626, "brin_minmax_multi_distance_numeric", 2, fc_dist_numeric),
    b(4627, "brin_minmax_multi_distance_tid", 2, fc_dist_tid),
    b(4628, "brin_minmax_multi_distance_uuid", 2, fc_dist_uuid),
    b(4629, "brin_minmax_multi_distance_date", 2, fc_dist_date),
    b(4630, "brin_minmax_multi_distance_time", 2, fc_dist_time),
    b(4631, "brin_minmax_multi_distance_interval", 2, fc_dist_interval),
    b(4632, "brin_minmax_multi_distance_timetz", 2, fc_dist_timetz),
    b(4633, "brin_minmax_multi_distance_pg_lsn", 2, fc_dist_pg_lsn),
    b(4634, "brin_minmax_multi_distance_macaddr", 2, fc_dist_macaddr),
    b(4635, "brin_minmax_multi_distance_macaddr8", 2, fc_dist_macaddr8),
    b(4636, "brin_minmax_multi_distance_inet", 2, fc_dist_inet),
    b(4637, "brin_minmax_multi_distance_timestamp", 2, fc_dist_timestamp),
    b(4638, "brin_minmax_multi_summary_in", 1, fc_summary_in),
    b(4639, "brin_minmax_multi_summary_out", 1, fc_summary_out),
    b(4640, "brin_minmax_multi_summary_recv", 1, fc_summary_recv),
    b(4641, "brin_minmax_multi_summary_send", 1, fc_summary_send),
];

#[cfg(test)]
mod send_tests {
    use super::*;
    use ::mcx::MemoryContext;

    // C brin_minmax_multi.c brin_minmax_multi_summary_send is exactly
    // `return byteasend(fcinfo);` — the wire image is the bytea payload
    // unchanged. Red at base: the stub errored 0A000 "binary output of type
    // brin_minmax_multi_summary is not supported" where C sends bytes
    // (inspect-audit residual F-send). The value has no in/recv constructor
    // on this tree, so the binary-COPY path is pinned as a direct unit.
    #[test]
    fn summary_send_is_byteasend() {
        let ctx = MemoryContext::new("t");
        let payload: &[u8] = &[0x01, 0x02, 0xff, 0x00, 0x7f];
        let mut image = Vec::with_capacity(payload.len() + 4);
        image.extend_from_slice(&::datum::varlena::set_varsize_4b(payload.len() + 4));
        image.extend_from_slice(payload);

        let mut fci = ::fmgr::LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { fci.set_result_mcx(ctx.mcx()) };
        fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
        let d = fc_summary_send(None, &mut fci).unwrap();
        // SAFETY: byteasend returns an inline bytea image.
        let v = unsafe { ::fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
        assert_eq!(v.data(), payload);
    }
}
