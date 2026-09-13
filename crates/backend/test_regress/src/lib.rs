// regress.c as an in-process ported library: registered under the dfmgr
// registry key `regress`; there is no real .so.
#![allow(non_upper_case_globals)]

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use ::datum::Datum;
use ::fmgr::{
    byref_result, cstring_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};
use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_FUNCTION, WARNING,
};
use ::types_nodes::supportnodes;

const LIBRARY: &str = "regress";
const NAMEDATALEN: usize = 64;

fn err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

fn warn(msg: String) -> PgResult<()> {
    elog_seams::ereport_msg::call(WARNING, msg, None)
}

fn out_cstring(fcinfo: &Fcinfo, bytes: &[u8]) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut v: ::mcx::PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, bytes.len() + 1)?;
    ::mcx::vec_append_bytes(&mut v, bytes)?;
    v.push(0);
    Ok(cstring_result(v))
}

fn out_text(fcinfo: &Fcinfo, payload: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(fcinfo.result_mcx(), payload)?))
}

// SAFETY contract of callers: the declared arg is a non-null live varlena.
unsafe fn arg_text<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    Ok(unsafe { fcinfo.arg_varlena_packed(i) }?.data())
}

unsafe fn arg_text_str(fcinfo: &Fcinfo, i: usize) -> PgResult<String> {
    Ok(String::from_utf8_lossy(unsafe { arg_text(fcinfo, i) }?).into_owned())
}

/* ======================== interpt_pp(path, path) ========================= */

// SAFETY: strict fn; catalog arg i is a non-null path varlena, live for the call.
unsafe fn arg_path<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<::adt_geo::PathRef<'a>> {
    Ok(::adt_geo::PathRef::from_payload(unsafe { fcinfo.arg_varlena_packed(i) }?.data()))
}

fn fc_interpt_pp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::adt_geo::lseg::{lseg_interpt, lseg_intersect, statlseg_construct};
    use ::adt_geo::Pts;

    // SAFETY: strict fn; catalog args are non-null path varlenas.
    let p1 = unsafe { arg_path(fcinfo, 0) }?;
    let p2 = unsafe { arg_path(fcinfo, 1) }?;

    let mut found = None;
    'outer: for i in 0..p1.n().saturating_sub(1) {
        let seg1 = statlseg_construct(&p1.pt(i), &p1.pt(i + 1));
        for j in 0..p2.n().saturating_sub(1) {
            let seg2 = statlseg_construct(&p2.pt(j), &p2.pt(j + 1));
            if lseg_intersect(&seg1, &seg2)? {
                found = Some((seg1, seg2));
                break 'outer;
            }
        }
    }

    let Some((seg1, seg2)) = found else {
        return Ok(fcinfo.return_null());
    };

    // The two segments are known to intersect, so lseg_interpt cannot return None.
    let pt = lseg_interpt(&seg1, &seg2)?.expect("intersecting segments always yield a point");
    byref_result(fcinfo.result_mcx(), &pt.to_datum_bytes())
}

/* ============================ overpaid(emp) ============================== */

fn fc_overpaid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::types_tuple::{
        heap_deform_tuple, HeapTupleData, HeapTupleHeaderData, ItemPointerData,
    };
    let (salary_null, salary) = {
        let mcx = fcinfo.result_mcx();
        // SAFETY: strict fn; catalog arg 0 is a non-null composite datum.
        let p = unsafe { fcinfo.arg_ptr(0) };
        // SAFETY: a live varlena-headed composite image.
        let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
        // SAFETY: `total` readable bytes at p, per the datum contract.
        let raw = unsafe { core::slice::from_raw_parts(p, total) };
        let rec = ::detoast_seams::detoast_attr::call(mcx, raw)?;
        // SAFETY: detoasted composite image; header prefix is in bounds.
        let hdr = unsafe { &*(rec.as_ptr() as *const HeapTupleHeaderData) };
        let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, hdr.type_id(), hdr.typmod())?;
        let ncolumns = tupdesc.natts as usize;
        // SAFETY: MAXALIGN'd detoasted image of datum_length() bytes.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                rec.as_ptr(),
                hdr.datum_length(),
                ItemPointerData::invalid(),
                ::types_core::InvalidOid,
            )
        };
        let mut values: ::mcx::PgVec<'_, Datum> =
            ::mcx::vec_from_elem_in(mcx, Datum::null(), ncolumns);
        let mut nulls: ::mcx::PgVec<'_, bool> = ::mcx::vec_from_elem_in(mcx, true, ncolumns);
        heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut nulls);

        let mut attrno: Option<usize> = None;
        for i in 0..ncolumns {
            let att = &tupdesc.attrs[i];
            if !att.attisdropped && att.attname.name_str() == b"salary" {
                attrno = Some(i);
                break;
            }
        }
        let Some(i) = attrno else {
            return Err(err("attribute \"salary\" does not exist".to_string()));
        };
        (nulls[i], values[i].as_i32())
    };
    if salary_null {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    Ok(Datum::from_bool(salary > 699))
}

/* ===================== widget: in / out / pt_in_widget =================== */

// C atof(): strtod's longest valid leading prefix (hex floats, inf, nan
// included), else 0.0.
fn c_atof(bytes: &[u8]) -> f64 {
    let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let Ok(c) = std::ffi::CString::new(&bytes[..nul]) else {
        return 0.0;
    };
    // SAFETY: NUL-terminated buffer; strtod reads within it.
    unsafe { libc::strtod(c.as_ptr(), core::ptr::null_mut()) }
}

// C "%g" (precision 6): %e when the decimal exponent is < -4 or >= 6, else
// %f, both with trailing zeros (and a bare '.') stripped.
fn fmt_g(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0".into() } else { "0".into() };
    }
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return if x < 0.0 { "-inf".into() } else { "inf".into() };
    }
    let s = format!("{x:.5e}");
    let (m, e) = s.split_once('e').expect("{:e} always has an exponent");
    let exp: i32 = e.parse().expect("exponent is an integer");
    if exp < -4 || exp >= 6 {
        let m = m.trim_end_matches('0').trim_end_matches('.');
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{m}e{sign}{:02}", exp.abs())
    } else {
        let prec = (5 - exp).max(0) as usize;
        let t = format!("{x:.prec$}");
        if t.contains('.') {
            t.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            t
        }
    }
}

const LDELIM: u8 = b'(';
const RDELIM: u8 = b')';
const DELIM: u8 = b',';
const WIDGET_NARGS: usize = 3;

fn fc_widget_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let bytes = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mut coord = [0usize; WIDGET_NARGS];
    let mut i = 0usize;
    let mut p = 0usize;
    while p < bytes.len() && i < WIDGET_NARGS && bytes[p] != RDELIM {
        if bytes[p] == DELIM || (bytes[p] == LDELIM && i == 0) {
            coord[i] = p + 1;
            i += 1;
        }
        p += 1;
    }
    if i < WIDGET_NARGS {
        // Note (regress.c): DON'T convert this to a soft error.
        return Err(Box::new(
            PgError::error(format!(
                "invalid input syntax for type widget: \"{}\"",
                String::from_utf8_lossy(bytes)
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ));
    }
    let mut image = [0u8; 24];
    image[0..8].copy_from_slice(&c_atof(&bytes[coord[0]..]).to_ne_bytes());
    image[8..16].copy_from_slice(&c_atof(&bytes[coord[1]..]).to_ne_bytes());
    image[16..24].copy_from_slice(&c_atof(&bytes[coord[2]..]).to_ne_bytes());
    byref_result(fcinfo.result_mcx(), &image)
}

fn fc_widget_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg type widget is 24B fixed by-ref, non-null (strict).
    let b = unsafe { fcinfo.arg_fixed(0, 24) };
    let cx = f64::from_ne_bytes(b[0..8].try_into().unwrap());
    let cy = f64::from_ne_bytes(b[8..16].try_into().unwrap());
    let radius = f64::from_ne_bytes(b[16..24].try_into().unwrap());
    out_cstring(
        fcinfo,
        format!("({},{},{})", fmt_g(cx), fmt_g(cy), fmt_g(radius)).as_bytes(),
    )
}

fn fc_pt_in_widget(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::types_core::geo::Point;
    // SAFETY: strict fn; point is 16B fixed by-ref, widget 24B fixed by-ref.
    let point = Point::from_datum_bytes(unsafe { fcinfo.arg_fixed(0, 16) });
    let w = unsafe { fcinfo.arg_fixed(1, 24) };
    let center = Point::from_datum_bytes(&w[0..16]);
    let radius = f64::from_ne_bytes(w[16..24].try_into().unwrap());
    let distance = adt_geo::point_dt(&point, &center)?;
    Ok(Datum::from_bool(distance < radius))
}

/* ========================== reverse_name(name) =========================== */

fn fc_reverse_name(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn; a name value is a NUL-terminated NAMEDATALEN buffer
    // read as a cstring (C: PG_GETARG_CSTRING over the name arg).
    let string = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mut out = [0u8; NAMEDATALEN];
    let mut i: isize = 0;
    while (i as usize) < NAMEDATALEN && (i as usize) < string.len() {
        i += 1;
    }
    if i as usize == NAMEDATALEN || (i as usize) >= string.len() {
        i -= 1;
    }
    let len = i;
    while i >= 0 {
        out[(len - i) as usize] = string[i as usize];
        i -= 1;
    }
    byref_result(fcinfo.result_mcx(), &out)
}

/* ==================== int44in / int44out (city_budget) =================== */

fn fc_int44in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: typinput arg0 is a non-null cstring.
    let bytes = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // sscanf "%d, %d, %d, %d": the ',' literal must match exactly; missing
    // slots are 0.
    let mut result = [0i32; 4];
    let mut p = 0usize;
    let mut i = 0usize;
    'scan: while i < 4 {
        while p < bytes.len() && (bytes[p] as char).is_ascii_whitespace() {
            p += 1;
        }
        let start = p;
        if p < bytes.len() && (bytes[p] == b'+' || bytes[p] == b'-') {
            p += 1;
        }
        let digits = p;
        while p < bytes.len() && bytes[p].is_ascii_digit() {
            p += 1;
        }
        if p == digits {
            break 'scan;
        }
        result[i] = core::str::from_utf8(&bytes[start..p])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .map(|v| v as i32)
            .unwrap_or(i32::MAX);
        i += 1;
        if i < 4 {
            if p < bytes.len() && bytes[p] == b',' {
                p += 1;
            } else {
                break 'scan;
            }
        }
    }
    let mut image = [0u8; 16];
    for (j, v) in result.iter().enumerate() {
        image[j * 4..j * 4 + 4].copy_from_slice(&v.to_ne_bytes());
    }
    byref_result(fcinfo.result_mcx(), &image)
}

fn fc_int44out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg type city_budget is 16B fixed by-ref (strict).
    let b = unsafe { fcinfo.arg_fixed(0, 16) };
    let mut a = [0i32; 4];
    for (j, slot) in a.iter_mut().enumerate() {
        *slot = i32::from_ne_bytes(b[j * 4..j * 4 + 4].try_into().unwrap());
    }
    out_cstring(fcinfo, format!("{},{},{},{}", a[0], a[1], a[2], a[3]).as_bytes())
}

/* ==================== test_canonicalize_path(text) ======================= */

fn fc_test_canonicalize_path(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, text arg.
    let path = unsafe { arg_text_str(fcinfo, 0) }?;
    out_text(fcinfo, pg_path::canonicalize_path(&path).as_bytes())
}

/* ===================== make_tuple_indirect(record) ======================= */

// Marker byte for a 1-byte-header "external" varlena (VARATT_IS_1B_E); this
// codebase's varlena helpers assume little-endian throughout (see detoast.rs).
const VARATT_EXTERNAL_MARKER: u8 = 0x01;

#[inline]
fn regress_is_external(b: &[u8]) -> bool {
    b[0] == VARATT_EXTERNAL_MARKER
}

#[inline]
fn regress_is_external_ondisk(b: &[u8]) -> bool {
    regress_is_external(b) && b[1] == ::types_tuple::varatt::VARTAG_ONDISK
}

#[inline]
fn regress_is_external_indirect(b: &[u8]) -> bool {
    regress_is_external(b) && b[1] == ::types_tuple::varatt::VARTAG_INDIRECT
}

/// # Safety
/// `d` carries a live varlena pointer readable for its full VARSIZE_ANY.
unsafe fn regress_va_slice<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract.
    unsafe {
        let len = ::types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, len)
    }
}

// C allocates the indirect targets (and the returned tuple) in
// TopTransactionContext so they outlive the call: the tuple image gets
// copied around by the executor, the pointed-to targets do not. Rust
// analogue per the pg_enum precedent: a retained backend-life leaked arena
// (test-only path; the regress file's footprint is bounded).
fn indirect_target_mcx() -> ::mcx::Mcx<'static> {
    thread_local! {
        static TCX: ::mcx::Mcx<'static> =
            ::mcx::session_root("MakeTupleIndirectTargets").mcx();
    }
    TCX.with(|m| *m)
}

fn fc_make_tuple_indirect(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 is a non-null composite datum (strict fn).
    let raw = unsafe { fcinfo.arg_varlena_raw(0) };
    let rec = detoast_seams::detoast_attr::call(mcx, raw)?;
    debug_assert!(rec.len() >= ::types_tuple::SizeofHeapTupleHeader);
    // SAFETY: detoasted composite image; header prefix is in bounds.
    let hdr = unsafe { &*(rec.as_ptr() as *const ::types_tuple::HeapTupleHeaderData) };
    let tup_type = hdr.type_id();
    let tup_typmod = hdr.typmod();
    let tupdesc = ::typcache::lookup_rowtype_tupdesc_copy(mcx, tup_type, tup_typmod)?;
    let ncolumns = tupdesc.natts as usize;

    // SAFETY: MAXALIGN'd detoasted image of datum_length() == rec.len() bytes.
    let tuple = unsafe {
        ::types_tuple::HeapTupleData::from_raw_parts(
            rec.as_ptr(),
            hdr.datum_length(),
            ::types_tuple::ItemPointerData::invalid(),
            ::types_core::InvalidOid,
        )
    };

    let mut values: ::mcx::PgVec<'_, Datum> =
        ::mcx::vec_from_elem_in(mcx, Datum::null(), ncolumns);
    let mut nulls: ::mcx::PgVec<'_, bool> = ::mcx::vec_from_elem_in(mcx, true, ncolumns);
    ::types_tuple::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut nulls);

    for i in 0..ncolumns {
        let att = &tupdesc.attrs[i];
        // only work on existing, not-null varlenas
        if att.attisdropped
            || nulls[i]
            || att.attlen != -1
            || att.attstorage == ::types_tuple::TYPSTORAGE_PLAIN
        {
            continue;
        }

        // SAFETY: non-null varlena datum produced by heap_deform_tuple.
        let attr = unsafe { regress_va_slice(values[i]) };

        // don't recursively indirect
        if regress_is_external_indirect(attr) {
            continue;
        }

        // copy datum, so it still lives later (C: TopTransactionContext)
        let tcx = indirect_target_mcx();
        let copy: ::mcx::PgVec<'static, u8> = if regress_is_external_ondisk(attr) {
            detoast::detoast_external_attr(tcx, attr)?
        } else {
            let mut v = ::mcx::vec_with_capacity_in(tcx, attr.len())?;
            ::mcx::vec_append_bytes(&mut v, attr)?;
            v
        };
        let target_ptr = copy.leak().as_ptr() as usize;

        // build indirection Datum: 1-byte-header external tag + a raw
        // pointer to the copy (C's `struct varatt_indirect`).
        let mut wrapper = [0u8; ::types_tuple::varatt::VARHDRSZ_EXTERNAL + 8];
        wrapper[0] = VARATT_EXTERNAL_MARKER;
        wrapper[1] = ::types_tuple::varatt::VARTAG_INDIRECT;
        wrapper[::types_tuple::varatt::VARHDRSZ_EXTERNAL..].copy_from_slice(&target_ptr.to_ne_bytes());
        let new_attr = byref_result(mcx, &wrapper)?;

        values[i] = new_attr;
    }

    // C forms the result inside the TopTransactionContext switch as well.
    let newtup = ::heaptuple::heap_form_tuple(indirect_target_mcx(), &tupdesc, &values, &nulls)?;
    // Intentionally not flattened through the normal composite-result path
    // (C's comment): returning t_data as-is keeps the indirect pointers live
    // for later statements to exercise, matching regress.c's contract.
    let t_data_ptr = newtup.header_ptr() as usize;
    core::mem::forget(newtup);
    Ok(Datum::from_usize(t_data_ptr))
}

/* ========================= get_environ() ================================= */

fn fc_get_environ(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let env: Vec<String> = std::env::vars().map(|(k, v)| format!("{k}={v}")).collect();
    let mut datums: Vec<Datum> = Vec::with_capacity(env.len());
    for s in &env {
        datums.push(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?));
    }
    let (elmlen, elmbyval, elmalign) =
        arrayfuncs::construct::builtin_meta(::types_core::TEXTOID).expect("text is a builtin");
    let arr = arrayfuncs::construct::construct_array(
        mcx,
        &datums,
        ::types_core::TEXTOID,
        elmlen,
        elmbyval,
        elmalign,
    )?;
    let d = Datum::from_usize(arr.as_ptr() as usize);
    core::mem::forget(arr);
    Ok(d)
}

/* ===================== regress_setenv(text, text) ======================== */

fn fc_regress_setenv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, text args.
    let envvar = unsafe { arg_text_str(fcinfo, 0) }?;
    let envval = unsafe { arg_text_str(fcinfo, 1) }?;
    if !superuser::superuser()? {
        return Err(err("must be superuser to change environment variables".to_string()));
    }
    let envvar_c = std::ffi::CString::new(envvar).map_err(|_| {
        err("could not set environment variable: Invalid argument".to_string())
    })?;
    let envval_c = std::ffi::CString::new(envval).map_err(|_| {
        err("could not set environment variable: Invalid argument".to_string())
    })?;
    #[cfg(target_family = "wasm")]
    {
        let _ = (envvar_c, envval_c);
        return Err(err(
            "could not set environment variable: Function not implemented".to_string(),
        ));
    }
    #[cfg(not(target_family = "wasm"))]
    {
        // SAFETY: CStrings are NUL-terminated; setenv copies them.
        if unsafe { libc::setenv(envvar_c.as_ptr(), envval_c.as_ptr(), 1) } != 0 {
            let errno = std::io::Error::last_os_error();
            return Err(err(format!("could not set environment variable: {errno}")));
        }
        Ok(Datum::null())
    }
}

/* ============================ wait_pid(int4) ============================= */

fn fc_wait_pid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let pid = fcinfo.arg_i32(0);
    if !superuser::superuser()? {
        return Err(err("must be superuser to check PID liveness".to_string()));
    }
    loop {
        // wasm32: no processes and no kill(2) on WASI — every probed pid is
        // dead, so the wait returns immediately (matches miscinit's
        // pid_appears_live wasm arm).
        #[cfg(target_family = "wasm")]
        {
            let _ = pid;
            break;
        }
        // SAFETY: kill(pid, 0) sends nothing; it only probes liveness.
        #[cfg(not(target_family = "wasm"))]
        if unsafe { libc::kill(pid as libc::pid_t, 0) } != 0 {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno != libc::ESRCH {
                return Err(err(format!(
                    "could not check PID {pid} liveness: {}",
                    std::io::Error::from_raw_os_error(errno)
                )));
            }
            break;
        }
        // CHECK_FOR_INTERRUPTS() — route a pending interrupt through the
        // ported ProcessInterrupts seam (the gist/spgist/gin/hash pattern).
        if init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        std::thread::sleep(std::time::Duration::from_micros(50000));
    }
    Ok(Datum::null())
}

/* ========================= test_atomic_ops() ============================= */

fn expect_true(cond: bool, what: &str) -> PgResult<()> {
    if cond {
        Ok(())
    } else {
        Err(err(format!("{what} was unexpectedly false")))
    }
}

fn test_atomic_flag() -> PgResult<()> {
    let flag = AtomicBool::new(false);
    expect_true(!flag.load(Ordering::SeqCst), "pg_atomic_unlocked_test_flag(&flag)")?;
    expect_true(!flag.swap(true, Ordering::SeqCst), "pg_atomic_test_set_flag(&flag)")?;
    expect_true(flag.load(Ordering::SeqCst), "!pg_atomic_unlocked_test_flag(&flag)")?;
    expect_true(flag.swap(true, Ordering::SeqCst), "!pg_atomic_test_set_flag(&flag)")?;
    flag.store(false, Ordering::SeqCst);
    expect_true(!flag.load(Ordering::SeqCst), "pg_atomic_unlocked_test_flag(&flag)")?;
    expect_true(!flag.swap(true, Ordering::SeqCst), "pg_atomic_test_set_flag(&flag)")?;
    Ok(())
}

fn eq_u32(actual: u32, expected: u32, what: &str) -> PgResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(err(format!("{what} yielded {actual}, expected {expected}")))
    }
}

fn test_atomic_uint32() -> PgResult<()> {
    const SEQ: Ordering = Ordering::SeqCst;
    let var = AtomicU32::new(0);
    eq_u32(var.load(SEQ), 0, "pg_atomic_read_u32(&var)")?;
    var.store(3, SEQ);
    eq_u32(var.load(SEQ), 3, "pg_atomic_read_u32(&var)")?;
    let d = var.load(SEQ).wrapping_sub(2);
    eq_u32(var.fetch_add(d, SEQ), 3, "pg_atomic_fetch_add_u32")?;
    eq_u32(var.fetch_sub(1, SEQ), 4, "pg_atomic_fetch_sub_u32")?;
    eq_u32(var.fetch_sub(3, SEQ).wrapping_sub(3), 0, "pg_atomic_sub_fetch_u32")?;
    eq_u32(var.fetch_add(10, SEQ).wrapping_add(10), 10, "pg_atomic_add_fetch_u32")?;
    eq_u32(var.swap(5, SEQ), 10, "pg_atomic_exchange_u32")?;
    eq_u32(var.swap(0, SEQ), 5, "pg_atomic_exchange_u32")?;

    const INT_MAX: u32 = i32::MAX as u32;
    const INT16_MAX: u32 = i16::MAX as u32;
    const INT16_MIN: u32 = i16::MIN as i32 as u32;
    eq_u32(var.fetch_add(INT_MAX, SEQ), 0, "fetch_add INT_MAX")?;
    eq_u32(var.fetch_add(INT_MAX, SEQ), INT_MAX, "fetch_add INT_MAX")?;
    var.fetch_add(2, SEQ);
    eq_u32(var.fetch_add(INT16_MAX, SEQ), 0, "fetch_add PG_INT16_MAX")?;
    eq_u32(var.fetch_add(INT16_MAX + 1, SEQ), INT16_MAX, "fetch_add PG_INT16_MAX+1")?;
    eq_u32(var.fetch_add(INT16_MIN, SEQ), 2 * INT16_MAX + 1, "fetch_add PG_INT16_MIN")?;
    eq_u32(
        var.fetch_add(INT16_MIN.wrapping_sub(1), SEQ),
        INT16_MAX,
        "fetch_add PG_INT16_MIN-1",
    )?;
    var.fetch_add(1, SEQ);
    eq_u32(var.load(SEQ), u32::MAX, "pg_atomic_read_u32(&var)")?;
    eq_u32(var.fetch_sub(INT_MAX, SEQ), u32::MAX, "fetch_sub INT_MAX")?;
    eq_u32(var.load(SEQ), INT_MAX + 1, "pg_atomic_read_u32(&var)")?;
    eq_u32(var.fetch_sub(INT_MAX, SEQ).wrapping_sub(INT_MAX), 1, "sub_fetch INT_MAX")?;
    var.fetch_sub(1, SEQ);
    for exp in [INT16_MAX, INT16_MAX + 1, INT16_MIN, INT16_MIN.wrapping_sub(1), 10] {
        expect_true(
            var.compare_exchange(exp, 1, SEQ, SEQ).is_err(),
            "!pg_atomic_compare_exchange_u32(&var, &expected, 1)",
        )?;
    }
    let mut ok = false;
    for _ in 0..1000 {
        if var.compare_exchange(0, 1, SEQ, SEQ).is_ok() {
            ok = true;
            break;
        }
    }
    if !ok {
        return Err(err("atomic_compare_exchange_u32() never succeeded".to_string()));
    }
    eq_u32(var.load(SEQ), 1, "pg_atomic_read_u32(&var)")?;
    var.store(0, SEQ);
    expect_true(var.fetch_or(1, SEQ) & 1 == 0, "!(pg_atomic_fetch_or_u32(&var, 1) & 1)")?;
    expect_true(var.fetch_or(2, SEQ) & 1 != 0, "pg_atomic_fetch_or_u32(&var, 2) & 1")?;
    eq_u32(var.load(SEQ), 3, "pg_atomic_read_u32(&var)")?;
    eq_u32(var.fetch_and(!2u32, SEQ) & 3, 3, "pg_atomic_fetch_and_u32(&var, ~2) & 3")?;
    eq_u32(var.fetch_and(!1u32, SEQ), 1, "pg_atomic_fetch_and_u32(&var, ~1)")?;
    eq_u32(var.fetch_and(!0u32, SEQ), 0, "pg_atomic_fetch_and_u32(&var, ~0)")?;
    Ok(())
}

fn eq_u64(actual: u64, expected: u64, what: &str) -> PgResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(err(format!("{what} yielded {actual}, expected {expected}")))
    }
}

fn test_atomic_uint64() -> PgResult<()> {
    const SEQ: Ordering = Ordering::SeqCst;
    let var = AtomicU64::new(0);
    eq_u64(var.load(SEQ), 0, "pg_atomic_read_u64(&var)")?;
    var.store(3, SEQ);
    eq_u64(var.load(SEQ), 3, "pg_atomic_read_u64(&var)")?;
    let d = var.load(SEQ).wrapping_sub(2);
    eq_u64(var.fetch_add(d, SEQ), 3, "pg_atomic_fetch_add_u64")?;
    eq_u64(var.fetch_sub(1, SEQ), 4, "pg_atomic_fetch_sub_u64")?;
    eq_u64(var.fetch_sub(3, SEQ).wrapping_sub(3), 0, "pg_atomic_sub_fetch_u64")?;
    eq_u64(var.fetch_add(10, SEQ).wrapping_add(10), 10, "pg_atomic_add_fetch_u64")?;
    eq_u64(var.swap(5, SEQ), 10, "pg_atomic_exchange_u64")?;
    eq_u64(var.swap(0, SEQ), 5, "pg_atomic_exchange_u64")?;
    expect_true(
        var.compare_exchange(10, 1, SEQ, SEQ).is_err(),
        "!pg_atomic_compare_exchange_u64(&var, &expected, 1)",
    )?;
    let mut ok = false;
    for _ in 0..100 {
        if var.compare_exchange(0, 1, SEQ, SEQ).is_ok() {
            ok = true;
            break;
        }
    }
    if !ok {
        return Err(err("atomic_compare_exchange_u64() never succeeded".to_string()));
    }
    eq_u64(var.load(SEQ), 1, "pg_atomic_read_u64(&var)")?;
    var.store(0, SEQ);
    expect_true(var.fetch_or(1, SEQ) & 1 == 0, "!(pg_atomic_fetch_or_u64(&var, 1) & 1)")?;
    expect_true(var.fetch_or(2, SEQ) & 1 != 0, "pg_atomic_fetch_or_u64(&var, 2) & 1")?;
    eq_u64(var.load(SEQ), 3, "pg_atomic_read_u64(&var)")?;
    eq_u64(var.fetch_and(!2u64, SEQ) & 3, 3, "pg_atomic_fetch_and_u64(&var, ~2) & 3")?;
    eq_u64(var.fetch_and(!1u64, SEQ), 1, "pg_atomic_fetch_and_u64(&var, ~1)")?;
    eq_u64(var.fetch_and(!0u64, SEQ), 0, "pg_atomic_fetch_and_u64(&var, ~0)")?;
    Ok(())
}

// s_lock.c:98 s_lock: the contended-acquisition loop (TAS_SPIN + the
// perform/finish_spin_delay backoff); returns the delay count like C.
fn s_lock(
    lock: &types_storage::Spinlock,
    file: &'static str,
    line: i32,
    func: &'static str,
) -> i32 {
    let mut delay_status = s_lock_seams::SpinDelayStatus::new(file, line, func);
    while lock.tas_spin() != 0 {
        s_lock_seams::perform_spin_delay::call(&mut delay_status);
    }
    s_lock_seams::finish_spin_delay::call(&delay_status);
    delay_status.delays
}

// s_lock.h S_LOCK: one TAS, then s_lock() for the contended case.
fn spin_lock_acquire(lock: &types_storage::Spinlock, func: &'static str) {
    if lock.tas() != 0 {
        s_lock(lock, file!(), line!() as i32, func);
    }
}

// regress.c:638 test_spinlock: the spinlock embedded between two padding
// members, driven through the SpinLock*/S_* API, s_lock(), and TAS/TAS_SPIN
// (types_storage::Spinlock is the port's slock_t: tas/tas_spin/unlock).
fn test_spinlock() -> PgResult<()> {
    #[repr(C)]
    struct TestLockStruct {
        data_before: [u8; 4],
        lock: types_storage::Spinlock,
        data_after: [u8; 4],
    }
    let s = TestLockStruct {
        data_before: *b"abcd",
        lock: types_storage::Spinlock::new(),
        data_after: *b"ef12",
    };

    /* test basic operations via the SpinLock* API (S_INIT_LOCK == S_UNLOCK) */
    s.lock.unlock();
    spin_lock_acquire(&s.lock, "test_spinlock");
    s.lock.unlock();

    /* test basic operations via underlying S_* API */
    s.lock.unlock();
    spin_lock_acquire(&s.lock, "test_spinlock");
    s.lock.unlock();

    /* and that "contended" acquisition works */
    s_lock(&s.lock, "testfile", 17, "testfunc");
    s.lock.unlock();

    /*
     * Check, using TAS directly, that a single spin cycle doesn't block
     * when acquiring an already acquired lock.
     */
    spin_lock_acquire(&s.lock, "test_spinlock");
    if s.lock.tas() == 0 {
        return Err(err("acquired already held spinlock".to_string()));
    }
    if s.lock.tas_spin() == 0 {
        return Err(err("acquired already held spinlock".to_string()));
    }
    s.lock.unlock();

    /*
     * Verify that after all of this the non-lock contents are still
     * correct.
     */
    if &s.data_before != b"abcd" {
        return Err(err("padding before spinlock modified".to_string()));
    }
    if &s.data_after != b"ef12" {
        return Err(err("padding after spinlock modified".to_string()));
    }
    Ok(())
}

fn fc_test_atomic_ops(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    test_atomic_flag()?;
    test_atomic_uint32()?;
    test_atomic_uint64()?;
    test_spinlock()?;
    Ok(Datum::from_bool(true))
}

/* ========================= test_fdw_handler() ============================ */

fn fc_test_fdw_handler(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(err("test_fdw_handler is not implemented".to_string()))
}

/* ================ is_catalog_text_unique_index_oid(oid) ================== */

fn fc_is_catalog_text_unique_index_oid(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_bool(catalog::IsCatalogTextUniqueIndexOid(fcinfo.arg_oid(0))))
}

/* ====================== test_support_func(internal) ====================== */

// regress.c test_support_func: assumes its subject is int4eq (selectivity)
// or generate_series_int4 (rows).
fn fc_test_support_func(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    const Int4EqualOperator: ::types_core::Oid = 96;
    let raw = fcinfo.arg(0).as_usize() as *mut ();
    let mut ret = 0usize;
    // SAFETY (each demux): prosupport contract — arg 0 is a live tag-first
    // support-request node owned by the planner for this call's duration.
    if let Some(req) = unsafe { supportnodes::support_request_selectivity_mut(raw) } {
        req.selectivity = (req.estimate)(Int4EqualOperator)?;
        ret = raw as usize;
    }
    if let Some(req) = unsafe { supportnodes::support_request_cost_mut(raw) } {
        req.startup = 0.0;
        req.per_tuple = 2.0 * (guc_tables::vars::cpu_operator_cost.get().get)();
        ret = raw as usize;
    }
    if let Some(req) = unsafe { supportnodes::support_request_rows_mut(raw) } {
        if let Some(fe) = req.node.and_then(|n| n.as_func_expr()) {
            let args: Vec<_> = fe.args.iter().collect();
            let (arg1, arg2) = (args[0].as_const(), args[1].as_const());
            if let (Some(c1), Some(c2)) = (arg1, arg2) {
                if !c1.constisnull && !c2.constisnull {
                    let val1 = c1.constvalue.as_i32();
                    let val2 = c2.constvalue.as_i32();
                    req.rows = (val2 - val1 + 1) as f64;
                    ret = raw as usize;
                }
            }
        }
    }
    Ok(Datum::from_usize(ret))
}

/* ================== test_opclass_options_func(internal) ================== */

fn fc_test_opclass_options_func(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    fcinfo.isnull = true;
    Ok(Datum::null())
}

/* ========================== test_enc_setup() ============================= */

fn fc_test_enc_setup(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    for i in 0..wchar::_PG_LAST_ENCODING_ {
        if wchar::pg_encoding_max_length(i) == 1 {
            continue;
        }
        let mut buf = [0u8; 2];
        wchar::pg_encoding_set_invalid(i, &mut buf);
        let name = mbutils::pg_encoding_to_char(i);
        let len = buf.iter().position(|&c| c == 0).unwrap_or(2);
        if len != 2 {
            warn(format!(
                "official invalid string for encoding \"{name}\" has length {len}"
            ))?;
        }
        let mblen = wchar::pg_encoding_mblen(i, &buf);
        if mblen != 2 {
            warn(format!(
                "official invalid string for encoding \"{name}\" has mblen {mblen}"
            ))?;
        }
        let valid = wchar::pg_encoding_verifymbstr(i, &buf[..len]);
        if valid != 0 {
            warn(format!(
                "official invalid string for encoding \"{name}\" has valid prefix of length {valid}"
            ))?;
        }
        let valid = wchar::pg_encoding_verifymbstr(i, &buf[..1]);
        if valid != 0 {
            warn(format!(
                "first byte of official invalid string for encoding \"{name}\" has valid prefix of length {valid}"
            ))?;
        }
        let mut bigbuf = [b' '; 16];
        bigbuf[0] = buf[0];
        bigbuf[1] = buf[1];
        let valid = wchar::pg_encoding_verifymbstr(i, &bigbuf);
        if valid != 0 {
            warn(format!(
                "trailing data changed official invalid string for encoding \"{name}\" to have valid prefix of length {valid}"
            ))?;
        }
    }
    Ok(Datum::null())
}

/* ============ test_enc_conversion(bytea, name, name, bool) =============== */

// SAFETY: strict fn; catalog arg i is a `name` datum, live for the call.
unsafe fn arg_name_str(fcinfo: &Fcinfo, i: usize) -> String {
    let raw = unsafe { fcinfo.arg_name(i) };
    let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end]).into_owned()
}

#[track_caller]
#[cold]
fn invalid_encoding_name_error(which: &str, name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid {which} encoding name \"{name}\""))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
fn no_default_conversion_error(src_encoding: i32, dest_encoding: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "default conversion function for encoding \"{}\" to \"{}\" does not exist",
            mbutils::pg_encoding_to_char(src_encoding),
            mbutils::pg_encoding_to_char(dest_encoding),
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
    )
}

// regress.c test_enc_conversion: OUT (validlen int, result bytea) row built via
// get_call_result_type + heap_form_tuple, the same convention as
// commit_ts::fmgr_builtins::composite_result / pg_controldata's builtins.
fn composite_result_2(
    flinfo: &FmgrInfo,
    fcinfo: &mut Fcinfo,
    values: &[Datum; 2],
    isnull: &[bool; 2],
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(err("return type must be a row type".to_string()));
    }
    let tupdesc = resolved
        .result_tuple_desc
        .expect("composite result has tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, values, isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

fn fc_test_enc_conversion(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("test_enc_conversion: NULL flinfo");

    // SAFETY: strict fn, bytea arg live for the call.
    let string = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let src_bytes = string.data();
    // SAFETY: strict fn, name args live for the call.
    let src_name = unsafe { arg_name_str(fcinfo, 1) };
    let dest_name = unsafe { arg_name_str(fcinfo, 2) };
    let no_error = fcinfo.arg_bool(3);

    let src_encoding = mbutils::pg_char_to_encoding(&src_name);
    if src_encoding < 0 {
        return Err(invalid_encoding_name_error("source", &src_name));
    }
    let dest_encoding = mbutils::pg_char_to_encoding(&dest_name);
    if dest_encoding < 0 {
        return Err(invalid_encoding_name_error("destination", &dest_name));
    }

    // SAFETY: mcx stays live for this call only; composite_result_2 re-derives
    // its own handle rather than reusing this one across the &mut borrow below.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    if src_encoding == dest_encoding {
        // No conversion possible: validate in place, byte-offset semantics
        // (mbutils::pg_verify_mbstr's oklen) so the SQL wrapper's substr()
        // math on `validlen` lines up with C's report_invalid_encoding cut.
        let oklen = wchar::pg_encoding_verifymbstr(src_encoding, src_bytes) as usize;
        if oklen != src_bytes.len() {
            if no_error {
                let result = varlena_result(varlena::cstring_to_text(mcx, &src_bytes[..oklen])?);
                return composite_result_2(
                    flinfo,
                    fcinfo,
                    &[Datum::from_i32(oklen as i32), result],
                    &[false, false],
                );
            }
            return Err(mbutils::report_invalid_encoding(
                src_encoding,
                &src_bytes[oklen..],
            ));
        }
        let result = varlena_result(varlena::cstring_to_text(mcx, src_bytes)?);
        return composite_result_2(
            flinfo,
            fcinfo,
            &[Datum::from_i32(oklen as i32), result],
            &[false, false],
        );
    }

    let proc = namespace_seams::find_default_conversion_proc::call(src_encoding, dest_encoding)?;
    if proc == types_core::InvalidOid {
        return Err(no_default_conversion_error(src_encoding, dest_encoding));
    }

    if src_bytes.len() >= ::mcx::MAX_ALLOC_SIZE / mbutils::MAX_CONVERSION_GROWTH {
        return Err(Box::new(
            PgError::error("out of memory")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_detail(format!(
                    "String of {} bytes is too long for encoding conversion.",
                    src_bytes.len() as i32
                )),
        ));
    }

    let cap = src_bytes.len() * mbutils::MAX_CONVERSION_GROWTH + 1;
    let (convertedlen, dest) = mbutils::pg_do_encoding_conversion_buf(
        mcx,
        proc,
        src_encoding,
        dest_encoding,
        src_bytes,
        cap as i32,
        no_error,
    )?;

    let result = varlena_result(varlena::cstring_to_text(mcx, &dest)?);
    drop(dest); // ends the mcx-tied borrow before the &mut fcinfo call below
    composite_result_2(
        flinfo,
        fcinfo,
        &[Datum::from_i32(convertedlen), result],
        &[false, false],
    )
}

/* ============== test_bytea_to_text / test_text_to_bytea ================== */

fn fc_test_bytea_to_text(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, varlena arg; C returns the detoasted input as-is.
    let pv = unsafe { fcinfo.arg_varlena_packed(0) }?;
    Ok(Datum::from_usize(pv.as_ptr() as usize))
}

fn fc_test_text_to_bytea(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, varlena arg.
    let pv = unsafe { fcinfo.arg_varlena_packed(0) }?;
    Ok(Datum::from_usize(pv.as_ptr() as usize))
}

/* ============== test_mblen_func(text, text, text, int4) ================== */

fn fc_test_mblen_func(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, text args.
    let func = unsafe { arg_text_str(fcinfo, 0) }?;
    let encoding = unsafe { arg_text_str(fcinfo, 1) }?;
    let data = unsafe { arg_text(fcinfo, 2) }?;
    let size = data.len();
    let offset = fcinfo.arg_i32(3) as usize;
    let at_offset = &data[offset.min(size)..];

    let result: i32 = match func.as_str() {
        "pg_mblen_unbounded" => mbutils::pg_mblen(at_offset),
        "pg_mblen_cstr" => mblen_cstr(at_offset)?,
        "pg_mblen_with_len" => mbutils::pg_mblen_with_len(at_offset, (size - offset) as i32)?,
        "pg_mblen_range" => mbutils::pg_mblen_range(at_offset)?,
        "pg_encoding_mblen" => {
            wchar::pg_encoding_mblen(mbutils::pg_char_to_encoding(&encoding), at_offset)
        }
        _ => return Err(err("unknown function".to_string())),
    };
    Ok(Datum::from_i32(result))
}

// pg_mblen_cstr (mbutils.c): the .mblen functions return 1 for a pointer to
// the terminator; otherwise the char is bounded at the first NUL, and the
// bounded-range walk over that window raises the same invalid-char error.
fn mblen_cstr(bytes: &[u8]) -> PgResult<i32> {
    if bytes.first().is_none_or(|&b| b == 0) {
        return Ok(1);
    }
    let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    mbutils::pg_mblen_range(&bytes[..nul])
}

/* ================== test_text_to_wchars(text, text) ====================== */

fn fc_test_text_to_wchars(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict fn, text args.
    let encoding_name = unsafe { arg_text_str(fcinfo, 0) }?;
    let data = unsafe { arg_text(fcinfo, 1) }?;
    let encoding = mbutils::pg_char_to_encoding(&encoding_name);
    if encoding < 0 {
        return Err(err(format!("unknown encoding name: {encoding_name}")));
    }
    let datums: Vec<Datum> = if !data.is_empty() {
        let wchars = mbutils::pg_encoding_mb2wchar_with_len(mcx, encoding, data)?;
        wchars.iter().map(|&w| Datum::from_i32(w as i32)).collect()
    } else {
        Vec::new()
    };
    let (elmlen, elmbyval, elmalign) =
        arrayfuncs::construct::builtin_meta(::types_core::INT4OID).expect("int4 is a builtin");
    let arr = arrayfuncs::construct::construct_array(
        mcx,
        &datums,
        ::types_core::INT4OID,
        elmlen,
        elmbyval,
        elmalign,
    )?;
    let d = Datum::from_usize(arr.as_ptr() as usize);
    core::mem::forget(arr);
    Ok(d)
}

/* ================= test_wchars_to_text(text, int4[]) ===================== */

fn fc_test_wchars_to_text(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict fn; arg0 text, arg1 int4[] varlena.
    let encoding_name = unsafe { arg_text_str(fcinfo, 0) }?;
    // SAFETY: strict fn; arg 1 is a non-null int4[] varlena.
    let p = unsafe { fcinfo.arg_ptr(1) };
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: a live varlena of `total` bytes.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let array = ::detoast_seams::detoast_attr::call(mcx, raw)?;
    let encoding = mbutils::pg_char_to_encoding(&encoding_name);
    if encoding < 0 {
        return Err(err(format!("unknown encoding name: {encoding_name}")));
    }
    let (datums, nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, &array, ::types_core::INT4OID, true)?;
    let bytes: Vec<u8> = if !datums.is_empty() {
        let mut wchars: Vec<wchar::pg_wchar> = Vec::with_capacity(datums.len());
        for (d, isnull) in datums.iter().zip(nulls.iter()) {
            if *isnull {
                return Err(err("unexpected NULL in array".to_string()));
            }
            wchars.push(d.as_i32() as wchar::pg_wchar);
        }
        mbutils::pg_encoding_wchar2mb_with_len(mcx, encoding, &wchars)?.to_vec()
    } else {
        Vec::new()
    };
    out_text(fcinfo, &bytes)
}

/* =================== test_valid_server_encoding(text) ==================== */

fn fc_test_valid_server_encoding(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, text arg.
    let name = unsafe { arg_text_str(fcinfo, 0) }?;
    Ok(Datum::from_bool(mbutils::pg_valid_server_encoding(&name) >= 0))
}

/* ==================== binary_coercible(oid, oid) ========================= */

fn fc_binary_coercible(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let srctype = fcinfo.arg_oid(0);
    let targettype = fcinfo.arg_oid(1);
    Ok(Datum::from_bool(coerce::IsBinaryCoercible(srctype, targettype)?))
}

/* ======================= trigger_return_old() ============================ */

fn fc_trigger_return_old(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: a T_TRIGGER_DATA-tagged context is the live TriggerData the
    // trigger manager armed for this call.
    let Some(trigdata) = (unsafe { types_trigger_call::trigger_data_from_fcinfo(fcinfo) }) else {
        return Err(err("trigger_return_old: not fired by trigger manager".to_string()));
    };
    Ok(Datum::from_usize(
        trigdata.tg_trigtuple.map_or(0, |p| p.as_ptr() as usize),
    ))
}

/* =========================== test_relpath() ============================== */

const PROCNUMBER_CHARS: usize = 6;
const OIDCHARS: usize = 10;
const FORKNAMECHARS: usize = 4;
const REL_PATH_STR_MAXLEN: usize = types_storage::PG_TBLSPC_DIR.len()
    + 1
    + OIDCHARS
    + 1
    + types_storage::TABLESPACE_VERSION_DIRECTORY.len()
    + 1
    + OIDCHARS
    + 1
    + 1
    + PROCNUMBER_CHARS
    + 1
    + OIDCHARS
    + 1
    + FORKNAMECHARS;

fn fc_test_relpath(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    const MAX_BACKENDS: u32 = (1 << 18) - 1;
    if (MAX_BACKENDS as f64).log10().ceil() as i32 != PROCNUMBER_CHARS as i32 {
        warn("mismatch between MAX_BACKENDS and PROCNUMBER_CHARS".to_string())?;
    }
    let rpath = relpath::GetRelationPath(
        types_storage::RelFileLocator {
            spcOid: u32::MAX,
            dbOid: u32::MAX,
            relNumber: u32::MAX,
        },
        (MAX_BACKENDS - 1) as ::types_core::ProcNumber,
        ::types_core::ForkNumber::INIT_FORKNUM,
    );
    if rpath.len() != REL_PATH_STR_MAXLEN {
        // C's message typo ("is if length") kept byte-exact.
        warn(format!(
            "maximum length relpath is if length {} instead of {}",
            rpath.len(),
            REL_PATH_STR_MAXLEN
        ))?;
    }
    Ok(Datum::null())
}

/* ============= test_pglz_compress / test_pglz_decompress ================= */
// upstream 42473d90098d (18.4): Add tests for low-level PGLZ [de]compression routines

/// C `test_pglz_compress(bytea) RETURNS bytea`: pglz_compress() under
/// PGLZ_strategy_always; NULL where the compressor returns -1.
fn fc_test_pglz_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let compressed = {
        // SAFETY: strict fn, bytea arg live for the call.
        let input = unsafe { fcinfo.arg_varlena_packed(0) }?;
        let source = input.data();
        let maxout = pglz::pglz_max_output(source.len());
        // C: palloc(maxout + VARHDRSZ), SET_VARSIZE to the compressed length.
        let mut result = varlena::image_with_header(fcinfo.result_mcx(), maxout)?;
        match pglz::pglz_compress_into(
            source,
            &mut result.spare_capacity_mut()[..maxout],
            &pglz::PGLZ_STRATEGY_ALWAYS,
        ) {
            Some(clen) => {
                // SAFETY: 4 header bytes appended + clen compressed bytes initialized.
                unsafe { result.set_len(varlena::VARHDRSZ + clen) };
                Some(varlena_result(::datum::Varlena::from_image(result)))
            }
            None => None,
        }
    };
    Ok(compressed.unwrap_or_else(|| fcinfo.return_null()))
}

/// C `test_pglz_decompress(bytea, int4, bool) RETURNS bytea`:
/// pglz_decompress() into a `rawsize` buffer under `check_complete`;
/// `elog(ERROR)` where the decompressor returns -1.
fn fc_test_pglz_decompress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn, bytea arg live for the call.
    let input = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let rawsize = fcinfo.arg_i32(1);
    let check_complete = fcinfo.arg_bool(2);

    if rawsize < 0 {
        return Err(err("rawsize must not be negative".to_string()));
    }
    let rawsize = rawsize as usize;

    // C: palloc(rawsize + VARHDRSZ), SET_VARSIZE to the decompressed length.
    let mut result = varlena::image_with_header(fcinfo.result_mcx(), rawsize)?;
    let dlen = pglz::pglz_decompress(
        input.data(),
        &mut result.spare_capacity_mut()[..rawsize],
        check_complete,
    )
    .ok_or_else(|| err("pglz_decompress failed".to_string()))?;
    // SAFETY: 4 header bytes appended + dlen decompressed bytes initialized.
    unsafe { result.set_len(varlena::VARHDRSZ + dlen) };
    Ok(varlena_result(::datum::Varlena::from_image(result)))
}

/* ========================== registry lookup ============================== */

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "interpt_pp" => fc_interpt_pp,
        "overpaid" => fc_overpaid,
        "widget_in" => fc_widget_in,
        "widget_out" => fc_widget_out,
        "pt_in_widget" => fc_pt_in_widget,
        "reverse_name" => fc_reverse_name,
        "trigger_return_old" => fc_trigger_return_old,
        "int44in" => fc_int44in,
        "int44out" => fc_int44out,
        "test_canonicalize_path" => fc_test_canonicalize_path,
        "make_tuple_indirect" => fc_make_tuple_indirect,
        "get_environ" => fc_get_environ,
        "regress_setenv" => fc_regress_setenv,
        "wait_pid" => fc_wait_pid,
        "test_atomic_ops" => fc_test_atomic_ops,
        "test_fdw_handler" => fc_test_fdw_handler,
        "is_catalog_text_unique_index_oid" => fc_is_catalog_text_unique_index_oid,
        "test_support_func" => fc_test_support_func,
        "test_opclass_options_func" => fc_test_opclass_options_func,
        "test_enc_setup" => fc_test_enc_setup,
        "test_enc_conversion" => fc_test_enc_conversion,
        "test_bytea_to_text" => fc_test_bytea_to_text,
        "test_text_to_bytea" => fc_test_text_to_bytea,
        "test_mblen_func" => fc_test_mblen_func,
        "test_text_to_wchars" => fc_test_text_to_wchars,
        "test_wchars_to_text" => fc_test_wchars_to_text,
        "test_valid_server_encoding" => fc_test_valid_server_encoding,
        "binary_coercible" => fc_binary_coercible,
        "test_relpath" => fc_test_relpath,
        "test_pglz_compress" => fc_test_pglz_compress,
        "test_pglz_decompress" => fc_test_pglz_decompress,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry { name: LIBRARY, lookup, pg_init: None });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atof_prefixes() {
        assert_eq!(c_atof(b"1,3,5)"), 1.0);
        assert_eq!(c_atof(b"  -2.5e2junk"), -250.0);
        assert_eq!(c_atof(b"1e"), 1.0);
        assert_eq!(c_atof(b"junk"), 0.0);
        assert_eq!(c_atof(b".5"), 0.5);
        assert_eq!(c_atof(b"0x1p2,0,1)"), 4.0);
        assert_eq!(c_atof(b"Infinity)"), f64::INFINITY);
        assert_eq!(c_atof(b"-inf"), f64::NEG_INFINITY);
        assert!(c_atof(b"nan").is_nan());
    }

    #[test]
    fn mblen_cstr_returns_one_at_the_terminator() {
        assert_eq!(mblen_cstr(b"").unwrap(), 1);
        assert_eq!(mblen_cstr(b"\0abc").unwrap(), 1);
        assert_eq!(mblen_cstr(b"abc").unwrap(), 1);
    }

    #[test]
    fn g_format() {
        assert_eq!(fmt_g(1.0), "1");
        assert_eq!(fmt_g(3.0), "3");
        assert_eq!(fmt_g(0.05), "0.05");
        assert_eq!(fmt_g(-2.5), "-2.5");
        assert_eq!(fmt_g(1e20), "1e+20");
        assert_eq!(fmt_g(123456.0), "123456");
        assert_eq!(fmt_g(1234567.0), "1.23457e+06");
        assert_eq!(fmt_g(0.0001), "0.0001");
        assert_eq!(fmt_g(0.00001), "1e-05");
        assert_eq!(fmt_g(0.0), "0");
    }

    #[test]
    fn relpath_maxlen_matches_c() {
        assert_eq!(REL_PATH_STR_MAXLEN, 71);
    }

    /// upstream 42473d90098d (18.4): the regress.c pglz helpers over the
    /// compression_pglz.sql vectors — roundtrips, both rawsize mismatches,
    /// and the truncated-match-tag corruptions.
    #[test]
    fn pglz_helpers_match_compression_pglz_vectors() {
        use ::datum::varlena::{set_varsize_4b, VarlenaRef, VARHDRSZ};
        use ::mcx::MemoryContext;
        use ::types_error::ERRCODE_INTERNAL_ERROR;

        fn bytea(payload: &[u8]) -> Vec<u8> {
            let mut image = set_varsize_4b(VARHDRSZ + payload.len()).to_vec();
            image.extend_from_slice(payload);
            image
        }
        fn payload(d: Datum) -> Vec<u8> {
            // SAFETY: a 4B-header varlena result left in the armed context.
            unsafe { VarlenaRef::from_ptr(d.as_u64() as usize as *const u8) }.data().to_vec()
        }
        let ctx = MemoryContext::new("t");
        let compress = |input: &[u8]| -> Option<Vec<u8>> {
            let image = bytea(input);
            let mut fci = fmgr::LocalFcinfo::<1>::new(0);
            // SAFETY: ctx outlives the call and the payload reads.
            unsafe { fci.set_result_mcx(ctx.mcx()) };
            fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
            let d = fc_test_pglz_compress(None, &mut fci).unwrap();
            (!fci.isnull).then(|| payload(d))
        };
        let decompress = |src: &[u8], rawsize: i32, check_complete: bool| -> PgResult<Vec<u8>> {
            let image = bytea(src);
            let mut fci = fmgr::LocalFcinfo::<3>::new(0);
            // SAFETY: ctx outlives the call and the payload reads.
            unsafe { fci.set_result_mcx(ctx.mcx()) };
            fci.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
            fci.set_arg(1, Datum::from_i32(rawsize));
            fci.set_arg(2, Datum::from_bool(check_complete));
            fc_test_pglz_decompress(None, &mut fci).map(payload)
        };
        let failed = |r: PgResult<Vec<u8>>| {
            let e = r.unwrap_err();
            assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
            assert_eq!(e.message(), "pglz_decompress failed");
        };

        let raw = b"abcd".repeat(100);
        let c = compress(&raw).expect("PGLZ_strategy_always compresses 'abcd' x 100");
        assert_eq!(decompress(&c, 400, false).unwrap(), raw);
        assert_eq!(decompress(&c, 400, true).unwrap(), raw);
        // rawsize too large: the destination never fills.
        failed(decompress(&c, 500, true));
        // rawsize too small: the source is not fully consumed.
        failed(decompress(&c, 100, true));
        // Set control bit with read of a match tag, no data follows.
        assert_eq!(decompress(b"\x01", 1024, false).unwrap(), b"");
        failed(decompress(b"\x01", 1024, true));
        // Set control bit with read of a match tag, 1 byte follows.
        failed(decompress(b"\x01\xff", 1024, false));
        failed(decompress(b"\x01\xff", 1024, true));
        // Match tag whose length nibble is 3 bytes (extended), no data follows.
        failed(decompress(b"\x01\x0f\x01", 1024, false));
        failed(decompress(b"\x01\x0f\x01", 1024, true));
    }

    #[test]
    fn lookup_covers_every_regress_symbol() {
        for sym in [
            "interpt_pp", "overpaid", "widget_in", "widget_out", "pt_in_widget",
            "reverse_name", "trigger_return_old", "int44in", "int44out",
            "test_canonicalize_path", "make_tuple_indirect", "get_environ",
            "regress_setenv", "wait_pid", "test_atomic_ops", "test_fdw_handler",
            "is_catalog_text_unique_index_oid", "test_support_func",
            "test_opclass_options_func", "test_enc_setup", "test_enc_conversion",
            "test_bytea_to_text", "test_text_to_bytea", "test_mblen_func",
            "test_text_to_wchars", "test_wchars_to_text",
            "test_valid_server_encoding", "binary_coercible", "test_relpath",
            "test_pglz_compress", "test_pglz_decompress",
        ] {
            assert!(lookup(sym).is_some(), "regress symbol {sym} missing from lookup");
        }
        assert!(lookup("nosuchsymbol").is_none());
    }

    #[test]
    fn registry_roundtrip() {
        init_seams();
        let mut pkglib = [0u8; types_core::MAXPGPATH];
        pkglib[..b"/nonexistent-pkglib".len()].copy_from_slice(b"/nonexistent-pkglib");
        init_small::globals::set_pkglib_path(pkglib);
        assert!(dfmgr::load_external_function("$libdir/regress", "binary_coercible", true).is_ok());
        assert!(dfmgr::load_external_function("regress", "binary_coercible", true).is_ok());
        let err = dfmgr::load_external_function("/x/y/regress.so", "binary_coercible", true).unwrap_err();
        assert!(err.message().contains("could not access file \"/x/y/regress.so\""));
    }

    /// wait_pid's poll loop: a pending interrupt routes through the ported
    /// ProcessInterrupts seam (C regress.c wait_pid runs CHECK_FOR_INTERRUPTS
    /// per 50ms poll) instead of the former unported-panic stub. Probing our
    /// own (live) pid guarantees the loop reaches the check.
    #[test]
    fn wait_pid_pending_interrupt_cancels() {
        use ::types_error::{PgError, ERRCODE_QUERY_CANCELED};

        // Bare test thread: install a session identity so the superuser gate
        // resolves (single-user escape path: bootstrap superuser, not under
        // postmaster).
        miscinit::SetUserIdAndSecContext(types_core::catalog::BOOTSTRAP_SUPERUSERID, 0);

        // ProcessInterrupts mock: consumes the flag and raises the cancel
        // error, as C's query-cancel arm does. (Seams are set-once per
        // process: this is the only test in this binary installing it.)
        ::postgres_seams::check_for_interrupts::set(|| {
            init_small::globals::SetInterruptPending(false);
            Err(Box::new(
                PgError::error("canceling statement due to user request")
                    .with_sqlstate(ERRCODE_QUERY_CANCELED),
            ))
        });

        init_small::globals::SetInterruptPending(true);
        let mut fci = fmgr::LocalFcinfo::<1>::new(0);
        fci.set_arg(0, Datum::from_i32(std::process::id() as i32));
        let err = fc_wait_pid(None, &mut fci).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_QUERY_CANCELED);
        // The interrupt was consumed on the seam side; the flag is clean.
        assert!(!init_small::globals::InterruptPending());
    }

    // s_lock.c backoff seams for the spinlock arms: set-once per process,
    // shared by every test in this binary that reaches test_spinlock.
    static FINISH_SPIN_DELAY_CALLS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    fn install_spin_delay_seams() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
            s_lock_seams::finish_spin_delay::set(|_| {
                FINISH_SPIN_DELAY_CALLS.fetch_add(1, Ordering::SeqCst);
            });
        });
    }

    #[test]
    fn atomics() {
        install_spin_delay_seams();
        assert!(test_atomic_flag().is_ok());
        assert!(test_atomic_uint32().is_ok());
        assert!(test_atomic_uint64().is_ok());
        assert!(test_spinlock().is_ok());
    }

    /// regress.c:638 test_spinlock exercises the real spinlock primitives:
    /// its `s_lock(&lock, "testfile", 17, "testfunc")` arm (regress.c:668)
    /// runs s_lock.c's contended-acquire loop, which ends in
    /// finish_spin_delay() even when the lock was free — so the ported test
    /// must engage that seam at least once per call. The former Mutex stand-in
    /// never did.
    #[test]
    fn spinlock_arm_runs_s_lock_backoff() {
        install_spin_delay_seams();
        let before = FINISH_SPIN_DELAY_CALLS.load(Ordering::SeqCst);
        assert!(test_spinlock().is_ok());
        let after = FINISH_SPIN_DELAY_CALLS.load(Ordering::SeqCst);
        assert!(
            after > before,
            "test_spinlock never reached s_lock()/finish_spin_delay (before={before} after={after})"
        );
    }

    /// regress.c:678-683: TAS / TAS_SPIN on an already held spinlock must
    /// fail, and the padding around the lock word must stay untouched — the
    /// contract test_spinlock asserts against the types_storage primitive.
    #[test]
    fn spinlock_tas_refuses_held_lock_and_keeps_padding() {
        #[repr(C)]
        struct TestLockStruct {
            data_before: [u8; 4],
            lock: types_storage::Spinlock,
            data_after: [u8; 4],
        }
        let s = TestLockStruct {
            data_before: *b"abcd",
            lock: types_storage::Spinlock::new(),
            data_after: *b"ef12",
        };
        assert_eq!(s.lock.tas(), 0, "S_LOCK on a free lock");
        assert_ne!(s.lock.tas(), 0, "acquired already held spinlock (TAS)");
        assert_ne!(s.lock.tas_spin(), 0, "acquired already held spinlock (TAS_SPIN)");
        s.lock.unlock();
        assert!(s.lock.is_free());
        assert_eq!(&s.data_before, b"abcd", "padding before spinlock modified");
        assert_eq!(&s.data_after, b"ef12", "padding after spinlock modified");
    }

    #[test]
    fn test_enc_conversion_invalid_name_is_22023() {
        use ::types_error::ERRCODE_INVALID_PARAMETER_VALUE;
        let src = invalid_encoding_name_error("source", "no_such_enc");
        assert_eq!(src.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(src.message(), "invalid source encoding name \"no_such_enc\"");
        let dest = invalid_encoding_name_error("destination", "no_such_enc");
        assert_eq!(dest.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(
            dest.message(),
            "invalid destination encoding name \"no_such_enc\""
        );
    }
}
