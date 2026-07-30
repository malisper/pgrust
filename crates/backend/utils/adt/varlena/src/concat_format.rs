//! concat/concat_ws + format() (varlena.c:5644-6406). quote_literal_cstr
//! (quote.c) is inlined pending an adt_quote unit.

use core::ffi::CStr;

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::catalog::{INT2OID, INT4OID};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_fmgr::{function_call1_coll_in, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::cstring_to_text;

const TEXT_FORMAT_FLAG_MINUS: i32 = 0x0001;

// std Vec rides the open-set fn_extra Box slot (same justification as OutBuf).
struct ConcatFout(Vec<FmgrInfo>);
struct ArrayOutCache {
    element_type: Oid,
    typlen: i16,
    typbyval: bool,
    typalign: i8,
    finfo: FmgrInfo,
}

#[track_caller]
#[cold]
#[inline(never)]
fn unterminated_specifier() -> Box<PgError> {
    Box::new(
        PgError::error("unterminated format() type specifier")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint("For a single \"%\" use \"%%\"."),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn number_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("number is out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn argument_zero() -> Box<PgError> {
    Box::new(
        PgError::error("format specifies argument 0, but arguments are numbered from 1")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn width_position_unterminated() -> Box<PgError> {
    Box::new(
        PgError::error("width argument position must be ended by \"$\"")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_few_arguments() -> Box<PgError> {
    Box::new(
        PgError::error("too few arguments for format()")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn unrecognized_specifier(fmt: &[u8], cp: usize) -> Box<PgError> {
    let tail = &fmt[cp..];
    let ch = match core::str::from_utf8(tail) {
        Ok(s) => s.chars().next().map(String::from).unwrap_or_default(),
        Err(e) if e.valid_up_to() > 0 => {
            // SAFETY: valid_up_to bytes are valid UTF-8.
            unsafe { core::str::from_utf8_unchecked(&tail[..e.valid_up_to()]) }
                .chars()
                .next()
                .map(String::from)
                .unwrap_or_default()
        }
        Err(_) => String::from_utf8_lossy(&tail[..1]).into_owned(),
    };
    Box::new(
        PgError::error(format!("unrecognized format() type specifier \"{ch}\""))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint("For a single \"%\" use \"%%\"."),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn null_identifier() -> Box<PgError> {
    Box::new(
        PgError::error("null values cannot be formatted as an SQL identifier")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn indeterminate_input(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("could not determine data type of {what} input")))
}

fn output_to_bytes<'a>(finfo: &mut FmgrInfo, mcx: Mcx<'a>, value: Datum) -> PgResult<&'a [u8]> {
    let out = function_call1_coll_in(finfo, InvalidOid, mcx, value)?;
    // SAFETY: output functions return a NUL-terminated cstring datum
    // allocated in `mcx` (the contract C's DatumGetCString trusts).
    Ok(unsafe { CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) }.to_bytes())
}

fn fetch_array_image<'mcx>(
    fcinfo: &Fcinfo,
    i: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    // SAFETY: arg i checked non-null by the caller; live varlena datum.
    let raw = unsafe {
        let p = fcinfo.arg_ptr(i);
        core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
    };
    detoast_seams::detoast_attr::call(mcx, raw)
}

fn array_out_cache<'f>(
    flinfo: &'f mut FmgrInfo,
    element_type: Oid,
) -> PgResult<&'f mut ArrayOutCache> {
    let need = match flinfo.fn_extra_ref::<ArrayOutCache>() {
        Some(c) => c.element_type != element_type,
        None => true,
    };
    if need {
        let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(element_type)?;
        let (outfunc, _) = lsyscache::getTypeOutputInfo(element_type)?;
        let finfo = fmgr_seams::fmgr_info::call(outfunc)?;
        flinfo.set_fn_extra(ArrayOutCache { element_type, typlen, typbyval, typalign, finfo });
    }
    Ok(flinfo.fn_extra_mut::<ArrayOutCache>().unwrap())
}

// concat_internal's VARIADIC-array leg == array_to_text_internal with a NULL
// null_string (varlena.c:5697-5725): null elements skipped.
fn concat_variadic_array(
    flinfo: &mut FmgrInfo,
    mcx: Mcx<'_>,
    array: &[u8],
    sepstr: &[u8],
) -> PgResult<Datum> {
    let element_type = arrayfuncs::arr_elemtype(array);
    let cache = array_out_cache(flinfo, element_type)?;
    let (elems, nulls) = arrayfuncs::deconstruct_array(
        mcx,
        array,
        cache.typlen as i32,
        cache.typbyval,
        cache.typalign as u8,
        true,
    )?;
    let mut out: PgVec<'_, u8> = PgVec::new_in(mcx);
    let mut first = true;
    for (i, &value) in elems.iter().enumerate() {
        if nulls[i] {
            continue;
        }
        if first {
            first = false;
        } else {
            mcx::vec_append_bytes(&mut out, sepstr)?;
        }
        let s = output_to_bytes(&mut cache.finfo, mcx, value)?;
        mcx::vec_append_bytes(&mut out, s)?;
    }
    Ok(types_fmgr::varlena_result(cstring_to_text(mcx, &out)?))
}

// concat_internal (varlena.c:5682-5757); build_concat_foutcache's per-arg
// FmgrInfo array rides fn_extra, resolved once per call site. None == SQL NULL.
fn concat_internal(
    flinfo: &mut FmgrInfo,
    fcinfo: &Fcinfo,
    sepstr: &[u8],
    argidx: usize,
) -> PgResult<Option<Datum>> {
    let nargs = fcinfo.nargs();

    if fmgr_seams::get_fn_expr_variadic::call(flinfo) {
        debug_assert_eq!(argidx, nargs - 1);
        if fcinfo.argisnull(argidx) {
            return Ok(None);
        }
        let mcx = fcinfo.result_mcx();
        let array = fetch_array_image(fcinfo, argidx, mcx)?;
        return concat_variadic_array(flinfo, mcx, &array, sepstr).map(Some);
    }

    if flinfo.fn_extra_ref::<ConcatFout>().is_none() {
        let mut infos = Vec::with_capacity(nargs);
        for i in 0..nargs {
            let valtype = fmgr_seams::get_fn_expr_argtype::call(flinfo, i as i16);
            if !OidIsValid(valtype) {
                return Err(indeterminate_input("concat()"));
            }
            let (outfunc, _) = lsyscache::getTypeOutputInfo(valtype)?;
            infos.push(fmgr_seams::fmgr_info::call(outfunc)?);
        }
        flinfo.set_fn_extra(ConcatFout(infos));
    }
    let cache = flinfo.fn_extra_mut::<ConcatFout>().unwrap();

    let mcx = fcinfo.result_mcx();
    let mut out: PgVec<'_, u8> = PgVec::new_in(mcx);
    let mut first_arg = true;
    for i in argidx..nargs {
        if fcinfo.argisnull(i) {
            continue;
        }
        if first_arg {
            first_arg = false;
        } else {
            mcx::vec_append_bytes(&mut out, sepstr)?;
        }
        let s = output_to_bytes(&mut cache.0[i], mcx, fcinfo.arg(i))?;
        mcx::vec_append_bytes(&mut out, s)?;
    }
    Ok(Some(types_fmgr::varlena_result(cstring_to_text(mcx, &out)?)))
}

pub fn fc_text_concat(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("text_concat: NULL flinfo");
    match concat_internal(flinfo, fcinfo, b"", 0)? {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_text_concat_ws(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("text_concat_ws: NULL flinfo");
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let result = {
        // SAFETY: arg 0 checked non-null; live text varlena.
        let sep = unsafe { fcinfo.arg_varlena_packed(0) }?;
        concat_internal(flinfo, fcinfo, sep.data(), 1)?
    };
    match result {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

struct FormatSpec {
    argpos: i32,
    widthpos: i32,
    flags: i32,
    width: i32,
}

#[inline]
fn at(fmt: &[u8], cp: usize) -> u8 {
    fmt[cp]
}

fn advance_parse_pointer(cp: usize, end: usize) -> PgResult<usize> {
    let next = cp + 1;
    if next >= end {
        return Err(unterminated_specifier());
    }
    Ok(next)
}

// text_format_parse_digits (varlena.c:6175-6199).
fn parse_digits(fmt: &[u8], mut cp: usize) -> PgResult<(usize, bool, i32)> {
    let end = fmt.len();
    let mut found = false;
    let mut val: i32 = 0;
    while at(fmt, cp).is_ascii_digit() {
        let digit = (at(fmt, cp) - b'0') as i32;
        match val.checked_mul(10).and_then(|m| m.checked_add(digit)) {
            Some(v) => val = v,
            None => return Err(number_out_of_range()),
        }
        cp = advance_parse_pointer(cp, end)?;
        found = true;
    }
    Ok((cp, found, val))
}

// text_format_parse_format (varlena.c:6224-6296); returns cp at the type char.
fn parse_format(fmt: &[u8], mut cp: usize) -> PgResult<(usize, FormatSpec)> {
    let end = fmt.len();
    let mut spec = FormatSpec { argpos: -1, widthpos: -1, flags: 0, width: 0 };

    let (newcp, found, n) = parse_digits(fmt, cp)?;
    cp = newcp;
    if found {
        if at(fmt, cp) != b'$' {
            spec.width = n;
            return Ok((cp, spec));
        }
        spec.argpos = n;
        if n == 0 {
            return Err(argument_zero());
        }
        cp = advance_parse_pointer(cp, end)?;
    }

    while at(fmt, cp) == b'-' {
        spec.flags |= TEXT_FORMAT_FLAG_MINUS;
        cp = advance_parse_pointer(cp, end)?;
    }

    if at(fmt, cp) == b'*' {
        cp = advance_parse_pointer(cp, end)?;
        let (newcp, found, n) = parse_digits(fmt, cp)?;
        cp = newcp;
        if found {
            if at(fmt, cp) != b'$' {
                return Err(width_position_unterminated());
            }
            spec.widthpos = n;
            if n == 0 {
                return Err(argument_zero());
            }
            cp = advance_parse_pointer(cp, end)?;
        } else {
            spec.widthpos = 0;
        }
    } else {
        let (newcp, found, n) = parse_digits(fmt, cp)?;
        cp = newcp;
        if found {
            spec.width = n;
        }
    }
    Ok((cp, spec))
}

// quote_literal_cstr (quote.c): E-prefix when a backslash is present; quotes
// and backslashes doubled inside '...'.
fn append_quote_literal(out: &mut PgVec<'_, u8>, s: &[u8]) {
    if s.contains(&b'\\') {
        out.push(b'E');
    }
    out.push(b'\'');
    for &c in s {
        if c == b'\'' || c == b'\\' {
            out.push(c);
        }
        out.push(c);
    }
    out.push(b'\'');
}

// text_format_append_string (varlena.c:6350-6393); width is in characters.
fn append_padded(out: &mut PgVec<'_, u8>, s: &[u8], flags: i32, mut width: i32) -> PgResult<()> {
    if width == 0 {
        return mcx::vec_append_bytes(out, s);
    }
    let mut align_to_left = false;
    if width < 0 {
        align_to_left = true;
        if width == i32::MIN {
            return Err(number_out_of_range());
        }
        width = -width;
    } else if flags & TEXT_FORMAT_FLAG_MINUS != 0 {
        align_to_left = true;
    }
    let len = mbutils_seams::pg_mbstrlen_with_len::call(s)?;
    let pad = (width - len).max(0) as usize;
    if align_to_left {
        mcx::vec_append_bytes(out, s)?;
        for _ in 0..pad {
            out.push(b' ');
        }
    } else {
        for _ in 0..pad {
            out.push(b' ');
        }
        mcx::vec_append_bytes(out, s)?;
    }
    Ok(())
}

// text_format_string_conversion (varlena.c:6297-6345).
#[allow(clippy::too_many_arguments)]
fn string_conversion<'mcx>(
    out: &mut PgVec<'mcx, u8>,
    conversion: u8,
    finfo: &mut FmgrInfo,
    mcx: Mcx<'mcx>,
    value: Datum,
    is_null: bool,
    flags: i32,
    width: i32,
) -> PgResult<()> {
    if is_null {
        return match conversion {
            b's' => append_padded(out, b"", flags, width),
            b'L' => append_padded(out, b"NULL", flags, width),
            _ => Err(null_identifier()),
        };
    }
    let s = output_to_bytes(finfo, mcx, value)?;
    match conversion {
        b'I' => {
            let ident = core::str::from_utf8(s)
                .expect("format %I: output function produced invalid UTF-8");
            let quoted = format_type::quote_identifier(ident);
            append_padded(out, quoted.as_bytes(), flags, width)
        }
        b'L' => {
            let mut q: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len() * 2 + 3)?;
            append_quote_literal(&mut q, s);
            append_padded(out, &q, flags, width)
        }
        _ => append_padded(out, s, flags, width),
    }
}

struct FormatArgs<'a> {
    elements: Option<(PgVec<'a, Datum>, PgVec<'a, bool>, Oid)>,
    // By-ref element Datums borrow this detoasted image; dropping it before
    // the last args.get read is the rowtypes deform_record UAF.
    _array: Option<PgVec<'a, u8>>,
    nargs: usize,
}

impl FormatArgs<'_> {
    fn get(&self, fcinfo: &Fcinfo, flinfo: &FmgrInfo, arg: usize) -> PgResult<(Datum, bool, Oid)> {
        match &self.elements {
            Some((elems, nulls, element_type)) => {
                Ok((elems[arg - 1], nulls[arg - 1], *element_type))
            }
            None => {
                let typid = fmgr_seams::get_fn_expr_argtype::call(flinfo, arg as i16);
                if !OidIsValid(typid) {
                    return Err(indeterminate_input("format()"));
                }
                Ok((fcinfo.arg(arg), fcinfo.argisnull(arg), typid))
            }
        }
    }
}

// text_format (varlena.c:5898-6170).
pub fn fc_text_format(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("text_format: NULL flinfo");
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();

    let args = if fmgr_seams::get_fn_expr_variadic::call(flinfo) {
        debug_assert_eq!(fcinfo.nargs(), 2);
        if fcinfo.argisnull(1) {
            FormatArgs { elements: None, _array: None, nargs: 1 }
        } else {
            let array = fetch_array_image(fcinfo, 1, mcx)?;
            let element_type = arrayfuncs::arr_elemtype(&array);
            let (typlen, typbyval, typalign) = lsyscache::get_typlenbyvalalign(element_type)?;
            let (elems, nulls) = arrayfuncs::deconstruct_array(
                mcx,
                &array,
                typlen as i32,
                typbyval,
                typalign as u8,
                true,
            )?;
            let nitems = elems.len();
            FormatArgs {
                elements: Some((elems, nulls, element_type)),
                _array: Some(array),
                nargs: nitems + 1,
            }
        }
    } else {
        FormatArgs { elements: None, _array: None, nargs: fcinfo.nargs() }
    };

    // SAFETY: arg 0 checked non-null; live text varlena.
    let fmt_arg = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let fmt = fmt_arg.data();
    let end = fmt.len();

    let mut out: PgVec<'_, u8> = PgVec::new_in(mcx);
    let mut arg = 1usize;
    let mut prev_type = InvalidOid;
    let mut prev_width_type = InvalidOid;
    let mut typoutputfinfo = FmgrInfo::unresolved();
    let mut typoutputinfo_width = FmgrInfo::unresolved();

    let mut cp = 0usize;
    while cp < end {
        if at(fmt, cp) != b'%' {
            out.push(at(fmt, cp));
            cp += 1;
            continue;
        }
        cp = advance_parse_pointer(cp, end)?;
        if at(fmt, cp) == b'%' {
            out.push(b'%');
            cp += 1;
            continue;
        }

        let (newcp, spec) = parse_format(fmt, cp)?;
        cp = newcp;

        if !matches!(at(fmt, cp), b's' | b'I' | b'L') {
            return Err(unrecognized_specifier(fmt, cp));
        }

        let mut width = spec.width;
        if spec.widthpos >= 0 {
            if spec.widthpos > 0 {
                arg = spec.widthpos as usize;
            }
            if arg >= args.nargs {
                return Err(too_few_arguments());
            }
            let (value, is_null, typid) = args.get(fcinfo, flinfo, arg)?;
            arg += 1;
            if is_null {
                width = 0;
            } else if typid == INT4OID {
                width = value.as_i32();
            } else if typid == INT2OID {
                width = value.as_i16() as i32;
            } else {
                if typid != prev_width_type {
                    let (outfunc, _) = lsyscache::getTypeOutputInfo(typid)?;
                    typoutputinfo_width = fmgr_seams::fmgr_info::call(outfunc)?;
                    prev_width_type = typid;
                }
                let s = output_to_bytes(&mut typoutputinfo_width, mcx, value)?;
                let s = core::str::from_utf8(s)
                    .expect("format width: output function produced invalid UTF-8");
                width = numutils::pg_strtoint32(s)?;
            }
        }

        if spec.argpos > 0 {
            arg = spec.argpos as usize;
        }
        if arg >= args.nargs {
            return Err(too_few_arguments());
        }
        let (value, is_null, typid) = args.get(fcinfo, flinfo, arg)?;
        arg += 1;

        if typid != prev_type {
            let (outfunc, _) = lsyscache::getTypeOutputInfo(typid)?;
            typoutputfinfo = fmgr_seams::fmgr_info::call(outfunc)?;
            prev_type = typid;
        }

        string_conversion(
            &mut out,
            at(fmt, cp),
            &mut typoutputfinfo,
            mcx,
            value,
            is_null,
            spec.flags,
            width,
        )?;
        cp += 1;
    }

    Ok(types_fmgr::varlena_result(cstring_to_text(mcx, &out)?))
}
