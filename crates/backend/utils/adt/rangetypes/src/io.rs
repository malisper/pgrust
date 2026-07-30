use std::ffi::CString;

use ::datum::Datum;
use ::lsyscache::{get_type_io_data, IOFuncSelector};
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION,
};
use ::types_fmgr::{
    function_call1_coll_in, input_function_call_safe, receive_function_call, send_function_call,
    FmgrInfo,
};

use crate::{
    make_range, range_deserialize, range_get_flags, range_has_lbound, range_has_ubound,
    RangeBound, RangeInfo, RANGE_EMPTY, RANGE_EMPTY_LITERAL, RANGE_LB_INC, RANGE_LB_INF,
    RANGE_UB_INC, RANGE_UB_INF,
};

// RangeIOData (rangetypes.c): fn_extra cache for the I/O functions.
pub struct RangeIOData {
    pub ri: RangeInfo,
    pub typioproc: FmgrInfo,
    pub typioparam: Oid,
}

#[track_caller]
#[cold]
fn no_binary_io(recv: bool, elem_typid: Oid) -> Box<PgError> {
    let what = if recv { "input" } else { "output" };
    let t = ::format_type::format_type_be(elem_typid)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| format!("{elem_typid}"));
    Box::new(
        PgError::error(format!("no binary {what} function available for type {t}"))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
    )
}

pub fn cached_range_io_data<'f>(
    flinfo: &'f mut FmgrInfo,
    rngtypid: Oid,
    func: IOFuncSelector,
) -> PgResult<&'f mut RangeIOData> {
    let need = match flinfo.fn_extra_ref::<RangeIOData>() {
        Some(c) => c.ri.rngtypid != rngtypid,
        None => true,
    };
    if need {
        let ri = RangeInfo::lookup(rngtypid)?;
        let io = get_type_io_data(ri.elem_typid, func)?;
        if io.func == 0 {
            return Err(no_binary_io(matches!(func, IOFuncSelector::IOFunc_receive), ri.elem_typid));
        }
        let typioproc = ::fmgr_seams::fmgr_info::call(io.func)?;
        flinfo.set_fn_extra(RangeIOData { ri, typioproc, typioparam: io.typioparam });
    }
    Ok(flinfo.fn_extra_mut::<RangeIOData>().unwrap())
}

#[inline]
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

#[track_caller]
#[cold]
fn invalid_flags_err() -> Box<PgError> {
    Box::new(
        PgError::error("invalid range bound flags")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_hint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"."),
    )
}

/// range_parse_flags (rangetypes.c) — the constructor3 flags argument.
pub fn range_parse_flags(flags_str: &[u8]) -> PgResult<u8> {
    if flags_str.len() != 2 {
        return Err(invalid_flags_err());
    }
    let mut flags = 0u8;
    match flags_str[0] {
        b'[' => flags |= RANGE_LB_INC,
        b'(' => {}
        _ => return Err(invalid_flags_err()),
    }
    match flags_str[1] {
        b']' => flags |= RANGE_UB_INC,
        b')' => {}
        _ => return Err(invalid_flags_err()),
    }
    Ok(flags)
}

#[cold]
fn malformed(string: &[u8], detail: &str) -> PgError {
    PgError::error(format!(
        "malformed range literal: \"{}\"",
        String::from_utf8_lossy(string)
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
    .with_detail(detail.to_string())
}

#[derive(Debug)]
pub struct ParsedRange<'m> {
    pub flags: u8,
    pub lbound: Option<PgVec<'m, u8>>,
    pub ubound: Option<PgVec<'m, u8>>,
}

/// range_parse (rangetypes.c). `Ok(None)` = soft error captured.
pub fn range_parse<'m>(
    mcx: Mcx<'m>,
    string: &[u8],
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<ParsedRange<'m>>> {
    let mut pos = 0usize;
    while pos < string.len() && is_space(string[pos]) {
        pos += 1;
    }

    let emp = RANGE_EMPTY_LITERAL.as_bytes();
    if string.len() - pos >= emp.len()
        && string[pos..pos + emp.len()].eq_ignore_ascii_case(emp)
    {
        pos += emp.len();
        while pos < string.len() && is_space(string[pos]) {
            pos += 1;
        }
        if pos != string.len() {
            return ereturn(esc, None, malformed(string, "Junk after \"empty\" key word."));
        }
        return Ok(Some(ParsedRange { flags: RANGE_EMPTY, lbound: None, ubound: None }));
    }

    let mut flags = 0u8;
    match string.get(pos) {
        Some(b'[') => {
            flags |= RANGE_LB_INC;
            pos += 1;
        }
        Some(b'(') => pos += 1,
        _ => {
            return ereturn(
                esc,
                None,
                malformed(string, "Missing left parenthesis or bracket."),
            )
        }
    }

    let (lbound, infinite) = match parse_bound(mcx, string, &mut pos, esc.as_deref_mut())? {
        Some(v) => v,
        None => return Ok(None),
    };
    if infinite {
        flags |= RANGE_LB_INF;
    }

    if string.get(pos) == Some(&b',') {
        pos += 1;
    } else {
        return ereturn(esc, None, malformed(string, "Missing comma after lower bound."));
    }

    let (ubound, infinite) = match parse_bound(mcx, string, &mut pos, esc.as_deref_mut())? {
        Some(v) => v,
        None => return Ok(None),
    };
    if infinite {
        flags |= RANGE_UB_INF;
    }

    match string.get(pos) {
        Some(b']') => {
            flags |= RANGE_UB_INC;
            pos += 1;
        }
        Some(b')') => pos += 1,
        _ => return ereturn(esc, None, malformed(string, "Too many commas.")),
    }

    while pos < string.len() && is_space(string[pos]) {
        pos += 1;
    }
    if pos != string.len() {
        return ereturn(
            esc,
            None,
            malformed(string, "Junk after right parenthesis or bracket."),
        );
    }

    Ok(Some(ParsedRange { flags, lbound, ubound }))
}

// range_parse_bound (rangetypes.c): (bound, infinite); `Ok(None)` = soft error.
#[allow(clippy::type_complexity)]
fn parse_bound<'m>(
    mcx: Mcx<'m>,
    string: &[u8],
    pos: &mut usize,
    esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<(Option<PgVec<'m, u8>>, bool)>> {
    match string.get(*pos) {
        Some(b',') | Some(b')') | Some(b']') => return Ok(Some((None, true))),
        _ => {}
    }
    let mut buf: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, 16)?;
    let mut inquote = false;
    loop {
        if !inquote {
            if let Some(b',' | b')' | b']') = string.get(*pos) {
                break;
            }
        }
        let Some(&ch) = string.get(*pos) else {
            return ereturn(esc, None, malformed(string, "Unexpected end of input."));
        };
        *pos += 1;
        if ch == b'\\' {
            let Some(&nx) = string.get(*pos) else {
                return ereturn(esc, None, malformed(string, "Unexpected end of input."));
            };
            *pos += 1;
            buf.push(nx);
        } else if ch == b'"' {
            if !inquote {
                inquote = true;
            } else if string.get(*pos) == Some(&b'"') {
                buf.push(b'"');
                *pos += 1;
            } else {
                inquote = false;
            }
        } else {
            buf.push(ch);
        }
    }
    Ok(Some((Some(buf), false)))
}

/// range_deparse (rangetypes.c): NUL-terminated cstring image.
pub fn range_deparse<'m>(
    mcx: Mcx<'m>,
    flags: u8,
    lbound: Option<&[u8]>,
    ubound: Option<&[u8]>,
) -> PgResult<PgVec<'m, u8>> {
    let mut out: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, 32)?;
    if flags & RANGE_EMPTY != 0 {
        ::mcx::vec_append_bytes(&mut out, RANGE_EMPTY_LITERAL.as_bytes())?;
        out.push(0);
        return Ok(out);
    }
    out.push(if flags & RANGE_LB_INC != 0 { b'[' } else { b'(' });
    if range_has_lbound(flags) {
        bound_escape(&mut out, lbound.expect("lower bound string"))?;
    }
    out.push(b',');
    if range_has_ubound(flags) {
        bound_escape(&mut out, ubound.expect("upper bound string"))?;
    }
    out.push(if flags & RANGE_UB_INC != 0 { b']' } else { b')' });
    out.push(0);
    Ok(out)
}

// range_bound_escape (rangetypes.c).
fn bound_escape(out: &mut PgVec<'_, u8>, value: &[u8]) -> PgResult<()> {
    let nq = value.is_empty()
        || value.iter().any(|&ch| {
            matches!(ch, b'"' | b'\\' | b'(' | b')' | b'[' | b']' | b',') || is_space(ch)
        });
    out.try_reserve(2 * value.len() + 2).map_err(|_| out.allocator().oom(2 * value.len() + 2))?;
    if nq {
        out.push(b'"');
    }
    for &ch in value {
        if ch == b'"' || ch == b'\\' {
            out.push(ch);
        }
        out.push(ch);
    }
    if nq {
        out.push(b'"');
    }
    Ok(())
}

/// range_in body shared with fc_range_in; `Ok(None)` = soft error captured.
pub fn range_in<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeIOData,
    input: &[u8],
    typmod: i32,
    esc: Option<&mut ::types_fmgr::ErrorSaveNode>,
) -> PgResult<Option<PgVec<'m, u8>>> {
    let mut soft = esc;
    let parsed = {
        let ctx = soft.as_deref_mut().map(|n| &mut n.ctx);
        match range_parse(mcx, input, ctx)? {
            Some(p) => p,
            None => return Ok(None),
        }
    };
    let flags = parsed.flags;

    let mut lower_val = Datum::null();
    let mut upper_val = Datum::null();
    if range_has_lbound(flags) {
        let s = CString::new(parsed.lbound.as_deref().unwrap_or(&[]))
            .expect("bound string has no interior NUL");
        if !input_function_call_safe(
            &mut cache.typioproc,
            Some(&s),
            cache.typioparam,
            typmod,
            mcx,
            soft.as_deref_mut(),
            &mut lower_val,
        )? {
            return Ok(None);
        }
        // An input function's by-ref result may alias its retained scratch
        // (textin's OutBuf); copy before the upper bound's call overwrites it.
        lower_val = copy_byref_bound(mcx, &cache.ri, lower_val)?;
    }
    if range_has_ubound(flags) {
        let s = CString::new(parsed.ubound.as_deref().unwrap_or(&[]))
            .expect("bound string has no interior NUL");
        if !input_function_call_safe(
            &mut cache.typioproc,
            Some(&s),
            cache.typioparam,
            typmod,
            mcx,
            soft.as_deref_mut(),
            &mut upper_val,
        )? {
            return Ok(None);
        }
    }

    let mut lower = RangeBound {
        val: lower_val,
        infinite: flags & RANGE_LB_INF != 0,
        inclusive: flags & RANGE_LB_INC != 0,
        lower: true,
    };
    let mut upper = RangeBound {
        val: upper_val,
        infinite: flags & RANGE_UB_INF != 0,
        inclusive: flags & RANGE_UB_INC != 0,
        lower: false,
    };

    let ctx = soft.as_deref_mut().map(|n| &mut n.ctx);
    make_range(mcx, &mut cache.ri, &mut lower, &mut upper, flags & RANGE_EMPTY != 0, ctx)
}

/// range_out body: NUL-terminated cstring image.
pub fn range_out<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeIOData,
    range: &[u8],
) -> PgResult<PgVec<'m, u8>> {
    let (lower, upper, _empty) = range_deserialize(&cache.ri.elem, range);
    let flags = range_get_flags(range);

    // An out function's cstring may alias its retained scratch; copy the
    // lower bound before the upper bound's out call overwrites it.
    let mut lbound_copy: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, 0)?;
    let mut lb: Option<&[u8]> = None;
    if range_has_lbound(flags) {
        let d =
            function_call1_coll_in(&mut cache.typioproc, ::types_core::InvalidOid, mcx, lower.val)?;
        ::mcx::vec_append_bytes(&mut lbound_copy, cstr_bytes(d))?;
        lb = Some(&lbound_copy);
    }
    let ubound_str;
    let mut ub: Option<&[u8]> = None;
    if range_has_ubound(flags) {
        ubound_str =
            function_call1_coll_in(&mut cache.typioproc, ::types_core::InvalidOid, mcx, upper.val)?;
        ub = Some(cstr_bytes(ubound_str));
    }
    range_deparse(mcx, flags, lb, ub)
}

#[inline]
fn cstr_bytes<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: an out function's result is a live NUL-terminated cstring.
    unsafe {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p, n)
    }
}

fn copy_byref_bound<'m>(mcx: Mcx<'m>, ri: &RangeInfo, d: Datum) -> PgResult<Datum> {
    let el = &ri.elem;
    if el.typbyval || d.as_usize() == 0 {
        return Ok(d);
    }
    let p = d.as_usize() as *const u8;
    let n = match el.typlen {
        // SAFETY: live varlena header readable through its full VARSIZE_ANY.
        -1 => unsafe { ::types_tuple::varatt::varsize_any(p) },
        l if l > 0 => l as usize,
        other => panic!("range bound copy: unsupported typlen {other}"),
    };
    let mut v: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, n)?;
    // SAFETY: n readable bytes at p; capacity reserved above.
    unsafe {
        core::ptr::copy_nonoverlapping(p, v.as_mut_ptr(), n);
        v.set_len(n);
    }
    Ok(Datum::from_usize(v.leak().as_ptr() as usize))
}

/// range_recv body.
pub fn range_recv<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeIOData,
    buf: &mut ::stringinfo::StringInfo<'_>,
    typmod: i32,
) -> PgResult<PgVec<'m, u8>> {
    let mut flags = ::pqformat::pq_getmsgbyte(buf)? as u8;
    flags &= RANGE_EMPTY | RANGE_LB_INC | RANGE_LB_INF | RANGE_UB_INC | RANGE_UB_INF;

    let mut recv_bound = |has: bool| -> PgResult<Datum> {
        if !has {
            return Ok(Datum::from_usize(0));
        }
        let bound_len = ::pqformat::pq_getmsgint(buf, 4)? as usize;
        let mut bound_buf = ::stringinfo::StringInfo::with_capacity_in(mcx, bound_len)?;
        bound_buf.append_bytes(::pqformat::pq_getmsgbytes(buf, bound_len)?)?;
        receive_function_call(
            &mut cache.typioproc,
            Some(&mut bound_buf),
            cache.typioparam,
            typmod,
            mcx,
        )
    };

    let lower_val = recv_bound(range_has_lbound(flags))?;
    let upper_val = recv_bound(range_has_ubound(flags))?;
    ::pqformat::pq_getmsgend(buf)?;

    let mut lower = RangeBound {
        val: lower_val,
        infinite: flags & RANGE_LB_INF != 0,
        inclusive: flags & RANGE_LB_INC != 0,
        lower: true,
    };
    let mut upper = RangeBound {
        val: upper_val,
        infinite: flags & RANGE_UB_INF != 0,
        inclusive: flags & RANGE_UB_INC != 0,
        lower: false,
    };

    Ok(make_range(mcx, &mut cache.ri, &mut lower, &mut upper, flags & RANGE_EMPTY != 0, None)?
        .expect("hard error path returns Some"))
}

/// range_send body: bytea image.
pub fn range_send<'m>(
    mcx: Mcx<'m>,
    cache: &mut RangeIOData,
    range: &[u8],
) -> PgResult<::datum::Bytea<'m>> {
    let (lower, upper, _empty) = range_deserialize(&cache.ri.elem, range);
    let flags = range_get_flags(range);

    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendbyte(&mut buf, flags)?;

    let mut send_bound = |buf: &mut ::stringinfo::StringInfo<'_>, val: Datum| -> PgResult<()> {
        let bound = send_function_call(&mut cache.typioproc, val, mcx)?;
        let p = bound.as_usize() as *const u8;
        let total = crate::varsize_4b(p);
        // SAFETY: a send function's result is a live 4-byte-header bytea.
        let payload = unsafe { core::slice::from_raw_parts(p.add(4), total - 4) };
        ::pqformat::pq_sendint32(buf, payload.len() as u32)?;
        ::pqformat::pq_sendbytes(buf, payload)
    };

    if range_has_lbound(flags) {
        send_bound(&mut buf, lower.val)?;
    }
    if range_has_ubound(flags) {
        send_bound(&mut buf, upper.val)?;
    }
    Ok(::pqformat::pq_endtypsend(buf))
}
