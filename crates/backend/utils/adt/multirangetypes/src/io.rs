use std::ffi::CString;

use ::datum::Datum;
use ::lsyscache::{get_type_io_data, IOFuncSelector};
use ::mcx::{Mcx, PgVec};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{
    ereturn, PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_UNDEFINED_FUNCTION,
};
use ::types_fmgr::{
    function_call1_coll_in, input_function_call_safe, receive_function_call, send_function_call,
    ErrorSaveNode, FmgrInfo,
};

use crate::{make_multirange, multirange_count, multirange_deserialize, MultirangeInfo};
use ::adt_rangetypes::{range_is_empty, RANGE_EMPTY_LITERAL};

pub struct MultirangeIOData {
    pub mi: MultirangeInfo,
    pub typioproc: FmgrInfo,
    pub typioparam: Oid,
}

#[track_caller]
#[cold]
fn no_binary_io(recv: bool, rngtypid: Oid) -> Box<PgError> {
    let what = if recv { "input" } else { "output" };
    let t = ::format_type::format_type_be(rngtypid)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| format!("{rngtypid}"));
    Box::new(
        PgError::error(format!("no binary {what} function available for type {t}"))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
    )
}

pub fn cached_multirange_io_data<'f>(
    flinfo: &'f mut FmgrInfo,
    mltrngtypid: Oid,
    func: IOFuncSelector,
) -> PgResult<&'f mut MultirangeIOData> {
    let need = match flinfo.fn_extra_ref::<MultirangeIOData>() {
        Some(c) => c.mi.mltrngtypid != mltrngtypid,
        None => true,
    };
    if need {
        let mi = MultirangeInfo::lookup(mltrngtypid)?;
        let io = get_type_io_data(mi.rng.rngtypid, func)?;
        if io.func == 0 {
            return Err(no_binary_io(matches!(func, IOFuncSelector::IOFunc_receive), mi.rng.rngtypid));
        }
        let typioproc = ::fmgr_seams::fmgr_info::call(io.func)?;
        flinfo.set_fn_extra(MultirangeIOData { mi, typioproc, typioparam: io.typioparam });
    }
    Ok(flinfo.fn_extra_mut::<MultirangeIOData>().unwrap())
}

#[inline]
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

#[cold]
fn malformed(string: &[u8], detail: &str) -> PgError {
    PgError::error(format!(
        "malformed multirange literal: \"{}\"",
        String::from_utf8_lossy(string)
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
    .with_detail(detail.to_string())
}

#[derive(PartialEq)]
enum ParseState {
    BeforeRange,
    InRange,
    InRangeEscaped,
    InRangeQuoted,
    InRangeQuotedEscaped,
    AfterRange,
    Finished,
}

/// multirange_in body; `Ok(None)` = soft error captured.
pub fn multirange_in<'m>(
    mcx: Mcx<'m>,
    cache: &mut MultirangeIOData,
    input: &[u8],
    typmod: i32,
    mut esc: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<PgVec<'m, u8>>> {
    let mltrngtypid = cache.mi.mltrngtypid;
    let mut ranges: PgVec<'_, &'m [u8]> = ::mcx::vec_with_capacity_in(mcx, 8)?;
    let mut ranges_seen = 0usize;

    let mut pos = 0usize;
    while pos < input.len() && is_space(input[pos]) {
        pos += 1;
    }
    if input.get(pos) == Some(&b'{') {
        pos += 1;
    } else {
        let ctx = esc.as_deref_mut().map(|n| &mut n.ctx);
        return ereturn(ctx, None, malformed(input, "Missing left brace."));
    }

    let mut range_str_begin = 0usize;
    let mut state = ParseState::BeforeRange;
    while state != ParseState::Finished {
        let Some(&ch) = input.get(pos) else {
            let ctx = esc.as_deref_mut().map(|n| &mut n.ctx);
            return ereturn(ctx, None, malformed(input, "Unexpected end of input."));
        };
        if is_space(ch) {
            pos += 1;
            continue;
        }
        match state {
            ParseState::BeforeRange => {
                if ch == b'[' || ch == b'(' {
                    range_str_begin = pos;
                    state = ParseState::InRange;
                } else if ch == b'}' && ranges_seen == 0 {
                    state = ParseState::Finished;
                } else if input.len() - pos >= RANGE_EMPTY_LITERAL.len()
                    && input[pos..pos + RANGE_EMPTY_LITERAL.len()]
                        .eq_ignore_ascii_case(RANGE_EMPTY_LITERAL.as_bytes())
                {
                    ranges_seen += 1;
                    pos += RANGE_EMPTY_LITERAL.len() - 1;
                    state = ParseState::AfterRange;
                } else {
                    let ctx = esc.as_deref_mut().map(|n| &mut n.ctx);
                    return ereturn(ctx, None, malformed(input, "Expected range start."));
                }
            }
            ParseState::InRange => {
                if ch == b']' || ch == b')' {
                    let range_str = &input[range_str_begin..=pos];
                    ranges_seen += 1;
                    let s = CString::new(range_str).expect("range string has no interior NUL");
                    let mut range_datum = Datum::null();
                    if !input_function_call_safe(
                        &mut cache.typioproc,
                        Some(&s),
                        cache.typioparam,
                        typmod,
                        mcx,
                        esc.as_deref_mut(),
                        &mut range_datum,
                    )? {
                        return Ok(None);
                    }
                    let range = range_datum_bytes(range_datum);
                    if !range_is_empty(range) {
                        ranges.push(range);
                    }
                    state = ParseState::AfterRange;
                } else if ch == b'"' {
                    state = ParseState::InRangeQuoted;
                } else if ch == b'\\' {
                    state = ParseState::InRangeEscaped;
                }
            }
            ParseState::InRangeEscaped => state = ParseState::InRange,
            ParseState::InRangeQuoted => {
                if ch == b'"' {
                    if input.get(pos + 1) == Some(&b'"') {
                        pos += 1;
                    } else {
                        state = ParseState::InRange;
                    }
                } else if ch == b'\\' {
                    state = ParseState::InRangeQuotedEscaped;
                }
            }
            ParseState::InRangeQuotedEscaped => state = ParseState::InRangeQuoted,
            ParseState::AfterRange => {
                if ch == b',' {
                    state = ParseState::BeforeRange;
                } else if ch == b'}' {
                    state = ParseState::Finished;
                } else {
                    let ctx = esc.as_deref_mut().map(|n| &mut n.ctx);
                    return ereturn(
                        ctx,
                        None,
                        malformed(input, "Expected comma or end of multirange."),
                    );
                }
            }
            ParseState::Finished => unreachable!(),
        }
        pos += 1;
    }

    while pos < input.len() && is_space(input[pos]) {
        pos += 1;
    }
    if pos != input.len() {
        let ctx = esc.as_deref_mut().map(|n| &mut n.ctx);
        return ereturn(ctx, None, malformed(input, "Junk after closing right brace."));
    }

    Ok(Some(make_multirange(mcx, mltrngtypid, &mut cache.mi.rng, &mut ranges)?))
}

// A range input/receive result datum is a fresh 4-byte-header image in mcx.
fn range_datum_bytes<'m>(d: Datum) -> &'m [u8] {
    let p = d.as_usize() as *const u8;
    let total = ::adt_rangetypes::varsize_4b(p);
    // SAFETY: live range image of `total` bytes, mcx-owned (leaked to arena).
    unsafe { core::slice::from_raw_parts(p, total) }
}

/// multirange_out body: NUL-terminated cstring image.
pub fn multirange_out<'m>(
    mcx: Mcx<'m>,
    cache: &mut MultirangeIOData,
    mr: &[u8],
) -> PgResult<PgVec<'m, u8>> {
    let mut out: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, 32)?;
    out.push(b'{');
    let ranges = multirange_deserialize(mcx, &cache.mi.rng, mr)?;
    for (i, r) in ranges.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        let d = function_call1_coll_in(
            &mut cache.typioproc,
            InvalidOid,
            mcx,
            Datum::from_usize(r.as_ptr() as usize),
        )?;
        let p = d.as_usize() as *const u8;
        // SAFETY: an out function's result is a live NUL-terminated cstring.
        unsafe {
            let mut n = 0usize;
            while *p.add(n) != 0 {
                n += 1;
            }
            ::mcx::vec_append_bytes(&mut out, core::slice::from_raw_parts(p, n))?;
        }
    }
    out.push(b'}');
    out.push(0);
    Ok(out)
}

/// multirange_recv body.
pub fn multirange_recv<'m>(
    mcx: Mcx<'m>,
    cache: &mut MultirangeIOData,
    buf: &mut ::stringinfo::StringInfo<'_>,
    typmod: i32,
) -> PgResult<PgVec<'m, u8>> {
    let range_count = ::pqformat::pq_getmsgint(buf, 4)? as usize;
    let mut ranges: PgVec<'_, &'m [u8]> = ::mcx::vec_with_capacity_in(mcx, range_count)?;

    for _ in 0..range_count {
        let range_len = ::pqformat::pq_getmsgint(buf, 4)? as usize;
        let mut tmpbuf = ::stringinfo::StringInfo::with_capacity_in(mcx, range_len)?;
        tmpbuf.append_bytes(::pqformat::pq_getmsgbytes(buf, range_len)?)?;
        let d = receive_function_call(
            &mut cache.typioproc,
            Some(&mut tmpbuf),
            cache.typioparam,
            typmod,
            mcx,
        )?;
        ranges.push(range_datum_bytes(d));
    }
    ::pqformat::pq_getmsgend(buf)?;

    make_multirange(mcx, cache.mi.mltrngtypid, &mut cache.mi.rng, &mut ranges)
}

/// multirange_send body: bytea image.
pub fn multirange_send<'m>(
    mcx: Mcx<'m>,
    cache: &mut MultirangeIOData,
    mr: &[u8],
) -> PgResult<::datum::Bytea<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, multirange_count(mr))?;

    let ranges = multirange_deserialize(mcx, &cache.mi.rng, mr)?;
    for r in ranges.iter() {
        let d = send_function_call(
            &mut cache.typioproc,
            Datum::from_usize(r.as_ptr() as usize),
            mcx,
        )?;
        let p = d.as_usize() as *const u8;
        let total = ::adt_rangetypes::varsize_4b(p);
        // SAFETY: a send function's result is a live 4-byte-header bytea.
        let payload = unsafe { core::slice::from_raw_parts(p.add(4), total - 4) };
        ::pqformat::pq_sendint32(&mut buf, payload.len() as u32)?;
        ::pqformat::pq_sendbytes(&mut buf, payload)?;
    }
    Ok(::pqformat::pq_endtypsend(buf))
}
