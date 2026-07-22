use alloc::boxed::Box;
use core::ffi::CStr;

use ::datum::Datum;
use ::mcx::{vec_new_in, vec_with_capacity_in, Mcx, PgVec};
use ::stringinfo::StringInfo;
use ::types_core::{primitive::InvalidOid, Oid};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_fmgr::{
    function_call1_coll_in, input_function_call_safe, receive_function_call, send_function_call,
    ErrorSaveNode, FmgrInfo,
};

use crate::construct::{construct_empty_array, construct_md_array, deconstruct_array};
// C OutputFunctionCall arms CurrentMemoryContext implicitly; the local-frame
// helper must arm explicitly for out fns that allocate (record_out).
pub(crate) fn call1_armed(
    flinfo: &mut ::types_fmgr::FmgrInfo,
    mcx: ::mcx::Mcx<'_>,
    arg: ::datum::Datum,
) -> ::types_error::PgResult<::datum::Datum> {
    ::types_fmgr::function_call1_coll_in(flinfo, ::types_core::InvalidOid, mcx, arg)
}

use crate::foundation::*;
use ::arrayutils::{array_check_bounds, array_get_n_items};

// Resolved element I/O metadata for one array function call.
pub struct ArrayIoMeta {
    pub element_type: Oid,
    pub typlen: i32,
    pub typbyval: bool,
    pub typalign: u8,
    pub typdelim: u8,
    pub typioparam: Oid,
}

const ASSGN: &[u8] = b"=";

#[inline]
fn scanner_isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

#[cold]
fn malformed(orig: &str, detail: &str) -> PgError {
    PgError::error(alloc::format!("malformed array literal: \"{orig}\""))
        .with_detail(detail)
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

// ereturn against the fmgr ErrorSaveNode: soft path records + returns None.
fn soft<T>(escontext: Option<&mut ErrorSaveNode>, err: PgError) -> PgResult<Option<T>> {
    match escontext {
        Some(n) => {
            if n.ctx.details_wanted() {
                n.ctx.save(err);
            } else {
                n.ctx.mark_error_occurred();
            }
            Ok(None)
        }
        None => Err(Box::new(err)),
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum ArrayTok {
    LevelStart,
    LevelEnd,
    Delim,
    Elem,
    ElemNull,
}

// ---- array_in --------------------------------------------------------------

pub fn array_in<'mcx>(
    mcx: Mcx<'mcx>,
    string: &str,
    meta: &ArrayIoMeta,
    proc: &mut FmgrInfo,
    typmod: i32,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let s = string.as_bytes();
    let mut dim = [-1i32; MAXDIM];
    let mut lbound = [1i32; MAXDIM];
    let mut pos = 0usize;

    let ndim = match read_array_dimensions(s, &mut pos, &mut dim, &mut lbound, string, escontext.as_deref_mut())? {
        Some(n) => n,
        None => return Ok(None),
    };

    if ndim == 0 {
        if s.get(pos) != Some(&b'{') {
            return soft(escontext, malformed(string, "Array value must start with \"{\" or dimension information."));
        }
    } else {
        if !s[pos..].starts_with(ASSGN) {
            return soft(escontext, malformed(string, "Missing \"=\" after array dimensions."));
        }
        pos += ASSGN.len();
        while pos < s.len() && scanner_isspace(s[pos]) {
            pos += 1;
        }
        if s.get(pos) != Some(&b'{') {
            return soft(escontext, malformed(string, "Array contents must start with \"{\"."));
        }
    }

    let mut ndim = ndim;
    let parsed = read_array_str(
        mcx, s, &mut pos, &mut ndim, &mut dim, meta, proc, typmod, string,
        escontext.as_deref_mut(),
    )?;
    let (nitems, values, nulls) = match parsed {
        Some(v) => v,
        None => return Ok(None),
    };

    while pos < s.len() {
        if !scanner_isspace(s[pos]) {
            return soft(escontext, malformed(string, "Junk after closing right brace."));
        }
        pos += 1;
    }

    if nitems == 0 {
        return Ok(Some(construct_empty_array(mcx, meta.element_type)?));
    }

    let img = construct_md_array(
        mcx,
        &values,
        Some(&nulls),
        ndim,
        &dim[..ndim as usize],
        &lbound[..ndim as usize],
        meta.element_type,
        meta.typlen,
        meta.typbyval,
        meta.typalign,
    )?;
    Ok(Some(img))
}

fn read_dimension_int(s: &[u8], pos: &mut usize, escontext: Option<&mut ErrorSaveNode>) -> PgResult<Option<i32>> {
    let c = s.get(*pos).copied().unwrap_or(0);
    if !c.is_ascii_digit() && c != b'-' && c != b'+' {
        return Ok(Some(0));
    }
    let mut i = *pos;
    let mut neg = false;
    match s.get(i) {
        Some(b'+') => i += 1,
        Some(b'-') => {
            neg = true;
            i += 1;
        }
        _ => {}
    }
    let mut acc: i64 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        acc = acc * 10 + (s[i] - b'0') as i64;
        if acc > i64::from(i32::MAX) + 1 {
            overflow = true;
        }
        i += 1;
    }
    *pos = i;
    let val = if neg { -acc } else { acc };
    if overflow || val > i32::MAX as i64 || val < i32::MIN as i64 {
        return soft(
            escontext,
            PgError::error("array bound is out of integer range").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        );
    }
    Ok(Some(val as i32))
}

#[allow(clippy::too_many_arguments)]
fn read_array_dimensions(
    s: &[u8],
    pos: &mut usize,
    dim: &mut [i32; MAXDIM],
    lbound: &mut [i32; MAXDIM],
    orig: &str,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<i32>> {
    let mut ndim = 0usize;
    loop {
        while *pos < s.len() && scanner_isspace(s[*pos]) {
            *pos += 1;
        }
        if s.get(*pos) != Some(&b'[') {
            break;
        }
        *pos += 1;
        if ndim >= MAXDIM {
            return soft(
                escontext,
                PgError::error(alloc::format!(
                    "number of array dimensions exceeds the maximum allowed ({MAXDIM})"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        let before = *pos;
        let i = match read_dimension_int(s, pos, escontext.as_deref_mut())? {
            Some(v) => v,
            None => return Ok(None),
        };
        if *pos == before {
            return soft(escontext, malformed(orig, "\"[\" must introduce explicitly-specified array dimensions."));
        }
        let ub;
        if s.get(*pos) == Some(&b':') {
            lbound[ndim] = i;
            *pos += 1;
            let before2 = *pos;
            let u = match read_dimension_int(s, pos, escontext.as_deref_mut())? {
                Some(v) => v,
                None => return Ok(None),
            };
            if *pos == before2 {
                return soft(escontext, malformed(orig, "Missing array dimension value."));
            }
            ub = u;
        } else {
            lbound[ndim] = 1;
            ub = i;
        }
        if s.get(*pos) != Some(&b']') {
            return soft(escontext, malformed(orig, "Missing \"]\" after array dimensions."));
        }
        *pos += 1;
        if ub < lbound[ndim] {
            return soft(
                escontext,
                PgError::error("upper bound cannot be less than lower bound")
                    .with_sqlstate(::types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR),
            );
        }
        if ub == i32::MAX {
            return soft(
                escontext,
                PgError::error(alloc::format!("array upper bound is too large: {ub}"))
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        let (d, o1) = ub.overflowing_sub(lbound[ndim]);
        let (d, o2) = d.overflowing_add(1);
        if o1 || o2 {
            return soft(
                escontext,
                PgError::error(alloc::format!(
                    "array size exceeds the maximum allowed ({})",
                    ::arrayutils::MAX_ARRAY_SIZE
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        dim[ndim] = d;
        ndim += 1;
    }
    Ok(Some(ndim as i32))
}

struct ElemReader<'a, 'mcx> {
    s: &'a [u8],
    pos: usize,
    typdelim: u8,
    elembuf: PgVec<'mcx, u8>,
}

impl<'a, 'mcx> ElemReader<'a, 'mcx> {
    // Returns Ok(Some(tok)) / Ok(None) soft-error / Err hard.
    fn next_token(&mut self, orig: &str, escontext: Option<&mut ErrorSaveNode>) -> PgResult<Option<ArrayTok>> {
        self.elembuf.clear();
        // Identify token; skip leading whitespace.
        loop {
            let c = self.s.get(self.pos).copied().unwrap_or(0);
            match c {
                0 => return soft(escontext, malformed(orig, "Unexpected end of input.")),
                b'{' => {
                    self.pos += 1;
                    return Ok(Some(ArrayTok::LevelStart));
                }
                b'}' => {
                    self.pos += 1;
                    return Ok(Some(ArrayTok::LevelEnd));
                }
                b'"' => {
                    self.pos += 1;
                    return self.quoted_element(orig, escontext);
                }
                _ => {
                    if c == self.typdelim {
                        self.pos += 1;
                        return Ok(Some(ArrayTok::Delim));
                    }
                    if scanner_isspace(c) {
                        self.pos += 1;
                        continue;
                    }
                    return self.unquoted_element(orig, escontext);
                }
            }
        }
    }

    fn quoted_element(&mut self, orig: &str, escontext: Option<&mut ErrorSaveNode>) -> PgResult<Option<ArrayTok>> {
        loop {
            let c = self.s.get(self.pos).copied().unwrap_or(0);
            match c {
                0 => return soft(escontext, malformed(orig, "Unexpected end of input.")),
                b'\\' => {
                    self.pos += 1;
                    let n = self.s.get(self.pos).copied().unwrap_or(0);
                    if n == 0 {
                        return soft(escontext, malformed(orig, "Unexpected end of input."));
                    }
                    self.elembuf.push(n);
                    self.pos += 1;
                }
                b'"' => {
                    self.pos += 1;
                    while self.pos < self.s.len() {
                        let d = self.s[self.pos];
                        if d == self.typdelim || d == b'}' || d == b'{' {
                            return Ok(Some(ArrayTok::Elem));
                        }
                        if !scanner_isspace(d) {
                            return soft(escontext, malformed(orig, "Incorrectly quoted array element."));
                        }
                        self.pos += 1;
                    }
                    return soft(escontext, malformed(orig, "Unexpected end of input."));
                }
                _ => {
                    self.elembuf.push(c);
                    self.pos += 1;
                }
            }
        }
    }

    fn unquoted_element(&mut self, orig: &str, escontext: Option<&mut ErrorSaveNode>) -> PgResult<Option<ArrayTok>> {
        let mut dstlen = 0usize;
        let mut has_escapes = false;
        loop {
            let c = self.s.get(self.pos).copied().unwrap_or(0);
            match c {
                0 => return soft(escontext, malformed(orig, "Unexpected end of input.")),
                b'{' => return soft(escontext, malformed(orig, "Unexpected \"{\" character.")),
                b'"' => return soft(escontext, malformed(orig, "Incorrectly quoted array element.")),
                b'\\' => {
                    self.pos += 1;
                    let n = self.s.get(self.pos).copied().unwrap_or(0);
                    if n == 0 {
                        return soft(escontext, malformed(orig, "Unexpected end of input."));
                    }
                    self.elembuf.push(n);
                    self.pos += 1;
                    dstlen = self.elembuf.len();
                    has_escapes = true;
                }
                _ => {
                    if c == self.typdelim || c == b'}' {
                        self.elembuf.truncate(dstlen);
                        // C: `Array_nulls && !has_escapes && pg_strcasecmp
                        // (.., "NULL") == 0` — with array_nulls=off an
                        // unquoted NULL is the literal string (arrayfuncs.c).
                        if crate::array_nulls() && !has_escapes && eq_null_ci(&self.elembuf) {
                            return Ok(Some(ArrayTok::ElemNull));
                        }
                        return Ok(Some(ArrayTok::Elem));
                    }
                    self.elembuf.push(c);
                    if !scanner_isspace(c) {
                        dstlen = self.elembuf.len();
                    }
                    self.pos += 1;
                }
            }
        }
    }
}

fn eq_null_ci(b: &[u8]) -> bool {
    b.len() == 4 && b.eq_ignore_ascii_case(b"NULL")
}

#[allow(clippy::too_many_arguments)]
fn read_array_str<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    pos: &mut usize,
    ndim_p: &mut i32,
    dim: &mut [i32; MAXDIM],
    meta: &ArrayIoMeta,
    proc: &mut FmgrInfo,
    typmod: i32,
    orig: &str,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Option<(i32, PgVec<'mcx, Datum>, PgVec<'mcx, bool>)>> {
    let mut ndim = *ndim_p;
    let dimensions_specified = ndim != 0;
    let mut values: PgVec<Datum> = vec_new_in(mcx);
    let mut nulls: PgVec<bool> = vec_new_in(mcx);

    let mut reader = ElemReader {
        s,
        pos: *pos,
        typdelim: meta.typdelim,
        elembuf: vec_new_in(mcx),
    };

    let mut nest_level = 0i32;
    let mut nitems = 0i32;
    let mut ndim_frozen = dimensions_specified;
    let mut expect_delim = false;
    let mut nelems = [0i32; MAXDIM];

    loop {
        let tok = match reader.next_token(orig, escontext.as_deref_mut())? {
            Some(t) => t,
            None => return Ok(None),
        };
        match tok {
            ArrayTok::LevelStart => {
                if expect_delim {
                    return soft(escontext, malformed(orig, "Unexpected \"{\" character."));
                }
                if nest_level >= MAXDIM as i32 {
                    return soft(
                        escontext,
                        PgError::error(alloc::format!(
                            "number of array dimensions exceeds the maximum allowed ({MAXDIM})"
                        ))
                        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    );
                }
                nelems[nest_level as usize] = 0;
                nest_level += 1;
                if nest_level > ndim {
                    if ndim_frozen {
                        return dimension_error(escontext, orig, dimensions_specified);
                    }
                    ndim = nest_level;
                }
            }
            ArrayTok::LevelEnd => {
                if nelems[(nest_level - 1) as usize] > 0 && !expect_delim {
                    return soft(escontext, malformed(orig, "Unexpected \"}\" character."));
                }
                nest_level -= 1;
                if nest_level > 0 {
                    nelems[(nest_level - 1) as usize] += 1;
                }
                if dim[nest_level as usize] < 0 {
                    dim[nest_level as usize] = nelems[nest_level as usize];
                } else if nelems[nest_level as usize] != dim[nest_level as usize] {
                    return dimension_error(escontext, orig, dimensions_specified);
                }
                expect_delim = true;
            }
            ArrayTok::Delim => {
                if !expect_delim {
                    return soft(
                        escontext,
                        malformed(
                            orig,
                            &alloc::format!(
                                "Unexpected \"{}\" character.",
                                meta.typdelim as char
                            ),
                        ),
                    );
                }
                expect_delim = false;
            }
            ArrayTok::Elem | ArrayTok::ElemNull => {
                if expect_delim {
                    return soft(escontext, malformed(orig, "Unexpected array element."));
                }
                if nitems as usize >= ::arrayutils::MAX_ARRAY_SIZE as usize {
                    return soft(
                        escontext,
                        PgError::error(alloc::format!(
                            "array size exceeds the maximum allowed ({})",
                            ::arrayutils::MAX_ARRAY_SIZE
                        ))
                        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    );
                }
                let is_null = tok == ArrayTok::ElemNull;
                let mut result = Datum::null();
                let cstr_owned;
                let cstr: Option<&CStr> = if is_null {
                    None
                } else {
                    reader.elembuf.push(0);
                    cstr_owned = CStr::from_bytes_with_nul(&reader.elembuf)
                        .map_err(|_| Box::new(malformed(orig, "invalid array element")))?;
                    Some(cstr_owned)
                };
                let ok = input_function_call_safe(
                    proc,
                    cstr,
                    meta.typioparam,
                    typmod,
                    mcx,
                    escontext.as_deref_mut(),
                    &mut result,
                )?;
                if !ok {
                    return Ok(None);
                }
                if !is_null && !meta.typbyval {
                    // Input fc wrappers may return flinfo-scratch-backed bytes
                    // (fc_textin); batched element datums must own theirs
                    // (C: per-call palloc inside the input function).
                    result = copy_byref_datum(mcx, result, meta.typlen)?;
                }
                if values.try_reserve(1).is_err() {
                    return Err(Box::new(mcx.oom(8)));
                }
                values.push(result);
                nulls.push(is_null);
                nitems += 1;
                ndim_frozen = true;
                if nest_level != ndim {
                    return dimension_error(escontext, orig, dimensions_specified);
                }
                nelems[(nest_level - 1) as usize] += 1;
                expect_delim = true;
            }
        }
        if nest_level <= 0 {
            break;
        }
    }

    *pos = reader.pos;
    *ndim_p = ndim;
    Ok(Some((nitems, values, nulls)))
}

fn copy_byref_datum(mcx: Mcx<'_>, d: Datum, typlen: i32) -> PgResult<Datum> {
    let p = d.as_usize() as *const u8;
    let size = match typlen {
        -1 => unsafe { varsize_any(p) },
        // SAFETY (both arms): d is a live by-ref datum of its declared layout.
        -2 => unsafe { CStr::from_ptr(p.cast()) }.to_bytes_with_nul().len(),
        n if n > 0 => n as usize,
        other => panic!("copy_byref_datum: unexpected typlen {other}"),
    };
    // SAFETY: size bytes readable per the datum layout above.
    let image = unsafe { core::slice::from_raw_parts(p, size) };
    ::types_fmgr::byref_result(mcx, image)
}

fn dimension_error<T>(escontext: Option<&mut ErrorSaveNode>, orig: &str, specified: bool) -> PgResult<Option<T>> {
    let detail = if specified {
        "Specified array dimensions do not match array contents."
    } else {
        "Multidimensional arrays must have sub-arrays with matching dimensions."
    };
    soft(escontext, malformed(orig, detail))
}

// ---- array_out -------------------------------------------------------------

pub fn array_out<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    meta: &ArrayIoMeta,
    proc: &mut FmgrInfo,
) -> PgResult<PgVec<'mcx, u8>> {
    let (ndim, dims, lb) = read_dims_lbounds(array);
    let nitems = array_get_n_items(ndim, &dims)?;

    let mut out: PgVec<u8> = vec_new_in(mcx);
    if nitems == 0 {
        ::mcx::vec_append_bytes(&mut out, b"{}\0")?;
        return Ok(out);
    }

    let needdims = (0..ndim as usize).any(|i| lb[i] != 1);
    let (elems, elem_nulls) =
        deconstruct_array(mcx, array, meta.typlen, meta.typbyval, meta.typalign, true)?;

    if needdims {
        for i in 0..ndim as usize {
            let s = alloc::format!("[{}:{}]", lb[i], lb[i] + dims[i] - 1);
            ::mcx::vec_append_bytes(&mut out, s.as_bytes())?;
        }
        out.push(b'=');
    }

    out.push(b'{');
    let ndim_u = ndim as usize;
    let nulls_s: &[bool] = &elem_nulls;
    let elems_s: &[Datum] = &elems;
    let mut indx = [0i32; MAXDIM];
    let mut j = 0i32;
    let mut k = 0usize;
    loop {
        // Element k's output string is produced on demand; the out proc's
        // returned cstring aliases reusable scratch, so it is copied here
        // (with quoting) before the next element's out call. One reserve
        // covers this iteration's worst case; emission is unchecked cursor
        // writes (per-byte push costs ~15 instr/output byte vs C's strcpy).
        // SAFETY: k < nitems = length of both deconstruct vecs.
        let is_null = unsafe { *nulls_s.get_unchecked(k) };
        let bytes: &[u8] = if is_null {
            b"NULL"
        } else {
            // SAFETY: as above.
            // mcx-armed: out procs of packed-capable elements (numeric)
            // detoast-expand short images into the frame's result mcx.
            let d = function_call1_coll_in(proc, InvalidOid, mcx, unsafe {
                *elems_s.get_unchecked(k)
            })?;
            cstring_datum(d)
        };
        let quote = !is_null && element_needs_quote(bytes, meta.typdelim);
        let n = bytes.len();
        let extra = 2 * n + 2 + 2 * ndim_u + 2;
        if out.capacity() - out.len() < extra {
            out.try_reserve(extra).map_err(|_| mcx.oom(extra))?;
        }
        // SAFETY: `extra` bounds every write below (braces/delims <= 2*ndim_u+1,
        // element <= 2n+2); set_len covers exactly the bytes written.
        unsafe {
            let base = out.as_mut_ptr();
            let mut w = out.len();
            let mut i = j as usize;
            while i < ndim_u - 1 {
                *base.add(w) = b'{';
                w += 1;
                i += 1;
            }
            if quote {
                *base.add(w) = b'"';
                w += 1;
                for &ch in bytes {
                    if ch == b'"' || ch == b'\\' {
                        *base.add(w) = b'\\';
                        w += 1;
                    }
                    *base.add(w) = ch;
                    w += 1;
                }
                *base.add(w) = b'"';
                w += 1;
            } else {
                core::ptr::copy_nonoverlapping(bytes.as_ptr(), base.add(w), n);
                w += n;
            }
            k += 1;

            let mut ii = ndim - 1;
            loop {
                indx[ii as usize] += 1;
                if indx[ii as usize] < dims[ii as usize] {
                    *base.add(w) = meta.typdelim;
                    w += 1;
                    break;
                } else {
                    indx[ii as usize] = 0;
                    *base.add(w) = b'}';
                    w += 1;
                }
                ii -= 1;
                if ii < 0 {
                    break;
                }
            }
            j = ii;
            out.set_len(w);
        }
        if j == -1 {
            break;
        }
    }
    out.push(0);
    Ok(out)
}

fn element_needs_quote(bytes: &[u8], typdelim: u8) -> bool {
    if bytes.is_empty() {
        return true;
    }
    if bytes.eq_ignore_ascii_case(b"NULL") {
        return true;
    }
    for &ch in bytes {
        if ch == b'"' || ch == b'\\' {
            return true;
        }
        if ch == b'{' || ch == b'}' || ch == typdelim || scanner_isspace(ch) {
            return true;
        }
    }
    false
}

#[inline]
fn cstring_datum<'a>(d: Datum) -> &'a [u8] {
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

// ---- array_recv / array_send ----------------------------------------------

pub fn array_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    meta: &ArrayIoMeta,
    proc: &mut FmgrInfo,
    typmod: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let ndim = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    if ndim < 0 {
        return Err(Box::new(
            PgError::error(alloc::format!("invalid number of dimensions: {ndim}"))
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
        ));
    }
    if ndim as usize > MAXDIM {
        return Err(Box::new(
            PgError::error(alloc::format!(
                "number of array dimensions ({ndim}) exceeds the maximum allowed ({MAXDIM})"
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    let flags = ::pqformat::pq_getmsgint(buf, 4)?;
    if flags != 0 && flags != 1 {
        return Err(Box::new(
            PgError::error("invalid array flags").with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
        ));
    }
    // Element type recorded in the data (we trust the caller's spec type).
    let _elem_type_wire = ::pqformat::pq_getmsgint(buf, 4)?;
    let mut dim = [0i32; MAXDIM];
    let mut lbound = [0i32; MAXDIM];
    for i in 0..ndim as usize {
        dim[i] = ::pqformat::pq_getmsgint(buf, 4)? as i32;
        lbound[i] = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    }
    let nitems = array_get_n_items(ndim, &dim)?;
    array_check_bounds(ndim, &dim, &lbound)?;

    if nitems == 0 {
        return construct_empty_array(mcx, meta.element_type);
    }

    let mut values: PgVec<Datum> = vec_with_capacity_in(mcx, nitems as usize)?;
    let mut nulls: PgVec<bool> = vec_with_capacity_in(mcx, nitems as usize)?;
    read_array_binary(mcx, buf, nitems, proc, meta, typmod, &mut values, &mut nulls)?;

    construct_md_array(
        mcx,
        &values,
        Some(&nulls),
        ndim,
        &dim[..ndim as usize],
        &lbound[..ndim as usize],
        meta.element_type,
        meta.typlen,
        meta.typbyval,
        meta.typalign,
    )
}

#[allow(clippy::too_many_arguments)]
fn read_array_binary<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    nitems: i32,
    proc: &mut FmgrInfo,
    meta: &ArrayIoMeta,
    typmod: i32,
    values: &mut PgVec<'mcx, Datum>,
    nulls: &mut PgVec<'mcx, bool>,
) -> PgResult<()> {
    for i in 0..nitems as usize {
        let itemlen = ::pqformat::pq_getmsgint(buf, 4)? as i32;
        let remaining = (buf.len() - buf.cursor) as i32;
        if itemlen < -1 || itemlen > remaining {
            return Err(Box::new(
                PgError::error("insufficient data left in message")
                    .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
            ));
        }
        if itemlen == -1 {
            values.push(Datum::null());
            nulls.push(true);
            continue;
        }
        // Read a copy of the element bytes into a fresh StringInfo.
        let mut elem_buf = StringInfo::with_capacity_in(mcx, itemlen as usize + 1)?;
        {
            let slice = ::pqformat::pq_getmsgbytes(buf, itemlen as usize)?;
            elem_buf.append_bytes(slice)?;
        }
        let v = receive_function_call(proc, Some(&mut elem_buf), meta.typioparam, typmod, mcx)?;
        if elem_buf.cursor != itemlen as usize {
            return Err(Box::new(
                PgError::error(alloc::format!("improper binary format in array element {}", i + 1))
                    .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
            ));
        }
        values.push(v);
        nulls.push(false);
    }
    Ok(())
}

pub fn array_send<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
    meta: &ArrayIoMeta,
    proc: &mut FmgrInfo,
) -> PgResult<::datum::Bytea<'mcx>> {
    let (ndim, dims, lb) = read_dims_lbounds(array);
    let nitems = array_get_n_items(ndim, &dims)?;
    let hasnull = crate::construct::array_contains_nulls(array);
    let (elems, elem_nulls) =
        deconstruct_array(mcx, array, meta.typlen, meta.typbyval, meta.typalign, true)?;

    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, ndim as u32)?;
    ::pqformat::pq_sendint32(&mut buf, if hasnull { 1 } else { 0 })?;
    ::pqformat::pq_sendint32(&mut buf, meta.element_type as u32)?;
    for i in 0..ndim as usize {
        ::pqformat::pq_sendint32(&mut buf, dims[i] as u32)?;
        ::pqformat::pq_sendint32(&mut buf, lb[i] as u32)?;
    }
    for i in 0..nitems as usize {
        if elem_nulls[i] {
            ::pqformat::pq_sendint32(&mut buf, (-1i32) as u32)?;
        } else {
            let d = send_function_call(proc, elems[i], mcx)?;
            let p = d.as_usize() as *const u8;
            let total = varsize_any(p);
            // SAFETY: send returns a live 4B-header bytea of `total` bytes.
            let payload = unsafe { core::slice::from_raw_parts(p.add(::datum::VARHDRSZ), total - ::datum::VARHDRSZ) };
            ::pqformat::pq_sendint32(&mut buf, (total - ::datum::VARHDRSZ) as u32)?;
            ::pqformat::pq_sendbytes(&mut buf, payload)?;
        }
    }
    Ok(::pqformat::pq_endtypsend(buf))
}
