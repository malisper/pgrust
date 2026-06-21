//! Family `range-io`: text and binary I/O.
//!
//! Mirrors `rangetypes.c`: `range_in` / `range_out`, `range_recv` /
//! `range_send`, `get_range_io_data`, and the private `range_parse` /
//! `range_parse_flags` / `range_parse_bound` / `range_deparse` /
//! `range_bound_escape` helpers. Element text/binary I/O routes through the
//! element type's typio support fns (fmgr seam); the wire buffer through the
//! pqformat seam.

use backend_utils_adt_format_type_seams as format_type_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_typcache_seams as typcache_seams;
use backend_utils_fmgr_fmgr_seams as fmgr_seams;
use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION,
};
use types_rangetypes::{
    RangeBound, RangeTypeP, RANGE_EMPTY, RANGE_EMPTY_LITERAL, RANGE_LB_INC, RANGE_LB_INF,
    RANGE_LB_NULL, RANGE_UB_INC, RANGE_UB_INF, RANGE_UB_NULL,
};

use crate::range_repr_serialize::{make_range, make_range_soft, range_deserialize, range_get_flags};

/// `RANGE_HAS_LBOUND(flags)` (rangetypes.h:48).
#[inline]
fn range_has_lbound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF) == 0
}

/// `RANGE_HAS_UBOUND(flags)` (rangetypes.h:52).
#[inline]
fn range_has_ubound(flags: u8) -> bool {
    flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF) == 0
}

/// `RangeIOData` (rangetypes.c:50): the cached per-range-type I/O support: the
/// element type's typcache entry plus its in/out/recv/send function infos. The
/// fmgr `FmgrInfo`s are inherited-opacity handles owned by fmgr.
#[derive(Clone, Debug, Default)]
pub struct RangeIOData {
    /// `typcache` — the range type's cache entry.
    pub typcache: TypeCacheEntry,
    /// `typiofunc` — the element type's I/O function OID for the requested op.
    pub typiofunc: Oid,
    /// `typioparam` — the element type's I/O parameter OID.
    pub typioparam: Oid,
}

/// `IOFuncSelector` (fmgr.h): which element I/O direction `get_range_io_data`
/// resolves. Canonical definition in `types-core::fmgr`.
pub use types_core::fmgr::IOFuncSelector;

/// `TYPECACHE_RANGE_INFO` (typcache.h): the flag selecting the range-info fields
/// (`rngelemtype` / `rng_collation` / `rng_cmp_proc_finfo` /
/// `rng_canonical_finfo` / `rng_subdiff_finfo`) when resolving a range type's
/// `TypeCacheEntry`. Value matches `backend-utils-cache-typcache`'s
/// `TYPECACHE_RANGE_INFO`.
const TYPECACHE_RANGE_INFO: i32 = 0x00800;

/// `get_range_io_data(fcinfo, rngtypid, func)` (rangetypes.c:319): resolve and
/// cache the element I/O support for one direction.
///
/// Mirrors the `cache == NULL` build path: `lookup_type_cache(rngtypid,
/// TYPECACHE_RANGE_INFO)` (typcache.c, with the "type %u is not a range type"
/// elog when `rngelemtype == NULL`), `get_type_io_data(rngelemtype->type_id,
/// func, ...)` (lsyscache.c) for the element I/O proc + I/O param, and the
/// `ERRCODE_UNDEFINED_FUNCTION` "no binary {in,out}put function" ereport when
/// `typiofunc` is invalid. The `fmgr_info_cxt` step folds into carrying the
/// resolved `typiofunc` OID (the I/O wrappers dispatch by OID through the
/// element type's fmgr seams). `lookup_type_cache` / `get_type_io_data` are
/// owned by the genuinely-unported `backend-utils-cache-typcache` /
/// `backend-utils-cache-lsyscache`; the resolution routes through their seams
/// (mirroring the sibling `multirangetypes::get_multirange_io_data`).
pub fn get_range_io_data(rngtypid: Oid, func: IOFuncSelector) -> PgResult<RangeIOData> {
    // cache->typcache = lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO);
    let typcache =
        typcache_seams::lookup_type_cache_entry::call(rngtypid, TYPECACHE_RANGE_INFO)?;

    // if (cache->typcache->rngelemtype == NULL)
    //     elog(ERROR, "type %u is not a range type", rngtypid);
    let rngelemtype = match typcache.rngelemtype.as_deref() {
        Some(e) => e,
        None => {
            return Err(PgError::error(format!(
                "type {rngtypid} is not a range type"
            )));
        }
    };

    // get_type_io_data(cache->typcache->rngelemtype->type_id, func, &typlen,
    //                  &typbyval, &typalign, &typdelim, &cache->typioparam,
    //                  &typiofunc);
    let which = match func {
        IOFuncSelector::Input => lsyscache_seams::IOFuncSelector::Input,
        IOFuncSelector::Output => lsyscache_seams::IOFuncSelector::Output,
        IOFuncSelector::Receive => lsyscache_seams::IOFuncSelector::Receive,
        IOFuncSelector::Send => lsyscache_seams::IOFuncSelector::Send,
    };
    let io = lsyscache_seams::get_type_io_data::call(rngelemtype.type_id, which)?;

    // if (!OidIsValid(typiofunc)) -- can only happen for receive or send.
    if io.func == 0 {
        // C: ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION),
        //            errmsg("no binary {input,output} function available for
        //            type %s", format_type_be(cache->typcache->rngelemtype->type_id)));
        let elem_oid = rngelemtype.type_id;
        let cx = mcx::MemoryContext::new("get_range_io_data error");
        let name = match format_type_seams::format_type_be::call(cx.mcx(), elem_oid) {
            Ok(s) => s.as_str().to_string(),
            Err(_) => elem_oid.to_string(),
        };
        if func == IOFuncSelector::Receive {
            return Err(PgError::error(format!(
                "no binary input function available for type {name}"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
        } else {
            return Err(PgError::error(format!(
                "no binary output function available for type {name}"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION));
        }
    }

    // fmgr_info_cxt(typiofunc, &cache->typioproc, ...) -- the owned cache carries
    // the resolved proc OID; the I/O wrappers re-resolve / dispatch by OID
    // through the element type's fmgr seams.
    Ok(RangeIOData {
        typcache,
        typiofunc: io.func,
        typioparam: io.typioparam,
    })
}

/// Call the element type's text input function on one bound substring,
/// mirroring C's `InputFunctionCallSafe(&cache->typioproc, bound_str,
/// cache->typioparam, typmod, escontext, &bound.val)`.
///
/// Routes through the SAFE seam (`input_function_call_safe`), forwarding
/// `escontext` exactly as C forwards `fcinfo->context`. A recoverable (soft)
/// conversion error returns `Ok(None)` (C's `InputFunctionCallSafe` returning
/// `false`, with the error already saved into `escontext`), which `range_in`
/// turns into `PG_RETURN_NULL`. When `escontext` is `None` the seam escalates a
/// conversion error to a hard `Err`, exactly as C's NULL-escontext path does.
///
/// On success the seam yields the bare element `Datum` already in the
/// `RangeBound.val` word form — a by-value scalar's machine word, or a pointer
/// into `mcx` to the by-reference element's flattened image (the same shape
/// `canon_to_bound_word` produces on the hard path) — so no further lowering is
/// needed.
fn call_bound_input<'mcx>(
    mcx: Mcx<'mcx>,
    cache: &RangeIOData,
    bound_str: &str,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<types_datum::datum::Datum>> {
    fmgr_seams::input_function_call_safe::call(
        mcx,
        cache.typiofunc,
        bound_str,
        cache.typioparam,
        typmod,
        escontext,
    )
}

/// `range_in(input, typioparam, typmod, escontext)` body (rangetypes.c:90).
///
/// Returns `Ok(None)` for a soft (`escontext`) error (C `PG_RETURN_NULL` after a
/// recoverable parse / element-input failure); `Ok(Some(range))` on success.
pub fn range_in<'mcx>(
    mcx: Mcx<'mcx>,
    cache: &RangeIOData,
    input: &str,
    _typmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<RangeTypeP<'mcx>>> {
    // check_stack_depth(); -- recursion guard, owned by the C runtime.

    // C: cache = get_range_io_data(fcinfo, rngtypoid, IOFunc_input); -- the
    // caller resolved it; we receive the resolved cache.

    // parse
    // C: if (!range_parse(input_str, &flags, &lbound_str, &ubound_str, escontext))
    //        PG_RETURN_NULL();
    let (flags, lbound_str, ubound_str) =
        match range_parse(input, escontext.as_deref_mut())? {
            Some(parsed) => parsed,
            None => return Ok(None),
        };

    // call element type's input function
    let mut lower = RangeBound::default();
    let mut upper = RangeBound::default();

    if range_has_lbound(flags) {
        let lbound_str = lbound_str.expect("RANGE_HAS_LBOUND implies a parsed lower bound string");
        // C: if (!InputFunctionCallSafe(&cache->typioproc, lbound_str,
        //          cache->typioparam, typmod, escontext, &lower.val))
        //        PG_RETURN_NULL();
        match call_bound_input(mcx, cache, lbound_str.as_str(), _typmod, escontext.as_deref_mut())? {
            Some(val) => lower.val = val,
            None => return Ok(None),
        }
    }
    if range_has_ubound(flags) {
        let ubound_str = ubound_str.expect("RANGE_HAS_UBOUND implies a parsed upper bound string");
        // C: if (!InputFunctionCallSafe(&cache->typioproc, ubound_str,
        //          cache->typioparam, typmod, escontext, &upper.val))
        //        PG_RETURN_NULL();
        match call_bound_input(mcx, cache, ubound_str.as_str(), _typmod, escontext.as_deref_mut())? {
            Some(val) => upper.val = val,
            None => return Ok(None),
        }
    }

    lower.infinite = flags & RANGE_LB_INF != 0;
    lower.inclusive = flags & RANGE_LB_INC != 0;
    lower.lower = true;
    upper.infinite = flags & RANGE_UB_INF != 0;
    upper.inclusive = flags & RANGE_UB_INC != 0;
    upper.lower = false;

    // serialize and canonicalize
    // C: range = make_range(typcache, &lower, &upper, flags & RANGE_EMPTY,
    //                       escontext); if (!range) PG_RETURN_NULL();
    //
    // `make_range` forwards `escontext` to `range_serialize`, whose
    // lower-bound-above-upper-bound check (rangetypes.c:1819) `ereturn`s a soft
    // error — so a malformed `[5,1)` literal under `pg_input_is_valid` returns
    // false instead of raising.
    match make_range_soft(
        mcx,
        &cache.typcache,
        &lower,
        &upper,
        flags & RANGE_EMPTY != 0,
        escontext.as_deref_mut(),
    )? {
        Some(range) => Ok(Some(range)),
        None => Ok(None),
    }
}

/// `range_out(range)` body (rangetypes.c:139): the canonical text form.
///
/// Takes `Mcx<'mcx>` because the element type's output function allocates its
/// `char *` result in a memory context (the bytes the seam returns are charged
/// to `mcx`); C charges them to `CurrentMemoryContext`.
pub fn range_out<'mcx>(
    mcx: Mcx<'mcx>,
    cache: &RangeIOData,
    range: RangeTypeP<'_>,
) -> PgResult<String> {
    // check_stack_depth();

    // C: cache = get_range_io_data(fcinfo, RangeTypeGetOid(range), IOFunc_output);
    // -- the caller resolved it; we receive the resolved cache.

    // deserialize
    let (lower, upper, _empty) = range_deserialize(&cache.typcache, range)?;
    let flags = range_get_flags(range);

    // call element type's output function
    let lbound_str: Option<String> = if range_has_lbound(flags) {
        // C: lbound_str = OutputFunctionCall(&cache->typioproc, lower.val);
        // The element output function is resolved by its OID (`cache.typiofunc`)
        // and run on the bound value. The deserialized `lower.val` is the bare
        // element-value word (a scalar for by-value types, a pointer into the
        // range object for by-reference types); it crosses to the seam as the
        // owned per-attribute value model's by-value word. The result is the
        // NUL-excluded text bytes.
        let bytes = fmgr_seams::oid_output_function_call::call(
            mcx,
            cache.typiofunc,
            &crate::range_bounds_compare::elem_word_to_canon(mcx, &cache.typcache, lower.val)?,
        )?;
        Some(io_bytes_to_string(&bytes))
    } else {
        None
    };
    let ubound_str: Option<String> = if range_has_ubound(flags) {
        // C: ubound_str = OutputFunctionCall(&cache->typioproc, upper.val);
        let bytes = fmgr_seams::oid_output_function_call::call(
            mcx,
            cache.typiofunc,
            &crate::range_bounds_compare::elem_word_to_canon(mcx, &cache.typcache, upper.val)?,
        )?;
        Some(io_bytes_to_string(&bytes))
    } else {
        None
    };

    // construct result string
    range_deparse(flags, lbound_str.as_deref(), ubound_str.as_deref())
}

/// `range_recv(buf, typioparam, typmod)` body (rangetypes.c:179).
///
/// Binary representation: the first byte is the flags, then the lower bound (if
/// present), then the upper bound (if present). Each bound is a 4-byte length
/// header and the subtype send function's binary image.
pub fn range_recv<'mcx>(
    mcx: Mcx<'mcx>,
    cache: &RangeIOData,
    buf: &[u8],
    _typmod: i32,
) -> PgResult<RangeTypeP<'mcx>> {
    // check_stack_depth();

    // C: cache = get_range_io_data(fcinfo, rngtypoid, IOFunc_receive);

    // The wire buffer is a forward-only cursor (the C `StringInfo` read cursor).
    let mut cur: &[u8] = buf;

    // receive the flags ...
    // C: flags = (unsigned char) pq_getmsgbyte(buf);
    let flags = pq_getmsgbyte(&mut cur)?;

    // Mask out any unsupported flags, particularly RANGE_xB_NULL which would
    // confuse following tests. range_serialize cleans up the rest.
    let flags = flags & (RANGE_EMPTY | RANGE_LB_INC | RANGE_LB_INF | RANGE_UB_INC | RANGE_UB_INF);

    let mut lower = RangeBound::default();
    let mut upper = RangeBound::default();

    // receive the bounds ...
    if range_has_lbound(flags) {
        // C: bound_len = pq_getmsgint(buf, 4);
        //    bound_data = pq_getmsgbytes(buf, bound_len);
        //    initStringInfo(&bound_buf); appendBinaryStringInfo(...);
        //    lower.val = ReceiveFunctionCall(&cache->typioproc, &bound_buf,
        //                                    cache->typioparam, typmod);
        let bound_len = pq_getmsgint32(&mut cur)? as usize;
        let bound_data = pq_getmsgbytes(&mut cur, bound_len)?;
        lower.val = fmgr_seams::receive_function_call::call(
            mcx,
            cache.typiofunc,
            bound_data,
            cache.typioparam,
            _typmod,
        )?;
    } else {
        lower.val = types_datum::datum::Datum::from_usize(0);
    }

    if range_has_ubound(flags) {
        // C: same as above for upper.val
        let bound_len = pq_getmsgint32(&mut cur)? as usize;
        let bound_data = pq_getmsgbytes(&mut cur, bound_len)?;
        upper.val = fmgr_seams::receive_function_call::call(
            mcx,
            cache.typiofunc,
            bound_data,
            cache.typioparam,
            _typmod,
        )?;
    } else {
        upper.val = types_datum::datum::Datum::from_usize(0);
    }

    // C: pq_getmsgend(buf);
    pq_getmsgend(&cur)?;

    // finish constructing RangeBound representation
    lower.infinite = flags & RANGE_LB_INF != 0;
    lower.inclusive = flags & RANGE_LB_INC != 0;
    lower.lower = true;
    upper.infinite = flags & RANGE_UB_INF != 0;
    upper.inclusive = flags & RANGE_UB_INC != 0;
    upper.lower = false;

    // serialize and canonicalize
    make_range(mcx, &cache.typcache, &lower, &upper, flags & RANGE_EMPTY != 0)
}

/// `range_send(range)` body (rangetypes.c:263): the binary wire image.
///
/// Takes `Mcx<'mcx>` because the element type's send function allocates its
/// `bytea *` result in a memory context (the payload bytes the seam returns are
/// charged to `mcx`); C charges them to `CurrentMemoryContext`.
pub fn range_send<'mcx>(
    mcx: Mcx<'mcx>,
    cache: &RangeIOData,
    range: RangeTypeP<'_>,
) -> PgResult<Vec<u8>> {
    // check_stack_depth();

    // C: cache = get_range_io_data(fcinfo, RangeTypeGetOid(range), IOFunc_send);

    // deserialize
    let (lower, upper, _empty) = range_deserialize(&cache.typcache, range)?;
    let flags = range_get_flags(range);

    // construct output
    // C: pq_begintypsend(buf); pq_sendbyte(buf, flags);
    let mut buf: Vec<u8> = Vec::new();
    buf.push(flags);

    if range_has_lbound(flags) {
        // C: bound = PointerGetDatum(SendFunctionCall(&cache->typioproc, lower.val));
        //    bound_len = VARSIZE(bound) - VARHDRSZ; bound_data = VARDATA(bound);
        //    pq_sendint32(buf, bound_len); pq_sendbytes(buf, bound_data, bound_len);
        //
        // The seam resolves the element send function by OID and returns the
        // `bytea` payload with the varlena header already stripped (so its
        // length is the C `VARSIZE - VARHDRSZ` and the slice is `VARDATA`).
        let payload = fmgr_seams::oid_send_function_call::call(
            mcx,
            cache.typiofunc,
            &crate::range_bounds_compare::elem_word_to_canon(mcx, &cache.typcache, lower.val)?,
        )?;
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
    }

    if range_has_ubound(flags) {
        // C: same as above for upper.val
        let payload = fmgr_seams::oid_send_function_call::call(
            mcx,
            cache.typiofunc,
            &crate::range_bounds_compare::elem_word_to_canon(mcx, &cache.typcache, upper.val)?,
        )?;
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
    }

    // C: PG_RETURN_BYTEA_P(pq_endtypsend(buf));
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Inward seam adapters: the generic range I/O procs the built-in range types
// register and that `multirangetypes` invokes via the rangetypes-seams
// `range_in` / `range_out` / `range_recv` / `range_send` seams. Each resolves
// the per-direction element I/O cache (`get_range_io_data`, which the boundary
// also does for the `PG_FUNCTION_ARGS` entries) then runs the kernel.
// ---------------------------------------------------------------------------

/// Inward seam shape for `range_in` (rangetypes-seams). Resolves the input
/// cache then runs the kernel, forwarding `escontext` (C's `fcinfo->context`):
/// a recoverable parse / element-input error surfaces as `Ok(None)` when
/// `escontext` is `Some`, or a hard `Err` when `None`.
pub fn range_in_seam<'mcx>(
    mcx: Mcx<'mcx>,
    input: &str,
    rngtypoid: Oid,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<RangeTypeP<'mcx>>> {
    let cache = get_range_io_data(rngtypoid, IOFuncSelector::Input)?;
    range_in(mcx, &cache, input, typmod, escontext)
}

/// Inward seam shape for `range_out` (rangetypes-seams). The seam returns an
/// owned `String`, so the element output function's transient allocations are
/// run against a private scratch context dropped on return (mirroring the
/// `bounds_adjacent` / `range_adjacent` adapters).
pub fn range_out_seam(range: RangeTypeP<'_>) -> PgResult<String> {
    let cache = get_range_io_data(range.rangetypid(), IOFuncSelector::Output)?;
    let scratch = mcx::MemoryContext::new_bump("range_out element output");
    range_out(scratch.mcx(), &cache, range)
}

/// Inward seam shape for `range_recv` (rangetypes-seams). Resolves the receive
/// cache then runs the kernel; the result range escapes in `mcx`.
pub fn range_recv_seam<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    rngtypoid: Oid,
    typmod: i32,
) -> PgResult<RangeTypeP<'mcx>> {
    let cache = get_range_io_data(rngtypoid, IOFuncSelector::Receive)?;
    range_recv(mcx, &cache, buf, typmod)
}

/// Inward seam shape for `range_send` (rangetypes-seams). The seam returns an
/// owned `Vec<u8>`, so the element send function's transient allocations are run
/// against a private scratch context dropped on return.
pub fn range_send_seam(range: RangeTypeP<'_>) -> PgResult<Vec<u8>> {
    let cache = get_range_io_data(range.rangetypid(), IOFuncSelector::Send)?;
    let scratch = mcx::MemoryContext::new_bump("range_send element send");
    range_send(scratch.mcx(), &cache, range)
}

/// `range_parse(string, &flags, &lbound, &ubound)` (rangetypes.c:2386): split a
/// text literal into its flags byte and bound substrings (`None` = infinite).
///
/// `escontext` is C's soft-error sink: every malformed-literal `ereturn`
/// becomes a soft `Ok(None)` when `escontext` is `Some` (the error saved into
/// it) or a hard `Err` when `None`. On success returns `Ok(Some((flags,
/// lbound, ubound)))`.
pub fn range_parse(
    string: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<(u8, Option<String>, Option<String>)>> {
    let bytes = string.as_bytes();
    let mut ptr = 0usize;
    let mut flags: u8 = 0;

    // consume whitespace
    while ptr < bytes.len() && is_space(bytes[ptr]) {
        ptr += 1;
    }

    // check for empty range (pg_strncasecmp against "empty")
    let empty_lit = RANGE_EMPTY_LITERAL.as_bytes();
    if strncasecmp_prefix(&bytes[ptr..], empty_lit) {
        flags = RANGE_EMPTY;
        ptr += empty_lit.len();

        // the rest should be whitespace
        while ptr < bytes.len() && is_space(bytes[ptr]) {
            ptr += 1;
        }

        // should have consumed everything
        if ptr < bytes.len() {
            return ereturn(
                escontext,
                None,
                malformed_literal(string).with_detail("Junk after \"empty\" key word."),
            );
        }

        return Ok(Some((flags, None, None)));
    }

    if ptr < bytes.len() && bytes[ptr] == b'[' {
        flags |= RANGE_LB_INC;
        ptr += 1;
    } else if ptr < bytes.len() && bytes[ptr] == b'(' {
        ptr += 1;
    } else {
        return ereturn(
            escontext,
            None,
            malformed_literal(string).with_detail("Missing left parenthesis or bracket."),
        );
    }

    // C: ptr = range_parse_bound(string, ptr, lbound_str, &infinite, escontext);
    //    if (ptr == NULL) return false;  -- here the soft `Ok(None)` propagates.
    //    if (infinite) *flags |= RANGE_LB_INF;
    // range_parse_bound returns "" for the infinite case; C leaves *lbound_str
    // NULL then, which `range_deparse`/`RANGE_HAS_LBOUND` never reads anyway.
    let (lbound_str, infinite, next) =
        match range_parse_bound(string, ptr, escontext.as_deref_mut())? {
            Some(v) => v,
            None => return Ok(None),
        };
    ptr = next;
    let lbound_str = if infinite { None } else { Some(lbound_str) };
    if infinite {
        flags |= RANGE_LB_INF;
    }

    if ptr < bytes.len() && bytes[ptr] == b',' {
        ptr += 1;
    } else {
        return ereturn(
            escontext,
            None,
            malformed_literal(string).with_detail("Missing comma after lower bound."),
        );
    }

    let (ubound_str, infinite, next) =
        match range_parse_bound(string, ptr, escontext.as_deref_mut())? {
            Some(v) => v,
            None => return Ok(None),
        };
    ptr = next;
    let ubound_str = if infinite { None } else { Some(ubound_str) };
    if infinite {
        flags |= RANGE_UB_INF;
    }

    if ptr < bytes.len() && bytes[ptr] == b']' {
        flags |= RANGE_UB_INC;
        ptr += 1;
    } else if ptr < bytes.len() && bytes[ptr] == b')' {
        ptr += 1;
    } else {
        // must be a comma
        return ereturn(
            escontext,
            None,
            malformed_literal(string).with_detail("Too many commas."),
        );
    }

    // consume whitespace
    while ptr < bytes.len() && is_space(bytes[ptr]) {
        ptr += 1;
    }

    if ptr < bytes.len() {
        return ereturn(
            escontext,
            None,
            malformed_literal(string).with_detail("Junk after right parenthesis or bracket."),
        );
    }

    Ok(Some((flags, lbound_str, ubound_str)))
}

/// `range_parse_flags(flags_str)` (rangetypes.c:2311): the `[)`/`(]`/... flags.
pub fn range_parse_flags(flags_str: &str) -> PgResult<u8> {
    let bytes = flags_str.as_bytes();
    let mut flags: u8 = 0;

    // C tests flags_str[0]/[1]/[2] against '\0'. flags_str is NUL-terminated;
    // here the `&str` length plays the role of the NUL position.
    //   flags_str[0] == '\0'  -> len == 0
    //   flags_str[1] == '\0'  -> len == 1
    //   flags_str[2] != '\0'  -> len > 2
    if bytes.is_empty() || bytes.len() == 1 || bytes.len() > 2 {
        return Err(PgError::error("invalid range bound flags")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_hint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"."));
    }

    match bytes[0] {
        b'[' => flags |= RANGE_LB_INC,
        b'(' => {}
        _ => {
            return Err(PgError::error("invalid range bound flags")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                .with_hint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"."));
        }
    }

    match bytes[1] {
        b']' => flags |= RANGE_UB_INC,
        b')' => {}
        _ => {
            return Err(PgError::error("invalid range bound flags")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                .with_hint("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"."));
        }
    }

    Ok(flags)
}

/// `range_parse_bound(string, ptr, &bound, &infinite)` (rangetypes.c:2502):
/// scan one bound substring, returning `(bound_text, infinite, next_offset)`.
///
/// `escontext` is C's soft-error sink: the "Unexpected end of input" `ereturn`s
/// become a soft `Ok(None)` when `escontext` is `Some` (error saved) or a hard
/// `Err` when `None`. On success returns `Ok(Some((bound_text, infinite,
/// next_offset)))`.
pub fn range_parse_bound(
    string: &str,
    ptr: usize,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<(String, bool, usize)>> {
    let bytes = string.as_bytes();
    let mut ptr = ptr;

    // Check for null: a bound terminator right here means no bound.
    if ptr < bytes.len() && (bytes[ptr] == b',' || bytes[ptr] == b')' || bytes[ptr] == b']') {
        return Ok(Some((String::new(), true, ptr)));
    }
    // C also enters the else branch when at end-of-string ('\0'); the scan loop
    // then immediately hits ch == '\0' and ereturns. Mirror that.

    // Extract string for this bound.
    let mut inquote = false;
    let mut buf: Vec<u8> = Vec::new();

    loop {
        let at_terminator = ptr < bytes.len()
            && (bytes[ptr] == b',' || bytes[ptr] == b')' || bytes[ptr] == b']');
        if !(inquote || !at_terminator) {
            break;
        }

        // char ch = *ptr++;
        let ch = if ptr < bytes.len() { bytes[ptr] } else { 0 };
        ptr += 1;

        if ch == 0 {
            return ereturn(
                escontext,
                None,
                malformed_literal(string).with_detail("Unexpected end of input."),
            );
        }
        if ch == b'\\' {
            // if (*ptr == '\0') ereturn; appendStringInfoChar(&buf, *ptr++);
            if ptr >= bytes.len() {
                return ereturn(
                    escontext,
                    None,
                    malformed_literal(string).with_detail("Unexpected end of input."),
                );
            }
            buf.push(bytes[ptr]);
            ptr += 1;
        } else if ch == b'"' {
            if !inquote {
                inquote = true;
            } else if ptr < bytes.len() && bytes[ptr] == b'"' {
                // doubled quote within quote sequence
                buf.push(bytes[ptr]);
                ptr += 1;
            } else {
                inquote = false;
            }
        } else {
            buf.push(ch);
        }
    }

    // buf is built from input bytes (which were valid UTF-8 in `string`); the
    // escaping only ever copies whole input bytes, so the result is valid too.
    let bound_str = String::from_utf8(buf).expect("bound bytes are a subsequence of valid UTF-8");
    Ok(Some((bound_str, false, ptr)))
}

/// `range_deparse(flags, lbound, ubound)` (rangetypes.c:2571): assemble the
/// text literal from a flags byte and the two escaped bound strings.
pub fn range_deparse(flags: u8, lbound: Option<&str>, ubound: Option<&str>) -> PgResult<String> {
    if flags & RANGE_EMPTY != 0 {
        return Ok(RANGE_EMPTY_LITERAL.to_string());
    }

    let mut buf = String::new();

    buf.push(if flags & RANGE_LB_INC != 0 { '[' } else { '(' });

    if range_has_lbound(flags) {
        let lb = lbound.expect("RANGE_HAS_LBOUND implies a lower bound string");
        buf.push_str(&range_bound_escape(lb)?);
    }

    buf.push(',');

    if range_has_ubound(flags) {
        let ub = ubound.expect("RANGE_HAS_UBOUND implies an upper bound string");
        buf.push_str(&range_bound_escape(ub)?);
    }

    buf.push(if flags & RANGE_UB_INC != 0 { ']' } else { ')' });

    Ok(buf)
}

/// `range_bound_escape(value)` (rangetypes.c:2601): quote/escape one bound
/// value for the text representation.
pub fn range_bound_escape(value: &str) -> PgResult<String> {
    let bytes = value.as_bytes();
    let mut buf = String::new();

    // Detect whether we need double quotes for this value.
    // nq = (value[0] == '\0');  -- force quotes for empty string
    let mut nq = bytes.is_empty();
    for &ch in bytes {
        if ch == b'"'
            || ch == b'\\'
            || ch == b'('
            || ch == b')'
            || ch == b'['
            || ch == b']'
            || ch == b','
            || is_space(ch)
        {
            nq = true;
            break;
        }
    }

    // And emit the string.
    if nq {
        buf.push('"');
    }
    for ch in value.chars() {
        if ch == '"' || ch == '\\' {
            buf.push(ch);
        }
        buf.push(ch);
    }
    if nq {
        buf.push('"');
    }

    Ok(buf)
}

// --- private helpers --------------------------------------------------------

/// C `isspace((unsigned char) ch)` over the "C" locale set used by the parser.
#[inline]
fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// `pg_strncasecmp(ptr, lit, strlen(lit)) == 0`: case-insensitive (ASCII)
/// prefix match of `lit` at the start of `haystack`.
fn strncasecmp_prefix(haystack: &[u8], lit: &[u8]) -> bool {
    if haystack.len() < lit.len() {
        return false;
    }
    haystack[..lit.len()]
        .iter()
        .zip(lit.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// The shared `errmsg("malformed range literal: \"%s\"", string)` with
/// `ERRCODE_INVALID_TEXT_REPRESENTATION`; callers attach the per-site
/// `errdetail`.
fn malformed_literal(string: &str) -> PgError {
    PgError::error(format!("malformed range literal: \"{string}\""))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// Decode the element type's text output bytes (the NUL-excluded `char *` image
/// the output-function seam returns) into the owned `String` the deparser
/// escapes. The output function emits valid server-encoding (UTF-8) text.
#[inline]
fn io_bytes_to_string(bytes: &[u8]) -> String {
    core::str::from_utf8(bytes)
        .expect("type output function returns valid UTF-8 text")
        .to_string()
}

// ---------------------------------------------------------------------------
// pqformat-style binary cursor helpers (pqformat.c), over the `&mut &[u8]`
// receive cursor. The wire integers are network byte order (big-endian),
// mirroring `pq_getmsgint`. Same shape as the sibling
// `multirangetypes::typcache_io` cursor readers (pqformat is not ported into
// this unit; the readers are re-derived locally, matching that precedent).
// ---------------------------------------------------------------------------

/// `pq_getmsgbyte(buf)` — read one byte, advancing the cursor. `Err` is the C
/// `errmsg("insufficient data left in message")`.
fn pq_getmsgbyte(buf: &mut &[u8]) -> PgResult<u8> {
    if buf.is_empty() {
        return Err(insufficient_data());
    }
    let (head, tail) = buf.split_at(1);
    *buf = tail;
    Ok(head[0])
}

/// `pq_getmsgint(buf, 4)` — read a big-endian `uint32`, advancing the cursor.
/// `Err` is the C `errmsg("insufficient data left in message")`.
fn pq_getmsgint32(buf: &mut &[u8]) -> PgResult<u32> {
    if buf.len() < 4 {
        return Err(insufficient_data());
    }
    let (head, tail) = buf.split_at(4);
    let v = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
    *buf = tail;
    Ok(v)
}

/// `pq_getmsgbytes(buf, datalen)` — consume `datalen` bytes, advancing the
/// cursor. `Err` is the C `errmsg("insufficient data left in message")`.
fn pq_getmsgbytes<'a>(buf: &mut &'a [u8], datalen: usize) -> PgResult<&'a [u8]> {
    if datalen > buf.len() {
        return Err(insufficient_data());
    }
    let (head, tail) = buf.split_at(datalen);
    *buf = tail;
    Ok(head)
}

/// `pq_getmsgend(buf)` — verify the message is fully consumed; C raises
/// `errmsg("invalid message format")` otherwise.
fn pq_getmsgend(buf: &&[u8]) -> PgResult<()> {
    if !buf.is_empty() {
        return Err(PgError::error("invalid message format".to_string())
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION));
    }
    Ok(())
}

/// The C `ereport(ERROR, errcode(ERRCODE_PROTOCOL_VIOLATION),
/// errmsg("insufficient data left in message"))` shared by the cursor readers.
fn insufficient_data() -> PgError {
    PgError::error("insufficient data left in message".to_string())
}
