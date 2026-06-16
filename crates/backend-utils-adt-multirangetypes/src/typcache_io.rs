//! typcache+IO: the multirange typcache lookup, `DatumGetMultirangeTypeP`
//! detoast, and the text/binary I/O functions.
//!
//! `multirange_in`/`recv` parse a multirange by delegating each member range to
//! the range type's own I/O proc (reached through `rangetypes-seams`);
//! `multirange_out`/`send` do the reverse. Owns the inward seams
//! `multirange_get_typcache` and `datum_get_multirange_type_p`.

use mcx::Mcx;
use types_cache::typcache::TypeCacheEntry;
use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_error::{PgError, PgResult};
use types_error::error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_UNDEFINED_FUNCTION};
use types_rangetypes::{MultirangeType, MultirangeTypeP, RANGE_EMPTY, RANGE_EMPTY_LITERAL};

use backend_access_common_detoast::pg_detoast_datum;
use backend_utils_adt_format_type_seams as format_type_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_typcache_seams as typcache_seams;
use backend_utils_adt_rangetypes_seams as rangetypes_seams;

use crate::serialize_core::{make_multirange, multirange_deserialize};

/// `TYPECACHE_MULTIRANGE_INFO` (typcache.h): the flag selecting the multirange
/// info (`->rngtype`) when looking up a multirange type's cache entry.
pub const TYPECACHE_MULTIRANGE_INFO: i32 = 0x10000;

/// `fn_extra` cache entry for one of the range I/O functions
/// (`MultirangeIOData`, multirangetypes.c:48): the multirange typcache plus the
/// member range type's I/O proc and its I/O parameter OID.
pub struct MultirangeIOData {
    /// `typcache` — the multirange type's typcache entry.
    pub typcache: TypeCacheEntry,
    /// `typioproc` — the range type's I/O proc.
    pub typioproc: FmgrInfo,
    /// `typioparam` — the range type's I/O parameter OID.
    pub typioparam: Oid,
}

/// `IOFuncSelector` (fmgr.h): which I/O direction `get_multirange_io_data`
/// resolves a proc for. Canonical definition in `types-core::fmgr`.
pub use types_core::fmgr::IOFuncSelector;

/// `MultirangeParseState` (multirangetypes.c:56): the `multirange_in` parser's
/// state machine.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum MultirangeParseState {
    BeforeRange,
    InRange,
    InRangeEscaped,
    InRangeQuoted,
    InRangeQuotedEscaped,
    AfterRange,
    Finished,
}

/// `isspace((unsigned char) ch)` — mirror C's `<ctype.h>` classification over a
/// single byte (the C cast widens then tests for the standard whitespace set).
#[inline]
fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// The byte at `bytes[i]`, or `0` (`'\0'`) at/after the end — mirroring C's
/// NUL-terminated `*ptr`.
#[inline]
fn at(bytes: &[u8], i: usize) -> u8 {
    bytes.get(i).copied().unwrap_or(0)
}

/// `multirange_get_typcache(fcinfo, mltrngtypid)` (multirangetypes.c:549): the
/// cached `TypeCacheEntry` for the multirange type. The inward
/// `multirange_get_typcache` seam.
///
/// C caches the entry in `fcinfo->flinfo->fn_extra`; the owned model re-looks-up
/// each call (the cache is the typcache's own job) and returns the entry by
/// value.
pub fn multirange_get_typcache(mltrngtypid: Oid) -> PgResult<TypeCacheEntry> {
    // typcache = lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO);
    let typcache =
        typcache_seams::lookup_type_cache_entry::call(mltrngtypid, TYPECACHE_MULTIRANGE_INFO)?;
    // if (typcache->rngtype == NULL) elog(ERROR, "type %u is not a multirange type", ...);
    if typcache.rngtype.is_none() {
        return Err(PgError::error(format!(
            "type {} is not a multirange type",
            mltrngtypid
        )));
    }
    Ok(typcache)
}

/// `get_multirange_io_data(fcinfo, mltrngtypid, func)` (multirangetypes.c:415):
/// resolve and cache the multirange typcache + member range I/O proc.
///
/// C caches the resolved `MultirangeIOData` in `fn_extra`; the owned model
/// rebuilds it each call (matching the `cache == NULL` branch) and returns it by
/// value.
pub fn get_multirange_io_data(
    mltrngtypid: Oid,
    func: IOFuncSelector,
) -> PgResult<MultirangeIOData> {
    // cache->typcache = lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO);
    let typcache =
        typcache_seams::lookup_type_cache_entry::call(mltrngtypid, TYPECACHE_MULTIRANGE_INFO)?;
    // if (cache->typcache->rngtype == NULL) elog(ERROR, "type %u is not a multirange type", ...);
    let rngtype = match typcache.rngtype.as_deref() {
        Some(r) => r,
        None => {
            return Err(PgError::error(format!(
                "type {} is not a multirange type",
                mltrngtypid
            )))
        }
    };

    // get_type_io_data(cache->typcache->rngtype->type_id, func, &typlen,
    //                  &typbyval, &typalign, &typdelim, &cache->typioparam,
    //                  &typiofunc);
    let which = match func {
        IOFuncSelector::Input => lsyscache_seams::IOFuncSelector::Input,
        IOFuncSelector::Output => lsyscache_seams::IOFuncSelector::Output,
        IOFuncSelector::Receive => lsyscache_seams::IOFuncSelector::Receive,
        IOFuncSelector::Send => lsyscache_seams::IOFuncSelector::Send,
    };
    let io = lsyscache_seams::get_type_io_data::call(rngtype.type_id, which)?;

    // if (!OidIsValid(typiofunc)) -- can only happen for receive or send.
    if io.func == 0 {
        // C: ereport(ERROR, errcode(ERRCODE_UNDEFINED_FUNCTION),
        //            errmsg("no binary {input,output} function available for
        //            type %s", format_type_be(rngtype->type_id))).
        // `format_type_be` needs a context for the palloc'd name; a transient
        // context suffices and is dropped with this builder.
        let rngtype_oid = rngtype.type_id;
        let cx = mcx::MemoryContext::new("get_multirange_io_data error");
        let name = match format_type_seams::format_type_be::call(cx.mcx(), rngtype_oid) {
            Ok(s) => s.as_str().to_string(),
            Err(_) => rngtype_oid.to_string(),
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

    // fmgr_info_cxt(typiofunc, &cache->typioproc, fcinfo->flinfo->fn_mcxt);
    // The owned `FmgrInfo` carries only the resolved proc OID (the lookup key);
    // the I/O wrappers re-resolve / dispatch by OID through the range owner's
    // I/O seams.
    let typioproc = FmgrInfo { fn_oid: io.func, ..Default::default() };

    Ok(MultirangeIOData {
        typcache,
        typioproc,
        typioparam: io.typioparam,
    })
}

/// `DatumGetMultirangeTypeP(d)` (multirangetypes.h): detoast a `Datum` into a
/// `MultirangeType *`. The inward `datum_get_multirange_type_p` seam.
///
/// C: `((MultirangeType *) PG_DETOAST_DATUM(d))`, i.e. `pg_detoast_datum` over
/// the varlena `DatumGetPointer(d)` points at. The detoasted (or verbatim-copy)
/// buffer is allocated in `mcx`; we hand its address out as the opaque
/// `MultirangeTypeP` and let the buffer live for `'mcx` (C leaves it in the
/// current context).
pub fn datum_get_multirange_type_p<'mcx>(
    mcx: Mcx<'mcx>,
    d: Datum,
) -> PgResult<MultirangeTypeP<'mcx>> {
    // DatumGetPointer(d): the Datum is a uintptr_t pointing at the varlena.
    let var_ptr = d.as_usize() as *const u8;
    // Read VARSIZE_ANY to know how many bytes the (possibly short/extended)
    // varlena occupies, then detoast that slice.
    let total_len = unsafe { varsize_any(var_ptr) };
    let var_bytes: &[u8] = unsafe { core::slice::from_raw_parts(var_ptr, total_len) };

    let detoasted = pg_detoast_datum(mcx, var_bytes)?;
    // Keep the detoasted buffer alive for 'mcx (it lives in mcx; forgetting the
    // owning handle leaves the bytes charged to the context, freed at reset --
    // matching C's "the pointer lives in the current context").
    let ptr = detoasted.as_ptr() as *const MultirangeType;
    core::mem::forget(detoasted);

    Ok(MultirangeTypeP {
        ptr,
        _marker: core::marker::PhantomData,
    })
}

/// Seam `multirange_is_empty` — `MultirangeIsEmpty(DatumGetMultirangeTypeP(attval))`
/// (execIndexing.c's `ExecWithoutOverlapsNotEmpty`): detoast the by-reference
/// multirange value and report whether it has zero member ranges.
pub fn multirange_is_empty_seam<'mcx>(mcx: Mcx<'mcx>, attval: Datum) -> PgResult<bool> {
    let mr = datum_get_multirange_type_p(mcx, attval)?;
    Ok(mr.range_count() == 0)
}

/// `VARSIZE_ANY(PTR)` (varatt.h): total length of a varlena regardless of header
/// kind. Mirrors the `VARATT_IS_*` dispatch the toast macros expand to.
///
/// # Safety
/// `ptr` must point at a valid varlena header.
unsafe fn varsize_any(ptr: *const u8) -> usize {
    let b0 = *ptr;
    if b0 & 0x01 != 0 {
        // 1-byte header.
        if b0 == 0x01 {
            // VARATT_IS_1B_E: external TOAST pointer (1 byte header + tag).
            // VARSIZE_EXTERNAL = VARHDRSZ_EXTERNAL + VARTAG_SIZE(tag).
            let tag = *ptr.add(1);
            // VARHDRSZ_EXTERNAL == 2; VARTAG_SIZE depends on tag.
            2 + vartag_size(tag)
        } else {
            // VARATT_IS_1B: short inline header, length in high 7 bits.
            ((b0 >> 1) & 0x7F) as usize
        }
    } else {
        // 4-byte header: VARSIZE_4B reads the native-order length word and masks
        // off the two kind/tag bits (their position differs by endianness, as in
        // `types_datum::varlena`).
        let w = (ptr as *const u32).read_unaligned();
        #[cfg(target_endian = "big")]
        let len = w & 0x3FFF_FFFF;
        #[cfg(target_endian = "little")]
        let len = (w >> 2) & 0x3FFF_FFFF;
        len as usize
    }
}

/// `VARTAG_SIZE(tag)` (varatt.h): size of an external TOAST pointer datum body
/// for the given vartag. Pure arithmetic over the tag byte -- no pointer access.
fn vartag_size(tag: u8) -> usize {
    // VARTAG_INDIRECT = 1 -> sizeof(varatt_indirect); VARTAG_EXPANDED_RO/RW =
    // 2/3 -> sizeof(varatt_expanded); VARTAG_ONDISK = 18 -> sizeof(varatt_external).
    match tag {
        1 => core::mem::size_of::<usize>(),                 // varatt_indirect (one pointer)
        2 | 3 => core::mem::size_of::<usize>() + 4,          // varatt_expanded (pointer + int)
        18 => 4 + 4 + 4 + 4,                                 // varatt_external (rawsize, extsize, valueid, toastrelid)
        _ => 4 + 4 + 4 + 4,
    }
}

/// `multirange_in(PG_FUNCTION_ARGS)` (multirangetypes.c:117): parse a text
/// multirange literal into a serialized multirange.
///
/// The scaffold drops `escontext` (the C `fcinfo->context`); a soft (`escontext`)
/// error therefore surfaces as a hard `Err`, matching the `escontext == NULL`
/// behavior of the SQL-callable entry point.
pub fn multirange_in<'mcx>(
    mcx: Mcx<'mcx>,
    input: &str,
    mltrngtypoid: Oid,
    typmod: i32,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let input_bytes = input.as_bytes();

    let cache = get_multirange_io_data(mltrngtypoid, IOFuncSelector::Input)?;
    let rangetyp = cache
        .typcache
        .rngtype
        .as_deref()
        .expect("get_multirange_io_data guarantees rngtype");

    let mut ranges: Vec<types_rangetypes::RangeTypeP<'mcx>> = Vec::with_capacity(8);
    let mut ranges_seen: i32 = 0;

    let malformed = |detail: &str| {
        PgError::error(format!("malformed multirange literal: \"{input}\""))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .with_detail(detail.to_string())
    };

    let mut ptr: usize = 0;

    // consume whitespace
    while at(input_bytes, ptr) != 0 && is_space(at(input_bytes, ptr)) {
        ptr += 1;
    }

    if at(input_bytes, ptr) == b'{' {
        ptr += 1;
    } else {
        return Err(malformed("Missing left brace."));
    }

    // consume ranges
    let mut parse_state = MultirangeParseState::BeforeRange;
    let mut range_str_begin: usize = 0;
    while parse_state != MultirangeParseState::Finished {
        let ch = at(input_bytes, ptr);

        if ch == 0 {
            return Err(malformed("Unexpected end of input."));
        }

        // skip whitespace
        if is_space(ch) {
            ptr += 1;
            continue;
        }

        match parse_state {
            MultirangeParseState::BeforeRange => {
                if ch == b'[' || ch == b'(' {
                    range_str_begin = ptr;
                    parse_state = MultirangeParseState::InRange;
                } else if ch == b'}' && ranges_seen == 0 {
                    parse_state = MultirangeParseState::Finished;
                } else if strncasecmp_empty(&input_bytes[ptr..]) {
                    ranges_seen += 1;
                    // nothing to do with an empty range
                    ptr += RANGE_EMPTY_LITERAL.len() - 1;
                    parse_state = MultirangeParseState::AfterRange;
                } else {
                    return Err(malformed("Expected range start."));
                }
            }
            MultirangeParseState::InRange => {
                if ch == b']' || ch == b')' {
                    let range_str_len = ptr - range_str_begin + 1;
                    let range_str = core::str::from_utf8(
                        &input_bytes[range_str_begin..range_str_begin + range_str_len],
                    )
                    .map_err(|_| malformed("Invalid UTF-8 in range."))?;
                    ranges_seen += 1;
                    // InputFunctionCallSafe(&cache->typioproc, range_str, ...)
                    let range = match rangetypes_seams::range_in::call(
                        mcx,
                        range_str,
                        cache.typioparam,
                        typmod,
                    )? {
                        Some(r) => r,
                        // soft error: C would PG_RETURN_NULL(); the scaffold
                        // has no NULL channel, so re-raise as a hard error.
                        None => {
                            return Err(malformed("Invalid range literal."));
                        }
                    };
                    // if (!RangeIsEmpty(range)) ranges[range_count++] = range;
                    let flags = rangetypes_seams::range_get_flags::call(range);
                    if flags & RANGE_EMPTY == 0 {
                        ranges.push(range);
                    }
                    parse_state = MultirangeParseState::AfterRange;
                } else if ch == b'"' {
                    parse_state = MultirangeParseState::InRangeQuoted;
                } else if ch == b'\\' {
                    parse_state = MultirangeParseState::InRangeEscaped;
                }
            }
            MultirangeParseState::InRangeEscaped => {
                parse_state = MultirangeParseState::InRange;
            }
            MultirangeParseState::InRangeQuoted => {
                if ch == b'"' {
                    if at(input_bytes, ptr + 1) == b'"' {
                        // two quote marks means an escaped quote mark
                        ptr += 1;
                    } else {
                        parse_state = MultirangeParseState::InRange;
                    }
                } else if ch == b'\\' {
                    parse_state = MultirangeParseState::InRangeQuotedEscaped;
                }
            }
            MultirangeParseState::AfterRange => {
                if ch == b',' {
                    parse_state = MultirangeParseState::BeforeRange;
                } else if ch == b'}' {
                    parse_state = MultirangeParseState::Finished;
                } else {
                    return Err(malformed("Expected comma or end of multirange."));
                }
            }
            MultirangeParseState::InRangeQuotedEscaped => {
                parse_state = MultirangeParseState::InRangeQuoted;
            }
            MultirangeParseState::Finished => unreachable!(),
        }

        ptr += 1;
    }

    // consume whitespace
    while at(input_bytes, ptr) != 0 && is_space(at(input_bytes, ptr)) {
        ptr += 1;
    }

    if at(input_bytes, ptr) != 0 {
        return Err(malformed("Junk after closing right brace."));
    }

    make_multirange(mcx, mltrngtypoid, rangetyp, &ranges)
}

/// `pg_strncasecmp(ptr, RANGE_EMPTY_LITERAL, strlen(RANGE_EMPTY_LITERAL)) == 0`
/// — does the byte slice begin (case-insensitively) with `"empty"`.
fn strncasecmp_empty(bytes: &[u8]) -> bool {
    let lit = RANGE_EMPTY_LITERAL.as_bytes();
    if bytes.len() < lit.len() {
        return false;
    }
    bytes[..lit.len()]
        .iter()
        .zip(lit.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// `multirange_out(PG_FUNCTION_ARGS)` (multirangetypes.c:299): render a
/// multirange as its text representation.
pub fn multirange_out(mcx: Mcx<'_>, multirange: Datum) -> PgResult<String> {
    let mr = datum_get_multirange_type_p(mcx, multirange)?;
    // mltrngtypoid = MultirangeTypeGetOid(multirange);
    let mltrngtypoid = mr.multirangetypid();

    let cache = get_multirange_io_data(mltrngtypoid, IOFuncSelector::Output)?;
    let rngtype = cache
        .typcache
        .rngtype
        .as_deref()
        .expect("get_multirange_io_data guarantees rngtype");

    let mut buf = String::new();
    buf.push('{');

    // multirange_deserialize(cache->typcache->rngtype, multirange, &range_count, &ranges);
    let ranges = multirange_deserialize(mcx, rngtype, mr)?;
    for (i, range) in ranges.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        // rangeStr = OutputFunctionCall(&cache->typioproc, RangeTypePGetDatum(range));
        let range_str = rangetypes_seams::range_out::call(*range)?;
        buf.push_str(&range_str);
    }

    buf.push('}');
    let _ = &cache.typioproc; // typioproc identifies range_out (range ADT's generic output proc).
    Ok(buf)
}

/// `multirange_recv(PG_FUNCTION_ARGS)` (multirangetypes.c:337): decode a
/// multirange from its binary wire representation.
///
/// Binary representation: an int32 count of ranges, then each range as an
/// int32 length followed by that many bytes of the range's native binary form.
pub fn multirange_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut &[u8],
    mltrngtypoid: Oid,
    typmod: i32,
) -> PgResult<MultirangeTypeP<'mcx>> {
    let cache = get_multirange_io_data(mltrngtypoid, IOFuncSelector::Receive)?;
    let rngtype = cache
        .typcache
        .rngtype
        .as_deref()
        .expect("get_multirange_io_data guarantees rngtype");

    // range_count = pq_getmsgint(buf, 4);
    let range_count = pq_getmsgint32(buf)?;
    let mut ranges: Vec<types_rangetypes::RangeTypeP<'mcx>> =
        Vec::with_capacity(range_count as usize);

    for _ in 0..range_count {
        // uint32 range_len = pq_getmsgint(buf, 4);
        let range_len = pq_getmsgint32(buf)? as usize;
        // const char *range_data = pq_getmsgbytes(buf, range_len);
        let range_data = pq_getmsgbytes(buf, range_len)?;

        // ranges[i] = DatumGetRangeTypeP(ReceiveFunctionCall(&cache->typioproc,
        //                                &tmpbuf, cache->typioparam, typmod));
        let range = rangetypes_seams::range_recv::call(mcx, range_data, cache.typioparam, typmod)?;
        ranges.push(range);
    }

    // pq_getmsgend(buf);
    pq_getmsgend(buf)?;

    let _ = &cache.typioproc; // typioproc identifies range_recv (range ADT's generic receive proc).
    make_multirange(mcx, mltrngtypoid, rngtype, &ranges)
}

/// `multirange_send(PG_FUNCTION_ARGS)` (multirangetypes.c:377): encode a
/// multirange into its binary wire representation.
pub fn multirange_send(mcx: Mcx<'_>, multirange: Datum) -> PgResult<Vec<u8>> {
    let mr = datum_get_multirange_type_p(mcx, multirange)?;
    // mltrngtypoid = MultirangeTypeGetOid(multirange);
    let mltrngtypoid = mr.multirangetypid();
    let range_count_hdr = mr.range_count();

    let cache = get_multirange_io_data(mltrngtypoid, IOFuncSelector::Send)?;
    let rngtype = cache
        .typcache
        .rngtype
        .as_deref()
        .expect("get_multirange_io_data guarantees rngtype");

    let mut buf: Vec<u8> = Vec::new();

    // pq_begintypsend(buf): begin with an empty body (no length prefix here --
    // the varlena header is added by pq_endtypsend, which the boundary applies).
    // pq_sendint32(buf, multirange->rangeCount);
    buf.extend_from_slice(&range_count_hdr.to_be_bytes());

    // multirange_deserialize(cache->typcache->rngtype, multirange, &range_count, &ranges);
    let ranges = multirange_deserialize(mcx, rngtype, mr)?;
    for range in &ranges {
        // range = PointerGetDatum(SendFunctionCall(&cache->typioproc, range));
        // pq_sendint32(buf, VARSIZE(range) - VARHDRSZ);
        // pq_sendbytes(buf, VARDATA(range), VARSIZE(range) - VARHDRSZ);
        let payload = rangetypes_seams::range_send::call(*range)?;
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
    }

    let _ = &cache.typioproc; // typioproc identifies range_send (range ADT's generic send proc).
    Ok(buf)
}

// ---------------------------------------------------------------------------
// pqformat-style binary cursor helpers (pqformat.c), over the scaffold's
// `&mut &[u8]` receive cursor. The wire integers are network byte order
// (big-endian), mirroring `pq_getmsgint`.
// ---------------------------------------------------------------------------

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
