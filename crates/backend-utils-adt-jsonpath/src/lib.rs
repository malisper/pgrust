//! Idiomatic port of PostgreSQL's `jsonpath.c` — the `jsonpath` type
//! input/output, the on-disk flatten/unflatten of the parsed expression, the
//! textual printer, and the `JsonPathItem` reader API.
//!
//! Mirrors `postgres-18.3/src/backend/utils/adt/jsonpath.c` function for
//! function (same names, same branch order, same message text and SQLSTATE).
//!
//! # Buffer model
//!
//! `jsonpath.c` builds its flattened/printed result into a `StringInfo`
//! allocated in the current memory context. The faithful analog here is a
//! [`mcx::PgVec`]`<u8>` (the context-charged byte spine == `StringInfoData.data`)
//! whose every growth is fallible (`try_reserve`, guarded against
//! [`MAX_ALLOC_SIZE`]) so OOM / over-`MaxAllocSize` surfaces as a recoverable
//! [`PgError`] rather than aborting. The flatten output is raw binary (numeric
//! varlenas, NUL terminators, `int32` links) so it is `PgVec<u8>`; the printer
//! output is jsonpath text and is also accumulated as bytes.
//!
//! # External operations
//!
//!  * `parsejsonpath` — the bison/flex grammar+scanner
//!    (`jsonpath_gram.y` / `jsonpath_scan.l`), seamed
//!    (`backend-utils-adt-jsonpath-gram-seams`), unported;
//!  * `escape_json_with_len` — the JSON string-literal escaper (`json.c`),
//!    reached through `backend-utils-adt-json-seams`;
//!  * `numeric_out` — the canonical decimal text of an on-disk `Numeric`
//!    (`numeric.c`, direct dep);
//!  * `datetime_format_has_tz` — the template-has-timezone check
//!    (`formatting.c`, direct dep);
//!  * `exprType` — the expression-node type lookup (`nodeFuncs.c`), reached
//!    through `backend-nodes-nodeFuncs-seams`.
//!
//! # Deferrals
//!
//! * **fmgr / wire-protocol envelope (systemic project deferral):**
//!   `jsonpath_recv` / `jsonpath_send` are `Datum fn(PG_FUNCTION_ARGS)` wrappers
//!   whose only non-core work is the libpq binary framing. The framing is
//!   deferred; the *core* each performs is implemented here on plain byte
//!   inputs/outputs.
//! * **`check_stack_depth` / `CHECK_FOR_INTERRUPTS`:** observability-only
//!   guards in sibling subsystems; they do not affect the produced bytes and
//!   the recursion is bounded by the parse tree / on-disk node tree, so they
//!   are omitted here rather than fabricated.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgString, PgVec, MAX_ALLOC_SIZE};
use types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use types_error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_SYNTAX_ERROR,
};
use types_fmgr::ExternalFnExpr;
use types_tuple::{DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID};

pub use types_jsonpath::jsonpath::{
    jsp_is_scalar, JsonPathItemType, JSONPATH_HDRSZ, JSONPATH_LAX, JSONPATH_VERSION,
    JSP_REGEX_DOTALL, JSP_REGEX_ICASE, JSP_REGEX_MLINE, JSP_REGEX_QUOTE, JSP_REGEX_WSPACE,
};
pub use types_jsonpath::parse::{
    JsonPathNumeric, JsonPathParseItem, JsonPathParseResult, JsonPathParseValue, JsonPathSubscript,
    JsonPathVariable,
};

use JsonPathItemType::*;

mod fmgr_builtins;
pub use fmgr_builtins::register_jsonpath_builtins;

mod seams;
pub use seams::init_seams;

#[cfg(test)]
mod tests;

/// `VARHDRSZ`, the varlena length-header size in bytes.
const VARHDRSZ: usize = 4;

/// `PG_UINT32_MAX`.
const PG_UINT32_MAX: u32 = u32::MAX;

/// `INTALIGN(LEN)` == `TYPEALIGN(ALIGNOF_INT, LEN)`, with `ALIGNOF_INT == 4`.
#[inline]
fn intalign(len: usize) -> usize {
    (len + 3) & !3usize
}

// ===========================================================================
// Append helpers — the idiomatic stand-in for `StringInfo` over PgVec<u8>
// ===========================================================================

/// Over-limit error, mirroring `enlargeStringInfo`'s failed `!AllocSizeIsValid`
/// check (`palloc`'s "invalid memory alloc request size"). Recoverable.
#[inline]
fn out_of_memory() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Over-limit guard mirroring `enlargeStringInfo`'s `AllocSizeIsValid` (1 GiB).
#[inline]
fn check_limit(buf: &PgVec<'_, u8>, additional: usize) -> PgResult<()> {
    let needed = buf.len().saturating_add(additional);
    if needed > MAX_ALLOC_SIZE {
        return Err(out_of_memory());
    }
    Ok(())
}

/// `enlargeStringInfo(buf, needed)` — reserve room for `needed` more bytes.
#[inline]
fn buf_enlarge(buf: &mut PgVec<'_, u8>, needed: usize) -> PgResult<()> {
    check_limit(buf, needed)?;
    let mcx = *buf.allocator();
    buf.try_reserve(needed).map_err(|_| mcx.oom(needed))
}

/// `appendStringInfoChar(buf, ch)`.
#[inline]
fn buf_push(buf: &mut PgVec<'_, u8>, ch: u8) -> PgResult<()> {
    check_limit(buf, 1)?;
    let mcx = *buf.allocator();
    buf.try_reserve(1).map_err(|_| mcx.oom(1))?;
    buf.push(ch);
    Ok(())
}

/// `appendBinaryStringInfo(buf, bytes, len)`.
#[inline]
fn buf_binary(buf: &mut PgVec<'_, u8>, bytes: &[u8]) -> PgResult<()> {
    check_limit(buf, bytes.len())?;
    let mcx = *buf.allocator();
    buf.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    buf.extend_from_slice(bytes);
    Ok(())
}

/// `appendStringInfoString(buf, s)`.
#[inline]
fn buf_str(buf: &mut PgVec<'_, u8>, s: &str) -> PgResult<()> {
    buf_binary(buf, s.as_bytes())
}

/// `appendStringInfoSpaces(buf, count)` for the on-disk reservations. The
/// reserved bytes are subsequently overwritten via `patch_i32` / the header
/// store, so the fill value never reaches disk; we zero-fill to match the
/// on-disk reservation semantics.
#[inline]
fn buf_zeros(buf: &mut PgVec<'_, u8>, count: usize) -> PgResult<()> {
    check_limit(buf, count)?;
    let mcx = *buf.allocator();
    buf.try_reserve(count).map_err(|_| mcx.oom(count))?;
    buf.resize(buf.len() + count, 0);
    Ok(())
}

/// Write an `int32` value into the already-reserved slot at byte `at`.
/// C: `*(int32 *) (buf->data + at) = value;`
#[inline]
fn patch_i32(buf: &mut PgVec<'_, u8>, at: usize, value: i32) {
    buf[at..at + 4].copy_from_slice(&value.to_ne_bytes());
}

// ===========================================================================
// INPUT/OUTPUT
// ===========================================================================

/// C: `jsonpath_in(PG_FUNCTION_ARGS)` — parse text into an on-disk jsonpath
/// varlena byte buffer. `escontext` carries soft errors exactly as the SQL entry
/// point's `fcinfo->context` does. Returns `None` when a soft error was recorded
/// in `escontext` (C's `(Datum) 0` returns).
pub fn jsonpath_in<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let len = input.len() as i32;
    jsonPathFromCstring(mcx, input, len, escontext)
}

/// C: `jsonpath_out(PG_FUNCTION_ARGS)` — render an on-disk jsonpath to text.
pub fn jsonpath_out<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // `jsonpath_out` expects the FULL on-disk image (`[VARHDRSZ][header][nodes]`),
    // reading the version/flags word at `input[4..8]`. The two JSON_TABLE
    // path-`Const` shapes in this tree differ: the row-pattern path Const
    // (makeJsonTablePathScan) stores a single-header image, while a column
    // path-spec Const const-folded through the text->jsonpath cast (fmgr
    // `jsonpath_in` re-framed by `ret_varlena`) carries an extra leading VARHDRSZ
    // word (`[VARHDRSZ-outer][VARHDRSZ-inner][header][nodes]`). Detect the latter
    // by checking whether the word at `[4..8]` is a valid jsonpath header
    // (version, optionally `| LAX`); if not, the real image starts one VARHDRSZ
    // word in. This keeps deparse correct for both Const shapes without
    // perturbing the executor, which consumes each shape through its own path.
    let normalized = normalize_jsonpath_for_out(input);
    let estimated_len = varsize(normalized);
    jsonPathToCstring(mcx, normalized, estimated_len)
}

/// See [`jsonpath_out`]: strip a spurious leading VARHDRSZ word from a
/// double-wrapped jsonpath varlena so the header read at `[4..8]` lands on the
/// version/flags word. Returns `input` unchanged when it is already a
/// well-formed single-header image.
fn normalize_jsonpath_for_out(input: &[u8]) -> &[u8] {
    const VARHDRSZ: usize = 4;
    if input.len() >= 8 {
        let hdr = u32::from_ne_bytes([input[4], input[5], input[6], input[7]]);
        if hdr & !JSONPATH_LAX == JSONPATH_VERSION {
            return input;
        }
        // Header at [4..8] is not a valid jsonpath version: this is the
        // double-wrapped column path-spec Const. The genuine image begins after
        // the outer VARHDRSZ word.
        if input.len() >= VARHDRSZ + 8 {
            let inner = &input[VARHDRSZ..];
            let inner_hdr =
                u32::from_ne_bytes([inner[4], inner[5], inner[6], inner[7]]);
            if inner_hdr & !JSONPATH_LAX == JSONPATH_VERSION {
                return inner;
            }
        }
    }
    input
}

/// Core of C: `jsonpath_recv(PG_FUNCTION_ARGS)`.
///
/// The libpq binary framing (the version byte + remaining text) is the deferred
/// fmgr/wire-protocol envelope; this core takes the already-decoded `version`
/// byte and the decoded text, performs the version check, and dispatches to
/// [`jsonPathFromCstring`] exactly as the C does.
pub fn jsonpath_recv<'mcx>(
    mcx: Mcx<'mcx>,
    version: i32,
    text: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if version != JSONPATH_VERSION as i32 {
        return Err(PgError::error(format!(
            "unsupported jsonpath version number: {version}"
        )));
    }
    let nbytes = text.len() as i32;
    jsonPathFromCstring(mcx, text, nbytes, None)
}

/// Core of C: `jsonpath_send(PG_FUNCTION_ARGS)`.
///
/// The libpq binary framing is the deferred fmgr/wire-protocol envelope; this
/// core returns the two payload components — the protocol version byte and the
/// rendered jsonpath text — ready for a future fmgr layer to frame as a `bytea`.
pub fn jsonpath_send<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<(u8, PgVec<'mcx, u8>)> {
    let version = JSONPATH_VERSION as u8;
    let jtext = jsonPathToCstring(mcx, input, varsize(input))?;
    Ok((version, jtext))
}

/// `VARSIZE(in)` for a varlena byte slice — the full datum length.
#[inline]
fn varsize(input: &[u8]) -> usize {
    input.len()
}

/// `(JsonPath *)->header` — read the version/flags word from a flattened
/// jsonpath varlena.
#[inline]
fn jsonpath_header(input: &[u8]) -> u32 {
    u32::from_ne_bytes([input[4], input[5], input[6], input[7]])
}

/// `js->data` — the flattened-node region of a jsonpath varlena (after the
/// 8-byte `JSONPATH_HDRSZ`).
#[inline]
fn jsonpath_data(input: &[u8]) -> &[u8] {
    &input[JSONPATH_HDRSZ..]
}

/// C: `(jp->header & JSONPATH_LAX) != 0` — whether the flattened jsonpath
/// varlena `input` was parsed in lax (default) mode.
#[inline]
pub fn jsonpath_is_lax(input: &[u8]) -> bool {
    jsonpath_header(input) & JSONPATH_LAX != 0
}

/// C: `jsonPathFromCstring(char *in, int len, struct Node *escontext)`.
///
/// Uses the jsonpath parser (seamed) to turn the string into an AST, then
/// [`flattenJsonPathParseItem`] does a second pass turning the AST into the
/// binary representation of jsonpath. Returns `None` on a soft error.
fn jsonPathFromCstring<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    len: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let jsonpath =
        backend_utils_adt_jsonpath_gram_seams::parse::call(input, escontext.as_deref_mut())?;

    if escontext.as_ref().is_some_and(|c| c.error_occurred()) {
        return Ok(None);
    }

    let Some(jsonpath) = jsonpath else {
        return invalid_input_syntax(escontext.as_deref_mut(), input).map(|()| None);
    };

    let mut buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    // C: enlargeStringInfo(&buf, 4 * len) -- estimation.
    buf_enlarge(&mut buf, (4i64 * len as i64).max(0) as usize)?;

    buf_zeros(&mut buf, JSONPATH_HDRSZ)?;

    let Some(expr) = jsonpath.expr.as_deref() else {
        // C dereferences jsonpath->expr unconditionally; a parse result without
        // an expr is treated as invalid input.
        return invalid_input_syntax(escontext.as_deref_mut(), input).map(|()| None);
    };

    if !flattenJsonPathParseItem(&mut buf, None, escontext, expr, 0, false)? {
        return Ok(None);
    }

    // res = (JsonPath *) buf.data; SET_VARSIZE(res, buf.len);
    let total_len = buf.len();
    let mut header = JSONPATH_VERSION;
    if jsonpath.lax {
        header |= JSONPATH_LAX;
    }
    set_varsize(&mut buf, total_len);
    buf[4..8].copy_from_slice(&header.to_ne_bytes());

    Ok(Some(buf))
}

/// C: `SET_VARSIZE(res, buf.len)` — write the 4-byte uncompressed varlena length
/// header (low 30 bits hold the byte length; the repo's `SET_VARSIZE_4B`
/// convention is LE `len << 2`, BE `len & 0x3FFFFFFF`).
#[inline]
fn set_varsize(data: &mut [u8], len: usize) {
    let word: u32 = if cfg!(target_endian = "big") {
        (len as u32) & 0x3FFF_FFFF
    } else {
        (len as u32) << 2
    };
    data[..VARHDRSZ].copy_from_slice(&word.to_ne_bytes());
}

/// C: `ereturn(escontext, (Datum) 0, errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
/// errmsg("invalid input syntax for type %s: \"%s\"", "jsonpath", in))`.
fn invalid_input_syntax(escontext: Option<&mut SoftErrorContext>, input: &[u8]) -> PgResult<()> {
    let in_str = String::from_utf8_lossy(input);
    ereturn(
        escontext,
        (),
        PgError::error(format!(
            "invalid input syntax for type {}: \"{}\"",
            "jsonpath", in_str
        ))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

/// C: `jsonPathToCstring(StringInfo out, JsonPath *in, int estimated_len)`.
///
/// In the idiomatic surface the result is always returned as owned bytes (C's
/// `out` parameter is the caller's `StringInfo`; here the caller takes the
/// returned bytes).
fn jsonPathToCstring<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    estimated_len: usize,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    buf_enlarge(&mut out, estimated_len)?;

    if jsonpath_header(input) & JSONPATH_LAX == 0 {
        buf_str(&mut out, "strict ")?;
    }

    let v = jspInit(input);
    printJsonPathItem(mcx, &mut out, &v, false, true)?;

    Ok(out)
}

// ===========================================================================
// FLATTEN (AST -> binary)
// ===========================================================================

/// C: `flattenJsonPathParseItem(StringInfo buf, int *result, struct Node
/// *escontext, JsonPathParseItem *item, int nestingLevel, bool
/// insideArraySubscript)`.
///
/// Recursive function converting a jsonpath parse item and all its children into
/// the binary representation. Returns `Ok(false)` on a soft error (when
/// `escontext` is set), otherwise propagates a hard error via `Err`. `result`,
/// when present, receives `pos` for the caller's link patch-up.
pub fn flattenJsonPathParseItem(
    buf: &mut PgVec<'_, u8>,
    result: Option<&mut i32>,
    mut escontext: Option<&mut SoftErrorContext>,
    item: &JsonPathParseItem,
    nesting_level: i32,
    inside_array_subscript: bool,
) -> PgResult<bool> {
    // position from beginning of jsonpath data
    let pos = buf.len() as i32 - JSONPATH_HDRSZ as i32;
    let mut chld: i32 = 0;
    let mut arg_nesting_level = 0i32;

    // C: check_stack_depth(); CHECK_FOR_INTERRUPTS(); -- omitted (see crate
    // docs): observability-only guards living in sibling subsystems.

    buf_push(buf, item.typ as i32 as u8)?;

    // We align buffer to int32 because a series of int32 values often goes after
    // the header, and we want to read them directly (see jspInitByBuffer).
    alignStringInfoInt(buf)?;

    // Reserve space for next item pointer.  Actual value recorded later.
    let next = reserveSpaceForItemPointer(buf)?;

    match item.typ {
        jpiString | jpiVariable | jpiKey => {
            let JsonPathParseValue::String(s) = &item.value else {
                unreachable!("jpiString/Variable/Key carries String value");
            };
            let strlen = s.len() as i32;
            buf_binary(buf, &strlen.to_ne_bytes())?;
            buf_binary(buf, s)?;
            buf_push(buf, b'\0')?;
        }
        jpiNumeric => {
            let JsonPathParseValue::Numeric(num) = &item.value else {
                unreachable!("jpiNumeric carries Numeric value");
            };
            buf_binary(buf, num)?;
        }
        jpiBool => {
            let JsonPathParseValue::Boolean(b) = &item.value else {
                unreachable!("jpiBool carries Boolean value");
            };
            // C: appendBinaryStringInfo(&boolean, sizeof(bool)) -- one byte.
            buf_binary(buf, &[*b as u8])?;
        }
        jpiAnd | jpiOr | jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual
        | jpiGreaterOrEqual | jpiAdd | jpiSub | jpiMul | jpiDiv | jpiMod | jpiStartsWith
        | jpiDecimal => {
            let (left_item, right_item) = match &item.value {
                JsonPathParseValue::Args { left, right } => (left.as_deref(), right.as_deref()),
                _ => unreachable!("binary op carries Args value"),
            };

            // Reserve place for left/right arg positions, then record both args
            // and set their actual positions in the reserved places.
            let left = reserveSpaceForItemPointer(buf)?;
            let right = reserveSpaceForItemPointer(buf)?;

            match left_item {
                None => chld = pos,
                Some(left_item) => {
                    if !flattenJsonPathParseItem(
                        buf,
                        Some(&mut chld),
                        escontext.as_deref_mut(),
                        left_item,
                        nesting_level + arg_nesting_level,
                        inside_array_subscript,
                    )? {
                        return Ok(false);
                    }
                }
            }
            patch_i32(buf, left as usize, chld - pos);

            match right_item {
                None => chld = pos,
                Some(right_item) => {
                    if !flattenJsonPathParseItem(
                        buf,
                        Some(&mut chld),
                        escontext.as_deref_mut(),
                        right_item,
                        nesting_level + arg_nesting_level,
                        inside_array_subscript,
                    )? {
                        return Ok(false);
                    }
                }
            }
            patch_i32(buf, right as usize, chld - pos);
        }
        jpiLikeRegex => {
            let (expr, pattern, flags) = match &item.value {
                JsonPathParseValue::LikeRegex {
                    expr,
                    pattern,
                    flags,
                } => (expr.as_deref(), pattern, *flags),
                _ => unreachable!("jpiLikeRegex carries LikeRegex value"),
            };

            buf_binary(buf, &flags.to_ne_bytes())?;
            let offs = reserveSpaceForItemPointer(buf)?;
            let patternlen = pattern.len() as i32;
            buf_binary(buf, &patternlen.to_ne_bytes())?;
            buf_binary(buf, pattern)?;
            buf_push(buf, b'\0')?;

            // C dereferences item->value.like_regex.expr unconditionally.
            let expr = expr.expect("jpiLikeRegex expr is non-null");
            if !flattenJsonPathParseItem(
                buf,
                Some(&mut chld),
                escontext.as_deref_mut(),
                expr,
                nesting_level,
                inside_array_subscript,
            )? {
                return Ok(false);
            }
            patch_i32(buf, offs as usize, chld - pos);
        }
        jpiFilter | jpiIsUnknown | jpiNot | jpiPlus | jpiMinus | jpiExists | jpiDatetime
        | jpiTime | jpiTimeTz | jpiTimestamp | jpiTimestampTz => {
            if item.typ == jpiFilter {
                arg_nesting_level += 1;
            }

            let arg_item = match &item.value {
                JsonPathParseValue::Arg(a) => a.as_deref(),
                JsonPathParseValue::None => None,
                _ => unreachable!("unary op carries Arg value"),
            };

            let arg = reserveSpaceForItemPointer(buf)?;

            match arg_item {
                None => chld = pos,
                Some(arg_item) => {
                    if !flattenJsonPathParseItem(
                        buf,
                        Some(&mut chld),
                        escontext.as_deref_mut(),
                        arg_item,
                        nesting_level + arg_nesting_level,
                        inside_array_subscript,
                    )? {
                        return Ok(false);
                    }
                }
            }
            patch_i32(buf, arg as usize, chld - pos);
        }
        jpiNull => {}
        jpiRoot => {}
        jpiAnyArray | jpiAnyKey => {}
        jpiCurrent => {
            if nesting_level <= 0 {
                return ereturn(
                    escontext.as_deref_mut(),
                    false,
                    PgError::error("@ is not allowed in root expressions")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                );
            }
        }
        jpiLast => {
            if !inside_array_subscript {
                return ereturn(
                    escontext.as_deref_mut(),
                    false,
                    PgError::error("LAST is allowed only in array subscripts")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                );
            }
        }
        jpiIndexArray => {
            let elems = match &item.value {
                JsonPathParseValue::Array(elems) => elems,
                _ => unreachable!("jpiIndexArray carries Array value"),
            };
            let nelems = elems.len() as i32;

            buf_binary(buf, &nelems.to_ne_bytes())?;

            let offset = buf.len();

            buf_zeros(buf, 4 * 2 * elems.len())?;

            for (i, elem) in elems.iter().enumerate() {
                let mut frompos = 0i32;
                let from = elem.from.as_deref().expect("subscript from is non-null");
                if !flattenJsonPathParseItem(
                    buf,
                    Some(&mut frompos),
                    escontext.as_deref_mut(),
                    from,
                    nesting_level,
                    true,
                )? {
                    return Ok(false);
                }
                frompos -= pos;

                let topos;
                if let Some(to) = elem.to.as_deref() {
                    let mut t = 0i32;
                    if !flattenJsonPathParseItem(
                        buf,
                        Some(&mut t),
                        escontext.as_deref_mut(),
                        to,
                        nesting_level,
                        true,
                    )? {
                        return Ok(false);
                    }
                    topos = t - pos;
                } else {
                    topos = 0;
                }

                let ppos = offset + i * 2 * 4;
                patch_i32(buf, ppos, frompos);
                patch_i32(buf, ppos + 4, topos);
            }
        }
        jpiAny => {
            let (first, last) = match &item.value {
                JsonPathParseValue::AnyBounds { first, last } => (*first, *last),
                _ => unreachable!("jpiAny carries AnyBounds value"),
            };
            buf_binary(buf, &first.to_ne_bytes())?;
            buf_binary(buf, &last.to_ne_bytes())?;
        }
        jpiType | jpiSize | jpiAbs | jpiFloor | jpiCeiling | jpiDouble | jpiKeyValue
        | jpiBigint | jpiBoolean | jpiDate | jpiInteger | jpiNumber | jpiStringFunc => {}
        jpiSubscript => {
            // C: default -> elog(ERROR, "unrecognized jsonpath item type: %d")
            return Err(unrecognized_item_type(item.typ));
        }
    }

    if let Some(next_item) = item.next.as_deref() {
        if !flattenJsonPathParseItem(
            buf,
            Some(&mut chld),
            escontext,
            next_item,
            nesting_level,
            inside_array_subscript,
        )? {
            return Ok(false);
        }
        chld -= pos;
        patch_i32(buf, next as usize, chld);
    }

    if let Some(result) = result {
        *result = pos;
    }
    Ok(true)
}

/// C: `alignStringInfoInt(StringInfo buf)` — align to int by zero padding.
fn alignStringInfoInt(buf: &mut PgVec<'_, u8>) -> PgResult<()> {
    match intalign(buf.len()) - buf.len() {
        3 => {
            buf_push(buf, 0)?;
            buf_push(buf, 0)?;
            buf_push(buf, 0)?;
        }
        2 => {
            buf_push(buf, 0)?;
            buf_push(buf, 0)?;
        }
        1 => {
            buf_push(buf, 0)?;
        }
        _ => {}
    }
    Ok(())
}

/// C: `reserveSpaceForItemPointer(StringInfo buf)` — reserve space for an `int32`
/// JsonPathItem pointer (written as zero now), returning its position.
fn reserveSpaceForItemPointer(buf: &mut PgVec<'_, u8>) -> PgResult<i32> {
    let pos = buf.len() as i32;
    let ptr: i32 = 0;
    buf_binary(buf, &ptr.to_ne_bytes())?;
    Ok(pos)
}

// ===========================================================================
// PRINTER (binary -> text)
// ===========================================================================

/// C: `printJsonPathItem(StringInfo buf, JsonPathItem *v, bool inKey, bool
/// printBracketes)` — print the text representation of a jsonpath item and all
/// its children.
pub fn printJsonPathItem(
    mcx: Mcx<'_>,
    buf: &mut PgVec<'_, u8>,
    v: &JsonPathItem<'_>,
    in_key: bool,
    print_bracketes: bool,
) -> PgResult<()> {
    // C: check_stack_depth(); CHECK_FOR_INTERRUPTS(); -- omitted (see crate
    // docs): observability-only; the recursion is bounded by the on-disk tree.

    match v.typ {
        jpiNull => {
            buf_str(buf, "null")?;
        }
        jpiString => {
            let s = jspGetString(v);
            escape_json_with_len(mcx, buf, s)?;
        }
        jpiNumeric => {
            if jspHasNext(v) {
                buf_push(buf, b'(')?;
            }
            let s = numeric_out(mcx, jspGetNumeric(v))?;
            buf_binary(buf, s.as_bytes())?;
            if jspHasNext(v) {
                buf_push(buf, b')')?;
            }
        }
        jpiBool => {
            if jspGetBool(v) {
                buf_str(buf, "true")?;
            } else {
                buf_str(buf, "false")?;
            }
        }
        jpiAnd | jpiOr | jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual
        | jpiGreaterOrEqual | jpiAdd | jpiSub | jpiMul | jpiDiv | jpiMod | jpiStartsWith => {
            if print_bracketes {
                buf_push(buf, b'(')?;
            }
            let elem = jspGetLeftArg(v);
            printJsonPathItem(
                mcx,
                buf,
                &elem,
                false,
                operationPriority(elem.typ) <= operationPriority(v.typ),
            )?;
            buf_push(buf, b' ')?;
            buf_str(buf, jspOperationName(v.typ)?)?;
            buf_push(buf, b' ')?;
            let elem = jspGetRightArg(v);
            printJsonPathItem(
                mcx,
                buf,
                &elem,
                false,
                operationPriority(elem.typ) <= operationPriority(v.typ),
            )?;
            if print_bracketes {
                buf_push(buf, b')')?;
            }
        }
        jpiNot => {
            buf_str(buf, "!(")?;
            let elem = jspGetArg(v);
            printJsonPathItem(mcx, buf, &elem, false, false)?;
            buf_push(buf, b')')?;
        }
        jpiIsUnknown => {
            buf_push(buf, b'(')?;
            let elem = jspGetArg(v);
            printJsonPathItem(mcx, buf, &elem, false, false)?;
            buf_str(buf, ") is unknown")?;
        }
        jpiPlus | jpiMinus => {
            if print_bracketes {
                buf_push(buf, b'(')?;
            }
            buf_push(buf, if v.typ == jpiPlus { b'+' } else { b'-' })?;
            let elem = jspGetArg(v);
            printJsonPathItem(
                mcx,
                buf,
                &elem,
                false,
                operationPriority(elem.typ) <= operationPriority(v.typ),
            )?;
            if print_bracketes {
                buf_push(buf, b')')?;
            }
        }
        jpiAnyArray => {
            buf_str(buf, "[*]")?;
        }
        jpiAnyKey => {
            if in_key {
                buf_push(buf, b'.')?;
            }
            buf_push(buf, b'*')?;
        }
        jpiIndexArray => {
            buf_push(buf, b'[')?;
            for i in 0..v.content.array.nelems {
                let (from, to) = jspGetArraySubscript(v, i);
                let range = to.is_some();

                if i != 0 {
                    buf_push(buf, b',')?;
                }

                printJsonPathItem(mcx, buf, &from, false, false)?;

                if range {
                    buf_str(buf, " to ")?;
                    printJsonPathItem(
                        mcx,
                        buf,
                        &to.ok_or_else(|| {
                            PgError::error("printJsonPathItem: range 'to' item is NULL")
                        })?,
                        false,
                        false,
                    )?;
                }
            }
            buf_push(buf, b']')?;
        }
        jpiAny => {
            if in_key {
                buf_push(buf, b'.')?;
            }

            let first = v.content.anybounds.first;
            let last = v.content.anybounds.last;
            if first == 0 && last == PG_UINT32_MAX {
                buf_str(buf, "**")?;
            } else if first == last {
                if first == PG_UINT32_MAX {
                    buf_str(buf, "**{last}")?;
                } else {
                    buf_str(buf, &format!("**{{{first}}}"))?;
                }
            } else if first == PG_UINT32_MAX {
                buf_str(buf, &format!("**{{last to {last}}}"))?;
            } else if last == PG_UINT32_MAX {
                buf_str(buf, &format!("**{{{first} to last}}"))?;
            } else {
                buf_str(buf, &format!("**{{{first} to {last}}}"))?;
            }
        }
        jpiKey => {
            if in_key {
                buf_push(buf, b'.')?;
            }
            let s = jspGetString(v);
            escape_json_with_len(mcx, buf, s)?;
        }
        jpiCurrent => {
            debug_assert!(!in_key);
            buf_push(buf, b'@')?;
        }
        jpiRoot => {
            debug_assert!(!in_key);
            buf_push(buf, b'$')?;
        }
        jpiVariable => {
            buf_push(buf, b'$')?;
            let s = jspGetString(v);
            escape_json_with_len(mcx, buf, s)?;
        }
        jpiFilter => {
            buf_str(buf, "?(")?;
            let elem = jspGetArg(v);
            printJsonPathItem(mcx, buf, &elem, false, false)?;
            buf_push(buf, b')')?;
        }
        jpiExists => {
            buf_str(buf, "exists (")?;
            let elem = jspGetArg(v);
            printJsonPathItem(mcx, buf, &elem, false, false)?;
            buf_push(buf, b')')?;
        }
        jpiType => {
            buf_str(buf, ".type()")?;
        }
        jpiSize => {
            buf_str(buf, ".size()")?;
        }
        jpiAbs => {
            buf_str(buf, ".abs()")?;
        }
        jpiFloor => {
            buf_str(buf, ".floor()")?;
        }
        jpiCeiling => {
            buf_str(buf, ".ceiling()")?;
        }
        jpiDouble => {
            buf_str(buf, ".double()")?;
        }
        jpiDatetime => {
            buf_str(buf, ".datetime(")?;
            if v.content.arg != 0 {
                let elem = jspGetArg(v);
                printJsonPathItem(mcx, buf, &elem, false, false)?;
            }
            buf_push(buf, b')')?;
        }
        jpiKeyValue => {
            buf_str(buf, ".keyvalue()")?;
        }
        jpiLast => {
            buf_str(buf, "last")?;
        }
        jpiLikeRegex => {
            if print_bracketes {
                buf_push(buf, b'(')?;
            }

            let elem = v.child_at(v.content.like_regex.expr);
            printJsonPathItem(
                mcx,
                buf,
                &elem,
                false,
                operationPriority(elem.typ) <= operationPriority(v.typ),
            )?;

            buf_str(buf, " like_regex ")?;

            let pattern = like_regex_pattern(v);
            escape_json_with_len(mcx, buf, pattern)?;

            let flags = v.content.like_regex.flags;
            if flags != 0 {
                buf_str(buf, " flag \"")?;

                if flags & JSP_REGEX_ICASE != 0 {
                    buf_push(buf, b'i')?;
                }
                if flags & JSP_REGEX_DOTALL != 0 {
                    buf_push(buf, b's')?;
                }
                if flags & JSP_REGEX_MLINE != 0 {
                    buf_push(buf, b'm')?;
                }
                if flags & JSP_REGEX_WSPACE != 0 {
                    buf_push(buf, b'x')?;
                }
                if flags & JSP_REGEX_QUOTE != 0 {
                    buf_push(buf, b'q')?;
                }

                buf_push(buf, b'"')?;
            }

            if print_bracketes {
                buf_push(buf, b')')?;
            }
        }
        jpiBigint => {
            buf_str(buf, ".bigint()")?;
        }
        jpiBoolean => {
            buf_str(buf, ".boolean()")?;
        }
        jpiDate => {
            buf_str(buf, ".date()")?;
        }
        jpiDecimal => {
            buf_str(buf, ".decimal(")?;
            if v.content.args.left != 0 {
                let elem = jspGetLeftArg(v);
                printJsonPathItem(mcx, buf, &elem, false, false)?;
            }
            if v.content.args.right != 0 {
                buf_push(buf, b',')?;
                let elem = jspGetRightArg(v);
                printJsonPathItem(mcx, buf, &elem, false, false)?;
            }
            buf_push(buf, b')')?;
        }
        jpiInteger => {
            buf_str(buf, ".integer()")?;
        }
        jpiNumber => {
            buf_str(buf, ".number()")?;
        }
        jpiStringFunc => {
            buf_str(buf, ".string()")?;
        }
        jpiTime => {
            buf_str(buf, ".time(")?;
            if v.content.arg != 0 {
                let elem = jspGetArg(v);
                printJsonPathItem(mcx, buf, &elem, false, false)?;
            }
            buf_push(buf, b')')?;
        }
        jpiTimeTz => {
            buf_str(buf, ".time_tz(")?;
            if v.content.arg != 0 {
                let elem = jspGetArg(v);
                printJsonPathItem(mcx, buf, &elem, false, false)?;
            }
            buf_push(buf, b')')?;
        }
        jpiTimestamp => {
            buf_str(buf, ".timestamp(")?;
            if v.content.arg != 0 {
                let elem = jspGetArg(v);
                printJsonPathItem(mcx, buf, &elem, false, false)?;
            }
            buf_push(buf, b')')?;
        }
        jpiTimestampTz => {
            buf_str(buf, ".timestamp_tz(")?;
            if v.content.arg != 0 {
                let elem = jspGetArg(v);
                printJsonPathItem(mcx, buf, &elem, false, false)?;
            }
            buf_push(buf, b')')?;
        }
        jpiSubscript => {
            // C: default -> elog(ERROR, "unrecognized jsonpath item type: %d")
            return Err(unrecognized_item_type(v.typ));
        }
    }

    if let Some(elem) = jspGetNext(v) {
        printJsonPathItem(mcx, buf, &elem, true, true)?;
    }
    Ok(())
}

/// Seam-routed `escape_json_with_len(buf, str, str.len())` from `json.c`.
///
/// The seam appends to a `PgString` (the JSON escaper's UTF-8-typed buffer). We
/// escape into a transient scratch `PgString`, then append the produced bytes
/// to the jsonpath output buffer (which is byte-typed).
fn escape_json_with_len(mcx: Mcx<'_>, buf: &mut PgVec<'_, u8>, str: &[u8]) -> PgResult<()> {
    let mut scratch: PgString = PgString::new_in(mcx);
    backend_utils_adt_json_seams::escape_json_with_len::call(&mut scratch, str)?;
    buf_binary(buf, scratch.as_bytes())
}

/// `numeric_out(num)` from `numeric.c` (direct dep).
fn numeric_out(mcx: Mcx<'_>, num: &[u8]) -> PgResult<String> {
    backend_utils_adt_numeric::io::numeric_out(mcx, num)
}

/// C: `jspOperationName(JsonPathItemType type)`.
pub fn jspOperationName(typ: JsonPathItemType) -> PgResult<&'static str> {
    Ok(match typ {
        jpiAnd => "&&",
        jpiOr => "||",
        jpiEqual => "==",
        jpiNotEqual => "!=",
        jpiLess => "<",
        jpiGreater => ">",
        jpiLessOrEqual => "<=",
        jpiGreaterOrEqual => ">=",
        jpiAdd | jpiPlus => "+",
        jpiSub | jpiMinus => "-",
        jpiMul => "*",
        jpiDiv => "/",
        jpiMod => "%",
        jpiType => "type",
        jpiSize => "size",
        jpiAbs => "abs",
        jpiFloor => "floor",
        jpiCeiling => "ceiling",
        jpiDouble => "double",
        jpiDatetime => "datetime",
        jpiKeyValue => "keyvalue",
        jpiStartsWith => "starts with",
        jpiLikeRegex => "like_regex",
        jpiBigint => "bigint",
        jpiBoolean => "boolean",
        jpiDate => "date",
        jpiDecimal => "decimal",
        jpiInteger => "integer",
        jpiNumber => "number",
        jpiStringFunc => "string",
        jpiTime => "time",
        jpiTimeTz => "time_tz",
        jpiTimestamp => "timestamp",
        jpiTimestampTz => "timestamp_tz",
        _ => return Err(unrecognized_item_type(typ)),
    })
}

/// C: `operationPriority(JsonPathItemType op)`.
fn operationPriority(op: JsonPathItemType) -> i32 {
    match op {
        jpiOr => 0,
        jpiAnd => 1,
        jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual | jpiGreaterOrEqual
        | jpiStartsWith => 2,
        jpiAdd | jpiSub => 3,
        jpiMul | jpiDiv | jpiMod => 4,
        jpiPlus | jpiMinus => 5,
        _ => 6,
    }
}

/// `elog(ERROR, "unrecognized jsonpath item type: %d", type)`.
fn unrecognized_item_type(typ: JsonPathItemType) -> PgError {
    PgError::error(format!("unrecognized jsonpath item type: {}", typ as i32))
}

// ===========================================================================
// JsonPathItem reader API
// ===========================================================================

/// Reader handle into a flattened on-disk `JsonPath` value
/// (C: `struct JsonPathItem`). All positions are relative to `base`.
///
/// `base` mirrors the C `char *base` (a pointer into the flattened value at the
/// current node) by carrying the whole flattened byte buffer plus the byte
/// offset of this node within it.
#[derive(Clone, Debug)]
pub struct JsonPathItem<'a> {
    /// Node type (C: `JsonPathItemType type`).
    pub typ: JsonPathItemType,
    /// Position from base to the next node (C: `int32 nextPos`).
    pub nextPos: i32,
    /// The whole flattened buffer (C: the storage `base` points into).
    pub buffer: &'a [u8],
    /// Byte offset of this node within `buffer` (C: `base - js->data`).
    pub base: i32,
    /// The decoded per-type content (C: the `content` union).
    pub content: JsonPathItemContent,
}

/// The per-type content of a [`JsonPathItem`] (C: `JsonPathItem.content`).
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonPathItemContent {
    /// Binary operator operands (C: `content.args`).
    pub args: ContentArgs,
    /// Unary operand (C: `content.arg`).
    pub arg: i32,
    /// `jpiIndexArray` (C: `content.array`).
    pub array: ContentArray,
    /// `jpiAny`: (first, last) levels (C: `content.anybounds`).
    pub anybounds: ContentAnyBounds,
    /// bool/numeric/string/key payload (C: `content.value`).
    pub value: ContentValue,
    /// `jpiLikeRegex` payload (C: `content.like_regex`).
    pub like_regex: ContentLikeRegex,
}

/// C: `content.args` (left/right operand offsets).
#[derive(Clone, Copy, Debug, Default)]
pub struct ContentArgs {
    pub left: i32,
    pub right: i32,
}

/// C: `content.array` (nelems plus the byte offset of the from/to pair array
/// within `buffer`).
#[derive(Clone, Copy, Debug, Default)]
pub struct ContentArray {
    pub nelems: i32,
    /// Byte offset within `buffer` of the `int32 from, int32 to` pairs.
    pub elems_pos: i32,
}

/// C: `content.anybounds`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ContentAnyBounds {
    pub first: u32,
    pub last: u32,
}

/// C: `content.value` (bool/numeric/string/key payload position & length).
#[derive(Clone, Copy, Debug, Default)]
pub struct ContentValue {
    /// Byte offset within `buffer` of the payload data (C: `data`).
    pub data_pos: i32,
    /// Length, filled only for string/key (C: `datalen`).
    pub datalen: i32,
}

/// C: `content.like_regex`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ContentLikeRegex {
    pub expr: i32,
    /// Byte offset within `buffer` of the pattern bytes (C: `pattern`).
    pub pattern_pos: i32,
    pub patternlen: i32,
    pub flags: u32,
}

/// C: `jspHasNext(jsp)` — `(jsp)->nextPos > 0`.
#[inline]
pub fn jspHasNext(v: &JsonPathItem<'_>) -> bool {
    v.nextPos > 0
}

/// Read a `u8` at offset `pos` of `base`; C: `read_byte`.
#[inline]
fn read_byte(base: &[u8], pos: &mut usize) -> u8 {
    let b = base[*pos];
    *pos += 1;
    b
}

/// Read a `u32` at offset `pos` of `base`; C: `read_int32`.
#[inline]
fn read_int32(base: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_ne_bytes([base[*pos], base[*pos + 1], base[*pos + 2], base[*pos + 3]]);
    *pos += 4;
    v
}

impl<'a> JsonPathItem<'a> {
    /// C: `jspInitByBuffer(a, v->base, off)` — read a node whose `off` is
    /// relative to this node's base (as all the `content`/`nextPos` offsets
    /// are). `self.base` is the node's absolute offset within `buffer`, exactly
    /// as C's `v->base` is the node's absolute address.
    #[inline]
    fn child_at(&self, off: i32) -> JsonPathItem<'a> {
        jspInitByBuffer(self.buffer, self.base + off)
    }
}

/// C: `jspInit(JsonPathItem *v, JsonPath *js)` — read the root node.
pub fn jspInit(js: &[u8]) -> JsonPathItem<'_> {
    debug_assert_eq!(
        jsonpath_header(js) & !JSONPATH_LAX,
        JSONPATH_VERSION,
        "jspInit: bad jsonpath header"
    );
    jspInitByBuffer(jsonpath_data(js), 0)
}

/// C: `jspInitByBuffer(JsonPathItem *v, char *base, int32 pos)` — read a node
/// from `base` at `pos` and fill its representation. `base` is the flattened node
/// region (`js->data`).
pub fn jspInitByBuffer(base: &[u8], pos: i32) -> JsonPathItem<'_> {
    let node_base = pos; // C: v->base = base + pos;
    let mut p = pos as usize;

    let typ_byte = read_byte(base, &mut p);
    // pos = INTALIGN((uintptr_t)(base + pos)) - (uintptr_t) base;
    // The data region begins at a 4-aligned address, so this reduces to
    // INTALIGN(pos) relative to the region start.
    p = intalign(p);
    let next_pos = read_int32(base, &mut p) as i32;

    let typ = item_type_from_byte(typ_byte);
    let mut content = JsonPathItemContent::default();

    match typ {
        jpiNull | jpiRoot | jpiCurrent | jpiAnyArray | jpiAnyKey | jpiType | jpiSize | jpiAbs
        | jpiFloor | jpiCeiling | jpiDouble | jpiKeyValue | jpiLast | jpiBigint | jpiBoolean
        | jpiDate | jpiInteger | jpiNumber | jpiStringFunc => {}
        jpiString | jpiKey | jpiVariable => {
            content.value.datalen = read_int32(base, &mut p) as i32;
            content.value.data_pos = p as i32;
        }
        jpiNumeric | jpiBool => {
            content.value.data_pos = p as i32;
        }
        jpiAnd | jpiOr | jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual
        | jpiGreaterOrEqual | jpiAdd | jpiSub | jpiMul | jpiDiv | jpiMod | jpiStartsWith
        | jpiDecimal => {
            content.args.left = read_int32(base, &mut p) as i32;
            content.args.right = read_int32(base, &mut p) as i32;
        }
        jpiNot | jpiIsUnknown | jpiExists | jpiPlus | jpiMinus | jpiFilter | jpiDatetime
        | jpiTime | jpiTimeTz | jpiTimestamp | jpiTimestampTz => {
            content.arg = read_int32(base, &mut p) as i32;
        }
        jpiIndexArray => {
            content.array.nelems = read_int32(base, &mut p) as i32;
            // read_int32_n: elems points at base + p (then advances by 2*nelems int32s).
            content.array.elems_pos = p as i32;
        }
        jpiAny => {
            content.anybounds.first = read_int32(base, &mut p);
            content.anybounds.last = read_int32(base, &mut p);
        }
        jpiLikeRegex => {
            content.like_regex.flags = read_int32(base, &mut p);
            content.like_regex.expr = read_int32(base, &mut p) as i32;
            content.like_regex.patternlen = read_int32(base, &mut p) as i32;
            content.like_regex.pattern_pos = p as i32;
        }
        jpiSubscript => {
            // C: default -> elog(ERROR, ...).  jspInitByBuffer returns void in C,
            // but the bad-type branch is unreachable for well-formed on-disk
            // data; keep a debug assert.
            debug_assert!(false, "unrecognized jsonpath item type: {}", typ as i32);
        }
    }

    JsonPathItem {
        typ,
        nextPos: next_pos,
        buffer: base,
        base: node_base,
        content,
    }
}

/// Map an on-disk node-type byte back to [`JsonPathItemType`].
#[inline]
fn item_type_from_byte(byte: u8) -> JsonPathItemType {
    // The on-disk byte is the enum discriminant, which is contiguous from
    // jpiNull(0)..=jpiTimestampTz(53).
    debug_assert!(byte <= jpiTimestampTz as i32 as u8);
    ALL_ITEM_TYPES[byte as usize]
}

/// All node types in discriminant order (index == discriminant).
const ALL_ITEM_TYPES: [JsonPathItemType; 54] = [
    jpiNull,
    jpiString,
    jpiNumeric,
    jpiBool,
    jpiAnd,
    jpiOr,
    jpiNot,
    jpiIsUnknown,
    jpiEqual,
    jpiNotEqual,
    jpiLess,
    jpiGreater,
    jpiLessOrEqual,
    jpiGreaterOrEqual,
    jpiAdd,
    jpiSub,
    jpiMul,
    jpiDiv,
    jpiMod,
    jpiPlus,
    jpiMinus,
    jpiAnyArray,
    jpiAnyKey,
    jpiIndexArray,
    jpiAny,
    jpiKey,
    jpiCurrent,
    jpiRoot,
    jpiVariable,
    jpiFilter,
    jpiExists,
    jpiType,
    jpiSize,
    jpiAbs,
    jpiFloor,
    jpiCeiling,
    jpiDouble,
    jpiDatetime,
    jpiKeyValue,
    jpiSubscript,
    jpiLast,
    jpiStartsWith,
    jpiLikeRegex,
    jpiBigint,
    jpiBoolean,
    jpiDate,
    jpiDecimal,
    jpiInteger,
    jpiNumber,
    jpiStringFunc,
    jpiTime,
    jpiTimeTz,
    jpiTimestamp,
    jpiTimestampTz,
];

/// C: `jspGetArg(JsonPathItem *v, JsonPathItem *a)`.
pub fn jspGetArg<'a>(v: &JsonPathItem<'a>) -> JsonPathItem<'a> {
    debug_assert!(matches!(
        v.typ,
        jpiNot
            | jpiIsUnknown
            | jpiPlus
            | jpiMinus
            | jpiFilter
            | jpiExists
            | jpiDatetime
            | jpiTime
            | jpiTimeTz
            | jpiTimestamp
            | jpiTimestampTz
    ));
    v.child_at(v.content.arg)
}

/// C: `jspGetNext(JsonPathItem *v, JsonPathItem *a)`.
pub fn jspGetNext<'a>(v: &JsonPathItem<'a>) -> Option<JsonPathItem<'a>> {
    if jspHasNext(v) {
        debug_assert!(matches!(
            v.typ,
            jpiNull
                | jpiString
                | jpiNumeric
                | jpiBool
                | jpiAnd
                | jpiOr
                | jpiNot
                | jpiIsUnknown
                | jpiEqual
                | jpiNotEqual
                | jpiLess
                | jpiGreater
                | jpiLessOrEqual
                | jpiGreaterOrEqual
                | jpiAdd
                | jpiSub
                | jpiMul
                | jpiDiv
                | jpiMod
                | jpiPlus
                | jpiMinus
                | jpiAnyArray
                | jpiAnyKey
                | jpiIndexArray
                | jpiAny
                | jpiKey
                | jpiCurrent
                | jpiRoot
                | jpiVariable
                | jpiFilter
                | jpiExists
                | jpiType
                | jpiSize
                | jpiAbs
                | jpiFloor
                | jpiCeiling
                | jpiDouble
                | jpiDatetime
                | jpiKeyValue
                | jpiLast
                | jpiStartsWith
                | jpiLikeRegex
                | jpiBigint
                | jpiBoolean
                | jpiDate
                | jpiDecimal
                | jpiInteger
                | jpiNumber
                | jpiStringFunc
                | jpiTime
                | jpiTimeTz
                | jpiTimestamp
                | jpiTimestampTz
        ));
        Some(v.child_at(v.nextPos))
    } else {
        None
    }
}

/// C: `jspGetLeftArg(JsonPathItem *v, JsonPathItem *a)`.
pub fn jspGetLeftArg<'a>(v: &JsonPathItem<'a>) -> JsonPathItem<'a> {
    debug_assert!(matches!(
        v.typ,
        jpiAnd
            | jpiOr
            | jpiEqual
            | jpiNotEqual
            | jpiLess
            | jpiGreater
            | jpiLessOrEqual
            | jpiGreaterOrEqual
            | jpiAdd
            | jpiSub
            | jpiMul
            | jpiDiv
            | jpiMod
            | jpiStartsWith
            | jpiDecimal
    ));
    v.child_at(v.content.args.left)
}

/// C: `jspGetRightArg(JsonPathItem *v, JsonPathItem *a)`.
pub fn jspGetRightArg<'a>(v: &JsonPathItem<'a>) -> JsonPathItem<'a> {
    debug_assert!(matches!(
        v.typ,
        jpiAnd
            | jpiOr
            | jpiEqual
            | jpiNotEqual
            | jpiLess
            | jpiGreater
            | jpiLessOrEqual
            | jpiGreaterOrEqual
            | jpiAdd
            | jpiSub
            | jpiMul
            | jpiDiv
            | jpiMod
            | jpiStartsWith
            | jpiDecimal
    ));
    v.child_at(v.content.args.right)
}

/// C: `jspGetBool(JsonPathItem *v)`.
pub fn jspGetBool(v: &JsonPathItem<'_>) -> bool {
    debug_assert_eq!(v.typ, jpiBool);
    v.buffer[v.content.value.data_pos as usize] != 0
}

/// C: `jspGetNumeric(JsonPathItem *v)` — the on-disk `Numeric` payload bytes.
///
/// C returns `(Numeric) v->content.value.data`, a pointer into the flattened
/// buffer at the numeric. Consumers then read its length from the numeric's own
/// `VARSIZE`. In this slice-based port we therefore bound the returned slice to
/// the numeric's own varlena length (the 4-byte uncompressed-varlena header at
/// `data`) rather than returning the unbounded remainder of the flattened path.
pub fn jspGetNumeric<'a>(v: &JsonPathItem<'a>) -> &'a [u8] {
    debug_assert_eq!(v.typ, jpiNumeric);
    let start = v.content.value.data_pos as usize;
    // VARSIZE(num): the on-disk numeric is an uncompressed 4-byte varlena, so the
    // byte length lives in the low 30 bits of the header word. Mirror the repo's
    // SET_VARSIZE_4B convention (LE `len << 2`, BE `len & 0x3FFFFFFF`).
    let raw = u32::from_ne_bytes([
        v.buffer[start],
        v.buffer[start + 1],
        v.buffer[start + 2],
        v.buffer[start + 3],
    ]);
    let vl = if cfg!(target_endian = "big") {
        (raw & 0x3FFF_FFFF) as usize
    } else {
        (raw >> 2) as usize
    };
    &v.buffer[start..start + vl]
}

/// C: `jspGetString(JsonPathItem *v, int32 *len)`.
pub fn jspGetString<'a>(v: &JsonPathItem<'a>) -> &'a [u8] {
    debug_assert!(matches!(v.typ, jpiKey | jpiString | jpiVariable));
    let start = v.content.value.data_pos as usize;
    let len = v.content.value.datalen as usize;
    &v.buffer[start..start + len]
}

/// The `jpiLikeRegex` pattern bytes (C: `v->content.like_regex.pattern` /
/// `patternlen`).
fn like_regex_pattern<'a>(v: &JsonPathItem<'a>) -> &'a [u8] {
    let start = v.content.like_regex.pattern_pos as usize;
    let len = v.content.like_regex.patternlen as usize;
    &v.buffer[start..start + len]
}

/// C: `jspGetArraySubscript(JsonPathItem *v, JsonPathItem *from, JsonPathItem
/// *to, int i)` — returns `(from, Some(to))` when the subscript is a range, else
/// `(from, None)`.
pub fn jspGetArraySubscript<'a>(
    v: &JsonPathItem<'a>,
    i: i32,
) -> (JsonPathItem<'a>, Option<JsonPathItem<'a>>) {
    debug_assert_eq!(v.typ, jpiIndexArray);
    let pair = v.content.array.elems_pos as usize + (i as usize) * 2 * 4;
    let from_off = i32::from_ne_bytes([
        v.buffer[pair],
        v.buffer[pair + 1],
        v.buffer[pair + 2],
        v.buffer[pair + 3],
    ]);
    let to_off = i32::from_ne_bytes([
        v.buffer[pair + 4],
        v.buffer[pair + 5],
        v.buffer[pair + 6],
        v.buffer[pair + 7],
    ]);

    let from = v.child_at(from_off);

    if to_off == 0 {
        return (from, None);
    }

    let to = v.child_at(to_off);
    (from, Some(to))
}

// ===========================================================================
// MUTABILITY
// ===========================================================================

/// SQL/JSON datatype status (C: `enum JsonPathDatatypeStatus`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonPathDatatypeStatus {
    /// null, bool, numeric, string, array, object
    NonDateTime,
    /// unknown datetime type
    UnknownDateTime,
    /// timetz, timestamptz
    DateTimeZoned,
    /// time, timestamp, date
    DateTimeNonZoned,
}

use JsonPathDatatypeStatus::*;

/// Context for [`jspIsMutableWalker`] (C: `struct JsonPathMutableContext`).
struct JsonPathMutableContext<'a> {
    /// list of variable names (C: `List *varnames` of `String` nodes).
    varnames: &'a [Vec<u8>],
    /// list of variable expressions (C: `List *varexprs` of `Node`s). Carried
    /// as the `ExternalFnExpr` node-tag carrier the `exprType` seam consumes.
    varexprs: &'a [ExternalFnExpr],
    /// status of `@` item.
    current: JsonPathDatatypeStatus,
    /// jsonpath is lax or strict.
    lax: bool,
    /// resulting mutability status.
    mutable: bool,
}

/// C: `jspIsMutable(JsonPath *path, List *varnames, List *varexprs)`.
///
/// Whether the jsonpath expression is mutable, for the planner's
/// `contain_mutable_functions()`.
pub fn jspIsMutable(
    path: &[u8],
    varnames: &[Vec<u8>],
    varexprs: &[ExternalFnExpr],
) -> PgResult<bool> {
    let mut cxt = JsonPathMutableContext {
        varnames,
        varexprs,
        current: NonDateTime,
        lax: (jsonpath_header(path) & JSONPATH_LAX) != 0,
        mutable: false,
    };

    let jpi = jspInit(path);
    jspIsMutableWalker(&jpi, &mut cxt)?;

    Ok(cxt.mutable)
}

/// `jsp_is_mutable` inward-seam adapter for `clauses.c`'s
/// `contain_mutable_functions_walker` `JsonExpr` arm (clauses.c:430):
///
/// ```c
/// if (jspIsMutable(DatumGetJsonPathP(cnst->constvalue),
///                  jexpr->passing_names, jexpr->passing_values))
/// ```
///
/// The consumer hands the whole [`JsonExpr`] across the seam (it has the node
/// accessors but not the jsonpath reader); this owner extracts the constant
/// jsonpath image from `path_spec`, marshals the PASSING names/values, and runs
/// [`jspIsMutable`]. The caller has already verified `path_spec` is a non-null
/// `Const` of type `JSONPATHOID`; we re-check defensively and treat any other
/// shape as non-mutable-by-this-path (the C `Assert(cnst->consttype ==
/// JSONPATHOID)` plus the `constisnull` guard).
fn jsp_is_mutable_seam(jsonexpr: &types_nodes::primnodes::Expr) -> PgResult<bool> {
    use types_nodes::primnodes::Expr;

    let Expr::JsonExpr(jexpr) = jsonexpr else {
        return Ok(false);
    };

    // DatumGetJsonPathP(cnst->constvalue): the path_spec must be a non-null
    // jsonpath Const.
    let Some(path_spec) = jexpr.path_spec.as_deref() else {
        return Ok(false);
    };
    let Expr::Const(cnst) = path_spec else {
        return Ok(false);
    };
    if cnst.constisnull {
        return Ok(false);
    }

    // The jsonpath `Const`'s by-ref value carries the full jsonpath varlena
    // behind one leading `VARHDRSZ` word (the canonical pass-by-reference ABI
    // framing); strip it to recover the image the cores slice into
    // (`jsonpath_header` reads `[4..8]`, `jspInit` slices `[8..]`).
    const VARHDRSZ: usize = 4;
    let raw = cnst.constvalue.as_ref_bytes();
    let path: &[u8] = if raw.len() >= VARHDRSZ {
        &raw[VARHDRSZ..]
    } else {
        raw
    };

    // jexpr->passing_names (list of String) -> varnames.
    let varnames: Vec<Vec<u8>> = jexpr
        .passing_names
        .iter()
        .map(|n| n.as_bytes().to_vec())
        .collect();

    // jexpr->passing_values (list of Node) -> varexprs. Each is carried as the
    // field-bearing `ExternalFnExpr` the `exprType` seam downcasts back to read
    // the value's result type (jspIsMutableWalker's `jpiVariable` arm).
    let varexprs: Vec<ExternalFnExpr> = jexpr
        .passing_values
        .iter()
        .map(|e| ExternalFnExpr {
            tag: e.expr_tag().0,
            // Erase the borrowed `Expr<'_>` clone into the `'static` `fn_expr`
            // carrier via the sanctioned from_node_erased boundary (the value is
            // only read back transiently by exprType during the walk).
            node: Some(types_core::fmgr::FnExprErased::from_node_erased::<
                types_nodes::primnodes::Expr<'_>,
                types_nodes::primnodes::Expr<'static>,
            >(e.clone())),
        })
        .collect();

    jspIsMutable(path, &varnames, &varexprs)
}

/// C: `jspIsMutableWalker(JsonPathItem *jpi, struct JsonPathMutableContext
/// *cxt)` — recursive walker for [`jspIsMutable`].
fn jspIsMutableWalker(
    jpi: &JsonPathItem<'_>,
    cxt: &mut JsonPathMutableContext<'_>,
) -> PgResult<JsonPathDatatypeStatus> {
    let mut jpi = jpi.clone();
    let mut status = NonDateTime;

    while !cxt.mutable {
        match jpi.typ {
            jpiRoot => {
                debug_assert_eq!(status, NonDateTime);
            }
            jpiCurrent => {
                debug_assert_eq!(status, NonDateTime);
                status = cxt.current;
            }
            jpiFilter => {
                let prev_status = cxt.current;

                cxt.current = status;
                let arg = jspGetArg(&jpi);
                jspIsMutableWalker(&arg, cxt)?;

                cxt.current = prev_status;
            }
            jpiVariable => {
                let name = jspGetString(&jpi);
                let len = name.len();

                debug_assert_eq!(status, NonDateTime);

                for (varname, varexpr) in cxt.varnames.iter().zip(cxt.varexprs.iter()) {
                    // C: strncmp(varname->sval, name, len) -- compare the first
                    // `len` bytes of the (NUL-terminated) varname against name.
                    if !strncmp_eq(varname, name, len) {
                        continue;
                    }

                    let oid = backend_nodes_nodeFuncs_seams::expr_type::call(varexpr.clone());
                    status = if oid == DATEOID || oid == TIMEOID || oid == TIMESTAMPOID {
                        DateTimeNonZoned
                    } else if oid == TIMETZOID || oid == TIMESTAMPTZOID {
                        DateTimeZoned
                    } else {
                        NonDateTime
                    };

                    break;
                }
            }
            jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual | jpiGreaterOrEqual => {
                debug_assert_eq!(status, NonDateTime);
                let arg = jspGetLeftArg(&jpi);
                let left_status = jspIsMutableWalker(&arg, cxt)?;

                let arg = jspGetRightArg(&jpi);
                let right_status = jspIsMutableWalker(&arg, cxt)?;

                // Comparison of datetime type with different timezone status is
                // mutable.
                if left_status != NonDateTime
                    && right_status != NonDateTime
                    && (left_status == UnknownDateTime
                        || right_status == UnknownDateTime
                        || left_status != right_status)
                {
                    cxt.mutable = true;
                }
            }
            jpiNot | jpiIsUnknown | jpiExists | jpiPlus | jpiMinus => {
                debug_assert_eq!(status, NonDateTime);
                let arg = jspGetArg(&jpi);
                jspIsMutableWalker(&arg, cxt)?;
            }
            jpiAnd | jpiOr | jpiAdd | jpiSub | jpiMul | jpiDiv | jpiMod | jpiStartsWith => {
                debug_assert_eq!(status, NonDateTime);
                let arg = jspGetLeftArg(&jpi);
                jspIsMutableWalker(&arg, cxt)?;
                let arg = jspGetRightArg(&jpi);
                jspIsMutableWalker(&arg, cxt)?;
            }
            jpiIndexArray => {
                for i in 0..jpi.content.array.nelems {
                    let (from, to) = jspGetArraySubscript(&jpi, i);

                    if let Some(to) = to {
                        jspIsMutableWalker(&to, cxt)?;
                    }

                    jspIsMutableWalker(&from, cxt)?;
                }
                // FALLTHROUGH to jpiAnyArray
                if !cxt.lax {
                    status = NonDateTime;
                }
            }
            jpiAnyArray => {
                if !cxt.lax {
                    status = NonDateTime;
                }
            }
            jpiAny => {
                if jpi.content.anybounds.first > 0 {
                    status = NonDateTime;
                }
            }
            jpiDatetime => {
                if jpi.content.arg != 0 {
                    let arg = jspGetArg(&jpi);
                    if arg.typ != jpiString {
                        status = NonDateTime;
                        // there will be runtime error
                    } else {
                        let template = jspGetString(&arg);
                        if datetime_format_has_tz(template)? {
                            status = DateTimeZoned;
                        } else {
                            status = DateTimeNonZoned;
                        }
                    }
                } else {
                    status = UnknownDateTime;
                }
            }
            jpiLikeRegex => {
                debug_assert_eq!(status, NonDateTime);
                let arg = jpi.child_at(jpi.content.like_regex.expr);
                jspIsMutableWalker(&arg, cxt)?;
            }
            // literals
            jpiNull | jpiString | jpiNumeric | jpiBool
            // accessors
            | jpiKey | jpiAnyKey
            // special items
            | jpiSubscript | jpiLast
            // item methods
            | jpiType | jpiSize | jpiAbs | jpiFloor | jpiCeiling | jpiDouble | jpiKeyValue
            | jpiBigint | jpiBoolean | jpiDecimal | jpiInteger | jpiNumber | jpiStringFunc => {
                status = NonDateTime;
            }
            jpiTime | jpiDate | jpiTimestamp => {
                status = DateTimeNonZoned;
                cxt.mutable = true;
            }
            jpiTimeTz | jpiTimestampTz => {
                status = DateTimeNonZoned;
                cxt.mutable = true;
            }
        }

        match jspGetNext(&jpi) {
            Some(next) => jpi = next,
            None => break,
        }
    }

    Ok(status)
}

/// `datetime_format_has_tz(template)` from `formatting.c` (direct dep).
fn datetime_format_has_tz(template: &[u8]) -> PgResult<bool> {
    backend_utils_adt_formatting::datetime_format_has_tz(template)
}

/// C: `strncmp(varname->sval, name, len) == 0` — true when the first `len` bytes
/// of `varname` (NUL-terminated) equal `name[..len]`.
#[inline]
fn strncmp_eq(varname: &[u8], name: &[u8], len: usize) -> bool {
    // C compares up to `len` bytes; a NUL in `varname` short-circuits.
    for i in 0..len {
        let a = varname.get(i).copied().unwrap_or(0);
        let b = name.get(i).copied().unwrap_or(0);
        if a != b {
            return false;
        }
        if a == 0 {
            break;
        }
    }
    true
}

/// C: `jspConvertRegexFlags(uint32 xflags, int *result, struct Node *escontext)`
/// (`jsonpath_gram.y`) — map the on-disk XQuery `JSP_REGEX_*` `like_regex` flag
/// bits to the PostgreSQL `REG_*` cflags consumed by `RE_compile_and_execute`.
///
/// Hard-error form: the `jsonpath_exec.c` call path passes a NULL `escontext`,
/// so the single `ereturn` (the unimplemented XQuery `'x'` flag) surfaces as a
/// plain recoverable `Err` here. Logic/branch order is 1:1 with the C.
pub fn jspConvertRegexFlags(xflags: u32) -> PgResult<i32> {
    use backend_regex_core::regex_consts::{
        REG_ADVANCED, REG_ICASE, REG_NLANCH, REG_NLSTOP, REG_QUOTE,
    };

    // C: "By default, XQuery is very nearly the same as Spencer's AREs".
    let mut cflags: i32 = REG_ADVANCED;

    // C: "Ignore-case means the same thing, too, modulo locale issues".
    if xflags & JSP_REGEX_ICASE != 0 {
        cflags |= REG_ICASE;
    }

    // C: "Per XQuery spec, if 'q' is specified then 'm', 's', 'x' are ignored".
    if xflags & JSP_REGEX_QUOTE != 0 {
        cflags &= !REG_ADVANCED;
        cflags |= REG_QUOTE;
    } else {
        // C: "Note that dotall mode is the default in POSIX".
        if xflags & JSP_REGEX_DOTALL == 0 {
            cflags |= REG_NLSTOP;
        }
        if xflags & JSP_REGEX_MLINE != 0 {
            cflags |= REG_NLANCH;
        }
        // C: XQuery's 'x' mode is not really REG_EXPANDED; treated as
        // unimplemented (the C `ereturn` -> recoverable Err, same message).
        if xflags & JSP_REGEX_WSPACE != 0 {
            return Err(PgError::error(
                "XQuery \"x\" flag (expanded regular expressions) is not implemented",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }

    Ok(cflags)
}
