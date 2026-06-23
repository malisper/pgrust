//! FAMILY: split/join, the Split*String identifier/path parsers, format() /
//! concat(), and string_agg.
//!
//! `split_part`, `text_to_array`/`text_to_array_null`/`text_to_table*`,
//! `array_to_text`/`array_to_text_null`/`array_to_text_internal`,
//! `split_text`/`split_text_accum_result`, `text_isequal`,
//! `textToQualifiedNameList`, `SplitIdentifierString`,
//! `SplitDirectoriesString`, `SplitGUCList`, `appendStringInfoText`,
//! `text_concat`/`text_concat_ws`/`concat_internal`/`build_concat_foutcache`,
//! `text_format`/`text_format_nv` + the four `text_format_*` parse/append
//! helpers, the `makeStringAggState`/`string_agg_*`/`bytea_string_agg_*`
//! aggregate family, and `pg_column_size`/`pg_column_compression`/
//! `pg_column_toast_chunk_id`.
//!
//! Genuinely-external owners reached by seam-and-panic (owners not yet ported;
//! each panic names the owning C subsystem):
//! - the array build/deconstruct subsystem (`utils/adt/arrayfuncs.c`:
//!   `accumArrayResult`/`makeArrayResult`/`construct_empty_array`/
//!   `deconstruct_array`) for the `text_to_array`/`array_to_text`/format
//!   variadic paths,
//! - the tuplestore SRF sink (`utils/sort/tuplestore.c`
//!   `tuplestore_putvalues`) for `text_to_table`,
//! - the fmgr type-output dispatch (`fmgr.c` `OutputFunctionCall` +
//!   `lsyscache.c` `getTypeOutputInfo`/`get_type_io_data`) for
//!   `format()`/`concat()`/`array_to_text`,
//! - the identifier helpers (`parser/scansup.c`
//!   `downcase_truncate_identifier`/`truncate_identifier`) and
//!   `canonicalize_path` (`port/path.c`) for the `Split*String` parsers,
//!   `quote_identifier` (`ruleutils.c`, owner not yet ported) for the `%I`
//!   formatting conversion,
//! - the TOAST owner (`access/common/detoast.c` `toast_datum_size`,
//!   `access/common/toast_compression.c` `toast_get_compression_id`, the
//!   on-disk-external `va_valueid` extraction) for `pg_column_*`.
//!
//! Sibling families this family's split workers lean on: [`crate::comparison`]
//! (`texteq` for `text_isequal`) and the `text_position_*` Boyer-Moore-Horspool
//! state machine owned by [`crate::position_ops`]; the latter is not yet
//! exposed, so the few `split_part`/`split_text` calls into it route through
//! the named seam-and-panic shims below (owner = the `position_ops` family).
//!
//! Depends on the keystone for `cstring_to_text*`/`text_to_cstring` carrier
//! building and the [`SplitTextOutputData`](crate::keystone) marker.

use mcx::{Mcx, PgString, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;

use mbutils_seams as mb;

use crate::keystone::{cstring_to_text_with_len, text_to_cstring};

/// C: `MAXPGPATH` (`pg_config_manual.h`) — `SplitDirectoriesString`'s
/// overlength-path truncation length (varlena.c:3780-3781).
const MAXPGPATH: usize = 1024;

/// C: `INT4OID` (`catalog/pg_type_d.h`) — the `int4` type OID, used by
/// `text_format`'s indirect-width fast path.
const INT4OID: Oid = 23;
/// C: `INT2OID` (`catalog/pg_type_d.h`) — the `int2` type OID.
const INT2OID: Oid = 21;

// ===========================================================================
// Genuinely-external owner seams (owners not yet ported — seam-and-panic).
// Each is named after the real C owner; calling one panics loudly until the
// owner lands, exactly as the project `seam!`-not-installed path would.
// ===========================================================================

/// One `text`/`bytea` array element as returned by the array-deconstruct
/// owner: the element's type-output cstring bytes (`Some`) or a NULL marker
/// (`None`), in array storage order. C: the `deconstruct`/`OutputFunctionCall`
/// walk of `array_to_text_internal` (varlena.c:5130-5178).
pub enum ArrayElement<'mcx> {
    /// A non-null element already run through its type output function.
    Value(PgVec<'mcx, u8>),
    /// A null element.
    Null,
}

/// C: `accumArrayResult` loop + `makeArrayResult`/`construct_empty_array`
/// building a `text[]` Datum from the split fields (arrayfuncs.c). Routes to the
/// merged `backend-utils-adt-arrayfuncs` owner via its installed
/// `build_text_array_nullable` seam: each split field is either a non-null
/// `text` payload or a SQL NULL (the `is_null` flag set when it matched the
/// null-string), preserved per element. An empty field set yields
/// `construct_empty_array(TEXTOID)`.
fn build_text_array<'mcx>(mcx: Mcx<'mcx>, fields: &[SplitField<'mcx>]) -> PgResult<Datum<'mcx>> {
    let elems: PgVec<'mcx, Option<&[u8]>> = {
        let mut v = ::mcx::vec_with_capacity_in(mcx, fields.len())?;
        for f in fields {
            v.push(if f.is_null { None } else { Some(f.bytes.as_slice()) });
        }
        v
    };
    let bytes = arrayfuncs_seams::build_text_array_nullable::call(mcx, &elems)?;
    // C: PG_RETURN_DATUM(makeArrayResult(...)) — the array varlena image rides
    // the canonical by-reference `Datum`.
    Ok(Datum::ByRef(bytes))
}

/// C: `tuplestore_putvalues(tstate->tupstore, tstate->tupdesc, values, nulls)`
/// for one `text_to_table` row (tuplestore.c). Routes to the merged
/// `backend-utils-sort-storage` owner via its installed `tuplestore_putvalues`
/// seam. The single `text` column's value is the field payload (`ByRef` text
/// varlena bytes) or SQL NULL.
fn tuplestore_put_field<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut nodes::Tuplestorestate<'_>,
    tupdesc: &::types_tuple::heaptuple::TupleDescData<'_>,
    field_value: &[u8],
    is_null: bool,
) -> PgResult<()> {
    // C:5008-5014 values[0] = PointerGetDatum(cstring_to_text_with_len(...));
    // nulls[0] = is_null; tuplestore_putvalues(...).
    let value: Datum<'mcx> = if is_null {
        Datum::null()
    } else {
        let text = cstring_to_text_with_len(mcx, field_value, field_value.len() as i32)?;
        Datum::ByRef(text)
    };
    let values = [value];
    let nulls = [is_null];
    sort_storage_seams::tuplestore_putvalues::call(state, tupdesc, &values, &nulls)
}

/// C: the element deconstruction + per-element `OutputFunctionCall` of
/// `array_to_text_internal` (arrayfuncs.c `deconstruct_array` + fmgr output
/// dispatch). Routes to the merged `backend-utils-adt-arrayfuncs` owner via its
/// installed `array_to_text_elements` seam, which detoasts the array, looks up
/// the element type's output function and walks the elements. The detoasted
/// array bytes ride the canonical `Datum`'s `ByRef` payload (C
/// `DatumGetArrayTypeP(v)`).
fn array_to_text_elements<'mcx>(
    mcx: Mcx<'mcx>,
    v: &Datum<'mcx>,
    element_type: Oid,
) -> PgResult<PgVec<'mcx, ArrayElement<'mcx>>> {
    let array = v.as_ref_bytes();
    let raw =
        arrayfuncs_seams::array_to_text_elements::call(mcx, array, element_type)?;
    let mut out = ::mcx::vec_with_capacity_in(mcx, raw.len())?;
    for item in raw {
        out.push(match item {
            Some(bytes) => ArrayElement::Value(bytes),
            None => ArrayElement::Null,
        });
    }
    Ok(out)
}

/// C: `downcase_truncate_identifier(ident, len, false)` (parser/scansup.c).
/// Routes to the merged `backend-parser-small1` owner (scansup family) via its
/// installed `downcase_truncate_identifier` seam.
fn downcase_truncate_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    scansup_seams::downcase_truncate_identifier::call(mcx, ident, false)
}

/// C: `truncate_identifier(ident, strlen(ident), false)` (parser/scansup.c).
/// Routes to the merged `backend-parser-small1` owner (scansup family) via its
/// installed `truncate_identifier` seam.
fn truncate_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    scansup_seams::truncate_identifier::call(mcx, ident, false)
}

/// C: `canonicalize_path(path)` (common/path.c). Routes through the per-owner
/// `common-path-seams::canonicalize_path` seam (loud panic-until-installed: the
/// `common/path.c` owner is not yet ported, so the call faults exactly as the
/// project seam-not-installed path would — and auto-lights when the owner lands,
/// unlike a crate-local panic the recurrence guard can't see). The C input is a
/// `char *` in the database encoding that `canonicalize_path` cleans up in
/// place; the seam crosses the payload as `String` -> `String`.
fn canonicalize_path<'mcx>(mcx: Mcx<'mcx>, path: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let s = core::str::from_utf8(path).map_err(|_| {
        ::types_error::PgError::error("invalid byte sequence for encoding")
            .with_sqlstate(::types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
    })?;
    let canonical = common_path_seams::canonicalize_path::call(s.to_string());
    let bytes = canonical.as_bytes();
    let mut out = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(out)
}

/// C: `OutputFunctionCall(&finfo, value)` after `getTypeOutputInfo(typid)`
/// (fmgr.c / lsyscache.c) — stringify one value through its type output
/// function. Routes to the merged `backend-utils-cache-lsyscache`
/// (`get_type_output_info`) and `backend-utils-fmgr-core`
/// (`OidOutputFunctionCall`) owners via their installed seams.
fn output_function_call<'mcx>(
    mcx: Mcx<'mcx>,
    typid: Oid,
    value: &Datum<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    // C: getTypeOutputInfo(valtype, &typOutput, &typIsVarlena).
    let (typ_output, _typ_is_varlena) =
        lsyscache_seams::get_type_output_info::call(typid)?;
    // C: OutputFunctionCall(&foutcache->finfo, value) — the cache reduces to a
    // by-OID lookup-and-call per the owner seam contract.
    fmgr_seams::oid_output_function_call::call(mcx, typ_output, value)
}

/// C: `quote_identifier(ident)` (ruleutils.c) for the `%I` conversion. Routes
/// through the per-owner `backend-utils-adt-ruleutils-seams::quote_identifier`
/// seam (loud panic-until-installed: the `ruleutils.c` owner is not yet ported,
/// so the call faults exactly as the project seam-not-installed path would, and
/// auto-lights when the owner lands). The C input is a NUL-terminated C string;
/// the seam crosses the content as `&str` -> `PgString`.
fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let s = core::str::from_utf8(ident).map_err(|_| {
        ::types_error::PgError::error("invalid byte sequence for encoding")
            .with_sqlstate(::types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
    })?;
    let quoted = ruleutils_seams::quote_identifier::call(mcx, s)?;
    let bytes = quoted.as_bytes();
    let mut out = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(out)
}

/// C: `quote_literal_cstr(literal)` (quote.c) for the `%L` conversion. Calls the
/// merged `backend-utils-adt-quote` owner via its installed seam. The C input is
/// a NUL-terminated C string in the database encoding and the result is a freshly
/// `palloc`'d quoted literal; the seam contract crosses the content as `&str` ->
/// `String` (see backend-utils-adt-quote-seams), so we marshal the payload bytes
/// to/from a `PgVec` charged to `mcx`. Non-UTF-8 input surfaces as the encoding
/// error the fmgr text<->cstring boundary would otherwise raise.
fn quote_literal_cstr<'mcx>(mcx: Mcx<'mcx>, literal: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let s = core::str::from_utf8(literal).map_err(|_| {
        ::types_error::PgError::error("invalid byte sequence for encoding")
            .with_sqlstate(::types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
    })?;
    let quoted = quote_seams::quote_literal_cstr::call(s);
    let bytes = quoted.as_bytes();
    let mut out = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(out)
}

/// C: `pg_strtoint32(s)` (numutils.c) — parse a stringified indirect-width arg.
/// Calls the merged `backend-utils-adt-numutils` owner directly (a leaf with no
/// seam crate). The C input is a NUL-terminated C string; `pg_strtoint32`
/// ereports on bad data or overflow, surfaced here as the `Err`.
fn pg_strtoint32(s: &[u8]) -> PgResult<i32> {
    let s = core::str::from_utf8(s).map_err(|_| {
        ::types_error::PgError::error("invalid byte sequence for encoding")
            .with_sqlstate(::types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
    })?;
    numutils::pg_strtoint32(s)
}

/// C: `toast_datum_size(value)` (access/common/detoast.c) for `pg_column_size`.
/// Routes to the merged `backend-access-common-detoast` owner via its installed
/// `toast_datum_size` seam. `value` is the varlena attribute (`ByRef` bytes).
fn toast_datum_size(mcx: Mcx<'_>, value: &Datum<'_>) -> PgResult<i32> {
    let size = detoast_seams::toast_datum_size::call(mcx, value.as_ref_bytes())?;
    Ok(size as i32)
}

/// C: `strlen(DatumGetCString(value)) + 1` carrier for the cstring branch of
/// `pg_column_size`; the Datum->cstring deref is the fmgr/Datum boundary's job.
fn cstring_datum_len(_value: &Datum<'_>) -> PgResult<i32> {
    panic!(
        "unported owner: fmgr/Datum boundary DatumGetCString \
         (pg_column_size cstring path) — completed at the varlena Datum boundary"
    )
}

/// C: `toast_get_compression_id(varlena)` (access/common/toast_compression.c).
/// The owner (`backend-access-common-toast-compression`) reads the compression
/// method id off the varlena header (external-on-disk pointer or inline
/// compressed header); a direct cargo dep (no cycle) is used. The owner's id is
/// the C `int` code (`TOAST_{PGLZ,LZ4,INVALID}_COMPRESSION_ID` = 0/1/2), mapped
/// to this crate's [`ToastCompressionId`].
fn toast_get_compression_id(value: &Datum<'_>) -> PgResult<ToastCompressionId> {
    use toast_compression as tc;
    let cmid = tc::toast_get_compression_id(value.as_ref_bytes());
    Ok(if cmid == tc::TOAST_PGLZ_COMPRESSION_ID {
        ToastCompressionId::Pglz
    } else if cmid == tc::TOAST_LZ4_COMPRESSION_ID {
        ToastCompressionId::Lz4
    } else {
        ToastCompressionId::Invalid
    })
}

/// C: `VARATT_IS_EXTERNAL_ONDISK(attr)` test + `VARATT_EXTERNAL_GET_POINTER`'s
/// `va_valueid` extraction (postgres.h / detoast) for
/// `pg_column_toast_chunk_id`. Routes to the merged
/// `backend-access-common-detoast` owner via its installed `toast_chunk_id`
/// seam (`None` when the value is not stored on-disk-external). `value` is the
/// varlena attribute (`ByRef` bytes).
fn toast_chunk_id(value: &Datum<'_>) -> PgResult<Option<Oid>> {
    detoast_seams::toast_chunk_id::call(value.as_ref_bytes())
}

// The text_position_* Boyer-Moore-Horspool state machine is owned by the
// `position_ops` sibling family; `split_part`/`split_text` reach its lower-level
// entry points directly. The state itself is the keystone's `TextPositionState`.
// `text_position_get_match_off` adapts C's `text_position_get_match_ptr`
// (returns a `char *`) to the byte-offset carrier used throughout this crate.
use crate::position_ops::{
    text_position_cleanup, text_position_get_match_ptr as text_position_get_match_off,
    text_position_next, text_position_reset, text_position_setup,
};

// Migration target: the canonical value type (the unified `Datum` enum), not
// the bare-word `datum::Datum` newtype. By-value scalars ride `ByVal`;
// by-reference (varlena/array) images ride `ByRef(PgVec<'mcx, u8>)`. The `_v`
// `&Datum<'mcx>` borrowing convention is used for the value-consuming seam
// stand-ins so the by-ref payload is not needlessly cloned.
use ::types_tuple::heaptuple::Datum;

// ===========================================================================
// Family-local carriers.
// ===========================================================================

/// C: one accumulated split field (the reduced form of the array-build /
/// tuplestore dispatch of `split_text_accum_result`, varlena.c:4987-5019): the
/// field payload bytes plus its computed SQL-NULL flag.
pub struct SplitField<'mcx> {
    /// The field payload (a `text` value's bytes), charged to the working mcx.
    pub bytes: PgVec<'mcx, u8>,
    /// Whether the field matched the null-string and so maps to SQL NULL.
    pub is_null: bool,
}

/// C: the `StringInfo` transition value of `string_agg`/`bytea_string_agg`
/// (varlena.c:5422-5444 `makeStringAggState`). `data` is the running buffer in
/// the aggregate context; `cursor` holds the byte length of the first
/// delimiter, stripped only in the final function (varlena.c:5466-5468).
pub struct StringAggState<'mcx> {
    /// C: `StringInfoData.data` — the accumulated payload, charged to the
    /// per-aggregate context.
    pub data: PgVec<'mcx, u8>,
    /// C: `StringInfoData.cursor` — length of the first delimiter.
    pub cursor: i32,
}

impl<'mcx> StringAggState<'mcx> {
    /// C: `appendBinaryStringInfo(state, bytes, len)` (stringinfo.c). Appending
    /// a `text` payload (C `appendStringInfoText`, varlena.c:4240) reduces to
    /// the same. The buffer is charged to the aggregate context (fallible per
    /// the allocation-safety rule).
    fn append_binary(&mut self, bytes: &[u8]) -> PgResult<()> {
        let mcx = *self.data.allocator();
        self.data.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    /// C: `state->len` — current accumulated payload length.
    fn len(&self) -> i32 {
        self.data.len() as i32
    }
}

/// C: `TEXT_FORMAT_FLAG_MINUS` (varlena.c:5884) — left-justify flag.
pub const TEXT_FORMAT_FLAG_MINUS: i32 = 0x0001;

/// C: one `format()`/`concat()` argument: its value Datum, null flag, and type
/// OID. The variadic-array expansion of the C original is the caller's job (the
/// fmgr/Datum boundary); this carries the already-expanded per-argument view.
// No longer `Copy`: the canonical `Datum` carries an owned `PgVec` in its
// by-reference arm, so the per-argument view is borrowed (`&FormatArg`) at the
// consumption sites rather than copied.
#[derive(Clone)]
pub struct FormatArg<'mcx> {
    /// The argument's value (the Datum the fmgr boundary passes through).
    pub value: Datum<'mcx>,
    /// Whether the argument is SQL NULL (`PG_ARGISNULL`).
    pub is_null: bool,
    /// The argument's data type OID (`get_fn_expr_argtype` / `ARR_ELEMTYPE`).
    pub typid: Oid,
}

/// C: the parsed `%`-specifier output of `text_format_parse_format`.
#[derive(Clone, Copy)]
pub struct FormatSpec {
    /// `argpos`: explicit 1-based arg position, or -1 if unspecified.
    pub argpos: i32,
    /// `widthpos`: -1 none, 0 next-arg, >0 explicit width arg position.
    pub widthpos: i32,
    /// `flags`: bitmask (only `TEXT_FORMAT_FLAG_MINUS` today).
    pub flags: i32,
    /// `width`: directly-specified width (0 means omitted).
    pub width: i32,
}

/// C: `ToastCompressionId` (`access/toast_compression.h`) — the compression
/// method id stored in a compressed varlena, returned by
/// `toast_get_compression_id`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ToastCompressionId {
    /// `TOAST_PGLZ_COMPRESSION_ID`.
    Pglz,
    /// `TOAST_LZ4_COMPRESSION_ID`.
    Lz4,
    /// `TOAST_INVALID_COMPRESSION_ID`.
    Invalid,
}

// ===========================================================================
// Small pure helpers shared by the Split*String parsers.
// ===========================================================================

/// C: `scanner_isspace(ch)` (parser/scansup.c:117) — true iff the flex scanner
/// treats `ch` as whitespace. Pure character classifier; ported in-place so the
/// `Split*String` parsers match the lexer's `{space}` set.
fn scanner_isspace(ch: u8) -> bool {
    ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r' || ch == 0x0b || ch == 0x0c
}

/// C: `strchr(buf + from, '"')` — byte offset of the next double-quote at or
/// after `from`, or `None`.
fn strchr_quote(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..].iter().position(|&b| b == b'"').map(|i| from + i)
}

/// C: read `*p` of a NUL-terminated C string. Reading at the terminator (index
/// == len) yields `'\0'`, and the one-past peek (`endp[1]`) is also `'\0'`
/// because the buffer is NUL-terminated.
fn at(buf: &[u8], idx: usize) -> u8 {
    buf.get(idx).copied().unwrap_or(0)
}

/// Build a working NUL-terminated, modifiable copy of `rawstring` charged to
/// `mcx` (C's `Split*String` scribbles on its `char *` input in place).
fn nul_terminated_copy<'mcx>(mcx: Mcx<'mcx>, rawstring: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut buf = ::mcx::vec_with_capacity_in(mcx, rawstring.len() + 1)?;
    buf.extend_from_slice(rawstring);
    buf.push(0);
    Ok(buf)
}

/// Capture a parsed name as a NUL-free `PgString` charged to `mcx` (C's
/// `pstrdup` of the isolated name; the stored value is the NUL-terminated C
/// string). Invalid UTF-8 in the DB-encoding name surfaces as the seam's `Err`.
fn name_string<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgString<'mcx>> {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let mut out = ::mcx::vec_with_capacity_in(mcx, end)?;
    out.extend_from_slice(&bytes[..end]);
    PgString::from_utf8(out).map_err(|_| {
        ::types_error::PgError::error("invalid byte sequence for encoding")
            .with_sqlstate(::types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
    })
}

/// Append `bytes` to a working buffer charged to `mcx` (C
/// `appendBinaryStringInfo`/`appendStringInfoString`). Fallible per the
/// allocation-safety rule.
fn append_bytes<'mcx>(out: &mut PgVec<'mcx, u8>, bytes: &[u8]) -> PgResult<()> {
    let mcx = *out.allocator();
    out.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    out.extend_from_slice(bytes);
    Ok(())
}

/// Append a single byte to a working buffer charged to its mcx.
fn append_byte<'mcx>(out: &mut PgVec<'mcx, u8>, b: u8) -> PgResult<()> {
    let mcx = *out.allocator();
    out.try_reserve(1).map_err(|_| mcx.oom(1))?;
    out.push(b);
    Ok(())
}

/// C: `appendStringInfoSpaces(buf, count)` (stringinfo.c).
fn append_spaces<'mcx>(out: &mut PgVec<'mcx, u8>, count: i32) -> PgResult<()> {
    if count > 0 {
        let count = count as usize;
        let mcx = *out.allocator();
        out.try_reserve(count).map_err(|_| mcx.oom(count))?;
        out.extend(core::iter::repeat(b' ').take(count));
    }
    Ok(())
}

/// C: `appendStringInfoText(str, t)` (varlena.c:4239-4243) — append a `text`
/// payload to `str`; "Like `appendStringInfoString(str, text_to_cstring(t))`
/// but faster" via `appendBinaryStringInfo(str, VARDATA_ANY(t),
/// VARSIZE_ANY_EXHDR(t))`. The carrier `t` is the detoasted payload bytes.
pub fn append_string_info_text<'mcx>(str: &mut PgVec<'mcx, u8>, t: &[u8]) -> PgResult<()> {
    append_bytes(str, t)
}

// ===========================================================================
// Errors the C ereport()s (varlena.c).
// ===========================================================================

fn invalid_name_syntax() -> ::types_error::PgError {
    ::types_error::PgError::error("invalid name syntax").with_sqlstate(::types_error::ERRCODE_INVALID_NAME)
}

fn field_position_zero() -> ::types_error::PgError {
    ::types_error::PgError::error("field position must not be zero")
        .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

fn number_out_of_range() -> ::types_error::PgError {
    ::types_error::PgError::error("number is out of range")
        .with_sqlstate(::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

fn unterminated_specifier() -> ::types_error::PgError {
    ::types_error::PgError::error("unterminated format() type specifier")
        .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
        .with_hint("For a single \"%\" use \"%%\".")
}

fn argument_zero() -> ::types_error::PgError {
    ::types_error::PgError::error(
        "format specifies argument 0, but arguments are numbered from 1",
    )
    .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

fn width_position_unterminated() -> ::types_error::PgError {
    ::types_error::PgError::error("width argument position must be ended by \"$\"")
        .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

fn too_few_arguments() -> ::types_error::PgError {
    ::types_error::PgError::error("too few arguments for format()")
        .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

fn could_not_determine_type() -> ::types_error::PgError {
    ::types_error::PgError::error("could not determine data type of format() input")
        .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR)
}

fn null_identifier() -> ::types_error::PgError {
    ::types_error::PgError::error("null values cannot be formatted as an SQL identifier")
        .with_sqlstate(::types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED)
}

fn unrecognized_specifier(fmt: &[u8], cp: usize) -> PgResult<::types_error::PgError> {
    // C: errmsg("unrecognized format() type specifier \"%.*s\"",
    //           pg_mblen_range(cp, end_ptr), cp). pg_mblen_range never reads
    //  past the slice end; a byte sequence invalid in the database encoding
    //  surfaces as that report_invalid_encoding error instead (propagated).
    let n = mb::pg_mblen_range::call(&fmt[cp..])?.max(1) as usize;
    let bytes = &fmt[cp..(cp + n).min(fmt.len())];
    Ok(::types_error::PgError::error(format!(
        "unrecognized format() type specifier \"{}\"",
        String::from_utf8_lossy(bytes)
    ))
    .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
    .with_hint("For a single \"%\" use \"%%\"."))
}

fn insufficient_data() -> ::types_error::PgError {
    ::types_error::PgError::error("insufficient data left in message")
        .with_sqlstate(::types_error::ERRCODE_PROTOCOL_VIOLATION)
}

// ===========================================================================
// qualified-name and Split*String helpers (varlena.c:3522-3906).
// ===========================================================================

/// C: `textToQualifiedNameList(text *textval)` (varlena.c:3522-3555) — split a
/// (possibly qualified) name `text` on `.` into its identifier parts. Owner
/// seam `text_to_qualified_name_list` routes here.
pub fn text_to_qualified_name_list<'mcx>(
    mcx: Mcx<'mcx>,
    textval: &[u8],
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    // C:3532 rawname = text_to_cstring(textval); (handles detoast; modifiable).
    // text_to_cstring appends a trailing NUL; the parser wants the raw bytes.
    let rawname = text_to_cstring(mcx, textval)?;
    let raw = &rawname[..rawname.len().saturating_sub(1)];

    // C:3534-3537 if (!SplitIdentifierString(rawname, '.', &namelist)) ereport.
    let namelist = match split_identifier_string_bytes(mcx, raw, b'.')? {
        None => return Err(invalid_name_syntax()),
        Some(list) => list,
    };

    // C:3539-3542 if (namelist == NIL) ereport.
    if namelist.is_empty() {
        return Err(invalid_name_syntax());
    }

    // C:3544-3549 foreach: makeString(pstrdup(curname)). The parsed names are
    // already owned PgStrings.
    Ok(namelist)
}

/// C: `SplitIdentifierString(rawstring, separator, &namelist)`
/// (varlena.c:3580-3679) — parse a `separator`-separated list of (possibly
/// quoted) identifiers, downcasing per identifier rules. `Ok(None)` is the C
/// `false` (syntax error). Owner seam `split_identifier_string` routes here.
pub fn split_identifier_string<'mcx>(
    mcx: Mcx<'mcx>,
    raw: &str,
    separator: char,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    split_identifier_string_bytes(mcx, raw.as_bytes(), separator as u32 as u8)
}

/// Bytes-based core of [`split_identifier_string`] (the C input is a `char *`
/// in the database encoding, not necessarily UTF-8). The `&str` seam wrapper
/// above delegates here, as does `textToQualifiedNameList`.
fn split_identifier_string_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    raw: &[u8],
    separator: u8,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    // C operates in place on a NUL-terminated, modifiable copy of rawstring.
    let mut buf = nul_terminated_copy(mcx, raw)?;

    // C:3584-3585 nextp = rawstring; done = false.
    let mut nextp: usize = 0;
    let mut done = false;

    // C:3587 *namelist = NIL.
    let mut namelist: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);

    // C:3589-3590 skip leading whitespace.
    while scanner_isspace(at(&buf, nextp)) {
        nextp += 1;
    }

    // C:3592-3593 allow empty string.
    if at(&buf, nextp) == 0 {
        return Ok(Some(namelist));
    }

    // C:3596-3676 do { ... } while (!done);
    loop {
        let curname: usize;
        let endp: usize;

        if at(&buf, nextp) == b'"' {
            // C:3601-3618 Quoted name --- collapse quote-quote pairs, no downcase.
            curname = nextp + 1;
            let final_endp;
            loop {
                // C:3607 endp = strchr(nextp + 1, '"');
                let e = match strchr_quote(&buf, nextp + 1) {
                    None => return Ok(None), // C:3609 mismatched quotes.
                    Some(e) => e,
                };
                // C:3610-3611 if (endp[1] != '"') break;
                if at(&buf, e + 1) != b'"' {
                    final_endp = e;
                    break;
                }
                // C:3613 memmove(endp, endp + 1, strlen(endp)); collapse pair.
                buf.copy_within(e + 1.., e);
                buf.pop();
                // C:3614 nextp = endp.
                nextp = e;
            }
            // C:3617 nextp = endp + 1; (endp at terminating quote).
            endp = final_endp;
            nextp = endp + 1;
        } else {
            // C:3619-3647 Unquoted name --- extends to separator or whitespace.
            curname = nextp;
            // C:3626-3628.
            while at(&buf, nextp) != 0
                && at(&buf, nextp) != separator
                && !scanner_isspace(at(&buf, nextp))
            {
                nextp += 1;
            }
            // C:3629 endp = nextp.
            endp = nextp;
            // C:3630-3631 empty unquoted name not allowed.
            if curname == nextp {
                return Ok(None);
            }

            // C:3642-3646 downcase the identifier in place.
            let len = endp - curname;
            // C:3643 downname = downcase_truncate_identifier(curname, len, false).
            let downname = downcase_truncate_identifier(mcx, &buf[curname..endp])?;
            // C:3644 Assert(strlen(downname) <= len).
            debug_assert!(downname.len() <= len);
            // C:3645 strncpy(curname, downname, len); (copy + NUL-pad to len).
            for i in 0..len {
                buf[curname + i] = if i < downname.len() { downname[i] } else { 0 };
            }
        }

        // C:3649-3650 skip trailing whitespace.
        while scanner_isspace(at(&buf, nextp)) {
            nextp += 1;
        }

        // C:3652-3662 separator / end / invalid-syntax dispatch.
        if at(&buf, nextp) == separator {
            nextp += 1;
            while scanner_isspace(at(&buf, nextp)) {
                nextp += 1;
            }
        } else if at(&buf, nextp) == 0 {
            done = true;
        } else {
            return Ok(None); // C:3662 invalid syntax.
        }

        // C:3665 *endp = '\0'.
        buf[endp] = 0;

        // C:3668 truncate_identifier(curname, strlen(curname), false).
        let curlen = buf[curname..].iter().position(|&b| b == 0).unwrap_or(0);
        let truncated = truncate_identifier(mcx, &buf[curname..curname + curlen])?;

        // C:3673 *namelist = lappend(*namelist, curname).
        let name = name_string(mcx, &truncated)?;
        namelist.try_reserve(1).map_err(|_| mcx.oom(1))?;
        namelist.push(name);

        if done {
            break;
        }
    }

    Ok(Some(namelist))
}

/// C: `SplitDirectoriesString(rawstring, ',', &elemlist)`
/// (varlena.c:3707-3794) — split a comma-separated, possibly-quoted directory
/// list into canonicalized path elements. Owner seam `split_directories_string`
/// routes here (the C separator is `','` per the call sites).
pub fn split_directories_string<'mcx>(
    mcx: Mcx<'mcx>,
    rawstring: &str,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    let separator = b',';
    let mut buf = nul_terminated_copy(mcx, rawstring.as_bytes())?;

    // C:3711-3712 nextp = rawstring; done = false.
    let mut nextp: usize = 0;
    let mut done = false;

    let mut namelist: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);

    // C:3716-3717 skip leading whitespace.
    while scanner_isspace(at(&buf, nextp)) {
        nextp += 1;
    }

    // C:3719-3720 allow empty string.
    if at(&buf, nextp) == 0 {
        return Ok(Some(namelist));
    }

    // C:3723-3791 do { ... } while (!done);
    loop {
        let curname: usize;
        let endp: usize;

        if at(&buf, nextp) == b'"' {
            // C:3728-3745 Quoted name --- collapse quote-quote pairs.
            curname = nextp + 1;
            let final_endp;
            loop {
                let e = match strchr_quote(&buf, nextp + 1) {
                    None => return Ok(None), // C:3736 mismatched quotes.
                    Some(e) => e,
                };
                if at(&buf, e + 1) != b'"' {
                    final_endp = e;
                    break;
                }
                buf.copy_within(e + 1.., e);
                buf.pop();
                nextp = e;
            }
            endp = final_endp;
            nextp = endp + 1;
        } else {
            // C:3746-3759 Unquoted name --- extends to separator or end.
            curname = nextp;
            let mut e = nextp; // C: curname = endp = nextp.
            while at(&buf, nextp) != 0 && at(&buf, nextp) != separator {
                // C:3752-3754 trailing whitespace should not be in the name.
                if !scanner_isspace(at(&buf, nextp)) {
                    e = nextp + 1;
                }
                nextp += 1;
            }
            endp = e;
            // C:3757-3758 empty unquoted name not allowed.
            if curname == endp {
                return Ok(None);
            }
        }

        // C:3761-3762 skip trailing whitespace.
        while scanner_isspace(at(&buf, nextp)) {
            nextp += 1;
        }

        // C:3764-3774 dispatch.
        if at(&buf, nextp) == separator {
            nextp += 1;
            while scanner_isspace(at(&buf, nextp)) {
                nextp += 1;
            }
        } else if at(&buf, nextp) == 0 {
            done = true;
        } else {
            return Ok(None); // C:3774 invalid syntax.
        }

        // C:3777 *endp = '\0'.
        buf[endp] = 0;

        // C:3780-3781 if (strlen(curname) >= MAXPGPATH) curname[MAXPGPATH-1] = 0.
        let curlen = buf[curname..].iter().position(|&b| b == 0).unwrap_or(0);
        let curlen = if curlen >= MAXPGPATH {
            buf[curname + MAXPGPATH - 1] = 0;
            MAXPGPATH - 1
        } else {
            curlen
        };

        // C:3786-3788 curname = pstrdup(curname); canonicalize_path(curname).
        let canonical = canonicalize_path(mcx, &buf[curname..curname + curlen])?;
        let name = name_string(mcx, &canonical)?;
        namelist.try_reserve(1).map_err(|_| mcx.oom(1))?;
        namelist.push(name);

        if done {
            break;
        }
    }

    Ok(Some(namelist))
}

/// C: `SplitGUCList(rawstring, separator, &namelist)` (varlena.c:3828-3906) —
/// like `SplitIdentifierString` but with GUC-list quoting rules: no downcasing
/// and no truncation.
pub fn split_guc_list<'mcx>(
    mcx: Mcx<'mcx>,
    rawstring: &str,
    separator: char,
) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>> {
    let separator = separator as u32 as u8;
    let mut buf = nul_terminated_copy(mcx, rawstring.as_bytes())?;

    // C:3832-3833 nextp = rawstring; done = false.
    let mut nextp: usize = 0;
    let mut done = false;

    let mut namelist: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);

    // C:3837-3838 skip leading whitespace.
    while scanner_isspace(at(&buf, nextp)) {
        nextp += 1;
    }

    // C:3840-3841 allow empty string.
    if at(&buf, nextp) == 0 {
        return Ok(Some(namelist));
    }

    // C:3844-3903 do { ... } while (!done);
    loop {
        let curname: usize;
        let endp: usize;

        if at(&buf, nextp) == b'"' {
            // C:3849-3866 Quoted name --- collapse quote-quote pairs.
            curname = nextp + 1;
            let final_endp;
            loop {
                let e = match strchr_quote(&buf, nextp + 1) {
                    None => return Ok(None), // C:3857 mismatched quotes.
                    Some(e) => e,
                };
                if at(&buf, e + 1) != b'"' {
                    final_endp = e;
                    break;
                }
                buf.copy_within(e + 1.., e);
                buf.pop();
                nextp = e;
            }
            endp = final_endp;
            nextp = endp + 1;
        } else {
            // C:3867-3877 Unquoted name --- extends to separator or whitespace.
            curname = nextp;
            while at(&buf, nextp) != 0
                && at(&buf, nextp) != separator
                && !scanner_isspace(at(&buf, nextp))
            {
                nextp += 1;
            }
            endp = nextp;
            if curname == nextp {
                return Ok(None);
            }
        }

        // C:3879-3880 skip trailing whitespace.
        while scanner_isspace(at(&buf, nextp)) {
            nextp += 1;
        }

        // C:3882-3892 dispatch.
        if at(&buf, nextp) == separator {
            nextp += 1;
            while scanner_isspace(at(&buf, nextp)) {
                nextp += 1;
            }
        } else if at(&buf, nextp) == 0 {
            done = true;
        } else {
            return Ok(None); // C:3892 invalid syntax.
        }

        // C:3895 *endp = '\0'. No downcasing/truncation.
        buf[endp] = 0;

        // C:3900 *namelist = lappend(*namelist, curname).
        let curlen = buf[curname..].iter().position(|&b| b == 0).unwrap_or(0);
        let name = name_string(mcx, &buf[curname..curname + curlen])?;
        namelist.try_reserve(1).map_err(|_| mcx.oom(1))?;
        namelist.push(name);

        if done {
            break;
        }
    }

    Ok(Some(namelist))
}

// ===========================================================================
// split_part + split_text worker + array/table entry points
// (varlena.c:4626-5063).
// ===========================================================================

/// C: `split_part(PG_FUNCTION_ARGS)` (varlena.c:4625-4752) — return the
/// `fldnum`'th field of `inputstring` split on `fldsep`. `inputstring`/`fldsep`
/// are the detoasted payloads; `fldnum` is 1-based (negative counts from the
/// right). The result payload is charged to `mcx`.
pub fn split_part<'mcx>(
    mcx: Mcx<'mcx>,
    inputstring: &[u8],
    fldsep: &[u8],
    fldnum: i32,
    collid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut fldnum = fldnum;

    // C:4640-4643 field number is 1 based.
    if fldnum == 0 {
        return Err(field_position_zero());
    }

    // C:4645-4646 inputstring_len / fldsep_len.
    let inputstring_len = inputstring.len();
    let fldsep_len = fldsep.len();

    // C:4648-4650 return empty string for empty input string.
    if inputstring_len < 1 {
        return cstring_to_text_with_len(mcx, b"", 0);
    }

    // C:4652-4660 handle empty field separator.
    if fldsep_len < 1 {
        // C:4656-4659 if first or last field, return input string, else empty.
        if fldnum == 1 || fldnum == -1 {
            return cstring_to_text_with_len(mcx, inputstring, inputstring_len as i32);
        } else {
            return cstring_to_text_with_len(mcx, b"", 0);
        }
    }

    // C:4663 find the first field separator.
    let mut state = text_position_setup(mcx, inputstring, fldsep, collid)?;

    // C:4665 found = text_position_next(&state).
    let mut found = text_position_next(&mut state)?;

    // C:4667-4676 special case if fldsep not found at all.
    if !found {
        text_position_cleanup(&mut state);
        if fldnum == 1 || fldnum == -1 {
            return cstring_to_text_with_len(mcx, inputstring, inputstring_len as i32);
        } else {
            return cstring_to_text_with_len(mcx, b"", 0);
        }
    }

    // C:4682-4714 take care of a negative field number.
    if fldnum < 0 {
        // C:4685 we found a fldsep, so there are at least two fields.
        let mut numfields = 2;

        // C:4687-4688 count remaining fields.
        while text_position_next(&mut state)? {
            numfields += 1;
        }

        // C:4691-4698 special case of last field needs no extra pass.
        if fldnum == -1 {
            let start_off = text_position_get_match_off(&state) + state.last_match_len as usize;
            let end_off = inputstring_len;
            text_position_cleanup(&mut state);
            return cstring_to_text_with_len(
                mcx,
                &inputstring[start_off..end_off],
                (end_off - start_off) as i32,
            );
        }

        // C:4701 convert fldnum to positive notation.
        fldnum += numfields + 1;

        // C:4703-4708 if nonexistent field, return empty string.
        if fldnum <= 0 {
            text_position_cleanup(&mut state);
            return cstring_to_text_with_len(mcx, b"", 0);
        }

        // C:4711-4713 reset to first match, now with positive fldnum.
        text_position_reset(&mut state);
        found = text_position_next(&mut state)?;
        debug_assert!(found);
    }

    // C:4717-4718 identify bounds of first field.
    let mut start_off: usize = 0;
    let mut end_off = text_position_get_match_off(&state);

    // C:4720-4727.
    while found && {
        fldnum -= 1;
        fldnum > 0
    } {
        // C:4723 start_ptr = end_ptr + state.last_match_len.
        start_off = end_off + state.last_match_len as usize;
        // C:4724 found = text_position_next(&state).
        found = text_position_next(&mut state)?;
        if found {
            end_off = text_position_get_match_off(&state);
        }
    }

    // C:4729 text_position_cleanup(&state).
    text_position_cleanup(&mut state);

    if fldnum > 0 {
        // C:4731-4744 N'th field separator not found.
        if fldnum == 1 {
            // C:4737-4740 last field requested, return it.
            let last_len = start_off; // start_off - 0 (VARDATA_ANY base).
            cstring_to_text_with_len(
                mcx,
                &inputstring[start_off..inputstring_len],
                (inputstring_len - last_len) as i32,
            )
        } else {
            // C:4743 else empty string.
            cstring_to_text_with_len(mcx, b"", 0)
        }
    } else {
        // C:4748 non-last field requested.
        cstring_to_text_with_len(
            mcx,
            &inputstring[start_off..end_off],
            (end_off - start_off) as i32,
        )
    }
}

/// C: `text_isequal(txt1, txt2, collid)` (varlena.c:4757-4764) — true iff two
/// texts compare equal under `collid` (`DirectFunctionCall2Coll(texteq, ...)`).
pub fn text_isequal(txt1: &[u8], txt2: &[u8], collid: Oid) -> PgResult<bool> {
    crate::comparison::texteq(txt1, txt2, collid)
}

/// C: `split_text_accum_result(tstate, field_value, null_string, collation)`
/// (varlena.c:4987-5019) — map `field_value` to NULL when it matches
/// `null_string`, then append the field. The array-build / tuplestore dispatch
/// of the C version reduces to appending the ordered field with its NULL flag.
fn split_text_accum_result<'mcx>(
    mcx: Mcx<'mcx>,
    fields: &mut PgVec<'mcx, SplitField<'mcx>>,
    field_value: &[u8],
    null_string: Option<&[u8]>,
    collation: Oid,
) -> PgResult<()> {
    // C:4993 bool is_null = false;
    let mut is_null = false;

    // C:4995-4996 if (null_string && text_isequal(field_value, null_string)).
    if let Some(null_string) = null_string {
        if text_isequal(field_value, null_string, collation)? {
            is_null = true;
        }
    }

    // C:4998-5018 stash the field (array/tuplestore both reduce to appending).
    let mut bytes = ::mcx::vec_with_capacity_in(mcx, field_value.len())?;
    bytes.extend_from_slice(field_value);
    fields.try_reserve(1).map_err(|_| mcx.oom(1))?;
    fields.push(SplitField { bytes, is_null });
    Ok(())
}

/// C: `split_text` worker (varlena.c:4848-4979) — split `inputstring` on
/// `fldsep` (None == split into individual characters) producing the ordered
/// fields; `null_string` maps matching fields to NULL. Returns `None` when the
/// input is SQL NULL (C returns false). Shared by the array/table entry points.
pub fn split_text<'mcx>(
    mcx: Mcx<'mcx>,
    inputstring: Option<&[u8]>,
    fldsep: Option<&[u8]>,
    null_string: Option<&[u8]>,
    collation: Oid,
) -> PgResult<Option<PgVec<'mcx, SplitField<'mcx>>>> {
    // C:4860-4862 when input string is NULL, result is NULL too.
    let input_bytes = match inputstring {
        None => return Ok(None),
        Some(b) => b,
    };

    let mut fields: PgVec<'mcx, SplitField<'mcx>> = PgVec::new_in(mcx);

    if let Some(fldsep_bytes) = fldsep {
        // C:4878-4943 Normal case with non-null fldsep.
        let inputstring_len = input_bytes.len();
        let fldsep_len = fldsep_bytes.len();

        // C:4889-4891 return empty set for empty input string.
        if inputstring_len < 1 {
            return Ok(Some(fields));
        }

        // C:4893-4899 empty field separator: one-element set.
        if fldsep_len < 1 {
            split_text_accum_result(mcx, &mut fields, input_bytes, null_string, collation)?;
            return Ok(Some(fields));
        }

        // C:4901 text_position_setup.
        let mut state = text_position_setup(mcx, input_bytes, fldsep_bytes, collation)?;

        // C:4903 start_ptr = VARDATA_ANY(inputstring); (offset 0).
        let mut start_off: usize = 0;

        // C:4905-4940 for (;;).
        loop {
            // C:4913 found = text_position_next(&state).
            let found = text_position_next(&mut state)?;

            let chunk_len;
            let match_off;
            if !found {
                // C:4916-4918 fetch last field (remainder after start_ptr).
                chunk_len = inputstring_len - start_off;
                match_off = 0; // not used.
            } else {
                // C:4922-4924 fetch non-last field.
                match_off = text_position_get_match_off(&state);
                chunk_len = match_off - start_off;
            }

            // C:4928 cstring_to_text_with_len(start_ptr, chunk_len).
            let chunk = &input_bytes[start_off..start_off + chunk_len];

            // C:4931-4932 stash away this field.
            split_text_accum_result(mcx, &mut fields, chunk, null_string, collation)?;

            // C:4936-4937 if (!found) break.
            if !found {
                break;
            }

            // C:4939 start_ptr = end_ptr + state.last_match_len.
            start_off = match_off + state.last_match_len as usize;
        }

        // C:4942 text_position_cleanup(&state).
        text_position_cleanup(&mut state);
    } else {
        // C:4944-4976 When fldsep is NULL, each character is a separate element.
        let inputstring_len = input_bytes.len();

        // C:4955-4956 start_ptr / end_ptr.
        let mut start_off: usize = 0;
        let mut remaining = inputstring_len;

        // C:4958 while (inputstring_len > 0).
        while remaining > 0 {
            // C:4960 chunk_len = pg_mblen_range(start_ptr, end_ptr).
            let chunk_len = mb::pg_mblen_range::call(&input_bytes[start_off..inputstring_len])?.max(1) as usize;

            // C:4965 cstring_to_text_with_len(start_ptr, chunk_len).
            let chunk = &input_bytes[start_off..start_off + chunk_len];

            // C:4968-4969 stash away this field.
            split_text_accum_result(mcx, &mut fields, chunk, null_string, collation)?;

            // C:4973-4974 advance.
            start_off += chunk_len;
            remaining -= chunk_len;
        }
    }

    // C:4978 return true.
    Ok(Some(fields))
}

/// C: `text_to_array` / `text_to_array_null` (varlena.c:4771-4801) — build a
/// `text[]` from the split fields via the array subsystem (seam). Returns the
/// array Datum, or `None` for a NULL result.
pub fn text_to_array<'mcx>(
    mcx: Mcx<'mcx>,
    inputstring: Option<&[u8]>,
    fldsep: Option<&[u8]>,
    null_string: Option<&[u8]>,
    collation: Oid,
) -> PgResult<Option<Datum<'mcx>>> {
    // C:4779-4780 if (!split_text(fcinfo, &tstate)) PG_RETURN_NULL().
    let fields = match split_text(mcx, inputstring, fldsep, null_string, collation)? {
        None => return Ok(None),
        Some(fields) => fields,
    };

    // C:4782-4786 astate == NULL -> construct_empty_array(TEXTOID); else
    // makeArrayResult. The owner builds either form from the ordered fields.
    Ok(Some(build_text_array(mcx, &fields)?))
}

/// C: `text_to_array_null` (varlena.c:4797-4801) — separate entry point only;
/// `return text_to_array(fcinfo)`.
pub fn text_to_array_null<'mcx>(
    mcx: Mcx<'mcx>,
    inputstring: Option<&[u8]>,
    fldsep: Option<&[u8]>,
    null_string: Option<&[u8]>,
    collation: Oid,
) -> PgResult<Option<Datum<'mcx>>> {
    text_to_array(mcx, inputstring, fldsep, null_string, collation)
}

/// C: `text_to_table` / `text_to_table_null` (varlena.c:4808-4836) — emit the
/// split fields into the SRF tuplestore (owner seam). A NULL input produces no
/// rows (C's `split_text` returns false without touching the tuplestore).
pub fn text_to_table<'mcx>(
    mcx: Mcx<'mcx>,
    tupstore: &mut nodes::Tuplestorestate<'_>,
    tupdesc: &::types_tuple::heaptuple::TupleDescData<'_>,
    inputstring: Option<&[u8]>,
    fldsep: Option<&[u8]>,
    null_string: Option<&[u8]>,
    collation: Oid,
) -> PgResult<()> {
    // C:4810-4817 the SRF tuplestore/tupdesc come from the ReturnSetInfo
    // (InitMaterializedSRF); the fmgr/SRF boundary supplies them here.
    // C:4819 (void) split_text(fcinfo, &tstate); — split_text's tupstore arm
    // pushes each field via tuplestore_putvalues. Here we re-route the
    // accumulated fields to the tuplestore owner seam.
    if let Some(fields) = split_text(mcx, inputstring, fldsep, null_string, collation)? {
        for field in &fields {
            tuplestore_put_field(mcx, tupstore, tupdesc, &field.bytes, field.is_null)?;
        }
    }
    Ok(())
}

/// C: `text_to_table_null` (varlena.c:4832-4836) — separate entry point only.
pub fn text_to_table_null<'mcx>(
    mcx: Mcx<'mcx>,
    tupstore: &mut nodes::Tuplestorestate<'_>,
    tupdesc: &::types_tuple::heaptuple::TupleDescData<'_>,
    inputstring: Option<&[u8]>,
    fldsep: Option<&[u8]>,
    null_string: Option<&[u8]>,
    collation: Oid,
) -> PgResult<()> {
    text_to_table(mcx, tupstore, tupdesc, inputstring, fldsep, null_string, collation)
}

// ===========================================================================
// array_to_text join (varlena.c:5026-5184).
// ===========================================================================

/// C: `array_to_text(PG_FUNCTION_ARGS)` (varlena.c:5026-5033) — join array
/// elements with `fldsep`, ignoring NULL elements.
pub fn array_to_text<'mcx>(
    mcx: Mcx<'mcx>,
    v: Datum<'mcx>,
    element_type: Oid,
    fldsep: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    // C:5032 array_to_text_internal(fcinfo, v, fldsep, NULL).
    array_to_text_internal(mcx, v, element_type, fldsep, None)
}

/// C: `array_to_text_null(PG_FUNCTION_ARGS)` (varlena.c:5042-5063) — like
/// `array_to_text` but renders NULL elements as `null_string` (when non-None).
pub fn array_to_text_null<'mcx>(
    mcx: Mcx<'mcx>,
    v: Datum<'mcx>,
    element_type: Oid,
    fldsep: &[u8],
    null_string: Option<&[u8]>,
) -> PgResult<PgVec<'mcx, u8>> {
    // C:5062 array_to_text_internal(fcinfo, v, fldsep, null_string).
    array_to_text_internal(mcx, v, element_type, fldsep, null_string)
}

/// C: `array_to_text_internal` (varlena.c:5068-5184) — walk array elements,
/// output-format each non-null one (owner seam), interleaving `fldsep` and
/// optionally emitting `null_string` for nulls.
pub fn array_to_text_internal<'mcx>(
    mcx: Mcx<'mcx>,
    v: Datum<'mcx>,
    element_type: Oid,
    fldsep: &[u8],
    null_string: Option<&[u8]>,
) -> PgResult<PgVec<'mcx, u8>> {
    // C:5088-5094 nitems == 0 -> empty string. The owner returns the
    // deconstructed, output-formatted elements; an empty list maps to
    // cstring_to_text_with_len("", 0).
    let elements = array_to_text_elements(mcx, &v, element_type)?;
    if elements.is_empty() {
        return cstring_to_text_with_len(mcx, b"", 0);
    }

    // C:5097 initStringInfo(&buf).
    let mut buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    // C:5081 bool printed = false.
    let mut printed = false;

    // C:5134-5178 for (i = 0; i < nitems; i++).
    for item in &elements {
        match item {
            ArrayElement::Null => {
                // C:5140-5151 NULL element.
                if let Some(null_string) = null_string {
                    if printed {
                        // C:5146 appendStringInfo(&buf, "%s%s", fldsep, null_string).
                        append_bytes(&mut buf, fldsep)?;
                        append_bytes(&mut buf, null_string)?;
                    } else {
                        // C:5148 appendStringInfoString(&buf, null_string).
                        append_bytes(&mut buf, null_string)?;
                    }
                    printed = true;
                }
            }
            ArrayElement::Value(value) => {
                // C:5154-5165 non-null element (already output-formatted).
                if printed {
                    // C:5159 appendStringInfo(&buf, "%s%s", fldsep, value).
                    append_bytes(&mut buf, fldsep)?;
                    append_bytes(&mut buf, value)?;
                } else {
                    // C:5161 appendStringInfoString(&buf, value).
                    append_bytes(&mut buf, value)?;
                }
                printed = true;
            }
        }
    }

    // C:5180-5183 result = cstring_to_text_with_len(buf.data, buf.len).
    cstring_to_text_with_len(mcx, &buf, buf.len() as i32)
}

// ===========================================================================
// pg_column_* introspection (varlena.c:5274-5409).
// ===========================================================================

/// C: `pg_column_size(PG_FUNCTION_ARGS)` (varlena.c:5274-5315) — on-disk/
/// compressed size of any datum. The `fn_extra` typlen cache + `get_typlen`
/// lookup are the fmgr/Datum boundary's job; `typlen` is supplied resolved
/// (known non-zero per the C `elog` guard).
pub fn pg_column_size(mcx: Mcx<'_>, value: &Datum<'_>, typlen: i32) -> PgResult<i32> {
    let result;

    if typlen == -1 {
        // C:5300-5301 varlena type, possibly toasted.
        result = toast_datum_size(mcx, value)?;
    } else if typlen == -2 {
        // C:5305-5306 cstring -> strlen(DatumGetCString(value)) + 1.
        result = cstring_datum_len(value)? + 1;
    } else {
        // C:5310-5311 ordinary fixed-width type.
        result = typlen;
    }

    Ok(result)
}

/// C: `pg_column_compression(PG_FUNCTION_ARGS)` (varlena.c:5321-5368) — the
/// compression method name ("pglz"/"lz4") of a compressed varlena, or `None`.
/// Returns the `text` payload charged to `mcx`.
pub fn pg_column_compression<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'_>,
    typlen: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C:5345-5346 if (typlen != -1) PG_RETURN_NULL().
    if typlen != -1 {
        return Ok(None);
    }

    // C:5348-5350 cmid = toast_get_compression_id(...).
    let cmid = toast_get_compression_id(value)?;
    // C:5351-5352 if (cmid == TOAST_INVALID_COMPRESSION_ID) PG_RETURN_NULL().
    if cmid == ToastCompressionId::Invalid {
        return Ok(None);
    }

    // C:5355-5365 convert compression method id to name.
    let result: &[u8] = match cmid {
        ToastCompressionId::Pglz => b"pglz",
        ToastCompressionId::Lz4 => b"lz4",
        // C:5363-5364 default: elog(ERROR, "invalid compression method id %d").
        ToastCompressionId::Invalid => {
            return Err(::types_error::PgError::error("invalid compression method id")
                .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR));
        }
    };

    // C:5367 cstring_to_text(result).
    Ok(Some(cstring_to_text_with_len(mcx, result, result.len() as i32)?))
}

/// C: `pg_column_toast_chunk_id(PG_FUNCTION_ARGS)` (varlena.c:5374-5409) — the
/// TOAST value OID of an on-disk external varlena, or `None`. The `fn_extra`
/// typlen cache is the fmgr boundary's job; the `VARATT_IS_EXTERNAL_ONDISK`
/// check and `va_valueid` extraction live behind the `toast_chunk_id` owner
/// seam (`None` when not on-disk).
pub fn pg_column_toast_chunk_id(value: &Datum<'_>, typlen: i32) -> PgResult<Option<Oid>> {
    // C:5398-5399 if (typlen != -1) PG_RETURN_NULL().
    if typlen != -1 {
        return Ok(None);
    }
    // C:5403-5408 if (!VARATT_IS_EXTERNAL_ONDISK) NULL; else va_valueid.
    toast_chunk_id(value)
}

// ===========================================================================
// string_agg / bytea_string_agg (varlena.c:506-582, 5412-5636).
// ===========================================================================

/// C: `makeStringAggState(fcinfo)` (varlena.c:5422-5444) — allocate an empty
/// transition state in the aggregate context. `AggCheckCallContext` + the
/// context switch are the fmgr/agg boundary's job; `makeStringInfo()` yields an
/// empty buffer with `cursor == 0`, which this builds charged to `mcx` (the
/// aggregate context).
pub fn make_string_agg_state<'mcx>(mcx: Mcx<'mcx>) -> StringAggState<'mcx> {
    StringAggState { data: PgVec::new_in(mcx), cursor: 0 }
}

/// C: `string_agg_transfn(PG_FUNCTION_ARGS)` (varlena.c:5446-5495) — append
/// `delim` (first call records its length in `cursor`) then `value`; a NULL
/// value is a no-op.
pub fn string_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<StringAggState<'mcx>>,
    value: Option<&[u8]>,
    delim: Option<&[u8]>,
) -> PgResult<Option<StringAggState<'mcx>>> {
    // C:5451 state = PG_ARGISNULL(0) ? NULL : ...;
    let mut state = state;

    // C:5454 if (!PG_ARGISNULL(1)).
    if let Some(value) = value {
        // C:5457 bool isfirst = false;
        let mut isfirst = false;

        // C:5470-5474 if (state == NULL) { make; isfirst = true; }.
        let state_ref = match state {
            Some(ref mut s) => s,
            None => {
                state = Some(make_string_agg_state(mcx));
                isfirst = true;
                state.as_mut().expect("just assigned")
            }
        };

        // C:5476-5483 if (!PG_ARGISNULL(2)) { appendStringInfoText(delim);
        // if (isfirst) state->cursor = VARSIZE_ANY_EXHDR(delim); }.
        if let Some(delim) = delim {
            state_ref.append_binary(delim)?;
            if isfirst {
                state_ref.cursor = delim.len() as i32;
            }
        }

        // C:5485 appendStringInfoText(state, value).
        state_ref.append_binary(value)?;
    }

    // C:5492-5494 if (state) PG_RETURN_POINTER; else PG_RETURN_NULL.
    Ok(state)
}

/// C: `bytea_string_agg_transfn(PG_FUNCTION_ARGS)` (varlena.c:506-557) —
/// identical to `string_agg_transfn` but on raw bytea payloads.
pub fn bytea_string_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<StringAggState<'mcx>>,
    value: Option<&[u8]>,
    delim: Option<&[u8]>,
) -> PgResult<Option<StringAggState<'mcx>>> {
    // C:511 state = PG_ARGISNULL(0) ? NULL : ...;
    let mut state = state;

    // C:514 if (!PG_ARGISNULL(1)).
    if let Some(value) = value {
        // C:517 bool isfirst = false;
        let mut isfirst = false;

        // C:530-534 if (state == NULL) { make; isfirst = true; }.
        let state_ref = match state {
            Some(ref mut s) => s,
            None => {
                state = Some(make_string_agg_state(mcx));
                isfirst = true;
                state.as_mut().expect("just assigned")
            }
        };

        // C:536-544 if (!PG_ARGISNULL(2)) { appendBinaryStringInfo(delim);
        // if (isfirst) state->cursor = VARSIZE_ANY_EXHDR(delim); }.
        if let Some(delim) = delim {
            state_ref.append_binary(delim)?;
            if isfirst {
                state_ref.cursor = delim.len() as i32;
            }
        }

        // C:546-547 appendBinaryStringInfo(value).
        state_ref.append_binary(value)?;
    }

    // C:554-556 if (state) PG_RETURN_POINTER; else PG_RETURN_NULL.
    Ok(state)
}

/// C: `string_agg_combine(PG_FUNCTION_ARGS)` (varlena.c:5501-5543) — merge two
/// partial states (parallel aggregation); state1's cursor is preserved.
pub fn string_agg_combine<'mcx>(
    mcx: Mcx<'mcx>,
    state1: Option<StringAggState<'mcx>>,
    state2: Option<StringAggState<'mcx>>,
) -> PgResult<Option<StringAggState<'mcx>>> {
    let mut state1 = state1;

    // C:5514-5523 if (state2 == NULL) return state1 (NULL or not).
    let state2 = match state2 {
        None => return Ok(state1),
        Some(state2) => state2,
    };

    match state1 {
        // C:5525-5535 if (state1 == NULL): copy state2's data into agg_context.
        None => {
            let mut new_state = make_string_agg_state(mcx);
            new_state.append_binary(&state2.data)?;
            new_state.cursor = state2.cursor;
            state1 = Some(new_state);
        }
        // C:5536-5540 else if (state2->len > 0): append; cursor unchanged.
        Some(ref mut s) => {
            if state2.len() > 0 {
                s.append_binary(&state2.data)?;
            }
        }
    }

    // C:5542 PG_RETURN_POINTER(state1).
    Ok(state1)
}

/// C: `string_agg_serialize(PG_FUNCTION_ARGS)` (varlena.c:5551-5574) — serialize
/// the state to a bytea wire blob: int4 cursor (network byte order) then the
/// data bytes. Strict, so NULL input is not handled.
pub fn string_agg_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    state: &StringAggState<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    // C's pq_begintypsend/pq_endtypsend build a per-call StringInfo then copy
    // it out into a fresh bytea; the wire blob is [cursor:4 BE][data..].
    let mut buf = ::mcx::vec_with_capacity_in(mcx, 4 + state.data.len())?;
    // C:5566 pq_sendint(&buf, state->cursor, 4); (network byte order).
    buf.extend_from_slice(&(state.cursor as u32).to_be_bytes());
    // C:5569 pq_sendbytes(&buf, state->data, state->len).
    buf.extend_from_slice(&state.data);
    // C:5571 result = pq_endtypsend(&buf).
    Ok(buf)
}

/// C: `string_agg_deserialize(PG_FUNCTION_ARGS)` (varlena.c:5582-5616) —
/// reconstruct a state from the bytea wire blob produced by
/// `string_agg_serialize`. Strict, so NULL input is not handled.
pub fn string_agg_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    sstate: &[u8],
) -> PgResult<StringAggState<'mcx>> {
    // C:5600-5601 initReadOnlyStringInfo(&buf, VARDATA_ANY, VARSIZE_ANY_EXHDR).
    let buf = sstate;

    // C:5603 result = makeStringAggState(fcinfo).
    let mut result = make_string_agg_state(mcx);

    // C:5606 result->cursor = pq_getmsgint(&buf, 4); (PROTOCOL_VIOLATION if <4).
    if buf.len() < 4 {
        return Err(insufficient_data());
    }
    let cursor = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    result.cursor = cursor as i32;

    // C:5609-5611 datalen = VARSIZE_ANY_EXHDR - 4; append the data bytes.
    let data = &buf[4..];
    result.append_binary(data)?;

    // C:5613 pq_getmsgend(&buf) — all bytes consumed.
    Ok(result)
}

/// C: `string_agg_finalfn(PG_FUNCTION_ARGS)` (varlena.c:5618-5636) — result is
/// `data` with the first delimiter (cursor bytes) stripped, as `text`. The
/// stripped payload is charged to `mcx`.
pub fn string_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<&StringAggState<'mcx>>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C:5626-5635 if (state != NULL) strip data before cursor -> text; else NULL.
    match state {
        Some(state) => {
            // C:5631-5632 cstring_to_text_with_len(&state->data[cursor],
            //                                       state->len - state->cursor).
            let start = state.cursor as usize;
            let stripped = &state.data[start..];
            Ok(Some(cstring_to_text_with_len(mcx, stripped, stripped.len() as i32)?))
        }
        None => Ok(None),
    }
}

/// C: `bytea_string_agg_finalfn(PG_FUNCTION_ARGS)` (varlena.c:559-582) — result
/// is `data` with the first delimiter (cursor bytes) stripped, as `bytea`. C
/// `palloc(strippedlen + VARHDRSZ)` + memcpy reduces to wrapping the stripped
/// payload (the header is the Datum boundary's job); charged to `mcx`.
pub fn bytea_string_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<&StringAggState<'mcx>>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C:567-581 if (state != NULL) strippedlen = len - cursor; copy out; else NULL.
    match state {
        Some(state) => {
            let start = state.cursor as usize;
            let stripped = &state.data[start..];
            let mut out = ::mcx::vec_with_capacity_in(mcx, stripped.len())?;
            out.extend_from_slice(stripped);
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

// ===========================================================================
// concat / concat_ws (varlena.c:5644-5792).
// ===========================================================================

/// C: `build_concat_foutcache(fcinfo, argidx)` (varlena.c:5644-5671) — prepare
/// fmgr output-function info for each concat-like argument from `argidx`. The
/// `FmgrInfo` cache lives in `fn_mcxt` (the fmgr/Datum boundary); the per-call
/// stringify is done by the `output_function_call` owner seam, so there is no
/// separate cache to materialize here. The validity check
/// (`!OidIsValid(valtype) -> elog`) is mirrored by `concat_internal`'s
/// per-argument `output_function_call`, which carries the type. (No body in the
/// payload layer — folded into `concat_internal`'s seam dispatch.)
pub fn build_concat_foutcache(_argidx: usize, _args: &[FormatArg<'_>]) {
    // C builds and caches FmgrInfo per arg; in the layered surface the
    // owner-seam dispatch is keyed by typid each call, so this is a no-op
    // placeholder kept for the named-symbol map. See concat_internal.
}

/// C: `concat_internal(sepstr, argidx, fcinfo)` (varlena.c:5682-5757) — join
/// args (from `argidx`) with `sepstr`, stringifying each non-null arg via its
/// output function (owner seam). Returns `None` if the result should be NULL.
///
/// The `concat(VARIADIC some-array)` fast path (delegating to
/// `array_to_text_internal`) is taken when `variadic_array` carries the single
/// array argument; otherwise `args` is the already-expanded value list.
pub fn concat_internal<'mcx>(
    mcx: Mcx<'mcx>,
    sepstr: &[u8],
    argidx: usize,
    args: &[FormatArg<'mcx>],
    variadic_array: Option<(Datum<'mcx>, Oid)>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C:5697-5725 concat(VARIADIC some-array): hand off to array_to_text with a
    // NULL null_string (ignore nulls), matching the loop below.
    if let Some((arr, element_type)) = variadic_array {
        // C:5705-5706 concat(VARIADIC NULL) is defined as NULL.
        // (The caller passes None for a NULL array; a Some here is non-null.)
        return Ok(Some(array_to_text_internal(mcx, arr, element_type, sepstr, None)?));
    }

    // C:5728 initStringInfo(&str); C:5689 bool first_arg = true.
    let mut str: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    let mut first_arg = true;

    // C:5735-5751 for (i = argidx; i < PG_NARGS(); i++).
    let mut i = argidx;
    while i < args.len() {
        // C:5737 if (!PG_ARGISNULL(i)).
        if !args[i].is_null {
            // C:5742-5745 add separator if appropriate.
            if first_arg {
                first_arg = false;
            } else {
                append_bytes(&mut str, sepstr)?;
            }

            // C:5748-5749 OutputFunctionCall(&foutcache[i], value); append.
            let out = output_function_call(mcx, args[i].typid, &args[i].value)?;
            append_bytes(&mut str, &out)?;
        }
        i += 1;
    }

    // C:5753 result = cstring_to_text_with_len(str.data, str.len).
    Ok(Some(cstring_to_text_with_len(mcx, &str, str.len() as i32)?))
}

/// C: `text_concat(PG_FUNCTION_ARGS)` (varlena.c:5762-5771) — concatenate all
/// args, no separator.
pub fn text_concat<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[FormatArg<'mcx>],
    variadic_array: Option<(Datum<'mcx>, Oid)>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C:5767 result = concat_internal("", 0, fcinfo).
    concat_internal(mcx, b"", 0, args, variadic_array)
}

/// C: `text_concat_ws(PG_FUNCTION_ARGS)` (varlena.c:5777-5792) — first arg is
/// the separator, concatenate the remaining args.
pub fn text_concat_ws<'mcx>(
    mcx: Mcx<'mcx>,
    sep: Option<&[u8]>,
    args: &[FormatArg<'mcx>],
    variadic_array: Option<(Datum<'mcx>, Oid)>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C:5784-5786 return NULL when separator is NULL.
    let sep_bytes = match sep {
        None => return Ok(None),
        Some(s) => s,
    };
    // C:5788 result = concat_internal(sep, 1, fcinfo).
    concat_internal(mcx, sep_bytes, 1, args, variadic_array)
}

/// C: the `text_format` VARIADIC-array expansion (varlena.c:5921-5959) —
/// `deconstruct_array` the labeled array argument into its per-element
/// `(value, isnull, element_type)` `FormatArg`s, so the main `text_format` loop
/// indexes them exactly like the non-variadic per-argument case. The element
/// type's storage attributes come from `get_typlenbyvalalign` (C:
/// `get_typlenbyvalalign(element_type, ...)`).
pub fn array_to_format_args<'mcx>(
    mcx: Mcx<'mcx>,
    arr: Datum<'mcx>,
    element_type: Oid,
) -> PgResult<PgVec<'mcx, FormatArg<'mcx>>> {
    // C:5953 get_typlenbyvalalign(element_type, &elmlen, &elmbyval, &elmalign).
    let lba = lsyscache_seams::get_typlenbyvalalign::call(element_type)?;
    // C:5956-5957 deconstruct_array(arr, element_type, ...).
    let elems = arrayfuncs_seams::deconstruct_array_v::call(
        mcx,
        arr,
        element_type,
        lba.typlen,
        lba.typbyval,
        lba.typalign as core::ffi::c_char,
    )?;
    let mut out = ::mcx::vec_with_capacity_in(mcx, elems.len())?;
    for (value, isnull) in elems {
        out.push(FormatArg { value, is_null: isnull, typid: element_type });
    }
    Ok(out)
}

// ===========================================================================
// format() (varlena.c:5884-6406).
// ===========================================================================

/// C: `ADVANCE_PARSE_POINTER(ptr, end_ptr)` (varlena.c:5886-5893) —
/// pre-increment the parse cursor, erroring if it reaches `end`.
fn advance_parse_pointer(cp: usize, end: usize) -> PgResult<usize> {
    let next = cp + 1;
    if next >= end {
        return Err(unterminated_specifier());
    }
    Ok(next)
}

/// C: `text_format_parse_digits(&ptr, end_ptr, &value)` (varlena.c:6175-6199) —
/// parse contiguous decimal digits into `value`; returns (consumed offset,
/// found, value).
pub fn text_format_parse_digits(fmt: &[u8], cp: usize) -> PgResult<(usize, bool, i32)> {
    let mut found = false;
    let mut cp = cp;
    let mut val: i32 = 0;
    let end = fmt.len();

    // C: while (*cp >= '0' && *cp <= '9').
    while at(fmt, cp) >= b'0' && at(fmt, cp) <= b'9' {
        let digit = (at(fmt, cp) - b'0') as i32;
        // C: if (pg_mul_s32_overflow || pg_add_s32_overflow) ereport(...).
        let next = val.checked_mul(10).and_then(|m| m.checked_add(digit));
        match next {
            Some(v) => val = v,
            None => return Err(number_out_of_range()),
        }
        // C: ADVANCE_PARSE_POINTER(cp, end_ptr).
        cp = advance_parse_pointer(cp, end)?;
        found = true;
    }

    Ok((cp, found, val))
}

/// C: `text_format_parse_format(start_ptr, end_ptr, ...)` (varlena.c:6224-6296)
/// — parse `[argpos][flags][width]` after the `%`, leaving `cp` at the type
/// char.
pub fn text_format_parse_format(fmt: &[u8], start: usize) -> PgResult<(usize, FormatSpec)> {
    let mut cp = start;
    let end = fmt.len();

    // C: set defaults.
    let mut spec = FormatSpec { argpos: -1, widthpos: -1, flags: 0, width: 0 };

    // C: try to identify first number.
    let (newcp, found, n) = text_format_parse_digits(fmt, cp)?;
    cp = newcp;
    if found {
        if at(fmt, cp) != b'$' {
            // C: Must be just a width and a type, so we're done.
            spec.width = n;
            return Ok((cp, spec));
        }
        // C: The number was argument position.
        spec.argpos = n;
        if n == 0 {
            return Err(argument_zero());
        }
        cp = advance_parse_pointer(cp, end)?;
    }

    // C: Handle flags (only minus is supported now).
    while at(fmt, cp) == b'-' {
        spec.flags |= TEXT_FORMAT_FLAG_MINUS;
        cp = advance_parse_pointer(cp, end)?;
    }

    if at(fmt, cp) == b'*' {
        // C: Handle indirect width.
        cp = advance_parse_pointer(cp, end)?;
        let (newcp, found, n) = text_format_parse_digits(fmt, cp)?;
        cp = newcp;
        if found {
            // C: number in this position must be closed by $.
            if at(fmt, cp) != b'$' {
                return Err(width_position_unterminated());
            }
            spec.widthpos = n;
            if n == 0 {
                return Err(argument_zero());
            }
            cp = advance_parse_pointer(cp, end)?;
        } else {
            // C: width's argument position is unspecified.
            spec.widthpos = 0;
        }
    } else {
        // C: Check for direct width specification.
        let (newcp, found, n) = text_format_parse_digits(fmt, cp)?;
        cp = newcp;
        if found {
            spec.width = n;
        }
    }

    // C: cp should now be pointing at the type character.
    Ok((cp, spec))
}

/// C: `text_format_append_string(buf, str, flags, width)` (varlena.c:6350-6393)
/// — append `str` to `buf`, padding/justifying to `width` per flags
/// (char-length aware).
pub fn text_format_append_string<'mcx>(
    buf: &mut PgVec<'mcx, u8>,
    str: &[u8],
    flags: i32,
    width: i32,
) -> PgResult<()> {
    let mut align_to_left = false;
    let mut width = width;

    // C: fast path for typical easy case.
    if width == 0 {
        append_bytes(buf, str)?;
        return Ok(());
    }

    if width < 0 {
        // C: Negative width: implicit '-' flag, then take absolute value.
        align_to_left = true;
        // C: -INT_MIN is undefined; if (width <= INT_MIN) ereport. For i32 this
        // can only hold at exactly i32::MIN.
        if width == i32::MIN {
            return Err(number_out_of_range());
        }
        width = -width;
    } else if flags & TEXT_FORMAT_FLAG_MINUS != 0 {
        align_to_left = true;
    }

    // C: len = pg_mbstrlen(str).
    let len = mb::pg_mbstrlen_with_len::call(str, str.len() as i32)?;
    if align_to_left {
        // C: left justify.
        append_bytes(buf, str)?;
        if len < width {
            append_spaces(buf, width - len)?;
        }
    } else {
        // C: right justify.
        if len < width {
            append_spaces(buf, width - len)?;
        }
        append_bytes(buf, str)?;
    }

    Ok(())
}

/// C: `text_format_string_conversion(buf, conversion, ...)`
/// (varlena.c:6301-6345) — format one `%s`/`%I`/`%L` value into `buf` (NULL
/// handling, quote escaping via owner seams).
pub fn text_format_string_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut PgVec<'mcx, u8>,
    conversion: u8,
    arg: &FormatArg<'mcx>,
    flags: i32,
    width: i32,
) -> PgResult<()> {
    // C:6309-6321 Handle NULL arguments before stringifying.
    if arg.is_null {
        if conversion == b's' {
            text_format_append_string(buf, b"", flags, width)?;
        } else if conversion == b'L' {
            text_format_append_string(buf, b"NULL", flags, width)?;
        } else if conversion == b'I' {
            return Err(null_identifier());
        }
        return Ok(());
    }

    // C:6324 str = OutputFunctionCall(typOutputInfo, value).
    let str = output_function_call(mcx, arg.typid, &arg.value)?;

    // C:6326-6341 Escape.
    if conversion == b'I' {
        // C:6330 quote_identifier(str).
        let q = quote_identifier(mcx, &str)?;
        text_format_append_string(buf, &q, flags, width)?;
    } else if conversion == b'L' {
        // C:6334 quote_literal_cstr(str).
        let qstr = quote_literal_cstr(mcx, &str)?;
        text_format_append_string(buf, &qstr, flags, width)?;
    } else {
        text_format_append_string(buf, &str, flags, width)?;
    }

    Ok(())
}

/// C: `text_format(PG_FUNCTION_ARGS)` (varlena.c:5898-6163) — scan the format
/// string, dispatch each conversion, and assemble the result text. Returns
/// `None` when the format string is SQL NULL.
///
/// `args` is the already-expanded argument list (argument position 1 maps to
/// `args[0]`): the variadic-array deconstruction of the C original is the fmgr/
/// Datum boundary's job (it produces the per-element `FormatArg`s).
pub fn text_format<'mcx>(
    mcx: Mcx<'mcx>,
    fmt: Option<&[u8]>,
    args: &[FormatArg<'mcx>],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C:5918-5920 When format string is null, immediately return null.
    let fmt_bytes = match fmt {
        None => return Ok(None),
        Some(f) => f,
    };

    // C:5961/5967 nargs counts the format string + value args.
    let nargs = args.len() as i32 + 1;

    // C:5973-5974 start_ptr / end_ptr.
    let end_ptr = fmt_bytes.len();

    // C:5975 initStringInfo(&str).
    let mut str: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    // C:5976 arg = 1; (next argument position to print).
    let mut arg: i32 = 1;

    // C:5979-6150 Scan format string.
    let mut cp = 0usize;
    while cp < end_ptr {
        // C:5993-5997 not a conversion: copy out.
        if fmt_bytes[cp] != b'%' {
            append_byte(&mut str, fmt_bytes[cp])?;
            cp += 1;
            continue;
        }

        // C:5999 ADVANCE_PARSE_POINTER(cp, end_ptr).
        cp = advance_parse_pointer(cp, end_ptr)?;

        // C:6002-6006 Easy case: %% outputs a single %.
        if fmt_bytes[cp] == b'%' {
            append_byte(&mut str, fmt_bytes[cp])?;
            cp += 1;
            continue;
        }

        // C:6009-6011 Parse the optional portions.
        let (newcp, spec) = text_format_parse_format(fmt_bytes, cp)?;
        cp = newcp;
        let argpos = spec.argpos;
        let widthpos = spec.widthpos;
        let flags = spec.flags;
        let mut width = spec.width;

        // C:6021-6026 main conversion specifier must be one of s/I/L.
        if !matches!(fmt_bytes[cp], b's' | b'I' | b'L') {
            return Err(unrecognized_specifier(fmt_bytes, cp)?);
        }

        // C:6029-6086 If indirect width was specified, get its value.
        if widthpos >= 0 {
            // C:6032-6033 collect specified or next arg position.
            if widthpos > 0 {
                arg = widthpos;
            }
            if arg >= nargs {
                return Err(too_few_arguments());
            }

            // C:6040-6053 get the value/type of the selected argument.
            let cur = &args[(arg - 1) as usize];
            if !::types_core::OidIsValid(cur.typid) {
                return Err(could_not_determine_type());
            }

            arg += 1;

            // C:6057-6085 NULL width == 0; int4/int2 fast paths; else stringify.
            if cur.is_null {
                width = 0;
            } else if cur.typid == INT4OID {
                width = datum_get_int32(&cur.value);
            } else if cur.typid == INT2OID {
                width = datum_get_int16(&cur.value) as i32;
            } else {
                // C:6079 str = OutputFunctionCall(&typoutputinfo_width, value).
                let s = output_function_call(mcx, cur.typid, &cur.value)?;
                // C:6082 width = pg_strtoint32(str).
                width = pg_strtoint32(&s)?;
            }
        }

        // C:6089-6090 collect specified or next arg position.
        if argpos > 0 {
            arg = argpos;
        }
        if arg >= nargs {
            return Err(too_few_arguments());
        }

        // C:6097-6110 get value/type of selected argument.
        let cur = &args[(arg - 1) as usize];
        if !::types_core::OidIsValid(cur.typid) {
            return Err(could_not_determine_type());
        }

        arg += 1;

        // C:6119-6149 format the value (the C output-fn reuse cache is the fmgr
        // deferral; the owner seam dispatches per typid).
        match fmt_bytes[cp] {
            b's' | b'I' | b'L' => {
                text_format_string_conversion(mcx, &mut str, fmt_bytes[cp], cur, flags, width)?;
            }
            _ => {
                // C:6142-6147 should not get here (checked above).
                return Err(unrecognized_specifier(fmt_bytes, cp)?);
            }
        }

        cp += 1;
    }

    // C:6158-6160 Generate results.
    cstring_to_text_with_len(mcx, &str, str.len() as i32).map(Some)
}

/// C: `text_format_nv(PG_FUNCTION_ARGS)` (varlena.c:6402-6406) — nonvariadic
/// wrapper around `text_format` (exists only for opr_sanity).
pub fn text_format_nv<'mcx>(
    mcx: Mcx<'mcx>,
    fmt: Option<&[u8]>,
    args: &[FormatArg<'mcx>],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C: return text_format(fcinfo).
    text_format(mcx, fmt, args)
}

// ---------------------------------------------------------------------------
// fmgr/Datum value-layer helpers for the format() integer fast paths.
// ---------------------------------------------------------------------------

/// C: `DatumGetInt32(value)` (postgres.h) — the low 32 bits of the Datum.
fn datum_get_int32(value: &Datum<'_>) -> i32 {
    value.as_i32()
}

/// C: `DatumGetInt16(value)` (postgres.h) — the low 16 bits of the Datum.
fn datum_get_int16(value: &Datum<'_>) -> i16 {
    value.as_i16()
}

#[cfg(test)]
mod format_tests {
    use super::*;

    fn null_arg<'mcx>(typid: Oid) -> FormatArg<'mcx> {
        FormatArg { value: Datum::null(), is_null: true, typid }
    }

    // C: SELECT format('%L', NULL::text) -> "NULL". The %L NULL leg
    // (varlena.c:6313) is now live (no OutputFunctionCall needed); width 0 so
    // the append fast path avoids the mb seam.
    #[test]
    fn format_percent_l_null_yields_null_keyword() {
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        // text-typed NULL argument; TEXTOID = 25.
        let out = text_format(mcx, Some(b"%L"), &[null_arg(25)]).unwrap().unwrap();
        assert_eq!(&out[..], b"NULL");
    }

    // The pg_strtoint32 owner-call helper rejects garbage (numutils ereport),
    // surfaced as Err — proving the stand-in panic is gone.
    #[test]
    fn pg_strtoint32_helper_rejects_garbage() {
        assert!(pg_strtoint32(b"42").is_ok());
        assert_eq!(pg_strtoint32(b"42").unwrap(), 42);
        assert!(pg_strtoint32(b"notanumber").is_err());
    }

    // The quote_literal_cstr owner-call helper now produces a real quoted
    // literal via the merged quote.c seam (must be installed by init_seams).
    #[test]
    fn quote_literal_cstr_helper_quotes() {
        quote::init_seams();
        let ctx = ::mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        let q = quote_literal_cstr(mcx, b"it's").unwrap();
        assert_eq!(&q[..], b"'it''s'");
    }
}
