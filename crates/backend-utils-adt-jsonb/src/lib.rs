//! Idiomatic port of PostgreSQL's `jsonb.c` — the SQL-facing `jsonb` type
//! input/output, builders, aggregates, casts, and text rendering.
//!
//! Mirrors `postgres-18.3/src/backend/utils/adt/jsonb.c` (PostgreSQL 18.3).
//!
//! The on-disk `jsonb` format and the `JsonbValue` tree are reused from
//! [`backend_utils_adt_jsonb_util`]; this crate is the SQL-facing layer on top
//! of it. The `escape_json_char` / `escape_json_with_len` text helpers are pure
//! and ported here 1:1 from `json.c` so `jsonb_out` is self-contained.
//!
//! # Reconciliation to this repo's model
//!
//! Outputs that the C builds in `CurrentMemoryContext` are returned as
//! `PgVec<'mcx, u8>` allocated in a caller-supplied [`Mcx`]; Datums are the
//! canonical [`types_tuple::Datum`]. Genuinely-external operations are routed
//! through per-owner seams: the JSON lexer/parser
//! ([`backend_utils_adt_jsonb_seams::parse_to_jsonb`]), `OidFunctionCall1`
//! ([`oid_function_call1`](backend_utils_adt_jsonb_seams::oid_function_call1)),
//! jsonb detoast
//! ([`jsonb_datum_bytes`](backend_utils_adt_jsonb_seams::jsonb_datum_bytes)),
//! and the `numeric`→int casts (`numeric_int2`/`numeric_int4`/`numeric_int8`);
//! the type-classification + output/cast/array/composite catalog half via
//! [`backend_utils_adt_jsonfuncs_seams`]; and datetime rendering via
//! [`backend_utils_adt_timestamp_seams::json_encode_datetime`].

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
// These ports keep C's `% N` parity checks verbatim (clippy prefers
// `is_multiple_of`, which would obscure the 1:1 correspondence).
#![allow(clippy::manual_is_multiple_of)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::error::{
    ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_PROTOCOL_VIOLATION,
};
use types_error::{PgError, PgResult};
use types_json::{JsonTokenType, JsonTypeCategory};
use types_tuple::heaptuple::{DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID};
use types_tuple::Datum;

use backend_utils_adt_jsonb_util::{
    json_container_is_array, json_container_is_object, json_container_is_scalar, jbvType,
    pushJsonbValue as jbu_pushJsonbValue, JsonbIteratorInit, JsonbIteratorNext, JsonbIteratorToken,
    JsonbParseState, JsonbValue, JsonbValueData, JsonbValueToJsonb, VARHDRSZ,
};

use backend_utils_adt_jsonb_seams as jsonb_seam;
use backend_utils_adt_jsonfuncs_seams as catalog_fmgr;
use backend_utils_adt_timestamp_seams as timestamp_seam;

// ---------------------------------------------------------------------------
// In-memory parse / aggregate state (C: JsonbInState / JsonbAggState).
//
// These hold the in-memory `JsonbValue` tree (never on disk, no `Mcx`); they
// are crate-internal scaffolding for the parse-into-tree and Datum-render
// paths, not referenced by any seam, so they live here in the owner.
// ---------------------------------------------------------------------------

/// C: `JsonbInState` — semantic-action state while parsing text (or rendering a
/// Datum) into a `JsonbValue` tree.
#[derive(Debug, Default)]
pub struct JsonbInState {
    /// The parse-state stack (C: `JsonbParseState *parseState`).
    pub parse_state: Option<Box<JsonbParseState>>,
    /// The resulting root value (C: `JsonbValue *res`).
    pub res: Option<JsonbValue>,
    /// Whether to enforce unique object keys (C: `bool unique_keys`).
    pub unique_keys: bool,
}

/// State carried across `jsonb_agg` / `jsonb_object_agg` transitions
/// (C: `JsonbAggState`). The element categorization is resolved once on the
/// first call.
#[derive(Debug, Default)]
pub struct JsonbAggState {
    pub res: JsonbInState,
    pub key_category: Option<JsonTypeCategory>,
    pub key_output_func: Oid,
    pub val_category: Option<JsonTypeCategory>,
    pub val_output_func: Oid,
}

/// `JENTRY_OFFLENMASK` — the per-string length limit checked by `checkStringLen`.
const JENTRY_OFFLENMASK: usize = 0x0FFF_FFFF;

/// `'i'`, `PROVOLATILE_IMMUTABLE`.
const PROVOLATILE_IMMUTABLE: u8 = b'i';

/// Wires every inward seam this crate owns. The catalog/fmgr/datetime boundary
/// seams it *consumes* are installed by their owners (jsonfuncs / timestamp /
/// the jsonapi parser).
pub fn init_seams() {
    fmgr_builtins::register_jsonb_builtins();
    backend_optimizer_util_clauses_seams::to_jsonb_is_immutable::set(to_jsonb_is_immutable);

    // `OidFunctionCall1(outfuncoid, val)` (JSONTYPE_CAST arm) — `fmgr.c`
    // resolves an `FmgrInfo` from `outfuncoid` and runs the cast under the
    // default (invalid) collation. Delegate to the fmgr-core
    // `function_call1_coll_datum` seam (the real `fmgr.c` owner) over the
    // canonical `Datum` lane.
    jsonb_seam::oid_function_call1::set(seam_oid_function_call1);

    // `DatumGetJsonbP(val)` = `PG_DETOAST_DATUM(val)` (JSONTYPE_JSONB arm) —
    // detoast the on-disk `jsonb` varlena via the `detoast_attr` seam.
    jsonb_seam::jsonb_datum_bytes::set(seam_jsonb_datum_bytes);
}

/// `OidFunctionCall1(outfuncoid, val)` (fmgr.c): resolve the cast function and
/// invoke it under `InvalidOid` collation, returning its resulting `Datum`.
fn seam_oid_function_call1<'mcx>(
    mcx: Mcx<'mcx>,
    outfuncoid: Oid,
    val: &Datum<'mcx>,
) -> PgResult<Datum<'mcx>> {
    backend_utils_fmgr_fmgr_seams::function_call1_coll_datum::call(
        mcx,
        outfuncoid,
        types_core::InvalidOid,
        val.clone_in(mcx)?,
    )
}

/// `DatumGetJsonbP(val)` = `PG_DETOAST_DATUM(val)`: return a de-TOASTed copy of
/// the `jsonb` varlena image (length header + root container) in `mcx`.
fn seam_jsonb_datum_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    val: &Datum<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    backend_access_common_detoast_seams::detoast_attr::call(mcx, val.as_ref_bytes())
}

pub mod fmgr_builtins;

// ===========================================================================
// I/O entry points.
// ===========================================================================

/// C: `jsonb_in(PG_FUNCTION_ARGS)` — parse a NUL-terminated cstring into an
/// on-disk jsonb varlena.
pub fn jsonb_in<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    jsonb_from_cstring(mcx, input, false)
}

/// C: `jsonb_recv(PG_FUNCTION_ARGS)` — binary recv: a 1-byte version followed by
/// the JSON text. Only version 1 is supported.
pub fn jsonb_recv<'mcx>(mcx: Mcx<'mcx>, buf: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // C: version = pq_getmsgint(buf, 1).
    if buf.is_empty() {
        return Err(PgError::error("insufficient data left in message")
            .with_sqlstate(ERRCODE_PROTOCOL_VIOLATION));
    }
    let version = buf[0] as i32;
    if version == 1 {
        // C: str = pq_getmsgtext(buf, len - cursor, &nbytes); the remaining
        // message bytes are the JSON text (encoding-converted by libpq before
        // we see them).
        jsonb_from_cstring(mcx, &buf[1..], false)
    } else {
        Err(PgError::error(format!("unsupported jsonb version number {}", version))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR))
    }
}

/// C: `jsonb_out(PG_FUNCTION_ARGS)` — render an on-disk jsonb varlena to text.
pub fn jsonb_out<'mcx>(mcx: Mcx<'mcx>, jsonb: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // C: JsonbToCString(NULL, &jb->root, VARSIZE(jb)).
    JsonbToCString(mcx, &jsonb[VARHDRSZ..], jsonb.len() as i32)
}

/// C: `jsonb_send(PG_FUNCTION_ARGS)` — binary send: version byte then text.
pub fn jsonb_send<'mcx>(mcx: Mcx<'mcx>, jsonb: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let jtext = JsonbToCString(mcx, &jsonb[VARHDRSZ..], jsonb.len() as i32)?;

    // C: pq_begintypsend; pq_sendint8(version=1); pq_sendtext(jtext); endtypsend.
    let mut buf = PgVec::with_capacity_in(1 + jtext.len(), mcx);
    buf.push(1u8);
    buf.extend_from_slice(&jtext);
    Ok(buf)
}

/// C: `jsonb_from_text(text *js, bool unique_keys)`.
pub fn jsonb_from_text<'mcx>(
    mcx: Mcx<'mcx>,
    js: &[u8],
    unique_keys: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    jsonb_from_cstring(mcx, js, unique_keys)
}

/// C: `jsonb_from_cstring(char *json, int len, bool unique_keys, Node
/// *escontext)`. The parser + `jsonb_in_*` semantic actions live in the jsonapi
/// subsystem, so this funnels through the seam.
fn jsonb_from_cstring<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    unique_keys: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    jsonb_seam::parse_to_jsonb::call(mcx, json, unique_keys)
}

// ---------------------------------------------------------------------------
// Semantic-action callbacks driving `pushJsonbValue` during a parse.
//
// In C these are the `JsonSemAction` hooks invoked by `pg_parse_json`. The
// lexer/parser is a sibling subsystem (jsonapi), so the `parse_to_jsonb` seam
// owns the parse loop; it calls these ported callbacks to do the actual work —
// each is the C body 1:1 over the shared `JsonbInState`. They are public so a
// provider can reuse them. The `numeric_in` for `JSON_TOKEN_NUMBER` needs an
// arena, so the scalar callback threads the caller's `Mcx`.
// ---------------------------------------------------------------------------

/// C: `jsonb_in_object_start(void *pstate)`.
pub fn jsonb_in_object_start(state: &mut JsonbInState) -> PgResult<()> {
    state.res = pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;
    let unique = state.unique_keys;
    if let Some(ps) = state.parse_state.as_mut() {
        ps.unique_keys = unique;
    }
    Ok(())
}

/// C: `jsonb_in_object_end(void *pstate)`.
pub fn jsonb_in_object_end(state: &mut JsonbInState) -> PgResult<()> {
    state.res = pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_END_OBJECT, None)?;
    Ok(())
}

/// C: `jsonb_in_array_start(void *pstate)`.
pub fn jsonb_in_array_start(state: &mut JsonbInState) -> PgResult<()> {
    state.res = pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_BEGIN_ARRAY, None)?;
    Ok(())
}

/// C: `jsonb_in_array_end(void *pstate)`.
pub fn jsonb_in_array_end(state: &mut JsonbInState) -> PgResult<()> {
    state.res = pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
    Ok(())
}

/// C: `jsonb_in_object_field_start(void *pstate, char *fname, bool isnull)`.
/// `fname` is the (de-escaped) field name.
pub fn jsonb_in_object_field_start(state: &mut JsonbInState, fname: &[u8]) -> PgResult<()> {
    // C: v.type = jbvString; v.val.string.len = strlen(fname).
    checkStringLen(fname.len())?;
    let v = JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(fname.to_vec()),
    };
    state.res = pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_KEY, Some(&v))?;
    Ok(())
}

/// C: `jsonb_in_scalar(void *pstate, char *token, JsonTokenType tokentype)`.
pub fn jsonb_in_scalar<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut JsonbInState,
    token: Option<&[u8]>,
    tokentype: JsonTokenType,
) -> PgResult<()> {
    use JsonTokenType::*;
    let v = match tokentype {
        JSON_TOKEN_STRING => {
            let t = token.ok_or_else(|| {
                PgError::error("jsonb_in_scalar: JSON_TOKEN_STRING carries a token")
            })?;
            checkStringLen(t.len())?;
            JsonbValue {
                typ: jbvType::jbvString,
                val: JsonbValueData::String(t.to_vec()),
            }
        }
        JSON_TOKEN_NUMBER => {
            let t = token.ok_or_else(|| {
                PgError::error("jsonb_in_scalar: JSON_TOKEN_NUMBER carries a token")
            })?;
            let text = core::str::from_utf8(t).map_err(|_| {
                PgError::error("invalid numeric token").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            })?;
            let bytes = numeric_in_to_bytes(mcx, text)?;
            JsonbValue {
                typ: jbvType::jbvNumeric,
                val: JsonbValueData::Numeric(bytes),
            }
        }
        JSON_TOKEN_TRUE => JsonbValue {
            typ: jbvType::jbvBool,
            val: JsonbValueData::Bool(true),
        },
        JSON_TOKEN_FALSE => JsonbValue {
            typ: jbvType::jbvBool,
            val: JsonbValueData::Bool(false),
        },
        JSON_TOKEN_NULL => JsonbValue::null(),
        _ => return Err(elog_internal("invalid json token type")),
    };

    if state.parse_state.is_none() {
        // single scalar
        let va = JsonbValue {
            typ: jbvType::jbvArray,
            val: JsonbValueData::Array {
                elems: Vec::new(),
                raw_scalar: true,
            },
        };
        state.res =
            pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_BEGIN_ARRAY, Some(&va))?;
        state.res = pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_ELEM, Some(&v))?;
        state.res = pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
    } else {
        let parent = state
            .parse_state
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_in_scalar: parse_state is NULL"))?
            .cont_val
            .typ;
        match parent {
            jbvType::jbvArray => {
                state.res =
                    pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_ELEM, Some(&v))?;
            }
            jbvType::jbvObject => {
                state.res =
                    pushJsonbValue(&mut state.parse_state, JsonbIteratorToken::WJB_VALUE, Some(&v))?;
            }
            _ => return Err(elog_internal("unexpected parent of nested structure")),
        }
    }
    Ok(())
}

// ===========================================================================
// Type-name helpers.
// ===========================================================================

/// C: `JsonbContainerTypeName(JsonbContainer *jbc)` — container is the bytes
/// starting at the container header word.
pub fn JsonbContainerTypeName(jbc: &[u8]) -> PgResult<&'static str> {
    let mut scalar = JsonbValue::null();
    if JsonbExtractScalar(jbc, &mut scalar)? {
        JsonbTypeName(&scalar)
    } else if json_container_is_array(container_header(jbc)) {
        Ok("array")
    } else if json_container_is_object(container_header(jbc)) {
        Ok("object")
    } else {
        Err(PgError::error(format!(
            "invalid jsonb container type: 0x{:08x}",
            container_header(jbc)
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR))
    }
}

/// C: `JsonbTypeName(JsonbValue *val)`.
pub fn JsonbTypeName(val: &JsonbValue) -> PgResult<&'static str> {
    match val.typ {
        jbvType::jbvBinary => match &val.val {
            JsonbValueData::Binary { data, .. } => JsonbContainerTypeName(data),
            _ => unreachable!(),
        },
        jbvType::jbvObject => Ok("object"),
        jbvType::jbvArray => Ok("array"),
        jbvType::jbvNumeric => Ok("number"),
        jbvType::jbvString => Ok("string"),
        jbvType::jbvBool => Ok("boolean"),
        jbvType::jbvNull => Ok("null"),
        jbvType::jbvDatetime => match &val.val {
            JsonbValueData::Datetime(dt) => match dt.typid {
                DATEOID => Ok("date"),
                TIMEOID => Ok("time without time zone"),
                TIMETZOID => Ok("time with time zone"),
                TIMESTAMPOID => Ok("timestamp without time zone"),
                TIMESTAMPTZOID => Ok("timestamp with time zone"),
                other => Err(PgError::error(format!(
                    "unrecognized jsonb value datetime type: {}",
                    other
                ))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR)),
            },
            _ => unreachable!(),
        },
    }
}

/// C: `jsonb_typeof(PG_FUNCTION_ARGS)` -> text. Returns the type name string.
pub fn jsonb_typeof(jsonb: &[u8]) -> PgResult<&'static str> {
    JsonbContainerTypeName(&jsonb[VARHDRSZ..])
}

// ===========================================================================
// Text rendering: JsonbToCString[Indent] + worker.
// ===========================================================================

/// C: `JsonbToCString(StringInfo out, JsonbContainer *in, int estimated_len)`.
///
/// The C `out` is a `StringInfo` in `CurrentMemoryContext`; here the rendered
/// bytes are built directly into a `PgVec` allocated in `mcx` and returned.
pub fn JsonbToCString<'mcx>(
    mcx: Mcx<'mcx>,
    container: &[u8],
    estimated_len: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    JsonbToCStringWorker(mcx, container, estimated_len, false)
}

/// C: `JsonbToCStringIndent(StringInfo out, JsonbContainer *in, int
/// estimated_len)`.
pub fn JsonbToCStringIndent<'mcx>(
    mcx: Mcx<'mcx>,
    container: &[u8],
    estimated_len: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    JsonbToCStringWorker(mcx, container, estimated_len, true)
}

/// C: `JsonbToCStringWorker(StringInfo out, JsonbContainer *in, int
/// estimated_len, bool indent)`.
fn JsonbToCStringWorker<'mcx>(
    mcx: Mcx<'mcx>,
    container: &[u8],
    estimated_len: i32,
    indent: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    use JsonbIteratorToken::*;

    let mut first = true;
    // `typ` persists across a `redo_switch` iteration (C declares it outside the
    // loop and the `goto redo_switch` reuses its current value).
    let mut typ;
    let mut level: i32 = 0;
    let mut redo_switch = false;

    // If we are indenting, don't add a space after a comma (C: ispaces = indent
    // ? 1 : 2, used as the byte-count of the ", " literal).
    let ispaces: &[u8] = b", ";
    let comma_len = if indent { 1 } else { 2 };

    let mut use_indent = false;
    let mut raw_scalar = false;
    let mut last_was_key = false;

    // C: enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64).
    let want = if estimated_len >= 0 { estimated_len as usize } else { 64 };
    let mut buf: PgVec<'mcx, u8> = PgVec::with_capacity_in(want, mcx);

    let mut it = JsonbIteratorInit(container);
    let mut v = JsonbValue::null();
    typ = WJB_DONE;

    loop {
        if !redo_switch {
            typ = JsonbIteratorNext(&mut it, &mut v, false)?;
            if typ == WJB_DONE {
                break;
            }
        }
        // On a redo, `typ` keeps the WJB_BEGIN_OBJECT/WJB_BEGIN_ARRAY value the
        // WJB_KEY arm left it at (C's `goto redo_switch`); `v` is unchanged.
        redo_switch = false;

        match typ {
            WJB_BEGIN_ARRAY => {
                if !first {
                    buf.extend_from_slice(&ispaces[..comma_len]);
                }
                let rs = matches!(&v.val, JsonbValueData::Array { raw_scalar, .. } if *raw_scalar);
                if !rs {
                    add_indent(&mut buf, use_indent && !last_was_key, level);
                    buf.push(b'[');
                } else {
                    raw_scalar = true;
                }
                first = true;
                level += 1;
            }
            WJB_BEGIN_OBJECT => {
                if !first {
                    buf.extend_from_slice(&ispaces[..comma_len]);
                }
                add_indent(&mut buf, use_indent && !last_was_key, level);
                buf.push(b'{');
                first = true;
                level += 1;
            }
            WJB_KEY => {
                if !first {
                    buf.extend_from_slice(&ispaces[..comma_len]);
                }
                first = true;
                add_indent(&mut buf, use_indent, level);
                // json rules guarantee this is a string.
                jsonb_put_escaped_value(mcx, &mut buf, &v)?;
                buf.extend_from_slice(b": ");

                typ = JsonbIteratorNext(&mut it, &mut v, false)?;
                if typ == WJB_VALUE {
                    first = false;
                    jsonb_put_escaped_value(mcx, &mut buf, &v)?;
                } else {
                    debug_assert!(typ == WJB_BEGIN_OBJECT || typ == WJB_BEGIN_ARRAY);
                    // Rerun the switch to output the container we just got.
                    redo_switch = true;
                }
            }
            WJB_ELEM => {
                if !first {
                    buf.extend_from_slice(&ispaces[..comma_len]);
                }
                first = false;
                if !raw_scalar {
                    add_indent(&mut buf, use_indent, level);
                }
                jsonb_put_escaped_value(mcx, &mut buf, &v)?;
            }
            WJB_END_ARRAY => {
                level -= 1;
                if !raw_scalar {
                    add_indent(&mut buf, use_indent, level);
                    buf.push(b']');
                }
                first = false;
            }
            WJB_END_OBJECT => {
                level -= 1;
                add_indent(&mut buf, use_indent, level);
                buf.push(b'}');
                first = false;
            }
            WJB_VALUE | WJB_DONE => {
                // C: default -> elog(ERROR, "unknown jsonb iterator token type").
                // WJB_VALUE is consumed inside the WJB_KEY arm, so it never
                // reaches here at top level; WJB_DONE breaks the loop.
                return Err(unknown_token());
            }
        }
        use_indent = indent;
        last_was_key = redo_switch;
    }

    debug_assert_eq!(level, 0);
    Ok(buf)
}

/// C: `add_indent(StringInfo out, bool indent, int level)`.
fn add_indent(buf: &mut PgVec<'_, u8>, indent: bool, level: i32) {
    if indent {
        buf.push(b'\n');
        for _ in 0..(level * 4) {
            buf.push(b' ');
        }
    }
}

/// C: `jsonb_put_escaped_value(StringInfo out, JsonbValue *scalarVal)`.
fn jsonb_put_escaped_value<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut PgVec<'mcx, u8>,
    scalar_val: &JsonbValue,
) -> PgResult<()> {
    match &scalar_val.val {
        JsonbValueData::Null => buf.extend_from_slice(b"null"),
        JsonbValueData::String(s) => escape_json_with_len(buf, s),
        JsonbValueData::Numeric(num) => {
            let s = numeric_out(mcx, num)?;
            buf.extend_from_slice(s.as_bytes());
        }
        JsonbValueData::Bool(b) => {
            if *b {
                buf.extend_from_slice(b"true");
            } else {
                buf.extend_from_slice(b"false");
            }
        }
        _ => return Err(elog_internal("unknown jsonb scalar type")),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// escape_json helpers (json.c) — pure, ported 1:1 (jsonb_out is self-contained).
// ---------------------------------------------------------------------------

/// C: `escape_json_char(StringInfo buf, char c)`.
fn escape_json_char(buf: &mut PgVec<'_, u8>, c: u8) {
    match c {
        0x08 => buf.extend_from_slice(b"\\b"),
        0x0C => buf.extend_from_slice(b"\\f"),
        b'\n' => buf.extend_from_slice(b"\\n"),
        b'\r' => buf.extend_from_slice(b"\\r"),
        b'\t' => buf.extend_from_slice(b"\\t"),
        b'"' => buf.extend_from_slice(b"\\\""),
        b'\\' => buf.extend_from_slice(b"\\\\"),
        _ => {
            if c < b' ' {
                // appendStringInfo(buf, "\\u%04x", (int) c)
                let mut tmp = [b'\\', b'u', b'0', b'0', 0, 0];
                const HEX: &[u8; 16] = b"0123456789abcdef";
                tmp[4] = HEX[((c >> 4) & 0xF) as usize];
                tmp[5] = HEX[(c & 0xF) as usize];
                buf.extend_from_slice(&tmp);
            } else {
                buf.push(c);
            }
        }
    }
}

/// C: `escape_json_with_len(StringInfo buf, const char *str, int len)`. The C
/// version SIMD-scans; the byte-by-byte loop has identical semantics.
fn escape_json_with_len(buf: &mut PgVec<'_, u8>, str: &[u8]) {
    buf.push(b'"');
    for &c in str {
        escape_json_char(buf, c);
    }
    buf.push(b'"');
}

// ===========================================================================
// Scalar extraction + unquote.
// ===========================================================================

/// C: `JsonbExtractScalar(JsonbContainer *jbc, JsonbValue *res)` — `jbc` is the
/// container bytes starting at the header word.
pub fn JsonbExtractScalar(jbc: &[u8], res: &mut JsonbValue) -> PgResult<bool> {
    use JsonbIteratorToken::*;
    let header = container_header(jbc);

    if !json_container_is_array(header) || !json_container_is_scalar(header) {
        // Inform caller about actual type of container.
        res.typ = if json_container_is_array(header) {
            jbvType::jbvArray
        } else {
            jbvType::jbvObject
        };
        return Ok(false);
    }

    // A root scalar is stored as an array of one element.
    let mut it = JsonbIteratorInit(jbc);
    let mut tmp = JsonbValue::null();

    let tok = JsonbIteratorNext(&mut it, &mut tmp, true)?;
    debug_assert_eq!(tok, WJB_BEGIN_ARRAY);

    let tok = JsonbIteratorNext(&mut it, res, true)?;
    debug_assert_eq!(tok, WJB_ELEM);
    debug_assert!(res.is_scalar());

    let tok = JsonbIteratorNext(&mut it, &mut tmp, true)?;
    debug_assert_eq!(tok, WJB_END_ARRAY);

    let tok = JsonbIteratorNext(&mut it, &mut tmp, true)?;
    debug_assert_eq!(tok, WJB_DONE);

    Ok(true)
}

/// C: `JsonbUnquote(Jsonb *jb)` — `jb` is the full on-disk varlena bytes.
pub fn JsonbUnquote<'mcx>(mcx: Mcx<'mcx>, jb: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let root = &jb[VARHDRSZ..];
    if json_container_is_scalar(container_header(root)) {
        let mut v = JsonbValue::null();
        JsonbExtractScalar(root, &mut v)?;

        let bytes: Vec<u8> = match &v.val {
            JsonbValueData::String(s) => s.clone(),
            JsonbValueData::Bool(b) => {
                if *b {
                    b"true".to_vec()
                } else {
                    b"false".to_vec()
                }
            }
            JsonbValueData::Numeric(num) => numeric_out(mcx, num)?.into_bytes(),
            JsonbValueData::Null => b"null".to_vec(),
            _ => {
                return Err(PgError::error(format!(
                    "unrecognized jsonb value type {}",
                    v.typ as i32
                ))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR))
            }
        };
        let mut out = PgVec::with_capacity_in(bytes.len(), mcx);
        out.extend_from_slice(&bytes);
        Ok(out)
    } else {
        JsonbToCString(mcx, root, jb.len() as i32)
    }
}

// ===========================================================================
// Casts: jsonb -> bool / numeric / intN / floatN.
// ===========================================================================

/// C: `cannotCastJsonbValue(enum jbvType type, const char *sqltype)`.
fn cannotCastJsonbValue(typ: jbvType, sqltype: &str) -> PgError {
    let msg = match typ {
        jbvType::jbvNull => format!("cannot cast jsonb null to type {}", sqltype),
        jbvType::jbvString => format!("cannot cast jsonb string to type {}", sqltype),
        jbvType::jbvNumeric => format!("cannot cast jsonb numeric to type {}", sqltype),
        jbvType::jbvBool => format!("cannot cast jsonb boolean to type {}", sqltype),
        jbvType::jbvArray => format!("cannot cast jsonb array to type {}", sqltype),
        jbvType::jbvObject => format!("cannot cast jsonb object to type {}", sqltype),
        jbvType::jbvBinary => format!("cannot cast jsonb array or object to type {}", sqltype),
        other => {
            return elog_internal(&format!("unknown jsonb type: {}", other as i32));
        }
    };
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// Shared extract + type-gate for the scalar casts. Returns `Ok(None)` for a
/// jbvNull (C: `PG_RETURN_NULL()`), else the extracted `JsonbValue`.
fn cast_extract(jb: &[u8], sqltype: &str) -> PgResult<Option<JsonbValue>> {
    let mut v = JsonbValue::null();
    if !JsonbExtractScalar(&jb[VARHDRSZ..], &mut v)? {
        return Err(cannotCastJsonbValue(v.typ, sqltype));
    }
    if v.typ == jbvType::jbvNull {
        return Ok(None);
    }
    Ok(Some(v))
}

/// C: `jsonb_bool(PG_FUNCTION_ARGS)`.
pub fn jsonb_bool(jb: &[u8]) -> PgResult<Option<bool>> {
    let Some(v) = cast_extract(jb, "boolean")? else {
        return Ok(None);
    };
    if v.typ != jbvType::jbvBool {
        return Err(cannotCastJsonbValue(v.typ, "boolean"));
    }
    match v.val {
        JsonbValueData::Bool(b) => Ok(Some(b)),
        _ => unreachable!(),
    }
}

/// Extract the on-disk `numeric` bytes from a casts' scalar, gating type.
fn cast_numeric_bytes(jb: &[u8], sqltype: &str) -> PgResult<Option<Vec<u8>>> {
    let Some(v) = cast_extract(jb, sqltype)? else {
        return Ok(None);
    };
    if v.typ != jbvType::jbvNumeric {
        return Err(cannotCastJsonbValue(v.typ, sqltype));
    }
    match v.val {
        JsonbValueData::Numeric(n) => Ok(Some(n)),
        _ => unreachable!(),
    }
}

/// C: `jsonb_numeric(PG_FUNCTION_ARGS)` — returns the on-disk numeric bytes (a
/// copy, as the C makes via `DatumGetNumericCopy`).
pub fn jsonb_numeric<'mcx>(mcx: Mcx<'mcx>, jb: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match cast_numeric_bytes(jb, "numeric")? {
        Some(n) => {
            let mut out = PgVec::with_capacity_in(n.len(), mcx);
            out.extend_from_slice(&n);
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

/// C: `jsonb_int2(PG_FUNCTION_ARGS)`.
pub fn jsonb_int2(jb: &[u8]) -> PgResult<Option<i16>> {
    match cast_numeric_bytes(jb, "smallint")? {
        Some(n) => Ok(Some(jsonb_seam::numeric_int2::call(&n)?)),
        None => Ok(None),
    }
}

/// C: `jsonb_int4(PG_FUNCTION_ARGS)`.
pub fn jsonb_int4(jb: &[u8]) -> PgResult<Option<i32>> {
    match cast_numeric_bytes(jb, "integer")? {
        Some(n) => Ok(Some(jsonb_seam::numeric_int4::call(&n)?)),
        None => Ok(None),
    }
}

/// C: `jsonb_int8(PG_FUNCTION_ARGS)`.
pub fn jsonb_int8(jb: &[u8]) -> PgResult<Option<i64>> {
    match cast_numeric_bytes(jb, "bigint")? {
        Some(n) => Ok(Some(jsonb_seam::numeric_int8::call(&n)?)),
        None => Ok(None),
    }
}

/// C: `jsonb_float4(PG_FUNCTION_ARGS)`.
pub fn jsonb_float4(jb: &[u8]) -> PgResult<Option<f32>> {
    match cast_numeric_bytes(jb, "real")? {
        Some(n) => Ok(Some(backend_utils_adt_numeric::convert::numeric_to_float4(&n)?)),
        None => Ok(None),
    }
}

/// C: `jsonb_float8(PG_FUNCTION_ARGS)`.
pub fn jsonb_float8(jb: &[u8]) -> PgResult<Option<f64>> {
    match cast_numeric_bytes(jb, "double precision")? {
        Some(n) => Ok(Some(backend_utils_adt_numeric::convert::numeric_to_float8(&n)?)),
        None => Ok(None),
    }
}

// ===========================================================================
// to_jsonb / datum_to_jsonb and the Datum builders.
// ===========================================================================

/// C: `to_jsonb_is_immutable(Oid typoid)`.
pub fn to_jsonb_is_immutable(typoid: Oid) -> PgResult<bool> {
    let (tcategory, outfuncoid) = catalog_fmgr::categorize_type::call(typoid)?;

    match tcategory {
        JsonTypeCategory::JSONTYPE_NULL
        | JsonTypeCategory::JSONTYPE_BOOL
        | JsonTypeCategory::JSONTYPE_JSON
        | JsonTypeCategory::JSONTYPE_JSONB => Ok(true),

        JsonTypeCategory::JSONTYPE_DATE
        | JsonTypeCategory::JSONTYPE_TIMESTAMP
        | JsonTypeCategory::JSONTYPE_TIMESTAMPTZ => Ok(false),

        JsonTypeCategory::JSONTYPE_ARRAY => Ok(false), // TODO recurse into elements
        JsonTypeCategory::JSONTYPE_COMPOSITE => Ok(false), // TODO recurse into fields

        JsonTypeCategory::JSONTYPE_NUMERIC
        | JsonTypeCategory::JSONTYPE_CAST
        | JsonTypeCategory::JSONTYPE_OTHER => {
            Ok(catalog_fmgr::func_volatile::call(outfuncoid)? == PROVOLATILE_IMMUTABLE)
        }
    }
}

/// C: `to_jsonb(PG_FUNCTION_ARGS)` — classify `val_type` then render `val`.
pub fn to_jsonb<'mcx>(mcx: Mcx<'mcx>, val: &Datum<'mcx>, val_type: Oid) -> PgResult<PgVec<'mcx, u8>> {
    if val_type == 0 {
        return Err(PgError::error("could not determine input data type")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    let (tcategory, outfuncoid) = catalog_fmgr::categorize_type::call(val_type)?;
    datum_to_jsonb(mcx, val, tcategory, outfuncoid)
}

/// C: `datum_to_jsonb_internal(Datum val, bool is_null, JsonbInState *result,
/// JsonTypeCategory tcategory, Oid outfuncoid, bool key_scalar)`.
///
/// Ported 1:1. Only the genuinely external pieces are seamed: `OidFunctionCall1`
/// (JSONTYPE_CAST), `OidOutputFunctionCall` (JSONTYPE_NUMERIC/default),
/// `JsonEncodeDateTime` (datetimes), the array/composite catalog half, the JSON
/// lexer for the JSONTYPE_JSON/CAST text-parse tail, and the JSONTYPE_JSONB
/// detoast.
pub fn datum_to_jsonb_internal<'mcx>(
    mcx: Mcx<'mcx>,
    val: &Datum<'mcx>,
    is_null: bool,
    result: &mut JsonbInState,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
    key_scalar: bool,
) -> PgResult<()> {
    use JsonTypeCategory::*;

    // C: check_stack_depth() — array/composite rendering recurses, so guard the
    // execution stack (ERRCODE_STATEMENT_TOO_COMPLEX) before descending.
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    // Convert val to a JsonbValue in jb (in most cases).
    let mut jb = JsonbValue::null();
    let mut scalar_jsonb = false;

    if is_null {
        debug_assert!(!key_scalar);
        jb = JsonbValue::null();
    } else if key_scalar
        && (tcategory == JSONTYPE_ARRAY
            || tcategory == JSONTYPE_COMPOSITE
            || tcategory == JSONTYPE_JSON
            || tcategory == JSONTYPE_JSONB
            || tcategory == JSONTYPE_CAST)
    {
        return Err(PgError::error("key value must be scalar, not array, composite, or json")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    } else {
        // For JSONTYPE_CAST the cast yields a new (json/jsonb) Datum we then
        // dispatch on below; hold it so the borrow outlives the match.
        let cast_val;
        let val: &Datum<'mcx> = if tcategory == JSONTYPE_CAST {
            cast_val = jsonb_seam::oid_function_call1::call(mcx, outfuncoid, val)?;
            &cast_val
        } else {
            val
        };

        match tcategory {
            JSONTYPE_ARRAY => {
                array_to_jsonb_internal(mcx, val, result)?;
            }
            JSONTYPE_COMPOSITE => {
                composite_to_jsonb(mcx, val, result)?;
            }
            JSONTYPE_BOOL => {
                if key_scalar {
                    // outputstr = DatumGetBool(val) ? "true" : "false"; quoted key.
                    let outputstr: &[u8] = if val.as_bool() { b"true" } else { b"false" };
                    jb = JsonbValue {
                        typ: jbvType::jbvString,
                        val: JsonbValueData::String(outputstr.to_vec()),
                    };
                } else {
                    jb = JsonbValue {
                        typ: jbvType::jbvBool,
                        val: JsonbValueData::Bool(val.as_bool()),
                    };
                }
            }
            JSONTYPE_NUMERIC => {
                let outputstr = catalog_fmgr::output_function_call::call(mcx, outfuncoid, val)?;
                if key_scalar {
                    // always quote keys
                    jb = JsonbValue {
                        typ: jbvType::jbvString,
                        val: JsonbValueData::String(outputstr),
                    };
                } else {
                    // Make it numeric if it's a valid JSON number, otherwise a
                    // string. Invalid numeric output will always have an 'N' or
                    // 'n' in it (I think).
                    let numeric_error = outputstr.contains(&b'N') || outputstr.contains(&b'n');
                    if !numeric_error {
                        let text = core::str::from_utf8(&outputstr).map_err(|_| {
                            PgError::error("invalid numeric output")
                                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                        })?;
                        // jb.val.numeric = numeric_in(outputstr, InvalidOid, -1).
                        let bytes = numeric_in_to_bytes(mcx, text)?;
                        jb = JsonbValue {
                            typ: jbvType::jbvNumeric,
                            val: JsonbValueData::Numeric(bytes),
                        };
                    } else {
                        jb = JsonbValue {
                            typ: jbvType::jbvString,
                            val: JsonbValueData::String(outputstr),
                        };
                    }
                }
            }
            JSONTYPE_DATE => {
                let s = timestamp_seam::json_encode_datetime::call(val, DATEOID, None)?;
                jb = JsonbValue {
                    typ: jbvType::jbvString,
                    val: JsonbValueData::String(s.into_bytes()),
                };
            }
            JSONTYPE_TIMESTAMP => {
                let s = timestamp_seam::json_encode_datetime::call(val, TIMESTAMPOID, None)?;
                jb = JsonbValue {
                    typ: jbvType::jbvString,
                    val: JsonbValueData::String(s.into_bytes()),
                };
            }
            JSONTYPE_TIMESTAMPTZ => {
                let s = timestamp_seam::json_encode_datetime::call(val, TIMESTAMPTZOID, None)?;
                jb = JsonbValue {
                    typ: jbvType::jbvString,
                    val: JsonbValueData::String(s.into_bytes()),
                };
            }
            JSONTYPE_CAST | JSONTYPE_JSON => {
                // Parse the json right into the existing result object. In C this
                // drives the jsonb_in_* semantic actions over `result` directly;
                // the lexer/parser is the jsonapi subsystem, so the seam parses
                // the text into standalone jsonb bytes which are then spliced
                // into `result` by the iterator loop — an identical tree.
                let json = catalog_fmgr::text_datum_bytes::call(mcx, val)?;
                let parsed = jsonb_seam::parse_to_jsonb::call(mcx, &json, false)?;
                splice_jsonb_tokens(result, &parsed)?;
            }
            JSONTYPE_JSONB => {
                let jsonb = jsonb_seam::jsonb_datum_bytes::call(mcx, val)?;
                let root = &jsonb[VARHDRSZ..];
                let mut it = JsonbIteratorInit(root);
                if json_container_is_scalar(container_header(root)) {
                    // JB_ROOT_IS_SCALAR: pull WJB_BEGIN_ARRAY then WJB_ELEM.
                    let _ = JsonbIteratorNext(&mut it, &mut jb, true)?;
                    debug_assert_eq!(jb.typ, jbvType::jbvArray);
                    let _ = JsonbIteratorNext(&mut it, &mut jb, true)?;
                    scalar_jsonb = true;
                } else {
                    loop {
                        let mut v = JsonbValue::null();
                        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
                        if typ == JsonbIteratorToken::WJB_DONE {
                            break;
                        }
                        use JsonbIteratorToken::*;
                        if matches!(
                            typ,
                            WJB_END_ARRAY | WJB_END_OBJECT | WJB_BEGIN_ARRAY | WJB_BEGIN_OBJECT
                        ) {
                            result.res = pushJsonbValue(&mut result.parse_state, typ, None)?;
                        } else {
                            result.res = pushJsonbValue(&mut result.parse_state, typ, Some(&v))?;
                        }
                    }
                }
            }
            // C default: OidOutputFunctionCall + checkStringLen, as a string.
            JSONTYPE_NULL | JSONTYPE_OTHER => {
                let outputstr = catalog_fmgr::output_function_call::call(mcx, outfuncoid, val)?;
                checkStringLen(outputstr.len())?;
                jb = JsonbValue {
                    typ: jbvType::jbvString,
                    val: JsonbValueData::String(outputstr),
                };
            }
        }
    }

    // Now insert jb into result, unless we did it recursively.
    // C: `tcategory >= JSONTYPE_JSON && tcategory <= JSONTYPE_CAST` — the
    // contiguous block of recursive-work categories in `JsonTypeCategory`.
    let recursive = matches!(
        tcategory,
        JSONTYPE_JSON | JSONTYPE_JSONB | JSONTYPE_ARRAY | JSONTYPE_COMPOSITE | JSONTYPE_CAST
    );
    if !is_null && !scalar_jsonb && recursive {
        // Work has been done recursively (ARRAY/COMPOSITE/JSON/JSONB/CAST).
        Ok(())
    } else if result.parse_state.is_none() {
        // single root scalar
        let va = JsonbValue {
            typ: jbvType::jbvArray,
            val: JsonbValueData::Array {
                elems: Vec::new(),
                raw_scalar: true,
            },
        };
        result.res =
            pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_ARRAY, Some(&va))?;
        result.res =
            pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_ELEM, Some(&jb))?;
        result.res =
            pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
        Ok(())
    } else {
        let parent_type = result.parse_state.as_ref().unwrap().cont_val.typ;
        match parent_type {
            jbvType::jbvArray => {
                result.res =
                    pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_ELEM, Some(&jb))?;
            }
            jbvType::jbvObject => {
                let tok = if key_scalar {
                    JsonbIteratorToken::WJB_KEY
                } else {
                    JsonbIteratorToken::WJB_VALUE
                };
                result.res = pushJsonbValue(&mut result.parse_state, tok, Some(&jb))?;
            }
            _ => return Err(elog_internal("unexpected parent of nested structure")),
        }
        Ok(())
    }
}

/// C: `array_dim_to_jsonb(JsonbInState *result, int dim, int ndims, int *dims,
/// const Datum *vals, const bool *nulls, int *valcount, JsonTypeCategory
/// tcategory, Oid outfuncoid)`.
fn array_dim_to_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    result: &mut JsonbInState,
    dim: usize,
    ndims: usize,
    dims: &[i32],
    vals: &[Datum<'mcx>],
    nulls: &[bool],
    valcount: &mut usize,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
) -> PgResult<()> {
    debug_assert!(dim < ndims);

    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_ARRAY, None)?;

    let mut i = 1;
    while i <= dims[dim] {
        if dim + 1 == ndims {
            datum_to_jsonb_internal(
                mcx,
                &vals[*valcount],
                nulls[*valcount],
                result,
                tcategory,
                outfuncoid,
                false,
            )?;
            *valcount += 1;
        } else {
            array_dim_to_jsonb(
                mcx, result, dim + 1, ndims, dims, vals, nulls, valcount, tcategory, outfuncoid,
            )?;
        }
        i += 1;
    }

    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
    Ok(())
}

/// C: `array_to_jsonb_internal(Datum array, JsonbInState *result)`. The
/// `array.c`/catalog half (`get_typlenbyvalalign`, element classification,
/// `deconstruct_array`) is seamed; the structural `[ ... ]` assembly stays in-
/// crate.
fn array_to_jsonb_internal<'mcx>(
    mcx: Mcx<'mcx>,
    array: &Datum<'mcx>,
    result: &mut JsonbInState,
) -> PgResult<()> {
    let arr = catalog_fmgr::deconstruct_array::call(mcx, array)?;

    // nitems = ArrayGetNItems(ndim, dim). The overflow guard lives in the seam
    // (deconstruct_array owns ArrayGetNItems); here we recompute the product to
    // drive the `nitems <= 0` early-return.
    let nitems: i64 = if arr.ndim <= 0 {
        0
    } else {
        let mut n: i64 = 1;
        for &d in &arr.dims {
            n *= d as i64;
        }
        n
    };

    if nitems <= 0 {
        result.res =
            pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_ARRAY, None)?;
        result.res =
            pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
        return Ok(());
    }

    let mut count = 0usize;
    array_dim_to_jsonb(
        mcx,
        result,
        0,
        arr.ndim as usize,
        &arr.dims,
        &arr.elements,
        &arr.nulls,
        &mut count,
        arr.element_tcategory,
        arr.element_outfuncoid,
    )
}

/// C: `composite_to_jsonb(Datum composite, JsonbInState *result)`.
/// `lookup_rowtype_tupdesc`, `heap_getattr`, and the per-attribute
/// classification (the catalog half) are seamed via `walk_composite`; the
/// `{ ... }` assembly, the WJB_KEY pushes, and re-entry into
/// [`datum_to_jsonb_internal`] stay in-crate.
fn composite_to_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    composite: &Datum<'mcx>,
    result: &mut JsonbInState,
) -> PgResult<()> {
    let fields = catalog_fmgr::walk_composite::call(mcx, composite)?;

    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;

    for field in &fields {
        // (att->attisdropped fields are already filtered out by walk_composite,
        // matching the C `if (att->attisdropped) continue;`.)
        let v = JsonbValue {
            typ: jbvType::jbvString,
            // don't need checkStringLen here - can't exceed maximum name length
            val: JsonbValueData::String(field.attname.clone()),
        };
        result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_KEY, Some(&v))?;

        datum_to_jsonb_internal(
            mcx,
            &field.val,
            field.is_null,
            result,
            field.tcategory,
            field.outfuncoid,
            false,
        )?;
    }

    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_OBJECT, None)?;
    Ok(())
}

/// C: `datum_to_jsonb(Datum val, JsonTypeCategory tcategory, Oid outfuncoid)`.
pub fn datum_to_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    val: &Datum<'mcx>,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = JsonbInState::default();
    datum_to_jsonb_internal(mcx, val, false, &mut result, tcategory, outfuncoid, false)?;
    JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("datum_to_jsonb: result.res is NULL"))?,
    )
}

/// C: `jsonb_build_object_worker(int nargs, const Datum *args, const bool
/// *nulls, const Oid *types, bool absent_on_null, bool unique_keys)`.
pub fn jsonb_build_object_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum<'mcx>],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let nargs = args.len();
    if nargs % 2 != 0 {
        return Err(PgError::error("argument list must have even number of elements")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint(
                "The arguments of jsonb_build_object() must consist of alternating keys and values.",
            ));
    }

    let mut result = JsonbInState::default();
    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;
    if let Some(ps) = result.parse_state.as_mut() {
        ps.unique_keys = unique_keys;
        ps.skip_nulls = absent_on_null;
    }

    let mut i = 0;
    while i < nargs {
        if nulls[i] {
            return Err(PgError::error(format!("argument {}: key must not be null", i + 1))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        let skip = absent_on_null && nulls[i + 1];
        if skip && !unique_keys {
            i += 2;
            continue;
        }
        add_jsonb(mcx, &args[i], false, &mut result, types[i], true)?;
        add_jsonb(mcx, &args[i + 1], nulls[i + 1], &mut result, types[i + 1], false)?;
        i += 2;
    }

    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_OBJECT, None)?;
    JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_build_object_worker: result.res is NULL"))?,
    )
}

/// C: `jsonb_build_object_noargs(PG_FUNCTION_ARGS)`.
pub fn jsonb_build_object_noargs<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = JsonbInState::default();
    pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;
    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_OBJECT, None)?;
    JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_build_object_noargs: result.res is NULL"))?,
    )
}

/// C: `jsonb_build_object(PG_FUNCTION_ARGS)`. `extract_variadic_args` is the
/// executor boundary: the caller supplies the already-extracted variadic
/// arguments, or `None` for the negative-`nargs` `PG_RETURN_NULL()` case.
pub fn jsonb_build_object<'mcx>(
    mcx: Mcx<'mcx>,
    extracted: Option<(&[Datum<'mcx>], &[Oid], &[bool])>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match extracted {
        None => Ok(None),
        Some((args, types, nulls)) => {
            Ok(Some(jsonb_build_object_worker(mcx, args, nulls, types, false, false)?))
        }
    }
}

/// C: `jsonb_build_array_worker(int nargs, const Datum *args, const bool
/// *nulls, const Oid *types, bool absent_on_null)`.
pub fn jsonb_build_array_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum<'mcx>],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = JsonbInState::default();
    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_ARRAY, None)?;

    for i in 0..args.len() {
        if absent_on_null && nulls[i] {
            continue;
        }
        add_jsonb(mcx, &args[i], nulls[i], &mut result, types[i], false)?;
    }

    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
    JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_build_array_worker: result.res is NULL"))?,
    )
}

/// C: `jsonb_build_array_noargs(PG_FUNCTION_ARGS)`.
pub fn jsonb_build_array_noargs<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = JsonbInState::default();
    pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_ARRAY, None)?;
    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
    JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_build_array_noargs: result.res is NULL"))?,
    )
}

/// C: `jsonb_build_array(PG_FUNCTION_ARGS)`.
pub fn jsonb_build_array<'mcx>(
    mcx: Mcx<'mcx>,
    extracted: Option<(&[Datum<'mcx>], &[Oid], &[bool])>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match extracted {
        None => Ok(None),
        Some((args, types, nulls)) => {
            Ok(Some(jsonb_build_array_worker(mcx, args, nulls, types, false)?))
        }
    }
}

/// C: `add_jsonb(Datum val, bool is_null, JsonbInState *result, Oid val_type,
/// bool key_scalar)`. Classifies `val_type` then dispatches.
fn add_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    val: &Datum<'mcx>,
    is_null: bool,
    result: &mut JsonbInState,
    val_type: Oid,
    key_scalar: bool,
) -> PgResult<()> {
    if val_type == 0 {
        return Err(PgError::error("could not determine input data type")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let (tcategory, outfuncoid) = if is_null {
        (JsonTypeCategory::JSONTYPE_NULL, 0)
    } else {
        catalog_fmgr::categorize_type::call(val_type)?
    };

    datum_to_jsonb_internal(mcx, val, is_null, result, tcategory, outfuncoid, key_scalar)
}

/// Splice every iterator token of a standalone jsonb varlena into `result`'s
/// parse state (the JSONTYPE_JSON/CAST text-parse tail).
fn splice_jsonb_tokens(result: &mut JsonbInState, jsonb: &[u8]) -> PgResult<()> {
    use JsonbIteratorToken::*;
    let root = &jsonb[VARHDRSZ..];
    let mut it = JsonbIteratorInit(root);
    loop {
        let mut v = JsonbValue::null();
        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
        if typ == WJB_DONE {
            break;
        }
        if matches!(typ, WJB_END_ARRAY | WJB_END_OBJECT | WJB_BEGIN_ARRAY | WJB_BEGIN_OBJECT) {
            result.res = pushJsonbValue(&mut result.parse_state, typ, None)?;
        } else {
            result.res = pushJsonbValue(&mut result.parse_state, typ, Some(&v))?;
        }
    }
    Ok(())
}

// ===========================================================================
// jsonb_object(text[]) / jsonb_object(text[], text[]).
//
// The C functions call deconstruct_array_builtin (a catalog/array op); here we
// take the already-deconstructed text datums (each `Option<Vec<u8>>`, None ==
// SQL NULL) plus the original number of array dimensions, and port 1:1.
// ===========================================================================

/// C: `jsonb_object(PG_FUNCTION_ARGS)` — one or two dimensional text array of
/// name/value pairs.
pub fn jsonb_object<'mcx>(
    mcx: Mcx<'mcx>,
    ndims: i32,
    dims: &[i32],
    in_datums: &[Option<Vec<u8>>],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = JsonbInState::default();
    pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;

    match ndims {
        0 => {
            return close_object(mcx, &mut result);
        }
        1 => {
            if dims[0] % 2 != 0 {
                return Err(PgError::error("array must have even number of elements")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
        }
        2 => {
            if dims[1] != 2 {
                return Err(PgError::error("array must have two columns")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
        }
        _ => {
            return Err(PgError::error("wrong number of array subscripts")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
    }

    let count = in_datums.len() / 2;
    for i in 0..count {
        if in_datums[i * 2].is_none() {
            return Err(PgError::error("null value not allowed for object key")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        let key = in_datums[i * 2]
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_object: key datum is NULL"))?;
        let v = JsonbValue {
            typ: jbvType::jbvString,
            val: JsonbValueData::String(key.clone()),
        };
        pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_KEY, Some(&v))?;

        let v = match &in_datums[i * 2 + 1] {
            None => JsonbValue::null(),
            Some(s) => JsonbValue {
                typ: jbvType::jbvString,
                val: JsonbValueData::String(s.clone()),
            },
        };
        pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_VALUE, Some(&v))?;
    }

    close_object(mcx, &mut result)
}

/// C: `jsonb_object_two_arg(PG_FUNCTION_ARGS)`.
pub fn jsonb_object_two_arg<'mcx>(
    mcx: Mcx<'mcx>,
    nkdims: i32,
    nvdims: i32,
    key_datums: &[Option<Vec<u8>>],
    val_datums: &[Option<Vec<u8>>],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = JsonbInState::default();
    pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;

    if nkdims > 1 || nkdims != nvdims {
        return Err(PgError::error("wrong number of array subscripts")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    if nkdims == 0 {
        return close_object(mcx, &mut result);
    }

    if key_datums.len() != val_datums.len() {
        return Err(PgError::error("mismatched array dimensions")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    for i in 0..key_datums.len() {
        if key_datums[i].is_none() {
            return Err(PgError::error("null value not allowed for object key")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        let key = key_datums[i]
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_object_two_arg: key datum is NULL"))?;
        let v = JsonbValue {
            typ: jbvType::jbvString,
            val: JsonbValueData::String(key.clone()),
        };
        pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_KEY, Some(&v))?;

        let v = match &val_datums[i] {
            None => JsonbValue::null(),
            Some(s) => JsonbValue {
                typ: jbvType::jbvString,
                val: JsonbValueData::String(s.clone()),
            },
        };
        pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_VALUE, Some(&v))?;
    }

    close_object(mcx, &mut result)
}

fn close_object<'mcx>(mcx: Mcx<'mcx>, result: &mut JsonbInState) -> PgResult<PgVec<'mcx, u8>> {
    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_OBJECT, None)?;
    JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("close_object: result.res is NULL"))?,
    )
}

// ===========================================================================
// Aggregate support.
// ===========================================================================

/// C: `clone_parse_state(JsonbParseState *state)` — used by the agg final
/// functions to avoid mutating the aggregate state if the finalfn runs more
/// than once. C copies each frame's `contVal` by struct value; our
/// `JsonbParseState` owns its children, so the structural deep clone is
/// output-equivalent for the append-only finalfn usage.
pub fn clone_parse_state(state: &Option<Box<JsonbParseState>>) -> Option<Box<JsonbParseState>> {
    state.clone()
}

// ---------------------------------------------------------------------------
// jsonb_agg / jsonb_object_agg aggregates.
//
// The fmgr/aggregate-context marshaling is the executor boundary; these take
// the persistent `JsonbAggState` (`None` on the first call) and the already
// extracted Datums/null flags, and port the splice-loop bodies 1:1. The
// per-element "copy string/numeric into the aggregate context" is implicit in
// our owned-bytes `JsonbValue` model (`pushJsonbValue` clones the value).
// ---------------------------------------------------------------------------

/// C: `jsonb_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)`.
pub fn jsonb_agg_transfn_worker<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    arg_type: Oid,
    val: &Datum<'mcx>,
    val_is_null: bool,
    absent_on_null: bool,
) -> PgResult<JsonbAggState> {
    use JsonbIteratorToken::*;

    // set up the accumulator on the first go round
    let mut state = match state {
        None => {
            if arg_type == 0 {
                return Err(PgError::error("could not determine input data type")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            let mut s = JsonbAggState::default();
            s.res.res = pushJsonbValue(&mut s.res.parse_state, WJB_BEGIN_ARRAY, None)?;
            let (cat, out) = catalog_fmgr::categorize_type::call(arg_type)?;
            s.val_category = Some(cat);
            s.val_output_func = out;
            s
        }
        Some(s) => s,
    };

    if absent_on_null && val_is_null {
        return Ok(state);
    }

    // turn the argument into jsonb in the normal function context
    let mut elem = JsonbInState::default();
    let val_category = state
        .val_category
        .ok_or_else(|| PgError::error("jsonb_agg_transfn_worker: val_category set on first call"))?;
    let val_output_func = state.val_output_func;
    let null_datum = Datum::null();
    datum_to_jsonb_internal(
        mcx,
        if val_is_null { &null_datum } else { val },
        val_is_null,
        &mut elem,
        val_category,
        val_output_func,
        false,
    )?;
    let jbelem = JsonbValueToJsonb(
        mcx,
        elem.res
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_agg_transfn_worker: elem.res is NULL"))?,
    )?;

    // splice the rendered element into the accumulator
    let mut single_scalar = false;
    let mut it = JsonbIteratorInit(&jbelem[VARHDRSZ..]);
    loop {
        let mut v = JsonbValue::null();
        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
        if typ == WJB_DONE {
            break;
        }
        match typ {
            WJB_BEGIN_ARRAY => {
                if is_raw_scalar_array(&v) {
                    single_scalar = true;
                } else {
                    state.res.res = pushJsonbValue(&mut state.res.parse_state, typ, None)?;
                }
            }
            WJB_END_ARRAY => {
                if !single_scalar {
                    state.res.res = pushJsonbValue(&mut state.res.parse_state, typ, None)?;
                }
            }
            WJB_BEGIN_OBJECT | WJB_END_OBJECT => {
                state.res.res = pushJsonbValue(&mut state.res.parse_state, typ, None)?;
            }
            WJB_ELEM | WJB_KEY | WJB_VALUE => {
                // string/numeric values are already owned copies in v.
                state.res.res = pushJsonbValue(&mut state.res.parse_state, typ, Some(&v))?;
            }
            WJB_DONE => unreachable!(),
        }
    }

    Ok(state)
}

/// C: `jsonb_agg_transfn(PG_FUNCTION_ARGS)`.
pub fn jsonb_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    arg_type: Oid,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonbAggState> {
    jsonb_agg_transfn_worker(mcx, state, arg_type, val, val_is_null, false)
}

/// C: `jsonb_agg_strict_transfn(PG_FUNCTION_ARGS)`.
pub fn jsonb_agg_strict_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    arg_type: Oid,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonbAggState> {
    jsonb_agg_transfn_worker(mcx, state, arg_type, val, val_is_null, true)
}

/// C: `jsonb_agg_finalfn(PG_FUNCTION_ARGS)`. Returns `None` for the no-rows
/// case (`PG_RETURN_NULL`).
pub fn jsonb_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    arg: Option<&JsonbAggState>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let Some(arg) = arg else {
        return Ok(None); // returns null iff no input values
    };

    let mut result = JsonbInState {
        parse_state: clone_parse_state(&arg.res.parse_state),
        ..Default::default()
    };
    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_ARRAY, None)?;
    Ok(Some(JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_agg_finalfn: result.res is NULL"))?,
    )?))
}

/// C: `jsonb_object_agg_transfn_worker(FunctionCallInfo fcinfo, bool
/// absent_on_null, bool unique_keys)`.
pub fn jsonb_object_agg_transfn_worker<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<JsonbAggState> {
    use JsonbIteratorToken::*;

    // set up the accumulator on the first go round
    let mut state = match state {
        None => {
            let mut s = JsonbAggState::default();
            s.res.res = pushJsonbValue(&mut s.res.parse_state, WJB_BEGIN_OBJECT, None)?;
            if let Some(ps) = s.res.parse_state.as_mut() {
                ps.unique_keys = unique_keys;
                ps.skip_nulls = absent_on_null;
            }

            if key_arg_type == 0 {
                return Err(PgError::error("could not determine input data type")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            let (kcat, kout) = catalog_fmgr::categorize_type::call(key_arg_type)?;
            s.key_category = Some(kcat);
            s.key_output_func = kout;

            if val_arg_type == 0 {
                return Err(PgError::error("could not determine input data type")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            let (vcat, vout) = catalog_fmgr::categorize_type::call(val_arg_type)?;
            s.val_category = Some(vcat);
            s.val_output_func = vout;
            s
        }
        Some(s) => s,
    };

    // turn the argument into jsonb in the normal function context
    if key_is_null {
        return Err(PgError::error("field name must not be null")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // Skip null values if absent_on_null unless key uniqueness check is needed
    // (because we must save keys in this case).
    let skip = absent_on_null && val_is_null;

    if skip && !unique_keys {
        return Ok(state);
    }

    let key_category = state.key_category.expect("key_category set on first call");
    let key_output_func = state.key_output_func;
    let mut elem = JsonbInState::default();
    datum_to_jsonb_internal(mcx, key, false, &mut elem, key_category, key_output_func, true)?;
    let jbkey = JsonbValueToJsonb(mcx, elem.res.as_ref().unwrap())?;

    let val_category = state.val_category.expect("val_category set on first call");
    let val_output_func = state.val_output_func;
    let mut elem = JsonbInState::default();
    let null_datum = Datum::null();
    datum_to_jsonb_internal(
        mcx,
        if val_is_null { &null_datum } else { val },
        val_is_null,
        &mut elem,
        val_category,
        val_output_func,
        false,
    )?;
    let jbval = JsonbValueToJsonb(mcx, elem.res.as_ref().unwrap())?;

    // keys should be scalar, and we should have already checked for that above
    // when calling datum_to_jsonb, so we only need to look for these things.
    let mut it = JsonbIteratorInit(&jbkey[VARHDRSZ..]);
    loop {
        let mut v = JsonbValue::null();
        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
        if typ == WJB_DONE {
            break;
        }
        match typ {
            WJB_BEGIN_ARRAY => {
                if !is_raw_scalar_array(&v) {
                    return Err(elog_internal("unexpected structure for key"));
                }
            }
            WJB_ELEM => {
                if v.typ == jbvType::jbvString {
                    // string value is already an owned copy in v.
                } else {
                    return Err(PgError::error("object keys must be strings")
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
                }
                state.res.res = pushJsonbValue(&mut state.res.parse_state, WJB_KEY, Some(&v))?;

                if skip {
                    let nullv = JsonbValue::null();
                    state.res.res =
                        pushJsonbValue(&mut state.res.parse_state, WJB_VALUE, Some(&nullv))?;
                    return Ok(state);
                }
            }
            WJB_END_ARRAY => {}
            _ => return Err(elog_internal("unexpected structure for key")),
        }
    }

    let mut single_scalar = false;
    let mut it = JsonbIteratorInit(&jbval[VARHDRSZ..]);
    loop {
        let mut v = JsonbValue::null();
        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
        if typ == WJB_DONE {
            break;
        }
        match typ {
            WJB_BEGIN_ARRAY => {
                if is_raw_scalar_array(&v) {
                    single_scalar = true;
                } else {
                    state.res.res = pushJsonbValue(&mut state.res.parse_state, typ, None)?;
                }
            }
            WJB_END_ARRAY => {
                if !single_scalar {
                    state.res.res = pushJsonbValue(&mut state.res.parse_state, typ, None)?;
                }
            }
            WJB_BEGIN_OBJECT | WJB_END_OBJECT => {
                state.res.res = pushJsonbValue(&mut state.res.parse_state, typ, None)?;
            }
            WJB_ELEM | WJB_KEY | WJB_VALUE => {
                let tok = if single_scalar { WJB_VALUE } else { typ };
                state.res.res = pushJsonbValue(&mut state.res.parse_state, tok, Some(&v))?;
            }
            WJB_DONE => unreachable!(),
        }
    }

    Ok(state)
}

/// C: `jsonb_object_agg_transfn(PG_FUNCTION_ARGS)`.
pub fn jsonb_object_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonbAggState> {
    jsonb_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, false, false,
    )
}

/// C: `jsonb_object_agg_strict_transfn(PG_FUNCTION_ARGS)`.
pub fn jsonb_object_agg_strict_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonbAggState> {
    jsonb_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, true, false,
    )
}

/// C: `jsonb_object_agg_unique_transfn(PG_FUNCTION_ARGS)`.
pub fn jsonb_object_agg_unique_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonbAggState> {
    jsonb_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, false, true,
    )
}

/// C: `jsonb_object_agg_unique_strict_transfn(PG_FUNCTION_ARGS)`.
pub fn jsonb_object_agg_unique_strict_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonbAggState>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonbAggState> {
    jsonb_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, true, true,
    )
}

/// C: `jsonb_object_agg_finalfn(PG_FUNCTION_ARGS)`. Returns `None` for the
/// no-rows case.
pub fn jsonb_object_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    arg: Option<&JsonbAggState>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let Some(arg) = arg else {
        return Ok(None); // returns null iff no input values
    };

    let mut result = JsonbInState {
        parse_state: clone_parse_state(&arg.res.parse_state),
        ..Default::default()
    };
    result.res = pushJsonbValue(&mut result.parse_state, JsonbIteratorToken::WJB_END_OBJECT, None)?;
    Ok(Some(JsonbValueToJsonb(
        mcx,
        result
            .res
            .as_ref()
            .ok_or_else(|| PgError::error("jsonb_object_agg_finalfn: result.res is NULL"))?,
    )?))
}

/// C: `v.val.array.rawScalar` test for a `WJB_BEGIN_ARRAY` iterator value.
#[inline]
fn is_raw_scalar_array(v: &JsonbValue) -> bool {
    matches!(&v.val, JsonbValueData::Array { raw_scalar, .. } if *raw_scalar)
}

// ---------------------------------------------------------------------------
// Numeric bridge + helpers / errors.
// ---------------------------------------------------------------------------

/// C: `DatumGetCString(DirectFunctionCall1(numeric_out, num))` — canonical
/// decimal text of the on-disk `numeric` varlena `num`.
fn numeric_out<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<String> {
    backend_utils_adt_numeric::io::numeric_out(mcx, num)
}

/// C: `DatumGetNumeric(DirectFunctionCall3(numeric_in, token, InvalidOid, -1))`
/// — parse a JSON number `token` into the on-disk `numeric` varlena bytes.
/// `typmod = -1` (no scale/precision enforcement). Returned as a plain `Vec`
/// owned by the `JsonbValue` tree (decoupled from `mcx`'s lifetime).
fn numeric_in_to_bytes<'mcx>(mcx: Mcx<'mcx>, token: &str) -> PgResult<Vec<u8>> {
    let bytes = backend_utils_adt_numeric::io::numeric_in(mcx, token, -1)?;
    Ok(bytes.as_slice().to_vec())
}

/// C: `pushJsonbValue` re-export for the SQL-facing builders.
fn pushJsonbValue(
    pstate: &mut Option<Box<JsonbParseState>>,
    seq: JsonbIteratorToken,
    jbval: Option<&JsonbValue>,
) -> PgResult<Option<JsonbValue>> {
    jbu_pushJsonbValue(pstate, seq, jbval)
}

/// Read the container header word from container bytes.
#[inline]
fn container_header(jc: &[u8]) -> u32 {
    u32::from_ne_bytes([jc[0], jc[1], jc[2], jc[3]])
}

fn elog_internal(msg: &str) -> PgError {
    PgError::error(msg.to_string()).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

fn unknown_token() -> PgError {
    elog_internal("unknown jsonb iterator token type")
}

/// C: `checkStringLen(size_t len, Node *escontext)` — exposed for the parser
/// provider that drives the `jsonb_in_*` semantic actions.
pub fn checkStringLen(len: usize) -> PgResult<()> {
    if len > JENTRY_OFFLENMASK {
        return Err(PgError::error("string too long to represent as jsonb string")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(format!(
                "Due to an implementation restriction, jsonb strings cannot exceed {} bytes.",
                JENTRY_OFFLENMASK
            )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
