//! `fmgr`-callable wrappers for the `internal`-transtype `string_agg` aggregate
//! (`varlena.c`): `string_agg_transfn`(3535) / `string_agg_finalfn`(3536), plus
//! the `bytea` variants `bytea_string_agg_transfn`(3543) /
//! `bytea_string_agg_finalfn`(3544).
//!
//! ## The `internal` transition state crosses the fmgr boundary
//!
//! C's transition state is a `StringInfo` (a `void *` to a `StringInfoData`
//! living in the per-aggregate `MemoryContext`). Here it rides the canonical
//! `Datum::Internal(Box<dyn Any>)` arm (`RefPayload::Internal`): nodeAgg moves
//! the box in/out of the call frame, the transfn appends to it in place, and
//! returns the same box.
//!
//! Unlike `array_agg`, `string_agg` needs no leaked aggcontext: a `StringInfo`'s
//! `data` buffer is modeled by an owned global-allocator `Vec<u8>` carried in
//! the `Box`, so the accumulated bytes are fully self-contained across rows (the
//! `makeStringAggState` `MemoryContextSwitchTo(aggcontext)` is a no-op here — the
//! buffer is not context-charged). The `StringInfo`'s `cursor` field records the
//! length of the first delimiter (stripped off only in the final function), per
//! the C comment.

use datum::Datum;
use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// `StringInfo` transition state for `string_agg`. `data` is the accumulated
/// buffer (`StringInfoData.data`); `cursor` is `StringInfoData.cursor`, reused
/// here to store the length of the first delimiter.
struct StringAggState {
    /// `StringInfoData.data` — the accumulated bytes (delimiters + values).
    data: Vec<u8>,
    /// `StringInfoData.cursor` — the byte length of the first delimiter, so the
    /// final function can strip it off the front.
    cursor: usize,
}

impl StringAggState {
    /// `makeStringAggState(fcinfo)` → `makeStringInfo()` in the aggcontext (the
    /// owned `Vec` needs no context switch).
    fn new() -> Box<StringAggState> {
        Box::new(StringAggState {
            data: Vec::new(),
            cursor: 0,
        })
    }
}

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// `VARDATA_ANY` of an inline (non-compressed, non-external) varlena image: skip
/// ONE header byte for a short (1-byte) header, else `VARHDRSZ`. A small stored
/// value arrives short-headed once `SHORT_VARLENA_PACKING` is on; a fixed
/// `VARHDRSZ` strip would drop three payload bytes. No-op while packing is off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `PG_GETARG_TEXT_PP(i)` — the detoasted `text` payload (`VARDATA_ANY`). Under
/// the header-ful-everywhere convention the by-ref lane carries the full
/// varlena image (4-byte length word + payload); this skips the header.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("string_agg fn: by-ref `text` arg missing from by-ref lane");
    vardata_any(image)
}

/// Take the `internal` transition state out of `args[0]`. `None` is C's
/// `PG_ARGISNULL(0)`.
fn take_string_state(fcinfo: &mut FunctionCallInfoBaseData) -> Option<Box<StringAggState>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(
            b.downcast::<StringAggState>().unwrap_or_else(|_| {
                panic!("string_agg fn: args[0] internal state is not a StringAggState")
            }),
        ),
        Some(other) => panic!("string_agg fn: args[0] is not an internal state ({other:?})"),
        None => None,
    }
}

/// `PG_RETURN_POINTER(state)`.
fn ret_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn core::any::Any>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Internal(state));
    Datum::from_usize(0)
}

/// Restore an `internal` StringAggState into `args[0]` after a *final* function
/// read it. C's `PG_GETARG_POINTER(0)` does NOT consume the state; the same live
/// state must survive for a sharing aggregate's finalfn and, in a moving window
/// frame, for the next row's forward/inverse transition (mirrors numeric's
/// `keep_internal`). `take_string_state` moved the box out, so put it back.
#[inline]
fn keep_string_state(fcinfo: &mut FunctionCallInfoBaseData, state: Box<StringAggState>) {
    fcinfo.set_ref_arg(0, RefPayload::Internal(state));
}

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// `PG_RETURN_TEXT_P(image)` — a by-ref `text` result. Under the
/// header-ful-everywhere convention this stamps the 4-byte uncompressed varlena
/// length word in front of the payload (`SET_VARSIZE`), symmetric with how
/// `arg_text` reads args back (skipping the header).
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let mut image = Vec::with_capacity(payload.len() + VARHDRSZ);
    image.extend_from_slice(&datum::varlena::set_varsize_4b(payload.len() + VARHDRSZ));
    image.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// `string_agg_transfn`(3535): append `value` (and the preceding `delim`) to the
/// running buffer.
fn fc_string_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);
    let mut state = take_string_state(fcinfo);

    // if (!PG_ARGISNULL(1))
    if !arg_isnull(fcinfo, 1) {
        // text *value = PG_GETARG_TEXT_PP(1);
        let value = arg_text(fcinfo, 1).to_vec();
        let mut isfirst = false;

        // if (state == NULL) { state = makeStringAggState(fcinfo); isfirst = true; }
        let st = match state.as_mut() {
            Some(s) => s,
            None => {
                state = Some(StringAggState::new());
                isfirst = true;
                state.as_mut().unwrap()
            }
        };

        // if (!PG_ARGISNULL(2)) {
        //     text *delim = PG_GETARG_TEXT_PP(2);
        //     appendStringInfoText(state, delim);
        //     if (isfirst) state->cursor = VARSIZE_ANY_EXHDR(delim);
        // }
        if !arg_isnull(fcinfo, 2) {
            let delim = arg_text(fcinfo, 2);
            // appendStringInfoText == appendBinaryStringInfo(VARDATA_ANY, EXHDR len).
            st.data.extend_from_slice(delim);
            if isfirst {
                st.cursor = delim.len();
            }
        }

        // appendStringInfoText(state, value);
        st.data.extend_from_slice(&value);
    }

    // if (state) PG_RETURN_POINTER(state); else PG_RETURN_NULL();
    Ok(match state {
        Some(s) => ret_internal(fcinfo, s),
        None => ret_null(fcinfo),
    })
}

/// `string_agg_finalfn`(3536): the accumulated string with the first delimiter
/// stripped off the front.
fn fc_string_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);
    Ok(match take_string_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(state) => {
            // PG_RETURN_TEXT_P(cstring_to_text_with_len(&state->data[state->cursor],
            //                                           state->len - state->cursor));
            let cursor = state.cursor.min(state.data.len());
            let payload = state.data[cursor..].to_vec();
            // C `PG_GETARG_POINTER(0)` does not consume the state; restore it.
            keep_string_state(fcinfo, state);
            ret_text(fcinfo, payload)
        }
    })
}

/// `PG_GETARG_BYTEA_PP(i)` — the detoasted `bytea` payload (`VARDATA_ANY`).
/// Identical on-wire framing to `text` under the header-ful-everywhere
/// convention: the by-ref lane carries the full varlena image (4-byte length
/// word + payload); this skips the header. C's `bytea_string_agg_transfn` reads
/// args via the same `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` macros as the `text`
/// transfn, so the byte handling is byte-for-byte identical.
#[inline]
fn arg_bytea<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("bytea_string_agg fn: by-ref `bytea` arg missing from by-ref lane");
    vardata_any(image)
}

/// `PG_RETURN_BYTEA_P(image)` — a by-ref `bytea` result. Same varlena framing as
/// `ret_text` (`SET_VARSIZE` over a 4-byte uncompressed length word + payload).
fn ret_bytea(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let mut image = Vec::with_capacity(payload.len() + VARHDRSZ);
    image.extend_from_slice(&datum::varlena::set_varsize_4b(payload.len() + VARHDRSZ));
    image.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// `bytea_string_agg_transfn`(3543): append `value` (and the preceding `delim`)
/// to the running buffer. Mirrors `bytea_string_agg_transfn` in `varlena.c`,
/// which is structurally identical to `string_agg_transfn` but reads `bytea`
/// args; the transition state is the same `internal` `StringInfo`.
fn fc_bytea_string_agg_transfn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    // state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);
    let mut state = take_string_state(fcinfo);

    // if (!PG_ARGISNULL(1))
    if !arg_isnull(fcinfo, 1) {
        // bytea *value = PG_GETARG_BYTEA_PP(1);
        let value = arg_bytea(fcinfo, 1).to_vec();
        let mut isfirst = false;

        // if (state == NULL) { state = makeStringAggState(fcinfo); isfirst = true; }
        let st = match state.as_mut() {
            Some(s) => s,
            None => {
                state = Some(StringAggState::new());
                isfirst = true;
                state.as_mut().unwrap()
            }
        };

        // if (!PG_ARGISNULL(2)) {
        //     bytea *delim = PG_GETARG_BYTEA_PP(2);
        //     appendBinaryStringInfo(state, VARDATA_ANY(delim), VARSIZE_ANY_EXHDR(delim));
        //     if (isfirst) state->cursor = VARSIZE_ANY_EXHDR(delim);
        // }
        if !arg_isnull(fcinfo, 2) {
            let delim = arg_bytea(fcinfo, 2);
            st.data.extend_from_slice(delim);
            if isfirst {
                st.cursor = delim.len();
            }
        }

        // appendBinaryStringInfo(state, VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value));
        st.data.extend_from_slice(&value);
    }

    // if (state) PG_RETURN_POINTER(state); else PG_RETURN_NULL();
    Ok(match state {
        Some(s) => ret_internal(fcinfo, s),
        None => ret_null(fcinfo),
    })
}

/// `bytea_string_agg_finalfn`(3544): the accumulated bytes with the first
/// delimiter stripped off the front (C: `PG_RETURN_BYTEA_P` over
/// `&state->data[state->cursor]`, length `state->len - state->cursor`).
fn fc_bytea_string_agg_finalfn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    // state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);
    Ok(match take_string_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(state) => {
            let cursor = state.cursor.min(state.data.len());
            let payload = state.data[cursor..].to_vec();
            // C `PG_GETARG_POINTER(0)` does not consume the state; restore it.
            keep_string_state(fcinfo, state);
            ret_bytea(fcinfo, payload)
        }
    })
}

/// `string_agg_combine`(6299): combine two `StringInfo` transition states for
/// parallel `string_agg`. `proisstrict => 'f'`. C: if state2 is NULL return
/// state1; if state1 is NULL copy state2's data + cursor into a fresh state;
/// else append state2's bytes (cursor unchanged). The first delimiter is kept in
/// every partial state so the combine joins the strings correctly; it is
/// stripped only in the final function.
fn fc_string_agg_combine(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // state1 = PG_ARGISNULL(0) ? NULL : ...; state2 = PG_ARGISNULL(1) ? NULL : ...
    let state1 = take_string_state(fcinfo);
    let state2 = take_string_state_at(fcinfo, 1);

    // if (state2 == NULL) { if (state1==NULL) PG_RETURN_NULL(); PG_RETURN_POINTER(state1); }
    let state2 = match state2 {
        None => {
            return Ok(match state1 {
                None => ret_null(fcinfo),
                Some(s1) => ret_internal(fcinfo, s1),
            });
        }
        Some(s2) => s2,
    };

    let combined = match state1 {
        // state1 == NULL: copy state2's data into the agg_context.
        None => {
            let mut s1 = StringAggState::new();
            s1.data.extend_from_slice(&state2.data);
            s1.cursor = state2.cursor;
            s1
        }
        // else if (state2->len > 0): append state2's bytes; cursor unchanged.
        Some(mut s1) => {
            if !state2.data.is_empty() {
                s1.data.extend_from_slice(&state2.data);
            }
            s1
        }
    };

    Ok(ret_internal(fcinfo, combined))
}

/// `string_agg_serialize`(6300): serialize a `StringInfo` transition state into a
/// `bytea` for parallel transfer. Strict. Wire format (mirrors C
/// `pq_begintypsend`/`pq_sendint(cursor,4)`/`pq_sendbytes(data)`): a 4-byte
/// big-endian cursor followed by the raw data bytes.
fn fc_string_agg_serialize(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let state = take_string_state(fcinfo)
        .expect("string_agg_serialize: NULL internal state (strict aggregate)");

    // pq_sendint(&buf, state->cursor, 4) — 4-byte big-endian.
    let mut payload = Vec::with_capacity(4 + state.data.len());
    payload.extend_from_slice(&(state.cursor as u32).to_be_bytes());
    // pq_sendbytes(&buf, state->data, state->len).
    payload.extend_from_slice(&state.data);

    // C `PG_GETARG_POINTER(0)` does not consume the state; restore it.
    keep_string_state(fcinfo, state);
    Ok(ret_bytea(fcinfo, payload))
}

/// `string_agg_deserialize`(6301): rebuild a `StringInfo` transition state from a
/// `bytea`. Strict. The payload is a 4-byte big-endian cursor followed by the
/// raw data bytes (everything after the cursor word).
fn fc_string_agg_deserialize(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    // sstate = PG_GETARG_BYTEA_PP(0); VARDATA_ANY skips the 4-byte varlena header.
    let body = arg_bytea(fcinfo, 0);

    let mut result = StringAggState::new();
    // cursor = pq_getmsgint(&buf, 4) — 4-byte big-endian.
    let cursor = if body.len() >= 4 {
        u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize
    } else {
        0
    };
    result.cursor = cursor;
    // datalen = VARSIZE_ANY_EXHDR(sstate) - 4; data = pq_getmsgbytes(&buf, datalen).
    if body.len() > 4 {
        result.data.extend_from_slice(&body[4..]);
    }

    Ok(ret_internal(fcinfo, result))
}

/// Take the `internal` transition state out of `args[i]`. `None` is C's
/// `PG_ARGISNULL(i)`.
fn take_string_state_at(
    fcinfo: &mut FunctionCallInfoBaseData,
    i: usize,
) -> Option<Box<StringAggState>> {
    if arg_isnull(fcinfo, i) {
        return None;
    }
    match fcinfo.take_ref_arg(i) {
        Some(RefPayload::Internal(b)) => Some(b.downcast::<StringAggState>().unwrap_or_else(|_| {
            panic!("string_agg fn: args[{i}] internal state is not a StringAggState")
        })),
        Some(other) => panic!("string_agg fn: args[{i}] is not an internal state ({other:?})"),
        None => None,
    }
}

// ---------------------------------------------------------------------------
// Registration (C: their `fmgr_builtins[]` rows; transfn/finalfn/combine are
// `proisstrict => 'f'` — they handle the NULL `internal` running state / NULL
// input themselves; serialize/deserialize are `proisstrict => 't'`).
// ---------------------------------------------------------------------------

pub fn register_string_agg_builtins() {
    fmgr_core::register_builtins_native([
        builtin(3535, "string_agg_transfn", 3, fc_string_agg_transfn),
        builtin(3536, "string_agg_finalfn", 1, fc_string_agg_finalfn),
        builtin(3543, "bytea_string_agg_transfn", 3, fc_bytea_string_agg_transfn),
        builtin(3544, "bytea_string_agg_finalfn", 1, fc_bytea_string_agg_finalfn),
        builtin(6299, "string_agg_combine", 2, fc_string_agg_combine),
    ]);
    fmgr_core::register_builtins_native([
        builtin_strict(6300, "string_agg_serialize", 1, fc_string_agg_serialize),
        builtin_strict(6301, "string_agg_deserialize", 2, fc_string_agg_deserialize),
    ]);
}

/// A strict (`proisstrict => 't'`) Result-native builtin row.
fn builtin_strict(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        native,
    )
}

/// A non-strict (`proisstrict => 'f'`) Result-native builtin row (`func: None`;
/// dispatch goes through the native overlay) paired with its [`PgFnNative`] body.
fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: false,
            retset: false,
            func: None,
        },
        native,
    )
}
