//! `fmgr`-callable wrappers for the `internal`-transtype `string_agg` aggregate
//! (`varlena.c`): `string_agg_transfn`(3535) / `string_agg_finalfn`(3536).
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

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

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

/// `PG_GETARG_TEXT_PP(i)` — the detoasted `text` payload (`VARDATA_ANY`,
/// header-stripped) the by-ref lane delivers.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("string_agg fn: by-ref `text` arg missing from by-ref lane")
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

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// `PG_RETURN_TEXT_P(image)` — a by-ref `text` result, header-stripped (the
/// payload bytes; symmetric with the `arg_text` lane).
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(payload));
    Datum::from_usize(0)
}

/// `string_agg_transfn`(3535): append `value` (and the preceding `delim`) to the
/// running buffer.
fn fc_string_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
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
    match state {
        Some(s) => ret_internal(fcinfo, s),
        None => ret_null(fcinfo),
    }
}

/// `string_agg_finalfn`(3536): the accumulated string with the first delimiter
/// stripped off the front.
fn fc_string_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);
    match take_string_state(fcinfo) {
        None => ret_null(fcinfo),
        Some(state) => {
            // PG_RETURN_TEXT_P(cstring_to_text_with_len(&state->data[state->cursor],
            //                                           state->len - state->cursor));
            let cursor = state.cursor.min(state.data.len());
            let payload = state.data[cursor..].to_vec();
            ret_text(fcinfo, payload)
        }
    }
}

// ---------------------------------------------------------------------------
// Registration (C: their `fmgr_builtins[]` rows; both `proisstrict => 'f'` —
// they handle the NULL `internal` running state / NULL input themselves).
// ---------------------------------------------------------------------------

pub fn register_string_agg_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3535, "string_agg_transfn", 3, fc_string_agg_transfn),
        builtin(3536, "string_agg_finalfn", 1, fc_string_agg_finalfn),
    ]);
}

/// A non-strict (`proisstrict => 'f'`) builtin row.
fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: false,
        retset: false,
        func: Some(func),
    }
}
