//! jsonb.c aggregate slice: jsonb_agg and jsonb_object_agg trans/final
//! functions over an INTERNAL aggcontext-lived open parse state.

extern crate alloc;

use crate::build::convert_to_jsonb;
use crate::container::*;
use crate::iter::{JsonbIterator, WjbToken};
use crate::mutate::JsonbPush;
use crate::tojsonb::{datum_to_jsonb_internal, json_categorize_type, no_input_type, ValCategory};
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

// C: JsonbAggState minus the category fields — those depend only on the
// declared argument types, so they live once per FmgrInfo (fn_extra memo),
// not per group. ManuallyDrop: the aggcontext arena resets wholesale.
struct JsonbAggState {
    push: core::mem::ManuallyDrop<JsonbPush<'static>>,
}

// Per-flinfo categorized argument types (C stores them per group).
pub(crate) struct AggCats {
    val: ValCategory,
    key: Option<ValCategory>,
}

#[track_caller]
#[cold]
#[inline(never)]
fn non_aggregate_context(name: &str) -> Box<PgError> {
    Box::new(PgError::error(alloc::format!(
        "{name} called in non-aggregate context"
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_param(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

fn agg_mcx<'a>(fcinfo: &Fcinfo, name: &str) -> PgResult<Mcx<'a>> {
    // SAFETY: context, if set, is the evaltrans build's AggStateNode, live
    // across every call through this frame.
    match unsafe { fcinfo.agg_context() } {
        Some(m) => Ok(m),
        None => Err(non_aggregate_context(name)),
    }
}

fn new_state<'a>(
    aggmcx: Mcx<'a>,
    open: impl FnOnce(&mut JsonbPush<'a>) -> PgResult<()>,
) -> PgResult<*mut JsonbAggState> {
    const { assert!(!core::mem::needs_drop::<JsonbAggState>()) }
    let mut push = JsonbPush::new(aggmcx)?;
    open(&mut push)?;
    // SAFETY: the aggcontext outlives every trans/final call of this node;
    // restamping the arena brand to 'static never outlives it.
    let push: JsonbPush<'static> = unsafe { core::mem::transmute(push) };
    let layout = core::alloc::Layout::new::<JsonbAggState>();
    let raw = mcx::Allocator::allocate(&aggmcx, layout).map_err(|_| aggmcx.oom(layout.size()))?;
    let p = raw.cast::<JsonbAggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe {
        p.write(JsonbAggState {
            push: core::mem::ManuallyDrop::new(push),
        })
    };
    Ok(p)
}

fn cats<'f>(
    flinfo: &'f mut FmgrInfo,
    build: impl FnOnce() -> PgResult<AggCats>,
) -> PgResult<&'f mut AggCats> {
    if flinfo.fn_extra_ref::<AggCats>().is_none() {
        let c = build()?;
        flinfo.set_fn_extra(c);
    }
    Ok(flinfo.fn_extra_mut::<AggCats>().unwrap())
}

fn copy_static<'a>(aggmcx: Mcx<'a>, bytes: &[u8]) -> PgResult<&'static [u8]> {
    let s = mcx::slice_in(aggmcx, bytes)?.leak();
    // SAFETY: aggcontext-lived; see new_state.
    Ok(unsafe { core::mem::transmute::<&[u8], &'static [u8]>(s) })
}

// Copy string/numeric leaves into the aggcontext before pushing (the source
// image lives in per-call memory), exactly C's iterate-and-copy loop.
fn item_into_agg<'a>(aggmcx: Mcx<'a>, v: JsonbItem<'_>) -> PgResult<JsonbItem<'static>> {
    Ok(match v {
        JsonbItem::String(s) => JsonbItem::String(copy_static(aggmcx, s)?),
        JsonbItem::Numeric(img) => JsonbItem::Numeric(copy_static(aggmcx, img)?),
        JsonbItem::Null => JsonbItem::Null,
        JsonbItem::Bool(b) => JsonbItem::Bool(b),
        JsonbItem::Array {
            n_elems,
            raw_scalar,
        } => JsonbItem::Array {
            n_elems,
            raw_scalar,
        },
        JsonbItem::Object { n_pairs } => JsonbItem::Object { n_pairs },
        JsonbItem::Binary(_) => panic!("nested binary in full-descent iteration"),
    })
}

// Serialize one argument to a jsonb image in per-call memory.
fn elem_image<'mcx>(
    mcx: Mcx<'mcx>,
    val: Datum,
    is_null: bool,
    cat: &mut ValCategory,
    key_scalar: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut ps = JsonbPush::new(mcx)?;
    datum_to_jsonb_internal(mcx, &mut ps, val, is_null, cat, key_scalar)?;
    convert_to_jsonb(mcx, &ps.finish())
}

/// C: jsonb_agg_transfn_worker's accumulate loop.
fn accumulate_value<'a, 'mcx>(
    aggmcx: Mcx<'a>,
    mcx: Mcx<'mcx>,
    state: &mut JsonbAggState,
    image: &[u8],
    value_of_scalar: bool,
) -> PgResult<()> {
    let payload = &image[4..];
    let mut single_scalar = false;
    let mut it = JsonbIterator::init(mcx, payload)?;
    loop {
        let (tok, v) = it.next(false);
        match tok {
            WjbToken::Done => break,
            WjbToken::BeginArray => {
                if matches!(
                    v,
                    JsonbItem::Array {
                        raw_scalar: true,
                        ..
                    }
                ) {
                    single_scalar = true;
                } else {
                    state.push.push_token(tok)?;
                }
            }
            WjbToken::EndArray => {
                if !single_scalar {
                    state.push.push_token(tok)?;
                }
            }
            WjbToken::BeginObject | WjbToken::EndObject => state.push.push_token(tok)?,
            WjbToken::Elem | WjbToken::Key | WjbToken::Value => {
                let item = item_into_agg(aggmcx, v)?;
                let tok = if single_scalar && value_of_scalar && tok == WjbToken::Elem {
                    WjbToken::Value
                } else {
                    tok
                };
                state.push.push(tok, item)?;
            }
        }
    }
    Ok(())
}

/// C: jsonb_agg_transfn_worker.
fn jsonb_agg_transfn_worker(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    absent_on_null: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("jsonb_agg_transfn needs a resolved FmgrInfo");
    let aggmcx = agg_mcx(fcinfo, "jsonb_agg_transfn")?;
    let [a, b] = *fcinfo.args_n::<2>();

    let state = if a.isnull {
        let arg_type = funcapi::get_fn_expr_argtype(Some(flinfo), 1);
        if arg_type == types_core::InvalidOid {
            return Err(no_input_type());
        }
        cats(flinfo, || {
            Ok(AggCats {
                val: json_categorize_type(arg_type)?,
                key: None,
            })
        })?;
        new_state(aggmcx, |ps| ps.push_token(WjbToken::BeginArray))?
    } else {
        a.value.as_usize() as *mut JsonbAggState
    };
    // SAFETY: the state pointer is this transfn chain's aggcontext-lived
    // allocation; no other reference is live during the call.
    let state = unsafe { &mut *state };

    if absent_on_null && b.isnull {
        return Ok(Datum::from_usize(state as *mut JsonbAggState as usize));
    }

    let mcx = fcinfo.result_mcx();
    let c = flinfo.fn_extra_mut::<AggCats>().expect("cats built on first call");
    let image = elem_image(mcx, b.value, b.isnull, &mut c.val, false)?;
    accumulate_value(aggmcx, mcx, state, &image, false)?;
    Ok(Datum::from_usize(state as *mut JsonbAggState as usize))
}

pub fn fc_jsonb_agg_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    jsonb_agg_transfn_worker(flinfo, fcinfo, false)
}

pub fn fc_jsonb_agg_strict_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    jsonb_agg_transfn_worker(flinfo, fcinfo, true)
}

fn finalize(fcinfo: &mut Fcinfo, name: &str, close: WjbToken) -> PgResult<Datum> {
    debug_assert!(
        // SAFETY: build-time tag check only.
        unsafe { fcinfo.agg_context() }.is_some(),
        "{name} called in non-aggregate context"
    );
    let arg0 = fcinfo.args_n::<1>()[0];
    if arg0.isnull {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    // SAFETY: non-null arg0 is the transfn chain's aggcontext state.
    let state = unsafe { &*(arg0.value.as_usize() as *const JsonbAggState) };
    let mut clone = state.push.clone_shallow()?;
    clone.push_token(close)?;
    let d = crate::builtins::image_result(convert_to_jsonb(mcx, &clone.finish())?);
    Ok(d)
}

pub fn fc_jsonb_agg_finalfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    finalize(fcinfo, "jsonb_agg_finalfn", WjbToken::EndArray)
}

/// C: jsonb_object_agg_transfn_worker.
fn jsonb_object_agg_transfn_worker(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("jsonb_object_agg_transfn needs a resolved FmgrInfo");
    let aggmcx = agg_mcx(fcinfo, "jsonb_object_agg_transfn")?;
    let [a, k, v] = *fcinfo.args_n::<3>();

    let state = if a.isnull {
        let key_type = funcapi::get_fn_expr_argtype(Some(flinfo), 1);
        let val_type = funcapi::get_fn_expr_argtype(Some(flinfo), 2);
        if key_type == types_core::InvalidOid || val_type == types_core::InvalidOid {
            return Err(no_input_type());
        }
        cats(flinfo, || {
            Ok(AggCats {
                val: json_categorize_type(val_type)?,
                key: Some(json_categorize_type(key_type)?),
            })
        })?;
        new_state(aggmcx, |ps| ps.push_object_start(unique_keys, absent_on_null))?
    } else {
        a.value.as_usize() as *mut JsonbAggState
    };
    // SAFETY: as jsonb_agg_transfn_worker.
    let state = unsafe { &mut *state };
    let state_datum = Datum::from_usize(state as *mut JsonbAggState as usize);

    if k.isnull {
        return Err(invalid_param("field name must not be null"));
    }
    let skip = absent_on_null && v.isnull;
    if skip && !unique_keys {
        return Ok(state_datum);
    }

    let mcx = fcinfo.result_mcx();
    let c = flinfo.fn_extra_mut::<AggCats>().expect("cats built on first call");
    let key_image = elem_image(mcx, k.value, false, c.key.as_mut().unwrap(), true)?;
    let val_image = elem_image(mcx, v.value, v.isnull, &mut c.val, false)?;

    // Key: must be a raw-scalar string.
    let key_payload = &key_image[4..];
    let mut it = JsonbIterator::init(mcx, key_payload)?;
    loop {
        let (tok, kv) = it.next(false);
        match tok {
            WjbToken::Done => break,
            WjbToken::BeginArray => {
                if !matches!(
                    kv,
                    JsonbItem::Array {
                        raw_scalar: true,
                        ..
                    }
                ) {
                    panic!("unexpected structure for key");
                }
            }
            WjbToken::Elem => {
                let JsonbItem::String(s) = kv else {
                    return Err(invalid_param("object keys must be strings"));
                };
                let key = copy_static(aggmcx, s)?;
                state.push.push(WjbToken::Key, JsonbItem::String(key))?;
                if skip {
                    state.push.push(WjbToken::Value, JsonbItem::Null)?;
                    return Ok(state_datum);
                }
            }
            WjbToken::EndArray => {}
            _ => panic!("unexpected structure for key"),
        }
    }

    accumulate_value(aggmcx, mcx, state, &val_image, true)?;
    Ok(state_datum)
}

pub fn fc_jsonb_object_agg_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    jsonb_object_agg_transfn_worker(flinfo, fcinfo, false, false)
}

pub fn fc_jsonb_object_agg_strict_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    jsonb_object_agg_transfn_worker(flinfo, fcinfo, true, false)
}

pub fn fc_jsonb_object_agg_unique_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    jsonb_object_agg_transfn_worker(flinfo, fcinfo, false, true)
}

pub fn fc_jsonb_object_agg_unique_strict_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    jsonb_object_agg_transfn_worker(flinfo, fcinfo, true, true)
}

pub fn fc_jsonb_object_agg_finalfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    finalize(fcinfo, "jsonb_object_agg_finalfn", WjbToken::EndObject)
}
