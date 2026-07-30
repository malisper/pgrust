//! json.c aggregate slice: json_agg[_strict] and json_object_agg[_strict]
//! trans/final functions over an INTERNAL aggcontext-lived StringInfo, plus
//! the _unique variants' json_unique_check_key hash-set (object_id is always
//! 0 here — flat object aggregation never nests JsonUniqueBuilderState).

extern crate alloc;

use crate::tojson::{
    datum_to_json_internal, json_categorize_type, no_input_type, JsonTypeCategory,
    TypeCat,
};
use datum::Datum;
use mcx::{Mcx, PgFxHashMap};
use stringinfo::StringInfo;
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

// C JsonUniqueBuilderState (object_id fixed at 0 for this flat builder —
// never records nested-object collisions); `skipped` is the throwaway
// key-only buffer for absent_on_null skips under unique_keys.
type UniqueKeys = PgFxHashMap<'static, &'static [u8], ()>;

struct UniqueCheck {
    keys: UniqueKeys,
    skipped: Option<StringInfo<'static>>,
}

// C: JsonAggState minus the category fields — those depend only on the
// declared argument types, so they live once per FmgrInfo (fn_extra memo),
// not per group. ManuallyDrop: the aggcontext arena resets wholesale.
struct JsonAggState {
    str: core::mem::ManuallyDrop<StringInfo<'static>>,
    unique: core::mem::ManuallyDrop<Option<UniqueCheck>>,
}

struct AggCats {
    val: TypeCat,
    key: Option<TypeCat>,
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
fn no_arg_type(argno: usize) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "could not determine data type for argument {argno}"
        ))
        .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn null_object_key() -> Box<PgError> {
    Box::new(
        PgError::error("null value not allowed for object key")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

fn agg_mcx<'a>(fcinfo: &Fcinfo, name: &str) -> PgResult<Mcx<'a>> {
    // SAFETY: context, if set, is the evaltrans build's AggStateNode, live
    // across every call through this frame.
    match unsafe { fcinfo.agg_context() } {
        Some(m) => Ok(m),
        None => Err(non_aggregate_context(name)),
    }
}

fn new_state(aggmcx: Mcx<'_>, open: &[u8], unique_keys: bool) -> PgResult<*mut JsonAggState> {
    const { assert!(!core::mem::needs_drop::<JsonAggState>()) }
    let mut str = StringInfo::new_in(aggmcx)?;
    str.append_bytes(open)?;
    // SAFETY: the aggcontext outlives every trans/final call of this node;
    // restamping the arena brand to 'static never outlives it.
    let str: StringInfo<'static> = unsafe { core::mem::transmute(str) };
    let unique: Option<UniqueCheck> = if unique_keys {
        let m: PgFxHashMap<'_, &[u8], ()> = PgFxHashMap::with_hasher_in(Default::default(), aggmcx);
        // SAFETY: as the str transmute above.
        let keys: UniqueKeys = unsafe { core::mem::transmute(m) };
        Some(UniqueCheck { keys, skipped: None })
    } else {
        None
    };
    let layout = core::alloc::Layout::new::<JsonAggState>();
    let raw = mcx::Allocator::allocate(&aggmcx, layout).map_err(|_| aggmcx.oom(layout.size()))?;
    let p = raw.cast::<JsonAggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe {
        p.write(JsonAggState {
            str: core::mem::ManuallyDrop::new(str),
            unique: core::mem::ManuallyDrop::new(unique),
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

/// C: json_agg_transfn_worker.
fn json_agg_transfn_worker(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    absent_on_null: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("json_agg_transfn needs a resolved FmgrInfo");
    let aggmcx = agg_mcx(fcinfo, "json_agg_transfn")?;
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
        new_state(aggmcx, b"[", false)?
    } else {
        a.value.as_usize() as *mut JsonAggState
    };
    // SAFETY: the state pointer is this transfn chain's aggcontext-lived
    // allocation; no other reference is live during the call.
    let state = unsafe { &mut *state };
    let state_datum = Datum::from_usize(state as *mut JsonAggState as usize);

    if absent_on_null && b.isnull {
        return Ok(state_datum);
    }

    if state.str.len() > 1 {
        state.str.append_bytes(b", ")?;
    }

    let mcx = fcinfo.result_mcx();
    let c = flinfo.fn_extra_mut::<AggCats>().expect("cats built on first call");
    if b.isnull {
        let mut null_cat = TypeCat::null();
        datum_to_json_internal(mcx, &mut *state.str, Datum::null(), true, &mut null_cat, false)?;
        return Ok(state_datum);
    }

    if !a.isnull
        && state.str.len() > 1
        && matches!(
            c.val.category,
            JsonTypeCategory::Array | JsonTypeCategory::Composite
        )
    {
        state.str.append_bytes(b"\n ")?;
    }

    datum_to_json_internal(mcx, &mut *state.str, b.value, false, &mut c.val, false)?;
    Ok(state_datum)
}

pub fn fc_json_agg_transfn(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    json_agg_transfn_worker(flinfo, fcinfo, false)
}

pub fn fc_json_agg_strict_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    json_agg_transfn_worker(flinfo, fcinfo, true)
}

// C: catenate_stringinfo_string — final functions may not modify the state.
fn finalize(fcinfo: &mut Fcinfo, name: &str, close: &[u8]) -> PgResult<Datum> {
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
    let state = unsafe { &*(arg0.value.as_usize() as *const JsonAggState) };
    let mut out: mcx::PgVec<'_, u8> =
        mcx::vec_with_capacity_in(mcx, state.str.len() + close.len())?;
    mcx::vec_append_bytes(&mut out, state.str.as_bytes())?;
    mcx::vec_append_bytes(&mut out, close)?;
    Ok(varlena_result(varlena::cstring_to_text(mcx, &out)?))
}

pub fn fc_json_agg_finalfn(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    finalize(fcinfo, "json_agg_finalfn", b"]")
}

#[track_caller]
#[cold]
#[inline(never)]
fn duplicate_json_object_key(key: &[u8]) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "duplicate JSON object key value: {}",
            String::from_utf8_lossy(key)
        ))
        .with_sqlstate(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE),
    )
}

// C json_unique_check_key: object_id is always 0 for this flat builder.
fn check_unique_key(aggmcx: Mcx<'_>, keys: &mut UniqueKeys, key: &[u8]) -> PgResult<bool> {
    let owned: &[u8] = mcx::slice_in(aggmcx, key)?.leak();
    // SAFETY: aggcontext outlives every trans/final call of this node.
    let owned: &'static [u8] = unsafe { core::mem::transmute(owned) };
    Ok(keys.insert(owned, ()).is_none())
}

/// C: json_object_agg_transfn_worker.
fn json_object_agg_transfn_worker(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("json_object_agg_transfn needs a resolved FmgrInfo");
    let aggmcx = agg_mcx(fcinfo, "json_object_agg_transfn")?;
    let [a, k, v] = *fcinfo.args_n::<3>();

    let state = if a.isnull {
        let key_type = funcapi::get_fn_expr_argtype(Some(flinfo), 1);
        if key_type == types_core::InvalidOid {
            return Err(no_arg_type(1));
        }
        let val_type = funcapi::get_fn_expr_argtype(Some(flinfo), 2);
        if val_type == types_core::InvalidOid {
            return Err(no_arg_type(2));
        }
        cats(flinfo, || {
            Ok(AggCats {
                val: json_categorize_type(val_type)?,
                key: Some(json_categorize_type(key_type)?),
            })
        })?;
        new_state(aggmcx, b"{ ", unique_keys)?
    } else {
        a.value.as_usize() as *mut JsonAggState
    };
    // SAFETY: as json_agg_transfn_worker.
    let state = unsafe { &mut *state };
    let state_datum = Datum::from_usize(state as *mut JsonAggState as usize);

    if k.isnull {
        return Err(null_object_key());
    }
    let skip = absent_on_null && v.isnull;

    if skip && !unique_keys {
        return Ok(state_datum);
    }

    let mcx = fcinfo.result_mcx();
    let c = flinfo.fn_extra_mut::<AggCats>().expect("cats built on first call");

    // C json_unique_builder_get_throwawaybuf: a key-only scratch buffer that
    // never enters the output, reset (not reallocated) on each skip.
    let key_offset;
    if skip {
        let scratch = state.str.allocator();
        let uc = state.unique.as_mut().expect("unique_keys builder present");
        let out = uc.skipped.get_or_insert_with(|| {
            // SAFETY: as the str transmute in new_state.
            unsafe { core::mem::transmute(StringInfo::new_in(scratch).expect("scratch buf")) }
        });
        out.truncate(0);
        key_offset = out.len();
        datum_to_json_internal(mcx, out, k.value, false, c.key.as_mut().unwrap(), true)?;
    } else {
        if state.str.len() > 2 {
            state.str.append_bytes(b", ")?;
        }
        key_offset = state.str.len();
        datum_to_json_internal(mcx, &mut *state.str, k.value, false, c.key.as_mut().unwrap(), true)?;
    }

    if unique_keys {
        let uc = state.unique.as_mut().unwrap();
        let key_bytes: &[u8] = if skip {
            &uc.skipped.as_ref().unwrap().as_bytes()[key_offset..]
        } else {
            &state.str.as_bytes()[key_offset..]
        };
        if !check_unique_key(aggmcx, &mut uc.keys, key_bytes)? {
            return Err(duplicate_json_object_key(key_bytes));
        }
        if skip {
            return Ok(state_datum);
        }
    }

    state.str.append_bytes(b" : ")?;
    let val = if v.isnull { Datum::null() } else { v.value };
    datum_to_json_internal(mcx, &mut *state.str, val, v.isnull, &mut c.val, false)?;
    Ok(state_datum)
}

pub fn fc_json_object_agg_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    json_object_agg_transfn_worker(flinfo, fcinfo, false, false)
}

pub fn fc_json_object_agg_strict_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    json_object_agg_transfn_worker(flinfo, fcinfo, true, false)
}

pub fn fc_json_object_agg_unique_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    json_object_agg_transfn_worker(flinfo, fcinfo, false, true)
}

pub fn fc_json_object_agg_unique_strict_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    json_object_agg_transfn_worker(flinfo, fcinfo, true, true)
}

pub fn fc_json_object_agg_finalfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    finalize(fcinfo, "json_object_agg_finalfn", b" }")
}
