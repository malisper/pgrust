//! `fmgr`-callable wrappers for the `internal`-transtype `jsonb` aggregates
//! (`jsonb.c`): `jsonb_agg`(3267) / `jsonb_agg_strict`(6284), `jsonb_object_agg`
//! (3270) and the strict/unique variants, built on the [`JsonbAggState`] /
//! `jsonb_agg_transfn_worker` / `jsonb_object_agg_transfn_worker` machinery in
//! [`crate`].
//!
//! ## The `internal` transition state crosses the fmgr boundary
//!
//! C's transition state is a `void *` to a `JsonbAggState` in the per-aggregate
//! `MemoryContext`. Here it rides the canonical `Datum::Internal(Box<dyn Any>)`
//! arm (`RefPayload::Internal`), exactly like `array_agg` / the `json` aggregates
//! (`backend-utils-adt-json::agg_fmgr`): nodeAgg moves the box in/out of the call
//! frame, the transfn mutates it in place, and returns the same box.
//!
//! [`JsonbAggState`] carries no `'mcx` lifetime (its `JsonbParseState` /
//! `JsonbValue` content is global-allocator owned), so the running state is
//! `'static` directly — no leaked aggcontext is needed for it. A per-call scratch
//! context backs the materialized value image and the finalfn's
//! `JsonbValueToJsonb` output.
//!
//! The polymorphic argument types (`anyelement` for `jsonb_agg`, the two `"any"`
//! arguments for `jsonb_object_agg`) resolve via `get_fn_expr_argtype`.

use ::mcx::MemoryContext;
use ::types_core::Oid;
use ::datum::datum::Datum as BoundaryDatum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use ::types_tuple::Datum as ValDatum;

use crate::{
    jsonb_agg_finalfn, jsonb_agg_strict_transfn, jsonb_agg_transfn, jsonb_object_agg_finalfn,
    jsonb_object_agg_strict_transfn, jsonb_object_agg_transfn,
    jsonb_object_agg_unique_strict_transfn, jsonb_object_agg_unique_transfn, JsonbAggOwned,
    JsonbAggState,
};

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `get_fn_expr_argtype(fcinfo->flinfo, i)`.
#[inline]
fn fn_expr_argtype(fcinfo: &FunctionCallInfoBaseData, i: i32) -> Oid {
    fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i)
}

/// Take the `internal` transition state out of `args[0]`. `None` is C's
/// `PG_ARGISNULL(0)` (first call).
///
/// The state is the persistent, context-owning [`JsonbAggOwned`]: its working
/// `JsonbValue` tree lives in its own [`McxOwned`] arena (the C aggregate
/// context), so it survives across transition calls and is bulk-freed when the
/// handle drops.
fn take_state(fcinfo: &mut FunctionCallInfoBaseData) -> Option<Box<JsonbAggOwned>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(b.downcast::<JsonbAggOwned>().unwrap_or_else(|_| {
            panic!("jsonb_agg fn: args[0] internal state is not a JsonbAggOwned")
        })),
        Some(other) => panic!("jsonb_agg fn: args[0] is not an internal state ({other:?})"),
        None => None,
    }
}

/// Create a fresh, empty persistent aggregate state in its own context (the C
/// aggregate context).  The working tree is built into this arena across the
/// aggregation and bulk-freed when the handle drops.
fn new_agg_owned() -> Box<JsonbAggOwned> {
    let owned = JsonbAggOwned::try_new(MemoryContext::new("jsonb agg state"), |mcx| {
        let _ = mcx;
        Ok(JsonbAggState::default())
    })
    .expect("jsonb agg: allocating the aggregate-state context");
    Box::new(owned)
}

/// `PG_RETURN_POINTER(state)` — hand the persistent transition state back as
/// `internal`.
fn ret_internal(
    fcinfo: &mut FunctionCallInfoBaseData,
    state: Box<JsonbAggOwned>,
) -> BoundaryDatum {
    fcinfo.set_ref_result(RefPayload::Internal(state));
    BoundaryDatum::from_usize(0)
}

/// Restore an `internal` JsonbAggState into `args[0]` after a *final* function
/// read it. C's `PG_GETARG_POINTER(0)` does NOT consume the state; the same live
/// state must survive for a sharing aggregate's finalfn and, in a moving window
/// frame, for the next row's forward/inverse transition (mirrors numeric's
/// `keep_internal`).
#[inline]
fn keep_state(fcinfo: &mut FunctionCallInfoBaseData, state: Box<JsonbAggOwned>) {
    fcinfo.set_ref_arg(0, RefPayload::Internal(state));
}

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> BoundaryDatum {
    fcinfo.set_result_null(true);
    BoundaryDatum::from_usize(0)
}

/// Set a `jsonb` (by-reference) result on the by-ref lane. `JsonbValueToJsonb`
/// already produces the full varlena image (header + body), so hand it back
/// verbatim (matching this crate's `fmgr_builtins::ret_jsonb`).
fn ret_jsonb(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> BoundaryDatum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    BoundaryDatum::from_usize(0)
}

/// Materialize argument `i` as the unified `::types_tuple::Datum`, charging by-ref
/// copies to `mcx`.
fn arg_value<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> PgResult<ValDatum<'mcx>> {
    Ok(match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => ValDatum::ByRef(::mcx::slice_in(mcx, b)?),
        Some(RefPayload::Cstring(s)) => ValDatum::Cstring(s.clone()),
        Some(RefPayload::Composite(image)) => {
            ValDatum::Composite(::types_tuple::FormedTuple::from_datum_image(mcx, image)?)
        }
        Some(RefPayload::Expanded(eo)) => ValDatum::ByRef(::mcx::slice_in(
            mcx,
            &::datum::flatten_expanded(eo.as_ref()),
        )?),
        Some(RefPayload::Internal(_)) => {
            panic!("jsonb_agg fn: unexpected `internal` argument on the by-ref lane")
        }
        None => ValDatum::ByVal(fcinfo.arg(i).map(|d| d.value.as_usize()).unwrap_or(0)),
    })
}

// ===========================================================================
// jsonb_agg(3267) / jsonb_agg_strict(6284): transfn + shared finalfn.
// ===========================================================================

fn jsonb_agg_transfn_impl(
    fcinfo: &mut FunctionCallInfoBaseData,
    strict: bool,
) -> PgResult<BoundaryDatum> {
    let arg_type = fn_expr_argtype(fcinfo, 1);
    let val_is_null = arg_isnull(fcinfo, 1);
    // The persistent state owns its arena (the C aggregate context). On the
    // first call create it; otherwise re-enter the existing one. The argument
    // and the working tree both live in that ONE arena, so a spliced element
    // outlives the call exactly as C copies it into the aggregate context.
    let (mut owned, first) = match take_state(fcinfo) {
        Some(o) => (o, false),
        None => (new_agg_owned(), true),
    };
    owned.with_mut_mcx(|mcx, state| {
        let val = arg_value(mcx, fcinfo, 1)?;
        // `None` drives the worker's first-call init (BEGIN_ARRAY + categorize);
        // on later calls the accumulated `state` is threaded back in.
        let prev = if first { None } else { Some(core::mem::take(state)) };
        *state = if strict {
            jsonb_agg_strict_transfn(mcx, prev, arg_type, &val, val_is_null)
        } else {
            jsonb_agg_transfn(mcx, prev, arg_type, &val, val_is_null)
        }?;
        Ok(())
    })?;
    Ok(ret_internal(fcinfo, owned))
}

fn fc_jsonb_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    jsonb_agg_transfn_impl(fcinfo, false)
}

fn fc_jsonb_agg_strict_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    jsonb_agg_transfn_impl(fcinfo, true)
}

/// `jsonb_agg_finalfn`(shared): close the array; NULL for the no-rows case.
fn fc_jsonb_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    match take_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(mut state) => {
            // Run the finalfn inside the state's own arena so the closed-array
            // working tree and the serialized output share its lifetime; copy
            // the resulting varlena out to an owned `Vec` before the arena
            // borrow ends (the by-ref result lane keeps its own owned image).
            let out: Option<Vec<u8>> = state.with_mut_mcx(|mcx, s| {
                Ok(jsonb_agg_finalfn(mcx, Some(s))?.map(|b| b.as_slice().to_vec()))
            })?;
            // C `PG_GETARG_POINTER(0)` does not consume the state; restore it.
            keep_state(fcinfo, state);
            Ok(match out {
                None => ret_null(fcinfo),
                Some(image) => ret_jsonb(fcinfo, image),
            })
        }
    }
}

// ===========================================================================
// jsonb_object_agg(3270) + strict / unique / unique_strict variants.
// ===========================================================================

#[derive(Clone, Copy)]
enum ObjAggKind {
    Plain,
    Strict,
    Unique,
    UniqueStrict,
}

fn jsonb_object_agg_transfn_impl(
    fcinfo: &mut FunctionCallInfoBaseData,
    kind: ObjAggKind,
) -> PgResult<BoundaryDatum> {
    let key_arg_type = fn_expr_argtype(fcinfo, 1);
    let val_arg_type = fn_expr_argtype(fcinfo, 2);
    let key_is_null = arg_isnull(fcinfo, 1);
    let val_is_null = arg_isnull(fcinfo, 2);
    let (mut owned, first) = match take_state(fcinfo) {
        Some(o) => (o, false),
        None => (new_agg_owned(), true),
    };
    owned.with_mut_mcx(|mcx, state| {
        let key = arg_value(mcx, fcinfo, 1)?;
        let val = arg_value(mcx, fcinfo, 2)?;
        let prev = if first { None } else { Some(core::mem::take(state)) };
        *state = match kind {
            ObjAggKind::Plain => jsonb_object_agg_transfn(
                mcx, prev, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
            ),
            ObjAggKind::Strict => jsonb_object_agg_strict_transfn(
                mcx, prev, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
            ),
            ObjAggKind::Unique => jsonb_object_agg_unique_transfn(
                mcx, prev, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
            ),
            ObjAggKind::UniqueStrict => jsonb_object_agg_unique_strict_transfn(
                mcx, prev, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
            ),
        }?;
        Ok(())
    })?;

    Ok(ret_internal(fcinfo, owned))
}

fn fc_jsonb_object_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    jsonb_object_agg_transfn_impl(fcinfo, ObjAggKind::Plain)
}

fn fc_jsonb_object_agg_strict_transfn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<BoundaryDatum> {
    jsonb_object_agg_transfn_impl(fcinfo, ObjAggKind::Strict)
}

fn fc_jsonb_object_agg_unique_transfn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<BoundaryDatum> {
    jsonb_object_agg_transfn_impl(fcinfo, ObjAggKind::Unique)
}

fn fc_jsonb_object_agg_unique_strict_transfn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<BoundaryDatum> {
    jsonb_object_agg_transfn_impl(fcinfo, ObjAggKind::UniqueStrict)
}

/// `jsonb_object_agg_finalfn`(shared): close the object; NULL for no-rows.
fn fc_jsonb_object_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    match take_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(mut state) => {
            let out: Option<Vec<u8>> = state.with_mut_mcx(|mcx, s| {
                Ok(jsonb_object_agg_finalfn(mcx, Some(s))?.map(|b| b.as_slice().to_vec()))
            })?;
            // C `PG_GETARG_POINTER(0)` does not consume the state; restore it.
            keep_state(fcinfo, state);
            Ok(match out {
                None => ret_null(fcinfo),
                Some(image) => ret_jsonb(fcinfo, image),
            })
        }
    }
}

// ===========================================================================
// Registration (C: the `fmgr_builtins[]` rows; transition/final functions are
// `proisstrict => 'f'`). OIDs from pg_proc.dat / builtin_canonical.
// ===========================================================================

pub fn register_jsonb_agg_builtins() {
    fmgr_core::register_builtins_native([
        builtin(3265, "jsonb_agg_transfn", 2, fc_jsonb_agg_transfn),
        builtin(3266, "jsonb_agg_finalfn", 1, fc_jsonb_agg_finalfn),
        builtin(6283, "jsonb_agg_strict_transfn", 2, fc_jsonb_agg_strict_transfn),
        builtin(3268, "jsonb_object_agg_transfn", 3, fc_jsonb_object_agg_transfn),
        builtin(3269, "jsonb_object_agg_finalfn", 1, fc_jsonb_object_agg_finalfn),
        builtin(
            6285,
            "jsonb_object_agg_strict_transfn",
            3,
            fc_jsonb_object_agg_strict_transfn,
        ),
        builtin(
            6286,
            "jsonb_object_agg_unique_transfn",
            3,
            fc_jsonb_object_agg_unique_transfn,
        ),
        builtin(
            6287,
            "jsonb_object_agg_unique_strict_transfn",
            3,
            fc_jsonb_object_agg_unique_strict_transfn,
        ),
    ]);
}

/// A non-strict (`proisstrict => 'f'`) native builtin row (`func: None`; the
/// dispatch goes through the `NATIVE` overlay and threads `Err` with `?`).
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
