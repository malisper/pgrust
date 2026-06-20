//! `fmgr`-callable wrappers for the `internal`-transtype `json` aggregates
//! (`json.c`): `json_agg`(3175) / `json_agg_strict`(6276), `json_object_agg`
//! (3197) and the strict/unique variants, built on the [`JsonAggState`] /
//! `json_agg_transfn_worker` / `json_object_agg_transfn_worker` machinery in
//! [`crate`].
//!
//! ## The `internal` transition state crosses the fmgr boundary
//!
//! C's transition state is a `void *` to a `JsonAggState` living in the
//! per-aggregate `MemoryContext`. Here it rides the canonical
//! `Datum::Internal(Box<dyn Any>)` arm (`RefPayload::Internal`), exactly like
//! `array_agg` (`backend-utils-adt-arrayfuncs::agg_fmgr`): nodeAgg moves the box
//! in/out of the call frame, the transfn mutates it in place, and returns the
//! same box. The state's `StringInfo str` (and the unique-key check) live in a
//! leaked `&'static MemoryContext` modeling the aggcontext the by-OID dispatch
//! cannot thread; the per-call value image is materialized into that same
//! context (small per-row leak, the repo-wide by-ref-free TODO).
//!
//! The polymorphic argument types (`anyelement` for `json_agg`, the two `"any"`
//! arguments for `json_object_agg`) resolve via `get_fn_expr_argtype`, off the
//! `build_aggregate_transfn_expr` call node nodeAgg stamps onto `flinfo`.

use mcx::MemoryContext;
use types_core::Oid;
use types_datum::datum::Datum as BoundaryDatum;
use types_error::PgResult;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_tuple::Datum as ValDatum;

use crate::{
    json_agg_finalfn, json_agg_strict_transfn, json_agg_transfn, json_object_agg_finalfn,
    json_object_agg_strict_transfn, json_object_agg_transfn, json_object_agg_unique_strict_transfn,
    json_object_agg_unique_transfn, JsonAggState,
};

/// The leaked per-aggregate context plus the running `JsonAggState` bound to it.
/// `'static` because that context lives for the whole backend (C resets it per
/// group; we leak — the repo-wide by-ref-free TODO).
struct JsonAggInternal {
    /// The leaked per-aggregate `MemoryContext` (kept alive for `Drop` parity;
    /// the state below borrows its `'static` mcx).
    _ctx: &'static MemoryContext,
    /// The running `JsonAggState`, with its `StringInfo`/element copies charged
    /// to `_ctx`.
    state: JsonAggState<'static>,
}

impl JsonAggInternal {
    /// Wrap `state` (already allocated in the leaked context) into a carrier.
    fn from_state(ctx: &'static MemoryContext, state: JsonAggState<'static>) -> Box<JsonAggInternal> {
        Box::new(JsonAggInternal { _ctx: ctx, state })
    }
}

/// A fresh leaked aggcontext (`'static` so the state outlives every transfn call
/// without a borrow).
fn new_agg_ctx() -> &'static MemoryContext {
    Box::leak(Box::new(MemoryContext::new("json_agg state")))
}

/// `PG_ARGISNULL(i)`.
#[inline]
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `get_fn_expr_argtype(fcinfo->flinfo, i)` — the resolved (polymorphic) type
/// OID of argument `i`.
#[inline]
fn fn_expr_argtype(fcinfo: &FunctionCallInfoBaseData, i: i32) -> Oid {
    backend_utils_fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i)
}

/// Take the `internal` transition state out of `args[0]`. `None` is C's
/// `PG_ARGISNULL(0)` (first call).
fn take_state(fcinfo: &mut FunctionCallInfoBaseData) -> Option<Box<JsonAggInternal>> {
    if arg_isnull(fcinfo, 0) {
        return None;
    }
    match fcinfo.take_ref_arg(0) {
        Some(RefPayload::Internal(b)) => Some(b.downcast::<JsonAggInternal>().unwrap_or_else(|_| {
            panic!("json_agg fn: args[0] internal state is not a JsonAggInternal")
        })),
        Some(other) => panic!("json_agg fn: args[0] is not an internal state ({other:?})"),
        None => None,
    }
}

/// `PG_RETURN_POINTER(state)` — hand the transition state back as `internal`.
fn ret_internal(fcinfo: &mut FunctionCallInfoBaseData, state: Box<dyn core::any::Any>) -> BoundaryDatum {
    fcinfo.set_ref_result(RefPayload::Internal(state));
    BoundaryDatum::from_usize(0)
}

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> BoundaryDatum {
    fcinfo.set_result_null(true);
    BoundaryDatum::from_usize(0)
}

/// `VARHDRSZ` — the uncompressed 4-byte varlena length-word size.
const VARHDRSZ: usize = 4;

/// Set a `json` (by-reference) result on the by-ref lane, framed header-ful
/// (`SET_VARSIZE(image, VARHDRSZ + len)`), and return the dummy by-value word.
fn ret_json(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> BoundaryDatum {
    let total = VARHDRSZ + payload.len();
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    image.extend_from_slice(payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    BoundaryDatum::from_usize(0)
}

/// Materialize argument `i` as the unified `types_tuple::Datum` the json value
/// cores consume, charging by-ref copies to `mcx` (the aggcontext). The arg may
/// be NULL — the caller passes the matching `is_null` flag separately.
fn arg_value<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> PgResult<ValDatum<'mcx>> {
    Ok(match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => ValDatum::ByRef(mcx::slice_in(mcx, b)?),
        Some(RefPayload::Cstring(s)) => ValDatum::Cstring(s.clone()),
        Some(RefPayload::Composite(image)) => {
            ValDatum::Composite(types_tuple::FormedTuple::from_datum_image(mcx, image)?)
        }
        Some(RefPayload::Expanded(eo)) => ValDatum::ByRef(mcx::slice_in(
            mcx,
            &types_datum::flatten_expanded(eo.as_ref()),
        )?),
        Some(RefPayload::Internal(_)) => {
            panic!("json_agg fn: unexpected `internal` argument on the by-ref lane")
        }
        None => ValDatum::ByVal(
            fcinfo
                .arg(i)
                .map(|d| d.value.as_usize())
                .unwrap_or(0),
        ),
    })
}

// ===========================================================================
// json_agg(3175) / json_agg_strict(6276): transfn + shared finalfn(json_agg_finalfn).
// ===========================================================================

/// Common driver for `json_agg_transfn` (`absent_on_null = false`) and
/// `json_agg_strict_transfn` (`absent_on_null = true`).
fn json_agg_transfn_impl(
    fcinfo: &mut FunctionCallInfoBaseData,
    strict: bool,
) -> PgResult<BoundaryDatum> {
    let arg_type = fn_expr_argtype(fcinfo, 1);
    let val_is_null = arg_isnull(fcinfo, 1);

    let carrier = take_state(fcinfo);
    let (ctx, prev_state) = match carrier {
        Some(c) => (c._ctx, Some(c.state)),
        None => (new_agg_ctx(), None),
    };
    let mcx = ctx.mcx();
    let val = arg_value(mcx, fcinfo, 1)?;

    let new_state = if strict {
        json_agg_strict_transfn(mcx, prev_state, arg_type, &val, val_is_null)
    } else {
        json_agg_transfn(mcx, prev_state, arg_type, &val, val_is_null)
    }?;

    Ok(ret_internal(fcinfo, JsonAggInternal::from_state(ctx, new_state)))
}

fn fc_json_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    json_agg_transfn_impl(fcinfo, false)
}

fn fc_json_agg_strict_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    json_agg_transfn_impl(fcinfo, true)
}

/// `json_agg_finalfn`(3176 — shared finalfn for json_agg / json_agg_strict):
/// `[` + accumulated elements + `]`, or NULL for the no-rows case.
fn fc_json_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    match take_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(carrier) => {
            let m = MemoryContext::new("json_agg finalfn");
            let out: Option<Vec<u8>> =
                json_agg_finalfn(m.mcx(), Some(&carrier.state))?.map(|b| b.as_slice().to_vec());
            Ok(match out {
                None => ret_null(fcinfo),
                Some(bytes) => ret_json(fcinfo, &bytes),
            })
        }
    }
}

// ===========================================================================
// json_object_agg(3197) + strict / unique / unique_strict variants.
// ===========================================================================

/// `JsonObjectAggKind` — selects the underlying `_worker` flag pair.
#[derive(Clone, Copy)]
enum ObjAggKind {
    Plain,
    Strict,
    Unique,
    UniqueStrict,
}

fn json_object_agg_transfn_impl(
    fcinfo: &mut FunctionCallInfoBaseData,
    kind: ObjAggKind,
) -> PgResult<BoundaryDatum> {
    let key_arg_type = fn_expr_argtype(fcinfo, 1);
    let val_arg_type = fn_expr_argtype(fcinfo, 2);
    let key_is_null = arg_isnull(fcinfo, 1);
    let val_is_null = arg_isnull(fcinfo, 2);

    let carrier = take_state(fcinfo);
    let (ctx, prev_state) = match carrier {
        Some(c) => (c._ctx, Some(c.state)),
        None => (new_agg_ctx(), None),
    };
    let mcx = ctx.mcx();
    let key = arg_value(mcx, fcinfo, 1)?;
    let val = arg_value(mcx, fcinfo, 2)?;

    let new_state = match kind {
        ObjAggKind::Plain => json_object_agg_transfn(
            mcx, prev_state, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
        ),
        ObjAggKind::Strict => json_object_agg_strict_transfn(
            mcx, prev_state, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
        ),
        ObjAggKind::Unique => json_object_agg_unique_transfn(
            mcx, prev_state, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
        ),
        ObjAggKind::UniqueStrict => json_object_agg_unique_strict_transfn(
            mcx, prev_state, key_arg_type, val_arg_type, &key, key_is_null, &val, val_is_null,
        ),
    }?;

    Ok(ret_internal(fcinfo, JsonAggInternal::from_state(ctx, new_state)))
}

fn fc_json_object_agg_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    json_object_agg_transfn_impl(fcinfo, ObjAggKind::Plain)
}

fn fc_json_object_agg_strict_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    json_object_agg_transfn_impl(fcinfo, ObjAggKind::Strict)
}

fn fc_json_object_agg_unique_transfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    json_object_agg_transfn_impl(fcinfo, ObjAggKind::Unique)
}

fn fc_json_object_agg_unique_strict_transfn(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<BoundaryDatum> {
    json_object_agg_transfn_impl(fcinfo, ObjAggKind::UniqueStrict)
}

/// `json_object_agg_finalfn`(3198 — shared): `{` + pairs + ` }`, or NULL.
fn fc_json_object_agg_finalfn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<BoundaryDatum> {
    match take_state(fcinfo) {
        None => Ok(ret_null(fcinfo)),
        Some(carrier) => {
            let m = MemoryContext::new("json_object_agg finalfn");
            let out: Option<Vec<u8>> = json_object_agg_finalfn(m.mcx(), Some(&carrier.state))?
                .map(|b| b.as_slice().to_vec());
            Ok(match out {
                None => ret_null(fcinfo),
                Some(bytes) => ret_json(fcinfo, &bytes),
            })
        }
    }
}

// ===========================================================================
// Registration (C: the `fmgr_builtins[]` rows; transition/final functions are
// `proisstrict => 'f'`, handling the NULL `internal` state / NULL input
// themselves). OIDs from pg_proc.dat.
// ===========================================================================

pub fn register_json_agg_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // json_agg family: transfn(internal, anyelement), finalfn(internal).
        builtin(3173, "json_agg_transfn", 2, fc_json_agg_transfn),
        builtin(3174, "json_agg_finalfn", 1, fc_json_agg_finalfn),
        builtin(6275, "json_agg_strict_transfn", 2, fc_json_agg_strict_transfn),
        // json_object_agg family: transfn(internal, "any", "any"), finalfn(internal).
        builtin(3180, "json_object_agg_transfn", 3, fc_json_object_agg_transfn),
        builtin(3196, "json_object_agg_finalfn", 1, fc_json_object_agg_finalfn),
        builtin(
            6277,
            "json_object_agg_strict_transfn",
            3,
            fc_json_object_agg_strict_transfn,
        ),
        builtin(
            6278,
            "json_object_agg_unique_transfn",
            3,
            fc_json_object_agg_unique_transfn,
        ),
        builtin(
            6279,
            "json_object_agg_unique_strict_transfn",
            3,
            fc_json_object_agg_unique_strict_transfn,
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
