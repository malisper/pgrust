//! The planner support-function `SupportRequestRows` dispatch table.
//!
//! `get_function_rows` (plancat.c:2186) estimates the rowcount of a
//! set-returning function by calling the function's `pg_proc.prosupport`
//! support function with a `SupportRequestRows` node (the FuncExpr + the
//! `PlannerInfo`); the support function reads the call's argument values and
//! returns an estimate, or declines (leaving the caller to fall back on
//! `pg_proc.prorows`). The dispatch is by the `prosupport` OID.
//!
//! The owned model decomposes the request: the [`call_support_rows`] entry
//! hands the support kernel the function OID and the (already const-folded)
//! `FuncExpr` node. This table is the `prosupport`-OID counterpart of fmgr's
//! builtin table for the `SupportRequestRows` request: each support-bearing
//! crate registers its decomposed row-estimate kernel here from its own
//! `init_seams`.
//!
//! A `prosupport` OID with no registered rows kernel, or a kernel that declines
//! (no constant arguments), returns `Ok(None)` — the caller then falls back on
//! `pg_proc.prorows`, exactly as in C.
//!
//! Process-global, like the executor-frame SRF table and the support-simplify
//! table.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use types_core::Oid;
use types_error::PgResult;
use nodes::primnodes::Expr;

/// A decomposed `SupportRequestRows` kernel: the function OID and the call's
/// (const-folded) `FuncExpr` node, mirroring what the support function reads off
/// `req->funcid`/`req->node` in C. Returns the row estimate (`Ok(Some)`) or a
/// decline (`Ok(None)`); `Err` carries the support function's `ereport(ERROR)`.
pub type SupportRowsFn = fn(funcid: Oid, node: &Expr) -> PgResult<Option<f64>>;

fn table() -> &'static Mutex<HashMap<Oid, SupportRowsFn>> {
    static SUPPORT_ROWS_TABLE: OnceLock<Mutex<HashMap<Oid, SupportRowsFn>>> = OnceLock::new();
    SUPPORT_ROWS_TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The by-`prosrc`-symbol counterpart of [`table`], for support functions whose
/// `prosupport` OID is assigned dynamically at `CREATE FUNCTION` time (e.g. a
/// user-created C-language support function like the regress test's
/// `test_support_func`, or `my_gen_series ... SUPPORT test_support_func`): they
/// cannot be keyed by a fixed builtin OID, so the dispatch resolves the OID's
/// `prosrc` symbol and looks it up here, mirroring fmgr's by-`prosrc`
/// C-language resolution.
fn symbol_table() -> &'static Mutex<HashMap<String, SupportRowsFn>> {
    static T: OnceLock<Mutex<HashMap<String, SupportRowsFn>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a decomposed `SupportRequestRows` kernel under its `prosupport` OID.
/// Returns the previous registration if the OID was already present.
pub fn register_support_rows(prosupport: Oid, func: SupportRowsFn) -> Option<SupportRowsFn> {
    table()
        .lock()
        .expect("support-rows table lock")
        .insert(prosupport, func)
}

/// Register a decomposed `SupportRequestRows` kernel under its `prosrc` symbol
/// name (for dynamically-OID'd support functions). Returns any previous one.
pub fn register_support_rows_by_symbol(prosrc: &str, func: SupportRowsFn) -> Option<SupportRowsFn> {
    symbol_table()
        .lock()
        .expect("support-rows symbol table lock")
        .insert(prosrc.to_string(), func)
}

/// `call_support_rows` by `prosrc` symbol — the dynamic-OID fallback. Returns
/// `Ok(None)` when no kernel is registered under the symbol (decline).
pub fn call_support_rows_by_symbol(
    prosrc: &str,
    funcid: Oid,
    node: &Expr,
) -> PgResult<Option<f64>> {
    let func = symbol_table()
        .lock()
        .expect("support-rows symbol table lock")
        .get(prosrc)
        .copied();
    match func {
        Some(f) => f(funcid, node),
        None => Ok(None),
    }
}

/// `call_support_rows(prosupport, funcid, node)` — the decomposed
/// `SupportRequestRows` dispatch (plancat.c:2200-2213). Resolve `prosupport` in
/// the table and run the support function's rows kernel; an OID with no
/// registered kernel (or a kernel that declines) returns `Ok(None)`, the
/// faithful counterpart of "no support function, or it failed, so rely on
/// prorows".
pub fn call_support_rows(prosupport: Oid, funcid: Oid, node: &Expr) -> PgResult<Option<f64>> {
    let func = table()
        .lock()
        .expect("support-rows table lock")
        .get(&prosupport)
        .copied();
    match func {
        Some(f) => f(funcid, node),
        None => Ok(None),
    }
}

// ===========================================================================
// Built-in `SupportRequestRows` kernels (registered from this crate's
// `init_seams`). These mirror `generate_series_int{4,8}_support`'s
// `SupportRequestRows` leg (int.c:1614 / int8.c) over the already const-folded
// `FuncExpr` argument list. C reads the args with `estimate_expression_value`;
// at this planning point preprocess_expression has already const-folded the
// FuNCTION RTE's funcexprs, so the args are `Const` nodes (or non-constant, in
// which case we decline, exactly as C does for a non-`Const` argument).
// ===========================================================================

/// `generate_series_int4_support` rows OIDs (`pg_proc`): the int4 series
/// support function is 3994.
pub const GENERATE_SERIES_INT4_SUPPORT: Oid = 3994;
/// `generate_series_int8_support` is 3995.
pub const GENERATE_SERIES_INT8_SUPPORT: Oid = 3995;
/// `generate_series_timestamp_support` is 6354 (covers both the timestamp and
/// timestamptz series, whose pg_proc rows share this prosupport).
pub const GENERATE_SERIES_TIMESTAMP_SUPPORT: Oid = 6354;

/// Register the built-in support-rows kernels in the dispatch table.
pub fn register_builtin_support_rows() {
    register_support_rows(GENERATE_SERIES_INT4_SUPPORT, generate_series_int4_support_rows);
    register_support_rows(GENERATE_SERIES_INT8_SUPPORT, generate_series_int8_support_rows);
    register_support_rows(
        GENERATE_SERIES_TIMESTAMP_SUPPORT,
        generate_series_timestamp_support_rows,
    );
}

/// Read the `i64`-valued constant from a (const-folded) argument `Expr`. Returns
/// `Some(Some(v))` for a non-NULL `Const`, `Some(None)` for a `Const` NULL, and
/// `None` for a non-`Const` (decline). Used for both int4 and int8 (int4
/// `Const`s carry their value in the low 32 bits of the Datum).
fn const_int_arg(arg: &Expr, is_int8: bool) -> Option<Option<i64>> {
    let Some(c) = arg.as_const() else {
        return None;
    };
    if c.constisnull {
        return Some(None);
    }
    let v = if is_int8 {
        c.constvalue.as_i64()
    } else {
        c.constvalue.as_i32() as i64
    };
    Some(Some(v))
}

/// `generate_series_int4_support`'s `SupportRequestRows` estimate (int.c:1659-
/// 1685): `floor((finish - start + step) / step)` in double arithmetic, or 0
/// rows if any argument is a constant NULL, or decline if any argument is
/// non-constant.
fn generate_series_int4_support_rows(_funcid: Oid, node: &Expr) -> PgResult<Option<f64>> {
    generate_series_support_rows(node, false)
}

/// `generate_series_int8_support`'s `SupportRequestRows` estimate (int8.c).
fn generate_series_int8_support_rows(_funcid: Oid, node: &Expr) -> PgResult<Option<f64>> {
    generate_series_support_rows(node, true)
}

fn generate_series_support_rows(node: &Expr, is_int8: bool) -> PgResult<Option<f64>> {
    // if (is_funcclause(req->node)) — be paranoid.
    let Some(fexpr) = node.as_funcexpr() else {
        return Ok(None);
    };
    let args = &fexpr.args;
    if args.len() < 2 {
        return Ok(None);
    }

    let arg1 = const_int_arg(&args[0], is_int8);
    let arg2 = const_int_arg(&args[1], is_int8);
    let arg3 = if args.len() >= 3 {
        Some(const_int_arg(&args[2], is_int8))
    } else {
        None
    };

    // If any argument is constant NULL, zero rows are returned.
    let is_const_null = |a: &Option<Option<i64>>| matches!(a, Some(None));
    if is_const_null(&arg1)
        || is_const_null(&arg2)
        || matches!(arg3, Some(Some(None)))
    {
        return Ok(Some(0.0));
    }

    // Otherwise, if they're all non-NULL constants, compute the rowcount. If
    // any argument is non-constant, decline.
    let start = match arg1 {
        Some(Some(v)) => v as f64,
        _ => return Ok(None),
    };
    let finish = match arg2 {
        Some(Some(v)) => v as f64,
        _ => return Ok(None),
    };
    let step = match arg3 {
        None => 1.0,                       // 2-arg form: step defaults to 1
        Some(Some(Some(v))) => v as f64,
        Some(_) => return Ok(None),        // non-constant step
    };

    // This equation works for either sign of step.
    if step != 0.0 {
        Ok(Some(((finish - start + step) / step).floor()))
    } else {
        Ok(None)
    }
}

/// `DAYS_PER_MONTH` (datatype/timestamp.h).
const DAYS_PER_MONTH: f64 = 30.0;
/// `USECS_PER_DAY` (datatype/timestamp.h).
const USECS_PER_DAY: f64 = 86_400_000_000.0;
/// `TIMESTAMP_NOT_FINITE(t)` — `±infinity` are `PG_INT64_MIN`/`PG_INT64_MAX`.
fn timestamp_not_finite(t: i64) -> bool {
    t == i64::MIN || t == i64::MAX
}

/// `generate_series_timestamp_support`'s `SupportRequestRows` estimate
/// (timestamp.c:6849). Shared by the timestamp and timestamptz series (same
/// prosupport). The C path is `start = arg1; finish = arg2; step = arg3;` then,
/// for finite endpoints with no `finish - start` overflow, `diff =
/// timestamp_mi(finish, start)` and `rows = floor(ddiff / dstep + 1.0)` where
/// the `INTERVAL_TO_MICROSECONDS` of `diff`/`step` is `(month*30 + day)*USECS_PER_DAY
/// + time`. `timestamp_mi` produces an interval with `month = 0`, `time =
/// finish - start`, then `interval_justify_hours` rolls whole days from `time`
/// into `day`; since `day*USECS_PER_DAY + time` reconstructs the same total, the
/// diff's microsecond value is exactly `finish - start`. So we compute it
/// directly, skipping the justify round-trip.
fn generate_series_timestamp_support_rows(_funcid: Oid, node: &Expr) -> PgResult<Option<f64>> {
    // if (is_funcclause(req->node)) — be paranoid.
    let Some(fexpr) = node.as_funcexpr() else {
        return Ok(None);
    };
    if fexpr.args.len() < 3 {
        return Ok(None);
    }

    // C: arg{1,2,3} = estimate_expression_value(req->root, l{initial,second,third}(args)).
    // The argument Exprs may still be unfolded casts (e.g. `TIMESTAMPTZ '...'`
    // is a CoerceViaIO/FuncExpr until folded), so run them through
    // estimate_expression_value — exactly as the numeric series support does —
    // before classifying them as Const. The fold runs in a transient context.
    let cx = mcx::MemoryContext::new("generate_series_timestamp_support rows");
    let mcx = cx.mcx();
    let arg1 = crate::fold::estimate_expression_value(mcx, fexpr.args[0].clone_in(mcx)?)?;
    let arg2 = crate::fold::estimate_expression_value(mcx, fexpr.args[1].clone_in(mcx)?)?;
    let arg3 = crate::fold::estimate_expression_value(mcx, fexpr.args[2].clone_in(mcx)?)?;

    let c1 = arg1.as_const();
    let c2 = arg2.as_const();
    let c3 = arg3.as_const();

    // If any argument is a constant NULL, zero rows are returned.
    if matches!(&c1, Some(c) if c.constisnull)
        || matches!(&c2, Some(c) if c.constisnull)
        || matches!(&c3, Some(c) if c.constisnull)
    {
        return Ok(Some(0.0));
    }

    // Otherwise, all three must be non-NULL constants to compute; else decline.
    let (Some(c1), Some(c2), Some(c3)) = (c1, c2, c3) else {
        return Ok(None);
    };

    let start = c1.constvalue.as_i64();
    let finish = c2.constvalue.as_i64();

    // Prechecks that would make timestamp_mi raise an ERROR: infinite endpoints
    // or a `finish - start` i64 overflow. Return no estimate (decline) rather
    // than error out in a support function.
    if timestamp_not_finite(start)
        || timestamp_not_finite(finish)
        || finish.checked_sub(start).is_none()
    {
        return Ok(None);
    }
    let ddiff = (finish - start) as f64;

    // INTERVAL_TO_MICROSECONDS(step) over the 16-byte Interval image (time:i64,
    // day:i32, month:i32 — little-endian, the same layout the executor's
    // `arg_interval` reads).
    let image = c3.constvalue.as_ref_bytes();
    if image.len() < 16 {
        return Ok(None);
    }
    let step_time = i64::from_le_bytes(image[0..8].try_into().expect("interval image >= 16 bytes"));
    let step_day = i32::from_le_bytes(image[8..12].try_into().expect("interval image >= 16 bytes"));
    let step_month =
        i32::from_le_bytes(image[12..16].try_into().expect("interval image >= 16 bytes"));
    let dstep = (step_month as f64 * DAYS_PER_MONTH + step_day as f64) * USECS_PER_DAY
        + step_time as f64;

    // This equation works for either sign of step.
    if dstep != 0.0 {
        Ok(Some((ddiff / dstep + 1.0).floor()))
    } else {
        Ok(None)
    }
}
