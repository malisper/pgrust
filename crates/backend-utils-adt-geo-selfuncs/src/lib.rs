//! Selectivity routines for geometric operators
//! (`utils/adt/geo_selfuncs.c`, PostgreSQL 18.3): the bogus constant
//! restriction-/join-selectivity estimators registered in the operator
//! catalog's `oprrest`/`oprjoin` attributes for the geometric operators
//! (overlap, strictly-left/right/above/below, contains/contained-by).
//!
//! As the C file itself notes, "These are totally bogus" — unless the actual
//! key distribution in a GiST index is known, no good selectivity prediction
//! is possible, so each estimator ignores its arguments and returns a fixed
//! constant. The values are deliberately small so the optimizer prefers a
//! geometric index when one is available.
//!
//! The C entry points take `PG_FUNCTION_ARGS` and `PG_RETURN_FLOAT8` a
//! constant. Because they read none of their fmgr arguments, the ported
//! functions take no parameters and return the constant [`Selectivity`]
//! directly; the selfuncs `call_oprrest` / `call_oprjoin` dispatch reaches
//! them by their `oprrest`/`oprjoin` `pg_proc` OID (the established `F_*`
//! pattern). This crate owns no inward seams.

#![allow(clippy::unreadable_literal)]

use types_core::primitive::Selectivity;
use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// Install this crate's contributions. The selectivity estimators are reached
/// through the selfuncs fmgr dispatch by `pg_proc` OID (so there is no seam to
/// install), but they MUST also live in the fmgr builtin REGISTRY so that
/// `fmgr_internal_function(prosrc)` resolves their names — `CREATE OPERATOR`
/// (and `CREATE FUNCTION ... LANGUAGE internal`) validates an operator's
/// `RESTRICT`/`JOIN` (and an internal function's `prosrc`) against that registry
/// via `fmgr_internal_validator`. Without these rows, `CREATE OPERATOR (...,
/// RESTRICT = contsel, JOIN = contjoinsel)` fails with "there is no built-in
/// function named". Register them here (the crate's natural installer).
pub fn init_seams() {
    register_geo_selfuncs_builtins();
}

/// Register the geometric selectivity estimators (C: their `fmgr_builtins[]`
/// rows) into the fmgr-core builtin table. OIDs / nargs / strict / retset are
/// transcribed from `pg_proc.dat` (all `proisstrict => 't'`, none retset). These
/// are dispatched by the planner by OID, but the fmgr-callable native bodies are
/// also wired (they ignore every argument and return the constant, exactly as
/// the C entry points do) so a direct `OidFunctionCall*` resolves identically.
fn register_geo_selfuncs_builtins() {
    fn entry(
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
    backend_utils_fmgr_core::register_builtins_native([
        entry(139, "areasel", 4, fc_areasel),
        entry(140, "areajoinsel", 5, fc_areajoinsel),
        entry(1300, "positionsel", 4, fc_positionsel),
        entry(1301, "positionjoinsel", 5, fc_positionjoinsel),
        entry(1302, "contsel", 4, fc_contsel),
        entry(1303, "contjoinsel", 5, fc_contjoinsel),
    ]);
}

/// `PG_RETURN_FLOAT8(constant)` — the fmgr-1 native body shape for each bogus
/// constant estimator (ignores `fcinfo`, returns the constant as a `float8`
/// Datum).
fn fc_areasel(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(Datum::from_f64(areasel()))
}
fn fc_areajoinsel(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(Datum::from_f64(areajoinsel()))
}
fn fc_positionsel(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(Datum::from_f64(positionsel()))
}
fn fc_positionjoinsel(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(Datum::from_f64(positionjoinsel()))
}
fn fc_contsel(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(Datum::from_f64(contsel()))
}
fn fc_contjoinsel(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(Datum::from_f64(contjoinsel()))
}

/*
 *	Selectivity functions for geometric operators.  These are bogus -- unless
 *	we know the actual key distribution in the index, we can't make a good
 *	prediction of the selectivity of these operators.
 */

/// `areasel` (geo_selfuncs.c) — selectivity for operators that depend on area,
/// such as "overlap". `PG_RETURN_FLOAT8(0.005)`.
pub fn areasel() -> Selectivity {
    0.005
}

/// `areajoinsel` (geo_selfuncs.c) — join selectivity for area-dependent
/// operators. `PG_RETURN_FLOAT8(0.005)`.
pub fn areajoinsel() -> Selectivity {
    0.005
}

/// `positionsel` (geo_selfuncs.c) — how likely is a box to be strictly left of
/// (right of, above, below) a given box? `PG_RETURN_FLOAT8(0.1)`.
pub fn positionsel() -> Selectivity {
    0.1
}

/// `positionjoinsel` (geo_selfuncs.c) — join selectivity for the position
/// operators. `PG_RETURN_FLOAT8(0.1)`.
pub fn positionjoinsel() -> Selectivity {
    0.1
}

/// `contsel` (geo_selfuncs.c) — how likely is a box to contain (be contained
/// by) a given box? A tighter constraint than "overlap", so a smaller estimate
/// than `areasel`. `PG_RETURN_FLOAT8(0.001)`.
pub fn contsel() -> Selectivity {
    0.001
}

/// `contjoinsel` (geo_selfuncs.c) — join selectivity for the containment
/// operators. `PG_RETURN_FLOAT8(0.001)`.
pub fn contjoinsel() -> Selectivity {
    0.001
}
