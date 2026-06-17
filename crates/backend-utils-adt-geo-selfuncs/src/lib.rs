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

/// This crate owns no inward seams (its entry points are reached through the
/// selfuncs fmgr dispatch by `pg_proc` OID), so there is nothing to install.
/// Mirrors `backend-utils-adt-array-selfuncs::init_seams`.
pub fn init_seams() {}

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
