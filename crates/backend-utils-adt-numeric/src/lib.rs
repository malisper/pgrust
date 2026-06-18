//! Port of PostgreSQL's `numeric` data type
//! (`postgres-18.3/src/backend/utils/adt/numeric.c`, ~12.6k LOC).
//!
//! This is a DECOMPOSED port: numeric.c is too large for one pass, so it is
//! split into a keystone carrier crate ([`types_numeric`]) plus the family
//! modules declared below. The scaffold lands the keystone + module skeletons;
//! each family is then filled with 100%-faithful logic in its own pass.
//!
//! # Lifetime keystone
//!
//! The in-memory working type is [`types_numeric::var::NumericVar`]`<'mcx>`,
//! whose digit buffer is a *charged* `mcx::PgVec<'mcx, NumericDigit>`. The
//! `'mcx` lifetime (the memory context that owns the digits) threads through
//! every family. There is no ambient `CurrentMemoryContext` in this repo, so
//! every allocating core takes an explicit `mcx::Mcx<'mcx>`.
//!
//! # Errors
//!
//! Hard errors mirror the C `ereport(ERROR, ...)` sites as
//! [`types_error::PgError`] via [`types_error::PgResult`], with the same
//! SQLSTATEs.
//!
//! # Family decomposition
//!
//! * [`kernel_var`]          -- `NumericVar` lifecycle/constants + cmp/addsub/
//!   round/mul/div (the base-NBASE arithmetic kernel);
//! * [`kernel_transcendental`] -- sqrt/exp/ln/log/power + int<->var helpers;
//! * [`convert`]             -- NumericVar <-> on-disk image / int / float /
//!   `NumericData` (struct codec);
//! * [`io`]                  -- text in/out + binary recv/send + serialize;
//! * [`ops_sql`]             -- SQL operator/function cores + special funcs +
//!   scale/typmod/series helpers;
//! * [`aggregate`]           -- Youngs-Cramer accumulators, sum/avg/variance/
//!   stddev/regr, sort-support + hash.

#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

pub mod aggregate;
pub mod convert;
pub mod fmgr_builtins;
pub mod io;
pub mod kernel_transcendental;
pub mod kernel_var;
pub mod ops_sql;
pub mod random;
pub mod series_srf;

use mcx::{Mcx, PgVec};
use types_error::PgError;
use types_numeric::NumericDigit;

/// Re-export the on-disk ABI surface under a single path.
pub mod on_disk {
    //! The on-disk storage vocabulary and its safe byte-view accessors.
    pub use types_numeric::*;
}

/// Allocate a zeroed, **charged** `PgVec<'mcx, NumericDigit>` of length `n`,
/// OOM-safely (the project HARD RULE: validated bound + fallible reserve).
///
/// numeric.c sizes its digit buffers from the operands' digit counts and
/// scales, all bounded by `NUMERIC_MAX_*` (the C code itself `ereport`s well
/// before any pathological size). We mirror that: reserve exactly `n` via the
/// charged fallible path and surface OOM as a `numeric value out of range`
/// error rather than aborting — the same SQLSTATE C raises for an over-large
/// result.
#[inline]
pub(crate) fn alloc_digits<'mcx>(
    mcx: Mcx<'mcx>,
    n: usize,
) -> types_error::PgResult<PgVec<'mcx, NumericDigit>> {
    use types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
    let mut v = mcx::vec_with_capacity_in::<NumericDigit>(mcx, n).map_err(|_| {
        PgError::error("value overflows numeric format")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
    })?;
    // Capacity already reserved -> no realloc -> infallible.
    v.resize(n, 0);
    Ok(v)
}

/// Install the seams this unit OWNS (declared in
/// `backend-utils-adt-numeric-seams`). Called from `seams-init::init_all()`.
///
/// These are the genuine cross-crate contracts the numeric unit implements:
/// the on-disk-byte value comparison/equality reached from `jsonb_util`, the
/// `numeric_maximum_size` typmod helper, and the `numrange` subtype distance
/// reached from `rangetypes`.
pub fn init_seams() {
    use backend_utils_adt_numeric_seams as seams;
    seams::numeric_eq::set(io::seam_numeric_eq);
    seams::numeric_cmp::set(io::seam_numeric_cmp);
    seams::numeric_maximum_size::set(ops_sql::seam_numeric_maximum_size);
    seams::numeric_subdiff::set(ops_sql::seam_numeric_subdiff);
    seams::numeric_subdiff_bytes::set(ops_sql::seam_numeric_subdiff_bytes);

    // numeric->integer casts (numeric.c numeric_int2/4/8): slots declared in
    // jsonb-seams (jsonb.c is the consumer), owned and installed here.
    backend_utils_adt_jsonb_seams::numeric_int2::set(ops_sql::seam_numeric_int2);
    backend_utils_adt_jsonb_seams::numeric_int4::set(ops_sql::seam_numeric_int4);
    backend_utils_adt_jsonb_seams::numeric_int8::set(ops_sql::seam_numeric_int8);

    // make_const's T_Float (numeric) arm: DirectFunctionCall3(numeric_in, str,
    // InvalidOid, -1). Slot declared in backend-parser-small1-seams (parse_node.c
    // is the consumer), owned and installed here.
    backend_parser_small1_seams::numeric_in::set(|mcx, s| io::numeric_in(mcx, s, -1));

    // Register this unit's SQL-callable functions into the fmgr-core builtin
    // table (C: fmgr_builtins[]), so by-OID dispatch resolves them.
    fmgr_builtins::register_numeric_builtins();
}
