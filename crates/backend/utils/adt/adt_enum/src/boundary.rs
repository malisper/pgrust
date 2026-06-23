//! Typed fmgr boundary for `enum.c`. `enum_in`/`out`/`recv`/`send` are the
//! `*in`/`*out`/`*recv`/`*send` entry points; the comparison operators and the
//! `enum_first`/`enum_last`/`enum_range` helpers are the operator/function
//! entry points. Their bodies are the cores in [`crate`]; these wrappers
//! re-shape them at the "Option 4" boundary ([`::fmgr::boundary`]):
//!
//!   * an enum value is a 4-byte pass-by-value type, so its `Datum` word is the
//!     value's OID (`PG_GETARG_OID` / `PG_RETURN_OID`);
//!   * a `cstring` input arrives as `&str`, a `cstring` output leaves as `String`;
//!   * `enum_send` returns the `bytea` varlena body, `enum_range` the array
//!     varlena image, both as [`FmgrOut::Ref`] [`RefPayload::Varlena`].
//!
//! `transaction_xmin` is C's `TransactionXmin` global, threaded explicitly per
//! the no-ambient-global seam rule (the caller reads it off its snapshot).
//!
//! The bare-word PGFunction registry (`fmgr_builtins[]` rows) is deferred.

use mcx::{Mcx, PgVec};
use ::types_core::primitive::{InvalidOid, Oid};
use ::types_core::TransactionId;
use ::types_tuple::Datum;
use types_error::{PgResult, SoftErrorContext};
use ::fmgr::boundary::{FmgrArg, FmgrOut, RefPayload};
use ::stringinfo::StringInfo;

/// `PG_GETARG_OID(n)`: read a by-value OID argument (an enum value's `Datum`).
fn arg_oid(arg: FmgrArg) -> Oid {
    match arg {
        FmgrArg::ByVal(d) => d.as_oid(),
        FmgrArg::Ref(_) => InvalidOid,
    }
}

/// `enum_in` at the boundary: `cstring` → `FmgrOut::ByVal(oid)` (or `Ok(None)`
/// on the soft-error path).
pub fn enum_in<'mcx>(
    name: &str,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<FmgrOut<'mcx>>> {
    match crate::enum_in(name, enumtypoid, transaction_xmin, escontext)? {
        Some(oid) => Ok(Some(FmgrOut::ByVal(Datum::from_oid(oid)))),
        None => Ok(None),
    }
}

/// `enum_out` at the boundary: `FmgrArg::ByVal(oid)` → `cstring` (`String`).
pub fn enum_out(value: FmgrArg) -> PgResult<String> {
    crate::enum_out(arg_oid(value))
}

/// `enum_recv` at the boundary: a receive buffer → `FmgrOut::ByVal(oid)`.
pub fn enum_recv<'mcx>(
    buf: &mut StringInfo<'_>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<FmgrOut<'mcx>> {
    let oid = crate::enum_recv(buf, enumtypoid, transaction_xmin)?;
    Ok(FmgrOut::ByVal(Datum::from_oid(oid)))
}

/// `enum_send` at the boundary: `FmgrArg::ByVal(oid)` → `bytea` varlena body.
pub fn enum_send<'mcx>(mcx: Mcx<'mcx>, value: FmgrArg) -> PgResult<PgVec<'mcx, u8>> {
    crate::enum_send(mcx, arg_oid(value))
}

macro_rules! cmp_boundary {
    ($name:ident, $core:ident) => {
        pub fn $name<'mcx>(a: FmgrArg, b: FmgrArg) -> PgResult<FmgrOut<'mcx>> {
            Ok(FmgrOut::ByVal(Datum::from_bool(crate::$core(
                arg_oid(a),
                arg_oid(b),
            )?)))
        }
    };
}
cmp_boundary!(enum_lt, enum_lt);
cmp_boundary!(enum_le, enum_le);
cmp_boundary!(enum_ge, enum_ge);
cmp_boundary!(enum_gt, enum_gt);

/// `enum_eq` at the boundary (OID equality, no catalog access).
pub fn enum_eq<'mcx>(a: FmgrArg, b: FmgrArg) -> FmgrOut<'mcx> {
    FmgrOut::ByVal(Datum::from_bool(crate::enum_eq(arg_oid(a), arg_oid(b))))
}

/// `enum_ne` at the boundary (OID inequality, no catalog access).
pub fn enum_ne<'mcx>(a: FmgrArg, b: FmgrArg) -> FmgrOut<'mcx> {
    FmgrOut::ByVal(Datum::from_bool(crate::enum_ne(arg_oid(a), arg_oid(b))))
}

/// `enum_smaller` at the boundary → `FmgrOut::ByVal(oid)`.
pub fn enum_smaller<'mcx>(a: FmgrArg, b: FmgrArg) -> PgResult<FmgrOut<'mcx>> {
    Ok(FmgrOut::ByVal(Datum::from_oid(crate::enum_smaller(
        arg_oid(a),
        arg_oid(b),
    )?)))
}

/// `enum_larger` at the boundary → `FmgrOut::ByVal(oid)`.
pub fn enum_larger<'mcx>(a: FmgrArg, b: FmgrArg) -> PgResult<FmgrOut<'mcx>> {
    Ok(FmgrOut::ByVal(Datum::from_oid(crate::enum_larger(
        arg_oid(a),
        arg_oid(b),
    )?)))
}

/// `enum_cmp` at the boundary → `FmgrOut::ByVal(int4)`.
pub fn enum_cmp<'mcx>(a: FmgrArg, b: FmgrArg) -> PgResult<FmgrOut<'mcx>> {
    Ok(FmgrOut::ByVal(Datum::from_i32(crate::enum_cmp(
        arg_oid(a),
        arg_oid(b),
    )?)))
}

/// `enum_first` at the boundary. `enumtypoid` is C's
/// `get_fn_expr_argtype(fcinfo->flinfo, 0)`.
pub fn enum_first<'mcx>(
    mcx: Mcx<'mcx>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<FmgrOut<'mcx>> {
    Ok(FmgrOut::ByVal(Datum::from_oid(crate::enum_first(
        mcx,
        enumtypoid,
        transaction_xmin,
    )?)))
}

/// `enum_last` at the boundary.
pub fn enum_last<'mcx>(
    mcx: Mcx<'mcx>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<FmgrOut<'mcx>> {
    Ok(FmgrOut::ByVal(Datum::from_oid(crate::enum_last(
        mcx,
        enumtypoid,
        transaction_xmin,
    )?)))
}

/// `enum_range` (2-argument) at the boundary → the array varlena image.
pub fn enum_range_bounds<'mcx>(
    mcx: Mcx<'mcx>,
    lower: Option<Oid>,
    upper: Option<Oid>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<FmgrOut<'mcx>> {
    let image = crate::enum_range_bounds(mcx, lower, upper, enumtypoid, transaction_xmin)?;
    Ok(FmgrOut::Ref(RefPayload::Varlena(image.to_vec())))
}

/// `enum_range` (1-argument) at the boundary: every member of the type.
pub fn enum_range_all<'mcx>(
    mcx: Mcx<'mcx>,
    enumtypoid: Oid,
    transaction_xmin: TransactionId,
) -> PgResult<FmgrOut<'mcx>> {
    let image = crate::enum_range_all(mcx, enumtypoid, transaction_xmin)?;
    Ok(FmgrOut::Ref(RefPayload::Varlena(image.to_vec())))
}
