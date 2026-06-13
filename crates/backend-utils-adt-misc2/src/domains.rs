//! Family `domains` — `src/backend/utils/adt/domains.c`.
//!
//! Domain type support: `domain_in` / `domain_recv` (the I/O functions for
//! domain types), plus `domain_check` and the `domain_check_internal` engine
//! that validates a value against a domain's NOT NULL and CHECK constraints,
//! caching the compiled constraint expressions in a `DomainIOData` /
//! `DomainConstraintRef`. Consumed by the `expandedrecord` family
//! (check_domain_for_new_field / _new_tuple) and by external callers.
//!
//! These build cached state and evaluate expressions, so they take `Mcx` and
//! surface constraint-violation `ereport(ERROR)`s as `PgResult`. The value
//! crosses as a `Datum`.

use mcx::Mcx;
use types_datum::Datum;
use types_error::PgResult;

/// `domain_in(string, typioparam, typmod)` — FmgrInfo entrypoint.
pub fn domain_in<'mcx>(
    _mcx: Mcx<'mcx>,
    _string: Option<&str>,
    _typioparam: u32,
    _typmod: i32,
) -> PgResult<Datum> {
    todo!("domain_in")
}

/// `domain_recv(buf, typioparam, typmod)` — binary-recv entrypoint.
pub fn domain_recv<'mcx>(
    _mcx: Mcx<'mcx>,
    _buf: &[u8],
    _typioparam: u32,
    _typmod: i32,
) -> PgResult<Datum> {
    todo!("domain_recv")
}

/// `domain_check(value, isnull, domainType, extra, mcxt)` — validate a value
/// against the given domain type's constraints.
pub fn domain_check<'mcx>(
    _mcx: Mcx<'mcx>,
    _value: Datum,
    _isnull: bool,
    _domain_type: u32,
) -> PgResult<()> {
    todo!("domain_check")
}

/// `errdatatype(datatypeOid)` — errcontext helper naming the domain type.
pub fn errdatatype(_datatype_oid: u32) -> PgResult<()> {
    todo!("errdatatype")
}

/// `errdomainconstraint(datatypeOid, conname)` — errcontext helper naming the
/// violated domain constraint.
pub fn errdomainconstraint(_datatype_oid: u32, _conname: &str) -> PgResult<()> {
    todo!("errdomainconstraint")
}
