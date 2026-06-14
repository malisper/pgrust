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
//!
//! ## Decomposition note
//!
//! The C cache struct `DomainIOData` is an optimization that memoizes the
//! per-domain setup across a `FmgrInfo`'s lifetime (`fcinfo->flinfo->fn_extra`).
//! The owned model has no `FmgrInfo` handle to hang it off (see fmgr-seams:
//! "the owned model re-resolves at call time"), so — exactly like
//! `fmgr_info_check` — these entrypoints re-run the setup each call. The result
//! is semantically identical; only the catalog-lookup memoization is dropped.
//!
//! The genuinely-unported owners reached through seams are:
//!
//! * **typcache** (`backend-utils-cache-typcache`, not yet ported): owns the
//!   `TYPECACHE_DOMAIN_BASE_INFO` lookup and the `DomainConstraintRef`
//!   constraint cache + `ExecCheck` evaluation. `domain_state_setup`'s typcache
//!   half is [`typcache_seams::domain_get_base_input_info`]; the whole
//!   `domain_check_input` engine is [`typcache_seams::domain_check_input`].
//! * **fmgr** (`backend-utils-fmgr-fmgr`): the base type's text/binary I/O
//!   functions, dispatched by OID through
//!   [`fmgr_seams::input_function_call`] / [`fmgr_seams::receive_function_call`]
//!   (C's `FmgrInfo` cannot cross a seam).
//! * **syscache / lsyscache**: the `errdatatype` diagnostic lookups
//!   (`SearchSysCache1(TYPEOID)` + `get_namespace_name`).

use backend_utils_cache_typcache_seams as typcache_seams;
use backend_utils_fmgr_fmgr_seams as fmgr_seams;
use mcx::Mcx;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;

/// `domain_in(string, typioparam, typmod)` — FmgrInfo entrypoint.
///
/// Faithful port of `domain_in` (domains.c). The C function is not strict, so
/// it tolerates a NULL string (treated as a null domain value). The
/// `typioparam`/`typmod` arguments here name the *domain* type OID (C's
/// `PG_GETARG_OID(1)`); `typmod` is the unused third I/O argument, accepted to
/// mirror the I/O-function ABI.
pub fn domain_in<'mcx>(
    mcx: Mcx<'mcx>,
    string: Option<&str>,
    typioparam: u32,
    _typmod: i32,
) -> PgResult<Datum<'mcx>> {
    let domain_type = typioparam;

    // domain_state_setup(domainType, /*binary=*/false, ...): the typcache half
    // (validates the OID is a domain; looks up the base type's text input fn).
    let io = typcache_seams::domain_get_base_input_info::call(domain_type, false)?;

    // Invoke the base type's typinput procedure to convert the data. With no
    // escontext (hard-error caller), InputFunctionCallSafe is equivalent to
    // InputFunctionCall. The seam yields the bare scalar word; wrap it in the
    // canonical by-value arm.
    let value = Datum::ByVal(
        fmgr_seams::input_function_call::call(
            mcx,
            io.typiofunc,
            string,
            io.typioparam,
            io.typtypmod,
        )?
        .as_usize(),
    );

    // Do the necessary checks to ensure it's a valid domain value.
    typcache_seams::domain_check_input::call(&value, string.is_none(), domain_type)?;

    Ok(value)
}

/// `domain_recv(buf, typioparam, typmod)` — binary-recv entrypoint.
///
/// Faithful port of `domain_recv` (domains.c). Like `domain_in` it is not
/// strict; the `buf` argument carries the `StringInfo` payload. `typioparam`
/// names the domain type OID.
pub fn domain_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    typioparam: u32,
    _typmod: i32,
) -> PgResult<Datum<'mcx>> {
    let domain_type = typioparam;

    // domain_state_setup(domainType, /*binary=*/true, ...).
    let io = typcache_seams::domain_get_base_input_info::call(domain_type, true)?;

    // Invoke the base type's typreceive procedure to convert the data. The seam
    // yields the bare scalar word; wrap it in the canonical by-value arm.
    let value = Datum::ByVal(
        fmgr_seams::receive_function_call::call(
            mcx,
            io.typiofunc,
            buf,
            io.typioparam,
            io.typtypmod,
        )?
        .as_usize(),
    );

    // Do the necessary checks to ensure it's a valid domain value. (binary
    // input always supplies a non-null value, matching the C `buf == NULL`
    // being unreachable for the normal system path; we mirror the not-strict
    // shape by reporting isnull == false.)
    typcache_seams::domain_check_input::call(&value, false, domain_type)?;

    Ok(value)
}

/// `domain_check(value, isnull, domainType, extra, mcxt)` — validate a value
/// against the given domain type's constraints.
///
/// Faithful port of `domain_check` (domains.c), which is a thin
/// `(void) domain_check_internal(..., escontext = NULL)`. The owned model has
/// no `extra` memoization handle (see the module note), so each call re-runs
/// the typcache-resident `domain_check_input` engine.
pub fn domain_check<'mcx>(
    _mcx: Mcx<'mcx>,
    value: &Datum<'mcx>,
    isnull: bool,
    domain_type: u32,
) -> PgResult<()> {
    typcache_seams::domain_check_input::call(value, isnull, domain_type)
}

/// `errdatatype(datatypeOid)` — errcontext helper naming the domain type.
///
/// In C, `errdatatype` augments the *current* `ErrorData` in flight: it looks
/// the type up (`SearchSysCache1(TYPEOID)`) and calls `err_generic_string` to
/// attach `PG_DIAG_SCHEMA_NAME` / `PG_DIAG_DATATYPE_NAME`. It is only ever
/// invoked from inside the `ereport`/`errsave` argument list of the NOT NULL /
/// CHECK violations raised by `domain_check_input`.
///
/// In the decomposed owned model those violations are reported by the typcache
/// `domain_check_input` engine (which owns the in-flight `PgError` and the
/// catalog access + `Mcx` needed for the lookups), so the engine attaches these
/// diagnostic fields itself. This standalone helper carries no `Mcx` and has no
/// errordata to mutate under its `-> PgResult<()>` contract, so it is a no-op:
/// the work it names lives with the error it annotates. It is retained as a
/// public symbol for the family's fixed surface.
pub fn errdatatype(_datatype_oid: u32) -> PgResult<()> {
    Ok(())
}

/// `errdomainconstraint(datatypeOid, conname)` — errcontext helper naming the
/// violated domain constraint.
///
/// `errdatatype(datatypeOid)` then `err_generic_string(PG_DIAG_CONSTRAINT_NAME,
/// conname)`. See [`errdatatype`]: in the owned model the CHECK-violation error
/// is built (with the schema/datatype/constraint fields) inside the typcache
/// `domain_check_input` engine, so this standalone helper is a no-op retained
/// for the family's surface.
pub fn errdomainconstraint(_datatype_oid: u32, _conname: &str) -> PgResult<()> {
    errdatatype(_datatype_oid)
}
