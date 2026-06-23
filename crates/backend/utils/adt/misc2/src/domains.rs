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

use syscache_seams as syscache_seams;
use typcache_seams as typcache_seams;
use fmgr_seams as fmgr_seams;
use mcx::{Mcx, MemoryContext};
use cache::typcache::{DomainCtxHandle, DomainLevelScan};
use types_core::Oid;
use types_error::{PgError, PgResult};
use nodes::primnodes::Expr;
use types_tuple::heaptuple::Datum;

/// `TYPTYPE_DOMAIN` (`pg_type.h`).
const TYPTYPE_DOMAIN: i8 = b'd' as i8;

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
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<Datum<'mcx>> {
    let domain_type = typioparam;
    let mut escontext = escontext;

    // domain_state_setup(domainType, /*binary=*/false, ...): the typcache half
    // (validates the OID is a domain; looks up the base type's text input fn).
    let io = typcache_seams::domain_get_base_input_info::call(domain_type, false)?;

    // C: if (!InputFunctionCallSafe(&my_extra->proc, string, ..., escontext, &value))
    //        return (Datum) 0;
    // Invoke the base type's typinput procedure to convert the data, threading
    // the soft-error sink so a malformed base-value input (e.g. `pg_input_is_valid
    // ('junk','positiveint')`) records a soft error and bails instead of hard
    // erroring. The seam yields the canonical `Datum<'mcx>` — a by-value scalar
    // as `ByVal`, a by-reference base value (e.g. a domain over text/varchar) as
    // an owned `ByRef` — threaded straight through.
    let value = fmgr_seams::input_function_call::call(
        mcx,
        io.typiofunc,
        string,
        io.typioparam,
        io.typtypmod,
        escontext.as_deref_mut(),
    )?;
    // A soft base-input error: stop here (C returns (Datum) 0 immediately).
    if escontext.as_ref().is_some_and(|c| c.error_occurred()) {
        return Ok(Datum::null());
    }

    // Do the necessary checks to ensure it's a valid domain value.
    typcache_seams::domain_check_input::call(
        &value,
        string.is_none(),
        domain_type,
        escontext.as_deref_mut(),
    )?;

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
    // shape by reporting isnull == false.) C's `domain_recv` passes NULL
    // escontext (the binary-protocol path is always hard-error).
    typcache_seams::domain_check_input::call(&value, false, domain_type, None)?;

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
    typcache_seams::domain_check_input::call(value, isnull, domain_type, None)
}

/// `domain_check_safe(value, isnull, domainType, extra, mcxt, escontext)` — the
/// error-safe variant of [`domain_check`] (domains.c). C is a thin
/// `return domain_check_internal(..., escontext)`; it returns `false` (with the
/// `ErrorSaveContext` populated) when a constraint violation is captured softly,
/// `true` otherwise.
///
/// The `domain_check_input` typcache seam now carries the soft-error sink, so a
/// constraint violation is `errsave`d into `escontext` (when `Some`) and this
/// returns `false`; with `None` (the hard caller) the violation propagates as an
/// `Err`. On success (or a softly-captured violation) the C return is
/// `!SOFT_ERROR_OCCURRED(escontext)`.
pub fn domain_check_safe<'mcx>(
    _mcx: Mcx<'mcx>,
    value: &Datum<'mcx>,
    isnull: bool,
    domain_type: u32,
    escontext: Option<&mut types_error::SoftErrorContext>,
) -> PgResult<bool> {
    let mut escontext = escontext;
    typcache_seams::domain_check_input::call(value, isnull, domain_type, escontext.as_deref_mut())?;
    // C: return !SOFT_ERROR_OCCURRED(escontext);
    Ok(!escontext.is_some_and(|c| c.error_occurred()))
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

/* ==========================================================================
 * Domain constraint planning seam (typcache -> domains.c -> planner).
 *
 * `load_domaintype_info` (typcache.c) plans each domain CHECK constraint's
 * `conbin` node-string with `stringToNode(conbin)` + `expression_planner()`.
 * The typcache owns the orchestration (stack crawl, name sort, parent-first
 * `lcons`); this seam is the single per-constraint plan step, installed here
 * because `domains.c` is the natural home of the domain-constraint machinery
 * and can reach the (value-typed) planner + node reader through their thin
 * seam crates without forming a cycle.
 * ======================================================================== */

std::thread_local! {
    /// Backend-lifetime context backing [`plan_check_expr`]. The planned domain
    /// CHECK `Expr` it produces is cached in the typcache at backend lifetime
    /// and can embed context-allocated `mcx::PgBox`/`PgVec` children, so a
    /// transient context freed on return would dangle them (double-free /
    /// SIGSEGV on later domain-check evaluation or cache drop). This leaked,
    /// never-reset context keeps them valid for the node's lifetime (mirrors
    /// parser-coerce's `COERCE_NODE_CONTEXT` and makefuncs' `CONST_VALUE_CONTEXT`).
    static DOMAIN_CHECK_CONTEXT: &'static MemoryContext =
        alloc::boxed::Box::leak(alloc::boxed::Box::new(MemoryContext::new("Domain constraints")));
}

/// `Mcx<'static>` for the backend-lifetime [`DOMAIN_CHECK_CONTEXT`].
fn domain_check_mcx() -> Mcx<'static> {
    DOMAIN_CHECK_CONTEXT.with(|c| c.mcx())
}

/// `stringToNode(conbin)` + `expression_planner()` for one domain CHECK
/// constraint (the `plan_check_expr` seam). Returns the planned expression as
/// the real owned [`Expr`] value.
///
/// C plans into the domain's "Domain constraints" `MemoryContext` (`ctx`) so the
/// node lives at cache lifetime. The returned [`Expr`] is stored long-term in
/// the typcache `DomainConstraintState` and evaluated on every cast to the
/// domain, so it MUST outlive this call. A planned `Expr` tree can embed
/// context-allocated `mcx::PgBox`/`PgVec` children (e.g. const-folded
/// sub-expressions the `expression_planner` builds into the passed `Mcx`), so a
/// transient `MemoryContext::new(..)` freed on return would leave those
/// children's allocator dangling — a later domain-constraint evaluation or the
/// typcache drop would then double-free through a NULL `Mcx` (SIGSEGV), exactly
/// the parser-coerce `coerce(agg(x))` crash class. We therefore back the read +
/// plan with the leaked, backend-lifetime [`DOMAIN_CHECK_CONTEXT`] (mirrors
/// parser-coerce's `COERCE_NODE_CONTEXT` / makefuncs' `CONST_VALUE_CONTEXT`),
/// which keeps the planned node valid for the cache's lifetime. `ctx` is unused
/// — the durable backend context subsumes the C "plan into ctx" detail.
pub fn plan_check_expr(conbin: &str, _ctx: DomainCtxHandle) -> PgResult<Expr<'static>> {
    // The node reader / const-folder allocate their intermediate graph — and the
    // escaping planned `Expr`'s `PgBox`/`PgVec` children — in this durable
    // backend-lifetime context, so they remain valid after this seam returns.
    let mcx: Mcx<'static> = domain_check_mcx();

    // expr = stringToNode(conbin);
    let node = read_seams::string_to_node::call(mcx, conbin)?;

    // The stored `conbin` of a domain CHECK is always an expression node. Clone
    // it out as an owned `Expr` (deep copy into the global heap, independent of
    // `scratch`).
    let expr: Expr<'static> = node
        .as_expr()
        .ok_or_else(|| {
            PgError::error("domain CHECK constraint conbin did not parse to an expression node")
        })?
        .clone();

    // expr = expression_planner(expr);  (eval_const_expressions + fix_opfuncids)
    let planned = planner_pc_seams::expression_planner_value::call(mcx, expr)?;

    Ok(planned)
}

/// One level of `load_domaintype_info`'s domain-stack crawl (typcache.c:1126):
/// `tup = SearchSysCache1(TYPEOID, typeOid)` (`elog(ERROR, "cache lookup failed
/// for type %u")` when missing) then reading `typtype` / `typnotnull` /
/// `typbasetype` from the `Form_pg_type`. `is_domain` is `typtype ==
/// TYPTYPE_DOMAIN`; when false the typcache stops crawling. Installed here
/// (alongside the other domain-constraint seams) because `domains.c` is the
/// natural home of the domain machinery and reaches the `pg_type` syscache
/// projection through its thin seam crate without a cycle.
pub fn lookup_domain_type_level(type_id: Oid) -> PgResult<DomainLevelScan> {
    // SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid)); the projection seam
    // returns None on !HeapTupleIsValid so we raise the C elog(ERROR).
    let form = syscache_seams::pg_type_form::call(type_id)?.ok_or_else(|| {
        PgError::error(alloc::format!("cache lookup failed for type {type_id}"))
    })?;

    Ok(DomainLevelScan {
        is_domain: form.typtype == TYPTYPE_DOMAIN,
        typnotnull: form.typnotnull,
        typbasetype: form.typbasetype,
    })
}
