//! Domain CHECK-constraint expression machinery — the `EState`-less
//! `ExecInitExpr(expr, NULL)` compile and the standalone-`ExprContext`
//! `ExecCheck` evaluation that the typcache's domain-constraint refs
//! (`InitDomainConstraintRef` / `domain_check_input`, utils/cache/typcache.c +
//! utils/adt/domains.c) drive through the
//! `backend-utils-adt-domains-seams` seams.
//!
//! # Why this is a distinct compile path
//!
//! Every other `ExecInitExpr` entry point (execExpr_core) compiles into an
//! `EStateData<'mcx>`'s per-query arena (`estate.es_query_cxt`) and frees the
//! program when the EState's context is reset. A `DomainConstraintRef`'s
//! `refctx`, however, is a plain "Domain constraints" memory context with **no
//! associated EState** (the C `ExecInitExpr(check_expr, NULL)` in
//! `prep_domain_constraints`): the planned CHECK expression is compiled once
//! and cached at the constraint's lifetime, then evaluated against many
//! candidate values via a standalone `ExprContext` (`domain_check_input`).
//!
//! The owned-model bridge for the C `DomainConstraintState.check_exprstate`
//! (`ExprState *`) is a u64 [`ExprStateHandle`] keyed into a backend-local
//! registry [`DOMAIN_EXPRSTATES`]. Each registry entry is an [`McxOwned`]
//! bundle (the same self-owning-context idiom the regexp cache uses) owning a
//! private child `MemoryContext` that holds the compiled `ExprState<'mcx>` plus
//! a throwaway `EStateData<'mcx>` carrying the standalone `ExprContext` the
//! eval needs (the interpreter eval seam ties `ExprState`, `EcxtId`, and
//! `EStateData` to one `'mcx`). The bundle — and thus the C `ExprState`'s
//! storage — is reclaimed when the owning "Domain constraints" context is
//! deleted, which the typcache's `delete_domain_ctx` seam signals here via
//! [`free_ctx_exprstates`] (the faithful equivalent of C's
//! `MemoryContextDelete(refctx)` freeing every `ExprState` palloc'd into it).

use core::cell::{Cell, RefCell};
use std::collections::HashMap;

use mcx::{McxOwned, MemoryContext};
use cache::typcache::{DomainCtxHandle, ExprStateHandle};
use types_error::PgResult;
use nodes::execexpr::ExprState;
use nodes::primnodes::Expr;
use nodes::{EStateData, EcxtId};
use types_tuple::heaptuple::Datum;

use crate::execExpr_core;

/// `int16` sentinel for a varlena type's `pg_type.typlen` (`-1`). Only this
/// length reaches the `MakeExpandedObjectReadOnly` transform (the macro's
/// `typlen == -1` arm); by-value and fixed-length by-ref base types pass the
/// candidate datum through unchanged.
const VARLENA_TYPLEN: i16 = -1;

/// The self-owning bundle a registry entry holds: the compiled CHECK
/// `ExprState`, a throwaway `EStateData` that supplies the standalone
/// `ExprContext` the interpreter needs, and the `EcxtId` of that context in the
/// EState's pool. All three share the bundle's private `MemoryContext` arena
/// (`'mcx`), so the interpreter eval seam — which unifies `ExprState<'mcx>`,
/// `EcxtId`, and `EStateData<'mcx>` to one lifetime — typechecks.
struct DomainExprBundle<'mcx> {
    estate: EStateData<'mcx>,
    state: ExprState<'mcx>,
    econtext: EcxtId,
}

mcx::bind!(DomainExprBundleTy => DomainExprBundle<'mcx>);

thread_local! {
    /// Backend-local registry of compiled domain-CHECK `ExprState`s, keyed by the
    /// u64 [`ExprStateHandle`] the typcache stores in
    /// `DomainConstraintState.check_exprstate`. Each entry is reclaimed when its
    /// owning "Domain constraints" context is deleted (typcache
    /// `delete_domain_ctx` → [`free_ctx_exprstates`]).
    static DOMAIN_EXPRSTATES: RefCell<HashMap<u64, McxOwned<DomainExprBundleTy>>> =
        RefCell::new(HashMap::new());
    /// Maps a compiled handle back to the `DomainCtxHandle` it was compiled into,
    /// so `free_ctx_exprstates(ctx)` can evict exactly the entries that context
    /// owns (the C `ExprState`s palloc'd in `refctx`).
    static HANDLE_CTX: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
    /// Monotonic handle counter (never 0, so `0` stays the [`ExprStateHandle::NULL`]
    /// sentinel).
    static DOMAIN_EXPRSTATE_NEXT: Cell<u64> = const { Cell::new(1) };
}

/// `ExecInitExpr(check_expr, NULL)` in `execctx` (the typcache's
/// `prep_domain_constraints` per-CHECK compile) — the `EState`-less variant.
///
/// Compiles `check_expr` (a `CoerceToDomainValue`-bearing CHECK predicate
/// already const-folded by `expression_planner` in `plan_check_expr`) into an
/// [`ExprState`] allocated in a fresh private child of the caller's "Domain
/// constraints" context, registers the bundle under a fresh non-zero handle,
/// and returns that handle. The compiled program is identical to
/// [`execExpr_core::exec_init_expr_no_parent`] — same `ExecCreateExprSetupSteps`
/// + `ExecInitExprRec` + `EEOP_DONE_RETURN` + `ExecReadyExpr` spine — but with
/// no `es_link` stamped (a domain CHECK references no Vars, Params, or
/// SubPlans, only the domain test value supplied by the econtext at eval time).
pub fn exec_init_expr(check_expr: &Expr, execctx: DomainCtxHandle) -> PgResult<ExprStateHandle> {
    // The C `ExprState` is palloc'd in `refctx`; the owned-model analogue is a
    // private child context of that "Domain constraints" context (so it counts
    // toward the same accounting subtree and is reclaimed together). The bundle
    // owns this context and bounds the `ExprState`/`EState`'s `'mcx`.
    //
    // We clone `check_expr` into the bundle's arena so the cached planned Expr's
    // own lifetime does not have to outlive the compile (C's
    // `ExecInitExpr` reads the node tree and copies what it needs into the
    // program's context).
    let bundle = McxOwned::<DomainExprBundleTy>::try_new(
        MemoryContext::new("Domain CHECK ExprState"),
        |mcx| {
            let owned_expr = check_expr.clone_in(mcx)?;

            // ExecInitExpr(node, NULL): compile the program. This is the
            // EState-less spine — same as exec_init_expr_no_parent but the
            // arena is the bundle's private context, not estate.es_query_cxt,
            // and no EState back-link is stamped.
            let state = execExpr_core::compile_standalone_expr(mcx, &owned_expr)?;

            // Build the throwaway EState whose per-query context IS this same
            // arena, then create a standalone ExprContext for the evaluations
            // (CreateStandaloneExprContext) and register it so the interpreter
            // eval seam can address it by EcxtId.
            let mut estate = EStateData::new_in(mcx);
            let econtext =
                execUtils_seams::create_standalone_expr_context::call(mcx)?;
            let econtext = estate.add_expr_context(econtext)?;

            Ok(DomainExprBundle { estate, state, econtext })
        },
    )?;

    let handle = DOMAIN_EXPRSTATE_NEXT.with(|n| {
        let h = n.get();
        n.set(h + 1);
        h
    });
    DOMAIN_EXPRSTATES.with(|m| m.borrow_mut().insert(handle, bundle));
    HANDLE_CTX.with(|m| m.borrow_mut().insert(handle, execctx.0));
    Ok(ExprStateHandle(handle))
}

/// `domain_check_input`'s per-CHECK evaluation (utils/adt/domains.c:163-203):
///
/// ```c
/// econtext->domainValue_datum =
///     MakeExpandedObjectReadOnly(value, isnull,
///                                con->check_exprstate ... tcache->typlen);
/// econtext->domainValue_isNull = isnull;
/// conResult = ExecEvalExprSwitchContext(con->check_exprstate, econtext, &isNull);
/// if (!isNull && !DatumGetBool(conResult)) ereport(...);
/// ```
///
/// Resolves the handle to its bundle, sets the standalone `ExprContext`'s
/// `domainValue_datum` / `domainValue_isNull` (the value read by the CHECK's
/// `CoerceToDomainValue` → `EEOP_DOMAIN_TESTVAL` step), runs the compiled
/// `ExprState` via [`execExpr_core::exec_check`] (a NULL conjunction result is
/// TRUE, exactly as `ExecCheck`), then resets the context's per-tuple memory.
/// Returns the CHECK result; the typcache raises `ERRCODE_CHECK_VIOLATION` on
/// `false`.
pub fn domain_check_exec(
    exprstate: ExprStateHandle,
    value: &Datum<'_>,
    isnull: bool,
    typlen: i16,
) -> PgResult<bool> {
    if exprstate == ExprStateHandle::NULL {
        // A NOT NULL constraint (no compiled CHECK) never reaches here — but if
        // a caller passes the NULL handle, the C macro short-circuits the
        // ExecInitExpr to NULL and ExecCheck(NULL, ...) is TRUE.
        return Ok(true);
    }
    DOMAIN_EXPRSTATES.with(|m| {
        let mut map = m.borrow_mut();
        let bundle = match map.get_mut(&exprstate.0) {
            Some(b) => b,
            // The typcache only ever passes a handle it obtained from
            // `exec_init_expr`; a miss is an invariant violation. Raise a clean
            // error rather than panicking on the unwind path.
            None => {
                return Err(types_error::PgError::error(format!(
                    "domain_check_exec: unknown ExprStateHandle {}",
                    exprstate.0
                )))
            }
        };
        bundle.with_mut(|b| {
            // MakeExpandedObjectReadOnly(value, isnull, typlen): the macro is
            //   isnull           -> value (unchanged)
            //   typlen == -1     -> MakeExpandedObjectReadOnlyInternal(value)
            //   otherwise        -> value (unchanged)
            // Force a possibly-multiply-read varlena domain value to read-only;
            // the misc2 seam is the identity on any non-R/W-expanded datum.
            //
            // The clone lands in the bundle's own arena (its EState's per-query
            // context). That arena is short-lived: the typcache's
            // `domain_check_input` creates a fresh "Domain constraints" context,
            // compiles the CHECK exprstates into it, evaluates every constraint
            // of ONE candidate value once, then `delete_domain_ctx` drops the
            // bundle — so the per-call domainValue copy is reclaimed immediately,
            // not accumulated across rows (matching C, where the transient input
            // value lives in the caller's per-call context).
            let mcx = b.estate.es_query_cxt;
            let dval = if !isnull && typlen == VARLENA_TYPLEN {
                misc2_seams::make_expanded_object_read_only_internal_v::call(
                    mcx, value,
                )?
            } else {
                value.clone_in(mcx)?
            };

            // econtext->domainValue_datum = ...; domainValue_isNull = isnull;
            {
                let ec = b.estate.ecxt_mut(b.econtext);
                ec.domainValue_datum = dval;
                ec.domainValue_isNull = isnull;
            }

            // conResult = ExecCheck(con->check_exprstate, econtext);
            let ok = execExpr_core::exec_check(Some(&mut b.state), b.econtext, &mut b.estate)?;

            // Reset the per-tuple memory the eval used (ResetExprContext), so a
            // pass-by-reference CHECK result does not accumulate across calls.
            execUtils_seams::reset_expr_context::call(&mut b.estate, b.econtext)?;

            Ok(ok)
        })
    })
}

/// Free every compiled domain-CHECK `ExprState` that was compiled into the
/// given "Domain constraints" context — the owned-model equivalent of C's
/// `MemoryContextDelete(refctx)` reclaiming the `ExprState`s palloc'd in it.
/// Called from the typcache's `delete_domain_ctx` seam.
pub fn free_ctx_exprstates(ctx: DomainCtxHandle) -> PgResult<()> {
    let handles: Vec<u64> = HANDLE_CTX.with(|m| {
        let map = m.borrow();
        map.iter()
            .filter_map(|(h, c)| if *c == ctx.0 { Some(*h) } else { None })
            .collect()
    });
    for h in handles {
        HANDLE_CTX.with(|m| m.borrow_mut().remove(&h));
        // Dropping the McxOwned bundle drops its private context, reclaiming the
        // ExprState/EState arena (and firing any context reset callbacks LIFO).
        DOMAIN_EXPRSTATES.with(|m| m.borrow_mut().remove(&h));
    }
    Ok(())
}
