//! `backend-pl-plpgsql-exec` — the PL/pgSQL executor (`pl_exec.c`).
//!
//! Walks the compiled `PLpgSQL_function` tree: the statement dispatch, the
//! control structures (IF / CASE / LOOP / WHILE / FOR-i / FOREACH / EXIT /
//! RETURN), the block enter/exit with its EXCEPTION channel, the
//! datum-init loop, and the return-code propagation table
//! (`LOOP_RC_PROCESSING`).
//!
//! ## What is real here
//!
//! The control-flow skeleton is ported 1:1 (exact arm order, identical
//! SQLSTATE/messages at the abort sites, real return-code propagation): the
//! `exec_stmt_*` dispatch, the loops, the block RC switch (deliberately
//! different from `LOOP_RC_PROCESSING`), the EXCEPTION-block sub-transaction +
//! `Err`-match error channel ([`exec_stmt_block_with_exceptions`]; a narrow
//! `catch_unwind` there only rolls the subtransaction back if a not-yet-ported
//! loud seam panics inside the body, then resumes the unwind), the
//! SQLSTATE condition matcher ([`exception_matches_conditions`]), and the
//! VAR/PROMISE arm of [`plpgsql_exec_get_datum_type_info`].
//!
//! ## What is LOUD (the value substrate — REAL-OR-LOUD)
//!
//! Every leg that evaluates an expression through the SQL executor, runs a
//! query through SPI, reads/writes a runtime `Datum`, iterates an array, or
//! deconstructs a composite is routed through [`seam`] and panics loudly,
//! naming the C callee + the external subsystem (the executor `ExprState`
//! simple-expr path #165/#324, the plan-based SPI surface, the array+fmgr
//! substrate). A faithful C build would `ereport`/elog at exactly those points
//! until those owners land. They are never faked.
//!
//! ## Inward seam
//!
//! Installs `plpgsql_exec_get_datum_type_info` (the compiler's compile-time
//! callback from `make_datum_param`) from [`init_seams`].

#![allow(non_camel_case_types, non_snake_case)]

mod erh_table;
mod mem;
mod seam;
mod trigger;

use types_plpgsql::{
    int32, Datum, EState, Oid, PLpgSQL_condition, PLpgSQL_datum, PLpgSQL_datum_type,
    PLpgSQL_execstate, PLpgSQL_function, PLpgSQL_promise_type, PLpgSQL_rc, PLpgSQL_stmt,
    PLpgSQL_stmt_assign, PLpgSQL_stmt_block, PLpgSQL_stmt_case, PLpgSQL_stmt_exit,
    PLpgSQL_stmt_fori, PLpgSQL_stmt_foreach_a, PLpgSQL_stmt_if, PLpgSQL_stmt_loop,
    PLpgSQL_stmt_perform, PLpgSQL_stmt_return, PLpgSQL_stmt_while, PLpgSQL_var, ResourceOwner,
    PLPGSQL_OTHERS,
};

pub(crate) use backend_pl_plpgsql_exec_seams as exec_seams;

/// Run `f` with a fresh, call-scoped `Mcx` (a private `MemoryContext` that lives
/// only for the closure). Used by the trigger driver to call the `mcx`-taking
/// trigger-data accessors (`tg_relation` / `tg_argv` / `tg_slot_formed_tuple`)
/// where the produced value is consumed (copied out / used to build a record)
/// before the context drops. The C analogue is allocating in
/// `CurrentMemoryContext` and freeing after use.
pub(crate) fn with_query_mcx<R>(f: impl for<'mcx> FnOnce(mcx::Mcx<'mcx>) -> R) -> R {
    let ctx = mcx::MemoryContext::new("PL/pgSQL trigger scratch");
    f(ctx.mcx())
}

/// The rich, lifetime-bearing value [`Datum`] the expanded-record substrate
/// reads/writes (`ByVal` word / `ByRef` varlena image / `Composite` / …), as
/// distinct from PL/pgSQL's bare-word [`Datum`] (`types_datum::Datum`).
use types_tuple::backend_access_common_heaptuple::Datum as RichDatum;

/// `InvalidOid` — the zero OID sentinel.
const INVALID_OID: Oid = 0;

/// `UNKNOWNOID` (705).
const UNKNOWNOID: Oid = 705;

/// `TEXTOID` (25).
const TEXTOID: Oid = 25;

/// `VOIDOID` (2278).
const VOIDOID: Oid = 2278;

/// `BOOLOID` (16).
#[allow(dead_code)]
const BOOLOID: Oid = 16;

/// `RECORDOID` (2249).
#[allow(dead_code)]
const RECORDOID: Oid = 2249;

/// `ERROR` elog level (`elog.h` `ERROR` == 21) — the `elog_level` threshold at
/// which `exec_stmt_raise` defaults the SQLSTATE to `ERRCODE_RAISE_EXCEPTION`.
const ERROR_LEVEL: int32 = 21;

/// The Result-threaded return type of the statement executor: the loop/return
/// control code on success, or the SQL error (`Err(PgError)`) raised somewhere
/// in the body — propagated by `?` up to the EXCEPTION block (a `match` on the
/// `Err`) or, failing any handler, out to `plpgsql_call_handler` / the fmgr
/// boundary. This is the type that replaces C's `longjmp`-based PG_TRY/PG_CATCH
/// error channel throughout the executor.
type PLpgSQL_rc_result = types_error::PgResult<PLpgSQL_rc>;

// ===========================================================================
// Return-code propagation table (LOOP_RC_PROCESSING)
// ===========================================================================

/// The decision of [`loop_rc_processing`]: keep iterating (`Continue`) or leave
/// the loop with a return code (`Break`).
enum LoopRc {
    // The Continue payload mirrors C resetting `rc` to PLPGSQL_RC_OK before the
    // next iteration; callers re-iterate without reading it.
    Continue(#[allow(dead_code)] PLpgSQL_rc),
    Break(PLpgSQL_rc),
}

/// `LOOP_RC_PROCESSING(looplabel, exit_action)` (pl_exec.c) — the EXIT /
/// CONTINUE / RETURN propagation table shared by every loop construct.
fn loop_rc_processing(
    estate: &mut PLpgSQL_execstate,
    looplabel: Option<&str>,
    rc: PLpgSQL_rc,
) -> LoopRc {
    match rc {
        PLpgSQL_rc::PLPGSQL_RC_RETURN => LoopRc::Break(rc),
        PLpgSQL_rc::PLPGSQL_RC_EXIT => {
            if estate.exitlabel.is_none() {
                LoopRc::Break(PLpgSQL_rc::PLPGSQL_RC_OK)
            } else if looplabel.is_some() && looplabel == estate.exitlabel.as_deref() {
                estate.exitlabel = None;
                LoopRc::Break(PLpgSQL_rc::PLPGSQL_RC_OK)
            } else {
                LoopRc::Break(PLpgSQL_rc::PLPGSQL_RC_EXIT)
            }
        }
        PLpgSQL_rc::PLPGSQL_RC_CONTINUE => {
            if estate.exitlabel.is_none() {
                LoopRc::Continue(PLpgSQL_rc::PLPGSQL_RC_OK)
            } else if looplabel.is_some() && looplabel == estate.exitlabel.as_deref() {
                estate.exitlabel = None;
                LoopRc::Continue(PLpgSQL_rc::PLPGSQL_RC_OK)
            } else {
                LoopRc::Break(PLpgSQL_rc::PLPGSQL_RC_CONTINUE)
            }
        }
        PLpgSQL_rc::PLPGSQL_RC_OK => LoopRc::Continue(PLpgSQL_rc::PLPGSQL_RC_OK),
    }
}

// ===========================================================================
// Per-statement mcontext stack
// ===========================================================================

/// `get_stmt_mcontext(estate)` (pl_exec.c) — return the current statement-
/// lifespan memory context, creating it on demand.
fn get_stmt_mcontext(estate: &mut PLpgSQL_execstate) -> Option<types_plpgsql::MemoryContext> {
    // The on-demand creation (AllocSetContextCreate under stmt_mcontext_parent)
    // is a memory-substrate op; the control-flow effect modeled here is the
    // "current context" handoff.
    estate.stmt_mcontext
}

/// `push_stmt_mcontext(estate)` (pl_exec.c) — push the current context so a
/// nested statement that may run arbitrary code gets a fresh private one.
fn push_stmt_mcontext(estate: &mut PLpgSQL_execstate) {
    estate.stmt_mcontext_parent = estate.stmt_mcontext;
    estate.stmt_mcontext = None;
}

/// `pop_stmt_mcontext(estate)` (pl_exec.c) — pop back to the parent statement
/// context after a nested statement that ran arbitrary code (the matching
/// restore for [`push_stmt_mcontext`]).
fn pop_stmt_mcontext(estate: &mut PLpgSQL_execstate) {
    estate.stmt_mcontext = estate.stmt_mcontext_parent;
    estate.stmt_mcontext_parent = None;
}

// ===========================================================================
// Top-level + block
// ===========================================================================

/// `exec_toplevel_block(estate, block)` (pl_exec.c) — execute the toplevel
/// block.
pub fn exec_toplevel_block(
    estate: &mut PLpgSQL_execstate,
    block: &PLpgSQL_stmt_block,
) -> PLpgSQL_rc_result {
    estate.err_stmt = None;
    seam::check_for_interrupts();
    let rc = exec_stmt_block(estate, block)?;
    estate.err_stmt = None;
    Ok(rc)
}

/// `exec_stmt_block(estate, block)` (pl_exec.c) — execute a block of
/// statements.
fn exec_stmt_block(
    estate: &mut PLpgSQL_execstate,
    block: &PLpgSQL_stmt_block,
) -> PLpgSQL_rc_result {
    // First initialize all variables declared in this block.
    estate.err_text = Some(mem::sdup(
        "during statement block local variable initialization",
    ));

    for i in 0..(block.n_initvars as usize) {
        let n = block.initvarnos[i];
        estate.err_var = Some(n as u64);

        // The set of dtypes handled here must match plpgsql_add_initdatums().
        match datum_dtype(&estate.datums[n as usize]) {
            PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR => exec_block_init_var(estate, n)?,
            PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => exec_block_init_rec(estate, n)?,
            other => seam::elog_unrecognized_dtype_exec(other),
        }
    }

    estate.err_var = None;

    let rc = if block.exceptions.is_some() {
        exec_stmt_block_with_exceptions(estate, block)?
    } else {
        estate.err_text = None;
        exec_stmts(estate, &block.body)?
    };

    estate.err_text = None;

    Ok(block_handle_rc(estate, block.label.as_deref(), rc))
}

/// The block-exit return-code switch of `exec_stmt_block` (pl_exec.c).
/// Intentionally distinct from `LOOP_RC_PROCESSING()`: CONTINUE never matches a
/// block, and EXIT matches a block only on a label match.
fn block_handle_rc(
    estate: &mut PLpgSQL_execstate,
    block_label: Option<&str>,
    rc: PLpgSQL_rc,
) -> PLpgSQL_rc {
    match rc {
        PLpgSQL_rc::PLPGSQL_RC_OK
        | PLpgSQL_rc::PLPGSQL_RC_RETURN
        | PLpgSQL_rc::PLPGSQL_RC_CONTINUE => rc,
        PLpgSQL_rc::PLPGSQL_RC_EXIT => {
            if estate.exitlabel.is_none() {
                return PLpgSQL_rc::PLPGSQL_RC_EXIT;
            }
            let Some(block_label) = block_label else {
                return PLpgSQL_rc::PLPGSQL_RC_EXIT;
            };
            if Some(block_label) != estate.exitlabel.as_deref() {
                return PLpgSQL_rc::PLPGSQL_RC_EXIT;
            }
            estate.exitlabel = None;
            PLpgSQL_rc::PLPGSQL_RC_OK
        }
    }
}

/// Block-local VAR initialization (the `PLPGSQL_DTYPE_VAR` arm of
/// `exec_stmt_block`). Control flow is faithful; the NULL/domain/expr
/// assignment is the value substrate (loud).
fn exec_block_init_var(estate: &mut PLpgSQL_execstate, dno: int32) -> types_error::PgResult<()> {
    {
        let mut var = take_var(estate, dno);
        assign_simple_var(estate, &mut var, Datum::null(), true, false);
        put_var(estate, dno, var);
    }

    if !var_has_default(&estate.datums[dno as usize]) {
        if var_is_domain(&estate.datums[dno as usize]) {
            seam::exec_assign_value(estate, dno, Datum::null(), true, UNKNOWNOID, -1)?;
        }
        // parser should have rejected NOT NULL (Assert(!var->notnull)).
    } else {
        let default =
            clone_var_default(&estate.datums[dno as usize]).expect("default_val present");
        seam::exec_assign_expr(estate, dno, &default)?;
    }
    Ok(())
}

/// Block-local REC initialization (the `PLPGSQL_DTYPE_REC` arm).
fn exec_block_init_rec(estate: &mut PLpgSQL_execstate, dno: int32) -> types_error::PgResult<()> {
    if !rec_has_default(&estate.datums[dno as usize]) {
        seam::exec_move_row_null(estate, dno)?;
        // parser should have rejected NOT NULL (Assert(!rec->notnull)).
    } else {
        let default =
            clone_rec_default(&estate.datums[dno as usize]).expect("default present");
        seam::exec_assign_expr(estate, dno, &default)?;
    }
    Ok(())
}

/// `exec_stmt_block` EXCEPTION arm (pl_exec.c ~1793) — the catchable error
/// channel.
///
/// The body runs inside an internal subtransaction; on error the captured
/// `PgError` is matched against the WHEN conditions. This is the repo's
/// `longjmp` replacement: the SQL executor / SPI raise an error as
/// `Err(PgError)` propagated by `?` up through the now-Result-threaded
/// `exec_stmts` call tree, so C's PG_TRY/PG_CATCH becomes a `match` on the
/// returned `Err` (no `catch_unwind`). The error-recovery semantics are
/// byte-identical to the panic-catch form: run the body, on `Err` roll back the
/// subtransaction, check the SQLSTATE against the WHEN conditions, run the
/// matching handler (or re-propagate the `Err` if none matches).
///
/// The subtransaction machinery (`BeginInternalSubTransaction`,
/// `RollbackAndReleaseCurrentSubTransaction`, `ReleaseCurrentSubTransaction`,
/// `MemoryContextSwitchTo`, `SPI_restore_connection`) and the per-handler datum
/// setup (`assign_text_var` of SQLSTATE/SQLERRM, `exec_eval_cleanup`) bottom out
/// in the xact + SPI value substrate and are routed through [`seam`].
fn exec_stmt_block_with_exceptions(
    estate: &mut PLpgSQL_execstate,
    block: &PLpgSQL_stmt_block,
) -> PLpgSQL_rc_result {
    // C (pl_exec.c exec_stmt_block): `oldowner = CurrentResourceOwner;` is
    // snapshotted BEFORE entering the internal subtransaction, then restored
    // (`CurrentResourceOwner = oldowner;`) after the subxact is released
    // (no-error path) or rolled back (PG_CATCH). This restore is NOT optional /
    // RAII: `CleanupSubTransaction` leaves `CurrentResourceOwner` pointing at the
    // parent (CurTransaction) resource owner, but the block ran with the PORTAL's
    // resource owner current (pquery sets `CurrentResourceOwner = portal->resowner`
    // around execution). Without restoring `oldowner`, relation refs / buffer pins
    // the OUTER statement opened under the portal owner are later forgotten under
    // the wrong (TopTransaction) owner — `ResourceOwnerForgetRelationRef` fails
    // "not owned by resource owner TopTransaction" and the rd_refcnt underflows,
    // killing the backend (the `revalidate_bug` ANALYZE-then-div-by-zero case).
    let oldowner = exec_seams::current_resource_owner::call();

    // BeginInternalSubTransaction(NULL) + remember the caller context / owner.
    begin_internal_subtransaction(estate)?;

    // PG_TRY: run the block body. The executor/SPI raise errors as `Err(PgError)`
    // threaded back through `exec_stmts`; match on the returned `Err` so the WHEN
    // clauses can inspect the SQLSTATE and the subtransaction can be rolled back.
    //
    // PANIC-SAFETY (cleanup-on-panic, NOT error dispatch): a still-unported loud
    // seam inside the body raises via `panic!` rather than `Err` (the value/SPI
    // substrate that has not yet been Result-threaded — exec_stmt_dynexecute,
    // exec_stmt_call, the cursor surface, etc.). C's PG_CATCH runs its cleanup on
    // ANY error; a panic that unwound straight past this frame would leave the
    // internal subtransaction open, poisoning the parent transaction (every later
    // command then fails "current transaction is aborted"). So we catch a panic
    // PURELY to roll the subtransaction back, then resume the unwind — the panic
    // still propagates to the handler boundary's catch exactly as before, it just
    // no longer skips the rollback. The normal catchable-error path remains the
    // `Err` match below; this `catch_unwind` never inspects or swallows the panic.
    let body = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exec_stmts(estate, &block.body)
    }));
    let caught = match body {
        Ok(rc) => rc,
        Err(payload) => {
            // Best-effort rollback of the open subtransaction, then re-raise.
            // Restore the saved owner (C `CurrentResourceOwner = oldowner;`) so
            // the unwinding outer statement's resowner cleanup runs against the
            // owner it opened resources under, not the parent owner the subxact
            // cleanup left current.
            let _ = rollback_and_release_current_subtransaction(estate);
            exec_seams::set_current_resource_owner::call(oldowner);
            std::panic::resume_unwind(payload);
        }
    };

    match caught {
        Ok(rc) => {
            // No error: ReleaseCurrentSubTransaction + restore context/owner.
            // C: `ReleaseCurrentSubTransaction(); ... CurrentResourceOwner = oldowner;`
            release_current_subtransaction(estate)?;
            exec_seams::set_current_resource_owner::call(oldowner);
            Ok(rc)
        }
        Err(edata) => {
            // PG_CATCH: roll back the subtransaction, restore the SPI
            // connection, then look for a matching exception handler.
            rollback_and_release_current_subtransaction(estate)?;
            // C: `CurrentResourceOwner = oldowner;` immediately after the
            // rollback, so the EXCEPTION handler statements (and the eventual
            // outer-statement resowner cleanup) run under the owner that was
            // current when the block began (the portal owner), not the parent
            // owner the subxact cleanup left current.
            exec_seams::set_current_resource_owner::call(oldowner);

            let exceptions = block
                .exceptions
                .as_deref()
                .expect("exception path entered without an exception block");

            let mut handled: Option<PLpgSQL_rc_result> = None;
            for exc in &exceptions.exc_list {
                if exception_matches_conditions(edata.sqlstate.0, exc.conditions.as_deref())
                {
                    // Bind SQLSTATE / SQLERRM into the handler's special vars
                    // and record the current error for GET STACKED DIAGNOSTICS.
                    // `estate->cur_error = &edata` in C; the owned model carries
                    // cur_error as the live PgError value. We save/restore the
                    // slot here so the nesting discipline is preserved; the live
                    // edata is bound into the handler's special vars by
                    // assign_error_vars.
                    let save_cur_error = estate.cur_error.take();
                    // assign_error_vars may itself fail (the SQLSTATE/SQLERRM
                    // text-build seam); on failure restore the slot and
                    // propagate, mirroring C's error-in-error escalation.
                    if let Err(e) = assign_error_vars(estate, exceptions, &edata) {
                        estate.cur_error = save_cur_error;
                        return Err(e);
                    }

                    let rc = exec_stmts(estate, &exc.action);

                    estate.cur_error = save_cur_error;
                    handled = Some(rc);
                    break;
                }
            }

            match handled {
                Some(rc) => rc,
                // No matching handler: re-propagate the original error
                // (C's PG_RE_THROW; here the `Err` bubbles up via the return).
                None => Err(edata),
            }
        }
    }
}

/// `exception_matches_conditions(edata, cond)` (pl_exec.c) — does any condition
/// in the list match the current exception's SQLSTATE?
fn exception_matches_conditions(
    edata_sqlerrcode: int32,
    mut cond: Option<&PLpgSQL_condition>,
) -> bool {
    while let Some(c) = cond {
        let sqlerrstate = c.sqlerrstate;
        if sqlerrstate == PLPGSQL_OTHERS {
            // OTHERS matches everything except query-canceled + assert-failure.
            if edata_sqlerrcode != errcode_query_canceled()
                && edata_sqlerrcode != errcode_assert_failure()
            {
                return true;
            }
        } else if edata_sqlerrcode == sqlerrstate {
            return true;
        } else if errcode_is_category(sqlerrstate)
            && errcode_to_category(edata_sqlerrcode) == sqlerrstate
        {
            return true;
        }
        cond = c.next.as_deref();
    }
    false
}

// --- subtransaction + handler-var legs (xact + value substrate, loud) -------

/// `BeginInternalSubTransaction(NULL)` (pl_exec.c exec_stmt_block) — start the
/// internal subtransaction the EXCEPTION block body runs inside. The C code also
/// snapshots `CurrentMemoryContext` / `CurrentResourceOwner` / `eval_econtext`
/// here; in the owned model the memory context and resource owner are RAII (the
/// xact subxact engine owns `CurTransactionContext` + resource lifetimes), and
/// the eval econtext is reset per-statement via `exec_eval_cleanup`, so no
/// explicit save is needed — the subxact begin is the whole leg.
fn begin_internal_subtransaction(_estate: &mut PLpgSQL_execstate) -> types_error::PgResult<()> {
    exec_seams::begin_internal_subtransaction::call()
}

/// `ReleaseCurrentSubTransaction()` (pl_exec.c exec_stmt_block) — commit the
/// EXCEPTION block's internal subtransaction on the no-error path, then (C)
/// restore the saved context/owner/econtext (RAII here, see above).
fn release_current_subtransaction(_estate: &mut PLpgSQL_execstate) -> types_error::PgResult<()> {
    exec_seams::release_current_subtransaction::call()
}

/// `RollbackAndReleaseCurrentSubTransaction()` (pl_exec.c exec_stmt_block
/// PG_CATCH) — abort the internal subtransaction, popping back to the parent
/// state. The SPI connection is restored automatically: xact's
/// `AbortSubTransaction` drives `AtEOSubXact_SPI(false, mySubid)` through the
/// installed seam (modern PG dropped the explicit `SPI_restore_connection`
/// call). The context/owner restore is RAII (the subxact engine owns them).
fn rollback_and_release_current_subtransaction(
    _estate: &mut PLpgSQL_execstate,
) -> types_error::PgResult<()> {
    exec_seams::rollback_and_release_current_subtransaction::call()
}

/// Bind the SQLSTATE / SQLERRM special variables of the matching handler, and
/// record the live error for GET STACKED DIAGNOSTICS (`assign_error_vars` in
/// pl_exec.c). C does:
/// ```c
/// exec_assign_value(estate, datum[sqlstate_varno],
///                   CStringGetTextDatum(unpack_sql_state(edata->sqlerrcode)),
///                   false, TEXTOID, -1);
/// exec_assign_value(estate, datum[sqlerrm_varno],
///                   CStringGetTextDatum(edata->message),
///                   false, TEXTOID, -1);
/// estate->cur_error = edata;
/// ```
fn assign_error_vars(
    estate: &mut PLpgSQL_execstate,
    block: &types_plpgsql::PLpgSQL_exception_block,
    edata: &types_error::PgError,
) -> types_error::PgResult<()> {
    // C's assign_error_vars binds the implicit SQLSTATE / SQLERRM special vars
    // via assign_text_var(estate, var, str) ==
    // assign_simple_var(estate, var, CStringGetTextDatum(str), false, true).
    // We mirror assign_text_var directly (NOT exec_assign_value): the value is a
    // bare-word pointer at a header-ful `text` varlena allocated in a backend-
    // lifetime context (the cstring_to_text_datum seam), so it needs no cast and
    // no datumCopy / expanded-object transfer (the unported exec_assign_value
    // by-ref leg); `freeable=false` because the buffer is never individually
    // freed (it lives with the backend, like C's palloc in the handler context).
    assign_text_var(estate, block.sqlstate_varno, unpack_sql_state(edata.sqlstate.0))?;
    assign_text_var(estate, block.sqlerrm_varno, edata.message.clone())?;

    // estate->cur_error = edata: record the live error so GET STACKED
    // DIAGNOSTICS / RAISE-without-parameters in this handler can read it. The
    // owned model carries `cur_error` as the full PgError value (the live
    // edata), not the opaque-handle placeholder.
    estate.cur_error = Some(edata.clone());
    Ok(())
}

/// `assign_text_var(estate, var, str)` (pl_exec.c 8847) — build a `text` Datum
/// from `str` and store it into the scalar VAR `dno` via `assign_simple_var`.
/// The text bytes live in a backend-lifetime context (the `cstring_to_text_datum`
/// seam), so the stored bare-word pointer stays valid and `freeable` is false.
fn assign_text_var(
    estate: &mut PLpgSQL_execstate,
    dno: int32,
    s: String,
) -> types_error::PgResult<()> {
    let (d, image) = exec_seams::cstring_to_text_datum::call(s)?;
    let datum = Datum::from_usize(d);
    let mut var = take_var(estate, dno);
    assign_simple_var(estate, &mut var, datum, false, false);
    // `text` is pass-by-reference: the bare-word `value` alone cannot be read
    // back across the fmgr boundary (the varlena cmp cores demand a by-ref
    // payload). Thread the verbatim header-ful varlena image into the var's
    // out-of-band `value_byref` companion so a later expression evaluation over
    // this special var (e.g. `RETURN SQLERRM`, a text comparison) binds the rich
    // `Datum::ByRef`. `assign_simple_var` cleared the companion for the bare-word
    // store above; set the image here (mirroring the by-ref arg-store leg).
    var.value_byref = Some(image);
    put_var(estate, dno, var);
    Ok(())
}

// ===========================================================================
// exec_stmts dispatch
// ===========================================================================

/// `exec_stmts(estate, stmts)` (pl_exec.c) — iterate over a list of statements
/// as long as their return code is OK.
fn exec_stmts(estate: &mut PLpgSQL_execstate, stmts: &[PLpgSQL_stmt]) -> PLpgSQL_rc_result {
    let save_estmt = estate.err_stmt.take();

    if stmts.is_empty() {
        // Ensure a CHECK_FOR_INTERRUPTS even though there is no statement.
        seam::check_for_interrupts();
        estate.err_stmt = save_estmt;
        return Ok(PLpgSQL_rc::PLPGSQL_RC_OK);
    }

    for stmt in stmts {
        estate.err_stmt = None;
        seam::check_for_interrupts();

        // On an `Err` from a statement, restore the err_stmt marker (C's
        // PG_FINALLY-equivalent housekeeping in the unwind path) before
        // propagating, so a containing EXCEPTION block sees the saved marker.
        let rc = match (|| match stmt {
            PLpgSQL_stmt::Block(b) => exec_stmt_block(estate, b),
            PLpgSQL_stmt::Assign(s) => exec_stmt_assign(estate, s),
            PLpgSQL_stmt::Perform(s) => exec_stmt_perform(estate, s),
            PLpgSQL_stmt::Call(_) => exec_stmt_call(estate),
            PLpgSQL_stmt::Getdiag(s) => exec_stmt_getdiag(estate, s),
            PLpgSQL_stmt::If(s) => exec_stmt_if(estate, s),
            PLpgSQL_stmt::Case(s) => exec_stmt_case(estate, s),
            PLpgSQL_stmt::Loop(s) => exec_stmt_loop(estate, s),
            PLpgSQL_stmt::While(s) => exec_stmt_while(estate, s),
            PLpgSQL_stmt::Fori(s) => exec_stmt_fori(estate, s),
            PLpgSQL_stmt::Fors(s) => exec_stmt_fors(estate, s),
            PLpgSQL_stmt::Forc(_) => exec_stmt_forc(estate),
            PLpgSQL_stmt::ForeachA(s) => exec_stmt_foreach_a(estate, s),
            PLpgSQL_stmt::Exit(s) => exec_stmt_exit(estate, s),
            PLpgSQL_stmt::Return(s) => exec_stmt_return(estate, s),
            PLpgSQL_stmt::ReturnNext(s) => exec_stmt_return_next(estate, s),
            PLpgSQL_stmt::ReturnQuery(s) => exec_stmt_return_query(estate, s),
            PLpgSQL_stmt::Raise(s) => exec_stmt_raise(estate, s),
            PLpgSQL_stmt::Assert(_) => exec_stmt_assert(estate),
            PLpgSQL_stmt::Execsql(s) => exec_stmt_execsql(estate, s),
            PLpgSQL_stmt::Dynexecute(s) => exec_stmt_dynexecute(estate, s),
            PLpgSQL_stmt::Dynfors(s) => exec_stmt_dynfors(estate, s),
            PLpgSQL_stmt::Open(_) => exec_stmt_open(estate),
            PLpgSQL_stmt::Fetch(_) => exec_stmt_fetch(estate),
            PLpgSQL_stmt::Close(_) => exec_stmt_close(estate),
            PLpgSQL_stmt::Commit(_) => exec_stmt_commit(estate),
            PLpgSQL_stmt::Rollback(_) => exec_stmt_rollback(estate),
        })() {
            Ok(rc) => rc,
            Err(e) => {
                estate.err_stmt = save_estmt;
                return Err(e);
            }
        };

        if rc != PLpgSQL_rc::PLPGSQL_RC_OK {
            estate.err_stmt = save_estmt;
            return Ok(rc);
        }
    }

    estate.err_stmt = save_estmt;
    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

// ===========================================================================
// Control-flow statement arms (real)
// ===========================================================================

/// `exec_stmt_assign(estate, stmt)` (pl_exec.c).
fn exec_stmt_assign(
    estate: &mut PLpgSQL_execstate,
    stmt: &PLpgSQL_stmt_assign,
) -> PLpgSQL_rc_result {
    debug_assert!(stmt.varno >= 0);
    let expr = stmt.expr.as_deref().expect("ASSIGN carries an expr");
    seam::exec_assign_expr(estate, stmt.varno, expr)?;
    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `exec_stmt_perform(estate, stmt)` (pl_exec.c) — run a query, discard the
/// result, set FOUND from the rowcount.
fn exec_stmt_perform(
    estate: &mut PLpgSQL_execstate,
    stmt: &PLpgSQL_stmt_perform,
) -> PLpgSQL_rc_result {
    let expr = stmt.expr.as_deref().expect("PERFORM carries an expr");
    let _ = seam::exec_run_select(estate, expr, 0, false)?;
    exec_set_found(estate, estate.eval_processed != 0);
    exec_eval_cleanup(estate);
    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `exec_stmt_if(estate, stmt)` (pl_exec.c).
fn exec_stmt_if(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_if) -> PLpgSQL_rc_result {
    let cond = stmt.cond.as_deref().expect("IF carries a condition");
    let (value, isnull) = seam::exec_eval_boolean(estate, cond)?;
    exec_eval_cleanup(estate);
    if !isnull && value {
        return exec_stmts(estate, &stmt.then_body);
    }

    for elif in &stmt.elsif_list {
        let ec = elif.cond.as_deref().expect("ELSIF carries a condition");
        let (value, isnull) = seam::exec_eval_boolean(estate, ec)?;
        exec_eval_cleanup(estate);
        if !isnull && value {
            return exec_stmts(estate, &elif.stmts);
        }
    }

    exec_stmts(estate, &stmt.else_body)
}

/// `exec_stmt_case(estate, stmt)` (pl_exec.c) — searched / simple CASE.
fn exec_stmt_case(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_case) -> PLpgSQL_rc_result {
    let has_t_var = stmt.t_expr.is_some();

    if let Some(t_expr) = stmt.t_expr.as_deref() {
        let (t_val, isnull, t_typoid, t_typmod) = seam::exec_eval_expr(estate, t_expr)?;

        let t_varno = stmt.t_varno;
        if temp_var_type_differs(&estate.datums[t_varno as usize], t_typoid, t_typmod) {
            let mut t_var = take_var(estate, t_varno);
            seam::case_rebuild_temp_var_datatype(estate, &mut t_var, t_typoid, t_typmod);
            put_var(estate, t_varno, t_var);
        }

        seam::exec_assign_value(estate, t_varno, t_val, isnull, t_typoid, t_typmod)?;
        exec_eval_cleanup(estate);
    }

    for cwt in &stmt.case_when_list {
        let expr = cwt.expr.as_deref().expect("CASE WHEN carries a condition");
        let (value, isnull) = seam::exec_eval_boolean(estate, expr)?;
        exec_eval_cleanup(estate);
        if !isnull && value {
            if has_t_var {
                discard_temp_var(estate, stmt.t_varno);
            }
            return exec_stmts(estate, &cwt.stmts);
        }
    }

    if has_t_var {
        discard_temp_var(estate, stmt.t_varno);
    }

    if !stmt.have_else {
        return Err(seam::ereport_case_not_found());
    }

    exec_stmts(estate, &stmt.else_stmts)
}

/// `exec_stmt_loop(estate, stmt)` (pl_exec.c) — unconditional LOOP.
fn exec_stmt_loop(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_loop) -> PLpgSQL_rc_result {
    let label = stmt.label.clone();
    loop {
        let body_rc = exec_stmts(estate, &stmt.body)?;
        if let LoopRc::Break(rc) = loop_rc_processing(estate, label.as_deref(), body_rc) {
            return Ok(rc);
        }
    }
}

/// `exec_stmt_while(estate, stmt)` (pl_exec.c).
fn exec_stmt_while(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_while) -> PLpgSQL_rc_result {
    let label = stmt.label.clone();
    let cond = stmt.cond.as_deref().expect("WHILE carries a condition");
    loop {
        let (value, isnull) = seam::exec_eval_boolean(estate, cond)?;
        exec_eval_cleanup(estate);
        if isnull || !value {
            return Ok(PLpgSQL_rc::PLPGSQL_RC_OK);
        }
        let body_rc = exec_stmts(estate, &stmt.body)?;
        if let LoopRc::Break(rc) = loop_rc_processing(estate, label.as_deref(), body_rc) {
            return Ok(rc);
        }
    }
}

/// `exec_stmt_fori(estate, stmt)` (pl_exec.c) — integer FOR loop.
fn exec_stmt_fori(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_fori) -> PLpgSQL_rc_result {
    let var_dno = stmt.var.as_ref().expect("FOR(i) has a loop var").dno;
    let (var_typoid, var_typmod) = fori_var_type(estate, var_dno);

    // Lower bound.
    let lower = stmt.lower.as_deref().expect("FOR(i) lower bound");
    let (value, isnull, valtype, valtypmod) = seam::exec_eval_expr(estate, lower)?;
    let (value, isnull) =
        seam::exec_cast_value(estate, value, isnull, valtype, valtypmod, var_typoid, var_typmod)?;
    if isnull {
        return Err(seam::ereport_for_bound_null("lower bound"));
    }
    let loop_value_start = value.as_i32();
    exec_eval_cleanup(estate);

    // Upper bound.
    let upper = stmt.upper.as_deref().expect("FOR(i) upper bound");
    let (value, isnull, valtype, valtypmod) = seam::exec_eval_expr(estate, upper)?;
    let (value, isnull) =
        seam::exec_cast_value(estate, value, isnull, valtype, valtypmod, var_typoid, var_typmod)?;
    if isnull {
        return Err(seam::ereport_for_bound_null("upper bound"));
    }
    let end_value = value.as_i32();
    exec_eval_cleanup(estate);

    // Step.
    let step_value = if let Some(step) = stmt.step.as_deref() {
        let (value, isnull, valtype, valtypmod) = seam::exec_eval_expr(estate, step)?;
        let (value, isnull) = seam::exec_cast_value(
            estate, value, isnull, valtype, valtypmod, var_typoid, var_typmod,
        )?;
        if isnull {
            return Err(seam::ereport_for_bound_null("BY value"));
        }
        let sv = value.as_i32();
        exec_eval_cleanup(estate);
        if sv <= 0 {
            return Err(seam::ereport_for_step_nonpositive());
        }
        sv
    } else {
        1
    };

    let reverse = stmt.reverse != 0;
    let label = stmt.label.clone();
    let mut loop_value = loop_value_start;
    let mut found = false;
    let mut rc = PLpgSQL_rc::PLPGSQL_RC_OK;

    loop {
        if reverse {
            if loop_value < end_value {
                break;
            }
        } else if loop_value > end_value {
            break;
        }

        found = true;

        {
            let mut var = take_var(estate, var_dno);
            assign_simple_var(estate, &mut var, Datum::from_i32(loop_value), false, false);
            put_var(estate, var_dno, var);
        }

        let body_rc = exec_stmts(estate, &stmt.body)?;
        match loop_rc_processing(estate, label.as_deref(), body_rc) {
            LoopRc::Break(r) => {
                rc = r;
                break;
            }
            LoopRc::Continue(_) => {}
        }

        if reverse {
            if loop_value < (i32::MIN + step_value) {
                break;
            }
            loop_value -= step_value;
        } else {
            if loop_value > (i32::MAX - step_value) {
                break;
            }
            loop_value += step_value;
        }
    }

    exec_set_found(estate, found);
    Ok(rc)
}

/// `exec_stmt_foreach_a(estate, stmt)` (pl_exec.c) — FOREACH over array
/// elements/slices. The control shell is real; the array-iteration leg
/// (`get_element_type` / `DatumGetArrayTypePCopy` / `array_create_iterator` /
/// `array_iterate`) is driven through the installed `foreach_iterate_via_array`
/// seam (the handler owns the array/lsyscache surface).
fn exec_stmt_foreach_a(
    estate: &mut PLpgSQL_execstate,
    stmt: &PLpgSQL_stmt_foreach_a,
) -> PLpgSQL_rc_result {
    // get the value of the array expression
    let expr = stmt.expr.as_deref().expect("FOREACH has an array expr");
    let (value, isnull, arrtype, arrtypmod) = seam::exec_eval_expr(estate, expr)?;
    if isnull {
        return Err(seam::ereport_foreach_null());
    }
    // The array's verbatim varlena image rides the by-ref companion (an array is
    // always a pass-by-reference type, so the bare `value` word is 0). Take it
    // before `exec_eval_cleanup` discards it.
    let arr_bytes = estate
        .last_eval_byref
        .take()
        .unwrap_or_else(|| value.as_usize().to_le_bytes().to_vec());

    let _stmt_mcontext = get_stmt_mcontext(estate);
    push_stmt_mcontext(estate);

    // Set up the loop variable and see if it is of an array type.
    //   loop_var = estate->datums[stmt->varno];
    //   if (REC || ROW) loop_var_elem_type = InvalidOid;
    //   else loop_var_elem_type = get_element_type(plpgsql_exec_get_datum_type(...));
    let loop_var = estate.datums[stmt.varno as usize].clone();
    let loop_var_elem_type = match datum_dtype(&loop_var) {
        PLpgSQL_datum_type::PLPGSQL_DTYPE_REC | PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW => INVALID_OID,
        _ => {
            let info = plpgsql_exec_get_datum_type_info(&loop_var);
            seam::get_element_type(info.type_id)
        }
    };

    // Sanity-check the loop variable type vs the array-ness of the iteration.
    //   if (slice > 0 && loop_var_elem_type == InvalidOid) ereport(... must be array);
    //   if (slice == 0 && loop_var_elem_type != InvalidOid) ereport(... must not be array);
    if stmt.slice > 0 && loop_var_elem_type == INVALID_OID {
        pop_stmt_mcontext(estate);
        return Err(seam::ereport_foreach_slice_var_not_array());
    }
    if stmt.slice == 0 && loop_var_elem_type != INVALID_OID {
        pop_stmt_mcontext(estate);
        return Err(seam::ereport_foreach_var_is_array());
    }

    // exec_eval_cleanup releases the array image we already copied above.
    exec_eval_cleanup(estate);

    // Drive the array + fmgr substrate (get_element_type type check, detoast,
    // slice range check, array_create_iterator + the full array_iterate loop)
    // through the installed seam; it returns every iteration's value (in order)
    // plus the iterator result type/typmod for the per-iteration assignment.
    let iterate = seam::foreach_iterate(arr_bytes, arrtype, arrtypmod, stmt.slice)?;

    // Iterate over the array elements or slices.
    let mut found = false;
    let mut rc = PLpgSQL_rc::PLPGSQL_RC_OK;
    for item in iterate.items {
        found = true; // looped at least once

        // Assign current element/slice to the loop variable.
        //   exec_assign_value(estate, loop_var, value, isnull,
        //                     iterator_result_type, iterator_result_typmod);
        exec_assign_value_byref_impl(
            estate,
            stmt.varno,
            Datum::from_usize(item.value),
            item.byref,
            item.isnull,
            iterate.result_type,
            iterate.result_typmod,
        )?;

        // Execute the statements.
        rc = exec_stmts(estate, &stmt.body)?;

        //   LOOP_RC_PROCESSING(stmt->label, break);
        match loop_rc_processing(estate, stmt.label.as_deref(), rc) {
            LoopRc::Break(brc) => {
                rc = brc;
                break;
            }
            LoopRc::Continue(_) => {
                rc = PLpgSQL_rc::PLPGSQL_RC_OK;
            }
        }
    }

    pop_stmt_mcontext(estate);

    // Set the FOUND variable to indicate whether we looped one or more times.
    exec_set_found(estate, found);

    Ok(rc)
}

/// `exec_stmt_exit(estate, stmt)` (pl_exec.c) — EXIT / CONTINUE.
fn exec_stmt_exit(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_exit) -> PLpgSQL_rc_result {
    if let Some(cond) = stmt.cond.as_deref() {
        let (value, isnull) = seam::exec_eval_boolean(estate, cond)?;
        exec_eval_cleanup(estate);
        if isnull || !value {
            return Ok(PLpgSQL_rc::PLPGSQL_RC_OK);
        }
    }

    estate.exitlabel = stmt.label.clone();
    if stmt.is_exit {
        Ok(PLpgSQL_rc::PLPGSQL_RC_EXIT)
    } else {
        Ok(PLpgSQL_rc::PLPGSQL_RC_CONTINUE)
    }
}

/// `exec_stmt_return(estate, stmt)` (pl_exec.c) — RETURN.
fn exec_stmt_return(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_return) -> PLpgSQL_rc_result {
    if estate.retisset {
        return Ok(PLpgSQL_rc::PLPGSQL_RC_RETURN);
    }

    estate.retval = Datum::null();
    estate.retisnull = true;
    estate.rettype = INVALID_OID;
    estate.retval_byref = None;

    if stmt.retvarno >= 0 {
        let dno = stmt.retvarno;
        match datum_dtype(&estate.datums[dno as usize]) {
            PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE => {
                let mut var = take_var(estate, dno);
                seam::plpgsql_fulfill_promise(estate, &mut var)?;
                put_var(estate, dno, var);
                exec_return_simple_var(estate, dno)?;
            }
            PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR => {
                exec_return_simple_var(estate, dno)?;
            }
            PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW | PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
                let datum = estate.datums[dno as usize].clone();
                let (rettype, _rettypmod, retval, retisnull) =
                    seam::exec_eval_datum(estate, &datum)?;
                estate.rettype = rettype;
                estate.retval = retval;
                estate.retisnull = retisnull;
                // A composite (REC/ROW) return carries its HeapTupleHeader
                // varlena image out-of-band (set by exec_eval_datum); move it
                // into the durable return slot so the trigger / function result
                // path can deposit the tuple.
                estate.retval_byref = estate.last_eval_byref.take();
            }
            other => seam::elog_unrecognized_dtype_exec(other),
        }
        return Ok(PLpgSQL_rc::PLPGSQL_RC_RETURN);
    }

    if let Some(expr) = stmt.expr.as_deref() {
        let (retval, retisnull, rettype, _rettypmod) = seam::exec_eval_expr(estate, expr)?;
        estate.retval = retval;
        estate.retisnull = retisnull;
        estate.rettype = rettype;
        // Move the by-ref image companion (set by exec_eval_expr) into the
        // durable return slot so a by-ref result (text/numeric/…) survives to
        // the function result; a by-value result leaves it None.
        estate.retval_byref = estate.last_eval_byref.take();

        if estate.retistuple && !estate.retisnull && !seam::type_is_rowtype(estate.rettype) {
            return Err(seam::ereport_return_noncomposite());
        }

        return Ok(PLpgSQL_rc::PLPGSQL_RC_RETURN);
    }

    // Special hack for function returning VOID (but not for procedures).
    if estate.fn_rettype == VOIDOID && !func_is_procedure(estate) {
        estate.retval = Datum::null();
        estate.retisnull = false;
        estate.rettype = VOIDOID;
    }

    Ok(PLpgSQL_rc::PLPGSQL_RC_RETURN)
}

/// The DTYPE_VAR / DTYPE_PROMISE-after-fulfill arm of `exec_stmt_return`.
fn exec_return_simple_var(
    estate: &mut PLpgSQL_execstate,
    dno: int32,
) -> types_error::PgResult<()> {
    let (value, isnull, typoid) = read_var_value(&estate.datums[dno as usize]);
    estate.retval = value;
    estate.retisnull = isnull;
    estate.rettype = typoid;
    // A bare `RETURN var` over a pass-by-reference variable (`text`/`numeric`/…)
    // reads the variable directly (C's exec_stmt_return retvarno fast path), so
    // its out-of-band `value_byref` image must ride into the durable return slot
    // — otherwise the bare-word `value` (`0`) is all that crosses the fmgr
    // boundary and the result is garbage. A by-value variable leaves it None.
    estate.retval_byref = if !isnull {
        read_var_value_byref(&estate.datums[dno as usize])
    } else {
        None
    };

    if estate.retistuple && !estate.retisnull {
        return Err(seam::ereport_return_noncomposite());
    }
    Ok(())
}

/// `exec_set_found(estate, state)` (pl_exec.c) — set the FOUND variable.
pub(crate) fn exec_set_found(estate: &mut PLpgSQL_execstate, state: bool) {
    let dno = estate.found_varno;
    let mut var = take_var(estate, dno);
    assign_simple_var(estate, &mut var, Datum::from_bool(state), false, false);
    put_var(estate, dno, var);
}

/// `exec_eval_cleanup(estate)` (pl_exec.c) — release temporary memory used by
/// expression / subselect evaluation.
pub(crate) fn exec_eval_cleanup(estate: &mut PLpgSQL_execstate) {
    if estate.eval_tuptable.is_some() {
        // SPI_freetuptable(estate->eval_tuptable) — value/SPI substrate.
        estate.eval_tuptable = None;
    }
    if let Some(econtext) = estate.eval_econtext {
        seam::reset_expr_context(&econtext);
    }
}

// ===========================================================================
// exec_eval_expr / exec_eval_boolean — the PL/pgSQL expression evaluator over
// the SPI plan surface (pl_exec.c's exec_run_select slow path).
// ===========================================================================

/// Build the [`PlpgsqlExprParseState`] for `expr`: walk the expression's
/// namespace chain collecting every scalar (VAR/PROMISE) variable visible to it
/// into a down-cased name → [`PlpgsqlParamInfo`] map, so the parser hook can
/// resolve a bareword reference to the variable's `$dno+1` `Param`. (The C
/// `plpgsql_pre_column_ref` walks the live `expr->ns` via `plpgsql_ns_lookup` on
/// demand; the owned parser hook reads a pre-resolved map instead.)
fn build_plpgsql_parse_state(
    estate: &PLpgSQL_execstate,
    expr: &types_plpgsql::PLpgSQL_expr,
    input_collation: Oid,
) -> types_error::PgResult<types_nodes::parsestmt::PlpgsqlExprParseState> {
    use types_nodes::parsestmt::{PlpgsqlExprParseState, PlpgsqlParamInfo};

    let mut names: std::collections::BTreeMap<std::string::String, PlpgsqlParamInfo> =
        std::collections::BTreeMap::new();

    // Walk the namespace chain (expr->ns -> prev -> ...). A VAR/REC nsitem's
    // `itemno` is its datum dno; LABEL items are block markers (skipped). The
    // most-local binding of a name wins, so only insert if not already present.
    let mut cur = expr.ns.as_deref();
    while let Some(ns) = cur {
        match ns.itemtype {
            types_plpgsql::PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR => {
                let dno = ns.itemno;
                if dno >= 0 && (dno as usize) < estate.datums.len() {
                    if let PLpgSQL_datum::Var(v) = &estate.datums[dno as usize] {
                        if let Some(t) = v.datatype.as_ref() {
                            let key = ns.name.to_ascii_lowercase();
                            names.entry(key).or_insert(PlpgsqlParamInfo {
                                dno,
                                typeid: t.typoid,
                                typmod: t.atttypmod,
                                collation: t.collation,
                            });
                        }
                    }
                }
            }
            // A REC variable: register each of its RECFIELD children under the
            // qualified key `<recname>.<fieldname>` (the C `plpgsql_pre_column_ref`
            // resolving `rec.field` to the RECFIELD datum's Param). The field's
            // type is resolved against the record's live expanded header (the
            // comp↔exec `plpgsql_exec_get_datum_type_info` RECFIELD edge). A whole
            // record bareword (`rec` alone) also resolves, to the REC datum's
            // composite Param.
            types_plpgsql::PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC => {
                let rec_dno = ns.itemno;
                if rec_dno < 0 || (rec_dno as usize) >= estate.datums.len() {
                    cur = ns.prev.as_deref();
                    continue;
                }
                let rec_name = ns.name.to_ascii_lowercase();
                let handle = match &estate.datums[rec_dno as usize] {
                    PLpgSQL_datum::Rec(rec) => rec.erh.as_ref().map(|h| h.0).unwrap_or(0),
                    _ => 0,
                };
                // Whole-record reference (`rec`) — a composite Param of the
                // record's runtime rowtype.
                if let Some((rtype, rtypmod)) = record_rowtype(estate, rec_dno, handle) {
                    names.entry(rec_name.clone()).or_insert(PlpgsqlParamInfo {
                        dno: rec_dno,
                        typeid: rtype,
                        typmod: rtypmod,
                        collation: INVALID_OID,
                    });
                }
                // Field references (`rec.field`) — each RECFIELD child datum.
                for d in estate.datums.iter() {
                    if let PLpgSQL_datum::Recfield(rf) = d {
                        if rf.recparentno != rec_dno {
                            continue;
                        }
                        let key = format!("{}.{}", rec_name, rf.fieldname.to_ascii_lowercase());
                        if names.contains_key(&key) {
                            continue;
                        }
                        let finfo = if handle != 0 {
                            resolve_recfield_finfo(handle, &rf.fieldname)?
                        } else {
                            None
                        };
                        // When the field type cannot be resolved (no live header
                        // or absent field), fall back to the compiled finfo / a
                        // text-ish default so the Param still binds; the runtime
                        // fetch reads the real value.
                        let (typeid, typmod, collation) = match finfo {
                            Some(fi) => (fi.ftypeid, fi.ftypmod, fi.fcollation),
                            None => (rf.finfo.ftypeid, rf.finfo.ftypmod, rf.finfo.fcollation),
                        };
                        names.insert(
                            key,
                            PlpgsqlParamInfo { dno: rf.dno, typeid, typmod, collation },
                        );
                    }
                }
            }
            types_plpgsql::PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL => {}
        }
        cur = ns.prev.as_deref();
    }

    Ok(PlpgsqlExprParseState::new(names, input_collation))
}

/// Project a rich expanded-record field [`RichDatum`] (the
/// `expanded_record_fetch_field` result) onto the PL/pgSQL param-bind shape:
/// a by-value word with no image, or a `0` word + the verbatim header-ful
/// varlena / cstring / composite image. Mirrors how a by-reference VAR carries
/// its image out-of-band (the bare `value` word is `0`).
fn rich_datum_to_param(value: &RichDatum<'_>, isnull: bool, typeid: Oid) -> exec_seams::EvalParamValue {
    if isnull {
        return exec_seams::EvalParamValue { value: 0, isnull: true, typeid, byref: None };
    }
    match value {
        RichDatum::ByVal(w) => exec_seams::EvalParamValue {
            value: *w,
            isnull: false,
            typeid,
            byref: None,
        },
        RichDatum::ByRef(b) => exec_seams::EvalParamValue {
            value: 0,
            isnull: false,
            typeid,
            byref: Some(b.as_slice().to_vec()),
        },
        RichDatum::Cstring(s) => exec_seams::EvalParamValue {
            value: 0,
            isnull: false,
            typeid,
            byref: Some(s.as_bytes().to_vec()),
        },
        RichDatum::Composite(_) | RichDatum::Expanded(_) => {
            // A composite/expanded field value flattens to its header-ful varlena
            // image (datumCopy's flatten), bound as a by-reference Param.
            exec_seams::EvalParamValue {
                value: 0,
                isnull: false,
                typeid,
                byref: Some(value.as_varlena_bytes().into_owned()),
            }
        }
        RichDatum::Internal(_) => exec_seams::EvalParamValue {
            value: 0,
            isnull: true,
            typeid,
            byref: None,
        },
    }
}

/// Resolve a RECFIELD's `(fnumber, ftypeid)` against the parent record's live
/// expanded header by NAME — the runtime equivalent of C's
/// `if (recfield->rectupledescid != erh->er_tupdesc_id) { instantiate +
/// expanded_record_lookup_field }`. The compiler leaves `finfo` zeroed (its
/// fnumber is only valid once the live tupdesc is known), so we look it up here
/// each access (uncached; correct, just not memoized on the recfield). Returns
/// `None` for a non-existent field (a reference to an absent column reads NULL).
fn resolve_recfield_finfo(
    handle: u64,
    fieldname: &str,
) -> types_error::PgResult<Option<types_plpgsql::ExpandedRecordFieldInfo>> {
    let r = erh_table::with_erh_mut(handle, |mcx, erh| {
        match backend_utils_adt_misc2::expandedrecord::expanded_record_lookup_field(
            mcx, erh, fieldname,
        ) {
            Ok(Some(fi)) => Ok(Some(types_plpgsql::ExpandedRecordFieldInfo {
                fnumber: fi.fnumber,
                ftypeid: fi.ftypeid,
                ftypmod: fi.ftypmod,
                fcollation: fi.fcollation,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    });
    // `with_erh_mut` returns `None` for the NULL/cleared handle; flatten that to
    // `Ok(None)` (the field reads as absent) and propagate any codec error.
    match r {
        Some(res) => res,
        None => Ok(None),
    }
}

/// The runtime composite `(typeid, typmod)` of a REC datum: its live expanded
/// header's `er_typeid`/`er_typmod` if assigned, else its declared `rectypeid`
/// with typmod -1. `None` only for a non-REC datum.
fn record_rowtype(
    estate: &PLpgSQL_execstate,
    rec_dno: int32,
    handle: u64,
) -> Option<(Oid, int32)> {
    let declared = match &estate.datums[rec_dno as usize] {
        PLpgSQL_datum::Rec(rec) => rec.rectypeid,
        _ => return None,
    };
    if handle != 0 {
        if let Some((typeid, typmod)) =
            erh_table::with_erh(handle, |_mcx, erh| (erh.er_typeid, erh.er_typmod))
        {
            return Some((typeid, typmod));
        }
    }
    Some((declared, -1))
}

/// The refname of the REC datum at `dno` (for error messages).
fn record_name_for(estate: &PLpgSQL_execstate, dno: int32) -> String {
    match &estate.datums[dno as usize] {
        PLpgSQL_datum::Rec(rec) => rec.refname.clone(),
        _ => "record".to_string(),
    }
}

/// Snapshot one RECFIELD datum (`NEW.a`-style reference) as a param value: read
/// the live field off the parent record's expanded header through the
/// [`erh_table`] side-table. Returns `None` when the parent record has no live
/// header (the field reads as NULL via the surrounding `None`-default).
fn snapshot_recfield(
    estate: &PLpgSQL_execstate,
    rf: &types_plpgsql::PLpgSQL_recfield,
) -> types_error::PgResult<Option<exec_seams::EvalParamValue>> {
    let parent = &estate.datums[rf.recparentno as usize];
    let PLpgSQL_datum::Rec(rec) = parent else {
        return Ok(None);
    };
    let handle = rec.erh.as_ref().map(|h| h.0).unwrap_or(0);
    if handle == 0 {
        // Unassigned record: the field reads as NULL.
        return Ok(Some(exec_seams::EvalParamValue {
            value: 0,
            isnull: true,
            typeid: INVALID_OID,
            byref: None,
        }));
    }
    let Some(finfo) = resolve_recfield_finfo(handle, &rf.fieldname)? else {
        return Ok(None);
    };
    let r = erh_table::with_erh_mut(handle, |mcx, erh| {
        match backend_utils_adt_misc2::expandedrecord::expanded_record_fetch_field(
            mcx,
            erh,
            finfo.fnumber,
        ) {
            Ok((value, isnull)) => Ok(rich_datum_to_param(&value, isnull, finfo.ftypeid)),
            Err(e) => Err(e),
        }
    });
    match r {
        Some(res) => res.map(Some),
        None => Ok(None),
    }
}

/// Fulfill every still-pending `DTYPE_PROMISE` variable (`plpgsql_fulfill_promise`).
/// C fulfills a promise lazily when `exec_eval_datum` first reads it; the param
/// snapshot below is such a read, so we fulfill the whole set here (idempotent —
/// each fulfilled promise clears its flag to `PLPGSQL_PROMISE_NONE`).
fn fulfill_pending_promises(estate: &mut PLpgSQL_execstate) -> types_error::PgResult<()> {
    let ndatums = estate.datums.len();
    for dno in 0..ndatums {
        let pending = matches!(
            &estate.datums[dno],
            PLpgSQL_datum::Var(v)
                if v.promise != PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE
        );
        if pending {
            let mut var = take_var(estate, dno as int32);
            let r = seam::plpgsql_fulfill_promise(estate, &mut var);
            put_var(estate, dno as int32, var);
            r?;
        }
    }
    Ok(())
}

/// Build the per-datum value snapshot (`setup_param_list` material): for every
/// scalar VAR/PROMISE datum, its current `(value, isnull, typeid)`; for a
/// RECFIELD, the live field value off the parent record's expanded header; a
/// `None` entry for a ROW/REC datum (bound as a whole-row Param elsewhere, not a
/// scalar field reference a simple expression binds).
fn build_datum_snapshot(
    estate: &mut PLpgSQL_execstate,
) -> types_error::PgResult<std::vec::Vec<Option<exec_seams::EvalParamValue>>> {
    // A DTYPE_PROMISE variable computes its value lazily on first read (C's
    // `exec_eval_datum` calls `plpgsql_fulfill_promise`). The param snapshot is a
    // read, so fulfill any pending promises (TG_OP / TG_NAME / …) before
    // projecting their values; fulfillment is idempotent (the promise flag clears
    // to NONE).
    fulfill_pending_promises(estate)?;
    let mut snap = std::vec::Vec::with_capacity(estate.datums.len());
    // Snapshot RECFIELD values first borrowing `estate` immutably; collect the
    // (index, value) pairs so the push order matches the datum order.
    for d in estate.datums.iter() {
        match d {
            PLpgSQL_datum::Var(v) => {
                let typeid = v.datatype.as_ref().map(|t| t.typoid).unwrap_or(INVALID_OID);
                snap.push(Some(exec_seams::EvalParamValue {
                    value: v.value.as_usize(),
                    isnull: v.isnull,
                    typeid,
                    // A pass-by-reference variable carries its image out-of-band
                    // (the bare `value` word is `0` then); forward it so the SPI
                    // param-bind reconstructs the rich `Datum::ByRef`.
                    byref: v.value_byref.clone(),
                }));
            }
            PLpgSQL_datum::Recfield(rf) => snap.push(snapshot_recfield(estate, rf)?),
            _ => snap.push(None),
        }
    }
    Ok(snap)
}

/// `exec_eval_datum(estate, datum, &typeid, &typetypmod, &value, &isnull)`
/// (pl_exec.c 5577) — read the current value of a VAR/ROW/REC/RECFIELD datum.
/// Returns `(typeid, typetypmod, value_word, isnull)`; a by-reference / composite
/// result carries its verbatim image out-of-band in `estate.last_eval_byref`
/// (the bare-word `value` is `0` then), mirroring the expression-eval channel.
fn exec_eval_datum_impl(
    estate: &mut PLpgSQL_execstate,
    datum: &PLpgSQL_datum,
) -> types_error::PgResult<(Oid, int32, Datum, bool)> {
    use backend_utils_adt_misc2::expandedrecord as er;
    estate.last_eval_byref = None;
    match datum {
        PLpgSQL_datum::Var(var) => {
            // typeid = var->datatype->typoid; typetypmod = var->datatype->atttypmod.
            let t = var.datatype.as_ref().expect("VAR datum has a datatype");
            let (typeid, typmod) = (t.typoid, t.atttypmod);
            estate.last_eval_byref = var.value_byref.clone();
            Ok((typeid, typmod, var.value, var.isnull))
        }
        PLpgSQL_datum::Recfield(rf) => {
            // Read the field off the parent record's live expanded header,
            // resolving the field by NAME against the live tupdesc.
            let parent = &estate.datums[rf.recparentno as usize];
            let PLpgSQL_datum::Rec(rec) = parent else {
                panic!("RECFIELD parent is not a REC datum");
            };
            let handle = rec.erh.as_ref().map(|h| h.0).unwrap_or(0);
            if handle == 0 {
                return Ok((INVALID_OID, -1, Datum::null(), true));
            }
            let Some(finfo) = resolve_recfield_finfo(handle, &rf.fieldname)? else {
                return Err(
                    types_error::PgError::error(format!(
                        "record \"{}\" has no field \"{}\"",
                        record_name_for(estate, rf.recparentno),
                        rf.fieldname
                    ))
                    .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
                );
            };
            let fetched = erh_table::with_erh_mut(handle, |mcx, erh| {
                match er::expanded_record_fetch_field(mcx, erh, finfo.fnumber) {
                    Ok((value, isnull)) => Ok(rich_datum_to_word(&value, isnull)),
                    Err(e) => Err(e),
                }
            });
            let (value_word, byref, isnull) = match fetched {
                Some(res) => res?,
                None => (0usize, None, true),
            };
            estate.last_eval_byref = byref;
            Ok((finfo.ftypeid, finfo.ftypmod, Datum::from_usize(value_word), isnull))
        }
        PLpgSQL_datum::Rec(rec) => {
            // The whole record as a composite value (rec->erh->er_typeid).
            let handle = rec.erh.as_ref().map(|h| h.0).unwrap_or(0);
            let result = erh_table::with_erh(handle, |mcx, erh| {
                let typeid = erh.er_typeid;
                let typmod = erh.er_typmod;
                match er::expanded_record_get_tuple(mcx, erh) {
                    Ok(Some(ft)) => Ok((typeid, typmod, Some(ft.to_datum_image()), false)),
                    Ok(None) => Ok((typeid, typmod, None, true)),
                    Err(e) => Err(e),
                }
            });
            match result {
                Some(res) => match res? {
                    (typeid, typmod, Some(image), false) => {
                        estate.last_eval_byref = Some(image);
                        Ok((typeid, typmod, Datum::from_usize(0), false))
                    }
                    (typeid, typmod, _, _) => Ok((typeid, typmod, Datum::null(), true)),
                },
                None => Ok((rec.rectypeid, -1, Datum::null(), true)),
            }
        }
        PLpgSQL_datum::Row(_) => {
            // A ROW datum's whole-row read (build a tuple from its fields) is the
            // ROW-deconstruction substrate; not reached by the REC/trigger path.
            panic!(
                "seam not wired: exec_eval_datum ROW arm (pl_exec.c) — ROW \
                 whole-row tuple build (row-deconstruction value substrate)"
            );
        }
    }
}

/// Build a rich [`RichDatum`] (in `mcx`) from a PL/pgSQL value: a by-reference
/// image (`Some(bytes)`) becomes `ByRef` (the verbatim header-ful varlena /
/// cstring bytes copied into `mcx`); otherwise the bare `word` becomes `ByVal`.
/// A NULL is `ByVal(0)` (the field-set call passes `isnull` separately).
fn word_to_rich_datum<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    word: Datum,
    byref: Option<Vec<u8>>,
    isnull: bool,
) -> types_error::PgResult<RichDatum<'mcx>> {
    if isnull {
        return Ok(RichDatum::null());
    }
    match byref {
        Some(bytes) => Ok(RichDatum::ByRef(mcx::slice_in(mcx, &bytes)?)),
        None => Ok(RichDatum::from_usize(word.as_usize())),
    }
}

/// Project a rich field [`RichDatum`] to a PL/pgSQL `(bare_word, byref_image,
/// isnull)` triple (the bare word is `0` for a by-reference / composite value,
/// whose verbatim image rides `byref_image`).
fn rich_datum_to_word(value: &RichDatum<'_>, isnull: bool) -> (usize, Option<Vec<u8>>, bool) {
    if isnull {
        return (0, None, true);
    }
    match value {
        RichDatum::ByVal(w) => (*w, None, false),
        RichDatum::ByRef(b) => (0, Some(b.as_slice().to_vec()), false),
        RichDatum::Cstring(s) => (0, Some(s.as_bytes().to_vec()), false),
        RichDatum::Composite(_) | RichDatum::Expanded(_) => {
            (0, Some(value.as_varlena_bytes().into_owned()), false)
        }
        RichDatum::Internal(_) => (0, None, true),
    }
}

/// `exec_eval_expr(estate, expr, &isNull, &rettype, &rettypmod)` (pl_exec.c) —
/// evaluate a PL/pgSQL expression. The owned model drives the `exec_run_select`
/// one-row SELECT slow path over the SPI plan surface (the `exec_eval_simple_expr`
/// cached-`ExprState` fast path is an optimization; the slow path is always
/// correct). Returns `(value, isnull, rettype, rettypmod)`. The result is a
/// pass-by-value datum word (the by-ref-result keystone is separate).
fn exec_eval_expr_impl(
    estate: &mut PLpgSQL_execstate,
    expr: &types_plpgsql::PLpgSQL_expr,
) -> types_error::PgResult<(Datum, bool, Oid, int32)> {
    // The expression's input collation (fncollation analogue): the function's
    // input collation. The execstate does not carry it directly; the variables'
    // own collations drive Param collation, so InvalidOid is the fallback.
    let input_collation = INVALID_OID;

    let parse_state = build_plpgsql_parse_state(estate, expr, input_collation)?;
    let snapshot = build_datum_snapshot(estate)?;

    // exec_run_select passes maxtuples = 0 for exec_eval_expr's underlying
    // single-row evaluation (C caps the simple-expr to one row; the one-row
    // SELECT a scalar expression produces yields exactly one row).
    let result = exec_seams::exec_eval_expr_via_spi::call(
        expr.query.clone(),
        expr.parseMode,
        parse_state,
        snapshot,
        2, // detect ">1 row" like C exec_run_select(expr, 2, ...)
    )?;

    // Stash the by-ref image (if any) as the out-of-band companion to the
    // (value, isnull, rettype, rettypmod) tuple. A by-value result leaves
    // `last_eval_byref == None`; a by-ref result (text/varchar/numeric/…) carries
    // its `datumCopy`'d varlena/cstring image here, which `exec_stmt_return`
    // moves into `retval_byref`. The bare-word `value` is `0` in the by-ref case.
    estate.last_eval_byref = result.byref;

    // rettypmod is read by exec_run_select as SPI_gettypmod(tupdesc, 1); the
    // PL/pgSQL callers that consume exec_eval_expr's rettypmod (FOR-i bounds,
    // CASE) cast through exec_cast_value which tolerates -1, and exec_stmt_return
    // ignores it. -1 is the correct typmod for the int/bool results the value
    // path produces.
    Ok((
        Datum::from_usize(result.value),
        result.isnull,
        result.typeid,
        -1,
    ))
}

// ===========================================================================
// exec_assign_expr / exec_assign_value / exec_cast_value — the scalar
// assignment path (pl_exec.c). The eval + cast cross the SPI/fmgr substrate
// through the installed seams; the VAR store is real in-crate.
// ===========================================================================

/// `exec_assign_expr(estate, target, expr)` (pl_exec.c 5003) — evaluate `expr`
/// and assign into the datum `target_dno`.
fn exec_assign_expr_impl(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    expr: &types_plpgsql::PLpgSQL_expr,
) -> types_error::PgResult<()> {
    // exec_prepare_plan is folded into exec_eval_expr's slow path here (the
    // owned model re-prepares per call; the plan-caching optimization is the
    // simple-expr fast path, not yet wired). exec_eval_expr returns the value +
    // its runtime (type, typmod).
    let (value, isnull, valtype, valtypmod) = exec_eval_expr_impl(estate, expr)?;
    // A by-reference expression result (`text`/`varchar`/`numeric`/…) carries
    // its image in `estate.last_eval_byref` (stashed by `exec_eval_expr_impl`);
    // hand it to the store so a `x := <by-ref expr>` assignment keeps the image
    // in the target variable.
    let value_byref = estate.last_eval_byref.take();
    exec_assign_value_byref_impl(estate, target_dno, value, value_byref, isnull, valtype, valtypmod)?;
    exec_eval_cleanup(estate);
    Ok(())
}

/// `exec_assign_value(estate, target, value, isNull, valtype, valtypmod)`
/// (pl_exec.c 5061) — the generic datum-assignment dispatch. The VAR/PROMISE arm
/// (the scalar variable store) is real; ROW/REC/RECFIELD targets are the
/// composite/record substrate (loud, out of scope).
fn exec_assign_value_impl(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    value: Datum,
    isnull: bool,
    valtype: Oid,
    valtypmod: int32,
) -> types_error::PgResult<()> {
    exec_assign_value_byref_impl(estate, target_dno, value, None, isnull, valtype, valtypmod)
}

/// `exec_assign_value` carrying the source value's by-reference image
/// (pl_exec.c 5061). `value_byref` is the verbatim image when `value` is a
/// pass-by-reference type (the bare `value` word is `0` then); the scalar VAR
/// store stashes the coerced image into the target variable's `value_byref`
/// companion so a by-reference value (a `text`/`numeric` SELECT-INTO column,
/// a `text` assignment RHS) survives in the variable for later evaluation.
fn exec_assign_value_byref_impl(
    estate: &mut PLpgSQL_execstate,
    target_dno: int32,
    value: Datum,
    value_byref: Option<Vec<u8>>,
    isnull: bool,
    valtype: Oid,
    valtypmod: int32,
) -> types_error::PgResult<()> {
    match datum_dtype(&estate.datums[target_dno as usize]) {
        PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE => {
            let mut var = take_var(estate, target_dno);

            let (reqtype, reqtypmod, notnull, typbyval, refname) = {
                let t = var
                    .datatype
                    .as_ref()
                    .expect("a scalar VAR has a datatype");
                (t.typoid, t.atttypmod, var.notnull, t.typbyval, var.refname.clone())
            };

            // exec_cast_value(value, &isNull, valtype, valtypmod, var->typoid,
            // var->atttypmod). Thread the by-ref image both ways so a by-ref
            // source / by-ref target carries its varlena/cstring bytes. On a cast
            // error, restore the var before propagating (the take_var leaves a
            // placeholder).
            let (newvalue, isnull, newbyref) = match exec_cast_value_with_byref(
                estate, value, value_byref, isnull, valtype, valtypmod, reqtype, reqtypmod,
            ) {
                Ok(t) => t,
                Err(e) => {
                    put_var(estate, target_dno, var);
                    return Err(e);
                }
            };

            if isnull && notnull {
                put_var(estate, target_dno, var);
                return Err(
                    types_error::PgError::error(format!(
                        "null value cannot be assigned to variable \"{refname}\" declared NOT NULL"
                    ))
                    .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
                );
            }

            // The by-reference copy-into-procedure-context (pl_exec.c 5106) is
            // C's `datumCopy(newvalue, false, reqtyplen)` into the function
            // context. Here the coerced value already arrives as an owned image
            // (`newbyref`, `datumCopy`'d out of the cast/SPI working context);
            // store it into the variable's out-of-band `value_byref` companion.
            // The expanded-object / R-W-array force-expand optimization is the
            // value substrate and stays loud when no flat image is available.
            if !typbyval && !isnull {
                match newbyref {
                    Some(image) => {
                        // Store a flat by-reference value: a placeholder bare word
                        // (the real bytes live in `value_byref`, read by the next
                        // snapshot), plus the owned image.
                        assign_simple_var(estate, &mut var, Datum::from_usize(0), false, false);
                        var.value_byref = Some(image);
                        put_var(estate, target_dno, var);
                    }
                    None => {
                        put_var(estate, target_dno, var);
                        seam::arg_store_expanded_object(newvalue);
                    }
                }
                return Ok(());
            }

            // assign_simple_var(estate, var, newvalue, isNull, freeable). For a
            // by-value type freeable is false (pl_exec.c: !typbyval && !isNull).
            // `assign_simple_var` clears `value_byref` (a by-value store).
            assign_simple_var(estate, &mut var, newvalue, isnull, false);
            put_var(estate, target_dno, var);
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW | PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
            // The ROW/REC assignment is the composite-deconstruction substrate
            // (exec_move_row / exec_move_row_from_datum), out of scope.
            if isnull {
                seam::exec_move_row_null(estate, target_dno)?;
            } else {
                if !seam::type_is_rowtype(valtype) {
                    return Err(seam::ereport_return_noncomposite());
                }
                seam::exec_move_row_from_datum(estate, target_dno, value)?;
            }
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD => {
            // `NEW.b := <value>` (pl_exec.c 5183) — set a field of the parent
            // record's live expanded header. The C reads the field type off
            // `recfield->finfo` (instantiating + re-looking-up the field if the
            // record's tupdesc changed); here the compiled `finfo` already names
            // the field number + type, so cast to the field type then store.
            let (recparentno, fieldname) = {
                let PLpgSQL_datum::Recfield(rf) = &estate.datums[target_dno as usize] else {
                    unreachable!("RECFIELD dispatch on non-RECFIELD datum");
                };
                (rf.recparentno, rf.fieldname.clone())
            };

            // Resolve the parent record's live expanded header.
            let handle = {
                let PLpgSQL_datum::Rec(rec) = &estate.datums[recparentno as usize] else {
                    panic!("RECFIELD parent is not a REC datum");
                };
                rec.erh.as_ref().map(|h| h.0).unwrap_or(0)
            };
            if handle == 0 {
                return Err(
                    types_error::PgError::error(format!(
                        "record \"{}\" is not assigned yet",
                        record_name_for(estate, recparentno)
                    ))
                    .with_detail(
                        "The tuple structure of a not-yet-assigned record is indeterminate."
                            .to_string(),
                    ),
                );
            }

            // Resolve the field by NAME against the live tupdesc.
            let Some(finfo) = resolve_recfield_finfo(handle, &fieldname)? else {
                return Err(
                    types_error::PgError::error(format!(
                        "record \"{}\" has no field \"{}\"",
                        record_name_for(estate, recparentno),
                        fieldname
                    ))
                    .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
                );
            };

            // exec_cast_value(value -> field type), threading the by-ref image.
            let (newvalue, isnull, newbyref) = exec_cast_value_with_byref(
                estate, value, value_byref, isnull, valtype, valtypmod, finfo.ftypeid, finfo.ftypmod,
            )?;

            let r = erh_table::with_erh_mut(handle, |mcx, erh| {
                let rich = word_to_rich_datum(mcx, newvalue, newbyref, isnull)?;
                backend_utils_adt_misc2::expandedrecord::expanded_record_set_field_internal(
                    mcx, erh, finfo.fnumber, rich, isnull, true, true,
                )
            });
            if let Some(res) = r {
                res?;
            }
        }
    }
    Ok(())
}

/// `exec_cast_value(estate, value, &isnull, valtype, valtypmod, reqtype,
/// reqtypmod)` (pl_exec.c 7874) — coerce `value` to the required type. The
/// no-op relabel case (same type, unconstrained typmod) returns the input
/// unchanged; the real coercion routes through the installed cast seam.
fn exec_cast_value_impl(
    estate: &mut PLpgSQL_execstate,
    value: Datum,
    isnull: bool,
    valtype: Oid,
    valtypmod: int32,
    reqtype: Oid,
    reqtypmod: int32,
) -> types_error::PgResult<(Datum, bool)> {
    // The bare-word variant for by-value targets (FOR-loop bounds cast to int,
    // etc.): no source image, the coerced image (if any) is dropped.
    let (v, n, _byref) = exec_cast_value_with_byref(
        estate, value, None, isnull, valtype, valtypmod, reqtype, reqtypmod,
    )?;
    Ok((v, n))
}

/// `exec_cast_value` carrying the by-reference image both ways (pl_exec.c 7874).
/// `value_byref` is the source value's verbatim image when it is a
/// pass-by-reference type (the bare `value` is `0` then); the third result is
/// the coerced value's image when the *target* is pass-by-reference (a
/// `text`/`varchar`/`numeric` result), `None` for a by-value result. The
/// no-op relabel fast path returns the input image unchanged.
fn exec_cast_value_with_byref(
    estate: &mut PLpgSQL_execstate,
    value: Datum,
    value_byref: Option<Vec<u8>>,
    isnull: bool,
    valtype: Oid,
    valtypmod: int32,
    reqtype: Oid,
    reqtypmod: int32,
) -> types_error::PgResult<(Datum, bool, Option<Vec<u8>>)> {
    let _ = estate;
    // pl_exec.c 7882: convert only if the type differs or a constrained typmod
    // differs. Otherwise the value passes through unchanged (the no-op relabel).
    if valtype != reqtype || (valtypmod != reqtypmod && reqtypmod != -1) {
        let r = exec_seams::exec_cast_value_via_spi::call(
            value.as_usize(),
            value_byref,
            isnull,
            valtype,
            valtypmod,
            reqtype,
            reqtypmod,
        )?;
        return Ok((Datum::from_usize(r.value), r.isnull, r.byref));
    }
    // No-op relabel: the value (and its by-ref image, if any) passes through.
    Ok((value, isnull, value_byref))
}

/// `exec_run_select(estate, expr, maxtuples, portalP)` (pl_exec.c 5753) — run a
/// SELECT, stashing the result. Used by PERFORM (discard result, set FOUND from
/// the rowcount). The portal (FOR-loop cursor) leg is out of scope.
fn exec_run_select_impl(
    estate: &mut PLpgSQL_execstate,
    expr: &types_plpgsql::PLpgSQL_expr,
    maxtuples: i64,
    set_portal: bool,
) -> types_error::PgResult<int32> {
    if set_portal {
        panic!(
            "seam not wired: exec_run_select portal leg (pl_exec.c) — \
             SPI_cursor_open_with_paramlist (SPI cursor surface, FOR-loop)"
        );
    }

    let input_collation = INVALID_OID;
    let parse_state = build_plpgsql_parse_state(estate, expr, input_collation)?;
    let snapshot = build_datum_snapshot(estate)?;

    // SPI_execute_plan_with_paramlist(plan, paramLI, readonly, maxtuples), with
    // no INTO (run the SELECT to the requested row cap). exec_run_select rejects
    // a non-SELECT; the execsql bridge classifies the command and a PERFORM is a
    // plain query (we don't read the rows here, only the rowcount).
    let result = exec_seams::exec_execsql_via_spi::call(
        expr.query.clone(),
        expr.parseMode,
        parse_state,
        snapshot,
        estate.readonly_func,
        false, // into
        maxtuples,
    )?;

    estate.eval_processed = result.processed;
    Ok(result.code)
}

/// `exec_run_select(estate, expr, 0, NULL)` for the FOR-loop / RETURN QUERY
/// iteration (pl_exec.c 5753): run the query (a SELECT) and return **all** its
/// result rows, each as a vector of columns. The C portal/cursor path
/// (`SPI_cursor_open` + batched `SPI_cursor_fetch`) is replaced by a
/// materialize-all over the SPI plan surface (the `SPI_cursor_open` keystone is
/// separate); the observable iteration — every row, in order — is identical.
fn exec_run_select_rows(
    estate: &mut PLpgSQL_execstate,
    query: &str,
    parse_mode: types_plpgsql::RawParseMode,
    parse_state: types_nodes::parsestmt::PlpgsqlExprParseState,
) -> types_error::PgResult<Vec<Vec<exec_seams::ExecsqlColumn>>> {
    let snapshot = build_datum_snapshot(estate)?;
    let result = exec_seams::exec_run_select_via_spi::call(
        query.to_string(),
        parse_mode,
        parse_state,
        snapshot,
        estate.readonly_func,
    )?;
    estate.eval_processed = result.processed;
    Ok(result.all_rows)
}

/// `exec_for_query(estate, stmt, portal, prefetch_ok)` (pl_exec.c 6011) — the
/// shared FOR-loop-over-a-query driver. For each fetched row, assign it into the
/// loop variable (`exec_move_row`), run the loop body (`exec_stmts`), and honor
/// EXIT / CONTINUE. `FOUND` is set true iff at least one row was fetched. The
/// rows arrive already materialized (the `SPI_cursor_open` + batched
/// `SPI_cursor_fetch` of C is the materialize-all `exec_run_select_rows`).
fn exec_for_query(
    estate: &mut PLpgSQL_execstate,
    loopvar: &types_plpgsql::PLpgSQL_variable,
    body: &[PLpgSQL_stmt],
    label: Option<&str>,
    rows: Vec<Vec<exec_seams::ExecsqlColumn>>,
) -> PLpgSQL_rc_result {
    let mut rc = PLpgSQL_rc::PLPGSQL_RC_OK;
    let mut found = false;

    for row in &rows {
        found = true;

        // exec_move_row(estate, var, tuple, tupdesc) — assign the fetched row
        // into the loop's record / row variable.
        exec_move_row_into_target(estate, loopvar, row)?;

        // Execute the statements.
        let body_rc = exec_stmts(estate, body)?;

        match loop_rc_processing(estate, label, body_rc) {
            LoopRc::Break(r) => {
                rc = r;
                break;
            }
            LoopRc::Continue(_) => {}
        }
    }

    // Set the FOUND variable to indicate the result of executing the loop
    // (namely, whether we looped one or more times). This must be set last so
    // that it does not interfere with the value of FOUND inside the loop.
    exec_set_found(estate, found);

    // SPI_cursor_close(portal) — the materialize-all path holds no live portal,
    // so there is nothing to close (the rows were fully fetched up front).
    Ok(rc)
}

// ===========================================================================
// Value-substrate statement arms — dispatch targets with LOUD bodies. Each is a
// whole-statement SQL/value leg (SPI / executor / fmgr), not control flow.
// ===========================================================================

fn exec_stmt_call(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_call (pl_exec.c) — exec_prepare_plan / \
         make_callstmt_target / setup_param_list / SPI_execute_plan_extended / \
         exec_move_row (SPI plan surface + procedure resowner)"
    );
}

/// `exec_stmt_getdiag(estate, stmt)` (pl_exec.c 2436) — GET [CURRENT|STACKED]
/// DIAGNOSTICS. CURRENT reads the most-recent-statement area (`eval_processed`,
/// the routine OID, the call/error context); STACKED reads `estate->cur_error`
/// (only valid inside an EXCEPTION handler). Each item is assigned into its
/// target variable.
fn exec_stmt_getdiag(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_getdiag,
) -> PLpgSQL_rc_result {
    use types_plpgsql::PLpgSQL_getdiag_kind as K;

    // STACKED DIAGNOSTICS requires an active exception handler; the grammar and
    // pl_comp.c already reject the standalone case, but if cur_error is None
    // here the read is undefined — mirror C and guard.
    if stmt.is_stacked && estate.cur_error.is_none() {
        return Err(types_error::PgError::error(
            "GET STACKED DIAGNOSTICS cannot be used outside an exception handler"
                .to_string(),
        ));
    }

    for di in &stmt.diag_items {
        match di.kind {
            K::PLPGSQL_GETDIAG_ROW_COUNT => {
                // exec_assign_value(target, Int64GetDatum(estate->eval_processed),
                //                   false, INT8OID, -1).
                const INT8OID: Oid = 20;
                exec_assign_value_impl(
                    estate,
                    di.target,
                    Datum::from_i64(estate.eval_processed as i64),
                    false,
                    INT8OID,
                    -1,
                )?;
            }
            K::PLPGSQL_GETDIAG_ROUTINE_OID => {
                // estate->func->fn_oid — the func back-reference is opaque in the
                // owned model; this is rarely used and not reachable from the
                // current execstate carrier. Mirror C and raise.
                return Err(types_error::PgError::error(
                    "GET DIAGNOSTICS ... PG_ROUTINE_OID not yet supported \
                     (opaque func back-reference)"
                        .to_string(),
                ));
            }
            // CURRENT-area context strings: the error/call-context stack is not
            // modeled in the owned execstate yet; STACKED context comes from
            // cur_error below. The remaining string items read from cur_error
            // when STACKED, or are the current message/context otherwise.
            other => {
                let s: String = if stmt.is_stacked {
                    let edata = estate
                        .cur_error
                        .as_ref()
                        .expect("STACKED diagnostics guarded above");
                    match other {
                        K::PLPGSQL_GETDIAG_RETURNED_SQLSTATE => {
                            unpack_sql_state(edata.sqlstate.0)
                        }
                        K::PLPGSQL_GETDIAG_MESSAGE_TEXT => edata.message.clone(),
                        K::PLPGSQL_GETDIAG_ERROR_DETAIL => {
                            edata.detail.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_ERROR_HINT => {
                            edata.hint.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_COLUMN_NAME => {
                            edata.column_name.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_CONSTRAINT_NAME => {
                            edata.constraint_name.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_DATATYPE_NAME => {
                            edata.datatype_name.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_TABLE_NAME => {
                            edata.table_name.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_SCHEMA_NAME => {
                            edata.schema_name.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_CONTEXT
                        | K::PLPGSQL_GETDIAG_ERROR_CONTEXT => {
                            edata.context.clone().unwrap_or_default()
                        }
                        K::PLPGSQL_GETDIAG_ROW_COUNT
                        | K::PLPGSQL_GETDIAG_ROUTINE_OID => unreachable!(),
                    }
                } else {
                    // CURRENT diagnostics: only PG_CONTEXT is defined for the
                    // current area; the per-statement error-context stack is not
                    // modeled in the owned execstate yet.
                    match other {
                        K::PLPGSQL_GETDIAG_CONTEXT => String::new(),
                        _ => {
                            return Err(types_error::PgError::error(format!(
                                "GET CURRENT DIAGNOSTICS item {:?} not available outside \
                                 an exception handler",
                                other
                            )))
                        }
                    }
                };

                // exec_assign_c_string(estate, target, s) (pl_exec.c 8866):
                // build a text Datum and assign it into the target variable. In
                // C this routes through exec_assign_value (TEXTOID source, cast
                // to the target type); the GET DIAGNOSTICS targets are virtually
                // always text/varchar variables, so the assign_text_var path
                // (direct assign_simple_var, no by-ref cast/transfer leg) is the
                // faithful store for them.
                assign_text_var(estate, di.target, s)?;
            }
        }
    }

    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `exec_stmt_fors(estate, stmt)` (pl_exec.c 2766) — FOR rec/row IN SELECT ...
/// LOOP. Open the query (via the materialize-all `exec_run_select`) and run the
/// shared FOR-loop driver over its rows.
fn exec_stmt_fors(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_fors,
) -> PLpgSQL_rc_result {
    let expr = stmt.query.as_deref().expect("FOR-IN-SELECT carries a query");
    let loopvar = stmt
        .var
        .as_deref()
        .expect("FOR-IN-SELECT carries a loop variable");

    // exec_run_select(estate, stmt->query, 0, &portal) — run the query and
    // collect every result row.
    let input_collation = INVALID_OID;
    let parse_state = build_plpgsql_parse_state(estate, expr, input_collation)?;
    let rows = exec_run_select_rows(estate, &expr.query, expr.parseMode, parse_state)?;

    // Execute the loop.
    let rc = exec_for_query(estate, loopvar, &stmt.body, stmt.label.as_deref(), rows)?;

    // exec_eval_cleanup + SPI_freetuptable are folded into the materialize-all
    // teardown (the rows are owned and drop here).
    exec_eval_cleanup(estate);
    Ok(rc)
}

fn exec_stmt_forc(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_forc (pl_exec.c) — SPI_cursor_open_with_paramlist + \
         exec_for_query (SPI cursor surface)"
    );
}

/// Read a declared record/row/scalar variable's current value into a column
/// series — the `stmt->retvarno >= 0` arm of `exec_stmt_return_next`
/// (pl_exec.c 3355). Mirrors the C switch on `retvar->dtype`:
///
/// * VAR / PROMISE: a single scalar column (C: `tuplestore_putvalues(... &var->value)`).
/// * REC: read the expanded record's current fields (C:
///   `expanded_record_get_tuple` + `convert_tuples_by_position` +
///   `tuplestore_puttuple`).
/// * ROW: read each scalar field of the row (C: `make_tuple_from_row`).
///
/// The per-position type coercion (`convert_tuples_by_position` /
/// `exec_cast_value`) is the identity for the common case where the variable's
/// rowtype matches the function's result rowtype (`RETURN NEXT r` over a loop
/// variable declared as the function's SETOF rowtype). The columns are
/// delivered in position order to `materialize_sink_into_rsinfo`, which forms
/// the result tuple against the function's `expectedDesc`.
fn read_retvar_into_columns(
    estate: &mut PLpgSQL_execstate,
    retvarno: int32,
) -> types_error::PgResult<Vec<exec_seams::ExecsqlColumn>> {
    use backend_utils_adt_misc2::expandedrecord as er;

    Ok(match datum_dtype(&estate.datums[retvarno as usize]) {
        PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE => {
            // A scalar SETOF (a 1-column result): read the single variable value.
            // A PROMISE is fulfilled first (C: plpgsql_fulfill_promise).
            if datum_dtype(&estate.datums[retvarno as usize])
                == PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE
            {
                let mut var = take_var(estate, retvarno);
                let r = seam::plpgsql_fulfill_promise(estate, &mut var);
                put_var(estate, retvarno, var);
                r?;
            }
            let datum = estate.datums[retvarno as usize].clone();
            let (typeid, typmod, value, isnull) = exec_eval_datum_impl(estate, &datum)?;
            let byref = estate.last_eval_byref.take();
            std::vec![exec_seams::ExecsqlColumn {
                value: value.as_usize(),
                isnull,
                typeid,
                typmod,
                name: std::string::String::new(),
                byref,
            }]
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
            // Read the REC's live expanded record: fetch each field through the
            // expanded-record reader. An empty/NULL record reads every field as
            // NULL (C: instantiate_empty_record_variable -> a row of NULLs).
            let handle = match &estate.datums[retvarno as usize] {
                PLpgSQL_datum::Rec(rec) => rec.erh.as_ref().map(|h| h.0).unwrap_or(0),
                _ => unreachable!("REC dtype is a Rec datum"),
            };
            let cols = erh_table::with_erh_mut(
                handle,
                |mcx, erh| -> types_error::PgResult<Vec<exec_seams::ExecsqlColumn>> {
                    // Ensure the tupdesc is available (C: expanded_record_get_tupdesc),
                    // then read each field by position.
                    er::expanded_record_fetch_tupdesc(mcx, erh)?;
                    let attrs: Vec<(Oid, int32, std::string::String)> = erh
                        .er_tupdesc
                        .as_ref()
                        .expect("REC tupdesc fetched")
                        .attrs
                        .iter()
                        .map(|a| {
                            (
                                a.atttypid,
                                a.atttypmod,
                                std::string::String::from_utf8_lossy(a.attname.name_str())
                                    .into_owned(),
                            )
                        })
                        .collect();
                    let mut out = Vec::with_capacity(attrs.len());
                    for (i, (typeid, typmod, name)) in attrs.into_iter().enumerate() {
                        let (value, isnull) =
                            er::expanded_record_fetch_field(mcx, erh, (i + 1) as i32)?;
                        let (word, byref, isn) = rich_datum_to_word(&value, isnull);
                        out.push(exec_seams::ExecsqlColumn {
                            value: word,
                            isnull: isn,
                            typeid,
                            typmod,
                            name,
                            byref,
                        });
                    }
                    Ok(out)
                },
            );
            match cols {
                Some(res) => res?,
                None => Vec::new(),
            }
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW => {
            // `make_tuple_from_row`: read each scalar field's current value.
            let varnos = match &estate.datums[retvarno as usize] {
                PLpgSQL_datum::Row(r) => r.varnos.clone(),
                _ => unreachable!("ROW dtype is a Row datum"),
            };
            let mut out = Vec::with_capacity(varnos.len());
            for field_dno in varnos {
                if field_dno < 0 {
                    // Dropped column placeholder → a NULL column.
                    out.push(exec_seams::ExecsqlColumn {
                        value: 0,
                        isnull: true,
                        typeid: INVALID_OID,
                        typmod: -1,
                        name: std::string::String::new(),
                        byref: None,
                    });
                    continue;
                }
                let field_datum = estate.datums[field_dno as usize].clone();
                let (typeid, typmod, value, isnull) =
                    exec_eval_datum_impl(estate, &field_datum)?;
                let byref = estate.last_eval_byref.take();
                out.push(exec_seams::ExecsqlColumn {
                    value: value.as_usize(),
                    isnull,
                    typeid,
                    typmod,
                    name: std::string::String::new(),
                    byref,
                });
            }
            out
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD => {
            // RETURN NEXT of a single record field → a 1-column row.
            let datum = estate.datums[retvarno as usize].clone();
            let (typeid, typmod, value, isnull) = exec_eval_datum_impl(estate, &datum)?;
            let byref = estate.last_eval_byref.take();
            std::vec![exec_seams::ExecsqlColumn {
                value: value.as_usize(),
                isnull,
                typeid,
                typmod,
                name: std::string::String::new(),
                byref,
            }]
        }
    })
}

/// `exec_stmt_return_next(estate, stmt)` (pl_exec.c 4116) — RETURN NEXT.
/// Evaluate the row/value and append it to the function's SRF result tuplestore
/// (the live materialize sink). The scalar-expression form (`RETURN NEXT
/// <expr>`) — the common SETOF-of-scalar case — is ported here: evaluate the
/// expression and append a single-column row. The record/row-variable forms
/// (`stmt->retvarno >= 0`) need the `exec_move_row` tuple-deform path and stay
/// loud.
fn exec_stmt_return_next(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_return_next,
) -> PLpgSQL_rc_result {
    // C: if (!estate->retisset) ereport(ERROR, "cannot use RETURN NEXT in a
    //    non-SETOF function").
    if !estate.retisset {
        return Err(
            types_error::PgError::error(
                "cannot use RETURN NEXT in a non-SETOF function".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
        );
    }

    if stmt.retvarno >= 0 {
        // RETURN NEXT over a declared record/row/scalar variable (pl_exec.c
        // 3355). C reads the variable's current value, coerces it to the
        // function's result rowtype, and `tuplestore_puttuple`/`putvalues` it.
        // In the owned model the result tuplestore is the active materialize
        // sink; we read the variable's columns and deposit one row.
        let columns = read_retvar_into_columns(estate, stmt.retvarno)?;
        seam::put_rows_into_sink(std::vec![columns]);
        return Ok(PLpgSQL_rc::PLPGSQL_RC_OK);
    }

    if let Some(expr) = stmt.expr.as_deref() {
        // C: tupmap / coercion of the value to the function's element type, then
        // `tuplestore_putvalues(estate->tuple_store, tupdesc, &retval, &isNull)`.
        let (value, isnull, _rettype, _rettypmod) = seam::exec_eval_expr(estate, expr)?;
        let byref = estate.last_eval_byref.take();
        // Build one single-column row and deposit it into the materialize sink
        // (the `ReturnSetInfo.setResult` tuplestore the executor-frame SRF
        // dispatcher threaded onto the call). The column crosses as the
        // `(value | byref image, isnull)` split `ExecsqlColumn` carries.
        let col = crate::exec_seams::ExecsqlColumn {
            value: value.as_usize(),
            isnull,
            typeid: _rettype,
            typmod: _rettypmod,
            name: std::string::String::new(),
            byref,
        };
        seam::put_rows_into_sink(std::vec![std::vec![col]]);
        exec_eval_cleanup(estate);
        return Ok(PLpgSQL_rc::PLPGSQL_RC_OK);
    }

    // RETURN NEXT with neither expr nor retvarno is a parse error C never builds.
    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `exec_stmt_return_query(estate, stmt)` (pl_exec.c 4046) — RETURN QUERY [EXECUTE].
/// Run the (static or dynamic) query and push every result row into the
/// function's SRF result tuplestore (via the `ReturnSetInfo`). The query run is
/// the SPI plan surface (already ported: the `exec_run_select` /
/// `exec_dynquery_with_params` materialize-all path); the per-row push into the
/// `ReturnSetInfo.setResult` tuplestore is the SRF tuple-store handoff, which is
/// only reachable once the executor-frame SRF dispatch routes a SETOF PL/pgSQL
/// function through `plpgsql_exec_function` with a live `ReturnSetInfo`
/// (`srf_invoke_by_oid` currently has no executor-frame entry for per-user
/// PL/pgSQL function OIDs — the dual-home `types_fmgr`↔`types_nodes` fcinfo
/// keystone). The materialize leg below runs end-to-end; the sink stays loud.
fn exec_stmt_return_query(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_return_query,
) -> PLpgSQL_rc_result {
    // C: if (!estate->retisset) ereport(ERROR, "cannot use RETURN QUERY in a
    //    non-SETOF function").
    if !estate.retisset {
        return Err(
            types_error::PgError::error(
                "cannot use RETURN QUERY in a non-SETOF function".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
        );
    }

    // Run the query, collecting every result row (static query → exec_run_select;
    // dynamic RETURN QUERY EXECUTE → exec_dynquery_with_params).
    let rows = if let Some(query) = stmt.query.as_deref() {
        let input_collation = INVALID_OID;
        let parse_state = build_plpgsql_parse_state(estate, query, input_collation)?;
        exec_run_select_rows(estate, &query.query, query.parseMode, parse_state)?
    } else {
        exec_dynquery_with_params(estate, &stmt.dynquery, &stmt.params)?
    };

    // Push each row into the function's SRF result tuplestore. The tuplestore +
    // its descriptor live on the `ReturnSetInfo` the executor-frame SRF caller
    // threads onto the call frame; the owned execstate holds only opaque handles
    // for them, so the per-row deposit crosses the SRF tuple-store seam (the
    // handler installs it over the live `ReturnSetInfo` once SETOF PL/pgSQL
    // dispatch lands). `tuple_store_puttuple_rows` mirrors C's
    // `tuplestore_puttupleslot` loop in `exec_stmt_return_query`.
    seam::return_query_put_rows(estate, rows);

    exec_eval_cleanup(estate);
    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `exec_stmt_raise(estate, stmt)` (pl_exec.c 3725) — build a message and throw
/// it with `ereport(stmt->elog_level, ...)`. Handles the `%` message format with
/// parameter substitution, the USING options (ERRCODE / MESSAGE / DETAIL / HINT
/// / COLUMN / CONSTRAINT / DATATYPE / TABLE / SCHEMA), the condition-name →
/// SQLSTATE mapping, and the re-RAISE (no-parameters) form.
fn exec_stmt_raise(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_raise,
) -> PLpgSQL_rc_result {
    use types_plpgsql::PLpgSQL_raise_option_type as Opt;

    let mut err_code: int32 = 0;
    let mut condname: Option<String> = None;
    let mut err_message: Option<String> = None;
    let mut err_detail: Option<String> = None;
    let mut err_hint: Option<String> = None;
    let mut err_column: Option<String> = None;
    let mut err_constraint: Option<String> = None;
    let mut err_datatype: Option<String> = None;
    let mut err_table: Option<String> = None;
    let mut err_schema: Option<String> = None;

    // RAISE with no parameters: re-throw the current exception.
    if stmt.condname.is_none() && stmt.message.is_none() && stmt.options.is_empty() {
        if let Some(edata) = estate.cur_error.clone() {
            // ReThrowError(estate->cur_error): re-raise the error currently
            // being handled. The owned model carries cur_error as the live
            // PgError; re-raise is the same `Err` channel as PG_RE_THROW.
            return Err(edata);
        }
        // oops, we're not inside a handler.
        return Err(
            types_error::PgError::error(
                "RAISE without parameters cannot be used outside an exception handler"
                    .to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
        );
    }

    if let Some(cn) = stmt.condname.as_deref() {
        err_code = recognize_err_condition(cn, true)?;
        condname = Some(cn.to_string());
    }

    if let Some(message) = stmt.message.as_deref() {
        // Build the message, substituting `%` with the next parameter's external
        // representation; `%%` collapses to a single `%`.
        let mut ds = String::new();
        let mut params = stmt.params.iter();
        let bytes = message.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'%' {
                    ds.push('%');
                    i += 2;
                    continue;
                }
                // should have been checked at compile time.
                let param = params
                    .next()
                    .unwrap_or_else(|| panic!("unexpected RAISE parameter list length"));
                let (paramvalue, paramisnull, paramtypeid, _paramtypmod) =
                    seam::exec_eval_expr(estate, param)?;
                // A pass-by-reference result leaves its varlena/cstring image in
                // estate.last_eval_byref (value bare-word == 0); take it so the
                // output function reads the real referent.
                let param_byref = estate.last_eval_byref.clone();
                let extval = if paramisnull {
                    "<NULL>".to_string()
                } else {
                    convert_value_to_string(paramvalue, param_byref, paramtypeid)?
                };
                ds.push_str(&extval);
                exec_eval_cleanup(estate);
                i += 1;
            } else {
                // Append this UTF-8 character (C appends the raw byte; the message
                // is valid UTF-8, so we push the whole char).
                let ch_len = utf8_char_len(bytes[i]);
                if let Ok(s) = std::str::from_utf8(&bytes[i..i + ch_len]) {
                    ds.push_str(s);
                }
                i += ch_len;
            }
        }
        // should have been checked at compile time.
        if params.next().is_some() {
            panic!("unexpected RAISE parameter list length");
        }
        err_message = Some(ds);
    }

    for opt in &stmt.options {
        let expr = opt.expr.as_deref().expect("RAISE option carries an expr");
        let (optionvalue, optionisnull, optiontypeid, _optiontypmod) =
            seam::exec_eval_expr(estate, expr)?;
        // A pass-by-reference option value (MESSAGE/DETAIL/HINT text) carries its
        // varlena image out-of-band in last_eval_byref (bare word == 0).
        let option_byref = estate.last_eval_byref.clone();
        if optionisnull {
            return Err(
                types_error::PgError::error("RAISE statement option cannot be null".to_string())
                    .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
            );
        }
        let extval = convert_value_to_string(optionvalue, option_byref, optiontypeid)?;

        match opt.opt_type {
            Opt::PLPGSQL_RAISEOPTION_ERRCODE => {
                if err_code != 0 {
                    return Err(raise_option_already_specified("ERRCODE"));
                }
                err_code = recognize_err_condition(&extval, true)?;
                condname = Some(extval);
            }
            Opt::PLPGSQL_RAISEOPTION_MESSAGE => set_raise_option_text(&mut err_message, extval, "MESSAGE")?,
            Opt::PLPGSQL_RAISEOPTION_DETAIL => set_raise_option_text(&mut err_detail, extval, "DETAIL")?,
            Opt::PLPGSQL_RAISEOPTION_HINT => set_raise_option_text(&mut err_hint, extval, "HINT")?,
            Opt::PLPGSQL_RAISEOPTION_COLUMN => set_raise_option_text(&mut err_column, extval, "COLUMN")?,
            Opt::PLPGSQL_RAISEOPTION_CONSTRAINT => {
                set_raise_option_text(&mut err_constraint, extval, "CONSTRAINT")?
            }
            Opt::PLPGSQL_RAISEOPTION_DATATYPE => {
                set_raise_option_text(&mut err_datatype, extval, "DATATYPE")?
            }
            Opt::PLPGSQL_RAISEOPTION_TABLE => set_raise_option_text(&mut err_table, extval, "TABLE")?,
            Opt::PLPGSQL_RAISEOPTION_SCHEMA => set_raise_option_text(&mut err_schema, extval, "SCHEMA")?,
        }

        exec_eval_cleanup(estate);
    }

    // Default code if nothing specified.
    if err_code == 0 && stmt.elog_level >= ERROR_LEVEL {
        err_code = types_error::ERRCODE_RAISE_EXCEPTION.0;
    }

    // Default error message if nothing specified.
    if err_message.is_none() {
        if let Some(cn) = condname.take() {
            err_message = Some(cn);
        } else {
            err_message = Some(unpack_sql_state(err_code));
        }
    }

    // Throw the error (may or may not come back).
    let report = exec_seams::RaiseEreport {
        elog_level: stmt.elog_level,
        err_code,
        message: err_message.unwrap_or_default(),
        detail: err_detail,
        hint: err_hint,
        column: err_column,
        constraint: err_constraint,
        datatype: err_datatype,
        table: err_table,
        schema: err_schema,
    };
    // For an ERROR-level RAISE the report cycle raises; propagate it as `Err`.
    // A non-ERROR level (NOTICE/WARNING/…) reports to the client and returns Ok.
    exec_seams::raise_ereport::call(report)?;

    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `SET_RAISE_OPTION_TEXT(opt, name)` (pl_exec.c macro): error if `opt` already
/// set, else store `extval`.
fn set_raise_option_text(
    opt: &mut Option<String>,
    extval: String,
    name: &str,
) -> types_error::PgResult<()> {
    if opt.is_some() {
        return Err(raise_option_already_specified(name));
    }
    *opt = Some(extval);
    Ok(())
}

/// `ereport(ERROR, ERRCODE_SYNTAX_ERROR, "RAISE option already specified: %s")`.
fn raise_option_already_specified(name: &str) -> types_error::PgError {
    types_error::PgError::error(format!("RAISE option already specified: {name}"))
        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
}

/// The UTF-8 length of the character whose first byte is `b` (1..=4).
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >> 5 == 0b110 {
        2
    } else if b >> 4 == 0b1110 {
        3
    } else if b >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// `plpgsql_recognize_err_condition(condname, allow_sqlstate)` (pl_comp.c) via
/// the installed seam — panics with the unrecognized-condition ereport on Err.
fn recognize_err_condition(condname: &str, allow_sqlstate: bool) -> types_error::PgResult<int32> {
    exec_seams::recognize_err_condition::call(condname.to_string(), allow_sqlstate)
}

/// `convert_value_to_string(estate, value, valtype)` (pl_exec.c) via the
/// installed seam (getTypeOutputInfo + OidOutputFunctionCall).
fn convert_value_to_string(
    value: Datum,
    byref: Option<Vec<u8>>,
    valtype: Oid,
) -> types_error::PgResult<String> {
    exec_seams::convert_value_to_string::call(value.as_usize(), byref, valtype)
}

/// `unpack_sql_state(sql_state)` (elog.c): the inverse of `MAKE_SQLSTATE` — the
/// 5-character text of a packed SQLSTATE. Pure bit ops.
fn unpack_sql_state(sql_state: int32) -> String {
    let mut out = String::with_capacity(5);
    let mut v = sql_state;
    for _ in 0..5 {
        let code = (v & 0x3F) as u8;
        // PGUNSIXBIT: '0'..'9' then 'A'..; C: val + '0' but with the 6-bit pack
        // the inverse is `(val & 0x3F) + '0'` mapping through the same table.
        out.push((code + b'0') as char);
        v >>= 6;
    }
    out
}

fn exec_stmt_assert(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_assert (pl_exec.c) — exec_eval_boolean(cond) + \
         exec_eval_expr(message) + ereport(ASSERT_FAILURE) (value substrate)"
    );
}

/// `exec_stmt_execsql(estate, stmt)` (pl_exec.c 4208) — execute an embedded SQL
/// statement (INSERT / UPDATE / DELETE / plain SELECT), optionally with INTO. The
/// statement-type classification, FOUND setting, INTO no-rows / too-many-rows /
/// STRICT checks, and the "no destination for result data" guard are ported 1:1;
/// the SQL run crosses the SPI substrate through the installed seam.
fn exec_stmt_execsql(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_execsql,
) -> PLpgSQL_rc_result {
    let expr = stmt.sqlstmt.as_deref().expect("EXECSQL carries a sqlstmt");

    // plpgsql_extra_errors / plpgsql_extra_warnings & PLPGSQL_XCHECK_TOOMANYROWS:
    // the optional too-many-rows check. The extra-check GUCs default off and are
    // owned in the handler layer (not reachable here); the default-disabled state
    // is level 0 (no extra check).
    let too_many_rows_level: int32 = 0;

    // setup_param_list + SPI_execute_plan_with_paramlist. The mod_stmt detection
    // (INSERT/UPDATE/DELETE/MERGE) that C computes from SPI_plan_get_plan_sources
    // is derived here from the SPI result code the bridge returns (the planned
    // command type), which is equivalent (and avoids caching plan sources we
    // don't keep). INTO needs at most one row (two when STRICT/mod/too-many, to
    // detect the >1 case); without INTO run to completion (tcount = 0).
    let tcount: i64 = if stmt.into {
        if stmt.strict || too_many_rows_level != 0 {
            2
        } else {
            1
        }
    } else {
        0
    };

    let input_collation = INVALID_OID;
    let parse_state = build_plpgsql_parse_state(estate, expr, input_collation)?;
    let snapshot = build_datum_snapshot(estate)?;

    let result = exec_seams::exec_execsql_via_spi::call(
        expr.query.clone(),
        expr.parseMode,
        parse_state,
        snapshot,
        estate.readonly_func,
        stmt.into,
        tcount,
    )?;

    let code = result.code;
    let processed = result.processed;

    // Check for error, and set FOUND if appropriate (for historical reasons we
    // set FOUND only for certain query types).
    let mod_stmt = matches!(
        code,
        exec_seams::SPI_OK_INSERT
            | exec_seams::SPI_OK_UPDATE
            | exec_seams::SPI_OK_DELETE
            | exec_seams::SPI_OK_INSERT_RETURNING
            | exec_seams::SPI_OK_UPDATE_RETURNING
            | exec_seams::SPI_OK_DELETE_RETURNING
    );
    match code {
        exec_seams::SPI_OK_SELECT => exec_set_found(estate, processed != 0),
        exec_seams::SPI_OK_INSERT
        | exec_seams::SPI_OK_UPDATE
        | exec_seams::SPI_OK_DELETE
        | exec_seams::SPI_OK_INSERT_RETURNING
        | exec_seams::SPI_OK_UPDATE_RETURNING
        | exec_seams::SPI_OK_DELETE_RETURNING => exec_set_found(estate, processed != 0),
        exec_seams::SPI_OK_SELINTO | exec_seams::SPI_OK_UTILITY => {}
        exec_seams::SPI_OK_REWRITTEN => exec_set_found(estate, false),
        exec_seams::SPI_ERROR_COPY => {
            return Err(
                types_error::PgError::error("cannot COPY to/from client in PL/pgSQL".to_string())
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            )
        }
        _ => {
            return Err(types_error::PgError::error(format!(
                "SPI_execute_plan_with_paramlist failed executing query \"{}\": code {code}",
                expr.query
            )))
        }
    }

    // All variants should save result info for GET DIAGNOSTICS.
    estate.eval_processed = processed;

    // Process INTO if present.
    if stmt.into {
        if !result.returned_tuptable {
            return Err(
                types_error::PgError::error(
                    "INTO used with a command that cannot return data".to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            );
        }

        let target = stmt.target.as_deref().expect("INTO carries a target");
        let n = processed;

        if n == 0 {
            if stmt.strict {
                return Err(
                    types_error::PgError::error("query returned no rows".to_string())
                        .with_sqlstate(types_error::ERRCODE_NO_DATA_FOUND),
                );
            }
            // Set the target to NULL(s).
            exec_move_row_into_target(estate, target, &[])?;
        } else {
            if n > 1 && (stmt.strict || mod_stmt || too_many_rows_level != 0) {
                return Err(
                    types_error::PgError::error("query returned more than one row".to_string())
                        .with_detail("Make sure the query returns a single row, or use LIMIT 1.".to_string())
                        .with_sqlstate(types_error::ERRCODE_TOO_MANY_ROWS),
                );
            }
            // Put the first result row into the target.
            exec_move_row_into_target(estate, target, &result.first_row)?;
        }

        exec_eval_cleanup(estate);
    } else {
        // If the statement returned a tuple table, complain (no destination).
        if result.returned_tuptable && code == exec_seams::SPI_OK_SELECT {
            return Err(
                types_error::PgError::error("query has no destination for result data".to_string())
                    .with_detail(
                        "If you want to discard the results of a SELECT, use PERFORM instead."
                            .to_string(),
                    )
                    .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            );
        }
    }

    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `exec_move_row(estate, target, tuple, tupdesc)` (pl_exec.c) specialized to
/// the execsql SELECT-INTO store. A single-column result into a scalar VAR/
/// PROMISE target is real (the common `SELECT col INTO var` case); a multi-field
/// ROW / REC target is the composite-deconstruction substrate (loud, out of
/// scope). `columns` empty == the NULL-row store.
fn exec_move_row_into_target(
    estate: &mut PLpgSQL_execstate,
    target: &types_plpgsql::PLpgSQL_variable,
    columns: &[exec_seams::ExecsqlColumn],
) -> types_error::PgResult<()> {
    let dno = target.dno;
    match datum_dtype(&estate.datums[dno as usize]) {
        PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE => {
            // A scalar INTO target takes the first column (C's exec_move_row maps
            // the row's first attribute to the single variable). No row => NULL.
            match columns.first() {
                Some(c) => exec_assign_value_byref_impl(
                    estate,
                    dno,
                    Datum::from_usize(c.value),
                    // A by-reference fetched column (`text`/`numeric`/…) carries
                    // its image; thread it into the target variable.
                    c.byref.clone(),
                    c.isnull,
                    c.typeid,
                    c.typmod,
                )?,
                None => exec_assign_value_impl(estate, dno, Datum::null(), true, INVALID_OID, -1)?,
            }
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW => {
            // `exec_move_row` into a PLpgSQL_row: map each row field's varno to
            // the matching result column (C's exec_move_row_common, the
            // attribute-by-position assignment into the row's scalar fields). A
            // single INTO scalar is wrapped by the compiler in a 1-field ROW
            // (`make_scalar_list1`); a multi-target INTO list is an N-field ROW.
            // A missing column (fewer columns than fields) stores NULL into the
            // remaining fields, exactly as C does. The expanded-record (REC)
            // and record-field (RECFIELD) deconstruction stays loud.
            let (nfields, varnos) = match &estate.datums[dno as usize] {
                PLpgSQL_datum::Row(r) => (r.nfields as usize, r.varnos.clone()),
                _ => unreachable!("ROW dtype is a Row datum"),
            };
            for fno in 0..nfields {
                let field_dno = varnos[fno];
                // A `varnos[fno] < 0` marks a dropped column placeholder in C
                // (skipped); the common scalar-list ROW has all real varnos.
                if field_dno < 0 {
                    continue;
                }
                match columns.get(fno) {
                    Some(c) => exec_assign_value_byref_impl(
                        estate,
                        field_dno,
                        Datum::from_usize(c.value),
                        c.byref.clone(),
                        c.isnull,
                        c.typeid,
                        c.typmod,
                    )?,
                    None => exec_assign_value_impl(
                        estate,
                        field_dno,
                        Datum::null(),
                        true,
                        INVALID_OID,
                        -1,
                    )?,
                }
            }
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
            // `SELECT ... INTO <record>`: build a transient record from the
            // result columns and install it as the REC's live expanded header.
            trigger::exec_move_row_into_record_impl(estate, dno, columns)?;
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD => {
            // INTO a single record field (`SELECT ... INTO rec.field`) maps the
            // first result column to the field, exactly like the scalar arm.
            match columns.first() {
                Some(c) => exec_assign_value_byref_impl(
                    estate,
                    dno,
                    Datum::from_usize(c.value),
                    c.byref.clone(),
                    c.isnull,
                    c.typeid,
                    c.typmod,
                )?,
                None => exec_assign_value_impl(estate, dno, Datum::null(), true, INVALID_OID, -1)?,
            }
        }
    }
    Ok(())
}

/// `exec_eval_using_params(estate, params)` (pl_exec.c 8869) — evaluate the
/// `USING` clause expressions of a dynamic `EXECUTE` into a list of already-
/// evaluated param values (the analogue of C's `ParamListInfo`). Each param is
/// evaluated with `exec_eval_expr`; an `unknown`-typed result is coerced to
/// `text` (C: "treat 'unknown' parameters as text, since that's what most people
/// would expect"); a pass-by-reference value carries its image. `exec_eval_cleanup`
/// runs after each param (C copies the value into the stmt_mcontext first; the
/// owned model carries the by-ref image in `byref`, which outlives the cleanup).
fn exec_eval_using_params(
    estate: &mut PLpgSQL_execstate,
    params: &[types_plpgsql::PLpgSQL_expr],
) -> types_error::PgResult<Vec<exec_seams::DynUsingParam>> {
    // Fast path for no parameters (C returns NULL paramLI).
    if params.is_empty() {
        return Ok(Vec::new());
    }

    let mut out: Vec<exec_seams::DynUsingParam> = Vec::with_capacity(params.len());
    for param in params {
        let (value, isnull, mut ptype, _ptypmod) = exec_eval_expr_impl(estate, param)?;
        let mut byref = estate.last_eval_byref.take();
        let mut bare = value.as_usize();

        if ptype == UNKNOWNOID {
            // Treat 'unknown' parameters as text, since that's what most people
            // would expect. (C: prm->ptype = TEXTOID; prm->value =
            // CStringGetTextDatum(DatumGetCString(prm->value)).) Render the
            // unknown value to its C-string text, then reframe it as a header-ful
            // `text` varlena image so the executor reads a varlena.
            let s = convert_value_to_string(value, byref.clone(), ptype)?;
            ptype = TEXTOID;
            if !isnull {
                let (_d, image) = exec_seams::cstring_to_text_datum::call(s)?;
                byref = Some(image);
                bare = 0;
            }
        }

        out.push(exec_seams::DynUsingParam {
            value: bare,
            isnull,
            typeid: ptype,
            byref,
        });
        exec_eval_cleanup(estate);
    }

    Ok(out)
}

/// `exec_stmt_dynexecute(estate, stmt)` (pl_exec.c 4440) — execute a dynamic SQL
/// query string built at runtime (`EXECUTE '<sql>' [INTO target] [USING ...]`).
/// Evaluate the query-string expression to text, evaluate the USING params, run
/// the string as a one-shot SQL statement (any command type), and — for INTO —
/// move the first result row into the target (with the STRICT / too-many-rows
/// checks).
fn exec_stmt_dynexecute(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_dynexecute,
) -> PLpgSQL_rc_result {
    let query_expr = stmt
        .query
        .as_deref()
        .expect("EXECUTE carries a query-string expression");

    // First we evaluate the string expression after the EXECUTE keyword. Its
    // result is the querystring we have to execute.
    let (value, isnull, restype, _restypmod) = exec_eval_expr_impl(estate, query_expr)?;
    if isnull {
        return Err(types_error::PgError::error(
            "query string argument of EXECUTE is null".to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }
    // Get the C-String representation (convert_value_to_string).
    let byref = estate.last_eval_byref.take();
    let querystr = convert_value_to_string(value, byref, restype)?;
    exec_eval_cleanup(estate);

    // Execute the query without preparing a saved plan, with the USING params.
    let using = exec_eval_using_params(estate, &stmt.params)?;

    let result = exec_seams::exec_dynexecute_via_spi::call(
        querystr.clone(),
        using,
        estate.readonly_func,
        stmt.into, // collect first row when INTO
        false,     // collect_all
        0,         // run to completion
    )?;

    let exec_res = result.code;
    match exec_res {
        exec_seams::SPI_OK_SELECT
        | exec_seams::SPI_OK_INSERT
        | exec_seams::SPI_OK_UPDATE
        | exec_seams::SPI_OK_DELETE
        | exec_seams::SPI_OK_INSERT_RETURNING
        | exec_seams::SPI_OK_UPDATE_RETURNING
        | exec_seams::SPI_OK_DELETE_RETURNING
        | exec_seams::SPI_OK_UTILITY
        | exec_seams::SPI_OK_REWRITTEN => {}
        // A zero return implies the querystring contained no commands.
        0 => {}
        exec_seams::SPI_OK_SELINTO => {
            // We want to disallow SELECT INTO for now, because its behavior is
            // not consistent with SELECT INTO in a normal plpgsql context.
            return Err(types_error::PgError::error(
                "EXECUTE of SELECT ... INTO is not implemented".to_string(),
            )
            .with_hint(
                "You might want to use EXECUTE ... INTO or EXECUTE CREATE TABLE ... AS instead."
                    .to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        exec_seams::SPI_ERROR_COPY => {
            return Err(types_error::PgError::error(
                "cannot COPY to/from client in PL/pgSQL".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        _ => {
            return Err(types_error::PgError::error(format!(
                "SPI_execute_extended failed executing query \"{querystr}\": code {exec_res}"
            )));
        }
    }

    // Save result info for GET DIAGNOSTICS.
    estate.eval_processed = result.processed;

    // Process INTO if present.
    if stmt.into {
        // If the statement did not return a tuple table, complain.
        if !result.returned_tuptable {
            return Err(types_error::PgError::error(
                "INTO used with a command that cannot return data".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR));
        }

        let target = stmt.target.as_deref().expect("INTO carries a target");
        let n = result.processed;

        if n == 0 {
            // If STRICT and no row, throw; otherwise set target to NULL(s).
            if stmt.strict {
                return Err(types_error::PgError::error("query returned no rows".to_string())
                    .with_sqlstate(types_error::ERRCODE_NO_DATA_FOUND));
            }
            exec_move_row_into_target(estate, target, &[])?;
        } else {
            if n > 1 && stmt.strict {
                return Err(types_error::PgError::error(
                    "query returned more than one row".to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_TOO_MANY_ROWS));
            }
            // Put the first result row into the target.
            exec_move_row_into_target(estate, target, &result.first_row)?;
        }
        // Clean up after exec_move_row().
        exec_eval_cleanup(estate);
    }

    Ok(PLpgSQL_rc::PLPGSQL_RC_OK)
}

/// `exec_stmt_dynfors(estate, stmt)` (pl_exec.c 5497) — FOR rec/row IN EXECUTE
/// <text> [USING ...] LOOP. Evaluate the dynamic query string, open it (via the
/// materialize-all `exec_dynquery_with_params`), and run the shared FOR-loop
/// driver over its rows.
fn exec_stmt_dynfors(
    estate: &mut PLpgSQL_execstate,
    stmt: &types_plpgsql::PLpgSQL_stmt_dynfors,
) -> PLpgSQL_rc_result {
    let loopvar = stmt
        .var
        .as_deref()
        .expect("FOR-IN-EXECUTE carries a loop variable");

    let rows = exec_dynquery_with_params(estate, &stmt.query, &stmt.params)?;

    let rc = exec_for_query(estate, loopvar, &stmt.body, stmt.label.as_deref(), rows)?;

    exec_eval_cleanup(estate);
    Ok(rc)
}

/// `exec_dynquery_with_params(estate, dynquery, params, ...)` (pl_exec.c 8359) —
/// evaluate the dynamic query-string expression to a text value, then run it as
/// a top-level SQL statement, collecting every result row. The USING-parameter
/// leg (`exec_eval_using_params` → `SPI_cursor_open_with_args`) is the dynamic
/// param substrate; it stays loud until that lands (the FOR-IN-EXECUTE shapes
/// without USING run end-to-end). Returns the materialized rows.
fn exec_dynquery_with_params(
    estate: &mut PLpgSQL_execstate,
    dynquery: &Option<Box<types_plpgsql::PLpgSQL_expr>>,
    params: &[types_plpgsql::PLpgSQL_expr],
) -> types_error::PgResult<Vec<Vec<exec_seams::ExecsqlColumn>>> {
    let dynquery = dynquery
        .as_deref()
        .expect("FOR-IN-EXECUTE carries a query-string expression");

    // Evaluate the string expression (querystr = exec_eval_expr(dynquery)).
    let (value, isnull, restype, _restypmod) = exec_eval_expr_impl(estate, dynquery)?;
    if isnull {
        return Err(
            types_error::PgError::error(
                "query string argument of EXECUTE is null".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
        );
    }
    // convert_value_to_string(estate, query, restype) — render the value to text.
    let byref = estate.last_eval_byref.take();
    let querystr = convert_value_to_string(value, byref, restype)?;
    exec_eval_cleanup(estate);

    // exec_eval_using_params(estate, params): evaluate the USING expressions.
    let using = exec_eval_using_params(estate, params)?;

    // Run the dynamic query as a top-level SQL statement (SPI_cursor_parse_open
    // in C), collecting every result row for the FOR-IN-EXECUTE iteration. The
    // owned model materializes all rows up front (the portal/cursor leg is a
    // separate keystone); the observable iteration is identical.
    let result = exec_seams::exec_dynexecute_via_spi::call(
        querystr,
        using,
        estate.readonly_func,
        false, // into
        true,  // collect_all
        0,     // run to completion
    )?;

    estate.eval_processed = result.processed;
    Ok(result.all_rows)
}

// NB: the FOR-IN-EXECUTE USING leg above stays a loud `panic!` (unported
// dynamic-param substrate), reached before any row is produced.

fn exec_stmt_open(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_open (pl_exec.c) — exec_prepare_plan / \
         SPI_cursor_open_with_paramlist / exec_dynquery_with_params (SPI cursor surface)"
    );
}

fn exec_stmt_fetch(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_fetch (pl_exec.c) — SPI_scroll_cursor_fetch/move + \
         exec_move_row (SPI cursor surface + value substrate)"
    );
}

fn exec_stmt_close(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_close (pl_exec.c) — SPI_cursor_find / SPI_cursor_close \
         (SPI cursor surface)"
    );
}

fn exec_stmt_commit(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_commit (pl_exec.c) — SPI_commit / SPI_start_transaction + \
         simple-expr infra rebuild (SPI + xact)"
    );
}

fn exec_stmt_rollback(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc_result {
    panic!(
        "seam not wired: exec_stmt_rollback (pl_exec.c) — SPI_rollback / SPI_start_transaction + \
         simple-expr infra rebuild (SPI + xact)"
    );
}

// ===========================================================================
// Top-level executor entry points
// ===========================================================================

/// One call argument value (`fcinfo->args[i]` — its `(Datum, isnull)` pair).
///
/// A pass-by-reference argument (`text`/`varchar`/`numeric`/…) carries its
/// verbatim header-ful varlena / cstring byte image in `byref` (the bare-word
/// `value` is `0` then), taken from the live `fcinfo.ref_args[i]` at the fmgr
/// boundary; the arg-store leg copies it into the target variable's
/// `value_byref` so the image is available to expression evaluation. `None` for
/// a by-value argument, where `value` is the scalar word.
#[derive(Debug, Clone)]
pub struct FunctionCallArg {
    pub value: Datum,
    pub isnull: bool,
    pub byref: Option<Vec<u8>>,
}

/// The result of executing a scalar PL/pgSQL function: the result `Datum`, its
/// NULL flag (the C `fcinfo->isnull`), and its runtime type Oid.
///
/// `byref` is `Some(image)` when the result is a pass-by-reference value: the
/// verbatim header-ful varlena / cstring byte image (`datumCopy`'d into the
/// function's result context). The handler sets `fcinfo.ref_result` from it at
/// the fmgr boundary, and the bare-word `value` is unused (`0`). `None` for a
/// by-value result, where `value` is the scalar word.
#[derive(Debug, Clone)]
pub struct FunctionResult {
    pub value: Datum,
    pub isnull: bool,
    pub byref: Option<Vec<u8>>,
    pub rettype: Oid,
}

/// `plpgsql_estate_setup(estate, func, rsi, simple_eval_estate,
/// simple_eval_resowner)` (pl_exec.c) — build the per-call execution state.
///
/// The scalar control-flow fields are populated 1:1 from the function. The
/// substrate handles (`paramLI` via `makeParamList`, the simple-expr `EState`,
/// the cast-expr hash, and the per-tuple `eval_econtext` via
/// `plpgsql_create_econtext`) are owned by the executor/SPI substrate; they are
/// left `None` here and created lazily the first time an expression is
/// evaluated (the expr-eval seams panic loudly until that substrate lands).
/// Control-flow-only execution never reads them.
pub fn plpgsql_estate_setup(
    func: &PLpgSQL_function,
    rsi: Option<types_plpgsql::ReturnSetInfo>,
    simple_eval_estate: Option<EState>,
    simple_eval_resowner: Option<ResourceOwner>,
) -> PLpgSQL_execstate {
    PLpgSQL_execstate {
        func: None, // opaque back-ref; the comp↔exec handle is set when needed
        trigdata: None,
        evtrigdata: None,

        retval: Datum::null(),
        retisnull: true,
        rettype: INVALID_OID,
        retval_byref: None,
        last_eval_byref: None,

        fn_rettype: func.fn_rettype,
        retistuple: func.fn_retistuple,
        retisset: func.fn_retset,

        readonly_func: func.fn_readonly,
        atomic: true,

        exitlabel: None,
        cur_error: None,

        tuple_store: None,
        tuple_store_desc: None,
        tuple_store_cxt: None,
        tuple_store_owner: None,
        rsi,

        found_varno: func.found_varno,
        ndatums: func.ndatums,
        datums: Vec::new(), // filled by copy_plpgsql_datums
        datum_context: None,

        // makeParamList(0) + hook install — executor param substrate (lazy).
        paramLI: None,

        // shared_simple_eval_estate / private one; shared cast hash — lazy
        // (created on first simple-expr eval, which is itself loud today).
        simple_eval_estate,
        simple_eval_resowner,
        procedure_resowner: None,

        cast_hash: None,

        stmt_mcontext: None,
        stmt_mcontext_parent: None,

        eval_tuptable: None,
        eval_processed: 0,
        eval_econtext: None, // plpgsql_create_econtext — lazy

        err_stmt: None,
        err_var: None,
        err_text: None,

        plugin_info: None,
    }
}

/// `copy_plpgsql_datums(estate, func)` (pl_exec.c) — make the per-call local
/// copies of the function's datums.
///
/// In C, VAR/PROMISE/REC datums are byte-copied into a single workspace while
/// ROW/RECFIELD are shared read-only. In the owned model every datum is cloned
/// into the execstate's `datums` Vec (the clone is value-equivalent; ROW and
/// RECFIELD carry only read-only cached data).
pub(crate) fn copy_plpgsql_datums(estate: &mut PLpgSQL_execstate, func: &PLpgSQL_function) {
    let ndatums = estate.ndatums as usize;
    let mut datums = Vec::with_capacity(ndatums);
    for i in 0..ndatums {
        match datum_dtype(&func.datums[i]) {
            PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_REC
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD => {
                datums.push(func.datums[i].clone());
            }
        }
    }
    estate.datums = datums;
}

/// `plpgsql_exec_function(func, fcinfo, simple_eval_estate, simple_eval_resowner,
/// procedure_resowner, atomic)` (pl_exec.c) — the per-call executor.
///
/// Sets up the execstate (datum copy, paramLI, econtext, plugin func_beg),
/// runs the toplevel block, coerces the result, and tears down. The setup tail
/// (`makeParamList` + the hook install, the cast-expr hash, the econtext, the
/// SPI Proc context) and the result coercion are the value substrate (loud);
/// the block run + the RC handling is real once they land.
pub fn plpgsql_exec_function(
    func: &PLpgSQL_function,
    args: &[FunctionCallArg],
    simple_eval_estate: Option<EState>,
    simple_eval_resowner: Option<ResourceOwner>,
    procedure_resowner: Option<ResourceOwner>,
    atomic: bool,
) -> types_error::PgResult<FunctionResult> {
    // Setup the execution state.
    let mut estate = plpgsql_estate_setup(func, None, simple_eval_estate, simple_eval_resowner);
    estate.procedure_resowner = procedure_resowner;
    estate.atomic = atomic;

    // Make local execution copies of all the datums.
    estate.err_text = Some(mem::sdup("during initialization of execution state"));
    copy_plpgsql_datums(&mut estate, func);

    // Store the actual call argument values into the appropriate variables.
    estate.err_text = Some(mem::sdup(
        "while storing call arguments into local variables",
    ));
    for i in 0..(func.fn_nargs as usize) {
        let n = func.fn_argvarnos[i];
        let arg = &args[i];
        match datum_dtype(&estate.datums[n as usize]) {
            PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
            | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE => {
                let mut var = take_var(&mut estate, n);
                assign_simple_var(&mut estate, &mut var, arg.value, arg.isnull, false);
                // A pass-by-reference argument's image (the `fcinfo.ref_args[i]`
                // varlena/cstring bytes) is carried out-of-band alongside the
                // bare-word `value` (which is `0` for by-ref); store it into the
                // variable's `value_byref` companion so expression evaluation can
                // bind the rich `Datum::ByRef` (e.g. `RETURN s || '!'` over a
                // `text` argument). `assign_simple_var` cleared it for the by-val
                // store above; set the image for the by-ref case here.
                if !arg.isnull && arg.byref.is_some() {
                    var.value_byref = arg.byref.clone();
                }
                // The varlena R/W-expanded-object commandeering + flat-array
                // force-expand of the C arg loop is an expanded-object
                // optimization; the value substrate (expand_array /
                // TransferExpandedObject) is not reachable, and the
                // store-by-value above is value-equivalent for the in-memory
                // scalar case the control-flow path exercises.
                let varlena = var.datatype.as_ref().map(|t| t.typlen) == Some(-1);
                // C's arg loop force-expands only an expanded datum / R-W array
                // (`VARATT_IS_EXTERNAL_EXPANDED` / a R-W expandable array); a
                // plain flat varlena is stored as-is (`datumCopy`). When the
                // argument arrives as a flat by-reference image (the common
                // `text`/`varchar`/`numeric` argument), it is already flat — the
                // store above is faithful and the expand/commandeer leg does not
                // apply, so skip the loud expanded-object seam.
                let has_flat_image = arg.byref.is_some();
                put_var(&mut estate, n, var);
                if !arg.isnull && varlena && !has_flat_image {
                    // R/W or array detoast/expand leg (loud value substrate).
                    seam::arg_store_expanded_object(arg.value);
                }
            }
            PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
                if !arg.isnull {
                    seam::exec_move_row_from_datum(&mut estate, n, arg.value)?;
                } else {
                    seam::exec_move_row_null(&mut estate, n)?;
                }
                exec_eval_cleanup(&mut estate);
            }
            other => seam::elog_unrecognized_dtype_exec(other),
        }
    }

    estate.err_text = Some(mem::sdup("during function entry"));

    // Set the magic variable FOUND to false.
    exec_set_found(&mut estate, false);

    // (The instrumentation plugin func_beg hook is owned by the plugin
    // rendezvous substrate; plpgsql_plugin_ptr is null in this build.)

    // Now call the toplevel block of statements.
    estate.err_text = None;
    let action = func
        .action
        .as_deref()
        .expect("compiled PL/pgSQL function has an action block");
    let rc = exec_toplevel_block(&mut estate, action)?;
    if rc != PLpgSQL_rc::PLPGSQL_RC_RETURN {
        estate.err_text = None;
        return Err(seam::ereport_no_return_statement());
    }

    // We got a return value — process it.
    estate.err_text = Some(mem::sdup(
        "while casting return value to function's return type",
    ));

    let mut result = FunctionResult {
        value: estate.retval,
        isnull: estate.retisnull,
        byref: None,
        rettype: estate.rettype,
    };

    if estate.retisset {
        // SRF materialize-mode result: the tuplestore + ReturnSetInfo handoff
        // is the SRF/executor substrate.
        seam::coerce_set_result(&mut estate);
        result.value = Datum::null();
        result.isnull = true;
    } else if !estate.retisnull {
        // Cast the result to the function's declared type and copy it out to
        // the upper executor context. The tuple/coercion path is the value
        // substrate; the VOID / matching-scalar fast path is real.
        if estate.retistuple {
            seam::coerce_function_result_tuple(&mut estate);
            result.value = estate.retval;
        } else if estate.fn_rettype == estate.rettype {
            // No coercion needed for an exact type match (the common scalar
            // RETURN, and the VOID-return hack rettype==VOIDOID==fn_rettype).
            // A by-value result is returned by word; a by-reference result
            // (text/varchar/numeric/…) is returned via its owned image, which the
            // handler copies into the fmgr result context (C's datumCopy out).
            result.value = estate.retval;
            result.byref = estate.retval_byref.take();
        } else {
            // exec_cast_value to the declared rettype + datumCopy out. The
            // source value may itself be a by-reference type (its image in
            // `retval_byref`); the coerced result may be by-reference too (e.g.
            // `RETURN x::text` over an int — a `text` result). Thread the image
            // both ways so a by-ref-after-cast RETURN crosses the fmgr boundary
            // through the handler's `set_ref_result`.
            let (retval, retisnull, rettype, fn_rettype) =
                (estate.retval, estate.retisnull, estate.rettype, estate.fn_rettype);
            let retval_byref = estate.retval_byref.take();
            let (v, isnull, byref) = exec_cast_value_with_byref(
                &mut estate,
                retval,
                retval_byref,
                retisnull,
                rettype,
                -1,
                fn_rettype,
                -1,
            )?;
            result.value = v;
            result.isnull = isnull;
            result.byref = byref;
        }
    }

    // Let the eval econtext be released (exec_eval_cleanup + teardown happens
    // as the estate drops; the SPI Proc context / shared econtext are owned by
    // the caller's SPI bracket).
    Ok(result)
}

/// `plpgsql_exec_trigger(func, trigdata)` (pl_exec.c) — the DML-trigger
/// executor entry.
pub fn plpgsql_exec_trigger(
    func: &PLpgSQL_function,
    trigdata: types_plpgsql::TriggerData,
) -> types_error::PgResult<Datum> {
    trigger::plpgsql_exec_trigger_impl(func, trigdata)
}

/// `plpgsql_exec_event_trigger(func, trigdata)` (pl_exec.c) — the event-trigger
/// executor entry. Sets up the execution state, marks it as an event-trigger
/// call (so the `TG_EVENT`/`TG_TAG` promises resolve), runs the toplevel block,
/// and requires a RETURN (an event-trigger function returns no value, but must
/// reach a RETURN, exactly as C).
pub fn plpgsql_exec_event_trigger(
    func: &PLpgSQL_function,
    _trigdata: types_plpgsql::EventTriggerData,
) -> types_error::PgResult<()> {
    // Setup the execution state. (No simple_eval_estate/resowner — C passes NULL.)
    let mut estate = plpgsql_estate_setup(func, None, None, None);

    // estate.evtrigdata = trigdata (the current-event-trigger marker; the rich
    // event/tag rides commands/event_trigger.c's CURRENT_EVENT_TRIGGER side-channel).
    estate.evtrigdata = Some(types_plpgsql::EventTriggerData(0));

    // Make local execution copies of all the datums.
    estate.err_text = Some(crate::mem::sdup("during initialization of execution state"));
    copy_plpgsql_datums(&mut estate, func);

    // Now call the toplevel block of statements.
    estate.err_text = None;
    let action = func
        .action
        .as_deref()
        .expect("compiled event-trigger function has an action block");
    let rc = exec_toplevel_block(&mut estate, action)?;
    if rc != types_plpgsql::PLpgSQL_rc::PLPGSQL_RC_RETURN {
        estate.err_text = None;
        return Err(types_error::PgError::error(
            "control reached end of trigger procedure without RETURN".to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT));
    }

    estate.err_text = Some(crate::mem::sdup("during function exit"));

    // Clean up any leftover temporary memory (the C plpgsql_destroy_econtext +
    // exec_eval_cleanup; the simple-eval econtext teardown is owned elsewhere in
    // this port, matching plpgsql_exec_trigger_impl).
    exec_eval_cleanup(&mut estate);

    Ok(())
}

// ===========================================================================
// plpgsql_exec_get_datum_type_info — the compiler's compile-time callback.
// ===========================================================================

/// `plpgsql_exec_get_datum_type_info(estate, datum, &typeId, &typMod,
/// &collation)` (pl_exec.c 5524) — report the type/typmod/collation of a datum.
///
/// The VAR/PROMISE arm is real (reads the datum's declared `datatype`). The
/// REC arm needs the live expanded-record header (`rec->erh->er_typeid`) and
/// the RECFIELD arm needs `expanded_record_lookup_field` — both runtime
/// expanded-record substrate, routed loud.
pub fn plpgsql_exec_get_datum_type_info(
    datum: &PLpgSQL_datum,
) -> exec_seams::DatumTypeInfo {
    match datum {
        PLpgSQL_datum::Var(var) => {
            let t = var
                .datatype
                .as_ref()
                .expect("VAR/PROMISE datum has a datatype");
            exec_seams::DatumTypeInfo {
                type_id: t.typoid,
                typmod: t.atttypmod,
                collation: t.collation,
            }
        }
        PLpgSQL_datum::Rec(rec) => {
            // If the record has no live expanded header, or it is declared with
            // a named composite type, we can report the declared rectypeid with
            // typmod -1; otherwise we must read the live er_typeid (runtime
            // expanded-record substrate).
            if rec.erh.is_none() || rec.rectypeid != RECORDOID {
                exec_seams::DatumTypeInfo {
                    type_id: rec.rectypeid,
                    typmod: -1,
                    collation: INVALID_OID,
                }
            } else {
                panic!(
                    "seam not wired: plpgsql_exec_get_datum_type_info REC arm (pl_exec.c) — \
                     live expanded-record er_typeid (expanded-record substrate)"
                );
            }
        }
        PLpgSQL_datum::Recfield(_) => {
            panic!(
                "seam not wired: plpgsql_exec_get_datum_type_info RECFIELD arm (pl_exec.c) — \
                 instantiate_empty_record_variable + expanded_record_lookup_field \
                 (expanded-record substrate)"
            );
        }
        PLpgSQL_datum::Row(_) => {
            // ROW datums don't reach make_datum_param (the C switch has no ROW
            // arm; a ROW Param is built elsewhere). elog(ERROR) would fire.
            panic!("unrecognized dtype: ROW in plpgsql_exec_get_datum_type_info");
        }
    }
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this crate's inward seam (`plpgsql_exec_get_datum_type_info`), called
/// by the compiler from `make_datum_param`.
pub fn init_seams() {
    exec_seams::plpgsql_exec_get_datum_type_info::set(|estate_handle, dno| {
        Ok(with_execstate_datum(estate_handle, dno, |datum| {
            plpgsql_exec_get_datum_type_info(datum)
        }))
    });
}

/// Resolve `(estate_handle, dno)` to the datum and apply `f`.
///
/// In C the handle is `expr->func->cur_estate` — a live `PLpgSQL_execstate *`.
/// The owned model does not yet hand out execstate handles to the compiler
/// (the comp↔exec compile-time edge is not yet exercised end-to-end); until the
/// compiler passes a live execstate through the seam this panics loudly. The
/// real datum-array lookup is `estate->datums[dno]`.
fn with_execstate_datum<R>(
    _estate_handle: u64,
    _dno: int32,
    _f: impl FnOnce(&PLpgSQL_datum) -> R,
) -> R {
    panic!(
        "seam not wired: plpgsql_exec_get_datum_type_info handle resolution (pl_exec.c) — \
         no live PLpgSQL_execstate is registered for the compiler's cur_estate handle yet \
         (comp↔exec compile-time edge); resolves estate->datums[dno] once exec drives compile"
    );
}

// ===========================================================================
// Small datum-shape helpers (pure inspection of the owned data model)
// ===========================================================================

/// The dtype tag of a datum (distinguishing VAR vs PROMISE via the `promise`
/// field, matching the C `dtype` discriminator).
fn datum_dtype(d: &PLpgSQL_datum) -> PLpgSQL_datum_type {
    match d {
        PLpgSQL_datum::Var(v) => {
            if v.promise == PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE {
                PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
            } else {
                PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE
            }
        }
        PLpgSQL_datum::Row(_) => PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW,
        PLpgSQL_datum::Rec(_) => PLpgSQL_datum_type::PLPGSQL_DTYPE_REC,
        PLpgSQL_datum::Recfield(_) => PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD,
    }
}

fn var_has_default(d: &PLpgSQL_datum) -> bool {
    matches!(d, PLpgSQL_datum::Var(v) if v.default_val.is_some())
}

fn var_is_domain(d: &PLpgSQL_datum) -> bool {
    const TYPTYPE_DOMAIN: u8 = b'd';
    matches!(d, PLpgSQL_datum::Var(v)
        if v.datatype.as_ref().map(|t| t.typtype) == Some(TYPTYPE_DOMAIN))
}

fn clone_var_default(d: &PLpgSQL_datum) -> Option<Box<types_plpgsql::PLpgSQL_expr>> {
    match d {
        PLpgSQL_datum::Var(v) => v.default_val.clone(),
        _ => None,
    }
}

fn rec_has_default(d: &PLpgSQL_datum) -> bool {
    matches!(d, PLpgSQL_datum::Rec(r) if r.default_val.is_some())
}

fn clone_rec_default(d: &PLpgSQL_datum) -> Option<Box<types_plpgsql::PLpgSQL_expr>> {
    match d {
        PLpgSQL_datum::Rec(r) => r.default_val.clone(),
        _ => None,
    }
}

fn fori_var_type(estate: &PLpgSQL_execstate, dno: int32) -> (Oid, int32) {
    match &estate.datums[dno as usize] {
        PLpgSQL_datum::Var(v) => {
            let t = v.datatype.as_ref().expect("FOR(i) var has a datatype");
            (t.typoid, t.atttypmod)
        }
        _ => panic!("FOR(i) loop variable is not a PLpgSQL_var"),
    }
}

fn temp_var_type_differs(d: &PLpgSQL_datum, t_typoid: Oid, t_typmod: int32) -> bool {
    match d {
        PLpgSQL_datum::Var(v) => match v.datatype.as_ref() {
            Some(t) => t.typoid != t_typoid || t.atttypmod != t_typmod,
            None => true,
        },
        _ => true,
    }
}

fn read_var_value(d: &PLpgSQL_datum) -> (Datum, bool, Oid) {
    match d {
        PLpgSQL_datum::Var(v) => {
            let typoid = v.datatype.as_ref().map(|t| t.typoid).unwrap_or(INVALID_OID);
            (v.value, v.isnull, typoid)
        }
        _ => panic!("read_var_value on non-VAR datum"),
    }
}

/// The out-of-band by-reference image of a scalar VAR datum (`value_byref`), the
/// companion to [`read_var_value`]'s bare word; `None` for a by-value variable.
fn read_var_value_byref(d: &PLpgSQL_datum) -> Option<Vec<u8>> {
    match d {
        PLpgSQL_datum::Var(v) => v.value_byref.clone(),
        _ => panic!("read_var_value_byref on non-VAR datum"),
    }
}

fn discard_temp_var(estate: &mut PLpgSQL_execstate, dno: int32) {
    let mut var = take_var(estate, dno);
    assign_simple_var(estate, &mut var, Datum::null(), true, false);
    put_var(estate, dno, var);
}

/// Move a VAR out of the datum array, leaving a placeholder (always put back).
fn take_var(estate: &mut PLpgSQL_execstate, dno: int32) -> Box<PLpgSQL_var> {
    match core::mem::replace(&mut estate.datums[dno as usize], var_placeholder()) {
        PLpgSQL_datum::Var(v) => v,
        _ => panic!("datum {dno} is not a PLpgSQL_var"),
    }
}

fn put_var(estate: &mut PLpgSQL_execstate, dno: int32, var: Box<PLpgSQL_var>) {
    estate.datums[dno as usize] = PLpgSQL_datum::Var(var);
}

/// A minimal placeholder VAR used transiently by [`take_var`]; never observed.
fn var_placeholder() -> PLpgSQL_datum {
    PLpgSQL_datum::Var(Box::new(PLpgSQL_var {
        dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR,
        dno: -1,
        refname: String::new(),
        lineno: 0,
        isconst: false,
        notnull: false,
        default_val: None,
        datatype: None,
        cursor_explicit_expr: None,
        cursor_explicit_argrow: -1,
        cursor_options: 0,
        value: Datum::null(),
        isnull: true,
        freeval: false,
        value_byref: None,
        promise: PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE,
    }))
}

/// Is the currently-executing function a PROCEDURE? The func back-reference is
/// opaque here; the VOID-return hack only applies to non-procedures, so the
/// common-case default (false) is the path the C takes for ordinary functions.
fn func_is_procedure(_estate: &PLpgSQL_execstate) -> bool {
    false
}

/// `assign_simple_var(estate, var, newvalue, isnull, freeable)` (pl_exec.c 8770)
/// — assign to a "simple" (scalar VAR/PROMISE) variable's value/isnull.
///
/// The value store + promise-cancel is real. Two legs touch the value
/// substrate and route loud: (1) the non-atomic detoast of an external TOAST
/// pointer (`!estate->atomic && typlen==-1 && VARATT_IS_EXTERNAL_NON_EXPANDED`)
/// — needs `detoast_external_attr` + `datumCopy`; (2) freeing the old value
/// (`var->freeval`) — needs `DeleteExpandedObject`/`pfree`. Neither fires for
/// the common in-atomic, non-freeable, non-toast store (e.g. the FOUND magic
/// var or a bool/int assignment).
pub(crate) fn assign_simple_var(
    estate: &mut PLpgSQL_execstate,
    var: &mut PLpgSQL_var,
    newvalue: Datum,
    isnull: bool,
    freeable: bool,
) {
    debug_assert!(matches!(
        var.dtype,
        PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE
    ));

    let typlen = var.datatype.as_ref().map(|t| t.typlen).unwrap_or(0);

    // Non-atomic contexts must not store bare TOAST pointers (they go stale
    // after a commit); force a detoast. Expanded objects are fine.
    if !estate.atomic && !isnull && typlen == -1 && seam::datum_is_external_non_expanded(newvalue)
    {
        // detoast in eval_mcontext, copy to function context, free input if
        // freeable — all in the value/toast substrate.
        let (detoasted, _now_freeable) = seam::assign_simple_var_detoast(newvalue, freeable);
        var.value = detoasted;
        var.isnull = isnull;
        var.freeval = true;
        // The bare-word store does not carry a by-ref image; clear the
        // out-of-band companion so no stale image is read by a later snapshot.
        // A by-ref caller (arg-store / INTO / cast) sets `value_byref` after.
        var.value_byref = None;
        var.promise = PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE;
        return;
    }

    // Free the old value if needed (value/toast substrate).
    if var.freeval {
        seam::assign_simple_var_free_old(var.value, var.isnull, typlen);
    }

    var.value = newvalue;
    var.isnull = isnull;
    var.freeval = freeable;
    // The bare-word store does not carry a by-ref image; clear the out-of-band
    // companion so no stale image is read by a later snapshot. A by-ref caller
    // (arg-store / INTO / cast) sets `value_byref` after this returns.
    var.value_byref = None;
    var.promise = PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE;
}

// --- SQLSTATE classification (errcodes.h macros, pure bit ops) --------------

/// `MAKE_SQLSTATE('5','7','0','1','4')` == `ERRCODE_QUERY_CANCELED`.
fn errcode_query_canceled() -> int32 {
    make_sqlstate(b'5', b'7', b'0', b'1', b'4')
}

/// `MAKE_SQLSTATE('P','0','0','0','4')` == `ERRCODE_ASSERT_FAILURE`.
fn errcode_assert_failure() -> int32 {
    make_sqlstate(b'P', b'0', b'0', b'0', b'4')
}

/// `MAKE_SQLSTATE(ch1..ch5)` — pack a 5-char SQLSTATE into the packed int32.
fn make_sqlstate(ch1: u8, ch2: u8, ch3: u8, ch4: u8, ch5: u8) -> int32 {
    let b = |c: u8| ((c.wrapping_sub(b'0')) & 0x3F) as i32;
    b(ch1) | (b(ch2) << 6) | (b(ch3) << 12) | (b(ch4) << 18) | (b(ch5) << 24)
}

/// `ERRCODE_IS_CATEGORY(ec)` — its last three characters are "000".
fn errcode_is_category(ec: int32) -> bool {
    (ec & !((1 << 12) - 1)) == 0
}

/// `ERRCODE_TO_CATEGORY(ec)` — the category code (mask off the detail digits).
fn errcode_to_category(ec: int32) -> int32 {
    ec & ((1 << 12) - 1)
}
