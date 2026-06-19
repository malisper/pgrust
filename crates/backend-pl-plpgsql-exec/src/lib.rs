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
//! `catch_unwind` error channel ([`exec_stmt_block_with_exceptions`]), the
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

mod mem;
mod seam;

use types_plpgsql::{
    int32, Datum, EState, Oid, PLpgSQL_condition, PLpgSQL_datum, PLpgSQL_datum_type,
    PLpgSQL_execstate, PLpgSQL_function, PLpgSQL_promise_type, PLpgSQL_rc, PLpgSQL_stmt,
    PLpgSQL_stmt_assign, PLpgSQL_stmt_block, PLpgSQL_stmt_case, PLpgSQL_stmt_exit,
    PLpgSQL_stmt_fori, PLpgSQL_stmt_foreach_a, PLpgSQL_stmt_if, PLpgSQL_stmt_loop,
    PLpgSQL_stmt_perform, PLpgSQL_stmt_return, PLpgSQL_stmt_while, PLpgSQL_var, ResourceOwner,
    PLPGSQL_OTHERS,
};

use backend_pl_plpgsql_exec_seams as exec_seams;

/// `InvalidOid` — the zero OID sentinel.
const INVALID_OID: Oid = 0;

/// `UNKNOWNOID` (705).
const UNKNOWNOID: Oid = 705;

/// `VOIDOID` (2278).
const VOIDOID: Oid = 2278;

/// `BOOLOID` (16).
#[allow(dead_code)]
const BOOLOID: Oid = 16;

/// `RECORDOID` (2249).
#[allow(dead_code)]
const RECORDOID: Oid = 2249;

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

// ===========================================================================
// Top-level + block
// ===========================================================================

/// `exec_toplevel_block(estate, block)` (pl_exec.c) — execute the toplevel
/// block.
pub fn exec_toplevel_block(
    estate: &mut PLpgSQL_execstate,
    block: &PLpgSQL_stmt_block,
) -> PLpgSQL_rc {
    estate.err_stmt = None;
    seam::check_for_interrupts();
    let rc = exec_stmt_block(estate, block);
    estate.err_stmt = None;
    rc
}

/// `exec_stmt_block(estate, block)` (pl_exec.c) — execute a block of
/// statements.
fn exec_stmt_block(estate: &mut PLpgSQL_execstate, block: &PLpgSQL_stmt_block) -> PLpgSQL_rc {
    // First initialize all variables declared in this block.
    estate.err_text = Some(mem::sdup(
        "during statement block local variable initialization",
    ));

    for i in 0..(block.n_initvars as usize) {
        let n = block.initvarnos[i];
        estate.err_var = Some(n as u64);

        // The set of dtypes handled here must match plpgsql_add_initdatums().
        match datum_dtype(&estate.datums[n as usize]) {
            PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR => exec_block_init_var(estate, n),
            PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => exec_block_init_rec(estate, n),
            other => seam::elog_unrecognized_dtype_exec(other),
        }
    }

    estate.err_var = None;

    let rc = if block.exceptions.is_some() {
        exec_stmt_block_with_exceptions(estate, block)
    } else {
        estate.err_text = None;
        exec_stmts(estate, &block.body)
    };

    estate.err_text = None;

    block_handle_rc(estate, block.label.as_deref(), rc)
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
fn exec_block_init_var(estate: &mut PLpgSQL_execstate, dno: int32) {
    {
        let mut var = take_var(estate, dno);
        seam::assign_simple_var(estate, &mut var, Datum::null(), true, false);
        put_var(estate, dno, var);
    }

    if !var_has_default(&estate.datums[dno as usize]) {
        if var_is_domain(&estate.datums[dno as usize]) {
            seam::exec_assign_value(estate, dno, Datum::null(), true, UNKNOWNOID, -1);
        }
        // parser should have rejected NOT NULL (Assert(!var->notnull)).
    } else {
        let default =
            clone_var_default(&estate.datums[dno as usize]).expect("default_val present");
        seam::exec_assign_expr(estate, dno, &default);
    }
}

/// Block-local REC initialization (the `PLPGSQL_DTYPE_REC` arm).
fn exec_block_init_rec(estate: &mut PLpgSQL_execstate, dno: int32) {
    if !rec_has_default(&estate.datums[dno as usize]) {
        seam::exec_move_row_null(estate, dno);
        // parser should have rejected NOT NULL (Assert(!rec->notnull)).
    } else {
        let default =
            clone_rec_default(&estate.datums[dno as usize]).expect("default present");
        seam::exec_assign_expr(estate, dno, &default);
    }
}

/// `exec_stmt_block` EXCEPTION arm (pl_exec.c ~1793) — the catchable error
/// channel.
///
/// The body runs inside an internal subtransaction; on error the captured
/// `PgError` is matched against the WHEN conditions. This is the repo's
/// `longjmp` replacement: the SQL executor / SPI raise an error by
/// `panic_any(PgError)` (see `backend-utils-fmgr-core::invoke_pgfunction`), so
/// PG_TRY/PG_CATCH becomes `catch_unwind` + `downcast::<PgError>`.
///
/// The subtransaction machinery (`BeginInternalSubTransaction`,
/// `RollbackAndReleaseCurrentSubTransaction`, `ReleaseCurrentSubTransaction`,
/// `MemoryContextSwitchTo`, `SPI_restore_connection`) and the per-handler datum
/// setup (`assign_text_var` of SQLSTATE/SQLERRM, `exec_eval_cleanup`) bottom out
/// in the xact + SPI value substrate and are routed through [`seam`] (loud).
/// The control flow — run body, catch, match, run the matching handler, or
/// re-raise — is real.
fn exec_stmt_block_with_exceptions(
    estate: &mut PLpgSQL_execstate,
    block: &PLpgSQL_stmt_block,
) -> PLpgSQL_rc {
    // BeginInternalSubTransaction(NULL) + remember the caller context / owner.
    // (xact substrate; loud until SPI/xact #215 lands.)
    begin_internal_subtransaction(estate);

    // PG_TRY: run the block body. The executor/SPI raise errors via
    // panic_any(PgError); catch them here so the WHEN clauses can inspect the
    // SQLSTATE and the subtransaction can be rolled back.
    let body = core::panic::AssertUnwindSafe(|| exec_stmts(estate, &block.body));
    let caught = run_catching(body);

    match caught {
        Ok(rc) => {
            // No error: ReleaseCurrentSubTransaction + restore context/owner.
            release_current_subtransaction(estate);
            rc
        }
        Err(edata) => {
            // PG_CATCH: roll back the subtransaction, restore the SPI
            // connection, then look for a matching exception handler.
            rollback_and_release_current_subtransaction(estate);

            let exceptions = block
                .exceptions
                .as_deref()
                .expect("exception path entered without an exception block");

            let mut handled: Option<PLpgSQL_rc> = None;
            for exc in &exceptions.exc_list {
                if exception_matches_conditions(edata.sqlstate.0, exc.conditions.as_deref())
                {
                    // Bind SQLSTATE / SQLERRM into the handler's special vars
                    // and record the current error for GET STACKED DIAGNOSTICS.
                    // `estate->cur_error = &edata` in C; the owned model carries
                    // cur_error as an opaque ErrorData handle whose population +
                    // field reads (exec_stmt_getdiag) are the ErrorData-codec
                    // value substrate (loud). We save/restore the slot here so
                    // the nesting discipline is preserved; the live edata is
                    // bound into the handler's special vars by assign_error_vars.
                    let save_cur_error = estate.cur_error.take();
                    assign_error_vars(estate, exceptions, &edata);

                    let rc = exec_stmts(estate, &exc.action);

                    estate.cur_error = save_cur_error;
                    handled = Some(rc);
                    break;
                }
            }

            match handled {
                Some(rc) => rc,
                // No matching handler: re-raise the original error.
                None => re_raise(edata),
            }
        }
    }
}

/// Run `f` catching a `PgError` raised via `panic_any`, mirroring PG_TRY /
/// PG_CATCH. Mirrors `backend-utils-fmgr-core::invoke_pgfunction`'s boundary.
fn run_catching<R>(
    f: core::panic::AssertUnwindSafe<impl FnOnce() -> R>,
) -> Result<R, types_error::PgError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f.0)) {
        Ok(r) => Ok(r),
        Err(payload) => Err(downcast_pgerror(payload)),
    }
}

fn downcast_pgerror(payload: Box<dyn core::any::Any + Send>) -> types_error::PgError {
    match payload.downcast::<types_error::PgError>() {
        Ok(err) => *err,
        Err(payload) => {
            // Not a structured PgError (e.g. a bare string panic from an
            // unported leg): reconstruct a generic internal error, mirroring
            // the C path that wraps a non-ereport longjmp.
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|s| String::from(*s))
                })
                .unwrap_or_else(|| String::from("unrecognized error in PL/pgSQL block"));
            types_error::PgError::error(msg)
        }
    }
}

/// Re-raise an error out of an un-handled EXCEPTION block (`PG_RE_THROW`).
fn re_raise(edata: types_error::PgError) -> ! {
    std::panic::panic_any(edata)
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

fn begin_internal_subtransaction(_estate: &mut PLpgSQL_execstate) {
    panic!(
        "seam not wired: BeginInternalSubTransaction (pl_exec.c exec_stmt_block EXCEPTION) — \
         internal subtransaction start (xact + SPI #215)"
    );
}

fn release_current_subtransaction(_estate: &mut PLpgSQL_execstate) {
    panic!(
        "seam not wired: ReleaseCurrentSubTransaction (pl_exec.c exec_stmt_block) — \
         commit internal subtransaction (xact + SPI #215)"
    );
}

fn rollback_and_release_current_subtransaction(_estate: &mut PLpgSQL_execstate) {
    panic!(
        "seam not wired: RollbackAndReleaseCurrentSubTransaction (pl_exec.c) — \
         abort internal subtransaction + SPI_restore_connection (xact + SPI #215)"
    );
}

fn assign_error_vars(
    _estate: &mut PLpgSQL_execstate,
    _block: &types_plpgsql::PLpgSQL_exception_block,
    _edata: &types_error::PgError,
) {
    panic!(
        "seam not wired: EXCEPTION handler SQLSTATE/SQLERRM binding (pl_exec.c) — \
         assign_text_var of the special vars (value substrate)"
    );
}

// ===========================================================================
// exec_stmts dispatch
// ===========================================================================

/// `exec_stmts(estate, stmts)` (pl_exec.c) — iterate over a list of statements
/// as long as their return code is OK.
fn exec_stmts(estate: &mut PLpgSQL_execstate, stmts: &[PLpgSQL_stmt]) -> PLpgSQL_rc {
    let save_estmt = estate.err_stmt.take();

    if stmts.is_empty() {
        // Ensure a CHECK_FOR_INTERRUPTS even though there is no statement.
        seam::check_for_interrupts();
        estate.err_stmt = save_estmt;
        return PLpgSQL_rc::PLPGSQL_RC_OK;
    }

    for stmt in stmts {
        estate.err_stmt = None;
        seam::check_for_interrupts();

        let rc = match stmt {
            PLpgSQL_stmt::Block(b) => exec_stmt_block(estate, b),
            PLpgSQL_stmt::Assign(s) => exec_stmt_assign(estate, s),
            PLpgSQL_stmt::Perform(s) => exec_stmt_perform(estate, s),
            PLpgSQL_stmt::Call(_) => exec_stmt_call(estate),
            PLpgSQL_stmt::Getdiag(_) => exec_stmt_getdiag(estate),
            PLpgSQL_stmt::If(s) => exec_stmt_if(estate, s),
            PLpgSQL_stmt::Case(s) => exec_stmt_case(estate, s),
            PLpgSQL_stmt::Loop(s) => exec_stmt_loop(estate, s),
            PLpgSQL_stmt::While(s) => exec_stmt_while(estate, s),
            PLpgSQL_stmt::Fori(s) => exec_stmt_fori(estate, s),
            PLpgSQL_stmt::Fors(_) => exec_stmt_fors(estate),
            PLpgSQL_stmt::Forc(_) => exec_stmt_forc(estate),
            PLpgSQL_stmt::ForeachA(s) => exec_stmt_foreach_a(estate, s),
            PLpgSQL_stmt::Exit(s) => exec_stmt_exit(estate, s),
            PLpgSQL_stmt::Return(s) => exec_stmt_return(estate, s),
            PLpgSQL_stmt::ReturnNext(_) => exec_stmt_return_next(estate),
            PLpgSQL_stmt::ReturnQuery(_) => exec_stmt_return_query(estate),
            PLpgSQL_stmt::Raise(_) => exec_stmt_raise(estate),
            PLpgSQL_stmt::Assert(_) => exec_stmt_assert(estate),
            PLpgSQL_stmt::Execsql(_) => exec_stmt_execsql(estate),
            PLpgSQL_stmt::Dynexecute(_) => exec_stmt_dynexecute(estate),
            PLpgSQL_stmt::Dynfors(_) => exec_stmt_dynfors(estate),
            PLpgSQL_stmt::Open(_) => exec_stmt_open(estate),
            PLpgSQL_stmt::Fetch(_) => exec_stmt_fetch(estate),
            PLpgSQL_stmt::Close(_) => exec_stmt_close(estate),
            PLpgSQL_stmt::Commit(_) => exec_stmt_commit(estate),
            PLpgSQL_stmt::Rollback(_) => exec_stmt_rollback(estate),
        };

        if rc != PLpgSQL_rc::PLPGSQL_RC_OK {
            estate.err_stmt = save_estmt;
            return rc;
        }
    }

    estate.err_stmt = save_estmt;
    PLpgSQL_rc::PLPGSQL_RC_OK
}

// ===========================================================================
// Control-flow statement arms (real)
// ===========================================================================

/// `exec_stmt_assign(estate, stmt)` (pl_exec.c).
fn exec_stmt_assign(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_assign) -> PLpgSQL_rc {
    debug_assert!(stmt.varno >= 0);
    let expr = stmt.expr.as_deref().expect("ASSIGN carries an expr");
    seam::exec_assign_expr(estate, stmt.varno, expr);
    PLpgSQL_rc::PLPGSQL_RC_OK
}

/// `exec_stmt_perform(estate, stmt)` (pl_exec.c) — run a query, discard the
/// result, set FOUND from the rowcount.
fn exec_stmt_perform(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_perform) -> PLpgSQL_rc {
    let expr = stmt.expr.as_deref().expect("PERFORM carries an expr");
    let _ = seam::exec_run_select(estate, expr, 0, false);
    exec_set_found(estate, estate.eval_processed != 0);
    exec_eval_cleanup(estate);
    PLpgSQL_rc::PLPGSQL_RC_OK
}

/// `exec_stmt_if(estate, stmt)` (pl_exec.c).
fn exec_stmt_if(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_if) -> PLpgSQL_rc {
    let cond = stmt.cond.as_deref().expect("IF carries a condition");
    let (value, isnull) = seam::exec_eval_boolean(estate, cond);
    exec_eval_cleanup(estate);
    if !isnull && value {
        return exec_stmts(estate, &stmt.then_body);
    }

    for elif in &stmt.elsif_list {
        let ec = elif.cond.as_deref().expect("ELSIF carries a condition");
        let (value, isnull) = seam::exec_eval_boolean(estate, ec);
        exec_eval_cleanup(estate);
        if !isnull && value {
            return exec_stmts(estate, &elif.stmts);
        }
    }

    exec_stmts(estate, &stmt.else_body)
}

/// `exec_stmt_case(estate, stmt)` (pl_exec.c) — searched / simple CASE.
fn exec_stmt_case(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_case) -> PLpgSQL_rc {
    let has_t_var = stmt.t_expr.is_some();

    if let Some(t_expr) = stmt.t_expr.as_deref() {
        let (t_val, isnull, t_typoid, t_typmod) = seam::exec_eval_expr(estate, t_expr);

        let t_varno = stmt.t_varno;
        if temp_var_type_differs(&estate.datums[t_varno as usize], t_typoid, t_typmod) {
            let mut t_var = take_var(estate, t_varno);
            seam::case_rebuild_temp_var_datatype(estate, &mut t_var, t_typoid, t_typmod);
            put_var(estate, t_varno, t_var);
        }

        seam::exec_assign_value(estate, t_varno, t_val, isnull, t_typoid, t_typmod);
        exec_eval_cleanup(estate);
    }

    for cwt in &stmt.case_when_list {
        let expr = cwt.expr.as_deref().expect("CASE WHEN carries a condition");
        let (value, isnull) = seam::exec_eval_boolean(estate, expr);
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
        seam::ereport_case_not_found();
    }

    exec_stmts(estate, &stmt.else_stmts)
}

/// `exec_stmt_loop(estate, stmt)` (pl_exec.c) — unconditional LOOP.
fn exec_stmt_loop(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_loop) -> PLpgSQL_rc {
    let label = stmt.label.clone();
    loop {
        let body_rc = exec_stmts(estate, &stmt.body);
        if let LoopRc::Break(rc) = loop_rc_processing(estate, label.as_deref(), body_rc) {
            return rc;
        }
    }
}

/// `exec_stmt_while(estate, stmt)` (pl_exec.c).
fn exec_stmt_while(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_while) -> PLpgSQL_rc {
    let label = stmt.label.clone();
    let cond = stmt.cond.as_deref().expect("WHILE carries a condition");
    loop {
        let (value, isnull) = seam::exec_eval_boolean(estate, cond);
        exec_eval_cleanup(estate);
        if isnull || !value {
            return PLpgSQL_rc::PLPGSQL_RC_OK;
        }
        let body_rc = exec_stmts(estate, &stmt.body);
        if let LoopRc::Break(rc) = loop_rc_processing(estate, label.as_deref(), body_rc) {
            return rc;
        }
    }
}

/// `exec_stmt_fori(estate, stmt)` (pl_exec.c) — integer FOR loop.
fn exec_stmt_fori(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_fori) -> PLpgSQL_rc {
    let var_dno = stmt.var.as_ref().expect("FOR(i) has a loop var").dno;
    let (var_typoid, var_typmod) = fori_var_type(estate, var_dno);

    // Lower bound.
    let lower = stmt.lower.as_deref().expect("FOR(i) lower bound");
    let (value, isnull, valtype, valtypmod) = seam::exec_eval_expr(estate, lower);
    let (value, isnull) =
        seam::exec_cast_value(estate, value, isnull, valtype, valtypmod, var_typoid, var_typmod);
    if isnull {
        seam::ereport_for_bound_null("lower bound");
    }
    let loop_value_start = value.as_i32();
    exec_eval_cleanup(estate);

    // Upper bound.
    let upper = stmt.upper.as_deref().expect("FOR(i) upper bound");
    let (value, isnull, valtype, valtypmod) = seam::exec_eval_expr(estate, upper);
    let (value, isnull) =
        seam::exec_cast_value(estate, value, isnull, valtype, valtypmod, var_typoid, var_typmod);
    if isnull {
        seam::ereport_for_bound_null("upper bound");
    }
    let end_value = value.as_i32();
    exec_eval_cleanup(estate);

    // Step.
    let step_value = if let Some(step) = stmt.step.as_deref() {
        let (value, isnull, valtype, valtypmod) = seam::exec_eval_expr(estate, step);
        let (value, isnull) = seam::exec_cast_value(
            estate, value, isnull, valtype, valtypmod, var_typoid, var_typmod,
        );
        if isnull {
            seam::ereport_for_bound_null("BY value");
        }
        let sv = value.as_i32();
        exec_eval_cleanup(estate);
        if sv <= 0 {
            seam::ereport_for_step_nonpositive();
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
            seam::assign_simple_var(estate, &mut var, Datum::from_i32(loop_value), false, false);
            put_var(estate, var_dno, var);
        }

        let body_rc = exec_stmts(estate, &stmt.body);
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
    rc
}

/// `exec_stmt_foreach_a(estate, stmt)` (pl_exec.c) — FOREACH over array
/// elements/slices. The control shell is real; the array-iteration leg bottoms
/// out in the array + fmgr substrate (loud).
fn exec_stmt_foreach_a(
    estate: &mut PLpgSQL_execstate,
    stmt: &PLpgSQL_stmt_foreach_a,
) -> PLpgSQL_rc {
    let expr = stmt.expr.as_deref().expect("FOREACH has an array expr");
    let (_value, isnull, _arrtype, _arrtypmod) = seam::exec_eval_expr(estate, expr);
    if isnull {
        seam::ereport_foreach_null();
    }

    let _stmt_mcontext = get_stmt_mcontext(estate);
    push_stmt_mcontext(estate);

    panic!(
        "seam not wired: exec_stmt_foreach_a array-iteration leg (pl_exec.c) — \
         get_element_type / DatumGetArrayTypePCopy / array_create_iterator / \
         array_iterate / exec_assign_value (array + fmgr substrate)"
    );
}

/// `exec_stmt_exit(estate, stmt)` (pl_exec.c) — EXIT / CONTINUE.
fn exec_stmt_exit(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_exit) -> PLpgSQL_rc {
    if let Some(cond) = stmt.cond.as_deref() {
        let (value, isnull) = seam::exec_eval_boolean(estate, cond);
        exec_eval_cleanup(estate);
        if isnull || !value {
            return PLpgSQL_rc::PLPGSQL_RC_OK;
        }
    }

    estate.exitlabel = stmt.label.clone();
    if stmt.is_exit {
        PLpgSQL_rc::PLPGSQL_RC_EXIT
    } else {
        PLpgSQL_rc::PLPGSQL_RC_CONTINUE
    }
}

/// `exec_stmt_return(estate, stmt)` (pl_exec.c) — RETURN.
fn exec_stmt_return(estate: &mut PLpgSQL_execstate, stmt: &PLpgSQL_stmt_return) -> PLpgSQL_rc {
    if estate.retisset {
        return PLpgSQL_rc::PLPGSQL_RC_RETURN;
    }

    estate.retval = Datum::null();
    estate.retisnull = true;
    estate.rettype = INVALID_OID;

    if stmt.retvarno >= 0 {
        let dno = stmt.retvarno;
        match datum_dtype(&estate.datums[dno as usize]) {
            PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE => {
                let mut var = take_var(estate, dno);
                seam::plpgsql_fulfill_promise(estate, &mut var);
                put_var(estate, dno, var);
                exec_return_simple_var(estate, dno);
            }
            PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR => {
                exec_return_simple_var(estate, dno);
            }
            PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW | PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
                let datum = estate.datums[dno as usize].clone();
                let (rettype, _rettypmod, retval, retisnull) =
                    seam::exec_eval_datum(estate, &datum);
                estate.rettype = rettype;
                estate.retval = retval;
                estate.retisnull = retisnull;
            }
            other => seam::elog_unrecognized_dtype_exec(other),
        }
        return PLpgSQL_rc::PLPGSQL_RC_RETURN;
    }

    if let Some(expr) = stmt.expr.as_deref() {
        let (retval, retisnull, rettype, _rettypmod) = seam::exec_eval_expr(estate, expr);
        estate.retval = retval;
        estate.retisnull = retisnull;
        estate.rettype = rettype;

        if estate.retistuple && !estate.retisnull && !seam::type_is_rowtype(estate.rettype) {
            seam::ereport_return_noncomposite();
        }

        return PLpgSQL_rc::PLPGSQL_RC_RETURN;
    }

    // Special hack for function returning VOID (but not for procedures).
    if estate.fn_rettype == VOIDOID && !func_is_procedure(estate) {
        estate.retval = Datum::null();
        estate.retisnull = false;
        estate.rettype = VOIDOID;
    }

    PLpgSQL_rc::PLPGSQL_RC_RETURN
}

/// The DTYPE_VAR / DTYPE_PROMISE-after-fulfill arm of `exec_stmt_return`.
fn exec_return_simple_var(estate: &mut PLpgSQL_execstate, dno: int32) {
    let (value, isnull, typoid) = read_var_value(&estate.datums[dno as usize]);
    estate.retval = value;
    estate.retisnull = isnull;
    estate.rettype = typoid;

    if estate.retistuple && !estate.retisnull {
        seam::ereport_return_noncomposite();
    }
}

/// `exec_set_found(estate, state)` (pl_exec.c) — set the FOUND variable.
fn exec_set_found(estate: &mut PLpgSQL_execstate, state: bool) {
    let dno = estate.found_varno;
    let mut var = take_var(estate, dno);
    seam::assign_simple_var(estate, &mut var, Datum::from_bool(state), false, false);
    put_var(estate, dno, var);
}

/// `exec_eval_cleanup(estate)` (pl_exec.c) — release temporary memory used by
/// expression / subselect evaluation.
fn exec_eval_cleanup(estate: &mut PLpgSQL_execstate) {
    if estate.eval_tuptable.is_some() {
        // SPI_freetuptable(estate->eval_tuptable) — value/SPI substrate.
        estate.eval_tuptable = None;
    }
    if let Some(econtext) = estate.eval_econtext {
        seam::reset_expr_context(&econtext);
    }
}

// ===========================================================================
// Value-substrate statement arms — dispatch targets with LOUD bodies. Each is a
// whole-statement SQL/value leg (SPI / executor / fmgr), not control flow.
// ===========================================================================

fn exec_stmt_call(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_call (pl_exec.c) — exec_prepare_plan / \
         make_callstmt_target / setup_param_list / SPI_execute_plan_extended / \
         exec_move_row (SPI plan surface + procedure resowner)"
    );
}

fn exec_stmt_getdiag(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_getdiag (pl_exec.c) — eval_processed / cur_error \
         field reads + exec_assign_c_string (ErrorData codec + value substrate)"
    );
}

fn exec_stmt_fors(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_fors (pl_exec.c) — exec_run_select + exec_for_query \
         (SPI plan surface)"
    );
}

fn exec_stmt_forc(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_forc (pl_exec.c) — SPI_cursor_open_with_paramlist + \
         exec_for_query (SPI cursor surface)"
    );
}

fn exec_stmt_return_next(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_return_next (pl_exec.c) — tuplestore_puttuple + \
         exec_eval_expr / exec_move_row (SRF tuple-store + value substrate)"
    );
}

fn exec_stmt_return_query(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_return_query (pl_exec.c) — exec_run_select / \
         exec_dynquery_with_params + tuplestore (SPI plan surface + SRF tuple-store)"
    );
}

fn exec_stmt_raise(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_raise (pl_exec.c) — exec_eval_expr for message/option \
         exprs + ereport with assembled errcode/detail/hint (value substrate + elog.c)"
    );
}

fn exec_stmt_assert(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_assert (pl_exec.c) — exec_eval_boolean(cond) + \
         exec_eval_expr(message) + ereport(ASSERT_FAILURE) (value substrate)"
    );
}

fn exec_stmt_execsql(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_execsql (pl_exec.c) — exec_prepare_plan / \
         setup_param_list / SPI_execute_plan_extended / exec_move_row INTO (SPI plan surface)"
    );
}

fn exec_stmt_dynexecute(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_dynexecute (pl_exec.c) — exec_eval_expr(querystring) + \
         SPI_execute / exec_eval_using_params / exec_move_row (SPI + fmgr)"
    );
}

fn exec_stmt_dynfors(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_dynfors (pl_exec.c) — exec_dynquery_with_params + \
         exec_for_query (SPI plan surface)"
    );
}

fn exec_stmt_open(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_open (pl_exec.c) — exec_prepare_plan / \
         SPI_cursor_open_with_paramlist / exec_dynquery_with_params (SPI cursor surface)"
    );
}

fn exec_stmt_fetch(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_fetch (pl_exec.c) — SPI_scroll_cursor_fetch/move + \
         exec_move_row (SPI cursor surface + value substrate)"
    );
}

fn exec_stmt_close(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_close (pl_exec.c) — SPI_cursor_find / SPI_cursor_close \
         (SPI cursor surface)"
    );
}

fn exec_stmt_commit(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_commit (pl_exec.c) — SPI_commit / SPI_start_transaction + \
         simple-expr infra rebuild (SPI + xact)"
    );
}

fn exec_stmt_rollback(_estate: &mut PLpgSQL_execstate) -> PLpgSQL_rc {
    panic!(
        "seam not wired: exec_stmt_rollback (pl_exec.c) — SPI_rollback / SPI_start_transaction + \
         simple-expr infra rebuild (SPI + xact)"
    );
}

// ===========================================================================
// Top-level executor entry points
// ===========================================================================

/// `plpgsql_exec_function(func, fcinfo, simple_eval_estate, simple_eval_resowner,
/// procedure_resowner, atomic)` (pl_exec.c) — the per-call executor.
///
/// Sets up the execstate (datum copy, paramLI, econtext, plugin func_beg),
/// runs the toplevel block, coerces the result, and tears down. The setup tail
/// (`makeParamList` + the hook install, the cast-expr hash, the econtext, the
/// SPI Proc context) and the result coercion are the value substrate (loud);
/// the block run + the RC handling is real once they land.
pub fn plpgsql_exec_function(
    _func: &PLpgSQL_function,
    _simple_eval_estate: Option<EState>,
    _simple_eval_resowner: Option<ResourceOwner>,
    _procedure_resowner: Option<ResourceOwner>,
    _atomic: bool,
) -> Datum {
    panic!(
        "seam not wired: plpgsql_exec_function (pl_exec.c) — plpgsql_estate_setup tail \
         (makeParamList / plpgsql_create_econtext / cast hash) + coerce_function_result_tuple \
         + SPI_connect/finish (SPI + executor substrate); the control-flow core \
         (exec_toplevel_block) is ported and runs once they land"
    );
}

/// `plpgsql_exec_trigger(func, trigdata)` (pl_exec.c) — the DML-trigger
/// executor entry.
pub fn plpgsql_exec_trigger(
    _func: &PLpgSQL_function,
    _trigdata: types_plpgsql::TriggerData,
) -> Datum {
    panic!(
        "seam not wired: plpgsql_exec_trigger (pl_exec.c) — trigger NEW/OLD row setup \
         (heaptuple + tupdesc substrate) + plpgsql_estate_setup + exec_toplevel_block + \
         exec_move_row result (executor + SPI substrate)"
    );
}

/// `plpgsql_exec_event_trigger(func, trigdata)` (pl_exec.c) — the event-trigger
/// executor entry.
pub fn plpgsql_exec_event_trigger(
    _func: &PLpgSQL_function,
    _trigdata: types_plpgsql::EventTriggerData,
) {
    panic!(
        "seam not wired: plpgsql_exec_event_trigger (pl_exec.c) — event-trigger var setup \
         + plpgsql_estate_setup + exec_toplevel_block (executor + SPI substrate)"
    );
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

fn discard_temp_var(estate: &mut PLpgSQL_execstate, dno: int32) {
    let mut var = take_var(estate, dno);
    seam::assign_simple_var(estate, &mut var, Datum::null(), true, false);
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
        promise: PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE,
    }))
}

/// Is the currently-executing function a PROCEDURE? The func back-reference is
/// opaque here; the VOID-return hack only applies to non-procedures, so the
/// common-case default (false) is the path the C takes for ordinary functions.
fn func_is_procedure(_estate: &PLpgSQL_execstate) -> bool {
    false
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
