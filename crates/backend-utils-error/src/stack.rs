//! The errordata stack and the errstart/errfinish reporting cycle.
//!
//! This is elog.c's core: a small per-thread stack of in-flight `ErrorData`
//! records (re-entrant reports during message construction get their own
//! frame), a recursion-depth guard against errors raised while processing an
//! error, and the errfinish dispatch that emits the report and performs the
//! per-level recovery action.
//!
//! Divergence (sanctioned, see the crate docs): at ERROR level `errfinish`
//! pops the frame and returns `Err(PgError)` instead of `PG_RE_THROW()`'s
//! `siglongjmp`.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::io::Write;
use std::cell::Cell;

use types_dest::CommandDest;
use types_error::{
    ErrorField, ErrorLocation, ErrorLevel, PgError, PgResult, SqlState, ERROR, FATAL, PANIC,
};

use crate::{config, errno, policy, report, sink};

/// `ERRORDATA_STACK_SIZE` (elog.c): the re-entrant ErrorData stack depth.
pub const ERRORDATA_STACK_SIZE: usize = 5;

pub(crate) struct Frame {
    pub error: PgError,
    pub output_to_server: bool,
    pub output_to_client: bool,
}

struct StackState {
    frames: Vec<Frame>,
    /// `recursion_depth` (elog.c): detects actual recursion (an error raised
    /// while elog.c routines are processing another error).
    recursion_depth: i32,
}

thread_local! {
    static STACK: RefCell<StackState> = RefCell::new(StackState {
        frames: Vec::new(),
        recursion_depth: 0,
    });
}

/// The recursion-trouble fallback `debug_query_string = NULL`: the query
/// string is owned by tcop (behind the context provider), so suppression is
/// recorded here and honored by `check_log_of_query`. tcop clears it when it
/// installs a new statement.
thread_local! { static STATEMENT_SUPPRESSED: Cell<bool> = const { Cell::new(false) }; }

pub(crate) fn statement_suppressed() -> bool {
    STATEMENT_SUPPRESSED.with(Cell::get)
}

pub fn reset_statement_suppressed() {
    STATEMENT_SUPPRESSED.with(|c| c.set(false));
}

fn errstart_not_called() -> PgError {
    // CHECK_STACK_DEPTH(): ereport(ERROR, errmsg_internal("errstart was not called"))
    PgError::error("errstart was not called")
}

/// `in_error_recursion_trouble` — are we at risk of infinite error recursion?
/// Pull the plug if we recurse more than once.
pub fn in_error_recursion_trouble() -> bool {
    STACK.with(|s| s.borrow().recursion_depth > 2)
}

/// `errstart` — begin an error-reporting cycle.
///
/// Returns `true` in the normal case; `false` to short-circuit the report (a
/// warning or lower that is not to be reported anywhere).
pub fn errstart(elevel: ErrorLevel, domain: Option<&str>) -> bool {
    let mut elevel = elevel;

    // Promote into a more severe error where required.
    if elevel >= ERROR {
        // Inside a critical section, all errors become PANIC errors.
        if config::crit_section_count() > 0 {
            elevel = PANIC;
        }

        // Reasons for treating ERROR as FATAL: ExitOnAnyError (initdb), or the
        // error occurred after proc_exit began to run. (The third C reason —
        // PG_exception_stack == NULL, i.e. no handler — is part of the
        // sigsetjmp machinery this port replaces with PgResult propagation;
        // see the crate docs.)
        if elevel == ERROR && (config::exit_on_any_error() || config::proc_exit_inprogress()) {
            elevel = FATAL;
        }

        // Don't allow a stacked FATAL/PANIC in progress to be downgraded by
        // this lower-grade interruption.
        STACK.with(|s| {
            for frame in &s.borrow().frames {
                if frame.error.level > elevel {
                    elevel = frame.error.level;
                }
            }
        });
    }

    // Decide whether to process this report at all.
    let output_to_server = policy::should_output_to_server(elevel);
    let output_to_client = policy::should_output_to_client(elevel);
    if elevel < ERROR && !output_to_server && !output_to_client {
        return false;
    }

    // (The C "ErrorContext == NULL -> hard exit(2)" check guards the palloc
    // arena; the owned model has no arena to be missing.)

    let overflow = STACK.with(|s| {
        let mut st = s.borrow_mut();

        // Error during error processing?
        st.recursion_depth += 1;
        if st.recursion_depth > 1 && elevel >= ERROR {
            // C resets ErrorContext (the allocation arena) here; no arena in
            // the owned model. The infinite-recursion fallbacks do apply:
            if st.recursion_depth > 2 {
                // in_error_recursion_trouble(): abandon statement logging — it
                // could be the source of the recursive failure. (C also clears
                // error_context_stack; that chain is retired in favor of
                // attach-on-propagation, see the crate docs.)
                STATEMENT_SUPPRESSED.with(|c| c.set(true));
            }
        }

        // get_error_stack_entry()
        if st.frames.len() >= ERRORDATA_STACK_SIZE {
            // Stack not big enough: make room and PANIC (below, outside the
            // borrow). C leaves recursion_depth elevated; PANIC never returns.
            st.frames.clear();
            return true;
        }

        let mut error = PgError::new(elevel, String::new());
        // Save errno immediately so error parameter evaluation can't change it.
        error.saved_errno = Some(errno::current_errno());
        // set_stack_entry_domain(): the default text domain is the backend's.
        let domain = domain.unwrap_or("postgres");
        error.domain = Some(domain.to_owned());
        error.context_domain = Some(domain.to_owned());
        // (PgError::new already selected the default errcode based on elevel,
        // exactly as errstart does.)

        st.frames.push(Frame {
            error,
            output_to_server,
            output_to_client,
        });
        st.recursion_depth -= 1;
        false
    });

    if overflow {
        // ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")))
        let _ = ThrowErrorData(PgError::new(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
        // PANIC aborts; not reached.
        std::process::abort();
    }

    true
}

/// `errstart_cold` — identical to [`errstart`]; the C version only adds a
/// compiler hot/cold-splitting hint.
pub fn errstart_cold(elevel: ErrorLevel, domain: Option<&str>) -> bool {
    errstart(elevel, domain)
}

/// `set_stack_entry_location`: keep only the base name of `__FILE__` (both
/// slash directions, as some Windows compilers emit backslashes).
fn normalize_filename(filename: &str) -> &str {
    let filename = match filename.rfind('/') {
        Some(pos) => &filename[pos + 1..],
        None => filename,
    };
    match filename.rfind('\\') {
        Some(pos) => &filename[pos + 1..],
        None => filename,
    }
}

/// `errfinish` — end an error-reporting cycle: produce the report(s) and pop
/// the error stack. At ERROR returns `Err` (the C longjmp); at FATAL runs
/// proc_exit(1); at PANIC aborts; below ERROR emits and returns `Ok(())`.
pub fn errfinish(filename: Option<&str>, lineno: i32, funcname: Option<&str>) -> PgResult<()> {
    // recursion_depth++; CHECK_STACK_DEPTH(); save the last bits of state.
    let prepared = STACK.with(|s| {
        let mut st = s.borrow_mut();
        if st.frames.is_empty() {
            return None;
        }
        st.recursion_depth += 1;
        let top = st.frames.last_mut().expect("frame checked above");
        top.error.location = Some(ErrorLocation {
            filename: filename.map(|f| normalize_filename(f).to_owned()),
            lineno,
            funcname: funcname.map(str::to_owned),
        });
        Some((top.error.level, top.error.backtrace.is_none()))
    });
    let Some((elevel, backtrace_unset)) = prepared else {
        return Err(errstart_not_called());
    };

    // Collect backtrace, if enabled and we didn't already.
    if backtrace_unset {
        if let Some(funcname) = funcname {
            if config::matches_backtrace_functions(funcname) {
                with_current_mut_unchecked(|error| report::set_backtrace(error, 2));
            }
        }
    }

    // (C walks error_context_stack here; context now attaches on propagation
    // via PgError::add_context — see the crate docs.)

    // If ERROR (not more nor less), hand it to the handler: pop the frame and
    // return it as Err (divergence from C's PG_RE_THROW; see crate docs).
    if elevel == ERROR {
        // Minimal cleanup so handlers run in a sane state. C also zeroes
        // InterruptHoldoffCount / QueryCancelHoldoffCount; those globals
        // belong to the interrupt machinery and are reset by the catching
        // recovery block in this model.
        config::set_crit_section_count(0);

        let error = pop_top_frame();
        return Err(error);
    }

    // Emit the message to the right places.
    emit_top_frame();

    // Free the stack entry.
    let _ = pop_top_frame();

    // Perform error recovery action as specified by elevel.
    if elevel == FATAL {
        // If we just reported a startup failure, the client will disconnect
        // on receiving it, so don't send any more to the client. (The C gate
        // `PG_exception_stack == NULL` is subsumed by the divergence above.)
        if config::where_to_send_output() == CommandDest::Remote {
            config::set_where_to_send_output(CommandDest::None);
        }

        // fflush(NULL): improve the odds the message is seen if proc_exit crashes.
        flush_all();

        // Let the cumulative stats system know. (Only marks the session as
        // terminated by fatal error if there is no other known cause.)
        backend_utils_activity_stat_seams::pgstat_set_session_end_cause_fatal::call();

        // Normal process-exit cleanup, exit code 1 for FATAL termination.
        backend_storage_ipc_dsm_core_seams::proc_exit::call(
            1,
            backend_utils_init_small_seams::my_proc_pid::call(),
        );
    }

    if elevel >= PANIC {
        flush_all();
        std::process::abort();
    }

    // C ends with CHECK_FOR_INTERRUPTS(); the interrupt machinery (miscadmin/
    // tcop) owns that and is documented as the caller's responsibility here.
    Ok(())
}

fn flush_all() {
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
}

fn pop_top_frame() -> PgError {
    STACK.with(|s| {
        let mut st = s.borrow_mut();
        let frame = st.frames.pop().expect("pop_top_frame on empty stack");
        st.recursion_depth -= 1;
        frame.error
    })
}

/// Emit the top-of-stack report (the body shared by `errfinish` and the
/// public `EmitErrorReport`): reset the formatted timestamps, run
/// `emit_log_hook`, then dispatch to the server log and/or the client.
fn emit_top_frame() {
    let (error, mut output_to_server, output_to_client) = STACK.with(|s| {
        let st = s.borrow();
        let top = st.frames.last().expect("emit_top_frame on empty stack");
        (top.error.clone(), top.output_to_server, top.output_to_client)
    });

    // Reset the formatted timestamp fields before emitting any logs, so all
    // destinations (and the hook) observe one consistent timestamp.
    report::reset_formatted_log_time();

    // The hook may only turn output_to_server off; recheck afterward.
    if output_to_server {
        sink::call_emit_log_hook(&error, &mut output_to_server);
    }

    if output_to_server {
        report::send_message_to_server_log(&error);
    }

    if output_to_client {
        report::send_message_to_frontend(&error);
    }
}

/// `EmitErrorReport` — actual output of the top-of-stack error message. In
/// the ereport(ERROR) case the C version is called from PostgresMain; with
/// the PgResult divergence the recovery block instead holds the `Err` value
/// and calls [`emit_error_report_for`].
pub fn EmitErrorReport() -> PgResult<()> {
    let has_frame = STACK.with(|s| {
        let mut st = s.borrow_mut();
        if st.frames.is_empty() {
            return false;
        }
        st.recursion_depth += 1;
        true
    });
    if !has_frame {
        return Err(errstart_not_called());
    }
    emit_top_frame();
    STACK.with(|s| s.borrow_mut().recursion_depth -= 1);
    Ok(())
}

/// Emit a caught (already-popped) ERROR the way PostgresMain's recovery block
/// calls `EmitErrorReport()`. Output decisions are recomputed from the current
/// config, exactly as `pg_re_throw` recomputes them on severity change.
pub fn emit_error_report_for(error: &PgError) {
    report::reset_formatted_log_time();
    let mut output_to_server = policy::should_output_to_server(error.level);
    let output_to_client = policy::should_output_to_client(error.level);
    if output_to_server {
        sink::call_emit_log_hook(error, &mut output_to_server);
    }
    if output_to_server {
        report::send_message_to_server_log(error);
    }
    if output_to_client {
        report::send_message_to_frontend(error);
    }
}

// ---------------------------------------------------------------------------
// Current-frame mutators (the C error-data-supplying functions: errcode,
// errmsg, errdetail, ...). Each returns Err("errstart was not called") when
// no report is in flight, mirroring CHECK_STACK_DEPTH.
// ---------------------------------------------------------------------------

fn with_current<R>(f: impl FnOnce(&PgError) -> R) -> PgResult<R> {
    STACK.with(|s| {
        let st = s.borrow();
        let frame = st.frames.last().ok_or_else(errstart_not_called)?;
        Ok(f(&frame.error))
    })
}

fn with_current_mut(f: impl FnOnce(&mut PgError)) -> PgResult<()> {
    STACK.with(|s| {
        let mut st = s.borrow_mut();
        let frame = st.frames.last_mut().ok_or_else(errstart_not_called)?;
        f(&mut frame.error);
        Ok(())
    })
}

fn with_current_mut_unchecked(f: impl FnOnce(&mut PgError)) {
    STACK.with(|s| {
        let mut st = s.borrow_mut();
        if let Some(frame) = st.frames.last_mut() {
            f(&mut frame.error);
        }
    });
}

/// `errcode` — add a SQLSTATE error code to the current error.
pub fn errcode(sqlerrcode: SqlState) -> PgResult<()> {
    with_current_mut(|error| error.sqlstate = sqlerrcode)
}

/// `errcode_for_file_access` — SQLSTATE from the saved errno, assuming the
/// failing operation was a disk file access.
pub fn errcode_for_file_access() -> PgResult<()> {
    with_current_mut(|error| {
        error.sqlstate = errno::sqlstate_for_file_access(error.saved_errno.unwrap_or(0));
    })
}

/// `errcode_for_socket_access` — SQLSTATE from the saved errno, assuming the
/// failing operation was a socket access.
pub fn errcode_for_socket_access() -> PgResult<()> {
    with_current_mut(|error| {
        error.sqlstate = errno::sqlstate_for_socket_access(error.saved_errno.unwrap_or(0));
    })
}

/// `errmsg` — set the primary message (caller pre-formats; `%m` expands from
/// the frame's saved errno, as `EVALUATE_MESSAGE` restores errno before
/// formatting). Also records the message id.
pub fn errmsg(message: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.message_id = Some(message.to_owned());
        error.message = errno::replace_percent_m(message, error.saved_errno.unwrap_or(0));
    })
}

/// `errmsg_internal` — like `errmsg` but never translated / no message id.
pub fn errmsg_internal(message: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.message_id = Some(message.to_owned());
        error.message = errno::replace_percent_m(message, error.saved_errno.unwrap_or(0));
    })
}

/// `errmsg_plural` — primary message with pluralization (`dngettext` picks by
/// n; without NLS that is the n == 1 test). The message id is always the
/// singular form, as in C (`edata->message_id = fmt_singular`).
pub fn errmsg_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    let picked = if n == 1 { fmt_singular } else { fmt_plural };
    with_current_mut(|error| {
        error.message_id = Some(fmt_singular.to_owned());
        error.message = errno::replace_percent_m(picked, error.saved_errno.unwrap_or(0));
    })
}

/// `errdetail` — add a detail message to the current error.
pub fn errdetail(detail: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.detail = Some(errno::replace_percent_m(detail, error.saved_errno.unwrap_or(0)));
    })
}

/// `errdetail_internal` — like `errdetail`, untranslated.
pub fn errdetail_internal(detail: &str) -> PgResult<()> {
    errdetail(detail)
}

/// `errdetail_log` — add a detail_log message (server log only).
pub fn errdetail_log(detail_log: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.detail_log = Some(errno::replace_percent_m(
            detail_log,
            error.saved_errno.unwrap_or(0),
        ));
    })
}

/// `errdetail_log_plural`.
pub fn errdetail_log_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    errdetail_log(if n == 1 { fmt_singular } else { fmt_plural })
}

/// `errdetail_plural`.
pub fn errdetail_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    errdetail(if n == 1 { fmt_singular } else { fmt_plural })
}

/// `errhint` — add a hint message to the current error.
pub fn errhint(hint: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.hint = Some(errno::replace_percent_m(hint, error.saved_errno.unwrap_or(0)));
    })
}

/// `errhint_internal`.
pub fn errhint_internal(hint: &str) -> PgResult<()> {
    errhint(hint)
}

/// `errhint_plural`.
pub fn errhint_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    errhint(if n == 1 { fmt_singular } else { fmt_plural })
}

/// `errcontext_msg` — add a context line; multiple calls stack up,
/// newline-joined, earlier calls being more closely nested.
pub fn errcontext_msg(context: &str) -> PgResult<()> {
    with_current_mut(|error| {
        let line = errno::replace_percent_m(context, error.saved_errno.unwrap_or(0));
        error.add_context_line(line);
    })
}

/// `set_errcontext_domain` — set the message domain used by errcontext().
pub fn set_errcontext_domain(domain: Option<&str>) -> PgResult<()> {
    with_current_mut(|error| {
        error.context_domain = Some(domain.unwrap_or("postgres").to_owned());
    })
}

/// `errhidestmt` — optionally suppress the STATEMENT: field of the log entry.
pub fn errhidestmt(hide_stmt: bool) -> PgResult<()> {
    with_current_mut(|error| error.hide_statement = hide_stmt)
}

/// `errhidecontext` — optionally suppress the CONTEXT: field of the log entry.
pub fn errhidecontext(hide_ctx: bool) -> PgResult<()> {
    with_current_mut(|error| error.hide_context = hide_ctx)
}

/// `errbacktrace` — attach a backtrace to the containing report.
pub fn errbacktrace() -> PgResult<()> {
    with_current_mut(|error| report::set_backtrace(error, 1))
}

/// `errposition` — add a cursor position to the current error.
pub fn errposition(cursorpos: i32) -> PgResult<()> {
    with_current_mut(|error| error.cursor_position = nonzero(cursorpos))
}

/// `internalerrposition` — add an internal cursor position.
pub fn internalerrposition(cursorpos: i32) -> PgResult<()> {
    with_current_mut(|error| error.internal_position = nonzero(cursorpos))
}

/// `internalerrquery` — add (or with `None`, drop) the internal query text.
pub fn internalerrquery(query: Option<&str>) -> PgResult<()> {
    with_current_mut(|error| error.internal_query = query.map(str::to_owned))
}

/// `err_generic_string` — set one PG_DIAG_xxx string field on the current
/// error; unknown fields elog(ERROR).
pub fn err_generic_string(field: ErrorField, value: &str) -> PgResult<()> {
    let mut result = Ok(());
    with_current_mut(|error| result = error.set_error_field(field, value))?;
    result
}

/// `geterrcode` — the currently set SQLSTATE (error callbacks only).
pub fn geterrcode() -> PgResult<SqlState> {
    with_current(|error| error.sqlstate)
}

/// `geterrposition` — the currently set error position (0 if none).
pub fn geterrposition() -> PgResult<i32> {
    with_current(|error| error.cursor_position.unwrap_or(0))
}

/// `getinternalerrposition` — same for the internal error position.
pub fn getinternalerrposition() -> PgResult<i32> {
    with_current(|error| error.internal_position.unwrap_or(0))
}

fn nonzero(position: i32) -> Option<i32> {
    (position != 0).then_some(position)
}

// ---------------------------------------------------------------------------
// Whole-error operations
// ---------------------------------------------------------------------------

/// `CopyErrorData` — obtain a copy of the topmost error stack entry (error
/// handler code only).
pub fn CopyErrorData() -> PgResult<PgError> {
    with_current(PgError::clone)
}

/// `FreeErrorData` — free the structure returned by `CopyErrorData`. Owned
/// values drop themselves; this consumes for API parity.
pub fn FreeErrorData(_edata: PgError) {}

/// `FlushErrorState` — flush the error state after error recovery. You are
/// not "out" of the error subsystem until you have done this.
pub fn FlushErrorState() {
    STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.frames.clear();
        st.recursion_depth = 0;
    });
}

/// `ThrowErrorData` — report an error described by an ErrorData value that
/// isn't on the stack: errstart, copy the supplied fields, errfinish.
///
/// This is also the implementation behind the `ereport` seam: a fully-built
/// `PgError` enters the same errstart/errfinish cycle a C `ereport(elevel,
/// (...))` does, including severity promotion and the recursion guard.
pub fn ThrowErrorData(edata: PgError) -> PgResult<()> {
    if !errstart(edata.level, edata.domain.as_deref()) {
        return Ok(()); // error is not to be reported at all
    }

    STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.recursion_depth += 1;
        let frame = st.frames.last_mut().expect("errstart pushed a frame");
        let new = &mut frame.error;

        // Copy the supplied fields onto the stack entry. The frame keeps the
        // elevel errstart computed (promotion) and its output decisions.
        if edata.sqlstate.0 != 0 {
            new.sqlstate = edata.sqlstate;
        }
        if !edata.message.is_empty() {
            new.message = edata.message;
        }
        new.detail = edata.detail;
        new.detail_log = edata.detail_log;
        new.hint = edata.hint;
        new.context = edata.context;
        new.backtrace = edata.backtrace;
        if edata.message_id.is_some() {
            new.message_id = edata.message_id;
        }
        if edata.context_domain.is_some() {
            new.context_domain = edata.context_domain;
        }
        new.schema_name = edata.schema_name;
        new.table_name = edata.table_name;
        new.column_name = edata.column_name;
        new.datatype_name = edata.datatype_name;
        new.constraint_name = edata.constraint_name;
        new.cursor_position = edata.cursor_position;
        new.internal_position = edata.internal_position;
        new.internal_query = edata.internal_query;
        if edata.saved_errno.is_some() {
            new.saved_errno = edata.saved_errno;
        }
        new.hide_statement = edata.hide_statement;
        new.hide_context = edata.hide_context;

        st.recursion_depth -= 1;
    });

    // Process the error.
    let location = edata.location;
    match location {
        Some(loc) => errfinish(loc.filename.as_deref(), loc.lineno, loc.funcname.as_deref()),
        None => errfinish(None, 0, None),
    }
}

/// `ReThrowError` — re-throw a previously copied error (handler did
/// CopyErrorData/FlushErrorState, processed, now re-throws).
pub fn ReThrowError<T>(edata: PgError) -> PgResult<T> {
    // Assert(edata->elevel == ERROR)
    if edata.level != ERROR {
        return Err(PgError::new(
            PANIC,
            "ReThrowError called with non-ERROR error data",
        ));
    }
    Err(edata)
}

/// `pg_re_throw` — re-throw the in-progress error to the next outer handler.
/// With PgResult propagation this pops the current frame and returns it as
/// `Err`; the C no-outer-handler FATAL promotion is part of the replaced
/// sigsetjmp machinery (see crate docs).
pub fn pg_re_throw<T>() -> PgResult<T> {
    let popped = STACK.with(|s| {
        let mut st = s.borrow_mut();
        let frame = st.frames.pop();
        if frame.is_some() {
            st.recursion_depth = (st.recursion_depth - 1).max(0);
        }
        frame
    });
    match popped {
        Some(frame) => Err(frame.error),
        // ExceptionalCondition("pg_re_throw tried to return")
        None => Err(PgError::new(PANIC, "pg_re_throw tried to return")),
    }
}

/// `GetErrorContextStack` — return the context stack, for display/diags.
///
/// Cranks up a scratch stack entry, sets its assoc_context to the caller's
/// memory context (no arena in the owned model, same elision as errstart), and
/// fires each registered `error_context_stack` callback so that the callbacks'
/// `errcontext()` calls accumulate into the scratch entry's `context` field.
/// The accumulated string is then returned and the scratch entry popped.
///
/// Under this crate's sanctioned divergence the `error_context_stack` callback
/// chain is retired in favor of attach-on-propagation (see the crate docs), so
/// the callback walk fires nothing — exactly as errfinish's callback walk is
/// elided. The C control flow (recursion_depth bracket, scratch entry, walk,
/// pop) is reproduced faithfully; the returned context is whatever the (empty)
/// chain produced, i.e. `None`.
pub fn GetErrorContextStack() -> Option<String> {
    // Crank up a stack entry to store the info in. recursion_depth is elevated
    // around the callbacks (feeds in_error_recursion_trouble), as in C.
    let overflow = STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.recursion_depth += 1;
        // get_error_stack_entry(): stack not big enough -> make room and PANIC.
        if st.frames.len() >= ERRORDATA_STACK_SIZE {
            st.frames.clear();
            return true;
        }
        // Zero-init scratch entry. assoc_context = CurrentMemoryContext has no
        // counterpart in the owned model (no palloc arena); the entry's context
        // String accumulates in-place.
        let mut error = PgError::new(types_error::LOG, String::new());
        error.saved_errno = Some(errno::current_errno());
        error.domain = Some("postgres".to_owned());
        error.context_domain = Some("postgres".to_owned());
        st.frames.push(Frame {
            error,
            output_to_server: false,
            output_to_client: false,
        });
        false
    });
    if overflow {
        // ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")))
        let _ = ThrowErrorData(PgError::new(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
        std::process::abort();
    }

    // Call any context callback functions to collect the context information
    // into the scratch entry. The error_context_stack chain is retired (see the
    // crate docs / divergence #10), so there are no callbacks to fire here.

    // Clean ourselves off the stack and decrement recursion depth, then return
    // the accumulated context string.
    STACK.with(|s| {
        let mut st = s.borrow_mut();
        let frame = st.frames.pop();
        st.recursion_depth -= 1;
        frame.and_then(|f| f.error.context)
    })
}

// ---------------------------------------------------------------------------
// Soft-error support (errsave_start / errsave_finish)
// ---------------------------------------------------------------------------

/// `errsave_start` — begin a "soft" error-reporting cycle. With no save
/// context this is exactly `errstart(ERROR, domain)`. With one, the
/// error_occurred flag is set; `false` is returned (skip everything) unless
/// details are wanted, in which case a frame is pushed at LOG level (the
/// "all is well" signal `errsave_finish` looks for).
pub fn errsave_start(context: Option<&mut types_error::SoftErrorContext>, domain: Option<&str>) -> bool {
    let Some(escontext) = context else {
        return errstart(ERROR, domain);
    };

    escontext.mark_error_occurred();
    if !escontext.details_wanted() {
        return false;
    }

    let overflow = STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.recursion_depth += 1;
        // get_error_stack_entry(): stack not big enough -> make room and PANIC.
        if st.frames.len() >= ERRORDATA_STACK_SIZE {
            st.frames.clear();
            return true;
        }
        let mut error = PgError::new(types_error::LOG, String::new());
        error.saved_errno = Some(errno::current_errno());
        let domain = domain.unwrap_or("postgres");
        error.domain = Some(domain.to_owned());
        error.context_domain = Some(domain.to_owned());
        // Select default errcode based on the assumed elevel of ERROR.
        error.sqlstate = types_error::ERRCODE_INTERNAL_ERROR;
        st.frames.push(Frame {
            error,
            output_to_server: false,
            output_to_client: false,
        });
        st.recursion_depth -= 1;
        false
    });
    if overflow {
        // ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")))
        let _ = ThrowErrorData(PgError::new(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
        // PANIC aborts; not reached.
        std::process::abort();
    }
    true
}

/// `errsave_finish` — end a "soft" error-reporting cycle: punt to errfinish
/// if errsave_start punted to errstart (frame level >= ERROR), else package
/// the details into the save context and pop.
pub fn errsave_finish(
    context: Option<&mut types_error::SoftErrorContext>,
    filename: Option<&str>,
    lineno: i32,
    funcname: Option<&str>,
) -> PgResult<()> {
    let top_level = STACK.with(|s| s.borrow().frames.last().map(|f| f.error.level));
    let Some(top_level) = top_level else {
        return Err(errstart_not_called());
    };

    // If errsave_start punted to errstart, elevel is ERROR or above: punt to
    // errfinish likewise.
    if top_level >= ERROR {
        return errfinish(filename, lineno, funcname);
    }

    // Package up the stack entry contents and deliver them to the caller.
    // (Backtrace and context callbacks are deliberately skipped here.)
    let mut error = STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.recursion_depth += 1;
        let frame = st.frames.pop().expect("frame checked above");
        st.recursion_depth -= 1;
        frame.error
    });
    error.location = Some(ErrorLocation {
        filename: filename.map(|f| normalize_filename(f).to_owned()),
        lineno,
        funcname: funcname.map(str::to_owned),
    });
    // Replace the LOG value that errsave_start inserted.
    error.level = ERROR;

    if let Some(escontext) = context {
        escontext.save(error);
    }
    Ok(())
}
