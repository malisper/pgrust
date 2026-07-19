//! The errordata stack and the errstart/errfinish reporting cycle.
//! Divergence (sanctioned): at ERROR level `errfinish` pops the frame and
//! returns `Err(PgError)` instead of `PG_RE_THROW()`'s siglongjmp.

#![allow(non_snake_case)]

use std::cell::Cell;
use std::cell::RefCell;
use std::io::Write;

use ::types_dest::CommandDest;
use ::types_error::{
    ErrorField, ErrorLevel, ErrorLocation, PgError, PgResult, SqlState, ERROR, FATAL, PANIC,
};

use crate::{config, errno, policy, report, sink};

pub const ERRORDATA_STACK_SIZE: usize = 5;

pub(crate) struct Frame {
    pub error: PgError,
    pub output_to_server: bool,
    pub output_to_client: bool,
}

struct StackState {
    frames: Vec<Frame>,
    recursion_depth: i32,
}

thread_local! {
    static STACK: RefCell<StackState> = RefCell::new(StackState {
        frames: Vec::new(),
        recursion_depth: 0,
    });
}

// Scoped replacement for `error_context_stack`'s emit-time decoration of
// non-ERROR reports (divergence #10): warnings/notices are emitted inline by
// `errfinish`, so an enclosing frame registers a callback that runs over the
// in-flight non-ERROR report before emission (innermost-first). The ERROR
// path is unaffected (context attaches on propagation).
thread_local! {
    static EMIT_CONTEXT_CALLBACKS: RefCell<Vec<EmitContextCallback>> = const {
        RefCell::new(Vec::new())
    };
}

struct EmitContextCallback {
    id: u64,
    callback: Box<dyn FnMut(&mut PgError)>,
}

thread_local! { static EMIT_CONTEXT_NEXT_ID: Cell<u64> = const { Cell::new(1) }; }

pub fn push_emit_context_callback(callback: Box<dyn FnMut(&mut PgError)>) -> u64 {
    let id = EMIT_CONTEXT_NEXT_ID.with(|n| {
        let id = n.get();
        n.set(id + 1);
        id
    });
    EMIT_CONTEXT_CALLBACKS.with(|s| s.borrow_mut().push(EmitContextCallback { id, callback }));
    id
}

pub fn pop_emit_context_callback(id: u64) {
    EMIT_CONTEXT_CALLBACKS.with(|s| {
        let mut st = s.borrow_mut();
        if let Some(pos) = st.iter().position(|c| c.id == id) {
            st.remove(pos);
        }
    });
}

fn run_emit_context_callbacks() {
    if EMIT_CONTEXT_CALLBACKS.with(|s| s.borrow().is_empty()) {
        return;
    }
    // Clone the in-flight error out, run the callbacks over the clone, then
    // write back: keeps the borrow non-overlapping with any error machinery
    // the callback itself might touch.
    let mut error = match STACK.with(|s| s.borrow().frames.last().map(|f| f.error.clone())) {
        Some(e) => e,
        None => return,
    };
    EMIT_CONTEXT_CALLBACKS.with(|s| {
        let mut st = s.borrow_mut();
        // innermost-registered first, as C walks error_context_stack.
        for entry in st.iter_mut().rev() {
            (entry.callback)(&mut error);
        }
    });
    STACK.with(|s| {
        if let Some(f) = s.borrow_mut().frames.last_mut() {
            f.error = error;
        }
    });
}

// The recursion-trouble fallback `debug_query_string = NULL`: the query
// string is owned by tcop (behind the context provider), so suppression is
// recorded here and honored by `check_log_of_query`.
thread_local! { static STATEMENT_SUPPRESSED: Cell<bool> = const { Cell::new(false) }; }

pub(crate) fn statement_suppressed() -> bool {
    STATEMENT_SUPPRESSED.with(Cell::get)
}

pub fn reset_statement_suppressed() {
    STATEMENT_SUPPRESSED.with(|c| c.set(false));
}

/// proc_exit_prepare's `debug_query_string = NULL` (the query string itself
/// is owned by tcop's log-context provider).
pub fn suppress_statement() {
    STATEMENT_SUPPRESSED.with(|c| c.set(true));
}

/// proc_exit_prepare's `error_context_stack = NULL`.
pub fn clear_emit_context_callbacks() {
    EMIT_CONTEXT_CALLBACKS.with(|s| s.borrow_mut().clear());
}

#[cold]
#[inline(never)]
fn errstart_not_called() -> PgError {
    // CHECK_STACK_DEPTH(): ereport(ERROR, errmsg_internal("errstart was not called"))
    PgError::error("errstart was not called")
}

pub fn in_error_recursion_trouble() -> bool {
    STACK.with(|s| s.borrow().recursion_depth > 2)
}

/// True when this thread carries no in-flight error state: no open error
/// frames and no emit-context callbacks. The session-envelope boundary
/// (harvested with SessionEnvelope Phase 0, elog side ported from
/// 9a8eff9b1) refuses to bind or unbind across a dirty error boundary.
pub fn error_stack_clean() -> bool {
    STACK.with(|s| s.borrow().frames.is_empty())
        && EMIT_CONTEXT_CALLBACKS.with(|s| s.borrow().is_empty())
}

#[cold]
#[inline(never)]
pub fn errstart(elevel: ErrorLevel, domain: Option<&str>) -> bool {
    let mut elevel = elevel;

    if elevel >= ERROR {
        if config::crit_section_count() > 0 {
            elevel = PANIC;
        }

        // ERROR -> FATAL: ExitOnAnyError (initdb), or proc_exit has begun.
        if elevel == ERROR && (config::exit_on_any_error() || config::proc_exit_inprogress()) {
            elevel = FATAL;
        }

        // Don't let a stacked FATAL/PANIC in progress be downgraded by this
        STACK.with(|s| {
            for frame in &s.borrow().frames {
                if frame.error.level > elevel {
                    elevel = frame.error.level;
                }
            }
        });
    }

    let output_to_server = policy::should_output_to_server(elevel);
    let output_to_client = policy::should_output_to_client(elevel);
    if elevel < ERROR && !output_to_server && !output_to_client {
        return false;
    }


    let overflow = STACK.with(|s| {
        let mut st = s.borrow_mut();

        st.recursion_depth += 1;
        if st.recursion_depth > 1 && elevel >= ERROR {
            if st.recursion_depth > 2 {
                // in_error_recursion_trouble(): abandon statement logging.
                STATEMENT_SUPPRESSED.with(|c| c.set(true));
            }
        }

        if st.frames.len() >= ERRORDATA_STACK_SIZE {
            st.frames.clear();
            return true;
        }

        let mut error = PgError::new(elevel, String::new());
        // The C-shaped stack API takes its location from errfinish's
        // arguments; drop the constructor's capture (it would point here).
        error.location = None;
        // Save errno immediately so error parameter eval can't change it.
        error.saved_errno = Some(errno::current_errno());
        let domain = domain.unwrap_or("postgres");
        error.domain = Some(domain.to_owned());
        error.context_domain = Some(domain.to_owned());
        // (PgError::new already selected the default errcode from elevel.)

        st.frames.push(Frame {
            error,
            output_to_server,
            output_to_client,
        });
        st.recursion_depth -= 1;
        false
    });

    if overflow {
        let _ = ThrowErrorData(PgError::new(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
        // PANIC unwinds the thread; not reached.
        std::process::abort();
    }

    true
}

#[cold]
#[inline(never)]
pub fn errstart_cold(elevel: ErrorLevel, domain: Option<&str>) -> bool {
    errstart(elevel, domain)
}

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

#[cold]
#[inline(never)]
pub fn errfinish(filename: Option<&str>, lineno: i32, funcname: Option<&str>) -> PgResult<()> {
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
        return Err(Box::new(errstart_not_called()));
    };

    if backtrace_unset {
        if let Some(funcname) = funcname {
            if config::matches_backtrace_functions(funcname) {
                with_current_mut_unchecked(|error| report::set_backtrace(error, 2));
            }
        }
    }

    if elevel == ERROR {
        // Minimal cleanup so handlers run in a sane state. C also zeroes
        // InterruptHoldoffCount / QueryCancelHoldoffCount; those belong to
        // the interrupt machinery and are reset by the catching block.
        config::set_crit_section_count(0);

        let error = pop_top_frame();
        return Err(Box::new(error));
    }

    run_emit_context_callbacks();

    emit_top_frame();

    let _ = pop_top_frame();

    // Perform error recovery action as specified by elevel.
    if elevel == FATAL {
        if config::where_to_send_output() == CommandDest::Remote {
            config::set_where_to_send_output(CommandDest::None);
        }

        flush_all();

        pgstat_seams::pgstat_set_session_end_cause_fatal::call();

        ipc_seams::proc_exit::call(1, init_small_seams::my_proc_pid::call());
    }

    if elevel >= PANIC {
        flush_all();
        // C abort(): under the thread model the catchable crash class — the
        // unwind escapes the backend thread and the postmaster runs the crash
        // choreography (notes/crash-restart-design.md).
        std::panic::panic_any(types_error::PanicExitThread);
    }

    // C ends with CHECK_FOR_INTERRUPTS(); the interrupt machinery owns that
    // and it is the caller's responsibility here.
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

#[cold]
#[inline(never)]
fn emit_top_frame() {
    let (error, mut output_to_server, output_to_client) = STACK.with(|s| {
        let st = s.borrow();
        let top = st.frames.last().expect("emit_top_frame on empty stack");
        (top.error.clone(), top.output_to_server, top.output_to_client)
    });

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

#[cold]
#[inline(never)]
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
        return Err(errstart_not_called().into());
    }
    emit_top_frame();
    STACK.with(|s| s.borrow_mut().recursion_depth -= 1);
    Ok(())
}

#[cold]
#[inline(never)]
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

// Current-frame mutators (errcode, errmsg, errdetail, ...). Each returns
// Err("errstart was not called") with no report in flight (CHECK_STACK_DEPTH).

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

#[cold]
#[inline(never)]
pub fn errcode(sqlerrcode: SqlState) -> PgResult<()> {
    with_current_mut(|error| error.sqlstate = sqlerrcode)
}

#[cold]
#[inline(never)]
pub fn errcode_for_file_access() -> PgResult<()> {
    with_current_mut(|error| {
        error.sqlstate = errno::sqlstate_for_file_access(error.saved_errno.unwrap_or(0));
    })
}

#[cold]
#[inline(never)]
pub fn errcode_for_socket_access() -> PgResult<()> {
    with_current_mut(|error| {
        error.sqlstate = errno::sqlstate_for_socket_access(error.saved_errno.unwrap_or(0));
    })
}

#[cold]
#[inline(never)]
pub fn errmsg(message: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.message_id = Some(message.to_owned());
        error.message = errno::replace_percent_m(message, error.saved_errno.unwrap_or(0));
    })
}

#[cold]
#[inline(never)]
pub fn errmsg_internal(message: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.message_id = Some(message.to_owned());
        error.message = errno::replace_percent_m(message, error.saved_errno.unwrap_or(0));
    })
}

#[cold]
#[inline(never)]
pub fn errmsg_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    let picked = if n == 1 { fmt_singular } else { fmt_plural };
    with_current_mut(|error| {
        error.message_id = Some(fmt_singular.to_owned());
        error.message = errno::replace_percent_m(picked, error.saved_errno.unwrap_or(0));
    })
}

#[cold]
#[inline(never)]
pub fn errdetail(detail: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.detail = Some(errno::replace_percent_m(detail, error.saved_errno.unwrap_or(0)));
    })
}

#[cold]
#[inline(never)]
pub fn errdetail_internal(detail: &str) -> PgResult<()> {
    errdetail(detail)
}

#[cold]
#[inline(never)]
pub fn ereport_msg(elevel: ErrorLevel, msg: String, detail: Option<String>) -> PgResult<()> {
    if !errstart(elevel, None) {
        // Short-circuit exactly like the C ereport() macro.
        return Ok(());
    }
    errmsg_internal(&msg)?;
    if let Some(detail) = detail {
        errdetail(&detail)?;
    }
    errfinish(None, 0, None)
}

#[cold]
#[inline(never)]
pub fn errdetail_log(detail_log: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.detail_log = Some(errno::replace_percent_m(
            detail_log,
            error.saved_errno.unwrap_or(0),
        ));
    })
}

#[cold]
#[inline(never)]
pub fn errdetail_log_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    errdetail_log(if n == 1 { fmt_singular } else { fmt_plural })
}

#[cold]
#[inline(never)]
pub fn errdetail_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    errdetail(if n == 1 { fmt_singular } else { fmt_plural })
}

#[cold]
#[inline(never)]
pub fn errhint(hint: &str) -> PgResult<()> {
    with_current_mut(|error| {
        error.hint = Some(errno::replace_percent_m(hint, error.saved_errno.unwrap_or(0)));
    })
}

#[cold]
#[inline(never)]
pub fn errhint_internal(hint: &str) -> PgResult<()> {
    errhint(hint)
}

#[cold]
#[inline(never)]
pub fn errhint_plural(fmt_singular: &str, fmt_plural: &str, n: u64) -> PgResult<()> {
    errhint(if n == 1 { fmt_singular } else { fmt_plural })
}

#[cold]
#[inline(never)]
pub fn errcontext_msg(context: &str) -> PgResult<()> {
    with_current_mut(|error| {
        let line = errno::replace_percent_m(context, error.saved_errno.unwrap_or(0));
        error.add_context_line(line);
    })
}

#[cold]
#[inline(never)]
pub fn set_errcontext_domain(domain: Option<&str>) -> PgResult<()> {
    with_current_mut(|error| {
        error.context_domain = Some(domain.unwrap_or("postgres").to_owned());
    })
}

#[cold]
#[inline(never)]
pub fn errhidestmt(hide_stmt: bool) -> PgResult<()> {
    with_current_mut(|error| error.hide_statement = hide_stmt)
}

#[cold]
#[inline(never)]
pub fn errhidecontext(hide_ctx: bool) -> PgResult<()> {
    with_current_mut(|error| error.hide_context = hide_ctx)
}

#[cold]
#[inline(never)]
pub fn errbacktrace() -> PgResult<()> {
    with_current_mut(|error| report::set_backtrace(error, 1))
}

#[cold]
#[inline(never)]
pub fn errposition(cursorpos: i32) -> PgResult<()> {
    with_current_mut(|error| error.cursor_position = nonzero(cursorpos))
}

#[cold]
#[inline(never)]
pub fn internalerrposition(cursorpos: i32) -> PgResult<()> {
    with_current_mut(|error| error.internal_position = nonzero(cursorpos))
}

#[cold]
#[inline(never)]
pub fn internalerrquery(query: Option<&str>) -> PgResult<()> {
    with_current_mut(|error| error.internal_query = query.map(str::to_owned))
}

#[cold]
#[inline(never)]
pub fn err_generic_string(field: ErrorField, value: &str) -> PgResult<()> {
    let mut result = Ok(());
    with_current_mut(|error| result = error.set_error_field(field, value))?;
    result
}

pub fn geterrcode() -> PgResult<SqlState> {
    with_current(|error| error.sqlstate)
}

pub fn geterrposition() -> PgResult<i32> {
    with_current(|error| error.cursor_position.unwrap_or(0))
}

pub fn getinternalerrposition() -> PgResult<i32> {
    with_current(|error| error.internal_position.unwrap_or(0))
}

fn nonzero(position: i32) -> Option<i32> {
    (position != 0).then_some(position)
}

pub fn CopyErrorData() -> PgResult<PgError> {
    with_current(PgError::clone)
}

pub fn FreeErrorData(_edata: PgError) {}

pub fn FlushErrorState() {
    STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.frames.clear();
        st.recursion_depth = 0;
    });
}

#[cold]
#[inline(never)]
pub fn ThrowErrorData(edata: PgError) -> PgResult<()> {
    if !errstart(edata.level, edata.domain.as_deref()) {
        return Ok(()); // error is not to be reported at all
    }

    STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.recursion_depth += 1;
        let frame = st.frames.last_mut().expect("errstart pushed a frame");
        let new = &mut frame.error;

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

    let location = edata.location;
    match location {
        Some(loc) => errfinish(loc.filename.as_deref(), loc.lineno, loc.funcname.as_deref()),
        None => errfinish(None, 0, None),
    }
}

#[cold]
#[inline(never)]
pub fn ReThrowError<T>(edata: PgError) -> PgResult<T> {
    // Assert(edata->elevel == ERROR)
    if edata.level != ERROR {
        return Err(PgError::new(PANIC, "ReThrowError called with non-ERROR error data").into());
    }
    Err(edata.into())
}

#[cold]
#[inline(never)]
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
        Some(frame) => Err(frame.error.into()),
        // ExceptionalCondition("pg_re_throw tried to return")
        None => Err(PgError::new(PANIC, "pg_re_throw tried to return").into()),
    }
}

pub fn GetErrorContextStack() -> Option<String> {
    let overflow = STACK.with(|s| {
        let mut st = s.borrow_mut();
        st.recursion_depth += 1;
        // get_error_stack_entry(): stack not big enough -> make room, PANIC.
        if st.frames.len() >= ERRORDATA_STACK_SIZE {
            st.frames.clear();
            return true;
        }
        let mut error = PgError::new(::types_error::LOG, String::new());
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
        let _ = ThrowErrorData(PgError::new(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
        std::process::abort();
    }

    STACK.with(|s| {
        let mut st = s.borrow_mut();
        let frame = st.frames.pop();
        st.recursion_depth -= 1;
        frame.and_then(|f| f.error.context)
    })
}

#[cold]
#[inline(never)]
pub fn errsave_start(
    context: Option<&mut ::types_error::SoftErrorContext>,
    domain: Option<&str>,
) -> bool {
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
        if st.frames.len() >= ERRORDATA_STACK_SIZE {
            st.frames.clear();
            return true;
        }
        let mut error = PgError::new(::types_error::LOG, String::new());
        error.saved_errno = Some(errno::current_errno());
        let domain = domain.unwrap_or("postgres");
        error.domain = Some(domain.to_owned());
        error.context_domain = Some(domain.to_owned());
        // Select default errcode based on the assumed elevel of ERROR.
        error.sqlstate = ::types_error::ERRCODE_INTERNAL_ERROR;
        st.frames.push(Frame {
            error,
            output_to_server: false,
            output_to_client: false,
        });
        st.recursion_depth -= 1;
        false
    });
    if overflow {
        let _ = ThrowErrorData(PgError::new(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
        std::process::abort();
    }
    true
}

#[cold]
#[inline(never)]
pub fn errsave_finish(
    context: Option<&mut ::types_error::SoftErrorContext>,
    filename: Option<&str>,
    lineno: i32,
    funcname: Option<&str>,
) -> PgResult<()> {
    let top_level = STACK.with(|s| s.borrow().frames.last().map(|f| f.error.level));
    let Some(top_level) = top_level else {
        return Err(errstart_not_called().into());
    };

    if top_level >= ERROR {
        return errfinish(filename, lineno, funcname);
    }

    // Package up the stack entry contents and deliver them to the caller.
    // (Backtrace and context callbacks are deliberately skipped, as in C.)
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
