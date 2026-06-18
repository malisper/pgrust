#![allow(non_snake_case)]

use std::cell::RefCell;

use pgrust_pg_ffi::{ErrorLevel, SqlState, ERRCODE_INTERNAL_ERROR, ERROR, LOG, PANIC};

use crate::{ErrorLocation, PgError, PgResult};

pub const ERRORDATA_STACK_SIZE: usize = 5;

thread_local! {
    static ERROR_STACK: RefCell<ErrorStackState> = RefCell::new(ErrorStackState::default());
}

#[derive(Clone, Debug, Default)]
struct ErrorStackState {
    frames: Vec<StackFrame>,
    recursion_depth: usize,
    next_frame_id: u64,
}

#[derive(Clone, Debug)]
struct StackFrame {
    id: u64,
    error: PgError,
}

#[derive(Debug)]
pub struct ErrorStackFrame {
    active: bool,
    id: u64,
}

impl ErrorStackFrame {
    pub fn push(level: ErrorLevel, domain: Option<&str>) -> PgResult<Self> {
        ERROR_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.frames.len() >= ERRORDATA_STACK_SIZE {
                return Err(PgError::new(PANIC, "ERRORDATA_STACK_SIZE exceeded"));
            }

            stack.recursion_depth += 1;
            let mut error = PgError::new(level, "PostgreSQL error");
            error.sqlstate = if level >= ERROR {
                ERRCODE_INTERNAL_ERROR
            } else {
                crate::default_sqlstate_for_level(level)
            };
            if let Some(domain) = domain {
                error.domain = Some(domain.to_owned());
                error.context_domain = Some(domain.to_owned());
            }
            let id = stack.next_frame_id;
            stack.next_frame_id = stack.next_frame_id.wrapping_add(1);
            stack.frames.push(StackFrame { id, error });
            Ok(Self { active: true, id })
        })
    }

    pub fn errcode(&mut self, sqlstate: SqlState) -> PgResult<&mut Self> {
        with_current_mut(|error| {
            error.sqlstate = sqlstate;
        })?;
        Ok(self)
    }

    pub fn errmsg(&mut self, message: impl Into<String>) -> PgResult<&mut Self> {
        let message = message.into();
        with_current_mut(|error| {
            error.message_id = Some(message.clone());
            error.message = message;
        })?;
        Ok(self)
    }

    pub fn errmsg_internal(&mut self, message: impl Into<String>) -> PgResult<&mut Self> {
        with_current_mut(|error| {
            error.message = message.into();
            error.message_id = None;
        })?;
        Ok(self)
    }

    pub fn errdetail(&mut self, detail: impl Into<String>) -> PgResult<&mut Self> {
        with_current_mut(|error| error.detail = Some(detail.into()))?;
        Ok(self)
    }

    pub fn errdetail_log(&mut self, detail_log: impl Into<String>) -> PgResult<&mut Self> {
        with_current_mut(|error| error.detail_log = Some(detail_log.into()))?;
        Ok(self)
    }

    pub fn errhint(&mut self, hint: impl Into<String>) -> PgResult<&mut Self> {
        with_current_mut(|error| error.hint = Some(hint.into()))?;
        Ok(self)
    }

    pub fn errcontext_msg(&mut self, context: impl Into<String>) -> PgResult<&mut Self> {
        let context = context.into();
        with_current_mut(|error| {
            error.context = match error.context.take() {
                Some(mut existing) => {
                    existing.push('\n');
                    existing.push_str(&context);
                    Some(existing)
                }
                None => Some(context),
            };
        })?;
        Ok(self)
    }

    pub fn errposition(&mut self, cursor_position: i32) -> PgResult<&mut Self> {
        with_current_mut(|error| error.cursor_position = nonzero_position(cursor_position))?;
        Ok(self)
    }

    pub fn internalerrposition(&mut self, internal_position: i32) -> PgResult<&mut Self> {
        with_current_mut(|error| error.internal_position = nonzero_position(internal_position))?;
        Ok(self)
    }

    pub fn internalerrquery(&mut self, query: impl Into<String>) -> PgResult<&mut Self> {
        with_current_mut(|error| error.internal_query = Some(query.into()))?;
        Ok(self)
    }

    /// `err_generic_string(field, str)` — set one of the generic string fields
    /// (`schema_name` / `table_name` / `column_name` / `datatype_name` /
    /// `constraint_name`) on the in-flight error, mirroring the C helper of the
    /// same name that mutates the current errordata on the stack.
    pub fn err_generic_string(
        &mut self,
        field: crate::ErrorField,
        value: impl Into<String>,
    ) -> PgResult<&mut Self> {
        let value = value.into();
        let mut result = Ok(());
        with_current_mut(|error| {
            result = error.set_error_field(field, value);
        })?;
        result?;
        Ok(self)
    }

    pub fn set_errcontext_domain(&mut self, domain: impl Into<String>) -> PgResult<&mut Self> {
        let domain = domain.into();
        with_current_mut(|error| error.context_domain = Some(domain))?;
        Ok(self)
    }

    pub fn errhidestmt(&mut self, hide_statement: bool) -> PgResult<&mut Self> {
        with_current_mut(|error| error.hide_statement = hide_statement)?;
        Ok(self)
    }

    pub fn errhidecontext(&mut self, hide_context: bool) -> PgResult<&mut Self> {
        with_current_mut(|error| error.hide_context = hide_context)?;
        Ok(self)
    }

    pub fn finish(mut self, location: ErrorLocation) -> PgResult<PgError> {
        let mut error = pop_frame(self.id)?;
        self.active = false;
        error.level = ERROR;
        error.location = Some(location);
        Ok(error)
    }
}

impl Drop for ErrorStackFrame {
    fn drop(&mut self) {
        if self.active {
            let _ = try_pop_frame(self.id);
            self.active = false;
        }
    }
}

/// Rust-owned copy of PostgreSQL-style `ErrorData`.
///
/// PostgreSQL's `CopyErrorData` returns a palloc-owned `ErrorData *` with
/// separately duplicated strings. In this crate the equivalent safe handle owns
/// a cloned `PgError`, so `FreeErrorData` consumes the handle and lets Rust drop
/// it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopiedErrorData {
    error: PgError,
}

impl CopiedErrorData {
    pub fn new(error: PgError) -> Self {
        Self { error }
    }

    pub fn error(&self) -> &PgError {
        &self.error
    }

    pub fn into_error(self) -> PgError {
        self.error
    }
}

impl From<PgError> for CopiedErrorData {
    fn from(error: PgError) -> Self {
        Self::new(error)
    }
}

pub fn CopyErrorData() -> PgResult<CopiedErrorData> {
    with_current(|error| CopiedErrorData::new(error.clone()))
}

pub fn FreeErrorData(_edata: CopiedErrorData) {}

pub fn ThrowErrorData<T>(edata: CopiedErrorData) -> PgResult<T> {
    Err(edata.into_error())
}

pub fn ReThrowError<T>(edata: CopiedErrorData) -> PgResult<T> {
    let error = edata.into_error();
    if error.level() != ERROR {
        return Err(PgError::new(
            PANIC,
            "ReThrowError called with non-ERROR error data",
        ));
    }
    Err(error)
}

pub fn pg_re_throw<T>() -> PgResult<T> {
    match pop_current() {
        Ok(error) => Err(error),
        Err(_) => Err(PgError::new(PANIC, "pg_re_throw tried to return")),
    }
}

pub fn FlushErrorState() {
    ERROR_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        stack.frames.clear();
        stack.recursion_depth = 0;
    });
}

pub fn in_error_recursion_trouble() -> bool {
    ERROR_STACK.with(|stack| stack.borrow().recursion_depth > 2)
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

pub fn set_errcontext_domain(domain: impl Into<String>) -> PgResult<()> {
    with_current_mut(|error| error.context_domain = Some(domain.into()))
}

fn with_current<R>(f: impl FnOnce(&PgError) -> R) -> PgResult<R> {
    ERROR_STACK.with(|stack| {
        let stack = stack.borrow();
        let frame = stack.frames.last().ok_or_else(errstart_not_called)?;
        Ok(f(&frame.error))
    })
}

fn with_current_mut(f: impl FnOnce(&mut PgError)) -> PgResult<()> {
    ERROR_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let frame = stack.frames.last_mut().ok_or_else(errstart_not_called)?;
        f(&mut frame.error);
        Ok(())
    })
}

fn pop_current() -> PgResult<PgError> {
    ERROR_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let frame = stack.frames.pop().ok_or_else(errstart_not_called)?;
        stack.recursion_depth = stack.recursion_depth.saturating_sub(1);
        Ok(frame.error)
    })
}

fn pop_frame(id: u64) -> PgResult<PgError> {
    ERROR_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let frame = stack.frames.last().ok_or_else(errstart_not_called)?;
        if frame.id != id {
            return Err(PgError::new(
                PANIC,
                "error stack frame finished out of order",
            ));
        }
        let frame = stack.frames.pop().expect("last frame was checked");
        stack.recursion_depth = stack.recursion_depth.saturating_sub(1);
        Ok(frame.error)
    })
}

fn try_pop_frame(id: u64) -> PgResult<Option<PgError>> {
    // `try_with`, not `with`: this is the only access reached from
    // `ErrorStackFrame::drop`, which can fire while the stack is unwinding a
    // process entry point WITHOUT a clean exit. By that point the thread's
    // `ERROR_STACK` TLS may already be mid- or post-destruction, and a plain
    // `with` would raise an `AccessError`. A panic out of a `Drop` that runs
    // during an unwind `abort()`s the process (the double-panic crash mode), so
    // probe with `try_with` and treat a destroyed TLS as "no frame to pop"
    // (mirrors the dsm-core atexit guard, 06181ad9f).
    ERROR_STACK
        .try_with(|stack| {
            let mut stack = stack.borrow_mut();
            let Some(frame) = stack.frames.last() else {
                return Ok(None);
            };
            if frame.id != id {
                return Ok(None);
            }
            let frame = stack.frames.pop().expect("last frame was checked");
            stack.recursion_depth = stack.recursion_depth.saturating_sub(1);
            Ok(Some(frame.error))
        })
        .unwrap_or(Ok(None))
}

fn errstart_not_called() -> PgError {
    PgError::error("errstart was not called")
}

fn nonzero_position(position: i32) -> Option<i32> {
    (position != 0).then_some(position)
}

pub(crate) fn soft_error_frame(domain: Option<&str>) -> PgResult<ErrorStackFrame> {
    ErrorStackFrame::push(LOG, domain)
}
