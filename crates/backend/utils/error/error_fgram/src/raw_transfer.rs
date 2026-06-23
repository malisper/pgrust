use std::ffi::CStr;
use std::ptr;

use pg_ffi_fgram::{ErrorLevel, PgrustErrorData, SqlState};

use crate::{value::nonzero_position, ErrorLocation, PgError};

impl PgError {
    pub unsafe fn from_raw_transfer(raw: PgrustErrorData) -> Self {
        Self {
            level: ErrorLevel(raw.elevel),
            sqlstate: SqlState(raw.sqlerrcode),
            message: unsafe { take_c_string(raw.message) }
                .unwrap_or_else(|| "PostgreSQL error".to_owned()),
            detail: unsafe { take_c_string(raw.detail) },
            detail_log: unsafe { take_c_string(raw.detail_log) },
            hint: unsafe { take_c_string(raw.hint) },
            context: unsafe { take_c_string(raw.context) },
            backtrace: unsafe { take_c_string(raw.backtrace) },
            message_id: unsafe { take_c_string(raw.message_id) },
            domain: unsafe { take_c_string(raw.domain) },
            context_domain: unsafe { take_c_string(raw.context_domain) },
            hide_statement: raw.hide_stmt,
            hide_context: raw.hide_ctx,
            location: Some(ErrorLocation {
                filename: unsafe { take_c_string(raw.filename) },
                lineno: raw.lineno,
                funcname: unsafe { take_c_string(raw.funcname) },
            }),
            saved_errno: raw.has_saved_errno.then_some(raw.saved_errno),
            cursor_position: nonzero_position(raw.cursorpos),
            internal_position: nonzero_position(raw.internalpos),
            internal_query: unsafe { take_c_string(raw.internalquery) },
            schema_name: unsafe { take_c_string(raw.schema_name) },
            table_name: unsafe { take_c_string(raw.table_name) },
            column_name: unsafe { take_c_string(raw.column_name) },
            datatype_name: unsafe { take_c_string(raw.datatype_name) },
            constraint_name: unsafe { take_c_string(raw.constraint_name) },
        }
    }
}

pub fn pgrust_error_data_from_error(error: &PgError) -> PgrustErrorData {
    let location = error.location.as_ref();
    PgrustErrorData {
        elevel: error.level.0,
        sqlerrcode: error.sqlstate.0,
        message: malloc_string(Some(&error.message)),
        detail: malloc_string(error.detail.as_deref()),
        detail_log: malloc_string(error.detail_log.as_deref()),
        hint: malloc_string(error.hint.as_deref()),
        context: malloc_string(error.context.as_deref()),
        backtrace: malloc_string(error.backtrace.as_deref()),
        message_id: malloc_string(error.message_id.as_deref()),
        filename: malloc_string(location.and_then(|location| location.filename.as_deref())),
        lineno: location.map_or(0, |location| location.lineno),
        funcname: malloc_string(location.and_then(|location| location.funcname.as_deref())),
        domain: malloc_string(error.domain.as_deref()),
        context_domain: malloc_string(error.context_domain.as_deref()),
        hide_stmt: error.hide_statement,
        hide_ctx: error.hide_context,
        saved_errno: error.saved_errno.unwrap_or(0),
        has_saved_errno: error.saved_errno.is_some(),
        cursorpos: error.cursor_position.unwrap_or(0),
        internalpos: error.internal_position.unwrap_or(0),
        internalquery: malloc_string(error.internal_query.as_deref()),
        schema_name: malloc_string(error.schema_name.as_deref()),
        table_name: malloc_string(error.table_name.as_deref()),
        column_name: malloc_string(error.column_name.as_deref()),
        datatype_name: malloc_string(error.datatype_name.as_deref()),
        constraint_name: malloc_string(error.constraint_name.as_deref()),
    }
}

pub unsafe fn pgrust_error_data_free_owned_fields(raw: &mut PgrustErrorData) {
    unsafe {
        free_ptr(raw.message);
        free_ptr(raw.detail);
        free_ptr(raw.detail_log);
        free_ptr(raw.hint);
        free_ptr(raw.context);
        free_ptr(raw.backtrace);
        free_ptr(raw.message_id);
        free_ptr(raw.filename);
        free_ptr(raw.funcname);
        free_ptr(raw.domain);
        free_ptr(raw.context_domain);
        free_ptr(raw.internalquery);
        free_ptr(raw.schema_name);
        free_ptr(raw.table_name);
        free_ptr(raw.column_name);
        free_ptr(raw.datatype_name);
        free_ptr(raw.constraint_name);
    }

    raw.message = ptr::null_mut();
    raw.detail = ptr::null_mut();
    raw.detail_log = ptr::null_mut();
    raw.hint = ptr::null_mut();
    raw.context = ptr::null_mut();
    raw.backtrace = ptr::null_mut();
    raw.message_id = ptr::null_mut();
    raw.filename = ptr::null_mut();
    raw.funcname = ptr::null_mut();
    raw.domain = ptr::null_mut();
    raw.context_domain = ptr::null_mut();
    raw.internalquery = ptr::null_mut();
    raw.schema_name = ptr::null_mut();
    raw.table_name = ptr::null_mut();
    raw.column_name = ptr::null_mut();
    raw.datatype_name = ptr::null_mut();
    raw.constraint_name = ptr::null_mut();
}

fn malloc_string(value: Option<&str>) -> *mut std::ffi::c_char {
    let Some(value) = value else {
        return ptr::null_mut();
    };

    let bytes = value.as_bytes();
    let ptr = unsafe { libc::malloc(bytes.len() + 1) } as *mut u8;
    if ptr.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
        *ptr.add(bytes.len()) = 0;
    }

    ptr as *mut std::ffi::c_char
}

unsafe fn take_c_string(ptr: *mut std::ffi::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }

    let value = unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    unsafe { free_ptr(ptr) };
    Some(value)
}

unsafe fn free_ptr(ptr: *mut std::ffi::c_char) {
    if !ptr.is_null() {
        unsafe { libc::free(ptr.cast()) };
    }
}
