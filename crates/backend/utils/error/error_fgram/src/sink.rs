use std::sync::Mutex;
use std::time::SystemTime;

use crate::{ErrorLevel, LogDestination, PgError, PgResult, ERROR};

pub type ReportSink = fn(&PgError);

static REPORT_SINK: Mutex<Option<ReportSink>> = Mutex::new(None);
static SERVER_LOG_SINK: Mutex<Option<&'static dyn ServerLogSink>> = Mutex::new(None);
static FRONTEND_ERROR_SINK: Mutex<Option<&'static dyn FrontendErrorSink>> = Mutex::new(None);
static SYSLOGGER_SINK: Mutex<Option<&'static dyn SysloggerSink>> = Mutex::new(None);
static BACKEND_LOG_CONTEXT: Mutex<Option<&'static dyn BackendLogContext>> = Mutex::new(None);

pub trait ServerLogSink: Sync {
    fn write_server_log(&self, error: &PgError, formatted: &str) -> PgResult<()>;
}

pub trait FrontendErrorSink: Sync {
    fn send_error_to_frontend(&self, error: &PgError) -> PgResult<()>;
}

pub trait SysloggerSink: Sync {
    fn write_pipe_chunks(&self, bytes: &[u8], destination: LogDestination) -> PgResult<()>;
}

pub trait BackendLogContext: Sync {
    fn backend_type(&self) -> Option<&str> {
        None
    }

    fn application_name(&self) -> Option<&str> {
        None
    }

    fn database_name(&self) -> Option<&str> {
        None
    }

    fn user_name(&self) -> Option<&str> {
        None
    }

    fn remote_host(&self) -> Option<&str> {
        None
    }

    fn remote_port(&self) -> Option<&str> {
        None
    }

    fn local_host(&self) -> Option<&str> {
        None
    }

    fn query_id(&self) -> Option<i64> {
        None
    }

    fn query_string(&self) -> Option<&str> {
        None
    }

    fn log_min_error_statement(&self) -> ErrorLevel {
        ERROR
    }

    fn top_transaction_id(&self) -> Option<u32> {
        None
    }

    fn process_id(&self) -> Option<u32> {
        None
    }

    fn parallel_leader_process_id(&self) -> Option<u32> {
        None
    }

    fn virtual_transaction_id(&self) -> Option<(i32, u32)> {
        None
    }

    fn session_start_time(&self) -> Option<SystemTime> {
        None
    }

    fn ps_display(&self) -> Option<&str> {
        None
    }
}

pub fn set_report_sink(sink: Option<ReportSink>) -> Option<ReportSink> {
    let mut slot = REPORT_SINK.lock().expect("report sink lock poisoned");
    let previous = *slot;
    *slot = sink;
    previous
}

pub fn emit_report(error: &PgError) {
    let sink = *REPORT_SINK.lock().expect("report sink lock poisoned");
    if let Some(sink) = sink {
        sink(error);
    }
}

pub fn set_server_log_sink(
    sink: Option<&'static dyn ServerLogSink>,
) -> Option<&'static dyn ServerLogSink> {
    let mut slot = SERVER_LOG_SINK
        .lock()
        .expect("server log sink lock poisoned");
    let previous = *slot;
    *slot = sink;
    previous
}

pub fn set_frontend_error_sink(
    sink: Option<&'static dyn FrontendErrorSink>,
) -> Option<&'static dyn FrontendErrorSink> {
    let mut slot = FRONTEND_ERROR_SINK
        .lock()
        .expect("frontend error sink lock poisoned");
    let previous = *slot;
    *slot = sink;
    previous
}

pub fn set_syslogger_sink(
    sink: Option<&'static dyn SysloggerSink>,
) -> Option<&'static dyn SysloggerSink> {
    let mut slot = SYSLOGGER_SINK.lock().expect("syslogger sink lock poisoned");
    let previous = *slot;
    *slot = sink;
    previous
}

pub fn set_backend_log_context(
    context: Option<&'static dyn BackendLogContext>,
) -> Option<&'static dyn BackendLogContext> {
    let mut slot = BACKEND_LOG_CONTEXT
        .lock()
        .expect("backend log context lock poisoned");
    let previous = *slot;
    *slot = context;
    previous
}

pub fn backend_log_context() -> Option<&'static dyn BackendLogContext> {
    *BACKEND_LOG_CONTEXT
        .lock()
        .expect("backend log context lock poisoned")
}

pub fn emit_error_report(error: &PgError, formatted: &str) -> PgResult<()> {
    emit_report(error);

    let server_sink = *SERVER_LOG_SINK
        .lock()
        .expect("server log sink lock poisoned");
    if let Some(server_sink) = server_sink {
        server_sink.write_server_log(error, formatted)?;
    }

    let frontend_sink = *FRONTEND_ERROR_SINK
        .lock()
        .expect("frontend error sink lock poisoned");
    if let Some(frontend_sink) = frontend_sink {
        frontend_sink.send_error_to_frontend(error)?;
    }

    Ok(())
}

pub fn write_pipe_chunks(bytes: &[u8], destination: LogDestination) -> PgResult<()> {
    let sink = *SYSLOGGER_SINK.lock().expect("syslogger sink lock poisoned");
    if let Some(sink) = sink {
        sink.write_pipe_chunks(bytes, destination)?;
    }
    Ok(())
}
