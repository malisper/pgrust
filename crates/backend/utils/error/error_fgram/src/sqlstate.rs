use pg_ffi_fgram::{
    ErrorLevel, SqlState, DEBUG1, DEBUG2, DEBUG3, DEBUG4, DEBUG5, ERRCODE_INTERNAL_ERROR,
    ERRCODE_SUCCESSFUL_COMPLETION, ERRCODE_WARNING, ERROR, FATAL, INFO, LOG, LOG_SERVER_ONLY,
    NOTICE, PANIC, WARNING, WARNING_CLIENT_ONLY,
};

pub fn severity(level: ErrorLevel) -> &'static str {
    match level {
        DEBUG5 | DEBUG4 | DEBUG3 | DEBUG2 | DEBUG1 => "DEBUG",
        LOG | LOG_SERVER_ONLY => "LOG",
        INFO => "INFO",
        NOTICE => "NOTICE",
        WARNING | WARNING_CLIENT_ONLY => "WARNING",
        ERROR => "ERROR",
        FATAL => "FATAL",
        PANIC => "PANIC",
        _ => "???",
    }
}

pub fn default_sqlstate_for_level(level: ErrorLevel) -> SqlState {
    if level >= ERROR {
        ERRCODE_INTERNAL_ERROR
    } else if level >= WARNING {
        ERRCODE_WARNING
    } else {
        ERRCODE_SUCCESSFUL_COMPLETION
    }
}
