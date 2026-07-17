//! elog.c's GUC parameters and cross-subsystem globals, OWNED here (not
//! seamed) with C boot defaults; owning units call the setters when they land.
//! Per-backend C globals live in `thread_local!` cells.

use std::cell::{Cell, RefCell};

use ::types_core::NAMEDATALEN;
use ::types_dest::CommandDest;
use ::types_error::{
    ErrorLevel, PGErrorVerbosity, PgError, PgResult, ERROR, LOG_DESTINATION_CSVLOG,
    LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR, LOG_DESTINATION_SYSLOG, NOTICE, WARNING,
};

thread_local! { static LOG_MIN_MESSAGES: Cell<i32> = const { Cell::new(WARNING.0) }; }
thread_local! { static CLIENT_MIN_MESSAGES: Cell<i32> = const { Cell::new(NOTICE.0) }; }
// `CommandDest whereToSendOutput = DestDebug;` (postgres.c) — the canonical
// per-backend home; tcop/postmaster/walsender consumers delegate here.
thread_local! { static WHERE_TO_SEND_OUTPUT: Cell<CommandDest> = const { Cell::new(CommandDest::Debug) }; }
thread_local! { static CLIENT_AUTH_IN_PROGRESS: Cell<bool> = const { Cell::new(false) }; }
thread_local! { static LOG_MIN_ERROR_STATEMENT: Cell<i32> = const { Cell::new(ERROR.0) }; }

#[inline]
pub fn log_min_messages() -> ErrorLevel {
    ErrorLevel(LOG_MIN_MESSAGES.with(Cell::get))
}

pub fn set_log_min_messages(level: ErrorLevel) {
    LOG_MIN_MESSAGES.with(|c| c.set(level.0));
}

#[inline]
pub fn client_min_messages() -> ErrorLevel {
    ErrorLevel(CLIENT_MIN_MESSAGES.with(Cell::get))
}

pub fn set_client_min_messages(level: ErrorLevel) {
    CLIENT_MIN_MESSAGES.with(|c| c.set(level.0));
}

#[inline]
pub fn where_to_send_output() -> CommandDest {
    WHERE_TO_SEND_OUTPUT.with(Cell::get)
}

pub fn set_where_to_send_output(dest: CommandDest) {
    WHERE_TO_SEND_OUTPUT.with(|c| c.set(dest));
}

#[inline]
pub fn client_auth_in_progress() -> bool {
    CLIENT_AUTH_IN_PROGRESS.with(Cell::get)
}

pub fn set_client_auth_in_progress(value: bool) {
    CLIENT_AUTH_IN_PROGRESS.with(|c| c.set(value));
}

#[inline]
pub fn log_min_error_statement() -> ErrorLevel {
    ErrorLevel(LOG_MIN_ERROR_STATEMENT.with(Cell::get))
}

pub fn set_log_min_error_statement(level: ErrorLevel) {
    LOG_MIN_ERROR_STATEMENT.with(|c| c.set(level.0));
}

thread_local! { static LOG_ERROR_VERBOSITY: Cell<PGErrorVerbosity> = const { Cell::new(PGErrorVerbosity::Default) }; }
thread_local! { static LOG_LINE_PREFIX: RefCell<Option<String>> = const { RefCell::new(None) }; }
thread_local! { static LOG_DESTINATION: Cell<i32> = const { Cell::new(LOG_DESTINATION_STDERR) }; }
thread_local! { static LOG_DESTINATION_STRING: RefCell<Option<String>> = const { RefCell::new(None) }; }
thread_local! { static SYSLOG_SEQUENCE_NUMBERS: Cell<bool> = const { Cell::new(true) }; }
thread_local! { static SYSLOG_SPLIT_MESSAGES: Cell<bool> = const { Cell::new(true) }; }

pub fn log_error_verbosity() -> PGErrorVerbosity {
    LOG_ERROR_VERBOSITY.with(Cell::get)
}

pub fn set_log_error_verbosity(verbosity: PGErrorVerbosity) {
    LOG_ERROR_VERBOSITY.with(|c| c.set(verbosity));
}

pub fn log_line_prefix_format() -> Option<String> {
    LOG_LINE_PREFIX.with(|c| c.borrow().clone())
}

pub fn set_log_line_prefix(format: Option<String>) {
    LOG_LINE_PREFIX.with(|c| *c.borrow_mut() = format);
}

pub fn log_destination() -> i32 {
    LOG_DESTINATION.with(Cell::get)
}

pub fn syslog_sequence_numbers() -> bool {
    SYSLOG_SEQUENCE_NUMBERS.with(Cell::get)
}

pub fn set_syslog_sequence_numbers(value: bool) {
    SYSLOG_SEQUENCE_NUMBERS.with(|c| c.set(value));
}

pub fn syslog_split_messages() -> bool {
    SYSLOG_SPLIT_MESSAGES.with(Cell::get)
}

pub fn set_syslog_split_messages(value: bool) {
    SYSLOG_SPLIT_MESSAGES.with(|c| c.set(value));
}

// `CritSectionCount` (miscadmin.h; owned by the crit-section machinery).
thread_local! { static CRIT_SECTION_COUNT: Cell<u32> = const { Cell::new(0) }; }
// `ExitOnAnyError` (globals.c; initdb sets it).
thread_local! { static EXIT_ON_ANY_ERROR: Cell<bool> = const { Cell::new(false) }; }
// `proc_exit_inprogress` (storage/ipc/ipc.c).
thread_local! { static PROC_EXIT_INPROGRESS: Cell<bool> = const { Cell::new(false) }; }
// `redirection_done` (postmaster.c): stderr goes to the syslogger pipe.
// Process-wide: C fork-inherits it into every child; one fd table shares the
// redirected fd 2, so every thread must agree.
static REDIRECTION_DONE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
// Mirrors `MyBackendType == B_LOGGER` (miscinit.c).
thread_local! { static AM_SYSLOGGER: Cell<bool> = const { Cell::new(false) }; }
// `IsUnderPostmaster` (globals.c).
thread_local! { static IS_UNDER_POSTMASTER: Cell<bool> = const { Cell::new(false) }; }
// `FrontendProtocol` (globals.c); 0 = not yet negotiated.
thread_local! { static FRONTEND_PROTOCOL: Cell<u32> = const { Cell::new(0) }; }
// `OutputFileName` (globals.c); empty = none.
thread_local! { static OUTPUT_FILE_NAME: RefCell<Option<String>> = const { RefCell::new(None) }; }

#[inline]
pub fn crit_section_count() -> u32 {
    CRIT_SECTION_COUNT.with(Cell::get)
}

pub fn set_crit_section_count(count: u32) {
    CRIT_SECTION_COUNT.with(|c| c.set(count));
}

#[inline]
pub fn exit_on_any_error() -> bool {
    EXIT_ON_ANY_ERROR.with(Cell::get)
}

pub fn set_exit_on_any_error(value: bool) {
    EXIT_ON_ANY_ERROR.with(|c| c.set(value));
}

#[inline]
pub fn proc_exit_inprogress() -> bool {
    PROC_EXIT_INPROGRESS.with(Cell::get)
}

pub fn set_proc_exit_inprogress(value: bool) {
    PROC_EXIT_INPROGRESS.with(|c| c.set(value));
}

pub fn redirection_done() -> bool {
    REDIRECTION_DONE.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_redirection_done(value: bool) {
    REDIRECTION_DONE.store(value, std::sync::atomic::Ordering::Relaxed);
}

pub fn am_syslogger() -> bool {
    AM_SYSLOGGER.with(Cell::get)
}

pub fn set_am_syslogger(value: bool) {
    AM_SYSLOGGER.with(|c| c.set(value));
}

pub fn is_under_postmaster() -> bool {
    IS_UNDER_POSTMASTER.with(Cell::get)
}

pub fn set_is_under_postmaster(value: bool) {
    IS_UNDER_POSTMASTER.with(|c| c.set(value));
}

pub fn frontend_protocol() -> u32 {
    FRONTEND_PROTOCOL.with(Cell::get)
}

pub fn set_frontend_protocol(version: u32) {
    FRONTEND_PROTOCOL.with(|c| c.set(version));
}

pub fn output_file_name() -> Option<String> {
    OUTPUT_FILE_NAME.with(|c| c.borrow().clone())
}

pub fn set_output_file_name(name: Option<String>) {
    OUTPUT_FILE_NAME.with(|c| *c.borrow_mut() = name);
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BacktraceFunctionList {
    functions: Vec<String>,
}

impl BacktraceFunctionList {
    pub fn functions(&self) -> &[String] {
        &self.functions
    }

    pub fn matches(&self, funcname: &str) -> bool {
        if funcname.is_empty() {
            return false;
        }
        for function in &self.functions {
            if function.is_empty() {
                break;
            }
            if function == funcname {
                return true;
            }
        }
        false
    }
}

thread_local! { static BACKTRACE_FUNCTION_LIST: RefCell<Option<BacktraceFunctionList>> = const { RefCell::new(None) }; }

pub fn check_backtrace_functions(newval: &str) -> PgResult<Option<BacktraceFunctionList>> {
    let valid = |b: u8| {
        b.is_ascii_digit()
            || b == b'_'
            || b.is_ascii_lowercase()
            || b.is_ascii_uppercase()
            || matches!(b, b',' | b' ' | b'\n' | b'\t')
    };
    if !newval.bytes().all(valid) {
        return Err(PgError::error("Invalid character.").into());
    }

    if newval.is_empty() {
        return Ok(None);
    }

    let mut functions = Vec::new();
    let mut current = String::new();
    for byte in newval.bytes() {
        match byte {
            b',' => functions.push(std::mem::take(&mut current)),
            b' ' | b'\n' | b'\t' => {}
            _ => current.push(byte as char),
        }
    }
    functions.push(current);

    Ok(Some(BacktraceFunctionList { functions }))
}

pub fn assign_backtrace_functions(extra: Option<BacktraceFunctionList>) {
    BACKTRACE_FUNCTION_LIST.with(|c| *c.borrow_mut() = extra);
}

pub fn matches_backtrace_functions(funcname: &str) -> bool {
    BACKTRACE_FUNCTION_LIST
        .with(|c| c.borrow().as_ref().is_some_and(|list| list.matches(funcname)))
}

pub fn check_log_destination(newval: &str) -> PgResult<i32> {
    let identifiers = split_identifier_string(newval, ',')?;
    let mut newlogdest = 0;

    for tok in identifiers {
        // pg_strcasecmp: case-insensitive even for quoted identifiers.
        if tok.eq_ignore_ascii_case("stderr") {
            newlogdest |= LOG_DESTINATION_STDERR;
        } else if tok.eq_ignore_ascii_case("csvlog") || tok.eq_ignore_ascii_case("jsonlog") {
            // The write_csvlog/write_jsonlog seams are unported (no
            // implementation exists anywhere), so accepting these values
            // panicked inside error reporting on every log line once
            // redirection_done — including the postmaster (seam-audit F2).
            // Reject at check time until the writers land: the same class as
            // "eventlog", which C only accepts #ifdef WIN32. Clean-error
            // convention per the panicfix program.
            return Err(PgError::error(format!(
                "Destination \"{}\" is not supported in this build.",
                tok.to_ascii_lowercase()
            ))
            .into());
        } else if tok.eq_ignore_ascii_case("syslog") {
            newlogdest |= LOG_DESTINATION_SYSLOG;
        } else {
            return Err(PgError::error(format!("Unrecognized key word: \"{}\".", tok)).into());
        }
    }

    Ok(newlogdest)
}

pub fn assign_log_destination(extra: i32) {
    LOG_DESTINATION.with(|c| c.set(extra));
}

pub fn assign_syslog_ident(newval: &str) {
    crate::syslog::assign_syslog_ident(newval);
}

pub fn syslog_facility() -> i32 {
    crate::syslog::syslog_facility()
}

pub fn assign_syslog_facility(newval: i32) {
    crate::syslog::assign_syslog_facility(newval);
}

pub fn log_destination_string() -> Option<String> {
    LOG_DESTINATION_STRING.with(|c| c.borrow().clone())
}

pub fn set_log_destination_string(value: Option<String>) {
    LOG_DESTINATION_STRING.with(|c| *c.borrow_mut() = value);
}

fn scanner_isspace(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\n' | '\r' | '\x0c')
}

fn split_identifier_string(raw: &str, separator: char) -> PgResult<Vec<String>> {
    let syntax_error = || PgError::error("List syntax is invalid.");
    let mut chars = raw.char_indices().peekable();
    let mut identifiers = Vec::new();

    while matches!(chars.peek(), Some((_, ch)) if scanner_isspace(*ch)) {
        chars.next();
    }
    if chars.peek().is_none() {
        return Ok(identifiers);
    }

    loop {
        let identifier = if matches!(chars.peek(), Some((_, '"'))) {
            chars.next();
            let mut identifier = String::new();
            loop {
                match chars.next() {
                    Some((_, '"')) if matches!(chars.peek(), Some((_, '"'))) => {
                        chars.next();
                        identifier.push('"');
                    }
                    Some((_, '"')) => break,
                    Some((_, ch)) => identifier.push(ch),
                    None => return Err(syntax_error().into()),
                }
            }
            identifier
        } else {
            let mut identifier = String::new();
            while let Some((_, ch)) = chars.peek().copied() {
                if ch == separator || scanner_isspace(ch) {
                    break;
                }
                chars.next();
                identifier.push(ch.to_ascii_lowercase());
            }
            if identifier.is_empty() {
                return Err(syntax_error().into());
            }
            if identifier.len() >= NAMEDATALEN as usize {
                let mut clip = NAMEDATALEN as usize - 1;
                while !identifier.is_char_boundary(clip) {
                    clip -= 1;
                }
                identifier.truncate(clip);
            }
            identifier
        };

        while matches!(chars.peek(), Some((_, ch)) if scanner_isspace(*ch)) {
            chars.next();
        }
        match chars.peek().copied() {
            Some((_, ch)) if ch == separator => {
                chars.next();
                while matches!(chars.peek(), Some((_, ch)) if scanner_isspace(*ch)) {
                    chars.next();
                }
                identifiers.push(identifier);
                if chars.peek().is_none() {
                    return Err(syntax_error().into());
                }
            }
            Some(_) => return Err(syntax_error().into()),
            None => {
                identifiers.push(identifier);
                break;
            }
        }
    }

    Ok(identifiers)
}
