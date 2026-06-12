//! elog.c's GUC parameters and the cross-subsystem globals it reads.
//!
//! These are OWNED by this crate (not seamed): every static holds PostgreSQL's
//! boot-time default and exposes a `pub` setter that the owning unit (guc,
//! postmaster, miscinit, tcop, storage/ipc) calls when it lands. `guc ->
//! error` is acyclic, so the setters are plain function calls — no seams and
//! no possible panic on the logging hot path.
//!
//! Boot defaults mirror the C initializers / GUC boot values:
//! `log_min_messages = WARNING`, `client_min_messages = NOTICE`,
//! `whereToSendOutput = DestNone`, `ClientAuthInProgress = false`,
//! `log_min_error_statement = ERROR`, `Log_error_verbosity = PGERROR_DEFAULT`,
//! `Log_line_prefix = NULL`, `Log_destination = LOG_DESTINATION_STDERR`,
//! `syslog_sequence_numbers = syslog_split_messages = true`, and all the
//! process-state booleans `false` / zero.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::Mutex;

use types_dest::{CommandDest, DestNone};
use types_error::{
    ErrorLevel, PgError, PgResult, ERROR, LOG_DESTINATION_CSVLOG, LOG_DESTINATION_JSONLOG,
    LOG_DESTINATION_STDERR, LOG_DESTINATION_SYSLOG, NOTICE, PGERROR_DEFAULT, WARNING,
};

// ---------------------------------------------------------------------------
// Output-decision GUCs (read on every report)
// ---------------------------------------------------------------------------

static LOG_MIN_MESSAGES: AtomicI32 = AtomicI32::new(WARNING.0);
static CLIENT_MIN_MESSAGES: AtomicI32 = AtomicI32::new(NOTICE.0);
static WHERE_TO_SEND_OUTPUT: AtomicU32 = AtomicU32::new(DestNone);
static CLIENT_AUTH_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
static LOG_MIN_ERROR_STATEMENT: AtomicI32 = AtomicI32::new(ERROR.0);

pub fn log_min_messages() -> ErrorLevel {
    ErrorLevel(LOG_MIN_MESSAGES.load(Ordering::Relaxed))
}

pub fn set_log_min_messages(level: ErrorLevel) {
    LOG_MIN_MESSAGES.store(level.0, Ordering::Relaxed);
}

pub fn client_min_messages() -> ErrorLevel {
    ErrorLevel(CLIENT_MIN_MESSAGES.load(Ordering::Relaxed))
}

pub fn set_client_min_messages(level: ErrorLevel) {
    CLIENT_MIN_MESSAGES.store(level.0, Ordering::Relaxed);
}

pub fn where_to_send_output() -> CommandDest {
    WHERE_TO_SEND_OUTPUT.load(Ordering::Relaxed)
}

pub fn set_where_to_send_output(dest: CommandDest) {
    WHERE_TO_SEND_OUTPUT.store(dest, Ordering::Relaxed);
}

pub fn client_auth_in_progress() -> bool {
    CLIENT_AUTH_IN_PROGRESS.load(Ordering::Relaxed)
}

pub fn set_client_auth_in_progress(value: bool) {
    CLIENT_AUTH_IN_PROGRESS.store(value, Ordering::Relaxed);
}

pub fn log_min_error_statement() -> ErrorLevel {
    ErrorLevel(LOG_MIN_ERROR_STATEMENT.load(Ordering::Relaxed))
}

pub fn set_log_min_error_statement(level: ErrorLevel) {
    LOG_MIN_ERROR_STATEMENT.store(level.0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Server-log formatting GUCs (elog.c file-level globals)
// ---------------------------------------------------------------------------

static LOG_ERROR_VERBOSITY: AtomicI32 = AtomicI32::new(PGERROR_DEFAULT);
static LOG_LINE_PREFIX: Mutex<Option<String>> = Mutex::new(None);
static LOG_DESTINATION: AtomicI32 = AtomicI32::new(LOG_DESTINATION_STDERR);
static SYSLOG_SEQUENCE_NUMBERS: AtomicBool = AtomicBool::new(true);
static SYSLOG_SPLIT_MESSAGES: AtomicBool = AtomicBool::new(true);

pub fn log_error_verbosity() -> i32 {
    LOG_ERROR_VERBOSITY.load(Ordering::Relaxed)
}

pub fn set_log_error_verbosity(verbosity: i32) {
    LOG_ERROR_VERBOSITY.store(verbosity, Ordering::Relaxed);
}

/// `Log_line_prefix` — `None` mirrors the C boot state (NULL pointer until the
/// GUC machinery runs).
pub fn log_line_prefix_format() -> Option<String> {
    LOG_LINE_PREFIX.lock().expect("log_line_prefix poisoned").clone()
}

pub fn set_log_line_prefix(format: Option<String>) {
    *LOG_LINE_PREFIX.lock().expect("log_line_prefix poisoned") = format;
}

pub fn log_destination() -> i32 {
    LOG_DESTINATION.load(Ordering::Relaxed)
}

pub fn syslog_sequence_numbers() -> bool {
    SYSLOG_SEQUENCE_NUMBERS.load(Ordering::Relaxed)
}

pub fn set_syslog_sequence_numbers(value: bool) {
    SYSLOG_SEQUENCE_NUMBERS.store(value, Ordering::Relaxed);
}

pub fn syslog_split_messages() -> bool {
    SYSLOG_SPLIT_MESSAGES.load(Ordering::Relaxed)
}

pub fn set_syslog_split_messages(value: bool) {
    SYSLOG_SPLIT_MESSAGES.store(value, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Cross-subsystem process state read by elog.c (owned globals elsewhere in C;
// mirrored here with setters for the owning units)
// ---------------------------------------------------------------------------

/// `CritSectionCount` (miscadmin.h; owned by the crit-section machinery).
static CRIT_SECTION_COUNT: AtomicU32 = AtomicU32::new(0);
/// `ExitOnAnyError` (globals.c; initdb sets it).
static EXIT_ON_ANY_ERROR: AtomicBool = AtomicBool::new(false);
/// `proc_exit_inprogress` (storage/ipc/ipc.c).
static PROC_EXIT_INPROGRESS: AtomicBool = AtomicBool::new(false);
/// `redirection_done` (postmaster.c): stderr is redirected into the syslogger pipe.
static REDIRECTION_DONE: AtomicBool = AtomicBool::new(false);
/// Mirrors `MyBackendType == B_LOGGER` (miscinit.c): we ARE the syslogger.
static AM_SYSLOGGER: AtomicBool = AtomicBool::new(false);
/// `IsUnderPostmaster` (globals.c).
static IS_UNDER_POSTMASTER: AtomicBool = AtomicBool::new(false);
/// `FrontendProtocol` (globals.c); 0 = not yet negotiated.
static FRONTEND_PROTOCOL: AtomicU32 = AtomicU32::new(0);
/// `OutputFileName` (globals.c); empty = none.
static OUTPUT_FILE_NAME: Mutex<Option<String>> = Mutex::new(None);

pub fn crit_section_count() -> u32 {
    CRIT_SECTION_COUNT.load(Ordering::Relaxed)
}

pub fn set_crit_section_count(count: u32) {
    CRIT_SECTION_COUNT.store(count, Ordering::Relaxed);
}

pub fn exit_on_any_error() -> bool {
    EXIT_ON_ANY_ERROR.load(Ordering::Relaxed)
}

pub fn set_exit_on_any_error(value: bool) {
    EXIT_ON_ANY_ERROR.store(value, Ordering::Relaxed);
}

pub fn proc_exit_inprogress() -> bool {
    PROC_EXIT_INPROGRESS.load(Ordering::Relaxed)
}

pub fn set_proc_exit_inprogress(value: bool) {
    PROC_EXIT_INPROGRESS.store(value, Ordering::Relaxed);
}

pub fn redirection_done() -> bool {
    REDIRECTION_DONE.load(Ordering::Relaxed)
}

pub fn set_redirection_done(value: bool) {
    REDIRECTION_DONE.store(value, Ordering::Relaxed);
}

pub fn am_syslogger() -> bool {
    AM_SYSLOGGER.load(Ordering::Relaxed)
}

pub fn set_am_syslogger(value: bool) {
    AM_SYSLOGGER.store(value, Ordering::Relaxed);
}

pub fn is_under_postmaster() -> bool {
    IS_UNDER_POSTMASTER.load(Ordering::Relaxed)
}

pub fn set_is_under_postmaster(value: bool) {
    IS_UNDER_POSTMASTER.store(value, Ordering::Relaxed);
}

pub fn frontend_protocol() -> u32 {
    FRONTEND_PROTOCOL.load(Ordering::Relaxed)
}

pub fn set_frontend_protocol(version: u32) {
    FRONTEND_PROTOCOL.store(version, Ordering::Relaxed);
}

pub fn output_file_name() -> Option<String> {
    OUTPUT_FILE_NAME.lock().expect("output_file_name poisoned").clone()
}

pub fn set_output_file_name(name: Option<String>) {
    *OUTPUT_FILE_NAME.lock().expect("output_file_name poisoned") = name;
}

// ---------------------------------------------------------------------------
// backtrace_functions GUC (check/assign hooks + matcher)
// ---------------------------------------------------------------------------

/// Processed form of the `backtrace_functions` GUC. The C representation is a
/// `\0`-separated, `\0\0`-terminated list; the Vec keeps the entries verbatim
/// (including any empty entries a `",,"` produces) so [`matches_backtrace_functions`]
/// can reproduce the C scan exactly: an empty entry terminates the list, hiding
/// anything after it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BacktraceFunctionList {
    functions: Vec<String>,
}

impl BacktraceFunctionList {
    pub fn functions(&self) -> &[String] {
        &self.functions
    }

    /// `matches_backtrace_functions` over this list (the C `p += strlen(p) + 1`
    /// walk: stop at the first empty entry).
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

static BACKTRACE_FUNCTION_LIST: Mutex<Option<BacktraceFunctionList>> = Mutex::new(None);

/// GUC check_hook for `backtrace_functions`: validate the charset and split on
/// commas, ignoring space/newline/tab. `Err` carries the `GUC_check_errdetail`
/// text; `Ok(None)` means the empty setting.
pub fn check_backtrace_functions(newval: &str) -> PgResult<Option<BacktraceFunctionList>> {
    let valid = |b: u8| {
        b.is_ascii_digit()
            || b == b'_'
            || b.is_ascii_lowercase()
            || b.is_ascii_uppercase()
            || matches!(b, b',' | b' ' | b'\n' | b'\t')
    };
    if !newval.bytes().all(valid) {
        return Err(PgError::error("Invalid character."));
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

/// GUC assign_hook for `backtrace_functions`.
pub fn assign_backtrace_functions(extra: Option<BacktraceFunctionList>) {
    *BACKTRACE_FUNCTION_LIST
        .lock()
        .expect("backtrace_functions poisoned") = extra;
}

/// `matches_backtrace_functions(funcname)` — does the given function name
/// appear in the processed `backtrace_functions` list?
pub fn matches_backtrace_functions(funcname: &str) -> bool {
    BACKTRACE_FUNCTION_LIST
        .lock()
        .expect("backtrace_functions poisoned")
        .as_ref()
        .is_some_and(|list| list.matches(funcname))
}

// ---------------------------------------------------------------------------
// log_destination GUC (check/assign hooks)
// ---------------------------------------------------------------------------

/// GUC check_hook for `log_destination`. The win32-only `eventlog` keyword is
/// not accepted (win32 branches are out of scope for this port).
pub fn check_log_destination(newval: &str) -> PgResult<i32> {
    let identifiers = split_identifier_string(newval, ',')?;
    let mut newlogdest = 0;

    for tok in identifiers {
        // pg_strcasecmp: keyword match is case-insensitive even for quoted
        // identifiers SplitIdentifierString left in original case.
        if tok.eq_ignore_ascii_case("stderr") {
            newlogdest |= LOG_DESTINATION_STDERR;
        } else if tok.eq_ignore_ascii_case("csvlog") {
            newlogdest |= LOG_DESTINATION_CSVLOG;
        } else if tok.eq_ignore_ascii_case("jsonlog") {
            newlogdest |= LOG_DESTINATION_JSONLOG;
        } else if tok.eq_ignore_ascii_case("syslog") {
            newlogdest |= LOG_DESTINATION_SYSLOG;
        } else {
            return Err(PgError::error(format!(
                "Unrecognized key word: \"{}\".",
                tok
            )));
        }
    }

    Ok(newlogdest)
}

/// GUC assign_hook for `log_destination`.
pub fn assign_log_destination(extra: i32) {
    LOG_DESTINATION.store(extra, Ordering::Relaxed);
}

/// GUC assign_hook for `syslog_ident`.
pub fn assign_syslog_ident(newval: &str) {
    crate::syslog::assign_syslog_ident(newval);
}

/// GUC assign_hook for `syslog_facility`.
pub fn assign_syslog_facility(newval: i32) {
    crate::syslog::assign_syslog_facility(newval);
}

/// `SplitIdentifierString` (varlena.c) semantics inlined for the
/// `log_destination` list: comma-separated identifiers, unquoted ones
/// whitespace-trimmed and downcased, double-quoted ones taken verbatim with
/// `""` as an escaped quote. `Err` is the C `false` (syntax error) return.
fn split_identifier_string(raw: &str, separator: char) -> PgResult<Vec<String>> {
    let syntax_error = || PgError::error("List syntax is invalid.");
    let mut chars = raw.char_indices().peekable();
    let mut identifiers = Vec::new();

    while matches!(chars.peek(), Some((_, ch)) if ch.is_whitespace()) {
        chars.next();
    }
    if chars.peek().is_none() {
        return Ok(identifiers);
    }

    loop {
        let identifier = if matches!(chars.peek(), Some((_, '"'))) {
            // Quoted identifier: take verbatim, "" -> "
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
                    None => return Err(syntax_error()),
                }
            }
            identifier
        } else {
            // Unquoted identifier: up to separator or whitespace, downcased
            let mut identifier = String::new();
            while let Some((_, ch)) = chars.peek().copied() {
                if ch == separator || ch.is_whitespace() {
                    break;
                }
                chars.next();
                identifier.extend(ch.to_lowercase());
            }
            if identifier.is_empty() {
                return Err(syntax_error());
            }
            identifier
        };

        while matches!(chars.peek(), Some((_, ch)) if ch.is_whitespace()) {
            chars.next();
        }
        match chars.peek().copied() {
            Some((_, ch)) if ch == separator => {
                chars.next();
                while matches!(chars.peek(), Some((_, ch)) if ch.is_whitespace()) {
                    chars.next();
                }
                identifiers.push(identifier);
                if chars.peek().is_none() {
                    return Err(syntax_error());
                }
            }
            Some(_) => return Err(syntax_error()),
            None => {
                identifiers.push(identifier);
                break;
            }
        }
    }

    Ok(identifiers)
}
