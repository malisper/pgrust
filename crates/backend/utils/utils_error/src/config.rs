//! elog.c's GUC parameters and the cross-subsystem globals it reads.
//!
//! These are OWNED by this crate (not seamed): every static holds PostgreSQL's
//! boot-time default and exposes a `pub` setter that the owning unit (guc,
//! postmaster, miscinit, tcop, storage/ipc) calls when it lands. `guc ->
//! error` is acyclic, so the setters are plain function calls — no seams and
//! no possible panic on the logging hot path.
//!
//! Boot defaults mirror the C initializers / GUC boot values:
//! Per AGENTS.md "Backend-global state": these are per-backend values (C
//! per-process globals), so they live in `thread_local!` — one session's SET
//! must never leak into another backend's thread.
//!
//! `log_min_messages = WARNING`, `client_min_messages = NOTICE`,
//! `whereToSendOutput = DestNone`, `ClientAuthInProgress = false`,
//! `log_min_error_statement = ERROR`, `Log_error_verbosity = PGERROR_DEFAULT`,
//! `Log_line_prefix = NULL`, `Log_destination = LOG_DESTINATION_STDERR`,
//! `syslog_sequence_numbers = syslog_split_messages = true`, and all the
//! process-state booleans `false` / zero.

use std::cell::{Cell, RefCell};

use ::types_core::NAMEDATALEN;
use ::types_dest::CommandDest;
use types_error::{
    ErrorLevel, PGErrorVerbosity, PgError, PgResult, ERROR, LOG_DESTINATION_CSVLOG,
    LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR, LOG_DESTINATION_SYSLOG, NOTICE, WARNING,
};

// ---------------------------------------------------------------------------
// Output-decision GUCs (read on every report)
// ---------------------------------------------------------------------------

thread_local! { static LOG_MIN_MESSAGES: Cell<i32> = const { Cell::new(WARNING.0) }; }
thread_local! { static CLIENT_MIN_MESSAGES: Cell<i32> = const { Cell::new(NOTICE.0) }; }
// `CommandDest whereToSendOutput = DestDebug;` (postgres.c:91). This is the
// single canonical home for the per-backend output destination: the error
// reporter reads it here, `BackendInitialize` sets it to `DestRemote`, and the
// tcop `ReadCommand` / postmaster / async / walsender consumers all delegate to
// this cell (tcop-postgres `globals::where_to_send_output` forwards here). The
// C default is `DestDebug` (server log / single-user interactive), NOT `None`.
thread_local! { static WHERE_TO_SEND_OUTPUT: Cell<CommandDest> = const { Cell::new(CommandDest::Debug) }; }
thread_local! { static CLIENT_AUTH_IN_PROGRESS: Cell<bool> = const { Cell::new(false) }; }
thread_local! { static LOG_MIN_ERROR_STATEMENT: Cell<i32> = const { Cell::new(ERROR.0) }; }

pub fn log_min_messages() -> ErrorLevel {
    ErrorLevel(LOG_MIN_MESSAGES.with(Cell::get))
}

pub fn set_log_min_messages(level: ErrorLevel) {
    LOG_MIN_MESSAGES.with(|c| c.set(level.0));
}

pub fn client_min_messages() -> ErrorLevel {
    ErrorLevel(CLIENT_MIN_MESSAGES.with(Cell::get))
}

pub fn set_client_min_messages(level: ErrorLevel) {
    CLIENT_MIN_MESSAGES.with(|c| c.set(level.0));
}

pub fn where_to_send_output() -> CommandDest {
    WHERE_TO_SEND_OUTPUT.with(Cell::get)
}

pub fn set_where_to_send_output(dest: CommandDest) {
    WHERE_TO_SEND_OUTPUT.with(|c| c.set(dest));
}

pub fn client_auth_in_progress() -> bool {
    CLIENT_AUTH_IN_PROGRESS.with(Cell::get)
}

pub fn set_client_auth_in_progress(value: bool) {
    CLIENT_AUTH_IN_PROGRESS.with(|c| c.set(value));
}

pub fn log_min_error_statement() -> ErrorLevel {
    ErrorLevel(LOG_MIN_ERROR_STATEMENT.with(Cell::get))
}

pub fn set_log_min_error_statement(level: ErrorLevel) {
    LOG_MIN_ERROR_STATEMENT.with(|c| c.set(level.0));
}

// ---------------------------------------------------------------------------
// Server-log formatting GUCs (elog.c file-level globals)
// ---------------------------------------------------------------------------

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

/// `Log_line_prefix` — `None` mirrors the C boot state (NULL pointer until the
/// GUC machinery runs).
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

// ---------------------------------------------------------------------------
// Cross-subsystem process state read by elog.c (owned globals elsewhere in C;
// mirrored here with setters for the owning units)
// ---------------------------------------------------------------------------

/// `CritSectionCount` (miscadmin.h; owned by the crit-section machinery).
thread_local! { static CRIT_SECTION_COUNT: Cell<u32> = const { Cell::new(0) }; }
/// `ExitOnAnyError` (globals.c; initdb sets it).
thread_local! { static EXIT_ON_ANY_ERROR: Cell<bool> = const { Cell::new(false) }; }
/// `proc_exit_inprogress` (storage/ipc/ipc.c).
thread_local! { static PROC_EXIT_INPROGRESS: Cell<bool> = const { Cell::new(false) }; }
/// `redirection_done` (postmaster.c): stderr is redirected into the syslogger pipe.
thread_local! { static REDIRECTION_DONE: Cell<bool> = const { Cell::new(false) }; }
/// Mirrors `MyBackendType == B_LOGGER` (miscinit.c): we ARE the syslogger.
thread_local! { static AM_SYSLOGGER: Cell<bool> = const { Cell::new(false) }; }
/// `IsUnderPostmaster` (globals.c).
thread_local! { static IS_UNDER_POSTMASTER: Cell<bool> = const { Cell::new(false) }; }
/// `FrontendProtocol` (globals.c); 0 = not yet negotiated.
thread_local! { static FRONTEND_PROTOCOL: Cell<u32> = const { Cell::new(0) }; }
/// `OutputFileName` (globals.c); empty = none.
thread_local! { static OUTPUT_FILE_NAME: RefCell<Option<String>> = const { RefCell::new(None) }; }

pub fn crit_section_count() -> u32 {
    CRIT_SECTION_COUNT.with(Cell::get)
}

pub fn set_crit_section_count(count: u32) {
    CRIT_SECTION_COUNT.with(|c| c.set(count));
}

pub fn exit_on_any_error() -> bool {
    EXIT_ON_ANY_ERROR.with(Cell::get)
}

pub fn set_exit_on_any_error(value: bool) {
    EXIT_ON_ANY_ERROR.with(|c| c.set(value));
}

pub fn proc_exit_inprogress() -> bool {
    PROC_EXIT_INPROGRESS.with(Cell::get)
}

pub fn set_proc_exit_inprogress(value: bool) {
    PROC_EXIT_INPROGRESS.with(|c| c.set(value));
}

pub fn redirection_done() -> bool {
    REDIRECTION_DONE.with(Cell::get)
}

pub fn set_redirection_done(value: bool) {
    REDIRECTION_DONE.with(|c| c.set(value));
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

thread_local! { static BACKTRACE_FUNCTION_LIST: RefCell<Option<BacktraceFunctionList>> = const { RefCell::new(None) }; }

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
    BACKTRACE_FUNCTION_LIST.with(|c| *c.borrow_mut() = extra);
}

/// `matches_backtrace_functions(funcname)` — does the given function name
/// appear in the processed `backtrace_functions` list?
pub fn matches_backtrace_functions(funcname: &str) -> bool {
    BACKTRACE_FUNCTION_LIST.with(|c| c.borrow().as_ref().is_some_and(|list| list.matches(funcname)))
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
    LOG_DESTINATION.with(|c| c.set(extra));
}

/// GUC assign_hook for `syslog_ident`.
pub fn assign_syslog_ident(newval: &str) {
    crate::syslog::assign_syslog_ident(newval);
}

/// GUC var accessor get for `syslog_facility`: read the live facility the
/// assign_hook installed into the (process-global) syslog connection state.
/// Mirrors C reading the `int syslog_facility` GUC variable.
pub fn syslog_facility() -> i32 {
    crate::syslog::syslog_facility()
}

/// GUC assign_hook for `syslog_facility`.
pub fn assign_syslog_facility(newval: i32) {
    crate::syslog::assign_syslog_facility(newval);
}

/// `Log_destination_string` — the raw GUC string the user set (e.g.
/// `"stderr,csvlog"`). In C this is the `char *Log_destination_string` GUC
/// variable; the assign_hook parses it into the `int Log_destination` bitmask
/// (mirrored by [`LOG_DESTINATION`]). Boots to "stderr" like the C boot value.
pub fn log_destination_string() -> Option<String> {
    LOG_DESTINATION_STRING.with(|c| c.borrow().clone())
}

pub fn set_log_destination_string(value: Option<String>) {
    LOG_DESTINATION_STRING.with(|c| *c.borrow_mut() = value);
}

/// `scanner_isspace` (scansup.c): the lexer's {space} set — NOT Unicode
/// whitespace.
fn scanner_isspace(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\n' | '\r' | '\x0c')
}

/// `SplitIdentifierString` (varlena.c) semantics, duplicated here until the
/// adt/varlena unit lands (recorded on its CATALOG.tsv row): comma-separated
/// identifiers; unquoted ones trimmed and put through
/// `downcase_truncate_identifier` (scansup.c) — ASCII downcasing (the
/// high-bit `tolower` branch only fires in single-byte encodings; owned
/// strings are UTF-8) plus truncation to NAMEDATALEN-1 bytes on a character
/// boundary; double-quoted ones taken verbatim with `""` as an escaped quote.
/// `Err` is the C `false` (syntax error) return.
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
            // Unquoted identifier: up to separator or whitespace, then
            // downcase_truncate_identifier.
            let mut identifier = String::new();
            while let Some((_, ch)) = chars.peek().copied() {
                if ch == separator || scanner_isspace(ch) {
                    break;
                }
                chars.next();
                identifier.push(ch.to_ascii_lowercase());
            }
            if identifier.is_empty() {
                return Err(syntax_error());
            }
            // truncate_identifier: clip to NAMEDATALEN-1 bytes at a
            // character boundary.
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
