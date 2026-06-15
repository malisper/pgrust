#![allow(non_snake_case)]

use std::sync::Mutex;

use pgrust_pg_ffi::{
    LOG_DESTINATION_CSVLOG, LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR, LOG_DESTINATION_SYSLOG,
};

use crate::{PgError, PgResult};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BacktraceFunctionList {
    functions: Vec<String>,
}

impl BacktraceFunctionList {
    pub fn functions(&self) -> &[String] {
        &self.functions
    }

    pub fn matches(&self, funcname: &str) -> bool {
        !funcname.is_empty()
            && self
                .functions
                .iter()
                .any(|function| function.as_str() == funcname)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogDestination(pub i32);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorLogConfig {
    pub log_destination: LogDestination,
    pub syslog_ident: Option<String>,
    pub syslog_facility: i32,
    pub syslog_open: bool,
    pub backtrace_functions: Option<BacktraceFunctionList>,
}

impl Default for ErrorLogConfig {
    fn default() -> Self {
        Self {
            log_destination: LogDestination(LOG_DESTINATION_STDERR),
            syslog_ident: None,
            syslog_facility: default_syslog_facility(),
            syslog_open: false,
            backtrace_functions: None,
        }
    }
}

static ERROR_LOG_CONFIG: Mutex<ErrorLogConfig> = Mutex::new(ErrorLogConfig {
    log_destination: LogDestination(LOG_DESTINATION_STDERR),
    syslog_ident: None,
    syslog_facility: default_syslog_facility(),
    syslog_open: false,
    backtrace_functions: None,
});

pub fn error_log_config() -> ErrorLogConfig {
    ERROR_LOG_CONFIG
        .lock()
        .expect("error log config lock poisoned")
        .clone()
}

pub fn check_backtrace_functions(newval: &str) -> PgResult<Option<BacktraceFunctionList>> {
    if !newval.bytes().all(valid_backtrace_function_byte) {
        return Err(PgError::error("Invalid character."));
    }

    if newval.is_empty() {
        return Ok(None);
    }

    let mut functions = Vec::new();
    let mut current = String::new();
    for byte in newval.bytes() {
        match byte {
            b',' => {
                functions.push(std::mem::take(&mut current));
            }
            b' ' | b'\n' | b'\t' => {}
            _ => current.push(byte as char),
        }
    }
    functions.push(current);

    Ok(Some(BacktraceFunctionList { functions }))
}

pub fn assign_backtrace_functions(
    _newval: &str,
    extra: Option<BacktraceFunctionList>,
) -> PgResult<()> {
    ERROR_LOG_CONFIG
        .lock()
        .expect("error log config lock poisoned")
        .backtrace_functions = extra;
    Ok(())
}

pub fn matches_backtrace_functions(funcname: &str) -> bool {
    ERROR_LOG_CONFIG
        .lock()
        .expect("error log config lock poisoned")
        .backtrace_functions
        .as_ref()
        .is_some_and(|functions| functions.matches(funcname))
}

pub fn check_log_destination(newval: &str) -> PgResult<LogDestination> {
    let identifiers = split_identifier_string(newval, ',')?;
    let mut destination = 0;

    for identifier in identifiers {
        match identifier.as_str() {
            "stderr" => destination |= LOG_DESTINATION_STDERR,
            "csvlog" => destination |= LOG_DESTINATION_CSVLOG,
            "jsonlog" => destination |= LOG_DESTINATION_JSONLOG,
            "syslog" => destination |= LOG_DESTINATION_SYSLOG,
            keyword => {
                return Err(PgError::error(format!(
                    "Unrecognized key word: \"{}\".",
                    keyword
                )));
            }
        }
    }

    Ok(LogDestination(destination))
}

pub fn assign_log_destination(_newval: &str, extra: LogDestination) -> PgResult<()> {
    ERROR_LOG_CONFIG
        .lock()
        .expect("error log config lock poisoned")
        .log_destination = extra;
    Ok(())
}

pub fn assign_syslog_ident(newval: &str) -> PgResult<()> {
    let mut config = ERROR_LOG_CONFIG
        .lock()
        .expect("error log config lock poisoned");
    if config.syslog_ident.as_deref() != Some(newval) {
        config.syslog_open = false;
        config.syslog_ident = Some(newval.to_owned());
    }
    Ok(())
}

pub fn assign_syslog_facility(newval: i32) -> PgResult<()> {
    let mut config = ERROR_LOG_CONFIG
        .lock()
        .expect("error log config lock poisoned");
    if config.syslog_facility != newval {
        config.syslog_open = false;
        config.syslog_facility = newval;
    }
    Ok(())
}

fn split_identifier_string(raw: &str, separator: char) -> PgResult<Vec<String>> {
    let mut chars = raw.char_indices().peekable();
    let mut identifiers = Vec::new();

    skip_whitespace(&mut chars);
    if chars.peek().is_none() {
        return Ok(identifiers);
    }

    loop {
        let identifier = if matches!(chars.peek(), Some((_, '"'))) {
            parse_quoted_identifier(&mut chars)?
        } else {
            parse_unquoted_identifier(&mut chars, separator)?
        };

        skip_whitespace(&mut chars);
        match chars.peek().copied() {
            Some((_, ch)) if ch == separator => {
                chars.next();
                skip_whitespace(&mut chars);
            }
            Some(_) => return Err(PgError::error("List syntax is invalid.")),
            None => {
                identifiers.push(identifier);
                break;
            }
        }

        identifiers.push(identifier);
        if chars.peek().is_none() {
            return Err(PgError::error("List syntax is invalid."));
        }
    }

    Ok(identifiers)
}

fn parse_quoted_identifier(
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
) -> PgResult<String> {
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
            None => return Err(PgError::error("List syntax is invalid.")),
        }
    }

    Ok(identifier)
}

fn parse_unquoted_identifier(
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    separator: char,
) -> PgResult<String> {
    let mut identifier = String::new();
    while let Some((_, ch)) = chars.peek().copied() {
        if ch == separator || ch.is_whitespace() {
            break;
        }
        chars.next();
        identifier.extend(ch.to_lowercase());
    }

    if identifier.is_empty() {
        return Err(PgError::error("List syntax is invalid."));
    }

    Ok(identifier)
}

fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    while matches!(chars.peek(), Some((_, ch)) if ch.is_whitespace()) {
        chars.next();
    }
}

fn valid_backtrace_function_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b',' | b' ' | b'\n' | b'\t')
}

const fn default_syslog_facility() -> i32 {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        libc::LOG_LOCAL0
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}
