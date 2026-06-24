//! `tcop/postgres.c` — the backend's main processing loop (F0a).
//!
//! This module ports the post-auth backend driver:
//!
//!   * [`PostgresMain`]   — postgres.c:4184 (the `for (;;)` ReadCommand loop +
//!     error-recovery block + idle-state handling + message-tag dispatch)
//!   * [`ReadCommand`]    — postgres.c:479
//!   * [`SocketBackend`]  — postgres.c:351 (frontend message read)
//!   * [`forbidden_in_wal_sender`] — postgres.c:5031
//!
//! The simple-Query (`'Q'`) path is landed end-to-end:
//! [`PostgresMain`] reads the `'Q'` message, extracts the query string into the
//! per-message `MessageContext`, and calls
//! [`crate::simple_query::exec_simple_query`], whose parse→analyze→rewrite→plan
//! →portal→run pipeline is complete.
//!
//! # Sanctioned divergences (audit against these)
//!
//! 1. **`sigsetjmp` → `PgResult` recovery.** The C `if (sigsetjmp(...))` outer
//!    error handler becomes: the per-command work runs in a helper returning
//!    `PgResult`; on `Err` the loop runs the recovery block ([`error_recovery`])
//!    — `EmitErrorReport`/`FlushErrorState`/`AbortCurrentTransaction` — exactly
//!    as the C catch arm does, then continues the loop. This is the
//!    backend-utils-error sanctioned model (PG_exception_stack does not exist).
//! 2. **Per-iteration `MessageContext`.** C resets a long-lived `MessageContext`
//!    once per loop; here each iteration creates a child `MemoryContext` off the
//!    backend top context and lets it drop at end of iteration (the `'mcx`
//!    region is the iteration body). The query string is copied into it (it
//!    points into `MessageContext` in C too).
//! 3. **Unported message paths seam-and-panic.** The extended-query protocol
//!    (`'P'`/`'B'`/`'E'`/`'D'`) exec functions (`exec_parse_message` etc.) are
//!    the F2 family and exist nowhere yet; their dispatch arms panic with a
//!    precise rationale (the simple-Query target never reaches them). The
//!    `'C'` Close and `'S'` Sync arms are ported (their deps exist). Fastpath
//!    (`'F'`) is landed (backend-tcop-fastpath). COPY-protocol data messages
//!    are accepted-and-ignored per spec.

#![allow(non_snake_case)]

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
extern crate alloc;

use alloc::string::String;

use ::mcx::{Mcx, MemoryContext};
use ::types_dest::dest::CommandDest;
use ::types_error::{PgResult, ERROR, FATAL};
use ::stringinfo::StringInfo;
use ::types_timeout::TimeoutId;

use ::utils_error::ereport;

use crate::globals;

// Seam crate aliases.
use transam_xact_seams as xact_seams;
use dest_seams as dest_seams;
use status_seams as status_seams;
use more_seams as more_seams;

// Owner crates called directly for entry points with no consumable seam
// (acyclic: none depends on this crate — fastpath/pquery dep only the
// `*-seams` leaves, verified at the Cargo level).
use transam_xact as xact;
use pqcomm as pqcomm;
use pqformat as pqformat;
use interrupt as pm_interrupt;
use fastpath as fastpath;
use misc_timeout as timeout;

// ===========================================================================
// PqMsg_* frontend message type codes (protocol.h)
// ===========================================================================

mod pqmsg {
    pub const QUERY: i32 = b'Q' as i32;
    pub const PARSE: i32 = b'P' as i32;
    pub const BIND: i32 = b'B' as i32;
    pub const EXECUTE: i32 = b'E' as i32;
    pub const FUNCTION_CALL: i32 = b'F' as i32;
    pub const CLOSE: i32 = b'C' as i32;
    pub const DESCRIBE: i32 = b'D' as i32;
    pub const FLUSH: i32 = b'H' as i32;
    pub const SYNC: i32 = b'S' as i32;
    pub const TERMINATE: i32 = b'X' as i32;
    pub const COPY_DATA: i32 = b'd' as i32;
    pub const COPY_DONE: i32 = b'c' as i32;
    pub const COPY_FAIL: i32 = b'f' as i32;
    pub const CLOSE_COMPLETE: u8 = b'3';
}

/// `EOF` sentinel returned by [`ReadCommand`]/[`SocketBackend`] on a lost
/// connection (C's `EOF == -1`).
const EOF: i32 = -1;

std::thread_local! {
    /// Regress-output mode: the text of the statement currently being read/run,
    /// captured by `interactive_backend_regress`. `debug_query_string` is left
    /// NULL in the single-user path (the `'mcx`->`'static` store is skipped, see
    /// simple_query.rs), so `emit_regress_error` reads the offending statement
    /// here for the psql `LINE n:`/caret echo.
    static REGRESS_CUR_QUERY: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// `PQ_SMALL_MESSAGE_LIMIT` (libpq.h): cap for short fixed-shape messages.
const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;
/// `PQ_LARGE_MESSAGE_LIMIT` (libpq.h): `MaxAllocSize - 1`.
const PQ_LARGE_MESSAGE_LIMIT: i32 = ::mcx::MAX_ALLOC_SIZE as i32 - 1;

// ===========================================================================
// SocketBackend — postgres.c:351
// ===========================================================================

/// `SocketBackend(inBuf)` (postgres.c:351) — read one frontend message,
/// returning its type code and loading the body into `in_buf`. Returns
/// [`EOF`] on a lost connection.
///
/// `HOLD_CANCEL_INTERRUPTS`/`RESUME_CANCEL_INTERRUPTS` bracket the read in C;
/// the cancel-holdoff counter is owned by the interrupt machinery. We mirror
/// the read sequence (`pq_startmsgread`, `pq_getbyte`, validate, `pq_getmessage`)
/// and set the extended-query / skip-till-Sync flags exactly as C does.
fn SocketBackend(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    // HOLD_CANCEL_INTERRUPTS();
    // (cancel-holdoff bracket is interrupt-machinery state; the read itself is
    // faithful.)
    pqcomm::pq_startmsgread()?;
    let qtype = pqcomm::pq_getbyte()?;

    if qtype == EOF {
        // frontend disconnected
        if xact::IsTransactionState() {
            return Err(ereport(ERROR)
                .errcode(::types_error::error::ERRCODE_CONNECTION_FAILURE)
                .errmsg(
                    "unexpected EOF on client connection with an open transaction",
                )
                .into_error());
        } else {
            // Can't send DEBUG to client now; disconnecting, so don't restore
            // whereToSendOutput.
            globals::set_where_to_send_output(CommandDest::None);
            // ereport(DEBUG1, "unexpected EOF on client connection") — a DEBUG
            // line, dropped (logging-only, below the default threshold).
        }
        return Ok(qtype);
    }

    // Validate the message type code, choose a type-dependent length limit, and
    // set doing_extended_query_message / ignore_till_sync as early as possible.
    let maxmsglen: i32 = match qtype {
        x if x == pqmsg::QUERY => {
            globals::set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::FUNCTION_CALL => {
            globals::set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::TERMINATE => {
            globals::set_doing_extended_query_message(false);
            globals::set_ignore_till_sync(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::BIND || x == pqmsg::PARSE => {
            globals::set_doing_extended_query_message(true);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::CLOSE
            || x == pqmsg::DESCRIBE
            || x == pqmsg::EXECUTE
            || x == pqmsg::FLUSH =>
        {
            globals::set_doing_extended_query_message(true);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::SYNC => {
            // stop any active skip-till-Sync
            globals::set_ignore_till_sync(false);
            // mark not-extended, so a new error doesn't begin skip
            globals::set_doing_extended_query_message(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        x if x == pqmsg::COPY_DATA => {
            globals::set_doing_extended_query_message(false);
            PQ_LARGE_MESSAGE_LIMIT
        }
        x if x == pqmsg::COPY_DONE || x == pqmsg::COPY_FAIL => {
            globals::set_doing_extended_query_message(false);
            PQ_SMALL_MESSAGE_LIMIT
        }
        other => {
            // Garbage from the frontend: probably lost message-boundary sync.
            // Fatal, no good recovery.
            return Err(ereport(FATAL)
                .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(alloc::format!("invalid frontend message type {other}"))
                .into_error());
        }
    };

    // In protocol v3 every frontend message has a length word after the type
    // code, so the body can be read independently of the type.
    if pqcomm::pq_getmessage(in_buf, maxmsglen)? != 0 {
        return Ok(EOF); // suitable message already logged
    }
    // RESUME_CANCEL_INTERRUPTS();

    Ok(qtype)
}

// ===========================================================================
// InteractiveBackend / interactive_getc — postgres.c:236 / :324
// ===========================================================================

/// `InteractiveBackend(inBuf)` (postgres.c:236) — called for user interactive
/// (single-user / standalone) connections. The string entered by the user is
/// placed in `in_buf` and we act like a `'Q'` (simple query) message was
/// received. Returns [`EOF`] if end-of-file input is seen (time to shut down).
fn InteractiveBackend(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    #[cfg(not(target_family = "wasm"))]
    use std::io::Write;

    // Regress-output mode (psql -a -q emulation): no "backend> " prompt; read a
    // whole `;`-terminated statement while echoing each raw input line verbatim
    // (psql `-a` echoes every line, including comments and blank lines), so the
    // backend's stdout byte-matches expected/*.out. Comment/blank lines that
    // precede a statement are echoed and accumulated into the buffer too — the
    // parser ignores leading comments/whitespace, so a multi-statement read that
    // begins with comments still parses to the intended statement.
    if globals::regress_output() {
        return interactive_backend_regress(in_buf);
    }

    // Display a prompt and obtain input from the user.
    interactive_print("backend> ");

    in_buf.reset(); // resetStringInfo(inBuf)

    // Read characters until EOF or the appropriate delimiter is seen.
    let mut c = interactive_getc()?;
    while c != EOF {
        if c == b'\n' as i32 {
            if globals::use_semi_newline_newline() {
                // In -j mode, semicolon followed by two newlines ends the
                // command; otherwise treat newline as a regular character.
                let len = in_buf.len();
                if len > 1
                    && in_buf.data[len - 1] == b'\n'
                    && in_buf.data[len - 2] == b';'
                {
                    // might as well drop the second newline
                    break;
                }
            } else {
                // In plain mode, newline ends the command unless preceded by
                // backslash.
                let len = in_buf.len();
                if len > 0 && in_buf.data[len - 1] == b'\\' {
                    // discard backslash from inBuf
                    in_buf.data.pop();
                    // discard newline too
                    c = interactive_getc()?;
                    continue;
                } else {
                    // keep the newline character, but end the command
                    in_buf.data.push(b'\n');
                    break;
                }
            }
        }

        // Not newline, or newline treated as a regular character.
        in_buf.data.push(c as u8);
        c = interactive_getc()?;
    }

    // No input before EOF signal means time to quit.
    if c == EOF && in_buf.len() == 0 {
        return Ok(EOF);
    }

    // Otherwise we have a user query so process it.

    // Add '\0' to make it look the same as the message case.
    in_buf.data.push(b'\0');

    // If the query echo flag was given, print the query.
    if globals::echo_query() {
        // inBuf->data is NUL-terminated; print up to (but not including) it.
        let text = core::str::from_utf8(&in_buf.data[..in_buf.len() - 1]).unwrap_or("");
        interactive_print(&format!("statement: {text}\n"));
    }

    Ok(pqmsg::QUERY) // PqMsg_Query
}

/// Regress-output reader: read one `;`-terminated statement, echoing each raw
/// input line verbatim (psql `-a`). Leading comment/blank lines are echoed and
/// included in the buffer (the parser skips them). Returns [`EOF`] when input
/// ends with nothing pending (after echoing any trailing comment lines).
fn interactive_backend_regress(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    in_buf.reset();
    let mut line: Vec<u8> = Vec::new();
    let mut saw_any = false;
    // psql echoes leading comment lines but does NOT include them in the query
    // string it sends to the server, so the parser's error position (and thus the
    // `LINE n:` echo) is relative to the statement alone. Mirror that: until the
    // first line carrying actual statement text, echo comment-only lines but do
    // NOT add them to `in_buf`.
    let mut stmt_started = false;
    loop {
        let c = interactive_getc()?;
        if c == EOF {
            // Flush a final unterminated line (e.g. trailing comment with no
            // newline): echo it and, if it held statement text, run it.
            if !line.is_empty() {
                echo_regress_line(&line);
                if stmt_started || !is_comment_only(&line) {
                    in_buf.data.extend_from_slice(&line);
                }
                saw_any = true;
            }
            if !saw_any || in_buf.len() == 0 {
                return Ok(EOF);
            }
            break;
        }
        let ch = c as u8;
        line.push(ch);
        if ch == b'\n' {
            let content_len = line.len() - 1; // bytes before the trailing '\n'
            let is_blank = content_len == 0 && !in_statement_literal(&in_buf.data);
            if is_blank {
                // Empirical psql `-X -a -q` rule (verified against the live
                // server): source blank lines are NEVER echoed (psql's MainLoop
                // skips empty lines via `continue` before the `puts(line)` echo).
                // The blank lines in expected/*.out come solely from psql printing
                // ONE blank line after each query RESULT SET (emitted by the
                // regress DestReceiver), not from echoing source blanks.
                line.clear();
                continue;
            }
            // Echo the completed line verbatim (psql -a).
            echo_regress_line(&line);
            // psql backslash meta-commands (`\getenv`, `\set`, `\pset`, …) are
            // intercepted by the psql client at a statement boundary: echoed
            // (under -a) but NEVER sent to the SQL parser. The single-user
            // backend must do the same, or each `\`-line becomes a bogus
            // `syntax error at or near "\"`. We only intercept when no statement
            // is currently in progress (a `\` inside a multi-line statement is
            // not a meta-command for the regress files). Handle the one command
            // whose effect the diff observes — `\pset null '...'` — and treat
            // every other backslash line as an echoed no-op.
            if !stmt_started && line_is_backslash_command(&line) {
                maybe_apply_pset_null(&line);
                line.clear();
                continue;
            }
            // Add to the query buffer only once real statement text has begun;
            // leading comment-only lines are echoed but not sent to the parser
            // (so `LINE n:` is relative to the statement, as psql does).
            if !stmt_started && is_comment_only(&line) {
                line.clear();
                continue;
            }
            stmt_started = true;
            in_buf.data.extend_from_slice(&line);
            // A statement ends when this line carries a ';' that, ignoring any
            // trailing `-- line comment` and trailing whitespace, is the last
            // significant token. This matches psql/regress: one statement per
            // ';'-terminated run, with any leading comment lines attached. The
            // `;` may be followed on the same line by a trailing comment (e.g.
            // `SELECT gcd(...); -- overflow`) — a bare "last non-whitespace byte
            // is ';'" test misses that and wrongly fuses the statement with the
            // following ones, so the parser runs them as one batch (an earlier
            // statement's error then suppresses the rest). Scan the accumulated
            // buffer so a ';' inside a string literal is correctly ignored.
            let terminates = buffer_ends_statement(&in_buf.data);
            line.clear();
            if terminates {
                saw_any = true;
                break;
            }
            saw_any = true;
        }
    }

    if in_buf.len() == 0 {
        return Ok(EOF);
    }
    // Trim any trailing whitespace + trailing `-- line comment` that follows the
    // terminating ';'. psql echoes that tail (already done above via
    // echo_regress_line) but does NOT include it in the query string it sends to
    // the server, so the server's `LINE n:`/caret echo must reflect only the
    // statement text. Without this, an error on a statement with a trailing
    // comment (e.g. `INSERT ...;  -- error, type mismatch`) echoes the comment
    // in the LINE line and diffs against expected.
    if let Some(end) = statement_query_end(&in_buf.data) {
        in_buf.data.truncate(end);
    }
    // Capture the statement text (sans the soon-to-be-added NUL) for the psql
    // LINE/caret echo in emit_regress_error. The error position the parser
    // records is 1-based into THIS statement string, so it must match what the
    // parser saw: the raw accumulated bytes (leading comments included, as the
    // parser receives them).
    let stmt = String::from_utf8_lossy(&in_buf.data).into_owned();
    REGRESS_CUR_QUERY.with(|q| *q.borrow_mut() = Some(stmt));
    // Add '\0' to make it look the same as the message case.
    in_buf.data.push(b'\0');
    Ok(pqmsg::QUERY)
}

/// Approximate psql's `psql_scan_in_quote`: are we currently inside an open
/// single-quoted string literal in the accumulated buffer? Used so a blank line
/// *inside* a multi-line literal is preserved (not skipped like an inter-
/// statement blank). A simple unescaped single-quote parity over the buffer
/// covers the regress simple-file set (no dollar-quoting there); refine if a
/// file with multi-line dollar-quoted bodies needs it.
fn in_statement_literal(buf: &[u8]) -> bool {
    let mut in_quote = false;
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'\'' {
            // A doubled '' inside a literal is an escaped quote, not a close.
            if in_quote && i + 1 < buf.len() && buf[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_quote = !in_quote;
        }
        i += 1;
    }
    in_quote
}

/// True if the accumulated query buffer holds a complete `;`-terminated
/// statement, i.e. there is a `;` outside any string literal / line comment and
/// everything after it is only whitespace or a trailing `-- line comment`.
///
/// psql's lexer ends a statement at the `;` even when a comment follows it on
/// the same line (`SELECT 1; -- note`). A naive "last non-whitespace byte is
/// ';'" test fails there and fuses the statement with the following ones. This
/// walks the buffer tracking single-quoted literals and `--` line comments so
/// only a real top-level `;` counts, then confirms the tail is insignificant.
fn buffer_ends_statement(buf: &[u8]) -> bool {
    let mut in_quote = false;
    let mut last_semi: Option<usize> = None;
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if in_quote {
            if b == b'\'' {
                // A doubled '' is an escaped quote, not a close.
                if i + 1 < buf.len() && buf[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_quote = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_quote = true,
            b'-' if i + 1 < buf.len() && buf[i + 1] == b'-' => {
                // Line comment: skip to end of line.
                while i < buf.len() && buf[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b';' => last_semi = Some(i),
            _ => {}
        }
        i += 1;
    }

    statement_terminator_pos(buf, last_semi).is_some()
}

/// If `buf` is a complete statement, return the byte index just past its
/// terminating `;` (so `buf[..pos]` is exactly the query text psql sends to the
/// server — trailing whitespace + trailing `-- line comment` excluded). Returns
/// `None` if the buffer is not yet a complete statement. `last_semi` is the
/// index of the final top-level `;` already located by the caller's scan.
fn statement_terminator_pos(buf: &[u8], last_semi: Option<usize>) -> Option<usize> {
    let pos = last_semi?;
    // Everything after the terminating ';' must be whitespace or a trailing
    // line comment (psql echoes it but it does not extend the statement, and —
    // critically — does NOT include it in the query string sent to the server,
    // so the server's `LINE n:`/caret echo must not show it either).
    let mut j = pos + 1;
    while j < buf.len() {
        let b = buf[j];
        if b.is_ascii_whitespace() {
            j += 1;
            continue;
        }
        if b == b'-' && j + 1 < buf.len() && buf[j + 1] == b'-' {
            while j < buf.len() && buf[j] != b'\n' {
                j += 1;
            }
            continue;
        }
        // Real statement text after the ';' — not yet terminated.
        return None;
    }
    Some(pos + 1)
}

/// Scan `buf` for the terminating `;` (top level, ignoring quotes and comments)
/// and return the byte index just past it when the buffer is a complete
/// statement, else `None`. Wrapper used at statement finalization to trim the
/// trailing comment/whitespace off the query string.
fn statement_query_end(buf: &[u8]) -> Option<usize> {
    let mut in_quote = false;
    let mut last_semi: Option<usize> = None;
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if in_quote {
            if b == b'\'' {
                if i + 1 < buf.len() && buf[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_quote = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_quote = true,
            b'-' if i + 1 < buf.len() && buf[i + 1] == b'-' => {
                while i < buf.len() && buf[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b';' => last_semi = Some(i),
            _ => {}
        }
        i += 1;
    }
    statement_terminator_pos(buf, last_semi)
}

/// True if `line` (with optional trailing newline) is a single-line SQL comment
/// — optional leading whitespace then `--` running to end of line. Used to keep
/// leading comment lines out of the statement buffer (they're echoed but not
/// sent to the parser, so the error LINE/caret is relative to the statement).
fn is_comment_only(line: &[u8]) -> bool {
    let mut i = 0;
    while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
        i += 1;
    }
    i + 1 < line.len() && line[i] == b'-' && line[i + 1] == b'-'
}

/// Echo one raw input line (already including its trailing newline if present)
/// to stdout, exactly as psql `-a` does.
fn echo_regress_line(line: &[u8]) {
    let s = String::from_utf8_lossy(line);
    interactive_print(&s);
}

/// True if `line` (optional trailing newline) is a psql backslash meta-command:
/// optional leading whitespace then a `\`. These are handled by the psql client,
/// not the SQL parser.
fn line_is_backslash_command(line: &[u8]) -> bool {
    let mut i = 0;
    while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
        i += 1;
    }
    i < line.len() && line[i] == b'\\'
}

/// If `line` is `\pset null '...'` (the only backslash command whose effect the
/// regress diff observes), apply the NULL display string to the printtup
/// formatter. The value may be single-quoted (`'(null)'`) or a bare token.
/// Other `\pset` subcommands and other backslash commands are ignored (echoed
/// no-ops), matching what the regress files need.
fn maybe_apply_pset_null(line: &[u8]) {
    let s = String::from_utf8_lossy(line);
    let s = s.trim();
    // Split on whitespace: ["\pset", "null", "'(null)'"] (the value token may
    // itself contain spaces inside quotes — handle that by taking everything
    // after the `null` keyword and stripping one layer of single quotes).
    let mut it = s.split_whitespace();
    if it.next() != Some("\\pset") {
        return;
    }
    if it.next() != Some("null") {
        return;
    }
    // The remainder (rejoined) is the value argument.
    let rest: String = it.collect::<Vec<_>>().join(" ");
    let val = if rest.len() >= 2 && rest.starts_with('\'') && rest.ends_with('\'') {
        rest[1..rest.len() - 1].to_string()
    } else {
        rest
    };
    backend_access_common_printtup::set_regress_null_display(&val);
}

/// Render a per-statement error to stdout in psql's client format (regress mode).
/// Builds a [`psql_format::PsqlError`] from the structured [`PgError`] (message /
/// detail / hint / cursor position) plus the current statement text (for the
/// `LINE n:`/caret echo) and writes the formatted block to stdout.
fn emit_regress_error(err: &types_error::PgError) {
    use backend_access_common_printtup::psql_format::{format_error, PsqlError};
    let severity = ::utils_error::error_severity(err.level()).to_string();
    let pe = PsqlError {
        severity,
        message: err.message.clone(),
        detail: err.detail.clone(),
        hint: err.hint.clone(),
        // cursor_position is 1-based; only emit the LINE/caret when > 0.
        position: err.cursor_position.filter(|&p| p > 0).map(|p| p as usize),
        // Prefer debug_query_string; in the single-user path it's NULL (store
        // skipped), so fall back to the regress-captured statement text.
        query: globals::debug_query_string()
            .map(|s| s.to_string())
            .or_else(|| REGRESS_CUR_QUERY.with(|q| q.borrow().clone())),
    };
    interactive_print(&format_error(&pe));
}

/// Write a prompt/echo string to the interactive output. Natively this is
/// `print!` + flush over std stdout; on `wasm64-unknown-unknown` std stdout is a
/// no-op, so route to the host stdout import.
fn interactive_print(s: &str) {
    #[cfg(not(target_family = "wasm"))]
    {
        use std::io::Write;
        print!("{s}");
        let _ = std::io::stdout().flush();
    }
    #[cfg(target_family = "wasm")]
    {
        wasm_libc_shim::stdout_write(s.as_bytes());
    }
}

/// `interactive_getc()` (postgres.c:324) — collect one character from stdin.
/// Even though we are not reading from a "client" process, we still want to
/// respond to signals, particularly SIGTERM/SIGQUIT. Returns [`EOF`] (-1) at
/// end of file.
fn interactive_getc() -> PgResult<i32> {
    #[cfg(not(target_family = "wasm"))]
    use std::io::Read;

    // This will not process catchup interrupts or notifications while reading.
    // But those can't really be relevant for a standalone backend anyway.
    crate::interrupt::check_for_interrupts()?; // CHECK_FOR_INTERRUPTS()

    // c = getc(stdin);
    let mut byte = [0u8; 1];
    #[cfg(not(target_family = "wasm"))]
    let c = match std::io::stdin().read(&mut byte) {
        Ok(0) => EOF,
        Ok(_) => byte[0] as i32,
        Err(_) => EOF,
    };
    // std stdin is a no-op on wasm64-unknown-unknown; read SQL via the host.
    #[cfg(target_family = "wasm")]
    let c = match wasm_libc_shim::stdin_read(&mut byte) {
        0 => EOF,
        _ => byte[0] as i32,
    };

    crate::interrupt::ProcessClientReadInterrupt(false)?;

    Ok(c)
}

// ===========================================================================
// ReadCommand — postgres.c:479
// ===========================================================================

/// `ReadCommand(inBuf)` (postgres.c:479) — read a command from the frontend (or
/// standard input), placing it in `in_buf` and returning the message type code.
/// [`EOF`] on end of file.
///
/// `if (whereToSendOutput == DestRemote) SocketBackend(inBuf); else
/// InteractiveBackend(inBuf);` — the interactive arm is reached from
/// `PostgresSingleUserMain` (the standalone single-user backend), where
/// `whereToSendOutput` is not `DestRemote`.
fn ReadCommand(in_buf: &mut StringInfo<'_>) -> PgResult<i32> {
    if globals::where_to_send_output() == CommandDest::Remote {
        SocketBackend(in_buf)
    } else {
        InteractiveBackend(in_buf)
    }
}

// ===========================================================================
// forbidden_in_wal_sender — postgres.c:5031
// ===========================================================================

/// `forbidden_in_wal_sender(firstchar)` (postgres.c:5031) — throw if this is a
/// WAL sender process receiving a non-simple-query message.
fn forbidden_in_wal_sender(firstchar: i32) -> PgResult<()> {
    if walsender_seams::am_walsender::call() {
        if firstchar == pqmsg::FUNCTION_CALL {
            return Err(ereport(ERROR)
                .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("fastpath function calls not supported in a replication connection")
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("extended query protocol not supported in a replication connection")
                .into_error());
        }
    }
    Ok(())
}

// ===========================================================================
// GUC reads used by the idle-state handling
// ===========================================================================

fn idle_in_transaction_session_timeout() -> i32 {
    guc_tables::vars::IdleInTransactionSessionTimeout.read()
}

fn idle_session_timeout() -> i32 {
    guc_tables::vars::IdleSessionTimeout.read()
}

fn transaction_timeout() -> i32 {
    guc_tables::vars::TransactionTimeout.read()
}

// ===========================================================================
// The loop's per-message state carried across the sigsetjmp boundary
// ===========================================================================

/// The C `volatile` locals preserved across `longjmp`: the only ones that
/// survive an error are reset in the recovery block, so we carry them in a
/// struct threaded through the loop helpers.
struct LoopState {
    send_ready_for_query: bool,
    idle_in_transaction_timeout_enabled: bool,
    idle_session_timeout_enabled: bool,
}

// ===========================================================================
// Error-recovery block — the `if (sigsetjmp(...)) { ... }` arm, postgres.c:4393
// ===========================================================================

/// The outer error-recovery block (postgres.c:4393-4504), run when a command
/// (or the idle-state work / ReadCommand) returns `Err`. Mirrors the C catch
/// arm: forget the cancel request, disable timeouts, emit the error, abort the
/// transaction, flush error state, and arm skip-till-Sync if we were mid
/// extended-query.
///
/// `mcx` is the (about-to-be-reset) MessageContext; the C switches into it
/// before `FlushErrorState`.
fn error_recovery(mcx: Mcx<'_>, err: ::types_error::PgError, state: &mut LoopState) -> PgResult<()> {
    // error_context_stack = NULL — the ambient callback chain is retired
    // (backend-utils-error divergence #10); nothing to reset.

    // C's `errfinish` (elog.c, ERROR-level path, just before `PG_RE_THROW()`)
    // hard-resets the interrupt-holdoff counters: "Reset InterruptHoldoffCount
    // in case we ereport'd from inside an interrupt holdoff section." Because
    // pgrust's `errfinish` returns `Err(PgError)` instead of `siglongjmp`-ing,
    // that reset is the catching frame's responsibility (backend-utils-error
    // lib.rs divergence #1: "the ERROR-path resets of InterruptHoldoffCount /
    // QueryCancelHoldoffCount … belong to the catching frame"). Do it here, at
    // the earliest point the catch can intercept, mirroring C exactly:
    //   InterruptHoldoffCount = 0;
    //   QueryCancelHoldoffCount = 0;
    //   CritSectionCount = 0;          /* should be unnecessary, but... */
    // Without this, any `HOLD_INTERRUPTS()` left unbalanced along the unwind
    // path leaks. The concrete observed leak: `ProcessParallelMessages`
    // (parallel.c) does HOLD_INTERRUPTS()/RESUME_INTERRUPTS(), but when it
    // rethrows a parallel worker's error mid-loop, the trailing
    // RESUME_INTERRUPTS() is skipped (exactly as in C). C recovers via this
    // elog.c reset; pgrust must do the same, or `InterruptHoldoffCount` stays 1
    // forever, making `INTERRUPTS_CAN_BE_PROCESSED()` false so every subsequent
    // parallel query in the session launches zero workers.
    init_small::globals::SetInterruptHoldoffCount(0);
    init_small::globals::SetQueryCancelHoldoffCount(0);
    init_small::globals::SetCritSectionCount(0);

    // HOLD_INTERRUPTS() — the interrupt-holdoff bracket; the abort/cleanup
    // below cannot itself be interrupted in C. The holdoff counter lives in the
    // interrupt machinery; the cleanup here is straight-line.

    // Forget any pending QueryCancel and cancel active timeouts. Clearing the
    // statement/lock timeout indicators prevents a future plain cancel from
    // being misreported as a timeout.
    timeout::disable_all_timeouts(false)?; // first, to avoid a race
    init_small::globals::SetQueryCancelPending(false);
    state.idle_in_transaction_timeout_enabled = false;
    state.idle_session_timeout_enabled = false;

    // Not reading from the client anymore.
    globals::set_doing_command_read(false);

    // Make sure libpq is in a good state.
    pqcomm::pq_comm_reset();

    // Report the error to the client and/or server log. In regress-output mode
    // the client format is psql's (ERROR:/DETAIL:/HINT:/LINE+caret) on stdout,
    // so it byte-matches expected/*.out; otherwise the normal LOG-style report.
    if globals::regress_output() {
        emit_regress_error(&err);
    } else {
        ::utils_error::emit_error_report_for(&err);
    }

    // valgrind_report_error_query(debug_query_string) — valgrind-only, skipped.

    // Make sure debug_query_string gets reset before we possibly clobber the
    // storage it points at.
    globals::set_debug_query_string(None);

    // Abort the current transaction in order to recover.
    xact_seams::abort_current_transaction::call()?;

    // if (am_walsender) WalSndErrorCleanup(); — reached only on a replication
    // connection (am_walsender): release LWLocks, cancel CV sleeps, close the
    // xlogreader, and free / clean up the active replication slot.
    if walsender_seams::am_walsender::call() {
        walsender_seams::wal_snd_error_cleanup::call();
    }

    portalmem::PortalErrorCleanup()?;

    // Replication-slot release/cleanup on a top-level error: only relevant when
    // a slot is acquired (replication / logical-decoding sessions). Not reached
    // on the simple-Query target. The slot owner is a separate unit; faithfully
    // a no-op here when no slot is held (MyReplicationSlot == NULL and no
    // temporary slots), which is always the case on this path.
    //   if (MyReplicationSlot != NULL) ReplicationSlotRelease();
    //   ReplicationSlotCleanup(false);

    // jit_reset_after_error() — JIT is compiled out by default; no-op.

    // Now return to the MessageContext and clear ErrorContext for next time.
    // (We already operate in `mcx`.)
    ::utils_error::FlushErrorState();
    let _ = mcx;

    // If we were handling an extended-query message, initiate skip till Sync.
    // This also suppresses ReadyForQuery until we get Sync.
    if globals::doing_extended_query_message() {
        globals::set_ignore_till_sync(true);
    }

    // We don't have a transaction command open anymore.
    globals::set_xact_started(false);

    // If the error occurred while reading a message, we may have lost track of
    // message boundaries; we can't safely read more.
    if pqcomm::pq_is_reading_msg() {
        return Err(ereport(FATAL)
            .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("terminating connection because protocol synchronization was lost")
            .into_error());
    }

    // RESUME_INTERRUPTS();
    Ok(())
}

/// Re-entrant wrapper around [`error_recovery`], mirroring C's re-armed
/// `sigsetjmp` (postgres.c:4393).
///
/// In C the outer error-recovery block runs *inside* the `sigsetjmp` scope, so
/// an `ereport(ERROR)` raised while cleaning up (e.g. a second error inside
/// `AbortCurrentTransaction` / `PortalErrorCleanup` — an "abort-in-abort")
/// `longjmp`s right back to the top of the same recovery block and re-runs it;
/// it does NOT fall out of `PostgresMain`. The backend keeps serving once the
/// cleanup eventually settles. Only a `FATAL`/`PANIC` ends the process.
///
/// The straight `error_recovery(...)?` this replaces did the opposite: any
/// `Err` (or panic) raised *during* recovery propagated out of the loop and the
/// `!`-returning `PostgresMain` wrapper called `proc_exit(1)` — turning a
/// recoverable second error in the cleanup path into a backend kill (the
/// port-introduced unwind/cleanup escalation class). This wrapper restores the
/// C contract: a recovery-time `ERROR` (or any panic, which we map to `ERROR`)
/// re-runs recovery on the new error; a recovery-time `FATAL`/`PANIC`
/// (e.g. the lost-protocol-sync `FATAL` `error_recovery` raises deliberately)
/// propagates so the backend exits exactly as C does.
fn run_error_recovery(
    mcx: Mcx<'_>,
    err: ::types_error::PgError,
    state: &mut LoopState,
) -> PgResult<()> {
    // Re-arming bound: C relies on AbortTransaction being re-entrant-safe and
    // eventually succeeding; a recovery that cannot settle is genuine
    // unrecoverable corruption, which C escalates to PANIC (process exit). We
    // mirror that terminal outcome after a generous cap so a pathological
    // abort-in-abort loop ends the backend instead of spinning forever.
    const MAX_RECOVERY_ATTEMPTS: u32 = 16;

    let mut pending = err;
    for _ in 0..MAX_RECOVERY_ATTEMPTS {
        // Catch a panic raised inside the recovery block itself (e.g. a seam
        // miss or `panic!`-shaped hard error inside abort/cleanup), exactly as
        // C's re-armed sigsetjmp catches an ereport(ERROR) thrown there.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            error_recovery(mcx, pending.clone(), state)
        }));
        match outcome {
            // Recovery completed cleanly — back to the idle loop.
            Ok(Ok(())) => return Ok(()),
            // Recovery raised a structured error. A FATAL/PANIC ends the
            // backend (C `proc_exit`); a lesser ERROR re-runs recovery on it.
            Ok(Err(next)) => {
                if next.level() >= FATAL {
                    return Err(next);
                }
                pending = next;
            }
            // Recovery panicked. Reconstruct the structured PgError (same
            // payload channels as the per-iteration catch_unwind) and re-run
            // recovery on it — an ERROR-level second pass.
            Err(payload) => {
                pending = match payload.downcast::<::types_error::PgError>() {
                    Ok(e) => *e,
                    Err(payload) => {
                        let msg = payload
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| {
                                payload.downcast_ref::<&str>().map(|s| s.to_string())
                            });
                        match msg {
                            Some(m) => ::types_error::PgError::error(m),
                            None => {
                                ::types_error::PgError::error("error recovery panicked")
                            }
                        }
                    }
                };
            }
        }
    }

    // Could not settle the transaction after repeated attempts: unrecoverable,
    // mirror C's PANIC-during-recovery process exit.
    Err(ereport(FATAL)
        .errcode(::types_error::error::ERRCODE_INTERNAL_ERROR)
        .errmsg("error recovery failed to settle the transaction; terminating backend")
        .into_error())
}

// ===========================================================================
// Idle-state handling — the `if (send_ready_for_query)` block, postgres.c:4565
// ===========================================================================

/// The idle-state work done before each blocking read when
/// `send_ready_for_query` is set (postgres.c:4565-4685): set the PS display /
/// activity state, arm idle timeouts, report changed GUCs, and send
/// `ReadyForQuery`.
fn ready_state(mcx: Mcx<'_>, state: &mut LoopState) -> PgResult<()> {
    let dest = globals::where_to_send_output();

    if xact_seams::is_aborted_transaction_block_state::call() {
        more_seams::set_ps_display::call("idle in transaction (aborted)");
        // pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, NULL): the
        // typed idle-in-transaction(-aborted) BackendState report seams are not
        // modeled (only idle/running variants exist). Monitoring-only; skipped.
        if idle_in_transaction_session_timeout() > 0
            && (idle_in_transaction_session_timeout() < transaction_timeout()
                || transaction_timeout() == 0)
        {
            state.idle_in_transaction_timeout_enabled = true;
            timeout::enable_timeout_after(
                TimeoutId::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                idle_in_transaction_session_timeout(),
            )?;
        }
    } else if xact_seams::is_transaction_or_transaction_block::call() {
        more_seams::set_ps_display::call("idle in transaction");
        // pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL): see above.
        if idle_in_transaction_session_timeout() > 0
            && (idle_in_transaction_session_timeout() < transaction_timeout()
                || transaction_timeout() == 0)
        {
            state.idle_in_transaction_timeout_enabled = true;
            timeout::enable_timeout_after(
                TimeoutId::IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                idle_in_transaction_session_timeout(),
            )?;
        }
    } else {
        // Process incoming notifies (including self-notifies), if any, and send
        // relevant messages to the client.  Doing it here helps ensure stable
        // behavior in tests: if any notifies were received during the
        // just-finished transaction, they'll be seen by the client before
        // ReadyForQuery is (postgres.c:4598-4607).
        //   if (notifyInterruptPending) ProcessNotifyInterrupt(false);
        if commands_async::notify_interrupt_pending() {
            commands_async::ProcessNotifyInterrupt(false)?;
        }

        // Check if we need to report stats; arm IDLE_STATS_UPDATE_TIMEOUT if
        // pgstat_report_stat() asks us to retry later.
        let stats_timeout =
            pgstat_seams::pgstat_report_stat::call(false)?;
        if stats_timeout > 0 {
            if !timeout::get_timeout_active(TimeoutId::IDLE_STATS_UPDATE_TIMEOUT) {
                timeout::enable_timeout_after(
                    TimeoutId::IDLE_STATS_UPDATE_TIMEOUT,
                    stats_timeout as i32,
                )?;
            }
        } else if timeout::get_timeout_active(TimeoutId::IDLE_STATS_UPDATE_TIMEOUT) {
            timeout::disable_timeout(TimeoutId::IDLE_STATS_UPDATE_TIMEOUT, false);
        }

        more_seams::set_ps_display::call("idle");
        status_seams::pgstat_report_activity_idle::call();

        if idle_session_timeout() > 0 {
            state.idle_session_timeout_enabled = true;
            timeout::enable_timeout_after(
                TimeoutId::IDLE_SESSION_TIMEOUT,
                idle_session_timeout(),
            )?;
        }
    }

    // Report any recently-changed GUC options.
    misc_guc::report::report_changed_guc_options();

    // The first-ready connection-timing LOG line (conn_timing.ready_for_use) is
    // a connection-setup-duration diagnostic; the conn_timing accounting owner
    // is a separate unit. Not threaded here (logging-only).

    dest_seams::ready_for_query::call(mcx, dest)?;
    state.send_ready_for_query = false;

    Ok(())
}

// ===========================================================================
// Per-message dispatch — the `switch (firstchar)` block, postgres.c:4748
// ===========================================================================

/// Process one client message (postgres.c:4748-5020). `firstchar` is the type
/// code; `input_message` holds the body (cursor at the start). Returns the
/// updated `send_ready_for_query` decision; `proc_exit` does not return.
fn dispatch_message<'mcx>(
    mcx: Mcx<'mcx>,
    firstchar: i32,
    input_message: &mut StringInfo<'mcx>,
    state: &mut LoopState,
) -> PgResult<()> {
    match firstchar {
        x if x == pqmsg::QUERY => {
            // Set statement_timestamp().
            xact_seams::set_current_statement_start_timestamp::call();

            // Copy the query string into the MessageContext (`mcx`) so it has
            // the `'mcx` lifetime exec_simple_query needs (it points into
            // MessageContext in C too, outliving the portal). Copy before
            // pq_getmsgend so the mutable message borrow is released.
            let qstr: &'mcx str = {
                let query_string = pqformat::pq_getmsgstring(mcx, input_message)?;
                leak_str_in(mcx, query_string.as_bytes())?
            };
            pqformat::pq_getmsgend(input_message)?;

            if walsender_seams::am_walsender::call() {
                // if (!exec_replication_command(query_string))
                //     exec_simple_query(query_string);
                // A WAL sender first tries the replication-command grammar; if
                // the string is not a replication command, fall back to a plain
                // SQL query (allowed on a database-connected/logical walsender).
                if !walsender_seams::exec_replication_command::call(qstr) {
                    crate::simple_query::exec_simple_query(mcx, qstr)?;
                }
            } else {
                crate::simple_query::exec_simple_query(mcx, qstr)?;
            }

            // valgrind_report_error_query — valgrind-only, skipped.
            state.send_ready_for_query = true;
        }

        x if x == pqmsg::PARSE => {
            forbidden_in_wal_sender(firstchar)?;
            // Set statement_timestamp().
            xact_seams::set_current_statement_start_timestamp::call();

            // stmt_name, query_string, numParams, paramTypes[].
            let stmt_name = {
                let s = pqformat::pq_getmsgstring(mcx, input_message)?;
                String::from_utf8_lossy(s.as_bytes()).into_owned()
            };
            let query_string: &'mcx str = {
                let qs = pqformat::pq_getmsgstring(mcx, input_message)?;
                leak_str_in(mcx, qs.as_bytes())?
            };
            let num_params = pqformat::pq_getmsgint(input_message, 2)? as i32;
            let mut param_types: alloc::vec::Vec<types_core::primitive::Oid> =
                alloc::vec::Vec::with_capacity(num_params.max(0) as usize);
            for _ in 0..num_params {
                param_types.push(pqformat::pq_getmsgint(input_message, 4)?);
            }
            pqformat::pq_getmsgend(input_message)?;

            crate::extended_query::exec_parse_message(
                mcx,
                query_string,
                &stmt_name,
                &param_types,
            )?;
        }

        x if x == pqmsg::BIND => {
            forbidden_in_wal_sender(firstchar)?;
            // Set statement_timestamp().
            xact_seams::set_current_statement_start_timestamp::call();
            // The field extraction is complex enough to keep out-of-line.
            crate::extended_query::exec_bind_message(mcx, input_message)?;
        }

        x if x == pqmsg::EXECUTE => {
            forbidden_in_wal_sender(firstchar)?;
            // Set statement_timestamp().
            xact_seams::set_current_statement_start_timestamp::call();

            let portal_name = {
                let s = pqformat::pq_getmsgstring(mcx, input_message)?;
                String::from_utf8_lossy(s.as_bytes()).into_owned()
            };
            let max_rows = pqformat::pq_getmsgint(input_message, 4)? as i32 as i64;
            pqformat::pq_getmsgend(input_message)?;

            crate::extended_query::exec_execute_message(mcx, &portal_name, max_rows)?;
        }

        x if x == pqmsg::FUNCTION_CALL => {
            dispatch_function_call_message(mcx, firstchar, input_message, state)?;
        }

        x if x == pqmsg::CLOSE => {
            dispatch_close_message(mcx, firstchar, input_message)?;
        }

        x if x == pqmsg::DESCRIBE => {
            forbidden_in_wal_sender(firstchar)?;
            // Set statement_timestamp() (needed for xact).
            xact_seams::set_current_statement_start_timestamp::call();

            let describe_type = pqformat::pq_getmsgbyte(input_message)?;
            let describe_target = {
                let s = pqformat::pq_getmsgstring(mcx, input_message)?;
                String::from_utf8_lossy(s.as_bytes()).into_owned()
            };
            pqformat::pq_getmsgend(input_message)?;

            match describe_type as u8 {
                b'S' => {
                    crate::extended_query::exec_describe_statement_message(
                        mcx,
                        &describe_target,
                    )?;
                }
                b'P' => {
                    crate::extended_query::exec_describe_portal_message(mcx, &describe_target)?;
                }
                other => {
                    return Err(ereport(ERROR)
                        .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                        .errmsg(alloc::format!("invalid DESCRIBE message subtype {other}"))
                        .into_error());
                }
            }
        }

        x if x == pqmsg::FLUSH => {
            pqformat::pq_getmsgend(input_message)?;
            if globals::where_to_send_output() == CommandDest::Remote {
                pqcomm::pq_flush()?;
            }
        }

        x if x == pqmsg::SYNC => {
            pqformat::pq_getmsgend(input_message)?;
            // If pipelining was used we may be in an implicit transaction block;
            // close it before finish_xact_command.
            xact::EndImplicitTransactionBlock();
            crate::simple_query::finish_xact_command()?;
            state.send_ready_for_query = true;
        }

        // PqMsg_Terminate: the frontend is closing the socket. EOF: unexpected
        // loss of the connection. Either way, normal shutdown.
        x if x == EOF || x == pqmsg::TERMINATE => {
            // pgStatSessionEndCause = DISCONNECT_CLIENT_EOF (on EOF) — the
            // session-end-cause stat is owned by pgstat; not threaded here.

            // Reset whereToSendOutput so ereport won't try to send more to the
            // client.
            if globals::where_to_send_output() == CommandDest::Remote {
                globals::set_where_to_send_output(CommandDest::None);
            }

            // NOTE: anything to do at shutdown belongs in an on_proc_exit /
            // on_shmem_exit callback, not here.
            ipc_seams::proc_exit::call(0);
        }

        x if x == pqmsg::COPY_DATA || x == pqmsg::COPY_DONE || x == pqmsg::COPY_FAIL => {
            // Accept but ignore these per protocol spec (probably a failed COPY
            // whose frontend is still sending data).
        }

        other => {
            return Err(ereport(FATAL)
                .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(alloc::format!("invalid frontend message type {other}"))
                .into_error());
        }
    }

    Ok(())
}

/// The `'C'` (Close) dispatch arm, postgres.c:4922. Extracted into its own
/// `#[inline(never)]` frame so its owned `close_target: String`, the nested
/// subtype `match`, and that match's `panic!`/`ereport` builders do not enlarge
/// `dispatch_message`'s frame — in an unoptimized build that frame otherwise
/// reserves the union of every arm's locals for the whole dispatch, including
/// the hot `'Q'` path that stays resident through planning/execution. Mirrors C,
/// where the Close arm's work is a plain block but the heavy state is local to
/// that block. Behavior-preserving: statements and `?` flow are unchanged.
#[inline(never)]
fn dispatch_close_message<'mcx>(
    mcx: Mcx<'mcx>,
    firstchar: i32,
    input_message: &mut StringInfo<'mcx>,
) -> PgResult<()> {
    forbidden_in_wal_sender(firstchar)?;

    let close_type = pqformat::pq_getmsgbyte(input_message)?;
    let close_target = pqformat::pq_getmsgstring(mcx, input_message)?;
    let close_target = String::from_utf8_lossy(close_target.as_bytes()).into_owned();
    pqformat::pq_getmsgend(input_message)?;

    match close_type as u8 {
        b'S' => {
            if !close_target.is_empty() {
                // DropPreparedStatement(close_target, false): drop a named
                // prepared statement (extended-query Parse created it).
                prepare::DropPreparedStatement(&close_target, false)?;
            } else {
                // special-case the unnamed statement
                crate::simple_query::drop_unnamed_stmt()?;
            }
        }
        b'P' => {
            if let Some(portal) =
                portalmem_seams::get_portal_by_name::call(&close_target)?
            {
                portalmem_seams::portal_drop::call(&portal, false)?;
            }
        }
        other => {
            return Err(ereport(ERROR)
                .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(alloc::format!("invalid CLOSE message subtype {other}"))
                .into_error());
        }
    }

    if globals::where_to_send_output() == CommandDest::Remote {
        pqformat::pq_putemptymessage(pqmsg::CLOSE_COMPLETE)?;
    }
    Ok(())
}

/// The `'F'` (fast-path Function call) dispatch arm, postgres.c:4892. Extracted
/// into its own `#[inline(never)]` frame for the same reason as
/// [`dispatch_close_message`]: keep the fast-path xact bookkeeping and the
/// `handle_function_request` call out of the shared `dispatch_message` frame so
/// the hot `'Q'` path does not carry this arm's locals. Mirrors C
/// (`HandleFunctionRequest` is a separate function). Behavior-preserving.
#[inline(never)]
fn dispatch_function_call_message<'mcx>(
    mcx: Mcx<'mcx>,
    firstchar: i32,
    input_message: &mut StringInfo<'mcx>,
    state: &mut LoopState,
) -> PgResult<()> {
    forbidden_in_wal_sender(firstchar)?;

    // Set statement_timestamp().
    xact_seams::set_current_statement_start_timestamp::call();

    // Report query to monitoring facilities.
    // pgstat_report_activity(STATE_FASTPATH, NULL): the STATE_FASTPATH
    // BackendState report seam is not modeled (monitoring-only); the PS
    // display is set below.
    more_seams::set_ps_display::call("<FASTPATH>");

    // Start an xact for this function invocation.
    crate::simple_query::start_xact_command()?;

    // Note: we may be inside an aborted transaction here;
    // HandleFunctionRequest checks for that after reading the message.

    // (MemoryContextSwitchTo(MessageContext) — already in `mcx`.)
    fastpath::handle_function_request(mcx, input_message)?;

    // Commit the function-invocation transaction.
    crate::simple_query::finish_xact_command()?;

    state.send_ready_for_query = true;
    Ok(())
}

// ===========================================================================
// PostgresMain — postgres.c:4184
// ===========================================================================

/// `PostgresMain(dbname, username)` (postgres.c:4184) — the regular backend's
/// post-auth main loop. Never returns (exits through `proc_exit`).
///
/// The per-backend signal-handler installation (postgres.c:4213-4252), the
/// cancel-key generation + `BackendKeyData` send (4264-4339), the welcome
/// banner, the `row_description_context` (extended-query RowDescription buffer),
/// and `EventTriggerOnLogin` are setup steps whose owners are separate units;
/// where unported they are skipped-with-note (signal install, banner, login
/// triggers) — none is exercised by the simple-Query end-to-end path. `BaseInit`
/// + `InitPostgres` run here via their seams (the catalog/shmem connection
/// setup C does in this vicinity).
pub fn PostgresMain(dbname: Option<&str>, username: Option<&str>) -> ! {
    // Run the backend body under a top-level unwind guard. The per-iteration
    // `catch_unwind` inside the main loop already turns query-time panics into
    // recoverable SQL errors, but the *setup phase* before the loop — most
    // importantly `InitPostgres` (catalog/relcache/catcache connection on a
    // fresh backend, including a `\c`-reconnect after heavy catalog churn) — is
    // not covered by it. In C an error there is `ereport(FATAL)`, which exits
    // the backend cleanly (status 1); the postmaster treats that as a normal
    // disconnect, NOT a crash. A bare Rust panic escaping `postgres_main_inner`
    // would instead unwind through this `-> !` frame to the process top and exit
    // 101, which the postmaster's `CleanupBackend` classifies as a crash
    // (`exit_status != 0 && != 1`) → `HandleChildCrash` → crash recovery. Since
    // pgrust's `StartupXLOG` crash recovery cannot replay a crashed datadir, the
    // postmaster then wedges forever in "the database system is in recovery
    // mode", refusing all new connections — the file-level TIMEOUT hang. Catch
    // the escaped panic here and route it through the same FATAL report + clean
    // `proc_exit(1)` path a returned setup-phase FATAL already uses, mirroring
    // C's top-level PostgresMain sigsetjmp handler.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        postgres_main_inner(dbname, username)
    }));
    let outcome: PgResult<()> = match result {
        Ok(r) => r,
        // Reconstruct the structured PgError from the panic payload (the same
        // payload channels as the loop's per-iteration catch_unwind): a
        // structured `PgError`, a legacy `PGRUST-SQLSTATE:`/seam-miss string,
        // or an unknown payload. Force the level to FATAL — an error that
        // escaped all of `postgres_main_inner` is unrecoverable for this
        // backend (C `ereport(FATAL)`).
        Err(payload) => {
            let mut err = match payload.downcast::<::types_error::PgError>() {
                Ok(e) => *e,
                Err(payload) => {
                    let msg = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()));
                    match msg {
                        Some(m) => match m.strip_prefix("PGRUST-SQLSTATE:") {
                            Some(rest) if rest.len() > 6 && rest.as_bytes()[5] == b':' => {
                                let (code, msg) = rest.split_at(5);
                                let mut chars = [0u8; 5];
                                chars.copy_from_slice(code.as_bytes());
                                ::types_error::PgError::error(msg[1..].to_string())
                                    .with_sqlstate(::types_error::make_sqlstate(chars))
                            }
                            _ => ::types_error::PgError::error(m),
                        },
                        None => ::types_error::PgError::error("backend setup path panicked"),
                    }
                }
            };
            // Force FATAL: an error that escaped all of `postgres_main_inner` is
            // unrecoverable for this backend (C `ereport(FATAL)`).
            err.level = FATAL;
            Err(err)
        }
    };
    match outcome {
        Ok(()) => {
            // The loop only exits via proc_exit (diverging); reaching here means
            // the loop returned Ok, which it never should.
            unreachable!("PostgresMain loop returned without proc_exit")
        }
        Err(err) => {
            // A FATAL escaped the loop's own recovery (e.g. lost protocol sync,
            // or a setup-phase FATAL/panic). Report it and exit cleanly (status
            // 1), mirroring the C where an unrecoverable FATAL ends the process
            // without provoking postmaster crash recovery.
            ::utils_error::emit_error_report_for(&err);
            ipc_seams::proc_exit::call(1)
        }
    }
}

/// The body of [`PostgresMain`], returning `PgResult` so a setup-phase or
/// escaped-FATAL error is reported by the `!`-returning wrapper.
fn postgres_main_inner(dbname: Option<&str>, username: Option<&str>) -> PgResult<()> {
    // Assert(dbname != NULL); Assert(username != NULL);
    let dbname = dbname.expect("PostgresMain requires a non-NULL dbname");
    let username = username.expect("PostgresMain requires a non-NULL username");

    // Re-anchor the stack-depth reference point at the backend's command-loop
    // frame.
    //
    // C records the stack base once, in `main()` (main.c:120), and relies on the
    // postmaster->backend hand-off being a shallow, fixed-depth delta: the
    // postmaster sits in `ServerLoop` (a near-`main()`-depth frame) when it forks,
    // so the inherited base is essentially at the backend's own command-loop
    // frame, leaving the full `max_stack_depth` (default 100kB) of headroom for
    // query execution. pgrust's fork path is much deeper than C's: by the time a
    // forked backend reaches query execution, it is already >100kB above the base
    // recorded in `main()`, so `check_stack_depth()` rejects even a trivial
    // top-level `SELECT 1` whenever `max_stack_depth` is set to its 100kB floor
    // (the jsonb recursion test does exactly this, then `RESET`s — the RESET and
    // every following statement spuriously failed "stack depth limit exceeded").
    //
    // Re-establishing the base here, at the start of the backend's
    // `PostgresMain`, restores the C invariant that the base ~= the command-loop
    // frame, so the configured headroom applies to query frames as C intends.
    // This is benign in single-user mode (the prior base, set in `main()`, is
    // simply replaced by an equivalent-or-deeper one) and does not change the
    // genuine deep-recursion behaviour the test exercises.
    postgres_seams::set_stack_base::call();

    // --- Per-backend signal-handler setup (postgres.c:4213-4252) ---
    //
    // The postmaster blocked all signals before forking, so the handlers are
    // installed race-free here. We install the regular-backend `pqsignal(...)`
    // block (the `!am_walsender` arm; walsenders run `WalSndSignals()` from
    // their own crate). Installing `SIGUSR1 -> procsignal_sigusr1_handler` is
    // load-bearing: `EmitProcSignalBarrier` (DROP DATABASE / DROP TABLESPACE)
    // signals every backend, including the emitter itself, and
    // `WaitForProcSignalBarrier` blocks until every slot's
    // `pss_barrierGeneration` reaches the new generation. The emitter absorbs
    // its OWN barrier only when the self-`kill(SIGUSR1)` runs the handler,
    // which sets `ProcSignalBarrierPending` so the next `CHECK_FOR_INTERRUPTS`
    // (reached from `ConditionVariableTimedSleep`) calls
    // `ProcessProcSignalBarrier`. With no SIGUSR1 handler installed the barrier
    // was never absorbed and DROP DATABASE hung forever in
    // `WaitForProcSignalBarrier`.
    //
    //   if (am_walsender) WalSndSignals(); else { pqsignal(...); }
    let am_walsender_signals = walsender_seams::am_walsender::call();
    if am_walsender_signals {
        // WalSndSignals() installs the WAL-sender handler set and calls
        // InitializeTimeouts() itself (establishing the SIGALRM handler), so the
        // shared InitializeTimeouts() below is skipped for a walsender.
        walsender_seams::wal_snd_signals::call();
    }
    if !am_walsender_signals {
        use ::signal::SigHandler;
        let pqsignal = port_pqsignal_seams::pqsignal::call;

        // pqsignal(SIGHUP, SignalHandlerForConfigReload);
        fn config_reload(_sig: i32) {
            pm_interrupt::SignalHandlerForConfigReload();
        }
        pqsignal(libc::SIGHUP, SigHandler::Handler(config_reload));

        // pqsignal(SIGINT, StatementCancelHandler);  /* cancel current query */
        pqsignal(
            libc::SIGINT,
            SigHandler::Handler(crate::interrupt::StatementCancelHandler),
        );
        // pqsignal(SIGTERM, die);  /* cancel current query and exit */
        pqsignal(libc::SIGTERM, SigHandler::Handler(crate::interrupt::die));
        // SIGQUIT handler (quickdie) was already set up by InitPostmasterChild;
        // we must not clobber it (postgres.c keeps it as-is here).
        // pqsignal(SIGALRM, handle_sig_alarm);  /* installed by InitializeTimeouts() below */
        // pqsignal(SIGPIPE, SIG_IGN);
        pqsignal(libc::SIGPIPE, SigHandler::Ignore);
        // pqsignal(SIGUSR1, procsignal_sigusr1_handler);
        pqsignal(
            libc::SIGUSR1,
            SigHandler::Handler(procsignal::procsignal_sigusr1_handler_signal),
        );
        // pqsignal(SIGUSR2, SIG_IGN);
        pqsignal(libc::SIGUSR2, SigHandler::Ignore);
        // pqsignal(SIGFPE, FloatExceptionHandler);  /* exception handler */
        pqsignal(
            libc::SIGFPE,
            SigHandler::Handler(crate::interrupt::float_exception_handler_fn),
        );
        // Reset some signals that are accepted by postmaster but not here:
        // pqsignal(SIGCHLD, SIG_DFL);  /* system() requires this */
        pqsignal(libc::SIGCHLD, SigHandler::Default);
    }

    // InitializeTimeouts() (postgres.c:4232) IS run here for a regular backend:
    // it establishes the timeout module's per-backend slot table (and the
    // SIGALRM handler), which InitPostgres relies on when it RegisterTimeout()s
    // the deadlock / statement / lock timeouts. A WAL sender already ran it from
    // WalSndSignals() above (C: InitializeTimeouts() lives in the non-walsender
    // else arm), so skip the duplicate here.
    if !am_walsender_signals {
        timeout_seams::initialize_timeouts::call();
    }

    // --- Early initialization (postgres.c:4255) ---
    // BaseInit(): open the per-backend low-level subsystems (smgr, buffers, ...).
    miscinit_seams::base_init::call()?;

    // sigprocmask(SIG_SETMASK, &UnBlockSig, NULL) (postgres.c:4264): allow
    // SIGINT etc during the initial transaction. This is load-bearing: the
    // backend inherits `BlockSig` (all signals blocked) from
    // InitPostmasterChild, so until we install `UnBlockSig` the SIGUSR1 that
    // `EmitProcSignalBarrier` sends to this very backend stays pending and is
    // never delivered to `procsignal_sigusr1_handler` — leaving
    // `ProcSignalBarrierPending` unset and `WaitForProcSignalBarrier` (DROP
    // DATABASE / DROP TABLESPACE) blocked forever waiting on this backend's own
    // slot. `UnBlockSig` is empty (plus SIGURG, added by
    // InitializeWaitEventSupport), so installing it unblocks SIGUSR1 while
    // keeping the latch's SIGURG self-wake working.
    libpq_pqsignal_seams::unblock_signals::call();

    // Generate a random cancel key + advertise it (postgres.c:4264). The
    // MyCancelKey storage + advertisement is owned by the proc/cancel-key unit;
    // not threaded here. The BackendKeyData send (below) is likewise skipped.

    // --- General initialization (postgres.c:4289) ---
    // InitPostgres(dbname, InvalidOid, username, InvalidOid,
    //              (!am_walsender) ? INIT_PG_LOAD_SESSION_LIBS : 0, NULL):
    // connect to the database, resolve the connecting role by name (from
    // MyProcPort->user_name, threaded down as `username`), load the relcache /
    // catcache, set MyDatabaseId, run session_preload_libraries. The slotsync-
    // flavored `init_postgres(dbname)` seam passes a NULL username and is for the
    // background-worker path only; the regular backend must pass the
    // authenticated role name or InitializeSessionUserId resolves OID 0.
    // `INIT_PG_LOAD_SESSION_LIBS` (miscadmin.h) — the InitPostgres flag bit
    // requesting session_preload_libraries be loaded.
    const INIT_PG_LOAD_SESSION_LIBS: u32 = 0x0001;
    let init_flags = if walsender_seams::am_walsender::call() {
        0
    } else {
        INIT_PG_LOAD_SESSION_LIBS
    };
    postinit_seams::init_postgres_by_name::call(
        Some(dbname),
        Some(username),
        init_flags,
    )?;

    // if (PostmasterContext) { MemoryContextDelete; PostmasterContext = NULL; }
    // — the postmaster-handoff context recycle; that context is owned by the
    // launcher and not modeled as a deletable here.

    // SetProcessingMode(NormalProcessing): now fully connected.
    miscinit::SetProcessingMode(
        types_core::init::ProcessingMode::NormalProcessing,
    );

    // Mirror the regress-output flag into the printtup DestDebug receiver so it
    // emits psql-aligned tables instead of the debug dump.
    backend_access_common_printtup::set_regress_output(globals::regress_output());

    // BeginReportingGUCOptions(): report GUCs to the client if appropriate.
    // In regress-output mode (psql -a -q) GUC reports are suppressed (-q).
    if !globals::regress_output() {
        misc_guc::report::begin_reporting_guc_options();
    }

    // if (IsUnderPostmaster && Log_disconnections) on_proc_exit(log_disconnections)
    // — the disconnect-log callback; registration is process-exit plumbing,
    // skipped (the body, logging::log_disconnections, is landed).

    // pgstat_report_connect(MyDatabaseId): the connection-establishment stat
    // (bumps pg_stat_database.sessions). Routed through the pgstat-database
    // seam, installed from that crate's init_seams.
    pgstat_seams::pgstat_report_connect::call(
        init_small_seams::my_database_id::call(),
    )?;

    // if (am_walsender) InitWalSender(); — claim this backend's per-walsender
    // shmem slot, create the aux-process resource owner, and advertise WAL-sender
    // status to the postmaster before the replication-command loop.
    if walsender_seams::am_walsender::call() {
        walsender_seams::init_wal_sender::call();
    }

    // Send this backend's cancellation info to the frontend (postgres.c:4328).
    // `BackendKeyData` ('K') = int32 MyProcPid + the cancel key bytes. While the
    // cancel key itself is not yet wired into a shared cancel-key registry (so
    // query cancellation by key is a no-op), the PID it carries is load-bearing:
    // libpq's `PQbackendPID` returns it, and tools that key off the backend PID —
    // notably the isolation tester's `pg_isolation_test_session_is_blocked`
    // blocked-session poll, which queries by `PQbackendPID(conn)` — get 0 (and
    // silently never detect a lock wait, hanging every blocking permutation)
    // without it. We send the classic protocol-3.0 form (a 4-byte key); the key
    // value is a fixed placeholder since cancellation-by-key is not yet served.
    if globals::where_to_send_output() == CommandDest::Remote {
        let my_pid = init_small_seams::my_proc_pid::call();
        let mut body = [0u8; 8];
        body[0..4].copy_from_slice(&my_pid.to_be_bytes());
        // 4-byte placeholder cancel key (protocol 3.0 length).
        body[4..8].copy_from_slice(&0u32.to_be_bytes());
        // PqMsg_BackendKeyData == 'K'. Need not flush; ReadyForQuery will.
        pqcomm::pq_putmessage(b'K', &body)?;
    }

    // Welcome banner for the standalone (DestDebug) case — single-user only.

    // --- The main-loop memory context (postgres.c:4351) ---
    // MessageContext is reset once per loop iteration; here a child of the
    // backend top context, created fresh each iteration.
    let backend_top = MemoryContext::new("MessageContext");

    // row_description_context + row_description_buf (extended-query
    // RowDescription reuse buffer, postgres.c:4361) — used only by
    // exec_describe_statement_message (unported F2). Not created here.

    // --- The processing loop (postgres.c:4516) ---
    let mut state = LoopState {
        send_ready_for_query: true,
        idle_in_transaction_timeout_enabled: false,
        idle_session_timeout_enabled: false,
    };

    // if (!ignore_till_sync) send_ready_for_query = true; (initially / after error)
    if !globals::ignore_till_sync() {
        state.send_ready_for_query = true;
    }

    // EventTriggerOnLogin(): fire login event triggers (postgres.c, called just
    // after the sigsetjmp recovery point is established). Fast-exits unless the
    // connected database has a login event trigger
    // (MyDatabaseHasLoginEventTriggers); otherwise runs them in a fresh
    // transaction. Because C reaches this AFTER arming sigsetjmp, an error must
    // route to the same recovery handler the loop uses (abort the transaction,
    // re-issue ReadyForQuery) and NOT abort the backend — so we run it under the
    // same catch_unwind + run_error_recovery protection as a loop iteration.
    {
        let login_cxt = backend_top.new_child("MessageContext");
        let res = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            event_trigger_seams::event_trigger_on_login::call()
        })) {
            Ok(r) => r,
            Err(payload) => match payload.downcast::<::types_error::PgError>() {
                Ok(err) => Err(*err),
                Err(_) => Err(::types_error::PgError::error(
                    "login event trigger path panicked",
                )),
            },
        };
        if let Err(err) = res {
            run_error_recovery(login_cxt.mcx(), err, &mut state)?;
            if !globals::ignore_till_sync() {
                state.send_ready_for_query = true;
            }
        }
        drop(login_cxt);
    }

    loop {
        // Run one full iteration; on Err, run the recovery block and continue.
        // This is the `sigsetjmp` outer handler expressed over PgResult.
        let message_context = backend_top.new_child("MessageContext");
        // Wrap the iteration in catch_unwind so an unported-path panic (a
        // seam-miss or a `panic!`-shaped hard error) is converted into a
        // recoverable structured ERROR rather than aborting the backend — the
        // Rust expression of C's sigsetjmp outer handler. AssertUnwindSafe is
        // sound here because the recovery path (AbortCurrentTransaction etc.)
        // resets all backend state regardless of where the panic landed.
        let iter = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_one_iteration(message_context.mcx(), &mut state)
        })) {
            Ok(r) => r,
            // Mirror invoke_pgfunction (backend-utils-fmgr-core): a builtin
            // (or any ereport) that panicked with the full structured
            // `PgError` value keeps every ErrorData field (hint/detail/...).
            Err(payload) => match payload.downcast::<::types_error::PgError>() {
                Ok(err) => Err(*err),
                Err(payload) => {
                    // Legacy string channel: honor a `PGRUST-SQLSTATE:`
                    // prefix, else default XX000.
                    let msg = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()));
                    let pgerr = match msg {
                        Some(m) => match m.strip_prefix("PGRUST-SQLSTATE:") {
                            Some(rest) if rest.len() > 6 && rest.as_bytes()[5] == b':' => {
                                let (code, msg) = rest.split_at(5);
                                let mut chars = [0u8; 5];
                                chars.copy_from_slice(code.as_bytes());
                                ::types_error::PgError::error(msg[1..].to_string())
                                    .with_sqlstate(::types_error::make_sqlstate(chars))
                            }
                            _ => ::types_error::PgError::error(m),
                        },
                        None => ::types_error::PgError::error("unported path panicked"),
                    };
                    Err(pgerr)
                }
            },
        };
        if let Err(err) = iter {
            // The C switches into MessageContext before FlushErrorState; we pass
            // the (about-to-be-reset) per-iteration context.
            run_error_recovery(message_context.mcx(), err, &mut state)?;

            // C's sigsetjmp handler ends with `if (!ignore_till_sync)
            // send_ready_for_query = true;` (postgres.c) so the next loop
            // iteration re-issues ReadyForQuery after recovering from an error.
            // Without this the backend skips ready_state and blocks forever in
            // ReadCommand while the client waits for a ReadyForQuery that never
            // comes — the post-error hang that looked like a backend crash.
            if !globals::ignore_till_sync() {
                state.send_ready_for_query = true;
            }
        }
        // MemoryContextReset(MessageContext): the per-iteration arena is
        // reclaimed by dropping the child context (all `'mcx` borrows ended).
        drop(message_context);
    }
}

/// One pass of the loop body up to (not including) the recovery block: the
/// idle-state work, the blocking read, the post-read interrupt checks, and the
/// message dispatch (postgres.c:4516-5021).
fn run_one_iteration<'mcx>(mcx: Mcx<'mcx>, state: &mut LoopState) -> PgResult<()> {
    // At top of loop, reset the extended-query flag so an "idle"-state error
    // doesn't provoke skip.
    globals::set_doing_extended_query_message(false);

    // (MemoryContextReset(MessageContext) — handled by the caller's per-iteration
    // child context.)
    let mut input_message = StringInfo::new_in(mcx);

    // Consider releasing the catalog snapshot so it doesn't pin global xmin
    // while we wait for the client.
    snapmgr::InvalidateCatalogSnapshotConditionally()?;

    // (1) If idle, tell the frontend we're ready for a new query.
    if state.send_ready_for_query {
        ready_state(mcx, state)?;
    }

    // (2) Allow async signals to run immediately while waiting for input.
    globals::set_doing_command_read(true);

    // (3) Read a command (blocks here).
    let firstchar = ReadCommand(&mut input_message)?;

    // (4) Turn off idle-in-transaction / idle-session timeouts if active. Done
    // before (5) so any last-moment timeout is detected in (5).
    if state.idle_in_transaction_timeout_enabled {
        timeout::disable_timeout(TimeoutId::IDLE_IN_TRANSACTION_SESSION_TIMEOUT, false);
        state.idle_in_transaction_timeout_enabled = false;
    }
    if state.idle_session_timeout_enabled {
        timeout::disable_timeout(TimeoutId::IDLE_SESSION_TIMEOUT, false);
        state.idle_session_timeout_enabled = false;
    }

    // (5) Disable async signal conditions again. Check for interrupts before
    // resetting DoingCommandRead so an idle-arrived cancel is reset (a no-op
    // when no query is in progress).
    crate::interrupt::check_for_interrupts()?;
    globals::set_doing_command_read(false);

    // (6) Other interesting events that happened while we slept.
    if pm_interrupt::ConfigReloadPending() {
        pm_interrupt::SetConfigReloadPending(false);
        guc_seams::process_config_file_sighup::call()?;
    }

    // (7) Process the command, unless skipping till Sync.
    if globals::ignore_till_sync() && firstchar != EOF {
        return Ok(());
    }

    dispatch_message(mcx, firstchar, &mut input_message, state)
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Copy `bytes` (a valid UTF-8 query string from the message buffer) into `mcx`
/// and leak it to a `&'mcx str`, mirroring the C query string that lives in
/// `MessageContext` for the message's lifetime.
fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<&'mcx str> {
    let mut v: ::mcx::PgVec<'mcx, u8> = ::mcx::PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    let leaked: &'mcx [u8] = allocator_api2::boxed::Box::leak(v.into_boxed_slice());
    core::str::from_utf8(leaked).map_err(|_| {
        ereport(ERROR)
            .errcode(::types_error::error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
            .errmsg("invalid byte sequence in query string")
            .into_error()
    })
}

#[cfg(test)]
mod regress_split_tests {
    use super::buffer_ends_statement;

    #[test]
    fn semicolon_then_trailing_comment_terminates() {
        // The int4.sql case that previously fused statements: a ';' followed by
        // a trailing line comment must still end the statement.
        assert!(buffer_ends_statement(b"SELECT gcd((-2147483648)::int4, 0::int4); -- overflow\n"));
    }

    #[test]
    fn bare_semicolon_terminates() {
        assert!(buffer_ends_statement(b"SELECT 1;\n"));
        assert!(buffer_ends_statement(b"SELECT 1;   \n"));
    }

    #[test]
    fn no_semicolon_does_not_terminate() {
        assert!(!buffer_ends_statement(b"SELECT a, b, lcm(a, b)\n"));
        assert!(!buffer_ends_statement(b"-- test lcm()\n"));
    }

    #[test]
    fn semicolon_inside_literal_is_not_a_terminator() {
        // The ';' is inside a string literal; the statement is not complete.
        assert!(!buffer_ends_statement(b"SELECT ';\n"));
        // Closed literal then a real terminator -> complete.
        assert!(buffer_ends_statement(b"SELECT ';';\n"));
    }

    #[test]
    fn semicolon_in_a_line_comment_is_not_a_terminator() {
        assert!(!buffer_ends_statement(b"SELECT 1 -- a; b\n"));
    }
}
