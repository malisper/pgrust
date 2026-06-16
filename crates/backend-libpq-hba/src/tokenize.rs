//! The file-tokenizer of `hba.c`: `next_field_expand`, `tokenize_include_file`,
//! `tokenize_expand_file`, and the central `tokenize_auth_file`. These turn an
//! auth config file into a list of [`TokenizedAuthLine`], handling
//! `@`-inclusion, the `include` / `include_dir` / `include_if_exists` records,
//! comments, and backslash continuations.
//!
//! Ported from `src/backend/libpq/hba.c` (lines 380-915).
//!
//! ## Memory-context / error-context note
//!
//! The C allocates the token/line lists in `tokenize_context` and pushes a
//! `tokenize_error_callback` onto `error_context_stack`; here the data is owned
//! `Vec`s (dropped on scope exit), and the `errcontext("line %d of
//! configuration file ...")` is attached directly to each built error.

use std::path::PathBuf;

use backend_storage_file_fd_seams as fd;
use backend_utils_misc_conffiles_seams as conffiles;
use types_error::ErrorLevel;
use types_error::PgResult;
use types_net::AuthToken;

use crate::token::{free_auth_file, make_auth_token, next_token, open_auth_file, FileHandle};
use crate::{report_file_access, tok_str, TokenizedAuthLine, CONF_FILE_START_DEPTH, ENOENT};

/// `AbsoluteConfigLocation(inc_filename, outer_filename)` adapter (PathBuf seam).
fn absolute_config_location(inc_filename: &str, outer_filename: &str) -> String {
    let p = conffiles::absolute_config_location::call(
        inc_filename.to_string(),
        Some(PathBuf::from(outer_filename)),
    );
    p.to_string_lossy().into_owned()
}

/// `static List *next_field_expand(const char *filename, char **lineptr, int
/// elevel, int depth, char **err_msg)` (hba.c:380).
///
/// Tokenize one HBA field from the line at `*pos`, handling file inclusion (`@`)
/// and comma lists. Returns the field's tokens (empty == C `NIL`, reached EOL).
pub(crate) fn next_field_expand(
    filename: &str,
    line: &[u8],
    pos: &mut usize,
    elevel: ErrorLevel,
    depth: i32,
    err_msg: &mut Option<String>,
    tok_lines: &mut Vec<TokenizedAuthLine>,
) -> PgResult<Vec<AuthToken>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut trailing_comma = false;
    let mut initial_quote = false;
    let mut tokens: Vec<AuthToken> = Vec::new();

    // do { ... } while (trailing_comma && (*err_msg == NULL));
    loop {
        // if (!next_token(lineptr, &buf, &initial_quote, &trailing_comma)) break;
        if !next_token(line, pos, &mut buf, &mut initial_quote, &mut trailing_comma) {
            break;
        }

        // Is this referencing a file?
        // if (!initial_quote && buf.len > 1 && buf.data[0] == '@')
        if !initial_quote && buf.len() > 1 && buf[0] == b'@' {
            let inc = String::from_utf8_lossy(&buf[1..]).into_owned();
            tokenize_expand_file(&mut tokens, filename, &inc, elevel, depth + 1, err_msg, tok_lines)?;
        } else {
            // tokens = lappend(tokens, make_auth_token(buf.data, initial_quote));
            tokens.push(make_auth_token(&buf, initial_quote));
        }

        if !(trailing_comma && err_msg.is_none()) {
            break;
        }
    }

    Ok(tokens)
}

/// `static void tokenize_include_file(const char *outer_filename, const char
/// *inc_filename, List **tok_lines, int elevel, int depth, bool missing_ok,
/// char **err_msg)` (hba.c:439).
///
/// Open and tokenize a file referenced by an `include*` record, appending its
/// lines to `tok_lines`. If `missing_ok`, a missing file is ignored.
pub(crate) fn tokenize_include_file(
    outer_filename: &str,
    inc_filename: &str,
    tok_lines: &mut Vec<TokenizedAuthLine>,
    elevel: ErrorLevel,
    depth: i32,
    missing_ok: bool,
    err_msg: &mut Option<String>,
) -> PgResult<()> {
    // inc_fullname = AbsoluteConfigLocation(inc_filename, outer_filename);
    let inc_fullname = absolute_config_location(inc_filename, outer_filename);
    // inc_file = open_auth_file(inc_fullname, elevel, depth, err_msg);
    let inc_file = open_auth_file(&inc_fullname, elevel, depth, err_msg)?;

    let inc_file = match inc_file {
        None => {
            // if (errno == ENOENT && missing_ok)
            if fd::last_errno::call() == ENOENT && missing_ok {
                // ereport(elevel, (errmsg("skipping missing authentication file \"%s\"", inc_fullname)))
                crate::report_plain(
                    elevel,
                    "tokenize_include_file",
                    types_error::ERRCODE_INTERNAL_ERROR,
                    format!("skipping missing authentication file \"{inc_fullname}\""),
                )?;
                // *err_msg = NULL;
                *err_msg = None;
                return Ok(());
            }
            // error in err_msg, so leave and report.
            return Ok(());
        }
        Some(file) => file,
    };

    // tokenize_auth_file(inc_fullname, inc_file, tok_lines, elevel, depth);
    tokenize_auth_file(&inc_fullname, &inc_file, tok_lines, elevel, depth)?;
    // free_auth_file(inc_file, depth);
    free_auth_file(inc_file, depth);
    Ok(())
}

/// `static List *tokenize_expand_file(List *tokens, const char *outer_filename,
/// const char *inc_filename, int elevel, int depth, char **err_msg)`
/// (hba.c:494).
///
/// Expand a file referenced with `@` into a flat list of [`AuthToken`],
/// appending to `tokens`. Recurses on nested `@` / include records.
pub(crate) fn tokenize_expand_file(
    tokens: &mut Vec<AuthToken>,
    outer_filename: &str,
    inc_filename: &str,
    elevel: ErrorLevel,
    depth: i32,
    err_msg: &mut Option<String>,
    _tok_lines: &mut Vec<TokenizedAuthLine>,
) -> PgResult<()> {
    // inc_fullname = AbsoluteConfigLocation(inc_filename, outer_filename);
    let inc_fullname = absolute_config_location(inc_filename, outer_filename);
    // inc_file = open_auth_file(inc_fullname, elevel, depth, err_msg);
    let inc_file = match open_auth_file(&inc_fullname, elevel, depth, err_msg)? {
        None => return Ok(()), // error already logged
        Some(file) => file,
    };

    // There is possible recursion here if the file contains @ or an include
    // record. tokenize_auth_file(inc_fullname, inc_file, &inc_lines, elevel, depth);
    let mut inc_lines: Vec<TokenizedAuthLine> = Vec::new();
    tokenize_auth_file(&inc_fullname, &inc_file, &mut inc_lines, elevel, depth)?;

    // Move all the tokens found in the file to the tokens list.
    // foreach(inc_line, inc_lines)
    for tok_line in inc_lines {
        // If any line has an error, propagate that up to caller.
        // if (tok_line->err_msg) { *err_msg = pstrdup(tok_line->err_msg); break; }
        if let Some(e) = &tok_line.err_msg {
            *err_msg = Some(e.clone());
            break;
        }

        // foreach(inc_field, tok_line->fields) foreach(inc_token, inc_tokens)
        for inc_tokens in tok_line.fields {
            for token in inc_tokens {
                // tokens = lappend(tokens, token);
                tokens.push(token);
            }
        }
    }

    // free_auth_file(inc_file, depth);
    free_auth_file(inc_file, depth);
    Ok(())
}

/// `void tokenize_auth_file(const char *filename, FILE *file, List **tok_lines,
/// int elevel, int depth)` (hba.c:690).
///
/// Tokenize the already-opened `file`, appending [`TokenizedAuthLine`]s to
/// `tok_lines`. This is the central parsing routine.
pub fn tokenize_auth_file(
    filename: &str,
    file: &FileHandle,
    tok_lines: &mut Vec<TokenizedAuthLine>,
    elevel: ErrorLevel,
    depth: i32,
) -> PgResult<()> {
    let content = &file.content;
    // Split the whole-file content into lines (the C `pg_get_line_append` loop),
    // keeping the byte content; line endings are stripped per-line below.
    let raw_lines = split_lines(content);
    let mut idx = 0usize;
    // int line_number = 1;
    let mut line_number: i32 = 1;

    // if (depth == CONF_FILE_START_DEPTH) *tok_lines = NIL;
    if depth == CONF_FILE_START_DEPTH {
        tok_lines.clear();
    }

    // while (!feof(file) && !ferror(file))
    while idx < raw_lines.len() {
        let mut current_line: Vec<Vec<AuthToken>> = Vec::new();
        let mut err_msg: Option<String> = None;
        let mut last_backslash_buflen: usize = 0;
        let mut continuations: i32 = 0;

        // Collect the next input line, handling backslash continuations.
        // resetStringInfo(&buf);
        let mut buf: Vec<u8> = Vec::new();

        // while (pg_get_line_append(file, &buf, NULL))
        while idx < raw_lines.len() {
            // buf.len = pg_strip_crlf(buf.data) — append this raw line, stripped.
            let mut piece = strip_crlf(&raw_lines[idx]);
            idx += 1;
            buf.append(&mut piece);

            // Check for backslash continuation.
            // if (buf.len > last_backslash_buflen && buf.data[buf.len - 1] == '\\')
            if buf.len() > last_backslash_buflen && buf.last() == Some(&b'\\') {
                // buf.data[--buf.len] = '\0';
                buf.pop();
                last_backslash_buflen = buf.len();
                continuations += 1;
                continue;
            }

            // Nope, so we have the whole line.
            break;
        }

        // (No I/O error path: the whole file was already read by open_auth_file.)

        // Parse fields.
        let mut lineptr: usize = 0;
        // while (*lineptr && err_msg == NULL)
        while lineptr < buf.len() && buf[lineptr] != 0 && err_msg.is_none() {
            let current_field =
                next_field_expand(filename, &buf, &mut lineptr, elevel, depth, &mut err_msg, tok_lines)?;
            // add field to line, unless we are at EOL or comment start
            if !current_field.is_empty() {
                current_line.push(current_field);
            }
        }

        // The C body uses goto labels `process_line` / `next_line`.
        let mut goto_next_line = false;

        // if (current_line == NIL && err_msg == NULL) goto next_line;
        if current_line.is_empty() && err_msg.is_none() {
            goto_next_line = true;
        }

        // If the line is valid, check if that's an include directive.
        // if (err_msg == NULL && list_length(current_line) == 2)
        if !goto_next_line && err_msg.is_none() && current_line.len() == 2 {
            let first = tok_str(&current_line[0][0]).to_vec();
            let second = String::from_utf8_lossy(tok_str(&current_line[1][0])).into_owned();

            if first == b"include" {
                tokenize_include_file(filename, &second, tok_lines, elevel, depth + 1, false, &mut err_msg)?;
                // if (err_msg) goto process_line; else goto next_line;
                if err_msg.is_none() {
                    goto_next_line = true;
                }
            } else if first == b"include_dir" {
                let dir_name = &second;
                // filenames = GetConfFilesInDir(dir_name, filename, elevel, &num, &err_msg);
                let res = conffiles::get_conf_files_in_dir::call(
                    dir_name.clone(),
                    Some(PathBuf::from(filename)),
                    elevel,
                )?;

                if let Some(m) = res.err_msg {
                    // the error is in err_msg, so create an entry; goto process_line.
                    err_msg = Some(m);
                } else {
                    // initStringInfo(&err_buf);
                    let mut err_buf = String::new();
                    // for (int i = 0; i < num_filenames; i++)
                    for fname in &res.filenames {
                        let fname_s = fname.to_string_lossy().into_owned();
                        tokenize_include_file(
                            filename, &fname_s, tok_lines, elevel, depth + 1, false, &mut err_msg,
                        )?;
                        // cumulate errors if any
                        if let Some(e) = &err_msg {
                            if !err_buf.is_empty() {
                                err_buf.push('\n');
                            }
                            err_buf.push_str(e);
                        }
                    }

                    // if (err_buf.len == 0) goto next_line;
                    if err_buf.is_empty() {
                        goto_next_line = true;
                    } else {
                        // err_msg = err_buf.data; goto process_line;
                        err_msg = Some(err_buf);
                    }
                }
            } else if first == b"include_if_exists" {
                tokenize_include_file(filename, &second, tok_lines, elevel, depth + 1, true, &mut err_msg)?;
                if err_msg.is_none() {
                    goto_next_line = true;
                }
            }
        }

        // process_line: emit line to the TokenizedAuthLine list unless we
        // jumped to next_line.
        if !goto_next_line {
            let tok_line = TokenizedAuthLine {
                fields: current_line,
                file_name: filename.to_string(),
                line_num: line_number,
                raw_line: String::from_utf8_lossy(&buf).into_owned(),
                err_msg: err_msg.take(),
            };
            tok_lines.push(tok_line);
        }

        // next_line:
        line_number += continuations + 1;
    }

    // Suppress the unused report_file_access import in the no-IO-error model;
    // referenced so the report vocabulary stays documented from one place.
    let _ = report_file_access;
    Ok(())
}

// ---------------------------------------------------------------------------
// Local string helpers (pure logic).
// ---------------------------------------------------------------------------

/// Split file content into raw lines (each including its trailing `\n` if any),
/// modelling the C `pg_get_line_append` sequence of reads.
fn split_lines(content: &[u8]) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut start = 0usize;
    for i in 0..content.len() {
        if content[i] == b'\n' {
            out.push(content[start..=i].to_vec());
            start = i + 1;
        }
    }
    if start < content.len() {
        out.push(content[start..].to_vec());
    }
    out
}

/// `pg_strip_crlf(char *str)` (src/common/string.c) — remove any trailing
/// `\n` / `\r`. (Operates on the C `strlen`, i.e. up to the first NUL.)
fn strip_crlf(line: &[u8]) -> Vec<u8> {
    let end = line.iter().position(|&c| c == 0).unwrap_or(line.len());
    let mut v = line[..end].to_vec();
    while matches!(v.last(), Some(b'\n') | Some(b'\r')) {
        v.pop();
    }
    v
}
