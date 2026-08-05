use std::cell::Cell;
use std::path::PathBuf;

use types_error::{ErrorLevel, PgResult, ERRCODE_INTERNAL_ERROR};
use types_startup::AuthToken;

use crate::token::{free_auth_file, make_auth_token, next_token, open_auth_file, FileHandle};
use crate::{report_plain, TokenizedAuthLine, CONF_FILE_START_DEPTH};

// errno after a failed open_auth_file (C reads the ambient errno for the
// include_if_exists ENOENT test).
thread_local! {
    static LAST_OPEN_ERRNO: Cell<i32> = const { Cell::new(0) };
}

pub(crate) fn set_last_open_errno(errno: i32) {
    LAST_OPEN_ERRNO.with(|c| c.set(errno));
}

fn absolute_config_location(inc_filename: &str, outer_filename: &str) -> String {
    conffiles_seams::absolute_config_location::call(
        inc_filename.to_string(),
        Some(PathBuf::from(outer_filename)),
    )
    .to_string_lossy()
    .into_owned()
}

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

    loop {
        if !next_token(line, pos, &mut buf, &mut initial_quote, &mut trailing_comma) {
            break;
        }

        // Is this referencing a file?
        if !initial_quote && buf.len() > 1 && buf[0] == b'@' {
            let inc = String::from_utf8_lossy(&buf[1..]).into_owned();
            tokenize_expand_file(&mut tokens, filename, &inc, elevel, depth + 1, err_msg, tok_lines)?;
        } else {
            tokens.push(make_auth_token(&buf, initial_quote));
        }

        if !(trailing_comma && err_msg.is_none()) {
            break;
        }
    }

    Ok(tokens)
}

pub(crate) fn tokenize_include_file(
    outer_filename: &str,
    inc_filename: &str,
    tok_lines: &mut Vec<TokenizedAuthLine>,
    elevel: ErrorLevel,
    depth: i32,
    missing_ok: bool,
    err_msg: &mut Option<String>,
) -> PgResult<()> {
    let inc_fullname = absolute_config_location(inc_filename, outer_filename);
    let inc_file = match open_auth_file(&inc_fullname, elevel, depth, err_msg)? {
        None => {
            if LAST_OPEN_ERRNO.with(Cell::get) == libc::ENOENT && missing_ok {
                report_plain(
                    elevel,
                    479,
                    "tokenize_include_file",
                    ERRCODE_INTERNAL_ERROR,
                    format!("skipping missing authentication file \"{inc_fullname}\""),
                )?;
                *err_msg = None;
                return Ok(());
            }
            // error in err_msg, so caller reports it.
            return Ok(());
        }
        Some(file) => file,
    };

    tokenize_auth_file(&inc_fullname, &inc_file, tok_lines, elevel, depth)?;
    free_auth_file(inc_file, depth);
    Ok(())
}

pub(crate) fn tokenize_expand_file(
    tokens: &mut Vec<AuthToken>,
    outer_filename: &str,
    inc_filename: &str,
    elevel: ErrorLevel,
    depth: i32,
    err_msg: &mut Option<String>,
    _tok_lines: &mut [TokenizedAuthLine],
) -> PgResult<()> {
    let inc_fullname = absolute_config_location(inc_filename, outer_filename);
    let Some(inc_file) = open_auth_file(&inc_fullname, elevel, depth, err_msg)? else {
        return Ok(()); // error already logged
    };

    let mut inc_lines: Vec<TokenizedAuthLine> = Vec::new();
    tokenize_auth_file(&inc_fullname, &inc_file, &mut inc_lines, elevel, depth)?;

    for tok_line in inc_lines {
        if let Some(e) = &tok_line.err_msg {
            *err_msg = Some(e.clone());
            break;
        }
        for inc_tokens in tok_line.fields {
            tokens.extend(inc_tokens);
        }
    }

    free_auth_file(inc_file, depth);
    Ok(())
}

pub fn tokenize_auth_file(
    filename: &str,
    file: &FileHandle,
    tok_lines: &mut Vec<TokenizedAuthLine>,
    elevel: ErrorLevel,
    depth: i32,
) -> PgResult<()> {
    let mut lines = LineIter::new(&file.content);
    let mut line_number: i32 = 1;

    if depth == CONF_FILE_START_DEPTH {
        tok_lines.clear();
    }

    while lines.peek().is_some() {
        let mut current_line: Vec<Vec<AuthToken>> = Vec::new();
        let mut err_msg: Option<String> = None;
        let mut last_backslash_buflen: usize = 0;
        let mut continuations: i32 = 0;

        // Collect the next input line, handling backslash continuations.
        let mut buf: Vec<u8> = Vec::new();
        while let Some(raw) = lines.next() {
            append_stripped(&mut buf, raw);

            if buf.len() > last_backslash_buflen && buf.last() == Some(&b'\\') {
                buf.pop();
                last_backslash_buflen = buf.len();
                continuations += 1;
                continue;
            }
            break;
        }

        let mut lineptr: usize = 0;
        while lineptr < buf.len() && err_msg.is_none() {
            let current_field =
                next_field_expand(filename, &buf, &mut lineptr, elevel, depth, &mut err_msg, tok_lines)?;
            if !current_field.is_empty() {
                current_line.push(current_field);
            }
        }

        // The C body's process_line / next_line goto labels.
        let mut goto_next_line = current_line.is_empty() && err_msg.is_none();

        if !goto_next_line && err_msg.is_none() && current_line.len() == 2 {
            let first = current_line[0][0].string.clone();
            let second = current_line[1][0].string.clone();

            if first == "include" {
                tokenize_include_file(filename, &second, tok_lines, elevel, depth + 1, false, &mut err_msg)?;
                if err_msg.is_none() {
                    goto_next_line = true;
                }
            } else if first == "include_dir" {
                let res = conffiles_seams::get_conf_files_in_dir::call(
                    second.clone(),
                    Some(PathBuf::from(filename)),
                    elevel,
                )?;
                if let Some(m) = res.err_msg {
                    err_msg = Some(m);
                } else {
                    let mut err_buf = String::new();
                    for fname in &res.filenames {
                        let fname_s = fname.to_string_lossy().into_owned();
                        tokenize_include_file(
                            filename, &fname_s, tok_lines, elevel, depth + 1, false, &mut err_msg,
                        )?;
                        if let Some(e) = &err_msg {
                            if !err_buf.is_empty() {
                                err_buf.push('\n');
                            }
                            err_buf.push_str(e);
                        }
                    }
                    if err_buf.is_empty() {
                        goto_next_line = true;
                    } else {
                        err_msg = Some(err_buf);
                    }
                }
            } else if first == "include_if_exists" {
                tokenize_include_file(filename, &second, tok_lines, elevel, depth + 1, true, &mut err_msg)?;
                if err_msg.is_none() {
                    goto_next_line = true;
                }
            }
        }

        if !goto_next_line {
            tok_lines.push(TokenizedAuthLine {
                fields: current_line,
                file_name: filename.to_string(),
                line_num: line_number,
                raw_line: String::from_utf8_lossy(&buf).into_owned(),
                err_msg: err_msg.take(),
            });
        }

        line_number += continuations + 1;
    }

    Ok(())
}

// pg_get_line_append + pg_strip_crlf: content is split on '\n'; each piece is
// truncated at the first NUL (C strlen) and stripped of trailing \r / \n.
struct LineIter<'a> {
    rest: &'a [u8],
}

impl<'a> LineIter<'a> {
    fn new(content: &'a [u8]) -> Self {
        Self { rest: content }
    }

    fn peek(&self) -> Option<&'a [u8]> {
        if self.rest.is_empty() {
            None
        } else {
            Some(self.rest)
        }
    }
}

impl<'a> Iterator for LineIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.rest.is_empty() {
            return None;
        }
        match self.rest.iter().position(|&c| c == b'\n') {
            Some(i) => {
                let line = &self.rest[..=i];
                self.rest = &self.rest[i + 1..];
                Some(line)
            }
            None => {
                let line = self.rest;
                self.rest = &[];
                Some(line)
            }
        }
    }
}

fn append_stripped(buf: &mut Vec<u8>, line: &[u8]) {
    let end = line.iter().position(|&c| c == 0).unwrap_or(line.len());
    let mut piece = &line[..end];
    while matches!(piece.last(), Some(b'\n') | Some(b'\r')) {
        piece = &piece[..piece.len() - 1];
    }
    buf.extend_from_slice(piece);
}
