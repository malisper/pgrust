//! Port of `repl_scanner.l`: hand-written matcher (no flex runtime).
//!
//! C-divergence: identifier folding needs a `Mcx` + encoding; this scanner
//! uses a scratch `MemoryContext` and assumes `PG_UTF8` (no ambient
//! `GetDatabaseEncoding()`). `Token` owns plain `String`, not
//! `PgString<'mcx>`, so the scratch context can drop per call.

#![allow(non_snake_case)]

use std::string::String;
use std::vec::Vec;

use elog::ereport;
use mcx::MemoryContext;
use types_core::primitive::XLogRecPtr;
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERROR};

const SCANNER_ENCODING: wchar::pg_enc = wchar::PG_UTF8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Eof,
    Char(u8),
    Ident(String),
    Sconst(String),
    Uconst(u32),
    Recptr(XLogRecPtr),
    BaseBackup,
    IdentifySystem,
    ReadReplicationSlot,
    Show,
    Timeline,
    StartReplication,
    CreateReplicationSlot,
    DropReplicationSlot,
    AlterReplicationSlot,
    TimelineHistory,
    Physical,
    ReserveWal,
    Logical,
    Slot,
    Temporary,
    TwoPhase,
    ExportSnapshot,
    NoexportSnapshot,
    UseSnapshot,
    Wait,
    UploadManifest,
}

impl Token {
    pub fn is_replication_command_first(&self) -> bool {
        matches!(
            self,
            Token::IdentifySystem
                | Token::BaseBackup
                | Token::StartReplication
                | Token::CreateReplicationSlot
                | Token::DropReplicationSlot
                | Token::AlterReplicationSlot
                | Token::ReadReplicationSlot
                | Token::TimelineHistory
                | Token::Show
                | Token::UploadManifest
        )
    }
}

// Recoverable PgError, not an unwind.
fn replication_yyerror(message: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg_internal(message)
        .into_error()
}

struct Scanner<'a> {
    input: &'a [u8],
    pos: usize,
    mcx: MemoryContext,
}

impl<'a> Scanner<'a> {
    fn new(input: &'a str) -> Self {
        Scanner {
            input: input.as_bytes(),
            pos: 0,
            mcx: MemoryContext::new("repl_scanner"),
        }
    }

    fn next_token(&mut self) -> PgResult<Token> {
        loop {
            if self.pos >= self.input.len() {
                return Ok(Token::Eof);
            }
            let ch = self.input[self.pos];

            if is_space(ch) {
                self.pos += 1;
                continue;
            }
            if ch == b'\'' {
                return self.lex_quoted_string();
            }
            if ch == b'"' {
                return self.lex_delimited_identifier();
            }
            if is_hexdigit(ch) {
                return self.lex_number_or_recptr();
            }
            if is_ident_start(ch) {
                return self.lex_word();
            }
            self.pos += 1;
            return Ok(Token::Char(ch));
        }
    }

    fn lex_quoted_string(&mut self) -> PgResult<Token> {
        debug_assert_eq!(self.input[self.pos], b'\'');
        self.pos += 1;
        let mut lit: Vec<u8> = Vec::new();
        loop {
            if self.pos >= self.input.len() {
                return Err(replication_yyerror("unterminated quoted string").into());
            }
            let ch = self.input[self.pos];
            if ch == b'\'' {
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'\'' {
                    lit.push(b'\'');
                    self.pos += 2;
                } else {
                    self.pos += 1;
                    return Ok(Token::Sconst(bytes_to_string(&lit)));
                }
            } else {
                lit.push(ch);
                self.pos += 1;
            }
        }
    }

    fn lex_delimited_identifier(&mut self) -> PgResult<Token> {
        debug_assert_eq!(self.input[self.pos], b'"');
        self.pos += 1;
        let mut lit: Vec<u8> = Vec::new();
        loop {
            if self.pos >= self.input.len() {
                return Err(replication_yyerror("unterminated quoted string").into());
            }
            let ch = self.input[self.pos];
            if ch == b'"' {
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'"' {
                    lit.push(b'"');
                    self.pos += 2;
                } else {
                    self.pos += 1;
                    let mut folded = mcx::slice_in(self.mcx.mcx(), &lit)
                        .map_err(|_| replication_yyerror("out of memory"))?;
                    parser_small1::truncate_identifier(&mut folded, true, SCANNER_ENCODING)?;
                    return Ok(Token::Ident(bytes_to_string(&folded)));
                }
            } else {
                lit.push(ch);
                self.pos += 1;
            }
        }
    }

    // `{digit}+` -> UCONST vs `{hexdigit}+/{hexdigit}+` -> RECPTR: both begin
    // with a hex-digit run; a run followed by `/` and another hex run wins.
    fn lex_number_or_recptr(&mut self) -> PgResult<Token> {
        let start = self.pos;
        while self.pos < self.input.len() && is_hexdigit(self.input[self.pos]) {
            self.pos += 1;
        }
        let hi_end = self.pos;

        if self.pos < self.input.len()
            && self.input[self.pos] == b'/'
            && self.pos + 1 < self.input.len()
            && is_hexdigit(self.input[self.pos + 1])
        {
            self.pos += 1;
            while self.pos < self.input.len() && is_hexdigit(self.input[self.pos]) {
                self.pos += 1;
            }
            let text = &self.input[start..self.pos];
            return match parse_recptr(text) {
                Some(recptr) => Ok(Token::Recptr(recptr)),
                None => Err(replication_yyerror("invalid streaming start location").into()),
            };
        }

        // A letter first byte can only start `{identifier}`, never `{digit}+`.
        let first = self.input[start];
        if is_ident_start(first) {
            self.pos = start;
            return self.lex_word();
        }

        let mut dec_end = start;
        while dec_end < hi_end && self.input[dec_end].is_ascii_digit() {
            dec_end += 1;
        }
        let run = &self.input[start..dec_end];
        let val = parse_decimal_u32(run);
        self.pos = dec_end;
        Ok(Token::Uconst(val))
    }

    fn lex_word(&mut self) -> PgResult<Token> {
        let start = self.pos;
        while self.pos < self.input.len() && is_ident_cont(self.input[self.pos]) {
            self.pos += 1;
        }
        let word = &self.input[start..self.pos];

        if let Some(tok) = keyword_token(word) {
            return Ok(tok);
        }

        let folded = parser_small1::downcase_truncate_identifier(
            self.mcx.mcx(),
            word,
            true,
            SCANNER_ENCODING,
        )?;
        Ok(Token::Ident(bytes_to_string(&folded)))
    }
}

#[inline]
fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

#[inline]
fn is_hexdigit(ch: u8) -> bool {
    ch.is_ascii_hexdigit()
}

#[inline]
fn is_ident_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_' || ch >= 0x80
}

#[inline]
fn is_ident_cont(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'$' || ch >= 0x80
}

// Exact, case-sensitive; precedes `{identifier}` in `repl_scanner.l`.
fn keyword_token(word: &[u8]) -> Option<Token> {
    Some(match word {
        b"BASE_BACKUP" => Token::BaseBackup,
        b"IDENTIFY_SYSTEM" => Token::IdentifySystem,
        b"READ_REPLICATION_SLOT" => Token::ReadReplicationSlot,
        b"SHOW" => Token::Show,
        b"TIMELINE" => Token::Timeline,
        b"START_REPLICATION" => Token::StartReplication,
        b"CREATE_REPLICATION_SLOT" => Token::CreateReplicationSlot,
        b"DROP_REPLICATION_SLOT" => Token::DropReplicationSlot,
        b"ALTER_REPLICATION_SLOT" => Token::AlterReplicationSlot,
        b"TIMELINE_HISTORY" => Token::TimelineHistory,
        b"PHYSICAL" => Token::Physical,
        b"RESERVE_WAL" => Token::ReserveWal,
        b"LOGICAL" => Token::Logical,
        b"SLOT" => Token::Slot,
        b"TEMPORARY" => Token::Temporary,
        b"TWO_PHASE" => Token::TwoPhase,
        b"EXPORT_SNAPSHOT" => Token::ExportSnapshot,
        b"NOEXPORT_SNAPSHOT" => Token::NoexportSnapshot,
        b"USE_SNAPSHOT" => Token::UseSnapshot,
        b"WAIT" => Token::Wait,
        b"UPLOAD_MANIFEST" => Token::UploadManifest,
        _ => return None,
    })
}

fn parse_recptr(text: &[u8]) -> Option<XLogRecPtr> {
    let mut parts = text.splitn(2, |&b| b == b'/');
    let hi_bytes = parts.next()?;
    let lo_bytes = parts.next()?;
    if hi_bytes.is_empty() || lo_bytes.is_empty() {
        return None;
    }
    let hi = parse_hex_u32(hi_bytes)?;
    let lo = parse_hex_u32(lo_bytes)?;
    Some(((hi as u64) << 32) | (lo as u64))
}

fn parse_hex_u32(bytes: &[u8]) -> Option<u32> {
    let mut acc: u32 = 0;
    for &b in bytes {
        let d = (b as char).to_digit(16)?;
        acc = acc.checked_mul(16)?.checked_add(d)?;
    }
    Some(acc)
}

// `strtoul(..., 10)` narrowed to `uint32`: reproduces the 32-bit wrap.
fn parse_decimal_u32(bytes: &[u8]) -> u32 {
    let mut acc: u64 = 0;
    for &b in bytes {
        let d = (b - b'0') as u64;
        acc = acc.wrapping_mul(10).wrapping_add(d);
    }
    acc as u32
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

pub fn replication_lex_all(input: &str) -> PgResult<Vec<Token>> {
    let mut scanner = Scanner::new(input);
    let mut tokens: Vec<Token> = Vec::new();
    loop {
        let tok = scanner.next_token()?;
        let is_eof = tok == Token::Eof;
        tokens.push(tok);
        if is_eof {
            return Ok(tokens);
        }
    }
}

pub fn is_replication_command(input: &str) -> PgResult<bool> {
    let mut scanner = Scanner::new(input);
    let first = scanner.next_token()?;
    Ok(first.is_replication_command_first())
}

#[cfg(test)]
mod tests;
