//! Port of `repl_scanner.l` — the lexical scanner for the WalSender replication
//! commands.
//!
//! This unit owns ONLY the lexer (`repl_scanner.l`): the flex DFA over the
//! `K_*` keyword rules, `SCONST`/`IDENT`/`UCONST`/`RECPTR`, the single-character
//! `.` rule, the `<xq>`/`<xd>` quoted-literal states, `replication_yyerror`, and
//! `replication_scanner_is_replication_command`.
//!
//! The Bison parser (`repl_gram.y`) — every grammar production, the
//! `ReplCommand` node vocabulary, and the public parse entry points — is the
//! cycle-partner unit `backend-replication-repl-gram` and is NOT ported here.
//! The grammar drives this lexer through the
//! `backend-replication-repl-scanner-seams` contract, whose two seams this crate
//! owns and installs from [`init_seams`]:
//!
//!  * [`replication_lex_all`](backend_replication_repl_scanner_seams::replication_lex_all)
//!    — lex an entire command string into its token stream (terminated by a
//!    single [`Token::Eof`]). The C parser reads tokens left-to-right with one
//!    token of lookahead and observes no scanner state beyond the token sequence
//!    and its first error, so producing the whole stream up front is
//!    behavior-preserving.
//!  * [`replication_scanner_is_replication_command`](backend_replication_repl_scanner_seams::replication_scanner_is_replication_command)
//!    — lex only the first token and report whether it is a WalSender
//!    command-introducing keyword (the C `replication_scanner_is_replication_command`).
//!
//! # Differences from the C source (and why)
//!
//!  * **No flex runtime.** The flex DFA is small enough that a hand-written
//!    matcher over the exact `repl_scanner.l` rules reproduces the same accepted
//!    language. Maximal-munch + first-rule-wins are reproduced explicitly (the
//!    `{hexdigit}+\/{hexdigit}+` RECPTR vs `{digit}+` UCONST resolution, and the
//!    exact case-sensitive keyword match preceding `{identifier}`).
//!  * **Identifier folding.** `repl_scanner.l` calls `downcase_truncate_identifier`
//!    and `truncate_identifier` (from `scansup.c`). Those are reached through the
//!    already-declared `backend-parser-scansup-seams`, whose signatures take an
//!    `Mcx`/return `PgVec`. The scanner seams carry no `mcx`, so a transient
//!    `MemoryContext` hosts the call and the result is copied into the owned
//!    `Token` (which holds `String`). The seam owner (`backend-parser-small1`)
//!    installs those; until then a call panics loudly (mirror-PG-and-panic).
//!  * **Error reporting.** `repl_scanner.l`'s `replication_yyerror` does
//!    `ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR), errmsg_internal("%s", message))`
//!    and unwinds. The port returns that as a recoverable [`PgError`].

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, MemoryContext};
use types_core::primitive::XLogRecPtr;
use types_error::{PgError, PgResult};
use types_replication::repl_token::Token;

use backend_parser_scansup_seams::{downcase_truncate_identifier, truncate_identifier};
use backend_replication_repl_scanner_seams as seams;

/// `replication_yyerror(..., message)` (`repl_scanner.l`): the WalSender scanner
/// reports a lexical problem as `ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR),
/// errmsg_internal("%s", message))` and unwinds. The port returns that as a
/// recoverable [`PgError`], matching the cycle-partner grammar unit's identical
/// helper.
fn replication_yyerror(message: &str) -> PgError {
    use backend_utils_error::ereport;
    use types_error::{ERRCODE_SYNTAX_ERROR, ERROR};
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg_internal(message)
        .into_error()
}

// ===========================================================================
// Scanner (repl_scanner.l)
// ===========================================================================

/// The lexical scanner for replication commands — the analogue of the flex
/// scanner generated from `repl_scanner.l`, together with its
/// `replication_yy_extra_type` work area (the `litbuf` accumulation buffer).
///
/// `'mcx` is the memory context under which `scansup` identifier folding is
/// performed (the C parse `CurrentMemoryContext`). The lexed token payloads are
/// owned `String`s in the returned [`Token`]s, so the scanner itself borrows the
/// input and the `mcx` only for the duration of a lex.
struct Scanner<'a, 'mcx> {
    /// The full input (the C `yy_scan_string` buffer).
    input: &'a [u8],
    /// Current scan position (the flex read cursor).
    pos: usize,
    /// The memory context for `downcase_truncate_identifier` / `truncate_identifier`.
    mcx: Mcx<'mcx>,
}

impl<'a, 'mcx> Scanner<'a, 'mcx> {
    /// `replication_scanner_init(str, &yyscanner)` — set up a scanner over `str`.
    fn new(input: &'a str, mcx: Mcx<'mcx>) -> Self {
        Scanner {
            input: input.as_bytes(),
            pos: 0,
            mcx,
        }
    }

    /// `replication_yylex()` — return the next token, running the lexer rules in
    /// `repl_scanner.l` source order. (The one-token `repl_pushed_back_token`
    /// pushback is handled by the seam drivers below, not the per-token lexer.)
    fn next_token(&mut self) -> PgResult<Token> {
        loop {
            // `<<EOF>>`: yyterminate(). Surface the dedicated EOF sentinel; the
            // grammar terminates its stream on it.
            if self.pos >= self.input.len() {
                return Ok(Token::Eof);
            }

            let ch = self.input[self.pos];

            // `{space}+` -> do nothing (skip).  space = [ \t\n\r\f\v]
            if is_space(ch) {
                self.pos += 1;
                continue;
            }

            // `{xqstart}` -> begin xq (single-quoted string), yields SCONST.
            if ch == b'\'' {
                return self.lex_quoted_string();
            }

            // `{xdstart}` -> begin xd (double-quoted identifier), yields IDENT.
            if ch == b'"' {
                return self.lex_delimited_identifier();
            }

            // `{digit}+` -> UCONST, BUT `{hexdigit}+\/{hexdigit}+` -> RECPTR.
            // Flex's maximal-munch + rule order means a run of hex digits
            // followed by `/` and more hex digits is a RECPTR; an all-decimal run
            // not followed by `/...` is a UCONST.
            if is_hexdigit(ch) {
                return self.lex_number_or_recptr();
            }

            // `{identifier}` -> downcase_truncate_identifier -> IDENT.
            // identifier = {ident_start}{ident_cont}* with
            //   ident_start = [A-Za-z\200-\377_]
            //   ident_cont  = [A-Za-z\200-\377_0-9$]
            // The keyword rules (`BASE_BACKUP` ...) precede `{identifier}` and
            // are case-sensitive exact matches; flex's longest-match/first-rule
            // semantics make a keyword win only when the whole token matches it
            // exactly. We reproduce that by lexing the identifier run, then
            // checking for an exact keyword match.
            if is_ident_start(ch) {
                return self.lex_word();
            }

            // `.` -> return yytext[0]: any char not recognized above is returned
            // as itself.
            self.pos += 1;
            return Ok(Token::Char(ch));
        }
    }

    /// `{xqstart} ... <xq>...` — lex a single-quoted string into `SCONST`.
    ///
    /// `xqinside = [^']+`, `xqdouble = ''` (an embedded quote), `quotestop = '`.
    /// The C scanner enters state `xq`, accumulates into the litbuf, turns `''`
    /// into a single `'`, and on the closing quote returns `SCONST`.
    fn lex_quoted_string(&mut self) -> PgResult<Token> {
        debug_assert_eq!(self.input[self.pos], b'\'');
        self.pos += 1; // consume opening quote ({xqstart}, startlit)
        let mut lit: Vec<u8> = Vec::new();
        loop {
            if self.pos >= self.input.len() {
                // `<xq,xd><<EOF>>` -> "unterminated quoted string".
                return Err(replication_yyerror("unterminated quoted string"));
            }
            let ch = self.input[self.pos];
            if ch == b'\'' {
                // `{xqdouble}` ('') -> addlitchar('\''); else `{quotestop}` ends.
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'\'' {
                    push(&mut lit, b'\'')?;
                    self.pos += 2;
                } else {
                    // `<xq>{quotestop}`: yyless(1); BEGIN(INITIAL); SCONST.
                    self.pos += 1; // consume the closing quote
                    let s = bytes_to_string(lit)?;
                    return Ok(Token::Sconst(s));
                }
            } else {
                // `{xqinside}` ([^']+): copy the run verbatim.
                push(&mut lit, ch)?;
                self.pos += 1;
            }
        }
    }

    /// `{xdstart} ... <xd>...` — lex a double-quoted delimited identifier into
    /// `IDENT`, applying `truncate_identifier` (NOT downcasing — quoted
    /// identifiers preserve case), exactly as `<xd>{xdstop}` does.
    ///
    /// `xdinside = [^"]+`, `xddouble = ""` (an embedded double quote).
    fn lex_delimited_identifier(&mut self) -> PgResult<Token> {
        debug_assert_eq!(self.input[self.pos], b'"');
        self.pos += 1; // consume opening dquote ({xdstart}, startlit)
        let mut lit: Vec<u8> = Vec::new();
        loop {
            if self.pos >= self.input.len() {
                return Err(replication_yyerror("unterminated quoted string"));
            }
            let ch = self.input[self.pos];
            if ch == b'"' {
                // `{xddouble}` ("") -> addlitchar('"'); else `{xdstop}` ends.
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'"' {
                    push(&mut lit, b'"')?;
                    self.pos += 2;
                } else {
                    // `<xd>{xdstop}`: yyless(1); BEGIN(INITIAL);
                    // yylval->str = litbufdup; len = strlen(str);
                    // truncate_identifier(str, len, true); return IDENT;
                    self.pos += 1; // consume the closing dquote
                    let folded = truncate_identifier::call(self.mcx, &lit, true)?;
                    let s = bytes_to_string_slice(&folded)?;
                    return Ok(Token::Ident(s));
                }
            } else {
                // `{xdinside}` ([^"]+): copy the run verbatim.
                push(&mut lit, ch)?;
                self.pos += 1;
            }
        }
    }

    /// Resolve the `{digit}+` (UCONST) vs `{hexdigit}+\/{hexdigit}+` (RECPTR)
    /// ambiguity. Both rules begin with a hex-digit run; flex prefers the longer
    /// match, so a hex run followed by `/` and another hex run is a RECPTR.
    fn lex_number_or_recptr(&mut self) -> PgResult<Token> {
        let start = self.pos;
        // Scan the leading hex-digit run.
        while self.pos < self.input.len() && is_hexdigit(self.input[self.pos]) {
            self.pos += 1;
        }
        let hi_end = self.pos;

        // Is this a `%X/%X` RECPTR?  `{hexdigit}+\/{hexdigit}+`
        if self.pos < self.input.len()
            && self.input[self.pos] == b'/'
            && self.pos + 1 < self.input.len()
            && is_hexdigit(self.input[self.pos + 1])
        {
            self.pos += 1; // consume '/'
            while self.pos < self.input.len() && is_hexdigit(self.input[self.pos]) {
                self.pos += 1;
            }
            let text = &self.input[start..self.pos];
            // C: `sscanf(yytext, "%X/%X", &hi, &lo)`; on failure ->
            // replication_yyerror("invalid streaming start location").
            return match parse_recptr(text) {
                Some(recptr) => Ok(Token::Recptr(recptr)),
                None => Err(replication_yyerror("invalid streaming start location")),
            };
        }

        // Not a RECPTR. At this position the only rules that can fire are
        // `{digit}+` (UCONST) and `{identifier}` (which requires an `ident_start`
        // first byte — a *letter*, not a digit). flex picks the longest match.
        //
        //  * If the run starts with a letter (A-F/a-f are `ident_start`), then
        //    `{digit}+` cannot match here at all; the whole run is an identifier
        //    prefix, so re-lex it as a word (it may extend further with more
        //    ident_cont chars).
        //  * If the run starts with a digit, `{digit}+` matches the maximal
        //    leading *decimal*-digit prefix (hex letters are not digits, so the
        //    digit run stops at the first letter). A non-decimal tail (e.g. the
        //    `A` of `1A`) is then re-lexed on the next call as its own token.
        let first = self.input[start];
        if is_ident_start(first) {
            // Reset and lex the full word (collects the remaining ident_cont).
            self.pos = start;
            return self.lex_word();
        }

        // `{digit}+`: the leading decimal-digit prefix only.
        let mut dec_end = start;
        while dec_end < hi_end && self.input[dec_end].is_ascii_digit() {
            dec_end += 1;
        }
        let run = &self.input[start..dec_end];
        // `strtoul(yytext, NULL, 10)` -> UCONST. C uses unsigned long then
        // assigns to uint32 (yylval->uintval); reproduce the 32-bit wrap.
        let val = parse_decimal_u32(run);
        // Leave the cursor right after the decimal prefix so the non-decimal tail
        // (a hex-letter run, an identifier) is lexed on the next call.
        self.pos = dec_end;
        Ok(Token::Uconst(val))
    }

    /// `{identifier}` -> `downcase_truncate_identifier(...)` -> IDENT, unless the
    /// (case-sensitive, exact) token matches one of the `K_*` keyword rules,
    /// which precede `{identifier}` in the scanner.
    fn lex_word(&mut self) -> PgResult<Token> {
        let start = self.pos;
        // ident_start already verified by the caller for the first byte; collect
        // the ident_cont run (the first byte is also a valid ident_cont).
        while self.pos < self.input.len() && is_ident_cont(self.input[self.pos]) {
            self.pos += 1;
        }
        let word = &self.input[start..self.pos];

        // Keyword rules are exact, case-sensitive matches (`BASE_BACKUP { ... }`).
        if let Some(tok) = keyword_token(word) {
            return Ok(tok);
        }

        // `downcase_truncate_identifier(yytext, len, true)` -> IDENT.
        let folded = downcase_truncate_identifier::call(self.mcx, word, true)?;
        let s = bytes_to_string_slice(&folded)?;
        Ok(Token::Ident(s))
    }
}

/// `{space} = [ \t\n\r\f\v]`.
#[inline]
fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

/// `{hexdigit} = [0-9A-Fa-f]`.
#[inline]
fn is_hexdigit(ch: u8) -> bool {
    ch.is_ascii_hexdigit()
}

/// `ident_start = [A-Za-z\200-\377_]`.
#[inline]
fn is_ident_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_' || ch >= 0x80
}

/// `ident_cont = [A-Za-z\200-\377_0-9\$]`.
#[inline]
fn is_ident_cont(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'$' || ch >= 0x80
}

/// Map an exact, case-sensitive keyword spelling to its token, reproducing the
/// `K_*` rules at the top of `repl_scanner.l` (which precede `{identifier}`).
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

/// `sscanf(text, "%X/%X", &hi, &lo)` then `((uint64) hi) << 32 | lo`.
/// Returns `None` when the text is not exactly two hex runs split by one `/`,
/// or when either half overflows a `uint32` (matching `%X`'s 32-bit `unsigned`).
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

/// Parse a hex run as `uint32`, the C `%X` conversion. Returns `None` on a
/// non-hex byte or on overflow past 32 bits.
fn parse_hex_u32(bytes: &[u8]) -> Option<u32> {
    let mut acc: u32 = 0;
    for &b in bytes {
        let d = (b as char).to_digit(16)?;
        acc = acc.checked_mul(16)?.checked_add(d)?;
    }
    Some(acc)
}

/// `strtoul(yytext, NULL, 10)` assigned into a `uint32`. C `strtoul` saturates
/// at `ULONG_MAX` on overflow; the value is then narrowed to `uint32`
/// (`yylval->uintval`). We reproduce the 32-bit wrapping narrowing.
fn parse_decimal_u32(bytes: &[u8]) -> u32 {
    let mut acc: u64 = 0;
    for &b in bytes {
        let d = (b - b'0') as u64;
        acc = acc.wrapping_mul(10).wrapping_add(d);
    }
    acc as u32
}

/// Materialize lexed bytes into an owned `String` (`litbufdup` / `pstrdup`).
/// The bytes come from already-validated input (`yytext` / the litbuf), which is
/// a `&str` slice, so it is valid UTF-8.
fn bytes_to_string(bytes: Vec<u8>) -> PgResult<String> {
    bytes_to_string_slice(&bytes)
}

/// Like [`bytes_to_string`] but from a borrowed slice (the `scansup`-folded
/// `PgVec<u8>` result).
fn bytes_to_string_slice(bytes: &[u8]) -> PgResult<String> {
    let mut s = String::new();
    s.try_reserve_exact(bytes.len())
        .map_err(|_| replication_yyerror("out of memory"))?;
    s.push_str(&String::from_utf8_lossy(bytes));
    Ok(s)
}

/// Fallibly push one byte onto the literal-accumulation buffer (`addlitchar` /
/// `addlit`). `try_reserve(1)` keeps OOM recoverable.
#[inline]
fn push(buf: &mut Vec<u8>, ch: u8) -> PgResult<()> {
    buf.try_reserve(1)
        .map_err(|_| replication_yyerror("out of memory"))?;
    buf.push(ch);
    Ok(())
}

// ===========================================================================
// Seam drivers (the inward seams this crate owns and installs)
// ===========================================================================

/// `replication_lex_all(input)` — lex the entire command string into its token
/// stream, terminated by a single [`Token::Eof`].
///
/// The C grammar drives `replication_yylex()` to `<<EOF>>`, reading one token of
/// lookahead at a time. Bundling the whole stream is behavior-preserving: the
/// only scanner state the grammar can observe is the token sequence and its
/// first lexical error, both reproduced here.
fn lex_all(input: &str) -> PgResult<Vec<Token>> {
    // A transient context hosts the `scansup` identifier-folding seams (which
    // take an `Mcx`/return `PgVec`); the folded bytes are copied into the owned
    // `Token` payloads, so nothing outlives this context.
    let ctx = MemoryContext::new("replication_lex_all");
    let mut scanner = Scanner::new(input, ctx.mcx());
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

/// `replication_scanner_is_replication_command(yyscanner)` — lex only the first
/// token and report whether it is one of the WalSender command-introducing
/// keywords. Mirrors the C function, which lexes a single token and switches on
/// it (the C version pushes the token back; here the grammar re-lexes via
/// `replication_lex_all`, so no pushback state is retained).
fn is_replication_command(input: &str) -> PgResult<bool> {
    let ctx = MemoryContext::new("replication_scanner_is_replication_command");
    let mut scanner = Scanner::new(input, ctx.mcx());
    let first = scanner.next_token()?;
    Ok(first.is_replication_command_first())
}

/// Wire this crate's inward seams. The cycle-partner grammar (`repl_gram.y`)
/// drives this lexer through the `backend-replication-repl-scanner-seams`
/// contract; the scanner owns and installs both seams here.
pub fn init_seams() {
    seams::replication_lex_all::set(lex_all);
    seams::replication_scanner_is_replication_command::set(is_replication_command);
}

#[cfg(test)]
mod tests;
