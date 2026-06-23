//! Port of `src/backend/replication/syncrep_gram.y` — the Bison grammar for
//! the `synchronous_standby_names` GUC value.
//!
//! The `.y` file declares the parser's token vocabulary (emitted by Bison into
//! `syncrep_gram.h` and consumed by the scanner), the grammar productions, and
//! the static helper `create_syncrep_config`. The generated `syncrep_yyparse`
//! drives the scanner (`syncrep_scanner.l`) one token at a time, accumulating a
//! `List *` of standby names, then flattens that list into a single
//! `palloc`'d [`SyncRepConfigData`] chunk that the GUC machinery stores as the
//! setting's "extra" data.
//!
//! This port keeps the grammar — not Bison's generated state-machine tables,
//! which are a build artifact of the grammar — as the source of truth: the
//! four `standby_config` alternatives, the comma-separated `standby_list`, and
//! the `NAME | NUM` `standby_name` are implemented directly as a recursive
//! descent over the token stream, with the single point of LALR(1) lookahead
//! (a leading `NUM` that may begin either a bare list or the `NUM '('...')'`
//! form) resolved exactly as Bison resolves it.
//!
//! The scanner is the cycle partner: `syncrep_yylex`/`syncrep_yyerror` and the
//! scanner lifecycle are reached through
//! `scanner_seams` (they panic until the scanner
//! unit lands).

#![no_std]

extern crate alloc;

use scanner_seams as scanner;
use mcx::{Mcx, PgString, PgVec};
use types_error::{PgError, PgResult};

/// Bison token codes for the `synchronous_standby_names` grammar, declared by
/// the `%token <str> NAME NUM JUNK ANY FIRST` line of `syncrep_gram.y` and
/// emitted into `syncrep_gram.h`. Bison numbers named tokens consecutively from
/// 258, in declaration order; single-character tokens (`,`, `(`, `)`) use their
/// ASCII byte value, and `0` denotes end of input.
pub const NAME: i32 = 258;
pub const NUM: i32 = 259;
pub const JUNK: i32 = 260;
pub const ANY: i32 = 261;
pub const FIRST: i32 = 262;

/// `syncrep_method` of [`SyncRepConfigData`] (`replication/syncrep.h`).
pub const SYNC_REP_PRIORITY: u8 = 0;
pub const SYNC_REP_QUORUM: u8 = 1;

/// Fixed C-ABI header of `SyncRepConfigData` (`replication/syncrep.h`).
///
/// The C struct is a flat representation held in a single `palloc`'d chunk so
/// it can be stored as the "extra" data for the `synchronous_standby_names`
/// GUC. The trailing `char member_names[FLEXIBLE_ARRAY_MEMBER]` holds
/// `nmembers` consecutive nul-terminated C strings; its bytes live immediately
/// after this header in the flat chunk (see [`FlatSyncRepConfig`]). The
/// `repr(C)` layout is load-bearing: `config_size` and the flat byte image are
/// computed from the field offsets matching the C struct.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncRepConfigData {
    /// Total size of the flat struct, in bytes.
    pub config_size: i32,
    /// Number of sync standbys that we need to wait for.
    pub num_sync: i32,
    /// Method used to choose sync standbys.
    pub syncrep_method: u8,
    /// Number of members in the following name list.
    pub nmembers: i32,
    /// Placeholder for the C `member_names[FLEXIBLE_ARRAY_MEMBER]`; the bytes
    /// themselves follow the header in the flat chunk.
    pub member_names: [u8; 0],
}

/// The flat `palloc`'d `SyncRepConfigData` chunk that `create_syncrep_config`
/// returns: the `repr(C)` header followed by the packed, nul-terminated member
/// names, held in one [`PgVec<u8>`] charged to the parse memory context. The
/// header view ([`Self::header`]) reads the first `offsetof(member_names)`
/// bytes; the member-name region ([`Self::member_names_bytes`]) is everything
/// after it.
pub struct FlatSyncRepConfig<'mcx> {
    bytes: PgVec<'mcx, u8>,
    num_sync: i32,
    syncrep_method: u8,
    nmembers: i32,
}

/// Byte offset of `member_names` within `SyncRepConfigData`, i.e. C's
/// `offsetof(SyncRepConfigData, member_names)` — the size of the fixed header.
pub const SYNCREP_HEADER_SIZE: usize = core::mem::offset_of!(SyncRepConfigData, member_names);

impl<'mcx> FlatSyncRepConfig<'mcx> {
    /// Total chunk size (`config->config_size`).
    pub fn config_size(&self) -> i32 {
        self.bytes.len() as i32
    }

    /// `config->num_sync`.
    pub fn num_sync(&self) -> i32 {
        self.num_sync
    }

    /// `config->syncrep_method`.
    pub fn syncrep_method(&self) -> u8 {
        self.syncrep_method
    }

    /// `config->nmembers`.
    pub fn nmembers(&self) -> i32 {
        self.nmembers
    }

    /// The full flat chunk image (the C `palloc`'d `SyncRepConfigData *` viewed
    /// as bytes): the `repr(C)` header, including its trailing padding, followed
    /// by the packed nul-terminated `member_names`.
    pub fn as_flat_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The packed `member_names` region (the C flexible-array tail): `nmembers`
    /// consecutive nul-terminated C strings.
    pub fn member_names_bytes(&self) -> &[u8] {
        &self.bytes[SYNCREP_HEADER_SIZE..]
    }

    /// Iterate the configured member names (decoded from the nul-terminated
    /// flexible-array tail), mirroring how `syncrep.c` walks `member_names`.
    pub fn member_names(&self) -> impl Iterator<Item = &str> {
        self.member_names_bytes()
            .split(|byte| *byte == 0)
            .take(self.nmembers as usize)
            .map(|bytes| core::str::from_utf8(bytes).expect("scanner stored valid UTF-8 names"))
    }
}

/// `create_syncrep_config(num_sync, members, syncrep_method)` (syncrep_gram.y):
/// transform the parsed `List *members` into the flat
/// [`SyncRepConfigData`] representation.
///
/// Mirrors the C exactly: compute `size = offsetof(member_names) + sum(strlen +
/// 1)`, `palloc(size)` the chunk, then fill the header (`config_size`,
/// `num_sync = atoi(num_sync)`, `syncrep_method`, `nmembers =
/// list_length(members)`) and `strcpy` each name into the trailing flexible
/// array, advancing by `strlen + 1`. The chunk is charged to `mcx` (the C parse
/// `CurrentMemoryContext`); an allocation failure surfaces as the `palloc` OOM
/// `ereport(ERROR)` via [`PgError`].
fn create_syncrep_config<'mcx>(
    mcx: Mcx<'mcx>,
    num_sync: &str,
    members: &PgVec<'mcx, PgString<'mcx>>,
    syncrep_method: u8,
) -> PgResult<FlatSyncRepConfig<'mcx>> {
    // Compute space needed for the flat representation: the fixed header plus,
    // for each member, its bytes and a nul terminator.
    let mut size = SYNCREP_HEADER_SIZE;
    for standby_name in members.iter() {
        size += standby_name.len() + 1;
    }

    // And transform the data into the flat representation: palloc(size).
    let mut bytes: PgVec<u8> = ::mcx::vec_with_capacity_in(mcx, size)?;

    let config_size = size as i32;
    let num_sync = atoi(num_sync);
    let nmembers = members.len() as i32;

    // Write the repr(C) header, including the explicit padding between
    // `syncrep_method` (a `uint8` at offset 8) and `nmembers` (an `int` at
    // offset 12) so the leading `SYNCREP_HEADER_SIZE` bytes match the C struct
    // image byte-for-byte.
    bytes.extend_from_slice(&config_size.to_ne_bytes());
    bytes.extend_from_slice(&num_sync.to_ne_bytes());
    bytes.push(syncrep_method);
    bytes.extend_from_slice(&[0u8; 3]);
    bytes.extend_from_slice(&nmembers.to_ne_bytes());
    debug_assert_eq!(bytes.len(), SYNCREP_HEADER_SIZE);

    // ptr = config->member_names; strcpy each name and advance by strlen + 1.
    for standby_name in members.iter() {
        bytes.extend_from_slice(standby_name.as_bytes());
        bytes.push(0);
    }
    debug_assert_eq!(bytes.len(), size);

    Ok(FlatSyncRepConfig {
        bytes,
        num_sync,
        syncrep_method,
        nmembers,
    })
}

/// C `atoi` semantics for the `num_sync` literal: parse a leading decimal
/// integer, ignoring trailing junk, returning 0 when there is no leading digit.
/// The scanner only ever hands `create_syncrep_config` a `NUM` lexeme (one or
/// more digits) or the constant `"1"`, so overflow saturates to `i32::MAX`.
fn atoi(s: &str) -> i32 {
    let digits: &str = match s.find(|c: char| !c.is_ascii_digit()) {
        Some(end) => &s[..end],
        None => s,
    };
    if digits.is_empty() {
        return 0;
    }
    // The scanner only hands us a digit run, so the only failure here is
    // overflow, which C's `atoi` leaves undefined; saturate to `i32::MAX`.
    digits.parse::<i32>().unwrap_or(i32::MAX)
}

/// The accumulating parser state for one `syncrep_yyparse` call: the working
/// `List *members` (charged to the parse context), the chosen `num_sync` source
/// string and `syncrep_method`, plus the one-token lookahead and the scanner
/// handle the parser drives.
struct Parser<'mcx> {
    mcx: Mcx<'mcx>,
    scanner: scanner::SyncrepScannerHandle,
    /// Pending lookahead token (Bison's `yychar`), `None` once consumed.
    lookahead: Option<scanner::SyncrepLexeme<'mcx>>,
    /// The `List *members` being built (C `lappend`); each entry is a name.
    members: PgVec<'mcx, PgString<'mcx>>,
    /// Source string for `num_sync`; `"1"` for the implicit-priority form.
    num_sync: PgString<'mcx>,
    syncrep_method: u8,
}

impl<'mcx> Parser<'mcx> {
    /// Peek at the current token without consuming it (fills the lookahead).
    fn peek(&mut self) -> PgResult<i32> {
        if self.lookahead.is_none() {
            self.lookahead = Some(scanner::syncrep_yylex::call(self.mcx, self.scanner)?);
        }
        Ok(self.lookahead.as_ref().expect("lookahead just filled").token)
    }

    /// Consume and return the current token (Bison's shift).
    fn next(&mut self) -> PgResult<scanner::SyncrepLexeme<'mcx>> {
        match self.lookahead.take() {
            Some(lexeme) => Ok(lexeme),
            None => scanner::syncrep_yylex::call(self.mcx, self.scanner),
        }
    }

    /// Append a name to the `List *members` (C `lappend`/`list_make1`). The
    /// member count is bounded by the comma-separated tokens in the input, so
    /// the spine growth is bounded; an allocation failure becomes the `palloc`
    /// OOM `ereport(ERROR)`.
    fn append_member(&mut self, name: PgString<'mcx>) -> PgResult<()> {
        self.members
            .try_reserve(1)
            .map_err(|_| self.mcx.oom(core::mem::size_of::<PgString<'mcx>>()))?;
        self.members.push(name);
        Ok(())
    }

    /// Produce the Bison syntax error: call `syncrep_yyerror(... "syntax
    /// error")` (which records the message against the scanner's current
    /// `yytext`) and return the recorded message as a [`PgError`], matching the
    /// C control flow where `yyparse` returns nonzero after `yyerror` set
    /// `*syncrep_parse_error_msg_p`.
    fn syntax_error(&mut self) -> PgResult<PgError> {
        scanner::syncrep_yyerror::call(self.scanner, "syntax error");
        Ok(self.recorded_error()?)
    }

    /// Read back the scanner's recorded first error message as a [`PgError`].
    fn recorded_error(&mut self) -> PgResult<PgError> {
        let msg = scanner::syncrep_scanner_error_msg::call(self.mcx, self.scanner)?;
        let text = match msg {
            Some(text) => text,
            // The scanner always records a message before the parser surfaces an
            // error (an unterminated quote records its own; a grammar mismatch
            // goes through `syntax_error`, which records first). A missing
            // message would be an internal contract violation.
            None => PgString::from_str_in("syntax error", self.mcx)?,
        };
        Ok(PgError::new(::types_error::ERROR, text.as_str()))
    }

    /// `standby_name: NAME | NUM` — append the lexeme's `yylval->str` to the
    /// member list.
    fn standby_name(&mut self) -> PgResult<()> {
        let lexeme = self.next()?;
        if lexeme.token == NAME || lexeme.token == NUM {
            let name = lexeme
                .value
                .expect("NAME/NUM carry a semantic string (yylval->str)");
            self.append_member(name)
        } else {
            self.put_back(lexeme);
            Err(self.syntax_error()?)
        }
    }

    /// Restore an over-consumed token to the lookahead so `syntax_error`
    /// reports against the right `yytext`.
    fn put_back(&mut self, lexeme: scanner::SyncrepLexeme<'mcx>) {
        debug_assert!(self.lookahead.is_none());
        self.lookahead = Some(lexeme);
    }

    /// `standby_list: standby_name | standby_list ',' standby_name` — one or
    /// more comma-separated names.
    fn standby_list(&mut self) -> PgResult<()> {
        self.standby_name()?;
        self.standby_list_tail()
    }

    /// The `standby_list ',' standby_name` left-recursion, after the first
    /// `standby_name` has been parsed.
    fn standby_list_tail(&mut self) -> PgResult<()> {
        while self.peek()? == (b',' as i32) {
            self.next()?; // shift ','
            self.standby_name()?;
        }
        Ok(())
    }

    /// Expect a single-character token, else a syntax error.
    fn expect(&mut self, ch: u8) -> PgResult<()> {
        let lexeme = self.next()?;
        if lexeme.token == ch as i32 {
            Ok(())
        } else {
            self.put_back(lexeme);
            Err(self.syntax_error()?)
        }
    }

    /// Expect a `NUM`, returning its lexeme text (the `num_sync` source).
    fn expect_num(&mut self) -> PgResult<PgString<'mcx>> {
        let lexeme = self.next()?;
        if lexeme.token == NUM {
            Ok(lexeme.value.expect("NUM carries a semantic string"))
        } else {
            self.put_back(lexeme);
            Err(self.syntax_error()?)
        }
    }

    /// `standby_config` — the four alternatives, with the LALR(1) lookahead on a
    /// leading `NUM` (bare list vs. `NUM '(' ... ')'`).
    fn standby_config(&mut self) -> PgResult<()> {
        match self.peek()? {
            // ANY NUM '(' standby_list ')'  -> QUORUM
            ANY => {
                self.next()?; // shift ANY
                self.num_sync = self.expect_num()?;
                self.syncrep_method = SYNC_REP_QUORUM;
                self.expect(b'(')?;
                self.standby_list()?;
                self.expect(b')')
            }
            // FIRST NUM '(' standby_list ')'  -> PRIORITY
            FIRST => {
                self.next()?; // shift FIRST
                self.num_sync = self.expect_num()?;
                self.syncrep_method = SYNC_REP_PRIORITY;
                self.expect(b'(')?;
                self.standby_list()?;
                self.expect(b')')
            }
            // A leading NUM is the LALR(1) decision point. After shifting it,
            // a following '(' selects `NUM '(' standby_list ')'`; anything else
            // reduces the NUM to `standby_name` and continues as a bare
            // `standby_list` (implicit num_sync = "1", PRIORITY).
            NUM => {
                let num_lexeme = self.next()?; // shift NUM
                let num_text = num_lexeme
                    .value
                    .expect("NUM carries a semantic string");
                if self.peek()? == (b'(' as i32) {
                    self.next()?; // shift '('
                    self.num_sync = num_text;
                    self.syncrep_method = SYNC_REP_PRIORITY;
                    self.standby_list()?;
                    self.expect(b')')
                } else {
                    // The NUM is the first `standby_name` of a bare list.
                    self.num_sync = PgString::from_str_in("1", self.mcx)?;
                    self.syncrep_method = SYNC_REP_PRIORITY;
                    self.append_member(num_text)?;
                    self.standby_list_tail()
                }
            }
            // standby_list  -> implicit num_sync = "1", PRIORITY
            NAME => {
                self.num_sync = PgString::from_str_in("1", self.mcx)?;
                self.syncrep_method = SYNC_REP_PRIORITY;
                self.standby_list()
            }
            _ => Err(self.syntax_error()?),
        }
    }

    /// `result: standby_config` then end of input.
    fn parse(&mut self) -> PgResult<()> {
        self.standby_config()?;
        if self.peek()? == 0 {
            Ok(())
        } else {
            Err(self.syntax_error()?)
        }
    }
}

/// `syncrep_yyparse(&result, &error_msg, yyscanner)` (syncrep_gram.y): parse the
/// `synchronous_standby_names` value the scanner is initialized over, returning
/// the flattened [`SyncRepConfigData`].
///
/// The scanner handle is supplied by the caller (in C, `syncrep.c`'s
/// `check_synchronous_standby_names`, which runs `syncrep_scanner_init` first),
/// matching the C `yyscan_t` parameter. On success this is the C
/// `*syncrep_parse_result_p`; on a lexical or grammar error it returns the first
/// recorded message (the C `*syncrep_parse_error_msg_p`) as a [`PgError`].
pub fn syncrep_yyparse<'mcx>(
    mcx: Mcx<'mcx>,
    scanner: scanner::SyncrepScannerHandle,
) -> PgResult<FlatSyncRepConfig<'mcx>> {
    let mut parser = Parser {
        mcx,
        scanner,
        lookahead: None,
        members: PgVec::new_in(mcx),
        num_sync: PgString::new_in(mcx),
        syncrep_method: SYNC_REP_PRIORITY,
    };
    parser.parse()?;
    create_syncrep_config(
        mcx,
        parser.num_sync.as_str(),
        &parser.members,
        parser.syncrep_method,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};
    use ::mcx::MemoryContext;

    #[test]
    fn sync_rep_config_layout_matches_c_header() {
        // The flat chunk image and `config_size` depend on these offsets
        // matching the C `SyncRepConfigData` exactly.
        assert_eq!(size_of::<SyncRepConfigData>(), 16);
        assert_eq!(align_of::<SyncRepConfigData>(), 4);
        assert_eq!(offset_of!(SyncRepConfigData, config_size), 0);
        assert_eq!(offset_of!(SyncRepConfigData, num_sync), 4);
        assert_eq!(offset_of!(SyncRepConfigData, syncrep_method), 8);
        assert_eq!(offset_of!(SyncRepConfigData, nmembers), 12);
        assert_eq!(SYNCREP_HEADER_SIZE, 16);
    }

    #[test]
    fn atoi_matches_c_semantics() {
        assert_eq!(atoi("1"), 1);
        assert_eq!(atoi("0"), 0);
        assert_eq!(atoi("42"), 42);
        // Leading digits only; trailing junk ignored, no leading digit -> 0.
        assert_eq!(atoi("12abc"), 12);
        assert_eq!(atoi("abc"), 0);
        assert_eq!(atoi(""), 0);
    }

    #[test]
    fn create_syncrep_config_packs_flat_chunk() {
        let ctx = MemoryContext::new("create_syncrep_config-test");
        let mcx = ctx.mcx();
        let mut members: PgVec<PgString> = PgVec::new_in(mcx);
        for name in ["node1", "node\"2", "3"] {
            members.push(PgString::from_str_in(name, mcx).unwrap());
        }

        let config = create_syncrep_config(mcx, "1", &members, SYNC_REP_PRIORITY).unwrap();

        assert_eq!(config.num_sync(), 1);
        assert_eq!(config.syncrep_method(), SYNC_REP_PRIORITY);
        assert_eq!(config.nmembers(), 3);
        let names = b"node1\0node\"2\03\0";
        assert_eq!(config.config_size(), (SYNCREP_HEADER_SIZE + names.len()) as i32);
        assert_eq!(config.member_names_bytes(), names);
        assert_eq!(
            config.member_names().collect::<alloc::vec::Vec<_>>(),
            ["node1", "node\"2", "3"]
        );

        // The leading header bytes are the repr(C) image: config_size, num_sync,
        // syncrep_method, 3 padding bytes, nmembers.
        let flat = config.as_flat_bytes();
        assert_eq!(&flat[0..4], &(config.config_size()).to_ne_bytes());
        assert_eq!(&flat[4..8], &1i32.to_ne_bytes());
        assert_eq!(flat[8], SYNC_REP_PRIORITY);
        assert_eq!(&flat[12..16], &3i32.to_ne_bytes());
        assert_eq!(&flat[16..], names);
    }

    // ------------------------------------------------------------------
    // End-to-end grammar coverage: a minimal in-test scanner installed into
    // the scanner seams drives `syncrep_yyparse` through every production and
    // the LALR(1) lookahead, proving the grammar reductions and error paths.
    // ------------------------------------------------------------------

    extern crate std;
    use alloc::string::{String, ToString};
    use alloc::vec::Vec as StdVec;
    use core::cell::RefCell;
    use scanner::{SyncrepLexeme, SyncrepScannerHandle};
    use std::sync::Once;

    std::thread_local! {
        static SCANNERS: RefCell<StdVec<Option<TestScanner>>> = const { RefCell::new(StdVec::new()) };
    }

    struct TestScanner {
        tokens: StdVec<(i32, Option<String>)>,
        pos: usize,
        yytext: String,
        error: Option<String>,
    }

    /// Tokenize like `syncrep_scanner.l` for the inputs the grammar tests use:
    /// keywords ANY/FIRST (case-insensitive), numbers, `(`/`)`/`,`, `*`,
    /// double-quoted identifiers with `""` escape, and bare identifiers.
    fn lex_all(input: &str) -> (StdVec<(i32, Option<String>)>, Option<String>) {
        let bytes = input.as_bytes();
        let mut i = 0;
        let mut out = StdVec::new();
        let mut error = None;
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_whitespace() {
                i += 1;
                continue;
            }
            match c {
                ',' | '(' | ')' => {
                    out.push((c as i32, None));
                    i += 1;
                }
                '*' => {
                    out.push((NAME, Some("*".to_string())));
                    i += 1;
                }
                '"' => {
                    i += 1;
                    let mut val = String::new();
                    let mut closed = false;
                    while i < bytes.len() {
                        if bytes[i] == b'"' {
                            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                                val.push('"');
                                i += 2;
                                continue;
                            }
                            i += 1;
                            closed = true;
                            break;
                        }
                        val.push(bytes[i] as char);
                        i += 1;
                    }
                    if closed {
                        out.push((NAME, Some(val)));
                    } else {
                        error.get_or_insert_with(|| {
                            "unterminated quoted identifier at end of input".to_string()
                        });
                        out.push((JUNK, None));
                    }
                }
                c if c.is_ascii_digit() => {
                    let start = i;
                    while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                        i += 1;
                    }
                    out.push((NUM, Some(input[start..i].to_string())));
                }
                c if c.is_ascii_alphabetic() || c == '_' => {
                    let start = i;
                    while i < bytes.len()
                        && (((bytes[i] as char).is_ascii_alphanumeric())
                            || bytes[i] == b'_'
                            || bytes[i] == b'$')
                    {
                        i += 1;
                    }
                    let word = &input[start..i];
                    if word.eq_ignore_ascii_case("any") {
                        out.push((ANY, None));
                    } else if word.eq_ignore_ascii_case("first") {
                        out.push((FIRST, None));
                    } else {
                        out.push((NAME, Some(word.to_string())));
                    }
                }
                _ => {
                    out.push((JUNK, None));
                    i += 1;
                }
            }
        }
        (out, error)
    }

    fn install_test_scanner() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            scanner::syncrep_scanner_init::set(|mcx, input| {
                let (tokens, error) = lex_all(input);
                let _ = mcx;
                let handle = SCANNERS.with(|s| {
                    let mut s = s.borrow_mut();
                    s.push(Some(TestScanner {
                        tokens,
                        pos: 0,
                        yytext: String::new(),
                        error,
                    }));
                    (s.len() - 1) as u64
                });
                Ok(SyncrepScannerHandle(handle))
            });
            scanner::syncrep_yylex::set(|mcx, handle| {
                SCANNERS.with(|s| {
                    let mut s = s.borrow_mut();
                    let sc = s[handle.0 as usize].as_mut().unwrap();
                    if sc.pos >= sc.tokens.len() {
                        sc.yytext.clear();
                        return Ok(SyncrepLexeme { token: 0, value: None });
                    }
                    let (token, text) = sc.tokens[sc.pos].clone();
                    sc.pos += 1;
                    sc.yytext = match (&text, token) {
                        (Some(t), _) => t.clone(),
                        (None, t) if t < 256 => ((t as u8) as char).to_string(),
                        _ => String::new(),
                    };
                    let value = match text {
                        Some(t) => Some(PgString::from_str_in(&t, mcx)?),
                        None => None,
                    };
                    Ok(SyncrepLexeme { token, value })
                })
            });
            scanner::syncrep_yyerror::set(|handle, message| {
                SCANNERS.with(|s| {
                    let mut s = s.borrow_mut();
                    let sc = s[handle.0 as usize].as_mut().unwrap();
                    if sc.error.is_some() {
                        return;
                    }
                    sc.error = Some(if sc.yytext.is_empty() {
                        alloc::format!("{message} at end of input")
                    } else {
                        alloc::format!("{message} at or near \"{}\"", sc.yytext)
                    });
                });
            });
            scanner::syncrep_scanner_error_msg::set(|mcx, handle| {
                SCANNERS.with(|s| {
                    let s = s.borrow();
                    let sc = s[handle.0 as usize].as_ref().unwrap();
                    match &sc.error {
                        Some(msg) => Ok(Some(PgString::from_str_in(msg, mcx)?)),
                        None => Ok(None),
                    }
                })
            });
            scanner::syncrep_scanner_finish::set(|handle| {
                SCANNERS.with(|s| {
                    s.borrow_mut()[handle.0 as usize] = None;
                });
            });
        });
    }

    fn parse(input: &str) -> Result<(i32, u8, StdVec<String>), String> {
        install_test_scanner();
        let ctx = MemoryContext::new("syncrep_yyparse-test");
        let mcx = ctx.mcx();
        let handle = scanner::syncrep_scanner_init::call(mcx, input).unwrap();
        let result = syncrep_yyparse(mcx, handle);
        scanner::syncrep_scanner_finish::call(handle);
        match result {
            Ok(config) => Ok((
                config.num_sync(),
                config.syncrep_method(),
                config.member_names().map(|s| s.to_string()).collect(),
            )),
            Err(e) => Err(e.message.clone()),
        }
    }

    #[test]
    fn parses_implicit_priority_list() {
        let (num_sync, method, names) = parse("node1, \"node\"\"2\", 3").unwrap();
        assert_eq!(num_sync, 1);
        assert_eq!(method, SYNC_REP_PRIORITY);
        assert_eq!(names, ["node1", "node\"2", "3"]);
    }

    #[test]
    fn parses_explicit_priority_num_paren_form() {
        let (num_sync, method, names) = parse("2(node1,node2)").unwrap();
        assert_eq!(num_sync, 2);
        assert_eq!(method, SYNC_REP_PRIORITY);
        assert_eq!(names, ["node1", "node2"]);
    }

    #[test]
    fn parses_any_quorum_and_first_priority() {
        let (num_sync, method, names) = parse("ANY 3 (a,b,c)").unwrap();
        assert_eq!(num_sync, 3);
        assert_eq!(method, SYNC_REP_QUORUM);
        assert_eq!(names, ["a", "b", "c"]);

        let (num_sync, method, names) = parse("FIRST 1 (*)").unwrap();
        assert_eq!(num_sync, 1);
        assert_eq!(method, SYNC_REP_PRIORITY);
        assert_eq!(names, ["*"]);
    }

    #[test]
    fn leading_num_then_name_is_a_bare_list() {
        // The LALR(1) lookahead: NUM not followed by '(' reduces to the first
        // standby_name of an implicit-priority list.
        let (num_sync, method, names) = parse("3, node").unwrap();
        assert_eq!(num_sync, 1);
        assert_eq!(method, SYNC_REP_PRIORITY);
        assert_eq!(names, ["3", "node"]);
    }

    #[test]
    fn reports_syntax_and_scanner_errors() {
        assert_eq!(parse("\"abc").unwrap_err(), "unterminated quoted identifier at end of input");
        assert_eq!(parse("ANY (a)").unwrap_err(), "syntax error at or near \"(\"");
        assert_eq!(parse("a b").unwrap_err(), "syntax error at or near \"b\"");
    }

    #[test]
    fn create_syncrep_config_quorum_with_numeric_num_sync() {
        let ctx = MemoryContext::new("create_syncrep_config-quorum");
        let mcx = ctx.mcx();
        let mut members: PgVec<PgString> = PgVec::new_in(mcx);
        for name in ["a", "b", "c"] {
            members.push(PgString::from_str_in(name, mcx).unwrap());
        }
        let config = create_syncrep_config(mcx, "3", &members, SYNC_REP_QUORUM).unwrap();
        assert_eq!(config.num_sync(), 3);
        assert_eq!(config.syncrep_method(), SYNC_REP_QUORUM);
        assert_eq!(config.nmembers(), 3);
        assert_eq!(config.member_names_bytes(), b"a\0b\0c\0");
    }
}
