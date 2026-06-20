//! Port of `src/backend/utils/adt/tsvector_parser.c` — the shared
//! `tsvector`/`tsquery` value tokenizer.
//!
//! C declares `struct TSVectorParseStateData` opaque and exposes
//! init/reset/gettoken/close over an opaque `TSVectorParseState` pointer. The
//! `tsquery` parser drives this engine to tokenize each operand. Across the
//! seam boundary the opaque state lives behind a [`TsVectorParseStateHandle`]
//! token; the owner keeps the real state in a process-local registry keyed by
//! the token's `u64` (the parser state is per-call scratch with its own
//! lifetime, exactly like the C `palloc`'d state).
//!
//! `tsvectorin` ([`crate::io`]) needs the per-token positions, so it calls the
//! richer in-crate [`gettoken_tsvector_full`] directly (same crate). The seam
//! `gettoken_tsvector` wrapper calls it with `want_pos = false` and packages
//! `strval`+`endptr` (the `tsquery` callers pass NULL for the position
//! out-params).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use std::collections::HashMap;

use backend_utils_error::ereport;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_tsearch::tsearch::{
    WordEntryPos, LIMITPOS, P_TSV_IS_TSQUERY, P_TSV_IS_WEB, P_TSV_OPR_IS_DELIM, WEP_GETPOS,
    WEP_GETWEIGHT, WEP_SETPOS, WEP_SETWEIGHT, TsVectorParseStateHandle,
};

use backend_utils_adt_ts_small::util::oom;
use backend_utils_adt_tsvector_core_seams::TsVectorToken;
use backend_utils_mb_mbutils_seams as mb;

extern crate alloc;

// State codes used in gettoken_tsvector (tsvector_parser.c:129-137).
const WAITWORD: i32 = 1;
const WAITENDWORD: i32 = 2;
const WAITNEXTCHAR: i32 = 3;
const WAITENDCMPLX: i32 = 4;
const WAITPOSINFO: i32 = 5;
const INPOSINFO: i32 = 6;
const WAITPOSDELIM: i32 = 7;
const WAITCHARCMPLX: i32 = 8;

/// `struct TSVectorParseStateData` (tsvector_parser.c:37).
///
/// The parser state is process-local scratch with its own lifetime (the C
/// `palloc`'d state), so its buffers are plain owned `Vec`s.
struct TSVectorParseStateData {
    /// the whole input string (C: `bufstart`), used for scanning and for error
    /// messages; `prsbuf` is the cursor into it.
    buf: Vec<u8>,
    /// next input character — index into `buf` (C: `prsbuf`).
    prsbuf: usize,
    /// buffer to hold the current word (C: `word`).
    word: Vec<u8>,
    /// size in bytes "allocated" for `word` (C: `len`) — kept for fidelity of
    /// the documented capacity trace.
    len: i32,
    /// max bytes per character (C: `eml`).
    eml: i32,
    /// treat `! & | ( ) <` as delimiters? (C: `oprisdelim`).
    oprisdelim: bool,
    /// say "tsquery" not "tsvector" in errors? (C: `is_tsquery`).
    is_tsquery: bool,
    /// we're in `websearch_to_tsquery()` (C: `is_web`).
    is_web: bool,
}

thread_local! {
    static PARSER_STATES: RefCell<HashMap<u64, TSVectorParseStateData>> =
        RefCell::new(HashMap::new());
    static NEXT_ID: Cell<u64> = const { Cell::new(1) };
}

impl TSVectorParseStateData {
    /// `*(state->prsbuf)` — the current input byte, or `'\0'` past end of input.
    #[inline]
    fn cur(&self) -> u8 {
        self.buf.get(self.prsbuf).copied().unwrap_or(0)
    }

    /// `state->bufstart` rendered for an error message (`"%s"`).
    fn bufstart_str(&self) -> String {
        String::from_utf8_lossy(&self.buf).into_owned()
    }

    /// `isspace((unsigned char) c)` in the default ("C") locale.
    #[inline]
    fn isspace(c: u8) -> bool {
        matches!(c, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r')
    }

    /// `isdigit((unsigned char) c)`.
    #[inline]
    fn isdigit(c: u8) -> bool {
        c.is_ascii_digit()
    }

    /// `ISOPERATOR(state->prsbuf)` (ts_utils.h:43) — `! & | ( ) <`.
    #[inline]
    fn is_operator(c: u8) -> bool {
        matches!(c, b'!' | b'&' | b'|' | b'(' | b')' | b'<')
    }

    /// `pg_mblen_cstr(state->prsbuf)` — byte length of the current character.
    fn mblen(&self) -> usize {
        // C's `pg_mblen_cstr` does not validate; the range-clamped seam only
        // Errs when the leading char would overrun the slice, where the clamped
        // length is the slice length (dead error path falls back there).
        let rest = &self.buf[self.prsbuf..];
        mb::pg_mblen_range::call(rest).unwrap_or(rest.len() as i32) as usize
    }

    /// `atoi(state->prsbuf)`. The sole caller wraps this in `LIMITPOS(...)`,
    /// which clamps anything `>= MAXENTRYPOS` (16384) to 16383, so saturating
    /// accumulation at `i32::MAX` is observationally identical to glibc `atoi`.
    fn atoi(&self) -> i32 {
        let bytes = &self.buf[self.prsbuf..];
        let mut i = 0usize;
        while i < bytes.len() && Self::isspace(bytes[i]) {
            i += 1;
        }
        let mut sign = 1i64;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            if bytes[i] == b'-' {
                sign = -1;
            }
            i += 1;
        }
        let mut val: i64 = 0;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            val = val * 10 + (bytes[i] - b'0') as i64;
            if val > i32::MAX as i64 {
                val = i32::MAX as i64;
            }
            i += 1;
        }
        (sign * val) as i32
    }

    /// `RESIZEPRSBUF` (tsvector_parser.c:97) — grow `word` if it can no longer
    /// hold one more (up-to-`eml`-byte) character. The `Vec`-backed store grows
    /// on its own in [`copychar`](Self::copychar); this only maintains the
    /// `len` book-keeping for the documented capacity trace.
    #[inline]
    fn resizeprsbuf(&mut self) {
        let clen = self.word.len() as i32;
        if clen + self.eml >= self.len {
            self.len = self.len.saturating_mul(2);
        }
    }

    /// `curpos += ts_copychar_cstr(curpos, state->prsbuf)` — copy the current
    /// multibyte character into `word`. The scan cursor `prsbuf` is advanced
    /// once per loop, at the bottom, exactly as in C.
    fn copychar(&mut self) -> PgResult<()> {
        let n = self.mblen();
        let start = self.prsbuf;
        self.word.try_reserve(n).map_err(|_| oom())?;
        self.word.extend_from_slice(&self.buf[start..start + n]);
        Ok(())
    }

    /// `prssyntaxerror(state)` (tsvector_parser.c:142) — record a syntax error.
    fn prssyntaxerror(&self, escontext: Option<&mut SoftErrorContext>) -> PgResult<bool> {
        let msg = if self.is_tsquery {
            format!("syntax error in tsquery: \"{}\"", self.bufstart_str())
        } else {
            format!("syntax error in tsvector: \"{}\"", self.bufstart_str())
        };
        let err = ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg)
            .into_error();
        ereturn(escontext, false, err)?;
        Ok(false)
    }
}

/// The disposition of the [`gettoken_tsvector_full`] state-machine loop.
enum Outcome {
    /// `return Ok(None)`: end-of-input or a recorded soft error.
    EndOrSoftError,
    /// `RETURN_TOKEN`: assemble the token from `state.word` and `pos`.
    Token,
    /// A hard (thrown) error escaped — `return Err`.
    Hard(PgError),
}

fn prssyntaxerror_outcome(
    state: &TSVectorParseStateData,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> Outcome {
    match state.prssyntaxerror(escontext.as_deref_mut()) {
        Ok(_) => Outcome::EndOrSoftError,
        Err(e) => Outcome::Hard(e),
    }
}

/// One successful result of [`gettoken_tsvector_full`]: the de-escaped token
/// bytes, the positions (if `want_pos`), and the scan resumption point.
pub struct FullToken {
    pub strval: Vec<u8>,
    pub pos: Vec<WordEntryPos>,
    pub endptr: usize,
}

// ===========================================================================
// Registry-backed seam entry points.
// ===========================================================================

/// `init_tsvector_parser(input, flags, escontext)` (tsvector_parser.c:57).
pub fn init_tsvector_parser_seam(
    input: &[u8],
    flags: i32,
) -> PgResult<TsVectorParseStateHandle> {
    // C: state->bufstart = state->prsbuf = pstrdup(input); state->len = 32;
    //    state->word = palloc(state->len).
    let mut buf = Vec::new();
    buf.try_reserve(input.len()).map_err(|_| oom())?;
    buf.extend_from_slice(input);
    let len = 32;
    let mut word = Vec::new();
    word.try_reserve(len as usize).map_err(|_| oom())?;

    let state = TSVectorParseStateData {
        buf,
        prsbuf: 0,
        word,
        len,
        eml: mb::pg_database_encoding_max_length::call(),
        oprisdelim: (flags & P_TSV_OPR_IS_DELIM) != 0,
        is_tsquery: (flags & P_TSV_IS_TSQUERY) != 0,
        is_web: (flags & P_TSV_IS_WEB) != 0,
    };

    let id = NEXT_ID.with(|n| {
        let v = n.get();
        n.set(v + 1);
        v
    });
    PARSER_STATES.with(|m| m.borrow_mut().insert(id, state));
    Ok(TsVectorParseStateHandle(id))
}

/// `reset_tsvector_parser(state, input)` (tsvector_parser.c:81).
pub fn reset_tsvector_parser_seam(state: TsVectorParseStateHandle, input_offset: usize) {
    PARSER_STATES.with(|m| {
        if let Some(s) = m.borrow_mut().get_mut(&state.0) {
            s.prsbuf = input_offset;
        }
    });
}

/// `close_tsvector_parser(state)` (tsvector_parser.c:90).
pub fn close_tsvector_parser_seam(state: TsVectorParseStateHandle) {
    PARSER_STATES.with(|m| {
        m.borrow_mut().remove(&state.0);
    });
}

/// The seam `gettoken_tsvector` (the `tsquery` callers pass NULL for the
/// position out-params, so `want_pos = false`). Packages `strval`+`endptr`.
pub fn gettoken_tsvector_seam<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    state: TsVectorParseStateHandle,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<TsVectorToken>> {
    match gettoken_tsvector_full(state, false, escontext)? {
        Some(tok) => Ok(Some(TsVectorToken {
            strval: tok.strval,
            endptr_offset: tok.endptr,
        })),
        None => Ok(None),
    }
}

/// `gettoken_tsvector(state, strval, lenval, pos_ptr, poslen, endptr)`
/// (tsvector_parser.c:176). Get the next token. Returns `Ok(Some(_))` on
/// success, `Ok(None)` at end-of-input *or* when a soft error was recorded
/// (the caller inspects `escontext.error_occurred()`), and `Err` for a hard
/// error.
///
/// `want_pos` selects whether positions are collected and returned (C: a
/// non-NULL `pos_ptr`/`poslen`). When `false`, parsed positions are discarded.
pub fn gettoken_tsvector_full(
    state: TsVectorParseStateHandle,
    want_pos: bool,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<FullToken>> {
    PARSER_STATES.with(|m| {
        let mut map = m.borrow_mut();
        let state = match map.get_mut(&state.0) {
            Some(s) => s,
            None => return Ok(None),
        };
        gettoken_impl(state, want_pos, &mut escontext)
    })
}

fn gettoken_impl(
    state: &mut TSVectorParseStateData,
    want_pos: bool,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> PgResult<Option<FullToken>> {
    let mut oldstate: i32 = 0;
    // C: char *curpos = state->word; -> reset the word accumulator to empty.
    state.word.clear();
    let mut statecode = WAITWORD;

    // pos collects the comma-delimited list of positions (C: WordEntryPos *pos;
    // int npos; int posalen).
    let mut pos: Vec<WordEntryPos> = Vec::new();
    let mut npos: i32 = 0; // elements of pos used
    let mut posalen: i32 = 0; // allocated size of pos

    let outcome: Outcome = 'sm: loop {
        if statecode == WAITWORD {
            if state.cur() == b'\0' {
                break 'sm Outcome::EndOrSoftError;
            } else if !state.is_web && state.cur() == b'\'' {
                statecode = WAITENDCMPLX;
            } else if !state.is_web && state.cur() == b'\\' {
                statecode = WAITNEXTCHAR;
                oldstate = WAITENDWORD;
            } else if (state.oprisdelim && TSVectorParseStateData::is_operator(state.cur()))
                || (state.is_web && state.cur() == b'"')
            {
                break 'sm prssyntaxerror_outcome(state, escontext);
            } else if !TSVectorParseStateData::isspace(state.cur()) {
                if let Err(e) = state.copychar() {
                    break 'sm Outcome::Hard(e);
                }
                statecode = WAITENDWORD;
            }
        } else if statecode == WAITNEXTCHAR {
            if state.cur() == b'\0' {
                let err = ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "there is no escaped character: \"{}\"",
                        state.bufstart_str()
                    ))
                    .into_error();
                match ereturn(escontext.as_deref_mut(), (), err) {
                    Ok(()) => break 'sm Outcome::EndOrSoftError,
                    Err(e) => break 'sm Outcome::Hard(e),
                }
            } else {
                state.resizeprsbuf();
                if let Err(e) = state.copychar() {
                    break 'sm Outcome::Hard(e);
                }
                debug_assert!(oldstate != 0); // Assert(oldstate != 0)
                statecode = oldstate;
            }
        } else if statecode == WAITENDWORD {
            if !state.is_web && state.cur() == b'\\' {
                statecode = WAITNEXTCHAR;
                oldstate = WAITENDWORD;
            } else if TSVectorParseStateData::isspace(state.cur())
                || state.cur() == b'\0'
                || (state.oprisdelim && TSVectorParseStateData::is_operator(state.cur()))
                || (state.is_web && state.cur() == b'"')
            {
                state.resizeprsbuf();
                if state.word.is_empty() {
                    break 'sm prssyntaxerror_outcome(state, escontext);
                }
                break 'sm Outcome::Token;
            } else if state.cur() == b':' {
                if state.word.is_empty() {
                    break 'sm prssyntaxerror_outcome(state, escontext);
                }
                if state.oprisdelim {
                    break 'sm Outcome::Token;
                } else {
                    statecode = INPOSINFO;
                }
            } else {
                state.resizeprsbuf();
                if let Err(e) = state.copychar() {
                    break 'sm Outcome::Hard(e);
                }
            }
        } else if statecode == WAITENDCMPLX {
            if !state.is_web && state.cur() == b'\'' {
                statecode = WAITCHARCMPLX;
            } else if !state.is_web && state.cur() == b'\\' {
                statecode = WAITNEXTCHAR;
                oldstate = WAITENDCMPLX;
            } else if state.cur() == b'\0' {
                break 'sm prssyntaxerror_outcome(state, escontext);
            } else {
                state.resizeprsbuf();
                if let Err(e) = state.copychar() {
                    break 'sm Outcome::Hard(e);
                }
            }
        } else if statecode == WAITCHARCMPLX {
            if !state.is_web && state.cur() == b'\'' {
                state.resizeprsbuf();
                if let Err(e) = state.copychar() {
                    break 'sm Outcome::Hard(e);
                }
                statecode = WAITENDCMPLX;
            } else {
                state.resizeprsbuf();
                if state.word.is_empty() {
                    break 'sm prssyntaxerror_outcome(state, escontext);
                }
                if state.oprisdelim {
                    break 'sm Outcome::Token;
                } else {
                    statecode = WAITPOSINFO;
                }
                continue 'sm; // recheck current character
            }
        } else if statecode == WAITPOSINFO {
            if state.cur() == b':' {
                statecode = INPOSINFO;
            } else {
                break 'sm Outcome::Token;
            }
        } else if statecode == INPOSINFO {
            if TSVectorParseStateData::isdigit(state.cur()) {
                if posalen == 0 {
                    posalen = 4;
                    npos = 0;
                } else if npos + 1 >= posalen {
                    posalen = posalen.saturating_mul(2);
                }
                npos += 1;
                while (pos.len() as i32) < npos {
                    if pos.try_reserve(1).is_err() {
                        break 'sm Outcome::Hard(oom());
                    }
                    pos.push(0);
                }
                let limited = LIMITPOS(state.atoi()) as u16;
                WEP_SETPOS(&mut pos[(npos - 1) as usize], limited);
                // we cannot get here in tsquery, so no need for 2 errmsgs
                if WEP_GETPOS(pos[(npos - 1) as usize]) == 0 {
                    let err = ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!(
                            "wrong position info in tsvector: \"{}\"",
                            state.bufstart_str()
                        ))
                        .into_error();
                    match ereturn(escontext.as_deref_mut(), (), err) {
                        Ok(()) => break 'sm Outcome::EndOrSoftError,
                        Err(e) => break 'sm Outcome::Hard(e),
                    }
                }
                WEP_SETWEIGHT(&mut pos[(npos - 1) as usize], 0);
                statecode = WAITPOSDELIM;
            } else {
                break 'sm prssyntaxerror_outcome(state, escontext);
            }
        } else if statecode == WAITPOSDELIM {
            if state.cur() == b',' {
                statecode = INPOSINFO;
            } else if state.cur() == b'a' || state.cur() == b'A' || state.cur() == b'*' {
                if WEP_GETWEIGHT(pos[(npos - 1) as usize]) != 0 {
                    break 'sm prssyntaxerror_outcome(state, escontext);
                }
                WEP_SETWEIGHT(&mut pos[(npos - 1) as usize], 3);
            } else if state.cur() == b'b' || state.cur() == b'B' {
                if WEP_GETWEIGHT(pos[(npos - 1) as usize]) != 0 {
                    break 'sm prssyntaxerror_outcome(state, escontext);
                }
                WEP_SETWEIGHT(&mut pos[(npos - 1) as usize], 2);
            } else if state.cur() == b'c' || state.cur() == b'C' {
                if WEP_GETWEIGHT(pos[(npos - 1) as usize]) != 0 {
                    break 'sm prssyntaxerror_outcome(state, escontext);
                }
                WEP_SETWEIGHT(&mut pos[(npos - 1) as usize], 1);
            } else if state.cur() == b'd' || state.cur() == b'D' {
                if WEP_GETWEIGHT(pos[(npos - 1) as usize]) != 0 {
                    break 'sm prssyntaxerror_outcome(state, escontext);
                }
                WEP_SETWEIGHT(&mut pos[(npos - 1) as usize], 0);
            } else if TSVectorParseStateData::isspace(state.cur()) || state.cur() == b'\0' {
                break 'sm Outcome::Token;
            } else if !TSVectorParseStateData::isdigit(state.cur()) {
                break 'sm prssyntaxerror_outcome(state, escontext);
            }
            // (a digit here falls through to "get next char", as in C)
        } else {
            let e = PgError::new(
                ERROR,
                format!("unrecognized state in gettoken_tsvector: {statecode}"),
            );
            break 'sm Outcome::Hard(e);
        }

        // get next char: state->prsbuf += pg_mblen_cstr(state->prsbuf)
        let n = state.mblen();
        state.prsbuf += n;
    };

    // RETURN_TOKEN (tsvector_parser.c:109): fill the output parameters.
    let _ = posalen;
    match outcome {
        Outcome::Hard(e) => Err(e),
        Outcome::EndOrSoftError => Ok(None),
        Outcome::Token => {
            let out_pos: Vec<WordEntryPos> = if want_pos {
                pos[..npos as usize].to_vec()
            } else {
                Vec::new()
            };
            Ok(Some(FullToken {
                strval: state.word.clone(),
                pos: out_pos,
                endptr: state.prsbuf,
            }))
        }
    }
}
