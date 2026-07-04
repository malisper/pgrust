use ::mcx::{Mcx, PgVec};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_SYNTAX_ERROR,
};

use crate::layout::{limitpos, wep_getpos, wep_getweight, wep_setpos, wep_setweight, WordEntryPos};

pub const P_TSV_OPR_IS_DELIM: i32 = 1 << 0;
pub const P_TSV_IS_TSQUERY: i32 = 1 << 1;
pub const P_TSV_IS_WEB: i32 = 1 << 2;

#[inline]
pub fn ts_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

#[inline]
pub fn is_ts_operator(b: u8) -> bool {
    matches!(b, b'!' | b'&' | b'|' | b'(' | b')' | b'<')
}

pub enum Next {
    End,
    Err,
    Tok,
}

pub struct TsvParser<'s, 'e, 'mcx> {
    pub input: &'s [u8],
    pub off: usize,
    oprisdelim: bool,
    is_tsquery: bool,
    is_web: bool,
    pub esc: Option<&'e mut SoftErrorContext>,
    pub word: PgVec<'mcx, u8>,
    pub pos: PgVec<'mcx, WordEntryPos>,
}

enum St {
    WaitWord,
    WaitEndWord,
    WaitNextChar(u8),
    WaitEndCmplx,
    WaitPosInfo,
    InPosInfo,
    WaitPosDelim,
    WaitCharCmplx,
}

impl<'s, 'e, 'mcx> TsvParser<'s, 'e, 'mcx> {
    pub fn new(
        mcx: Mcx<'mcx>,
        input: &'s [u8],
        flags: i32,
        esc: Option<&'e mut SoftErrorContext>,
    ) -> Self {
        TsvParser {
            input,
            off: 0,
            oprisdelim: flags & P_TSV_OPR_IS_DELIM != 0,
            is_tsquery: flags & P_TSV_IS_TSQUERY != 0,
            is_web: flags & P_TSV_IS_WEB != 0,
            esc,
            word: PgVec::new_in(mcx),
            pos: PgVec::new_in(mcx),
        }
    }

    pub fn reset(&mut self, off: usize) {
        self.off = off;
    }

    #[inline]
    fn cur(&self) -> u8 {
        if self.off < self.input.len() {
            self.input[self.off]
        } else {
            0
        }
    }

    #[inline]
    fn at_end(&self) -> bool {
        self.off >= self.input.len()
    }

    #[inline]
    fn mblen(&self) -> usize {
        if self.at_end() {
            1
        } else {
            ::mbutils::pg_mblen(&self.input[self.off..]) as usize
        }
    }

    fn copy_char(&mut self) {
        let cl = self.mblen().min(self.input.len() - self.off);
        self.word.extend_from_slice(&self.input[self.off..self.off + cl]);
    }

    #[cold]
    fn syntax_error(&mut self) -> PgResult<Next> {
        let kind = if self.is_tsquery { "tsquery" } else { "tsvector" };
        ereturn(
            self.esc.as_deref_mut(),
            Next::Err,
            PgError::error(format!(
                "syntax error in {kind}: \"{}\"",
                String::from_utf8_lossy(self.input)
            ))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )
    }

    // gettoken_tsvector; on Tok, `word` and `pos` carry the token.
    pub fn next_token(&mut self) -> PgResult<Next> {
        self.word.clear();
        self.pos.clear();
        let mut state = St::WaitWord;

        loop {
            let c = self.cur();
            match state {
                St::WaitWord => {
                    if self.at_end() {
                        return Ok(Next::End);
                    } else if !self.is_web && c == b'\'' {
                        state = St::WaitEndCmplx;
                    } else if !self.is_web && c == b'\\' {
                        state = St::WaitNextChar(0);
                    } else if (self.oprisdelim && is_ts_operator(c))
                        || (self.is_web && c == b'"')
                    {
                        return self.syntax_error();
                    } else if !ts_isspace(c) {
                        self.copy_char();
                        state = St::WaitEndWord;
                    }
                }
                St::WaitNextChar(oldstate) => {
                    if self.at_end() {
                        return ereturn(
                            self.esc.as_deref_mut(),
                            Next::Err,
                            PgError::error(format!(
                                "there is no escaped character: \"{}\"",
                                String::from_utf8_lossy(self.input)
                            ))
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                        );
                    }
                    self.copy_char();
                    state = if oldstate == 1 { St::WaitEndCmplx } else { St::WaitEndWord };
                }
                St::WaitEndWord => {
                    if !self.is_web && c == b'\\' {
                        state = St::WaitNextChar(0);
                    } else if ts_isspace(c)
                        || self.at_end()
                        || (self.oprisdelim && is_ts_operator(c))
                        || (self.is_web && c == b'"')
                    {
                        if self.word.is_empty() {
                            return self.syntax_error();
                        }
                        return Ok(Next::Tok);
                    } else if c == b':' {
                        if self.word.is_empty() {
                            return self.syntax_error();
                        }
                        if self.oprisdelim {
                            return Ok(Next::Tok);
                        }
                        state = St::InPosInfo;
                    } else {
                        self.copy_char();
                    }
                }
                St::WaitEndCmplx => {
                    if !self.is_web && c == b'\'' {
                        state = St::WaitCharCmplx;
                    } else if !self.is_web && c == b'\\' {
                        state = St::WaitNextChar(1);
                    } else if self.at_end() {
                        return self.syntax_error();
                    } else {
                        self.copy_char();
                    }
                }
                St::WaitCharCmplx => {
                    if !self.is_web && c == b'\'' {
                        self.copy_char();
                        state = St::WaitEndCmplx;
                    } else {
                        if self.word.is_empty() {
                            return self.syntax_error();
                        }
                        if self.oprisdelim {
                            return Ok(Next::Tok);
                        }
                        state = St::WaitPosInfo;
                        continue;
                    }
                }
                St::WaitPosInfo => {
                    if c == b':' && !self.at_end() {
                        state = St::InPosInfo;
                    } else {
                        return Ok(Next::Tok);
                    }
                }
                St::InPosInfo => {
                    if !self.at_end() && c.is_ascii_digit() {
                        let mut v: u32 = 0;
                        let mut i = self.off;
                        while i < self.input.len() && self.input[i].is_ascii_digit() {
                            v = v.saturating_mul(10).saturating_add((self.input[i] - b'0') as u32);
                            i += 1;
                        }
                        let mut p: WordEntryPos = 0;
                        wep_setpos(&mut p, limitpos(v));
                        if wep_getpos(p) == 0 {
                            return ereturn(
                                self.esc.as_deref_mut(),
                                Next::Err,
                                PgError::error(format!(
                                    "wrong position info in tsvector: \"{}\"",
                                    String::from_utf8_lossy(self.input)
                                ))
                                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                            );
                        }
                        wep_setweight(&mut p, 0);
                        self.pos.push(p);
                        state = St::WaitPosDelim;
                    } else {
                        return self.syntax_error();
                    }
                }
                St::WaitPosDelim => {
                    let li = self.pos.len() - 1;
                    if c == b',' && !self.at_end() {
                        state = St::InPosInfo;
                    } else if !self.at_end() && matches!(c, b'a' | b'A' | b'*') {
                        if wep_getweight(self.pos[li]) != 0 {
                            return self.syntax_error();
                        }
                        wep_setweight(&mut self.pos[li], 3);
                    } else if !self.at_end() && matches!(c, b'b' | b'B') {
                        if wep_getweight(self.pos[li]) != 0 {
                            return self.syntax_error();
                        }
                        wep_setweight(&mut self.pos[li], 2);
                    } else if !self.at_end() && matches!(c, b'c' | b'C') {
                        if wep_getweight(self.pos[li]) != 0 {
                            return self.syntax_error();
                        }
                        wep_setweight(&mut self.pos[li], 1);
                    } else if !self.at_end() && matches!(c, b'd' | b'D') {
                        if wep_getweight(self.pos[li]) != 0 {
                            return self.syntax_error();
                        }
                        wep_setweight(&mut self.pos[li], 0);
                    } else if ts_isspace(c) || self.at_end() {
                        return Ok(Next::Tok);
                    } else if !c.is_ascii_digit() {
                        return self.syntax_error();
                    }
                }
            }
            self.off += self.mblen();
        }
    }
}
