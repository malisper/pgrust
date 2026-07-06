use ::adt_tsvector_core::layout::MAXENTRYPOS;
use ::adt_tsvector_core::parser::{
    is_ts_operator, ts_isspace, Next, TsvParser, P_TSV_IS_TSQUERY, P_TSV_IS_WEB,
    P_TSV_OPR_IS_DELIM,
};
use ::adt_tsvector_core::query::*;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR,
};

pub const P_TSQ_PLAIN: i32 = 1 << 0;
pub const P_TSQ_WEB: i32 = 1 << 1;

pub const MAXSTRLEN: usize = ::adt_tsvector_core::layout::MAXSTRLEN;
pub const MAXSTRPOS: usize = ::adt_tsvector_core::layout::MAXSTRPOS;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PState {
    WaitOperand,
    WaitOperator,
    WaitFirstOperand,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tok {
    End,
    Err,
    Val,
    Opr(i8),
    Open,
    Close,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tokenizer {
    Standard,
    Websearch,
    Plain,
}

pub struct QueryParser<'s, 'e, 'mcx> {
    mcx: Mcx<'mcx>,
    tokenizer: Tokenizer,
    count: i32,
    state: PState,
    pub polstr: PgVec<'mcx, Item>,
    pub op_pool: PgVec<'mcx, u8>,
    vals: TsvParser<'s, 'e, 'mcx>,
    // Token output scratch (the C strval/lenval/weight/prefix outs).
    curval: PgVec<'mcx, u8>,
    weight: i16,
    prefix: bool,
}

impl<'s, 'e, 'mcx> QueryParser<'s, 'e, 'mcx> {
    pub fn new(
        mcx: Mcx<'mcx>,
        input: &'s [u8],
        flags: i32,
        esc: Option<&'e mut SoftErrorContext>,
    ) -> Self {
        let mut tsv_flags = P_TSV_OPR_IS_DELIM | P_TSV_IS_TSQUERY;
        let tokenizer = if flags & P_TSQ_PLAIN != 0 {
            Tokenizer::Plain
        } else if flags & P_TSQ_WEB != 0 {
            tsv_flags |= P_TSV_IS_WEB;
            Tokenizer::Websearch
        } else {
            Tokenizer::Standard
        };
        QueryParser {
            mcx,
            tokenizer,
            count: 0,
            state: PState::WaitFirstOperand,
            polstr: PgVec::new_in(mcx),
            op_pool: PgVec::new_in(mcx),
            vals: TsvParser::new(mcx, input, tsv_flags, esc),
            curval: PgVec::new_in(mcx),
            weight: 0,
            prefix: false,
        }
    }

    #[inline]
    pub fn input(&self) -> &'s [u8] {
        self.vals.input
    }

    #[inline]
    fn cur(&self) -> u8 {
        if self.vals.off < self.vals.input.len() {
            self.vals.input[self.vals.off]
        } else {
            0
        }
    }

    #[inline]
    fn at_end(&self) -> bool {
        self.vals.off >= self.vals.input.len()
    }

    #[inline]
    fn advance(&mut self) {
        let n = if self.at_end() {
            1
        } else {
            ::mbutils::pg_mblen(&self.vals.input[self.vals.off..]) as usize
        };
        self.vals.off += n;
    }

    pub fn soft_error_occurred(&self) -> bool {
        self.vals.esc.as_deref().map(|c| c.error_occurred()).unwrap_or(false)
    }

    pub fn take_esc(&mut self) -> Option<&'e mut SoftErrorContext> {
        self.vals.esc.take()
    }

    pub fn put_esc(&mut self, esc: Option<&'e mut SoftErrorContext>) {
        self.vals.esc = esc;
    }

    fn get_modifiers(&mut self) {
        self.weight = 0;
        self.prefix = false;
        if self.cur() != b':' || self.at_end() {
            return;
        }
        self.vals.off += 1;
        while !self.at_end() && ::mbutils::pg_mblen(&self.vals.input[self.vals.off..]) == 1 {
            match self.cur() {
                b'a' | b'A' => self.weight |= 1 << 3,
                b'b' | b'B' => self.weight |= 1 << 2,
                b'c' | b'C' => self.weight |= 1 << 1,
                b'd' | b'D' => self.weight |= 1,
                b'*' => self.prefix = true,
                _ => return,
            }
            self.vals.off += 1;
        }
    }

    fn parse_phrase_operator(&mut self) -> PgResult<Option<bool>> {
        #[derive(PartialEq)]
        enum Ph {
            Open,
            Dist,
            Close,
            Finish,
        }
        let mut state = Ph::Open;
        let mut ptr = self.vals.off;
        let mut l: i64 = 1;
        let input = self.vals.input;

        while ptr < input.len() || state == Ph::Finish {
            match state {
                Ph::Open => {
                    if input[ptr] == b'<' {
                        state = Ph::Dist;
                        ptr += 1;
                    } else {
                        return Ok(Some(false));
                    }
                }
                Ph::Dist => {
                    if input[ptr] == b'-' {
                        state = Ph::Close;
                        ptr += 1;
                        continue;
                    }
                    if !input[ptr].is_ascii_digit() {
                        return Ok(Some(false));
                    }
                    let start = ptr;
                    let mut v: i64 = 0;
                    while ptr < input.len() && input[ptr].is_ascii_digit() {
                        v = v.saturating_mul(10).saturating_add((input[ptr] - b'0') as i64);
                        ptr += 1;
                    }
                    if ptr == start {
                        return Ok(Some(false));
                    } else if v > MAXENTRYPOS as i64 {
                        ereturn(
                            self.vals.esc.as_deref_mut(),
                            (),
                            PgError::error(format!(
                                "distance in phrase operator must be an integer value between zero and {MAXENTRYPOS} inclusive"
                            ))
                            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
                        )?;
                        return Ok(None);
                    } else {
                        l = v;
                        state = Ph::Close;
                    }
                }
                Ph::Close => {
                    if input[ptr] == b'>' {
                        state = Ph::Finish;
                        ptr += 1;
                    } else {
                        return Ok(Some(false));
                    }
                }
                Ph::Finish => {
                    self.weight = l as i16;
                    self.vals.off = ptr;
                    return Ok(Some(true));
                }
            }
        }
        Ok(Some(false))
    }

    fn parse_or_operator(&mut self) -> bool {
        let input = self.vals.input;
        let start = self.vals.off;
        if input.len() < start + 2 {
            return false;
        }
        if !input[start].eq_ignore_ascii_case(&b'o') || !input[start + 1].eq_ignore_ascii_case(&b'r')
        {
            return false;
        }
        let mut ptr = start + 2;
        if ptr >= input.len() {
            return false;
        }
        if input[ptr] == b'-'
            || input[ptr] == b'_'
            || ::ts_locale::t_isalnum(&input[ptr..])
        {
            return false;
        }
        loop {
            ptr += (::mbutils::pg_mblen(&input[ptr..]) as usize).max(1);
            if ptr >= input.len() {
                return false;
            }
            if !ts_isspace(input[ptr]) {
                break;
            }
        }
        self.vals.off = start + 2;
        true
    }

    // gettoken_query_*; Ok(None) = PT_ERR with soft error already recorded.
    fn next(&mut self) -> PgResult<Tok> {
        match self.tokenizer {
            Tokenizer::Standard => self.next_standard(),
            Tokenizer::Websearch => self.next_websearch(),
            Tokenizer::Plain => self.next_plain(),
        }
    }

    fn take_value(&mut self) -> PgResult<bool> {
        self.vals.reset(self.vals.off);
        match self.vals.next_token()? {
            Next::Tok => {
                self.curval.clear();
                self.curval.extend_from_slice(&self.vals.word);
                Ok(true)
            }
            Next::Err => Ok(false),
            Next::End => Ok(false),
        }
    }

    fn next_standard(&mut self) -> PgResult<Tok> {
        self.weight = 0;
        self.prefix = false;
        loop {
            match self.state {
                PState::WaitFirstOperand | PState::WaitOperand => {
                    if self.cur() == b'!' && !self.at_end() {
                        self.vals.off += 1;
                        self.state = PState::WaitOperand;
                        return Ok(Tok::Opr(OP_NOT));
                    } else if self.cur() == b'(' && !self.at_end() {
                        self.vals.off += 1;
                        self.state = PState::WaitOperand;
                        self.count += 1;
                        return Ok(Tok::Open);
                    } else if self.cur() == b':' && !self.at_end() {
                        return Ok(Tok::Err);
                    } else if !ts_isspace(self.cur()) || self.at_end() {
                        let was_first = self.state == PState::WaitFirstOperand;
                        if self.take_value()? {
                            self.get_modifiers();
                            self.state = PState::WaitOperator;
                            return Ok(Tok::Val);
                        } else if self.soft_error_occurred() {
                            return Ok(Tok::Err);
                        } else if was_first {
                            return Ok(Tok::End);
                        } else {
                            ereturn(
                                self.vals.esc.as_deref_mut(),
                                (),
                                PgError::error(format!(
                                    "no operand in tsquery: \"{}\"",
                                    String::from_utf8_lossy(self.vals.input)
                                ))
                                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                            )?;
                            return Ok(Tok::Err);
                        }
                    }
                }
                PState::WaitOperator => {
                    if self.cur() == b'&' && !self.at_end() {
                        self.vals.off += 1;
                        self.state = PState::WaitOperand;
                        return Ok(Tok::Opr(OP_AND));
                    } else if self.cur() == b'|' && !self.at_end() {
                        self.vals.off += 1;
                        self.state = PState::WaitOperand;
                        return Ok(Tok::Opr(OP_OR));
                    } else {
                        match if self.at_end() { Some(false) } else { self.parse_phrase_operator()? } {
                            None => return Ok(Tok::Err),
                            Some(true) => {
                                self.state = PState::WaitOperand;
                                return Ok(Tok::Opr(OP_PHRASE));
                            }
                            Some(false) => {}
                        }
                        if self.soft_error_occurred() {
                            return Ok(Tok::Err);
                        }
                        if self.cur() == b')' && !self.at_end() {
                            self.vals.off += 1;
                            self.count -= 1;
                            return Ok(if self.count < 0 { Tok::Err } else { Tok::Close });
                        } else if self.at_end() {
                            return Ok(if self.count != 0 { Tok::Err } else { Tok::End });
                        } else if !ts_isspace(self.cur()) {
                            return Ok(Tok::Err);
                        }
                    }
                }
            }
            self.advance();
        }
    }

    fn next_websearch(&mut self) -> PgResult<Tok> {
        self.weight = 0;
        self.prefix = false;
        loop {
            match self.state {
                PState::WaitFirstOperand | PState::WaitOperand => {
                    if self.cur() == b'-' && !self.at_end() {
                        self.vals.off += 1;
                        self.state = PState::WaitOperand;
                        return Ok(Tok::Opr(OP_NOT));
                    } else if self.cur() == b'"' && !self.at_end() {
                        self.vals.off += 1;
                        let start = self.vals.off;
                        while !self.at_end() && self.cur() != b'"' {
                            self.vals.off += 1;
                        }
                        self.curval.clear();
                        let end = self.vals.off;
                        let input = self.vals.input;
                        self.curval.extend_from_slice(&input[start..end]);
                        if !self.at_end() {
                            self.vals.off += 1;
                        }
                        self.state = PState::WaitOperator;
                        self.count += 1;
                        return Ok(Tok::Val);
                    } else if is_ts_operator(self.cur()) && !self.at_end() {
                        self.vals.off += 1;
                        self.state = PState::WaitOperand;
                        continue;
                    } else if !ts_isspace(self.cur()) || self.at_end() {
                        let was_first = self.state == PState::WaitFirstOperand;
                        if self.take_value()? {
                            self.state = PState::WaitOperator;
                            return Ok(Tok::Val);
                        } else if self.soft_error_occurred() {
                            return Ok(Tok::Err);
                        } else if was_first {
                            return Ok(Tok::End);
                        } else {
                            self.push_stop();
                            return Ok(Tok::End);
                        }
                    }
                }
                PState::WaitOperator => {
                    if self.at_end() {
                        return Ok(Tok::End);
                    } else if self.parse_or_operator() {
                        self.state = PState::WaitOperand;
                        return Ok(Tok::Opr(OP_OR));
                    } else if is_ts_operator(self.cur()) {
                        // C stays in WAITOPERATOR here (tsquery.c:488-493); a
                        // state change would drop the implicit AND below.
                        self.vals.off += 1;
                        continue;
                    } else if !ts_isspace(self.cur()) {
                        self.state = PState::WaitOperand;
                        return Ok(Tok::Opr(OP_AND));
                    }
                }
            }
            self.advance();
        }
    }

    fn next_plain(&mut self) -> PgResult<Tok> {
        self.weight = 0;
        self.prefix = false;
        if self.at_end() {
            return Ok(Tok::End);
        }
        self.curval.clear();
        let rest = &self.vals.input[self.vals.off..];
        self.curval.extend_from_slice(rest);
        self.vals.off = self.vals.input.len();
        self.count += 1;
        Ok(Tok::Val)
    }

    pub fn push_operator(&mut self, oper: i8, distance: i16) {
        self.polstr.push(Item::Opr(Operator {
            oper,
            distance: if oper == OP_PHRASE { distance } else { 0 },
            left: 0,
        }));
    }

    pub fn push_value(&mut self, strval: &[u8], weight: i16, prefix: bool) -> PgResult<()> {
        if strval.len() >= MAXSTRLEN {
            return ereturn(
                self.vals.esc.as_deref_mut(),
                (),
                PgError::error(format!(
                    "word is too long in tsquery: \"{}\"",
                    String::from_utf8_lossy(self.vals.input)
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        let distance = self.op_pool.len();
        if distance >= MAXSTRPOS {
            return ereturn(
                self.vals.esc.as_deref_mut(),
                (),
                PgError::error(format!(
                    "value is too big in tsquery: \"{}\"",
                    String::from_utf8_lossy(self.vals.input)
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        let valcrc = ::crc32c::legacy_crc32_lexeme(strval) as i32;
        self.polstr.push(Item::Val(Operand {
            weight: weight as u8,
            prefix,
            valcrc,
            length: strval.len(),
            distance,
        }));
        ::mcx::vec_append_bytes(&mut self.op_pool, strval)?;
        self.op_pool.push(0);
        Ok(())
    }

    pub fn push_stop(&mut self) {
        self.polstr.push(Item::ValStop);
    }

    // makepol; pushval sees the parser plus the token's (value, weight, prefix).
    pub fn makepol(
        &mut self,
        pushval: &mut dyn FnMut(&mut Self, &[u8], i16, bool) -> PgResult<()>,
    ) -> PgResult<()> {
        const STACKDEPTH: usize = 32;
        let mut opstack: [(i8, i16); STACKDEPTH] = [(0, 0); STACKDEPTH];
        let mut lenstack = 0usize;

        loop {
            let tok = self.next()?;
            match tok {
                Tok::End => break,
                Tok::Val => {
                    let val = core::mem::replace(&mut self.curval, PgVec::new_in(self.mcx));
                    let (w, p) = (self.weight, self.prefix);
                    pushval(self, &val, w, p)?;
                    self.curval = val;
                }
                Tok::Opr(operator) => {
                    let op_prio = op_priority(operator);
                    while lenstack > 0 {
                        let (top_op, top_dist) = opstack[lenstack - 1];
                        let keep = if operator != OP_NOT {
                            op_prio > op_priority(top_op)
                        } else {
                            op_prio >= op_priority(top_op)
                        };
                        if keep {
                            break;
                        }
                        lenstack -= 1;
                        self.push_operator(top_op, top_dist);
                    }
                    if lenstack == STACKDEPTH {
                        return Err(PgError::error("tsquery stack too small").into());
                    }
                    opstack[lenstack] = (operator, self.weight);
                    lenstack += 1;
                }
                Tok::Open => {
                    self.makepol(pushval)?;
                }
                Tok::Close => {
                    while lenstack > 0 {
                        lenstack -= 1;
                        let (op, dist) = opstack[lenstack];
                        self.push_operator(op, dist);
                    }
                    return Ok(());
                }
                Tok::Err => {
                    if !self.soft_error_occurred() {
                        ereturn(
                            self.vals.esc.as_deref_mut(),
                            (),
                            PgError::error(format!(
                                "syntax error in tsquery: \"{}\"",
                                String::from_utf8_lossy(self.vals.input)
                            ))
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                        )?;
                    }
                    return Ok(());
                }
            }
            if self.soft_error_occurred() {
                return Ok(());
            }
        }
        while lenstack > 0 {
            lenstack -= 1;
            let (op, dist) = opstack[lenstack];
            self.push_operator(op, dist);
        }
        Ok(())
    }
}

fn findoprnd_recurse(
    items: &mut [Item],
    pos: &mut usize,
    needcleanup: &mut bool,
) -> PgResult<()> {
    if *pos >= items.len() {
        return Err(PgError::error("malformed tsquery: operand not found").into());
    }
    match items[*pos] {
        Item::Val(_) => {
            *pos += 1;
        }
        Item::ValStop => {
            *needcleanup = true;
            *pos += 1;
        }
        Item::Opr(mut opr) => {
            if opr.oper == OP_NOT {
                opr.left = 1;
                items[*pos] = Item::Opr(opr);
                *pos += 1;
                findoprnd_recurse(items, pos, needcleanup)?;
            } else {
                let tmp = *pos;
                *pos += 1;
                findoprnd_recurse(items, pos, needcleanup)?;
                opr.left = (*pos - tmp) as u32;
                items[tmp] = Item::Opr(opr);
                findoprnd_recurse(items, pos, needcleanup)?;
            }
        }
    }
    Ok(())
}

pub fn findoprnd(items: &mut [Item], needcleanup: &mut bool) -> PgResult<()> {
    *needcleanup = false;
    let mut pos = 0usize;
    findoprnd_recurse(items, &mut pos, needcleanup)?;
    if pos != items.len() {
        return Err(PgError::error("malformed tsquery: extra nodes").into());
    }
    Ok(())
}

pub fn build_query_image<'mcx>(
    mcx: Mcx<'mcx>,
    items: &[Item],
    op_pool: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let total = 4 + 4 + items.len() * QUERYITEM_SIZE + op_pool.len();
    let mut img = vec_with_capacity_in(mcx, total)?;
    ::mcx::vec_append_bytes(&mut img, &[0u8; 4])?;
    ::mcx::vec_append_bytes(&mut img, &(items.len() as i32).to_ne_bytes())?;
    for it in items {
        ::mcx::vec_append_bytes(&mut img, &it.encode())?;
    }
    ::mcx::vec_append_bytes(&mut img, op_pool)?;
    Ok(img)
}

pub struct ParsedQuery<'mcx> {
    pub img: PgVec<'mcx, u8>,
    pub empty: bool,
}

// parse_tsquery; Ok(None) = soft error recorded.
pub fn parse_tsquery<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    flags: i32,
    esc: Option<&mut SoftErrorContext>,
    pushval: &mut dyn FnMut(&mut QueryParser<'_, '_, 'mcx>, &[u8], i16, bool) -> PgResult<()>,
) -> PgResult<Option<ParsedQuery<'mcx>>> {
    let noisy = esc.is_none();
    let mut p = QueryParser::new(mcx, input, flags, esc);
    p.makepol(pushval)?;
    if p.soft_error_occurred() {
        return Ok(None);
    }

    if p.polstr.is_empty() {
        if noisy {
            ::elog::ThrowErrorData(PgError::notice(format!(
                "text-search query doesn't contain lexemes: \"{}\"",
                String::from_utf8_lossy(input)
            )))?;
        }
        let img = build_query_image(mcx, &[], &[])?;
        return Ok(Some(ParsedQuery { img, empty: true }));
    }

    if p.polstr.len() > (MAX_ALLOC_SIZE - HDRSIZETQ - p.op_pool.len()) / QUERYITEM_SIZE {
        let esc2 = p.take_esc();
        ereturn(
            esc2,
            (),
            PgError::error("tsquery is too large").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        )?;
        return Ok(None);
    }

    // lcons builds the list front-to-back reversed relative to push order.
    let mut items: PgVec<Item> = vec_with_capacity_in(mcx, p.polstr.len())?;
    for it in p.polstr.iter().rev() {
        items.push(*it);
    }
    let mut needcleanup = false;
    findoprnd(&mut items, &mut needcleanup)?;

    let img = build_query_image(mcx, &items, &p.op_pool)?;
    if needcleanup {
        let cleaned = crate::cleanup::cleanup_tsquery_stopwords(mcx, &img, noisy)?;
        return Ok(Some(ParsedQuery { img: cleaned, empty: false }));
    }
    Ok(Some(ParsedQuery { img, empty: false }))
}

pub fn pushval_asis(
    p: &mut QueryParser<'_, '_, '_>,
    val: &[u8],
    weight: i16,
    prefix: bool,
) -> PgResult<()> {
    p.push_value(val, weight, prefix)
}
