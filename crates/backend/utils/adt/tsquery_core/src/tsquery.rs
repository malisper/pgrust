//! Port of `src/backend/utils/adt/tsquery.c` — `tsquery` I/O and the shared
//! `parse_tsquery` parser.
//!
//! A `tsquery` value is its flat varlena image. The input scanner works over
//! the input cstring's bytes (NUL excluded): the C `char *buf` scan pointer is
//! a `usize` offset, "end of string" is `off == input.len()`, and
//! `t_iseq(buf, c)` is `input[off] == c`. Multibyte advancement uses the
//! `pg_mblen` seam; the operand values are tokenized by the (unported) stateful
//! `tsvector_parser.c` engine, reached through its owner seam crate.

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use ts_small::cleanup::{clean_NOT, cleanup_tsquery_stopwords};
use ts_small::util::{
    self, encode_record, get_operand, get_query, operand_length, QI_SIZE,
};
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::{
    ereturn, ErrorLocation, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERROR, NOTICE,
};
use stringinfo::StringInfo;
use tsearch::tsearch::{
    QueryItem, QueryOperand, QueryOperator, HDRSIZETQ, MAXENTRYPOS, MAXSTRLEN, MAXSTRPOS, OP_AND,
    OP_NOT, OP_OR, OP_PHRASE, P_TSQ_PLAIN, P_TSQ_WEB, P_TSV_IS_TSQUERY, P_TSV_IS_WEB,
    P_TSV_OPR_IS_DELIM, QI_OPR, QI_VAL, QI_VALSTOP, TsVectorParseStateHandle,
};

use pqformat as pq;
use postgres_seams as tcop;
use ts_locale_seams as locale;
use tsvector_core_seams as tsvparser;
use utils_error::ereport;
use mbutils_seams as mb;

use pgstrcasecmp::pg_strncasecmp;

/// `MaxAllocSize` (`memutils.h`).
const MAX_ALLOC_SIZE: usize = mcx::MAX_ALLOC_SIZE;

/// `tsearch_op_priority[OP_COUNT]` (tsquery.c:29) — FTS operator priorities.
/// Indexed by `OP_* - 1` (the C array is indexed by the operator codes which
/// are 1..=4; `OP_PRIORITY(x)` is `tsearch_op_priority[(x) - 1]`).
const TSEARCH_OP_PRIORITY: [i32; 4] = [
    4, // OP_NOT
    2, // OP_AND
    1, // OP_OR
    3, // OP_PHRASE
];

/// `OP_PRIORITY(x)` (ts_type.h).
#[inline]
fn op_priority(op: i8) -> i32 {
    TSEARCH_OP_PRIORITY[(op - 1) as usize]
}

/// `QO_PRIORITY(x)` (ts_type.h) — priority of an operator `QueryItem`.
#[inline]
fn qo_priority(item: &QueryItem) -> i32 {
    match item {
        QueryItem::Qoperator(o) => op_priority(o.oper),
        _ => 0,
    }
}

// ===========================================================================
// Parser state (struct TSQueryParserStateData, tsquery.c:78)
// ===========================================================================

/// `ts_parserstate` (tsquery.c:40).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ParserState {
    WaitOperand,
    WaitOperator,
    WaitFirstOperand,
}

/// `ts_tokentype` (tsquery.c:50).
#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenType {
    End,
    Err,
    Val,
    Opr,
    Open,
    Close,
}

/// Which tokenizer `parse_tsquery` selected (the C `state->gettoken` fn-ptr).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Tokenizer {
    Standard,
    Websearch,
    Plain,
}

/// The output of a `gettoken` call (the C out-params filled per token type).
#[derive(Default)]
struct Token {
    /// `*operator` (for `PT_OPR`)
    operator: i8,
    /// `*lenval`/`*strval` (for `PT_VAL`); the operand bytes
    strval: Vec<u8>,
    /// `*weight`; doubles as the phrase distance for `OP_PHRASE`
    weight: i16,
    /// `*prefix`
    prefix: bool,
}

/// `struct TSQueryParserStateData` (tsquery.c:78).
struct ParserStateData<'mcx> {
    /// the tokenizer in use (`state->gettoken`)
    gettoken: Tokenizer,
    /// the whole input string we scan (`state->buffer`, NUL excluded)
    buffer: &'mcx [u8],
    /// current scan offset into `buffer` (`state->buf`)
    buf: usize,
    /// nesting count, `(` increments, `)` decrements (`state->count`)
    count: i32,
    /// tokenizer FSM state (`state->state`)
    state: ParserState,
    /// the polish-notation node list, built front-to-back by `lcons` (a
    /// front-insert; modeled as push-to-`Vec` then the list is read in order,
    /// matching C's `foreach` over the `lcons`-built list).
    polstr: Vec<QueryItem>,
    /// operand string accumulator (`state->op`/`state->curop`/`state->sumlen`)
    op: Vec<u8>,
    /// value parser state token (`state->valstate`)
    valstate: TsVectorParseStateHandle,
    /// the per-call mcx (charges the working buffers; the C contexts)
    mcx: Mcx<'mcx>,
}

impl<'mcx> ParserStateData<'mcx> {
    /// `state->sumlen` — used bytes of the operand store.
    #[inline]
    fn sumlen(&self) -> usize {
        self.op.len()
    }
}

// ===========================================================================
// get_modifiers (tsquery.c:113)
// ===========================================================================

/// `get_modifiers(buf, *weight, *prefix)` (tsquery.c:113) — parse a `:AB*`
/// modifier suffix beginning at `buf` within `buffer`. Returns the new scan
/// offset and fills `weight`/`prefix`. (The C `buf` is a pointer; here it is an
/// absolute offset into `buffer`.)
fn get_modifiers(buffer: &[u8], mut off: usize, weight: &mut i16, prefix: &mut bool) -> PgResult<usize> {
    *weight = 0;
    *prefix = false;

    if !t_iseq(buffer, off, b':') {
        return Ok(off);
    }

    off += 1;
    while off < buffer.len() && pg_mblen(buffer, off)? == 1 {
        match buffer[off] {
            b'a' | b'A' => *weight |= 1 << 3,
            b'b' | b'B' => *weight |= 1 << 2,
            b'c' | b'C' => *weight |= 1 << 1,
            b'd' | b'D' => *weight |= 1,
            b'*' => *prefix = true,
            _ => return Ok(off),
        }
        off += 1;
    }

    Ok(off)
}

// ===========================================================================
// parse_phrase_operator (tsquery.c:164)
// ===========================================================================

/// `parse_phrase_operator(pstate, *distance)` (tsquery.c:164) — parse `<N>` /
/// `<->`. Updates `pstate.buf` and fills `distance` on success. Soft errors go
/// to `escontext`.
fn parse_phrase_operator(
    pstate: &mut ParserStateData<'_>,
    escontext: &mut Option<&mut SoftErrorContext>,
    distance: &mut i16,
) -> PgResult<bool> {
    #[derive(PartialEq, Eq)]
    enum PhraseState {
        Open,
        Dist,
        Close,
        Finish,
    }
    let mut state = PhraseState::Open;
    let buffer = pstate.buffer;
    let mut ptr = pstate.buf;
    let mut l: i64 = 1; // default distance

    while ptr < buffer.len() {
        match state {
            PhraseState::Open => {
                if t_iseq(buffer, ptr, b'<') {
                    state = PhraseState::Dist;
                    ptr += 1;
                } else {
                    return Ok(false);
                }
            }
            PhraseState::Dist => {
                if t_iseq(buffer, ptr, b'-') {
                    state = PhraseState::Close;
                    ptr += 1;
                    continue;
                }
                if !buffer[ptr].is_ascii_digit() {
                    return Ok(false);
                }
                // l = strtol(ptr, &endptr, 10);
                let (val, endptr, overflow) = strtol(buffer, ptr);
                if ptr == endptr {
                    return Ok(false);
                } else if overflow || val < 0 || val > MAXENTRYPOS as i64 {
                    return ereturn(
                        escontext.as_deref_mut(),
                        false,
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!(
                                "distance in phrase operator must be an integer value between zero and {} inclusive",
                                MAXENTRYPOS
                            ))
                            .into_error(),
                    );
                } else {
                    l = val;
                    state = PhraseState::Close;
                    ptr = endptr;
                }
            }
            PhraseState::Close => {
                if t_iseq(buffer, ptr, b'>') {
                    state = PhraseState::Finish;
                    ptr += 1;
                } else {
                    return Ok(false);
                }
            }
            PhraseState::Finish => {
                *distance = l as i16;
                pstate.buf = ptr;
                return Ok(true);
            }
        }
    }

    Ok(false)
}

// ===========================================================================
// parse_or_operator (tsquery.c:243)
// ===========================================================================

/// `parse_or_operator(pstate)` (tsquery.c:243) — recognize a websearch `OR`.
fn parse_or_operator(pstate: &mut ParserStateData<'_>) -> PgResult<bool> {
    let buffer = pstate.buffer;
    let mut ptr = pstate.buf;

    // it should begin with "OR" literal
    if pg_strncasecmp(&buffer[ptr..], b"or", 2) != 0 {
        return Ok(false);
    }

    ptr += 2;

    // it shouldn't be a part of any word but somewhere later it should be some
    // operand
    if ptr >= buffer.len() {
        // no operand (*ptr == '\0')
        return Ok(false);
    }

    // it shouldn't be a part of any word
    if t_iseq(buffer, ptr, b'-') || t_iseq(buffer, ptr, b'_') || locale::t_isalnum::call(&buffer[ptr..]) {
        return Ok(false);
    }

    loop {
        ptr += pg_mblen(buffer, ptr)? as usize;

        if ptr >= buffer.len() {
            // got end of string without operand
            return Ok(false);
        }

        // Suppose we found an operand (could be incorrect, but we still treat OR
        // as an operator).
        if !(buffer[ptr] as char).is_whitespace_ascii() {
            break;
        }
    }

    pstate.buf += 2;
    Ok(true)
}

// ===========================================================================
// The three tokenizers (tsquery.c:285/397/509)
// ===========================================================================

/// `gettoken_query_standard` (tsquery.c:285).
fn gettoken_query_standard(
    state: &mut ParserStateData<'_>,
    escontext: &mut Option<&mut SoftErrorContext>,
    tok: &mut Token,
) -> PgResult<TokenType> {
    tok.weight = 0;
    tok.prefix = false;
    let buffer = state.buffer;

    loop {
        match state.state {
            ParserState::WaitFirstOperand | ParserState::WaitOperand => {
                if t_iseq(buffer, state.buf, b'!') {
                    state.buf += 1;
                    state.state = ParserState::WaitOperand;
                    tok.operator = OP_NOT;
                    return Ok(TokenType::Opr);
                } else if t_iseq(buffer, state.buf, b'(') {
                    state.buf += 1;
                    state.state = ParserState::WaitOperand;
                    state.count += 1;
                    return Ok(TokenType::Open);
                } else if t_iseq(buffer, state.buf, b':') {
                    // generic syntax error message is fine
                    return Ok(TokenType::Err);
                } else if not_space(buffer, state.buf) {
                    // We rely on the tsvector parser to parse the value for us.
                    tsvparser::reset_tsvector_parser::call(state.valstate, state.buf);
                    match tsvparser::gettoken_tsvector::call(
                        state.mcx,
                        state.valstate,
                        escontext.as_deref_mut(),
                    )? {
                        Some(t) => {
                            tok.strval = t.strval;
                            state.buf = t.endptr_offset;
                            state.buf = get_modifiers(buffer, state.buf, &mut tok.weight, &mut tok.prefix)?;
                            state.state = ParserState::WaitOperator;
                            return Ok(TokenType::Val);
                        }
                        None => {
                            if soft_error_occurred(escontext) {
                                return Ok(TokenType::Err);
                            } else if state.state == ParserState::WaitFirstOperand {
                                return Ok(TokenType::End);
                            } else {
                                return ereturn(
                                    escontext.as_deref_mut(),
                                    TokenType::Err,
                                    ereport(ERROR)
                                        .errcode(ERRCODE_SYNTAX_ERROR)
                                        .errmsg(format!(
                                            "no operand in tsquery: \"{}\"",
                                            lossy(state.buffer)
                                        ))
                                        .into_error(),
                                );
                            }
                        }
                    }
                }
            }
            ParserState::WaitOperator => {
                if t_iseq(buffer, state.buf, b'&') {
                    state.buf += 1;
                    state.state = ParserState::WaitOperand;
                    tok.operator = OP_AND;
                    return Ok(TokenType::Opr);
                } else if t_iseq(buffer, state.buf, b'|') {
                    state.buf += 1;
                    state.state = ParserState::WaitOperand;
                    tok.operator = OP_OR;
                    return Ok(TokenType::Opr);
                } else if parse_phrase_operator(state, escontext, &mut tok.weight)? {
                    // weight var is used as storage for distance
                    state.state = ParserState::WaitOperand;
                    tok.operator = OP_PHRASE;
                    return Ok(TokenType::Opr);
                } else if soft_error_occurred(escontext) {
                    return Ok(TokenType::Err);
                } else if t_iseq(buffer, state.buf, b')') {
                    state.buf += 1;
                    state.count -= 1;
                    return Ok(if state.count < 0 { TokenType::Err } else { TokenType::Close });
                } else if state.buf >= buffer.len() {
                    return Ok(if state.count != 0 { TokenType::Err } else { TokenType::End });
                } else if !is_space(buffer[state.buf]) {
                    return Ok(TokenType::Err);
                }
            }
        }

        state.buf += pg_mblen(buffer, state.buf)? as usize;
    }
}

/// `gettoken_query_websearch` (tsquery.c:397).
fn gettoken_query_websearch(
    state: &mut ParserStateData<'_>,
    escontext: &mut Option<&mut SoftErrorContext>,
    tok: &mut Token,
) -> PgResult<TokenType> {
    tok.weight = 0;
    tok.prefix = false;
    let buffer = state.buffer;

    loop {
        match state.state {
            ParserState::WaitFirstOperand | ParserState::WaitOperand => {
                if t_iseq(buffer, state.buf, b'-') {
                    state.buf += 1;
                    state.state = ParserState::WaitOperand;
                    tok.operator = OP_NOT;
                    return Ok(TokenType::Opr);
                } else if t_iseq(buffer, state.buf, b'"') {
                    // Everything in quotes is processed as a single token.
                    state.buf += 1; // skip opening quote
                    let start = state.buf;
                    while state.buf < buffer.len() && !t_iseq(buffer, state.buf, b'"') {
                        state.buf += 1;
                    }
                    let lenval = state.buf - start;
                    tok.strval = copy_bytes(&buffer[start..start + lenval]);
                    // skip closing quote if not end of the string
                    if state.buf < buffer.len() {
                        state.buf += 1;
                    }
                    state.state = ParserState::WaitOperator;
                    state.count += 1;
                    return Ok(TokenType::Val);
                } else if isoperator(buffer, state.buf) {
                    // ignore, else gettoken_tsvector() will raise an error
                    state.buf += 1;
                    state.state = ParserState::WaitOperand;
                    continue;
                } else if not_space(buffer, state.buf) {
                    tsvparser::reset_tsvector_parser::call(state.valstate, state.buf);
                    match tsvparser::gettoken_tsvector::call(
                        state.mcx,
                        state.valstate,
                        escontext.as_deref_mut(),
                    )? {
                        Some(t) => {
                            tok.strval = t.strval;
                            state.buf = t.endptr_offset;
                            state.state = ParserState::WaitOperator;
                            return Ok(TokenType::Val);
                        }
                        None => {
                            if soft_error_occurred(escontext) {
                                return Ok(TokenType::Err);
                            } else if state.state == ParserState::WaitFirstOperand {
                                return Ok(TokenType::End);
                            } else {
                                // finally, we have to provide an operand
                                push_stop(state)?;
                                return Ok(TokenType::End);
                            }
                        }
                    }
                }
            }
            ParserState::WaitOperator => {
                if state.buf >= buffer.len() {
                    return Ok(TokenType::End);
                } else if parse_or_operator(state)? {
                    state.state = ParserState::WaitOperand;
                    tok.operator = OP_OR;
                    return Ok(TokenType::Opr);
                } else if isoperator(buffer, state.buf) {
                    // ignore other operators in this state too
                    state.buf += 1;
                    continue;
                } else if !is_space(buffer[state.buf]) {
                    // insert implicit AND between operands
                    state.state = ParserState::WaitOperand;
                    tok.operator = OP_AND;
                    return Ok(TokenType::Opr);
                }
            }
        }

        state.buf += pg_mblen(buffer, state.buf)? as usize;
    }
}

/// `gettoken_query_plain` (tsquery.c:509).
fn gettoken_query_plain(state: &mut ParserStateData<'_>, tok: &mut Token) -> TokenType {
    tok.weight = 0;
    tok.prefix = false;
    let buffer = state.buffer;

    if state.buf >= buffer.len() {
        return TokenType::End;
    }

    // *strval = state->buf; *lenval = strlen(state->buf); state->buf += *lenval;
    tok.strval = copy_bytes(&buffer[state.buf..]);
    state.buf = buffer.len();
    state.count += 1;
    TokenType::Val
}

/// Dispatch to the selected tokenizer (the C `state->gettoken(...)` call).
fn gettoken(
    state: &mut ParserStateData<'_>,
    escontext: &mut Option<&mut SoftErrorContext>,
    tok: &mut Token,
) -> PgResult<TokenType> {
    match state.gettoken {
        Tokenizer::Standard => gettoken_query_standard(state, escontext, tok),
        Tokenizer::Websearch => gettoken_query_websearch(state, escontext, tok),
        Tokenizer::Plain => Ok(gettoken_query_plain(state, tok)),
    }
}

// ===========================================================================
// push helpers (tsquery.c:530..624)
// ===========================================================================

/// `pushOperator(state, oper, distance)` (tsquery.c:530).
fn push_operator(state: &mut ParserStateData<'_>, oper: i8, distance: i16) -> PgResult<()> {
    debug_assert!(oper == OP_NOT || oper == OP_AND || oper == OP_OR || oper == OP_PHRASE);

    let tmp = QueryItem::Qoperator(QueryOperator {
        type_: QI_OPR,
        oper,
        distance: if oper == OP_PHRASE { distance } else { 0 },
        left: 0, // filled in later with findoprnd
    });
    lcons(state, tmp)
}

/// `pushValue_internal(state, valcrc, distance, lenval, weight, prefix)`
/// (tsquery.c:546).
fn push_value_internal(
    state: &mut ParserStateData<'_>,
    escontext: &mut Option<&mut SoftErrorContext>,
    valcrc: u32,
    distance: usize,
    lenval: usize,
    weight: i16,
    prefix: bool,
) -> PgResult<()> {
    if distance >= MAXSTRPOS as usize {
        return ereturn(
            escontext.as_deref_mut(),
            (),
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!("value is too big in tsquery: \"{}\"", lossy(state.buffer)))
                .into_error(),
        );
    }
    if lenval >= MAXSTRLEN as usize {
        return ereturn(
            escontext.as_deref_mut(),
            (),
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!("operand is too long in tsquery: \"{}\"", lossy(state.buffer)))
                .into_error(),
        );
    }

    let mut tmp = QueryOperand {
        type_: QI_VAL,
        weight: weight as u8,
        prefix,
        valcrc: valcrc as i32,
        len_dist: 0,
    };
    tmp.set_length(lenval as u32);
    tmp.set_distance(distance as u32);

    lcons(state, QueryItem::Qoperand(tmp))
}

/// `pushValue(state, strval, lenval, weight, prefix)` (tsquery.c:579).
fn push_value(
    state: &mut ParserStateData<'_>,
    escontext: &mut Option<&mut SoftErrorContext>,
    strval: &[u8],
    lenval: usize,
    weight: i16,
    prefix: bool,
) -> PgResult<()> {
    if lenval >= MAXSTRLEN as usize {
        return ereturn(
            escontext.as_deref_mut(),
            (),
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!("word is too long in tsquery: \"{}\"", lossy(state.buffer)))
                .into_error(),
        );
    }

    // INIT/COMP/FIN_LEGACY_CRC32 over the operand bytes.
    let valcrc = hash_small_seams::legacy_crc32_lexeme::call(&strval[..lenval]);

    let distance = state.sumlen();
    push_value_internal(state, escontext, valcrc, distance, lenval, weight, prefix)?;
    if soft_error_occurred(escontext) {
        return Ok(());
    }

    // append the value string to state.op (+ NUL terminator). The C grows a
    // doubling buffer; the owned Vec grows automatically. sumlen += lenval + 1.
    state.op.try_reserve(lenval + 1).map_err(|_| util::oom())?;
    state.op.extend_from_slice(&strval[..lenval]);
    state.op.push(0u8);
    Ok(())
}

/// `pushStop(state)` (tsquery.c:615).
fn push_stop(state: &mut ParserStateData<'_>) -> PgResult<()> {
    lcons(state, QueryItem::Type_(QI_VALSTOP))
}

/// `lcons(item, state->polstr)` — prepend to the polish list. The C list is
/// built by front-insertion and later read front-to-back; the equivalent
/// linear order is achieved by pushing to the front of a `Vec`.
fn lcons(state: &mut ParserStateData<'_>, item: QueryItem) -> PgResult<()> {
    state.polstr.try_reserve(1).map_err(|_| util::oom())?;
    state.polstr.insert(0, item);
    Ok(())
}

// ===========================================================================
// operator-precedence stack + makepol (tsquery.c:627..723)
// ===========================================================================

/// `STACKDEPTH` (tsquery.c:627).
const STACKDEPTH: usize = 32;

/// `OperatorElement` (tsquery.c:629).
#[derive(Clone, Copy)]
struct OperatorElement {
    op: i8,
    distance: i16,
}

/// `pushOpStack(stack, *lenstack, op, distance)` (tsquery.c:635).
fn push_op_stack(
    stack: &mut [OperatorElement; STACKDEPTH],
    lenstack: &mut usize,
    op: i8,
    distance: i16,
) -> PgResult<()> {
    if *lenstack == STACKDEPTH {
        // internal error
        return Err(PgError::error("tsquery stack too small"));
    }
    stack[*lenstack] = OperatorElement { op, distance };
    *lenstack += 1;
    Ok(())
}

/// `cleanOpStack(state, stack, *lenstack, op)` (tsquery.c:647).
fn clean_op_stack(
    state: &mut ParserStateData<'_>,
    stack: &mut [OperatorElement; STACKDEPTH],
    lenstack: &mut usize,
    op: i8,
) -> PgResult<()> {
    let op_priority_v = op_priority(op);

    while *lenstack != 0 {
        // NOT is right associative unlike to others
        let top = stack[*lenstack - 1];
        if (op != OP_NOT && op_priority_v > op_priority(top.op))
            || (op == OP_NOT && op_priority_v >= op_priority(top.op))
        {
            break;
        }
        *lenstack -= 1;
        push_operator(state, stack[*lenstack].op, stack[*lenstack].distance)?;
    }
    Ok(())
}

/// The parser-stack handle handed to a [`PushvalFn`] callback.
///
/// In C, `pushval_morph` (to_tsany.c) receives the opaque `TSQueryParserState`
/// and drives it with `pushValue` / `pushStop` / `pushOperator`. This struct is
/// the Rust analogue: it wraps the in-flight parser state + soft-error context
/// and exposes exactly those three stack operations, so an out-of-crate
/// `pushval` (the morphology callback) can build the polish-notation node list
/// without seeing `ParserStateData`'s internals.
pub struct QueryBuilder<'a, 'mcx, 'e> {
    state: &'a mut ParserStateData<'mcx>,
    escontext: &'a mut Option<&'e mut SoftErrorContext>,
}

impl QueryBuilder<'_, '_, '_> {
    /// `pushValue(state, strval, lenval, weight, prefix)` (tsquery.c:601).
    pub fn push_value(
        &mut self,
        strval: &[u8],
        lenval: usize,
        weight: i16,
        prefix: bool,
    ) -> PgResult<()> {
        push_value(self.state, self.escontext, strval, lenval, weight, prefix)
    }

    /// `pushStop(state)` (tsquery.c:615).
    pub fn push_stop(&mut self) -> PgResult<()> {
        push_stop(self.state)
    }

    /// `pushOperator(state, oper, distance)` (tsquery.c:584).
    pub fn push_operator(&mut self, oper: i8, distance: i16) -> PgResult<()> {
        push_operator(self.state, oper, distance)
    }
}

/// A `pushval` callback (`PushFunction` in C): invoked by [`makepol`] for each
/// `Val` token. `(builder, strval, lenval, weight, prefix)`. The in-tree
/// default is `pushval_asis` (a single `pushValue`); `pushval_morph`
/// (to_tsany.c) is supplied from out of crate for the `to_tsquery` family.
pub type PushvalFn<'a> =
    dyn FnMut(&mut QueryBuilder<'_, '_, '_>, &[u8], usize, i16, bool) -> PgResult<()> + 'a;

/// `makepol(state, pushval, opaque)` (tsquery.c:671).
///
/// `pushval` is the `PushFunction` callback. For the in-tree `tsqueryin` /
/// web / plain callers it is `pushval_asis` (a direct `pushValue`); the
/// `to_tsquery` family supplies `pushval_morph` (to_tsany.c) over the same
/// [`QueryBuilder`] handle.
fn makepol(
    state: &mut ParserStateData<'_>,
    escontext: &mut Option<&mut SoftErrorContext>,
    pushval: &mut PushvalFn<'_>,
) -> PgResult<()> {
    let mut opstack = [OperatorElement { op: 0, distance: 0 }; STACKDEPTH];
    let mut lenstack = 0usize;

    // since this function recurses, it could be driven to stack overflow
    tcop::check_stack_depth::call()?;

    loop {
        let mut tok = Token::default();
        let ty = gettoken(state, escontext, &mut tok)?;
        if ty == TokenType::End {
            break;
        }
        match ty {
            TokenType::Val => {
                // pushval(opaque, state, strval, lenval, weight, prefix)
                let lenval = tok.strval.len();
                let strval = core::mem::take(&mut tok.strval);
                let mut builder = QueryBuilder { state, escontext };
                pushval(&mut builder, &strval, lenval, tok.weight, tok.prefix)?;
            }
            TokenType::Opr => {
                clean_op_stack(state, &mut opstack, &mut lenstack, tok.operator)?;
                push_op_stack(&mut opstack, &mut lenstack, tok.operator, tok.weight)?;
            }
            TokenType::Open => {
                makepol(state, escontext, pushval)?;
            }
            TokenType::Close => {
                clean_op_stack(state, &mut opstack, &mut lenstack, OP_OR /* lowest */)?;
                return Ok(());
            }
            TokenType::Err | TokenType::End => {
                // don't overwrite a soft error saved by gettoken function
                if !soft_error_occurred(escontext) {
                    errsave(
                        escontext.as_deref_mut(),
                        ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg(format!("syntax error in tsquery: \"{}\"", lossy(state.buffer)))
                            .into_error(),
                    )?;
                }
                return Ok(());
            }
        }
        // detect soft error in pushval or recursion
        if soft_error_occurred(escontext) {
            return Ok(());
        }
    }

    clean_op_stack(state, &mut opstack, &mut lenstack, OP_OR /* lowest */)
}

// ===========================================================================
// findoprnd (tsquery.c:725..794)
// ===========================================================================

/// `findoprnd_recurse(ptr, *pos, nnodes, *needcleanup)` (tsquery.c:725).
fn findoprnd_recurse(
    ptr: &mut [QueryItem],
    pos: &mut u32,
    nnodes: usize,
    needcleanup: &mut bool,
) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    if *pos as usize >= nnodes {
        return Err(PgError::error("malformed tsquery: operand not found"));
    }

    let cur = *pos as usize;
    match ptr[cur].item_type() {
        QI_VAL => {
            *pos += 1;
        }
        QI_VALSTOP => {
            *needcleanup = true; // we'll have to remove stop words
            *pos += 1;
        }
        QI_OPR => {
            let oper = match &ptr[cur] {
                QueryItem::Qoperator(o) => o.oper,
                _ => 0,
            };
            if oper == OP_NOT {
                // ptr[*pos].qoperator.left = 1; (fixed offset)
                if let QueryItem::Qoperator(o) = &mut ptr[cur] {
                    o.left = 1;
                }
                *pos += 1;
                // process the only argument
                findoprnd_recurse(ptr, pos, nnodes, needcleanup)?;
            } else {
                debug_assert!(oper == OP_AND || oper == OP_OR || oper == OP_PHRASE);
                let tmp = *pos; // save current position
                *pos += 1;
                // process RIGHT argument
                findoprnd_recurse(ptr, pos, nnodes, needcleanup)?;
                // curitem->left = *pos - tmp;  (set LEFT arg's offset)
                let left = *pos - tmp;
                if let QueryItem::Qoperator(o) = &mut ptr[cur] {
                    o.left = left;
                }
                // process LEFT argument
                findoprnd_recurse(ptr, pos, nnodes, needcleanup)?;
            }
        }
        other => {
            return Err(PgError::error(format!("unrecognized QueryItem type: {}", other)));
        }
    }
    Ok(())
}

/// `findoprnd(ptr, size, *needcleanup)` (tsquery.c:783).
fn findoprnd(ptr: &mut [QueryItem], size: usize, needcleanup: &mut bool) -> PgResult<()> {
    *needcleanup = false;
    let mut pos = 0u32;
    findoprnd_recurse(ptr, &mut pos, size, needcleanup)?;

    if pos as usize != size {
        return Err(PgError::error("malformed tsquery: extra nodes"));
    }
    Ok(())
}

// ===========================================================================
// parse_tsquery (tsquery.c:816)
// ===========================================================================

/// `parse_tsquery(buf, pushval, opaque, flags, escontext)` (tsquery.c:816).
///
/// `buf` is the input cstring's bytes (NUL excluded). `flags` is the `P_TSQ_*`
/// bitmask. The only `pushval` used in-tree is `pushval_asis`, so it is inlined
/// (see [`makepol`]). Returns the flat `tsquery` image, or `None` on a soft
/// error.
pub fn parse_tsquery(
    mcx: Mcx<'_>,
    buf: &[u8],
    flags: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Vec<u8>>> {
    // `pushval_asis(state, strval, lenval, weight, prefix)` (tsquery.c:806) —
    // the only in-tree `PushFunction`: a single `pushValue`. The `tsqueryin`,
    // plain, and web callers all use it.
    let mut asis = |b: &mut QueryBuilder<'_, '_, '_>,
                    strval: &[u8],
                    lenval: usize,
                    weight: i16,
                    prefix: bool|
     -> PgResult<()> { b.push_value(strval, lenval, weight, prefix) };
    parse_tsquery_with_pushval(mcx, buf, flags, escontext, &mut asis)
}

/// `parse_tsquery(buf, pushval, opaque, flags, escontext)` (tsquery.c:816) with
/// an explicit `pushval` `PushFunction` — the form the `to_tsquery` family
/// (to_tsany.c) uses with `pushval_morph`.
pub fn parse_tsquery_with_pushval(
    mcx: Mcx<'_>,
    buf: &[u8],
    flags: i32,
    mut escontext: Option<&mut SoftErrorContext>,
    pushval: &mut PushvalFn<'_>,
) -> PgResult<Option<Vec<u8>>> {
    // plain should not be used with web
    debug_assert!((flags & (P_TSQ_PLAIN | P_TSQ_WEB)) != (P_TSQ_PLAIN | P_TSQ_WEB));

    let mut tsv_flags = P_TSV_OPR_IS_DELIM | P_TSV_IS_TSQUERY;

    // select suitable tokenizer
    let gettoken = if flags & P_TSQ_PLAIN != 0 {
        Tokenizer::Plain
    } else if flags & P_TSQ_WEB != 0 {
        tsv_flags |= P_TSV_IS_WEB;
        Tokenizer::Websearch
    } else {
        Tokenizer::Standard
    };

    // emit nuisance NOTICEs only if not doing soft errors. C tests `IsA(escontext,
    // ErrorSaveContext)`; here an explicit SoftErrorContext is exactly that.
    let noisy = escontext.is_none();

    // init value parser's state
    let valstate = tsvparser::init_tsvector_parser::call(buf, tsv_flags)?;

    let mut state = ParserStateData {
        gettoken,
        buffer: buf,
        buf: 0,
        count: 0,
        state: ParserState::WaitFirstOperand,
        polstr: Vec::new(),
        op: Vec::new(),
        valstate,
        mcx,
    };

    // parse query & make polish notation (postfix, but in reverse order)
    let res = makepol(&mut state, &mut escontext, pushval);

    tsvparser::close_tsvector_parser::call(state.valstate);

    res?;

    if soft_error_occurred(&escontext) {
        return Ok(None);
    }

    if state.polstr.is_empty() {
        if noisy {
            ereport(NOTICE)
                .errmsg(format!(
                    "text-search query doesn't contain lexemes: \"{}\"",
                    lossy(state.buffer)
                ))
                .finish(ErrorLocation::new("tsquery.c", 878, "parse_tsquery"))?;
        }
        let mut query = try_zeroed(HDRSIZETQ)?;
        set_varsize(&mut query, HDRSIZETQ);
        // query->size = 0 (already zero)
        return Ok(Some(query));
    }

    let nnode = state.polstr.len();
    let sumlen = state.sumlen();
    if tsquery_too_big(nnode, sumlen) {
        return ereturn(
            escontext.as_deref_mut(),
            None,
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("tsquery is too large")
                .into_error(),
        );
    }
    let commonlen = computesize(nnode, sumlen);

    // Pack the QueryItems in the final TSQuery struct.
    let mut query = try_zeroed(commonlen)?;
    set_varsize(&mut query, commonlen);
    set_tsq_size(&mut query, nnode as i32);

    // Decode each polstr item into a flat QueryItem array (ptr = GETQUERY(query)).
    let mut ptr: PgVec<'_, QueryItem> = vec_with_capacity_in(mcx, nnode).map_err(|_| util::oom())?;
    for item in state.polstr.iter() {
        match item {
            QueryItem::Qoperand(o) => ptr.push(QueryItem::Qoperand(*o)),
            QueryItem::Type_(t) if *t == QI_VALSTOP => ptr.push(QueryItem::Type_(QI_VALSTOP)),
            QueryItem::Qoperator(o) => ptr.push(QueryItem::Qoperator(*o)),
            other => {
                return Err(PgError::error(format!(
                    "unrecognized QueryItem type: {}",
                    other.item_type()
                )));
            }
        }
    }

    // Copy all the operand strings to TSQuery (GETOPERAND(query) = state.op).
    let opbase = HDRSIZETQ + nnode * QI_SIZE;
    query[opbase..opbase + sumlen].copy_from_slice(&state.op);

    // Set left operand pointers for every operator; detect QI_VALSTOP nodes.
    let mut needcleanup = false;
    findoprnd(ptr.as_mut_slice(), nnode, &mut needcleanup)?;

    // Encode the (now left-filled) QueryItem array into the flat image.
    for (i, it) in ptr.iter().enumerate() {
        let base = HDRSIZETQ + i * QI_SIZE;
        encode_record(it, &mut query[base..base + QI_SIZE]);
    }

    // If there are QI_VALSTOP nodes, delete them and simplify the tree.
    if needcleanup {
        query = cleanup_tsquery_stopwords(mcx, &query, noisy)?;
    }

    Ok(Some(query))
}

// ===========================================================================
// tsqueryin (tsquery.c:951)
// ===========================================================================

/// `tsqueryin(in)` (tsquery.c:951) — `tsquery` text input. `escontext` carries
/// soft errors; `pushval_asis` is the (inlined) push function.
pub fn tsqueryin(
    mcx: Mcx<'_>,
    in_: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Vec<u8>>> {
    parse_tsquery(mcx, in_, 0, escontext)
}

// ===========================================================================
// infix printer + tsqueryout / tsquerytree (tsquery.c:967..1166, 1361..1398)
// ===========================================================================

/// `INFIX` (tsquery.c:967) — the recursive print cursor.
struct Infix<'a> {
    /// the QueryItem array being printed (`in->curpol` is the cursor index)
    curpol: &'a [QueryItem],
    /// cursor into `curpol`
    cur_idx: usize,
    /// the operand string store (`in->op`)
    op: &'a [u8],
    /// accumulated output (`in->buf`/`in->cur`)
    out: Vec<u8>,
}

/// `infix(in, parentPriority, rightPhraseOp)` (tsquery.c:990).
fn infix(inf: &mut Infix<'_>, parent_priority: i32, right_phrase_op: bool, max_mblen: i32) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    let item = inf.curpol[inf.cur_idx].clone();

    if item.item_type() == QI_VAL {
        let curpol = match &item {
            QueryItem::Qoperand(o) => *o,
            _ => QueryOperand::default(),
        };
        let mut op = curpol.distance() as usize;
        let end = op + curpol.length() as usize;

        inf.out.try_reserve(curpol.length() as usize * (max_mblen as usize + 1) + 2 + 6)
            .map_err(|_| util::oom())?;
        inf.out.push(b'\'');
        while op < end {
            if inf.op[op] == b'\'' {
                inf.out.push(b'\'');
            } else if inf.op[op] == b'\\' {
                inf.out.push(b'\\');
            }
            // ts_copychar_cstr: copy the whole leading multibyte char.
            let clen = pg_mblen(inf.op, op)? as usize;
            inf.out.extend_from_slice(&inf.op[op..op + clen]);
            op += clen;
        }
        inf.out.push(b'\'');
        if curpol.weight != 0 || curpol.prefix {
            inf.out.push(b':');
            if curpol.prefix {
                inf.out.push(b'*');
            }
            if curpol.weight & (1 << 3) != 0 {
                inf.out.push(b'A');
            }
            if curpol.weight & (1 << 2) != 0 {
                inf.out.push(b'B');
            }
            if curpol.weight & (1 << 1) != 0 {
                inf.out.push(b'C');
            }
            if curpol.weight & 1 != 0 {
                inf.out.push(b'D');
            }
        }
        inf.cur_idx += 1;
    } else if oper_of(&item) == OP_NOT {
        let priority = qo_priority(&item);

        if priority < parent_priority {
            inf.out.extend_from_slice(b"( ");
        }
        inf.out.push(b'!');
        inf.cur_idx += 1;

        infix(inf, priority, false, max_mblen)?;
        if priority < parent_priority {
            inf.out.extend_from_slice(b" )");
        }
    } else {
        let op = oper_of(&item);
        let priority = qo_priority(&item);
        let distance = match &item {
            QueryItem::Qoperator(o) => o.distance,
            _ => 0,
        };

        inf.cur_idx += 1;
        let mut need_parenthesis = false;
        if priority < parent_priority || (op == OP_PHRASE && right_phrase_op) {
            need_parenthesis = true;
            inf.out.extend_from_slice(b"( ");
        }

        // get right operand into a fresh sub-buffer (nrm)
        let mut nrm = Infix {
            curpol: inf.curpol,
            cur_idx: inf.cur_idx,
            op: inf.op,
            out: Vec::new(),
        };
        infix(&mut nrm, priority, op == OP_PHRASE, max_mblen)?;

        // get & print left operand
        inf.cur_idx = nrm.cur_idx;
        infix(inf, priority, false, max_mblen)?;

        // print operator & right operand
        match op {
            OP_OR => {
                inf.out.extend_from_slice(b" | ");
                inf.out.extend_from_slice(&nrm.out);
            }
            OP_AND => {
                inf.out.extend_from_slice(b" & ");
                inf.out.extend_from_slice(&nrm.out);
            }
            OP_PHRASE => {
                if distance != 1 {
                    inf.out.extend_from_slice(format!(" <{}> ", distance).as_bytes());
                } else {
                    inf.out.extend_from_slice(b" <-> ");
                }
                inf.out.extend_from_slice(&nrm.out);
            }
            _ => {
                return Err(PgError::error(format!("unrecognized operator type: {}", op)));
            }
        }

        if need_parenthesis {
            inf.out.extend_from_slice(b" )");
        }
    }
    Ok(())
}

/// `tsqueryout(query)` (tsquery.c:1144) — render a `tsquery` to its text form.
/// Returns the bytes (no trailing NUL — the cstring boundary supplies one).
pub fn tsqueryout(mcx: Mcx<'_>, query: &[u8]) -> PgResult<Vec<u8>> {
    if util::tsq_size(query) == 0 {
        return Ok(Vec::new());
    }
    let items = get_query(query)?;
    let max_mblen = mb::pg_database_encoding_max_length::call();
    let mut nrm = Infix {
        curpol: &items,
        cur_idx: 0,
        op: get_operand(query),
        out: Vec::new(),
    };
    let _ = mcx;
    infix(&mut nrm, -1 /* lowest priority */, false, max_mblen)?;
    Ok(nrm.out)
}

/// `tsquerytree(query)` (tsquery.c:1361) — render the tree with all `!`
/// subtrees dropped (a debug view of the index-searchable part). Returns the
/// `text` body bytes.
pub fn tsquerytree(mcx: Mcx<'_>, query: &[u8]) -> PgResult<Vec<u8>> {
    if util::tsq_size(query) == 0 {
        return Ok(Vec::new());
    }

    let items = get_query(query)?;
    // q = clean_NOT(GETQUERY(query), &len);
    let (q, _len) = clean_NOT(mcx, &items)?;

    if q.is_empty() {
        // res = cstring_to_text("T");
        return Ok(b"T".to_vec());
    }

    let max_mblen = mb::pg_database_encoding_max_length::call();
    let mut nrm = Infix {
        curpol: &q,
        cur_idx: 0,
        op: get_operand(query),
        out: Vec::new(),
    };
    infix(&mut nrm, -1, false, max_mblen)?;
    Ok(nrm.out)
}

// ===========================================================================
// Binary I/O: tsquerysend / tsqueryrecv (tsquery.c:1187..1355)
// ===========================================================================

/// `tsquerysend(query)` (tsquery.c:1187) — serialize to the binary wire form.
/// Returns the `bytea` body bytes (the libpq message, no varlena header).
pub fn tsquerysend(mcx: Mcx<'_>, query: &[u8]) -> PgResult<Vec<u8>> {
    let size = util::tsq_size(query);
    let items = get_query(query)?;
    let operand = get_operand(query);

    let mut buf = pq::pq_begintypsend(mcx)?;

    pq::pq_sendint32(&mut buf, size as u32)?;
    for item in &items {
        pq::pq_sendint8(&mut buf, item.item_type() as u8)?;
        match item.item_type() {
            QI_VAL => {
                let o = match item {
                    QueryItem::Qoperand(o) => *o,
                    _ => QueryOperand::default(),
                };
                pq::pq_sendint8(&mut buf, o.weight)?;
                pq::pq_sendint8(&mut buf, o.prefix as u8)?;
                let dist = o.distance() as usize;
                let len = o.length() as usize;
                pq::pq_sendstring(&mut buf, &operand[dist..dist + len])?;
            }
            QI_OPR => {
                let o = match item {
                    QueryItem::Qoperator(o) => *o,
                    _ => QueryOperator::default(),
                };
                pq::pq_sendint8(&mut buf, o.oper as u8)?;
                if o.oper == OP_PHRASE {
                    pq::pq_sendint16(&mut buf, o.distance as u16)?;
                }
            }
            other => {
                return Err(PgError::error(format!("unrecognized tsquery node type: {}", other)));
            }
        }
    }

    let bytea = pq::pq_endtypsend(buf);
    Ok(bytea.as_bytes().to_vec())
}

/// `tsqueryrecv(buf)` (tsquery.c:1225) — deserialize from the binary wire form.
/// Returns the flat `tsquery` image.
pub fn tsqueryrecv(mcx: Mcx<'_>, buf: &mut StringInfo<'_>) -> PgResult<Vec<u8>> {
    let size = pq::pq_getmsgint(buf, core::mem::size_of::<u32>() as i32)?;
    if size as usize > (MAX_ALLOC_SIZE / QI_SIZE) {
        return Err(PgError::error("invalid size of tsquery"));
    }
    let size = size as usize;

    // Allocate space to temporarily hold operand strings (one per QI_VAL).
    let mut operands: PgVec<'_, Vec<u8>> = vec_with_capacity_in(mcx, size).map_err(|_| util::oom())?;

    // Allocate space for all the QueryItems.
    let mut items: PgVec<'_, QueryItem> = vec_with_capacity_in(mcx, size).map_err(|_| util::oom())?;

    let mut datalen: usize = 0;
    for i in 0..size {
        let ty = pq::pq_getmsgint(buf, core::mem::size_of::<i8>() as i32)? as i8;

        if ty == QI_VAL {
            let weight = pq::pq_getmsgint(buf, core::mem::size_of::<u8>() as i32)? as u8;
            let prefix = pq::pq_getmsgint(buf, core::mem::size_of::<u8>() as i32)? as u8;
            let val = pq::pq_getmsgstring(mcx, buf)?;
            let val_bytes = val.as_bytes().to_vec();
            let val_len = val_bytes.len();

            // Sanity checks.
            if weight > 0xF {
                return Err(PgError::error("invalid tsquery: invalid weight bitmap"));
            }
            if val_len > MAXSTRLEN as usize {
                return Err(PgError::error("invalid tsquery: operand too long"));
            }
            if datalen > MAXSTRPOS as usize {
                return Err(PgError::error("invalid tsquery: total operand length exceeded"));
            }

            let valcrc = hash_small_seams::legacy_crc32_lexeme::call(&val_bytes);

            let mut o = QueryOperand {
                type_: QI_VAL,
                weight,
                prefix: prefix != 0,
                valcrc: valcrc as i32,
                len_dist: 0,
            };
            o.set_length(val_len as u32);
            o.set_distance(datalen as u32);
            items.push(QueryItem::Qoperand(o));
            // record the operand bytes for the post-loop copy (index i)
            while operands.len() < i {
                operands.push(Vec::new());
            }
            operands.push(val_bytes);

            datalen += val_len + 1; // + 1 for the '\0' terminator
        } else if ty == QI_OPR {
            let oper = pq::pq_getmsgint(buf, core::mem::size_of::<i8>() as i32)? as i8;
            if oper != OP_NOT && oper != OP_OR && oper != OP_AND && oper != OP_PHRASE {
                return Err(PgError::error(format!(
                    "invalid tsquery: unrecognized operator type {}",
                    oper as i32
                )));
            }
            if i == size - 1 {
                return Err(PgError::error("invalid pointer to right operand"));
            }
            let mut o = QueryOperator {
                type_: QI_OPR,
                oper,
                distance: 0,
                left: 0,
            };
            if oper == OP_PHRASE {
                o.distance = pq::pq_getmsgint(buf, core::mem::size_of::<i16>() as i32)? as i16;
            }
            items.push(QueryItem::Qoperator(o));
            while operands.len() <= i {
                operands.push(Vec::new());
            }
        } else {
            return Err(PgError::error(format!("unrecognized tsquery node type: {}", ty)));
        }
    }

    // Fill in the left-pointers; checks the tree is well-formed.
    let mut needcleanup = false;
    findoprnd(items.as_mut_slice(), size, &mut needcleanup)?;
    debug_assert!(!needcleanup); // Can't have found any QI_VALSTOP nodes

    // Build the final image: HDRSIZETQ + size QueryItems + datalen operand bytes.
    let total = HDRSIZETQ + size * QI_SIZE + datalen;
    let mut query = try_zeroed(total)?;
    set_varsize(&mut query, total);
    set_tsq_size(&mut query, size as i32);

    for (i, it) in items.iter().enumerate() {
        let base = HDRSIZETQ + i * QI_SIZE;
        encode_record(it, &mut query[base..base + QI_SIZE]);
    }

    // Copy operands to output struct (only for QI_VAL nodes, in order).
    let opbase = HDRSIZETQ + size * QI_SIZE;
    let mut ptr = 0usize;
    for (i, it) in items.iter().enumerate() {
        if it.item_type() == QI_VAL {
            let len = operand_length(it) as usize;
            query[opbase + ptr..opbase + ptr + len].copy_from_slice(&operands[i]);
            query[opbase + ptr + len] = 0;
            ptr += len + 1;
        }
    }
    debug_assert_eq!(ptr, datalen);

    Ok(query)
}

// ===========================================================================
// Small byte/scan helpers
// ===========================================================================

/// `t_iseq(buffer + off, c)` (ts_locale.h) — `*ptr == c` (single-byte ASCII).
/// `false` at end-of-string (the C NUL would not equal a non-NUL `c`; callers
/// never compare against `'\0'` here).
#[inline]
fn t_iseq(buffer: &[u8], off: usize, c: u8) -> bool {
    off < buffer.len() && buffer[off] == c
}

/// C `!isspace((unsigned char) *state->buf)` — true for any non-space byte,
/// INCLUDING the terminating NUL at end-of-string (`isspace('\0') == 0`), so
/// the WAITOPERAND value branch is entered at EOS (where `gettoken_tsvector`
/// then reports end-of-input). Mirrors the C predicate exactly.
#[inline]
fn not_space(buffer: &[u8], off: usize) -> bool {
    off >= buffer.len() || !is_space(buffer[off])
}

/// C `isspace((unsigned char) c)` for the "C" locale (the bytes tsquery scans).
#[inline]
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// `ISOPERATOR(x)` (ts_utils.h).
#[inline]
fn isoperator(buffer: &[u8], off: usize) -> bool {
    if off >= buffer.len() {
        return false;
    }
    matches!(buffer[off], b'!' | b'&' | b'|' | b'(' | b')' | b'<')
}

/// `pg_mblen_cstr(buffer + off)` — the leading multibyte character's length.
#[inline]
fn pg_mblen(buffer: &[u8], off: usize) -> PgResult<i32> {
    mb::pg_mblen_range::call(&buffer[off..])
}

/// `in->curpol->qoperator.oper`, for an operator `QueryItem`.
#[inline]
fn oper_of(item: &QueryItem) -> i8 {
    match item {
        QueryItem::Qoperator(o) => o.oper,
        _ => 0,
    }
}

/// `SOFT_ERROR_OCCURRED(escontext)`.
#[inline]
fn soft_error_occurred(escontext: &Option<&mut SoftErrorContext>) -> bool {
    escontext.as_ref().is_some_and(|c| c.error_occurred())
}

/// C `errsave(escontext, …)` for a hard or soft error with no value.
#[inline]
fn errsave(escontext: Option<&mut SoftErrorContext>, error: PgError) -> PgResult<()> {
    ereturn(escontext, (), error)
}

/// `strtol(buffer + off, &endptr, 10)` for a non-negative decimal — returns
/// `(value, endptr_offset, overflow)`. C `errno == ERANGE` becomes `overflow`.
fn strtol(buffer: &[u8], off: usize) -> (i64, usize, bool) {
    let mut i = off;
    // C strtol skips leading whitespace, but the caller already verified the
    // first byte is a digit, so there is none to skip.
    let mut val: i64 = 0;
    let mut overflow = false;
    while i < buffer.len() && buffer[i].is_ascii_digit() {
        let d = (buffer[i] - b'0') as i64;
        match val.checked_mul(10).and_then(|v| v.checked_add(d)) {
            Some(v) => val = v,
            None => overflow = true,
        }
        i += 1;
    }
    (val, i, overflow)
}

/// Lossy UTF-8 view of the input buffer for error messages (the C `%s` of the
/// raw cstring).
fn lossy(buffer: &[u8]) -> alloc::string::String {
    alloc::string::String::from_utf8_lossy(buffer).into_owned()
}

/// A fresh zero-filled owned result buffer of `len` bytes (palloc0 analog).
fn try_zeroed(len: usize) -> PgResult<Vec<u8>> {
    let mut v: Vec<u8> = Vec::new();
    v.try_reserve(len).map_err(|_| util::oom())?;
    v.resize(len, 0u8);
    Ok(v)
}

/// `SET_VARSIZE(q, len)` — stamp the 4-byte varlena length word.
#[inline]
fn set_varsize(q: &mut [u8], len: usize) {
    q[0..4].copy_from_slice(&((len as u32) << 2).to_ne_bytes());
}

/// `q->size = n` — the `int32` after the varlena header.
#[inline]
fn set_tsq_size(q: &mut [u8], n: i32) {
    q[4..8].copy_from_slice(&n.to_ne_bytes());
}

/// `COMPUTESIZE(size, lenofoperand)` (ts_type.h).
#[inline]
fn computesize(nnode: usize, sumlen: usize) -> usize {
    HDRSIZETQ + nnode * QI_SIZE + sumlen
}

/// `TSQUERY_TOO_BIG(size, lenofoperand)` (ts_type.h).
#[inline]
fn tsquery_too_big(nnode: usize, sumlen: usize) -> bool {
    nnode > (MAX_ALLOC_SIZE - HDRSIZETQ - sumlen) / QI_SIZE
}

/// Copy `src` into a fresh owned `Vec`.
fn copy_bytes(src: &[u8]) -> Vec<u8> {
    src.to_vec()
}

/// ASCII whitespace test, char form (used in [`parse_or_operator`]).
trait AsciiWhitespaceExt {
    fn is_whitespace_ascii(self) -> bool;
}
impl AsciiWhitespaceExt for char {
    #[inline]
    fn is_whitespace_ascii(self) -> bool {
        is_space(self as u8)
    }
}
