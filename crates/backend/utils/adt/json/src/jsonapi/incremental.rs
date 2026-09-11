//! C: src/common/jsonapi.c — the incremental JSON parser.
//!
//! `makeJsonLexContextIncremental` (jsonapi.c:498), the table-driven
//! non-recursive `pg_parse_json_incremental` (jsonapi.c:869) with its
//! prediction / field-name / null-indicator stacks (`JsonParserStack`,
//! jsonapi.c:141), and the partial-token accumulator (`JsonIncrementalState`,
//! jsonapi.c:159) that `json_lex` / `json_lex_string` / `json_lex_number`
//! consult when a chunk boundary falls inside a token. The grammar tables
//! (`JSON_PROD_*`, `td_parser_table`, jsonapi.c:186-260) are verbatim.
//!
//! Shape: C keeps one `JsonLexContext` whose `input` is re-pointed at every
//! chunk. Here the state that outlives a chunk ([`JsonLexIncremental`]: the
//! three stacks, the partial token, the pending scalar, line number and
//! nesting level) is separate from the per-chunk lexer
//! ([`JsonIncrementalChunk`], a [`JsonLexDe`] over the chunk plus a borrow of
//! the persistent state), so the chunk slice never has to outlive the call.
//! Persistent buffers are plain vectors — C's `StringInfo` / `REALLOC`'d
//! stacks, not per-token pallocs — reused across chunks; the per-token
//! scratch (de-escaped strings, server-encoding conversion) lives in the
//! chunk's memory context and is bulk-freed with it. Semantic-action
//! payloads borrow the persistent buffers (C: `STRDUP`'d `fnames[]` /
//! `scalar_val` handed to the callbacks), so a sink must be
//! `for<'a> JsonSem<'a>` — it copies what it wants to keep, as C sinks own
//! the tokens they are handed.

use mcx::Mcx;
use types_error::PgResult;

use super::{
    errdetail_for, is_alnum, JsonError, JsonLexDe, JsonSem, JsonSemToken, JsonToken, ParseCtx,
};

// C: enum JsonNonTerminal (jsonapi.c:119).
const JSON_NT_JSON: u8 = 32;
const JSON_NT_ARRAY_ELEMENTS: u8 = 33;
const JSON_NT_MORE_ARRAY_ELEMENTS: u8 = 34;
const JSON_NT_KEY_PAIRS: u8 = 35;
const JSON_NT_MORE_KEY_PAIRS: u8 = 36;

// C: enum JsonParserSem (jsonapi.c:127).
const JSON_SEM_OSTART: u8 = 64;
const JSON_SEM_OEND: u8 = 65;
const JSON_SEM_ASTART: u8 = 66;
const JSON_SEM_AEND: u8 = 67;
const JSON_SEM_OFIELD_INIT: u8 = 68;
const JSON_SEM_OFIELD_START: u8 = 69;
const JSON_SEM_OFIELD_END: u8 = 70;
const JSON_SEM_AELEM_START: u8 = 71;
const JSON_SEM_AELEM_END: u8 = 72;
const JSON_SEM_SCALAR_INIT: u8 = 73;
const JSON_SEM_SCALAR_CALL: u8 = 74;

// C: JsonTokenType values as they appear on the prediction stack.
const T_STRING: u8 = JsonToken::String as u8;
const T_NUMBER: u8 = JsonToken::Number as u8;
const T_OBJECT_START: u8 = JsonToken::ObjectStart as u8;
const T_OBJECT_END: u8 = JsonToken::ObjectEnd as u8;
const T_ARRAY_START: u8 = JsonToken::ArrayStart as u8;
const T_ARRAY_END: u8 = JsonToken::ArrayEnd as u8;
const T_COMMA: u8 = JsonToken::Comma as u8;
const T_COLON: u8 = JsonToken::Colon as u8;
const T_TRUE: u8 = JsonToken::True as u8;
const T_FALSE: u8 = JsonToken::False as u8;
const T_NULL: u8 = JsonToken::Null as u8;
const T_END: u8 = JsonToken::End as u8;

// C: IS_SEM / IS_NT (jsonapi.c:176).
#[inline]
fn is_sem(x: u8) -> bool {
    x & 0x40 != 0
}
#[inline]
fn is_nt(x: u8) -> bool {
    x & 0x20 != 0
}

/// C: JSON_TD_MAX_STACK (jsonapi.c:432) — "hard coded for now - this is a
/// REALLY high number".
pub const JSON_TD_MAX_STACK: i32 = 6400;

/*
 * C: the productions (jsonapi.c:186-224), stored in reverse order right to
 * left so that when they are pushed on the stack what we expect next is at
 * the top of the stack.
 */
/// epsilon - an empty production
const JSON_PROD_EPSILON: &[u8] = &[];
/// JSON -> string
const JSON_PROD_SCALAR_STRING: &[u8] = &[JSON_SEM_SCALAR_CALL, T_STRING, JSON_SEM_SCALAR_INIT];
/// JSON -> number
const JSON_PROD_SCALAR_NUMBER: &[u8] = &[JSON_SEM_SCALAR_CALL, T_NUMBER, JSON_SEM_SCALAR_INIT];
/// JSON -> 'true'
const JSON_PROD_SCALAR_TRUE: &[u8] = &[JSON_SEM_SCALAR_CALL, T_TRUE, JSON_SEM_SCALAR_INIT];
/// JSON -> 'false'
const JSON_PROD_SCALAR_FALSE: &[u8] = &[JSON_SEM_SCALAR_CALL, T_FALSE, JSON_SEM_SCALAR_INIT];
/// JSON -> 'null'
const JSON_PROD_SCALAR_NULL: &[u8] = &[JSON_SEM_SCALAR_CALL, T_NULL, JSON_SEM_SCALAR_INIT];
/// JSON -> '{' KEY_PAIRS '}'
const JSON_PROD_OBJECT: &[u8] =
    &[JSON_SEM_OEND, T_OBJECT_END, JSON_NT_KEY_PAIRS, T_OBJECT_START, JSON_SEM_OSTART];
/// JSON -> '[' ARRAY_ELEMENTS ']'
const JSON_PROD_ARRAY: &[u8] =
    &[JSON_SEM_AEND, T_ARRAY_END, JSON_NT_ARRAY_ELEMENTS, T_ARRAY_START, JSON_SEM_ASTART];
/// ARRAY_ELEMENTS -> JSON MORE_ARRAY_ELEMENTS
const JSON_PROD_ARRAY_ELEMENTS: &[u8] =
    &[JSON_NT_MORE_ARRAY_ELEMENTS, JSON_SEM_AELEM_END, JSON_NT_JSON, JSON_SEM_AELEM_START];
/// MORE_ARRAY_ELEMENTS -> ',' JSON MORE_ARRAY_ELEMENTS
const JSON_PROD_MORE_ARRAY_ELEMENTS: &[u8] = &[
    JSON_NT_MORE_ARRAY_ELEMENTS,
    JSON_SEM_AELEM_END,
    JSON_NT_JSON,
    JSON_SEM_AELEM_START,
    T_COMMA,
];
/// KEY_PAIRS -> string ':' JSON MORE_KEY_PAIRS
const JSON_PROD_KEY_PAIRS: &[u8] = &[
    JSON_NT_MORE_KEY_PAIRS,
    JSON_SEM_OFIELD_END,
    JSON_NT_JSON,
    JSON_SEM_OFIELD_START,
    T_COLON,
    T_STRING,
    JSON_SEM_OFIELD_INIT,
];
/// MORE_KEY_PAIRS -> ',' string ':'  JSON MORE_KEY_PAIRS
const JSON_PROD_MORE_KEY_PAIRS: &[u8] = &[
    JSON_NT_MORE_KEY_PAIRS,
    JSON_SEM_OFIELD_END,
    JSON_NT_JSON,
    JSON_SEM_OFIELD_START,
    T_COLON,
    T_STRING,
    JSON_SEM_OFIELD_INIT,
    T_COMMA,
];
/// the GOAL production. Not stored in the table, but will be the initial
/// contents of the prediction stack
const JSON_PROD_GOAL: &[u8] = &[T_END, JSON_NT_JSON];

/// C: td_parser_table (jsonapi.c:234) — the productions with their director
/// sets of terminal symbols. Any combination not specified here represents
/// an error (C: a NULL `prod`).
fn td_parser_table(nt: u8, tok: JsonToken) -> Option<&'static [u8]> {
    use JsonToken as T;
    match (nt, tok) {
        /* JSON */
        (JSON_NT_JSON, T::String) => Some(JSON_PROD_SCALAR_STRING),
        (JSON_NT_JSON, T::Number) => Some(JSON_PROD_SCALAR_NUMBER),
        (JSON_NT_JSON, T::True) => Some(JSON_PROD_SCALAR_TRUE),
        (JSON_NT_JSON, T::False) => Some(JSON_PROD_SCALAR_FALSE),
        (JSON_NT_JSON, T::Null) => Some(JSON_PROD_SCALAR_NULL),
        (JSON_NT_JSON, T::ArrayStart) => Some(JSON_PROD_ARRAY),
        (JSON_NT_JSON, T::ObjectStart) => Some(JSON_PROD_OBJECT),
        /* ARRAY_ELEMENTS */
        (
            JSON_NT_ARRAY_ELEMENTS,
            T::ArrayStart | T::ObjectStart | T::String | T::Number | T::True | T::False | T::Null,
        ) => Some(JSON_PROD_ARRAY_ELEMENTS),
        (JSON_NT_ARRAY_ELEMENTS, T::ArrayEnd) => Some(JSON_PROD_EPSILON),
        /* MORE_ARRAY_ELEMENTS */
        (JSON_NT_MORE_ARRAY_ELEMENTS, T::Comma) => Some(JSON_PROD_MORE_ARRAY_ELEMENTS),
        (JSON_NT_MORE_ARRAY_ELEMENTS, T::ArrayEnd) => Some(JSON_PROD_EPSILON),
        /* KEY_PAIRS */
        (JSON_NT_KEY_PAIRS, T::String) => Some(JSON_PROD_KEY_PAIRS),
        (JSON_NT_KEY_PAIRS, T::ObjectEnd) => Some(JSON_PROD_EPSILON),
        /* MORE_KEY_PAIRS */
        (JSON_NT_MORE_KEY_PAIRS, T::Comma) => Some(JSON_PROD_MORE_KEY_PAIRS),
        (JSON_NT_MORE_KEY_PAIRS, T::ObjectEnd) => Some(JSON_PROD_EPSILON),
        _ => None,
    }
}

/// C: the incremental parts of `JsonLexContext` — `JsonParserStack`
/// (jsonapi.c:141) + `JsonIncrementalState` (jsonapi.c:159) + the fields
/// that carry over between chunks (`line_number`, `lex_level`).
///
/// Built by [`JsonLexIncremental::new`] (C: makeJsonLexContextIncremental);
/// fed one chunk at a time through [`pg_parse_json_incremental`] or
/// [`JsonLexIncremental::chunk`].
pub struct JsonLexIncremental {
    encoding: i32,
    need_escapes: bool,
    /// C: lex->line_number (persists across chunks).
    line_number: i32,
    /// C: lex->lex_level.
    lex_level: i32,

    /* C: JsonParserStack */
    /// C: pstack->prediction / pred_index.
    prediction: Vec<u8>,
    /// C: pstack->fnames, indexed by lex_level. An empty entry is C's NULL
    /// (no field name recorded at that level).
    fnames: Vec<Vec<u8>>,
    /// C: pstack->fnull, indexed by lex_level.
    fnull: Vec<bool>,
    /// C: pstack->scalar_tok.
    scalar_tok: JsonToken,
    /// C: pstack->scalar_val — the scalar preserved between
    /// JSON_SEM_SCALAR_INIT and JSON_SEM_SCALAR_CALL (which may land in
    /// different chunks). `None` is C's NULL (a string with
    /// need_escapes=false).
    scalar_val: Option<Vec<u8>>,

    /* C: JsonIncrementalState */
    /// C: inc_state->started.
    started: bool,
    /// C: inc_state->is_last_chunk.
    is_last_chunk: bool,
    /// C: inc_state->partial_completed.
    partial_completed: bool,
    /// C: inc_state->partial_token.
    partial_token: Vec<u8>,
}

/// C: JS_STACK_CHUNK_SIZE — the initial stack depth (jsonapi.c:430).
const JS_STACK_CHUNK_SIZE: usize = 64;

impl JsonLexIncremental {
    // C keeps partial_token in a StringInfo: enlargeStringInfo's MaxAllocSize
    // ceiling and its ERROR apply, never an infallible growth.
    fn admit(&mut self, more: usize) -> PgResult<()> {
        let len = self.partial_token.len();
        if len.saturating_add(more) > mcx::MAX_ALLOC_SIZE {
            return Err(types_error::PgError::error(format!(
                "string buffer exceeds maximum allowed length ({} bytes)",
                mcx::MAX_ALLOC_SIZE
            ))
            .with_sqlstate(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .with_detail(format!(
                "Cannot enlarge string buffer containing {len} bytes by {more} more bytes."
            ))
            .into());
        }
        self.partial_token
            .try_reserve(more)
            .map_err(|_| mcx::oom_named("JSON incremental lexer", more).into())
    }

    fn stash(&mut self, bytes: &[u8]) -> PgResult<()> {
        self.admit(bytes.len())?;
        self.partial_token.extend_from_slice(bytes);
        Ok(())
    }

    /// C: makeJsonLexContextIncremental(lex, encoding, need_escapes) +
    /// allocate_incremental_state.
    pub fn new(encoding: i32, need_escapes: bool) -> Self {
        let mut fnames = Vec::with_capacity(JS_STACK_CHUNK_SIZE);
        // fnames between 0 and lex_level must always be defined; level 0 is
        // NULL from the start (C: lex->pstack->fnames[0] = NULL).
        fnames.push(Vec::new());
        let mut fnull = Vec::with_capacity(JS_STACK_CHUNK_SIZE);
        fnull.push(false);
        JsonLexIncremental {
            encoding,
            need_escapes,
            line_number: 1,
            lex_level: 0,
            prediction: Vec::with_capacity(JS_STACK_CHUNK_SIZE * 10),
            fnames,
            fnull,
            scalar_tok: JsonToken::Invalid,
            scalar_val: None,
            started: false,
            is_last_chunk: false,
            partial_completed: false,
            partial_token: Vec::new(),
        }
    }

    /// C: lex->inc_state->started — whether any chunk has been parsed.
    pub fn started(&self) -> bool {
        self.started
    }

    /// C: lex->lex_level.
    pub fn lex_level(&self) -> i32 {
        self.lex_level
    }

    /// Begin parsing `chunk` (C: pg_parse_json_incremental's re-pointing of
    /// `lex->input` at the chunk; jsonapi.c:887). `mcx` backs the per-chunk
    /// token scratch. Call [`JsonIncrementalChunk::parse`] on the result;
    /// keep it around for [`JsonIncrementalChunk::errdetail`] on failure.
    pub fn chunk<'st, 'src, 'mcx>(
        &'st mut self,
        mcx: Mcx<'mcx>,
        chunk: &'src [u8],
        is_last: bool,
    ) -> JsonIncrementalChunk<'st, 'src, 'mcx> {
        let mut lex = JsonLexDe::with_escapes(mcx, chunk, self.encoding, self.need_escapes);
        lex.lex.line_number = self.line_number;
        lex.lex.lex_level = self.lex_level;
        self.is_last_chunk = is_last;
        self.started = true;
        JsonIncrementalChunk {
            st: self,
            lex,
            token_in_partial: false,
            ptok_start: 0,
            ptok_end: 0,
        }
    }

    // C: inc_lex_level (jsonapi.c:562) — grow the stacks on demand and make
    // sure no stale fname sits at the new level.
    fn inc_lex_level(&mut self) {
        self.lex_level += 1;
        let level = self.lex_level as usize;
        if self.fnames.len() <= level {
            self.fnames.resize_with(level + 1, Vec::new);
            self.fnull.resize(level + 1, false);
        }
        // Ensure freeJsonLexContext() remains safe even if no fname is
        // assigned at this level.
        self.fnames[level].clear();
    }

    // C: dec_lex_level (jsonapi.c:614).
    fn dec_lex_level(&mut self) {
        self.set_fname(&[]); /* free the current level's fname, if needed */
        self.lex_level -= 1;
    }

    // C: push_prediction (jsonapi.c:621).
    fn push_prediction(&mut self, entry: &[u8]) {
        self.prediction.extend_from_slice(entry);
    }

    // C: pop_prediction (jsonapi.c:628).
    fn pop_prediction(&mut self) -> u8 {
        self.prediction.pop().expect("pred_index > 0")
    }

    // C: next_prediction (jsonapi.c:635).
    fn next_prediction(&self) -> u8 {
        *self.prediction.last().expect("pred_index > 0")
    }

    // C: have_prediction (jsonapi.c:642).
    fn have_prediction(&self) -> bool {
        !self.prediction.is_empty()
    }

    // C: set_fname (jsonapi.c:648). An empty slice is C's NULL.
    fn set_fname(&mut self, fname: &[u8]) {
        let slot = &mut self.fnames[self.lex_level as usize];
        slot.clear();
        slot.extend_from_slice(fname);
    }

    // C: get_fname (jsonapi.c:663).
    fn get_fname(&self) -> &[u8] {
        &self.fnames[self.lex_level as usize]
    }

    // C: set_fnull (jsonapi.c:669).
    fn set_fnull(&mut self, fnull: bool) {
        self.fnull[self.lex_level as usize] = fnull;
    }

    // C: get_fnull (jsonapi.c:675).
    fn get_fnull(&self) -> bool {
        self.fnull[self.lex_level as usize]
    }
}

/// One chunk being parsed: C's `JsonLexContext` with `input` pointing at the
/// chunk, borrowing the cross-chunk state.
pub struct JsonIncrementalChunk<'st, 'src, 'mcx> {
    st: &'st mut JsonLexIncremental,
    /// The lexer over the chunk (C: lex->input .. lex->input_length).
    lex: JsonLexDe<'src, 'mcx>,
    /// True while the current token is the completed partial token (C:
    /// lex->token_start/token_terminator pointing into
    /// inc_state->partial_token). `ptok_start..ptok_end` locate it there.
    token_in_partial: bool,
    ptok_start: usize,
    ptok_end: usize,
}

impl<'src, 'mcx> JsonIncrementalChunk<'_, 'src, 'mcx> {
    /// The lexer over the chunk — hook implementations receive `&self.lex.lex`.
    pub fn lex(&self) -> &JsonLexDe<'src, 'mcx> {
        &self.lex
    }

    /// C: json_errdetail(error, lex) for the state this chunk left behind.
    /// When the current token was assembled in the partial-token buffer the
    /// text comes from there (C: lex->token_start = ptok->data).
    pub fn errdetail(&self, error: JsonError) -> String {
        if self.token_in_partial {
            let ptok = &self.st.partial_token;
            errdetail_for(
                error,
                &ptok[self.ptok_start..self.ptok_end],
                ptok.get(self.ptok_end).copied(),
            )
        } else {
            self.lex.lex.errdetail(error)
        }
    }

    /// The bytes of the current token (C: lex->token_start ..
    /// lex->token_terminator, wherever they live).
    fn current_token_bytes(&self) -> &[u8] {
        if self.token_in_partial {
            &self.st.partial_token[self.ptok_start..self.ptok_end]
        } else {
            let start = self.lex.lex.token_start.unwrap_or(self.lex.lex.token_terminator);
            &self.lex.lex.input[start..self.lex.lex.token_terminator]
        }
    }

    /// C: json_lex (jsonapi.c:1589), the incremental lexer: completes a
    /// partial token left by the previous chunk, or lexes the next token of
    /// this chunk and stashes it as a partial token when the chunk ends
    /// inside it (JSON_INCOMPLETE).
    fn json_lex(&mut self) -> PgResult<JsonError> {
        if self.st.partial_completed {
            /*
             * We just lexed a completed partial token on the last call, so
             * reset everything
             */
            self.st.partial_token.clear();
            self.lex.lex.token_terminator = 0; /* = lex->input, already advanced */
            self.token_in_partial = false;
            self.st.partial_completed = false;
        }

        if !self.st.partial_token.is_empty() {
            /*
             * We have a partial token. Extend it and if completed lex it by
             * a recursive call
             */
            return self.json_lex_partial();
        }

        let input = self.lex.lex.input;
        let end = input.len();
        let is_last = self.st.is_last_chunk;
        let mut s = self.lex.lex.token_terminator;
        self.lex.lex.prev_token_terminator = self.lex.lex.token_terminator;

        /* Skip leading whitespace. */
        while s < end && matches!(input[s], b' ' | b'\t' | b'\n' | b'\r') {
            let c = input[s];
            s += 1;
            if c == b'\n' {
                self.lex.lex.line_number += 1;
                self.lex.lex.line_start = s;
            }
        }
        self.lex.lex.token_start = Some(s);

        /* Determine token type. */
        if s >= end {
            self.lex.lex.token_start = None;
            self.lex.lex.token_terminator = s;
            self.lex.lex.token_type = JsonToken::End;
        } else {
            match input[s] {
                /* Single-character token, some kind of punctuation mark. */
                b'{' => {
                    self.lex.lex.single(s, JsonToken::ObjectStart);
                }
                b'}' => {
                    self.lex.lex.single(s, JsonToken::ObjectEnd);
                }
                b'[' => {
                    self.lex.lex.single(s, JsonToken::ArrayStart);
                }
                b']' => {
                    self.lex.lex.single(s, JsonToken::ArrayEnd);
                }
                b',' => {
                    self.lex.lex.single(s, JsonToken::Comma);
                }
                b':' => {
                    self.lex.lex.single(s, JsonToken::Colon);
                }
                b'"' => {
                    /* string */
                    let r = if self.lex.need_escapes {
                        self.lex.lex_string_de()?
                    } else {
                        self.lex.lex.lex_string()
                    };
                    // C: FAIL_OR_INCOMPLETE_AT_CHAR_START (json_lex_string,
                    // jsonapi.c:2023) — the chunk ended inside the string:
                    // stash it and ask for more.
                    if r == JsonError::InvalidToken
                        && self.lex.lex.token_terminator >= end
                        && !is_last
                    {
                        let start = self.lex.lex.token_start.expect("string token start");
                        self.st.stash(&input[start..end])?;
                        return Ok(JsonError::Incomplete);
                    }
                    if r != JsonError::Success {
                        return Ok(r);
                    }
                    self.lex.lex.token_type = JsonToken::String;
                }
                b'-' | b'0'..=b'9' => {
                    /* Negative number / Positive number. */
                    let first = if input[s] == b'-' { s + 1 } else { s };
                    let r = self.lex.lex.lex_number(first);
                    // C: json_lex_number (jsonapi.c:2387) — the number ran to
                    // the end of the chunk: stash it, error flag and all.
                    if !is_last && self.lex.lex.token_terminator >= end {
                        self.st.stash(&input[s..self.lex.lex.token_terminator])?;
                        return Ok(JsonError::Incomplete);
                    }
                    if r != JsonError::Success {
                        return Ok(r);
                    }
                    self.lex.lex.token_type = JsonToken::Number;
                }
                _ => {
                    /*
                     * We're not dealing with a string, number, legal
                     * punctuation mark, or end of string.  The only legal
                     * tokens we might find here are true, false, and null,
                     * but for error reporting purposes we scan until we see a
                     * non-alphanumeric character.  That way, we can report
                     * the whole word as an unexpected token, rather than just
                     * some unintuitive prefix thereof.
                     */
                    let mut p = s;
                    while p < end && is_alnum(input[p]) {
                        p += 1;
                    }

                    /*
                     * We got some sort of unexpected punctuation or an
                     * otherwise unexpected character, so just complain about
                     * that one character.
                     */
                    if p == s {
                        self.lex.lex.token_terminator = s + 1;
                        return Ok(JsonError::InvalidToken);
                    }

                    if !is_last && p == end {
                        self.st.stash(&input[s..end])?;
                        return Ok(JsonError::Incomplete);
                    }

                    /*
                     * We've got a real alphanumeric token here.  If it
                     * happens to be true, false, or null, all is well.  If
                     * not, error out.
                     */
                    self.lex.lex.token_terminator = p;
                    self.lex.lex.token_type = match &input[s..p] {
                        b"true" => JsonToken::True,
                        b"null" => JsonToken::Null,
                        b"false" => JsonToken::False,
                        _ => return Ok(JsonError::InvalidToken),
                    };
                }
            } /* end of switch */
        }

        if self.lex.lex.token_type == JsonToken::End && !is_last {
            Ok(JsonError::Incomplete)
        } else {
            Ok(JsonError::Success)
        }
    }

    /// C: the partial-token branch of json_lex (jsonapi.c:1620-1798).
    fn json_lex_partial(&mut self) -> PgResult<JsonError> {
        let input = self.lex.lex.input;
        let input_length = input.len();
        let is_last = self.st.is_last_chunk;
        // Every push below adds at most one byte per input byte.
        self.st.admit(input_length)?;
        let ptok = &mut self.st.partial_token;
        let mut added: usize = 0;
        let mut tok_done = false;

        if ptok[0] == b'"' {
            /*
             * It's a string. Accumulate characters until we reach an
             * unescaped '"'.
             */
            let mut escapes: usize = 0;

            /* count the trailing backslashes on the partial token */
            let mut i = ptok.len() - 1;
            while i > 0 {
                if ptok[i] == b'\\' {
                    escapes += 1;
                } else {
                    break;
                }
                i -= 1;
            }

            for &c in input.iter() {
                ptok.push(c);
                added += 1;
                if c == b'"' && escapes % 2 == 0 {
                    tok_done = true;
                    break;
                }
                if c == b'\\' {
                    escapes += 1;
                } else {
                    escapes = 0;
                }
            }
        } else {
            /* not a string */
            let c = ptok[0];

            if c == b'-' || c.is_ascii_digit() {
                /*
                 * Accumulate numeric continuations, respecting JSON number
                 * grammar: -? int [frac] [exp]
                 *
                 * We must track what parts of the number we've already seen
                 * so we don't over-consume.  '.' is valid only once and not
                 * after 'e'/'E'; 'e'/'E' is valid only once; '+'/'-' are
                 * valid only immediately after 'e'/'E'.
                 */
                let mut numend = false;
                let mut seen_dot = false;
                let mut seen_exp = false;

                /* Scan existing partial token for state */
                for &pc in ptok.iter() {
                    if pc == b'.' {
                        seen_dot = true;
                    } else if pc == b'e' || pc == b'E' {
                        seen_exp = true;
                    }
                }
                let mut prev = ptok[ptok.len() - 1];

                let mut i = 0;
                while i < input_length && !numend {
                    let cc = input[i];

                    match cc {
                        b'+' | b'-' => {
                            if prev != b'e' && prev != b'E' {
                                numend = true;
                            } else {
                                ptok.push(cc);
                                added += 1;
                            }
                        }
                        b'.' => {
                            if seen_dot || seen_exp {
                                numend = true;
                            } else {
                                seen_dot = true;
                                ptok.push(cc);
                                added += 1;
                            }
                        }
                        b'e' | b'E' => {
                            if seen_exp {
                                numend = true;
                            } else {
                                seen_exp = true;
                                ptok.push(cc);
                                added += 1;
                            }
                        }
                        b'0'..=b'9' => {
                            ptok.push(cc);
                            added += 1;
                        }
                        _ => numend = true,
                    }
                    if !numend {
                        prev = cc;
                    }
                    i += 1;
                }
            }

            /*
             * Add any remaining alphanumeric chars. This takes care of the
             * {null, false, true} literals as well as any trailing
             * alphanumeric junk on non-string tokens.
             */
            let mut i = added;
            while i < input_length {
                let cc = input[i];

                if is_alnum(cc) {
                    ptok.push(cc);
                    added += 1;
                } else {
                    tok_done = true;
                    break;
                }
                i += 1;
            }
            if added == input_length && is_last {
                tok_done = true;
            }
        }

        if !tok_done {
            /* We should have consumed the whole chunk in this case. */
            debug_assert_eq!(added, input_length);

            if !is_last {
                return Ok(JsonError::Incomplete);
            }

            /* json_errdetail() needs access to the accumulated token. */
            self.token_in_partial = true;
            self.ptok_start = 0;
            self.ptok_end = ptok.len();
            self.lex.lex.token_start = Some(0);
            return Ok(JsonError::InvalidToken);
        }

        /*
         * Everything up to lex->input[added] has been added to the partial
         * token, so move the input past it.
         */
        self.lex.lex.input = &input[added..];

        // C: dummy_lex over the partial token (jsonapi.c:1758), sharing
        // need_escapes/strval with the real lexer, non-incremental.
        let (partial_result, token_type, line_number, token_start, token_terminator, strval) = {
            let mut dummy = JsonLexDe::with_escapes(
                self.lex.mcx,
                &self.st.partial_token,
                self.lex.lex.encoding,
                self.lex.need_escapes,
            );
            dummy.lex.line_number = self.lex.lex.line_number;
            let partial_result = dummy.lex()?;
            (
                partial_result,
                dummy.lex.token_type,
                dummy.lex.line_number,
                dummy.lex.token_start,
                dummy.lex.token_terminator,
                dummy.strval,
            )
        };

        /*
         * We either have a complete token or an error. In either case we need
         * to point to the partial token data for the semantic or error
         * routines. If it's not an error we'll readjust on the next call to
         * json_lex.
         */
        self.lex.lex.token_type = token_type;
        self.lex.lex.line_number = line_number;
        self.lex.strval = strval;

        /*
         * Normally token_start would be ptok->data, but it could be later,
         * see json_lex_string's handling of invalid escapes.
         */
        self.token_in_partial = true;
        self.ptok_start = token_start.unwrap_or(token_terminator);
        self.ptok_end = token_terminator;
        // The chunk-relative positions are meaningless while the token lives
        // in the partial buffer; keep token_start non-NULL (C: ptok->data)
        // so report_parse_error does not mistake this for end of input.
        self.lex.lex.token_start = Some(0);
        self.lex.lex.token_terminator = 0;
        if partial_result == JsonError::Success {
            /* make sure we've used all the input */
            if self.ptok_end - self.ptok_start != self.st.partial_token.len() {
                debug_assert!(false, "partial token not fully consumed");
                return Ok(JsonError::InvalidToken);
            }

            self.st.partial_completed = true;
        }
        Ok(partial_result)
        /* end of partial token processing */
    }

    /// C: pg_parse_json_incremental (jsonapi.c:869) for this chunk. Returns
    /// `JsonError::Incomplete` when a non-last chunk was consumed without
    /// error, `Success` when the last chunk completed the document, and the
    /// parse error otherwise (see [`Self::errdetail`]). Hook failures
    /// propagate as `Err`.
    pub fn parse<S>(&mut self, sem: &mut S) -> PgResult<JsonError>
    where
        S: for<'a> JsonSem<'a> + ?Sized,
    {
        let r = self.parse_inner(sem);
        // C: lex->line_number lives on the persistent context.
        self.st.line_number = self.lex.lex.line_number;
        r
    }

    fn parse_inner<S>(&mut self, sem: &mut S) -> PgResult<JsonError>
    where
        S: for<'a> JsonSem<'a> + ?Sized,
    {
        /* get the initial token */
        let result = self.json_lex()?;
        if result != JsonError::Success {
            return Ok(result);
        }

        let mut tok = self.lex.lex.token_type;

        /* use prediction stack for incremental parsing */

        if !self.st.have_prediction() {
            self.st.push_prediction(JSON_PROD_GOAL);
        }

        while self.st.have_prediction() {
            let top = self.st.pop_prediction();

            /*
             * these first two branches are the guts of the Table Driven method
             */
            if top == tok as u8 {
                /*
                 * tok can only be a terminal symbol, so top must be too. the
                 * token matches the top of the stack, so get the next token.
                 */
                if (tok as u8) < T_END {
                    let result = self.json_lex()?;
                    if result != JsonError::Success {
                        return Ok(result);
                    }
                    tok = self.lex.lex.token_type;
                }
            } else if let Some(entry) = td_parser_table(top, tok).filter(|_| is_nt(top)) {
                /*
                 * the token is in the director set for a production of the
                 * non-terminal at the top of the stack, so push the reversed
                 * RHS of the production onto the stack.
                 */
                self.st.push_prediction(entry);
            } else if is_sem(top) {
                /*
                 * top is a semantic action marker, so take action accordingly.
                 * It's important to have these markers in the prediction
                 * stack before any token they might need so we don't advance
                 * the token prematurely. Note in a couple of cases we need to
                 * do something both before and after the token.
                 */
                match top {
                    JSON_SEM_OSTART => {
                        if self.st.lex_level >= JSON_TD_MAX_STACK {
                            return Ok(JsonError::NestingTooDeep);
                        }

                        self.lex.lex.lex_level = self.st.lex_level;
                        if !sem.object_start(&self.lex.lex)? {
                            return Ok(JsonError::SemActionFailed);
                        }

                        self.st.inc_lex_level();
                    }
                    JSON_SEM_OEND => {
                        self.st.dec_lex_level();
                        self.lex.lex.lex_level = self.st.lex_level;
                        if !sem.object_end(&self.lex.lex)? {
                            return Ok(JsonError::SemActionFailed);
                        }
                    }
                    JSON_SEM_ASTART => {
                        if self.st.lex_level >= JSON_TD_MAX_STACK {
                            return Ok(JsonError::NestingTooDeep);
                        }

                        self.lex.lex.lex_level = self.st.lex_level;
                        if !sem.array_start(&self.lex.lex)? {
                            return Ok(JsonError::SemActionFailed);
                        }

                        self.st.inc_lex_level();
                    }
                    JSON_SEM_AEND => {
                        self.st.dec_lex_level();
                        self.lex.lex.lex_level = self.st.lex_level;
                        if !sem.array_end(&self.lex.lex)? {
                            return Ok(JsonError::SemActionFailed);
                        }
                    }
                    JSON_SEM_OFIELD_INIT => {
                        /*
                         * all we do here is save out the field name. We have
                         * to wait to get past the ':' to see if the next
                         * value is null so we can call the semantic routine
                         */
                        if self.lex.need_escapes {
                            // C: STRDUP(lex->strval->data) into fnames[level].
                            let slot = &mut self.st.fnames[self.st.lex_level as usize];
                            slot.clear();
                            slot.extend_from_slice(&self.lex.strval);
                        } else {
                            self.st.set_fname(&[]);
                        }
                    }
                    JSON_SEM_OFIELD_START => {
                        /*
                         * the current token should be the first token of the
                         * value
                         */
                        let isnull = tok == JsonToken::Null;

                        self.st.set_fnull(isnull);

                        self.lex.lex.lex_level = self.st.lex_level;
                        let fname = self.st.get_fname();
                        if !sem.object_field_start(&self.lex.lex, fname, isnull)? {
                            return Ok(JsonError::SemActionFailed);
                        }
                    }
                    JSON_SEM_OFIELD_END => {
                        let fname = self.st.get_fname();
                        let isnull = self.st.get_fnull();

                        self.lex.lex.lex_level = self.st.lex_level;
                        if !sem.object_field_end(&self.lex.lex, fname, isnull)? {
                            return Ok(JsonError::SemActionFailed);
                        }
                    }
                    JSON_SEM_AELEM_START => {
                        let isnull = tok == JsonToken::Null;

                        self.st.set_fnull(isnull);

                        self.lex.lex.lex_level = self.st.lex_level;
                        if !sem.array_element_start(&self.lex.lex, isnull)? {
                            return Ok(JsonError::SemActionFailed);
                        }
                    }
                    JSON_SEM_AELEM_END => {
                        let isnull = self.st.get_fnull();

                        self.lex.lex.lex_level = self.st.lex_level;
                        if !sem.array_element_end(&self.lex.lex, isnull)? {
                            return Ok(JsonError::SemActionFailed);
                        }
                    }
                    JSON_SEM_SCALAR_INIT => {
                        /*
                         * extract the de-escaped string value, or the raw
                         * lexeme
                         */
                        let mut val = self.st.scalar_val.take().unwrap_or_default();
                        val.clear();
                        if tok == JsonToken::String {
                            if self.lex.need_escapes {
                                val.extend_from_slice(&self.lex.strval);
                                self.st.scalar_val = Some(val);
                            } else {
                                // C: scalar_val stays NULL.
                                self.st.scalar_val = None;
                            }
                        } else {
                            val.extend_from_slice(self.current_token_bytes());
                            self.st.scalar_val = Some(val);
                        }
                        self.st.scalar_tok = tok;
                    }
                    JSON_SEM_SCALAR_CALL => {
                        /*
                         * We'd like to be able to get rid of this business of
                         * two bits of scalar action, but we can't. It breaks
                         * certain semantic actions which expect that when
                         * called the lexer has consumed the item. See for
                         * example get_scalar() in jsonfuncs.c.
                         */
                        let val: &[u8] = self.st.scalar_val.as_deref().unwrap_or(&[]);
                        let token = match self.st.scalar_tok {
                            JsonToken::String => JsonSemToken::String(val),
                            JsonToken::Number => JsonSemToken::Number(val),
                            JsonToken::True => JsonSemToken::True,
                            JsonToken::False => JsonSemToken::False,
                            JsonToken::Null => JsonSemToken::Null,
                            other => unreachable!("scalar_tok {other:?} is not a scalar"),
                        };
                        self.lex.lex.lex_level = self.st.lex_level;
                        let ok = sem.scalar(&self.lex.lex, token)?;

                        /*
                         * Either ownership of the token passed to the
                         * callback, or we need to free it now. Either way,
                         * clear our pointer to it so it doesn't get freed in
                         * the future. (The buffer is kept for reuse.)
                         */
                        if let Some(v) = self.st.scalar_val.as_mut() {
                            v.clear();
                        }

                        if !ok {
                            return Ok(JsonError::SemActionFailed);
                        }
                    }
                    _ => {
                        /* should not happen */
                    }
                }
            } else {
                /*
                 * The token didn't match the stack top if it's a terminal nor
                 * a production for the stack top if it's a non-terminal.
                 *
                 * Various cases here are Asserted to be not possible, as the
                 * token would not appear at the top of the prediction stack
                 * unless the lookahead matched.
                 */
                let ctx = match top {
                    T_STRING => {
                        if self.st.next_prediction() == T_COLON {
                            ParseCtx::String
                        } else {
                            debug_assert!(false);
                            ParseCtx::Value
                        }
                    }
                    T_NUMBER | T_TRUE | T_FALSE | T_NULL | T_ARRAY_START | T_OBJECT_START => {
                        debug_assert!(false);
                        ParseCtx::Value
                    }
                    T_ARRAY_END => {
                        debug_assert!(false);
                        ParseCtx::ArrayNext
                    }
                    T_OBJECT_END => {
                        debug_assert!(false);
                        ParseCtx::ObjectNext
                    }
                    T_COMMA => {
                        debug_assert!(false);
                        if self.st.next_prediction() == T_STRING {
                            ParseCtx::ObjectNext
                        } else {
                            ParseCtx::ArrayNext
                        }
                    }
                    T_COLON => ParseCtx::ObjectLabel,
                    T_END => ParseCtx::End,
                    JSON_NT_MORE_ARRAY_ELEMENTS => ParseCtx::ArrayNext,
                    JSON_NT_ARRAY_ELEMENTS => ParseCtx::ArrayStart,
                    JSON_NT_MORE_KEY_PAIRS => ParseCtx::ObjectNext,
                    JSON_NT_KEY_PAIRS => ParseCtx::ObjectStart,
                    _ => ParseCtx::Value,
                };
                return Ok(self.lex.lex.report_parse_error(ctx));
            }
        }

        Ok(JsonError::Success)
    }
}

/// C: pg_parse_json_incremental(lex, sem, json, len, is_last) — parse one
/// chunk of a document against `lex`; on the final chunk `is_last` must be
/// true. Convenience over [`JsonLexIncremental::chunk`] +
/// [`JsonIncrementalChunk::parse`] for callers that do not need
/// [`JsonIncrementalChunk::errdetail`].
pub fn pg_parse_json_incremental<S>(
    lex: &mut JsonLexIncremental,
    mcx: Mcx<'_>,
    sem: &mut S,
    chunk: &[u8],
    is_last: bool,
) -> PgResult<JsonError>
where
    S: for<'a> JsonSem<'a> + ?Sized,
{
    lex.chunk(mcx, chunk, is_last).parse(sem)
}

#[cfg(test)]
mod tests;
