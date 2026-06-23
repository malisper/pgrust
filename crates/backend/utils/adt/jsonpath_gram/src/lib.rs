//! Port of `src/backend/utils/adt/jsonpath_gram.y` (PostgreSQL 18.3) — the
//! bison grammar that turns the jsonpath token stream into a tree of
//! `JsonPathParseItem` structs.
//!
//! Bison generates an LALR(1) table-driven parser; this is a faithful
//! hand-written recursive-descent / operator-precedence equivalent of that
//! grammar. The grammar is small, unambiguous once the declared precedences
//! (`%left OR_P`, `%left AND_P`, `%right NOT_P`, `%left '+' '-'`,
//! `%left '*' '/' '%'`, `%left UMINUS`) are applied, so the recursive-descent
//! shape reproduces the same reductions. The rule actions are mirrored exactly:
//! the `makeItem*` / `makeIndexArray` / `makeAny` / `makeItemLikeRegex`
//! helpers below are direct ports of the C functions in the `.y` epilogue.
//!
//! The token stream is produced by [`jsonpath_scan`]; the
//! driver [`parsejsonpath`] mirrors the C `parsejsonpath()` in
//! `jsonpath_scan.l`: scan the whole input, run the parser, and on a syntax
//! error report `"invalid input"` via the scanner's `jsonpath_yyerror`.
//!
//! `jspConvertRegexFlags` is co-located with the grammar in C (it is a
//! non-static function in `jsonpath_gram.y`). In this workspace it was placed
//! in `backend-utils-adt-jsonpath`, which depends on this crate's seam, so we
//! cannot call it from here without a cycle; the small flag-conversion logic is
//! reproduced inline in [`make_item_like_regex`] exactly as the C
//! `makeItemLikeRegex` -> `jspConvertRegexFlags` path does.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use jsonpath_scan::{
    jsonpath_yyerror, jsonpath_yyerror_yytext, JsonPathLexer, Lexeme, Token,
};
use mcx::MemoryContext;
use types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use types_error::{ERRCODE_INVALID_REGULAR_EXPRESSION, ERRCODE_SYNTAX_ERROR};
use types_jsonpath::jsonpath::{
    JsonPathItemType, JSP_REGEX_DOTALL, JSP_REGEX_ICASE, JSP_REGEX_MLINE, JSP_REGEX_QUOTE,
    JSP_REGEX_WSPACE,
};
use types_jsonpath::parse::{
    JsonPathParseItem, JsonPathParseResult, JsonPathParseValue, JsonPathString, JsonPathSubscript,
};

mod seams;
pub use seams::init_seams;

type Item = Box<JsonPathParseItem>;

// ===========================================================================
// makeItem* helpers — direct ports of the jsonpath_gram.y epilogue.
// ===========================================================================

/// C: `makeItemType(JsonPathItemType type)`.
fn make_item_type(typ: JsonPathItemType) -> Item {
    // C: CHECK_FOR_INTERRUPTS(); — best-effort interrupt poll.
    // (no ambient interrupt facility at this layer; the scanner already runs
    // bounded over the in-memory token stream.)
    Box::new(JsonPathParseItem { typ, next: None, value: JsonPathParseValue::None })
}

/// C: `makeItemString(JsonPathString *s)` — `NULL` s => a `jpiNull` literal.
fn make_item_string(s: Option<&JsonPathString>) -> Item {
    match s {
        None => make_item_type(JsonPathItemType::jpiNull),
        Some(s) => {
            let mut v = make_item_type(JsonPathItemType::jpiString);
            // C: v->value.string.{val,len} = s->{val,len}.
            v.value = JsonPathParseValue::String(s.bytes().to_vec());
            v
        }
    }
}

/// C: `makeItemVariable(JsonPathString *s)`.
fn make_item_variable(s: &JsonPathString) -> Item {
    let mut v = make_item_type(JsonPathItemType::jpiVariable);
    v.value = JsonPathParseValue::String(s.bytes().to_vec());
    v
}

/// C: `makeItemKey(JsonPathString *s)` — a `makeItemString` retyped to `jpiKey`.
fn make_item_key(s: &JsonPathString) -> Item {
    let mut v = make_item_string(Some(s));
    v.typ = JsonPathItemType::jpiKey;
    v
}

/// C: `makeItemNumeric(JsonPathString *s)` —
/// `numeric_in(s->val, InvalidOid, -1)`. The decoded numeric varlena bytes are
/// owned by the parse item (copied out of the transient context).
fn make_item_numeric(s: &JsonPathString) -> PgResult<Item> {
    let mut v = make_item_type(JsonPathItemType::jpiNumeric);
    let bytes = numeric_in_bytes(s.bytes())?;
    v.value = JsonPathParseValue::Numeric(bytes);
    Ok(v)
}

/// C: `makeItemBool(bool val)`.
fn make_item_bool(val: bool) -> Item {
    let mut v = make_item_type(JsonPathItemType::jpiBool);
    v.value = JsonPathParseValue::Boolean(val);
    v
}

/// C: `makeItemBinary(type, la, ra)`.
fn make_item_binary(typ: JsonPathItemType, la: Option<Item>, ra: Option<Item>) -> Item {
    let mut v = make_item_type(typ);
    v.value = JsonPathParseValue::Args { left: la, right: ra };
    v
}

/// C: `makeItemUnary(type, a)`. Folds `+numeric`/`-numeric` constants exactly
/// as the C does (`jpiPlus`/`jpiMinus` over a lone `jpiNumeric`).
fn make_item_unary(typ: JsonPathItemType, a: Item) -> PgResult<Item> {
    // C: if (type == jpiPlus && a->type == jpiNumeric && !a->next) return a;
    if typ == JsonPathItemType::jpiPlus
        && a.typ == JsonPathItemType::jpiNumeric
        && a.next.is_none()
    {
        return Ok(a);
    }

    // C: if (type == jpiMinus && a->type == jpiNumeric && !a->next) ...
    if typ == JsonPathItemType::jpiMinus
        && a.typ == JsonPathItemType::jpiNumeric
        && a.next.is_none()
    {
        let mut v = make_item_type(JsonPathItemType::jpiNumeric);
        let num = match &a.value {
            JsonPathParseValue::Numeric(n) => n.as_slice(),
            // can't happen: typ == jpiNumeric implies a Numeric payload.
            _ => unreachable!("jpiNumeric item without Numeric value"),
        };
        let negated = numeric_uminus_bytes(num)?;
        v.value = JsonPathParseValue::Numeric(negated);
        return Ok(v);
    }

    let mut v = make_item_type(typ);
    v.value = JsonPathParseValue::Arg(Some(a));
    Ok(v)
}

/// C: `makeItemList(List *list)` — chain the items via `->next` and return the
/// head. The input is the `accessor_expr` list (non-empty).
fn make_item_list(mut list: Vec<Item>) -> Item {
    // C: head = end = linitial(list); if (list_length == 1) return head;
    debug_assert!(!list.is_empty());
    if list.len() == 1 {
        return list.pop().unwrap();
    }

    // C walks to the end of the (possibly already-chained) head's ->next list,
    // then appends list[1..]. We build the chain back-to-front to own the boxes.
    let mut iter = list.into_iter();
    let mut head = iter.next().unwrap();

    // Find the current tail of `head`'s ->next chain.
    {
        let mut end: &mut JsonPathParseItem = &mut head;
        while end.next.is_some() {
            end = end.next.as_mut().unwrap();
        }
        // Append the remaining items, each becoming the new tail.
        for c in iter {
            end.next = Some(c);
            end = end.next.as_mut().unwrap();
        }
    }

    head
}

/// C: `makeIndexArray(List *list)` — list of `jpiSubscript` items become the
/// `from`/`to` pairs of a `jpiIndexArray`.
fn make_index_array(list: Vec<Item>) -> Item {
    debug_assert!(!list.is_empty()); // C: Assert(list != NIL).
    let mut v = make_item_type(JsonPathItemType::jpiIndexArray);

    let mut elems: Vec<JsonPathSubscript> = Vec::with_capacity(list.len());
    for jpi in list {
        debug_assert_eq!(jpi.typ, JsonPathItemType::jpiSubscript);
        // C reads jpi->value.args.{left,right} into elems[i].{from,to}.
        let (from, to) = match jpi.value {
            JsonPathParseValue::Args { left, right } => (left, right),
            // can't happen: index_elem always builds a jpiSubscript Args item.
            _ => unreachable!("jpiSubscript item without Args value"),
        };
        elems.push(JsonPathSubscript { from, to });
    }

    v.value = JsonPathParseValue::Array(elems);
    v
}

/// C: `makeAny(int first, int last)` — `jpiAny` with negative bounds mapped to
/// `PG_UINT32_MAX`.
fn make_any(first: i32, last: i32) -> Item {
    let mut v = make_item_type(JsonPathItemType::jpiAny);
    let f = if first >= 0 { first as u32 } else { u32::MAX };
    let l = if last >= 0 { last as u32 } else { u32::MAX };
    v.value = JsonPathParseValue::AnyBounds { first: f, last: l };
    v
}

/// C: `makeItemLikeRegex(expr, pattern, flags, &result, escontext)`. Returns
/// `Ok(None)` when a soft error was recorded (C `return false`).
fn make_item_like_regex(
    expr: Option<Item>,
    pattern: &JsonPathString,
    flags: Option<&JsonPathString>,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> PgResult<Option<Item>> {
    let mut v = make_item_type(JsonPathItemType::jpiLikeRegex);

    let pattern_bytes = pattern.bytes().to_vec();
    let pattern_len = pattern.len;

    // C: parse the flags string into the bitmask. Duplicate flags are OK.
    let mut xflags: u32 = 0;
    if let Some(flags) = flags {
        let fbytes = flags.bytes();
        for (i, &c) in fbytes.iter().enumerate() {
            match c {
                b'i' => xflags |= JSP_REGEX_ICASE,
                b's' => xflags |= JSP_REGEX_DOTALL,
                b'm' => xflags |= JSP_REGEX_MLINE,
                b'x' => xflags |= JSP_REGEX_WSPACE,
                b'q' => xflags |= JSP_REGEX_QUOTE,
                _ => {
                    // C uses pg_mblen_range to render the offending character;
                    // for the soft-error detail we render the remaining flag
                    // text from the offending byte, matching the visible text.
                    let near =
                        alloc::string::String::from_utf8_lossy(&fbytes[i..]).into_owned();
                    return ereturn(
                        escontext.as_deref_mut(),
                        None,
                        PgError::error("invalid input syntax for type jsonpath")
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                            .with_detail(alloc::format!(
                                "Unrecognized flag character \"{}\" in LIKE_REGEX predicate.",
                                near
                            )),
                    );
                }
            }
        }
    }

    // C: jspConvertRegexFlags(flags, &cflags, escontext). Co-located in the .y;
    // reproduced inline here (the canonical copy lives in
    // backend-utils-adt-jsonpath, which we cannot depend on without a cycle).
    let cflags = match jsp_convert_regex_flags(xflags, escontext)? {
        Some(c) => c,
        None => return Ok(None),
    };

    // C: check regex validity — pg_mb2wchar_with_len + pg_regcomp + pg_regfree.
    let _ = pattern_len;
    {
        let cx = MemoryContext::new("JsonPathRegexValidate");
        let wpattern =
            mbutils::pg_mb2wchar_with_len(cx.mcx(), &pattern_bytes)?;
        match regex_core::regex_compile::pg_regcomp(
            cx.mcx(),
            wpattern.as_slice(),
            cflags,
            types_tuple::heaptuple::DEFAULT_COLLATION_OID,
        ) {
            Ok(_re) => {
                // C: pg_regfree(&re_tmp). The owned RegexT is dropped with `cx`.
            }
            Err(e) => {
                // C: pg_regerror(...) -> ereturn ERRCODE_INVALID_REGULAR_EXPRESSION.
                let msg = regex_core::regex_export_free_error::pg_regerror(e.0);
                return ereturn(
                    escontext.as_deref_mut(),
                    None,
                    PgError::error(alloc::format!("invalid regular expression: {}", msg))
                        .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION),
                );
            }
        }
    }

    v.value = JsonPathParseValue::LikeRegex { expr, pattern: pattern_bytes, flags: xflags };
    Ok(Some(v))
}

/// C: `jspConvertRegexFlags(uint32 xflags, int *result, escontext)` — XQuery
/// regex flags -> regex-library cflags. Reproduced inline (see module note).
/// Returns `Ok(None)` if a soft error was recorded.
fn jsp_convert_regex_flags(
    xflags: u32,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> PgResult<Option<i32>> {
    use regex_core::regex_consts::{
        REG_ADVANCED, REG_ICASE, REG_NLANCH, REG_NLSTOP, REG_QUOTE,
    };
    use types_error::ERRCODE_FEATURE_NOT_SUPPORTED;

    // C: "By default, XQuery is very nearly the same as Spencer's AREs".
    let mut cflags: i32 = REG_ADVANCED;

    if xflags & JSP_REGEX_ICASE != 0 {
        cflags |= REG_ICASE;
    }

    if xflags & JSP_REGEX_QUOTE != 0 {
        cflags &= !REG_ADVANCED;
        cflags |= REG_QUOTE;
    } else {
        if xflags & JSP_REGEX_DOTALL == 0 {
            cflags |= REG_NLSTOP;
        }
        if xflags & JSP_REGEX_MLINE != 0 {
            cflags |= REG_NLANCH;
        }
        if xflags & JSP_REGEX_WSPACE != 0 {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                PgError::error(
                    "XQuery \"x\" flag (expanded regular expressions) is not implemented",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            );
        }
    }

    Ok(Some(cflags))
}

// ===========================================================================
// numeric bridge — numeric_in / numeric_uminus over a transient context, with
// the result bytes copied out to an owned Vec the parse tree keeps.
// ===========================================================================

/// C: `numeric_in(cstring, InvalidOid, -1)` — decode the literal text into the
/// numeric on-disk varlena bytes (owned).
fn numeric_in_bytes(text: &[u8]) -> PgResult<Vec<u8>> {
    // numeric_in takes &str (it scans ASCII numeric syntax). The literal text
    // is ASCII (decimal/real/hex/oct/bin digits with '_'); lossless for ASCII.
    let s = alloc::string::String::from_utf8_lossy(text);
    let cx = MemoryContext::new("JsonPathNumericIn");
    let v = adt_numeric::io::numeric_in(cx.mcx(), &s, -1)?;
    Ok(v.as_slice().to_vec())
}

/// C: `numeric_uminus(numeric)` — negate the numeric varlena bytes (owned out).
fn numeric_uminus_bytes(num: &[u8]) -> PgResult<Vec<u8>> {
    let cx = MemoryContext::new("JsonPathNumericUminus");
    let v = adt_numeric::ops_sql::numeric_uminus(cx.mcx(), num)?;
    Ok(v.as_slice().to_vec())
}

// ===========================================================================
// The parser: a recursive-descent / operator-precedence equivalent of the
// bison grammar. Operates over the fully-scanned token vector.
// ===========================================================================

struct Parser<'e, 's> {
    toks: Vec<Lexeme>,
    idx: usize,
    escontext: &'e mut Option<&'s mut SoftErrorContext>,
    /// Set when a `makeItem*` action recorded a soft error (C: YYABORT after a
    /// failed `makeItemLikeRegex`/`.decimal()` shape check). The parse stops.
    aborted: bool,
}

/// A parse step either yields a value or signals "syntax error" (`None`),
/// which bison would surface by `jsonpath_yyparse` returning nonzero.
type POut<T> = PgResult<Option<T>>;

impl<'e, 's> Parser<'e, 's> {
    fn peek(&self) -> Option<&Lexeme> {
        self.toks.get(self.idx)
    }

    fn peek_tok(&self) -> Option<Token> {
        self.toks.get(self.idx).map(|l| l.token)
    }

    fn at_char(&self, c: u8) -> bool {
        matches!(self.peek_tok(), Some(Token::Char(x)) if x == c)
    }

    fn bump(&mut self) -> Option<Lexeme> {
        let l = self.toks.get(self.idx).cloned();
        if l.is_some() {
            self.idx += 1;
        }
        l
    }

    /// Consume a `Char(c)` literal; syntax error if not present.
    fn expect_char(&mut self, c: u8) -> POut<()> {
        if self.at_char(c) {
            self.idx += 1;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    /// Consume the given keyword token; syntax error if not present.
    fn expect(&mut self, t: Token) -> POut<()> {
        if self.peek_tok() == Some(t) {
            self.idx += 1;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    /// Take the `JsonPathString` value of the current token (for value tokens),
    /// advancing past it.
    fn take_str(&mut self) -> JsonPathString {
        let l = self.bump().expect("take_str on exhausted token stream");
        l.value.unwrap_or_default()
    }

    // -- result: mode expr_or_predicate | /* empty */ ------------------------

    fn parse_result(&mut self) -> POut<Option<JsonPathParseResult>> {
        // C: `result: /* EMPTY */ { *result = NULL; }` — empty input.
        if self.peek().is_none() {
            return Ok(Some(None));
        }

        // mode: STRICT_P -> false | LAX_P -> true | /* empty */ -> true.
        let lax = match self.peek_tok() {
            Some(Token::STRICT_P) => {
                self.idx += 1;
                false
            }
            Some(Token::LAX_P) => {
                self.idx += 1;
                true
            }
            _ => true,
        };

        // expr_or_predicate: expr | predicate.
        let expr = match self.parse_expr_or_predicate()? {
            Some(e) => e,
            None => return Ok(None),
        };
        if self.aborted {
            return Ok(None);
        }

        // The whole token stream must be consumed (bison reaches accept only at
        // end of input).
        if self.peek().is_some() {
            return Ok(None);
        }

        Ok(Some(Some(JsonPathParseResult { expr: Some(expr), lax })))
    }

    // -- expr_or_predicate ---------------------------------------------------
    //
    // `expr` and `predicate` share one precedence stack in bison (the operator
    // precedences are global). We parse one unified precedence-climbing
    // expression that may yield either an arithmetic `expr` node or a boolean
    // `predicate` node; the node types track which.

    fn parse_expr_or_predicate(&mut self) -> POut<Item> {
        // Lowest precedence first: OR (%left), then AND (%left), then the
        // comparison / STARTS WITH / LIKE_REGEX predicates, then NOT (%right),
        // then additive, multiplicative, unary +/-, then primary.
        self.parse_or()
    }

    /// `predicate OR_P predicate` (%left OR_P).
    fn parse_or(&mut self) -> POut<Item> {
        let mut left = match self.parse_and()? {
            Some(v) => v,
            None => return Ok(None),
        };
        while self.peek_tok() == Some(Token::OR_P) {
            self.idx += 1;
            let right = match self.parse_and()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(JsonPathItemType::jpiOr, Some(left), Some(right));
        }
        Ok(Some(left))
    }

    /// `predicate AND_P predicate` (%left AND_P).
    fn parse_and(&mut self) -> POut<Item> {
        let mut left = match self.parse_comparison()? {
            Some(v) => v,
            None => return Ok(None),
        };
        while self.peek_tok() == Some(Token::AND_P) {
            self.idx += 1;
            let right = match self.parse_comparison()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(JsonPathItemType::jpiAnd, Some(left), Some(right));
        }
        Ok(Some(left))
    }

    /// The comparison / STARTS WITH / LIKE_REGEX predicate layer, which sits
    /// between the boolean connectives and arithmetic:
    ///   predicate: expr comp_op expr
    ///            | expr STARTS_P WITH_P starts_with_initial
    ///            | expr LIKE_REGEX_P STRING_P [FLAG_P STRING_P]
    /// plus the NOT_P / IS UNKNOWN / parenthesised predicate forms handled in
    /// `parse_not`.
    fn parse_comparison(&mut self) -> POut<Item> {
        let left = match self.parse_not()? {
            Some(v) => v,
            None => return Ok(None),
        };

        // comp_op: == != < > <= >=.
        if let Some(op) = self.comp_op() {
            self.idx += 1;
            let right = match self.parse_not()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_binary(op, Some(left), Some(right))));
        }

        // expr STARTS_P WITH_P starts_with_initial.
        if self.peek_tok() == Some(Token::STARTS_P) {
            self.idx += 1;
            if self.expect(Token::WITH_P)?.is_none() {
                return Ok(None);
            }
            let init = match self.parse_starts_with_initial()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_binary(
                JsonPathItemType::jpiStartsWith,
                Some(left),
                Some(init),
            )));
        }

        // expr LIKE_REGEX_P STRING_P [FLAG_P STRING_P].
        if self.peek_tok() == Some(Token::LIKE_REGEX_P) {
            self.idx += 1;
            if self.peek_tok() != Some(Token::STRING_P) {
                return Ok(None);
            }
            let pattern = self.take_str();
            let flags = if self.peek_tok() == Some(Token::FLAG_P) {
                self.idx += 1;
                if self.peek_tok() != Some(Token::STRING_P) {
                    return Ok(None);
                }
                Some(self.take_str())
            } else {
                None
            };
            let res = make_item_like_regex(
                Some(left),
                &pattern,
                flags.as_ref(),
                self.escontext,
            )?;
            match res {
                Some(v) => return Ok(Some(v)),
                None => {
                    // C: `YYABORT` after makeItemLikeRegex failed (soft error).
                    self.aborted = true;
                    return Ok(None);
                }
            }
        }

        Ok(Some(left))
    }

    fn comp_op(&self) -> Option<JsonPathItemType> {
        match self.peek_tok()? {
            Token::EQUAL_P => Some(JsonPathItemType::jpiEqual),
            Token::NOTEQUAL_P => Some(JsonPathItemType::jpiNotEqual),
            Token::LESS_P => Some(JsonPathItemType::jpiLess),
            Token::GREATER_P => Some(JsonPathItemType::jpiGreater),
            Token::LESSEQUAL_P => Some(JsonPathItemType::jpiLessOrEqual),
            Token::GREATEREQUAL_P => Some(JsonPathItemType::jpiGreaterOrEqual),
            _ => None,
        }
    }

    /// `NOT_P delimited_predicate` (%right NOT_P) and the additive layer below.
    fn parse_not(&mut self) -> POut<Item> {
        if self.peek_tok() == Some(Token::NOT_P) {
            self.idx += 1;
            let p = match self.parse_delimited_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_unary_pg(JsonPathItemType::jpiNot, p, self)?));
        }
        self.parse_additive()
    }

    /// `expr '+' expr | expr '-' expr` (%left '+' '-').
    fn parse_additive(&mut self) -> POut<Item> {
        let mut left = match self.parse_multiplicative()? {
            Some(v) => v,
            None => return Ok(None),
        };
        loop {
            let op = if self.at_char(b'+') {
                JsonPathItemType::jpiAdd
            } else if self.at_char(b'-') {
                JsonPathItemType::jpiSub
            } else {
                break;
            };
            self.idx += 1;
            let right = match self.parse_multiplicative()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(op, Some(left), Some(right));
        }
        Ok(Some(left))
    }

    /// `expr '*'|'/'|'%' expr` (%left '*' '/' '%').
    fn parse_multiplicative(&mut self) -> POut<Item> {
        let mut left = match self.parse_unary()? {
            Some(v) => v,
            None => return Ok(None),
        };
        loop {
            let op = if self.at_char(b'*') {
                JsonPathItemType::jpiMul
            } else if self.at_char(b'/') {
                JsonPathItemType::jpiDiv
            } else if self.at_char(b'%') {
                JsonPathItemType::jpiMod
            } else {
                break;
            };
            self.idx += 1;
            let right = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(op, Some(left), Some(right));
        }
        Ok(Some(left))
    }

    /// `'+' expr | '-' expr` (%prec UMINUS) and the primary layer below.
    fn parse_unary(&mut self) -> POut<Item> {
        if self.at_char(b'+') {
            self.idx += 1;
            let e = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_unary_pg(JsonPathItemType::jpiPlus, e, self)?));
        }
        if self.at_char(b'-') {
            self.idx += 1;
            let e = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_unary_pg(JsonPathItemType::jpiMinus, e, self)?));
        }
        self.parse_expr_primary()
    }

    // -- expr: accessor_expr | '(' expr ')' (the parenthesised arithmetic /
    //    predicate form is reached via accessor_expr or this primary) ---------

    fn parse_expr_primary(&mut self) -> POut<Item> {
        // EXISTS_P '(' expr ')' — a delimited_predicate. In the bison grammar
        // `predicate` reduces directly from `delimited_predicate`, so a bare
        // `exists(...)` is a valid primary anywhere an expr/predicate is
        // expected (top level, after AND/OR, etc.).
        if self.peek_tok() == Some(Token::EXISTS_P) {
            return self.parse_delimited_predicate();
        }

        // '(' expr ')' — but a parenthesised group may be either an arithmetic
        // expr (`expr: '(' expr ')'`) or the head of an `accessor_expr`
        // (`'(' expr ')' accessor_op ...`) or a predicate group. We parse the
        // group, then check whether an accessor_op follows.
        if self.at_char(b'(') {
            // Could be:  '(' expr ')'              -> expr
            //            '(' expr ')' accessor_op  -> accessor_expr (list)
            //            '(' predicate ')' accessor_op
            //            '(' predicate ')'         (delimited_predicate)
            //            '(' predicate ')' IS UNKNOWN
            return self.parse_paren_primary();
        }

        // Otherwise it is an `accessor_expr`: path_primary accessor_op*.
        self.parse_accessor_expr()
    }

    /// Handle a leading `(` for the expr/predicate/accessor-expr forms.
    fn parse_paren_primary(&mut self) -> POut<Item> {
        // consume '('
        self.idx += 1;
        // Inside, parse a full expr-or-predicate.
        let inner = match self.parse_expr_or_predicate()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.expect_char(b')')?.is_none() {
            return Ok(None);
        }

        // '(' predicate ')' IS_P UNKNOWN_P -> jpiIsUnknown.
        if self.peek_tok() == Some(Token::IS_P) {
            self.idx += 1;
            if self.expect(Token::UNKNOWN_P)?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_unary_pg(
                JsonPathItemType::jpiIsUnknown,
                inner,
                self,
            )?));
        }

        // If an accessor_op follows, this is `'(' expr|predicate ')' accessor_op`
        // -> list_make2(inner, accessor_op), continued as an accessor_expr.
        if self.at_accessor_op_start() {
            let mut list: Vec<Item> = Vec::new();
            list.push(inner);
            let op = match self.parse_accessor_op()? {
                Some(v) => v,
                None => return Ok(None),
            };
            list.push(op);
            // accessor_expr: accessor_expr accessor_op  (left recursion).
            while self.at_accessor_op_start() {
                let op = match self.parse_accessor_op()? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                list.push(op);
            }
            return Ok(Some(make_item_list(list)));
        }

        // Plain '(' expr ')' / delimited predicate: the value is just `inner`.
        Ok(Some(inner))
    }

    /// `accessor_expr: path_primary accessor_op*` then `makeItemList`.
    fn parse_accessor_expr(&mut self) -> POut<Item> {
        let head = match self.parse_path_primary()? {
            Some(v) => v,
            None => return Ok(None),
        };
        let mut list: Vec<Item> = Vec::new();
        list.push(head);
        while self.at_accessor_op_start() {
            let op = match self.parse_accessor_op()? {
                Some(v) => v,
                None => return Ok(None),
            };
            list.push(op);
        }
        // expr: accessor_expr -> makeItemList.
        Ok(Some(make_item_list(list)))
    }

    /// `delimited_predicate: '(' predicate ')' | EXISTS_P '(' expr ')'`.
    fn parse_delimited_predicate(&mut self) -> POut<Item> {
        if self.peek_tok() == Some(Token::EXISTS_P) {
            self.idx += 1;
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            let e = match self.parse_expr_or_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_unary_pg(JsonPathItemType::jpiExists, e, self)?));
        }
        // '(' predicate ')'.
        if self.at_char(b'(') {
            self.idx += 1;
            let p = match self.parse_expr_or_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(p));
        }
        Ok(None)
    }

    /// `starts_with_initial: STRING_P | VARIABLE_P`.
    fn parse_starts_with_initial(&mut self) -> POut<Item> {
        match self.peek_tok() {
            Some(Token::STRING_P) => {
                let s = self.take_str();
                Ok(Some(make_item_string(Some(&s))))
            }
            Some(Token::VARIABLE_P) => {
                let s = self.take_str();
                Ok(Some(make_item_variable(&s)))
            }
            _ => Ok(None),
        }
    }

    /// `path_primary: scalar_value | '$' | '@' | LAST_P`.
    fn parse_path_primary(&mut self) -> POut<Item> {
        match self.peek_tok() {
            Some(Token::STRING_P) => {
                let s = self.take_str();
                Ok(Some(make_item_string(Some(&s))))
            }
            Some(Token::NULL_P) => {
                self.idx += 1;
                Ok(Some(make_item_string(None)))
            }
            Some(Token::TRUE_P) => {
                self.idx += 1;
                Ok(Some(make_item_bool(true)))
            }
            Some(Token::FALSE_P) => {
                self.idx += 1;
                Ok(Some(make_item_bool(false)))
            }
            Some(Token::NUMERIC_P) | Some(Token::INT_P) => {
                let s = self.take_str();
                Ok(Some(make_item_numeric(&s)?))
            }
            Some(Token::VARIABLE_P) => {
                let s = self.take_str();
                Ok(Some(make_item_variable(&s)))
            }
            Some(Token::Char(b'$')) => {
                self.idx += 1;
                Ok(Some(make_item_type(JsonPathItemType::jpiRoot)))
            }
            Some(Token::Char(b'@')) => {
                self.idx += 1;
                Ok(Some(make_item_type(JsonPathItemType::jpiCurrent)))
            }
            Some(Token::LAST_P) => {
                self.idx += 1;
                Ok(Some(make_item_type(JsonPathItemType::jpiLast)))
            }
            _ => Ok(None),
        }
    }

    /// Whether the current token can begin an `accessor_op`:
    ///   '.' ... | array_accessor ('[') | '?' '(' ...
    fn at_accessor_op_start(&self) -> bool {
        self.at_char(b'.') || self.at_char(b'[') || self.at_char(b'?')
    }

    /// `accessor_op`. See the grammar; returns the accessor node.
    fn parse_accessor_op(&mut self) -> POut<Item> {
        // array_accessor: '[' '*' ']' | '[' index_list ']'.
        if self.at_char(b'[') {
            return self.parse_array_accessor();
        }

        // '?' '(' predicate ')' -> jpiFilter.
        if self.at_char(b'?') {
            self.idx += 1;
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            let p = match self.parse_expr_or_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_unary_pg(JsonPathItemType::jpiFilter, p, self)?));
        }

        // '.' ...
        if self.expect_char(b'.')?.is_none() {
            return Ok(None);
        }

        // '.' '*' -> jpiAnyKey.
        if self.at_char(b'*') {
            self.idx += 1;
            return Ok(Some(make_item_type(JsonPathItemType::jpiAnyKey)));
        }

        // '.' any_path.
        if self.peek_tok() == Some(Token::ANY_P) {
            return self.parse_any_path();
        }

        // '.' method '(' ')'  (method is one of the no-arg item methods).
        if let Some(m) = self.method_optype() {
            // Look ahead: method then '(' ')'.
            // (DECIMAL/DATETIME/TIME*/TIMESTAMP* have argument forms below, so
            // they are not part of `method`.)
            self.idx += 1;
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_type(m)));
        }

        // '.' DECIMAL_P '(' opt_csv_list ')'.
        if self.peek_tok() == Some(Token::DECIMAL_P) {
            return self.parse_decimal_accessor();
        }

        // '.' DATETIME_P '(' opt_datetime_template ')'.
        if self.peek_tok() == Some(Token::DATETIME_P) {
            self.idx += 1;
            // opt_datetime_template -> Some(None) empty / Some(Some(item)) /
            // None on syntax error.
            let arg = match self.parse_paren_opt_datetime_template()? {
                Some(a) => a,
                None => return Ok(None),
            };
            // C: makeItemUnary(jpiDatetime, $4) — $4 may be NULL.
            return Ok(Some(make_item_unary_optional(JsonPathItemType::jpiDatetime, arg)));
        }

        // '.' TIME_P|TIME_TZ_P|TIMESTAMP_P|TIMESTAMP_TZ_P '(' opt_datetime_precision ')'.
        let dt = match self.peek_tok() {
            Some(Token::TIME_P) => Some(JsonPathItemType::jpiTime),
            Some(Token::TIME_TZ_P) => Some(JsonPathItemType::jpiTimeTz),
            Some(Token::TIMESTAMP_P) => Some(JsonPathItemType::jpiTimestamp),
            Some(Token::TIMESTAMP_TZ_P) => Some(JsonPathItemType::jpiTimestampTz),
            _ => None,
        };
        if let Some(dt) = dt {
            self.idx += 1;
            // '(' opt_datetime_precision ')'.
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            // opt_datetime_precision: datetime_precision (INT_P) | empty.
            let arg = if self.peek_tok() == Some(Token::INT_P) {
                let s = self.take_str();
                Some(make_item_numeric(&s)?)
            } else {
                None
            };
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            // makeItemUnary(dt, arg) — arg may be NULL (empty precision).
            return Ok(Some(make_item_unary_optional(dt, arg)));
        }

        // '.' key  (key_name: IDENT_P, STRING_P, or any keyword).
        if let Some(s) = self.try_key_name() {
            return Ok(Some(make_item_key(&s)));
        }

        Ok(None)
    }

    /// `array_accessor: '[' '*' ']' | '[' index_list ']'`.
    fn parse_array_accessor(&mut self) -> POut<Item> {
        self.idx += 1; // '['
        // '[' '*' ']'.
        if self.at_char(b'*') {
            self.idx += 1;
            if self.expect_char(b']')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_type(JsonPathItemType::jpiAnyArray)));
        }

        // '[' index_list ']'.
        let mut list: Vec<Item> = Vec::new();
        let first = match self.parse_index_elem()? {
            Some(v) => v,
            None => return Ok(None),
        };
        list.push(first);
        while self.at_char(b',') {
            self.idx += 1;
            let e = match self.parse_index_elem()? {
                Some(v) => v,
                None => return Ok(None),
            };
            list.push(e);
        }
        if self.expect_char(b']')?.is_none() {
            return Ok(None);
        }
        Ok(Some(make_index_array(list)))
    }

    /// `index_elem: expr | expr TO_P expr` -> `jpiSubscript`.
    fn parse_index_elem(&mut self) -> POut<Item> {
        let from = match self.parse_expr_or_predicate()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.peek_tok() == Some(Token::TO_P) {
            self.idx += 1;
            let to = match self.parse_expr_or_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_binary(
                JsonPathItemType::jpiSubscript,
                Some(from),
                Some(to),
            )));
        }
        Ok(Some(make_item_binary(JsonPathItemType::jpiSubscript, Some(from), None)))
    }

    /// `any_path: ANY_P | ANY_P '{' any_level '}' | ANY_P '{' any_level TO_P any_level '}'`.
    fn parse_any_path(&mut self) -> POut<Item> {
        self.idx += 1; // ANY_P
        if !self.at_char(b'{') {
            // ANY_P -> makeAny(0, -1).
            return Ok(Some(make_any(0, -1)));
        }
        self.idx += 1; // '{'
        let first = match self.parse_any_level()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.peek_tok() == Some(Token::TO_P) {
            self.idx += 1;
            let last = match self.parse_any_level()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if self.expect_char(b'}')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_any(first, last)));
        }
        if self.expect_char(b'}')?.is_none() {
            return Ok(None);
        }
        Ok(Some(make_any(first, first)))
    }

    /// `any_level: INT_P -> pg_strtoint32($1.val) | LAST_P -> -1`.
    fn parse_any_level(&mut self) -> POut<i32> {
        match self.peek_tok() {
            Some(Token::INT_P) => {
                let s = self.take_str();
                // C: pg_strtoint32($1.val) — hard error on overflow.
                let text = alloc::string::String::from_utf8_lossy(s.bytes());
                let n = numutils::pg_strtoint32(&text)?;
                Ok(Some(n))
            }
            Some(Token::LAST_P) => {
                self.idx += 1;
                Ok(Some(-1))
            }
            _ => Ok(None),
        }
    }

    /// `'.' DECIMAL_P '(' opt_csv_list ')'`.
    fn parse_decimal_accessor(&mut self) -> POut<Item> {
        self.idx += 1; // DECIMAL_P
        if self.expect_char(b'(')?.is_none() {
            return Ok(None);
        }
        // opt_csv_list: csv_list | empty.
        let mut list: Vec<Item> = Vec::new();
        if !self.at_char(b')') {
            // csv_list: csv_elem (',' csv_elem)*.
            let first = match self.parse_csv_elem()? {
                Some(v) => v,
                None => return Ok(None),
            };
            list.push(first);
            while self.at_char(b',') {
                self.idx += 1;
                let e = match self.parse_csv_elem()? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                list.push(e);
            }
        }
        if self.expect_char(b')')?.is_none() {
            return Ok(None);
        }

        // C: list_length checks -> makeItemBinary(jpiDecimal, ...).
        match list.len() {
            0 => Ok(Some(make_item_binary(JsonPathItemType::jpiDecimal, None, None))),
            1 => {
                let a = list.pop();
                Ok(Some(make_item_binary(JsonPathItemType::jpiDecimal, a, None)))
            }
            2 => {
                let b = list.pop();
                let a = list.pop();
                Ok(Some(make_item_binary(JsonPathItemType::jpiDecimal, a, b)))
            }
            _ => {
                // C: ereturn ERRCODE_SYNTAX_ERROR + YYABORT.
                let r: POut<Item> = ereturn(
                    self.escontext.as_deref_mut(),
                    None,
                    PgError::error("invalid input syntax for type jsonpath")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                        .with_detail(
                            ".decimal() can only have an optional precision[,scale].",
                        ),
                );
                self.aborted = true;
                r
            }
        }
    }

    /// `csv_elem: INT_P | '+' INT_P %prec UMINUS | '-' INT_P %prec UMINUS`.
    fn parse_csv_elem(&mut self) -> POut<Item> {
        if self.at_char(b'+') {
            self.idx += 1;
            if self.peek_tok() != Some(Token::INT_P) {
                return Ok(None);
            }
            let s = self.take_str();
            let num = make_item_numeric(&s)?;
            return Ok(Some(make_item_unary_pg(JsonPathItemType::jpiPlus, num, self)?));
        }
        if self.at_char(b'-') {
            self.idx += 1;
            if self.peek_tok() != Some(Token::INT_P) {
                return Ok(None);
            }
            let s = self.take_str();
            let num = make_item_numeric(&s)?;
            return Ok(Some(make_item_unary_pg(JsonPathItemType::jpiMinus, num, self)?));
        }
        if self.peek_tok() == Some(Token::INT_P) {
            let s = self.take_str();
            return Ok(Some(make_item_numeric(&s)?));
        }
        Ok(None)
    }

    /// `'(' opt_datetime_template ')'` -> returns `Some(None)` for an empty
    /// template, `Some(Some(item))` for a STRING_P template, or `None` on a
    /// syntax error.
    fn parse_paren_opt_datetime_template(&mut self) -> POut<Option<Item>> {
        if self.expect_char(b'(')?.is_none() {
            return Ok(None);
        }
        // opt_datetime_template: datetime_template (STRING_P) | empty.
        let arg = if self.peek_tok() == Some(Token::STRING_P) {
            let s = self.take_str();
            Some(make_item_string(Some(&s)))
        } else {
            None
        };
        if self.expect_char(b')')?.is_none() {
            return Ok(None);
        }
        Ok(Some(arg))
    }

    /// `method` -> the no-arg item-method optype (C `method:` production).
    fn method_optype(&self) -> Option<JsonPathItemType> {
        match self.peek_tok()? {
            Token::ABS_P => Some(JsonPathItemType::jpiAbs),
            Token::SIZE_P => Some(JsonPathItemType::jpiSize),
            Token::TYPE_P => Some(JsonPathItemType::jpiType),
            Token::FLOOR_P => Some(JsonPathItemType::jpiFloor),
            Token::DOUBLE_P => Some(JsonPathItemType::jpiDouble),
            Token::CEILING_P => Some(JsonPathItemType::jpiCeiling),
            Token::KEYVALUE_P => Some(JsonPathItemType::jpiKeyValue),
            Token::BIGINT_P => Some(JsonPathItemType::jpiBigint),
            Token::BOOLEAN_P => Some(JsonPathItemType::jpiBoolean),
            Token::DATE_P => Some(JsonPathItemType::jpiDate),
            Token::INTEGER_P => Some(JsonPathItemType::jpiInteger),
            Token::NUMBER_P => Some(JsonPathItemType::jpiNumber),
            Token::STRINGFUNC_P => Some(JsonPathItemType::jpiStringFunc),
            _ => None,
        }
    }

    /// `key_name`: IDENT_P, STRING_P, or any of the keyword tokens. Returns the
    /// `JsonPathString` of the matched token (consuming it) or `None`.
    fn try_key_name(&mut self) -> Option<JsonPathString> {
        let tok = self.peek_tok()?;
        let is_key_name = matches!(
            tok,
            Token::IDENT_P
                | Token::STRING_P
                | Token::TO_P
                | Token::NULL_P
                | Token::TRUE_P
                | Token::FALSE_P
                | Token::IS_P
                | Token::UNKNOWN_P
                | Token::EXISTS_P
                | Token::STRICT_P
                | Token::LAX_P
                | Token::ABS_P
                | Token::SIZE_P
                | Token::TYPE_P
                | Token::FLOOR_P
                | Token::DOUBLE_P
                | Token::CEILING_P
                | Token::DATETIME_P
                | Token::KEYVALUE_P
                | Token::LAST_P
                | Token::STARTS_P
                | Token::WITH_P
                | Token::LIKE_REGEX_P
                | Token::FLAG_P
                | Token::BIGINT_P
                | Token::BOOLEAN_P
                | Token::DATE_P
                | Token::DECIMAL_P
                | Token::INTEGER_P
                | Token::NUMBER_P
                | Token::STRINGFUNC_P
                | Token::TIME_P
                | Token::TIME_TZ_P
                | Token::TIMESTAMP_P
                | Token::TIMESTAMP_TZ_P
        );
        if !is_key_name {
            return None;
        }
        // All key_name tokens carry the literal text as their JsonPathString
        // value (keywords are scanned through the xnq/checkKeyword path, so the
        // scanstring is populated). For value-less keyword tokens, the value is
        // the keyword text captured by the scanner.
        Some(self.take_str())
    }
}

/// `makeItemUnary` wrapper that surfaces the numeric-fold path (which needs
/// `numeric_uminus`, hence `PgResult`). The `&mut Parser` arg is unused but
/// keeps the call sites uniform; kept minimal.
fn make_item_unary_pg(
    typ: JsonPathItemType,
    a: Item,
    _p: &mut Parser<'_, '_>,
) -> PgResult<Item> {
    make_item_unary(typ, a)
}

/// `makeItemUnary(type, arg)` where `arg` may be absent (datetime methods with
/// an empty parenthesised argument): C passes `NULL`, producing a unary item
/// with a `NULL` arg.
fn make_item_unary_optional(typ: JsonPathItemType, arg: Option<Item>) -> Item {
    let mut v = make_item_type(typ);
    v.value = JsonPathParseValue::Arg(arg);
    v
}

// ===========================================================================
// Driver: parsejsonpath (jsonpath_scan.l footer).
// ===========================================================================

/// C: `parsejsonpath(const char *str, int len, struct Node *escontext)`.
/// Scans the whole input into a token vector, runs the recursive-descent
/// parser, and on a syntax error records `"invalid input"` through the
/// scanner's `jsonpath_yyerror` (mirroring the C `jsonpath_yyparse != 0` arm).
/// Returns `Ok(None)` for the empty-input / soft-error cases (C `NULL`).
pub fn parsejsonpath(
    str: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<JsonPathParseResult>> {
    // jsonpath_yy_scan_bytes + the lex loop: drive the scanner to a token list.
    let mut lexer = JsonPathLexer::new(str);
    let mut toks: Vec<Lexeme> = Vec::new();
    loop {
        let next = lexer.next_token(&mut escontext)?;
        match next {
            Some(lex) => toks.push(lex),
            None => break,
        }
    }

    // A scanner soft error (escontext set) aborts before parsing — the C lexer
    // returns the error token and yyparse fails; the recorded soft error stands.
    if escontext.as_ref().is_some_and(|c| c.error_occurred()) {
        return Ok(None);
    }

    let result = {
        let mut escontext_ref = escontext;
        let mut parser = Parser {
            toks,
            idx: 0,
            escontext: &mut escontext_ref,
            aborted: false,
        };
        let parsed = parser.parse_result()?;
        let aborted = parser.aborted;
        let consumed_all = parser.peek().is_none();
        // The lookahead token the parser could not accept — C's bison `yytext`
        // at the point `jsonpath_yyerror` fires. Its exact byte span gives the
        // `at or near "<lexeme>"` near-text; when the parser stopped at end of
        // input (no lookahead) there is no span and yyerror reports "at end of
        // jsonpath input".
        let err_span: Option<(usize, usize)> = parser.peek().map(|l| (l.start, l.end));
        // Release the parser's borrow of `escontext_ref` before re-borrowing it
        // for the error path below (the parser's job is done).
        drop(parser);
        // Re-borrow escontext for the error path below.
        match parsed {
            Some(r) if !aborted && consumed_all => Ok::<_, PgError>(Some(r)),
            _ => {
                // C: on a syntax error bison's generated parser calls
                // jsonpath_yyerror(result, escontext, scanner, "syntax error")
                // (jsonpath_gram.c). If a soft error is already set (e.g. from a
                // makeItem* action), yyerror leaves it intact.
                if !escontext_ref.as_ref().is_some_and(|c| c.error_occurred()) {
                    match err_span {
                        Some((s, e)) if s < e && e <= str.len() => {
                            jsonpath_yyerror_yytext(
                                escontext_ref.as_deref_mut(),
                                &str[s..e],
                                "syntax error",
                            )?;
                        }
                        _ => {
                            jsonpath_yyerror(
                                escontext_ref.as_deref_mut(),
                                str,
                                str.len(),
                                "syntax error",
                            )?;
                        }
                    }
                }
                Ok(None)
            }
        }
    };

    result.map(|opt| opt.flatten())
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_jsonpath::parse::JsonPathParseValue;

    fn parse(s: &str) -> Option<JsonPathParseResult> {
        parsejsonpath(s.as_bytes(), None).expect("hard error")
    }

    #[test]
    fn empty_is_none() {
        assert!(parse("").is_none());
    }

    #[test]
    fn lax_default_strict_keyword() {
        assert!(parse("$").unwrap().lax);
        assert!(parse("lax $").unwrap().lax);
        assert!(!parse("strict $").unwrap().lax);
    }

    #[test]
    fn root_key_accessor() {
        let r = parse("$.a").unwrap();
        let head = r.expr.as_ref().unwrap();
        assert_eq!(head.typ, JsonPathItemType::jpiRoot);
        let next = head.next.as_ref().unwrap();
        assert_eq!(next.typ, JsonPathItemType::jpiKey);
        match &next.value {
            JsonPathParseValue::String(b) => assert_eq!(b, b"a"),
            _ => panic!("expected key string"),
        }
    }

    #[test]
    fn arithmetic_precedence() {
        // 1 + 2 * 3 -> Add(1, Mul(2,3))
        let r = parse("1 + 2 * 3").unwrap();
        let e = r.expr.as_ref().unwrap();
        assert_eq!(e.typ, JsonPathItemType::jpiAdd);
        match &e.value {
            JsonPathParseValue::Args { right, .. } => {
                assert_eq!(right.as_ref().unwrap().typ, JsonPathItemType::jpiMul);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn top_level_exists_predicate() {
        // `exists($)` and `exists($.a)` are valid top-level predicates
        // (predicate -> delimited_predicate -> EXISTS_P '(' expr ')').
        let r = parse("exists($)").unwrap();
        assert_eq!(r.expr.as_ref().unwrap().typ, JsonPathItemType::jpiExists);
        let r = parse("exists($.public)").unwrap();
        assert_eq!(r.expr.as_ref().unwrap().typ, JsonPathItemType::jpiExists);
        // and combined with boolean connectives.
        let r = parse("exists($.a) && exists($.b)").unwrap();
        assert_eq!(r.expr.as_ref().unwrap().typ, JsonPathItemType::jpiAnd);
    }

    #[test]
    fn unary_minus_numeric_fold() {
        // -5 folds into a single jpiNumeric (not a jpiMinus over jpiNumeric).
        let r = parse("-5").unwrap();
        assert_eq!(r.expr.as_ref().unwrap().typ, JsonPathItemType::jpiNumeric);
    }

    #[test]
    fn filter_and_comparison() {
        // $.a ? (@ > 1)
        let r = parse("$.a ? (@ > 1)").unwrap();
        // Walk to the filter node.
        let mut item = r.expr.as_ref().unwrap().as_ref();
        let mut saw_filter = false;
        loop {
            if item.typ == JsonPathItemType::jpiFilter {
                saw_filter = true;
                break;
            }
            match item.next.as_ref() {
                Some(n) => item = n.as_ref(),
                None => break,
            }
        }
        assert!(saw_filter, "expected a jpiFilter in the chain");
    }

    #[test]
    fn array_subscript() {
        let r = parse("$[0 to 2]").unwrap();
        let next = r.expr.as_ref().unwrap().next.as_ref().unwrap();
        assert_eq!(next.typ, JsonPathItemType::jpiIndexArray);
    }

    #[test]
    fn syntax_error_is_none_soft() {
        let mut ctx = SoftErrorContext::new(true);
        let res = parsejsonpath(b"$.", Some(&mut ctx)).expect("no hard error");
        assert!(res.is_none());
        assert!(ctx.error_occurred());
    }

    #[test]
    fn keyword_as_key() {
        // `.size` is the size() method, but `.with` (a keyword) used as a key.
        let r = parse("$.with").unwrap();
        let next = r.expr.as_ref().unwrap().next.as_ref().unwrap();
        assert_eq!(next.typ, JsonPathItemType::jpiKey);
    }
}
