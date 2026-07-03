//! Hand-written recursive-descent equivalent of the bison grammar in
//! jsonpath_gram.y (small, unambiguous under the declared precedences; the
//! reductions and makeItem* actions are mirrored 1:1). Produces the
//! JsonPathParseItem tree the flattener consumes.

use ::mcx::{alloc_in, slice_in, Mcx, PgBox, PgVec};
use ::types_core::DEFAULT_COLLATION_OID;
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use ::types_error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_REGULAR_EXPRESSION, ERRCODE_SYNTAX_ERROR,
};

use crate::path::ItemType;
use crate::scan::{jsonpath_yyerror, jsonpath_yyerror_yytext, Lexeme, Lexer, Token};

pub struct ParseItem<'mcx> {
    pub typ: ItemType,
    pub next: Option<PgBox<'mcx, ParseItem<'mcx>>>,
    pub value: ParseValue<'mcx>,
}

pub enum ParseValue<'mcx> {
    None,
    Args {
        left: Option<PgBox<'mcx, ParseItem<'mcx>>>,
        right: Option<PgBox<'mcx, ParseItem<'mcx>>>,
    },
    Arg(Option<PgBox<'mcx, ParseItem<'mcx>>>),
    Array(PgVec<'mcx, Subscript<'mcx>>),
    AnyBounds {
        first: u32,
        last: u32,
    },
    LikeRegex {
        expr: Option<PgBox<'mcx, ParseItem<'mcx>>>,
        pattern: PgVec<'mcx, u8>,
        flags: u32,
    },
    /// Full on-disk numeric varlena bytes (header included).
    Numeric(PgVec<'mcx, u8>),
    Boolean(bool),
    String(PgVec<'mcx, u8>),
}

pub struct Subscript<'mcx> {
    pub from: Option<PgBox<'mcx, ParseItem<'mcx>>>,
    pub to: Option<PgBox<'mcx, ParseItem<'mcx>>>,
}

pub struct ParseResult<'mcx> {
    pub expr: PgBox<'mcx, ParseItem<'mcx>>,
    pub lax: bool,
}

pub const JSP_REGEX_ICASE: u32 = 0x01;
pub const JSP_REGEX_DOTALL: u32 = 0x02;
pub const JSP_REGEX_MLINE: u32 = 0x04;
pub const JSP_REGEX_WSPACE: u32 = 0x08;
pub const JSP_REGEX_QUOTE: u32 = 0x10;

type Item<'mcx> = PgBox<'mcx, ParseItem<'mcx>>;
type POut<T> = PgResult<Option<T>>;

fn make_item_type<'mcx>(mcx: Mcx<'mcx>, typ: ItemType) -> PgResult<Item<'mcx>> {
    alloc_in(mcx, ParseItem { typ, next: None, value: ParseValue::None })
}

fn make_item_string<'mcx>(mcx: Mcx<'mcx>, s: Option<PgVec<'mcx, u8>>) -> PgResult<Item<'mcx>> {
    match s {
        None => make_item_type(mcx, ItemType::Null),
        Some(s) => {
            let mut v = make_item_type(mcx, ItemType::String)?;
            v.value = ParseValue::String(s);
            Ok(v)
        }
    }
}

fn make_item_variable<'mcx>(mcx: Mcx<'mcx>, s: PgVec<'mcx, u8>) -> PgResult<Item<'mcx>> {
    let mut v = make_item_type(mcx, ItemType::Variable)?;
    v.value = ParseValue::String(s);
    Ok(v)
}

fn make_item_key<'mcx>(mcx: Mcx<'mcx>, s: PgVec<'mcx, u8>) -> PgResult<Item<'mcx>> {
    let mut v = make_item_string(mcx, Some(s))?;
    v.typ = ItemType::Key;
    Ok(v)
}

/// C: numeric_in(s->val, InvalidOid, -1) — hard error, matching the grammar
/// action's DirectFunctionCall3.
fn make_item_numeric<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Item<'mcx>> {
    let text = core::str::from_utf8(s).expect("scanner numerics are ASCII");
    let img = adt_numeric::numeric_in(text, -1, None)?
        .expect("hard numeric_in returns Err, not soft None");
    let mut v = make_item_type(mcx, ItemType::Numeric)?;
    v.value = ParseValue::Numeric(slice_in(mcx, img.as_bytes())?);
    Ok(v)
}

fn make_item_bool<'mcx>(mcx: Mcx<'mcx>, val: bool) -> PgResult<Item<'mcx>> {
    let mut v = make_item_type(mcx, ItemType::Bool)?;
    v.value = ParseValue::Boolean(val);
    Ok(v)
}

fn make_item_binary<'mcx>(
    mcx: Mcx<'mcx>,
    typ: ItemType,
    la: Option<Item<'mcx>>,
    ra: Option<Item<'mcx>>,
) -> PgResult<Item<'mcx>> {
    let mut v = make_item_type(mcx, typ)?;
    v.value = ParseValue::Args { left: la, right: ra };
    Ok(v)
}

/// C: makeItemUnary — folds +/- over a lone numeric literal.
fn make_item_unary<'mcx>(mcx: Mcx<'mcx>, typ: ItemType, a: Item<'mcx>) -> PgResult<Item<'mcx>> {
    if typ == ItemType::Plus && a.typ == ItemType::Numeric && a.next.is_none() {
        return Ok(a);
    }
    if typ == ItemType::Minus && a.typ == ItemType::Numeric && a.next.is_none() {
        let num = match &a.value {
            ParseValue::Numeric(n) => n,
            _ => unreachable!("Numeric item without Numeric value"),
        };
        let negated = adt_numeric::numeric_uminus(adt_numeric::Num::from_payload(&num[4..]));
        let mut v = make_item_type(mcx, ItemType::Numeric)?;
        v.value = ParseValue::Numeric(slice_in(mcx, negated.as_bytes())?);
        return Ok(v);
    }
    let mut v = make_item_type(mcx, typ)?;
    v.value = ParseValue::Arg(Some(a));
    Ok(v)
}

fn make_item_unary_optional<'mcx>(
    mcx: Mcx<'mcx>,
    typ: ItemType,
    arg: Option<Item<'mcx>>,
) -> PgResult<Item<'mcx>> {
    let mut v = make_item_type(mcx, typ)?;
    v.value = ParseValue::Arg(arg);
    Ok(v)
}

/// C: makeItemList — chain the accessor list through ->next.
fn make_item_list<'mcx>(list: PgVec<'mcx, Item<'mcx>>) -> Item<'mcx> {
    debug_assert!(!list.is_empty());
    let mut iter = list.into_iter();
    let mut head = iter.next().unwrap();
    {
        let mut end: &mut ParseItem<'mcx> = &mut head;
        while end.next.is_some() {
            end = end.next.as_mut().unwrap();
        }
        for c in iter {
            end.next = Some(c);
            end = end.next.as_mut().unwrap();
        }
    }
    head
}

fn make_index_array<'mcx>(mcx: Mcx<'mcx>, list: PgVec<'mcx, Item<'mcx>>) -> PgResult<Item<'mcx>> {
    debug_assert!(!list.is_empty());
    let mut elems: PgVec<'mcx, Subscript<'mcx>> = ::mcx::vec_with_capacity_in(mcx, list.len())?;
    for jpi in list {
        debug_assert_eq!(jpi.typ, ItemType::Subscript);
        let inner = ::mcx::box_into_inner(jpi);
        let (from, to) = match inner.value {
            ParseValue::Args { left, right } => (left, right),
            _ => unreachable!("Subscript item without Args value"),
        };
        elems.push(Subscript { from, to });
    }
    let mut v = make_item_type(mcx, ItemType::IndexArray)?;
    v.value = ParseValue::Array(elems);
    Ok(v)
}

fn make_any<'mcx>(mcx: Mcx<'mcx>, first: i32, last: i32) -> PgResult<Item<'mcx>> {
    let mut v = make_item_type(mcx, ItemType::Any)?;
    let f = if first >= 0 { first as u32 } else { u32::MAX };
    let l = if last >= 0 { last as u32 } else { u32::MAX };
    v.value = ParseValue::AnyBounds { first: f, last: l };
    Ok(v)
}

/// One server-encoding character starting the unrecognized flag text
/// (C: errdetail with pg_mblen bytes of the offending flag character).
fn first_char_lossy(rest: &[u8]) -> String {
    match core::str::from_utf8(rest) {
        Ok(s) => s.chars().next().map(String::from).unwrap_or_default(),
        Err(e) if e.valid_up_to() > 0 => {
            let s = core::str::from_utf8(&rest[..e.valid_up_to()]).unwrap();
            s.chars().next().map(String::from).unwrap_or_default()
        }
        Err(_) => String::from_utf8_lossy(&rest[..1]).into_owned(),
    }
}

/// C: makeItemLikeRegex. Ok(None) = soft error recorded (grammar YYABORT).
fn make_item_like_regex<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Option<Item<'mcx>>,
    pattern: PgVec<'mcx, u8>,
    flags: Option<&[u8]>,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> POut<Item<'mcx>> {
    let mut xflags: u32 = 0;
    if let Some(fbytes) = flags {
        for (i, &c) in fbytes.iter().enumerate() {
            match c {
                b'i' => xflags |= JSP_REGEX_ICASE,
                b's' => xflags |= JSP_REGEX_DOTALL,
                b'm' => xflags |= JSP_REGEX_MLINE,
                b'x' => xflags |= JSP_REGEX_WSPACE,
                b'q' => xflags |= JSP_REGEX_QUOTE,
                _ => {
                    return ereturn(
                        escontext.as_deref_mut(),
                        None,
                        PgError::error("invalid input syntax for type jsonpath")
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                            .with_detail(format!(
                                "Unrecognized flag character \"{}\" in LIKE_REGEX predicate.",
                                first_char_lossy(&fbytes[i..])
                            )),
                    );
                }
            }
        }
    }

    let cflags = match jsp_convert_regex_flags(xflags, escontext.as_deref_mut())? {
        Some(c) => c,
        None => return Ok(None),
    };

    // C: validity check only — pg_regcomp + pg_regfree over the wide pattern.
    let wpattern = mbutils::pg_mb2wchar_with_len(mcx, &pattern)?;
    if let Err(e) = regex_core::regex_compile::pg_regcomp(
        mcx,
        &wpattern,
        cflags,
        DEFAULT_COLLATION_OID,
    ) {
        let msg = regex_core::regex_export_free_error::pg_regerror(e.0);
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error(format!("invalid regular expression: {msg}"))
                .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION),
        );
    }

    let mut v = make_item_type(mcx, ItemType::LikeRegex)?;
    v.value = ParseValue::LikeRegex { expr, pattern, flags: xflags };
    Ok(Some(v))
}

/// C: jspConvertRegexFlags (jsonpath_gram.y) — XQuery flag bits to REG_* cflags.
pub fn jsp_convert_regex_flags(
    xflags: u32,
    escontext: Option<&mut SoftErrorContext>,
) -> POut<i32> {
    use regex_core::regex_consts::{REG_ADVANCED, REG_ICASE, REG_NLANCH, REG_NLSTOP, REG_QUOTE};

    let mut cflags: i32 = REG_ADVANCED;
    if xflags & JSP_REGEX_ICASE != 0 {
        cflags |= REG_ICASE;
    }
    // Per XQuery spec, 'q' makes 'm', 's', 'x' ignored.
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
                escontext,
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

struct Parser<'e, 's, 'mcx> {
    mcx: Mcx<'mcx>,
    toks: PgVec<'mcx, Lexeme<'mcx>>,
    idx: usize,
    escontext: &'e mut Option<&'s mut SoftErrorContext>,
    aborted: bool,
}

impl<'e, 's, 'mcx> Parser<'e, 's, 'mcx> {
    fn peek(&self) -> Option<&Lexeme<'mcx>> {
        self.toks.get(self.idx)
    }

    fn peek_tok(&self) -> Option<Token> {
        self.toks.get(self.idx).map(|l| l.token)
    }

    fn at_char(&self, c: u8) -> bool {
        matches!(self.peek_tok(), Some(Token::Char(x)) if x == c)
    }

    fn expect_char(&mut self, c: u8) -> POut<()> {
        if self.at_char(c) {
            self.idx += 1;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    fn expect(&mut self, t: Token) -> POut<()> {
        if self.peek_tok() == Some(t) {
            self.idx += 1;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    fn take_str(&mut self) -> PgVec<'mcx, u8> {
        let mcx = self.mcx;
        let l = &mut self.toks[self.idx];
        self.idx += 1;
        l.value.take().unwrap_or_else(|| PgVec::new_in(mcx))
    }

    fn parse_result(&mut self) -> POut<Option<ParseResult<'mcx>>> {
        // result: /* EMPTY */ -> NULL.
        if self.peek().is_none() {
            return Ok(Some(None));
        }

        let lax = match self.peek_tok() {
            Some(Token::StrictP) => {
                self.idx += 1;
                false
            }
            Some(Token::LaxP) => {
                self.idx += 1;
                true
            }
            _ => true,
        };

        let expr = match self.parse_expr_or_predicate()? {
            Some(e) => e,
            None => return Ok(None),
        };
        if self.aborted {
            return Ok(None);
        }
        if self.peek().is_some() {
            return Ok(None);
        }
        Ok(Some(Some(ParseResult { expr, lax })))
    }

    // expr and predicate share one global precedence stack in bison; one
    // unified precedence climb reproduces the same reductions.
    fn parse_expr_or_predicate(&mut self) -> POut<Item<'mcx>> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> POut<Item<'mcx>> {
        let mut left = match self.parse_and()? {
            Some(v) => v,
            None => return Ok(None),
        };
        while self.peek_tok() == Some(Token::OrP) {
            self.idx += 1;
            let right = match self.parse_and()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(self.mcx, ItemType::Or, Some(left), Some(right))?;
        }
        Ok(Some(left))
    }

    fn parse_and(&mut self) -> POut<Item<'mcx>> {
        let mut left = match self.parse_comparison()? {
            Some(v) => v,
            None => return Ok(None),
        };
        while self.peek_tok() == Some(Token::AndP) {
            self.idx += 1;
            let right = match self.parse_comparison()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(self.mcx, ItemType::And, Some(left), Some(right))?;
        }
        Ok(Some(left))
    }

    fn parse_comparison(&mut self) -> POut<Item<'mcx>> {
        let left = match self.parse_not()? {
            Some(v) => v,
            None => return Ok(None),
        };

        if let Some(op) = self.comp_op() {
            self.idx += 1;
            let right = match self.parse_not()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_binary(self.mcx, op, Some(left), Some(right))?));
        }

        if self.peek_tok() == Some(Token::StartsP) {
            self.idx += 1;
            if self.expect(Token::WithP)?.is_none() {
                return Ok(None);
            }
            let init = match self.parse_starts_with_initial()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_binary(
                self.mcx,
                ItemType::StartsWith,
                Some(left),
                Some(init),
            )?));
        }

        if self.peek_tok() == Some(Token::LikeRegexP) {
            self.idx += 1;
            if self.peek_tok() != Some(Token::StringP) {
                return Ok(None);
            }
            let pattern = self.take_str();
            let flags = if self.peek_tok() == Some(Token::FlagP) {
                self.idx += 1;
                if self.peek_tok() != Some(Token::StringP) {
                    return Ok(None);
                }
                Some(self.take_str())
            } else {
                None
            };
            let res = make_item_like_regex(
                self.mcx,
                Some(left),
                pattern,
                flags.as_deref(),
                self.escontext,
            )?;
            match res {
                Some(v) => return Ok(Some(v)),
                None => {
                    self.aborted = true;
                    return Ok(None);
                }
            }
        }

        Ok(Some(left))
    }

    fn comp_op(&self) -> Option<ItemType> {
        match self.peek_tok()? {
            Token::EqualP => Some(ItemType::Equal),
            Token::NotEqualP => Some(ItemType::NotEqual),
            Token::LessP => Some(ItemType::Less),
            Token::GreaterP => Some(ItemType::Greater),
            Token::LessEqualP => Some(ItemType::LessOrEqual),
            Token::GreaterEqualP => Some(ItemType::GreaterOrEqual),
            _ => None,
        }
    }

    fn parse_not(&mut self) -> POut<Item<'mcx>> {
        if self.peek_tok() == Some(Token::NotP) {
            self.idx += 1;
            let p = match self.parse_delimited_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_unary(self.mcx, ItemType::Not, p)?));
        }
        self.parse_additive()
    }

    fn parse_additive(&mut self) -> POut<Item<'mcx>> {
        let mut left = match self.parse_multiplicative()? {
            Some(v) => v,
            None => return Ok(None),
        };
        loop {
            let op = if self.at_char(b'+') {
                ItemType::Add
            } else if self.at_char(b'-') {
                ItemType::Sub
            } else {
                break;
            };
            self.idx += 1;
            let right = match self.parse_multiplicative()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(self.mcx, op, Some(left), Some(right))?;
        }
        Ok(Some(left))
    }

    fn parse_multiplicative(&mut self) -> POut<Item<'mcx>> {
        let mut left = match self.parse_unary()? {
            Some(v) => v,
            None => return Ok(None),
        };
        loop {
            let op = if self.at_char(b'*') {
                ItemType::Mul
            } else if self.at_char(b'/') {
                ItemType::Div
            } else if self.at_char(b'%') {
                ItemType::Mod
            } else {
                break;
            };
            self.idx += 1;
            let right = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            left = make_item_binary(self.mcx, op, Some(left), Some(right))?;
        }
        Ok(Some(left))
    }

    fn parse_unary(&mut self) -> POut<Item<'mcx>> {
        if self.at_char(b'+') {
            self.idx += 1;
            let e = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_unary(self.mcx, ItemType::Plus, e)?));
        }
        if self.at_char(b'-') {
            self.idx += 1;
            let e = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_unary(self.mcx, ItemType::Minus, e)?));
        }
        self.parse_expr_primary()
    }

    fn parse_expr_primary(&mut self) -> POut<Item<'mcx>> {
        if self.peek_tok() == Some(Token::ExistsP) {
            return self.parse_delimited_predicate();
        }
        if self.at_char(b'(') {
            return self.parse_paren_primary();
        }
        self.parse_accessor_expr()
    }

    fn parse_paren_primary(&mut self) -> POut<Item<'mcx>> {
        self.idx += 1;
        let inner = match self.parse_expr_or_predicate()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.expect_char(b')')?.is_none() {
            return Ok(None);
        }

        // '(' predicate ')' IS_P UNKNOWN_P.
        if self.peek_tok() == Some(Token::IsP) {
            self.idx += 1;
            if self.expect(Token::UnknownP)?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_unary(self.mcx, ItemType::IsUnknown, inner)?));
        }

        // '(' expr ')' accessor_op* — continued as an accessor_expr list.
        if self.at_accessor_op_start() {
            let mut list: PgVec<'mcx, Item<'mcx>> = PgVec::new_in(self.mcx);
            list.push(inner);
            while self.at_accessor_op_start() {
                let op = match self.parse_accessor_op()? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                list.push(op);
            }
            return Ok(Some(make_item_list(list)));
        }

        Ok(Some(inner))
    }

    fn parse_accessor_expr(&mut self) -> POut<Item<'mcx>> {
        let head = match self.parse_path_primary()? {
            Some(v) => v,
            None => return Ok(None),
        };
        let mut list: PgVec<'mcx, Item<'mcx>> = PgVec::new_in(self.mcx);
        list.push(head);
        while self.at_accessor_op_start() {
            let op = match self.parse_accessor_op()? {
                Some(v) => v,
                None => return Ok(None),
            };
            list.push(op);
        }
        Ok(Some(make_item_list(list)))
    }

    fn parse_delimited_predicate(&mut self) -> POut<Item<'mcx>> {
        if self.peek_tok() == Some(Token::ExistsP) {
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
            return Ok(Some(make_item_unary(self.mcx, ItemType::Exists, e)?));
        }
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

    fn parse_starts_with_initial(&mut self) -> POut<Item<'mcx>> {
        match self.peek_tok() {
            Some(Token::StringP) => {
                let s = self.take_str();
                Ok(Some(make_item_string(self.mcx, Some(s))?))
            }
            Some(Token::VariableP) => {
                let s = self.take_str();
                Ok(Some(make_item_variable(self.mcx, s)?))
            }
            _ => Ok(None),
        }
    }

    fn parse_path_primary(&mut self) -> POut<Item<'mcx>> {
        match self.peek_tok() {
            Some(Token::StringP) => {
                let s = self.take_str();
                Ok(Some(make_item_string(self.mcx, Some(s))?))
            }
            Some(Token::NullP) => {
                self.idx += 1;
                Ok(Some(make_item_string(self.mcx, None)?))
            }
            Some(Token::TrueP) => {
                self.idx += 1;
                Ok(Some(make_item_bool(self.mcx, true)?))
            }
            Some(Token::FalseP) => {
                self.idx += 1;
                Ok(Some(make_item_bool(self.mcx, false)?))
            }
            Some(Token::NumericP) | Some(Token::IntP) => {
                let s = self.take_str();
                Ok(Some(make_item_numeric(self.mcx, &s)?))
            }
            Some(Token::VariableP) => {
                let s = self.take_str();
                Ok(Some(make_item_variable(self.mcx, s)?))
            }
            Some(Token::Char(b'$')) => {
                self.idx += 1;
                Ok(Some(make_item_type(self.mcx, ItemType::Root)?))
            }
            Some(Token::Char(b'@')) => {
                self.idx += 1;
                Ok(Some(make_item_type(self.mcx, ItemType::Current)?))
            }
            Some(Token::LastP) => {
                self.idx += 1;
                Ok(Some(make_item_type(self.mcx, ItemType::Last)?))
            }
            _ => Ok(None),
        }
    }

    fn at_accessor_op_start(&self) -> bool {
        self.at_char(b'.') || self.at_char(b'[') || self.at_char(b'?')
    }

    fn parse_accessor_op(&mut self) -> POut<Item<'mcx>> {
        if self.at_char(b'[') {
            return self.parse_array_accessor();
        }

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
            return Ok(Some(make_item_unary(self.mcx, ItemType::Filter, p)?));
        }

        if self.expect_char(b'.')?.is_none() {
            return Ok(None);
        }

        if self.at_char(b'*') {
            self.idx += 1;
            return Ok(Some(make_item_type(self.mcx, ItemType::AnyKey)?));
        }

        if self.peek_tok() == Some(Token::AnyP) {
            return self.parse_any_path();
        }

        if let Some(m) = self.method_optype() {
            self.idx += 1;
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_type(self.mcx, m)?));
        }

        if self.peek_tok() == Some(Token::DecimalP) {
            return self.parse_decimal_accessor();
        }

        if self.peek_tok() == Some(Token::DatetimeP) {
            self.idx += 1;
            let arg = match self.parse_paren_opt_datetime_template()? {
                Some(a) => a,
                None => return Ok(None),
            };
            return Ok(Some(make_item_unary_optional(self.mcx, ItemType::Datetime, arg)?));
        }

        let dt = match self.peek_tok() {
            Some(Token::TimeP) => Some(ItemType::Time),
            Some(Token::TimeTzP) => Some(ItemType::TimeTz),
            Some(Token::TimestampP) => Some(ItemType::Timestamp),
            Some(Token::TimestampTzP) => Some(ItemType::TimestampTz),
            _ => None,
        };
        if let Some(dt) = dt {
            self.idx += 1;
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            let arg = if self.peek_tok() == Some(Token::IntP) {
                let s = self.take_str();
                Some(make_item_numeric(self.mcx, &s)?)
            } else {
                None
            };
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_unary_optional(self.mcx, dt, arg)?));
        }

        if let Some(s) = self.try_key_name() {
            return Ok(Some(make_item_key(self.mcx, s)?));
        }

        Ok(None)
    }

    fn parse_array_accessor(&mut self) -> POut<Item<'mcx>> {
        self.idx += 1;
        if self.at_char(b'*') {
            self.idx += 1;
            if self.expect_char(b']')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_type(self.mcx, ItemType::AnyArray)?));
        }

        let mut list: PgVec<'mcx, Item<'mcx>> = PgVec::new_in(self.mcx);
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
        Ok(Some(make_index_array(self.mcx, list)?))
    }

    fn parse_index_elem(&mut self) -> POut<Item<'mcx>> {
        let from = match self.parse_expr_or_predicate()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.peek_tok() == Some(Token::ToP) {
            self.idx += 1;
            let to = match self.parse_expr_or_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some(make_item_binary(
                self.mcx,
                ItemType::Subscript,
                Some(from),
                Some(to),
            )?));
        }
        Ok(Some(make_item_binary(self.mcx, ItemType::Subscript, Some(from), None)?))
    }

    fn parse_any_path(&mut self) -> POut<Item<'mcx>> {
        self.idx += 1;
        if !self.at_char(b'{') {
            return Ok(Some(make_any(self.mcx, 0, -1)?));
        }
        self.idx += 1;
        let first = match self.parse_any_level()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.peek_tok() == Some(Token::ToP) {
            self.idx += 1;
            let last = match self.parse_any_level()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if self.expect_char(b'}')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_any(self.mcx, first, last)?));
        }
        if self.expect_char(b'}')?.is_none() {
            return Ok(None);
        }
        Ok(Some(make_any(self.mcx, first, first)?))
    }

    fn parse_any_level(&mut self) -> POut<i32> {
        match self.peek_tok() {
            Some(Token::IntP) => {
                let s = self.take_str();
                let text = core::str::from_utf8(&s).expect("scanner ints are ASCII");
                let n = numutils::pg_strtoint32(text)?;
                Ok(Some(n))
            }
            Some(Token::LastP) => {
                self.idx += 1;
                Ok(Some(-1))
            }
            _ => Ok(None),
        }
    }

    fn parse_decimal_accessor(&mut self) -> POut<Item<'mcx>> {
        self.idx += 1;
        if self.expect_char(b'(')?.is_none() {
            return Ok(None);
        }
        let mut list: PgVec<'mcx, Item<'mcx>> = PgVec::new_in(self.mcx);
        if !self.at_char(b')') {
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

        match list.len() {
            0 => Ok(Some(make_item_binary(self.mcx, ItemType::Decimal, None, None)?)),
            1 => {
                let a = list.pop();
                Ok(Some(make_item_binary(self.mcx, ItemType::Decimal, a, None)?))
            }
            2 => {
                let b = list.pop();
                let a = list.pop();
                Ok(Some(make_item_binary(self.mcx, ItemType::Decimal, a, b)?))
            }
            _ => {
                let r: POut<Item<'mcx>> = ereturn(
                    self.escontext.as_deref_mut(),
                    None,
                    PgError::error("invalid input syntax for type jsonpath")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                        .with_detail(".decimal() can only have an optional precision[,scale]."),
                );
                self.aborted = true;
                r
            }
        }
    }

    fn parse_csv_elem(&mut self) -> POut<Item<'mcx>> {
        if self.at_char(b'+') {
            self.idx += 1;
            if self.peek_tok() != Some(Token::IntP) {
                return Ok(None);
            }
            let s = self.take_str();
            let num = make_item_numeric(self.mcx, &s)?;
            return Ok(Some(make_item_unary(self.mcx, ItemType::Plus, num)?));
        }
        if self.at_char(b'-') {
            self.idx += 1;
            if self.peek_tok() != Some(Token::IntP) {
                return Ok(None);
            }
            let s = self.take_str();
            let num = make_item_numeric(self.mcx, &s)?;
            return Ok(Some(make_item_unary(self.mcx, ItemType::Minus, num)?));
        }
        if self.peek_tok() == Some(Token::IntP) {
            let s = self.take_str();
            return Ok(Some(make_item_numeric(self.mcx, &s)?));
        }
        Ok(None)
    }

    fn parse_paren_opt_datetime_template(&mut self) -> POut<Option<Item<'mcx>>> {
        if self.expect_char(b'(')?.is_none() {
            return Ok(None);
        }
        let arg = if self.peek_tok() == Some(Token::StringP) {
            let s = self.take_str();
            Some(make_item_string(self.mcx, Some(s))?)
        } else {
            None
        };
        if self.expect_char(b')')?.is_none() {
            return Ok(None);
        }
        Ok(Some(arg))
    }

    fn method_optype(&self) -> Option<ItemType> {
        match self.peek_tok()? {
            Token::AbsP => Some(ItemType::Abs),
            Token::SizeP => Some(ItemType::Size),
            Token::TypeP => Some(ItemType::Type),
            Token::FloorP => Some(ItemType::Floor),
            Token::DoubleP => Some(ItemType::Double),
            Token::CeilingP => Some(ItemType::Ceiling),
            Token::KeyValueP => Some(ItemType::KeyValue),
            Token::BigintP => Some(ItemType::Bigint),
            Token::BooleanP => Some(ItemType::Boolean),
            Token::DateP => Some(ItemType::Date),
            Token::IntegerP => Some(ItemType::Integer),
            Token::NumberP => Some(ItemType::Number),
            Token::StringFuncP => Some(ItemType::StringFunc),
            _ => None,
        }
    }

    fn try_key_name(&mut self) -> Option<PgVec<'mcx, u8>> {
        let tok = self.peek_tok()?;
        let is_key_name = matches!(
            tok,
            Token::IdentP
                | Token::StringP
                | Token::ToP
                | Token::NullP
                | Token::TrueP
                | Token::FalseP
                | Token::IsP
                | Token::UnknownP
                | Token::ExistsP
                | Token::StrictP
                | Token::LaxP
                | Token::AbsP
                | Token::SizeP
                | Token::TypeP
                | Token::FloorP
                | Token::DoubleP
                | Token::CeilingP
                | Token::DatetimeP
                | Token::KeyValueP
                | Token::LastP
                | Token::StartsP
                | Token::WithP
                | Token::LikeRegexP
                | Token::FlagP
                | Token::BigintP
                | Token::BooleanP
                | Token::DateP
                | Token::DecimalP
                | Token::IntegerP
                | Token::NumberP
                | Token::StringFuncP
                | Token::TimeP
                | Token::TimeTzP
                | Token::TimestampP
                | Token::TimestampTzP
        );
        if !is_key_name {
            return None;
        }
        Some(self.take_str())
    }
}

/// C: parsejsonpath (jsonpath_scan.l) — scan, parse, and on a bison syntax
/// error report through jsonpath_yyerror. Ok(None) = empty input / soft error.
pub fn parsejsonpath<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<ParseResult<'mcx>>> {
    let mut lexer = Lexer::new(mcx, str);
    let mut toks: PgVec<'mcx, Lexeme<'mcx>> = PgVec::new_in(mcx);
    loop {
        match lexer.next_token(&mut escontext)? {
            Some(lex) => toks.push(lex),
            None => break,
        }
    }

    if escontext.as_ref().is_some_and(|c| c.error_occurred()) {
        return Ok(None);
    }

    let mut escontext_ref = escontext;
    let mut parser = Parser {
        mcx,
        toks,
        idx: 0,
        escontext: &mut escontext_ref,
        aborted: false,
    };
    let parsed = parser.parse_result()?;
    let aborted = parser.aborted;
    let consumed_all = parser.peek().is_none();
    // The rejected lookahead's byte span is bison's yytext for the
    // "syntax error at or near" clause; none at end of input.
    let err_span: Option<(usize, usize)> = parser.peek().map(|l| (l.start, l.end));
    drop(parser);

    match parsed {
        Some(r) if !aborted && consumed_all => Ok(r),
        _ => {
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
}
