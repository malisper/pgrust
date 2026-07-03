//! Hand-written recursive-descent equivalent of the bison grammar in
//! jsonpath_gram.y (small, unambiguous under the declared precedences; the
//! reductions and makeItem* actions are mirrored 1:1). Produces the
//! JsonPathParseItem tree the flattener consumes. Nodes are leaked into the
//! caller's mcx (C's palloc model, bulk-freed at context reset), so the tree
//! is drop-free.

use core::cell::Cell;

use ::mcx::{alloc_in, leak_in, slice_in, Mcx, PgVec};
use ::types_core::DEFAULT_COLLATION_OID;
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use ::types_error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_REGULAR_EXPRESSION, ERRCODE_SYNTAX_ERROR,
};

use crate::path::ItemType;
use crate::scan::{jsonpath_yyerror, jsonpath_yyerror_yytext, Lexeme, Lexer, Token};

pub struct ParseItem<'mcx> {
    pub typ: ItemType,
    pub next: Cell<Option<&'mcx ParseItem<'mcx>>>,
    pub value: ParseValue<'mcx>,
}

#[derive(Clone, Copy)]
pub enum ParseValue<'mcx> {
    None,
    Args {
        left: Option<&'mcx ParseItem<'mcx>>,
        right: Option<&'mcx ParseItem<'mcx>>,
    },
    Arg(Option<&'mcx ParseItem<'mcx>>),
    Array(&'mcx [Subscript<'mcx>]),
    AnyBounds {
        first: u32,
        last: u32,
    },
    LikeRegex {
        expr: Option<&'mcx ParseItem<'mcx>>,
        pattern: &'mcx [u8],
        flags: u32,
    },
    /// Full on-disk numeric varlena bytes (header included).
    Numeric(&'mcx [u8]),
    Boolean(bool),
    String(&'mcx [u8]),
}

#[derive(Clone, Copy)]
pub struct Subscript<'mcx> {
    pub from: Option<&'mcx ParseItem<'mcx>>,
    pub to: Option<&'mcx ParseItem<'mcx>>,
}

pub struct ParseResult<'mcx> {
    pub expr: &'mcx ParseItem<'mcx>,
    pub lax: bool,
}

pub const JSP_REGEX_ICASE: u32 = 0x01;
pub const JSP_REGEX_DOTALL: u32 = 0x02;
pub const JSP_REGEX_MLINE: u32 = 0x04;
pub const JSP_REGEX_WSPACE: u32 = 0x08;
pub const JSP_REGEX_QUOTE: u32 = 0x10;

type Item<'mcx> = &'mcx ParseItem<'mcx>;
type POut<T> = PgResult<Option<T>>;

fn make_item<'mcx>(
    mcx: Mcx<'mcx>,
    typ: ItemType,
    value: ParseValue<'mcx>,
) -> PgResult<Item<'mcx>> {
    Ok(leak_in(alloc_in(
        mcx,
        ParseItem { typ, next: Cell::new(None), value },
    )?))
}

fn make_item_type<'mcx>(mcx: Mcx<'mcx>, typ: ItemType) -> PgResult<Item<'mcx>> {
    make_item(mcx, typ, ParseValue::None)
}

fn make_item_string<'mcx>(mcx: Mcx<'mcx>, s: Option<&'mcx [u8]>) -> PgResult<Item<'mcx>> {
    match s {
        None => make_item_type(mcx, ItemType::Null),
        Some(s) => make_item(mcx, ItemType::String, ParseValue::String(s)),
    }
}

fn make_item_variable<'mcx>(mcx: Mcx<'mcx>, s: &'mcx [u8]) -> PgResult<Item<'mcx>> {
    make_item(mcx, ItemType::Variable, ParseValue::String(s))
}

fn make_item_key<'mcx>(mcx: Mcx<'mcx>, s: &'mcx [u8]) -> PgResult<Item<'mcx>> {
    make_item(mcx, ItemType::Key, ParseValue::String(s))
}

/// C: numeric_in(s->val, InvalidOid, -1) — hard error, matching the grammar
/// action's DirectFunctionCall3.
fn make_item_numeric<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Item<'mcx>> {
    let text = core::str::from_utf8(s).expect("scanner numerics are ASCII");
    let img = adt_numeric::numeric_in(text, -1, None)?
        .expect("hard numeric_in returns Err, not soft None");
    let bytes = slice_in(mcx, img.as_bytes())?.leak();
    make_item(mcx, ItemType::Numeric, ParseValue::Numeric(bytes))
}

fn make_item_bool<'mcx>(mcx: Mcx<'mcx>, val: bool) -> PgResult<Item<'mcx>> {
    make_item(mcx, ItemType::Bool, ParseValue::Boolean(val))
}

fn make_item_binary<'mcx>(
    mcx: Mcx<'mcx>,
    typ: ItemType,
    la: Option<Item<'mcx>>,
    ra: Option<Item<'mcx>>,
) -> PgResult<Item<'mcx>> {
    make_item(mcx, typ, ParseValue::Args { left: la, right: ra })
}

/// C: makeItemUnary — folds +/- over a lone numeric literal.
fn make_item_unary<'mcx>(mcx: Mcx<'mcx>, typ: ItemType, a: Item<'mcx>) -> PgResult<Item<'mcx>> {
    if typ == ItemType::Plus && a.typ == ItemType::Numeric && a.next.get().is_none() {
        return Ok(a);
    }
    if typ == ItemType::Minus && a.typ == ItemType::Numeric && a.next.get().is_none() {
        let num = match a.value {
            ParseValue::Numeric(n) => n,
            _ => unreachable!("Numeric item without Numeric value"),
        };
        let negated = adt_numeric::numeric_uminus(adt_numeric::Num::from_payload(&num[4..]));
        let bytes = slice_in(mcx, negated.as_bytes())?.leak();
        return make_item(mcx, ItemType::Numeric, ParseValue::Numeric(bytes));
    }
    make_item(mcx, typ, ParseValue::Arg(Some(a)))
}

fn make_item_unary_optional<'mcx>(
    mcx: Mcx<'mcx>,
    typ: ItemType,
    arg: Option<Item<'mcx>>,
) -> PgResult<Item<'mcx>> {
    make_item(mcx, typ, ParseValue::Arg(arg))
}

/// C: makeItemList — chain the accessor list through ->next.
fn make_item_list<'mcx>(list: &[Item<'mcx>]) -> Item<'mcx> {
    debug_assert!(!list.is_empty());
    let head = list[0];
    let mut end = head;
    while let Some(n) = end.next.get() {
        end = n;
    }
    for &c in &list[1..] {
        end.next.set(Some(c));
        end = c;
    }
    head
}

fn make_index_array<'mcx>(mcx: Mcx<'mcx>, list: PgVec<'mcx, Item<'mcx>>) -> PgResult<Item<'mcx>> {
    debug_assert!(!list.is_empty());
    let mut elems: PgVec<'mcx, Subscript<'mcx>> = ::mcx::vec_with_capacity_in(mcx, list.len())?;
    for jpi in list.iter() {
        debug_assert_eq!(jpi.typ, ItemType::Subscript);
        let (from, to) = match jpi.value {
            ParseValue::Args { left, right } => (left, right),
            _ => unreachable!("Subscript item without Args value"),
        };
        elems.push(Subscript { from, to });
    }
    make_item(mcx, ItemType::IndexArray, ParseValue::Array(elems.leak()))
}

fn make_any<'mcx>(mcx: Mcx<'mcx>, first: i32, last: i32) -> PgResult<Item<'mcx>> {
    let f = if first >= 0 { first as u32 } else { u32::MAX };
    let l = if last >= 0 { last as u32 } else { u32::MAX };
    make_item(mcx, ItemType::Any, ParseValue::AnyBounds { first: f, last: l })
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
    pattern: &'mcx [u8],
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
    let wpattern = mbutils::pg_mb2wchar_with_len(mcx, pattern)?;
    if let Err(e) =
        regex_core::regex_compile::pg_regcomp(mcx, &wpattern, cflags, DEFAULT_COLLATION_OID)
    {
        let msg = regex_core::regex_export_free_error::pg_regerror(e.0);
        return ereturn(
            escontext.as_deref_mut(),
            None,
            PgError::error(format!("invalid regular expression: {msg}"))
                .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION),
        );
    }

    Ok(Some(make_item(
        mcx,
        ItemType::LikeRegex,
        ParseValue::LikeRegex { expr, pattern, flags: xflags },
    )?))
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

/// Syntactic class of a completed subparse — bison's expr vs predicate
/// nonterminals. Not derivable from the head item type ('(' predicate ')'
/// followed by an accessor_op is an expr).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Expr,
    Pred,
}

use Kind::{Expr, Pred};

struct Parser<'a, 'e, 's, 'mcx> {
    mcx: Mcx<'mcx>,
    lexer: Lexer<'a, 'mcx>,
    /// One-token lookahead (bison's), pulled lazily so scanner errors fire in
    /// C's order. None = not fetched; Some(None) = end of token stream.
    lookahead: Option<Option<Lexeme<'mcx>>>,
    escontext: &'e mut Option<&'s mut SoftErrorContext>,
    aborted: bool,
}

type PK<'mcx> = (Item<'mcx>, Kind);

impl<'a, 'e, 's, 'mcx> Parser<'a, 'e, 's, 'mcx> {
    fn fill(&mut self) -> PgResult<()> {
        if self.lookahead.is_none() {
            self.lookahead = Some(self.lexer.next_token(self.escontext)?);
        }
        Ok(())
    }

    fn peek_tok(&mut self) -> PgResult<Option<Token>> {
        self.fill()?;
        Ok(self.lookahead.as_ref().unwrap().as_ref().map(|l| l.token))
    }

    fn at_eof(&mut self) -> PgResult<bool> {
        Ok(self.peek_tok()?.is_none())
    }

    fn at_char(&mut self, c: u8) -> PgResult<bool> {
        Ok(matches!(self.peek_tok()?, Some(Token::Char(x)) if x == c))
    }

    fn advance(&mut self) {
        debug_assert!(matches!(self.lookahead, Some(Some(_))));
        self.lookahead = None;
    }

    fn expect_char(&mut self, c: u8) -> POut<()> {
        if self.at_char(c)? {
            self.advance();
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    fn expect(&mut self, t: Token) -> POut<()> {
        if self.peek_tok()? == Some(t) {
            self.advance();
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    fn take_str(&mut self) -> &'mcx [u8] {
        let l = self
            .lookahead
            .as_mut()
            .expect("take_str without lookahead")
            .as_mut()
            .expect("take_str at end of stream");
        let v = l.value.take().unwrap_or(&[]);
        self.lookahead = None;
        v
    }

    /// Byte span of the current (fetched) lookahead — bison's yytext at the
    /// point of a syntax error.
    fn current_span(&self) -> Option<(usize, usize)> {
        match &self.lookahead {
            Some(Some(l)) => Some((l.start, l.end)),
            _ => None,
        }
    }

    fn parse_result(&mut self) -> POut<Option<ParseResult<'mcx>>> {
        // result: /* EMPTY */ -> NULL.
        if self.at_eof()? {
            return Ok(Some(None));
        }

        let lax = match self.peek_tok()? {
            Some(Token::StrictP) => {
                self.advance();
                false
            }
            Some(Token::LaxP) => {
                self.advance();
                true
            }
            _ => true,
        };

        let (expr, _kind) = match self.parse_or()? {
            Some(e) => e,
            None => return Ok(None),
        };
        if self.aborted {
            return Ok(None);
        }
        if !self.at_eof()? {
            return Ok(None);
        }
        Ok(Some(Some(ParseResult { expr, lax })))
    }

    // expr_or_predicate — the full climb; Kind carries which nonterminal the
    // subparse completed as, and each operator arm enforces bison's
    // expr-vs-predicate operand classes (wrong class = stop at the operator
    // token, exactly where the LALR tables error).

    fn parse_or(&mut self) -> POut<PK<'mcx>> {
        let (mut left, mut lkind) = match self.parse_and()? {
            Some(v) => v,
            None => return Ok(None),
        };
        while self.peek_tok()? == Some(Token::OrP) {
            if lkind != Pred {
                break;
            }
            self.advance();
            let (right, rkind) = match self.parse_and()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if rkind != Pred {
                return Ok(None);
            }
            left = make_item_binary(self.mcx, ItemType::Or, Some(left), Some(right))?;
            lkind = Pred;
        }
        Ok(Some((left, lkind)))
    }

    fn parse_and(&mut self) -> POut<PK<'mcx>> {
        let (mut left, mut lkind) = match self.parse_comparison()? {
            Some(v) => v,
            None => return Ok(None),
        };
        while self.peek_tok()? == Some(Token::AndP) {
            if lkind != Pred {
                break;
            }
            self.advance();
            let (right, rkind) = match self.parse_comparison()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if rkind != Pred {
                return Ok(None);
            }
            left = make_item_binary(self.mcx, ItemType::And, Some(left), Some(right))?;
            lkind = Pred;
        }
        Ok(Some((left, lkind)))
    }

    fn parse_comparison(&mut self) -> POut<PK<'mcx>> {
        let (left, lkind) = match self.parse_not()? {
            Some(v) => v,
            None => return Ok(None),
        };

        if lkind == Expr {
            if let Some(op) = self.comp_op()? {
                self.advance();
                let (right, rkind) = match self.parse_additive()? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                if rkind != Expr {
                    return Ok(None);
                }
                return Ok(Some((
                    make_item_binary(self.mcx, op, Some(left), Some(right))?,
                    Pred,
                )));
            }

            if self.peek_tok()? == Some(Token::StartsP) {
                self.advance();
                if self.expect(Token::WithP)?.is_none() {
                    return Ok(None);
                }
                let init = match self.parse_starts_with_initial()? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                return Ok(Some((
                    make_item_binary(self.mcx, ItemType::StartsWith, Some(left), Some(init))?,
                    Pred,
                )));
            }

            if self.peek_tok()? == Some(Token::LikeRegexP) {
                self.advance();
                if self.peek_tok()? != Some(Token::StringP) {
                    return Ok(None);
                }
                let pattern = self.take_str();
                let flags = if self.peek_tok()? == Some(Token::FlagP) {
                    self.advance();
                    if self.peek_tok()? != Some(Token::StringP) {
                        return Ok(None);
                    }
                    Some(self.take_str())
                } else {
                    None
                };
                let res =
                    make_item_like_regex(self.mcx, Some(left), pattern, flags, self.escontext)?;
                match res {
                    Some(v) => return Ok(Some((v, Pred))),
                    None => {
                        self.aborted = true;
                        return Ok(None);
                    }
                }
            }
        }

        Ok(Some((left, lkind)))
    }

    fn comp_op(&mut self) -> PgResult<Option<ItemType>> {
        Ok(match self.peek_tok()? {
            Some(Token::EqualP) => Some(ItemType::Equal),
            Some(Token::NotEqualP) => Some(ItemType::NotEqual),
            Some(Token::LessP) => Some(ItemType::Less),
            Some(Token::GreaterP) => Some(ItemType::Greater),
            Some(Token::LessEqualP) => Some(ItemType::LessOrEqual),
            Some(Token::GreaterEqualP) => Some(ItemType::GreaterOrEqual),
            _ => None,
        })
    }

    fn parse_not(&mut self) -> POut<PK<'mcx>> {
        if self.peek_tok()? == Some(Token::NotP) {
            self.advance();
            let p = match self.parse_delimited_predicate()? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some((make_item_unary(self.mcx, ItemType::Not, p)?, Pred)));
        }
        self.parse_additive()
    }

    fn parse_additive(&mut self) -> POut<PK<'mcx>> {
        let (mut left, mut lkind) = match self.parse_multiplicative()? {
            Some(v) => v,
            None => return Ok(None),
        };
        loop {
            let op = if self.at_char(b'+')? {
                ItemType::Add
            } else if self.at_char(b'-')? {
                ItemType::Sub
            } else {
                break;
            };
            if lkind != Expr {
                break;
            }
            self.advance();
            let (right, rkind) = match self.parse_multiplicative()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if rkind != Expr {
                return Ok(None);
            }
            left = make_item_binary(self.mcx, op, Some(left), Some(right))?;
            lkind = Expr;
        }
        Ok(Some((left, lkind)))
    }

    fn parse_multiplicative(&mut self) -> POut<PK<'mcx>> {
        let (mut left, mut lkind) = match self.parse_unary()? {
            Some(v) => v,
            None => return Ok(None),
        };
        loop {
            let op = if self.at_char(b'*')? {
                ItemType::Mul
            } else if self.at_char(b'/')? {
                ItemType::Div
            } else if self.at_char(b'%')? {
                ItemType::Mod
            } else {
                break;
            };
            if lkind != Expr {
                break;
            }
            self.advance();
            let (right, rkind) = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if rkind != Expr {
                return Ok(None);
            }
            left = make_item_binary(self.mcx, op, Some(left), Some(right))?;
            lkind = Expr;
        }
        Ok(Some((left, lkind)))
    }

    fn parse_unary(&mut self) -> POut<PK<'mcx>> {
        if self.at_char(b'+')? {
            self.advance();
            let (e, k) = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if k != Expr {
                return Ok(None);
            }
            return Ok(Some((make_item_unary(self.mcx, ItemType::Plus, e)?, Expr)));
        }
        if self.at_char(b'-')? {
            self.advance();
            let (e, k) = match self.parse_unary()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if k != Expr {
                return Ok(None);
            }
            return Ok(Some((make_item_unary(self.mcx, ItemType::Minus, e)?, Expr)));
        }
        self.parse_expr_primary()
    }

    fn parse_expr_primary(&mut self) -> POut<PK<'mcx>> {
        if self.peek_tok()? == Some(Token::ExistsP) {
            return Ok(self.parse_delimited_predicate()?.map(|p| (p, Pred)));
        }
        if self.at_char(b'(')? {
            return self.parse_paren_primary();
        }
        Ok(self.parse_accessor_expr()?.map(|e| (e, Expr)))
    }

    fn parse_paren_primary(&mut self) -> POut<PK<'mcx>> {
        self.advance();
        let (inner, kind) = match self.parse_or()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.expect_char(b')')?.is_none() {
            return Ok(None);
        }

        // '(' predicate ')' IS_P UNKNOWN_P.
        if self.peek_tok()? == Some(Token::IsP) {
            if kind != Pred {
                return Ok(None);
            }
            self.advance();
            if self.expect(Token::UnknownP)?.is_none() {
                return Ok(None);
            }
            return Ok(Some((
                make_item_unary(self.mcx, ItemType::IsUnknown, inner)?,
                Pred,
            )));
        }

        // '(' expr|predicate ')' accessor_op* — an accessor_expr (expr).
        if self.at_accessor_op_start()? {
            let mut list: PgVec<'mcx, Item<'mcx>> = PgVec::new_in(self.mcx);
            list.push(inner);
            while self.at_accessor_op_start()? {
                let op = match self.parse_accessor_op()? {
                    Some(v) => v,
                    None => return Ok(None),
                };
                list.push(op);
            }
            return Ok(Some((make_item_list(&list), Expr)));
        }

        Ok(Some((inner, kind)))
    }

    fn parse_accessor_expr(&mut self) -> POut<Item<'mcx>> {
        let head = match self.parse_path_primary()? {
            Some(v) => v,
            None => return Ok(None),
        };
        let mut list: PgVec<'mcx, Item<'mcx>> = PgVec::new_in(self.mcx);
        list.push(head);
        while self.at_accessor_op_start()? {
            let op = match self.parse_accessor_op()? {
                Some(v) => v,
                None => return Ok(None),
            };
            list.push(op);
        }
        Ok(Some(make_item_list(&list)))
    }

    fn parse_delimited_predicate(&mut self) -> POut<Item<'mcx>> {
        // EXISTS_P '(' expr ')' — expr only.
        if self.peek_tok()? == Some(Token::ExistsP) {
            self.advance();
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            let (e, k) = match self.parse_additive()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if k != Expr {
                return Ok(None);
            }
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_unary(self.mcx, ItemType::Exists, e)?));
        }
        // '(' predicate ')'.
        if self.at_char(b'(')? {
            self.advance();
            let (p, k) = match self.parse_or()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if k != Pred {
                return Ok(None);
            }
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(p));
        }
        Ok(None)
    }

    fn parse_starts_with_initial(&mut self) -> POut<Item<'mcx>> {
        match self.peek_tok()? {
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
        match self.peek_tok()? {
            Some(Token::StringP) => {
                let s = self.take_str();
                Ok(Some(make_item_string(self.mcx, Some(s))?))
            }
            Some(Token::NullP) => {
                self.advance();
                Ok(Some(make_item_string(self.mcx, None)?))
            }
            Some(Token::TrueP) => {
                self.advance();
                Ok(Some(make_item_bool(self.mcx, true)?))
            }
            Some(Token::FalseP) => {
                self.advance();
                Ok(Some(make_item_bool(self.mcx, false)?))
            }
            Some(Token::NumericP) | Some(Token::IntP) => {
                let s = self.take_str();
                Ok(Some(make_item_numeric(self.mcx, s)?))
            }
            Some(Token::VariableP) => {
                let s = self.take_str();
                Ok(Some(make_item_variable(self.mcx, s)?))
            }
            Some(Token::Char(b'$')) => {
                self.advance();
                Ok(Some(make_item_type(self.mcx, ItemType::Root)?))
            }
            Some(Token::Char(b'@')) => {
                self.advance();
                Ok(Some(make_item_type(self.mcx, ItemType::Current)?))
            }
            Some(Token::LastP) => {
                self.advance();
                Ok(Some(make_item_type(self.mcx, ItemType::Last)?))
            }
            _ => Ok(None),
        }
    }

    fn at_accessor_op_start(&mut self) -> PgResult<bool> {
        Ok(self.at_char(b'.')? || self.at_char(b'[')? || self.at_char(b'?')?)
    }

    fn parse_accessor_op(&mut self) -> POut<Item<'mcx>> {
        if self.at_char(b'[')? {
            return self.parse_array_accessor();
        }

        // '?' '(' predicate ')' -> Filter.
        if self.at_char(b'?')? {
            self.advance();
            if self.expect_char(b'(')?.is_none() {
                return Ok(None);
            }
            let (p, k) = match self.parse_or()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if k != Pred {
                return Ok(None);
            }
            if self.expect_char(b')')?.is_none() {
                return Ok(None);
            }
            return Ok(Some(make_item_unary(self.mcx, ItemType::Filter, p)?));
        }

        if self.expect_char(b'.')?.is_none() {
            return Ok(None);
        }

        if self.at_char(b'*')? {
            self.advance();
            return Ok(Some(make_item_type(self.mcx, ItemType::AnyKey)?));
        }

        if self.peek_tok()? == Some(Token::AnyP) {
            return self.parse_any_path();
        }

        // Method-form keywords: LALR shifts the keyword, then '(' selects the
        // method production; any other lookahead reduces key_name.
        let tok = self.peek_tok()?;
        if let Some(tok) = tok {
            if let Some(m) = method_optype(tok) {
                let s = self.take_str();
                if !self.at_char(b'(')? {
                    return Ok(Some(make_item_key(self.mcx, s)?));
                }
                self.advance();
                if self.expect_char(b')')?.is_none() {
                    return Ok(None);
                }
                return Ok(Some(make_item_type(self.mcx, m)?));
            }

            if tok == Token::DecimalP {
                let s = self.take_str();
                if !self.at_char(b'(')? {
                    return Ok(Some(make_item_key(self.mcx, s)?));
                }
                return self.parse_decimal_args();
            }

            if tok == Token::DatetimeP {
                let s = self.take_str();
                if !self.at_char(b'(')? {
                    return Ok(Some(make_item_key(self.mcx, s)?));
                }
                self.advance();
                // opt_datetime_template: STRING_P | empty.
                let arg = if self.peek_tok()? == Some(Token::StringP) {
                    let t = self.take_str();
                    Some(make_item_string(self.mcx, Some(t))?)
                } else {
                    None
                };
                if self.expect_char(b')')?.is_none() {
                    return Ok(None);
                }
                return Ok(Some(make_item_unary_optional(
                    self.mcx,
                    ItemType::Datetime,
                    arg,
                )?));
            }

            let dt = match tok {
                Token::TimeP => Some(ItemType::Time),
                Token::TimeTzP => Some(ItemType::TimeTz),
                Token::TimestampP => Some(ItemType::Timestamp),
                Token::TimestampTzP => Some(ItemType::TimestampTz),
                _ => None,
            };
            if let Some(dt) = dt {
                let s = self.take_str();
                if !self.at_char(b'(')? {
                    return Ok(Some(make_item_key(self.mcx, s)?));
                }
                self.advance();
                // opt_datetime_precision: INT_P | empty.
                let arg = if self.peek_tok()? == Some(Token::IntP) {
                    let t = self.take_str();
                    Some(make_item_numeric(self.mcx, t)?)
                } else {
                    None
                };
                if self.expect_char(b')')?.is_none() {
                    return Ok(None);
                }
                return Ok(Some(make_item_unary_optional(self.mcx, dt, arg)?));
            }
        }

        if let Some(s) = self.try_key_name()? {
            return Ok(Some(make_item_key(self.mcx, s)?));
        }

        Ok(None)
    }

    fn parse_array_accessor(&mut self) -> POut<Item<'mcx>> {
        self.advance();
        if self.at_char(b'*')? {
            self.advance();
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
        while self.at_char(b',')? {
            self.advance();
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

    /// index_elem: expr | expr TO_P expr — expr only.
    fn parse_index_elem(&mut self) -> POut<Item<'mcx>> {
        let (from, fk) = match self.parse_additive()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if fk != Expr {
            return Ok(None);
        }
        if self.peek_tok()? == Some(Token::ToP) {
            self.advance();
            let (to, tk) = match self.parse_additive()? {
                Some(v) => v,
                None => return Ok(None),
            };
            if tk != Expr {
                return Ok(None);
            }
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
        self.advance();
        if !self.at_char(b'{')? {
            return Ok(Some(make_any(self.mcx, 0, -1)?));
        }
        self.advance();
        let first = match self.parse_any_level()? {
            Some(v) => v,
            None => return Ok(None),
        };
        if self.peek_tok()? == Some(Token::ToP) {
            self.advance();
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
        match self.peek_tok()? {
            Some(Token::IntP) => {
                let s = self.take_str();
                let text = core::str::from_utf8(s).expect("scanner ints are ASCII");
                let n = numutils::pg_strtoint32(text)?;
                Ok(Some(n))
            }
            Some(Token::LastP) => {
                self.advance();
                Ok(Some(-1))
            }
            _ => Ok(None),
        }
    }

    /// '.' DECIMAL_P '(' opt_csv_list ')' — keyword already consumed, '('
    /// peeked-present.
    fn parse_decimal_args(&mut self) -> POut<Item<'mcx>> {
        self.advance();
        let mut list: PgVec<'mcx, Item<'mcx>> = PgVec::new_in(self.mcx);
        if !self.at_char(b')')? {
            let first = match self.parse_csv_elem()? {
                Some(v) => v,
                None => return Ok(None),
            };
            list.push(first);
            while self.at_char(b',')? {
                self.advance();
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
        if self.at_char(b'+')? {
            self.advance();
            if self.peek_tok()? != Some(Token::IntP) {
                return Ok(None);
            }
            let s = self.take_str();
            let num = make_item_numeric(self.mcx, s)?;
            return Ok(Some(make_item_unary(self.mcx, ItemType::Plus, num)?));
        }
        if self.at_char(b'-')? {
            self.advance();
            if self.peek_tok()? != Some(Token::IntP) {
                return Ok(None);
            }
            let s = self.take_str();
            let num = make_item_numeric(self.mcx, s)?;
            return Ok(Some(make_item_unary(self.mcx, ItemType::Minus, num)?));
        }
        if self.peek_tok()? == Some(Token::IntP) {
            let s = self.take_str();
            return Ok(Some(make_item_numeric(self.mcx, s)?));
        }
        Ok(None)
    }

    fn try_key_name(&mut self) -> PgResult<Option<&'mcx [u8]>> {
        let Some(tok) = self.peek_tok()? else {
            return Ok(None);
        };
        // The method-form keywords are consumed by parse_accessor_op before
        // this fallback; the remaining key_name tokens:
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
                | Token::LastP
                | Token::StartsP
                | Token::WithP
                | Token::LikeRegexP
                | Token::FlagP
        );
        if !is_key_name {
            return Ok(None);
        }
        Ok(Some(self.take_str()))
    }
}

fn method_optype(tok: Token) -> Option<ItemType> {
    match tok {
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

/// C: parsejsonpath (jsonpath_scan.l) — lex lazily (bison's one-token
/// lookahead), parse, and on a syntax error report through jsonpath_yyerror.
/// Ok(None) = empty input / soft error.
pub fn parsejsonpath<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<ParseResult<'mcx>>> {
    let mut escontext_ref = escontext;
    let mut parser = Parser {
        mcx,
        lexer: Lexer::new(mcx, str),
        lookahead: None,
        escontext: &mut escontext_ref,
        aborted: false,
    };
    let parsed = parser.parse_result()?;
    let aborted = parser.aborted;
    // The rejected lookahead's byte span is bison's yytext for the
    // "syntax error at or near" clause; none at end of input.
    let err_span = parser.current_span();
    drop(parser);

    // A scanner soft error recorded mid-stream stands (C: yyerror keeps the
    // first error), and the parse result is discarded as C's failed yyparse.
    if escontext_ref.as_ref().is_some_and(|c| c.error_occurred()) {
        return Ok(None);
    }

    match parsed {
        Some(r) if !aborted => Ok(r),
        _ => {
            match err_span {
                Some((s, e)) if s < e && e <= str.len() => {
                    jsonpath_yyerror_yytext(
                        escontext_ref.as_deref_mut(),
                        &str[s..e],
                        "syntax error",
                    )?;
                }
                _ => {
                    jsonpath_yyerror(escontext_ref.as_deref_mut(), str, str.len(), "syntax error")?;
                }
            }
            Ok(None)
        }
    }
}
