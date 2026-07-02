use types_core::catalog::RELPERSISTENCE_PERMANENT;
use types_error::PgResult;
use types_nodes::rawnodes::A_Expr_Kind::AEXPR_OP;
use types_nodes::{Alias, Node, NodeList, RangeVar, RawStmt, SelectStmt, ValUnion};
use types_nodes::{BitString, Boolean, Float, Integer};

use crate::parse::Parser;
use crate::tables::names::{HAS_ACTION, YYRLINE, YYTNAME};
use crate::tables::YYR1;
use crate::yystype::YYSTYPE;

// gram.y's explicitly-precedenced operator set, in MathOp declaration order
// (shared by the a_expr/b_expr binary productions and MathOp itself).
static MATH_OPS: [&str; 12] =
    ["+", "-", "*", "/", "%", "^", "<", ">", "=", "<=", ">=", "<>"];

#[cold]
#[inline(never)]
fn unimplemented_rule(rule: usize) -> ! {
    panic!(
        "gram_core: unimplemented grammar action: rule {rule} ({}), gram.y:{}",
        YYTNAME[YYR1[rule] as usize], YYRLINE[rule]
    )
}

impl<'mcx> Parser<'mcx> {
    // gram.y semantic actions, numbered by the generated gram.c reduction
    // switch. Rules without a case take bison's default `$$ = $1`; cased
    // rules outside the ported SELECT path panic with their gram.y location.
    pub(crate) fn reduce(
        &mut self,
        rule: usize,
        yylen: usize,
        yyval: &mut YYSTYPE<'mcx>,
        yyloc: i32,
    ) -> PgResult<()> {
        if !HAS_ACTION[rule] {
            if yylen >= 1 {
                *yyval = self.v(yylen, 1);
            }
            return Ok(());
        }
        let mcx = self.mcx;
        let _ = yyloc;
        match rule {
            // parse_toplevel: stmtmulti
            2 => self.parsetree = self.v(yylen, 1).list(),
            // stmtmulti: stmtmulti ';' toplevel_stmt
            8 => {
                let mut list = self.v(yylen, 1).list();
                if !list.is_nil() {
                    let end = self.l(yylen, 2);
                    let last = list.last().expect("stmtmulti cell");
                    // SAFETY: the parse tree is uniquely owned by the parser
                    // stacks until yyparse returns; no derived refs are live.
                    unsafe {
                        last.with_mut::<RawStmt, _>(|rs| {
                            // updateRawStmtEnd: keep an already-set length.
                            if rs.stmt_len <= 0 {
                                rs.stmt_len = end - rs.stmt_location;
                            }
                        })
                        .expect("llast_node(RawStmt)");
                    }
                }
                if let Some(stmt) = self.v(yylen, 3).node() {
                    let loc = self.l(yylen, 3);
                    list.lappend(mcx, Node::mk_raw_stmt(mcx, Some(stmt), loc, 0)?)?;
                }
                *yyval = YYSTYPE::List(list);
            }
            // stmtmulti: toplevel_stmt
            9 => {
                *yyval = YYSTYPE::List(match self.v(yylen, 1).node() {
                    Some(stmt) => {
                        let loc = self.l(yylen, 1);
                        NodeList::make1(mcx, Node::mk_raw_stmt(mcx, Some(stmt), loc, 0)?)?
                    }
                    None => NodeList::nil(),
                });
            }
            // stmt: /*EMPTY*/
            136 => *yyval = YYSTYPE::Node(None),
            // select_with_parens: '(' select_no_parens|select_with_parens ')'
            1707 | 1708 => *yyval = self.v(yylen, 2),
            // select_no_parens: simple_select; select_clause: both alts
            1709 | 1717 | 1718 => *yyval = self.v(yylen, 1),
            // simple_select: SELECT opt_all_clause opt_target_list into_clause
            //   from_clause where_clause group_clause having_clause window_clause
            1719 => {
                let mut n = Node::build::<SelectStmt>(mcx)?;
                n.targetList = self.v(yylen, 3).list();
                n.intoClause = self.v(yylen, 4).node();
                n.fromClause = self.v(yylen, 5).list();
                n.whereClause = self.v(yylen, 6).node();
                let (distinct, list) = self.v(yylen, 7).group();
                n.groupClause = list;
                n.groupDistinct = distinct;
                n.havingClause = self.v(yylen, 8).node();
                n.windowClause = self.v(yylen, 9).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // into_clause: /*EMPTY*/
            1744 => *yyval = YYSTYPE::Node(None),
            // opt_sort_clause: sort_clause
            1765 => *yyval = self.v(yylen, 1),
            // opt_sort_clause: /*EMPTY*/
            1766 => *yyval = YYSTYPE::List(NodeList::nil()),
            // sort_clause: ORDER BY sortby_list
            1767 => *yyval = self.v(yylen, 3),
            // group_clause: /*EMPTY*/
            1799 => {
                *yyval = YYSTYPE::Group { distinct: false, list: NodeList::nil() };
            }
            // having_clause: HAVING a_expr
            1811 => *yyval = self.v(yylen, 2),
            // having_clause: /*EMPTY*/
            1812 => *yyval = YYSTYPE::Node(None),
            // from_clause: FROM from_list
            1828 => *yyval = self.v(yylen, 2),
            // from_clause: /*EMPTY*/
            1829 => *yyval = YYSTYPE::List(NodeList::nil()),
            // from_list: table_ref
            1830 => {
                let t = self.v(yylen, 1).node().expect("table_ref");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            // from_list: from_list ',' table_ref
            1831 => {
                let mut list = self.v(yylen, 1).list();
                let t = self.v(yylen, 3).node().expect("table_ref");
                list.lappend(mcx, t)?;
                *yyval = YYSTYPE::List(list);
            }
            // table_ref: relation_expr opt_alias_clause
            1832 => {
                let rv = self.v(yylen, 1).node().expect("relation_expr");
                let alias = self.v(yylen, 2).alias();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.alias = alias)
                        .expect("relation_expr is RangeVar");
                }
                *yyval = YYSTYPE::Node(Some(rv));
            }
            // alias_clause: AS ColId
            1851 => {
                let name = self.v(yylen, 2).str_val();
                *yyval = YYSTYPE::Alias(Some(mk_alias(mcx, name)?));
            }
            // alias_clause: ColId
            1853 => {
                let name = self.v(yylen, 1).str_val();
                *yyval = YYSTYPE::Alias(Some(mk_alias(mcx, name)?));
            }
            // opt_alias_clause: alias_clause
            1854 => *yyval = self.v(yylen, 1),
            // opt_alias_clause: /*EMPTY*/
            1855 => *yyval = YYSTYPE::Alias(None),
            // relation_expr: extended_relation_expr passthrough
            1872 => *yyval = self.v(yylen, 1),
            // relation_expr: qualified_name; extended_relation_expr:
            //   qualified_name '*' | ONLY qualified_name | ONLY '(' q_n ')'
            1871 | 1873 | 1874 | 1875 => {
                let arg = match rule {
                    1874 => 2,
                    1875 => 3,
                    _ => 1,
                };
                let rv = self.v(yylen, arg).node().expect("qualified_name");
                let inh = rule <= 1873;
                // SAFETY: as rule 8.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| {
                        r.inh = inh;
                        r.alias = None;
                    })
                    .expect("qualified_name is RangeVar");
                }
                *yyval = YYSTYPE::Node(Some(rv));
            }
            // where_clause: WHERE a_expr
            1893 => *yyval = self.v(yylen, 2),
            // where_clause: /*EMPTY*/
            1894 => *yyval = YYSTYPE::Node(None),
            // a_expr: c_expr
            2020 => *yyval = self.v(yylen, 1),
            // a_expr: '+' a_expr %prec UMINUS
            2025 => {
                let r = self.v(yylen, 2).node();
                *yyval = self.simple_a_expr("+", None, r, self.l(yylen, 1))?;
            }
            // a_expr: '-' a_expr %prec UMINUS (doNegate)
            2026 => {
                let n = self.v(yylen, 2).node().expect("a_expr");
                *yyval = self.do_negate(n, self.l(yylen, 1))?;
            }
            // a_expr: a_expr <op> a_expr (bison-precedence operators)
            2027..=2038 => {
                let op = MATH_OPS[rule - 2027];
                let l = self.v(yylen, 1).node();
                let r = self.v(yylen, 3).node();
                *yyval = self.simple_a_expr(op, l, r, self.l(yylen, 2))?;
            }
            // a_expr: a_expr qual_Op a_expr %prec Op (makeA_Expr)
            2039 => {
                let name = self.v(yylen, 2).list();
                let l = self.v(yylen, 1).node();
                let r = self.v(yylen, 3).node();
                *yyval = YYSTYPE::Node(Some(Node::mk_a_expr(
                    mcx,
                    AEXPR_OP,
                    name,
                    l,
                    r,
                    self.l(yylen, 2),
                )?));
            }
            // a_expr: qual_Op a_expr %prec Op
            2040 => {
                let name = self.v(yylen, 1).list();
                let r = self.v(yylen, 2).node();
                *yyval = YYSTYPE::Node(Some(Node::mk_a_expr(
                    mcx,
                    AEXPR_OP,
                    name,
                    None,
                    r,
                    self.l(yylen, 1),
                )?));
            }
            // c_expr: columnref | AexprConst
            2112 | 2113 => *yyval = self.v(yylen, 1),
            // c_expr: PARAM opt_indirection
            2114 => {
                let number = self.v(yylen, 1).ival();
                let ind = self.v(yylen, 2).list();
                if !ind.is_nil() {
                    panic!("gram_core: A_Indirection over PARAM not ported (types_nodes gap)");
                }
                *yyval = YYSTYPE::Node(Some(Node::mk_param_ref(mcx, number, self.l(yylen, 1))?));
            }
            // c_expr: '(' a_expr ')' opt_indirection
            2115 => {
                let e = self.v(yylen, 2);
                let ind = self.v(yylen, 4).list();
                if !ind.is_nil() {
                    panic!("gram_core: A_Indirection over (a_expr) not ported (types_nodes gap)");
                }
                *yyval = e;
            }
            // all_Op: Op | MathOp
            2266 | 2267 => *yyval = self.v(yylen, 1),
            // MathOp: '+' .. NOT_EQUALS
            2268..=2279 => *yyval = YYSTYPE::Keyword(MATH_OPS[rule - 2268]),
            // qual_Op | qual_all_Op | subquery_Op: all_Op → list_make1(makeString($1))
            2280 | 2282 | 2284 => {
                let op = self.v(yylen, 1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, op)?)?);
            }
            // ... : OPERATOR '(' any_operator ')'
            2281 | 2283 | 2285 => *yyval = self.v(yylen, 3),
            // subquery_Op: [NOT_LA] LIKE/ILIKE
            2286..=2289 => {
                let op = ["~~", "!~~", "~~*", "!~~*"][rule - 2286];
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, op)?)?);
            }
            // window_clause: WINDOW window_definition_list
            2228 => *yyval = self.v(yylen, 2),
            // window_clause: /*EMPTY*/
            2229 => *yyval = YYSTYPE::List(NodeList::nil()),
            // columnref: ColId [indirection] (makeColumnRef)
            2338 | 2339 => {
                let name = self.v(yylen, 1).str_val();
                let ind = if rule == 2339 { self.v(yylen, 2).list() } else { NodeList::nil() };
                *yyval = YYSTYPE::Node(Some(self.make_column_ref(
                    name,
                    ind,
                    self.l(yylen, 1),
                )?));
            }
            // indirection_el: '.' attr_name | '.' '*' | subscripts
            2340 => {
                let s = self.v(yylen, 2).str_val();
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, s)?));
            }
            2341 => *yyval = YYSTYPE::Node(Some(Node::mk_a_star(mcx)?)),
            2342 | 2343 => {
                panic!("gram_core: A_Indices subscripting not ported (types_nodes gap)")
            }
            // opt_slice_bound: a_expr | /*EMPTY*/
            2344 => *yyval = self.v(yylen, 1),
            2345 => *yyval = YYSTYPE::Node(None),
            // indirection: indirection_el | indirection indirection_el
            2346 => {
                let el = self.v(yylen, 1).node().expect("indirection_el");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            2347 => {
                let mut list = self.v(yylen, 1).list();
                let el = self.v(yylen, 2).node().expect("indirection_el");
                list.lappend(mcx, el)?;
                *yyval = YYSTYPE::List(list);
            }
            // opt_indirection: /*EMPTY*/ | opt_indirection indirection_el
            2348 => *yyval = YYSTYPE::List(NodeList::nil()),
            2349 => {
                let mut list = self.v(yylen, 1).list();
                let el = self.v(yylen, 2).node().expect("indirection_el");
                list.lappend(mcx, el)?;
                *yyval = YYSTYPE::List(list);
            }
            // opt_target_list: target_list
            2420 => *yyval = self.v(yylen, 1),
            // opt_target_list: /*EMPTY*/
            2421 => *yyval = YYSTYPE::List(NodeList::nil()),
            // target_list: target_el
            2422 => {
                let t = self.v(yylen, 1).node().expect("target_el");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            // target_list: target_list ',' target_el
            2423 => {
                let mut list = self.v(yylen, 1).list();
                let t = self.v(yylen, 3).node().expect("target_el");
                list.lappend(mcx, t)?;
                *yyval = YYSTYPE::List(list);
            }
            // target_el: a_expr AS ColLabel | a_expr BareColLabel | a_expr | '*'
            2424..=2427 => {
                let (name, val) = match rule {
                    2424 => {
                        let val = self.v(yylen, 1).node();
                        (Some(self.v(yylen, 3).str_val()), val)
                    }
                    2425 => {
                        let val = self.v(yylen, 1).node();
                        (Some(self.v(yylen, 2).str_val()), val)
                    }
                    2426 => (None, self.v(yylen, 1).node()),
                    _ => {
                        let star = NodeList::make1(mcx, Node::mk_a_star(mcx)?)?;
                        (None, Some(Node::mk_column_ref(mcx, star, self.l(yylen, 1))?))
                    }
                };
                let loc = self.l(yylen, 1);
                *yyval = YYSTYPE::Node(Some(Node::mk_res_target(
                    mcx,
                    name,
                    NodeList::nil(),
                    val,
                    loc,
                )?));
            }
            // qualified_name: ColId (makeRangeVar(NULL, $1, @1))
            2430 => {
                let relname = self.v(yylen, 1).str_val();
                let rv = make_range_var(mcx, None, None, Some(relname), self.l(yylen, 1))?;
                *yyval = YYSTYPE::Node(Some(rv));
            }
            // qualified_name: ColId indirection (makeRangeVarFromQualifiedName)
            2431 => {
                let name = self.v(yylen, 1).str_val();
                let ind = self.v(yylen, 2).list();
                let loc = self.l(yylen, 1);
                let mut parts = [None; 2];
                for (i, n) in ind.iter().enumerate() {
                    // check_qualified_name
                    let Some(s) = n.as_string() else {
                        return Err(self.parser_yyerror("syntax error"));
                    };
                    if i < 2 {
                        parts[i] = Some(s.sval);
                    }
                }
                let rv = match ind.len() {
                    1 => make_range_var(mcx, None, Some(name), parts[0], loc)?,
                    2 => make_range_var(mcx, Some(name), parts[0], parts[1], loc)?,
                    _ => return Err(self.improper_qualified_name(Some(name), &ind, loc)),
                };
                *yyval = YYSTYPE::Node(Some(rv));
            }
            // AexprConst: Iconst | FCONST | Sconst | BCONST | XCONST
            2439 => {
                let v = self.v(yylen, 1).ival();
                *yyval = self.a_const(ValUnion::Integer(Integer { ival: v }), yylen)?;
            }
            2440 => {
                let s = self.v(yylen, 1).str_val();
                *yyval = self.a_const(ValUnion::Float(Float { fval: s }), yylen)?;
            }
            2441 => {
                let s = self.v(yylen, 1).str_val();
                *yyval = self.a_const(
                    ValUnion::String(types_nodes::String { sval: s }),
                    yylen,
                )?;
            }
            2442 | 2443 => {
                let s = self.v(yylen, 1).str_val();
                *yyval = self.a_const(ValUnion::BitString(BitString { bsval: s }), yylen)?;
            }
            // AexprConst: TRUE_P | FALSE_P | NULL_P
            2449 => *yyval = self.a_const(ValUnion::Boolean(Boolean { boolval: true }), yylen)?,
            2450 => *yyval = self.a_const(ValUnion::Boolean(Boolean { boolval: false }), yylen)?,
            2451 => {
                *yyval =
                    YYSTYPE::Node(Some(Node::mk_a_const(mcx, None, self.l(yylen, 1))?));
            }
            // Iconst / Sconst / SignedIconst
            2452 | 2453 | 2454 => *yyval = self.v(yylen, 1),
            2455 => *yyval = YYSTYPE::Ival(self.v(yylen, 2).ival()),
            2456 => *yyval = YYSTYPE::Ival(-self.v(yylen, 2).ival()),
            // ColId / type_function_name / NonReservedWord / ColLabel /
            // BareColLabel: IDENT passthrough or keyword pstrdup (borrowed
            // &'static str here; C copies only for writability).
            2470..=2486 => *yyval = YYSTYPE::Str(self.v(yylen, 1).str_val()),
            _ => unimplemented_rule(rule),
        }
        Ok(())
    }

    // makeColumnRef (A_Indices arms are unreachable: rules 2342/2343 panic).
    fn make_column_ref(
        &self,
        colname: &'mcx str,
        indirection: NodeList<'mcx>,
        location: i32,
    ) -> PgResult<Node<'mcx>> {
        let n = indirection.len();
        for (i, el) in indirection.iter().enumerate() {
            if el.as_a_star().is_some() && i + 1 != n {
                return Err(self.parser_yyerror("improper use of \"*\""));
            }
        }
        let mut fields = indirection;
        fields.lcons(self.mcx, Node::mk_string(self.mcx, colname)?)?;
        Node::mk_column_ref(self.mcx, fields, location)
    }

    #[cold]
    fn improper_qualified_name(
        &self,
        first: Option<&str>,
        names: &NodeList<'mcx>,
        location: i32,
    ) -> Box<types_error::PgError> {
        // NameListToString (namespace.c): dot-join String/A_Star elements.
        let mut joined = std::string::String::new();
        for s in first.into_iter().chain(names.iter().map(|n| {
            if n.as_a_star().is_some() {
                "*"
            } else {
                n.as_string().map(|s| s.sval).unwrap_or("?")
            }
        })) {
            if !joined.is_empty() {
                joined.push('.');
            }
            joined.push_str(s);
        }
        self.errposition_error(
            format!("improper qualified name (too many dotted names): {joined}"),
            location,
        )
    }

    // doNegate: fold the '-' into integer/float A_Const literals so
    // "-123.456" stays in string form until its type is known.
    fn do_negate(&self, n: Node<'mcx>, location: i32) -> PgResult<YYSTYPE<'mcx>> {
        if n.as_a_const().is_some() {
            let mcx = self.mcx;
            let mut negate_err = Ok(());
            // SAFETY: parser-owned tree, no live derived refs (as rule 8).
            let folded = unsafe {
                n.with_mut::<types_nodes::A_Const, _>(|con| {
                    con.location = location;
                    match &mut con.val {
                        Some(ValUnion::Integer(i)) => {
                            i.ival = -i.ival;
                            true
                        }
                        Some(ValUnion::Float(f)) => {
                            match negate_float(mcx, f.fval) {
                                Ok(s) => f.fval = s,
                                Err(e) => negate_err = Err(e),
                            }
                            true
                        }
                        _ => false,
                    }
                })
                .expect("A_Const")
            };
            negate_err?;
            if folded {
                return Ok(YYSTYPE::Node(Some(n)));
            }
        }
        self.simple_a_expr("-", None, Some(n), location)
    }

    // makeSimpleA_Expr(AEXPR_OP, name, lexpr, rexpr, location)
    fn simple_a_expr(
        &self,
        op: &'static str,
        lexpr: Option<Node<'mcx>>,
        rexpr: Option<Node<'mcx>>,
        location: i32,
    ) -> PgResult<YYSTYPE<'mcx>> {
        let name = NodeList::make1(self.mcx, Node::mk_string(self.mcx, op)?)?;
        Ok(YYSTYPE::Node(Some(Node::mk_a_expr(
            self.mcx, AEXPR_OP, name, lexpr, rexpr, location,
        )?)))
    }

    fn a_const(&self, val: ValUnion<'mcx>, yylen: usize) -> PgResult<YYSTYPE<'mcx>> {
        Ok(YYSTYPE::Node(Some(Node::mk_a_const(
            self.mcx,
            Some(val),
            self.l(yylen, 1),
        )?)))
    }
}

// makeRangeVar (makefuncs.c): inh = true, permanent persistence.
fn make_range_var<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    catalogname: Option<&'mcx str>,
    schemaname: Option<&'mcx str>,
    relname: Option<&'mcx str>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        RangeVar {
            catalogname,
            schemaname,
            relname,
            inh: true,
            relpersistence: RELPERSISTENCE_PERMANENT,
            alias: None,
            location,
        },
    )
}

// doNegateFloat: strip a leading '+'/'-' pair-wise or prepend '-'.
fn negate_float<'mcx>(mcx: mcx::Mcx<'mcx>, fval: &'mcx str) -> PgResult<&'mcx str> {
    let s = fval.strip_prefix('+').unwrap_or(fval);
    if let Some(stripped) = s.strip_prefix('-') {
        return Ok(stripped);
    }
    let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
    v.push(b'-');
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    // SAFETY: '-' + valid UTF-8.
    Ok(unsafe { core::str::from_utf8_unchecked(v.leak()) })
}

fn mk_alias<'mcx>(mcx: mcx::Mcx<'mcx>, name: &'mcx str) -> PgResult<&'mcx Alias<'mcx>> {
    Ok(Node::mk_mut(
        mcx,
        Alias { aliasname: Some(name), colnames: NodeList::nil() },
    )?
    .seal_ref())
}
