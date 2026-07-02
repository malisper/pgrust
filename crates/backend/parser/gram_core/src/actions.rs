use types_core::catalog::RELPERSISTENCE_PERMANENT;
use types_error::PgResult;
use types_nodes::rawnodes::A_Expr_Kind::AEXPR_OP;
use types_nodes::{Alias, Node, NodeList, RangeVar, RawStmt, SelectStmt, ValUnion};
use types_nodes::{BitString, Boolean, Float, Integer};

use crate::parse::Parser;
use crate::stack::Stacks;
use crate::tables::names::{YYRLINE, YYTNAME};
use crate::tables::YYR1;
use crate::yystype::YYSTYPE;

// Explicitly-precedenced operators, MathOp declaration order.
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
    // gram.y actions by generated-gram.c rule number (DISPATCH == 0 rules).
    #[inline(never)]
    pub(crate) fn reduce(
        &mut self,
        stk: &mut Stacks<'mcx>,
        rule: usize,
        yylen: usize,
        yyval: &mut YYSTYPE<'mcx>,
        yyloc: i32,
    ) -> PgResult<()> {
        let mcx = self.mcx;
        let _ = yyloc;
        match rule {
            2 => self.parsetree = stk.v(yylen, 1).list(),
            // stmtmulti: stmtmulti ';' toplevel_stmt
            8 => {
                let mut list = stk.v(yylen, 1).list();
                if !list.is_nil() {
                    let end = stk.l(yylen, 2);
                    let last = list.last().expect("stmtmulti cell");
                    // SAFETY: tree is parser-owned; no derived refs live.
                    unsafe {
                        last.with_mut::<RawStmt, _>(|rs| {
                            if rs.stmt_len <= 0 {
                                rs.stmt_len = end - rs.stmt_location;
                            }
                        })
                        .expect("llast_node(RawStmt)");
                    }
                }
                if let Some(stmt) = stk.v(yylen, 3).node() {
                    let loc = stk.l(yylen, 3);
                    list.lappend(mcx, Node::mk_raw_stmt(mcx, Some(stmt), loc, 0)?)?;
                }
                *yyval = YYSTYPE::List(list);
            }
            // stmtmulti: toplevel_stmt
            9 => {
                *yyval = YYSTYPE::List(match stk.v(yylen, 1).node() {
                    Some(stmt) => {
                        let loc = stk.l(yylen, 1);
                        NodeList::make1(mcx, Node::mk_raw_stmt(mcx, Some(stmt), loc, 0)?)?
                    }
                    None => NodeList::nil(),
                });
            }
            1719 => {
                let mut n = Node::build::<SelectStmt>(mcx)?;
                n.targetList = stk.v(yylen, 3).list();
                n.intoClause = stk.v(yylen, 4).node();
                n.fromClause = stk.v(yylen, 5).list();
                n.whereClause = stk.v(yylen, 6).node();
                let (distinct, list) = stk.v(yylen, 7).group();
                n.groupClause = list;
                n.groupDistinct = distinct;
                n.havingClause = stk.v(yylen, 8).node();
                n.windowClause = stk.v(yylen, 9).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1799 => {
                *yyval = YYSTYPE::Group { distinct: false, list: NodeList::nil() };
            }
            1830 => {
                let t = stk.v(yylen, 1).node().expect("table_ref");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            1831 => {
                let mut list = stk.v(yylen, 1).list();
                let t = stk.v(yylen, 3).node().expect("table_ref");
                list.lappend(mcx, t)?;
                *yyval = YYSTYPE::List(list);
            }
            1832 => {
                let rv = stk.v(yylen, 1).node().expect("relation_expr");
                let alias = stk.v(yylen, 2).alias();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.alias = alias)
                        .expect("relation_expr is RangeVar");
                }
                *yyval = YYSTYPE::Node(Some(rv));
            }
            1851 => {
                let name = stk.v(yylen, 2).str_val();
                *yyval = YYSTYPE::Alias(Some(mk_alias(mcx, name)?));
            }
            1853 => {
                let name = stk.v(yylen, 1).str_val();
                *yyval = YYSTYPE::Alias(Some(mk_alias(mcx, name)?));
            }
            // relation_expr: qualified_name; extended_relation_expr:
            //   qualified_name '*' | ONLY qualified_name | ONLY '(' q_n ')'
            1871 | 1873 | 1874 | 1875 => {
                let arg = match rule {
                    1874 => 2,
                    1875 => 3,
                    _ => 1,
                };
                let rv = stk.v(yylen, arg).node().expect("qualified_name");
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
            2025 => {
                let r = stk.v(yylen, 2).node();
                *yyval = self.simple_a_expr("+", None, r, stk.l(yylen, 1))?;
            }
            2026 => {
                let n = stk.v(yylen, 2).node().expect("a_expr");
                *yyval = self.do_negate(n, stk.l(yylen, 1))?;
            }
            2027..=2038 => {
                let op = MATH_OPS[rule - 2027];
                let l = stk.v(yylen, 1).node();
                let r = stk.v(yylen, 3).node();
                *yyval = self.simple_a_expr(op, l, r, stk.l(yylen, 2))?;
            }
            2039 => {
                let name = stk.v(yylen, 2).list();
                let l = stk.v(yylen, 1).node();
                let r = stk.v(yylen, 3).node();
                *yyval =
                    YYSTYPE::Node(Some(make_a_expr(mcx, name, l, r, stk.l(yylen, 2))?));
            }
            2040 => {
                let name = stk.v(yylen, 1).list();
                let r = stk.v(yylen, 2).node();
                *yyval =
                    YYSTYPE::Node(Some(make_a_expr(mcx, name, None, r, stk.l(yylen, 1))?));
            }
            // c_expr: PARAM / parenthesized a_expr (opt_indirection)
            2114 => {
                let number = stk.v(yylen, 1).ival();
                let ind = stk.v(yylen, 2).list();
                if !ind.is_nil() {
                    panic!("gram_core: A_Indirection over PARAM not ported (types_nodes gap)");
                }
                *yyval = YYSTYPE::Node(Some(Node::mk_param_ref(mcx, number, stk.l(yylen, 1))?));
            }
            2115 => {
                let e = stk.v(yylen, 2);
                let ind = stk.v(yylen, 4).list();
                if !ind.is_nil() {
                    panic!("gram_core: A_Indirection over (a_expr) not ported (types_nodes gap)");
                }
                *yyval = e;
            }
            2268..=2279 => *yyval = YYSTYPE::Keyword(MATH_OPS[rule - 2268]),
            2280 | 2282 | 2284 => {
                let op = stk.v(yylen, 1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, op)?)?);
            }
            2286..=2289 => {
                let op = ["~~", "!~~", "~~*", "!~~*"][rule - 2286];
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, op)?)?);
            }
            2338 | 2339 => {
                let name = stk.v(yylen, 1).str_val();
                let ind = if rule == 2339 { stk.v(yylen, 2).list() } else { NodeList::nil() };
                *yyval = YYSTYPE::Node(Some(self.make_column_ref(
                    name,
                    ind,
                    stk.l(yylen, 1),
                )?));
            }
            2340 => {
                let s = stk.v(yylen, 2).str_val();
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, s)?));
            }
            2341 => *yyval = YYSTYPE::Node(Some(Node::mk_a_star(mcx)?)),
            2342 | 2343 => {
                panic!("gram_core: A_Indices subscripting not ported (types_nodes gap)")
            }
            2346 => {
                let el = stk.v(yylen, 1).node().expect("indirection_el");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            2347 => {
                let mut list = stk.v(yylen, 1).list();
                let el = stk.v(yylen, 2).node().expect("indirection_el");
                list.lappend(mcx, el)?;
                *yyval = YYSTYPE::List(list);
            }
            2349 => {
                let mut list = stk.v(yylen, 1).list();
                let el = stk.v(yylen, 2).node().expect("indirection_el");
                list.lappend(mcx, el)?;
                *yyval = YYSTYPE::List(list);
            }
            2422 => {
                let t = stk.v(yylen, 1).node().expect("target_el");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            2423 => {
                let mut list = stk.v(yylen, 1).list();
                let t = stk.v(yylen, 3).node().expect("target_el");
                list.lappend(mcx, t)?;
                *yyval = YYSTYPE::List(list);
            }
            2424..=2427 => {
                let (name, val) = match rule {
                    2424 => {
                        let val = stk.v(yylen, 1).node();
                        (Some(stk.v(yylen, 3).str_val()), val)
                    }
                    2425 => {
                        let val = stk.v(yylen, 1).node();
                        (Some(stk.v(yylen, 2).str_val()), val)
                    }
                    2426 => (None, stk.v(yylen, 1).node()),
                    _ => {
                        let star = NodeList::make1(mcx, Node::mk_a_star(mcx)?)?;
                        (None, Some(Node::mk_column_ref(mcx, star, stk.l(yylen, 1))?))
                    }
                };
                let loc = stk.l(yylen, 1);
                *yyval = YYSTYPE::Node(Some(Node::mk_res_target(
                    mcx,
                    name,
                    NodeList::nil(),
                    val,
                    loc,
                )?));
            }
            2428 => {
                let q = stk.v(yylen, 1).node().expect("qualified_name");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, q)?);
            }
            2429 => {
                let mut list = stk.v(yylen, 1).list();
                let q = stk.v(yylen, 3).node().expect("qualified_name");
                list.lappend(mcx, q)?;
                *yyval = YYSTYPE::List(list);
            }
            2432 => {
                let s = stk.v(yylen, 1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, s)?)?);
            }
            2433 => {
                let mut list = stk.v(yylen, 1).list();
                let s = stk.v(yylen, 3).str_val();
                list.lappend(mcx, Node::mk_string(mcx, s)?)?;
                *yyval = YYSTYPE::List(list);
            }
            2430 => {
                let relname = stk.v(yylen, 1).str_val();
                let rv = make_range_var(mcx, None, None, Some(relname), stk.l(yylen, 1))?;
                *yyval = YYSTYPE::Node(Some(rv));
            }
            2431 => {
                let name = stk.v(yylen, 1).str_val();
                let ind = stk.v(yylen, 2).list();
                let loc = stk.l(yylen, 1);
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
            2439 => {
                let v = stk.v(yylen, 1).ival();
                *yyval = self.a_const(ValUnion::Integer(Integer { ival: v }), stk.l(yylen, 1))?;
            }
            2440 => {
                let s = stk.v(yylen, 1).str_val();
                *yyval = self.a_const(ValUnion::Float(Float { fval: s }), stk.l(yylen, 1))?;
            }
            2441 => {
                let s = stk.v(yylen, 1).str_val();
                *yyval = self.a_const(
                    ValUnion::String(types_nodes::String { sval: s }),
                    stk.l(yylen, 1),
                )?;
            }
            2442 | 2443 => {
                let s = stk.v(yylen, 1).str_val();
                *yyval = self.a_const(ValUnion::BitString(BitString { bsval: s }), stk.l(yylen, 1))?;
            }
            2449 => *yyval = self.a_const(ValUnion::Boolean(Boolean { boolval: true }), stk.l(yylen, 1))?,
            2450 => *yyval = self.a_const(ValUnion::Boolean(Boolean { boolval: false }), stk.l(yylen, 1))?,
            2451 => {
                *yyval =
                    YYSTYPE::Node(Some(Node::mk_a_const(mcx, None, stk.l(yylen, 1))?));
            }
            2455 => *yyval = YYSTYPE::Ival(stk.v(yylen, 2).ival()),
            2456 => *yyval = YYSTYPE::Ival(-stk.v(yylen, 2).ival()),
            2470..=2486 => *yyval = YYSTYPE::Str(stk.v(yylen, 1).str_val()),
            _ => unimplemented_rule(rule),
        }
        Ok(())
    }

    // makeColumnRef; A_Indices arms unreachable (rules 2342/2343 panic).
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

    fn simple_a_expr(
        &self,
        op: &'static str,
        lexpr: Option<Node<'mcx>>,
        rexpr: Option<Node<'mcx>>,
        location: i32,
    ) -> PgResult<YYSTYPE<'mcx>> {
        let name = NodeList::make1(self.mcx, Node::mk_string(self.mcx, op)?)?;
        Ok(YYSTYPE::Node(Some(make_a_expr(
            self.mcx, name, lexpr, rexpr, location,
        )?)))
    }

    fn a_const(&self, val: ValUnion<'mcx>, location: i32) -> PgResult<YYSTYPE<'mcx>> {
        Ok(YYSTYPE::Node(Some(Node::mk_a_const(
            self.mcx,
            Some(val),
            location,
        )?)))
    }
}

// makeA_Expr (makefuncs.c): makeNode zero-fill leaves rexpr_list_start/end 0
// (types_nodes::mk_a_expr's -1 diverges from C; ground truth is gram.c).
fn make_a_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    name: NodeList<'mcx>,
    lexpr: Option<Node<'mcx>>,
    rexpr: Option<Node<'mcx>>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        types_nodes::A_Expr {
            kind: AEXPR_OP,
            name,
            lexpr,
            rexpr,
            rexpr_list_start: 0,
            rexpr_list_end: 0,
            location,
        },
    )
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
