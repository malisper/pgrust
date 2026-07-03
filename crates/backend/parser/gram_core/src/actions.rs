use types_core::catalog::RELPERSISTENCE_PERMANENT;
use types_error::PgResult;
use types_nodes::rawnodes::A_Expr_Kind::AEXPR_OP;
use types_nodes::{Alias, Node, NodeList, RangeVar, RawStmt, SelectStmt, ValUnion};
use types_nodes::{BitString, Boolean, Float, Integer};
use types_nodes::{
    BoolExpr, BoolExprType, CoercionForm, DistinctClause, FuncCall, LimitOption, NodeMut,
    NullTest, NullTestType, SortBy, SortByDir, SortByNulls, TypeCast, TypeName,
};

use crate::parse::Parser;
use crate::stack::Stacks;
use crate::tables::names::{YYRLINE, YYTNAME};
use crate::tables::YYR1;
use crate::yystype::{SelectLimit, YYSTYPE};

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
            1719 | 1720 => {
                let mut n = Node::build::<SelectStmt>(mcx)?;
                if rule == 1720 {
                    n.distinctClause = match stk.v(yylen, 2) {
                        YYSTYPE::DistinctAll => DistinctClause::All,
                        v => DistinctClause::On(v.list()),
                    };
                }
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
            // select_no_parens: select_clause sort_clause | select_clause
            // opt_sort_clause [for_locking_clause select_limit] (both orders);
            // WITH variants (1713..1716) stay unported.
            1710 => {
                let stmt = stk.v(yylen, 1).node().expect("select_clause");
                let sort = stk.v(yylen, 2).list();
                self.insert_select_options(stmt, sort, NodeList::nil(), None)?;
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            1711 | 1712 => {
                let stmt = stk.v(yylen, 1).node().expect("select_clause");
                let sort = stk.v(yylen, 2).list();
                let (lock_i, limit_i) = if rule == 1711 { (3, 4) } else { (4, 3) };
                let locking = stk.v(yylen, lock_i).list();
                let limit = stk.v(yylen, limit_i).limit();
                self.insert_select_options(stmt, sort, locking, limit)?;
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            // opt_asc_desc / opt_nulls_order constants (shared with index_elem).
            1120 => *yyval = YYSTYPE::Ival(SortByDir::SORTBY_ASC as i32),
            1121 => *yyval = YYSTYPE::Ival(SortByDir::SORTBY_DESC as i32),
            1122 => *yyval = YYSTYPE::Ival(SortByDir::SORTBY_DEFAULT as i32),
            1123 => *yyval = YYSTYPE::Ival(SortByNulls::SORTBY_NULLS_FIRST as i32),
            1124 => *yyval = YYSTYPE::Ival(SortByNulls::SORTBY_NULLS_LAST as i32),
            1125 => *yyval = YYSTYPE::Ival(SortByNulls::SORTBY_NULLS_DEFAULT as i32),
            1759 => *yyval = YYSTYPE::DistinctAll,
            1768 => {
                let s = stk.v(yylen, 1).node().expect("sortby");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, s)?);
            }
            1769 => {
                let mut list = stk.v(yylen, 1).list();
                let s = stk.v(yylen, 3).node().expect("sortby");
                list.lappend(mcx, s)?;
                *yyval = YYSTYPE::List(list);
            }
            1770 => {
                let node = stk.v(yylen, 1).node();
                let use_op = stk.v(yylen, 3).list();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    SortBy {
                        node,
                        sortby_dir: SortByDir::SORTBY_USING,
                        sortby_nulls: sortby_nulls(stk.v(yylen, 4).ival()),
                        useOp: use_op,
                        location: stk.l(yylen, 3),
                    },
                )?));
            }
            1771 => {
                let node = stk.v(yylen, 1).node();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    SortBy {
                        node,
                        sortby_dir: sortby_dir(stk.v(yylen, 2).ival()),
                        sortby_nulls: sortby_nulls(stk.v(yylen, 3).ival()),
                        useOp: NodeList::nil(),
                        location: -1,
                    },
                )?));
            }
            // select_limit: limit_clause offset_clause (either order) / alone.
            1772 | 1773 => {
                let (sl_i, off_i) = if rule == 1772 { (1, 2) } else { (2, 1) };
                let sl = stk.v(yylen, sl_i).limit().expect("limit_clause");
                sl.limitOffset = stk.v(yylen, off_i).node();
                sl.offsetLoc = stk.l(yylen, off_i);
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1775 => {
                let offset = stk.v(yylen, 1).node();
                let sl = mk_select_limit(
                    mcx,
                    offset,
                    None,
                    LimitOption::LIMIT_OPTION_COUNT,
                    stk.l(yylen, 1),
                    -1,
                    -1,
                )?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1778 => {
                let count = stk.v(yylen, 2).node();
                let sl = mk_select_limit(
                    mcx,
                    None,
                    count,
                    LimitOption::LIMIT_OPTION_COUNT,
                    -1,
                    stk.l(yylen, 1),
                    -1,
                )?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1779 => {
                return Err(Box::new(
                    (*self.errposition_error(
                        "LIMIT #,# syntax is not supported".into(),
                        stk.l(yylen, 1),
                    ))
                    .with_hint("Use separate LIMIT and OFFSET clauses."),
                ));
            }
            // FETCH { FIRST | NEXT } [count] { ROW | ROWS } { ONLY | WITH TIES }
            1780 | 1781 => {
                let count = stk.v(yylen, 3).node();
                let (option, option_loc) = if rule == 1781 {
                    (LimitOption::LIMIT_OPTION_WITH_TIES, stk.l(yylen, 5))
                } else {
                    (LimitOption::LIMIT_OPTION_COUNT, -1)
                };
                let sl =
                    mk_select_limit(mcx, None, count, option, -1, stk.l(yylen, 1), option_loc)?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1782 | 1783 => {
                let count = Some(make_int_const(mcx, 1, -1)?);
                let (option, option_loc) = if rule == 1783 {
                    (LimitOption::LIMIT_OPTION_WITH_TIES, stk.l(yylen, 4))
                } else {
                    (LimitOption::LIMIT_OPTION_COUNT, -1)
                };
                let sl =
                    mk_select_limit(mcx, None, count, option, -1, stk.l(yylen, 1), option_loc)?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            // LIMIT ALL is a NULL constant.
            1787 => {
                *yyval = YYSTYPE::Node(Some(Node::mk_a_const(mcx, None, stk.l(yylen, 1))?));
            }
            1790 => {
                let r = stk.v(yylen, 2).node();
                *yyval = self.simple_a_expr("+", None, r, stk.l(yylen, 1))?;
            }
            1791 => {
                let n = stk.v(yylen, 2).node().expect("I_or_F_const");
                *yyval = self.do_negate(n, stk.l(yylen, 1))?;
            }
            1792 => {
                let v = stk.v(yylen, 1).ival();
                *yyval = self.a_const(ValUnion::Integer(Integer { ival: v }), stk.l(yylen, 1))?;
            }
            1793 => {
                let s = stk.v(yylen, 1).str_val();
                *yyval = self.a_const(ValUnion::Float(Float { fval: s }), stk.l(yylen, 1))?;
            }
            // row_or_rows / first_or_next (values unused downstream).
            1794..=1797 => *yyval = YYSTYPE::Ival(0),
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
            // Typename: SimpleTypename opt_array_bounds (bounds themselves
            // are unported louds, so the assigned list is always NIL).
            1936 => {
                let t = stk.v(yylen, 1).node().expect("SimpleTypename");
                let bounds = stk.v(yylen, 2).list();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.arrayBounds = bounds).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            // ConstInterval opt_interval (non-empty opt_interval is loud).
            1950 => {
                let t = stk.v(yylen, 1).node().expect("ConstInterval");
                let typmods = stk.v(yylen, 2).list();
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.typmods = typmods).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            // GenericType: type_function_name [attrs] opt_type_modifiers
            1958 => {
                let name = stk.v(yylen, 1).str_val();
                let typmods = stk.v(yylen, 2).list();
                let names = NodeList::make1(mcx, Node::mk_string(mcx, name)?)?;
                *yyval =
                    YYSTYPE::Node(Some(make_type_name(mcx, names, typmods, stk.l(yylen, 1))?));
            }
            1959 => {
                let name = stk.v(yylen, 1).str_val();
                let mut names = stk.v(yylen, 2).list();
                let typmods = stk.v(yylen, 3).list();
                names.lcons(mcx, Node::mk_string(mcx, name)?)?;
                *yyval =
                    YYSTYPE::Node(Some(make_type_name(mcx, names, typmods, stk.l(yylen, 1))?));
            }
            963 => {
                let s = stk.v(yylen, 2).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, s)?)?);
            }
            964 => {
                let mut list = stk.v(yylen, 1).list();
                let s = stk.v(yylen, 3).str_val();
                list.lappend(mcx, Node::mk_string(mcx, s)?)?;
                *yyval = YYSTYPE::List(list);
            }
            // Numeric / Bit / Character / ConstDatetime SimpleTypenames.
            1962 | 1963 | 1964 | 1965 | 1966 | 1968 | 1972 | 1999 | 2019 => {
                let name = match rule {
                    1962 | 1963 => "int4",
                    1964 => "int2",
                    1965 => "int8",
                    1966 => "float4",
                    1968 => "float8",
                    1972 => "bool",
                    1999 => "interval",
                    _ => "json",
                };
                *yyval = YYSTYPE::Node(Some(system_type_name(
                    mcx,
                    name,
                    NodeList::nil(),
                    stk.l(yylen, 1),
                )?));
            }
            1967 => {
                let t = stk.v(yylen, 2).node().expect("opt_float");
                let loc = stk.l(yylen, 1);
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.location = loc).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            1969 | 1970 | 1971 => {
                let typmods = stk.v(yylen, 2).list();
                *yyval = YYSTYPE::Node(Some(system_type_name(
                    mcx,
                    "numeric",
                    typmods,
                    stk.l(yylen, 1),
                )?));
            }
            // FLOAT '(' Iconst ')': IEEE precision buckets.
            1973 => {
                let p = stk.v(yylen, 2).ival();
                let name = if p < 1 {
                    return Err(self.invalid_parameter_error(
                        "precision for type float must be at least 1 bit",
                        stk.l(yylen, 2),
                    ));
                } else if p <= 24 {
                    "float4"
                } else if p <= 53 {
                    "float8"
                } else {
                    return Err(self.invalid_parameter_error(
                        "precision for type float must be less than 54 bits",
                        stk.l(yylen, 2),
                    ));
                };
                *yyval = YYSTYPE::Node(Some(system_type_name(mcx, name, NodeList::nil(), -1)?));
            }
            1974 => {
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, "float8", NodeList::nil(), -1)?));
            }
            1979 => {
                let name = if stk.v(yylen, 2).boolean() { "varbit" } else { "bit" };
                let typmods = stk.v(yylen, 4).list();
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, stk.l(yylen, 1))?));
            }
            // bit defaults to bit(1), varbit to no limit.
            1980 => {
                let (name, typmods) = if stk.v(yylen, 2).boolean() {
                    ("varbit", NodeList::nil())
                } else {
                    ("bit", NodeList::make1(mcx, make_int_const(mcx, 1, -1)?)?)
                };
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, stk.l(yylen, 1))?));
            }
            1984 => {
                let t = stk.v(yylen, 1).node().expect("CharacterWithLength");
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.typmods = NodeList::nil())
                        .expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            1985 => {
                let name = stk.v(yylen, 1).str_val();
                let len = stk.v(yylen, 3).ival();
                let typmods =
                    NodeList::make1(mcx, make_int_const(mcx, len, stk.l(yylen, 3))?)?;
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, stk.l(yylen, 1))?));
            }
            // char defaults to char(1), varchar to no limit.
            1986 => {
                let name = stk.v(yylen, 1).str_val();
                let typmods = if name == "bpchar" {
                    NodeList::make1(mcx, make_int_const(mcx, 1, -1)?)?
                } else {
                    NodeList::nil()
                };
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, stk.l(yylen, 1))?));
            }
            1987 | 1988 | 1992 => {
                let v = stk.v(yylen, 2).boolean();
                *yyval = YYSTYPE::Str(if v { "varchar" } else { "bpchar" });
            }
            1990 | 1991 => {
                let v = stk.v(yylen, 3).boolean();
                *yyval = YYSTYPE::Str(if v { "varchar" } else { "bpchar" });
            }
            1989 => *yyval = YYSTYPE::Str("varchar"),
            1993 => *yyval = YYSTYPE::Boolean(true),
            1994 => *yyval = YYSTYPE::Boolean(false),
            1995 | 1997 => {
                let len = stk.v(yylen, 3).ival();
                let tz = stk.v(yylen, 5).boolean();
                let name = match (rule, tz) {
                    (1995, true) => "timestamptz",
                    (1995, false) => "timestamp",
                    (_, true) => "timetz",
                    _ => "time",
                };
                let typmods =
                    NodeList::make1(mcx, make_int_const(mcx, len, stk.l(yylen, 3))?)?;
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, stk.l(yylen, 1))?));
            }
            1996 | 1998 => {
                let tz = stk.v(yylen, 2).boolean();
                let name = match (rule, tz) {
                    (1996, true) => "timestamptz",
                    (1996, false) => "timestamp",
                    (_, true) => "timetz",
                    _ => "time",
                };
                *yyval = YYSTYPE::Node(Some(system_type_name(
                    mcx,
                    name,
                    NodeList::nil(),
                    stk.l(yylen, 1),
                )?));
            }
            2000 => *yyval = YYSTYPE::Boolean(true),
            2001 | 2002 => *yyval = YYSTYPE::Boolean(false),
            // a_expr TYPECAST Typename / CAST '(' a_expr AS Typename ')'
            2021 => {
                let arg = stk.v(yylen, 1).node();
                let t = stk.v(yylen, 3).node().expect("Typename");
                *yyval = YYSTYPE::Node(Some(make_type_cast(mcx, arg, t, stk.l(yylen, 2))?));
            }
            2156 => {
                let arg = stk.v(yylen, 3).node();
                let t = stk.v(yylen, 5).node().expect("Typename");
                *yyval = YYSTYPE::Node(Some(make_type_cast(mcx, arg, t, stk.l(yylen, 1))?));
            }
            2041 | 2042 => {
                let op = if rule == 2041 { BoolExprType::AND_EXPR } else { BoolExprType::OR_EXPR };
                let l = stk.v(yylen, 1).node().expect("a_expr");
                let r = stk.v(yylen, 3).node().expect("a_expr");
                *yyval = YYSTYPE::Node(Some(self.make_and_or_expr(op, l, r, stk.l(yylen, 2))?));
            }
            2043 | 2044 => {
                let arg = stk.v(yylen, 2).node().expect("a_expr");
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    BoolExpr {
                        boolop: BoolExprType::NOT_EXPR,
                        args: NodeList::make1(mcx, arg)?,
                        location: stk.l(yylen, 1),
                    },
                )?));
            }
            // IS [NOT] NULL / ISNULL / NOTNULL
            2057..=2060 => {
                let arg = stk.v(yylen, 1).node();
                let t = if rule >= 2059 {
                    NullTestType::IS_NOT_NULL
                } else {
                    NullTestType::IS_NULL
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    NullTest { arg, nulltesttype: t, argisrow: false, location: stk.l(yylen, 2) },
                )?));
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
            // func_application: func_name '(' [args] ')' shapes.
            2126 => {
                let funcname = stk.v(yylen, 1).list();
                let f = make_func_call(
                    mcx,
                    funcname,
                    NodeList::nil(),
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    stk.l(yylen, 1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            2127 | 2130 | 2131 => {
                let funcname = stk.v(yylen, 1).list();
                let args_i = if rule == 2127 { 3 } else { 4 };
                let args = stk.v(yylen, args_i).list();
                let agg_order = stk.v(yylen, args_i + 1).list();
                let mut f = make_func_call(
                    mcx,
                    funcname,
                    args,
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    stk.l(yylen, 1),
                )?;
                f.agg_order = agg_order;
                f.agg_distinct = rule == 2131;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            2128 | 2129 => {
                let funcname = stk.v(yylen, 1).list();
                let (args, agg_order) = if rule == 2128 {
                    let arg = stk.v(yylen, 4).node().expect("func_arg_expr");
                    (NodeList::make1(mcx, arg)?, stk.v(yylen, 5).list())
                } else {
                    let mut args = stk.v(yylen, 3).list();
                    let last = stk.v(yylen, 6).node().expect("func_arg_expr");
                    args.lappend(mcx, last)?;
                    (args, stk.v(yylen, 7).list())
                };
                let mut f = make_func_call(
                    mcx,
                    funcname,
                    args,
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    stk.l(yylen, 1),
                )?;
                f.func_variadic = true;
                f.agg_order = agg_order;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // AGGREGATE(*): parameterless, agg_star marks the original form.
            2132 => {
                let funcname = stk.v(yylen, 1).list();
                let mut f = make_func_call(
                    mcx,
                    funcname,
                    NodeList::nil(),
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    stk.l(yylen, 1),
                )?;
                f.agg_star = true;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // func_expr: func_application within_group_clause filter_clause
            // over_clause (OVER paths panic inside window_specification).
            2133 => {
                let f = stk.v(yylen, 1).node().expect("func_application");
                let within = stk.v(yylen, 2).list();
                let filter = stk.v(yylen, 3).node();
                let over = stk.v(yylen, 4).node();
                if !within.is_nil() {
                    let fc = f.as_func_call().expect("FuncCall");
                    let msg = if !fc.agg_order.is_nil() {
                        Some("cannot use multiple ORDER BY clauses with WITHIN GROUP")
                    } else if fc.agg_distinct {
                        Some("cannot use DISTINCT with WITHIN GROUP")
                    } else if fc.func_variadic {
                        Some("cannot use VARIADIC with WITHIN GROUP")
                    } else {
                        None
                    };
                    if let Some(msg) = msg {
                        return Err(self.errposition_error(msg.into(), stk.l(yylen, 2)));
                    }
                }
                // SAFETY: as rule 8 (the `fc` borrow above is dead here).
                unsafe {
                    f.with_mut::<FuncCall, _>(|n| {
                        if !within.is_nil() {
                            n.agg_order = within;
                            n.agg_within_group = true;
                        }
                        n.agg_filter = filter;
                        n.over = over;
                    })
                    .expect("FuncCall");
                }
                *yyval = YYSTYPE::Node(Some(f));
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
            // expr_list / func_arg_list
            2290 | 2292 => {
                let e = stk.v(yylen, 1).node().expect("a_expr");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, e)?);
            }
            2291 | 2293 => {
                let mut list = stk.v(yylen, 1).list();
                let e = stk.v(yylen, 3).node().expect("a_expr");
                list.lappend(mcx, e)?;
                *yyval = YYSTYPE::List(list);
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
            // func_name: type_function_name [indirection] (check_func_name).
            2437 => {
                let s = stk.v(yylen, 1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, s)?)?);
            }
            2438 => {
                let name = stk.v(yylen, 1).str_val();
                let mut list = stk.v(yylen, 2).list();
                for n in &list {
                    if n.as_string().is_none() {
                        return Err(self.parser_yyerror("syntax error"));
                    }
                }
                list.lcons(mcx, Node::mk_string(mcx, name)?)?;
                *yyval = YYSTYPE::List(list);
            }
            // AexprConst typed literals: func_name Sconst / ConstTypename Sconst.
            2444 => {
                let names = stk.v(yylen, 1).list();
                let s = stk.v(yylen, 2).str_val();
                let t = make_type_name(mcx, names, NodeList::nil(), stk.l(yylen, 1))?;
                *yyval = YYSTYPE::Node(Some(make_string_const_cast(
                    mcx,
                    s,
                    stk.l(yylen, 2),
                    t,
                )?));
            }
            2446 => {
                let t = stk.v(yylen, 1).node().expect("ConstTypename");
                let s = stk.v(yylen, 2).str_val();
                *yyval = YYSTYPE::Node(Some(make_string_const_cast(
                    mcx,
                    s,
                    stk.l(yylen, 2),
                    t,
                )?));
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

    // makeAndExpr/makeOrExpr: flatten onto an existing same-op BoolExpr.
    fn make_and_or_expr(
        &self,
        boolop: BoolExprType,
        lexpr: Node<'mcx>,
        rexpr: Node<'mcx>,
        location: i32,
    ) -> PgResult<Node<'mcx>> {
        if lexpr.as_bool_expr().is_some_and(|b| b.boolop == boolop) {
            let mut appended = Ok(());
            // SAFETY: as rule 8; the as_bool_expr probe above is dead here.
            unsafe {
                lexpr
                    .with_mut::<BoolExpr, _>(|b| appended = b.args.lappend(self.mcx, rexpr))
                    .expect("BoolExpr");
            }
            appended?;
            return Ok(lexpr);
        }
        Node::mk(
            self.mcx,
            BoolExpr { boolop, args: NodeList::make2(self.mcx, lexpr, rexpr)?, location },
        )
    }

    // insertSelectOptions; withClause omitted (the WITH select_no_parens
    // alternatives are unported louds), SKIP LOCKED × WITH TIES check
    // unreachable (for_locking_items is an unported loud).
    fn insert_select_options(
        &self,
        stmt: Node<'mcx>,
        sort_clause: NodeList<'mcx>,
        locking_clause: NodeList<'mcx>,
        limit: Option<&mut SelectLimit<'mcx>>,
    ) -> PgResult<()> {
        let mcx = self.mcx;
        let mut err: PgResult<()> = Ok(());
        // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
        unsafe {
            stmt.with_mut::<SelectStmt, _>(|n| {
                if !sort_clause.is_nil() {
                    if !n.sortClause.is_nil() {
                        err = Err(self.errposition_error(
                            "multiple ORDER BY clauses not allowed".into(),
                            expr_location_list(&sort_clause),
                        ));
                        return;
                    }
                    n.sortClause = sort_clause;
                }
                if let Err(e) = n.lockingClause.concat(mcx, &locking_clause) {
                    err = Err(e);
                    return;
                }
                let Some(l) = limit else { return };
                if let Some(off) = l.limitOffset {
                    if n.limitOffset.is_some() {
                        err = Err(self.errposition_error(
                            "multiple OFFSET clauses not allowed".into(),
                            l.offsetLoc,
                        ));
                        return;
                    }
                    n.limitOffset = Some(off);
                }
                if let Some(cnt) = l.limitCount {
                    if n.limitCount.is_some() {
                        err = Err(self.errposition_error(
                            "multiple LIMIT clauses not allowed".into(),
                            l.countLoc,
                        ));
                        return;
                    }
                    n.limitCount = Some(cnt);
                }
                if n.sortClause.is_nil()
                    && l.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES
                {
                    err = Err(self.errposition_error(
                        "WITH TIES cannot be specified without ORDER BY clause".into(),
                        l.optionLoc,
                    ));
                    return;
                }
                n.limitOption = l.limitOption;
            })
            .expect("SelectStmt");
        }
        err
    }

    #[cold]
    fn invalid_parameter_error(&self, message: &str, location: i32) -> Box<types_error::PgError> {
        Box::new(
            (*self.errposition_error(message.into(), location))
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        )
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

fn sortby_dir(v: i32) -> SortByDir {
    match v {
        1 => SortByDir::SORTBY_ASC,
        2 => SortByDir::SORTBY_DESC,
        3 => SortByDir::SORTBY_USING,
        _ => SortByDir::SORTBY_DEFAULT,
    }
}

fn sortby_nulls(v: i32) -> SortByNulls {
    match v {
        1 => SortByNulls::SORTBY_NULLS_FIRST,
        2 => SortByNulls::SORTBY_NULLS_LAST,
        _ => SortByNulls::SORTBY_NULLS_DEFAULT,
    }
}

fn mk_select_limit<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    limit_offset: Option<Node<'mcx>>,
    limit_count: Option<Node<'mcx>>,
    limit_option: LimitOption,
    offset_loc: i32,
    count_loc: i32,
    option_loc: i32,
) -> PgResult<&'mcx mut SelectLimit<'mcx>> {
    Ok(mcx::leak_in(mcx::alloc_in(
        mcx,
        SelectLimit {
            limitOffset: limit_offset,
            limitCount: limit_count,
            limitOption: limit_option,
            offsetLoc: offset_loc,
            countLoc: count_loc,
            optionLoc: option_loc,
        },
    )?))
}

// makeTypeName/makeTypeNameFromNameList (makefuncs.c): typemod -1; grammar
// actions pass the token location (C assigns it right after).
fn make_type_name<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    names: NodeList<'mcx>,
    typmods: NodeList<'mcx>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        TypeName {
            names,
            typeOid: 0,
            setof: false,
            pct_type: false,
            typmods,
            typemod: -1,
            arrayBounds: NodeList::nil(),
            location,
        },
    )
}

fn system_type_name<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    name: &'mcx str,
    typmods: NodeList<'mcx>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    let names = NodeList::make2(
        mcx,
        Node::mk_string(mcx, "pg_catalog")?,
        Node::mk_string(mcx, name)?,
    )?;
    make_type_name(mcx, names, typmods, location)
}

fn make_type_cast<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    arg: Option<Node<'mcx>>,
    type_name: Node<'mcx>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    Node::mk(mcx, TypeCast { arg, typeName: Some(type_name), location })
}

fn make_string_const_cast<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    s: &'mcx str,
    location: i32,
    type_name: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let sc = Node::mk_a_const(
        mcx,
        Some(ValUnion::String(types_nodes::String { sval: s })),
        location,
    )?;
    make_type_cast(mcx, Some(sc), type_name, -1)
}

fn make_int_const<'mcx>(mcx: mcx::Mcx<'mcx>, ival: i32, location: i32) -> PgResult<Node<'mcx>> {
    Node::mk_a_const(mcx, Some(ValUnion::Integer(Integer { ival })), location)
}

fn make_func_call<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    funcname: NodeList<'mcx>,
    args: NodeList<'mcx>,
    funcformat: CoercionForm,
    location: i32,
) -> PgResult<NodeMut<'mcx, FuncCall<'mcx>>> {
    Node::mk_mut(
        mcx,
        FuncCall {
            funcname,
            args,
            agg_order: NodeList::nil(),
            agg_filter: None,
            over: None,
            agg_within_group: false,
            agg_star: false,
            agg_distinct: false,
            func_variadic: false,
            funcformat,
            location,
        },
    )
}

fn leftmost_loc(loc1: i32, loc2: i32) -> i32 {
    if loc1 < 0 {
        loc2
    } else if loc2 < 0 {
        loc1
    } else {
        loc1.min(loc2)
    }
}

fn expr_location_opt(n: Option<Node<'_>>) -> i32 {
    n.map_or(-1, expr_location)
}

fn expr_location_list(l: &NodeList<'_>) -> i32 {
    for n in l {
        let loc = expr_location(n);
        if loc >= 0 {
            return loc;
        }
    }
    -1
}

// exprLocation (nodeFuncs.c), the raw-node arms this grammar can produce;
// anything else is a loud gap, not a silent -1.
fn expr_location(n: Node<'_>) -> i32 {
    if let Some(sb) = n.as_sort_by() {
        expr_location_opt(sb.node)
    } else if let Some(c) = n.as_a_const() {
        c.location
    } else if let Some(cr) = n.as_column_ref() {
        cr.location
    } else if let Some(p) = n.as_param_ref() {
        p.location
    } else if let Some(e) = n.as_a_expr() {
        leftmost_loc(e.location, expr_location_opt(e.lexpr))
    } else if let Some(f) = n.as_func_call() {
        leftmost_loc(f.location, expr_location_list(&f.args))
    } else if let Some(b) = n.as_bool_expr() {
        leftmost_loc(b.location, expr_location_list(&b.args))
    } else if let Some(nt) = n.as_null_test() {
        leftmost_loc(nt.location, expr_location_opt(nt.arg))
    } else if let Some(tc) = n.as_type_cast() {
        let mut loc = expr_location_opt(tc.arg);
        loc = leftmost_loc(loc, expr_location_opt(tc.typeName));
        leftmost_loc(loc, tc.location)
    } else if let Some(t) = n.as_type_name() {
        t.location
    } else {
        panic!("gram_core: exprLocation arm unported for tag {:?}", n.node_tag())
    }
}
