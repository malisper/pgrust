use types_core::catalog::RELPERSISTENCE_PERMANENT;
use types_error::PgResult;
use types_nodes::parsenodes::{
    CopyStmt, DeallocateStmt, DefElem, DefElemAction, ExecuteStmt, PrepareStmt, VacuumRelation,
    VacuumStmt,
};
use types_nodes::rawnodes::A_Expr_Kind::AEXPR_OP;
use types_nodes::rawnodes::{ColumnDef, Constraint, ConstrType, CreateStmt, OnCommitAction};
use types_nodes::{
    Alias, DeleteStmt, InsertStmt, Node, NodeList, RangeFunction, RangeVar, RawStmt, SelectStmt,
    UpdateStmt,
    ValUnion,
};
use types_nodes::{BitString, Boolean, Float, Integer};
use types_nodes::{
    BoolExpr, BoolExprType, CoercionForm, DistinctClause, FuncCall, LimitOption, NodeMut,
    NullTest, NullTestType, SortBy, SortByDir, SortByNulls, TypeCast, TypeName,
};

use crate::parse::Parser;
use crate::stack::ActionView;
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
        view: ActionView<'mcx>,
        rule: usize,
        yyval: &mut YYSTYPE<'mcx>,
        yyloc: i32,
    ) -> PgResult<()> {
        let mcx = self.mcx;
        let _ = yyloc;
        match rule {
            2 => self.parsetree = view.v(1).list(),
            // stmtmulti: stmtmulti ';' toplevel_stmt
            8 => {
                let mut list = view.v(1).list();
                if !list.is_nil() {
                    let end = view.l(2);
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
                if let Some(stmt) = view.v(3).node() {
                    let loc = view.l(3);
                    list.lappend(mcx, Node::mk_raw_stmt(mcx, Some(stmt), loc, 0)?)?;
                }
                *yyval = YYSTYPE::List(list);
            }
            // stmtmulti: toplevel_stmt
            9 => {
                *yyval = YYSTYPE::List(match view.v(1).node() {
                    Some(stmt) => {
                        let loc = view.l(1);
                        NodeList::make1(mcx, Node::mk_raw_stmt(mcx, Some(stmt), loc, 0)?)?
                    }
                    None => NodeList::nil(),
                });
            }
            // CreateStmt: CREATE OptTemp TABLE qualified_name '('
            // OptTableElementList ')' OptInherit OptPartitionSpec
            // table_access_method_clause OptWith OnCommitOption OptTableSpace
            1719 | 1720 => {
                let mut n = Node::build::<SelectStmt>(mcx)?;
                if rule == 1720 {
                    let v = view.v(2);
                    n.distinctClause = if v.is_distinct_all() {
                        DistinctClause::All
                    } else {
                        DistinctClause::On(v.list())
                    };
                }
                n.targetList = view.v(3).list();
                n.intoClause = view.v(4).node();
                n.fromClause = view.v(5).list();
                n.whereClause = view.v(6).node();
                let (distinct, list) = view.v(7).group();
                n.groupClause = list;
                n.groupDistinct = distinct;
                n.havingClause = view.v(8).node();
                n.windowClause = view.v(9).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // select_no_parens: select_clause sort_clause | select_clause
            // opt_sort_clause [for_locking_clause select_limit] (both orders);
            // WITH variants (1713..1716) stay unported.
            1799 => {
                *yyval = YYSTYPE::Group(false, NodeList::nil());
            }
            1830 => {
                let t = view.v(1).node().expect("table_ref");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            1831 => {
                let mut list = view.v(1).list();
                let t = view.v(3).node().expect("table_ref");
                list.lappend(mcx, t)?;
                *yyval = YYSTYPE::List(list);
            }
            // InsertStmt: opt_with_clause INSERT INTO insert_target insert_rest
            //             opt_on_conflict returning_clause
            1832 => {
                let rv = view.v(1).node().expect("relation_expr");
                let alias = view.v(2).alias();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.alias = alias)
                        .expect("relation_expr is RangeVar");
                }
                *yyval = YYSTYPE::Node(Some(rv));
            }
            1851 => {
                let name = view.v(2).str_val();
                *yyval = YYSTYPE::Alias(Some(mk_alias(mcx, name)?));
            }
            1853 => {
                let name = view.v(1).str_val();
                *yyval = YYSTYPE::Alias(Some(mk_alias(mcx, name)?));
            }
            1871 | 1873 | 1874 | 1875 => {
                let arg = match rule {
                    1874 => 2,
                    1875 => 3,
                    _ => 1,
                };
                let rv = view.v(arg).node().expect("qualified_name");
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
            2026 => {
                let n = view.v(2).node().expect("a_expr");
                *yyval = self.do_negate(n, view.l(1))?;
            }
            2027..=2038 => {
                let op = MATH_OPS[rule - 2027];
                let l = view.v(1).node();
                let r = view.v(3).node();
                *yyval = self.simple_a_expr(op, l, r, view.l(2))?;
            }
            2114 => {
                let number = view.v(1).ival();
                let ind = view.v(2).list();
                if !ind.is_nil() {
                    panic!("gram_core: A_Indirection over PARAM not ported (types_nodes gap)");
                }
                *yyval = YYSTYPE::Node(Some(Node::mk_param_ref(mcx, number, view.l(1))?));
            }
            2338 | 2339 => {
                let name = view.v(1).str_val();
                let ind = if rule == 2339 { view.v(2).list() } else { NodeList::nil() };
                *yyval = YYSTYPE::Node(Some(self.make_column_ref(
                    name,
                    ind,
                    view.l(1),
                )?));
            }
            2340 => {
                let s = view.v(2).str_val();
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, s)?));
            }
            2346 => {
                let el = view.v(1).node().expect("indirection_el");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            2422 => {
                let t = view.v(1).node().expect("target_el");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            2423 => {
                let mut list = view.v(1).list();
                let t = view.v(3).node().expect("target_el");
                list.lappend(mcx, t)?;
                *yyval = YYSTYPE::List(list);
            }
            2424..=2427 => {
                let (name, val) = match rule {
                    2424 => {
                        let val = view.v(1).node();
                        (Some(view.v(3).str_val()), val)
                    }
                    2425 => {
                        let val = view.v(1).node();
                        (Some(view.v(2).str_val()), val)
                    }
                    2426 => (None, view.v(1).node()),
                    _ => {
                        let star = NodeList::make1(mcx, Node::mk_a_star(mcx)?)?;
                        (None, Some(Node::mk_column_ref(mcx, star, view.l(1))?))
                    }
                };
                let loc = view.l(1);
                *yyval = YYSTYPE::Node(Some(Node::mk_res_target(
                    mcx,
                    name,
                    NodeList::nil(),
                    val,
                    loc,
                )?));
            }
            2430 => {
                let relname = view.v(1).str_val();
                let rv = make_range_var(mcx, None, None, Some(relname), view.l(1))?;
                *yyval = YYSTYPE::Node(Some(rv));
            }
            2431 => {
                let name = view.v(1).str_val();
                let ind = view.v(2).list();
                let loc = view.l(1);
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
            2439 => {
                let v = view.v(1).ival();
                *yyval = self.a_const(ValUnion::Integer(Integer { ival: v }), view.l(1))?;
            }
            2440 => {
                let s = view.v(1).str_val();
                *yyval = self.a_const(ValUnion::Float(Float { fval: s }), view.l(1))?;
            }
            2441 => {
                let s = view.v(1).str_val();
                *yyval = self.a_const(
                    ValUnion::String(types_nodes::String { sval: s }),
                    view.l(1),
                )?;
            }
            2449 => *yyval = self.a_const(ValUnion::Boolean(Boolean { boolval: true }), view.l(1))?,
            2451 => {
                *yyval =
                    YYSTYPE::Node(Some(Node::mk_a_const(mcx, None, view.l(1))?));
            }
            _ => return self.reduce_cold(view, rule, yyval, yyloc),
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn reduce_cold(
        &mut self,
        view: ActionView<'mcx>,
        rule: usize,
        yyval: &mut YYSTYPE<'mcx>,
        yyloc: i32,
    ) -> PgResult<()> {
        let mcx = self.mcx;
        let _ = yyloc;
        match rule {
            455 => {
                let persistence = view.v(2).ival() as u8;
                let relation = view.v(4).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let mut n = Node::build::<CreateStmt>(mcx)?;
                n.relation = relation.as_variant::<RangeVar>();
                n.tableElts = view.v(6).list();
                n.inhRelations = view.v(8).list();
                n.partspec = view.v(9).node();
                n.accessMethod = opt_str(view.v(10));
                n.options = view.v(11).list();
                n.oncommit = on_commit_action(view.v(12).ival());
                n.tablespacename = opt_str(view.v(13));
                n.if_not_exists = false;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OptTemp: /*EMPTY*/ (TEMP/UNLOGGED variants stay unported).
            468 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_PERMANENT as i32),
            // TableElementList: TableElement | TableElementList ',' TableElement
            473 => {
                let el = view.v(1).node().expect("TableElement");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            474 => {
                let mut list = view.v(1).list();
                let el = view.v(3).node().expect("TableElement");
                list.lappend(mcx, el)?;
                *yyval = YYSTYPE::List(list);
            }
            // columnDef: ColId Typename opt_column_storage
            // opt_column_compression create_generic_options ColQualList
            482 => {
                let colname = view.v(1).str_val();
                let type_name = view.v(2).node();
                let storage_name = opt_str(view.v(3));
                let compression = opt_str(view.v(4));
                let fdwoptions = view.v(5).list();
                let quals = view.v(6).list();
                // SplitColQualList: COLLATE splits out; Constraints stay.
                let mut constraints = NodeList::nil();
                for q in quals.iter() {
                    match q.node_tag() {
                        NodeTag::T_Constraint => constraints.lappend(mcx, q)?,
                        NodeTag::T_CollateClause => panic!(
                            "gram_core: SplitColQualList COLLATE arm unported"
                        ),
                        other => panic!("unexpected node type {other:?} in ColQualList"),
                    }
                }
                let mut n = Node::build::<ColumnDef>(mcx)?;
                n.colname = Some(colname);
                n.typeName = type_name;
                n.storage_name = storage_name;
                n.compression = compression;
                n.is_local = true;
                n.constraints = constraints;
                n.fdwoptions = fdwoptions;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ColQualList: ColQualList ColConstraint
            493 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("ColConstraint"))?;
                *yyval = YYSTYPE::List(list);
            }
            // ColConstraint: CONSTRAINT name ColConstraintElem
            495 => {
                let name = view.v(2).str_val();
                let node = view.v(3).node().expect("ColConstraintElem");
                let loc = view.l(1);
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    node.with_mut::<Constraint, _>(|c| {
                        c.conname = Some(name);
                        c.location = loc;
                    })
                    .expect("ColConstraintElem is Constraint");
                }
                *yyval = YYSTYPE::Node(Some(node));
            }
            // ColConstraintElem: CHECK '(' a_expr ')' opt_no_inherit
            503 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_CHECK;
                n.location = view.l(1);
                n.is_no_inherit = view.v(5).boolean();
                n.raw_expr = view.v(3).node();
                n.is_enforced = true;
                n.initially_valid = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ColConstraintElem: DEFAULT b_expr
            504 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_DEFAULT;
                n.location = view.l(1);
                n.raw_expr = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_no_inherit: NO INHERIT | /*EMPTY*/
            550 => *yyval = YYSTYPE::Boolean(true),
            551 => *yyval = YYSTYPE::Boolean(false),
            // OnCommitOption: /*EMPTY*/
            605 => *yyval = YYSTYPE::Ival(OnCommitAction::ONCOMMIT_NOOP as i32),
            1710 => {
                let stmt = view.v(1).node().expect("select_clause");
                let sort = view.v(2).list();
                self.insert_select_options(stmt, sort, NodeList::nil(), None)?;
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            1711 | 1712 => {
                let stmt = view.v(1).node().expect("select_clause");
                let sort = view.v(2).list();
                let (lock_i, limit_i) = if rule == 1711 { (3, 4) } else { (4, 3) };
                let locking = view.v(lock_i).list();
                let limit = view.v(limit_i).limit();
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
            // set_quantifier: ALL | DISTINCT | EMPTY (SetQuantifier values).
            1756 => *yyval = YYSTYPE::Ival(1),
            1757 => *yyval = YYSTYPE::Ival(2),
            1758 => *yyval = YYSTYPE::Ival(0),
            1759 => *yyval = YYSTYPE::DistinctAll,
            1768 => {
                let s = view.v(1).node().expect("sortby");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, s)?);
            }
            1769 => {
                let mut list = view.v(1).list();
                let s = view.v(3).node().expect("sortby");
                list.lappend(mcx, s)?;
                *yyval = YYSTYPE::List(list);
            }
            1770 => {
                let node = view.v(1).node();
                let use_op = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    SortBy {
                        node,
                        sortby_dir: SortByDir::SORTBY_USING,
                        sortby_nulls: sortby_nulls(view.v(4).ival()),
                        useOp: use_op,
                        location: view.l(3),
                    },
                )?));
            }
            1771 => {
                let node = view.v(1).node();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    SortBy {
                        node,
                        sortby_dir: sortby_dir(view.v(2).ival()),
                        sortby_nulls: sortby_nulls(view.v(3).ival()),
                        useOp: NodeList::nil(),
                        location: -1,
                    },
                )?));
            }
            // select_limit: limit_clause offset_clause (either order) / alone.
            1772 | 1773 => {
                let (sl_i, off_i) = if rule == 1772 { (1, 2) } else { (2, 1) };
                let sl = view.v(sl_i).limit().expect("limit_clause");
                sl.limitOffset = view.v(off_i).node();
                sl.offsetLoc = view.l(off_i);
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1775 => {
                let offset = view.v(1).node();
                let sl = mk_select_limit(
                    mcx,
                    offset,
                    None,
                    LimitOption::LIMIT_OPTION_COUNT,
                    view.l(1),
                    -1,
                    -1,
                )?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1778 => {
                let count = view.v(2).node();
                let sl = mk_select_limit(
                    mcx,
                    None,
                    count,
                    LimitOption::LIMIT_OPTION_COUNT,
                    -1,
                    view.l(1),
                    -1,
                )?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1779 => {
                return Err(Box::new(
                    (*self.errposition_error(
                        "LIMIT #,# syntax is not supported".into(),
                        view.l(1),
                    ))
                    .with_hint("Use separate LIMIT and OFFSET clauses."),
                ));
            }
            // FETCH { FIRST | NEXT } [count] { ROW | ROWS } { ONLY | WITH TIES }
            1780 | 1781 => {
                let count = view.v(3).node();
                let (option, option_loc) = if rule == 1781 {
                    (LimitOption::LIMIT_OPTION_WITH_TIES, view.l(5))
                } else {
                    (LimitOption::LIMIT_OPTION_COUNT, -1)
                };
                let sl =
                    mk_select_limit(mcx, None, count, option, -1, view.l(1), option_loc)?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            1782 | 1783 => {
                let count = Some(make_int_const(mcx, 1, -1)?);
                let (option, option_loc) = if rule == 1783 {
                    (LimitOption::LIMIT_OPTION_WITH_TIES, view.l(4))
                } else {
                    (LimitOption::LIMIT_OPTION_COUNT, -1)
                };
                let sl =
                    mk_select_limit(mcx, None, count, option, -1, view.l(1), option_loc)?;
                *yyval = YYSTYPE::Limit(Some(sl));
            }
            // LIMIT ALL is a NULL constant.
            1787 => {
                *yyval = YYSTYPE::Node(Some(Node::mk_a_const(mcx, None, view.l(1))?));
            }
            1790 => {
                let r = view.v(2).node();
                *yyval = self.simple_a_expr("+", None, r, view.l(1))?;
            }
            1791 => {
                let n = view.v(2).node().expect("I_or_F_const");
                *yyval = self.do_negate(n, view.l(1))?;
            }
            1792 => {
                let v = view.v(1).ival();
                *yyval = self.a_const(ValUnion::Integer(Integer { ival: v }), view.l(1))?;
            }
            1793 => {
                let s = view.v(1).str_val();
                *yyval = self.a_const(ValUnion::Float(Float { fval: s }), view.l(1))?;
            }
            // row_or_rows / first_or_next (values unused downstream).
            1794..=1797 => *yyval = YYSTYPE::Ival(0),
            // group_clause: GROUP_P BY set_quantifier group_by_list | EMPTY.
            1798 => {
                let quantifier = view.v(3).ival();
                let list = view.v(4).list();
                *yyval = YYSTYPE::Group(quantifier == 2, list);
            }
            // group_by_list; group_by_item's a_expr arm passes through, the
            // grouping-sets arms (empty/CUBE/ROLLUP/SETS) panic loudly.
            1800 => {
                let item = view.v(1).node().expect("group_by_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, item)?);
            }
            1801 => {
                let mut list = view.v(1).list();
                let item = view.v(3).node().expect("group_by_item");
                list.lappend(mcx, item)?;
                *yyval = YYSTYPE::List(list);
            }
            1617 => {
                let istmt = view.v(5).node().expect("insert_rest");
                let relation = view.v(4).node();
                let onconflict = view.v(6).node();
                let retclause = view.v(7).node();
                let with = view.v(1).node();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    istmt
                        .with_mut::<InsertStmt, _>(|n| {
                            n.relation = relation;
                            n.onConflictClause = onconflict;
                            n.returningClause = retclause;
                            n.withClause = with;
                        })
                        .expect("insert_rest is InsertStmt");
                }
                *yyval = YYSTYPE::Node(Some(istmt));
            }
            // returning_clause: RETURNING returning_with_clause target_list
            1636 => panic!(
                "gram_core: RETURNING clause (ReturningClause vocabulary) not ported"
            ),
            // DeleteStmt: opt_with_clause DELETE_P FROM relation_expr_opt_alias
            //             using_clause where_or_current_clause returning_clause
            1645 => {
                let n = Node::mk(
                    mcx,
                    DeleteStmt {
                        relation: view.v(4).node(),
                        usingClause: view.v(5).list(),
                        whereClause: view.v(6).node(),
                        returningClause: view.v(7).node(),
                        withClause: view.v(1).node(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // UpdateStmt: opt_with_clause UPDATE relation_expr_opt_alias SET
            //             set_clause_list from_clause where_or_current_clause
            //             returning_clause
            1664 => {
                let n = Node::mk(
                    mcx,
                    UpdateStmt {
                        relation: view.v(3).node(),
                        targetList: view.v(5).list(),
                        whereClause: view.v(7).node(),
                        fromClause: view.v(6).list(),
                        returningClause: view.v(8).node(),
                        withClause: view.v(1).node(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // set_clause_list: set_clause_list ',' set_clause (list_concat)
            1666 => {
                let mut list = view.v(1).list();
                list.concat(mcx, &view.v(3).list())?;
                *yyval = YYSTYPE::List(list);
            }
            // set_clause: set_target '=' a_expr
            1667 => {
                let target = view.v(1).node().expect("set_target");
                let val = view.v(3).node();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    target
                        .with_mut::<types_nodes::ResTarget, _>(|r| r.val = val)
                        .expect("set_target is ResTarget");
                }
                *yyval = YYSTYPE::List(NodeList::make1(mcx, target)?);
            }
            // set_clause: '(' set_target_list ')' '=' a_expr
            1668 => panic!(
                "gram_core: multiple-assignment SET (MultiAssignRef) not ported"
            ),
            // set_target: ColId opt_indirection (check_indirection is a no-op:
            // A_Indices construction is an unported loud).
            1669 => {
                let name = view.v(1).str_val();
                let indirection = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(Node::mk_res_target(
                    mcx,
                    Some(name),
                    indirection,
                    None,
                    view.l(1),
                )?));
            }
            1670 => {
                let t = view.v(1).node().expect("set_target");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            1671 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("set_target"))?;
                *yyval = YYSTYPE::List(list);
            }
            // relation_expr_opt_alias: relation_expr [AS] ColId
            1879 | 1880 => {
                let rv = view.v(1).node().expect("relation_expr");
                let name_i = if rule == 1880 { 3 } else { 2 };
                let alias = mk_alias(mcx, view.v(name_i).str_val())?;
                // SAFETY: as rule 8.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.alias = Some(alias))
                        .expect("relation_expr is RangeVar");
                }
                *yyval = YYSTYPE::Node(Some(rv));
            }
            // where_or_current_clause: WHERE CURRENT_P OF cursor_name
            1896 => panic!(
                "gram_core: WHERE CURRENT OF (CurrentOfExpr) not ported"
            ),
            // insert_target: qualified_name AS ColId
            1619 => {
                let rv = view.v(1).node().expect("qualified_name");
                let alias = mk_alias(mcx, view.v(3).str_val())?;
                // SAFETY: as rule 8.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.alias = Some(alias))
                        .expect("insert_target is RangeVar");
                }
                *yyval = YYSTYPE::Node(Some(rv));
            }
            // insert_rest: SelectStmt | '(' insert_column_list ')' SelectStmt
            //            | DEFAULT VALUES
            1620 | 1622 | 1624 => {
                let mut n = Node::build::<InsertStmt>(mcx)?;
                if rule == 1622 {
                    n.cols = view.v(2).list();
                }
                if rule != 1624 {
                    n.selectStmt = view.v(if rule == 1622 { 4 } else { 1 }).node();
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1627 => {
                let t = view.v(1).node().expect("insert_column_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            1628 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("insert_column_item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // insert_column_item: ColId opt_indirection (check_indirection is
            // a no-op here: A_Indices construction is an unported loud).
            1629 => {
                let name = view.v(1).str_val();
                let indirection = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(Node::mk_res_target(
                    mcx,
                    Some(name),
                    indirection,
                    None,
                    view.l(1),
                )?));
            }
            // values_clause: VALUES '(' expr_list ')' | values_clause ',' ...
            1826 => {
                let row = Node::mk_list(mcx, view.v(3).list())?;
                let mut n = Node::build::<SelectStmt>(mcx)?;
                n.valuesLists = NodeList::make1(mcx, row)?;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1827 => {
                let stmt = view.v(1).node().expect("values_clause");
                let row = Node::mk_list(mcx, view.v(4).list())?;
                // SAFETY: as rule 8.
                unsafe {
                    stmt.with_mut::<SelectStmt, _>(|n| n.valuesLists.lappend(mcx, row))
                        .expect("values_clause is SelectStmt")?;
                }
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            1834 | 1835 => {
                let fpos = if rule == 1835 { 2 } else { 1 };
                let rf = view.v(fpos).node().expect("func_table");
                let (alias, coldeflist) = view.v(fpos + 1).func_alias();
                // SAFETY: as rule 8.
                unsafe {
                    rf.with_mut::<RangeFunction, _>(|n| {
                        n.lateral = rule == 1835;
                        n.alias = alias;
                        n.coldeflist = coldeflist;
                    })
                    .expect("func_table is RangeFunction");
                }
                *yyval = YYSTYPE::Node(Some(rf));
            }
            // func_alias_clause coldeflist arms stay unimplemented-rule louds.
            1858 => {
                let alias = view.v(1).alias();
                *yyval = YYSTYPE::FuncAlias(alias, NodeList::nil());
            }
            1862 => {
                *yyval = YYSTYPE::FuncAlias(None, NodeList::nil());
            }
            // DIVERGENCE: functions holds bare funcexprs, not C's (funcexpr,
            // coldeflist) sublists; the ROWS FROM lane restores the pair shape.
            1884 => {
                let fexpr = view.v(1).node().expect("func_expr_windowless");
                let ordinality = view.v(2).boolean();
                let mut n = Node::build::<RangeFunction>(mcx)?;
                n.ordinality = ordinality;
                n.functions = NodeList::make1(mcx, fexpr)?;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1891 => *yyval = YYSTYPE::Boolean(true),
            1892 => *yyval = YYSTYPE::Boolean(false),
            // relation_expr: qualified_name; extended_relation_expr:
            //   qualified_name '*' | ONLY qualified_name | ONLY '(' q_n ')'
            1936 => {
                let t = view.v(1).node().expect("SimpleTypename");
                let bounds = view.v(2).list();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.arrayBounds = bounds).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            // ConstInterval opt_interval (non-empty opt_interval is loud).
            1950 => {
                let t = view.v(1).node().expect("ConstInterval");
                let typmods = view.v(2).list();
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.typmods = typmods).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            // GenericType: type_function_name [attrs] opt_type_modifiers
            1958 => {
                let name = view.v(1).str_val();
                let typmods = view.v(2).list();
                let names = NodeList::make1(mcx, Node::mk_string(mcx, name)?)?;
                *yyval =
                    YYSTYPE::Node(Some(make_type_name(mcx, names, typmods, view.l(1))?));
            }
            1959 => {
                let name = view.v(1).str_val();
                let mut names = view.v(2).list();
                let typmods = view.v(3).list();
                names.lcons(mcx, Node::mk_string(mcx, name)?)?;
                *yyval =
                    YYSTYPE::Node(Some(make_type_name(mcx, names, typmods, view.l(1))?));
            }
            963 => {
                let s = view.v(2).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, s)?)?);
            }
            964 => {
                let mut list = view.v(1).list();
                let s = view.v(3).str_val();
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
                    view.l(1),
                )?));
            }
            1967 => {
                let t = view.v(2).node().expect("opt_float");
                let loc = view.l(1);
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.location = loc).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            1969 | 1970 | 1971 => {
                let typmods = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(system_type_name(
                    mcx,
                    "numeric",
                    typmods,
                    view.l(1),
                )?));
            }
            // FLOAT '(' Iconst ')': IEEE precision buckets.
            1973 => {
                let p = view.v(2).ival();
                let name = if p < 1 {
                    return Err(self.invalid_parameter_error(
                        "precision for type float must be at least 1 bit",
                        view.l(2),
                    ));
                } else if p <= 24 {
                    "float4"
                } else if p <= 53 {
                    "float8"
                } else {
                    return Err(self.invalid_parameter_error(
                        "precision for type float must be less than 54 bits",
                        view.l(2),
                    ));
                };
                *yyval = YYSTYPE::Node(Some(system_type_name(mcx, name, NodeList::nil(), -1)?));
            }
            1974 => {
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, "float8", NodeList::nil(), -1)?));
            }
            1979 => {
                let name = if view.v(2).boolean() { "varbit" } else { "bit" };
                let typmods = view.v(4).list();
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, view.l(1))?));
            }
            // bit defaults to bit(1), varbit to no limit.
            1980 => {
                let (name, typmods) = if view.v(2).boolean() {
                    ("varbit", NodeList::nil())
                } else {
                    ("bit", NodeList::make1(mcx, make_int_const(mcx, 1, -1)?)?)
                };
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, view.l(1))?));
            }
            1984 => {
                let t = view.v(1).node().expect("CharacterWithLength");
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.typmods = NodeList::nil())
                        .expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            1985 => {
                let name = view.v(1).str_val();
                let len = view.v(3).ival();
                let typmods =
                    NodeList::make1(mcx, make_int_const(mcx, len, view.l(3))?)?;
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, view.l(1))?));
            }
            // char defaults to char(1), varchar to no limit.
            1986 => {
                let name = view.v(1).str_val();
                let typmods = if name == "bpchar" {
                    NodeList::make1(mcx, make_int_const(mcx, 1, -1)?)?
                } else {
                    NodeList::nil()
                };
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, view.l(1))?));
            }
            1987 | 1988 | 1992 => {
                let v = view.v(2).boolean();
                *yyval = YYSTYPE::Str(if v { "varchar" } else { "bpchar" });
            }
            1990 | 1991 => {
                let v = view.v(3).boolean();
                *yyval = YYSTYPE::Str(if v { "varchar" } else { "bpchar" });
            }
            1989 => *yyval = YYSTYPE::Str("varchar"),
            1993 => *yyval = YYSTYPE::Boolean(true),
            1994 => *yyval = YYSTYPE::Boolean(false),
            1995 | 1997 => {
                let len = view.v(3).ival();
                let tz = view.v(5).boolean();
                let name = match (rule, tz) {
                    (1995, true) => "timestamptz",
                    (1995, false) => "timestamp",
                    (_, true) => "timetz",
                    _ => "time",
                };
                let typmods =
                    NodeList::make1(mcx, make_int_const(mcx, len, view.l(3))?)?;
                *yyval =
                    YYSTYPE::Node(Some(system_type_name(mcx, name, typmods, view.l(1))?));
            }
            1996 | 1998 => {
                let tz = view.v(2).boolean();
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
                    view.l(1),
                )?));
            }
            2000 => *yyval = YYSTYPE::Boolean(true),
            2001 | 2002 => *yyval = YYSTYPE::Boolean(false),
            // a_expr TYPECAST Typename / CAST '(' a_expr AS Typename ')'
            2021 => {
                let arg = view.v(1).node();
                let t = view.v(3).node().expect("Typename");
                *yyval = YYSTYPE::Node(Some(make_type_cast(mcx, arg, t, view.l(2))?));
            }
            2156 => {
                let arg = view.v(3).node();
                let t = view.v(5).node().expect("Typename");
                *yyval = YYSTYPE::Node(Some(make_type_cast(mcx, arg, t, view.l(1))?));
            }
            2041 | 2042 => {
                let op = if rule == 2041 { BoolExprType::AND_EXPR } else { BoolExprType::OR_EXPR };
                let l = view.v(1).node().expect("a_expr");
                let r = view.v(3).node().expect("a_expr");
                *yyval = YYSTYPE::Node(Some(self.make_and_or_expr(op, l, r, view.l(2))?));
            }
            2043 | 2044 => {
                let arg = view.v(2).node().expect("a_expr");
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    BoolExpr {
                        boolop: BoolExprType::NOT_EXPR,
                        args: NodeList::make1(mcx, arg)?,
                        location: view.l(1),
                    },
                )?));
            }
            // IS [NOT] NULL / ISNULL / NOTNULL
            2057..=2060 => {
                let arg = view.v(1).node();
                let t = if rule >= 2059 {
                    NullTestType::IS_NOT_NULL
                } else {
                    NullTestType::IS_NULL
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    NullTest { arg, nulltesttype: t, argisrow: false, location: view.l(2) },
                )?));
            }
            2025 => {
                let r = view.v(2).node();
                *yyval = self.simple_a_expr("+", None, r, view.l(1))?;
            }
            2039 => {
                let name = view.v(2).list();
                let l = view.v(1).node();
                let r = view.v(3).node();
                *yyval =
                    YYSTYPE::Node(Some(make_a_expr(mcx, name, l, r, view.l(2))?));
            }
            2040 => {
                let name = view.v(1).list();
                let r = view.v(2).node();
                *yyval =
                    YYSTYPE::Node(Some(make_a_expr(mcx, name, None, r, view.l(1))?));
            }
            // c_expr: PARAM / parenthesized a_expr (opt_indirection)
            2115 => {
                let e = view.v(2);
                let ind = view.v(4).list();
                if !ind.is_nil() {
                    panic!("gram_core: A_Indirection over (a_expr) not ported (types_nodes gap)");
                }
                *yyval = e;
            }
            // c_expr: select_with_parens %prec UMINUS
            2118 => {
                let subselect = view.v(1).node().expect("select_with_parens");
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::SubLink {
                        subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
                        subLinkId: 0,
                        testexpr: None,
                        operName: NodeList::nil(),
                        subselect,
                        location: view.l(1),
                    },
                )?));
            }
            2119 => panic!(
                "gram_core: indirection over a sub-SELECT (A_Indirection) not ported \
                 (unit backend-parser-gram)"
            ),
            // c_expr: EXISTS select_with_parens
            2120 => {
                let subselect = view.v(2).node().expect("select_with_parens");
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::SubLink {
                        subLinkType: types_nodes::SubLinkType::EXISTS_SUBLINK,
                        subLinkId: 0,
                        testexpr: None,
                        operName: NodeList::nil(),
                        subselect,
                        location: view.l(1),
                    },
                )?));
            }
            2121 => panic!(
                "gram_core: ARRAY_SUBLINK (ARRAY select_with_parens) not ported \
                 (unit backend-parser-gram)"
            ),
            // func_application: func_name '(' [args] ')' shapes.
            2126 => {
                let funcname = view.v(1).list();
                let f = make_func_call(
                    mcx,
                    funcname,
                    NodeList::nil(),
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            2127 | 2130 | 2131 => {
                let funcname = view.v(1).list();
                let args_i = if rule == 2127 { 3 } else { 4 };
                let args = view.v(args_i).list();
                let agg_order = view.v(args_i + 1).list();
                let mut f = make_func_call(
                    mcx,
                    funcname,
                    args,
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                f.agg_order = agg_order;
                f.agg_distinct = rule == 2131;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            2128 | 2129 => {
                let funcname = view.v(1).list();
                let (args, agg_order) = if rule == 2128 {
                    let arg = view.v(4).node().expect("func_arg_expr");
                    (NodeList::make1(mcx, arg)?, view.v(5).list())
                } else {
                    let mut args = view.v(3).list();
                    let last = view.v(6).node().expect("func_arg_expr");
                    args.lappend(mcx, last)?;
                    (args, view.v(7).list())
                };
                let mut f = make_func_call(
                    mcx,
                    funcname,
                    args,
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                f.func_variadic = true;
                f.agg_order = agg_order;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // AGGREGATE(*): parameterless, agg_star marks the original form.
            2132 => {
                let funcname = view.v(1).list();
                let mut f = make_func_call(
                    mcx,
                    funcname,
                    NodeList::nil(),
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                f.agg_star = true;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // func_expr: func_application within_group_clause filter_clause
            // over_clause (OVER paths panic inside window_specification).
            2133 => {
                let f = view.v(1).node().expect("func_application");
                let within = view.v(2).list();
                let filter = view.v(3).node();
                let over = view.v(4).node();
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
                        return Err(self.errposition_error(msg.into(), view.l(2)));
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
                let op = view.v(1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, op)?)?);
            }
            2286..=2289 => {
                let op = ["~~", "!~~", "~~*", "!~~*"][rule - 2286];
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, op)?)?);
            }
            // expr_list / func_arg_list
            2290 | 2292 => {
                let e = view.v(1).node().expect("a_expr");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, e)?);
            }
            2291 | 2293 => {
                let mut list = view.v(1).list();
                let e = view.v(3).node().expect("a_expr");
                list.lappend(mcx, e)?;
                *yyval = YYSTYPE::List(list);
            }
            2341 => *yyval = YYSTYPE::Node(Some(Node::mk_a_star(mcx)?)),
            2342 | 2343 => {
                panic!("gram_core: A_Indices subscripting not ported (types_nodes gap)")
            }
            2347 => {
                let mut list = view.v(1).list();
                let el = view.v(2).node().expect("indirection_el");
                list.lappend(mcx, el)?;
                *yyval = YYSTYPE::List(list);
            }
            2349 => {
                let mut list = view.v(1).list();
                let el = view.v(2).node().expect("indirection_el");
                list.lappend(mcx, el)?;
                *yyval = YYSTYPE::List(list);
            }
            2428 => {
                let q = view.v(1).node().expect("qualified_name");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, q)?);
            }
            2429 => {
                let mut list = view.v(1).list();
                let q = view.v(3).node().expect("qualified_name");
                list.lappend(mcx, q)?;
                *yyval = YYSTYPE::List(list);
            }
            2432 => {
                let s = view.v(1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, s)?)?);
            }
            2433 => {
                let mut list = view.v(1).list();
                let s = view.v(3).str_val();
                list.lappend(mcx, Node::mk_string(mcx, s)?)?;
                *yyval = YYSTYPE::List(list);
            }
            2437 => {
                let s = view.v(1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, s)?)?);
            }
            2438 => {
                let name = view.v(1).str_val();
                let mut list = view.v(2).list();
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
                let names = view.v(1).list();
                let s = view.v(2).str_val();
                let t = make_type_name(mcx, names, NodeList::nil(), view.l(1))?;
                *yyval = YYSTYPE::Node(Some(make_string_const_cast(
                    mcx,
                    s,
                    view.l(2),
                    t,
                )?));
            }
            2446 => {
                let t = view.v(1).node().expect("ConstTypename");
                let s = view.v(2).str_val();
                *yyval = YYSTYPE::Node(Some(make_string_const_cast(
                    mcx,
                    s,
                    view.l(2),
                    t,
                )?));
            }
            2442 | 2443 => {
                let s = view.v(1).str_val();
                *yyval = self.a_const(ValUnion::BitString(BitString { bsval: s }), view.l(1))?;
            }
            2450 => *yyval = self.a_const(ValUnion::Boolean(Boolean { boolval: false }), view.l(1))?,
            2455 => *yyval = YYSTYPE::Ival(view.v(2).ival()),
            2456 => *yyval = YYSTYPE::Ival(-view.v(2).ival()),
            2470..=2486 => *yyval = YYSTYPE::Str(view.v(1).str_val()),
            // opt_boolean_or_string keyword arms.
            232 => *yyval = YYSTYPE::Str("true"),
            233 => *yyval = YYSTYPE::Str("false"),
            234 => *yyval = YYSTYPE::Str("on"),
            // CopyStmt: COPY opt_binary qualified_name opt_column_list
            //   copy_from opt_program copy_file_name copy_delimiter opt_with
            //   copy_options where_clause
            409 => {
                let mut n = Node::build::<CopyStmt>(mcx)?;
                n.relation = view.v(3).node();
                n.attlist = view.v(4).list();
                n.is_from = view.v(5).boolean();
                n.is_program = view.v(6).boolean();
                n.filename = opt_str(view.v(7));
                n.whereClause = view.v(11).node();
                if n.is_program && n.filename.is_none() {
                    return Err(self.errposition_error(
                        "STDIN/STDOUT not allowed with PROGRAM".into(),
                        view.l(8),
                    ));
                }
                if !n.is_from && n.whereClause.is_some() {
                    return Err(self.errposition_error(
                        "WHERE clause not allowed with COPY TO".into(),
                        view.l(11),
                    ));
                }
                let mut options = NodeList::nil();
                if let Some(d) = view.v(2).node() {
                    options.lappend(mcx, d)?;
                }
                if let Some(d) = view.v(8).node() {
                    options.lappend(mcx, d)?;
                }
                options.concat(mcx, &view.v(10).list())?;
                n.options = options;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CopyStmt: COPY '(' PreparableStmt ')' TO opt_program
            //   copy_file_name opt_with copy_options
            410 => {
                let mut n = Node::build::<CopyStmt>(mcx)?;
                n.query = view.v(3).node();
                n.is_program = view.v(6).boolean();
                n.filename = opt_str(view.v(7));
                n.options = view.v(9).list();
                if n.is_program && n.filename.is_none() {
                    return Err(self.errposition_error(
                        "STDIN/STDOUT not allowed with PROGRAM".into(),
                        view.l(5),
                    ));
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // copy_from / opt_program.
            411 | 413 => *yyval = YYSTYPE::Boolean(true),
            412 | 414 => *yyval = YYSTYPE::Boolean(false),
            // copy_opt_list: copy_opt_list copy_opt_item.
            420 => {
                let mut list = view.v(1).list();
                if let Some(d) = view.v(2).node() {
                    list.lappend(mcx, d)?;
                }
                *yyval = YYSTYPE::List(list);
            }
            // copy_opt_item arms (legacy WITH syntax) + opt_binary (437).
            422 | 437 => {
                let arg = Node::mk_string(mcx, "binary")?;
                *yyval = def_elem(mcx, "format", Some(arg), view.l(1))?;
            }
            423 => {
                let arg = Node::mk(mcx, Boolean { boolval: true })?;
                *yyval = def_elem(mcx, "freeze", Some(arg), view.l(1))?;
            }
            424 => {
                let arg = Node::mk_string(mcx, view.v(3).str_val())?;
                *yyval = def_elem(mcx, "delimiter", Some(arg), view.l(1))?;
            }
            425 => {
                let arg = Node::mk_string(mcx, view.v(3).str_val())?;
                *yyval = def_elem(mcx, "null", Some(arg), view.l(1))?;
            }
            426 => {
                let arg = Node::mk_string(mcx, "csv")?;
                *yyval = def_elem(mcx, "format", Some(arg), view.l(1))?;
            }
            427 => {
                let arg = Node::mk(mcx, Boolean { boolval: true })?;
                *yyval = def_elem(mcx, "header", Some(arg), view.l(1))?;
            }
            428 => {
                let arg = Node::mk_string(mcx, view.v(3).str_val())?;
                *yyval = def_elem(mcx, "quote", Some(arg), view.l(1))?;
            }
            429 => {
                let arg = Node::mk_string(mcx, view.v(3).str_val())?;
                *yyval = def_elem(mcx, "escape", Some(arg), view.l(1))?;
            }
            430 | 434 => {
                let name = if rule == 430 { "force_quote" } else { "force_null" };
                let arg = Node::mk_list(mcx, view.v(3).list())?;
                *yyval = def_elem(mcx, name, Some(arg), view.l(1))?;
            }
            431 | 435 => {
                let name = if rule == 431 { "force_quote" } else { "force_null" };
                let arg = Node::mk(mcx, types_nodes::A_Star {})?;
                *yyval = def_elem(mcx, name, Some(arg), view.l(1))?;
            }
            432 => {
                let arg = Node::mk_list(mcx, view.v(4).list())?;
                *yyval = def_elem(mcx, "force_not_null", Some(arg), view.l(1))?;
            }
            433 => {
                let arg = Node::mk(mcx, types_nodes::A_Star {})?;
                *yyval = def_elem(mcx, "force_not_null", Some(arg), view.l(1))?;
            }
            436 => {
                let arg = Node::mk_string(mcx, view.v(2).str_val())?;
                *yyval = def_elem(mcx, "encoding", Some(arg), view.l(1))?;
            }
            // copy_delimiter: opt_using DELIMITERS Sconst.
            439 => {
                let arg = Node::mk_string(mcx, view.v(3).str_val())?;
                *yyval = def_elem(mcx, "delimiter", Some(arg), view.l(2))?;
            }
            // copy_generic_opt_list.
            443 => {
                let d = view.v(1).node().expect("copy_generic_opt_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, d)?);
            }
            444 => {
                let mut list = view.v(1).list();
                let d = view.v(3).node().expect("copy_generic_opt_elem");
                list.lappend(mcx, d)?;
                *yyval = YYSTYPE::List(list);
            }
            // copy_generic_opt_elem: ColLabel copy_generic_opt_arg.
            445 => {
                let name = view.v(1).str_val();
                let arg = view.v(2).node();
                *yyval = def_elem(mcx, name, arg, view.l(1))?;
            }
            // copy_generic_opt_arg arms.
            446 => {
                let s = view.v(1).str_val();
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, s)?));
            }
            447 => *yyval = YYSTYPE::Node(view.v(1).node()),
            448 => *yyval = YYSTYPE::Node(Some(Node::mk(mcx, types_nodes::A_Star {})?)),
            449 => *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, "default")?)),
            450 => {
                let list = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(Node::mk_list(mcx, list)?));
            }
            // copy_generic_opt_arg_list (+ _item) and columnList / columnElem.
            452 | 556 => {
                let n = view.v(1).node().expect("list item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            453 | 557 => {
                let mut list = view.v(1).list();
                let n = view.v(3).node().expect("list item");
                list.lappend(mcx, n)?;
                *yyval = YYSTYPE::List(list);
            }
            454 | 562 => {
                let s = view.v(1).str_val();
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, s)?));
            }
            // VACUUM/ANALYZE productions; rule numbers pinned by
            // vacuum_analyze_rule_numbers_match_tables.
            1556 => {
                let mut options = NodeList::nil();
                for (slot, name) in [(2, "full"), (3, "freeze"), (4, "verbose"), (5, "analyze")] {
                    if view.v(slot).boolean() {
                        let d = def_elem(mcx, name, None, view.l(slot))?.node().unwrap();
                        options.lappend(mcx, d)?;
                    }
                }
                let mut n = Node::build::<VacuumStmt>(mcx)?;
                n.options = options;
                n.rels = view.v(6).list();
                n.is_vacuumcmd = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1557 | 1559 => {
                let mut n = Node::build::<VacuumStmt>(mcx)?;
                n.options = view.v(3).list();
                n.rels = view.v(5).list();
                n.is_vacuumcmd = rule == 1557;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1558 => {
                let mut options = NodeList::nil();
                if view.v(2).boolean() {
                    let d = def_elem(mcx, "verbose", None, view.l(2))?.node().unwrap();
                    options.lappend(mcx, d)?;
                }
                let mut n = Node::build::<VacuumStmt>(mcx)?;
                n.options = options;
                n.rels = view.v(3).list();
                n.is_vacuumcmd = false;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1560 | 1582 => {
                let d = view.v(1).node().expect("list item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, d)?);
            }
            1561 | 1583 => {
                let mut list = view.v(1).list();
                let d = view.v(3).node().expect("list item");
                list.lappend(mcx, d)?;
                *yyval = YYSTYPE::List(list);
            }
            1564 => {
                let name = view.v(1).str_val();
                let arg = view.v(2).node();
                *yyval = def_elem(mcx, name, arg, view.l(1))?;
            }
            1566 => *yyval = YYSTYPE::Str("analyze"),
            1567 => *yyval = YYSTYPE::Str("format"),
            1568 => {
                let s = view.v(1).str_val();
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, s)?));
            }
            1571 | 1573 | 1575 | 1577 => *yyval = YYSTYPE::Boolean(true),
            1572 | 1574 | 1576 | 1578 => *yyval = YYSTYPE::Boolean(false),
            // PREPARE/EXECUTE/DEALLOCATE; CREATE TABLE AS EXECUTE stays loud.
            1600 => {
                let n = Node::mk(
                    mcx,
                    PrepareStmt {
                        name: Some(view.v(2).str_val()),
                        argtypes: view.v(3).list(),
                        query: view.v(5).node(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1608 => {
                let n = Node::mk(
                    mcx,
                    ExecuteStmt { name: Some(view.v(2).str_val()), params: view.v(3).list() },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1613 | 1614 => {
                let i = if rule == 1614 { 3 } else { 2 };
                let n = Node::mk(
                    mcx,
                    DeallocateStmt {
                        name: Some(view.v(i).str_val()),
                        isall: false,
                        location: view.l(i),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1615 | 1616 => {
                let n = Node::mk(mcx, DeallocateStmt { name: None, isall: true, location: -1 })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            2299 => {
                let t = view.v(1).node().expect("Typename");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            2300 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("Typename"))?;
                *yyval = YYSTYPE::List(list);
            }
            // ExplainStmt: EXPLAIN ExplainableStmt (options arms 1587-1589
            // stay unimplemented-rule louds).
            1586 => {
                let mut n = Node::build::<types_nodes::parsenodes::ExplainStmt>(mcx)?;
                n.query = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1581 => {
                let mut n = Node::build::<VacuumRelation>(mcx)?;
                n.relation = view.v(1).node();
                n.oid = 0;
                n.va_cols = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
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

// copy_file_name yields Sconst (Str) or NULL for STDIN/STDOUT (Node(None)).
fn opt_str<'mcx>(v: YYSTYPE<'mcx>) -> Option<&'mcx str> {
    if v.is_null_node() {
        None
    } else {
        Some(v.str_val())
    }
}

// makeDefElem (makefuncs.c).
fn def_elem<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    name: &'mcx str,
    arg: Option<Node<'mcx>>,
    location: i32,
) -> PgResult<YYSTYPE<'mcx>> {
    Ok(YYSTYPE::Node(Some(Node::mk(
        mcx,
        DefElem {
            defnamespace: None,
            defname: Some(name),
            arg,
            defaction: DefElemAction::DEFELEM_UNSPEC,
            location,
        },
    )?)))
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


fn on_commit_action(v: i32) -> OnCommitAction {
    match v {
        0 => OnCommitAction::ONCOMMIT_NOOP,
        1 => OnCommitAction::ONCOMMIT_PRESERVE_ROWS,
        2 => OnCommitAction::ONCOMMIT_DELETE_ROWS,
        3 => OnCommitAction::ONCOMMIT_DROP,
        _ => panic!("invalid OnCommitAction {v}"),
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
