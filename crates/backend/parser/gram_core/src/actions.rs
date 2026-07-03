use types_core::catalog::{
    ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL, ATTRIBUTE_IDENTITY_ALWAYS,
    ATTRIBUTE_IDENTITY_BY_DEFAULT, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
use types_error::PgResult;
use types_nodes::parsenodes::{
    AlterTableCmd, AlterTableStmt, AlterTableType, CTEMaterialize, ClosePortalStmt, CommentStmt, CommonTableExpr, CopyStmt, CreateFunctionStmt, CreateSchemaStmt,
    DeallocateStmt, DeclareCursorStmt, DefElem, DefElemAction, DiscardMode, DiscardStmt, FunctionParameter, FunctionParameterMode, CheckPointStmt, LoadStmt, LockStmt,
    AccessPriv, DropBehavior, DropStmt, ExecuteStmt, FetchStmt, GrantStmt, GrantTargetType,
    GroupingSetKind, ListenStmt, NotifyStmt, ObjectType, PrepareStmt, RenameStmt, RoleSpec,
    RoleSpecType, SetOperation, TransactionStmt, TransactionStmtKind, TruncateStmt, UnlistenStmt,
    VacuumRelation, ReplicaIdentityStmt,
    VacuumStmt, ClusterStmt, ReindexObjectType, ReindexStmt, VariableSetKind, VariableSetStmt, VariableShowStmt, WithClause,
    CURSOR_OPT_ASENSITIVE, CURSOR_OPT_BINARY, CURSOR_OPT_FAST_PLAN, CURSOR_OPT_HOLD,
    CURSOR_OPT_INSENSITIVE, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL, FETCH_ALL,
    REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL, REPLICA_IDENTITY_INDEX,
    REPLICA_IDENTITY_NOTHING,
};
use types_nodes::primnodes::{
    CaseExpr, CaseWhen, CoalesceExpr, CollateClause, GroupingFunc, JoinExpr, MinMaxExpr, MinMaxOp,
    OverridingKind, RowExpr,
    SQLValueFunction, SQLValueFunctionOp,
};

use types_nodes::rawnodes::CreateDomainStmt;
use types_nodes::JoinType;
use types_nodes::rawnodes::A_Expr_Kind::{self, AEXPR_OP};
use types_nodes::rawnodes::{
    ColumnDef, Constraint, ConstrType, CreateSeqStmt, CreateStmt, CreateTableAsStmt, IndexElem,
    IndexStmt, IntoClause, OnCommitAction,
    FKCONSTR_ACTION_CASCADE, FKCONSTR_ACTION_NOACTION, FKCONSTR_ACTION_RESTRICT,
    FKCONSTR_ACTION_SETDEFAULT, FKCONSTR_ACTION_SETNULL, FKCONSTR_MATCH_FULL,
    FKCONSTR_MATCH_SIMPLE,
    PartitionBoundSpec, PartitionElem, PartitionSpec, PartitionStrategy,
    RangeSubselect, RefreshMatViewStmt, TableLikeClause, ViewCheckOption, ViewStmt, WindowDef,
    CREATE_TABLE_LIKE_ALL,
    CREATE_TABLE_LIKE_COMMENTS, CREATE_TABLE_LIKE_COMPRESSION, CREATE_TABLE_LIKE_CONSTRAINTS,
    CREATE_TABLE_LIKE_DEFAULTS, CREATE_TABLE_LIKE_GENERATED, CREATE_TABLE_LIKE_IDENTITY,
    CREATE_TABLE_LIKE_INDEXES, CREATE_TABLE_LIKE_STATISTICS, CREATE_TABLE_LIKE_STORAGE,
    FRAMEOPTION_BETWEEN, FRAMEOPTION_DEFAULTS,
    FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_OFFSET_PRECEDING,
    FRAMEOPTION_END_UNBOUNDED_PRECEDING, FRAMEOPTION_EXCLUDE_CURRENT_ROW,
    FRAMEOPTION_EXCLUDE_GROUP, FRAMEOPTION_EXCLUDE_TIES, FRAMEOPTION_GROUPS,
    FRAMEOPTION_NONDEFAULT, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS, FRAMEOPTION_START_CURRENT_ROW,
    FRAMEOPTION_START_OFFSET_FOLLOWING, FRAMEOPTION_START_OFFSET_PRECEDING,
    FRAMEOPTION_START_UNBOUNDED_FOLLOWING, FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};
use types_nodes::{
    Alias, DeleteStmt, InsertStmt, Node, NodeList, NodeTag, RangeFunction, RangeVar, RawStmt,
    SelectStmt, UpdateStmt,
    ValUnion,
};
use types_nodes::{BitString, Boolean, Float, Integer};
use types_nodes::{
    BoolExpr, BoolExprType, CoercionForm, DistinctClause, FuncCall, LimitOption,
    LockClauseStrength, LockWaitPolicy, NodeMut, NullTest, NullTestType, SortBy, SortByDir,
    SortByNulls, TypeCast, TypeName,
};

use crate::parse::Parser;
use crate::stack::ActionView;
use crate::tables::names::{YYRLINE, YYTNAME};
use crate::tables::YYR1;
use crate::yystype::{KeyAction, KeyActions, SelectLimit, YYSTYPE};

// Explicitly-precedenced operators, MathOp declaration order.
const CAS_NOT_DEFERRABLE: i32 = 0x01;
const CAS_DEFERRABLE: i32 = 0x02;
const CAS_INITIALLY_IMMEDIATE: i32 = 0x04;
const CAS_INITIALLY_DEFERRED: i32 = 0x08;
const CAS_NOT_VALID: i32 = 0x10;
const CAS_NO_INHERIT: i32 = 0x20;
const CAS_NOT_ENFORCED: i32 = 0x40;
const CAS_ENFORCED: i32 = 0x80;

// Which pointers C's processCASbits caller passes (NULL target + bit = error).
struct CasTargets {
    deferrable: bool,
    initdeferred: bool,
    is_enforced: bool,
    not_valid: bool,
    no_inherit: bool,
}

struct CasBits {
    deferrable: bool,
    initdeferred: bool,
    is_enforced: bool,
    not_valid: bool,
    no_inherit: bool,
}

static MATH_OPS: [&str; 12] =
    ["+", "-", "*", "/", "%", "^", "<", ">", "=", "<=", ">=", "<>"];

// INTERVAL_MASK(MONTH/YEAR/DAY/HOUR/MINUTE/SECOND) and INTERVAL_FULL_RANGE,
// values verified against datetime.h / timestamp.h.
const IM_MONTH: i32 = 1 << 1;
const IM_YEAR: i32 = 1 << 2;
const IM_DAY: i32 = 1 << 3;
const IM_HOUR: i32 = 1 << 10;
const IM_MINUTE: i32 = 1 << 11;
const IM_SECOND: i32 = 1 << 12;
const INTERVAL_FULL_RANGE: i32 = 0x7FFF;

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
            // parse_toplevel: MODE_TYPE_NAME Typename
            3 => {
                self.parsetree =
                    NodeList::make1(mcx, view.v(2).node().expect("Typename node"))?;
            }
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
            // opt_sort_clause [for_locking_clause select_limit] (both orders),
            // plus the with_clause-prefixed variants (cold).
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
            1852 => {
                let a = Node::mk_mut(
                    mcx,
                    Alias { aliasname: Some(view.v(1).str_val()), colnames: view.v(3).list() },
                )?;
                *yyval = YYSTYPE::Alias(Some(a.seal_ref()));
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
            // PartitionBoundSpec: FOR VALUES WITH (hash_partbound) — HASH lane.
            393 => panic!("gram_core: HASH partition bounds unported (rule 393)"),
            // PartitionBoundSpec: FOR VALUES IN_P '(' expr_list ')'
            394 => {
                let mut n = Node::build::<PartitionBoundSpec>(mcx)?;
                n.strategy = PartitionStrategy::List as u8;
                n.listdatums = view.v(5).list();
                n.location = view.l(3);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PartitionBoundSpec: FOR VALUES FROM '(' expr_list ')' TO '(' expr_list ')'
            395 => {
                let mut n = Node::build::<PartitionBoundSpec>(mcx)?;
                n.strategy = PartitionStrategy::Range as u8;
                n.lowerdatums = view.v(5).list();
                n.upperdatums = view.v(9).list();
                n.location = view.l(3);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PartitionBoundSpec: DEFAULT
            396 => {
                let mut n = Node::build::<PartitionBoundSpec>(mcx)?;
                n.is_default = true;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateStmt: CREATE OptTemp TABLE qualified_name PARTITION OF
            // qualified_name OptTypedTableElementList PartitionBoundSpec
            // OptPartitionSpec table_access_method_clause OptWith
            // OnCommitOption OptTableSpace
            459 => {
                let persistence = view.v(2).ival() as u8;
                let relation = view.v(4).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let parent = view.v(7).node().expect("qualified_name");
                let mut n = Node::build::<CreateStmt>(mcx)?;
                n.relation = relation.as_variant::<RangeVar>();
                n.tableElts = view.v(8).list();
                n.inhRelations = NodeList::make1(mcx, parent)?;
                n.partbound = view.v(9).node();
                n.partspec = view.v(10).node();
                n.accessMethod = opt_str(view.v(11));
                n.options = view.v(12).list();
                n.oncommit = on_commit_action(view.v(13).ival());
                n.tablespacename = opt_str(view.v(14));
                n.if_not_exists = false;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateAsStmt: CREATE OptTemp TABLE [IF_P NOT EXISTS]
            // create_as_target AS SelectStmt opt_with_data
            620 | 621 => {
                let ine = rule == 621;
                let (t, q, w) = if ine { (7, 9, 10) } else { (4, 6, 7) };
                let persistence = view.v(2).ival() as u8;
                let into_node = view.v(t).node().expect("create_as_target");
                let with_data = view.v(w).boolean();
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    let rel = into_node
                        .with_mut::<IntoClause, _>(|ic| {
                            ic.skipData = !with_data;
                            ic.rel
                        })
                        .expect("create_as_target is IntoClause")
                        .expect("IntoClause.rel");
                    rel.with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("IntoClause.rel is RangeVar");
                }
                let mut n = Node::build::<CreateTableAsStmt>(mcx)?;
                n.query = view.v(q).node();
                n.into = Some(into_node);
                n.objtype = ObjectType::OBJECT_TABLE;
                n.is_select_into = false;
                n.if_not_exists = ine;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateMatViewStmt (626 | IF_P NOT EXISTS 627)
            626 | 627 => {
                let ine = rule == 627;
                let (t, q, w) = if ine { (8, 10, 11) } else { (5, 7, 8) };
                let persistence = view.v(2).ival() as u8;
                let into_node = view.v(t).node().expect("create_mv_target");
                let with_data = view.v(w).boolean();
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    let rel = into_node
                        .with_mut::<IntoClause, _>(|ic| {
                            ic.skipData = !with_data;
                            ic.rel
                        })
                        .expect("create_mv_target is IntoClause")
                        .expect("IntoClause.rel");
                    rel.with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("IntoClause.rel is RangeVar");
                }
                let mut n = Node::build::<CreateTableAsStmt>(mcx)?;
                n.query = view.v(q).node();
                n.into = Some(into_node);
                n.objtype = ObjectType::OBJECT_MATVIEW;
                n.is_select_into = false;
                n.if_not_exists = ine;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // create_mv_target
            628 => {
                let mut n = Node::build::<IntoClause>(mcx)?;
                n.rel = view.v(1).node();
                n.colNames = view.v(2).list();
                n.accessMethod = opt_str(view.v(3));
                n.options = view.v(4).list();
                n.onCommit = on_commit_action(0);
                n.tableSpaceName = opt_str(view.v(5));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OptNoLog: UNLOGGED | /*EMPTY*/
            629 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_UNLOGGED as i32),
            630 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_PERMANENT as i32),
            // RefreshMatViewStmt
            631 => {
                let mut n = Node::build::<RefreshMatViewStmt>(mcx)?;
                n.concurrent = view.v(4).boolean();
                n.relation = view.v(5).node().expect("qualified_name").as_range_var();
                n.skipData = !view.v(6).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // create_as_target: qualified_name opt_column_list
            // table_access_method_clause OptWith OnCommitOption OptTableSpace
            622 => {
                let mut n = Node::build::<IntoClause>(mcx)?;
                n.rel = view.v(1).node();
                n.colNames = view.v(2).list();
                n.accessMethod = opt_str(view.v(3));
                n.options = view.v(4).list();
                n.onCommit = on_commit_action(view.v(5).ival());
                n.tableSpaceName = opt_str(view.v(6));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_with_data: WITH DATA_P | WITH NO DATA_P | /*EMPTY*/
            623 | 625 => *yyval = YYSTYPE::Boolean(true),
            624 => *yyval = YYSTYPE::Boolean(false),
            // into_clause: INTO OptTempTableName
            1743 => {
                let mut n = Node::build::<IntoClause>(mcx)?;
                n.rel = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OptTempTableName (GLOBAL-deprecated arms stay unported).
            1745 | 1746 | 1747 | 1748 | 1751 | 1752 | 1753 => {
                let (slot, persistence) = match rule {
                    1745 | 1746 => (3, RELPERSISTENCE_TEMP),
                    1747 | 1748 => (4, RELPERSISTENCE_TEMP),
                    1751 => (3, RELPERSISTENCE_UNLOGGED),
                    1752 => (2, RELPERSISTENCE_PERMANENT),
                    _ => (1, RELPERSISTENCE_PERMANENT),
                };
                let rel = view.v(slot).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    rel.with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                *yyval = YYSTYPE::Node(Some(rel));
            }
            // PartitionSpec: PARTITION BY ColId '(' part_params ')'
            591 => {
                let strategy = view.v(3).str_val();
                let mut n = Node::build::<PartitionSpec>(mcx)?;
                n.strategy = if strategy.eq_ignore_ascii_case("list") {
                    PartitionStrategy::List
                } else if strategy.eq_ignore_ascii_case("range") {
                    PartitionStrategy::Range
                } else if strategy.eq_ignore_ascii_case("hash") {
                    PartitionStrategy::Hash
                } else {
                    return Err(Box::new(
                        (*self.errposition_error(
                            format!("unrecognized partitioning strategy \"{strategy}\""),
                            view.l(3),
                        ))
                        .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
                    ));
                };
                n.partParams = view.v(5).list();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // part_params: part_elem | part_params ',' part_elem
            592 => {
                let el = view.v(1).node().expect("part_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            593 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("part_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
            // part_elem: ColId opt_collate opt_qualified_name
            594 => {
                let mut n = Node::build::<PartitionElem>(mcx)?;
                n.name = Some(view.v(1).str_val());
                n.collation = view.v(2).list();
                n.opclass = view.v(3).list();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // part_elem: func_expr_windowless opt_collate opt_qualified_name
            595 => {
                let mut n = Node::build::<PartitionElem>(mcx)?;
                n.expr = view.v(1).node();
                n.collation = view.v(2).list();
                n.opclass = view.v(3).list();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // part_elem: '(' a_expr ')' opt_collate opt_qualified_name
            596 => {
                let mut n = Node::build::<PartitionElem>(mcx)?;
                n.expr = view.v(2).node();
                n.collation = view.v(4).list();
                n.opclass = view.v(5).list();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OptTemp (GLOBAL-deprecated and UNLOGGED variants stay unported).
            461..=464 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_TEMP as i32),
            468 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_PERMANENT as i32),
            // ViewStmt (RECURSIVE variants 1490/1491 stay unported).
            1488 | 1489 => {
                let replace = rule == 1489;
                let off = if replace { 2 } else { 0 };
                let persistence = view.v(2 + off).ival() as u8;
                let relation = view.v(4 + off).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let mut n = Node::build::<ViewStmt>(mcx)?;
                n.view = relation.as_variant::<RangeVar>();
                n.aliases = view.v(5 + off).list();
                n.query = view.v(8 + off).node();
                n.replace = replace;
                n.options = view.v(6 + off).list();
                n.withCheckOption = view_check_option(view.v(9 + off).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1492 | 1493 => *yyval = YYSTYPE::Ival(ViewCheckOption::CASCADED_CHECK_OPTION as i32),
            1494 => *yyval = YYSTYPE::Ival(ViewCheckOption::LOCAL_CHECK_OPTION as i32),
            1495 => *yyval = YYSTYPE::Ival(ViewCheckOption::NO_CHECK_OPTION as i32),
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
            // ColConstraintElem: NOT NULL_P opt_no_inherit
            499 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_NOTNULL;
                n.location = view.l(1);
                n.is_no_inherit = view.v(3).boolean();
                n.is_enforced = true;
                n.initially_valid = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ColConstraintElem: NULL_P
            500 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_NULL;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateDomainStmt: CREATE DOMAIN_P any_name opt_as Typename ColQualList
            1529 => {
                let mut constraints = NodeList::nil();
                for q in view.v(6).list().iter() {
                    match q.node_tag() {
                        NodeTag::T_Constraint => constraints.lappend(mcx, q)?,
                        NodeTag::T_CollateClause => {
                            panic!("gram_core: SplitColQualList COLLATE arm unported")
                        }
                        other => panic!("unexpected node type {other:?} in ColQualList"),
                    }
                }
                let mut n = Node::build::<CreateDomainStmt>(mcx)?;
                n.domainname = view.v(3).list();
                n.typeName = view.v(5).node();
                n.constraints = constraints;
                *yyval = YYSTYPE::Node(Some(n.seal()));
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
            // ColConstraintElem: UNIQUE opt_unique_null_treatment
            // opt_definition OptConsTableSpace
            501 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_UNIQUE;
                n.location = view.l(1);
                n.nulls_not_distinct = !view.v(2).boolean();
                n.options = view.v(3).list();
                n.indexspace = opt_str(view.v(4));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ColConstraintElem: PRIMARY KEY opt_definition OptConsTableSpace
            502 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_PRIMARY;
                n.location = view.l(1);
                n.options = view.v(3).list();
                n.indexspace = opt_str(view.v(4));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ColConstraintElem: REFERENCES qualified_name opt_column_list
            // key_match key_actions
            507 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_FOREIGN;
                n.location = view.l(1);
                n.pktable = view.v(2).node().and_then(|n| n.as_range_var());
                n.fk_attrs = NodeList::nil();
                n.pk_attrs = view.v(3).list();
                n.fk_matchtype = view.v(4).ival() as u8;
                let ka = view.v(5).key_actions();
                n.fk_upd_action = ka.update_action.action;
                n.fk_del_action = ka.delete_action.action;
                n.fk_del_set_cols = core::mem::take(&mut ka.delete_action.cols);
                n.is_enforced = true;
                n.skip_validation = false;
                n.initially_valid = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // TableConstraint: CONSTRAINT name ConstraintElem
            536 => {
                let name = view.v(2).str_val();
                let node = view.v(3).node().expect("ConstraintElem");
                let loc = view.l(1);
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    node.with_mut::<Constraint, _>(|c| {
                        c.conname = Some(name);
                        c.location = loc;
                    })
                    .expect("ConstraintElem is Constraint");
                }
                *yyval = YYSTYPE::Node(Some(node));
            }
            // ConstraintElem: CHECK '(' a_expr ')' ConstraintAttributeSpec
            538 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_CHECK;
                n.location = view.l(1);
                n.raw_expr = view.v(3).node();
                n.cooked_expr = Option::None;
                let cas = self.process_cas_bits(
                    view.v(5).ival(),
                    view.l(5),
                    "CHECK",
                    CasTargets { deferrable: false, initdeferred: false, is_enforced: true, not_valid: true, no_inherit: true },
                )?;
                n.is_enforced = cas.is_enforced;
                n.skip_validation = cas.not_valid;
                n.is_no_inherit = cas.no_inherit;
                n.initially_valid = !n.skip_validation;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ConstraintElem: UNIQUE opt_unique_null_treatment '(' columnList
            // opt_without_overlaps ')' opt_c_include opt_definition
            // OptConsTableSpace ConstraintAttributeSpec
            540 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_UNIQUE;
                n.location = view.l(1);
                n.nulls_not_distinct = !view.v(2).boolean();
                n.keys = view.v(4).list();
                if view.v(5).boolean() {
                    panic!("gram_core: WITHOUT OVERLAPS (temporal constraint) unported");
                }
                n.including = view.v(7).list();
                n.options = view.v(8).list();
                n.indexspace = opt_str(view.v(9));
                let cas = self.process_cas_bits(
                    view.v(10).ival(),
                    view.l(10),
                    "UNIQUE",
                    CasTargets { deferrable: true, initdeferred: true, is_enforced: false, not_valid: false, no_inherit: false },
                )?;
                n.deferrable = cas.deferrable;
                n.initdeferred = cas.initdeferred;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ConstraintElem: PRIMARY KEY '(' columnList opt_without_overlaps
            // ')' opt_c_include opt_definition OptConsTableSpace
            // ConstraintAttributeSpec
            542 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_PRIMARY;
                n.location = view.l(1);
                n.keys = view.v(4).list();
                if view.v(5).boolean() {
                    panic!("gram_core: WITHOUT OVERLAPS (temporal constraint) unported");
                }
                n.including = view.v(7).list();
                n.options = view.v(8).list();
                n.indexspace = opt_str(view.v(9));
                let cas = self.process_cas_bits(
                    view.v(10).ival(),
                    view.l(10),
                    "PRIMARY KEY",
                    CasTargets { deferrable: true, initdeferred: true, is_enforced: false, not_valid: false, no_inherit: false },
                )?;
                n.deferrable = cas.deferrable;
                n.initdeferred = cas.initdeferred;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ConstraintElem: FOREIGN KEY '(' columnList optionalPeriodName ')'
            // REFERENCES qualified_name opt_column_and_period_list key_match
            // key_actions ConstraintAttributeSpec
            545 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_FOREIGN;
                n.location = view.l(1);
                n.pktable = view.v(8).node().and_then(|n| n.as_range_var());
                n.fk_attrs = view.v(4).list();
                if view.v(5).node().is_some() {
                    panic!("gram_core: PERIOD FK columns (temporal FK) unported");
                }
                // opt_column_and_period_list flattened: rule 560 panics on a
                // PERIOD name, so the pair collapses to its column list.
                n.pk_attrs = view.v(9).list();
                n.fk_matchtype = view.v(10).ival() as u8;
                let ka = view.v(11).key_actions();
                n.fk_upd_action = ka.update_action.action;
                n.fk_del_action = ka.delete_action.action;
                n.fk_del_set_cols = core::mem::take(&mut ka.delete_action.cols);
                let cas = self.process_cas_bits(
                    view.v(12).ival(),
                    view.l(12),
                    "FOREIGN KEY",
                    CasTargets { deferrable: true, initdeferred: true, is_enforced: true, not_valid: true, no_inherit: false },
                )?;
                n.deferrable = cas.deferrable;
                n.initdeferred = cas.initdeferred;
                n.is_enforced = cas.is_enforced;
                n.skip_validation = cas.not_valid;
                n.initially_valid = !n.skip_validation;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // columnList: columnElem | columnList ',' columnElem
            556 => {
                let el = view.v(1).node().expect("columnElem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            557 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("columnElem"))?;
                *yyval = YYSTYPE::List(list);
            }
            // opt_column_and_period_list: '(' columnList optionalPeriodName ')'
            // C: list_make2($2, $3); flattened to the column list (PERIOD loud).
            560 => {
                if view.v(3).node().is_some() {
                    panic!("gram_core: PERIOD referenced columns (temporal FK) unported");
                }
                *yyval = YYSTYPE::List(view.v(2).list());
            }
            561 => *yyval = YYSTYPE::List(NodeList::nil()),
            // columnElem: ColId
            562 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::String { sval: view.v(1).str_val() },
                )?));
            }
            // key_match: MATCH FULL | MATCH PARTIAL | MATCH SIMPLE | /*EMPTY*/
            565 => *yyval = YYSTYPE::Ival(FKCONSTR_MATCH_FULL as i32),
            566 => {
                return Err(Box::new(
                    (*self.errposition_error(
                        "MATCH PARTIAL not yet implemented".into(),
                        view.l(1),
                    ))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            567 | 568 => *yyval = YYSTYPE::Ival(FKCONSTR_MATCH_SIMPLE as i32),
            // key_actions: key_update | key_delete | both orders | /*EMPTY*/
            575 | 576 | 577 | 578 | 579 => {
                let noaction = || KeyAction { action: FKCONSTR_ACTION_NOACTION, cols: NodeList::nil() };
                let (upd, del) = match rule {
                    575 => (core::mem::take(view.v(1).key_action()), noaction()),
                    576 => (noaction(), core::mem::take(view.v(1).key_action())),
                    577 => (core::mem::take(view.v(1).key_action()), core::mem::take(view.v(2).key_action())),
                    578 => (core::mem::take(view.v(2).key_action()), core::mem::take(view.v(1).key_action())),
                    _ => (noaction(), noaction()),
                };
                *yyval = YYSTYPE::KeyActionsV(mcx::leak_in(mcx::alloc_in(
                    mcx,
                    KeyActions { update_action: upd, delete_action: del },
                )?));
            }
            // key_update: ON UPDATE key_action
            580 => {
                let ka = view.v(3).key_action();
                if !ka.cols.is_nil() {
                    let which = if ka.action == FKCONSTR_ACTION_SETNULL { "SET NULL" } else { "SET DEFAULT" };
                    return Err(Box::new(
                        (*self.errposition_error(
                            format!("a column list with {which} is only supported for ON DELETE actions"),
                            view.l(1),
                        ))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                *yyval = YYSTYPE::KeyActionV(ka);
            }
            // key_action: NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
            582 | 583 | 584 | 585 | 586 => {
                let (action, cols) = match rule {
                    582 => (FKCONSTR_ACTION_NOACTION, NodeList::nil()),
                    583 => (FKCONSTR_ACTION_RESTRICT, NodeList::nil()),
                    584 => (FKCONSTR_ACTION_CASCADE, NodeList::nil()),
                    585 => (FKCONSTR_ACTION_SETNULL, view.v(3).list()),
                    _ => (FKCONSTR_ACTION_SETDEFAULT, view.v(3).list()),
                };
                *yyval = YYSTYPE::KeyActionV(mcx::leak_in(mcx::alloc_in(
                    mcx,
                    KeyAction { action, cols },
                )?));
            }
            // ConstraintAttributeSpec: /*EMPTY*/ | spec ConstraintAttributeElem
            825 => *yyval = YYSTYPE::Ival(0),
            826 => {
                let newspec = view.v(1).ival() | view.v(2).ival();
                if (newspec & (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED))
                    == (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED)
                {
                    return Err(self.errposition_error(
                        "constraint declared INITIALLY DEFERRED must be DEFERRABLE".into(),
                        view.l(2),
                    ));
                }
                if (newspec & (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)) == (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)
                    || (newspec & (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED))
                        == (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED)
                    || (newspec & (CAS_NOT_ENFORCED | CAS_ENFORCED)) == (CAS_NOT_ENFORCED | CAS_ENFORCED)
                {
                    return Err(self.errposition_error(
                        "conflicting constraint properties".into(),
                        view.l(2),
                    ));
                }
                *yyval = YYSTYPE::Ival(newspec);
            }
            // ConstraintAttributeElem
            827 => *yyval = YYSTYPE::Ival(CAS_NOT_DEFERRABLE),
            828 => *yyval = YYSTYPE::Ival(CAS_DEFERRABLE),
            829 => *yyval = YYSTYPE::Ival(CAS_INITIALLY_IMMEDIATE),
            830 => *yyval = YYSTYPE::Ival(CAS_INITIALLY_DEFERRED),
            831 => *yyval = YYSTYPE::Ival(CAS_NOT_VALID),
            832 => *yyval = YYSTYPE::Ival(CAS_NO_INHERIT),
            833 => *yyval = YYSTYPE::Ival(CAS_NOT_ENFORCED),
            834 => *yyval = YYSTYPE::Ival(CAS_ENFORCED),
            // opt_without_overlaps: WITHOUT OVERLAPS | /*EMPTY*/
            552 => *yyval = YYSTYPE::Boolean(true),
            553 => *yyval = YYSTYPE::Boolean(false),
            // TableLikeClause: LIKE qualified_name TableLikeOptionList
            522 => {
                let relation = view.v(2).node().expect("qualified_name");
                let mut n = Node::build::<TableLikeClause>(mcx)?;
                n.relation = relation.as_variant::<RangeVar>();
                n.options = view.v(3).ival() as u32;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // TableLikeOptionList: [list INCLUDING opt | list EXCLUDING opt | empty]
            523 => *yyval = YYSTYPE::Ival(view.v(1).ival() | view.v(3).ival()),
            524 => *yyval = YYSTYPE::Ival(view.v(1).ival() & !view.v(3).ival()),
            525 => *yyval = YYSTYPE::Ival(0),
            526 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_COMMENTS as i32),
            527 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_COMPRESSION as i32),
            528 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_CONSTRAINTS as i32),
            529 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_DEFAULTS as i32),
            530 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_IDENTITY as i32),
            531 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_GENERATED as i32),
            532 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_INDEXES as i32),
            533 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_STATISTICS as i32),
            534 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_STORAGE as i32),
            535 => *yyval = YYSTYPE::Ival(CREATE_TABLE_LIKE_ALL as i32),
            // ColConstraintElem: GENERATED generated_when AS IDENTITY_P
            // OptParenthesizedSeqOptList
            505 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_IDENTITY;
                n.generated_when = view.v(2).ival() as u8;
                n.options = view.v(5).list();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ColConstraintElem: GENERATED generated_when AS '(' a_expr ')'
            // opt_virtual_or_stored
            506 => {
                let when = view.v(2).ival() as u8;
                if when != ATTRIBUTE_IDENTITY_ALWAYS {
                    return Err(self.errposition_error(
                        "for a generated column, GENERATED ALWAYS must be specified".into(),
                        view.l(2),
                    ));
                }
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_GENERATED;
                n.generated_when = when;
                n.raw_expr = view.v(5).node();
                n.generated_kind = view.v(7).ival() as u8;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // generated_when: ALWAYS | BY DEFAULT
            511 => *yyval = YYSTYPE::Ival(ATTRIBUTE_IDENTITY_ALWAYS as i32),
            512 => *yyval = YYSTYPE::Ival(ATTRIBUTE_IDENTITY_BY_DEFAULT as i32),
            // opt_virtual_or_stored: STORED | VIRTUAL | /*EMPTY*/
            513 => *yyval = YYSTYPE::Ival(ATTRIBUTE_GENERATED_STORED as i32),
            514 | 515 => *yyval = YYSTYPE::Ival(ATTRIBUTE_GENERATED_VIRTUAL as i32),
            // opt_no_inherit: NO INHERIT | /*EMPTY*/
            550 => *yyval = YYSTYPE::Boolean(true),
            551 => *yyval = YYSTYPE::Boolean(false),
            141 => *yyval = YYSTYPE::Boolean(true),
            142 => *yyval = YYSTYPE::Boolean(false),
            377 => {
                let el = view.v(1).node().expect("reloption_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            378 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("reloption_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
            379 => {
                let v = def_elem(mcx, view.v(1).str_val(), view.v(3).node(), view.l(1))?;
                *yyval = v;
            }
            380 => {
                let v = def_elem(mcx, view.v(1).str_val(), Option::None, view.l(1))?;
                *yyval = v;
            }
            872 => *yyval = YYSTYPE::Node(view.v(1).node()),
            873 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::String { sval: view.v(1).str_val() },
                )?));
            }
            660 => *yyval = YYSTYPE::Node(Some(Node::mk(mcx, Float { fval: view.v(1).str_val() })?)),
            661 => *yyval = YYSTYPE::Node(Some(Node::mk(mcx, Float { fval: view.v(2).str_val() })?)),
            662 => {
                let fval = negate_float(mcx, view.v(2).str_val())?;
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, Float { fval })?));
            }
            663 => {
                *yyval =
                    YYSTYPE::Node(Some(Node::mk(mcx, Integer { ival: view.v(1).ival() })?));
            }
            508 | 510 => *yyval = YYSTYPE::Boolean(true),
            509 => *yyval = YYSTYPE::Boolean(false),
            // IndexStmt: CREATE opt_unique INDEX opt_concurrently
            // [IF NOT EXISTS name | opt_single_name] ON relation_expr
            // access_method_clause '(' index_params ')' opt_include
            // opt_unique_null_treatment opt_reloptions OptTableSpace where_clause
            1101 | 1102 => {
                let b = if rule == 1102 { 3 } else { 0 };
                let mut n = Node::build::<IndexStmt>(mcx)?;
                n.unique = view.v(2).boolean();
                n.concurrent = view.v(4).boolean();
                n.idxname = if rule == 1102 {
                    Some(view.v(8).str_val())
                } else {
                    opt_str(view.v(5))
                };
                let relation = view.v(7 + b).node().expect("relation_expr");
                n.relation = relation.as_variant::<RangeVar>();
                n.accessMethod = Some(view.v(8 + b).str_val());
                n.indexParams = view.v(10 + b).list();
                n.indexIncludingParams = view.v(12 + b).list();
                n.nulls_not_distinct = !view.v(13 + b).boolean();
                n.options = view.v(14 + b).list();
                n.tableSpace = opt_str(view.v(15 + b));
                n.whereClause = view.v(16 + b).node();
                n.if_not_exists = rule == 1102;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1103 => *yyval = YYSTYPE::Boolean(true),
            1104 => *yyval = YYSTYPE::Boolean(false),
            1106 => *yyval = YYSTYPE::Keyword("btree"),
            1107 | 1116 => {
                let el = view.v(1).node().expect("index_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1108 | 1117 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("index_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
            // index_elem_options: opt_collate opt_qualified_name
            // [reloptions] opt_asc_desc opt_nulls_order
            1109 | 1110 => {
                let r = if rule == 1110 { 1 } else { 0 };
                let mut n = Node::build::<IndexElem>(mcx)?;
                n.collation = view.v(1).list();
                n.opclass = view.v(2).list();
                if rule == 1110 {
                    n.opclassopts = view.v(3).list();
                }
                n.ordering = sortby_dir(view.v(3 + r).ival());
                n.nulls_ordering = sortby_nulls(view.v(4 + r).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1111 => {
                let name = view.v(1).str_val();
                let node = view.v(2).node().expect("index_elem_options");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    node.with_mut::<IndexElem, _>(|e| e.name = Some(name))
                        .expect("index_elem_options is IndexElem");
                }
                *yyval = YYSTYPE::Node(Some(node));
            }
            1112 | 1113 => {
                let (expr_i, elem_i) = if rule == 1113 { (2, 4) } else { (1, 2) };
                let expr = view.v(expr_i).node();
                let node = view.v(elem_i).node().expect("index_elem_options");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    node.with_mut::<IndexElem, _>(|e| e.expr = expr)
                        .expect("index_elem_options is IndexElem");
                }
                *yyval = YYSTYPE::Node(Some(node));
            }
            // opt_qualified_name
            139 => *yyval = view.v(1),
            140 => *yyval = YYSTYPE::List(NodeList::nil()),
            // opt_name_list
            1579 => *yyval = view.v(2),
            1580 => *yyval = YYSTYPE::List(NodeList::nil()),
            // CreateStatsStmt: CREATE STATISTICS [IF NOT EXISTS]
            // opt_qualified_name opt_name_list ON stats_params FROM from_list
            611 | 612 => {
                let b = if rule == 612 { 3 } else { 0 };
                let mut n = Node::build::<types_nodes::rawnodes::CreateStatsStmt>(mcx)?;
                n.defnames = view.v(3 + b).list();
                n.stat_types = view.v(4 + b).list();
                n.exprs = view.v(6 + b).list();
                n.relations = view.v(8 + b).list();
                n.if_not_exists = rule == 612;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // stats_params
            613 => {
                let el = view.v(1).node().expect("stats_param");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            614 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("stats_param"))?;
                *yyval = YYSTYPE::List(list);
            }
            // stats_param: ColId | func_expr_windowless | '(' a_expr ')'
            615 => {
                let mut n = Node::build::<types_nodes::rawnodes::StatsElem>(mcx)?;
                n.name = Some(view.v(1).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            616 | 617 => {
                let i = if rule == 617 { 2 } else { 1 };
                let mut n = Node::build::<types_nodes::rawnodes::StatsElem>(mcx)?;
                n.expr = view.v(i).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OnCommitOption
            602 => *yyval = YYSTYPE::Ival(OnCommitAction::ONCOMMIT_DROP as i32),
            603 => *yyval = YYSTYPE::Ival(OnCommitAction::ONCOMMIT_DELETE_ROWS as i32),
            604 => *yyval = YYSTYPE::Ival(OnCommitAction::ONCOMMIT_PRESERVE_ROWS as i32),
            605 => *yyval = YYSTYPE::Ival(OnCommitAction::ONCOMMIT_NOOP as i32),
            // CreateSeqStmt: CREATE OptTemp SEQUENCE [IF_P NOT EXISTS]
            // qualified_name OptSeqOptList
            632 | 633 => {
                let (rv, opts) =
                    if rule == 632 { (view.v(4), view.v(5)) } else { (view.v(7), view.v(8)) };
                let persistence = view.v(2).ival() as u8;
                let relation = rv.node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let mut n = Node::build::<CreateSeqStmt>(mcx)?;
                n.sequence = relation.as_variant::<RangeVar>();
                n.options = opts.list();
                n.if_not_exists = rule == 633;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OptParenthesizedSeqOptList: '(' SeqOptList ')' | /*EMPTY*/
            638 => *yyval = YYSTYPE::List(view.v(2).list()),
            639 => *yyval = YYSTYPE::List(NodeList::nil()),
            // SeqOptList: SeqOptElem | SeqOptList SeqOptElem
            640 => {
                let el = view.v(1).node().expect("SeqOptElem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            641 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("SeqOptElem"))?;
                *yyval = YYSTYPE::List(list);
            }
            // SeqOptElem: AS SimpleTypename | CACHE NumericOnly
            642 => *yyval = def_elem(mcx, "as", view.v(2).node(), view.l(1))?,
            643 => *yyval = def_elem(mcx, "cache", view.v(2).node(), view.l(1))?,
            // CYCLE | NO CYCLE
            644 | 645 => {
                let arg = Node::mk(mcx, Boolean { boolval: rule == 644 })?;
                *yyval = def_elem(mcx, "cycle", Some(arg), view.l(1))?;
            }
            646 => *yyval = def_elem(mcx, "increment", view.v(3).node(), view.l(1))?,
            647 => *yyval = def_elem(mcx, "logged", None, view.l(1))?,
            648 => *yyval = def_elem(mcx, "maxvalue", view.v(2).node(), view.l(1))?,
            649 => *yyval = def_elem(mcx, "minvalue", view.v(2).node(), view.l(1))?,
            650 => *yyval = def_elem(mcx, "maxvalue", None, view.l(1))?,
            651 => *yyval = def_elem(mcx, "minvalue", None, view.l(1))?,
            // OWNED BY any_name | SEQUENCE NAME_P any_name
            652 | 653 => {
                let name = if rule == 652 { "owned_by" } else { "sequence_name" };
                let arg = Node::mk_list(mcx, view.v(3).list())?;
                *yyval = def_elem(mcx, name, Some(arg), view.l(1))?;
            }
            654 => *yyval = def_elem(mcx, "start", view.v(3).node(), view.l(1))?,
            655 => *yyval = def_elem(mcx, "restart", None, view.l(1))?,
            656 => *yyval = def_elem(mcx, "restart", view.v(3).node(), view.l(1))?,
            657 => *yyval = def_elem(mcx, "unlogged", None, view.l(1))?,
            1710 => {
                let stmt = view.v(1).node().expect("select_clause");
                let sort = view.v(2).list();
                self.insert_select_options(stmt, sort, NodeList::nil(), None, None)?;
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            1711 | 1712 => {
                let stmt = view.v(1).node().expect("select_clause");
                let sort = view.v(2).list();
                let (lock_i, limit_i) = if rule == 1711 { (3, 4) } else { (4, 3) };
                let locking = view.v(lock_i).list();
                let limit = view.v(limit_i).limit();
                self.insert_select_options(stmt, sort, locking, limit, None)?;
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            1713 | 1714 => {
                let stmt = view.v(2).node().expect("select_clause");
                let sort = if rule == 1714 { view.v(3).list() } else { NodeList::nil() };
                self.insert_select_options(stmt, sort, NodeList::nil(), None, view.v(1).node())?;
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            1715 | 1716 => {
                let stmt = view.v(2).node().expect("select_clause");
                let sort = view.v(3).list();
                let (lock_i, limit_i) = if rule == 1715 { (4, 5) } else { (5, 4) };
                let locking = view.v(lock_i).list();
                let limit = view.v(limit_i).limit();
                self.insert_select_options(stmt, sort, locking, limit, view.v(1).node())?;
                *yyval = YYSTYPE::Node(Some(stmt));
            }
            // simple_select: select_clause {UNION|INTERSECT|EXCEPT} set_quantifier select_clause
            1723 | 1724 | 1725 => {
                let mut n = Node::build::<SelectStmt>(mcx)?;
                n.op = match rule {
                    1723 => SetOperation::SETOP_UNION,
                    1724 => SetOperation::SETOP_INTERSECT,
                    _ => SetOperation::SETOP_EXCEPT,
                };
                n.all = view.v(3).ival() == 1;
                n.larg =
                    Some(view.v(1).node().and_then(Node::as_select_stmt).expect("select_clause"));
                n.rarg =
                    Some(view.v(4).node().and_then(Node::as_select_stmt).expect("select_clause"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // with_clause: WITH cte_list | WITH_LA cte_list | WITH RECURSIVE cte_list
            1726 | 1727 | 1728 => {
                let recursive = rule == 1728;
                let mut n = Node::build::<WithClause>(mcx)?;
                n.ctes = view.v(if recursive { 3 } else { 2 }).list();
                n.recursive = recursive;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1729 => {
                let cte = view.v(1).node().expect("common_table_expr");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, cte)?);
            }
            1730 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("common_table_expr"))?;
                *yyval = YYSTYPE::List(list);
            }
            1731 => {
                let mut n = Node::build::<CommonTableExpr>(mcx)?;
                n.ctename = Some(view.v(1).str_val());
                n.aliascolnames = view.v(2).list();
                n.ctematerialized = match view.v(4).ival() {
                    1 => CTEMaterialize::CTEMaterializeAlways,
                    2 => CTEMaterialize::CTEMaterializeNever,
                    _ => CTEMaterialize::CTEMaterializeDefault,
                };
                n.ctequery = view.v(6).node();
                // SEARCH/CYCLE productions are unported louds; only the
                // NULL-yielding empty variants can reach here.
                debug_assert!(view.v(8).node().is_none() && view.v(9).node().is_none());
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1732 => *yyval = YYSTYPE::Ival(CTEMaterialize::CTEMaterializeAlways as i32),
            1733 => *yyval = YYSTYPE::Ival(CTEMaterialize::CTEMaterializeNever as i32),
            1734 => *yyval = YYSTYPE::Ival(CTEMaterialize::CTEMaterializeDefault as i32),
            // opt_asc_desc / opt_nulls_order constants (shared with index_elem).
            1120 => *yyval = YYSTYPE::Ival(SortByDir::SORTBY_ASC as i32),
            1121 => *yyval = YYSTYPE::Ival(SortByDir::SORTBY_DESC as i32),
            1122 => *yyval = YYSTYPE::Ival(SortByDir::SORTBY_DEFAULT as i32),
            1123 => *yyval = YYSTYPE::Ival(SortByNulls::SORTBY_NULLS_FIRST as i32),
            1124 => *yyval = YYSTYPE::Ival(SortByNulls::SORTBY_NULLS_LAST as i32),
            1125 => *yyval = YYSTYPE::Ival(SortByNulls::SORTBY_NULLS_DEFAULT as i32),
            // opt_nowait_or_skip: NOWAIT | SKIP LOCKED | EMPTY.
            1661 => *yyval = YYSTYPE::Ival(LockWaitPolicy::LockWaitError as i32),
            1662 => *yyval = YYSTYPE::Ival(LockWaitPolicy::LockWaitSkip as i32),
            1663 => *yyval = YYSTYPE::Ival(LockWaitPolicy::LockWaitBlock as i32),
            // for_locking_items: item | items item.
            1817 => {
                let item = view.v(1).node().expect("for_locking_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, item)?);
            }
            1818 => {
                let mut list = view.v(1).list();
                let item = view.v(2).node().expect("for_locking_item");
                list.lappend(mcx, item)?;
                *yyval = YYSTYPE::List(list);
            }
            // for_locking_item: for_locking_strength locked_rels_list opt_nowait_or_skip
            1819 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::LockingClause {
                        lockedRels: view.v(2).list(),
                        strength: lock_strength(view.v(1).ival()),
                        waitPolicy: lock_wait_policy(view.v(3).ival()),
                    },
                )?));
            }
            1820 => *yyval = YYSTYPE::Ival(LockClauseStrength::LCS_FORUPDATE as i32),
            1821 => *yyval = YYSTYPE::Ival(LockClauseStrength::LCS_FORNOKEYUPDATE as i32),
            1822 => *yyval = YYSTYPE::Ival(LockClauseStrength::LCS_FORSHARE as i32),
            1823 => *yyval = YYSTYPE::Ival(LockClauseStrength::LCS_FORKEYSHARE as i32),
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
            // group_by_list; group_by_item's five arms are DISPATCH
            // passthroughs.
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
            // empty_grouping_set / rollup_clause / cube_clause /
            // grouping_sets_clause -> GroupingSet.
            1807 => {
                *yyval = YYSTYPE::Node(Some(Node::mk_grouping_set(
                    mcx,
                    GroupingSetKind::GROUPING_SET_EMPTY,
                    NodeList::nil(),
                    view.l(1),
                )?));
            }
            1808 => {
                *yyval = YYSTYPE::Node(Some(Node::mk_grouping_set(
                    mcx,
                    GroupingSetKind::GROUPING_SET_ROLLUP,
                    view.v(3).list(),
                    view.l(1),
                )?));
            }
            1809 => {
                *yyval = YYSTYPE::Node(Some(Node::mk_grouping_set(
                    mcx,
                    GroupingSetKind::GROUPING_SET_CUBE,
                    view.v(3).list(),
                    view.l(1),
                )?));
            }
            1810 => {
                *yyval = YYSTYPE::Node(Some(Node::mk_grouping_set(
                    mcx,
                    GroupingSetKind::GROUPING_SET_SETS,
                    view.v(4).list(),
                    view.l(1),
                )?));
            }
            // JSON_OBJECT '(' func_arg_list ')' -- legacy json_object().
            2186 => {
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "json_object")?,
                    view.v(3).list(),
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // GROUPING '(' expr_list ')' -> GroupingFunc.
            2125 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    GroupingFunc {
                        args: view.v(3).list(),
                        location: view.l(1),
                        ..Default::default()
                    },
                )?));
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
            // opt_on_conflict: ON CONFLICT opt_conf_expr DO UPDATE SET
            //                  set_clause_list where_clause
            1630 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::OnConflictClause {
                        action: types_nodes::OnConflictAction::ONCONFLICT_UPDATE,
                        infer: view.v(3).node(),
                        targetList: view.v(7).list(),
                        whereClause: view.v(8).node(),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // opt_on_conflict: ON CONFLICT opt_conf_expr DO NOTHING
            1631 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::OnConflictClause {
                        action: types_nodes::OnConflictAction::ONCONFLICT_NOTHING,
                        infer: view.v(3).node(),
                        targetList: NodeList::nil(),
                        whereClause: None,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // opt_conf_expr: '(' index_params ')' where_clause
            1633 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::InferClause {
                        indexElems: view.v(2).list(),
                        whereClause: view.v(4).node(),
                        conname: None,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // opt_conf_expr: ON CONSTRAINT name
            1634 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::InferClause {
                        indexElems: NodeList::nil(),
                        whereClause: None,
                        conname: Some(view.v(3).str_val()),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // returning_clause: RETURNING returning_with_clause target_list;
            // WITH(...) options stay loud in the returning_option arms.
            1636 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::ReturningClause {
                        options: view.v(2).list(),
                        exprs: view.v(3).list(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
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
            //            | OVERRIDING override_kind VALUE_P SelectStmt
            //            | '(' insert_column_list ')' OVERRIDING override_kind
            //              VALUE_P SelectStmt | DEFAULT VALUES
            1620 | 1621 | 1622 | 1623 | 1624 => {
                let mut n = Node::build::<InsertStmt>(mcx)?;
                if rule == 1622 || rule == 1623 {
                    n.cols = view.v(2).list();
                }
                match rule {
                    1621 => n.r#override = override_kind(view.v(2).ival()),
                    1623 => n.r#override = override_kind(view.v(5).ival()),
                    _ => {}
                }
                if rule != 1624 {
                    n.selectStmt = view
                        .v(match rule {
                            1620 => 1,
                            1621 | 1622 => 4,
                            _ => 7,
                        })
                        .node();
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // override_kind: USER | SYSTEM_P
            1625 => *yyval = YYSTYPE::Ival(OverridingKind::OVERRIDING_USER_VALUE as i32),
            1626 => *yyval = YYSTYPE::Ival(OverridingKind::OVERRIDING_SYSTEM_VALUE as i32),
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
            // Typename: SETOF SimpleTypename opt_array_bounds
            1937 => {
                let t = view.v(2).node().expect("SimpleTypename");
                let bounds = view.v(3).list();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| {
                        tn.arrayBounds = bounds;
                        tn.setof = true;
                    })
                    .expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            // opt_array_bounds: bounds accumulate as Integer(-1)/Integer(n).
            1942 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, Node::mk_integer(mcx, -1)?)?;
                *yyval = YYSTYPE::List(list);
            }
            1943 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, Node::mk_integer(mcx, view.v(3).ival())?)?;
                *yyval = YYSTYPE::List(list);
            }
            // ConstInterval opt_interval
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
            // a_expr IS [NOT] TRUE_P / FALSE_P / UNKNOWN
            2062..=2067 => {
                use types_nodes::primnodes::{BoolTestType, BooleanTest};
                let t = match rule {
                    2062 => BoolTestType::IS_TRUE,
                    2063 => BoolTestType::IS_NOT_TRUE,
                    2064 => BoolTestType::IS_FALSE,
                    2065 => BoolTestType::IS_NOT_FALSE,
                    2066 => BoolTestType::IS_UNKNOWN,
                    _ => BoolTestType::IS_NOT_UNKNOWN,
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    BooleanTest { arg: view.v(1).node(), booltesttype: t, location: view.l(2) },
                )?));
            }
            // a_expr IS [NOT] DISTINCT FROM a_expr
            2068 | 2069 => {
                use types_nodes::rawnodes::A_Expr_Kind;
                let (kind, r_i) = if rule == 2068 {
                    (A_Expr_Kind::AEXPR_DISTINCT, 5)
                } else {
                    (A_Expr_Kind::AEXPR_NOT_DISTINCT, 6)
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Expr {
                        kind,
                        name: NodeList::make1(mcx, Node::mk_string(mcx, "=")?)?,
                        lexpr: view.v(1).node(),
                        rexpr: view.v(r_i).node(),
                        rexpr_list_start: 0,
                        rexpr_list_end: 0,
                        location: view.l(2),
                    },
                )?));
            }
            // a_expr COLLATE any_name
            2022 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::CollateClause {
                        arg: view.v(1).node(),
                        collname: view.v(3).list(),
                        location: view.l(2),
                    },
                )?));
            }
            // a_expr subquery_Op sub_type '(' a_expr ')'
            2079 => {
                use types_nodes::rawnodes::A_Expr_Kind;
                let kind = if view.v(3).ival() == types_nodes::SubLinkType::ALL_SUBLINK as i32 {
                    A_Expr_Kind::AEXPR_OP_ALL
                } else {
                    A_Expr_Kind::AEXPR_OP_ANY
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Expr {
                        kind,
                        name: view.v(2).list(),
                        lexpr: view.v(1).node(),
                        rexpr: view.v(5).node(),
                        rexpr_list_start: 0,
                        rexpr_list_end: 0,
                        location: view.l(2),
                    },
                )?));
            }
            // c_expr: explicit_row | implicit_row
            2123 | 2124 => {
                let row_format = if rule == 2123 {
                    CoercionForm::COERCE_EXPLICIT_CALL
                } else {
                    CoercionForm::COERCE_IMPLICIT_CAST
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    RowExpr {
                        args: view.v(1).list(),
                        row_typeid: 0,
                        row_format,
                        colnames: NodeList::nil(),
                        location: view.l(1),
                    },
                )?));
            }
            // implicit_row: '(' expr_list ',' a_expr ')'
            2262 => {
                let mut list = view.v(2).list();
                list.lappend(mcx, view.v(4).node().expect("a_expr"))?;
                *yyval = YYSTYPE::List(list);
            }
            // a_expr subquery_Op sub_type select_with_parens
            2078 => {
                let sub_type = view.v(3).ival();
                let link = if sub_type == types_nodes::SubLinkType::ALL_SUBLINK as i32 {
                    types_nodes::SubLinkType::ALL_SUBLINK
                } else {
                    types_nodes::SubLinkType::ANY_SUBLINK
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::SubLink {
                        subLinkType: link,
                        subLinkId: 0,
                        testexpr: view.v(1).node(),
                        operName: view.v(2).list(),
                        subselect: view.v(4).node().expect("select_with_parens"),
                        location: view.l(2),
                    },
                )?));
            }
            // a_expr [NOT_LA] BETWEEN [SYMMETRIC] b_expr AND a_expr
            2070..=2073 => {
                use types_nodes::rawnodes::A_Expr_Kind;
                let (kind, name, b_i) = match rule {
                    2070 => (A_Expr_Kind::AEXPR_BETWEEN, "BETWEEN", 4),
                    2071 => (A_Expr_Kind::AEXPR_NOT_BETWEEN, "NOT BETWEEN", 5),
                    2072 => (A_Expr_Kind::AEXPR_BETWEEN_SYM, "BETWEEN SYMMETRIC", 4),
                    _ => (A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM, "NOT BETWEEN SYMMETRIC", 5),
                };
                let lexpr = view.v(1).node();
                let lo = view.v(b_i).node().expect("b_expr");
                let hi = view.v(b_i + 2).node().expect("a_expr");
                let rexpr = Node::mk_list(mcx, NodeList::make2(mcx, lo, hi)?)?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Expr {
                        kind,
                        name: NodeList::make1(mcx, Node::mk_string(mcx, name)?)?,
                        lexpr,
                        rexpr: Some(rexpr),
                        rexpr_list_start: 0,
                        rexpr_list_end: 0,
                        location: view.l(2),
                    },
                )?));
            }
            // a_expr [NOT] LIKE/ILIKE a_expr [ESCAPE a_expr] and
            // a_expr [NOT] SIMILAR TO a_expr [ESCAPE a_expr]
            2045..=2056 => {
                use types_nodes::rawnodes::A_Expr_Kind;
                let (kind, op) = match rule {
                    2045 | 2046 => (A_Expr_Kind::AEXPR_LIKE, "~~"),
                    2047 | 2048 => (A_Expr_Kind::AEXPR_LIKE, "!~~"),
                    2049 | 2050 => (A_Expr_Kind::AEXPR_ILIKE, "~~*"),
                    2051 | 2052 => (A_Expr_Kind::AEXPR_ILIKE, "!~~*"),
                    2053 | 2054 => (A_Expr_Kind::AEXPR_SIMILAR, "~"),
                    _ => (A_Expr_Kind::AEXPR_SIMILAR, "!~"),
                };
                let similar = rule >= 2053;
                let not = matches!(rule, 2047 | 2048 | 2051 | 2052 | 2055 | 2056);
                let escape = matches!(rule, 2046 | 2048 | 2050 | 2052 | 2054 | 2056);
                let pat_i = match (similar, not) {
                    (false, false) => 3,
                    (false, true) => 4,
                    (true, false) => 4,
                    (true, true) => 5,
                };
                let lexpr = view.v(1).node();
                let pat = view.v(pat_i).node().expect("a_expr");
                let rexpr = if similar {
                    let args = if escape {
                        NodeList::make2(
                            mcx,
                            pat,
                            view.v(pat_i + 2).node().expect("a_expr"),
                        )?
                    } else {
                        NodeList::make1(mcx, pat)?
                    };
                    make_func_call(
                        mcx,
                        system_func_name(mcx, "similar_to_escape")?,
                        args,
                        CoercionForm::COERCE_EXPLICIT_CALL,
                        view.l(2),
                    )?
                    .seal()
                } else if escape {
                    let args = NodeList::make2(
                        mcx,
                        pat,
                        view.v(pat_i + 2).node().expect("a_expr"),
                    )?;
                    make_func_call(
                        mcx,
                        system_func_name(mcx, "like_escape")?,
                        args,
                        CoercionForm::COERCE_EXPLICIT_CALL,
                        view.l(2),
                    )?
                    .seal()
                } else {
                    pat
                };
                let name = NodeList::make1(mcx, Node::mk_string(mcx, op)?)?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Expr {
                        kind,
                        name,
                        lexpr,
                        rexpr: Some(rexpr),
                        rexpr_list_start: 0,
                        rexpr_list_end: 0,
                        location: view.l(2),
                    },
                )?));
            }
            // a_expr [NOT] IN_P select_with_parens
            2074 | 2076 => {
                let subselect_i = if rule == 2074 { 3 } else { 4 };
                let sublink = Node::mk(
                    mcx,
                    types_nodes::SubLink {
                        subLinkType: types_nodes::SubLinkType::ANY_SUBLINK,
                        subLinkId: 0,
                        testexpr: view.v(1).node(),
                        operName: NodeList::nil(),
                        subselect: view.v(subselect_i).node().expect("select_with_parens"),
                        location: view.l(2),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(if rule == 2074 {
                    sublink
                } else {
                    Node::mk(
                        mcx,
                        BoolExpr {
                            boolop: BoolExprType::NOT_EXPR,
                            args: NodeList::make1(mcx, sublink)?,
                            location: view.l(2),
                        },
                    )?
                }));
            }
            // a_expr [NOT] IN_P '(' expr_list ')'
            2075 | 2077 => {
                let (op, lparen_i) = if rule == 2075 { ("=", 3) } else { ("<>", 4) };
                let list = Node::mk_list(mcx, view.v(lparen_i + 1).list())?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Expr {
                        kind: A_Expr_Kind::AEXPR_IN,
                        name: NodeList::make1(mcx, Node::mk_string(mcx, op)?)?,
                        lexpr: view.v(1).node(),
                        rexpr: Some(list),
                        rexpr_list_start: view.l(lparen_i),
                        rexpr_list_end: view.l(lparen_i + 2),
                        location: view.l(2),
                    },
                )?));
            }
            // a_expr: DEFAULT (parse analysis errors it outside VALUES/SET)
            2089 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::SetToDefault {
                        typeId: 0,
                        typeMod: 0,
                        collation: 0,
                        location: view.l(1),
                    },
                )?));
            }
            // b_expr: the a_expr forms without boolean/IS tails (DISTINCT and
            // IS DOCUMENT arms 2108-2111 stay unimplemented-rule loud).
            2091 => {
                let arg = view.v(1).node();
                let t = view.v(3).node().expect("Typename");
                *yyval = YYSTYPE::Node(Some(make_type_cast(mcx, arg, t, view.l(2))?));
            }
            2092 => {
                let r = view.v(2).node();
                *yyval = self.simple_a_expr("+", None, r, view.l(1))?;
            }
            2093 => {
                let n = view.v(2).node().expect("b_expr");
                *yyval = self.do_negate(n, view.l(1))?;
            }
            2094..=2105 => {
                let op = MATH_OPS[rule - 2094];
                let l = view.v(1).node();
                let r = view.v(3).node();
                *yyval = self.simple_a_expr(op, l, r, view.l(2))?;
            }
            2106 => {
                let name = view.v(2).list();
                let l = view.v(1).node();
                let r = view.v(3).node();
                *yyval = YYSTYPE::Node(Some(make_a_expr(mcx, name, l, r, view.l(2))?));
            }
            2107 => {
                let name = view.v(1).list();
                let r = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(make_a_expr(mcx, name, None, r, view.l(1))?));
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
            // SUBSTRING '(' substr_list ')' — substring(A FROM B FOR C)
            // becomes substring(A, B, C), SQL-syntax form.
            2163 => {
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "substring")?,
                    view.v(3).list(),
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // SUBSTRING '(' func_arg_list_opt ')' — plain call form.
            2164 => {
                let f = make_func_call(
                    mcx,
                    NodeList::make1(mcx, Node::mk_string(mcx, "substring")?)?,
                    view.v(3).list(),
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // substr_list: FROM..FOR / FOR..FROM / FROM / FOR / SIMILAR..ESCAPE.
            2322 | 2326 => {
                let a = view.v(1).node().expect("a_expr");
                let b = view.v(3).node().expect("a_expr");
                let c = view.v(5).node().expect("a_expr");
                *yyval = YYSTYPE::List(NodeList::make3(mcx, a, b, c)?);
            }
            2323 => {
                let a = view.v(1).node().expect("a_expr");
                let b = view.v(5).node().expect("a_expr");
                let c = view.v(3).node().expect("a_expr");
                *yyval = YYSTYPE::List(NodeList::make3(mcx, a, b, c)?);
            }
            2324 => {
                let a = view.v(1).node().expect("a_expr");
                let b = view.v(3).node().expect("a_expr");
                *yyval = YYSTYPE::List(NodeList::make2(mcx, a, b)?);
            }
            // FOR-only: forcibly cast the length to int4 so resolution picks
            // substring(text,int4,int4) over substring(text,text).
            2325 => {
                let a = view.v(1).node().expect("a_expr");
                let one = make_int_const(mcx, 1, -1)?;
                let cast = make_type_cast(
                    mcx,
                    view.v(3).node(),
                    system_type_name(mcx, "int4", NodeList::nil(), -1)?,
                    -1,
                )?;
                *yyval = YYSTYPE::List(NodeList::make3(mcx, a, one, cast)?);
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
            // sub_type: ANY/SOME -> ANY_SUBLINK, ALL -> ALL_SUBLINK
            2263 | 2264 => {
                *yyval = YYSTYPE::Ival(types_nodes::SubLinkType::ANY_SUBLINK as i32)
            }
            2265 => *yyval = YYSTYPE::Ival(types_nodes::SubLinkType::ALL_SUBLINK as i32),
            2268..=2279 => *yyval = YYSTYPE::Keyword(MATH_OPS[rule - 2268]),
            // any_operator: all_Op | ColId '.' any_operator
            1238 => {
                let op = view.v(1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, op)?)?);
            }
            1239 => {
                let name = view.v(1).str_val();
                let rest = view.v(3).list();
                let mut list = NodeList::make1(mcx, Node::mk_string(mcx, name)?)?;
                for n in rest.iter() {
                    list.lappend(mcx, n)?;
                }
                *yyval = YYSTYPE::List(list);
            }
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
            // REINDEX + CLUSTER productions; rule numbers pinned by
            // cluster_reindex_rule_numbers_match_tables.
            1263 => {
                let v2 = view.v(2);
                let mut params =
                    if v2.is_null_node() { NodeList::nil() } else { v2.list() };
                if view.v(4).boolean() {
                    let d = def_elem(mcx, "concurrently", None, view.l(4))?.node().unwrap();
                    params.lappend(mcx, d)?;
                }
                let mut n = Node::build::<ReindexStmt>(mcx)?;
                n.kind = reindex_object_type(view.v(3).ival());
                n.relation = view.v(5).node();
                n.name = None;
                n.params = params;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1264 | 1265 => {
                let v2 = view.v(2);
                let mut params =
                    if v2.is_null_node() { NodeList::nil() } else { v2.list() };
                if view.v(4).boolean() {
                    let d = def_elem(mcx, "concurrently", None, view.l(4))?.node().unwrap();
                    params.lappend(mcx, d)?;
                }
                let mut n = Node::build::<ReindexStmt>(mcx)?;
                n.kind = if rule == 1264 {
                    ReindexObjectType::REINDEX_OBJECT_SCHEMA
                } else {
                    reindex_object_type(view.v(3).ival())
                };
                n.relation = None;
                n.name = opt_str(view.v(5));
                n.params = params;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1266 => *yyval = YYSTYPE::Ival(ReindexObjectType::REINDEX_OBJECT_INDEX as i32),
            1267 => *yyval = YYSTYPE::Ival(ReindexObjectType::REINDEX_OBJECT_TABLE as i32),
            1268 => *yyval = YYSTYPE::Ival(ReindexObjectType::REINDEX_OBJECT_SYSTEM as i32),
            1269 => *yyval = YYSTYPE::Ival(ReindexObjectType::REINDEX_OBJECT_DATABASE as i32),
            1549 => {
                let mut n = Node::build::<ClusterStmt>(mcx)?;
                n.relation = view.v(5).node();
                n.indexname = opt_str(view.v(6));
                n.params = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1550 => {
                let mut n = Node::build::<ClusterStmt>(mcx)?;
                n.params = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1551 | 1552 | 1553 => {
                let mut params = NodeList::nil();
                if view.v(2).boolean() {
                    let d = def_elem(mcx, "verbose", None, view.l(2))?.node().unwrap();
                    params.lappend(mcx, d)?;
                }
                let mut n = Node::build::<ClusterStmt>(mcx)?;
                match rule {
                    1551 => {
                        n.relation = view.v(3).node();
                        n.indexname = opt_str(view.v(4));
                    }
                    1553 => {
                        n.indexname = Some(view.v(3).str_val());
                        n.relation = view.v(5).node();
                    }
                    _ => {}
                }
                n.params = params;
                *yyval = YYSTYPE::Node(Some(n.seal()));
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
            // ClosePortalStmt: CLOSE cursor_name | CLOSE ALL.
            407 => {
                let n = Node::mk(mcx, ClosePortalStmt { portalname: Some(view.v(2).str_val()) })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            408 => {
                let n = Node::mk(mcx, ClosePortalStmt { portalname: None })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // FetchStmt: FETCH fetch_args | MOVE fetch_args.
            1005 | 1006 => {
                let node = view.v(2).node().expect("fetch_args");
                let ismove = rule == 1006;
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    node.with_mut::<FetchStmt, _>(|f| f.ismove = ismove)
                        .expect("fetch_args is FetchStmt");
                }
                *yyval = YYSTYPE::Node(Some(node));
            }
            // fetch_args: all sixteen direction forms (gram.y 7462-7623).
            1007..=1022 => {
                use types_nodes::parsenodes::FetchDirection::*;
                let (name_slot, direction, how_many) = match rule {
                    1007 => (1, FETCH_FORWARD, 1),
                    1008 => (2, FETCH_FORWARD, 1),
                    1009 => (3, FETCH_FORWARD, 1),
                    1010 => (3, FETCH_BACKWARD, 1),
                    1011 => (3, FETCH_ABSOLUTE, 1),
                    1012 => (3, FETCH_ABSOLUTE, -1),
                    1013 => (4, FETCH_ABSOLUTE, view.v(2).ival() as i64),
                    1014 => (4, FETCH_RELATIVE, view.v(2).ival() as i64),
                    1015 => (3, FETCH_FORWARD, view.v(1).ival() as i64),
                    1016 => (3, FETCH_FORWARD, FETCH_ALL),
                    1017 => (3, FETCH_FORWARD, 1),
                    1018 => (4, FETCH_FORWARD, view.v(2).ival() as i64),
                    1019 => (4, FETCH_FORWARD, FETCH_ALL),
                    1020 => (3, FETCH_BACKWARD, 1),
                    1021 => (4, FETCH_BACKWARD, view.v(2).ival() as i64),
                    _ => (4, FETCH_BACKWARD, FETCH_ALL),
                };
                let n = Node::mk(
                    mcx,
                    FetchStmt {
                        direction,
                        howMany: how_many,
                        portalname: Some(view.v(name_slot).str_val()),
                        ismove: false,
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // DeclareCursorStmt: DECLARE cursor_name cursor_options CURSOR
            // opt_hold FOR SelectStmt; FAST_PLAN always set (gram.y 12756).
            1694 => {
                let n = Node::mk(
                    mcx,
                    DeclareCursorStmt {
                        portalname: Some(view.v(2).str_val()),
                        options: view.v(3).ival() | view.v(5).ival() | CURSOR_OPT_FAST_PLAN,
                        query: view.v(7).node(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1696 => *yyval = YYSTYPE::Ival(0),
            1697 => *yyval = YYSTYPE::Ival(view.v(1).ival() | CURSOR_OPT_NO_SCROLL),
            1698 => *yyval = YYSTYPE::Ival(view.v(1).ival() | CURSOR_OPT_SCROLL),
            1699 => *yyval = YYSTYPE::Ival(view.v(1).ival() | CURSOR_OPT_BINARY),
            1700 => *yyval = YYSTYPE::Ival(view.v(1).ival() | CURSOR_OPT_ASENSITIVE),
            1701 => *yyval = YYSTYPE::Ival(view.v(1).ival() | CURSOR_OPT_INSENSITIVE),
            1702 | 1704 => *yyval = YYSTYPE::Ival(0),
            1703 => *yyval = YYSTYPE::Ival(CURSOR_OPT_HOLD),
            2299 => {
                let t = view.v(1).node().expect("Typename");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            2300 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("Typename"))?;
                *yyval = YYSTYPE::List(list);
            }
            // ExplainStmt: EXPLAIN [analyze_keyword opt_verbose | VERBOSE |
            // '(' utility_option_list ')'] ExplainableStmt.
            1586 => {
                let mut n = Node::build::<types_nodes::parsenodes::ExplainStmt>(mcx)?;
                n.query = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1587 => {
                let mut n = Node::build::<types_nodes::parsenodes::ExplainStmt>(mcx)?;
                n.query = view.v(4).node();
                let analyze = def_elem(mcx, "analyze", None, view.l(2))?.node().unwrap();
                let mut options = NodeList::make1(mcx, analyze)?;
                if view.v(3).boolean() {
                    let verbose = def_elem(mcx, "verbose", None, view.l(3))?.node().unwrap();
                    options.lappend(mcx, verbose)?;
                }
                n.options = options;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1588 => {
                let mut n = Node::build::<types_nodes::parsenodes::ExplainStmt>(mcx)?;
                n.query = view.v(3).node();
                let verbose = def_elem(mcx, "verbose", None, view.l(2))?.node().unwrap();
                n.options = NodeList::make1(mcx, verbose)?;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1581 => {
                let mut n = Node::build::<VacuumRelation>(mcx)?;
                n.relation = view.v(1).node();
                n.oid = 0;
                n.va_cols = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // VariableSetStmt: SET set_rest / SET LOCAL set_rest / SET SESSION set_rest.
            201 | 202 | 203 => {
                let n = view.v(if rule == 201 { 2 } else { 3 }).node().expect("set_rest");
                let local = rule == 202;
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    n.with_mut::<VariableSetStmt, _>(|v| v.is_local = local)
                        .expect("set_rest is VariableSetStmt");
                }
                *yyval = YYSTYPE::Node(Some(n));
            }
            // set_rest: TRANSACTION / SESSION CHARACTERISTICS mode lists.
            204 | 205 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_MULTI;
                n.name = Some(if rule == 204 { "TRANSACTION" } else { "SESSION CHARACTERISTICS" });
                n.args = view.v(if rule == 204 { 2 } else { 5 }).list();
                n.jumble_args = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // a_expr AT TIME ZONE a_expr | a_expr AT LOCAL.
            2023 => {
                let args = NodeList::make2(
                    mcx,
                    view.v(5).node().expect("a_expr"),
                    view.v(1).node().expect("a_expr"),
                )?;
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "timezone")?,
                    args,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(2),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            2024 => {
                let args = NodeList::make1(mcx, view.v(1).node().expect("a_expr"))?;
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "timezone")?,
                    args,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    -1,
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // CURRENT_DATE .. LOCALTIMESTAMP[(n)] (makeSQLValueFunction).
            2140..=2148 => {
                use SQLValueFunctionOp as Op;
                let (op, typmod) = match rule {
                    2140 => (Op::SVFOP_CURRENT_DATE, -1),
                    2141 => (Op::SVFOP_CURRENT_TIME, -1),
                    2142 => (Op::SVFOP_CURRENT_TIME_N, view.v(3).ival()),
                    2143 => (Op::SVFOP_CURRENT_TIMESTAMP, -1),
                    2144 => (Op::SVFOP_CURRENT_TIMESTAMP_N, view.v(3).ival()),
                    2145 => (Op::SVFOP_LOCALTIME, -1),
                    2146 => (Op::SVFOP_LOCALTIME_N, view.v(3).ival()),
                    2147 => (Op::SVFOP_LOCALTIMESTAMP, -1),
                    _ => (Op::SVFOP_LOCALTIMESTAMP_N, view.v(3).ival()),
                };
                let n = Node::mk(
                    mcx,
                    SQLValueFunction { op, r#type: 0, typmod, location: view.l(1) },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            2149 | 2150 | 2151 | 2153 | 2154 | 2155 => {
                use types_nodes::SQLValueFunctionOp::*;
                let op = match rule {
                    2149 => SVFOP_CURRENT_ROLE,
                    2150 => SVFOP_CURRENT_USER,
                    2151 => SVFOP_SESSION_USER,
                    2153 => SVFOP_USER,
                    2154 => SVFOP_CURRENT_CATALOG,
                    _ => SVFOP_CURRENT_SCHEMA,
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::SQLValueFunction {
                        op,
                        r#type: 0,
                        typmod: -1,
                        location: view.l(1),
                    },
                )?));
            }
            // SYSTEM_USER: FuncCall via SystemFuncName.
            2152 => {
                let names = NodeList::make2(
                    mcx,
                    Node::mk_string(mcx, "pg_catalog")?,
                    Node::mk_string(mcx, "system_user")?,
                )?;
                let f = make_func_call(
                    mcx,
                    names,
                    NodeList::nil(),
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // EXTRACT '(' extract_list ')'.
            2157 => {
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "extract")?,
                    view.v(3).list(),
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // extract_list: extract_arg FROM a_expr.
            2306 => {
                let s = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String { sval: view.v(1).str_val() })),
                    view.l(1),
                )?;
                let e = view.v(3).node().expect("a_expr");
                *yyval = YYSTYPE::List(NodeList::make2(mcx, s, e)?);
            }
            // extract_arg keyword forms (IDENT/Sconst ride DISPATCH).
            2308..=2313 => {
                *yyval = YYSTYPE::Str(
                    ["year", "month", "day", "hour", "minute", "second"][rule - 2308],
                );
            }
            // generic_set: var_name TO var_list | var_name '=' var_list.
            207 | 208 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_VALUE;
                n.name = Some(view.v(1).str_val());
                n.args = view.v(3).list();
                n.location = view.l(3);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // set_rest_more: var_name TO/= DEFAULT | var_name FROM CURRENT.
            209 | 210 | 212 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = if rule == 212 {
                    VariableSetKind::VAR_SET_CURRENT
                } else {
                    VariableSetKind::VAR_SET_DEFAULT
                };
                n.name = Some(view.v(1).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            211 => *yyval = view.v(1),
            // set_rest_more: TIME ZONE zone_value (NULL zone_value = DEFAULT).
            213 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_VALUE;
                n.name = Some("timezone");
                n.jumble_args = true;
                match view.v(3).node() {
                    Some(z) => n.args = NodeList::make1(mcx, z)?,
                    None => n.kind = VariableSetKind::VAR_SET_DEFAULT,
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // set_rest_more: SESSION AUTHORIZATION NonReservedWord_or_Sconst | DEFAULT.
            218 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_VALUE;
                n.name = Some("session_authorization");
                let s = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String { sval: view.v(3).str_val() })),
                    view.l(3),
                )?;
                n.args = NodeList::make1(mcx, s)?;
                n.location = view.l(3);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            219 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_DEFAULT;
                n.name = Some("session_authorization");
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // set_rest: TRANSACTION SNAPSHOT Sconst.
            221 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_MULTI;
                n.name = Some("TRANSACTION SNAPSHOT");
                let s = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String { sval: view.v(3).str_val() })),
                    view.l(3),
                )?;
                n.args = NodeList::make1(mcx, s)?;
                n.location = view.l(3);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // var_name: var_name '.' ColId (psprintf "%s.%s").
            223 => {
                let a = view.v(1).str_val();
                let b = view.v(3).str_val();
                let mut v: mcx::PgVec<'mcx, u8> =
                    mcx::vec_with_capacity_in(mcx, a.len() + 1 + b.len())?;
                mcx::vec_append_bytes(&mut v, a.as_bytes())?;
                v.push(b'.');
                mcx::vec_append_bytes(&mut v, b.as_bytes())?;
                // SAFETY: concatenation of valid UTF-8 and '.'.
                *yyval = YYSTYPE::Str(unsafe { core::str::from_utf8_unchecked(v.leak()) });
            }
            224 => {
                let v = view.v(1).node().expect("var_value");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, v)?);
            }
            225 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("var_value"))?;
                *yyval = YYSTYPE::List(list);
            }
            // var_value: opt_boolean_or_string -> makeStringConst.
            226 => {
                let s = view.v(1).str_val();
                *yyval = self.a_const(
                    ValUnion::String(types_nodes::String { sval: s }),
                    view.l(1),
                )?;
            }
            227 => {
                let v = view.v(1).node().expect("NumericOnly");
                *yyval = make_a_const(mcx, v, view.l(1))?;
            }
            228 => *yyval = YYSTYPE::Str("read uncommitted"),
            229 => *yyval = YYSTYPE::Str("read committed"),
            230 => *yyval = YYSTYPE::Str("repeatable read"),
            231 => *yyval = YYSTYPE::Str("serializable"),
            // zone_value: Sconst | IDENT | NumericOnly (interval arms in reduce_cold).
            236 | 237 => {
                let s = view.v(1).str_val();
                *yyval = self.a_const(
                    ValUnion::String(types_nodes::String { sval: s }),
                    view.l(1),
                )?;
            }
            240 => {
                let v = view.v(1).node().expect("NumericOnly");
                *yyval = make_a_const(mcx, v, view.l(1))?;
            }
            248 => *yyval = view.v(2),
            // reset_rest: TIME ZONE / TRANSACTION ISOLATION LEVEL / SESSION AUTHORIZATION.
            250 | 251 | 252 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_RESET;
                n.name = Some(match rule {
                    250 => "timezone",
                    251 => "transaction_isolation",
                    _ => "session_authorization",
                });
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // generic_reset: var_name.
            253 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_RESET;
                n.name = Some(view.v(1).str_val());
                n.location = -1;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // generic_reset: ALL.
            254 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_RESET_ALL;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // VariableShowStmt: var_name and the four keyword SHOW forms.
            259 => {
                let n = Node::mk(mcx, VariableShowStmt { name: Some(view.v(2).str_val()) })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            260 | 261 | 262 | 263 => {
                let name = match rule {
                    260 => "timezone",
                    261 => "transaction_isolation",
                    262 => "session_authorization",
                    _ => "all",
                };
                let n = Node::mk(mcx, VariableShowStmt { name: Some(name) })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // LoadStmt: LOAD file_name
            1496 => {
                let filename = view.v(2).str_val();
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, LoadStmt { filename })?));
            }
            // file_name: Sconst
            2436 => *yyval = YYSTYPE::Str(view.v(1).str_val()),
            // CheckPointStmt: CHECKPOINT
            269 => *yyval = YYSTYPE::Node(Some(Node::mk(mcx, CheckPointStmt {})?)),
            // LockStmt: LOCK_P opt_table relation_expr_list opt_lock opt_nowait
            1648 => {
                let n = Node::mk(
                    mcx,
                    LockStmt {
                        relations: view.v(3).list(),
                        mode: view.v(4).ival(),
                        nowait: view.v(5).ival() != 0,
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // opt_lock: IN lock_type MODE | EMPTY
            1649 => *yyval = YYSTYPE::Ival(view.v(2).ival()),
            1650 => *yyval = YYSTYPE::Ival(8),
            // lock_type (lockdefs.h values, declaration order)
            1651 => *yyval = YYSTYPE::Ival(1),
            1652 => *yyval = YYSTYPE::Ival(2),
            1653 => *yyval = YYSTYPE::Ival(3),
            1654 => *yyval = YYSTYPE::Ival(4),
            1655 => *yyval = YYSTYPE::Ival(5),
            1656 => *yyval = YYSTYPE::Ival(6),
            1657 => *yyval = YYSTYPE::Ival(7),
            1658 => *yyval = YYSTYPE::Ival(8),
            // opt_nowait: NOWAIT | EMPTY
            1659 => *yyval = YYSTYPE::Ival(1),
            1660 => *yyval = YYSTYPE::Ival(0),
            // DiscardStmt: DISCARD ALL/TEMP/TEMPORARY/PLANS/SEQUENCES.
            270 | 271 | 272 | 273 | 274 => {
                let target = match rule {
                    270 => DiscardMode::DISCARD_ALL,
                    271 | 272 => DiscardMode::DISCARD_TEMP,
                    273 => DiscardMode::DISCARD_PLANS,
                    _ => DiscardMode::DISCARD_SEQUENCES,
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, DiscardStmt { target })?));
            }
            // NumericOnly: FCONST | '+' FCONST | '-' FCONST | SignedIconst.
            660 => *yyval = YYSTYPE::Node(Some(Node::mk_float(mcx, view.v(1).str_val())?)),
            661 => *yyval = YYSTYPE::Node(Some(Node::mk_float(mcx, view.v(2).str_val())?)),
            662 => {
                *yyval =
                    YYSTYPE::Node(Some(Node::mk_float(mcx, negate_float(mcx, view.v(2).str_val())?)?));
            }
            663 => *yyval = YYSTYPE::Node(Some(Node::mk_integer(mcx, view.v(1).ival())?)),
            // NotifyStmt/ListenStmt/UnlistenStmt: parse is C-complete; execution is the loud async lane.
            1452 => {
                let n = Node::mk(
                    mcx,
                    NotifyStmt {
                        conditionname: Some(view.v(2).str_val()),
                        payload: opt_str(view.v(3)),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1455 => {
                let n = Node::mk(mcx, ListenStmt { conditionname: Some(view.v(2).str_val()) })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1456 | 1457 => {
                let conditionname = if rule == 1456 { Some(view.v(2).str_val()) } else { None };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, UnlistenStmt { conditionname })?));
            }
            // TransactionStmt: ABORT [chain] / START TRANSACTION modes.
            1458 => {
                let mut n = Node::build::<TransactionStmt>(mcx)?;
                n.kind = TransactionStmtKind::TRANS_STMT_ROLLBACK;
                n.chain = view.v(3).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1459 => {
                let mut n = Node::build::<TransactionStmt>(mcx)?;
                n.kind = TransactionStmtKind::TRANS_STMT_START;
                n.options = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // TransactionStmt: COMMIT/ROLLBACK [chain], SAVEPOINT, RELEASE
            // SAVEPOINT, ROLLBACK TO SAVEPOINT; TransactionStmtLegacy BEGIN.
            1460 | 1461 => {
                let mut n = Node::build::<TransactionStmt>(mcx)?;
                n.kind = if rule == 1460 {
                    TransactionStmtKind::TRANS_STMT_COMMIT
                } else {
                    TransactionStmtKind::TRANS_STMT_ROLLBACK
                };
                n.chain = view.v(3).boolean();
                n.location = -1;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1462 | 1463 | 1464 | 1465 | 1466 => {
                let (kind, i) = match rule {
                    1462 => (TransactionStmtKind::TRANS_STMT_SAVEPOINT, 2),
                    1463 => (TransactionStmtKind::TRANS_STMT_RELEASE, 3),
                    1464 => (TransactionStmtKind::TRANS_STMT_RELEASE, 2),
                    1465 => (TransactionStmtKind::TRANS_STMT_ROLLBACK_TO, 5),
                    _ => (TransactionStmtKind::TRANS_STMT_ROLLBACK_TO, 4),
                };
                let mut n = Node::build::<TransactionStmt>(mcx)?;
                n.kind = kind;
                n.savepoint_name = Some(view.v(i).str_val());
                n.location = view.l(i);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1470 => {
                let mut n = Node::build::<TransactionStmt>(mcx)?;
                n.kind = TransactionStmtKind::TRANS_STMT_BEGIN;
                n.options = view.v(3).list();
                n.location = -1;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // TransactionStmtLegacy: END [chain].
            1471 => {
                let mut n = Node::build::<TransactionStmt>(mcx)?;
                n.kind = TransactionStmtKind::TRANS_STMT_COMMIT;
                n.chain = view.v(3).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // transaction_mode_item -> DefElem.
            1475 => {
                let s = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String { sval: view.v(3).str_val() })),
                    view.l(3),
                )?;
                *yyval = def_elem(mcx, "transaction_isolation", Some(s), view.l(1))?;
            }
            1476 | 1477 => {
                let c = make_int_const(mcx, (rule == 1476) as i32, view.l(1))?;
                *yyval = def_elem(mcx, "transaction_read_only", Some(c), view.l(1))?;
            }
            1478 | 1479 => {
                let c = make_int_const(mcx, (rule == 1478) as i32, view.l(1))?;
                *yyval = def_elem(mcx, "transaction_deferrable", Some(c), view.l(1))?;
            }
            // transaction_mode_list ( ',' | nothing ) transaction_mode_item.
            1480 => {
                let item = view.v(1).node().expect("transaction_mode_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, item)?);
            }
            1481 | 1482 => {
                let mut list = view.v(1).list();
                let at = if rule == 1481 { 3 } else { 2 };
                list.lappend(mcx, view.v(at).node().expect("transaction_mode_item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // opt_transaction_chain: AND CHAIN | AND NO CHAIN | empty (1487).
            1485 => *yyval = YYSTYPE::Boolean(true),
            1486 | 1487 => *yyval = YYSTYPE::Boolean(false),
            // CreatedbStmt: CREATE DATABASE name opt_with createdb_opt_list.
            1497 => {
                let mut n = Node::build::<types_nodes::parsenodes::CreatedbStmt>(mcx)?;
                n.dbname = Some(view.v(3).str_val());
                n.options = view.v(5).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1500 => {
                let item = view.v(1).node().expect("createdb_opt_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, item)?);
            }
            1501 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("createdb_opt_item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // createdb_opt_item: name opt_equal (NumericOnly | opt_boolean_or_string | DEFAULT).
            1502 => *yyval = def_elem(mcx, view.v(1).str_val(), view.v(3).node(), view.l(1))?,
            1503 => {
                let s = Node::mk_string(mcx, view.v(3).str_val())?;
                *yyval = def_elem(mcx, view.v(1).str_val(), Some(s), view.l(1))?;
            }
            1504 => *yyval = def_elem(mcx, view.v(1).str_val(), None, view.l(1))?,
            // createdb_opt_name: CONNECTION LIMIT (IDENT/keyword arms ride DISPATCH).
            1506 => *yyval = YYSTYPE::Str("connection_limit"),
            // AlterDatabaseStmt / AlterDatabaseRefreshCollStmt / AlterDatabaseSetStmt.
            1514 | 1515 => {
                let mut n = Node::build::<types_nodes::parsenodes::AlterDatabaseStmt>(mcx)?;
                n.dbname = Some(view.v(3).str_val());
                n.options = view.v(if rule == 1514 { 5 } else { 4 }).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1516 => {
                let mut n = Node::build::<types_nodes::parsenodes::AlterDatabaseStmt>(mcx)?;
                n.dbname = Some(view.v(3).str_val());
                let s = Node::mk_string(mcx, view.v(6).str_val())?;
                let d = Node::mk(
                    mcx,
                    DefElem {
                        defnamespace: None,
                        defname: Some("tablespace"),
                        arg: Some(s),
                        defaction: DefElemAction::DEFELEM_UNSPEC,
                        location: view.l(6),
                    },
                )?;
                n.options = NodeList::make1(mcx, d)?;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1517 => {
                let mut n =
                    Node::build::<types_nodes::parsenodes::AlterDatabaseRefreshCollStmt>(mcx)?;
                n.dbname = Some(view.v(3).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1518 => {
                let mut n = Node::build::<types_nodes::parsenodes::AlterDatabaseSetStmt>(mcx)?;
                n.dbname = Some(view.v(3).str_val());
                n.setstmt = view.v(4).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DropdbStmt: DROP DATABASE [IF EXISTS] name [opt_with '(' drop_option_list ')'].
            1519 | 1520 => {
                let mut n = Node::build::<types_nodes::parsenodes::DropdbStmt>(mcx)?;
                n.dbname = Some(view.v(if rule == 1519 { 3 } else { 5 }).str_val());
                n.missing_ok = rule == 1520;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1521 | 1522 => {
                let mut n = Node::build::<types_nodes::parsenodes::DropdbStmt>(mcx)?;
                let (name, opts) = if rule == 1521 { (3, 6) } else { (5, 8) };
                n.dbname = Some(view.v(name).str_val());
                n.missing_ok = rule == 1522;
                n.options = view.v(opts).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1523 => {
                let item = view.v(1).node().expect("drop_option");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, item)?);
            }
            1524 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("drop_option"))?;
                *yyval = YYSTYPE::List(list);
            }
            // drop_option: FORCE.
            1525 => *yyval = def_elem(mcx, "force", None, view.l(1))?,
            1589 => {
                let mut n = Node::build::<types_nodes::parsenodes::ExplainStmt>(mcx)?;
                n.query = view.v(5).node();
                n.options = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // table_ref: select_with_parens opt_alias_clause.
            1838 => {
                let mut n = Node::build::<RangeSubselect>(mcx)?;
                n.lateral = false;
                n.subquery = view.v(1).node();
                n.alias = view.v(2).alias();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alias_clause: AS ColId '(' name_list ')'.
            1850 => {
                let a = Node::mk_mut(
                    mcx,
                    Alias { aliasname: Some(view.v(2).str_val()), colnames: view.v(4).list() },
                )?;
                *yyval = YYSTYPE::Alias(Some(a.seal_ref()));
            }
            // table_ref: joined_table | '(' joined_table ')' alias_clause.
            1840 => *yyval = YYSTYPE::Node(view.v(1).node()),
            1841 => {
                let j = view.v(2).node().expect("joined_table");
                let alias = view.v(4).alias();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    j.with_mut::<JoinExpr, _>(|n| n.alias = alias).expect("joined_table is JoinExpr")
                };
                *yyval = YYSTYPE::Node(Some(j));
            }
            // joined_table: CROSS JOIN | join_type JOIN ... join_qual |
            // JOIN ... join_qual | NATURAL variants (unported louds).
            1845 | 1846 | 1847 => {
                let (jointype, rarg_at, qual_at) = match rule {
                    1845 => (JoinType::JOIN_INNER, 4, 0),
                    1846 => (join_type_from_ival(view.v(2).ival()), 4, 5),
                    _ => (JoinType::JOIN_INNER, 3, 4),
                };
                let quals = if qual_at == 0 { None } else { view.v(qual_at).node() };
                // join_qual USING is a loud unported rule (1869), so quals
                // here is always the ON expression.
                debug_assert!(!quals.is_some_and(|q| q.node_tag() == NodeTag::T_List));
                let n = Node::mk(
                    mcx,
                    JoinExpr {
                        jointype,
                        isNatural: false,
                        larg: view.v(1).node().expect("table_ref"),
                        rarg: view.v(rarg_at).node().expect("table_ref"),
                        usingClause: NodeList::nil(),
                        join_using_alias: None,
                        quals,
                        alias: None,
                        rtindex: 0,
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1848 | 1849 => panic!(
                "gram_core: NATURAL JOIN unimplemented (rule {rule}); join-using lane"
            ),
            // join_type: FULL/LEFT/RIGHT/INNER opt_outer.
            1863 => *yyval = YYSTYPE::Ival(JoinType::JOIN_FULL as i32),
            1864 => *yyval = YYSTYPE::Ival(JoinType::JOIN_LEFT as i32),
            1865 => *yyval = YYSTYPE::Ival(JoinType::JOIN_RIGHT as i32),
            1866 => *yyval = YYSTYPE::Ival(JoinType::JOIN_INNER as i32),
            1869 => panic!("gram_core: JOIN USING unimplemented (rule 1869); join-using lane"),
            2171 => {
                let n = Node::mk(
                    mcx,
                    CoalesceExpr {
                        coalescetype: 0,
                        coalescecollid: 0,
                        args: view.v(3).list(),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            2172 | 2173 => {
                let n = Node::mk(
                    mcx,
                    MinMaxExpr {
                        minmaxtype: 0,
                        minmaxcollid: 0,
                        inputcollid: 0,
                        op: if rule == 2172 { MinMaxOp::IS_GREATEST } else { MinMaxOp::IS_LEAST },
                        args: view.v(3).list(),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // case_expr / when_clause_list / when_clause.
            2330 => {
                let n = Node::mk(
                    mcx,
                    CaseExpr {
                        casetype: 0,
                        casecollid: 0,
                        arg: view.v(2).node(),
                        args: view.v(3).list(),
                        defresult: view.v(4).node(),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            2331 => {
                let w = view.v(1).node().expect("when_clause");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, w)?);
            }
            2332 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("when_clause"))?;
                *yyval = YYSTYPE::List(list);
            }
            2333 => {
                let n = Node::mk(
                    mcx,
                    CaseWhen {
                        expr: view.v(2).node(),
                        result: view.v(4).node(),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // window_definition_list: window_definition [, window_definition]
            2230 => {
                let w = view.v(1).node().expect("window_definition");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, w)?);
            }
            2231 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("window_definition"))?;
                *yyval = YYSTYPE::List(list);
            }
            // window_definition: ColId AS window_specification
            2232 => {
                let name = view.v(1).str_val();
                let n = view.v(3).node().expect("window_specification");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    n.with_mut::<WindowDef, _>(|w| w.name = Some(name)).expect("WindowDef");
                }
                *yyval = YYSTYPE::Node(Some(n));
            }
            // over_clause: OVER ColId
            2234 => {
                let mut n = Node::build::<WindowDef>(mcx)?;
                n.name = Some(view.v(2).str_val());
                n.frameOptions = FRAMEOPTION_DEFAULTS;
                n.location = view.l(2);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // window_specification: '(' opt_existing_window_name
            // opt_partition_clause opt_sort_clause opt_frame_clause ')'
            2236 => {
                let frame = view.v(5).node().expect("opt_frame_clause");
                let frame = frame.as_window_def().expect("WindowDef");
                let mut n = Node::build::<WindowDef>(mcx)?;
                n.refname = opt_str(view.v(2));
                n.partitionClause = view.v(3).list();
                n.orderClause = view.v(4).list();
                n.frameOptions = frame.frameOptions;
                n.startOffset = frame.startOffset;
                n.endOffset = frame.endOffset;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_frame_clause: RANGE|ROWS|GROUPS frame_extent
            // opt_window_exclusion_clause
            2241 | 2242 | 2243 => {
                let n = view.v(2).node().expect("frame_extent");
                let mode = match rule {
                    2241 => FRAMEOPTION_RANGE,
                    2242 => FRAMEOPTION_ROWS,
                    _ => FRAMEOPTION_GROUPS,
                };
                let exclusion = view.v(3).ival();
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    n.with_mut::<WindowDef, _>(|w| {
                        w.frameOptions |= FRAMEOPTION_NONDEFAULT | mode | exclusion;
                    })
                    .expect("WindowDef");
                }
                *yyval = YYSTYPE::Node(Some(n));
            }
            2244 => {
                let mut n = Node::build::<WindowDef>(mcx)?;
                n.frameOptions = FRAMEOPTION_DEFAULTS;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // frame_extent: frame_bound
            2245 => {
                let n = view.v(1).node().expect("frame_bound");
                let fo = n.as_window_def().expect("WindowDef").frameOptions;
                if fo & FRAMEOPTION_START_UNBOUNDED_FOLLOWING != 0 {
                    return Err(self.windowing_error(
                        "frame start cannot be UNBOUNDED FOLLOWING",
                        view.l(1),
                    ));
                }
                if fo & FRAMEOPTION_START_OFFSET_FOLLOWING != 0 {
                    return Err(self.windowing_error(
                        "frame starting from following row cannot end with current row",
                        view.l(1),
                    ));
                }
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    n.with_mut::<WindowDef, _>(|w| {
                        w.frameOptions |= FRAMEOPTION_END_CURRENT_ROW;
                    })
                    .expect("WindowDef");
                }
                *yyval = YYSTYPE::Node(Some(n));
            }
            // frame_extent: BETWEEN frame_bound AND frame_bound
            2246 => {
                let n1 = view.v(2).node().expect("frame_bound");
                let n2 = view.v(4).node().expect("frame_bound");
                let n2 = n2.as_window_def().expect("WindowDef");
                let mut fo = n1.as_window_def().expect("WindowDef").frameOptions;
                fo |= n2.frameOptions << 1;
                fo |= FRAMEOPTION_BETWEEN;
                if fo & FRAMEOPTION_START_UNBOUNDED_FOLLOWING != 0 {
                    return Err(self.windowing_error(
                        "frame start cannot be UNBOUNDED FOLLOWING",
                        view.l(2),
                    ));
                }
                if fo & FRAMEOPTION_END_UNBOUNDED_PRECEDING != 0 {
                    return Err(self.windowing_error(
                        "frame end cannot be UNBOUNDED PRECEDING",
                        view.l(4),
                    ));
                }
                if fo & FRAMEOPTION_START_CURRENT_ROW != 0
                    && fo & FRAMEOPTION_END_OFFSET_PRECEDING != 0
                {
                    return Err(self.windowing_error(
                        "frame starting from current row cannot have preceding rows",
                        view.l(4),
                    ));
                }
                if fo & FRAMEOPTION_START_OFFSET_FOLLOWING != 0
                    && fo & (FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_CURRENT_ROW) != 0
                {
                    return Err(self.windowing_error(
                        "frame starting from following row cannot have preceding rows",
                        view.l(4),
                    ));
                }
                let end_offset = n2.startOffset;
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    n1.with_mut::<WindowDef, _>(|w| {
                        w.frameOptions = fo;
                        w.endOffset = end_offset;
                    })
                    .expect("WindowDef");
                }
                *yyval = YYSTYPE::Node(Some(n1));
            }
            // frame_bound: UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING |
            // CURRENT ROW | a_expr PRECEDING | a_expr FOLLOWING
            2247..=2251 => {
                let mut n = Node::build::<WindowDef>(mcx)?;
                n.frameOptions = match rule {
                    2247 => FRAMEOPTION_START_UNBOUNDED_PRECEDING,
                    2248 => FRAMEOPTION_START_UNBOUNDED_FOLLOWING,
                    2249 => FRAMEOPTION_START_CURRENT_ROW,
                    2250 => FRAMEOPTION_START_OFFSET_PRECEDING,
                    _ => FRAMEOPTION_START_OFFSET_FOLLOWING,
                };
                if rule >= 2250 {
                    n.startOffset = view.v(1).node();
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_window_exclusion_clause
            2252 => *yyval = YYSTYPE::Ival(FRAMEOPTION_EXCLUDE_CURRENT_ROW),
            2253 => *yyval = YYSTYPE::Ival(FRAMEOPTION_EXCLUDE_GROUP),
            2254 => *yyval = YYSTYPE::Ival(FRAMEOPTION_EXCLUDE_TIES),
            2255 | 2256 => *yyval = YYSTYPE::Ival(0),
            // opt_drop_behavior: CASCADE | RESTRICT | /*EMPTY*/.
            143 => *yyval = YYSTYPE::Ival(DropBehavior::DROP_CASCADE as i32),
            144 | 145 => *yyval = YYSTYPE::Ival(DropBehavior::DROP_RESTRICT as i32),
            // AlterTableStmt: ALTER TABLE [IF_P EXISTS] relation_expr
            // alter_table_cmds (partition/tablespace forms 277-279 stay loud).
            275 | 276 => {
                let (rv, cmds) = if rule == 275 {
                    (view.v(3), view.v(4))
                } else {
                    (view.v(5), view.v(6))
                };
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = rv.node().expect("relation_expr").as_variant::<RangeVar>();
                n.cmds = cmds.list();
                n.objtype = ObjectType::OBJECT_TABLE;
                n.missing_ok = rule == 276;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alter_table_cmds: alter_table_cmd | alter_table_cmds ',' alter_table_cmd
            296 => {
                let el = view.v(1).node().expect("alter_table_cmd");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            297 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("alter_table_cmd"))?;
                *yyval = YYSTYPE::List(list);
            }
            // alter_table_cmd: ADD_P [COLUMN] [IF_P NOT EXISTS] columnDef
            // (other alter_table_cmd forms stay unimplemented-rule loud).
            302 | 303 | 304 | 305 => {
                let def = match rule {
                    302 => view.v(2),
                    303 => view.v(5),
                    304 => view.v(3),
                    _ => view.v(6),
                };
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_AddColumn;
                n.def = def.node();
                n.missing_ok = rule == 303 || rule == 305;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alter_column_default: SET DEFAULT a_expr | DROP DEFAULT
            364 => *yyval = YYSTYPE::Node(view.v(3).node()),
            365 => *yyval = YYSTYPE::Node(Option::None),
            // opt_collate_clause: /*EMPTY*/
            367 => *yyval = YYSTYPE::Node(Option::None),
            // alter_using: USING a_expr | /*EMPTY*/
            368 => *yyval = YYSTYPE::Node(view.v(2).node()),
            369 => *yyval = YYSTYPE::Node(Option::None),
            // TableConstraint: ConstraintElem (536 CONSTRAINT-name arm above)
            537 => *yyval = YYSTYPE::Node(view.v(1).node()),
            // RenameStmt: ALTER TABLE [IF_P EXISTS] relation_expr RENAME TO name
            1294 | 1295 => {
                let (rv, nm) = if rule == 1294 { (3, 6) } else { (5, 8) };
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_TABLE;
                n.relation = view.v(rv).node().expect("relation_expr").as_variant::<RangeVar>();
                n.newname = Some(view.v(nm).str_val());
                n.missing_ok = rule == 1295;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER TABLE [IF_P EXISTS] relation_expr RENAME
            // opt_column name TO name
            1306 | 1307 => {
                let (rv, sub, nm) = if rule == 1306 { (3, 6, 8) } else { (5, 8, 10) };
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_COLUMN;
                n.relationType = ObjectType::OBJECT_TABLE;
                n.relation = view.v(rv).node().expect("relation_expr").as_variant::<RangeVar>();
                n.subname = Some(view.v(sub).str_val());
                n.newname = Some(view.v(nm).str_val());
                n.missing_ok = rule == 1307;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_column: COLUMN | /*EMPTY*/
            1329 | 1330 => *yyval = YYSTYPE::Ival(0),
            // opt_set_data: SET DATA_P | /*EMPTY*/
            1331 => *yyval = YYSTYPE::Ival(1),
            1332 => *yyval = YYSTYPE::Ival(0),
            // alter_table_cmd: DROP opt_column [IF_P EXISTS] ColId opt_drop_behavior
            322 => {
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_DropColumn;
                n.name = Some(view.v(5).str_val());
                n.behavior = drop_behavior(view.v(6).ival());
                n.missing_ok = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            323 => {
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_DropColumn;
                n.name = Some(view.v(3).str_val());
                n.behavior = drop_behavior(view.v(4).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alter_table_cmd: ALTER [COLUMN] col forms (execution is the
            // tablecmds/parse_utilcmd named gates; 318/327/328 stay loud —
            // identity-generated and fk-constraints lanes own their inputs).
            306 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_ColumnDefault,
                    Some(view.v(3).str_val()),
                    view.v(4).node(),
                )?;
            }
            307 | 308 => {
                let subtype = if rule == 307 {
                    AlterTableType::AT_DropNotNull
                } else {
                    AlterTableType::AT_SetNotNull
                };
                *yyval = alter_table_cmd(mcx, subtype, Some(view.v(3).str_val()), None)?;
            }
            309 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_SetExpression,
                    Some(view.v(3).str_val()),
                    view.v(8).node(),
                )?;
            }
            310 | 311 => {
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_DropExpression;
                n.name = Some(view.v(3).str_val());
                n.missing_ok = rule == 311;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            312 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_SetStatistics,
                    Some(view.v(3).str_val()),
                    view.v(6).node(),
                )?;
            }
            313 => {
                let colnum = view.v(3).ival();
                if colnum <= 0 || colnum > i16::MAX as i32 {
                    return Err(self.invalid_parameter_error(
                        "column number must be in range from 1 to 32767",
                        view.l(3),
                    ));
                }
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_SetStatistics;
                n.num = colnum as i16;
                n.def = view.v(6).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            314 | 315 => {
                let subtype = if rule == 314 {
                    AlterTableType::AT_SetOptions
                } else {
                    AlterTableType::AT_ResetOptions
                };
                let def = Node::mk_list(mcx, view.v(5).list())?;
                *yyval = alter_table_cmd(mcx, subtype, Some(view.v(3).str_val()), Some(def))?;
            }
            316 | 317 => {
                let subtype = if rule == 316 {
                    AlterTableType::AT_SetStorage
                } else {
                    AlterTableType::AT_SetCompression
                };
                let def = Node::mk_string(mcx, view.v(5).str_val())?;
                *yyval = alter_table_cmd(mcx, subtype, Some(view.v(3).str_val()), Some(def))?;
            }
            319 => {
                let def = Node::mk_list(mcx, view.v(4).list())?;
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_SetIdentity,
                    Some(view.v(3).str_val()),
                    Some(def),
                )?;
            }
            320 | 321 => {
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_DropIdentity;
                n.name = Some(view.v(3).str_val());
                n.missing_ok = rule == 321;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ALTER [COLUMN] col [SET DATA] TYPE Typename opt_collate_clause
            // alter_using
            324 => {
                let mut def = Node::build::<ColumnDef>(mcx)?;
                def.typeName = view.v(6).node();
                def.collClause = view.v(7).node();
                def.raw_default = view.v(8).node();
                def.location = view.l(3);
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_AlterColumnType,
                    Some(view.v(3).str_val()),
                    Some(def.seal()),
                )?;
            }
            325 => {
                let def = Node::mk_list(mcx, view.v(4).list())?;
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_AlterColumnGenericOptions,
                    Some(view.v(3).str_val()),
                    Some(def),
                )?;
            }
            326 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_AddConstraint,
                    None,
                    view.v(2).node(),
                )?;
            }
            329 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_ValidateConstraint,
                    Some(view.v(3).str_val()),
                    None,
                )?;
            }
            330 | 331 => {
                let name_i = if rule == 330 { 5 } else { 3 };
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_DropConstraint;
                n.name = Some(view.v(name_i).str_val());
                n.behavior = drop_behavior(view.v(name_i + 1).ival());
                n.missing_ok = rule == 330;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            332 => *yyval = alter_table_cmd(mcx, AlterTableType::AT_DropOids, None, None)?,
            333 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_ClusterOn,
                    Some(view.v(3).str_val()),
                    None,
                )?;
            }
            334 => *yyval = alter_table_cmd(mcx, AlterTableType::AT_DropCluster, None, None)?,
            335 => *yyval = alter_table_cmd(mcx, AlterTableType::AT_SetLogged, None, None)?,
            336 => *yyval = alter_table_cmd(mcx, AlterTableType::AT_SetUnLogged, None, None)?,
            337..=348 => {
                let (subtype, name_i) = match rule {
                    337 => (AlterTableType::AT_EnableTrig, 3),
                    338 => (AlterTableType::AT_EnableAlwaysTrig, 4),
                    339 => (AlterTableType::AT_EnableReplicaTrig, 4),
                    340 => (AlterTableType::AT_EnableTrigAll, 0),
                    341 => (AlterTableType::AT_EnableTrigUser, 0),
                    342 => (AlterTableType::AT_DisableTrig, 3),
                    343 => (AlterTableType::AT_DisableTrigAll, 0),
                    344 => (AlterTableType::AT_DisableTrigUser, 0),
                    345 => (AlterTableType::AT_EnableRule, 3),
                    346 => (AlterTableType::AT_EnableAlwaysRule, 4),
                    347 => (AlterTableType::AT_EnableReplicaRule, 4),
                    _ => (AlterTableType::AT_DisableRule, 3),
                };
                let name = if name_i == 0 { None } else { Some(view.v(name_i).str_val()) };
                *yyval = alter_table_cmd(mcx, subtype, name, None)?;
            }
            349 | 350 => {
                let (subtype, rv_i) = if rule == 349 {
                    (AlterTableType::AT_AddInherit, 2)
                } else {
                    (AlterTableType::AT_DropInherit, 3)
                };
                *yyval = alter_table_cmd(mcx, subtype, None, view.v(rv_i).node())?;
            }
            351 => {
                let def = make_type_name(mcx, view.v(2).list(), NodeList::nil(), view.l(2))?;
                *yyval = alter_table_cmd(mcx, AlterTableType::AT_AddOf, None, Some(def))?;
            }
            352 => *yyval = alter_table_cmd(mcx, AlterTableType::AT_DropOf, None, None)?,
            353 => {
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_ChangeOwner;
                n.newowner = view.v(3).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            354 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_SetAccessMethod,
                    opt_str(view.v(4)),
                    None,
                )?;
            }
            355 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_SetTableSpace,
                    Some(view.v(3).str_val()),
                    None,
                )?;
            }
            356 | 357 => {
                let subtype = if rule == 356 {
                    AlterTableType::AT_SetRelOptions
                } else {
                    AlterTableType::AT_ResetRelOptions
                };
                let def = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = alter_table_cmd(mcx, subtype, None, Some(def))?;
            }
            358 => {
                *yyval = alter_table_cmd(
                    mcx,
                    AlterTableType::AT_ReplicaIdentity,
                    None,
                    view.v(3).node(),
                )?;
            }
            359 => *yyval = alter_table_cmd(mcx, AlterTableType::AT_EnableRowSecurity, None, None)?,
            360 => {
                *yyval = alter_table_cmd(mcx, AlterTableType::AT_DisableRowSecurity, None, None)?
            }
            361 => *yyval = alter_table_cmd(mcx, AlterTableType::AT_ForceRowSecurity, None, None)?,
            362 => {
                *yyval = alter_table_cmd(mcx, AlterTableType::AT_NoForceRowSecurity, None, None)?
            }
            363 => {
                let def = Node::mk_list(mcx, view.v(1).list())?;
                *yyval = alter_table_cmd(mcx, AlterTableType::AT_GenericOptions, None, Some(def))?;
            }
            // opt_collate_clause: COLLATE any_name.
            366 => {
                let n = Node::mk(
                    mcx,
                    CollateClause {
                        arg: None,
                        collname: view.v(2).list(),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // replica_identity: NOTHING | FULL | DEFAULT | USING INDEX name.
            370..=373 => {
                let (identity_type, name) = match rule {
                    370 => (REPLICA_IDENTITY_NOTHING, None),
                    371 => (REPLICA_IDENTITY_FULL, None),
                    372 => (REPLICA_IDENTITY_DEFAULT, None),
                    _ => (REPLICA_IDENTITY_INDEX, Some(view.v(3).str_val())),
                };
                let n = Node::mk(mcx, ReplicaIdentityStmt { identity_type, name })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // alter_identity_column_option_list / alter_identity_column_option.
            383 => {
                let d = view.v(1).node().expect("alter_identity_column_option");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, d)?);
            }
            384 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("alter_identity_column_option"))?;
                *yyval = YYSTYPE::List(list);
            }
            385 => *yyval = def_elem(mcx, "restart", None, view.l(1))?,
            386 => *yyval = def_elem(mcx, "restart", view.v(3).node(), view.l(1))?,
            387 => {
                let d = view.v(2).node().expect("SeqOptElem");
                let defname =
                    d.as_def_elem().expect("DefElem").defname.expect("SeqOptElem defname");
                if matches!(defname, "as" | "restart" | "owned_by") {
                    return Err(self.errposition_error(
                        format!("sequence option \"{defname}\" not supported here"),
                        view.l(2),
                    ));
                }
                *yyval = YYSTYPE::Node(Some(d));
            }
            388 => {
                let arg = Node::mk_integer(mcx, view.v(3).ival())?;
                *yyval = def_elem(mcx, "generated", Some(arg), view.l(1))?;
            }
            // set_statistics_value: SignedIconst (DEFAULT rides NULL dispatch).
            389 => *yyval = YYSTYPE::Node(Some(Node::mk_integer(mcx, view.v(1).ival())?)),
            // column_compression / column_storage: ... DEFAULT.
            486 | 490 => *yyval = YYSTYPE::Str("default"),
            // alter_generic_option_list / _elem / generic_option_elem / _arg.
            726 => {
                let d = view.v(1).node().expect("alter_generic_option_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, d)?);
            }
            727 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("alter_generic_option_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
            729 | 730 => {
                let d = view.v(2).node().expect("generic_option_elem");
                let action =
                    if rule == 729 { DefElemAction::DEFELEM_SET } else { DefElemAction::DEFELEM_ADD };
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    d.with_mut::<DefElem, _>(|e| e.defaction = action).expect("DefElem");
                }
                *yyval = YYSTYPE::Node(Some(d));
            }
            731 => {
                let n = Node::mk(
                    mcx,
                    DefElem {
                        defnamespace: None,
                        defname: Some(view.v(2).str_val()),
                        arg: None,
                        defaction: DefElemAction::DEFELEM_DROP,
                        location: view.l(2),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            732 => {
                *yyval = def_elem(mcx, view.v(1).str_val(), view.v(2).node(), view.l(1))?;
            }
            734 => *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, view.v(1).str_val())?)),
            // opt_with_data: WITH DATA | WITH NO DATA | EMPTY.
            623 | 625 => *yyval = YYSTYPE::Boolean(true),
            624 => *yyval = YYSTYPE::Boolean(false),
            // simple_select: TABLE relation_expr.
            1722 => {
                let star = NodeList::make1(mcx, Node::mk_a_star(mcx)?)?;
                let cr = Node::mk_column_ref(mcx, star, -1)?;
                let rt = Node::mk_res_target(mcx, None, NodeList::nil(), Some(cr), -1)?;
                let mut n = Node::build::<SelectStmt>(mcx)?;
                n.targetList = NodeList::make1(mcx, rt)?;
                n.fromClause =
                    NodeList::make1(mcx, view.v(2).node().expect("relation_expr"))?;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DropStmt: DROP {object_type_any_name any_name_list |
            // drop_type_name name_list} [IF EXISTS] opt_drop_behavior
            // (TYPE/DOMAIN/INDEX CONCURRENTLY/ON-name forms 922-929 stay loud).
            918 | 920 => {
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = object_type(view.v(2).ival());
                n.missing_ok = true;
                n.objects = view.v(5).list();
                n.behavior = drop_behavior(view.v(6).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            919 | 921 => {
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = object_type(view.v(2).ival());
                n.objects = view.v(3).list();
                n.behavior = drop_behavior(view.v(4).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // object_type_any_name / object_type_name / drop_type_name /
            // object_type_name_on_any_name constants (943 rides DISPATCH).
            930..=942 => *yyval = YYSTYPE::Ival(OBJECT_TYPE_ANY_NAME[rule - 930] as i32),
            944..=947 => *yyval = YYSTYPE::Ival(OBJECT_TYPE_NAME[rule - 944] as i32),
            948..=955 => *yyval = YYSTYPE::Ival(DROP_TYPE_NAME[rule - 948] as i32),
            956..=958 => *yyval = YYSTYPE::Ival(OBJECT_TYPE_ON_ANY_NAME[rule - 956] as i32),
            // any_name_list / any_name (attrs is 963/964 above).
            959 => {
                let n = Node::mk_list(mcx, view.v(1).list())?;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            960 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, Node::mk_list(mcx, view.v(3).list())?)?;
                *yyval = YYSTYPE::List(list);
            }
            961 => {
                let s = view.v(1).str_val();
                *yyval = YYSTYPE::List(NodeList::make1(mcx, Node::mk_string(mcx, s)?)?);
            }
            962 => {
                let s = view.v(1).str_val();
                let mut list = view.v(2).list();
                list.lcons(mcx, Node::mk_string(mcx, s)?)?;
                *yyval = YYSTYPE::List(list);
            }
            // CreateSchemaStmt (AUTHORIZATION forms 189/191 and non-empty
            // element lists 193 stay unimplemented-rule louds).
            190 | 192 => {
                let (name, elts) = if rule == 190 { (3, 4) } else { (6, 7) };
                let mut n = Node::build::<CreateSchemaStmt>(mcx)?;
                n.schemaname = Some(view.v(name).str_val());
                n.schemaElts = view.v(elts).list();
                n.if_not_exists = rule == 192;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            967 => {
                let mut n = Node::build::<TruncateStmt>(mcx)?;
                n.relations = view.v(3).list();
                n.restart_seqs = view.v(4).boolean();
                n.behavior = drop_behavior(view.v(5).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateFunctionStmt (RETURNS TABLE 1127 stays loud:
            // mergeTableFuncParameters unported).
            1126 | 1128 | 1129 => {
                let mut n = Node::build::<CreateFunctionStmt>(mcx)?;
                n.is_procedure = rule == 1129;
                n.replace = view.v(2).boolean();
                n.funcname = view.v(4).list();
                n.parameters = view.v(5).list();
                let (ret, opts, body) = if rule == 1126 { (Some(7), 8, 9) } else { (None, 6, 7) };
                n.returnType = ret.and_then(|i| view.v(i).node());
                n.options = view.v(opts).list();
                n.sql_body = view.v(body).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1130 => *yyval = YYSTYPE::Boolean(true),
            1131 => *yyval = YYSTYPE::Boolean(false),
            // func_args_with_defaults_list
            1144 => {
                let el = view.v(1).node().expect("func_arg_with_default");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1145 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("func_arg_with_default"))?;
                *yyval = YYSTYPE::List(list);
            }
            // func_arg: [arg_class] [param_name] func_type permutations.
            1146..=1150 => {
                let (mode, name, ty) = match rule {
                    1146 => (Some(1), Some(2), 3),
                    1147 => (Some(2), Some(1), 3),
                    1148 => (None, Some(1), 2),
                    1149 => (Some(1), None, 2),
                    _ => (None, None, 1),
                };
                let mut n = Node::build::<FunctionParameter>(mcx)?;
                n.name = name.map(|i| view.v(i).str_val());
                n.argType = view.v(ty).node();
                n.mode = mode
                    .map(|i| param_mode(view.v(i).ival()))
                    .unwrap_or(FunctionParameterMode::FUNC_PARAM_DEFAULT);
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1151 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_IN as i32),
            1152 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_OUT as i32),
            1153 | 1154 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_INOUT as i32),
            1155 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_VARIADIC as i32),
            1157 => *yyval = view.v(1),
            // func_arg_with_default DEFAULT/'=' a_expr (%TYPE 1159/1160 stay loud).
            1162 | 1163 => {
                let param = view.v(1).node().expect("func_arg");
                let def = view.v(3).node();
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    param
                        .with_mut::<FunctionParameter, _>(|p| p.defexpr = def)
                        .expect("func_arg is FunctionParameter");
                }
                *yyval = YYSTYPE::Node(Some(param));
            }
            // createfunc_opt_list
            1176 => {
                let el = view.v(1).node().expect("createfunc_opt_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1177 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("createfunc_opt_item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // common_func_opt_item
            1178 => {
                *yyval = def_elem(mcx, "strict", Some(Node::mk_boolean(mcx, false)?), view.l(1))?;
            }
            1179 | 1180 => {
                *yyval = def_elem(mcx, "strict", Some(Node::mk_boolean(mcx, true)?), view.l(1))?;
            }
            1181..=1183 => {
                let v = ["immutable", "stable", "volatile"][rule - 1181];
                *yyval = def_elem(mcx, "volatility", Some(Node::mk_string(mcx, v)?), view.l(1))?;
            }
            1184 | 1186 => {
                *yyval = def_elem(mcx, "security", Some(Node::mk_boolean(mcx, true)?), view.l(1))?;
            }
            1185 | 1187 => {
                *yyval = def_elem(mcx, "security", Some(Node::mk_boolean(mcx, false)?), view.l(1))?;
            }
            1188 => {
                *yyval = def_elem(mcx, "leakproof", Some(Node::mk_boolean(mcx, true)?), view.l(1))?;
            }
            1189 => {
                *yyval = def_elem(mcx, "leakproof", Some(Node::mk_boolean(mcx, false)?), view.l(1))?;
            }
            1190 => *yyval = def_elem(mcx, "cost", view.v(2).node(), view.l(1))?,
            1191 => *yyval = def_elem(mcx, "rows", view.v(2).node(), view.l(1))?,
            1192 => {
                let arg = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = def_elem(mcx, "support", Some(arg), view.l(1))?;
            }
            1193 => *yyval = def_elem(mcx, "set", view.v(1).node(), view.l(1))?,
            1194 => {
                let arg = Node::mk_string(mcx, view.v(2).str_val())?;
                *yyval = def_elem(mcx, "parallel", Some(arg), view.l(1))?;
            }
            // createfunc_opt_item (TRANSFORM 1197 stays loud).
            1195 => {
                let arg = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = def_elem(mcx, "as", Some(arg), view.l(1))?;
            }
            1196 => {
                let arg = Node::mk_string(mcx, view.v(2).str_val())?;
                *yyval = def_elem(mcx, "language", Some(arg), view.l(1))?;
            }
            1198 => {
                *yyval = def_elem(mcx, "window", Some(Node::mk_boolean(mcx, true)?), view.l(1))?;
            }
            // func_as
            1200 => {
                let s = Node::mk_string(mcx, view.v(1).str_val())?;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, s)?);
            }
            1201 => {
                let mut list = NodeList::make1(mcx, Node::mk_string(mcx, view.v(1).str_val())?)?;
                list.lappend(mcx, Node::mk_string(mcx, view.v(3).str_val())?)?;
                *yyval = YYSTYPE::List(list);
            }
            // GrantStmt / RevokeStmt; privilege_target rides as a GrantStmt
            // carrier holding (targtype, objtype, objects).
            1027 => {
                let target = view.v(4).node().expect("privilege_target");
                // SAFETY: parser-owned carrier; no derived refs live.
                let (targtype, objtype, objects) = unsafe {
                    target.with_mut::<GrantStmt, _>(|t| {
                        (t.targtype, t.objtype, core::mem::take(&mut t.objects))
                    })
                }
                .expect("privilege_target carrier");
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.is_grant = true;
                n.privileges = view.v(2).list();
                n.targtype = targtype;
                n.objtype = objtype;
                n.objects = objects;
                n.grantees = view.v(6).list();
                n.grant_option = view.v(7).boolean();
                n.grantor = view.v(8).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1028 | 1029 => {
                let (pi, ti, gi, byi, bi) =
                    if rule == 1028 { (2, 4, 6, 7, 8) } else { (5, 7, 9, 10, 11) };
                let target = view.v(ti).node().expect("privilege_target");
                // SAFETY: parser-owned carrier; no derived refs live.
                let (targtype, objtype, objects) = unsafe {
                    target.with_mut::<GrantStmt, _>(|t| {
                        (t.targtype, t.objtype, core::mem::take(&mut t.objects))
                    })
                }
                .expect("privilege_target carrier");
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.is_grant = false;
                n.grant_option = rule == 1029;
                n.privileges = view.v(pi).list();
                n.targtype = targtype;
                n.objtype = objtype;
                n.objects = objects;
                n.grantees = view.v(gi).list();
                n.grantor = view.v(byi).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                n.behavior = drop_behavior(view.v(bi).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // privileges: list | ALL [PRIVILEGES] -> NIL | ALL [(cols)].
            1030 => *yyval = YYSTYPE::List(view.v(1).list()),
            1031 | 1032 => *yyval = YYSTYPE::List(NodeList::nil()),
            1033 | 1034 => {
                let cols = view.v(if rule == 1033 { 3 } else { 4 }).list();
                let mut n = Node::build::<AccessPriv>(mcx)?;
                n.priv_name = None;
                n.cols = cols;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n.seal())?);
            }
            1035 => {
                let n = view.v(1).node().expect("privilege");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            1036 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("privilege"))?;
                *yyval = YYSTYPE::List(list);
            }
            // privilege: SELECT | REFERENCES | CREATE [cols] | ALTER SYSTEM |
            // ColId [cols].
            1037..=1039 | 1041 => {
                let (name, cols) = match rule {
                    1037 => ("select", view.v(2).list()),
                    1038 => ("references", view.v(2).list()),
                    1039 => ("create", view.v(2).list()),
                    _ => (view.v(1).str_val(), view.v(2).list()),
                };
                let mut n = Node::build::<AccessPriv>(mcx)?;
                n.priv_name = Some(name);
                n.cols = cols;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1040 => {
                let mut n = Node::build::<AccessPriv>(mcx)?;
                n.priv_name = Some("alter system");
                n.cols = NodeList::nil();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // privilege_target: [TABLE] qualified_name_list | SEQUENCE ...
            // (other target forms 1049-1066 stay louds).
            1046 | 1047 | 1048 => {
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.targtype = GrantTargetType::ACL_TARGET_OBJECT;
                n.objtype = if rule == 1048 {
                    ObjectType::OBJECT_SEQUENCE
                } else {
                    ObjectType::OBJECT_TABLE
                };
                n.objects = view.v(if rule == 1046 { 1 } else { 2 }).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1067 => {
                let n = view.v(1).node().expect("grantee");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            1068 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("grantee"))?;
                *yyval = YYSTYPE::List(list);
            }
            1069 => *yyval = YYSTYPE::Node(view.v(1).node()),
            1070 => *yyval = YYSTYPE::Node(view.v(2).node()),
            1071 => *yyval = YYSTYPE::Boolean(true),
            1072 => *yyval = YYSTYPE::Boolean(false),
            // opt_granted_by: GRANTED BY RoleSpec | EMPTY.
            1083 => *yyval = YYSTYPE::Node(view.v(3).node()),
            1084 => *yyval = YYSTYPE::Node(None),
            // RoleSpec: NonReservedWord | CURRENT_ROLE | CURRENT_USER |
            // SESSION_USER ("public"/"none" are not keywords).
            2458 => {
                let name = view.v(1).str_val();
                let mut n = Node::build::<RoleSpec>(mcx)?;
                n.location = view.l(1);
                if name == "public" {
                    n.roletype = RoleSpecType::ROLESPEC_PUBLIC;
                } else if name == "none" {
                    return Err(Box::new(
                        (*self.errposition_error(
                            "role name \"none\" is reserved".into(),
                            view.l(1),
                        ))
                        .with_sqlstate(types_error::ERRCODE_RESERVED_NAME),
                    ));
                } else {
                    n.roletype = RoleSpecType::ROLESPEC_CSTRING;
                    n.rolename = Some(name);
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            2459..=2461 => {
                let mut n = Node::build::<RoleSpec>(mcx)?;
                n.location = view.l(1);
                n.roletype = match rule {
                    2459 => RoleSpecType::ROLESPEC_CURRENT_ROLE,
                    2460 => RoleSpecType::ROLESPEC_CURRENT_USER,
                    _ => RoleSpecType::ROLESPEC_SESSION_USER,
                };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            968 | 970 => *yyval = YYSTYPE::Boolean(false),
            969 => *yyval = YYSTYPE::Boolean(true),
            // CommentStmt TABLE/COLUMN arms (object forms 973-988 stay louds).
            971 | 972 => {
                let mut n = Node::build::<CommentStmt>(mcx)?;
                n.objtype = if rule == 972 {
                    ObjectType::OBJECT_COLUMN
                } else {
                    object_type(view.v(3).ival())
                };
                n.object = Some(Node::mk_list(mcx, view.v(4).list())?);
                let c = view.v(6);
                n.comment = if c.is_null_node() { None } else { Some(c.str_val()) };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1876 => {
                let n = view.v(1).node().expect("relation_expr");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            1877 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("relation_expr"))?;
                *yyval = YYSTYPE::List(list);
            }
            // opt_interval single-field / X TO Y masks (values vs datetime.h).
            2003..=2007 | 2009..=2011 | 2013 => {
                let mask = match rule {
                    2003 => IM_YEAR,
                    2004 => IM_MONTH,
                    2005 => IM_DAY,
                    2006 => IM_HOUR,
                    2007 => IM_MINUTE,
                    2009 => IM_YEAR | IM_MONTH,
                    2010 => IM_DAY | IM_HOUR,
                    2011 => IM_DAY | IM_HOUR | IM_MINUTE,
                    _ => IM_HOUR | IM_MINUTE,
                };
                *yyval = YYSTYPE::List(NodeList::make1(
                    mcx,
                    make_int_const(mcx, mask, view.l(1))?,
                )?);
            }
            // X TO interval_second: keep interval_second's precision cell,
            // replace its mask cell (C mutates linitial in place).
            2012 | 2014 | 2015 => {
                let mask = match rule {
                    2012 => IM_DAY | IM_HOUR | IM_MINUTE | IM_SECOND,
                    2014 => IM_HOUR | IM_MINUTE | IM_SECOND,
                    _ => IM_MINUTE | IM_SECOND,
                };
                let sec = view.v(3).list();
                let mut list = NodeList::make1(mcx, make_int_const(mcx, mask, view.l(1))?)?;
                for extra in sec.iter().skip(1) {
                    list.lappend(mcx, extra)?;
                }
                *yyval = YYSTYPE::List(list);
            }
            2017 => {
                *yyval = YYSTYPE::List(NodeList::make1(
                    mcx,
                    make_int_const(mcx, IM_SECOND, view.l(1))?,
                )?);
            }
            2018 => {
                *yyval = YYSTYPE::List(NodeList::make2(
                    mcx,
                    make_int_const(mcx, IM_SECOND, view.l(1))?,
                    make_int_const(mcx, view.v(3).ival(), view.l(3))?,
                )?);
            }
            // SimpleTypename ConstInterval '(' Iconst ')'
            1951 => {
                let t = view.v(1).node().expect("ConstInterval");
                let typmods = NodeList::make2(
                    mcx,
                    make_int_const(mcx, INTERVAL_FULL_RANGE, -1)?,
                    make_int_const(mcx, view.v(3).ival(), view.l(3))?,
                )?;
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.typmods = typmods).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            // AexprConst: ConstInterval Sconst opt_interval
            //           | ConstInterval '(' Iconst ')' Sconst
            2447 | 2448 => {
                let t = view.v(1).node().expect("ConstInterval");
                let (typmods, s, sloc) = if rule == 2447 {
                    (view.v(3).list(), view.v(2).str_val(), view.l(2))
                } else {
                    (
                        NodeList::make2(
                            mcx,
                            make_int_const(mcx, INTERVAL_FULL_RANGE, -1)?,
                            make_int_const(mcx, view.v(3).ival(), view.l(3))?,
                        )?,
                        view.v(5).str_val(),
                        view.l(5),
                    )
                };
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.typmods = typmods).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(make_string_const_cast(mcx, s, sloc, t)?));
            }
            // a_expr: row OVERLAPS row
            2061 => {
                let left = view.v(1).list();
                let right = view.v(3).list();
                if left.len() != 2 {
                    return Err(self.errposition_error(
                        "wrong number of parameters on left side of OVERLAPS expression".into(),
                        view.l(1),
                    ));
                }
                if right.len() != 2 {
                    return Err(self.errposition_error(
                        "wrong number of parameters on right side of OVERLAPS expression".into(),
                        view.l(3),
                    ));
                }
                let mut args = left;
                args.concat(mcx, &right)?;
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "overlaps")?,
                    args,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(2),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // row: '(' expr_list ',' a_expr ')'
            2259 => {
                let mut list = view.v(2).list();
                list.lappend(mcx, view.v(4).node().expect("a_expr"))?;
                *yyval = YYSTYPE::List(list);
            }
            // zone_value: ConstInterval Sconst opt_interval
            //           | ConstInterval '(' Iconst ')' Sconst
            238 | 239 => {
                let t = view.v(1).node().expect("ConstInterval");
                let (typmods, s, sloc) = if rule == 238 {
                    let tl = view.v(3).list();
                    if let Some(first) = tl.first() {
                        let ival = first
                            .as_a_const()
                            .and_then(|c| match c.val {
                                Some(ValUnion::Integer(Integer { ival })) => Some(ival),
                                _ => None,
                            })
                            .expect("opt_interval mask A_Const");
                        if ival & !(IM_HOUR | IM_MINUTE) != 0 {
                            return Err(self.errposition_error(
                                "time zone interval must be HOUR or HOUR TO MINUTE".into(),
                                view.l(3),
                            ));
                        }
                    }
                    (tl, view.v(2).str_val(), view.l(2))
                } else {
                    (
                        NodeList::make2(
                            mcx,
                            make_int_const(mcx, INTERVAL_FULL_RANGE, -1)?,
                            make_int_const(mcx, view.v(3).ival(), view.l(3))?,
                        )?,
                        view.v(5).str_val(),
                        view.l(5),
                    )
                };
                // SAFETY: as rule 8.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| tn.typmods = typmods).expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(make_string_const_cast(mcx, s, sloc, t)?));
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

    // insertSelectOptions.
    fn insert_select_options(
        &self,
        stmt: Node<'mcx>,
        sort_clause: NodeList<'mcx>,
        locking_clause: NodeList<'mcx>,
        limit: Option<&mut SelectLimit<'mcx>>,
        with: Option<Node<'mcx>>,
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
                if l.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
                    for lock_node in &n.lockingClause {
                        let lock = lock_node.as_locking_clause().expect("LockingClause");
                        if lock.waitPolicy == types_nodes::LockWaitPolicy::LockWaitSkip {
                            err = Err(self.errposition_error(
                                "SKIP LOCKED and WITH TIES options cannot be used together"
                                    .into(),
                                l.optionLoc,
                            ));
                            return;
                        }
                    }
                }
                n.limitOption = l.limitOption;
            })
            .expect("SelectStmt");
        }
        err?;
        if let Some(w) = with {
            let mut err: PgResult<()> = Ok(());
            // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
            unsafe {
                stmt.with_mut::<SelectStmt, _>(|n| {
                    if n.withClause.is_some() {
                        err = Err(self.errposition_error(
                            "multiple WITH clauses not allowed".into(),
                            w.as_with_clause().expect("with_clause").location,
                        ));
                        return;
                    }
                    n.withClause = Some(w);
                })
                .expect("SelectStmt");
            }
            err?;
        }
        Ok(())
    }

    // processCASbits (gram.y): C's error surface for misplaced attributes;
    // attributes C supports but this port does not are loud panics.
    fn process_cas_bits(
        &self,
        cas_bits: i32,
        location: i32,
        constr_type: &str,
        t: CasTargets,
    ) -> PgResult<CasBits> {
        let mut out = CasBits {
            deferrable: false,
            initdeferred: false,
            is_enforced: true,
            not_valid: false,
            no_inherit: false,
        };
        let err = |msg: std::string::String| {
            Box::new(
                (*self.errposition_error(msg, location))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            )
        };
        if cas_bits & (CAS_DEFERRABLE | CAS_INITIALLY_DEFERRED) != 0 {
            if !t.deferrable {
                return Err(err(format!(
                    "{constr_type} constraints cannot be marked DEFERRABLE"
                )));
            }
            out.deferrable = true;
        }
        if cas_bits & CAS_INITIALLY_DEFERRED != 0 {
            if !t.initdeferred {
                return Err(err(format!(
                    "{constr_type} constraints cannot be marked DEFERRABLE"
                )));
            }
            out.initdeferred = true;
        }
        if cas_bits & CAS_NOT_VALID != 0 {
            if !t.not_valid {
                return Err(err(format!(
                    "{constr_type} constraints cannot be marked NOT VALID"
                )));
            }
            out.not_valid = true;
        }
        if cas_bits & CAS_NO_INHERIT != 0 {
            if !t.no_inherit {
                return Err(err(format!(
                    "{constr_type} constraints cannot be marked NO INHERIT"
                )));
            }
            out.no_inherit = true;
        }
        if cas_bits & CAS_NOT_ENFORCED != 0 {
            if !t.is_enforced {
                return Err(err(format!(
                    "{constr_type} constraints cannot be marked NOT ENFORCED"
                )));
            }
            out.is_enforced = false;
            if t.not_valid {
                out.not_valid = true;
            }
        }
        if cas_bits & CAS_ENFORCED != 0 {
            if !t.is_enforced {
                return Err(err(format!(
                    "{constr_type} constraints cannot be marked ENFORCED"
                )));
            }
            out.is_enforced = true;
        }
        if out.deferrable || out.initdeferred {
            panic!("gram_core: DEFERRABLE {constr_type} constraints unported");
        }
        if out.not_valid {
            panic!("gram_core: NOT VALID {constr_type} constraints unported");
        }
        if !out.is_enforced {
            panic!("gram_core: NOT ENFORCED {constr_type} constraints unported");
        }
        Ok(out)
    }

    #[cold]
    fn invalid_parameter_error(&self, message: &str, location: i32) -> Box<types_error::PgError> {
        Box::new(
            (*self.errposition_error(message.into(), location))
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        )
    }

    #[cold]
    fn windowing_error(&self, message: &str, location: i32) -> Box<types_error::PgError> {
        Box::new(
            (*self.errposition_error(message.into(), location))
                .with_sqlstate(types_error::ERRCODE_WINDOWING_ERROR),
        )
    }
}

fn override_kind(v: i32) -> OverridingKind {
    match v {
        1 => OverridingKind::OVERRIDING_USER_VALUE,
        2 => OverridingKind::OVERRIDING_SYSTEM_VALUE,
        _ => OverridingKind::OVERRIDING_NOT_SET,
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

fn param_mode(v: i32) -> FunctionParameterMode {
    match v as u8 {
        b'i' => FunctionParameterMode::FUNC_PARAM_IN,
        b'o' => FunctionParameterMode::FUNC_PARAM_OUT,
        b'b' => FunctionParameterMode::FUNC_PARAM_INOUT,
        b'v' => FunctionParameterMode::FUNC_PARAM_VARIADIC,
        b't' => FunctionParameterMode::FUNC_PARAM_TABLE,
        _ => FunctionParameterMode::FUNC_PARAM_DEFAULT,
    }
}

// makeAConst (makefuncs.c): wrap a bare Integer/Float value node.
fn make_a_const<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    v: Node<'mcx>,
    location: i32,
) -> PgResult<YYSTYPE<'mcx>> {
    let val = if let Some(i) = v.as_integer() {
        ValUnion::Integer(Integer { ival: i.ival })
    } else if let Some(f) = v.as_float() {
        ValUnion::Float(Float { fval: f.fval })
    } else {
        panic!("make_a_const: unexpected node type {:?}", v.node_tag())
    };
    Ok(YYSTYPE::Node(Some(Node::mk_a_const(mcx, Some(val), location)?)))
}

fn alter_table_cmd<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    subtype: AlterTableType,
    name: Option<&'mcx str>,
    def: Option<Node<'mcx>>,
) -> PgResult<YYSTYPE<'mcx>> {
    let mut n = Node::build::<AlterTableCmd>(mcx)?;
    n.subtype = subtype;
    n.name = name;
    n.def = def;
    Ok(YYSTYPE::Node(Some(n.seal())))
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


// gram.y declaration order within each object-type production.
static OBJECT_TYPE_ANY_NAME: [ObjectType; 13] = [
    ObjectType::OBJECT_TABLE,
    ObjectType::OBJECT_SEQUENCE,
    ObjectType::OBJECT_VIEW,
    ObjectType::OBJECT_MATVIEW,
    ObjectType::OBJECT_INDEX,
    ObjectType::OBJECT_FOREIGN_TABLE,
    ObjectType::OBJECT_COLLATION,
    ObjectType::OBJECT_CONVERSION,
    ObjectType::OBJECT_STATISTIC_EXT,
    ObjectType::OBJECT_TSPARSER,
    ObjectType::OBJECT_TSDICTIONARY,
    ObjectType::OBJECT_TSTEMPLATE,
    ObjectType::OBJECT_TSCONFIGURATION,
];
static OBJECT_TYPE_NAME: [ObjectType; 4] = [
    ObjectType::OBJECT_DATABASE,
    ObjectType::OBJECT_ROLE,
    ObjectType::OBJECT_SUBSCRIPTION,
    ObjectType::OBJECT_TABLESPACE,
];
static DROP_TYPE_NAME: [ObjectType; 8] = [
    ObjectType::OBJECT_ACCESS_METHOD,
    ObjectType::OBJECT_EVENT_TRIGGER,
    ObjectType::OBJECT_EXTENSION,
    ObjectType::OBJECT_FDW,
    ObjectType::OBJECT_LANGUAGE,
    ObjectType::OBJECT_PUBLICATION,
    ObjectType::OBJECT_SCHEMA,
    ObjectType::OBJECT_FOREIGN_SERVER,
];
static OBJECT_TYPE_ON_ANY_NAME: [ObjectType; 3] =
    [ObjectType::OBJECT_POLICY, ObjectType::OBJECT_RULE, ObjectType::OBJECT_TRIGGER];

fn object_type(v: i32) -> ObjectType {
    [&OBJECT_TYPE_ANY_NAME[..], &OBJECT_TYPE_NAME, &DROP_TYPE_NAME, &OBJECT_TYPE_ON_ANY_NAME]
        .into_iter()
        .flatten()
        .copied()
        .find(|t| *t as i32 == v)
        .unwrap_or_else(|| panic!("invalid ObjectType {v}"))
}

fn drop_behavior(v: i32) -> DropBehavior {
    match v {
        0 => DropBehavior::DROP_RESTRICT,
        1 => DropBehavior::DROP_CASCADE,
        _ => panic!("invalid DropBehavior {v}"),
    }
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

fn view_check_option(v: i32) -> ViewCheckOption {
    match v {
        0 => ViewCheckOption::NO_CHECK_OPTION,
        1 => ViewCheckOption::LOCAL_CHECK_OPTION,
        2 => ViewCheckOption::CASCADED_CHECK_OPTION,
        other => panic!("gram.y: bad ViewCheckOption {other}"),
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

fn join_type_from_ival(v: i32) -> JoinType {
    match v {
        0 => JoinType::JOIN_INNER,
        1 => JoinType::JOIN_LEFT,
        2 => JoinType::JOIN_FULL,
        3 => JoinType::JOIN_RIGHT,
        other => panic!("join_type_from_ival: {other}"),
    }
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

fn lock_strength(v: i32) -> LockClauseStrength {
    match v {
        1 => LockClauseStrength::LCS_FORKEYSHARE,
        2 => LockClauseStrength::LCS_FORSHARE,
        3 => LockClauseStrength::LCS_FORNOKEYUPDATE,
        4 => LockClauseStrength::LCS_FORUPDATE,
        _ => LockClauseStrength::LCS_NONE,
    }
}

fn lock_wait_policy(v: i32) -> LockWaitPolicy {
    match v {
        1 => LockWaitPolicy::LockWaitSkip,
        2 => LockWaitPolicy::LockWaitError,
        _ => LockWaitPolicy::LockWaitBlock,
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

// SystemFuncName (parse_type.h shape): pg_catalog-qualified function name.
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

fn system_func_name<'mcx>(mcx: mcx::Mcx<'mcx>, name: &'mcx str) -> PgResult<NodeList<'mcx>> {
    NodeList::make2(
        mcx,
        Node::mk_string(mcx, "pg_catalog")?,
        Node::mk_string(mcx, name)?,
    )
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

fn reindex_object_type(v: i32) -> ReindexObjectType {
    match v {
        0 => ReindexObjectType::REINDEX_OBJECT_INDEX,
        1 => ReindexObjectType::REINDEX_OBJECT_TABLE,
        2 => ReindexObjectType::REINDEX_OBJECT_SCHEMA,
        3 => ReindexObjectType::REINDEX_OBJECT_SYSTEM,
        4 => ReindexObjectType::REINDEX_OBJECT_DATABASE,
        _ => unreachable!("reindex_target ival {v}"),
    }
}
