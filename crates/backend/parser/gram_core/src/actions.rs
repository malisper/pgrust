use types_core::catalog::{
    ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL, ATTRIBUTE_IDENTITY_ALWAYS,
    ATTRIBUTE_IDENTITY_BY_DEFAULT, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
use types_error::PgResult;
use types_nodes::parsenodes;
use types_nodes::parsenodes::{
    AccessPriv, AlterDefaultPrivilegesStmt, AlterFunctionStmt, AlterOwnerStmt, AlterPolicyStmt,
    AlterRoleSetStmt, AlterRoleStmt, AlterSystemStmt,
    AlterTableCmd, AlterTableMoveAllStmt, AlterTableStmt, AlterTableType,
    CTECycleClause, CTEMaterialize, CTESearchClause, CheckPointStmt, ClosePortalStmt, ClusterStmt,
    CommentStmt, CommonTableExpr,
    CopyStmt, CreateFunctionStmt, CreatePolicyStmt, CreateRoleStmt, CreateSchemaStmt, DeallocateStmt,
    DeclareCursorStmt, DefElem, DefElemAction, DiscardMode, DiscardStmt, DropBehavior,
    DropOwnedStmt, DropRoleStmt, DropStmt, ExecuteStmt, FetchStmt, FunctionParameter,
    FunctionParameterMode, GrantRoleStmt, GrantStmt, GrantTargetType, GroupingSetKind,
    ListenStmt, LoadStmt, LockStmt, NotifyStmt, ObjectType, ObjectWithArgs, PrepareStmt,
    AlterPublicationAction, AlterPublicationStmt, AlterSubscriptionStmt, AlterSubscriptionType,
    CreatePublicationStmt, CreateSubscriptionStmt, DropSubscriptionStmt, PublicationObjSpec,
    PublicationObjSpecType, PublicationTable,
    ReassignOwnedStmt,
    ReindexObjectType, ReindexStmt, RenameStmt, ReplicaIdentityStmt, RoleSpec, RoleSpecType,
    RoleStmtType, SecLabelStmt, SetOperation, TransactionStmt, TransactionStmtKind, TruncateStmt,
    UnlistenStmt, VacuumRelation, VacuumStmt, VariableSetKind, VariableSetStmt,
    VariableShowStmt, WithClause,
    CURSOR_OPT_ASENSITIVE, CURSOR_OPT_BINARY, CURSOR_OPT_FAST_PLAN, CURSOR_OPT_HOLD,
    CURSOR_OPT_INSENSITIVE, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL, FETCH_ALL,
    REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_FULL, REPLICA_IDENTITY_INDEX,
    REPLICA_IDENTITY_NOTHING,
};
use types_nodes::primnodes::{
    CaseExpr, CaseWhen, CoalesceExpr, CollateClause, CurrentOfExpr, GroupingFunc, JoinExpr, MergeSupportFunc, MinMaxExpr, MinMaxOp,
    JsonBehavior, JsonBehaviorType, JsonEncoding, JsonExprOp, JsonFormat, JsonFormatType,
    JsonIsPredicate, JsonReturning, JsonValueExpr, JsonValueType, JsonWrapper,
    OverridingKind, RowExpr,
    SQLValueFunction, SQLValueFunctionOp,
};
use types_nodes::rawnodes::{
    JsonAggConstructor, JsonArgument, JsonArrayAgg, JsonArrayConstructor,
    JsonArrayQueryConstructor, JsonFuncExpr, JsonKeyValue, JsonObjectAgg, JsonObjectConstructor,
    JsonOutput, JsonParseExpr, JsonQuotes, JsonScalarExpr, JsonSerializeExpr,
    JsonTableColumnType,
};

use types_nodes::rawnodes::CreateDomainStmt;
use types_nodes::rawnodes::{AlterExtensionContentsStmt, AlterExtensionStmt, CreateExtensionStmt};
use types_nodes::JoinType;
use types_nodes::rawnodes::A_Expr_Kind::{self, AEXPR_OP};
use types_nodes::primnodes::{CoercionContext, XmlExpr, XmlExprOp, XmlOptionType};
use types_nodes::rawnodes::{RangeTableFunc, RangeTableFuncCol, RangeTableSample};
use types_nodes::rawnodes::{
    AlterTSConfigType, AlterTSConfigurationStmt, AlterTSDictionaryStmt, CompositeTypeStmt,
    AlterEnumStmt, AlterTypeStmt, ColumnDef, Constraint, ConstrType, ConstraintsSetStmt,
    CreateEnumStmt,
    CreateRangeStmt, CreateSeqStmt, CreateStmt, CreateTableAsStmt, CreateTrigStmt, IndexElem,
    IndexStmt,
    IntoClause, OnCommitAction, TriggerTransition,
    FKCONSTR_ACTION_CASCADE, FKCONSTR_ACTION_NOACTION, FKCONSTR_ACTION_RESTRICT,
    FKCONSTR_ACTION_SETDEFAULT, FKCONSTR_ACTION_SETNULL, FKCONSTR_MATCH_FULL,
    FKCONSTR_MATCH_SIMPLE,
    PartitionBoundSpec, PartitionCmd, PartitionElem, PartitionSpec, PartitionStrategy,
    RangeSubselect, RefreshMatViewStmt, ReturningOptionKind, TableLikeClause, ViewCheckOption,
    ViewStmt, WindowDef,
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
    Alias, DefineStmt, DeleteStmt, InsertStmt, Node, NodeList, NodeTag, OptNodeList,
    RangeFunction, RangeVar,
    RawStmt, SelectStmt, UpdateStmt,
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
use crate::yystype::{FuncAliasCols, JoinQualUsing, JsonBehaviors, KeyAction, KeyActions, SelectLimit, TransformElements, YYSTYPE};

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
#[derive(Default)]
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

// TRIGGER_TYPE bits, verified against catalog/pg_trigger.h.
const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;
const TRIGGER_TYPE_AFTER: i16 = 0;

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
            // parse_toplevel: MODE_PLPGSQL_EXPR PLpgSQL_Expr
            //              | MODE_PLPGSQL_ASSIGN{1,2,3} PLAssignStmt
            4..=7 => {
                let stmt = view.v(2).node().expect("plpgsql toplevel node");
                if rule >= 5 {
                    // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                    unsafe {
                        stmt.with_mut::<types_nodes::PLAssignStmt, _>(|n| {
                            n.nnames = rule as i32 - 4;
                        })
                        .expect("PLAssignStmt");
                    }
                }
                self.parsetree =
                    NodeList::make1(mcx, Node::mk_raw_stmt(mcx, Some(stmt), view.l(2), 0)?)?;
            }
            // PLpgSQL_Expr: opt_distinct_clause opt_target_list from_clause
            //   where_clause group_clause having_clause window_clause
            //   opt_sort_clause opt_select_limit opt_for_locking_clause
            2464 => {
                let mut n = Node::build::<SelectStmt>(mcx)?;
                let v = view.v(1);
                if v.is_distinct_all() {
                    n.distinctClause = DistinctClause::All;
                } else {
                    let l = v.list();
                    if !l.is_nil() {
                        n.distinctClause = DistinctClause::On(l);
                    }
                }
                n.targetList = view.v(2).list();
                n.fromClause = view.v(3).list();
                n.whereClause = view.v(4).node();
                let (distinct, list) = view.v(5).group();
                n.groupClause = list;
                n.groupDistinct = distinct;
                n.havingClause = view.v(6).node();
                n.windowClause = view.v(7).list();
                n.sortClause = view.v(8).list();
                if let Some(l) = view.v(9).limit() {
                    n.limitOffset = l.limitOffset;
                    n.limitCount = l.limitCount;
                    if n.sortClause.is_nil()
                        && l.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES
                    {
                        return Err(self.errposition_error(
                            "WITH TIES cannot be specified without ORDER BY clause".into(),
                            l.optionLoc,
                        ));
                    }
                    n.limitOption = l.limitOption;
                }
                n.lockingClause = view.v(10).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PLAssignStmt: plassign_target opt_indirection plassign_equals PLpgSQL_Expr
            2465 => {
                let mut n = Node::build::<types_nodes::PLAssignStmt>(mcx)?;
                n.name = view.v(1).str_val();
                // check_indirection is a no-op: A_Indices construction is an
                // unported loud.
                n.indirection = view.v(2).list();
                n.val = view.v(4).node();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // plassign_target: ColId | PARAM
            2467 => {
                let txt = format!("${}", view.v(1).ival());
                let bytes = mcx::slice_borrow_in(mcx, txt.as_bytes())?;
                *yyval = YYSTYPE::Str(core::str::from_utf8(bytes).expect("ascii"));
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
                let p = Node::mk_param_ref(mcx, number, view.l(1))?;
                *yyval = YYSTYPE::Node(Some(if ind.is_nil() {
                    p
                } else {
                    self.check_indirection(&ind)?;
                    Node::mk(
                        mcx,
                        types_nodes::A_Indirection { arg: Some(p), indirection: ind },
                    )?
                }));
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
                let rv = self.range_var_from_qualified_name(
                    view.v(1).str_val(),
                    view.v(2).list(),
                    view.l(1),
                )?;
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
            // CreateStmt: CREATE OptTemp TABLE [IF_P NOT EXISTS] qualified_name
            // '(' OptTableElementList ')' OptInherit OptPartitionSpec
            // table_access_method_clause OptWith OnCommitOption OptTableSpace
            455 | 456 => {
                let d = if rule == 456 { 3 } else { 0 };
                let persistence = view.v(2).ival() as u8;
                let relation = view.v(4 + d).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let mut n = Node::build::<CreateStmt>(mcx)?;
                n.relation = relation.as_variant::<RangeVar>();
                n.tableElts = view.v(6 + d).list();
                n.inhRelations = view.v(8 + d).list();
                n.partspec = view.v(9 + d).node();
                n.accessMethod = opt_str(view.v(10 + d));
                n.options = view.v(11 + d).list();
                n.oncommit = on_commit_action(view.v(12 + d).ival());
                n.tablespacename = opt_str(view.v(13 + d));
                n.if_not_exists = rule == 456;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateStmt: CREATE OptTemp TABLE [IF_P NOT EXISTS] qualified_name
            // OF any_name OptTypedTableElementList OptPartitionSpec
            // table_access_method_clause OptWith OnCommitOption OptTableSpace
            457 | 458 => {
                let d = if rule == 458 { 3 } else { 0 };
                let persistence = view.v(2).ival() as u8;
                let relation = view.v(4 + d).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let of_typename =
                    make_type_name(mcx, view.v(6 + d).list(), NodeList::nil(), view.l(6 + d))?;
                let mut n = Node::build::<CreateStmt>(mcx)?;
                n.relation = relation.as_variant::<RangeVar>();
                n.tableElts = view.v(7 + d).list();
                n.partspec = view.v(8 + d).node();
                n.ofTypename = Some(of_typename);
                n.accessMethod = opt_str(view.v(9 + d));
                n.options = view.v(10 + d).list();
                n.oncommit = on_commit_action(view.v(11 + d).ival());
                n.tablespacename = opt_str(view.v(12 + d));
                n.if_not_exists = rule == 458;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PartitionBoundSpec: FOR VALUES WITH '(' hash_partbound ')'
            393 => {
                let mut n = Node::build::<PartitionBoundSpec>(mcx)?;
                n.strategy = PartitionStrategy::Hash as u8;
                n.modulus = -1;
                n.remainder = -1;
                let opts = view.v(5).list();
                for opt in opts.iter() {
                    let d = opt.as_def_elem().expect("hash_partbound DefElem");
                    let name = d.defname.expect("hash_partbound defname");
                    match name {
                        "modulus" => {
                            if n.modulus != -1 {
                                return Err(self.errposition_error_code(
                                    types_error::ERRCODE_DUPLICATE_OBJECT,
                                    "modulus for hash partition provided more than once".into(),
                                    d.location,
                                ));
                            }
                            n.modulus = def_get_int32(d);
                        }
                        "remainder" => {
                            if n.remainder != -1 {
                                return Err(self.errposition_error_code(
                                    types_error::ERRCODE_DUPLICATE_OBJECT,
                                    "remainder for hash partition provided more than once".into(),
                                    d.location,
                                ));
                            }
                            n.remainder = def_get_int32(d);
                        }
                        _ => {
                            return Err(self.errposition_error(
                                format!(
                                    "unrecognized hash partition bound specification \"{name}\""
                                ),
                                d.location,
                            ))
                        }
                    }
                }
                if n.modulus == -1 {
                    return Err(self.errposition_error(
                        "modulus for hash partition must be specified".into(),
                        view.l(3),
                    ));
                }
                if n.remainder == -1 {
                    return Err(self.errposition_error(
                        "remainder for hash partition must be specified".into(),
                        view.l(3),
                    ));
                }
                n.location = view.l(3);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // hash_partbound_elem: NonReservedWord Iconst
            397 => {
                let iv = Node::mk_integer(mcx, view.v(2).ival())?;
                *yyval = def_elem(mcx, view.v(1).str_val(), Some(iv), view.l(1))?;
            }
            // hash_partbound: hash_partbound_elem | hash_partbound ',' hash_partbound_elem
            398 => {
                let el = view.v(1).node().expect("hash_partbound_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            399 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("hash_partbound_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
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
            // CreateStmt: CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name
            // PARTITION OF qualified_name OptTypedTableElementList
            // PartitionBoundSpec OptPartitionSpec table_access_method_clause
            // OptWith OnCommitOption OptTableSpace
            460 => {
                let persistence = view.v(2).ival() as u8;
                let relation = view.v(7).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let parent = view.v(10).node().expect("qualified_name");
                let mut n = Node::build::<CreateStmt>(mcx)?;
                n.relation = relation.as_variant::<RangeVar>();
                n.tableElts = view.v(11).list();
                n.inhRelations = NodeList::make1(mcx, parent)?;
                n.partbound = view.v(12).node();
                n.partspec = view.v(13).node();
                n.accessMethod = opt_str(view.v(14));
                n.options = view.v(15).list();
                n.oncommit = on_commit_action(view.v(16).ival());
                n.tablespacename = opt_str(view.v(17));
                n.if_not_exists = true;
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
            // OptTemp (GLOBAL-deprecated variants stay unported).
            461..=464 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_TEMP as i32),
            467 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_UNLOGGED as i32),
            468 => *yyval = YYSTYPE::Ival(RELPERSISTENCE_PERMANENT as i32),
            // ViewStmt: CREATE [OR REPLACE] OptTemp VIEW ...
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
            // ViewStmt: CREATE [OR REPLACE] OptTemp RECURSIVE VIEW name
            // '(' columnList ')' opt_reloptions AS SelectStmt opt_check_option
            1490 | 1491 => {
                let replace = rule == 1491;
                let off = if replace { 2 } else { 0 };
                let persistence = view.v(2 + off).ival() as u8;
                let relation = view.v(5 + off).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    relation
                        .with_mut::<RangeVar, _>(|r| r.relpersistence = persistence)
                        .expect("qualified_name is RangeVar");
                }
                let check = view_check_option(view.v(12 + off).ival());
                if check != ViewCheckOption::NO_CHECK_OPTION {
                    return Err(Box::new(
                        (*self.errposition_error(
                            "WITH CHECK OPTION not supported on recursive views".into(),
                            view.l(12 + off),
                        ))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                let rv = relation.as_variant::<RangeVar>().expect("qualified_name is RangeVar");
                let mut n = Node::build::<ViewStmt>(mcx)?;
                n.view = Some(rv);
                n.aliases = view.v(7 + off).list();
                n.query = Some(make_recursive_view_select(
                    mcx,
                    rv.relname.expect("view relname"),
                    view.v(7 + off).list(),
                    view.v(11 + off).node(),
                )?);
                n.replace = replace;
                n.options = view.v(9 + off).list();
                n.withCheckOption = check;
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
            // TypedTableElementList: TypedTableElement [',' TypedTableElement]
            475 => {
                let el = view.v(1).node().expect("TypedTableElement");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            476 => {
                let mut list = view.v(1).list();
                let el = view.v(3).node().expect("TypedTableElement");
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
                let mut coll_clause: Option<Node<'_>> = None;
                for q in quals.iter() {
                    match q.node_tag() {
                        NodeTag::T_Constraint => constraints.lappend(mcx, q)?,
                        NodeTag::T_CollateClause => {
                            if coll_clause.is_some() {
                                return Err(self.errposition_error(
                                    "multiple COLLATE clauses not allowed".into(),
                                    q.as_collate_clause().unwrap().location,
                                ));
                            }
                            coll_clause = Some(q);
                        }
                        other => panic!("unexpected node type {other:?} in ColQualList"),
                    }
                }
                let mut n = Node::build::<ColumnDef>(mcx)?;
                n.collClause = coll_clause;
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
            // columnOptions: ColId ColQualList | ColId WITH OPTIONS ColQualList
            483 | 484 => {
                let quals = view.v(if rule == 484 { 4 } else { 2 }).list();
                // SplitColQualList: COLLATE splits out; Constraints stay.
                let mut constraints = NodeList::nil();
                let mut coll_clause: Option<Node<'_>> = None;
                for q in quals.iter() {
                    match q.node_tag() {
                        NodeTag::T_Constraint => constraints.lappend(mcx, q)?,
                        NodeTag::T_CollateClause => {
                            if coll_clause.is_some() {
                                return Err(self.errposition_error(
                                    "multiple COLLATE clauses not allowed".into(),
                                    q.as_collate_clause().unwrap().location,
                                ));
                            }
                            coll_clause = Some(q);
                        }
                        other => panic!("unexpected node type {other:?} in ColQualList"),
                    }
                }
                let mut n = Node::build::<ColumnDef>(mcx)?;
                n.collClause = coll_clause;
                n.colname = Some(view.v(1).str_val());
                n.is_local = true;
                n.constraints = constraints;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ColConstraintElem: COLLATE any_name
            498 => {
                let collname = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    CollateClause { arg: None, collname, location: view.l(1) },
                )?));
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
            // ConstraintAttr: DEFERRABLE | NOT DEFERRABLE | INITIALLY DEFERRED
            //   | INITIALLY IMMEDIATE | ENFORCED | NOT ENFORCED
            516..=521 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = match rule {
                    516 => ConstrType::CONSTR_ATTR_DEFERRABLE,
                    517 => ConstrType::CONSTR_ATTR_NOT_DEFERRABLE,
                    518 => ConstrType::CONSTR_ATTR_DEFERRED,
                    519 => ConstrType::CONSTR_ATTR_IMMEDIATE,
                    520 => ConstrType::CONSTR_ATTR_ENFORCED,
                    _ => ConstrType::CONSTR_ATTR_NOT_ENFORCED,
                };
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
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
                let mut coll_clause: Option<Node<'_>> = None;
                for q in view.v(6).list().iter() {
                    match q.node_tag() {
                        NodeTag::T_Constraint => constraints.lappend(mcx, q)?,
                        NodeTag::T_CollateClause => {
                            if coll_clause.is_some() {
                                return Err(self.errposition_error(
                                    "multiple COLLATE clauses not allowed".into(),
                                    q.as_collate_clause().unwrap().location,
                                ));
                            }
                            coll_clause = Some(q);
                        }
                        other => panic!("unexpected node type {other:?} in ColQualList"),
                    }
                }
                let mut n = Node::build::<CreateDomainStmt>(mcx)?;
                n.domainname = view.v(3).list();
                n.typeName = view.v(5).node();
                n.collClause = coll_clause;
                n.constraints = constraints;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterDomainStmt: ALTER DOMAIN_P any_name ...
            1530 => {
                let mut n = Node::build::<parsenodes::AlterDomainStmt>(mcx)?;
                n.subtype = b'T';
                n.typeName = view.v(3).list();
                n.def = view.v(4).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1531 | 1532 => {
                let mut n = Node::build::<parsenodes::AlterDomainStmt>(mcx)?;
                n.subtype = if rule == 1531 { b'N' } else { b'O' };
                n.typeName = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1533 => {
                let mut n = Node::build::<parsenodes::AlterDomainStmt>(mcx)?;
                n.subtype = b'C';
                n.typeName = view.v(3).list();
                n.def = view.v(5).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1534 | 1535 => {
                let (ni, bi) = if rule == 1534 { (6, 7) } else { (8, 9) };
                let mut n = Node::build::<parsenodes::AlterDomainStmt>(mcx)?;
                n.subtype = b'X';
                n.typeName = view.v(3).list();
                n.name = Some(view.v(ni).str_val());
                n.behavior = drop_behavior(view.v(bi).ival());
                n.missing_ok = rule == 1535;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1536 => {
                let mut n = Node::build::<parsenodes::AlterDomainStmt>(mcx)?;
                n.subtype = b'V';
                n.typeName = view.v(3).list();
                n.name = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DomainConstraint: CONSTRAINT name DomainConstraintElem
            546 => {
                let name = view.v(2).str_val();
                let node = view.v(3).node().expect("DomainConstraintElem");
                let loc = view.l(1);
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    node.with_mut::<Constraint, _>(|c| {
                        c.conname = Some(name);
                        c.location = loc;
                    })
                    .expect("DomainConstraintElem is Constraint");
                }
                *yyval = YYSTYPE::Node(Some(node));
            }
            547 => *yyval = YYSTYPE::Node(view.v(1).node()),
            // DomainConstraintElem: CHECK '(' a_expr ')' ConstraintAttributeSpec
            548 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_CHECK;
                n.location = view.l(1);
                n.raw_expr = view.v(3).node();
                n.cooked_expr = Option::None;
                let cas = self.process_cas_bits(
                    view.v(5).ival(),
                    view.l(5),
                    "CHECK",
                    CasTargets { not_valid: true, no_inherit: true, ..Default::default() },
                )?;
                n.skip_validation = cas.not_valid;
                n.is_no_inherit = cas.no_inherit;
                n.is_enforced = true;
                n.initially_valid = !n.skip_validation;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DomainConstraintElem: NOT NULL_P ConstraintAttributeSpec
            549 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_NOTNULL;
                n.location = view.l(1);
                n.keys = NodeList::make1(mcx, Node::mk_string(mcx, "value")?)?;
                self.process_cas_bits(
                    view.v(3).ival(),
                    view.l(3),
                    "NOT NULL",
                    CasTargets { deferrable: false, initdeferred: false, is_enforced: false, not_valid: false, no_inherit: false },
                )?;
                n.initially_valid = true;
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
            // ConstraintElem: NOT NULL_P ColId ConstraintAttributeSpec
            539 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_NOTNULL;
                n.location = view.l(1);
                n.keys = NodeList::make1(mcx, Node::mk_string(mcx, view.v(3).str_val())?)?;
                let cas = self.process_cas_bits(
                    view.v(4).ival(),
                    view.l(4),
                    "NOT NULL",
                    CasTargets { deferrable: false, initdeferred: false, is_enforced: false, not_valid: true, no_inherit: true },
                )?;
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
                n.without_overlaps = view.v(5).boolean();
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
            // ConstraintElem: UNIQUE ExistingIndex ConstraintAttributeSpec
            541 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_UNIQUE;
                n.location = view.l(1);
                n.keys = NodeList::nil();
                n.including = NodeList::nil();
                n.options = NodeList::nil();
                n.indexname = Some(view.v(2).str_val());
                n.indexspace = None;
                let cas = self.process_cas_bits(
                    view.v(3).ival(),
                    view.l(3),
                    "UNIQUE",
                    CasTargets { deferrable: true, initdeferred: true, is_enforced: false, not_valid: false, no_inherit: false },
                )?;
                n.deferrable = cas.deferrable;
                n.initdeferred = cas.initdeferred;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ConstraintElem: PRIMARY KEY ExistingIndex ConstraintAttributeSpec
            543 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_PRIMARY;
                n.location = view.l(1);
                n.keys = NodeList::nil();
                n.including = NodeList::nil();
                n.options = NodeList::nil();
                n.indexname = Some(view.v(3).str_val());
                n.indexspace = None;
                let cas = self.process_cas_bits(
                    view.v(4).ival(),
                    view.l(4),
                    "PRIMARY KEY",
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
                n.without_overlaps = view.v(5).boolean();
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
            // ConstraintElem: EXCLUDE access_method_clause '('
            // ExclusionConstraintList ')' opt_c_include opt_definition
            // OptConsTableSpace OptWhereClause ConstraintAttributeSpec
            544 => {
                let mut n = Node::build::<Constraint>(mcx)?;
                n.contype = ConstrType::CONSTR_EXCLUSION;
                n.location = view.l(1);
                n.access_method = Some(view.v(2).str_val());
                n.exclusions = view.v(4).list();
                n.including = view.v(6).list();
                n.options = view.v(7).list();
                n.indexname = Option::None;
                n.indexspace = opt_str(view.v(8));
                n.where_clause = view.v(9).node();
                let cas = self.process_cas_bits(
                    view.v(10).ival(),
                    view.l(10),
                    "EXCLUDE",
                    CasTargets { deferrable: true, initdeferred: true, is_enforced: false, not_valid: false, no_inherit: false },
                )?;
                n.deferrable = cas.deferrable;
                n.initdeferred = cas.initdeferred;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ExclusionConstraintList; ExclusionConstraintElem:
            // index_elem WITH any_operator | index_elem WITH OPERATOR '(' any_operator ')'
            569 => {
                let el = view.v(1).node().expect("ExclusionConstraintElem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            570 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("ExclusionConstraintElem"))?;
                *yyval = YYSTYPE::List(list);
            }
            571 | 572 => {
                let opno = if rule == 571 { 3 } else { 5 };
                let elem = view.v(1).node().expect("index_elem");
                let op = Node::mk_list(mcx, view.v(opno).list())?;
                *yyval = YYSTYPE::Node(Some(Node::mk_list(
                    mcx,
                    NodeList::make2(mcx, elem, op)?,
                )?));
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
                if let Some(period) = view.v(5).node() {
                    n.fk_attrs.lappend(mcx, period)?;
                    n.fk_with_period = true;
                }
                // opt_column_and_period_list: C's list_make2(cols, period) is
                // encoded as [mk_list(cols)] or [mk_list(cols), period]
                // (rule 560); nil = the EMPTY production.
                let pk_pair = view.v(9).list();
                if !pk_pair.is_nil() {
                    let cols = pk_pair.nth(0).as_list().expect("column list");
                    let mut pk_attrs = NodeList::nil();
                    for c in cols.iter() {
                        pk_attrs.lappend(mcx, c)?;
                    }
                    if pk_pair.len() == 2 {
                        pk_attrs.lappend(mcx, pk_pair.nth(1))?;
                        n.pk_with_period = true;
                    }
                    n.pk_attrs = pk_attrs;
                }
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
            // C: list_make2($2, $3). NULL can't sit in a NodeList, so the
            // pair is [mk_list(cols)] or [mk_list(cols), period]; the EMPTY
            // production's list_make2(NIL, NULL) is the nil list. Consumed
            // only by rule 545.
            560 => {
                let cols = Node::mk_list(mcx, view.v(2).list())?;
                let list = match view.v(3).node() {
                    Some(period) => NodeList::make2(mcx, cols, period)?,
                    None => NodeList::make1(mcx, cols)?,
                };
                *yyval = YYSTYPE::List(list);
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
            // CreateEventTrigStmt: CREATE EVENT TRIGGER name ON ColLabel
            // [WHEN event_trigger_when_list] EXECUTE F_or_P func_name '(' ')'
            835 | 836 => {
                let mut n = Node::build::<types_nodes::parsenodes::CreateEventTrigStmt>(mcx)?;
                n.trigname = Some(view.v(4).str_val());
                n.eventname = Some(view.v(6).str_val());
                if rule == 836 {
                    n.whenclause = view.v(8).list();
                    n.funcname = view.v(11).list();
                } else {
                    n.funcname = view.v(9).list();
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // event_trigger_when_list
            837 => {
                let item = view.v(1).node().expect("event_trigger_when_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, item)?);
            }
            838 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("event_trigger_when_item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // event_trigger_when_item: ColId IN '(' event_trigger_value_list ')'
            839 => {
                let vals = Node::mk_list(mcx, view.v(4).list())?;
                *yyval = def_elem(mcx, view.v(1).str_val(), Some(vals), view.l(1))?;
            }
            // event_trigger_value_list
            840 => {
                let s = Node::mk_string(mcx, view.v(1).str_val())?;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, s)?);
            }
            841 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, Node::mk_string(mcx, view.v(3).str_val())?)?;
                *yyval = YYSTYPE::List(list);
            }
            // AlterEventTrigStmt: ALTER EVENT TRIGGER name enable_trigger
            842 => {
                let mut n = Node::build::<types_nodes::parsenodes::AlterEventTrigStmt>(mcx)?;
                n.trigname = Some(view.v(4).str_val());
                n.tgenabled = view.v(5).ival() as i8;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // enable_trigger: ENABLE | ENABLE REPLICA | ENABLE ALWAYS | DISABLE
            843 => *yyval = YYSTYPE::Ival(b'O' as i32),
            844 => *yyval = YYSTYPE::Ival(b'R' as i32),
            845 => *yyval = YYSTYPE::Ival(b'A' as i32),
            846 => *yyval = YYSTYPE::Ival(b'D' as i32),
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
            // reloption_elem: ColLabel '.' ColLabel ['=' def_arg]
            // (makeDefElemExtended: defnamespace carries the qualifier)
            381 | 382 => {
                let arg = if rule == 381 { view.v(5).node() } else { Option::None };
                let n = Node::mk(
                    mcx,
                    DefElem {
                        defnamespace: Some(view.v(1).str_val()),
                        defname: Some(view.v(3).str_val()),
                        arg,
                        defaction: DefElemAction::DEFELEM_UNSPEC,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
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
            // AlterStatsStmt: ALTER STATISTICS [IF_P EXISTS] any_name SET
            // STATISTICS set_statistics_value
            618 | 619 => {
                let (names, target) =
                    if rule == 618 { (view.v(3), view.v(6)) } else { (view.v(5), view.v(8)) };
                let mut n = Node::build::<types_nodes::rawnodes::AlterStatsStmt>(mcx)?;
                n.defnames = names.list();
                n.stxstattarget = target.node();
                n.missing_ok = rule == 619;
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
            // AlterSeqStmt: ALTER SEQUENCE [IF_P EXISTS] qualified_name SeqOptList
            634 | 635 => {
                let (rv, opts) =
                    if rule == 634 { (view.v(3), view.v(4)) } else { (view.v(5), view.v(6)) };
                let mut n = Node::build::<types_nodes::AlterSeqStmt>(mcx)?;
                n.sequence =
                    rv.node().expect("qualified_name").as_variant::<RangeVar>();
                n.options = opts.list();
                n.missing_ok = rule == 635;
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
            // CreateTableSpaceStmt: CREATE TABLESPACE name OptTableSpaceOwner
            // LOCATION Sconst opt_reloptions
            680 => {
                let mut n = Node::build::<parsenodes::CreateTableSpaceStmt>(mcx)?;
                n.tablespacename = Some(view.v(3).str_val());
                n.owner = view.v(4).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                n.location = Some(view.v(6).str_val());
                n.options = view.v(7).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OptTableSpaceOwner: OWNER RoleSpec | empty.
            681 => *yyval = YYSTYPE::Node(view.v(2).node()),
            682 => *yyval = YYSTYPE::Node(None),
            // DropTableSpaceStmt: DROP TABLESPACE [IF EXISTS] name
            683 | 684 => {
                let mut n = Node::build::<parsenodes::DropTableSpaceStmt>(mcx)?;
                n.tablespacename = Some(view.v(if rule == 683 { 3 } else { 5 }).str_val());
                n.missing_ok = rule == 684;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTblSpcStmt: ALTER TABLESPACE name SET/RESET reloptions
            1272 | 1273 => {
                let mut n = Node::build::<parsenodes::AlterTableSpaceOptionsStmt>(mcx)?;
                n.tablespacename = Some(view.v(3).str_val());
                n.options = view.v(5).list();
                n.isReset = rule == 1273;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateExtensionStmt: CREATE EXTENSION [IF NOT EXISTS] name
            // opt_with create_extension_opt_list
            685 | 686 => {
                let (name_i, opts_i) = if rule == 685 { (3, 5) } else { (6, 8) };
                let mut n = Node::build::<CreateExtensionStmt>(mcx)?;
                n.extname = Some(view.v(name_i).str_val());
                n.if_not_exists = rule == 686;
                n.options = view.v(opts_i).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            687 | 694 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("extension opt item"))?;
                *yyval = YYSTYPE::List(list);
            }
            688 | 695 => *yyval = YYSTYPE::List(NodeList::nil()),
            689 => {
                let arg = Node::mk_string(mcx, view.v(2).str_val())?;
                *yyval = def_elem(mcx, "schema", Some(arg), view.l(1))?;
            }
            690 | 696 => {
                let arg = Node::mk_string(mcx, view.v(2).str_val())?;
                *yyval = def_elem(mcx, "new_version", Some(arg), view.l(1))?;
            }
            691 => {
                return Err(Box::new(
                    (*self.errposition_error(
                        "CREATE EXTENSION ... FROM is no longer supported".into(),
                        view.l(1),
                    ))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            692 => {
                let arg = Node::mk(mcx, Boolean { boolval: true })?;
                *yyval = def_elem(mcx, "cascade", Some(arg), view.l(1))?;
            }
            // AlterExtensionStmt: ALTER EXTENSION name UPDATE alter_extension_opt_list
            693 => {
                let mut n = Node::build::<AlterExtensionStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.options = view.v(5).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterExtensionContentsStmt: ALTER EXTENSION name add_drop
            // {object_type_name name | object_type_any_name any_name |
            //  FUNCTION function_with_argtypes} (all 13 gram.y forms ported).
            697 => {
                let mut n = Node::build::<AlterExtensionContentsStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.action = view.v(4).ival();
                n.objtype = object_type(view.v(5).ival());
                n.object = Some(Node::mk_string(mcx, view.v(6).str_val())?);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            698 => {
                let mut n = Node::build::<AlterExtensionContentsStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.action = view.v(4).ival();
                n.objtype = object_type(view.v(5).ival());
                n.object = Some(Node::mk_list(mcx, view.v(6).list())?);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            702 | 706 | 707 => {
                let mut n = Node::build::<AlterExtensionContentsStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.action = view.v(4).ival();
                n.objtype = match rule {
                    702 => ObjectType::OBJECT_FUNCTION,
                    706 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.object = view.v(6).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ... add_drop {AGGREGATE aggregate_with_argtypes | DOMAIN_P
            // Typename | OPERATOR operator_with_argtypes | TYPE_P Typename}:
            // object is the $6 node directly.
            699 | 701 | 703 | 709 => {
                let mut n = Node::build::<AlterExtensionContentsStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.action = view.v(4).ival();
                n.objtype = match rule {
                    699 => ObjectType::OBJECT_AGGREGATE,
                    701 => ObjectType::OBJECT_DOMAIN,
                    703 => ObjectType::OBJECT_OPERATOR,
                    _ => ObjectType::OBJECT_TYPE,
                };
                n.object = view.v(6).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ... add_drop CAST '(' Typename AS Typename ')':
            // C: object = list_make2($7, $9).
            700 => {
                let mut n = Node::build::<AlterExtensionContentsStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.action = view.v(4).ival();
                n.objtype = ObjectType::OBJECT_CAST;
                n.object = Some(Node::mk_list(
                    mcx,
                    NodeList::make2(
                        mcx,
                        view.v(7).node().expect("Typename"),
                        view.v(9).node().expect("Typename"),
                    )?,
                )?);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ... add_drop OPERATOR {CLASS|FAMILY} any_name USING name:
            // C: object = lcons(makeString($9), $7).
            704 | 705 => {
                let mut n = Node::build::<AlterExtensionContentsStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.action = view.v(4).ival();
                n.objtype = if rule == 704 {
                    ObjectType::OBJECT_OPCLASS
                } else {
                    ObjectType::OBJECT_OPFAMILY
                };
                let mut names = view.v(7).list();
                names.lcons(mcx, Node::mk_string(mcx, view.v(9).str_val())?)?;
                n.object = Some(Node::mk_list(mcx, names)?);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ... add_drop TRANSFORM FOR Typename LANGUAGE name:
            // C: object = list_make2($7, makeString($9)).
            708 => {
                let mut n = Node::build::<AlterExtensionContentsStmt>(mcx)?;
                n.extname = Some(view.v(3).str_val());
                n.action = view.v(4).ival();
                n.objtype = ObjectType::OBJECT_TRANSFORM;
                n.object = Some(Node::mk_list(
                    mcx,
                    NodeList::make2(
                        mcx,
                        view.v(7).node().expect("Typename"),
                        Node::mk_string(mcx, view.v(9).str_val())?,
                    )?,
                )?);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
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
                n.search_clause = view.v(8).node();
                n.cycle_clause = view.v(9).node();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_search_clause: SEARCH DEPTH|BREADTH FIRST_P BY columnList SET ColId
            1735 | 1736 => {
                let mut n = Node::build::<CTESearchClause>(mcx)?;
                n.search_col_list = view.v(5).list();
                n.search_breadth_first = rule == 1736;
                n.search_seq_column = Some(view.v(7).str_val());
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_cycle_clause: CYCLE columnList SET ColId [TO AexprConst
            // DEFAULT AexprConst] USING ColId
            1738 => {
                let mut n = Node::build::<CTECycleClause>(mcx)?;
                n.cycle_col_list = view.v(2).list();
                n.cycle_mark_column = Some(view.v(4).str_val());
                n.cycle_mark_value = view.v(6).node();
                n.cycle_mark_default = view.v(8).node();
                n.cycle_path_column = Some(view.v(10).str_val());
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1739 => {
                let mut n = Node::build::<CTECycleClause>(mcx)?;
                n.cycle_col_list = view.v(2).list();
                n.cycle_mark_column = Some(view.v(4).str_val());
                n.cycle_mark_value = Some(Node::mk_a_const(
                    mcx,
                    Some(ValUnion::Boolean(Boolean { boolval: true })),
                    -1,
                )?);
                n.cycle_mark_default = Some(Node::mk_a_const(
                    mcx,
                    Some(ValUnion::Boolean(Boolean { boolval: false })),
                    -1,
                )?);
                n.cycle_path_column = Some(view.v(6).str_val());
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
            // returning_options: returning_option [, ...]
            1640 => {
                *yyval = YYSTYPE::List(NodeList::make1(
                    mcx,
                    view.v(1).node().expect("returning_option"),
                )?)
            }
            1641 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("returning_option"))?;
                *yyval = YYSTYPE::List(list);
            }
            // returning_option: returning_option_kind AS ColId
            1642 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::rawnodes::ReturningOption {
                        option: if view.v(1).ival()
                            == ReturningOptionKind::RETURNING_OPTION_NEW as i32
                        {
                            ReturningOptionKind::RETURNING_OPTION_NEW
                        } else {
                            ReturningOptionKind::RETURNING_OPTION_OLD
                        },
                        value: Some(view.v(3).str_val()),
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1643 => {
                *yyval = YYSTYPE::Ival(ReturningOptionKind::RETURNING_OPTION_OLD as i32)
            }
            1644 => {
                *yyval = YYSTYPE::Ival(ReturningOptionKind::RETURNING_OPTION_NEW as i32)
            }
            // returning_clause: RETURNING returning_with_clause target_list;
            // WITH(...) options parse here; the analyze leg stays loud
            // (transformReturningClause, returning/rules lane).
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
            1668 => {
                let targets = view.v(2).list();
                let source = view.v(5).node();
                let ncolumns = targets.len() as i32;
                for (i, col) in targets.iter().enumerate() {
                    let r = Node::mk(
                        mcx,
                        types_nodes::rawnodes::MultiAssignRef {
                            source,
                            colno: i as i32 + 1,
                            ncolumns,
                        },
                    )?;
                    // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                    unsafe {
                        col.with_mut::<types_nodes::ResTarget, _>(|rt| rt.val = Some(r))
                            .expect("set_target is ResTarget");
                    }
                }
                *yyval = YYSTYPE::List(targets);
            }
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
            // MergeStmt: opt_with_clause MERGE INTO relation_expr_opt_alias
            //            USING table_ref ON a_expr merge_when_list
            //            returning_clause
            1672 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::MergeStmt {
                        withClause: view.v(1).node(),
                        relation: view.v(4).node(),
                        sourceRelation: view.v(6).node(),
                        joinCondition: view.v(8).node(),
                        mergeWhenClauses: view.v(9).list(),
                        returningClause: view.v(10).node(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1673 => {
                let t = view.v(1).node().expect("merge_when_clause");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            1674 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("merge_when_clause"))?;
                *yyval = YYSTYPE::List(list);
            }
            // merge_when_clause: the merge_update/merge_delete/merge_insert
            // sub-rule built the MergeWhenClause node.
            1675 | 1676 | 1677 => {
                let m = view.v(4).node().expect("merge action");
                let kind = merge_match_kind(view.v(1).ival());
                let cond = view.v(2).node();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    m.with_mut::<types_nodes::MergeWhenClause, _>(|w| {
                        w.matchKind = kind;
                        w.condition = cond;
                    })
                }
                .expect("merge action is MergeWhenClause");
                *yyval = YYSTYPE::Node(Some(m));
            }
            1678 | 1679 => {
                let mut n = Node::build::<types_nodes::MergeWhenClause>(mcx)?;
                n.matchKind = merge_match_kind(view.v(1).ival());
                n.commandType = types_nodes::CmdType::CMD_NOTHING;
                n.condition = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1680 => {
                *yyval =
                    YYSTYPE::Ival(types_nodes::MergeMatchKind::MERGE_WHEN_MATCHED as i32)
            }
            1681 => {
                *yyval = YYSTYPE::Ival(
                    types_nodes::MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE as i32,
                )
            }
            1682 | 1683 => {
                *yyval = YYSTYPE::Ival(
                    types_nodes::MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET as i32,
                )
            }
            // opt_merge_when_condition: AND a_expr | empty
            1684 => *yyval = YYSTYPE::Node(view.v(2).node()),
            1685 => *yyval = YYSTYPE::Node(None),
            // merge_update: UPDATE SET set_clause_list
            1686 => {
                let mut n = Node::build::<types_nodes::MergeWhenClause>(mcx)?;
                n.commandType = types_nodes::CmdType::CMD_UPDATE;
                n.targetList = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1687 => {
                let mut n = Node::build::<types_nodes::MergeWhenClause>(mcx)?;
                n.commandType = types_nodes::CmdType::CMD_DELETE;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // merge_insert: values / OVERRIDING / column-list / DEFAULT VALUES
            1688 | 1689 | 1690 | 1691 | 1692 => {
                let mut n = Node::build::<types_nodes::MergeWhenClause>(mcx)?;
                n.commandType = types_nodes::CmdType::CMD_INSERT;
                match rule {
                    1688 => n.values = view.v(2).list(),
                    1689 => {
                        n.r#override = override_kind(view.v(3).ival());
                        n.values = view.v(5).list();
                    }
                    1690 => {
                        n.targetList = view.v(3).list();
                        n.values = view.v(5).list();
                    }
                    1691 => {
                        n.targetList = view.v(3).list();
                        n.r#override = override_kind(view.v(6).ival());
                        n.values = view.v(8).list();
                    }
                    _ => {}
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // merge_values_clause: VALUES '(' expr_list ')'
            1693 => *yyval = YYSTYPE::List(view.v(3).list()),
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
            1896 => {
                // cvarno is filled in by parse analysis.
                let n = Node::mk(
                    mcx,
                    CurrentOfExpr {
                        cvarno: 0,
                        cursor_name: Some(view.v(4).str_val()),
                        cursor_param: 0,
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
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
            1858 => {
                let alias = view.v(1).alias();
                *yyval = YYSTYPE::FuncAlias(alias, NodeList::nil());
            }
            1859..=1861 => {
                let (alias, cols) = match rule {
                    1859 => (None, view.v(3).list()),
                    1860 | 1861 => {
                        let off = if rule == 1860 { 1 } else { 0 };
                        let a = Node::mk_mut(
                            mcx,
                            Alias {
                                aliasname: Some(view.v(1 + off).str_val()),
                                colnames: NodeList::nil(),
                            },
                        )?;
                        (Some(a.seal_ref()), view.v(3 + off).list())
                    }
                    _ => unreachable!(),
                };
                let c = mcx::leak_in(mcx::alloc_in(mcx, FuncAliasCols { alias, coldeflist: cols })?);
                *yyval = YYSTYPE::FuncAliasV(c);
            }
            1862 => {
                *yyval = YYSTYPE::FuncAlias(None, NodeList::nil());
            }
            // func_table: functions cells are C's (funcexpr, coldeflist) 2-lists.
            1884 => {
                let fexpr = view.v(1).node().expect("func_expr_windowless");
                let ordinality = view.v(2).boolean();
                let mut pair = NodeList::make1(mcx, fexpr)?;
                pair.lappend(mcx, Node::mk_list(mcx, NodeList::nil())?)?;
                let mut n = Node::build::<RangeFunction>(mcx)?;
                n.ordinality = ordinality;
                n.functions = NodeList::make1(mcx, Node::mk_list(mcx, pair)?)?;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // func_table: ROWS FROM '(' rowsfrom_list ')' opt_ordinality
            1885 => {
                let mut n = Node::build::<RangeFunction>(mcx)?;
                n.ordinality = view.v(6).boolean();
                n.is_rowsfrom = true;
                n.functions = view.v(4).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // rowsfrom_item: func_expr_windowless opt_col_def_list
            1886 => {
                let mut pair =
                    NodeList::make1(mcx, view.v(1).node().expect("func_expr_windowless"))?;
                pair.lappend(mcx, Node::mk_list(mcx, view.v(2).list())?)?;
                *yyval = YYSTYPE::List(pair);
            }
            1887 => {
                *yyval =
                    YYSTYPE::List(NodeList::make1(mcx, Node::mk_list(mcx, view.v(1).list())?)?);
            }
            1888 => {
                let mut l = view.v(1).list();
                l.lappend(mcx, Node::mk_list(mcx, view.v(3).list())?)?;
                *yyval = YYSTYPE::List(l);
            }
            1891 => *yyval = YYSTYPE::Boolean(true),
            1892 => *yyval = YYSTYPE::Boolean(false),
            // table_ref: xmltable opt_alias_clause | LATERAL_P xmltable opt_alias_clause
            1836 | 1837 => {
                let off = if rule == 1837 { 1 } else { 0 };
                let n = view.v(1 + off).node().expect("xmltable");
                let alias = view.v(2 + off).alias();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    n.with_mut::<RangeTableFunc, _>(|t| {
                        t.lateral = rule == 1837;
                        t.alias = alias;
                    })
                    .expect("xmltable is RangeTableFunc");
                }
                *yyval = YYSTYPE::Node(Some(n));
            }
            1900 | 1905 | 1917 | 2206 => {
                *yyval = YYSTYPE::List(NodeList::make1(mcx, view.v(1).node().expect("el"))?)
            }
            1901 | 1906 | 1918 | 2207 => {
                let mut l = view.v(1).list();
                l.lappend(mcx, view.v(3).node().expect("el"))?;
                *yyval = YYSTYPE::List(l);
            }
            // TableFuncElement: ColId Typename opt_collate_clause
            1902 => {
                let mut n = Node::build::<ColumnDef>(mcx)?;
                n.colname = Some(view.v(1).str_val());
                n.typeName = view.v(2).node();
                n.is_local = true;
                n.collClause = view.v(3).node();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1903 | 1904 => {
                let off = if rule == 1904 { 5 } else { 0 };
                let mut n = Node::build::<RangeTableFunc>(mcx)?;
                n.rowexpr = view.v(3 + off).node();
                n.docexpr = view.v(4 + off).node();
                n.columns = view.v(6 + off).list();
                if rule == 1904 {
                    n.namespaces = view.v(5).list();
                }
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1907 | 1908 => {
                let mut n = Node::build::<RangeTableFuncCol>(mcx)?;
                n.colname = Some(view.v(1).str_val());
                n.typeName = view.v(2).node();
                n.location = view.l(1);
                if rule == 1908 {
                    let mut nullability_seen = false;
                    for opt in view.v(3).list().iter() {
                        let d = opt.as_def_elem().expect("DefElem");
                        match d.defname {
                            Some("default") => {
                                if n.coldefexpr.is_some() {
                                    return Err(self.errposition_error(
                                        "only one DEFAULT value is allowed".into(),
                                        d.location,
                                    ));
                                }
                                n.coldefexpr = d.arg;
                            }
                            Some("path") => {
                                if n.colexpr.is_some() {
                                    return Err(self.errposition_error(
                                        "only one PATH value per column is allowed".into(),
                                        d.location,
                                    ));
                                }
                                n.colexpr = d.arg;
                            }
                            Some("__pg__is_not_null") => {
                                if nullability_seen {
                                    return Err(self.errposition_error(
                                        format!(
                                            "conflicting or redundant NULL / NOT NULL declarations for column \"{}\"",
                                            n.colname.unwrap()
                                        ),
                                        d.location,
                                    ));
                                }
                                n.is_not_null =
                                    d.arg.and_then(|a| a.as_boolean()).expect("Boolean").boolval;
                                nullability_seen = true;
                            }
                            other => {
                                return Err(self.errposition_error(
                                    format!(
                                        "unrecognized column option \"{}\"",
                                        other.unwrap_or("?")
                                    ),
                                    d.location,
                                ));
                            }
                        }
                    }
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1909 => {
                let mut n = Node::build::<RangeTableFuncCol>(mcx)?;
                n.colname = Some(view.v(1).str_val());
                n.for_ordinality = true;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1910 => *yyval = YYSTYPE::List(NodeList::make1(mcx, view.v(1).node().expect("opt"))?),
            1911 => {
                let mut l = view.v(1).list();
                l.lappend(mcx, view.v(2).node().expect("opt"))?;
                *yyval = YYSTYPE::List(l);
            }
            1912 => {
                let name = view.v(1).str_val();
                if name == "__pg__is_not_null" {
                    return Err(self.errposition_error(
                        format!("option name \"{name}\" cannot be used in XMLTABLE"),
                        view.l(1),
                    ));
                }
                *yyval = def_elem(mcx, name, view.v(2).node(), view.l(1))?;
            }
            1913 => *yyval = def_elem(mcx, "default", view.v(2).node(), view.l(1))?,
            1914 | 1915 => {
                let arg = Node::mk(mcx, Boolean { boolval: rule == 1914 })?;
                *yyval = def_elem(mcx, "__pg__is_not_null", Some(arg), view.l(1))?;
            }
            1916 => *yyval = def_elem(mcx, "path", view.v(2).node(), view.l(1))?,
            // xml_namespace_el: b_expr AS ColLabel | DEFAULT b_expr;
            // xml_attribute_el: a_expr AS ColLabel | a_expr
            1919 | 2208 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::ResTarget {
                        name: Some(view.v(3).str_val()),
                        indirection: NodeList::nil(),
                        val: view.v(1).node(),
                        location: view.l(1),
                    },
                )?));
            }
            1920 | 2209 => {
                let at = if rule == 1920 { 2 } else { 1 };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::ResTarget {
                        name: None,
                        indirection: NodeList::nil(),
                        val: view.v(at).node(),
                        location: view.l(1),
                    },
                )?));
            }
            // a_expr/b_expr IS [NOT] DOCUMENT_P
            2081 | 2110 => {
                let args = NodeList::make1(mcx, view.v(1).node().expect("a_expr"))?;
                *yyval = YYSTYPE::Node(Some(make_xml_expr(
                    mcx,
                    XmlExprOp::IS_DOCUMENT,
                    None,
                    NodeList::nil(),
                    args,
                    view.l(2),
                )?));
            }
            2082 | 2111 => {
                let args = NodeList::make1(mcx, view.v(1).node().expect("a_expr"))?;
                let x =
                    make_xml_expr(mcx, XmlExprOp::IS_DOCUMENT, None, NodeList::nil(), args, view.l(2))?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    BoolExpr {
                        boolop: BoolExprType::NOT_EXPR,
                        args: NodeList::make1(mcx, x)?,
                        location: view.l(2),
                    },
                )?));
            }
            // a_expr IS [NOT] [unicode_normal_form] NORMALIZED.
            2083..=2086 => {
                let not = rule == 2085 || rule == 2086;
                let form = match rule {
                    2084 => Some((view.v(3).str_val(), view.l(3))),
                    2086 => Some((view.v(4).str_val(), view.l(4))),
                    _ => None,
                };
                let expr = view.v(1).node().expect("a_expr");
                let args = match form {
                    Some((sval, loc)) => {
                        let c = Node::mk_a_const(
                            mcx,
                            Some(ValUnion::String(types_nodes::String { sval })),
                            loc,
                        )?;
                        NodeList::make2(mcx, expr, c)?
                    }
                    None => NodeList::make1(mcx, expr)?,
                };
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "is_normalized")?,
                    args,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(2),
                )?;
                *yyval = YYSTYPE::Node(Some(if not {
                    Node::mk(
                        mcx,
                        BoolExpr {
                            boolop: BoolExprType::NOT_EXPR,
                            args: NodeList::make1(mcx, f.seal())?,
                            location: view.l(2),
                        },
                    )?
                } else {
                    f.seal()
                }));
            }
            2174 => {
                *yyval = YYSTYPE::Node(Some(make_xml_expr(
                    mcx,
                    XmlExprOp::IS_XMLCONCAT,
                    None,
                    NodeList::nil(),
                    view.v(3).list(),
                    view.l(1),
                )?));
            }
            2175..=2178 => {
                let name = Some(view.v(4).str_val());
                let (named_args, args) = match rule {
                    2176 => (view.v(6).list(), NodeList::nil()),
                    2177 => (NodeList::nil(), view.v(6).list()),
                    2178 => (view.v(6).list(), view.v(8).list()),
                    _ => (NodeList::nil(), NodeList::nil()),
                };
                *yyval = YYSTYPE::Node(Some(make_xml_expr(
                    mcx,
                    XmlExprOp::IS_XMLELEMENT,
                    name,
                    named_args,
                    args,
                    view.l(1),
                )?));
            }
            // xmlexists(A PASSING [BY REF] B [BY REF]) -> xmlexists(A, B)
            2179 => {
                let args = NodeList::make2(
                    mcx,
                    view.v(3).node().expect("c_expr"),
                    view.v(4).node().expect("xmlexists_argument"),
                )?;
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "xmlexists")?,
                    args,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            2180 => {
                *yyval = YYSTYPE::Node(Some(make_xml_expr(
                    mcx,
                    XmlExprOp::IS_XMLFOREST,
                    None,
                    view.v(3).list(),
                    NodeList::nil(),
                    view.l(1),
                )?));
            }
            2181 => {
                let ws = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::Boolean(Boolean { boolval: view.v(5).boolean() })),
                    -1,
                )?;
                let args = NodeList::make2(mcx, view.v(4).node().expect("a_expr"), ws)?;
                let x = make_xml_expr(
                    mcx,
                    XmlExprOp::IS_XMLPARSE,
                    None,
                    NodeList::nil(),
                    args,
                    view.l(1),
                )?;
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    x.with_mut::<XmlExpr, _>(|n| n.xmloption = xml_option_from_ival(view.v(3).ival()))
                        .expect("XmlExpr");
                }
                *yyval = YYSTYPE::Node(Some(x));
            }
            2182 | 2183 => {
                let args = if rule == 2183 {
                    NodeList::make1(mcx, view.v(6).node().expect("a_expr"))?
                } else {
                    NodeList::nil()
                };
                *yyval = YYSTYPE::Node(Some(make_xml_expr(
                    mcx,
                    XmlExprOp::IS_XMLPI,
                    Some(view.v(4).str_val()),
                    NodeList::nil(),
                    args,
                    view.l(1),
                )?));
            }
            2184 => {
                let args = NodeList::make3(
                    mcx,
                    view.v(3).node().expect("a_expr"),
                    view.v(5).node().expect("xml_root_version"),
                    view.v(6).node().expect("opt_xml_root_standalone"),
                )?;
                *yyval = YYSTYPE::Node(Some(make_xml_expr(
                    mcx,
                    XmlExprOp::IS_XMLROOT,
                    None,
                    NodeList::nil(),
                    args,
                    view.l(1),
                )?));
            }
            2185 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::rawnodes::XmlSerialize {
                        xmloption: xml_option_from_ival(view.v(3).ival()),
                        expr: view.v(4).node(),
                        typeName: view.v(6).node(),
                        indent: view.v(7).boolean(),
                        location: view.l(1),
                    },
                )?));
            }
            2200 => *yyval = YYSTYPE::Node(Some(Node::mk_a_const(mcx, None, -1)?)),
            2201..=2204 => {
                let v = match rule {
                    2201 => 0,
                    2202 => 1,
                    2203 => 2,
                    _ => 3,
                };
                *yyval = YYSTYPE::Node(Some(Node::mk_a_const(
                    mcx,
                    Some(ValUnion::Integer(Integer { ival: v })),
                    -1,
                )?));
            }
            2210 => *yyval = YYSTYPE::Ival(0),
            2211 => *yyval = YYSTYPE::Ival(1),
            2212 | 2215 => *yyval = YYSTYPE::Boolean(true),
            2213 | 2214 | 2216 | 2217 => *yyval = YYSTYPE::Boolean(false),
            // CompositeTypeStmt: CREATE TYPE_P any_name AS '(' OptTableFuncElementList ')'
            853 => {
                let names = view.v(3).list();
                let mut it = names.iter();
                let (c, s, r) = match names.len() {
                    1 => (None, None, it.next()),
                    2 => (None, it.next(), it.next()),
                    3 => (it.next(), it.next(), it.next()),
                    _ => return Err(self.improper_qualified_name(None, &names, view.l(3))),
                };
                let sval = |n: Option<Node<'mcx>>| {
                    n.and_then(|n| n.as_string()).map(|s| s.sval)
                };
                // makeRangeVarFromAnyName: makeNode zero-fill => inh false.
                let rv = Node::mk(
                    mcx,
                    RangeVar {
                        catalogname: sval(c),
                        schemaname: sval(s),
                        relname: sval(r),
                        inh: false,
                        relpersistence: RELPERSISTENCE_PERMANENT,
                        alias: None,
                        location: view.l(3),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::rawnodes::CompositeTypeStmt {
                        typevar: rv.as_range_var(),
                        coldeflist: view.v(6).list(),
                    },
                )?));
            }
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
            // b_expr: the a_expr forms without boolean/IS tails (DISTINCT
            // arms 2108/2109 stay unimplemented-rule loud).
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
                    let arg = e.node();
                    self.check_indirection(&ind)?;
                    *yyval = YYSTYPE::Node(Some(Node::mk(
                        mcx,
                        types_nodes::A_Indirection { arg, indirection: ind },
                    )?));
                } else {
                    *yyval = e;
                }
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
            // c_expr: select_with_parens indirection
            2119 => {
                let subselect = view.v(1).node().expect("select_with_parens");
                let ind = view.v(2).list();
                self.check_indirection(&ind)?;
                let sub = Node::mk(
                    mcx,
                    types_nodes::SubLink {
                        subLinkType: types_nodes::SubLinkType::EXPR_SUBLINK,
                        subLinkId: 0,
                        testexpr: None,
                        operName: NodeList::nil(),
                        subselect,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Indirection { arg: Some(sub), indirection: ind },
                )?));
            }
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
            // c_expr: ARRAY select_with_parens
            2121 => {
                let subselect = view.v(2).node().expect("select_with_parens");
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::SubLink {
                        subLinkType: types_nodes::SubLinkType::ARRAY_SUBLINK,
                        subLinkId: 0,
                        testexpr: None,
                        operName: NodeList::nil(),
                        subselect,
                        location: view.l(1),
                    },
                )?));
            }
            // c_expr: ARRAY array_expr (point outermost A_ArrayExpr at ARRAY)
            2122 => {
                let n = view.v(2).node().expect("array_expr");
                debug_assert!(n.node_tag() == types_nodes::NodeTag::T_A_ArrayExpr);
                // SAFETY: node built by rules 2301-2303 below, exclusively ours.
                unsafe {
                    Node::with_mut::<types_nodes::A_ArrayExpr, ()>(n, |a| a.location = view.l(1));
                }
                *yyval = YYSTYPE::Node(Some(n));
            }
            // array_expr: '[' expr_list ']' | '[' array_expr_list ']' | '[' ']'
            2301 | 2302 => {
                let elements = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_ArrayExpr {
                        elements,
                        list_start: view.l(1),
                        list_end: view.l(3),
                        location: view.l(1),
                    },
                )?));
            }
            2303 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_ArrayExpr {
                        elements: NodeList::nil(),
                        list_start: view.l(1),
                        list_end: view.l(2),
                        location: view.l(1),
                    },
                )?));
            }
            2304 => {
                let e = view.v(1).node().expect("array_expr");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, e)?);
            }
            2305 => {
                let mut list = view.v(1).list();
                let e = view.v(3).node().expect("array_expr");
                list.lappend(mcx, e)?;
                *yyval = YYSTYPE::List(list);
            }
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
            // func_arg_expr: param_name COLON_EQUALS/EQUALS_GREATER a_expr
            2295 | 2296 => {
                let n = Node::mk(
                    mcx,
                    types_nodes::NamedArgExpr {
                        arg: Some(view.v(3).node().expect("a_expr")),
                        name: Some(view.v(1).str_val()),
                        argnumber: -1,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            2291 | 2293 => {
                let mut list = view.v(1).list();
                let e = view.v(3).node().expect("a_expr");
                list.lappend(mcx, e)?;
                *yyval = YYSTYPE::List(list);
            }
            2341 => *yyval = YYSTYPE::Node(Some(Node::mk_a_star(mcx)?)),
            1944 => *yyval = YYSTYPE::List(NodeList::nil()),
            // [SETOF] SimpleTypename ARRAY -> arrayBounds = [-1]
            1940 | 1941 => {
                let tn =
                    view.v(if rule == 1941 { 2 } else { 1 }).node().expect("SimpleTypename");
                // SAFETY: TypeName node built by this parse, exclusively ours.
                unsafe {
                    let bounds = NodeList::make1(mcx, Node::mk_integer(mcx, -1)?)?;
                    Node::with_mut::<types_nodes::TypeName, ()>(tn, |t| {
                        t.arrayBounds = bounds;
                        if rule == 1941 {
                            t.setof = true;
                        }
                    });
                }
                *yyval = YYSTYPE::Node(Some(tn));
            }
            // [SETOF] SimpleTypename ARRAY '[' Iconst ']' -> arrayBounds = [n]
            1938 | 1939 => {
                let (t_i, n_i) = if rule == 1939 { (2, 5) } else { (1, 4) };
                let tn = view.v(t_i).node().expect("SimpleTypename");
                // SAFETY: TypeName node built by this parse, exclusively ours.
                unsafe {
                    let bounds =
                        NodeList::make1(mcx, Node::mk_integer(mcx, view.v(n_i).ival())?)?;
                    Node::with_mut::<types_nodes::TypeName, ()>(tn, |t| {
                        t.arrayBounds = bounds;
                        if rule == 1939 {
                            t.setof = true;
                        }
                    });
                }
                *yyval = YYSTYPE::Node(Some(tn));
            }
            2342 => {
                let uidx = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Indices { is_slice: false, lidx: None, uidx },
                )?));
            }
            2343 => {
                let lidx = view.v(2).node();
                let uidx = view.v(4).node();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Indices { is_slice: true, lidx, uidx },
                )?));
            }
            // opt_slice_bound: a_expr | empty
            2344 => *yyval = YYSTYPE::Node(view.v(1).node()),
            2345 => *yyval = YYSTYPE::Node(None),
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
            1548 => {
                let mut n = Node::build::<types_nodes::parsenodes::CreateConversionStmt>(mcx)?;
                n.conversion_name = view.v(4).list();
                n.for_encoding_name = Some(view.v(6).str_val());
                n.to_encoding_name = Some(view.v(8).str_val());
                n.func_name = view.v(10).list();
                n.def = view.v(2).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
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
            // utility_option_arg: NumericOnly passthrough.
            1569 => *yyval = view.v(1),
            1571 | 1573 | 1575 | 1577 => *yyval = YYSTYPE::Boolean(true),
            1572 | 1574 | 1576 | 1578 => *yyval = YYSTYPE::Boolean(false),
            // PREPARE/EXECUTE/DEALLOCATE, incl. CREATE TABLE AS EXECUTE.
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
            1609 | 1610 => {
                let ine = rule == 1610;
                let (t, nm, w) = if ine { (7, 10, 12) } else { (4, 7, 9) };
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
                let e = Node::mk(
                    mcx,
                    ExecuteStmt {
                        name: Some(view.v(nm).str_val()),
                        params: view.v(nm + 1).list(),
                    },
                )?;
                let mut n = Node::build::<CreateTableAsStmt>(mcx)?;
                n.query = Some(e);
                n.into = Some(into_node);
                n.objtype = ObjectType::OBJECT_TABLE;
                n.is_select_into = false;
                n.if_not_exists = ine;
                *yyval = YYSTYPE::Node(Some(n.seal()));
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
            // CURRENT_DATE .. CURRENT_SCHEMA (makeSQLValueFunction; 2152
            // SYSTEM_USER is a makeFuncCall, not an SVFOP — stays a loud).
            2140..=2151 | 2153..=2155 => {
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
                    2148 => (Op::SVFOP_LOCALTIMESTAMP_N, view.v(3).ival()),
                    2149 => (Op::SVFOP_CURRENT_ROLE, -1),
                    2150 => (Op::SVFOP_CURRENT_USER, -1),
                    2151 => (Op::SVFOP_SESSION_USER, -1),
                    2153 => (Op::SVFOP_USER, -1),
                    2154 => (Op::SVFOP_CURRENT_CATALOG, -1),
                    _ => (Op::SVFOP_CURRENT_SCHEMA, -1),
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
            // COLLATION FOR '(' a_expr ')'.
            2139 => {
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "pg_collation_for")?,
                    NodeList::make1(mcx, view.v(4).node().expect("a_expr"))?,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
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
            // NORMALIZE '(' a_expr [',' unicode_normal_form] ')'.
            2158 => {
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "normalize")?,
                    NodeList::make1(mcx, view.v(3).node().expect("a_expr"))?,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            2159 => {
                let form = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String { sval: view.v(5).str_val() })),
                    view.l(5),
                )?;
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "normalize")?,
                    NodeList::make2(mcx, view.v(3).node().expect("a_expr"), form)?,
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // OVERLAY '(' overlay_list ')' — SQL-syntax form.
            2160 => {
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "overlay")?,
                    view.v(3).list(),
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // OVERLAY '(' func_arg_list_opt ')' — plain call form.
            2161 => {
                let f = make_func_call(
                    mcx,
                    NodeList::make1(mcx, Node::mk_string(mcx, "overlay")?)?,
                    view.v(3).list(),
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // POSITION '(' position_list ')'.
            2162 => {
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, "position")?,
                    view.v(3).list(),
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // TREAT '(' a_expr AS Typename ')': funcname from type's last name.
            2165 => {
                let t = view.v(5).node().expect("Typename").as_type_name().expect("TypeName");
                let name = t.names.last().expect("TypeName names").as_string().expect("String");
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, name.sval)?,
                    NodeList::make1(mcx, view.v(3).node().expect("a_expr"))?,
                    CoercionForm::COERCE_EXPLICIT_CALL,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // TRIM '(' [BOTH|LEADING|TRAILING] trim_list ')'.
            2166..=2169 => {
                let (name, li) = match rule {
                    2166 => ("btrim", 4),
                    2167 => ("ltrim", 4),
                    2168 => ("rtrim", 4),
                    _ => ("btrim", 3),
                };
                let f = make_func_call(
                    mcx,
                    system_func_name(mcx, name)?,
                    view.v(li).list(),
                    CoercionForm::COERCE_SQL_SYNTAX,
                    view.l(1),
                )?;
                *yyval = YYSTYPE::Node(Some(f.seal()));
            }
            // NULLIF '(' a_expr ',' a_expr ')' — makeSimpleA_Expr(AEXPR_NULLIF, "=").
            2170 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::A_Expr {
                        kind: A_Expr_Kind::AEXPR_NULLIF,
                        name: NodeList::make1(mcx, Node::mk_string(mcx, "=")?)?,
                        lexpr: view.v(3).node(),
                        rexpr: view.v(5).node(),
                        rexpr_list_start: 0,
                        rexpr_list_end: 0,
                        location: view.l(1),
                    },
                )?));
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
            // unicode_normal_form: NFC | NFD | NFKC | NFKD.
            2315..=2318 => {
                *yyval = YYSTYPE::Str(["NFC", "NFD", "NFKC", "NFKD"][rule - 2315]);
            }
            // overlay_list: a_expr PLACING a_expr FROM a_expr [FOR a_expr].
            2319 => {
                *yyval = YYSTYPE::List(NodeList::from_slice(
                    mcx,
                    &[
                        view.v(1).node().expect("a_expr"),
                        view.v(3).node().expect("a_expr"),
                        view.v(5).node().expect("a_expr"),
                        view.v(7).node().expect("a_expr"),
                    ],
                )?);
            }
            2320 => {
                *yyval = YYSTYPE::List(NodeList::make3(
                    mcx,
                    view.v(1).node().expect("a_expr"),
                    view.v(3).node().expect("a_expr"),
                    view.v(5).node().expect("a_expr"),
                )?);
            }
            // position_list: b_expr IN_P b_expr — position(A in B) is position(B, A).
            2321 => {
                *yyval = YYSTYPE::List(NodeList::make2(
                    mcx,
                    view.v(3).node().expect("b_expr"),
                    view.v(1).node().expect("b_expr"),
                )?);
            }
            // trim_list: a_expr FROM expr_list (FROM-only/plain ride DISPATCH).
            2327 => {
                let mut list = view.v(3).list();
                list.lappend(mcx, view.v(1).node().expect("a_expr"))?;
                *yyval = YYSTYPE::List(list);
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
            // set_rest_more: SCHEMA Sconst -> SET search_path.
            215 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_VALUE;
                n.name = Some("search_path");
                let s = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String { sval: view.v(2).str_val() })),
                    view.l(2),
                )?;
                n.args = NodeList::make1(mcx, s)?;
                n.location = view.l(2);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            217 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_VALUE;
                n.name = Some("role");
                let s = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String { sval: view.v(2).str_val() })),
                    view.l(2),
                )?;
                n.args = NodeList::make1(mcx, s)?;
                n.location = view.l(2);
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
            // set_rest_more: XML_P OPTION document_or_content.
            220 => {
                let mut n = Node::build::<VariableSetStmt>(mcx)?;
                n.kind = VariableSetKind::VAR_SET_VALUE;
                n.name = Some("xmloption");
                let s = Node::mk_a_const(
                    mcx,
                    Some(ValUnion::String(types_nodes::String {
                        sval: if view.v(3).ival() == 1 { "CONTENT" } else { "DOCUMENT" },
                    })),
                    view.l(3),
                )?;
                n.args = NodeList::make1(mcx, s)?;
                n.jumble_args = true;
                n.location = -1;
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
            // [Function]SetResetClause: VariableResetStmt (node -> vsetstmt cast in C).
            256 | 258 => *yyval = view.v(1),
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
            // TransactionStmt: PREPARE TRANSACTION / COMMIT PREPARED /
            // ROLLBACK PREPARED, all over Sconst gids.
            1467 | 1468 | 1469 => {
                let kind = match rule {
                    1467 => TransactionStmtKind::TRANS_STMT_PREPARE,
                    1468 => TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED,
                    _ => TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED,
                };
                let mut n = Node::build::<TransactionStmt>(mcx)?;
                n.kind = kind;
                n.gid = Some(view.v(3).str_val());
                n.location = view.l(3);
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
            // AlterCollationStmt: ALTER COLLATION any_name REFRESH VERSION_P.
            1526 => {
                let mut n = Node::build::<types_nodes::parsenodes::AlterCollationStmt>(mcx)?;
                n.collname = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterSystemStmt: ALTER SYSTEM_P SET generic_set
            //               | ALTER SYSTEM_P RESET generic_reset.
            1527 | 1528 => {
                let setstmt = view
                    .v(4)
                    .node()
                    .expect("generic_set")
                    .as_variable_set_stmt()
                    .expect("VariableSetStmt");
                let n = Node::mk(mcx, AlterSystemStmt { setstmt })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1589 => {
                let mut n = Node::build::<types_nodes::parsenodes::ExplainStmt>(mcx)?;
                n.query = view.v(5).node();
                n.options = view.v(3).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // table_ref: select_with_parens opt_alias_clause
            //          | LATERAL_P select_with_parens opt_alias_clause.
            1838 | 1839 => {
                let off = if rule == 1839 { 1 } else { 0 };
                let mut n = Node::build::<RangeSubselect>(mcx)?;
                n.lateral = rule == 1839;
                n.subquery = view.v(1 + off).node();
                n.alias = view.v(2 + off).alias();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alias_clause: AS ColId '(' name_list ')' | ColId '(' name_list ')'.
            1850 | 1852 => {
                let off = if rule == 1850 { 1 } else { 0 };
                let a = Node::mk_mut(
                    mcx,
                    Alias {
                        aliasname: Some(view.v(1 + off).str_val()),
                        colnames: view.v(3 + off).list(),
                    },
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
            // JOIN ... join_qual | NATURAL join_type JOIN | NATURAL JOIN.
            1845 | 1846 | 1847 | 1848 | 1849 => {
                let (jointype, is_natural, rarg_at, qual_at) = match rule {
                    1845 => (JoinType::JOIN_INNER, false, 4, 0),
                    1846 => (join_type_from_ival(view.v(2).ival()), false, 4, 5),
                    1847 => (JoinType::JOIN_INNER, false, 3, 4),
                    1848 => (join_type_from_ival(view.v(3).ival()), true, 5, 0),
                    _ => (JoinType::JOIN_INNER, true, 4, 0),
                };
                let (using_clause, join_using_alias, quals) = if qual_at == 0 {
                    (NodeList::nil(), None, None)
                } else {
                    let q = view.v(qual_at);
                    if q.is_join_using() {
                        let (cols, alias) = q.join_using();
                        (cols, alias, None)
                    } else {
                        (NodeList::nil(), None, q.node())
                    }
                };
                let n = Node::mk(
                    mcx,
                    JoinExpr {
                        jointype,
                        isNatural: is_natural,
                        larg: view.v(1).node().expect("table_ref"),
                        rarg: view.v(rarg_at).node().expect("table_ref"),
                        usingClause: using_clause,
                        join_using_alias,
                        quals,
                        alias: None,
                        rtindex: 0,
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // opt_alias_clause_for_join_using: AS ColId (empty rides DISPATCH).
            1856 => {
                let name = view.v(2).str_val();
                *yyval = YYSTYPE::Alias(Some(mk_alias(mcx, name)?));
            }
            // join_type: FULL/LEFT/RIGHT/INNER opt_outer.
            1863 => *yyval = YYSTYPE::Ival(JoinType::JOIN_FULL as i32),
            1864 => *yyval = YYSTYPE::Ival(JoinType::JOIN_LEFT as i32),
            1865 => *yyval = YYSTYPE::Ival(JoinType::JOIN_RIGHT as i32),
            1866 => *yyval = YYSTYPE::Ival(JoinType::JOIN_INNER as i32),
            // join_qual: USING '(' name_list ')' opt_alias_clause_for_join_using.
            1869 => {
                *yyval = YYSTYPE::JoinUsing(mcx::leak_in(mcx::alloc_in(
                    mcx,
                    JoinQualUsing { cols: view.v(3).list(), alias: view.v(5).alias() },
                )?));
            }
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
            // alter_table_cmds (tablespace forms 279-280 stay loud).
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
            // AlterTableStmt: ALTER TABLE [IF_P EXISTS] relation_expr partition_cmd
            277 | 278 => {
                let (rv, cmd) = if rule == 277 {
                    (view.v(3), view.v(4))
                } else {
                    (view.v(5), view.v(6))
                };
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = rv.node().expect("relation_expr").as_variant::<RangeVar>();
                n.cmds = NodeList::make1(mcx, cmd.node().expect("partition_cmd"))?;
                n.objtype = ObjectType::OBJECT_TABLE;
                n.missing_ok = rule == 278;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableMoveAllStmt: ALTER TABLE ALL IN TABLESPACE name
            // [OWNED BY role_list] SET TABLESPACE name [NOWAIT] (gram.y:2136,2148).
            279 | 280 => {
                let mut n = Node::build::<AlterTableMoveAllStmt>(mcx)?;
                n.orig_tablespacename = Some(view.v(6).str_val());
                n.objtype = ObjectType::OBJECT_TABLE;
                if rule == 279 {
                    n.new_tablespacename = Some(view.v(9).str_val());
                    n.nowait = view.v(10).ival() != 0;
                } else {
                    n.roles = view.v(9).list();
                    n.new_tablespacename = Some(view.v(12).str_val());
                    n.nowait = view.v(13).ival() != 0;
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableStmt: ALTER INDEX qualified_name index_partition_cmd
            283 => {
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = view.v(3).node().expect("qualified_name").as_variant::<RangeVar>();
                n.cmds = NodeList::make1(mcx, view.v(4).node().expect("index_partition_cmd"))?;
                n.objtype = ObjectType::OBJECT_INDEX;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableMoveAllStmt: ALTER INDEX ALL IN TABLESPACE ... (gram.y:2190,2202).
            284 | 285 => {
                let mut n = Node::build::<AlterTableMoveAllStmt>(mcx)?;
                n.orig_tablespacename = Some(view.v(6).str_val());
                n.objtype = ObjectType::OBJECT_INDEX;
                if rule == 284 {
                    n.new_tablespacename = Some(view.v(9).str_val());
                    n.nowait = view.v(10).ival() != 0;
                } else {
                    n.roles = view.v(9).list();
                    n.new_tablespacename = Some(view.v(12).str_val());
                    n.nowait = view.v(13).ival() != 0;
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableStmt: ALTER SEQUENCE [IF_P EXISTS] qualified_name
            // alter_table_cmds
            286 | 287 => {
                let (rv, cmds) = if rule == 286 {
                    (view.v(3), view.v(4))
                } else {
                    (view.v(5), view.v(6))
                };
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = rv.node().expect("qualified_name").as_variant::<RangeVar>();
                n.cmds = cmds.list();
                n.objtype = ObjectType::OBJECT_SEQUENCE;
                n.missing_ok = rule == 287;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableStmt: ALTER VIEW [IF_P EXISTS] qualified_name alter_table_cmds
            288 | 289 => {
                let (rv, cmds) = if rule == 288 {
                    (view.v(3), view.v(4))
                } else {
                    (view.v(5), view.v(6))
                };
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = rv.node().expect("qualified_name").as_variant::<RangeVar>();
                n.cmds = cmds.list();
                n.objtype = ObjectType::OBJECT_VIEW;
                n.missing_ok = rule == 289;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableStmt: ALTER MATERIALIZED VIEW [IF_P EXISTS]
            // qualified_name alter_table_cmds
            290 | 291 => {
                let (rv, cmds) = if rule == 290 {
                    (view.v(4), view.v(5))
                } else {
                    (view.v(6), view.v(7))
                };
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = rv.node().expect("qualified_name").as_variant::<RangeVar>();
                n.cmds = cmds.list();
                n.objtype = ObjectType::OBJECT_MATVIEW;
                n.missing_ok = rule == 291;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableMoveAllStmt: ALTER MATERIALIZED VIEW ALL IN TABLESPACE
            // ... (gram.y:2274,2286).
            292 | 293 => {
                let mut n = Node::build::<AlterTableMoveAllStmt>(mcx)?;
                n.orig_tablespacename = Some(view.v(7).str_val());
                n.objtype = ObjectType::OBJECT_MATVIEW;
                if rule == 292 {
                    n.new_tablespacename = Some(view.v(10).str_val());
                    n.nowait = view.v(11).ival() != 0;
                } else {
                    n.roles = view.v(10).list();
                    n.new_tablespacename = Some(view.v(13).str_val());
                    n.nowait = view.v(14).ival() != 0;
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableStmt: ALTER FOREIGN TABLE [IF_P EXISTS] relation_expr
            // alter_table_cmds
            294 | 295 => {
                let (rv, cmds) = if rule == 294 {
                    (view.v(4), view.v(5))
                } else {
                    (view.v(6), view.v(7))
                };
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = rv.node().expect("relation_expr").as_variant::<RangeVar>();
                n.cmds = cmds.list();
                n.objtype = ObjectType::OBJECT_FOREIGN_TABLE;
                n.missing_ok = rule == 295;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // partition_cmd: ATTACH PARTITION qualified_name PartitionBoundSpec
            //             | DETACH PARTITION qualified_name opt_concurrently
            //             | DETACH PARTITION qualified_name FINALIZE
            // index_partition_cmd: ATTACH PARTITION qualified_name
            298..=301 => {
                let mut cmd = Node::build::<PartitionCmd>(mcx)?;
                cmd.name = view.v(3).node().expect("qualified_name").as_variant::<RangeVar>();
                let subtype = match rule {
                    298 => {
                        cmd.bound = view.v(4).node();
                        AlterTableType::AT_AttachPartition
                    }
                    299 => {
                        cmd.concurrent = view.v(4).boolean();
                        AlterTableType::AT_DetachPartition
                    }
                    300 => AlterTableType::AT_DetachPartitionFinalize,
                    _ => AlterTableType::AT_AttachPartition,
                };
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = subtype;
                n.def = Some(cmd.seal());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTableStmt: ALTER INDEX [IF_P EXISTS] qualified_name
            // alter_table_cmds
            281 | 282 => {
                let (rv, cmds) = if rule == 281 {
                    (view.v(3), view.v(4))
                } else {
                    (view.v(5), view.v(6))
                };
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = rv.node().expect("qualified_name").as_variant::<RangeVar>();
                n.cmds = cmds.list();
                n.objtype = ObjectType::OBJECT_INDEX;
                n.missing_ok = rule == 282;
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
            // alter_table_cmd: ALTER CONSTRAINT name ConstraintAttributeSpec
            327 => {
                let mut c = Node::build::<parsenodes::ATAlterConstraint>(mcx)?;
                c.conname = Some(view.v(3).str_val());
                let bits = view.v(4).ival();
                if bits & (CAS_NOT_ENFORCED | CAS_ENFORCED) != 0 {
                    c.alterEnforceability = true;
                }
                if bits
                    & (CAS_DEFERRABLE
                        | CAS_NOT_DEFERRABLE
                        | CAS_INITIALLY_DEFERRED
                        | CAS_INITIALLY_IMMEDIATE)
                    != 0
                {
                    c.alterDeferrability = true;
                }
                if bits & CAS_NO_INHERIT != 0 {
                    c.alterInheritability = true;
                }
                // C raises this before processCASbits.
                if bits & CAS_NOT_VALID != 0 {
                    return Err(self.errposition_error_code(
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        "constraints cannot be altered to be NOT VALID".to_string(),
                        view.l(4),
                    ));
                }
                let cas = self.process_cas_bits(
                    bits,
                    view.l(4),
                    "FOREIGN KEY",
                    CasTargets {
                        deferrable: true,
                        initdeferred: true,
                        is_enforced: true,
                        no_inherit: true,
                        ..Default::default()
                    },
                )?;
                c.deferrable = cas.deferrable;
                c.initdeferred = cas.initdeferred;
                c.is_enforced = cas.is_enforced;
                c.noinherit = cas.no_inherit;
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_AlterConstraint;
                n.def = Some(c.seal());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alter_table_cmd: ALTER CONSTRAINT name INHERIT
            328 => {
                let mut c = Node::build::<parsenodes::ATAlterConstraint>(mcx)?;
                c.conname = Some(view.v(3).str_val());
                c.alterInheritability = true;
                c.noinherit = false;
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_AlterConstraint;
                n.def = Some(c.seal());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterCompositeTypeStmt: ALTER TYPE_P any_name alter_type_cmds
            400 => {
                let mut n = Node::build::<AlterTableStmt>(mcx)?;
                n.relation = Some(self.range_var_from_any_name(&view.v(3).list(), view.l(3))?);
                n.cmds = view.v(4).list();
                n.objtype = ObjectType::OBJECT_TYPE;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            401 => {
                let el = view.v(1).node().expect("alter_type_cmd");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            402 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("alter_type_cmd"))?;
                *yyval = YYSTYPE::List(list);
            }
            403 => {
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_AddColumn;
                n.def = view.v(3).node();
                n.behavior = drop_behavior(view.v(4).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            404 | 405 => {
                let name_i = if rule == 404 { 5 } else { 3 };
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_DropColumn;
                n.name = Some(view.v(name_i).str_val());
                n.behavior = drop_behavior(view.v(name_i + 1).ival());
                n.missing_ok = rule == 404;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            406 => {
                let mut def = Node::build::<ColumnDef>(mcx)?;
                def.typeName = view.v(6).node();
                def.collClause = view.v(7).node();
                def.location = view.l(3);
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_AlterColumnType;
                n.name = Some(view.v(3).str_val());
                n.def = Some(def.seal());
                n.behavior = drop_behavior(view.v(8).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alter_column_default: SET DEFAULT a_expr | DROP DEFAULT
            364 => *yyval = YYSTYPE::Node(view.v(3).node()),
            365 => *yyval = YYSTYPE::Node(Option::None),
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
            // RenameStmt: ALTER POLICY [IF_P EXISTS] name ON qualified_name
            // RENAME TO name
            1286 | 1287 => {
                let (sub, rv, nm) = if rule == 1286 { (3, 5, 8) } else { (5, 7, 10) };
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_POLICY;
                n.relation = view.v(rv).node().expect("qualified_name").as_variant::<RangeVar>();
                n.subname = Some(view.v(sub).str_val());
                n.newname = Some(view.v(nm).str_val());
                n.missing_ok = rule == 1287;
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
            // RenameStmt: ALTER TABLE [IF_P EXISTS] relation_expr RENAME
            // CONSTRAINT name TO name
            1312 | 1313 => {
                let (rv, sub, nm) = if rule == 1312 { (3, 6, 8) } else { (5, 8, 10) };
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_TABCONSTRAINT;
                n.relationType = ObjectType::OBJECT_TABLE;
                n.relation = view.v(rv).node().expect("relation_expr").as_variant::<RangeVar>();
                n.subname = Some(view.v(sub).str_val());
                n.newname = Some(view.v(nm).str_val());
                n.missing_ok = rule == 1313;
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
            // alter_table_cmd: [ENABLE|DISABLE|FORCE|NO FORCE] ROW LEVEL SECURITY
            359..=362 => {
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = match rule {
                    359 => AlterTableType::AT_EnableRowSecurity,
                    360 => AlterTableType::AT_DisableRowSecurity,
                    361 => AlterTableType::AT_ForceRowSecurity,
                    _ => AlterTableType::AT_NoForceRowSecurity,
                };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // alter_table_cmd: ALTER [COLUMN] col forms (execution is the
            // tablecmds/parse_utilcmd named gates; 327/328 stay loud — the
            // fk-constraints lane owns their inputs).
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
            // ALTER [COLUMN] col ADD_P GENERATED generated_when AS IDENTITY_P
            // OptParenthesizedSeqOptList
            318 => {
                let mut c = Node::build::<Constraint>(mcx)?;
                c.contype = ConstrType::CONSTR_IDENTITY;
                c.generated_when = view.v(6).ival() as u8;
                c.options = view.v(9).list();
                c.location = view.l(5);
                let mut n = Node::build::<AlterTableCmd>(mcx)?;
                n.subtype = AlterTableType::AT_AddIdentity;
                n.name = Some(view.v(3).str_val());
                n.def = Some(c.seal());
                *yyval = YYSTYPE::Node(Some(n.seal()));
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
            // CreatePLangStmt: parameterless form is CREATE EXTENSION (OR
            // REPLACE read as IF NOT EXISTS, TRUSTED ignored — gram.y 5102).
            666 => {
                let mut n = Node::build::<types_nodes::rawnodes::CreateExtensionStmt>(mcx)?;
                n.if_not_exists = view.v(2).boolean();
                n.extname = Some(view.v(6).str_val());
                n.options = NodeList::nil();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            667 => {
                let mut n = Node::build::<types_nodes::parsenodes::CreatePLangStmt>(mcx)?;
                n.replace = view.v(2).boolean();
                n.plname = Some(view.v(6).str_val());
                n.plhandler = view.v(8).list();
                n.plinline = view.v(9).list();
                n.plvalidator = view.v(10).list();
                n.pltrusted = view.v(3).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_trusted.
            668 => *yyval = YYSTYPE::Boolean(true),
            669 => *yyval = YYSTYPE::Boolean(false),
            // handler_name: name | name attrs.
            670 => {
                let s = Node::mk_string(mcx, view.v(1).str_val())?;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, s)?);
            }
            671 => {
                let mut list = view.v(2).list();
                list.lcons(mcx, Node::mk_string(mcx, view.v(1).str_val())?)?;
                *yyval = YYSTYPE::List(list);
            }
            // CreateFdwStmt / AlterFdwStmt.
            710 => {
                let mut n = Node::build::<types_nodes::rawnodes::CreateFdwStmt>(mcx)?;
                n.fdwname = Some(view.v(5).str_val());
                n.func_options = view.v(6).list();
                n.options = view.v(7).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // fdw_option: [NO] HANDLER/VALIDATOR handler_name.
            711 | 713 => {
                let name = if rule == 711 { "handler" } else { "validator" };
                let arg = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = def_elem(mcx, name, Some(arg), view.l(1))?;
            }
            712 | 714 => {
                let name = if rule == 712 { "handler" } else { "validator" };
                *yyval = def_elem(mcx, name, None, view.l(1))?;
            }
            // fdw_options: fdw_option | fdw_options fdw_option.
            715 => {
                let d = view.v(1).node().expect("fdw_option");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, d)?);
            }
            716 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("fdw_option"))?;
                *yyval = YYSTYPE::List(list);
            }
            719 | 720 => {
                let mut n = Node::build::<types_nodes::rawnodes::AlterFdwStmt>(mcx)?;
                n.fdwname = Some(view.v(5).str_val());
                n.func_options = view.v(6).list();
                n.options = if rule == 719 { view.v(7).list() } else { NodeList::nil() };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // generic_option_list: generic_option_elem [, ...].
            723 => {
                let d = view.v(1).node().expect("generic_option_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, d)?);
            }
            724 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("generic_option_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
            // CreateForeignServerStmt.
            735 | 736 => {
                let base = if rule == 735 { 0 } else { 3 };
                let mut n = Node::build::<types_nodes::rawnodes::CreateForeignServerStmt>(mcx)?;
                n.servername = Some(view.v(3 + base).str_val());
                n.servertype = opt_str(view.v(4 + base));
                n.version = opt_str(view.v(5 + base));
                n.fdwname = Some(view.v(9 + base).str_val());
                n.options = view.v(10 + base).list();
                n.if_not_exists = rule == 736;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterForeignServerStmt: name [foreign_server_version] [options].
            743 | 744 | 745 => {
                let mut n = Node::build::<types_nodes::rawnodes::AlterForeignServerStmt>(mcx)?;
                n.servername = Some(view.v(3).str_val());
                if rule != 745 {
                    n.version = opt_str(view.v(4));
                    n.has_version = true;
                }
                match rule {
                    743 => n.options = view.v(5).list(),
                    745 => n.options = view.v(4).list(),
                    _ => {}
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateForeignTableStmt, plain and PARTITION OF forms.
            746 | 747 => {
                let base = if rule == 746 { 0 } else { 3 };
                let rv = view.v(4 + base).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.relpersistence = RELPERSISTENCE_PERMANENT)
                        .expect("qualified_name is RangeVar");
                }
                let mut n = Node::build::<types_nodes::rawnodes::CreateForeignTableStmt>(mcx)?;
                n.base.relation = rv.as_variant::<RangeVar>();
                n.base.tableElts = view.v(6 + base).list();
                n.base.inhRelations = view.v(8 + base).list();
                n.base.oncommit = OnCommitAction::ONCOMMIT_NOOP;
                n.base.if_not_exists = rule == 747;
                n.servername = Some(view.v(10 + base).str_val());
                n.options = view.v(11 + base).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            748 | 749 => {
                let base = if rule == 748 { 0 } else { 3 };
                let rv = view.v(4 + base).node().expect("qualified_name");
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.relpersistence = RELPERSISTENCE_PERMANENT)
                        .expect("qualified_name is RangeVar");
                }
                let parent = view.v(7 + base).node().expect("qualified_name");
                let mut n = Node::build::<types_nodes::rawnodes::CreateForeignTableStmt>(mcx)?;
                n.base.relation = rv.as_variant::<RangeVar>();
                n.base.inhRelations = NodeList::make1(mcx, parent)?;
                n.base.tableElts = view.v(8 + base).list();
                n.base.partbound = view.v(9 + base).node();
                n.base.oncommit = OnCommitAction::ONCOMMIT_NOOP;
                n.base.if_not_exists = rule == 749;
                n.servername = Some(view.v(11 + base).str_val());
                n.options = view.v(12 + base).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // ImportForeignSchemaStmt; import_qualification rides the
            // KeyAction carrier (u8 list_type + table list).
            750 => {
                let q = view.v(5).key_action();
                let mut n = Node::build::<types_nodes::rawnodes::ImportForeignSchemaStmt>(mcx)?;
                n.server_name = Some(view.v(8).str_val());
                n.remote_schema = Some(view.v(4).str_val());
                n.local_schema = Some(view.v(10).str_val());
                n.list_type = match q.action {
                    0 => types_nodes::rawnodes::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_ALL,
                    1 => {
                        types_nodes::rawnodes::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO
                    }
                    _ => types_nodes::rawnodes::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_EXCEPT,
                };
                n.table_list = core::mem::replace(&mut q.cols, NodeList::nil());
                n.options = view.v(11).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            751 => *yyval = YYSTYPE::Ival(1),
            752 => *yyval = YYSTYPE::Ival(2),
            753 => {
                *yyval = YYSTYPE::KeyActionV(mcx::leak_in(mcx::alloc_in(
                    mcx,
                    KeyAction { action: view.v(1).ival() as u8, cols: view.v(3).list() },
                )?));
            }
            754 => {
                *yyval = YYSTYPE::KeyActionV(mcx::leak_in(mcx::alloc_in(
                    mcx,
                    KeyAction { action: 0, cols: NodeList::nil() },
                )?));
            }
            // CreateUserMappingStmt / auth_ident / DropUserMappingStmt /
            // AlterUserMappingStmt.
            755 | 756 => {
                let base = if rule == 755 { 0 } else { 3 };
                let mut n = Node::build::<types_nodes::rawnodes::CreateUserMappingStmt>(mcx)?;
                n.user = view
                    .v(5 + base)
                    .node()
                    .map(|u| u.as_role_spec().expect("auth_ident is RoleSpec"));
                n.servername = Some(view.v(7 + base).str_val());
                n.options = view.v(8 + base).list();
                n.if_not_exists = rule == 756;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            758 => {
                let mut n = Node::build::<RoleSpec>(mcx)?;
                n.roletype = RoleSpecType::ROLESPEC_CURRENT_USER;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            759 | 760 => {
                let base = if rule == 759 { 0 } else { 2 };
                let mut n = Node::build::<types_nodes::rawnodes::DropUserMappingStmt>(mcx)?;
                n.user = view
                    .v(5 + base)
                    .node()
                    .map(|u| u.as_role_spec().expect("auth_ident is RoleSpec"));
                n.servername = Some(view.v(7 + base).str_val());
                n.missing_ok = rule == 760;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            761 => {
                let mut n = Node::build::<types_nodes::rawnodes::AlterUserMappingStmt>(mcx)?;
                n.user = view
                    .v(5)
                    .node()
                    .map(|u| u.as_role_spec().expect("auth_ident is RoleSpec"));
                n.servername = Some(view.v(7).str_val());
                n.options = view.v(8).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
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
            // drop_type_name name_list | TYPE_P/DOMAIN_P type_name_list}
            // [IF EXISTS] opt_drop_behavior
            924 | 926 => {
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = if rule == 924 {
                    ObjectType::OBJECT_TYPE
                } else {
                    ObjectType::OBJECT_DOMAIN
                };
                n.objects = view.v(3).list();
                n.behavior = drop_behavior(view.v(4).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            925 | 927 => {
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = if rule == 925 {
                    ObjectType::OBJECT_TYPE
                } else {
                    ObjectType::OBJECT_DOMAIN
                };
                n.missing_ok = true;
                n.objects = view.v(5).list();
                n.behavior = drop_behavior(view.v(6).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            965 => {
                let t = view.v(1).node().expect("Typename");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            966 => {
                let mut list = view.v(1).list();
                let t = view.v(3).node().expect("Typename");
                list.lappend(mcx, t)?;
                *yyval = YYSTYPE::List(list);
            }
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
            // DropStmt: DROP INDEX CONCURRENTLY [IF_P EXISTS] any_name_list
            // opt_drop_behavior
            928 | 929 => {
                let (obj, beh) = if rule == 929 { (6, 7) } else { (4, 5) };
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = ObjectType::OBJECT_INDEX;
                n.missing_ok = rule == 929;
                n.objects = view.v(obj).list();
                n.behavior = drop_behavior(view.v(beh).ival());
                n.concurrent = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DropCastStmt: DROP CAST opt_if_exists '(' Typename AS Typename
            // ')' opt_drop_behavior
            1254 => {
                let pair = Node::mk_list(
                    mcx,
                    NodeList::make2(
                        mcx,
                        view.v(5).node().expect("Typename"),
                        view.v(7).node().expect("Typename"),
                    )?,
                )?;
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = ObjectType::OBJECT_CAST;
                n.objects = NodeList::make1(mcx, pair)?;
                n.behavior = drop_behavior(view.v(9).ival());
                n.missing_ok = view.v(3).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_if_exists: IF_P EXISTS | /*EMPTY*/
            1255 => *yyval = YYSTYPE::Boolean(true),
            1256 => *yyval = YYSTYPE::Boolean(false),
            // DropTransformStmt: DROP TRANSFORM opt_if_exists FOR Typename
            // LANGUAGE name opt_drop_behavior
            1262 => {
                let pair = Node::mk_list(
                    mcx,
                    NodeList::make2(
                        mcx,
                        view.v(5).node().expect("Typename"),
                        Node::mk_string(mcx, view.v(7).str_val())?,
                    )?,
                )?;
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = ObjectType::OBJECT_TRANSFORM;
                n.objects = NodeList::make1(mcx, pair)?;
                n.behavior = drop_behavior(view.v(8).ival());
                n.missing_ok = view.v(3).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DropStmt: DROP object_type_name_on_any_name [IF_P EXISTS] name
            // ON any_name opt_drop_behavior
            922 | 923 => {
                let (nm, any, beh) = if rule == 922 { (3, 5, 6) } else { (5, 7, 8) };
                let mut inner = view.v(any).list();
                inner.lappend(mcx, Node::mk_string(mcx, view.v(nm).str_val())?)?;
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = object_type(view.v(2).ival());
                n.objects = NodeList::make1(mcx, Node::mk_list(mcx, inner)?)?;
                n.behavior = drop_behavior(view.v(beh).ival());
                n.missing_ok = rule == 923;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreatePolicyStmt: CREATE POLICY name ON qualified_name
            // RowSecurityDefaultPermissive RowSecurityDefaultForCmd
            // RowSecurityDefaultToRole RowSecurityOptionalExpr
            // RowSecurityOptionalWithCheck
            762 => {
                let n = CreatePolicyStmt {
                    policy_name: Some(view.v(3).str_val()),
                    table: view.v(5).node().expect("qualified_name").as_variant::<RangeVar>(),
                    cmd_name: Some(view.v(7).str_val()),
                    permissive: view.v(6).boolean(),
                    roles: view.v(8).list(),
                    qual: view.v(9).node(),
                    with_check: view.v(10).node(),
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // AlterPolicyStmt: ALTER POLICY name ON qualified_name
            // RowSecurityOptionalToRole RowSecurityOptionalExpr
            // RowSecurityOptionalWithCheck
            763 => {
                let n = AlterPolicyStmt {
                    policy_name: Some(view.v(3).str_val()),
                    table: view.v(5).node().expect("qualified_name").as_variant::<RangeVar>(),
                    roles: view.v(6).list(),
                    qual: view.v(7).node(),
                    with_check: view.v(8).node(),
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // RowSecurityOptionalExpr: USING '(' a_expr ')' | EMPTY
            764 => *yyval = YYSTYPE::Node(view.v(3).node()),
            765 => *yyval = YYSTYPE::Node(None),
            // RowSecurityOptionalWithCheck: WITH CHECK '(' a_expr ')' | EMPTY
            766 => *yyval = YYSTYPE::Node(view.v(4).node()),
            767 => *yyval = YYSTYPE::Node(None),
            // RowSecurityDefaultToRole: TO role_list | EMPTY -> [PUBLIC]
            768 => *yyval = YYSTYPE::List(view.v(2).list()),
            769 => {
                let mut n = Node::build::<RoleSpec>(mcx)?;
                n.roletype = RoleSpecType::ROLESPEC_PUBLIC;
                n.location = -1;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n.seal())?);
            }
            // RowSecurityOptionalToRole: TO role_list | EMPTY -> NIL
            770 => *yyval = YYSTYPE::List(view.v(2).list()),
            771 => *yyval = YYSTYPE::List(NodeList::nil()),
            // RowSecurityDefaultPermissive: AS IDENT | EMPTY -> true
            772 => {
                let s = view.v(2).str_val();
                let v = match s {
                    "permissive" => true,
                    "restrictive" => false,
                    _ => {
                        return Err(Box::new(
                            (*self.errposition_error(
                                format!("unrecognized row security option \"{s}\""),
                                view.l(2),
                            ))
                            .with_hint(
                                "Only PERMISSIVE or RESTRICTIVE policies are supported currently.",
                            ),
                        ))
                    }
                };
                *yyval = YYSTYPE::Boolean(v);
            }
            773 => *yyval = YYSTYPE::Boolean(true),
            // RowSecurityDefaultForCmd: FOR row_security_cmd | EMPTY -> "all"
            774 => *yyval = YYSTYPE::Str(view.v(2).str_val()),
            775 => *yyval = YYSTYPE::Str("all"),
            // row_security_cmd
            776 => *yyval = YYSTYPE::Str("all"),
            777 => *yyval = YYSTYPE::Str("select"),
            778 => *yyval = YYSTYPE::Str("insert"),
            779 => *yyval = YYSTYPE::Str("update"),
            780 => *yyval = YYSTYPE::Str("delete"),
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
            // DropStmt: DROP object_type_name_on_any_name [IF_P EXISTS] name
            // ON any_name opt_drop_behavior
            922 | 923 => {
                let (nm, an, bh) = if rule == 922 { (3, 5, 6) } else { (5, 7, 8) };
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = object_type(view.v(2).ival());
                let mut any_name = view.v(an).list();
                any_name.lappend(mcx, Node::mk_string(mcx, view.v(nm).str_val())?)?;
                n.objects = NodeList::make1(mcx, Node::mk_list(mcx, any_name)?)?;
                n.behavior = drop_behavior(view.v(bh).ival());
                n.missing_ok = rule == 923;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateTrigStmt: CREATE opt_or_replace TRIGGER name
            // TriggerActionTime TriggerEvents ON qualified_name
            // TriggerReferencing TriggerForSpec TriggerWhen EXECUTE ...
            784 => {
                let mut n = Node::build::<CreateTrigStmt>(mcx)?;
                n.replace = view.v(2).boolean();
                n.trigname = Some(view.v(4).str_val());
                n.relation =
                    view.v(8).node().expect("qualified_name").as_variant::<RangeVar>();
                n.funcname = view.v(14).list();
                n.args = view.v(16).list();
                n.row = view.v(10).boolean();
                n.timing = view.v(5).ival() as i16;
                let (events, columns) = trigger_events(mcx, view.v(6))?;
                n.events = events;
                n.columns = columns;
                n.whenClause = view.v(11).node();
                n.transitionRels = view.v(9).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CONSTRAINT TRIGGER form (OptConstrFromTable + CAS bits)
            785 => {
                let mut n = Node::build::<CreateTrigStmt>(mcx)?;
                n.replace = view.v(2).boolean();
                if n.replace {
                    return Err(Box::new(
                        (*self.errposition_error(
                            "CREATE OR REPLACE CONSTRAINT TRIGGER is not supported".into(),
                            view.l(1),
                        ))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                n.isconstraint = true;
                n.trigname = Some(view.v(5).str_val());
                n.relation =
                    view.v(9).node().expect("qualified_name").as_variant::<RangeVar>();
                n.funcname = view.v(18).list();
                n.args = view.v(20).list();
                n.row = true;
                n.timing = TRIGGER_TYPE_AFTER;
                let (events, columns) = trigger_events(mcx, view.v(7))?;
                n.events = events;
                n.columns = columns;
                n.whenClause = view.v(15).node();
                let cas = self.process_cas_bits(
                    view.v(11).ival(),
                    view.l(11),
                    "TRIGGER",
                    CasTargets {
                        deferrable: true,
                        initdeferred: true,
                        is_enforced: false,
                        not_valid: false,
                        no_inherit: false },
                )?;
                n.deferrable = cas.deferrable;
                n.initdeferred = cas.initdeferred;
                n.constrrel = view
                    .v(10)
                    .node()
                    .and_then(|rv| rv.as_variant::<RangeVar>());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            786 => *yyval = YYSTYPE::Ival(TRIGGER_TYPE_BEFORE as i32),
            787 => *yyval = YYSTYPE::Ival(TRIGGER_TYPE_AFTER as i32),
            788 => *yyval = YYSTYPE::Ival(TRIGGER_TYPE_INSTEAD as i32),
            790 => {
                let (e1, c1) = trigger_events(mcx, view.v(1))?;
                let (e2, c2) = trigger_events(mcx, view.v(3))?;
                if e1 & e2 != 0 {
                    return Err(self.parser_yyerror("duplicate trigger events specified"));
                }
                let mut cols = c1;
                cols.concat(mcx, &c2)?;
                *yyval = trigger_one_event(mcx, (e1 | e2) as i32, cols)?;
            }
            791 => {
                *yyval = trigger_one_event(mcx, TRIGGER_TYPE_INSERT as i32, NodeList::nil())?
            }
            792 => {
                *yyval = trigger_one_event(mcx, TRIGGER_TYPE_DELETE as i32, NodeList::nil())?
            }
            793 => {
                *yyval = trigger_one_event(mcx, TRIGGER_TYPE_UPDATE as i32, NodeList::nil())?
            }
            794 => {
                *yyval = trigger_one_event(mcx, TRIGGER_TYPE_UPDATE as i32, view.v(3).list())?
            }
            795 => {
                *yyval = trigger_one_event(mcx, TRIGGER_TYPE_TRUNCATE as i32, NodeList::nil())?
            }
            798 => {
                let t = view.v(1).node().expect("TriggerTransition");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, t)?);
            }
            799 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("TriggerTransition"))?;
                *yyval = YYSTYPE::List(list);
            }
            // TriggerTransition: OldOrNew RowOrTable opt_as RelName
            800 => {
                let mut n = Node::build::<TriggerTransition>(mcx)?;
                n.name = Some(view.v(4).str_val());
                n.isNew = view.v(1).boolean();
                n.isTable = view.v(2).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            801 | 803 => *yyval = YYSTYPE::Boolean(true),
            802 | 804 => *yyval = YYSTYPE::Boolean(false),
            807 => *yyval = YYSTYPE::Boolean(false),
            810 => *yyval = YYSTYPE::Boolean(true),
            811 => *yyval = YYSTYPE::Boolean(false),
            816 => {
                let a = view.v(1).node().expect("TriggerFuncArg");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, a)?);
            }
            817 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("TriggerFuncArg"))?;
                *yyval = YYSTYPE::List(list);
            }
            819 => {
                let s = arena_int_str(mcx, view.v(1).ival())?;
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, s)?));
            }
            820..=822 => {
                *yyval =
                    YYSTYPE::Node(Some(Node::mk_string(mcx, view.v(1).str_val())?));
            }
            // ConstraintsSetStmt: SET CONSTRAINTS list mode
            264 => {
                let mut n = Node::build::<ConstraintsSetStmt>(mcx)?;
                n.constraints = view.v(3).list();
                n.deferred = view.v(4).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            265 => *yyval = YYSTYPE::List(NodeList::nil()),
            267 => *yyval = YYSTYPE::Boolean(true),
            268 => *yyval = YYSTYPE::Boolean(false),
            // RenameStmt: ALTER TABLESPACE name RENAME TO name
            1321 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_TABLESPACE;
                n.subname = Some(view.v(3).str_val());
                n.newname = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER TABLESPACE name OWNER TO RoleSpec
            1395 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = ObjectType::OBJECT_TABLESPACE;
                n.object = Some(Node::mk_string(mcx, view.v(3).str_val())?);
                n.newowner = view.v(6).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER TRIGGER name ON qualified_name RENAME TO name
            1317 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_TRIGGER;
                n.relation =
                    view.v(5).node().expect("qualified_name").as_variant::<RangeVar>();
                n.subname = Some(view.v(3).str_val());
                n.newname = Some(view.v(8).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateSchemaStmt: CREATE SCHEMA [IF NOT EXISTS]
            //   {ColId | opt_single_name AUTHORIZATION RoleSpec} OptSchemaEltList
            189..=192 => {
                let (name, role, elts) = match rule {
                    189 => (3, Some(5), 6),
                    190 => (3, None, 4),
                    191 => (6, Some(8), 9),
                    _ => (6, None, 7),
                };
                let mut n = Node::build::<CreateSchemaStmt>(mcx)?;
                n.schemaname = opt_str(view.v(name));
                n.authrole = role.and_then(|i| view.v(i).node());
                n.schemaElts = view.v(elts).list();
                n.if_not_exists = rule >= 191;
                if n.if_not_exists && !n.schemaElts.is_nil() {
                    return Err(Box::new(
                        (*self.errposition_error(
                            "CREATE SCHEMA IF NOT EXISTS cannot include schema elements"
                                .into(),
                            view.l(elts),
                        ))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // OptSchemaEltList: OptSchemaEltList schema_stmt
            193 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("schema_stmt"))?;
                *yyval = YYSTYPE::List(list);
            }
            967 => {
                let mut n = Node::build::<TruncateStmt>(mcx)?;
                n.relations = view.v(3).list();
                n.restart_seqs = view.v(4).boolean();
                n.behavior = drop_behavior(view.v(5).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CreateFunctionStmt: RETURNS TABLE (mergeTableFuncParameters +
            // TableFuncTypeName inline).
            1127 => {
                let mut n = Node::build::<CreateFunctionStmt>(mcx)?;
                n.is_procedure = false;
                n.replace = view.v(2).boolean();
                n.funcname = view.v(4).list();
                let mut params = view.v(5).list();
                let columns = view.v(9).list();
                for p in &params {
                    let fp = p
                        .as_variant::<FunctionParameter>()
                        .expect("func_args_with_defaults cell");
                    match fp.mode {
                        FunctionParameterMode::FUNC_PARAM_DEFAULT
                        | FunctionParameterMode::FUNC_PARAM_IN
                        | FunctionParameterMode::FUNC_PARAM_VARIADIC => {}
                        _ => {
                            return Err(self.errposition_error(
                                "OUT and INOUT arguments aren't allowed in TABLE functions"
                                    .into(),
                                fp.location,
                            ))
                        }
                    }
                }
                for c in &columns {
                    params.lappend(mcx, c)?;
                }
                n.parameters = params;
                let mut t = Node::build::<TypeName>(mcx)?;
                if columns.len() == 1 {
                    let p = columns
                        .nth(0)
                        .as_variant::<FunctionParameter>()
                        .expect("table_func_column");
                    let src = p
                        .argType
                        .expect("table_func_column has a type")
                        .as_type_name()
                        .expect("TypeName");
                    t.names = src.names.clone_in(mcx)?;
                    t.typeOid = src.typeOid;
                    t.pct_type = src.pct_type;
                    t.typmods = src.typmods.clone_in(mcx)?;
                    t.typemod = src.typemod;
                    t.arrayBounds = src.arrayBounds.clone_in(mcx)?;
                } else {
                    t.names = NodeList::make2(
                        mcx,
                        Node::mk_string(mcx, "pg_catalog")?,
                        Node::mk_string(mcx, "record")?,
                    )?;
                    t.typemod = -1;
                }
                t.setof = true;
                t.location = view.l(7);
                n.returnType = Some(t.seal());
                n.options = view.v(11).list();
                n.sql_body = view.v(12).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
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
            // ReturnStmt: RETURN a_expr
            1202 => {
                let mut n = Node::build::<parsenodes::ReturnStmt>(mcx)?;
                n.returnval = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opt_routine_body: BEGIN_P ATOMIC routine_body_stmt_list END_P —
            // the compound body is a single-item list wrapping the stmt list.
            1204 => {
                let body = Node::mk_list(mcx, view.v(3).list())?;
                *yyval =
                    YYSTYPE::Node(Some(Node::mk_list(mcx, NodeList::make1(mcx, body)?)?));
            }
            // routine_body_stmt_list: empty statements are discarded as in
            // stmtmulti.
            1206 => {
                let mut list = view.v(1).list();
                if let Some(s) = view.v(2).node() {
                    list.lappend(mcx, s)?;
                }
                *yyval = YYSTYPE::List(list);
            }
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
            // table_func_column: param_name func_type
            1214 => {
                let mut n = Node::build::<FunctionParameter>(mcx)?;
                n.name = Some(view.v(1).str_val());
                n.argType = view.v(2).node();
                n.mode = FunctionParameterMode::FUNC_PARAM_TABLE;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1215 => {
                let el = view.v(1).node().expect("table_func_column");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1216 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("table_func_column"))?;
                *yyval = YYSTYPE::List(list);
            }
            1151 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_IN as i32),
            1152 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_OUT as i32),
            1153 | 1154 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_INOUT as i32),
            1155 => *yyval = YYSTYPE::Ival(FunctionParameterMode::FUNC_PARAM_VARIADIC as i32),
            1157 => *yyval = view.v(1),
            // func_type: [SETOF] type_function_name attrs '%' TYPE_P.
            1159 | 1160 => {
                let off: usize = if rule == 1160 { 1 } else { 0 };
                let name = view.v(1 + off).str_val();
                let mut names = view.v(2 + off).list();
                names.lcons(mcx, Node::mk_string(mcx, name)?)?;
                let t = make_type_name(mcx, names, NodeList::nil(), view.l(1 + off))?;
                // SAFETY: tree is parser-owned; no derived refs live.
                unsafe {
                    t.with_mut::<TypeName, _>(|tn| {
                        tn.pct_type = true;
                        tn.setof = rule == 1160;
                    })
                    .expect("TypeName");
                }
                *yyval = YYSTYPE::Node(Some(t));
            }
            // func_arg_with_default DEFAULT/'=' a_expr.
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
            // aggr_arg / aggr_args; aggr_args' C shape is
            // (arglist, Integer numdirect); NIL rides as an empty List node.
            1164 => {
                let param = view.v(1).node().expect("func_arg");
                let mode = param
                    .as_variant::<FunctionParameter>()
                    .expect("func_arg is FunctionParameter")
                    .mode;
                if !matches!(
                    mode,
                    FunctionParameterMode::FUNC_PARAM_DEFAULT
                        | FunctionParameterMode::FUNC_PARAM_IN
                        | FunctionParameterMode::FUNC_PARAM_VARIADIC
                ) {
                    return Err(Box::new(
                        (*self.errposition_error(
                            "aggregates cannot have output arguments".into(),
                            view.l(1),
                        ))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                *yyval = YYSTYPE::Node(Some(param));
            }
            1169 => {
                let el = view.v(1).node().expect("aggr_arg");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1170 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("aggr_arg"))?;
                *yyval = YYSTYPE::List(list);
            }
            1165..=1167 => {
                let (args, numdirect) = match rule {
                    1165 => (NodeList::nil(), -1),
                    1166 => (view.v(2).list(), -1),
                    _ => (view.v(4).list(), 0),
                };
                let mut pair = NodeList::make1(mcx, Node::mk_list(mcx, args)?)?;
                pair.lappend(mcx, Node::mk_integer(mcx, numdirect)?)?;
                *yyval = YYSTYPE::List(pair);
            }
            // makeOrderedSetArgs: a trailing VARIADIC direct arg needs a lone
            // matching VARIADIC aggregated arg, dropped from the internal form.
            1168 => {
                let mut direct = view.v(2).list();
                let mut ordered = view.v(5).list();
                let lastd = direct
                    .last()
                    .and_then(|n| n.as_function_parameter())
                    .expect("FunctionParameter");
                if lastd.mode == FunctionParameterMode::FUNC_PARAM_VARIADIC {
                    let firsto = ordered
                        .first()
                        .and_then(|n| n.as_function_parameter())
                        .expect("FunctionParameter");
                    if ordered.len() != 1
                        || firsto.mode != FunctionParameterMode::FUNC_PARAM_VARIADIC
                        || !types_nodes::equal_opt(lastd.argType, firsto.argType)
                    {
                        return Err(self.errposition_error_code(
                            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                            "an ordered-set aggregate with a VARIADIC direct argument \
                             must have one VARIADIC aggregated argument of the same data type"
                                .into(),
                            firsto.location,
                        ));
                    }
                    ordered = NodeList::nil();
                }
                let ndirect = direct.len() as i32;
                direct.concat(mcx, &ordered)?;
                let mut pair = NodeList::make1(mcx, Node::mk_list(mcx, direct)?)?;
                pair.lappend(mcx, Node::mk_integer(mcx, ndirect)?)?;
                *yyval = YYSTYPE::List(pair);
            }
            // aggregate_with_argtypes: func_name aggr_args
            1171 => {
                let pair = view.v(2).list();
                let fargs = pair
                    .first()
                    .and_then(|n| n.as_list())
                    .expect("aggr_args carries the arg list first");
                let mut owa = Node::build::<parsenodes::ObjectWithArgs>(mcx)?;
                owa.objname = view.v(1).list();
                owa.objargs = extract_arg_types(mcx, fargs)?;
                for cell in fargs {
                    owa.objfuncargs.lappend(mcx, cell)?;
                }
                *yyval = YYSTYPE::Node(Some(owa.seal()));
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
            // createfunc_opt_item
            1195 => {
                let arg = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = def_elem(mcx, "as", Some(arg), view.l(1))?;
            }
            1196 => {
                let arg = Node::mk_string(mcx, view.v(2).str_val())?;
                *yyval = def_elem(mcx, "language", Some(arg), view.l(1))?;
            }
            // createfunc_opt_item: TRANSFORM transform_type_list
            1197 => {
                let arg = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = def_elem(mcx, "transform", Some(arg), view.l(1))?;
            }
            1198 => {
                *yyval = def_elem(mcx, "window", Some(Node::mk_boolean(mcx, true)?), view.l(1))?;
            }
            // transform_type_list: FOR TYPE_P Typename
            //                    | transform_type_list ',' FOR TYPE_P Typename
            1210 => {
                *yyval =
                    YYSTYPE::List(NodeList::make1(mcx, view.v(3).node().expect("Typename"))?);
            }
            1211 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(5).node().expect("Typename"))?;
                *yyval = YYSTYPE::List(list);
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
            // func_args_list / function_with_argtypes_list / aggr_args_list /
            // aggregate_with_argtypes_list: `el | list ',' el`.
            1134 | 1136 | 1169 | 1172 => {
                let el = view.v(1).node().expect("list element");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1135 | 1137 | 1170 | 1173 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("list element"))?;
                *yyval = YYSTYPE::List(list);
            }
            1138 => {
                let args = view.v(2).list();
                let mut n = Node::build::<ObjectWithArgs>(mcx)?;
                n.objname = view.v(1).list();
                n.objargs = extract_arg_types(mcx, &args)?;
                n.objfuncargs = args;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1139 | 1140 => {
                let mut n = Node::build::<ObjectWithArgs>(mcx)?;
                n.objname = NodeList::make1(mcx, Node::mk_string(mcx, view.v(1).str_val())?)?;
                n.args_unspecified = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1141 => {
                let name = view.v(1).str_val();
                let mut list = view.v(2).list();
                for e in &list {
                    if e.as_string().is_none() {
                        return Err(self.parser_yyerror("syntax error"));
                    }
                }
                list.lcons(mcx, Node::mk_string(mcx, name)?)?;
                let mut n = Node::build::<ObjectWithArgs>(mcx)?;
                n.objname = list;
                n.args_unspecified = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1164 => {
                let p = view.v(1).node().expect("func_arg");
                let mode = p.as_function_parameter().expect("FunctionParameter").mode;
                if !matches!(
                    mode,
                    FunctionParameterMode::FUNC_PARAM_DEFAULT
                        | FunctionParameterMode::FUNC_PARAM_IN
                        | FunctionParameterMode::FUNC_PARAM_VARIADIC
                ) {
                    return Err(self.errposition_error_code(
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        "aggregates cannot have output arguments".into(),
                        view.l(1),
                    ));
                }
                *yyval = YYSTYPE::Node(Some(p));
            }
            // aggr_args carrier: [args sublist, numdirectargs Integer].
            1165..=1167 => {
                let sub = match rule {
                    1165 => NodeList::nil(),
                    1166 => view.v(2).list(),
                    _ => view.v(4).list(),
                };
                let ndirect = if rule == 1167 { 0 } else { -1 };
                let mut list = NodeList::make1(mcx, Node::mk_list(mcx, sub)?)?;
                list.lappend(mcx, Node::mk_integer(mcx, ndirect)?)?;
                *yyval = YYSTYPE::List(list);
            }
            // aggregate_with_argtypes: objfuncargs = linitial(aggr_args).
            1171 => {
                let aggr = view.v(2).list();
                let sub = aggr.as_slice()[0];
                // SAFETY: parser-owned carrier; no derived refs live.
                let params = unsafe { sub.with_mut::<NodeList, _>(core::mem::take) }
                    .expect("aggr_args sublist");
                let mut n = Node::build::<ObjectWithArgs>(mcx)?;
                n.objname = view.v(1).list();
                n.objargs = extract_arg_types(mcx, &params)?;
                n.objfuncargs = params;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1217..=1219 => {
                let mut n = Node::build::<AlterFunctionStmt>(mcx)?;
                n.objtype = match rule {
                    1217 => ObjectType::OBJECT_FUNCTION,
                    1218 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.func = view.v(3).node().and_then(|o| o.as_object_with_args());
                n.actions = view.v(4).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1220 => {
                let el = view.v(1).node().expect("common_func_opt_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1221 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("common_func_opt_item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // RemoveFuncStmt / RemoveAggrStmt (operator forms 1232-1242 stay
            // loud with the rest of DROP/ALTER OPERATOR: operator lane).
            1224 | 1226 | 1228 | 1230 => {
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = match rule {
                    1224 => ObjectType::OBJECT_FUNCTION,
                    1226 => ObjectType::OBJECT_PROCEDURE,
                    1228 => ObjectType::OBJECT_ROUTINE,
                    _ => ObjectType::OBJECT_AGGREGATE,
                };
                n.objects = view.v(3).list();
                n.behavior = drop_behavior(view.v(4).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1225 | 1227 | 1229 | 1231 => {
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = match rule {
                    1225 => ObjectType::OBJECT_FUNCTION,
                    1227 => ObjectType::OBJECT_PROCEDURE,
                    1229 => ObjectType::OBJECT_ROUTINE,
                    _ => ObjectType::OBJECT_AGGREGATE,
                };
                n.missing_ok = true;
                n.objects = view.v(5).list();
                n.behavior = drop_behavior(view.v(6).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CommentStmt objwithargs forms (OPERATOR 978 stays loud).
            976 | 977 | 982 | 983 => {
                let mut n = Node::build::<CommentStmt>(mcx)?;
                n.objtype = match rule {
                    976 => ObjectType::OBJECT_AGGREGATE,
                    977 => ObjectType::OBJECT_FUNCTION,
                    982 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.object = view.v(4).node();
                let c = view.v(6);
                n.comment = if c.is_null_node() { None } else { Some(c.str_val()) };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER DOMAIN_P any_name RENAME TO name
            1278 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_DOMAIN;
                n.object = Some(Node::mk_list(mcx, view.v(3).list())?);
                n.newname = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER DOMAIN_P any_name RENAME CONSTRAINT name TO name
            1279 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_DOMCONSTRAINT;
                n.object = Some(Node::mk_list(mcx, view.v(3).list())?);
                n.subname = Some(view.v(6).str_val());
                n.newname = Some(view.v(8).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterObjectSchemaStmt: ALTER DOMAIN_P any_name SET SCHEMA name
            1344 => {
                let mut n = Node::build::<parsenodes::AlterObjectSchemaStmt>(mcx)?;
                n.objectType = ObjectType::OBJECT_DOMAIN;
                n.object = Some(Node::mk_list(mcx, view.v(3).list())?);
                n.newschema = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER DOMAIN_P any_name OWNER TO RoleSpec
            1384 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = ObjectType::OBJECT_DOMAIN;
                n.object = Some(Node::mk_list(mcx, view.v(3).list())?);
                n.newowner = view.v(6).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1274 | 1281 | 1288 | 1290 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = match rule {
                    1274 => ObjectType::OBJECT_AGGREGATE,
                    1281 => ObjectType::OBJECT_FUNCTION,
                    1288 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.object = view.v(3).node();
                n.newname = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER {COLLATION|CONVERSION|STATISTICS|TYPE} any_name
            // RENAME TO name
            1275 | 1276 | 1322 | 1327 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = match rule {
                    1275 => ObjectType::OBJECT_COLLATION,
                    1276 => ObjectType::OBJECT_CONVERSION,
                    1322 => ObjectType::OBJECT_STATISTIC_EXT,
                    _ => ObjectType::OBJECT_TYPE,
                };
                n.object = Some(Node::mk_list(mcx, view.v(3).list())?);
                n.newname = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER TEXT_P SEARCH {PARSER|DICTIONARY|TEMPLATE|
            // CONFIGURATION} any_name RENAME TO name
            1323..=1326 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = match rule {
                    1323 => ObjectType::OBJECT_TSPARSER,
                    1324 => ObjectType::OBJECT_TSDICTIONARY,
                    1325 => ObjectType::OBJECT_TSTEMPLATE,
                    _ => ObjectType::OBJECT_TSCONFIGURATION,
                };
                n.object = Some(Node::mk_list(mcx, view.v(5).list())?);
                n.newname = Some(view.v(8).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER {FDW|LANGUAGE|SERVER|EVENT TRIGGER} name
            // RENAME TO name
            1280 | 1283 | 1292 | 1318 => {
                let (obj, nm, ty) = match rule {
                    1280 => (5, 8, ObjectType::OBJECT_FDW),
                    1283 => (4, 7, ObjectType::OBJECT_LANGUAGE),
                    1292 => (3, 6, ObjectType::OBJECT_FOREIGN_SERVER),
                    _ => (4, 7, ObjectType::OBJECT_EVENT_TRIGGER),
                };
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ty;
                n.object = Some(Node::mk_string(mcx, view.v(obj).str_val())?);
                n.newname = Some(view.v(nm).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER OPERATOR {CLASS|FAMILY} any_name USING name
            // RENAME TO name
            1284 | 1285 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = if rule == 1284 {
                    ObjectType::OBJECT_OPCLASS
                } else {
                    ObjectType::OBJECT_OPFAMILY
                };
                let mut names = view.v(4).list();
                names.lcons(mcx, Node::mk_string(mcx, view.v(6).str_val())?)?;
                n.object = Some(Node::mk_list(mcx, names)?);
                n.newname = Some(view.v(9).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER {GROUP_P|ROLE|USER} RoleId RENAME TO RoleId;
            // ALTER SCHEMA name RENAME TO name
            1282 | 1291 | 1319 | 1320 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = if rule == 1291 {
                    ObjectType::OBJECT_SCHEMA
                } else {
                    ObjectType::OBJECT_ROLE
                };
                n.subname = Some(view.v(3).str_val());
                n.newname = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER {SEQUENCE|VIEW|INDEX|MATERIALIZED VIEW|FOREIGN
            // TABLE} [IF_P EXISTS] rel RENAME TO name
            1296..=1305 => {
                let (rv, nm, ty, mok) = match rule {
                    1296 => (3, 6, ObjectType::OBJECT_SEQUENCE, false),
                    1297 => (5, 8, ObjectType::OBJECT_SEQUENCE, true),
                    1298 => (3, 6, ObjectType::OBJECT_VIEW, false),
                    1299 => (5, 8, ObjectType::OBJECT_VIEW, true),
                    1300 => (4, 7, ObjectType::OBJECT_MATVIEW, false),
                    1301 => (6, 9, ObjectType::OBJECT_MATVIEW, true),
                    1302 => (3, 6, ObjectType::OBJECT_INDEX, false),
                    1303 => (5, 8, ObjectType::OBJECT_INDEX, true),
                    1304 => (4, 7, ObjectType::OBJECT_FOREIGN_TABLE, false),
                    _ => (6, 9, ObjectType::OBJECT_FOREIGN_TABLE, true),
                };
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ty;
                n.relation =
                    view.v(rv).node().expect("qualified_name").as_variant::<RangeVar>();
                n.newname = Some(view.v(nm).str_val());
                n.missing_ok = mok;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER {VIEW|MATERIALIZED VIEW|FOREIGN TABLE}
            // [IF_P EXISTS] rel RENAME opt_column name TO name
            1308..=1311 | 1314 | 1315 => {
                let (rv, sub, nm, ty, mok) = match rule {
                    1308 => (3, 6, 8, ObjectType::OBJECT_VIEW, false),
                    1309 => (5, 8, 10, ObjectType::OBJECT_VIEW, true),
                    1310 => (4, 7, 9, ObjectType::OBJECT_MATVIEW, false),
                    1311 => (6, 9, 11, ObjectType::OBJECT_MATVIEW, true),
                    1314 => (4, 7, 9, ObjectType::OBJECT_FOREIGN_TABLE, false),
                    _ => (6, 9, 11, ObjectType::OBJECT_FOREIGN_TABLE, true),
                };
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_COLUMN;
                n.relationType = ty;
                n.relation =
                    view.v(rv).node().expect("qualified_name").as_variant::<RangeVar>();
                n.subname = Some(view.v(sub).str_val());
                n.newname = Some(view.v(nm).str_val());
                n.missing_ok = mok;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER RULE name ON qualified_name RENAME TO name
            1316 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_RULE;
                n.relation =
                    view.v(5).node().expect("qualified_name").as_variant::<RangeVar>();
                n.subname = Some(view.v(3).str_val());
                n.newname = Some(view.v(8).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER TYPE_P any_name RENAME ATTRIBUTE name TO name
            // opt_drop_behavior
            1328 => {
                let names = view.v(3).list();
                let mut it = names.iter();
                let (c, s, r) = match names.len() {
                    1 => (None, None, it.next()),
                    2 => (None, it.next(), it.next()),
                    3 => (it.next(), it.next(), it.next()),
                    _ => return Err(self.improper_qualified_name(None, &names, view.l(3))),
                };
                let sval =
                    |n: Option<Node<'mcx>>| n.and_then(|n| n.as_string()).map(|s| s.sval);
                // makeRangeVarFromAnyName: makeNode zero-fill => inh false.
                let rv = Node::mk(
                    mcx,
                    RangeVar {
                        catalogname: sval(c),
                        schemaname: sval(s),
                        relname: sval(r),
                        inh: false,
                        relpersistence: RELPERSISTENCE_PERMANENT,
                        alias: None,
                        location: view.l(3),
                    },
                )?;
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_ATTRIBUTE;
                n.relationType = ObjectType::OBJECT_TYPE;
                n.relation = rv.as_range_var();
                n.subname = Some(view.v(6).str_val());
                n.newname = Some(view.v(8).str_val());
                n.behavior = drop_behavior(view.v(9).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterObjectSchemaStmt: ALTER {AGGREGATE|FUNCTION|OPERATOR|
            // PROCEDURE|ROUTINE} with_argtypes SET SCHEMA name
            1341 | 1346 | 1347 | 1350 | 1351 => {
                let mut n = Node::build::<parsenodes::AlterObjectSchemaStmt>(mcx)?;
                n.objectType = match rule {
                    1341 => ObjectType::OBJECT_AGGREGATE,
                    1346 => ObjectType::OBJECT_FUNCTION,
                    1347 => ObjectType::OBJECT_OPERATOR,
                    1350 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.object = view.v(3).node();
                n.newschema = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterObjectSchemaStmt: ALTER {COLLATION|CONVERSION|STATISTICS|
            // TYPE} any_name SET SCHEMA name
            1342 | 1343 | 1354 | 1367 => {
                let mut n = Node::build::<parsenodes::AlterObjectSchemaStmt>(mcx)?;
                n.objectType = match rule {
                    1342 => ObjectType::OBJECT_COLLATION,
                    1343 => ObjectType::OBJECT_CONVERSION,
                    1354 => ObjectType::OBJECT_STATISTIC_EXT,
                    _ => ObjectType::OBJECT_TYPE,
                };
                n.object = Some(Node::mk_list(mcx, view.v(3).list())?);
                n.newschema = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterObjectSchemaStmt: ALTER EXTENSION name SET SCHEMA name
            1345 => {
                let mut n = Node::build::<parsenodes::AlterObjectSchemaStmt>(mcx)?;
                n.objectType = ObjectType::OBJECT_EXTENSION;
                n.object = Some(Node::mk_string(mcx, view.v(3).str_val())?);
                n.newschema = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterObjectSchemaStmt: ALTER OPERATOR {CLASS|FAMILY} any_name
            // USING name SET SCHEMA name
            1348 | 1349 => {
                let mut n = Node::build::<parsenodes::AlterObjectSchemaStmt>(mcx)?;
                n.objectType = if rule == 1348 {
                    ObjectType::OBJECT_OPCLASS
                } else {
                    ObjectType::OBJECT_OPFAMILY
                };
                let mut names = view.v(4).list();
                names.lcons(mcx, Node::mk_string(mcx, view.v(6).str_val())?)?;
                n.object = Some(Node::mk_list(mcx, names)?);
                n.newschema = Some(view.v(9).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterObjectSchemaStmt: ALTER TEXT_P SEARCH {PARSER|DICTIONARY|
            // TEMPLATE|CONFIGURATION} any_name SET SCHEMA name
            1355..=1358 => {
                let mut n = Node::build::<parsenodes::AlterObjectSchemaStmt>(mcx)?;
                n.objectType = match rule {
                    1355 => ObjectType::OBJECT_TSPARSER,
                    1356 => ObjectType::OBJECT_TSDICTIONARY,
                    1357 => ObjectType::OBJECT_TSTEMPLATE,
                    _ => ObjectType::OBJECT_TSCONFIGURATION,
                };
                n.object = Some(Node::mk_list(mcx, view.v(5).list())?);
                n.newschema = Some(view.v(8).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterObjectSchemaStmt: ALTER {TABLE|SEQUENCE|VIEW|MATERIALIZED
            // VIEW|FOREIGN TABLE} [IF_P EXISTS] rel SET SCHEMA name
            1352 | 1353 | 1359..=1366 => {
                let (rv, ns, ty, mok) = match rule {
                    1352 => (3, 6, ObjectType::OBJECT_TABLE, false),
                    1353 => (5, 8, ObjectType::OBJECT_TABLE, true),
                    1359 => (3, 6, ObjectType::OBJECT_SEQUENCE, false),
                    1360 => (5, 8, ObjectType::OBJECT_SEQUENCE, true),
                    1361 => (3, 6, ObjectType::OBJECT_VIEW, false),
                    1362 => (5, 8, ObjectType::OBJECT_VIEW, true),
                    1363 => (4, 7, ObjectType::OBJECT_MATVIEW, false),
                    1364 => (6, 9, ObjectType::OBJECT_MATVIEW, true),
                    1365 => (4, 7, ObjectType::OBJECT_FOREIGN_TABLE, false),
                    _ => (6, 9, ObjectType::OBJECT_FOREIGN_TABLE, true),
                };
                let mut n = Node::build::<parsenodes::AlterObjectSchemaStmt>(mcx)?;
                n.objectType = ty;
                n.relation =
                    view.v(rv).node().expect("qualified_name").as_variant::<RangeVar>();
                n.newschema = Some(view.v(ns).str_val());
                n.missing_ok = mok;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER {COLLATION|CONVERSION|TYPE|STATISTICS}
            // any_name OWNER TO RoleSpec
            1381 | 1382 | 1394 | 1396 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = match rule {
                    1381 => ObjectType::OBJECT_COLLATION,
                    1382 => ObjectType::OBJECT_CONVERSION,
                    1394 => ObjectType::OBJECT_TYPE,
                    _ => ObjectType::OBJECT_STATISTIC_EXT,
                };
                n.object = Some(Node::mk_list(mcx, view.v(3).list())?);
                n.newowner = view.v(6).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER {LANGUAGE|SCHEMA|FDW|SERVER|EVENT TRIGGER}
            // name OWNER TO RoleSpec
            1386 | 1393 | 1399 | 1400 | 1401 => {
                let (obj, own, ty) = match rule {
                    1386 => (4, 7, ObjectType::OBJECT_LANGUAGE),
                    1393 => (3, 6, ObjectType::OBJECT_SCHEMA),
                    1399 => (5, 8, ObjectType::OBJECT_FDW),
                    1400 => (3, 6, ObjectType::OBJECT_FOREIGN_SERVER),
                    _ => (4, 7, ObjectType::OBJECT_EVENT_TRIGGER),
                };
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = ty;
                n.object = Some(Node::mk_string(mcx, view.v(obj).str_val())?);
                n.newowner =
                    view.v(own).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER LARGE_P OBJECT_P NumericOnly OWNER TO
            // RoleSpec; ALTER OPERATOR operator_with_argtypes OWNER TO RoleSpec
            1387 | 1388 => {
                let (obj, own) = if rule == 1387 { (4, 7) } else { (3, 6) };
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = if rule == 1387 {
                    ObjectType::OBJECT_LARGEOBJECT
                } else {
                    ObjectType::OBJECT_OPERATOR
                };
                n.object = view.v(obj).node();
                n.newowner =
                    view.v(own).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER OPERATOR {CLASS|FAMILY} any_name USING
            // name OWNER TO RoleSpec
            1389 | 1390 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = if rule == 1389 {
                    ObjectType::OBJECT_OPCLASS
                } else {
                    ObjectType::OBJECT_OPFAMILY
                };
                let mut names = view.v(4).list();
                names.lcons(mcx, Node::mk_string(mcx, view.v(6).str_val())?)?;
                n.object = Some(Node::mk_list(mcx, names)?);
                n.newowner = view.v(9).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER TEXT_P SEARCH {DICTIONARY|CONFIGURATION}
            // any_name OWNER TO RoleSpec
            1397 | 1398 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = if rule == 1397 {
                    ObjectType::OBJECT_TSDICTIONARY
                } else {
                    ObjectType::OBJECT_TSCONFIGURATION
                };
                n.object = Some(Node::mk_list(mcx, view.v(5).list())?);
                n.newowner = view.v(8).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER DATABASE name RENAME TO name
            1277 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = ObjectType::OBJECT_DATABASE;
                n.subname = Some(view.v(3).str_val());
                n.newname = Some(view.v(6).str_val());
                n.missing_ok = false;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER DATABASE name OWNER TO RoleSpec
            1383 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = ObjectType::OBJECT_DATABASE;
                n.object = Some(Node::mk_string(mcx, view.v(3).str_val())?);
                n.newowner = view.v(6).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RenameStmt: ALTER PUBLICATION/SUBSCRIPTION name RENAME TO name
            1289 | 1293 => {
                let mut n = Node::build::<RenameStmt>(mcx)?;
                n.renameType = if rule == 1289 {
                    ObjectType::OBJECT_PUBLICATION
                } else {
                    ObjectType::OBJECT_SUBSCRIPTION
                };
                n.object = Some(Node::mk_string(mcx, view.v(3).str_val())?);
                n.newname = Some(view.v(6).str_val());
                n.missing_ok = false;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt: ALTER PUBLICATION/SUBSCRIPTION name OWNER TO RoleSpec
            1402 | 1403 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = if rule == 1402 {
                    ObjectType::OBJECT_PUBLICATION
                } else {
                    ObjectType::OBJECT_SUBSCRIPTION
                };
                n.object = Some(Node::mk_string(mcx, view.v(3).str_val())?);
                n.newowner = view.v(6).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOwnerStmt objwithargs forms (OPERATOR 1388 stays loud).
            1380 | 1385 | 1391 | 1392 => {
                let mut n = Node::build::<AlterOwnerStmt>(mcx)?;
                n.objectType = match rule {
                    1380 => ObjectType::OBJECT_AGGREGATE,
                    1385 => ObjectType::OBJECT_FUNCTION,
                    1391 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.object = view.v(3).node();
                n.newowner = view.v(6).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
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
            // privilege_target FUNCTION/PROCEDURE/ROUTINE forms.
            1051..=1053 => {
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.targtype = GrantTargetType::ACL_TARGET_OBJECT;
                n.objtype = match rule {
                    1051 => ObjectType::OBJECT_FUNCTION,
                    1052 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.objects = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // privilege_target FOREIGN DATA WRAPPER name_list / FOREIGN SERVER
            // name_list.
            1049 | 1050 => {
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.targtype = GrantTargetType::ACL_TARGET_OBJECT;
                n.objtype = if rule == 1049 {
                    ObjectType::OBJECT_FDW
                } else {
                    ObjectType::OBJECT_FOREIGN_SERVER
                };
                n.objects = view.v(if rule == 1049 { 4 } else { 3 }).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // privilege_target DATABASE/DOMAIN/LANGUAGE/LARGE OBJECT/SCHEMA/
            // TABLESPACE/TYPE forms.
            1054 | 1055 | 1056 | 1057 | 1059 | 1060 | 1061 => {
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.targtype = GrantTargetType::ACL_TARGET_OBJECT;
                n.objtype = match rule {
                    1054 => ObjectType::OBJECT_DATABASE,
                    1055 => ObjectType::OBJECT_DOMAIN,
                    1056 => ObjectType::OBJECT_LANGUAGE,
                    1057 => ObjectType::OBJECT_LARGEOBJECT,
                    1059 => ObjectType::OBJECT_SCHEMA,
                    1060 => ObjectType::OBJECT_TABLESPACE,
                    _ => ObjectType::OBJECT_TYPE,
                };
                n.objects = view.v(if rule == 1057 { 3 } else { 2 }).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1058 => {
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.targtype = GrantTargetType::ACL_TARGET_OBJECT;
                n.objtype = ObjectType::OBJECT_PARAMETER_ACL;
                n.objects = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // parameter_name_list / dotted parameter_name (plain-ColId 1044
            // rides DISPATCH).
            1042 => {
                let n = Node::mk_string(mcx, view.v(1).str_val())?;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            1043 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, Node::mk_string(mcx, view.v(3).str_val())?)?;
                *yyval = YYSTYPE::List(list);
            }
            1045 => {
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
            // NumericOnly_list.
            664 => {
                let n = view.v(1).node().expect("NumericOnly");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            665 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("NumericOnly"))?;
                *yyval = YYSTYPE::List(list);
            }
            1062..=1066 => {
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.targtype = GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA;
                n.objtype = match rule {
                    1062 => ObjectType::OBJECT_TABLE,
                    1063 => ObjectType::OBJECT_SEQUENCE,
                    1064 => ObjectType::OBJECT_FUNCTION,
                    1065 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.objects = view.v(5).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // privilege_target: [TABLE] qualified_name_list | SEQUENCE ...
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
            // AlterDefaultPrivilegesStmt: ALTER DEFAULT PRIVILEGES
            // DefACLOptionList DefACLAction.
            1085 => {
                let mut n = Node::build::<AlterDefaultPrivilegesStmt>(mcx)?;
                n.options = view.v(4).list();
                n.action = view.v(5).node().map(|a| a.as_grant_stmt().expect("DefACLAction"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1086 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("DefACLOption"))?;
                *yyval = YYSTYPE::List(list);
            }
            1087 => *yyval = YYSTYPE::List(NodeList::nil()),
            // DefACLOption: IN SCHEMA name_list | FOR ROLE/USER role_list.
            1088..=1090 => {
                let arg = Node::mk_list(mcx, view.v(3).list())?;
                let d = Node::mk(
                    mcx,
                    DefElem {
                        defnamespace: None,
                        defname: Some(if rule == 1088 { "schemas" } else { "roles" }),
                        arg: Some(arg),
                        defaction: DefElemAction::DEFELEM_UNSPEC,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(d));
            }
            // DefACLAction: GRANT/REVOKE [GRANT OPTION FOR] privileges ON
            // defacl_privilege_target TO/FROM grantee_list.
            1091 => {
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.is_grant = true;
                n.privileges = view.v(2).list();
                n.targtype = GrantTargetType::ACL_TARGET_DEFAULTS;
                n.objtype = defacl_objtype(view.v(4).ival());
                n.objects = NodeList::nil();
                n.grantees = view.v(6).list();
                n.grant_option = view.v(7).boolean();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1092 | 1093 => {
                let (pi, ti, gi, bi) = if rule == 1092 { (2, 4, 6, 7) } else { (5, 7, 9, 10) };
                let mut n = Node::build::<GrantStmt>(mcx)?;
                n.is_grant = false;
                n.grant_option = rule == 1093;
                n.privileges = view.v(pi).list();
                n.targtype = GrantTargetType::ACL_TARGET_DEFAULTS;
                n.objtype = defacl_objtype(view.v(ti).ival());
                n.objects = NodeList::nil();
                n.grantees = view.v(gi).list();
                n.behavior = drop_behavior(view.v(bi).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // defacl_privilege_target.
            1094..=1100 => {
                let t = match rule {
                    1094 => ObjectType::OBJECT_TABLE,
                    1095 | 1096 => ObjectType::OBJECT_FUNCTION,
                    1097 => ObjectType::OBJECT_SEQUENCE,
                    1098 => ObjectType::OBJECT_TYPE,
                    1099 => ObjectType::OBJECT_SCHEMA,
                    _ => ObjectType::OBJECT_LARGEOBJECT,
                };
                *yyval = YYSTYPE::Ival(t as i32);
            }
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
            2457 => {
                let spec =
                    view.v(1).node().expect("RoleSpec").as_role_spec().expect("RoleSpec");
                match spec.roletype {
                    RoleSpecType::ROLESPEC_CSTRING => {
                        *yyval = YYSTYPE::Str(spec.rolename.expect("rolename"));
                    }
                    other => {
                        let message = match other {
                            RoleSpecType::ROLESPEC_PUBLIC => {
                                "role name \"public\" is reserved".into()
                            }
                            RoleSpecType::ROLESPEC_SESSION_USER => {
                                "SESSION_USER cannot be used as a role name here".into()
                            }
                            RoleSpecType::ROLESPEC_CURRENT_USER => {
                                "CURRENT_USER cannot be used as a role name here".into()
                            }
                            _ => "CURRENT_ROLE cannot be used as a role name here".into(),
                        };
                        return Err(Box::new(
                            (*self.errposition_error(message, view.l(1)))
                                .with_sqlstate(types_error::ERRCODE_RESERVED_NAME),
                        ));
                    }
                }
            }
            2462 => {
                let r = view.v(1).node().expect("RoleSpec");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, r)?);
            }
            2463 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("RoleSpec"))?;
                *yyval = YYSTYPE::List(list);
            }
            // func_arg_expr: param_name COLON_EQUALS/EQUALS_GREATER a_expr
            2295 | 2296 => {
                let mut n = Node::build::<types_nodes::primnodes::NamedArgExpr>(mcx)?;
                n.name = Some(view.v(1).str_val());
                n.arg = Some(view.v(3).node().expect("a_expr"));
                n.argnumber = -1;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // CallStmt: CALL func_application
            146 => {
                let mut n = Node::build::<types_nodes::CallStmt>(mcx)?;
                n.funccall =
                    view.v(2).node().expect("func_application").as_variant::<FuncCall>();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            147 | 170 | 185 => {
                let mut n = Node::build::<CreateRoleStmt>(mcx)?;
                n.stmt_type = match rule {
                    147 => RoleStmtType::ROLESTMT_ROLE,
                    170 => RoleStmtType::ROLESTMT_USER,
                    _ => RoleStmtType::ROLESTMT_GROUP,
                };
                n.role = Some(view.v(3).str_val());
                n.options = view.v(5).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            151 | 153 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("role option"))?;
                *yyval = YYSTYPE::List(list);
            }
            155 | 157 => {
                let s = view.v(if rule == 157 { 3 } else { 2 }).str_val();
                *yyval =
                    def_elem(mcx, "password", Some(Node::mk_string(mcx, s)?), view.l(1))?;
            }
            156 => *yyval = def_elem(mcx, "password", None, view.l(1))?,
            158 => {
                return Err(Box::new(
                    (*self.errposition_error(
                        "UNENCRYPTED PASSWORD is no longer supported".into(),
                        view.l(1),
                    ))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_hint(
                        "Remove UNENCRYPTED to store the password in encrypted form instead.",
                    ),
                ));
            }
            159 => {
                let b = Node::mk(mcx, Boolean { boolval: true })?;
                *yyval = def_elem(mcx, "inherit", Some(b), view.l(1))?;
            }
            160 => {
                let i = Node::mk(mcx, Integer { ival: view.v(3).ival() })?;
                *yyval = def_elem(mcx, "connectionlimit", Some(i), view.l(1))?;
            }
            161 => {
                let s = Node::mk_string(mcx, view.v(3).str_val())?;
                *yyval = def_elem(mcx, "validUntil", Some(s), view.l(1))?;
            }
            162 => {
                let l = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = def_elem(mcx, "rolemembers", Some(l), view.l(1))?;
            }
            163 => {
                let name = view.v(1).str_val();
                let loc = view.l(1);
                let (defname, value) = match name {
                    "superuser" => ("superuser", true),
                    "nosuperuser" => ("superuser", false),
                    "createrole" => ("createrole", true),
                    "nocreaterole" => ("createrole", false),
                    "replication" => ("isreplication", true),
                    "noreplication" => ("isreplication", false),
                    "createdb" => ("createdb", true),
                    "nocreatedb" => ("createdb", false),
                    "login" => ("canlogin", true),
                    "nologin" => ("canlogin", false),
                    "bypassrls" => ("bypassrls", true),
                    "nobypassrls" => ("bypassrls", false),
                    "noinherit" => ("inherit", false),
                    _ => {
                        return Err(self.errposition_error(
                            format!("unrecognized role option \"{name}\""),
                            loc,
                        ));
                    }
                };
                let b = Node::mk(mcx, Boolean { boolval: value })?;
                *yyval = def_elem(mcx, defname, Some(b), loc)?;
            }
            165 => {
                let i = Node::mk(mcx, Integer { ival: view.v(2).ival() })?;
                *yyval = def_elem(mcx, "sysid", Some(i), view.l(1))?;
            }
            166 | 167 => {
                let name = if rule == 166 { "adminmembers" } else { "rolemembers" };
                let l = Node::mk_list(mcx, view.v(2).list())?;
                *yyval = def_elem(mcx, name, Some(l), view.l(1))?;
            }
            168 | 169 => {
                let l = Node::mk_list(mcx, view.v(3).list())?;
                *yyval = def_elem(mcx, "addroleto", Some(l), view.l(1))?;
            }
            171 | 172 => {
                let role =
                    view.v(3).node().expect("RoleSpec").as_role_spec().expect("RoleSpec");
                let n = Node::mk(
                    mcx,
                    AlterRoleStmt { role, options: view.v(5).list(), action: 1 },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            175..=178 => {
                let role = if rule == 175 || rule == 177 {
                    Some(view.v(3).node().expect("RoleSpec").as_role_spec().expect("RoleSpec"))
                } else {
                    None
                };
                let setstmt = view
                    .v(5)
                    .node()
                    .expect("SetResetClause")
                    .as_variable_set_stmt()
                    .expect("VariableSetStmt");
                let n = Node::mk(
                    mcx,
                    AlterRoleSetStmt { role, database: opt_str(view.v(4)), setstmt },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            179..=184 => {
                let missing_ok = rule % 2 == 0;
                let mut n = Node::build::<DropRoleStmt>(mcx)?;
                n.missing_ok = missing_ok;
                n.roles = view.v(if missing_ok { 5 } else { 3 }).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            186 => {
                let role =
                    view.v(3).node().expect("RoleSpec").as_role_spec().expect("RoleSpec");
                let members = Node::mk_list(mcx, view.v(6).list())?;
                let d = def_elem(mcx, "rolemembers", Some(members), view.l(6))?
                    .node()
                    .unwrap();
                let n = Node::mk(
                    mcx,
                    AlterRoleStmt {
                        role,
                        options: NodeList::make1(mcx, d)?,
                        action: view.v(4).ival(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            187 => *yyval = YYSTYPE::Ival(1),
            188 => *yyval = YYSTYPE::Ival(-1),
            916 => {
                let mut n = Node::build::<DropOwnedStmt>(mcx)?;
                n.roles = view.v(4).list();
                n.behavior = drop_behavior(view.v(5).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            917 => {
                let newrole =
                    view.v(6).node().expect("RoleSpec").as_role_spec().expect("RoleSpec");
                let n =
                    Node::mk(mcx, ReassignOwnedStmt { roles: view.v(4).list(), newrole })?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            1073 | 1074 => {
                let (opt, byi) =
                    if rule == 1074 { (view.v(6).list(), 7) } else { (NodeList::nil(), 5) };
                let mut n = Node::build::<GrantRoleStmt>(mcx)?;
                n.is_grant = true;
                n.granted_roles = view.v(2).list();
                n.grantee_roles = view.v(4).list();
                n.opt = opt;
                n.grantor = view.v(byi).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1075 | 1076 => {
                let mut n = Node::build::<GrantRoleStmt>(mcx)?;
                n.is_grant = false;
                let (pi, gi, byi, bi) = if rule == 1076 {
                    let b = Node::mk(mcx, Boolean { boolval: false })?;
                    let opt = def_elem(mcx, view.v(2).str_val(), Some(b), view.l(2))?
                        .node()
                        .unwrap();
                    n.opt = NodeList::make1(mcx, opt)?;
                    (5, 7, 8, 9)
                } else {
                    (2, 4, 5, 6)
                };
                n.granted_roles = view.v(pi).list();
                n.grantee_roles = view.v(gi).list();
                n.grantor = view.v(byi).node().map(|g| g.as_role_spec().expect("RoleSpec"));
                n.behavior = drop_behavior(view.v(bi).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1077 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("grant_role_opt"))?;
                *yyval = YYSTYPE::List(list);
            }
            1078 => {
                let d = view.v(1).node().expect("grant_role_opt");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, d)?);
            }
            1079 => {
                *yyval = def_elem(mcx, view.v(1).str_val(), view.v(2).node(), view.l(1))?;
            }
            1080..=1082 => {
                let b = Node::mk(mcx, Boolean { boolval: rule != 1082 })?;
                *yyval = YYSTYPE::Node(Some(b));
            }
            968 | 970 => *yyval = YYSTYPE::Boolean(false),
            969 => *yyval = YYSTYPE::Boolean(true),
            // CommentStmt (TRANSFORM 984 parses; its address arm is loud).
            971..=988 => {
                let mut n = Node::build::<CommentStmt>(mcx)?;
                let comment_at = match rule {
                    979 | 981 => 8,
                    980 | 984..=986 => 9,
                    987 => 7,
                    988 => 10,
                    _ => 6,
                };
                n.objtype = match rule {
                    971 | 973 | 981 => object_type(view.v(3).ival()),
                    972 => ObjectType::OBJECT_COLUMN,
                    974 => ObjectType::OBJECT_TYPE,
                    975 => ObjectType::OBJECT_DOMAIN,
                    976 => ObjectType::OBJECT_AGGREGATE,
                    977 => ObjectType::OBJECT_FUNCTION,
                    978 => ObjectType::OBJECT_OPERATOR,
                    979 => ObjectType::OBJECT_TABCONSTRAINT,
                    980 => ObjectType::OBJECT_DOMCONSTRAINT,
                    982 => ObjectType::OBJECT_PROCEDURE,
                    983 => ObjectType::OBJECT_ROUTINE,
                    984 => ObjectType::OBJECT_TRANSFORM,
                    985 => ObjectType::OBJECT_OPCLASS,
                    986 => ObjectType::OBJECT_OPFAMILY,
                    987 => ObjectType::OBJECT_LARGEOBJECT,
                    _ => ObjectType::OBJECT_CAST,
                };
                n.object = match rule {
                    971 | 972 => Some(Node::mk_list(mcx, view.v(4).list())?),
                    973 => Some(Node::mk_string(mcx, view.v(4).str_val())?),
                    974..=978 | 982 | 983 => view.v(4).node(),
                    979 | 981 => {
                        let mut list = view.v(6).list();
                        list.lappend(mcx, Node::mk_string(mcx, view.v(4).str_val())?)?;
                        Some(Node::mk_list(mcx, list)?)
                    }
                    980 => {
                        let tn = make_type_name(mcx, view.v(7).list(), NodeList::nil(), -1)?;
                        let mut list = NodeList::make1(mcx, tn)?;
                        list.lappend(mcx, Node::mk_string(mcx, view.v(4).str_val())?)?;
                        Some(Node::mk_list(mcx, list)?)
                    }
                    984 => {
                        let mut list =
                            NodeList::make1(mcx, view.v(5).node().expect("Typename"))?;
                        list.lappend(mcx, Node::mk_string(mcx, view.v(7).str_val())?)?;
                        Some(Node::mk_list(mcx, list)?)
                    }
                    985 | 986 => {
                        let mut list = view.v(5).list();
                        list.lcons(mcx, Node::mk_string(mcx, view.v(7).str_val())?)?;
                        Some(Node::mk_list(mcx, list)?)
                    }
                    987 => view.v(5).node(),
                    _ => {
                        let mut list =
                            NodeList::make1(mcx, view.v(5).node().expect("Typename"))?;
                        list.lappend(mcx, view.v(7).node().expect("Typename"))?;
                        Some(Node::mk_list(mcx, list)?)
                    }
                };
                let c = view.v(comment_at);
                n.comment = if c.is_null_node() { None } else { Some(c.str_val()) };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            973 => {
                let mut n = Node::build::<CommentStmt>(mcx)?;
                n.objtype = object_type(view.v(3).ival());
                n.object = Some(Node::mk_string(mcx, view.v(4).str_val())?);
                let c = view.v(6);
                n.comment = if c.is_null_node() { None } else { Some(c.str_val()) };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // SecLabelStmt; LARGE OBJECT (998) shifts object/label one slot right.
            991..=1000 => {
                let mut n = Node::build::<SecLabelStmt>(mcx)?;
                let p = view.v(3);
                n.provider = if p.is_null_node() { None } else { Some(p.str_val()) };
                n.objtype = match rule {
                    991 | 993 => object_type(view.v(5).ival()),
                    992 => ObjectType::OBJECT_COLUMN,
                    994 => ObjectType::OBJECT_TYPE,
                    995 => ObjectType::OBJECT_DOMAIN,
                    996 => ObjectType::OBJECT_AGGREGATE,
                    997 => ObjectType::OBJECT_FUNCTION,
                    998 => ObjectType::OBJECT_LARGEOBJECT,
                    999 => ObjectType::OBJECT_PROCEDURE,
                    _ => ObjectType::OBJECT_ROUTINE,
                };
                n.object = match rule {
                    991 | 992 => Some(Node::mk_list(mcx, view.v(6).list())?),
                    993 => Some(Node::mk_string(mcx, view.v(6).str_val())?),
                    998 => view.v(7).node(),
                    _ => view.v(6).node(),
                };
                let l = view.v(if rule == 998 { 9 } else { 8 });
                n.label = if l.is_null_node() { None } else { Some(l.str_val()) };
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            670 => {
                let s = Node::mk_string(mcx, view.v(1).str_val())?;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, s)?);
            }
            671 => {
                let mut list = view.v(2).list();
                list.lcons(mcx, Node::mk_string(mcx, view.v(1).str_val())?)?;
                *yyval = YYSTYPE::List(list);
            }
            // CreateAmStmt; am_type rides as Ival(AMTYPE_*).
            781 => {
                let mut n = Node::build::<parsenodes::CreateAmStmt>(mcx)?;
                n.amname = Some(view.v(4).str_val());
                n.handler_name = view.v(8).list();
                n.amtype = view.v(6).ival() as u8;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            782 => *yyval = YYSTYPE::Ival(parsenodes::AMTYPE_INDEX as i32),
            783 => *yyval = YYSTYPE::Ival(parsenodes::AMTYPE_TABLE as i32),
            // DefineStmt AGGREGATE: 849 is the old (pre-8.2) syntax.
            848 | 849 => {
                let mut n = Node::build::<parsenodes::DefineStmt>(mcx)?;
                n.kind = ObjectType::OBJECT_AGGREGATE;
                n.oldstyle = rule == 849;
                n.replace = view.v(2).boolean();
                n.defnames = view.v(4).list();
                if rule == 848 {
                    n.args = view.v(5).list();
                    n.definition = view.v(6).list();
                } else {
                    n.definition = view.v(5).list();
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DefineStmt: CREATE OPERATOR any_operator definition
            850 => {
                let mut n = Node::build::<parsenodes::DefineStmt>(mcx)?;
                n.kind = ObjectType::OBJECT_OPERATOR;
                n.defnames = view.v(3).list();
                n.definition = view.v(4).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            865 | 876 => {
                let el = view.v(1).node().expect("def_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            866 | 877 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("def_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
            867 | 878 => {
                *yyval = def_elem(mcx, view.v(1).str_val(), view.v(3).node(), view.l(1))?;
            }
            868 => *yyval = def_elem(mcx, view.v(1).str_val(), Option::None, view.l(1))?,
            // def_arg / operator_def_arg: func_type | reserved_keyword |
            // qual_all_Op | NumericOnly | Sconst | NONE (872/873 = the
            // NumericOnly/Sconst def_arg arms already in the hot match).
            869 | 1374 | 1377 | 1378 => *yyval = YYSTYPE::Node(view.v(1).node()),
            870 | 874 | 1375 => {
                *yyval = YYSTYPE::Node(Some(Node::mk_string(mcx, view.v(1).str_val())?));
            }
            871 | 1376 => *yyval = YYSTYPE::Node(Some(Node::mk_list(mcx, view.v(1).list())?)),
            // CreateOpClassStmt: CREATE OPERATOR CLASS any_name opt_default
            // FOR TYPE_P Typename USING name opt_opfamily AS opclass_item_list
            890 => {
                let mut n = Node::build::<parsenodes::CreateOpClassStmt>(mcx)?;
                n.opclassname = view.v(4).list();
                n.isDefault = view.v(5).boolean();
                n.datatype = view.v(8).node();
                n.amname = Some(view.v(10).str_val());
                n.opfamilyname = view.v(11).list();
                n.items = view.v(13).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            891 | 908 => {
                let el = view.v(1).node().expect("opclass item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            892 | 909 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("opclass item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // opclass_item: OPERATOR Iconst {any_operator|operator_with_argtypes}
            // opclass_purpose | FUNCTION Iconst ['(' type_list ')']
            // function_with_argtypes | STORAGE Typename
            893 => {
                let mut owa = Node::build::<parsenodes::ObjectWithArgs>(mcx)?;
                owa.objname = view.v(3).list();
                let mut n = Node::build::<parsenodes::CreateOpClassItem>(mcx)?;
                n.itemtype = parsenodes::OPCLASS_ITEM_OPERATOR;
                n.name = Some(owa.seal());
                n.number = view.v(2).ival();
                n.order_family = view.v(4).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            894 => {
                let mut n = Node::build::<parsenodes::CreateOpClassItem>(mcx)?;
                n.itemtype = parsenodes::OPCLASS_ITEM_OPERATOR;
                n.name = view.v(3).node();
                n.number = view.v(2).ival();
                n.order_family = view.v(4).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            895 | 896 => {
                let mut n = Node::build::<parsenodes::CreateOpClassItem>(mcx)?;
                n.itemtype = parsenodes::OPCLASS_ITEM_FUNCTION;
                n.number = view.v(2).ival();
                if rule == 896 {
                    n.class_args = view.v(4).list();
                    n.name = view.v(6).node();
                } else {
                    n.name = view.v(3).node();
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            897 => {
                let mut n = Node::build::<parsenodes::CreateOpClassItem>(mcx)?;
                n.itemtype = parsenodes::OPCLASS_ITEM_STORAGETYPE;
                n.storedtype = view.v(2).node();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            898 => *yyval = YYSTYPE::Boolean(true),
            899 => *yyval = YYSTYPE::Boolean(false),
            // CreateOpFamilyStmt: CREATE OPERATOR FAMILY any_name USING name
            905 => {
                let mut n = Node::build::<parsenodes::CreateOpFamilyStmt>(mcx)?;
                n.opfamilyname = view.v(4).list();
                n.amname = Some(view.v(6).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterOpFamilyStmt: ALTER OPERATOR FAMILY any_name USING name
            // {ADD_P opclass_item_list | DROP opclass_drop_list}
            906 | 907 => {
                let mut n = Node::build::<parsenodes::AlterOpFamilyStmt>(mcx)?;
                n.opfamilyname = view.v(4).list();
                n.amname = Some(view.v(6).str_val());
                n.isDrop = rule == 907;
                n.items = view.v(8).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // opclass_drop: {OPERATOR|FUNCTION} Iconst '(' type_list ')'
            910 | 911 => {
                let mut n = Node::build::<parsenodes::CreateOpClassItem>(mcx)?;
                n.itemtype = if rule == 910 {
                    parsenodes::OPCLASS_ITEM_OPERATOR
                } else {
                    parsenodes::OPCLASS_ITEM_FUNCTION
                };
                n.number = view.v(2).ival();
                n.class_args = view.v(4).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DropOpClassStmt / DropOpFamilyStmt: DROP OPERATOR CLASS|FAMILY
            // [IF_P EXISTS] any_name USING name opt_drop_behavior
            912 | 913 | 914 | 915 => {
                let (an, nm, bh) = if rule == 912 || rule == 914 { (4, 6, 7) } else { (6, 8, 9) };
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = if rule <= 913 {
                    ObjectType::OBJECT_OPCLASS
                } else {
                    ObjectType::OBJECT_OPFAMILY
                };
                let mut any_name = view.v(an).list();
                any_name.lcons(mcx, Node::mk_string(mcx, view.v(nm).str_val())?)?;
                n.objects = NodeList::make1(mcx, Node::mk_list(mcx, any_name)?)?;
                n.behavior = drop_behavior(view.v(bh).ival());
                n.missing_ok = rule == 913 || rule == 915;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // RemoveOperStmt: DROP OPERATOR [IF_P EXISTS]
            // operator_with_argtypes_list opt_drop_behavior
            1232 | 1233 => {
                let (ls, bh) = if rule == 1232 { (3, 4) } else { (5, 6) };
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = ObjectType::OBJECT_OPERATOR;
                n.objects = view.v(ls).list();
                n.behavior = drop_behavior(view.v(bh).ival());
                n.missing_ok = rule == 1233;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // oper_argtypes: '(' Typename ')' is C's missing-argument error.
            1234 => {
                return Err(Box::new(
                    (*self.errposition_error("missing argument".into(), view.l(3)))
                        .with_hint(
                            "Use NONE to denote the missing argument of a unary operator.",
                        ),
                ));
            }
            // CreateCastStmt: 1248 WITH FUNCTION, 1249 WITHOUT, 1250 INOUT.
            1248..=1250 => {
                let mut n = Node::build::<parsenodes::CreateCastStmt>(mcx)?;
                n.sourcetype = view.v(4).node();
                n.targettype = view.v(6).node();
                if rule == 1248 {
                    n.func = view.v(10).node();
                }
                let ctx = view.v(if rule == 1248 { 11 } else { 10 }).ival();
                n.context = match ctx {
                    0 => CoercionContext::COERCION_IMPLICIT,
                    1 => CoercionContext::COERCION_ASSIGNMENT,
                    _ => CoercionContext::COERCION_EXPLICIT,
                };
                n.inout = rule == 1250;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1251 => *yyval = YYSTYPE::Ival(CoercionContext::COERCION_IMPLICIT as i32),
            1252 => *yyval = YYSTYPE::Ival(CoercionContext::COERCION_ASSIGNMENT as i32),
            1253 => *yyval = YYSTYPE::Ival(CoercionContext::COERCION_EXPLICIT as i32),
            // CreateTransformStmt + transform_element_list arms.
            1257 => {
                let mut n = Node::build::<parsenodes::CreateTransformStmt>(mcx)?;
                n.replace = view.v(2).boolean();
                n.type_name = view.v(5).node();
                n.lang = Some(view.v(7).str_val());
                let elems = view.v(9).transform_elements();
                n.fromsql = elems.fromsql;
                n.tosql = elems.tosql;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1258..=1261 => {
                let (fromsql, tosql) = match rule {
                    1258 => (view.v(5).node(), view.v(11).node()),
                    1259 => (view.v(11).node(), view.v(5).node()),
                    1260 => (view.v(5).node(), None),
                    _ => (None, view.v(5).node()),
                };
                *yyval = YYSTYPE::TransformElementsV(mcx::leak_in(mcx::alloc_in(
                    mcx,
                    TransformElements { fromsql, tosql },
                )?));
            }
            // oper_argtypes: '(' Typename ',' Typename ')' and the unary NONE
            // forms; C's NULL TypeName cell rides the Option cells.
            1235 => {
                let l = view.v(2).node();
                let r = view.v(4).node();
                *yyval = YYSTYPE::OptList(OptNodeList::from_slice(mcx, &[l, r])?);
            }
            1236 => {
                let r = view.v(4).node();
                *yyval = YYSTYPE::OptList(OptNodeList::from_slice(mcx, &[None, r])?);
            }
            1237 => {
                let l = view.v(2).node();
                *yyval = YYSTYPE::OptList(OptNodeList::from_slice(mcx, &[l, None])?);
            }
            1240 => {
                let el = view.v(1).node().expect("operator_with_argtypes");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1241 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("operator_with_argtypes"))?;
                *yyval = YYSTYPE::List(list);
            }
            1242 => {
                let mut owa = Node::build::<parsenodes::ObjectWithArgs>(mcx)?;
                owa.objname = view.v(1).list();
                owa.objargs = view.v(2).opt_list();
                *yyval = YYSTYPE::Node(Some(owa.seal()));
            }
            // DoStmt: DO dostmt_opt_list
            1243 => {
                let mut n = Node::build::<parsenodes::DoStmt>(mcx)?;
                n.args = view.v(2).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1244 => {
                let el = view.v(1).node().expect("dostmt_opt_item");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1245 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(2).node().expect("dostmt_opt_item"))?;
                *yyval = YYSTYPE::List(list);
            }
            // dostmt_opt_item: Sconst | LANGUAGE NonReservedWord_or_Sconst
            1246 => {
                let arg = Node::mk_string(mcx, view.v(1).str_val())?;
                *yyval = def_elem(mcx, "as", Some(arg), view.l(1))?;
            }
            1247 => {
                let arg = Node::mk_string(mcx, view.v(2).str_val())?;
                *yyval = def_elem(mcx, "language", Some(arg), view.l(1))?;
            }
            // AlterOperatorStmt: ALTER OPERATOR operator_with_argtypes SET
            // '(' operator_def_list ')'
            1368 => {
                let mut n = Node::build::<parsenodes::AlterOperatorStmt>(mcx)?;
                n.opername = view.v(3).node();
                n.options = view.v(6).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1369 => {
                let el = view.v(1).node().expect("operator_def_elem");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1370 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("operator_def_elem"))?;
                *yyval = YYSTYPE::List(list);
            }
            1371 | 1373 => *yyval = def_elem(mcx, view.v(1).str_val(), Option::None, view.l(1))?,
            1372 => *yyval = def_elem(mcx, view.v(1).str_val(), view.v(3).node(), view.l(1))?,
            // AlterTypeStmt: ALTER TYPE_P any_name SET '(' operator_def_list ')'
            1379 => {
                let mut n = Node::build::<AlterTypeStmt>(mcx)?;
                n.typeName = view.v(3).list();
                n.options = view.v(6).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DefineStmt: CREATE TYPE_P any_name definition | CREATE TYPE_P
            // any_name (shell)
            851 | 852 => {
                let mut n = Node::build::<parsenodes::DefineStmt>(mcx)?;
                n.kind = ObjectType::OBJECT_TYPE;
                n.defnames = view.v(3).list();
                if rule == 851 {
                    n.definition = view.v(4).list();
                }
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DefineStmt: CREATE TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
            854 => {
                let mut n = Node::build::<CreateEnumStmt>(mcx)?;
                n.typeName = view.v(3).list();
                n.vals = view.v(7).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DefineStmt: CREATE TYPE_P any_name AS RANGE definition
            855 => {
                let mut n = Node::build::<CreateRangeStmt>(mcx)?;
                n.typeName = view.v(3).list();
                n.params = view.v(6).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DefineStmt: CREATE COLLATION [IF NOT EXISTS] any_name definition
            //           | CREATE COLLATION [IF NOT EXISTS] any_name FROM any_name
            860 | 861 | 862 | 863 => {
                let ine = rule == 861 || rule == 863;
                let off = if ine { 3 } else { 0 };
                let mut n = Node::build::<DefineStmt>(mcx)?;
                n.kind = ObjectType::OBJECT_COLLATION;
                n.defnames = view.v(3 + off).list();
                n.definition = if rule == 860 || rule == 861 {
                    view.v(4 + off).list()
                } else {
                    let from = Node::mk_list(mcx, view.v(5 + off).list())?;
                    let el = Node::mk(
                        mcx,
                        DefElem {
                            defnamespace: None,
                            defname: Some("from"),
                            arg: Some(from),
                            defaction: DefElemAction::DEFELEM_UNSPEC,
                            location: view.l(5 + off),
                        },
                    )?;
                    NodeList::make1(mcx, el)?
                };
                n.if_not_exists = ine;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // definition: '(' def_list ')'
            864 => *yyval = YYSTYPE::List(view.v(2).list()),
            // opt_enum_val_list: enum_val_list | /*EMPTY*/
            879 => *yyval = YYSTYPE::List(view.v(1).list()),
            880 => *yyval = YYSTYPE::List(NodeList::nil()),
            // enum_val_list: Sconst | enum_val_list ',' Sconst
            881 => {
                let s = Node::mk_string(mcx, view.v(1).str_val())?;
                *yyval = YYSTYPE::List(NodeList::make1(mcx, s)?);
            }
            882 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, Node::mk_string(mcx, view.v(3).str_val())?)?;
                *yyval = YYSTYPE::List(list);
            }
            // AlterEnumStmt: ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst
            //   [ BEFORE Sconst | AFTER Sconst ] / RENAME VALUE_P / DROP VALUE_P
            883 | 884 | 885 => {
                let mut n = Node::build::<AlterEnumStmt>(mcx)?;
                n.typeName = view.v(3).list();
                n.newVal = Some(view.v(7).str_val());
                n.skipIfNewValExists = view.v(6).boolean();
                if rule != 883 {
                    n.newValNeighbor = Some(view.v(9).str_val());
                }
                n.newValIsAfter = rule != 884;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            886 => {
                let mut n = Node::build::<AlterEnumStmt>(mcx)?;
                n.typeName = view.v(3).list();
                n.oldVal = Some(view.v(6).str_val());
                n.newVal = Some(view.v(8).str_val());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            887 => {
                return Err(self.errposition_error_code(
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "dropping an enum value is not implemented".into(),
                    view.l(4),
                ))
            }
            // opt_if_not_exists: IF_P NOT EXISTS | /*EMPTY*/
            888 => *yyval = YYSTYPE::Boolean(true),
            889 => *yyval = YYSTYPE::Boolean(false),
            // table_ref: relation_expr opt_alias_clause tablesample_clause
            1833 => {
                let rv = view.v(1).node().expect("relation_expr");
                let alias = view.v(2).alias();
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    rv.with_mut::<RangeVar, _>(|r| r.alias = alias)
                        .expect("relation_expr is RangeVar");
                }
                let rts = view.v(3).node().expect("tablesample_clause");
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    rts.with_mut::<RangeTableSample, _>(|n| n.relation = Some(rv))
                        .expect("tablesample_clause is RangeTableSample");
                }
                *yyval = YYSTYPE::Node(Some(rts));
            }
            // opt_repeatable_clause: REPEATABLE '(' a_expr ')'
            1882 => *yyval = YYSTYPE::Node(view.v(3).node()),
            // tablesample_clause: TABLESAMPLE func_name '(' expr_list ')'
            //   opt_repeatable_clause
            1881 => {
                let mut n = Node::build::<RangeTableSample>(mcx)?;
                n.method = view.v(2).list();
                n.args = view.v(4).list();
                n.repeatable = view.v(6).node();
                n.location = view.l(2);
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
            // CompositeTypeStmt: CREATE TYPE_P any_name AS '(' OptTableFuncElementList ')'
            853 => {
                let names = view.v(3).list();
                let loc = view.l(3);
                let mut parts = [None; 3];
                for (i, el) in names.iter().enumerate() {
                    if i < 3 {
                        parts[i] = el.as_string().map(|s| s.sval);
                    }
                }
                // makeRangeVarFromAnyName: makeNode zero-fill leaves inh=false.
                let (catalogname, schemaname, relname) = match names.len() {
                    1 => (None, None, parts[0]),
                    2 => (None, parts[0], parts[1]),
                    3 => (parts[0], parts[1], parts[2]),
                    _ => return Err(self.improper_qualified_name(None, &names, loc)),
                };
                let rv = Node::mk_mut(
                    mcx,
                    RangeVar {
                        catalogname,
                        schemaname,
                        relname,
                        inh: false,
                        relpersistence: RELPERSISTENCE_PERMANENT,
                        alias: None,
                        location: loc,
                    },
                )?
                .seal_ref();
                let mut n = Node::build::<CompositeTypeStmt>(mcx)?;
                n.typevar = Some(rv);
                n.coldeflist = view.v(6).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // DefineStmt: CREATE TEXT_P SEARCH {PARSER|DICTIONARY|TEMPLATE|CONFIGURATION}
            // any_name definition
            856..=859 => {
                let mut n = Node::build::<DefineStmt>(mcx)?;
                n.kind = match rule {
                    856 => ObjectType::OBJECT_TSPARSER,
                    857 => ObjectType::OBJECT_TSDICTIONARY,
                    858 => ObjectType::OBJECT_TSTEMPLATE,
                    _ => ObjectType::OBJECT_TSCONFIGURATION,
                };
                n.defnames = view.v(5).list();
                n.definition = view.v(6).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTSDictionaryStmt: ALTER TEXT_P SEARCH DICTIONARY any_name definition
            1539 => {
                let mut n = Node::build::<AlterTSDictionaryStmt>(mcx)?;
                n.dictname = view.v(5).list();
                n.options = view.v(6).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // AlterTSConfigurationStmt: ALTER TEXT_P SEARCH CONFIGURATION any_name ...
            1540 | 1541 => {
                let mut n = Node::build::<AlterTSConfigurationStmt>(mcx)?;
                n.kind = if rule == 1540 {
                    AlterTSConfigType::ALTER_TSCONFIG_ADD_MAPPING
                } else {
                    AlterTSConfigType::ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN
                };
                n.cfgname = view.v(5).list();
                n.tokentype = view.v(9).list();
                n.dicts = view.v(11).list();
                n.r#override = rule == 1541;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1542 | 1543 => {
                let mut n = Node::build::<AlterTSConfigurationStmt>(mcx)?;
                n.kind = if rule == 1542 {
                    AlterTSConfigType::ALTER_TSCONFIG_REPLACE_DICT
                } else {
                    AlterTSConfigType::ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN
                };
                n.cfgname = view.v(5).list();
                let (d1, d2) = if rule == 1542 {
                    (9, 11)
                } else {
                    n.tokentype = view.v(9).list();
                    (11, 13)
                };
                let mut dicts = NodeList::make1(mcx, Node::mk_list(mcx, view.v(d1).list())?)?;
                dicts.lappend(mcx, Node::mk_list(mcx, view.v(d2).list())?)?;
                n.dicts = dicts;
                n.replace = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            1544 | 1545 => {
                let mut n = Node::build::<AlterTSConfigurationStmt>(mcx)?;
                n.kind = AlterTSConfigType::ALTER_TSCONFIG_DROP_MAPPING;
                n.cfgname = view.v(5).list();
                n.missing_ok = rule == 1545;
                n.tokentype = view.v(if rule == 1544 { 9 } else { 11 }).list();
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // TableFuncElementList / TableFuncElement (1898/1899 ride DISPATCH)
            1900 => {
                let el = view.v(1).node().expect("TableFuncElement");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, el)?);
            }
            1901 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("TableFuncElement"))?;
                *yyval = YYSTYPE::List(list);
            }
            1902 => {
                let mut n = Node::build::<ColumnDef>(mcx)?;
                n.colname = Some(view.v(1).str_val());
                n.typeName = view.v(2).node();
                n.is_local = true;
                n.collClause = view.v(3).node();
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
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
            // CreatePublicationStmt: CREATE PUBLICATION name
            //   [FOR ALL TABLES | FOR pub_obj_list] opt_definition
            1404 => {
                let n = CreatePublicationStmt {
                    pubname: Some(view.v(3).str_val()),
                    options: view.v(4).list(),
                    pubobjects: NodeList::nil(),
                    for_all_tables: false,
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            1405 => {
                let n = CreatePublicationStmt {
                    pubname: Some(view.v(3).str_val()),
                    options: view.v(7).list(),
                    pubobjects: NodeList::nil(),
                    for_all_tables: true,
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            1406 => {
                let pubobjects = view.v(5).list();
                self.preprocess_pubobj_list(&pubobjects)?;
                let n = CreatePublicationStmt {
                    pubname: Some(view.v(3).str_val()),
                    options: view.v(6).list(),
                    pubobjects,
                    for_all_tables: false,
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // PublicationObjSpec: TABLE relation_expr opt_column_list OptWhereClause
            1407 => {
                let mut pt = Node::build::<PublicationTable>(mcx)?;
                pt.relation =
                    view.v(2).node().expect("relation_expr").as_variant::<RangeVar>();
                pt.columns = view.v(3).list();
                pt.whereClause = view.v(4).node();
                let mut n = Node::build::<PublicationObjSpec>(mcx)?;
                n.pubobjtype = PublicationObjSpecType::PUBLICATIONOBJ_TABLE;
                n.pubtable = Some(pt.seal_ref());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PublicationObjSpec: TABLES IN SCHEMA ColId
            1408 => {
                let mut n = Node::build::<PublicationObjSpec>(mcx)?;
                n.pubobjtype = PublicationObjSpecType::PUBLICATIONOBJ_TABLES_IN_SCHEMA;
                n.name = Some(view.v(4).str_val());
                n.location = view.l(4);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PublicationObjSpec: TABLES IN SCHEMA CURRENT_SCHEMA
            1409 => {
                let mut n = Node::build::<PublicationObjSpec>(mcx)?;
                n.pubobjtype =
                    PublicationObjSpecType::PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA;
                n.location = view.l(4);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PublicationObjSpec: ColId opt_column_list OptWhereClause
            1410 => {
                let name = view.v(1).str_val();
                let columns = view.v(2).list();
                let where_clause = view.v(3).node();
                let loc = view.l(1);
                let mut n = Node::build::<PublicationObjSpec>(mcx)?;
                n.pubobjtype = PublicationObjSpecType::PUBLICATIONOBJ_CONTINUATION;
                if !columns.is_nil() || where_clause.is_some() {
                    let mut pt = Node::build::<PublicationTable>(mcx)?;
                    pt.relation = make_range_var(mcx, None, None, Some(name), loc)?
                        .as_variant::<RangeVar>();
                    pt.columns = columns;
                    pt.whereClause = where_clause;
                    n.pubtable = Some(pt.seal_ref());
                } else {
                    n.name = Some(name);
                }
                n.location = loc;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PublicationObjSpec: ColId indirection opt_column_list OptWhereClause
            1411 => {
                let mut pt = Node::build::<PublicationTable>(mcx)?;
                pt.relation = self
                    .range_var_from_qualified_name(
                        view.v(1).str_val(),
                        view.v(2).list(),
                        view.l(1),
                    )?
                    .as_variant::<RangeVar>();
                pt.columns = view.v(3).list();
                pt.whereClause = view.v(4).node();
                let mut n = Node::build::<PublicationObjSpec>(mcx)?;
                n.pubobjtype = PublicationObjSpecType::PUBLICATIONOBJ_CONTINUATION;
                n.pubtable = Some(pt.seal_ref());
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PublicationObjSpec: extended_relation_expr opt_column_list OptWhereClause
            1412 => {
                let mut pt = Node::build::<PublicationTable>(mcx)?;
                pt.relation = view
                    .v(1)
                    .node()
                    .expect("extended_relation_expr")
                    .as_variant::<RangeVar>();
                pt.columns = view.v(2).list();
                pt.whereClause = view.v(3).node();
                let mut n = Node::build::<PublicationObjSpec>(mcx)?;
                n.pubobjtype = PublicationObjSpecType::PUBLICATIONOBJ_CONTINUATION;
                n.pubtable = Some(pt.seal_ref());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // PublicationObjSpec: CURRENT_SCHEMA
            1413 => {
                let mut n = Node::build::<PublicationObjSpec>(mcx)?;
                n.pubobjtype = PublicationObjSpecType::PUBLICATIONOBJ_CONTINUATION;
                n.location = view.l(1);
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // pub_obj_list
            1414 => {
                let n = view.v(1).node().expect("PublicationObjSpec");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            1415 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("PublicationObjSpec"))?;
                *yyval = YYSTYPE::List(list);
            }
            // AlterPublicationStmt: ALTER PUBLICATION name
            //   SET definition | ADD_P/SET/DROP pub_obj_list
            1416 => {
                let n = AlterPublicationStmt {
                    pubname: Some(view.v(3).str_val()),
                    options: view.v(5).list(),
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            1417 | 1418 | 1419 => {
                let pubobjects = view.v(5).list();
                self.preprocess_pubobj_list(&pubobjects)?;
                let n = AlterPublicationStmt {
                    pubname: Some(view.v(3).str_val()),
                    pubobjects,
                    action: match rule {
                        1417 => AlterPublicationAction::AP_AddObjects,
                        1418 => AlterPublicationAction::AP_SetObjects,
                        _ => AlterPublicationAction::AP_DropObjects,
                    },
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // CreateSubscriptionStmt: CREATE SUBSCRIPTION name CONNECTION Sconst
            //   PUBLICATION name_list opt_definition
            1420 => {
                let n = CreateSubscriptionStmt {
                    subname: Some(view.v(3).str_val()),
                    conninfo: Some(view.v(5).str_val()),
                    publication: view.v(7).list(),
                    options: view.v(8).list(),
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // AlterSubscriptionStmt: SET definition
            1421 => {
                let n = AlterSubscriptionStmt {
                    kind: AlterSubscriptionType::ALTER_SUBSCRIPTION_OPTIONS,
                    subname: Some(view.v(3).str_val()),
                    options: view.v(5).list(),
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // AlterSubscriptionStmt: CONNECTION Sconst
            1422 => {
                let n = AlterSubscriptionStmt {
                    kind: AlterSubscriptionType::ALTER_SUBSCRIPTION_CONNECTION,
                    subname: Some(view.v(3).str_val()),
                    conninfo: Some(view.v(5).str_val()),
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // AlterSubscriptionStmt: REFRESH PUBLICATION opt_definition
            1423 => {
                let n = AlterSubscriptionStmt {
                    kind: AlterSubscriptionType::ALTER_SUBSCRIPTION_REFRESH,
                    subname: Some(view.v(3).str_val()),
                    options: view.v(6).list(),
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // AlterSubscriptionStmt: ADD_P/DROP/SET PUBLICATION name_list opt_definition
            1424 | 1425 | 1426 => {
                let n = AlterSubscriptionStmt {
                    kind: match rule {
                        1424 => AlterSubscriptionType::ALTER_SUBSCRIPTION_ADD_PUBLICATION,
                        1425 => {
                            AlterSubscriptionType::ALTER_SUBSCRIPTION_DROP_PUBLICATION
                        }
                        _ => AlterSubscriptionType::ALTER_SUBSCRIPTION_SET_PUBLICATION,
                    },
                    subname: Some(view.v(3).str_val()),
                    publication: view.v(6).list(),
                    options: view.v(7).list(),
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // AlterSubscriptionStmt: ENABLE_P | DISABLE_P
            1427 | 1428 => {
                let enabled = def_elem(
                    mcx,
                    "enabled",
                    Some(Node::mk_boolean(mcx, rule == 1427)?),
                    view.l(1),
                )?
                .node()
                .expect("DefElem");
                let n = AlterSubscriptionStmt {
                    kind: AlterSubscriptionType::ALTER_SUBSCRIPTION_ENABLED,
                    subname: Some(view.v(3).str_val()),
                    options: NodeList::make1(mcx, enabled)?,
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // AlterSubscriptionStmt: SKIP definition
            1429 => {
                let n = AlterSubscriptionStmt {
                    kind: AlterSubscriptionType::ALTER_SUBSCRIPTION_SKIP,
                    subname: Some(view.v(3).str_val()),
                    options: view.v(5).list(),
                    ..Default::default()
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // DropSubscriptionStmt: DROP SUBSCRIPTION [IF EXISTS] name opt_drop_behavior
            1430 | 1431 => {
                let (nm, beh) = if rule == 1430 { (3, 4) } else { (5, 6) };
                let n = DropSubscriptionStmt {
                    subname: Some(view.v(nm).str_val()),
                    missing_ok: rule == 1431,
                    behavior: drop_behavior(view.v(beh).ival()),
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, n)?));
            }
            // --- rules-lane arms (append-only; splice after grammar-batch) ---
            // RuleStmt: CREATE opt_or_replace RULE name AS ON event TO
            // qualified_name where_clause DO opt_instead RuleActionList
            1432 => {
                let relation = view.v(9).node().expect("qualified_name");
                let n = Node::mk(
                    mcx,
                    types_nodes::rawnodes::RuleStmt {
                        relation: relation.as_variant::<RangeVar>(),
                        rulename: view.v(4).str_val(),
                        whereClause: view.v(10).node(),
                        event: int_to_cmd_type(view.v(7).ival()),
                        instead: view.v(12).boolean(),
                        actions: view.v(13).list(),
                        replace: view.v(2).boolean(),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(n));
            }
            // RuleActionList single / RuleActionMulti
            1434 => {
                let n = view.v(1).node().expect("RuleActionStmt");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            1436 => {
                let mut list = view.v(1).list();
                if let Some(n) = view.v(3).node() {
                    list.lappend(mcx, n)?;
                }
                *yyval = YYSTYPE::List(list);
            }
            1437 => {
                *yyval = YYSTYPE::List(match view.v(1).node() {
                    Some(n) => NodeList::make1(mcx, n)?,
                    None => NodeList::nil(),
                });
            }
            // event
            1445 => *yyval = YYSTYPE::Ival(types_nodes::nodes_enums::CmdType::CMD_SELECT as i32),
            1446 => *yyval = YYSTYPE::Ival(types_nodes::nodes_enums::CmdType::CMD_UPDATE as i32),
            1447 => *yyval = YYSTYPE::Ival(types_nodes::nodes_enums::CmdType::CMD_DELETE as i32),
            1448 => *yyval = YYSTYPE::Ival(types_nodes::nodes_enums::CmdType::CMD_INSERT as i32),
            // opt_instead: INSTEAD | ALSO | empty
            1449 => *yyval = YYSTYPE::Boolean(true),
            1450 | 1451 => *yyval = YYSTYPE::Boolean(false),
            // DropStmt: DROP object_type_name_on_any_name [IF_P EXISTS] name
            // ON any_name opt_drop_behavior
            922 => {
                let mut objects = view.v(5).list();
                objects.lappend(mcx, Node::mk_string(mcx, view.v(3).str_val())?)?;
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = object_type(view.v(2).ival());
                n.objects = NodeList::make1(mcx, Node::mk_list(mcx, objects)?)?;
                n.behavior = drop_behavior(view.v(6).ival());
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            923 => {
                let mut objects = view.v(7).list();
                objects.lappend(mcx, Node::mk_string(mcx, view.v(5).str_val())?)?;
                let mut n = Node::build::<DropStmt>(mcx)?;
                n.removeType = object_type(view.v(2).ival());
                n.objects = NodeList::make1(mcx, Node::mk_list(mcx, objects)?)?;
                n.behavior = drop_behavior(view.v(8).ival());
                n.missing_ok = true;
                *yyval = YYSTYPE::Node(Some(n.seal()));
            }
            // --- sqljson-lane arms (append-only) ---
            2187 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonObjectConstructor {
                        exprs: view.v(3).list(),
                        output: view.v(6).node(),
                        absent_on_null: view.v(4).boolean(),
                        unique: view.v(5).boolean(),
                        location: view.l(1),
                    },
                )?));
            }
            2188 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonObjectConstructor {
                        exprs: NodeList::nil(),
                        output: view.v(3).node(),
                        absent_on_null: false,
                        unique: false,
                        location: view.l(1),
                    },
                )?));
            }
            2189 | 2191 => {
                let (exprs, absent, output) = if rule == 2189 {
                    (view.v(3).list(), view.v(4).boolean(), view.v(5).node())
                } else {
                    (NodeList::nil(), true, view.v(3).node())
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonArrayConstructor {
                        exprs,
                        output,
                        absent_on_null: absent,
                        location: view.l(1),
                    },
                )?));
            }
            2190 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonArrayQueryConstructor {
                        query: view.v(3).node(),
                        output: view.v(5).node(),
                        format: json_format_ref(view.v(4).node()),
                        absent_on_null: true,
                        location: view.l(1),
                    },
                )?));
            }
            2192 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonParseExpr {
                        expr: view.v(3).node(),
                        output: None,
                        unique_keys: view.v(4).boolean(),
                        location: view.l(1),
                    },
                )?));
            }
            2193 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonScalarExpr {
                        expr: view.v(3).node(),
                        output: None,
                        location: view.l(1),
                    },
                )?));
            }
            2194 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonSerializeExpr {
                        expr: view.v(3).node(),
                        output: view.v(4).node(),
                        location: view.l(1),
                    },
                )?));
            }
            2195 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    MergeSupportFunc {
                        msftype: types_core::catalog::TEXTOID,
                        msfcollid: types_core::InvalidOid,
                        location: view.l(1),
                    },
                )?));
            }
            2196 => {
                let behaviors = view.v(10).json_behaviors();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonFuncExpr {
                        op: JsonExprOp::JSON_QUERY_OP,
                        column_name: None,
                        context_item: view.v(3).node(),
                        pathspec: view.v(5).node(),
                        passing: view.v(6).list(),
                        output: view.v(7).node(),
                        on_empty: behaviors.on_empty,
                        on_error: behaviors.on_error,
                        wrapper: json_wrapper(view.v(8).ival()),
                        quotes: json_quotes(view.v(9).ival()),
                        location: view.l(1),
                    },
                )?));
            }
            2197 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonFuncExpr {
                        op: JsonExprOp::JSON_EXISTS_OP,
                        column_name: None,
                        context_item: view.v(3).node(),
                        pathspec: view.v(5).node(),
                        passing: view.v(6).list(),
                        output: None,
                        on_empty: None,
                        on_error: view.v(7).node(),
                        wrapper: JsonWrapper::JSW_UNSPEC,
                        quotes: JsonQuotes::JS_QUOTES_UNSPEC,
                        location: view.l(1),
                    },
                )?));
            }
            2198 => {
                let behaviors = view.v(8).json_behaviors();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonFuncExpr {
                        op: JsonExprOp::JSON_VALUE_OP,
                        column_name: None,
                        context_item: view.v(3).node(),
                        pathspec: view.v(5).node(),
                        passing: view.v(6).list(),
                        output: view.v(7).node(),
                        on_empty: behaviors.on_empty,
                        on_error: behaviors.on_error,
                        wrapper: JsonWrapper::JSW_UNSPEC,
                        quotes: JsonQuotes::JS_QUOTES_UNSPEC,
                        location: view.l(1),
                    },
                )?));
            }
            // a_expr IS [NOT] json_predicate_type_constraint unique_opt
            2087 | 2088 => {
                let not = rule == 2088;
                let (ty, uniq) = if not {
                    (view.v(4).ival(), view.v(5).boolean())
                } else {
                    (view.v(3).ival(), view.v(4).boolean())
                };
                let format = Node::mk_mut(mcx, JsonFormat::default())?.seal_ref();
                let pred = Node::mk(
                    mcx,
                    JsonIsPredicate {
                        expr: view.v(1).node(),
                        format: Some(format),
                        item_type: json_value_type(ty),
                        unique_keys: uniq,
                        location: view.l(1),
                    },
                )?;
                let out = if not {
                    Node::mk(
                        mcx,
                        BoolExpr {
                            boolop: BoolExprType::NOT_EXPR,
                            args: NodeList::make1(mcx, pred)?,
                            location: view.l(1),
                        },
                    )?
                } else {
                    pred
                };
                *yyval = YYSTYPE::Node(Some(out));
            }
            // func_expr: json_aggregate_func filter_clause over_clause
            2134 => {
                let agg = view.v(1).node().expect("json_aggregate_func");
                let filter = view.v(2).node();
                let over = view.v(3).node();
                let ctor = if let Some(oa) = agg.as_json_object_agg() {
                    oa.constructor
                } else {
                    agg.as_json_array_agg().expect("JsonArrayAgg").constructor
                }
                .expect("JsonAggConstructor");
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    ctor.with_mut::<JsonAggConstructor, _>(|c| {
                        c.agg_filter = filter;
                        c.over = over;
                    })
                    .expect("JsonAggConstructor");
                }
                *yyval = YYSTYPE::Node(Some(agg));
            }
            // json_arguments
            2354 => {
                let n = view.v(1).node().expect("json_argument");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            2355 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("json_argument"))?;
                *yyval = YYSTYPE::List(list);
            }
            2356 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonArgument {
                        val: view.v(1).node(),
                        name: Some(view.v(3).str_val()),
                    },
                )?));
            }
            // json_wrapper_behavior
            2357 | 2358 => *yyval = YYSTYPE::Ival(JsonWrapper::JSW_NONE as i32),
            2359 | 2360 | 2362 | 2364 => {
                *yyval = YYSTYPE::Ival(JsonWrapper::JSW_UNCONDITIONAL as i32)
            }
            2361 | 2363 => *yyval = YYSTYPE::Ival(JsonWrapper::JSW_CONDITIONAL as i32),
            2365 => *yyval = YYSTYPE::Ival(JsonWrapper::JSW_UNSPEC as i32),
            // json_behavior: DEFAULT a_expr | json_behavior_type
            2366 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonBehavior {
                        btype: JsonBehaviorType::JSON_BEHAVIOR_DEFAULT,
                        expr: view.v(2).node(),
                        coerce: false,
                        location: view.l(1),
                    },
                )?));
            }
            2367 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonBehavior {
                        btype: json_behavior_type(view.v(1).ival()),
                        expr: None,
                        coerce: false,
                        location: view.l(1),
                    },
                )?));
            }
            // json_behavior_type
            2368 => *yyval = YYSTYPE::Ival(JsonBehaviorType::JSON_BEHAVIOR_ERROR as i32),
            2369 => *yyval = YYSTYPE::Ival(JsonBehaviorType::JSON_BEHAVIOR_NULL as i32),
            2370 => *yyval = YYSTYPE::Ival(JsonBehaviorType::JSON_BEHAVIOR_TRUE as i32),
            2371 => *yyval = YYSTYPE::Ival(JsonBehaviorType::JSON_BEHAVIOR_FALSE as i32),
            2372 => *yyval = YYSTYPE::Ival(JsonBehaviorType::JSON_BEHAVIOR_UNKNOWN as i32),
            2373 | 2375 => {
                *yyval = YYSTYPE::Ival(JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY as i32)
            }
            2374 => *yyval = YYSTYPE::Ival(JsonBehaviorType::JSON_BEHAVIOR_EMPTY_OBJECT as i32),
            // json_behavior_clause_opt
            2376..=2379 => {
                let (on_empty, on_error) = match rule {
                    2376 => (view.v(1).node(), None),
                    2377 => (None, view.v(1).node()),
                    2378 => (view.v(1).node(), view.v(4).node()),
                    _ => (None, None),
                };
                *yyval = YYSTYPE::JsonBehaviorsV(mcx::leak_in(mcx::alloc_in(
                    mcx,
                    JsonBehaviors { on_empty, on_error },
                )?));
            }
            // json_value_expr: a_expr json_format_clause_opt
            2382 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonValueExpr {
                        raw_expr: view.v(1).node(),
                        formatted_expr: None,
                        format: json_format_ref(view.v(2).node()),
                    },
                )?));
            }
            // json_format_clause: FORMAT_LA JSON [ENCODING name]
            2383 => {
                let name = view.v(4).str_val();
                let encoding = if name.eq_ignore_ascii_case("utf8") {
                    JsonEncoding::JS_ENC_UTF8
                } else if name.eq_ignore_ascii_case("utf16") {
                    JsonEncoding::JS_ENC_UTF16
                } else if name.eq_ignore_ascii_case("utf32") {
                    JsonEncoding::JS_ENC_UTF32
                } else {
                    return Err(Box::new(
                        (*self.errposition_error(
                            format!("unrecognized JSON encoding: {name}"),
                            view.l(4),
                        ))
                        .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
                    ));
                };
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonFormat {
                        format_type: JsonFormatType::JS_FORMAT_JSON,
                        encoding,
                        location: view.l(1),
                    },
                )?));
            }
            2384 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonFormat {
                        format_type: JsonFormatType::JS_FORMAT_JSON,
                        encoding: JsonEncoding::JS_ENC_DEFAULT,
                        location: view.l(1),
                    },
                )?));
            }
            2386 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(mcx, JsonFormat::default())?));
            }
            // json_quotes_clause_opt
            2387 | 2388 => *yyval = YYSTYPE::Ival(JsonQuotes::JS_QUOTES_KEEP as i32),
            2389 | 2390 => *yyval = YYSTYPE::Ival(JsonQuotes::JS_QUOTES_OMIT as i32),
            2391 => *yyval = YYSTYPE::Ival(JsonQuotes::JS_QUOTES_UNSPEC as i32),
            // json_returning_clause_opt: RETURNING Typename json_format_clause_opt
            2392 => {
                // C makeNode zeroing: typmod 0 here (transform assigns both).
                let returning = Node::mk_mut(
                    mcx,
                    JsonReturning {
                        format: json_format_ref(view.v(3).node()),
                        typid: 0,
                        typmod: 0,
                    },
                )?
                .seal_ref();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonOutput { typeName: view.v(2).node(), returning: Some(returning) },
                )?));
            }
            // json_predicate_type_constraint
            2394 | 2395 => *yyval = YYSTYPE::Ival(JsonValueType::JS_TYPE_ANY as i32),
            2396 => *yyval = YYSTYPE::Ival(JsonValueType::JS_TYPE_ARRAY as i32),
            2397 => *yyval = YYSTYPE::Ival(JsonValueType::JS_TYPE_OBJECT as i32),
            2398 => *yyval = YYSTYPE::Ival(JsonValueType::JS_TYPE_SCALAR as i32),
            // json_key_uniqueness_constraint_opt
            2399 | 2400 => *yyval = YYSTYPE::Boolean(true),
            2401 | 2402 | 2403 => *yyval = YYSTYPE::Boolean(false),
            // json_name_and_value_list
            2404 => {
                let n = view.v(1).node().expect("json_name_and_value");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            2405 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("json_name_and_value"))?;
                *yyval = YYSTYPE::List(list);
            }
            // json_name_and_value: c_expr VALUE_P jve | a_expr ':' jve
            2406 | 2407 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonKeyValue { key: view.v(1).node(), value: view.v(3).node() },
                )?));
            }
            // json_object_constructor_null_clause_opt
            2408 | 2410 => *yyval = YYSTYPE::Boolean(false),
            2409 => *yyval = YYSTYPE::Boolean(true),
            // json_array_constructor_null_clause_opt
            2411 => *yyval = YYSTYPE::Boolean(false),
            2412 | 2413 => *yyval = YYSTYPE::Boolean(true),
            // json_value_expr_list
            2414 => {
                let n = view.v(1).node().expect("json_value_expr");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            2415 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("json_value_expr"))?;
                *yyval = YYSTYPE::List(list);
            }
            // json_aggregate_func: JSON_OBJECTAGG | JSON_ARRAYAGG
            2416 => {
                let ctor = Node::mk(
                    mcx,
                    JsonAggConstructor {
                        output: view.v(6).node(),
                        agg_filter: None,
                        agg_order: NodeList::nil(),
                        over: None,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonObjectAgg {
                        constructor: Some(ctor),
                        arg: view.v(3).node(),
                        absent_on_null: view.v(4).boolean(),
                        unique: view.v(5).boolean(),
                    },
                )?));
            }
            2417 => {
                let ctor = Node::mk(
                    mcx,
                    JsonAggConstructor {
                        output: view.v(6).node(),
                        agg_filter: None,
                        agg_order: view.v(4).list(),
                        over: None,
                        location: view.l(1),
                    },
                )?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    JsonArrayAgg {
                        constructor: Some(ctor),
                        arg: view.v(3).node(),
                        absent_on_null: view.v(5).boolean(),
                    },
                )?));
            }
            // --- json-table-lane arms (append-only) ---
            // table_ref: json_table opt_alias_clause
            //          | LATERAL_P json_table opt_alias_clause.
            1842 | 1843 => {
                let off = if rule == 1843 { 1 } else { 0 };
                let jt = view.v(1 + off).node().expect("json_table");
                let alias = view.v(2 + off).alias();
                let lateral = rule == 1843;
                // SAFETY: as rule 8 — parser-owned tree, no live derived refs.
                unsafe {
                    jt.with_mut::<types_nodes::rawnodes::JsonTable, _>(|n| {
                        n.alias = alias;
                        if lateral {
                            n.lateral = true;
                        }
                    })
                    .expect("json_table is JsonTable")
                };
                *yyval = YYSTYPE::Node(Some(jt));
            }
            // json_table: JSON_TABLE '(' json_value_expr ',' a_expr
            //   json_table_path_name_opt json_passing_clause_opt
            //   COLUMNS '(' json_table_column_definition_list ')'
            //   json_on_error_clause_opt ')'.
            1921 => {
                let path = view.v(5).node().expect("a_expr");
                let is_string_const = path
                    .as_a_const()
                    .is_some_and(|c| matches!(c.val, Some(ValUnion::String(_))));
                if !is_string_const {
                    return Err(self.errposition_error_code(
                        types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                        "only string constants are supported in JSON_TABLE path specification"
                            .into(),
                        view.l(5),
                    ));
                }
                let pathstring = match path.as_a_const().unwrap().val {
                    Some(ValUnion::String(s)) => s.sval,
                    _ => unreachable!(),
                };
                let name6 = view.v(6);
                let name = if name6.is_null_node() { None } else { Some(name6.str_val()) };
                let pathspec =
                    self.make_json_table_path_spec(pathstring, name, view.l(5), view.l(6))?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::rawnodes::JsonTable {
                        context_item: view.v(3).node(),
                        pathspec: Some(pathspec),
                        passing: view.v(7).list(),
                        columns: view.v(10).list(),
                        on_error: view.v(12).node(),
                        alias: None,
                        lateral: false,
                        location: view.l(1),
                    },
                )?));
            }
            // json_table_column_definition_list.
            1924 => {
                let n = view.v(1).node().expect("json_table_column_definition");
                *yyval = YYSTYPE::List(NodeList::make1(mcx, n)?);
            }
            1925 => {
                let mut list = view.v(1).list();
                list.lappend(mcx, view.v(3).node().expect("json_table_column_definition"))?;
                *yyval = YYSTYPE::List(list);
            }
            // json_table_column_definition: ColId FOR ORDINALITY.
            1926 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::rawnodes::JsonTableColumn {
                        coltype: JsonTableColumnType::JTC_FOR_ORDINALITY,
                        name: Some(view.v(1).str_val()),
                        location: view.l(1),
                        ..Default::default()
                    },
                )?));
            }
            // ColId Typename [json_format_clause] json_table_column_path_clause_opt
            //   json_wrapper_behavior json_quotes_clause_opt json_behavior_clause_opt.
            1927 | 1928 => {
                let off = if rule == 1928 { 1 } else { 0 };
                let format = if rule == 1928 {
                    json_format_ref(view.v(3).node())
                } else {
                    json_format_ref(Some(Node::mk(mcx, JsonFormat::default())?))
                };
                let behaviors = view.v(6 + off).json_behaviors();
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::rawnodes::JsonTableColumn {
                        coltype: if rule == 1928 {
                            JsonTableColumnType::JTC_FORMATTED
                        } else {
                            JsonTableColumnType::JTC_REGULAR
                        },
                        name: Some(view.v(1).str_val()),
                        typeName: view.v(2).node(),
                        pathspec: view.v(3 + off).node(),
                        format,
                        wrapper: json_wrapper(view.v(4 + off).ival()),
                        quotes: json_quotes(view.v(5 + off).ival()),
                        columns: NodeList::nil(),
                        on_empty: behaviors.on_empty,
                        on_error: behaviors.on_error,
                        location: view.l(1),
                    },
                )?));
            }
            // ColId Typename EXISTS json_table_column_path_clause_opt
            //   json_on_error_clause_opt.
            1929 => {
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::rawnodes::JsonTableColumn {
                        coltype: JsonTableColumnType::JTC_EXISTS,
                        name: Some(view.v(1).str_val()),
                        typeName: view.v(2).node(),
                        pathspec: view.v(4).node(),
                        format: json_format_ref(Some(Node::mk(mcx, JsonFormat::default())?)),
                        wrapper: JsonWrapper::JSW_NONE,
                        quotes: JsonQuotes::JS_QUOTES_UNSPEC,
                        columns: NodeList::nil(),
                        on_empty: None,
                        on_error: view.v(5).node(),
                        location: view.l(1),
                    },
                )?));
            }
            // NESTED path_opt Sconst [AS name] COLUMNS '(' ... ')'.
            1930 | 1931 => {
                let (name, name_loc, cols_at) = if rule == 1931 {
                    (Some(view.v(5).str_val()), view.l(5), 8)
                } else {
                    (None, -1, 6)
                };
                let pathspec = self.make_json_table_path_spec(
                    view.v(3).str_val(),
                    name,
                    view.l(3),
                    name_loc,
                )?;
                *yyval = YYSTYPE::Node(Some(Node::mk(
                    mcx,
                    types_nodes::rawnodes::JsonTableColumn {
                        coltype: JsonTableColumnType::JTC_NESTED,
                        pathspec: Some(pathspec),
                        columns: view.v(cols_at).list(),
                        location: view.l(1),
                        ..Default::default()
                    },
                )?));
            }
            // json_table_column_path_clause_opt: PATH Sconst.
            1934 => {
                *yyval = YYSTYPE::Node(Some(self.make_json_table_path_spec(
                    view.v(2).str_val(),
                    None,
                    view.l(2),
                    -1,
                )?));
            }
            // alter_table_cmd ENABLE/DISABLE RULE
            _ => unimplemented_rule(rule),
        }
        Ok(())
    }

    // makeJsonTablePathSpec (makefuncs.c): the path string re-wraps as an
    // A_Const String at the string's location.
    fn make_json_table_path_spec(
        &self,
        pathstring: &'mcx str,
        name: Option<&'mcx str>,
        string_location: i32,
        name_location: i32,
    ) -> PgResult<Node<'mcx>> {
        let string = Node::mk_a_const(
            self.mcx,
            Some(ValUnion::String(types_nodes::String { sval: pathstring })),
            string_location,
        )?;
        Node::mk(
            self.mcx,
            types_nodes::rawnodes::JsonTablePathSpec {
                string: Some(string),
                name,
                name_location,
                location: string_location,
            },
        )
    }

    // makeColumnRef: leading field selections fold into ColumnRef.fields; the
    // first A_Indices switches the remainder into an A_Indirection wrapper.
    fn make_column_ref(
        &self,
        colname: &'mcx str,
        indirection: NodeList<'mcx>,
        location: i32,
    ) -> PgResult<Node<'mcx>> {
        let n = indirection.len();
        let mut nfields = 0usize;
        for (i, el) in indirection.iter().enumerate() {
            if el.node_tag() == types_nodes::NodeTag::T_A_Indices {
                let cells = indirection.as_slice();
                let head = NodeList::from_slice(self.mcx, &cells[..nfields])?;
                let tail = NodeList::from_slice(self.mcx, &cells[nfields..])?;
                self.check_indirection(&tail)?;
                let mut fields = head;
                fields.lcons(self.mcx, Node::mk_string(self.mcx, colname)?)?;
                let c = Node::mk_column_ref(self.mcx, fields, location)?;
                return Node::mk(
                    self.mcx,
                    types_nodes::A_Indirection { arg: Some(c), indirection: tail },
                );
            }
            if el.as_a_star().is_some() && i + 1 != n {
                return Err(self.parser_yyerror("improper use of \"*\""));
            }
            nfields += 1;
        }
        let mut fields = indirection;
        fields.lcons(self.mcx, Node::mk_string(self.mcx, colname)?)?;
        Node::mk_column_ref(self.mcx, fields, location)
    }

    // check_indirection: '*' is legal only as the last indirection item.
    fn check_indirection(&self, indirection: &NodeList<'mcx>) -> PgResult<()> {
        let n = indirection.len();
        for (i, el) in indirection.iter().enumerate() {
            if el.as_a_star().is_some() && i + 1 != n {
                return Err(self.parser_yyerror("improper use of \"*\""));
            }
        }
        Ok(())
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

    // makeRangeVarFromAnyName: makeNode zero-fill leaves inh=false.
    fn range_var_from_any_name(
        &self,
        names: &NodeList<'mcx>,
        location: i32,
    ) -> PgResult<&'mcx RangeVar<'mcx>> {
        let mut parts = [None; 3];
        for (i, el) in names.iter().enumerate() {
            if i < 3 {
                parts[i] = el.as_string().map(|s| s.sval);
            }
        }
        let (catalogname, schemaname, relname) = match names.len() {
            1 => (None, None, parts[0]),
            2 => (None, parts[0], parts[1]),
            3 => (parts[0], parts[1], parts[2]),
            _ => return Err(self.improper_qualified_name(None, names, location)),
        };
        Ok(Node::mk_mut(
            self.mcx,
            RangeVar {
                catalogname,
                schemaname,
                relname,
                inh: false,
                relpersistence: RELPERSISTENCE_PERMANENT,
                alias: None,
                location,
            },
        )?
        .seal_ref())
    }

    // makeRangeVarFromQualifiedName (incl. check_qualified_name).
    fn range_var_from_qualified_name(
        &self,
        name: &'mcx str,
        ind: NodeList<'mcx>,
        location: i32,
    ) -> PgResult<Node<'mcx>> {
        let mut parts = [None; 2];
        for (i, n) in ind.iter().enumerate() {
            let Some(s) = n.as_string() else {
                return Err(self.parser_yyerror("syntax error"));
            };
            if i < 2 {
                parts[i] = Some(s.sval);
            }
        }
        match ind.len() {
            1 => make_range_var(self.mcx, None, Some(name), parts[0], location),
            2 => make_range_var(self.mcx, Some(name), parts[0], parts[1], location),
            _ => Err(self.improper_qualified_name(Some(name), &ind, location)),
        }
    }

    fn preprocess_pubobj_list(&self, list: &NodeList<'mcx>) -> PgResult<()> {
        use PublicationObjSpecType::*;
        let mcx = self.mcx;
        let Some(first) = list.iter().next() else {
            return Ok(());
        };
        {
            let p = first.as_variant::<PublicationObjSpec>().expect("PublicationObjSpec");
            if p.pubobjtype == PUBLICATIONOBJ_CONTINUATION {
                return Err(Box::new(
                    (*self.errposition_error(
                        "invalid publication object list".into(),
                        p.location,
                    ))
                    .with_detail(
                        "One of TABLE or TABLES IN SCHEMA must be specified before a standalone table or schema name.",
                    ),
                ));
            }
        }
        let mut prevobjtype = PUBLICATIONOBJ_CONTINUATION;
        for nd in list.iter() {
            let (mut objtype, name, pubtable, location) = {
                let p = nd.as_variant::<PublicationObjSpec>().expect("PublicationObjSpec");
                (p.pubobjtype, p.name, p.pubtable, p.location)
            };
            if objtype == PUBLICATIONOBJ_CONTINUATION {
                objtype = prevobjtype;
            }
            let mut new_pubtable = None;
            if objtype == PUBLICATIONOBJ_TABLE {
                if name.is_none() && pubtable.is_none() {
                    return Err(self.errposition_error("invalid table name".into(), location));
                }
                if let Some(nm) = name {
                    let mut pt = Node::build::<PublicationTable>(mcx)?;
                    pt.relation =
                        make_range_var(mcx, None, None, Some(nm), location)?.as_variant();
                    new_pubtable = Some(pt.seal_ref());
                }
            } else if objtype == PUBLICATIONOBJ_TABLES_IN_SCHEMA
                || objtype == PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA
            {
                if let Some(pt) = pubtable {
                    if pt.whereClause.is_some() {
                        return Err(self.errposition_error(
                            "WHERE clause not allowed for schema".into(),
                            location,
                        ));
                    }
                    if !pt.columns.is_nil() {
                        return Err(self.errposition_error(
                            "column specification not allowed for schema".into(),
                            location,
                        ));
                    }
                }
                objtype = if name.is_some() {
                    PUBLICATIONOBJ_TABLES_IN_SCHEMA
                } else if pubtable.is_none() {
                    PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA
                } else {
                    return Err(
                        self.errposition_error("invalid schema name".into(), location)
                    );
                };
            }
            // SAFETY: parser-owned tree, no live derived refs (as rule 8).
            unsafe {
                nd.with_mut::<PublicationObjSpec, _>(|m| {
                    m.pubobjtype = objtype;
                    if new_pubtable.is_some() {
                        m.pubtable = new_pubtable;
                        m.name = None;
                    }
                })
                .expect("PublicationObjSpec");
            }
            prevobjtype = objtype;
        }
        Ok(())
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

fn merge_match_kind(v: i32) -> types_nodes::MergeMatchKind {
    match v {
        0 => types_nodes::MergeMatchKind::MERGE_WHEN_MATCHED,
        1 => types_nodes::MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE,
        2 => types_nodes::MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET,
        other => panic!("gram_core: bad MergeMatchKind {other}"),
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

// extractArgTypes (gram.y): input-argument TypeNames only. Function argtype
// cells are never None; the Option cells exist for oper_argtypes NONE.
fn extract_arg_types<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    params: &NodeList<'mcx>,
) -> PgResult<OptNodeList<'mcx>> {
    let mut result = OptNodeList::nil();
    for p in params {
        let fp = p.as_function_parameter().expect("FunctionParameter");
        if !matches!(
            fp.mode,
            FunctionParameterMode::FUNC_PARAM_OUT | FunctionParameterMode::FUNC_PARAM_TABLE
        ) {
            result.lappend(mcx, Some(fp.argType.expect("func_arg argType")))?;
        }
    }
    Ok(result)
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

// defGetInt32 (define.c); hash_partbound_elem args are always Integer.
fn def_get_int32(d: &DefElem<'_>) -> i32 {
    d.arg
        .expect("defGetInt32 arg")
        .as_integer()
        .expect("defGetInt32 Integer")
        .ival
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

fn defacl_objtype(v: i32) -> ObjectType {
    for t in [
        ObjectType::OBJECT_TABLE,
        ObjectType::OBJECT_FUNCTION,
        ObjectType::OBJECT_SEQUENCE,
        ObjectType::OBJECT_TYPE,
        ObjectType::OBJECT_SCHEMA,
        ObjectType::OBJECT_LARGEOBJECT,
    ] {
        if t as i32 == v {
            return t;
        }
    }
    panic!("invalid defacl_privilege_target ObjectType {v}")
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

// makeRecursiveViewSelect (gram.y): WITH RECURSIVE relname (aliases) AS
// (query) SELECT aliases FROM relname.
fn make_recursive_view_select<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relname: &'mcx str,
    aliases: NodeList<'mcx>,
    query: Option<Node<'mcx>>,
) -> PgResult<Node<'mcx>> {
    let mut tl = NodeList::nil();
    for alias in aliases.iter() {
        let colname = alias.as_string().expect("alias list holds String nodes").sval;
        let fields = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
        let mut rt = Node::build::<types_nodes::ResTarget>(mcx)?;
        rt.val = Some(Node::mk_column_ref(mcx, fields, -1)?);
        rt.location = -1;
        tl.lappend(mcx, rt.seal())?;
    }

    let mut cte = Node::build::<CommonTableExpr>(mcx)?;
    cte.ctename = Some(relname);
    cte.aliascolnames = aliases;
    cte.ctematerialized = CTEMaterialize::CTEMaterializeDefault;
    cte.ctequery = query;
    cte.location = -1;

    let mut w = Node::build::<WithClause>(mcx)?;
    w.recursive = true;
    w.ctes = NodeList::make1(mcx, cte.seal())?;
    w.location = -1;

    let mut s = Node::build::<SelectStmt>(mcx)?;
    s.withClause = Some(w.seal());
    s.targetList = tl;
    s.fromClause = NodeList::make1(mcx, make_range_var(mcx, None, None, Some(relname), -1)?)?;
    Ok(s.seal())
}

// doNegateFloat: strip a leading '+'/'-' pair-wise or prepend '-'.
// C's list_make2(makeInteger(events), columns) TriggerEvents carrier,
// flattened to [Integer(events), columns...]; never escapes into the tree.
fn trigger_one_event<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    events: i32,
    columns: NodeList<'mcx>,
) -> PgResult<YYSTYPE<'mcx>> {
    let mut l = NodeList::make1(mcx, Node::mk_integer(mcx, events)?)?;
    l.concat(mcx, &columns)?;
    Ok(YYSTYPE::List(l))
}

fn trigger_events<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    v: YYSTYPE<'mcx>,
) -> PgResult<(i16, NodeList<'mcx>)> {
    let l = v.list();
    let events = l.nth(0).as_integer().expect("events Integer").ival as i16;
    let mut cols = NodeList::nil();
    for c in l.as_slice()[1..].iter().copied() {
        cols.lappend(mcx, c)?;
    }
    Ok((events, cols))
}

fn arena_int_str<'mcx>(mcx: mcx::Mcx<'mcx>, v: i32) -> PgResult<&'mcx str> {
    use core::fmt::Write;
    let mut s = mcx::PgString::new_in(mcx);
    write!(s, "{v}").expect("int fmt");
    Ok(core::str::from_utf8(s.into_bytes().leak()).expect("was ASCII"))
}

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

fn json_format_ref<'mcx>(n: Option<Node<'mcx>>) -> Option<&'mcx JsonFormat> {
    n.map(|n| n.as_json_format().expect("JsonFormat"))
}

fn json_wrapper(v: i32) -> JsonWrapper {
    match v {
        1 => JsonWrapper::JSW_NONE,
        2 => JsonWrapper::JSW_CONDITIONAL,
        3 => JsonWrapper::JSW_UNCONDITIONAL,
        _ => JsonWrapper::JSW_UNSPEC,
    }
}

fn json_quotes(v: i32) -> JsonQuotes {
    match v {
        1 => JsonQuotes::JS_QUOTES_KEEP,
        2 => JsonQuotes::JS_QUOTES_OMIT,
        _ => JsonQuotes::JS_QUOTES_UNSPEC,
    }
}

fn json_behavior_type(v: i32) -> JsonBehaviorType {
    match v {
        1 => JsonBehaviorType::JSON_BEHAVIOR_ERROR,
        2 => JsonBehaviorType::JSON_BEHAVIOR_EMPTY,
        3 => JsonBehaviorType::JSON_BEHAVIOR_TRUE,
        4 => JsonBehaviorType::JSON_BEHAVIOR_FALSE,
        5 => JsonBehaviorType::JSON_BEHAVIOR_UNKNOWN,
        6 => JsonBehaviorType::JSON_BEHAVIOR_EMPTY_ARRAY,
        7 => JsonBehaviorType::JSON_BEHAVIOR_EMPTY_OBJECT,
        8 => JsonBehaviorType::JSON_BEHAVIOR_DEFAULT,
        _ => JsonBehaviorType::JSON_BEHAVIOR_NULL,
    }
}

fn json_value_type(v: i32) -> JsonValueType {
    match v {
        1 => JsonValueType::JS_TYPE_OBJECT,
        2 => JsonValueType::JS_TYPE_ARRAY,
        3 => JsonValueType::JS_TYPE_SCALAR,
        _ => JsonValueType::JS_TYPE_ANY,
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

// makeXmlExpr (gram.y): xmloption/indent/type/typmod stay makeNode zero-fill.
fn make_xml_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    op: XmlExprOp,
    name: Option<&'mcx str>,
    named_args: NodeList<'mcx>,
    args: NodeList<'mcx>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    let mut n = Node::build::<XmlExpr>(mcx)?;
    n.op = op;
    n.name = name;
    n.named_args = named_args;
    n.args = args;
    n.location = location;
    Ok(n.seal())
}

fn xml_option_from_ival(v: i32) -> XmlOptionType {
    if v == 1 { XmlOptionType::XMLOPTION_CONTENT } else { XmlOptionType::XMLOPTION_DOCUMENT }
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

fn int_to_cmd_type(v: i32) -> types_nodes::nodes_enums::CmdType {
    use types_nodes::nodes_enums::CmdType::*;
    match v {
        1 => CMD_SELECT,
        2 => CMD_UPDATE,
        3 => CMD_INSERT,
        4 => CMD_DELETE,
        other => panic!("gram_core: unexpected CmdType value {other}"),
    }
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
// C's default arm returns -1 ("just unknown"), and so does this — the only
// caller builds error cursor positions, where a panic on an exotic ORDER BY
// expression (e.g. CASE) would out-crash the real error.
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
    } else if let Some(ce) = n.as_case_expr() {
        ce.location
    } else if let Some(ce) = n.as_coalesce_expr() {
        ce.location
    } else if let Some(mm) = n.as_min_max_expr() {
        mm.location
    } else if let Some(sv) = n.as_sql_value_function() {
        sv.location
    } else if let Some(re) = n.as_row_expr() {
        re.location
    } else if let Some(ae) = n.as_a_array_expr() {
        ae.location
    } else if let Some(sl) = n.as_sub_link() {
        leftmost_loc(expr_location_opt(sl.testexpr), sl.location)
    } else if let Some(bt) = n.as_boolean_test() {
        leftmost_loc(bt.location, expr_location_opt(bt.arg))
    } else if let Some(cc) = n.as_collate_clause() {
        expr_location_opt(cc.arg)
    } else {
        -1
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
