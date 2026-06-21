pub enum Node<'mcx> {
    /// `T_Append`.
    Append(crate::nodeappend::Append<'mcx>),
    /// `T_ModifyTable`.
    ModifyTable(crate::modifytable::ModifyTable<'mcx>),
    /// `T_Material`.
    Material(crate::nodeforeigncustom::Material<'mcx>),
    /// `T_Gather`.
    Gather(crate::nodegather::Gather<'mcx>),
    /// `T_GatherMerge`.
    GatherMerge(crate::nodegathermerge::GatherMerge<'mcx>),
    /// `T_MergeAppend`.
    MergeAppend(crate::nodemergeappend::MergeAppend<'mcx>),
    /// `T_BitmapAnd`.
    BitmapAnd(crate::nodebitmapand::BitmapAnd<'mcx>),
    /// `T_BitmapOr`.
    BitmapOr(crate::nodebitmapor::BitmapOr<'mcx>),
    /// `T_MergeJoin`.
    MergeJoin(crate::nodemergejoin::MergeJoin<'mcx>),
    /// `T_RecursiveUnion`.
    RecursiveUnion(crate::noderecursiveunion::RecursiveUnion<'mcx>),
    /// `T_Group`.
    Group(crate::nodegroup::Group<'mcx>),
    /// `T_ProjectSet`.
    ProjectSet(crate::nodeprojectset::ProjectSet<'mcx>),
    /// `T_Result`.
    Result(crate::noderesult::Result<'mcx>),
    /// `T_SetOp`.
    SetOp(crate::nodesetop::SetOp<'mcx>),
    /// `T_Memoize`.
    Memoize(crate::nodememoize::Memoize<'mcx>),
    /// `T_IndexScan`.
    IndexScan(crate::nodeindexscan::IndexScan<'mcx>),
    /// `T_IndexOnlyScan`.
    IndexOnlyScan(crate::nodeindexonlyscan::IndexOnlyScan<'mcx>),
    /// `T_BitmapIndexScan`.
    BitmapIndexScan(crate::nodebitmapindexscan::BitmapIndexScan<'mcx>),
    /// `T_BitmapHeapScan`.
    BitmapHeapScan(crate::nodebitmapheapscan::BitmapHeapScan<'mcx>),
    /// `T_LockRows`.
    LockRows(crate::nodelockrows::LockRows<'mcx>),
    /// `T_Limit`.
    Limit(crate::nodelimit::Limit<'mcx>),
    /// `T_Unique`.
    Unique(crate::nodeunique::Unique<'mcx>),
    /// `T_Sort`.
    Sort(crate::nodesort::Sort<'mcx>),
    /// `T_IncrementalSort`.
    IncrementalSort(crate::nodeincrementalsort::IncrementalSort<'mcx>),
    /// `T_Agg`.
    Agg(crate::nodeagg::Agg<'mcx>),
    /// `T_WindowAgg`.
    WindowAgg(crate::nodewindowagg::WindowAgg<'mcx>),
    /// `T_TableFuncScan`.
    TableFuncScan(crate::nodetablefuncscan::TableFuncScan<'mcx>),
    /// `T_FunctionScan`.
    FunctionScan(crate::nodefunctionscan::FunctionScan<'mcx>),
    /// `T_ValuesScan`.
    ValuesScan(crate::nodevaluesscan::ValuesScan<'mcx>),
    /// `T_CteScan`.
    CteScan(crate::nodectescan::CteScan<'mcx>),
    /// `T_NamedTuplestoreScan`.
    NamedTuplestoreScan(crate::nodenamedtuplestorescan::NamedTuplestoreScan<'mcx>),
    /// `T_NestLoop`.
    NestLoop(crate::nodenestloop::NestLoop<'mcx>),
    /// `T_HashJoin`.
    HashJoin(crate::nodehashjoin::HashJoin<'mcx>),
    /// `T_Hash` — the inner child of a HashJoin.
    Hash(crate::nodehashjoin::Hash<'mcx>),
    /// `T_TidRangeScan`.
    TidRangeScan(crate::nodetidrangescan::TidRangeScan<'mcx>),
    /// `T_SampleScan`.
    SampleScan(crate::nodesamplescan::SampleScan<'mcx>),
    /// `T_TidScan`.
    TidScan(crate::nodeindexscan::TidScan<'mcx>),
    /// `T_WorkTableScan`.
    WorkTableScan(crate::nodeworktablescan::WorkTableScan<'mcx>),
    /// `T_SeqScan`.
    SeqScan(crate::nodeseqscan::SeqScan<'mcx>),
    /// `T_SubqueryScan`.
    SubqueryScan(crate::nodeindexscan::SubqueryScan<'mcx>),
    /// `T_ForeignScan`.
    ForeignScan(crate::nodeforeigncustom::ForeignScan<'mcx>),
    /// `T_CustomScan`.
    CustomScan(crate::nodeforeigncustom::CustomScan<'mcx>),
    // --- Parse-tree statement / producer / raw-grammar nodes (K1-parsetree) ---
    // Additive arms making the parse vocabulary participate in the `Node`
    // machinery (`tag`/`clone_in`). `plan_head` panics for these (they have no
    // embedded `Plan`), exactly as it does for `Expr`.
    /// `T_Query` — a parse/analyze/rewrite query tree.
    Query(crate::copy_query::Query<'mcx>),
    /// `T_RangeTblEntry`.
    RangeTblEntry(crate::parsenodes::RangeTblEntry<'mcx>),
    /// `T_RTEPermissionInfo`.
    RTEPermissionInfo(crate::parsenodes::RTEPermissionInfo<'mcx>),
    /// `T_RangeTblFunction`.
    RangeTblFunction(crate::rawnodes::RangeTblFunction<'mcx>),
    /// `T_TargetEntry`.
    TargetEntry(crate::primnodes::TargetEntry<'mcx>),
    /// `T_TableFunc` — an XMLTABLE/JSON_TABLE table function. In C this is a
    /// `Node *` carried by `RangeTblEntry.tablefunc` and walked by
    /// `expression_tree_walker`; a first-class `Node` arm so the walkers can
    /// dispatch it and the parser can store it in the RTE's `tablefunc` slot.
    TableFunc(crate::primnodes::TableFunc<'mcx>),
    /// `T_RangeTblRef`.
    RangeTblRef(crate::rawnodes::RangeTblRef),
    /// `T_FromExpr`.
    FromExpr(crate::rawnodes::FromExpr<'mcx>),
    /// `T_JoinExpr`.
    JoinExpr(crate::rawnodes::JoinExpr<'mcx>),
    /// `T_OnConflictExpr`.
    OnConflictExpr(crate::rawnodes::OnConflictExpr<'mcx>),
    /// `T_MergeAction` — the parse-tree MergeAction (cf the executor's
    /// `MergeActionState`-paired one in `modifytable`).
    MergeAction(crate::rawnodes::MergeAction<'mcx>),
    /// `T_SortGroupClause`.
    SortGroupClause(crate::rawnodes::SortGroupClause),
    /// `T_GroupingSet`.
    GroupingSet(crate::rawnodes::GroupingSet<'mcx>),
    /// `T_WindowClause`.
    WindowClause(crate::rawnodes::WindowClause<'mcx>),
    /// `T_RowMarkClause`.
    RowMarkClause(crate::rawnodes::RowMarkClause),
    /// `T_LockingClause`.
    LockingClause(crate::rawnodes::LockingClause<'mcx>),
    /// `T_WithCheckOption`.
    WithCheckOption(crate::rawnodes::WithCheckOption<'mcx>),
    /// `T_CTECycleClause` — the CYCLE clause of a recursive CTE. In C this is a
    /// `Node *` carried by `CommonTableExpr.cycle_clause` and walked by
    /// `expression_tree_walker` (into `cycle_mark_value`/`cycle_mark_default`);
    /// a first-class `Node` arm so the walkers can dispatch it.
    CTECycleClause(crate::rawnodes::CTECycleClause<'mcx>),
    /// `T_CommonTableExpr`.
    CommonTableExpr(crate::rawnodes::CommonTableExpr<'mcx>),
    /// `T_SetOperationStmt`.
    SetOperationStmt(crate::rawnodes::SetOperationStmt<'mcx>),
    /// `T_Alias`.
    Alias(crate::rawnodes::Alias<'mcx>),
    /// `T_RangeVar`.
    RangeVar(crate::rawnodes::RangeVar<'mcx>),
    /// `T_TypeName`.
    TypeName(crate::rawnodes::TypeName<'mcx>),
    /// `T_ColumnDef`.
    ColumnDef(crate::rawnodes::ColumnDef<'mcx>),
    // --- raw-grammar INPUT nodes ---
    /// `T_SelectStmt`.
    SelectStmt(crate::rawnodes::SelectStmt<'mcx>),
    /// `T_InsertStmt`.
    InsertStmt(crate::rawnodes::InsertStmt<'mcx>),
    /// `T_UpdateStmt`.
    UpdateStmt(crate::rawnodes::UpdateStmt<'mcx>),
    /// `T_DeleteStmt`.
    DeleteStmt(crate::rawnodes::DeleteStmt<'mcx>),
    /// `T_MergeStmt`.
    MergeStmt(crate::rawnodes::MergeStmt<'mcx>),
    /// `T_A_Expr`.
    A_Expr(crate::rawnodes::A_Expr<'mcx>),
    /// `T_ColumnRef`.
    ColumnRef(crate::rawnodes::ColumnRef<'mcx>),
    /// `T_ParamRef`.
    ParamRef(crate::rawnodes::ParamRef),
    /// `T_A_Const`.
    A_Const(crate::rawnodes::A_Const<'mcx>),
    /// `T_FuncCall`.
    FuncCall(crate::rawnodes::FuncCall<'mcx>),
    /// `T_A_Star`.
    A_Star(crate::rawnodes::A_Star),
    /// `T_A_Indices`.
    A_Indices(crate::rawnodes::A_Indices<'mcx>),
    /// `T_A_Indirection`.
    A_Indirection(crate::rawnodes::A_Indirection<'mcx>),
    /// `T_A_ArrayExpr`.
    A_ArrayExpr(crate::rawnodes::A_ArrayExpr<'mcx>),
    /// `T_ResTarget`.
    ResTarget(crate::rawnodes::ResTarget<'mcx>),
    /// `T_MultiAssignRef`.
    MultiAssignRef(crate::rawnodes::MultiAssignRef<'mcx>),
    /// `T_TypeCast`.
    TypeCast(crate::rawnodes::TypeCast<'mcx>),
    /// `T_CollateClause`.
    CollateClause(crate::rawnodes::CollateClause<'mcx>),
    /// `T_SortBy`.
    SortBy(crate::rawnodes::SortBy<'mcx>),
    /// `T_WindowDef`.
    WindowDef(crate::rawnodes::WindowDef<'mcx>),
    /// `T_RangeSubselect`.
    RangeSubselect(crate::rawnodes::RangeSubselect<'mcx>),
    /// `T_RangeFunction`.
    RangeFunction(crate::rawnodes::RangeFunction<'mcx>),
    /// `T_RangeTableSample`.
    RangeTableSample(crate::rawnodes::RangeTableSample<'mcx>),
    /// `T_TableSampleClause` — the post-analysis form stored in
    /// `RangeTblEntry.tablesample` (built by `transformRangeTableSample`).
    TableSampleClause(crate::nodesamplescan::TableSampleClause<'mcx>),
    /// `T_WithClause`.
    WithClause(crate::rawnodes::WithClause<'mcx>),
    /// `T_InferClause`.
    InferClause(crate::rawnodes::InferClause<'mcx>),
    /// `T_OnConflictClause`.
    OnConflictClause(crate::rawnodes::OnConflictClause<'mcx>),
    /// `T_MergeWhenClause`.
    MergeWhenClause(crate::rawnodes::MergeWhenClause<'mcx>),
    /// `T_ReturningClause`.
    ReturningClause(crate::rawnodes::ReturningClause<'mcx>),
    /// `T_ReturningOption`.
    ReturningOption(crate::rawnodes::ReturningOption<'mcx>),
    /// `T_TriggerTransition`.
    TriggerTransition(crate::rawnodes::TriggerTransition<'mcx>),
    /// `T_RangeTableFunc`.
    RangeTableFunc(crate::rawnodes::RangeTableFunc<'mcx>),
    /// `T_RangeTableFuncCol`.
    RangeTableFuncCol(crate::rawnodes::RangeTableFuncCol<'mcx>),
    // --- raw-grammar `Expr`-deriving expression nodes (rawexprnodes) ---
    // In C, these `Expr`-deriving node types are the same struct in the raw
    // grammar output and the post-analysis tree, but the grammar fills their
    // `Node *`/`List *` children with RAW parse-tree nodes
    // (`ColumnRef`/`A_Expr`/…). The owned model's post-analysis
    // [`crate::primnodes::Expr`] enum carries `Expr` children, so it cannot hold
    // the raw children; the raw counterparts live in
    // [`crate::rawexprnodes`] and ride as their own `Node` arms (no conflation
    // with `Node::Expr`). `transformExpr` (analyze.c) turns these into
    // post-analysis `Expr`. Additive (`#[non_exhaustive]`); tags verified vs
    // nodes/nodetags.h.
    /// `T_BoolExpr` (raw).
    BoolExpr(crate::rawexprnodes::BoolExpr<'mcx>),
    /// `T_CaseExpr` (raw).
    CaseExpr(crate::rawexprnodes::CaseExpr<'mcx>),
    /// `T_CaseWhen` (raw).
    CaseWhen(crate::rawexprnodes::CaseWhen<'mcx>),
    /// `T_CoalesceExpr` (raw).
    CoalesceExpr(crate::rawexprnodes::CoalesceExpr<'mcx>),
    /// `T_MinMaxExpr` (raw).
    MinMaxExpr(crate::rawexprnodes::MinMaxExpr<'mcx>),
    /// `T_SubLink` (raw).
    SubLink(crate::rawexprnodes::SubLink<'mcx>),
    /// `T_NullTest` (raw).
    NullTest(crate::rawexprnodes::NullTest<'mcx>),
    /// `T_BooleanTest` (raw).
    BooleanTest(crate::rawexprnodes::BooleanTest<'mcx>),
    /// `T_RowExpr` (raw).
    RowExpr(crate::rawexprnodes::RowExpr<'mcx>),
    /// `T_GroupingFunc` (raw).
    GroupingFunc(crate::rawexprnodes::GroupingFunc<'mcx>),
    /// `T_CollateExpr` (raw).
    CollateExpr(crate::rawexprnodes::CollateExpr<'mcx>),
    /// `T_SetToDefault` (raw).
    SetToDefault(crate::rawexprnodes::SetToDefault),
    /// `T_CurrentOfExpr` (raw).
    CurrentOfExpr(crate::rawexprnodes::CurrentOfExpr<'mcx>),
    /// `T_NamedArgExpr` (raw).
    NamedArgExpr(crate::rawexprnodes::NamedArgExpr<'mcx>),
    /// `T_SQLValueFunction` (raw).
    SQLValueFunction(crate::rawexprnodes::SQLValueFunction),
    /// `T_XmlExpr` (raw).
    XmlExpr(crate::rawexprnodes::XmlExpr<'mcx>),
    /// `T_XmlSerialize` (raw).
    XmlSerialize(crate::rawexprnodes::XmlSerialize<'mcx>),
    // --- SQL/JSON raw-grammar nodes (nodes/parsenodes.h, pre-analysis) ---
    /// `T_JsonValueExpr` (raw).
    JsonValueExpr(crate::rawexprnodes::JsonValueExpr<'mcx>),
    /// `T_JsonBehavior` (raw).
    JsonBehavior(crate::rawexprnodes::JsonBehavior<'mcx>),
    /// `T_JsonIsPredicate` (raw).
    JsonIsPredicate(crate::rawexprnodes::JsonIsPredicate<'mcx>),
    /// `T_JsonOutput` (raw).
    JsonOutput(crate::rawexprnodes::JsonOutput<'mcx>),
    /// `T_JsonKeyValue` (raw).
    JsonKeyValue(crate::rawexprnodes::JsonKeyValue<'mcx>),
    /// `T_JsonObjectConstructor` (raw).
    JsonObjectConstructor(crate::rawexprnodes::JsonObjectConstructor<'mcx>),
    /// `T_JsonArrayConstructor` (raw).
    JsonArrayConstructor(crate::rawexprnodes::JsonArrayConstructor<'mcx>),
    /// `T_JsonArrayQueryConstructor` (raw).
    JsonArrayQueryConstructor(crate::rawexprnodes::JsonArrayQueryConstructor<'mcx>),
    /// `T_JsonAggConstructor` (raw).
    JsonAggConstructor(crate::rawexprnodes::JsonAggConstructor<'mcx>),
    /// `T_JsonObjectAgg` (raw).
    JsonObjectAgg(crate::rawexprnodes::JsonObjectAgg<'mcx>),
    /// `T_JsonArrayAgg` (raw).
    JsonArrayAgg(crate::rawexprnodes::JsonArrayAgg<'mcx>),
    /// `T_JsonParseExpr` (raw).
    JsonParseExpr(crate::rawexprnodes::JsonParseExpr<'mcx>),
    /// `T_JsonScalarExpr` (raw).
    JsonScalarExpr(crate::rawexprnodes::JsonScalarExpr<'mcx>),
    /// `T_JsonSerializeExpr` (raw).
    JsonSerializeExpr(crate::rawexprnodes::JsonSerializeExpr<'mcx>),
    /// `T_JsonArgument` (raw).
    JsonArgument(crate::rawexprnodes::JsonArgument<'mcx>),
    /// `T_JsonFuncExpr` (raw).
    JsonFuncExpr(crate::rawexprnodes::JsonFuncExpr<'mcx>),
    /// `T_JsonTablePathSpec` (raw).
    JsonTablePathSpec(crate::rawexprnodes::JsonTablePathSpec<'mcx>),
    /// `T_JsonTable` (raw).
    JsonTable(crate::rawexprnodes::JsonTable<'mcx>),
    /// `T_JsonTableColumn` (raw).
    JsonTableColumn(crate::rawexprnodes::JsonTableColumn<'mcx>),
    /// `T_JsonTablePathScan` — a JSON_TABLE path-scan plan node (`tf->plan`).
    JsonTablePathScan(crate::primnodes::JsonTablePathScan<'mcx>),
    /// `T_JsonTableSiblingJoin` — a JSON_TABLE sibling-join plan node.
    JsonTableSiblingJoin(crate::primnodes::JsonTableSiblingJoin<'mcx>),
    // --- value nodes (nodes/value.h) — leaf literals ---
    // The lexer/grammar emits these `Value`-family leaves (`A_Const.val`,
    // operator-name `list_make1(makeString(name))`, etc.) as `Node *`, so they
    // must be arms of the central `Node` enum to flow through the generic
    // `Node *` machinery. Additive (`#[non_exhaustive]`); tags verified vs
    // nodes/nodetags.h (T_Integer=465 … T_BitString=469).
    /// `T_Integer`.
    Integer(crate::value::Integer),
    /// `T_Float`.
    Float(crate::value::Float<'mcx>),
    /// `T_Boolean`.
    Boolean(crate::value::Boolean),
    /// `T_String` (the `String` value node; the carrier is `StringNode` to avoid
    /// colliding with Rust's `String`).
    String(crate::value::StringNode<'mcx>),
    /// `T_BitString`.
    BitString(crate::value::BitString<'mcx>),
    /// An expression node (`Const`, `BoolExpr`, `Var`, …) carried as a `Node`.
    ///
    /// In C, every `Expr`-derived node is a `Node *` via the
    /// `Expr`/`Node` supertype relationship, and constructors such as
    /// `makeConst`/`makeBoolExpr` are routinely cast to `(Node *)` and returned
    /// through `Node *`-typed APIs (`get_typdefault`, the partition-qual list,
    /// `stringToNode`). This variant is that cast: it embeds the lifetime-free
    /// [`crate::primnodes::Expr`] subtree as a `Node` without collapsing the two
    /// types (the split Expr/Node model is preserved — `Expr` remains its own
    /// enum, this arm only makes an expression reachable where a `Node` is
    /// expected). Additive: the enum is `#[non_exhaustive]`.
    Expr(crate::primnodes::Expr),
    /// `T_List` — a bare `List *` node carried as a `Node *`.
    ///
    /// PostgreSQL parse trees contain `List` nodes directly where a `Node *`
    /// slot holds a list of nodes — most visibly the rows of `VALUES`
    /// (`SelectStmt.valuesLists` is a `List` of `List *`, each sublist a `Node`)
    /// and a few other list-of-lists fields. A `List` is itself a `Node`
    /// (`nodeTag == T_List`), so it is an arm of the central `Node` enum,
    /// holding the sublist's elements. Additive (`#[non_exhaustive]`).
    List(PgVec<'mcx, NodePtr<'mcx>>),
    /// `T_IntList` — a `List *` of bare integers carried as a `Node *`.
    ///
    /// In C an integer `List` (`nodeTag == T_IntList`) holds raw `int` cells
    /// built by `lappend_int`, not boxed `Integer` value-nodes. It is itself a
    /// `Node`, so it is an arm of the central `Node` enum. Used where a `Node *`
    /// slot holds a list of ints — most visibly the expanded `groupingSets`,
    /// where `expand_grouping_sets` stores a `List` of `T_IntList`s back into
    /// `Query.groupingSets`. Additive (`#[non_exhaustive]`).
    IntList(PgVec<'mcx, i32>),

    // --- raw-grammar DDL "CREATE" family nodes (crate::ddlnodes) ---
    RoleSpec(crate::ddlnodes::RoleSpec<'mcx>),
    DefElem(crate::ddlnodes::DefElem<'mcx>),
    Constraint(crate::ddlnodes::Constraint<'mcx>),
    TableLikeClause(crate::ddlnodes::TableLikeClause<'mcx>),
    IndexElem(crate::ddlnodes::IndexElem<'mcx>),
    FunctionParameter(crate::ddlnodes::FunctionParameter<'mcx>),
    ObjectWithArgs(crate::ddlnodes::ObjectWithArgs<'mcx>),
    AccessPriv(crate::ddlnodes::AccessPriv<'mcx>),
    CreateOpClassItem(crate::ddlnodes::CreateOpClassItem<'mcx>),
    StatsElem(crate::ddlnodes::StatsElem<'mcx>),
    PartitionElem(crate::ddlnodes::PartitionElem<'mcx>),
    PartitionSpec(crate::ddlnodes::PartitionSpec<'mcx>),
    PartitionBoundSpec(crate::ddlnodes::PartitionBoundSpec<'mcx>),
    PartitionRangeDatum(crate::ddlnodes::PartitionRangeDatum<'mcx>),
    IntoClause(crate::ddlnodes::IntoClause<'mcx>),
    CreateStmt(crate::ddlnodes::CreateStmt<'mcx>),
    IndexStmt(crate::ddlnodes::IndexStmt<'mcx>),
    CreateSeqStmt(crate::ddlnodes::CreateSeqStmt<'mcx>),
    CreateStatsStmt(crate::ddlnodes::CreateStatsStmt<'mcx>),
    CreateFunctionStmt(crate::ddlnodes::CreateFunctionStmt<'mcx>),
    DefineStmt(crate::ddlnodes::DefineStmt<'mcx>),
    CreateDomainStmt(crate::ddlnodes::CreateDomainStmt<'mcx>),
    CompositeTypeStmt(crate::ddlnodes::CompositeTypeStmt<'mcx>),
    CreateEnumStmt(crate::ddlnodes::CreateEnumStmt<'mcx>),
    CreateRangeStmt(crate::ddlnodes::CreateRangeStmt<'mcx>),
    ViewStmt(crate::ddlnodes::ViewStmt<'mcx>),
    CreateTableAsStmt(crate::ddlnodes::CreateTableAsStmt<'mcx>),
    CreateSchemaStmt(crate::ddlnodes::CreateSchemaStmt<'mcx>),
    CreateExtensionStmt(crate::ddlnodes::CreateExtensionStmt<'mcx>),
    CreateTrigStmt(crate::ddlnodes::CreateTrigStmt<'mcx>),
    CreateRoleStmt(crate::ddlnodes::CreateRoleStmt<'mcx>),
    CreatedbStmt(crate::ddlnodes::CreatedbStmt<'mcx>),
    CreateCastStmt(crate::ddlnodes::CreateCastStmt<'mcx>),
    CreateOpClassStmt(crate::ddlnodes::CreateOpClassStmt<'mcx>),
    CreateOpFamilyStmt(crate::ddlnodes::CreateOpFamilyStmt<'mcx>),
    CreatePLangStmt(crate::ddlnodes::CreatePLangStmt<'mcx>),
    CreateTableSpaceStmt(crate::ddlnodes::CreateTableSpaceStmt<'mcx>),
    CreateConversionStmt(crate::ddlnodes::CreateConversionStmt<'mcx>),
    CreateAmStmt(crate::ddlnodes::CreateAmStmt<'mcx>),

    // --- raw-grammar DDL "ALTER/DROP" family nodes (crate::ddlnodes) ---
    PartitionCmd(crate::ddlnodes::PartitionCmd<'mcx>),
    ReplicaIdentityStmt(crate::ddlnodes::ReplicaIdentityStmt<'mcx>),
    ATAlterConstraint(crate::ddlnodes::ATAlterConstraint<'mcx>),
    AlterTableStmt(crate::ddlnodes::AlterTableStmt<'mcx>),
    AlterTableCmd(crate::ddlnodes::AlterTableCmd<'mcx>),
    AlterCollationStmt(crate::ddlnodes::AlterCollationStmt<'mcx>),
    AlterDomainStmt(crate::ddlnodes::AlterDomainStmt<'mcx>),
    AlterEnumStmt(crate::ddlnodes::AlterEnumStmt<'mcx>),
    AlterStatsStmt(crate::ddlnodes::AlterStatsStmt<'mcx>),
    AlterSeqStmt(crate::ddlnodes::AlterSeqStmt<'mcx>),
    AlterOpFamilyStmt(crate::ddlnodes::AlterOpFamilyStmt<'mcx>),
    AlterFunctionStmt(crate::ddlnodes::AlterFunctionStmt<'mcx>),
    DropStmt(crate::ddlnodes::DropStmt<'mcx>),
    RenameStmt(crate::ddlnodes::RenameStmt<'mcx>),
    AlterObjectDependsStmt(crate::ddlnodes::AlterObjectDependsStmt<'mcx>),
    AlterObjectSchemaStmt(crate::ddlnodes::AlterObjectSchemaStmt<'mcx>),
    AlterOwnerStmt(crate::ddlnodes::AlterOwnerStmt<'mcx>),
    AlterOperatorStmt(crate::ddlnodes::AlterOperatorStmt<'mcx>),
    AlterTypeStmt(crate::ddlnodes::AlterTypeStmt<'mcx>),
    AlterDefaultPrivilegesStmt(crate::ddlnodes::AlterDefaultPrivilegesStmt<'mcx>),
    AlterRoleStmt(crate::ddlnodes::AlterRoleStmt<'mcx>),
    AlterRoleSetStmt(crate::ddlnodes::AlterRoleSetStmt<'mcx>),
    DropOwnedStmt(crate::ddlnodes::DropOwnedStmt<'mcx>),
    ReassignOwnedStmt(crate::ddlnodes::ReassignOwnedStmt<'mcx>),
    AlterTableSpaceOptionsStmt(crate::ddlnodes::AlterTableSpaceOptionsStmt<'mcx>),
    AlterTableMoveAllStmt(crate::ddlnodes::AlterTableMoveAllStmt<'mcx>),
    AlterExtensionStmt(crate::ddlnodes::AlterExtensionStmt<'mcx>),
    AlterExtensionContentsStmt(crate::ddlnodes::AlterExtensionContentsStmt<'mcx>),
    AlterFdwStmt(crate::ddlnodes::AlterFdwStmt<'mcx>),
    AlterForeignServerStmt(crate::ddlnodes::AlterForeignServerStmt<'mcx>),
    AlterUserMappingStmt(crate::ddlnodes::AlterUserMappingStmt<'mcx>),
    AlterPolicyStmt(crate::ddlnodes::AlterPolicyStmt<'mcx>),
    AlterDatabaseStmt(crate::ddlnodes::AlterDatabaseStmt<'mcx>),
    AlterDatabaseRefreshCollStmt(crate::ddlnodes::AlterDatabaseRefreshCollStmt<'mcx>),
    AlterDatabaseSetStmt(crate::ddlnodes::AlterDatabaseSetStmt<'mcx>),
    AlterTSDictionaryStmt(crate::ddlnodes::AlterTSDictionaryStmt<'mcx>),
    AlterTSConfigurationStmt(crate::ddlnodes::AlterTSConfigurationStmt<'mcx>),
    AlterPublicationStmt(crate::ddlnodes::AlterPublicationStmt<'mcx>),
    AlterSubscriptionStmt(crate::ddlnodes::AlterSubscriptionStmt<'mcx>),
    // raw-grammar utility / GRANT / transaction family (parser grammar F4).
    CheckPointStmt(crate::ddlnodes::CheckPointStmt),
    DiscardStmt(crate::ddlnodes::DiscardStmt),
    GrantStmt(crate::ddlnodes::GrantStmt<'mcx>),
    GrantRoleStmt(crate::ddlnodes::GrantRoleStmt<'mcx>),
    VariableSetStmt(crate::ddlnodes::VariableSetStmt<'mcx>),
    VariableShowStmt(crate::ddlnodes::VariableShowStmt<'mcx>),
    TransactionStmt(crate::ddlnodes::TransactionStmt<'mcx>),
    CopyStmt(crate::ddlnodes::CopyStmt<'mcx>),
    ExplainStmt(crate::ddlnodes::ExplainStmt<'mcx>),
    PrepareStmt(crate::ddlnodes::PrepareStmt<'mcx>),
    ExecuteStmt(crate::ddlnodes::ExecuteStmt<'mcx>),
    DeallocateStmt(crate::ddlnodes::DeallocateStmt<'mcx>),
    DeclareCursorStmt(crate::ddlnodes::DeclareCursorStmt<'mcx>),
    ClosePortalStmt(crate::ddlnodes::ClosePortalStmt<'mcx>),
    FetchStmt(crate::ddlnodes::FetchStmt<'mcx>),
    VacuumStmt(crate::ddlnodes::VacuumStmt<'mcx>),
    VacuumRelation(crate::ddlnodes::VacuumRelation<'mcx>),
    ClusterStmt(crate::ddlnodes::ClusterStmt<'mcx>),
    ReindexStmt(crate::ddlnodes::ReindexStmt<'mcx>),
    LockStmt(crate::ddlnodes::LockStmt<'mcx>),
    ConstraintsSetStmt(crate::ddlnodes::ConstraintsSetStmt<'mcx>),
    LoadStmt(crate::ddlnodes::LoadStmt<'mcx>),
    TruncateStmt(crate::ddlnodes::TruncateStmt<'mcx>),
    CommentStmt(crate::ddlnodes::CommentStmt<'mcx>),
    SecLabelStmt(crate::ddlnodes::SecLabelStmt<'mcx>),
    RuleStmt(crate::ddlnodes::RuleStmt<'mcx>),
    NotifyStmt(crate::ddlnodes::NotifyStmt<'mcx>),
    ListenStmt(crate::ddlnodes::ListenStmt<'mcx>),
    UnlistenStmt(crate::ddlnodes::UnlistenStmt<'mcx>),
    DoStmt(crate::ddlnodes::DoStmt<'mcx>),
    CallStmt(crate::ddlnodes::CallStmt<'mcx>),
    RefreshMatViewStmt(crate::ddlnodes::RefreshMatViewStmt<'mcx>),
    AlterSystemStmt(crate::ddlnodes::AlterSystemStmt<'mcx>),
    DropdbStmt(crate::ddlnodes::DropdbStmt<'mcx>),
    DropRoleStmt(crate::ddlnodes::DropRoleStmt<'mcx>),
    DropTableSpaceStmt(crate::ddlnodes::DropTableSpaceStmt<'mcx>),
    CreateFdwStmt(crate::ddlnodes::CreateFdwStmt<'mcx>),
    CreateForeignServerStmt(crate::ddlnodes::CreateForeignServerStmt<'mcx>),
    CreateForeignTableStmt(crate::ddlnodes::CreateForeignTableStmt<'mcx>),
    CreateUserMappingStmt(crate::ddlnodes::CreateUserMappingStmt<'mcx>),
    DropUserMappingStmt(crate::ddlnodes::DropUserMappingStmt<'mcx>),
    ImportForeignSchemaStmt(crate::ddlnodes::ImportForeignSchemaStmt<'mcx>),
    CreatePolicyStmt(crate::ddlnodes::CreatePolicyStmt<'mcx>),
    PublicationTable(crate::ddlnodes::PublicationTable<'mcx>),
    PublicationObjSpec(crate::ddlnodes::PublicationObjSpec<'mcx>),
    CreatePublicationStmt(crate::ddlnodes::CreatePublicationStmt<'mcx>),
    CreateSubscriptionStmt(crate::ddlnodes::CreateSubscriptionStmt<'mcx>),
    DropSubscriptionStmt(crate::ddlnodes::DropSubscriptionStmt<'mcx>),
    CreateEventTrigStmt(crate::ddlnodes::CreateEventTrigStmt<'mcx>),
    AlterEventTrigStmt(crate::ddlnodes::AlterEventTrigStmt<'mcx>),
    CreateTransformStmt(crate::ddlnodes::CreateTransformStmt<'mcx>),
    ReturnStmt(crate::ddlnodes::ReturnStmt<'mcx>),
    PLAssignStmt(crate::ddlnodes::PLAssignStmt<'mcx>),
