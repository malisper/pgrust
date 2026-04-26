use crate::include::catalog::PolicyCommand;
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{
    AggAccum, Expr, JoinType, QueryColumn, RelationDesc, SetReturningCall, SortGroupClause,
    TargetEntry, ToastRelationRef, WindowClause,
};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UngroupedColumnClause {
    SelectTarget,
    Having,
    OrderBy,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Positioned {
        source: Box<ParseError>,
        position: usize,
    },
    UnexpectedEof,
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    InvalidInteger(String),
    InvalidNumeric(String),
    UnknownTable(String),
    UnknownColumn(String),
    MissingKeyColumn(String),
    AmbiguousColumn(String),
    InvalidFromClauseReference(String),
    MissingFromClauseEntry(String),
    DuplicateTableName(String),
    EmptySelectList,
    UnsupportedQualifiedName(String),
    InvalidInsertTargetCount {
        expected: usize,
        actual: usize,
    },
    TableAlreadyExists(String),
    TableDoesNotExist(String),
    UnsupportedType(String),
    MissingDefaultOpclass {
        access_method: String,
        type_name: String,
    },
    WindowingError(String),
    UndefinedOperator {
        op: &'static str,
        left_type: String,
        right_type: String,
    },
    UngroupedColumn {
        display_name: String,
        token: String,
        clause: UngroupedColumnClause,
    },
    SubqueryMustReturnOneColumn,
    UnknownConfigurationParameter(String),
    UnrecognizedParameter(String),
    UnrecognizedPublicationParameter(String),
    UnrecognizedPublicationOptionValue {
        option: String,
        value: String,
    },
    InvalidPublicationParameterValue {
        parameter: String,
        value: String,
    },
    InvalidPublicationTableName(String),
    InvalidPublicationSchemaName(String),
    ConflictingOrRedundantOptions {
        option: String,
    },
    CantChangeRuntimeParam(String),
    TablesDeclaredWithOidsNotSupported,
    OuterLevelAggregateNestedCte(String),
    ActiveSqlTransaction(&'static str),
    OnCommitOnlyForTempTables,
    TempTableInNonTempSchema(String),
    OnlyTemporaryRelationsInTemporarySchemas(String),
    InvalidTableDefinition(String),
    NoSchemaSelectedForCreate,
    FeatureNotSupported(String),
    FeatureNotSupportedMessage(String),
    DetailedError {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
    InvalidRecursion(String),
    WrongObjectType {
        name: String,
        expected: &'static str,
    },
    RecursiveView(String),
}

impl ParseError {
    pub fn with_position(self, position: usize) -> Self {
        match self {
            ParseError::Positioned { source, .. } => ParseError::Positioned { source, position },
            source => ParseError::Positioned {
                source: Box::new(source),
                position,
            },
        }
    }

    pub fn position(&self) -> Option<usize> {
        match self {
            ParseError::Positioned { position, .. } => Some(*position),
            _ => None,
        }
    }

    pub fn unpositioned(&self) -> &ParseError {
        match self {
            ParseError::Positioned { source, .. } => source.unpositioned(),
            other => other,
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.unpositioned() {
            ParseError::UnexpectedEof => write!(f, "unexpected end of input"),
            ParseError::UnexpectedToken { actual, .. } => write!(f, "{actual}"),
            ParseError::InvalidInteger(value) => write!(f, "invalid integer: {value}"),
            ParseError::InvalidNumeric(value) => write!(f, "invalid numeric: {value}"),
            ParseError::UnknownTable(name) => write!(f, "relation \"{name}\" does not exist"),
            ParseError::UnknownColumn(name) => {
                if name.contains('.') {
                    write!(f, "column {name} does not exist")
                } else {
                    write!(f, "column \"{name}\" does not exist")
                }
            }
            ParseError::MissingKeyColumn(name) => {
                write!(f, "column \"{name}\" named in key does not exist")
            }
            ParseError::AmbiguousColumn(name) => {
                write!(f, "column reference \"{name}\" is ambiguous")
            }
            ParseError::InvalidFromClauseReference(name) => {
                write!(
                    f,
                    "invalid reference to FROM-clause entry for table \"{name}\""
                )
            }
            ParseError::MissingFromClauseEntry(name) => {
                write!(f, "missing FROM-clause entry for table \"{name}\"")
            }
            ParseError::DuplicateTableName(name) => {
                write!(f, "table name \"{name}\" specified more than once")
            }
            ParseError::EmptySelectList => {
                write!(f, "SELECT requires a target list or FROM clause")
            }
            ParseError::UnsupportedQualifiedName(name) => {
                write!(f, "unsupported qualified name: {name}")
            }
            ParseError::InvalidInsertTargetCount { expected, actual } => write!(
                f,
                "INSERT has {actual} values but target list requires {expected}"
            ),
            ParseError::TableAlreadyExists(name) => write!(f, "table already exists: {name}"),
            ParseError::TableDoesNotExist(name) => write!(f, "table \"{name}\" does not exist"),
            ParseError::UnsupportedType(name) => write!(f, "type \"{name}\" does not exist"),
            ParseError::MissingDefaultOpclass {
                access_method,
                type_name,
            } => write!(
                f,
                "data type {type_name} has no default operator class for access method \"{access_method}\""
            ),
            ParseError::WindowingError(message) => write!(f, "{message}"),
            ParseError::UndefinedOperator {
                op,
                left_type,
                right_type,
            } => {
                write!(f, "operator does not exist: {left_type} {op} {right_type}")
            }
            ParseError::UngroupedColumn { display_name, .. } => {
                write!(
                    f,
                    "column \"{display_name}\" must appear in the GROUP BY clause or be used in an aggregate function"
                )
            }
            ParseError::SubqueryMustReturnOneColumn => {
                write!(f, "subquery must return only one column")
            }
            ParseError::UnknownConfigurationParameter(name) => {
                write!(f, "unrecognized configuration parameter \"{name}\"")
            }
            ParseError::UnrecognizedParameter(name) => {
                write!(f, "unrecognized parameter \"{name}\"")
            }
            ParseError::UnrecognizedPublicationParameter(name) => {
                write!(f, "unrecognized publication parameter: \"{name}\"")
            }
            ParseError::UnrecognizedPublicationOptionValue { option, value } => {
                write!(
                    f,
                    "unrecognized value for publication option \"{option}\": \"{value}\""
                )
            }
            ParseError::InvalidPublicationParameterValue { parameter, value } => {
                write!(
                    f,
                    "invalid value for publication parameter \"{parameter}\": \"{value}\""
                )
            }
            ParseError::InvalidPublicationTableName(_) => {
                write!(f, "invalid table name")
            }
            ParseError::InvalidPublicationSchemaName(_) => {
                write!(f, "invalid schema name")
            }
            ParseError::ConflictingOrRedundantOptions { .. } => {
                write!(f, "conflicting or redundant options")
            }
            ParseError::CantChangeRuntimeParam(name) => {
                write!(f, "parameter \"{name}\" cannot be changed now")
            }
            ParseError::TablesDeclaredWithOidsNotSupported => {
                write!(f, "tables declared WITH OIDS are not supported")
            }
            ParseError::OuterLevelAggregateNestedCte(_) => {
                write!(f, "outer-level aggregate cannot use a nested CTE")
            }
            ParseError::ActiveSqlTransaction(stmt) => {
                write!(f, "{stmt} cannot run inside a transaction block")
            }
            ParseError::OnCommitOnlyForTempTables => {
                write!(f, "ON COMMIT can only be used on temporary tables")
            }
            ParseError::TempTableInNonTempSchema(_name) => {
                write!(
                    f,
                    "cannot create temporary relation in non-temporary schema"
                )
            }
            ParseError::OnlyTemporaryRelationsInTemporarySchemas(name) => {
                let _ = name;
                write!(
                    f,
                    "only temporary relations may be created in temporary schemas"
                )
            }
            ParseError::InvalidTableDefinition(message) => write!(f, "{message}"),
            ParseError::NoSchemaSelectedForCreate => {
                write!(f, "no schema has been selected to create in")
            }
            ParseError::FeatureNotSupported(feature) => {
                write!(f, "feature not supported: {feature}")
            }
            ParseError::FeatureNotSupportedMessage(message) => write!(f, "{message}"),
            ParseError::DetailedError { message, .. } => write!(f, "{message}"),
            ParseError::InvalidRecursion(message) => {
                write!(f, "{message}")
            }
            ParseError::WrongObjectType { name, expected } => {
                write!(f, "\"{name}\" is not a {expected}")
            }
            ParseError::RecursiveView(name) => {
                write!(f, "infinite recursion detected in view \"{name}\"")
            }
            ParseError::Positioned { .. } => {
                unreachable!("positioned parse errors unwrap before display")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ParseError;

    #[test]
    fn unknown_column_display_matches_postgres_shape() {
        assert_eq!(
            ParseError::UnknownColumn("x.t".into()).to_string(),
            "column x.t does not exist"
        );
        assert_eq!(
            ParseError::UnknownColumn("missing".into()).to_string(),
            "column \"missing\" does not exist"
        );
    }

    #[test]
    fn unknown_table_display_matches_postgres_shape() {
        assert_eq!(
            ParseError::UnknownTable("attmp".into()).to_string(),
            "relation \"attmp\" does not exist"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Do(DoStatement),
    Explain(ExplainStatement),
    Show(ShowStatement),
    Select(SelectStatement),
    Values(ValuesStatement),
    CopyFrom(CopyFromStatement),
    CopyTo(CopyToStatement),
    Analyze(AnalyzeStatement),
    Checkpoint(CheckpointStatement),
    Set(SetStatement),
    SetConstraints(SetConstraintsStatement),
    Reset(ResetStatement),
    Call(CallStatement),
    CreateFunction(CreateFunctionStatement),
    CreateProcedure(CreateProcedureStatement),
    CreateAggregate(CreateAggregateStatement),
    CreateCast(CreateCastStatement),
    CreateTrigger(CreateTriggerStatement),
    CreateType(CreateTypeStatement),
    AlterType(AlterTypeStatement),
    AlterTypeOwner(AlterTypeOwnerStatement),
    CreateDatabase(CreateDatabaseStatement),
    AlterDatabase(AlterDatabaseStatement),
    CreateSchema(CreateSchemaStatement),
    CreateTablespace(CreateTablespaceStatement),
    CreateTable(CreateTableStatement),
    CreateTableAs(CreateTableAsStatement),
    CreateSequence(CreateSequenceStatement),
    CreateView(CreateViewStatement),
    RefreshMaterializedView(RefreshMaterializedViewStatement),
    CreateRule(CreateRuleStatement),
    CreatePolicy(CreatePolicyStatement),
    CreateStatistics(CreateStatisticsStatement),
    AlterStatistics(AlterStatisticsStatement),
    CreateForeignDataWrapper(CreateForeignDataWrapperStatement),
    CreateForeignServer(CreateForeignServerStatement),
    CreateForeignTable(CreateForeignTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateOperator(CreateOperatorStatement),
    CreateOperatorClass(CreateOperatorClassStatement),
    AlterSequence(AlterSequenceStatement),
    AlterSequenceOwner(AlterRelationOwnerStatement),
    AlterSequenceRename(AlterTableRenameStatement),
    AlterIndexRename(AlterTableRenameStatement),
    AlterViewRename(AlterTableRenameStatement),
    AlterViewRenameColumn(AlterTableRenameColumnStatement),
    AlterIndexAttachPartition(AlterIndexAttachPartitionStatement),
    AlterIndexAlterColumnStatistics(AlterIndexAlterColumnStatisticsStatement),
    AlterTableAddColumn(AlterTableAddColumnStatement),
    AlterTableAddConstraint(AlterTableAddConstraintStatement),
    AlterTableDropColumn(AlterTableDropColumnStatement),
    AlterTableDropConstraint(AlterTableDropConstraintStatement),
    AlterTableAlterConstraint(AlterTableAlterConstraintStatement),
    AlterTableRenameConstraint(AlterTableRenameConstraintStatement),
    AlterTableAlterColumnType(AlterTableAlterColumnTypeStatement),
    AlterTableAlterColumnDefault(AlterTableAlterColumnDefaultStatement),
    AlterTableAlterColumnExpression(AlterTableAlterColumnExpressionStatement),
    AlterTableAlterColumnCompression(AlterTableAlterColumnCompressionStatement),
    AlterTableAlterColumnStorage(AlterTableAlterColumnStorageStatement),
    AlterTableAlterColumnOptions(AlterTableAlterColumnOptionsStatement),
    AlterTableAlterColumnStatistics(AlterTableAlterColumnStatisticsStatement),
    AlterTableOwner(AlterRelationOwnerStatement),
    AlterTableRenameColumn(AlterTableRenameColumnStatement),
    AlterTableRename(AlterTableRenameStatement),
    AlterTableSetSchema(AlterRelationSetSchemaStatement),
    AlterViewSetSchema(AlterRelationSetSchemaStatement),
    AlterViewOwner(AlterRelationOwnerStatement),
    AlterSchemaOwner(AlterSchemaOwnerStatement),
    AlterTableSet(AlterTableSetStatement),
    AlterTableSetRowSecurity(AlterTableSetRowSecurityStatement),
    AlterPolicy(AlterPolicyStatement),
    AlterTableSetNotNull(AlterTableSetNotNullStatement),
    AlterTableDropNotNull(AlterTableDropNotNullStatement),
    AlterTableValidateConstraint(AlterTableValidateConstraintStatement),
    AlterTableInherit(AlterTableInheritStatement),
    AlterTableNoInherit(AlterTableNoInheritStatement),
    AlterTableOf(AlterTableOfStatement),
    AlterTableNotOf(AlterTableNotOfStatement),
    AlterTableAttachPartition(AlterTableAttachPartitionStatement),
    AlterTableDetachPartition(AlterTableDetachPartitionStatement),
    AlterTableTriggerState(AlterTableTriggerStateStatement),
    AlterPublication(AlterPublicationStatement),
    AlterOperator(AlterOperatorStatement),
    AlterTriggerRename(AlterTriggerRenameStatement),
    CommentOnTable(CommentOnTableStatement),
    CommentOnView(CommentOnViewStatement),
    CommentOnIndex(CommentOnIndexStatement),
    CommentOnType(CommentOnTypeStatement),
    CommentOnColumn(CommentOnColumnStatement),
    CommentOnConstraint(CommentOnConstraintStatement),
    CommentOnRule(CommentOnRuleStatement),
    CommentOnTrigger(CommentOnTriggerStatement),
    CommentOnDomain(CommentOnDomainStatement),
    CommentOnConversion(CommentOnConversionStatement),
    CommentOnForeignDataWrapper(CommentOnForeignDataWrapperStatement),
    CommentOnPublication(CommentOnPublicationStatement),
    CommentOnStatistics(CommentOnStatisticsStatement),
    CommentOnAggregate(CommentOnAggregateStatement),
    CommentOnFunction(CommentOnFunctionStatement),
    CommentOnOperator(CommentOnOperatorStatement),
    CreateDomain(CreateDomainStatement),
    CreateConversion(CreateConversionStatement),
    CreatePublication(CreatePublicationStatement),
    CommentOnRole(CommentOnRoleStatement),
    GrantObject(GrantObjectStatement),
    RevokeObject(RevokeObjectStatement),
    GrantRoleMembership(GrantRoleMembershipStatement),
    RevokeRoleMembership(RevokeRoleMembershipStatement),
    DropType(DropTypeStatement),
    DropSequence(DropSequenceStatement),
    DropConversion(DropConversionStatement),
    DropDatabase(DropDatabaseStatement),
    DropPublication(DropPublicationStatement),
    DropStatistics(DropStatisticsStatement),
    DropCast(DropCastStatement),
    DropFunction(DropFunctionStatement),
    DropProcedure(DropProcedureStatement),
    DropRoutine(DropProcedureStatement),
    DropOperator(DropOperatorStatement),
    DropAggregate(DropAggregateStatement),
    DropTable(DropTableStatement),
    DropTrigger(DropTriggerStatement),
    DropIndex(DropIndexStatement),
    ReindexIndex(ReindexIndexStatement),
    DropDomain(DropDomainStatement),
    DropForeignDataWrapper(DropForeignDataWrapperStatement),
    DropView(DropViewStatement),
    DropMaterializedView(DropMaterializedViewStatement),
    DropRule(DropRuleStatement),
    DropPolicy(DropPolicyStatement),
    DropSchema(DropSchemaStatement),
    CreateRole(CreateRoleStatement),
    AlterRole(AlterRoleStatement),
    AlterProcedure(AlterProcedureStatement),
    AlterRoutine(AlterRoutineStatement),
    AlterForeignDataWrapper(AlterForeignDataWrapperStatement),
    AlterForeignDataWrapperOwner(AlterForeignDataWrapperOwnerStatement),
    AlterForeignDataWrapperRename(AlterForeignDataWrapperRenameStatement),
    DropRole(DropRoleStatement),
    SetRole(SetRoleStatement),
    ResetRole(ResetRoleStatement),
    SetSessionAuthorization(SetSessionAuthorizationStatement),
    ResetSessionAuthorization(ResetSessionAuthorizationStatement),
    DropOwned(DropOwnedStatement),
    ReassignOwned(ReassignOwnedStatement),
    TruncateTable(TruncateTableStatement),
    Vacuum(VacuumStatement),
    Notify(NotifyStatement),
    Listen(ListenStatement),
    Unlisten(UnlistenStatement),
    DeclareCursor(DeclareCursorStatement),
    Fetch(FetchStatement),
    Move(FetchStatement),
    ClosePortal(ClosePortalStatement),
    Insert(InsertStatement),
    Merge(MergeStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Unsupported(UnsupportedStatement),
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackTo(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedStatement {
    pub sql: String,
    pub feature: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub command_type: CommandType,
    pub depends_on_row_security: bool,
    pub rtable: Vec<RangeTblEntry>,
    pub jointree: Option<JoinTreeNode>,
    pub target_list: Vec<TargetEntry>,
    pub distinct: bool,
    pub distinct_on: Vec<SortGroupClause>,
    pub where_qual: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub accumulators: Vec<AggAccum>,
    pub window_clauses: Vec<WindowClause>,
    pub having_qual: Option<Expr>,
    pub sort_clause: Vec<SortGroupClause>,
    pub limit_count: Option<usize>,
    pub limit_offset: usize,
    pub locking_clause: Option<SelectLockingClause>,
    pub row_marks: Vec<QueryRowMark>,
    pub has_target_srfs: bool,
    pub recursive_union: Option<Box<RecursiveUnionQuery>>,
    pub set_operation: Option<Box<SetOperationQuery>>,
}

impl Query {
    pub fn columns(&self) -> Vec<QueryColumn> {
        self.target_list
            .iter()
            .map(|target| QueryColumn {
                name: target.name.clone(),
                sql_type: target.sql_type,
                wire_type_oid: None,
            })
            .collect()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns()
            .into_iter()
            .map(|column| column.name)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRowMark {
    pub rtindex: usize,
    pub strength: SelectLockingClause,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTblEntry {
    pub alias: Option<String>,
    pub desc: RelationDesc,
    pub inh: bool,
    pub security_quals: Vec<Expr>,
    pub kind: RangeTblEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeTblEntryKind {
    Result,
    Relation {
        rel: crate::RelFileLocator,
        relation_oid: u32,
        relkind: char,
        relispopulated: bool,
        toast: Option<ToastRelationRef>,
    },
    Join {
        jointype: JoinType,
        joinmergedcols: usize,
        joinaliasvars: Vec<Expr>,
        joinleftcols: Vec<usize>,
        joinrightcols: Vec<usize>,
    },
    Values {
        rows: Vec<Vec<Expr>>,
        output_columns: Vec<QueryColumn>,
    },
    Function {
        call: SetReturningCall,
    },
    WorkTable {
        worktable_id: usize,
    },
    Cte {
        cte_id: usize,
        query: Box<Query>,
    },
    Subquery {
        query: Box<Query>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecursiveUnionQuery {
    pub output_desc: RelationDesc,
    pub anchor: Query,
    pub recursive: Query,
    pub distinct: bool,
    pub recursive_references_worktable: bool,
    pub worktable_id: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOperationQuery {
    pub output_desc: RelationDesc,
    pub op: SetOperator,
    pub inputs: Vec<Query>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinTreeNode {
    RangeTblRef(usize),
    JoinExpr {
        left: Box<JoinTreeNode>,
        right: Box<JoinTreeNode>,
        kind: JoinType,
        quals: Expr,
        rtindex: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoStatement {
    pub language: Option<String>,
    pub code: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionArgMode {
    In,
    Out,
    InOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionVolatility {
    Volatile,
    Stable,
    Immutable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionParallel {
    Unsafe,
    Restricted,
    Safe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateArgType {
    Type(RawTypeName),
    AnyPseudo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateSignatureKind {
    Star,
    Args(Vec<AggregateArgType>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunctionArg {
    pub mode: FunctionArgMode,
    pub name: Option<String>,
    pub ty: RawTypeName,
    pub type_position: Option<usize>,
    pub default_expr: Option<String>,
    pub variadic: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunctionTableColumn {
    pub name: String,
    pub ty: RawTypeName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateFunctionReturnSpec {
    Type { ty: RawTypeName, setof: bool },
    Table(Vec<CreateFunctionTableColumn>),
    DerivedFromOutArgs { setof_record: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunctionStatement {
    pub schema_name: Option<String>,
    pub function_name: String,
    pub replace_existing: bool,
    pub cost: Option<String>,
    pub args: Vec<CreateFunctionArg>,
    pub return_spec: CreateFunctionReturnSpec,
    pub strict: bool,
    pub leakproof: bool,
    pub volatility: FunctionVolatility,
    pub parallel: FunctionParallel,
    pub language: String,
    pub body: String,
    pub link_symbol: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallStatement {
    pub schema_name: Option<String>,
    pub procedure_name: String,
    pub args: SqlCallArgs,
    pub raw_arg_sql: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProcedureStatement {
    pub schema_name: Option<String>,
    pub procedure_name: String,
    pub replace_existing: bool,
    pub args: Vec<CreateFunctionArg>,
    pub strict: bool,
    pub volatility: FunctionVolatility,
    pub language: String,
    pub body: String,
    pub sql_standard_body: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineKind {
    Function,
    Procedure,
    Routine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineSignature {
    pub schema_name: Option<String>,
    pub routine_name: String,
    pub arg_types: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastContext {
    Explicit,
    Assignment,
    Implicit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateCastMethod {
    Function {
        schema_name: Option<String>,
        function_name: String,
        arg_types: Vec<RawTypeName>,
    },
    WithoutFunction,
    InOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCastStatement {
    pub source_type: RawTypeName,
    pub target_type: RawTypeName,
    pub method: CreateCastMethod,
    pub context: CastContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropCastStatement {
    pub if_exists: bool,
    pub source_type: RawTypeName,
    pub target_type: RawTypeName,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterProcedureAction {
    Strict,
    Volatility(FunctionVolatility),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterProcedureStatement {
    pub schema_name: Option<String>,
    pub procedure_name: String,
    pub arg_types: Vec<String>,
    pub action: AlterProcedureAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterRoutineOption {
    Strict(bool),
    Volatility(FunctionVolatility),
    SecurityDefiner(bool),
    Leakproof(bool),
    Parallel(FunctionParallel),
    Cost(String),
    Rows(String),
    Support(String),
    SetConfig { name: String, value: String },
    ResetConfig(String),
    ResetAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterRoutineAction {
    Options(Vec<AlterRoutineOption>),
    Rename {
        new_name: String,
    },
    SetSchema {
        new_schema: String,
    },
    OwnerTo {
        new_owner: String,
    },
    DependsOnExtension {
        extension_name: String,
        remove: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRoutineStatement {
    pub kind: RoutineKind,
    pub signature: RoutineSignature,
    pub action: AlterRoutineAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAggregateStatement {
    pub schema_name: Option<String>,
    pub aggregate_name: String,
    pub replace_existing: bool,
    pub signature: AggregateSignatureKind,
    pub sfunc_name: String,
    pub stype: RawTypeName,
    pub finalfunc_name: Option<String>,
    pub initcond: Option<String>,
    pub parallel: Option<FunctionParallel>,
    pub transspace: i32,
    pub combinefunc_name: Option<String>,
    pub serialfunc_name: Option<String>,
    pub deserialfunc_name: Option<String>,
    pub finalfunc_extra: bool,
    pub finalfunc_modify: char,
    pub mstype: Option<RawTypeName>,
    pub msfunc_name: Option<String>,
    pub minvfunc_name: Option<String>,
    pub mfinalfunc_name: Option<String>,
    pub minitcond: Option<String>,
    pub mtransspace: i32,
    pub mfinalfunc_extra: bool,
    pub mfinalfunc_modify: char,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
    Instead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerLevel {
    Row,
    Statement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEventSpec {
    pub event: TriggerEvent,
    pub update_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerReferencingSpec {
    pub is_new: bool,
    pub is_table: bool,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTriggerStatement {
    pub replace_existing: bool,
    pub trigger_name: String,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub timing: TriggerTiming,
    pub level: TriggerLevel,
    pub events: Vec<TriggerEventSpec>,
    pub referencing: Vec<TriggerReferencingSpec>,
    pub when_clause_sql: Option<String>,
    pub function_schema_name: Option<String>,
    pub function_name: String,
    pub func_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTypeStatement {
    Shell(CreateShellTypeStatement),
    Base(CreateBaseTypeStatement),
    Composite(CreateCompositeTypeStatement),
    Enum(CreateEnumTypeStatement),
    Range(CreateRangeTypeStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateShellTypeStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBaseTypeStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub options: Vec<CreateBaseTypeOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBaseTypeOption {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCompositeTypeStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub attributes: Vec<CompositeTypeAttributeDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeTypeAttributeDef {
    pub name: String,
    pub ty: RawTypeName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateEnumTypeStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTypeStatement {
    AddEnumValue(AlterTypeAddEnumValueStatement),
    RenameEnumValue(AlterTypeRenameEnumValueStatement),
    RenameType(AlterTypeRenameTypeStatement),
    AlterComposite(AlterCompositeTypeStatement),
    SetOptions(AlterTypeSetOptionsStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterEnumValuePosition {
    Before(String),
    After(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTypeAddEnumValueStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub if_not_exists: bool,
    pub label: String,
    pub position: Option<AlterEnumValuePosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTypeRenameEnumValueStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub old_label: String,
    pub new_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTypeRenameTypeStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub new_type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterCompositeTypeStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub actions: Vec<AlterCompositeTypeAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterCompositeTypeAction {
    AddAttribute {
        attribute: CompositeTypeAttributeDef,
        cascade: bool,
    },
    DropAttribute {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    AlterAttributeType {
        name: String,
        ty: RawTypeName,
        cascade: bool,
    },
    RenameAttribute {
        old_name: String,
        new_name: String,
        cascade: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTypeSetOptionsStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub options: Vec<CreateBaseTypeOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRangeTypeStatement {
    pub schema_name: Option<String>,
    pub type_name: String,
    pub subtype: RawTypeName,
    pub subtype_opclass: Option<String>,
    pub subtype_diff: Option<String>,
    pub collation: Option<String>,
    pub multirange_type_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TablePersistence {
    Permanent,
    Temporary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCommitAction {
    PreserveRows,
    DeleteRows,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetStatement {
    pub name: String,
    pub value: Option<String>,
    pub is_local: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetConstraintsStatement {
    pub constraints: Option<Vec<QualifiedNameRef>>,
    pub deferred: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlOption {
    Document,
    Content,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlStandalone {
    Yes,
    No,
    NoValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawXmlExprOp {
    Concat,
    Element,
    Forest,
    Parse,
    Pi,
    Root,
    Serialize,
    IsDocument,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawXmlExpr {
    pub op: RawXmlExprOp,
    pub name: Option<String>,
    pub named_args: Vec<SqlExpr>,
    pub arg_names: Vec<String>,
    pub args: Vec<SqlExpr>,
    pub xml_option: Option<XmlOption>,
    pub indent: Option<bool>,
    pub target_type: Option<RawTypeName>,
    pub standalone: Option<XmlStandalone>,
}

impl RawXmlExpr {
    pub fn child_exprs(&self) -> impl Iterator<Item = &SqlExpr> {
        self.named_args.iter().chain(self.args.iter())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetStatement {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowStatement {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyStatement {
    pub channel: String,
    pub payload: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenStatement {
    pub channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlistenStatement {
    pub channel: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorScrollOption {
    Unspecified,
    Scroll,
    NoScroll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclareCursorStatement {
    pub name: String,
    pub binary: bool,
    pub insensitive: bool,
    pub scroll: CursorScrollOption,
    pub hold: bool,
    pub query: SelectStatement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute(i64),
    Relative(i64),
    Forward(Option<i64>),
    Backward(Option<i64>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchStatement {
    pub cursor_name: String,
    pub direction: FetchDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosePortalStatement {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatement {
    pub analyze: bool,
    pub buffers: bool,
    pub costs: bool,
    pub timing: bool,
    pub verbose: bool,
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopySource {
    File(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: CopyFormat,
    pub encoding: Option<String>,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            format: CopyFormat::Text,
            encoding: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFromStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub source: CopySource,
    pub options: CopyOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyForceQuote {
    None,
    All,
    Columns(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyToOptions {
    pub format: CopyFormat,
    pub encoding: Option<String>,
    pub delimiter: String,
    pub null: String,
    pub header: bool,
    pub quote: String,
    pub escape: String,
    pub force_quote: CopyForceQuote,
}

impl Default for CopyToOptions {
    fn default() -> Self {
        Self {
            format: CopyFormat::Text,
            encoding: None,
            delimiter: "\t".into(),
            null: "\\N".into(),
            header: false,
            quote: "\"".into(),
            escape: "\"".into(),
            force_quote: CopyForceQuote::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToSource {
    Relation {
        table_name: String,
        columns: Option<Vec<String>>,
    },
    Query {
        statement: Box<Statement>,
        sql: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToDestination {
    Stdout,
    File(String),
    Program(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyToStatement {
    pub source: CopyToSource,
    pub destination: CopyToDestination,
    pub options: CopyToOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTarget {
    pub table_name: String,
    pub columns: Vec<String>,
    pub only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeStatement {
    pub targets: Vec<MaintenanceTarget>,
    pub verbose: bool,
    pub skip_locked: bool,
    pub buffer_usage_limit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub with_recursive: bool,
    pub with: Vec<CommonTableExpr>,
    pub distinct: bool,
    pub distinct_on: Vec<SqlExpr>,
    pub from: Option<FromItem>,
    pub targets: Vec<SelectItem>,
    pub where_clause: Option<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub having: Option<SqlExpr>,
    pub window_clauses: Vec<RawWindowClause>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub locking_clause: Option<SelectLockingClause>,
    pub set_operation: Option<Box<SetOperationStatement>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectLockingClause {
    ForNoKeyUpdate,
    ForUpdate,
    ForKeyShare,
    ForShare,
}

impl SelectLockingClause {
    pub fn sql(self) -> &'static str {
        match self {
            SelectLockingClause::ForNoKeyUpdate => "FOR NO KEY UPDATE",
            SelectLockingClause::ForUpdate => "FOR UPDATE",
            SelectLockingClause::ForKeyShare => "FOR KEY SHARE",
            SelectLockingClause::ForShare => "FOR SHARE",
        }
    }

    pub fn strongest(self, other: SelectLockingClause) -> SelectLockingClause {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }

    fn rank(self) -> u8 {
        match self {
            SelectLockingClause::ForKeyShare => 0,
            SelectLockingClause::ForShare => 1,
            SelectLockingClause::ForNoKeyUpdate => 2,
            SelectLockingClause::ForUpdate => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union { all: bool },
    Intersect { all: bool },
    Except { all: bool },
}

impl SetOperator {
    pub fn all(self) -> bool {
        match self {
            SetOperator::Union { all }
            | SetOperator::Intersect { all }
            | SetOperator::Except { all } => all,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOperationStatement {
    pub op: SetOperator,
    pub inputs: Vec<SelectStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesStatement {
    pub with_recursive: bool,
    pub with: Vec<CommonTableExpr>,
    pub rows: Vec<Vec<SqlExpr>>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonTableExpr {
    pub name: String,
    pub column_names: Vec<String>,
    pub body: CteBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCaseWhen {
    pub expr: SqlExpr,
    pub result: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CteBody {
    Select(Box<SelectStatement>),
    Values(ValuesStatement),
    RecursiveUnion {
        all: bool,
        anchor: Box<CteBody>,
        recursive: Box<SelectStatement>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromItem {
    Table {
        name: String,
        only: bool,
    },
    Values {
        rows: Vec<Vec<SqlExpr>>,
    },
    FunctionCall {
        name: String,
        args: Vec<SqlFunctionArg>,
        func_variadic: bool,
        with_ordinality: bool,
    },
    Lateral(Box<FromItem>),
    DerivedTable(Box<SelectStatement>),
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
    Alias {
        source: Box<FromItem>,
        alias: String,
        column_aliases: AliasColumnSpec,
        preserve_source_names: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AliasColumnSpec {
    None,
    Names(Vec<String>),
    Definitions(Vec<AliasColumnDef>),
}

impl AliasColumnSpec {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::None => true,
            Self::Names(names) => names.is_empty(),
            Self::Definitions(defs) => defs.is_empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasColumnDef {
    pub name: String,
    pub ty: RawTypeName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFunctionArg {
    pub name: Option<String>,
    pub value: SqlExpr,
}

impl SqlFunctionArg {
    pub fn positional(value: SqlExpr) -> Self {
        Self { name: None, value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCallArgs {
    Star,
    Args(Vec<SqlFunctionArg>),
}

impl SqlCallArgs {
    pub fn args(&self) -> &[SqlFunctionArg] {
        match self {
            Self::Star => &[],
            Self::Args(args) => args,
        }
    }

    pub fn is_star(&self) -> bool {
        matches!(self, Self::Star)
    }
}

pub fn function_arg_values(args: &SqlCallArgs) -> impl Iterator<Item = &SqlExpr> {
    args.args().iter().map(|arg| &arg.value)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Cross,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinConstraint {
    None,
    On(SqlExpr),
    Using(Vec<String>),
    Natural,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectItem {
    pub output_name: String,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByItem {
    pub expr: SqlExpr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
    pub using_operator: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawWindowSpec {
    pub name: Option<String>,
    pub partition_by: Vec<SqlExpr>,
    pub order_by: Vec<OrderByItem>,
    pub frame: Option<Box<RawWindowFrame>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawWindowClause {
    pub name: String,
    pub spec: RawWindowSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameMode {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawWindowFrameBound {
    UnboundedPreceding,
    OffsetPreceding(Box<SqlExpr>),
    CurrentRow,
    OffsetFollowing(Box<SqlExpr>),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawWindowFrame {
    pub mode: WindowFrameMode,
    pub start_bound: RawWindowFrameBound,
    pub end_bound: RawWindowFrameBound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub with_recursive: bool,
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub table_alias: Option<String>,
    pub columns: Option<Vec<AssignmentTarget>>,
    pub source: InsertSource,
    pub on_conflict: Option<OnConflictClause>,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertSource {
    Values(Vec<Vec<SqlExpr>>),
    DefaultValues,
    Select(Box<SelectStatement>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeStatement {
    pub with_recursive: bool,
    pub with: Vec<CommonTableExpr>,
    pub target_table: String,
    pub target_alias: Option<String>,
    pub target_only: bool,
    pub source: FromItem,
    pub join_condition: SqlExpr,
    pub when_clauses: Vec<MergeWhenClause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeWhenClause {
    pub match_kind: MergeMatchKind,
    pub condition: Option<SqlExpr>,
    pub action: MergeAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMatchKind {
    Matched,
    NotMatchedBySource,
    NotMatchedByTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeAction {
    DoNothing,
    Delete,
    Update {
        assignments: Vec<Assignment>,
    },
    Insert {
        columns: Option<Vec<String>>,
        source: MergeInsertSource,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeInsertSource {
    Values(Vec<SqlExpr>),
    DefaultValues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnConflictClause {
    pub target: Option<OnConflictTarget>,
    pub action: OnConflictAction,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnConflictAction {
    Nothing,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnConflictTarget {
    Inference(OnConflictInferenceSpec),
    Constraint(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnConflictInferenceSpec {
    pub elements: Vec<OnConflictInferenceElem>,
    pub predicate: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnConflictInferenceElem {
    pub expr: SqlExpr,
    pub collation: Option<String>,
    pub opclass: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub of_type_name: Option<String>,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub elements: Vec<CreateTableElement>,
    pub inherits: Vec<String>,
    pub partition_spec: Option<RawPartitionSpec>,
    pub partition_of: Option<String>,
    pub partition_bound: Option<RawPartitionBoundSpec>,
    pub if_not_exists: bool,
}

impl CreateTableStatement {
    pub fn columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.elements.iter().filter_map(|element| match element {
            CreateTableElement::Column(column) => Some(column),
            CreateTableElement::TypedColumnOptions(_)
            | CreateTableElement::PartitionColumnOverride(_)
            | CreateTableElement::Constraint(_)
            | CreateTableElement::Like(_) => None,
        })
    }

    pub fn constraints(&self) -> impl Iterator<Item = &TableConstraint> {
        self.elements.iter().filter_map(|element| match element {
            CreateTableElement::Column(_)
            | CreateTableElement::TypedColumnOptions(_)
            | CreateTableElement::PartitionColumnOverride(_)
            | CreateTableElement::Like(_) => None,
            CreateTableElement::Constraint(constraint) => Some(constraint),
        })
    }

    pub fn partition_column_overrides(&self) -> impl Iterator<Item = &PartitionColumnOverride> {
        self.elements.iter().filter_map(|element| match element {
            CreateTableElement::PartitionColumnOverride(override_) => Some(override_),
            CreateTableElement::Column(_)
            | CreateTableElement::TypedColumnOptions(_)
            | CreateTableElement::Constraint(_)
            | CreateTableElement::Like(_) => None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    List,
    Range,
    Hash,
}

impl PartitionStrategy {
    pub fn catalog_code(self) -> char {
        match self {
            PartitionStrategy::List => 'l',
            PartitionStrategy::Range => 'r',
            PartitionStrategy::Hash => 'h',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPartitionSpec {
    pub strategy: PartitionStrategy,
    pub keys: Vec<RawPartitionKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPartitionKey {
    pub expr: SqlExpr,
    pub expr_sql: String,
    pub opclass: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawPartitionBoundSpec {
    List {
        values: Vec<SqlExpr>,
        is_default: bool,
    },
    Range {
        from: Vec<RawPartitionRangeDatum>,
        to: Vec<RawPartitionRangeDatum>,
        is_default: bool,
    },
    Hash {
        modulus: i32,
        remainder: i32,
    },
}

impl RawPartitionBoundSpec {
    pub fn is_default(&self) -> bool {
        match self {
            RawPartitionBoundSpec::List { is_default, .. }
            | RawPartitionBoundSpec::Range { is_default, .. } => *is_default,
            RawPartitionBoundSpec::Hash { .. } => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawPartitionRangeDatum {
    MinValue,
    MaxValue,
    Value(SqlExpr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabaseStatement {
    pub database_name: String,
    pub options: CreateDatabaseOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateDatabaseOptions {
    pub template: Option<String>,
    pub encoding: Option<String>,
    pub lc_collate: Option<String>,
    pub lc_ctype: Option<String>,
    pub owner: Option<String>,
    pub tablespace: Option<String>,
    pub connection_limit: Option<i32>,
    pub allow_connections: Option<bool>,
    pub is_template: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabaseStatement {
    pub database_name: String,
    pub action: AlterDatabaseAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterDatabaseAction {
    Rename { new_name: String },
    SetTablespace { tablespace_name: String },
    ResetTablespace,
    ConnectionLimit { limit: i32 },
    OwnerTo { new_owner: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSchemaStatement {
    pub schema_name: Option<String>,
    pub auth_role: Option<RoleSpec>,
    pub if_not_exists: bool,
    pub elements: Vec<Box<Statement>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleSpec {
    RoleName(String),
    CurrentUser,
    CurrentRole,
    SessionUser,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTablespaceStatement {
    pub tablespace_name: String,
    pub location: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAsStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub column_names: Vec<String>,
    pub query: SelectStatement,
    pub query_sql: Option<String>,
    pub if_not_exists: bool,
    pub object_type: TableAsObjectType,
    pub skip_data: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableAsObjectType {
    Table,
    MaterializedView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSequenceStatement {
    pub schema_name: Option<String>,
    pub sequence_name: String,
    pub persistence: TablePersistence,
    pub if_not_exists: bool,
    pub options: SequenceOptionsSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateViewStatement {
    pub schema_name: Option<String>,
    pub view_name: String,
    pub persistence: TablePersistence,
    pub options: Vec<RelOption>,
    pub query: SelectStatement,
    pub query_sql: String,
    pub or_replace: bool,
    pub check_option: ViewCheckOption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewCheckOption {
    None,
    Local,
    Cascaded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshMaterializedViewStatement {
    pub relation_name: String,
    pub concurrently: bool,
    pub skip_data: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleEvent {
    Insert,
    Update,
    Delete,
    Select,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleDoKind {
    Also,
    Instead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleActionStatement {
    pub statement: Statement,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRuleStatement {
    pub rule_name: String,
    pub relation_name: String,
    pub event: RuleEvent,
    pub do_kind: RuleDoKind,
    pub where_clause: Option<SqlExpr>,
    pub where_sql: Option<String>,
    pub actions: Vec<RuleActionStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePolicyStatement {
    pub policy_name: String,
    pub table_name: String,
    pub permissive: bool,
    pub command: PolicyCommand,
    pub role_names: Vec<String>,
    pub using_expr: Option<SqlExpr>,
    pub using_sql: Option<String>,
    pub with_check_expr: Option<SqlExpr>,
    pub with_check_sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStatisticsStatement {
    pub if_not_exists: bool,
    pub statistics_name: Option<String>,
    pub kinds: Vec<String>,
    pub targets: Vec<String>,
    pub from_clause: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterStatisticsStatement {
    pub if_exists: bool,
    pub statistics_name: String,
    pub action: AlterStatisticsAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterStatisticsAction {
    Rename { new_name: String },
    SetStatistics { target: i16 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropStatisticsStatement {
    pub if_exists: bool,
    pub statistics_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnStatisticsStatement {
    pub statistics_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub unique: bool,
    pub nulls_not_distinct: bool,
    pub concurrently: bool,
    pub only: bool,
    pub if_not_exists: bool,
    pub index_name: String,
    pub table_name: String,
    pub using_method: Option<String>,
    pub columns: Vec<IndexColumnDef>,
    pub include_columns: Vec<String>,
    pub predicate: Option<SqlExpr>,
    pub predicate_sql: Option<String>,
    pub options: Vec<RelOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterIndexAttachPartitionStatement {
    pub parent_index_name: String,
    pub child_index_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedNameRef {
    pub schema_name: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOperatorStatement {
    pub schema_name: Option<String>,
    pub operator_name: String,
    pub left_arg: Option<RawTypeName>,
    pub right_arg: Option<RawTypeName>,
    pub procedure: Option<QualifiedNameRef>,
    pub commutator: Option<String>,
    pub negator: Option<String>,
    pub restrict: Option<QualifiedNameRef>,
    pub join: Option<QualifiedNameRef>,
    pub hashes: bool,
    pub merges: bool,
    pub unrecognized_attributes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOperatorClassStatement {
    pub schema_name: Option<String>,
    pub opclass_name: String,
    pub data_type: RawTypeName,
    pub access_method: String,
    pub is_default: bool,
    pub items: Vec<CreateOperatorClassItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOperatorClassItem {
    Operator {
        strategy_number: i16,
        operator_name: String,
    },
    Function {
        support_number: i16,
        schema_name: Option<String>,
        function_name: String,
        arg_types: Vec<RawTypeName>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumnDef {
    pub name: String,
    pub expr_sql: Option<String>,
    pub expr_type: Option<SqlType>,
    pub collation: Option<String>,
    pub opclass: Option<String>,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

impl From<&str> for IndexColumnDef {
    fn from(value: &str) -> Self {
        Self {
            name: value.to_string(),
            expr_sql: None,
            expr_type: None,
            collation: None,
            opclass: None,
            descending: false,
            nulls_first: None,
        }
    }
}

impl From<String> for IndexColumnDef {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableSetStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub options: Vec<RelOption>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterTableRowSecurityAction {
    Enable,
    Disable,
    Force,
    NoForce,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableSetRowSecurityStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub action: AlterTableRowSecurityAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterSequenceStatement {
    pub sequence_name: String,
    pub options: SequenceOptionsPatchSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAddColumnStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column: ColumnDef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAddConstraintStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub constraint: TableConstraint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableDropColumnStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableDropConstraintStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub constraint_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterConstraintStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub constraint_name: String,
    pub deferrable: Option<bool>,
    pub initially_deferred: Option<bool>,
    pub enforced: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRenameConstraintStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub constraint_name: String,
    pub new_constraint_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnTypeStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub ty: RawTypeName,
    pub using_expr: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnDefaultStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub default_expr: Option<SqlExpr>,
    pub default_expr_sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterColumnExpressionAction {
    Set { expr: SqlExpr, expr_sql: String },
    Drop { missing_ok: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnExpressionStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub action: AlterColumnExpressionAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnCompressionStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub compression: crate::include::access::htup::AttributeCompression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnStorageStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub storage: crate::include::access::htup::AttributeStorage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterColumnOptionsAction {
    Set(Vec<RelOption>),
    Reset(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnOptionsStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub action: AlterColumnOptionsAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnStatisticsStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub statistics_target: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterIndexAlterColumnStatisticsStatement {
    pub if_exists: bool,
    pub index_name: String,
    pub column_number: i16,
    pub statistics_target: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRelationOwnerStatement {
    pub if_exists: bool,
    pub only: bool,
    pub relation_name: String,
    pub new_owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterSchemaOwnerStatement {
    pub schema_name: String,
    pub new_owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRenameColumnStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
    pub new_column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRenameStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub new_table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRelationSetSchemaStatement {
    pub if_exists: bool,
    pub relation_name: String,
    pub schema_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterPolicyAction {
    Rename {
        new_name: String,
    },
    Update {
        role_names: Option<Vec<String>>,
        using_expr: Option<SqlExpr>,
        using_sql: Option<String>,
        with_check_expr: Option<SqlExpr>,
        with_check_sql: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPolicyStatement {
    pub policy_name: String,
    pub table_name: String,
    pub action: AlterPolicyAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableSetNotNullStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableDropNotNullStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableValidateConstraintStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub constraint_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableNoInheritStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub parent_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableInheritStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub parent_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableOfStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableNotOfStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAttachPartitionStatement {
    pub if_exists: bool,
    pub only: bool,
    pub parent_table: String,
    pub partition_table: String,
    pub bound: RawPartitionBoundSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachPartitionMode {
    Immediate,
    Concurrently,
    Finalize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableDetachPartitionStatement {
    pub if_exists: bool,
    pub only: bool,
    pub parent_table: String,
    pub partition_table: String,
    pub mode: DetachPartitionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableTriggerTarget {
    Named(String),
    All,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterTableTriggerMode {
    Disable,
    EnableOrigin,
    EnableReplica,
    EnableAlways,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableTriggerStateStatement {
    pub if_exists: bool,
    pub only: bool,
    pub table_name: String,
    pub target: AlterTableTriggerTarget,
    pub mode: AlterTableTriggerMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTriggerRenameStatement {
    pub trigger_name: String,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub new_trigger_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnTableStatement {
    pub table_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnViewStatement {
    pub view_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnIndexStatement {
    pub index_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnTypeStatement {
    pub type_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnColumnStatement {
    pub relation_name: String,
    pub column_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnConstraintStatement {
    pub constraint_name: String,
    pub table_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnRuleStatement {
    pub rule_name: String,
    pub relation_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnTriggerStatement {
    pub trigger_name: String,
    pub table_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnDomainStatement {
    pub domain_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnConversionStatement {
    pub conversion_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationPublishActions {
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishGeneratedColumns {
    None,
    Stored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationOption {
    Publish(PublicationPublishActions),
    PublishViaPartitionRoot(bool),
    PublishGeneratedColumns(PublishGeneratedColumns),
    Raw { name: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublicationOptions {
    pub options: Vec<PublicationOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationSchemaName {
    Name(String),
    CurrentSchema,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationTableSpec {
    pub relation_name: String,
    pub only: bool,
    pub column_names: Vec<String>,
    pub where_clause: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationSchemaSpec {
    pub schema_name: PublicationSchemaName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationObjectSpec {
    Table(PublicationTableSpec),
    Schema(PublicationSchemaSpec),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublicationTargetSpec {
    pub for_all_tables: bool,
    pub objects: Vec<PublicationObjectSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePublicationStatement {
    pub publication_name: String,
    pub target: PublicationTargetSpec,
    pub options: PublicationOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPublicationStatement {
    pub publication_name: String,
    pub action: AlterPublicationAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterPublicationAction {
    SetOptions(PublicationOptions),
    AddObjects(PublicationTargetSpec),
    DropObjects(PublicationTargetSpec),
    SetObjects(PublicationTargetSpec),
    Rename { new_name: String },
    OwnerTo { new_owner: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterOperatorStatement {
    pub schema_name: Option<String>,
    pub operator_name: String,
    pub left_arg: Option<RawTypeName>,
    pub right_arg: Option<RawTypeName>,
    pub options: Vec<AlterOperatorOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterOperatorOption {
    Restrict {
        option_name: String,
        function: Option<QualifiedNameRef>,
    },
    Join {
        option_name: String,
        function: Option<QualifiedNameRef>,
    },
    Commutator {
        option_name: String,
        operator_name: String,
    },
    Negator {
        option_name: String,
        operator_name: String,
    },
    Merges {
        option_name: String,
        enabled: bool,
    },
    Hashes {
        option_name: String,
        enabled: bool,
    },
    Unrecognized {
        option_name: String,
        raw_tokens: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropPublicationStatement {
    pub if_exists: bool,
    pub publication_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnPublicationStatement {
    pub publication_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnForeignDataWrapperStatement {
    pub fdw_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateConversionStatement {
    pub conversion_name: String,
    pub for_encoding: String,
    pub to_encoding: String,
    pub function_name: String,
    pub is_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateForeignDataWrapperStatement {
    pub fdw_name: String,
    pub handler_name: Option<String>,
    pub validator_name: Option<String>,
    pub options: Vec<RelOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateForeignServerStatement {
    pub server_name: String,
    pub fdw_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateForeignTableStatement {
    pub create_table: CreateTableStatement,
    pub server_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRoleStatement {
    pub role_name: String,
    pub is_user: bool,
    pub options: Vec<RoleOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRoleStatement {
    pub role_name: String,
    pub action: AlterRoleAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterRoleAction {
    Rename { new_name: String },
    Options(Vec<RoleOption>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRoleStatement {
    pub if_exists: bool,
    pub role_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropConversionStatement {
    pub if_exists: bool,
    pub conversion_name: String,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDatabaseStatement {
    pub if_exists: bool,
    pub database_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropFunctionStatement {
    pub if_exists: bool,
    pub schema_name: Option<String>,
    pub function_name: String,
    pub arg_types: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRoutineItem {
    pub schema_name: Option<String>,
    pub routine_name: String,
    pub arg_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropProcedureStatement {
    pub if_exists: bool,
    pub procedures: Vec<DropRoutineItem>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropOperatorStatement {
    pub if_exists: bool,
    pub schema_name: Option<String>,
    pub operator_name: String,
    pub left_arg: Option<RawTypeName>,
    pub right_arg: Option<RawTypeName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropAggregateStatement {
    pub if_exists: bool,
    pub schema_name: Option<String>,
    pub aggregate_name: String,
    pub signature: AggregateSignatureKind,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetSessionAuthorizationStatement {
    pub role_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetSessionAuthorizationStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetRoleStatement {
    pub role_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetRoleStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnRoleStatement {
    pub role_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnAggregateStatement {
    pub schema_name: Option<String>,
    pub aggregate_name: String,
    pub signature: AggregateSignatureKind,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnFunctionStatement {
    pub schema_name: Option<String>,
    pub function_name: String,
    pub arg_types: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnOperatorStatement {
    pub schema_name: Option<String>,
    pub operator_name: String,
    pub left_arg: Option<RawTypeName>,
    pub right_arg: Option<RawTypeName>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantObjectPrivilege {
    CreateOnDatabase,
    AllPrivilegesOnTable,
    SelectOnTable,
    InsertOnTable,
    AllPrivilegesOnSchema,
    UsageOnSchema,
    UsageOnType,
    ExecuteOnFunction,
    ExecuteOnProcedure,
    ExecuteOnRoutine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantObjectStatement {
    pub privilege: GrantObjectPrivilege,
    pub object_names: Vec<String>,
    pub grantee_names: Vec<String>,
    pub with_grant_option: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeObjectStatement {
    pub privilege: GrantObjectPrivilege,
    pub object_names: Vec<String>,
    pub grantee_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTypeOwnerStatement {
    pub type_name: String,
    pub new_owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRoleMembershipStatement {
    pub role_names: Vec<String>,
    pub grantee_names: Vec<String>,
    pub admin_option: bool,
    pub inherit_option: Option<bool>,
    pub set_option: Option<bool>,
    pub granted_by: Option<RoleGrantorSpec>,
    pub legacy_group_syntax: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeRoleMembershipStatement {
    pub role_names: Vec<String>,
    pub grantee_names: Vec<String>,
    pub revoke_membership: bool,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
    pub cascade: bool,
    pub granted_by: Option<RoleGrantorSpec>,
    pub legacy_group_syntax: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleGrantorSpec {
    RoleName(String),
    CurrentUser,
    CurrentRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignOwnedStatement {
    pub old_roles: Vec<String>,
    pub new_role: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropOwnedStatement {
    pub role_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleOption {
    Superuser(bool),
    CreateDb(bool),
    CreateRole(bool),
    Inherit(bool),
    Login(bool),
    Replication(bool),
    BypassRls(bool),
    ConnectionLimit(i32),
    Password(Option<String>),
    EncryptedPassword(String),
    InRole(Vec<String>),
    Role(Vec<String>),
    Admin(Vec<String>),
    Sysid(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelOption {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterGenericOptionAction {
    Add,
    Set,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterGenericOption {
    pub action: AlterGenericOptionAction,
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub if_exists: bool,
    pub table_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTriggerStatement {
    pub if_exists: bool,
    pub trigger_name: String,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStatement {
    pub concurrently: bool,
    pub if_exists: bool,
    pub index_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReindexIndexStatement {
    pub concurrently: bool,
    pub index_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSequenceStatement {
    pub if_exists: bool,
    pub sequence_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDomainStatement {
    pub if_exists: bool,
    pub domain_name: String,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropForeignDataWrapperStatement {
    pub if_exists: bool,
    pub fdw_name: String,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTypeStatement {
    pub if_exists: bool,
    pub type_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropViewStatement {
    pub if_exists: bool,
    pub view_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMaterializedViewStatement {
    pub if_exists: bool,
    pub view_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRuleStatement {
    pub if_exists: bool,
    pub rule_name: String,
    pub relation_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropPolicyStatement {
    pub if_exists: bool,
    pub policy_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSchemaStatement {
    pub if_exists: bool,
    pub schema_names: Vec<String>,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterForeignDataWrapperStatement {
    pub fdw_name: String,
    pub handler_name: Option<Option<String>>,
    pub validator_name: Option<Option<String>>,
    pub options: Vec<AlterGenericOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterForeignDataWrapperOwnerStatement {
    pub fdw_name: String,
    pub new_owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterForeignDataWrapperRenameStatement {
    pub fdw_name: String,
    pub new_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateTableStatement {
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumStatement {
    pub targets: Vec<MaintenanceTarget>,
    pub analyze: bool,
    pub full: bool,
    pub verbose: bool,
    pub skip_locked: bool,
    pub buffer_usage_limit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: RawTypeName,
    pub collation: Option<String>,
    pub default_expr: Option<String>,
    pub generated: Option<ColumnGeneratedDef>,
    pub identity: Option<ColumnIdentityKind>,
    pub storage: Option<crate::include::access::htup::AttributeStorage>,
    pub compression: Option<crate::include::access::htup::AttributeCompression>,
    pub constraints: Vec<ColumnConstraint>,
}

impl ColumnDef {
    pub fn nullable(&self) -> bool {
        !self.constraints.iter().any(|constraint| {
            matches!(
                constraint,
                ColumnConstraint::NotNull { .. } | ColumnConstraint::PrimaryKey { .. }
            )
        })
    }

    pub fn primary_key(&self) -> bool {
        self.constraints
            .iter()
            .any(|constraint| matches!(constraint, ColumnConstraint::PrimaryKey { .. }))
    }

    pub fn unique(&self) -> bool {
        self.constraints
            .iter()
            .any(|constraint| matches!(constraint, ColumnConstraint::Unique { .. }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnGeneratedDef {
    pub expr_sql: String,
    pub kind: ColumnGeneratedKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnIdentityKind {
    Always,
    ByDefault,
}

impl ColumnIdentityKind {
    pub const fn catalog_char(self) -> char {
        match self {
            Self::Always => 'a',
            Self::ByDefault => 'd',
        }
    }

    pub const fn from_catalog_char(value: char) -> Option<Self> {
        match value {
            'a' => Some(Self::Always),
            'd' => Some(Self::ByDefault),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnGeneratedKind {
    Virtual,
    Stored,
}

impl ColumnGeneratedKind {
    pub const fn catalog_char(self) -> char {
        match self {
            Self::Virtual => 'v',
            Self::Stored => 's',
        }
    }

    pub const fn from_catalog_char(value: char) -> Option<Self> {
        match value {
            'v' => Some(Self::Virtual),
            's' => Some(Self::Stored),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDomainStatement {
    pub domain_name: String,
    pub ty: RawTypeName,
    pub default: Option<String>,
    pub check: Option<String>,
    pub not_null: bool,
    pub enum_check: Option<DomainCheckConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainCheckConstraint {
    pub name: Option<String>,
    pub allowed_values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionColumnOverride {
    pub name: String,
    pub default_expr: Option<String>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedColumnOptions {
    pub name: String,
    pub collation: Option<String>,
    pub default_expr: Option<String>,
    pub generated: Option<ColumnGeneratedDef>,
    pub identity: Option<ColumnIdentityKind>,
    pub storage: Option<crate::include::access::htup::AttributeStorage>,
    pub compression: Option<crate::include::access::htup::AttributeCompression>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTableElement {
    Column(ColumnDef),
    TypedColumnOptions(TypedColumnOptions),
    PartitionColumnOverride(PartitionColumnOverride),
    Constraint(TableConstraint),
    Like(CreateTableLikeClause),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableLikeClause {
    pub relation_name: String,
    pub options: Vec<CreateTableLikeOption>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTableLikeOption {
    IncludingDefaults,
    IncludingConstraints,
    IncludingIndexes,
    IncludingIdentity,
    IncludingGenerated,
    IncludingComments,
    IncludingStorage,
    IncludingCompression,
    IncludingStatistics,
    IncludingAll,
    ExcludingDefaults,
    ExcludingConstraints,
    ExcludingIndexes,
    ExcludingIdentity,
    ExcludingGenerated,
    ExcludingComments,
    ExcludingStorage,
    ExcludingCompression,
    ExcludingStatistics,
    ExcludingAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintAttributes {
    pub name: Option<String>,
    pub not_valid: bool,
    pub no_inherit: bool,
    pub deferrable: Option<bool>,
    pub initially_deferred: Option<bool>,
    pub enforced: Option<bool>,
    pub nulls_not_distinct: bool,
}

impl Default for ConstraintAttributes {
    fn default() -> Self {
        Self {
            name: None,
            not_valid: false,
            no_inherit: false,
            deferrable: None,
            initially_deferred: None,
            enforced: None,
            nulls_not_distinct: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyMatchType {
    Simple,
    Full,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnConstraint {
    NotNull {
        attributes: ConstraintAttributes,
    },
    Check {
        attributes: ConstraintAttributes,
        expr_sql: String,
    },
    PrimaryKey {
        attributes: ConstraintAttributes,
    },
    Unique {
        attributes: ConstraintAttributes,
    },
    References {
        attributes: ConstraintAttributes,
        referenced_table: String,
        referenced_columns: Option<Vec<String>>,
        match_type: ForeignKeyMatchType,
        on_delete: ForeignKeyAction,
        on_delete_set_columns: Option<Vec<String>>,
        on_update: ForeignKeyAction,
    },
}

impl ColumnConstraint {
    pub fn attributes(&self) -> &ConstraintAttributes {
        match self {
            Self::NotNull { attributes }
            | Self::Check { attributes, .. }
            | Self::PrimaryKey { attributes }
            | Self::Unique { attributes }
            | Self::References { attributes, .. } => attributes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraint {
    NotNull {
        attributes: ConstraintAttributes,
        column: String,
    },
    Check {
        attributes: ConstraintAttributes,
        expr_sql: String,
    },
    PrimaryKey {
        attributes: ConstraintAttributes,
        columns: Vec<String>,
        include_columns: Vec<String>,
        without_overlaps: Option<String>,
    },
    Unique {
        attributes: ConstraintAttributes,
        columns: Vec<String>,
        include_columns: Vec<String>,
        without_overlaps: Option<String>,
    },
    PrimaryKeyUsingIndex {
        attributes: ConstraintAttributes,
        index_name: String,
    },
    UniqueUsingIndex {
        attributes: ConstraintAttributes,
        index_name: String,
    },
    Exclusion {
        attributes: ConstraintAttributes,
        using_method: String,
        elements: Vec<ExclusionElement>,
        include_columns: Vec<String>,
    },
    ForeignKey {
        attributes: ConstraintAttributes,
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Option<Vec<String>>,
        match_type: ForeignKeyMatchType,
        on_delete: ForeignKeyAction,
        on_delete_set_columns: Option<Vec<String>>,
        on_update: ForeignKeyAction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExclusionElement {
    pub column: String,
    pub operator: String,
}

impl TableConstraint {
    pub fn attributes(&self) -> &ConstraintAttributes {
        match self {
            Self::NotNull { attributes, .. }
            | Self::Check { attributes, .. }
            | Self::PrimaryKey { attributes, .. }
            | Self::Unique { attributes, .. }
            | Self::PrimaryKeyUsingIndex { attributes, .. }
            | Self::UniqueUsingIndex { attributes, .. }
            | Self::Exclusion { attributes, .. }
            | Self::ForeignKey { attributes, .. } => attributes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SqlTypeKind {
    AnyArray,
    AnyElement,
    AnyRange,
    AnyMultirange,
    AnyCompatible,
    AnyCompatibleArray,
    AnyCompatibleRange,
    AnyCompatibleMultirange,
    AnyEnum,
    Record,
    Composite,
    Enum,
    Shell,
    Void,
    Trigger,
    FdwHandler,
    Int2,
    Int2Vector,
    Int4,
    Int8,
    Name,
    Oid,
    RegProc,
    RegClass,
    RegType,
    RegRole,
    RegNamespace,
    RegOper,
    RegOperator,
    RegProcedure,
    RegCollation,
    Tid,
    Xid,
    OidVector,
    Bit,
    VarBit,
    Bytea,
    Uuid,
    Inet,
    Cidr,
    MacAddr,
    MacAddr8,
    Float4,
    Float8,
    Money,
    Numeric,
    Range,
    Int4Range,
    Int8Range,
    NumericRange,
    DateRange,
    TimestampRange,
    TimestampTzRange,
    Multirange,
    Json,
    Jsonb,
    JsonPath,
    Xml,
    Date,
    Time,
    TimeTz,
    Interval,
    TsVector,
    TsQuery,
    RegConfig,
    RegDictionary,
    PgLsn,
    Text,
    Bool,
    Point,
    Lseg,
    Path,
    Box,
    Polygon,
    Line,
    Circle,
    Timestamp,
    TimestampTz,
    PgNodeTree,
    Internal,
    Cstring,
    InternalChar,
    Char,
    Varchar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialKind {
    Small,
    Regular,
    Big,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeometryUnaryOp {
    Center,
    Length,
    Npoints,
    IsVertical,
    IsHorizontal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeometryBinaryOp {
    Same,
    Distance,
    ClosestPoint,
    Intersects,
    Parallel,
    Perpendicular,
    IsVertical,
    IsHorizontal,
    OverLeft,
    OverRight,
    Below,
    Above,
    OverBelow,
    OverAbove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SqlType {
    pub kind: SqlTypeKind,
    pub typmod: i32,
    pub is_array: bool,
    pub type_oid: u32,
    pub typrelid: u32,
    pub range_subtype_oid: u32,
    pub range_multitype_oid: u32,
    pub range_discrete: bool,
    pub multirange_range_oid: u32,
}

impl SqlType {
    pub const NO_TYPEMOD: i32 = -1;
    pub const VARHDRSZ: i32 = 4;
    pub const INTERVAL_FULL_RANGE: i32 = 0x7fff;
    pub const INTERVAL_FULL_PRECISION: i32 = 0xffff;
    pub const INTERVAL_RANGE_MASK: i32 = 0x7fff;
    pub const INTERVAL_PRECISION_MASK: i32 = 0xffff;
    pub const INTERVAL_MASK_MONTH: i32 = 1 << 1;
    pub const INTERVAL_MASK_YEAR: i32 = 1 << 2;
    pub const INTERVAL_MASK_DAY: i32 = 1 << 3;
    pub const INTERVAL_MASK_HOUR: i32 = 1 << 10;
    pub const INTERVAL_MASK_MINUTE: i32 = 1 << 11;
    pub const INTERVAL_MASK_SECOND: i32 = 1 << 12;

    pub const fn new(kind: SqlTypeKind) -> Self {
        Self {
            kind,
            typmod: Self::NO_TYPEMOD,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
            range_subtype_oid: 0,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: 0,
        }
    }

    pub const fn with_char_len(kind: SqlTypeKind, len: i32) -> Self {
        Self {
            kind,
            typmod: Self::VARHDRSZ + len,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
            range_subtype_oid: 0,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: 0,
        }
    }

    pub const fn with_bit_len(kind: SqlTypeKind, len: i32) -> Self {
        Self {
            kind,
            typmod: Self::VARHDRSZ + len,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
            range_subtype_oid: 0,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: 0,
        }
    }

    pub const fn with_numeric_precision_scale(precision: i32, scale: i32) -> Self {
        Self {
            kind: SqlTypeKind::Numeric,
            typmod: Self::VARHDRSZ + ((precision << 16) | (scale & 0xffff)),
            is_array: false,
            type_oid: 0,
            typrelid: 0,
            range_subtype_oid: 0,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: 0,
        }
    }

    pub const fn with_time_precision(kind: SqlTypeKind, precision: i32) -> Self {
        Self {
            kind,
            typmod: precision,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
            range_subtype_oid: 0,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: 0,
        }
    }

    pub const fn with_interval_typmod(precision: Option<i32>, range: Option<i32>) -> Self {
        let typmod = match range {
            Some(range) => {
                let precision = match precision {
                    Some(precision) => precision,
                    None => Self::INTERVAL_FULL_PRECISION,
                };
                ((range & Self::INTERVAL_RANGE_MASK) << 16)
                    | (precision & Self::INTERVAL_PRECISION_MASK)
            }
            None => match precision {
                Some(precision) => precision,
                None => Self::NO_TYPEMOD,
            },
        };
        Self {
            kind: SqlTypeKind::Interval,
            typmod,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
            range_subtype_oid: 0,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: 0,
        }
    }

    pub const fn range(type_oid: u32, subtype_oid: u32) -> Self {
        Self {
            kind: SqlTypeKind::Range,
            typmod: Self::NO_TYPEMOD,
            is_array: false,
            type_oid,
            typrelid: 0,
            range_subtype_oid: subtype_oid,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: 0,
        }
    }

    pub const fn multirange(type_oid: u32, range_oid: u32) -> Self {
        Self {
            kind: SqlTypeKind::Multirange,
            typmod: Self::NO_TYPEMOD,
            is_array: false,
            type_oid,
            typrelid: 0,
            range_subtype_oid: 0,
            range_multitype_oid: 0,
            range_discrete: false,
            multirange_range_oid: range_oid,
        }
    }

    pub const fn with_identity(mut self, type_oid: u32, typrelid: u32) -> Self {
        self.type_oid = type_oid;
        self.typrelid = typrelid;
        self
    }

    pub const fn with_range_metadata(
        mut self,
        subtype_oid: u32,
        multitype_oid: u32,
        discrete: bool,
    ) -> Self {
        self.range_subtype_oid = subtype_oid;
        self.range_multitype_oid = multitype_oid;
        self.range_discrete = discrete;
        self
    }

    pub const fn with_multirange_range_oid(mut self, range_oid: u32) -> Self {
        self.multirange_range_oid = range_oid;
        self
    }

    pub const fn with_typmod(mut self, typmod: i32) -> Self {
        self.typmod = typmod;
        self
    }

    pub const fn record(type_oid: u32) -> Self {
        Self::new(SqlTypeKind::Record).with_identity(type_oid, 0)
    }

    pub const fn named_composite(type_oid: u32, typrelid: u32) -> Self {
        Self::new(SqlTypeKind::Composite).with_identity(type_oid, typrelid)
    }

    pub const fn array_of(mut elem: SqlType) -> Self {
        elem.is_array = true;
        elem
    }

    pub const fn element_type(self) -> Self {
        Self {
            kind: self.kind,
            typmod: self.typmod,
            is_array: false,
            type_oid: self.type_oid,
            typrelid: self.typrelid,
            range_subtype_oid: self.range_subtype_oid,
            range_multitype_oid: self.range_multitype_oid,
            range_discrete: self.range_discrete,
            multirange_range_oid: self.multirange_range_oid,
        }
    }

    pub const fn is_range(self) -> bool {
        !self.is_array
            && matches!(
                self.kind,
                SqlTypeKind::Range
                    | SqlTypeKind::Int4Range
                    | SqlTypeKind::Int8Range
                    | SqlTypeKind::NumericRange
                    | SqlTypeKind::DateRange
                    | SqlTypeKind::TimestampRange
                    | SqlTypeKind::TimestampTzRange
            )
    }

    pub const fn is_multirange(self) -> bool {
        !self.is_array && matches!(self.kind, SqlTypeKind::Multirange)
    }

    pub const fn char_len(self) -> Option<i32> {
        if self.typmod >= Self::VARHDRSZ {
            Some(self.typmod - Self::VARHDRSZ)
        } else {
            None
        }
    }

    pub const fn bit_len(self) -> Option<i32> {
        if self.typmod >= Self::VARHDRSZ {
            Some(self.typmod - Self::VARHDRSZ)
        } else {
            None
        }
    }

    pub fn numeric_precision_scale(self) -> Option<(i32, i32)> {
        if self.kind != SqlTypeKind::Numeric || self.typmod < Self::VARHDRSZ {
            None
        } else {
            let packed = self.typmod - Self::VARHDRSZ;
            let precision = (packed >> 16) & 0xffff;
            let scale = ((packed & 0xffff) as i16) as i32;
            Some((precision, scale))
        }
    }

    pub const fn time_precision(self) -> Option<i32> {
        match self.kind {
            SqlTypeKind::Time
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz
                if self.typmod >= 0 =>
            {
                Some(self.typmod)
            }
            _ => None,
        }
    }

    pub const fn interval_precision(self) -> Option<i32> {
        if !matches!(self.kind, SqlTypeKind::Interval) || self.typmod < 0 {
            return None;
        }
        if self.typmod <= crate::include::nodes::datetime::MAX_TIME_PRECISION {
            return Some(self.typmod);
        }
        let precision = self.typmod & Self::INTERVAL_PRECISION_MASK;
        if precision == Self::INTERVAL_FULL_PRECISION {
            None
        } else {
            Some(precision)
        }
    }

    pub const fn interval_range(self) -> Option<i32> {
        if !matches!(self.kind, SqlTypeKind::Interval)
            || self.typmod <= crate::include::nodes::datetime::MAX_TIME_PRECISION
        {
            return None;
        }
        let range = (self.typmod >> 16) & Self::INTERVAL_RANGE_MASK;
        if range == 0 || range == Self::INTERVAL_FULL_RANGE {
            None
        } else {
            Some(range)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawTypeName {
    Builtin(SqlType),
    Serial(SerialKind),
    Named { name: String, array_bounds: usize },
    Record,
}

impl RawTypeName {
    pub fn builtin(kind: SqlTypeKind) -> Self {
        Self::Builtin(SqlType::new(kind))
    }

    pub fn as_builtin(&self) -> Option<SqlType> {
        match self {
            Self::Builtin(ty) => Some(*ty),
            Self::Serial(_) | Self::Named { .. } | Self::Record => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceOptionsSpec {
    pub increment: Option<i64>,
    pub minvalue: Option<Option<i64>>,
    pub maxvalue: Option<Option<i64>>,
    pub start: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    pub owned_by: Option<SequenceOwnedByClause>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceOptionsPatchSpec {
    pub increment: Option<i64>,
    pub minvalue: Option<Option<i64>>,
    pub maxvalue: Option<Option<i64>>,
    pub start: Option<i64>,
    pub restart: Option<Option<i64>>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    pub owned_by: Option<SequenceOwnedByClause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceOwnedByClause {
    None,
    Column {
        table_name: String,
        column_name: String,
    },
}

impl PartialEq<SqlType> for RawTypeName {
    fn eq(&self, other: &SqlType) -> bool {
        self.as_builtin().is_some_and(|ty| ty == *other)
    }
}

impl PartialEq<RawTypeName> for SqlType {
    fn eq(&self, other: &RawTypeName) -> bool {
        other == self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub with_recursive: bool,
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub target_alias: Option<String>,
    pub only: bool,
    pub assignments: Vec<Assignment>,
    pub from: Option<FromItem>,
    pub where_clause: Option<SqlExpr>,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub with_recursive: bool,
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub only: bool,
    pub where_clause: Option<SqlExpr>,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub target: AssignmentTarget,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentTarget {
    pub column: String,
    pub subscripts: Vec<ArraySubscript>,
    pub field_path: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArraySubscript {
    pub is_slice: bool,
    pub lower: Option<Box<SqlExpr>>,
    pub upper: Option<Box<SqlExpr>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Match,
    Like,
    NotLike,
    ILike,
    NotILike,
    Similar,
    NotSimilar,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpr {
    Column(String),
    Default,
    Const(Value),
    IntegerLiteral(String),
    NumericLiteral(String),
    Add(Box<SqlExpr>, Box<SqlExpr>),
    Sub(Box<SqlExpr>, Box<SqlExpr>),
    BitAnd(Box<SqlExpr>, Box<SqlExpr>),
    BitOr(Box<SqlExpr>, Box<SqlExpr>),
    BitXor(Box<SqlExpr>, Box<SqlExpr>),
    Shl(Box<SqlExpr>, Box<SqlExpr>),
    Shr(Box<SqlExpr>, Box<SqlExpr>),
    Mul(Box<SqlExpr>, Box<SqlExpr>),
    Div(Box<SqlExpr>, Box<SqlExpr>),
    Mod(Box<SqlExpr>, Box<SqlExpr>),
    Concat(Box<SqlExpr>, Box<SqlExpr>),
    BinaryOperator {
        op: String,
        left: Box<SqlExpr>,
        right: Box<SqlExpr>,
    },
    UnaryPlus(Box<SqlExpr>),
    Negate(Box<SqlExpr>),
    BitNot(Box<SqlExpr>),
    Subscript {
        expr: Box<SqlExpr>,
        index: i32,
    },
    GeometryUnaryOp {
        op: GeometryUnaryOp,
        expr: Box<SqlExpr>,
    },
    GeometryBinaryOp {
        op: GeometryBinaryOp,
        left: Box<SqlExpr>,
        right: Box<SqlExpr>,
    },
    PrefixOperator {
        op: String,
        expr: Box<SqlExpr>,
    },
    Cast(Box<SqlExpr>, RawTypeName),
    Collate {
        expr: Box<SqlExpr>,
        collation: String,
    },
    AtTimeZone {
        expr: Box<SqlExpr>,
        zone: Box<SqlExpr>,
    },
    Eq(Box<SqlExpr>, Box<SqlExpr>),
    NotEq(Box<SqlExpr>, Box<SqlExpr>),
    Lt(Box<SqlExpr>, Box<SqlExpr>),
    LtEq(Box<SqlExpr>, Box<SqlExpr>),
    Gt(Box<SqlExpr>, Box<SqlExpr>),
    GtEq(Box<SqlExpr>, Box<SqlExpr>),
    RegexMatch(Box<SqlExpr>, Box<SqlExpr>),
    Like {
        expr: Box<SqlExpr>,
        pattern: Box<SqlExpr>,
        escape: Option<Box<SqlExpr>>,
        case_insensitive: bool,
        negated: bool,
    },
    Similar {
        expr: Box<SqlExpr>,
        pattern: Box<SqlExpr>,
        escape: Option<Box<SqlExpr>>,
        negated: bool,
    },
    Case {
        arg: Option<Box<SqlExpr>>,
        args: Vec<SqlCaseWhen>,
        defresult: Option<Box<SqlExpr>>,
    },
    And(Box<SqlExpr>, Box<SqlExpr>),
    Or(Box<SqlExpr>, Box<SqlExpr>),
    Not(Box<SqlExpr>),
    IsNull(Box<SqlExpr>),
    IsNotNull(Box<SqlExpr>),
    IsDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    IsNotDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    Overlaps(Box<SqlExpr>, Box<SqlExpr>),
    ArrayLiteral(Vec<SqlExpr>),
    Row(Vec<SqlExpr>),
    ArrayOverlap(Box<SqlExpr>, Box<SqlExpr>),
    ArrayContains(Box<SqlExpr>, Box<SqlExpr>),
    ArrayContained(Box<SqlExpr>, Box<SqlExpr>),
    JsonbContains(Box<SqlExpr>, Box<SqlExpr>),
    JsonbContained(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExists(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExistsAny(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExistsAll(Box<SqlExpr>, Box<SqlExpr>),
    JsonbPathExists(Box<SqlExpr>, Box<SqlExpr>),
    JsonbPathMatch(Box<SqlExpr>, Box<SqlExpr>),
    ScalarSubquery(Box<SelectStatement>),
    ArraySubquery(Box<SelectStatement>),
    Exists(Box<SelectStatement>),
    InSubquery {
        expr: Box<SqlExpr>,
        subquery: Box<SelectStatement>,
        negated: bool,
    },
    QuantifiedSubquery {
        left: Box<SqlExpr>,
        op: SubqueryComparisonOp,
        is_all: bool,
        subquery: Box<SelectStatement>,
    },
    QuantifiedArray {
        left: Box<SqlExpr>,
        op: SubqueryComparisonOp,
        is_all: bool,
        array: Box<SqlExpr>,
    },
    ArraySubscript {
        array: Box<SqlExpr>,
        subscripts: Vec<ArraySubscript>,
    },
    Xml(Box<RawXmlExpr>),
    Random,
    JsonGet(Box<SqlExpr>, Box<SqlExpr>),
    JsonGetText(Box<SqlExpr>, Box<SqlExpr>),
    JsonPath(Box<SqlExpr>, Box<SqlExpr>),
    JsonPathText(Box<SqlExpr>, Box<SqlExpr>),
    FuncCall {
        name: String,
        args: SqlCallArgs,
        order_by: Vec<OrderByItem>,
        within_group: Option<Vec<OrderByItem>>,
        distinct: bool,
        func_variadic: bool,
        filter: Option<Box<SqlExpr>>,
        over: Option<RawWindowSpec>,
    },
    FieldSelect {
        expr: Box<SqlExpr>,
        field: String,
    },
    CurrentDate,
    CurrentCatalog,
    CurrentSchema,
    CurrentUser,
    SessionUser,
    CurrentRole,
    CurrentTime {
        precision: Option<i32>,
    },
    CurrentTimestamp {
        precision: Option<i32>,
    },
    LocalTime {
        precision: Option<i32>,
    },
    LocalTimestamp {
        precision: Option<i32>,
    },
}
