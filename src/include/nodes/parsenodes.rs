use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::AggFunc;
use crate::include::nodes::primnodes::{
    AggAccum, Expr, JoinType, ProjectSetTarget, QueryColumn, RelationDesc, SetReturningCall,
    SortGroupClause, TargetEntry, ToastRelationRef,
};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UngroupedColumnClause {
    SelectTarget,
    Having,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnexpectedEof,
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    InvalidInteger(String),
    InvalidNumeric(String),
    UnknownTable(String),
    UnknownColumn(String),
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
    AggInWhere,
    SubqueryMustReturnOneColumn,
    UnknownConfigurationParameter(String),
    UnrecognizedParameter(String),
    TablesDeclaredWithOidsNotSupported,
    ActiveSqlTransaction(&'static str),
    OnCommitOnlyForTempTables,
    TempTableInNonTempSchema(String),
    OnlyTemporaryRelationsInTemporarySchemas(String),
    NoSchemaSelectedForCreate,
    FeatureNotSupported(String),
    WrongObjectType {
        name: String,
        expected: &'static str,
    },
    RecursiveView(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEof => write!(f, "unexpected end of input"),
            ParseError::UnexpectedToken { actual, .. } => write!(f, "{actual}"),
            ParseError::InvalidInteger(value) => write!(f, "invalid integer: {value}"),
            ParseError::InvalidNumeric(value) => write!(f, "invalid numeric: {value}"),
            ParseError::UnknownTable(name) => write!(f, "unknown table: {name}"),
            ParseError::UnknownColumn(name) => {
                if name.contains('.') {
                    write!(f, "column {name} does not exist")
                } else {
                    write!(f, "column \"{name}\" does not exist")
                }
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
            ParseError::TableDoesNotExist(name) => write!(f, "table does not exist: {name}"),
            ParseError::UnsupportedType(name) => write!(f, "unsupported type: {name}"),
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
            ParseError::AggInWhere => {
                write!(f, "aggregate functions are not allowed in WHERE")
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
            ParseError::TablesDeclaredWithOidsNotSupported => {
                write!(f, "tables declared WITH OIDS are not supported")
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
            ParseError::NoSchemaSelectedForCreate => {
                write!(f, "no schema has been selected to create in")
            }
            ParseError::FeatureNotSupported(feature) => {
                write!(f, "feature not supported: {feature}")
            }
            ParseError::WrongObjectType { name, expected } => {
                write!(f, "\"{name}\" is not a {expected}")
            }
            ParseError::RecursiveView(name) => {
                write!(f, "infinite recursion detected in view \"{name}\"")
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Do(DoStatement),
    Explain(ExplainStatement),
    Show(ShowStatement),
    Select(SelectStatement),
    Values(ValuesStatement),
    CopyFrom(CopyFromStatement),
    Analyze(AnalyzeStatement),
    Set(SetStatement),
    Reset(ResetStatement),
    CreateFunction(CreateFunctionStatement),
    CreateTable(CreateTableStatement),
    CreateTableAs(CreateTableAsStatement),
    CreateView(CreateViewStatement),
    CreateIndex(CreateIndexStatement),
    AlterTableAddColumn(AlterTableAddColumnStatement),
    AlterTableDropColumn(AlterTableDropColumnStatement),
    AlterTableAlterColumnType(AlterTableAlterColumnTypeStatement),
    AlterTableRenameColumn(AlterTableRenameColumnStatement),
    AlterTableRename(AlterTableRenameStatement),
    AlterTableSet(AlterTableSetStatement),
    CommentOnTable(CommentOnTableStatement),
    CommentOnDomain(CommentOnDomainStatement),
    CreateDomain(CreateDomainStatement),
    DropTable(DropTableStatement),
    DropDomain(DropDomainStatement),
    DropView(DropViewStatement),
    TruncateTable(TruncateTableStatement),
    Vacuum(VacuumStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Unsupported(UnsupportedStatement),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedStatement {
    pub sql: String,
    pub feature: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub command_type: CommandType,
    pub rtable: Vec<RangeTblEntry>,
    pub jointree: Option<JoinTreeNode>,
    pub target_list: Vec<TargetEntry>,
    pub where_qual: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub accumulators: Vec<AggAccum>,
    pub having_qual: Option<Expr>,
    pub sort_clause: Vec<SortGroupClause>,
    pub limit_count: Option<usize>,
    pub limit_offset: usize,
    pub project_set: Option<Vec<ProjectSetTarget>>,
}

impl Query {
    pub fn columns(&self) -> Vec<QueryColumn> {
        self.target_list
            .iter()
            .map(|target| QueryColumn {
                name: target.name.clone(),
                sql_type: target.sql_type,
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
pub struct RangeTblEntry {
    pub alias: Option<String>,
    pub desc: RelationDesc,
    pub kind: RangeTblEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeTblEntryKind {
    Result,
    Relation {
        rel: crate::RelFileLocator,
        relation_oid: u32,
        relkind: char,
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
    Subquery {
        query: Box<Query>,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunctionArg {
    pub mode: FunctionArgMode,
    pub name: String,
    pub ty: RawTypeName,
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
    pub args: Vec<CreateFunctionArg>,
    pub return_spec: CreateFunctionReturnSpec,
    pub language: String,
    pub body: String,
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
    pub value: String,
    pub is_local: bool,
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
pub struct ExplainStatement {
    pub analyze: bool,
    pub buffers: bool,
    pub timing: bool,
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopySource {
    File(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFromStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub source: CopySource,
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
    pub with: Vec<CommonTableExpr>,
    pub from: Option<FromItem>,
    pub targets: Vec<SelectItem>,
    pub where_clause: Option<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub having: Option<SqlExpr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesStatement {
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
pub enum CteBody {
    Select(Box<SelectStatement>),
    Values(ValuesStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromItem {
    Table {
        name: String,
    },
    Values {
        rows: Vec<Vec<SqlExpr>>,
    },
    FunctionCall {
        name: String,
        args: Vec<SqlFunctionArg>,
        func_variadic: bool,
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

pub fn function_arg_values(args: &[SqlFunctionArg]) -> impl Iterator<Item = &SqlExpr> {
    args.iter().map(|arg| &arg.value)
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub columns: Option<Vec<AssignmentTarget>>,
    pub source: InsertSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertSource {
    Values(Vec<Vec<SqlExpr>>),
    DefaultValues,
    Select(Box<SelectStatement>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub elements: Vec<CreateTableElement>,
    pub if_not_exists: bool,
}

impl CreateTableStatement {
    pub fn columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.elements.iter().filter_map(|element| match element {
            CreateTableElement::Column(column) => Some(column),
            CreateTableElement::Constraint(_) => None,
        })
    }

    pub fn constraints(&self) -> impl Iterator<Item = &TableConstraint> {
        self.elements.iter().filter_map(|element| match element {
            CreateTableElement::Column(_) => None,
            CreateTableElement::Constraint(constraint) => Some(constraint),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAsStatement {
    pub schema_name: Option<String>,
    pub table_name: String,
    pub persistence: TablePersistence,
    pub on_commit: OnCommitAction,
    pub column_names: Vec<String>,
    pub query: SelectStatement,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateViewStatement {
    pub schema_name: Option<String>,
    pub view_name: String,
    pub query: SelectStatement,
    pub query_sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub unique: bool,
    pub index_name: String,
    pub table_name: String,
    pub using_method: Option<String>,
    pub columns: Vec<IndexColumnDef>,
    pub include_columns: Vec<String>,
    pub predicate: Option<SqlExpr>,
    pub options: Vec<RelOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexColumnDef {
    pub name: String,
    pub collation: Option<String>,
    pub opclass: Option<String>,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

impl From<&str> for IndexColumnDef {
    fn from(value: &str) -> Self {
        Self {
            name: value.to_string(),
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
    pub table_name: String,
    pub options: Vec<RelOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAddColumnStatement {
    pub table_name: String,
    pub column: ColumnDef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableDropColumnStatement {
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAlterColumnTypeStatement {
    pub table_name: String,
    pub column_name: String,
    pub ty: RawTypeName,
    pub using_expr: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRenameColumnStatement {
    pub table_name: String,
    pub column_name: String,
    pub new_column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRenameStatement {
    pub table_name: String,
    pub new_table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnTableStatement {
    pub table_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentOnDomainStatement {
    pub domain_name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelOption {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub if_exists: bool,
    pub table_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDomainStatement {
    pub if_exists: bool,
    pub domain_name: String,
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropViewStatement {
    pub if_exists: bool,
    pub view_names: Vec<String>,
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
    pub default_expr: Option<String>,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDomainStatement {
    pub domain_name: String,
    pub ty: RawTypeName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateTableElement {
    Column(ColumnDef),
    Constraint(TableConstraint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraint {
    PrimaryKey { columns: Vec<String> },
    Unique { columns: Vec<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTypeKind {
    AnyArray,
    Record,
    Composite,
    Int2,
    Int2Vector,
    Int4,
    Int8,
    Name,
    Oid,
    OidVector,
    Bit,
    VarBit,
    Bytea,
    Float4,
    Float8,
    Money,
    Numeric,
    Json,
    Jsonb,
    JsonPath,
    Date,
    Time,
    TimeTz,
    TsVector,
    TsQuery,
    RegConfig,
    RegDictionary,
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
    InternalChar,
    Char,
    Varchar,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlType {
    pub kind: SqlTypeKind,
    pub typmod: i32,
    pub is_array: bool,
    pub type_oid: u32,
    pub typrelid: u32,
}

impl SqlType {
    pub const NO_TYPEMOD: i32 = -1;
    pub const VARHDRSZ: i32 = 4;

    pub const fn new(kind: SqlTypeKind) -> Self {
        Self {
            kind,
            typmod: Self::NO_TYPEMOD,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
        }
    }

    pub const fn with_char_len(kind: SqlTypeKind, len: i32) -> Self {
        Self {
            kind,
            typmod: Self::VARHDRSZ + len,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
        }
    }

    pub const fn with_bit_len(kind: SqlTypeKind, len: i32) -> Self {
        Self {
            kind,
            typmod: Self::VARHDRSZ + len,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
        }
    }

    pub const fn with_numeric_precision_scale(precision: i32, scale: i32) -> Self {
        Self {
            kind: SqlTypeKind::Numeric,
            typmod: Self::VARHDRSZ + ((precision << 16) | (scale & 0xffff)),
            is_array: false,
            type_oid: 0,
            typrelid: 0,
        }
    }

    pub const fn with_time_precision(kind: SqlTypeKind, precision: i32) -> Self {
        Self {
            kind,
            typmod: precision,
            is_array: false,
            type_oid: 0,
            typrelid: 0,
        }
    }

    pub const fn with_identity(mut self, type_oid: u32, typrelid: u32) -> Self {
        self.type_oid = type_oid;
        self.typrelid = typrelid;
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
        }
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawTypeName {
    Builtin(SqlType),
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
            Self::Named { .. } | Self::Record => None,
        }
    }
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
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub with: Vec<CommonTableExpr>,
    pub table_name: String,
    pub where_clause: Option<SqlExpr>,
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
    And(Box<SqlExpr>, Box<SqlExpr>),
    Or(Box<SqlExpr>, Box<SqlExpr>),
    Not(Box<SqlExpr>),
    IsNull(Box<SqlExpr>),
    IsNotNull(Box<SqlExpr>),
    IsDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    IsNotDistinctFrom(Box<SqlExpr>, Box<SqlExpr>),
    ArrayLiteral(Vec<SqlExpr>),
    Row(Vec<SqlExpr>),
    ArrayOverlap(Box<SqlExpr>, Box<SqlExpr>),
    JsonbContains(Box<SqlExpr>, Box<SqlExpr>),
    JsonbContained(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExists(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExistsAny(Box<SqlExpr>, Box<SqlExpr>),
    JsonbExistsAll(Box<SqlExpr>, Box<SqlExpr>),
    JsonbPathExists(Box<SqlExpr>, Box<SqlExpr>),
    JsonbPathMatch(Box<SqlExpr>, Box<SqlExpr>),
    AggCall {
        func: AggFunc,
        args: Vec<SqlFunctionArg>,
        distinct: bool,
        func_variadic: bool,
    },
    ScalarSubquery(Box<SelectStatement>),
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
    Random,
    JsonGet(Box<SqlExpr>, Box<SqlExpr>),
    JsonGetText(Box<SqlExpr>, Box<SqlExpr>),
    JsonPath(Box<SqlExpr>, Box<SqlExpr>),
    JsonPathText(Box<SqlExpr>, Box<SqlExpr>),
    FuncCall {
        name: String,
        args: Vec<SqlFunctionArg>,
        func_variadic: bool,
    },
    FieldSelect {
        expr: Box<SqlExpr>,
        field: String,
    },
    CurrentDate,
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
