use crate::backend::parser::SqlType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub label: Option<String>,
    pub declarations: Vec<Decl>,
    pub statements: Vec<Stmt>,
    pub exception_handlers: Vec<ExceptionHandler>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarDecl {
    pub name: String,
    pub type_name: String,
    pub ty: SqlType,
    pub default_expr: Option<String>,
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorDecl {
    pub name: String,
    pub scrollable: bool,
    pub params: Vec<CursorParamDecl>,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorParamDecl {
    pub name: String,
    pub type_name: String,
    pub ty: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasDecl {
    pub name: String,
    pub target: AliasTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AliasTarget {
    Parameter(usize),
    New,
    Old,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decl {
    Var(VarDecl),
    Cursor(CursorDecl),
    Alias(AliasDecl),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaiseLevel {
    Info,
    Log,
    Notice,
    Warning,
    Exception,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaiseCondition {
    SqlState(String),
    ConditionName(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaiseUsingOption {
    pub name: String,
    pub expr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignTarget {
    Name(String),
    Field { relation: String, field: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForTarget {
    Single(AssignTarget),
    List(Vec<AssignTarget>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForQuerySource {
    Static(String),
    Execute {
        sql_expr: String,
        using_exprs: Vec<String>,
    },
    Cursor {
        name: String,
        args: Vec<CursorArg>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorArg {
    Positional(String),
    Named { name: String, expr: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCursorSource {
    Declared {
        args: Vec<CursorArg>,
    },
    Static(String),
    Dynamic {
        sql_expr: String,
        using_exprs: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    Next,
    Prior,
    First,
    Last,
    Forward(i64),
    Backward(i64),
    ForwardAll,
    BackwardAll,
    Absolute(i64),
    Relative(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExceptionHandler {
    pub conditions: Vec<ExceptionCondition>,
    pub statements: Vec<Stmt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExceptionCondition {
    Others,
    SqlState(String),
    ConditionName(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    WithLine {
        line: usize,
        stmt: Box<Stmt>,
    },
    Block(Block),
    Assign {
        target: AssignTarget,
        expr: String,
    },
    Null,
    If {
        branches: Vec<(String, Vec<Stmt>)>,
        else_branch: Vec<Stmt>,
    },
    While {
        condition: String,
        body: Vec<Stmt>,
    },
    Loop {
        body: Vec<Stmt>,
    },
    Exit {
        condition: Option<String>,
    },
    ForInt {
        var_name: String,
        start_expr: String,
        end_expr: String,
        body: Vec<Stmt>,
    },
    ForQuery {
        target: ForTarget,
        source: ForQuerySource,
        body: Vec<Stmt>,
    },
    ForEach {
        target: ForTarget,
        slice: usize,
        array_expr: String,
        body: Vec<Stmt>,
    },
    Raise {
        level: RaiseLevel,
        condition: Option<RaiseCondition>,
        message: Option<String>,
        params: Vec<String>,
        using_options: Vec<RaiseUsingOption>,
    },
    Assert {
        condition: String,
        message: Option<String>,
    },
    Continue,
    Return {
        expr: Option<String>,
    },
    ReturnNext {
        expr: Option<String>,
    },
    ReturnQuery {
        source: ForQuerySource,
    },
    Perform {
        sql: String,
        line: usize,
    },
    DynamicExecute {
        sql_expr: String,
        strict: bool,
        into_targets: Vec<AssignTarget>,
        using_exprs: Vec<String>,
        line: usize,
    },
    GetDiagnostics {
        stacked: bool,
        items: Vec<(AssignTarget, String)>,
    },
    OpenCursor {
        name: String,
        source: OpenCursorSource,
    },
    FetchCursor {
        name: String,
        direction: CursorDirection,
        targets: Vec<AssignTarget>,
    },
    MoveCursor {
        name: String,
        direction: CursorDirection,
    },
    CloseCursor {
        name: String,
    },
    ExecSql {
        sql: String,
    },
}
