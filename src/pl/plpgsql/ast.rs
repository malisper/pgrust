use crate::backend::parser::SqlType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub label: Option<String>,
    pub declarations: Vec<Decl>,
    pub statements: Vec<Stmt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarDecl {
    pub name: String,
    pub type_name: String,
    pub ty: SqlType,
    pub default_expr: Option<String>,
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
    Alias(AliasDecl),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaiseLevel {
    Notice,
    Warning,
    Exception,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnQueryKind {
    Select,
    Values,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
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
    Raise {
        level: RaiseLevel,
        message: String,
        params: Vec<String>,
    },
    Return {
        expr: Option<String>,
    },
    ReturnNext {
        expr: Option<String>,
    },
    ReturnQuery {
        sql: String,
        kind: ReturnQueryKind,
    },
    Perform {
        sql: String,
    },
    ExecSql {
        sql: String,
    },
}
