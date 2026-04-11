use crate::backend::parser::SqlType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub declarations: Vec<VarDecl>,
    pub statements: Vec<Stmt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarDecl {
    pub name: String,
    pub ty: SqlType,
    pub default_expr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaiseLevel {
    Notice,
    Warning,
    Exception,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    Block(Block),
    Assign {
        name: String,
        expr: String,
    },
    Null,
    If {
        branches: Vec<(String, Vec<Stmt>)>,
        else_branch: Vec<Stmt>,
    },
    ForInt {
        var_name: String,
        start_expr: String,
        end_expr: String,
        body: Vec<Stmt>,
    },
    Raise {
        level: RaiseLevel,
        message: String,
        params: Vec<String>,
    },
}
