pub use crate::catalog::{Catalog, CatalogEntry};
use crate::catalog::column_desc;
use crate::executor::{Expr, Plan, RelationDesc, TargetEntry, Value};
use crate::RelFileLocator;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnexpectedEof,
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    InvalidInteger(String),
    UnknownTable(String),
    UnknownColumn(String),
    EmptySelectList,
    UnsupportedQualifiedName(String),
    InvalidInsertTargetCount {
        expected: usize,
        actual: usize,
    },
    TableAlreadyExists(String),
    TableDoesNotExist(String),
    UnsupportedType(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub table_name: String,
    pub targets: Vec<SelectItem>,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectItem {
    pub output_name: String,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<SqlExpr>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Int4,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<SqlExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: String,
    pub expr: SqlExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpr {
    Column(String),
    Const(Value),
    Eq(Box<SqlExpr>, Box<SqlExpr>),
    Lt(Box<SqlExpr>, Box<SqlExpr>),
    Gt(Box<SqlExpr>, Box<SqlExpr>),
    And(Box<SqlExpr>, Box<SqlExpr>),
    Or(Box<SqlExpr>, Box<SqlExpr>),
    Not(Box<SqlExpr>),
    IsNull(Box<SqlExpr>),
}

pub fn parse_select(sql: &str) -> Result<SelectStatement, ParseError> {
    let tokens = tokenize(sql);
    let mut parser = Parser::new(tokens);
    parser.parse_select()
}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    let tokens = tokenize(sql);
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}

pub fn create_relation_desc(stmt: &CreateTableStatement) -> RelationDesc {
    RelationDesc {
        columns: stmt
            .columns
            .iter()
            .map(|column| {
                column_desc(
                    column.name.clone(),
                    match column.ty {
                        SqlType::Int4 => crate::executor::ScalarType::Int32,
                        SqlType::Text => crate::executor::ScalarType::Text,
                        SqlType::Bool => crate::executor::ScalarType::Bool,
                    },
                    column.nullable,
                )
            })
            .collect(),
    }
}

pub fn bind_create_table(
    stmt: &CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<CatalogEntry, ParseError> {
    catalog
        .create_table(stmt.table_name.clone(), create_relation_desc(stmt))
        .map_err(|err| match err {
            crate::catalog::CatalogError::TableAlreadyExists(name) => {
                ParseError::TableAlreadyExists(name)
            }
            crate::catalog::CatalogError::UnknownTable(name) => ParseError::TableDoesNotExist(name),
            crate::catalog::CatalogError::UnknownType(name) => ParseError::UnsupportedType(name),
            crate::catalog::CatalogError::Io(_)
            | crate::catalog::CatalogError::Corrupt(_) => ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            },
        })
}

pub fn build_plan(stmt: &SelectStatement, catalog: &Catalog) -> Result<Plan, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;

    let base = Plan::SeqScan {
        rel: entry.rel,
        desc: entry.desc.clone(),
    };

    let plan = if let Some(predicate) = &stmt.where_clause {
        Plan::Filter {
            input: Box::new(base),
            predicate: bind_expr(predicate, &entry.desc)?,
        }
    } else {
        base
    };

    Ok(Plan::Projection {
        input: Box::new(plan),
        targets: bind_select_targets(&stmt.targets, &entry.desc)?,
    })
}

fn bind_select_targets(
    targets: &[SelectItem],
    desc: &RelationDesc,
) -> Result<Vec<TargetEntry>, ParseError> {
    if targets.len() == 1 && matches!(targets[0].expr, SqlExpr::Column(ref name) if name == "*") {
        return Ok(desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| TargetEntry {
                name: column.name.clone(),
                expr: Expr::Column(index),
            })
            .collect());
    }

    targets
        .iter()
        .map(|item| {
            Ok(TargetEntry {
                name: item.output_name.clone(),
                expr: bind_expr(&item.expr, desc)?,
            })
        })
        .collect()
}

fn bind_expr(expr: &SqlExpr, desc: &RelationDesc) -> Result<Expr, ParseError> {
    Ok(match expr {
        SqlExpr::Column(name) => Expr::Column(resolve_column(desc, name)?),
        SqlExpr::Const(value) => Expr::Const(value.clone()),
        SqlExpr::Eq(left, right) => Expr::Eq(
            Box::new(bind_expr(left, desc)?),
            Box::new(bind_expr(right, desc)?),
        ),
        SqlExpr::Lt(left, right) => Expr::Lt(
            Box::new(bind_expr(left, desc)?),
            Box::new(bind_expr(right, desc)?),
        ),
        SqlExpr::Gt(left, right) => Expr::Gt(
            Box::new(bind_expr(left, desc)?),
            Box::new(bind_expr(right, desc)?),
        ),
        SqlExpr::And(left, right) => Expr::And(
            Box::new(bind_expr(left, desc)?),
            Box::new(bind_expr(right, desc)?),
        ),
        SqlExpr::Or(left, right) => Expr::Or(
            Box::new(bind_expr(left, desc)?),
            Box::new(bind_expr(right, desc)?),
        ),
        SqlExpr::Not(inner) => Expr::Not(Box::new(bind_expr(inner, desc)?)),
        SqlExpr::IsNull(inner) => Expr::IsNull(Box::new(bind_expr(inner, desc)?)),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub target_indexes: Vec<usize>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub assignments: Vec<BoundAssignment>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub rel: RelFileLocator,
    pub desc: RelationDesc,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundAssignment {
    pub column_index: usize,
    pub expr: Expr,
}

pub fn bind_insert(
    stmt: &InsertStatement,
    catalog: &Catalog,
) -> Result<BoundInsertStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;

    let target_indexes = if let Some(columns) = &stmt.columns {
        columns
            .iter()
            .map(|column| resolve_column(&entry.desc, column))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        (0..entry.desc.columns.len()).collect()
    };

    for row in &stmt.values {
        if target_indexes.len() != row.len() {
            return Err(ParseError::InvalidInsertTargetCount {
                expected: target_indexes.len(),
                actual: row.len(),
            });
        }
    }

    Ok(BoundInsertStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        target_indexes,
        values: stmt
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| bind_expr(expr, &entry.desc))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub fn bind_update(
    stmt: &UpdateStatement,
    catalog: &Catalog,
) -> Result<BoundUpdateStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;

    Ok(BoundUpdateStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        assignments: stmt
            .assignments
            .iter()
            .map(|assignment| {
                Ok(BoundAssignment {
                    column_index: resolve_column(&entry.desc, &assignment.column)?,
                    expr: bind_expr(&assignment.expr, &entry.desc)?,
                })
            })
            .collect::<Result<Vec<_>, ParseError>>()?,
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr(expr, &entry.desc))
            .transpose()?,
    })
}

pub fn bind_delete(
    stmt: &DeleteStatement,
    catalog: &Catalog,
) -> Result<BoundDeleteStatement, ParseError> {
    let entry = catalog
        .get(&stmt.table_name)
        .ok_or_else(|| ParseError::UnknownTable(stmt.table_name.clone()))?;

    Ok(BoundDeleteStatement {
        rel: entry.rel,
        desc: entry.desc.clone(),
        predicate: stmt
            .where_clause
            .as_ref()
            .map(|expr| bind_expr(expr, &entry.desc))
            .transpose()?,
    })
}

fn resolve_column(desc: &RelationDesc, name: &str) -> Result<usize, ParseError> {
    if name.contains('.') {
        return Err(ParseError::UnsupportedQualifiedName(name.to_string()));
    }
    desc.columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| ParseError::UnknownColumn(name.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    Integer(String),
    StringLiteral(String),
    Comma,
    LParen,
    RParen,
    Eq,
    Lt,
    Gt,
    Star,
}

fn tokenize(sql: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }

        match ch {
            ',' => {
                chars.next();
                tokens.push(Token::Comma);
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            '=' => {
                chars.next();
                tokens.push(Token::Eq);
            }
            '<' => {
                chars.next();
                tokens.push(Token::Lt);
            }
            '>' => {
                chars.next();
                tokens.push(Token::Gt);
            }
            '*' => {
                chars.next();
                tokens.push(Token::Star);
            }
            '\'' => {
                chars.next();
                let mut value = String::new();
                while let Some(next) = chars.next() {
                    if next == '\'' {
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                            value.push('\'');
                            continue;
                        }
                        break;
                    }
                    value.push(next);
                }
                tokens.push(Token::StringLiteral(value));
            }
            c if c.is_ascii_digit() => {
                let mut value = String::new();
                while let Some(next) = chars.peek().copied() {
                    if next.is_ascii_digit() {
                        value.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Integer(value));
            }
            _ => {
                let mut value = String::new();
                while let Some(next) = chars.peek().copied() {
                    if next.is_ascii_alphanumeric() || next == '_' || next == '.' {
                        value.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Word(value));
            }
        }
    }

    tokens
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("select") => {
                self.parse_select().map(Statement::Select)
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("create") => {
                self.parse_create_table().map(Statement::CreateTable)
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("drop") => {
                self.parse_drop_table().map(Statement::DropTable)
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("insert") => {
                self.parse_insert().map(Statement::Insert)
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("update") => {
                self.parse_update().map(Statement::Update)
            }
            Some(Token::Word(word)) if word.eq_ignore_ascii_case("delete") => {
                self.parse_delete().map(Statement::Delete)
            }
            Some(_) => Err(ParseError::UnexpectedToken {
                expected: "statement",
                actual: self.describe_current(),
            }),
            None => Err(ParseError::UnexpectedEof),
        }
    }

    fn parse_select(&mut self) -> Result<SelectStatement, ParseError> {
        self.expect_keyword("select")?;
        let targets = self.parse_select_list()?;
        self.expect_keyword("from")?;
        let table_name = self.expect_word()?;
        let where_clause = if self.matches_keyword("where") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        if self.peek().is_some() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of query",
                actual: self.describe_current(),
            });
        }

        Ok(SelectStatement {
            table_name,
            targets,
            where_clause,
        })
    }

    fn parse_insert(&mut self) -> Result<InsertStatement, ParseError> {
        self.expect_keyword("insert")?;
        self.expect_keyword("into")?;
        let table_name = self.expect_word()?;
        let columns = if matches!(self.peek(), Some(Token::LParen)) {
            self.next();
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen, "')'")?;
            Some(cols)
        } else {
            None
        };
        self.expect_keyword("values")?;
        let mut values = Vec::new();
        loop {
            self.expect_token(Token::LParen, "'('")?;
            values.push(self.parse_expr_list()?);
            self.expect_token(Token::RParen, "')'")?;
            if !matches!(self.peek(), Some(Token::Comma)) {
                break;
            }
            self.next();
        }
        self.expect_end()?;
        Ok(InsertStatement {
            table_name,
            columns,
            values,
        })
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStatement, ParseError> {
        self.expect_keyword("create")?;
        self.expect_keyword("table")?;
        let table_name = self.expect_word()?;
        self.expect_token(Token::LParen, "'('")?;
        let mut columns = Vec::new();
        loop {
            let name = self.expect_word()?;
            let ty = self.parse_type_name()?;
            let nullable = if self.matches_keyword("not") {
                self.expect_keyword("null")?;
                false
            } else {
                self.matches_keyword("null");
                true
            };
            columns.push(ColumnDef { name, ty, nullable });
            if !matches!(self.peek(), Some(Token::Comma)) {
                break;
            }
            self.next();
        }
        self.expect_token(Token::RParen, "')'")?;
        self.expect_end()?;
        Ok(CreateTableStatement { table_name, columns })
    }

    fn parse_drop_table(&mut self) -> Result<DropTableStatement, ParseError> {
        self.expect_keyword("drop")?;
        self.expect_keyword("table")?;
        let table_name = self.expect_word()?;
        self.expect_end()?;
        Ok(DropTableStatement { table_name })
    }

    fn parse_update(&mut self) -> Result<UpdateStatement, ParseError> {
        self.expect_keyword("update")?;
        let table_name = self.expect_word()?;
        self.expect_keyword("set")?;
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_word()?;
            self.expect_token(Token::Eq, "'='")?;
            let expr = self.parse_expr()?;
            assignments.push(Assignment { column, expr });
            if !matches!(self.peek(), Some(Token::Comma)) {
                break;
            }
            self.next();
        }
        let where_clause = if self.matches_keyword("where") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        self.expect_end()?;
        Ok(UpdateStatement {
            table_name,
            assignments,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteStatement, ParseError> {
        self.expect_keyword("delete")?;
        self.expect_keyword("from")?;
        let table_name = self.expect_word()?;
        let where_clause = if self.matches_keyword("where") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        self.expect_end()?;
        Ok(DeleteStatement {
            table_name,
            where_clause,
        })
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        if matches!(self.peek(), Some(Token::Star)) {
            self.next();
            return Ok(vec![SelectItem {
                output_name: "*".into(),
                expr: SqlExpr::Column("*".into()),
            }]);
        }

        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let output_name = match &expr {
                SqlExpr::Column(name) => name.clone(),
                _ => format!("expr{}", items.len() + 1),
            };
            items.push(SelectItem { output_name, expr });
            if !matches!(self.peek(), Some(Token::Comma)) {
                break;
            }
            self.next();
        }

        if items.is_empty() {
            return Err(ParseError::EmptySelectList);
        }
        Ok(items)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut items = Vec::new();
        loop {
            items.push(self.expect_word()?);
            if !matches!(self.peek(), Some(Token::Comma)) {
                break;
            }
            self.next();
        }
        Ok(items)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<SqlExpr>, ParseError> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expr()?);
            if !matches!(self.peek(), Some(Token::Comma)) {
                break;
            }
            self.next();
        }
        Ok(items)
    }

    fn parse_expr(&mut self) -> Result<SqlExpr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<SqlExpr, ParseError> {
        let mut expr = self.parse_and()?;
        while self.matches_keyword("or") {
            let right = self.parse_and()?;
            expr = SqlExpr::Or(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<SqlExpr, ParseError> {
        let mut expr = self.parse_not()?;
        while self.matches_keyword("and") {
            let right = self.parse_not()?;
            expr = SqlExpr::And(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_not(&mut self) -> Result<SqlExpr, ParseError> {
        if self.matches_keyword("not") {
            return Ok(SqlExpr::Not(Box::new(self.parse_not()?)));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<SqlExpr, ParseError> {
        let mut expr = self.parse_primary()?;

        if self.matches_keyword("is") {
            self.expect_keyword("null")?;
            return Ok(SqlExpr::IsNull(Box::new(expr)));
        }

        if let Some(op) = self.match_comparison() {
            let right = self.parse_primary()?;
            expr = match op {
                Token::Eq => SqlExpr::Eq(Box::new(expr), Box::new(right)),
                Token::Lt => SqlExpr::Lt(Box::new(expr), Box::new(right)),
                Token::Gt => SqlExpr::Gt(Box::new(expr), Box::new(right)),
                _ => unreachable!(),
            };
        }

        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<SqlExpr, ParseError> {
        match self.next().ok_or(ParseError::UnexpectedEof)? {
            Token::Word(word) => {
                if word.eq_ignore_ascii_case("null") {
                    Ok(SqlExpr::Const(Value::Null))
                } else if word.eq_ignore_ascii_case("true") {
                    Ok(SqlExpr::Const(Value::Bool(true)))
                } else if word.eq_ignore_ascii_case("false") {
                    Ok(SqlExpr::Const(Value::Bool(false)))
                } else {
                    Ok(SqlExpr::Column(word))
                }
            }
            Token::Integer(value) => value
                .parse::<i32>()
                .map(Value::Int32)
                .map(SqlExpr::Const)
                .map_err(|_| ParseError::InvalidInteger(value)),
            Token::StringLiteral(value) => Ok(SqlExpr::Const(Value::Text(value))),
            Token::LParen => {
                let expr = self.parse_expr()?;
                self.expect_token(Token::RParen, "')'")?;
                Ok(expr)
            }
            other => Err(ParseError::UnexpectedToken {
                expected: "expression",
                actual: describe_token(&other),
            }),
        }
    }

    fn match_comparison(&mut self) -> Option<Token> {
        match self.peek() {
            Some(Token::Eq) | Some(Token::Lt) | Some(Token::Gt) => self.next(),
            _ => None,
        }
    }

    fn matches_keyword(&mut self, expected: &str) -> bool {
        match self.peek() {
            Some(Token::Word(word)) if word.eq_ignore_ascii_case(expected) => {
                self.next();
                true
            }
            _ => false,
        }
    }

    fn expect_keyword(&mut self, expected: &'static str) -> Result<(), ParseError> {
        match self.next() {
            Some(Token::Word(word)) if word.eq_ignore_ascii_case(expected) => Ok(()),
            Some(token) => Err(ParseError::UnexpectedToken {
                expected,
                actual: describe_token(&token),
            }),
            None => Err(ParseError::UnexpectedEof),
        }
    }

    fn expect_word(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Token::Word(word)) => Ok(word),
            Some(token) => Err(ParseError::UnexpectedToken {
                expected: "identifier",
                actual: describe_token(&token),
            }),
            None => Err(ParseError::UnexpectedEof),
        }
    }

    fn expect_token(
        &mut self,
        expected_token: Token,
        expected: &'static str,
    ) -> Result<(), ParseError> {
        match self.next() {
            Some(token) if token == expected_token => Ok(()),
            Some(token) => Err(ParseError::UnexpectedToken {
                expected,
                actual: describe_token(&token),
            }),
            None => Err(ParseError::UnexpectedEof),
        }
    }

    fn expect_end(&mut self) -> Result<(), ParseError> {
        if self.peek().is_some() {
            Err(ParseError::UnexpectedToken {
                expected: "end of query",
                actual: self.describe_current(),
            })
        } else {
            Ok(())
        }
    }

    fn describe_current(&self) -> String {
        self.peek()
            .map(|token| describe_token(&token))
            .unwrap_or_else(|| "end of input".into())
    }

    fn peek(&self) -> Option<Token> {
        self.tokens.get(self.pos).cloned()
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn parse_type_name(&mut self) -> Result<SqlType, ParseError> {
        match self.expect_word()?.to_ascii_lowercase().as_str() {
            "int4" | "int" | "integer" => Ok(SqlType::Int4),
            "text" => Ok(SqlType::Text),
            "bool" | "boolean" => Ok(SqlType::Bool),
            other => Err(ParseError::UnsupportedType(other.to_string())),
        }
    }
}

fn describe_token(token: &Token) -> String {
    match token {
        Token::Word(word) => format!("identifier {:?}", word),
        Token::Integer(value) => format!("integer {:?}", value),
        Token::StringLiteral(value) => format!("string {:?}", value),
        Token::Comma => ",".into(),
        Token::LParen => "(".into(),
        Token::RParen => ")".into(),
        Token::Eq => "=".into(),
        Token::Lt => "<".into(),
        Token::Gt => ">".into(),
        Token::Star => "*".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::tuple::{AttributeAlign, AttributeDesc};
    use crate::executor::{ColumnDesc, ScalarType};

    fn desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                ColumnDesc {
                    name: "id".into(),
                    storage: AttributeDesc {
                        name: "id".into(),
                        attlen: 4,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Int32,
                },
                ColumnDesc {
                    name: "name".into(),
                    storage: AttributeDesc {
                        name: "name".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Text,
                },
                ColumnDesc {
                    name: "note".into(),
                    storage: AttributeDesc {
                        name: "note".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: true,
                    },
                    ty: ScalarType::Text,
                },
            ],
        }
    }

    fn catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            "people",
            CatalogEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: 15000,
                },
                desc: desc(),
            },
        );
        catalog
    }

    #[test]
    fn parse_select_with_where() {
        let stmt =
            parse_select("select name, note from people where id > 1 and note is null").unwrap();
        assert_eq!(stmt.table_name, "people");
        assert_eq!(stmt.targets.len(), 2);
        assert!(matches!(stmt.where_clause, Some(SqlExpr::And(_, _))));
    }

    #[test]
    fn build_plan_resolves_columns() {
        let stmt = parse_select("select name, note from people where id > 1").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 2);
                match *input {
                    Plan::Filter { input, predicate } => {
                        assert!(matches!(predicate, Expr::Gt(_, _)));
                        assert!(matches!(*input, Plan::SeqScan { .. }));
                    }
                    other => panic!("expected filter, got {:?}", other),
                }
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn unknown_column_is_rejected() {
        let stmt = parse_select("select missing from people").unwrap();
        assert!(matches!(
            build_plan(&stmt, &catalog()),
            Err(ParseError::UnknownColumn(name)) if name == "missing"
        ));
    }

    #[test]
    fn select_star_expands_to_all_columns() {
        let stmt = parse_select("select * from people").unwrap();
        let plan = build_plan(&stmt, &catalog()).unwrap();
        match plan {
            Plan::Projection { input, targets } => {
                assert_eq!(targets.len(), 3);
                assert_eq!(targets[0].name, "id");
                assert_eq!(targets[1].name, "name");
                assert_eq!(targets[2].name, "note");
                assert!(matches!(*input, Plan::SeqScan { .. }));
            }
            other => panic!("expected projection, got {:?}", other),
        }
    }

    #[test]
    fn parse_insert_update_delete() {
        assert!(matches!(
            parse_statement("insert into people (id, name) values (1, 'alice')").unwrap(),
            Statement::Insert(InsertStatement { table_name, .. }) if table_name == "people"
        ));
        assert!(matches!(
            parse_statement("insert into people (id, name) values (1, 'alice'), (2, 'bob')").unwrap(),
            Statement::Insert(InsertStatement { table_name, values, .. })
                if table_name == "people" && values.len() == 2
        ));
        assert!(matches!(
            parse_statement("create table widgets (id int4 not null, name text)").unwrap(),
            Statement::CreateTable(CreateTableStatement { table_name, columns })
                if table_name == "widgets" && columns.len() == 2
        ));
        assert!(matches!(
            parse_statement("drop table widgets").unwrap(),
            Statement::DropTable(DropTableStatement { table_name }) if table_name == "widgets"
        ));
        assert!(matches!(
            parse_statement("update people set note = 'x' where id = 1").unwrap(),
            Statement::Update(UpdateStatement { table_name, .. }) if table_name == "people"
        ));
        assert!(matches!(
            parse_statement("delete from people where note is null").unwrap(),
            Statement::Delete(DeleteStatement { table_name, .. }) if table_name == "people"
        ));
    }
}
