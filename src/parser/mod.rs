pub use crate::catalog::{Catalog, CatalogEntry};

use crate::RelFileLocator;
use crate::catalog::column_desc;
use crate::executor::{Expr, Plan, RelationDesc, TargetEntry, Value};
use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "parser/sql.pest"]
struct SqlParser;

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
    let stmt = parse_statement(sql)?;
    match stmt {
        Statement::Select(stmt) => Ok(stmt),
        other => Err(ParseError::UnexpectedToken {
            expected: "SELECT",
            actual: format!("{other:?}"),
        }),
    }
}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    SqlParser::parse(Rule::statement, sql)
        .map_err(|e| map_pest_error("statement", e))
        .and_then(|mut pairs| build_statement(pairs.next().ok_or(ParseError::UnexpectedEof)?))
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

fn map_pest_error(expected: &'static str, err: pest::error::Error<Rule>) -> ParseError {
    use pest::error::ErrorVariant;

    match err.variant {
        ErrorVariant::ParsingError { .. } => ParseError::UnexpectedToken {
            expected,
            actual: err.to_string(),
        },
        ErrorVariant::CustomError { message } => ParseError::UnexpectedToken {
            expected,
            actual: message,
        },
    }
}

fn build_statement(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::select_stmt => Ok(Statement::Select(build_select(inner)?)),
        Rule::create_table_stmt => Ok(Statement::CreateTable(build_create_table(inner)?)),
        Rule::drop_table_stmt => Ok(Statement::DropTable(build_drop_table(inner)?)),
        Rule::insert_stmt => Ok(Statement::Insert(build_insert(inner)?)),
        Rule::update_stmt => Ok(Statement::Update(build_update(inner)?)),
        Rule::delete_stmt => Ok(Statement::Delete(build_delete(inner)?)),
        _ => Err(ParseError::UnexpectedToken {
            expected: "statement",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_select(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    let mut targets = None;
    let mut table_name = None;
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::select_list => targets = Some(build_select_list(part)?),
            Rule::identifier => table_name = Some(build_identifier(part)),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(SelectStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        targets: targets.ok_or(ParseError::EmptySelectList)?,
        where_clause,
    })
}

fn build_insert(pair: Pair<'_, Rule>) -> Result<InsertStatement, ParseError> {
    let mut table_name = None;
    let mut columns = None;
    let mut values = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::ident_list => {
                columns = Some(part.into_inner().map(build_identifier).collect::<Vec<_>>())
            }
            Rule::values_row => values.push(build_values_row(part)?),
            _ => {}
        }
    }

    Ok(InsertStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        values,
    })
}

fn build_create_table(pair: Pair<'_, Rule>) -> Result<CreateTableStatement, ParseError> {
    let mut table_name = None;
    let mut columns = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::column_def => columns.push(build_column_def(part)?),
            _ => {}
        }
    }
    Ok(CreateTableStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
    })
}

fn build_drop_table(pair: Pair<'_, Rule>) -> Result<DropTableStatement, ParseError> {
    let table_name = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(DropTableStatement { table_name })
}

fn build_update(pair: Pair<'_, Rule>) -> Result<UpdateStatement, ParseError> {
    let mut table_name = None;
    let mut assignments = Vec::new();
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::assignment => assignments.push(build_assignment(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(UpdateStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        assignments,
        where_clause,
    })
}

fn build_delete(pair: Pair<'_, Rule>) -> Result<DeleteStatement, ParseError> {
    let mut table_name = None;
    let mut where_clause = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::expr => where_clause = Some(build_expr(part)?),
            _ => {}
        }
    }
    Ok(DeleteStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        where_clause,
    })
}

fn build_select_list(pair: Pair<'_, Rule>) -> Result<Vec<SelectItem>, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or(ParseError::EmptySelectList)?;
    if first.as_rule() == Rule::star {
        return Ok(vec![SelectItem {
            output_name: "*".into(),
            expr: SqlExpr::Column("*".into()),
        }]);
    }

    let mut items = Vec::new();
    {
        let expr = build_expr(first)?;
        let output_name = match &expr {
            SqlExpr::Column(name) => name.clone(),
            _ => format!("expr{}", items.len() + 1),
        };
        items.push(SelectItem { output_name, expr });
    }

    for expr_pair in inner {
        let expr = build_expr(expr_pair)?;
        let output_name = match &expr {
            SqlExpr::Column(name) => name.clone(),
            _ => format!("expr{}", items.len() + 1),
        };
        items.push(SelectItem { output_name, expr });
    }

    Ok(items)
}

fn build_values_row(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .next()
        .ok_or(ParseError::UnexpectedEof)?
        .into_inner()
        .map(build_expr)
        .collect()
}

fn build_assignment(pair: Pair<'_, Rule>) -> Result<Assignment, ParseError> {
    let mut inner = pair.into_inner();
    Ok(Assignment {
        column: build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?),
        expr: build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?,
    })
}

fn build_column_def(pair: Pair<'_, Rule>) -> Result<ColumnDef, ParseError> {
    let mut inner = pair.into_inner();
    let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let ty = build_type(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let nullable = match inner.next() {
        Some(flag) => flag.as_rule() == Rule::nullable,
        None => true,
    };
    Ok(ColumnDef { name, ty, nullable })
}

fn build_type(pair: Pair<'_, Rule>) -> SqlType {
    match pair.as_str().to_ascii_lowercase().as_str() {
        "int4" | "int" | "integer" => SqlType::Int4,
        "text" => SqlType::Text,
        "bool" | "boolean" => SqlType::Bool,
        _ => unreachable!(),
    }
}

fn build_identifier(pair: Pair<'_, Rule>) -> String {
    pair.as_str().to_string()
}

fn build_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    match pair.as_rule() {
        Rule::expr | Rule::or_expr | Rule::and_expr => {
            let mut inner = pair.into_inner();
            let first = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            fold_infix(first, inner)
        }
        Rule::not_expr => {
            let mut inner = pair.into_inner();
            let first = inner.next().ok_or(ParseError::UnexpectedEof)?;
            if first.as_rule() == Rule::kw_not {
                Ok(SqlExpr::Not(Box::new(build_expr(
                    inner.next().ok_or(ParseError::UnexpectedEof)?,
                )?)))
            } else {
                build_expr(first)
            }
        }
        Rule::cmp_expr => {
            let mut inner = pair.into_inner();
            let left = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let Some(next) = inner.next() else {
                return Ok(left);
            };

            match next.as_rule() {
                Rule::is_null_suffix => Ok(SqlExpr::IsNull(Box::new(left))),
                Rule::comp_op => {
                    let right = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(match next.as_str() {
                        "=" => SqlExpr::Eq(Box::new(left), Box::new(right)),
                        "<" => SqlExpr::Lt(Box::new(left), Box::new(right)),
                        ">" => SqlExpr::Gt(Box::new(left), Box::new(right)),
                        _ => unreachable!(),
                    })
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "comparison",
                    actual: next.as_str().into(),
                }),
            }
        }
        Rule::primary_expr => build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?),
        Rule::identifier => Ok(SqlExpr::Column(pair.as_str().to_string())),
        Rule::integer => pair
            .as_str()
            .parse::<i32>()
            .map(|value| SqlExpr::Const(Value::Int32(value)))
            .map_err(|_| ParseError::InvalidInteger(pair.as_str().into())),
        Rule::string_literal => Ok(SqlExpr::Const(Value::Text(unescape_string(pair.as_str())))),
        Rule::kw_null => Ok(SqlExpr::Const(Value::Null)),
        Rule::kw_true => Ok(SqlExpr::Const(Value::Bool(true))),
        Rule::kw_false => Ok(SqlExpr::Const(Value::Bool(false))),
        _ => Err(ParseError::UnexpectedToken {
            expected: "expression",
            actual: pair.as_str().into(),
        }),
    }
}

fn fold_infix(
    first: SqlExpr,
    mut tail: pest::iterators::Pairs<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    let mut expr = first;
    while let Some(op) = tail.next() {
        let rhs = build_expr(tail.next().ok_or(ParseError::UnexpectedEof)?)?;
        expr = match op.as_rule() {
            Rule::kw_or => SqlExpr::Or(Box::new(expr), Box::new(rhs)),
            Rule::kw_and => SqlExpr::And(Box::new(expr), Box::new(rhs)),
            _ => unreachable!(),
        };
    }
    Ok(expr)
}

fn unescape_string(raw: &str) -> String {
    raw[1..raw.len() - 1].replace("''", "'")
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
    fn pest_matches_basic_select_keyword() {
        let mut pairs = SqlParser::parse(Rule::kw_select_atom, "select").unwrap();
        assert_eq!(pairs.next().unwrap().as_str(), "select");
    }

    #[test]
    fn pest_matches_minimal_select_statement() {
        let mut pairs = SqlParser::parse(Rule::statement, "select id from people").unwrap();
        let stmt = build_statement(pairs.next().unwrap()).unwrap();
        match stmt {
            Statement::Select(stmt) => {
                assert_eq!(stmt.table_name, "people");
                assert_eq!(stmt.targets.len(), 1);
            }
            other => panic!("expected select statement, got {other:?}"),
        }
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
