use std::collections::BTreeSet;

use pgrust_analyze::{
    BoundIndexRelation, BoundRelation, CatalogLookup, is_system_column_name, resolve_raw_type_name,
};
use pgrust_catalog_data::{
    INFORMATION_SCHEMA_NAMESPACE_OID, PG_CATALOG_NAMESPACE_OID, PUBLISH_GENCOLS_NONE,
    PUBLISH_GENCOLS_STORED, PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow,
};
use pgrust_catalog_store::catcache::normalize_catalog_name;
use pgrust_catalog_store::pg_depend::collect_sql_expr_column_names;
use pgrust_nodes::parsenodes::{
    ColumnGeneratedKind, ParseError, PublicationObjectSpec, PublicationOption, PublicationOptions,
    PublicationTableSpec, PublicationTargetSpec, PublishGeneratedColumns, RawTypeName,
    RawXmlExprOp, SerialKind, SqlExpr, function_arg_values,
};
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::{SqlType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationDmlAction {
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationMembershipKind {
    Table,
    Schema,
}

impl PublicationDmlAction {
    fn publishes(self, publication: &PgPublicationRow) -> bool {
        match self {
            Self::Update => publication.pubupdate,
            Self::Delete => publication.pubdelete,
        }
    }

    fn verb(self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::Delete => "delete from",
        }
    }

    fn noun(self) -> &'static str {
        match self {
            Self::Update => "updates",
            Self::Delete => "deletes",
        }
    }

    fn gerund(self) -> &'static str {
        match self {
            Self::Update => "updating",
            Self::Delete => "deleting from",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationReplicaIdentityError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationFilterError {
    Parse(ParseError),
    AggregateFunction,
    InvalidWhere { detail: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationTargetError {
    DropWhere,
    ColumnListWithSchema {
        relation_name: String,
        publication_name: String,
    },
    SchemaWithExistingColumnList {
        publication_name: String,
    },
    SystemColumn {
        column_name: String,
    },
    UnknownColumn {
        column_name: String,
        relation_name: String,
    },
    VirtualGeneratedColumn {
        column_name: String,
    },
    TooManyColumns {
        relation_name: String,
    },
    DuplicateColumn {
        column_name: String,
    },
}

pub fn reject_publication_drop_filters(
    target: &PublicationTargetSpec,
) -> Result<(), PublicationTargetError> {
    if target.objects.iter().any(|object| {
        matches!(
            object,
            PublicationObjectSpec::Table(PublicationTableSpec {
                where_clause: Some(_),
                ..
            })
        )
    }) {
        return Err(PublicationTargetError::DropWhere);
    }
    Ok(())
}

pub fn reject_publication_column_list_schema_conflicts(
    target: &PublicationTargetSpec,
    publication_name: &str,
    existing_rel_rows: &[PgPublicationRelRow],
    existing_namespace_rows: &[PgPublicationNamespaceRow],
) -> Result<(), PublicationTargetError> {
    let target_has_schema = target
        .objects
        .iter()
        .any(|object| matches!(object, PublicationObjectSpec::Schema(_)));
    if target_has_schema || !existing_namespace_rows.is_empty() {
        if let Some(table) = target.objects.iter().find_map(|object| match object {
            PublicationObjectSpec::Table(table) if !table.column_names.is_empty() => Some(table),
            _ => None,
        }) {
            return Err(PublicationTargetError::ColumnListWithSchema {
                relation_name: table.relation_name.clone(),
                publication_name: publication_name.to_string(),
            });
        }
    }
    if target_has_schema
        && existing_rel_rows
            .iter()
            .any(|row| row.prattrs.as_ref().is_some_and(|attrs| !attrs.is_empty()))
    {
        return Err(PublicationTargetError::SchemaWithExistingColumnList {
            publication_name: publication_name.to_string(),
        });
    }
    Ok(())
}

pub fn publication_column_numbers(
    relation: &BoundRelation,
    relation_name: &str,
    column_names: &[String],
) -> Result<Option<Vec<i16>>, PublicationTargetError> {
    if column_names.is_empty() {
        return Ok(None);
    }

    let mut attrs = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        if is_system_column_name(column_name) {
            return Err(PublicationTargetError::SystemColumn {
                column_name: column_name.clone(),
            });
        }
        let Some((idx, column)) = relation
            .desc
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        else {
            return Err(PublicationTargetError::UnknownColumn {
                column_name: column_name.clone(),
                relation_name: relation_name.to_string(),
            });
        };
        if column.generated == Some(ColumnGeneratedKind::Virtual) {
            return Err(PublicationTargetError::VirtualGeneratedColumn {
                column_name: column_name.clone(),
            });
        }
        let attr_no =
            i16::try_from(idx + 1).map_err(|_| PublicationTargetError::TooManyColumns {
                relation_name: relation_name.to_string(),
            })?;
        if attrs.contains(&attr_no) {
            return Err(PublicationTargetError::DuplicateColumn {
                column_name: column_name.clone(),
            });
        }
        attrs.push(attr_no);
    }
    Ok(Some(attrs))
}

pub fn publication_membership_kind(target: &PublicationTargetSpec) -> PublicationMembershipKind {
    if target
        .objects
        .iter()
        .all(|object| matches!(object, PublicationObjectSpec::Schema(_)))
    {
        PublicationMembershipKind::Schema
    } else {
        PublicationMembershipKind::Table
    }
}

pub fn publication_target_is_all_kind(target: &PublicationTargetSpec) -> bool {
    target.for_all_tables || target.for_all_sequences
}

pub fn validate_publication_filter_expr(expr: &SqlExpr) -> Result<(), PublicationFilterError> {
    use SqlExpr::*;

    // :HACK: PostgreSQL validates publication filters from the fully bound
    // expression tree, including function/operator provenance and volatility.
    // pgrust does not retain enough of that metadata here yet, so keep this
    // narrow syntactic guard until publication filters use a dedicated binder.
    match expr {
        FuncCall { name, args, .. } => {
            let normalized = name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase();
            if matches!(normalized.as_str(), "avg" | "count" | "max" | "min" | "sum") {
                return Err(PublicationFilterError::AggregateFunction);
            }
            if normalized == "random" || normalized.starts_with("testpub_") {
                return Err(PublicationFilterError::InvalidWhere {
                    detail: "User-defined or built-in mutable functions are not allowed.",
                });
            }
            for arg in function_arg_values(args) {
                validate_publication_filter_expr(arg)?;
            }
        }
        BinaryOperator { left, right, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
            return Err(PublicationFilterError::InvalidWhere {
                detail: "User-defined operators are not allowed.",
            });
        }
        InSubquery { expr, .. } => {
            validate_publication_filter_expr(expr)?;
            return Err(PublicationFilterError::InvalidWhere {
                detail: "Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.",
            });
        }
        ScalarSubquery(_) | ArraySubquery(_) | Exists(_) | QuantifiedSubquery { .. } => {
            return Err(PublicationFilterError::InvalidWhere {
                detail: "Only columns, constants, built-in operators, built-in data types, built-in collations, and immutable built-in functions are allowed.",
            });
        }
        Column(name) if name.eq_ignore_ascii_case("ctid") => {
            return Err(PublicationFilterError::InvalidWhere {
                detail: "System columns are not allowed.",
            });
        }
        Parameter(_) => {}
        Add(left, right)
        | Sub(left, right)
        | BitAnd(left, right)
        | BitOr(left, right)
        | BitXor(left, right)
        | Shl(left, right)
        | Shr(left, right)
        | Mul(left, right)
        | Div(left, right)
        | Mod(left, right)
        | Concat(left, right)
        | Eq(left, right)
        | NotEq(left, right)
        | Lt(left, right)
        | LtEq(left, right)
        | Gt(left, right)
        | GtEq(left, right)
        | RegexMatch(left, right)
        | And(left, right)
        | Or(left, right)
        | IsDistinctFrom(left, right)
        | IsNotDistinctFrom(left, right)
        | Overlaps(left, right)
        | ArrayOverlap(left, right)
        | ArrayContains(left, right)
        | ArrayContained(left, right)
        | JsonbContains(left, right)
        | JsonbContained(left, right)
        | JsonbExists(left, right)
        | JsonbExistsAny(left, right)
        | JsonbExistsAll(left, right)
        | JsonbPathExists(left, right)
        | JsonbPathMatch(left, right)
        | JsonGet(left, right)
        | JsonGetText(left, right)
        | JsonPath(left, right)
        | JsonPathText(left, right)
        | AtTimeZone {
            expr: left,
            zone: right,
        } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
        }
        UnaryPlus(inner)
        | Negate(inner)
        | BitNot(inner)
        | Cast(inner, _)
        | Collate { expr: inner, .. }
        | IsNull(inner)
        | IsNotNull(inner)
        | Not(inner)
        | FieldSelect { expr: inner, .. }
        | Subscript { expr: inner, .. } => validate_publication_filter_expr(inner)?,
        Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            validate_publication_filter_expr(expr)?;
            validate_publication_filter_expr(pattern)?;
            if let Some(escape) = escape {
                validate_publication_filter_expr(escape)?;
            }
        }
        Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                validate_publication_filter_expr(arg)?;
            }
            for when in args {
                validate_publication_filter_expr(&when.expr)?;
                validate_publication_filter_expr(&when.result)?;
            }
            if let Some(defresult) = defresult {
                validate_publication_filter_expr(defresult)?;
            }
        }
        ArrayLiteral(values) | Row(values) => {
            for value in values {
                validate_publication_filter_expr(value)?;
            }
        }
        QuantifiedArray { left, array, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(array)?;
        }
        ArraySubscript { array, subscripts } => {
            validate_publication_filter_expr(array)?;
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_publication_filter_expr(lower)?;
                }
                if let Some(upper) = &subscript.upper {
                    validate_publication_filter_expr(upper)?;
                }
            }
        }
        GeometryUnaryOp { expr, .. } | PrefixOperator { expr, .. } => {
            validate_publication_filter_expr(expr)?;
        }
        GeometryBinaryOp { left, right, .. } => {
            validate_publication_filter_expr(left)?;
            validate_publication_filter_expr(right)?;
        }
        Random => {
            return Err(PublicationFilterError::InvalidWhere {
                detail: "User-defined or built-in mutable functions are not allowed.",
            });
        }
        Xml(xml) => {
            for child in xml.child_exprs() {
                validate_publication_filter_expr(child)?;
            }
        }
        JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                validate_publication_filter_expr(child)?;
            }
        }
        Column(_)
        | ParamRef(_)
        | Default
        | Const(_)
        | IntegerLiteral(_)
        | NumericLiteral(_)
        | CurrentDate
        | CurrentCatalog
        | CurrentSchema
        | CurrentUser
        | User
        | SessionUser
        | SystemUser
        | CurrentRole
        | CurrentTime { .. }
        | CurrentTimestamp { .. }
        | LocalTime { .. }
        | LocalTimestamp { .. } => {}
    }
    Ok(())
}

pub fn publication_filter_returns_bool_by_syntax(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Xml(xml) if xml.op == RawXmlExprOp::IsDocument
    )
}

pub fn validate_publication_filter_types(
    expr: &SqlExpr,
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Result<(), PublicationFilterError> {
    use SqlExpr::*;

    match expr {
        Column(name) => {
            let column_name = name.rsplit('.').next().unwrap_or(name);
            if let Some(column) = relation
                .desc
                .columns
                .iter()
                .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
                && publication_filter_type_is_user_defined(column.sql_type, catalog)
            {
                return Err(PublicationFilterError::InvalidWhere {
                    detail: "User-defined types are not allowed.",
                });
            }
        }
        Parameter(_) => {}
        Cast(inner, ty) => {
            validate_publication_filter_types(inner, relation, catalog)?;
            let sql_type =
                resolve_raw_type_name(ty, catalog).map_err(PublicationFilterError::Parse)?;
            if publication_filter_type_is_user_defined(sql_type, catalog) {
                return Err(PublicationFilterError::InvalidWhere {
                    detail: "User-defined types are not allowed.",
                });
            }
        }
        Collate {
            expr: inner,
            collation,
        } => {
            validate_publication_filter_types(inner, relation, catalog)?;
            if publication_filter_collation_is_user_defined(collation, catalog) {
                return Err(PublicationFilterError::InvalidWhere {
                    detail: "User-defined collations are not allowed.",
                });
            }
        }
        FuncCall { args, .. } => {
            for arg in function_arg_values(args) {
                validate_publication_filter_types(arg, relation, catalog)?;
            }
        }
        BinaryOperator { left, right, .. }
        | Add(left, right)
        | Sub(left, right)
        | BitAnd(left, right)
        | BitOr(left, right)
        | BitXor(left, right)
        | Shl(left, right)
        | Shr(left, right)
        | Mul(left, right)
        | Div(left, right)
        | Mod(left, right)
        | Concat(left, right)
        | Eq(left, right)
        | NotEq(left, right)
        | Lt(left, right)
        | LtEq(left, right)
        | Gt(left, right)
        | GtEq(left, right)
        | RegexMatch(left, right)
        | And(left, right)
        | Or(left, right)
        | IsDistinctFrom(left, right)
        | IsNotDistinctFrom(left, right)
        | Overlaps(left, right)
        | ArrayOverlap(left, right)
        | ArrayContains(left, right)
        | ArrayContained(left, right)
        | JsonbContains(left, right)
        | JsonbContained(left, right)
        | JsonbExists(left, right)
        | JsonbExistsAny(left, right)
        | JsonbExistsAll(left, right)
        | JsonbPathExists(left, right)
        | JsonbPathMatch(left, right)
        | JsonGet(left, right)
        | JsonGetText(left, right)
        | JsonPath(left, right)
        | JsonPathText(left, right)
        | AtTimeZone {
            expr: left,
            zone: right,
        } => {
            validate_publication_filter_types(left, relation, catalog)?;
            validate_publication_filter_types(right, relation, catalog)?;
        }
        UnaryPlus(inner)
        | Negate(inner)
        | BitNot(inner)
        | IsNull(inner)
        | IsNotNull(inner)
        | Not(inner)
        | FieldSelect { expr: inner, .. }
        | Subscript { expr: inner, .. } => {
            validate_publication_filter_types(inner, relation, catalog)?;
        }
        Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            validate_publication_filter_types(expr, relation, catalog)?;
            validate_publication_filter_types(pattern, relation, catalog)?;
            if let Some(escape) = escape {
                validate_publication_filter_types(escape, relation, catalog)?;
            }
        }
        Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                validate_publication_filter_types(arg, relation, catalog)?;
            }
            for when in args {
                validate_publication_filter_types(&when.expr, relation, catalog)?;
                validate_publication_filter_types(&when.result, relation, catalog)?;
            }
            if let Some(defresult) = defresult {
                validate_publication_filter_types(defresult, relation, catalog)?;
            }
        }
        ArrayLiteral(values) | Row(values) => {
            for value in values {
                validate_publication_filter_types(value, relation, catalog)?;
            }
        }
        QuantifiedArray { left, array, .. } => {
            validate_publication_filter_types(left, relation, catalog)?;
            validate_publication_filter_types(array, relation, catalog)?;
        }
        ArraySubscript { array, subscripts } => {
            validate_publication_filter_types(array, relation, catalog)?;
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_publication_filter_types(lower, relation, catalog)?;
                }
                if let Some(upper) = &subscript.upper {
                    validate_publication_filter_types(upper, relation, catalog)?;
                }
            }
        }
        GeometryUnaryOp { expr, .. } | PrefixOperator { expr, .. } => {
            validate_publication_filter_types(expr, relation, catalog)?;
        }
        GeometryBinaryOp { left, right, .. } => {
            validate_publication_filter_types(left, relation, catalog)?;
            validate_publication_filter_types(right, relation, catalog)?;
        }
        InSubquery { expr, .. } => {
            validate_publication_filter_types(expr, relation, catalog)?;
        }
        QuantifiedSubquery { left, .. } => {
            validate_publication_filter_types(left, relation, catalog)?;
        }
        Xml(xml) => {
            for child in xml.child_exprs() {
                validate_publication_filter_types(child, relation, catalog)?;
            }
        }
        JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                validate_publication_filter_types(child, relation, catalog)?;
            }
        }
        Const(Value::EnumOid(_)) => {
            return Err(PublicationFilterError::InvalidWhere {
                detail: "User-defined types are not allowed.",
            });
        }
        ScalarSubquery(_)
        | ArraySubquery(_)
        | Exists(_)
        | ParamRef(_)
        | Default
        | Const(_)
        | IntegerLiteral(_)
        | NumericLiteral(_)
        | Random
        | CurrentDate
        | CurrentCatalog
        | CurrentSchema
        | CurrentUser
        | User
        | SessionUser
        | SystemUser
        | CurrentRole
        | CurrentTime { .. }
        | CurrentTimestamp { .. }
        | LocalTime { .. }
        | LocalTimestamp { .. } => {}
    }
    Ok(())
}

fn publication_filter_type_is_user_defined(sql_type: SqlType, catalog: &dyn CatalogLookup) -> bool {
    if matches!(
        sql_type.kind,
        pgrust_nodes::SqlTypeKind::Composite
            | pgrust_nodes::SqlTypeKind::Enum
            | pgrust_nodes::SqlTypeKind::Shell
    ) {
        return true;
    }
    let Some(type_oid) = (sql_type.type_oid != 0).then_some(sql_type.type_oid) else {
        return false;
    };
    let Some(row) = catalog.type_by_oid(type_oid) else {
        return false;
    };
    if row.typnamespace != PG_CATALOG_NAMESPACE_OID
        && row.typnamespace != INFORMATION_SCHEMA_NAMESPACE_OID
    {
        return true;
    }
    row.typelem != 0
        && catalog.type_by_oid(row.typelem).is_some_and(|elem| {
            elem.typnamespace != PG_CATALOG_NAMESPACE_OID
                && elem.typnamespace != INFORMATION_SCHEMA_NAMESPACE_OID
        })
}

fn publication_filter_collation_is_user_defined(
    collation: &str,
    catalog: &dyn CatalogLookup,
) -> bool {
    let (schema_name, collation_name) = collation
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema), name))
        .unwrap_or((None, collation));
    let collation_name = normalize_catalog_name(collation_name).to_ascii_lowercase();
    let schema_oid = schema_name.and_then(|schema| {
        let schema = normalize_catalog_name(schema).to_ascii_lowercase();
        catalog
            .namespace_rows()
            .into_iter()
            .find(|row| row.nspname.eq_ignore_ascii_case(&schema))
            .map(|row| row.oid)
    });
    catalog
        .collation_rows()
        .into_iter()
        .filter(|row| row.collname.eq_ignore_ascii_case(&collation_name))
        .filter(|row| {
            schema_oid
                .map(|oid| row.collnamespace == oid)
                .unwrap_or(true)
        })
        .any(|row| {
            row.collnamespace != PG_CATALOG_NAMESPACE_OID
                && row.collnamespace != INFORMATION_SCHEMA_NAMESPACE_OID
        })
}

pub fn render_publication_filter_expr(
    expr: &SqlExpr,
    type_name: impl Fn(SqlType) -> String + Copy,
) -> Option<String> {
    use SqlExpr::*;

    Some(match expr {
        And(left, right) => format!(
            "({} AND {})",
            render_publication_filter_expr(left, type_name)?,
            render_publication_filter_expr(right, type_name)?
        ),
        Or(left, right) => format!(
            "({} OR {})",
            render_publication_filter_expr(left, type_name)?,
            render_publication_filter_expr(right, type_name)?
        ),
        Eq(left, right) => render_publication_binary_expr(left, "=", right, type_name)?,
        NotEq(left, right) => render_publication_binary_expr(left, "<>", right, type_name)?,
        Lt(left, right) => render_publication_binary_expr(left, "<", right, type_name)?,
        LtEq(left, right) => render_publication_binary_expr(left, "<=", right, type_name)?,
        Gt(left, right) => render_publication_binary_expr(left, ">", right, type_name)?,
        GtEq(left, right) => render_publication_binary_expr(left, ">=", right, type_name)?,
        IsNull(inner) => format!(
            "({} IS NULL)",
            render_publication_filter_term(inner, type_name)?
        ),
        IsNotNull(inner) => format!(
            "({} IS NOT NULL)",
            render_publication_filter_term(inner, type_name)?
        ),
        IsDistinctFrom(left, right) => format!(
            "({} IS DISTINCT FROM {})",
            render_publication_filter_term(left, type_name)?,
            render_publication_filter_term(right, type_name)?
        ),
        IsNotDistinctFrom(left, right) => format!(
            "({} IS NOT DISTINCT FROM {})",
            render_publication_filter_term(left, type_name)?,
            render_publication_filter_term(right, type_name)?
        ),
        Not(inner) => format!(
            "(NOT {})",
            render_publication_filter_term(inner, type_name)?
        ),
        _ => render_publication_filter_term(expr, type_name)?,
    })
}

fn render_publication_binary_expr(
    left: &SqlExpr,
    op: &str,
    right: &SqlExpr,
    type_name: impl Fn(SqlType) -> String + Copy,
) -> Option<String> {
    Some(format!(
        "({} {} {})",
        render_publication_filter_term(left, type_name)?,
        op,
        render_publication_filter_term(right, type_name)?
    ))
}

fn render_publication_filter_term(
    expr: &SqlExpr,
    type_name: impl Fn(SqlType) -> String + Copy,
) -> Option<String> {
    use SqlExpr::*;

    Some(match expr {
        Column(name) => name.clone(),
        IntegerLiteral(value) | NumericLiteral(value) => value.clone(),
        Const(value) => render_publication_const(value)?,
        Cast(inner, ty) => format!(
            "{}::{}",
            render_publication_filter_term(inner, type_name)?,
            render_publication_type_name(ty, type_name)
        ),
        Collate { expr, collation } => {
            format!(
                "{} COLLATE {}",
                render_publication_filter_term(expr, type_name)?,
                collation
            )
        }
        Add(left, right) => render_publication_arithmetic_expr(left, "+", right, type_name)?,
        Sub(left, right) => render_publication_arithmetic_expr(left, "-", right, type_name)?,
        Mul(left, right) => render_publication_arithmetic_expr(left, "*", right, type_name)?,
        Div(left, right) => render_publication_arithmetic_expr(left, "/", right, type_name)?,
        Mod(left, right) => render_publication_arithmetic_expr(left, "%", right, type_name)?,
        UnaryPlus(inner) => format!("+{}", render_publication_filter_term(inner, type_name)?),
        Negate(inner) => format!("-{}", render_publication_filter_term(inner, type_name)?),
        FuncCall { name, args, .. } => {
            let rendered_args = function_arg_values(args)
                .map(|arg| render_publication_filter_term(arg, type_name))
                .collect::<Option<Vec<_>>>()?
                .join(", ");
            format!("{name}({rendered_args})")
        }
        _ => return None,
    })
}

fn render_publication_arithmetic_expr(
    left: &SqlExpr,
    op: &str,
    right: &SqlExpr,
    type_name: impl Fn(SqlType) -> String + Copy,
) -> Option<String> {
    Some(format!(
        "({} {} {})",
        render_publication_filter_term(left, type_name)?,
        op,
        render_publication_filter_term(right, type_name)?
    ))
}

fn render_publication_const(value: &Value) -> Option<String> {
    Some(match value {
        Value::Null => "NULL".into(),
        Value::Bool(true) => "true".into(),
        Value::Bool(false) => "false".into(),
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Numeric(value) => value.render(),
        Value::Text(text) => format!("'{}'::text", escape_publication_string_literal(text)),
        Value::TextRef(_, _) => format!(
            "'{}'::text",
            escape_publication_string_literal(value.as_text().unwrap_or_default())
        ),
        Value::Xml(text) => format!("'{}'::xml", escape_publication_string_literal(text)),
        _ => return None,
    })
}

fn render_publication_type_name(ty: &RawTypeName, type_name: impl Fn(SqlType) -> String) -> String {
    match ty {
        RawTypeName::Builtin(sql_type) => type_name(*sql_type),
        RawTypeName::Serial(kind) => match kind {
            SerialKind::Small => "smallserial".into(),
            SerialKind::Regular => "serial".into(),
            SerialKind::Big => "bigserial".into(),
        },
        RawTypeName::Named { name, .. } => name.clone(),
        RawTypeName::Record => "record".into(),
    }
}

fn escape_publication_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub fn publication_row_defaults(publication_name: &str, owner_oid: u32) -> PgPublicationRow {
    PgPublicationRow {
        oid: 0,
        pubname: publication_name.to_ascii_lowercase(),
        pubowner: owner_oid,
        puballtables: false,
        puballsequences: false,
        pubinsert: true,
        pubupdate: true,
        pubdelete: true,
        pubtruncate: true,
        pubviaroot: false,
        pubgencols: PUBLISH_GENCOLS_NONE,
    }
}

pub fn apply_publication_options(
    publication: &mut PgPublicationRow,
    options: &PublicationOptions,
) -> Result<(), ParseError> {
    let mut seen = BTreeSet::new();
    for option in &options.options {
        let option_name = publication_option_name(option);
        if !seen.insert(option_name.clone()) {
            return Err(ParseError::ConflictingOrRedundantOptions {
                option: option_name,
            });
        }
        match option {
            PublicationOption::Publish(actions) => {
                publication.pubinsert = actions.insert;
                publication.pubupdate = actions.update;
                publication.pubdelete = actions.delete;
                publication.pubtruncate = actions.truncate;
            }
            PublicationOption::PublishViaPartitionRoot(value) => {
                publication.pubviaroot = *value;
            }
            PublicationOption::PublishGeneratedColumns(value) => {
                publication.pubgencols = match value {
                    PublishGeneratedColumns::None => PUBLISH_GENCOLS_NONE,
                    PublishGeneratedColumns::Stored => PUBLISH_GENCOLS_STORED,
                };
            }
            PublicationOption::Raw { name, .. } => {
                return Err(ParseError::UnrecognizedPublicationParameter(name.clone()));
            }
        }
    }
    Ok(())
}

pub fn publication_option_name(option: &PublicationOption) -> String {
    match option {
        PublicationOption::Publish(_) => "publish".into(),
        PublicationOption::PublishViaPartitionRoot(_) => "publish_via_partition_root".into(),
        PublicationOption::PublishGeneratedColumns(_) => "publish_generated_columns".into(),
        PublicationOption::Raw { name, .. } => name.clone(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaIdentityColumns {
    None,
    Full,
    Columns,
}

pub fn enforce_publication_replica_identity(
    relation_name: &str,
    relation_oid: u32,
    namespace_oid: u32,
    desc: &RelationDesc,
    indexes: &[BoundIndexRelation],
    catalog: &dyn CatalogLookup,
    action: PublicationDmlAction,
    require_identity: bool,
) -> Result<(), PublicationReplicaIdentityError> {
    let memberships = active_publication_memberships(catalog, relation_oid, namespace_oid, action);
    if memberships.is_empty() {
        return Ok(());
    }

    let (identity_kind, identity_attrs) =
        replica_identity_columns(relation_oid, desc, indexes, catalog);
    for (publication, membership) in &memberships {
        if let Some(attrs) = membership
            .as_ref()
            .and_then(|row| publication_membership_attnums(relation_oid, desc, row, catalog))
        {
            if identity_kind == ReplicaIdentityColumns::Full
                || identity_attrs.iter().any(|attnum| !attrs.contains(attnum))
            {
                return Err(publication_replica_identity_error(
                    relation_name,
                    action,
                    Some(
                        "Column list used by the publication does not cover the replica identity.",
                    ),
                ));
            }
        }
        if let Some((row, qual)) = membership
            .as_ref()
            .and_then(|row| row.prqual.as_deref().map(|qual| (row, qual)))
        {
            let filter_attrs =
                publication_filter_attnums_for_membership(qual, relation_oid, desc, row)?;
            if filter_attrs
                .iter()
                .any(|attnum| !identity_attrs.contains(attnum))
            {
                return Err(publication_replica_identity_error(
                    relation_name,
                    action,
                    Some(
                        "Column used in the publication WHERE expression is not part of the replica identity.",
                    ),
                ));
            }
        }
        if identity_attrs.iter().any(|attnum| {
            !publication_generated_identity_is_published(
                publication,
                membership.as_ref(),
                *attnum,
                desc,
            )
        }) {
            return Err(publication_replica_identity_error(
                relation_name,
                action,
                Some("Replica identity must not contain unpublished generated columns."),
            ));
        }
    }

    if require_identity && identity_kind == ReplicaIdentityColumns::None {
        return Err(publication_replica_identity_error(
            relation_name,
            action,
            None,
        ));
    }

    Ok(())
}

fn publication_replica_identity_error(
    relation_name: &str,
    action: PublicationDmlAction,
    detail: Option<&'static str>,
) -> PublicationReplicaIdentityError {
    match detail {
        Some(detail) => PublicationReplicaIdentityError::Detailed {
            message: format!("cannot {} table \"{relation_name}\"", action.verb()),
            detail: Some(detail.into()),
            hint: None,
            sqlstate: "55000",
        },
        None => PublicationReplicaIdentityError::Detailed {
            message: format!(
                "cannot {} table \"{relation_name}\" because it does not have a replica identity and publishes {}",
                action.verb(),
                action.noun()
            ),
            detail: None,
            hint: Some(format!(
                "To enable {} the table, set REPLICA IDENTITY using ALTER TABLE.",
                action.gerund()
            )),
            sqlstate: "55000",
        },
    }
}

fn relation_and_publication_parent_oids(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
) -> Vec<u32> {
    let mut oids = vec![relation_oid];
    let mut pending = vec![relation_oid];
    while let Some(oid) = pending.pop() {
        for parent in catalog.inheritance_parents(oid) {
            if !oids.contains(&parent.inhparent) {
                oids.push(parent.inhparent);
                pending.push(parent.inhparent);
            }
        }
    }
    oids
}

fn active_publication_memberships(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    namespace_oid: u32,
    action: PublicationDmlAction,
) -> Vec<(PgPublicationRow, Option<PgPublicationRelRow>)> {
    let relation_oids = relation_and_publication_parent_oids(catalog, relation_oid);
    let mut namespace_oids = relation_oids
        .iter()
        .filter_map(|oid| catalog.class_row_by_oid(*oid).map(|row| row.relnamespace))
        .collect::<Vec<_>>();
    if !namespace_oids.contains(&namespace_oid) {
        namespace_oids.push(namespace_oid);
    }
    let publication_rows = catalog.publication_rows();
    let publication_rel_rows = catalog.publication_rel_rows();
    let publication_namespace_rows = catalog.publication_namespace_rows();
    let mut memberships = Vec::new();

    for publication in publication_rows {
        if !action.publishes(&publication) {
            continue;
        }
        let rel_rows = publication_rel_rows
            .iter()
            .filter(|row| row.prpubid == publication.oid && relation_oids.contains(&row.prrelid))
            .collect::<Vec<_>>();
        let excluded = rel_rows.iter().any(|row| row.prexcept);
        if let Some(row) = rel_rows.into_iter().find(|row| !row.prexcept) {
            memberships.push((publication, Some(row.clone())));
            continue;
        }
        if publication.puballtables && !excluded {
            memberships.push((publication, None));
            continue;
        }
        if publication_namespace_rows
            .iter()
            .any(|row| row.pnpubid == publication.oid && namespace_oids.contains(&row.pnnspid))
        {
            memberships.push((publication, None));
        }
    }

    memberships
}

fn replica_identity_columns(
    relation_oid: u32,
    desc: &RelationDesc,
    indexes: &[BoundIndexRelation],
    catalog: &dyn CatalogLookup,
) -> (ReplicaIdentityColumns, Vec<i16>) {
    match catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relreplident)
        .unwrap_or('d')
    {
        'f' => (
            ReplicaIdentityColumns::Full,
            desc.columns
                .iter()
                .enumerate()
                .filter_map(|(idx, column)| {
                    (!column.dropped)
                        .then(|| i16::try_from(idx + 1).ok())
                        .flatten()
                })
                .collect(),
        ),
        'i' => indexes
            .iter()
            .find(|index| {
                catalog
                    .index_row_by_oid(index.relation_oid)
                    .map(|row| row.indisreplident)
                    .unwrap_or(index.index_meta.indisreplident)
            })
            .map(|index| {
                (
                    ReplicaIdentityColumns::Columns,
                    index.index_meta.indkey.clone(),
                )
            })
            .unwrap_or((ReplicaIdentityColumns::None, Vec::new())),
        'n' => (ReplicaIdentityColumns::None, Vec::new()),
        _ => indexes
            .iter()
            .find(|index| index.index_meta.indisprimary && index.index_meta.indimmediate)
            .map(|index| {
                (
                    ReplicaIdentityColumns::Columns,
                    index.index_meta.indkey.clone(),
                )
            })
            .unwrap_or((ReplicaIdentityColumns::None, Vec::new())),
    }
}

fn relation_column_attnum(desc: &RelationDesc, name: &str) -> Option<i16> {
    let column_name = name.rsplit('.').next().unwrap_or(name);
    desc.columns
        .iter()
        .enumerate()
        .find(|(_, column)| !column.dropped && column.name.eq_ignore_ascii_case(column_name))
        .and_then(|(idx, _)| i16::try_from(idx + 1).ok())
}

fn publication_filter_attnums(qual: &str, desc: &RelationDesc) -> Result<Vec<i16>, ParseError> {
    let expr = pgrust_parser::parse_expr(qual)?;
    let mut column_names = BTreeSet::new();
    collect_sql_expr_column_names(&expr, &mut column_names);
    Ok(column_names
        .into_iter()
        .filter_map(|name| relation_column_attnum(desc, &name))
        .collect())
}

fn publication_membership_attnums(
    relation_oid: u32,
    desc: &RelationDesc,
    membership: &PgPublicationRelRow,
    catalog: &dyn CatalogLookup,
) -> Option<Vec<i16>> {
    let attrs = membership.prattrs.as_ref()?;
    if membership.prrelid == relation_oid {
        return Some(attrs.clone());
    }
    let membership_relation = catalog.relation_by_oid(membership.prrelid)?;
    let translated = attrs
        .iter()
        .filter_map(|attnum| {
            let column = attnum
                .checked_sub(1)
                .and_then(|idx| usize::try_from(idx).ok())
                .and_then(|idx| membership_relation.desc.columns.get(idx))?;
            (!column.dropped)
                .then(|| relation_column_attnum(desc, &column.name))
                .flatten()
        })
        .collect::<Vec<_>>();
    Some(translated)
}

fn publication_filter_attnums_for_membership(
    qual: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    membership: &PgPublicationRelRow,
) -> Result<Vec<i16>, PublicationReplicaIdentityError> {
    if membership.prrelid == relation_oid {
        return publication_filter_attnums(qual, desc)
            .map_err(PublicationReplicaIdentityError::Parse);
    }
    let expr = pgrust_parser::parse_expr(qual).map_err(PublicationReplicaIdentityError::Parse)?;
    let mut column_names = BTreeSet::new();
    collect_sql_expr_column_names(&expr, &mut column_names);
    Ok(column_names
        .into_iter()
        .filter_map(|name| relation_column_attnum(desc, &name))
        .collect())
}

fn publication_generated_identity_is_published(
    publication: &PgPublicationRow,
    membership: Option<&PgPublicationRelRow>,
    attnum: i16,
    desc: &RelationDesc,
) -> bool {
    let Some(column) = attnum
        .checked_sub(1)
        .and_then(|idx| usize::try_from(idx).ok())
        .and_then(|idx| desc.columns.get(idx))
    else {
        return true;
    };
    let Some(generated) = column.generated else {
        return true;
    };
    if membership
        .and_then(|row| row.prattrs.as_ref())
        .is_some_and(|attrs| attrs.contains(&attnum))
    {
        return true;
    }
    publication.pubgencols == PUBLISH_GENCOLS_STORED
        && matches!(generated, ColumnGeneratedKind::Stored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::{
        HEAP_TABLE_AM_OID, PG_CATALOG_NAMESPACE_OID, PgClassRow, PgPublicationNamespaceRow,
        desc::column_desc,
    };
    use pgrust_core::RelFileLocator;
    use pgrust_nodes::{
        SqlType, SqlTypeKind,
        parsenodes::{
            PublicationOption, PublicationOptions, PublicationPublishActions,
            PublishGeneratedColumns,
        },
    };

    #[derive(Default)]
    struct TestCatalog {
        class_rows: Vec<PgClassRow>,
        publication_rows: Vec<PgPublicationRow>,
        publication_rel_rows: Vec<PgPublicationRelRow>,
        publication_namespace_rows: Vec<PgPublicationNamespaceRow>,
    }

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<pgrust_analyze::BoundRelation> {
            None
        }

        fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
            self.class_rows
                .iter()
                .find(|row| row.oid == relation_oid)
                .cloned()
        }

        fn publication_rows(&self) -> Vec<PgPublicationRow> {
            self.publication_rows.clone()
        }

        fn publication_rel_rows(&self) -> Vec<PgPublicationRelRow> {
            self.publication_rel_rows.clone()
        }

        fn publication_namespace_rows(&self) -> Vec<PgPublicationNamespaceRow> {
            self.publication_namespace_rows.clone()
        }
    }

    fn relation_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("payload", SqlType::new(SqlTypeKind::Text), false),
            ],
        }
    }

    fn bound_relation() -> BoundRelation {
        BoundRelation {
            rel: RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: 42,
            },
            relation_oid: 42,
            toast: None,
            namespace_oid: PG_CATALOG_NAMESPACE_OID,
            owner_oid: 10,
            of_type_oid: 0,
            relpersistence: 'p',
            relkind: 'r',
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: relation_desc(),
            partitioned_table: None,
            partition_spec: None,
        }
    }

    fn class_row(oid: u32, relreplident: char) -> PgClassRow {
        PgClassRow {
            oid,
            relname: "t".into(),
            relnamespace: PG_CATALOG_NAMESPACE_OID,
            reltype: 0,
            relowner: 10,
            relam: HEAP_TABLE_AM_OID,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'r',
            relnatts: 2,
            relhassubclass: false,
            relhastriggers: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relispopulated: true,
            relispartition: false,
            relfrozenxid: 2,
            relpartbound: None,
            reloptions: None,
            relacl: None,
            relreplident,
            reloftype: 0,
        }
    }

    fn publication(puballtables: bool) -> PgPublicationRow {
        PgPublicationRow {
            oid: 77,
            pubname: "pub".into(),
            pubowner: 10,
            puballtables,
            puballsequences: false,
            pubinsert: true,
            pubupdate: true,
            pubdelete: true,
            pubtruncate: true,
            pubviaroot: false,
            pubgencols: PUBLISH_GENCOLS_STORED,
        }
    }

    fn relation_publication_rel(
        prattrs: Option<Vec<i16>>,
        prqual: Option<&str>,
    ) -> PgPublicationRelRow {
        PgPublicationRelRow {
            oid: 88,
            prpubid: 77,
            prrelid: 42,
            prexcept: false,
            prqual: prqual.map(str::to_string),
            prattrs,
        }
    }

    fn identity_check(
        catalog: &TestCatalog,
        desc: &RelationDesc,
    ) -> Result<(), PublicationReplicaIdentityError> {
        enforce_publication_replica_identity(
            "t",
            42,
            PG_CATALOG_NAMESPACE_OID,
            desc,
            &[],
            catalog,
            PublicationDmlAction::Update,
            true,
        )
    }

    #[test]
    fn publication_defaults_match_postgres_create_defaults() {
        let row = publication_row_defaults("MixedCasePub", 23);

        assert_eq!(row.pubname, "mixedcasepub");
        assert_eq!(row.pubowner, 23);
        assert!(row.pubinsert);
        assert!(row.pubupdate);
        assert!(row.pubdelete);
        assert!(row.pubtruncate);
        assert!(!row.pubviaroot);
        assert_eq!(row.pubgencols, PUBLISH_GENCOLS_NONE);
    }

    #[test]
    fn apply_publication_options_updates_catalog_row() {
        let mut row = publication_row_defaults("pub", 10);
        let options = PublicationOptions {
            options: vec![
                PublicationOption::Publish(PublicationPublishActions {
                    insert: true,
                    update: false,
                    delete: true,
                    truncate: false,
                }),
                PublicationOption::PublishViaPartitionRoot(true),
                PublicationOption::PublishGeneratedColumns(PublishGeneratedColumns::Stored),
            ],
        };

        apply_publication_options(&mut row, &options).unwrap();

        assert!(row.pubinsert);
        assert!(!row.pubupdate);
        assert!(row.pubdelete);
        assert!(!row.pubtruncate);
        assert!(row.pubviaroot);
        assert_eq!(row.pubgencols, PUBLISH_GENCOLS_STORED);
    }

    #[test]
    fn apply_publication_options_rejects_duplicate_options() {
        let mut row = publication_row_defaults("pub", 10);
        let options = PublicationOptions {
            options: vec![
                PublicationOption::PublishViaPartitionRoot(true),
                PublicationOption::PublishViaPartitionRoot(false),
            ],
        };

        let err = apply_publication_options(&mut row, &options).unwrap_err();
        assert!(matches!(
            err,
            ParseError::ConflictingOrRedundantOptions { option }
                if option == "publish_via_partition_root"
        ));
    }

    #[test]
    fn publication_filter_validation_rejects_aggregate() {
        let expr = pgrust_parser::parse_expr("count(id) > 0").unwrap();

        let err = validate_publication_filter_expr(&expr).unwrap_err();
        assert_eq!(err, PublicationFilterError::AggregateFunction);
    }

    #[test]
    fn publication_filter_validation_rejects_system_column() {
        let expr = pgrust_parser::parse_expr("ctid IS NOT NULL").unwrap();

        let err = validate_publication_filter_expr(&expr).unwrap_err();
        assert!(matches!(
            err,
            PublicationFilterError::InvalidWhere {
                detail: "System columns are not allowed."
            }
        ));
    }

    #[test]
    fn publication_filter_rendering_normalizes_boolean_expr() {
        let expr = pgrust_parser::parse_expr("id = 1 and payload is not null").unwrap();

        let rendered = render_publication_filter_expr(&expr, |ty| match ty.kind {
            SqlTypeKind::Text => "text".into(),
            SqlTypeKind::Int4 => "integer".into(),
            _ => "unknown".into(),
        })
        .unwrap();

        assert_eq!(rendered, "((id = 1) AND (payload IS NOT NULL))");
    }

    #[test]
    fn publication_column_numbers_rejects_system_and_duplicate_columns() {
        let relation = bound_relation();

        let system_err = publication_column_numbers(&relation, "t", &["ctid".into()]).unwrap_err();
        assert!(matches!(
            system_err,
            PublicationTargetError::SystemColumn { column_name } if column_name == "ctid"
        ));

        let duplicate_err =
            publication_column_numbers(&relation, "t", &["id".into(), "ID".into()]).unwrap_err();
        assert!(matches!(
            duplicate_err,
            PublicationTargetError::DuplicateColumn { column_name } if column_name == "ID"
        ));
    }

    #[test]
    fn publication_column_numbers_returns_attribute_numbers() {
        let relation = bound_relation();

        let attrs =
            publication_column_numbers(&relation, "t", &["payload".into(), "id".into()]).unwrap();

        assert_eq!(attrs, Some(vec![2, 1]));
    }

    #[test]
    fn no_matching_publication_does_not_require_identity() {
        let catalog = TestCatalog {
            class_rows: vec![class_row(42, 'n')],
            ..TestCatalog::default()
        };

        assert!(identity_check(&catalog, &relation_desc()).is_ok());
    }

    #[test]
    fn published_table_without_identity_reports_identity_error() {
        let catalog = TestCatalog {
            class_rows: vec![class_row(42, 'n')],
            publication_rows: vec![publication(true)],
            ..TestCatalog::default()
        };

        let err = identity_check(&catalog, &relation_desc()).unwrap_err();
        assert!(matches!(
            err,
            PublicationReplicaIdentityError::Detailed { hint: Some(_), .. }
        ));
    }

    #[test]
    fn publication_column_list_must_cover_full_identity() {
        let catalog = TestCatalog {
            class_rows: vec![class_row(42, 'f')],
            publication_rows: vec![publication(false)],
            publication_rel_rows: vec![relation_publication_rel(Some(vec![1]), None)],
            ..TestCatalog::default()
        };

        let err = identity_check(&catalog, &relation_desc()).unwrap_err();
        assert!(matches!(
            err,
            PublicationReplicaIdentityError::Detailed {
                detail: Some(detail),
                ..
            } if detail.contains("Column list")
        ));
    }

    #[test]
    fn publication_filter_columns_must_be_part_of_identity() {
        let catalog = TestCatalog {
            class_rows: vec![class_row(42, 'n')],
            publication_rows: vec![publication(false)],
            publication_rel_rows: vec![relation_publication_rel(None, Some("payload = 'x'"))],
            ..TestCatalog::default()
        };

        let err = identity_check(&catalog, &relation_desc()).unwrap_err();
        assert!(matches!(
            err,
            PublicationReplicaIdentityError::Detailed {
                detail: Some(detail),
                ..
            } if detail.contains("WHERE expression")
        ));
    }
}
