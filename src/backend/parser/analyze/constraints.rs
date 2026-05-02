use std::collections::{BTreeMap, BTreeSet};

use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlExpr, SqlType, SqlTypeKind, parse_expr};
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{PgConstraintRow, bootstrap_pg_cast_rows};
use crate::include::nodes::parsenodes::{
    ColumnConstraint, ConstraintAttributes, CreateTableStatement, ForeignKeyAction,
    ForeignKeyMatchType, IndexColumnDef, TableConstraint, TablePersistence,
};
use crate::include::nodes::primnodes::{
    CMAX_ATTR_NO, CMIN_ATTR_NO, Expr, SELF_ITEM_POINTER_ATTR_NO, TABLE_OID_ATTR_NO, XMAX_ATTR_NO,
    XMIN_ATTR_NO,
};

use super::ParseError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBackedConstraintAction {
    pub constraint_name: Option<String>,
    pub existing_index_name: Option<String>,
    pub columns: Vec<String>,
    pub index_columns: Vec<IndexColumnDef>,
    pub include_columns: Vec<String>,
    pub primary: bool,
    pub exclusion: bool,
    pub nulls_not_distinct: bool,
    pub without_overlaps: Option<String>,
    pub tablespace: Option<String>,
    pub access_method: Option<String>,
    pub exclusion_operators: Vec<String>,
    pub predicate_sql: Option<String>,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotNullConstraintAction {
    pub constraint_name: String,
    pub column: String,
    pub not_valid: bool,
    pub no_inherit: bool,
    pub primary_key_owned: bool,
    pub is_local: bool,
    pub inhcount: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintAction {
    pub constraint_name: String,
    pub expr_sql: String,
    pub not_valid: bool,
    pub no_inherit: bool,
    pub enforced: bool,
    pub parent_constraint_oid: Option<u32>,
    pub is_local: bool,
    pub inhcount: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyConstraintAction {
    pub constraint_name: String,
    pub columns: Vec<String>,
    pub period: Option<String>,
    pub referenced_table: String,
    pub referenced_relation_oid: u32,
    pub referenced_index_oid: u32,
    // CREATE TABLE self-references are resolved after the table and its key
    // indexes exist, matching PostgreSQL's post-create FK installation order.
    pub self_referential: bool,
    pub referenced_columns: Vec<String>,
    pub referenced_period: Option<String>,
    pub match_type: ForeignKeyMatchType,
    pub on_delete: ForeignKeyAction,
    pub on_delete_set_columns: Option<Vec<String>>,
    pub on_update: ForeignKeyAction,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub not_valid: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BoundRelationConstraints {
    pub relation_oid: Option<u32>,
    pub not_nulls: Vec<BoundNotNullConstraint>,
    pub checks: Vec<BoundCheckConstraint>,
    pub foreign_keys: Vec<BoundForeignKeyConstraint>,
    pub temporal: Vec<BoundTemporalConstraint>,
    pub exclusions: Vec<BoundExclusionConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundNotNullConstraint {
    pub column_index: usize,
    pub constraint_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundCheckConstraint {
    pub constraint_name: String,
    pub expr: Expr,
    pub enforced: bool,
    pub validated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundForeignKeyConstraint {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub relation_name: String,
    pub column_names: Vec<String>,
    pub column_indexes: Vec<usize>,
    pub period_column_index: Option<usize>,
    pub match_type: ForeignKeyMatchType,
    pub referenced_relation_name: String,
    pub referenced_relation_oid: u32,
    pub referenced_rel: crate::backend::storage::smgr::RelFileLocator,
    pub referenced_toast: Option<crate::include::nodes::execnodes::ToastRelationRef>,
    pub referenced_desc: RelationDesc,
    pub referenced_column_indexes: Vec<usize>,
    pub referenced_period_column_index: Option<usize>,
    pub referenced_index: super::BoundIndexRelation,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundTemporalConstraint {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub column_names: Vec<String>,
    pub column_indexes: Vec<usize>,
    pub period_column_index: usize,
    pub primary: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundExclusionConstraint {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub relation_oid: u32,
    pub column_names: Vec<String>,
    pub column_indexes: Vec<usize>,
    pub key_columns: Vec<Option<usize>>,
    pub key_exprs: Vec<Option<Expr>>,
    pub operator_oids: Vec<u32>,
    pub operator_proc_oids: Vec<u32>,
    pub predicate: Option<Expr>,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundReferencedByForeignKey {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub display_constraint_name: String,
    pub referenced_relation_oid: u32,
    pub child_relation_name: String,
    pub display_child_relation_name: String,
    pub child_relation_oid: u32,
    pub child_rel: crate::backend::storage::smgr::RelFileLocator,
    pub child_toast: Option<crate::include::nodes::execnodes::ToastRelationRef>,
    pub child_desc: RelationDesc,
    pub child_column_indexes: Vec<usize>,
    pub child_period_column_index: Option<usize>,
    pub referenced_rel: crate::backend::storage::smgr::RelFileLocator,
    pub referenced_toast: Option<crate::include::nodes::execnodes::ToastRelationRef>,
    pub referenced_desc: RelationDesc,
    pub referenced_column_names: Vec<String>,
    pub referenced_column_indexes: Vec<usize>,
    pub referenced_period_column_index: Option<usize>,
    pub child_index: Option<super::BoundIndexRelation>,
    pub on_delete: ForeignKeyAction,
    pub on_delete_set_column_indexes: Option<Vec<usize>>,
    pub on_update: ForeignKeyAction,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedCreateTableConstraints {
    pub not_nulls: Vec<NotNullConstraintAction>,
    pub checks: Vec<CheckConstraintAction>,
    pub index_backed: Vec<IndexBackedConstraintAction>,
    pub foreign_keys: Vec<ForeignKeyConstraintAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NormalizedAddColumnConstraints {
    pub not_null: Option<NotNullConstraintAction>,
    pub checks: Vec<CheckConstraintAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedAlterTableConstraint {
    Noop,
    NotNull(NotNullConstraintAction),
    Check(CheckConstraintAction),
    IndexBacked(IndexBackedConstraintAction),
    ForeignKey(ForeignKeyConstraintAction),
}

#[derive(Debug, Clone)]
struct PendingIndexConstraint {
    explicit_name: Option<String>,
    existing_index_name: Option<String>,
    generated_base: GeneratedConstraintName,
    columns: Vec<String>,
    index_columns: Vec<IndexColumnDef>,
    include_columns: Vec<String>,
    primary: bool,
    exclusion: bool,
    nulls_not_distinct: bool,
    without_overlaps: Option<String>,
    tablespace: Option<String>,
    access_method: Option<String>,
    exclusion_operators: Vec<String>,
    predicate_sql: Option<String>,
    deferrable: bool,
    initially_deferred: bool,
}

fn index_columns_from_names(columns: &[String]) -> Vec<IndexColumnDef> {
    columns.iter().cloned().map(IndexColumnDef::from).collect()
}

fn exclusion_index_columns(
    elements: &[crate::include::nodes::parsenodes::ExclusionElement],
    resolved_columns: &[String],
) -> Vec<IndexColumnDef> {
    let mut resolved_iter = resolved_columns.iter();
    elements
        .iter()
        .map(|element| {
            if let Some(expr_sql) = element.expr_sql.as_ref() {
                IndexColumnDef {
                    name: String::new(),
                    expr_sql: Some(expr_sql.clone()),
                    expr_type: None,
                    collation: None,
                    opclass: None,
                    opclass_options: Vec::new(),
                    descending: false,
                    nulls_first: None,
                }
            } else {
                resolved_iter
                    .next()
                    .cloned()
                    .map(IndexColumnDef::from)
                    .unwrap_or_else(|| IndexColumnDef::from(element.column.clone()))
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct PendingCheckConstraint {
    explicit_name: Option<String>,
    generated_base: GeneratedConstraintName,
    expr_sql: String,
    not_valid: bool,
    no_inherit: bool,
    enforced: bool,
}

#[derive(Debug, Clone)]
struct PendingNotNullConstraint {
    explicit_name: Option<String>,
    generated_base: GeneratedConstraintName,
    column: String,
    not_valid: bool,
    no_inherit: bool,
    primary_key_owned: bool,
    column_index: usize,
}

#[derive(Debug, Clone)]
struct PendingForeignKeyConstraint {
    explicit_name: Option<String>,
    generated_base: GeneratedConstraintName,
    columns: Vec<String>,
    period: Option<String>,
    referenced_table: String,
    referenced_columns: Option<Vec<String>>,
    referenced_period: Option<String>,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<Vec<String>>,
    on_update: ForeignKeyAction,
    deferrable: bool,
    initially_deferred: bool,
    not_valid: bool,
    enforced: bool,
}

#[derive(Debug, Clone)]
struct GeneratedConstraintName {
    name1: String,
    name2: Option<String>,
    label: String,
}

impl GeneratedConstraintName {
    fn new(name1: &str, name2: Option<String>, label: &str) -> Self {
        Self {
            name1: name1.to_string(),
            name2,
            label: label.to_string(),
        }
    }
}

fn check_constraint_name_column(expr_sql: &str, relation_columns: &[String]) -> Option<String> {
    let expr = parse_expr(expr_sql).ok()?;
    let relation_column_names = relation_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut user_columns = BTreeSet::new();
    let mut system_columns = BTreeSet::new();
    collect_check_expr_column_names(
        &expr,
        &relation_column_names,
        &mut user_columns,
        &mut system_columns,
    );
    (user_columns.len() == 1 && system_columns.is_empty())
        .then(|| user_columns.into_iter().next().expect("one column"))
}

fn collect_check_expr_column_names(
    expr: &SqlExpr,
    relation_columns: &BTreeSet<String>,
    user_columns: &mut BTreeSet<String>,
    system_columns: &mut BTreeSet<String>,
) {
    match expr {
        SqlExpr::Column(name) => {
            let column_name = name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase();
            if relation_columns.contains(&column_name) {
                user_columns.insert(column_name);
            } else if is_system_column_name_for_check(&column_name) {
                system_columns.insert(column_name);
            }
        }
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right) => {
            collect_check_expr_column_names(left, relation_columns, user_columns, system_columns);
            collect_check_expr_column_names(right, relation_columns, user_columns, system_columns);
        }
        SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::GeometryBinaryOp { left, right, .. } => {
            collect_check_expr_column_names(left, relation_columns, user_columns, system_columns);
            collect_check_expr_column_names(right, relation_columns, user_columns, system_columns);
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Cast(inner, _)
        | SqlExpr::Collate { expr: inner, .. }
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::FieldSelect { expr: inner, .. }
        | SqlExpr::GeometryUnaryOp { expr: inner, .. } => {
            collect_check_expr_column_names(inner, relation_columns, user_columns, system_columns);
        }
        SqlExpr::Subscript { expr, .. } => {
            collect_check_expr_column_names(expr, relation_columns, user_columns, system_columns);
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            collect_check_expr_column_names(expr, relation_columns, user_columns, system_columns);
            collect_check_expr_column_names(zone, relation_columns, user_columns, system_columns);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_check_expr_column_names(expr, relation_columns, user_columns, system_columns);
            collect_check_expr_column_names(
                pattern,
                relation_columns,
                user_columns,
                system_columns,
            );
            if let Some(escape) = escape {
                collect_check_expr_column_names(
                    escape,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                collect_check_expr_column_names(
                    arg,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
            for when in args {
                collect_check_expr_column_names(
                    &when.expr,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
                collect_check_expr_column_names(
                    &when.result,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
            if let Some(defresult) = defresult {
                collect_check_expr_column_names(
                    defresult,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            for element in elements {
                collect_check_expr_column_names(
                    element,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            collect_check_expr_column_names(array, relation_columns, user_columns, system_columns);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_check_expr_column_names(
                        lower,
                        relation_columns,
                        user_columns,
                        system_columns,
                    );
                }
                if let Some(upper) = &subscript.upper {
                    collect_check_expr_column_names(
                        upper,
                        relation_columns,
                        user_columns,
                        system_columns,
                    );
                }
            }
        }
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_check_expr_column_names(
                    child,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
        }
        SqlExpr::JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_check_expr_column_names(
                    child,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
        }
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            for arg in args.args() {
                collect_check_expr_column_names(
                    &arg.value,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
            for item in order_by {
                collect_check_expr_column_names(
                    &item.expr,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
            if let Some(within_group) = within_group {
                for item in within_group {
                    collect_check_expr_column_names(
                        &item.expr,
                        relation_columns,
                        user_columns,
                        system_columns,
                    );
                }
            }
            if let Some(filter) = filter {
                collect_check_expr_column_names(
                    filter,
                    relation_columns,
                    user_columns,
                    system_columns,
                );
            }
        }
        SqlExpr::InSubquery { expr, .. }
        | SqlExpr::QuantifiedArray { left: expr, .. }
        | SqlExpr::QuantifiedSubquery { left: expr, .. } => {
            collect_check_expr_column_names(expr, relation_columns, user_columns, system_columns);
        }
        _ => {}
    }
}

fn is_system_column_name_for_check(name: &str) -> bool {
    matches!(
        name,
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    )
}

#[derive(Debug, Clone)]
struct ResolvedReferencedKey {
    relation: super::BoundRelation,
    columns: Vec<String>,
    period: Option<String>,
    index_oid: u32,
}

#[derive(Debug, Clone)]
struct ResolvedPendingReferencedKey {
    columns: Vec<String>,
    period: Option<String>,
}

fn table_persistence_code(persistence: TablePersistence) -> char {
    match persistence {
        TablePersistence::Permanent => 'p',
        TablePersistence::Unlogged => 'u',
        TablePersistence::Temporary => 't',
    }
}

fn validate_foreign_key_persistence(
    child_persistence: char,
    referenced_persistence: char,
) -> Result<(), ParseError> {
    match child_persistence {
        'p' if referenced_persistence != 'p' => Err(ParseError::InvalidTableDefinition(
            "constraints on permanent tables may reference only permanent tables".into(),
        )),
        't' if referenced_persistence != 't' => Err(ParseError::InvalidTableDefinition(
            "constraints on temporary tables may reference only temporary tables".into(),
        )),
        _ => Ok(()),
    }
}

pub fn normalize_create_table_constraints(
    stmt: &CreateTableStatement,
    catalog: &dyn super::CatalogLookup,
) -> Result<NormalizedCreateTableConstraints, ParseError> {
    let columns = stmt.columns().cloned().collect::<Vec<_>>();
    let relation_columns = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let column_lookup = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.to_ascii_lowercase(), index))
        .collect::<BTreeMap<_, _>>();

    let mut index_constraints = Vec::new();
    let mut check_constraints = Vec::new();
    let mut not_nulls = BTreeMap::<String, PendingNotNullConstraint>::new();
    let mut foreign_keys = Vec::new();

    for column in &columns {
        for constraint in &column.constraints {
            match constraint {
                ColumnConstraint::NotNull { attributes } => {
                    validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
                    merge_not_null_constraint(
                        &mut not_nulls,
                        &column_lookup,
                        &column.name,
                        attributes,
                        false,
                        stmt.table_name.as_str(),
                    )?;
                }
                ColumnConstraint::Check {
                    attributes,
                    expr_sql,
                } => {
                    validate_check_attributes(attributes)?;
                    check_constraints.push(PendingCheckConstraint {
                        explicit_name: attributes.name.clone(),
                        generated_base: GeneratedConstraintName::new(
                            &stmt.table_name,
                            check_constraint_name_column(expr_sql, &relation_columns),
                            "check",
                        ),
                        expr_sql: expr_sql.clone(),
                        not_valid: attributes.not_valid,
                        no_inherit: attributes.no_inherit,
                        enforced: attributes.enforced.unwrap_or(true),
                    });
                }
                ColumnConstraint::PrimaryKey {
                    attributes,
                    tablespace,
                } => {
                    let (deferrable, initially_deferred) =
                        validate_key_attributes(attributes, "PRIMARY KEY")?;
                    index_constraints.push(PendingIndexConstraint {
                        explicit_name: attributes.name.clone(),
                        existing_index_name: None,
                        generated_base: GeneratedConstraintName::new(
                            &stmt.table_name,
                            None,
                            "pkey",
                        ),
                        columns: vec![column.name.clone()],
                        index_columns: vec![IndexColumnDef::from(column.name.clone())],
                        include_columns: Vec::new(),
                        primary: true,
                        exclusion: false,
                        nulls_not_distinct: false,
                        without_overlaps: None,
                        tablespace: tablespace.clone(),
                        access_method: None,
                        exclusion_operators: Vec::new(),
                        predicate_sql: None,
                        deferrable,
                        initially_deferred,
                    });
                }
                ColumnConstraint::Unique {
                    attributes,
                    tablespace,
                } => {
                    let (deferrable, initially_deferred) =
                        validate_key_attributes(attributes, "UNIQUE")?;
                    index_constraints.push(PendingIndexConstraint {
                        explicit_name: attributes.name.clone(),
                        existing_index_name: None,
                        generated_base: GeneratedConstraintName::new(
                            &stmt.table_name,
                            Some(column.name.clone()),
                            "key",
                        ),
                        columns: vec![column.name.clone()],
                        index_columns: vec![IndexColumnDef::from(column.name.clone())],
                        include_columns: Vec::new(),
                        primary: false,
                        exclusion: false,
                        nulls_not_distinct: attributes.nulls_not_distinct,
                        without_overlaps: None,
                        tablespace: tablespace.clone(),
                        access_method: None,
                        exclusion_operators: Vec::new(),
                        predicate_sql: None,
                        deferrable,
                        initially_deferred,
                    });
                }
                ColumnConstraint::References {
                    attributes,
                    referenced_table,
                    referenced_columns,
                    match_type,
                    on_delete,
                    on_delete_set_columns,
                    on_update,
                } => {
                    validate_create_foreign_key(
                        attributes,
                        *match_type,
                        *on_delete,
                        on_delete_set_columns.as_deref(),
                        *on_update,
                    )?;
                    let (deferrable, initially_deferred) = foreign_key_deferrability(attributes);
                    foreign_keys.push(PendingForeignKeyConstraint {
                        explicit_name: attributes.name.clone(),
                        generated_base: GeneratedConstraintName::new(
                            &stmt.table_name,
                            Some(column.name.clone()),
                            "fkey",
                        ),
                        columns: vec![column.name.clone()],
                        period: None,
                        referenced_table: referenced_table.clone(),
                        referenced_columns: referenced_columns.clone(),
                        referenced_period: None,
                        match_type: *match_type,
                        on_delete: *on_delete,
                        on_delete_set_columns: on_delete_set_columns.clone(),
                        on_update: *on_update,
                        deferrable,
                        initially_deferred,
                        not_valid: attributes.not_valid,
                        enforced: attributes.enforced.unwrap_or(true),
                    });
                }
            }
        }
    }

    for constraint in stmt.constraints() {
        match constraint {
            TableConstraint::NotNull { attributes, column } => {
                validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
                merge_not_null_constraint(
                    &mut not_nulls,
                    &column_lookup,
                    column,
                    attributes,
                    false,
                    stmt.table_name.as_str(),
                )?;
            }
            TableConstraint::Check {
                attributes,
                expr_sql,
            } => {
                validate_check_attributes(attributes)?;
                check_constraints.push(PendingCheckConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: GeneratedConstraintName::new(
                        &stmt.table_name,
                        check_constraint_name_column(expr_sql, &relation_columns),
                        "check",
                    ),
                    expr_sql: expr_sql.clone(),
                    not_valid: attributes.not_valid,
                    no_inherit: attributes.no_inherit,
                    enforced: attributes.enforced.unwrap_or(true),
                });
            }
            TableConstraint::PrimaryKey {
                attributes,
                columns: key_columns,
                include_columns,
                without_overlaps,
                tablespace,
            } => {
                let (deferrable, initially_deferred) =
                    validate_key_attributes(attributes, "PRIMARY KEY")?;
                let resolved = resolve_index_constraint_columns(
                    key_columns,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                )?;
                validate_without_overlaps_column(
                    &resolved,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                    catalog,
                )?;
                let resolved_include =
                    resolve_constraint_columns(include_columns, &columns, &column_lookup)?;
                let index_columns = index_columns_from_names(&resolved);
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    existing_index_name: None,
                    generated_base: GeneratedConstraintName::new(&stmt.table_name, None, "pkey"),
                    columns: resolved,
                    index_columns,
                    include_columns: resolved_include,
                    primary: true,
                    exclusion: false,
                    nulls_not_distinct: false,
                    without_overlaps: without_overlaps.clone(),
                    tablespace: tablespace.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    predicate_sql: None,
                    deferrable,
                    initially_deferred,
                });
            }
            TableConstraint::Unique {
                attributes,
                columns: key_columns,
                include_columns,
                without_overlaps,
                tablespace,
            } => {
                let (deferrable, initially_deferred) =
                    validate_key_attributes(attributes, "UNIQUE")?;
                let resolved = resolve_index_constraint_columns(
                    key_columns,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                )?;
                validate_without_overlaps_column(
                    &resolved,
                    without_overlaps.as_deref(),
                    &columns,
                    &column_lookup,
                    catalog,
                )?;
                let resolved_include =
                    resolve_constraint_columns(include_columns, &columns, &column_lookup)?;
                let generated_columns = resolved
                    .iter()
                    .chain(resolved_include.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                let index_columns = index_columns_from_names(&resolved);
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    existing_index_name: None,
                    generated_base: GeneratedConstraintName::new(
                        &stmt.table_name,
                        Some(generated_columns.join("_")),
                        "key",
                    ),
                    columns: resolved,
                    index_columns,
                    include_columns: resolved_include,
                    primary: false,
                    exclusion: false,
                    nulls_not_distinct: attributes.nulls_not_distinct,
                    without_overlaps: without_overlaps.clone(),
                    tablespace: tablespace.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    predicate_sql: None,
                    deferrable,
                    initially_deferred,
                });
            }
            TableConstraint::Exclusion {
                attributes,
                using_method,
                elements,
                include_columns,
                predicate_sql,
            } => {
                let (deferrable, initially_deferred) =
                    validate_key_attributes(attributes, "EXCLUDE")?;
                let key_columns = elements
                    .iter()
                    .filter(|element| element.expr_sql.is_none())
                    .map(|element| element.column.clone())
                    .collect::<Vec<_>>();
                let resolved = resolve_constraint_columns(&key_columns, &columns, &column_lookup)?;
                let resolved_include =
                    resolve_constraint_columns(include_columns, &columns, &column_lookup)?;
                let index_columns = exclusion_index_columns(elements, &resolved);
                let relation_columns = columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                let generated_columns = index_columns
                    .iter()
                    .map(|column| {
                        column.expr_sql.as_deref().map_or_else(
                            || column.name.clone(),
                            |expr_sql| {
                                check_constraint_name_column(expr_sql, &relation_columns)
                                    .unwrap_or_else(|| "expr".into())
                            },
                        )
                    })
                    .chain(resolved_include.iter().cloned())
                    .collect::<Vec<_>>();
                index_constraints.push(PendingIndexConstraint {
                    explicit_name: attributes.name.clone(),
                    existing_index_name: None,
                    generated_base: GeneratedConstraintName::new(
                        &stmt.table_name,
                        Some(generated_columns.join("_")),
                        "excl",
                    ),
                    columns: resolved,
                    index_columns,
                    include_columns: resolved_include,
                    primary: false,
                    nulls_not_distinct: false,
                    without_overlaps: None,
                    tablespace: None,
                    exclusion: true,
                    access_method: Some(using_method.clone()),
                    exclusion_operators: elements
                        .iter()
                        .map(|element| element.operator.clone())
                        .collect(),
                    predicate_sql: predicate_sql.clone(),
                    deferrable,
                    initially_deferred,
                });
            }
            TableConstraint::PrimaryKeyUsingIndex { .. }
            | TableConstraint::UniqueUsingIndex { .. } => {
                return Err(ParseError::UnexpectedToken {
                    expected: "CREATE TABLE constraint",
                    actual: "USING INDEX constraint".into(),
                });
            }
            TableConstraint::ForeignKey {
                attributes,
                columns: key_columns,
                period,
                referenced_table,
                referenced_columns,
                referenced_period,
                match_type,
                on_delete,
                on_delete_set_columns,
                on_update,
            } => {
                validate_create_foreign_key(
                    attributes,
                    *match_type,
                    *on_delete,
                    on_delete_set_columns.as_deref(),
                    *on_update,
                )?;
                let (deferrable, initially_deferred) = foreign_key_deferrability(attributes);
                let resolved = resolve_foreign_key_columns(
                    key_columns,
                    period.as_deref(),
                    &columns,
                    &column_lookup,
                )?;
                foreign_keys.push(PendingForeignKeyConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: GeneratedConstraintName::new(
                        &stmt.table_name,
                        Some(resolved.join("_")),
                        "fkey",
                    ),
                    columns: resolved,
                    period: period.clone(),
                    referenced_table: referenced_table.clone(),
                    referenced_columns: referenced_columns.clone(),
                    referenced_period: referenced_period.clone(),
                    match_type: *match_type,
                    on_delete: *on_delete,
                    on_delete_set_columns: on_delete_set_columns.clone(),
                    on_update: *on_update,
                    deferrable,
                    initially_deferred,
                    not_valid: attributes.not_valid,
                    enforced: attributes.enforced.unwrap_or(true),
                });
            }
        }
    }

    if index_constraints
        .iter()
        .filter(|constraint| constraint.primary)
        .count()
        > 1
    {
        return Err(multiple_primary_keys_error(&stmt.table_name));
    }

    for primary in index_constraints
        .iter()
        .filter(|constraint| constraint.primary)
    {
        for column in &primary.columns {
            merge_not_null_constraint(
                &mut not_nulls,
                &column_lookup,
                column,
                &ConstraintAttributes::default(),
                true,
                stmt.table_name.as_str(),
            )?;
        }
    }

    let mut used_names = BTreeSet::new();
    reserve_explicit_constraint_names(
        &mut used_names,
        not_nulls
            .values()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        check_constraints
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        index_constraints
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        foreign_keys
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;

    let mut finalized_not_nulls = not_nulls.into_values().collect::<Vec<_>>();
    finalized_not_nulls.sort_by_key(|constraint| constraint.column_index);
    let finalized_not_nulls = finalized_not_nulls
        .into_iter()
        .map(|constraint| NotNullConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            column: constraint.column,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
            primary_key_owned: constraint.primary_key_owned,
            is_local: true,
            inhcount: 0,
        })
        .collect();

    let finalized_checks = check_constraints
        .into_iter()
        .map(|constraint| CheckConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            expr_sql: constraint.expr_sql,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
            enforced: constraint.enforced,
            parent_constraint_oid: None,
            is_local: true,
            inhcount: 0,
        })
        .collect();

    let finalized_index_backed: Vec<IndexBackedConstraintAction> = index_constraints
        .into_iter()
        .map(|constraint| IndexBackedConstraintAction {
            constraint_name: Some(constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            })),
            existing_index_name: constraint.existing_index_name,
            columns: constraint.columns,
            index_columns: constraint.index_columns,
            include_columns: constraint.include_columns,
            primary: constraint.primary,
            exclusion: constraint.exclusion,
            nulls_not_distinct: constraint.nulls_not_distinct,
            without_overlaps: constraint.without_overlaps,
            tablespace: constraint.tablespace,
            access_method: constraint.access_method,
            exclusion_operators: constraint.exclusion_operators,
            predicate_sql: constraint.predicate_sql,
            deferrable: constraint.deferrable,
            initially_deferred: constraint.initially_deferred,
        })
        .collect();

    let finalized_foreign_keys = foreign_keys
        .into_iter()
        .map(|constraint| {
            let local_columns = constraint.columns.clone();
            let child_types = local_columns
                .iter()
                .map(|column| {
                    let index = *column_lookup
                        .get(&column.to_ascii_lowercase())
                        .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
                    resolve_create_table_column_type_for_foreign_key(&columns[index].ty, catalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (
                referenced_table,
                referenced_relation_oid,
                referenced_index_oid,
                self_referential,
                referenced_columns,
                referenced_period,
                constraint_name,
            ) = if constraint
                .referenced_table
                .eq_ignore_ascii_case(&stmt.table_name)
            {
                let constraint_name = constraint.explicit_name.clone().unwrap_or_else(|| {
                    choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
                });
                let referenced = resolve_pending_self_referenced_key(
                    &stmt.table_name,
                    &columns,
                    &column_lookup,
                    &finalized_index_backed,
                    constraint.referenced_columns.as_deref(),
                    constraint.period.as_deref(),
                    constraint.referenced_period.as_deref(),
                    &constraint_name,
                    &local_columns,
                    &child_types,
                    catalog,
                )?;
                (
                    stmt.table_name.clone(),
                    0,
                    0,
                    true,
                    referenced.columns,
                    referenced.period,
                    constraint_name,
                )
            } else {
                let constraint_name = constraint.explicit_name.clone().unwrap_or_else(|| {
                    choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
                });
                let referenced = resolve_referenced_key(
                    &stmt.table_name,
                    None,
                    table_persistence_code(stmt.persistence),
                    &constraint.referenced_table,
                    constraint.referenced_columns.as_deref(),
                    constraint.period.as_deref(),
                    constraint.referenced_period.as_deref(),
                    &constraint_name,
                    &local_columns,
                    &child_types,
                    catalog,
                )?;
                (
                    relation_display_name(
                        catalog,
                        referenced.relation.relation_oid,
                        &constraint.referenced_table,
                    ),
                    referenced.relation.relation_oid,
                    referenced.index_oid,
                    false,
                    referenced.columns,
                    referenced.period,
                    constraint_name,
                )
            };
            validate_foreign_key_period_actions(
                constraint.period.is_some() || referenced_period.is_some(),
                constraint.on_delete,
                constraint.on_update,
            )?;
            Ok(ForeignKeyConstraintAction {
                constraint_name,
                columns: local_columns,
                period: constraint.period,
                referenced_table,
                referenced_relation_oid,
                referenced_index_oid,
                self_referential,
                referenced_columns,
                referenced_period,
                match_type: constraint.match_type,
                on_delete: constraint.on_delete,
                on_delete_set_columns: resolve_foreign_key_delete_set_columns(
                    constraint.on_delete_set_columns.as_deref(),
                    &relation_columns,
                    &constraint.columns,
                )?,
                on_update: constraint.on_update,
                deferrable: constraint.deferrable,
                initially_deferred: constraint.initially_deferred,
                not_valid: constraint.not_valid,
                enforced: constraint.enforced,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;

    Ok(NormalizedCreateTableConstraints {
        not_nulls: finalized_not_nulls,
        checks: finalized_checks,
        index_backed: finalized_index_backed,
        foreign_keys: finalized_foreign_keys,
    })
}

pub fn bind_relation_constraints(
    relation_name: Option<&str>,
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundRelationConstraints, ParseError> {
    let rows = catalog.constraint_rows_for_relation(relation_oid);
    let mut not_nulls = Vec::new();
    let mut checks = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut temporal = Vec::new();
    let mut exclusions = Vec::new();

    for row in rows {
        match row.contype {
            crate::include::catalog::CONSTRAINT_NOTNULL => {
                let Some(attnum) = row
                    .conkey
                    .as_ref()
                    .and_then(|conkey| conkey.first())
                    .copied()
                else {
                    continue;
                };
                not_nulls.push(BoundNotNullConstraint {
                    column_index: attnum.saturating_sub(1) as usize,
                    constraint_name: row.conname,
                });
            }
            crate::include::catalog::CONSTRAINT_CHECK => {
                let expr_sql = row.conbin.ok_or(ParseError::UnexpectedToken {
                    expected: "stored CHECK constraint expression",
                    actual: format!("missing expression for constraint {}", row.conname),
                })?;
                checks.push(BoundCheckConstraint {
                    constraint_name: row.conname,
                    expr: bind_check_constraint_expr(&expr_sql, relation_name, desc, catalog)?,
                    enforced: row.conenforced,
                    validated: row.convalidated,
                });
            }
            crate::include::catalog::CONSTRAINT_FOREIGN => {
                if is_referenced_side_foreign_key_clone(&row, catalog) {
                    continue;
                }
                foreign_keys.push(bind_outbound_foreign_key_constraint(
                    relation_oid,
                    desc,
                    row,
                    catalog,
                )?);
            }
            crate::include::catalog::CONSTRAINT_PRIMARY
            | crate::include::catalog::CONSTRAINT_UNIQUE
            | crate::include::catalog::CONSTRAINT_EXCLUSION
                if row.conperiod =>
            {
                temporal.push(bind_temporal_constraint(row, desc)?);
            }
            crate::include::catalog::CONSTRAINT_EXCLUSION => {
                exclusions.push(bind_exclusion_constraint(row, desc, catalog)?);
            }
            _ => {}
        }
    }

    Ok(BoundRelationConstraints {
        relation_oid: Some(relation_oid),
        not_nulls,
        checks,
        foreign_keys,
        temporal,
        exclusions,
    })
}

pub(crate) fn bind_temporal_constraint(
    row: PgConstraintRow,
    desc: &RelationDesc,
) -> Result<BoundTemporalConstraint, ParseError> {
    let conkey = row
        .conkey
        .clone()
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "temporal constraint columns",
            actual: format!("missing conkey for constraint {}", row.conname),
        })?;
    let mut column_names = Vec::with_capacity(conkey.len());
    let mut column_indexes = Vec::with_capacity(conkey.len());
    for attnum in &conkey {
        let index = usize::try_from(*attnum)
            .ok()
            .and_then(|attnum| attnum.checked_sub(1))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "positive temporal constraint attnum",
                actual: format!("invalid attnum {attnum}"),
            })?;
        let column = desc
            .columns
            .get(index)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "temporal constraint column",
                actual: format!("attnum {attnum} out of range"),
            })?;
        if column.dropped {
            return Err(ParseError::UnexpectedToken {
                expected: "live temporal constraint column",
                actual: format!("constraint {} references dropped column", row.conname),
            });
        }
        column_names.push(column.name.clone());
        column_indexes.push(index);
    }
    let period_column_index =
        *column_indexes
            .last()
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "temporal constraint period column",
                actual: format!("constraint {} has no key columns", row.conname),
            })?;
    Ok(BoundTemporalConstraint {
        constraint_oid: row.oid,
        constraint_name: row.conname,
        column_names,
        column_indexes,
        period_column_index,
        primary: row.contype == crate::include::catalog::CONSTRAINT_PRIMARY,
        enforced: row.conenforced,
    })
}

pub(crate) fn bind_exclusion_constraint(
    row: PgConstraintRow,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundExclusionConstraint, ParseError> {
    let operator_oids = row
        .conexclop
        .clone()
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "exclusion constraint operators",
            actual: format!("missing conexclop for constraint {}", row.conname),
        })?;
    let conkey = row
        .conkey
        .clone()
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "exclusion constraint columns",
            actual: format!("missing conkey for constraint {}", row.conname),
        })?;
    let operator_proc_oids = operator_oids
        .iter()
        .map(|operator_oid| {
            catalog
                .operator_by_oid(*operator_oid)
                .map(|operator| operator.oprcode)
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "exclusion constraint operator",
                    actual: format!("unknown operator oid {operator_oid}"),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let index_relation = catalog
        .index_relations_for_heap(row.conrelid)
        .into_iter()
        .find(|index| index.relation_oid == row.conindid);
    let predicate = index_relation
        .as_ref()
        .and_then(|index| index.index_predicate.clone());
    let expression_key_names =
        exclusion_expression_key_names(row.conrelid, row.conindid, &conkey, desc, catalog)
            .unwrap_or_else(|| {
                conkey
                    .iter()
                    .take(operator_oids.len())
                    .enumerate()
                    .map(|(index, attnum)| {
                        if *attnum > 0 {
                            desc.columns
                                .get((*attnum as usize).saturating_sub(1))
                                .map(|column| column.name.clone())
                                .unwrap_or_else(|| format!("attnum{attnum}"))
                        } else {
                            format!("expr{}", index + 1)
                        }
                    })
                    .collect()
            });
    let mut expression_iter = index_relation
        .as_ref()
        .map(|index| index.index_exprs.iter())
        .into_iter()
        .flatten();
    let mut column_names = Vec::with_capacity(operator_oids.len());
    let mut column_indexes = Vec::with_capacity(operator_oids.len());
    let mut key_columns = Vec::with_capacity(operator_oids.len());
    let mut key_exprs = Vec::with_capacity(operator_oids.len());
    for (key_index, attnum) in conkey.iter().take(operator_oids.len()).enumerate() {
        if *attnum <= 0 {
            let expr =
                expression_iter
                    .next()
                    .cloned()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "exclusion index expression",
                        actual: format!("missing expression for constraint {}", row.conname),
                    })?;
            column_names.push(
                expression_key_names
                    .get(key_index)
                    .cloned()
                    .unwrap_or_else(|| format!("expr{}", key_index + 1)),
            );
            key_columns.push(None);
            key_exprs.push(Some(expr));
            continue;
        }
        let index = usize::try_from(*attnum)
            .ok()
            .and_then(|attnum| attnum.checked_sub(1))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "positive exclusion constraint attnum",
                actual: format!("invalid attnum {attnum}"),
            })?;
        let column = desc
            .columns
            .get(index)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "exclusion constraint column",
                actual: format!("attnum {attnum} out of range"),
            })?;
        if column.dropped {
            return Err(ParseError::UnexpectedToken {
                expected: "live exclusion constraint column",
                actual: format!("constraint {} references dropped column", row.conname),
            });
        }
        column_names.push(column.name.clone());
        column_indexes.push(index);
        key_columns.push(Some(index));
        key_exprs.push(None);
    }
    if key_columns.len() != operator_oids.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "one exclusion operator per key column",
            actual: format!(
                "constraint {} has {} keys and {} operators",
                row.conname,
                key_columns.len(),
                operator_oids.len()
            ),
        });
    }
    Ok(BoundExclusionConstraint {
        constraint_oid: row.oid,
        constraint_name: row.conname,
        relation_oid: row.conrelid,
        column_names,
        column_indexes,
        key_columns,
        key_exprs,
        operator_oids,
        operator_proc_oids,
        predicate,
        deferrable: row.condeferrable,
        initially_deferred: row.condeferred,
        enforced: row.conenforced,
    })
}

fn is_referenced_side_foreign_key_clone(
    row: &PgConstraintRow,
    catalog: &dyn super::CatalogLookup,
) -> bool {
    if row.contype != crate::include::catalog::CONSTRAINT_FOREIGN || row.conparentid == 0 {
        return false;
    }
    catalog
        .constraint_row_by_oid(row.conparentid)
        .is_some_and(|parent| parent.conrelid == row.conrelid)
}

fn exclusion_expression_key_names(
    relation_oid: u32,
    index_oid: u32,
    conkey: &[i16],
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Option<Vec<String>> {
    let index = catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| index.relation_oid == index_oid)?;
    let expression_sqls = index
        .index_meta
        .indexprs
        .as_deref()
        .and_then(|sql| serde_json::from_str::<Vec<String>>(sql).ok())
        .unwrap_or_default();
    let mut expression_index = 0usize;
    conkey
        .iter()
        .map(|attnum| {
            if *attnum > 0 {
                return desc
                    .columns
                    .get((*attnum as usize).saturating_sub(1))
                    .map(|column| column.name.clone());
            }
            let rendered = expression_sqls
                .get(expression_index)
                .cloned()
                .unwrap_or_else(|| format!("expr{}", expression_index + 1));
            expression_index += 1;
            Some(rendered)
        })
        .collect()
}

pub fn bind_referenced_by_foreign_keys(
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Vec<BoundReferencedByForeignKey>, ParseError> {
    let mut rows = catalog
        .foreign_key_constraint_rows_referencing_relation(relation_oid)
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    for row in catalog.constraint_rows().into_iter().filter(|row| {
        row.contype == crate::include::catalog::CONSTRAINT_FOREIGN && row.confrelid == relation_oid
    }) {
        rows.entry(row.oid).or_insert(row);
    }
    if catalog
        .relation_by_oid(relation_oid)
        .is_some_and(|relation| relation.relispartition)
    {
        let mut pending = catalog.inheritance_parents(relation_oid);
        while let Some(parent) = pending.pop() {
            for row in catalog.foreign_key_constraint_rows_referencing_relation(parent.inhparent) {
                rows.entry(row.oid).or_insert(row);
            }
            for row in catalog.constraint_rows().into_iter().filter(|row| {
                row.contype == crate::include::catalog::CONSTRAINT_FOREIGN
                    && row.confrelid == parent.inhparent
            }) {
                rows.entry(row.oid).or_insert(row);
            }
            pending.extend(catalog.inheritance_parents(parent.inhparent));
        }
    }
    let mut rows = rows.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|row| inbound_foreign_key_row_priority(relation_oid, row, catalog));
    rows.into_iter()
        .map(|row| bind_inbound_foreign_key_constraint(relation_oid, desc, row, catalog))
        .collect()
}

fn inbound_foreign_key_row_priority(
    referenced_relation_oid: u32,
    row: &PgConstraintRow,
    catalog: &dyn super::CatalogLookup,
) -> (u8, u32) {
    let exact_referenced_relation = row.confrelid == referenced_relation_oid;
    let referenced_side_clone = is_referenced_side_foreign_key_clone(row, catalog);
    let references_ancestor = row.confrelid != referenced_relation_oid
        && partition_contains_relation(catalog, row.confrelid, referenced_relation_oid);
    let child_is_partitioned = catalog
        .relation_by_oid(row.conrelid)
        .is_some_and(|relation| relation.relkind == 'p');

    let priority = if exact_referenced_relation && referenced_side_clone {
        0
    } else if exact_referenced_relation && row.conparentid == 0 && child_is_partitioned {
        1
    } else if exact_referenced_relation && row.conparentid == 0 {
        2
    } else if exact_referenced_relation {
        3
    } else if references_ancestor && referenced_side_clone {
        4
    } else if references_ancestor && row.conparentid != 0 {
        5
    } else if references_ancestor && !child_is_partitioned {
        6
    } else if references_ancestor {
        7
    } else {
        8
    };
    (priority, row.oid)
}

pub fn normalize_alter_table_add_constraint(
    table_name: &str,
    relation_oid: u32,
    relpersistence: char,
    desc: &RelationDesc,
    existing_constraints: &[PgConstraintRow],
    constraint: &TableConstraint,
    catalog: &dyn super::CatalogLookup,
) -> Result<NormalizedAlterTableConstraint, ParseError> {
    let column_lookup = relation_column_lookup(desc);
    let relation_columns = desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let mut used_names = existing_constraint_names(existing_constraints);

    match constraint {
        TableConstraint::NotNull { attributes, column } => {
            validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
            let column_index = *column_lookup
                .get(&column.to_ascii_lowercase())
                .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
            if desc.columns[column_index].storage.nullable {
                let constraint_name = assign_constraint_name(
                    attributes.name.clone(),
                    GeneratedConstraintName::new(table_name, Some(column.clone()), "not_null"),
                    &mut used_names,
                )?;
                Ok(NormalizedAlterTableConstraint::NotNull(
                    NotNullConstraintAction {
                        constraint_name,
                        column: desc.columns[column_index].name.clone(),
                        not_valid: attributes.not_valid,
                        no_inherit: attributes.no_inherit,
                        primary_key_owned: false,
                        is_local: true,
                        inhcount: 0,
                    },
                ))
            } else {
                let existing = &desc.columns[column_index];
                if existing.not_null_constraint_no_inherit != attributes.no_inherit {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                            existing.not_null_constraint_name.as_deref().unwrap_or(column),
                            table_name,
                        ),
                        detail: None,
                        hint: Some(
                            "You might need to make the existing constraint inheritable using ALTER TABLE ... ALTER CONSTRAINT ... INHERIT.".into(),
                        ),
                        sqlstate: "0A000",
                    });
                }
                if !existing.not_null_constraint_validated {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "incompatible NOT VALID constraint \"{}\" on relation \"{}\"",
                            existing.not_null_constraint_name.as_deref().unwrap_or(column),
                            table_name,
                        ),
                        detail: None,
                        hint: Some(
                            "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.".into(),
                        ),
                        sqlstate: "55000",
                    });
                }
                if let Some(incoming_name) = attributes.name.as_deref() {
                    let existing_name = existing
                        .not_null_constraint_name
                        .as_deref()
                        .unwrap_or(column);
                    if !incoming_name.eq_ignore_ascii_case(existing_name) {
                        return Err(ParseError::DetailedError {
                            message: format!(
                                "cannot create not-null constraint \"{}\" on column \"{}\" of table \"{}\"",
                                incoming_name, existing.name, table_name,
                            ),
                            detail: Some(format!(
                                "A not-null constraint named \"{}\" already exists for this column.",
                                existing_name,
                            )),
                            hint: None,
                            sqlstate: "42P16",
                        });
                    }
                }
                Ok(NormalizedAlterTableConstraint::Noop)
            }
        }
        TableConstraint::Check {
            attributes,
            expr_sql,
        } => {
            validate_check_attributes(attributes)?;
            if let Some(requested_name) = attributes.name.as_deref()
                && let Some(existing) = existing_constraints.iter().find(|row| {
                    row.conname.eq_ignore_ascii_case(requested_name)
                        && row.contype == crate::include::catalog::CONSTRAINT_CHECK
                        && (row.coninhcount > 0 || !row.conislocal)
                        && row.conbin.as_deref().is_some_and(|conbin| {
                            conbin.trim().eq_ignore_ascii_case(expr_sql.trim())
                        })
                })
            {
                if attributes.no_inherit {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "constraint \"{requested_name}\" conflicts with inherited constraint on relation \"{table_name}\""
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P17",
                    });
                }
                if existing.conenforced && !attributes.enforced.unwrap_or(true) {
                    return Err(ParseError::DetailedError {
                        message: format!(
                            "constraint \"{requested_name}\" conflicts with NOT ENFORCED constraint on relation \"{table_name}\""
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "42P17",
                    });
                }
                return Ok(NormalizedAlterTableConstraint::Check(
                    CheckConstraintAction {
                        constraint_name: existing.conname.clone(),
                        expr_sql: expr_sql.clone(),
                        not_valid: attributes.not_valid,
                        no_inherit: false,
                        enforced: attributes.enforced.unwrap_or(true),
                        parent_constraint_oid: (existing.conparentid != 0)
                            .then_some(existing.conparentid),
                        is_local: true,
                        inhcount: existing.coninhcount,
                    },
                ));
            }
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                GeneratedConstraintName::new(
                    table_name,
                    check_constraint_name_column(expr_sql, &relation_columns),
                    "check",
                ),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::Check(
                CheckConstraintAction {
                    constraint_name,
                    expr_sql: expr_sql.clone(),
                    not_valid: attributes.not_valid,
                    no_inherit: attributes.no_inherit,
                    enforced: attributes.enforced.unwrap_or(true),
                    parent_constraint_oid: None,
                    is_local: true,
                    inhcount: 0,
                },
            ))
        }
        TableConstraint::PrimaryKey {
            attributes,
            columns,
            include_columns,
            without_overlaps,
            tablespace,
        } => {
            let (deferrable, initially_deferred) =
                validate_key_attributes(attributes, "PRIMARY KEY")?;
            if existing_constraints
                .iter()
                .any(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
            {
                return Err(multiple_primary_keys_error(table_name));
            }
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                GeneratedConstraintName::new(table_name, None, "pkey"),
                &mut used_names,
            )?;
            let resolved = resolve_relation_index_constraint_columns(
                columns,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            validate_without_overlaps_relation_column(
                &resolved,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            let resolved_include =
                resolve_relation_constraint_columns(include_columns, desc, &column_lookup)?;
            let index_columns = index_columns_from_names(&resolved);
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: None,
                    columns: resolved,
                    index_columns,
                    include_columns: resolved_include,
                    primary: true,
                    exclusion: false,
                    nulls_not_distinct: false,
                    without_overlaps: without_overlaps.clone(),
                    tablespace: tablespace.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    predicate_sql: None,
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::Unique {
            attributes,
            columns,
            include_columns,
            without_overlaps,
            tablespace,
        } => {
            let (deferrable, initially_deferred) = validate_key_attributes(attributes, "UNIQUE")?;
            let resolved = resolve_relation_index_constraint_columns(
                columns,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            validate_without_overlaps_relation_column(
                &resolved,
                without_overlaps.as_deref(),
                desc,
                &column_lookup,
            )?;
            let resolved_include =
                resolve_relation_constraint_columns(include_columns, desc, &column_lookup)?;
            let generated_columns = resolved
                .iter()
                .chain(resolved_include.iter())
                .cloned()
                .collect::<Vec<_>>();
            let index_columns = index_columns_from_names(&resolved);
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                GeneratedConstraintName::new(table_name, Some(generated_columns.join("_")), "key"),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: None,
                    columns: resolved,
                    index_columns,
                    include_columns: resolved_include,
                    primary: false,
                    exclusion: false,
                    nulls_not_distinct: attributes.nulls_not_distinct,
                    without_overlaps: without_overlaps.clone(),
                    tablespace: tablespace.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    predicate_sql: None,
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::Exclusion {
            attributes,
            using_method,
            elements,
            include_columns,
            predicate_sql,
        } => {
            let (deferrable, initially_deferred) = validate_key_attributes(attributes, "EXCLUDE")?;
            let key_columns = elements
                .iter()
                .filter(|element| element.expr_sql.is_none())
                .map(|element| element.column.clone())
                .collect::<Vec<_>>();
            let resolved = resolve_relation_constraint_columns(&key_columns, desc, &column_lookup)?;
            let resolved_include =
                resolve_relation_constraint_columns(include_columns, desc, &column_lookup)?;
            let index_columns = exclusion_index_columns(elements, &resolved);
            let generated_columns = index_columns
                .iter()
                .map(|column| {
                    column.expr_sql.as_deref().map_or_else(
                        || column.name.clone(),
                        |expr_sql| {
                            check_constraint_name_column(expr_sql, &relation_columns)
                                .unwrap_or_else(|| "expr".into())
                        },
                    )
                })
                .chain(resolved_include.iter().cloned())
                .collect::<Vec<_>>();
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                GeneratedConstraintName::new(table_name, Some(generated_columns.join("_")), "excl"),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: None,
                    columns: resolved,
                    index_columns,
                    include_columns: resolved_include,
                    primary: false,
                    nulls_not_distinct: false,
                    without_overlaps: None,
                    tablespace: None,
                    exclusion: true,
                    access_method: Some(using_method.clone()),
                    exclusion_operators: elements
                        .iter()
                        .map(|element| element.operator.clone())
                        .collect(),
                    predicate_sql: predicate_sql.clone(),
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::PrimaryKeyUsingIndex {
            attributes,
            index_name,
            tablespace,
        } => {
            let (deferrable, initially_deferred) =
                validate_key_attributes(attributes, "PRIMARY KEY")?;
            if existing_constraints
                .iter()
                .any(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
            {
                return Err(multiple_primary_keys_error(table_name));
            }
            let (columns, include_columns, nulls_not_distinct) = index_columns_for_existing_index(
                table_name,
                relation_oid,
                desc,
                index_name,
                catalog,
            )?;
            if nulls_not_distinct {
                return Err(ParseError::UnexpectedToken {
                    expected: "default NULL handling",
                    actual: "primary keys cannot use NULLS NOT DISTINCT indexes".into(),
                });
            }
            let constraint_name = assign_constraint_name(
                Some(
                    attributes
                        .name
                        .clone()
                        .unwrap_or_else(|| index_name.clone()),
                ),
                GeneratedConstraintName::new(index_name, None, ""),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: Some(index_name.clone()),
                    index_columns: index_columns_from_names(&columns),
                    columns,
                    include_columns,
                    primary: true,
                    exclusion: false,
                    nulls_not_distinct,
                    without_overlaps: None,
                    tablespace: tablespace.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    predicate_sql: None,
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::UniqueUsingIndex {
            attributes,
            index_name,
            tablespace,
        } => {
            let (deferrable, initially_deferred) = validate_key_attributes(attributes, "UNIQUE")?;
            let (columns, include_columns, nulls_not_distinct) = index_columns_for_existing_index(
                table_name,
                relation_oid,
                desc,
                index_name,
                catalog,
            )?;
            let constraint_name = assign_constraint_name(
                Some(
                    attributes
                        .name
                        .clone()
                        .unwrap_or_else(|| index_name.clone()),
                ),
                GeneratedConstraintName::new(index_name, None, ""),
                &mut used_names,
            )?;
            Ok(NormalizedAlterTableConstraint::IndexBacked(
                IndexBackedConstraintAction {
                    constraint_name: Some(constraint_name),
                    existing_index_name: Some(index_name.clone()),
                    index_columns: index_columns_from_names(&columns),
                    columns,
                    include_columns,
                    primary: false,
                    exclusion: false,
                    nulls_not_distinct,
                    without_overlaps: None,
                    tablespace: tablespace.clone(),
                    access_method: None,
                    exclusion_operators: Vec::new(),
                    predicate_sql: None,
                    deferrable,
                    initially_deferred,
                },
            ))
        }
        TableConstraint::ForeignKey {
            attributes,
            columns,
            period,
            referenced_table,
            referenced_columns,
            referenced_period,
            match_type,
            on_delete,
            on_delete_set_columns,
            on_update,
        } => {
            validate_alter_foreign_key(
                attributes,
                *match_type,
                *on_delete,
                on_delete_set_columns.as_deref(),
                *on_update,
            )?;
            let (deferrable, initially_deferred) = foreign_key_deferrability(attributes);
            let resolved = resolve_relation_foreign_key_columns(
                columns,
                period.as_deref(),
                desc,
                &column_lookup,
            )?;
            let child_types = resolved
                .iter()
                .map(|column| {
                    let index = *column_lookup
                        .get(&column.to_ascii_lowercase())
                        .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
                    Ok(desc.columns[index].sql_type)
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            let constraint_name = assign_constraint_name(
                attributes.name.clone(),
                GeneratedConstraintName::new(table_name, Some(resolved.join("_")), "fkey"),
                &mut used_names,
            )?;
            let referenced = resolve_referenced_key(
                table_name,
                Some(relation_oid),
                relpersistence,
                referenced_table,
                referenced_columns.as_deref(),
                period.as_deref(),
                referenced_period.as_deref(),
                &constraint_name,
                &resolved,
                &child_types,
                catalog,
            )?;
            validate_foreign_key_period_actions(
                period.is_some() || referenced.period.is_some(),
                *on_delete,
                *on_update,
            )?;
            Ok(NormalizedAlterTableConstraint::ForeignKey(
                ForeignKeyConstraintAction {
                    constraint_name,
                    columns: resolved,
                    period: period.clone(),
                    referenced_table: relation_display_name(
                        catalog,
                        referenced.relation.relation_oid,
                        referenced_table,
                    ),
                    referenced_relation_oid: referenced.relation.relation_oid,
                    referenced_index_oid: referenced.index_oid,
                    self_referential: false,
                    referenced_columns: referenced.columns,
                    referenced_period: referenced.period,
                    match_type: *match_type,
                    on_delete: *on_delete,
                    on_delete_set_columns: resolve_foreign_key_delete_set_columns(
                        on_delete_set_columns.as_deref(),
                        &relation_columns,
                        columns,
                    )?,
                    on_update: *on_update,
                    deferrable,
                    initially_deferred,
                    not_valid: attributes.not_valid,
                    enforced: attributes.enforced.unwrap_or(true),
                },
            ))
        }
    }
}

pub fn normalize_alter_table_add_column_constraints(
    table_name: &str,
    column: &crate::include::nodes::parsenodes::ColumnDef,
    existing_constraints: &[PgConstraintRow],
) -> Result<NormalizedAddColumnConstraints, ParseError> {
    let column_lookup = BTreeMap::from([(column.name.to_ascii_lowercase(), 0usize)]);
    let mut not_nulls = BTreeMap::<String, PendingNotNullConstraint>::new();
    let mut checks = Vec::new();

    for constraint in &column.constraints {
        match constraint {
            ColumnConstraint::NotNull { attributes } => {
                validate_not_null_or_check_attributes(attributes, "NOT NULL")?;
                merge_not_null_constraint(
                    &mut not_nulls,
                    &column_lookup,
                    &column.name,
                    attributes,
                    false,
                    table_name,
                )?;
            }
            ColumnConstraint::Check {
                attributes,
                expr_sql,
            } => {
                validate_check_attributes(attributes)?;
                checks.push(PendingCheckConstraint {
                    explicit_name: attributes.name.clone(),
                    generated_base: GeneratedConstraintName::new(
                        table_name,
                        Some(column.name.clone()),
                        "check",
                    ),
                    expr_sql: expr_sql.clone(),
                    not_valid: attributes.not_valid,
                    no_inherit: attributes.no_inherit,
                    enforced: attributes.enforced.unwrap_or(true),
                });
            }
            ColumnConstraint::PrimaryKey { .. }
            | ColumnConstraint::Unique { .. }
            | ColumnConstraint::References { .. } => {}
        }
    }

    let mut used_names = existing_constraint_names(existing_constraints);
    reserve_explicit_constraint_names(
        &mut used_names,
        not_nulls
            .values()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;
    reserve_explicit_constraint_names(
        &mut used_names,
        checks
            .iter()
            .filter_map(|constraint| constraint.explicit_name.as_deref()),
    )?;

    let not_null = not_nulls
        .into_values()
        .next()
        .map(|constraint| NotNullConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            column: constraint.column,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
            primary_key_owned: false,
            is_local: true,
            inhcount: 0,
        });
    let checks = checks
        .into_iter()
        .map(|constraint| CheckConstraintAction {
            constraint_name: constraint.explicit_name.unwrap_or_else(|| {
                choose_generated_constraint_name(&constraint.generated_base, &mut used_names)
            }),
            expr_sql: constraint.expr_sql,
            not_valid: constraint.not_valid,
            no_inherit: constraint.no_inherit,
            enforced: constraint.enforced,
            parent_constraint_oid: None,
            is_local: true,
            inhcount: 0,
        })
        .collect();

    Ok(NormalizedAddColumnConstraints { not_null, checks })
}

pub fn generated_not_null_constraint_name(
    table_name: &str,
    column_name: &str,
    existing_constraints: &[PgConstraintRow],
) -> String {
    let mut used_names = existing_constraint_names(existing_constraints);
    choose_generated_constraint_name(
        &GeneratedConstraintName::new(table_name, Some(column_name.to_string()), "not_null"),
        &mut used_names,
    )
}

pub fn bind_check_constraint_expr(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    bind_check_constraint_sql_expr(&parsed, relation_name, desc, catalog)
}

pub fn bind_index_predicate_sql_expr(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    bind_index_predicate_expr(&parsed, relation_name, desc, catalog)
}

pub fn bind_check_constraint_sql_expr(
    expr: &SqlExpr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    bind_boolean_relation_predicate(
        expr,
        relation_name,
        desc,
        catalog,
        "boolean CHECK constraint expression",
        "CHECK constraint expression must return boolean",
    )
}

pub fn bind_index_predicate_expr(
    expr: &SqlExpr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let scope = super::scope_for_base_relation_with_optional_name(relation_name, desc);
    let inferred = super::infer_sql_expr_type_with_ctes(expr, &scope, catalog, &[], None, &[]);
    if inferred != SqlType::new(SqlTypeKind::Bool) {
        return Err(ParseError::UnexpectedToken {
            expected: "boolean index predicate expression",
            actual: "index predicate expression must return boolean".into(),
        });
    }

    let bound = super::bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &[])?;
    reject_unsupported_index_predicate_expr(&bound)?;
    Ok(bound)
}

fn bind_boolean_relation_predicate(
    expr: &SqlExpr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
    expected: &'static str,
    actual: &'static str,
) -> Result<Expr, ParseError> {
    let scope = super::scope_for_base_relation_with_optional_name(relation_name, desc);
    let inferred = super::infer_sql_expr_type_with_ctes(expr, &scope, catalog, &[], None, &[]);
    if inferred != SqlType::new(SqlTypeKind::Bool) {
        return Err(ParseError::UnexpectedToken {
            expected,
            actual: actual.into(),
        });
    }

    let bound = super::bind_expr_with_outer_and_ctes(expr, &scope, catalog, &[], None, &[])?;
    reject_unsupported_check_expr(&bound)?;
    Ok(bound)
}

pub(crate) fn infer_relation_expr_sql_type(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<SqlType, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    let scope = super::scope_for_relation(relation_name, desc);
    Ok(super::infer_sql_expr_type_with_ctes(
        &parsed,
        &scope,
        catalog,
        &[],
        None,
        &[],
    ))
}

pub(crate) fn bind_relation_expr(
    expr_sql: &str,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn super::CatalogLookup,
) -> Result<Expr, ParseError> {
    let parsed = parse_expr(expr_sql)?;
    let scope = super::scope_for_relation(relation_name, desc);
    super::bind_expr_with_outer_and_ctes(&parsed, &scope, catalog, &[], None, &[])
}

fn validate_not_null_or_check_attributes(
    attributes: &ConstraintAttributes,
    constraint_kind: &'static str,
) -> Result<(), ParseError> {
    if attributes.deferrable.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} DEFERRABLE"
        )));
    }
    if attributes.initially_deferred.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} INITIALLY"
        )));
    }
    if attributes.enforced.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} ENFORCED/NOT ENFORCED"
        )));
    }
    Ok(())
}

fn validate_check_attributes(attributes: &ConstraintAttributes) -> Result<(), ParseError> {
    if attributes.deferrable.is_some() {
        return Err(ParseError::FeatureNotSupported("CHECK DEFERRABLE".into()));
    }
    if attributes.initially_deferred.is_some() {
        return Err(ParseError::FeatureNotSupported("CHECK INITIALLY".into()));
    }
    Ok(())
}

fn validate_key_attributes(
    attributes: &ConstraintAttributes,
    constraint_kind: &'static str,
) -> Result<(bool, bool), ParseError> {
    if attributes.not_valid {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} NOT VALID"
        )));
    }
    if attributes.no_inherit {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} NO INHERIT"
        )));
    }
    if attributes.enforced.is_some() {
        return Err(ParseError::FeatureNotSupported(format!(
            "{constraint_kind} ENFORCED/NOT ENFORCED"
        )));
    }
    let mut deferrable = attributes.deferrable.unwrap_or(false);
    let initially_deferred = attributes.initially_deferred.unwrap_or(false);
    if initially_deferred {
        deferrable = true;
    }
    Ok((deferrable, initially_deferred))
}

fn validate_create_foreign_key(
    attributes: &ConstraintAttributes,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<&[String]>,
    on_update: ForeignKeyAction,
) -> Result<(), ParseError> {
    validate_foreign_key(
        attributes,
        match_type,
        on_delete,
        on_delete_set_columns,
        on_update,
    )
}

fn validate_alter_foreign_key(
    attributes: &ConstraintAttributes,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<&[String]>,
    on_update: ForeignKeyAction,
) -> Result<(), ParseError> {
    validate_foreign_key(
        attributes,
        match_type,
        on_delete,
        on_delete_set_columns,
        on_update,
    )
}

fn validate_foreign_key(
    attributes: &ConstraintAttributes,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<&[String]>,
    _on_update: ForeignKeyAction,
) -> Result<(), ParseError> {
    if attributes.deferrable.is_some() || attributes.initially_deferred.is_some() {
        if attributes.enforced == Some(false) {
            return Err(ParseError::FeatureNotSupported(
                "FOREIGN KEY NOT ENFORCED with DEFERRABLE/INITIALLY".into(),
            ));
        }
    }
    if match_type == ForeignKeyMatchType::Partial {
        return Err(ParseError::FeatureNotSupported(format!(
            "FOREIGN KEY MATCH {}",
            foreign_key_match_keyword(match_type)
        )));
    }
    if on_delete_set_columns.is_some()
        && !matches!(
            on_delete,
            ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault
        )
    {
        return Err(ParseError::FeatureNotSupported(
            "ON DELETE column lists require SET NULL or SET DEFAULT".into(),
        ));
    }
    Ok(())
}

fn validate_foreign_key_period_actions(
    uses_period: bool,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
) -> Result<(), ParseError> {
    if !uses_period {
        return Ok(());
    }
    if on_update != ForeignKeyAction::NoAction {
        return Err(ParseError::DetailedError {
            message: "unsupported ON UPDATE action for foreign key constraint using PERIOD".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if on_delete != ForeignKeyAction::NoAction {
        return Err(ParseError::DetailedError {
            message: "unsupported ON DELETE action for foreign key constraint using PERIOD".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
}

fn foreign_key_deferrability(attributes: &ConstraintAttributes) -> (bool, bool) {
    let mut deferrable = attributes.deferrable.unwrap_or(false);
    let initially_deferred = attributes.initially_deferred.unwrap_or(false);
    if initially_deferred {
        deferrable = true;
    }
    (deferrable, initially_deferred)
}

fn resolve_foreign_key_delete_set_columns(
    delete_set_columns: Option<&[String]>,
    relation_columns: &[String],
    foreign_key_columns: &[String],
) -> Result<Option<Vec<String>>, ParseError> {
    let Some(delete_set_columns) = delete_set_columns else {
        return Ok(None);
    };
    let mut resolved = Vec::new();
    let mut seen = BTreeSet::new();
    for column_name in delete_set_columns {
        let Some(relation_column) = relation_columns
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(column_name))
        else {
            return Err(ParseError::DetailedError {
                message: format!(
                    "column \"{column_name}\" referenced in foreign key constraint does not exist"
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            });
        };
        let Some(foreign_key_column) = foreign_key_columns
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(column_name))
        else {
            return Err(ParseError::DetailedError {
                message: format!(
                    "column \"{column_name}\" referenced in ON DELETE SET action must be part of foreign key"
                ),
                detail: None,
                hint: None,
                sqlstate: "42P10",
            });
        };
        let normalized = foreign_key_column.to_ascii_lowercase();
        if seen.insert(normalized) {
            resolved.push(relation_column.clone());
        }
    }
    Ok(Some(resolved))
}

fn resolve_pending_self_referenced_key(
    table_name: &str,
    column_defs: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
    index_constraints: &[IndexBackedConstraintAction],
    referenced_columns: Option<&[String]>,
    period: Option<&str>,
    referenced_period: Option<&str>,
    constraint_name: &str,
    child_columns: &[String],
    child_types: &[SqlType],
    catalog: &dyn super::CatalogLookup,
) -> Result<ResolvedPendingReferencedKey, ParseError> {
    if referenced_columns.is_some_and(has_duplicate_column) {
        return Err(foreign_key_referenced_columns_duplicate_error());
    }

    let explicit_referenced_columns = referenced_columns.is_some();
    let explicit_referenced_period = referenced_period.is_some();
    let (referenced_columns, matched_period, matched_primary) =
        if let Some(referenced_columns) = referenced_columns {
            let referenced_columns = resolve_foreign_key_columns(
                referenced_columns,
                referenced_period,
                column_defs,
                column_lookup,
            )?;
            let matched = index_constraints.iter().find(|constraint| {
                !constraint.exclusion
                    && string_key_columns_match_as_set(&constraint.columns, &referenced_columns)
            });
            let Some(matched) = matched else {
                if period.is_some() && referenced_period.is_none() {
                    return Err(foreign_key_period_on_child_only_error());
                }
                if period.is_none() && referenced_period.is_some() {
                    return Err(foreign_key_period_on_parent_only_error());
                }
                return Err(foreign_key_no_unique_constraint_error(table_name));
            };
            (
                referenced_columns,
                matched.without_overlaps.clone(),
                matched.primary,
            )
        } else {
            let primary = index_constraints
                .iter()
                .filter(|constraint| constraint.primary)
                .collect::<Vec<_>>();
            if primary.len() != 1 {
                return Err(ParseError::UnexpectedToken {
                    expected: "referenced PRIMARY KEY",
                    actual: if primary.is_empty() {
                        format!("table \"{table_name}\" has no PRIMARY KEY")
                    } else {
                        format!("table \"{table_name}\" has multiple PRIMARY KEY constraints")
                    },
                });
            }
            (
                primary[0].columns.clone(),
                primary[0].without_overlaps.clone(),
                true,
            )
        };

    let resolved_period = if let Some(matched_period) = matched_period {
        if explicit_referenced_columns && !explicit_referenced_period {
            if period.is_none() && matched_primary {
                return Err(foreign_key_must_use_period_for_temporal_key_error(true));
            }
            if period.is_none() {
                return Err(foreign_key_must_use_period_for_temporal_key_error(false));
            }
            return Err(foreign_key_period_on_child_only_error());
        }
        if period.is_none() {
            return Err(foreign_key_period_on_parent_only_error());
        }
        Some(
            referenced_period
                .map(str::to_string)
                .unwrap_or(matched_period),
        )
    } else {
        if period.is_some() {
            return Err(foreign_key_period_on_child_only_error());
        }
        if referenced_period.is_some() {
            return Err(foreign_key_period_on_parent_only_error());
        }
        None
    };

    if child_types.len() != referenced_columns.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "matching foreign-key column counts",
            actual: format!(
                "{} referencing column(s) for {} local column(s)",
                referenced_columns.len(),
                child_types.len()
            ),
        });
    }

    let parent_types = referenced_columns
        .iter()
        .map(|column| {
            let index = *column_lookup
                .get(&column.to_ascii_lowercase())
                .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
            resolve_create_table_column_type_for_foreign_key(&column_defs[index].ty, catalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !foreign_key_types_compatible(child_types, &parent_types) {
        return Err(foreign_key_type_mismatch_error(
            constraint_name,
            child_columns,
            &referenced_columns,
            child_types,
            &parent_types,
            catalog,
        ));
    }

    Ok(ResolvedPendingReferencedKey {
        columns: referenced_columns,
        period: resolved_period,
    })
}

fn resolve_create_table_column_type_for_foreign_key(
    raw: &crate::backend::parser::RawTypeName,
    catalog: &dyn super::CatalogLookup,
) -> Result<SqlType, ParseError> {
    match raw {
        crate::backend::parser::RawTypeName::Serial(_) => Ok(super::raw_type_name_hint(raw)),
        _ => super::resolve_raw_type_name(raw, catalog),
    }
}

fn foreign_key_types_compatible(child_types: &[SqlType], parent_types: &[SqlType]) -> bool {
    child_types
        .iter()
        .zip(parent_types)
        .all(|(&child, &parent)| foreign_key_type_compatible(child, parent))
}

fn has_duplicate_column(columns: &[String]) -> bool {
    let mut seen = BTreeSet::new();
    columns
        .iter()
        .any(|column| !seen.insert(column.to_ascii_lowercase()))
}

fn foreign_key_referenced_columns_duplicate_error() -> ParseError {
    ParseError::DetailedError {
        message: "foreign key referenced-columns list must not contain duplicates".into(),
        detail: None,
        hint: None,
        sqlstate: "42701",
    }
}

fn foreign_key_no_unique_constraint_error(table_name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!(
            "there is no unique constraint matching given keys for referenced table \"{table_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "42830",
    }
}

fn foreign_key_period_on_child_only_error() -> ParseError {
    ParseError::DetailedError {
        message: "foreign key uses PERIOD on the referencing table but not the referenced table"
            .into(),
        detail: None,
        hint: None,
        sqlstate: "42830",
    }
}

fn foreign_key_period_on_parent_only_error() -> ParseError {
    ParseError::DetailedError {
        message: "foreign key uses PERIOD on the referenced table but not the referencing table"
            .into(),
        detail: None,
        hint: None,
        sqlstate: "42830",
    }
}

fn foreign_key_must_use_period_for_temporal_key_error(primary: bool) -> ParseError {
    let key_kind = if primary {
        "primary key"
    } else {
        "unique constraint"
    };
    ParseError::DetailedError {
        message: format!(
            "foreign key must use PERIOD when referencing a {key_kind} using WITHOUT OVERLAPS"
        ),
        detail: None,
        hint: None,
        sqlstate: "42830",
    }
}

fn foreign_key_type_mismatch_error(
    constraint_name: &str,
    child_columns: &[String],
    parent_columns: &[String],
    child_types: &[SqlType],
    parent_types: &[SqlType],
    catalog: &dyn super::CatalogLookup,
) -> ParseError {
    let mismatch = child_types
        .iter()
        .zip(parent_types)
        .enumerate()
        .find_map(|(index, (&child, &parent))| {
            (!foreign_key_type_compatible(child, parent)).then(|| {
                (
                    child_columns
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| "?column?".into()),
                    parent_columns
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| "?column?".into()),
                    child,
                    parent,
                )
            })
        })
        .expect("foreign_key_type_mismatch_error requires at least one mismatched column");
    let (child_column, parent_column, child_type, parent_type) = mismatch;
    let child_name = quoted_column(&child_column);
    let parent_name = quoted_column(&parent_column);
    let child_type_name = foreign_key_type_name(child_type, catalog);
    let parent_type_name = foreign_key_type_name(parent_type, catalog);
    ParseError::DetailedError {
        message: format!("foreign key constraint \"{constraint_name}\" cannot be implemented"),
        detail: Some(format!(
            "Key columns {child_name} of the referencing table and {parent_name} of the referenced table are of incompatible types: {child_type_name} and {parent_type_name}."
        )),
        hint: None,
        sqlstate: "42804",
    }
}

fn quoted_column(column: &str) -> String {
    format!("\"{column}\"")
}

fn quoted_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

fn foreign_key_type_name(ty: SqlType, catalog: &dyn super::CatalogLookup) -> String {
    if !ty.is_array
        && ty.type_oid != 0
        && let Some(row) = catalog.type_by_oid(ty.type_oid)
    {
        return row.typname;
    }
    super::coerce::sql_type_name(ty)
}

fn foreign_key_type_compatible(child: SqlType, parent: SqlType) -> bool {
    if child == parent {
        return true;
    }
    if child.is_array || parent.is_array {
        return false;
    }
    if foreign_key_integer_type(child) && foreign_key_integer_type(parent) {
        return true;
    }
    if foreign_key_text_like_type(child) && foreign_key_text_like_type(parent) {
        return true;
    }
    if foreign_key_integer_type(child) && foreign_key_float_type(parent) {
        return true;
    }

    let child_oid = sql_type_oid(child);
    let parent_oid = sql_type_oid(parent);
    child_oid != 0
        && parent_oid != 0
        && bootstrap_pg_cast_rows().into_iter().any(|row| {
            row.castsource == child_oid && row.casttarget == parent_oid && row.castcontext == 'i'
        })
}

fn foreign_key_integer_type(ty: SqlType) -> bool {
    matches!(
        ty.kind,
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
    )
}

fn foreign_key_float_type(ty: SqlType) -> bool {
    matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8)
}

fn foreign_key_text_like_type(ty: SqlType) -> bool {
    matches!(
        ty.kind,
        SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
    )
}

fn resolve_referenced_key(
    child_relation_name: &str,
    child_relation_oid: Option<u32>,
    child_persistence: char,
    referenced_table: &str,
    referenced_columns: Option<&[String]>,
    period: Option<&str>,
    referenced_period: Option<&str>,
    constraint_name: &str,
    child_columns: &[String],
    child_types: &[SqlType],
    catalog: &dyn super::CatalogLookup,
) -> Result<ResolvedReferencedKey, ParseError> {
    let relation = catalog
        .lookup_any_relation(referenced_table)
        .ok_or_else(|| ParseError::UnknownTable(referenced_table.to_string()))?;
    if !matches!(relation.relkind, 'r' | 'p') {
        return Err(ParseError::UnknownTable(referenced_table.to_string()));
    }
    let _ = child_relation_name;
    let _ = child_relation_oid;
    validate_foreign_key_persistence(child_persistence, relation.relpersistence)?;

    let relation_lookup = relation_column_lookup(&relation.desc);
    if referenced_columns.is_some_and(has_duplicate_column) {
        return Err(foreign_key_referenced_columns_duplicate_error());
    }
    let explicit_referenced_columns = referenced_columns.is_some();
    let explicit_referenced_period = referenced_period.is_some();
    let (columns, referenced_attnums, index_oid, referenced_period, temporal_key, primary_key) =
        if let Some(referenced_columns) = referenced_columns {
            let columns = resolve_relation_foreign_key_columns(
                referenced_columns,
                referenced_period,
                &relation.desc,
                &relation_lookup,
            )?;
            let attnums = column_attnums_for_names(&relation.desc, &columns)?;
            if let Some(row) =
                exact_referenced_constraint_for_attnums(catalog, relation.relation_oid, &attnums)
            {
                let resolved_period = if row.conperiod {
                    Some(
                        referenced_period
                            .map(str::to_string)
                            .or_else(|| columns.last().cloned())
                            .ok_or(ParseError::UnexpectedEof)?,
                    )
                } else {
                    None
                };
                (
                    columns,
                    attnums,
                    row.conindid,
                    resolved_period,
                    row.conperiod,
                    row.contype == crate::include::catalog::CONSTRAINT_PRIMARY,
                )
            } else if period.is_some() && referenced_period.is_none() {
                return Err(foreign_key_period_on_child_only_error());
            } else if period.is_none() && referenced_period.is_some() {
                return Err(foreign_key_period_on_parent_only_error());
            } else {
                let index = find_compatible_unique_index_for_attnums(
                    catalog,
                    relation.relation_oid,
                    &attnums,
                )
                .ok_or_else(|| foreign_key_no_unique_constraint_error(referenced_table))?;
                (columns, attnums, index.relation_oid, None, false, false)
            }
        } else {
            let primary = catalog
                .constraint_rows_for_relation(relation.relation_oid)
                .into_iter()
                .filter(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
                .collect::<Vec<_>>();
            if primary.len() != 1 {
                return Err(ParseError::UnexpectedToken {
                    expected: "referenced PRIMARY KEY",
                    actual: if primary.is_empty() {
                        format!("table \"{}\" has no PRIMARY KEY", referenced_table)
                    } else {
                        format!(
                            "table \"{}\" has multiple PRIMARY KEY constraints",
                            referenced_table
                        )
                    },
                });
            }
            let row = &primary[0];
            let attnums = constraint_attnums(row, "PRIMARY KEY")?;
            let columns = attnums_to_column_names(&relation.desc, &attnums)?;
            (
                columns.clone(),
                attnums,
                row.conindid,
                row.conperiod.then(|| columns.last().cloned()).flatten(),
                row.conperiod,
                true,
            )
        };

    if temporal_key {
        if explicit_referenced_columns && !explicit_referenced_period {
            if period.is_none() {
                return Err(foreign_key_must_use_period_for_temporal_key_error(
                    primary_key,
                ));
            }
            return Err(foreign_key_period_on_child_only_error());
        }
        if period.is_none() {
            return Err(foreign_key_period_on_parent_only_error());
        }
    } else {
        if period.is_some() {
            return Err(foreign_key_period_on_child_only_error());
        }
        if referenced_period.is_some() {
            return Err(foreign_key_period_on_parent_only_error());
        }
    }

    if child_types.len() != referenced_attnums.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "matching foreign-key column counts",
            actual: format!(
                "{} referencing column(s) for {} local column(s)",
                referenced_attnums.len(),
                child_types.len()
            ),
        });
    }

    let parent_types = referenced_attnums
        .iter()
        .map(|&attnum| {
            column_index_for_attnum(&relation.desc, attnum)
                .map(|index| relation.desc.columns[index].sql_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !foreign_key_types_compatible(child_types, &parent_types) {
        return Err(foreign_key_type_mismatch_error(
            constraint_name,
            child_columns,
            &columns,
            child_types,
            &parent_types,
            catalog,
        ));
    }

    Ok(ResolvedReferencedKey {
        relation,
        columns,
        period: referenced_period,
        index_oid,
    })
}

fn exact_referenced_constraint_for_attnums(
    catalog: &dyn super::CatalogLookup,
    relation_oid: u32,
    attnums: &[i16],
) -> Option<PgConstraintRow> {
    catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .find(|row| {
            matches!(
                row.contype,
                crate::include::catalog::CONSTRAINT_PRIMARY
                    | crate::include::catalog::CONSTRAINT_UNIQUE
            ) && row.conindid != 0
                && row
                    .conkey
                    .as_deref()
                    .is_some_and(|conkey| attnum_key_columns_match_as_set(conkey, attnums))
        })
}

fn bind_outbound_foreign_key_constraint(
    relation_oid: u32,
    desc: &RelationDesc,
    row: PgConstraintRow,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundForeignKeyConstraint, ParseError> {
    let local_attnums = constraint_attnums(&row, "FOREIGN KEY")?;
    let referenced_attnums = row
        .confkey
        .clone()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "referenced foreign-key columns",
            actual: format!("missing confkey for constraint {}", row.conname),
        })?;
    let referenced_relation = catalog
        .lookup_relation_by_oid(row.confrelid)
        .or_else(|| catalog.relation_by_oid(row.confrelid))
        .ok_or_else(|| ParseError::UnknownTable(row.confrelid.to_string()))?;
    let referenced_index = catalog
        .index_relations_for_heap(referenced_relation.relation_oid)
        .into_iter()
        .find(|index| index.relation_oid == row.conindid)
        .or_else(|| {
            find_exact_index_for_attnums(
                catalog,
                referenced_relation.relation_oid,
                &referenced_attnums,
                true,
            )
        })
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "referenced foreign-key index",
            actual: format!("missing referenced index {}", row.conindid),
        })?;
    let constraint_name = row.conname.clone();
    Ok(BoundForeignKeyConstraint {
        constraint_oid: row.oid,
        constraint_name: constraint_name.clone(),
        relation_name: relation_display_name(catalog, relation_oid, &relation_oid.to_string()),
        column_names: attnums_to_column_names(desc, &local_attnums)?,
        column_indexes: attnums_to_column_indexes(desc, &local_attnums)?,
        period_column_index: if row.conperiod {
            Some(
                *attnums_to_column_indexes(desc, &local_attnums)?
                    .last()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "foreign-key period column",
                        actual: format!("missing period column for constraint {constraint_name}"),
                    })?,
            )
        } else {
            None
        },
        match_type: foreign_key_match_from_code(row.confmatchtype)?,
        referenced_relation_name: relation_display_name(
            catalog,
            referenced_relation.relation_oid,
            &row.confrelid.to_string(),
        ),
        referenced_relation_oid: referenced_relation.relation_oid,
        referenced_rel: referenced_relation.rel,
        referenced_toast: referenced_relation.toast,
        referenced_desc: referenced_relation.desc.clone(),
        referenced_column_indexes: attnums_to_column_indexes(
            &referenced_relation.desc,
            &referenced_attnums,
        )?,
        referenced_period_column_index: if row.conperiod {
            Some(
                *attnums_to_column_indexes(&referenced_relation.desc, &referenced_attnums)?
                    .last()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "referenced foreign-key period column",
                        actual: format!("missing referenced period column for {constraint_name}"),
                    })?,
            )
        } else {
            None
        },
        referenced_index,
        deferrable: row.condeferrable,
        initially_deferred: row.condeferred,
        enforced: row.conenforced,
    })
}

fn bind_inbound_foreign_key_constraint(
    relation_oid: u32,
    desc: &RelationDesc,
    row: PgConstraintRow,
    catalog: &dyn super::CatalogLookup,
) -> Result<BoundReferencedByForeignKey, ParseError> {
    let referenced_relation = catalog
        .lookup_relation_by_oid(relation_oid)
        .or_else(|| catalog.relation_by_oid(relation_oid))
        .ok_or_else(|| ParseError::UnknownTable(relation_oid.to_string()))?;
    let constraint_referenced_relation = if row.confrelid == relation_oid {
        referenced_relation.clone()
    } else {
        catalog
            .lookup_relation_by_oid(row.confrelid)
            .or_else(|| catalog.relation_by_oid(row.confrelid))
            .ok_or_else(|| ParseError::UnknownTable(row.confrelid.to_string()))?
    };
    let child_relation = catalog
        .lookup_relation_by_oid(row.conrelid)
        .or_else(|| catalog.relation_by_oid(row.conrelid))
        .ok_or_else(|| ParseError::UnknownTable(row.conrelid.to_string()))?;
    let child_attnums = constraint_attnums(&row, "FOREIGN KEY")?;
    let referenced_attnums = row
        .confkey
        .clone()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "referenced foreign-key columns",
            actual: format!("missing confkey for constraint {}", row.conname),
        })?;
    let constraint_name = row.conname.clone();
    let (display_constraint_name, display_child_relation_name) =
        inbound_foreign_key_display_names(relation_oid, &row, &child_relation, catalog);
    let referenced_column_names =
        attnums_to_column_names(&constraint_referenced_relation.desc, &referenced_attnums)?;
    let referenced_column_indexes = column_indexes_for_names(desc, &referenced_column_names)?;
    Ok(BoundReferencedByForeignKey {
        constraint_oid: row.oid,
        constraint_name: constraint_name.clone(),
        display_constraint_name,
        referenced_relation_oid: row.confrelid,
        child_relation_name: relation_display_name(catalog, child_relation.relation_oid, "<child>"),
        display_child_relation_name,
        child_relation_oid: child_relation.relation_oid,
        child_rel: child_relation.rel,
        child_toast: child_relation.toast,
        child_desc: child_relation.desc.clone(),
        child_column_indexes: attnums_to_column_indexes(&child_relation.desc, &child_attnums)?,
        child_period_column_index: if row.conperiod {
            Some(
                *attnums_to_column_indexes(&child_relation.desc, &child_attnums)?
                    .last()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "child foreign-key period column",
                        actual: format!("missing child period column for {constraint_name}"),
                    })?,
            )
        } else {
            None
        },
        referenced_rel: referenced_relation.rel,
        referenced_toast: referenced_relation.toast,
        referenced_desc: desc.clone(),
        referenced_column_names,
        referenced_column_indexes: referenced_column_indexes.clone(),
        referenced_period_column_index: if row.conperiod {
            Some(
                *referenced_column_indexes
                    .last()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "referenced foreign-key period column",
                        actual: format!("missing referenced period column for {constraint_name}"),
                    })?,
            )
        } else {
            None
        },
        child_index: find_exact_index_for_attnums(
            catalog,
            child_relation.relation_oid,
            &child_attnums,
            false,
        ),
        on_delete: foreign_key_action_from_code(row.confdeltype)?,
        on_delete_set_column_indexes: row
            .confdelsetcols
            .as_ref()
            .map(|attnums| attnums_to_column_indexes(&child_relation.desc, attnums))
            .transpose()?,
        on_update: foreign_key_action_from_code(row.confupdtype)?,
        deferrable: row.condeferrable,
        initially_deferred: row.condeferred,
        enforced: row.conenforced,
    })
}

fn inbound_foreign_key_display_names(
    referenced_relation_oid: u32,
    row: &PgConstraintRow,
    child_relation: &super::BoundRelation,
    catalog: &dyn super::CatalogLookup,
) -> (String, String) {
    let default_child_name = relation_display_name(catalog, child_relation.relation_oid, "<child>");
    if is_referenced_side_foreign_key_clone(row, catalog) {
        return (row.conname.clone(), default_child_name);
    }
    let Some(parent_row) = (row.conparentid != 0)
        .then(|| catalog.constraint_row_by_oid(row.conparentid))
        .flatten()
    else {
        return (row.conname.clone(), default_child_name);
    };
    let referenced_root_oid = if catalog
        .relation_by_oid(row.confrelid)
        .is_some_and(|relation| relation.relkind == 'p')
    {
        row.confrelid
    } else {
        parent_row.confrelid
    };
    if referenced_root_oid == 0 || referenced_relation_oid == referenced_root_oid {
        return (row.conname.clone(), default_child_name);
    }
    if !catalog
        .relation_by_oid(referenced_root_oid)
        .is_some_and(|relation| relation.relkind == 'p')
    {
        return (row.conname.clone(), default_child_name);
    }
    let Some(ordinal) =
        partition_child_ordinal_for_relation(catalog, referenced_root_oid, referenced_relation_oid)
    else {
        return (row.conname.clone(), default_child_name);
    };
    let display_child_name = catalog
        .relation_by_oid(parent_row.conrelid)
        .map(|relation| relation_display_name(catalog, relation.relation_oid, "<child>"))
        .unwrap_or(default_child_name);
    (
        format!("{}_{}", parent_row.conname, ordinal),
        display_child_name,
    )
}

fn partition_child_ordinal_for_relation(
    catalog: &dyn super::CatalogLookup,
    root_oid: u32,
    relation_oid: u32,
) -> Option<usize> {
    let mut children = catalog
        .inheritance_children(root_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .filter_map(|row| catalog.relation_by_oid(row.inhrelid))
        .collect::<Vec<_>>();
    children.sort_by(|left, right| {
        partition_bound_sort_key(left)
            .cmp(&partition_bound_sort_key(right))
            .then_with(|| left.relation_oid.cmp(&right.relation_oid))
    });
    children.iter().enumerate().find_map(|(index, child)| {
        (child.relation_oid == relation_oid
            || partition_contains_relation(catalog, child.relation_oid, relation_oid))
        .then_some(index + 1)
    })
}

fn partition_bound_sort_key(relation: &super::BoundRelation) -> (bool, String) {
    let bound = relation
        .relpartbound
        .clone()
        .unwrap_or_else(|| relation.relation_oid.to_string());
    let is_default = partition_bound_text_is_default(&bound);
    (is_default, bound)
}

fn partition_bound_text_is_default(bound: &str) -> bool {
    let lower_bound = bound.to_ascii_lowercase();
    if lower_bound.contains("\"is_default\":true")
        || lower_bound.contains("\"is_default\": true")
        || lower_bound.contains("default")
    {
        return true;
    }
    serde_json::from_str::<serde_json::Value>(bound)
        .ok()
        .is_some_and(|value| json_value_has_true_is_default(&value))
}

fn json_value_has_true_is_default(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            map.get("is_default")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
                || map.values().any(json_value_has_true_is_default)
        }
        serde_json::Value::Array(values) => values.iter().any(json_value_has_true_is_default),
        _ => false,
    }
}

fn partition_contains_relation(
    catalog: &dyn super::CatalogLookup,
    root_oid: u32,
    relation_oid: u32,
) -> bool {
    catalog
        .inheritance_children(root_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .any(|row| {
            row.inhrelid == relation_oid
                || partition_contains_relation(catalog, row.inhrelid, relation_oid)
        })
}

fn merge_not_null_constraint(
    not_nulls: &mut BTreeMap<String, PendingNotNullConstraint>,
    column_lookup: &BTreeMap<String, usize>,
    column_name: &str,
    attributes: &ConstraintAttributes,
    primary_key_owned: bool,
    relation_name: &str,
) -> Result<(), ParseError> {
    let normalized = column_name.to_ascii_lowercase();
    let Some(&column_index) = column_lookup.get(&normalized) else {
        return Err(ParseError::UnknownColumn(column_name.to_string()));
    };

    let entry = not_nulls
        .entry(normalized)
        .or_insert_with(|| PendingNotNullConstraint {
            explicit_name: None,
            generated_base: GeneratedConstraintName::new(
                relation_name,
                Some(column_name.to_string()),
                "not_null",
            ),
            column: column_name.to_string(),
            not_valid: attributes.not_valid,
            no_inherit: attributes.no_inherit,
            primary_key_owned,
            column_index,
        });

    if let (Some(existing), Some(incoming)) =
        (entry.explicit_name.as_deref(), attributes.name.as_deref())
        && !existing.eq_ignore_ascii_case(incoming)
    {
        return Err(ParseError::InvalidTableDefinition(format!(
            "conflicting not-null constraint names \"{existing}\" and \"{incoming}\""
        )));
    }

    if entry.explicit_name.is_none() {
        entry.explicit_name = attributes.name.clone();
    }
    if entry.no_inherit != attributes.no_inherit {
        return Err(ParseError::InvalidTableDefinition(format!(
            "conflicting NO INHERIT declaration for not-null constraint on column \"{column_name}\""
        )));
    }
    entry.not_valid &= attributes.not_valid;
    entry.primary_key_owned &= primary_key_owned;
    Ok(())
}

fn resolve_constraint_columns(
    referenced: &[String],
    columns: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(referenced.len());
    for column in referenced {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            return Err(ParseError::UnexpectedToken {
                expected: "unique column names in table constraint",
                actual: format!("duplicate column in constraint: {column}"),
            });
        }
        let index = column_lookup
            .get(&normalized)
            .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
        resolved.push(columns[*index].name.clone());
    }
    Ok(resolved)
}

fn resolve_foreign_key_columns(
    referenced: &[String],
    period: Option<&str>,
    columns: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    let period = period.map(str::to_ascii_lowercase);
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(referenced.len());
    for column in referenced {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) && period.as_deref() != Some(normalized.as_str()) {
            return Err(ParseError::UnexpectedToken {
                expected: "unique column names in foreign key",
                actual: format!("duplicate column in constraint: {column}"),
            });
        }
        let index = foreign_key_column_index(column_lookup, column)?;
        resolved.push(columns[index].name.clone());
    }
    Ok(resolved)
}

fn resolve_relation_foreign_key_columns(
    referenced: &[String],
    period: Option<&str>,
    desc: &RelationDesc,
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    let period = period.map(str::to_ascii_lowercase);
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(referenced.len());
    for column in referenced {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) && period.as_deref() != Some(normalized.as_str()) {
            return Err(ParseError::UnexpectedToken {
                expected: "unique column names in foreign key",
                actual: format!("duplicate column in constraint: {column}"),
            });
        }
        let index = foreign_key_column_index(column_lookup, column)?;
        let desc_column = &desc.columns[index];
        if desc_column.dropped {
            return Err(foreign_key_column_missing_error(column));
        }
        resolved.push(desc_column.name.clone());
    }
    Ok(resolved)
}

fn foreign_key_column_index(
    column_lookup: &BTreeMap<String, usize>,
    column: &str,
) -> Result<usize, ParseError> {
    let normalized = column.to_ascii_lowercase();
    column_lookup
        .get(&normalized)
        .copied()
        .ok_or_else(|| foreign_key_column_lookup_error(column))
}

fn foreign_key_column_lookup_error(column: &str) -> ParseError {
    if is_system_column_name(column) {
        return ParseError::DetailedError {
            message: "system columns cannot be used in foreign keys".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        };
    }
    foreign_key_column_missing_error(column)
}

fn foreign_key_column_missing_error(column: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("column \"{column}\" referenced in foreign key constraint does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42703",
    }
}

fn is_system_column_name(column: &str) -> bool {
    matches!(
        column.to_ascii_lowercase().as_str(),
        "tableoid" | "ctid" | "xmin" | "cmin" | "xmax" | "cmax" | "oid"
    )
}

fn resolve_index_constraint_columns(
    referenced: &[String],
    without_overlaps: Option<&str>,
    columns: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    resolve_constraint_columns(referenced, columns, column_lookup).map_err(|err| {
        match (&err, without_overlaps) {
            (ParseError::UnknownColumn(column), Some(period_column))
                if column.eq_ignore_ascii_case(period_column) =>
            {
                ParseError::MissingKeyColumn(column.clone())
            }
            _ => err,
        }
    })
}

fn validate_without_overlaps_column(
    columns: &[String],
    without_overlaps: Option<&str>,
    column_defs: &[crate::backend::parser::ColumnDef],
    column_lookup: &BTreeMap<String, usize>,
    catalog: &dyn super::CatalogLookup,
) -> Result<(), ParseError> {
    let Some(period_column) = without_overlaps else {
        return Ok(());
    };
    validate_without_overlaps_shape(columns, period_column)?;
    let index = *column_lookup
        .get(&period_column.to_ascii_lowercase())
        .ok_or_else(|| ParseError::MissingKeyColumn(period_column.to_string()))?;
    let sql_type = super::resolve_raw_type_name(&column_defs[index].ty, catalog)?;
    if !sql_type.is_range() && !sql_type.is_multirange() {
        return Err(ParseError::DetailedError {
            message: format!(
                "column \"{}\" in WITHOUT OVERLAPS is not a range or multirange type",
                column_defs[index].name
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(())
}

fn validate_without_overlaps_relation_column(
    columns: &[String],
    without_overlaps: Option<&str>,
    desc: &RelationDesc,
    column_lookup: &BTreeMap<String, usize>,
) -> Result<(), ParseError> {
    let Some(period_column) = without_overlaps else {
        return Ok(());
    };
    validate_without_overlaps_shape(columns, period_column)?;
    let index = *column_lookup
        .get(&period_column.to_ascii_lowercase())
        .ok_or_else(|| ParseError::MissingKeyColumn(period_column.to_string()))?;
    let column = &desc.columns[index];
    if column.dropped {
        return Err(ParseError::MissingKeyColumn(period_column.to_string()));
    }
    if !column.sql_type.is_range() && !column.sql_type.is_multirange() {
        return Err(ParseError::DetailedError {
            message: format!(
                "column \"{}\" in WITHOUT OVERLAPS is not a range or multirange type",
                column.name
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(())
}

fn validate_without_overlaps_shape(
    columns: &[String],
    period_column: &str,
) -> Result<(), ParseError> {
    if columns.len() < 2 {
        return Err(ParseError::DetailedError {
            message: "constraint using WITHOUT OVERLAPS needs at least two columns".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    let Some(last) = columns.last() else {
        return Err(ParseError::UnexpectedEof);
    };
    if !last.eq_ignore_ascii_case(period_column) {
        return Err(ParseError::DetailedError {
            message: "WITHOUT OVERLAPS column must be the last column in the constraint".into(),
            detail: None,
            hint: None,
            sqlstate: "42601",
        });
    }
    Ok(())
}

fn resolve_relation_constraint_columns(
    referenced: &[String],
    desc: &RelationDesc,
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(referenced.len());
    for column in referenced {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            return Err(ParseError::UnexpectedToken {
                expected: "unique column names in table constraint",
                actual: format!("duplicate column in constraint: {column}"),
            });
        }
        let index = column_lookup
            .get(&normalized)
            .ok_or_else(|| ParseError::UnknownColumn(column.clone()))?;
        let desc_column = &desc.columns[*index];
        if desc_column.dropped {
            return Err(ParseError::UnknownColumn(column.clone()));
        }
        resolved.push(desc_column.name.clone());
    }
    Ok(resolved)
}

fn index_columns_for_existing_index(
    table_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    index_name: &str,
    catalog: &dyn super::CatalogLookup,
) -> Result<(Vec<String>, Vec<String>, bool), ParseError> {
    let index = catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "existing index on table",
            actual: format!("index \"{index_name}\" does not exist for table \"{table_name}\""),
        })?;
    if !index.index_meta.indisunique {
        return Err(ParseError::DetailedError {
            message: format!("\"{index_name}\" is not a unique index"),
            detail: Some(
                "Cannot create a primary key or unique constraint using such an index.".into(),
            ),
            hint: None,
            sqlstate: "42809",
        });
    }
    if catalog
        .class_row_by_oid(index.relation_oid)
        .is_some_and(|row| row.relkind == 'I')
    {
        return Err(ParseError::UnexpectedToken {
            expected: "non-partitioned index",
            actual:
                "ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables"
                    .into(),
        });
    }
    if index
        .index_meta
        .indpred
        .as_deref()
        .is_some_and(|pred| !pred.trim().is_empty())
    {
        return Err(ParseError::UnexpectedToken {
            expected: "non-partial index",
            actual: format!("index \"{index_name}\" contains a predicate"),
        });
    }
    if let Some(column_index) = index
        .index_meta
        .indoption
        .iter()
        .position(|option| *option != 0)
    {
        return Err(ParseError::DetailedError {
            message: format!(
                "index \"{index_name}\" column number {} does not have default sorting behavior",
                column_index + 1
            ),
            detail: Some(
                "Cannot create a primary key or unique constraint using such an index.".into(),
            ),
            hint: None,
            sqlstate: "42809",
        });
    }
    if let Some(column_index) = index
        .index_meta
        .indcollation
        .iter()
        .position(|collation_oid| *collation_oid != 0)
    {
        return Err(ParseError::DetailedError {
            message: format!(
                "index \"{index_name}\" column number {} does not have default sorting behavior",
                column_index + 1
            ),
            detail: Some(
                "Cannot create a primary key or unique constraint using such an index.".into(),
            ),
            hint: None,
            sqlstate: "42809",
        });
    }
    let mut all_columns = Vec::with_capacity(index.index_meta.indkey.len());
    for attnum in &index.index_meta.indkey {
        if *attnum <= 0 {
            return Err(ParseError::UnexpectedToken {
                expected: "simple column index",
                actual: format!("index \"{index_name}\" contains expressions"),
            });
        }
        let column = desc
            .columns
            .get((*attnum as usize).saturating_sub(1))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "index column",
                actual: format!("index \"{index_name}\" has invalid column reference"),
            })?;
        all_columns.push(column.name.clone());
    }
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0)).unwrap_or_default();
    let include_columns = all_columns.split_off(key_count.min(all_columns.len()));
    Ok((
        all_columns,
        include_columns,
        index.index_meta.indnullsnotdistinct,
    ))
}

fn resolve_relation_index_constraint_columns(
    referenced: &[String],
    without_overlaps: Option<&str>,
    desc: &RelationDesc,
    column_lookup: &BTreeMap<String, usize>,
) -> Result<Vec<String>, ParseError> {
    resolve_relation_constraint_columns(referenced, desc, column_lookup).map_err(|err| {
        match (&err, without_overlaps) {
            (ParseError::UnknownColumn(column), Some(period_column))
                if column.eq_ignore_ascii_case(period_column) =>
            {
                ParseError::MissingKeyColumn(column.clone())
            }
            _ => err,
        }
    })
}

fn relation_column_lookup(desc: &RelationDesc) -> BTreeMap<String, usize> {
    desc.columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !column.dropped)
        .map(|(index, column)| (column.name.to_ascii_lowercase(), index))
        .collect()
}

fn multiple_primary_keys_error(table_name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("multiple primary keys for table \"{table_name}\" are not allowed"),
        detail: None,
        hint: None,
        sqlstate: "42P16",
    }
}

fn relation_display_name(
    catalog: &dyn super::CatalogLookup,
    relation_oid: u32,
    fallback: &str,
) -> String {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| fallback.to_string())
}

fn column_attnums_for_names(
    desc: &RelationDesc,
    columns: &[String],
) -> Result<Vec<i16>, ParseError> {
    let lookup = relation_column_lookup(desc);
    columns
        .iter()
        .map(|column| {
            lookup
                .get(&column.to_ascii_lowercase())
                .copied()
                .map(|index| (index + 1) as i16)
                .ok_or_else(|| ParseError::UnknownColumn(column.clone()))
        })
        .collect()
}

fn constraint_attnums(
    row: &PgConstraintRow,
    _constraint_kind: &str,
) -> Result<Vec<i16>, ParseError> {
    row.conkey
        .clone()
        .filter(|keys| !keys.is_empty())
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "constraint columns",
            actual: format!("missing conkey for constraint {}", row.conname),
        })
}

fn column_index_for_attnum(desc: &RelationDesc, attnum: i16) -> Result<usize, ParseError> {
    let index =
        usize::try_from(attnum.saturating_sub(1)).map_err(|_| ParseError::UnexpectedToken {
            expected: "user column attnum",
            actual: format!("invalid attnum {attnum}"),
        })?;
    desc.columns
        .get(index)
        .filter(|column| !column.dropped)
        .map(|_| index)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "user column attnum",
            actual: format!("invalid attnum {attnum}"),
        })
}

fn attnums_to_column_indexes(
    desc: &RelationDesc,
    attnums: &[i16],
) -> Result<Vec<usize>, ParseError> {
    attnums
        .iter()
        .map(|&attnum| column_index_for_attnum(desc, attnum))
        .collect()
}

fn attnums_to_column_names(
    desc: &RelationDesc,
    attnums: &[i16],
) -> Result<Vec<String>, ParseError> {
    attnums
        .iter()
        .map(|&attnum| {
            let index = column_index_for_attnum(desc, attnum)?;
            Ok(desc.columns[index].name.clone())
        })
        .collect()
}

fn column_indexes_for_names(
    desc: &RelationDesc,
    names: &[String],
) -> Result<Vec<usize>, ParseError> {
    names
        .iter()
        .map(|name| {
            desc.columns
                .iter()
                .position(|column| !column.dropped && column.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| ParseError::UnknownColumn(name.clone()))
        })
        .collect()
}

fn find_exact_index_for_attnums(
    catalog: &dyn super::CatalogLookup,
    relation_oid: u32,
    attnums: &[i16],
    unique_required: bool,
) -> Option<super::BoundIndexRelation> {
    catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| {
            (!unique_required || index.index_meta.indisunique)
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.am_oid == crate::include::catalog::BTREE_AM_OID
                && index.index_meta.indkey == attnums
                && !index
                    .index_meta
                    .indpred
                    .as_deref()
                    .is_some_and(|pred| !pred.is_empty())
                && !index
                    .index_meta
                    .indexprs
                    .as_deref()
                    .is_some_and(|exprs| !exprs.is_empty())
        })
}

fn find_compatible_unique_index_for_attnums(
    catalog: &dyn super::CatalogLookup,
    relation_oid: u32,
    attnums: &[i16],
) -> Option<super::BoundIndexRelation> {
    catalog
        .index_relations_for_heap(relation_oid)
        .into_iter()
        .find(|index| {
            index.index_meta.indisunique
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.am_oid == crate::include::catalog::BTREE_AM_OID
                && index_key_attnums(index).is_some_and(|key_attnums| {
                    attnum_key_columns_match_as_set(&key_attnums, attnums)
                })
                && !index
                    .index_meta
                    .indpred
                    .as_deref()
                    .is_some_and(|pred| !pred.is_empty())
                && !index
                    .index_meta
                    .indexprs
                    .as_deref()
                    .is_some_and(|exprs| !exprs.is_empty())
        })
}

fn index_key_attnums(index: &super::BoundIndexRelation) -> Option<Vec<i16>> {
    let key_count = usize::try_from(index.index_meta.indnkeyatts.max(0)).ok()?;
    if key_count > index.index_meta.indkey.len() {
        return None;
    }
    Some(
        index
            .index_meta
            .indkey
            .iter()
            .take(key_count)
            .copied()
            .collect(),
    )
}

fn string_key_columns_match_as_set(left: &[String], right: &[String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let left = left
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let right = right
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    left.len() == right.len() && left == right
}

fn attnum_key_columns_match_as_set(left: &[i16], right: &[i16]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let left = left.iter().copied().collect::<BTreeSet<_>>();
    let right = right.iter().copied().collect::<BTreeSet<_>>();
    left.len() == right.len() && left == right
}

fn foreign_key_match_keyword(match_type: ForeignKeyMatchType) -> &'static str {
    match match_type {
        ForeignKeyMatchType::Simple => "SIMPLE",
        ForeignKeyMatchType::Full => "FULL",
        ForeignKeyMatchType::Partial => "PARTIAL",
    }
}

fn foreign_key_action_from_code(code: char) -> Result<ForeignKeyAction, ParseError> {
    match code {
        'a' | ' ' => Ok(ForeignKeyAction::NoAction),
        'r' => Ok(ForeignKeyAction::Restrict),
        'c' => Ok(ForeignKeyAction::Cascade),
        'n' => Ok(ForeignKeyAction::SetNull),
        'd' => Ok(ForeignKeyAction::SetDefault),
        other => Err(ParseError::UnexpectedToken {
            expected: "foreign-key action code",
            actual: other.to_string(),
        }),
    }
}

fn foreign_key_match_from_code(code: char) -> Result<ForeignKeyMatchType, ParseError> {
    match code {
        's' | ' ' => Ok(ForeignKeyMatchType::Simple),
        'f' => Ok(ForeignKeyMatchType::Full),
        'p' => Ok(ForeignKeyMatchType::Partial),
        other => Err(ParseError::UnexpectedToken {
            expected: "foreign-key match code",
            actual: other.to_string(),
        }),
    }
}

fn existing_constraint_names(existing_constraints: &[PgConstraintRow]) -> BTreeSet<String> {
    existing_constraints
        .iter()
        .map(|row| row.conname.to_ascii_lowercase())
        .collect()
}

fn assign_constraint_name(
    explicit_name: Option<String>,
    generated_base: GeneratedConstraintName,
    used_names: &mut BTreeSet<String>,
) -> Result<String, ParseError> {
    if let Some(name) = explicit_name {
        if !used_names.insert(name.to_ascii_lowercase()) {
            return Err(ParseError::UnexpectedToken {
                expected: "distinct constraint names",
                actual: format!("duplicate constraint name: {name}"),
            });
        }
        Ok(name)
    } else {
        Ok(choose_generated_constraint_name(
            &generated_base,
            used_names,
        ))
    }
}

const MAX_IDENTIFIER_BYTES: usize = 63;

fn reserve_explicit_constraint_names<'a>(
    used_names: &mut BTreeSet<String>,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), ParseError> {
    for name in names {
        let normalized = name.to_ascii_lowercase();
        if !used_names.insert(normalized) {
            return Err(ParseError::UnexpectedToken {
                expected: "distinct constraint names",
                actual: format!("duplicate constraint name: {name}"),
            });
        }
    }
    Ok(())
}

fn choose_generated_constraint_name(
    base: &GeneratedConstraintName,
    used_names: &mut BTreeSet<String>,
) -> String {
    for suffix in std::iter::once(String::new()).chain((1..).map(|pass| pass.to_string())) {
        let candidate = make_generated_constraint_name(base, &suffix);
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }

    unreachable!("constraint name suffix space exhausted")
}

fn make_generated_constraint_name(base: &GeneratedConstraintName, label_suffix: &str) -> String {
    let label = format!("{}{}", base.label, label_suffix);
    let name2 = base.name2.as_deref();
    let overhead =
        if label.is_empty() { 0 } else { label.len() + 1 } + usize::from(name2.is_some());
    let avail = MAX_IDENTIFIER_BYTES.saturating_sub(overhead);
    let mut name1_len = base.name1.len();
    let mut name2_len = name2.map(str::len).unwrap_or_default();

    while name1_len + name2_len > avail {
        if name1_len > name2_len {
            name1_len = name1_len.saturating_sub(1);
        } else {
            name2_len = name2_len.saturating_sub(1);
        }
    }

    let mut name = clip_to_char_boundary(&base.name1, name1_len).to_string();
    if let Some(name2) = name2 {
        name.push('_');
        name.push_str(clip_to_char_boundary(name2, name2_len));
    }
    if !label.is_empty() {
        name.push('_');
        name.push_str(&label);
    }
    name
}

fn clip_to_char_boundary(value: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn reject_unsupported_check_expr(expr: &Expr) -> Result<(), ParseError> {
    reject_unsupported_relation_predicate_expr(expr, false)
}

fn reject_unsupported_index_predicate_expr(expr: &Expr) -> Result<(), ParseError> {
    reject_unsupported_relation_predicate_expr(expr, true)
}

fn reject_unsupported_relation_predicate_expr(
    expr: &Expr,
    allow_non_tableoid_system_columns: bool,
) -> Result<(), ParseError> {
    match expr {
        Expr::Aggref(_) => Err(ParseError::FeatureNotSupported(
            "aggregate functions in CHECK constraints".into(),
        )),
        Expr::WindowFunc(_) => Err(ParseError::FeatureNotSupported(
            "window functions in CHECK constraints".into(),
        )),
        Expr::SetReturning(_) => Err(ParseError::FeatureNotSupported(
            "set-returning functions in CHECK constraints".into(),
        )),
        Expr::SubLink(_) | Expr::SubPlan(_) => Err(ParseError::FeatureNotSupported(
            "subqueries in CHECK constraints".into(),
        )),
        Expr::Var(var) if var.varlevelsup > 0 => Err(ParseError::FeatureNotSupported(
            "outer references in CHECK constraints".into(),
        )),
        Expr::GroupingKey(grouping_key) => reject_unsupported_relation_predicate_expr(
            &grouping_key.expr,
            allow_non_tableoid_system_columns,
        ),
        Expr::GroupingFunc(grouping_func) => {
            for arg in &grouping_func.args {
                reject_unsupported_relation_predicate_expr(arg, allow_non_tableoid_system_columns)?;
            }
            Ok(())
        }
        Expr::Var(var)
            if !allow_non_tableoid_system_columns
                && var.varattno < 0
                && var.varattno != TABLE_OID_ATTR_NO =>
        {
            let column_name = system_column_name(var.varattno).unwrap_or("unknown");
            Err(ParseError::DetailedError {
                message: format!(
                    "system column \"{column_name}\" reference in check constraint is invalid"
                ),
                detail: None,
                hint: None,
                sqlstate: "42P16",
            })
        }
        Expr::Op(op) => {
            for arg in &op.args {
                reject_unsupported_relation_predicate_expr(arg, allow_non_tableoid_system_columns)?;
            }
            Ok(())
        }
        Expr::Bool(expr) => {
            for arg in &expr.args {
                reject_unsupported_relation_predicate_expr(arg, allow_non_tableoid_system_columns)?;
            }
            Ok(())
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                reject_unsupported_relation_predicate_expr(arg, allow_non_tableoid_system_columns)?;
            }
            for arm in &case_expr.args {
                reject_unsupported_relation_predicate_expr(
                    &arm.expr,
                    allow_non_tableoid_system_columns,
                )?;
                reject_unsupported_relation_predicate_expr(
                    &arm.result,
                    allow_non_tableoid_system_columns,
                )?;
            }
            reject_unsupported_relation_predicate_expr(
                &case_expr.defresult,
                allow_non_tableoid_system_columns,
            )
        }
        Expr::CaseTest(_) => Ok(()),
        Expr::Func(func) => {
            for arg in &func.args {
                reject_unsupported_relation_predicate_expr(arg, allow_non_tableoid_system_columns)?;
            }
            Ok(())
        }
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                reject_unsupported_relation_predicate_expr(
                    child,
                    allow_non_tableoid_system_columns,
                )?;
            }
            Ok(())
        }
        Expr::ScalarArrayOp(expr) => {
            reject_unsupported_relation_predicate_expr(
                &expr.left,
                allow_non_tableoid_system_columns,
            )?;
            reject_unsupported_relation_predicate_expr(
                &expr.right,
                allow_non_tableoid_system_columns,
            )
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            reject_unsupported_relation_predicate_expr(inner, allow_non_tableoid_system_columns)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            reject_unsupported_relation_predicate_expr(expr, allow_non_tableoid_system_columns)?;
            reject_unsupported_relation_predicate_expr(pattern, allow_non_tableoid_system_columns)?;
            if let Some(escape) = escape {
                reject_unsupported_relation_predicate_expr(
                    escape,
                    allow_non_tableoid_system_columns,
                )?;
            }
            Ok(())
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            reject_unsupported_relation_predicate_expr(left, allow_non_tableoid_system_columns)?;
            reject_unsupported_relation_predicate_expr(right, allow_non_tableoid_system_columns)
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                reject_unsupported_relation_predicate_expr(
                    element,
                    allow_non_tableoid_system_columns,
                )?;
            }
            Ok(())
        }
        Expr::Row { fields, .. } => {
            for (_, field) in fields {
                reject_unsupported_relation_predicate_expr(
                    field,
                    allow_non_tableoid_system_columns,
                )?;
            }
            Ok(())
        }
        Expr::FieldSelect { expr, .. } => {
            reject_unsupported_relation_predicate_expr(expr, allow_non_tableoid_system_columns)
        }
        Expr::ArraySubscript { array, subscripts } => {
            reject_unsupported_relation_predicate_expr(array, allow_non_tableoid_system_columns)?;
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    reject_unsupported_relation_predicate_expr(
                        lower,
                        allow_non_tableoid_system_columns,
                    )?;
                }
                if let Some(upper) = &subscript.upper {
                    reject_unsupported_relation_predicate_expr(
                        upper,
                        allow_non_tableoid_system_columns,
                    )?;
                }
            }
            Ok(())
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                reject_unsupported_relation_predicate_expr(
                    child,
                    allow_non_tableoid_system_columns,
                )?;
            }
            Ok(())
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => Ok(()),
    }
}

fn system_column_name(varattno: i32) -> Option<&'static str> {
    match varattno {
        TABLE_OID_ATTR_NO => Some("tableoid"),
        SELF_ITEM_POINTER_ATTR_NO => Some("ctid"),
        XMIN_ATTR_NO => Some("xmin"),
        CMIN_ATTR_NO => Some("cmin"),
        XMAX_ATTR_NO => Some("xmax"),
        CMAX_ATTR_NO => Some("cmax"),
        _ => None,
    }
}
