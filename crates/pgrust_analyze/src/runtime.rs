use std::cell::RefCell;
use std::cmp::Ordering;

use pgrust_nodes::datum::Value;
use pgrust_nodes::parsenodes::{ParseError, Query, SelectStatement, SqlType};
use pgrust_nodes::pathnodes::PlannerConfig;
use pgrust_nodes::plannodes::PlannedStmt;
use pgrust_nodes::primnodes::{Expr, RelationDesc};

use crate::CatalogLookup;
use crate::rewrite::{
    ResolvedAutoViewTarget, TargetRlsState, ViewDmlEvent, ViewDmlRewriteError,
    ViewRuleEventClassification,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormattedTriggerDefinition {
    pub definition: String,
    pub event_manipulations: Vec<&'static str>,
    pub action_condition: Option<String>,
    pub action_statement: String,
    pub action_orientation: &'static str,
    pub action_timing: &'static str,
    pub action_reference_old_table: Option<String>,
    pub action_reference_new_table: Option<String>,
}

pub trait AnalyzeServices: Sync {
    fn cast_value(&self, value: Value, ty: SqlType) -> Result<Value, ParseError> {
        default_cast_value(value, ty)
    }

    fn cast_value_with_source_type(
        &self,
        value: Value,
        source_type: Option<SqlType>,
        ty: SqlType,
        catalog: Option<&dyn CatalogLookup>,
    ) -> Result<Value, ParseError> {
        let _ = (source_type, catalog);
        self.cast_value(value, ty)
    }

    fn compare_order_values(
        &self,
        left: &Value,
        right: &Value,
        collation_oid: Option<u32>,
        nulls_first: Option<bool>,
        descending: bool,
    ) -> Result<Ordering, ParseError> {
        default_compare_order_values(left, right, collation_oid, nulls_first, descending)
    }

    fn fold_expr_constants(&self, expr: Expr) -> Result<Expr, ParseError> {
        Ok(expr)
    }

    fn fold_query_constants(&self, query: Query) -> Result<Query, ParseError> {
        Ok(query)
    }

    fn planner_with_config(
        &self,
        query: Query,
        catalog: &dyn CatalogLookup,
        config: PlannerConfig,
    ) -> Result<PlannedStmt, ParseError>;

    fn pg_rewrite_query(
        &self,
        query: Query,
        catalog: &dyn CatalogLookup,
    ) -> Result<Vec<Query>, ParseError> {
        let _ = catalog;
        Ok(vec![query])
    }

    fn render_relation_expr_sql(
        &self,
        expr: &Expr,
        relation_name: Option<&str>,
        desc: &RelationDesc,
        catalog: &dyn CatalogLookup,
    ) -> String {
        let _ = (relation_name, desc, catalog);
        format!("{expr:?}")
    }

    fn render_relation_expr_sql_for_information_schema(
        &self,
        expr: &Expr,
        relation_name: Option<&str>,
        desc: &RelationDesc,
        catalog: &dyn CatalogLookup,
    ) -> String {
        self.render_relation_expr_sql(expr, relation_name, desc, catalog)
    }

    fn format_view_definition(
        &self,
        relation_oid: u32,
        relation_desc: &RelationDesc,
        catalog: &dyn CatalogLookup,
    ) -> Result<String, ParseError> {
        let _ = (relation_oid, relation_desc, catalog);
        Err(ParseError::FeatureNotSupported(
            "view deparsing requires root analyze services".into(),
        ))
    }

    fn split_stored_view_definition_sql<'a>(
        &self,
        sql: &'a str,
    ) -> (&'a str, pgrust_nodes::parsenodes::ViewCheckOption) {
        split_stored_view_definition_sql(sql)
    }

    fn load_view_return_query(
        &self,
        relation_oid: u32,
        relation_desc: &RelationDesc,
        alias: Option<&str>,
        catalog: &dyn CatalogLookup,
        expanded_views: &[u32],
    ) -> Result<Query, ParseError> {
        let _ = (relation_oid, relation_desc, alias, catalog, expanded_views);
        Err(ParseError::FeatureNotSupported(
            "view expansion requires root analyze services".into(),
        ))
    }

    fn load_view_return_select(
        &self,
        relation_oid: u32,
        alias: Option<&str>,
        catalog: &dyn CatalogLookup,
        expanded_views: &[u32],
    ) -> Result<SelectStatement, ParseError> {
        let _ = (relation_oid, alias, catalog, expanded_views);
        Err(ParseError::FeatureNotSupported(
            "view expansion requires root analyze services".into(),
        ))
    }

    fn classify_view_dml_rules(
        &self,
        relation_oid: u32,
        event: ViewDmlEvent,
        catalog: &dyn CatalogLookup,
    ) -> ViewRuleEventClassification {
        let _ = (relation_oid, event, catalog);
        ViewRuleEventClassification::default()
    }

    fn resolve_auto_updatable_view_target(
        &self,
        relation_oid: u32,
        relation_desc: &RelationDesc,
        event: ViewDmlEvent,
        catalog: &dyn CatalogLookup,
        expanded_views: &[u32],
    ) -> Result<ResolvedAutoViewTarget, ViewDmlRewriteError> {
        let _ = (relation_oid, relation_desc, event, catalog, expanded_views);
        Err(ViewDmlRewriteError::DeferredFeature(
            "auto-updatable view rewrite requires root analyze services".into(),
        ))
    }

    fn relation_has_row_security(&self, relation_oid: u32, catalog: &dyn CatalogLookup) -> bool {
        catalog
            .class_row_by_oid(relation_oid)
            .is_some_and(|row| row.relrowsecurity)
    }

    fn relation_has_security_invoker(
        &self,
        relation_oid: u32,
        catalog: &dyn CatalogLookup,
    ) -> bool {
        catalog
            .class_row_by_oid(relation_oid)
            .and_then(|row| row.reloptions)
            .is_some_and(|options| {
                options.iter().any(|option| {
                    let (name, value) = option
                        .split_once('=')
                        .map(|(name, value)| (name, value))
                        .unwrap_or((option.as_str(), "true"));
                    name.eq_ignore_ascii_case("security_invoker")
                        && matches!(value.to_ascii_lowercase().as_str(), "true" | "on")
                })
            })
    }

    fn apply_query_row_security(
        &self,
        query: &mut Query,
        catalog: &dyn CatalogLookup,
    ) -> Result<(), ParseError> {
        let _ = (query, catalog);
        Ok(())
    }

    fn build_target_relation_row_security(
        &self,
        relation_name: &str,
        relation_oid: u32,
        desc: &RelationDesc,
        command: pgrust_core::PolicyCommand,
        include_select_visibility: bool,
        include_select_check: bool,
        catalog: &dyn CatalogLookup,
    ) -> Result<TargetRlsState, ParseError> {
        let _ = (
            relation_name,
            relation_oid,
            desc,
            command,
            include_select_visibility,
            include_select_check,
            catalog,
        );
        Ok(TargetRlsState {
            visibility_quals: Vec::new(),
            write_checks: Vec::new(),
            depends_on_row_security: false,
        })
    }

    fn build_target_relation_row_security_for_user(
        &self,
        relation_name: &str,
        relation_oid: u32,
        desc: &RelationDesc,
        command: pgrust_core::PolicyCommand,
        include_select_visibility: bool,
        include_select_check: bool,
        user_oid: u32,
        catalog: &dyn CatalogLookup,
    ) -> Result<TargetRlsState, ParseError> {
        let _ = user_oid;
        self.build_target_relation_row_security(
            relation_name,
            relation_oid,
            desc,
            command,
            include_select_visibility,
            include_select_check,
            catalog,
        )
    }

    fn current_timestamp_value(&self, precision: Option<i32>, with_time_zone: bool) -> Value {
        let _ = precision;
        if with_time_zone {
            Value::TimestampTz(pgrust_nodes::datetime::TimestampTzADT(0))
        } else {
            Value::Timestamp(pgrust_nodes::datetime::TimestampADT(0))
        }
    }

    fn eval_to_char_function(&self, values: &[Value]) -> Result<Value, ParseError> {
        let _ = values;
        Err(ParseError::FeatureNotSupported(
            "to_char evaluation requires root analyze services".into(),
        ))
    }

    fn format_trigger_definition(
        &self,
        row: &pgrust_catalog_data::PgTriggerRow,
        relation_name: Option<&str>,
        catalog: &dyn CatalogLookup,
    ) -> Option<FormattedTriggerDefinition> {
        let _ = (relation_name, catalog);
        Some(FormattedTriggerDefinition {
            definition: row.tgname.clone(),
            event_manipulations: Vec::new(),
            action_condition: None,
            action_statement: String::new(),
            action_orientation: "ROW",
            action_timing: "BEFORE",
            action_reference_old_table: None,
            action_reference_new_table: None,
        })
    }
}

struct DefaultAnalyzeServices;

impl AnalyzeServices for DefaultAnalyzeServices {
    fn planner_with_config(
        &self,
        query: Query,
        catalog: &dyn CatalogLookup,
        config: PlannerConfig,
    ) -> Result<PlannedStmt, ParseError> {
        let _ = (query, catalog, config);
        Err(ParseError::FeatureNotSupported(
            "planning requires root analyze services".into(),
        ))
    }
}

static DEFAULT_SERVICES: DefaultAnalyzeServices = DefaultAnalyzeServices;

thread_local! {
    static SERVICE_STACK: RefCell<Vec<&'static dyn AnalyzeServices>> = const { RefCell::new(Vec::new()) };
}

pub fn with_analyze_services<T>(
    services: &'static dyn AnalyzeServices,
    f: impl FnOnce() -> T,
) -> T {
    SERVICE_STACK.with(|stack| stack.borrow_mut().push(services));
    let result = f();
    SERVICE_STACK.with(|stack| {
        let popped = stack.borrow_mut().pop();
        debug_assert!(popped.is_some());
    });
    result
}

fn with_services<T>(f: impl FnOnce(&dyn AnalyzeServices) -> T) -> T {
    let services =
        SERVICE_STACK.with(|stack| stack.borrow().last().copied().unwrap_or(&DEFAULT_SERVICES));
    f(services)
}

pub fn cast_value(value: Value, ty: SqlType) -> Result<Value, ParseError> {
    with_services(|services| services.cast_value(value, ty))
}

pub fn cast_value_with_source_type(
    value: Value,
    source_type: Option<SqlType>,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ParseError> {
    with_services(|services| services.cast_value_with_source_type(value, source_type, ty, catalog))
}

pub fn compare_order_values(
    left: &Value,
    right: &Value,
    collation_oid: Option<u32>,
    nulls_first: Option<bool>,
    descending: bool,
) -> Result<Ordering, ParseError> {
    with_services(|services| {
        services.compare_order_values(left, right, collation_oid, nulls_first, descending)
    })
}

pub fn fold_expr_constants(expr: Expr) -> Result<Expr, ParseError> {
    with_services(|services| services.fold_expr_constants(expr))
}

pub fn fold_query_constants(query: Query) -> Result<Query, ParseError> {
    with_services(|services| services.fold_query_constants(query))
}

pub fn planner_with_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    with_services(|services| services.planner_with_config(query, catalog, config))
}

pub fn planner(query: Query, catalog: &dyn CatalogLookup) -> Result<PlannedStmt, ParseError> {
    planner_with_config(query, catalog, PlannerConfig::default())
}

pub fn pg_rewrite_query(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Query>, ParseError> {
    with_services(|services| services.pg_rewrite_query(query, catalog))
}

pub fn render_relation_expr_sql(
    expr: &Expr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> String {
    with_services(|services| services.render_relation_expr_sql(expr, relation_name, desc, catalog))
}

pub fn render_relation_expr_sql_for_information_schema(
    expr: &Expr,
    relation_name: Option<&str>,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> String {
    with_services(|services| {
        services.render_relation_expr_sql_for_information_schema(expr, relation_name, desc, catalog)
    })
}

pub fn format_view_definition(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<String, ParseError> {
    with_services(|services| services.format_view_definition(relation_oid, relation_desc, catalog))
}

pub fn load_view_return_query(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    alias: Option<&str>,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<Query, ParseError> {
    with_services(|services| {
        services.load_view_return_query(relation_oid, relation_desc, alias, catalog, expanded_views)
    })
}

pub fn load_view_return_select(
    relation_oid: u32,
    alias: Option<&str>,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<SelectStatement, ParseError> {
    with_services(|services| {
        services.load_view_return_select(relation_oid, alias, catalog, expanded_views)
    })
}

pub fn classify_view_dml_rules(
    relation_oid: u32,
    event: ViewDmlEvent,
    catalog: &dyn CatalogLookup,
) -> ViewRuleEventClassification {
    with_services(|services| services.classify_view_dml_rules(relation_oid, event, catalog))
}

pub fn resolve_auto_updatable_view_target(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    event: ViewDmlEvent,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<ResolvedAutoViewTarget, ViewDmlRewriteError> {
    with_services(|services| {
        services.resolve_auto_updatable_view_target(
            relation_oid,
            relation_desc,
            event,
            catalog,
            expanded_views,
        )
    })
}

pub fn relation_has_row_security(relation_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    with_services(|services| services.relation_has_row_security(relation_oid, catalog))
}

pub fn relation_has_security_invoker(relation_oid: u32, catalog: &dyn CatalogLookup) -> bool {
    with_services(|services| services.relation_has_security_invoker(relation_oid, catalog))
}

pub fn apply_query_row_security(
    query: &mut Query,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    with_services(|services| services.apply_query_row_security(query, catalog))
}

pub fn build_target_relation_row_security(
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    command: pgrust_core::PolicyCommand,
    include_select_visibility: bool,
    include_select_check: bool,
    catalog: &dyn CatalogLookup,
) -> Result<TargetRlsState, ParseError> {
    with_services(|services| {
        services.build_target_relation_row_security(
            relation_name,
            relation_oid,
            desc,
            command,
            include_select_visibility,
            include_select_check,
            catalog,
        )
    })
}

pub fn build_target_relation_row_security_for_user(
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    command: pgrust_core::PolicyCommand,
    include_select_visibility: bool,
    include_select_check: bool,
    user_oid: u32,
    catalog: &dyn CatalogLookup,
) -> Result<TargetRlsState, ParseError> {
    with_services(|services| {
        services.build_target_relation_row_security_for_user(
            relation_name,
            relation_oid,
            desc,
            command,
            include_select_visibility,
            include_select_check,
            user_oid,
            catalog,
        )
    })
}

pub fn current_timestamp_value(precision: Option<i32>, with_time_zone: bool) -> Value {
    with_services(|services| services.current_timestamp_value(precision, with_time_zone))
}

pub fn eval_to_char_function(values: &[Value]) -> Result<Value, ParseError> {
    with_services(|services| services.eval_to_char_function(values))
}

pub fn format_trigger_definition(
    row: &pgrust_catalog_data::PgTriggerRow,
    relation_name: Option<&str>,
    catalog: &dyn CatalogLookup,
) -> Option<FormattedTriggerDefinition> {
    with_services(|services| services.format_trigger_definition(row, relation_name, catalog))
}

pub fn split_stored_view_definition_sql<'a>(
    sql: &'a str,
) -> (&'a str, pgrust_nodes::parsenodes::ViewCheckOption) {
    const CHECK_OPTION_PREFIX: &str = " /* pgrust_check_option=";
    let Some((body, suffix)) = sql.rsplit_once(CHECK_OPTION_PREFIX) else {
        return (sql, pgrust_nodes::parsenodes::ViewCheckOption::None);
    };
    let Some((option, trailing)) = suffix.split_once(" */") else {
        return (sql, pgrust_nodes::parsenodes::ViewCheckOption::None);
    };
    if !trailing.trim().is_empty() {
        return (sql, pgrust_nodes::parsenodes::ViewCheckOption::None);
    }
    let check_option = match option {
        "local" => pgrust_nodes::parsenodes::ViewCheckOption::Local,
        "cascaded" => pgrust_nodes::parsenodes::ViewCheckOption::Cascaded,
        _ => pgrust_nodes::parsenodes::ViewCheckOption::None,
    };
    (body, check_option)
}

fn default_cast_value(value: Value, ty: SqlType) -> Result<Value, ParseError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if ty.is_array {
        return Ok(value);
    }
    match (value, ty.kind) {
        (Value::Int16(value), pgrust_nodes::parsenodes::SqlTypeKind::Int2) => {
            Ok(Value::Int16(value))
        }
        (Value::Int16(value), pgrust_nodes::parsenodes::SqlTypeKind::Int4) => {
            Ok(Value::Int32(value.into()))
        }
        (Value::Int16(value), pgrust_nodes::parsenodes::SqlTypeKind::Int8) => {
            Ok(Value::Int64(value.into()))
        }
        (Value::Int32(value), pgrust_nodes::parsenodes::SqlTypeKind::Int2) => value
            .try_into()
            .map(Value::Int16)
            .map_err(|_| ParseError::InvalidNumeric(value.to_string())),
        (Value::Int32(value), pgrust_nodes::parsenodes::SqlTypeKind::Int4) => {
            Ok(Value::Int32(value))
        }
        (Value::Int32(value), pgrust_nodes::parsenodes::SqlTypeKind::Int8) => {
            Ok(Value::Int64(value.into()))
        }
        (Value::Int64(value), pgrust_nodes::parsenodes::SqlTypeKind::Int2) => value
            .try_into()
            .map(Value::Int16)
            .map_err(|_| ParseError::InvalidNumeric(value.to_string())),
        (Value::Int64(value), pgrust_nodes::parsenodes::SqlTypeKind::Int4) => value
            .try_into()
            .map(Value::Int32)
            .map_err(|_| ParseError::InvalidNumeric(value.to_string())),
        (Value::Int64(value), pgrust_nodes::parsenodes::SqlTypeKind::Int8) => {
            Ok(Value::Int64(value))
        }
        (Value::Float64(value), pgrust_nodes::parsenodes::SqlTypeKind::Float4)
        | (Value::Float64(value), pgrust_nodes::parsenodes::SqlTypeKind::Float8) => {
            Ok(Value::Float64(value))
        }
        (value, _) => Ok(value),
    }
}

fn default_compare_order_values(
    left: &Value,
    right: &Value,
    _collation_oid: Option<u32>,
    nulls_first: Option<bool>,
    descending: bool,
) -> Result<Ordering, ParseError> {
    let nulls_first = nulls_first.unwrap_or(descending);
    let ordering = match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Int16(left), Value::Int16(right)) => left.cmp(right),
        (Value::Int32(left), Value::Int32(right)) => left.cmp(right),
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        _ => {
            return Err(ParseError::FeatureNotSupported(
                "standalone analyzer comparison for this value type".into(),
            ));
        }
    };
    Ok(if descending {
        ordering.reverse()
    } else {
        ordering
    })
}
