use pgrust_catalog_data::PG_CATALOG_NAMESPACE_OID;
use pgrust_nodes::parsenodes::{
    CommonTableExpr, CteBody, FromItem, InsertSource, SelectStatement, UnsupportedStatement,
};
use pgrust_nodes::{Plan, PlannedStmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestrictedRelationInfo {
    pub relation_oid: u32,
    pub relation_name: String,
    pub namespace_oid: u32,
    pub relkind: char,
}

pub trait RestrictedViewCatalog {
    fn lookup_relation_by_name(&self, name: &str) -> Option<RestrictedRelationInfo>;
    fn relation_info_by_oid(&self, relation_oid: u32) -> Option<RestrictedRelationInfo>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestrictedViewError {
    pub relation_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadonlyCreateStatisticsError {
    UnexpectedEof,
    UnsupportedFromClause,
    UnknownTable(String),
    UnsupportedRelation {
        relation_name: String,
        relkind: char,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsupportedStatementExecError {
    SecurityLabel { sql: String },
    AlterTableWithOids,
    FeatureNotSupported { feature: &'static str, sql: String },
}

pub fn unsupported_statement_error(stmt: &UnsupportedStatement) -> UnsupportedStatementExecError {
    if stmt.feature == "SECURITY LABEL" {
        return UnsupportedStatementExecError::SecurityLabel {
            sql: stmt.sql.clone(),
        };
    }
    if stmt.feature == "ALTER TABLE form" {
        let lower = stmt.sql.to_ascii_lowercase();
        if lower.contains(" set with oids") {
            return UnsupportedStatementExecError::AlterTableWithOids;
        }
    }
    UnsupportedStatementExecError::FeatureNotSupported {
        feature: stmt.feature,
        sql: stmt.sql.clone(),
    }
}

#[cfg(test)]
mod unsupported_statement_tests {
    use super::*;

    #[test]
    fn unsupported_statement_error_classifies_special_cases() {
        assert_eq!(
            unsupported_statement_error(&UnsupportedStatement {
                sql: "SECURITY LABEL ON TABLE t IS 'x'".into(),
                feature: "SECURITY LABEL",
            }),
            UnsupportedStatementExecError::SecurityLabel {
                sql: "SECURITY LABEL ON TABLE t IS 'x'".into(),
            }
        );
        assert_eq!(
            unsupported_statement_error(&UnsupportedStatement {
                sql: "ALTER TABLE t SET WITH OIDS".into(),
                feature: "ALTER TABLE form",
            }),
            UnsupportedStatementExecError::AlterTableWithOids
        );
    }
}

pub fn restrict_nonsystem_view_enabled(value: Option<&str>) -> bool {
    value
        .map(|value| {
            value
                .split(',')
                .any(|part| part.trim().trim_matches('\'').eq_ignore_ascii_case("view"))
        })
        .unwrap_or(false)
}

pub fn reject_restricted_views_in_planned_stmt(
    planned_stmt: &PlannedStmt,
    catalog: &dyn RestrictedViewCatalog,
) -> Result<(), RestrictedViewError> {
    for requirement in &planned_stmt.relation_privileges {
        if requirement.relkind != 'v' {
            continue;
        }
        let Some(relation) = catalog.relation_info_by_oid(requirement.relation_oid) else {
            continue;
        };
        reject_restricted_view_info(&relation)?;
    }
    reject_restricted_views_in_plan(&planned_stmt.plan_tree, catalog)?;
    for subplan in &planned_stmt.subplans {
        reject_restricted_views_in_plan(subplan, catalog)?;
    }
    Ok(())
}

pub fn reject_restricted_views_in_plan(
    plan: &Plan,
    catalog: &dyn RestrictedViewCatalog,
) -> Result<(), RestrictedViewError> {
    match plan {
        Plan::Result { .. } | Plan::WorkTableScan { .. } | Plan::FunctionScan { .. } => Ok(()),
        Plan::SeqScan { relation_oid, .. }
        | Plan::TidScan { relation_oid, .. }
        | Plan::IndexOnlyScan { relation_oid, .. }
        | Plan::IndexScan { relation_oid, .. }
        | Plan::BitmapIndexScan { relation_oid, .. } => {
            reject_restricted_view_oid(*relation_oid, catalog)
        }
        Plan::BitmapHeapScan {
            relation_oid,
            bitmapqual,
            ..
        } => {
            reject_restricted_view_oid(*relation_oid, catalog)?;
            reject_restricted_views_in_plan(bitmapqual, catalog)
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => {
            for child in children {
                reject_restricted_views_in_plan(child, catalog)?;
            }
            Ok(())
        }
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => reject_restricted_views_in_plan(input, catalog),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            reject_restricted_views_in_plan(left, catalog)?;
            reject_restricted_views_in_plan(right, catalog)
        }
        Plan::CteScan { cte_plan, .. } => reject_restricted_views_in_plan(cte_plan, catalog),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            reject_restricted_views_in_plan(anchor, catalog)?;
            reject_restricted_views_in_plan(recursive, catalog)
        }
        Plan::Values { .. } => Ok(()),
    }
}

pub fn reject_restricted_views_in_select(
    select: &SelectStatement,
    catalog: &dyn RestrictedViewCatalog,
) -> Result<(), RestrictedViewError> {
    reject_restricted_views_in_select_with_ctes(select, catalog, &mut Vec::new())
}

pub fn validate_readonly_create_statistics(
    from_clause: &str,
    catalog: &dyn RestrictedViewCatalog,
) -> Result<(), ReadonlyCreateStatisticsError> {
    let relation_name = normalize_readonly_statistics_from_clause(from_clause)?;
    match catalog.lookup_relation_by_name(&relation_name) {
        Some(relation) if matches!(relation.relkind, 'r' | 'm' | 'p' | 'f') => Ok(()),
        Some(relation) => Err(ReadonlyCreateStatisticsError::UnsupportedRelation {
            relation_name,
            relkind: relation.relkind,
        }),
        None => Err(ReadonlyCreateStatisticsError::UnknownTable(relation_name)),
    }
}

fn normalize_readonly_statistics_from_clause(
    from_clause: &str,
) -> Result<String, ReadonlyCreateStatisticsError> {
    let input = from_clause.trim();
    if input.is_empty() {
        return Err(ReadonlyCreateStatisticsError::UnexpectedEof);
    }
    if input.contains(char::is_whitespace) || input.contains('(') {
        return Err(ReadonlyCreateStatisticsError::UnsupportedFromClause);
    }
    Ok(input.trim_matches('"').to_ascii_lowercase())
}

fn reject_restricted_view_oid(
    relation_oid: u32,
    catalog: &dyn RestrictedViewCatalog,
) -> Result<(), RestrictedViewError> {
    let Some(relation) = catalog.relation_info_by_oid(relation_oid) else {
        return Ok(());
    };
    reject_restricted_view_info(&relation)
}

fn reject_restricted_view_info(
    relation: &RestrictedRelationInfo,
) -> Result<(), RestrictedViewError> {
    if relation.relkind == 'v' && relation.namespace_oid != PG_CATALOG_NAMESPACE_OID {
        return Err(RestrictedViewError {
            relation_name: relation.relation_name.clone(),
        });
    }
    Ok(())
}

fn reject_restricted_view_access(
    name: &str,
    catalog: &dyn RestrictedViewCatalog,
) -> Result<(), RestrictedViewError> {
    let Some(relation) = catalog.lookup_relation_by_name(name) else {
        return Ok(());
    };
    if relation.relkind == 'v' && relation.namespace_oid != PG_CATALOG_NAMESPACE_OID {
        let relname = if relation.relation_name.is_empty() {
            name.rsplit_once('.')
                .map(|(_, relname)| relname)
                .unwrap_or(name)
                .trim_matches('"')
                .to_string()
        } else {
            relation.relation_name
        };
        return Err(RestrictedViewError {
            relation_name: relname,
        });
    }
    Ok(())
}

fn relation_name_matches_cte(name: &str, visible_ctes: &[String]) -> bool {
    let relname = name
        .rsplit_once('.')
        .map(|(_, relname)| relname)
        .unwrap_or(name)
        .trim_matches('"')
        .to_ascii_lowercase();
    visible_ctes.iter().any(|cte| cte == &relname)
}

fn reject_restricted_views_in_select_with_ctes(
    select: &SelectStatement,
    catalog: &dyn RestrictedViewCatalog,
    visible_ctes: &mut Vec<String>,
) -> Result<(), RestrictedViewError> {
    let outer_cte_count = visible_ctes.len();
    for cte in &select.with {
        reject_restricted_views_in_cte_body(cte, catalog, visible_ctes)?;
        visible_ctes.push(cte.name.to_ascii_lowercase());
    }
    if let Some(from) = &select.from {
        reject_restricted_views_in_from_item(from, catalog, visible_ctes)?;
    }
    if let Some(set_op) = &select.set_operation {
        for input in &set_op.inputs {
            reject_restricted_views_in_select_with_ctes(input, catalog, visible_ctes)?;
        }
    }
    visible_ctes.truncate(outer_cte_count);
    Ok(())
}

fn reject_restricted_views_in_cte_body(
    cte: &CommonTableExpr,
    catalog: &dyn RestrictedViewCatalog,
    visible_ctes: &mut Vec<String>,
) -> Result<(), RestrictedViewError> {
    match &cte.body {
        CteBody::Select(select) => {
            reject_restricted_views_in_select_with_ctes(select, catalog, visible_ctes)
        }
        CteBody::Values(_) => Ok(()),
        CteBody::Insert(insert) => {
            if let InsertSource::Select(select) = &insert.source {
                reject_restricted_views_in_select_with_ctes(select, catalog, visible_ctes)?;
            }
            Ok(())
        }
        CteBody::Update(update) => {
            if let Some(from) = &update.from {
                reject_restricted_views_in_from_item(from, catalog, visible_ctes)?;
            }
            Ok(())
        }
        CteBody::Delete(delete) => {
            if let Some(using) = &delete.using {
                reject_restricted_views_in_from_item(using, catalog, visible_ctes)?;
            }
            Ok(())
        }
        CteBody::Merge(merge) => {
            reject_restricted_views_in_from_item(&merge.source, catalog, visible_ctes)
        }
        CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            match anchor.as_ref() {
                CteBody::Select(select) => {
                    reject_restricted_views_in_select_with_ctes(select, catalog, visible_ctes)?
                }
                CteBody::Values(_) => {}
                _ => {}
            }
            reject_restricted_views_in_select_with_ctes(recursive, catalog, visible_ctes)
        }
    }
}

fn reject_restricted_views_in_from_item(
    item: &FromItem,
    catalog: &dyn RestrictedViewCatalog,
    visible_ctes: &[String],
) -> Result<(), RestrictedViewError> {
    match item {
        FromItem::Table { name, .. } if !relation_name_matches_cte(name, visible_ctes) => {
            reject_restricted_view_access(name, catalog)
        }
        FromItem::Table { .. } => Ok(()),
        FromItem::DerivedTable(select) => {
            reject_restricted_views_in_select_with_ctes(select, catalog, &mut visible_ctes.to_vec())
        }
        FromItem::Join { left, right, .. } => {
            reject_restricted_views_in_from_item(left, catalog, visible_ctes)?;
            reject_restricted_views_in_from_item(right, catalog, visible_ctes)
        }
        FromItem::Alias { source, .. }
        | FromItem::Lateral(source)
        | FromItem::TableSample { source, .. } => {
            reject_restricted_views_in_from_item(source, catalog, visible_ctes)
        }
        FromItem::Values { .. }
        | FromItem::Expression { .. }
        | FromItem::FunctionCall { .. }
        | FromItem::RowsFrom { .. }
        | FromItem::JsonTable(_)
        | FromItem::XmlTable(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::parsenodes::SelectStatement;

    struct TestCatalog;

    impl RestrictedViewCatalog for TestCatalog {
        fn lookup_relation_by_name(&self, name: &str) -> Option<RestrictedRelationInfo> {
            let (relation_oid, namespace_oid, relkind) = match name {
                "app_view" => (1, 2, 'v'),
                "pg_catalog.pg_class" => (2, PG_CATALOG_NAMESPACE_OID, 'v'),
                _ => return None,
            };
            Some(RestrictedRelationInfo {
                relation_oid,
                relation_name: name.rsplit('.').next().unwrap_or(name).to_string(),
                namespace_oid,
                relkind,
            })
        }

        fn relation_info_by_oid(&self, relation_oid: u32) -> Option<RestrictedRelationInfo> {
            let (relation_name, namespace_oid, relkind) = match relation_oid {
                1 => ("app_view", 2, 'v'),
                2 => ("pg_class", PG_CATALOG_NAMESPACE_OID, 'v'),
                _ => return None,
            };
            Some(RestrictedRelationInfo {
                relation_oid,
                relation_name: relation_name.into(),
                namespace_oid,
                relkind,
            })
        }
    }

    fn select_from(name: &str) -> SelectStatement {
        SelectStatement {
            with_recursive: false,
            with: Vec::new(),
            with_from_recursive_union_outer: false,
            distinct: false,
            distinct_on: Vec::new(),
            from: Some(FromItem::Table {
                name: name.into(),
                only: false,
                location: None,
            }),
            targets: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            group_by_distinct: false,
            having: None,
            window_clauses: Vec::new(),
            order_by: Vec::new(),
            order_by_location: None,
            limit: None,
            limit_location: None,
            offset: None,
            offset_location: None,
            locking_clause: None,
            locking_location: None,
            locking_targets: Vec::new(),
            locking_nowait: false,
            set_operation: None,
        }
    }

    #[test]
    fn restrict_nonsystem_view_guc_detects_view_member() {
        assert!(restrict_nonsystem_view_enabled(Some("'table', view")));
        assert!(!restrict_nonsystem_view_enabled(Some("table,index")));
    }

    #[test]
    fn select_rejects_non_system_view_but_allows_cte_shadow() {
        let catalog = TestCatalog;
        let err = reject_restricted_views_in_select(&select_from("app_view"), &catalog)
            .expect_err("app view should be restricted");
        assert_eq!(err.relation_name, "app_view");

        let mut select = select_from("app_view");
        select.with.push(CommonTableExpr {
            name: "app_view".into(),
            location: None,
            column_names: Vec::new(),
            body: CteBody::Values(pgrust_nodes::parsenodes::ValuesStatement {
                with_recursive: false,
                with: Vec::new(),
                rows: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }),
            search: None,
            cycle: None,
        });
        reject_restricted_views_in_select(&select, &catalog).unwrap();
    }

    #[test]
    fn plan_rejects_non_system_view_by_oid() {
        let catalog = TestCatalog;
        let plan = Plan::SeqScan {
            plan_info: pgrust_nodes::plannodes::PlanEstimate::default(),
            source_id: 0,
            parallel_scan_id: None,
            rel: pgrust_storage::RelFileLocator {
                spc_oid: 0,
                db_oid: 0,
                rel_number: 0,
            },
            relation_name: "app_view".into(),
            relation_oid: 1,
            relkind: 'v',
            relispopulated: true,
            toast: None,
            tablesample: None,
            desc: pgrust_nodes::primnodes::RelationDesc {
                columns: Vec::new(),
            },
            disabled: false,
            parallel_aware: false,
        };

        let err = reject_restricted_views_in_plan(&plan, &catalog)
            .expect_err("app view plan should be restricted");
        assert_eq!(err.relation_name, "app_view");
    }

    #[test]
    fn readonly_create_statistics_accepts_table_and_rejects_view() {
        let catalog = TestCatalog;
        let err = validate_readonly_create_statistics("app_view", &catalog)
            .expect_err("views should not be accepted");
        assert_eq!(
            err,
            ReadonlyCreateStatisticsError::UnsupportedRelation {
                relation_name: "app_view".into(),
                relkind: 'v'
            }
        );
        assert!(matches!(
            validate_readonly_create_statistics("missing", &catalog),
            Err(ReadonlyCreateStatisticsError::UnknownTable(name)) if name == "missing"
        ));
    }
}
