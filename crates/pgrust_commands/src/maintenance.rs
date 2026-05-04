use std::collections::{BTreeSet, HashMap};

use pgrust_analyze::{BoundRelation, CatalogLookup};
use pgrust_catalog_data::pg_class::relkind_is_analyzable;
use pgrust_nodes::parsenodes::{MaintenanceTarget, ParseError, VacuumStatement};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumExecOptions {
    pub analyze: bool,
    pub full: bool,
    pub index_cleanup: Option<bool>,
    pub truncate: Option<bool>,
    pub default_truncate: bool,
    pub parallel_workers: Option<i32>,
    pub process_main: bool,
    pub process_toast: bool,
    pub only_database_stats: bool,
}

pub fn validate_maintenance_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
) -> Result<(), MaintenanceError> {
    for target in targets {
        let entry = match catalog.lookup_any_relation(&target.table_name) {
            Some(entry) if matches!(entry.relkind, 'r' | 'm') => entry,
            Some(_) => {
                return Err(MaintenanceError::Parse(ParseError::WrongObjectType {
                    name: target.table_name.clone(),
                    expected: "table or materialized view",
                }));
            }
            None => {
                return Err(MaintenanceError::Parse(ParseError::UnknownTable(
                    target.table_name.clone(),
                )));
            }
        };
        for column in &target.columns {
            if !entry
                .desc
                .columns
                .iter()
                .any(|desc| desc.name.eq_ignore_ascii_case(column))
            {
                return Err(MaintenanceError::Parse(ParseError::UnknownColumn(
                    column.clone(),
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_vacuum_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
    analyze: bool,
) -> Result<(), MaintenanceError> {
    for target in targets {
        let entry = match catalog.lookup_any_relation(&target.table_name) {
            Some(entry) if matches!(entry.relkind, 'r' | 'm' | 'p') => entry,
            Some(_) => {
                return Err(MaintenanceError::Parse(ParseError::WrongObjectType {
                    name: target.table_name.clone(),
                    expected: "table or materialized view",
                }));
            }
            None => {
                return Err(MaintenanceError::Parse(ParseError::UnknownTable(
                    target.table_name.clone(),
                )));
            }
        };
        if !analyze && !target.columns.is_empty() {
            return Err(vacuum_option_error(
                "ANALYZE option must be specified when a column list is provided",
                "0A000",
            ));
        }
        validate_target_columns(target, &entry)?;
    }
    Ok(())
}

pub fn validate_analyze_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
) -> Result<(), MaintenanceError> {
    for target in targets {
        let entry = match catalog.lookup_any_relation(&target.table_name) {
            Some(entry) if relkind_is_analyzable(entry.relkind) => entry,
            Some(_) => {
                return Err(MaintenanceError::Parse(ParseError::WrongObjectType {
                    name: target.table_name.clone(),
                    expected: "table",
                }));
            }
            None => {
                return Err(MaintenanceError::Parse(ParseError::UnknownTable(
                    target.table_name.clone(),
                )));
            }
        };
        validate_target_columns(target, &entry)?;
    }
    Ok(())
}

pub fn vacuum_relations_for_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
    process_main: bool,
    process_toast: bool,
) -> Vec<BoundRelation> {
    let mut relations = Vec::with_capacity(targets.len());
    let mut seen = BTreeSet::new();
    for target in targets {
        let Some(entry) = catalog
            .lookup_any_relation(&target.table_name)
            .filter(|entry| matches!(entry.relkind, 'r' | 'm'))
        else {
            continue;
        };
        if process_main && seen.insert(entry.relation_oid) {
            relations.push(entry.clone());
        }
        if process_toast
            && let Some(toast) = entry.toast
            && seen.insert(toast.relation_oid)
            && let Some(toast_relation) = catalog.relation_by_oid(toast.relation_oid)
        {
            relations.push(toast_relation);
        }
    }
    relations
}

pub fn relation_vacuum_index_cleanup(
    relation_oid: u32,
    catalog: &dyn CatalogLookup,
    index_cleanup: Option<bool>,
) -> bool {
    if let Some(index_cleanup) = index_cleanup {
        return index_cleanup;
    }
    relation_bool_reloption(catalog, relation_oid, "vacuum_index_cleanup").unwrap_or(true)
}

pub fn relation_vacuum_truncate(
    relation_oid: u32,
    catalog: &dyn CatalogLookup,
    truncate: Option<bool>,
    default_truncate: bool,
) -> bool {
    if let Some(truncate) = truncate {
        return truncate;
    }
    relation_bool_reloption(catalog, relation_oid, "vacuum_truncate").unwrap_or(default_truncate)
}

pub fn vacuum_exec_options(
    stmt: &VacuumStatement,
    gucs: Option<&HashMap<String, String>>,
) -> Result<VacuumExecOptions, MaintenanceError> {
    let parallel_workers = parse_vacuum_parallel_workers(stmt)?;
    let index_cleanup = parse_vacuum_index_cleanup(stmt.index_cleanup.as_deref())?;
    if let Some(raw) = &stmt.buffer_usage_limit {
        validate_buffer_usage_limit(raw)?;
    }
    if stmt.targets.iter().any(|target| !target.columns.is_empty()) && !stmt.analyze {
        return Err(vacuum_option_error(
            "ANALYZE option must be specified when a column list is provided",
            "0A000",
        ));
    }
    if stmt.full && parallel_workers.unwrap_or(0) > 0 {
        return Err(vacuum_option_error(
            "VACUUM FULL cannot be performed in parallel",
            "0A000",
        ));
    }
    if stmt.full && stmt.buffer_usage_limit.is_some() && !stmt.analyze {
        return Err(vacuum_option_error(
            "BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL",
            "0A000",
        ));
    }
    if stmt.full && stmt.disable_page_skipping {
        return Err(vacuum_option_error(
            "VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL",
            "0A000",
        ));
    }
    let process_toast = stmt.process_toast.unwrap_or(true);
    if stmt.full && !process_toast {
        return Err(vacuum_option_error(
            "PROCESS_TOAST required with VACUUM FULL",
            "0A000",
        ));
    }
    if stmt.only_database_stats {
        if !stmt.targets.is_empty() {
            return Err(vacuum_option_error(
                "ONLY_DATABASE_STATS cannot be specified with a list of tables",
                "0A000",
            ));
        }
        if stmt.analyze
            || stmt.full
            || stmt.freeze
            || stmt.disable_page_skipping
            || stmt.buffer_usage_limit.is_some()
            || stmt.parallel_specified
            || stmt.skip_database_stats
        {
            return Err(vacuum_option_error(
                "ONLY_DATABASE_STATS cannot be specified with other VACUUM options",
                "0A000",
            ));
        }
    }
    Ok(VacuumExecOptions {
        analyze: stmt.analyze,
        full: stmt.full,
        index_cleanup,
        truncate: stmt.truncate,
        default_truncate: gucs
            .and_then(|gucs| gucs.get("vacuum_truncate"))
            .map(|value| {
                !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "false" | "off" | "no" | "0"
                )
            })
            .unwrap_or(true),
        parallel_workers,
        process_main: stmt.process_main.unwrap_or(true),
        process_toast,
        only_database_stats: stmt.only_database_stats,
    })
}

fn parse_vacuum_parallel_workers(stmt: &VacuumStatement) -> Result<Option<i32>, MaintenanceError> {
    if !stmt.parallel_specified {
        return Ok(None);
    }
    let Some(raw) = stmt.parallel.as_deref() else {
        return Err(vacuum_option_error(
            "parallel option requires a value between 0 and 1024",
            "42601",
        ));
    };
    let workers = raw.parse::<i32>().map_err(|_| {
        vacuum_option_error(
            "parallel workers for vacuum must be between 0 and 1024",
            "42601",
        )
    })?;
    if !(0..=1024).contains(&workers) {
        return Err(vacuum_option_error(
            "parallel workers for vacuum must be between 0 and 1024",
            "42601",
        ));
    }
    Ok(Some(workers))
}

fn validate_target_columns(
    target: &MaintenanceTarget,
    relation: &BoundRelation,
) -> Result<(), MaintenanceError> {
    let mut seen = BTreeSet::new();
    for column in &target.columns {
        let normalized = column.to_ascii_lowercase();
        if !seen.insert(normalized) {
            return Err(MaintenanceError::Detailed {
                message: format!(
                    "column \"{}\" of relation \"{}\" appears more than once",
                    column,
                    relation_basename(&target.table_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42701",
            });
        }
        if !relation
            .desc
            .columns
            .iter()
            .any(|desc| !desc.dropped && desc.name.eq_ignore_ascii_case(column))
        {
            return Err(MaintenanceError::Detailed {
                message: format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    column,
                    relation_basename(&target.table_name)
                ),
                detail: None,
                hint: None,
                sqlstate: "42703",
            });
        }
    }
    Ok(())
}

fn parse_vacuum_index_cleanup(raw: Option<&str>) -> Result<Option<bool>, MaintenanceError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(None),
        "true" | "on" | "yes" | "1" => Ok(Some(true)),
        "false" | "off" | "no" | "0" => Ok(Some(false)),
        _ => Err(vacuum_option_error(
            "index_cleanup requires a Boolean value",
            "42601",
        )),
    }
}

fn parse_buffer_usage_limit_kb(raw: &str) -> Option<i64> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    let mut parts = normalized.split_whitespace();
    let number = parts.next()?.parse::<i64>().ok()?;
    match parts.next() {
        None | Some("kb") | Some("k") => Some(number),
        Some(_) => None,
    }
}

fn validate_buffer_usage_limit(raw: &str) -> Result<(), MaintenanceError> {
    let Some(kb) = parse_buffer_usage_limit_kb(raw) else {
        return Err(vacuum_option_error(
            "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB",
            "22023",
        ));
    };
    if kb == 0 || (128..=16_777_216).contains(&kb) {
        return Ok(());
    }
    if raw.trim().split_whitespace().next().is_some_and(|number| {
        number
            .parse::<i64>()
            .ok()
            .is_some_and(|value| i32::try_from(value).is_err())
    }) {
        return Err(MaintenanceError::Detailed {
            message: "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB".into(),
            detail: None,
            hint: Some("Value exceeds integer range.".into()),
            sqlstate: "22023",
        });
    }
    Err(vacuum_option_error(
        "BUFFER_USAGE_LIMIT option must be 0 or between 128 kB and 16777216 kB",
        "22023",
    ))
}

fn vacuum_option_error(message: impl Into<String>, sqlstate: &'static str) -> MaintenanceError {
    MaintenanceError::Detailed {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate,
    }
}

fn relation_basename(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::{PG_CATALOG_NAMESPACE_OID, desc::column_desc};
    use pgrust_core::RelFileLocator;
    use pgrust_nodes::{SqlType, SqlTypeKind, primnodes::RelationDesc};

    #[derive(Default)]
    struct TestCatalog {
        relation: Option<BoundRelation>,
    }

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            self.relation.clone()
        }
    }

    fn vacuum_stmt() -> VacuumStatement {
        VacuumStatement {
            targets: Vec::new(),
            analyze: false,
            full: false,
            freeze: false,
            verbose: false,
            skip_locked: false,
            buffer_usage_limit: None,
            disable_page_skipping: false,
            index_cleanup: None,
            truncate: None,
            parallel: None,
            parallel_specified: false,
            process_main: None,
            process_toast: None,
            skip_database_stats: false,
            only_database_stats: false,
        }
    }

    fn maintenance_target() -> MaintenanceTarget {
        MaintenanceTarget {
            table_name: "public.t".into(),
            columns: Vec::new(),
            only: false,
        }
    }

    fn bound_relation(relkind: char) -> BoundRelation {
        let mut dropped_column = column_desc("old_id", SqlType::new(SqlTypeKind::Int4), false);
        dropped_column.dropped = true;
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
            relkind,
            relispopulated: true,
            relispartition: false,
            relpartbound: None,
            desc: RelationDesc {
                columns: vec![
                    column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                    dropped_column,
                ],
            },
            partitioned_table: None,
            partition_spec: None,
        }
    }

    #[test]
    fn vacuum_exec_options_parse_boolean_and_guc_defaults() {
        let mut stmt = vacuum_stmt();
        stmt.index_cleanup = Some("off".into());
        stmt.truncate = Some(true);
        stmt.process_main = Some(false);
        stmt.process_toast = Some(true);
        stmt.parallel = Some("3".into());
        stmt.parallel_specified = true;
        let gucs = HashMap::from([("vacuum_truncate".into(), "off".into())]);

        let options = vacuum_exec_options(&stmt, Some(&gucs)).unwrap();

        assert_eq!(options.index_cleanup, Some(false));
        assert_eq!(options.truncate, Some(true));
        assert!(!options.default_truncate);
        assert_eq!(options.parallel_workers, Some(3));
        assert!(!options.process_main);
        assert!(options.process_toast);
    }

    #[test]
    fn vacuum_exec_options_rejects_full_parallel() {
        let mut stmt = vacuum_stmt();
        stmt.full = true;
        stmt.parallel = Some("1".into());
        stmt.parallel_specified = true;

        let err = vacuum_exec_options(&stmt, None).unwrap_err();
        assert!(matches!(
            err,
            MaintenanceError::Detailed {
                message,
                sqlstate: "0A000",
                ..
            } if message == "VACUUM FULL cannot be performed in parallel"
        ));
    }

    #[test]
    fn vacuum_exec_options_rejects_column_list_without_analyze() {
        let mut stmt = vacuum_stmt();
        stmt.targets.push(MaintenanceTarget {
            table_name: "t".into(),
            columns: vec!["a".into()],
            only: false,
        });

        let err = vacuum_exec_options(&stmt, None).unwrap_err();
        assert!(matches!(
            err,
            MaintenanceError::Detailed {
                message,
                sqlstate: "0A000",
                ..
            } if message == "ANALYZE option must be specified when a column list is provided"
        ));
    }

    #[test]
    fn vacuum_exec_options_reports_buffer_limit_overflow_hint() {
        let mut stmt = vacuum_stmt();
        stmt.buffer_usage_limit = Some("2147483648".into());

        let err = vacuum_exec_options(&stmt, None).unwrap_err();
        assert!(matches!(
            err,
            MaintenanceError::Detailed {
                hint: Some(hint),
                sqlstate: "22023",
                ..
            } if hint == "Value exceeds integer range."
        ));
    }

    #[test]
    fn validate_vacuum_targets_accepts_partitioned_table() {
        let catalog = TestCatalog {
            relation: Some(bound_relation('p')),
        };

        validate_vacuum_targets(&[maintenance_target()], &catalog, false).unwrap();
    }

    #[test]
    fn validate_vacuum_targets_rejects_columns_without_analyze() {
        let catalog = TestCatalog {
            relation: Some(bound_relation('r')),
        };
        let mut target = maintenance_target();
        target.columns.push("id".into());

        let err = validate_vacuum_targets(&[target], &catalog, false).unwrap_err();
        assert!(matches!(
            err,
            MaintenanceError::Detailed {
                message,
                sqlstate: "0A000",
                ..
            } if message == "ANALYZE option must be specified when a column list is provided"
        ));
    }

    #[test]
    fn validate_analyze_targets_reports_dropped_column_as_missing() {
        let catalog = TestCatalog {
            relation: Some(bound_relation('r')),
        };
        let mut target = maintenance_target();
        target.columns.push("old_id".into());

        let err = validate_analyze_targets(&[target], &catalog).unwrap_err();
        assert!(matches!(
            err,
            MaintenanceError::Detailed {
                message,
                sqlstate: "42703",
                ..
            } if message == "column \"old_id\" of relation \"t\" does not exist"
        ));
    }
}

fn relation_bool_reloption(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    option_name: &str,
) -> Option<bool> {
    catalog
        .class_row_by_oid(relation_oid)
        .and_then(|row| row.reloptions)
        .and_then(|options| {
            options.into_iter().find_map(|option| {
                let (name, value) = option.split_once('=')?;
                name.eq_ignore_ascii_case(option_name).then(|| {
                    !matches!(
                        value.to_ascii_lowercase().as_str(),
                        "false" | "off" | "no" | "0"
                    )
                })
            })
        })
}
