use std::{cell::RefCell, collections::BTreeMap};

use crate::backend::executor::Value;
use crate::backend::rewrite::format_stored_rule_definition;
use crate::backend::utils::cache::system_view_registry::synthetic_system_views;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, NAME_TYPE_OID, PG_LANGUAGE_INTERNAL_OID, PgAmRow, PgAttributeRow,
    PgAuthIdRow, PgClassRow, PgIndexRow, PgNamespaceRow, PgPolicyRow, PgProcRow, PgRewriteRow,
    PgStatisticRow, PolicyCommand,
};
use crate::include::nodes::datum::ArrayValue;
use crate::pgrust::database::DatabaseStatsStore;

const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;
const STATISTIC_KIND_MCELEM: i16 = 4;
const STATISTIC_KIND_DECHIST: i16 = 5;
const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;

#[derive(Debug, Clone)]
pub(crate) struct CopyProgressSnapshot {
    pub pid: i32,
    pub datid: u32,
    pub datname: String,
    pub relid: u32,
    pub command: &'static str,
    pub copy_type: &'static str,
    pub bytes_processed: i64,
    pub bytes_total: i64,
    pub tuples_processed: i64,
    pub tuples_excluded: i64,
    pub tuples_skipped: i64,
}

thread_local! {
    static CURRENT_COPY_PROGRESS: RefCell<Option<CopyProgressSnapshot>> = const { RefCell::new(None) };
}

pub(crate) struct CopyProgressGuard;

impl Drop for CopyProgressGuard {
    fn drop(&mut self) {
        CURRENT_COPY_PROGRESS.with(|progress| {
            *progress.borrow_mut() = None;
        });
    }
}

pub(crate) fn install_copy_progress(snapshot: CopyProgressSnapshot) -> CopyProgressGuard {
    CURRENT_COPY_PROGRESS.with(|progress| {
        *progress.borrow_mut() = Some(snapshot);
    });
    CopyProgressGuard
}

pub(crate) fn current_pg_stat_progress_copy_rows() -> Vec<Vec<Value>> {
    CURRENT_COPY_PROGRESS.with(|progress| {
        progress
            .borrow()
            .as_ref()
            .map(|snapshot| {
                vec![vec![
                    Value::Int32(snapshot.pid),
                    Value::Int64(i64::from(snapshot.datid)),
                    Value::Text(snapshot.datname.clone().into()),
                    Value::Int64(i64::from(snapshot.relid)),
                    Value::Text(snapshot.command.into()),
                    Value::Text(snapshot.copy_type.into()),
                    Value::Int64(snapshot.bytes_processed),
                    Value::Int64(snapshot.bytes_total),
                    Value::Int64(snapshot.tuples_processed),
                    Value::Int64(snapshot.tuples_excluded),
                    Value::Int64(snapshot.tuples_skipped),
                ]]
            })
            .unwrap_or_default()
    })
}

pub fn build_pg_views_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
    rewrites: Vec<PgRewriteRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_names = authids
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<BTreeMap<_, _>>();
    let return_rules = rewrites
        .into_iter()
        .filter(|row| row.rulename == "_RETURN")
        .map(|row| (row.ev_class, row.ev_action))
        .collect::<BTreeMap<_, _>>();

    let mut rows = classes
        .into_iter()
        .filter(|class| class.relkind == 'v')
        .filter_map(|class| {
            let definition = return_rules.get(&class.oid)?.clone();
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.into()),
                    Value::Text(
                        role_names
                            .get(&class.relowner)
                            .cloned()
                            .unwrap_or_else(|| "unknown".into())
                            .into(),
                    ),
                    Value::Text(definition.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    append_synthetic_pg_catalog_view_rows(&mut rows, &role_names);
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub fn build_pg_matviews_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    rewrites: Vec<PgRewriteRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_names = authids
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<BTreeMap<_, _>>();
    let return_rules = rewrites
        .into_iter()
        .filter(|row| row.rulename == "_RETURN")
        .map(|row| (row.ev_class, row.ev_action))
        .collect::<BTreeMap<_, _>>();
    let mut index_counts = BTreeMap::<u32, usize>::new();
    for index in indexes {
        *index_counts.entry(index.indrelid).or_default() += 1;
    }

    let mut rows = classes
        .into_iter()
        .filter(|class| class.relkind == 'm')
        .filter_map(|class| {
            let definition = return_rules.get(&class.oid)?.clone();
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(
                        role_names
                            .get(&class.relowner)
                            .cloned()
                            .unwrap_or_else(|| "unknown".into())
                            .into(),
                    ),
                    Value::Null,
                    Value::Bool(index_counts.get(&class.oid).copied().unwrap_or_default() > 0),
                    Value::Bool(class.relispopulated),
                    Value::Text(definition.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub fn build_pg_indexes_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    indexes: Vec<PgIndexRow>,
    access_methods: Vec<PgAmRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let am_names = access_methods
        .into_iter()
        .map(|row| (row.oid, row.amname))
        .collect::<BTreeMap<_, _>>();
    let mut attributes_by_relation = BTreeMap::<u32, BTreeMap<i16, String>>::new();
    for attribute in attributes {
        if attribute.attnum <= 0 || attribute.attisdropped {
            continue;
        }
        attributes_by_relation
            .entry(attribute.attrelid)
            .or_default()
            .insert(attribute.attnum, attribute.attname);
    }

    let mut rows = indexes
        .into_iter()
        .filter_map(|index| {
            let table = classes_by_oid.get(&index.indrelid)?;
            let index_class = classes_by_oid.get(&index.indexrelid)?;
            if !matches!(index_class.relkind, 'i' | 'I') {
                return None;
            }
            let schemaname = namespace_names
                .get(&table.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let all_column_names = index
                .indkey
                .iter()
                .map(|attnum| {
                    if *attnum == 0 {
                        "expr".to_string()
                    } else {
                        attributes_by_relation
                            .get(&table.oid)
                            .and_then(|attrs| attrs.get(attnum))
                            .cloned()
                            .unwrap_or_else(|| attnum.to_string())
                    }
                })
                .collect::<Vec<_>>();
            let key_count = usize::try_from(index.indnkeyatts.max(0)).unwrap_or_default();
            let key_column_names = all_column_names
                .iter()
                .take(key_count)
                .cloned()
                .collect::<Vec<_>>();
            let include_column_names = all_column_names
                .iter()
                .skip(key_count)
                .cloned()
                .collect::<Vec<_>>();
            let unique = if index.indisunique { "UNIQUE " } else { "" };
            let only = if index_class.relkind == 'I' {
                " ONLY"
            } else {
                ""
            };
            let table_name = format!("{}.{}", schemaname, table.relname);
            let amname = am_names
                .get(&index_class.relam)
                .cloned()
                .unwrap_or_else(|| "btree".to_string());
            let mut indexdef = format!(
                "CREATE {unique}INDEX {} ON{only} {} USING {} ({})",
                index_class.relname,
                table_name,
                amname,
                key_column_names.join(", ")
            );
            if !include_column_names.is_empty() {
                indexdef.push_str(" INCLUDE (");
                indexdef.push_str(&include_column_names.join(", "));
                indexdef.push(')');
            }
            if let Some(predicate) = index.indpred.as_deref().filter(|sql| !sql.is_empty()) {
                indexdef.push_str(" WHERE (");
                indexdef.push_str(predicate);
                indexdef.push(')');
            }
            Some((
                schemaname.clone(),
                table.relname.clone(),
                index_class.relname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(table.relname.clone().into()),
                    Value::Text(index_class.relname.clone().into()),
                    Value::Null,
                    Value::Text(indexdef.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

fn append_synthetic_pg_catalog_view_rows(
    rows: &mut Vec<(String, String, Vec<Value>)>,
    role_names: &BTreeMap<u32, String>,
) {
    let view_owner = role_names
        .get(&BOOTSTRAP_SUPERUSER_OID)
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    rows.extend(
        synthetic_system_views()
            .iter()
            .filter(|view| {
                view.has_metadata_definition() && view.canonical_name.starts_with("pg_catalog.")
            })
            .map(|view| {
                let schemaname = "pg_catalog".to_string();
                let viewname = view.unqualified_name().to_string();
                (
                    schemaname.clone(),
                    viewname.clone(),
                    vec![
                        Value::Text(schemaname.into()),
                        Value::Text(viewname.into()),
                        Value::Text(view_owner.clone().into()),
                        Value::Text(view.view_definition_sql.to_string().into()),
                    ],
                )
            }),
    );
}

pub fn build_pg_rules_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    rewrites: Vec<PgRewriteRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();

    let mut rows = rewrites
        .into_iter()
        .filter(|row| row.rulename != "_RETURN")
        .filter_map(|row| {
            let class = classes_by_oid.get(&row.ev_class)?;
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let rulename = row.rulename.clone();
            let relation_name = format!("{}.{}", schemaname, class.relname);
            Some((
                schemaname.clone(),
                class.relname.clone(),
                rulename.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(rulename.into()),
                    Value::Text(format_stored_rule_definition(&row, &relation_name).into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

pub fn build_pg_policies_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
    policies: Vec<PgPolicyRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_names = authids
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();

    let mut rows = policies
        .into_iter()
        .filter_map(|policy| {
            let class = classes_by_oid.get(&policy.polrelid)?;
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some((
                schemaname.clone(),
                class.relname.clone(),
                policy.polname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(policy.polname.clone().into()),
                    Value::Text(
                        if policy.polpermissive {
                            "PERMISSIVE"
                        } else {
                            "RESTRICTIVE"
                        }
                        .into(),
                    ),
                    Value::PgArray(
                        ArrayValue::from_1d(
                            policy_role_names(&policy.polroles, &role_names)
                                .into_iter()
                                .map(|role_name| Value::Text(role_name.into()))
                                .collect(),
                        )
                        .with_element_type_oid(NAME_TYPE_OID),
                    ),
                    Value::Text(policy_command_name(policy.polcmd).into()),
                    optional_text_value(policy.polqual),
                    optional_text_value(policy.polwithcheck),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

fn policy_role_names(role_oids: &[u32], role_names: &BTreeMap<u32, String>) -> Vec<String> {
    // :HACK: pgrust currently allows PUBLIC to coexist with specific role OIDs,
    // while PostgreSQL normally normalizes that state away. We surface both
    // names here so callers can still inspect the underlying catalog state.
    let mut resolved = role_oids
        .iter()
        .map(|role_oid| {
            if *role_oid == 0 {
                "public".to_string()
            } else {
                role_names
                    .get(role_oid)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string())
            }
        })
        .collect::<Vec<_>>();
    resolved.sort();
    resolved.dedup();
    resolved
}

fn policy_command_name(command: PolicyCommand) -> &'static str {
    match command {
        PolicyCommand::All => "ALL",
        PolicyCommand::Select => "SELECT",
        PolicyCommand::Insert => "INSERT",
        PolicyCommand::Update => "UPDATE",
        PolicyCommand::Delete => "DELETE",
    }
}

fn optional_text_value(value: Option<String>) -> Value {
    value
        .map(|value| Value::Text(value.into()))
        .unwrap_or(Value::Null)
}

pub fn build_pg_stats_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    statistics: Vec<PgStatisticRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let attributes_by_key = attributes
        .into_iter()
        .map(|row| ((row.attrelid, row.attnum), row))
        .collect::<BTreeMap<_, _>>();

    let mut rows = statistics
        .into_iter()
        .filter_map(|stat| {
            let class = classes_by_oid.get(&stat.starelid)?;
            let attribute = attributes_by_key.get(&(stat.starelid, stat.staattnum))?;
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());

            Some((
                schemaname.clone(),
                class.relname.clone(),
                attribute.attname.clone(),
                stat.stainherit,
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(attribute.attname.clone().into()),
                    Value::Bool(stat.stainherit),
                    Value::Float64(stat.stanullfrac),
                    Value::Int32(stat.stawidth),
                    Value::Float64(stat.stadistinct),
                    slot_values(&stat, STATISTIC_KIND_MCV),
                    slot_numbers(&stat, STATISTIC_KIND_MCV),
                    slot_values(&stat, STATISTIC_KIND_HISTOGRAM),
                    slot_first_number(&stat, STATISTIC_KIND_CORRELATION),
                    slot_values(&stat, STATISTIC_KIND_MCELEM),
                    slot_numbers(&stat, STATISTIC_KIND_MCELEM),
                    slot_numbers(&stat, STATISTIC_KIND_DECHIST),
                    slot_values(&stat, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM),
                    slot_first_number(&stat, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM),
                    slot_values(&stat, STATISTIC_KIND_BOUNDS_HISTOGRAM),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.cmp(&right.3))
    });
    rows.into_iter().map(|(_, _, _, _, row)| row).collect()
}

pub fn build_pg_locks_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows
}

fn slot_index(stat: &PgStatisticRow, kind: i16) -> Option<usize> {
    stat.stakind.iter().position(|entry| *entry == kind)
}

fn slot_values(stat: &PgStatisticRow, kind: i16) -> Value {
    slot_index(stat, kind)
        .and_then(|idx| stat.stavalues[idx].clone())
        .map(Value::PgArray)
        .unwrap_or(Value::Null)
}

fn slot_numbers(stat: &PgStatisticRow, kind: i16) -> Value {
    slot_index(stat, kind)
        .and_then(|idx| stat.stanumbers[idx].clone())
        .map(Value::PgArray)
        .unwrap_or(Value::Null)
}

fn slot_first_number(stat: &PgStatisticRow, kind: i16) -> Value {
    slot_index(stat, kind)
        .and_then(|idx| stat.stanumbers[idx].as_ref())
        .and_then(|array| array.elements.first().cloned())
        .unwrap_or(Value::Null)
}

pub(crate) fn build_pg_stat_user_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    build_pg_stat_tables_rows(namespaces, classes, indexes, stats, false)
}

pub(crate) fn build_pg_stat_all_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    build_pg_stat_tables_rows(namespaces, classes, indexes, stats, true)
}

fn build_pg_stat_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
    include_system: bool,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let index_rows_by_heap =
        indexes
            .into_iter()
            .fold(BTreeMap::<u32, Vec<u32>>::new(), |mut acc, row| {
                acc.entry(row.indrelid).or_default().push(row.indexrelid);
                acc
            });

    let mut rows = classes
        .into_iter()
        .filter(|class| matches!(class.relkind, 'r' | 't' | 'm' | 'p'))
        .filter_map(|class| {
            let schemaname = namespace_names.get(&class.relnamespace)?.clone();
            if !include_system
                && (schemaname == "pg_catalog"
                    || schemaname == "information_schema"
                    || schemaname.starts_with("pg_toast"))
            {
                return None;
            }
            let rel_stats = stats.relations.get(&class.oid).cloned().unwrap_or_default();
            let mut last_idx_scan = None;
            let idx_scan = index_rows_by_heap
                .get(&class.oid)
                .into_iter()
                .flatten()
                .map(|index_oid| {
                    let entry = stats.relations.get(index_oid).cloned().unwrap_or_default();
                    if last_idx_scan < entry.lastscan {
                        last_idx_scan = entry.lastscan;
                    }
                    entry.numscans
                })
                .sum::<i64>();
            let idx_tup_fetch = index_rows_by_heap
                .get(&class.oid)
                .into_iter()
                .flatten()
                .map(|index_oid| {
                    stats
                        .relations
                        .get(index_oid)
                        .map(|entry| entry.tuples_fetched)
                        .unwrap_or(0)
                })
                .sum::<i64>()
                + rel_stats.tuples_fetched;
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Int64(class.oid as i64),
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.into()),
                    Value::Int64(rel_stats.numscans),
                    rel_stats
                        .lastscan
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    Value::Int64(rel_stats.tuples_returned),
                    Value::Int64(idx_scan),
                    last_idx_scan.map(Value::TimestampTz).unwrap_or(Value::Null),
                    Value::Int64(idx_tup_fetch),
                    Value::Int64(rel_stats.tuples_inserted),
                    Value::Int64(rel_stats.tuples_updated),
                    Value::Int64(rel_stats.tuples_deleted),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(rel_stats.live_tuples),
                    Value::Int64(rel_stats.dead_tuples),
                    Value::Int64(rel_stats.mod_since_analyze),
                    Value::Int64(rel_stats.ins_since_vacuum),
                    rel_stats
                        .last_vacuum
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    rel_stats
                        .last_autovacuum
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    rel_stats
                        .last_analyze
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    rel_stats
                        .last_autoanalyze
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    Value::Int64(rel_stats.vacuum_count),
                    Value::Int64(rel_stats.autovacuum_count),
                    Value::Int64(rel_stats.analyze_count),
                    Value::Int64(rel_stats.autoanalyze_count),
                    Value::Float64(rel_stats.total_vacuum_time_micros as f64 / 1000.0),
                    Value::Float64(rel_stats.total_autovacuum_time_micros as f64 / 1000.0),
                    Value::Float64(rel_stats.total_analyze_time_micros as f64 / 1000.0),
                    Value::Float64(rel_stats.total_autoanalyze_time_micros as f64 / 1000.0),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_pg_statio_user_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let index_rows_by_heap =
        indexes
            .into_iter()
            .fold(BTreeMap::<u32, Vec<u32>>::new(), |mut acc, row| {
                acc.entry(row.indrelid).or_default().push(row.indexrelid);
                acc
            });

    let mut rows = classes
        .into_iter()
        .filter(|class| matches!(class.relkind, 'r' | 't' | 'm'))
        .filter_map(|class| {
            let schemaname = namespace_names.get(&class.relnamespace)?.clone();
            if schemaname == "pg_catalog"
                || schemaname == "information_schema"
                || schemaname.starts_with("pg_toast")
            {
                return None;
            }
            let rel_stats = stats.relations.get(&class.oid).cloned().unwrap_or_default();
            let idx_stats = index_rows_by_heap
                .get(&class.oid)
                .into_iter()
                .flatten()
                .filter_map(|index_oid| stats.relations.get(index_oid))
                .fold((0_i64, 0_i64), |(read, hit), entry| {
                    (
                        read + (entry.blocks_fetched - entry.blocks_hit).max(0),
                        hit + entry.blocks_hit,
                    )
                });
            let toast_oid = class.reltoastrelid;
            let toast_stats = stats.relations.get(&toast_oid).cloned().unwrap_or_default();
            let toast_idx_stats = index_rows_by_heap
                .get(&toast_oid)
                .into_iter()
                .flatten()
                .filter_map(|index_oid| stats.relations.get(index_oid))
                .fold((0_i64, 0_i64), |(read, hit), entry| {
                    (
                        read + (entry.blocks_fetched - entry.blocks_hit).max(0),
                        hit + entry.blocks_hit,
                    )
                });
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Int64(class.oid as i64),
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.into()),
                    Value::Int64((rel_stats.blocks_fetched - rel_stats.blocks_hit).max(0)),
                    Value::Int64(rel_stats.blocks_hit),
                    Value::Int64(idx_stats.0),
                    Value::Int64(idx_stats.1),
                    Value::Int64((toast_stats.blocks_fetched - toast_stats.blocks_hit).max(0)),
                    Value::Int64(toast_stats.blocks_hit),
                    Value::Int64(toast_idx_stats.0),
                    Value::Int64(toast_idx_stats.1),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_pg_stat_user_functions_rows(
    namespaces: Vec<PgNamespaceRow>,
    procs: Vec<PgProcRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let mut rows = procs
        .into_iter()
        .filter(|proc| proc.prolang != PG_LANGUAGE_INTERNAL_OID)
        .filter_map(|proc| {
            let entry = stats.functions.get(&proc.oid)?;
            let schemaname = namespace_names.get(&proc.pronamespace)?.clone();
            Some((
                schemaname.clone(),
                proc.proname.clone(),
                vec![
                    Value::Int64(proc.oid as i64),
                    Value::Text(schemaname.into()),
                    Value::Text(proc.proname.into()),
                    Value::Int64(entry.calls),
                    Value::Float64(entry.total_time_micros as f64 / 1000.0),
                    Value::Float64(entry.self_time_micros as f64 / 1000.0),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_pg_stat_io_rows(stats: &DatabaseStatsStore) -> Vec<Vec<Value>> {
    stats
        .io
        .iter()
        .map(|(key, entry)| {
            vec![
                Value::Text(key.backend_type.clone().into()),
                Value::Text(key.object.clone().into()),
                Value::Text(key.context.clone().into()),
                Value::Int64(entry.reads),
                Value::Int64(entry.read_bytes),
                Value::Float64(entry.read_time_micros as f64 / 1000.0),
                Value::Int64(entry.writes),
                Value::Int64(entry.write_bytes),
                Value::Float64(entry.write_time_micros as f64 / 1000.0),
                Value::Int64(entry.writebacks),
                Value::Float64(entry.writeback_time_micros as f64 / 1000.0),
                Value::Int64(entry.extends),
                Value::Int64(entry.extend_bytes),
                Value::Float64(entry.extend_time_micros as f64 / 1000.0),
                Value::Int64(entry.hits),
                Value::Int64(entry.evictions),
                Value::Int64(entry.reuses),
                Value::Int64(entry.fsyncs),
                Value::Float64(entry.fsync_time_micros as f64 / 1000.0),
                entry
                    .stats_reset
                    .map(Value::TimestampTz)
                    .unwrap_or(Value::Null),
            ]
        })
        .collect()
}
