use std::collections::BTreeMap;

use crate::backend::executor::Value;
use crate::backend::rewrite::format_stored_rule_definition;
use crate::include::catalog::{
    PG_LANGUAGE_INTERNAL_OID, PgAttributeRow, PgAuthIdRow, PgClassRow, PgIndexRow, PgNamespaceRow,
    PgProcRow, PgRewriteRow, PgStatisticRow,
};
use crate::pgrust::database::DatabaseStatsStore;

const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;
const STATISTIC_KIND_MCELEM: i16 = 4;
const STATISTIC_KIND_DECHIST: i16 = 5;
const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;

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
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
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
            if schemaname == "pg_catalog"
                || schemaname == "information_schema"
                || schemaname.starts_with("pg_toast")
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
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Int64(0),
                    Value::Float64(0.0),
                    Value::Float64(0.0),
                    Value::Float64(0.0),
                    Value::Float64(0.0),
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
