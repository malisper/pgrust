use std::collections::BTreeMap;

use crate::backend::executor::Value;
use crate::backend::rewrite::format_stored_rule_definition;
use crate::include::catalog::{
    PgAttributeRow, PgAuthIdRow, PgClassRow, PgNamespaceRow, PgRewriteRow, PgStatisticRow,
};

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
