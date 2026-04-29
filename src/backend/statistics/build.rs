use std::collections::{BTreeMap, BTreeSet};

use crate::backend::executor::Value;

use super::types::{
    PgDependenciesPayload, PgDependencyItem, PgMcvItem, PgMcvListPayload, PgNdistinctItem,
    PgNdistinctPayload, encode_pg_dependencies_payload, encode_pg_mcv_list_payload,
    encode_pg_ndistinct_payload, statistics_value_key,
};

#[derive(Debug, Clone)]
pub struct ExtendedStatisticsPayloads {
    pub stxdndistinct: Option<Vec<u8>>,
    pub stxddependencies: Option<Vec<u8>>,
    pub stxdmcv: Option<Vec<u8>>,
}

pub fn build_extended_statistics_payloads(
    target_ids: &[i16],
    sample_rows: &[Vec<Value>],
    total_rows: f64,
    kind_bytes: &[u8],
    statistics_target: i16,
) -> Result<ExtendedStatisticsPayloads, String> {
    let wants_all = kind_bytes.is_empty();
    let wants_ndistinct = wants_all || kind_bytes.contains(&b'd');
    let wants_dependencies = wants_all || kind_bytes.contains(&b'f');
    let wants_mcv = wants_all || kind_bytes.contains(&b'm');
    let keyed_rows = sample_rows
        .iter()
        .map(|row| row.iter().map(statistics_value_key).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    Ok(ExtendedStatisticsPayloads {
        stxdndistinct: wants_ndistinct
            .then(|| build_ndistinct_payload(target_ids, &keyed_rows, total_rows))
            .transpose()?,
        stxddependencies: wants_dependencies
            .then(|| build_dependencies_payload(target_ids, &keyed_rows))
            .transpose()?,
        stxdmcv: wants_mcv
            .then(|| build_mcv_payload(&keyed_rows, statistics_target))
            .transpose()?,
    })
}

fn build_ndistinct_payload(
    target_ids: &[i16],
    rows: &[Vec<Option<String>>],
    total_rows: f64,
) -> Result<Vec<u8>, String> {
    let mut items = Vec::new();
    for size in 2..=target_ids.len() {
        for combination in combinations(target_ids.len(), size) {
            let mut counts = BTreeMap::<Vec<Option<String>>, usize>::new();
            for row in rows {
                let key = combination
                    .iter()
                    .map(|idx| row[*idx].clone())
                    .collect::<Vec<_>>();
                *counts.entry(key).or_insert(0) += 1;
            }
            if counts.is_empty() {
                continue;
            }
            let distinct = counts.len();
            let singleton_count = counts.values().filter(|count| **count == 1).count();
            let ndistinct =
                estimate_multivariate_ndistinct(total_rows, rows.len(), distinct, singleton_count);
            items.push(PgNdistinctItem {
                dimensions: combination.iter().map(|idx| target_ids[*idx]).collect(),
                ndistinct,
            });
        }
    }
    encode_pg_ndistinct_payload(&PgNdistinctPayload { items })
}

fn build_dependencies_payload(
    target_ids: &[i16],
    rows: &[Vec<Option<String>>],
) -> Result<Vec<u8>, String> {
    let mut items = Vec::new();
    if rows.is_empty() {
        return encode_pg_dependencies_payload(&PgDependenciesPayload { items });
    }
    for size in 2..=target_ids.len() {
        for combination in combinations(target_ids.len(), size) {
            for implied_pos in (0..combination.len()).rev() {
                let implied = combination[implied_pos];
                let from = combination
                    .iter()
                    .copied()
                    .filter(|idx| *idx != implied)
                    .collect::<Vec<_>>();
                let degree = dependency_degree(&from, implied, rows);
                if degree == 0.0 {
                    continue;
                }
                items.push(PgDependencyItem {
                    from: from.iter().map(|idx| target_ids[*idx]).collect(),
                    to: vec![target_ids[implied]],
                    degree,
                });
            }
        }
    }
    encode_pg_dependencies_payload(&PgDependenciesPayload { items })
}

fn build_mcv_payload(
    rows: &[Vec<Option<String>>],
    statistics_target: i16,
) -> Result<Vec<u8>, String> {
    let sample_total = rows.len();
    let mut items = Vec::new();
    if sample_total == 0 {
        return encode_pg_mcv_list_payload(&PgMcvListPayload { items });
    }
    let mut tuple_counts = BTreeMap::<Vec<Option<String>>, (usize, usize)>::new();
    let mut marginal_counts = (0..rows[0].len())
        .map(|_| BTreeMap::<Option<String>, usize>::new())
        .collect::<Vec<_>>();
    for (row_index, row) in rows.iter().enumerate() {
        tuple_counts
            .entry(row.clone())
            .and_modify(|(count, _)| *count += 1)
            .or_insert((1, row_index));
        for (index, value) in row.iter().enumerate() {
            *marginal_counts[index].entry(value.clone()).or_insert(0) += 1;
        }
    }

    let mut ranked = tuple_counts.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .1
            .0
            .cmp(&left.1.0)
            .then_with(|| left.1.1.cmp(&right.1.1))
            .then_with(|| left.0.cmp(&right.0))
    });
    let has_repeated_group = ranked.iter().any(|(_, (count, _))| *count > 1);
    let target = if statistics_target <= 0 {
        100
    } else {
        statistics_target as usize
    };
    for (values, (count, _)) in ranked.into_iter() {
        let base_frequency = values.iter().enumerate().fold(1.0, |acc, (index, value)| {
            let marginal = marginal_counts[index].get(value).copied().unwrap_or(0) as f64;
            acc * (marginal / sample_total as f64)
        });
        let frequency = rounded_stat_frequency(count as f64 / sample_total as f64);
        let base_frequency = rounded_stat_frequency(base_frequency);
        if count == 1 && (has_repeated_group || sample_total > target) {
            continue;
        }
        items.push(PgMcvItem {
            values,
            frequency,
            base_frequency,
        });
        if items.len() >= target {
            break;
        }
    }
    loop {
        let payload = PgMcvListPayload {
            items: items.clone(),
        };
        let encoded = encode_pg_mcv_list_payload(&payload)?;
        if encoded.len() <= 8_000 || items.is_empty() {
            return Ok(encoded);
        }
        items.pop();
    }
}

fn rounded_stat_frequency(value: f64) -> f64 {
    const SCALE: f64 = 1_000_000_000_000_000.0;
    if value.is_finite() {
        (value * SCALE).round() / SCALE
    } else {
        value
    }
}

fn estimate_multivariate_ndistinct(
    total_rows: f64,
    sample_rows: usize,
    distinct: usize,
    singleton_count: usize,
) -> f64 {
    if sample_rows == 0 || distinct == 0 || total_rows <= 0.0 {
        return 0.0;
    }
    if distinct == sample_rows {
        return total_rows.max(distinct as f64).round();
    }
    let numer = sample_rows as f64 * distinct as f64;
    let denom = (sample_rows - singleton_count) as f64
        + singleton_count as f64 * sample_rows as f64 / total_rows.max(1.0);
    if denom <= 0.0 {
        return distinct as f64;
    }
    let ndistinct = (numer / denom).clamp(distinct as f64, total_rows.max(distinct as f64));
    ndistinct.round()
}

fn dependency_degree(
    determinant_indices: &[usize],
    implied_index: usize,
    rows: &[Vec<Option<String>>],
) -> f64 {
    let mut groups = BTreeMap::<Vec<Option<String>>, BTreeSet<Option<String>>>::new();
    let mut group_sizes = BTreeMap::<Vec<Option<String>>, usize>::new();
    for row in rows {
        let key = determinant_indices
            .iter()
            .map(|idx| row[*idx].clone())
            .collect::<Vec<_>>();
        groups
            .entry(key.clone())
            .or_default()
            .insert(row[implied_index].clone());
        *group_sizes.entry(key).or_insert(0) += 1;
    }
    let supporting = groups
        .into_iter()
        .filter(|(_, implied_values)| implied_values.len() == 1)
        .map(|(key, _)| group_sizes.get(&key).copied().unwrap_or(0))
        .sum::<usize>();
    supporting as f64 / rows.len() as f64
}

fn combinations(width: usize, size: usize) -> Vec<Vec<usize>> {
    fn go(
        width: usize,
        size: usize,
        start: usize,
        current: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        if current.len() == size {
            out.push(current.clone());
            return;
        }
        for index in start..width {
            current.push(index);
            go(width, size, index + 1, current, out);
            current.pop();
        }
    }
    let mut out = Vec::new();
    go(width, size, 0, &mut Vec::new(), &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndistinct_builds_pair_and_triple_items() {
        let rows = vec![
            vec![Value::Int32(1), Value::Int32(1), Value::Int32(1)],
            vec![Value::Int32(1), Value::Int32(1), Value::Int32(2)],
            vec![Value::Int32(2), Value::Int32(2), Value::Int32(2)],
        ];
        let payload =
            build_extended_statistics_payloads(&[1, 2, 3], &rows, 3.0, b"d", 100).unwrap();
        let decoded = crate::backend::statistics::types::decode_pg_ndistinct_payload(
            &payload.stxdndistinct.unwrap(),
        )
        .unwrap();
        assert!(
            decoded
                .items
                .iter()
                .any(|item| item.dimensions == vec![1, 2])
        );
        assert!(
            decoded
                .items
                .iter()
                .any(|item| item.dimensions == vec![1, 2, 3])
        );
    }

    #[test]
    fn dependencies_detect_perfect_and_partial_degrees() {
        let rows = vec![
            vec![Value::Int32(1), Value::Int32(10)],
            vec![Value::Int32(1), Value::Int32(10)],
            vec![Value::Int32(2), Value::Int32(20)],
            vec![Value::Int32(2), Value::Int32(30)],
        ];
        let payload = build_extended_statistics_payloads(&[1, 2], &rows, 4.0, b"f", 100).unwrap();
        let decoded = crate::backend::statistics::types::decode_pg_dependencies_payload(
            &payload.stxddependencies.unwrap(),
        )
        .unwrap();
        let dep = decoded
            .items
            .iter()
            .find(|item| item.from == vec![1] && item.to == vec![2])
            .unwrap();
        assert_eq!(dep.degree, 0.5);
    }

    #[test]
    fn mcv_builds_frequency_and_base_frequency() {
        let rows = vec![
            vec![Value::Int32(1), Value::Text("x".into())],
            vec![Value::Int32(1), Value::Text("x".into())],
            vec![Value::Int32(1), Value::Text("y".into())],
            vec![Value::Int32(2), Value::Text("x".into())],
        ];
        let payload = build_extended_statistics_payloads(&[1, 2], &rows, 4.0, b"m", 100).unwrap();
        let decoded = crate::backend::statistics::types::decode_pg_mcv_list_payload(
            &payload.stxdmcv.unwrap(),
        )
        .unwrap();
        assert_eq!(decoded.items.len(), 1);
        assert_eq!(
            decoded.items[0].values,
            vec![Some("1".into()), Some("x".into())]
        );
        assert_eq!(decoded.items[0].frequency, 0.5);
        assert_eq!(decoded.items[0].base_frequency, 0.5625);
    }

    #[test]
    fn mcv_tie_break_keeps_earliest_observed_group() {
        let rows = vec![
            vec![Value::Text("z".into()), Value::Text("x".into())],
            vec![Value::Text("a".into()), Value::Text("x".into())],
            vec![Value::Text("z".into()), Value::Text("x".into())],
            vec![Value::Text("a".into()), Value::Text("x".into())],
        ];
        let payload = build_extended_statistics_payloads(&[1, 2], &rows, 4.0, b"m", 1).unwrap();
        let decoded = crate::backend::statistics::types::decode_pg_mcv_list_payload(
            &payload.stxdmcv.unwrap(),
        )
        .unwrap();
        assert_eq!(decoded.items.len(), 1);
        assert_eq!(
            decoded.items[0].values,
            vec![Some("z".into()), Some("x".into())]
        );
    }
}
