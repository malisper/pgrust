use std::collections::{HashMap, HashSet};

use pgrust_analyze::{BoundRelation, CatalogLookup};
use pgrust_catalog_data::{PgStatisticExtRow, PgStatisticRow, TEXT_TYPE_OID};
use pgrust_nodes::Value;
use pgrust_nodes::datum::ArrayValue;
use pgrust_nodes::parsenodes::{MaintenanceTarget, ParseError};
use pgrust_nodes::primnodes::RelationDesc;

pub const DEFAULT_STATISTICS_TARGET: i16 = 100;
pub const STATISTIC_KIND_MCV: i16 = 1;
pub const STATISTIC_KIND_HISTOGRAM: i16 = 2;
pub const STATISTIC_KIND_CORRELATION: i16 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalyzeError {
    Parse(ParseError),
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
}

#[derive(Debug, Clone)]
pub struct AnalyzeRng {
    state: u64,
}

impl AnalyzeRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    pub fn gen_range_u64(&mut self, upper_exclusive: u64) -> u64 {
        if upper_exclusive <= 1 {
            return 0;
        }
        self.next_u64() % upper_exclusive
    }
}

#[derive(Debug, Clone)]
pub struct BlockSampler {
    chosen_blocks: Vec<u32>,
    next_index: usize,
}

impl BlockSampler {
    pub fn new(nblocks: u32, target_blocks: u32, rng: &mut AnalyzeRng) -> Self {
        let target_blocks = target_blocks.min(nblocks);
        if target_blocks == 0 {
            return Self {
                chosen_blocks: Vec::new(),
                next_index: 0,
            };
        }

        let mut chosen = HashSet::new();
        while chosen.len() < target_blocks as usize {
            chosen.insert(rng.gen_range_u64(nblocks as u64) as u32);
        }
        let mut chosen_blocks = chosen.into_iter().collect::<Vec<_>>();
        chosen_blocks.sort_unstable();
        Self {
            chosen_blocks,
            next_index: 0,
        }
    }
}

impl Iterator for BlockSampler {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.chosen_blocks.get(self.next_index).copied()?;
        self.next_index += 1;
        Some(block)
    }
}

#[derive(Debug, Clone)]
pub struct ReservoirSampler<T> {
    target_size: usize,
    seen: usize,
    sample: Vec<T>,
}

#[derive(Debug, Clone)]
pub struct SampledRow {
    pub physical_ordinal: usize,
    pub values: Vec<Value>,
    pub widths: Vec<usize>,
}

impl<T> ReservoirSampler<T> {
    pub fn new(target_size: usize) -> Self {
        Self {
            target_size,
            seen: 0,
            sample: Vec::with_capacity(target_size),
        }
    }

    pub fn push(&mut self, value: T, rng: &mut AnalyzeRng) {
        self.seen += 1;
        if self.target_size == 0 {
            return;
        }
        if self.sample.len() < self.target_size {
            self.sample.push(value);
            return;
        }
        let replacement = rng.gen_range_u64(self.seen as u64) as usize;
        if replacement < self.target_size {
            self.sample[replacement] = value;
        }
    }

    pub fn into_inner(self) -> Vec<T> {
        self.sample
    }
}

pub fn target_sample_rows(statistics_target: i16) -> usize {
    let target = if statistics_target <= 0 {
        DEFAULT_STATISTICS_TARGET as usize
    } else {
        statistics_target as usize
    };
    target.saturating_mul(300)
}

pub fn target_sample_blocks(
    nblocks: u32,
    sample_rows: usize,
    estimated_rows_per_block: usize,
) -> u32 {
    if nblocks == 0 || sample_rows == 0 {
        return 0;
    }
    let rows_per_block = estimated_rows_per_block.max(1);
    (sample_rows.div_ceil(rows_per_block) as u32).clamp(1, nblocks)
}

pub fn selected_columns(
    relation: &BoundRelation,
    target: &MaintenanceTarget,
) -> Result<Vec<usize>, AnalyzeError> {
    if target.columns.is_empty() {
        return Ok((0..relation.desc.columns.len()).collect());
    }
    let mut out = Vec::with_capacity(target.columns.len());
    for column in &target.columns {
        let index = relation
            .desc
            .columns
            .iter()
            .position(|desc| desc.name.eq_ignore_ascii_case(column))
            .ok_or_else(|| AnalyzeError::Parse(ParseError::UnknownColumn(column.clone())))?;
        out.push(index);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

pub fn statistics_has_zero_column_target(
    stat: &PgStatisticExtRow,
    relation: &BoundRelation,
) -> bool {
    stat.stxkeys.iter().any(|attnum| {
        attnum
            .checked_sub(1)
            .and_then(|idx| relation.desc.columns.get(idx as usize))
            .is_some_and(|column| column.attstattarget == 0)
    })
}

pub fn qualified_statistics_name(stat: &PgStatisticExtRow, catalog: &dyn CatalogLookup) -> String {
    let namespace = catalog
        .namespace_row_by_oid(stat.stxnamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| stat.stxnamespace.to_string());
    format!("{namespace}.{}", stat.stxname)
}

pub fn qualified_relation_name(relation: &BoundRelation, catalog: &dyn CatalogLookup) -> String {
    let namespace = catalog
        .namespace_row_by_oid(relation.namespace_oid)
        .map(|row| row.nspname)
        .unwrap_or_else(|| relation.namespace_oid.to_string());
    let relname = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    format!("{namespace}.{relname}")
}

pub fn statistics_expression_texts(raw: &Option<String>) -> Result<Vec<String>, AnalyzeError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    serde_json::from_str::<Vec<String>>(raw).map_err(|err| AnalyzeError::Detailed {
        message: "could not parse stored statistics expressions".into(),
        detail: Some(err.to_string()),
        hint: None,
        sqlstate: "XX000",
    })
}

pub fn extended_statistics_target(stat: &PgStatisticExtRow, relation: &BoundRelation) -> i16 {
    stat.stxstattarget
        .unwrap_or_else(|| {
            stat.stxkeys
                .iter()
                .filter_map(|attnum| {
                    attnum.checked_sub(1).and_then(|idx| {
                        relation
                            .desc
                            .columns
                            .get(idx as usize)
                            .map(|column| column.attstattarget)
                    })
                })
                .filter(|target| *target > 0)
                .max()
                .unwrap_or(DEFAULT_STATISTICS_TARGET)
        })
        .max(1)
}

pub fn estimate_distinct(
    distinct_seen: usize,
    nonnull_rows: usize,
    reltuples: f64,
    stanullfrac: f64,
) -> f64 {
    if nonnull_rows == 0 {
        return 0.0;
    }
    if distinct_seen == nonnull_rows {
        return -1.0;
    }
    let estimated = (distinct_seen as f64 / nonnull_rows as f64)
        * (reltuples * (1.0 - stanullfrac)).max(nonnull_rows as f64);
    if reltuples > 0.0 && estimated > reltuples * 0.1 {
        -(estimated / reltuples.max(1.0))
    } else {
        estimated.max(1.0)
    }
}

pub fn sample_correlation(values: &[(usize, String)]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
    let ranks = sorted
        .into_iter()
        .enumerate()
        .map(|(rank, (physical, _))| (physical, rank as f64))
        .collect::<std::collections::HashMap<_, _>>();

    let n = values.len() as f64;
    let mean = (n - 1.0) / 2.0;
    let mut num = 0.0;
    let mut left_den = 0.0;
    let mut right_den = 0.0;
    for (physical, _) in values {
        let left = *physical as f64 - mean;
        let right = ranks.get(physical).copied().unwrap_or(0.0) - mean;
        num += left * right;
        left_den += left * left;
        right_den += right * right;
    }
    if left_den == 0.0 || right_den == 0.0 {
        0.0
    } else {
        num / (left_den.sqrt() * right_den.sqrt())
    }
}

pub fn build_statistics_rows(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    selected_columns: &[usize],
    sample_rows: &[SampledRow],
    reltuples: f64,
    catalog: &dyn CatalogLookup,
    stainherit: bool,
    mut value_key: impl FnMut(&Value) -> Option<String>,
) -> Vec<PgStatisticRow> {
    let mut out = Vec::with_capacity(selected_columns.len());
    let sample_total = sample_rows.len().max(1) as f64;
    for (sample_index, column_index) in selected_columns.iter().enumerate() {
        let column = &relation_desc.columns[*column_index];
        let nonnull_rows = sample_rows
            .iter()
            .filter(|row| !matches!(row.values[sample_index], Value::Null))
            .collect::<Vec<_>>();
        let null_count = sample_rows.len().saturating_sub(nonnull_rows.len());
        let stanullfrac = null_count as f64 / sample_total;
        let stawidth = if nonnull_rows.is_empty() {
            i32::from(column.storage.attlen.max(0))
        } else {
            (nonnull_rows
                .iter()
                .map(|row| row.widths[sample_index])
                .sum::<usize>() as f64
                / nonnull_rows.len() as f64)
                .round() as i32
        };

        let mut freq = HashMap::<String, (usize, Value)>::new();
        let mut rendered_values = Vec::with_capacity(nonnull_rows.len());
        for row in &nonnull_rows {
            let value = row.values[sample_index].to_owned_value();
            let rendered = value_key(&value).unwrap_or_else(|| "NULL".into());
            freq.entry(rendered.clone())
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, value));
            rendered_values.push((row.physical_ordinal, rendered));
        }
        let distinct_seen = freq.len();
        let stadistinct =
            estimate_distinct(distinct_seen, nonnull_rows.len(), reltuples, stanullfrac);

        let type_oid = catalog
            .type_oid_for_sql_type(column.sql_type)
            .unwrap_or(TEXT_TYPE_OID);
        let eq_op = catalog
            .operator_by_name_left_right("=", type_oid, type_oid)
            .map(|row| row.oid)
            .unwrap_or(0);
        let lt_op = catalog
            .operator_by_name_left_right("<", type_oid, type_oid)
            .map(|row| row.oid)
            .unwrap_or(0);

        let target = if column.attstattarget > 0 {
            column.attstattarget as usize
        } else {
            DEFAULT_STATISTICS_TARGET as usize
        };
        let value_slot_target = target.min((4_000 / (stawidth.max(1) as usize + 32)).max(1));
        let mut stakind = [0; 5];
        let mut staop = [0; 5];
        let mut stacoll = [0; 5];
        let mut stanumbers: [Option<ArrayValue>; 5] = Default::default();
        let mut stavalues: [Option<ArrayValue>; 5] = Default::default();
        let mut slot_idx = 0usize;
        let supports_value_slots = !column.sql_type.is_array;

        let mut ranked = freq.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|left, right| right.1.0.cmp(&left.1.0).then_with(|| left.0.cmp(&right.0)));

        let mcv = ranked
            .iter()
            .filter(|(_, (count, _))| *count > 1)
            .take(value_slot_target)
            .cloned()
            .collect::<Vec<_>>();
        if supports_value_slots && !mcv.is_empty() {
            stakind[slot_idx] = STATISTIC_KIND_MCV;
            staop[slot_idx] = eq_op;
            stacoll[slot_idx] = column.collation_oid;
            stanumbers[slot_idx] = Some(ArrayValue::from_1d(
                mcv.iter()
                    .map(|(_, (count, _))| Value::Float64(*count as f64 / sample_total))
                    .collect(),
            ));
            stavalues[slot_idx] = Some(
                ArrayValue::from_1d(
                    mcv.iter()
                        .map(|(_, (_, value))| value.to_owned_value())
                        .collect(),
                )
                .with_element_type_oid(type_oid),
            );
            slot_idx += 1;
        }

        if supports_value_slots && lt_op != 0 {
            let mcv_values = mcv
                .iter()
                .map(|(value, _)| value.clone())
                .collect::<HashSet<_>>();
            let mut histogram_values = ranked
                .iter()
                .map(|(value, (_, representative))| {
                    (value.clone(), representative.to_owned_value())
                })
                .filter(|(value, _)| !mcv_values.contains(value))
                .collect::<Vec<_>>();
            histogram_values.sort_by(|left, right| left.0.cmp(&right.0));
            histogram_values.dedup_by(|left, right| left.0 == right.0);
            if histogram_values.len() >= 2 {
                stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
                staop[slot_idx] = lt_op;
                stacoll[slot_idx] = column.collation_oid;
                stavalues[slot_idx] = Some(
                    ArrayValue::from_1d(
                        histogram_values
                            .into_iter()
                            .step_by((distinct_seen.max(2) / value_slot_target.max(2)).max(1))
                            .map(|(_, value)| value)
                            .collect(),
                    )
                    .with_element_type_oid(type_oid),
                );
                slot_idx += 1;
            }

            let correlation = sample_correlation(&rendered_values);
            stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
            staop[slot_idx] = lt_op;
            stacoll[slot_idx] = column.collation_oid;
            stanumbers[slot_idx] = Some(ArrayValue::from_1d(vec![Value::Float64(correlation)]));
        }

        out.push(PgStatisticRow {
            starelid: relation_oid,
            staattnum: (*column_index + 1) as i16,
            stainherit,
            stanullfrac,
            stawidth,
            stadistinct,
            stakind,
            staop,
            stacoll,
            stanumbers,
            stavalues,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::{
        PG_CATALOG_NAMESPACE_OID, PgClassRow, PgNamespaceRow, desc::column_desc,
    };
    use pgrust_core::RelFileLocator;
    use pgrust_nodes::{SqlType, SqlTypeKind, primnodes::RelationDesc};

    struct TestCatalog;

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }

        fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
            (oid == PG_CATALOG_NAMESPACE_OID).then(|| PgNamespaceRow {
                oid,
                nspname: "pg_catalog".into(),
                nspowner: 10,
                nspacl: None,
            })
        }

        fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
            (relation_oid == 42).then(|| PgClassRow {
                oid: 42,
                relname: "t".into(),
                relnamespace: PG_CATALOG_NAMESPACE_OID,
                reltype: 0,
                relowner: 10,
                relam: 0,
                relfilenode: 42,
                reltablespace: 0,
                relpages: 0,
                reltuples: 0.0,
                relallvisible: 0,
                relallfrozen: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relpersistence: 'p',
                relkind: 'r',
                relnatts: 1,
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
                relreplident: 'd',
                reloftype: 0,
            })
        }
    }

    fn bound_relation() -> BoundRelation {
        let mut zero_target = column_desc("b", SqlType::new(SqlTypeKind::Text), false);
        zero_target.attstattarget = 0;
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
            desc: RelationDesc {
                columns: vec![
                    column_desc("a", SqlType::new(SqlTypeKind::Int4), false),
                    zero_target,
                ],
            },
            partitioned_table: None,
            partition_spec: None,
        }
    }

    fn statistics_ext_row() -> PgStatisticExtRow {
        PgStatisticExtRow {
            oid: 1,
            stxrelid: 42,
            stxname: "st".into(),
            stxnamespace: PG_CATALOG_NAMESPACE_OID,
            stxowner: 10,
            stxstattarget: None,
            stxkeys: vec![1, 2],
            stxkind: vec![b'd'],
            stxexprs: None,
        }
    }

    #[test]
    fn block_sampler_returns_unique_sorted_blocks() {
        let mut rng = AnalyzeRng::new(123);
        let blocks = BlockSampler::new(20, 5, &mut rng).collect::<Vec<_>>();
        assert_eq!(blocks.len(), 5);
        assert!(blocks.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn reservoir_sampler_keeps_requested_size() {
        let mut rng = AnalyzeRng::new(42);
        let mut sampler = ReservoirSampler::new(10);
        for i in 0..1000 {
            sampler.push(i, &mut rng);
        }
        assert_eq!(sampler.into_inner().len(), 10);
    }

    #[test]
    fn target_sample_rows_matches_postgres_shape() {
        assert_eq!(target_sample_rows(1), 300);
        assert_eq!(target_sample_rows(100), 30_000);
    }

    #[test]
    fn selected_columns_resolves_names_and_deduplicates() {
        let target = MaintenanceTarget {
            table_name: "t".into(),
            columns: vec!["b".into(), "a".into(), "b".into()],
            only: false,
        };

        assert_eq!(
            selected_columns(&bound_relation(), &target).unwrap(),
            vec![0, 1]
        );
    }

    #[test]
    fn extended_statistics_target_uses_positive_column_targets() {
        assert!(statistics_has_zero_column_target(
            &statistics_ext_row(),
            &bound_relation()
        ));
        assert_eq!(
            extended_statistics_target(&statistics_ext_row(), &bound_relation()),
            100
        );
        assert_eq!(
            qualified_relation_name(&bound_relation(), &TestCatalog),
            "pg_catalog.t"
        );
        assert_eq!(
            qualified_statistics_name(&statistics_ext_row(), &TestCatalog),
            "pg_catalog.st"
        );
    }

    #[test]
    fn statistics_expression_texts_parses_json_array() {
        assert_eq!(
            statistics_expression_texts(&Some("[\"lower(a)\"]".into())).unwrap(),
            vec!["lower(a)".to_string()]
        );
        assert!(matches!(
            statistics_expression_texts(&Some("not json".into())),
            Err(AnalyzeError::Detailed {
                message,
                sqlstate: "XX000",
                ..
            }) if message == "could not parse stored statistics expressions"
        ));
    }

    #[test]
    fn estimate_distinct_and_correlation_match_expected_shape() {
        assert_eq!(estimate_distinct(0, 0, 100.0, 0.0), 0.0);
        assert_eq!(estimate_distinct(10, 10, 100.0, 0.0), -1.0);
        let correlation = sample_correlation(&[(0, "a".into()), (1, "b".into()), (2, "c".into())]);
        assert!((correlation - 1.0).abs() < 1e-12);
    }
}
