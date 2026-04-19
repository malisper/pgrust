use std::collections::{BTreeSet, HashMap, HashSet};

use crate::backend::executor::value_io::decode_value_with_toast;
use crate::backend::executor::{
    ExecError, ExecutorContext, Value, format_array_value_text, render_datetime_value_text,
    render_internal_char_text, render_tsquery_text, render_tsvector_text,
};
use crate::backend::parser::{BoundRelation, CatalogLookup, ParseError};
use crate::backend::storage::page::bufpage::ItemIdFlags;
use crate::backend::storage::page::bufpage::{
    page_get_item_id_unchecked, page_get_item_unchecked, page_get_max_offset_number,
};
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::storage::smgr::StorageManager;
use crate::include::access::htup::HeapTuple;
use crate::include::catalog::PgStatisticRow;
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::include::nodes::parsenodes::MaintenanceTarget;

const DEFAULT_STATISTICS_TARGET: i16 = 100;
const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;

#[derive(Debug, Clone)]
pub(crate) struct AnalyzeRelationStats {
    pub relation_oid: u32,
    pub relpages: i32,
    pub reltuples: f64,
    pub statistics: Vec<PgStatisticRow>,
}

#[derive(Debug, Clone)]
struct InheritanceAnalyzeStats {
    reltuples: f64,
    statistics: Vec<PgStatisticRow>,
}

#[derive(Debug, Clone)]
struct AnalyzeRng {
    state: u64,
}

impl AnalyzeRng {
    fn new(seed: u64) -> Self {
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

    fn gen_range_u64(&mut self, upper_exclusive: u64) -> u64 {
        if upper_exclusive <= 1 {
            return 0;
        }
        self.next_u64() % upper_exclusive
    }
}

#[derive(Debug, Clone)]
struct BlockSampler {
    chosen_blocks: Vec<u32>,
    next_index: usize,
}

impl BlockSampler {
    fn new(nblocks: u32, target_blocks: u32, rng: &mut AnalyzeRng) -> Self {
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
struct ReservoirSampler<T> {
    target_size: usize,
    seen: usize,
    sample: Vec<T>,
}

impl<T> ReservoirSampler<T> {
    fn new(target_size: usize) -> Self {
        Self {
            target_size,
            seen: 0,
            sample: Vec::with_capacity(target_size),
        }
    }

    fn push(&mut self, value: T, rng: &mut AnalyzeRng) {
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

    fn into_inner(self) -> Vec<T> {
        self.sample
    }
}

#[derive(Debug, Clone)]
struct SampledRow {
    physical_ordinal: usize,
    values: Vec<Value>,
    widths: Vec<usize>,
}

pub(crate) fn collect_analyze_stats(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<AnalyzeRelationStats>, ExecError> {
    let mut out = Vec::with_capacity(targets.len());
    for target in targets {
        ctx.check_for_interrupts()?;
        let relation = catalog
            .lookup_analyzable_relation(&target.table_name)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(target.table_name.clone())))?;
        let selected = selected_columns(&relation, target)?;
        if relation.relkind == 'p' {
            let inherited = if target.only {
                InheritanceAnalyzeStats {
                    reltuples: 0.0,
                    statistics: Vec::new(),
                }
            } else {
                sample_inheritance_tree(&relation, &selected, catalog, ctx)?
            };
            out.push(AnalyzeRelationStats {
                relation_oid: relation.relation_oid,
                relpages: -1,
                reltuples: inherited.reltuples,
                statistics: inherited.statistics,
            });
            continue;
        }
        let root_stats = sample_relation(&relation, &selected, catalog, ctx)?;
        let mut statistics = root_stats.statistics;
        if !target.only && catalog.has_subclass(relation.relation_oid) {
            statistics
                .extend(sample_inheritance_tree(&relation, &selected, catalog, ctx)?.statistics);
        }
        out.push(AnalyzeRelationStats {
            relation_oid: relation.relation_oid,
            relpages: root_stats.relpages,
            reltuples: root_stats.reltuples,
            statistics,
        });
    }
    Ok(out)
}

fn selected_columns(
    relation: &BoundRelation,
    target: &MaintenanceTarget,
) -> Result<Vec<usize>, ExecError> {
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
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column.clone())))?;
        out.push(index);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn sample_relation(
    relation: &BoundRelation,
    selected_columns: &[usize],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<AnalyzeRelationStats, ExecError> {
    let nblocks = ctx
        .pool
        .with_storage_mut(|s| s.smgr.nblocks(relation.rel, ForkNumber::Main))
        .map_err(crate::backend::access::heap::heapam::HeapError::Storage)?;
    let relpages = nblocks as i32;

    let stats_target = selected_columns
        .iter()
        .map(|index| relation.desc.columns[*index].attstattarget)
        .filter(|target| *target > 0)
        .max()
        .unwrap_or(DEFAULT_STATISTICS_TARGET);
    let sample_rows_target = target_sample_rows(stats_target);
    let sample_block_target = target_sample_blocks(nblocks, sample_rows_target, 32);

    let mut rng =
        AnalyzeRng::new((relation.relation_oid as u64) << 32 | (ctx.next_command_id as u64));
    let sampled_blocks =
        BlockSampler::new(nblocks, sample_block_target, &mut rng).collect::<BTreeSet<_>>();
    let sampled_block_count = sampled_blocks.len();
    let mut reservoir = ReservoirSampler::new(sample_rows_target);
    let mut visible_rows_on_sampled_blocks = 0usize;
    let toast_ctx = relation.toast.map(|toast| ToastFetchContext {
        relation: toast,
        pool: ctx.pool.clone(),
        txns: ctx.txns.clone(),
        snapshot: ctx.snapshot.clone(),
        client_id: ctx.client_id,
    });

    for block in &sampled_blocks {
        ctx.check_for_interrupts()?;
        let pin = ctx
            .pool
            .pin_existing_block(ctx.client_id, relation.rel, ForkNumber::Main, *block)
            .map_err(crate::backend::access::heap::heapam::HeapError::Buffer)?;
        let buffer_id = pin.buffer_id();
        let guard = ctx
            .pool
            .lock_buffer_shared(buffer_id)
            .map_err(crate::backend::access::heap::heapam::HeapError::Buffer)?;
        let page = &*guard;
        let max_offset = page_get_max_offset_number(page)
            .map_err(crate::include::access::htup::TupleError::from)?;
        for off in 1..=max_offset {
            ctx.check_for_interrupts()?;
            let item_id = page_get_item_id_unchecked(page, off);
            if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                continue;
            }
            let tuple_bytes = page_get_item_unchecked(page, off);
            let visible = if let Some(visible) =
                ctx.snapshot.tuple_bytes_try_visible_from_hints(tuple_bytes)
            {
                visible
            } else {
                let txns = ctx.txns.read();
                ctx.snapshot
                    .tuple_bytes_visible_with_hints(&txns, tuple_bytes)
                    .0
            };
            if !visible {
                continue;
            }
            let tuple = HeapTuple::parse(tuple_bytes)?;
            let raw = tuple.deform(&relation.desc.attribute_descs())?;
            let mut values = Vec::with_capacity(selected_columns.len());
            let mut widths = Vec::with_capacity(selected_columns.len());
            for index in selected_columns {
                let value = decode_value_with_toast(
                    &relation.desc.columns[*index],
                    raw.get(*index).copied().flatten(),
                    toast_ctx.as_ref(),
                )?;
                widths.push(
                    raw.get(*index)
                        .and_then(|bytes| *bytes)
                        .map(|bytes| bytes.len())
                        .unwrap_or(0),
                );
                values.push(value);
            }
            reservoir.push(
                SampledRow {
                    physical_ordinal: visible_rows_on_sampled_blocks,
                    values,
                    widths,
                },
                &mut rng,
            );
            visible_rows_on_sampled_blocks += 1;
        }
    }

    let reltuples = if sampled_block_count == 0 || nblocks == 0 {
        0.0
    } else {
        (visible_rows_on_sampled_blocks as f64 / sampled_block_count as f64) * nblocks as f64
    };
    let rows = reservoir.into_inner();
    let statistics = build_statistics_rows(
        relation.relation_oid,
        &relation.desc,
        selected_columns,
        &rows,
        reltuples,
        catalog,
        false,
    )?;

    Ok(AnalyzeRelationStats {
        relation_oid: relation.relation_oid,
        relpages,
        reltuples,
        statistics,
    })
}

fn sample_inheritance_tree(
    relation: &BoundRelation,
    selected_columns: &[usize],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<InheritanceAnalyzeStats, ExecError> {
    let stats_target = selected_columns
        .iter()
        .map(|index| relation.desc.columns[*index].attstattarget)
        .filter(|target| *target > 0)
        .max()
        .unwrap_or(DEFAULT_STATISTICS_TARGET);
    let sample_rows_target = target_sample_rows(stats_target);
    let mut rng =
        AnalyzeRng::new((relation.relation_oid as u64) << 32 | (ctx.next_command_id as u64) ^ 1);
    let mut reservoir = ReservoirSampler::new(sample_rows_target);
    let mut visible_rows = 0usize;

    for member_oid in catalog.find_all_inheritors(relation.relation_oid) {
        let Some(member) = catalog.relation_by_oid(member_oid) else {
            continue;
        };
        let mapping = inherited_selected_column_mapping(relation, &member, selected_columns)?;
        let toast_ctx = member.toast.map(|toast| ToastFetchContext {
            relation: toast,
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            snapshot: ctx.snapshot.clone(),
            client_id: ctx.client_id,
        });
        let nblocks = ctx
            .pool
            .with_storage_mut(|s| s.smgr.nblocks(member.rel, ForkNumber::Main))
            .map_err(crate::backend::access::heap::heapam::HeapError::Storage)?;
        for block in 0..nblocks {
            ctx.check_for_interrupts()?;
            let pin = ctx
                .pool
                .pin_existing_block(ctx.client_id, member.rel, ForkNumber::Main, block)
                .map_err(crate::backend::access::heap::heapam::HeapError::Buffer)?;
            let buffer_id = pin.buffer_id();
            let guard = ctx
                .pool
                .lock_buffer_shared(buffer_id)
                .map_err(crate::backend::access::heap::heapam::HeapError::Buffer)?;
            let page = &*guard;
            let max_offset = page_get_max_offset_number(page)
                .map_err(crate::include::access::htup::TupleError::from)?;
            for off in 1..=max_offset {
                ctx.check_for_interrupts()?;
                let item_id = page_get_item_id_unchecked(page, off);
                if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                    continue;
                }
                let tuple_bytes = page_get_item_unchecked(page, off);
                let visible = if let Some(visible) =
                    ctx.snapshot.tuple_bytes_try_visible_from_hints(tuple_bytes)
                {
                    visible
                } else {
                    let txns = ctx.txns.read();
                    ctx.snapshot
                        .tuple_bytes_visible_with_hints(&txns, tuple_bytes)
                        .0
                };
                if !visible {
                    continue;
                }
                let tuple = HeapTuple::parse(tuple_bytes)?;
                let raw = tuple.deform(&member.desc.attribute_descs())?;
                let mut values = Vec::with_capacity(selected_columns.len());
                let mut widths = Vec::with_capacity(selected_columns.len());
                for mapped_index in &mapping {
                    if let Some(index) = mapped_index {
                        let value = decode_value_with_toast(
                            &member.desc.columns[*index],
                            raw.get(*index).copied().flatten(),
                            toast_ctx.as_ref(),
                        )?;
                        widths.push(
                            raw.get(*index)
                                .and_then(|bytes| *bytes)
                                .map(|bytes| bytes.len())
                                .unwrap_or(0),
                        );
                        values.push(value);
                    } else {
                        values.push(Value::Null);
                        widths.push(0);
                    }
                }
                reservoir.push(
                    SampledRow {
                        physical_ordinal: visible_rows,
                        values,
                        widths,
                    },
                    &mut rng,
                );
                visible_rows += 1;
            }
        }
    }

    Ok(InheritanceAnalyzeStats {
        reltuples: visible_rows as f64,
        statistics: build_statistics_rows(
            relation.relation_oid,
            &relation.desc,
            selected_columns,
            &reservoir.into_inner(),
            visible_rows as f64,
            catalog,
            true,
        )?,
    })
}

fn inherited_selected_column_mapping(
    parent: &BoundRelation,
    member: &BoundRelation,
    selected_columns: &[usize],
) -> Result<Vec<Option<usize>>, ExecError> {
    selected_columns
        .iter()
        .map(|index| {
            let parent_column = &parent.desc.columns[*index];
            Ok(member.desc.columns.iter().position(|child_column| {
                !child_column.dropped
                    && child_column.name.eq_ignore_ascii_case(&parent_column.name)
                    && child_column.sql_type == parent_column.sql_type
            }))
        })
        .collect()
}

fn build_statistics_rows(
    relation_oid: u32,
    relation_desc: &crate::backend::executor::RelationDesc,
    selected_columns: &[usize],
    sample_rows: &[SampledRow],
    reltuples: f64,
    catalog: &dyn CatalogLookup,
    stainherit: bool,
) -> Result<Vec<PgStatisticRow>, ExecError> {
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
            let rendered = value_stats_text(&value);
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
            .unwrap_or(crate::include::catalog::TEXT_TYPE_OID);
        let eq_op = catalog
            .operator_by_name_left_right("=", type_oid, type_oid)
            .map(|row| row.oid)
            .unwrap_or(0);
        let lt_op = catalog
            .operator_by_name_left_right("<", type_oid, type_oid)
            .map(|row| row.oid)
            .unwrap_or(0);

        let target = column.attstattarget.max(1) as usize;
        let mut stakind = [0; 5];
        let mut staop = [0; 5];
        let stacoll = [0; 5];
        let mut stanumbers: [Option<ArrayValue>; 5] = Default::default();
        let mut stavalues: [Option<ArrayValue>; 5] = Default::default();

        let mut ranked = freq.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|left, right| right.1.0.cmp(&left.1.0).then_with(|| left.0.cmp(&right.0)));

        let mcv = ranked
            .iter()
            .filter(|(_, (count, _))| *count > 1)
            .take(target)
            .cloned()
            .collect::<Vec<_>>();
        if !mcv.is_empty() {
            stakind[0] = STATISTIC_KIND_MCV;
            staop[0] = eq_op;
            stanumbers[0] = Some(ArrayValue::from_1d(
                mcv.iter()
                    .map(|(_, (count, _))| Value::Float64(*count as f64 / sample_total))
                    .collect(),
            ));
            stavalues[0] = Some(
                ArrayValue::from_1d(
                    mcv.iter()
                        .map(|(_, (_, value))| value.to_owned_value())
                        .collect(),
                )
                .with_element_type_oid(type_oid),
            );
        }

        if lt_op != 0 {
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
                stakind[1] = STATISTIC_KIND_HISTOGRAM;
                staop[1] = lt_op;
                stavalues[1] = Some(
                    ArrayValue::from_1d(
                        histogram_values
                            .into_iter()
                            .step_by((distinct_seen.max(2) / target.max(2)).max(1))
                            .map(|(_, value)| value)
                            .collect(),
                    )
                    .with_element_type_oid(type_oid),
                );
            }

            let correlation = sample_correlation(&rendered_values);
            stakind[2] = STATISTIC_KIND_CORRELATION;
            staop[2] = lt_op;
            stanumbers[2] = Some(ArrayValue::from_1d(vec![Value::Float64(correlation)]));
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
    Ok(out)
}

fn estimate_distinct(
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

fn sample_correlation(values: &[(usize, String)]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
    let ranks = sorted
        .into_iter()
        .enumerate()
        .map(|(rank, (physical, _))| (physical, rank as f64))
        .collect::<HashMap<_, _>>();

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

fn value_stats_text(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Text(text) => text.to_string(),
        Value::TextRef(_, _) => value.as_text().unwrap_or_default().to_string(),
        Value::Numeric(v) => v.render(),
        Value::Date(_) => render_datetime_value_text(value).unwrap_or_else(|| format!("{value:?}")),
        Value::Time(_) => render_datetime_value_text(value).unwrap_or_else(|| format!("{value:?}")),
        Value::TimeTz(_) => {
            render_datetime_value_text(value).unwrap_or_else(|| format!("{value:?}"))
        }
        Value::Timestamp(_) => {
            render_datetime_value_text(value).unwrap_or_else(|| format!("{value:?}"))
        }
        Value::TimestampTz(_) => {
            render_datetime_value_text(value).unwrap_or_else(|| format!("{value:?}"))
        }
        Value::Bytea(v) => format!("{v:?}"),
        Value::Bit(v) => format!("{:?}", v.bytes),
        Value::Array(values) => format_array_value_text(&ArrayValue::from_1d(values.clone())),
        Value::PgArray(array) => format_array_value_text(array),
        Value::TsVector(v) => render_tsvector_text(v),
        Value::TsQuery(v) => render_tsquery_text(v),
        Value::InternalChar(v) => render_internal_char_text(*v),
        Value::Json(text) => text.to_string(),
        Value::Jsonb(bytes) => format!("{bytes:?}"),
        Value::JsonPath(text) => text.to_string(),
        other => format!("{other:?}"),
    }
}

fn target_sample_rows(statistics_target: i16) -> usize {
    let target = if statistics_target <= 0 {
        DEFAULT_STATISTICS_TARGET as usize
    } else {
        statistics_target as usize
    };
    target.saturating_mul(300)
}

fn target_sample_blocks(nblocks: u32, sample_rows: usize, estimated_rows_per_block: usize) -> u32 {
    if nblocks == 0 || sample_rows == 0 {
        return 0;
    }
    let rows_per_block = estimated_rows_per_block.max(1);
    (sample_rows.div_ceil(rows_per_block) as u32).clamp(1, nblocks)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
