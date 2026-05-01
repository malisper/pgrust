use std::collections::{BTreeSet, HashMap, HashSet};

use super::tablecmds::{
    collect_matching_rows_heap, index_key_values_for_row, row_matches_index_predicate,
};
use crate::RelFileLocator;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::value_io::decode_value_with_toast;
use crate::backend::executor::{ExecError, ExecutorContext, TupleSlot, Value, eval_expr};
use crate::backend::parser::analyze::{bind_expr_with_outer_and_ctes, scope_for_relation};
use crate::backend::parser::{BoundIndexRelation, BoundRelation, CatalogLookup, ParseError};
use crate::backend::statistics::build::build_extended_statistics_payloads;
use crate::backend::statistics::types::statistics_value_key;
use crate::backend::storage::page::bufpage::ItemIdFlags;
use crate::backend::storage::page::bufpage::{
    page_get_item_id_unchecked, page_get_item_unchecked, page_get_max_offset_number,
};
use crate::backend::storage::smgr::{ForkNumber, SmgrError, StorageManager};
use crate::include::access::htup::HeapTuple;
use crate::include::catalog::{PgStatisticExtDataRow, PgStatisticRow, relkind_has_storage};
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::include::nodes::parsenodes::MaintenanceTarget;
use crate::include::nodes::primnodes::expr_sql_type_hint;

const DEFAULT_STATISTICS_TARGET: i16 = 100;
const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;

#[derive(Debug, Clone)]
pub(crate) struct AnalyzeRelationStats {
    pub relation_oid: u32,
    pub relpages: i32,
    pub reltuples: f64,
    pub clear_relhassubclass: bool,
    pub statistics: Vec<PgStatisticRow>,
    pub statistics_ext_data: Vec<PgStatisticExtDataRow>,
}

#[derive(Debug, Clone)]
struct InheritanceAnalyzeStats {
    reltuples: f64,
    statistics: Vec<PgStatisticRow>,
    statistics_ext_data: Vec<PgStatisticExtDataRow>,
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
        collect_analyze_stats_for_relation(
            &relation,
            selected,
            target.only,
            catalog,
            ctx,
            &mut out,
        )?;
    }
    Ok(out)
}

pub(crate) fn collect_analyze_stats_for_relations(
    relations: &[BoundRelation],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<AnalyzeRelationStats>, ExecError> {
    let mut out = Vec::with_capacity(relations.len());
    for relation in relations {
        ctx.check_for_interrupts()?;
        collect_analyze_stats_for_relation(
            relation,
            (0..relation.desc.columns.len()).collect(),
            false,
            catalog,
            ctx,
            &mut out,
        )?;
    }
    Ok(out)
}

fn collect_analyze_stats_for_relation(
    relation: &BoundRelation,
    selected: Vec<usize>,
    only: bool,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    out: &mut Vec<AnalyzeRelationStats>,
) -> Result<(), ExecError> {
    if relation.relkind == 'p' {
        let has_subclass = catalog.has_subclass(relation.relation_oid);
        let member_oids = (!only && has_subclass)
            .then(|| catalog.find_all_inheritors(relation.relation_oid))
            .unwrap_or_default();
        let clear_relhassubclass = has_subclass && !only && member_oids.len() < 2;
        let inherited = if member_oids.len() < 2 {
            InheritanceAnalyzeStats {
                reltuples: 0.0,
                statistics: Vec::new(),
                statistics_ext_data: Vec::new(),
            }
        } else {
            sample_inheritance_tree(&relation, &selected, &member_oids, catalog, ctx)?
        };
        out.push(AnalyzeRelationStats {
            relation_oid: relation.relation_oid,
            relpages: -1,
            reltuples: inherited.reltuples,
            clear_relhassubclass,
            statistics: inherited.statistics,
            statistics_ext_data: inherited.statistics_ext_data,
        });
        return Ok(());
    }
    let root_stats = sample_relation(&relation, &selected, catalog, ctx)?;
    let mut statistics = root_stats.statistics;
    let mut statistics_ext_data = root_stats.statistics_ext_data;
    let mut clear_relhassubclass = false;
    if !only && catalog.has_subclass(relation.relation_oid) {
        let member_oids = catalog.find_all_inheritors(relation.relation_oid);
        if member_oids.len() < 2 {
            clear_relhassubclass = true;
        } else {
            let inherited =
                sample_inheritance_tree(&relation, &selected, &member_oids, catalog, ctx)?;
            statistics.extend(inherited.statistics);
            statistics_ext_data.extend(inherited.statistics_ext_data);
        }
    }
    out.push(AnalyzeRelationStats {
        relation_oid: relation.relation_oid,
        relpages: root_stats.relpages,
        reltuples: root_stats.reltuples,
        clear_relhassubclass,
        statistics,
        statistics_ext_data,
    });
    if selected.len() == relation.desc.columns.len() {
        out.extend(sample_expression_indexes(relation, catalog, ctx)?);
    }
    Ok(())
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

fn relation_nblocks_or_zero(
    ctx: &mut ExecutorContext,
    rel: RelFileLocator,
) -> Result<u32, ExecError> {
    ctx.pool
        .with_storage_mut(|s| match s.smgr.nblocks(rel, ForkNumber::Main) {
            Ok(nblocks) => Ok(nblocks),
            Err(SmgrError::RelationNotFound { .. }) => Ok(0),
            Err(err) => Err(err),
        })
        .map_err(crate::backend::access::heap::heapam::HeapError::Storage)
        .map_err(ExecError::from)
}

fn sample_relation(
    relation: &BoundRelation,
    selected_columns: &[usize],
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<AnalyzeRelationStats, ExecError> {
    let nblocks = relation_nblocks_or_zero(ctx, relation.rel)?;
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
    let expression_indexes = if selected_columns.len() == relation.desc.columns.len() {
        Vec::new()
    } else {
        analyze_expression_indexes(relation, catalog)
    };
    let mut expression_rows = Vec::new();
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
            if !expression_indexes.is_empty() {
                expression_rows.push(materialize_relation_row_values(
                    relation,
                    &raw,
                    toast_ctx.as_ref(),
                )?);
            }
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
    evaluate_expression_indexes_for_analyze(relation, &expression_indexes, &expression_rows, ctx)?;

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
    let statistics_ext_data = build_statistics_ext_data_rows(
        relation,
        selected_columns,
        &rows,
        reltuples,
        catalog,
        false,
        ctx,
    )?;

    Ok(AnalyzeRelationStats {
        relation_oid: relation.relation_oid,
        relpages,
        reltuples,
        clear_relhassubclass: false,
        statistics,
        statistics_ext_data,
    })
}

fn sample_expression_indexes(
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<Vec<AnalyzeRelationStats>, ExecError> {
    let expression_indexes = catalog
        .index_relations_for_heap(relation.relation_oid)
        .into_iter()
        .filter(|index| {
            index.index_meta.indkey.iter().any(|attnum| *attnum == 0)
                && index
                    .desc
                    .columns
                    .iter()
                    .any(|column| column.attstattarget != 0)
        })
        .collect::<Vec<_>>();
    if expression_indexes.is_empty() {
        return Ok(Vec::new());
    }

    let heap_rows =
        collect_matching_rows_heap(relation.rel, &relation.desc, relation.toast, None, ctx)?;
    let mut out = Vec::with_capacity(expression_indexes.len());
    for index in expression_indexes {
        let nblocks = relation_nblocks_or_zero(ctx, index.rel)?;
        let mut sample_rows = Vec::new();
        for (physical_ordinal, (heap_tid, values)) in heap_rows.iter().enumerate() {
            if !row_matches_index_predicate(
                &index,
                values,
                Some(*heap_tid),
                relation.relation_oid,
                ctx,
            )? {
                continue;
            }
            let key_values = index_key_values_for_row(&index, &relation.desc, values, ctx)?;
            let widths = key_values
                .iter()
                .map(|value| value_stats_text(value).len())
                .collect::<Vec<_>>();
            sample_rows.push(SampledRow {
                physical_ordinal,
                values: key_values,
                widths,
            });
        }
        let selected_columns = index
            .desc
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (column.attstattarget != 0).then_some(idx))
            .collect::<Vec<_>>();
        if selected_columns.is_empty() {
            continue;
        }
        let reltuples = sample_rows.len() as f64;
        let statistics = build_statistics_rows(
            index.relation_oid,
            &index.desc,
            &selected_columns,
            &sample_rows,
            reltuples,
            catalog,
            false,
        )?;
        out.push(AnalyzeRelationStats {
            relation_oid: index.relation_oid,
            relpages: nblocks as i32,
            reltuples,
            clear_relhassubclass: false,
            statistics,
            statistics_ext_data: Vec::new(),
        });
    }
    Ok(out)
}

fn sample_inheritance_tree(
    relation: &BoundRelation,
    selected_columns: &[usize],
    member_oids: &[u32],
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

    for member_oid in member_oids {
        let Some(member) = catalog.relation_by_oid(*member_oid) else {
            continue;
        };
        if !relkind_has_storage(member.relkind) {
            continue;
        }
        let mapping = inherited_selected_column_mapping(relation, &member, selected_columns)?;
        let toast_ctx = member.toast.map(|toast| ToastFetchContext {
            relation: toast,
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            snapshot: ctx.snapshot.clone(),
            client_id: ctx.client_id,
        });
        let nblocks = relation_nblocks_or_zero(ctx, member.rel)?;
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

    let rows = reservoir.into_inner();
    let reltuples = visible_rows as f64;
    Ok(InheritanceAnalyzeStats {
        reltuples,
        statistics: build_statistics_rows(
            relation.relation_oid,
            &relation.desc,
            selected_columns,
            &rows,
            reltuples,
            catalog,
            true,
        )?,
        statistics_ext_data: build_statistics_ext_data_rows(
            relation,
            selected_columns,
            &rows,
            reltuples,
            catalog,
            true,
            ctx,
        )?,
    })
}

fn analyze_expression_indexes(
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
) -> Vec<BoundIndexRelation> {
    catalog
        .index_relations_for_heap(relation.relation_oid)
        .into_iter()
        .filter(|index| {
            index.index_meta.indexprs.as_deref().is_some_and(|exprs| {
                !exprs.trim().is_empty()
                    && index.index_meta.indisvalid
                    && index.index_meta.indisready
            })
        })
        .collect()
}

fn materialize_relation_row_values(
    relation: &BoundRelation,
    raw: &[Option<&[u8]>],
    toast_ctx: Option<&ToastFetchContext>,
) -> Result<Vec<Value>, ExecError> {
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            decode_value_with_toast(column, raw.get(index).copied().flatten(), toast_ctx)
        })
        .collect()
}

fn evaluate_expression_indexes_for_analyze(
    relation: &BoundRelation,
    indexes: &[BoundIndexRelation],
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for values in rows {
        for index in indexes {
            if !crate::backend::commands::tablecmds::row_matches_index_predicate(
                index,
                values,
                None,
                relation.relation_oid,
                ctx,
            )? {
                continue;
            }
            let _ = crate::backend::commands::tablecmds::index_key_values_for_row(
                index,
                &relation.desc,
                values,
                ctx,
            )?;
        }
    }
    Ok(())
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

fn build_statistics_ext_data_rows(
    relation: &BoundRelation,
    selected_columns: &[usize],
    sample_rows: &[SampledRow],
    reltuples: f64,
    catalog: &dyn CatalogLookup,
    stxdinherit: bool,
    ctx: &mut ExecutorContext,
) -> Result<Vec<PgStatisticExtDataRow>, ExecError> {
    let statistic_ext_rows = catalog.statistic_ext_rows_for_relation(relation.relation_oid);
    if statistic_ext_rows.is_empty() {
        return Ok(Vec::new());
    }

    let selected_map = selected_columns
        .iter()
        .enumerate()
        .map(|(sample_index, column_index)| (*column_index, sample_index))
        .collect::<HashMap<_, _>>();
    let relation_name = catalog
        .class_row_by_oid(relation.relation_oid)
        .map(|row| row.relname)
        .unwrap_or_else(|| relation.relation_oid.to_string());
    let relation_scope = scope_for_relation(Some(&relation_name), &relation.desc);

    let mut out = Vec::new();
    for stat in statistic_ext_rows {
        if stat.stxstattarget == Some(0) {
            continue;
        }
        if statistics_has_zero_column_target(&stat, relation) {
            push_extended_statistics_warning(&stat, relation, catalog);
            continue;
        }
        let expression_texts = statistics_expression_texts(&stat.stxexprs)?;
        let mut target_ids = stat.stxkeys.clone();
        target_ids.extend((0..expression_texts.len()).map(|idx| -((idx as i16) + 1)));
        if target_ids.len() < 2 && expression_texts.is_empty() {
            continue;
        }

        let mut column_sample_indices = Vec::with_capacity(stat.stxkeys.len());
        let mut missing_column = false;
        for attnum in &stat.stxkeys {
            let Some(column_index) = attnum.checked_sub(1).map(|value| value as usize) else {
                missing_column = true;
                break;
            };
            let Some(sample_index) = selected_map.get(&column_index).copied() else {
                missing_column = true;
                break;
            };
            column_sample_indices.push(sample_index);
        }
        if missing_column {
            push_extended_statistics_warning(&stat, relation, catalog);
            continue;
        }

        let bound_exprs = expression_texts
            .iter()
            .map(|expr_text| {
                let parsed = crate::backend::parser::parse_expr(expr_text)?;
                bind_expr_with_outer_and_ctes(&parsed, &relation_scope, catalog, &[], None, &[])
            })
            .collect::<Result<Vec<_>, ParseError>>()?;
        if !bound_exprs.is_empty() && selected_columns.len() != relation.desc.columns.len() {
            push_extended_statistics_warning(&stat, relation, catalog);
            continue;
        }

        let mut rows = Vec::with_capacity(sample_rows.len());
        let mut expression_sample_rows = Vec::with_capacity(sample_rows.len());
        for row in sample_rows {
            let mut values = Vec::with_capacity(target_ids.len());
            for sample_index in &column_sample_indices {
                values.push(row.values[*sample_index].to_owned_value());
            }
            let mut expression_values = Vec::with_capacity(bound_exprs.len());
            let mut expression_widths = Vec::with_capacity(bound_exprs.len());
            if !bound_exprs.is_empty() {
                let mut slot = TupleSlot::virtual_row(row.values.clone());
                for expr in &bound_exprs {
                    let value = eval_expr(expr, &mut slot, ctx)?;
                    expression_widths.push(value_stats_text(&value).len());
                    values.push(value.to_owned_value());
                    expression_values.push(value);
                }
                expression_sample_rows.push(SampledRow {
                    physical_ordinal: row.physical_ordinal,
                    values: expression_values,
                    widths: expression_widths,
                });
            }
            rows.push(values);
        }

        let statistics_target = extended_statistics_target(&stat, relation);
        let payloads = if target_ids.len() >= 2 {
            build_extended_statistics_payloads(
                &target_ids,
                &rows,
                reltuples,
                &stat.stxkind,
                statistics_target,
            )
            .map_err(|message| ExecError::DetailedError {
                message: "could not build extended statistics".into(),
                detail: Some(message),
                hint: None,
                sqlstate: "XX000",
            })?
        } else {
            crate::backend::statistics::build::ExtendedStatisticsPayloads {
                stxdndistinct: None,
                stxddependencies: None,
                stxdmcv: None,
            }
        };
        let stxdexpr = if bound_exprs.is_empty() {
            None
        } else {
            Some(build_expression_statistics_rows(
                relation,
                &bound_exprs,
                &expression_sample_rows,
                reltuples,
                catalog,
                statistics_target,
                stxdinherit,
            )?)
        };
        out.push(PgStatisticExtDataRow {
            stxoid: stat.oid,
            stxdinherit,
            stxdndistinct: payloads.stxdndistinct,
            stxddependencies: payloads.stxddependencies,
            stxdmcv: payloads.stxdmcv,
            stxdexpr,
        });
    }
    Ok(out)
}

fn statistics_has_zero_column_target(
    stat: &crate::include::catalog::PgStatisticExtRow,
    relation: &BoundRelation,
) -> bool {
    stat.stxkeys.iter().any(|attnum| {
        attnum
            .checked_sub(1)
            .and_then(|idx| relation.desc.columns.get(idx as usize))
            .is_some_and(|column| column.attstattarget == 0)
    })
}

fn push_extended_statistics_warning(
    stat: &crate::include::catalog::PgStatisticExtRow,
    relation: &BoundRelation,
    catalog: &dyn CatalogLookup,
) {
    let stat_name = qualified_statistics_name(stat, catalog);
    let relation_name = qualified_relation_name(relation, catalog);
    crate::backend::utils::misc::notices::push_warning(format!(
        "statistics object \"{stat_name}\" could not be computed for relation \"{relation_name}\""
    ));
}

fn qualified_statistics_name(
    stat: &crate::include::catalog::PgStatisticExtRow,
    catalog: &dyn CatalogLookup,
) -> String {
    let namespace = catalog
        .namespace_row_by_oid(stat.stxnamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| stat.stxnamespace.to_string());
    format!("{namespace}.{}", stat.stxname)
}

fn qualified_relation_name(relation: &BoundRelation, catalog: &dyn CatalogLookup) -> String {
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

fn build_expression_statistics_rows(
    relation: &BoundRelation,
    expressions: &[crate::include::nodes::primnodes::Expr],
    sample_rows: &[SampledRow],
    reltuples: f64,
    catalog: &dyn CatalogLookup,
    statistics_target: i16,
    stainherit: bool,
) -> Result<Vec<PgStatisticRow>, ExecError> {
    let expression_desc = crate::backend::executor::RelationDesc {
        columns: expressions
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                let sql_type = expr_sql_type_hint(expr).unwrap_or_else(|| {
                    crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text)
                });
                let mut column = column_desc(format!("expr{}", idx + 1), sql_type, true);
                column.attstattarget = statistics_target;
                column
            })
            .collect(),
    };
    let selected = (0..expressions.len()).collect::<Vec<_>>();
    let mut rows = build_statistics_rows(
        relation.relation_oid,
        &expression_desc,
        &selected,
        sample_rows,
        reltuples,
        catalog,
        stainherit,
    )?;
    for (idx, row) in rows.iter_mut().enumerate() {
        row.staattnum = -((idx as i16) + 1);
    }
    Ok(rows)
}

fn statistics_expression_texts(raw: &Option<String>) -> Result<Vec<String>, ExecError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    serde_json::from_str::<Vec<String>>(raw).map_err(|err| ExecError::DetailedError {
        message: "could not parse stored statistics expressions".into(),
        detail: Some(err.to_string()),
        hint: None,
        sqlstate: "XX000",
    })
}

fn extended_statistics_target(
    stat: &crate::include::catalog::PgStatisticExtRow,
    relation: &BoundRelation,
) -> i16 {
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
    statistics_value_key(value).unwrap_or_else(|| "NULL".into())
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
