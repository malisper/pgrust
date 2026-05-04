use std::collections::{BTreeSet, HashMap};

use super::tablecmds::{
    collect_matching_rows_heap, index_key_values_for_row, row_matches_index_predicate,
};
use crate::RelFileLocator;
use crate::backend::access::heap::heapam_visibility::SnapshotVisibility;
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
use crate::include::nodes::execnodes::ToastFetchContext;
use crate::include::nodes::parsenodes::MaintenanceTarget;
use crate::include::nodes::primnodes::expr_sql_type_hint;
use pgrust_commands::analyze::{
    AnalyzeRng, BlockSampler, DEFAULT_STATISTICS_TARGET, ReservoirSampler, SampledRow,
    target_sample_blocks, target_sample_rows,
};

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
    pgrust_commands::analyze::selected_columns(relation, target).map_err(analyze_error_to_exec)
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
                    .tuple_bytes_visible_with_hints(&*txns, tuple_bytes)
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
            out.push(AnalyzeRelationStats {
                relation_oid: index.relation_oid,
                relpages: nblocks as i32,
                reltuples: sample_rows.len() as f64,
                clear_relhassubclass: false,
                statistics: Vec::new(),
                statistics_ext_data: Vec::new(),
            });
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
                        .tuple_bytes_visible_with_hints(&*txns, tuple_bytes)
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
    pgrust_commands::analyze::statistics_has_zero_column_target(stat, relation)
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
    pgrust_commands::analyze::qualified_statistics_name(stat, catalog)
}

fn qualified_relation_name(relation: &BoundRelation, catalog: &dyn CatalogLookup) -> String {
    pgrust_commands::analyze::qualified_relation_name(relation, catalog)
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
    pgrust_commands::analyze::statistics_expression_texts(raw).map_err(analyze_error_to_exec)
}

fn extended_statistics_target(
    stat: &crate::include::catalog::PgStatisticExtRow,
    relation: &BoundRelation,
) -> i16 {
    pgrust_commands::analyze::extended_statistics_target(stat, relation)
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
    Ok(pgrust_commands::analyze::build_statistics_rows(
        relation_oid,
        relation_desc,
        selected_columns,
        sample_rows,
        reltuples,
        catalog,
        stainherit,
        statistics_value_key,
    ))
}

fn value_stats_text(value: &Value) -> String {
    statistics_value_key(value).unwrap_or_else(|| "NULL".into())
}

fn analyze_error_to_exec(err: pgrust_commands::analyze::AnalyzeError) -> ExecError {
    match err {
        pgrust_commands::analyze::AnalyzeError::Parse(err) => ExecError::Parse(err),
        pgrust_commands::analyze::AnalyzeError::Detailed {
            message,
            detail,
            hint,
            sqlstate,
        } => ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
    }
}
