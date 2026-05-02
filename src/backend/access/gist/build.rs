use std::cmp::Ordering;

use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, materialize_heap_row_values,
};
use crate::backend::access::transam::xlog::{XLOG_GIST_PAGE_INIT, XLOG_GIST_PAGE_UPDATE};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::page::bufpage::{PageError, page_header};
use crate::include::access::amapi::{IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult};
use crate::include::access::gist::{
    F_LEAF, GIST_INVALID_BLOCKNO, GistBufferingMode, GistOptions, GistPageError,
    gist_page_replace_items,
};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;

use super::build_buffers::{GistBuildBufferStats, GistBuildBuffers};
use super::insert::{GistTupleEntry, insert_build_entries};
use super::page::{
    GistLoggedPage, GistPageWriteMode, ensure_empty_gist, ensure_empty_gist_with_mode,
    log_gist_build_newpage_range, write_buffered_page_with_mode, write_logged_pages_with_mode,
};
use super::state::GistState;
use super::support::sortsupport;
use super::tuple::{make_downlink_tuple, make_leaf_tuple, tuple_storage_size};

const GIST_BUFFERING_MIN_WORK_MEM_KB: usize = 64;
const GIST_BUFFERING_MODE_SWITCH_CHECK_STEP: u64 = 256;
const GIST_BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GistBuildMode {
    Sorted,
    Buffering,
    RepeatedInsert,
}

#[derive(Debug, Clone)]
pub(super) struct GistBuildTuple {
    pub(super) heap_tid: ItemPointerData,
    pub(super) key_values: Vec<Value>,
    pub(super) leaf_tuple: IndexTupleData,
    pub(super) approx_size: usize,
}

#[derive(Debug, Clone)]
struct BuildPageItem {
    tuple: IndexTupleData,
    values: Vec<Value>,
}

#[derive(Debug, Clone)]
struct PackedBuildPage {
    items: Vec<BuildPageItem>,
    union: Vec<Value>,
}

#[derive(Debug, Clone)]
struct PlannedBuildPage {
    block: u32,
    is_leaf: bool,
    tuples: Vec<IndexTupleData>,
    union: Vec<Value>,
    rightlink: u32,
}

#[derive(Debug, Clone)]
struct SortedBuildPlan {
    pages: Vec<PlannedBuildPage>,
    root: PlannedBuildPage,
}

pub(crate) fn gistbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    if ctx.index_meta.indisunique {
        return Err(CatalogError::Io(
            "GiST does not support unique indexes".into(),
        ));
    }
    ensure_empty_gist_with_mode(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        GistPageWriteMode::Build,
    )?;
    let state = GistState::new(&ctx.index_desc, &ctx.index_meta)?;
    let options = ctx.index_meta.gist_options.unwrap_or_default();
    let result = match select_build_mode(&state, ctx.maintenance_work_mem_kb, options) {
        GistBuildMode::Sorted => gistbuild_sorted(ctx, &state, options.fillfactor),
        GistBuildMode::Buffering => gistbuild_buffered(ctx, &state, options.buffering_mode),
        GistBuildMode::RepeatedInsert => gistbuild_repeated(ctx, &state),
    }?;
    log_gist_build_newpage_range(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
    )?;
    Ok(result)
}

pub(crate) fn gistbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    ensure_empty_gist(&ctx.pool, ctx.client_id, ctx.xid, ctx.index_relation)
}

fn select_build_mode(
    state: &GistState,
    maintenance_work_mem_kb: usize,
    options: GistOptions,
) -> GistBuildMode {
    if options.buffering_mode == GistBufferingMode::Off {
        return GistBuildMode::RepeatedInsert;
    }
    if options.buffering_mode != GistBufferingMode::On && has_all_sortsupport(state) {
        return GistBuildMode::Sorted;
    }
    if options.buffering_mode == GistBufferingMode::On
        || maintenance_work_mem_kb >= GIST_BUFFERING_MIN_WORK_MEM_KB
    {
        return GistBuildMode::Buffering;
    }
    GistBuildMode::RepeatedInsert
}

fn has_all_sortsupport(state: &GistState) -> bool {
    !state.columns.is_empty()
        && state
            .columns
            .iter()
            .all(|column| column.sortsupport_proc.and_then(sortsupport).is_some())
}

fn gistbuild_repeated(
    ctx: &IndexBuildContext,
    state: &GistState,
) -> Result<IndexBuildResult, CatalogError> {
    scan_visible_heap(ctx, |tid, key_values| {
        gistinsert_build_tuple(ctx, state, tid, key_values)
    })
}

fn gistbuild_buffered(
    ctx: &IndexBuildContext,
    state: &GistState,
    buffering_mode: GistBufferingMode,
) -> Result<IndexBuildResult, CatalogError> {
    match buffering_mode {
        GistBufferingMode::Auto => gistbuild_buffered_auto(ctx, state),
        GistBufferingMode::On => gistbuild_buffered_on(ctx, state),
        GistBufferingMode::Off => gistbuild_repeated(ctx, state),
    }
}

fn gistbuild_buffered_on(
    ctx: &IndexBuildContext,
    state: &GistState,
) -> Result<IndexBuildResult, CatalogError> {
    let mut stats = GistBuildBufferStats::default();
    let mut buffers: Option<GistBuildBuffers> = None;
    let mut buffering_disabled = false;
    let result = scan_visible_heap(ctx, |tid, key_values| {
        let tuple = make_build_tuple(&ctx.index_desc, tid, key_values)?;
        stats.observe(&tuple);
        if let Some(buffers) = buffers.as_mut() {
            buffers.insert(ctx, state, tuple)?;
            if stats.tuples % GIST_BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0 {
                buffers.recalculate_pages_per_buffer(stats);
            }
            return Ok(());
        }
        if buffering_disabled {
            return gistinsert_build_tuple_entry(ctx, state, tuple);
        }
        gistinsert_build_tuple_entry(ctx, state, tuple)?;
        if stats.tuples < GIST_BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET {
            return Ok(());
        }
        if let Some(new_buffers) = GistBuildBuffers::try_new(ctx, state, stats)? {
            buffers = Some(new_buffers);
        } else {
            buffering_disabled = true;
        }
        Ok(())
    })?;
    if let Some(buffers) = buffers.as_mut() {
        buffers.flush_all(ctx, state)?;
    }
    Ok(result)
}

fn gistbuild_buffered_auto(
    ctx: &IndexBuildContext,
    state: &GistState,
) -> Result<IndexBuildResult, CatalogError> {
    let mut stats = GistBuildBufferStats::default();
    let mut buffers: Option<GistBuildBuffers> = None;
    let mut buffering_disabled = false;
    let result = scan_visible_heap(ctx, |tid, key_values| {
        let tuple = make_build_tuple(&ctx.index_desc, tid, key_values)?;
        stats.observe(&tuple);
        if let Some(buffers) = buffers.as_mut() {
            buffers.insert(ctx, state, tuple)?;
            if stats.tuples % GIST_BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0 {
                buffers.recalculate_pages_per_buffer(stats);
            }
            return Ok(());
        }
        gistinsert_build_tuple_entry(ctx, state, tuple)?;
        if stats.tuples as u64 % GIST_BUFFERING_MODE_SWITCH_CHECK_STEP == 0
            && !buffering_disabled
            && relation_exceeds_effective_cache(ctx)?
        {
            if let Some(new_buffers) = GistBuildBuffers::try_new(ctx, state, stats)? {
                buffers = Some(new_buffers);
            } else {
                buffering_disabled = true;
            }
        }
        Ok(())
    })?;
    if let Some(buffers) = buffers.as_mut() {
        buffers.flush_all(ctx, state)?;
    }
    Ok(result)
}

fn relation_exceeds_effective_cache(ctx: &IndexBuildContext) -> Result<bool, CatalogError> {
    Ok(
        super::page::relation_nblocks(&ctx.pool, ctx.index_relation)? as usize
            > ctx.pool.capacity(),
    )
}

fn gistinsert_build_tuple(
    ctx: &IndexBuildContext,
    state: &GistState,
    heap_tid: ItemPointerData,
    values: Vec<Value>,
) -> Result<(), CatalogError> {
    let build_tuple = make_build_tuple(&ctx.index_desc, heap_tid, values)?;
    gistinsert_build_tuple_entry(ctx, state, build_tuple)
}

fn gistinsert_build_tuple_entry(
    ctx: &IndexBuildContext,
    state: &GistState,
    build_tuple: GistBuildTuple,
) -> Result<(), CatalogError> {
    insert_build_entries(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        &ctx.index_desc,
        state,
        vec![GistTupleEntry {
            tuple: build_tuple.leaf_tuple,
            values: build_tuple.key_values,
        }],
    )
}

fn gistbuild_sorted(
    ctx: &IndexBuildContext,
    state: &GistState,
    fillfactor: u16,
) -> Result<IndexBuildResult, CatalogError> {
    let mut build_tuples = Vec::new();
    let result = scan_visible_heap(ctx, |tid, key_values| {
        build_tuples.push(make_build_tuple(&ctx.index_desc, tid, key_values)?);
        Ok(())
    })?;
    if build_tuples.is_empty() {
        return Ok(result);
    }
    sort_build_tuples(state, &mut build_tuples);
    let plan = plan_sorted_build(
        &ctx.index_desc,
        state,
        &build_tuples,
        page_fillfactor_reserve(usize::from(fillfactor)),
    )?;
    write_sorted_build_plan(ctx, &plan)?;
    Ok(result)
}

fn scan_visible_heap(
    ctx: &IndexBuildContext,
    mut visit: impl FnMut(ItemPointerData, Vec<Value>) -> Result<(), CatalogError>,
) -> Result<IndexBuildResult, CatalogError> {
    let mut scan = heap_scan_begin_visible(
        &ctx.pool,
        ctx.client_id,
        ctx.heap_relation,
        ctx.snapshot.clone(),
    )
    .map_err(|err| CatalogError::Io(format!("heap scan begin failed: {err:?}")))?;
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut result = IndexBuildResult::default();
    loop {
        crate::backend::utils::misc::interrupts::check_for_interrupts(ctx.interrupts.as_ref())
            .map_err(CatalogError::Interrupted)?;
        let next = {
            let txns = ctx.txns.read();
            heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)
        };
        let Some((tid, tuple)) =
            next.map_err(|err| CatalogError::Io(format!("heap scan failed: {err:?}")))?
        else {
            break;
        };
        let datums = tuple
            .deform(&attr_descs)
            .map_err(|err| CatalogError::Io(format!("heap deform failed: {err:?}")))?;
        let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums)?;
        if let Some(key_values) = key_projector.project(ctx, &row_values, tid)? {
            visit(tid, key_values)?;
            result.index_tuples += 1;
        }
        result.heap_tuples += 1;
    }
    Ok(result)
}

fn make_build_tuple(
    desc: &RelationDesc,
    heap_tid: ItemPointerData,
    key_values: Vec<Value>,
) -> Result<GistBuildTuple, CatalogError> {
    let leaf_tuple = make_leaf_tuple(desc, &key_values, heap_tid)?;
    Ok(GistBuildTuple {
        heap_tid,
        approx_size: tuple_storage_size(desc, &key_values)?,
        key_values,
        leaf_tuple,
    })
}

fn sort_build_tuples(state: &GistState, tuples: &mut [GistBuildTuple]) {
    tuples.sort_by(|left, right| compare_build_tuples(state, left, right, true));
}

fn compare_build_tuples(
    state: &GistState,
    left: &GistBuildTuple,
    right: &GistBuildTuple,
    require_all_columns: bool,
) -> Ordering {
    let ord = compare_build_key_values(
        state,
        &left.key_values,
        &right.key_values,
        require_all_columns,
    );
    if ord != Ordering::Equal {
        return ord;
    }
    left.heap_tid
        .block_number
        .cmp(&right.heap_tid.block_number)
        .then_with(|| {
            left.heap_tid
                .offset_number
                .cmp(&right.heap_tid.offset_number)
        })
}

fn compare_build_key_values(
    state: &GistState,
    left: &[Value],
    right: &[Value],
    require_all_columns: bool,
) -> Ordering {
    for (column_index, column_state) in state.columns.iter().enumerate() {
        let Some(compare) = column_state.sortsupport_proc.and_then(sortsupport) else {
            if require_all_columns {
                return Ordering::Equal;
            }
            break;
        };
        let ord = compare(
            left.get(column_index).unwrap_or(&Value::Null),
            right.get(column_index).unwrap_or(&Value::Null),
        );
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn plan_sorted_build(
    desc: &RelationDesc,
    state: &GistState,
    tuples: &[GistBuildTuple],
    target_free_space: usize,
) -> Result<SortedBuildPlan, CatalogError> {
    let leaf_groups = pack_page_items(
        state,
        tuples
            .iter()
            .map(|tuple| BuildPageItem {
                tuple: tuple.leaf_tuple.clone(),
                values: tuple.key_values.clone(),
            })
            .collect(),
        F_LEAF,
        target_free_space,
    )?;
    if leaf_groups.is_empty() {
        return Err(CatalogError::Corrupt(
            "sorted GiST build produced no leaf pages",
        ));
    }
    if leaf_groups.len() == 1 {
        let root = leaf_groups
            .into_iter()
            .next()
            .expect("single leaf group must exist");
        return Ok(SortedBuildPlan {
            pages: Vec::new(),
            root: PlannedBuildPage {
                block: 0,
                is_leaf: true,
                tuples: root.items.into_iter().map(|item| item.tuple).collect(),
                union: root.union,
                rightlink: GIST_INVALID_BLOCKNO,
            },
        });
    }

    let mut next_block = 1u32;
    let mut pages = Vec::new();
    let mut current_level = assign_level_blocks(leaf_groups, true, &mut next_block, &mut pages);

    loop {
        let parent_groups = pack_page_items(
            state,
            current_level
                .iter()
                .map(|page| {
                    Ok(BuildPageItem {
                        tuple: make_downlink_tuple(desc, &page.union, page.block)?,
                        values: page.union.clone(),
                    })
                })
                .collect::<Result<Vec<_>, CatalogError>>()?,
            0,
            target_free_space,
        )?;

        if parent_groups.len() == 1 {
            let root = parent_groups
                .into_iter()
                .next()
                .expect("single root group must exist");
            return Ok(SortedBuildPlan {
                pages,
                root: PlannedBuildPage {
                    block: 0,
                    is_leaf: false,
                    tuples: root.items.into_iter().map(|item| item.tuple).collect(),
                    union: root.union,
                    rightlink: GIST_INVALID_BLOCKNO,
                },
            });
        }

        current_level = assign_level_blocks(parent_groups, false, &mut next_block, &mut pages);
    }
}

fn assign_level_blocks(
    groups: Vec<PackedBuildPage>,
    is_leaf: bool,
    next_block: &mut u32,
    out_pages: &mut Vec<PlannedBuildPage>,
) -> Vec<PlannedBuildPage> {
    let blocks = (0..groups.len())
        .map(|_| {
            let block = *next_block;
            *next_block = next_block.saturating_add(1);
            block
        })
        .collect::<Vec<_>>();
    let mut level_pages = Vec::with_capacity(groups.len());
    for (index, group) in groups.into_iter().enumerate() {
        let page = PlannedBuildPage {
            block: blocks[index],
            is_leaf,
            tuples: group.items.into_iter().map(|item| item.tuple).collect(),
            union: group.union,
            rightlink: blocks
                .get(index + 1)
                .copied()
                .unwrap_or(GIST_INVALID_BLOCKNO),
        };
        out_pages.push(page.clone());
        level_pages.push(page);
    }
    level_pages
}

fn pack_page_items(
    state: &GistState,
    items: Vec<BuildPageItem>,
    flags: u16,
    target_free_space: usize,
) -> Result<Vec<PackedBuildPage>, CatalogError> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let mut pages = Vec::new();
    let mut current = Vec::new();

    for item in items {
        if !current.is_empty() {
            let mut candidate = current.clone();
            candidate.push(item.clone());
            if !page_has_target_free_space(&candidate, flags, target_free_space)? {
                pages.push(materialize_packed_page(
                    state,
                    std::mem::take(&mut current),
                )?);
            }
        }

        current.push(item);
        if !page_can_fit(&current, flags)? {
            return Err(CatalogError::Io(
                "GiST build tuple too large to fit on a page".into(),
            ));
        }
    }

    if !current.is_empty() {
        pages.push(materialize_packed_page(state, current)?);
    }
    Ok(pages)
}

fn materialize_packed_page(
    state: &GistState,
    items: Vec<BuildPageItem>,
) -> Result<PackedBuildPage, CatalogError> {
    Ok(PackedBuildPage {
        union: state.union_all(
            &items
                .iter()
                .map(|item| item.values.clone())
                .collect::<Vec<_>>(),
        )?,
        items,
    })
}

fn page_has_target_free_space(
    items: &[BuildPageItem],
    flags: u16,
    target_free_space: usize,
) -> Result<bool, CatalogError> {
    let page = page_image_from_tuples(
        &items
            .iter()
            .map(|item| item.tuple.clone())
            .collect::<Vec<_>>(),
        flags,
        GIST_INVALID_BLOCKNO,
    )?;
    Ok(page_header(&page)
        .map_err(|err| CatalogError::Io(format!("gist page header read failed: {err:?}")))?
        .free_space()
        >= target_free_space)
}

fn page_can_fit(items: &[BuildPageItem], flags: u16) -> Result<bool, CatalogError> {
    let tuples = items
        .iter()
        .map(|item| item.tuple.clone())
        .collect::<Vec<_>>();
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    match gist_page_replace_items(
        &mut page,
        &tuples,
        super::page::init_opaque(flags, GIST_INVALID_BLOCKNO, 0),
    ) {
        Ok(()) => Ok(true),
        Err(GistPageError::Page(PageError::NoSpace)) => Ok(false),
        Err(other) => Err(CatalogError::Io(format!(
            "gist page fit check failed: {other:?}"
        ))),
    }
}

fn page_image_from_tuples(
    tuples: &[IndexTupleData],
    flags: u16,
    rightlink: u32,
) -> Result<[u8; crate::backend::storage::smgr::BLCKSZ], CatalogError> {
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    gist_page_replace_items(
        &mut page,
        tuples,
        super::page::init_opaque(flags, rightlink, 0),
    )
    .map_err(|err| CatalogError::Io(format!("gist build page init failed: {err:?}")))?;
    Ok(page)
}

fn write_sorted_build_plan(
    ctx: &IndexBuildContext,
    plan: &SortedBuildPlan,
) -> Result<(), CatalogError> {
    let mut pages = plan.pages.clone();
    pages.sort_by_key(|page| page.block);
    for page in &pages {
        let image = page_image_from_tuples(
            &page.tuples,
            if page.is_leaf { F_LEAF } else { 0 },
            page.rightlink,
        )?;
        write_logged_pages_with_mode(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            XLOG_GIST_PAGE_INIT,
            &[GistLoggedPage {
                block: page.block,
                page: &image,
                will_init: true,
            }],
            GistPageWriteMode::Build,
        )?;
    }

    let root_image = page_image_from_tuples(
        &plan.root.tuples,
        if plan.root.is_leaf { F_LEAF } else { 0 },
        GIST_INVALID_BLOCKNO,
    )?;
    write_buffered_page_with_mode(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        0,
        &root_image,
        XLOG_GIST_PAGE_UPDATE,
        GistPageWriteMode::Build,
    )?;
    Ok(())
}

fn page_fillfactor_reserve(fillfactor: usize) -> usize {
    crate::backend::storage::smgr::BLCKSZ * (100usize.saturating_sub(fillfactor.min(100))) / 100
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{
        GIST_BUFFERING_MIN_WORK_MEM_KB, GistBuildMode, GistBuildTuple, has_all_sortsupport,
        make_build_tuple, plan_sorted_build, select_build_mode,
    };
    use crate::backend::access::gist::state::{GistColumnState, GistState};
    use crate::backend::access::gist::support::sortsupport;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::expr_range::parse_range_text;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::access::gist::{
        F_LEAF, GIST_INVALID_BLOCKNO, GistBufferingMode, GistOptions,
    };
    use crate::include::access::itemptr::ItemPointerData;
    use crate::include::catalog::{
        GIST_BOX_CONSISTENT_PROC_OID, GIST_BOX_DISTANCE_PROC_OID, GIST_BOX_PENALTY_PROC_OID,
        GIST_BOX_PICKSPLIT_PROC_OID, GIST_BOX_SAME_PROC_OID, GIST_BOX_UNION_PROC_OID,
        GIST_POINT_CONSISTENT_PROC_OID, GIST_POINT_PENALTY_PROC_OID, GIST_POINT_PICKSPLIT_PROC_OID,
        GIST_POINT_SAME_PROC_OID, GIST_POINT_SORTSUPPORT_PROC_OID, GIST_POINT_UNION_PROC_OID,
        RANGE_GIST_CONSISTENT_PROC_OID, RANGE_GIST_PENALTY_PROC_OID, RANGE_GIST_PICKSPLIT_PROC_OID,
        RANGE_GIST_SAME_PROC_OID, RANGE_GIST_UNION_PROC_OID, RANGE_SORTSUPPORT_PROC_OID,
    };
    use crate::include::nodes::datum::Value;
    use crate::include::nodes::primnodes::RelationDesc;

    fn box_state() -> GistColumnState {
        GistColumnState {
            consistent_proc: GIST_BOX_CONSISTENT_PROC_OID,
            union_proc: GIST_BOX_UNION_PROC_OID,
            penalty_proc: GIST_BOX_PENALTY_PROC_OID,
            picksplit_proc: GIST_BOX_PICKSPLIT_PROC_OID,
            same_proc: GIST_BOX_SAME_PROC_OID,
            distance_proc: Some(GIST_BOX_DISTANCE_PROC_OID),
            sortsupport_proc: None,
            translate_cmptype_proc: None,
        }
    }

    fn range_state() -> GistColumnState {
        GistColumnState {
            consistent_proc: RANGE_GIST_CONSISTENT_PROC_OID,
            union_proc: RANGE_GIST_UNION_PROC_OID,
            penalty_proc: RANGE_GIST_PENALTY_PROC_OID,
            picksplit_proc: RANGE_GIST_PICKSPLIT_PROC_OID,
            same_proc: RANGE_GIST_SAME_PROC_OID,
            distance_proc: None,
            sortsupport_proc: Some(RANGE_SORTSUPPORT_PROC_OID),
            translate_cmptype_proc: None,
        }
    }

    fn point_state() -> GistColumnState {
        GistColumnState {
            consistent_proc: GIST_POINT_CONSISTENT_PROC_OID,
            union_proc: GIST_POINT_UNION_PROC_OID,
            penalty_proc: GIST_POINT_PENALTY_PROC_OID,
            picksplit_proc: GIST_POINT_PICKSPLIT_PROC_OID,
            same_proc: GIST_POINT_SAME_PROC_OID,
            distance_proc: None,
            sortsupport_proc: Some(GIST_POINT_SORTSUPPORT_PROC_OID),
            translate_cmptype_proc: None,
        }
    }

    fn range_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![column_desc(
                "span",
                SqlType::new(SqlTypeKind::Int4Range),
                true,
            )],
        }
    }

    fn parse_int4range(text: &str) -> Value {
        parse_range_text(text, SqlType::new(SqlTypeKind::Int4Range)).unwrap()
    }

    fn make_range_build_tuple(value: &str, block: u32, offset: u16) -> GistBuildTuple {
        make_build_tuple(
            &range_desc(),
            ItemPointerData {
                block_number: block,
                offset_number: offset,
            },
            vec![parse_int4range(value)],
        )
        .unwrap()
    }

    #[test]
    fn select_build_mode_prefers_sorted_when_all_columns_support_it() {
        let state = GistState {
            columns: vec![range_state(), range_state()],
        };

        assert!(has_all_sortsupport(&state));
        assert_eq!(
            select_build_mode(
                &state,
                GIST_BUFFERING_MIN_WORK_MEM_KB,
                GistOptions::default()
            ),
            GistBuildMode::Sorted
        );
    }

    #[test]
    fn select_build_mode_respects_forced_buffering_over_sortsupport() {
        let state = GistState {
            columns: vec![range_state(), range_state()],
        };

        assert_eq!(
            select_build_mode(
                &state,
                GIST_BUFFERING_MIN_WORK_MEM_KB,
                GistOptions {
                    fillfactor: 90,
                    buffering_mode: GistBufferingMode::On,
                },
            ),
            GistBuildMode::Buffering
        );
    }

    #[test]
    fn select_build_mode_uses_buffering_without_full_sortsupport() {
        let state = GistState {
            columns: vec![box_state()],
        };

        assert!(!has_all_sortsupport(&state));
        assert_eq!(
            select_build_mode(
                &state,
                GIST_BUFFERING_MIN_WORK_MEM_KB,
                GistOptions::default()
            ),
            GistBuildMode::Buffering
        );
        assert_eq!(
            select_build_mode(&state, 0, GistOptions::default()),
            GistBuildMode::RepeatedInsert
        );
        assert_eq!(
            select_build_mode(
                &state,
                GIST_BUFFERING_MIN_WORK_MEM_KB,
                GistOptions {
                    fillfactor: 90,
                    buffering_mode: GistBufferingMode::Off,
                },
            ),
            GistBuildMode::RepeatedInsert
        );
    }

    #[test]
    fn range_state_exposes_real_sortsupport_comparator() {
        let comparator = sortsupport(range_state().sortsupport_proc.unwrap())
            .expect("range proc 11 should produce a comparator");

        assert_eq!(
            comparator(&parse_int4range("[1,5)"), &parse_int4range("[5,9)")),
            Ordering::Less
        );
    }

    #[test]
    fn point_state_exposes_real_sortsupport_comparator() {
        let comparator = sortsupport(point_state().sortsupport_proc.unwrap())
            .expect("point proc 11 should produce a comparator");

        assert_eq!(
            comparator(
                &Value::Point(crate::include::nodes::datum::GeoPoint { x: -1.0, y: -1.0 }),
                &Value::Point(crate::include::nodes::datum::GeoPoint { x: 1.0, y: 1.0 }),
            ),
            Ordering::Less
        );
    }

    #[test]
    fn sorted_build_plan_creates_internal_root_for_many_ranges() {
        let state = GistState {
            columns: vec![range_state()],
        };
        let tuples = (0..4096u32)
            .map(|i| make_range_build_tuple(&format!("[{}, {})", i * 2, i * 2 + 2), i + 1, 1))
            .collect::<Vec<_>>();

        let plan = plan_sorted_build(
            &range_desc(),
            &state,
            &tuples,
            super::page_fillfactor_reserve(90),
        )
        .unwrap();

        assert!(!plan.root.is_leaf);
        assert!(plan.pages.iter().any(|page| page.is_leaf));
        assert_eq!(plan.root.block, 0);
        assert_eq!(plan.root.rightlink, GIST_INVALID_BLOCKNO);
    }

    #[test]
    fn sorted_build_plan_keeps_single_leaf_root_when_few_tuples() {
        let state = GistState {
            columns: vec![range_state()],
        };
        let tuples = vec![
            make_range_build_tuple("[1,5)", 1, 1),
            make_range_build_tuple("[5,9)", 1, 2),
        ];

        let plan = plan_sorted_build(
            &range_desc(),
            &state,
            &tuples,
            super::page_fillfactor_reserve(90),
        )
        .unwrap();

        assert!(plan.pages.is_empty());
        assert!(plan.root.is_leaf);
        assert_eq!(plan.root.block, 0);
        assert_eq!(plan.root.rightlink, GIST_INVALID_BLOCKNO);
        let page =
            super::page_image_from_tuples(&plan.root.tuples, F_LEAF, GIST_INVALID_BLOCKNO).unwrap();
        let opaque = crate::include::access::gist::gist_page_get_opaque(&page).unwrap();
        assert!(opaque.is_leaf());
    }

    #[test]
    fn make_build_tuple_preserves_heap_tid_and_tuple_size() {
        let tuple = make_range_build_tuple("[1,5)", 42, 7);

        assert_eq!(tuple.heap_tid.block_number, 42);
        assert_eq!(tuple.heap_tid.offset_number, 7);
        assert!(tuple.approx_size > 0);
    }
}
