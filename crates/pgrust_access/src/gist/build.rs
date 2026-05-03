use std::cmp::Ordering;

use pgrust_core::{XLOG_GIST_PAGE_INIT, XLOG_GIST_PAGE_UPDATE};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_storage::page::bufpage::{PageError, page_header};

use crate::access::amapi::{IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult};
use crate::access::gist::{
    F_LEAF, GIST_INVALID_BLOCKNO, GistBufferingMode, GistOptions, GistPageError,
    gist_page_replace_items,
};
use crate::access::itemptr::ItemPointerData;
use crate::access::itup::IndexTupleData;
use crate::{
    AccessError, AccessInterruptServices, AccessResult, AccessScalarServices, AccessWalServices,
};

use super::build_buffers::{GistBuildBufferStats, GistBuildBuffers};
use super::insert::{GistTupleEntry, insert_build_entries};
use super::page::{
    GistLoggedPage, GistPageWriteMode, ensure_empty_gist, ensure_empty_gist_with_mode,
    log_gist_build_newpage_range, write_buffered_page_with_mode, write_logged_pages_with_mode,
};
use super::state::GistState;
use super::support::sortsupport;
use super::tuple::{make_downlink_tuple, make_leaf_tuple, tuple_storage_size};

pub trait GistBuildRowSource {
    fn for_each_projected(
        &mut self,
        visit: &mut dyn FnMut(ItemPointerData, Vec<Value>) -> AccessResult<()>,
    ) -> AccessResult<u64>;
}

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

pub fn gistbuild(
    ctx: &IndexBuildContext,
    source: &mut dyn GistBuildRowSource,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> Result<IndexBuildResult, AccessError> {
    if ctx.index_meta.indisunique {
        return Err(AccessError::Scalar(
            "GiST does not support unique indexes".into(),
        ));
    }
    ensure_empty_gist_with_mode(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        GistPageWriteMode::Build,
        wal,
    )?;
    let state = GistState::new(&ctx.index_desc, &ctx.index_meta, scalar)?;
    let options = ctx.index_meta.gist_options.unwrap_or_default();
    let result = match select_build_mode(&state, ctx.maintenance_work_mem_kb, options) {
        GistBuildMode::Sorted => gistbuild_sorted(
            ctx,
            &state,
            source,
            interrupts,
            scalar,
            wal,
            options.fillfactor,
        ),
        GistBuildMode::Buffering => gistbuild_buffered(
            ctx,
            &state,
            source,
            interrupts,
            scalar,
            wal,
            options.buffering_mode,
        ),
        GistBuildMode::RepeatedInsert => {
            gistbuild_repeated(ctx, &state, source, interrupts, scalar, wal)
        }
    }?;
    log_gist_build_newpage_range(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        wal,
    )?;
    Ok(result)
}

pub fn gistbuildempty(
    ctx: &IndexBuildEmptyContext,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    ensure_empty_gist(&ctx.pool, ctx.client_id, ctx.xid, ctx.index_relation, wal)
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
    state: &GistState<'_>,
    source: &mut dyn GistBuildRowSource,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> Result<IndexBuildResult, AccessError> {
    scan_projected(source, interrupts, |tid, key_values| {
        gistinsert_build_tuple(ctx, state, tid, key_values, scalar, wal)
    })
}

fn gistbuild_buffered(
    ctx: &IndexBuildContext,
    state: &GistState<'_>,
    source: &mut dyn GistBuildRowSource,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
    buffering_mode: GistBufferingMode,
) -> Result<IndexBuildResult, AccessError> {
    match buffering_mode {
        GistBufferingMode::Auto => {
            gistbuild_buffered_auto(ctx, state, source, interrupts, scalar, wal)
        }
        GistBufferingMode::On => gistbuild_buffered_on(ctx, state, source, interrupts, scalar, wal),
        GistBufferingMode::Off => gistbuild_repeated(ctx, state, source, interrupts, scalar, wal),
    }
}

fn gistbuild_buffered_on(
    ctx: &IndexBuildContext,
    state: &GistState<'_>,
    source: &mut dyn GistBuildRowSource,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> Result<IndexBuildResult, AccessError> {
    let mut stats = GistBuildBufferStats::default();
    let mut buffers: Option<GistBuildBuffers> = None;
    let mut buffering_disabled = false;
    let result = scan_projected(source, interrupts, |tid, key_values| {
        let tuple = make_build_tuple(&ctx.index_desc, tid, key_values, scalar)?;
        stats.observe(&tuple);
        if let Some(buffers) = buffers.as_mut() {
            buffers.insert(ctx, state, tuple, scalar, wal)?;
            if stats.tuples % GIST_BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0 {
                buffers.recalculate_pages_per_buffer(stats);
            }
            return Ok(());
        }
        if buffering_disabled {
            return gistinsert_build_tuple_entry(ctx, state, tuple, wal);
        }
        gistinsert_build_tuple_entry(ctx, state, tuple, wal)?;
        if stats.tuples < GIST_BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET {
            return Ok(());
        }
        if let Some(new_buffers) = GistBuildBuffers::try_new(ctx, state, stats, scalar, wal)? {
            buffers = Some(new_buffers);
        } else {
            buffering_disabled = true;
        }
        Ok(())
    })?;
    if let Some(buffers) = buffers.as_mut() {
        buffers.flush_all(ctx, state, scalar, wal)?;
    }
    Ok(result)
}

fn gistbuild_buffered_auto(
    ctx: &IndexBuildContext,
    state: &GistState<'_>,
    source: &mut dyn GistBuildRowSource,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> Result<IndexBuildResult, AccessError> {
    let mut stats = GistBuildBufferStats::default();
    let mut buffers: Option<GistBuildBuffers> = None;
    let mut buffering_disabled = false;
    let result = scan_projected(source, interrupts, |tid, key_values| {
        let tuple = make_build_tuple(&ctx.index_desc, tid, key_values, scalar)?;
        stats.observe(&tuple);
        if let Some(buffers) = buffers.as_mut() {
            buffers.insert(ctx, state, tuple, scalar, wal)?;
            if stats.tuples % GIST_BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0 {
                buffers.recalculate_pages_per_buffer(stats);
            }
            return Ok(());
        }
        gistinsert_build_tuple_entry(ctx, state, tuple, wal)?;
        if stats.tuples as u64 % GIST_BUFFERING_MODE_SWITCH_CHECK_STEP == 0
            && !buffering_disabled
            && relation_exceeds_effective_cache(ctx)?
        {
            if let Some(new_buffers) = GistBuildBuffers::try_new(ctx, state, stats, scalar, wal)? {
                buffers = Some(new_buffers);
            } else {
                buffering_disabled = true;
            }
        }
        Ok(())
    })?;
    if let Some(buffers) = buffers.as_mut() {
        buffers.flush_all(ctx, state, scalar, wal)?;
    }
    Ok(result)
}

fn relation_exceeds_effective_cache(ctx: &IndexBuildContext) -> Result<bool, AccessError> {
    Ok(
        super::page::relation_nblocks(&ctx.pool, ctx.index_relation)? as usize
            > ctx.pool.capacity(),
    )
}

fn gistinsert_build_tuple(
    ctx: &IndexBuildContext,
    state: &GistState<'_>,
    heap_tid: ItemPointerData,
    values: Vec<Value>,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    let build_tuple = make_build_tuple(&ctx.index_desc, heap_tid, values, scalar)?;
    gistinsert_build_tuple_entry(ctx, state, build_tuple, wal)
}

fn gistinsert_build_tuple_entry(
    ctx: &IndexBuildContext,
    state: &GistState<'_>,
    build_tuple: GistBuildTuple,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
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
        wal,
    )
}

fn gistbuild_sorted(
    ctx: &IndexBuildContext,
    state: &GistState<'_>,
    source: &mut dyn GistBuildRowSource,
    interrupts: &dyn AccessInterruptServices,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
    fillfactor: u16,
) -> Result<IndexBuildResult, AccessError> {
    let mut build_tuples = Vec::new();
    let result = scan_projected(source, interrupts, |tid, key_values| {
        build_tuples.push(make_build_tuple(&ctx.index_desc, tid, key_values, scalar)?);
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
    write_sorted_build_plan(ctx, &plan, wal)?;
    Ok(result)
}

fn scan_projected(
    source: &mut dyn GistBuildRowSource,
    interrupts: &dyn AccessInterruptServices,
    mut visit: impl FnMut(ItemPointerData, Vec<Value>) -> Result<(), AccessError>,
) -> Result<IndexBuildResult, AccessError> {
    let mut result = IndexBuildResult::default();
    result.heap_tuples = source.for_each_projected(&mut |tid, key_values| {
        interrupts
            .check_interrupts()
            .map_err(AccessError::Interrupted)?;
        visit(tid, key_values)?;
        result.index_tuples += 1;
        Ok(())
    })?;
    Ok(result)
}

fn make_build_tuple(
    desc: &RelationDesc,
    heap_tid: ItemPointerData,
    key_values: Vec<Value>,
    scalar: &dyn AccessScalarServices,
) -> Result<GistBuildTuple, AccessError> {
    let leaf_tuple = make_leaf_tuple(desc, &key_values, heap_tid, scalar)?;
    Ok(GistBuildTuple {
        heap_tid,
        approx_size: tuple_storage_size(desc, &key_values, scalar)?,
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
            state.scalar_services(),
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
) -> Result<SortedBuildPlan, AccessError> {
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
        return Err(AccessError::Corrupt(
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
                        tuple: make_downlink_tuple(
                            desc,
                            &page.union,
                            page.block,
                            state.scalar_services(),
                        )?,
                        values: page.union.clone(),
                    })
                })
                .collect::<Result<Vec<_>, AccessError>>()?,
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
) -> Result<Vec<PackedBuildPage>, AccessError> {
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
            return Err(AccessError::Scalar(
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
) -> Result<PackedBuildPage, AccessError> {
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
) -> Result<bool, AccessError> {
    let page = page_image_from_tuples(
        &items
            .iter()
            .map(|item| item.tuple.clone())
            .collect::<Vec<_>>(),
        flags,
        GIST_INVALID_BLOCKNO,
    )?;
    Ok(page_header(&page)
        .map_err(|err| AccessError::Scalar(format!("gist page header read failed: {err:?}")))?
        .free_space()
        >= target_free_space)
}

fn page_can_fit(items: &[BuildPageItem], flags: u16) -> Result<bool, AccessError> {
    let tuples = items
        .iter()
        .map(|item| item.tuple.clone())
        .collect::<Vec<_>>();
    let mut page = [0u8; pgrust_storage::BLCKSZ];
    match gist_page_replace_items(
        &mut page,
        &tuples,
        super::page::init_opaque(flags, GIST_INVALID_BLOCKNO, 0),
    ) {
        Ok(()) => Ok(true),
        Err(GistPageError::Page(PageError::NoSpace)) => Ok(false),
        Err(other) => Err(AccessError::Scalar(format!(
            "gist page fit check failed: {other:?}"
        ))),
    }
}

fn page_image_from_tuples(
    tuples: &[IndexTupleData],
    flags: u16,
    rightlink: u32,
) -> Result<[u8; pgrust_storage::BLCKSZ], AccessError> {
    let mut page = [0u8; pgrust_storage::BLCKSZ];
    gist_page_replace_items(
        &mut page,
        tuples,
        super::page::init_opaque(flags, rightlink, 0),
    )
    .map_err(|err| AccessError::Scalar(format!("gist build page init failed: {err:?}")))?;
    Ok(page)
}

fn write_sorted_build_plan(
    ctx: &IndexBuildContext,
    plan: &SortedBuildPlan,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
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
            wal,
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
        wal,
    )?;
    Ok(())
}

fn page_fillfactor_reserve(fillfactor: usize) -> usize {
    pgrust_storage::BLCKSZ * (100usize.saturating_sub(fillfactor.min(100))) / 100
}
