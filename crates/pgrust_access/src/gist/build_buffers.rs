use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions, remove_file};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::access::amapi::IndexBuildContext;
use crate::access::gist::{GIST_ROOT_BLKNO, gist_downlink_block, gist_page_get_opaque};
use crate::access::itup::IndexTupleData;
use crate::{AccessError, AccessScalarServices, AccessWalServices};
use pgrust_nodes::datum::Value;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_storage::{BLCKSZ, BufferPool, ClientId, RelFileLocator, SmgrStorageBackend};

use super::build::GistBuildTuple;
use super::insert::{
    ChildSplit, GistTupleEntry, InsertOutcome, choose_child, insert_entries_into_block,
    load_page_entries, write_or_split_page,
};
use super::page::{GistPageWriteMode, clear_follow_right_with_mode, read_buffered_page};
use super::state::GistState;
use super::tuple::{decode_tuple_values, make_downlink_tuple};

const GIST_BUILD_BUFFERED_MIN_WORK_MEM_BYTES: usize = 64 * 1024;
const GIST_BUILD_TEMP_PAGE_HEADER_SIZE: usize = 10;
const GIST_BUILD_TEMP_PAGE_NONE: u64 = u64::MAX;
const GIST_BUILD_PAGE_USABLE_BYTES: usize = BLCKSZ - 256;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct GistBuildBufferStats {
    pub(super) tuples: usize,
    pub(super) total_tuple_bytes: usize,
    pub(super) min_tuple_bytes: usize,
}

impl GistBuildBufferStats {
    pub(super) fn observe(&mut self, tuple: &GistBuildTuple) {
        let size = tuple.approx_size.max(1);
        self.tuples = self.tuples.saturating_add(1);
        self.total_tuple_bytes = self.total_tuple_bytes.saturating_add(size);
        self.min_tuple_bytes = if self.min_tuple_bytes == 0 {
            size
        } else {
            self.min_tuple_bytes.min(size)
        };
    }
}

#[derive(Debug)]
pub(super) struct GistBuildBuffers {
    temp_file: TempGistBuildFile,
    node_buffers: HashMap<u32, GistNodeBuffer>,
    emptying_queue: VecDeque<u32>,
    queued_blocks: HashSet<u32>,
    level_buffers: HashMap<u16, VecDeque<u32>>,
    loaded_buffers: HashSet<u32>,
    parent_map: HashMap<u32, u32>,
    pub(super) root_level: u16,
    pub(super) level_step: u16,
    pub(super) pages_per_buffer: usize,
}

#[derive(Debug)]
struct GistNodeBuffer {
    level: u16,
    tail: Vec<IndexTupleData>,
    tail_bytes: usize,
    head_temp_block: Option<u64>,
    spilled_pages: usize,
    tuple_count: usize,
}

#[derive(Debug)]
struct TempGistBuildFile {
    path: PathBuf,
    file: File,
    next_block: u64,
    free_blocks: Vec<u64>,
}

impl GistBuildBuffers {
    pub(super) fn try_new(
        ctx: &IndexBuildContext,
        state: &GistState,
        stats: GistBuildBufferStats,
        _scalar: &dyn AccessScalarServices,
        _wal: &dyn AccessWalServices,
    ) -> Result<Option<Self>, AccessError> {
        let Some((level_step, pages_per_buffer)) = calculate_build_buffer_parameters(
            ctx.maintenance_work_mem_kb,
            ctx.pool.capacity(),
            stats,
        ) else {
            return Ok(None);
        };
        let mut buffers = Self {
            temp_file: TempGistBuildFile::new()?,
            node_buffers: HashMap::new(),
            emptying_queue: VecDeque::new(),
            queued_blocks: HashSet::new(),
            level_buffers: HashMap::new(),
            loaded_buffers: HashSet::new(),
            parent_map: HashMap::new(),
            root_level: get_max_level(
                &ctx.pool,
                ctx.client_id,
                ctx.index_relation,
                &ctx.index_desc,
                state.scalar_services(),
            )?,
            level_step,
            pages_per_buffer,
        };
        buffers.rebuild_parent_map(ctx, state)?;
        Ok(Some(buffers))
    }

    pub(super) fn insert(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        tuple: GistBuildTuple,
        _scalar: &dyn AccessScalarServices,
        wal: &dyn AccessWalServices,
    ) -> Result<(), AccessError> {
        let entry = GistTupleEntry {
            tuple: tuple.leaf_tuple,
            values: tuple.key_values,
        };
        self.process_entry(ctx, state, entry, GIST_ROOT_BLKNO, self.root_level, wal)?;
        self.process_emptying_queue(ctx, state, wal)
    }

    pub(super) fn flush_all(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        _scalar: &dyn AccessScalarServices,
        wal: &dyn AccessWalServices,
    ) -> Result<(), AccessError> {
        while self.any_nonempty_buffer() {
            let mut made_progress = false;
            for level in (1..=self.root_level).rev() {
                while let Some(block) = self.next_buffer_on_level(level) {
                    if !self.node_is_empty(block) {
                        made_progress = true;
                        self.queue_node(block);
                        self.process_emptying_queue(ctx, state, wal)?;
                        if !self.node_is_empty(block) {
                            self.level_buffers
                                .entry(level)
                                .or_default()
                                .push_back(block);
                        }
                    }
                }
            }
            if !made_progress {
                return Err(AccessError::Corrupt(
                    "GiST buffering could not empty remaining node buffers",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn recalculate_pages_per_buffer(&mut self, stats: GistBuildBufferStats) {
        if let Some(pages_per_buffer) = calculate_pages_per_buffer(stats, self.level_step) {
            self.pages_per_buffer = pages_per_buffer;
        }
    }

    fn process_entry(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        entry: GistTupleEntry,
        start_block: u32,
        start_level: u16,
        wal: &dyn AccessWalServices,
    ) -> Result<bool, AccessError> {
        let mut block = start_block;
        let mut level = start_level;
        loop {
            if self.level_has_buffers(level) && level != start_level {
                return self.push_to_node_buffer(block, level, entry);
            }
            let page = read_buffered_page(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
            let opaque = gist_page_get_opaque(&page)
                .map_err(|err| AccessError::Scalar(format!("gist page parse failed: {err:?}")))?;
            if opaque.is_leaf() {
                self.insert_into_known_block(ctx, state, block, 0, entry, wal)?;
                return Ok(false);
            }
            if level == 0 {
                return Err(AccessError::Corrupt(
                    "GiST buffering reached internal page at leaf level",
                ));
            }

            let items = load_page_entries(&ctx.index_desc, &page, state.scalar_services())?;
            let child_index = choose_child(&ctx.index_desc, state, &items, &entry.values)?;
            let child_block = gist_downlink_block(&items[child_index].tuple).ok_or(
                AccessError::Corrupt("gist internal tuple missing child block"),
            )?;
            self.parent_map.insert(child_block, block);
            block = child_block;
            level -= 1;
        }
    }

    fn insert_into_known_block(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        block: u32,
        level: u16,
        entry: GistTupleEntry,
        wal: &dyn AccessWalServices,
    ) -> Result<(), AccessError> {
        let outcome = insert_entries_into_block(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            &ctx.index_desc,
            state,
            block,
            vec![entry],
            None,
            block == GIST_ROOT_BLKNO,
            GistPageWriteMode::BuildNoExtend,
            wal,
        )?;
        self.finish_insert_outcome(ctx, state, block, level, outcome, wal)
    }

    fn finish_insert_outcome(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        block: u32,
        level: u16,
        mut outcome: InsertOutcome,
        wal: &dyn AccessWalServices,
    ) -> Result<(), AccessError> {
        self.apply_split_side_effects(ctx, state, block, level, &mut outcome)?;
        if block == GIST_ROOT_BLKNO {
            return Ok(());
        }
        self.propagate_to_parent(ctx, state, block, level, outcome, wal)
    }

    fn propagate_to_parent(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        child_block: u32,
        child_level: u16,
        child_outcome: InsertOutcome,
        wal: &dyn AccessWalServices,
    ) -> Result<(), AccessError> {
        let parent_block = self.parent_block(ctx, state, child_block)?;
        let parent_page =
            read_buffered_page(&ctx.pool, ctx.client_id, ctx.index_relation, parent_block)?;
        let parent_opaque = gist_page_get_opaque(&parent_page)
            .map_err(|err| AccessError::Scalar(format!("gist page parse failed: {err:?}")))?;
        let mut items = load_page_entries(&ctx.index_desc, &parent_page, state.scalar_services())?;
        let child_index = items
            .iter()
            .position(|item| gist_downlink_block(&item.tuple) == Some(child_block))
            .ok_or(AccessError::Corrupt(
                "GiST parent map pointed to page without child downlink",
            ))?;

        if child_outcome.split.is_none()
            && state.same_values(&items[child_index].values, &child_outcome.union)?
        {
            return Ok(());
        }

        items[child_index] = GistTupleEntry {
            tuple: make_downlink_tuple(
                &ctx.index_desc,
                &child_outcome.union,
                child_block,
                state.scalar_services(),
            )?,
            values: child_outcome.union.clone(),
        };
        if let Some(split) = &child_outcome.split {
            self.parent_map.insert(split.right_block, parent_block);
            items[child_index] = GistTupleEntry {
                tuple: make_downlink_tuple(
                    &ctx.index_desc,
                    &split.left_union,
                    child_block,
                    state.scalar_services(),
                )?,
                values: split.left_union.clone(),
            };
            items.insert(
                child_index + 1,
                GistTupleEntry {
                    tuple: make_downlink_tuple(
                        &ctx.index_desc,
                        &split.right_union,
                        split.right_block,
                        state.scalar_services(),
                    )?,
                    values: split.right_union.clone(),
                },
            );
        }

        let parent_level = child_level.saturating_add(1);
        let parent_outcome = write_or_split_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            &ctx.index_desc,
            state,
            parent_block,
            parent_opaque,
            items,
            parent_block == GIST_ROOT_BLKNO,
            GistPageWriteMode::BuildNoExtend,
            wal,
        )?;
        if child_outcome.split.is_some() {
            clear_follow_right_with_mode(
                &ctx.pool,
                ctx.client_id,
                ctx.snapshot.current_xid,
                ctx.index_relation,
                child_block,
                parent_outcome.write_lsn,
                GistPageWriteMode::BuildNoExtend,
                wal,
            )?;
        }
        self.finish_insert_outcome(ctx, state, parent_block, parent_level, parent_outcome, wal)
    }

    fn apply_split_side_effects(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        block: u32,
        level: u16,
        outcome: &mut InsertOutcome,
    ) -> Result<(), AccessError> {
        if let Some(root_split) = &outcome.root_split {
            self.root_level = self.root_level.max(level.saturating_add(1));
            self.parent_map
                .insert(root_split.left_block, GIST_ROOT_BLKNO);
            self.parent_map
                .insert(root_split.right_block, GIST_ROOT_BLKNO);
            if level > 0 {
                self.memorize_page_children(ctx, state, root_split.left_block, level)?;
                self.memorize_page_children(ctx, state, root_split.right_block, level)?;
            }
        }

        let Some(split) = outcome.split.as_mut() else {
            return Ok(());
        };
        if level > 0 {
            self.memorize_page_children(ctx, state, block, level)?;
            self.memorize_page_children(ctx, state, split.right_block, level)?;
        }
        if self.level_has_buffers(level) {
            let (left_union, right_union) =
                self.relocate_buffer_on_split(ctx, state, block, level, split)?;
            outcome.union = left_union.clone();
            split.left_union = left_union;
            split.right_union = right_union;
        }
        Ok(())
    }

    fn relocate_buffer_on_split(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        block: u32,
        level: u16,
        split: &ChildSplit,
    ) -> Result<(Vec<Value>, Vec<Value>), AccessError> {
        if self
            .node_buffers
            .get(&block)
            .is_none_or(GistNodeBuffer::is_empty)
        {
            return Ok((split.left_union.clone(), split.right_union.clone()));
        }

        let mut entries = Vec::new();
        while let Some(entry) =
            self.pop_from_node_buffer(block, &ctx.index_desc, state.scalar_services())?
        {
            entries.push(entry);
        }

        let mut targets = vec![
            SplitRelocationTarget {
                block,
                union: split.left_union.clone(),
            },
            SplitRelocationTarget {
                block: split.right_block,
                union: split.right_union.clone(),
            },
        ];

        self.get_node_buffer(block, level);
        self.get_node_buffer(split.right_block, level);
        for entry in entries {
            let target_entries = targets
                .iter()
                .map(|target| {
                    Ok(GistTupleEntry {
                        tuple: make_downlink_tuple(
                            &ctx.index_desc,
                            &target.union,
                            target.block,
                            state.scalar_services(),
                        )?,
                        values: target.union.clone(),
                    })
                })
                .collect::<Result<Vec<_>, AccessError>>()?;
            let target_index =
                choose_child(&ctx.index_desc, state, &target_entries, &entry.values)?;
            let target_block = targets[target_index].block;
            let entry_values = entry.values.clone();
            self.push_to_node_buffer(target_block, level, entry)?;
            targets[target_index].union =
                state.merge_values(&targets[target_index].union, &entry_values)?;
        }

        Ok((targets[0].union.clone(), targets[1].union.clone()))
    }

    fn process_emptying_queue(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        wal: &dyn AccessWalServices,
    ) -> Result<(), AccessError> {
        while let Some(block) = self.emptying_queue.pop_front() {
            self.queued_blocks.remove(&block);
            if !self.node_buffers.contains_key(&block) {
                continue;
            }
            self.unload_loaded_buffers()?;
            loop {
                let (level, entry) = {
                    let Some(node) = self.node_buffers.get(&block) else {
                        break;
                    };
                    let level = node.level;
                    let entry =
                        self.pop_from_node_buffer(block, &ctx.index_desc, state.scalar_services())?;
                    (level, entry)
                };
                let Some(entry) = entry else {
                    break;
                };
                let lower_overflow = self.process_entry(ctx, state, entry, block, level, wal)?;
                if lower_overflow {
                    break;
                }
            }
            if self
                .node_buffers
                .get(&block)
                .is_some_and(|node| node.buffer_page_count() > self.pages_per_buffer / 2)
            {
                self.queue_node(block);
            }
        }
        Ok(())
    }

    fn get_node_buffer(&mut self, block: u32, level: u16) -> &mut GistNodeBuffer {
        if !self.node_buffers.contains_key(&block) {
            self.level_buffers
                .entry(level)
                .or_default()
                .push_front(block);
        }
        self.node_buffers
            .entry(block)
            .or_insert_with(|| GistNodeBuffer::new(block, level))
    }

    fn push_to_node_buffer(
        &mut self,
        block: u32,
        level: u16,
        entry: GistTupleEntry,
    ) -> Result<bool, AccessError> {
        let pages_per_buffer = self.pages_per_buffer;
        if !self.node_buffers.contains_key(&block) {
            self.level_buffers
                .entry(level)
                .or_default()
                .push_front(block);
        }
        let node = self
            .node_buffers
            .entry(block)
            .or_insert_with(|| GistNodeBuffer::new(block, level));
        node.push_tuple(entry.tuple, &mut self.temp_file)?;
        let page_count = node.buffer_page_count();
        self.loaded_buffers.insert(block);
        if page_count > pages_per_buffer / 2 {
            self.queue_node(block);
        }
        Ok(page_count > pages_per_buffer)
    }

    fn pop_from_node_buffer(
        &mut self,
        block: u32,
        desc: &RelationDesc,
        scalar: &dyn AccessScalarServices,
    ) -> Result<Option<GistTupleEntry>, AccessError> {
        let Some(node) = self.node_buffers.get_mut(&block) else {
            return Ok(None);
        };
        let entry = node.pop_entry(desc, scalar, &mut self.temp_file)?;
        if !node.tail.is_empty() {
            self.loaded_buffers.insert(block);
        }
        Ok(entry)
    }

    fn unload_loaded_buffers(&mut self) -> Result<(), AccessError> {
        let blocks = self.loaded_buffers.drain().collect::<Vec<_>>();
        for block in blocks {
            if let Some(node) = self.node_buffers.get_mut(&block) {
                node.unload_tail(&mut self.temp_file)?;
            }
        }
        Ok(())
    }

    fn queue_node(&mut self, block: u32) {
        if self.queued_blocks.insert(block) {
            self.emptying_queue.push_back(block);
        }
    }

    fn next_buffer_on_level(&mut self, level: u16) -> Option<u32> {
        while let Some(block) = self.level_buffers.get_mut(&level)?.pop_front() {
            if self.node_buffers.contains_key(&block) {
                return Some(block);
            }
        }
        None
    }

    fn node_is_empty(&self, block: u32) -> bool {
        self.node_buffers
            .get(&block)
            .is_none_or(GistNodeBuffer::is_empty)
    }

    fn any_nonempty_buffer(&self) -> bool {
        self.node_buffers
            .values()
            .any(|node| !GistNodeBuffer::is_empty(node))
    }

    fn level_has_buffers(&self, level: u16) -> bool {
        level != 0 && level % self.level_step == 0 && level != self.root_level
    }

    fn rebuild_parent_map(
        &mut self,
        ctx: &IndexBuildContext,
        _state: &GistState,
    ) -> Result<(), AccessError> {
        self.parent_map.clear();
        self.memorize_page_children_recursive(ctx, _state, GIST_ROOT_BLKNO, self.root_level)
    }

    fn memorize_page_children_recursive(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        block: u32,
        level: u16,
    ) -> Result<(), AccessError> {
        if level == 0 {
            return Ok(());
        }
        let children = self.memorize_page_children(ctx, state, block, level)?;
        for child in children {
            self.memorize_page_children_recursive(ctx, state, child, level - 1)?;
        }
        Ok(())
    }

    fn memorize_page_children(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        block: u32,
        level: u16,
    ) -> Result<Vec<u32>, AccessError> {
        if level == 0 {
            return Ok(Vec::new());
        }
        let page = read_buffered_page(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
        let opaque = gist_page_get_opaque(&page)
            .map_err(|err| AccessError::Scalar(format!("gist page parse failed: {err:?}")))?;
        if opaque.is_leaf() {
            return Ok(Vec::new());
        }
        let items = load_page_entries(&ctx.index_desc, &page, state.scalar_services())?;
        let mut children = Vec::with_capacity(items.len());
        for item in items {
            if let Some(child) = gist_downlink_block(&item.tuple) {
                self.parent_map.insert(child, block);
                children.push(child);
            }
        }
        Ok(children)
    }

    fn parent_block(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        child_block: u32,
    ) -> Result<u32, AccessError> {
        if let Some(parent) = self.parent_map.get(&child_block).copied() {
            return Ok(parent);
        }
        self.rebuild_parent_map(ctx, state)?;
        self.parent_map
            .get(&child_block)
            .copied()
            .ok_or(AccessError::Corrupt(
                "GiST buffering parent map missing child",
            ))
    }
}

#[derive(Debug)]
struct SplitRelocationTarget {
    block: u32,
    union: Vec<Value>,
}

fn get_max_level(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    desc: &RelationDesc,
    scalar: &dyn AccessScalarServices,
) -> Result<u16, AccessError> {
    let mut block = GIST_ROOT_BLKNO;
    let mut level = 0u16;
    loop {
        let page = read_buffered_page(pool, client_id, rel, block)?;
        let opaque = gist_page_get_opaque(&page)
            .map_err(|err| AccessError::Scalar(format!("gist page parse failed: {err:?}")))?;
        if opaque.is_leaf() {
            return Ok(level);
        }
        let items = load_page_entries(desc, &page, scalar)?;
        let first = items
            .first()
            .ok_or(AccessError::Corrupt("empty GiST internal page"))?;
        block = gist_downlink_block(&first.tuple).ok_or(AccessError::Corrupt(
            "gist internal tuple missing child block",
        ))?;
        level = level.saturating_add(1);
    }
}

fn calculate_build_buffer_parameters(
    maintenance_work_mem_kb: usize,
    effective_cache_pages: usize,
    stats: GistBuildBufferStats,
) -> Option<(u16, usize)> {
    if stats.tuples == 0 {
        return None;
    }
    let memory_pages = maintenance_work_mem_kb
        .saturating_mul(1024)
        .max(GIST_BUILD_BUFFERED_MIN_WORK_MEM_BYTES)
        / BLCKSZ;
    if memory_pages == 0 || effective_cache_pages == 0 {
        return None;
    }
    let avg_tuple_size = (stats.total_tuple_bytes / stats.tuples).max(1);
    let min_tuple_size = stats.min_tuple_bytes.max(1);
    let avg_tuples_per_page = (GIST_BUILD_PAGE_USABLE_BYTES / avg_tuple_size).max(2);
    let max_tuples_per_page = (GIST_BUILD_PAGE_USABLE_BYTES / min_tuple_size).max(2);

    let mut level_step = 1usize;
    loop {
        let subtree_size = geometric_subtree_pages(avg_tuples_per_page, level_step + 1);
        let max_lowest_pages = saturating_pow(max_tuples_per_page, level_step);
        if subtree_size > effective_cache_pages / 4 || max_lowest_pages > memory_pages {
            break;
        }
        level_step = level_step.saturating_add(1);
        if level_step > u16::MAX as usize {
            break;
        }
    }
    level_step = level_step.saturating_sub(1);
    if level_step == 0 {
        return None;
    }
    Some((
        level_step as u16,
        calculate_pages_per_buffer(stats, level_step as u16)?,
    ))
}

fn calculate_pages_per_buffer(stats: GistBuildBufferStats, level_step: u16) -> Option<usize> {
    if stats.tuples == 0 {
        return None;
    }
    let avg_tuple_size = (stats.total_tuple_bytes / stats.tuples).max(1);
    let avg_tuples_per_page = (GIST_BUILD_PAGE_USABLE_BYTES / avg_tuple_size).max(2);
    Some(
        2usize
            .saturating_mul(saturating_pow(avg_tuples_per_page, level_step as usize))
            .max(1),
    )
}

fn geometric_subtree_pages(fanout: usize, levels: usize) -> usize {
    let mut total = 0usize;
    let mut level_pages = 1usize;
    for _ in 0..levels {
        total = total.saturating_add(level_pages);
        level_pages = level_pages.saturating_mul(fanout);
    }
    total
}

fn saturating_pow(base: usize, exp: usize) -> usize {
    let mut out = 1usize;
    for _ in 0..exp {
        out = out.saturating_mul(base);
    }
    out
}

impl GistNodeBuffer {
    fn new(_block: u32, level: u16) -> Self {
        Self {
            level,
            tail: Vec::new(),
            tail_bytes: GIST_BUILD_TEMP_PAGE_HEADER_SIZE,
            head_temp_block: None,
            spilled_pages: 0,
            tuple_count: 0,
        }
    }

    fn push_tuple(
        &mut self,
        tuple: IndexTupleData,
        temp_file: &mut TempGistBuildFile,
    ) -> Result<(), AccessError> {
        let tuple_len = tuple.serialize().len();
        if tuple_len > u16::MAX as usize
            || GIST_BUILD_TEMP_PAGE_HEADER_SIZE + 2 + tuple_len > BLCKSZ
        {
            return Err(AccessError::Scalar(
                "GiST build tuple too large for temp buffer page".into(),
            ));
        }
        if self.tail_bytes > GIST_BUILD_TEMP_PAGE_HEADER_SIZE
            && self.tail_bytes + 2 + tuple_len > BLCKSZ
        {
            self.unload_tail(temp_file)?;
        }
        self.tail_bytes += 2 + tuple_len;
        self.tail.push(tuple);
        self.tuple_count = self.tuple_count.saturating_add(1);
        Ok(())
    }

    fn pop_entry(
        &mut self,
        desc: &RelationDesc,
        scalar: &dyn AccessScalarServices,
        temp_file: &mut TempGistBuildFile,
    ) -> Result<Option<GistTupleEntry>, AccessError> {
        if self.tuple_count == 0 {
            return Ok(None);
        }
        if self.tail.is_empty() {
            self.load_tail(temp_file)?;
        }
        let tuple = self.tail.pop().ok_or(AccessError::Corrupt(
            "GiST node buffer had tuple count but no tuple",
        ))?;
        self.tuple_count -= 1;
        self.tail_bytes = temp_page_encoded_len(&self.tail);
        Ok(Some(GistTupleEntry {
            values: decode_tuple_values(desc, &tuple, scalar)?,
            tuple,
        }))
    }

    fn unload_tail(&mut self, temp_file: &mut TempGistBuildFile) -> Result<(), AccessError> {
        if self.tail.is_empty() {
            return Ok(());
        }
        let page = encode_temp_page(self.head_temp_block, &self.tail)?;
        let block = temp_file.write_page(&page)?;
        self.head_temp_block = Some(block);
        self.spilled_pages = self.spilled_pages.saturating_add(1);
        self.tail.clear();
        self.tail_bytes = GIST_BUILD_TEMP_PAGE_HEADER_SIZE;
        Ok(())
    }

    fn load_tail(&mut self, temp_file: &mut TempGistBuildFile) -> Result<(), AccessError> {
        let Some(block) = self.head_temp_block else {
            return Ok(());
        };
        let page = temp_file.read_page(block)?;
        temp_file.release_block(block);
        let (prev, tuples) = decode_temp_page(&page)?;
        self.head_temp_block = prev;
        self.spilled_pages = self.spilled_pages.saturating_sub(1);
        self.tail_bytes = temp_page_encoded_len(&tuples);
        self.tail = tuples;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.tuple_count == 0
    }

    fn buffer_page_count(&self) -> usize {
        self.spilled_pages + usize::from(!self.tail.is_empty())
    }
}

fn temp_page_encoded_len(tuples: &[IndexTupleData]) -> usize {
    GIST_BUILD_TEMP_PAGE_HEADER_SIZE
        + tuples
            .iter()
            .map(|tuple| 2 + tuple.serialize().len())
            .sum::<usize>()
}

fn encode_temp_page(
    prev_block: Option<u64>,
    tuples: &[IndexTupleData],
) -> Result<[u8; BLCKSZ], AccessError> {
    if tuples.len() > u16::MAX as usize {
        return Err(AccessError::Scalar(
            "too many GiST tuples for temp buffer page".into(),
        ));
    }
    let mut page = [0u8; BLCKSZ];
    page[0..8].copy_from_slice(
        &prev_block
            .unwrap_or(GIST_BUILD_TEMP_PAGE_NONE)
            .to_le_bytes(),
    );
    page[8..10].copy_from_slice(&(tuples.len() as u16).to_le_bytes());
    let mut pos = GIST_BUILD_TEMP_PAGE_HEADER_SIZE;
    for tuple in tuples {
        let bytes = tuple.serialize();
        if bytes.len() > u16::MAX as usize || pos + 2 + bytes.len() > BLCKSZ {
            return Err(AccessError::Scalar("GiST temp buffer page overflow".into()));
        }
        page[pos..pos + 2].copy_from_slice(&(bytes.len() as u16).to_le_bytes());
        pos += 2;
        page[pos..pos + bytes.len()].copy_from_slice(&bytes);
        pos += bytes.len();
    }
    Ok(page)
}

fn decode_temp_page(
    page: &[u8; BLCKSZ],
) -> Result<(Option<u64>, Vec<IndexTupleData>), AccessError> {
    let raw_prev = u64::from_le_bytes(page[0..8].try_into().unwrap());
    let prev = (raw_prev != GIST_BUILD_TEMP_PAGE_NONE).then_some(raw_prev);
    let count = u16::from_le_bytes(page[8..10].try_into().unwrap()) as usize;
    let mut pos = GIST_BUILD_TEMP_PAGE_HEADER_SIZE;
    let mut tuples = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 2 > page.len() {
            return Err(AccessError::Corrupt("truncated GiST temp buffer page"));
        }
        let len = u16::from_le_bytes(page[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + len > page.len() {
            return Err(AccessError::Corrupt("truncated GiST temp buffer tuple"));
        }
        let tuple = IndexTupleData::parse(&page[pos..pos + len]).map_err(|err| {
            AccessError::Scalar(format!("GiST temp tuple decode failed: {err:?}"))
        })?;
        tuples.push(tuple);
        pos += len;
    }
    Ok((prev, tuples))
}

impl TempGistBuildFile {
    fn new() -> Result<Self, AccessError> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let mut last_err = None;
        for _ in 0..16 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("pgrust-gist-build-{}-{id}.tmp", std::process::id()));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file,
                        next_block: 0,
                        free_blocks: Vec::new(),
                    });
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(AccessError::Scalar(format!(
            "could not create GiST build temp file: {}",
            last_err
                .map(|err| err.to_string())
                .unwrap_or_else(|| "unknown error".into())
        )))
    }

    fn write_page(&mut self, page: &[u8; BLCKSZ]) -> Result<u64, AccessError> {
        let block = self.free_blocks.pop().unwrap_or_else(|| {
            let block = self.next_block;
            self.next_block = self.next_block.saturating_add(1);
            block
        });
        self.file
            .seek(SeekFrom::Start(block.saturating_mul(BLCKSZ as u64)))
            .map_err(|err| AccessError::Scalar(format!("GiST temp seek failed: {err}")))?;
        self.file
            .write_all(page)
            .map_err(|err| AccessError::Scalar(format!("GiST temp write failed: {err}")))?;
        Ok(block)
    }

    fn read_page(&mut self, block: u64) -> Result<[u8; BLCKSZ], AccessError> {
        let mut page = [0u8; BLCKSZ];
        self.file
            .seek(SeekFrom::Start(block.saturating_mul(BLCKSZ as u64)))
            .map_err(|err| AccessError::Scalar(format!("GiST temp seek failed: {err}")))?;
        self.file
            .read_exact(&mut page)
            .map_err(|err| AccessError::Scalar(format!("GiST temp read failed: {err}")))?;
        Ok(page)
    }

    fn release_block(&mut self, block: u64) {
        self.free_blocks.push(block);
    }
}

impl Drop for TempGistBuildFile {
    fn drop(&mut self) {
        let _ = remove_file(&self.path);
    }
}
