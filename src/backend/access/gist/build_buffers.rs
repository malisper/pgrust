use std::collections::{HashMap, HashSet, VecDeque};

use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexBuildContext;

use super::build::GistBuildTuple;
use super::insert::{GistTupleEntry, find_target_leaf_block, insert_build_entries};
use super::state::GistState;

const GIST_BUILD_BUFFERED_INSERT_MAX_TUPLES: usize = 8;
const GIST_BUILD_BUFFERED_INSERT_MAX_BYTES: usize = crate::backend::storage::smgr::BLCKSZ / 16;
const GIST_BUILD_BUFFERED_MIN_WORK_MEM_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(super) struct GistBuildBuffers {
    nodes: HashMap<u32, GistNodeBuffer>,
    queued: VecDeque<u32>,
    queued_blocks: HashSet<u32>,
    bytes: usize,
    memory_limit_bytes: usize,
    per_node_limit_bytes: usize,
}

#[derive(Debug)]
struct GistNodeBuffer {
    level: u16,
    entries: Vec<BufferedGistEntry>,
    bytes: usize,
}

#[derive(Debug)]
struct BufferedGistEntry {
    entry: GistTupleEntry,
    approx_size: usize,
}

impl GistBuildBuffers {
    pub(super) fn new(maintenance_work_mem_kb: usize) -> Self {
        let memory_limit_bytes = maintenance_work_mem_kb
            .saturating_mul(1024)
            .max(GIST_BUILD_BUFFERED_MIN_WORK_MEM_BYTES);
        let per_node_limit_bytes = (memory_limit_bytes / 32).clamp(
            GIST_BUILD_BUFFERED_INSERT_MAX_BYTES,
            crate::backend::storage::smgr::BLCKSZ * 2,
        );
        Self {
            nodes: HashMap::new(),
            queued: VecDeque::new(),
            queued_blocks: HashSet::new(),
            bytes: 0,
            memory_limit_bytes,
            per_node_limit_bytes,
        }
    }

    pub(super) fn insert(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        tuple: GistBuildTuple,
    ) -> Result<(), CatalogError> {
        let target_block = find_target_leaf_block(
            &ctx.pool,
            ctx.client_id,
            ctx.index_relation,
            &ctx.index_desc,
            state,
            &tuple.key_values,
        )?;
        let approx_size = tuple.approx_size.max(1);
        let should_queue = {
            let node = self
                .nodes
                .entry(target_block)
                .or_insert_with(|| GistNodeBuffer {
                    level: 0,
                    entries: Vec::new(),
                    bytes: 0,
                });
            node.bytes = node.bytes.saturating_add(approx_size);
            node.entries.push(BufferedGistEntry {
                entry: GistTupleEntry {
                    tuple: tuple.leaf_tuple,
                    values: tuple.key_values,
                },
                approx_size,
            });
            self.bytes = self.bytes.saturating_add(approx_size);
            node.bytes >= self.per_node_limit_bytes
                || node.entries.len() >= GIST_BUILD_BUFFERED_INSERT_MAX_TUPLES
        };
        if should_queue {
            self.queue_node(target_block);
        }
        self.flush_queued(ctx, state)?;
        while self.bytes >= self.memory_limit_bytes {
            if !self.flush_next(ctx, state)? {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn flush_all(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
    ) -> Result<(), CatalogError> {
        while !self.nodes.is_empty() {
            if self.queued.is_empty() {
                let mut blocks = self.nodes.keys().copied().collect::<Vec<_>>();
                blocks.sort_by_key(|block| self.nodes.get(block).map(|node| node.level));
                for block in blocks {
                    self.queue_node(block);
                }
            }
            if !self.flush_next(ctx, state)? {
                break;
            }
        }
        Ok(())
    }

    fn flush_queued(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
    ) -> Result<(), CatalogError> {
        while self.flush_next(ctx, state)? {}
        Ok(())
    }

    fn queue_node(&mut self, block: u32) {
        if self.queued_blocks.insert(block) {
            self.queued.push_back(block);
        }
    }

    fn flush_next(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
    ) -> Result<bool, CatalogError> {
        while let Some(block) = self.queued.pop_front() {
            self.queued_blocks.remove(&block);
            if self.nodes.contains_key(&block) {
                self.flush_node(ctx, state, block)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn flush_node(
        &mut self,
        ctx: &IndexBuildContext,
        state: &GistState,
        block: u32,
    ) -> Result<(), CatalogError> {
        let (entries, should_requeue) = {
            let Some(node) = self.nodes.get_mut(&block) else {
                return Ok(());
            };
            let mut count = 0usize;
            let mut chunk_bytes = 0usize;
            for entry in &node.entries {
                let next_bytes = entry.approx_size.max(1);
                if count > 0
                    && (count >= GIST_BUILD_BUFFERED_INSERT_MAX_TUPLES
                        || chunk_bytes.saturating_add(next_bytes)
                            > GIST_BUILD_BUFFERED_INSERT_MAX_BYTES)
                {
                    break;
                }
                count += 1;
                chunk_bytes = chunk_bytes.saturating_add(next_bytes);
            }
            let drained = node.entries.drain(..count).collect::<Vec<_>>();
            node.bytes = node.bytes.saturating_sub(chunk_bytes);
            self.bytes = self.bytes.saturating_sub(chunk_bytes);
            let should_requeue = !node.entries.is_empty()
                && (node.bytes >= self.per_node_limit_bytes
                    || node.entries.len() >= GIST_BUILD_BUFFERED_INSERT_MAX_TUPLES);
            let entries = drained
                .into_iter()
                .map(|buffered| buffered.entry)
                .collect::<Vec<_>>();
            (entries, should_requeue)
        };

        if self
            .nodes
            .get(&block)
            .is_some_and(|node| node.entries.is_empty())
        {
            self.nodes.remove(&block);
        }
        if should_requeue {
            self.queue_node(block);
        }
        insert_build_entries(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            &ctx.index_desc,
            state,
            entries,
        )?;
        Ok(())
    }
}
