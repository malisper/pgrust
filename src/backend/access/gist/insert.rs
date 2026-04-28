use crate::backend::access::transam::xlog::{
    XLOG_GIST_INSERT, XLOG_GIST_PAGE_UPDATE, XLOG_GIST_SPLIT,
};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::page::bufpage::PageError;
use crate::include::access::amapi::IndexInsertContext;
use crate::include::access::gist::{
    F_FOLLOW_RIGHT, F_LEAF, GIST_INVALID_BLOCKNO, GIST_ROOT_BLKNO, GistPageError,
    gist_downlink_block, gist_page_append_tuple, gist_page_get_opaque, gist_page_items,
    gist_page_replace_items,
};
use crate::include::access::itup::IndexTupleData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;
use std::sync::OnceLock;

use super::page::{
    GistLoggedPage, GistPageWriteMode, allocate_new_block_with_mode, clear_follow_right_with_mode,
    ensure_empty_gist, init_opaque, page_lsn, read_buffered_page, relation_nblocks,
    write_buffered_page_with_mode, write_logged_pages_with_mode,
};
use super::state::{GistPageSplit, GistState};
use super::tuple::{decode_tuple_values, make_downlink_tuple, make_leaf_tuple, tuple_storage_size};

fn gist_insert_mutex() -> &'static parking_lot::Mutex<()> {
    static GIST_INSERT_MUTEX: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    GIST_INSERT_MUTEX.get_or_init(|| parking_lot::Mutex::new(()))
}

enum GistWriteError {
    NoSpace,
    Catalog(CatalogError),
}

impl From<CatalogError> for GistWriteError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

#[derive(Debug, Clone)]
pub(super) struct GistTupleEntry {
    pub(super) tuple: IndexTupleData,
    pub(super) values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub(super) struct ChildSplit {
    pub(super) right_block: u32,
    pub(super) left_union: Vec<Value>,
    pub(super) right_union: Vec<Value>,
}

#[derive(Debug, Clone)]
pub(super) struct RootSplit {
    pub(super) left_block: u32,
    pub(super) right_block: u32,
}

#[derive(Debug, Clone)]
pub(super) struct InsertOutcome {
    pub(super) union: Vec<Value>,
    pub(super) split: Option<ChildSplit>,
    pub(super) root_split: Option<RootSplit>,
    pub(super) write_lsn: u64,
}

pub(crate) fn gistinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    let _guard = gist_insert_mutex().lock();
    if relation_nblocks(&ctx.pool, ctx.index_relation)? == 0 {
        ensure_empty_gist(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
        )?;
    }
    let state = GistState::new(&ctx.index_desc, &ctx.index_meta)?;
    let new_entry = GistTupleEntry {
        tuple: make_leaf_tuple(&ctx.index_desc, &ctx.values, ctx.heap_tid)?,
        values: ctx.values.clone(),
    };
    let _ = insert_entries_into_block(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        &ctx.index_desc,
        &state,
        GIST_ROOT_BLKNO,
        vec![new_entry],
        None,
        true,
        GistPageWriteMode::Normal,
    )?;
    Ok(true)
}

pub(super) fn find_target_leaf_block(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    state: &GistState,
    values: &[Value],
) -> Result<u32, CatalogError> {
    let mut block = GIST_ROOT_BLKNO;
    loop {
        let page = read_buffered_page(pool, client_id, rel, block)?;
        let opaque = gist_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("gist page parse failed: {err:?}")))?;
        if opaque.is_leaf() {
            return Ok(block);
        }
        let items = load_page_entries(desc, &page)?;
        let child_index = choose_child(desc, state, &items, values)?;
        block = gist_downlink_block(&items[child_index].tuple).ok_or(CatalogError::Corrupt(
            "gist internal tuple missing child block",
        ))?;
    }
}

pub(super) fn insert_build_entries(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    state: &GistState,
    entries: Vec<GistTupleEntry>,
) -> Result<(), CatalogError> {
    insert_build_entries_with_mode(
        pool,
        client_id,
        xid,
        rel,
        desc,
        state,
        entries,
        GistPageWriteMode::BuildNoExtend,
    )
}

pub(super) fn insert_build_entries_with_mode(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    state: &GistState,
    entries: Vec<GistTupleEntry>,
    mode: GistPageWriteMode,
) -> Result<(), CatalogError> {
    if entries.is_empty() {
        return Ok(());
    }
    let _guard = gist_insert_mutex().lock();
    let _ = insert_entries_into_block(
        pool,
        client_id,
        xid,
        rel,
        desc,
        state,
        GIST_ROOT_BLKNO,
        entries,
        None,
        true,
        mode,
    )?;
    Ok(())
}

pub(super) fn load_page_entries(
    desc: &RelationDesc,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
) -> Result<Vec<GistTupleEntry>, CatalogError> {
    gist_page_items(page)
        .map_err(|err| CatalogError::Io(format!("gist tuple parse failed: {err:?}")))?
        .into_iter()
        .map(|tuple| {
            let values = decode_tuple_values(desc, &tuple)?;
            Ok(GistTupleEntry { tuple, values })
        })
        .collect()
}

pub(super) fn choose_child(
    desc: &RelationDesc,
    state: &GistState,
    items: &[GistTupleEntry],
    candidate_values: &[Value],
) -> Result<usize, CatalogError> {
    let mut best: Option<(usize, Vec<f32>, usize)> = None;
    for (index, item) in items.iter().enumerate() {
        let merged = state.merge_values(&item.values, candidate_values)?;
        let penalties = state.column_penalties(&item.values, candidate_values)?;
        let merged_size = tuple_storage_size(desc, &merged)?;
        if best
            .as_ref()
            .is_none_or(|(best_index, best_penalties, best_size)| {
                penalties_better(&penalties, best_penalties)
                    || (penalties == *best_penalties
                        && (merged_size < *best_size
                            || (merged_size == *best_size && index < *best_index)))
            })
        {
            best = Some((index, penalties, merged_size));
        }
    }
    best.map(|(index, _, _)| index)
        .ok_or(CatalogError::Corrupt("empty GiST internal page"))
}

fn penalties_better(left: &[f32], right: &[f32]) -> bool {
    for (left_penalty, right_penalty) in left.iter().zip(right.iter()) {
        if left_penalty < right_penalty {
            return true;
        }
        if left_penalty > right_penalty {
            return false;
        }
    }
    false
}

fn merge_appended_values(
    state: &GistState,
    old_union: &[Value],
    new_entries: &[GistTupleEntry],
) -> Result<Vec<Value>, CatalogError> {
    let mut union = old_union.to_vec();
    for entry in new_entries {
        union = state.merge_values(&union, &entry.values)?;
    }
    Ok(union)
}

fn try_append_leaf_entries(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
    state: &GistState,
    block: u32,
    old_union: Option<&[Value]>,
    new_entries: &[GistTupleEntry],
    mode: GistPageWriteMode,
) -> Result<Option<InsertOutcome>, CatalogError> {
    let Some(old_union) = old_union else {
        return Ok(None);
    };
    let mut appended = *page;
    for entry in new_entries {
        match gist_page_append_tuple(&mut appended, &entry.tuple) {
            Ok(_) => {}
            Err(crate::include::access::gist::GistPageError::Page(PageError::NoSpace)) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(CatalogError::Io(format!(
                    "gist leaf append failed: {err:?}"
                )));
            }
        }
    }
    let write_lsn = write_buffered_page_with_mode(
        pool,
        client_id,
        xid,
        rel,
        block,
        &appended,
        XLOG_GIST_INSERT,
        mode,
    )?;
    Ok(Some(InsertOutcome {
        union: merge_appended_values(state, old_union, new_entries)?,
        split: None,
        root_split: None,
        write_lsn,
    }))
}

fn try_write_page(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    block: u32,
    opaque: crate::include::access::gist::GistPageOpaqueData,
    items: &[GistTupleEntry],
    wal_info: u8,
    mode: GistPageWriteMode,
) -> Result<u64, GistWriteError> {
    let mut rebuilt = [0u8; crate::backend::storage::smgr::BLCKSZ];
    let tuples = items
        .iter()
        .map(|item| item.tuple.clone())
        .collect::<Vec<_>>();
    gist_page_replace_items(&mut rebuilt, &tuples, opaque).map_err(|err| match err {
        GistPageError::Page(PageError::NoSpace) => GistWriteError::NoSpace,
        other => GistWriteError::Catalog(CatalogError::Io(format!(
            "gist page rebuild failed: {other:?}"
        ))),
    })?;
    write_buffered_page_with_mode(pool, client_id, xid, rel, block, &rebuilt, wal_info, mode)
        .map_err(Into::into)
}

fn split_page_entries(
    items: &[GistTupleEntry],
    split: &GistPageSplit,
) -> Result<(Vec<GistTupleEntry>, Vec<GistTupleEntry>), CatalogError> {
    let mut left_flags = vec![false; items.len()];
    let mut right_flags = vec![false; items.len()];
    for index in &split.left {
        if let Some(slot) = left_flags.get_mut(*index) {
            *slot = true;
        }
    }
    for index in &split.right {
        if let Some(slot) = right_flags.get_mut(*index) {
            *slot = true;
        }
    }
    let mut left = Vec::new();
    let mut right = Vec::new();
    for (index, item) in items.iter().cloned().enumerate() {
        if left_flags[index] {
            left.push(item);
        } else if right_flags[index] {
            right.push(item);
        } else if left.len() <= right.len() {
            left.push(item);
        } else {
            right.push(item);
        }
    }
    if left.is_empty() || right.is_empty() {
        return Err(CatalogError::Corrupt(
            "gist picksplit returned empty partition",
        ));
    }
    Ok((left, right))
}

fn split_page_entries_balanced(
    items: &[GistTupleEntry],
) -> Result<(Vec<GistTupleEntry>, Vec<GistTupleEntry>), CatalogError> {
    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut left_size = 0usize;
    let mut right_size = 0usize;
    for item in items.iter().cloned() {
        let item_size = item.tuple.size();
        if left.is_empty() || (!right.is_empty() && left_size <= right_size) {
            left_size = left_size.saturating_add(item_size);
            left.push(item);
        } else {
            right_size = right_size.saturating_add(item_size);
            right.push(item);
        }
    }
    if right.is_empty() && left.len() > 1 {
        let moved = left.pop().expect("left length checked");
        right.push(moved);
    }
    if left.is_empty() || right.is_empty() {
        return Err(CatalogError::Corrupt(
            "gist split fallback returned empty partition",
        ));
    }
    Ok((left, right))
}

fn page_entries_can_fit(items: &[GistTupleEntry], flags: u16) -> Result<bool, CatalogError> {
    let tuples = items
        .iter()
        .map(|item| item.tuple.clone())
        .collect::<Vec<_>>();
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    match gist_page_replace_items(
        &mut page,
        &tuples,
        init_opaque(flags, GIST_INVALID_BLOCKNO, 0),
    ) {
        Ok(()) => Ok(true),
        Err(GistPageError::Page(PageError::NoSpace)) => Ok(false),
        Err(other) => Err(CatalogError::Io(format!(
            "gist split fit check failed: {other:?}"
        ))),
    }
}

fn union_entry_values(
    state: &GistState,
    items: &[GistTupleEntry],
) -> Result<Vec<Value>, CatalogError> {
    state.union_all(
        &items
            .iter()
            .map(|item| item.values.clone())
            .collect::<Vec<_>>(),
    )
}

pub(super) fn write_or_split_page(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    state: &GistState,
    block: u32,
    opaque: crate::include::access::gist::GistPageOpaqueData,
    items: Vec<GistTupleEntry>,
    is_root: bool,
    mode: GistPageWriteMode,
) -> Result<InsertOutcome, CatalogError> {
    let page_update_wal_info = if opaque.is_leaf() {
        XLOG_GIST_INSERT
    } else {
        XLOG_GIST_PAGE_UPDATE
    };
    match try_write_page(
        pool,
        client_id,
        xid,
        rel,
        block,
        opaque,
        &items,
        page_update_wal_info,
        mode,
    ) {
        Ok(write_lsn) => {
            let union = state.union_all(
                &items
                    .iter()
                    .map(|item| item.values.clone())
                    .collect::<Vec<_>>(),
            )?;
            Ok(InsertOutcome {
                union,
                split: None,
                root_split: None,
                write_lsn,
            })
        }
        Err(GistWriteError::NoSpace) => {
            let split = state.picksplit(
                &items
                    .iter()
                    .map(|item| item.values.clone())
                    .collect::<Vec<_>>(),
            )?;
            let child_flags = if opaque.is_leaf() { F_LEAF } else { 0 };
            let (mut left_items, mut right_items) = split_page_entries(&items, &split)?;
            if !page_entries_can_fit(&left_items, child_flags)?
                || !page_entries_can_fit(&right_items, child_flags)?
            {
                (left_items, right_items) = split_page_entries_balanced(&items)?;
            }
            let left_union = union_entry_values(state, &left_items)?;
            let right_union = union_entry_values(state, &right_items)?;
            let inherited_nsn = opaque.nsn;
            if is_root {
                let left_block = allocate_new_block_with_mode(pool, rel, mode)?;
                let right_block = allocate_new_block_with_mode(pool, rel, mode)?;
                let mut left_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                let mut right_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                let mut root_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                let root_items = vec![
                    GistTupleEntry {
                        tuple: make_downlink_tuple(desc, &left_union, left_block)?,
                        values: left_union.clone(),
                    },
                    GistTupleEntry {
                        tuple: make_downlink_tuple(desc, &right_union, right_block)?,
                        values: right_union.clone(),
                    },
                ];
                let root_union = state.union_all(&[left_union.clone(), right_union.clone()])?;
                gist_page_replace_items(
                    &mut left_page,
                    &left_items
                        .iter()
                        .map(|item| item.tuple.clone())
                        .collect::<Vec<_>>(),
                    init_opaque(child_flags, right_block, inherited_nsn),
                )
                .map_err(|err| CatalogError::Io(format!("gist split rebuild failed: {err:?}")))?;
                gist_page_replace_items(
                    &mut right_page,
                    &right_items
                        .iter()
                        .map(|item| item.tuple.clone())
                        .collect::<Vec<_>>(),
                    init_opaque(child_flags, opaque.rightlink, inherited_nsn),
                )
                .map_err(|err| CatalogError::Io(format!("gist split rebuild failed: {err:?}")))?;
                gist_page_replace_items(
                    &mut root_page,
                    &root_items
                        .iter()
                        .map(|item| item.tuple.clone())
                        .collect::<Vec<_>>(),
                    init_opaque(0, GIST_INVALID_BLOCKNO, inherited_nsn),
                )
                .map_err(|err| CatalogError::Io(format!("gist split rebuild failed: {err:?}")))?;
                let write_lsn = write_logged_pages_with_mode(
                    pool,
                    client_id,
                    xid,
                    rel,
                    XLOG_GIST_SPLIT,
                    &[
                        GistLoggedPage {
                            block: left_block,
                            page: &left_page,
                            will_init: true,
                        },
                        GistLoggedPage {
                            block: right_block,
                            page: &right_page,
                            will_init: true,
                        },
                        GistLoggedPage {
                            block,
                            page: &root_page,
                            will_init: false,
                        },
                    ],
                    mode,
                )?;
                Ok(InsertOutcome {
                    union: root_union,
                    split: None,
                    root_split: Some(RootSplit {
                        left_block,
                        right_block,
                    }),
                    write_lsn,
                })
            } else {
                let right_block = allocate_new_block_with_mode(pool, rel, mode)?;
                let mut left_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                let mut right_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                gist_page_replace_items(
                    &mut left_page,
                    &left_items
                        .iter()
                        .map(|item| item.tuple.clone())
                        .collect::<Vec<_>>(),
                    init_opaque(child_flags | F_FOLLOW_RIGHT, right_block, inherited_nsn),
                )
                .map_err(|err| CatalogError::Io(format!("gist split rebuild failed: {err:?}")))?;
                gist_page_replace_items(
                    &mut right_page,
                    &right_items
                        .iter()
                        .map(|item| item.tuple.clone())
                        .collect::<Vec<_>>(),
                    init_opaque(child_flags, opaque.rightlink, inherited_nsn),
                )
                .map_err(|err| CatalogError::Io(format!("gist split rebuild failed: {err:?}")))?;
                let write_lsn = write_logged_pages_with_mode(
                    pool,
                    client_id,
                    xid,
                    rel,
                    XLOG_GIST_SPLIT,
                    &[
                        GistLoggedPage {
                            block,
                            page: &left_page,
                            will_init: false,
                        },
                        GistLoggedPage {
                            block: right_block,
                            page: &right_page,
                            will_init: true,
                        },
                    ],
                    mode,
                )?;
                Ok(InsertOutcome {
                    union: left_union.clone(),
                    split: Some(ChildSplit {
                        right_block,
                        left_union,
                        right_union,
                    }),
                    root_split: None,
                    write_lsn,
                })
            }
        }
        Err(GistWriteError::Catalog(err)) => Err(err),
    }
}

pub(super) fn insert_entries_into_block(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    state: &GistState,
    block: u32,
    new_entries: Vec<GistTupleEntry>,
    old_union: Option<&[Value]>,
    is_root: bool,
    mode: GistPageWriteMode,
) -> Result<InsertOutcome, CatalogError> {
    if new_entries.is_empty() {
        let page = read_buffered_page(pool, client_id, rel, block)?;
        let items = load_page_entries(desc, &page)?;
        let union = state.union_all(
            &items
                .iter()
                .map(|item| item.values.clone())
                .collect::<Vec<_>>(),
        )?;
        return Ok(InsertOutcome {
            union,
            split: None,
            root_split: None,
            write_lsn: page_lsn(&page),
        });
    }

    let page = read_buffered_page(pool, client_id, rel, block)?;
    let opaque = gist_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("gist page parse failed: {err:?}")))?;

    if opaque.is_leaf() {
        if let Some(outcome) = try_append_leaf_entries(
            pool,
            client_id,
            xid,
            rel,
            &page,
            state,
            block,
            old_union,
            &new_entries,
            mode,
        )? {
            return Ok(outcome);
        }
        let mut items = load_page_entries(desc, &page)?;
        items.extend(new_entries);
        return write_or_split_page(
            pool, client_id, xid, rel, desc, state, block, opaque, items, is_root, mode,
        );
    }

    let mut items = load_page_entries(desc, &page)?;
    let mut child_batches: Vec<(usize, Vec<GistTupleEntry>)> = Vec::new();
    for new_entry in new_entries {
        let child_index = choose_child(desc, state, &items, &new_entry.values)?;
        if let Some((_, entries)) = child_batches
            .iter_mut()
            .find(|(index, _)| *index == child_index)
        {
            entries.push(new_entry);
        } else {
            child_batches.push((child_index, vec![new_entry]));
        }
    }
    child_batches.sort_by_key(|(index, _)| *index);

    let mut changed_parent = false;
    let mut child_split_blocks = Vec::new();
    let mut latest_child_lsn = page_lsn(&page);

    for (child_index, entries) in child_batches.into_iter().rev() {
        let child_block = gist_downlink_block(&items[child_index].tuple).ok_or(
            CatalogError::Corrupt("gist internal tuple missing child block"),
        )?;
        let child_old_union = items[child_index].values.clone();
        let child_outcome = insert_entries_into_block(
            pool,
            client_id,
            xid,
            rel,
            desc,
            state,
            child_block,
            entries,
            Some(&child_old_union),
            false,
            mode,
        )?;
        latest_child_lsn = latest_child_lsn.max(child_outcome.write_lsn);

        if child_outcome.split.is_none()
            && state.same_values(&items[child_index].values, &child_outcome.union)?
        {
            continue;
        }

        changed_parent = true;
        items[child_index] = GistTupleEntry {
            tuple: make_downlink_tuple(desc, &child_outcome.union, child_block)?,
            values: child_outcome.union.clone(),
        };
        if let Some(split) = &child_outcome.split {
            items[child_index] = GistTupleEntry {
                tuple: make_downlink_tuple(desc, &split.left_union, child_block)?,
                values: split.left_union.clone(),
            };
            items.insert(
                child_index + 1,
                GistTupleEntry {
                    tuple: make_downlink_tuple(desc, &split.right_union, split.right_block)?,
                    values: split.right_union.clone(),
                },
            );
            child_split_blocks.push(child_block);
        }
    }

    if !changed_parent {
        let union = state.union_all(
            &items
                .iter()
                .map(|item| item.values.clone())
                .collect::<Vec<_>>(),
        )?;
        return Ok(InsertOutcome {
            union,
            split: None,
            root_split: None,
            write_lsn: latest_child_lsn,
        });
    }

    let outcome = write_or_split_page(
        pool, client_id, xid, rel, desc, state, block, opaque, items, is_root, mode,
    )?;
    for child_block in child_split_blocks {
        clear_follow_right_with_mode(
            pool,
            client_id,
            xid,
            rel,
            child_block,
            outcome.write_lsn,
            mode,
        )?;
    }
    Ok(outcome)
}
