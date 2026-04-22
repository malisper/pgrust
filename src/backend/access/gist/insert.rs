use crate::backend::access::transam::xlog::{
    XLOG_GIST_INSERT, XLOG_GIST_PAGE_UPDATE, XLOG_GIST_SPLIT,
};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::page::bufpage::PageError;
use crate::include::access::amapi::IndexInsertContext;
use crate::include::access::gist::{
    F_FOLLOW_RIGHT, F_LEAF, GIST_INVALID_BLOCKNO, GIST_ROOT_BLKNO, GistPageError,
    gist_downlink_block, gist_page_get_opaque, gist_page_items, gist_page_replace_items,
};
use crate::include::access::itup::IndexTupleData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;
use std::sync::OnceLock;

use super::page::{
    GistLoggedPage, allocate_new_block, clear_follow_right, ensure_empty_gist, init_opaque,
    read_buffered_page, relation_nblocks, write_buffered_page, write_logged_pages,
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
struct GistTupleEntry {
    tuple: IndexTupleData,
    values: Vec<Value>,
}

#[derive(Debug, Clone)]
struct ChildSplit {
    right_block: u32,
    left_union: Vec<Value>,
    right_union: Vec<Value>,
}

#[derive(Debug, Clone)]
struct InsertOutcome {
    union: Vec<Value>,
    split: Option<ChildSplit>,
    write_lsn: u64,
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
    let _ = insert_into_block(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        &ctx.index_desc,
        &state,
        GIST_ROOT_BLKNO,
        new_entry,
        true,
    )?;
    Ok(true)
}

fn load_page_entries(
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

fn choose_child(
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

fn try_write_page(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    block: u32,
    opaque: crate::include::access::gist::GistPageOpaqueData,
    items: &[GistTupleEntry],
    wal_info: u8,
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
    write_buffered_page(pool, client_id, xid, rel, block, &rebuilt, wal_info).map_err(Into::into)
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

fn write_or_split_page(
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
            let (left_items, right_items) = split_page_entries(&items, &split)?;
            let inherited_nsn = opaque.nsn;
            if is_root {
                let left_block = allocate_new_block(pool, rel)?;
                let right_block = allocate_new_block(pool, rel)?;
                let child_flags = if opaque.is_leaf() { F_LEAF } else { 0 };
                let mut left_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                let mut right_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                let mut root_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
                let root_items = vec![
                    GistTupleEntry {
                        tuple: make_downlink_tuple(desc, &split.left_union, left_block)?,
                        values: split.left_union.clone(),
                    },
                    GistTupleEntry {
                        tuple: make_downlink_tuple(desc, &split.right_union, right_block)?,
                        values: split.right_union.clone(),
                    },
                ];
                let root_union =
                    state.union_all(&[split.left_union.clone(), split.right_union.clone()])?;
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
                let write_lsn = write_logged_pages(
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
                )?;
                Ok(InsertOutcome {
                    union: root_union,
                    split: None,
                    write_lsn,
                })
            } else {
                let right_block = allocate_new_block(pool, rel)?;
                let child_flags = if opaque.is_leaf() { F_LEAF } else { 0 };
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
                let write_lsn = write_logged_pages(
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
                )?;
                Ok(InsertOutcome {
                    union: split.left_union.clone(),
                    split: Some(ChildSplit {
                        right_block,
                        left_union: split.left_union,
                        right_union: split.right_union,
                    }),
                    write_lsn,
                })
            }
        }
        Err(GistWriteError::Catalog(err)) => Err(err),
    }
}

fn insert_into_block(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    xid: u32,
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    state: &GistState,
    block: u32,
    new_entry: GistTupleEntry,
    is_root: bool,
) -> Result<InsertOutcome, CatalogError> {
    let page = read_buffered_page(pool, client_id, rel, block)?;
    let opaque = gist_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("gist page parse failed: {err:?}")))?;
    let mut items = load_page_entries(desc, &page)?;

    if opaque.is_leaf() {
        items.push(new_entry);
        return write_or_split_page(
            pool, client_id, xid, rel, desc, state, block, opaque, items, is_root,
        );
    }

    let child_index = choose_child(desc, state, &items, &new_entry.values)?;
    let child_block = gist_downlink_block(&items[child_index].tuple).ok_or(
        CatalogError::Corrupt("gist internal tuple missing child block"),
    )?;
    let child_outcome = insert_into_block(
        pool,
        client_id,
        xid,
        rel,
        desc,
        state,
        child_block,
        new_entry,
        false,
    )?;

    if child_outcome.split.is_none()
        && state.same_values(&items[child_index].values, &child_outcome.union)?
    {
        let union = state.union_all(
            &items
                .iter()
                .map(|item| item.values.clone())
                .collect::<Vec<_>>(),
        )?;
        return Ok(InsertOutcome {
            union,
            split: None,
            write_lsn: child_outcome.write_lsn,
        });
    }

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
    }

    let outcome = write_or_split_page(
        pool, client_id, xid, rel, desc, state, block, opaque, items, is_root,
    )?;
    if let Some(_split) = child_outcome.split {
        clear_follow_right(pool, client_id, xid, rel, child_block, outcome.write_lsn)?;
    }
    Ok(outcome)
}
