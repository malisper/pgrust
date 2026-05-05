use std::collections::BTreeSet;

use super::visibilitymap::{
    VisibilityMapBuffer, visibilitymap_clear, visibilitymap_get_status, visibilitymap_pin,
    visibilitymap_prepare_truncate, visibilitymap_set,
};
use crate::AccessTransactionServices;
use crate::access::itemptr::ItemPointerData;
use crate::access::visibilitymapdefs::{VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE};
use crate::heap::heapam::HeapError;
use crate::heap::pruneheap::{classify_page_for_prune, freeze_page_tuples};
use pgrust_core::{ClientId, FROZEN_TRANSACTION_ID, INVALID_TRANSACTION_ID, TransactionId};
use pgrust_storage::page::bufpage::{
    ItemIdFlags, page_clear_all_visible, page_get_item_id, page_get_max_offset_number,
    page_is_all_visible, page_remove_item, page_set_all_visible,
};
use pgrust_storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use pgrust_storage::{BufferPool, SmgrStorageBackend};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumScanState {
    pub dead_tids: BTreeSet<ItemPointerData>,
    pub blocks_to_scan: Vec<u32>,
    removable_dead_tuples: i64,
    relpages: i32,
    skipped_all_visible: i32,
    skipped_all_frozen: i32,
    skipped_not_all_frozen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumRelationStats {
    pub relation_oid: u32,
    pub relpages: i32,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub relfrozenxid: TransactionId,
    pub removed_dead_tuples: i64,
    pub remaining_dead_tuples: i64,
}

pub fn vacuum_relation_scan(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &dyn AccessTransactionServices,
    disable_page_skipping: bool,
) -> Result<VacuumScanState, HeapError> {
    let nblocks = pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))?;
    let oldest_xmin = txns.oldest_active_xid();
    let freeze_cutoff_xid = oldest_xmin.max(FROZEN_TRANSACTION_ID);
    let mut dead_tids = BTreeSet::new();
    let mut blocks_to_scan = Vec::new();
    let mut removable_dead_tuples = 0i64;
    let mut vmbuf = None;
    let mut skipped_all_visible = 0;
    let mut skipped_all_frozen = 0;
    let mut skipped_not_all_frozen = false;

    for block in 0..nblocks {
        let vm_bits = visibilitymap_get_status(pool, client_id, rel, block, &mut vmbuf)?;
        if !disable_page_skipping && vm_bits & VISIBILITYMAP_ALL_VISIBLE != 0 {
            skipped_all_visible += 1;
            if vm_bits & VISIBILITYMAP_ALL_FROZEN != 0 {
                skipped_all_frozen += 1;
            } else {
                skipped_not_all_frozen = true;
            }
            continue;
        }

        let pin = pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)?;
        let guard = pool.lock_buffer_shared(pin.buffer_id())?;
        let result = classify_page_for_prune(&guard, txns, oldest_xmin, freeze_cutoff_xid)?;
        drop(guard);
        drop(pin);

        for offset in &result.removable_offsets {
            dead_tids.insert(ItemPointerData {
                block_number: block,
                offset_number: *offset,
            });
        }
        blocks_to_scan.push(block);
        removable_dead_tuples += result.removable_offsets.len() as i64;
    }

    Ok(VacuumScanState {
        dead_tids,
        blocks_to_scan,
        removable_dead_tuples,
        relpages: nblocks as i32,
        skipped_all_visible,
        skipped_all_frozen,
        skipped_not_all_frozen,
    })
}

pub fn vacuum_relation_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    relation_oid: u32,
    txns: &dyn AccessTransactionServices,
    scan: &VacuumScanState,
    previous_relfrozenxid: Option<TransactionId>,
    truncate: bool,
) -> Result<VacuumRelationStats, HeapError> {
    let oldest_xmin = txns.oldest_active_xid();
    let freeze_cutoff_xid = oldest_xmin.max(FROZEN_TRANSACTION_ID);
    let previous_relfrozenxid = previous_relfrozenxid.unwrap_or(FROZEN_TRANSACTION_ID);
    let mut relallvisible = scan.skipped_all_visible;
    let mut relallfrozen = scan.skipped_all_frozen;
    let mut relfrozenxid_candidate = None;
    let mut remaining_dead_tuples = 0i64;
    let mut vmbuf = None;

    for &block in &scan.blocks_to_scan {
        visibilitymap_pin(pool, rel, block, &mut vmbuf)?;
        let vm_bits = visibilitymap_get_status(pool, client_id, rel, block, &mut vmbuf)?;

        let pin = pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)?;
        let mut guard = pool.lock_buffer_exclusive(pin.buffer_id())?;
        let mut page = *guard;

        let removable_offsets = scan
            .dead_tids
            .iter()
            .filter_map(|tid| (tid.block_number == block).then_some(tid.offset_number))
            .collect::<Vec<_>>();
        for offset in removable_offsets.iter().rev().copied() {
            if offset <= page_get_max_offset_number(&page)? {
                page_remove_item(&mut page, offset)?;
            }
        }
        let _ = freeze_page_tuples(&mut page, txns, oldest_xmin, freeze_cutoff_xid)?;

        let final_result = classify_page_for_prune(&page, txns, oldest_xmin, freeze_cutoff_xid)?;
        if final_result.all_visible {
            page_set_all_visible(&mut page)?;
        } else if page_is_all_visible(&page)? {
            page_clear_all_visible(&mut page)?;
        }

        pool.write_page_image_locked(pin.buffer_id(), INVALID_TRANSACTION_ID, &page, &mut guard)?;
        drop(guard);
        drop(pin);

        if final_result.all_visible {
            let mut flags = VISIBILITYMAP_ALL_VISIBLE;
            if final_result.all_frozen {
                flags |= VISIBILITYMAP_ALL_FROZEN;
                relallfrozen += 1;
            }
            relallvisible += 1;
            let _ = visibilitymap_set(pool, client_id, rel, block, &vmbuf, flags)?;
        } else if vm_bits & (VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN) != 0 {
            let _ = visibilitymap_clear(
                pool,
                client_id,
                rel,
                block,
                &vmbuf,
                VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
            )?;
        }

        if let Some(candidate) = final_result.relfrozenxid_candidate {
            relfrozenxid_candidate = Some(
                relfrozenxid_candidate
                    .map(|current: TransactionId| current.min(candidate))
                    .unwrap_or(candidate),
            );
        }
        remaining_dead_tuples += final_result.nonremovable_dead_tuples as i64;
    }

    // :HACK: pgrust does not implement multixacts yet, so relfrozenxid is the
    // only freeze horizon we track here. Add relminmxid together with a
    // multixact subsystem instead of widening this shim further.
    let relfrozenxid = if scan.skipped_not_all_frozen {
        previous_relfrozenxid
    } else {
        relfrozenxid_candidate.unwrap_or(FROZEN_TRANSACTION_ID)
    };

    let relpages = if truncate {
        truncate_empty_tail(pool, client_id, rel, scan.relpages.max(0) as u32)? as i32
    } else {
        scan.relpages
    };
    relallvisible = relallvisible.min(relpages);
    relallfrozen = relallfrozen.min(relpages);

    Ok(VacuumRelationStats {
        relation_oid,
        relpages,
        relallvisible,
        relallfrozen,
        relfrozenxid,
        removed_dead_tuples: scan.removable_dead_tuples,
        remaining_dead_tuples,
    })
}

pub fn vacuum_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    relation_oid: u32,
    txns: &dyn AccessTransactionServices,
    previous_relfrozenxid: Option<TransactionId>,
) -> Result<(VacuumScanState, VacuumRelationStats), HeapError> {
    let scan = vacuum_relation_scan(pool, client_id, rel, txns, false)?;
    let stats = vacuum_relation_pages(
        pool,
        client_id,
        rel,
        relation_oid,
        txns,
        &scan,
        previous_relfrozenxid,
        true,
    )?;
    Ok((scan, stats))
}

fn truncate_empty_tail(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    nblocks: u32,
) -> Result<u32, HeapError> {
    let mut new_nblocks = nblocks;
    while new_nblocks > 0 {
        let block = new_nblocks - 1;
        let pin = pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)?;
        let guard = pool.lock_buffer_shared(pin.buffer_id())?;
        let empty = page_has_no_tuple_storage(&guard)?;
        drop(guard);
        drop(pin);
        if !empty {
            break;
        }
        new_nblocks -= 1;
    }

    if new_nblocks < nblocks {
        let visibility_map_blocks =
            visibilitymap_prepare_truncate(pool, client_id, rel, new_nblocks)?;
        pool.flush_relation(rel).map_err(HeapError::Buffer)?;
        pool.invalidate_relation(rel).map_err(HeapError::Buffer)?;
        pool.with_storage_mut(|storage| {
            storage.smgr.truncate(rel, ForkNumber::Main, new_nblocks)?;
            if let Some(nblocks) = visibility_map_blocks
                && storage.smgr.exists(rel, ForkNumber::VisibilityMap)
            {
                storage
                    .smgr
                    .truncate(rel, ForkNumber::VisibilityMap, nblocks)?;
            }
            Ok::<(), pgrust_storage::smgr::SmgrError>(())
        })?;
    }

    Ok(new_nblocks)
}

fn page_has_no_tuple_storage(page: &pgrust_storage::Page) -> Result<bool, HeapError> {
    let max_offset = page_get_max_offset_number(page)?;
    for off in 1..=max_offset {
        let item_id = page_get_item_id(page, off)?;
        if item_id.lp_flags != ItemIdFlags::Unused && item_id.has_storage() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(dead_code)]
fn _assert_vm_buffer_send(_buffer: VisibilityMapBuffer) {}
