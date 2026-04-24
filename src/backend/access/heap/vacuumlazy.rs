use std::collections::BTreeSet;

use parking_lot::RwLock;

use crate::backend::access::heap::pruneheap::{classify_page_for_prune, freeze_page_tuples};
use crate::backend::access::transam::xact::{
    FROZEN_TRANSACTION_ID, INVALID_TRANSACTION_ID, TransactionId, TransactionManager,
};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::page::bufpage::{
    page_clear_all_visible, page_get_max_offset_number, page_is_all_visible, page_remove_item,
    page_set_all_visible,
};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::visibilitymapdefs::{
    VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
};
use crate::{BufferPool, ClientId};

use super::heapam::HeapError;
use super::visibilitymap::{
    VisibilityMapBuffer, visibilitymap_clear, visibilitymap_get_status, visibilitymap_pin,
    visibilitymap_set,
};

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
    txns: &RwLock<TransactionManager>,
) -> Result<VacuumScanState, HeapError> {
    let nblocks = pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))?;
    let txns = txns.read();
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
        if vm_bits & VISIBILITYMAP_ALL_VISIBLE != 0 {
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
        let result = classify_page_for_prune(&guard, &txns, oldest_xmin, freeze_cutoff_xid)?;
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
    txns: &RwLock<TransactionManager>,
    scan: &VacuumScanState,
    previous_relfrozenxid: Option<TransactionId>,
) -> Result<VacuumRelationStats, HeapError> {
    let txns = txns.read();
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

        let initial = classify_page_for_prune(&page, &txns, oldest_xmin, freeze_cutoff_xid)?;
        for offset in initial.removable_offsets.iter().rev().copied() {
            if offset <= page_get_max_offset_number(&page)? {
                page_remove_item(&mut page, offset)?;
            }
        }
        let _ = freeze_page_tuples(&mut page, &txns, oldest_xmin, freeze_cutoff_xid)?;

        let final_result = classify_page_for_prune(&page, &txns, oldest_xmin, freeze_cutoff_xid)?;
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

    Ok(VacuumRelationStats {
        relation_oid,
        relpages: scan.relpages,
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
    txns: &RwLock<TransactionManager>,
    previous_relfrozenxid: Option<TransactionId>,
) -> Result<(VacuumScanState, VacuumRelationStats), HeapError> {
    let scan = vacuum_relation_scan(pool, client_id, rel, txns)?;
    let stats = vacuum_relation_pages(
        pool,
        client_id,
        rel,
        relation_oid,
        txns,
        &scan,
        previous_relfrozenxid,
    )?;
    Ok((scan, stats))
}

#[allow(dead_code)]
fn _assert_vm_buffer_send(_buffer: VisibilityMapBuffer) {}
