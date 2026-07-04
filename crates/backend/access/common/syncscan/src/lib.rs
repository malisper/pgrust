#![allow(non_snake_case)]

use std::sync::OnceLock;

use init_small::globals as g;
use lwlock::{LWLockAcquire, LWLockConditionalAcquire, LWLockRelease, LW_EXCLUSIVE};
use types_core::{BlockNumber, InvalidBlockNumber, BLCKSZ};
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::{RelFileLocator, RelFileLocatorEquals, SyncCell};

#[cfg(test)]
mod tests;

const SYNC_SCAN_NELEM: usize = 20;
const _: () = assert!(SYNC_SCAN_NELEM > 1);
const SYNC_SCAN_REPORT_INTERVAL: BlockNumber = (128 * 1024 / BLCKSZ) as BlockNumber;

// SyncScanLock offset in lwlocklist.h order (name pinned by test).
const SYNC_SCAN_LOCK_OFFSET: usize = 24;

const NONE: u8 = u8::MAX;
const _: () = assert!(SYNC_SCAN_NELEM < NONE as usize);

// C's ss_lru_item_t links by pointer; u8 indices into `items`, same layout
// discipline as the buffer table (C layout, our locking).
#[derive(Clone, Copy)]
struct SsLruItem {
    prev: u8,
    next: u8,
    relfilelocator: RelFileLocator,
    location: BlockNumber,
}

#[derive(Clone, Copy)]
struct ScanLocations {
    head: u8,
    tail: u8,
    items: [SsLruItem; SYNC_SCAN_NELEM],
}

// Whole struct guarded by SyncScanLock.
static SCAN_LOCATIONS: OnceLock<SyncCell<ScanLocations>> = OnceLock::new();

fn sync_scan_lock() -> &'static lwlock::LWLock {
    lwlock::main_lock(SYNC_SCAN_LOCK_OFFSET)
}

fn boot_image() -> ScanLocations {
    let mut items = [SsLruItem {
        prev: NONE,
        next: NONE,
        relfilelocator: RelFileLocator::new(0, 0, 0),
        location: InvalidBlockNumber,
    }; SYNC_SCAN_NELEM];
    for (i, item) in items.iter_mut().enumerate() {
        item.prev = if i > 0 { (i - 1) as u8 } else { NONE };
        item.next = if i < SYNC_SCAN_NELEM - 1 { (i + 1) as u8 } else { NONE };
    }
    ScanLocations {
        head: 0,
        tail: (SYNC_SCAN_NELEM - 1) as u8,
        items,
    }
}

pub fn SyncScanShmemSize() -> usize {
    core::mem::size_of::<ScanLocations>()
}

pub fn SyncScanShmemInit() {
    const {
        assert!(!core::mem::needs_drop::<ScanLocations>());
    }
    SCAN_LOCATIONS
        .set(SyncCell::new(boot_image()))
        .unwrap_or_else(|_| panic!("SyncScanShmemInit called twice"));
}

/// Crash-cycle reset to the boot image; postmaster thread, children dead
/// (notes/crash-restart-design.md). SyncScanLock resets in LWLockResetAfterCrash.
pub fn SyncScanShmemResetAfterCrash() {
    let cell = SCAN_LOCATIONS
        .get()
        .expect("SyncScanShmemResetAfterCrash before SyncScanShmemInit");
    // SAFETY: postmaster-only crash window; no concurrent access.
    unsafe { *cell.ptr() = boot_image() };
}

/// Caller holds SyncScanLock exclusively.
fn ss_search(
    sl: &mut ScanLocations,
    relfilelocator: RelFileLocator,
    location: BlockNumber,
    set: bool,
) -> BlockNumber {
    let mut idx = sl.head;
    loop {
        let item = &mut sl.items[idx as usize];
        let matched = RelFileLocatorEquals(&item.relfilelocator, &relfilelocator);

        if matched || item.next == NONE {
            if !matched {
                item.relfilelocator = relfilelocator;
                item.location = location;
            } else if set {
                item.location = location;
            }
            let result = item.location;

            if idx != sl.head {
                let (prev, next) = (item.prev, item.next);
                if idx == sl.tail {
                    sl.tail = prev;
                }
                sl.items[prev as usize].next = next;
                if next != NONE {
                    sl.items[next as usize].prev = prev;
                }

                sl.items[idx as usize].prev = NONE;
                sl.items[idx as usize].next = sl.head;
                sl.items[sl.head as usize].prev = idx;
                sl.head = idx;
            }

            return result;
        }

        idx = item.next;
    }
}

fn get_location(locator: RelFileLocator, relnblocks: BlockNumber) -> PgResult<BlockNumber> {
    let cell = SCAN_LOCATIONS.get().expect("syncscan shmem not initialized");
    LWLockAcquire(sync_scan_lock(), LW_EXCLUSIVE, g::MyProcNumber())?;
    // SAFETY: SyncScanLock held exclusively.
    let startloc = ss_search(unsafe { &mut *cell.ptr() }, locator, 0, false);
    LWLockRelease(sync_scan_lock())?;

    // Stale hint (VACUUM truncation) or a never-reported InvalidBlockNumber.
    if startloc >= relnblocks {
        return Ok(0);
    }
    Ok(startloc)
}

fn report_location(locator: RelFileLocator, location: BlockNumber) -> PgResult<()> {
    if location % SYNC_SCAN_REPORT_INTERVAL == 0 {
        // Contended hint updates are skippable; never block a running scan.
        if LWLockConditionalAcquire(sync_scan_lock(), LW_EXCLUSIVE)? {
            let cell = SCAN_LOCATIONS.get().expect("syncscan shmem not initialized");
            // SAFETY: SyncScanLock held exclusively.
            ss_search(unsafe { &mut *cell.ptr() }, locator, location, true);
            LWLockRelease(sync_scan_lock())?;
        }
    }
    Ok(())
}

pub fn ss_get_location(rel: &RelationData<'_>, relnblocks: BlockNumber) -> PgResult<BlockNumber> {
    get_location(rel.rd_locator.get(), relnblocks)
}

pub fn ss_report_location(rel: &RelationData<'_>, location: BlockNumber) -> PgResult<()> {
    report_location(rel.rd_locator.get(), location)
}

pub fn init_seams() {
    syncscan_seams::ss_get_location::set(ss_get_location);
    syncscan_seams::ss_report_location::set(ss_report_location);
}
