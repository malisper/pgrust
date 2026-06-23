//! `backend/access/common/syncscan.c` — scan synchronization support.
//!
//! Keeps concurrent sequential scans on the same table close together to
//! reduce overall I/O by tracking each table's last-reported scan location in a
//! small fixed-size LRU and starting new scans near the others.
//!
//! ## Shared-memory / lock model
//!
//! In C the LRU (`ss_scan_locations_t`: `head`/`tail`/`items[N]` with raw
//! `prev`/`next` pointers) lives in shared memory, placed by `ShmemInitStruct`
//! in `SyncScanShmemInit` and guarded by the built-in `SyncScanLock` LWLock.
//!
//! Following the established repo pattern for shmem-resident subsystem state
//! (cf. `backend-storage-ipc-pmsignal`), the LRU is a process-global
//! [`OnceLock`] of the structure, guarded by a [`Mutex`] standing in for
//! `SyncScanLock` (the conditional-acquire fast path becomes `try_lock`). The
//! intra-LRU pointers are modelled as `Option<usize>` index links (`None ==
//! NULL`), behaviorally identical but free of raw shmem pointers. The C
//! `ShmemInitStruct` byte-size handshake has no analogue here; `sync_scan_shmem_size`
//! still reports the exact C ABI footprint for the size accumulator in `ipci.c`.
//!
//! ## Identity-key divergence (documented in DESIGN_DEBT.md)
//!
//! C keys the LRU on `rel->rd_locator` (a `RelFileLocator`). The installed
//! `ss_get_location`/`ss_report_location` seams (consumed by `tableam`) carry
//! only the relation `Oid` (`rel->rd_id`), which is the identity available at
//! that seam boundary. The LRU is therefore keyed on the relation `Oid` here.
//! The structure is a best-effort synchronization *hint* (a stale or imprecise
//! key only costs a slightly-off start page), so the behaviour is equivalent;
//! the divergence is purely in the key type, fixed by the seam contract.

use core::mem::size_of;
use std::sync::{Mutex, OnceLock};

use types_core::primitive::{BlockNumber, InvalidBlockNumber, InvalidOid, Oid};
use types_core::Size;
use types_core::primitive::BLCKSZ;
use types_error::PgResult;

/// `SYNC_SCAN_NELEM` — size of the LRU list (syncscan.c). The code assumes
/// `SYNC_SCAN_NELEM > 1`.
pub const SYNC_SCAN_NELEM: usize = 20;

/// `SYNC_SCAN_REPORT_INTERVAL` — interval between reports of the current scan
/// location, in pages (`128 * 1024 / BLCKSZ`).
pub const SYNC_SCAN_REPORT_INTERVAL: u32 = (128 * 1024 / BLCKSZ) as u32;

/// `ss_scan_location_t` (syncscan.c) — identity of a relation plus its
/// last-reported location. The identity is the relation `Oid` (see the
/// identity-key divergence note above; C uses `RelFileLocator`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ss_scan_location_t {
    /// `RelFileLocator relfilelocator` in C; the relation `Oid` here.
    pub(crate) relid: Oid,
    /// `BlockNumber location` — last-reported location in the relation.
    pub(crate) location: BlockNumber,
}

/// `ss_lru_item_t` (syncscan.c) — a doubly-linked LRU node. The `prev`/`next`
/// raw pointers become `Option<usize>` index links (`None == NULL`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ss_lru_item_t {
    pub(crate) prev: Option<usize>,
    pub(crate) next: Option<usize>,
    pub(crate) location: ss_scan_location_t,
}

/// `ss_scan_locations_t` (syncscan.c) — head/tail plus the fixed item array.
pub(crate) struct ss_scan_locations_t {
    pub(crate) head: usize,
    pub(crate) tail: usize,
    pub(crate) items: [ss_lru_item_t; SYNC_SCAN_NELEM],
}

impl ss_scan_locations_t {
    /// Initialize all slots with invalid values and link them into a single
    /// LRU list (head = item 0, tail = item N-1). Mirrors the
    /// `!IsUnderPostmaster` branch of `SyncScanShmemInit`.
    fn new() -> Self {
        let mut items = [ss_lru_item_t {
            prev: None,
            next: None,
            location: ss_scan_location_t {
                relid: InvalidOid,
                location: InvalidBlockNumber,
            },
        }; SYNC_SCAN_NELEM];

        for i in 0..SYNC_SCAN_NELEM {
            items[i].location.relid = InvalidOid;
            items[i].location.location = InvalidBlockNumber;
            items[i].prev = if i > 0 { Some(i - 1) } else { None };
            items[i].next = if i < SYNC_SCAN_NELEM - 1 {
                Some(i + 1)
            } else {
                None
            };
        }

        ss_scan_locations_t {
            head: 0,
            tail: SYNC_SCAN_NELEM - 1,
            items,
        }
    }
}

/// The shmem-resident LRU, modelled as a process-global guarded by the
/// `SyncScanLock` stand-in [`Mutex`]. Built by [`sync_scan_shmem_init`].
static SCAN_LOCATIONS: OnceLock<Mutex<ss_scan_locations_t>> = OnceLock::new();

// On-shmem C ABI sizes, used only by `sync_scan_shmem_size` so the `ipci.c`
// size accumulator reserves the same number of bytes the C subsystem would.
//
// `RelFileLocator` = spcOid, dbOid, relNumber (three uint32) = 12.
const C_RELFILELOCATOR_SIZE: usize = 3 * size_of::<u32>();
// `ss_scan_location_t` = RelFileLocator + BlockNumber = 12 + 4 = 16.
const C_SCAN_LOCATION_SIZE: usize = C_RELFILELOCATOR_SIZE + size_of::<u32>();
// `ss_lru_item_t` = prev, next pointers + ss_scan_location_t (64-bit: 8+8+16=32).
const C_LRU_ITEM_SIZE: usize = 2 * size_of::<usize>() + C_SCAN_LOCATION_SIZE;
// `offsetof(ss_scan_locations_t, items)` = two leading pointers (head, tail).
const C_SCAN_LOCATIONS_HEADER: usize = 2 * size_of::<usize>();

/// `SizeOfScanLocations(N)` (syncscan.c) — `offsetof(items) + N *
/// sizeof(ss_lru_item_t)`, reported as the equivalent shmem footprint of the
/// pointer-based C struct.
const fn size_of_scan_locations(n: usize) -> Size {
    C_SCAN_LOCATIONS_HEADER + n * C_LRU_ITEM_SIZE
}

/// `SyncScanShmemSize()` (syncscan.c) — report amount of shared memory space
/// needed. Infallible here (no `add_size`/`mul_size` overflow on a `const`).
pub fn sync_scan_shmem_size() -> PgResult<Size> {
    Ok(size_of_scan_locations(SYNC_SCAN_NELEM))
}

/// `SyncScanShmemInit()` (syncscan.c) — initialize this module's shared memory.
/// The C `ShmemInitStruct` + `IsUnderPostmaster` branch reduces to building the
/// process-global LRU once (idempotent via `OnceLock`).
pub fn sync_scan_shmem_init() -> PgResult<()> {
    SCAN_LOCATIONS.get_or_init(|| Mutex::new(ss_scan_locations_t::new()));
    Ok(())
}

/// Resolve the LRU, initializing it on first touch if `sync_scan_shmem_init`
/// has not run (a backend can call `ss_get_location` before the explicit init
/// in some test paths; C would have placed the struct via shmem already).
fn locations() -> &'static Mutex<ss_scan_locations_t> {
    SCAN_LOCATIONS.get_or_init(|| Mutex::new(ss_scan_locations_t::new()))
}

/// `static BlockNumber ss_search(RelFileLocator, BlockNumber location, bool
/// set)` (syncscan.c) — search the LRU for an entry with the given `relid`. If
/// `set`, update its location. If none is found, the last entry is taken over.
/// In any case the entry is moved to the front of the LRU and its (possibly
/// updated) location is returned. Caller holds the lock.
fn ss_search(
    locs: &mut ss_scan_locations_t,
    relid: Oid,
    location: BlockNumber,
    set: bool,
) -> BlockNumber {
    let mut idx = locs.head;
    loop {
        let item = locs.items[idx];
        let r#match = item.location.relid == relid;

        if r#match || item.next.is_none() {
            // If we reached the end of list and no match was found, take over
            // the last entry.
            if !r#match {
                locs.items[idx].location.relid = relid;
                locs.items[idx].location.location = location;
            } else if set {
                locs.items[idx].location.location = location;
            }

            // Move the entry to the front of the LRU list.
            if idx != locs.head {
                // unlink
                if idx == locs.tail {
                    locs.tail = locs.items[idx].prev.expect("tail has prev");
                }
                let prev = locs.items[idx].prev;
                let next = locs.items[idx].next;
                if let Some(p) = prev {
                    locs.items[p].next = next;
                }
                if let Some(n) = next {
                    locs.items[n].prev = prev;
                }

                // link
                let old_head = locs.head;
                locs.items[idx].prev = None;
                locs.items[idx].next = Some(old_head);
                locs.items[old_head].prev = Some(idx);
                locs.head = idx;
            }

            return locs.items[idx].location.location;
        }

        idx = item.next.expect("non-tail item has next");
    }
}

/// `ss_get_location(Relation rel, BlockNumber relnblocks)` (syncscan.c) — the
/// optimal starting location for a scan: the last-reported location, or 0 if no
/// valid location is found (also 0 if the saved location is no longer a valid
/// block number, e.g. after a VACUUM truncation). The relation identity is the
/// `Oid` (`rel->rd_id`); see the identity-key divergence note.
pub fn ss_get_location(relid: Oid, relnblocks: BlockNumber) -> PgResult<BlockNumber> {
    let mut startloc = {
        // LWLockAcquire(SyncScanLock, LW_EXCLUSIVE) ... LWLockRelease.
        let mut locs = locations().lock().expect("SyncScanLock poisoned");
        ss_search(&mut locs, relid, 0, false)
    };

    // If the location is not a valid block number for this scan, start at 0.
    if startloc >= relnblocks {
        startloc = 0;
    }

    Ok(startloc)
}

/// `ss_report_location(Relation rel, BlockNumber location)` (syncscan.c) —
/// write `(relid, location)` into the shared Sync Scan state, throttled to
/// every `SYNC_SCAN_REPORT_INTERVAL` pages and skipping the update if the lock
/// isn't immediately available (the C `LWLockConditionalAcquire`).
pub fn ss_report_location(relid: Oid, location: BlockNumber) -> PgResult<()> {
    if location % SYNC_SCAN_REPORT_INTERVAL == 0 {
        // LWLockConditionalAcquire(SyncScanLock, LW_EXCLUSIVE): don't block.
        if let Ok(mut locs) = locations().try_lock() {
            let _ = ss_search(&mut locs, relid, location, true);
        }
    }
    Ok(())
}

/// Test-only handles into the otherwise-private LRU internals, so the
/// `syncscan_tests` module can drive `ss_search` over a fresh structure without
/// touching the process-global.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) type Locations = super::ss_scan_locations_t;

    pub(crate) fn fresh_locations() -> Locations {
        ss_scan_locations_t::new()
    }

    pub(crate) fn search(
        locs: &mut Locations,
        relid: Oid,
        location: BlockNumber,
        set: bool,
    ) -> BlockNumber {
        ss_search(locs, relid, location, set)
    }
}
