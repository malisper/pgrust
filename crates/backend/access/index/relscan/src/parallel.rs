//! Parallel index scan shared descriptors, thread-native: C's shm_toc blob
//! becomes typed Arc-shared state (std collections: cross-thread by design).

use pgsync::{Mutex, ParkLot};

use ::datum::Datum;
use ::types_core::{BlockNumber, InvalidBlockNumber};
use ::types_storage::storage::RelFileLocator;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BtPsState {
    NotInitialized,
    NeedPrimscan,
    Advancing,
    Idle,
    Done,
}

pub enum BtParallelSkipArg {
    Byval(Datum),
    Byref(Vec<u8>),
}

pub enum BtParallelArrayElem {
    Saop { cur_elem: i32 },
    // arg None iff flags carry SK_BT_MINVAL/SK_BT_MAXVAL or SK_ISNULL.
    Skip { flags: i32, arg: Option<BtParallelSkipArg> },
}

pub struct BtParallelScanState {
    pub next_scan_page: BlockNumber,
    pub last_curr_page: BlockNumber,
    pub page_status: BtPsState,
    pub arr_elems: Vec<BtParallelArrayElem>,
}

/// permit-s4 row-2 conversion (dst-p3-scheduler §3): the seize wait rides
/// the [`ParkLot`] eventcount, not a Condvar. Protocol (the loom-modeled
/// shape — `relscan_seize_release_wake_never_lost`, runtime/tests/loom.rs):
/// a seizer that observes `Advancing` captures `lot.epoch()` UNDER the
/// state lock, releases it, and parks; EVERY transition out of `Advancing`
/// (release → Idle, done → Done) bumps-then-notifies via `lot.wake_all()`.
/// Wake-all is deliberate (design row 2 v1 forgoes the notify_one
/// optimization): the herd is DOP-bounded, and the Condvar notify_one trap
/// — a wake delivered to a waiter that unwound between queueing and
/// waiting — cannot exist when every waiter is released on every advance.
/// There is no registration list to clean on error unwind either.
pub struct BTParallelScanShared {
    pub state: Mutex<BtParallelScanState>,
    pub lot: ParkLot,
}

impl BTParallelScanShared {
    pub fn new() -> Self {
        BTParallelScanShared {
            state: Mutex::new(BtParallelScanState {
                next_scan_page: InvalidBlockNumber,
                last_curr_page: InvalidBlockNumber,
                page_status: BtPsState::NotInitialized,
                arr_elems: Vec::new(),
            }),
            lot: ParkLot::new(),
        }
    }
}

impl Default for BTParallelScanShared {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ParallelIndexAmShared {
    Btree(BTParallelScanShared),
}

// The serialized snapshot replaces ps_snapshot_data (always MVCC here).
pub struct ParallelIndexScanDescShared {
    pub ps_locator: RelFileLocator,
    pub ps_indexlocator: RelFileLocator,
    pub snapshot: ::snapmgr::SerializedSnapshot,
    pub am: ParallelIndexAmShared,
}
