//! Parallel index scan shared descriptors, thread-native: C's shm_toc blob
//! becomes typed Arc-shared state (std collections: cross-thread by design).

use std::sync::{Condvar, Mutex};

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

pub struct BTParallelScanShared {
    pub state: Mutex<BtParallelScanState>,
    pub cv: Condvar,
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
            cv: Condvar::new(),
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
