use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use pgrust_core::ItemPointerData;
use pgrust_nodes::{SystemVarBinding, Value};

#[derive(Debug, Default)]
pub struct ParallelRuntime {
    seq_scans: parking_lot::Mutex<HashMap<usize, Arc<ParallelSeqScanState>>>,
}

#[derive(Debug, Default)]
struct ParallelSeqScanState {
    next_block: AtomicU32,
}

impl ParallelRuntime {
    pub fn next_seq_scan_block(&self, source_id: usize) -> u32 {
        let state = {
            let mut seq_scans = self.seq_scans.lock();
            seq_scans
                .entry(source_id)
                .or_insert_with(|| Arc::new(ParallelSeqScanState::default()))
                .clone()
        };
        state.next_block.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct WorkerTuple {
    pub values: Vec<Value>,
    pub system_bindings: Vec<SystemVarBinding>,
    pub grouping_refs: Vec<usize>,
    pub tid: Option<ItemPointerData>,
    pub table_oid: Option<u32>,
}
