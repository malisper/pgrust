use types_core::XLogRecPtr;

use crate::{dl_iter, ChangeId, ReorderBuffer, TxnId, INVALID_ID};

pub(crate) struct IterEntry {
    pub(crate) lsn: XLogRecPtr,
    pub(crate) change: ChangeId,
    pub(crate) txn: TxnId,
}

pub(crate) struct IterState {
    pub(crate) entries: Vec<IterEntry>,
    heap: Vec<i32>,
    built: bool,
}

// binaryheap.c subset; ReorderBufferIterCompare puts the smallest LSN on top.
impl IterState {
    fn compare(&self, a: i32, b: i32) -> i32 {
        let pos_a = self.entries[a as usize].lsn;
        let pos_b = self.entries[b as usize].lsn;
        if pos_a < pos_b {
            1
        } else if pos_a == pos_b {
            0
        } else {
            -1
        }
    }

    fn add_unordered(&mut self, d: i32) {
        self.heap.push(d);
        self.built = false;
    }

    fn build(&mut self) {
        let n = self.heap.len() as i32;
        for i in (0..n / 2).rev() {
            self.sift_down(i);
        }
        self.built = true;
    }

    fn first(&self) -> i32 {
        debug_assert!(self.built && !self.heap.is_empty());
        self.heap[0]
    }

    fn replace_first(&mut self, d: i32) {
        debug_assert!(self.built && !self.heap.is_empty());
        self.heap[0] = d;
        if self.heap.len() > 1 {
            self.sift_down(0);
        }
    }

    fn remove_first(&mut self) -> i32 {
        debug_assert!(self.built && !self.heap.is_empty());
        let result = self.heap[0];
        if self.heap.len() == 1 {
            self.heap.pop();
            return result;
        }
        self.heap[0] = self.heap.pop().expect("non-empty");
        self.sift_down(0);
        result
    }

    fn sift_down(&mut self, mut node_off: i32) {
        let node_val = self.heap[node_off as usize];
        let size = self.heap.len() as i32;
        loop {
            let left_off = 2 * node_off + 1;
            let right_off = 2 * node_off + 2;
            let mut swap_off = 0;

            if left_off < size && self.compare(self.heap[left_off as usize], node_val) > 0 {
                swap_off = left_off;
            }
            if right_off < size {
                let against = if swap_off != 0 {
                    self.heap[swap_off as usize]
                } else {
                    node_val
                };
                if self.compare(self.heap[right_off as usize], against) > 0 {
                    swap_off = right_off;
                }
            }
            if swap_off == 0 {
                break;
            }
            self.heap[node_off as usize] = self.heap[swap_off as usize];
            node_off = swap_off;
        }
        self.heap[node_off as usize] = node_val;
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl ReorderBuffer {
    pub(crate) fn iter_txn_init(&mut self, txn: TxnId) -> IterState {
        self.assert_change_lsn_order(txn);

        let mut nr_txns = 0usize;
        if self.txn(txn).nentries > 0 {
            nr_txns += 1;
        }
        let subtxns: Vec<TxnId> = dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node).collect();
        for &sub in &subtxns {
            self.assert_change_lsn_order(sub);
            if self.txn(sub).nentries > 0 {
                nr_txns += 1;
            }
        }

        let mut state = IterState {
            entries: Vec::with_capacity(nr_txns),
            heap: Vec::with_capacity(nr_txns),
            built: false,
        };

        for &cur in std::iter::once(&txn).chain(subtxns.iter()) {
            if self.txn(cur).nentries == 0 {
                continue;
            }
            debug_assert!(
                !self.txn(cur).is_serialized(),
                "unported callee reached from reorderbuffer.c: ReorderBufferRestoreChanges (spill-to-disk): phase-2",
            );
            let head = self.txn(cur).changes.head;
            debug_assert!(head != INVALID_ID);
            state.entries.push(IterEntry {
                lsn: self.change(head).lsn,
                change: head,
                txn: cur,
            });
            let off = (state.entries.len() - 1) as i32;
            state.add_unordered(off);
        }

        state.build();
        state
    }

    pub(crate) fn iter_txn_next(&mut self, state: &mut IterState) -> Option<ChangeId> {
        if state.is_empty() {
            return None;
        }

        let off = state.first();
        let entry_txn = state.entries[off as usize].txn;
        let change = state.entries[off as usize].change;

        let next = self.change(change).node.next;
        if next != INVALID_ID {
            state.entries[off as usize].lsn = self.change(next).lsn;
            state.entries[off as usize].change = next;
            state.replace_first(off);
            return Some(change);
        }

        debug_assert_eq!(
            self.txn(entry_txn).nentries,
            self.txn(entry_txn).nentries_mem,
            "spilled changes present; ReorderBufferRestoreChanges (spill-to-disk): phase-2",
        );

        state.remove_first();
        Some(change)
    }

    pub(crate) fn iter_txn_finish(&mut self, state: IterState) {
        drop(state);
    }

    #[cfg(test)]
    pub(crate) fn iter_collect_lsns(&mut self, txn: TxnId) -> Vec<XLogRecPtr> {
        let mut state = self.iter_txn_init(txn);
        let mut lsns = Vec::new();
        while let Some(cid) = self.iter_txn_next(&mut state) {
            lsns.push(self.change(cid).lsn);
        }
        self.iter_txn_finish(state);
        lsns
    }

}
