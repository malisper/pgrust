use types_core::XLogRecPtr;
use types_error::PgResult;

use crate::{dl_delete, dl_iter, ChangeId, ReorderBuffer, TxnId, INVALID_ID};

pub(crate) struct IterEntry {
    pub(crate) lsn: XLogRecPtr,
    pub(crate) change: ChangeId,
    pub(crate) txn: TxnId,
    // Spilled-transaction restore position (C TXNEntryFile + segno); the file
    // handle closes on drop where C's IterTXNFinish calls FileClose.
    pub(crate) file: Option<std::fs::File>,
    pub(crate) segno: u64,
}

pub(crate) struct IterState {
    pub(crate) entries: Vec<IterEntry>,
    heap: Vec<i32>,
    built: bool,
    // C's state->old_change: after a disk restore replaced the in-memory
    // batch, the just-returned change stays alive here until the next *Next
    // call (or Finish) frees it.
    pub(crate) old_change: Option<ChangeId>,
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
    // ReorderBufferIterTXNInit (reorderbuffer.c:1282). Writes through
    // `iter_state` before the fallible restore steps so the caller's error
    // arm can always hand a live state to iter_txn_finish (C's volatile
    // *iter_state contract).
    pub(crate) fn iter_txn_init(
        &mut self,
        txn: TxnId,
        iter_state: &mut Option<IterState>,
    ) -> PgResult<()> {
        *iter_state = None;
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

        *iter_state = Some(IterState {
            entries: Vec::with_capacity(nr_txns),
            heap: Vec::with_capacity(nr_txns),
            built: false,
            old_change: None,
        });

        for &cur in std::iter::once(&txn).chain(subtxns.iter()) {
            if self.txn(cur).nentries == 0 {
                continue;
            }
            let mut file: Option<std::fs::File> = None;
            let mut segno: u64 = 0;
            if self.txn(cur).is_serialized() {
                // Serialize the remaining in-memory changes, then read the
                // first batch back so the list head below is the lowest LSN.
                self.serialize_txn(cur)?;
                self.restore_changes(cur, &mut file, &mut segno)?;
            }
            let head = self.txn(cur).changes.head;
            debug_assert!(head != INVALID_ID);

            let state = iter_state.as_mut().expect("state installed above");
            state.entries.push(IterEntry {
                lsn: self.change(head).lsn,
                change: head,
                txn: cur,
                file,
                segno,
            });
            let off = (state.entries.len() - 1) as i32;
            state.add_unordered(off);
        }

        iter_state.as_mut().expect("state installed above").build();
        Ok(())
    }

    // ReorderBufferIterTXNNext (reorderbuffer.c:1407).
    pub(crate) fn iter_txn_next(&mut self, state: &mut IterState) -> PgResult<Option<ChangeId>> {
        if state.is_empty() {
            return Ok(None);
        }

        let off = state.first();

        // Free memory we might have "leaked" in the previous *Next call.
        if let Some(old) = state.old_change.take() {
            self.free_change(old, true);
        }

        let entry_txn = state.entries[off as usize].txn;
        let change = state.entries[off as usize].change;

        // There are more in-memory changes: advance within the batch.
        let next = self.change(change).node.next;
        if next != INVALID_ID {
            state.entries[off as usize].lsn = self.change(next).lsn;
            state.entries[off as usize].change = next;
            state.replace_first(off);
            return Ok(Some(change));
        }

        // Try to load more changes from disk.
        if self.txn(entry_txn).nentries != self.txn(entry_txn).nentries_mem {
            // Restoring reuses the txn's change list: unlink the change being
            // returned and free it only on the next call.
            let mut list = self.txn(entry_txn).changes;
            dl_delete(&mut self.changes, &mut list, change, |c| &mut c.node);
            self.txn_mut(entry_txn).changes = list;
            state.old_change = Some(change);

            // The batch being released counts toward the txn's processed
            // bytes (reorderbuffer.c:1463).
            self.totalBytes += self.txn(entry_txn).size as i64;

            let entry = &mut state.entries[off as usize];
            let mut file = entry.file.take();
            let mut segno = entry.segno;
            let restored = self.restore_changes(entry_txn, &mut file, &mut segno);
            let entry = &mut state.entries[off as usize];
            entry.file = file;
            entry.segno = segno;
            if restored? > 0 {
                // Successfully restored changes from disk.
                let next_head = self.txn(entry_txn).changes.head;
                debug_assert!(next_head != INVALID_ID);
                debug_assert!(self.txn(entry_txn).nentries_mem > 0);
                state.entries[off as usize].lsn = self.change(next_head).lsn;
                state.entries[off as usize].change = next_head;
                state.replace_first(off);
                return Ok(Some(change));
            }
        }

        // No changes there anymore, remove.
        state.remove_first();
        Ok(Some(change))
    }

    // ReorderBufferIterTXNFinish (reorderbuffer.c:1500): free the pending
    // old_change; the per-entry files close on drop.
    pub(crate) fn iter_txn_finish(&mut self, mut state: IterState) {
        if let Some(old) = state.old_change.take() {
            self.free_change(old, true);
        }
        drop(state);
    }

    // C unlinks a consumed change with a list-agnostic dlist_delete; here the
    // change may live either on its owning txn's list or in the iterator's
    // pending-free stash after a restore swapped the batch out.
    pub(crate) fn iter_extract_change(&mut self, state: &mut IterState, cid: ChangeId) {
        if state.old_change == Some(cid) {
            state.old_change = None;
            return;
        }
        let owner = self.change(cid).txn;
        let mut list = self.txn(owner).changes;
        dl_delete(&mut self.changes, &mut list, cid, |c| &mut c.node);
        self.txn_mut(owner).changes = list;
    }

    #[cfg(test)]
    pub(crate) fn iter_collect_lsns(&mut self, txn: TxnId) -> Vec<XLogRecPtr> {
        let mut state: Option<IterState> = None;
        self.iter_txn_init(txn, &mut state).expect("iter init");
        let mut state = state.expect("installed");
        let mut lsns = Vec::new();
        while let Some(cid) = self.iter_txn_next(&mut state).expect("iter next") {
            lsns.push(self.change(cid).lsn);
        }
        self.iter_txn_finish(state);
        lsns
    }
}
