//! Snapshot-management family of `replication/logical/reorderbuffer.c`:
//! the per-txn tuplecid hash (`ReorderBufferBuildTupleCidHash`), private
//! snapshot copy/free (`ReorderBufferCopySnap` / `ReorderBufferFreeSnap`), and
//! the `(relfilelocator, ctid) -> (cmin, cmax)` resolution
//! (`ResolveCminCmaxDuringDecoding`) consumed by `HeapTupleSatisfiesHistoricMVCC`.

extern crate alloc;

use alloc::vec::Vec;

use ::types_core::primitive::TransactionId;
use ::types_core::xact::{CommandId, InvalidCommandId};
use ::snapshot::SnapshotData;
// `ReorderBufferTupleCidKey`/`Ent` and the `TupleCidHash` alias are homed in
// `types-logical` so snapmgr — the owner of the active `tuplecid_data` storage
// (C `static HTAB *tuplecid_data`) — can name them without depending on this
// crate (the dependency runs the other way). Re-exported here so existing
// `crate::`/`pub use` consumers are unchanged.
pub use ::types_logical::{ReorderBufferTupleCidEnt, ReorderBufferTupleCidKey, TupleCidHash};

use crate::{ReorderBuffer, ReorderBufferChangeData, ReorderBufferChangeType};

impl ReorderBuffer {
    /// `ReorderBufferBuildTupleCidHash(rb, txn)` — build the `(relfilelocator,
    /// ctid) -> (cmin, cmax)` mapping for `HeapTupleSatisfiesHistoricMVCC` from
    /// the txn's recorded `INTERNAL_TUPLECID` changes.
    pub fn build_tuple_cid_hash(&mut self, xid: TransactionId) {
        let txn = self
            .by_txn_get_mut(xid)
            .expect("ReorderBufferTXN missing for xid");

        if !txn.has_catalog_changes() || txn.tuplecids.is_empty() {
            return;
        }

        // create the hash with the exact number of to-be-stored tuplecids.
        let mut hash: TupleCidHash = TupleCidHash::with_capacity(txn.ntuplecids as usize);

        for change in &txn.tuplecids {
            debug_assert!(change.action == ReorderBufferChangeType::InternalTupleCid);

            let (locator, tid, cmin, cmax, combocid) = match &change.data {
                ReorderBufferChangeData::TupleCid {
                    locator,
                    tid,
                    cmin,
                    cmax,
                    combocid,
                } => (*locator, *tid, *cmin, *cmax, *combocid),
                _ => unreachable!("INTERNAL_TUPLECID change carries TupleCid data"),
            };

            let key = ReorderBufferTupleCidKey {
                rlocator: locator,
                tid,
            };

            match hash.get_mut(&key) {
                None => {
                    hash.insert(
                        key,
                        ReorderBufferTupleCidEnt {
                            cmin,
                            cmax,
                            combocid,
                        },
                    );
                }
                Some(ent) => {
                    // Maybe we already saw this tuple before in this
                    // transaction, but if so it must have the same cmin.
                    debug_assert!(ent.cmin == cmin);
                    // cmax may be initially invalid, but once set it can only
                    // grow, and never become invalid again.
                    debug_assert!(
                        ent.cmax == InvalidCommandId
                            || (cmax != InvalidCommandId && cmax > ent.cmax)
                    );
                    ent.cmax = cmax;
                }
            }
        }

        txn.tuplecid_hash = Some(hash);
    }

    /// `ReorderBufferCopySnap(rb, orig_snap, txn, cid)` — copy a provided
    /// snapshot so it can be modified privately (catalog-modifying txns look
    /// into intermediate catalog states). The returned snapshot's `subxip`
    /// carries the toplevel xid plus every non-aborted subtransaction, sorted
    /// for `bsearch`, and `curcid` is set to `cid`.
    pub fn copy_snap(
        &self,
        orig_snap: &SnapshotData,
        xid: TransactionId,
        cid: CommandId,
    ) -> SnapshotData {
        let txn = self.by_txn_get(xid).expect("ReorderBufferTXN missing for xid");

        // memcpy(snap, orig_snap, sizeof(SnapshotData)) then overwrite.
        let mut snap = orig_snap.clone();

        snap.copied = true;
        snap.active_count = 1; // mark as active so nobody frees it
        snap.regd_count = 0;

        // snap->xip is a fresh copy of orig_snap->xip (xcnt unchanged).
        snap.xip = orig_snap.xip[..orig_snap.xcnt as usize].to_vec();
        snap.xcnt = orig_snap.xcnt;

        // subxip: all txids belonging to our transaction (cmin/cmax checks),
        // including the toplevel transaction itself.
        let mut subxip: Vec<TransactionId> = Vec::with_capacity(txn.nsubtxns as usize + 1);
        subxip.push(txn.xid);
        let mut subxcnt: i32 = 1;
        for &sub_xid in &txn.subtxns {
            subxip.push(sub_xid);
            subxcnt += 1;
        }

        // sort so we can bsearch() later (xidComparator: unsigned compare).
        subxip.sort_unstable();
        snap.subxip = subxip;
        snap.subxcnt = subxcnt;

        // store the specified current CommandId.
        snap.curcid = cid;

        snap
    }

    /// `ReorderBufferFreeSnap(rb, snap)` — free a previously `ReorderBufferCopySnap`'ed
    /// snapshot. A copied snapshot is owned outright (C `pfree`); a non-copied
    /// one belongs to the snapshot builder and its refcount is decremented there
    /// (`SnapBuildSnapDecRefcount`). The Rust owner takes the value by move; the
    /// caller performs the builder-side refcount decrement, mirroring the C
    /// `SnapBuildSnapIncRefcount`/`DecRefcount` discipline that lives in
    /// snapbuild.c.
    pub fn free_snap(&self, snap: SnapshotData) {
        // Copied snapshots are dropped here (C pfree). Non-copied snapshots are
        // builder-owned; the builder handles the refcount, so we simply drop our
        // reference.
        let _ = snap;
    }
}

