// Streaming of large in-progress transactions (reorderbuffer.c): when the
// output plugin supports the stream API and a consistent snapshot exists,
// eviction sends the largest streamable toplevel transaction to the plugin
// instead of spilling it, and commit of a (partially) streamed transaction
// finishes through stream_commit/stream_prepare.

use types_core::{
    CommandId, FirstCommandId, InvalidCommandId, InvalidTransactionId, InvalidXLogRecPtr,
};
use types_error::PgResult;

use snapmgr::Snapshot;

use crate::{
    dl_iter, ReorderBuffer, TxnId, RBTXN_SENT_PREPARE,
};

impl ReorderBuffer {
    // ReorderBufferCanStartStreaming (reorderbuffer.c:4285): the plugin must
    // stream (stream callbacks installed) and the decoding context must have
    // reached a consistent point that does not skip the current record. The
    // context half is mirrored into `streaming_ready` by the decode loop
    // before each record (the builder lives across the crate boundary).
    pub(crate) fn can_start_streaming(&self) -> bool {
        self.can_stream() && self.streaming_ready
    }

    // ReorderBufferLargestStreamableTopTXN (reorderbuffer.c:3811): the
    // largest toplevel transaction with a base snapshot that has complete
    // (non-partial) streamable changes and is not known aborted.
    pub(crate) fn largest_streamable_top_txn(&self) -> Option<TxnId> {
        let mut largest: Option<TxnId> = None;
        let mut largest_size = 0usize;
        for id in dl_iter(&self.txns, self.txns_by_base_snapshot_lsn, |t| t.base_snapshot_node) {
            let txn = self.txn(id);
            debug_assert!(!txn.is_known_subxact());
            debug_assert!(txn.base_snapshot.is_some());

            if txn.has_partial_change() || !txn.has_streamable_change() || txn.is_aborted() {
                continue;
            }
            if txn.total_size > largest_size {
                largest = Some(id);
                largest_size = txn.total_size;
            }
        }
        largest
    }

    // ReorderBufferSaveTXNSnapshot (reorderbuffer.c:2118): remember the
    // command id and snapshot at the end of a stream for the next run.
    pub(crate) fn save_txn_snapshot(
        &mut self,
        txn: TxnId,
        snapshot_now: &Snapshot,
        command_id: CommandId,
    ) {
        self.txn_mut(txn).command_id = command_id;
        // Avoid copying if it's already copied.
        let snap = if snapshot_now.copied {
            snapshot_now.clone()
        } else {
            self.copy_snap(snapshot_now, txn, command_id)
        };
        self.txn_mut(txn).snapshot_now = Some(snap);
    }

    // ReorderBufferStreamTXN (reorderbuffer.c:4310): send the data of a large
    // in-progress transaction (and its subtransactions) to the output plugin
    // through the stream API.
    pub(crate) fn stream_txn(&mut self, txn: TxnId) -> PgResult<()> {
        // We can never reach here for a subtransaction.
        debug_assert!(self.txn(txn).is_toptxn());

        let command_id;
        let snapshot_now;
        if self.txn(txn).snapshot_now.is_none() {
            // First streaming run: base_snapshot may still sit on a subxact
            // (ReorderBufferCommitChild has not run for an in-progress txn),
            // so transfer like the commit path would.
            debug_assert!(!self.txn(txn).is_streamed());
            debug_assert!(self.txn(txn).command_id == InvalidCommandId);

            let subs: Vec<TxnId> =
                dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node).collect();
            for sub in subs {
                self.transfer_snap_to_parent(txn, sub);
            }

            // No snapshot means no changes to the database: nothing to decode.
            if self.txn(txn).base_snapshot.is_none() {
                debug_assert_eq!(self.txn(txn).ninvalidations(), 0);
                return Ok(());
            }

            command_id = FirstCommandId;
            let base = self.txn(txn).base_snapshot.clone().expect("checked above");
            // The copy adds this transaction tree's xids to subxip so catalog
            // changes decoded so far stay visible.
            snapshot_now = self.copy_snap(&base, txn, command_id);
        } else {
            // Reuse the snapshot from the previous streaming run; re-copy so
            // subxacts that appeared since then are added to subxip.
            debug_assert!(self.txn(txn).is_streamed());
            command_id = self.txn(txn).command_id;
            let prev = self.txn_mut(txn).snapshot_now.take().expect("checked above");
            debug_assert!(prev.copied);
            snapshot_now = self.copy_snap(&prev, txn, command_id);
        }

        // Remember these before processing: an error mid-stream must not
        // accumulate stats, and processing truncates the txn.
        let txn_is_streamed = self.txn(txn).is_streamed();
        let stream_bytes = self.txn(txn).total_size;

        // Process and send the changes to the output plugin.
        self.process_txn(txn, InvalidXLogRecPtr, snapshot_now, command_id, true)?;

        self.streamCount += 1;
        self.streamBytes += stream_bytes as i64;
        // Don't count an already-streamed transaction again.
        self.streamTxns += if txn_is_streamed { 0 } else { 1 };
        // C flushes to pgstat through rb->private_data's decoding context
        // (reorderbuffer.c:4414); the owning context installs this hook.
        if let Some(update_stats) = self.update_stats {
            update_stats(self);
        }

        debug_assert!(self.txn(txn).changes.is_empty());
        debug_assert_eq!(self.txn(txn).nentries, 0);
        debug_assert_eq!(self.txn(txn).nentries_mem, 0);
        Ok(())
    }

    // ReorderBufferStreamCommit (reorderbuffer.c:1982): a (partially)
    // streamed transaction commits/prepares in the streamed way — stream the
    // remaining part, then send stream_commit or stream_prepare.
    pub(crate) fn stream_commit(&mut self, txn: TxnId) -> PgResult<()> {
        debug_assert!(self.txn(txn).is_streamed());

        self.stream_txn(txn)?;

        let final_lsn = self.txn(txn).final_lsn;
        if self.txn(txn).is_prepared() {
            // Send stream_prepare even if a concurrent abort was detected
            // (DecodePrepare's contract).
            debug_assert!(!self.txn(txn).sent_prepare());
            let cb = self
                .callbacks
                .stream_prepare
                .expect("streamed prepare requires the stream_prepare callback");
            cb(self, txn, final_lsn)?;
            self.txn_mut(txn).txn_flags |= RBTXN_SENT_PREPARE;

            // Two-phase: full cleanup happens at COMMIT PREPARED; truncate
            // the changes and tuplecids now.
            self.truncate_txn(txn, true)?;
            xact::SetCheckXidAlive(InvalidTransactionId);
        } else {
            let cb = self
                .callbacks
                .stream_commit
                .expect("streamed commit requires the stream_commit callback");
            cb(self, txn, final_lsn)?;
            self.cleanup_txn(txn)?;
        }
        Ok(())
    }
}
