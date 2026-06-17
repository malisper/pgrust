//! TOAST-reassembly family of `replication/logical/reorderbuffer.c`.
//!
//! When a logically-decoded tuple references an out-of-line (TOASTed) value,
//! the individual toast chunks arrive as separate INSERT changes into the
//! transaction's toast relation. `ReorderBufferToastAppendChunk` collects the
//! chunks per `chunk_id` into a [`ReorderBufferToastEnt`], and
//! `ReorderBufferToastReplace` stitches them back together and rewrites the
//! main tuple's external pointer into an in-memory indirect pointer.
//! `ReorderBufferToastReset` discards the per-txn toast hash once a tuple's
//! creator signals the chunks are no longer required.
//!
//! Chunk collection and reassembly read the decoded `HeapTuple`s stored on the
//! INSERT changes (`change->data.tp.newtuple`). Those decoded tuples are owned
//! and produced by the change-replay tuple-decode path, which is gated on the
//! decoded-tuple carrier keystone (see [`crate::replay`]); the per-txn toast
//! hash and its lifecycle (`ToastReset`) are modeled here so the spine that
//! drives it (`ReorderBufferProcessTXN`) is faithful, and the two chunk-data
//! entry points (`AppendChunk`/`Replace`) panic loudly until that carrier
//! lands.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::TransactionId;
use types_core::Oid;

use crate::{ReorderBuffer, ReorderBufferChange};

/// `ReorderBufferToastEnt` (reorderbuffer.c) — per `chunk_id` accumulation of
/// the toast chunks decoded for one out-of-line value.
#[derive(Debug)]
pub struct ReorderBufferToastEnt {
    /// `Oid chunk_id` — the toast_table.chunk_id, the hash key.
    pub chunk_id: Oid,
    /// `int32 last_chunk_seq` — chunk_seq of the last chunk we saw.
    pub last_chunk_seq: i32,
    /// `Size num_chunks` — number of chunks we've already seen.
    pub num_chunks: usize,
    /// `Size size` — combined size of chunks seen so far.
    pub size: usize,
    /// `dlist_head chunks` — the INSERT changes carrying the chunk data, in
    /// arrival order. Held as the owned changes themselves (the C threads them
    /// through the intrusive `ReorderBufferChange.node`).
    pub chunks: Vec<ReorderBufferChange>,
    /// `struct varlena *reconstructed` — the reassembled varlena pointed to in
    /// the main tuple; `None` until [`ReorderBuffer::toast_replace`] builds it.
    pub reconstructed: Option<Vec<u8>>,
}

impl ReorderBuffer {
    /// `ReorderBufferToastReset(rb, txn)` — free all reassembled toast data and
    /// drop the per-txn toast hash. Frees each `reconstructed` varlena and each
    /// chunk change (`ReorderBufferFreeChange(rb, change, true)`), then destroys
    /// the hash and clears `txn->toast_hash`.
    pub(crate) fn toast_reset(&mut self, xid: TransactionId) {
        let toast_hash = match self.toast_hash_take(xid) {
            None => return,
            Some(h) => h,
        };

        for (_chunk_id, ent) in toast_hash {
            // C pfree(ent->reconstructed): the owned Vec drops here.
            drop(ent.reconstructed);
            // dlist_foreach_modify over ent->chunks: free each chunk change
            // with upd_mem = true.
            for change in ent.chunks {
                self.free_change(change, true);
            }
        }
        // hash_destroy(txn->toast_hash); txn->toast_hash = NULL — the take()
        // above already removed it from the txn.
    }

    /// `ReorderBufferToastAppendChunk(rb, txn, relation, change)` — collect one
    /// decoded toast-chunk INSERT into the per-txn toast hash, keyed by the
    /// chunk's `chunk_id`.
    ///
    /// Reads `chunk_id`/`chunk_seq`/`chunk_data` via `fastgetattr` off the
    /// decoded `HeapTuple` carried on `change->data.tp.newtuple`. The decoded
    /// tuple carrier is not yet modeled on `ReorderBufferChangeData::Tp` (the
    /// change-replay tuple-decode keystone), so the chunk bytes are
    /// unreachable; mirror-PG-and-panic until that lands.
    #[allow(dead_code)]
    pub(crate) fn toast_append_chunk(
        &mut self,
        _xid: TransactionId,
        _relation: Oid,
        _change: ReorderBufferChange,
    ) {
        panic!(
            "ReorderBufferToastAppendChunk: decoded toast-chunk HeapTuple \
             carrier not yet modeled (change-replay tuple-decode keystone)"
        );
    }

    /// `ReorderBufferToastReplace(rb, txn, relation, change)` — reassemble every
    /// out-of-line attribute referenced by the main tuple from its collected
    /// chunks and rewrite the tuple's external pointers into in-memory indirect
    /// pointers.
    ///
    /// Needs `heap_deform_tuple`/`heap_form_tuple` over the decoded main tuple,
    /// the toast relation descriptor (`RelationIdGetRelation` on
    /// `rd_rel->reltoastrelid`), and the varlena/toast pointer machinery — all
    /// gated on the same decoded-tuple carrier keystone as
    /// [`ReorderBuffer::toast_append_chunk`]. Mirror-PG-and-panic until it lands.
    pub(crate) fn toast_replace(
        &mut self,
        xid: TransactionId,
        _relation: Oid,
        _change: &mut ReorderBufferChange,
    ) {
        // C: `if (txn->toast_hash == NULL) return;` — no out-of-line attribute
        // was reassembled for this txn, so the main tuple needs no rewrite. This
        // is the common path for tuples with no TOASTed values.
        let has_toast = self
            .by_txn_get(xid)
            .is_some_and(|t| t.toast_hash.is_some());
        if !has_toast {
            return;
        }
        // The reassembly path (heap_deform/form + detoast over the decoded main
        // tuple and the collected chunks) is gated on the decoded-tuple carrier
        // keystone; mirror-PG-and-panic until it lands.
        panic!(
            "ReorderBufferToastReplace: decoded-tuple reassembly (heap_form/\
             deform + detoast) not yet modeled (change-replay tuple-decode \
             keystone)"
        );
    }
}
