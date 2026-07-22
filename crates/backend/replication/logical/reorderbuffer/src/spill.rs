// Disk serialization support (reorderbuffer.c:3737): when decoded-but-not-yet-
// replayed changes exceed logical_decoding_work_mem, the largest (sub)txn is
// evicted from memory into per-WAL-segment spill files under
// pg_replslot/<slot>/, and read back in bounded batches while replaying.
//
// The on-disk record encoding is port-private (size-prefixed, native-endian):
// like C's memcpy'd ReorderBufferDiskChange it is only ever read back by the
// same build within the same decoding run — leftovers are deleted at startup
// and at slot allocate/free (startup.rs) — so no cross-version stability is
// promised. What is C-exact is the lifecycle: file naming/segmentation,
// final_lsn maintenance, nentries vs nentries_mem accounting, batch size,
// spill statistics, and every allocation on the restore path landing in the
// buffer's own context (rb->context parity; tuple/msg/inval/snapshot copies
// must never land in a shorter- or longer-lived context).

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

use mcx::{PgString, PgVec};
use types_core::{InvalidXLogRecPtr, Oid, TransactionId, XLogRecPtr};
use types_error::PgResult;
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::{RelFileLocator, SharedInvalidationMessage};
use types_tuple::{BlockIdData, ItemPointerData, SizeofHeapTupleHeader};

use crate::{
    dl_delete, dl_iter, rb_error, ChangeId, ListHead, ReorderBuffer,
    ReorderBufferChange, ReorderBufferChangeData, ReorderBufferChangeType,
    ReorderBufferChangeType::*, TxnId, INVALID_ID, RBTXN_IS_ABORTED, RBTXN_IS_SERIALIZED,
    RBTXN_IS_SERIALIZED_CLEAR,
};

// max_changes_in_memory (reorderbuffer.c:245): restore batch size.
const MAX_CHANGES_IN_MEMORY: u64 = 4096;

const SHARED_INVAL_MESSAGE_SIZE: usize = 16;

// ReorderBufferSerializedPath (reorderbuffer.c:4889):
// pg_replslot/<slot>/xid-%u-lsn-%X-%X.spill, LSN = the segment start.
pub(crate) fn serialized_path(slot_name: &str, xid: TransactionId, segno: u64) -> PathBuf {
    let seg_size = transam_xlog_seams::wal_segment_size::call() as u64;
    let recptr: XLogRecPtr = segno * seg_size;
    let mut path = crate::startup::replslot_dir().expect("DataDir set on spill paths");
    path.push(slot_name);
    path.push(format!(
        "xid-{}-lsn-{:X}-{:X}.spill",
        xid,
        (recptr >> 32) as u32,
        recptr as u32
    ));
    path
}

// ---- record encoding helpers -----------------------------------------------

fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}
fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_ne_bytes());
}

struct Cur<'a> {
    b: &'a [u8],
}

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> PgResult<&'a [u8]> {
        if self.b.len() < n {
            return Err(rb_error(format!(
                "could not read from reorderbuffer spill file: read {} instead of {} bytes",
                self.b.len(),
                n
            )));
        }
        let (head, tail) = self.b.split_at(n);
        self.b = tail;
        Ok(head)
    }
    fn u8(&mut self) -> PgResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> PgResult<u16> {
        Ok(u16::from_ne_bytes(self.take(2)?.try_into().expect("2 bytes")))
    }
    fn u32(&mut self) -> PgResult<u32> {
        Ok(u32::from_ne_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }
    fn i32(&mut self) -> PgResult<i32> {
        Ok(i32::from_ne_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }
    fn u64(&mut self) -> PgResult<u64> {
        Ok(u64::from_ne_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }
}

fn action_from_i32(v: i32) -> PgResult<ReorderBufferChangeType> {
    Ok(match v {
        0 => Insert,
        1 => Update,
        2 => Delete,
        3 => Message,
        4 => Invalidation,
        5 => InternalSnapshot,
        6 => InternalCommandId,
        7 => InternalTupleCid,
        8 => InternalSpecInsert,
        9 => InternalSpecConfirm,
        10 => InternalSpecAbort,
        11 => Truncate,
        _ => {
            return Err(rb_error(format!(
                "unrecognized change action {v} in reorderbuffer spill file"
            )))
        }
    })
}

fn snapshot_type_from_i32(v: i32) -> PgResult<SnapshotType> {
    Ok(match v {
        0 => SnapshotType::SNAPSHOT_MVCC,
        1 => SnapshotType::SNAPSHOT_SELF,
        2 => SnapshotType::SNAPSHOT_ANY,
        3 => SnapshotType::SNAPSHOT_TOAST,
        4 => SnapshotType::SNAPSHOT_DIRTY,
        5 => SnapshotType::SNAPSHOT_HISTORIC_MVCC,
        6 => SnapshotType::SNAPSHOT_NON_VACUUMABLE,
        _ => {
            return Err(rb_error(format!(
                "unrecognized snapshot type {v} in reorderbuffer spill file"
            )))
        }
    })
}

fn put_locator(buf: &mut Vec<u8>, loc: &RelFileLocator) {
    put_u32(buf, loc.spcOid);
    put_u32(buf, loc.dbOid);
    put_u32(buf, loc.relNumber);
}

fn get_locator(cur: &mut Cur) -> PgResult<RelFileLocator> {
    Ok(RelFileLocator {
        spcOid: cur.u32()?,
        dbOid: cur.u32()?,
        relNumber: cur.u32()?,
    })
}

fn put_tid(buf: &mut Vec<u8>, tid: &ItemPointerData) {
    put_u16(buf, tid.ip_blkid.bi_hi);
    put_u16(buf, tid.ip_blkid.bi_lo);
    put_u16(buf, tid.ip_posid);
}

fn get_tid(cur: &mut Cur) -> PgResult<ItemPointerData> {
    Ok(ItemPointerData {
        ip_blkid: BlockIdData { bi_hi: cur.u16()?, bi_lo: cur.u16()? },
        ip_posid: cur.u16()?,
    })
}

// Full owned tuple image: identity fields + header-and-data bytes, C's
// HeapTupleData copy followed by the t_len image (reorderbuffer.c:4107).
fn put_tuple(buf: &mut Vec<u8>, tuple: &heaptuple::HeapTuple<'static>) {
    put_tid(buf, &tuple.t_self);
    put_u32(buf, tuple.t_tableOid);
    put_u32(buf, tuple.t_len);
    buf.extend_from_slice(tuple.image());
}

impl ReorderBuffer {
    fn get_tuple(&self, cur: &mut Cur) -> PgResult<heaptuple::HeapTuple<'static>> {
        let t_self = get_tid(cur)?;
        let t_table_oid = cur.u32()?;
        let t_len = cur.u32()? as usize;
        if t_len < SizeofHeapTupleHeader {
            return Err(rb_error(format!(
                "invalid tuple length {t_len} in reorderbuffer spill file"
            )));
        }
        let image = cur.take(t_len)?;
        // ReorderBufferAllocTupleBuf parity: the copy lands in the buffer's
        // own context, never the caller's.
        let mut tuple = self.alloc_tuple_buf(t_len - SizeofHeapTupleHeader)?;
        tuple.image_mut().copy_from_slice(image);
        tuple.t_self = t_self;
        tuple.t_tableOid = t_table_oid;
        Ok(tuple)
    }

    // ReorderBufferLargestTXN (reorderbuffer.c:3789). C keeps a pairing
    // max-heap (rb->txn_heap) current on every memory-accounting update; this
    // port selects by scanning the live txn slab, only when the limit trips.
    // Same selection (max txn->size among accounted txns), no bookkeeping on
    // the per-change hot path.
    fn largest_txn(&self) -> Option<TxnId> {
        let mut largest: Option<TxnId> = None;
        let mut largest_size = 0usize;
        for (id, slot) in self.txns.iter().enumerate() {
            if let Some(txn) = slot {
                if txn.size > largest_size {
                    largest_size = txn.size;
                    largest = Some(id as TxnId);
                }
            }
        }
        debug_assert!(largest.is_none() || largest_size <= self.size);
        largest
    }

    // ReorderBufferCheckAndTruncateAbortedTXN (reorderbuffer.c:1773): CLOG-
    // check the eviction candidate and discard its changes when the
    // transaction already aborted; the verdict is cached in txn_flags.
    fn check_and_truncate_aborted_txn(&mut self, txn: TxnId) -> PgResult<bool> {
        // Quick return for regression tests.
        if guc_tables::vars::debug_logical_replication_streaming.read()
            == guc_tables::consts::DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE
        {
            return Ok(false);
        }

        if self.txn(txn).is_committed() {
            return Ok(false);
        }
        if self.txn(txn).is_aborted() {
            // Already-aborted transactions should not have any changes.
            debug_assert_eq!(self.txn(txn).size, 0);
            return Ok(true);
        }

        let xid = self.txn(txn).xid;
        if procarray_seams::transaction_id_is_in_progress::call(xid)? {
            return Ok(false);
        }
        if transam_seams::transaction_id_did_commit::call(xid)? {
            // Cache the commit so future eviction passes skip the CLOG.
            debug_assert!(!self.txn(txn).is_aborted());
            self.txn_mut(txn).txn_flags |= crate::RBTXN_IS_COMMITTED;
            return Ok(false);
        }

        // Aborted: discard the changes collected so far and the toast
        // reconstruction data; full cleanup happens when the ABORT record of
        // this transaction is decoded.
        let prepared = self.txn(txn).is_prepared();
        self.truncate_txn(txn, prepared)?;
        self.toast_reset(txn);
        debug_assert_eq!(self.txn(txn).size, 0);
        debug_assert!(!self.txn(txn).is_committed());
        self.txn_mut(txn).txn_flags |= RBTXN_IS_ABORTED;
        Ok(true)
    }

    // ReorderBufferCheckMemoryLimit (reorderbuffer.c:3883): evict the largest
    // (sub)transaction at a time until back under logical_decoding_work_mem;
    // under debug_logical_replication_streaming=immediate, until empty.
    pub(crate) fn check_memory_limit(&mut self) -> PgResult<()> {
        let limit = guc_tables::vars::logical_decoding_work_mem.read() as usize * 1024;
        let immediate = guc_tables::vars::debug_logical_replication_streaming.read()
            == guc_tables::consts::DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE;

        if !immediate && self.size < limit {
            return Ok(());
        }

        // Loop rather than evicting once: the GUC can shrink between changes,
        // so one eviction is not guaranteed to get back under the limit.
        while self.size >= limit || (immediate && self.size > 0) {
            // Prefer evicting the largest streamable toplevel transaction by
            // streaming it, when the plugin supports it and a consistent
            // point was reached; otherwise spill the largest (sub)txn.
            let streamable = if self.can_start_streaming() {
                self.largest_streamable_top_txn()
            } else {
                None
            };
            if let Some(txn) = streamable {
                debug_assert!(self.txn(txn).is_toptxn());
                debug_assert!(self.txn(txn).total_size > 0);
                debug_assert!(self.size >= self.txn(txn).total_size);

                // Skip the transaction if aborted.
                if self.check_and_truncate_aborted_txn(txn)? {
                    continue;
                }

                self.stream_txn(txn)?;
            } else {
                // Pick the largest transaction (or subtransaction) and evict
                // it from memory by serializing it to disk.
                let txn = self.largest_txn().expect("nonzero rb size implies a sized txn");
                debug_assert!(self.txn(txn).size > 0);
                debug_assert!(self.size >= self.txn(txn).size);

                // Skip the transaction if aborted.
                if self.check_and_truncate_aborted_txn(txn)? {
                    continue;
                }

                self.serialize_txn(txn)?;

                // After eviction, the transaction must hold no accounted
                // memory.
                debug_assert_eq!(self.txn(txn).size, 0);
                debug_assert_eq!(self.txn(txn).nentries_mem, 0);
            }
        }

        debug_assert!(self.size < limit);
        Ok(())
    }

    // ReorderBufferSerializeTXN (reorderbuffer.c:3963): spill a transaction
    // (and its subtransactions) to disk, one file per WAL segment of the
    // change LSNs, freeing the in-memory changes as they are written.
    pub(crate) fn serialize_txn(&mut self, txn: TxnId) -> PgResult<()> {
        let size = self.txn(txn).size;
        let nentries_mem = self.txn(txn).nentries_mem;

        // Do the same to all child TXs.
        let subs: Vec<TxnId> = dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node).collect();
        for sub in subs {
            self.serialize_txn(sub)?;
        }

        let seg_size = transam_xlog_seams::wal_segment_size::call() as u64;
        let xid = self.txn(txn).xid;
        let mut file: Option<File> = None;
        let mut cur_open_segno: u64 = 0;
        let mut spilled: u64 = 0;
        // C reuses rb->outbuf across calls; one buffer per spill pass keeps
        // the same one-write-per-change shape without a long-lived field.
        let mut buf: Vec<u8> = Vec::new();

        loop {
            let head = self.txn(txn).changes.head;
            if head == INVALID_ID {
                break;
            }

            // Store the change in the segment its start LSN falls into; a
            // change never splits across segment files.
            let segno = self.change(head).lsn / seg_size;
            if file.is_none() || segno != cur_open_segno {
                drop(file.take());
                cur_open_segno = segno;
                // No need to care about TLIs here: the files live for a
                // single run, so each LSN maps to one WAL record.
                let path = serialized_path(&self.slot_name, xid, segno);
                file = Some(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .map_err(|e| {
                            rb_error(format!("could not open file \"{}\": {e}", path.display()))
                        })?,
                );
            }

            self.serialize_change(txn, file.as_mut().expect("opened above"), head, &mut buf)?;
            let mut list = self.txn(txn).changes;
            dl_delete(&mut self.changes, &mut list, head, |c| &mut c.node);
            self.txn_mut(txn).changes = list;
            self.free_change(head, false);
            spilled += 1;
        }

        // Update the memory counter: everything this txn had accounted is now
        // on disk.
        self.change_memory_update(None, Some(txn), false, size);

        // Update the statistics iff we spilled anything.
        if spilled > 0 {
            self.spillCount += 1;
            self.spillBytes += size as i64;
            // Don't count already-serialized transactions again.
            let counted = self.txn(txn).txn_flags & (RBTXN_IS_SERIALIZED | RBTXN_IS_SERIALIZED_CLEAR)
                != 0;
            self.spillTxns += if counted { 0 } else { 1 };
            // C flushes to pgstat through rb->private_data's decoding context
            // (reorderbuffer.c:4042); the owning context installs this hook.
            if let Some(update_stats) = self.update_stats {
                update_stats(self);
            }
        }

        debug_assert_eq!(spilled, nentries_mem);
        debug_assert!(self.txn(txn).changes.is_empty());
        self.txn_mut(txn).nentries_mem = 0;
        self.txn_mut(txn).txn_flags |= RBTXN_IS_SERIALIZED;
        Ok(())
    }

    // ReorderBufferSerializeChange (reorderbuffer.c:4058): one size-prefixed
    // record, one write(2) per change.
    fn serialize_change(
        &mut self,
        txn: TxnId,
        file: &mut File,
        cid: ChangeId,
        buf: &mut Vec<u8>,
    ) -> PgResult<()> {
        let change = self.change(cid);
        let lsn = change.lsn;

        buf.clear();
        put_u64(buf, 0); // total record size, patched below
        put_i32(buf, change.action as i32);
        put_u16(buf, change.origin_id);
        put_u64(buf, change.lsn);

        match &change.data {
            // C memcpy's the base struct for every action, so the locator and
            // clear-toast flag survive for the spec-confirm/abort forms too;
            // tuples ride only when present.
            ReorderBufferChangeData::Tp { rlocator, clear_toast_afterwards, oldtuple, newtuple } =>
            {
                put_locator(buf, rlocator);
                put_u8(buf, *clear_toast_afterwards as u8);
                put_u8(buf, oldtuple.is_some() as u8);
                put_u8(buf, newtuple.is_some() as u8);
                if let Some(t) = oldtuple {
                    put_tuple(buf, t);
                }
                if let Some(t) = newtuple {
                    put_tuple(buf, t);
                }
            }
            ReorderBufferChangeData::Msg { prefix, message } => {
                put_u64(buf, prefix.len() as u64);
                buf.extend_from_slice(prefix.as_str().as_bytes());
                put_u64(buf, message.len() as u64);
                buf.extend_from_slice(message);
            }
            ReorderBufferChangeData::Inval { invalidations } => {
                put_u64(buf, invalidations.len() as u64);
                for msg in invalidations.iter() {
                    buf.extend_from_slice(&msg.to_wire_bytes());
                }
            }
            ReorderBufferChangeData::Snapshot(snap) => {
                put_i32(buf, snap.snapshot_type as i32);
                put_u32(buf, snap.xmin);
                put_u32(buf, snap.xmax);
                put_u32(buf, snap.xcnt);
                for xid in &snap.xip[..snap.xcnt as usize] {
                    put_u32(buf, *xid);
                }
                let subxcnt = snap.subxcnt.max(0);
                put_i32(buf, subxcnt);
                for xid in &snap.subxip[..subxcnt as usize] {
                    put_u32(buf, *xid);
                }
                put_u8(buf, snap.suboverflowed as u8);
                put_u8(buf, snap.takenDuringRecovery as u8);
                put_u32(buf, snap.speculativeToken);
                put_u32(buf, snap.curcid.get());
                put_u64(buf, snap.vistest.id);
                put_u64(buf, snap.snapXactCompletionCount);
            }
            ReorderBufferChangeData::Truncate { cascade, restart_seqs, relids } => {
                put_u8(buf, *cascade as u8);
                put_u8(buf, *restart_seqs as u8);
                put_u64(buf, relids.len() as u64);
                for relid in relids.iter() {
                    put_u32(buf, *relid);
                }
            }
            ReorderBufferChangeData::CommandId(cmd) => {
                put_u32(buf, *cmd);
            }
            ReorderBufferChangeData::TupleCid { locator, tid, cmin, cmax, combocid } => {
                put_locator(buf, locator);
                put_tid(buf, tid);
                put_u32(buf, *cmin);
                put_u32(buf, *cmax);
                put_u32(buf, *combocid);
            }
        }

        let size = buf.len() as u64;
        buf[..8].copy_from_slice(&size.to_ne_bytes());

        file.write_all(buf).map_err(|e| {
            let xid = self.txn(txn).xid;
            rb_error(format!("could not write to data file for XID {xid}: {e}"))
        })?;

        // Keep final_lsn current with each change sent to disk so that
        // RestoreCleanup works even if a crash leaves the transaction without
        // its abort record (reorderbuffer.c:4256). Never move it backwards.
        if self.txn(txn).final_lsn < lsn {
            self.txn_mut(txn).final_lsn = lsn;
        }
        Ok(())
    }

    // ReorderBufferRestoreChanges (reorderbuffer.c:4510): drop the current
    // in-memory batch and read back up to max_changes_in_memory changes,
    // walking segment files from first_lsn's segment to final_lsn's.
    pub(crate) fn restore_changes(
        &mut self,
        txn: TxnId,
        file: &mut Option<File>,
        segno: &mut u64,
    ) -> PgResult<u64> {
        debug_assert!(self.txn(txn).first_lsn != InvalidXLogRecPtr);
        debug_assert!(self.txn(txn).final_lsn != InvalidXLogRecPtr);

        // Free the current entries, so we have memory for more.
        let changes: Vec<ChangeId> =
            dl_iter(&self.changes, self.txn(txn).changes, |c| c.node).collect();
        self.txn_mut(txn).changes = ListHead::EMPTY;
        for cid in changes {
            self.free_change(cid, true);
        }
        self.txn_mut(txn).nentries_mem = 0;

        let seg_size = transam_xlog_seams::wal_segment_size::call() as u64;
        let last_segno = self.txn(txn).final_lsn / seg_size;
        let xid = self.txn(txn).xid;

        let mut restored: u64 = 0;
        let mut buf: Vec<u8> = Vec::new();
        while restored < MAX_CHANGES_IN_MEMORY && *segno <= last_segno {
            postgres_seams::check_for_interrupts::call()?;

            if file.is_none() {
                // First time in: start at the segment holding first_lsn.
                if *segno == 0 {
                    *segno = self.txn(txn).first_lsn / seg_size;
                }
                debug_assert!(*segno != 0 || self.txn(txn).changes.is_empty());

                let path = serialized_path(&self.slot_name, xid, *segno);
                match File::open(&path) {
                    Ok(f) => *file = Some(f),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        *segno += 1;
                        continue;
                    }
                    Err(e) => {
                        return Err(rb_error(format!(
                            "could not open file \"{}\": {e}",
                            path.display()
                        )))
                    }
                }
            }

            // Read the size prefix; a clean zero-byte read is end-of-segment.
            let f = file.as_mut().expect("opened above");
            let mut szbuf = [0u8; 8];
            let n = read_full(f, &mut szbuf)?;
            if n == 0 {
                *file = None;
                *segno += 1;
                continue;
            }
            if n != szbuf.len() {
                return Err(rb_error(format!(
                    "could not read from reorderbuffer spill file: read {n} instead of {} bytes",
                    szbuf.len()
                )));
            }
            let size = u64::from_ne_bytes(szbuf) as usize;
            if size < szbuf.len() {
                return Err(rb_error(format!(
                    "invalid record size {size} in reorderbuffer spill file"
                )));
            }

            let body = size - szbuf.len();
            buf.clear();
            buf.resize(body, 0);
            let n = read_full(f, &mut buf)?;
            if n != body {
                return Err(rb_error(format!(
                    "could not read from reorderbuffer spill file: read {n} instead of {body} bytes"
                )));
            }

            self.restore_change(txn, &buf)?;
            restored += 1;
        }

        Ok(restored)
    }

    // ReorderBufferRestoreChange (reorderbuffer.c:4653): decode one record
    // back into in-memory form and queue it onto the TXN's change list.
    fn restore_change(&mut self, txn: TxnId, body: &[u8]) -> PgResult<()> {
        let mut cur = Cur { b: body };
        let action = action_from_i32(cur.i32()?)?;
        let origin_id = cur.u16()?;
        let lsn = cur.u64()?;

        let data = match action {
            Insert | Update | Delete | InternalSpecInsert | InternalSpecConfirm
            | InternalSpecAbort => {
                let rlocator = get_locator(&mut cur)?;
                let clear_toast_afterwards = cur.u8()? != 0;
                let has_old = cur.u8()? != 0;
                let has_new = cur.u8()? != 0;
                let oldtuple = if has_old { Some(self.get_tuple(&mut cur)?) } else { None };
                let newtuple = if has_new { Some(self.get_tuple(&mut cur)?) } else { None };
                ReorderBufferChangeData::Tp { rlocator, clear_toast_afterwards, oldtuple, newtuple }
            }
            Message => {
                let prefix_len = cur.u64()? as usize;
                let prefix_bytes = cur.take(prefix_len)?;
                let prefix_str = std::str::from_utf8(prefix_bytes).map_err(|_| {
                    rb_error("invalid message prefix in reorderbuffer spill file".into())
                })?;
                let msg_len = cur.u64()? as usize;
                let msg_bytes = cur.take(msg_len)?;
                let mut message = PgVec::new_in(self.mcx);
                mcx::vec_append_bytes(&mut message, msg_bytes)?;
                ReorderBufferChangeData::Msg {
                    prefix: PgString::from_str_in(prefix_str, self.mcx)?,
                    message,
                }
            }
            Invalidation => {
                let n = cur.u64()? as usize;
                let mut invalidations = PgVec::new_in(self.mcx);
                for _ in 0..n {
                    let raw: [u8; SHARED_INVAL_MESSAGE_SIZE] = cur
                        .take(SHARED_INVAL_MESSAGE_SIZE)?
                        .try_into()
                        .expect("exact take");
                    let msg = SharedInvalidationMessage::from_wire_bytes(raw).ok_or_else(|| {
                        rb_error("unrecognized invalidation in reorderbuffer spill file".into())
                    })?;
                    invalidations.push(msg);
                }
                ReorderBufferChangeData::Inval { invalidations }
            }
            InternalSnapshot => {
                let ty = snapshot_type_from_i32(cur.i32()?)?;
                let mut snap = SnapshotData::sentinel(self.mcx, ty);
                snap.xmin = cur.u32()?;
                snap.xmax = cur.u32()?;
                let xcnt = cur.u32()?;
                let mut xip = PgVec::new_in(self.mcx);
                for _ in 0..xcnt {
                    xip.push(cur.u32()?);
                }
                snap.xip = xip;
                snap.xcnt = xcnt;
                let subxcnt = cur.i32()?.max(0);
                let mut subxip = PgVec::new_in(self.mcx);
                for _ in 0..subxcnt {
                    subxip.push(cur.u32()?);
                }
                snap.subxip = subxip;
                snap.subxcnt = subxcnt;
                snap.suboverflowed = cur.u8()? != 0;
                snap.takenDuringRecovery = cur.u8()? != 0;
                snap.speculativeToken = cur.u32()?;
                snap.curcid.set(cur.u32()?);
                snap.vistest = types_core::GlobalVisStateHandle::new(cur.u64()?);
                snap.snapXactCompletionCount = cur.u64()?;
                // The restored snapshot is a private copy (C sets copied=true,
                // reorderbuffer.c:4783); refcounts follow copy_snap's
                // fresh-copy convention — C memcpy's the serialized counts but
                // never reads them on this path.
                snap.copied = true;
                snap.active_count.set(1);
                snap.regd_count.set(0);
                ReorderBufferChangeData::Snapshot(std::rc::Rc::new(snap))
            }
            Truncate => {
                let cascade = cur.u8()? != 0;
                let restart_seqs = cur.u8()? != 0;
                let n = cur.u64()? as usize;
                let mut relids: PgVec<'static, Oid> = PgVec::new_in(self.mcx);
                for _ in 0..n {
                    relids.push(cur.u32()?);
                }
                ReorderBufferChangeData::Truncate { cascade, restart_seqs, relids }
            }
            InternalCommandId => ReorderBufferChangeData::CommandId(cur.u32()?),
            InternalTupleCid => {
                let locator = get_locator(&mut cur)?;
                let tid = get_tid(&mut cur)?;
                ReorderBufferChangeData::TupleCid {
                    locator,
                    tid,
                    cmin: cur.u32()?,
                    cmax: cur.u32()?,
                    combocid: cur.u32()?,
                }
            }
        };

        let mut change = ReorderBufferChange::new(action, data);
        change.lsn = lsn;
        change.origin_id = origin_id;
        change.txn = txn;

        let cid = self.alloc_change_slot(change);
        let mut list = self.txn(txn).changes;
        crate::dl_push_tail(&mut self.changes, &mut list, cid, |c| &mut c.node);
        self.txn_mut(txn).changes = list;
        self.txn_mut(txn).nentries_mem += 1;

        // Account the restored change: it is released through the same
        // accounting later, and the counters must not underflow then.
        let sz = self.change_size(cid);
        self.change_memory_update(Some(cid), None, true, sz);
        Ok(())
    }

    // ReorderBufferRestoreCleanup (reorderbuffer.c:4820): unlink every
    // possible segment file of the transaction.
    pub(crate) fn restore_cleanup(&mut self, txn: TxnId) -> PgResult<()> {
        debug_assert!(self.txn(txn).first_lsn != InvalidXLogRecPtr);
        debug_assert!(self.txn(txn).final_lsn != InvalidXLogRecPtr);

        let seg_size = transam_xlog_seams::wal_segment_size::call() as u64;
        let first = self.txn(txn).first_lsn / seg_size;
        let last = self.txn(txn).final_lsn / seg_size;
        let xid = self.txn(txn).xid;

        for segno in first..=last {
            let path = serialized_path(&self.slot_name, xid, segno);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(rb_error(format!(
                        "could not remove file \"{}\": {e}",
                        path.display()
                    )))
                }
            }
        }
        Ok(())
    }
}

// FileRead-loop shape: fill `buf` unless EOF arrives first; short only at EOF.
fn read_full(f: &mut File, buf: &mut [u8]) -> PgResult<usize> {
    let mut off = 0usize;
    while off < buf.len() {
        match f.read(&mut buf[off..]) {
            Ok(0) => break,
            Ok(n) => off += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(rb_error(format!(
                    "could not read from reorderbuffer spill file: {e}"
                )))
            }
        }
    }
    Ok(off)
}
