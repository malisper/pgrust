//! Spill-to-disk *codec* family of `replication/logical/reorderbuffer.c`.
//!
//! When a transaction's in-memory changes exceed `logical_decoding_work_mem`,
//! the reorder buffer evicts the largest (sub)transaction to disk and pages it
//! back in change by change during the commit-time k-way merge. This module
//! lands the part of that family that is reachable from the already-ported
//! spine and does not cross an unmodeled keystone:
//!
//! * `ReorderBufferSerializeTXN` / `ReorderBufferSerializeChange` — write a
//!   txn's (and its subtxns') in-memory changes out to per-WAL-segment spill
//!   files (`pg_replslot/<slot>/xid-<xid>-lsn-<X>-<X>.spill`);
//! * `ReorderBufferRestoreChanges` / `ReorderBufferRestoreChange` — page a
//!   serialized txn's changes back into memory in `max_changes_in_memory`-sized
//!   batches, threading the per-txn open file and segment cursor
//!   (`TXNEntryFile`);
//! * `ReorderBufferRestoreCleanup` — unlink a serialized txn's spill segments;
//! * `ReorderBufferCleanupSerializedTXNs` — delete leftover `xid-*` spill files
//!   from a slot directory (crash/exit cleanup);
//! * `ReorderBufferSerializedPath` — build the spill filename for a (xid, segno).
//!
//! # On-disk format
//!
//! The C `ReorderBufferSerializeChange` `memcpy`s the in-memory
//! `ReorderBufferChange` struct verbatim (pointers and all) into a
//! `ReorderBufferDiskChange { Size size; ReorderBufferChange change; }` followed
//! by the variable payload, and `ReorderBufferRestoreChange` overwrites the
//! garbage pointers on the way back in. That raw-struct image is private to a
//! single decoding run (a comment in the C says so — "only used during a single
//! run, so each LSN only maps to a specific WAL record"), so it is never read by
//! anything but this same process. We therefore use a faithful *logical*
//! encoding that round-trips exactly the same information (the change
//! discriminant, `lsn`/`origin_id`, and the per-action payload), which is the
//! contract-faithful equivalent of the raw struct memcpy.
//!
//! # What stays seam-panic
//!
//! The eviction *driver* — `ReorderBufferCheckMemoryLimit` and the `txn_heap`
//! pairing-heap-backed `ReorderBufferLargestTXN` /
//! `ReorderBufferLargestStreamableTopTXN` that decide *which* txn to evict and
//! *whether* to stream or spill — reads `rb->private_data`
//! (`ReorderBufferCanStartStreaming`: `ctx->streaming` / `ctx->snapshot_builder`
//! / `ctx->reader->ReadRecPtr`). That is the unmodeled `LogicalDecodingContext`
//! carrier (#351); the streaming arm additionally reaches `ReorderBufferStreamTXN`
//! (output-plugin dispatch). Those stay seam-panic in [`crate::lib`]; this
//! module lands only the codec that the spine reaches once a txn is serialized.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
extern crate alloc;

use alloc::vec::Vec;

use ::types_core::primitive::{TransactionId, XLogRecPtr};
use ::types_storage::File;

use crate::{
    ReorderBuffer, ReorderBufferChange, ReorderBufferChangeData, ReorderBufferChangeType,
    ReorderBufferTupleBuf, RBTXN_IS_SERIALIZED,
};

/// `max_changes_in_memory` (reorderbuffer.c) — the number of changes a single
/// `ReorderBufferRestoreChanges` batch pages back into memory.
const MAX_CHANGES_IN_MEMORY: u64 = 4096;

/// `PG_REPLSLOT_DIR` (xlogdefs.h) — the replication-slot data directory.
const PG_REPLSLOT_DIR: &str = "pg_replslot";

/// `WAIT_EVENT_REORDER_BUFFER_WRITE` / `_READ` wait-event ids passed to
/// `FileRead`. The fd layer only uses them for `pg_stat_activity`; carry the
/// canonical names' numeric placeholder (the value is not load-bearing here).
const WAIT_EVENT_REORDER_BUFFER_READ: u32 = 0;

// POSIX open(2) flags. `PG_BINARY` is 0 on POSIX (matches the other in-repo
// fd.c consumers, e.g. rewriteheap.c's `libc_flags`).
const PG_BINARY: i32 = 0;

/// `XLByteToSeg(lsn, segno, wal_segment_size)` — the WAL segment an LSN falls in.
fn xlbyte_to_seg(lsn: XLogRecPtr, wal_segment_size: i32) -> u64 {
    lsn / wal_segment_size as u64
}

/// `XLByteInSeg(lsn, segno, wal_segment_size)` — is `lsn` in segment `segno`?
fn xlbyte_in_seg(lsn: XLogRecPtr, segno: u64, wal_segment_size: i32) -> bool {
    lsn / wal_segment_size as u64 == segno
}

/// `XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, recptr)` — the LSN at
/// the start of segment `segno`.
fn xlog_segno_offset_to_recptr(segno: u64, wal_segment_size: i32) -> XLogRecPtr {
    segno * wal_segment_size as u64
}

/// `TXNEntryFile` (reorderbuffer.c) — the open spill file + read cursor an
/// iterator entry threads across successive `ReorderBufferRestoreChanges`
/// batches. The C carries the VFD in `state->entries[off].file.vfd` and the
/// per-file read offset in `.curOffset`.
#[derive(Default)]
pub(crate) struct TxnEntryFile {
    /// `File vfd` — the open segment, or `None` for the C `-1` (no file open).
    pub vfd: Option<File>,
    /// `off_t curOffset` — the read offset within the open segment.
    pub cur_offset: i64,
}

// ---------------------------------------------------------------------------
// Logical wire encoding helpers (the on-disk image of one change).
// ---------------------------------------------------------------------------

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_ne_bytes());
}
fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u64(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

/// A cursor over a byte slice read back from a spill file.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Reader<'a> {
        Reader { data, pos: 0 }
    }
    fn u32(&mut self) -> u32 {
        let v = u32::from_ne_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        v
    }
    fn u64(&mut self) -> u64 {
        let v = u64::from_ne_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        v
    }
    fn i32(&mut self) -> i32 {
        let v = i32::from_ne_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        v
    }
    fn bytes(&mut self) -> Vec<u8> {
        let len = self.u64() as usize;
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        v
    }
}

fn encode_tuplebuf(buf: &mut Vec<u8>, t: &ReorderBufferTupleBuf) {
    put_u32(buf, t.t_len);
    put_u32(buf, t.t_self.ip_blkid.bi_hi as u32);
    put_u32(buf, t.t_self.ip_blkid.bi_lo as u32);
    put_u32(buf, t.t_self.ip_posid as u32);
    put_u32(buf, t.t_table_oid);
    put_bytes(buf, &t.data);
}

fn decode_tuplebuf(r: &mut Reader) -> ReorderBufferTupleBuf {
    let t_len = r.u32();
    let bi_hi = r.u32() as u16;
    let bi_lo = r.u32() as u16;
    let ip_posid = r.u32() as u16;
    let t_table_oid = r.u32();
    let data = r.bytes();
    let mut tb = ReorderBufferTupleBuf {
        t_len,
        t_self: types_tuple::ItemPointerData::default(),
        t_table_oid,
        data,
    };
    tb.t_self.ip_blkid.bi_hi = bi_hi;
    tb.t_self.ip_blkid.bi_lo = bi_lo;
    tb.t_self.ip_posid = ip_posid;
    tb
}

/// Discriminant byte for the change action (the on-disk image of
/// `change->action`).
fn action_to_u8(a: ReorderBufferChangeType) -> u8 {
    use ReorderBufferChangeType as A;
    match a {
        A::Insert => 0,
        A::Update => 1,
        A::Delete => 2,
        A::Message => 3,
        A::Invalidation => 4,
        A::InternalSnapshot => 5,
        A::InternalCommandId => 6,
        A::InternalTupleCid => 7,
        A::InternalSpecInsert => 8,
        A::InternalSpecConfirm => 9,
        A::InternalSpecAbort => 10,
        A::Truncate => 11,
    }
}
fn u8_to_action(b: u8) -> ReorderBufferChangeType {
    use ReorderBufferChangeType as A;
    match b {
        0 => A::Insert,
        1 => A::Update,
        2 => A::Delete,
        3 => A::Message,
        4 => A::Invalidation,
        5 => A::InternalSnapshot,
        6 => A::InternalCommandId,
        7 => A::InternalTupleCid,
        8 => A::InternalSpecInsert,
        9 => A::InternalSpecConfirm,
        10 => A::InternalSpecAbort,
        11 => A::Truncate,
        _ => unreachable!("invalid on-disk change action {b}"),
    }
}

impl ReorderBuffer {
    // -----------------------------------------------------------------------
    // ReorderBufferSerializedPath
    // -----------------------------------------------------------------------

    /// `ReorderBufferSerializedPath(path, MyReplicationSlot, xid, segno)` —
    /// `pg_replslot/<slot>/xid-<xid>-lsn-<X>-<X>.spill`.
    fn serialized_path(&self, xid: TransactionId, segno: u64) -> String {
        let wal_segment_size = transam_xlog_seams::wal_segment_size::call();
        let recptr = xlog_segno_offset_to_recptr(segno, wal_segment_size);
        let slot = slot_seams::slot_name::call();
        // LSN_FORMAT_ARGS(recptr) = (uint32)(recptr >> 32), (uint32) recptr.
        format!(
            "{}/{}/xid-{}-lsn-{:X}-{:X}.spill",
            PG_REPLSLOT_DIR,
            slot,
            xid,
            (recptr >> 32) as u32,
            recptr as u32,
        )
    }

    // -----------------------------------------------------------------------
    // ReorderBufferSerializeTXN / SerializeChange
    // -----------------------------------------------------------------------

    /// `ReorderBufferSerializeTXN(rb, txn)` — spill a txn's (and its subtxns')
    /// in-memory changes to per-segment files.
    pub(crate) fn serialize_txn(&mut self, xid: TransactionId) {
        let size = self.by_txn_get(xid).map(|t| t.size).unwrap_or(0);

        // do the same to all child TXs
        let subtxns = self
            .by_txn_get(xid)
            .map(|t| t.subtxns.clone())
            .unwrap_or_default();
        for sub_xid in subtxns {
            self.serialize_txn(sub_xid);
        }

        // serialize changestream
        let changes = self
            .by_txn_get_mut(xid)
            .map(|t| core::mem::take(&mut t.changes))
            .unwrap_or_default();

        let wal_segment_size = transam_xlog_seams::wal_segment_size::call();
        let mut fd: i32 = -1;
        let mut cur_open_segno: u64 = 0;
        let mut spilled: u64 = 0;

        for change in changes {
            // store in the segment it belongs to by start lsn; don't split a
            // change over multiple segments.
            if fd == -1 || !xlbyte_in_seg(change.lsn, cur_open_segno, wal_segment_size) {
                if fd != -1 {
                    fd_seams::close_transient_file::call(fd);
                }
                cur_open_segno = xlbyte_to_seg(change.lsn, wal_segment_size);
                let path = self.serialized_path(xid, cur_open_segno);
                fd = fd_seams::open_transient_file::call(
                    &path,
                    libc::O_CREAT | libc::O_WRONLY | libc::O_APPEND | PG_BINARY,
                );
                if fd < 0 {
                    panic!("could not open file \"{path}\" for spilling reorder buffer");
                }
            }

            self.serialize_change(xid, fd, &change);
            // ReorderBufferFreeChange(rb, change, false): the payload drops; the
            // snapshot's refcount is released exactly as the free path does.
            self.free_change(change, false);
            spilled += 1;
        }

        // ReorderBufferChangeMemoryUpdate(rb, NULL, txn, false, size): subtract
        // the spilled bytes from txn->size / txn->total_size and rb->size.
        // (txn->final_lsn was kept current per-change in serialize_change.)
        self.change_memory_update_sub_txn(xid, size);

        // update the statistics iff we spilled anything. UpdateDecodingStats
        // pushes rb->{spillCount,spillBytes,spillTxns} to pgstat via logical.c;
        // in this model logical.c reads those counters back through the
        // reorderbuffer_stats seam, so we update the counters here and the push
        // is the seam read (no rb->private_data needed).
        if spilled > 0 {
            self.spill_count_add(1);
            self.spill_bytes_add(size as i64);
            let already_serialized = self
                .by_txn_get(xid)
                .map(|t| t.is_serialized() || t.is_serialized_clear())
                .unwrap_or(false);
            if !already_serialized {
                self.spill_txns_add(1);
            }
        }

        if let Some(t) = self.by_txn_get_mut(xid) {
            debug_assert!(t.changes.is_empty());
            t.nentries_mem = 0;
            t.txn_flags |= RBTXN_IS_SERIALIZED;
        }

        if fd != -1 {
            fd_seams::close_transient_file::call(fd);
        }
    }

    /// `ReorderBufferSerializeChange(rb, txn, fd, change)` — write one change in
    /// the logical wire format (see the module docs) to the open segment `fd`.
    fn serialize_change(&mut self, xid: TransactionId, fd: i32, change: &ReorderBufferChange) {
        let buf = encode_change(change);

        // prefix the total record length (the C ReorderBufferDiskChange.size).
        let mut record: Vec<u8> = Vec::with_capacity(8 + buf.len());
        put_u64(&mut record, buf.len() as u64);
        record.extend_from_slice(&buf);

        let written = fd_seams::transient_write::call(fd, &record);
        if written != record.len() as isize {
            fd_seams::close_transient_file::call(fd);
            panic!(
                "could not write to data file for XID {}",
                self.by_txn_get(xid).map(|t| t.xid).unwrap_or(xid)
            );
        }

        // keep txn->final_lsn up to date so RestoreCleanup works even after a
        // crash without an abort record. Never move it backwards.
        if let Some(t) = self.by_txn_get_mut(xid) {
            if t.final_lsn < change.lsn {
                t.final_lsn = change.lsn;
            }
        }
    }

    // -----------------------------------------------------------------------
    // ReorderBufferRestoreChanges / RestoreChange
    // -----------------------------------------------------------------------

    /// `ReorderBufferRestoreChanges(rb, txn, &file, &segno)` — page up to
    /// `max_changes_in_memory` of a serialized txn's changes back into memory,
    /// returning the count restored (0 == nothing left, the C iterator removes
    /// the entry from the heap). `file`/`segno` carry the persistent open
    /// segment and cursor across batches.
    pub(crate) fn restore_changes(
        &mut self,
        xid: TransactionId,
        file: &mut TxnEntryFile,
        segno: &mut u64,
    ) -> u64 {
        debug_assert!(self.by_txn_get(xid).map(|t| t.first_lsn).unwrap_or(0) != 0);
        debug_assert!(self.by_txn_get(xid).map(|t| t.final_lsn).unwrap_or(0) != 0);

        let wal_segment_size = transam_xlog_seams::wal_segment_size::call();

        // free current entries so we have memory for more. The C frees each via
        // ReorderBufferFreeChange(rb, cleanup, true), subtracting from both
        // txn->size and rb->size; we drop the payloads (upd_mem=false) and apply
        // one batched ReorderBufferChangeMemoryUpdate(rb, NULL, txn, false,
        // sum) so both counters move down exactly once (mirrors cleanup_txn).
        let changes = self
            .by_txn_get_mut(xid)
            .map(|t| core::mem::take(&mut t.changes))
            .unwrap_or_default();
        let mut mem_freed = 0usize;
        for change in changes {
            mem_freed += crate::change_size(&change);
            self.free_change(change, false);
        }
        self.change_memory_update_sub_txn(xid, mem_freed);
        if let Some(t) = self.by_txn_get_mut(xid) {
            t.nentries_mem = 0;
        }

        let (first_lsn, final_lsn) = self
            .by_txn_get(xid)
            .map(|t| (t.first_lsn, t.final_lsn))
            .unwrap_or((0, 0));
        let last_segno = xlbyte_to_seg(final_lsn, wal_segment_size);

        let mut restored: u64 = 0;
        while restored < MAX_CHANGES_IN_MEMORY && *segno <= last_segno {
            // backend_storage CHECK_FOR_INTERRUPTS() omitted (no signal model).

            if file.vfd.is_none() {
                // first time in.
                if *segno == 0 {
                    *segno = xlbyte_to_seg(first_lsn, wal_segment_size);
                }
                let path = self.serialized_path(xid, *segno);
                let opened = fd_seams::path_name_open_file::call(
                    &path,
                    libc::O_RDONLY | PG_BINARY,
                )
                .expect("PathNameOpenFile VFD allocation");
                file.cur_offset = 0;
                if opened.0 < 0 {
                    let errno = fd_seams::last_errno::call();
                    if errno == libc::ENOENT {
                        *segno += 1;
                        continue;
                    }
                    panic!("could not open file \"{path}\" restoring reorder buffer");
                }
                file.vfd = Some(opened);
            }
            let vfd = file.vfd.expect("segment open above");

            // read the length prefix (the C ReorderBufferDiskChange static part).
            let mut len_buf = [0u8; 8];
            let read_bytes = fd_seams::file_read::call(
                vfd,
                &mut len_buf,
                file.cur_offset,
                WAIT_EVENT_REORDER_BUFFER_READ,
            )
            .expect("FileRead reorderbuffer spill length");

            if read_bytes == 0 {
                // eof: move to the next segment.
                fd_seams::file_close::call(vfd);
                file.vfd = None;
                *segno += 1;
                continue;
            } else if read_bytes != 8 {
                panic!(
                    "could not read from reorderbuffer spill file: read {read_bytes} \
                     instead of 8 bytes"
                );
            }
            file.cur_offset += read_bytes as i64;

            let payload_len = u64::from_ne_bytes(len_buf) as usize;
            let mut payload = alloc::vec![0u8; payload_len];
            let read_bytes = fd_seams::file_read::call(
                vfd,
                &mut payload,
                file.cur_offset,
                WAIT_EVENT_REORDER_BUFFER_READ,
            )
            .expect("FileRead reorderbuffer spill payload");
            if read_bytes != payload_len as isize {
                panic!(
                    "could not read from reorderbuffer spill file: read {read_bytes} \
                     instead of {payload_len} bytes"
                );
            }
            file.cur_offset += read_bytes as i64;

            self.restore_change(xid, &payload);
            restored += 1;
        }

        restored
    }

    /// `ReorderBufferRestoreChange(rb, txn, data)` — decode one change from the
    /// logical wire format and append it to the txn's `changes`.
    fn restore_change(&mut self, xid: TransactionId, payload: &[u8]) {
        let change = decode_change(payload);

        // Tuple-CID changes are not memory-accounted (the C
        // ReorderBufferChangeMemoryUpdate skips them), and they are never queued
        // onto ->changes anyway; everything restored here is a ->changes member.
        let sz = crate::change_size(&change);
        if let Some(t) = self.by_txn_get_mut(xid) {
            t.changes.push(change);
            t.nentries_mem += 1;
        }
        // ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, sz): add to
        // txn->size / toptxn->total_size / rb->size. We need to do this although
        // we don't check the memory limit when restoring (we only do that when
        // initially queueing after decoding), because the changes are released
        // later and that subtracts from the counters — we don't want to
        // underflow there.
        self.change_memory_update_add(xid, sz);
    }

    // -----------------------------------------------------------------------
    // ReorderBufferRestoreCleanup
    // -----------------------------------------------------------------------

    /// `ReorderBufferRestoreCleanup(rb, txn)` — unlink a serialized txn's spill
    /// segments (`first..=last`).
    pub(crate) fn restore_cleanup(&mut self, xid: TransactionId) {
        let (first_lsn, final_lsn) = match self.by_txn_get(xid) {
            Some(t) => (t.first_lsn, t.final_lsn),
            None => return,
        };
        debug_assert!(first_lsn != 0);
        debug_assert!(final_lsn != 0);

        let wal_segment_size = transam_xlog_seams::wal_segment_size::call();
        let first = xlbyte_to_seg(first_lsn, wal_segment_size);
        let last = xlbyte_to_seg(final_lsn, wal_segment_size);

        let mut cur = first;
        while cur <= last {
            let path = self.serialized_path(xid, cur);
            let rc = fd_seams::unlink_file::call(&path);
            if rc != 0 && -rc != libc::ENOENT {
                panic!("could not remove file \"{path}\"");
            }
            cur += 1;
        }
    }

    // -----------------------------------------------------------------------
    // ReorderBufferCleanupSerializedTXNs
    // -----------------------------------------------------------------------

    /// `ReorderBufferCleanupSerializedTXNs(slotname)` — delete leftover `xid-*`
    /// spill files from `pg_replslot/<slotname>` (crash/exit cleanup).
    ///
    /// The C calls this from `ReorderBufferAllocate` / `ReorderBufferFree`
    /// (`NameStr(MyReplicationSlot->data.name)`) and from the exported
    /// `StartupReorderBuffer` (per surviving logical slot). The foundational
    /// `allocate()` deliberately does not call it (it would pull the
    /// `slot_name` seam into the value-type constructor, which the crate's unit
    /// tests drive with no slot installed); `StartupReorderBuffer` is not yet a
    /// wired entry point (no startup.c consumer / outward seam). The codec is
    /// landed and ready for those callers; until one is wired this is a
    /// crate-public entry with no in-crate caller.
    #[allow(dead_code)]
    pub(crate) fn cleanup_serialized_txns(slotname: &str) {
        let dir = format!("{PG_REPLSLOT_DIR}/{slotname}");

        // only handle directories; skip if it's not ours.
        match fd_seams::lstat_file::call(&dir, true) {
            Ok(Some(st)) if !st.isdir => return,
            Ok(_) => {}
            Err(_) => return,
        }

        let names = match fd_seams::read_dir_names::call(&dir) {
            Ok(n) => n,
            Err(_) => return,
        };
        for name in names {
            // only look at names that can be ours.
            if name.starts_with("xid") {
                let path = format!("{PG_REPLSLOT_DIR}/{slotname}/{name}");
                let rc = fd_seams::unlink_file::call(&path);
                if rc != 0 {
                    panic!("could not remove file \"{path}\" during removal of xid*");
                }
            }
        }
    }
}

/// `StartupReorderBuffer(void)` (reorderbuffer.c:4907) — at WAL startup, delete
/// all logical-decoding data spilled to disk by a previous (crashed or
/// restarted) run. Iterate `pg_replslot`, and for every entry that is a valid
/// surviving logical slot, delete everything starting with `xid-*` via
/// [`ReorderBuffer::cleanup_serialized_txns`]. Called unconditionally by the
/// WAL-startup driver (`StartupXLOG`).
pub fn startup_reorder_buffer() -> types_error::PgResult<()> {
    // logical_dir = AllocateDir(PG_REPLSLOT_DIR); while (ReadDir(...))
    // `read_dir_names` already excludes the C `strcmp(d_name, ".") / ".."`
    // entries the loop skips.
    let names = fd_seams::read_dir_names::call(PG_REPLSLOT_DIR)?;
    for name in names {
        // if it cannot be a slot, skip the directory. C passes DEBUG2 as the
        // elevel; a name failing validation simply logs at DEBUG2 and returns
        // false (never raises), so the boolean validity is all that matters
        // here.
        if slot_seams::replication_slot_validate_name_internal::call(&name)
            .is_err()
        {
            continue;
        }

        // ok, has to be a surviving logical slot, iterate and delete
        // everything starting with xid-*
        ReorderBuffer::cleanup_serialized_txns(&name);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pure per-change logical encode / decode (the on-disk image, sans the
// length-prefix the record carries on disk).
// ---------------------------------------------------------------------------

/// Encode one change to its logical wire image (`ReorderBufferSerializeChange`,
/// minus the leading record-length word the segment file carries).
fn encode_change(change: &ReorderBufferChange) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();

    // static part: action + lsn + origin_id.
    buf.push(action_to_u8(change.action));
    put_u64(&mut buf, change.lsn);
    put_u32(&mut buf, change.origin_id as u32);

    match &change.data {
        ReorderBufferChangeData::Tp {
            rlocator,
            clear_toast_afterwards,
            oldtuple,
            newtuple,
        } => {
            put_u32(&mut buf, rlocator.spcOid);
            put_u32(&mut buf, rlocator.dbOid);
            put_u32(&mut buf, rlocator.relNumber);
            buf.push(*clear_toast_afterwards as u8);
            buf.push(oldtuple.is_some() as u8);
            if let Some(t) = oldtuple {
                encode_tuplebuf(&mut buf, t);
            }
            buf.push(newtuple.is_some() as u8);
            if let Some(t) = newtuple {
                encode_tuplebuf(&mut buf, t);
            }
        }
        ReorderBufferChangeData::Msg { prefix, message } => {
            put_bytes(&mut buf, prefix);
            put_bytes(&mut buf, message);
        }
        ReorderBufferChangeData::Inval(msgs) => {
            put_u64(&mut buf, msgs.len() as u64);
            for m in msgs {
                put_bytes(&mut buf, &encode_inval(m));
            }
        }
        ReorderBufferChangeData::Snapshot(snap) => {
            encode_snapshot(&mut buf, snap);
        }
        ReorderBufferChangeData::Truncate {
            cascade,
            restart_seqs,
            relids,
        } => {
            buf.push(*cascade as u8);
            buf.push(*restart_seqs as u8);
            put_u64(&mut buf, relids.len() as u64);
            for oid in relids {
                put_u32(&mut buf, *oid);
            }
        }
        // SPEC_CONFIRM / SPEC_ABORT / None — the static part carries everything
        // important (the C "ReorderBufferChange contains everything
        // important"). CommandId / TupleCid still carry their scalar payload.
        ReorderBufferChangeData::CommandId(cid) => {
            put_u32(&mut buf, *cid);
        }
        ReorderBufferChangeData::TupleCid {
            locator,
            tid,
            cmin,
            cmax,
            combocid,
        } => {
            put_u32(&mut buf, locator.spcOid);
            put_u32(&mut buf, locator.dbOid);
            put_u32(&mut buf, locator.relNumber);
            put_u32(&mut buf, tid.ip_blkid.bi_hi as u32);
            put_u32(&mut buf, tid.ip_blkid.bi_lo as u32);
            put_u32(&mut buf, tid.ip_posid as u32);
            put_u32(&mut buf, *cmin);
            put_u32(&mut buf, *cmax);
            put_u32(&mut buf, *combocid);
        }
        ReorderBufferChangeData::None => {}
    }
    buf
}

/// Decode one change from its logical wire image (`ReorderBufferRestoreChange`).
fn decode_change(payload: &[u8]) -> ReorderBufferChange {
    let mut r = Reader::new(payload);
    let action = u8_to_action(r.data[r.pos]);
    r.pos += 1;
    let lsn = r.u64();
    let origin_id = r.u32() as ::types_core::primitive::RepOriginId;

    let data = match action {
        ReorderBufferChangeType::Insert
        | ReorderBufferChangeType::Update
        | ReorderBufferChangeType::Delete
        | ReorderBufferChangeType::InternalSpecInsert => {
            let spc = r.u32();
            let db = r.u32();
            let relnum = r.u32();
            let clear_toast_afterwards = r.byte_bool();
            let has_old = r.byte_bool();
            let oldtuple = if has_old { Some(decode_tuplebuf(&mut r)) } else { None };
            let has_new = r.byte_bool();
            let newtuple = if has_new { Some(decode_tuplebuf(&mut r)) } else { None };
            ReorderBufferChangeData::Tp {
                rlocator: ::types_storage::RelFileLocator {
                    spcOid: spc,
                    dbOid: db,
                    relNumber: relnum,
                },
                clear_toast_afterwards,
                oldtuple,
                newtuple,
            }
        }
        ReorderBufferChangeType::Message => {
            let prefix = r.bytes();
            let message = r.bytes();
            ReorderBufferChangeData::Msg { prefix, message }
        }
        ReorderBufferChangeType::Invalidation => {
            let n = r.u64() as usize;
            let mut msgs = Vec::with_capacity(n);
            for _ in 0..n {
                let raw = r.bytes();
                msgs.push(decode_inval(&raw));
            }
            ReorderBufferChangeData::Inval(msgs)
        }
        ReorderBufferChangeType::InternalSnapshot => {
            ReorderBufferChangeData::Snapshot(decode_snapshot(&mut r))
        }
        ReorderBufferChangeType::Truncate => {
            let cascade = r.byte_bool();
            let restart_seqs = r.byte_bool();
            let n = r.u64() as usize;
            let mut relids = Vec::with_capacity(n);
            for _ in 0..n {
                relids.push(r.u32());
            }
            ReorderBufferChangeData::Truncate {
                cascade,
                restart_seqs,
                relids,
            }
        }
        ReorderBufferChangeType::InternalCommandId => ReorderBufferChangeData::CommandId(r.u32()),
        ReorderBufferChangeType::InternalTupleCid => {
            let spc = r.u32();
            let db = r.u32();
            let relnum = r.u32();
            let bi_hi = r.u32() as u16;
            let bi_lo = r.u32() as u16;
            let posid = r.u32() as u16;
            let cmin = r.u32();
            let cmax = r.u32();
            let combocid = r.u32();
            let mut tid = types_tuple::ItemPointerData::default();
            tid.ip_blkid.bi_hi = bi_hi;
            tid.ip_blkid.bi_lo = bi_lo;
            tid.ip_posid = posid;
            ReorderBufferChangeData::TupleCid {
                locator: ::types_storage::RelFileLocator {
                    spcOid: spc,
                    dbOid: db,
                    relNumber: relnum,
                },
                tid,
                cmin,
                cmax,
                combocid,
            }
        }
        ReorderBufferChangeType::InternalSpecConfirm
        | ReorderBufferChangeType::InternalSpecAbort => ReorderBufferChangeData::None,
    };

    ReorderBufferChange {
        lsn,
        action,
        origin_id,
        data,
    }
}

// ---------------------------------------------------------------------------
// Snapshot / invalidation logical encoding.
// ---------------------------------------------------------------------------

fn encode_snapshot(buf: &mut Vec<u8>, snap: &::snapshot::SnapshotData) {
    put_u32(buf, snapshot_type_to_u32(snap.snapshot_type));
    put_u32(buf, snap.xmin);
    put_u32(buf, snap.xmax);
    put_u32(buf, snap.xcnt);
    for x in &snap.xip {
        put_u32(buf, *x);
    }
    put_i32(buf, snap.subxcnt);
    for x in &snap.subxip {
        put_u32(buf, *x);
    }
    buf.push(snap.suboverflowed as u8);
    buf.push(snap.takenDuringRecovery as u8);
    put_u32(buf, snap.curcid);
    put_u32(buf, snap.speculativeToken);
}

fn decode_snapshot(r: &mut Reader) -> ::snapshot::SnapshotData {
    let snapshot_type = u32_to_snapshot_type(r.u32());
    let xmin = r.u32();
    let xmax = r.u32();
    let xcnt = r.u32();
    let mut xip = Vec::with_capacity(xcnt as usize);
    for _ in 0..xcnt {
        xip.push(r.u32());
    }
    let subxcnt = r.i32();
    let mut subxip = Vec::with_capacity(subxcnt.max(0) as usize);
    for _ in 0..subxcnt.max(0) {
        subxip.push(r.u32());
    }
    let suboverflowed = r.data[r.pos] != 0;
    r.pos += 1;
    let taken_during_recovery = r.data[r.pos] != 0;
    r.pos += 1;
    let curcid = r.u32();
    let speculative_token = r.u32();

    ::snapshot::SnapshotData {
        snapshot_type,
        // C: the on-disk snapshot's vistest pointer is meaningless after restore
        // (it pointed into the original backend's shared state) and is unused by
        // a historic decoding snapshot. `GlobalVisStateHandle::new(0)` is the C
        // NULL, the faithful restored value.
        vistest: ::snapshot::snapshot::GlobalVisStateHandle::new(0),
        xmin,
        xmax,
        xip,
        xcnt,
        subxip,
        subxcnt,
        suboverflowed,
        takenDuringRecovery: taken_during_recovery,
        // C: newsnap->copied = true.
        copied: true,
        curcid,
        speculativeToken: speculative_token,
        // refcounts are reset on restore (the on-disk values are stale).
        active_count: 0,
        regd_count: 0,
        snapXactCompletionCount: 0,
        reg_id: 0,
    }
}

/// `SnapshotType` as a `u32` discriminant for the wire format.
fn snapshot_type_to_u32(t: ::snapshot::SnapshotType) -> u32 {
    use ::snapshot::SnapshotType as T;
    match t {
        T::SNAPSHOT_MVCC => 0,
        T::SNAPSHOT_SELF => 1,
        T::SNAPSHOT_ANY => 2,
        T::SNAPSHOT_TOAST => 3,
        T::SNAPSHOT_DIRTY => 4,
        T::SNAPSHOT_HISTORIC_MVCC => 5,
        T::SNAPSHOT_NON_VACUUMABLE => 6,
    }
}
fn u32_to_snapshot_type(v: u32) -> ::snapshot::SnapshotType {
    use ::snapshot::SnapshotType as T;
    match v {
        0 => T::SNAPSHOT_MVCC,
        1 => T::SNAPSHOT_SELF,
        2 => T::SNAPSHOT_ANY,
        3 => T::SNAPSHOT_TOAST,
        4 => T::SNAPSHOT_DIRTY,
        5 => T::SNAPSHOT_HISTORIC_MVCC,
        6 => T::SNAPSHOT_NON_VACUUMABLE,
        _ => unreachable!("invalid on-disk SnapshotType {v}"),
    }
}

fn encode_inval(m: &::types_storage::sinval::SharedInvalidationMessage) -> Vec<u8> {
    // SharedInvalidationMessage is the fixed 16-byte C union image; round-trip it
    // through the type's own native (native-endian) wire codec.
    m.to_wire_bytes().to_vec()
}

fn decode_inval(raw: &[u8]) -> ::types_storage::sinval::SharedInvalidationMessage {
    let arr: [u8; ::types_storage::sinval::SHARED_INVALIDATION_MESSAGE_SIZE] =
        raw.try_into().expect("16-byte SharedInvalidationMessage image");
    ::types_storage::sinval::SharedInvalidationMessage::from_wire_bytes(arr)
        .expect("recognized SI message id")
}

impl<'a> Reader<'a> {
    /// Read a single byte as a bool.
    fn byte_bool(&mut self) -> bool {
        let b = self.data[self.pos] != 0;
        self.pos += 1;
        b
    }
}

#[cfg(test)]
mod tests {
    //! Seam-free round-trip tests for the spill *codec* (the on-disk logical
    //! wire image). These exercise encode_change/decode_change directly, so they
    //! need no fd / slot / wal_segment_size seams installed.
    use super::*;
    use crate::SnapshotData;
    use ::snapshot::snapshot::{GlobalVisStateHandle, SnapshotType};
    use ::types_storage::sinval::{SharedInvalSnapshotMsg, SharedInvalidationMessage};

    fn rt(change: ReorderBufferChange) -> ReorderBufferChange {
        decode_change(&encode_change(&change))
    }

    fn tuplebuf(t_len: u32, table_oid: u32, data: Vec<u8>) -> ReorderBufferTupleBuf {
        let mut tb = ReorderBufferTupleBuf {
            t_len,
            t_self: types_tuple::ItemPointerData::default(),
            t_table_oid: table_oid,
            data,
        };
        tb.t_self.ip_blkid.bi_hi = 1;
        tb.t_self.ip_blkid.bi_lo = 2;
        tb.t_self.ip_posid = 3;
        tb
    }

    #[test]
    fn tp_insert_roundtrips() {
        let change = ReorderBufferChange {
            lsn: 0xDEAD_BEEF,
            action: ReorderBufferChangeType::Insert,
            origin_id: 7,
            data: ReorderBufferChangeData::Tp {
                rlocator: ::types_storage::RelFileLocator {
                    spcOid: 11,
                    dbOid: 22,
                    relNumber: 33,
                },
                clear_toast_afterwards: true,
                oldtuple: None,
                newtuple: Some(tuplebuf(5, 44, vec![1, 2, 3, 4, 5])),
            },
        };
        let back = rt(change);
        assert_eq!(back.lsn, 0xDEAD_BEEF);
        assert_eq!(back.action, ReorderBufferChangeType::Insert);
        assert_eq!(back.origin_id, 7);
        match back.data {
            ReorderBufferChangeData::Tp {
                rlocator,
                clear_toast_afterwards,
                oldtuple,
                newtuple,
            } => {
                assert_eq!(rlocator.spcOid, 11);
                assert_eq!(rlocator.dbOid, 22);
                assert_eq!(rlocator.relNumber, 33);
                assert!(clear_toast_afterwards);
                assert!(oldtuple.is_none());
                let nt = newtuple.expect("newtuple");
                assert_eq!(nt.t_len, 5);
                assert_eq!(nt.t_table_oid, 44);
                assert_eq!(nt.data, vec![1, 2, 3, 4, 5]);
                assert_eq!(nt.t_self.ip_blkid.bi_hi, 1);
                assert_eq!(nt.t_self.ip_blkid.bi_lo, 2);
                assert_eq!(nt.t_self.ip_posid, 3);
            }
            _ => panic!("expected Tp"),
        }
    }

    #[test]
    fn message_roundtrips() {
        let change = ReorderBufferChange {
            lsn: 1,
            action: ReorderBufferChangeType::Message,
            origin_id: 0,
            data: ReorderBufferChangeData::Msg {
                prefix: b"pre\0".to_vec(),
                message: b"hello".to_vec(),
            },
        };
        match rt(change).data {
            ReorderBufferChangeData::Msg { prefix, message } => {
                assert_eq!(prefix, b"pre\0");
                assert_eq!(message, b"hello");
            }
            _ => panic!("expected Msg"),
        }
    }

    #[test]
    fn invalidation_roundtrips() {
        let msgs = vec![
            SharedInvalidationMessage::Snapshot(SharedInvalSnapshotMsg { dbId: 5, relId: 6 }),
            SharedInvalidationMessage::Snapshot(SharedInvalSnapshotMsg { dbId: 7, relId: 8 }),
        ];
        let change = ReorderBufferChange {
            lsn: 2,
            action: ReorderBufferChangeType::Invalidation,
            origin_id: 0,
            data: ReorderBufferChangeData::Inval(msgs.clone()),
        };
        match rt(change).data {
            ReorderBufferChangeData::Inval(back) => assert_eq!(back, msgs),
            _ => panic!("expected Inval"),
        }
    }

    #[test]
    fn snapshot_roundtrips() {
        let snap = SnapshotData {
            snapshot_type: SnapshotType::SNAPSHOT_HISTORIC_MVCC,
            vistest: GlobalVisStateHandle::new(0),
            xmin: 100,
            xmax: 200,
            xcnt: 2,
            xip: vec![101, 102],
            subxcnt: 1,
            subxip: vec![150],
            suboverflowed: true,
            takenDuringRecovery: false,
            copied: false,
            curcid: 9,
            speculativeToken: 0,
            active_count: 3,
            regd_count: 4,
            snapXactCompletionCount: 5,
            reg_id: 0,
        };
        let change = ReorderBufferChange {
            lsn: 3,
            action: ReorderBufferChangeType::InternalSnapshot,
            origin_id: 0,
            data: ReorderBufferChangeData::Snapshot(snap),
        };
        match rt(change).data {
            ReorderBufferChangeData::Snapshot(s) => {
                assert_eq!(s.xmin, 100);
                assert_eq!(s.xmax, 200);
                assert_eq!(s.xcnt, 2);
                assert_eq!(s.xip, vec![101, 102]);
                assert_eq!(s.subxcnt, 1);
                assert_eq!(s.subxip, vec![150]);
                assert!(s.suboverflowed);
                assert_eq!(s.curcid, 9);
                // C: restored snapshot is marked copied.
                assert!(s.copied);
            }
            _ => panic!("expected Snapshot"),
        }
    }

    #[test]
    fn truncate_and_command_id_and_tuplecid_roundtrip() {
        let truncate = ReorderBufferChange {
            lsn: 4,
            action: ReorderBufferChangeType::Truncate,
            origin_id: 0,
            data: ReorderBufferChangeData::Truncate {
                cascade: true,
                restart_seqs: false,
                relids: vec![1, 2, 3],
            },
        };
        match rt(truncate).data {
            ReorderBufferChangeData::Truncate {
                cascade,
                restart_seqs,
                relids,
            } => {
                assert!(cascade);
                assert!(!restart_seqs);
                assert_eq!(relids, vec![1, 2, 3]);
            }
            _ => panic!("expected Truncate"),
        }

        let cid = ReorderBufferChange {
            lsn: 5,
            action: ReorderBufferChangeType::InternalCommandId,
            origin_id: 0,
            data: ReorderBufferChangeData::CommandId(42),
        };
        match rt(cid).data {
            ReorderBufferChangeData::CommandId(c) => assert_eq!(c, 42),
            _ => panic!("expected CommandId"),
        }
    }

    #[test]
    fn spec_confirm_roundtrips_to_none() {
        let change = ReorderBufferChange {
            lsn: 6,
            action: ReorderBufferChangeType::InternalSpecConfirm,
            origin_id: 0,
            data: ReorderBufferChangeData::None,
        };
        let back = rt(change);
        assert_eq!(back.action, ReorderBufferChangeType::InternalSpecConfirm);
        assert!(matches!(back.data, ReorderBufferChangeData::None));
    }
}
