//! W1 of the parallel-writes ladder (scratchpad/night/parallel-writes-design.md
//! §4): receiver-level multi-insert buffering for the table-rewrite
//! DestReceivers (intorel/transientrel receive — CREATE TABLE AS, SELECT
//! INTO, and the matview datafill). Incoming rows are copied into a
//! receiver-owned slot pool and flushed through `table_multi_insert`
//! (one multi-insert WAL record per filled page) instead of one
//! `table_tuple_insert` per row.
//!
//! Donor shape: commands/copy from.rs (CopyMultiInsertBuffer): same slot
//! pool, same flush thresholds, same abort discipline (buffered-but-unflushed
//! copies are simply dropped on the statement's Err path — the receiver's
//! shutdown, which flushes, only runs on success, and the aborted xact kills
//! the flushed ones).
//!
//! This moves the SERIAL rewrite baseline; a leader-drain funnel feeding the
//! same DestReceiver inherits it through the unchanged receive_slot contract.
//!
//! Correctness surface vs the per-tuple path: none new — the session thread
//! still owns every write, same (xid, cid) stamps, same page-fill logic, so
//! heap contents are byte-comparable (page LSNs aside). The WAL record class
//! moves from per-row HEAP INSERT to HEAP2 MULTI_INSERT.
//!
//! FLIPPED ON (GL-W0-2 + GL-W1-1): `PGRUST_CTAS_MULTIINSERT=0|off` kills,
//! restoring per-tuple inserts. The composition ladder measured the buffer
//! eliminating the write stack's entire serial-loss class (GL-W0-2 letter)
//! on top of GL-W1-1's own 17-32% serial CTAS wins.

use core::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::mcx::Mcx;
use ::types_core::xact::CommandId;
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_slot::SlotData;

use crate::{BulkInsertStateData, TableAm, WriteMultiInsertBuffer};

// copyfrom.c MAX_BUFFERED_TUPLES / MAX_BUFFERED_BYTES (the byte budget here
// counts materialized tuple images, not input line bytes).
const MAX_BUFFERED_TUPLES: usize = 1000;
const MAX_BUFFERED_BYTES: usize = 65535;

/// `PGRUST_CTAS_MULTIINSERT` (DEFAULT ON since the GL-W0-2 flip; =0|off
/// kills): the W1 write-buffer gate.
/// 0 = unresolved (read env on first use), 1 = OFF, 2 = ON. AtomicU8 +
/// `_set_for_tests` per the contract R-KNOBS idiom so a unit corpus can A/B
/// both paths in one process.
static CTAS_MULTIINSERT: AtomicU8 = AtomicU8::new(0);

pub fn write_multi_insert_enabled() -> bool {
    match CTAS_MULTIINSERT.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = !std::env::var("PGRUST_CTAS_MULTIINSERT")
                .is_ok_and(|v| matches!(v.trim(), "0" | "off"));
            CTAS_MULTIINSERT.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus.
#[cfg(test)]
pub(crate) fn write_multi_insert_set_for_tests(on: bool) {
    CTAS_MULTIINSERT.store(if on { 2 } else { 1 }, Relaxed);
}

/// Arm the buffer for a rewrite target, or None (per-tuple path) when the
/// switch is off or the AM is not heap (pgrcolumnar keeps its own insert
/// shape; the rewrite receivers only ever target plain/matview heaps).
pub fn write_buffer_begin<'mcx>(rel: &Relation<'mcx>) -> Option<WriteMultiInsertBuffer<'mcx>> {
    if write_multi_insert_enabled() && matches!(TableAm::of(rel), Some(TableAm::Heap)) {
        Some(WriteMultiInsertBuffer::new())
    } else {
        None
    }
}

/// Buffer one incoming row (the receiver's receive_slot body). The source
/// slot is copied — the executor owns and recycles it — so the buffered image
/// lives in the pool slot until flush. Oversized rows bypass the buffer:
/// flush the older rows first (heap order stays arrival order), then
/// single-insert so `heap_insert` owns the toast walk for exactly the rows
/// the per-tuple path would toast.
pub fn write_buffer_receive<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: &mut WriteMultiInsertBuffer<'mcx>,
    slot: &mut SlotData<'mcx>,
    cid: CommandId,
    options: i32,
    mut bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    if buf.nused == buf.slots.len() {
        buf.slots.push(crate::table_slot_create(mcx, rel)?);
    }
    let idx = buf.nused;
    exectuples::exec_copy_slot(&mut buf.slots[idx], slot, mcx, mcx)?;
    let (len, external) = {
        let t = match &buf.slots[idx] {
            SlotData::BufferHeap(b) => b.base.tuple.as_ref(),
            SlotData::Heap(h) => h.tuple.as_ref(),
            _ => None,
        }
        .expect("heap-rel pool slot holds a tuple after exec_copy_slot");
        (t.t_len as usize, t.has_external())
    };
    if external || len > ::heapam::dml::TOAST_TUPLE_THRESHOLD {
        write_buffer_flush(mcx, rel, buf, cid, options, bistate.as_deref_mut())?;
        // idx's copy sits past the (now empty) buffered prefix; insert it
        // directly from its pool slot. The next receive reuses slot 0.
        return crate::table_tuple_insert(mcx, rel, &mut buf.slots[idx], cid, options, bistate);
    }
    buf.nused += 1;
    buf.bytes += len;
    if buf.nused >= MAX_BUFFERED_TUPLES || buf.bytes >= MAX_BUFFERED_BYTES {
        write_buffer_flush(mcx, rel, buf, cid, options, bistate)?;
    }
    Ok(())
}

/// Flush the buffered prefix through one `table_multi_insert` call. Also the
/// receiver-shutdown tail flush (runs before `table_finish_bulk_insert`,
/// success path only).
pub fn write_buffer_flush<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: &mut WriteMultiInsertBuffer<'mcx>,
    cid: CommandId,
    options: i32,
    bistate: Option<&mut BulkInsertStateData>,
) -> PgResult<()> {
    if buf.nused == 0 {
        return Ok(());
    }
    let mut refs: Vec<&mut SlotData<'mcx>> = buf.slots[..buf.nused].iter_mut().collect();
    crate::table_multi_insert(mcx, rel, &mut refs, cid, options, bistate)?;
    buf.nused = 0;
    buf.bytes = 0;
    Ok(())
}
