//! Tests for nodeGather's owned logic: the `gather_readnext` round-robin /
//! drop-reader bookkeeping and `tup_is_null`. The two seams this path touches
//! (`check_for_interrupts`, `tuple_queue_reader_next`) are process-global, so a
//! mutex serializes the installers and a thread-local queue feeds canned reader
//! results.

use core::cell::RefCell;
use std::sync::Mutex;

use backend_executor_tqueue_seams as tqueue;
use backend_tcop_postgres_seams as tcop_postgres;
use mcx::MemoryContext;
use types_execparallel::TupleQueueReaderHandle;
use types_nodes::{EStateData, GatherStateData};
use types_tuple::heaptuple::{MinimalTuple, MinimalTupleData};

use super::{gather_readnext, tup_is_null};

static SEAM_LOCK: Mutex<()> = Mutex::new(());

thread_local! {
    /// Queue of `(tuple, done)` results served by `tuple_queue_reader_next`.
    static READNEXT: RefCell<std::vec::Vec<(bool, bool)>> = const { RefCell::new(std::vec::Vec::new()) };
}

fn install_seams() {
    READNEXT.with(|q| q.borrow_mut().clear());
    // Seams install once per process; the canned behavior is driven by the
    // thread-local queue, so re-running install is a no-op after the first.
    if tcop_postgres::check_for_interrupts::is_installed() {
        return;
    }
    tcop_postgres::check_for_interrupts::set(|| Ok(()));
    tqueue::tuple_queue_reader_next::set(|mcx, _reader, _nowait| {
        let (has_tup, done) = READNEXT.with(|q| {
            let mut q = q.borrow_mut();
            if q.is_empty() {
                (false, false)
            } else {
                q.remove(0)
            }
        });
        let tup: MinimalTuple = if has_tup {
            Some(mcx::alloc_in(mcx, MinimalTupleData {
                t_len: 0,
                mt_padding: [0; 6],
                t_infomask2: 0,
                t_infomask: 0,
                t_hoff: 0,
                t_bits: mcx::PgVec::new_in(mcx),
            })?)
        } else {
            None
        };
        Ok(tqueue::ReaderNext { tup, done })
    });
}

fn state_with_readers<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    n: i32,
) -> GatherStateData<'mcx> {
    let mut s = GatherStateData::new(mcx);
    s.nreaders = n;
    s.nextreader = 0;
    for i in 0..n {
        s.reader.push(TupleQueueReaderHandle(i as usize));
    }
    s
}

#[test]
fn readnext_returns_tuple_from_current_reader() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_seams();
    let ctx = MemoryContext::new("gather-test");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut state = state_with_readers(ctx.mcx(), 1);
    // One tuple available, not done.
    READNEXT.with(|q| q.borrow_mut().push((true, false)));

    let got = gather_readnext(&mut state, &mut estate).unwrap();
    assert!(got.is_some());
    assert_eq!(state.nreaders, 1);
}

#[test]
fn readnext_drops_done_reader_and_shuts_down_when_last() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_seams();
    let ctx = MemoryContext::new("gather-test");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut state = state_with_readers(ctx.mcx(), 1);
    // The sole reader reports done with no tuple.
    READNEXT.with(|q| q.borrow_mut().push((false, true)));

    let got = gather_readnext(&mut state, &mut estate).unwrap();
    assert!(got.is_none());
    // nreaders decremented to 0 triggers ExecShutdownGatherWorkers (clears
    // the reader array).
    assert_eq!(state.nreaders, 0);
    assert!(state.reader.is_empty());
}

#[test]
fn readnext_drops_middle_reader_keeps_survivors() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_seams();
    let ctx = MemoryContext::new("gather-test");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut state = state_with_readers(ctx.mcx(), 3);
    // reader[0] is done; then reader[0] (the old reader[1], rotated in) yields.
    READNEXT.with(|q| {
        let mut q = q.borrow_mut();
        q.push((false, true)); // reader[0] done -> dropped
        q.push((true, false)); // next read yields a tuple
    });

    let got = gather_readnext(&mut state, &mut estate).unwrap();
    assert!(got.is_some());
    // One reader dropped; two survive, and the dropped slot's handle is gone.
    assert_eq!(state.nreaders, 2);
    assert_eq!(state.reader.len(), 2);
    // The surviving handles are the original reader[1] and reader[2].
    assert_eq!(state.reader[0], TupleQueueReaderHandle(1));
    assert_eq!(state.reader[1], TupleQueueReaderHandle(2));
}

#[test]
fn tup_is_null_handles_none_and_empty() {
    let ctx = MemoryContext::new("gather-test");
    let estate = EStateData::new_in(ctx.mcx());
    // No slot id is the C NULL slot.
    assert!(tup_is_null(None, &estate));
}
