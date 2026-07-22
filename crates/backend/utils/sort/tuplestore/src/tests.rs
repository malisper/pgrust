use std::rc::Rc;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::*;

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("tuplestore-test")));
    m.mcx()
}

fn int4_desc(mcx: Mcx<'static>, natts: i32) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: 23,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn read_i32(slot: &mut SlotData<'_>) -> i32 {
    exectuples::slot_getallattrs(slot);
    assert!(!slot.base().tts_isnull[0]);
    slot.base().tts_values[0].as_i32()
}

fn put_i32(ts: &mut Tuplestore, desc: &TupleDescData<'_>, v: i32) {
    ts.putvalues(desc, &[Datum::from_i32(v)], &[false]).unwrap();
}

#[test]
fn putvalues_gettupleslot_roundtrip() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    for v in 0..100 {
        put_i32(&mut ts, &desc, v);
    }
    assert_eq!(ts.tuple_count(), 100);
    assert!(!ts.ateof());

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    for v in 0..100 {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(slot.base().is_empty());
    assert!(ts.ateof());
    ts.end();
}

#[test]
fn eof_reader_advances_with_writes() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    put_i32(&mut ts, &desc, 1);
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    // The active read pointer at EOF stays at EOF across puts (C API spec).
    put_i32(&mut ts, &desc, 2);
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
}

#[test]
fn puttupleslot_copies_out_of_virtual_slot() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 2);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    let mut vslot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    {
        let base = vslot.base_mut();
        base.tts_values[0] = Datum::from_i32(7);
        base.tts_values[1] = Datum::from_i32(9);
        base.tts_isnull[0] = false;
        base.tts_isnull[1] = false;
    }
    exectuples::exec_store_virtual_tuple(&mut vslot);
    ts.puttupleslot(&mut vslot, mcx).unwrap();
    exectuples::exec_clear_tuple(&mut vslot, mcx);

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    exectuples::slot_getallattrs(&mut slot);
    assert_eq!(slot.base().tts_values[0].as_i32(), 7);
    assert_eq!(slot.base().tts_values[1].as_i32(), 9);
}

#[test]
fn copy_true_survives_clear() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    put_i32(&mut ts, &desc, 42);
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, true, &mut slot, mcx).unwrap());
    ts.clear();
    assert_eq!(read_i32(&mut slot), 42);
    assert_eq!(ts.tuple_count(), 0);
    exectuples::exec_clear_tuple(&mut slot, mcx);
}

#[test]
fn clear_then_reuse_and_rescan() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    for v in 0..10 {
        put_i32(&mut ts, &desc, v);
    }
    ts.clear();
    put_i32(&mut ts, &desc, 99);
    assert_eq!(ts.tuple_count(), 1);
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 99);
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    ts.rescan().unwrap();
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 99);
}

#[test]
fn grow_memtuples_past_initial_size() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 4096);
    let n = (INITIAL_MEMTUPSIZE * 3) as i32;
    for v in 0..n {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    for v in 0..n {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
}

#[test]
fn backward_walks_to_start_then_none() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    for v in [1, 2, 3] {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    for v in [1, 2, 3] {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(ts.ateof());

    // C: backward after EOF re-returns the last tuple, then walks back.
    for v in [3, 2, 1] {
        assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert!(!ts.ateof());

    // Forward again from the start.
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 1);
}

#[test]
fn backward_before_eof_returns_tuple_before_last() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    for v in [1, 2, 3] {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 2);
    // Last returned was 2; backward yields the tuple before it.
    assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 1);
}

#[test]
fn backward_at_start_is_none_and_rescan_resets() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    put_i32(&mut ts, &desc, 7);
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    assert!(!ts.gettupleslot(false, false, &mut slot, mcx).unwrap());

    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    ts.rescan().unwrap();
    assert!(!ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 7);
}

#[test]
fn follower_read_pointer_replays_leader_fill() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    ts.set_eflags(EXEC_FLAG_REWIND);
    let follower = ts.alloc_read_pointer(EXEC_FLAG_REWIND);
    assert_eq!(follower, 1);

    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    // Leader (ptr 0) reads to EOF then fills; the ACTIVE eof pointer stays
    // at EOF across writes (why CteScanNext returns the subplan slot itself).
    put_i32(&mut ts, &desc, 10);
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 10);
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    put_i32(&mut ts, &desc, 20);
    put_i32(&mut ts, &desc, 30);
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());

    // Follower replays the whole store independently.
    ts.select_read_pointer(follower).unwrap();
    for v in [10, 20, 30] {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), v);
    }
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(ts.ateof());

    ts.rescan().unwrap();
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 10);

    ts.select_read_pointer(0).unwrap();
    assert!(ts.ateof());
}

#[test]
fn inactive_eof_pointer_advances_to_next_write() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    let follower = ts.alloc_read_pointer(EXEC_FLAG_REWIND);

    put_i32(&mut ts, &desc, 1);
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    ts.select_read_pointer(follower).unwrap();
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());

    // Follower hits EOF, becomes inactive, then a write lands: C spec says an
    // INACTIVE eof pointer un-eofs onto the new tuple.
    ts.select_read_pointer(0).unwrap();
    put_i32(&mut ts, &desc, 2);
    ts.select_read_pointer(follower).unwrap();
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 2);
}

#[test]
fn new_pointer_copies_pointer_zero_position() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, true, 64);
    for v in [1, 2, 3] {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    let p = ts.alloc_read_pointer(EXEC_FLAG_REWIND);
    ts.select_read_pointer(p).unwrap();
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 2);
}

#[test]
#[should_panic(expected = "too late to require new tuplestore eflags")]
fn late_eflags_increase_is_loud() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    put_i32(&mut ts, &desc, 1);
    let _ = ts.alloc_read_pointer(EXEC_FLAG_BACKWARD);
}

#[test]
fn hold_registry_roundtrip_and_staleness() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let h = hold::register(Tuplestore::begin_heap(false, true, 64));
    hold::with_store(h, |ts| put_i32(ts, &desc, 5));

    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    hold::with_store(h, |ts| {
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    });
    assert_eq!(read_i32(&mut slot), 5);
    exectuples::exec_clear_tuple(&mut slot, mcx);

    hold::end(h);
    hold::end(h); // double-end is a no-op, as C never double-frees a live ptr
    let stale = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        hold::with_store(h, |ts| ts.tuple_count())
    }));
    assert!(stale.is_err());
    hold::end(types_portal::TuplestoreHandle::NULL);
}
#[test]
fn putvalues_packs_varlena_short_form() {
    let mcx = leaked_mcx();
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: 25,
        attlen: -1,
        attbyval: false,
        attalign: TYPALIGN_INT,
        attstorage: b'x' as i8,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    let desc = Rc::new(TupleDescData {
        natts: 1, tdtypeid: 2249, tdtypmod: -1, tdrefcount: -1,
        constr: None, compact_attrs: compact, attrs,
    });

    let mut image: Vec<u8> = vec![];
    let payload = b"4MB";
    let hdr = ((payload.len() + 4) as u32) << 2;
    image.extend_from_slice(&hdr.to_ne_bytes());
    image.extend_from_slice(payload);
    let image = Box::leak(image.into_boxed_slice());
    let d = Datum::from_usize(image.as_ptr() as usize);

    let mtup = heaptuple::heap_form_minimal_tuple(mcx, &desc, &[d], &[false], 0).unwrap();
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    exectuples::exec_store_minimal_tuple_owned(&mut slot, mcx, mtup);
    exectuples::slot_getallattrs(&mut slot);
    // heap_form packs the 4B-header input to the 1B short form (C fill_val).
    let out = slot.base().tts_values[0];
    let p = out.as_usize() as *const u8;
    let b0 = unsafe { *p };
    assert_eq!(b0 & 0x01, 1, "short-form varlena header");
    assert_eq!((b0 >> 1) as usize, 1 + payload.len());
    let data = unsafe { std::slice::from_raw_parts(p.add(1), payload.len()) };
    assert_eq!(data, payload);
}

#[test]
fn skiptuples_and_advance_window_navigation() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(false, true, 64);
    ts.set_eflags(0);
    let rp = ts.alloc_read_pointer(::types_slot::EXEC_FLAG_BACKWARD);
    for v in 0..10 {
        put_i32(&mut ts, &desc, v);
    }
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));

    ts.select_read_pointer(rp).unwrap();
    assert!(ts.skiptuples(4, true).unwrap());
    assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 4);

    // seekpos == pos refetch shape: advance forward, then read backward.
    assert!(ts.advance(true).unwrap());
    assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 4);

    assert!(ts.skiptuples(3, false).unwrap());
    assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 0);

    // Forward skip past EOF fails and leaves the pointer at EOF.
    assert!(!ts.skiptuples(50, true).unwrap());
    assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
    // Backward from EOF: last tuple.
    assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
    assert_eq!(read_i32(&mut slot), 9);
    ts.end();
}

#[test]
fn get_stats_tracks_chunk_space_maximum_across_clear() {
    let mcx = leaked_mcx();
    let desc = int4_desc(mcx, 1);
    let mut ts = Tuplestore::begin_heap(true, false, 64);
    // Empty store: memtuples array only (2048 ptrs -> aset external chunk).
    let s0 = ts.get_stats();
    assert_eq!(s0.space_type.name(), "Memory");
    assert_eq!(s0.max_space, 2048 * 8 + 8);
    for i in 0..10 {
        ts.putvalues(&desc, &[Datum::from_i32(i)], &[false]).unwrap();
    }
    let s1 = ts.get_stats();
    // int4 minimal tuple: MAXALIGN(t_len) + 8-byte generation chunk header.
    assert!(s1.max_space > s0.max_space);
    ts.clear();
    // maxSpace survives clear (C tuplestore_updatemax in tuplestore_clear).
    assert_eq!(ts.get_stats().max_space, s1.max_space);
    ts.end();
}

mod spill {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Once;

    use super::*;

    static SETUP: Once = Once::new();
    static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);
    static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn enter_datadir(tag: &str) -> (std::sync::MutexGuard<'static, ()>, String) {
        let guard = CWD.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = format!(
            "{}/pgrust-tstorespill-{}-{}",
            std::env::temp_dir().display(),
            std::process::id(),
            tag
        );
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
        std::env::set_current_dir(&dir).unwrap();
        (guard, dir)
    }

    fn setup() {
        SETUP.call_once(|| {
            guc_tables::init_seams();
            elog::init_seams();
            fd::init_seams();
            xact_seams::get_current_sub_transaction_id::set(|| 1);
            aio_seams::pgaio_closing_fd::set(|_| {});
            aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
            waitevent_seams::pgstat_report_wait_start::set(|_| {});
            waitevent_seams::pgstat_report_wait_end::set(|| {});
            pgstat_seams::pgstat_report_tempfile::set(|_| {});
            ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
            ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
            resowner::init_seams();
            guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
                get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
                set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
            });
        });
        fd::InitFileAccess();
        let _ = fd::InitTemporaryFileAccess();
        if resowner_seams::current_resource_owner::call().is_null() {
            let owner =
                resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "spill-test")
                    .unwrap();
            resowner_seams::set_current_resource_owner::call(owner);
        }
    }

    fn temp_files(dir: &str) -> usize {
        std::fs::read_dir(format!("{dir}/base/pgsql_tmp"))
            .map(|d| d.count())
            .unwrap_or(0)
    }

    const N: i32 = 200_000; // ~200k 16B tuples >> 64KB work_mem

    #[test]
    fn spill_forward_roundtrip_and_stats() {
        setup();
        let (_cwd, dir) = enter_datadir("fwd");
        let mcx = leaked_mcx();
        let desc = int4_desc(mcx, 1);
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

        let mut ts = Tuplestore::begin_heap(false, false, 64);
        for v in 0..N {
            put_i32(&mut ts, &desc, v);
        }
        assert!(!ts.in_memory(), "200k tuples must spill at 64KB");
        assert!(temp_files(&dir) > 0);

        for v in 0..N {
            assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
            assert_eq!(read_i32(&mut slot), v);
        }
        assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert!(ts.ateof());

        let stats = ts.get_stats();
        assert!(matches!(
            stats.space_type,
            types_core::instrument::TuplesortSpaceType::Disk
        ));
        assert!(stats.max_space > 0);

        // Writes resume after reads (READFILE -> WRITEFILE switch); the
        // active pointer stays at EOF (C API spec), so replay via rescan.
        put_i32(&mut ts, &desc, N);
        assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        ts.rescan().unwrap();
        assert!(ts.skiptuples(i64::from(N), true).unwrap());
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), N);

        exectuples::exec_clear_tuple(&mut slot, mcx);
        ts.end();
        assert_eq!(temp_files(&dir), 0, "temp file must be removed at end");
    }

    #[test]
    fn spill_backward_and_rescan() {
        setup();
        let (_cwd, dir) = enter_datadir("back");
        let mcx = leaked_mcx();
        let desc = int4_desc(mcx, 1);
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

        let mut ts = Tuplestore::begin_heap(true, false, 64);
        for v in 0..N {
            put_i32(&mut ts, &desc, v);
        }
        assert!(!ts.in_memory());

        // Forward to EOF.
        let mut n = 0;
        while ts.gettupleslot(true, false, &mut slot, mcx).unwrap() {
            n += 1;
        }
        assert_eq!(n, N);

        // Backward walk: last tuple first, down to (not incl.) the first.
        let mut v = N - 1;
        while ts.gettupleslot(false, false, &mut slot, mcx).unwrap() {
            assert_eq!(read_i32(&mut slot), v);
            v -= 1;
        }
        assert_eq!(v, -1);

        ts.rescan().unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 0);

        // skiptuples across the file arms.
        assert!(ts.skiptuples(1000, true).unwrap());
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 1001);
        assert!(ts.skiptuples(2, false).unwrap());
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 1000);

        exectuples::exec_clear_tuple(&mut slot, mcx);
        ts.end();
        assert_eq!(temp_files(&dir), 0);
    }

    #[test]
    fn spill_two_read_pointers_and_copy() {
        setup();
        let (_cwd, _dir) = enter_datadir("ptrs");
        let mcx = leaked_mcx();
        let desc = int4_desc(mcx, 1);
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

        let mut ts = Tuplestore::begin_heap(true, false, 64);
        let follower = ts.alloc_read_pointer(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD);
        for v in 0..N {
            put_i32(&mut ts, &desc, v);
        }
        assert!(!ts.in_memory());

        // Pointer 0 reads ahead 10 tuples.
        for v in 0..10 {
            assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
            assert_eq!(read_i32(&mut slot), v);
        }
        // Follower still at the start.
        ts.select_read_pointer(follower).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 0);

        // copy_read_pointer: follower jumps to pointer 0's position.
        ts.copy_read_pointer(0, follower).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 10);

        // Back to pointer 0; it is unaffected.
        ts.select_read_pointer(0).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 10);

        exectuples::exec_clear_tuple(&mut slot, mcx);
        ts.end();
    }

    /// WAVE-2 WS-M INC-2 OPENER (integration contract §6 WS-M amendment 3):
    /// the SPILLED store in the WINDOW BUFFER'S EXACT CONFIGURATION —
    /// `begin_heap(false, ..)` + `set_eflags(0)` (emit pointer 0
    /// FORWARD-ONLY) + extra pointers allocated per nodewindowagg's
    /// `prepare_tuplestore`: BACKWARD-capable agg + per-func pointers,
    /// forward-only frame-head/tail pointers — five pointers at
    /// independent file positions, interleaved forward reads, mid-stream
    /// BACKWARD steps and backward `skiptuples` on the capable pointers,
    /// and EOF-then-backward (C tuplestore.c:1213-1227). This resolves the
    /// audit-note-vs-source contradiction the wave-2 contract adjudicated
    /// ("spill status — resolve by unit test at inc-2 start; audit note
    /// presumed stale"): the ported TSS_WRITEFILE/READFILE arms fully
    /// support the window's multi-read-pointer + backward configuration,
    /// so T2-B giant-partition work may proceed. Access patterns mirror
    /// the proven C-parity idioms of the three tests above; any failure
    /// here is a genuine source divergence, not fixture drift.
    #[test]
    fn spill_window_buffer_multi_pointer_backward_config() {
        setup();
        let (_cwd, dir) = enter_datadir("winbuf");
        let mcx = leaked_mcx();
        let desc = int4_desc(mcx, 1);
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

        // prepare_tuplestore's shape: non-random-access store, pointer 0
        // re-flagged forward-only, then the window's pointer family.
        let mut ts = Tuplestore::begin_heap(false, false, 64);
        ts.set_eflags(0);
        let agg = ts.alloc_read_pointer(EXEC_FLAG_BACKWARD);
        let func = ts.alloc_read_pointer(EXEC_FLAG_BACKWARD);
        let head = ts.alloc_read_pointer(0);
        let tail = ts.alloc_read_pointer(0);
        for v in 0..N {
            put_i32(&mut ts, &desc, v);
        }
        assert!(!ts.in_memory(), "200k tuples must spill at 64KB");
        assert!(temp_files(&dir) > 0);

        // Emit pointer (0) drains a prefix — the per-row emit walk.
        for v in 0..1_000 {
            assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
            assert_eq!(read_i32(&mut slot), v);
        }

        // The frame-tail pointer skips deep into the file; the frame-head
        // pointer advances a little — both forward-only, both positioned
        // independently of pointer 0.
        ts.select_read_pointer(tail).unwrap();
        assert!(ts.skiptuples(5_000, true).unwrap());
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 5_000);
        ts.select_read_pointer(head).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 0);

        // The agg pointer reads a frame forward, then BACKWARD-skips to
        // re-read it (the moving-frame restart cascade's access shape) —
        // mid-file, while three other pointers sit at distant positions.
        ts.select_read_pointer(agg).unwrap();
        assert!(ts.skiptuples(500, true).unwrap());
        for v in 500..510 {
            assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
            assert_eq!(read_i32(&mut slot), v);
        }
        assert!(ts.skiptuples(10, false).unwrap());
        for v in 500..510 {
            assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
            assert_eq!(read_i32(&mut slot), v);
        }
        // Single-step backward reads (tuplestore_gettuple(false)): each
        // returns the tuple before the last one returned.
        assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 508);
        assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 507);
        // Forward resumes from just after the last returned tuple.
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 508);

        // Pointer-switch seek dance: every pointer kept its own position.
        ts.select_read_pointer(0).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 1_000);
        ts.select_read_pointer(tail).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 5_001);
        ts.select_read_pointer(head).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 1);

        // The per-func pointer: forward to EOF then the first backward
        // step returns the LAST tuple (the C EOF special case).
        ts.select_read_pointer(func).unwrap();
        assert!(ts.skiptuples(i64::from(N), true).unwrap());
        assert!(!ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), N - 1);
        assert!(ts.gettupleslot(false, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), N - 2);

        // copy_read_pointer across spilled positions (the window mark
        // bookkeeping face): agg jumps to pointer 0's position.
        ts.copy_read_pointer(0, agg).unwrap();
        ts.select_read_pointer(agg).unwrap();
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 1_001);

        exectuples::exec_clear_tuple(&mut slot, mcx);
        ts.end();
        assert_eq!(temp_files(&dir), 0, "temp file must be removed at end");
    }

    #[test]
    fn spill_clear_returns_to_memory() {
        setup();
        let (_cwd, dir) = enter_datadir("clear");
        let mcx = leaked_mcx();
        let desc = int4_desc(mcx, 1);
        let mut slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));

        let mut ts = Tuplestore::begin_heap(false, false, 64);
        for v in 0..N {
            put_i32(&mut ts, &desc, v);
        }
        assert!(!ts.in_memory());
        ts.clear();
        assert!(ts.in_memory());
        assert_eq!(temp_files(&dir), 0, "clear must drop the temp file");
        assert_eq!(ts.tuple_count(), 0);

        put_i32(&mut ts, &desc, 7);
        assert!(ts.gettupleslot(true, false, &mut slot, mcx).unwrap());
        assert_eq!(read_i32(&mut slot), 7);

        // usedDisk sticks across clear (C contract).
        let stats = ts.get_stats();
        assert!(matches!(
            stats.space_type,
            types_core::instrument::TuplesortSpaceType::Disk
        ));

        exectuples::exec_clear_tuple(&mut slot, mcx);
        ts.end();
    }
}
