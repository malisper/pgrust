use super::*;
use core::cell::Cell;

extern crate std;

// mcx's acct pool is single-threaded (one backend = one thread); serialize.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static M: std::sync::Mutex<()> = std::sync::Mutex::new(());
    M.lock().unwrap_or_else(|p| p.into_inner())
}

fn root() -> MemoryContext {
    MemoryContext::new("test-root")
}

#[test]
fn create_free_executor_state() {
    let _serial = serial();
    let parent = root();
    let estate = create_executor_state(&parent).unwrap();
    estate.with(|es| {
        assert_eq!(es.es_direction, ScanDirection::ForwardScanDirection);
        assert!(es.es_snapshot.is_none());
        assert!(es.es_plannedstmt.is_none());
        assert_eq!(es.es_processed, 0);
        assert!(!es.es_finished);
        assert_eq!(es.es_range_table_size, 0);
    });
    free_executor_state(estate);
}

#[test]
fn per_tuple_reset_is_wholesale() {
    let _serial = serial();
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let id = es.create_expr_context();
        // Reset is wholesale: per-tuple usage is reclaimed and the context
        // returns to the same baseline every iteration (keeper stays charged
        // across bump reset, per mcx's C mem_allocated contract).
        let mut baseline = None;
        for _ in 0..3 {
            {
                let mcx = es.ecxt(id).per_tuple_mcx();
                let v = mcx::vec_from_elem_in(mcx, 7u8, 4096);
                assert_eq!(v.len(), 4096);
                core::mem::forget(v);
            }
            let used_before = es.ecxt(id).per_tuple_mcx().context().used();
            assert!(used_before >= 4096);
            es.reset_expr_context(id);
            let used_after = es.ecxt(id).per_tuple_mcx().context().used();
            assert!(used_after <= used_before);
            match baseline {
                None => baseline = Some(used_after),
                Some(b) => assert_eq!(used_after, b),
            }
        }
    });
    free_executor_state(estate);
}

#[test]
fn per_tuple_exprcontext_memoized() {
    let _serial = serial();
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let a = es.get_per_tuple_expr_context();
        let b = es.get_per_tuple_expr_context();
        assert_eq!(a, b);
        let _ = es.get_per_tuple_memory();
        es.reset_per_tuple_expr_context();
    });
    free_executor_state(estate);
}

std::thread_local! {
    static FIRED: Cell<u64> = const { Cell::new(0) };
}

fn cb_a(_m: Mcx<'_>, arg: Datum) {
    FIRED.with(|f| f.set(f.get() * 10 + arg.as_usize() as u64));
}

fn cb_b(_m: Mcx<'_>, arg: Datum) {
    FIRED.with(|f| f.set(f.get() * 10 + arg.as_usize() as u64));
}

#[test]
fn shutdown_callbacks_fire_newest_first() {
    let _serial = serial();
    FIRED.with(|f| f.set(0));
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let id = es.create_expr_context();
        let e = es.ecxt_mut(id);
        e.register_shutdown_callback(cb_a, Datum::from_usize(1));
        e.register_shutdown_callback(cb_b, Datum::from_usize(2));
        e.rescan();
        assert_eq!(FIRED.with(|f| f.get()), 21);
        e.rescan();
        assert_eq!(FIRED.with(|f| f.get()), 21);
    });
    free_executor_state(estate);
}

#[test]
fn unregister_removes_matching() {
    let _serial = serial();
    FIRED.with(|f| f.set(0));
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let id = es.create_expr_context();
        let e = es.ecxt_mut(id);
        e.register_shutdown_callback(cb_a, Datum::from_usize(1));
        e.register_shutdown_callback(cb_a, Datum::from_usize(2));
        e.register_shutdown_callback(cb_a, Datum::from_usize(1));
        e.unregister_shutdown_callback(cb_a, Datum::from_usize(1));
        e.shutdown(true);
        assert_eq!(FIRED.with(|f| f.get()), 2);
    });
    free_executor_state(estate);
}

#[test]
fn abort_path_skips_callbacks() {
    let _serial = serial();
    FIRED.with(|f| f.set(0));
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let id = es.create_expr_context();
        es.ecxt_mut(id)
            .register_shutdown_callback(cb_a, Datum::from_usize(9));
        es.free_expr_context(id, false);
    });
    assert_eq!(FIRED.with(|f| f.get()), 0);
    free_executor_state(estate);
}

#[test]
fn free_executor_state_fires_remaining_reverse() {
    let _serial = serial();
    FIRED.with(|f| f.set(0));
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let a = es.create_expr_context();
        let b = es.create_expr_context();
        es.ecxt_mut(a)
            .register_shutdown_callback(cb_a, Datum::from_usize(1));
        es.ecxt_mut(b)
            .register_shutdown_callback(cb_a, Datum::from_usize(2));
    });
    free_executor_state(estate);
    assert_eq!(FIRED.with(|f| f.get()), 21);
}

#[test]
fn tuple_table_init_and_reset() {
    let _serial = serial();
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let att = types_tuple::FormData_pg_attribute {
            attnum: 1,
            attlen: 4,
            attbyval: true,
            attalign: types_tuple::TYPALIGN_INT,
            attstorage: types_tuple::TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        let mcx = es.es_query_cxt;
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        compact.push(types_tuple::CompactAttribute::populate_from(&att));
        attrs.push(att);
        let desc = Rc::new(types_tuple::TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        });
        let id = es.exec_init_extra_tuple_slot(Some(desc), TupleSlotKind::Virtual);
        assert!(es.slot(id).base().is_empty());
        exectuples::exec_store_virtual_tuple(es.slot_mut(id));
        assert!(!es.slot(id).base().is_empty());
        es.exec_reset_tuple_table(false);
        assert_eq!(es.es_tupleTable.len(), 1);
        assert!(es.slot(id).base().is_empty());
        es.exec_reset_tuple_table(true);
        assert!(es.es_tupleTable.is_empty());
    });
    free_executor_state(estate);
}

#[test]
fn work_and_standalone_expr_contexts() {
    let _serial = serial();
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    estate.with_mut(|es| {
        let id = es.create_work_expr_context();
        es.reset_expr_context(id);
    });
    free_executor_state(estate);

    FIRED.with(|f| f.set(0));
    let caller = root();
    let mut standalone = create_standalone_expr_context(caller.mcx());
    standalone.register_shutdown_callback(cb_a, Datum::from_usize(5));
    standalone.rescan();
    assert_eq!(FIRED.with(|f| f.get()), 5);
}

#[test]
fn errposition_paths() {
    let _serial = serial();
    let parent = root();
    let mut estate = create_executor_state(&parent).unwrap();
    assert_eq!(executor_errposition(None, 3), 0);
    estate.with_mut(|es| {
        assert_eq!(executor_errposition(Some(es), -1), 0);
        assert_eq!(executor_errposition(Some(es), 3), 0);
    });
    free_executor_state(estate);
}
