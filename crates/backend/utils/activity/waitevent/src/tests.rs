use core::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::Once;

// WaitEventCustomNew/GetWaitEventCustomIdentifier take the real
// WAIT_EVENT_CUSTOM_LOCK; the process-global LWLock array must exist first
// (predicate::tests's minimal CreateLWLocks fixture).
fn setup_lwlocks() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        lwlock::CreateLWLocks(false).unwrap();
    });
}

#[test]
fn report_wait_start_writes_registered_slot_and_end_clears() {
    static SLOT: AtomicU32 = AtomicU32::new(7);
    super::pgstat_report_wait_start(42); // no storage: write sinks
    super::pgstat_set_wait_event_storage(&SLOT);
    super::pgstat_report_wait_start(42);
    assert_eq!(SLOT.load(Relaxed), 42);
    super::pgstat_report_wait_end();
    assert_eq!(SLOT.load(Relaxed), 0);
    super::pgstat_reset_wait_event_storage();
    super::pgstat_report_wait_start(9);
    assert_eq!(SLOT.load(Relaxed), 0);
}

#[test]
fn wait_event_type_decodes_classes() {
    use super::*;
    assert_eq!(pgstat_get_wait_event_type(0), None);
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_LWLOCK | 4), Some("LWLock"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_LOCK | 0), Some("Lock"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_BUFFERPIN), Some("BufferPin"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_ACTIVITY + 17), Some("Activity"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_CLIENT), Some("Client"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_EXTENSION), Some("Extension"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_IPC + 8), Some("IPC"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_TIMEOUT + 1), Some("Timeout"));
    assert_eq!(pgstat_get_wait_event_type(PG_WAIT_IO + 50), Some("IO"));
    assert_eq!(
        pgstat_get_wait_event_type(PG_WAIT_INJECTIONPOINT),
        Some("InjectionPoint")
    );
}

#[test]
fn wait_event_decodes_known_constants() {
    use super::*;
    assert_eq!(pgstat_get_wait_event(0), None);
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY), Some("ArchiverMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 1), Some("AutovacuumMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 2), Some("BgwriterHibernate"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 3), Some("BgwriterMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 4), Some("CheckpointerMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 5), Some("CheckpointerShutdown"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 17), Some("WalWriterMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_CLIENT), Some("ClientRead"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_CLIENT + 1), Some("ClientWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 8), Some("BufferIo"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 11), Some("CheckpointDone"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 12), Some("CheckpointStart"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 56), Some("XactGroupUpdate"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_TIMEOUT + 1), Some("CheckpointWriteDelay"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_TIMEOUT + 9), Some("WalSummarizerError"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 1), Some("AioIoUringExecution"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 2), Some("AioIoUringSubmit"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 7), Some("BuffileTruncate"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 8), Some("BuffileWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 40), Some("RelationMapRead"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 42), Some("RelationMapWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 50), Some("SlruFlushSync"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 53), Some("SlruWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 80), Some("WalWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_BUFFERPIN), Some("BufferPin"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_EXTENSION), Some("Extension"));
}

#[test]
#[should_panic(expected = "unknown wait event")]
fn wait_event_unknown_event_id_panics() {
    super::pgstat_get_wait_event(super::PG_WAIT_ACTIVITY + 18);
}

#[test]
#[should_panic(expected = "unknown wait event class")]
fn wait_event_type_unknown_class_panics() {
    super::pgstat_get_wait_event_type(0x0C00_0000);
}

// A single test: WAIT_EVENT_CUSTOM_LOCK is a real process-global LWLock
// (CreateLWLocks, not a stub), so two of these run in parallel test threads
// would genuinely contend it — the queued-waiter path needs PGPROC/latch
// machinery this crate's tests don't set up. One sequential test never
// contends itself.
#[test]
fn custom_wait_events_register_resolve_and_collide() {
    setup_lwlocks();
    let _ = super::custom::WaitEventCustomShmemInit();

    let ext = super::custom::WaitEventExtensionNew("my_ext_wait").unwrap();
    assert_eq!(ext & super::WAIT_EVENT_CLASS_MASK, super::PG_WAIT_EXTENSION);
    assert_eq!(super::custom::GetWaitEventCustomIdentifier(ext), "my_ext_wait");
    assert_eq!(super::pgstat_get_wait_event(ext), Some("my_ext_wait"));

    // Re-registering the same name returns the same info, not a new id.
    let ext2 = super::custom::WaitEventExtensionNew("my_ext_wait").unwrap();
    assert_eq!(ext, ext2);

    let inj = super::custom::WaitEventInjectionPointNew("my_inj_point").unwrap();
    assert_eq!(inj & super::WAIT_EVENT_CLASS_MASK, super::PG_WAIT_INJECTIONPOINT);
    assert_eq!(super::custom::GetWaitEventCustomIdentifier(inj), "my_inj_point");

    let ext_names = super::custom::GetWaitEventCustomNames(super::PG_WAIT_EXTENSION);
    assert!(ext_names.iter().any(|n| n == "my_ext_wait"));
    let inj_names = super::custom::GetWaitEventCustomNames(super::PG_WAIT_INJECTIONPOINT);
    assert!(inj_names.iter().any(|n| n == "my_inj_point"));

    // Same name, different class -> ERRCODE_DUPLICATE_OBJECT.
    super::custom::WaitEventExtensionNew("shared_name_for_collision_test").unwrap();
    assert!(super::custom::WaitEventInjectionPointNew("shared_name_for_collision_test").is_err());

    // Registration must work past C's init size of 16 (grows to 128 in C).
    for i in 0..30 {
        super::custom::WaitEventExtensionNew(&format!("bulk_event_{i}")).unwrap();
    }
}

#[test]
fn wait_event_funcs_data_has_273_rows_across_9_classes() {
    let rows: Vec<_> = super::funcs::WAIT_EVENT_FUNCS_DATA.lines().collect();
    assert_eq!(rows.len(), 273);
    let mut classes = std::collections::BTreeSet::new();
    for row in &rows {
        let mut parts = row.splitn(3, '\t');
        classes.insert(parts.next().unwrap());
        assert!(parts.next().is_some());
        assert!(parts.next().is_some());
    }
    assert_eq!(classes.len(), 9);
}
