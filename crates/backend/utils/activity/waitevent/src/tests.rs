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
    assert_eq!(pgstat_get_wait_event(0).unwrap(), None);
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY).unwrap(), Some("ArchiverMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 1).unwrap(), Some("AutovacuumMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 2).unwrap(), Some("BgwriterHibernate"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 3).unwrap(), Some("BgwriterMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 4).unwrap(), Some("CheckpointerMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 5).unwrap(), Some("CheckpointerShutdown"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_ACTIVITY + 17).unwrap(), Some("WalWriterMain"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_CLIENT).unwrap(), Some("ClientRead"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_CLIENT + 1).unwrap(), Some("ClientWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 8).unwrap(), Some("BufferIo"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 11).unwrap(), Some("CheckpointDone"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 12).unwrap(), Some("CheckpointStart"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 56).unwrap(), Some("XactGroupUpdate"));
    // upstream 33101632235a (18.6): the ABI_compatibility row.
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 57).unwrap(), Some("WalReceiverUpstreamCatchup"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IPC + 58).unwrap(), Some("FlushPipeline"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_TIMEOUT + 1).unwrap(), Some("CheckpointWriteDelay"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_TIMEOUT + 9).unwrap(), Some("WalSummarizerError"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 1).unwrap(), Some("AioIoUringExecution"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 2).unwrap(), Some("AioIoUringSubmit"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 7).unwrap(), Some("BuffileTruncate"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 8).unwrap(), Some("BuffileWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 40).unwrap(), Some("RelationMapRead"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 42).unwrap(), Some("RelationMapWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 50).unwrap(), Some("SlruFlushSync"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 53).unwrap(), Some("SlruWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_IO + 80).unwrap(), Some("WalWrite"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_BUFFERPIN).unwrap(), Some("BufferPin"));
    assert_eq!(pgstat_get_wait_event(PG_WAIT_EXTENSION).unwrap(), Some("Extension"));
}

// C-parity fallbacks (wait_event.c + generated pgstat_wait_event.c): C never
// errors on unknown inputs — out-of-range ids and unknown classes take the
// switch defaults. The previous panics here were ported-in constraints.
#[test]
fn wait_event_unknown_inputs_take_c_defaults() {
    // Out-of-range event id within a known class.
    assert_eq!(
        super::pgstat_get_wait_event(super::PG_WAIT_ACTIVITY + 18).unwrap(),
        Some("unknown wait event")
    );
    // Bits 16-23 set: C's generated switch compares the FULL value, so this
    // is NOT "BgwriterMain" even though the low 16 bits index a valid entry.
    assert_eq!(
        super::pgstat_get_wait_event(super::PG_WAIT_ACTIVITY | 0x0001_0003).unwrap(),
        Some("unknown wait event")
    );
    // Unknown class.
    assert_eq!(super::pgstat_get_wait_event(0x0C00_0000).unwrap(), Some("unknown wait event"));
    assert_eq!(super::pgstat_get_wait_event_type(0x0C00_0000), Some("???"));
    // Lock arm ignores bits 16-23 (C masks eventId before the lmgr lookup)
    // and returns "???" past LOCKTAG_LAST_TYPE.
    assert_eq!(
        super::pgstat_get_wait_event(super::PG_WAIT_LOCK | 0x0001_0004).unwrap(),
        Some("tuple")
    );
    assert_eq!(super::pgstat_get_wait_event(super::PG_WAIT_LOCK + 12).unwrap(), Some("???"));
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

    // audit-18.6 w2-014: both tables are ShmemInitHash creations
    // (wait_event.c:139/149) registered under their names with
    // hash_get_shared_size (shmem.c:353-363) for WAIT_EVENT_CUSTOM_HASH_MAX_SIZE
    // = 128 entries, so pg_shmem_allocations lists them with C's sizes; C
    // attaches by name and size (shmem.c:428-456), the probe used here.
    {
        use dynahash::{hash_get_shared_size, hash_select_dirsize};
        use types_hash::hsearch::{HASHCTL, HASH_DIRSIZE};
        let mut info = HASHCTL::new();
        info.dsize = hash_select_dirsize(128);
        info.max_dsize = info.dsize;
        let size = hash_get_shared_size(&info, HASH_DIRSIZE);
        let (_, found) =
            shmem::ShmemInitStruct("WaitEventCustom hash by wait event information", size)
                .unwrap();
        assert!(found, "the by-info table is not registered in the ShmemIndex");
        let (_, found) = shmem::ShmemInitStruct("WaitEventCustom hash by name", size).unwrap();
        assert!(found, "the by-name table is not registered in the ShmemIndex");
    }

    let ext = super::custom::WaitEventExtensionNew("my_ext_wait").unwrap();
    assert_eq!(ext & super::WAIT_EVENT_CLASS_MASK, super::PG_WAIT_EXTENSION);
    assert_eq!(super::custom::GetWaitEventCustomIdentifier(ext).unwrap(), "my_ext_wait");
    assert_eq!(super::pgstat_get_wait_event(ext).unwrap(), Some("my_ext_wait"));

    // Re-registering the same name returns the same info, not a new id.
    let ext2 = super::custom::WaitEventExtensionNew("my_ext_wait").unwrap();
    assert_eq!(ext, ext2);

    let inj = super::custom::WaitEventInjectionPointNew("my_inj_point").unwrap();
    assert_eq!(inj & super::WAIT_EVENT_CLASS_MASK, super::PG_WAIT_INJECTIONPOINT);
    assert_eq!(super::custom::GetWaitEventCustomIdentifier(inj).unwrap(), "my_inj_point");

    let ext_names = super::custom::GetWaitEventCustomNames(super::PG_WAIT_EXTENSION);
    assert!(ext_names.iter().any(|n| n == "my_ext_wait"));
    let inj_names = super::custom::GetWaitEventCustomNames(super::PG_WAIT_INJECTIONPOINT);
    assert!(inj_names.iter().any(|n| n == "my_inj_point"));

    // audit-18.6 b091: an unregistered custom id is C's elog(ERROR)
    // "could not find custom name for wait event information %u"
    // (wait_event.c:293), a recoverable error — never a panic.
    for bogus in [super::PG_WAIT_EXTENSION | 0x7f, super::PG_WAIT_INJECTIONPOINT | 0x7f] {
        let r = std::panic::catch_unwind(|| super::pgstat_get_wait_event(bogus));
        let err = r
            .unwrap_or_else(|_| panic!("unregistered custom wait event {bogus:#x} panicked"))
            .unwrap_err();
        assert_eq!(err.sqlstate, types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(
            err.message,
            format!("could not find custom name for wait event information {bogus}")
        );
    }

    // Same name, different class -> ERRCODE_DUPLICATE_OBJECT.
    super::custom::WaitEventExtensionNew("shared_name_for_collision_test").unwrap();
    assert!(super::custom::WaitEventInjectionPointNew("shared_name_for_collision_test").is_err());

    // Registration must work past C's init size of 16 (grows to 128 in C).
    for i in 0..30 {
        super::custom::WaitEventExtensionNew(&format!("bulk_event_{i}")).unwrap();
    }
}

// upstream 33101632235a (18.6): +1 IPC row (WalReceiverUpstreamCatchup).
#[test]
fn wait_event_funcs_data_has_274_rows_across_9_classes() {
    let rows: Vec<_> = super::funcs::WAIT_EVENT_FUNCS_DATA.lines().collect();
    assert_eq!(rows.len(), 274);
    let mut classes = std::collections::BTreeSet::new();
    for row in &rows {
        let mut parts = row.splitn(3, '\t');
        classes.insert(parts.next().unwrap());
        assert!(parts.next().is_some());
        assert!(parts.next().is_some());
    }
    assert_eq!(classes.len(), 9);
}
