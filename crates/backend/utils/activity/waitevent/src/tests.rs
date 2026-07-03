use core::sync::atomic::{AtomicU32, Ordering::Relaxed};

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
