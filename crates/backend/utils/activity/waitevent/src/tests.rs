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
    super::pgstat_get_wait_event_type(0x0B00_0000);
}
