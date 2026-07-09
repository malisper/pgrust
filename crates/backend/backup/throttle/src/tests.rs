//! Tests for the throttling sink (seams driven against a simulated clock).

use super::*;
use core::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, MutexGuard, Once};

use ::sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_begin_manifest,
    bbsink_cleanup, bbsink_end_archive, bbsink_end_backup, bbsink_end_manifest,
    bbsink_manifest_contents,
};
use ::mcx::MemoryContext;
use ::types_core::BLCKSZ;

static NOW: AtomicI64 = AtomicI64::new(0);
static WAIT_ADVANCE: AtomicI64 = AtomicI64::new(0);
static WAIT_CALLS: AtomicI64 = AtomicI64::new(0);
static LAST_TIMEOUT_MS: AtomicI64 = AtomicI64::new(-999);
static RESET_CALLS: AtomicI64 = AtomicI64::new(0);
static CFI_CALLS: AtomicI64 = AtomicI64::new(0);

static SERIALIZE: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

fn install_seams() {
    INSTALL.call_once(|| {
        get_current_timestamp::set(|| NOW.load(Ordering::SeqCst));
        check_for_interrupts::set(|| {
            CFI_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        reset_latch_my_latch::set(|| {
            RESET_CALLS.fetch_add(1, Ordering::SeqCst);
        });
        wait_latch_my_latch::set(|_events, timeout, _wei| {
            WAIT_CALLS.fetch_add(1, Ordering::SeqCst);
            LAST_TIMEOUT_MS.store(timeout, Ordering::SeqCst);
            NOW.fetch_add(WAIT_ADVANCE.load(Ordering::SeqCst), Ordering::SeqCst);
            WL_TIMEOUT
        });
    });
}

fn begin_test() -> MutexGuard<'static, ()> {
    let guard = SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
    install_seams();
    NOW.store(0, Ordering::SeqCst);
    WAIT_ADVANCE.store(0, Ordering::SeqCst);
    WAIT_CALLS.store(0, Ordering::SeqCst);
    LAST_TIMEOUT_MS.store(-999, Ordering::SeqCst);
    RESET_CALLS.store(0, Ordering::SeqCst);
    CFI_CALLS.store(0, Ordering::SeqCst);
    guard
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    BeginBackup,
    BeginArchive(String),
    ArchiveContents(Size),
    EndArchive,
    BeginManifest,
    ManifestContents(Size),
    EndManifest,
    EndBackup(XLogRecPtr, TimeLineID),
    Cleanup,
}

struct RecordingOps<'a, 'mcx> {
    log: &'a RefCell<Vec<Event>>,
    mcx: Mcx<'mcx>,
}

impl<'a, 'mcx> BbsinkOps<'mcx> for RecordingOps<'a, 'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginBackup);
        sink.set_buffer(self.mcx, BLCKSZ)
    }
    fn begin_archive(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        name: &str,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginArchive(name.to_string()));
        Ok(())
    }
    fn archive_contents(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::ArchiveContents(len));
        Ok(())
    }
    fn end_archive(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::EndArchive);
        Ok(())
    }
    fn begin_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::BeginManifest);
        Ok(())
    }
    fn manifest_contents(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::ManifestContents(len));
        Ok(())
    }
    fn end_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::EndManifest);
        Ok(())
    }
    fn end_backup(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()> {
        self.log.borrow_mut().push(Event::EndBackup(endptr, endtli));
        Ok(())
    }
    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        self.log.borrow_mut().push(Event::Cleanup);
        sink.clear_buffer(self.mcx);
        Ok(())
    }
}

fn leaf<'a, 'mcx>(mcx: Mcx<'mcx>, log: &'a RefCell<Vec<Event>>) -> Box<Bbsink<'mcx>>
where
    'a: 'mcx,
{
    Box::new(Bbsink::new(mcx, Box::new(RecordingOps { log, mcx }), None))
}

#[test]
fn new_computes_sample_and_unit() {
    let throttle = BbsinkThrottle {
        throttling_sample: ((1024i64) * 1024 / THROTTLING_FREQUENCY) as u64,
        throttling_counter: 0,
        elapsed_min_unit: USECS_PER_SEC / THROTTLING_FREQUENCY,
        throttled_last: 0,
    };
    assert_eq!(throttle.throttling_sample, 131_072);
    assert_eq!(throttle.elapsed_min_unit, 125_000);
}

#[test]
fn begin_backup_records_time_and_forwards() {
    let _g = begin_test();
    NOW.store(42_000, Ordering::SeqCst);
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 1024);

    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    assert!(log.borrow().contains(&Event::BeginBackup));
    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 0);

    bbsink_cleanup(&mut sink, &mut st).unwrap();
    assert_eq!(log.borrow().last(), Some(&Event::Cleanup));
}

#[test]
fn sub_sample_increment_does_not_wait() {
    let _g = begin_test();
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 1024);

    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    bbsink_archive_contents(&mut sink, &mut st, BLCKSZ).unwrap();

    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 0, "below-sample must not sleep");
    assert!(log.borrow().contains(&Event::ArchiveContents(BLCKSZ)));

    bbsink_end_backup(&mut sink, &mut st, 99, 7).unwrap();
    assert!(log.borrow().contains(&Event::EndBackup(99, 7)));
}

#[test]
fn crossing_sample_sleeps_when_too_fast() {
    let _g = begin_test();
    WAIT_ADVANCE.store(1_000_000, Ordering::SeqCst);
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 8);

    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    bbsink_archive_contents(&mut sink, &mut st, 2048).unwrap();

    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(RESET_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(LAST_TIMEOUT_MS.load(Ordering::SeqCst), 250);
    assert_eq!(CFI_CALLS.load(Ordering::SeqCst), 1);
    assert!(log.borrow().contains(&Event::ArchiveContents(2048)));

    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn counter_remainder_carries_over() {
    let _g = begin_test();
    WAIT_ADVANCE.store(1_000_000, Ordering::SeqCst);
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 8);
    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    bbsink_archive_contents(&mut sink, &mut st, 1536).unwrap();
    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 1);

    bbsink_archive_contents(&mut sink, &mut st, 256).unwrap();
    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 1, "remainder must carry over");

    bbsink_cleanup(&mut sink, &mut st).unwrap();
}

#[test]
fn manifest_contents_also_throttles_and_forwards() {
    let _g = begin_test();
    WAIT_ADVANCE.store(1_000_000, Ordering::SeqCst);
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 8);
    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    bbsink_begin_manifest(&mut sink, &mut st).unwrap();
    bbsink_manifest_contents(&mut sink, &mut st, 1024).unwrap();
    bbsink_end_manifest(&mut sink, &mut st).unwrap();

    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 1);
    let log = log.borrow();
    assert!(log.contains(&Event::BeginManifest));
    assert!(log.contains(&Event::ManifestContents(1024)));
    assert!(log.contains(&Event::EndManifest));
}

#[test]
fn pure_forward_callbacks_pass_through() {
    let _g = begin_test();
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 1024);
    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    bbsink_begin_archive(&mut sink, &mut st, "base.tar").unwrap();
    bbsink_end_archive(&mut sink, &mut st).unwrap();
    bbsink_end_backup(&mut sink, &mut st, 5, 3).unwrap();

    let log = log.borrow();
    assert!(log.contains(&Event::BeginArchive("base.tar".to_string())));
    assert!(log.contains(&Event::EndArchive));
    assert!(log.contains(&Event::EndBackup(5, 3)));
    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 0);
}

#[test]
fn already_slow_enough_does_not_wait() {
    let _g = begin_test();
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 8);
    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    NOW.store(500_000, Ordering::SeqCst);

    bbsink_archive_contents(&mut sink, &mut st, 1024).unwrap();

    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 0, "slow transfer must not sleep");
    assert!(log.borrow().contains(&Event::ArchiveContents(1024)));
}
