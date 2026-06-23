//! Tests for the base-backup throttling sink.
//!
//! The sink calls four seamed primitives: `get_current_timestamp` and
//! `check_for_interrupts` (this crate's own seams) and `wait_latch_my_latch` /
//! `reset_latch_my_latch` (the latch unit's seams). The harness installs them
//! against a simulated, controllable clock so the throttling loop terminates
//! deterministically, and serializes tests on a mutex (the seam slots are
//! process globals).

extern crate std;

use super::*;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, MutexGuard, Once};

use ::sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_begin_manifest,
    bbsink_cleanup, bbsink_end_archive, bbsink_end_backup, bbsink_end_manifest,
    bbsink_manifest_contents,
};
use ::mcx::MemoryContext;
use ::types_core::primitive::BLCKSZ;

// --- Simulated clock and seam bookkeeping. ---

/// Current simulated time in microseconds (the `GetCurrentTimestamp` value).
static NOW: AtomicI64 = AtomicI64::new(0);
/// How many microseconds each `wait_latch_my_latch` advances the clock.
static WAIT_ADVANCE: AtomicI64 = AtomicI64::new(0);
/// Count of `wait_latch_my_latch` calls observed.
static WAIT_CALLS: AtomicI64 = AtomicI64::new(0);
/// Last timeout (ms) passed to `wait_latch_my_latch`.
static LAST_TIMEOUT_MS: AtomicI64 = AtomicI64::new(-999);
/// Count of `reset_latch_my_latch` calls.
static RESET_CALLS: AtomicI64 = AtomicI64::new(0);
/// Count of `check_for_interrupts` calls.
static CFI_CALLS: AtomicI64 = AtomicI64::new(0);

static SERIALIZE: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

fn install_seams() {
    INSTALL.call_once(|| {
        get_current_timestamp::set(|| Ok(NOW.load(Ordering::SeqCst)));
        check_for_interrupts::set(|| {
            CFI_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        reset_latch_my_latch::set(|| {
            RESET_CALLS.fetch_add(1, Ordering::SeqCst);
        });
        // The wait advances the simulated clock by WAIT_ADVANCE and returns a
        // WL_TIMEOUT event (the normal "slept long enough" exit). The throttle
        // loop checks elapsed against the minimum after the advance.
        wait_latch_my_latch::set(|_events, timeout, _wei| {
            WAIT_CALLS.fetch_add(1, Ordering::SeqCst);
            LAST_TIMEOUT_MS.store(timeout, Ordering::SeqCst);
            NOW.fetch_add(WAIT_ADVANCE.load(Ordering::SeqCst), Ordering::SeqCst);
            Ok(WL_TIMEOUT)
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

// --- A leaf sink that records callbacks and owns the buffer. ---

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
    // maxrate = 1024 KB/s -> sample = 1024*1024/8 = 131072 bytes;
    // elapsed_min_unit = 1_000_000 / 8 = 125_000 us.
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
    // No throttling happened during begin_backup.
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
    // maxrate 1024 -> sample 131072. A single BLCKSZ (8192) is below sample.
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
    // Clock does not advance on its own; each wait advances it past the
    // minimum so the loop runs exactly once then exits via WL_TIMEOUT.
    WAIT_ADVANCE.store(1_000_000, Ordering::SeqCst);
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    // maxrate 8 KB/s -> sample = 8*1024/8 = 1024 bytes; elapsed_min_unit=125000.
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 8);

    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    // Push 2048 bytes (>= 2 * sample of 1024). counter becomes 2048, which is
    // >= sample, so it sleeps. elapsed_min = 125000 * (2048/1024) = 250000 us;
    // timeout passed = sleep/1000 ms. Since now==0 and throttled_last was set
    // in begin_backup (also 0 here), elapsed=0, sleep=250000 us -> 250 ms.
    bbsink_archive_contents(&mut sink, &mut st, 2048).unwrap();

    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(RESET_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(LAST_TIMEOUT_MS.load(Ordering::SeqCst), 250);
    // CFI: once before the wait, once after WL_LATCH_SET... but we returned
    // only WL_TIMEOUT, so just the pre-wait check_for_interrupts fired.
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
    // sample = 1024. Send 1536 (= sample + 512). After throttle, counter %=
    // sample leaves 512. A subsequent 256 byte chunk brings counter to 768,
    // still below sample, so no second wait.
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 8);
    let mut st = BbsinkState::default();
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();

    bbsink_archive_contents(&mut sink, &mut st, 1536).unwrap();
    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 1);

    bbsink_archive_contents(&mut sink, &mut st, 256).unwrap();
    // 512 (carried) + 256 = 768 < 1024 -> no new wait.
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
    // None of these forward-only callbacks throttle.
    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 0);
}

#[test]
fn already_slow_enough_does_not_wait() {
    let _g = begin_test();
    let ctx = MemoryContext::new("throttle test");
    let mcx = ctx.mcx();
    let log = RefCell::new(Vec::new());
    // sample = 1024, elapsed_min_unit = 125000 us.
    let mut sink = bbsink_throttle_new(mcx, leaf(mcx, &log), 8);
    let mut st = BbsinkState::default();
    // begin_backup at NOW=0 records throttled_last=0.
    bbsink_begin_backup(&mut sink, &mut st, BLCKSZ).unwrap();
    // Advance the clock well past the minimum before sending the sample, so
    // elapsed >= elapsed_min and sleep <= 0 -> the loop breaks immediately.
    NOW.store(500_000, Ordering::SeqCst);

    bbsink_archive_contents(&mut sink, &mut st, 1024).unwrap();

    assert_eq!(WAIT_CALLS.load(Ordering::SeqCst), 0, "slow transfer must not sleep");
    assert!(log.borrow().contains(&Event::ArchiveContents(1024)));
}
