//! Shared test harness: installs every seam this crate calls (exactly once
//! per process — seam slots are `OnceLock`s) with dispatchers that read a
//! per-thread fixture [`Env`], so tests stay isolated even when the test
//! binary runs them on multiple threads. A process-global mutex serializes
//! tests because the seam slots are process globals.

use std::cell::{Cell, RefCell};
use std::ops::Deref;
use std::sync::{Mutex, MutexGuard, Once};

use types_core::TimestampTz;
use types_pgstat::activity_pgstat::{
    PgStatShared_Archiver, PgStatShared_Checkpointer, PgStat_ArchiverStats,
    PgStat_CheckpointerStats, PgStat_Kind,
};
use types_pgstat::backend_status::PgBackendStatus;
use types_pgstat::backend_utils_activity_pgstat_bgwriter::{
    PgStatShared_BgWriter, PgStat_BgWriterStats,
};
use types_storage::LWLockMode;

/// Per-thread fixture backing every seam dispatcher.
pub(crate) struct Env {
    // backend_status entry (MyBEEntry) mirror + external flags.
    pub present: Cell<bool>,
    pub track: Cell<bool>,
    pub parallel: Cell<bool>,
    pub beentry: RefCell<PgBackendStatus>,
    // libpq message capture for the parallel-worker branch.
    pub sent: Cell<Option<(i32, i64)>>,
    // pgStatLocal mirrors.
    pub archiver_shmem: RefCell<PgStatShared_Archiver>,
    pub archiver_snapshot: RefCell<PgStat_ArchiverStats>,
    pub bgwriter_shmem: RefCell<PgStatShared_BgWriter>,
    pub bgwriter_snapshot: RefCell<PgStat_BgWriterStats>,
    pub checkpointer_shmem: RefCell<PgStatShared_Checkpointer>,
    pub checkpointer_snapshot: RefCell<PgStat_CheckpointerStats>,
    pub is_shutdown: Cell<bool>,
    pub assert_is_up_calls: Cell<u32>,
    pub snapshot_fixed_kinds: RefCell<Vec<PgStat_Kind>>,
    // GetCurrentTimestamp fixture.
    pub now: Cell<TimestampTz>,
    // pgstat_flush_io bookkeeping.
    pub flush_io_calls: Cell<u32>,
    // lwlock bookkeeping.
    pub lwlock_inits: Cell<u32>,
    pub lwlock_acquires: RefCell<Vec<LWLockMode>>,
    pub lwlock_releases: Cell<u32>,
}

impl Env {
    fn new() -> Self {
        Env {
            present: Cell::new(true),
            track: Cell::new(true),
            parallel: Cell::new(false),
            beentry: RefCell::new(PgBackendStatus::default()),
            sent: Cell::new(None),
            archiver_shmem: RefCell::new(Default::default()),
            archiver_snapshot: RefCell::new(Default::default()),
            bgwriter_shmem: RefCell::new(Default::default()),
            bgwriter_snapshot: RefCell::new(Default::default()),
            checkpointer_shmem: RefCell::new(Default::default()),
            checkpointer_snapshot: RefCell::new(Default::default()),
            is_shutdown: Cell::new(false),
            assert_is_up_calls: Cell::new(0),
            snapshot_fixed_kinds: RefCell::new(Vec::new()),
            now: Cell::new(0),
            flush_io_calls: Cell::new(0),
            lwlock_inits: Cell::new(0),
            lwlock_acquires: RefCell::new(Vec::new()),
            lwlock_releases: Cell::new(0),
        }
    }

    fn reset(&self) {
        self.present.set(true);
        self.track.set(true);
        self.parallel.set(false);
        *self.beentry.borrow_mut() = PgBackendStatus::default();
        self.sent.set(None);
        *self.archiver_shmem.borrow_mut() = Default::default();
        *self.archiver_snapshot.borrow_mut() = Default::default();
        *self.bgwriter_shmem.borrow_mut() = Default::default();
        *self.bgwriter_snapshot.borrow_mut() = Default::default();
        *self.checkpointer_shmem.borrow_mut() = Default::default();
        *self.checkpointer_snapshot.borrow_mut() = Default::default();
        self.is_shutdown.set(false);
        self.assert_is_up_calls.set(0);
        self.snapshot_fixed_kinds.borrow_mut().clear();
        self.now.set(0);
        self.flush_io_calls.set(0);
        self.lwlock_inits.set(0);
        self.lwlock_acquires.borrow_mut().clear();
        self.lwlock_releases.set(0);
    }
}

thread_local! {
    static ENV: &'static Env = Box::leak(Box::new(Env::new()));
}

pub(crate) fn env() -> &'static Env {
    ENV.with(|e| *e)
}

static LK: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

/// Held for the duration of a test; derefs to the per-thread [`Env`].
pub(crate) struct TestGuard {
    env: &'static Env,
    _guard: MutexGuard<'static, ()>,
}

impl Deref for TestGuard {
    type Target = Env;
    fn deref(&self) -> &Env {
        self.env
    }
}

/// Serialize the test, install the seam dispatchers (once per process), and
/// reset both the per-thread fixture and the thread-local pending buffers.
pub(crate) fn setup() -> TestGuard {
    let guard = LK.lock().unwrap_or_else(|p| p.into_inner());
    INSTALL.call_once(install_seams);
    let e = env();
    e.reset();
    crate::pgstat_bgwriter::with_pending_bgwriter_stats(|p| *p = Default::default());
    crate::pgstat_checkpointer::with_pending_checkpointer_stats(|p| *p = Default::default());
    TestGuard {
        env: e,
        _guard: guard,
    }
}

/// `setup()` plus the backend_progress external flags, then run `body`.
pub(crate) fn with_flags(present: bool, track: bool, parallel: bool, body: impl FnOnce()) {
    let env = setup();
    env.present.set(present);
    env.track.set(track);
    env.parallel.set(parallel);
    body();
}

pub(crate) fn with_fixture<R>(f: impl FnOnce(&Env) -> R) -> R {
    f(env())
}

fn install_seams() {
    use backend_libpq_pqcomm_seams as pqcomm;
    use backend_storage_lmgr_lwlock_seams as lwlock;
    use backend_utils_activity_pgstat_seams as pgstat;
    use backend_utils_activity_stat_seams as stat;
    use backend_utils_activity_status_seams as status;
    use backend_utils_adt_timestamp_seams as timestamp;

    status::my_be_entry_present::set(|| env().present.get());
    status::track_activities::set(|| env().track.get());
    status::with_my_beentry::set(|f| f(&mut env().beentry.borrow_mut()));


    // backend_progress builds PqMsg_Progress messages with the real pqformat
    // routines (a direct cargo dependency); only the transport is a seam.
    // Capture the completed message and decode its network-order body.
    pqcomm::pq_putmessage::set(|msgtype, body| {
        assert_eq!(msgtype, crate::backend_progress::PQ_MSG_PROGRESS);
        assert_eq!(body.len(), 12);
        let idx = i32::from_be_bytes(body[..4].try_into().unwrap());
        let incr = i64::from_be_bytes(body[4..].try_into().unwrap());
        env().sent.set(Some((idx, incr)));
        Ok(0)
    });

    // Callback shmem seams run the body against the per-thread fixture's
    // interior; the `RefCell` borrow lives exactly for the callback.
    pgstat::with_shmem_archiver::set(|f| f(&mut env().archiver_shmem.borrow_mut()));
    pgstat::with_snapshot_archiver::set(|f| f(&mut env().archiver_snapshot.borrow_mut()));
    pgstat::with_shmem_bgwriter::set(|f| f(&mut env().bgwriter_shmem.borrow_mut()));
    pgstat::with_snapshot_bgwriter::set(|f| f(&mut env().bgwriter_snapshot.borrow_mut()));
    pgstat::with_shmem_checkpointer::set(|f| f(&mut env().checkpointer_shmem.borrow_mut()));
    pgstat::with_snapshot_checkpointer::set(|f| f(&mut env().checkpointer_snapshot.borrow_mut()));
    pgstat::shmem_is_shutdown::set(|| env().is_shutdown.get());
    pgstat::assert_is_up::set(|| {
        let e = env();
        e.assert_is_up_calls.set(e.assert_is_up_calls.get() + 1);
    });
    pgstat::snapshot_fixed::set(|kind| {
        env().snapshot_fixed_kinds.borrow_mut().push(kind);
        Ok(())
    });

    timestamp::get_current_timestamp::set(|| env().now.get());

    lwlock::lwlock_initialize::set(|lock, tranche_id| {
        lock.tranche = tranche_id as u16;
        let e = env();
        e.lwlock_inits.set(e.lwlock_inits.get() + 1);
    });
    lwlock::lwlock_acquire::set(|lock, mode, _my_proc_number| {
        env().lwlock_acquires.borrow_mut().push(mode);
        Ok(lwlock::LWLockGuard::new(lock, true))
    });

    backend_utils_init_small_seams::my_proc_number::set(|| 0);
    lwlock::lwlock_release::set(|_lock| {
        let e = env();
        e.lwlock_releases.set(e.lwlock_releases.get() + 1);
        Ok(())
    });

    stat::pgstat_flush_io::set(|_nowait| {
        let e = env();
        e.flush_io_calls.set(e.flush_io_calls.get() + 1);
        Ok(false)
    });
}
