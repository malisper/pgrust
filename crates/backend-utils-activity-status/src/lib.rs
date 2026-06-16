//! Port of `src/backend/utils/activity/backend_status.c` (PostgreSQL 18.3).
//!
//! Backend status reporting infrastructure: every live backend (and auxiliary
//! process) advertises its current activity through a `PgBackendStatus` struct
//! kept in shared memory, organised by `ProcNumber`. This is unrelated to the
//! cumulative statistics system (`pgstat.c`).
//!
//! # What this crate OWNS (1:1 with the C file)
//!
//!   * the in-segment `PgBackendStatusEntry` / `PgBackendSSLStatus` structs,
//!   * the two GUC variables `pgstat_track_activities` /
//!     `pgstat_track_activity_query_size` (installed into the `guc-tables`
//!     accessor slots),
//!   * `MyBEEntry` and the shared `BackendStatusArray` plus the out-of-line
//!     `BackendAppnameBuffer` / `BackendClientHostnameBuffer` /
//!     `BackendActivityBuffer` (+ SSL status buffer),
//!   * the `st_changecount` write/read activity protocol and the control-flow /
//!     field-write logic of every function.
//!
//! # Real shared-memory parity
//!
//! The status array, the three string buffers and the SSL-status buffer are
//! genuine shared memory placed through `ShmemInitStruct`, so the bytes are
//! valid across processes and the `st_appname` / `st_clienthostname` /
//! `st_activity_raw` out-of-line pointers address the same in-segment buffers as
//! upstream. The in-segment structs are `#[repr(C)]`, reached through raw
//! pointers held in `Atomic*` cells (the C file-statics), exactly as the
//! `backend-storage-ipc-shmem` crate models its own in-segment structures.
//!
//! # `with_my_beentry` reconciliation
//!
//! `backend_progress.c` writes the `st_progress_*` fields of its own backend
//! entry through the `with_my_beentry` seam, which hands out the trimmed
//! `types_pgstat::backend_status::PgBackendStatus` (changecount + progress
//! fields). The full in-segment entry stores those fields with a plain `i32`
//! changecount under the C protocol; the seam copies the four progress/changecount
//! fields into the trimmed view, runs the consumer's callback (which does its own
//! `PGSTAT_BEGIN/END_WRITE_ACTIVITY` bracketing on the `AtomicU32` changecount),
//! then copies them back. The copy is sound because the entry is only written by
//! this backend, synchronously within the callback.
//!
//! # Build configuration
//!
//! This build defines `USE_SSL` (OpenSSL) but NOT `ENABLE_GSS` (matching the
//! repo's trimmed `types_net::Port`, which has no GSS state). The SSL paths are
//! compiled unconditionally; GSS is treated as always-off.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::{Cell, RefCell};
use core::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};

use backend_storage_ipc_shmem::{add_size, mul_size, ShmemInitStruct};
use backend_utils_adt_ascii::ascii_safe_strlcpy;
use backend_utils_error::PgResult;
use backend_utils_mb_mbutils::pg_mbcliplen;

use types_core::init::BackendType;
use types_core::{
    int64, InvalidOid, Oid, ProcNumber, Size, TimestampTz, TransactionId, INVALID_PROC_NUMBER,
};
use types_net::SockAddr;
use types_pgstat::backend_progress::{ProgressCommandType, PGSTAT_NUM_PROGRESS_PARAM};

mod globals;

// ---------------------------------------------------------------------------
// Constants mirrored from headers
// ---------------------------------------------------------------------------

/// `NAMEDATALEN` (`pg_config_manual.h`).
pub const NAMEDATALEN: usize = 64;

/// `NUM_AUXILIARY_PROCS` (`storage/proc.h`).
use globals::NUM_AUXILIARY_PROCS;

// ---------------------------------------------------------------------------
// Enums (utils/backend_status.h)
// ---------------------------------------------------------------------------

/// `BackendState` (`utils/backend_status.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum BackendState {
    Undefined = 0,
    Starting = 1,
    Idle = 2,
    Running = 3,
    IdleInTransaction = 4,
    Fastpath = 5,
    IdleInTransactionAborted = 6,
    Disabled = 7,
}

// Upstream spelling aliases for readability against the C source.
pub use BackendState::Disabled as STATE_DISABLED;
pub use BackendState::Fastpath as STATE_FASTPATH;
pub use BackendState::Idle as STATE_IDLE;
pub use BackendState::IdleInTransaction as STATE_IDLEINTRANSACTION;
pub use BackendState::IdleInTransactionAborted as STATE_IDLEINTRANSACTION_ABORTED;
pub use BackendState::Running as STATE_RUNNING;
pub use BackendState::Starting as STATE_STARTING;
pub use BackendState::Undefined as STATE_UNDEFINED;

/// `PROGRESS_COMMAND_INVALID`.
pub const PROGRESS_COMMAND_INVALID: ProgressCommandType = ProgressCommandType::Invalid;

// ---------------------------------------------------------------------------
// In-segment structs (utils/backend_status.h).
//
// These live at chosen byte addresses inside the raw shared segment, so they
// are `#[repr(C)]` and reached through raw pointers (faithful-shmem parity).
// ---------------------------------------------------------------------------

/// `PgBackendSSLStatus` — SSL connection details, only filled when SSL enabled.
/// All char arrays are NUL-terminated.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgBackendSSLStatus {
    pub ssl_bits: i32,
    pub ssl_version: [u8; NAMEDATALEN],
    pub ssl_cipher: [u8; NAMEDATALEN],
    pub ssl_client_dn: [u8; NAMEDATALEN],
    pub ssl_client_serial: [u8; NAMEDATALEN],
    pub ssl_issuer_dn: [u8; NAMEDATALEN],
}

impl PgBackendSSLStatus {
    fn zeroed() -> Self {
        PgBackendSSLStatus {
            ssl_bits: 0,
            ssl_version: [0; NAMEDATALEN],
            ssl_cipher: [0; NAMEDATALEN],
            ssl_client_dn: [0; NAMEDATALEN],
            ssl_client_serial: [0; NAMEDATALEN],
            ssl_issuer_dn: [0; NAMEDATALEN],
        }
    }
}

/// `PgBackendStatus` — per-backend current-activity entry in shared memory,
/// laid out `#[repr(C)]` field-for-field with the C struct (USE_SSL on, no GSS).
///
/// `st_changecount` is the plain C `int` manipulated only through the
/// begin/end-write-activity protocol below.
#[repr(C)]
pub struct PgBackendStatusEntry {
    pub st_changecount: i32,
    pub st_procpid: i32,
    pub st_backendType: BackendType,

    pub st_proc_start_timestamp: TimestampTz,
    pub st_xact_start_timestamp: TimestampTz,
    pub st_activity_start_timestamp: TimestampTz,
    pub st_state_start_timestamp: TimestampTz,

    pub st_databaseid: Oid,
    pub st_userid: Oid,
    pub st_clientaddr: SockAddr,
    /// `char *st_clienthostname;` — NUL-terminated (out-of-line).
    pub st_clienthostname: *mut u8,

    pub st_ssl: bool,
    /// `PgBackendSSLStatus *st_sslstatus;` (out-of-line).
    pub st_sslstatus: *mut PgBackendSSLStatus,

    pub st_gss: bool,

    pub st_state: BackendState,

    /// `char *st_appname;` — NUL-terminated (out-of-line).
    pub st_appname: *mut u8,
    /// `char *st_activity_raw;` — NUL-terminated (out-of-line).
    pub st_activity_raw: *mut u8,

    pub st_progress_command: ProgressCommandType,
    pub st_progress_command_target: Oid,
    pub st_progress_param: [int64; PGSTAT_NUM_PROGRESS_PARAM],

    pub st_query_id: int64,
    pub st_plan_id: int64,
}

/// `LocalPgBackendStatus` — process-local snapshot entry with locally computed
/// additions (xid/xmin etc.). Owned value type (no leak), the idiomatic
/// realization of C's `LocalPgBackendStatus`.
#[derive(Clone)]
pub struct LocalPgBackendStatus {
    pub backend_status: LocalBackendStatusFields,
    pub proc_number: ProcNumber,
    pub backend_xid: TransactionId,
    pub backend_xmin: TransactionId,
    pub backend_subxact_count: i32,
    pub backend_subxact_overflowed: bool,
}

/// Owned snapshot of the scalar `PgBackendStatus` fields plus the
/// previously-out-of-line strings (owned `Vec<u8>`, no trailing NUL).
#[derive(Clone)]
pub struct LocalBackendStatusFields {
    pub st_procpid: i32,
    pub st_backend_type: BackendType,
    pub st_proc_start_timestamp: TimestampTz,
    pub st_xact_start_timestamp: TimestampTz,
    pub st_activity_start_timestamp: TimestampTz,
    pub st_state_start_timestamp: TimestampTz,
    pub st_databaseid: Oid,
    pub st_userid: Oid,
    pub st_clientaddr: SockAddr,
    pub st_clienthostname: Vec<u8>,
    pub st_ssl: bool,
    pub st_sslstatus: Option<PgBackendSSLStatus>,
    pub st_gss: bool,
    pub st_state: BackendState,
    pub st_appname: Vec<u8>,
    pub st_activity_raw: Vec<u8>,
    pub st_progress_command: ProgressCommandType,
    pub st_progress_command_target: Oid,
    pub st_progress_param: [int64; PGSTAT_NUM_PROGRESS_PARAM],
    pub st_query_id: int64,
    pub st_plan_id: int64,
}

// ---------------------------------------------------------------------------
// st_changecount macros (utils/backend_status.h)
//
// `START_CRIT_SECTION`/`END_CRIT_SECTION` route to the miscinit critical-section
// seams; `pg_write_barrier`/`pg_read_barrier` map to Release/Acquire fences (the
// standard seqlock mapping, matching backend_progress.rs / changecount.rs).
// ---------------------------------------------------------------------------

/// `PGSTAT_BEGIN_WRITE_ACTIVITY(beentry)`:
/// `START_CRIT_SECTION(); st_changecount++; pg_write_barrier();`
#[inline]
unsafe fn PGSTAT_BEGIN_WRITE_ACTIVITY(beentry: *mut PgBackendStatusEntry) {
    backend_utils_init_miscinit_seams::start_crit_section::call();
    (*beentry).st_changecount = (*beentry).st_changecount.wrapping_add(1);
    fence(Ordering::Release);
}

/// `PGSTAT_END_WRITE_ACTIVITY(beentry)`:
/// `pg_write_barrier(); st_changecount++;
/// Assert((st_changecount & 1) == 0); END_CRIT_SECTION();`
#[inline]
unsafe fn PGSTAT_END_WRITE_ACTIVITY(beentry: *mut PgBackendStatusEntry) {
    fence(Ordering::Release);
    (*beentry).st_changecount = (*beentry).st_changecount.wrapping_add(1);
    debug_assert!(((*beentry).st_changecount & 1) == 0);
    backend_utils_init_miscinit_seams::end_crit_section::call();
}

/// `pgstat_begin_read_activity(beentry, before_changecount)`:
/// `before = st_changecount; pg_read_barrier();`
#[inline]
unsafe fn pgstat_begin_read_activity(beentry: *const PgBackendStatusEntry) -> i32 {
    let before = (*beentry).st_changecount;
    fence(Ordering::Acquire);
    before
}

/// `pgstat_end_read_activity(beentry, after_changecount)`:
/// `pg_read_barrier(); after = st_changecount;`
#[inline]
unsafe fn pgstat_end_read_activity(beentry: *const PgBackendStatusEntry) -> i32 {
    fence(Ordering::Acquire);
    (*beentry).st_changecount
}

/// `pgstat_read_activity_complete(before, after)`:
/// `((before) == (after) && ((before) & 1) == 0)`.
#[inline]
fn pgstat_read_activity_complete(before_changecount: i32, after_changecount: i32) -> bool {
    before_changecount == after_changecount && (before_changecount & 1) == 0
}

// ---------------------------------------------------------------------------
// GUC parameters (owned by this file)
// ---------------------------------------------------------------------------

thread_local! {
    /// `bool pgstat_track_activities = false;`
    static PGSTAT_TRACK_ACTIVITIES: Cell<bool> = const { Cell::new(false) };
    /// `int pgstat_track_activity_query_size = 1024;`
    static PGSTAT_TRACK_ACTIVITY_QUERY_SIZE: Cell<i32> = const { Cell::new(1024) };
}

/// Read the `pgstat_track_activities` GUC.
#[inline]
pub fn pgstat_track_activities() -> bool {
    PGSTAT_TRACK_ACTIVITIES.with(Cell::get)
}
fn set_pgstat_track_activities(v: bool) {
    PGSTAT_TRACK_ACTIVITIES.with(|c| c.set(v));
}
/// Read the `pgstat_track_activity_query_size` GUC.
#[inline]
pub fn pgstat_track_activity_query_size() -> i32 {
    PGSTAT_TRACK_ACTIVITY_QUERY_SIZE.with(Cell::get)
}
fn set_pgstat_track_activity_query_size(v: i32) {
    PGSTAT_TRACK_ACTIVITY_QUERY_SIZE.with(|c| c.set(v));
}

// ---------------------------------------------------------------------------
// Global variables (owned by this file). The pointers address real shared
// memory placed by `BackendStatusShmemInit`; held in `Atomic*` cells to mirror
// the C file-statics.
// ---------------------------------------------------------------------------

/// `PgBackendStatus *MyBEEntry = NULL;`
static MY_BE_ENTRY: AtomicPtr<PgBackendStatusEntry> = AtomicPtr::new(core::ptr::null_mut());

/// Read `MyBEEntry`.
#[inline]
fn MyBEEntry() -> *mut PgBackendStatusEntry {
    MY_BE_ENTRY.load(Ordering::Relaxed)
}

/// `static PgBackendStatus *BackendStatusArray = NULL;`
static BackendStatusArray: AtomicPtr<PgBackendStatusEntry> = AtomicPtr::new(core::ptr::null_mut());
/// `static char *BackendAppnameBuffer = NULL;`
static BackendAppnameBuffer: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
/// `static char *BackendClientHostnameBuffer = NULL;`
static BackendClientHostnameBuffer: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
/// `static char *BackendActivityBuffer = NULL;`
static BackendActivityBuffer: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
/// `static Size BackendActivityBufferSize = 0;`
static BackendActivityBufferSize: AtomicUsize = AtomicUsize::new(0);
/// `static PgBackendSSLStatus *BackendSslStatusBuffer = NULL;` (USE_SSL).
static BackendSslStatusBuffer: AtomicPtr<PgBackendSSLStatus> = AtomicPtr::new(core::ptr::null_mut());

thread_local! {
    /// `static LocalPgBackendStatus *localBackendStatusTable = NULL;`
    /// `None` ⇔ the C `NULL` (snapshot not yet built this transaction).
    static localBackendStatusTable: RefCell<Option<Vec<LocalPgBackendStatus>>> =
        const { RefCell::new(None) };
}

// ---------------------------------------------------------------------------
// Helpers shared by several functions
// ---------------------------------------------------------------------------

/// `#define NumBackendStatSlots (MaxBackends + NUM_AUXILIARY_PROCS)`
#[inline]
fn NumBackendStatSlots() -> i32 {
    backend_utils_init_small::globals::MaxBackends() + NUM_AUXILIARY_PROCS
}

/// `MemSet(ptr, 0, size)` over a raw byte region.
#[inline]
unsafe fn mem_set_zero(ptr: *mut u8, size: usize) {
    core::ptr::write_bytes(ptr, 0, size);
}

/// `strlcpy(dst, src, size)` — copy a server-encoded byte string into `dst`
/// (`size` bytes), always NUL-terminating, never overrunning. `src` excludes a
/// trailing NUL; copying stops at the first embedded NUL.
unsafe fn strlcpy(dst: *mut u8, src: &[u8], size: usize) {
    if size == 0 {
        return;
    }
    let end = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    let n = core::cmp::min(end, size - 1);
    core::ptr::copy_nonoverlapping(src.as_ptr(), dst, n);
    *dst.add(n) = 0;
}

/// Install a shmem region under `name`, returning the typed base pointer and
/// whether it already existed.
fn shmem_init<T>(name: &str, size: Size) -> PgResult<(*mut T, bool)> {
    let (ptr, found) = ShmemInitStruct(name, size)?;
    Ok((ptr.as_ptr() as *mut T, found))
}

/// Snapshot the out-of-line bytes of an in-segment NUL-terminated buffer at
/// `p`, capping at `cap` bytes.
unsafe fn snapshot_cstr(p: *const u8, cap: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < cap {
        let b = *p.add(i);
        if b == 0 {
            break;
        }
        out.try_reserve(1).expect("snapshot string allocation");
        out.push(b);
        i += 1;
    }
    out
}

// ===========================================================================
// Functions
// ===========================================================================

/// `BackendStatusShmemSize()` — report shared-memory space needed by
/// `BackendStatusShmemInit`.
pub fn BackendStatusShmemSize() -> PgResult<Size> {
    let n = NumBackendStatSlots() as usize;

    let mut size = mul_size(core::mem::size_of::<PgBackendStatusEntry>(), n)?;
    size = add_size(size, mul_size(NAMEDATALEN, n)?)?;
    size = add_size(size, mul_size(NAMEDATALEN, n)?)?;
    size = add_size(
        size,
        mul_size(pgstat_track_activity_query_size() as usize, n)?,
    )?;
    size = add_size(size, mul_size(core::mem::size_of::<PgBackendSSLStatus>(), n)?)?;
    Ok(size)
}

/// `BackendStatusShmemInit()` — initialize the shared status array and several
/// string buffers during postmaster startup.
pub fn BackendStatusShmemInit() -> PgResult<()> {
    let n = NumBackendStatSlots() as usize;
    let qsize = pgstat_track_activity_query_size() as usize;

    // Create or attach to the shared array
    let size = mul_size(core::mem::size_of::<PgBackendStatusEntry>(), n)?;
    let (arr, found) = shmem_init::<PgBackendStatusEntry>("Backend Status Array", size)?;
    BackendStatusArray.store(arr, Ordering::Relaxed);

    if !found {
        // SAFETY: `arr` addresses `size` writable shmem bytes.
        unsafe { mem_set_zero(arr as *mut u8, size) };
    }

    // Create or attach to the shared appname buffer
    let size = mul_size(NAMEDATALEN, n)?;
    let (appbuf, found) = shmem_init::<u8>("Backend Application Name Buffer", size)?;
    BackendAppnameBuffer.store(appbuf, Ordering::Relaxed);

    if !found {
        // SAFETY: `appbuf` addresses `size` writable shmem bytes.
        unsafe { mem_set_zero(appbuf, size) };
        let mut buffer = appbuf;
        for i in 0..n {
            // SAFETY: i < n; each slot is in-bounds of `arr` and the buffer.
            unsafe { (*arr.add(i)).st_appname = buffer };
            buffer = unsafe { buffer.add(NAMEDATALEN) };
        }
    }

    // Create or attach to the shared client hostname buffer
    let size = mul_size(NAMEDATALEN, n)?;
    let (hostbuf, found) = shmem_init::<u8>("Backend Client Host Name Buffer", size)?;
    BackendClientHostnameBuffer.store(hostbuf, Ordering::Relaxed);

    if !found {
        // SAFETY: as above.
        unsafe { mem_set_zero(hostbuf, size) };
        let mut buffer = hostbuf;
        for i in 0..n {
            // SAFETY: as above.
            unsafe { (*arr.add(i)).st_clienthostname = buffer };
            buffer = unsafe { buffer.add(NAMEDATALEN) };
        }
    }

    // Create or attach to the shared activity buffer
    let activity_size = mul_size(qsize, n)?;
    BackendActivityBufferSize.store(activity_size, Ordering::Relaxed);
    let (actbuf, found) = shmem_init::<u8>("Backend Activity Buffer", activity_size)?;
    BackendActivityBuffer.store(actbuf, Ordering::Relaxed);

    if !found {
        // SAFETY: `actbuf` addresses `activity_size` writable shmem bytes.
        unsafe { mem_set_zero(actbuf, activity_size) };
        let mut buffer = actbuf;
        for i in 0..n {
            // SAFETY: buffer is `qsize * n` long; each slot in-bounds.
            unsafe { (*arr.add(i)).st_activity_raw = buffer };
            buffer = unsafe { buffer.add(qsize) };
        }
    }

    // Create or attach to the shared SSL status buffer (USE_SSL)
    let size = mul_size(core::mem::size_of::<PgBackendSSLStatus>(), n)?;
    let (sslbuf, found) = shmem_init::<PgBackendSSLStatus>("Backend SSL Status Buffer", size)?;
    BackendSslStatusBuffer.store(sslbuf, Ordering::Relaxed);

    if !found {
        // SAFETY: `sslbuf` addresses `size` writable shmem bytes.
        unsafe { mem_set_zero(sslbuf as *mut u8, size) };
        let mut p = sslbuf;
        for i in 0..n {
            // SAFETY: `p` walks `n` PgBackendSSLStatus entries; each in-bounds.
            unsafe { (*arr.add(i)).st_sslstatus = p };
            p = unsafe { p.add(1) };
        }
    }

    Ok(())
}

/// `pgstat_beinit()` — initialize backend activity state and set up the
/// on-proc-exit hook.
pub fn pgstat_beinit() -> PgResult<()> {
    let procno = backend_utils_init_small::globals::MyProcNumber();
    debug_assert!(procno != INVALID_PROC_NUMBER);
    debug_assert!(procno >= 0 && procno < NumBackendStatSlots());
    let arr = BackendStatusArray.load(Ordering::Relaxed);
    // SAFETY: procno is a valid slot index; `arr` is the shmem-resident array.
    let entry = unsafe { arr.add(procno as usize) };
    MY_BE_ENTRY.store(entry, Ordering::Relaxed);

    // Set up a process-exit hook to clean up.
    backend_storage_ipc_dsm_core_seams::on_shmem_exit::call(
        pgstat_beshutdown_hook,
        types_tuple::Datum::from_u64(0),
    )
}

/// `pgstat_bestart_initial()` — initialize this backend's entry, reporting
/// `STATE_STARTING`.
pub fn pgstat_bestart_initial() -> PgResult<()> {
    let vbeentry = MyBEEntry();
    debug_assert!(!vbeentry.is_null());

    // We may not have a MyProcPort (eg autovacuum). If so, all-zeroes client
    // address; otherwise copy MyProcPort->raddr / remote_hostname.
    let (clientaddr, remote_hostname) =
        backend_utils_init_small::globals::WithMyProcPort(|port| {
            (port.raddr, port.remote_hostname.clone())
        })
        .unwrap_or((SockAddr::zeroed(), None));

    let my_proc_pid = backend_utils_init_small::globals::MyProcPid();
    let my_backend_type = backend_utils_init_small::globals::MyBackendType();
    let my_start_timestamp = backend_utils_init_small::globals::MyStartTimestamp();
    let qsize = pgstat_track_activity_query_size() as usize;
    let remote_hostname_bytes = remote_hostname.as_ref().map(|s| s.clone().into_bytes());

    // SAFETY: vbeentry is the live, shmem-resident MyBEEntry (asserted
    // non-null); the out-of-line pointers were initialized at shmem init.
    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(vbeentry);

        (*vbeentry).st_procpid = my_proc_pid;
        (*vbeentry).st_backendType = my_backend_type;
        (*vbeentry).st_proc_start_timestamp = my_start_timestamp;
        (*vbeentry).st_activity_start_timestamp = 0;
        (*vbeentry).st_state_start_timestamp = 0;
        (*vbeentry).st_xact_start_timestamp = 0;
        (*vbeentry).st_databaseid = InvalidOid;
        (*vbeentry).st_userid = InvalidOid;
        (*vbeentry).st_clientaddr = clientaddr;
        (*vbeentry).st_ssl = false;
        (*vbeentry).st_gss = false;
        (*vbeentry).st_state = STATE_STARTING;
        (*vbeentry).st_progress_command = PROGRESS_COMMAND_INVALID;
        (*vbeentry).st_progress_command_target = InvalidOid;
        (*vbeentry).st_query_id = 0;
        (*vbeentry).st_plan_id = 0;
        // (st_progress_param intentionally not zeroed, to save cycles.)

        let appname = (*vbeentry).st_appname;
        let clienthostname = (*vbeentry).st_clienthostname;
        let activity_raw = (*vbeentry).st_activity_raw;

        *appname = b'\0';
        match &remote_hostname_bytes {
            Some(rhn) => strlcpy(clienthostname, rhn, NAMEDATALEN),
            None => *clienthostname = b'\0',
        }
        *activity_raw = b'\0';
        // Also make sure the last byte in each string area is always 0.
        *appname.add(NAMEDATALEN - 1) = b'\0';
        *clienthostname.add(NAMEDATALEN - 1) = b'\0';
        *activity_raw.add(qsize - 1) = b'\0';

        // The SSL status struct starts from zeroes each time.
        mem_set_zero(
            (*vbeentry).st_sslstatus as *mut u8,
            core::mem::size_of::<PgBackendSSLStatus>(),
        );

        PGSTAT_END_WRITE_ACTIVITY(vbeentry);
    }
    Ok(())
}

/// `pgstat_bestart_security()` — fill in SSL information for the pgstat entry.
/// Only called from backends with a MyProcPort.
pub fn pgstat_bestart_security() -> PgResult<()> {
    let beentry = MyBEEntry();
    debug_assert!(!beentry.is_null());

    // (USE_SSL) read the negotiated TLS details off MyProcPort. The seam
    // `be_tls_get_*` accessors take an `Mcx`; build a short-lived context for
    // the transient string allocations (discarded when it drops).
    let ctx = mcx::MemoryContext::new("pgstat_bestart_security");
    let mcx = ctx.mcx();

    let mut lsslstatus = PgBackendSSLStatus::zeroed();
    let ssl = backend_utils_init_small::globals::WithMyProcPort(|port| -> PgResult<bool> {
        if !port.ssl_in_use {
            return Ok(false);
        }
        lsslstatus.ssl_bits = backend_libpq_be_secure_seams::be_tls_get_cipher_bits::call(port);
        // SAFETY: the destination arrays are NAMEDATALEN bytes; strlcpy never
        // overruns and always NUL-terminates.
        unsafe {
            let v = backend_libpq_be_secure_seams::be_tls_get_version::call(mcx, port)?;
            strlcpy(lsslstatus.ssl_version.as_mut_ptr(), v.as_str().as_bytes(), NAMEDATALEN);
            let c = backend_libpq_be_secure_seams::be_tls_get_cipher::call(mcx, port)?;
            strlcpy(lsslstatus.ssl_cipher.as_mut_ptr(), c.as_str().as_bytes(), NAMEDATALEN);
            let dn = backend_libpq_be_secure_seams::be_tls_get_peer_subject_name::call(mcx, port)?;
            strlcpy(lsslstatus.ssl_client_dn.as_mut_ptr(), dn.as_str().as_bytes(), NAMEDATALEN);
            let serial = backend_libpq_be_secure_seams::be_tls_get_peer_serial::call(mcx, port)?;
            strlcpy(lsslstatus.ssl_client_serial.as_mut_ptr(), serial.as_str().as_bytes(), NAMEDATALEN);
            let issuer = backend_libpq_be_secure_seams::be_tls_get_peer_issuer_name::call(mcx, port)?;
            strlcpy(lsslstatus.ssl_issuer_dn.as_mut_ptr(), issuer.as_str().as_bytes(), NAMEDATALEN);
        }
        Ok(true)
    })
    .transpose()?
    .unwrap_or(false);

    // Update my status entry, bumping st_changecount before and after.
    // SAFETY: beentry is the live, shmem-resident MyBEEntry; st_sslstatus
    // points at the in-segment status buffer.
    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

        (*beentry).st_ssl = ssl;
        (*beentry).st_gss = false;

        // (USE_SSL)
        *(*beentry).st_sslstatus = lsslstatus;

        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }
    Ok(())
}

/// `pgstat_bestart_final()` — finalize the backend entry: user/database IDs,
/// clear `STATE_STARTING`, report `application_name`.
pub fn pgstat_bestart_final() -> PgResult<()> {
    let beentry = MyBEEntry();
    debug_assert!(!beentry.is_null());

    // We have userid for client-backends, wal-sender and bgworker processes.
    let mybt = backend_utils_init_small::globals::MyBackendType();
    let userid = if mybt == BackendType::Backend
        || mybt == BackendType::WalSender
        || mybt == BackendType::BgWorker
    {
        backend_utils_init_miscinit_seams::get_session_user_id::call()
    } else {
        InvalidOid
    };
    let my_database_id = backend_utils_init_small::globals::MyDatabaseId();

    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

        (*beentry).st_databaseid = my_database_id;
        (*beentry).st_userid = userid;
        (*beentry).st_state = STATE_UNDEFINED;

        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }

    // Create the backend statistics entry.
    if backend_utils_activity_pgstat_backend_seams::pgstat_tracks_backend_bktype::call(mybt) {
        backend_utils_activity_pgstat_backend_seams::pgstat_create_backend::call(
            backend_utils_init_small::globals::MyProcNumber(),
        );
    }

    // Update app name to the current GUC setting.
    if let Some(appname) = backend_utils_misc_guc_tables::vars::application_name.read() {
        pgstat_report_appname(appname.as_bytes());
    }
    Ok(())
}

/// `pgstat_beshutdown_hook()` — clear out our entry in the status array.
/// Static in C; registered via `on_shmem_exit` in `pgstat_beinit`.
fn pgstat_beshutdown_hook(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    let beentry = MyBEEntry();

    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
        (*beentry).st_procpid = 0; // mark invalid
        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }

    // so that functions can check if backend_status.c is up via MyBEEntry
    MY_BE_ENTRY.store(core::ptr::null_mut(), Ordering::Relaxed);
    Ok(())
}

/// `pgstat_clear_backend_activity_snapshot()` — discard data collected in the
/// current transaction so subsequent requests read new snapshots.
pub fn pgstat_clear_backend_activity_snapshot() {
    // In C this deletes `backendStatusSnapContext`; here the owned snapshot Vec
    // is dropped (no leak).
    localBackendStatusTable.with(|c| *c.borrow_mut() = None);
}

/// `pgstat_report_activity()` — report what the backend is actually doing.
/// `cmd_str` is the server-encoded command bytes (no trailing NUL), or `None`.
pub fn pgstat_report_activity(state: BackendState, cmd_str: Option<&[u8]>) {
    let beentry = MyBEEntry();

    // TRACE_POSTGRESQL_STATEMENT_STATUS(cmd_str): DTrace probe, not modeled.

    if beentry.is_null() {
        return;
    }

    if !pgstat_track_activities() {
        // SAFETY: beentry is the live, shmem-resident MyBEEntry.
        if unsafe { (*beentry).st_state } != STATE_DISABLED {
            // track_activities is disabled, but we last reported a non-disabled
            // state. As our final update, change state and clear fields.
            unsafe {
                PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
                (*beentry).st_state = STATE_DISABLED;
                (*beentry).st_state_start_timestamp = 0;
                *(*beentry).st_activity_raw = b'\0';
                (*beentry).st_activity_start_timestamp = 0;
                (*beentry).st_xact_start_timestamp = 0;
                (*beentry).st_query_id = 0;
                (*beentry).st_plan_id = 0;
                // proc->wait_event_info = 0;
                backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();
                PGSTAT_END_WRITE_ACTIVITY(beentry);
            }
        }
        return;
    }

    // Fetch all the needed data first (minimise time inside the crit section).
    let start_timestamp =
        backend_access_transam_xact_seams::get_current_statement_start_timestamp::call();
    let mut len: usize = 0;
    if let Some(cmd) = cmd_str {
        let slen = cmd.iter().position(|&b| b == 0).unwrap_or(cmd.len());
        len = core::cmp::min(slen, (pgstat_track_activity_query_size() - 1) as usize);
    }
    let current_timestamp = backend_utils_adt_timestamp_seams::get_current_timestamp::call();

    // If the state has changed from "active" or "idle in transaction",
    // calculate the duration.
    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    let cur_state = unsafe { (*beentry).st_state };
    if (cur_state == STATE_RUNNING
        || cur_state == STATE_FASTPATH
        || cur_state == STATE_IDLEINTRANSACTION
        || cur_state == STATE_IDLEINTRANSACTION_ABORTED)
        && state != cur_state
    {
        let (secs, usecs) = backend_utils_adt_timestamp_seams::timestamp_difference::call(
            unsafe { (*beentry).st_state_start_timestamp },
            current_timestamp,
        );

        if cur_state == STATE_RUNNING || cur_state == STATE_FASTPATH {
            backend_utils_activity_pgstat_database_seams::pgstat_count_conn_active_time::call(
                secs * 1_000_000 + usecs as i64,
            );
        } else {
            backend_utils_activity_pgstat_database_seams::pgstat_count_conn_txn_idle_time::call(
                secs * 1_000_000 + usecs as i64,
            );
        }
    }

    // Now update the status entry.
    // SAFETY: beentry is the live, shmem-resident MyBEEntry; st_activity_raw
    // points at the in-segment activity buffer (>= qsize bytes), and len <
    // qsize so the write + NUL terminator are in-bounds.
    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

        (*beentry).st_state = state;
        (*beentry).st_state_start_timestamp = current_timestamp;

        // If a new query is started, reset the query/plan identifiers.
        if state == STATE_RUNNING {
            (*beentry).st_query_id = 0;
            (*beentry).st_plan_id = 0;
        }

        if let Some(cmd) = cmd_str {
            core::ptr::copy_nonoverlapping(cmd.as_ptr(), (*beentry).st_activity_raw, len);
            *(*beentry).st_activity_raw.add(len) = b'\0';
            (*beentry).st_activity_start_timestamp = start_timestamp;
        }

        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }
}

/// `pgstat_report_query_id()` — update the top-level query identifier.
pub fn pgstat_report_query_id(query_id: int64, force: bool) {
    let beentry = MyBEEntry();

    // if track_activities is disabled, st_query_id should already be reset
    if beentry.is_null() || !pgstat_track_activities() {
        return;
    }

    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    if unsafe { (*beentry).st_query_id } != 0 && !force {
        return;
    }

    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
        (*beentry).st_query_id = query_id;
        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }
}

/// `pgstat_report_plan_id()` — update the top-level plan identifier.
pub fn pgstat_report_plan_id(plan_id: int64, force: bool) {
    let beentry = MyBEEntry();

    if beentry.is_null() || !pgstat_track_activities() {
        return;
    }

    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    if unsafe { (*beentry).st_plan_id } != 0 && !force {
        return;
    }

    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
        (*beentry).st_plan_id = plan_id;
        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }
}

/// `pgstat_report_appname()` — update our application name.
/// `appname` is the server-encoded application name (no trailing NUL).
pub fn pgstat_report_appname(appname: &[u8]) {
    let beentry = MyBEEntry();

    if beentry.is_null() {
        return;
    }

    // This should be unnecessary if GUC did its job, but be safe.
    let len = pg_mbcliplen(appname, appname.len() as i32, (NAMEDATALEN - 1) as i32) as usize;

    // SAFETY: beentry is the live, shmem-resident MyBEEntry; st_appname points
    // at the in-segment appname buffer (NAMEDATALEN bytes) and len <=
    // NAMEDATALEN-1, so the write + NUL terminator are in-bounds.
    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
        core::ptr::copy_nonoverlapping(appname.as_ptr(), (*beentry).st_appname, len);
        *(*beentry).st_appname.add(len) = b'\0';
        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }
}

/// `pgstat_report_xact_timestamp()` — report current transaction start
/// timestamp. Zero means there is no active transaction.
pub fn pgstat_report_xact_timestamp(tstamp: TimestampTz) {
    let beentry = MyBEEntry();

    if !pgstat_track_activities() || beentry.is_null() {
        return;
    }

    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    unsafe {
        PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
        (*beentry).st_xact_start_timestamp = tstamp;
        PGSTAT_END_WRITE_ACTIVITY(beentry);
    }
}

/// `pgstat_read_current_status()` — copy the current status array to
/// process-local memory, if not already done in this transaction.
fn pgstat_read_current_status() {
    if localBackendStatusTable.with(|c| c.borrow().is_some()) {
        return; // already done
    }

    let n = NumBackendStatSlots() as usize;
    let qsize = pgstat_track_activity_query_size() as usize;

    let mut localtable: Vec<LocalPgBackendStatus> = Vec::new();
    localtable
        .try_reserve(n)
        .expect("backend status snapshot table allocation");

    let beentry_base = BackendStatusArray.load(Ordering::Relaxed);
    for procNumber in 0..(n as ProcNumber) {
        // SAFETY: procNumber < n; `beentry` is in-bounds of the shmem array.
        let beentry = unsafe { beentry_base.add(procNumber as usize) };

        let fields: LocalBackendStatusFields = loop {
            // SAFETY: beentry is in-bounds of the shmem array.
            let before_changecount = unsafe { pgstat_begin_read_activity(beentry) };

            // SAFETY: beentry is a live shmem entry; reading its scalar fields
            // and copying its NUL-terminated out-of-line buffers is sound (the
            // changecount retry below detects any concurrent modification).
            let st_procpid = unsafe { (*beentry).st_procpid };
            let snapshot: LocalBackendStatusFields = if st_procpid > 0 {
                unsafe {
                    let appname = snapshot_cstr((*beentry).st_appname, NAMEDATALEN);
                    let clienthostname = snapshot_cstr((*beentry).st_clienthostname, NAMEDATALEN);
                    let activity_raw = snapshot_cstr((*beentry).st_activity_raw, qsize);
                    let st_ssl = (*beentry).st_ssl;
                    let st_sslstatus = if st_ssl {
                        Some(*(*beentry).st_sslstatus)
                    } else {
                        None
                    };

                    LocalBackendStatusFields {
                        st_procpid,
                        st_backend_type: (*beentry).st_backendType,
                        st_proc_start_timestamp: (*beentry).st_proc_start_timestamp,
                        st_xact_start_timestamp: (*beentry).st_xact_start_timestamp,
                        st_activity_start_timestamp: (*beentry).st_activity_start_timestamp,
                        st_state_start_timestamp: (*beentry).st_state_start_timestamp,
                        st_databaseid: (*beentry).st_databaseid,
                        st_userid: (*beentry).st_userid,
                        st_clientaddr: (*beentry).st_clientaddr,
                        st_clienthostname: clienthostname,
                        st_ssl,
                        st_sslstatus,
                        st_gss: (*beentry).st_gss,
                        st_state: (*beentry).st_state,
                        st_appname: appname,
                        st_activity_raw: activity_raw,
                        st_progress_command: (*beentry).st_progress_command,
                        st_progress_command_target: (*beentry).st_progress_command_target,
                        st_progress_param: (*beentry).st_progress_param,
                        st_query_id: (*beentry).st_query_id,
                        st_plan_id: (*beentry).st_plan_id,
                    }
                }
            } else {
                // Not in use: record only st_procpid so the post-loop check can
                // skip it (matches C copying st_procpid before the use test).
                LocalBackendStatusFields {
                    st_procpid,
                    st_backend_type: BackendType::Invalid,
                    st_proc_start_timestamp: 0,
                    st_xact_start_timestamp: 0,
                    st_activity_start_timestamp: 0,
                    st_state_start_timestamp: 0,
                    st_databaseid: InvalidOid,
                    st_userid: InvalidOid,
                    st_clientaddr: SockAddr::zeroed(),
                    st_clienthostname: Vec::new(),
                    st_ssl: false,
                    st_sslstatus: None,
                    st_gss: false,
                    st_state: STATE_UNDEFINED,
                    st_appname: Vec::new(),
                    st_activity_raw: Vec::new(),
                    st_progress_command: PROGRESS_COMMAND_INVALID,
                    st_progress_command_target: InvalidOid,
                    st_progress_param: [0; PGSTAT_NUM_PROGRESS_PARAM],
                    st_query_id: 0,
                    st_plan_id: 0,
                }
            };

            // SAFETY: beentry is in-bounds of the shmem array.
            let after_changecount = unsafe { pgstat_end_read_activity(beentry) };

            if pgstat_read_activity_complete(before_changecount, after_changecount) {
                break snapshot;
            }

            // Make sure we can break out of loop if stuck...
            // CHECK_FOR_INTERRUPTS(): interrupt processing not driven here.
        };

        // Only valid entries get included into the local array.
        if fields.st_procpid > 0 {
            let (xid, xmin, subxact_count, overflowed) =
                backend_storage_ipc_procarray_seams::proc_number_get_transaction_ids::call(
                    procNumber,
                );
            // Bounded by the up-front reserve of `n`; this push won't grow.
            localtable.push(LocalPgBackendStatus {
                backend_status: fields,
                proc_number: procNumber,
                backend_xid: xid,
                backend_xmin: xmin,
                backend_subxact_count: subxact_count,
                backend_subxact_overflowed: overflowed,
            });
        }
    }

    localBackendStatusTable.with(|c| *c.borrow_mut() = Some(localtable));
}

/// `pgstat_get_backend_current_activity()` — current activity string of the
/// backend with the specified PID, looking directly at the status array.
/// Returns the server-encoded bytes.
pub fn pgstat_get_backend_current_activity(pid: i32, checkUser: bool) -> Vec<u8> {
    let beentry_base = BackendStatusArray.load(Ordering::Relaxed);
    let max_backends = backend_utils_init_small::globals::MaxBackends();

    // for (i = 1; i <= MaxBackends; i++)
    let mut i = 1;
    while i <= max_backends {
        // SAFETY: i in 1..=MaxBackends, so (i-1) indexes within the array.
        let beentry = unsafe { beentry_base.add((i - 1) as usize) };

        let found;
        loop {
            // SAFETY: beentry is in-bounds of the shmem array.
            let before_changecount = unsafe { pgstat_begin_read_activity(beentry) };
            let f = unsafe { (*beentry).st_procpid } == pid;
            let after_changecount = unsafe { pgstat_end_read_activity(beentry) };

            if pgstat_read_activity_complete(before_changecount, after_changecount) {
                found = f;
                break;
            }
        }

        if found {
            // Now it is safe to use the non-volatile pointer.
            // SAFETY: beentry is a live shmem entry.
            if checkUser && !current_user_is_superuser() && unsafe { (*beentry).st_userid } != current_user_id() {
                return b"<insufficient privilege>".to_vec();
            } else if unsafe { *(*beentry).st_activity_raw } == b'\0' {
                return b"<command string not enabled>".to_vec();
            } else {
                let qsize = pgstat_track_activity_query_size() as usize;
                let raw = unsafe { snapshot_cstr((*beentry).st_activity_raw, qsize) };
                return pgstat_clip_activity(&raw);
            }
        }

        i += 1;
    }

    b"<backend information not available>".to_vec()
}

/// `superuser()` over the current user. The deadlock-detector seam that drives
/// this carries no `Mcx` and no `PgResult`; build a short-lived context for the
/// syscache read and treat a read failure conservatively (not superuser, i.e.
/// redact), which is the safe default for the permission gate.
fn current_user_is_superuser() -> bool {
    let ctx = mcx::MemoryContext::new("pgstat_get_backend_current_activity");
    backend_utils_init_miscinit_seams::superuser::call(ctx.mcx()).unwrap_or(false)
}

/// `GetUserId()`.
fn current_user_id() -> Oid {
    backend_utils_init_miscinit_seams::get_user_id::call()
}

/// `pgstat_get_crashed_backend_activity()` — like the function above, but reads
/// shared memory with the expectation that it may be corrupt. On success copies
/// the string into `buffer` and returns `Some`. Used only by the postmaster.
pub fn pgstat_get_crashed_backend_activity(pid: i32, buffer: &mut [u8]) -> Option<()> {
    let beentry_base = BackendStatusArray.load(Ordering::Relaxed);
    let activity_buffer = BackendActivityBuffer.load(Ordering::Relaxed);
    let activity_buffer_size = BackendActivityBufferSize.load(Ordering::Relaxed);
    let qsize = pgstat_track_activity_query_size() as usize;
    let buflen = buffer.len();

    if beentry_base.is_null() || activity_buffer.is_null() {
        return None;
    }

    let max_backends = backend_utils_init_small::globals::MaxBackends();
    let mut i = 1;
    while i <= max_backends {
        // SAFETY: i in 1..=MaxBackends.
        let beentry = unsafe { beentry_base.add((i - 1) as usize) };
        if unsafe { (*beentry).st_procpid } == pid {
            // Read pointer just once, so it can't change after validation.
            let activity = unsafe { (*beentry).st_activity_raw };

            // We mustn't access the activity string before verifying it falls
            // within BackendActivityBuffer.
            // SAFETY: activity_buffer + size is one-past-end; subtracting qsize
            // stays in-bounds.
            let activity_last = unsafe { activity_buffer.add(activity_buffer_size).sub(qsize) };

            if (activity as *const u8) < (activity_buffer as *const u8)
                || (activity as *const u8) > (activity_last as *const u8)
            {
                return None;
            }

            if unsafe { *activity } == b'\0' {
                return None;
            }

            // Copy only ASCII-safe characters; don't run off the end of memory.
            let nbound = core::cmp::min(buflen, qsize);
            // SAFETY: `activity` is verified in-bounds (>= qsize bytes) and
            // NUL-terminated; snapshot up to qsize bytes.
            let src = unsafe { snapshot_cstr(activity, qsize) };
            ascii_safe_strlcpy(&mut buffer[..nbound], &src);

            return Some(());
        }
        i += 1;
    }

    None
}

/// `pgstat_get_my_query_id()` — current backend's query identifier.
pub fn pgstat_get_my_query_id() -> int64 {
    let beentry = MyBEEntry();
    if beentry.is_null() {
        return 0;
    }
    // No lock needed: only called from the same backend or under protection.
    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    unsafe { (*beentry).st_query_id }
}

/// `pgstat_get_my_plan_id()` — current backend's plan identifier.
pub fn pgstat_get_my_plan_id() -> int64 {
    let beentry = MyBEEntry();
    if beentry.is_null() {
        return 0;
    }
    // SAFETY: beentry is the live, shmem-resident MyBEEntry.
    unsafe { (*beentry).st_plan_id }
}

/// `pgstat_get_backend_type_by_proc_number()` — the type of the backend with the
/// specified ProcNumber, read directly from the status array (may be stale).
pub fn pgstat_get_backend_type_by_proc_number(procNumber: ProcNumber) -> BackendType {
    // SAFETY: callers guarantee procNumber is a valid slot index; the array is
    // the shmem-resident BackendStatusArray.
    let status = unsafe {
        BackendStatusArray
            .load(Ordering::Relaxed)
            .add(procNumber as usize)
    };
    // Bypass the changecount mechanism since fetching an int is atomic.
    unsafe { (*status).st_backendType }
}

/// `pgstat_get_beentry_by_proc_number()` — our local copy of the
/// current-activity entry for one backend, or `None`.
pub fn pgstat_get_beentry_by_proc_number(
    procNumber: ProcNumber,
) -> Option<LocalBackendStatusFields> {
    pgstat_get_local_beentry_by_proc_number(procNumber).map(|ret| ret.backend_status)
}

/// `pgstat_get_local_beentry_by_proc_number()` — like the above but with locally
/// computed additions (xid/xmin). `bsearch()` over the proc_number-ordered table.
pub fn pgstat_get_local_beentry_by_proc_number(
    procNumber: ProcNumber,
) -> Option<LocalPgBackendStatus> {
    pgstat_read_current_status();

    localBackendStatusTable.with(|c| {
        let table = c.borrow();
        let table = table.as_ref()?;
        bsearch_proc_number(procNumber, table).cloned()
    })
}

/// `pgstat_get_local_beentry_by_index()` — like
/// `pgstat_get_beentry_by_proc_number()` but indexed by a 1-based index.
pub fn pgstat_get_local_beentry_by_index(idx: i32) -> Option<LocalPgBackendStatus> {
    pgstat_read_current_status();

    localBackendStatusTable.with(|c| {
        let table = c.borrow();
        let table = table.as_ref()?;
        let num = table.len() as i32;
        if idx < 1 || idx > num {
            return None;
        }
        Some(table[(idx - 1) as usize].clone())
    })
}

/// `pgstat_fetch_stat_numbackends()` — the number of sessions known in the
/// `localBackendStatusTable`.
pub fn pgstat_fetch_stat_numbackends() -> i32 {
    pgstat_read_current_status();
    localBackendStatusTable.with(|c| c.borrow().as_ref().map_or(0, |t| t.len() as i32))
}

/// `pgstat_clip_activity()` — convert a potentially unsafely truncated activity
/// string into a correctly truncated one. Returns owned, NUL-free bytes.
/// `raw_activity` is the raw activity bytes (no trailing NUL).
pub fn pgstat_clip_activity(raw_activity: &[u8]) -> Vec<u8> {
    let qsize = pgstat_track_activity_query_size() as usize;
    let cap = qsize - 1;
    let mut activity: Vec<u8> = Vec::new();
    for &b in raw_activity.iter().take(cap) {
        if b == 0 {
            break;
        }
        activity.try_reserve(1).expect("clip_activity allocation");
        activity.push(b);
    }
    let rawlen = activity.len();

    // Multi-byte aware truncation.
    let cliplen = pg_mbcliplen(&activity, rawlen as i32, cap as i32) as usize;
    activity.truncate(cliplen);
    activity
}

// ---------------------------------------------------------------------------
// bsearch helper (faithful `cmp_lbestatus` / `bsearch` over proc_number).
// ---------------------------------------------------------------------------

/// `cmp_lbestatus()` — compare two `LocalPgBackendStatus` on `proc_number`.
fn cmp_lbestatus(a_proc_number: ProcNumber, b_proc_number: ProcNumber) -> i32 {
    a_proc_number - b_proc_number
}

/// `bsearch(...)` specialised for `LocalPgBackendStatus` keyed on `proc_number`.
fn bsearch_proc_number(
    proc_number: ProcNumber,
    base: &[LocalPgBackendStatus],
) -> Option<&LocalPgBackendStatus> {
    let mut lo = 0usize;
    let mut hi = base.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cmp = cmp_lbestatus(proc_number, base[mid].proc_number);
        match cmp.cmp(&0) {
            core::cmp::Ordering::Equal => return Some(&base[mid]),
            core::cmp::Ordering::Less => hi = mid,
            core::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    None
}

// ===========================================================================
// Seam installation
// ===========================================================================

mod seam_impls;

/// Install every seam this crate owns and the two GUC variables it owns.
pub fn init_seams() {
    use backend_utils_activity_status_seams as seams;

    // GUC variables owned by backend_status.c.
    backend_utils_misc_guc_tables::vars::pgstat_track_activities.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: pgstat_track_activities,
            set: set_pgstat_track_activities,
        },
    );
    backend_utils_misc_guc_tables::vars::pgstat_track_activity_query_size.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: pgstat_track_activity_query_size,
            set: set_pgstat_track_activity_query_size,
        },
    );

    seams::backend_status_shmem_size::set(seam_impls::backend_status_shmem_size);
    seams::backend_status_shmem_init::set(seam_impls::backend_status_shmem_init);
    seams::my_be_entry_present::set(seam_impls::my_be_entry_present);
    seams::track_activities::set(pgstat_track_activities);
    seams::with_my_beentry::set(seam_impls::with_my_beentry);
    seams::backend_current_activity::set(seam_impls::backend_current_activity);
    seams::pgstat_beinit::set(pgstat_beinit);
    seams::pgstat_bestart_initial::set(pgstat_bestart_initial);
    seams::pgstat_bestart_security::set(pgstat_bestart_security);
    seams::pgstat_bestart_final::set(pgstat_bestart_final);
    seams::pgstat_report_activity_idle::set(seam_impls::pgstat_report_activity_idle);
    seams::pgstat_report_activity_running::set(seam_impls::pgstat_report_activity_running);
    seams::pgstat_report_query_id::set(|query_id, force| pgstat_report_query_id(query_id as i64, force));
    seams::pgstat_report_plan_id::set(|plan_id, force| pgstat_report_plan_id(plan_id as i64, force));
    seams::pgstat_report_xact_timestamp::set(pgstat_report_xact_timestamp);
    seams::pgstat_get_backend_type_by_proc_number::set(pgstat_get_backend_type_by_proc_number);
}

#[cfg(test)]
mod tests;
