#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::sync::atomic::{fence, AtomicI32, Ordering::Acquire, Ordering::Relaxed};
use std::sync::OnceLock;

use init_small::globals as g;
use ip::SockAddr;
use types_core::init::BackendType;
use types_core::{
    InvalidOid, InvalidTransactionId, Oid, ProcNumber, TimestampTz, TransactionId,
    INVALID_PROC_NUMBER, NAMEDATALEN,
};
use types_error::PgResult;
use types_storage::storage::{SyncCell, NUM_AUXILIARY_PROCS};

pub use backend_status_seams::BackendState;

#[cfg(test)]
mod tests;

const NAMELEN: usize = NAMEDATALEN as usize;

pub const PROGRESS_COMMAND_INVALID: i32 = 0;
pub const PGSTAT_NUM_PROGRESS_PARAM: usize = 20;

// PgBackendStatus (backend_status.h). The NAMEDATALEN buffers are inline (C's
// separate shmem buffers only re-fix pointers per process); st_activity_raw is
// a shared runtime-sized buffer indexed by slot. Cross-thread SyncCell reads
// under the st_changecount seqlock (PGPROC precedent); string reads racy by
// design, as in C.
pub struct PgBackendStatus {
    st_changecount: AtomicI32,
    slot: i32,
    pub st_procpid: SyncCell<i32>,
    pub st_backendType: SyncCell<BackendType>,
    pub st_proc_start_timestamp: SyncCell<TimestampTz>,
    pub st_xact_start_timestamp: SyncCell<TimestampTz>,
    pub st_activity_start_timestamp: SyncCell<TimestampTz>,
    pub st_state_start_timestamp: SyncCell<TimestampTz>,
    pub st_databaseid: SyncCell<Oid>,
    pub st_userid: SyncCell<Oid>,
    pub st_clientaddr: SyncCell<SockAddr>,
    pub st_clienthostname: SyncCell<[u8; NAMELEN]>,
    pub st_ssl: SyncCell<bool>,
    pub st_gss: SyncCell<bool>,
    pub st_state: SyncCell<BackendState>,
    pub st_appname: SyncCell<[u8; NAMELEN]>,
    pub st_progress_command: SyncCell<i32>,
    pub st_progress_command_target: SyncCell<Oid>,
    pub st_progress_param: [SyncCell<i64>; PGSTAT_NUM_PROGRESS_PARAM],
    pub st_query_id: SyncCell<i64>,
    pub st_plan_id: SyncCell<i64>,
}

// SAFETY: every field is an atomic or a SyncCell whose cross-thread access is
// bracketed by the st_changecount seqlock (writers) / read-retry protocol
// (readers); string payloads tolerate torn reads per the C contract.
unsafe impl Sync for PgBackendStatus {}

struct ActivityBuffer {
    base: *mut u8,
    query_size: usize,
    total: usize,
}
// SAFETY: raw shared buffer; concurrent access follows the C contract above
// (writers only ever touch their own slot's range).
unsafe impl Sync for ActivityBuffer {}
unsafe impl Send for ActivityBuffer {}

static BACKEND_STATUS_ARRAY: OnceLock<&'static [PgBackendStatus]> = OnceLock::new();
static BACKEND_ACTIVITY_BUFFER: OnceLock<ActivityBuffer> = OnceLock::new();

thread_local! {
    static MY_BE_ENTRY: Cell<Option<&'static PgBackendStatus>> = const { Cell::new(None) };
}

pub fn MyBEEntry() -> Option<&'static PgBackendStatus> {
    MY_BE_ENTRY.get()
}

fn NumBackendStatSlots() -> i32 {
    g::MaxBackends() + NUM_AUXILIARY_PROCS
}

fn track_activities() -> bool {
    guc_tables::vars::pgstat_track_activities.read()
}

fn query_size() -> usize {
    guc_tables::vars::pgstat_track_activity_query_size.read() as usize
}

// pg_write_barrier is store-store only (dmb ishst on aarch64); fence(Release)
// is dmb ish (full barrier) and cost this lane 1.19x ns at 0.96x instr — see
// docs/benchmarks/backend_status.md. Non-aarch64/Miri fall back to the
// strictly stronger fence(Release) (compiler-only on x86, matching C).
#[inline(always)]
fn write_barrier() {
    #[cfg(all(target_arch = "aarch64", not(miri)))]
    // SAFETY: no operands or stack use; omitting `nomem` keeps the implicit
    // memory clobber, so this is a compiler barrier too, exactly C's
    // __asm__ __volatile__("dmb ishst" ::: "memory").
    unsafe {
        core::arch::asm!("dmb ishst", options(nostack, preserves_flags));
    }
    #[cfg(not(all(target_arch = "aarch64", not(miri))))]
    fence(std::sync::atomic::Ordering::Release);
}

// PGSTAT_BEGIN/END_WRITE_ACTIVITY: odd changecount = write in progress.
fn begin_write_activity(e: &PgBackendStatus) {
    g::StartCriticalSection();
    e.st_changecount.store(e.st_changecount.load(Relaxed).wrapping_add(1), Relaxed);
    write_barrier();
}

fn end_write_activity(e: &PgBackendStatus) {
    write_barrier();
    let c = e.st_changecount.load(Relaxed).wrapping_add(1);
    e.st_changecount.store(c, Relaxed);
    debug_assert_eq!(c & 1, 0);
    g::EndCriticalSection();
}

fn begin_read_activity(e: &PgBackendStatus) -> i32 {
    let before = e.st_changecount.load(Relaxed);
    fence(Acquire);
    before
}

fn end_read_activity(e: &PgBackendStatus) -> i32 {
    fence(Acquire);
    e.st_changecount.load(Relaxed)
}

fn read_activity_complete(before: i32, after: i32) -> bool {
    before == after && (before & 1) == 0
}

fn activity_ptr(slot: i32) -> *mut u8 {
    let buf = BACKEND_ACTIVITY_BUFFER
        .get()
        .unwrap_or_else(|| panic!("BackendStatusShmemInit has not run"));
    debug_assert!(((slot as usize + 1) * buf.query_size) <= buf.total);
    // SAFETY: in-bounds by construction (slot < NumBackendStatSlots).
    unsafe { buf.base.add(slot as usize * buf.query_size) }
}

// Single memcpy + NUL inside the caller's write bracket; byte-wise truncation
// per C, multibyte-corrected on read (pgstat_clip_activity).
fn write_activity(slot: i32, s: &[u8]) {
    let qsize = BACKEND_ACTIVITY_BUFFER.get().unwrap().query_size;
    let len = s.len().min(qsize - 1);
    let dst = activity_ptr(slot);
    // SAFETY: dst has qsize bytes; len < qsize; racy readers tolerated per the
    // C contract (a NUL always terminates the buffer).
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), dst, len);
        *dst.add(len) = 0;
    }
}

fn read_activity(slot: i32) -> Vec<u8> {
    let qsize = BACKEND_ACTIVITY_BUFFER.get().unwrap().query_size;
    let src = activity_ptr(slot);
    let mut out = Vec::with_capacity(qsize);
    // SAFETY: src has qsize bytes; concurrent writes only shorten at a NUL.
    unsafe {
        for i in 0..qsize - 1 {
            let b = *src.add(i);
            if b == 0 {
                break;
            }
            out.push(b);
        }
    }
    out
}

fn str_to_name(s: &str) -> [u8; NAMELEN] {
    let mut out = [0u8; NAMELEN];
    let len = s.len().min(NAMELEN - 1);
    out[..len].copy_from_slice(&s.as_bytes()[..len]);
    out
}

fn name_to_string(name: &[u8; NAMELEN]) -> String {
    let len = name.iter().position(|&b| b == 0).unwrap_or(NAMELEN - 1);
    String::from_utf8_lossy(&name[..len]).into_owned()
}

pub fn BackendStatusShmemSize() -> PgResult<usize> {
    let slots = NumBackendStatSlots() as usize;
    let mut size = shmem::mul_size(std::mem::size_of::<PgBackendStatus>(), slots)?;
    size = shmem::add_size(size, shmem::mul_size(NAMELEN, slots)?)?;
    size = shmem::add_size(size, shmem::mul_size(NAMELEN, slots)?)?;
    size = shmem::add_size(size, shmem::mul_size(query_size(), slots)?)?;
    Ok(size)
}

pub fn BackendStatusShmemInit() -> PgResult<()> {
    if BACKEND_STATUS_ARRAY.get().is_some() {
        return Ok(()); // C's found=true attach
    }
    let slots = NumBackendStatSlots();
    let qsize = query_size();
    let total = shmem::mul_size(qsize, slots as usize)?;
    let base = shmem::ShmemAlloc(total)?;
    let _ = BACKEND_ACTIVITY_BUFFER.set(ActivityBuffer { base, query_size: qsize, total });

    let array: Vec<PgBackendStatus> = (0..slots)
        .map(|slot| PgBackendStatus {
            st_changecount: AtomicI32::new(0),
            slot,
            st_procpid: SyncCell::new(0),
            st_backendType: SyncCell::new(BackendType::Invalid),
            st_proc_start_timestamp: SyncCell::new(0),
            st_xact_start_timestamp: SyncCell::new(0),
            st_activity_start_timestamp: SyncCell::new(0),
            st_state_start_timestamp: SyncCell::new(0),
            st_databaseid: SyncCell::new(InvalidOid),
            st_userid: SyncCell::new(InvalidOid),
            st_clientaddr: SyncCell::new(SockAddr::zeroed()),
            st_clienthostname: SyncCell::new([0; NAMELEN]),
            st_ssl: SyncCell::new(false),
            st_gss: SyncCell::new(false),
            st_state: SyncCell::new(BackendState::STATE_UNDEFINED),
            st_appname: SyncCell::new([0; NAMELEN]),
            st_progress_command: SyncCell::new(PROGRESS_COMMAND_INVALID),
            st_progress_command_target: SyncCell::new(InvalidOid),
            st_progress_param: std::array::from_fn(|_| SyncCell::new(0)),
            st_query_id: SyncCell::new(0),
            st_plan_id: SyncCell::new(0),
        })
        .collect();
    let _ = BACKEND_STATUS_ARRAY.set(Vec::leak(array));
    Ok(())
}

/// Crash-cycle reset in place to the post-BackendStatusShmemInit image
/// (notes/crash-restart-design.md); postmaster thread only, all children dead.
pub fn BackendStatusShmemResetAfterCrash() {
    let array = backend_status_array();
    let buf = BACKEND_ACTIVITY_BUFFER
        .get()
        .unwrap_or_else(|| panic!("BackendStatusShmemInit has not run"));
    for e in array {
        e.st_changecount.store(0, Relaxed);
        e.st_procpid.set(0);
        e.st_backendType.set(BackendType::Invalid);
        e.st_proc_start_timestamp.set(0);
        e.st_xact_start_timestamp.set(0);
        e.st_activity_start_timestamp.set(0);
        e.st_state_start_timestamp.set(0);
        e.st_databaseid.set(InvalidOid);
        e.st_userid.set(InvalidOid);
        e.st_clientaddr.set(SockAddr::zeroed());
        e.st_clienthostname.set([0; NAMELEN]);
        e.st_ssl.set(false);
        e.st_gss.set(false);
        e.st_state.set(BackendState::STATE_UNDEFINED);
        e.st_appname.set([0; NAMELEN]);
        e.st_progress_command.set(PROGRESS_COMMAND_INVALID);
        e.st_progress_command_target.set(InvalidOid);
        for p in &e.st_progress_param {
            p.set(0);
        }
        e.st_query_id.set(0);
        e.st_plan_id.set(0);
    }
    // SAFETY: buf spans `total` bytes; exclusive access per the fn contract.
    unsafe { std::ptr::write_bytes(buf.base, 0, buf.total) };
}

fn backend_status_array() -> &'static [PgBackendStatus] {
    BACKEND_STATUS_ARRAY
        .get()
        .unwrap_or_else(|| panic!("BackendStatusShmemInit has not run"))
}

pub fn pgstat_beinit() -> PgResult<()> {
    let procno = g::MyProcNumber();
    assert_ne!(procno, INVALID_PROC_NUMBER);
    let array = backend_status_array();
    assert!(procno >= 0 && (procno as usize) < array.len());
    MY_BE_ENTRY.set(Some(&array[procno as usize]));

    ipc_seams::on_shmem_exit::call(pgstat_beshutdown_hook, 0);
    Ok(())
}

fn my_beentry() -> &'static PgBackendStatus {
    MY_BE_ENTRY
        .get()
        .unwrap_or_else(|| panic!("pgstat_beinit has not run in this backend"))
}

pub fn pgstat_bestart_initial() -> PgResult<()> {
    let beentry = my_beentry();

    let clientaddr = if g::HaveMyProcPort() {
        g::WithMyProcPort(|p| p.raddr)
    } else {
        SockAddr::zeroed()
    };
    let clienthostname = if g::HaveMyProcPort() {
        g::WithMyProcPort(|p| p.remote_hostname.as_deref().map(str_to_name))
            .unwrap_or([0; NAMELEN])
    } else {
        [0; NAMELEN]
    };

    begin_write_activity(beentry);
    beentry.st_procpid.set(g::MyProcPid());
    beentry.st_backendType.set(miscinit::GetMyBackendType());
    beentry.st_proc_start_timestamp.set(g::MyStartTimestamp());
    beentry.st_activity_start_timestamp.set(0);
    beentry.st_state_start_timestamp.set(0);
    beentry.st_xact_start_timestamp.set(0);
    beentry.st_databaseid.set(InvalidOid);
    beentry.st_userid.set(InvalidOid);
    beentry.st_clientaddr.set(clientaddr);
    beentry.st_ssl.set(false);
    beentry.st_gss.set(false);
    beentry.st_state.set(BackendState::STATE_STARTING);
    beentry.st_progress_command.set(PROGRESS_COMMAND_INVALID);
    beentry.st_progress_command_target.set(InvalidOid);
    beentry.st_query_id.set(0);
    beentry.st_plan_id.set(0);
    // st_progress_param stays unzeroed as in C.
    beentry.st_appname.set([0; NAMELEN]);
    beentry.st_clienthostname.set(clienthostname);
    write_activity(beentry.slot, b"");
    end_write_activity(beentry);
    Ok(())
}

pub fn pgstat_bestart_security() -> PgResult<()> {
    let beentry = my_beentry();
    assert!(g::HaveMyProcPort());

    // No USE_SSL / ENABLE_GSS arms in this build: both report false, as C.
    begin_write_activity(beentry);
    beentry.st_ssl.set(false);
    beentry.st_gss.set(false);
    end_write_activity(beentry);
    Ok(())
}

pub fn pgstat_bestart_final() -> PgResult<()> {
    let beentry = my_beentry();

    let btype = miscinit::GetMyBackendType();
    let userid = if btype == BackendType::Backend
        || btype == BackendType::WalSender
        || btype == BackendType::BgWorker
    {
        miscinit::GetSessionUserId()
    } else {
        InvalidOid
    };

    begin_write_activity(beentry);
    beentry.st_databaseid.set(g::MyDatabaseId());
    beentry.st_userid.set(userid);
    beentry.st_state.set(BackendState::STATE_UNDEFINED);
    end_write_activity(beentry);

    if pgstat::backend::pgstat_tracks_backend_bktype(btype) {
        pgstat::backend::pgstat_create_backend(g::MyProcNumber());
    }

    if guc_tables::vars::application_name.installed() {
        if let Some(appname) = guc_tables::vars::application_name.read() {
            pgstat_report_appname(&appname);
        }
    }
    Ok(())
}

fn pgstat_beshutdown_hook(_code: i32, _arg: usize) {
    let Some(beentry) = MY_BE_ENTRY.get() else { return };
    begin_write_activity(beentry);
    beentry.st_procpid.set(0);
    end_write_activity(beentry);
    MY_BE_ENTRY.set(None);
}

pub fn pgstat_report_activity(state: BackendState, cmd_str: Option<&str>) {
    let Some(beentry) = MY_BE_ENTRY.get() else { return };

    if !track_activities() {
        if beentry.st_state.get() != BackendState::STATE_DISABLED {
            begin_write_activity(beentry);
            beentry.st_state.set(BackendState::STATE_DISABLED);
            beentry.st_state_start_timestamp.set(0);
            write_activity(beentry.slot, b"");
            beentry.st_activity_start_timestamp.set(0);
            beentry.st_xact_start_timestamp.set(0);
            beentry.st_query_id.set(0);
            beentry.st_plan_id.set(0);
            if let Some(procno) = lmgr_proc::MyProc() {
                lmgr_proc::GetPGProcByNumber(procno).wait_event_info.store(0, Relaxed);
            }
            end_write_activity(beentry);
        }
        return;
    }

    let start_timestamp = xact::GetCurrentStatementStartTimestamp();
    let current_timestamp = adt_timestamp::GetCurrentTimestamp();

    let prev_state = beentry.st_state.get();
    if (prev_state == BackendState::STATE_RUNNING
        || prev_state == BackendState::STATE_FASTPATH
        || prev_state == BackendState::STATE_IDLEINTRANSACTION
        || prev_state == BackendState::STATE_IDLEINTRANSACTION_ABORTED)
        && state != prev_state
    {
        let (secs, usecs) =
            adt_timestamp::TimestampDifference(beentry.st_state_start_timestamp.get(), current_timestamp);
        let micros = secs * 1_000_000 + usecs as i64;
        if prev_state == BackendState::STATE_RUNNING || prev_state == BackendState::STATE_FASTPATH {
            pgstat::database::pgstat_count_conn_active_time(micros);
        } else {
            pgstat::database::pgstat_count_conn_txn_idle_time(micros);
        }
    }

    begin_write_activity(beentry);
    beentry.st_state.set(state);
    beentry.st_state_start_timestamp.set(current_timestamp);

    // A new query resets the ids: only known after parse analysis.
    if state == BackendState::STATE_RUNNING {
        beentry.st_query_id.set(0);
        beentry.st_plan_id.set(0);
    }

    if let Some(cmd) = cmd_str {
        write_activity(beentry.slot, cmd.as_bytes());
        beentry.st_activity_start_timestamp.set(start_timestamp);
    }
    end_write_activity(beentry);
}

pub fn pgstat_report_query_id(query_id: i64, force: bool) {
    let Some(beentry) = MY_BE_ENTRY.get() else { return };
    if !track_activities() {
        return;
    }
    // Top-level ids only: nonzero saved id = nested command, unless forced.
    if beentry.st_query_id.get() != 0 && !force {
        return;
    }
    begin_write_activity(beentry);
    beentry.st_query_id.set(query_id);
    end_write_activity(beentry);
}

pub fn pgstat_report_plan_id(plan_id: i64, force: bool) {
    let Some(beentry) = MY_BE_ENTRY.get() else { return };
    if !track_activities() {
        return;
    }
    if beentry.st_plan_id.get() != 0 && !force {
        return;
    }
    begin_write_activity(beentry);
    beentry.st_plan_id.set(plan_id);
    end_write_activity(beentry);
}

pub fn pgstat_report_appname(appname: &str) {
    let Some(beentry) = MY_BE_ENTRY.get() else { return };
    let len = mbutils::pg_mbcliplen(appname.as_bytes(), appname.len() as i32, NAMEDATALEN - 1);
    let mut name = [0u8; NAMELEN];
    name[..len as usize].copy_from_slice(&appname.as_bytes()[..len as usize]);

    begin_write_activity(beentry);
    beentry.st_appname.set(name);
    end_write_activity(beentry);
}

pub fn pgstat_report_xact_timestamp(tstamp: TimestampTz) {
    let Some(beentry) = MY_BE_ENTRY.get() else { return };
    if !track_activities() {
        return;
    }
    begin_write_activity(beentry);
    beentry.st_xact_start_timestamp.set(tstamp);
    end_write_activity(beentry);
}

pub fn pgstat_get_my_query_id() -> i64 {
    MY_BE_ENTRY.get().map_or(0, |e| e.st_query_id.get())
}

pub fn pgstat_get_my_plan_id() -> i64 {
    MY_BE_ENTRY.get().map_or(0, |e| e.st_plan_id.get())
}

pub fn pgstat_get_backend_type_by_proc_number(procNumber: ProcNumber) -> BackendType {
    backend_status_array()[procNumber as usize].st_backendType.get()
}

pub fn pgstat_get_backend_current_activity(pid: i32, check_user: bool) -> PgResult<String> {
    let array = backend_status_array();
    for beentry in array.iter().take(g::MaxBackends() as usize) {
        let found = loop {
            let before = begin_read_activity(beentry);
            let found = beentry.st_procpid.get() == pid;
            let after = end_read_activity(beentry);
            if read_activity_complete(before, after) {
                break found;
            }
            std::hint::spin_loop();
        };

        if found {
            if check_user
                && !superuser_seams::superuser::call()?
                && beentry.st_userid.get() != miscinit::GetUserId()
            {
                return Ok("<insufficient privilege>".into());
            }
            let raw = read_activity(beentry.slot);
            if raw.is_empty() {
                return Ok("<command string not enabled>".into());
            }
            return Ok(pgstat_clip_activity(&raw));
        }
    }
    Ok("<backend information not available>".into())
}

pub fn pgstat_get_crashed_backend_activity(pid: i32) -> Option<String> {
    let array = BACKEND_STATUS_ARRAY.get()?;
    for beentry in array.iter().take(g::MaxBackends() as usize) {
        if beentry.st_procpid.get() == pid {
            let raw = read_activity(beentry.slot);
            if raw.is_empty() {
                return None;
            }
            let safe: String = raw
                .iter()
                .map(|&b| if (32..127).contains(&b) { b as char } else { '?' })
                .collect();
            return Some(safe);
        }
    }
    None
}

// Multibyte-aware truncation of a possibly mid-character truncated buffer.
pub fn pgstat_clip_activity(raw_activity: &[u8]) -> String {
    let limit = (query_size() - 1).min(raw_activity.len());
    let raw = &raw_activity[..limit];
    let cliplen = mbutils::pg_mbcliplen(raw, raw.len() as i32, (query_size() - 1) as i32);
    String::from_utf8_lossy(&raw[..cliplen as usize]).into_owned()
}

pub fn appname_of(beentry: &PgBackendStatus) -> String {
    name_to_string(&beentry.st_appname.get())
}

#[derive(Clone)]
pub struct LocalPgBackendStatus {
    pub st_procpid: i32,
    pub st_backendType: BackendType,
    pub st_proc_start_timestamp: TimestampTz,
    pub st_xact_start_timestamp: TimestampTz,
    pub st_activity_start_timestamp: TimestampTz,
    pub st_state_start_timestamp: TimestampTz,
    pub st_databaseid: Oid,
    pub st_userid: Oid,
    pub st_clientaddr: SockAddr,
    pub st_clienthostname: String,
    pub st_ssl: bool,
    pub st_gss: bool,
    pub st_state: BackendState,
    pub st_appname: String,
    pub st_activity_raw: String,
    pub st_progress_command: i32,
    pub st_progress_command_target: Oid,
    pub st_progress_param: [i64; PGSTAT_NUM_PROGRESS_PARAM],
    pub st_query_id: i64,
    pub st_plan_id: i64,
    pub proc_number: ProcNumber,
    pub backend_xid: TransactionId,
    pub backend_xmin: TransactionId,
    pub backend_subxact_count: i32,
    pub backend_subxact_overflowed: bool,
}

thread_local! {
    // Cold stats path: std Vec/String stand in for C's palloc'd
    // localBackendStatusTable, which also lives outside the query arenas.
    static LOCAL_BACKEND_STATUS_TABLE: RefCell<Option<Vec<LocalPgBackendStatus>>> =
        const { RefCell::new(None) };
}

// Ascending-slot iteration keeps the table sorted by proc_number;
// pgstat_get_beentry_by_proc_number's binary search depends on that.
fn pgstat_read_current_status() {
    if LOCAL_BACKEND_STATUS_TABLE.with(|t| t.borrow().is_some()) {
        return;
    }
    let array = backend_status_array();
    let mut table: Vec<LocalPgBackendStatus> = Vec::new();
    for (slot, beentry) in array.iter().enumerate() {
        let entry = loop {
            let before = begin_read_activity(beentry);
            let procpid = beentry.st_procpid.get();
            // Skip the copy work if the entry is not in use, but still
            // validate the changecount bracket, as C does.
            let entry = (procpid > 0).then(|| LocalPgBackendStatus {
                st_procpid: procpid,
                st_backendType: beentry.st_backendType.get(),
                st_proc_start_timestamp: beentry.st_proc_start_timestamp.get(),
                st_xact_start_timestamp: beentry.st_xact_start_timestamp.get(),
                st_activity_start_timestamp: beentry.st_activity_start_timestamp.get(),
                st_state_start_timestamp: beentry.st_state_start_timestamp.get(),
                st_databaseid: beentry.st_databaseid.get(),
                st_userid: beentry.st_userid.get(),
                st_clientaddr: beentry.st_clientaddr.get(),
                st_clienthostname: name_to_string(&beentry.st_clienthostname.get()),
                st_ssl: beentry.st_ssl.get(),
                st_gss: beentry.st_gss.get(),
                st_state: beentry.st_state.get(),
                st_appname: name_to_string(&beentry.st_appname.get()),
                st_activity_raw: String::from_utf8_lossy(&read_activity(beentry.slot))
                    .into_owned(),
                st_progress_command: beentry.st_progress_command.get(),
                st_progress_command_target: beentry.st_progress_command_target.get(),
                st_progress_param: std::array::from_fn(|i| beentry.st_progress_param[i].get()),
                st_query_id: beentry.st_query_id.get(),
                st_plan_id: beentry.st_plan_id.get(),
                proc_number: slot as ProcNumber,
                backend_xid: InvalidTransactionId,
                backend_xmin: InvalidTransactionId,
                backend_subxact_count: 0,
                backend_subxact_overflowed: false,
            });
            let after = end_read_activity(beentry);
            if read_activity_complete(before, after) {
                break entry;
            }
            std::hint::spin_loop();
        };
        if let Some(mut local) = entry {
            let (xid, xmin, nsubxid, overflowed) =
                procarray::ProcNumberGetTransactionIds(local.proc_number);
            local.backend_xid = xid;
            local.backend_xmin = xmin;
            local.backend_subxact_count = nsubxid;
            local.backend_subxact_overflowed = overflowed;
            table.push(local);
        }
    }
    LOCAL_BACKEND_STATUS_TABLE.with(|t| *t.borrow_mut() = Some(table));
}

pub fn pgstat_fetch_stat_numbackends() -> i32 {
    pgstat_read_current_status();
    LOCAL_BACKEND_STATUS_TABLE.with(|t| t.borrow().as_ref().unwrap().len() as i32)
}

// C hands out pointers into the local table; owned clones per row on this
// cold stats path.
pub fn pgstat_get_local_beentry_by_index(idx: i32) -> Option<LocalPgBackendStatus> {
    pgstat_read_current_status();
    if idx < 1 {
        return None;
    }
    LOCAL_BACKEND_STATUS_TABLE
        .with(|t| t.borrow().as_ref().unwrap().get(idx as usize - 1).cloned())
}

pub fn pgstat_get_beentry_by_proc_number(proc_number: ProcNumber) -> Option<LocalPgBackendStatus> {
    pgstat_read_current_status();
    LOCAL_BACKEND_STATUS_TABLE.with(|t| {
        let table = t.borrow();
        let table = table.as_ref().unwrap();
        table
            .binary_search_by_key(&proc_number, |e| e.proc_number)
            .ok()
            .map(|i| table[i].clone())
    })
}

pub fn pgstat_clear_backend_status_snapshot() {
    LOCAL_BACKEND_STATUS_TABLE.with(|t| *t.borrow_mut() = None);
}

// The two GUCs backend_status.c owns; C static initializers, overwritten by
// GUC boot values.
static TRACK_ACTIVITIES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static TRACK_ACTIVITY_QUERY_SIZE: AtomicI32 = AtomicI32::new(1024);

pub fn init_seams() {
    use backend_status_seams as s;
    guc_tables::vars::pgstat_track_activities.install(guc_tables::GucVarAccessors {
        get: || TRACK_ACTIVITIES.load(Relaxed),
        set: |v| TRACK_ACTIVITIES.store(v, Relaxed),
    });
    guc_tables::vars::pgstat_track_activity_query_size.install(guc_tables::GucVarAccessors {
        get: || TRACK_ACTIVITY_QUERY_SIZE.load(Relaxed),
        set: |v| TRACK_ACTIVITY_QUERY_SIZE.store(v, Relaxed),
    });
    s::backend_status_shmem_size::set(BackendStatusShmemSize);
    s::backend_status_shmem_init::set(BackendStatusShmemInit);
    s::backend_status_shmem_reset_after_crash::set(BackendStatusShmemResetAfterCrash);
    s::pgstat_beinit::set(pgstat_beinit);
    s::pgstat_bestart_initial::set(pgstat_bestart_initial);
    s::pgstat_bestart_security::set(pgstat_bestart_security);
    s::pgstat_bestart_final::set(pgstat_bestart_final);
    s::pgstat_report_activity::set(pgstat_report_activity);
    s::pgstat_report_query_id::set(pgstat_report_query_id);
    s::pgstat_report_plan_id::set(pgstat_report_plan_id);
    s::pgstat_report_xact_timestamp::set(pgstat_report_xact_timestamp);
    s::pgstat_clear_backend_status_snapshot::set(pgstat_clear_backend_status_snapshot);
}
