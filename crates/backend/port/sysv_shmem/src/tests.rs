//! The probe's decision tree, driven with real System V segments.
//!
//! GL-SHMSEAM-1: before this crate existed the seam had no implementation at
//! all, so every one of these cases panicked ("seam not installed") on the
//! migrate-from-C boot path. Release-effective: no debug_assert anywhere here.

use std::sync::Once;

use types_storage::{PGShmemHeader, PGShmemMagic};

use crate::{IpcMemoryState, PGSharedMemoryAttach, PGSharedMemoryIsInUse};

/// A segment this test owns, removed on drop even if the test panics.
struct Segment {
    id: libc::c_int,
    attached: Option<*mut libc::c_void>,
}

impl Segment {
    fn create() -> Segment {
        // SAFETY: IPC_PRIVATE always mints a fresh key; no shared state.
        let id = unsafe {
            libc::shmget(
                libc::IPC_PRIVATE,
                std::mem::size_of::<PGShmemHeader>(),
                libc::IPC_CREAT | libc::IPC_EXCL | 0o600,
            )
        };
        assert!(
            id >= 0,
            "shmget failed: {} — this environment has no System V shared memory, \
             which the migrate-from-C interlock cannot be tested without",
            std::io::Error::last_os_error()
        );
        Segment { id, attached: None }
    }

    fn attach(&mut self) -> *mut libc::c_void {
        // SAFETY: our own segment, kernel-chosen address.
        let addr = unsafe { libc::shmat(self.id, std::ptr::null(), 0) };
        assert!(addr as isize != -1, "shmat failed: {}", std::io::Error::last_os_error());
        self.attached = Some(addr);
        addr
    }

    fn detach(&mut self) {
        if let Some(addr) = self.attached.take() {
            // SAFETY: the mapping we made in `attach`.
            assert_eq!(unsafe { libc::shmdt(addr) }, 0);
        }
    }

    /// Writes the header a live C postmaster would have written for `datadir`.
    fn write_postgres_header(&mut self, datadir: &str) {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(datadir).unwrap();
        let addr = self.attach();
        let hdr = PGShmemHeader {
            magic: PGShmemMagic,
            creatorPID: 424242,
            totalsize: 0,
            freeoffset: 0,
            dsm_control: 0,
            index: std::ptr::null_mut(),
            device: meta.dev() as libc::dev_t,
            inode: meta.ino() as libc::ino_t,
        };
        // SAFETY: `addr` maps size_of::<PGShmemHeader>() bytes we just created.
        unsafe { std::ptr::write(addr as *mut PGShmemHeader, hdr) };
    }

    fn remove(&mut self) {
        self.detach();
        if self.id >= 0 {
            // SAFETY: our own segment id.
            unsafe { libc::shmctl(self.id, libc::IPC_RMID, std::ptr::null_mut()) };
            self.id = -1;
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        self.remove();
    }
}

/// `PGSharedMemoryAttach` + the detach its only C caller always performs; a
/// probe that leaked its mapping would inflate shm_nattch for the next probe.
fn probe_state(id: libc::c_int) -> IpcMemoryState {
    let (state, addr) = PGSharedMemoryAttach(id);
    if !addr.is_null() {
        // SAFETY: the mapping the probe just returned.
        assert_eq!(unsafe { libc::shmdt(addr) }, 0);
    }
    state
}

fn scratch_datadir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("pgrust_sysvshmem_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir = dir.to_str().unwrap().to_owned();
    // DataDir is thread-local, so every test owns its own (globals.rs).
    init_small::globals::SetDataDir(&dir);
    dir
}

#[test]
fn removed_segment_reads_as_enoent_not_in_use() {
    scratch_datadir("enoent");
    let mut seg = Segment::create();
    let id = seg.id;
    seg.remove();

    let (state, addr) = PGSharedMemoryAttach(id);
    assert_eq!(state, IpcMemoryState::Enoent);
    assert!(addr.is_null());
    assert!(!PGSharedMemoryIsInUse(0, id as u64).unwrap());
}

#[test]
fn segment_without_our_header_is_foreign_not_in_use() {
    scratch_datadir("foreign");
    // Fresh segments are zero-filled: magic 0 != PGShmemMagic, exactly the
    // "not a Postgres segment" arm.
    let mut seg = Segment::create();
    let addr = seg.attach();
    assert!(!addr.is_null());

    let (state, probe_addr) = PGSharedMemoryAttach(seg.id);
    assert_eq!(state, IpcMemoryState::Foreign);
    // C sets *addr before the identity test, so the caller detaches it.
    assert!(!probe_addr.is_null());
    // SAFETY: the probe's own mapping, which the real caller also detaches.
    assert_eq!(unsafe { libc::shmdt(probe_addr) }, 0);

    assert!(!PGSharedMemoryIsInUse(0, seg.id as u64).unwrap());
}

#[test]
fn our_datadirs_segment_with_a_live_attachment_is_in_use() {
    let dir = scratch_datadir("attached");
    let mut seg = Segment::create();
    seg.write_postgres_header(&dir);
    // Attachment stays: this is the orphaned-backend shape — the postmaster is
    // gone but a child still holds the segment.
    assert_eq!(probe_state(seg.id), IpcMemoryState::Attached);
    assert!(PGSharedMemoryIsInUse(0, seg.id as u64).unwrap());
}

#[test]
fn our_datadirs_segment_with_no_attachment_is_recyclable() {
    let dir = scratch_datadir("unattached");
    let mut seg = Segment::create();
    seg.write_postgres_header(&dir);
    seg.detach();

    assert_eq!(probe_state(seg.id), IpcMemoryState::Unattached);
    assert!(!PGSharedMemoryIsInUse(0, seg.id as u64).unwrap());
}

#[test]
fn a_matching_header_for_another_datadir_is_foreign() {
    let other = scratch_datadir("other-datadir");
    let mut seg = Segment::create();
    seg.write_postgres_header(&other);
    // Same segment, different data directory: the device/inode test is what
    // keeps an accidental key match from blocking an unrelated cluster.
    let mine = scratch_datadir("my-datadir");
    assert_ne!(mine, other);
    assert_eq!(probe_state(seg.id), IpcMemoryState::Foreign);
    assert!(!PGSharedMemoryIsInUse(0, seg.id as u64).unwrap());
}

#[test]
fn unstattable_datadir_is_conservatively_in_use() {
    init_small::globals::SetDataDir("/nonexistent/pgrust-shmseam-probe");
    let mut seg = Segment::create();
    let dir = std::env::temp_dir();
    let dir = dir.to_str().unwrap().to_owned();
    seg.write_postgres_header(&dir);

    // C: "can't stat; be conservative" -> ANALYSIS_FAILURE -> in use.
    assert_eq!(probe_state(seg.id), IpcMemoryState::AnalysisFailure);
    assert!(PGSharedMemoryIsInUse(0, seg.id as u64).unwrap());
}

// The seam slot is process-global and set-once, so exactly one test may install.
static INSTALL: Once = Once::new();

#[test]
fn the_seam_is_installed_by_init_seams() {
    assert!(!shmem_seams::pg_shared_memory_is_in_use::is_installed());
    INSTALL.call_once(crate::init_seams);
    assert!(shmem_seams::pg_shared_memory_is_in_use::is_installed());
    // check_huge_page_size (sysv_shmem.c:578) must be wired into its GUC slot
    // by the same init_seams: guc treats an uninstalled check-hook slot as
    // "no check hook" and silently accepts any huge_page_size (audit
    // a186-candidate-fp-port-sysv_shmem-c922fc56eca70a2a4d9c-1).
    assert!(guc_tables::hooks::check_huge_page_size.installed());
    let hook = guc_tables::hooks::check_huge_page_size.get();
    let mut extra = None;
    let mut z = 0;
    assert!(hook(&mut z, &mut extra, types_guc::GucSource::PGC_S_TEST).unwrap());
    let mut v = 2048;
    let ok = hook(&mut v, &mut extra, types_guc::GucSource::PGC_S_TEST).unwrap();
    assert_eq!(ok, cfg!(any(target_os = "linux", target_os = "android")));

    let dir = scratch_datadir("seam");
    let mut seg = Segment::create();
    seg.write_postgres_header(&dir);
    assert!(shmem_seams::pg_shared_memory_is_in_use::call(0, seg.id as u64).unwrap());
}

// check_huge_page_size (sysv_shmem.c:578-591): non-zero sizes are accepted
// only where MAP_HUGE_MASK/MAP_HUGE_SHIFT exist; 0 always passes. Audit
// a186-candidate-fp-port-sysv_shmem-c922fc56eca70a2a4d9c-1.
#[test]
fn check_huge_page_size_platform_gate() {
    use crate::{check_huge_page_size_value, HUGE_PAGE_SIZE_SELECTABLE};
    assert_eq!(check_huge_page_size_value(0, true), Ok(()));
    assert_eq!(check_huge_page_size_value(0, false), Ok(()));
    assert_eq!(check_huge_page_size_value(2048, true), Ok(()));
    assert_eq!(
        check_huge_page_size_value(2048, false),
        Err("\"huge_page_size\" must be 0 on this platform.")
    );
    assert_eq!(HUGE_PAGE_SIZE_SELECTABLE, cfg!(any(target_os = "linux", target_os = "android")));
}

// ---------------------------------------------------------------------------
// PGSharedMemoryCreate's startup interlocks (sysv_shmem.c:702-870). Audit rows
// a186-candidate-fp-port-sysv_shmem-{79ac1c53,53018cea,78cb3328}-1.

impl Segment {
    /// A segment at a chosen key: the shape a C postmaster leaves behind for
    /// the data directory whose inode the key is.
    fn create_keyed(key: libc::key_t) -> Segment {
        // SAFETY: IPC_CREAT|IPC_EXCL mints a segment at exactly this key or
        // fails; no shared state.
        let id = unsafe {
            libc::shmget(
                key,
                std::mem::size_of::<PGShmemHeader>(),
                libc::IPC_CREAT | libc::IPC_EXCL | 0o600,
            )
        };
        assert!(
            id >= 0,
            "shmget(key {key}) failed: {} — a segment already sits at this scratch directory's inode",
            std::io::Error::last_os_error()
        );
        Segment { id, attached: None }
    }

    fn exists(&self) -> bool {
        let mut st: libc::shmid_ds = unsafe { std::mem::zeroed() };
        // SAFETY: IPC_STAT only writes the caller-owned shmid_ds.
        unsafe { libc::shmctl(self.id, libc::IPC_STAT, &mut st) == 0 }
    }
}

fn datadir_ino(dir: &str) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(dir).unwrap().ino()
}

static DSM_CLEANUP_SEEN: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
fn record_dsm_cleanup(handle: u32) -> types_error::PgResult<()> {
    DSM_CLEANUP_SEEN.store(handle, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

/// What the FATAL path reported before exiting: (sqlstate, message, hint).
/// A process-wide record (no new thread_local: the census is pinned); the
/// emit hook itself is per-thread, so only this test's own reports land here.
static EMITTED: std::sync::Mutex<Vec<(types_error::SqlState, String, Option<String>)>> =
    std::sync::Mutex::new(Vec::new());

fn record_emitted(error: &types_error::PgError, _output_to_server: &mut bool) {
    EMITTED.lock().unwrap().push((error.sqlstate, error.message.clone(), error.hint.clone()));
}

/// The FATAL path (elog stack.rs) reports, then proc_exit(1)s through the
/// ipc seam: install it as a panic so the refusal is observable, as
/// miscinit's first-contact tests do.
fn setup_fatal_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        elog::init_seams();
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        init_small_seams::my_proc_pid::set(|| std::process::id() as i32);
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
    });
}

/// Runs `f`, which must refuse with a FATAL; returns the exit payload and
/// what was reported.
fn refuses(
    f: impl FnOnce() -> types_error::PgResult<()>,
) -> (String, (types_error::SqlState, String, Option<String>)) {
    setup_fatal_seams();
    EMITTED.lock().unwrap().clear();
    let previous = elog::set_emit_log_hook(Some(record_emitted));
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    elog::set_emit_log_hook(previous);
    let payload = match unwound {
        Ok(r) => panic!("expected a FATAL refusal; the call returned {r:?} instead"),
        Err(payload) => payload,
    };
    let exit = match payload.downcast::<String>() {
        Ok(s) => *s,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(s) => (*s).to_owned(),
            Err(_) => "<non-string panic payload>".to_owned(),
        },
    };
    let reported = EMITTED.lock().unwrap().last().cloned().expect("the FATAL was reported before exiting");
    (exit, reported)
}

// sysv_shmem.c:796-804: a segment keyed by DataDir's inode, carrying our
// header, with a process still attached (the orphaned-backend shape after
// postmaster.pid was removed) is FATAL F0001 with C's message and hint; once
// nobody is attached the walk recycles it (dsm cleanup on its control handle,
// then IPC_RMID) and startup proceeds.
#[test]
fn create_walk_refuses_a_datadir_segment_still_in_use_then_recycles_it() {
    use crate::probe_key_space;

    let dir = scratch_datadir("walk-attached");
    let ino = datadir_ino(&dir);
    let key = ino as libc::key_t;
    let mut seg = Segment::create_keyed(key);
    seg.write_postgres_header(&dir);
    // SAFETY: write_postgres_header left our own mapping attached.
    unsafe { (*(seg.attached.unwrap() as *mut PGShmemHeader)).dsm_control = 0x2a };

    let (exit, (sqlstate, message, hint)) = refuses(|| probe_key_space(&dir, ino, record_dsm_cleanup));
    assert_eq!(exit, "proc_exit(1)");
    assert_eq!(sqlstate, types_error::ERRCODE_LOCK_FILE_EXISTS, "{message}");
    assert_eq!(
        message,
        format!(
            "pre-existing shared memory block (key {}, ID {}) is still in use",
            key as i64 as u64,
            seg.id
        )
    );
    assert_eq!(
        hint.as_deref(),
        Some(format!("Terminate any old server processes associated with data directory \"{dir}\".").as_str())
    );
    assert!(seg.exists(), "a refused segment must be left alone");

    seg.detach();
    probe_key_space(&dir, ino, record_dsm_cleanup).unwrap();
    assert_eq!(DSM_CLEANUP_SEEN.load(std::sync::atomic::Ordering::Relaxed), 0x2a);
    assert!(!seg.exists(), "an unattached segment of this data directory is recycled");
    seg.id = -1;
}

// sysv_shmem.c:826-828: a segment at our seed key that is not ours (no
// header) is FOREIGN — the walk steps to the next key and leaves it alone.
#[test]
fn create_walk_steps_past_a_foreign_segment_at_the_seed_key() {
    use crate::probe_key_space;

    let dir = scratch_datadir("walk-foreign");
    let ino = datadir_ino(&dir);
    let mut seg = Segment::create_keyed(ino as libc::key_t);
    seg.attach();

    probe_key_space(&dir, ino, record_dsm_cleanup).unwrap();
    assert!(seg.exists(), "a foreign segment is never zapped");
}

// sysv_shmem.c:722-733, in C's order; plus pgrust's typed refusal where C
// would go on to mmap the main region with MAP_HUGETLB.
#[test]
fn huge_pages_on_is_refused_like_c() {
    use crate::{huge_pages_startup_gate, MAP_HUGETLB_AVAILABLE};
    use guc_tables::consts::{HUGE_PAGES_OFF, HUGE_PAGES_ON, HUGE_PAGES_TRY, SHMEM_TYPE_MMAP, SHMEM_TYPE_SYSV};

    const PLATFORM: &str = "huge pages not supported on this platform";
    const SHMTYPE: &str = "huge pages not supported with the current \"shared_memory_type\" setting";

    for hp in [HUGE_PAGES_OFF, HUGE_PAGES_TRY] {
        for smt in [SHMEM_TYPE_MMAP, SHMEM_TYPE_SYSV] {
            for hugetlb in [false, true] {
                assert_eq!(huge_pages_startup_gate(hp, smt, hugetlb), Ok(()));
            }
        }
    }
    // Without MAP_HUGETLB the platform check comes first, whatever the type.
    assert_eq!(huge_pages_startup_gate(HUGE_PAGES_ON, SHMEM_TYPE_MMAP, false), Err(PLATFORM));
    assert_eq!(huge_pages_startup_gate(HUGE_PAGES_ON, SHMEM_TYPE_SYSV, false), Err(PLATFORM));
    // With it, a non-mmap type is C's second refusal ...
    assert_eq!(huge_pages_startup_gate(HUGE_PAGES_ON, SHMEM_TYPE_SYSV, true), Err(SHMTYPE));
    // ... and mmap + on, which C honours with MAP_HUGETLB, is pgrust's typed
    // refusal: no region exists to map.
    assert_eq!(huge_pages_startup_gate(HUGE_PAGES_ON, SHMEM_TYPE_MMAP, true), Err(PLATFORM));
    assert_eq!(MAP_HUGETLB_AVAILABLE, cfg!(any(target_os = "linux", target_os = "android")));
}
