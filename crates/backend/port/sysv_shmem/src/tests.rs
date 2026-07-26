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

    let dir = scratch_datadir("seam");
    let mut seg = Segment::create();
    seg.write_postgres_header(&dir);
    assert!(shmem_seams::pg_shared_memory_is_in_use::call(0, seg.id as u64).unwrap());
}
