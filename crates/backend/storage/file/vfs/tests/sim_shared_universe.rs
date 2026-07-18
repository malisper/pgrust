//! Provider-seam (dst-p3-scheduler §9) battery: the PROCESS-SHARED sim
//! universe, the boot cwd, and the durable asset-ingest APIs.
//!
//! Run: `RUSTFLAGS='--cfg pgrust_sim' cargo test -p vfs --test sim_shared_universe`
//!
//! DELIBERATELY one #[test]: `install_process_universe` and `set_boot_cwd`
//! are one-shot PROCESS-GLOBAL installers, so all shared-mode assertions
//! must share one process and run in one sequence. This file must never
//! gain a second #[test] that assumes thread-local homing — the installer
//! has already re-homed the whole process by then. Thread-local-mode
//! coverage lives in the vfs unit battery (`sim::tests`), which runs in its
//! own process.
#![cfg(pgrust_sim)]

use std::ffi::CString;

use vfs::sim::SimVfs;
use vfs::Vfs;

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

#[test]
fn shared_universe_boot_cwd_and_ingest() {
    // --- installer: one-shot re-home to the process-shared universe ------
    SimVfs::install_process_universe();

    // --- ingest APIs: scaffold + durable-from-birth file -----------------
    SimVfs::ingest_dir(&c("/snap"), 0o700).unwrap();
    SimVfs::ingest_dir(&c("/snap/dd"), 0o700).unwrap();
    SimVfs::ingest_dir(&c("/snap/dd/global"), 0o700).unwrap();
    SimVfs::ingest_file(&c("/snap/dd/global/pg_control"), b"CTRL", 0o600).unwrap();
    // idempotent dir re-ingest (shared ancestor prefixes)
    SimVfs::ingest_dir(&c("/snap/dd"), 0o700).unwrap();
    // file collision fails loudly: one manifest entry per path
    assert_eq!(
        SimVfs::ingest_file(&c("/snap/dd/global/pg_control"), b"X", 0o600),
        Err(libc::EEXIST)
    );

    // --- boot cwd: relative paths resolve against the datadir ------------
    SimVfs::set_boot_cwd(std::path::Path::new("/snap/dd"));
    let v = SimVfs::new();
    let fd = v.open(&c("global/pg_control"), libc::O_RDONLY, 0);
    assert!(fd >= 0, "relative open through boot cwd");
    let mut buf = [0u8; 4];
    assert_eq!(v.pread(fd, &mut buf, 0), 4);
    assert_eq!(&buf, b"CTRL");
    assert_eq!(v.close(fd), 0);
    // absolute path resolves to the SAME node
    let fd = v.open(&c("/snap/dd/global/pg_control"), libc::O_RDONLY, 0);
    assert!(fd >= 0);
    assert_eq!(v.close(fd), 0);
    // idempotent same-path re-set is allowed
    SimVfs::set_boot_cwd(std::path::Path::new("/snap/dd"));

    // --- cross-thread visibility: the whole point of shared homing -------
    // (under thread-local homing this thread would see an EMPTY namespace)
    let seen = std::thread::spawn(|| {
        let v = SimVfs::new();
        let fd = v.open(&c("/snap/dd/global/pg_control"), libc::O_RDONLY, 0);
        if fd < 0 {
            return None;
        }
        let mut buf = [0u8; 4];
        let n = v.pread(fd, &mut buf, 0);
        v.close(fd);
        (n == 4).then(|| buf.to_vec())
    })
    .join()
    .unwrap();
    assert_eq!(seen.as_deref(), Some(&b"CTRL"[..]), "secondary thread sees the shared disk");

    // --- writes from a secondary thread visible on the main thread -------
    std::thread::spawn(|| {
        let v = SimVfs::new();
        let fd = v.open(
            &c("/snap/dd/global/from_thread"),
            libc::O_CREAT | libc::O_WRONLY,
            0o600,
        );
        assert!(fd >= 0);
        assert_eq!(v.pwrite(fd, b"hi", 0), 2);
        assert_eq!(v.close(fd), 0);
    })
    .join()
    .unwrap();
    let fd = v.open(&c("/snap/dd/global/from_thread"), libc::O_RDONLY, 0);
    assert!(fd >= 0, "main thread sees the secondary thread's write");
    assert_eq!(v.close(fd), 0);

    // --- ingested content is DURABLE from birth: survives a crash --------
    // (the boot write above was never fsync'd, so DropAll discards it; the
    // ingested snapshot is pre-history no fault plan may cut into)
    SimVfs::cut();
    let fd = v.open(&c("/snap/dd/global/pg_control"), libc::O_RDONLY, 0);
    assert!(fd >= 0, "ingested file survives a crash");
    let mut buf = [0u8; 4];
    assert_eq!(v.pread(fd, &mut buf, 0), 4);
    assert_eq!(&buf, b"CTRL", "ingested bytes are the durable image");
    assert_eq!(v.close(fd), 0);
    assert!(
        v.open(&c("/snap/dd/global/from_thread"), libc::O_RDONLY, 0) < 0,
        "unsynced post-ingest write does NOT survive the crash (DropAll)"
    );
}
