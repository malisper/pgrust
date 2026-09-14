// xlog.c:3235-3239: XLogFileInitInternal reports a failed temp-segment
// create as "could not create file" with the open's errno (pre-fix pgrust
// carried the -1 descriptor into the zero-fill and reported EBADF).
// xlog.c:3776/3835/3884: the pg_wal sweeps read through ReadDir, which
// raises ERROR for an unopenable directory (pre-fix pgrust returned
// success). Permission-based fault injection, skipped for root.
//
// NATIVE ARM ONLY: std::fs fixture plumbing + the fd data plane (see
// upstream_13f940b4_wal_init_io_stats.rs for the same split).
#![cfg(not(pgrust_sim))]

use std::os::unix::fs::PermissionsExt;

#[test]
fn wal_dir_failures_report_like_c() {
    // SAFETY: plain libc query of this process's effective uid.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let dir = std::env::temp_dir().join(format!("pgrust_wal_dir_errors_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal/archive_status", "pg_wal/summaries"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    // XLOGDIR paths are DataDir-relative.
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);
    shmem::init_seams();
    guc_tables::init_seams();
    transam_xlog::init_seams();
    fd::InitFileAccess();
    let wal = dir.join("pg_wal");

    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o555)).unwrap();
    let err = transam_xlog::write::XLogFileInit(1, 1).unwrap_err();
    let tmppath = format!("pg_wal/xlogtemp.{}", init_small::globals::process_id());
    assert_eq!(
        err.message(),
        format!("could not create file \"{tmppath}\": Permission denied"),
        "{err}"
    );
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INSUFFICIENT_PRIVILEGE);

    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o000)).unwrap();
    let err = transam_xlog::RemoveNonParentXlogFiles(0x100_0000, 2).unwrap_err();
    assert_eq!(err.message(), "could not open directory \"pg_wal\": Permission denied", "{err}");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INSUFFICIENT_PRIVILEGE);

    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
