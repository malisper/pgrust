// upstream 13f940b4b56f (18.6): Fix pgstat_count_io_op_time() calls passing
// incorrect information. XLogFileInitInternal reported a full segment's worth
// of WAL init writes to pg_stat_io BEFORE checking whether the zero-fill had
// succeeded, so a failed segment initialization (an ERROR -- recoverable, the
// pending stats are flushed later) left a bogus wal_segment_size-byte write
// op in pg_stat_io. C 18.6 aggregates the I/O numbers only after the write is
// known to have succeeded.
//
// Own process (integration test): the write failure is injected through
// RLIMIT_FSIZE, which is process-wide.
//
// NATIVE ARM ONLY: std::fs fixture plumbing + the fd data plane (see
// xlogrecovery's tests/crash_recovery.rs for the same split).
#![cfg(not(pgrust_sim))]

use pgstat::io::{IOContext, IOObject, IOOp};

#[test]
fn failed_segment_init_write_is_not_counted() {
    let dir = std::env::temp_dir().join(format!("pgrust_xlog_init_io_{}", std::process::id()));
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

    // Inject the failure: cap this process's file size at 1 MB (below the
    // 16 MB segment zero-fill) and ignore SIGXFSZ so the write comes back
    // with EFBIG instead of killing the process.
    // SAFETY: plain libc calls on this process's own resource limits and
    // signal disposition.
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut lim), 0);
        lim.rlim_cur = 1024 * 1024;
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &lim), 0);
    }
    assert!(guc_tables::vars::wal_init_zero.read());
    let seg = transam_xlog::wal_segment_size();
    assert!(seg as u64 > 1024 * 1024);

    let before = pgstat::io::pgstat_pending_io();
    let err = transam_xlog::write::XLogFileInit(1, 1).unwrap_err();
    assert!(
        err.message().starts_with("could not write to file "),
        "{err}"
    );
    let after = pgstat::io::pgstat_pending_io();

    let (o, c, p) = (
        IOObject::Wal as usize,
        IOContext::IOCONTEXT_INIT as usize,
        IOOp::Write as usize,
    );
    // Pre-fix: one op of wal_segment_size bytes was recorded for the failed
    // zero-fill.
    assert_eq!(after.counts[o][c][p] - before.counts[o][c][p], 0);
    assert_eq!(after.bytes[o][c][p] - before.bytes[o][c][p], 0);
    // The temp segment was cleaned up on the error path.
    let tmppath = format!("pg_wal/xlogtemp.{}", init_small::globals::process_id());
    assert!(!dir.join(tmppath).exists());

    let _ = std::fs::remove_dir_all(&dir);
}
