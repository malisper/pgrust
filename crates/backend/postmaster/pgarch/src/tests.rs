use std::sync::{Mutex, Once, OnceLock};

use crate::*;

fn shmem_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        PgArchShmemInit();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
    });
}

// cwd is process-global: every fs-touching test runs under this lock in its
// own fresh directory.
fn with_wal_cwd(f: impl FnOnce()) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|e| e.into_inner());
    shmem_once();
    let dir = std::env::temp_dir().join(format!(
        "pgarch-test-{}-{:?}",
        std::process::id(),
        std::time::Instant::now()
    ));
    std::fs::create_dir_all(dir.join("pg_wal/archive_status")).unwrap();
    std::env::set_current_dir(&dir).unwrap();
    f();
    let _ = std::env::set_current_dir(std::env::temp_dir());
    let _ = std::fs::remove_dir_all(&dir);
}

fn touch(path: &str) {
    std::fs::write(path, b"").unwrap();
}

fn ready(name: &str) {
    touch(&format!("pg_wal/archive_status/{name}.ready"));
}

fn seg(name: &str) {
    touch(&format!("pg_wal/{name}"));
}

#[test]
fn ready_file_comparator_ordering() {
    use std::cmp::Ordering::*;
    // History files always outrank segments; otherwise oldest (strcmp) first.
    assert_eq!(ready_file_cmp("00000002.history", "000000010000000000000001"), Less);
    assert_eq!(ready_file_cmp("000000010000000000000001", "00000002.history"), Greater);
    assert_eq!(ready_file_cmp("00000002.history", "00000003.history"), Less);
    assert_eq!(
        ready_file_cmp("000000010000000000000001", "000000010000000000000002"),
        Less
    );
    assert_eq!(ready_file_cmp("000000010000000000000001", "000000010000000000000001"), Equal);
}

#[test]
fn ready_xlog_ordering_and_batch_cache() {
    with_wal_cwd(|| {
        let mut af = ArchFilesState::new();

        // Empty directory: nothing to archive.
        assert_eq!(pgarch_readyXlog(&mut af).unwrap(), None);

        // 70 ready segments (> NUM_FILES_PER_DIRECTORY_SCAN) + one history
        // file created out of order.
        let mut names: Vec<String> =
            (1..=70).map(|i| format!("0000000100000000000000{i:02X}")).collect();
        for n in &names {
            ready(n);
            seg(n);
        }
        ready("00000002.history");
        seg("00000002.history");
        names.push("00000002.history".to_string());

        // History first, then oldest segments in order; exactly 64 come from
        // the first scan's batch. Each consumed file is marked done, as the
        // copy loop would.
        assert_eq!(pgarch_readyXlog(&mut af).unwrap().as_deref(), Some("00000002.history"));
        pgarch_archiveDone("00000002.history").unwrap();
        assert_eq!(af.files_size, 63);
        for i in 1..=63 {
            let expect = format!("0000000100000000000000{i:02X}");
            assert_eq!(pgarch_readyXlog(&mut af).unwrap().as_deref(), Some(expect.as_str()));
            pgarch_archiveDone(&expect).unwrap();
        }
        assert_eq!(af.files_size, 0);

        // Batch exhausted: the next call rescans and finds the remainder.
        assert_eq!(
            pgarch_readyXlog(&mut af).unwrap().as_deref(),
            Some("000000010000000000000040")
        );
        pgarch_archiveDone("000000010000000000000040").unwrap();
        assert_eq!(af.files_size, 6);

        // A .ready consumed behind our back is skipped by the stat recheck.
        std::fs::remove_file("pg_wal/archive_status/000000010000000000000041.ready").unwrap();
        assert_eq!(
            pgarch_readyXlog(&mut af).unwrap().as_deref(),
            Some("000000010000000000000042")
        );
        pgarch_archiveDone("000000010000000000000042").unwrap();

        // force_dir_scan discards the cached batch: a newly-ready older file
        // preempts remaining batched entries.
        ready("000000010000000000000001");
        PgArchForceDirScan();
        assert_eq!(
            pgarch_readyXlog(&mut af).unwrap().as_deref(),
            Some("000000010000000000000001")
        );
    });
}

#[test]
fn ready_xlog_name_filtering() {
    with_wal_cwd(|| {
        let mut af = ArchFilesState::new();
        // Wrong suffix, too short, invalid chars: all ignored.
        touch("pg_wal/archive_status/000000010000000000000001.done");
        touch("pg_wal/archive_status/0001.ready");
        touch("pg_wal/archive_status/00000001000000000000000z.ready");
        assert_eq!(pgarch_readyXlog(&mut af).unwrap(), None);

        ready("000000010000000000000002");
        assert_eq!(
            pgarch_readyXlog(&mut af).unwrap().as_deref(),
            Some("000000010000000000000002")
        );
    });
}

#[test]
fn archive_done_renames_ready() {
    with_wal_cwd(|| {
        ready("000000010000000000000005");
        pgarch_archiveDone("000000010000000000000005").unwrap();
        assert!(!std::path::Path::new("pg_wal/archive_status/000000010000000000000005.ready")
            .exists());
        assert!(std::path::Path::new("pg_wal/archive_status/000000010000000000000005.done")
            .exists());
    });
}

#[test]
fn can_restart_throttles() {
    assert!(PgArchCanRestart());
    assert!(!PgArchCanRestart());
}
