use super::*;
use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Mutex, Once};
use types_error::PgError;

// DST P1: writeTimeLineHistoryFile now rides fd's durable path, whose
// pg_fsync consults the wal_sync_method GUC slot (fd sync.rs). Install the
// same test accessor fd's own setup uses (fd/src/tests.rs).
static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);

thread_local! {
    static CAPTURED: RefCell<Vec<PgError>> = const { RefCell::new(Vec::new()) };
}

fn capture_hook(error: &PgError, _output_to_server: &mut bool) {
    CAPTURED.with(|c| c.borrow_mut().push(error.clone()));
}

static WRITE_LOCK: Mutex<()> = Mutex::new(());

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("timeline_test_{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("pg_wal")).unwrap();
        std::env::set_current_dir(&tmp).unwrap();

        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        init_small_seams::my_proc_pid::set(|| 4242);
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        guc_tables::init_seams();
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
            set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
        });
    });
}

fn expect_fatal(f: impl FnOnce()) -> PgError {
    CAPTURED.with(|c| c.borrow_mut().clear());
    let prev = elog::set_emit_log_hook(Some(capture_hook));
    let result = catch_unwind(AssertUnwindSafe(f));
    elog::set_emit_log_hook(prev);
    assert!(result.is_err(), "expected FATAL proc_exit");
    let err = CAPTURED
        .with(|c| c.borrow().last().cloned())
        .expect("FATAL report was emitted");
    assert_eq!(err.level(), FATAL);
    err
}

fn entry(tli: TimeLineID, begin: XLogRecPtr, end: XLogRecPtr) -> TimeLineHistoryEntry {
    TimeLineHistoryEntry { tli, begin, end }
}

#[test]
fn history_file_name_and_path() {
    assert_eq!(TLHistoryFileName(5), "00000005.history");
    assert_eq!(TLHistoryFilePath(0xAB), "pg_wal/000000AB.history");
}

// Expected bytes hand-derived from C's snprintf(buffer, sizeof(buffer),
// "%s%u\t%X/%X\t%s\n", ...) in writeTimeLineHistory (timeline.c:401-406): no
// parent file -> no leading newline; with a parent file the parent's bytes are
// copied verbatim and a '\n' precedes the appended line.
#[test]
fn write_and_read_round_trip() {
    setup();
    let _g = WRITE_LOCK.lock().unwrap();

    let _ = std::fs::remove_file("pg_wal/00000002.history");
    let _ = std::fs::remove_file("pg_wal/00000003.history");

    writeTimeLineHistory(2, 1, 0x0150_0028, "before 2", false, false).unwrap();
    assert_eq!(
        std::fs::read("pg_wal/00000002.history").unwrap(),
        b"1\t0/1500028\tbefore 2\n"
    );

    writeTimeLineHistory(3, 2, 0x1_0200_0000, "before 3", false, false).unwrap();
    assert_eq!(
        std::fs::read("pg_wal/00000003.history").unwrap(),
        b"1\t0/1500028\tbefore 2\n\n2\t1/2000000\tbefore 3\n"
    );

    let cx = mcx::MemoryContext::new("timeline test");
    let tles = readTimeLineHistory(cx.mcx(), 3, false).unwrap();
    assert_eq!(
        tles.as_slice(),
        &[
            entry(3, 0x1_0200_0000, InvalidXLogRecPtr),
            entry(2, 0x0150_0028, 0x1_0200_0000),
            entry(1, InvalidXLogRecPtr, 0x0150_0028),
        ]
    );
}

#[test]
fn read_dummy_histories() {
    setup();
    let cx = mcx::MemoryContext::new("timeline test");
    // Timeline 1 never reads a file.
    let tles = readTimeLineHistory(cx.mcx(), 1, false).unwrap();
    assert_eq!(tles.as_slice(), &[entry(1, 0, 0)]);
    // Absent file: assume no parents.
    let tles = readTimeLineHistory(cx.mcx(), 0x30, false).unwrap();
    assert_eq!(tles.as_slice(), &[entry(0x30, 0, 0)]);
}

#[test]
fn read_skips_comments_and_whitespace() {
    setup();
    std::fs::write(
        "pg_wal/0000000A.history",
        b"# a comment\n\n   \t\n  1\t0/10\tfirst\n2\t0x0/0X20\tsecond extra words\n",
    )
    .unwrap();
    let cx = mcx::MemoryContext::new("timeline test");
    let tles = readTimeLineHistory(cx.mcx(), 0xA, false).unwrap();
    assert_eq!(
        tles.as_slice(),
        &[entry(0xA, 0x20, 0), entry(2, 0x10, 0x20), entry(1, 0, 0x10)]
    );
}

#[test]
fn read_fatal_non_numeric_tli() {
    setup();
    std::fs::write("pg_wal/0000000B.history", b"bogus line\n").unwrap();
    let cx = mcx::MemoryContext::new("timeline test");
    let err = expect_fatal(|| {
        let _ = readTimeLineHistory(cx.mcx(), 0xB, false);
    });
    assert_eq!(err.message(), "syntax error in history file: bogus line\n");
    assert_eq!(err.hint(), Some("Expected a numeric timeline ID."));
}

#[test]
fn read_fatal_missing_switchpoint() {
    setup();
    std::fs::write("pg_wal/0000000C.history", b"7\n").unwrap();
    let cx = mcx::MemoryContext::new("timeline test");
    let err = expect_fatal(|| {
        let _ = readTimeLineHistory(cx.mcx(), 0xC, false);
    });
    assert_eq!(err.message(), "syntax error in history file: 7\n");
    assert_eq!(
        err.hint(),
        Some("Expected a write-ahead log switchpoint location.")
    );
}

#[test]
fn read_fatal_decreasing_tlis() {
    setup();
    std::fs::write("pg_wal/0000000D.history", b"2\t0/10\tx\n1\t0/20\ty\n").unwrap();
    let cx = mcx::MemoryContext::new("timeline test");
    let err = expect_fatal(|| {
        let _ = readTimeLineHistory(cx.mcx(), 0xD, false);
    });
    assert_eq!(err.message(), "invalid data in history file: 1\t0/20\ty\n");
    assert_eq!(err.hint(), Some("Timeline IDs must be in increasing sequence."));
}

#[test]
fn read_fatal_target_not_greater_than_last() {
    setup();
    std::fs::write("pg_wal/0000000E.history", b"14\t0/10\tx\n").unwrap();
    let cx = mcx::MemoryContext::new("timeline test");
    let err = expect_fatal(|| {
        let _ = readTimeLineHistory(cx.mcx(), 0xE, false);
    });
    assert_eq!(
        err.message(),
        "invalid data in history file \"pg_wal/0000000E.history\""
    );
    assert_eq!(
        err.hint(),
        Some("Timeline IDs must be less than child timeline's ID.")
    );
}

#[test]
fn exists_and_find_newest() {
    setup();
    std::fs::write("pg_wal/00000021.history", b"1\t0/10\tx\n").unwrap();
    std::fs::write("pg_wal/00000022.history", b"1\t0/10\tx\n").unwrap();
    assert!(!existsTimeLineHistory(1, false).unwrap());
    assert!(existsTimeLineHistory(0x21, false).unwrap());
    assert!(!existsTimeLineHistory(0x23, false).unwrap());
    assert_eq!(findNewestTimeLine(0x20, false).unwrap(), 0x22);
    assert_eq!(findNewestTimeLine(0x22, false).unwrap(), 0x22);
    assert_eq!(findNewestTimeLine(0x23, false).unwrap(), 0x23);
}

#[test]
fn write_history_file_replaces() {
    setup();
    let _g = WRITE_LOCK.lock().unwrap();
    writeTimeLineHistoryFile(0x40, b"1\t0/AB\tz\n").unwrap();
    assert_eq!(std::fs::read("pg_wal/00000040.history").unwrap(), b"1\t0/AB\tz\n");
    writeTimeLineHistoryFile(0x40, b"1\t0/CD\tw\n").unwrap();
    assert_eq!(std::fs::read("pg_wal/00000040.history").unwrap(), b"1\t0/CD\tw\n");
}

#[test]
fn tli_lookups() {
    let history = [
        entry(3, 0x200, InvalidXLogRecPtr),
        entry(2, 0x100, 0x200),
        entry(1, InvalidXLogRecPtr, 0x100),
    ];

    assert!(tliInHistory(2, &history));
    assert!(!tliInHistory(9, &history));
    assert!(!tliInHistory(9, &[]));

    assert_eq!(tliOfPointInHistory(0, &history).unwrap(), 1);
    assert_eq!(tliOfPointInHistory(0xFF, &history).unwrap(), 1);
    assert_eq!(tliOfPointInHistory(0x100, &history).unwrap(), 2);
    assert_eq!(tliOfPointInHistory(0x1FF, &history).unwrap(), 2);
    assert_eq!(tliOfPointInHistory(0x200, &history).unwrap(), 3);
    assert_eq!(tliOfPointInHistory(u64::MAX, &history).unwrap(), 3);

    let gap = [entry(2, 0x200, 0x300)];
    let err = tliOfPointInHistory(0x100, &gap).unwrap_err();
    assert_eq!(err.message(), "timeline history was not contiguous");

    assert_eq!(tliSwitchPoint(2, &history).unwrap(), (0x200, 3));
    assert_eq!(tliSwitchPoint(1, &history).unwrap(), (0x100, 2));
    assert_eq!(tliSwitchPoint(3, &history).unwrap(), (InvalidXLogRecPtr, 0));
    let err = tliSwitchPoint(9, &history).unwrap_err();
    assert_eq!(
        err.message(),
        "requested timeline 9 is not in this server's history"
    );
}

// sscanf semantics: the '/' literal must follow the hi digits immediately;
// whitespace runs match the '\t'; overflow wraps.
#[test]
fn sscanf_edge_cases() {
    assert_eq!(sscanf_history_line(b"12  34/AB rest"), (3, 12, 0x34, 0xAB));
    assert_eq!(sscanf_history_line(b"5\t10 /20\tr"), (2, 5, 0x10, 0));
    assert_eq!(sscanf_history_line(b"5\t/20"), (1, 5, 0, 0));
    assert_eq!(sscanf_history_line(b"-1\t0/0\tr"), (3, u32::MAX, 0, 0));
    assert_eq!(sscanf_history_line(b"x"), (0, 0, 0, 0));
    assert_eq!(sscanf_history_line(b"4294967297\t0/0\tr").1, 1);
}

// fgets splits a physical line into MAXPGPATH-1 chunks and stops at NUL.
#[test]
fn fgets_line_splitting() {
    let mut long = vec![b'a'; 1500];
    long.push(b'\n');
    let lines: Vec<&[u8]> = fgets_lines(&long).collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].len(), 1023);
    assert_eq!(lines[1].len(), 478);

    let with_nul = b"ab\0cd\nef";
    let lines: Vec<&[u8]> = fgets_lines(with_nul).collect();
    assert_eq!(lines, vec![b"ab".as_slice(), b"ef".as_slice()]);
}
