use super::*;

const WAL_SEG_SIZE: i32 = 16 * 1024 * 1024;

// log_timezone and the pg_tzset cache are per-thread; each test sets its own.
fn setup() {
    pgtz::set_log_timezone(Some(pgtz::pg_tzset(b"GMT").unwrap()));
}

// 2001-09-09 01:46:40 GMT
const START_TIME: pg_time_t = 1_000_000_000;
// 2001-09-09 04:33:20 GMT
const STOP_TIME: pg_time_t = 1_000_010_000;

fn state() -> BackupState {
    let mut st = BackupState::default();
    st.set_name(b"test label");
    st.startpoint = 0x2_A000_0028;
    st.starttli = 1;
    st.checkpointloc = 0x2_A000_0060;
    st.starttime = START_TIME;
    st.started_in_recovery = false;
    st.stoppoint = 0x2_A000_0128;
    st.stoptli = 1;
    st.stoptime = STOP_TIME;
    st
}

// Expected bytes hand-derived from C build_backup_content (xlogbackup.c:28-94,
// 18.3): "%X/%X" LSNs, "%08X%08X%08X" file names, strftime under GMT.
#[test]
fn backup_label_content() {
    setup();
    let cx = mcx::MemoryContext::new("xlogbackup test");
    let content = build_backup_content(cx.mcx(), &state(), false, WAL_SEG_SIZE).unwrap();
    let expected = b"START WAL LOCATION: 2/A0000028 (file 0000000100000002000000A0)\n\
CHECKPOINT LOCATION: 2/A0000060\n\
BACKUP METHOD: streamed\n\
BACKUP FROM: primary\n\
START TIME: 2001-09-09 01:46:40 GMT\n\
LABEL: test label\n\
START TIMELINE: 1\n";
    assert_eq!(content.as_slice(), expected.as_slice());
}

#[test]
fn backup_history_file_content() {
    setup();
    let cx = mcx::MemoryContext::new("xlogbackup test");
    let content = build_backup_content(cx.mcx(), &state(), true, WAL_SEG_SIZE).unwrap();
    let expected = b"START WAL LOCATION: 2/A0000028 (file 0000000100000002000000A0)\n\
STOP WAL LOCATION: 2/A0000128 (file 0000000100000002000000A0)\n\
CHECKPOINT LOCATION: 2/A0000060\n\
BACKUP METHOD: streamed\n\
BACKUP FROM: primary\n\
START TIME: 2001-09-09 01:46:40 GMT\n\
LABEL: test label\n\
START TIMELINE: 1\n\
STOP TIME: 2001-09-09 04:33:20 GMT\n\
STOP TIMELINE: 1\n";
    assert_eq!(content.as_slice(), expected.as_slice());
}

#[test]
fn incremental_and_standby_lines() {
    setup();
    let cx = mcx::MemoryContext::new("xlogbackup test");
    let mut st = state();
    st.started_in_recovery = true;
    st.istartpoint = 0x1_0000_0AB0;
    st.istarttli = 1;
    let content = build_backup_content(cx.mcx(), &st, false, WAL_SEG_SIZE).unwrap();
    let text = std::str::from_utf8(content.as_slice()).unwrap();
    assert!(text.contains("BACKUP FROM: standby\n"));
    assert!(text.ends_with("INCREMENTAL FROM LSN: 1/AB0\nINCREMENTAL FROM TLI: 1\n"));
}

// LABEL bytes are copied verbatim (server encoding, never UTF-8 validated).
#[test]
fn label_bytes_verbatim() {
    setup();
    let cx = mcx::MemoryContext::new("xlogbackup test");
    let mut st = state();
    st.set_name(b"caf\xE9 \xFF label");
    let content = build_backup_content(cx.mcx(), &st, false, WAL_SEG_SIZE).unwrap();
    let needle = b"LABEL: caf\xE9 \xFF label\n";
    assert!(content
        .as_slice()
        .windows(needle.len())
        .any(|w| w == needle.as_slice()));
}
