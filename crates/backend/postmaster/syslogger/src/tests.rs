use super::*;
use std::sync::MutexGuard;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

fn chunk(pid: i32, flags: u8, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() <= PIPE_MAX_PAYLOAD);
    let mut out = vec![0u8, 0u8];
    out.extend_from_slice(&(payload.len() as u16).to_ne_bytes());
    out.extend_from_slice(&pid.to_ne_bytes());
    out.push(flags);
    out.extend_from_slice(payload);
    out
}

fn install_logfile(path: &std::path::Path) {
    let c = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    let m = std::ffi::CString::new("w+").unwrap();
    let fh = unsafe { libc::fopen(c.as_ptr(), m.as_ptr()) };
    assert!(!fh.is_null());
    let old = SYSLOG_FILE.swap(fh, Relaxed);
    if !old.is_null() {
        unsafe { libc::fclose(old) };
    }
}

fn drain_logfile(path: &std::path::Path) -> Vec<u8> {
    unsafe { libc::fflush(SYSLOG_FILE.load(Relaxed)) };
    std::fs::read(path).unwrap()
}

fn feed(st: &mut SysLoggerState, wire: &[u8]) {
    let mut buf = wire.to_vec();
    buf.resize(wire.len().max(READ_BUF_SIZE), 0);
    let mut n = wire.len();
    process_pipe_input(st, &mut buf, &mut n);
    assert_eq!(n, 0, "no leftover expected");
}

#[test]
fn header_layout_matches_elog_writer() {
    assert_eq!(PIPE_HEADER_SIZE, 9);
    let c = chunk(0x0102_0304, PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_STDERR, b"xy");
    assert_eq!(c.len(), PIPE_HEADER_SIZE + 2);
    assert_eq!(&c[0..2], &[0, 0]);
    assert_eq!(u16::from_ne_bytes([c[2], c[3]]), 2);
    assert_eq!(i32::from_ne_bytes([c[4], c[5], c[6], c[7]]), 0x0102_0304);
    assert_eq!(c[8], 0x11);
}

#[test]
fn single_chunk_message_is_written() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("single.log");
    install_logfile(&path);

    let mut st = SysLoggerState::new();
    feed(&mut st, &chunk(42, PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_STDERR, b"hello\n"));
    assert_eq!(drain_logfile(&path), b"hello\n");
}

#[test]
fn multi_chunk_message_reassembles_and_interleaves() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("multi.log");
    install_logfile(&path);

    let mut st = SysLoggerState::new();
    let mut wire = Vec::new();
    wire.extend_from_slice(&chunk(7, PIPE_PROTO_DEST_STDERR, b"AAAA"));
    wire.extend_from_slice(&chunk(8, PIPE_PROTO_DEST_STDERR, b"BBBB"));
    wire.extend_from_slice(&chunk(7, PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_STDERR, b"aaaa\n"));
    wire.extend_from_slice(&chunk(8, PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_STDERR, b"bbbb\n"));
    feed(&mut st, &wire);
    assert_eq!(drain_logfile(&path), b"AAAAaaaa\nBBBBbbbb\n");

    // The completed slots are reusable (pid zeroed, capacity retained).
    let list = &st.buffer_lists[7 % NBUFFER_LISTS];
    assert!(list.iter().all(|b| b.pid == 0));
}

#[test]
fn oversized_message_spans_max_payload_chunks() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("big.log");
    install_logfile(&path);

    let msg = vec![b'x'; PIPE_MAX_PAYLOAD + 100];
    let mut st = SysLoggerState::new();
    let mut wire = Vec::new();
    wire.extend_from_slice(&chunk(9, PIPE_PROTO_DEST_STDERR, &msg[..PIPE_MAX_PAYLOAD]));
    wire.extend_from_slice(&chunk(
        9,
        PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_STDERR,
        &msg[PIPE_MAX_PAYLOAD..],
    ));
    feed(&mut st, &wire);
    assert_eq!(drain_logfile(&path), msg);
}

#[test]
fn partial_chunk_is_left_justified_and_completed() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("partial.log");
    install_logfile(&path);

    let full = chunk(5, PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_STDERR, b"payload\n");
    let split = PIPE_HEADER_SIZE + 3;

    let mut st = SysLoggerState::new();
    let mut buf = vec![0u8; READ_BUF_SIZE];
    buf[..split].copy_from_slice(&full[..split]);
    let mut n = split;
    process_pipe_input(&mut st, &mut buf, &mut n);
    assert_eq!(n, split, "incomplete chunk retained");
    assert_eq!(&buf[..split], &full[..split], "left-justified");

    buf[n..full.len()].copy_from_slice(&full[split..]);
    let mut n2 = full.len();
    process_pipe_input(&mut st, &mut buf, &mut n2);
    assert_eq!(n2, 0);
    assert_eq!(drain_logfile(&path), b"payload\n");
}

#[test]
fn non_protocol_data_passes_through_and_flush_dumps_partials() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("raw.log");
    install_logfile(&path);

    let mut st = SysLoggerState::new();
    feed(&mut st, b"third-party stderr line\n");
    assert_eq!(drain_logfile(&path), b"third-party stderr line\n");

    let mut buf = vec![0u8; READ_BUF_SIZE];
    let c = chunk(6, PIPE_PROTO_DEST_STDERR, b"unfinished");
    buf[..c.len()].copy_from_slice(&c);
    let mut n = c.len();
    process_pipe_input(&mut st, &mut buf, &mut n);
    assert_eq!(n, 0);

    flush_pipe_input(&mut st, &buf, &mut n);
    assert_eq!(drain_logfile(&path), b"third-party stderr line\nunfinished");
}

#[test]
fn csv_dest_without_csv_file_falls_back_to_syslog_file() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("csvfallback.log");
    install_logfile(&path);
    assert!(CSVLOG_FILE.load(Relaxed).is_null());

    let mut st = SysLoggerState::new();
    feed(&mut st, &chunk(3, PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_CSVLOG, b"c,s,v\n"));
    assert_eq!(drain_logfile(&path), b"c,s,v\n");
}

#[test]
fn update_metainfo_datafile_writes_current_logfiles() {
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-meta-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let saved_cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(&dir).unwrap();

    let mut st = SysLoggerState::new();
    st.last_sys_file_name = Some("log/postgresql-x.log".to_string());
    update_metainfo_datafile(&st).unwrap();

    let content = std::fs::read_to_string(LOG_METAINFO_DATAFILE).unwrap();
    assert_eq!(content, "stderr log/postgresql-x.log\n");

    std::env::set_current_dir(saved_cwd).unwrap();
}

/// syslogger.c:1228-1229: logfile_open opens under
/// `umask(~(Log_file_mode | S_IWUSR))`, so a NEW file gets
/// `0666 & (Log_file_mode | S_IWUSR)` — fopen's 0666 base never yields
/// execute bits (config.sgml) — and an EXISTING file keeps its mode
/// (umask plays no part in "a"/"w" on an existing file).
#[cfg(unix)]
#[test]
fn logfile_open_mode_matches_c_fopen_under_umask() {
    use std::os::unix::fs::PermissionsExt;
    let _g = lock();
    let dir = std::env::temp_dir().join(format!("sysltest-mode-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("mode.log");
    let _ = std::fs::remove_file(&path);
    let mode_of = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    let open = |mode: &str| {
        let fh = logfile_open(path.to_str().unwrap(), mode, true).unwrap();
        assert!(!fh.is_null(), "logfile_open({mode}) failed");
        unsafe { libc::fclose(fh) };
    };
    let saved = LOG_FILE_MODE.swap(0o755, Relaxed);

    // fresh file, log_file_mode = 0755: C creates 0644 (no execute bits)
    open("a");
    assert_eq!(mode_of(&path), 0o644, "new file: 0666 & (0755 | S_IWUSR)");

    // fresh file, log_file_mode = 0400: S_IWUSR is always kept
    std::fs::remove_file(&path).unwrap();
    LOG_FILE_MODE.store(0o400, Relaxed);
    open("a");
    assert_eq!(mode_of(&path), 0o600, "new file: 0666 & (0400 | S_IWUSR)");

    // pre-existing file: neither "a" nor "w" changes its mode
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    LOG_FILE_MODE.store(0o600, Relaxed);
    open("a");
    assert_eq!(mode_of(&path), 0o640, "existing file, append: mode preserved");
    open("w");
    assert_eq!(mode_of(&path), 0o640, "existing file, truncate: mode preserved");

    LOG_FILE_MODE.store(saved, Relaxed);
    let _ = std::fs::remove_file(&path);
}

/// syslogger.c:526 (read from the logger pipe) and :625 (pipe(2) for the
/// syslog pipe) report with errcode_for_socket_access, not
/// errcode_for_file_access: EMFILE/ENFILE/ENOMEM/EIO are XX000, the
/// connection-class errnos are 08006.
#[test]
fn logger_pipe_errors_use_socket_access_sqlstates() {
    use types_error::{ERRCODE_CONNECTION_FAILURE, ERRCODE_INTERNAL_ERROR};
    let sqlstate = |errnum: i32, message: &str| {
        logger_pipe_report(FATAL, errnum, message).into_error().sqlstate()
    };
    let create = "could not create pipe for syslog: %m";
    let read = "could not read from logger pipe: %m";
    assert_eq!(sqlstate(libc::EMFILE, create), ERRCODE_INTERNAL_ERROR);
    assert_eq!(sqlstate(libc::ENFILE, create), ERRCODE_INTERNAL_ERROR);
    assert_eq!(sqlstate(libc::ENOMEM, create), ERRCODE_INTERNAL_ERROR);
    assert_eq!(sqlstate(libc::EIO, read), ERRCODE_INTERNAL_ERROR);
    assert_eq!(sqlstate(libc::ECONNRESET, read), ERRCODE_CONNECTION_FAILURE);
    assert_eq!(sqlstate(libc::EPIPE, read), ERRCODE_CONNECTION_FAILURE);
    let err = logger_pipe_report(FATAL, libc::EMFILE, create).into_error();
    assert_eq!(err.message, format!("could not create pipe for syslog: {}", elog::errno::strerror(libc::EMFILE)));
}

/// syslogger.c:1424: `pg_strftime(filename + len, MAXPGPATH - len, ...)`
/// formats up to MAXPGPATH - len - 1 bytes (the last slot is the NUL); one
/// byte more overflows and leaves "<Log_directory>/".
#[test]
fn logfile_getname_formats_up_to_maxpgpath_minus_len_minus_one() {
    let _g = lock();
    let saved_dir = LOG_DIRECTORY.lock().unwrap().replace("log".to_string());
    let saved_tz = pgtz::log_timezone();
    pgtz::set_log_timezone(Some(pgtz::pg_tzset(b"GMT").unwrap().expect("GMT always parses")));

    let len = "log/".len();
    let fits = "x".repeat(MAXPGPATH - len - 1);
    let saved_name = LOG_FILENAME.lock().unwrap().replace(fits.clone());
    let name = logfile_getname(0, None);
    assert_eq!(name.len(), MAXPGPATH - 1);
    assert_eq!(name, format!("log/{fits}"));

    let over = "x".repeat(MAXPGPATH - len);
    *LOG_FILENAME.lock().unwrap() = Some(over);
    assert_eq!(logfile_getname(0, None), "log/");

    // the default pattern still expands normally
    *LOG_FILENAME.lock().unwrap() = Some("postgresql-%Y-%m-%d_%H%M%S.log".to_string());
    assert_eq!(logfile_getname(0, None), "log/postgresql-1970-01-01_000000.log");

    *LOG_FILENAME.lock().unwrap() = saved_name;
    *LOG_DIRECTORY.lock().unwrap() = saved_dir;
    pgtz::set_log_timezone(saved_tz);
}
