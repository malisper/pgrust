use super::*;

#[test]
fn sizes() {
    for (input, expect) in [
        ("1", 1),
        ("1 kB", 1024),
        ("  +1.5  MB ", 1572864),
        ("-1 GB", -1073741824),
        ("1TB", 1099511627776),
        ("1 PB", 1125899906842624),
        ("1e3 kB", 1024000),
        ("1B", 1),
        ("1 bytes", 1),
        (".5 kb", 512),
    ] {
        assert_eq!(pg_size_bytes(input).unwrap(), expect, "{input}");
    }
}

#[test]
fn tablespace_dir_paths() {
    assert_eq!(builtins::tablespace_dir_path(1663), "base");
    assert_eq!(builtins::tablespace_dir_path(1664), "global");
    assert_eq!(
        builtins::tablespace_dir_path(16385),
        format!("pg_tblspc/16385/{}", types_storage::TABLESPACE_VERSION_DIRECTORY)
    );
}

#[test]
fn errors() {
    assert!(pg_size_bytes("").is_err());
    assert!(pg_size_bytes("kB").is_err());
    let e = pg_size_bytes("1 xB").unwrap_err();
    assert!(e.to_string().contains("invalid size"), "{e}");
    assert!(pg_size_bytes("1 EB").is_err());
}

#[test]
fn missing_rel_bad_fork_is_null_pin() {
    let e = builtins::forkname_to_number("nope").unwrap_err();
    assert_eq!(e.message(), "invalid fork name");
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(!e.message().contains("nope"), "{}", e.message());
}

fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("dbsize-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

// audit-18.6 fp-adt-dbsize#4: dbsize.c:96 stats the dirent's own name
// bytes; a non-UTF-8 filename (Linux) must be counted, not lossily renamed
// into an ENOENT skip. Filesystems refusing such names skip that leg.
#[test]
fn dir_size_keeps_filename_bytes() {
    use std::os::unix::ffi::OsStrExt;
    let d = scratch_dir("bytes");
    std::fs::write(d.join("a"), b"abc").unwrap();
    let mut expect = 3;
    if std::fs::write(d.join(std::ffi::OsStr::from_bytes(b"\xff")), b"12345").is_ok() {
        expect += 5;
    }
    assert_eq!(builtins::db_dir_size(&d).unwrap(), expect);
    assert_eq!(builtins::db_dir_size(&d.join("absent")).unwrap(), 0);
    let _ = std::fs::remove_dir_all(&d);
}

// audit-18.6 fp-adt-dbsize#5: dbsize.c:264 reports a stat failure with
// errcode_for_file_access() and strerror (%m) -- ELOOP is XX000 with libc's
// text, never Rust's "(os error N)" suffix. db_dir_size already did this.
#[test]
fn stat_failures_are_file_access_errors() {
    let d = scratch_dir("loop");
    std::os::unix::fs::symlink("loop", d.join("loop")).unwrap();
    for r in [builtins::db_dir_size(&d), builtins::tablespace_dir_size(&d)] {
        let err = r.unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        let msg = err.message();
        assert!(msg.starts_with(&format!("could not stat file \"{}/loop\": ", d.display())), "{msg}");
        assert!(!msg.contains("os error"), "{msg}");
    }
    assert_eq!(builtins::tablespace_dir_size(&d.join("absent")).unwrap(), -1);
    let _ = std::fs::remove_dir_all(&d);
}
