use super::*;

#[test]
fn canonicalize_pins_c_behavior() {
    for (input, want) in [
        ("", ""),
        ("/", "/"),
        ("/a//b", "/a/b"),
        ("/a///b/", "/a/b"),
        ("/a/./b", "/a/b"),
        ("/a/b/..", "/a"),
        ("/a/b/../..", "/"),
        ("/a/b/../../..", "/"),
        ("/..", "/"),
        ("/../..", "/"),
        (".", "."),
        ("./", "."),
        ("..", ".."),
        ("../..", "../.."),
        ("a/..", "."),
        ("../dir/..", ".."),
        ("../dir/../x", "../x"),
        ("a/b/../c", "a/c"),
        ("a/./b", "a/b"),
        ("foo/bar/", "foo/bar"),
        ("/a/../b", "/b"),
        ("../a/b/../../c", "../c"),
    ] {
        assert_eq!(canonicalize_path(input), want, "input {input:?}");
    }
}

#[test]
fn join_omits_slash_for_empty_components() {
    assert_eq!(join_path_components("a", "b"), "a/b");
    assert_eq!(join_path_components("", "b"), "b");
    assert_eq!(join_path_components("a", ""), "a");
    assert_eq!(join_path_components("/", "b"), "//b");
}

#[test]
fn get_parent_directory_matches_trim_directory() {
    assert_eq!(get_parent_directory("/a/b/c"), "/a/b");
    assert_eq!(get_parent_directory("/a"), "/");
    assert_eq!(get_parent_directory("a"), "");
    assert_eq!(get_parent_directory("/a/b//"), "/a");
    assert_eq!(get_parent_directory("/"), "/");
}

#[test]
fn make_relative_path_relocates_per_c_comment() {
    // The worked example in path.c's make_relative_path header.
    assert_eq!(
        make_relative_path(
            "/usr/local/share/postgresql",
            "/usr/local/bin",
            "/opt/pgsql/bin/postgres"
        ),
        "/opt/pgsql/share/postgresql"
    );
    assert_eq!(
        make_relative_path(
            "/usr/local/pgsql/share",
            "/usr/local/pgsql/bin",
            "/home/me/inst/bin/postgres"
        ),
        "/home/me/inst/share"
    );
    // No tail match: compiled-in target wins.
    assert_eq!(
        make_relative_path(
            "/usr/local/pgsql/share",
            "/usr/local/pgsql/bin",
            "/somewhere/else/postgres"
        ),
        "/usr/local/pgsql/share"
    );
    // No common prefix.
    assert_eq!(
        make_relative_path("/opt/share", "/usr/bin", "/x/bin/postgres"),
        "/opt/share"
    );
    // '/usr/lib' vs '/usr/libexec': prefix must end on a separator.
    assert_eq!(
        make_relative_path("/usr/lib", "/usr/libexec", "/inst/libexec/postgres"),
        "/inst/lib"
    );
}

#[test]
fn share_path_is_absolute() {
    let share = get_share_path("/nonexistent/bin/postgres");
    assert!(is_absolute_path(&share));
}

#[test]
fn validate_and_find_my_exec() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("pg_path_test_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let exe = dir.join("fakepg");
    std::fs::write(&exe, b"#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    let exe_str = exe.to_str().unwrap();

    assert_eq!(validate_exec(exe_str), 0);
    // exec.c:124: a non-regular file sets errno itself (EISDIR for a
    // directory) so the caller's %m names the real reason.
    test_set_errno(0);
    assert_eq!(validate_exec(dir.to_str().unwrap()), -1);
    assert_eq!(test_errno(), libc::EISDIR);
    assert_eq!(validate_exec("/no/such/file"), -1);
    assert_eq!(test_errno(), libc::ENOENT);

    let mut logged = Vec::new();
    let found = find_my_exec(exe_str, |_, m| logged.push(m)).unwrap();
    assert!(is_absolute_path(&found));
    assert!(found.ends_with("/fakepg"));
    assert!(logged.is_empty());

    // exec.c:173: separator present + validate_exec failure = one LOG line
    // 'invalid binary "%s": %m' (ERRCODE_WRONG_OBJECT_TYPE) before -1.
    let mut logged = Vec::new();
    find_my_exec("/no/such/file", |c, m| logged.push((c, m))).unwrap_err();
    assert_eq!(
        logged,
        vec![(
            ExecLogCode::WrongObjectType,
            "invalid binary \"/no/such/file\": No such file or directory".to_string()
        )]
    );
    let mut logged = Vec::new();
    find_my_exec(dir.to_str().unwrap(), |c, m| logged.push((c, m))).unwrap_err();
    assert_eq!(
        logged,
        vec![(
            ExecLogCode::WrongObjectType,
            format!("invalid binary \"{}\": Is a directory", dir.to_str().unwrap())
        )]
    );
    assert_eq!(ExecLogCode::WrongObjectType.sqlstate(), Some(*b"42809"));
    assert_eq!(ExecLogCode::UndefinedFile.sqlstate(), Some(*b"58P01"));
    assert_eq!(ExecLogCode::FileAccess.sqlstate(), None);

    // PATH-search leg, including the -2 log-and-keep-scanning arm.
    let dir2 = dir.join("unreadable");
    std::fs::create_dir_all(&dir2).unwrap();
    let bad = dir2.join("fakepg");
    std::fs::write(&bad, b"").unwrap();
    std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o111)).unwrap();

    let saved = std::env::var("PATH").ok();
    std::env::set_var(
        "PATH",
        format!("{}:{}", dir2.to_str().unwrap(), dir.to_str().unwrap()),
    );
    let mut logged = Vec::new();
    let found = find_my_exec("fakepg", |_, m| logged.push(m));
    // exec.c:224: PATH exhausted without a candidate = one LOG line
    // 'could not find a "%s" to execute' (ERRCODE_UNDEFINED_FILE).
    let mut miss_logged = Vec::new();
    let miss = find_my_exec("fakepg-absent", |c, m| miss_logged.push((c, m)));
    match saved {
        Some(p) => std::env::set_var("PATH", p),
        None => std::env::remove_var("PATH"),
    }
    miss.unwrap_err();
    assert_eq!(
        miss_logged,
        vec![(
            ExecLogCode::UndefinedFile,
            "could not find a \"fakepg-absent\" to execute".to_string()
        )]
    );
    let found = found.unwrap();
    assert!(found.ends_with("/fakepg"));
    // Skipped when running as root (access(R_OK) succeeds regardless, so the
    // unreadable candidate wins the PATH scan).
    if unsafe { libc::geteuid() } != 0 {
        assert!(!found.contains("unreadable"));
        assert_eq!(logged.len(), 1);
        assert!(logged[0].starts_with("could not read binary"));
    }

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn path_membership_predicates() {
    assert!(path_contains_parent_reference(".."));
    assert!(path_contains_parent_reference("../x"));
    assert!(!path_contains_parent_reference("a/../x"));
    assert!(!path_contains_parent_reference("..x"));
    assert!(path_is_relative_and_below_cwd("base"));
    assert!(!path_is_relative_and_below_cwd("/abs"));
    assert!(!path_is_relative_and_below_cwd("../up"));
    assert!(path_is_prefix_of_path("/data", "/data"));
    assert!(path_is_prefix_of_path("/data", "/data/base"));
    assert!(!path_is_prefix_of_path("/data", "/database"));
}

#[test]
fn separator_scans() {
    assert_eq!(first_dir_separator("a/b"), Some(1));
    assert_eq!(first_dir_separator("ab"), None);
    assert_eq!(last_dir_separator("a/b/c"), Some(3));
    assert_eq!(first_path_var_separator("a:b"), Some(1));
}

fn test_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn test_set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    // SAFETY: writing the calling thread's errno slot.
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    // SAFETY: writing the calling thread's errno slot.
    unsafe {
        *libc::__errno_location() = value;
    }
}

#[test]
fn normalize_exec_path_reports_realpath_failure() {
    // exec.c:251: realpath failure = one LOG line 'could not resolve path
    // "%s" to absolute form: %m' (errcode_for_file_access) before -1.
    // realpath("") fails with ENOENT on every supported libc.
    let mut logged = Vec::new();
    let err = normalize_exec_path("", &mut |c, m| logged.push((c, m, test_errno()))).unwrap_err();
    assert_eq!(
        err,
        "could not resolve path \"\" to absolute form: No such file or directory"
    );
    // errno is ENOENT when the LOG sink runs (errcode_for_file_access reads it).
    assert_eq!(logged, vec![(ExecLogCode::FileAccess, err, libc::ENOENT)]);
}

#[test]
fn path_is_safe_for_extraction_pins_c() {
    // path.c:637: canonicalize, then relative-and-below-cwd.
    assert!(path_is_safe_for_extraction("base/1/2"));
    assert!(path_is_safe_for_extraction("./base/./1"));
    assert!(path_is_safe_for_extraction("base/../pg_wal/x"));
    assert!(!path_is_safe_for_extraction("/etc/passwd"));
    assert!(!path_is_safe_for_extraction("../x"));
    assert!(!path_is_safe_for_extraction("base/../../x"));
    assert!(!path_is_safe_for_extraction(".."));
    let long = "a/".repeat(MAXPGPATH);
    assert!(path_is_safe_for_extraction(&long));
}

#[test]
fn get_home_path_prefers_home_then_passwd() {
    // path.c:1022. HOME is read through the environment, so this test owns
    // the variable for its duration (no other test in this crate touches it).
    let saved = std::env::var_os("HOME");
    std::env::set_var("HOME", "/tmp/pg_path_home");
    let from_env = get_home_path();
    std::env::set_var("HOME", "");
    let from_pw = get_home_path();
    match saved {
        Some(h) => std::env::set_var("HOME", h),
        None => std::env::remove_var("HOME"),
    }
    assert_eq!(from_env.as_deref(), Some("/tmp/pg_path_home"));
    // Empty $HOME falls through to getpwuid_r(geteuid()): the passwd entry
    // of the running user exists on any box that can run cargo test.
    let pw = from_pw.expect("passwd entry for the effective uid");
    assert!(!pw.is_empty());
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn find_other_exec_runs_target_dash_v() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("pg_path_other_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mkexe = |name: &str, body: &str| {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        p
    };
    let me = mkexe("fakepg", "#!/bin/sh\n");
    mkexe("other", "#!/bin/sh\necho \"other (Fake) 1.0\"\n");
    mkexe("silent", "#!/bin/sh\nexit 3\n");
    let me = me.to_str().unwrap();

    // exec.c:310 happy path: the resolved sibling path, version line
    // compared newline-included (pg_get_line keeps it).
    let mut logged = Vec::new();
    let found = find_other_exec(me, "other", "other (Fake) 1.0\n", |c, m| logged.push((c, m)));
    // find_my_exec realpath()s, so compare against the canonical directory.
    let real_dir = std::fs::canonicalize(&dir).unwrap();
    assert_eq!(found.as_deref(), Ok(real_dir.join("other").to_str().unwrap()));
    assert!(logged.is_empty());

    // -2: ran, wrong version string.
    let found = find_other_exec(me, "other", "other (Fake) 2.0\n", |_, _| {});
    assert_eq!(found, Err(FindOtherExecError::WrongVersion));

    // -1: no such sibling (validate_exec fails, nothing logged).
    let mut logged = Vec::new();
    let found = find_other_exec(me, "missing", "x", |c, m| logged.push((c, m)));
    assert_eq!(found, Err(FindOtherExecError::NotFound));
    assert!(logged.is_empty());

    // -1 via pipe_read_line: no output (ERRCODE_NO_DATA) then pclose_check
    // reports the exit status (ERRCODE_SYSTEM_ERROR, wait_result_to_str).
    let mut logged = Vec::new();
    let found = find_other_exec(me, "silent", "x", |c, m| logged.push((c, m)));
    assert_eq!(found, Err(FindOtherExecError::NotFound));
    let cmd = format!("\"{}\" -V", real_dir.join("silent").to_str().unwrap());
    assert_eq!(
        logged,
        vec![
            (ExecLogCode::NoData, format!("no data was returned by command \"{cmd}\"")),
            (ExecLogCode::SystemError, "child process exited with exit code 3".to_string()),
        ]
    );

    // -1 from find_my_exec itself (its own LOG line).
    let mut logged = Vec::new();
    let found = find_other_exec("/no/such/pg", "other", "x", |c, m| logged.push((c, m)));
    assert_eq!(found, Err(FindOtherExecError::NotFound));
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0].0, ExecLogCode::WrongObjectType);

    std::fs::remove_dir_all(&dir).unwrap();
}
