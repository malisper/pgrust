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
    assert_eq!(validate_exec(dir.to_str().unwrap()), -1);
    assert_eq!(validate_exec("/no/such/file"), -1);

    let mut logged = Vec::new();
    let found = find_my_exec(exe_str, |m| logged.push(m)).unwrap();
    assert!(is_absolute_path(&found));
    assert!(found.ends_with("/fakepg"));
    assert!(logged.is_empty());

    let err = find_my_exec("/no/such/file", |_| {}).unwrap_err();
    assert_eq!(err, "invalid binary \"/no/such/file\"");

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
    let found = find_my_exec("fakepg", |m| logged.push(m));
    match saved {
        Some(p) => std::env::set_var("PATH", p),
        None => std::env::remove_var("PATH"),
    }
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
