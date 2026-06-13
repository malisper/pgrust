//! Tests for the dynamic function manager.
//!
//! Seam slots are process-global `OnceLock`s (install-once), so the loader,
//! path, and fd seams are installed exactly once here behind a `Once`; their
//! implementations consult thread-local test fixtures so individual tests can
//! configure behavior without reinstalling. The dfmgr state itself
//! (`FILE_LIST`/`RENDEZVOUS`) is thread-local, so state-mutating tests run on
//! their own threads to stay isolated.

use super::*;
use std::cell::RefCell;
use std::sync::Once;

use types_core::fmgr::FMGR_ABI_EXTRA;

thread_local! {
    /// Files the `pg_file_exists` test seam reports as existing.
    static EXISTING_FILES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    /// Scripted `stat_identity` result.
    static STAT_IDENTITY: RefCell<FileIdentity> =
        const { RefCell::new(FileIdentity { device: 0, inode: 0 }) };
    /// Scripted `open_library` outcome.
    static OPEN_RESULT: RefCell<Option<LibraryOpen>> = const { RefCell::new(None) };
    /// Symbols the `function_exists` test seam reports as present.
    static PRESENT_SYMBOLS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

static INSTALL: Once = Once::new();

fn install_seams() {
    INSTALL.call_once(|| {
        // common/path.c: lexical non-Windows canonicalize is enough.
        common_path_seams::canonicalize_path::set(|path| {
            let absolute = path.starts_with('/');
            let mut out: Vec<&str> = Vec::new();
            for comp in path.split('/') {
                match comp {
                    "" | "." => {}
                    ".." => {
                        out.pop();
                    }
                    other => out.push(other),
                }
            }
            let joined = out.join("/");
            if absolute {
                format!("/{joined}")
            } else if joined.is_empty() {
                ".".to_owned()
            } else {
                joined
            }
        });
        common_path_seams::is_absolute_path::set(|path| path.starts_with('/'));
        backend_storage_file_fd_seams::pg_file_exists::set(|name| {
            Ok(EXISTING_FILES.with(|f| f.borrow().iter().any(|e| e == name)))
        });
        loader::stat_identity::set(|_| Ok(STAT_IDENTITY.with(|s| *s.borrow())));
        loader::open_library::set(|_| {
            Ok(OPEN_RESULT
                .with(|o| o.borrow().clone())
                .expect("test must script open_library"))
        });
        loader::call_pg_init::set(|_| Ok(()));
        loader::close_library::set(|_| {});
        loader::function_exists::set(|_, funcname| {
            PRESENT_SYMBOLS.with(|s| s.borrow().iter().any(|e| e == funcname))
        });
    });
}

fn set_existing(files: &[&str]) {
    EXISTING_FILES.with(|f| *f.borrow_mut() = files.iter().map(|s| s.to_string()).collect());
}

fn with_mcx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
    let ctx = mcx::MemoryContext::new("dfmgr-test");
    f(ctx.mcx())
}

#[test]
fn substitutes_path_macros_like_postgres() {
    install_seams();
    with_mcx(|mcx| {
        assert_eq!(
            substitute_path_macro(mcx, "$libdir/foo", "$libdir", "/pg/lib")
                .unwrap()
                .as_str(),
            "/pg/lib/foo"
        );
        // No leading '$' => returned unchanged.
        assert_eq!(
            substitute_path_macro(mcx, "/absolute/foo", "$libdir", "/pg/lib")
                .unwrap()
                .as_str(),
            "/absolute/foo"
        );
        // A bare "$libdir" with no separator expands the whole string.
        assert_eq!(
            substitute_path_macro(mcx, "$libdir", "$libdir", "/pg/lib")
                .unwrap()
                .as_str(),
            "/pg/lib"
        );
        // Wrong macro name => ERRCODE_INVALID_NAME.
        let error =
            substitute_path_macro(mcx, "$wrong/foo", "$libdir", "/pg/lib").unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INVALID_NAME);
        assert_eq!(error.message(), "invalid macro name in path: $wrong/foo");
    });
}

#[test]
fn validates_restricted_library_names() {
    assert!(check_restricted_library_name("$libdir/plugins/auth").is_ok());

    let error = check_restricted_library_name("$libdir/plugins/nested/auth").unwrap_err();
    assert_eq!(error.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
    assert_eq!(
        error.message(),
        "access to library \"$libdir/plugins/nested/auth\" is not allowed"
    );

    // Not under $libdir/plugins/ at all.
    let error = check_restricted_library_name("/etc/passwd").unwrap_err();
    assert_eq!(error.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
}

#[test]
fn find_in_path_empty_path_returns_none() {
    install_seams();
    with_mcx(|mcx| {
        assert!(
            find_in_path(mcx, "demo.so", "", "dynamic_library_path", "$libdir", "/pg/lib")
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn find_in_path_zero_length_component_errors() {
    install_seams();
    with_mcx(|mcx| {
        // Leading ':' => zero-length first component.
        let error = find_in_path(
            mcx,
            "demo.so",
            ":/pg/lib",
            "dynamic_library_path",
            "$libdir",
            "/pg/lib",
        )
        .unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INVALID_NAME);
        assert_eq!(
            error.message(),
            "zero-length component in parameter \"dynamic_library_path\""
        );
    });
}

#[test]
fn find_in_path_non_absolute_component_errors() {
    install_seams();
    with_mcx(|mcx| {
        let error = find_in_path(
            mcx,
            "demo.so",
            "relative/dir",
            "dynamic_library_path",
            "$libdir",
            "/pg/lib",
        )
        .unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INVALID_NAME);
        assert_eq!(
            error.message(),
            "component in parameter \"dynamic_library_path\" is not an absolute path"
        );
    });
}

#[test]
fn find_in_path_locates_existing_file() {
    install_seams();
    set_existing(&["/pg/lib/demo.so"]);
    with_mcx(|mcx| {
        assert_eq!(
            find_in_path(
                mcx,
                "demo.so",
                "/other:/pg/lib",
                "dynamic_library_path",
                "$libdir",
                "/pg/lib",
            )
            .unwrap()
            .map(|s| s.as_str().to_owned()),
            Some("/pg/lib/demo.so".to_owned())
        );
    });
    set_existing(&[]);
    with_mcx(|mcx| {
        assert!(find_in_path(
            mcx,
            "missing.so",
            "/a:/b",
            "dynamic_library_path",
            "$libdir",
            "/pg/lib",
        )
        .unwrap()
        .is_none());
    });
}

#[test]
fn reports_version_mismatch() {
    let mut abi = PgAbiValues::server();
    abi.version += 1;
    let error = incompatible_module_error("demo", &abi);
    assert_eq!(error.message(), "incompatible library \"demo\": version mismatch");
    assert!(error.detail().unwrap().starts_with("Server is version "));
}

#[test]
fn reports_abi_extra_mismatch() {
    let mut abi = PgAbiValues::server();
    abi.abi_extra = *b"NotPostgreSQL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
    let error = incompatible_module_error("demo", &abi);
    assert_eq!(error.message(), "incompatible library \"demo\": ABI mismatch");
    assert_eq!(
        error.detail(),
        Some("Server has ABI \"PostgreSQL\", library has \"NotPostgreSQL\".")
    );
}

#[test]
fn abi_extra_compares_as_c_string_not_full_array() {
    // C compares abi_extra with strcmp (stops at the first NUL). A library
    // whose abi_extra matches "PostgreSQL\0" but differs only in bytes *after*
    // the terminator, with every other ABI field equal, must NOT trip the ABI
    // branch: C falls through to the field-by-field section, finds everything
    // equal, and emits the magic-block-mismatch fallback. (This function is
    // reached after the full-32-byte abi_fields memcmp in internal_load_library
    // has already failed, so this padding-only case is genuinely reachable.)
    let mut abi = PgAbiValues::server();
    // "PostgreSQL\0" prefix identical to the server; differ only past the NUL.
    abi.abi_extra[11] = b'X';
    assert_ne!(abi.abi_extra, PgAbiValues::server().abi_extra);

    let error = incompatible_module_error("demo", &abi);
    assert_eq!(
        error.message(),
        "incompatible library \"demo\": magic block mismatch"
    );
    assert_eq!(
        error.detail(),
        Some("Magic block has unexpected length or padding difference.")
    );
}

#[test]
fn reports_field_mismatch_details() {
    let mut abi = PgAbiValues::server();
    abi.funcmaxargs += 1;
    let error = incompatible_module_error("demo", &abi);
    assert_eq!(
        error.message(),
        "incompatible library \"demo\": magic block mismatch"
    );
    assert_eq!(
        error.detail(),
        Some("Server has FUNC_MAX_ARGS = 100, library has 101.")
    );
}

#[test]
fn reports_fallback_detail_when_only_padding_differs() {
    // No individual field differs (abi_fields equal) but the caller reached
    // here via a len mismatch: the fallback line is emitted.
    let server = PgAbiValues::server();
    let detail = build_field_mismatch_detail(&server, &server).unwrap();
    assert_eq!(detail, "Magic block has unexpected length or padding difference.");
}

#[test]
fn check_module_magic_accepts_matching_block() {
    let magic = Pg_magic_struct {
        len: magic_struct_len() as i32,
        abi_fields: PgAbiValues::server(),
        name: None,
        version: None,
    };
    assert!(check_module_magic("demo", &magic).is_ok());

    let mut bad = magic.clone();
    bad.len += 8;
    assert!(check_module_magic("demo", &bad).is_err());
}

#[test]
fn fmgr_abi_extra_renders_postgresql() {
    assert_eq!(abi_extra_string(&FMGR_ABI_EXTRA), "PostgreSQL");
}

#[test]
fn rendezvous_variables_are_stable_per_name() {
    std::thread::spawn(|| {
        assert_eq!(find_rendezvous_variable("x"), 0);
        set_rendezvous_variable("x", 0x1234);
        assert_eq!(find_rendezvous_variable("x"), 0x1234);
        assert_eq!(find_rendezvous_variable("y"), 0);
    })
    .join()
    .unwrap();
}

#[test]
fn loads_once_and_serializes_round_trips() {
    std::thread::spawn(|| {
        install_seams();
        STAT_IDENTITY.with(|s| *s.borrow_mut() = FileIdentity { device: 7, inode: 42 });
        OPEN_RESULT.with(|o| {
            *o.borrow_mut() = Some(LibraryOpen::WithMagic {
                handle: LibraryHandle(1),
                magic: Pg_magic_struct {
                    len: magic_struct_len() as i32,
                    abi_fields: PgAbiValues::server(),
                    name: Some("demo_module".to_owned()),
                    version: Some("1.0".to_owned()),
                },
            });
        });

        with_mcx(|mcx| {
            // Same filename string loads once (found by name).
            load_file(mcx, "/lib/demo.so", false).unwrap();
            load_file(mcx, "/lib/demo.so", false).unwrap();
        });
        assert_eq!(estimate_library_state_space(), "/lib/demo.so".len() + 2);

        let details = get_loaded_module_details(0).unwrap();
        assert_eq!(details.library_path, "/lib/demo.so");
        assert_eq!(details.module_name.as_deref(), Some("demo_module"));
        assert_eq!(details.module_version.as_deref(), Some("1.0"));
        assert_eq!(get_first_loaded_module(), Some(0));
        assert_eq!(get_next_loaded_module(0), None);

        let size = estimate_library_state_space();
        let mut buffer = vec![0_u8; size];
        serialize_library_state(size, &mut buffer).unwrap();
        // Restore in a fresh backend thread.
        std::thread::spawn(move || {
            install_seams();
            STAT_IDENTITY.with(|s| *s.borrow_mut() = FileIdentity { device: 7, inode: 42 });
            OPEN_RESULT.with(|o| {
                *o.borrow_mut() = Some(LibraryOpen::WithMagic {
                    handle: LibraryHandle(1),
                    magic: Pg_magic_struct {
                        len: magic_struct_len() as i32,
                        abi_fields: PgAbiValues::server(),
                        name: None,
                        version: None,
                    },
                });
            });
            restore_library_state(&buffer).unwrap();
            assert_eq!(get_loaded_module_details(0).unwrap().library_path, "/lib/demo.so");
        })
        .join()
        .unwrap();
    })
    .join()
    .unwrap();
}

#[test]
fn same_inode_dedups_distinct_paths() {
    std::thread::spawn(|| {
        install_seams();
        STAT_IDENTITY.with(|s| *s.borrow_mut() = FileIdentity { device: 7, inode: 99 });
        OPEN_RESULT.with(|o| {
            *o.borrow_mut() = Some(LibraryOpen::WithMagic {
                handle: LibraryHandle(5),
                magic: Pg_magic_struct {
                    len: magic_struct_len() as i32,
                    abi_fields: PgAbiValues::server(),
                    name: None,
                    version: None,
                },
            });
        });
        with_mcx(|mcx| {
            load_file(mcx, "/a/demo.so", false).unwrap();
            // Different filename, same inode => dedup by SAME_INODE.
            load_file(mcx, "/b/symlink.so", false).unwrap();
        });
        assert_eq!(get_first_loaded_module(), Some(0));
        assert_eq!(get_next_loaded_module(0), None);
    })
    .join()
    .unwrap();
}

#[test]
fn missing_magic_block_errors_and_closes() {
    std::thread::spawn(|| {
        install_seams();
        STAT_IDENTITY.with(|s| *s.borrow_mut() = FileIdentity { device: 1, inode: 2 });
        OPEN_RESULT.with(|o| {
            *o.borrow_mut() = Some(LibraryOpen::MissingMagic {
                handle: LibraryHandle(3),
            });
        });
        with_mcx(|mcx| {
            let error = load_file(mcx, "/lib/nomagic.so", false).unwrap_err();
            assert_eq!(
                error.message(),
                "incompatible library \"/lib/nomagic.so\": missing magic block"
            );
            assert_eq!(
                error.hint(),
                Some("Extension libraries are required to use the PG_MODULE_MAGIC macro.")
            );
        });
        assert_eq!(get_first_loaded_module(), None);
    })
    .join()
    .unwrap();
}

#[test]
fn load_external_function_signals_not_found() {
    std::thread::spawn(|| {
        install_seams();
        STAT_IDENTITY.with(|s| *s.borrow_mut() = FileIdentity { device: 1, inode: 2 });
        OPEN_RESULT.with(|o| {
            *o.borrow_mut() = Some(LibraryOpen::WithMagic {
                handle: LibraryHandle(8),
                magic: Pg_magic_struct {
                    len: magic_struct_len() as i32,
                    abi_fields: PgAbiValues::server(),
                    name: None,
                    version: None,
                },
            });
        });
        PRESENT_SYMBOLS.with(|s| *s.borrow_mut() = vec!["present".to_owned()]);

        with_mcx(|mcx| {
            let handle =
                load_external_function(mcx, "/lib/demo.so", "present", true).unwrap();
            assert_eq!(handle, LibraryHandle(8));

            let error =
                load_external_function(mcx, "/lib/demo.so", "absent", true).unwrap_err();
            assert_eq!(error.sqlstate(), ERRCODE_UNDEFINED_FUNCTION);
            assert_eq!(
                error.message(),
                "could not find function \"absent\" in file \"/lib/demo.so\""
            );

            let handle =
                load_external_function(mcx, "/lib/demo.so", "absent", false).unwrap();
            assert_eq!(handle, LibraryHandle(8));
        });
    })
    .join()
    .unwrap();
}

#[test]
fn load_file_restricted_check_runs_first() {
    install_seams();
    with_mcx(|mcx| {
        let error = load_file(mcx, "/etc/passwd", true).unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
    });
}
