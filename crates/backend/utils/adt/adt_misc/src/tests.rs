use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::TEXTOID;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, LocalFcinfo};

use crate::builtins::fc_parse_ident;

#[test]
fn pg_postmaster_start_time_reads_the_seam() {
    if !postmaster_seams::pg_start_time::is_installed() {
        postmaster_seams::pg_start_time::set(|| 123_456_789);
    }
    let ctx = MemoryContext::new("t");
    let mut fcinfo = LocalFcinfo::<0>::new(0);
    // SAFETY: mcx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    let d = crate::builtins::fc_pg_postmaster_start_time(None, &mut fcinfo).unwrap();
    assert_eq!(d.as_i64(), postmaster_seams::pg_start_time::call());
}

#[test]
fn pg_collation_for_no_argtype_is_null() {
    let ctx = MemoryContext::new("t");
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: mcx outlives the call.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    fcinfo.set_arg(0, Datum::null());
    // No fn_expr installed: C's get_fn_expr_argtype returns InvalidOid.
    let mut flinfo = FmgrInfo::new(crate::fc_pg_collation_for, 3162, 1, false, false);
    crate::fc_pg_collation_for(Some(&mut flinfo), &mut fcinfo).unwrap();
    assert!(fcinfo.isnull);
}

fn run(mcx: Mcx<'_>, input: &str, strict: bool) -> PgResult<Vec<String>> {
    // construct_array resolves the element type shape through the syscache
    // seam on current main (varlena tests' install_text_type_shape pattern).
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == TEXTOID).then_some(types_tuple::tupdesc::PgTypeShape {
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typstorage: b'x' as i8,
                typcollation: 100,
            }))
        });
    });
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    // SAFETY: mcx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    let text = varlena::cstring_to_text(mcx, input.as_bytes()).unwrap();
    fcinfo.set_arg(0, Datum::from_usize(text.as_bytes().as_ptr() as usize));
    fcinfo.set_arg(1, Datum::from_bool(strict));
    let d = fc_parse_ident(None, &mut fcinfo)?;
    let p = d.as_usize() as *const u8;
    let img = unsafe { core::slice::from_raw_parts(p, arrayfuncs::foundation::varsize_any(p)) };
    let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(mcx, img, TEXTOID, true).unwrap();
    Ok(elems
        .iter()
        .zip(nulls.iter())
        .map(|(&e, &isnull)| {
            assert!(!isnull);
            let p = e.as_usize() as *const u8;
            let bytes = unsafe {
                core::slice::from_raw_parts(p.add(4), arrayfuncs::foundation::varsize_any(p) - 4)
            };
            String::from_utf8(bytes.to_vec()).unwrap()
        })
        .collect())
}

#[test]
fn parse_ident_unquoted_downcases() {
    let ctx = MemoryContext::new("t");
    let parts = run(ctx.mcx(), "Foo.Bar", true).unwrap();
    assert_eq!(parts, vec!["foo", "bar"]);
}

#[test]
fn parse_ident_quoted_preserves_case() {
    let ctx = MemoryContext::new("t");
    let parts = run(ctx.mcx(), "\"MixedCase\"", true).unwrap();
    assert_eq!(parts, vec!["MixedCase"]);
}

#[test]
fn parse_ident_strict_trailing_garbage_errors() {
    let ctx = MemoryContext::new("t");
    let err = run(ctx.mcx(), "foo.bar!", true).unwrap_err();
    assert_eq!(err.message, "string is not a valid identifier: \"foo.bar!\"");
}

#[test]
fn parse_ident_nonstrict_tolerates_trailing_garbage() {
    let ctx = MemoryContext::new("t");
    let parts = run(ctx.mcx(), "foo.bar!", false).unwrap();
    assert_eq!(parts, vec!["foo", "bar"]);
}

#[test]
fn parse_ident_invalid_after_dot_message() {
    let ctx = MemoryContext::new("t");
    let err = run(ctx.mcx(), "foo.", true).unwrap_err();
    assert_eq!(err.message, "string is not a valid identifier: \"foo.\"");
    assert_eq!(err.detail.as_deref(), Some("No valid identifier after \".\"."));
}

#[test]
fn atooid_strtoul_semantics() {
    assert_eq!(crate::builtins::atooid("16384"), 16384);
    assert_eq!(crate::builtins::atooid("123abc"), 123);
    assert_eq!(crate::builtins::atooid("."), 0);
    assert_eq!(crate::builtins::atooid("pgsql_tmp"), 0);
}

#[test]
fn sys_fk_relationships_matches_generated_header() {
    let rows = crate::catalog_fk::SYS_FK_RELATIONSHIPS;
    assert_eq!(rows.len(), 219);
    assert_eq!(rows[0], (1255, 2615, "{pronamespace}", "{oid}", false, false));
    assert_eq!(rows[218], (6102, 1259, "{srrelid}", "{oid}", false, false));
    for (fk, pk, fkc, pkc, _, _) in rows {
        assert_ne!(*fk, 0);
        assert_ne!(*pk, 0);
        for cols in [fkc, pkc] {
            let inner = cols.strip_prefix('{').unwrap().strip_suffix('}').unwrap();
            assert!(!inner.is_empty() && !inner.contains(['"', ' ']));
        }
    }
}

fn install_jit_guc() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static JIT_GUC: AtomicBool = AtomicBool::new(true);
    guc_tables::vars::jit_enabled.install_if_absent(guc_tables::GucVarAccessors {
        get: || JIT_GUC.load(Ordering::Relaxed),
        set: |v| JIT_GUC.store(v, Ordering::Relaxed),
    });
}

fn jit_available() -> bool {
    let mut fcinfo = LocalFcinfo::<0>::new(0);
    crate::builtins::fc_pg_jit_available(None, &mut fcinfo).unwrap().as_bool()
}

#[test]
fn pg_jit_available_is_false_without_a_provider_shlib() {
    install_jit_guc();
    let dir = std::env::temp_dir().join(format!("pgrust-jit-probe-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut buf = [0u8; types_core::MAXPGPATH];
    let s = dir.to_str().unwrap().as_bytes();
    buf[..s.len()].copy_from_slice(s);
    init_small::globals::set_pkglib_path(buf);

    guc_tables::vars::jit_enabled.write(false);
    assert!(!jit_available(), "jit=off short-circuits, C provider_init()");
    guc_tables::vars::jit_enabled.write(true);
    assert!(!jit_available(), "no llvmjit.so in pkglib_path");
    // provider_failed_loading latches: a provider appearing later stays false.
    std::fs::write(dir.join("llvmjit.so"), b"").unwrap();
    assert!(!jit_available(), "failed probe is cached, C provider_failed_loading");
    let _ = std::fs::remove_dir_all(&dir);
    guc_tables::vars::jit_enabled.write(true);
}

#[test]
fn pg_trigger_depth_reads_the_executor_seam() {
    if !trigger_seams::my_trigger_depth::is_installed() {
        trigger_seams::my_trigger_depth::set(|| 2);
    }
    let mut fcinfo = LocalFcinfo::<0>::new(0);
    let d = crate::builtins::fc_pg_trigger_depth(None, &mut fcinfo).unwrap();
    assert_eq!(d.as_i32(), trigger_seams::my_trigger_depth::call());
}

#[test]
fn recovery_control_fns_error_outside_recovery() {
    use std::sync::atomic::{AtomicI32, Ordering};
    static XLOG_BUFFERS: AtomicI32 = AtomicI32::new(64);
    guc_tables::vars::XLOGbuffers.install_if_absent(guc_tables::GucVarAccessors {
        get: || XLOG_BUFFERS.load(Ordering::Relaxed),
        set: |v| XLOG_BUFFERS.store(v, Ordering::Relaxed),
    });
    transam_xlog::XLOGShmemInit();
    transam_xlog::ctl::XLogCtl()
        .SharedRecoveryState
        .store(transam_xlog::RECOVERY_STATE_DONE, std::sync::atomic::Ordering::Relaxed);
    for f in [
        crate::builtins::fc_pg_wal_replay_pause,
        crate::builtins::fc_pg_wal_replay_resume,
        crate::builtins::fc_pg_is_wal_replay_paused,
        crate::builtins::fc_pg_get_wal_replay_pause_state,
    ] {
        let mut fcinfo = LocalFcinfo::<0>::new(0);
        let e = f(None, &mut fcinfo).unwrap_err();
        assert_eq!(
            e.sqlstate(),
            types_error::make_sqlstate(*b"55000"),
            "ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE"
        );
        assert_eq!(e.message(), "recovery is not in progress");
        assert_eq!(
            e.hint(),
            Some("Recovery control functions can only be executed during recovery.")
        );
    }
}
