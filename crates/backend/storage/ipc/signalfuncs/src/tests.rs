use super::*;

fn setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(elog::init_seams);
}

#[test]
fn terminate_rejects_negative_timeout_before_signaling() {
    setup();
    let mut fci = ::types_fmgr::LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i32(12345));
    fci.set_arg(1, Datum::from_i64(-1));
    let err = fc_pg_terminate_backend(None, &mut fci).unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("must not be negative"), "{msg}");
}

#[test]
fn builtin_table_shape() {
    let rows = SIGNALFUNCS_BUILTINS;
    assert_eq!(rows.len(), 4);
    assert_eq!((rows[0].foid, rows[0].name, rows[0].nargs), (2096, "pg_terminate_backend", 2));
    assert_eq!((rows[1].foid, rows[1].name, rows[1].nargs), (2171, "pg_cancel_backend", 1));
    assert_eq!((rows[2].foid, rows[2].name, rows[2].nargs), (2621, "pg_reload_conf", 0));
    assert_eq!((rows[3].foid, rows[3].name, rows[3].nargs), (2622, "pg_rotate_logfile", 0));
    for r in rows {
        assert!(r.strict && !r.retset);
    }
}

#[test]
fn denied_error_surfaces_sqlstate_and_detail() {
    setup();
    let e = signal_denied(
        "permission denied to terminate process",
        "Only roles with the SUPERUSER attribute may terminate processes of roles with the \
         SUPERUSER attribute.",
    );
    let msg = format!("{e:?}");
    assert!(msg.contains("permission denied to terminate process"), "{msg}");
    assert!(msg.contains("SUPERUSER attribute"), "{msg}");
}
