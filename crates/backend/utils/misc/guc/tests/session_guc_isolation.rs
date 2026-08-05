// Regression for the cross-session GUC clobber/leak (P1, found by
// partition-aggregate-lane): backing vars were process-shared atomics, so one
// session's SET leaked to every session and any child-thread bring-up
// boot-wrote non-snapshot vars over other sessions' SETs mid-statement.
// Session-scoped backings must give per-thread SET semantics: the exact
// production wiring (guc_tables::session_guc_bool! + engine set path) is
// exercised here on the enable_incremental_sort slot.

use std::sync::mpsc;

use guc_tables::GucVarAccessors;
use types_guc::{GucContext, GucSource};

guc_tables::session_guc_bool!(
    ENABLE_INCREMENTAL_SORT,
    enable_incremental_sort,
    set_enable_incremental_sort,
    true
);

fn setup_seams() {
    guc_tables::init_seams();
    elog::init_seams();
    guc::init_seams();
    xact_seams::is_in_parallel_mode::set(|| false);
    scalar_seams::parse_bool::set(|value| match value {
        "true" | "on" | "yes" | "1" => Some(true),
        "false" | "off" | "no" | "0" => Some(false),
        _ => None,
    });
    aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
    mbutils_seams::get_database_encoding::set(|| 6);
    timestamp_seams::get_current_timestamp::set(|| 0);
    guc_tables::vars::enable_incremental_sort.install_if_absent(GucVarAccessors {
        get: enable_incremental_sort,
        set: set_enable_incremental_sort,
    });
}

fn spawn_session(
    snapshot: Vec<guc::store::NondefaultGuc>,
    body: impl FnOnce() + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // SetConfigOption derives srole via GetUserId, which a bare thread
        // never initializes (real backends do so in InitPostgres).
        miscinit::SetUserIdAndSecContext(10, 0);
        guc::store::initialize_guc_options_for_child(&snapshot).unwrap();
        guc::store::restore_nondefault_variables(&snapshot).unwrap();
        body();
    })
}

#[test]
fn session_set_is_private_and_survives_child_bringup() {
    setup_seams();
    guc::store::initialize_guc_options().unwrap();
    assert!(enable_incremental_sort());

    let (a_ready_tx, a_ready_rx) = mpsc::channel::<()>();
    let (a_go_tx, a_go_rx) = mpsc::channel::<&'static str>();

    let snapshot_a = guc::store::capture_nondefault_variables();
    let a = spawn_session(snapshot_a, move || {
        guc::SetConfigOption(
            "enable_incremental_sort",
            Some("off"),
            GucContext::PGC_USERSET,
            GucSource::PGC_S_SESSION,
        )
        .unwrap();
        assert!(!enable_incremental_sort(), "session A does not see its own SET");
        a_ready_tx.send(()).unwrap();

        // Holds across another child's full GUC bring-up (the clobber window).
        let phase = a_go_rx.recv().unwrap();
        assert_eq!(phase, "after-child-bringup");
        assert!(
            !enable_incremental_sort(),
            "child bring-up boot-write clobbered session A's SET"
        );
        a_ready_tx.send(()).unwrap();
    });

    a_ready_rx.recv().unwrap();
    assert!(
        enable_incremental_sort(),
        "session A's SET leaked to the postmaster thread"
    );

    let snapshot_b = guc::store::capture_nondefault_variables();
    assert!(!snapshot_b.iter().any(|v| v.name == "enable_incremental_sort"));
    spawn_session(snapshot_b, || {
        assert!(
            enable_incremental_sort(),
            "session A's SET leaked into fresh session B"
        );
    })
    .join()
    .unwrap();

    a_go_tx.send("after-child-bringup").unwrap();
    a_ready_rx.recv().unwrap();
    a.join().unwrap();

    // Session exit tears down only A's TLS copy; this thread's is untouched.
    assert!(enable_incremental_sort());
}
