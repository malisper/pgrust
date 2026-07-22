// Regression for the io_method boot race: a child thread's GUC bring-up must
// never publish a boot default over process-shared backing storage for a
// variable its snapshot restore is about to overwrite (postmaster read
// io_method=worker mid-window and launched io workers despite -c
// io_method=sync). The boot default has since flipped to sync (worker is
// unported), so this poisons the opposite direction: pin worker over a sync
// boot and assert bring-up never publishes sync. The registry treats the
// uninstalled check_io_method slot as C's NULL hook, so the pin is accepted
// here even though the full server refuses worker.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use guc_tables::consts::{IOMETHOD_SYNC, IOMETHOD_WORKER};
use guc_tables::GucVarAccessors;
use types_guc::{config_enum_entry, GucContext, GucSource};

static IO_METHOD: AtomicI32 = AtomicI32::new(IOMETHOD_SYNC);
static WRITES: Mutex<Vec<i32>> = Mutex::new(Vec::new());

fn io_method_get() -> i32 {
    IO_METHOD.load(Ordering::Relaxed)
}

fn io_method_set(v: i32) {
    WRITES.lock().unwrap().push(v);
    IO_METHOD.store(v, Ordering::Relaxed);
}

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
    guc_tables::option_sets::io_method_options.install(&[
        config_enum_entry { name: "sync", val: IOMETHOD_SYNC, hidden: false },
        config_enum_entry { name: "worker", val: IOMETHOD_WORKER, hidden: false },
    ]);
    guc_tables::vars::io_method.install(GucVarAccessors {
        get: io_method_get,
        set: io_method_set,
    });
}

#[test]
fn child_bringup_never_publishes_boot_value_over_snapshot() {
    setup_seams();

    guc::store::initialize_guc_options().unwrap();
    guc::SetConfigOption(
        "io_method",
        Some("worker"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .unwrap();
    assert_eq!(io_method_get(), IOMETHOD_WORKER);

    let snapshot = guc::store::capture_nondefault_variables();
    assert!(snapshot.iter().any(|v| v.name == "io_method"));
    WRITES.lock().unwrap().clear();

    std::thread::spawn(move || {
        guc::store::initialize_guc_options_for_child(&snapshot).unwrap();
        guc::store::restore_nondefault_variables(&snapshot).unwrap();
    })
    .join()
    .unwrap();

    let writes = WRITES.lock().unwrap().clone();
    assert!(
        !writes.contains(&IOMETHOD_SYNC),
        "child GUC bring-up published boot io_method=sync to shared storage: {writes:?}"
    );
    assert_eq!(io_method_get(), IOMETHOD_WORKER);
}
