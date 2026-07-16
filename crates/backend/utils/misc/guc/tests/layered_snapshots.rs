// GUC layered immutable snapshots (guc::layers, parallelism-redesign §2.4):
//
// 1. Reload dead-diff hazard regression (the recovery program's hazard class,
//    notes/recovery-standby-tail-state.md): SIGHUP-scope string GUC backings
//    are process-global, so C's reload-diff pattern (read global, run own
//    ProcessConfigFile, compare global) is silently dead in thread children —
//    the postmaster's reload writes the shared backing before the child's own
//    pass runs, so pre == post. The layered fix: a child diffs its OWN
//    started-with base Arc against the base it adopts after its own pass.
//    This test demonstrates the naive pattern is dead AND the layered pattern
//    sees exactly the true transition.
//
// 2. Base immutability + atomic republish (epoch monotonicity).
//
// 3. Query-pin caching and stability: same Arc within a statement window,
//    invalidated by the session's own SET, NOT by a concurrent republish;
//    adoption alone also invalidates the cache.
//
// 4. Worker pin bind: applying a leader's pin on a fresh thread reproduces
//    the leader's session state and adopts the leader's base.

use std::sync::{mpsc, Mutex, RwLock};

use guc_tables::GucVarAccessors;
use types_guc::{GucContext, GucSource};

// Tests in one binary run concurrently on separate threads; the base layer is
// process-global by design, so serialize.
static TEST_LOCK: Mutex<()> = Mutex::new(());

// The hazard subject: archive_library (PGC_SIGHUP string, no hooks), backed —
// as in production string_var! backings — by a PROCESS-GLOBAL RwLock shared
// by every thread. This shared cell is what makes the naive diff dead.
static ARCHIVE_LIBRARY_GLOBAL: RwLock<Option<String>> = RwLock::new(None);

fn archive_library_global() -> Option<String> {
    match &*ARCHIVE_LIBRARY_GLOBAL.read().unwrap() {
        Some(s) => Some(s.clone()),
        None => Some(String::new()), // boot default
    }
}

fn set_archive_library_global(v: Option<String>) {
    *ARCHIVE_LIBRARY_GLOBAL.write().unwrap() = v;
}

// A USERSET session var for the pin/bind legs (same wiring as
// session_guc_isolation.rs: production session-TLS backing shape).
guc_tables::session_guc_bool!(
    ENABLE_INCREMENTAL_SORT,
    enable_incremental_sort,
    set_enable_incremental_sort,
    true
);

fn setup_seams() {
    // Process-wide installs exactly once: several tests share this binary and
    // init_seams()' slot installs panic on a second call (slots.rs:50).
    static SEAMS: std::sync::Once = std::sync::Once::new();
    SEAMS.call_once(|| {
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
        guc_tables::vars::XLogArchiveLibrary.install_if_absent(GucVarAccessors {
            get: archive_library_global,
            set: set_archive_library_global,
        });
        guc_tables::vars::enable_incremental_sort.install_if_absent(GucVarAccessors {
            get: enable_incremental_sort,
            set: set_enable_incremental_sort,
        });
    });
    // Per-thread: SetConfigOption derives srole via GetUserId (real backends
    // run InitPostgres).
    miscinit::SetUserIdAndSecContext(10, 0);
}

fn captured_string(base: &guc::layers::GucBaseSnapshot, name: &str) -> Option<String> {
    match base.get(name).map(guc::store::CapturedGuc::value) {
        Some(guc::model::config_var_val::Stringval(v)) => v.clone(),
        other => panic!("{name}: expected a captured string value, got {other:?}"),
    }
}

fn spawn_session(
    snapshot: Vec<guc::store::NondefaultGuc>,
    body: impl FnOnce() + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        miscinit::SetUserIdAndSecContext(10, 0);
        guc::store::initialize_guc_options_for_child(&snapshot).unwrap();
        guc::store::restore_nondefault_variables(&snapshot).unwrap();
        body();
    })
}

#[test]
fn reload_diff_hazard_is_fixed_by_layered_bases() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    guc::layers::reset_layers_for_tests();
    setup_seams();
    guc::store::initialize_guc_options().unwrap();

    // Postmaster boot config: archive_library = 'walarch-v1' from the file.
    guc::SetConfigOption(
        "archive_library",
        Some("walarch-v1"),
        GucContext::PGC_SIGHUP,
        GucSource::PGC_S_FILE,
    )
    .unwrap();
    let base_boot = guc::layers::ensure_base_current();
    assert_eq!(captured_string(&base_boot, "archive_library"), Some("walarch-v1".into()));
    let boot_epoch = base_boot.epoch();

    // Re-publish with no store change: same base, same epoch (cache hit).
    let again = guc::layers::ensure_base_current();
    assert_eq!(again.epoch(), boot_epoch);

    let (child_ready_tx, child_ready_rx) = mpsc::channel::<()>();
    let (reloaded_tx, reloaded_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();

    let snapshot = guc::store::capture_nondefault_variables();
    let child = spawn_session(snapshot, move || {
        // Child start: record started-with values BOTH ways.
        let started_with = guc::layers::session_base();
        let naive_pre = archive_library_global(); // the C pattern's "old" read
        assert_eq!(naive_pre, Some("walarch-v1".into()));
        assert_eq!(
            captured_string(&started_with, "archive_library"),
            Some("walarch-v1".into())
        );
        child_ready_tx.send(()).unwrap();

        // ... the postmaster reloads while this child is busy ...
        reloaded_rx.recv().unwrap();

        // THE HAZARD, demonstrated: the child has not yet run its own reload
        // pass, but its "pre" value — re-read from the process-global backing
        // the way ported C reload-diff code would — already shows the NEW
        // value. old == new: the diff is dead.
        let naive_old_reread = archive_library_global();
        assert_eq!(
            naive_old_reread,
            Some("walarch-v2".into()),
            "expected the shared backing to be clobbered by the postmaster's reload \
             (that clobber IS the hazard this test regresses)"
        );

        // THE FIX: the child's started-with Arc is immutable — its pre value
        // is still the truth it started with.
        assert_eq!(
            captured_string(&started_with, "archive_library"),
            Some("walarch-v1".into()),
            "started-with base must be immune to the postmaster's reload"
        );

        // The child's own reload pass (a backend runs ProcessConfigFile
        // between statements; modeled here by the file value's set + base
        // adoption, the layers contract).
        guc::SetConfigOption(
            "archive_library",
            Some("walarch-v2"),
            GucContext::PGC_SIGHUP,
            GucSource::PGC_S_FILE,
        )
        .unwrap();
        let adopted = guc::layers::adopt_current_base();

        // The layered diff sees exactly the true transition, exactly once.
        assert!(adopted.epoch() > started_with.epoch());
        assert_eq!(captured_string(&adopted, "archive_library"), Some("walarch-v2".into()));
        assert_ne!(
            captured_string(&started_with, "archive_library"),
            captured_string(&adopted, "archive_library"),
            "layered reload-diff must fire on a real config change"
        );

        // Idempotent from here: the adopted base IS the session base now.
        let stable = guc::layers::session_base();
        assert_eq!(stable.epoch(), adopted.epoch());
        done_tx.send(()).unwrap();
    });

    child_ready_rx.recv().unwrap();

    // Postmaster SIGHUP: apply the new file value, publish a NEW base
    // atomically (process_pm_reload_request order: apply, publish, signal).
    guc::SetConfigOption(
        "archive_library",
        Some("walarch-v2"),
        GucContext::PGC_SIGHUP,
        GucSource::PGC_S_FILE,
    )
    .unwrap();
    let base_reloaded = guc::layers::ensure_base_current();
    assert_eq!(base_reloaded.epoch(), boot_epoch + 1);
    assert_eq!(captured_string(&base_reloaded, "archive_library"), Some("walarch-v2".into()));
    // Immutability: the boot base still says v1.
    assert_eq!(captured_string(&base_boot, "archive_library"), Some("walarch-v1".into()));
    reloaded_tx.send(()).unwrap();

    done_rx.recv().unwrap();
    child.join().unwrap();
}

#[test]
fn query_pin_caches_per_statement_window_and_ignores_republish() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    guc::layers::reset_layers_for_tests();
    setup_seams();
    guc::store::initialize_guc_options().unwrap();
    guc::layers::ensure_base_current();

    let pin1 = guc::layers::current_query_pin();
    let pin2 = guc::layers::current_query_pin();
    assert!(std::sync::Arc::ptr_eq(&pin1, &pin2), "clean window must reuse the pin");

    // The session's own SET invalidates the cache and lands in the new pin.
    guc::SetConfigOption(
        "enable_incremental_sort",
        Some("off"),
        GucContext::PGC_USERSET,
        GucSource::PGC_S_SESSION,
    )
    .unwrap();
    let pin3 = guc::layers::current_query_pin();
    assert!(!std::sync::Arc::ptr_eq(&pin2, &pin3), "SET must invalidate the pin cache");
    assert!(
        pin3.session_vars().iter().any(|v| v.name() == "enable_incremental_sort"),
        "the SET must be captured in the new pin"
    );

    // A concurrent postmaster republish must NOT move a session's pin: the
    // session adopts a new base only through its own reload pass.
    let session_epoch = pin3.base().epoch();
    std::thread::spawn(|| {
        miscinit::SetUserIdAndSecContext(10, 0);
        guc::store::initialize_guc_options().unwrap();
        guc::SetConfigOption(
            "archive_library",
            Some("walarch-v3"),
            GucContext::PGC_SIGHUP,
            GucSource::PGC_S_FILE,
        )
        .unwrap();
        guc::layers::ensure_base_current();
    })
    .join()
    .unwrap();
    assert!(guc::layers::current_base().epoch() > session_epoch);
    let pin4 = guc::layers::current_query_pin();
    assert!(
        std::sync::Arc::ptr_eq(&pin3, &pin4),
        "a republish elsewhere must not perturb this session's pin"
    );
    assert_eq!(pin4.base().epoch(), session_epoch);

    // Adoption (the session's own reload point) refreshes pin + base.
    let adopted = guc::layers::adopt_current_base();
    let pin5 = guc::layers::current_query_pin();
    assert!(!std::sync::Arc::ptr_eq(&pin4, &pin5), "adoption must invalidate the pin cache");
    assert_eq!(pin5.base().epoch(), adopted.epoch());
}

#[test]
fn query_pin_remint_dedup_preserves_identity_on_content_equal_state() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    guc::layers::reset_layers_for_tests();
    setup_seams();
    guc::store::initialize_guc_options().unwrap();
    guc::layers::ensure_base_current();

    guc::SetConfigOption(
        "enable_incremental_sort",
        Some("off"),
        GucContext::PGC_USERSET,
        GucSource::PGC_S_SESSION,
    )
    .unwrap();
    let pin1 = guc::layers::current_query_pin();

    // A no-op SET (same value, same source) bumps the store mutation counter
    // but re-mints content-identical state: the dedup must serve the SAME
    // Arc — pin identity is the standing-gang sticky-binding key.
    guc::SetConfigOption(
        "enable_incremental_sort",
        Some("off"),
        GucContext::PGC_USERSET,
        GucSource::PGC_S_SESSION,
    )
    .unwrap();
    let pin2 = guc::layers::current_query_pin();
    assert!(
        std::sync::Arc::ptr_eq(&pin1, &pin2),
        "no-op SET must dedup to the cached pin (content-equal re-mint)"
    );

    // A content CHANGE must still mint a new pin (dedup never hides real
    // state movement).
    guc::SetConfigOption(
        "enable_incremental_sort",
        Some("on"),
        GucContext::PGC_USERSET,
        GucSource::PGC_S_SESSION,
    )
    .unwrap();
    let pin3 = guc::layers::current_query_pin();
    assert!(!std::sync::Arc::ptr_eq(&pin2, &pin3), "content change must mint a new pin");
    assert!(
        pin3.session_vars()
            .iter()
            .any(|v| v.name() == "enable_incremental_sort"),
        "the SET must be captured in the new pin"
    );
}

#[test]
fn worker_pin_bind_reproduces_leader_state_and_base() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    guc::layers::reset_layers_for_tests();
    setup_seams();
    guc::store::initialize_guc_options().unwrap();
    guc::layers::ensure_base_current();

    guc::SetConfigOption(
        "enable_incremental_sort",
        Some("off"),
        GucContext::PGC_USERSET,
        GucSource::PGC_S_SESSION,
    )
    .unwrap();
    let pin = guc::layers::current_query_pin();
    let leader_epoch = pin.base().epoch();

    let worker_pin = pin.clone();
    std::thread::spawn(move || {
        miscinit::SetUserIdAndSecContext(10, 0);
        guc::store::initialize_guc_options().unwrap();
        assert!(enable_incremental_sort(), "fresh worker starts at boot default");
        let binding = guc::layers::bind_query_pin(&worker_pin).unwrap();
        assert!(!enable_incremental_sort(), "bind must reproduce the leader's SET");
        assert_eq!(
            guc::store::get_bool("enable_incremental_sort"),
            Some(false),
            "bind must land in the worker's registry too"
        );
        assert_eq!(
            guc::layers::session_base().epoch(),
            leader_epoch,
            "worker must adopt the leader's base"
        );
        drop(binding);
        assert!(!guc::store::session_bound());
    })
    .join()
    .unwrap();

    // Leader unaffected.
    assert!(!enable_incremental_sort());
}

// Inc-3 launch path: a child brought up from the shared base (typed bind, no
// string re-parse) reproduces the postmaster's nondefault state — value,
// source, and registry end-state — and adopts the base as its started-with
// view. Mirrors postmaster_child_launch's base_share_enabled arm.
#[test]
fn child_bringup_from_shared_base_matches_postmaster_state() {
    let _lock = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    guc::layers::reset_layers_for_tests();
    setup_seams();
    guc::store::initialize_guc_options().unwrap();

    guc::SetConfigOption(
        "archive_library",
        Some("walarch-base"),
        GucContext::PGC_SIGHUP,
        GucSource::PGC_S_FILE,
    )
    .unwrap();
    guc::SetConfigOption(
        "work_mem",
        Some("4321"),
        GucContext::PGC_POSTMASTER,
        GucSource::PGC_S_ARGV,
    )
    .unwrap();
    let base = guc::layers::ensure_base_current();
    assert!(base.contains("archive_library") && base.contains("work_mem"));

    let child_base = base.clone();
    let base_epoch = base.epoch();
    std::thread::spawn(move || {
        miscinit::SetUserIdAndSecContext(10, 0);
        guc::store::initialize_guc_options_for_child_base(&child_base).unwrap();
        guc::layers::bind_base(&child_base).unwrap();
        assert_eq!(
            guc::store::get_string("archive_library"),
            Some(Some("walarch-base".into())),
            "typed base bind must land the postmaster's string value"
        );
        assert_eq!(guc::store::get_int("work_mem"), Some(4321));
        assert_eq!(
            guc::layers::session_base().epoch(),
            base_epoch,
            "child must adopt the base it was launched from"
        );
    })
    .join()
    .unwrap();
}
