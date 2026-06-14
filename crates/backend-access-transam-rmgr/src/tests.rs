use super::*;

use std::cell::{Cell, RefCell};
use std::sync::Once;

use types_wal::rmgr::{RM_MAX_BUILTIN_ID, RM_NEXT_ID, RmgrIdIsValid, RM_EXPERIMENTAL_ID};

thread_local! {
    static IN_PRELOAD: Cell<bool> = const { Cell::new(false) };
    static STARTUPS: Cell<u32> = const { Cell::new(0) };
    static CLEANUPS: Cell<u32> = const { Cell::new(0) };
    static SRF_INITS: Cell<u32> = const { Cell::new(0) };
    // The SRF hands canonical `Datum<'mcx>` values whose lifetime is tied to
    // the per-query context; this `'static` thread_local cannot hold a borrow,
    // so we capture the by-value scalar word (`types_datum::Datum`, the `ByVal`
    // payload) — every emitted column is a by-value scalar here.
    static ROWS: RefCell<Vec<(Vec<types_datum::Datum>, Vec<bool>)>> =
        const { RefCell::new(Vec::new()) };
    static LOGS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// Install (process-wide, once) the seams the tested paths cross. The table
/// itself needs no installs — its slots hold the owner seams' `call` fns and
/// are only invoked where a test installs the owner (startup/cleanup below).
fn setup() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        miscinit::process_shared_preload_libraries_in_progress::set(|| {
            IN_PRELOAD.with(|c| c.get())
        });
        error_seams::ereport::set(|err| {
            LOGS.with(|l| l.borrow_mut().push(err.message().to_string()));
            Ok(())
        });

        // The four AMs with replay contexts (rmgrlist.h startup/cleanup
        // columns).
        nbtxlog::btree_xlog_startup::set(|_parent| {
            STARTUPS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        nbtxlog::btree_xlog_cleanup::set(|| CLEANUPS.with(|c| c.set(c.get() + 1)));
        ginxlog::gin_xlog_startup::set(|_parent| {
            STARTUPS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        ginxlog::gin_xlog_cleanup::set(|| CLEANUPS.with(|c| c.set(c.get() + 1)));
        gistxlog::gist_xlog_startup::set(|_parent| {
            STARTUPS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        gistxlog::gist_xlog_cleanup::set(|| CLEANUPS.with(|c| c.set(c.get() + 1)));
        spgxlog::spg_xlog_startup::set(|_parent| {
            STARTUPS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        spgxlog::spg_xlog_cleanup::set(|| CLEANUPS.with(|c| c.set(c.get() + 1)));

        // SRF plumbing for pg_get_wal_resource_managers.
        funcapi::InitMaterializedSRF::set(|fcinfo, _flags| {
            SRF_INITS.with(|c| c.set(c.get() + 1));
            fcinfo.resultinfo = Some(types_nodes::funcapi::ReturnSetInfo::default());
            Ok(())
        });
        funcapi::materialized_srf_putvalues::set(|_rsinfo, values, nulls| {
            // The SRF emits two by-value scalar columns (id, is_builtin) and one
            // by-reference `text` column (the rmgr name, built via the
            // `cstring_to_text_v` `ByRef` seam below). Record by-value words
            // verbatim and a by-reference column as its byte length, so the
            // assertions can compare against `Datum::from_usize(name.len())`.
            let words: Vec<types_datum::Datum> = values
                .iter()
                .map(|d| match d {
                    Datum::ByVal(w) => types_datum::Datum::from_usize(*w),
                    Datum::ByRef(b) => types_datum::Datum::from_usize(b.len()),
                })
                .collect();
            ROWS.with(|r| r.borrow_mut().push((words, nulls.to_vec())));
            Ok(())
        });
        // Stand-in `text` value: a `Datum::ByRef` whose byte length equals the
        // name length, so the SRF putvalues mock above records
        // `from_usize(name.len())` and the assertions still hold.
        // (`cstring_to_text_v` is the by-reference migration-target seam rmgr's
        // production code now calls.)
        varlena::cstring_to_text_v::set(|mcx, s| {
            let mut bytes = mcx::vec_with_capacity_in::<u8>(mcx, s.len())?;
            for _ in 0..s.len() {
                bytes.push(0u8);
            }
            Ok(types_tuple::backend_access_common_heaptuple::Datum::ByRef(bytes))
        });
    });
    IN_PRELOAD.with(|c| c.set(false));
    STARTUPS.with(|c| c.set(0));
    CLEANUPS.with(|c| c.set(0));
    SRF_INITS.with(|c| c.set(0));
    ROWS.with(|r| r.borrow_mut().clear());
    LOGS.with(|l| l.borrow_mut().clear());
}

/// rmgrlist.h, value by value: id, name, then which slots are non-NULL
/// (redo/desc/identify are always set for builtins).
/// Tuple: (id, name, has_startup, has_cleanup, has_mask, has_decode).
const RMGRLIST: [(u8, &str, bool, bool, bool, bool); 22] = [
    (0, "XLOG", false, false, false, true),
    (1, "Transaction", false, false, false, true),
    (2, "Storage", false, false, false, false),
    (3, "CLOG", false, false, false, false),
    (4, "Database", false, false, false, false),
    (5, "Tablespace", false, false, false, false),
    (6, "MultiXact", false, false, false, false),
    (7, "RelMap", false, false, false, false),
    (8, "Standby", false, false, false, true),
    (9, "Heap2", false, false, true, true),
    (10, "Heap", false, false, true, true),
    (11, "Btree", true, true, true, false),
    (12, "Hash", false, false, true, false),
    (13, "Gin", true, true, true, false),
    (14, "Gist", true, true, true, false),
    (15, "Sequence", false, false, true, false),
    (16, "SPGist", true, true, true, false),
    (17, "BRIN", false, false, true, false),
    (18, "CommitTs", false, false, false, false),
    (19, "ReplicationOrigin", false, false, false, false),
    (20, "Generic", false, false, true, false),
    (21, "LogicalMessage", false, false, false, true),
];

#[test]
fn builtin_table_matches_rmgrlist() {
    setup();
    for (id, name, has_startup, has_cleanup, has_mask, has_decode) in RMGRLIST {
        assert!(RmgrIdExists(id), "rmgr id {id} should exist");
        let row = GetRmgr(id).unwrap();
        assert_eq!(row.rm_name, Some(name));
        assert!(row.rm_redo.is_some(), "{name}: redo");
        assert!(row.rm_desc.is_some(), "{name}: desc");
        assert!(row.rm_identify.is_some(), "{name}: identify");
        assert_eq!(row.rm_startup.is_some(), has_startup, "{name}: startup");
        assert_eq!(row.rm_cleanup.is_some(), has_cleanup, "{name}: cleanup");
        assert_eq!(row.rm_mask.is_some(), has_mask, "{name}: mask");
        assert_eq!(row.rm_decode.is_some(), has_decode, "{name}: decode");
    }
    assert_eq!(RM_NEXT_ID, 22);
    assert_eq!(RM_MAX_BUILTIN_ID, 21);
}

#[test]
fn unregistered_ids_do_not_exist() {
    setup();
    // The gap 22..=127 and the custom range 128..=255 start empty.
    for id in [22u8, 100, 127, 128, 200, 255] {
        assert!(!RmgrIdExists(id), "rmgr id {id} should not exist");
    }
}

#[test]
fn rmgr_id_predicates() {
    setup();
    assert!(RmgrIdIsBuiltin(21));
    assert!(!RmgrIdIsBuiltin(22));
    assert!(RmgrIdIsCustom(128));
    assert!(RmgrIdIsCustom(255));
    assert!(!RmgrIdIsCustom(127));
    assert!(RmgrIdIsValid(0));
    assert!(RmgrIdIsValid(RM_EXPERIMENTAL_ID as i32));
    assert!(!RmgrIdIsValid(50));
}

#[test]
fn get_rmgr_unknown_id_errors() {
    setup();
    let err = GetRmgr(22).unwrap_err();
    assert!(err
        .message()
        .contains("resource manager with ID 22 not registered"));
    assert!(err
        .hint()
        .unwrap_or_default()
        .contains("shared_preload_libraries"));
}

#[test]
fn startup_and_cleanup_invoke_each_defined_callback_once() {
    setup();
    let root = mcx::MemoryContext::new("test");
    RmgrStartup(root.mcx()).unwrap();
    assert_eq!(STARTUPS.with(|c| c.get()), 4, "btree/gin/gist/spgist");
    RmgrCleanup();
    assert_eq!(CLEANUPS.with(|c| c.get()), 4, "btree/gin/gist/spgist");
}

#[test]
fn register_custom_rejects_empty_or_missing_name() {
    setup();
    IN_PRELOAD.with(|c| c.set(true));
    let mut rmgr = RmgrData::EMPTY;
    rmgr.rm_name = Some("");
    let err = RegisterCustomRmgr(130, &rmgr).unwrap_err();
    assert!(err.message().contains("custom resource manager name is invalid"));

    let err = RegisterCustomRmgr(130, &RmgrData::EMPTY).unwrap_err();
    assert!(err.message().contains("custom resource manager name is invalid"));
}

#[test]
fn register_custom_rejects_out_of_range_id() {
    setup();
    IN_PRELOAD.with(|c| c.set(true));
    let mut rmgr = RmgrData::EMPTY;
    rmgr.rm_name = Some("mycustom");
    let err = RegisterCustomRmgr(100, &rmgr).unwrap_err();
    assert!(err
        .message()
        .contains("custom resource manager ID 100 is out of range"));
    assert!(err
        .hint()
        .unwrap_or_default()
        .contains("between 128 and 255"));
}

#[test]
fn register_custom_requires_preload_window() {
    setup();
    let mut rmgr = RmgrData::EMPTY;
    rmgr.rm_name = Some("mycustom");
    let err = RegisterCustomRmgr(130, &rmgr).unwrap_err();
    assert!(err
        .message()
        .contains("failed to register custom resource manager \"mycustom\" with ID 130"));
    assert!(err
        .detail()
        .unwrap_or_default()
        .contains("shared_preload_libraries"));
}

#[test]
fn register_custom_rejects_duplicate_name_case_insensitively() {
    setup();
    IN_PRELOAD.with(|c| c.set(true));
    let mut rmgr = RmgrData::EMPTY;
    rmgr.rm_name = Some("heap");
    let err = RegisterCustomRmgr(131, &rmgr).unwrap_err();
    assert!(
        err.detail()
            .unwrap_or_default()
            .contains("Existing resource manager with ID 10 has the same name."),
        "detail was: {:?}",
        err.detail()
    );
}

#[test]
fn register_custom_happy_path_then_visible() {
    setup();
    IN_PRELOAD.with(|c| c.set(true));
    let id: RmgrId = 140;
    assert!(!RmgrIdExists(id));
    let mut rmgr = RmgrData::EMPTY;
    rmgr.rm_name = Some("MyExtensionRmgr");
    RegisterCustomRmgr(id, &rmgr).expect("registration should succeed");
    assert!(RmgrIdExists(id));
    assert_eq!(GetRmgr(id).unwrap().rm_name, Some("MyExtensionRmgr"));
    assert!(LOGS.with(|l| l
        .borrow()
        .iter()
        .any(|m| m.contains("registered custom resource manager \"MyExtensionRmgr\" with ID 140"))));

    // Re-registering the same id is rejected as already registered.
    let mut rmgr2 = RmgrData::EMPTY;
    rmgr2.rm_name = Some("Another");
    let err = RegisterCustomRmgr(id, &rmgr2).unwrap_err();
    assert!(err
        .detail()
        .unwrap_or_default()
        .contains("already registered with the same ID"));
}

#[test]
fn pg_get_wal_resource_managers_emits_one_row_per_existing_rmgr() {
    setup();
    let root = mcx::MemoryContext::new("test");
    let mut fcinfo = FunctionCallInfoBaseData::default();
    let ret =
        pg_get_wal_resource_managers(root.mcx(), &mut fcinfo).expect("SRF should succeed");
    assert_eq!(ret, Datum::null());
    assert!(fcinfo.resultinfo.is_some(), "InitMaterializedSRF ran");

    assert_eq!(SRF_INITS.with(|c| c.get()), 1);
    ROWS.with(|r| {
        let rows = r.borrow();
        // This thread's table holds exactly the 22 builtins (custom
        // registrations from other tests live in their own threads' tables).
        assert_eq!(rows.len(), 22, "one row per existing rmgr");

        let (values, nulls) = &rows[0];
        assert_eq!(values.len(), 3);
        assert_eq!(nulls, &vec![false, false, false]);
        // col0 = Int32GetDatum(0)
        assert_eq!(values[0], types_datum::Datum::from_i32(0));
        // col1 = CStringGetTextDatum("XLOG") -> stub encodes len("XLOG") = 4
        assert_eq!(values[1], types_datum::Datum::from_usize(4));
        // col2 = BoolGetDatum(RmgrIdIsBuiltin(0)) = true
        assert_eq!(values[2], types_datum::Datum::from_bool(true));

        // Last row is LogicalMessage (id 21), still builtin.
        let (values, _) = &rows[21];
        assert_eq!(values[0], types_datum::Datum::from_i32(21));
        assert_eq!(values[1], types_datum::Datum::from_usize("LogicalMessage".len()));
        assert_eq!(values[2], types_datum::Datum::from_bool(true));
    });
}
