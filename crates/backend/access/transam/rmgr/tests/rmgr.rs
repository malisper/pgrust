use rmgr::*;

#[test]
fn ids_match_rmgrlist_order() {
    assert_eq!(RM_XLOG_ID as u8, 0);
    assert_eq!(RM_XACT_ID as u8, 1);
    assert_eq!(RM_SMGR_ID as u8, 2);
    assert_eq!(RM_CLOG_ID as u8, 3);
    assert_eq!(RM_DBASE_ID as u8, 4);
    assert_eq!(RM_TBLSPC_ID as u8, 5);
    assert_eq!(RM_MULTIXACT_ID as u8, 6);
    assert_eq!(RM_RELMAP_ID as u8, 7);
    assert_eq!(RM_STANDBY_ID as u8, 8);
    assert_eq!(RM_HEAP2_ID as u8, 9);
    assert_eq!(RM_HEAP_ID as u8, 10);
    assert_eq!(RM_BTREE_ID as u8, 11);
    assert_eq!(RM_HASH_ID as u8, 12);
    assert_eq!(RM_GIN_ID as u8, 13);
    assert_eq!(RM_GIST_ID as u8, 14);
    assert_eq!(RM_SEQ_ID as u8, 15);
    assert_eq!(RM_SPGIST_ID as u8, 16);
    assert_eq!(RM_BRIN_ID as u8, 17);
    assert_eq!(RM_COMMIT_TS_ID as u8, 18);
    assert_eq!(RM_REPLORIGIN_ID as u8, 19);
    assert_eq!(RM_GENERIC_ID as u8, 20);
    assert_eq!(RM_LOGICALMSG_ID as u8, 21);
    assert_eq!(RM_NEXT_ID as u8, 22);
}

#[test]
fn constants_match_rmgr_h() {
    assert_eq!(RM_MAX_ID, 255);
    assert_eq!(RM_MAX_BUILTIN_ID, 21);
    assert_eq!(RM_MIN_CUSTOM_ID, 128);
    assert_eq!(RM_MAX_CUSTOM_ID, 255);
    assert_eq!(RM_N_IDS, 256);
    assert_eq!(RM_N_BUILTIN_IDS, 22);
    assert_eq!(RM_N_CUSTOM_IDS, 128);
    assert_eq!(RM_EXPERIMENTAL_ID, 128);
}

#[test]
fn id_predicates() {
    assert!(RmgrIdIsBuiltin(0));
    assert!(RmgrIdIsBuiltin(21));
    assert!(!RmgrIdIsCustom(21));
    assert!(!RmgrIdIsBuiltin(128));
    assert!(RmgrIdIsCustom(128));
    assert!(RmgrIdIsCustom(255));
    assert!(!RmgrIdIsCustom(256));
    assert!(RmgrIdIsValid(0) && RmgrIdIsValid(255));
    assert!(!RmgrIdIsValid(22) || RmgrIdIsBuiltin(22));
}

#[test]
fn table_names_match_rmgrlist() {
    let names: Vec<&str> = RmgrTable.iter().map(|r| r.rm_name).collect();
    assert_eq!(
        names,
        [
            "XLOG",
            "Transaction",
            "Storage",
            "CLOG",
            "Database",
            "Tablespace",
            "MultiXact",
            "RelMap",
            "Standby",
            "Heap2",
            "Heap",
            "Btree",
            "Hash",
            "Gin",
            "Gist",
            "Sequence",
            "SPGist",
            "BRIN",
            "CommitTs",
            "ReplicationOrigin",
            "Generic",
            "LogicalMessage",
        ]
    );
}

#[test]
fn startup_cleanup_mask_pattern_matches_rmgrlist() {
    for (i, row) in RmgrTable.iter().enumerate() {
        // C rows 11|13|14|16 (btree/gin/gist/spgist) carry rm_startup/
        // rm_cleanup; here those slots are None until their xlog units land
        // (the startup fns only allocate scratch their redo callbacks read,
        // and those redo callbacks are loud panics).
        let has_mask = matches!(i, 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 20);
        assert!(row.rm_startup.is_none(), "row {i} startup");
        assert!(row.rm_cleanup.is_none(), "row {i} cleanup");
        assert_eq!(row.rm_mask.is_some(), has_mask, "row {i} mask");
    }
}

#[test]
fn get_rmgr_builtin_and_exists() {
    assert!(RmgrIdExists(RM_HEAP_ID as u8));
    assert!(!RmgrIdExists(22));
    assert!(!RmgrIdExists(128));
    assert_eq!(GetRmgr(RM_HEAP_ID as u8).unwrap().rm_name, "Heap");
}

#[test]
fn get_rmgr_unregistered_errors_like_rmgr_not_found() {
    let err = match GetRmgr(131) {
        Err(err) => err,
        Ok(_) => panic!("expected RmgrNotFound error"),
    };
    assert_eq!(err.message, "resource manager with ID 131 not registered");
    assert!(err.hint.as_deref().unwrap().contains("shared_preload_libraries"));
}

#[test]
// replorigin redo is PORTED (t25 car-10, recovery-t24-merge): the rmgr row
// routes through origin_seams::replorigin_redo, installed at boot by the
// origin crate. In-process (seam uninstalled) the call must STILL fail loud
// — the property this test pins is "no silent replay of an unwired rmgr",
// now guaranteed by the seam default.
#[should_panic(expected = "seam not installed: origin_seams::replorigin_redo")]
fn unported_redo_panics_loudly() {
    let mut record = xlogreader_seams::XLogReaderState::default();
    let _ = (GetRmgr(RM_REPLORIGIN_ID as u8).unwrap().rm_redo)(&mut record);
}

#[test]
fn btree_redo_unknown_opcode_errors_loudly() {
    let mut rec = xlogreader_seams::DecodedXLogRecord::default();
    rec.xl_info = 0xF0;
    let mut record = xlogreader_seams::XLogReaderState {
        record: Some(rec),
        ..Default::default()
    };
    let err = (GetRmgr(RM_BTREE_ID as u8).unwrap().rm_redo)(&mut record)
        .expect_err("unknown btree opcode must not redo silently");
    assert!(err.message.contains("btree_redo: unknown op code"));
}
