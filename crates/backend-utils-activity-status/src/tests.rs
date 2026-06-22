use super::*;

#[test]
fn guc_defaults() {
    // C boot defaults: track_activities `boot_val` is `true` (guc_tables.c),
    // query size 1024. The thread_local is seeded with the boot value because the
    // accessor is installed after InitializeGUCOptions (see the seed comment on
    // PGSTAT_TRACK_ACTIVITIES). `guc_round_trip` may have left it false, so set
    // the boot value back before asserting.
    set_pgstat_track_activities(true);
    assert!(pgstat_track_activities());
    assert_eq!(pgstat_track_activity_query_size(), 1024);
}

#[test]
fn guc_round_trip() {
    set_pgstat_track_activities(true);
    assert!(pgstat_track_activities());
    set_pgstat_track_activities(false);

    set_pgstat_track_activity_query_size(2048);
    assert_eq!(pgstat_track_activity_query_size(), 2048);
    set_pgstat_track_activity_query_size(1024);
}

#[test]
fn clip_activity_truncates_to_query_size_minus_one() {
    set_pgstat_track_activity_query_size(8);
    // 16 bytes of ASCII -> clipped to 7 (qsize - 1).
    let raw = b"abcdefghijklmnop";
    let clipped = pgstat_clip_activity(raw);
    assert_eq!(clipped, b"abcdefg");
    set_pgstat_track_activity_query_size(1024);
}

#[test]
fn clip_activity_stops_at_embedded_nul() {
    set_pgstat_track_activity_query_size(1024);
    let raw = b"abc\0def";
    assert_eq!(pgstat_clip_activity(raw), b"abc");
}

#[test]
fn cmp_lbestatus_orders_by_proc_number() {
    assert!(cmp_lbestatus(2, 5) < 0);
    assert!(cmp_lbestatus(5, 2) > 0);
    assert_eq!(cmp_lbestatus(3, 3), 0);
}

#[test]
fn bsearch_finds_present_and_misses_absent() {
    let mk = |pn: ProcNumber| LocalPgBackendStatus {
        backend_status: LocalBackendStatusFields {
            st_procpid: pn,
            st_backend_type: BackendType::Backend,
            st_proc_start_timestamp: 0,
            st_xact_start_timestamp: 0,
            st_activity_start_timestamp: 0,
            st_state_start_timestamp: 0,
            st_databaseid: 0,
            st_userid: 0,
            st_clientaddr: SockAddr::zeroed(),
            st_clienthostname: Vec::new(),
            st_ssl: false,
            st_sslstatus: None,
            st_gss: false,
            st_state: STATE_UNDEFINED,
            st_appname: Vec::new(),
            st_activity_raw: Vec::new(),
            st_progress_command: ProgressCommandType::Invalid,
            st_progress_command_target: 0,
            st_progress_param: [0; PGSTAT_NUM_PROGRESS_PARAM],
            st_query_id: 0,
            st_plan_id: 0,
        },
        proc_number: pn,
        backend_xid: 0,
        backend_xmin: 0,
        backend_subxact_count: 0,
        backend_subxact_overflowed: false,
    };
    let table = vec![mk(1), mk(3), mk(5), mk(7)];
    assert_eq!(bsearch_proc_number(5, &table).map(|e| e.proc_number), Some(5));
    assert_eq!(bsearch_proc_number(1, &table).map(|e| e.proc_number), Some(1));
    assert_eq!(bsearch_proc_number(7, &table).map(|e| e.proc_number), Some(7));
    assert!(bsearch_proc_number(4, &table).is_none());
    assert!(bsearch_proc_number(8, &table).is_none());
}
