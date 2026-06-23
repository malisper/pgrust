//! Unit tests for the libpqwalreceiver port.
//!
//! These exercise the *owned logic* this module genuinely implements: the
//! conninfo key/val assembly + password gating, the command builders, the
//! result-status dispatch, the COPY byte framing, and the [`WalRcvExecResult`]
//! population.  The genuine externals (the libpq client library +
//! tuplestore/tupledesc/memctx machinery) are installed as scripted in-test
//! mock seams; the mock records the command strings the builders produced and
//! replays canned `PGresult`s.

use std::sync::{Mutex, Once};

use super::*;
use fe_seams as s;
use ::types_libpqwalreceiver::{ConnStatusType, ConninfoOption, ExecStatusType};

// ---------------------------------------------------------------------------
// Mock libpq provider state, shared by the installed seam fn pointers.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct MockResult {
    status: Option<ExecStatusType>,
    nfields: i32,
    ntuples: i32,
    values: Vec<Vec<Option<Vec<u8>>>>,
    fnames: Vec<Option<String>>,
    sqlstate: Option<String>,
}

struct Mock {
    exec_log: Vec<String>,
    exec_results: Vec<MockResult>,
    get_results: Vec<MockResult>,
    sent: Vec<Vec<u8>>,
    server_version: i32,
    status: ConnStatusType,
    used_password: bool,
    error_message: String,
    conninfo_parse: Result<Vec<ConninfoOption>, Option<String>>,
    conninfo: Option<Vec<ConninfoOption>>,
    database_id: Oid,
    results: Vec<MockResult>,
    copy_data: Vec<(i32, Vec<u8>)>,
    copy_data_idx: usize,
    consume_input_ret: i32,
    socket: i32,
    tuples_built: usize,
    tuples_put: usize,
}

static MOCK: Mutex<Option<Mock>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());

fn with_mock<R>(f: impl FnOnce(&mut Mock) -> R) -> R {
    let mut g = MOCK.lock().unwrap_or_else(|e| e.into_inner());
    f(g.as_mut().expect("mock not initialized"))
}

fn register_result(m: &mut Mock, r: MockResult) -> crate::PgResultId {
    m.results.push(r);
    m.results.len()
}

fn get_result(m: &Mock, id: crate::PgResultId) -> &MockResult {
    &m.results[id - 1]
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        s::libpqsrv_connect_params::set(|_keys, _vals, _expand, _we| 1 /* conn handle */);
        s::libpqsrv_exec::set(|_conn, query, _we| {
            with_mock(|m| {
                m.exec_log.push(query);
                let r = if m.exec_results.is_empty() {
                    MockResult {
                        status: Some(ExecStatusType::PGRES_COMMAND_OK),
                        ..Default::default()
                    }
                } else {
                    m.exec_results.remove(0)
                };
                register_result(m, r)
            })
        });
        s::libpqsrv_get_result::set(|_conn, _we| {
            with_mock(|m| {
                if m.get_results.is_empty() {
                    return 0;
                }
                let r = m.get_results.remove(0);
                register_result(m, r)
            })
        });
        s::libpqsrv_disconnect::set(|_conn| {});

        s::pq_status::set(|_conn| with_mock(|m| m.status));
        s::pq_connection_used_password::set(|_conn| with_mock(|m| m.used_password));
        s::pq_error_message::set(|_conn| with_mock(|m| m.error_message.clone()));
        s::pq_result_status::set(|res| with_mock(|m| get_result(m, res).status.expect("status set")));
        s::pq_result_error_field_sqlstate::set(|res| with_mock(|m| get_result(m, res).sqlstate.clone()));
        s::pq_clear::set(|_res| {});
        s::pq_nfields::set(|res| with_mock(|m| get_result(m, res).nfields));
        s::pq_ntuples::set(|res| with_mock(|m| get_result(m, res).ntuples));
        s::pq_fname::set(|res, n| with_mock(|m| get_result(m, res).fnames.get(n as usize).cloned().flatten()));
        s::pq_getvalue::set(|res, t, f| {
            with_mock(|m| {
                get_result(m, res).values[t as usize][f as usize]
                    .clone()
                    .unwrap_or_default()
            })
        });
        s::pq_getisnull::set(|res, t, f| {
            with_mock(|m| get_result(m, res).values[t as usize][f as usize].is_none())
        });
        s::pq_getlength::set(|res, t, f| {
            with_mock(|m| {
                get_result(m, res).values[t as usize][f as usize]
                    .as_ref()
                    .map(|v| v.len() as i32)
                    .unwrap_or(0)
            })
        });
        s::pq_get_copy_data::set(|_conn| {
            with_mock(|m| {
                let i = m.copy_data_idx;
                m.copy_data_idx += 1;
                m.copy_data.get(i).cloned().unwrap_or((0, Vec::new()))
            })
        });
        s::pq_put_copy_data::set(|_conn, buf| {
            with_mock(|m| {
                let n = buf.len() as i32;
                m.sent.push(buf);
                if n == 0 {
                    1
                } else {
                    n
                }
            })
        });
        s::pq_put_copy_end::set(|_conn| 1);
        s::pq_flush::set(|_conn| 0);
        s::pq_consume_input::set(|_conn| with_mock(|m| m.consume_input_ret));
        s::pq_socket::set(|_conn| with_mock(|m| m.socket));
        s::pq_endcopy::set(|_conn| 0);
        s::pq_host::set(|_conn| Some("primary.example".to_string()));
        s::pq_port::set(|_conn| Some("5432".to_string()));
        s::pq_server_version::set(|_conn| with_mock(|m| m.server_version));
        s::pq_backend_pid::set(|_conn| 4242);
        s::pq_conninfo::set(|_conn| with_mock(|m| m.conninfo.clone()));
        s::pq_conninfo_parse::set(|_ci| with_mock(|m| m.conninfo_parse.clone()));
        s::pq_escape_literal::set(|_conn, sv| Some(format!("'{sv}'")));
        s::pq_escape_identifier::set(|_conn, sv| Some(format!("\"{sv}\"")));

        s::get_database_encoding_name::set(|| "UTF8".to_string());
        s::quote_identifier::set(|i| format!("\"{i}\""));
        s::pg_strtoint32::set(|sv| Ok(sv.trim().parse::<i32>().unwrap_or(0)));
        s::pg_lsn_in::set(|v| {
            let st = String::from_utf8_lossy(&v).into_owned();
            let mut parts = st.split('/');
            let hi = u64::from_str_radix(parts.next().unwrap_or("0"), 16).unwrap_or(0);
            let lo = u64::from_str_radix(parts.next().unwrap_or("0"), 16).unwrap_or(0);
            Ok((hi << 32) | lo)
        });
        s::my_database_id::set(|| with_mock(|m| m.database_id));
        s::work_mem::set(|| 4096);
        s::check_for_interrupts::set(|| Ok(()));

        s::tuplestore_begin_heap::set(|_ra, _ix, _kb| 7);
        s::create_template_tuple_desc::set(|_n| 9);
        s::tuple_desc_init_entry::set(|_d, _a, _n, _o, _t, _ad| {});
        s::tuple_desc_get_att_in_metadata::set(|_d| 11);
        s::build_tuple_from_c_strings::set(|_a, vals| {
            with_mock(|m| {
                m.tuples_built += vals.len();
            });
            13
        });
        s::tuplestore_puttuple::set(|_st, _t| {
            with_mock(|m| m.tuples_put += 1);
        });
        s::alloc_set_context_create_default::set(|_n| 15);
        s::memory_context_switch_to::set(|_c| 0);
        s::memory_context_reset::set(|_c| {});
        s::memory_context_delete::set(|_c| {});
    });
}

impl Mock {
    fn fresh() -> Mock {
        Mock {
            exec_log: Vec::new(),
            exec_results: Vec::new(),
            get_results: Vec::new(),
            sent: Vec::new(),
            server_version: 170000,
            status: ConnStatusType::CONNECTION_OK,
            used_password: false,
            error_message: String::new(),
            conninfo_parse: Ok(Vec::new()),
            conninfo: None,
            database_id: 0,
            results: Vec::new(),
            copy_data: Vec::new(),
            copy_data_idx: 0,
            consume_input_ret: 1,
            socket: 33,
            tuples_built: 0,
            tuples_put: 0,
        }
    }
}

#[must_use]
fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    install_seams();
    let mut g = MOCK.lock().unwrap_or_else(|e| e.into_inner());
    *g = Some(Mock::fresh());
    guard
}

fn ok_conn(logical: bool) -> WalReceiverConn {
    WalReceiverConn {
        streamConn: 1,
        logical,
        recvBuf: Vec::new(),
    }
}

fn conn_opt(keyword: &str, val: Option<&str>, dispchar: &str) -> ConninfoOption {
    ConninfoOption {
        keyword: keyword.to_string(),
        val: val.map(|v| v.to_string()),
        dispchar: dispchar.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Pure-logic tests (no seam needed).
// ---------------------------------------------------------------------------

#[test]
fn pchomp_strips_trailing_newlines() {
    assert_eq!(pchomp("hello\n"), "hello");
    assert_eq!(pchomp("hello\n\n"), "hello");
    assert_eq!(pchomp("hello"), "hello");
    assert_eq!(pchomp("\n"), "");
    assert_eq!(pchomp("a\nb\n"), "a\nb");
}

#[test]
fn atoi_matches_c_semantics() {
    assert_eq!(atoi("5432"), 5432);
    assert_eq!(atoi("  42abc"), 42);
    assert_eq!(atoi("-7"), -7);
    assert_eq!(atoi("+9"), 9);
    assert_eq!(atoi("xyz"), 0);
    assert_eq!(atoi(""), 0);
}

#[test]
fn lsn_format_is_uppercase_hex_halves() {
    assert_eq!(lsn_format(0), "0/0");
    assert_eq!(lsn_format(0x0000_0001_ABCD_EF00), "1/ABCDEF00");
    assert_eq!(lsn_format(0xDEAD_BEEF_0000_0010), "DEADBEEF/10");
}

// ---------------------------------------------------------------------------
// conninfo handling.
// ---------------------------------------------------------------------------

#[test]
fn check_conninfo_requires_password_when_demanded() {
    let _serial = setup();
    with_mock(|m| {
        m.conninfo_parse = Ok(vec![conn_opt("password", Some("secret"), "*")]);
    });
    assert!(libpqrcv_check_conninfo("host=p password=secret", true).is_ok());

    with_mock(|m| {
        m.conninfo_parse = Ok(vec![conn_opt("password", Some(""), "*")]);
    });
    assert!(libpqrcv_check_conninfo("host=p", true).is_err());
}

#[test]
fn check_conninfo_parse_failure_is_syntax_error() {
    let _serial = setup();
    with_mock(|m| {
        m.conninfo_parse = Err(Some("bad =".to_string()));
    });
    let e = libpqrcv_check_conninfo("garbage", false).unwrap_err();
    assert_eq!(e.sqlstate, ERRCODE_SYNTAX_ERROR);
}

#[test]
fn get_option_returns_last_matching_value() {
    let _serial = setup();
    with_mock(|m| {
        m.conninfo_parse = Ok(vec![
            conn_opt("dbname", Some("first"), ""),
            conn_opt("dbname", Some("last"), ""),
            conn_opt("host", Some("h"), ""),
        ]);
    });
    assert_eq!(
        libpqrcv_get_option_from_conninfo("x", "dbname").unwrap(),
        Some("last".to_string())
    );
    assert_eq!(
        libpqrcv_get_dbname_from_conninfo("x").unwrap(),
        Some("last".to_string())
    );
    assert_eq!(
        libpqrcv_get_option_from_conninfo("x", "missing").unwrap(),
        None
    );
}

#[test]
fn get_conninfo_obfuscates_secret_and_skips_debug_empty() {
    let _serial = setup();
    with_mock(|m| {
        m.conninfo = Some(vec![
            conn_opt("host", Some("p"), ""),
            conn_opt("password", Some("hunter2"), "*"),
            conn_opt("debugopt", Some("x"), "D"),
            conn_opt("empty", Some(""), ""),
            conn_opt("unset", None, ""),
        ]);
    });
    let st = libpqrcv_get_conninfo(&ok_conn(false)).unwrap().unwrap();
    assert_eq!(st, "host=p password=********");
}

// ---------------------------------------------------------------------------
// connect.
// ---------------------------------------------------------------------------

#[test]
fn connect_logical_sets_encoding_and_options() {
    let _serial = setup();
    with_mock(|m| {
        m.conninfo_parse = Ok(Vec::new());
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 1,
            ntuples: 1,
            values: vec![vec![Some(b"".to_vec())]],
            ..Default::default()
        });
    });
    let r = libpqrcv_connect("host=p", true, true, false, Some("sub")).unwrap();
    let conn = r.conn.expect("connected");
    assert!(conn.logical);
    with_mock(|m| {
        assert!(m
            .exec_log
            .iter()
            .any(|c| c.contains("set_config('search_path'")));
    });
}

#[test]
fn connect_bad_status_returns_err_string() {
    let _serial = setup();
    with_mock(|m| {
        m.status = ConnStatusType::CONNECTION_BAD;
        m.error_message = "connection refused\n".to_string();
    });
    let r = libpqrcv_connect("host=p", false, false, false, Some("walreceiver")).unwrap();
    assert!(r.conn.is_none());
    assert_eq!(r.err.as_deref(), Some("connection refused"));
}

#[test]
fn connect_must_use_password_without_password_errors() {
    let _serial = setup();
    with_mock(|m| {
        m.status = ConnStatusType::CONNECTION_OK;
        m.used_password = false;
    });
    let e = libpqrcv_connect("host=p", false, false, true, Some("w")).unwrap_err();
    assert_eq!(e.sqlstate, ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED);
}

// ---------------------------------------------------------------------------
// command builders.
// ---------------------------------------------------------------------------

#[test]
fn startstreaming_physical_command() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COPY_BOTH),
            ..Default::default()
        });
    });
    let options = WalRcvStreamOptions {
        logical: false,
        slotname: None,
        startpoint: 0x1_0000_0028,
        proto: WalRcvStreamOptionsProto::Physical(WalRcvStreamOptionsPhysical { startpointTLI: 3 }),
    };
    let switched = libpqrcv_startstreaming(&ok_conn(false), &options).unwrap();
    assert!(switched);
    with_mock(|m| assert_eq!(m.exec_log[0], "START_REPLICATION 1/28 TIMELINE 3"));
}

#[test]
fn startstreaming_logical_command() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COPY_BOTH),
            ..Default::default()
        });
    });
    let options = WalRcvStreamOptions {
        logical: true,
        slotname: Some("sub_slot".to_string()),
        startpoint: 0x28,
        proto: WalRcvStreamOptionsProto::Logical(WalRcvStreamOptionsLogical {
            proto_version: 4,
            publication_names: vec!["pub1".to_string(), "pub2".to_string()],
            binary: true,
            streaming_str: Some("parallel".to_string()),
            twophase: true,
            origin: Some("none".to_string()),
        }),
    };
    libpqrcv_startstreaming(&ok_conn(true), &options).unwrap();
    with_mock(|m| {
        assert_eq!(
            m.exec_log[0],
            "START_REPLICATION SLOT \"sub_slot\" LOGICAL 0/28 (proto_version '4', streaming 'parallel', two_phase 'on', origin 'none', publication_names '\"pub1\",\"pub2\"', binary 'true')"
        );
    });
}

#[test]
fn startstreaming_command_ok_means_not_switched() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COMMAND_OK),
            ..Default::default()
        });
    });
    let options = WalRcvStreamOptions {
        logical: false,
        slotname: None,
        startpoint: 0,
        proto: WalRcvStreamOptionsProto::Physical(WalRcvStreamOptionsPhysical { startpointTLI: 1 }),
    };
    assert!(!libpqrcv_startstreaming(&ok_conn(false), &options).unwrap());
}

#[test]
fn create_slot_logical_new_syntax() {
    let _serial = setup();
    with_mock(|m| {
        m.server_version = 170000;
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 4,
            ntuples: 1,
            values: vec![vec![
                Some(b"sub_slot".to_vec()),
                Some(b"0/16B3748".to_vec()),
                Some(b"snap_0001".to_vec()),
                None,
            ]],
            ..Default::default()
        });
    });
    let (snapshot, lsn) = libpqrcv_create_slot(
        &ok_conn(true),
        "sub_slot",
        true,
        true,
        true,
        CRSSnapshotAction::CRS_EXPORT_SNAPSHOT,
        true,
    )
    .unwrap();
    assert_eq!(snapshot.as_deref(), Some("snap_0001"));
    assert_eq!(lsn, Some(0x16B3748));
    with_mock(|m| {
        assert_eq!(
            m.exec_log[0],
            "CREATE_REPLICATION_SLOT \"sub_slot\" TEMPORARY LOGICAL pgoutput (TWO_PHASE, FAILOVER, SNAPSHOT 'export')"
        );
    });
}

#[test]
fn create_slot_physical_old_syntax() {
    let _serial = setup();
    with_mock(|m| {
        m.server_version = 140000;
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 4,
            ntuples: 1,
            values: vec![vec![Some(b"s".to_vec()), Some(b"0/0".to_vec()), None, None]],
            ..Default::default()
        });
    });
    let (snapshot, lsn) = libpqrcv_create_slot(
        &ok_conn(false),
        "phys",
        false,
        false,
        false,
        CRSSnapshotAction::CRS_NOEXPORT_SNAPSHOT,
        false,
    )
    .unwrap();
    assert_eq!(snapshot, None);
    assert_eq!(lsn, None);
    with_mock(|m| assert_eq!(m.exec_log[0], "CREATE_REPLICATION_SLOT \"phys\" PHYSICAL RESERVE_WAL"));
}

#[test]
fn alter_slot_builds_both_options() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COMMAND_OK),
            ..Default::default()
        });
    });
    libpqrcv_alter_slot(&ok_conn(true), "s", Some(true), Some(false)).unwrap();
    with_mock(|m| {
        assert_eq!(
            m.exec_log[0],
            "ALTER_REPLICATION_SLOT \"s\" ( FAILOVER true, TWO_PHASE false );"
        );
    });
}

#[test]
fn alter_slot_only_failover() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COMMAND_OK),
            ..Default::default()
        });
    });
    libpqrcv_alter_slot(&ok_conn(true), "s", Some(false), None).unwrap();
    with_mock(|m| {
        assert_eq!(m.exec_log[0], "ALTER_REPLICATION_SLOT \"s\" ( FAILOVER false );");
    });
}

// ---------------------------------------------------------------------------
// identify_system / timeline history / server version / pid / senderinfo.
// ---------------------------------------------------------------------------

#[test]
fn identify_system_parses_sysid_and_tli() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 4,
            ntuples: 1,
            values: vec![vec![
                Some(b"6312345678901234567".to_vec()),
                Some(b"7".to_vec()),
                Some(b"0/0".to_vec()),
                None,
            ]],
            ..Default::default()
        });
    });
    let (sysid, tli) = libpqrcv_identify_system(&ok_conn(false)).unwrap();
    assert_eq!(sysid, "6312345678901234567");
    assert_eq!(tli, 7);
}

#[test]
fn identify_system_protocol_violation_on_few_fields() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 2,
            ntuples: 1,
            ..Default::default()
        });
    });
    let e = libpqrcv_identify_system(&ok_conn(false)).unwrap_err();
    assert_eq!(e.sqlstate, ERRCODE_PROTOCOL_VIOLATION);
}

#[test]
fn readtimelinehistoryfile_returns_name_and_content() {
    let _serial = setup();
    with_mock(|m| {
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 2,
            ntuples: 1,
            values: vec![vec![
                Some(b"00000007.history".to_vec()),
                Some(b"1\t0/30\tno recovery target\n".to_vec()),
            ]],
            ..Default::default()
        });
    });
    let (fname, content) = libpqrcv_readtimelinehistoryfile(&ok_conn(false), 7).unwrap();
    assert_eq!(fname, "00000007.history");
    assert_eq!(content, b"1\t0/30\tno recovery target\n");
    with_mock(|m| assert_eq!(m.exec_log[0], "TIMELINE_HISTORY 7"));
}

#[test]
fn server_version_and_backend_pid_passthrough() {
    let _serial = setup();
    with_mock(|m| m.server_version = 180003);
    assert_eq!(libpqrcv_server_version(&ok_conn(false)), 180003);
    assert_eq!(libpqrcv_get_backend_pid(&ok_conn(false)), 4242);
}

#[test]
fn senderinfo_reads_host_and_port() {
    let _serial = setup();
    let (host, port) = libpqrcv_get_senderinfo(&ok_conn(false));
    assert_eq!(host.as_deref(), Some("primary.example"));
    assert_eq!(port, 5432);
}

// ---------------------------------------------------------------------------
// endstreaming / receive / send.
// ---------------------------------------------------------------------------

#[test]
fn endstreaming_reads_next_tli() {
    let _serial = setup();
    with_mock(|m| {
        m.get_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 2,
            ntuples: 1,
            values: vec![vec![Some(b"8".to_vec()), Some(b"0/50".to_vec())]],
            ..Default::default()
        });
        m.get_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COMMAND_OK),
            ..Default::default()
        });
    });
    let next_tli = libpqrcv_endstreaming(&ok_conn(false)).unwrap();
    assert_eq!(next_tli, 8);
}

#[test]
fn receive_returns_data_in_recvbuf() {
    let _serial = setup();
    with_mock(|m| {
        m.copy_data.push((5, b"hello".to_vec()));
    });
    let mut conn = ok_conn(false);
    let (len, _fd) = libpqrcv_receive(&mut conn).unwrap();
    assert_eq!(len, 5);
    assert_eq!(conn.recvBuf, b"hello");
}

#[test]
fn receive_no_data_yet_returns_wait_fd() {
    let _serial = setup();
    with_mock(|m| {
        m.copy_data.push((0, Vec::new()));
        m.copy_data.push((0, Vec::new()));
        m.socket = 33;
    });
    let mut conn = ok_conn(false);
    let (len, fd) = libpqrcv_receive(&mut conn).unwrap();
    assert_eq!(len, 0);
    assert_eq!(fd, 33);
}

#[test]
fn receive_end_of_copy_returns_minus_one() {
    let _serial = setup();
    with_mock(|m| {
        m.copy_data.push((-1, Vec::new()));
        m.get_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COMMAND_OK),
            ..Default::default()
        });
    });
    let mut conn = ok_conn(false);
    let (len, _fd) = libpqrcv_receive(&mut conn).unwrap();
    assert_eq!(len, -1);
}

#[test]
fn send_pushes_copy_data() {
    let _serial = setup();
    libpqrcv_send(&ok_conn(false), b"reply").unwrap();
    with_mock(|m| assert_eq!(m.sent[0], b"reply"));
}

// ---------------------------------------------------------------------------
// exec / processTuples.
// ---------------------------------------------------------------------------

#[test]
fn exec_requires_database_connection() {
    let _serial = setup();
    with_mock(|m| m.database_id = InvalidOid);
    let e = libpqrcv_exec(&ok_conn(true), "SELECT 1", 1, &[23]).unwrap_err();
    assert_eq!(e.sqlstate, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
}

#[test]
fn exec_tuples_ok_populates_tuplestore() {
    let _serial = setup();
    with_mock(|m| {
        m.database_id = 16384;
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 2,
            ntuples: 2,
            fnames: vec![Some("a".to_string()), Some("b".to_string())],
            values: vec![
                vec![Some(b"1".to_vec()), None],
                vec![Some(b"2".to_vec()), Some(b"x".to_vec())],
            ],
            ..Default::default()
        });
    });
    let res = libpqrcv_exec(&ok_conn(true), "SELECT a, b", 2, &[23, 25]).unwrap();
    assert_eq!(res.status, WalRcvExecStatus::WALRCV_OK_TUPLES);
    assert_ne!(res.tuplestore, 0);
    assert_ne!(res.tupledesc, 0);
    with_mock(|m| assert_eq!(m.tuples_put, 2));
}

#[test]
fn exec_field_count_mismatch_is_protocol_violation() {
    let _serial = setup();
    with_mock(|m| {
        m.database_id = 16384;
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_TUPLES_OK),
            nfields: 3,
            ntuples: 0,
            ..Default::default()
        });
    });
    let e = libpqrcv_exec(&ok_conn(true), "SELECT a", 2, &[23, 25]).unwrap_err();
    assert_eq!(e.sqlstate, ERRCODE_PROTOCOL_VIOLATION);
}

#[test]
fn exec_empty_query_is_walrcv_error() {
    let _serial = setup();
    with_mock(|m| {
        m.database_id = 16384;
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_EMPTY_QUERY),
            ..Default::default()
        });
    });
    let res = libpqrcv_exec(&ok_conn(true), "", 0, &[]).unwrap();
    assert_eq!(res.status, WalRcvExecStatus::WALRCV_ERROR);
    assert_eq!(res.err.as_deref(), Some("empty query"));
}

#[test]
fn exec_fatal_error_carries_sqlstate() {
    let _serial = setup();
    with_mock(|m| {
        m.database_id = 16384;
        m.error_message = "relation does not exist\n".to_string();
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_FATAL_ERROR),
            sqlstate: Some("42P01".to_string()),
            ..Default::default()
        });
    });
    let res = libpqrcv_exec(&ok_conn(true), "SELECT bad", 0, &[]).unwrap();
    assert_eq!(res.status, WalRcvExecStatus::WALRCV_ERROR);
    assert_eq!(res.err.as_deref(), Some("relation does not exist"));
    assert_eq!(res.sqlstate, make_sqlstate(*b"42P01").0);
}

#[test]
fn exec_copy_modes_map_to_statuses() {
    let _serial = setup();
    with_mock(|m| {
        m.database_id = 16384;
        m.exec_results.push(MockResult {
            status: Some(ExecStatusType::PGRES_COPY_OUT),
            ..Default::default()
        });
    });
    let res = libpqrcv_exec(&ok_conn(true), "COPY t TO STDOUT", 0, &[]).unwrap();
    assert_eq!(res.status, WalRcvExecStatus::WALRCV_OK_COPY_OUT);
}

// ---------------------------------------------------------------------------
// _PG_init.
// ---------------------------------------------------------------------------

#[test]
fn pg_init_errors_on_double_load() {
    let _ = _PG_init();
    let e = _PG_init().unwrap_err();
    assert!(e.message.contains("already loaded"));
}
