use super::*;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Once;

use ip::SockAddr;
use types_startup::Port;

thread_local! {
    static WIRE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static INPUT: RefCell<VecDeque<Vec<u8>>> = const { RefCell::new(VecDeque::new()) };
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        be_secure_seams::secure_write::set(|buf| {
            WIRE.with(|w| w.borrow_mut().extend_from_slice(buf));
            Ok(Ok(buf.len()))
        });
        be_secure_seams::secure_read::set(|buf| {
            INPUT.with(|q| {
                let mut q = q.borrow_mut();
                match q.front_mut() {
                    None => Ok(Ok(0)),
                    Some(chunk) => {
                        let n = chunk.len().min(buf.len());
                        buf[..n].copy_from_slice(&chunk[..n]);
                        chunk.drain(..n);
                        if chunk.is_empty() {
                            q.pop_front();
                        }
                        Ok(Ok(n))
                    }
                }
            })
        });
        be_secure_seams::set_port_noblock::set(|_| true);
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        string_seams::pg_clean_ascii::set(|s, _| Some(s.to_string()));
        scalar_seams::parse_bool::set(|v| match v {
            "true" | "on" | "1" => Some(true),
            "false" | "off" | "0" => Some(false),
            _ => None,
        });
        guc_seams::guc_check_errdetail::set(|_| {});
        init_small::init_seams();
        pqcomm::init_seams();
        crate::init_seams();
    });
}

fn setup() {
    install();
    pqcomm::pq_init_buffers().unwrap();
    WIRE.with(|w| w.borrow_mut().clear());
    INPUT.with(|q| q.borrow_mut().clear());
    init_small::globals::SetClientConnectionLost(false);
    init_small::globals::SetInterruptPending(false);
    init_small::globals::SetMyProcPort(Port::new(&ClientSocket {
        sock: -1,
        raddr: SockAddr::zeroed(),
    }));
}

fn feed(pkt: Vec<u8>) {
    INPUT.with(|q| q.borrow_mut().push_back(pkt));
}

fn packet(proto: u32, kvs: &[(&str, &str)]) -> Vec<u8> {
    let mut body = proto.to_be_bytes().to_vec();
    for (k, v) in kvs {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut pkt = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    pkt.extend_from_slice(&body);
    pkt
}

fn run_startup_packet() -> PgResult<i32> {
    let ctx = MemoryContext::new("test");
    process_startup_packet(ctx.mcx(), false, false)
}

#[test]
fn startup_packet_fills_port() {
    setup();
    feed(packet(
        pg_protocol(3, 0),
        &[
            ("user", "alice"),
            ("database", "db1"),
            ("options", "-c work_mem=8MB"),
            ("application_name", "psql"),
            ("search_path", "public"),
        ],
    ));
    assert_eq!(run_startup_packet().unwrap(), STATUS_OK);
    init_small::globals::WithMyProcPort(|p| {
        assert_eq!(p.proto, pg_protocol(3, 0));
        assert_eq!(p.user_name.as_deref(), Some("alice"));
        assert_eq!(p.database_name.as_deref(), Some("db1"));
        assert_eq!(p.cmdline_options.as_deref(), Some("-c work_mem=8MB"));
        assert_eq!(p.application_name.as_deref(), Some("psql"));
        assert_eq!(
            p.guc_options,
            vec![
                "application_name".to_string(),
                "psql".to_string(),
                "search_path".to_string(),
                "public".to_string()
            ]
        );
    });
    assert_eq!(miscinit::GetMyBackendType(), types_core::BackendType::Backend);
    assert_eq!(init_small::globals::FrontendProtocol(), pg_protocol(3, 0));
}

#[test]
fn database_defaults_to_user_and_truncates() {
    setup();
    let long = "u".repeat(80);
    feed(packet(pg_protocol(3, 0), &[("user", &long)]));
    assert_eq!(run_startup_packet().unwrap(), STATUS_OK);
    init_small::globals::WithMyProcPort(|p| {
        assert_eq!(p.user_name.as_deref().unwrap().len(), NAMEDATALEN - 1);
        assert_eq!(p.database_name, p.user_name);
    });
}

#[test]
fn ssl_negotiation_rejected_then_startup_proceeds() {
    setup();
    let mut ssl_request = 8u32.to_be_bytes().to_vec();
    ssl_request.extend_from_slice(&NEGOTIATE_SSL_CODE.to_be_bytes());
    feed(ssl_request);
    feed(packet(pg_protocol(3, 0), &[("user", "bob")]));
    assert_eq!(run_startup_packet().unwrap(), STATUS_OK);
    assert_eq!(WIRE.with(|w| w.borrow().clone()), vec![b'N']);
    init_small::globals::WithMyProcPort(|p| {
        assert_eq!(p.user_name.as_deref(), Some("bob"));
    });
}

#[test]
fn newer_minor_protocol_negotiates_and_succeeds() {
    setup();
    feed(packet(pg_protocol(3, 9), &[("user", "carol")]));
    assert_eq!(run_startup_packet().unwrap(), STATUS_OK);
    // FrontendProtocol clamps to the newest minor we speak.
    assert_eq!(init_small::globals::FrontendProtocol(), PG_PROTOCOL_LATEST);
    pqcomm::pq_flush().unwrap();
    let wire = WIRE.with(|w| w.borrow().clone());
    assert_eq!(wire[0], b'v');
}

#[test]
fn empty_startup_is_silent_error() {
    setup();
    assert_eq!(run_startup_packet().unwrap(), STATUS_ERROR);
}

#[test]
fn oversized_length_is_error() {
    setup();
    feed(20000u32.to_be_bytes().to_vec());
    assert_eq!(run_startup_packet().unwrap(), STATUS_ERROR);
}

#[test]
#[should_panic(expected = "proc_exit(1)")]
fn missing_user_is_fatal() {
    setup();
    feed(packet(pg_protocol(3, 0), &[("database", "db1")]));
    let _ = run_startup_packet();
}

#[test]
#[should_panic(expected = "proc_exit(1)")]
fn ancient_protocol_is_fatal() {
    setup();
    feed(packet(pg_protocol(2, 0), &[]));
    let _ = run_startup_packet();
}

#[test]
fn cancel_request_length_validation() {
    setup();
    // Too short for the header, then an oversized key: both COMMERROR, Ok(()).
    process_cancel_request_packet(&[0u8; 4], 4).unwrap();
    process_cancel_request_packet(&[0u8; 300], 300).unwrap();
}

#[test]
fn direct_ssl_firstbyte_rejected() {
    setup();
    feed(vec![0x16, 0x03, 0x01]);
    assert_eq!(process_ssl_startup().unwrap(), STATUS_ERROR);
}

#[test]
fn validate_log_connections_options_matrix() {
    assert_eq!(validate_log_connections_options(&[]), Ok(0));
    assert_eq!(
        validate_log_connections_options(&["on".into()]),
        Ok(LOG_CONNECTION_ON)
    );
    assert_eq!(validate_log_connections_options(&["0".into()]), Ok(0));
    assert_eq!(
        validate_log_connections_options(&["receipt".into(), "setup_durations".into()]),
        Ok(LOG_CONNECTION_RECEIPT | LOG_CONNECTION_SETUP_DURATIONS)
    );
    assert_eq!(
        validate_log_connections_options(&["ALL".into()]),
        Ok(LOG_CONNECTION_ALL)
    );
    assert!(validate_log_connections_options(&["on".into(), "receipt".into()])
        .unwrap_err()
        .contains("in a list with other options"));
    assert!(validate_log_connections_options(&["bogus".into()])
        .unwrap_err()
        .contains("Invalid option"));
}

#[test]
fn check_log_connections_parses_lists() {
    setup();
    let ctx = MemoryContext::new("test");
    assert_eq!(check_log_connections(ctx.mcx(), "").unwrap(), Ok(0));
    assert_eq!(
        check_log_connections(ctx.mcx(), "receipt, authorization").unwrap(),
        Ok(LOG_CONNECTION_RECEIPT | LOG_CONNECTION_AUTHORIZATION)
    );
    assert!(check_log_connections(ctx.mcx(), "\"unterminated")
        .unwrap()
        .unwrap_err()
        .contains("Invalid list syntax"));
}

#[test]
fn conn_timing_init_matches_c() {
    assert_eq!(
        conn_timing::get().ready_for_use,
        types_startup::TIMESTAMP_MINUS_INFINITY
    );
    conn_timing::set_auth_start(5);
    assert_eq!(conn_timing::get().auth_start, 5);
}
