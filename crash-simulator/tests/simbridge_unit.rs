//! SIM-HARNESS-CONVERGE increment-1 unit coverage: the wire-transcript
//! parser, the sent-log alignment session, the script synthesizer's
//! refusals, and the NullBug mirror. Pure client-side (no sim binary).

use simharness::runner::driver::{ExecOutcome, Session};
use simharness::runner::planface::{FaultPoint, Plan, PlanHeader, Sql, SqlMeta, Step};
use simharness::runner::simbridge::{
    null_bug_rewrite, parse_transcript, synthesize_script, ReplaySession, RESET_STMTS,
};

// ---------------------------------------------------------------- frames

fn frame(ty: u8, body: &[u8]) -> Vec<u8> {
    let mut f = vec![ty];
    f.extend_from_slice(&(4 + body.len() as i32).to_be_bytes());
    f.extend_from_slice(body);
    f
}

fn zready() -> Vec<u8> {
    frame(b'Z', b"I")
}

fn startup_preamble() -> Vec<u8> {
    let mut t = Vec::new();
    t.extend(frame(b'R', &[0, 0, 0, 0]));
    t.extend(frame(b'S', b"client_encoding\0UTF8\0"));
    t.extend(frame(b'K', &[0u8; 8]));
    t.extend(zready());
    t
}

fn datarow(cols: &[Option<&str>]) -> Vec<u8> {
    let mut b = (cols.len() as u16).to_be_bytes().to_vec();
    for c in cols {
        match c {
            None => b.extend((-1i32).to_be_bytes()),
            Some(s) => {
                b.extend((s.len() as i32).to_be_bytes());
                b.extend(s.as_bytes());
            }
        }
    }
    frame(b'D', &b)
}

fn errresp(code: &str, msg: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(b'S');
    b.extend(b"ERROR\0");
    b.push(b'C');
    b.extend(code.as_bytes());
    b.push(0);
    b.push(b'M');
    b.extend(msg.as_bytes());
    b.push(0);
    b.push(0);
    frame(b'E', &b)
}

#[test]
fn transcript_rows_command_error_cycles() {
    let mut t = startup_preamble();
    // stmt 0: SELECT with one row, two cols (one NULL)
    t.extend(frame(b'T', &[0, 1]));
    t.extend(datarow(&[Some("42"), None]));
    t.extend(frame(b'C', b"SELECT 1\0"));
    t.extend(zready());
    // stmt 1: INSERT command tag
    t.extend(frame(b'C', b"INSERT 0 5\0"));
    t.extend(zready());
    // stmt 2: error (with the tx-status Z that follows)
    t.extend(errresp("42703", "no such column"));
    t.extend(frame(b'Z', b"E"));
    // stmt 3: empty SELECT — RowDescription, zero DataRows
    t.extend(frame(b'T', &[0, 1]));
    t.extend(frame(b'C', b"SELECT 0\0"));
    t.extend(zready());
    let p = parse_transcript(&t).expect("parse");
    assert_eq!(p.outcomes.len(), 4);
    assert_eq!(
        p.outcomes[0],
        ExecOutcome::Rows { rows: vec![vec![Some("42".into()), None]] }
    );
    // The postgres-crate mirror: tag carries the numeric affected count.
    assert_eq!(p.outcomes[1], ExecOutcome::Command { tag: "5".into() });
    assert_eq!(
        p.outcomes[2],
        ExecOutcome::SqlError { sqlstate: "42703".into(), message: "no such column".into() }
    );
    // An empty SELECT is Rows{[]}, never Command (the RowDescription law).
    assert_eq!(p.outcomes[3], ExecOutcome::Rows { rows: vec![] });
    assert!(p.trailing_error.is_none());
}

#[test]
fn transcript_trailing_fatal_is_surfaced() {
    let mut t = startup_preamble();
    t.extend(frame(b'C', b"CREATE TABLE\0"));
    t.extend(zready());
    // A FATAL error with no ReadyForQuery after it (connection died).
    t.extend(errresp("57P01", "terminating connection"));
    let p = parse_transcript(&t).expect("parse");
    assert_eq!(p.outcomes.len(), 1);
    let trailing = p.trailing_error.expect("trailing error");
    assert_eq!(
        trailing,
        ExecOutcome::SqlError { sqlstate: "57P01".into(), message: "terminating connection".into() }
    );
}

#[test]
fn transcript_truncated_frame_is_dropped_not_error() {
    let mut t = startup_preamble();
    t.extend(frame(b'C', b"BEGIN\0"));
    t.extend(zready());
    // A frame header cut mid-flush by the whole-node kill.
    t.extend([b'D', 0, 0, 0, 50, 1, 2]);
    let p = parse_transcript(&t).expect("parse");
    assert_eq!(p.outcomes.len(), 1);
}

// ---------------------------------------------------------------- replay

#[test]
fn replay_alignment_and_desync() {
    let entries = vec![
        ("SELECT 1".to_string(), ExecOutcome::Rows { rows: vec![vec![Some("1".into())]] }),
        ("ROLLBACK".to_string(), ExecOutcome::Command { tag: "0".into() }),
    ];
    let mut r = ReplaySession::new(entries.clone());
    assert!(matches!(r.execute("SELECT 1"), ExecOutcome::Rows { .. }));
    // Mismatch = loud desync, never a silent realign.
    let out = r.execute("SELECT 2");
    assert!(matches!(out, ExecOutcome::ConnectionLost { ref message } if message.starts_with("client:")));
    assert!(r.desync.is_some());

    // Exhaustion in fault mode = the cut boundary.
    let mut r2 = ReplaySession::new(vec![]);
    r2.stop_at_io_error = true;
    let out = r2.execute("SELECT 1");
    assert!(matches!(out, ExecOutcome::ConnectionLost { ref message } if message.contains("simbridge-cut")));
    assert!(r2.cut_hit);
    assert!(r2.desync.is_none());
}

#[test]
fn replay_io_dead_class_stops_fault_replay() {
    let entries = vec![(
        "INSERT INTO t VALUES (1)".to_string(),
        ExecOutcome::SqlError { sqlstate: "58030".into(), message: "io error".into() },
    )];
    let mut r = ReplaySession::new(entries);
    r.stop_at_io_error = true;
    let out = r.execute("INSERT INTO t VALUES (1)");
    assert!(matches!(out, ExecOutcome::ConnectionLost { ref message } if message.contains("simbridge-cut")));
    assert!(r.cut_hit);
    assert_eq!(r.consumed(), 0, "the io-dead statement is never fed to the model");
}

// ------------------------------------------------------------- synthesis

fn sql(text: &str) -> Sql {
    Sql { text: text.into(), mark: simharness::runner::planface::Mark::Read, meta: SqlMeta::default() }
}

fn plan_with(steps: Vec<Step>) -> Plan {
    Plan {
        header: PlanHeader {
            seed: 1,
            profile: "unit".into(),
            profile_sha256: "0".repeat(64),
            generator: "unit".into(),
        },
        steps,
    }
}

#[test]
fn synthesis_prefixes_reset_and_refuses_out_of_scope() {
    let p = plan_with(vec![Step::Query(sql("SELECT 1"))]);
    let s = synthesize_script(&p, false).expect("scriptable");
    assert_eq!(&s[..3], &RESET_STMTS.map(String::from));
    assert_eq!(s[3], "SELECT 1");

    let p = plan_with(vec![Step::Fault(FaultPoint::Disconnect)]);
    assert_eq!(synthesize_script(&p, false).unwrap_err(), "bridge-refused-fault");

    let p = plan_with(vec![Step::Session(1)]);
    assert_eq!(synthesize_script(&p, false).unwrap_err(), "bridge-refused-v2");
}

#[test]
fn null_bug_mirror_matches_shim_semantics() {
    // Doctors filter-side IS NULL on SELECTs past the first WHERE.
    assert_eq!(
        null_bug_rewrite("SELECT a FROM t WHERE b IS NULL"),
        "SELECT a FROM t WHERE b IS NULL AND false"
    );
    // Non-SELECTs and WHERE-less statements pass through.
    assert_eq!(
        null_bug_rewrite("UPDATE t SET a = 1 WHERE b IS NULL"),
        "UPDATE t SET a = 1 WHERE b IS NULL"
    );
    assert_eq!(null_bug_rewrite("SELECT b IS NULL FROM t"), "SELECT b IS NULL FROM t");
}
