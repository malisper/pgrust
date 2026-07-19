//! SIM-HARNESS-CONVERGE increment-1 unit coverage: the wire-transcript
//! parser, the sent-log alignment session, the script synthesizer's
//! refusals, and the NullBug mirror. Pure client-side (no sim binary).

use simharness::runner::driver::{ExecOutcome, Session};
use simharness::runner::planface::{FaultPoint, Plan, PlanHeader, Sql, SqlMeta, Step};
use simharness::runner::simbridge::{
    null_bug_rewrite, parse_transcript, synthesize_script, synthesize_two_session, ReplaySession,
    RESET_STMTS,
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

// ---------------------------------------------- SIM-CONVERGE inc-2: two-session

#[test]
fn two_session_split_maps_interleaving_onto_two_backends() {
    // Setup DDL (session 0), then an alternating cross-session interleaving:
    // s0 INSERT, s1 SELECT (reads s0's write), s0 INSERT, s1 SELECT.
    let p = plan_with(vec![
        Step::Ddl(sql("CREATE TABLE t (k int, v int)")),
        Step::Session(0),
        Step::Dml(sql("INSERT INTO t VALUES (1, 10)")),
        Step::Session(1),
        Step::Query(sql("SELECT count(*) FROM t")),
        Step::Session(0),
        Step::Dml(sql("INSERT INTO t VALUES (2, 20)")),
        Step::Session(1),
        Step::Query(sql("SELECT sum(v) FROM t")),
    ]);
    let ts = synthesize_two_session(&p, false).expect("two-session scriptable");

    // s1 boot setup: DROP/CREATE SCHEMA + SET search_path + the table DDL.
    assert_eq!(&ts.setup[..3], &RESET_STMTS.map(String::from));
    assert_eq!(ts.setup[3], "CREATE TABLE t (k int, v int)");

    // Each worker sets its own search_path, then its own statements.
    assert_eq!(ts.session_a[0], "SET search_path = simharness");
    assert_eq!(
        &ts.session_a[1..],
        &["INSERT INTO t VALUES (1, 10)", "INSERT INTO t VALUES (2, 20)"]
    );
    assert_eq!(ts.session_b[0], "SET search_path = simharness");
    assert_eq!(&ts.session_b[1..], &["SELECT count(*) FROM t", "SELECT sum(v) FROM t"]);

    // The global turn order: the two SET prologues (2, 3), then the plan's
    // strict alternation (2, 3, 2, 3).
    assert_eq!(ts.turns, vec![2, 3, 2, 3, 2, 3]);

    // Turn-accounting invariant: exactly one turn per statement, and each
    // turn's session-id owns the corresponding statement.
    assert_eq!(ts.turns.len(), ts.session_a.len() + ts.session_b.len());
    let (mut ia, mut ib) = (0usize, 0usize);
    for t in &ts.turns {
        match t {
            2 => ia += 1,
            3 => ib += 1,
            other => panic!("unexpected turn-id {other}"),
        }
    }
    assert_eq!(ia, ts.session_a.len());
    assert_eq!(ib, ts.session_b.len());
}

#[test]
fn two_session_refuses_inc3_shapes() {
    // Session fan-out beyond two (the S1 specconflict 4-session shape).
    let p = plan_with(vec![Step::Session(2)]);
    assert_eq!(synthesize_two_session(&p, false).unwrap_err(), "bridge-refused-v2-fanout");

    // The blocking-worker choreography (AsyncDml/Join/WaitUntil).
    let p = plan_with(vec![Step::Session(1), Step::AsyncDml(sql("UPDATE t SET v = 1"))]);
    assert_eq!(synthesize_two_session(&p, false).unwrap_err(), "bridge-refused-v2-async");
    let p = plan_with(vec![Step::Join(1)]);
    assert_eq!(synthesize_two_session(&p, false).unwrap_err(), "bridge-refused-v2-async");
    let p = plan_with(vec![Step::WaitUntil(sql("SELECT true"))]);
    assert_eq!(synthesize_two_session(&p, false).unwrap_err(), "bridge-refused-v2-async");

    // A DDL after the interleaving has begun (setup-reorder hazard).
    let p = plan_with(vec![
        Step::Dml(sql("INSERT INTO t VALUES (1)")),
        Step::Ddl(sql("CREATE TABLE u (x int)")),
    ]);
    assert_eq!(synthesize_two_session(&p, false).unwrap_err(), "bridge-refused-v2-lateddl");

    // Fault steps (as for v1).
    let p = plan_with(vec![Step::Fault(FaultPoint::Disconnect)]);
    assert_eq!(synthesize_two_session(&p, false).unwrap_err(), "bridge-refused-fault");

    // Tx steps: session-scoped tx modeling needs the replay pool (inc-3) —
    // the merged single-stream model walk would put the other session's
    // statements INSIDE this connection's transaction.
    let p = plan_with(vec![Step::Tx(simharness::runner::planface::TxCtl::Commit)]);
    assert_eq!(synthesize_two_session(&p, false).unwrap_err(), "bridge-refused-v2-tx");
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

// ------------------------------------------- SIM-CONVERGE inc-3: multi-session

use simharness::runner::simbridge::{
    check_entries_native, fixture_async_plan, s1_detector_rewrite, synthesize_multi_session,
    MultiSessionScripts, NativeStreams, TurnTok,
};

fn toks(ms: &MultiSessionScripts) -> Vec<String> {
    ms.turns.iter().map(|t| t.render()).collect()
}

#[test]
fn multi_synthesis_matches_two_session_on_the_inc2_shape() {
    // The inc-2 milestone shape must split IDENTICALLY under the new
    // synthesizer (same scripts, same rendered turn string) — the T1
    // agreement leg's precondition.
    let p = plan_with(vec![
        Step::Ddl(sql("CREATE TABLE t (k int, v int)")),
        Step::Session(0),
        Step::Dml(sql("INSERT INTO t VALUES (1, 10)")),
        Step::Session(1),
        Step::Query(sql("SELECT count(*) FROM t")),
        Step::Session(0),
        Step::Dml(sql("INSERT INTO t VALUES (2, 20)")),
        Step::Session(1),
        Step::Query(sql("SELECT sum(v) FROM t")),
    ]);
    let ts = synthesize_two_session(&p, false).expect("two-session scriptable");
    let ms = synthesize_multi_session(&p, None).expect("multi scriptable");
    assert_eq!(ms.setup, ts.setup);
    assert_eq!(ms.sessions.len(), 2);
    assert_eq!(ms.sessions[0], ts.session_a);
    assert_eq!(ms.sessions[1], ts.session_b);
    let want: Vec<String> = ts.turns.iter().map(|t| t.to_string()).collect();
    assert_eq!(toks(&ms), want);
}

#[test]
fn multi_synthesis_maps_async_join_poll_and_tx() {
    use simharness::runner::planface::{IsoLevel, TxCtl};
    let p = plan_with(vec![
        Step::Ddl(sql("CREATE TABLE t (k int)")),
        Step::Session(0),
        Step::Tx(TxCtl::Begin(IsoLevel::ReadCommitted)),
        Step::Dml(sql("LOCK TABLE t IN ACCESS EXCLUSIVE MODE")),
        Step::Session(1),
        Step::AsyncDml(sql("INSERT INTO t VALUES (1)")),
        Step::Session(0),
        Step::WaitUntil(sql("SELECT count(*) = 1 FROM pg_locks WHERE NOT granted")),
        Step::Tx(TxCtl::Commit),
        Step::Join(1),
        Step::Session(1),
        Step::Query(sql("SELECT count(*) FROM t")),
        Step::Session(0),
    ]);
    let ms = synthesize_multi_session(&p, None).expect("multi scriptable");
    assert_eq!(ms.sessions.len(), 2);
    // Session 0: prologue, BEGIN, LOCK, the WaitUntil probe, COMMIT.
    assert_eq!(
        ms.sessions[0],
        vec![
            "SET search_path = simharness",
            "BEGIN ISOLATION LEVEL READ COMMITTED",
            "LOCK TABLE t IN ACCESS EXCLUSIVE MODE",
            "SELECT count(*) = 1 FROM pg_locks WHERE NOT granted",
            "COMMIT",
        ]
    );
    // Session 1: prologue, the async INSERT, the final read.
    assert_eq!(
        ms.sessions[1],
        vec!["SET search_path = simharness", "INSERT INTO t VALUES (1)", "SELECT count(*) FROM t"]
    );
    // Turns: prologues (2, 3), BEGIN 2, LOCK 2, d3 (dispatch), p2 (poll),
    // COMMIT 2, j3 (join), read 3.
    assert_eq!(toks(&ms), vec!["2", "3", "2", "2", "d3", "p2", "2", "j3", "3"]);
    // One send per Stmt/Dispatch/Poll turn; joins add no statement.
    let sends = ms
        .turns
        .iter()
        .filter(|t| !matches!(t, TurnTok::Join(_)))
        .count();
    assert_eq!(sends, ms.sessions.iter().map(|s| s.len()).sum::<usize>());
}

#[test]
fn multi_synthesis_routes_late_ddl_and_four_sessions() {
    // Late DDL rides the active session now (the inc-2 refusal retired).
    let p = plan_with(vec![
        Step::Dml(sql("INSERT INTO t VALUES (1)")),
        Step::Ddl(sql("CREATE TABLE u (x int)")),
        Step::Session(3),
        Step::Query(sql("SELECT 1")),
    ]);
    let ms = synthesize_multi_session(&p, None).expect("multi scriptable");
    assert_eq!(ms.sessions.len(), 4);
    assert_eq!(
        ms.sessions[0],
        vec!["SET search_path = simharness", "INSERT INTO t VALUES (1)", "CREATE TABLE u (x int)"]
    );
    assert_eq!(ms.sessions[3], vec!["SET search_path = simharness", "SELECT 1"]);
    // Prologue turns 2..5, then s2 stmt, s2 late-ddl, s5 query.
    assert_eq!(toks(&ms), vec!["2", "3", "4", "5", "2", "2", "5"]);

    // Fan-out past four plan sessions still refuses.
    let p = plan_with(vec![Step::Session(4)]);
    assert_eq!(synthesize_multi_session(&p, None).unwrap_err(), "bridge-refused-v2-fanout");

    // Async on session 0 refuses (the walker's own leg would wedge).
    let p = plan_with(vec![Step::AsyncDml(sql("INSERT INTO t VALUES (1)"))]);
    assert_eq!(synthesize_multi_session(&p, None).unwrap_err(), "bridge-refused-v2-async0");
    let p = plan_with(vec![Step::Join(0)]);
    assert_eq!(synthesize_multi_session(&p, None).unwrap_err(), "bridge-refused-v2-async0");

    // Fault steps still refuse.
    let p = plan_with(vec![Step::Fault(FaultPoint::Disconnect)]);
    assert_eq!(synthesize_multi_session(&p, None).unwrap_err(), "bridge-refused-fault");
}

#[test]
fn s1_detector_rewrite_targets_only_the_detector() {
    assert_eq!(
        s1_detector_rewrite("SELECT key, data FROM shp_s1t_ab12"),
        "SELECT key, data FROM shp_s1t_ab12 WHERE false"
    );
    // Other statements pass through untouched.
    assert_eq!(
        s1_detector_rewrite("SELECT count(*) FROM shp_s1t_ab12"),
        "SELECT count(*) FROM shp_s1t_ab12"
    );
    assert_eq!(
        s1_detector_rewrite("INSERT INTO s1t_ab12(key, data) VALUES ('k1', 'x')"),
        "INSERT INTO s1t_ab12(key, data) VALUES ('k1', 'x')"
    );
}

#[test]
fn async_fixture_walks_natively_over_synthetic_streams() {
    // Pure client-side proof of the session-aware replay pool: hand-build
    // the per-session (sent, outcome) streams the sim WOULD record for the
    // async fixture, then drive the REAL execute_plan through
    // check_entries_native — asserts + join slot + full consumption.
    let (plan, ctx) = fixture_async_plan();
    let ms = synthesize_multi_session(&plan, None).expect("fixture scriptable");
    assert_eq!(
        toks(&ms),
        vec!["2", "3", "2", "2", "d3", "2", "2", "j3", "3"],
        "fixture turn schedule"
    );
    let cmd = |tag: &str| ExecOutcome::Command { tag: tag.to_string() };
    let rows1 = ExecOutcome::Rows { rows: vec![vec![Some("2".to_string())]] };
    // s1: reset prologue + hoisted CREATE TABLE.
    let mut primary: Vec<(String, ExecOutcome)> = vec![
        ("DROP SCHEMA IF EXISTS simharness CASCADE".into(), cmd("0")),
        ("CREATE SCHEMA simharness".into(), cmd("0")),
        ("SET search_path = simharness".into(), cmd("0")),
        ("CREATE TABLE at (k int)".into(), cmd("0")),
    ];
    // session 0 (SET prologue verified+dropped by native_streams — here we
    // build post-prologue streams directly).
    primary.extend([
        ("BEGIN ISOLATION LEVEL READ COMMITTED".to_string(), cmd("0")),
        ("LOCK TABLE at IN ACCESS EXCLUSIVE MODE".to_string(), cmd("0")),
        ("INSERT INTO at VALUES (2)".to_string(), cmd("1")),
        ("COMMIT".to_string(), cmd("0")),
    ]);
    let worker: Vec<(String, ExecOutcome)> = vec![
        ("INSERT INTO at VALUES (1)".to_string(), cmd("1")),
        ("SELECT count(*) FROM at".to_string(), rows1),
    ];
    let checked = check_entries_native(
        &plan,
        &ctx,
        NativeStreams { primary, workers: vec![worker] },
        None,
    )
    .expect("native walk");
    assert!(checked.desync.is_none(), "desync: {:?}", checked.desync);
    assert!(
        checked.report.failure.is_none(),
        "failure: {:?}",
        checked.report.failure
    );
    assert_eq!(checked.leftover, 0, "every recorded entry consumed");
    // 2 slot asserts evaluated + statements ok'd; no skips.
    assert_eq!(
        checked.report.class_counts.get("property-skipped").copied().unwrap_or(0),
        0
    );
}

#[test]
fn native_walk_desyncs_on_perturbed_worker_stream() {
    // The pool red's mechanism: swapping two recorded entries in a worker
    // stream must surface as a desync, never a silent pass.
    let (plan, ctx) = fixture_async_plan();
    let cmd = |tag: &str| ExecOutcome::Command { tag: tag.to_string() };
    let rows1 = ExecOutcome::Rows { rows: vec![vec![Some("2".to_string())]] };
    let mut primary: Vec<(String, ExecOutcome)> = vec![
        ("DROP SCHEMA IF EXISTS simharness CASCADE".into(), cmd("0")),
        ("CREATE SCHEMA simharness".into(), cmd("0")),
        ("SET search_path = simharness".into(), cmd("0")),
        ("CREATE TABLE at (k int)".into(), cmd("0")),
    ];
    primary.extend([
        ("BEGIN ISOLATION LEVEL READ COMMITTED".to_string(), cmd("0")),
        ("LOCK TABLE at IN ACCESS EXCLUSIVE MODE".to_string(), cmd("0")),
        ("INSERT INTO at VALUES (2)".to_string(), cmd("1")),
        ("COMMIT".to_string(), cmd("0")),
    ]);
    // Worker stream with the two entries SWAPPED.
    let worker: Vec<(String, ExecOutcome)> = vec![
        ("SELECT count(*) FROM at".to_string(), rows1),
        ("INSERT INTO at VALUES (1)".to_string(), cmd("1")),
    ];
    let checked = check_entries_native(
        &plan,
        &ctx,
        NativeStreams { primary, workers: vec![worker] },
        None,
    )
    .expect("native walk runs");
    assert!(checked.desync.is_some(), "perturbed stream must desync");
}

#[test]
fn async_fixture_round_trips_as_plan_v2_bytes() {
    let (plan, _ctx) = fixture_async_plan();
    let rendered = plan.render();
    assert!(rendered.starts_with("-- simharness plan v2 (multi-session)"), "v2 header");
    let back = Plan::parse(&rendered).expect("parses");
    assert_eq!(back, plan, "render/parse round trip");
}
