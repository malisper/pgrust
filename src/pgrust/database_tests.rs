use super::*;
use crate::backend::catalog::loader::load_physical_catalog_rows_visible_scoped;
use crate::backend::catalog::persistence::sync_catalog_rows_subset;
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::commands::analyze::collect_analyze_stats;
use crate::backend::executor::{ExecError, Value};
use crate::backend::parser::{BoundRelation, CatalogLookup, ParseError, SqlType, SqlTypeKind};
use crate::backend::utils::cache::lsyscache::LazyCatalogLookup;
use crate::backend::utils::misc::notices::{
    clear_notices as clear_backend_notices, take_notices as take_backend_notices,
};
use crate::include::catalog::{
    BootstrapCatalogKind, FLOAT8_TYPE_OID, INT4_TYPE_OID, INT4RANGE_TYPE_OID,
    PG_CLASS_RELATION_OID, PG_PROC_RELATION_OID, PG_TYPE_RELATION_OID, PgAggregateRow,
};
use crate::include::nodes::datum::IntervalValue;
use crate::include::nodes::parsenodes::MaintenanceTarget;
use crate::include::nodes::primnodes::QueryColumn;
use crate::pl::plpgsql::{clear_notices, take_notices};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use std::time::{Duration, Instant};

// These are deadlock watchdogs, not latency expectations. Full `cargo test`
// runs several storage/concurrency tests at once, so leave enough headroom for
// slow CI workers and parallel local agent workspaces.
const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONTENTION_TEST_TIMEOUT: Duration = Duration::from_secs(60);
const HEAVY_CONTENTION_TEST_TIMEOUT: Duration = Duration::from_secs(120);
const STRESS_TEST_TIMEOUT: Duration = Duration::from_secs(60);
const PIN_LEAK_CONTENTION_TEST_TIMEOUT: Duration = Duration::from_secs(120);
const SAME_ROW_UPDATE_TEST_TIMEOUT: Duration = Duration::from_secs(20);
const PGBENCH_STYLE_TEST_TIMEOUT: Duration = Duration::from_secs(60);
const SAME_ROW_UPDATE_FULL_SUITE_TIMEOUT: Duration = Duration::from_secs(60);

/// Start a background thread that periodically checks for deadlocks
/// using parking_lot's deadlock detector.  Called once via `Once`.
#[cfg(feature = "deadlock_detection")]
fn start_deadlock_checker() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        thread::Builder::new()
            .name("deadlock-checker".into())
            .spawn(|| {
                loop {
                    thread::sleep(Duration::from_secs(1));
                    let deadlocks = parking_lot::deadlock::check_deadlock();
                    if !deadlocks.is_empty() {
                        eprintln!("=== DEADLOCK DETECTED ({} cycle(s)) ===", deadlocks.len());
                        for (i, threads) in deadlocks.iter().enumerate() {
                            eprintln!("--- cycle {i} ---");
                            for t in threads {
                                eprintln!("thread {:?}:\n{:#?}", t.thread_id(), t.backtrace());
                            }
                        }
                        // Don't panic here — just log. The test timeout will handle it.
                    }
                }
            })
            .unwrap();
    });
}

#[cfg(not(feature = "deadlock_detection"))]
fn start_deadlock_checker() {}

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

fn join_all_with_timeout(handles: Vec<thread::JoinHandle<()>>, timeout: Duration) {
    start_deadlock_checker();
    let deadline = Instant::now() + timeout;
    for h in handles {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            #[cfg(feature = "deadlock_detection")]
            log_deadlocks();
            panic!("test timed out after {timeout:?} — likely deadlock");
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = thread::spawn(move || {
            let result = h.join();
            let _ = tx.send(result);
        });
        match rx.recv_timeout(remaining) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => std::panic::resume_unwind(e),
            Err(_) => {
                #[cfg(feature = "deadlock_detection")]
                log_deadlocks();
                panic!("test timed out after {timeout:?} — likely deadlock");
            }
        }
        let _ = waiter.join();
    }
}

#[cfg(feature = "deadlock_detection")]
fn log_deadlocks() {
    let deadlocks = parking_lot::deadlock::check_deadlock();
    if deadlocks.is_empty() {
        eprintln!("pgrust: parking_lot deadlock detector found no cycles");
        return;
    }

    eprintln!("pgrust: detected {} deadlock cycle(s)", deadlocks.len());
    for (i, threads) in deadlocks.iter().enumerate() {
        eprintln!("pgrust: deadlock cycle #{i}");
        for thread in threads {
            eprintln!("pgrust: thread id {:?}", thread.thread_id());
            eprintln!("{:#?}", thread.backtrace());
        }
    }
}

fn temp_dir(label: &str) -> PathBuf {
    let _ = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    crate::pgrust::test_support::seeded_temp_dir("database", label)
}

fn scratch_temp_dir(label: &str) -> PathBuf {
    let _ = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    crate::pgrust::test_support::scratch_temp_dir("database", label)
}

fn role_oid(db: &Database, role_name: &str) -> u32 {
    db.catalog
        .read()
        .catcache()
        .unwrap()
        .authid_rows()
        .into_iter()
        .find(|row| row.rolname.eq_ignore_ascii_case(role_name))
        .map(|row| row.oid)
        .unwrap()
}

struct AnalyzeRelkindOverrideCatalog<'a> {
    inner: LazyCatalogLookup<'a>,
    relkind_overrides: HashMap<u32, char>,
}

impl AnalyzeRelkindOverrideCatalog<'_> {
    fn apply_override(&self, mut relation: BoundRelation) -> BoundRelation {
        if let Some(relkind) = self.relkind_overrides.get(&relation.relation_oid) {
            relation.relkind = *relkind;
        }
        relation
    }
}

impl CatalogLookup for AnalyzeRelkindOverrideCatalog<'_> {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.inner
            .lookup_any_relation(name)
            .map(|relation| self.apply_override(relation))
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.inner
            .relation_by_oid(relation_oid)
            .map(|relation| self.apply_override(relation))
    }

    fn find_all_inheritors(&self, relation_oid: u32) -> Vec<u32> {
        self.inner.find_all_inheritors(relation_oid)
    }

    fn has_subclass(&self, relation_oid: u32) -> bool {
        self.inner.has_subclass(relation_oid)
    }
}

fn analyze_executor_context(
    db: &Database,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    visible_catalog: Option<crate::backend::utils::cache::visible_catalog::VisibleCatalog>,
) -> crate::backend::executor::ExecutorContext {
    crate::backend::executor::ExecutorContext {
        pool: Arc::clone(&db.pool),
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        lock_status_provider: Some(Arc::new(db.clone())),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: Arc::clone(&db.advisory_locks),
        row_locks: Arc::clone(&db.row_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        gucs: std::collections::HashMap::new(),
        interrupts: db.interrupt_state(client_id),
        stats: Arc::clone(&db.stats),
        session_stats: db.session_stats_state(client_id),
        snapshot: db.txns.read().snapshot_for_command(xid, cid).unwrap(),
        transaction_state: None,
        client_id,
        current_database_name: db.current_database_name(),
        session_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        active_role_oid: None,
        session_replication_role: Default::default(),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: cid,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        timed: false,
        allow_side_effects: false,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        pending_async_notifications: Vec::new(),
        catalog: visible_catalog,
        compiled_functions: HashMap::new(),
        cte_tables: HashMap::new(),
        cte_producers: HashMap::new(),
        recursive_worktables: HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    }
}

#[test]
fn ephemeral_database_executes_basic_sql() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int, name text)")
        .expect("create table");
    session
        .execute(&db, "insert into items values (1, 'a'), (2, 'b')")
        .expect("insert rows");

    let result = session
        .execute(&db, "select id, name from items order by id")
        .expect("select rows");
    let StatementResult::Query { rows, .. } = result else {
        panic!("expected query result");
    };
    assert_eq!(
        rows,
        vec![
            vec![Value::Int32(1), Value::Text("a".into())],
            vec![Value::Int32(2), Value::Text("b".into())],
        ]
    );
}

#[test]
fn generated_columns_compute_on_insert_update_and_read() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table stored_generated (a int4, b int4 generated always as (a + 1) stored)",
        )
        .expect("create stored generated table");
    session
        .execute(&db, "insert into stored_generated(a) values (4)")
        .expect("insert generated row");
    session
        .execute(&db, "insert into stored_generated values (5, default)")
        .expect("insert generated default row");
    assert_single_int_column_rows(
        session
            .execute(&db, "select b from stored_generated order by a")
            .expect("select stored generated rows"),
        vec![vec![Value::Int32(5)], vec![Value::Int32(6)]],
    );

    session
        .execute(&db, "update stored_generated set a = 9 where a = 4")
        .expect("update generated row");
    assert_single_int_column_rows(
        session
            .execute(&db, "select b from stored_generated where a = 9")
            .expect("select updated generated row"),
        vec![vec![Value::Int32(10)]],
    );

    let err = session
        .execute(&db, "insert into stored_generated values (7, 99)")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::DetailedError {
            sqlstate: "428C9",
            ..
        })
    ));

    session
        .execute(
            &db,
            "create table virtual_generated (a int4, b int4 generated always as (a + 1))",
        )
        .expect("create virtual generated table");
    session
        .execute(&db, "insert into virtual_generated(a) values (11)")
        .expect("insert virtual generated row");
    assert_single_int_column_rows(
        session
            .execute(&db, "select b from virtual_generated where b = 12")
            .expect("select virtual generated row"),
        vec![vec![Value::Int32(12)]],
    );
}

#[test]
fn pg_backend_pid_returns_session_client_id() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");

    assert_eq!(
        query_rows(&db, 41, "select pg_backend_pid()"),
        vec![vec![Value::Int32(41)]]
    );
}

#[test]
fn txid_snapshot_type_round_trips_and_validates_visibility() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table snapshot_test (snap txid_snapshot)")
        .unwrap();
    session
        .execute(&db, "insert into snapshot_test values ('12:16:14,14')")
        .unwrap();

    assert_eq!(
        session_query_rows(&mut session, &db, "select snap from snapshot_test"),
        vec![vec![Value::Text("12:16:14".into())]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select txid_visible_in_snapshot(13, '12:20:13,15,18'::txid_snapshot), \
                    txid_visible_in_snapshot(14, '12:20:13,15,18'::txid_snapshot), \
                    pg_input_is_valid('12:16:14,13', 'txid_snapshot')",
        ),
        vec![vec![
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
        ]]
    );
}

#[test]
fn txid_current_and_if_assigned_follow_lazy_xid_assignment() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session.execute(&db, "begin").unwrap();

    assert_eq!(
        session_query_rows(&mut session, &db, "select txid_current_if_assigned()"),
        vec![vec![Value::Null]]
    );

    let rows = session_query_rows(&mut session, &db, "select txid_current()");
    let txid = match &rows[..] {
        [row] => match &row[..] {
            [Value::Int64(txid)] => *txid,
            other => panic!("expected bigint txid_current result, got {other:?}"),
        },
        other => panic!("expected one txid_current row, got {other:?}"),
    };

    assert_eq!(
        session_query_rows(&mut session, &db, "select txid_current_if_assigned()"),
        vec![vec![Value::Int64(txid)]]
    );

    session.execute(&db, "commit").unwrap();
}

#[test]
fn create_temp_table_accepts_fixed_length_array_column_syntax() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create temp table arrtest2 (i integer ARRAY[4], f float8[], n numeric[], t text[], d timestamp[])",
        )
        .expect("create temp array table");

    session
        .execute(
            &db,
            "insert into arrtest2 values ('{1,2,3,4}', '{1.5}', '{2.5}', '{hi}', '{2001-01-01 00:00:00}')",
        )
        .expect("insert temp array row");
}

#[test]
fn quantified_like_any_all_array_operators_work() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
                'foo' like any (array['%a', '%o']), \
                'foo' like all (array['f%', '%o']), \
                'foo' like all (array['f%', '%b']), \
                'foo' not like any (array['%a', '%b']), \
                'foo' ilike any (array['%A', '%O']), \
                'foo' ilike all (array['F%', '%O'])",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
        ]]
    );
}

#[test]
fn quantified_similar_any_all_array_operators_work() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
                'foo' similar to any (array['%(o|a)%', 'bar']), \
                'foo' similar to all (array['%(o|a)%', '(f|g)%']), \
                'foo' similar to all (array['%(o|a)%', '(b|c)%']), \
                'foo' not similar to any (array['bar', 'baz']), \
                'foo' not similar to all (array['foo', 'bar'])",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
        ]]
    );
}

fn assert_stack_depth_limit_error(err: ExecError) {
    match err {
        ExecError::DetailedError {
            message,
            hint: Some(hint),
            sqlstate,
            ..
        }
        | ExecError::Parse(ParseError::DetailedError {
            message,
            hint: Some(hint),
            sqlstate,
            ..
        }) if message == "stack depth limit exceeded"
            && sqlstate == "54001"
            && hint.contains("\"max_stack_depth\" (currently 100kB)") => {}
        other => panic!("expected stack depth error, got {other:?}"),
    }
}

#[test]
fn jsonb_input_respects_max_stack_depth_setting() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "set max_stack_depth = '100kB'")
        .expect("set max_stack_depth");

    let err = session
        .execute(&db, &format!("select '{}'::jsonb", "[".repeat(10_000)))
        .unwrap_err();
    assert_stack_depth_limit_error(err);
}

#[test]
fn sql_function_recursion_respects_max_stack_depth_setting() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "set max_stack_depth = '100kB'")
        .expect("set max_stack_depth");
    session
        .execute(
            &db,
            "create function infinite_recurse() returns int4 as \
             'select infinite_recurse()' language sql",
        )
        .expect("create recursive function");

    let err = session
        .execute(&db, "select infinite_recurse()")
        .unwrap_err();
    assert_stack_depth_limit_error(err);
}

#[test]
fn mutually_recursive_sql_functions_respect_max_stack_depth_setting() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "set max_stack_depth = '100kB'")
        .expect("set max_stack_depth");
    session
        .execute(
            &db,
            "create function recurse_a() returns int4 as 'select recurse_b()' language sql",
        )
        .expect("create recurse_a");
    session
        .execute(
            &db,
            "create function recurse_b() returns int4 as 'select recurse_a()' language sql",
        )
        .expect("create recurse_b");

    let err = session.execute(&db, "select recurse_a()").unwrap_err();
    assert_stack_depth_limit_error(err);
}

#[test]
fn large_rust_stack_still_respects_sql_max_stack_depth() {
    std::thread::Builder::new()
        .name("large_rust_stack_still_respects_sql_max_stack_depth".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let db = Database::open_ephemeral(32).expect("open ephemeral database");
            let mut session = Session::new(1);

            session
                .execute(&db, "set max_stack_depth = '100kB'")
                .expect("set max_stack_depth");
            session
                .execute(
                    &db,
                    "create function large_stack_recurse() returns int4 as \
                     'select large_stack_recurse()' language sql",
                )
                .expect("create recursive function");

            let err = session
                .execute(&db, "select large_stack_recurse()")
                .unwrap_err();
            assert_stack_depth_limit_error(err);
        })
        .expect("spawn large-stack test")
        .join()
        .expect("large-stack test panicked");
}

#[test]
fn jsonb_populate_record_named_composite_works() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create type jbpop as (a text, b int4, c timestamp)")
        .expect("create composite type");

    let result = session
        .execute(
            &db,
            "select * from jsonb_populate_record(null::jbpop, '{\"a\":\"blurfl\",\"x\":43.2}') q",
        )
        .expect("run jsonb_populate_record");
    let StatementResult::Query { rows, .. } = result else {
        panic!("expected query result");
    };
    assert_eq!(
        rows,
        vec![vec![Value::Text("blurfl".into()), Value::Null, Value::Null,]]
    );
}

#[test]
fn named_composite_row_cast_coerces_fields() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create type jbpop as (a text, b int4, c timestamp)")
        .expect("create composite type");

    let timestamp_row = session
        .execute(&db, "select timestamp '2012-12-31 15:30:56'")
        .expect("select timestamp literal");
    let StatementResult::Query {
        rows: expected_rows,
        ..
    } = timestamp_row
    else {
        panic!("expected query result");
    };
    let expected_timestamp = expected_rows[0][0].clone();

    let field_result = session
        .execute(&db, "select (row('x',3,'2012-12-31 15:30:56')::jbpop).c")
        .expect("select named composite field");
    let StatementResult::Query { rows, .. } = field_result else {
        panic!("expected query result");
    };
    assert_eq!(rows, vec![vec![expected_timestamp.clone()]]);

    let populate_result = session
        .execute(
            &db,
            "select * from jsonb_populate_record(row('x',3,'2012-12-31 15:30:56')::jbpop, '{\"a\":\"blurfl\",\"x\":43.2}') q",
        )
        .expect("run jsonb_populate_record");
    let StatementResult::Query { rows, .. } = populate_result else {
        panic!("expected query result");
    };
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("blurfl".into()),
            Value::Int32(3),
            expected_timestamp,
        ]]
    );
}

#[test]
fn create_type_skips_temp_schema_in_search_path() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table temp_rows(x int4)")
        .expect("create temp table");
    session
        .execute(&db, "create type jbpop as (a text, b int4, c timestamp)")
        .expect("create composite type with temp schema in search_path");
}

#[test]
fn recursive_cte_union_all_counts_up() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "with recursive t(n) as (
                values (1)
                union all
                select n + 1 from t where n < 5
            )
            select n from t order by n",
        )
        .expect("run recursive cte");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)],
                    vec![Value::Int32(4)],
                    vec![Value::Int32(5)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_cte_respects_outer_limit() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "with recursive t(n) as (
                values (1)
                union all
                select n + 1 from t
            )
            select n from t limit 5",
        )
        .expect("run recursive cte with limit");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)],
                    vec![Value::Int32(4)],
                    vec![Value::Int32(5)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_cte_union_deduplicates_and_terminates() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "with recursive t(n) as (
                values (1), (1)
                union
                select n from t where n < 2
            )
            select n from t",
        )
        .expect("run recursive union");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_cte_rejects_self_reference_inside_subquery() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let err = session
        .execute(
            &db,
            "with recursive x(n) as (
                select 1
                union all
                select n + 1 from x where n in (select * from x)
            )
            select * from x",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ExecError::Parse(ParseError::InvalidRecursion(message))
            if message == "recursive reference to query \"x\" must not appear within a subquery"
    ));
}

#[test]
fn recursive_cte_intermediate_setop_with_can_read_worktable() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "with recursive outermost(x) as (
                select 1
                union (with innermost as (select 2)
                       select * from outermost
                       union select * from innermost)
            )
            select * from outermost order by 1",
        )
        .expect("execute recursive CTE");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_cte_nested_union_ctes_inside_recursive_term_execute() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "with recursive outermost(x) as (
             select 1
             union (with innermost1 as (
              select 2
              union (with innermost2 as (
               select 3
               union (with innermost3 as (
                select 4
                union (with innermost4 as (
                 select 5
                 union (with innermost5 as (
                  select 6
                  union (with innermost6 as
                   (select 7)
                   select * from innermost6))
                  select * from innermost5))
                 select * from innermost4))
                select * from innermost3))
               select * from innermost2))
              select * from outermost
              union select * from innermost1)
            )
            select * from outermost order by 1",
        )
        .expect("execute recursive CTE");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)],
                    vec![Value::Int32(4)],
                    vec![Value::Int32(5)],
                    vec![Value::Int32(6)],
                    vec![Value::Int32(7)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_cte_rejects_unsupported_term_decorations() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    for (sql, expected) in [
        (
            "with recursive x(n) as (
                select 1
                union all
                select n + 1 from x order by 1
            )
            select * from x",
            "ORDER BY in a recursive query is not implemented",
        ),
        (
            "with recursive x(n) as (
                select 1
                union all
                select n + 1 from x limit 10 offset 1
            )
            select * from x",
            "OFFSET in a recursive query is not implemented",
        ),
        (
            "with recursive x(n) as (
                select 1
                union all
                select n + 1 from x for update
            )
            select * from x",
            "FOR UPDATE/SHARE in a recursive query is not implemented",
        ),
    ] {
        let err = session.execute(&db, sql).unwrap_err();
        assert!(matches!(
            err,
            ExecError::Parse(ParseError::FeatureNotSupported(message)) if message == expected
        ));
    }
}

fn assert_single_int_column_rows(result: StatementResult, expected: Vec<Vec<Value>>) {
    match result {
        StatementResult::Query { rows, .. } => assert_eq!(rows, expected),
        other => panic!("expected query result, got {:?}", other),
    }
}

fn assert_single_int_column_shape(result: StatementResult, expected_len: usize) {
    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), expected_len);
            assert_eq!(rows.first(), Some(&vec![Value::Int32(1)]));
            assert_eq!(rows.last(), Some(&vec![Value::Int32(10)]));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_cte_x_shape_executes() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let x_only = session
        .execute(
            &db,
            "with recursive x(id) as (
                select 1
                union all
                select id + 1 from x where id < 3
            )
            select * from x",
        )
        .expect("run recursive x shape");
    assert_single_int_column_rows(
        x_only,
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
        ],
    );
}

#[test]
fn recursive_cte_x_y_shape_executes() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let y_shape = session
        .execute(
            &db,
            "with recursive x(id) as (
                select 1
                union all
                select id + 1 from x where id < 3
            ),
            y(id) as (
                select * from x
                union all
                select * from x
            )
            select * from y",
        )
        .expect("run x+y shape");
    assert_single_int_column_rows(
        y_shape,
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
        ],
    );
}

#[test]
fn recursive_cte_x_z_shape_executes() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let z_from_x = session
        .execute(
            &db,
            "with recursive x(id) as (
                select 1
                union all
                select id + 1 from x where id < 3
            ),
            z(id) as (
                select * from x
                union all
                select id + 1 from z where id < 10
            )
            select * from z",
        )
        .expect("run x+z shape");
    assert_single_int_column_shape(z_from_x, 27);
}

#[test]
fn recursive_cte_xyz_chain_executes() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let original = session
        .execute(
            &db,
            "with recursive
                x(id) as (
                    select 1
                    union all
                    select id + 1 from x where id < 3
                ),
                y(id) as (
                    select * from x
                    union all
                    select * from x
                ),
                z(id) as (
                    select * from y
                    union all
                    select id + 1 from z where id < 10
                )
            select * from z",
        )
        .expect("run original recursive cte chain");
    assert_single_int_column_shape(original, 54);
}

#[test]
fn scalar_values_subquery_expr_returns_single_value() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(&db, "select (values (1))")
        .expect("run scalar values subquery");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_union_distinct_rejects_varbit_columns() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    match session.execute(
        &db,
        "with recursive t(n) as (
            values ('01'::varbit)
            union
            select n || '10'::varbit from t where n < '100'::varbit
        )
        select n from t",
    ) {
        Err(ExecError::DetailedError {
            sqlstate,
            message,
            detail,
            ..
        }) => {
            assert_eq!(sqlstate, "0A000");
            assert_eq!(message, "could not implement recursive UNION");
            assert_eq!(
                detail.as_deref(),
                Some("All column datatypes must be hashable.")
            );
        }
        other => panic!(
            "expected recursive union hashability error, got {:?}",
            other
        ),
    }
}

#[test]
fn pg_typeof_reports_bound_expression_types() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "with q as (select 'foo' as x)
             select x, pg_typeof(x) from q",
        )
        .expect("run pg_typeof over cte column");

    match result {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(columns[1].sql_type, SqlType::new(SqlTypeKind::Text));
            assert_eq!(
                rows,
                vec![vec![Value::Text("foo".into()), Value::Text("text".into())]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn pg_typeof_tracks_recursive_cte_output_type() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "with recursive t(n) as (
                select 'foo'
                union all
                select n || ' bar' from t where length(n) < 20
             )
             select n, pg_typeof(n) from t",
        )
        .expect("run pg_typeof over recursive cte");

    match result {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(columns[1].sql_type, SqlType::new(SqlTypeKind::Text));
            assert_eq!(rows[0][1], Value::Text("text".into()));
            assert!(rows.iter().all(|row| row[1] == Value::Text("text".into())));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn union_all_selects_returns_all_rows() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(&db, "select 1 as x union all select 2 as x order by x")
        .expect("run union all");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn union_distinct_deduplicates_rows() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(&db, "select 1 as x union select 1 as x")
        .expect("run union distinct");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn mixed_union_chain_uses_postgres_left_associativity() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "select 1 as x union select 2 as x union all select 2 as x order by x",
        )
        .expect("run mixed union chain");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(2)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn except_all_with_distinct_right_input_executes() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (x int4)")
        .expect("create table");
    session
        .execute(&db, "insert into items values (1), (1), (2)")
        .expect("insert rows");

    let result = session
        .execute(
            &db,
            "select x from items except all select distinct x from items where x = 1 order by x",
        )
        .expect("run except all with distinct input");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn union_in_derived_subquery_with_cte_executes() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "select count(*) from (
                with q1(x) as (select random() from generate_series(1, 5))
                select * from q1
                union
                select * from q1
            ) ss",
        )
        .expect("run union in derived subquery");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(5)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn intersect_distinct_returns_shared_rows() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "select x from (select 1 as x union all select 2 as x) a
             intersect
             select x from (select 2 as x union all select 3 as x) b",
        )
        .expect("run intersect");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn intersect_all_preserves_min_multiplicity() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "select x from (select 1 as x union all select 1 as x union all select 2 as x) a
             intersect all
             select x from (select 1 as x union all select 2 as x union all select 2 as x) b
             order by x",
        )
        .expect("run intersect all");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn except_all_subtracts_multiplicity() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "select x from (select 1 as x union all select 1 as x union all select 2 as x) a
             except all
             select x from (select 1 as x union all select 3 as x) b
             order by x",
        )
        .expect("run except all");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn ephemeral_database_rolls_back_aborted_transaction() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int)")
        .expect("create table");
    session.execute(&db, "begin").expect("begin");
    session
        .execute(&db, "insert into items values (1)")
        .expect("insert row");
    session.execute(&db, "rollback").expect("rollback");

    let result = session
        .execute(&db, "select id from items")
        .expect("select after rollback");
    let StatementResult::Query { rows, .. } = result else {
        panic!("expected query result");
    };
    assert!(rows.is_empty(), "rolled back row should not be visible");
}

fn query_rows(db: &Database, client_id: u32, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(client_id, sql).unwrap() {
        StatementResult::Query { rows, .. } => rows,
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn filtered_cross_join_preserves_left_outer_row_order() {
    let base = temp_dir("filtered_cross_join_left_order");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table cross_join_order (v int4 not null)")
        .unwrap();
    db.execute(1, "insert into cross_join_order values (1), (2), (3)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select l.v, r.v from cross_join_order l, cross_join_order r where l.v <> r.v",
        ),
        vec![
            vec![Value::Int32(1), Value::Int32(2)],
            vec![Value::Int32(1), Value::Int32(3)],
            vec![Value::Int32(2), Value::Int32(1)],
            vec![Value::Int32(2), Value::Int32(3)],
            vec![Value::Int32(3), Value::Int32(1)],
            vec![Value::Int32(3), Value::Int32(2)],
        ]
    );
}

fn insert_items_sql(range: std::ops::Range<i32>, note_prefix: &str) -> String {
    let values = range
        .map(|id| format!("({id}, '{note_prefix}{id}')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("insert into items values {values}")
}

fn delete_items_before_sql(upper_bound: i32) -> String {
    format!("delete from items where id < {upper_bound}")
}

fn session_query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match session.execute(db, sql).unwrap() {
        StatementResult::Query { rows, .. } => rows,
        other => panic!("expected query result, got {:?}", other),
    }
}

fn set_large_test_max_stack_depth(session: &mut Session, db: &Database, requested_kb: u32) {
    let safe_kb = crate::backend::utils::misc::stack_depth::max_stack_depth_limit_kb()
        .map(|limit_kb| requested_kb.min(limit_kb))
        .unwrap_or(requested_kb)
        .max(crate::backend::utils::misc::stack_depth::MIN_MAX_STACK_DEPTH_KB);
    session
        .execute(db, &format!("set max_stack_depth = '{safe_kb}kB'"))
        .unwrap();
}

#[test]
fn materialized_view_create_refresh_metadata_and_drop() {
    let db = Database::open_ephemeral(64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table mv_base(id int4, name text)")
        .unwrap();
    session
        .execute(&db, "insert into mv_base values (1, 'one'), (2, 'two')")
        .unwrap();
    session
        .execute(
            &db,
            "create materialized view mv_partial_alias(id_alias) as select id, name from mv_base",
        )
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select id_alias, name from mv_partial_alias order by id_alias"
        ),
        vec![
            vec![Value::Int32(1), Value::Text("one".into())],
            vec![Value::Int32(2), Value::Text("two".into())],
        ]
    );
    let too_many_aliases_err = session
        .execute(
            &db,
            "create materialized view mv_too_many_aliases(id, name, extra) as \
             select id, name from mv_base",
        )
        .unwrap_err();
    assert!(matches!(
        too_many_aliases_err,
        ExecError::Parse(ParseError::DetailedError {
            message,
            sqlstate: "42601",
            ..
        }) if message == "too many column names were specified"
    ));
    session
        .execute(
            &db,
            "create materialized view mv_items as select id, name from mv_base where id > 1",
        )
        .unwrap();

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select id, name from mv_items order by id"
        ),
        vec![vec![Value::Int32(2), Value::Text("two".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relkind::text, relispopulated from pg_class where relname = 'mv_items'",
        ),
        vec![vec![Value::Text("m".into()), Value::Bool(true)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select r.rulename from pg_rewrite r join pg_class c on c.oid = r.ev_class \
             where c.relname = 'mv_items' order by r.rulename",
        ),
        vec![vec![Value::Text("_RETURN".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_matviews where matviewname = 'mv_items'",
        ),
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select matviewname, ispopulated from pg_matviews where matviewname = 'mv_items'",
        ),
        vec![vec![Value::Text("mv_items".into()), Value::Bool(true)]]
    );

    session
        .execute(&db, "insert into mv_base values (3, 'three')")
        .unwrap();
    session
        .execute(&db, "refresh materialized view mv_items")
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select id, name from mv_items order by id"
        ),
        vec![
            vec![Value::Int32(2), Value::Text("two".into())],
            vec![Value::Int32(3), Value::Text("three".into())],
        ]
    );

    session
        .execute(&db, "create index on mv_items(id)")
        .unwrap();
    session.execute(&db, "analyze mv_items").unwrap();
    session.execute(&db, "vacuum mv_items").unwrap();

    let drop_table_err = session.execute(&db, "drop table mv_items").unwrap_err();
    assert!(matches!(
        drop_table_err,
        ExecError::Parse(ParseError::WrongObjectType {
            expected: "table",
            ..
        })
    ));
    session
        .execute(&db, "drop materialized view if exists mv_items")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_class where relname = 'mv_items'",
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn materialized_view_with_no_data_refreshes_and_rejects_writes() {
    let dir = temp_dir("matview_no_data");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table mv_no_data_base(id int4, name text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into mv_no_data_base values (1, 'one'), (2, 'two')",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create materialized view mv_no_data(id, name) as \
             select id, name from mv_no_data_base with no data",
        )
        .unwrap();

    let scan_err = session
        .execute(&db, "select id, name from mv_no_data")
        .unwrap_err();
    assert!(matches!(
        scan_err,
        ExecError::DetailedError {
            message,
            hint: Some(hint),
            sqlstate,
            ..
        } if message == "materialized view \"mv_no_data\" has not been populated"
            && hint == "Use the REFRESH MATERIALIZED VIEW command."
            && sqlstate == "55000"
    ));
    assert_eq!(
        query_rows(
            &db,
            1,
            "select ispopulated from pg_matviews where matviewname = 'mv_no_data'",
        ),
        vec![vec![Value::Bool(false)]]
    );

    session
        .execute(&db, "refresh materialized view mv_no_data")
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select id, name from mv_no_data order by id"
        ),
        vec![
            vec![Value::Int32(1), Value::Text("one".into())],
            vec![Value::Int32(2), Value::Text("two".into())],
        ]
    );

    let insert_err = session
        .execute(&db, "insert into mv_no_data values (3, 'three')")
        .unwrap_err();
    assert!(matches!(
        insert_err,
        ExecError::Parse(ParseError::FeatureNotSupportedMessage(message))
            if message == "cannot change materialized view \"mv_no_data\""
    ));
    let copy_path = dir.join("mv_no_data.tsv");
    fs::write(&copy_path, "3\tthree\n").unwrap();
    let copy_err = session
        .execute(
            &db,
            &format!("copy mv_no_data from '{}'", copy_path.display()),
        )
        .unwrap_err();
    assert!(matches!(
        copy_err,
        ExecError::Parse(ParseError::FeatureNotSupportedMessage(message))
            if message == "cannot change materialized view \"mv_no_data\""
    ));

    session
        .execute(&db, "refresh materialized view mv_no_data with no data")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relispopulated from pg_class where relname = 'mv_no_data'",
        ),
        vec![vec![Value::Bool(false)]]
    );

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "refresh materialized view mv_no_data")
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select id, name from mv_no_data order by id"
        ),
        vec![
            vec![Value::Int32(1), Value::Text("one".into())],
            vec![Value::Int32(2), Value::Text("two".into())],
        ]
    );
    session
        .execute(&db, "refresh materialized view mv_no_data with no data")
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select relispopulated from pg_class where relname = 'mv_no_data'",
        ),
        vec![vec![Value::Bool(false)]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select ispopulated from pg_matviews where matviewname = 'mv_no_data'",
        ),
        vec![vec![Value::Bool(false)]]
    );
    session.execute(&db, "rollback").unwrap();
}

#[test]
fn sql_cursor_fetch_move_close_and_cleanup() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);
    session
        .execute(&db, "create table cursor_items (id int4)")
        .unwrap();
    session
        .execute(&db, "insert into cursor_items values (1), (2), (3)")
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "declare c scroll cursor for select id from cursor_items order by id",
        )
        .unwrap();

    let cursors = session.cursor_view_rows();
    assert_eq!(cursors.len(), 1);
    assert_eq!(cursors[0].name, "c");
    assert!(cursors[0].is_scrollable);

    assert_eq!(
        session_query_rows(&mut session, &db, "fetch forward 2 from c"),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]
    );
    assert_eq!(
        session.execute(&db, "move forward from c").unwrap(),
        StatementResult::AffectedRows(1)
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "fetch prior from c"),
        vec![vec![Value::Int32(3)]]
    );

    session.execute(&db, "close c").unwrap();
    assert!(session.cursor_view_rows().is_empty());
    session.execute(&db, "commit").unwrap();
}

#[test]
fn holdable_cursor_survives_commit_but_normal_cursor_does_not() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);
    session
        .execute(&db, "create table hold_cursor_items (id int4)")
        .unwrap();
    session
        .execute(&db, "insert into hold_cursor_items values (1), (2)")
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "declare hold_c cursor with hold for select id from hold_cursor_items order by id",
        )
        .unwrap();
    session.execute(&db, "commit").unwrap();
    assert_eq!(
        session_query_rows(&mut session, &db, "fetch all from hold_c"),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]
    );
    session.execute(&db, "close hold_c").unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "declare no_hold_c cursor for select id from hold_cursor_items order by id",
        )
        .unwrap();
    session.execute(&db, "commit").unwrap();
    let err = session
        .execute(&db, "fetch all from no_hold_c")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::DetailedError {
            sqlstate: "34000",
            ..
        })
    ));
}

#[test]
fn standalone_listen_and_unlisten_update_subscriptions_immediately() {
    let db = Database::open_ephemeral(32).unwrap();

    db.execute(1, "listen alerts").unwrap();
    assert!(db.async_notify_runtime.is_listening(1, "alerts"));

    db.execute(1, "unlisten alerts").unwrap();
    assert!(!db.async_notify_runtime.is_listening(1, "alerts"));
}

fn create_plain_test_aggregates(db: &Database) {
    db.execute(
        1,
        "create aggregate newavg ( \
         sfunc = int4_avg_accum, basetype = int4, stype = _int8, \
         finalfunc = int8_avg, initcond1 = '{0,0}')",
    )
    .unwrap();
    db.execute(
        1,
        "create aggregate newsum ( \
         sfunc1 = int4pl, basetype = int4, stype1 = int4, \
         initcond1 = '0')",
    )
    .unwrap();
    db.execute(
        1,
        "create aggregate newcnt (*) ( \
         sfunc = int8inc, stype = int8, \
         initcond = '0', parallel = safe)",
    )
    .unwrap();
    db.execute(
        1,
        "create aggregate oldcnt ( \
         sfunc = int8inc, basetype = 'ANY', stype = int8, \
         initcond = '0')",
    )
    .unwrap();
    db.execute(
        1,
        "create aggregate newcnt (\"any\") ( \
         sfunc = int8inc_any, stype = int8, \
         initcond = '0')",
    )
    .unwrap();
    db.execute(
        1,
        "create function sum3(int8, int8, int8) returns int8 as \
         'select $1 + $2 + $3' language sql strict immutable",
    )
    .unwrap();
    db.execute(
        1,
        "create aggregate sum2(int8, int8) ( \
         sfunc = sum3, stype = int8, \
         initcond = '0')",
    )
    .unwrap();
}

fn create_plain_test_aggregate_inputs(db: &Database) {
    db.execute(1, "create table agg_input (four int4, q1 int8, q2 int8)")
        .unwrap();
    db.execute(
        1,
        "insert into agg_input values \
         (1, 10, 100), \
         (2, 1, 2), \
         (null, null, null), \
         (3, 5, 6)",
    )
    .unwrap();
}

fn visible_aggregate_row(
    db: &Database,
    client_id: ClientId,
    aggfnoid: u32,
) -> Option<PgAggregateRow> {
    db.backend_catcache(client_id, None)
        .unwrap()
        .aggregate_by_fnoid(aggfnoid)
        .cloned()
}

fn take_notice_messages() -> Vec<String> {
    take_notices()
        .into_iter()
        .map(|notice| notice.message)
        .collect()
}

fn take_backend_notice_messages() -> Vec<String> {
    take_backend_notices()
        .into_iter()
        .map(|notice| notice.message)
        .collect()
}

fn explain_lines(db: &Database, client_id: u32, sql: &str) -> Vec<String> {
    match db.execute(client_id, &format!("explain {sql}")).unwrap() {
        StatementResult::Query { rows, .. } => rows
            .into_iter()
            .map(|row| match row.first() {
                Some(Value::Text(text)) => text.to_string(),
                other => panic!("expected explain text row, got {:?}", other),
            })
            .collect(),
        other => panic!("expected query result, got {:?}", other),
    }
}

fn session_explain_lines(session: &mut Session, db: &Database, sql: &str) -> Vec<String> {
    match session.execute(db, &format!("explain {sql}")).unwrap() {
        StatementResult::Query { rows, .. } => rows
            .into_iter()
            .map(|row| match row.first() {
                Some(Value::Text(text)) => text.to_string(),
                other => panic!("expected explain text row, got {:?}", other),
            })
            .collect(),
        other => panic!("expected query result, got {:?}", other),
    }
}

fn explain_estimated_rows(db: &Database, client_id: u32, sql: &str) -> u64 {
    let first = explain_lines(db, client_id, sql)
        .into_iter()
        .next()
        .expect("expected explain output");
    let marker = " rows=";
    let start = first
        .find(marker)
        .map(|index| index + marker.len())
        .expect("expected rows marker in explain output");
    let end = first[start..]
        .find(' ')
        .map(|index| start + index)
        .expect("expected rows terminator in explain output");
    first[start..end]
        .parse()
        .expect("expected integer rows value")
}

#[test]
fn point_subscript_assignments_return_rows() {
    let dir = temp_dir("point_subscript_returning");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table point_tbl (f1 point)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into point_tbl values (null), ('(10,10)'::point)",
        )
        .unwrap();

    match session
        .execute(
            &db,
            "update point_tbl set f1[0] = 10 where f1 is null returning *",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["f1"]);
            assert_eq!(rows, vec![vec![Value::Null]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match session
        .execute(&db, "insert into point_tbl(f1[0]) values(0) returning *")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Null]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match session
        .execute(
            &db,
            "update point_tbl set f1[0] = NULL where f1::text = '(10,10)'::point::text returning *",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Point(crate::include::nodes::datum::GeoPoint {
                    x: 10.0,
                    y: 10.0,
                })]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match session
        .execute(
            &db,
            "update point_tbl set f1[0] = -10, f1[1] = -10 where f1::text = '(10,10)'::point::text returning *",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Point(crate::include::nodes::datum::GeoPoint {
                    x: -10.0,
                    y: -10.0,
                })]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match session.execute(
        &db,
        "update point_tbl set f1[3] = 10 where f1::text = '(-10,-10)'::point::text returning *",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(message, "array subscript out of range");
            assert_eq!(sqlstate, "2202E");
        }
        other => panic!("expected subscript error, got {other:?}"),
    }
}

#[test]
fn insert_and_update_returning_target_lists() {
    let dir = temp_dir("returning_target_lists");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create temp table returning_tbl (id int, name text, note text)",
        )
        .unwrap();

    match session
        .execute(
            &db,
            "insert into returning_tbl values (1, 'alice', 'x') returning id + 1 as next_id, name || note as combined, *",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["next_id", "combined", "id", "name", "note"]);
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(2),
                    Value::Text("alicex".into()),
                    Value::Int32(1),
                    Value::Text("alice".into()),
                    Value::Text("x".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match session
        .execute(
            &db,
            "update returning_tbl set name = 'bob', note = name || '!' where id = 1 returning returning_tbl.*, note, id + 10 as bumped_id",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["id", "name", "note", "note", "bumped_id"]);
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(1),
                    Value::Text("bob".into()),
                    Value::Text("alice!".into()),
                    Value::Text("alice!".into()),
                    Value::Int32(11),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn update_from_updates_rows_and_returns_source_columns() {
    let dir = temp_dir("update_from_returning");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table people (id int, name text)")
        .unwrap();
    session
        .execute(&db, "create table pets (owner_id int, name text)")
        .unwrap();
    session
        .execute(&db, "insert into people values (1, 'alice'), (2, 'bob')")
        .unwrap();
    session
        .execute(&db, "insert into pets values (1, 'fido'), (2, 'spot')")
        .unwrap();

    match session
        .execute(
            &db,
            "update people p set name = s.name from pets s where s.owner_id = p.id and p.id = 1 returning p.id, p.name, s.name",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["id", "name", "name"]);
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(1),
                    Value::Text("fido".into()),
                    Value::Text("fido".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    assert_eq!(
        session_query_rows(&mut session, &db, "select id, name from people order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("fido".into())],
            vec![Value::Int32(2), Value::Text("bob".into())],
        ]
    );
}

#[test]
fn update_from_updates_inherited_children() {
    let dir = temp_dir("update_from_inherited");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parent_inh (id int, name text)")
        .unwrap();
    session
        .execute(
            &db,
            "create table child_inh (extra int) inherits (parent_inh)",
        )
        .unwrap();
    session
        .execute(&db, "create table src_inh (id int, name text)")
        .unwrap();
    session
        .execute(&db, "insert into child_inh values (1, 'old', 7)")
        .unwrap();
    session
        .execute(&db, "insert into src_inh values (1, 'new')")
        .unwrap();

    assert!(matches!(
        session
            .execute(
                &db,
                "update parent_inh p set name = s.name from src_inh s where s.id = p.id",
            )
            .unwrap(),
        StatementResult::AffectedRows(1)
    ));
    assert_eq!(
        session_query_rows(&mut session, &db, "select id, name, extra from child_inh"),
        vec![vec![
            Value::Int32(1),
            Value::Text("new".into()),
            Value::Int32(7),
        ]]
    );
}

#[test]
fn update_from_duplicate_source_matches_only_updates_once() {
    let dir = temp_dir("update_from_duplicate_matches");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table target_dup (id int, note text)")
        .unwrap();
    session
        .execute(&db, "create table source_dup (id int, note text)")
        .unwrap();
    session
        .execute(&db, "insert into target_dup values (1, 'old')")
        .unwrap();
    session
        .execute(&db, "insert into source_dup values (1, 'a'), (1, 'b')")
        .unwrap();

    assert!(matches!(
        session
            .execute(
                &db,
                "update target_dup t set note = s.note from source_dup s where s.id = t.id",
            )
            .unwrap(),
        StatementResult::AffectedRows(1)
    ));

    let rows = session_query_rows(&mut session, &db, "select note from target_dup");
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        rows[0].first(),
        Some(Value::Text(text)) if text.as_str() == "a" || text.as_str() == "b"
    ));
}

#[test]
fn update_from_rejects_view_targets() {
    let dir = temp_dir("update_from_view_target");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table base_view_target (id int, name text)")
        .unwrap();
    session
        .execute(
            &db,
            "create view view_target as select * from base_view_target",
        )
        .unwrap();
    session
        .execute(&db, "create table src_view_target (id int, name text)")
        .unwrap();

    let err = session
        .execute(
            &db,
            "update view_target v set name = s.name from src_view_target s where s.id = v.id",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::FeatureNotSupportedMessage(message))
            if message == "UPDATE ... FROM is not yet supported for views"
    ));
}

#[test]
fn delete_returning_target_lists() {
    let dir = temp_dir("delete_returning_target_lists");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create temp table delete_returning_tbl (id int, name text, note text)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "insert into delete_returning_tbl values (1, 'alice', 'x'), (2, 'bob', 'y')",
        )
        .unwrap();

    match session
        .execute(
            &db,
            "delete from delete_returning_tbl where id = 1 returning delete_returning_tbl.*, id + 100 as deleted_id",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(
                column_names,
                vec!["id", "name", "note", "deleted_id"]
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(1),
                    Value::Text("alice".into()),
                    Value::Text("x".into()),
                    Value::Int32(101),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match session
        .execute(&db, "select * from delete_returning_tbl order by id")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(2),
                    Value::Text("bob".into()),
                    Value::Text("y".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn insert_on_conflict_returning_rows() {
    std::thread::Builder::new()
        .name("db-test-insert-on-conflict-returning".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let dir = temp_dir("insert_on_conflict_returning_rows");
            let db = Database::open(&dir, 128).unwrap();
            let mut session = Session::new(1);

            session
                .execute(
                    &db,
                    "create temp table upsert_returning_tbl (id int4 primary key, name text, note text)",
                )
                .unwrap();

            assert_eq!(
                session_query_rows(
                    &mut session,
                    &db,
                    "insert into upsert_returning_tbl values (1, 'alice', 'seed') returning id, name",
                ),
                vec![vec![Value::Int32(1), Value::Text("alice".into())]]
            );

            assert_eq!(
                session_query_rows(
                    &mut session,
                    &db,
                    "insert into upsert_returning_tbl values (1, 'bob', 'beta') on conflict do nothing returning id, name",
                ),
                Vec::<Vec<Value>>::new()
            );

            assert_eq!(
                session_query_rows(
                    &mut session,
                    &db,
                    "insert into upsert_returning_tbl values (1, 'carol', 'gamma') on conflict (id) do update set name = excluded.name, note = upsert_returning_tbl.note || excluded.note returning id, name, note",
                ),
                vec![vec![
                    Value::Int32(1),
                    Value::Text("carol".into()),
                    Value::Text("seedgamma".into()),
                ]]
            );

            assert_eq!(
                session_query_rows(
                    &mut session,
                    &db,
                    "insert into upsert_returning_tbl values (1, 'dave', 'delta') on conflict (id) do update set name = excluded.name where false returning id, name",
                ),
                Vec::<Vec<Value>>::new()
            );

            assert_eq!(
                query_rows(&db, 1, "select id, name, note from upsert_returning_tbl"),
                vec![vec![
                    Value::Int32(1),
                    Value::Text("carol".into()),
                    Value::Text("seedgamma".into()),
                ]]
            );
        })
        .unwrap()
        .join()
        .unwrap()
}

#[test]
fn copy_from_file_loads_tsvector_rows() {
    let dir = temp_dir("copy_from_file");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table docs (t text, a tsvector)")
        .unwrap();

    let copy_path = dir.join("docs.tsv");
    std::fs::write(&copy_path, "hello\tbar:2 foo:1\n").unwrap();

    let sql = format!("copy docs from '{}'", copy_path.display());
    match session.execute(&db, &sql).unwrap() {
        StatementResult::AffectedRows(count) => assert_eq!(count, 1),
        other => panic!("expected affected rows, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select t, a from docs"),
        vec![vec![
            Value::Text("hello".into()),
            Value::TsVector(
                crate::include::nodes::tsearch::TsVector::parse("bar:2 foo:1").unwrap()
            ),
        ]]
    );
}

#[test]
fn copy_to_file_writes_selected_rows() {
    let dir = temp_dir("copy_to_file");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int, name text)")
        .unwrap();
    session
        .execute(&db, "insert into items values (1, 'alice'), (2, null)")
        .unwrap();

    let copy_path = dir.join("items.tsv");
    let sql = format!("copy items (id, name) to '{}'", copy_path.display());
    match session.execute(&db, &sql).unwrap() {
        StatementResult::AffectedRows(count) => assert_eq!(count, 2),
        other => panic!("expected affected rows, got {other:?}"),
    }
    assert_eq!(fs::read_to_string(copy_path).unwrap(), "1\talice\n2\t\\N\n");
}

#[test]
fn copy_to_dml_without_returning_has_no_side_effects() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int)")
        .expect("create table");
    let err = session
        .execute(&db, "copy (insert into items values (1)) to stdout")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::FeatureNotSupportedMessage(message))
            if message == "COPY query must have a RETURNING clause"
    ));
    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn copy_to_relative_file_rejects_before_query_execution() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int)")
        .expect("create table");
    let err = session
        .execute(
            &db,
            "copy (insert into items values (1) returning id) to 'relative.out'",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::DetailedError { message, .. })
            if message == "relative path not allowed for COPY to file"
    ));
    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn text_search_catalogs_are_bootstrapped() {
    let dir = temp_dir("text_search_catalogs");
    let db = Database::open(&dir, 64).unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select prsname, prsstart, prstoken, prsend, prslextype from pg_ts_parser order by oid",
        ),
        vec![vec![
            Value::Text("default".into()),
            Value::Int64(3717),
            Value::Int64(3718),
            Value::Int64(3719),
            Value::Int64(3721),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select dictname, dicttemplate, dictinitoption from pg_ts_dict order by oid",
        ),
        vec![vec![
            Value::Text("simple".into()),
            Value::Int64(3727),
            Value::Null,
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select tmplname, tmplinit, tmpllexize from pg_ts_template order by oid",
        ),
        vec![
            vec![
                Value::Text("simple".into()),
                Value::Int64(3725),
                Value::Int64(3726),
            ],
            vec![
                Value::Text("synonym".into()),
                Value::Int64(3728),
                Value::Int64(3729),
            ],
            vec![
                Value::Text("ispell".into()),
                Value::Int64(3731),
                Value::Int64(3732),
            ],
            vec![
                Value::Text("thesaurus".into()),
                Value::Int64(3740),
                Value::Int64(3741),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select cfgname, cfgparser from pg_ts_config order by oid",
        ),
        vec![vec![Value::Text("simple".into()), Value::Int64(3722)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from pg_ts_config_map"),
        vec![vec![Value::Int64(19)]]
    );
}

fn relfilenode_for(db: &Database, client_id: u32, relname: &str) -> i64 {
    let rows = query_rows(
        db,
        client_id,
        &format!("select relfilenode from pg_class where relname = '{relname}'"),
    );
    match rows.as_slice() {
        [row] => match row.first() {
            Some(Value::Int32(value)) => i64::from(*value),
            Some(Value::Int64(value)) => *value,
            other => panic!("expected relfilenode integer, got {:?}", other),
        },
        other => panic!("expected one relfilenode row, got {:?}", other),
    }
}

fn relation_oid_for(db: &Database, client_id: u32, relname: &str) -> i64 {
    let rows = query_rows(
        db,
        client_id,
        &format!("select oid from pg_class where relname = '{relname}'"),
    );
    match rows.as_slice() {
        [row] => match row.first() {
            Some(Value::Int32(value)) => i64::from(*value),
            Some(Value::Int64(value)) => *value,
            other => panic!("expected relation oid integer, got {:?}", other),
        },
        other => panic!("expected one relation oid row, got {:?}", other),
    }
}

fn int_value(value: &Value) -> i64 {
    match value {
        Value::Int16(value) => i64::from(*value),
        Value::Int32(value) => i64::from(*value),
        Value::Int64(value) => *value,
        other => panic!("expected integer value, got {:?}", other),
    }
}

fn float_value(value: &Value) -> f64 {
    match value {
        Value::Float64(value) => *value,
        other => panic!("expected float value, got {:?}", other),
    }
}

fn typed_text_array_value(values: &[&str], element_type_oid: u32) -> Value {
    Value::PgArray(
        crate::include::nodes::datum::ArrayValue::from_1d(
            values
                .iter()
                .map(|value| Value::Text((*value).into()))
                .collect(),
        )
        .with_element_type_oid(element_type_oid),
    )
}

fn relation_locator_for(db: &Database, client_id: u32, relname: &str) -> crate::RelFileLocator {
    crate::RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: relfilenode_for(db, client_id, relname) as u32,
    }
}

fn wait_for_pg_lock_row<F>(db: &Database, timeout: Duration, predicate: F) -> Vec<Value>
where
    F: Fn(&[Value]) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(row) = db
            .pg_locks_rows()
            .into_iter()
            .find(|row| predicate(row.as_slice()))
        {
            return row;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for pg_locks row");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn statement_timeout_interrupts_generate_series_query() {
    let dir = temp_dir("statement_timeout_generate_series");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "set statement_timeout = '5ms'")
        .unwrap();

    let err = session
        .execute(&db, "select * from generate_series(1, 1000000000)")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Interrupted(
            crate::backend::utils::misc::interrupts::InterruptReason::StatementTimeout
        )
    ));
}

#[test]
fn statement_timeout_interrupts_recursive_cte_query() {
    let dir = temp_dir("statement_timeout_recursive_cte");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "set statement_timeout = '5ms'")
        .unwrap();

    let err = session
        .execute(
            &db,
            "with recursive t(n) as (select 1 union all select n + 1 from t) select * from t",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Interrupted(
            crate::backend::utils::misc::interrupts::InterruptReason::StatementTimeout
        )
    ));
}

#[test]
fn statement_timeout_interrupts_waiting_tuple_update() {
    let dir = temp_dir("statement_timeout_waiting_update");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder.execute(&db, "create table t (id int)").unwrap();
    holder.execute(&db, "insert into t values (1)").unwrap();

    holder.execute(&db, "begin").unwrap();
    holder
        .execute(&db, "update t set id = 2 where id = 1")
        .unwrap();

    waiter
        .execute(&db, "set statement_timeout = '20ms'")
        .unwrap();
    let err = waiter
        .execute(&db, "update t set id = 3 where id = 1")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Interrupted(
            crate::backend::utils::misc::interrupts::InterruptReason::StatementTimeout
        )
    ));

    holder.execute(&db, "rollback").unwrap();
}

#[test]
fn statement_timeout_interrupts_unique_index_conflict_wait() {
    let dir = temp_dir("statement_timeout_unique_wait");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder.execute(&db, "create table t (id int)").unwrap();
    holder
        .execute(&db, "create unique index t_id_idx on t(id)")
        .unwrap();

    holder.execute(&db, "begin").unwrap();
    holder.execute(&db, "insert into t values (1)").unwrap();

    waiter
        .execute(&db, "set statement_timeout = '20ms'")
        .unwrap();
    let err = waiter.execute(&db, "insert into t values (1)").unwrap_err();
    assert!(
        matches!(
            err,
            ExecError::Interrupted(
                crate::backend::utils::misc::interrupts::InterruptReason::StatementTimeout
            )
        ),
        "unexpected error: {err:?}"
    );

    holder.execute(&db, "rollback").unwrap();
}

#[test]
fn disconnect_cleanup_aborts_open_transaction_and_releases_table_locks() {
    let dir = temp_dir("disconnect_cleanup_table_locks");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder.execute(&db, "create table t (id int)").unwrap();

    holder.execute(&db, "begin").unwrap();
    holder
        .execute(&db, "comment on table t is 'held by disconnected session'")
        .unwrap();
    assert!(db.table_locks.has_locks_for_client(1));

    holder.cleanup_on_disconnect(&db);
    assert!(!db.table_locks.has_locks_for_client(1));
    let snapshot = db
        .txns
        .read()
        .snapshot(crate::backend::access::transam::xact::INVALID_TRANSACTION_ID)
        .unwrap();
    assert_eq!(snapshot.xmin, snapshot.xmax);

    waiter.execute(&db, "set statement_timeout = '1s'").unwrap();
    match waiter.execute(&db, "select count(*) from t").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn advisory_session_and_transaction_locks_cleanup_and_encode_keys() {
    let dir = temp_dir("advisory_session_xact_cleanup");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "select pg_advisory_lock(4294967298)")
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select classid, objid, objsubid, pid, mode, granted, fastpath \
             from pg_locks where locktype = 'advisory'"
        ),
        vec![vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Int16(1),
            Value::Int32(1),
            Value::Text("ExclusiveLock".into()),
            Value::Bool(true),
            Value::Bool(false),
        ]]
    );

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "select pg_advisory_xact_lock(11, 22)")
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select classid, objid, objsubid, granted \
             from pg_locks where locktype = 'advisory' order by objsubid, classid, objid"
        ),
        vec![
            vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Int16(1),
                Value::Bool(true),
            ],
            vec![
                Value::Int64(11),
                Value::Int64(22),
                Value::Int16(2),
                Value::Bool(true),
            ],
        ]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_advisory_unlock(11, 22)"),
        vec![vec![Value::Bool(false)]]
    );

    session
        .execute(&db, "select pg_advisory_unlock_all()")
        .unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select classid, objid, objsubid \
             from pg_locks where locktype = 'advisory'"
        ),
        vec![vec![Value::Int64(11), Value::Int64(22), Value::Int16(2)]]
    );

    session.execute(&db, "commit").unwrap();
    assert!(
        session_query_rows(
            &mut session,
            &db,
            "select locktype from pg_locks where locktype = 'advisory'"
        )
        .is_empty()
    );

    session.execute(&db, "select pg_advisory_lock(9)").unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "select pg_advisory_xact_lock(10)")
        .unwrap();
    session.execute(&db, "rollback").unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select classid, objid, objsubid \
             from pg_locks where locktype = 'advisory'"
        ),
        vec![vec![Value::Int64(0), Value::Int64(9), Value::Int16(1)]]
    );

    session
        .execute(&db, "select pg_advisory_unlock_all()")
        .unwrap();
    assert!(
        session_query_rows(
            &mut session,
            &db,
            "select locktype from pg_locks where locktype = 'advisory'"
        )
        .is_empty()
    );
}

#[test]
fn pg_locks_pg_lock_status_return_same_advisory_rows() {
    let dir = temp_dir("pg_locks_pg_lock_status_advisory_rows");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "select pg_advisory_lock(4294967298), pg_advisory_lock_shared(7, 8)",
        )
        .unwrap();

    let select_list = "classid, objid, objsubid, pid, mode, granted, fastpath, waitstart is null";
    let order_by = "order by objsubid, classid, objid";
    let from_view = session_query_rows(
        &mut session,
        &db,
        &format!("select {select_list} from pg_locks where locktype = 'advisory' {order_by}"),
    );
    let from_catalog_view = session_query_rows(
        &mut session,
        &db,
        &format!(
            "select {select_list} from pg_catalog.pg_locks \
             where locktype = 'advisory' {order_by}"
        ),
    );
    let from_srf = session_query_rows(
        &mut session,
        &db,
        &format!(
            "select {select_list} from pg_catalog.pg_lock_status() \
             where locktype = 'advisory' {order_by}"
        ),
    );

    assert_eq!(from_view, from_catalog_view);
    assert_eq!(from_view, from_srf);

    session
        .execute(&db, "select pg_advisory_unlock_all()")
        .unwrap();
}

#[test]
fn pg_locks_pg_lock_status_virtualxid_includes_current_backend() {
    let dir = temp_dir("pg_locks_pg_lock_status_virtualxid");
    let db = Database::open(&dir, 64).unwrap();

    assert_eq!(
        query_rows(
            &db,
            7,
            "select count(*) from pg_lock_status() \
             where locktype = 'virtualxid' and pid = pg_backend_pid()"
        ),
        vec![vec![Value::Int64(1)]]
    );
}

#[test]
fn advisory_try_lock_shared_and_reentrant_counts_match_postgres() {
    let dir = temp_dir("advisory_try_lock_shared_reentrant");
    let db = Database::open(&dir, 64).unwrap();
    let mut first = Session::new(1);
    let mut second = Session::new(2);
    let mut third = Session::new(3);

    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_try_advisory_lock(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_try_advisory_lock(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut second, &db, "select pg_try_advisory_lock_shared(7)"),
        vec![vec![Value::Bool(false)]]
    );
    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_advisory_unlock(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut second, &db, "select pg_try_advisory_lock_shared(7)"),
        vec![vec![Value::Bool(false)]]
    );
    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_advisory_unlock(7)"),
        vec![vec![Value::Bool(true)]]
    );

    assert_eq!(
        session_query_rows(&mut second, &db, "select pg_try_advisory_lock_shared(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut third, &db, "select pg_try_advisory_lock_shared(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_try_advisory_lock(7)"),
        vec![vec![Value::Bool(false)]]
    );

    assert_eq!(
        session_query_rows(&mut second, &db, "select pg_advisory_unlock_shared(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_try_advisory_lock(7)"),
        vec![vec![Value::Bool(false)]]
    );
    assert_eq!(
        session_query_rows(&mut third, &db, "select pg_advisory_unlock_shared(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_try_advisory_lock(7)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut first, &db, "select pg_advisory_unlock(7)"),
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn advisory_session_and_xact_locks_on_same_key_do_not_block_same_backend() {
    let dir = temp_dir("advisory_same_backend_cross_scope");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "select pg_advisory_xact_lock(1), pg_advisory_xact_lock_shared(2), \
             pg_advisory_xact_lock(1, 1), pg_advisory_xact_lock_shared(2, 2)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "select pg_advisory_lock(1), pg_advisory_lock_shared(2), \
             pg_advisory_lock(1, 1), pg_advisory_lock_shared(2, 2)",
        )
        .unwrap();
    session.execute(&db, "rollback").unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select classid, objid, objsubid, mode, granted \
             from pg_locks where locktype = 'advisory' order by classid, objid, objsubid"
        ),
        vec![
            vec![
                Value::Int64(0),
                Value::Int64(1),
                Value::Int16(1),
                Value::Text("ExclusiveLock".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Int64(0),
                Value::Int64(2),
                Value::Int16(1),
                Value::Text("ShareLock".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Int64(1),
                Value::Int64(1),
                Value::Int16(2),
                Value::Text("ExclusiveLock".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Int64(2),
                Value::Int64(2),
                Value::Int16(2),
                Value::Text("ShareLock".into()),
                Value::Bool(true),
            ],
        ]
    );

    session
        .execute(&db, "select pg_advisory_unlock_all()")
        .unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "select pg_advisory_lock(1), pg_advisory_lock_shared(2), \
             pg_advisory_lock(1, 1), pg_advisory_lock_shared(2, 2)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "select pg_advisory_xact_lock(1), pg_advisory_xact_lock_shared(2), \
             pg_advisory_xact_lock(1, 1), pg_advisory_xact_lock_shared(2, 2)",
        )
        .unwrap();
    session.execute(&db, "rollback").unwrap();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select count(*) from pg_locks where locktype = 'advisory'"
        ),
        vec![vec![Value::Int64(4)]]
    );
}

#[test]
fn advisory_unlock_false_queues_warning() {
    let dir = temp_dir("advisory_unlock_warning");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "select pg_advisory_xact_lock(1), pg_advisory_xact_lock_shared(2)",
        )
        .unwrap();

    clear_backend_notices();
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select pg_advisory_unlock(1), pg_advisory_unlock_shared(2)"
        ),
        vec![vec![Value::Bool(false), Value::Bool(false)]]
    );
    assert_eq!(
        take_backend_notice_messages(),
        vec![
            "you don't own a lock of type ExclusiveLock".to_string(),
            "you don't own a lock of type ShareLock".to_string(),
        ]
    );
}

#[test]
fn advisory_lock_functions_return_null_for_null_inputs() {
    let dir = temp_dir("advisory_lock_null_inputs");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select \
                pg_try_advisory_lock(null::bigint), \
                pg_advisory_lock(null::bigint), \
                pg_advisory_unlock(null::bigint), \
                pg_try_advisory_xact_lock_shared(null::int4, 1)"
        ),
        vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]]
    );
}

#[test]
fn statement_timeout_interrupts_waiting_advisory_lock() {
    let dir = temp_dir("statement_timeout_waiting_advisory_lock");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder.execute(&db, "select pg_advisory_lock(44)").unwrap();
    waiter
        .execute(&db, "set statement_timeout = '20ms'")
        .unwrap();

    let err = waiter
        .execute(&db, "select pg_advisory_lock(44)")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Interrupted(
            crate::backend::utils::misc::interrupts::InterruptReason::StatementTimeout
        )
    ));

    holder
        .execute(&db, "select pg_advisory_unlock(44)")
        .unwrap();
}

#[test]
fn advisory_waiters_appear_in_pg_locks_with_waitstart() {
    use std::sync::mpsc;

    let dir = temp_dir("advisory_waiters_pg_locks");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder.execute(&db, "select pg_advisory_lock(55)").unwrap();
    db.install_interrupt_state(2, waiter.interrupts());

    let db2 = db.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        done_tx
            .send(waiter.execute(&db2, "select pg_advisory_xact_lock(55)"))
            .unwrap();
    });

    let waiting_row = wait_for_pg_lock_row(&db, TEST_TIMEOUT, |row| {
        matches!(row.first(), Some(Value::Text(locktype)) if locktype.as_str() == "advisory")
            && row.get(11) == Some(&Value::Int32(2))
            && row.get(13) == Some(&Value::Bool(false))
    });
    assert_eq!(waiting_row[12], Value::Text("ExclusiveLock".into()));
    assert!(!matches!(waiting_row[15], Value::Null));
    assert_eq!(
        query_rows(
            &db,
            3,
            "select pid, mode, granted, waitstart is not null \
             from pg_locks where locktype = 'advisory' and pid = 2"
        ),
        vec![vec![
            Value::Int32(2),
            Value::Text("ExclusiveLock".into()),
            Value::Bool(false),
            Value::Bool(true),
        ]]
    );

    holder
        .execute(&db, "select pg_advisory_unlock(55)")
        .unwrap();
    match done_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Null]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
    worker.join().unwrap();
    assert!(
        query_rows(
            &db,
            3,
            "select locktype from pg_locks where locktype = 'advisory' and pid = 2"
        )
        .is_empty()
    );
}

#[test]
fn advisory_lock_waits_can_be_canceled_explicitly() {
    use std::sync::mpsc;

    let dir = temp_dir("advisory_cancel_wait");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder.execute(&db, "select pg_advisory_lock(66)").unwrap();
    db.install_interrupt_state(2, waiter.interrupts());

    let db2 = db.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        done_tx
            .send(waiter.execute(&db2, "select pg_advisory_lock(66)"))
            .unwrap();
    });

    wait_for_pg_lock_row(&db, TEST_TIMEOUT, |row| {
        matches!(row.first(), Some(Value::Text(locktype)) if locktype.as_str() == "advisory")
            && row.get(11) == Some(&Value::Int32(2))
            && row.get(13) == Some(&Value::Bool(false))
    });
    db.interrupt_state(2)
        .set_pending(crate::backend::utils::misc::interrupts::InterruptReason::QueryCancel);

    let err = done_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap_err();
    assert!(matches!(
        err,
        ExecError::Interrupted(
            crate::backend::utils::misc::interrupts::InterruptReason::QueryCancel
        )
    ));
    holder
        .execute(&db, "select pg_advisory_unlock(66)")
        .unwrap();
    worker.join().unwrap();
}

#[test]
fn autocommit_xact_advisory_locks_release_after_statement_and_streaming_guard() {
    let dir = temp_dir("autocommit_xact_advisory_locks");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "select pg_advisory_xact_lock(77)")
        .unwrap();
    assert!(
        query_rows(
            &db,
            2,
            "select locktype from pg_locks where locktype = 'advisory' and pid = 1"
        )
        .is_empty()
    );

    let stmt = crate::backend::parser::parse_select("select pg_advisory_xact_lock(88), 1").unwrap();
    let mut guard = session.execute_streaming(&db, &stmt).unwrap();
    let slot = crate::backend::executor::exec_next(&mut guard.state, &mut guard.ctx)
        .unwrap()
        .expect("streaming row");
    let values = slot.values().unwrap();
    assert_eq!(values[1].to_owned_value(), Value::Int32(1));

    assert_eq!(
        query_rows(
            &db,
            2,
            "select pid, granted from pg_locks where locktype = 'advisory' and pid = 1"
        ),
        vec![vec![Value::Int32(1), Value::Bool(true)]]
    );

    drop(guard);
    assert!(
        query_rows(
            &db,
            2,
            "select locktype from pg_locks where locktype = 'advisory' and pid = 1"
        )
        .is_empty()
    );
}

#[test]
fn disconnect_cleanup_releases_advisory_locks() {
    let dir = temp_dir("disconnect_cleanup_advisory_locks");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder.execute(&db, "select pg_advisory_lock(91)").unwrap();
    holder.execute(&db, "begin").unwrap();
    holder
        .execute(&db, "select pg_advisory_xact_lock(92)")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            3,
            "select count(*) from pg_locks where locktype = 'advisory' and pid = 1"
        ),
        vec![vec![Value::Int64(2)]]
    );

    holder.cleanup_on_disconnect(&db);
    assert!(
        query_rows(
            &db,
            3,
            "select locktype from pg_locks where locktype = 'advisory' and pid = 1"
        )
        .is_empty()
    );

    assert_eq!(
        session_query_rows(&mut waiter, &db, "select pg_try_advisory_lock(91)"),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut waiter, &db, "select pg_try_advisory_lock(92)"),
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn pg_locks_shows_granted_and_waiting_relation_locks() {
    use std::sync::mpsc;

    let dir = temp_dir("pg_locks_relation_rows");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);

    holder.execute(&db, "create table t (id int)").unwrap();
    let relation_oid = relation_oid_for(&db, 1, "t");

    holder.execute(&db, "begin").unwrap();
    holder
        .execute(&db, "comment on table t is 'held relation lock'")
        .unwrap();

    let db2 = db.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut waiter = Session::new(2);
        done_tx
            .send(waiter.execute(&db2, "select count(*) from t"))
            .unwrap();
    });

    let waiting_row = wait_for_pg_lock_row(&db, TEST_TIMEOUT, |row| {
        matches!(row.first(), Some(Value::Text(locktype)) if locktype.as_str() == "relation")
            && row.get(2) == Some(&Value::Int64(relation_oid))
            && row.get(11) == Some(&Value::Int32(2))
            && row.get(13) == Some(&Value::Bool(false))
    });
    assert_eq!(waiting_row[12], Value::Text("AccessShareLock".into()));
    assert!(!matches!(waiting_row[15], Value::Null));

    let relation_rows = query_rows(
        &db,
        3,
        &format!(
            "select pid, mode, granted, waitstart is not null \
             from pg_locks where locktype = 'relation' and relation = {relation_oid} \
             order by pid, granted desc"
        ),
    );
    assert!(relation_rows.contains(&vec![
        Value::Int32(1),
        Value::Text("AccessExclusiveLock".into()),
        Value::Bool(true),
        Value::Bool(false),
    ]));
    assert!(relation_rows.contains(&vec![
        Value::Int32(2),
        Value::Text("AccessShareLock".into()),
        Value::Bool(false),
        Value::Bool(true),
    ]));
    assert_eq!(
        relation_rows,
        query_rows(
            &db,
            3,
            &format!(
                "select pid, mode, granted, waitstart is not null \
                 from pg_lock_status() where locktype = 'relation' and relation = {relation_oid} \
                 order by pid, granted desc"
            ),
        )
    );

    holder.execute(&db, "rollback").unwrap();
    match done_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
    worker.join().unwrap();
}

#[test]
fn pg_locks_reports_tuple_granted_and_waiting_rows() {
    use std::sync::mpsc;

    let dir = temp_dir("pg_locks_tuple_rows");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder
        .execute(
            &db,
            "create table tuple_lock_items (id int4 not null primary key)",
        )
        .unwrap();
    holder
        .execute(&db, "insert into tuple_lock_items values (1)")
        .unwrap();
    let relation_oid = relation_oid_for(&db, 1, "tuple_lock_items");

    holder.execute(&db, "begin").unwrap();
    assert_eq!(
        session_query_rows(
            &mut holder,
            &db,
            "select id from tuple_lock_items where id = 1 for update"
        ),
        vec![vec![Value::Int32(1)]]
    );

    db.install_interrupt_state(2, waiter.interrupts());
    let db2 = db.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        done_tx
            .send(waiter.execute(
                &db2,
                "select id from tuple_lock_items where id = 1 for share",
            ))
            .unwrap();
    });

    let waiting_row = wait_for_pg_lock_row(&db, TEST_TIMEOUT, |row| {
        matches!(row.first(), Some(Value::Text(locktype)) if locktype.as_str() == "tuple")
            && row.get(2) == Some(&Value::Int64(relation_oid))
            && row.get(11) == Some(&Value::Int32(2))
            && row.get(13) == Some(&Value::Bool(false))
    });
    assert_eq!(waiting_row[12], Value::Text("RowShareLock".into()));
    assert!(!matches!(waiting_row[15], Value::Null));

    let tuple_rows = query_rows(
        &db,
        3,
        &format!(
            "select pid, mode, granted, waitstart is not null \
             from pg_lock_status() where locktype = 'tuple' and relation = {relation_oid} \
             order by pid, granted desc"
        ),
    );
    assert!(tuple_rows.contains(&vec![
        Value::Int32(1),
        Value::Text("AccessExclusiveLock".into()),
        Value::Bool(true),
        Value::Bool(false),
    ]));
    assert!(tuple_rows.contains(&vec![
        Value::Int32(2),
        Value::Text("RowShareLock".into()),
        Value::Bool(false),
        Value::Bool(true),
    ]));

    holder.execute(&db, "rollback").unwrap();
    match done_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
    worker.join().unwrap();
    assert!(
        query_rows(
            &db,
            3,
            "select locktype from pg_lock_status() where locktype = 'tuple'"
        )
        .is_empty()
    );
}

#[test]
fn pg_locks_reports_transactionid_holders_waiters_and_cleanup() {
    use std::sync::mpsc;

    let dir = temp_dir("pg_locks_transactionid_rows");
    let db = Database::open(&dir, 64).unwrap();
    let mut holder = Session::new(1);
    let mut waiter = Session::new(2);

    holder
        .execute(
            &db,
            "create table transaction_lock_items (id int4 not null primary key)",
        )
        .unwrap();
    holder
        .execute(&db, "insert into transaction_lock_items values (1)")
        .unwrap();

    holder.execute(&db, "begin").unwrap();
    holder
        .execute(&db, "update transaction_lock_items set id = 2 where id = 1")
        .unwrap();

    db.install_interrupt_state(2, waiter.interrupts());
    let db2 = db.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        done_tx
            .send(waiter.execute(
                &db2,
                "update transaction_lock_items set id = 3 where id = 1",
            ))
            .unwrap();
    });

    let waiting_row = wait_for_pg_lock_row(&db, TEST_TIMEOUT, |row| {
        matches!(row.first(), Some(Value::Text(locktype)) if locktype.as_str() == "transactionid")
            && row.get(11) == Some(&Value::Int32(2))
            && row.get(13) == Some(&Value::Bool(false))
    });
    assert_eq!(waiting_row[12], Value::Text("ShareLock".into()));
    assert!(!matches!(waiting_row[15], Value::Null));

    let transaction_rows = query_rows(
        &db,
        3,
        "select pid, mode, granted, waitstart is not null \
         from pg_lock_status() where locktype = 'transactionid' order by pid, granted desc",
    );
    assert!(transaction_rows.contains(&vec![
        Value::Int32(1),
        Value::Text("ExclusiveLock".into()),
        Value::Bool(true),
        Value::Bool(false),
    ]));
    assert!(transaction_rows.contains(&vec![
        Value::Int32(2),
        Value::Text("ShareLock".into()),
        Value::Bool(false),
        Value::Bool(true),
    ]));

    holder.execute(&db, "rollback").unwrap();
    match done_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap() {
        StatementResult::AffectedRows(1) => {}
        other => panic!("expected update result, got {other:?}"),
    }
    worker.join().unwrap();
    assert!(
        query_rows(
            &db,
            3,
            "select locktype from pg_lock_status() where locktype = 'transactionid' \
             and pid in (1, 2)"
        )
        .is_empty()
    );
}

#[test]
fn pg_locks_includes_advisory_rows_from_other_open_databases() {
    let base = temp_dir("pg_locks_other_open_database");
    let cluster = Cluster::open(&base, 16).unwrap();
    let postgres = cluster.connect_database("postgres").unwrap();
    let mut admin = Session::new(1);

    admin
        .execute(&postgres, "create database analytics")
        .unwrap();
    let analytics = cluster.connect_database("analytics").unwrap();
    let mut analytics_session = Session::new(2);
    analytics_session
        .execute(&analytics, "select pg_advisory_lock(1234)")
        .unwrap();

    assert_eq!(
        query_rows(
            &postgres,
            1,
            "select \"database\", pid, mode, granted \
             from pg_locks where locktype = 'advisory' and pid = 2"
        ),
        vec![vec![
            Value::Int64(i64::from(analytics.database_oid)),
            Value::Int32(2),
            Value::Text("ExclusiveLock".into()),
            Value::Bool(true),
        ]]
    );

    analytics_session.cleanup_on_disconnect(&analytics);
}

#[test]
fn analyze_populates_pg_statistic_and_pg_class_stats() {
    let dir = temp_dir("analyze_populates_stats");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table analyze_t(a int4, b text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into analyze_t values
               (1, 'one'),
               (1, 'one'),
               (2, 'two'),
               (null, null),
               (3, 'three')",
        )
        .unwrap();

    match session.execute(&db, "analyze analyze_t").unwrap() {
        StatementResult::AffectedRows(count) => assert_eq!(count, 0),
        other => panic!("expected affected rows, got {other:?}"),
    }

    let rel_stats = query_rows(
        &db,
        1,
        "select relpages, reltuples from pg_class where relname = 'analyze_t'",
    );
    assert_eq!(rel_stats.len(), 1);
    assert!(int_value(&rel_stats[0][0]) >= 1);
    assert!(float_value(&rel_stats[0][1]) >= 4.0);

    let column_stats = query_rows(
        &db,
        1,
        "select staattnum, stanullfrac, stawidth, stadistinct
         from pg_statistic
         where starelid = (select oid from pg_class where relname = 'analyze_t')
         order by staattnum",
    );
    assert_eq!(column_stats.len(), 2);
    assert_eq!(int_value(&column_stats[0][0]), 1);
    assert_eq!(int_value(&column_stats[1][0]), 2);
    assert!(float_value(&column_stats[0][1]) > 0.0);
    assert!(int_value(&column_stats[0][2]) > 0);
    assert!(float_value(&column_stats[0][3]).abs() > 0.0);
}

#[test]
fn analyze_in_explicit_transaction_reports_stats_only_on_commit() {
    let dir = temp_dir("analyze_xact_stats_commit");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table analyze_xact_t(a int4)")
        .unwrap();
    session
        .execute(&db, "insert into analyze_xact_t values (1), (2), (3)")
        .unwrap();
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_force_next_flush()"),
        vec![vec![Value::Null]]
    );

    session.execute(&db, "begin").unwrap();
    session.execute(&db, "analyze analyze_xact_t").unwrap();
    session.execute(&db, "rollback").unwrap();

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select analyze_count, last_analyze is null, n_mod_since_analyze
             from pg_stat_user_tables
             where relname = 'analyze_xact_t'",
        ),
        vec![vec![Value::Int64(0), Value::Bool(true), Value::Int64(3)]]
    );

    session.execute(&db, "begin").unwrap();
    session.execute(&db, "analyze analyze_xact_t").unwrap();
    session.execute(&db, "commit").unwrap();
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_force_next_flush()"),
        vec![vec![Value::Null]]
    );

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select analyze_count, last_analyze is not null, n_mod_since_analyze
             from pg_stat_user_tables
             where relname = 'analyze_xact_t'",
        ),
        vec![vec![Value::Int64(1), Value::Bool(true), Value::Int64(0)]]
    );
}

#[test]
fn vacuum_populates_pg_class_visibility_stats() {
    let dir = temp_dir("vacuum_populates_visibility_stats");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create table vac_t(a int4)").unwrap();
    session
        .execute(&db, "insert into vac_t values (1), (2), (3), (4), (5), (6)")
        .unwrap();

    match session.execute(&db, "vacuum vac_t").unwrap() {
        StatementResult::AffectedRows(count) => assert_eq!(count, 0),
        other => panic!("expected affected rows, got {other:?}"),
    }

    let rows = query_rows(
        &db,
        1,
        "select relpages, relallvisible, relallfrozen, relfrozenxid
         from pg_class
         where relname = 'vac_t'",
    );
    assert_eq!(rows.len(), 1);
    assert!(int_value(&rows[0][0]) >= 1);
    assert!(int_value(&rows[0][1]) >= 1);
    assert!(int_value(&rows[0][2]) >= 1);
    assert_eq!(
        int_value(&rows[0][3]),
        crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID as i64
    );
}

#[test]
fn vacuum_analyze_updates_visibility_and_analyze_stats() {
    let dir = temp_dir("vacuum_analyze_updates_stats");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table vac_analyze_t(a int4, b text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into vac_analyze_t values
               (1, 'one'),
               (1, 'one'),
               (2, 'two'),
               (null, null),
               (3, 'three')",
        )
        .unwrap();

    match session
        .execute(&db, "vacuum analyze vac_analyze_t")
        .unwrap()
    {
        StatementResult::AffectedRows(count) => assert_eq!(count, 0),
        other => panic!("expected affected rows, got {other:?}"),
    }

    let rel_stats = query_rows(
        &db,
        1,
        "select relpages, reltuples, relallvisible, relallfrozen
         from pg_class
         where relname = 'vac_analyze_t'",
    );
    assert_eq!(rel_stats.len(), 1);
    assert!(int_value(&rel_stats[0][0]) >= 1);
    assert!(float_value(&rel_stats[0][1]) >= 4.0);
    assert!(int_value(&rel_stats[0][2]) >= 1);
    assert!(int_value(&rel_stats[0][3]) >= 1);

    let statistic_count = query_rows(
        &db,
        1,
        "select count(*)
         from pg_statistic
         where starelid = (select oid from pg_class where relname = 'vac_analyze_t')",
    );
    assert_eq!(statistic_count, vec![vec![Value::Int64(2)]]);
}

fn autovacuum_test_config() -> AutovacuumConfig {
    AutovacuumConfig {
        enabled: false,
        naptime: Duration::from_secs(3600),
        vacuum_threshold: 1,
        vacuum_max_threshold: 1_000,
        vacuum_scale_factor: 0.0,
        vacuum_insert_threshold: 1_000_000,
        vacuum_insert_scale_factor: 0.0,
        analyze_threshold: 1_000_000,
        analyze_scale_factor: 0.0,
        freeze_max_age: 200_000_000,
    }
}

#[test]
fn autovacuum_once_vacuums_dead_user_table() {
    let dir = temp_dir("autovacuum_once_vacuum");
    let db = Database::open_with_options(
        &dir,
        DatabaseOpenOptions::for_tests(128).with_autovacuum_config(autovacuum_test_config()),
    )
    .unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table av_items(id int4)")
        .unwrap();
    session
        .execute(&db, "insert into av_items values (1), (2), (3), (4), (5)")
        .unwrap();
    session
        .execute(&db, "delete from av_items where id <= 3")
        .unwrap();

    db.run_autovacuum_once().unwrap();

    let stats = query_rows(
        &db,
        1,
        "select autovacuum_count, last_autovacuum is not null, n_dead_tup, n_ins_since_vacuum
         from pg_stat_user_tables
         where relname = 'av_items'",
    );
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0][0], Value::Int64(1));
    assert_eq!(stats[0][1], Value::Bool(true));
    assert_eq!(stats[0][2], Value::Int64(0));
    assert_eq!(stats[0][3], Value::Int64(0));

    let rel_stats = query_rows(
        &db,
        1,
        "select relallvisible, relallfrozen
         from pg_class
         where relname = 'av_items'",
    );
    assert_eq!(rel_stats.len(), 1);
    assert!(int_value(&rel_stats[0][0]) >= 1);
    assert!(int_value(&rel_stats[0][1]) >= 1);
}

#[test]
fn autovacuum_once_autoanalyzes_modified_user_table() {
    let dir = temp_dir("autovacuum_once_analyze");
    let mut config = autovacuum_test_config();
    config.vacuum_threshold = 1_000_000;
    config.analyze_threshold = 1;
    let db = Database::open_with_options(
        &dir,
        DatabaseOpenOptions::for_tests(128).with_autovacuum_config(config),
    )
    .unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table av_analyze_items(id int4, note text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into av_analyze_items values
             (1, 'one'),
             (2, 'two'),
             (3, 'three')",
        )
        .unwrap();

    db.run_autovacuum_once().unwrap();

    let stats = query_rows(
        &db,
        1,
        "select autoanalyze_count, last_autoanalyze is not null, n_mod_since_analyze
         from pg_stat_user_tables
         where relname = 'av_analyze_items'",
    );
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0][0], Value::Int64(1));
    assert_eq!(stats[0][1], Value::Bool(true));
    assert_eq!(stats[0][2], Value::Int64(0));

    let statistic_count = query_rows(
        &db,
        1,
        "select count(*)
         from pg_statistic
         where starelid = (select oid from pg_class where relname = 'av_analyze_items')",
    );
    assert_eq!(statistic_count, vec![vec![Value::Int64(2)]]);
}

#[test]
fn autovacuum_once_skips_locked_table_without_blocking() {
    let dir = temp_dir("autovacuum_skip_locked");
    let db = Database::open_with_options(
        &dir,
        DatabaseOpenOptions::for_tests(128).with_autovacuum_config(autovacuum_test_config()),
    )
    .unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table av_locked_items(id int4)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into av_locked_items values (1), (2), (3), (4), (5)",
        )
        .unwrap();
    session
        .execute(&db, "delete from av_locked_items where id <= 3")
        .unwrap();

    let rel = relation_locator_for(&db, 1, "av_locked_items");
    db.table_locks.lock_table(
        rel,
        crate::backend::storage::lmgr::TableLockMode::RowExclusive,
        99,
    );
    let result = db.run_autovacuum_once();
    db.table_locks.unlock_table(rel, 99);
    result.unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select autovacuum_count, n_dead_tup
             from pg_stat_user_tables
             where relname = 'av_locked_items'",
        ),
        vec![vec![Value::Int64(0), Value::Int64(3)]]
    );
}

#[test]
fn analyze_without_targets_scans_permitted_heap_relations() {
    let dir = temp_dir("analyze_permitted_relations");
    let db = Database::open(&dir, 128).unwrap();
    let mut bootstrap = Session::new(1);
    let mut session = Session::new(2);

    bootstrap
        .execute(&db, "create role analyze_owner login")
        .unwrap();
    bootstrap
        .execute(&db, "create schema hidden authorization analyze_owner")
        .unwrap();
    bootstrap
        .execute(&db, "create table public.not_owned(id int4)")
        .unwrap();
    bootstrap
        .execute(&db, "insert into public.not_owned values (99)")
        .unwrap();

    session
        .execute(&db, "set session authorization analyze_owner")
        .unwrap();
    session
        .execute(&db, "create table visible_a(id int4)")
        .unwrap();
    session
        .execute(&db, "create table hidden.hidden_c(id int4)")
        .unwrap();
    session
        .execute(&db, "create table visible_b(id int4)")
        .unwrap();
    session
        .execute(&db, "insert into visible_a values (1), (2)")
        .unwrap();
    session
        .execute(&db, "insert into visible_b values (3)")
        .unwrap();
    session
        .execute(&db, "insert into hidden.hidden_c values (4)")
        .unwrap();

    match session.execute(&db, "analyze").unwrap() {
        StatementResult::AffectedRows(count) => assert_eq!(count, 0),
        other => panic!("expected affected rows, got {other:?}"),
    }

    let visible_a = query_rows(
        &db,
        1,
        "select relpages, reltuples from pg_class
         where relname = 'visible_a'
           and relnamespace = (select oid from pg_namespace where nspname = 'public')",
    );
    assert_eq!(visible_a.len(), 1);
    assert!(int_value(&visible_a[0][0]) >= 1);
    assert!(float_value(&visible_a[0][1]) >= 2.0);

    let visible_b = query_rows(
        &db,
        1,
        "select relpages, reltuples from pg_class
         where relname = 'visible_b'
           and relnamespace = (select oid from pg_namespace where nspname = 'public')",
    );
    assert_eq!(visible_b.len(), 1);
    assert!(int_value(&visible_b[0][0]) >= 1);
    assert!(float_value(&visible_b[0][1]) >= 1.0);

    let hidden_c = query_rows(
        &db,
        1,
        "select relpages, reltuples from pg_class
         where relname = 'hidden_c'
           and relnamespace = (select oid from pg_namespace where nspname = 'hidden')",
    );
    assert_eq!(hidden_c.len(), 1);
    assert!(int_value(&hidden_c[0][0]) >= 1);
    assert!(float_value(&hidden_c[0][1]) >= 1.0);

    let not_owned = query_rows(
        &db,
        1,
        "select relpages, reltuples from pg_class
         where relname = 'not_owned'
           and relnamespace = (select oid from pg_namespace where nspname = 'public')",
    );
    assert_eq!(not_owned.len(), 1);
    assert_eq!(int_value(&not_owned[0][0]), 0);
    assert_eq!(float_value(&not_owned[0][1]), -1.0);
}

#[test]
fn collect_analyze_stats_accepts_materialized_view_relkind() {
    let dir = temp_dir("analyze_matview_relkind");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table analyze_matview(a int4, b text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into analyze_matview values (1, 'one'), (2, 'two')",
        )
        .unwrap();

    let xid = db.txns.write().begin();
    let cid = 0;
    let base = db.lazy_catalog_lookup(1, Some((xid, cid)), None);
    let visible_catalog = base.materialize_visible_catalog();
    let relation = base.lookup_any_relation("analyze_matview").unwrap();
    let catalog = AnalyzeRelkindOverrideCatalog {
        inner: base,
        relkind_overrides: HashMap::from([(relation.relation_oid, 'm')]),
    };
    let mut ctx = analyze_executor_context(&db, 1, xid, cid, visible_catalog);

    let stats = collect_analyze_stats(
        &[MaintenanceTarget {
            table_name: "analyze_matview".into(),
            columns: Vec::new(),
            only: false,
        }],
        &catalog,
        &mut ctx,
    )
    .unwrap();

    assert_eq!(stats.len(), 1);
    assert!(stats[0].relpages >= 1);
    assert!(stats[0].reltuples >= 2.0);
    assert!(!stats[0].statistics.is_empty());
    assert!(stats[0].statistics.iter().all(|row| !row.stainherit));

    db.txns.write().abort(xid).unwrap();
}

#[test]
fn collect_analyze_stats_treats_partitioned_relkind_as_inherited_only() {
    let dir = temp_dir("analyze_partitioned_relkind");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table analyze_parent(a int4, b text)")
        .unwrap();
    session
        .execute(
            &db,
            "create table analyze_child() inherits (analyze_parent)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "insert into analyze_child values (1, 'one'), (2, 'two'), (3, 'three')",
        )
        .unwrap();

    let xid = db.txns.write().begin();
    let cid = 0;
    let base = db.lazy_catalog_lookup(1, Some((xid, cid)), None);
    let visible_catalog = base.materialize_visible_catalog();
    let relation = base.lookup_any_relation("analyze_parent").unwrap();
    let catalog = AnalyzeRelkindOverrideCatalog {
        inner: base,
        relkind_overrides: HashMap::from([(relation.relation_oid, 'p')]),
    };
    let mut ctx = analyze_executor_context(&db, 1, xid, cid, visible_catalog);

    let stats = collect_analyze_stats(
        &[MaintenanceTarget {
            table_name: "analyze_parent".into(),
            columns: Vec::new(),
            only: false,
        }],
        &catalog,
        &mut ctx,
    )
    .unwrap();

    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].relpages, -1);
    assert_eq!(stats[0].reltuples, 3.0);
    assert!(!stats[0].statistics.is_empty());
    assert!(stats[0].statistics.iter().all(|row| row.stainherit));

    db.txns.write().abort(xid).unwrap();
}

#[test]
fn drop_table_drops_partitioned_roots_and_subpartitioned_children() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table orders(id int4, region text) partition by list (region)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table orders_eu partition of orders for values in ('eu') partition by range (id)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table orders_eu_small partition of orders_eu for values from (minvalue) to (100)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table orders_eu_large partition of orders_eu for values from (100) to (maxvalue)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table orders_us partition of orders for values in ('us')",
        )
        .unwrap();
    session
        .execute(
            &db,
            "insert into orders values (1, 'eu'), (150, 'eu'), (5, 'us')",
        )
        .unwrap();

    session.execute(&db, "drop table orders_eu").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, region from orders order by id"),
        vec![vec![Value::Int32(5), Value::Text("us".into())]]
    );
    assert!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relname in ('orders_eu', 'orders_eu_small', 'orders_eu_large')",
        )
        .is_empty()
    );

    session.execute(&db, "drop table orders").unwrap();

    assert!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relname like 'orders%'",
        )
        .is_empty()
    );
}

#[test]
fn partition_tuple_routing_handles_nested_and_default_partitions() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table route_orders(id int4, region text) partition by list (region)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table route_orders_eu partition of route_orders for values in ('eu') partition by range (id)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table route_orders_eu_low partition of route_orders_eu for values from (minvalue) to (100)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table route_orders_eu_high partition of route_orders_eu for values from (100) to (maxvalue)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table route_orders_other partition of route_orders default",
        )
        .unwrap();
    session
        .execute(
            &db,
            "insert into route_orders values (1, 'eu'), (150, 'eu'), (5, 'us'), (6, 'apac')",
        )
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select count(*) from route_orders_eu_low"),
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from route_orders_eu_high"),
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from route_orders_other"),
        vec![vec![Value::Int64(2)]]
    );

    match session.execute(&db, "insert into route_orders_eu_low values (250, 'eu')") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message
            == "new row for relation \"route_orders_eu_low\" violates partition constraint"
            && sqlstate == "23514" => {}
        other => panic!("expected partition constraint violation, got {other:?}"),
    }
}

#[test]
fn hash_partitioned_tables_route_rows_and_validate_bounds() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table hp (a int4, payload text) partition by hash (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table hp0 partition of hp for values with (modulus 2, remainder 0)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table hp1 partition of hp for values with (modulus 2, remainder 1)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "insert into hp values (1, 'one'), (2, 'two'), (3, 'three'), (null, 'nil')",
        )
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select count(*) from hp"),
        vec![vec![Value::Int64(4)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select partstrat::text from pg_partitioned_table where partrelid = 'hp'::regclass",
        ),
        vec![vec![Value::Text("h".into())]]
    );

    match session.execute(&db, "create table hp_default partition of hp default") {
        Err(ExecError::Parse(ParseError::DetailedError {
            message, sqlstate, ..
        })) if message == "a hash-partitioned table may not have a default partition"
            && sqlstate == "42P17" => {}
        other => panic!("expected hash default partition rejection, got {other:?}"),
    }

    match session.execute(
        &db,
        "create table hp_bad partition of hp for values with (modulus 3, remainder 0)",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message
            == "every hash partition modulus must be a factor of the next larger modulus"
            && sqlstate == "42P17" => {}
        other => panic!("expected hash modulus factor rejection, got {other:?}"),
    }
}

#[test]
fn enable_partitionwise_join_explains_append_of_child_joins() {
    let dir = temp_dir("partitionwise_join_explain");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);
    set_large_test_max_stack_depth(&mut session, &db, 32 * 1024);
    for sql in [
        "create table pwj_l (k int4, v int4) partition by range (k)",
        "create table pwj_l1 partition of pwj_l for values from (0) to (10)",
        "create table pwj_l2 partition of pwj_l for values from (10) to (20)",
        "create table pwj_r (k int4, v int4) partition by range (k)",
        "create table pwj_r1 partition of pwj_r for values from (0) to (10)",
        "create table pwj_r2 partition of pwj_r for values from (10) to (20)",
        "set enable_partitionwise_join = on",
    ] {
        session.execute(&db, sql).unwrap();
    }

    for sql in [
        "select * from pwj_l join pwj_r on pwj_l.k = pwj_r.k",
        "select * from pwj_l left join pwj_r on pwj_l.k = pwj_r.k",
        "select * from pwj_l full join pwj_r on pwj_l.k = pwj_r.k",
    ] {
        let lines = session_explain_lines(&mut session, &db, sql);
        let rendered = lines.join("\n");
        assert!(
            rendered.contains("Append"),
            "expected partitionwise append in explain for {sql}, got:\n{rendered}"
        );
        assert!(
            rendered.matches("Join").count() >= 2,
            "expected child joins under append for {sql}, got:\n{rendered}"
        );
    }
}

#[test]
fn partition_keys_accept_collations_opclasses_and_expressions() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table coll_pruning (a text collate \"C\") partition by list (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table coll_pruning_a partition of coll_pruning for values in ('a')",
        )
        .unwrap();
    session
        .execute(&db, "insert into coll_pruning values ('a')")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attcollation from pg_attribute where attrelid = 'coll_pruning'::regclass and attname = 'a'",
        ),
        vec![vec![Value::Int64(i64::from(
            crate::include::catalog::C_COLLATION_OID
        ))]]
    );

    session
        .execute(
            &db,
            "create table rlp3 (b varchar, a int) partition by list (b varchar_ops)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table rlp3_a partition of rlp3 for values in ('a')",
        )
        .unwrap();
    session
        .execute(&db, "insert into rlp3 values ('a', 1)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select count(*) from rlp3_a"),
        vec![vec![Value::Int64(1)]]
    );

    session
        .execute(
            &db,
            "create table mc3p (a int, b int, c int) partition by range (a, abs(b), c)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table mc3p0 partition of mc3p for values from (minvalue, minvalue, minvalue) to (1, 1, 1)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table mc3p1 partition of mc3p for values from (1, 1, 1) to (2, 2, 2)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table mc3p2 partition of mc3p for values from (2, 2, 2) to (maxvalue, maxvalue, maxvalue)",
        )
        .unwrap();
    session
        .execute(&db, "insert into mc3p values (1, -1, 1)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select count(*) from mc3p1"),
        vec![vec![Value::Int64(1)]]
    );
    let lines = explain_lines(&db, 1, "select * from mc3p where a = 1 and abs(b) < 1");
    assert!(
        lines.iter().any(|line| line.contains("Seq Scan on mc3p0")),
        "expected multi-key expression pruning to keep mc3p0, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .all(|line| !line.contains("Seq Scan on mc3p1") && !line.contains("Seq Scan on mc3p2")),
        "expected multi-key expression pruning to remove mc3p1/mc3p2, got {lines:?}"
    );
}

#[test]
fn hash_partitioning_uses_custom_opclass_support_proc() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create function part_hashint4_noop(value int4, seed int8) returns int8 as $$ select value + seed; $$ language sql strict immutable parallel safe",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create operator class part_test_int4_ops for type int4 using hash as operator 1 =, function 2 part_hashint4_noop(int4, int8)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table hp_custom (a int4) partition by hash (a part_test_int4_ops)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table hp_custom_0 partition of hp_custom for values with (modulus 2, remainder 0)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table hp_custom_1 partition of hp_custom for values with (modulus 2, remainder 1)",
        )
        .unwrap();
    session
        .execute(&db, "insert into hp_custom values (2), (3)")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select a from hp_custom_0 order by a"),
        vec![vec![Value::Int32(2)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select a from hp_custom_1 order by a"),
        vec![vec![Value::Int32(3)]]
    );
}

#[test]
fn partitioned_primary_keys_support_rename_flow_and_index_tree_metadata() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table part_attmp (a int primary key) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table part_attmp1 partition of part_attmp for values from (0) to (10)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "alter index part_attmp_pkey rename to part_attmp_pkey_renamed",
        )
        .unwrap();
    session
        .execute(&db, "alter table part_attmp rename to part_attmp_renamed")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relkind::text from pg_class where relname = 'part_attmp_pkey_renamed'",
        ),
        vec![vec![Value::Text("I".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select child.relname, parent.relname \
               from pg_inherits i \
               join pg_class child on child.oid = i.inhrelid \
               join pg_class parent on parent.oid = i.inhparent \
              where parent.relname = 'part_attmp_pkey_renamed'",
        ),
        vec![vec![
            Value::Text("part_attmp1_pkey".into()),
            Value::Text("part_attmp_pkey_renamed".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relname = 'part_attmp_renamed'",
        ),
        vec![vec![Value::Text("part_attmp_renamed".into())]]
    );
}

#[test]
fn partitioned_primary_keys_preserve_deferrability_flags() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table part_items (id int4 primary key deferrable initially deferred) partition by range (id)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table part_items_1 partition of part_items for values from (0) to (10)",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select rel.relname, con.condeferrable, con.condeferred \
               from pg_constraint con \
               join pg_class rel on rel.oid = con.conrelid \
              where rel.relname in ('part_items', 'part_items_1') \
                and con.contype = 'p' \
              order by rel.relname",
        ),
        vec![
            vec![
                Value::Text("part_items".into()),
                Value::Bool(true),
                Value::Bool(true),
            ],
            vec![
                Value::Text("part_items_1".into()),
                Value::Bool(true),
                Value::Bool(true),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select c.relname, i.indimmediate \
               from pg_index i \
               join pg_class c on c.oid = i.indexrelid \
              where c.relname in ('part_items_pkey', 'part_items_1_pkey') \
              order by c.relname",
        ),
        vec![
            vec![Value::Text("part_items_1_pkey".into()), Value::Bool(false)],
            vec![Value::Text("part_items_pkey".into()), Value::Bool(false)],
        ]
    );
}

#[test]
fn create_index_on_partitioned_table_builds_index_tree() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table idxpart (a int4, b int4) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table idxpart1 partition of idxpart for values from (0) to (10)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table idxpart2 partition of idxpart for values from (10) to (20) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table idxpart2a partition of idxpart2 for values from (10) to (15)",
        )
        .unwrap();

    session.execute(&db, "create index on idxpart(a)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname, relkind::text, relhassubclass \
               from pg_class \
              where relname in ('idxpart_a_idx', 'idxpart1_a_idx', 'idxpart2_a_idx', 'idxpart2a_a_idx') \
              order by relname",
        ),
        vec![
            vec![
                Value::Text("idxpart1_a_idx".into()),
                Value::Text("i".into()),
                Value::Bool(false),
            ],
            vec![
                Value::Text("idxpart2_a_idx".into()),
                Value::Text("I".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Text("idxpart2a_a_idx".into()),
                Value::Text("i".into()),
                Value::Bool(false),
            ],
            vec![
                Value::Text("idxpart_a_idx".into()),
                Value::Text("I".into()),
                Value::Bool(true),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select child.relname, parent.relname \
               from pg_inherits i \
               join pg_class child on child.oid = i.inhrelid \
               join pg_class parent on parent.oid = i.inhparent \
              where parent.relname in ('idxpart_a_idx', 'idxpart2_a_idx') \
              order by parent.relname, child.relname",
        ),
        vec![
            vec![
                Value::Text("idxpart2a_a_idx".into()),
                Value::Text("idxpart2_a_idx".into()),
            ],
            vec![
                Value::Text("idxpart1_a_idx".into()),
                Value::Text("idxpart_a_idx".into()),
            ],
            vec![
                Value::Text("idxpart2_a_idx".into()),
                Value::Text("idxpart_a_idx".into()),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select indexdef from pg_indexes where tablename = 'idxpart' and indexname = 'idxpart_a_idx'",
        ),
        vec![vec![Value::Text(
            "CREATE INDEX idxpart_a_idx ON ONLY public.idxpart USING btree (a)".into(),
        )]]
    );
}

#[test]
fn create_index_on_partitioned_table_reuses_only_child_without_recursing() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    fn relation_oid(db: &Database, name: &str) -> u32 {
        db.lazy_catalog_lookup(1, None, None)
            .lookup_any_relation(name)
            .unwrap_or_else(|| panic!("expected relation {name}"))
            .relation_oid
    }

    fn index_summary(db: &Database, index_name: &str) -> (u32, Option<u32>, bool) {
        let catalog = db.lazy_catalog_lookup(1, None, None);
        let index_relation = catalog
            .lookup_any_relation(index_name)
            .unwrap_or_else(|| panic!("expected index {index_name}"));
        let index = crate::backend::utils::cache::lsyscache::describe_relation_by_oid(
            db,
            1,
            None,
            index_relation.relation_oid,
        )
        .unwrap_or_else(|| panic!("expected relcache entry for {index_name}"))
        .index
        .unwrap_or_else(|| panic!("expected index metadata for {index_name}"));
        let parent_oid = catalog
            .inheritance_parents(index_relation.relation_oid)
            .first()
            .map(|row| row.inhparent);
        (index.indrelid, parent_oid, index.indisvalid)
    }

    session
        .execute(&db, "create table idxpart (a int4) partition by range (a)")
        .unwrap();
    session
        .execute(
            &db,
            "create table idxpart1 partition of idxpart for values from (0) to (100)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table idxpart2 partition of idxpart for values from (100) to (1000) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table idxpart21 partition of idxpart2 for values from (100) to (200)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table idxpart22 partition of idxpart2 for values from (200) to (300)",
        )
        .unwrap();
    session
        .execute(&db, "create index on idxpart22(a)")
        .unwrap();
    session
        .execute(&db, "create index on only idxpart2(a)")
        .unwrap();
    session.execute(&db, "create index on idxpart(a)").unwrap();

    assert_eq!(
        index_summary(&db, "idxpart1_a_idx"),
        (
            relation_oid(&db, "idxpart1"),
            Some(relation_oid(&db, "idxpart_a_idx")),
            true,
        )
    );
    assert_eq!(
        index_summary(&db, "idxpart22_a_idx"),
        (relation_oid(&db, "idxpart22"), None, true)
    );
    assert_eq!(
        index_summary(&db, "idxpart2_a_idx"),
        (
            relation_oid(&db, "idxpart2"),
            Some(relation_oid(&db, "idxpart_a_idx")),
            false,
        )
    );
    assert_eq!(
        index_summary(&db, "idxpart_a_idx"),
        (relation_oid(&db, "idxpart"), None, false)
    );
    assert!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relname = 'idxpart21_a_idx'",
        )
        .is_empty()
    );

    session
        .execute(
            &db,
            "alter index idxpart2_a_idx attach partition idxpart22_a_idx",
        )
        .unwrap();
    session
        .execute(
            &db,
            "alter index idxpart2_a_idx attach partition idxpart22_a_idx",
        )
        .unwrap();
    assert!(!index_summary(&db, "idxpart2_a_idx").2);
    assert!(!index_summary(&db, "idxpart_a_idx").2);

    session
        .execute(&db, "create index on idxpart21(a)")
        .unwrap();
    session
        .execute(
            &db,
            "alter index idxpart2_a_idx attach partition idxpart21_a_idx",
        )
        .unwrap();
    assert!(index_summary(&db, "idxpart21_a_idx").2);
    assert!(index_summary(&db, "idxpart22_a_idx").2);
    assert!(index_summary(&db, "idxpart2_a_idx").2);
    assert!(index_summary(&db, "idxpart_a_idx").2);
}

#[test]
fn partitioned_index_only_and_future_partition_reconciliation() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table onlypart (a int4, b int4) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table onlypart1 partition of onlypart for values from (0) to (10)",
        )
        .unwrap();
    session
        .execute(&db, "create index on only onlypart(a)")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relkind::text, relhassubclass from pg_class where relname = 'onlypart_a_idx'",
        ),
        vec![vec![Value::Text("I".into()), Value::Bool(false)]]
    );
    assert!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relname = 'onlypart1_a_idx'"
        )
        .is_empty()
    );

    session
        .execute(
            &db,
            "create table futpart (a int4, b int4) partition by range (a)",
        )
        .unwrap();
    session.execute(&db, "create index on futpart(a)").unwrap();
    session
        .execute(
            &db,
            "create table futpart1 partition of futpart for values from (0) to (10)",
        )
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select child.relname, parent.relname \
               from pg_inherits i \
               join pg_class child on child.oid = i.inhrelid \
               join pg_class parent on parent.oid = i.inhparent \
              where parent.relname = 'futpart_a_idx'",
        ),
        vec![vec![
            Value::Text("futpart1_a_idx".into()),
            Value::Text("futpart_a_idx".into()),
        ]]
    );
}

#[test]
fn partitioned_index_reuses_explicit_child_and_supports_attach_drop() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table preidx (a int4, b int4) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table preidx1 partition of preidx for values from (0) to (10)",
        )
        .unwrap();
    session.execute(&db, "create index on preidx1(a)").unwrap();
    session.execute(&db, "create index on preidx(a)").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_class where relname like 'preidx1_a_idx%'",
        ),
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select child.relname, parent.relname \
               from pg_inherits i \
               join pg_class child on child.oid = i.inhrelid \
               join pg_class parent on parent.oid = i.inhparent \
              where parent.relname = 'preidx_a_idx'",
        ),
        vec![vec![
            Value::Text("preidx1_a_idx".into()),
            Value::Text("preidx_a_idx".into()),
        ]]
    );

    session
        .execute(
            &db,
            "create table attachidx (a int4, b int4) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table attachidx1 partition of attachidx for values from (0) to (10)",
        )
        .unwrap();
    session
        .execute(&db, "create index on only attachidx(a)")
        .unwrap();
    session
        .execute(&db, "create index on attachidx1(a)")
        .unwrap();
    session
        .execute(
            &db,
            "alter index attachidx_a_idx attach partition attachidx1_a_idx",
        )
        .unwrap();

    let err = session
        .execute(&db, "drop index attachidx1_a_idx")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, hint: Some(hint), sqlstate, .. }
            if message.contains("requires it")
                && hint == "You can drop index attachidx_a_idx instead."
                && sqlstate == "2BP01"
    ));
    session.execute(&db, "drop index attachidx_a_idx").unwrap();
    assert!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relname in ('attachidx_a_idx', 'attachidx1_a_idx')",
        )
        .is_empty()
    );
}

#[test]
fn partitioned_key_coverage_checks_fire_for_root_partition_of_and_attach_partition() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    match session.execute(
        &db,
        "create table miss_root (a int, b int, primary key (a)) partition by range (b)",
    ) {
        Err(ExecError::Parse(ParseError::DetailedError {
            message,
            detail: Some(detail),
            sqlstate,
            ..
        })) if message
            == "unique constraint on partitioned table must include all partitioning columns"
            && detail
                == "PRIMARY KEY constraint on table \"miss_root\" lacks column \"b\" which is part of the partition key."
            && sqlstate == "0A000" => {}
        other => panic!("expected root partition-key coverage error, got {other:?}"),
    }

    session
        .execute(
            &db,
            "create table dup_parent (a int primary key) partition by range (a)",
        )
        .unwrap();
    match session.execute(
        &db,
        "create table dup_parent_child partition of dup_parent (primary key (a)) for values from (0) to (10)",
    ) {
        Err(ExecError::Parse(ParseError::DetailedError {
            message,
            sqlstate,
            ..
        })) if message == "multiple primary keys for table \"dup_parent_child\" are not allowed"
            && sqlstate == "42P16" => {}
        other => panic!("expected duplicate primary-key rejection, got {other:?}"),
    }

    session
        .execute(
            &db,
            "create table sub_parent (a int, b int, primary key (a)) partition by range (a)",
        )
        .unwrap();
    match session.execute(
        &db,
        "create table sub_parent_child partition of sub_parent for values from (0) to (10) partition by range (b)",
    ) {
        Err(ExecError::Parse(ParseError::DetailedError {
            message,
            detail: Some(detail),
            sqlstate,
            ..
        })) if message
            == "unique constraint on partitioned table must include all partitioning columns"
            && detail
                == "PRIMARY KEY constraint on table \"sub_parent_child\" lacks column \"b\" which is part of the partition key."
            && sqlstate == "0A000" => {}
        other => panic!("expected PARTITION OF coverage error, got {other:?}"),
    }

    session
        .execute(
            &db,
            "create table attach_parent_cov (a int, b int, primary key (a)) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table attach_child_cov (a int, b int) partition by range (b)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table attach_child_cov_1 partition of attach_child_cov for values from (minvalue) to (10)",
        )
        .unwrap();
    match session.execute(
        &db,
        "alter table attach_parent_cov attach partition attach_child_cov for values from (0) to (10)",
    ) {
        Err(ExecError::Parse(ParseError::DetailedError {
            message,
            detail: Some(detail),
            sqlstate,
            ..
        })) if message
            == "unique constraint on partitioned table must include all partitioning columns"
            && detail
                == "PRIMARY KEY constraint on table \"attach_child_cov\" lacks column \"b\" which is part of the partition key."
            && sqlstate == "0A000" => {}
        other => panic!("expected ATTACH PARTITION coverage error, got {other:?}"),
    }
}

#[test]
fn alter_table_add_primary_key_builds_or_attaches_partition_key_trees() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table addpk_empty (a int) partition by range (a)",
        )
        .unwrap();
    session
        .execute(&db, "alter table addpk_empty add primary key (a)")
        .unwrap();

    session
        .execute(
            &db,
            "create table addpk_tree (a int) partition by range (a)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table addpk_tree_1 partition of addpk_tree for values from (0) to (10)",
        )
        .unwrap();
    session
        .execute(&db, "alter table addpk_tree add primary key (a)")
        .unwrap();

    session
        .execute(
            &db,
            "create table addpk_attach (a int) partition by range (a)",
        )
        .unwrap();
    session
        .execute(&db, "create table addpk_child (a int primary key)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table addpk_attach attach partition addpk_child for values from (0) to (10)",
        )
        .unwrap();
    session
        .execute(&db, "alter table addpk_attach add primary key (a)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname, relkind::text \
               from pg_class \
              where relname in ('addpk_empty_pkey', 'addpk_tree_pkey', 'addpk_attach_pkey') \
              order by relname",
        ),
        vec![
            vec![
                Value::Text("addpk_attach_pkey".into()),
                Value::Text("I".into()),
            ],
            vec![
                Value::Text("addpk_empty_pkey".into()),
                Value::Text("I".into()),
            ],
            vec![
                Value::Text("addpk_tree_pkey".into()),
                Value::Text("I".into()),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select child.relname, parent.relname \
               from pg_inherits i \
               join pg_class child on child.oid = i.inhrelid \
               join pg_class parent on parent.oid = i.inhparent \
              where parent.relname in ('addpk_tree_pkey', 'addpk_attach_pkey') \
              order by parent.relname, child.relname",
        ),
        vec![
            vec![
                Value::Text("addpk_child_pkey".into()),
                Value::Text("addpk_attach_pkey".into()),
            ],
            vec![
                Value::Text("addpk_tree_1_pkey".into()),
                Value::Text("addpk_tree_pkey".into()),
            ],
        ]
    );
}

#[test]
fn attach_partition_creates_missing_keys_and_fk_to_partitioned_key_stays_rejected() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table attach_parent (a int primary key) partition by range (a)",
        )
        .unwrap();
    session
        .execute(&db, "create table attach_child (a int)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table attach_parent attach partition attach_child for values from (0) to (10)",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select child.relname, parent.relname \
               from pg_inherits i \
               join pg_class child on child.oid = i.inhrelid \
               join pg_class parent on parent.oid = i.inhparent \
              where parent.relname = 'attach_parent_pkey'",
        ),
        vec![vec![
            Value::Text("attach_child_pkey".into()),
            Value::Text("attach_parent_pkey".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_constraint \
              where conrelid = 'attach_parent'::regclass \
                and contype::text = 'n'",
        ),
        vec![vec![Value::Int64(1)]]
    );

    match session.execute(
        &db,
        "create table fk_to_partitioned_parent (a int references attach_parent(a))",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(message)))
            if message == "REFERENCES to partitioned tables" => {}
        other => panic!("expected FK-to-partitioned-table rejection, got {other:?}"),
    }
}

#[test]
fn drop_table_still_rejects_legacy_inheritance_parents() {
    let db = Database::open_ephemeral(32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parent_inh(a int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create table child_inh(extra int4) inherits (parent_inh)",
        )
        .unwrap();

    match session.execute(&db, "drop table parent_inh") {
        Err(ExecError::DetailedError {
            message,
            detail: Some(detail),
            hint: Some(hint),
            sqlstate,
        }) if message == "cannot drop table parent_inh because other objects depend on it"
            && detail.contains("table child_inh depends on table parent_inh")
            && hint == "Use DROP ... CASCADE to drop the dependent objects too."
            && sqlstate == "2BP01" => {}
        other => panic!("expected legacy inheritance drop-table blocker, got {other:?}"),
    }
}

#[test]
fn new_tables_report_never_analyzed_reltuples() {
    let dir = temp_dir("new_table_reltuples");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table fresh_items(id int4)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relpages, reltuples from pg_class where relname = 'fresh_items'",
        ),
        vec![vec![Value::Int32(0), Value::Float64(-1.0)]]
    );
}

#[test]
fn new_indexes_report_never_analyzed_reltuples() {
    let dir = temp_dir("new_index_reltuples");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table fresh_items(id int4)")
        .unwrap();
    session
        .execute(&db, "create index fresh_items_id_idx on fresh_items(id)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select reltuples from pg_class where relname = 'fresh_items_id_idx'",
        ),
        vec![vec![Value::Float64(-1.0)]]
    );
}

#[test]
fn explain_uses_pg_style_width_density_for_unanalyzed_heaps() {
    let dir = temp_dir("explain_unanalyzed_width_density");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table narrow_items(id int4)")
        .unwrap();
    session
        .execute(&db, "create table wide_items(id int4, note text)")
        .unwrap();
    session
        .execute(&db, "insert into narrow_items values (1)")
        .unwrap();
    session
        .execute(&db, "insert into wide_items values (1, 'wide')")
        .unwrap();

    let narrow_rows = explain_estimated_rows(&db, 1, "select * from narrow_items");
    let wide_rows = explain_estimated_rows(&db, 1, "select * from wide_items");

    assert!(
        narrow_rows > wide_rows,
        "expected narrower heap to estimate more rows, got narrow={narrow_rows}, wide={wide_rows}"
    );
    assert_ne!(
        narrow_rows, 1000,
        "expected planner to avoid DEFAULT_NUM_ROWS fallback"
    );
    assert_ne!(
        wide_rows, 1000,
        "expected planner to avoid DEFAULT_NUM_ROWS fallback"
    );
}

#[test]
fn explain_uses_minimum_pages_for_never_analyzed_empty_heap() {
    let dir = temp_dir("explain_never_analyzed_empty_heap");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table empty_items(id int4)")
        .unwrap();

    let rows = explain_estimated_rows(&db, 1, "select * from empty_items");
    assert!(
        rows > 1000,
        "expected never-analyzed empty heap to use the minimum-pages heuristic, got rows={rows}"
    );
}

#[test]
fn explain_skips_minimum_pages_for_never_analyzed_parent_with_subclass() {
    let dir = temp_dir("explain_never_analyzed_parent_with_subclass");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parent_items(id int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create table child_items(extra int4) inherits (parent_items)",
        )
        .unwrap();

    let rows = explain_estimated_rows(&db, 1, "select * from only parent_items");
    assert_eq!(
        rows, 1,
        "expected inherited parent to skip the minimum-pages heuristic, got rows={rows}"
    );
}

#[test]
fn analyze_column_list_replaces_existing_pg_statistic_rows() {
    let dir = temp_dir("analyze_column_list");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table analyze_cols(a int4, b int4, c text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into analyze_cols values
               (1, 10, 'x'),
               (2, 20, 'y'),
               (3, 30, 'z')",
        )
        .unwrap();

    session.execute(&db, "analyze analyze_cols").unwrap();
    session.execute(&db, "analyze analyze_cols(a, c)").unwrap();

    let rows = query_rows(
        &db,
        1,
        "select staattnum
         from pg_statistic
         where starelid = (select oid from pg_class where relname = 'analyze_cols')
         order by staattnum",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(int_value(&rows[0][0]), 1);
    assert_eq!(int_value(&rows[1][0]), 3);
}

#[test]
fn analyze_populates_pg_stats_view_and_anyarray_columns() {
    use crate::include::catalog::{FLOAT4_TYPE_OID, INT4_TYPE_OID, TEXT_TYPE_OID};
    use crate::include::nodes::datum::ArrayValue;

    let dir = temp_dir("analyze_populates_pg_stats_view");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table stats_view_t(a int4, b text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into stats_view_t values
               (1, 'one'),
               (1, 'one'),
               (2, 'two'),
               (null, null),
               (3, 'three')",
        )
        .unwrap();
    session.execute(&db, "analyze stats_view_t").unwrap();

    let rows = query_rows(
        &db,
        1,
        "select attname, inherited, null_frac, avg_width, n_distinct,
                most_common_vals, most_common_freqs, histogram_bounds, correlation
         from pg_stats
         where tablename = 'stats_view_t'
         order by attname",
    );
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0][0], Value::Text("a".into()));
    assert_eq!(rows[0][1], Value::Bool(false));
    assert!(float_value(&rows[0][2]) > 0.0);
    assert!(int_value(&rows[0][3]) > 0);
    assert!(float_value(&rows[0][4]).abs() > 0.0);
    assert_eq!(
        rows[0][5],
        Value::PgArray(
            ArrayValue::from_1d(vec![Value::Int32(1)]).with_element_type_oid(INT4_TYPE_OID),
        )
    );
    match &rows[0][6] {
        Value::PgArray(array) => {
            assert_eq!(array.element_type_oid, Some(FLOAT4_TYPE_OID));
            assert_eq!(array.elements.len(), 1);
            assert!((float_value(&array.elements[0]) - 0.4).abs() < 0.01);
        }
        other => panic!("expected float4 frequency array, got {other:?}"),
    }
    match &rows[0][7] {
        Value::PgArray(array) => {
            assert_eq!(array.element_type_oid, Some(INT4_TYPE_OID));
            assert_eq!(array.ndim(), 1);
            assert!(!array.elements.is_empty());
        }
        other => panic!("expected int4 histogram array, got {other:?}"),
    }
    assert!(float_value(&rows[0][8]).abs() > 0.0);

    assert_eq!(rows[1][0], Value::Text("b".into()));
    match &rows[1][5] {
        Value::PgArray(array) => {
            assert_eq!(array.element_type_oid, Some(TEXT_TYPE_OID));
            assert_eq!(array.elements, vec![Value::Text("one".into())]);
        }
        other => panic!("expected text mcv array, got {other:?}"),
    }

    let histogram_dims = query_rows(
        &db,
        1,
        "select array_ndims(histogram_bounds)
         from pg_catalog.pg_stats
         where tablename = 'stats_view_t' and histogram_bounds is not null
         order by 1",
    );
    assert!(!histogram_dims.is_empty());
    assert!(
        histogram_dims
            .iter()
            .all(|row| row.as_slice() == [Value::Int32(1)])
    );
}

#[test]
fn table_inheritance_merges_columns_and_scans_children() {
    let dir = temp_dir("table_inheritance_scan");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table parent_inh(a int4, b text default 'parent')",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create table child_inh(b text default 'child', c int4) inherits (parent_inh)",
        )
        .unwrap();

    let att_rows = query_rows(
        &db,
        1,
        "select attname, attinhcount, attislocal
         from pg_attribute
         where attrelid = (select oid from pg_class where relname = 'child_inh')
           and attnum > 0
         order by attnum",
    );
    assert_eq!(att_rows.len(), 3);
    assert_eq!(att_rows[0][0], Value::Text("a".into()));
    assert_eq!(int_value(&att_rows[0][1]), 1);
    assert_eq!(att_rows[0][2], Value::Bool(false));
    assert_eq!(att_rows[1][0], Value::Text("b".into()));
    assert_eq!(int_value(&att_rows[1][1]), 1);
    assert_eq!(att_rows[1][2], Value::Bool(true));
    assert_eq!(att_rows[2][0], Value::Text("c".into()));
    assert_eq!(int_value(&att_rows[2][1]), 0);
    assert_eq!(att_rows[2][2], Value::Bool(true));

    session
        .execute(&db, "insert into parent_inh(a, b) values (1, 'parent')")
        .unwrap();
    session
        .execute(&db, "insert into child_inh(a, c) values (2, 20)")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select a, b from parent_inh order by a"),
        vec![
            vec![Value::Int32(1), Value::Text("parent".into())],
            vec![Value::Int32(2), Value::Text("child".into())],
        ]
    );
    assert_eq!(
        query_rows(&db, 1, "select a, b from only parent_inh order by a"),
        vec![vec![Value::Int32(1), Value::Text("parent".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select a, b, c from child_inh"),
        vec![vec![
            Value::Int32(2),
            Value::Text("child".into()),
            Value::Int32(20),
        ]]
    );

    let subclass_rows = query_rows(
        &db,
        1,
        "select relhassubclass from pg_class where relname = 'parent_inh'",
    );
    assert_eq!(subclass_rows, vec![vec![Value::Bool(true)]]);

    let inherit_rows = query_rows(
        &db,
        1,
        "select count(*) from pg_inherits where inhparent = (select oid from pg_class where relname = 'parent_inh')",
    );
    assert_eq!(int_value(&inherit_rows[0][0]), 1);

    let explain = explain_lines(&db, 1, "select a, b from parent_inh order by a");
    assert!(explain.iter().any(|line| line.contains("Append")));
}

#[test]
fn analyze_inheritance_tracks_root_and_inherited_stats_separately() {
    let dir = temp_dir("analyze_inheritance_stats");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parent_stats(a int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create table child_stats(extra int4) inherits (parent_stats)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "insert into child_stats(a, extra) values (1, 10), (2, 20), (3, 30)",
        )
        .unwrap();

    session.execute(&db, "analyze only parent_stats").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attname, inherited
             from pg_stats
             where tablename = 'parent_stats'
             order by inherited"
        ),
        vec![vec![Value::Text("a".into()), Value::Bool(false)]]
    );

    session.execute(&db, "analyze parent_stats").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attname, inherited
             from pg_stats
             where tablename = 'parent_stats'
             order by inherited"
        ),
        vec![
            vec![Value::Text("a".into()), Value::Bool(false)],
            vec![Value::Text("a".into()), Value::Bool(true)],
        ]
    );

    let reltuples = query_rows(
        &db,
        1,
        "select reltuples from pg_class where relname = 'parent_stats'",
    );
    assert_eq!(float_value(&reltuples[0][0]), 0.0);
}

#[test]
fn inheritance_multi_parent_create_and_drop_clean_up_catalog_rows() {
    let dir = temp_dir("inheritance_multi_parent_catalog");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create table a (aa text)").unwrap();
    session
        .execute(&db, "create table b (bb text) inherits (a)")
        .unwrap();
    session
        .execute(&db, "create table c (cc text) inherits (a)")
        .unwrap();
    clear_backend_notices();
    session
        .execute(&db, "create table d (dd text) inherits (b, c, a)")
        .unwrap();
    assert_eq!(
        take_backend_notice_messages(),
        vec![
            r#"merging multiple inherited definitions of column "aa""#.to_string(),
            r#"merging multiple inherited definitions of column "aa""#.to_string(),
        ]
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select p.relname
             from pg_inherits i
             join pg_class c on c.oid = i.inhrelid
             join pg_class p on p.oid = i.inhparent
             where c.relname = 'd'
             order by i.inhseqno",
        ),
        vec![
            vec![Value::Text("b".into())],
            vec![Value::Text("c".into())],
            vec![Value::Text("a".into())],
        ]
    );

    session.execute(&db, "drop table d").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relhassubclass
             from pg_class
             where relname in ('b', 'c')
             order by relname",
        ),
        vec![vec![Value::Bool(false)], vec![Value::Bool(false)]]
    );
}

#[test]
fn dropping_inherited_child_removes_pg_inherits_rows() {
    let dir = temp_dir("inheritance_drop_cleanup");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create table p1 (id int4)").unwrap();
    session.execute(&db, "create table p2 (id int4)").unwrap();
    session
        .execute(&db, "create table c1 (extra text) inherits (p1, p2)")
        .unwrap();

    assert_eq!(
        int_value(
            &query_rows(
                &db,
                1,
                "select count(*)
                 from pg_inherits i
                 join pg_class c on c.oid = i.inhrelid
                 where c.relname = 'c1'",
            )[0][0],
        ),
        2
    );

    session.execute(&db, "drop table c1").unwrap();

    assert_eq!(
        int_value(
            &query_rows(
                &db,
                1,
                "select count(*)
                 from pg_inherits i
                 join pg_class p on p.oid = i.inhparent
                 where p.relname in ('p1', 'p2')",
            )[0][0],
        ),
        0
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relhassubclass
             from pg_class
             where relname in ('p1', 'p2')
             order by relname",
        ),
        vec![vec![Value::Bool(false)], vec![Value::Bool(false)]]
    );
}

#[test]
fn alter_table_no_inherit_localizes_inherited_check_constraint() {
    let dir = temp_dir("alter_table_no_inherit_check");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table ac (aa int)").unwrap();
    db.execute(
        1,
        "alter table ac add constraint ac_check check (aa is not null)",
    )
    .unwrap();
    db.execute(1, "create table bc () inherits (ac)").unwrap();

    assert_eq!(
        int_value(
            &query_rows(
                &db,
                1,
                "select count(*)
                 from pg_inherits i
                 join pg_class c on c.oid = i.inhrelid
                 join pg_class p on p.oid = i.inhparent
                 where c.relname = 'bc' and p.relname = 'ac'",
            )[0][0],
        ),
        1
    );

    db.execute(1, "alter table bc no inherit ac").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, conislocal, coninhcount
             from pg_constraint c
             join pg_class r on r.oid = c.conrelid
             where r.relname = 'bc'
             order by conname",
        ),
        vec![vec![
            Value::Text("ac_check".into()),
            Value::Bool(true),
            Value::Int16(0),
        ]]
    );
    assert_eq!(
        int_value(
            &query_rows(
                &db,
                1,
                "select count(*)
                 from pg_inherits i
                 join pg_class c on c.oid = i.inhrelid
                 where c.relname = 'bc'",
            )[0][0],
        ),
        0
    );

    match db.execute(1, "insert into bc values (null)") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "bc" && constraint == "ac_check" => {}
        other => panic!("expected localized inherited check violation, got {other:?}"),
    }

    db.execute(1, "alter table bc drop constraint ac_check")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname
             from pg_constraint c
             join pg_class r on r.oid = c.conrelid
             where r.relname = 'bc'",
        ),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn alter_table_no_inherit_recomputes_multi_parent_column_and_not_null_metadata() {
    let dir = temp_dir("alter_table_no_inherit_multi_parent");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table p1 (a int not null)")
        .unwrap();
    session
        .execute(&db, "create table c1 () inherits (p1)")
        .unwrap();
    session
        .execute(&db, "create table c2 () inherits (p1, c1)")
        .unwrap();

    session
        .execute(&db, "alter table c2 no inherit p1")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attinhcount, attislocal
             from pg_attribute a
             join pg_class c on c.oid = a.attrelid
             where c.relname = 'c2' and a.attname = 'a'",
        ),
        vec![vec![Value::Int16(1), Value::Bool(false)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conislocal, coninhcount, connoinherit
             from pg_constraint pgc
             join pg_class c on c.oid = pgc.conrelid
             where c.relname = 'c2' and pgc.contype = 'n'",
        ),
        vec![vec![
            Value::Bool(false),
            Value::Int16(1),
            Value::Bool(false)
        ]]
    );

    session
        .execute(&db, "alter table c2 no inherit c1")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attinhcount, attislocal
             from pg_attribute a
             join pg_class c on c.oid = a.attrelid
             where c.relname = 'c2' and a.attname = 'a'",
        ),
        vec![vec![Value::Int16(0), Value::Bool(true)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conislocal, coninhcount, connoinherit
             from pg_constraint pgc
             join pg_class c on c.oid = pgc.conrelid
             where c.relname = 'c2' and pgc.contype = 'n'",
        ),
        vec![vec![Value::Bool(true), Value::Int16(0), Value::Bool(false)]]
    );
    assert_eq!(
        int_value(
            &query_rows(
                &db,
                1,
                "select count(*)
                 from pg_inherits i
                 join pg_class c on c.oid = i.inhrelid
                 where c.relname = 'c2'",
            )[0][0],
        ),
        0
    );

    match session.execute(&db, "insert into c2 values (null)") {
        Err(ExecError::NotNullViolation {
            relation, column, ..
        }) if relation == "c2" && column == "a" => {}
        other => panic!("expected localized inherited not-null violation, got {other:?}"),
    }
}

#[test]
fn create_table_not_null_no_inherit_sets_pg_constraint_flag() {
    let dir = temp_dir("create_table_not_null_no_inherit");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table p1 (ff1 int not null no inherit)")
        .unwrap();
    db.execute(1, "create table c1 () inherits (p1)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, connoinherit
             from pg_constraint c
             join pg_class r on r.oid = c.conrelid
             where r.relname = 'p1' and c.contype = 'n'
             order by conname",
        ),
        vec![vec![
            Value::Text("p1_ff1_not_null".into()),
            Value::Bool(true)
        ]]
    );
    assert_eq!(
        int_value(
            &query_rows(
                &db,
                1,
                "select count(*)
                 from pg_constraint c
                 join pg_class r on r.oid = c.conrelid
                 where r.relname = 'c1' and c.contype = 'n'",
            )[0][0],
        ),
        0
    );
    db.execute(1, "insert into c1 values (null)").unwrap();
}

#[test]
fn alter_table_add_not_null_no_inherit_sets_pg_constraint_flag() {
    let dir = temp_dir("alter_table_add_not_null_no_inherit");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table p1 (ff1 int)").unwrap();
    db.execute(
        1,
        "alter table p1 add constraint p1nn not null ff1 no inherit",
    )
    .unwrap();
    db.execute(1, "create table c1 () inherits (p1)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, connoinherit
             from pg_constraint c
             join pg_class r on r.oid = c.conrelid
             where r.relname = 'p1' and c.contype = 'n'
             order by conname",
        ),
        vec![vec![Value::Text("p1nn".into()), Value::Bool(true)]]
    );
    assert_eq!(
        int_value(
            &query_rows(
                &db,
                1,
                "select count(*)
                 from pg_constraint c
                 join pg_class r on r.oid = c.conrelid
                 where r.relname = 'c1' and c.contype = 'n'",
            )[0][0],
        ),
        0
    );
    db.execute(1, "insert into c1 values (null)").unwrap();
}

#[test]
fn create_table_rejects_not_null_no_inherit_on_inherited_not_null_column() {
    let dir = temp_dir("inherit_not_null_no_inherit_conflict");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table p1 (a int not null)").unwrap();
    clear_backend_notices();
    match db.execute(
        1,
        "create table c1 (a int not null no inherit) inherits (p1)",
    ) {
        Err(ExecError::Parse(ParseError::InvalidTableDefinition(message)))
            if message == "cannot define not-null constraint with NO INHERIT on column \"a\"" => {}
        other => panic!("expected inherited NO INHERIT conflict, got {other:?}"),
    }
}

#[test]
fn alter_table_set_not_null_rejects_existing_no_inherit_constraint() {
    let dir = temp_dir("set_not_null_existing_no_inherit");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table p1 (a int not null no inherit)")
        .unwrap();
    match db.execute(1, "alter table p1 alter column a set not null") {
        Err(ExecError::Parse(ParseError::InvalidTableDefinition(message)))
            if message
                == "cannot change NO INHERIT status of NOT NULL constraint \"p1_a_not_null\" on relation \"p1\"" =>
            {}
        other => panic!("expected existing NO INHERIT SET NOT NULL failure, got {other:?}"),
    }
}

#[test]
fn check_constraint_no_inherit_sets_pg_constraint_flag() {
    let dir = temp_dir("check_no_inherit_flag");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table p1 (ff1 int)").unwrap();
    db.execute(
        1,
        "alter table p1 add constraint p1chk check (ff1 > 0) no inherit",
    )
    .unwrap();
    db.execute(1, "alter table p1 add constraint p2chk check (ff1 > 10)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, connoinherit
             from pg_constraint c
             join pg_class r on r.oid = c.conrelid
             where r.relname = 'p1'
             order by conname",
        ),
        vec![
            vec![Value::Text("p1chk".into()), Value::Bool(true)],
            vec![Value::Text("p2chk".into()), Value::Bool(false)],
        ]
    );
}

#[test]
fn inherited_child_skips_check_constraints_marked_no_inherit() {
    let dir = temp_dir("inheritance_skips_no_inherit_checks");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table p1 (ff1 int)").unwrap();
    db.execute(
        1,
        "alter table p1 add constraint p1chk check (ff1 > 0) no inherit",
    )
    .unwrap();
    db.execute(1, "alter table p1 add constraint p2chk check (ff1 > 10)")
        .unwrap();
    db.execute(1, "create table c1 () inherits (p1)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, connoinherit
             from pg_constraint c
             join pg_class r on r.oid = c.conrelid
             where r.relname = 'c1'
             order by conname",
        ),
        vec![vec![Value::Text("p2chk".into()), Value::Bool(false)]]
    );
}

#[test]
fn temp_table_check_constraint_no_inherit_sets_pg_constraint_flag() {
    let dir = temp_dir("temp_check_no_inherit_flag");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table p1 (ff1 int)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table p1 add constraint p1chk check (ff1 > 0) no inherit",
        )
        .unwrap();

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select conname, connoinherit
             from pg_constraint c
             join pg_class r on r.oid = c.conrelid
             where r.relname = 'p1'
             order by conname",
        ),
        vec![vec![Value::Text("p1chk".into()), Value::Bool(true)]]
    );
}

#[test]
fn explain_inherited_self_join_with_order_by_does_not_panic() {
    let dir = temp_dir("inheritance_explain_self_join");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table matest0 (a int4, b int4, c int4, d int4)")
        .unwrap();
    session
        .execute(&db, "create table matest1 () inherits (matest0)")
        .unwrap();
    session
        .execute(&db, "create index matest0i on matest0 (b, c)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into matest0 values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)",
        )
        .unwrap();

    let lines = explain_lines(
        &db,
        1,
        "select t1.* from matest0 t1, matest0 t2
         where t1.b = t2.b and t2.c = t2.d
         order by t1.b
         limit 10",
    );
    assert!(
        !lines.is_empty(),
        "expected EXPLAIN output for inherited self-join"
    );
}

#[test]
fn explain_inherited_order_by_scan_does_not_panic() {
    let dir = temp_dir("inheritance_explain_order_by");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table matest0 (a int4 primary key)")
        .unwrap();
    session
        .execute(&db, "create table matest1 () inherits (matest0)")
        .unwrap();
    session
        .execute(&db, "insert into matest0 select generate_series(1, 400)")
        .unwrap();
    session.execute(&db, "analyze matest0").unwrap();

    let lines = explain_lines(&db, 1, "select * from matest0 where a < 100 order by a");
    assert!(
        !lines.is_empty(),
        "expected EXPLAIN output for inherited ordered scan"
    );
}

#[test]
fn explain_update_accepts_inherited_update_statement() {
    let dir = temp_dir("inheritance_explain_update");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(
        1,
        "create table some_tab (f1 int, f2 int, f3 int, check (f1 < 10) no inherit)",
    )
    .unwrap();
    db.execute(1, "create table some_tab_child () inherits(some_tab)")
        .unwrap();
    db.execute(
        1,
        "insert into some_tab_child select i, i + 1, 0 from generate_series(1, 1000) i",
    )
    .unwrap();
    db.execute(1, "create index on some_tab_child(f1, f2)")
        .unwrap();

    let StatementResult::Query { rows, .. } = db
        .execute(
            1,
            "explain (costs off) update some_tab set f3 = 11 where f1 = 12 and f2 = 13",
        )
        .unwrap()
    else {
        panic!("expected query result");
    };
    let lines = rows
        .into_iter()
        .map(|row| match row.first() {
            Some(Value::Text(text)) => text.to_string(),
            other => panic!("expected explain text row, got {:?}", other),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        lines.first().map(String::as_str),
        Some("Update on some_tab")
    );
    assert!(
        lines.iter().any(|line| {
            line.contains("Index Scan using") && line.contains("on some_tab_child some_tab_1")
        }),
        "expected EXPLAIN UPDATE to show inherited child index scan, got {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("f1 =")
            && line.contains("f2 =")
            && line.contains("12")
            && line.contains("13")),
        "expected EXPLAIN UPDATE to show index quals, got {lines:?}"
    );
}

#[test]
fn explain_verbose_update_where_false_is_accepted() {
    let dir = temp_dir("inheritance_explain_update_false");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table some_tab (a int, b int)")
        .unwrap();
    db.execute(1, "create table some_tab_child () inherits (some_tab)")
        .unwrap();
    db.execute(1, "insert into some_tab_child values (1, 2)")
        .unwrap();

    let StatementResult::Query { rows, .. } = db
        .execute(
            1,
            "explain (verbose, costs off) update some_tab set a = a + 1 where false",
        )
        .unwrap()
    else {
        panic!("expected query result");
    };
    let lines = rows
        .into_iter()
        .map(|row| match row.first() {
            Some(Value::Text(text)) => text.to_string(),
            other => panic!("expected explain text row, got {:?}", other),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        lines.first().map(String::as_str),
        Some("Update on public.some_tab")
    );
    assert!(
        lines
            .iter()
            .any(|line| line == "        Output: (some_tab.a + 1), NULL::oid, NULL::tid"),
        "expected EXPLAIN VERBOSE UPDATE to render assignment output, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line == "        One-Time Filter: false"),
        "expected EXPLAIN VERBOSE UPDATE to show false one-time filter, got {lines:?}"
    );
}

#[test]
fn explain_update_from_uses_join_plan_and_alias_header() {
    let dir = temp_dir("explain_update_from");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table some_tab (id int, name text)")
        .unwrap();
    db.execute(1, "create table src_tab (id int, name text)")
        .unwrap();

    let lines = explain_lines(
        &db,
        1,
        "(verbose, costs off) update some_tab t set name = s.name from src_tab s where s.id = t.id returning t.id, s.name",
    );

    assert_eq!(
        lines.first().map(String::as_str),
        Some("Update on public.some_tab t")
    );
    assert!(
        lines.iter().any(|line| line.starts_with("  Output: ")),
        "expected EXPLAIN VERBOSE UPDATE ... FROM to render statement output, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Join") || line.contains("Nested Loop")),
        "expected EXPLAIN UPDATE ... FROM to show a join plan, got {lines:?}"
    );
}

#[test]
fn inherited_scan_tableoid_tracks_physical_child_relation() {
    let dir = temp_dir("inheritance_tableoid");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parent_inh(a int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create table child_inh(extra int4) inherits (parent_inh)",
        )
        .unwrap();
    session
        .execute(&db, "insert into parent_inh values (1)")
        .unwrap();
    session
        .execute(&db, "insert into child_inh(a, extra) values (2, 10)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select c.relname, p.a
             from parent_inh p
             join pg_class c on p.tableoid = c.oid
             order by p.a",
        ),
        vec![
            vec![Value::Text("parent_inh".into()), Value::Int32(1)],
            vec![Value::Text("child_inh".into()), Value::Int32(2)],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select c.relname, p.a
             from only parent_inh p
             join pg_class c on p.tableoid = c.oid
             order by p.a",
        ),
        vec![vec![Value::Text("parent_inh".into()), Value::Int32(1)]]
    );
}

#[test]
fn base_table_scan_exposes_ctid_system_column() {
    let dir = temp_dir("scan_ctid");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table ctid_tbl(a int4)")
        .unwrap();
    session
        .execute(&db, "insert into ctid_tbl values (10), (20)")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select ctid, a from ctid_tbl order by a"),
        vec![
            vec![Value::Text("(0,1)".into()), Value::Int32(10)],
            vec![Value::Text("(0,2)".into()), Value::Int32(20)],
        ]
    );
    assert_eq!(
        query_rows(&db, 1, "select t.ctid, t.a from ctid_tbl t order by t.a"),
        vec![
            vec![Value::Text("(0,1)".into()), Value::Int32(10)],
            vec![Value::Text("(0,2)".into()), Value::Int32(20)],
        ]
    );
}

#[test]
fn inherited_update_delete_follow_postgres_targeting_rules() {
    let dir = temp_dir("inheritance_guardrails");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parent_guard(a int4)")
        .unwrap();
    session
        .execute(&db, "create table child_guard() inherits (parent_guard)")
        .unwrap();
    session
        .execute(&db, "insert into parent_guard values (1)")
        .unwrap();
    session
        .execute(&db, "insert into child_guard values (2)")
        .unwrap();

    match session
        .execute(&db, "update parent_guard set a = a + 10")
        .unwrap()
    {
        StatementResult::AffectedRows(2) => {}
        other => panic!("expected inherited update to touch parent and child, got {other:?}"),
    }
    assert_eq!(
        query_rows(&db, 1, "select a from only parent_guard order by 1"),
        vec![vec![Value::Int32(11)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select a from only child_guard order by 1"),
        vec![vec![Value::Int32(12)]]
    );

    match session
        .execute(&db, "update only parent_guard set a = a + 100")
        .unwrap()
    {
        StatementResult::AffectedRows(1) => {}
        other => panic!("expected ONLY update to touch parent only, got {other:?}"),
    }
    assert_eq!(
        query_rows(&db, 1, "select a from only parent_guard order by 1"),
        vec![vec![Value::Int32(111)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select a from only child_guard order by 1"),
        vec![vec![Value::Int32(12)]]
    );

    match session
        .execute(&db, "delete from only child_guard where a = 12")
        .unwrap()
    {
        StatementResult::AffectedRows(1) => {}
        other => panic!("expected ONLY delete to touch child only, got {other:?}"),
    }
    assert_eq!(
        query_rows(&db, 1, "select a from only child_guard order by 1"),
        Vec::<Vec<Value>>::new()
    );

    session
        .execute(&db, "insert into child_guard values (5)")
        .unwrap();
    match session.execute(&db, "delete from parent_guard").unwrap() {
        StatementResult::AffectedRows(2) => {}
        other => panic!("expected inherited delete to touch parent and child, got {other:?}"),
    }
    assert_eq!(
        query_rows(&db, 1, "select a from parent_guard order by 1"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn inheritance_guardrails_still_reject_truncate_and_column_alter() {
    let dir = temp_dir("inheritance_guardrails");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parent_guard(a int4)")
        .unwrap();
    session
        .execute(&db, "create table child_guard() inherits (parent_guard)")
        .unwrap();

    let truncate_err = session.execute(&db, "truncate parent_guard").unwrap_err();
    assert!(matches!(
        truncate_err,
        ExecError::Parse(ParseError::FeatureNotSupported(message))
            if message.contains("TRUNCATE on inherited parents")
    ));

    let alter_err = session
        .execute(&db, "alter table parent_guard add column b int4")
        .unwrap_err();
    assert!(matches!(
        alter_err,
        ExecError::Parse(ParseError::FeatureNotSupported(message))
            if message.contains("inheritance tree members")
    ));
}

#[test]
fn create_view_selects_and_persists_rewrite_rule() {
    let dir = temp_dir("create_view_selects");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items(id int4, name text)")
        .unwrap();
    session
        .execute(&db, "insert into items values (1, 'alpha'), (2, 'beta')")
        .unwrap();
    session
        .execute(&db, "create view item_names as select id, name from items")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select * from item_names order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("alpha".into())],
            vec![Value::Int32(2), Value::Text("beta".into())],
        ]
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select rulename, ev_action from pg_rewrite where ev_class = (select oid from pg_class where relname = 'item_names')",
        ),
        vec![vec![
            Value::Text("_RETURN".into()),
            Value::Text("select id, name from items".into()),
        ]]
    );
}

#[test]
fn set_operation_inputs_expand_views() {
    let dir = temp_dir("set_operation_view_inputs");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items(id int4, name text)")
        .unwrap();
    session
        .execute(&db, "insert into items values (1, 'alpha'), (2, 'beta')")
        .unwrap();
    session
        .execute(
            &db,
            "create view item_names as select 'v_' || name as name from items",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select name from items where id = 1 union select * from item_names order by 1",
        ),
        vec![
            vec![Value::Text("alpha".into())],
            vec![Value::Text("v_alpha".into())],
            vec![Value::Text("v_beta".into())],
        ]
    );
}

#[test]
fn view_return_rules_use_internal_dependency_and_user_rules_use_auto_dependency() {
    let dir = temp_dir("view_rule_dependency_types");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table items(id int4)").unwrap();
    db.execute(1, "create table item_log(id int4)").unwrap();
    db.execute(1, "create view item_names as select id from items")
        .unwrap();
    db.execute(
        1,
        "create rule item_log_rule as on insert to items do also insert into item_log values (new.id)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.deptype \
             from pg_depend d \
             join pg_rewrite r on r.oid = d.objid \
             where r.rulename = '_RETURN' \
               and d.classid = 2618 \
               and d.refclassid = 1259 \
               and d.refobjid = (select oid from pg_class where relname = 'item_names')",
        ),
        vec![vec![Value::Text("i".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.deptype \
             from pg_depend d \
             join pg_rewrite r on r.oid = d.objid \
             where r.rulename = 'item_log_rule' \
               and d.classid = 2618 \
               and d.refclassid = 1259 \
               and d.refobjid = (select oid from pg_class where relname = 'items')",
        ),
        vec![vec![Value::Text("a".into())]]
    );
}

#[test]
fn nested_views_and_pg_views_work() {
    let dir = temp_dir("nested_views");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table base_items(id int4)")
        .unwrap();
    session
        .execute(&db, "insert into base_items values (1), (2), (3)")
        .unwrap();
    session
        .execute(&db, "create view first_view as select id from base_items")
        .unwrap();
    session
        .execute(&db, "create view second_view as select id from first_view")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select * from second_view order by id"),
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
        ]
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select schemaname, viewname, viewowner, definition
             from pg_views
             where schemaname = 'public'
             order by viewname",
        ),
        vec![
            vec![
                Value::Text("public".into()),
                Value::Text("first_view".into()),
                Value::Text("postgres".into()),
                Value::Text("select id from base_items".into()),
            ],
            vec![
                Value::Text("public".into()),
                Value::Text("second_view".into()),
                Value::Text("postgres".into()),
                Value::Text("select id from first_view".into()),
            ],
        ]
    );
}

#[test]
fn pg_views_includes_pg_policies_metadata() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let rows = query_rows(
        &db,
        1,
        "select schemaname, viewname, viewowner, definition
         from pg_views
         where schemaname = 'pg_catalog' and viewname = 'pg_policies'",
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("pg_catalog".into()));
    assert_eq!(rows[0][1], Value::Text("pg_policies".into()));
    assert_eq!(rows[0][2], Value::Text("postgres".into()));
    match &rows[0][3] {
        Value::Text(definition) => assert!(definition.contains("FROM pg_catalog.pg_policy")),
        other => panic!("expected pg_policies definition text, got {other:?}"),
    }
}

#[test]
fn information_schema_view_metadata_tracks_updatable_views() {
    let dir = temp_dir("info_schema_updatable_views");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table base_tbl(a int primary key, b text)")
        .unwrap();
    session
        .execute(
            &db,
            "create view ro_view1 as select distinct a, b from base_tbl",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create view rw_view14 as select ctid, a, b from base_tbl",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create view rw_view15 as select a, upper(b) from base_tbl",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create view rw_view16 as select a, b, a as aa from base_tbl",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select table_name, is_insertable_into
             from information_schema.tables
             where table_name like E'r_\\\\_view%'
             order by table_name",
        ),
        vec![
            vec![Value::Text("ro_view1".into()), Value::Text("NO".into())],
            vec![Value::Text("rw_view14".into()), Value::Text("YES".into())],
            vec![Value::Text("rw_view15".into()), Value::Text("YES".into())],
            vec![Value::Text("rw_view16".into()), Value::Text("YES".into())],
        ]
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select table_name, is_updatable, is_insertable_into
             from information_schema.views
             where table_name like E'r_\\\\_view%'
             order by table_name",
        ),
        vec![
            vec![
                Value::Text("ro_view1".into()),
                Value::Text("NO".into()),
                Value::Text("NO".into()),
            ],
            vec![
                Value::Text("rw_view14".into()),
                Value::Text("YES".into()),
                Value::Text("YES".into()),
            ],
            vec![
                Value::Text("rw_view15".into()),
                Value::Text("YES".into()),
                Value::Text("YES".into()),
            ],
            vec![
                Value::Text("rw_view16".into()),
                Value::Text("YES".into()),
                Value::Text("YES".into()),
            ],
        ]
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select table_name, column_name, is_updatable
             from information_schema.columns
             where table_name like E'r_\\\\_view%'
             order by table_name, ordinal_position",
        ),
        vec![
            vec![
                Value::Text("ro_view1".into()),
                Value::Text("a".into()),
                Value::Text("NO".into()),
            ],
            vec![
                Value::Text("ro_view1".into()),
                Value::Text("b".into()),
                Value::Text("NO".into()),
            ],
            vec![
                Value::Text("rw_view14".into()),
                Value::Text("ctid".into()),
                Value::Text("NO".into()),
            ],
            vec![
                Value::Text("rw_view14".into()),
                Value::Text("a".into()),
                Value::Text("YES".into()),
            ],
            vec![
                Value::Text("rw_view14".into()),
                Value::Text("b".into()),
                Value::Text("YES".into()),
            ],
            vec![
                Value::Text("rw_view15".into()),
                Value::Text("a".into()),
                Value::Text("YES".into()),
            ],
            vec![
                Value::Text("rw_view15".into()),
                Value::Text("upper".into()),
                Value::Text("NO".into()),
            ],
            vec![
                Value::Text("rw_view16".into()),
                Value::Text("a".into()),
                Value::Text("YES".into()),
            ],
            vec![
                Value::Text("rw_view16".into()),
                Value::Text("b".into()),
                Value::Text("YES".into()),
            ],
            vec![
                Value::Text("rw_view16".into()),
                Value::Text("aa".into()),
                Value::Text("YES".into()),
            ],
        ]
    );
}

#[test]
fn create_view_supports_check_option_and_or_replace() {
    let dir = temp_dir("create_view_check_option_replace");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table base_tbl(a int)")
        .unwrap();
    session
        .execute(
            &db,
            "create view rw_view1 as select * from base_tbl where a > 0",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create view rw_view2 as select * from rw_view1 where a < 10 with check option",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select table_name, check_option from information_schema.views where table_name in ('rw_view1', 'rw_view2') order by table_name",
        ),
        vec![
            vec![Value::Text("rw_view1".into()), Value::Text("NONE".into())],
            vec![
                Value::Text("rw_view2".into()),
                Value::Text("CASCADED".into()),
            ],
        ]
    );

    session
        .execute(&db, "insert into rw_view2 values (5)")
        .unwrap();
    match session.execute(&db, "insert into rw_view2 values (-5)") {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(
                message,
                "new row violates check option for view \"rw_view1\""
            );
            assert_eq!(detail.as_deref(), Some("Failing row contains (-5)."));
            assert_eq!(sqlstate, "44000");
        }
        other => panic!("expected rw_view1 check-option violation, got {other:?}"),
    }
    match session.execute(&db, "insert into rw_view2 values (15)") {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(
                message,
                "new row violates check option for view \"rw_view2\""
            );
            assert_eq!(detail.as_deref(), Some("Failing row contains (15)."));
            assert_eq!(sqlstate, "44000");
        }
        other => panic!("expected rw_view2 check-option violation, got {other:?}"),
    }

    session
        .execute(
            &db,
            "create or replace view rw_view2 as select * from rw_view1 where a < 10 with local check option",
        )
        .unwrap();
    session
        .execute(&db, "insert into rw_view2 values (-10)")
        .unwrap();
    match session.execute(&db, "insert into rw_view2 values (20)") {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(
                message,
                "new row violates check option for view \"rw_view2\""
            );
            assert_eq!(detail.as_deref(), Some("Failing row contains (20)."));
            assert_eq!(sqlstate, "44000");
        }
        other => panic!("expected local check-option violation, got {other:?}"),
    }

    session
        .execute(&db, "create table t1(a int, b text)")
        .unwrap();
    session
        .execute(&db, "create view v1 as select null::int as a")
        .unwrap();
    session
        .execute(
            &db,
            "create or replace view v1 as select * from t1 where a > 0 with check option",
        )
        .unwrap();
    session
        .execute(&db, "insert into v1 values (1, 'ok')")
        .unwrap();
    match session.execute(&db, "insert into v1 values (-1, 'bad')") {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(message, "new row violates check option for view \"v1\"");
            assert_eq!(detail.as_deref(), Some("Failing row contains (-1, bad)."));
            assert_eq!(sqlstate, "44000");
        }
        other => panic!("expected replaced-view check-option violation, got {other:?}"),
    }
}

#[test]
fn view_relfilenode_is_zero_and_drop_table_rejects_view_name() {
    let dir = temp_dir("view_relfilenode_zero");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create table items(id int4)").unwrap();
    session
        .execute(&db, "create view item_view as select id from items")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relfilenode from pg_class where relname = 'item_view'",
        ),
        vec![vec![Value::Int64(0)]]
    );

    match session.execute(&db, "drop table item_view") {
        Err(ExecError::Parse(ParseError::WrongObjectType { name, expected }))
            if name == "item_view" && expected == "table" => {}
        other => panic!("expected drop-table wrong-object-type error, got {other:?}"),
    }
}

#[test]
fn dependent_views_block_alter_and_drop() {
    let dir = temp_dir("dependent_views_block_ddl");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table base_items(id int4)")
        .unwrap();
    session
        .execute(&db, "create view base_view as select id from base_items")
        .unwrap();

    match session.execute(&db, "alter table base_items add column note text") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. }))
            if actual.contains("view depends on it: base_view") => {}
        other => panic!("expected dependent-view alter-table error, got {other:?}"),
    }

    match session.execute(&db, "drop table base_items") {
        Err(ExecError::DetailedError {
            message,
            detail: Some(detail),
            hint: Some(hint),
            sqlstate,
        }) if message == "cannot drop table base_items because other objects depend on it"
            && detail == "view base_view depends on table base_items"
            && hint == "Use DROP ... CASCADE to drop the dependent objects too."
            && sqlstate == "2BP01" => {}
        other => panic!("expected dependent-view drop-table error, got {other:?}"),
    }
}

#[test]
fn drop_view_rejects_depended_on_view() {
    let dir = temp_dir("drop_view_rejects_depended_on_view");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table base_items(id int4)")
        .unwrap();
    session
        .execute(&db, "create view first_view as select id from base_items")
        .unwrap();
    session
        .execute(&db, "create view second_view as select id from first_view")
        .unwrap();

    match session.execute(&db, "drop view first_view") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. }))
            if actual.contains("view depends on it: second_view") => {}
        other => panic!("expected dependent-view drop-view error, got {other:?}"),
    }
}

#[test]
fn table_only_commands_reject_views() {
    let dir = temp_dir("table_only_commands_reject_views");
    let db = Database::open(&dir, 128).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create table items(id int4)").unwrap();
    session
        .execute(&db, "create view item_view as select id from items")
        .unwrap();

    for sql in [
        "comment on table item_view is 'nope'",
        "create index item_view_idx on item_view (id)",
        "analyze item_view",
        "vacuum item_view",
        "truncate item_view",
    ] {
        match session.execute(&db, sql) {
            Err(ExecError::Parse(ParseError::WrongObjectType { name, expected }))
                if name == "item_view"
                    && (expected == "table" || expected == "table or materialized view") => {}
            other => panic!("expected wrong-object-type error for `{sql}`, got {other:?}"),
        }
    }
}

fn read_relation_block(
    db: &Database,
    rel: crate::RelFileLocator,
    block: u32,
) -> [u8; crate::backend::storage::smgr::BLCKSZ] {
    read_relation_fork_block(
        db,
        rel,
        crate::backend::storage::smgr::ForkNumber::Main,
        block,
    )
}

fn read_relation_fork_block(
    db: &Database,
    rel: crate::RelFileLocator,
    fork: crate::backend::storage::smgr::ForkNumber,
    block: u32,
) -> [u8; crate::backend::storage::smgr::BLCKSZ] {
    let mut page = [0u8; crate::backend::storage::smgr::BLCKSZ];
    db.pool
        .with_storage_mut(|storage| storage.smgr.read_block(rel, fork, block, &mut page))
        .unwrap();
    page
}

fn read_buffered_relation_block(
    db: &Database,
    client_id: u32,
    rel: crate::RelFileLocator,
    block: u32,
) -> [u8; crate::backend::storage::smgr::BLCKSZ] {
    let pinned = db
        .pool
        .pin_existing_block(
            client_id,
            rel,
            crate::backend::storage::smgr::ForkNumber::Main,
            block,
        )
        .unwrap();
    let page = db.pool.read_page(pinned.buffer_id()).unwrap();
    drop(pinned);
    page
}

fn relation_fork_nblocks(
    db: &Database,
    rel: crate::RelFileLocator,
    fork: crate::backend::storage::smgr::ForkNumber,
) -> u32 {
    db.pool
        .with_storage_mut(|storage| storage.smgr.nblocks(rel, fork))
        .unwrap()
}

fn relation_has_dirty_buffered_page(
    db: &Database,
    client_id: u32,
    rel: crate::RelFileLocator,
) -> bool {
    let nblocks = relation_fork_nblocks(db, rel, crate::backend::storage::smgr::ForkNumber::Main);
    (0..nblocks).any(|block| {
        read_relation_block(db, rel, block)
            != read_buffered_relation_block(db, client_id, rel, block)
    })
}

fn gist_leaf_tuple_count(db: &Database, client_id: u32, rel: crate::RelFileLocator) -> usize {
    let nblocks = relation_fork_nblocks(db, rel, crate::backend::storage::smgr::ForkNumber::Main);
    let mut count = 0usize;
    for block in 0..nblocks {
        let page = read_buffered_relation_block(db, client_id, rel, block);
        let opaque = crate::include::access::gist::gist_page_get_opaque(&page).unwrap();
        if opaque.is_leaf() && !opaque.is_deleted() {
            count += crate::include::access::gist::gist_page_items(&page)
                .unwrap()
                .len();
        }
    }
    count
}

fn assert_explain_uses_index(db: &Database, client_id: u32, sql: &str, index_name: &str) {
    let relfilenode = relfilenode_for(db, client_id, index_name);
    let lines = explain_lines(db, client_id, sql);
    assert!(
        lines.iter().any(|line| {
            (line.contains("Index Scan using ")
                || line.contains("Index Scan Backward using ")
                || line.contains("Index Only Scan using ")
                || line.contains("Index Only Scan Backward using "))
                && (line.contains(&format!("using rel {relfilenode} "))
                    || line.contains(&format!("using {index_name} ")))
        }),
        "expected EXPLAIN to use index {index_name} (relfilenode {relfilenode}), got {lines:?}"
    );
}

fn assert_explain_uses_seqscan(db: &Database, client_id: u32, sql: &str, heap_name: &str) {
    let lines = explain_lines(db, client_id, sql);
    assert!(
        lines
            .iter()
            .any(|line| line.contains(&format!("Seq Scan on {heap_name}"))),
        "expected EXPLAIN to use seq scan on {heap_name}, got {lines:?}"
    );
    assert!(
        !lines
            .iter()
            .any(|line| line.contains("Index Scan") || line.contains("Index Only Scan")),
        "expected no index scan, got {lines:?}"
    );
}

fn psql_index_definition(
    db: &Database,
    client_id: u32,
    table_name: &str,
    index_name: &str,
) -> String {
    let lookup = db.lazy_catalog_lookup(client_id, None, None);
    let relation = lookup
        .lookup_any_relation(table_name)
        .unwrap_or_else(|| panic!("expected relation {table_name}"));
    let index = lookup
        .index_relations_for_heap(relation.relation_oid)
        .into_iter()
        .find(|index| index.name == index_name)
        .unwrap_or_else(|| panic!("expected index {index_name}"));
    crate::backend::tcop::postgres::format_psql_indexdef(db, &Session::new(client_id), &index)
}

fn setup_index_matrix_db(label: &str) -> Database {
    let base = temp_dir(label);
    let db = Database::open(&base, 16).unwrap();
    db.execute(
        1,
        "create table items (a int4 not null, b int4 not null, c int4 not null, note text)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into items values \
         (1, 10, 100, 'a1'), \
         (1, 20, 200, 'a2'), \
         (2, 15, 100, 'b1'), \
         (2, 25, 200, 'b2'), \
         (2, 35, 300, 'b3'), \
         (3, 30, 100, 'c1')",
    )
    .unwrap();
    db.execute(1, "create index items_a_idx on items (a)")
        .unwrap();
    db.execute(1, "create index items_ab_idx on items (a, b)")
        .unwrap();
    db.execute(1, "create index items_b_idx on items (b)")
        .unwrap();
    db.execute(1, "create index items_ba_idx on items (b, a)")
        .unwrap();
    db
}

fn setup_partial_index_matrix_db(label: &str) -> Database {
    let base = temp_dir(label);
    let db = Database::open(&base, 16).unwrap();
    db.execute(
        1,
        "create table items (id int4 not null, flag text not null, note text)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into items values \
         (1, 'skip', 's1'), \
         (1, 'keep', 'k1'), \
         (2, 'keep', 'k2'), \
         (2, 'skip', 's2'), \
         (3, 'skip', 's3')",
    )
    .unwrap();
    db.execute(
        1,
        "create index items_keep_idx on items (id) where flag = 'keep'",
    )
    .unwrap();
    db
}

#[test]
fn create_hash_index_catalog_and_equality_scan() {
    let base = temp_dir("hash_index_catalog_scan");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "insert into items values \
         (1, 'one'), (2, 'two'), (42, 'target'), (99, 'other')",
    )
    .unwrap();
    db.execute(
        1,
        "create index items_id_hash on items using hash (id) with (fillfactor = 80)",
    )
    .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select name from items where id = 42",
        "items_id_hash",
    );
    assert_eq!(
        query_rows(&db, 1, "select name from items where id = 42"),
        vec![vec![Value::Text("target".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select am.amname from pg_class c join pg_am am on am.oid = c.relam \
             where c.relname = 'items_id_hash'",
        ),
        vec![vec![Value::Text("hash".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select indexdef from pg_indexes where tablename = 'items' \
             and indexname = 'items_id_hash'",
        ),
        vec![vec![Value::Text(
            "CREATE INDEX items_id_hash ON public.items USING hash (id)".into()
        )]]
    );

    let catalog = db.catalog.read().catalog_snapshot().unwrap();
    let index = catalog.get("items_id_hash").unwrap();
    assert_eq!(
        index
            .index_meta
            .as_ref()
            .and_then(|meta| meta.hash_options)
            .map(|options| options.fillfactor),
        Some(80)
    );
    drop(catalog);
    drop(db);

    let reopened = Database::open(&base, 16).unwrap();
    assert_explain_uses_index(
        &reopened,
        1,
        "select name from items where id = 42",
        "items_id_hash",
    );
    assert_eq!(
        query_rows(&reopened, 1, "select name from items where id = 42"),
        vec![vec![Value::Text("target".into())]]
    );
}

#[test]
fn hash_expression_partial_index_matches_equality_quals() {
    let base = temp_dir("hash_expression_partial_index");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table items (id int4 not null, name text, flag text)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into items values \
         (1, 'Alpha', 'keep'), \
         (2, 'Alpha', 'skip'), \
         (3, 'Beta', 'keep')",
    )
    .unwrap();
    db.execute(
        1,
        "create index items_lower_hash on items using hash ((lower(name))) where flag = 'keep'",
    )
    .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select id from items where lower(name) = 'alpha' and flag = 'keep'",
        "items_lower_hash",
    );
    assert_explain_uses_seqscan(
        &db,
        1,
        "select id from items where lower(name) = 'alpha' and flag = 'skip'",
        "items",
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from items where lower(name) = 'alpha' and flag = 'keep'",
        ),
        vec![vec![Value::Int32(1)]]
    );
}

#[test]
fn hash_index_rejects_unsupported_shapes_and_options() {
    let base = temp_dir("hash_index_rejections");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (a int4 not null, b int4 not null)")
        .unwrap();

    match db.execute(
        1,
        "create unique index items_a_hash on items using hash (a)",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(message)))
            if message == "access method \"hash\" does not support unique indexes" => {}
        other => panic!("expected unique hash index rejection, got {other:?}"),
    }
    match db.execute(1, "create index items_ab_hash on items using hash (a, b)") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message == "access method \"hash\" does not support multicolumn indexes"
            && sqlstate == "0A000" => {}
        other => panic!("expected multicolumn hash index rejection, got {other:?}"),
    }
    match db.execute(
        1,
        "create index items_a_hash_badopt on items using hash (a) with (pages_per_range = 1)",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "hash index option \"pages_per_range\"" => {}
        other => panic!("expected hash reloption rejection, got {other:?}"),
    }
}

#[test]
fn single_thread_create_insert_select() {
    let base = temp_dir("single_thread");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(1, "insert into items (id, name) values (1, 'alpha')")
        .unwrap();
    db.execute(1, "insert into items (id, name) values (2, 'beta')")
        .unwrap();

    match db
        .execute(1, "select id, name from items order by id")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Int32(1), Value::Text("alpha".into())]);
            assert_eq!(rows[1], vec![Value::Int32(2), Value::Text("beta".into())]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn client_visible_cache_refreshes_after_create_table() {
    let base = temp_dir("client_visible_cache_refresh");
    let db = Database::open(&base, 16).unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    assert!(visible.lookup_any_relation("cache_test").is_none());
    assert!(db.backend_cache_states.read().contains_key(&1));

    db.execute(1, "create table cache_test (id int4)").unwrap();

    assert!(db.backend_cache_states.read().contains_key(&1));
    let visible = db.lazy_catalog_lookup(1, None, None);
    assert!(visible.lookup_any_relation("cache_test").is_some());
}

#[test]
fn committed_catalog_invalidation_evicts_other_sessions_without_global_reset() {
    let base = temp_dir("commit_catalog_invalidation_fanout");
    let db = Database::open(&base, 16).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    assert!(
        db.lazy_catalog_lookup(1, None, None)
            .lookup_any_relation("fanout_test")
            .is_none()
    );
    assert!(
        db.lazy_catalog_lookup(2, None, None)
            .lookup_any_relation("fanout_test")
            .is_none()
    );
    {
        let states = db.backend_cache_states.read();
        let writer_state = states.get(&1).unwrap();
        let reader_state = states.get(&2).unwrap();
        assert!(writer_state.catcache.is_some());
        assert!(writer_state.relcache.is_some());
        assert!(reader_state.catcache.is_some());
        assert!(reader_state.relcache.is_some());
        assert!(reader_state.pending_invalidations.is_empty());
    }

    writer.execute(&db, "begin").unwrap();
    writer
        .execute(&db, "create table fanout_test (id int4 not null)")
        .unwrap();

    assert!(db.backend_cache_states.read().contains_key(&1));
    assert!(db.backend_cache_states.read().contains_key(&2));
    assert!(
        reader
            .execute(&db, "select count(*) from fanout_test")
            .is_err(),
        "other sessions should keep their existing cache until commit"
    );

    writer.execute(&db, "commit").unwrap();

    {
        let states = db.backend_cache_states.read();
        let reader_state = states.get(&2).unwrap();
        assert!(reader_state.catcache.is_some());
        assert!(reader_state.relcache.is_some());
        assert_eq!(reader_state.pending_invalidations.len(), 1);
    }
    match reader
        .execute(&db, "select count(*) from fanout_test")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query, got {:?}", other),
    }
    {
        let states = db.backend_cache_states.read();
        let reader_state = states.get(&2).unwrap();
        assert!(reader_state.pending_invalidations.is_empty());
    }
}

#[test]
fn dropping_last_temp_table_keeps_temp_namespace() {
    let base = temp_dir("drop_temp_namespace_cleanup");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create temp table temp_cleanup (id int4)")
        .unwrap();
    assert!(db.has_active_temp_namespace(1));

    db.execute(1, "drop table temp_cleanup").unwrap();

    assert!(db.has_active_temp_namespace(1));
    let namespace = db.temp_relations.read().get(&1).cloned().unwrap();
    assert_eq!(namespace.name, "pg_temp_1");
    assert!(namespace.tables.is_empty());
}

#[test]
fn create_index_and_alter_table_set_are_noops() {
    let base = temp_dir("numeric_sql_noops");
    let db = Database::open(&base, 16).unwrap();

    match db
            .execute(
                1,
                "select d.datname, t.spcname from pg_database d join pg_tablespace t on t.oid = d.dattablespace order by d.datname",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            Value::Text("postgres".into()),
                            Value::Text("pg_default".into()),
                        ],
                        vec![
                            Value::Text("template0".into()),
                            Value::Text("pg_default".into()),
                        ],
                        vec![
                            Value::Text("template1".into()),
                            Value::Text("pg_default".into()),
                        ],
                    ]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }

    match db
        .execute(
            1,
            "select a.rolname from pg_database d join pg_authid a on a.oid = d.datdba order by d.datname",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("postgres".into())],
                    vec![Value::Text("postgres".into())],
                    vec![Value::Text("postgres".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(
            1,
            "select rolname, rolsuper, rolcreatedb from pg_authid order by oid",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert!(rows.contains(&vec![
                Value::Text("postgres".into()),
                Value::Bool(true),
                Value::Bool(true),
            ]));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(
            1,
            "select collname, collprovider from pg_collation order by oid",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("default".into()), Value::Text("d".into()),],
                    vec![Value::Text("C".into()), Value::Text("c".into())],
                    vec![Value::Text("POSIX".into()), Value::Text("c".into()),],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let proc_sql = "select p.proname, p.prokind, p.pronargs, p.proretset, t.typname, l.lanname \
             from pg_proc p \
             join pg_type t on t.oid = p.prorettype \
             join pg_language l on l.oid = p.prolang \
             where p.proname in ('count', 'json_array_elements', 'lower', 'random') \
             order by p.proname";
    match db.execute(1, proc_sql).unwrap() {
        StatementResult::Query { rows, .. } => {
            assert!(rows.contains(&vec![
                Value::Text("count".into()),
                Value::Text("a".into()),
                Value::Int16(1),
                Value::Bool(false),
                Value::Text("int8".into()),
                Value::Text("internal".into()),
            ]));
            assert!(rows.contains(&vec![
                Value::Text("json_array_elements".into()),
                Value::Text("f".into()),
                Value::Int16(1),
                Value::Bool(true),
                Value::Text("json".into()),
                Value::Text("internal".into()),
            ]));
            assert!(rows.contains(&vec![
                Value::Text("lower".into()),
                Value::Text("f".into()),
                Value::Int16(1),
                Value::Bool(false),
                Value::Text("text".into()),
                Value::Text("internal".into()),
            ]));
            assert!(rows.contains(&vec![
                Value::Text("random".into()),
                Value::Text("f".into()),
                Value::Int16(0),
                Value::Bool(false),
                Value::Text("float8".into()),
                Value::Text("internal".into()),
            ]));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let op_sql = "select o.oprname, l.typname, r.typname, p.proname \
             from pg_operator o \
             join pg_type l on l.oid = o.oprleft \
             join pg_type r on r.oid = o.oprright \
             join pg_proc p on p.oid = o.oprcode \
             where o.oid in (91, 96, 98, 531, 1694, 3877) \
             order by o.oid";
    match db.execute(1, op_sql).unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Text("=".into()),
                        Value::Text("bool".into()),
                        Value::Text("bool".into()),
                        Value::Text("booleq".into()),
                    ],
                    vec![
                        Value::Text("=".into()),
                        Value::Text("int4".into()),
                        Value::Text("int4".into()),
                        Value::Text("int4eq".into()),
                    ],
                    vec![
                        Value::Text("=".into()),
                        Value::Text("text".into()),
                        Value::Text("text".into()),
                        Value::Text("texteq".into()),
                    ],
                    vec![
                        Value::Text("<>".into()),
                        Value::Text("text".into()),
                        Value::Text("text".into()),
                        Value::Text("textne".into()),
                    ],
                    vec![
                        Value::Text("<=".into()),
                        Value::Text("bool".into()),
                        Value::Text("bool".into()),
                        Value::Text("boolle".into()),
                    ],
                    vec![
                        Value::Text("^@".into()),
                        Value::Text("text".into()),
                        Value::Text("text".into()),
                        Value::Text("starts_with".into()),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(
            1,
            "select s.typname, t.typname, c.castcontext, c.castmethod \
                 from pg_cast c \
                 join pg_type s on s.oid = c.castsource \
                 join pg_type t on t.oid = c.casttarget \
                 order by c.oid",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let expected_subset = vec![
                vec![
                    Value::Text("int2".into()),
                    Value::Text("int4".into()),
                    Value::Text("i".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int2".into()),
                    Value::Text("int8".into()),
                    Value::Text("i".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int2".into()),
                    Value::Text("numeric".into()),
                    Value::Text("i".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int4".into()),
                    Value::Text("int2".into()),
                    Value::Text("a".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int4".into()),
                    Value::Text("int8".into()),
                    Value::Text("i".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int4".into()),
                    Value::Text("numeric".into()),
                    Value::Text("i".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int4".into()),
                    Value::Text("oid".into()),
                    Value::Text("i".into()),
                    Value::Text("b".into()),
                ],
                vec![
                    Value::Text("int8".into()),
                    Value::Text("int2".into()),
                    Value::Text("a".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int8".into()),
                    Value::Text("int4".into()),
                    Value::Text("a".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("int8".into()),
                    Value::Text("numeric".into()),
                    Value::Text("i".into()),
                    Value::Text("f".into()),
                ],
                vec![
                    Value::Text("oid".into()),
                    Value::Text("int4".into()),
                    Value::Text("a".into()),
                    Value::Text("b".into()),
                ],
                vec![
                    Value::Text("varchar".into()),
                    Value::Text("text".into()),
                    Value::Text("i".into()),
                    Value::Text("b".into()),
                ],
                vec![
                    Value::Text("char".into()),
                    Value::Text("text".into()),
                    Value::Text("i".into()),
                    Value::Text("f".into()),
                ],
            ];
            for expected_row in expected_subset {
                assert!(
                    rows.contains(&expected_row),
                    "missing cast row: {:?}",
                    expected_row
                );
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(
            1,
            "select s.typname, t.typname, p.proname \
                 from pg_cast c \
                 join pg_type s on s.oid = c.castsource \
                 join pg_type t on t.oid = c.casttarget \
                 join pg_proc p on p.oid = c.castfunc \
                 where c.castfunc <> 0 \
                 order by c.oid",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Text("int2".into()),
                        Value::Text("int4".into()),
                        Value::Text("int4".into()),
                    ],
                    vec![
                        Value::Text("int2".into()),
                        Value::Text("int8".into()),
                        Value::Text("int8".into()),
                    ],
                    vec![
                        Value::Text("int2".into()),
                        Value::Text("numeric".into()),
                        Value::Text("numeric".into()),
                    ],
                    vec![
                        Value::Text("int4".into()),
                        Value::Text("int2".into()),
                        Value::Text("int2".into()),
                    ],
                    vec![
                        Value::Text("int4".into()),
                        Value::Text("int8".into()),
                        Value::Text("int8".into()),
                    ],
                    vec![
                        Value::Text("int4".into()),
                        Value::Text("numeric".into()),
                        Value::Text("numeric".into()),
                    ],
                    vec![
                        Value::Text("int8".into()),
                        Value::Text("int2".into()),
                        Value::Text("int2".into()),
                    ],
                    vec![
                        Value::Text("int8".into()),
                        Value::Text("int4".into()),
                        Value::Text("int4".into()),
                    ],
                    vec![
                        Value::Text("int8".into()),
                        Value::Text("numeric".into()),
                        Value::Text("numeric".into()),
                    ],
                    vec![
                        Value::Text("char".into()),
                        Value::Text("text".into()),
                        Value::Text("text".into()),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(1, "select count(*) from pg_auth_members")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db.execute(1, "select count(*) from pg_constraint").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            match &rows[0][0] {
                Value::Int64(count) => assert!(*count > 0),
                other => panic!("expected int64 count, got {:?}", other),
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }

    db.execute(1, "create table num_exp_add (id1 int4, id2 int4)")
        .unwrap();

    match db
            .execute(
                1,
                "select a.rolname from pg_class c join pg_authid a on a.oid = c.relowner where c.relname = 'num_exp_add'",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Text("postgres".into())]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }

    assert_eq!(
        db.execute(
            1,
            "create unique index num_exp_add_idx on num_exp_add (id1, id2)",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
    {
        let visible = db.lazy_catalog_lookup(1, None, None);
        let entry = visible.lookup_any_relation("num_exp_add_idx").unwrap();
        assert_eq!(entry.relkind, 'i');
        let described = crate::backend::utils::cache::lsyscache::describe_relation_by_oid(
            &db,
            1,
            None,
            entry.relation_oid,
        )
        .unwrap();
        let index = described.index.unwrap();
        assert_eq!(index.am_oid, crate::include::catalog::BTREE_AM_OID);
        assert!(index.indisunique);
        assert!(index.indisvalid);
        assert!(index.indisready);
        assert_eq!(index.indkey, vec![1, 2]);
        assert_eq!(index.indclass.len(), 2);
        assert_eq!(index.opfamily_oids.len(), 2);
        assert_eq!(index.opcintype_oids.len(), 2);
    }

    match db
        .execute(
            1,
            "select count(*) from pg_class where relname = 'num_exp_add_idx'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(
            1,
            "select relpersistence from pg_class where relname = 'num_exp_add_idx'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("p".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
            .execute(
                1,
                "select a.amname from pg_class c join pg_am a on a.oid = c.relam where c.relname = 'num_exp_add'",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Text("heap".into())]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }

    match db
            .execute(
                1,
                "select a.amname from pg_class c join pg_am a on a.oid = c.relam where c.relname = 'num_exp_add_idx'",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Text("btree".into())]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }

    match db.execute(1, "select * from num_exp_add_idx") {
        Err(ExecError::Parse(ParseError::UnknownTable(name)))
        | Err(ExecError::Parse(ParseError::TableDoesNotExist(name)))
            if name == "num_exp_add_idx" => {}
        Err(ExecError::Parse(ParseError::WrongObjectType { name, expected }))
            if name == "num_exp_add_idx"
                && (expected == "table"
                    || expected == "table, view, or sequence"
                    || expected == "table, view, materialized view, or sequence") => {}
        other => panic!("expected missing-table or wrong-object-type error, got {other:?}"),
    }

    assert_eq!(
        db.execute(1, "alter table num_exp_add set (parallel_workers = 4)",)
            .unwrap(),
        StatementResult::AffectedRows(0)
    );

    assert_eq!(
        db.execute(1, "drop table num_exp_add").unwrap(),
        StatementResult::AffectedRows(1)
    );
    {
        let visible = db.lazy_catalog_lookup(1, None, None);
        assert!(visible.lookup_any_relation("num_exp_add").is_none());
        assert!(visible.lookup_any_relation("num_exp_add_idx").is_none());
    }

    match db
        .execute(
            1,
            "select count(*) from pg_class where relname = 'num_exp_add_idx'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_index_without_name_uses_all_key_columns_in_default_name() {
    let dir = temp_dir("unnamed_multicol_index_name");
    let db = Database::open(&dir, 128).unwrap();

    db.execute(1, "create table items (a int, b int)").unwrap();
    assert_eq!(
        db.execute(1, "create index on items (a, b)").unwrap(),
        StatementResult::AffectedRows(0)
    );

    match db
        .execute(
            1,
            "select count(*) from pg_class where relname = 'items_a_b_idx'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn cluster_bootstraps_multiple_databases_and_connection_rules() {
    let base = scratch_temp_dir("cluster_bootstrap_databases");
    let cluster = Cluster::open(&base, 16).unwrap();
    let postgres = cluster.connect_database("postgres").unwrap();

    assert_eq!(
        query_rows(
            &postgres,
            1,
            "select datname, datallowconn, datistemplate from pg_database order by datname",
        ),
        vec![
            vec![
                Value::Text("postgres".into()),
                Value::Bool(true),
                Value::Bool(false),
            ],
            vec![
                Value::Text("template0".into()),
                Value::Bool(false),
                Value::Bool(true),
            ],
            vec![
                Value::Text("template1".into()),
                Value::Bool(true),
                Value::Bool(true),
            ],
        ]
    );

    let template1 = cluster.connect_database("template1").unwrap();
    assert_eq!(
        query_rows(&template1, 2, "select 1"),
        vec![vec![Value::Int32(1)]]
    );

    match cluster.connect_database("template0") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "55000");
            assert!(message.contains("template0"));
        }
        Ok(_) => panic!("expected template0 connection rejection"),
        Err(_) => panic!("expected template0 connection rejection"),
    }

    match cluster.connect_database("missingdb") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "3D000");
            assert!(message.contains("missingdb"));
        }
        Ok(_) => panic!("expected missing database error"),
        Err(_) => panic!("expected missing database error"),
    }
}

#[test]
fn create_database_clones_template1_and_persists_across_reopen() {
    let base = temp_dir("create_database_cluster");
    let cluster = Cluster::open_with_options(base.clone(), DatabaseOpenOptions::new(16)).unwrap();
    let template1 = cluster.connect_database("template1").unwrap();
    let mut template_session = Session::new(1);
    template_session
        .execute(&template1, "create table template_seed (id int4)")
        .unwrap();
    template_session
        .execute(&template1, "insert into template_seed values (7)")
        .unwrap();

    let postgres = cluster.connect_database("postgres").unwrap();
    let mut admin = Session::new(2);
    admin
        .execute(&postgres, "create database analytics")
        .unwrap();

    let analytics = cluster.connect_database("analytics").unwrap();
    assert_eq!(
        query_rows(
            &analytics,
            3,
            "select datname from pg_database order by datname"
        ),
        vec![
            vec![Value::Text("analytics".into())],
            vec![Value::Text("postgres".into())],
            vec![Value::Text("template0".into())],
            vec![Value::Text("template1".into())],
        ]
    );
    assert_eq!(
        query_rows(&analytics, 3, "select id from template_seed"),
        vec![vec![Value::Int32(7)]]
    );

    let mut analytics_session = Session::new(4);
    analytics_session
        .execute(&analytics, "create table analytics_only (id int4)")
        .unwrap();
    analytics_session
        .execute(&analytics, "insert into analytics_only values (11)")
        .unwrap();

    match postgres.execute(5, "select id from analytics_only") {
        Err(ExecError::Parse(ParseError::UnknownTable(name))) => {
            assert_eq!(name, "analytics_only");
        }
        other => panic!("expected postgres-local isolation error, got {:?}", other),
    }

    drop(analytics_session);
    drop(analytics);
    drop(admin);
    drop(postgres);
    drop(template_session);
    drop(template1);
    drop(cluster);

    let reopened = Cluster::open(&base, 16).unwrap();
    let analytics = reopened.connect_database("analytics").unwrap();
    assert_eq!(
        query_rows(&analytics, 6, "select id from template_seed"),
        vec![vec![Value::Int32(7)]]
    );
    assert_eq!(
        query_rows(&analytics, 6, "select id from analytics_only"),
        vec![vec![Value::Int32(11)]]
    );
}

#[test]
fn drop_database_rejects_current_and_active_connections_then_removes_files() {
    let base = temp_dir("drop_database_cluster");
    let cluster = Cluster::open(&base, 16).unwrap();
    let postgres = cluster.connect_database("postgres").unwrap();
    let mut admin = Session::new(1);
    admin.execute(&postgres, "create database doomed").unwrap();

    let doomed = cluster.connect_database("doomed").unwrap();
    let doomed_oid = doomed.database_oid;
    let doomed_dir = base.join("base").join(doomed_oid.to_string());
    assert!(doomed_dir.exists());

    let mut doomed_session = Session::new(2);
    match doomed_session.execute(&doomed, "drop database doomed") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "55006");
            assert!(message.contains("currently open database"));
        }
        other => panic!("expected current database rejection, got {:?}", other),
    }

    cluster.register_connection(doomed_oid);
    match admin.execute(&postgres, "drop database doomed") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "55006");
            assert!(message.contains("being accessed by other users"));
        }
        other => panic!("expected active connection rejection, got {:?}", other),
    }
    cluster.unregister_connection(doomed_oid);

    admin.execute(&postgres, "drop database doomed").unwrap();
    assert!(!doomed_dir.exists());

    match cluster.connect_database("doomed") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "3D000");
            assert!(message.contains("doomed"));
        }
        Ok(_) => panic!("expected dropped database to disappear"),
        Err(_) => panic!("expected dropped database to disappear"),
    }

    match admin.execute(&postgres, "drop database template0") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "55006");
            assert!(message.contains("cannot drop database"));
        }
        other => panic!("expected template database rejection, got {:?}", other),
    }
}

#[test]
fn create_schema_creates_namespace_row_and_allows_qualified_create_table() {
    let db = Database::open_ephemeral(16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create schema tenant").unwrap();
    session
        .execute(&db, "create table tenant.items (id int4)")
        .unwrap();

    match session
        .execute(
            &db,
            "select n.nspname, c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = 'items'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("tenant".into()),
                    Value::Text("items".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_schema_executes_embedded_create_table_elements() {
    let db = Database::open_ephemeral(16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create schema tenant
               create table parents (id int4 primary key)
               create table children (id int4 primary key, parent_id int4 references parents)",
        )
        .unwrap();

    session
        .execute(&db, "insert into tenant.parents values (1)")
        .unwrap();
    session
        .execute(&db, "insert into tenant.children values (10, 1)")
        .unwrap();

    match session
        .execute(
            &db,
            "select n.nspname, c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'tenant' and c.relkind = 'r' order by c.relname",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Text("tenant".into()),
                        Value::Text("children".into()),
                    ],
                    vec![Value::Text("tenant".into()), Value::Text("parents".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_schema_supports_authorization_and_if_not_exists() {
    let db = Database::open_ephemeral(16).unwrap();

    db.execute(1, "create schema authorization postgres")
        .unwrap();
    db.execute(1, "create schema if not exists postgres")
        .unwrap();

    match db
        .execute(
            1,
            "select n.nspname, a.rolname from pg_namespace n join pg_authid a on a.oid = n.nspowner where n.nspname = 'postgres'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("postgres".into()),
                    Value::Text("postgres".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_schema_reports_duplicate_and_reserved_name_errors() {
    let db = Database::open_ephemeral(16).unwrap();

    db.execute(1, "create schema tenant").unwrap();

    match db.execute(1, "create schema tenant") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "42P06");
            assert!(message.contains("schema \"tenant\" already exists"));
        }
        other => panic!("expected duplicate schema error, got {:?}", other),
    }

    match db.execute(1, "create schema pg_custom") {
        Err(ExecError::DetailedError {
            sqlstate, message, ..
        }) => {
            assert_eq!(sqlstate, "42939");
            assert!(message.contains("unacceptable schema name"));
        }
        other => panic!("expected reserved schema name error, got {:?}", other),
    }
}

#[test]
fn create_schema_respects_search_path_for_unqualified_create_table() {
    let db = Database::open_ephemeral(16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create schema tenant").unwrap();
    session.execute(&db, "set search_path to tenant").unwrap();
    session
        .execute(&db, "create table items (id int4)")
        .unwrap();

    match session
        .execute(
            &db,
            "select n.nspname, c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = 'items'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("tenant".into()),
                    Value::Text("items".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_view_uses_created_schema_namespace() {
    let db = Database::open_ephemeral(16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create schema tenant").unwrap();
    session
        .execute(&db, "create table tenant.items (id int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create view tenant.item_view as select id from tenant.items",
        )
        .unwrap();

    match session
        .execute(
            &db,
            "select n.nspname, c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = 'item_view'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("tenant".into()),
                    Value::Text("item_view".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn comment_on_table_upserts_and_clears_pg_description() {
    let base = temp_dir("comment_on_table");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "comment on table items is 'hello world'")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'items' and d.classoid = 1259 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("hello world".into())]]
    );

    db.execute(1, "comment on table items is 'second comment'")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'items' and d.classoid = 1259 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("second comment".into())]]
    );

    db.execute(1, "comment on table items is null").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'items' and d.classoid = 1259 and d.objsubid = 0"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn comment_on_index_upserts_and_clears_pg_description() {
    let base = temp_dir("comment_on_index");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "create index items_idx on items (id)")
        .unwrap();
    db.execute(1, "comment on index items_idx is 'hello world'")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'items_idx' and d.classoid = 1259 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("hello world".into())]]
    );

    db.execute(1, "comment on index items_idx is 'second comment'")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'items_idx' and d.classoid = 1259 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("second comment".into())]]
    );

    db.execute(1, "comment on index items_idx is null").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'items_idx' and d.classoid = 1259 and d.objsubid = 0"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn create_aggregate_supports_plain_custom_aggregate_execution() {
    let base = temp_dir("create_aggregate_execution");
    let db = Database::open(&base, 16).unwrap();

    create_plain_test_aggregates(&db);
    create_plain_test_aggregate_inputs(&db);
    let newavg_oid = int_value(
        &query_rows(
            &db,
            1,
            "select oid from pg_proc where proname = 'newavg' and prokind = 'a'",
        )[0][0],
    ) as u32;
    let snapshot = db
        .txns
        .read()
        .snapshot(crate::backend::access::transam::xact::INVALID_TRANSACTION_ID)
        .unwrap();
    let txns = db.txns.read();
    let visible_rows = load_physical_catalog_rows_visible_scoped(
        &db.cluster.base_dir,
        &db.pool,
        &txns,
        &snapshot,
        1,
        db.database_oid,
        &crate::include::catalog::bootstrap_catalog_kinds(),
    )
    .unwrap();
    let local_catcache = db
        .catalog
        .read()
        .catcache_with_snapshot(&db.pool, &txns, &snapshot, 1)
        .unwrap();
    let backend_catcache = db.backend_catcache(1, None).unwrap();
    assert!(
        visible_rows
            .aggregates
            .iter()
            .any(|row| row.aggfnoid == newavg_oid),
        "visible aggregate rows should include newavg; got {:?}",
        visible_rows
            .aggregates
            .iter()
            .map(|row| row.aggfnoid)
            .collect::<Vec<_>>()
    );
    assert!(
        local_catcache.aggregate_by_fnoid(newavg_oid).is_some(),
        "local catcache aggregate rows: {:?}",
        local_catcache
            .aggregate_rows()
            .into_iter()
            .map(|row| row.aggfnoid)
            .collect::<Vec<_>>()
    );
    assert!(
        backend_catcache.aggregate_by_fnoid(newavg_oid).is_some(),
        "backend catcache aggregate rows: {:?}",
        backend_catcache
            .aggregate_rows()
            .into_iter()
            .map(|row| row.aggfnoid)
            .collect::<Vec<_>>()
    );
    drop(txns);

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
             newavg(four), \
             newsum(four), \
             newcnt(four), \
             newcnt(*), \
             oldcnt(*), \
             sum2(q1, q2) \
             from agg_input"
        ),
        vec![vec![
            Value::Numeric("2".into()),
            Value::Int32(6),
            Value::Int64(3),
            Value::Int64(4),
            Value::Int64(4),
            Value::Int64(124),
        ]]
    );
}

#[test]
fn reopen_backfills_missing_pg_aggregate_bootstrap_rows() {
    let base = temp_dir("aggregate_backfill_reopen");
    let db = Database::open(&base, 16).unwrap();
    let db_oid = db.database_oid;
    drop(db);

    sync_catalog_rows_subset(
        &base,
        &PhysicalCatalogRows::default(),
        db_oid,
        &[BootstrapCatalogKind::PgAggregate],
    )
    .unwrap();

    let reopened = Database::open(&base, 16).unwrap();
    assert!(
        reopened
            .backend_catcache(1, None)
            .unwrap()
            .aggregate_by_fnoid(6219)
            .is_some()
    );
    assert_eq!(
        query_rows(
            &reopened,
            1,
            "select count(*) from pg_aggregate where aggfnoid = 6219"
        ),
        vec![vec![Value::Int64(1)]]
    );
}

#[test]
fn reopen_missing_pg_aggregate_custom_rows_is_corrupt() {
    let base = temp_dir("aggregate_backfill_custom_corrupt");
    let db = Database::open(&base, 16).unwrap();
    let db_oid = db.database_oid;

    create_plain_test_aggregates(&db);
    drop(db);

    sync_catalog_rows_subset(
        &base,
        &PhysicalCatalogRows::default(),
        db_oid,
        &[BootstrapCatalogKind::PgAggregate],
    )
    .unwrap();

    match Database::open(&base, 16) {
        Err(DatabaseError::Catalog(crate::backend::catalog::CatalogError::Corrupt(
            "missing pg_aggregate row for custom aggregate",
        ))) => {}
        Err(err) => panic!("unexpected reopen error: {err:?}"),
        Ok(_) => panic!("expected reopen to fail"),
    }
}

#[test]
fn comment_on_missing_index_reports_relation_does_not_exist() {
    let base = temp_dir("comment_on_missing_index");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();

    match db.execute(1, "comment on index missing_idx is 'nope'") {
        Err(ExecError::Parse(ParseError::TableDoesNotExist(name))) if name == "missing_idx" => {}
        other => panic!("expected missing index error, got {:?}", other),
    }
}

#[test]
fn comment_on_aggregate_uses_pg_proc_description_rows() {
    let base = temp_dir("comment_on_aggregate");
    let db = Database::open(&base, 16).unwrap();

    create_plain_test_aggregates(&db);

    db.execute(1, "comment on aggregate newcnt(*) is 'an agg(*) comment'")
        .unwrap();
    db.execute(
        1,
        "comment on aggregate newcnt(\"any\") is 'an agg(any) comment'",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            &format!(
                "select p.pronargs, d.description \
                 from pg_description d \
                 join pg_proc p on p.oid = d.objoid \
                 where p.proname = 'newcnt' \
                   and p.prokind = 'a' \
                   and d.classoid = {} \
                   and d.objsubid = 0 \
                 order by p.pronargs",
                PG_PROC_RELATION_OID
            )
        ),
        vec![
            vec![Value::Int16(0), Value::Text("an agg(*) comment".into())],
            vec![Value::Int16(1), Value::Text("an agg(any) comment".into())],
        ]
    );

    db.execute(1, "comment on aggregate newcnt(*) is null")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            &format!(
                "select p.pronargs, d.description \
                 from pg_description d \
                 join pg_proc p on p.oid = d.objoid \
                 where p.proname = 'newcnt' \
                   and p.prokind = 'a' \
                   and d.classoid = {} \
                   and d.objsubid = 0 \
                 order by p.pronargs",
                PG_PROC_RELATION_OID
            )
        ),
        vec![vec![
            Value::Int16(1),
            Value::Text("an agg(any) comment".into()),
        ]]
    );
}

#[test]
fn comment_on_function_uses_pg_proc_description_rows() {
    let base = temp_dir("comment_on_function");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create function add_one(x int4) returns int4 language plpgsql as $$ begin return x + 1; end $$",
    )
    .unwrap();

    db.execute(1, "comment on function add_one(int4) is 'increments input'")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            &format!(
                "select d.description \
                 from pg_description d \
                 join pg_proc p on p.oid = d.objoid \
                 where p.proname = 'add_one' \
                   and p.prokind = 'f' \
                   and d.classoid = {} \
                   and d.objsubid = 0",
                PG_PROC_RELATION_OID
            )
        ),
        vec![vec![Value::Text("increments input".into())]]
    );

    db.execute(1, "comment on function add_one(int4) is null")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            &format!(
                "select count(*) \
                 from pg_description d \
                 join pg_proc p on p.oid = d.objoid \
                 where p.proname = 'add_one' \
                   and p.prokind = 'f' \
                   and d.classoid = {} \
                   and d.objsubid = 0",
                PG_PROC_RELATION_OID
            )
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn drop_aggregate_removes_proc_and_aggregate_rows() {
    let base = temp_dir("drop_aggregate_rows");
    let db = Database::open(&base, 16).unwrap();

    create_plain_test_aggregates(&db);
    create_plain_test_aggregate_inputs(&db);

    let proc_oid = int_value(
        &query_rows(
            &db,
            1,
            "select oid \
             from pg_proc \
             where proname = 'newcnt' and prokind = 'a' and pronargs = 0",
        )[0][0],
    );
    assert!(visible_aggregate_row(&db, 1, proc_oid as u32).is_some());

    db.execute(1, "drop aggregate newcnt(*)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            &format!("select count(*) from pg_proc where oid = {proc_oid}"),
        ),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(visible_aggregate_row(&db, 1, proc_oid as u32), None);

    match db.execute(1, "select newcnt(*) from agg_input") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual }))
            if expected.contains("supported") && actual == "newcnt" => {}
        other => panic!("expected dropped aggregate call failure, got {other:?}"),
    }
}

#[test]
fn create_or_replace_aggregate_preserves_proc_oid() {
    let base = temp_dir("replace_aggregate_oid");
    let db = Database::open(&base, 16).unwrap();

    create_plain_test_aggregates(&db);
    create_plain_test_aggregate_inputs(&db);

    let before_rows = query_rows(
        &db,
        1,
        "select oid, proparallel \
         from pg_proc \
         where proname = 'sum2'",
    );
    assert_eq!(before_rows.len(), 1);
    let before_oid = int_value(&before_rows[0][0]);
    assert_eq!(before_rows[0][1], Value::Text("u".into()));
    assert_eq!(
        visible_aggregate_row(&db, 1, before_oid as u32)
            .expect("sum2 aggregate metadata should exist")
            .agginitval,
        Some("0".into())
    );
    assert_eq!(
        query_rows(&db, 1, "select sum2(q1, q2) from agg_input"),
        vec![vec![Value::Int64(124)]]
    );

    db.execute(
        1,
        "create or replace aggregate sum2(int8, int8) ( \
         sfunc = sum3, stype = int8, \
         initcond = '10', parallel = safe)",
    )
    .unwrap();

    let after_rows = query_rows(
        &db,
        1,
        "select oid, proparallel \
         from pg_proc \
         where proname = 'sum2'",
    );
    assert_eq!(after_rows.len(), 1);
    assert_eq!(int_value(&after_rows[0][0]), before_oid);
    assert_eq!(after_rows[0][1], Value::Text("s".into()));
    assert_eq!(
        visible_aggregate_row(&db, 1, before_oid as u32)
            .expect("sum2 aggregate metadata should still exist")
            .agginitval,
        Some("10".into())
    );
    assert_eq!(
        query_rows(&db, 1, "select sum2(q1, q2) from agg_input"),
        vec![vec![Value::Int64(134)]]
    );
}

#[test]
fn custom_aggregate_window_execution_is_rejected() {
    let base = temp_dir("aggregate_window_rejected");
    let db = Database::open(&base, 16).unwrap();

    create_plain_test_aggregates(&db);
    create_plain_test_aggregate_inputs(&db);

    match db.execute(1, "select newcnt(four) over () from agg_input") {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "window execution for custom aggregate newcnt" => {}
        other => panic!("expected custom aggregate window rejection, got {other:?}"),
    }
}

#[test]
fn comment_on_constraint_upserts_and_clears_pg_description() {
    let base = temp_dir("comment_on_constraint");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table items (id int4 constraint items_id_positive check (id > 0))",
    )
    .unwrap();
    db.execute(
        1,
        "comment on constraint items_id_positive on items is 'hello world'",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_constraint c on c.oid = d.objoid \
             where c.conname = 'items_id_positive' and d.classoid = 2606 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("hello world".into())]]
    );

    db.execute(
        1,
        "comment on constraint items_id_positive on items is 'second comment'",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_constraint c on c.oid = d.objoid \
             where c.conname = 'items_id_positive' and d.classoid = 2606 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("second comment".into())]]
    );

    db.execute(
        1,
        "comment on constraint items_id_positive on items is null",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) \
             from pg_description d \
             join pg_constraint c on c.oid = d.objoid \
             where c.conname = 'items_id_positive' and d.classoid = 2606 and d.objsubid = 0"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn comment_on_missing_constraint_reports_table_name() {
    let base = temp_dir("comment_on_missing_constraint");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();

    match db.execute(1, "comment on constraint missing on items is 'nope'") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual }))
            if expected == "existing table constraint"
                && actual == "constraint \"missing\" for table \"items\" does not exist" => {}
        other => panic!("expected missing constraint error, got {:?}", other),
    }
}

#[test]
fn comment_on_trigger_upserts_and_clears_pg_description() {
    let base = temp_dir("comment_on_trigger");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(
        1,
        "create function trig_fn() returns trigger language plpgsql as 'begin return new; end;'",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger item_trigger before insert on items for each row execute procedure trig_fn()",
    )
    .unwrap();

    db.execute(
        1,
        "comment on trigger item_trigger on items is 'hello world'",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_trigger t on t.oid = d.objoid \
             where t.tgname = 'item_trigger' and d.classoid = 2620 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("hello world".into())]]
    );

    db.execute(
        1,
        "comment on trigger item_trigger on items is 'second comment'",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_trigger t on t.oid = d.objoid \
             where t.tgname = 'item_trigger' and d.classoid = 2620 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("second comment".into())]]
    );

    db.execute(1, "comment on trigger item_trigger on items is null")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) \
             from pg_description d \
             join pg_trigger t on t.oid = d.objoid \
             where t.tgname = 'item_trigger' and d.classoid = 2620 and d.objsubid = 0"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn comment_on_missing_trigger_reports_table_name() {
    let base = temp_dir("comment_on_missing_trigger");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();

    match db.execute(1, "comment on trigger missing on items is 'nope'") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message == "trigger \"missing\" for table \"items\" does not exist"
            && sqlstate == "42704" => {}
        other => panic!("expected missing trigger error, got {:?}", other),
    }
}

#[test]
fn create_trigger_reports_postgres_style_instead_of_table_error() {
    let base = temp_dir("trigger_instead_of_table_error");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(
        1,
        "create function items_trig() returns trigger language plpgsql as $$ begin return new; end $$",
    )
    .unwrap();

    match db.execute(
        1,
        "create trigger items_instead instead of insert on items for each row execute function items_trig()",
    ) {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) if message == "\"items\" is a table"
            && detail.as_deref() == Some("Tables cannot have INSTEAD OF triggers.")
            && sqlstate == "42809" => {}
        other => panic!("expected table/instead-of error, got {:?}", other),
    }
}

#[test]
fn create_trigger_reports_postgres_style_transition_table_errors() {
    let base = temp_dir("trigger_transition_table_errors");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "create view item_view as select * from items")
        .unwrap();
    db.execute(
        1,
        "create table parent_part (id int4, note text) partition by range (id)",
    )
    .unwrap();
    db.execute(
        1,
        "create table child_part partition of parent_part for values from (0) to (10)",
    )
    .unwrap();
    db.execute(1, "create table parent_inh (id int4, note text)")
        .unwrap();
    db.execute(1, "create table child_inh () inherits (parent_inh)")
        .unwrap();
    db.execute(
        1,
        "create function items_trig() returns trigger language plpgsql as $$ begin return new; end $$",
    )
    .unwrap();

    let assert_error = |sql: &str,
                        expected_message: &str,
                        expected_detail: Option<&str>,
                        expected_sqlstate: &str| {
        match db.execute(1, sql) {
            Err(ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            }) if message == expected_message
                && detail.as_deref() == expected_detail
                && sqlstate == expected_sqlstate => {}
            other => panic!("expected {expected_message:?} error, got {:?}", other),
        }
    };

    assert_error(
        "create trigger items_multi after insert or update on items referencing new table as new_rows for each statement execute function items_trig()",
        "transition tables cannot be specified for triggers with more than one event",
        None,
        "0A000",
    );

    match db.execute(
        1,
        "create trigger items_rowref after insert on items referencing new row as new_row for each statement execute function items_trig()",
    ) {
        Err(ExecError::DetailedError {
            message, hint, ..
        }) if message == "ROW variable naming in the REFERENCING clause is not supported"
            && hint.as_deref()
                == Some("Use OLD TABLE or NEW TABLE for naming transition tables.") => {}
        other => panic!("expected referencing row-name error, got {:?}", other),
    }

    assert_error(
        "create trigger items_dup_new after insert on items referencing new table as new_rows new table as newer_rows for each statement execute function items_trig()",
        "NEW TABLE cannot be specified multiple times",
        None,
        "42P17",
    );
    assert_error(
        "create trigger items_bad_old after insert on items referencing old table as old_rows for each statement execute function items_trig()",
        "OLD TABLE can only be specified for a DELETE or UPDATE trigger",
        None,
        "42P17",
    );
    assert_error(
        "create trigger items_same after update on items referencing old table as rows new table as ROWS for each statement execute function items_trig()",
        "OLD TABLE name and NEW TABLE name cannot be the same",
        None,
        "42P17",
    );
    assert_error(
        "create trigger items_before before insert on items referencing new table as new_rows for each statement execute function items_trig()",
        "transition table name can only be specified for an AFTER trigger",
        None,
        "42P17",
    );
    assert_error(
        "create trigger items_col after update of note on items referencing new table as new_rows for each statement execute function items_trig()",
        "transition tables cannot be specified for triggers with column lists",
        None,
        "0A000",
    );
    assert_error(
        "create trigger items_trunc after truncate on items referencing old table as old_rows for each statement execute function items_trig()",
        "TRUNCATE triggers with transition tables are not supported",
        None,
        "0A000",
    );
    assert_error(
        "create trigger view_ref after insert on item_view referencing new table as new_rows for each statement execute function items_trig()",
        "\"item_view\" is a view",
        Some("Triggers on views cannot have transition tables."),
        "42809",
    );
    assert_error(
        "create trigger parent_part_ref after insert on parent_part referencing new table as new_rows for each row execute function items_trig()",
        "\"parent_part\" is a partitioned table",
        Some("ROW triggers with transition tables are not supported on partitioned tables."),
        "42809",
    );
    assert_error(
        "create trigger child_part_ref after insert on child_part referencing new table as new_rows for each row execute function items_trig()",
        "ROW triggers with transition tables are not supported on partitions",
        None,
        "0A000",
    );
    assert_error(
        "create trigger child_inh_ref after insert on child_inh referencing new table as new_rows for each row execute function items_trig()",
        "ROW triggers with transition tables are not supported on inheritance children",
        None,
        "0A000",
    );
}

#[test]
fn statement_trigger_return_value_is_ignored() {
    let dir = temp_dir("statement_trigger_return_value_ignored");
    let db = Database::open(&dir, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(
        1,
        "create function stmt_returns_new() returns trigger language plpgsql as $$ begin raise notice 'stmt fired'; return NEW; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_stmt before insert on items for each statement execute function stmt_returns_new()",
    )
    .unwrap();

    clear_notices();
    db.execute(1, "insert into items values (1)").unwrap();

    assert_eq!(take_notice_messages(), vec![String::from("stmt fired")]);
    assert_eq!(
        query_rows(&db, 1, "select id from items"),
        vec![vec![Value::Int32(1)]]
    );
}

#[test]
fn transition_table_statement_triggers_can_read_statement_rows() {
    let dir = temp_dir("transition_table_statement_rows");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "create table new_rows (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into new_rows values (100, 'catalog')")
        .unwrap();
    db.execute(
        1,
        "create function insert_transition_notice() returns trigger language plpgsql as $$
         declare c int4; dyn int4 := 0; loop_id int4; sum_ids int4 := 0; expr_c int4;
         begin
           select count(*) into c from new_rows;
           perform count(*) from new_rows;
           for dyn in execute 'select count(*) from new_rows' loop
             null;
           end loop;
           for loop_id in select id from new_rows loop
             sum_ids := sum_ids + loop_id;
           end loop;
           expr_c := (select count(*) from new_rows);
           raise notice 'insert:%:%:%:%', c, dyn, expr_c, sum_ids;
           return null;
         end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function update_transition_notice() returns trigger language plpgsql as $$
         declare oc int4; nc int4; osum int4; nsum int4;
         begin
           select count(*), sum(id) into oc, osum from old_rows;
           select count(*), sum(id) into nc, nsum from new_rows;
           raise notice 'update:%:%:%:%', oc, nc, osum, nsum;
           return null;
         end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function delete_transition_notice() returns trigger language plpgsql as $$
         declare oc int4; osum int4;
         begin
           select count(*), sum(id) into oc, osum from old_rows;
           raise notice 'delete:%:%', oc, osum;
           return null;
         end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_insert_ref after insert on items referencing new table as new_rows for each statement execute function insert_transition_notice()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_update_ref after update on items referencing old table as old_rows new table as new_rows for each statement execute function update_transition_notice()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_delete_ref after delete on items referencing old table as old_rows for each statement execute function delete_transition_notice()",
    )
    .unwrap();

    clear_notices();
    db.execute(1, "insert into items values (1, 'a'), (2, 'b')")
        .unwrap();
    assert_eq!(take_notice_messages(), vec!["insert:2:2:2:3".to_string()]);

    clear_notices();
    db.execute(1, "update items set id = id + 10 where id <= 2")
        .unwrap();
    assert_eq!(take_notice_messages(), vec!["update:2:2:3:23".to_string()]);

    clear_notices();
    db.execute(1, "delete from items where id in (11, 12)")
        .unwrap();
    assert_eq!(take_notice_messages(), vec!["delete:2:23".to_string()]);
}

#[test]
fn row_transition_table_triggers_see_full_statement_set() {
    let dir = temp_dir("transition_table_row_trigger_rows");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "create function row_transition_notice() returns trigger language plpgsql as $$
         declare c int4;
         begin
           select count(*) into c from new_rows;
           raise notice 'row:%:%', NEW.id, c;
           return NEW;
         end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_row_ref after insert on items referencing new table as new_rows for each row execute function row_transition_notice()",
    )
    .unwrap();

    clear_notices();
    db.execute(1, "insert into items values (1, 'a'), (2, 'b')")
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec!["row:1:2".to_string(), "row:2:2".to_string()]
    );
}

#[test]
fn transition_tables_cannot_be_referenced_by_persistent_objects() {
    let dir = temp_dir("transition_table_persistent_object");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(
        1,
        "create function make_bogus_matview() returns trigger language plpgsql as $$
         begin
           create materialized view transition_test_mv as select * from new_table;
           return NEW;
         end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_ref after insert on items referencing new table as new_table for each statement execute function make_bogus_matview()",
    )
    .unwrap();

    match db.execute(1, "insert into items values (42)") {
        Err(ExecError::Parse(ParseError::DetailedError {
            message, sqlstate, ..
        })) if message
            == "transition table \"new_table\" cannot be referenced in a persistent object"
            && sqlstate == "0A000" => {}
        other => panic!("expected persistent transition-table reference error, got {other:?}"),
    }
}

#[test]
fn create_comment_and_drop_rule_updates_catalogs() {
    let base = temp_dir("rule_catalog_rows");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "create table item_log (id int4 not null)")
        .unwrap();
    db.execute(
        1,
        "create rule item_log_rule as on insert to items do also insert into item_log values (new.id)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select rulename, ev_qual, ev_action, is_instead \
             from pg_rewrite r \
             join pg_class c on c.oid = r.ev_class \
             where c.relname = 'items' and rulename = 'item_log_rule'"
        ),
        vec![vec![
            Value::Text("item_log_rule".into()),
            Value::Text("".into()),
            Value::Text("insert into item_log values (new.id)".into()),
            Value::Bool(false),
        ]]
    );

    db.execute(
        1,
        "comment on rule item_log_rule on items is 'tracks inserts'",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_rewrite r on r.oid = d.objoid \
             where r.rulename = 'item_log_rule' and d.classoid = 2618 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("tracks inserts".into())]]
    );

    db.execute(1, "drop rule item_log_rule on items").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) \
             from pg_rewrite r \
             join pg_class c on c.oid = r.ev_class \
             where c.relname = 'items' and r.rulename = 'item_log_rule'"
        ),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) \
             from pg_description d \
             join pg_rewrite r on r.oid = d.objoid \
             where r.rulename = 'item_log_rule' and d.classoid = 2618 and d.objsubid = 0"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn create_rule_on_select_is_rejected() {
    let base = temp_dir("rule_on_select_rejected");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();

    match db.execute(
        1,
        "create rule item_select_rule as on select to items do instead delete from items where id = 1",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "CREATE RULE ... ON SELECT" => {}
        other => panic!("expected ON SELECT rule rejection, got {other:?}"),
    }
}

#[test]
fn insert_rules_support_do_also_and_instead_nothing() {
    let base = temp_dir("rule_insert_do_also");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "create table item_log (id int4 not null)")
        .unwrap();
    db.execute(
        1,
        "create rule item_log_rule as on insert to items do also insert into item_log values (new.id)",
    )
    .unwrap();

    db.execute(1, "insert into items values (1), (2)").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id from items order by id"),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select id from item_log order by id"),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]
    );

    db.execute(1, "drop rule item_log_rule on items").unwrap();
    db.execute(
        1,
        "create rule item_skip_rule as on insert to items where new.id < 10 do instead nothing",
    )
    .unwrap();
    db.execute(1, "insert into items values (3), (20)").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id from items order by id"),
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(20)],
        ]
    );
}

#[test]
fn create_rule_rejects_unqualified_action_reference() {
    let base = temp_dir("rule_unqualified_action");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table rules_foo (f1 int4)").unwrap();
    db.execute(1, "create table rules_foo2 (f1 int4)").unwrap();

    let err = db
        .execute(
            1,
            "create rule rules_foorule as on insert to rules_foo where f1 < 100 do instead insert into rules_foo2 values (f1)",
        )
        .unwrap_err();
    assert!(matches!(err, ExecError::Parse(ParseError::UnknownColumn(name)) if name == "f1"));
}

#[test]
fn view_dml_routes_through_instead_rules() {
    let base = temp_dir("rule_view_dml");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table base_items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "create view item_view as select id, name from base_items",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_ins as on insert to item_view do instead insert into base_items values (new.id, new.name)",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_upd as on update to item_view do instead update base_items set id = new.id, name = new.name where id = old.id",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_del as on delete to item_view do instead delete from base_items where id = old.id",
    )
    .unwrap();

    db.execute(1, "insert into item_view values (1, 'alpha')")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        vec![vec![Value::Int32(1), Value::Text("alpha".into())]]
    );

    db.execute(1, "update item_view set name = 'beta' where id = 1")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        vec![vec![Value::Int32(1), Value::Text("beta".into())]]
    );

    db.execute(1, "delete from item_view where id = 1").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn view_dml_returning_routes_through_instead_rules() {
    let base = temp_dir("rule_view_dml_returning");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table base_items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "create view item_view as select id, name from base_items",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_ins as on insert to item_view do instead insert into base_items values (new.id, new.name) returning id, name",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_upd as on update to item_view do instead update base_items set id = new.id, name = new.name where id = old.id returning id, name",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_del as on delete to item_view do instead delete from base_items where id = old.id returning id, name",
    )
    .unwrap();

    match db
        .execute(
            1,
            "insert into item_view values (1, 'alpha') returning name || '!' as excited_name",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["excited_name"]);
            assert_eq!(rows, vec![vec![Value::Text("alpha!".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match db
        .execute(
            1,
            "update item_view set name = 'beta' where id = 1 returning id + 10 as bumped_id, name",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["bumped_id", "name"]);
            assert_eq!(
                rows,
                vec![vec![Value::Int32(11), Value::Text("beta".into())]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match db
        .execute(
            1,
            "delete from item_view where id = 1 returning name || '!' as excited_name",
        )
        .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["excited_name"]);
            assert_eq!(rows, vec![vec![Value::Text("beta!".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

fn assert_rule_returning_error(err: ExecError, expected_message: &str, expected_hint: &str) {
    match err {
        ExecError::DetailedError {
            message,
            detail: None,
            hint: Some(hint),
            sqlstate,
        } => {
            assert_eq!(message, expected_message);
            assert_eq!(hint, expected_hint);
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected explicit-rule RETURNING error, got {other:?}"),
    }
}

#[test]
fn view_dml_returning_requires_rule_returning_clause() {
    let base = temp_dir("rule_view_missing_returning");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table base_items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "create view item_view as select id, name from base_items",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_ins as on insert to item_view do instead insert into base_items values (new.id, new.name)",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_upd as on update to item_view do instead update base_items set id = new.id, name = new.name where id = old.id",
    )
    .unwrap();
    db.execute(
        1,
        "create rule item_view_del as on delete to item_view do instead delete from base_items where id = old.id",
    )
    .unwrap();

    assert_rule_returning_error(
        db.execute(1, "insert into item_view values (1, 'alpha') returning id")
            .unwrap_err(),
        "cannot perform INSERT RETURNING on relation \"item_view\"",
        "You need an unconditional ON INSERT DO INSTEAD rule with a RETURNING clause.",
    );

    db.execute(1, "insert into base_items values (1, 'alpha')")
        .unwrap();
    assert_rule_returning_error(
        db.execute(
            1,
            "update item_view set name = 'beta' where id = 1 returning id",
        )
        .unwrap_err(),
        "cannot perform UPDATE RETURNING on relation \"item_view\"",
        "You need an unconditional ON UPDATE DO INSTEAD rule with a RETURNING clause.",
    );
    assert_rule_returning_error(
        db.execute(1, "delete from item_view where id = 1 returning id")
            .unwrap_err(),
        "cannot perform DELETE RETURNING on relation \"item_view\"",
        "You need an unconditional ON DELETE DO INSTEAD rule with a RETURNING clause.",
    );
}

#[test]
fn create_rule_rejects_invalid_returning_lists() {
    let base = temp_dir("rule_invalid_returning");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();

    match db.execute(
        1,
        "create rule items_ins as on insert to items where new.id > 0 do instead insert into items values (new.id, new.name) returning id, name",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "RETURNING lists are not supported in conditional rules" => {}
        other => panic!("expected conditional rule RETURNING rejection, got {other:?}"),
    }

    match db.execute(
        1,
        "create rule items_log as on insert to items do also insert into items values (new.id, new.name) returning id, name",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "RETURNING lists are not supported in non-INSTEAD rules" => {}
        other => panic!("expected non-INSTEAD RETURNING rejection, got {other:?}"),
    }

    match db.execute(
        1,
        "create rule items_multi as on insert to items do instead (insert into items values (new.id, new.name) returning id, name; insert into items values (new.id + 1, new.name) returning id, name;)",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "cannot have multiple RETURNING lists in a rule" => {}
        other => panic!("expected multiple RETURNING rejection, got {other:?}"),
    }
}

fn assert_view_dml_error(
    err: ExecError,
    expected_message: &str,
    expected_detail: &str,
    expected_hint_event: &str,
) {
    match err {
        ExecError::DetailedError {
            sqlstate,
            message,
            detail: Some(detail),
            hint: Some(hint),
        } => {
            assert_eq!(sqlstate, "55000");
            assert_eq!(message, expected_message);
            assert!(
                detail.contains(expected_detail),
                "expected detail `{expected_detail}`, got `{detail}`"
            );
            assert!(
                hint.contains(expected_hint_event),
                "expected hint event `{expected_hint_event}`, got `{hint}`"
            );
        }
        other => panic!("expected view DML detailed error, got {other:?}"),
    }
}

fn assert_view_column_dml_error(err: ExecError, expected_message: &str, expected_detail: &str) {
    match err {
        ExecError::DetailedError {
            sqlstate,
            message,
            detail: Some(detail),
            hint: None,
        } => {
            assert_eq!(sqlstate, "55000");
            assert_eq!(message, expected_message);
            assert_eq!(detail, expected_detail);
        }
        other => panic!("expected column-level view DML error, got {other:?}"),
    }
}

#[test]
fn simple_view_auto_dml_routes_to_base_table() {
    let base = temp_dir("auto_simple_view_dml");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table base_items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "create view item_view as select id, name from base_items",
    )
    .unwrap();

    db.execute(1, "insert into item_view values (1, 'alpha')")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        vec![vec![Value::Int32(1), Value::Text("alpha".into())]]
    );

    db.execute(1, "update item_view set name = 'beta' where id = 1")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        vec![vec![Value::Int32(1), Value::Text("beta".into())]]
    );

    db.execute(1, "delete from item_view where id = 1").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn auto_view_dml_returning_uses_view_projection() {
    let base = temp_dir("auto_view_dml_returning");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table people (id int4 not null, given_name text, tenant text default 'main' not null)",
    )
    .unwrap();
    db.execute(
        1,
        "create view public_people as select id as person_id, given_name as display_name from people",
    )
    .unwrap();

    match db
        .execute(1, "insert into public_people values (1, 'Ada') returning *")
        .unwrap()
    {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(
                columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["person_id", "display_name"]
            );
            assert_eq!(rows, vec![vec![Value::Int32(1), Value::Text("Ada".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match db
        .execute(
            1,
            "update public_people set display_name = 'Grace' where person_id = 1 returning person_id, display_name || '!' as emphasized_name",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int32(1), Value::Text("Grace!".into())]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match db
        .execute(
            1,
            "delete from public_people where person_id = 1 returning display_name, person_id",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Text("Grace".into()), Value::Int32(1)]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, given_name, tenant from people order by id",
        ),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn nested_simple_views_auto_dml_route_to_base_table() {
    let base = temp_dir("auto_nested_view_dml");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table base_items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "create view first_view as select id, name from base_items",
    )
    .unwrap();
    db.execute(
        1,
        "create view second_view as select id, name from first_view",
    )
    .unwrap();

    db.execute(1, "insert into second_view values (1, 'alpha')")
        .unwrap();
    db.execute(1, "update second_view set name = 'beta' where id = 1")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        vec![vec![Value::Int32(1), Value::Text("beta".into())]]
    );

    db.execute(1, "delete from second_view where id = 1")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn nested_simple_views_auto_dml_returning_route_to_base_table() {
    let base = temp_dir("auto_nested_view_dml_returning");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table base_items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "create view first_view as select id as inner_id, name as inner_name from base_items",
    )
    .unwrap();
    db.execute(
        1,
        "create view second_view as select inner_id as outer_id, inner_name as outer_name from first_view",
    )
    .unwrap();

    match db
        .execute(1, "insert into second_view values (1, 'alpha') returning *")
        .unwrap()
    {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(
                columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["outer_id", "outer_name"]
            );
            assert_eq!(
                rows,
                vec![vec![Value::Int32(1), Value::Text("alpha".into())]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match db
        .execute(
            1,
            "update second_view set outer_name = 'beta' where outer_id = 1 returning outer_name",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("beta".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match db
        .execute(
            1,
            "delete from second_view where outer_id = 1 returning outer_id, outer_name || '!' as emphasized_name",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int32(1), Value::Text("beta!".into())]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select id, name from base_items"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn filtered_views_auto_update_delete_visible_rows_and_insert_can_hide_rows() {
    let base = temp_dir("auto_filtered_view_dml");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table base_items (id int4 not null, name text, active bool default false not null)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into base_items values (1, 'alpha', true), (2, 'beta', false)",
    )
    .unwrap();
    db.execute(
        1,
        "create view active_items as select id, name from base_items where active",
    )
    .unwrap();

    db.execute(1, "update active_items set name = 'seen'")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, name, active from base_items order by id"
        ),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("seen".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Int32(2),
                Value::Text("beta".into()),
                Value::Bool(false),
            ],
        ]
    );

    db.execute(1, "delete from active_items where id = 1")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, name, active from base_items order by id"
        ),
        vec![vec![
            Value::Int32(2),
            Value::Text("beta".into()),
            Value::Bool(false),
        ]]
    );

    db.execute(1, "insert into active_items values (3, 'hidden')")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, name, active from base_items order by id"
        ),
        vec![
            vec![
                Value::Int32(2),
                Value::Text("beta".into()),
                Value::Bool(false),
            ],
            vec![
                Value::Int32(3),
                Value::Text("hidden".into()),
                Value::Bool(false),
            ],
        ]
    );
    assert_eq!(
        query_rows(&db, 1, "select id, name from active_items order by id"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn auto_view_insert_maps_renamed_columns_and_hidden_defaults() {
    let base = temp_dir("auto_view_renamed_columns");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table people (id int4 not null, given_name text, tenant text default 'main' not null)",
    )
    .unwrap();
    db.execute(
        1,
        "create view public_people as select id as person_id, given_name as display_name from people",
    )
    .unwrap();

    db.execute(1, "insert into public_people values (1, 'Ada')")
        .unwrap();
    db.execute(
        1,
        "update public_people set display_name = 'Grace' where person_id = 1",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, given_name, tenant from people order by id",
        ),
        vec![vec![
            Value::Int32(1),
            Value::Text("Grace".into()),
            Value::Text("main".into()),
        ]]
    );
}

#[test]
fn non_simple_views_reject_auto_dml() {
    let base = temp_dir("auto_view_rejects_non_simple");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(1, "create table notes (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();
    db.execute(1, "insert into notes values (1, 'memo')")
        .unwrap();

    db.execute(
        1,
        "create view join_view as select items.id, notes.note from items join notes on notes.id = items.id",
    )
    .unwrap();
    db.execute(
        1,
        "create view aggregate_view as select count(*) as total from items",
    )
    .unwrap();
    db.execute(
        1,
        "create view computed_view as select id, id + 1 as next_id from items",
    )
    .unwrap();

    assert_view_dml_error(
        db.execute(1, "update join_view set note = 'x' where id = 1")
            .unwrap_err(),
        "cannot update view \"join_view\"",
        "single table or view",
        "ON UPDATE DO INSTEAD rule",
    );
    assert_view_dml_error(
        db.execute(1, "insert into aggregate_view values (1)")
            .unwrap_err(),
        "cannot insert into view \"aggregate_view\"",
        "aggregate functions",
        "ON INSERT DO INSTEAD rule",
    );
    assert_view_column_dml_error(
        db.execute(1, "update computed_view set next_id = 5 where id = 1")
            .unwrap_err(),
        "cannot update column \"next_id\" of view \"computed_view\"",
        "View columns that are not columns of their base relation are not updatable.",
    );
}

#[test]
fn insert_on_conflict_is_rejected_for_auto_updatable_views() {
    let base = temp_dir("auto_view_on_conflict");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 primary key)")
        .unwrap();
    db.execute(1, "create view item_view as select id from items")
        .unwrap();

    match db.execute(1, "insert into item_view values (1) on conflict do nothing") {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature))) => {
            assert!(feature.contains("automatically updatable views"));
        }
        other => panic!("expected ON CONFLICT feature rejection, got {other:?}"),
    }
}

#[test]
fn nested_views_with_user_rules_are_not_auto_updatable() {
    let base = temp_dir("auto_view_nested_rule_reject");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table base_items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "create view inner_view as select id, name from base_items",
    )
    .unwrap();
    db.execute(
        1,
        "create rule inner_view_upd as on update to inner_view do instead update base_items set name = new.name where id = old.id",
    )
    .unwrap();
    db.execute(
        1,
        "create view outer_view as select id, name from inner_view",
    )
    .unwrap();

    assert_view_dml_error(
        db.execute(1, "update outer_view set name = 'beta' where id = 1")
            .unwrap_err(),
        "cannot update view \"outer_view\"",
        "nested view \"inner_view\"",
        "ON UPDATE DO INSTEAD rule",
    );
}

#[test]
fn auto_view_errors_preserve_postgres_distinct_with_and_hint_text() {
    let base = temp_dir("auto_view_distinct_with_messages");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();
    db.execute(
        1,
        "create view distinct_view as select distinct id, name from items",
    )
    .unwrap();
    db.execute(
        1,
        "create view with_view as with q as (select * from items) select * from q",
    )
    .unwrap();

    match db
        .execute(1, "delete from distinct_view where id = 1")
        .unwrap_err()
    {
        ExecError::DetailedError {
            message,
            detail: Some(detail),
            hint: Some(hint),
            ..
        } => {
            assert_eq!(message, "cannot delete from view \"distinct_view\"");
            assert_eq!(
                detail,
                "Views containing DISTINCT are not automatically updatable."
            );
            assert_eq!(
                hint,
                "To enable deleting from the view, provide an INSTEAD OF DELETE trigger or an unconditional ON DELETE DO INSTEAD rule."
            );
        }
        other => panic!("expected distinct view DML error, got {other:?}"),
    }

    match db
        .execute(1, "update with_view set name = 'beta' where id = 1")
        .unwrap_err()
    {
        ExecError::DetailedError {
            message,
            detail: Some(detail),
            hint: Some(hint),
            ..
        } => {
            assert_eq!(message, "cannot update view \"with_view\"");
            assert_eq!(
                detail,
                "Views containing WITH are not automatically updatable."
            );
            assert_eq!(
                hint,
                "To enable updating the view, provide an INSTEAD OF UPDATE trigger or an unconditional ON UPDATE DO INSTEAD rule."
            );
        }
        other => panic!("expected WITH view DML error, got {other:?}"),
    }
}

#[test]
fn auto_view_errors_preserve_postgres_column_specific_text() {
    let base = temp_dir("auto_view_column_messages");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();
    db.execute(
        1,
        "create view system_view as select ctid, id, name from items",
    )
    .unwrap();
    db.execute(
        1,
        "create view duplicate_view as select id, name, id as duplicate_id from items",
    )
    .unwrap();

    assert_view_column_dml_error(
        db.execute(1, "insert into system_view values (null, 2, 'beta')")
            .unwrap_err(),
        "cannot insert into column \"ctid\" of view \"system_view\"",
        "View columns that refer to system columns are not updatable.",
    );

    match db
        .execute(1, "update duplicate_view set id = 2, duplicate_id = 3")
        .unwrap_err()
    {
        ExecError::DetailedError {
            sqlstate,
            message,
            detail: None,
            hint: None,
        } => {
            assert_eq!(sqlstate, "42601");
            assert_eq!(message, "multiple assignments to same column \"id\"");
        }
        other => panic!("expected duplicate-assignment error, got {other:?}"),
    }
}

#[test]
fn view_dml_is_visible_within_transaction_after_create_view() {
    let base = temp_dir("auto_view_txn_visibility");
    let db = Database::open(&base, 32).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table base_items (id int4 not null, name text)")
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "create view item_view as select id, name from base_items",
        )
        .unwrap();
    session
        .execute(&db, "insert into item_view values (1, 'alpha')")
        .unwrap();
    session
        .execute(&db, "update item_view set name = 'beta' where id = 1")
        .unwrap();
    match session
        .execute(&db, "select name from item_view where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("beta".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
    session
        .execute(&db, "delete from item_view where id = 1")
        .unwrap();
    match session
        .execute(&db, "select count(*) from base_items")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
    session.execute(&db, "commit").unwrap();
}

#[test]
fn cascading_insert_rules_execute_recursively() {
    let base = temp_dir("rule_cascade_insert");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table source_items (id int4 not null)")
        .unwrap();
    db.execute(1, "create table mid_items (id int4 not null)")
        .unwrap();
    db.execute(1, "create table leaf_items (id int4 not null)")
        .unwrap();
    db.execute(
        1,
        "create rule source_mid as on insert to source_items do also insert into mid_items values (new.id)",
    )
    .unwrap();
    db.execute(
        1,
        "create rule mid_leaf as on insert to mid_items do also insert into leaf_items values (new.id)",
    )
    .unwrap();

    db.execute(1, "insert into source_items values (7)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id from source_items"),
        vec![vec![Value::Int32(7)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select id from mid_items"),
        vec![vec![Value::Int32(7)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select id from leaf_items"),
        vec![vec![Value::Int32(7)]]
    );
}

#[test]
fn update_and_delete_rules_propagate_old_and_new_values() {
    let base = temp_dir("rule_update_delete");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table systems (name text)").unwrap();
    db.execute(1, "create table interfaces (name text)")
        .unwrap();
    db.execute(
        1,
        "create rule systems_upd as on update to systems where new.name != old.name do also update interfaces set name = new.name where name = old.name",
    )
    .unwrap();
    db.execute(
        1,
        "create rule systems_del as on delete to systems do also delete from interfaces where name = old.name",
    )
    .unwrap();

    db.execute(1, "insert into systems values ('alpha')")
        .unwrap();
    db.execute(1, "insert into interfaces values ('alpha')")
        .unwrap();

    db.execute(1, "update systems set name = 'beta' where name = 'alpha'")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select name from systems"),
        vec![vec![Value::Text("beta".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select name from interfaces"),
        vec![vec![Value::Text("beta".into())]]
    );

    db.execute(1, "delete from systems where name = 'beta'")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select name from systems"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        query_rows(&db, 1, "select name from interfaces"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn insert_rules_fire_in_alphabetical_order() {
    let base = temp_dir("rule_fire_order");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table source_items (id int4)")
        .unwrap();
    db.execute(1, "create table rule_log (step int8, note text)")
        .unwrap();
    db.execute(1, "create sequence rule_order_seq").unwrap();
    db.execute(
        1,
        "create rule rule_c as on insert to source_items do also insert into rule_log values (nextval('rule_order_seq'), 'rule_c')",
    )
    .unwrap();
    db.execute(
        1,
        "create rule rule_a as on insert to source_items do also insert into rule_log values (nextval('rule_order_seq'), 'rule_a')",
    )
    .unwrap();
    db.execute(
        1,
        "create rule rule_b as on insert to source_items do also insert into rule_log values (nextval('rule_order_seq'), 'rule_b')",
    )
    .unwrap();

    db.execute(1, "insert into source_items values (1)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select note from rule_log order by step"),
        vec![
            vec![Value::Text("rule_a".into())],
            vec![Value::Text("rule_b".into())],
            vec![Value::Text("rule_c".into())],
        ]
    );
}

#[test]
fn pg_rules_exposes_user_rules_but_not_return_rules() {
    let base = temp_dir("pg_rules_view");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(1, "create view item_view as select id from items")
        .unwrap();
    db.execute(
        1,
        "create rule item_view_ins as on insert to item_view do instead insert into items values (new.id)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select tablename, rulename from pg_rules where schemaname = 'public' order by tablename, rulename",
        ),
        vec![vec![
            Value::Text("item_view".into()),
            Value::Text("item_view_ins".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select definition from pg_rules where tablename = 'item_view' and rulename = 'item_view_ins'",
        ),
        vec![vec![Value::Text(
            "CREATE RULE item_view_ins AS ON INSERT TO public.item_view DO INSTEAD insert into items values (new.id)"
                .into(),
        )]]
    );
}

#[test]
fn current_user_compares_against_name_columns() {
    let base = temp_dir("current_user_name_compare");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table audit_log (who name)").unwrap();
    db.execute(1, "insert into audit_log values (current_user)")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select who = current_user from audit_log",),
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn comment_on_table_respects_txn_commit_and_rollback() {
    let base = temp_dir("comment_on_table_txn");
    {
        let db = Database::open_with_options(&base, DatabaseOpenOptions::new(16)).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table items (id int4 not null)")
            .unwrap();
        session.execute(&db, "begin").unwrap();
        session
            .execute(&db, "comment on table items is 'rolled back'")
            .unwrap();
        session.execute(&db, "rollback").unwrap();

        assert_eq!(
            query_rows(
                &db,
                1,
                "select count(*) \
                 from pg_description d \
                 join pg_class c on c.oid = d.objoid \
                 where c.relname = 'items' and d.classoid = 1259 and d.objsubid = 0"
            ),
            vec![vec![Value::Int64(0)]]
        );

        session.execute(&db, "begin").unwrap();
        session
            .execute(&db, "comment on table items is 'committed'")
            .unwrap();
        session.execute(&db, "commit").unwrap();
    }

    let reopened = Database::open(&base, 16).unwrap();
    assert_eq!(
        query_rows(
            &reopened,
            1,
            "select d.description \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'items' and d.classoid = 1259 and d.objsubid = 0"
        ),
        vec![vec![Value::Text("committed".into())]]
    );
}

#[test]
fn create_comment_and_drop_conversion_track_catalog_state() {
    let base = temp_dir("conversion_catalog_state");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create default conversion public.mydef for 'LATIN1' to 'UTF8' from iso8859_1_to_utf8",
    )
    .unwrap();
    db.execute(1, "comment on conversion mydef is 'hello conversion'")
        .unwrap();

    let conversions = db.conversions.read();
    let entry = conversions.get("public.mydef").expect("conversion exists");
    assert_eq!(entry.for_encoding, "LATIN1");
    assert_eq!(entry.to_encoding, "UTF8");
    assert!(entry.is_default);
    assert_eq!(entry.comment.as_deref(), Some("hello conversion"));
    drop(conversions);

    db.execute(1, "drop conversion mydef").unwrap();
    assert!(db.conversions.read().get("public.mydef").is_none());
}

#[test]
fn create_conversion_rejects_duplicate_name_and_default_pair() {
    let base = temp_dir("conversion_duplicate_checks");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create conversion myconv for 'LATIN1' to 'UTF8' from iso8859_1_to_utf8",
    )
    .unwrap();
    match db.execute(
        1,
        "create conversion myconv for 'LATIN1' to 'UTF8' from iso8859_1_to_utf8",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(message, "conversion \"myconv\" already exists");
            assert_eq!(sqlstate, "42710");
        }
        other => panic!("expected duplicate conversion error, got {:?}", other),
    }

    db.execute(
        1,
        "create default conversion public.mydef for 'LATIN1' to 'UTF8' from iso8859_1_to_utf8",
    )
    .unwrap();
    match db.execute(
        1,
        "create default conversion public.mydef2 for 'LATIN1' to 'UTF8' from iso8859_1_to_utf8",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(
                message,
                "default conversion for LATIN1 to UTF8 already exists"
            );
            assert_eq!(sqlstate, "42710");
        }
        other => panic!(
            "expected duplicate default conversion error, got {:?}",
            other
        ),
    }
}

#[test]
fn comment_on_temp_table_is_unsupported() {
    let base = temp_dir("comment_on_temp_table");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create temp table items (id int4 not null)")
        .unwrap();
    match db.execute(1, "comment on table items is 'nope'") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual }))
            if expected == "permanent table for COMMENT ON TABLE"
                && actual == "temporary table" => {}
        other => panic!("expected temp-table comment rejection, got {:?}", other),
    }
}

#[test]
fn comment_on_missing_table_uses_table_does_not_exist_error() {
    let base = temp_dir("comment_on_missing_table");
    let db = Database::open(&base, 16).unwrap();

    match db.execute(1, "comment on table attmp_wrong is 'table comment'") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message == "relation \"attmp_wrong\" does not exist" && sqlstate == "42P01" => {}
        other => panic!("expected missing-table comment error, got {:?}", other),
    }
}

#[test]
fn regtype_literal_cast_resolves_type_name() {
    let base = temp_dir("regtype_literal_cast");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp_array (id int4)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select typname from pg_type where oid = 'attmp_array[]'::regtype"
        ),
        vec![vec![Value::Text("_attmp_array".into())]]
    );
}

#[test]
fn regclass_literal_cast_resolves_relation_name() {
    let base = temp_dir("regclass_literal_cast");
    let db = Database::open(&base, 16).unwrap();

    assert_eq!(
        query_rows(&db, 1, "select 'pg_operator'::regclass::oid"),
        vec![vec![Value::Int64(
            crate::include::catalog::PG_OPERATOR_RELATION_OID as i64
        )]]
    );
}

#[test]
fn regclass_cast_resolves_text_expression() {
    let base = temp_dir("regclass_text_cast");
    let db = Database::open(&base, 16).unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname::regclass::oid from (values ('pg_operator'::text)) rel(relname)",
        ),
        vec![vec![Value::Int64(
            crate::include::catalog::PG_OPERATOR_RELATION_OID as i64
        )]]
    );
}

#[test]
fn regoperator_literal_cast_resolves_operator_signature() {
    let base = temp_dir("regoperator_literal_cast");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create function regoperator_test_fn(boolean, boolean) returns boolean as $$ select null::boolean; $$ language sql immutable",
    )
    .unwrap();
    db.execute(
        1,
        "create operator === (leftarg = boolean, rightarg = boolean, procedure = regoperator_test_fn)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select oprleft, oprright from pg_operator where oid = '===(boolean,boolean)'::regoperator"
        ),
        vec![vec![Value::Int64(16), Value::Int64(16)]]
    );
}

#[test]
fn pg_describe_object_formats_operator_dependencies() {
    let base = temp_dir("pg_describe_object_operator_deps");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create function alter_op_test_fn(boolean, boolean) returns boolean as $$ select null::boolean; $$ language sql immutable",
    )
    .unwrap();
    db.execute(
        1,
        "create function customcontsel(internal, oid, internal, integer) returns float8 as 'contsel' language internal stable strict",
    )
    .unwrap();
    db.execute(
        1,
        "create operator === (leftarg = boolean, rightarg = boolean, procedure = alter_op_test_fn, restrict = customcontsel, join = contjoinsel)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
                pg_describe_object('pg_proc'::regclass, 'alter_op_test_fn(boolean,boolean)'::regprocedure::oid, 0), \
                pg_describe_object('pg_proc'::regclass, 'customcontsel(internal,oid,internal,integer)'::regprocedure::oid, 0), \
                pg_describe_object('pg_namespace'::regclass, 2200, 0), \
                pg_describe_object('pg_operator'::regclass, '===(boolean,boolean)'::regoperator::oid, 0)"
        ),
        vec![vec![
            Value::Text("function alter_op_test_fn(boolean,boolean)".into()),
            Value::Text("function customcontsel(internal,oid,internal,integer)".into()),
            Value::Text("schema public".into()),
            Value::Text("operator ===(boolean,boolean)".into()),
        ]]
    );
}

#[test]
fn alter_index_rename_supports_if_exists_and_rename() {
    let base = temp_dir("alter_index_rename");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(1, "create index items_idx on items (id)")
        .unwrap();
    db.execute(
        1,
        "alter index if exists missing_idx rename to items_idx_new",
    )
    .unwrap();
    db.execute(1, "alter index items_idx rename to items_idx_new")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relname = 'items_idx_new'"
        ),
        vec![vec![Value::Text("items_idx_new".into())]]
    );
}

#[test]
fn alter_index_rename_if_exists_missing_pushes_notice() {
    let base = temp_dir("alter_index_rename_if_exists_missing");
    let db = Database::open(&base, 16).unwrap();

    clear_backend_notices();
    db.execute(
        1,
        "alter index if exists missing_idx rename to items_idx_new",
    )
    .unwrap();

    assert_eq!(
        take_backend_notice_messages(),
        vec![r#"relation "missing_idx" does not exist, skipping"#.to_string()]
    );
}

#[test]
fn alter_index_rename_if_exists_missing_in_transaction_pushes_notice() {
    let base = temp_dir("alter_index_rename_if_exists_missing_txn");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    clear_backend_notices();
    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "alter index if exists missing_idx rename to items_idx_new",
        )
        .unwrap();
    session.execute(&db, "commit").unwrap();

    assert_eq!(
        take_backend_notice_messages(),
        vec![r#"relation "missing_idx" does not exist, skipping"#.to_string()]
    );
}

#[test]
fn alter_index_alter_column_set_statistics_updates_expression_column_and_resets() {
    let base = temp_dir("alter_index_set_statistics_update");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp (a int4, d float8, e float8, b name)")
        .unwrap();
    db.execute(1, "create index attmp_idx on attmp (a, (d + e), b)")
        .unwrap();

    db.execute(
        1,
        "alter index attmp_idx alter column 2 set statistics 1000",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstattarget from pg_attribute \
             where attrelid = (select oid from pg_class where relname = 'attmp_idx') \
               and attname = 'expr2'",
        ),
        vec![vec![Value::Int16(1000)]]
    );

    db.execute(1, "alter index attmp_idx alter column 2 set statistics -1")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstattarget from pg_attribute \
             where attrelid = (select oid from pg_class where relname = 'attmp_idx') \
               and attname = 'expr2'",
        ),
        vec![vec![Value::Int16(-1)]]
    );
}

#[test]
fn alter_index_alter_column_set_statistics_rejects_non_expression_and_missing_columns() {
    let base = temp_dir("alter_index_set_statistics_errors");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp (a int4, d float8, e float8, b name)")
        .unwrap();
    db.execute(1, "create index attmp_idx on attmp (a, (d + e), b)")
        .unwrap();

    match db.execute(
        1,
        "alter index attmp_idx alter column 1 set statistics 1000",
    ) {
        Err(ExecError::DetailedError {
            message,
            hint: Some(hint),
            sqlstate,
            ..
        }) if message
            == "cannot alter statistics on non-expression column \"a\" of index \"attmp_idx\""
            && hint == "Alter statistics on table column instead."
            && sqlstate == "0A000" => {}
        other => panic!("expected non-expression index-column error, got {other:?}"),
    }

    match db.execute(
        1,
        "alter index attmp_idx alter column 3 set statistics 1000",
    ) {
        Err(ExecError::DetailedError {
            message,
            hint: Some(hint),
            sqlstate,
            ..
        }) if message
            == "cannot alter statistics on non-expression column \"b\" of index \"attmp_idx\""
            && hint == "Alter statistics on table column instead."
            && sqlstate == "0A000" => {}
        other => panic!("expected non-expression index-column error, got {other:?}"),
    }

    match db.execute(
        1,
        "alter index attmp_idx alter column 4 set statistics 1000",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message == "column number 4 of relation \"attmp_idx\" does not exist"
            && sqlstate == "42703" => {}
        other => panic!("expected missing index-column error, got {other:?}"),
    }
}

#[test]
fn alter_index_alter_column_set_statistics_if_exists_missing_pushes_notice() {
    let base = temp_dir("alter_index_set_statistics_if_exists_missing");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    clear_backend_notices();
    db.execute(
        1,
        "alter index if exists missing_idx alter column 2 set statistics 1000",
    )
    .unwrap();
    assert_eq!(
        take_backend_notice_messages(),
        vec![r#"relation "missing_idx" does not exist, skipping"#.to_string()]
    );

    clear_backend_notices();
    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "alter index if exists missing_idx alter column 2 set statistics 1000",
        )
        .unwrap();
    session.execute(&db, "commit").unwrap();
    assert_eq!(
        take_backend_notice_messages(),
        vec![r#"relation "missing_idx" does not exist, skipping"#.to_string()]
    );
}

#[test]
fn alter_index_and_table_set_statistics_clamp_and_emit_warning() {
    let base = temp_dir("alter_index_set_statistics_warning");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp (a int4, d float8, e float8, b name)")
        .unwrap();
    db.execute(1, "create index attmp_idx on attmp (a, (d + e), b)")
        .unwrap();
    db.execute(1, "create table items (i int4)").unwrap();

    clear_backend_notices();
    db.execute(
        1,
        "alter index attmp_idx alter column 2 set statistics 50000",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstattarget from pg_attribute \
             where attrelid = (select oid from pg_class where relname = 'attmp_idx') \
               and attname = 'expr2'",
        ),
        vec![vec![Value::Int16(10000)]]
    );
    let notices = take_backend_notices();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].severity, "WARNING");
    assert_eq!(notices[0].sqlstate, "01000");
    assert_eq!(notices[0].message, "lowering statistics target to 10000");

    clear_backend_notices();
    db.execute(1, "alter table items alter column i set statistics 50000")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstattarget from pg_attribute \
             where attrelid = (select oid from pg_class where relname = 'items') \
               and attname = 'i'",
        ),
        vec![vec![Value::Int16(10000)]]
    );
    let notices = take_backend_notices();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].severity, "WARNING");
    assert_eq!(notices[0].sqlstate, "01000");
    assert_eq!(notices[0].message, "lowering statistics target to 10000");
}

#[test]
fn alter_table_inherit_validates_shape_and_constraints() {
    let base = temp_dir("alter_table_inherit_validate");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parent_items (test2 int4)")
        .unwrap();
    db.execute(1, "create table child_items ()").unwrap();

    match db.execute(1, "alter table child_items inherit parent_items") {
        Err(ExecError::DetailedError { message, .. })
            if message == "child table is missing column \"test2\"" => {}
        other => panic!("expected missing-column inherit error, got {other:?}"),
    }

    db.execute(1, "drop table child_items").unwrap();
    db.execute(1, "create table child_items (test2 bool)")
        .unwrap();
    match db.execute(1, "alter table child_items inherit parent_items") {
        Err(ExecError::DetailedError { message, .. })
            if message == "child table \"child_items\" has different type for column \"test2\"" => {
        }
        other => panic!("expected type-mismatch inherit error, got {other:?}"),
    }

    db.execute(1, "drop table child_items").unwrap();
    db.execute(1, "create table child_items (test2 int4)")
        .unwrap();
    db.execute(
        1,
        "alter table parent_items add constraint parent_items_check check (test2 > 0)",
    )
    .unwrap();
    match db.execute(1, "alter table child_items inherit parent_items") {
        Err(ExecError::DetailedError { message, .. })
            if message == "child table is missing constraint \"parent_items_check\"" => {}
        other => panic!("expected missing-constraint inherit error, got {other:?}"),
    }
}

#[test]
fn alter_table_inherit_supports_attach_duplicate_and_cycle_errors() {
    let base = temp_dir("alter_table_inherit_attach");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table atacc1 (test int4)").unwrap();
    db.execute(1, "create table atacc2 (test2 int4)").unwrap();
    db.execute(
        1,
        "create table atacc3 (test3 int4, test2 int4) inherits (atacc1)",
    )
    .unwrap();
    db.execute(1, "alter table atacc2 add constraint foo check (test2 > 0)")
        .unwrap();
    db.execute(1, "insert into atacc3 (test2) values (4)")
        .unwrap();
    db.execute(1, "update atacc3 set test2 = 4 where test2 is null")
        .unwrap();
    db.execute(1, "alter table atacc3 add constraint foo check (test2 > 0)")
        .unwrap();
    db.execute(1, "alter table atacc3 inherit atacc2").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select test2 from atacc2 order by test2"),
        vec![vec![Value::Int32(4)]]
    );

    match db.execute(1, "alter table atacc3 inherit atacc2") {
        Err(ExecError::DetailedError { message, .. })
            if message == "relation \"atacc2\" would be inherited from more than once" => {}
        other => panic!("expected duplicate-parent inherit error, got {other:?}"),
    }

    match db.execute(1, "alter table atacc2 inherit atacc3") {
        Err(ExecError::DetailedError {
            message, detail, ..
        }) if message == "circular inheritance not allowed"
            && detail == Some("\"atacc3\" is already a child of \"atacc2\".".into()) => {}
        other => panic!("expected circular inherit error, got {other:?}"),
    }

    match db.execute(1, "alter table atacc2 inherit atacc2") {
        Err(ExecError::DetailedError {
            message, detail, ..
        }) if message == "circular inheritance not allowed"
            && detail == Some("\"atacc2\" is already a child of \"atacc2\".".into()) => {}
        other => panic!("expected self-inherit error, got {other:?}"),
    }
}

#[test]
fn explain_inherited_append_uses_relation_names_and_sql_casts() {
    let base = temp_dir("explain_inherited_append_format");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table nv_parent (d date)").unwrap();
    db.execute(1, "create table nv_child_2009 () inherits (nv_parent)")
        .unwrap();
    db.execute(1, "create table nv_child_2010 () inherits (nv_parent)")
        .unwrap();
    db.execute(1, "create table nv_child_2011 () inherits (nv_parent)")
        .unwrap();

    let rows = query_rows(
        &db,
        1,
        "explain select * from nv_parent where d >= '2009-08-01'::date and d <= '2009-08-31'::date",
    );
    let rendered = rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(text) => text.clone(),
            other => panic!("expected explain text row, got {other:?}"),
        })
        .collect::<Vec<_>>();

    assert!(rendered.iter().any(|line| line.contains("Append")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("Seq Scan on nv_parent"))
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("Seq Scan on nv_child_2009"))
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("'2009-08-01'::date"))
    );
    assert!(
        rendered.iter().all(|line| !line.contains("Projection")),
        "expected inherited append explain to elide passthrough projections, got {rendered:?}"
    );
    assert!(
        rendered.iter().all(|line| !line.contains("Cast(Const(")),
        "expected sql-style cast rendering, got {rendered:?}"
    );
    assert!(
        rendered.iter().all(|line| !line.contains("rel ")),
        "expected relation names instead of relcache numbers, got {rendered:?}"
    );
}

#[test]
fn alter_table_add_column_reads_old_rows_with_null_or_default() {
    let base = temp_dir("alter_table_add_column_reads_old_rows");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "insert into items values (1), (2)").unwrap();
    db.execute(1, "alter table items add column note text")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![
            vec![Value::Int32(1), Value::Null],
            vec![Value::Int32(2), Value::Null],
        ]
    );

    db.execute(1, "alter table items add column bucket int4 default 3")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select id, note, bucket from items order by id"),
        vec![
            vec![Value::Int32(1), Value::Null, Value::Int32(3)],
            vec![Value::Int32(2), Value::Null, Value::Int32(3)],
        ]
    );
}

#[test]
fn alter_table_if_exists_ignores_missing_table() {
    let base = temp_dir("alter_table_if_exists_missing");
    let db = Database::open(&base, 16).unwrap();

    assert_eq!(
        db.execute(1, "alter table if exists missing add column note text")
            .unwrap(),
        StatementResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute(1, "alter table if exists missing rename to renamed_missing")
            .unwrap(),
        StatementResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute(
            1,
            "alter table if exists missing alter column note set default 'hello'",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute(
            1,
            "alter table if exists missing alter column note set (n_distinct = 1)",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute(
            1,
            "alter table if exists missing alter column note set statistics 150",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
}

#[test]
fn alter_table_only_is_accepted_for_supported_operations() {
    let base = temp_dir("alter_table_only_supported");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "alter table only items add column body text")
        .unwrap();
    db.execute(1, "alter table only items rename column note to summary")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, summary, body from items"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn alter_table_alter_column_set_default_applies_to_future_rows() {
    let base = temp_dir("alter_table_set_default");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "alter table items alter column note set default 'hello'")
        .unwrap();
    db.execute(1, "insert into items (id) values (1), (2)")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("hello".into())],
            vec![Value::Int32(2), Value::Text("hello".into())],
        ]
    );
}

#[test]
fn alter_table_alter_column_drop_default_removes_future_default() {
    let base = temp_dir("alter_table_drop_default");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "alter table items alter column note set default 'hello'")
        .unwrap();
    db.execute(1, "alter table items alter column note drop default")
        .unwrap();
    db.execute(1, "insert into items (id) values (1)").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items"),
        vec![vec![Value::Int32(1), Value::Null]]
    );
}

#[test]
fn alter_table_alter_column_set_default_rejects_mismatched_type() {
    let base = temp_dir("alter_table_set_default_type_error");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();

    match db.execute(1, "alter table items alter column id set default 'oops'") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message
            == "column \"id\" is of type integer but default expression is of type text"
            && sqlstate == "42804" => {}
        other => panic!("expected default type mismatch error, got {other:?}"),
    }
}

#[test]
fn alter_table_alter_column_set_options_is_accepted() {
    let base = temp_dir("alter_table_column_options");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp(i int4)").unwrap();
    assert_eq!(
        db.execute(
            1,
            "alter table attmp alter column i set (n_distinct = 1, n_distinct_inherited = 2)",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute(
            1,
            "alter table attmp alter column i reset (n_distinct_inherited)",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
}

#[test]
fn alter_table_alter_column_set_storage_updates_pg_attribute_and_write_behavior() {
    let base = temp_dir("alter_table_column_storage");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp(t text)").unwrap();
    db.execute(1, "alter table attmp alter column t set storage plain")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstorage from pg_attribute where attrelid = (select oid from pg_class where relname = 'attmp') and attname = 't'",
        ),
        vec![vec![Value::Text("p".into())]]
    );

    let oversized = "x".repeat(crate::backend::storage::page::bufpage::MAX_HEAP_TUPLE_SIZE);
    match db.execute(1, &format!("insert into attmp values ('{oversized}')")) {
        Err(ExecError::Heap(crate::backend::access::heap::heapam::HeapError::Tuple(
            crate::include::access::htup::TupleError::Oversized { .. },
        ))) => {}
        other => panic!("expected oversized tuple error after SET STORAGE PLAIN, got {other:?}"),
    }
}

#[test]
fn alter_table_alter_column_set_statistics_updates_pg_attribute() {
    let base = temp_dir("alter_table_column_statistics");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp(i int4)").unwrap();
    db.execute(1, "alter table attmp alter column i set statistics 150")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstattarget from pg_attribute where attrelid = (select oid from pg_class where relname = 'attmp') and attname = 'i'",
        ),
        vec![vec![Value::Int16(150)]]
    );

    db.execute(1, "alter table attmp alter column i set statistics -1")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstattarget from pg_attribute where attrelid = (select oid from pg_class where relname = 'attmp') and attname = 'i'",
        ),
        vec![vec![Value::Int16(-1)]]
    );
}

#[test]
fn alter_table_alter_column_set_statistics_rejects_values_below_minus_one() {
    let base = temp_dir("alter_table_column_statistics_low");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp(i int4)").unwrap();

    match db.execute(1, "alter table attmp alter column i set statistics -2") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message == "statistics target -2 is too low" && sqlstate == "22023" => {}
        other => panic!("expected statistics target error, got {other:?}"),
    }
}

#[test]
fn alter_statistics_updates_in_memory_statistics_target() {
    let base = temp_dir("alter_statistics_target");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table ab1(a int4, b int4)").unwrap();
    db.execute(1, "create statistics ab1_a_b_stats on a, b from ab1")
        .unwrap();
    db.execute(1, "alter statistics ab1_a_b_stats set statistics 0")
        .unwrap();

    let catcache = db.backend_catcache(1, None).unwrap();
    let entry = catcache
        .statistic_ext_rows()
        .into_iter()
        .find(|entry| entry.stxname == "ab1_a_b_stats")
        .unwrap();
    assert_eq!(entry.stxstattarget, Some(0));

    db.execute(1, "alter statistics ab1_a_b_stats set statistics -1")
        .unwrap();
    let catcache = db.backend_catcache(1, None).unwrap();
    let entry = catcache
        .statistic_ext_rows()
        .into_iter()
        .find(|entry| entry.stxname == "ab1_a_b_stats")
        .unwrap();
    assert_eq!(entry.stxstattarget, None);
}

#[test]
fn alter_statistics_if_exists_missing_pushes_notice() {
    let base = temp_dir("alter_statistics_if_exists");
    let db = Database::open(&base, 16).unwrap();

    clear_backend_notices();
    db.execute(
        1,
        "alter statistics if exists missing_stats set statistics 0",
    )
    .unwrap();
    assert_eq!(
        take_backend_notice_messages(),
        vec![r#"statistics object "missing_stats" does not exist, skipping"#.to_string()]
    );
}

#[test]
fn alter_table_add_column_serial_backfills_existing_rows_and_keeps_sequence_advancing() {
    let base = temp_dir("alter_table_add_column_serial");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (note text)").unwrap();
    db.execute(1, "insert into items values ('a'), ('b')")
        .unwrap();
    db.execute(1, "alter table items add column id serial")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("a".into())],
            vec![Value::Int32(2), Value::Text("b".into())],
        ]
    );
    assert_eq!(
        query_rows(&db, 1, "select pg_get_serial_sequence('items', 'id')"),
        vec![vec![Value::Text("items_id_seq".into())]]
    );

    db.execute(1, "insert into items values ('manual', 10)")
        .unwrap();
    db.execute(1, "insert into items (note) values ('c')")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("a".into())],
            vec![Value::Int32(2), Value::Text("b".into())],
            vec![Value::Int32(3), Value::Text("c".into())],
            vec![Value::Int32(10), Value::Text("manual".into())],
        ]
    );
}

#[test]
fn alter_table_add_column_uses_command_end_invalidation_and_rolls_back() {
    let base = temp_dir("alter_table_add_column_txn");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null)")
        .unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "alter table items add column note text default 'x'")
        .unwrap();
    assert_eq!(
        session.execute(&db, "select note from items").unwrap(),
        StatementResult::Query {
            columns: vec![QueryColumn {
                name: "note".into(),
                sql_type: SqlType::new(SqlTypeKind::Text),
                wire_type_oid: None
            }],
            column_names: vec!["note".into()],
            rows: vec![],
        }
    );
    session.execute(&db, "rollback").unwrap();

    match db.execute(1, "select note from items") {
        Err(ExecError::Parse(ParseError::UnknownColumn(name)))
        | Err(ExecError::Parse(ParseError::UnexpectedToken { actual: name, .. }))
            if name.contains("note") => {}
        other => panic!("expected rolled-back column to be absent, got {other:?}"),
    }
}

#[test]
fn alter_table_add_column_rejects_unsupported_forms() {
    let base = temp_dir("alter_table_add_column_rejects_unsupported");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();

    match db.execute(1, "alter table items add column xmin int4") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message == "column name \"xmin\" conflicts with a system column name"
            && sqlstate == "42701" => {}
        other => panic!("expected system-column rejection, got {other:?}"),
    }

    db.execute(1, "alter table items add column note text not null")
        .unwrap();

    match db.execute(1, "alter table items add column key_id int4 primary key") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual }))
            if expected == "ADD COLUMN without PRIMARY KEY" && actual == "PRIMARY KEY" => {}
        other => panic!("expected PRIMARY KEY rejection, got {other:?}"),
    }

    match db.execute(1, "alter table items add column code text unique") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual }))
            if expected == "ADD COLUMN without UNIQUE" && actual == "UNIQUE" => {}
        other => panic!("expected UNIQUE rejection, got {other:?}"),
    }
}

#[test]
fn alter_table_add_column_supports_tid_xid_and_interval() {
    let base = temp_dir("alter_table_add_column_tid_xid_interval");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table attmp (initial int4)")
        .unwrap();
    session
        .execute(&db, "alter table attmp add column l tid")
        .unwrap();
    session
        .execute(&db, "alter table attmp add column m xid")
        .unwrap();
    session
        .execute(&db, "alter table attmp add column w interval")
        .unwrap();
    session
        .execute(
            &db,
            "insert into attmp (l, m, w) values ('(1,1)', '512', '01:00:10')",
        )
        .unwrap();

    match session.execute(&db, "select l, m, w from attmp").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("(1,1)".into()),
                    Value::Int64(512),
                    Value::Interval(IntervalValue {
                        time_micros: 3_610_000_000,
                        days: 0,
                        months: 0,
                    })
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn alter_table_add_column_propagates_to_temp_inherited_child() {
    let base = temp_dir("alter_table_add_column_temp_inherits");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table parent1 (f1 int4)")
        .unwrap();
    session
        .execute(&db, "create temp table child1 () inherits (parent1)")
        .unwrap();
    session
        .execute(&db, "insert into child1 values (1)")
        .unwrap();
    session
        .execute(&db, "alter table parent1 add column a1 int4 default 3")
        .unwrap();

    match session
        .execute(&db, "select f1, a1 from child1")
        .expect("select propagated temp child column")
    {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(columns[1].sql_type, SqlType::new(SqlTypeKind::Int4));
            assert_eq!(rows, vec![vec![Value::Int32(1), Value::Int32(3)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn alter_table_add_column_merges_temp_multi_parent_child_metadata() {
    let base = temp_dir("alter_table_add_column_temp_multi_parent");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table pp1 (f1 int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create temp table cc1 (f2 text, f3 int4) inherits (pp1)",
        )
        .unwrap();
    session
        .execute(&db, "create temp table cc2 (f4 float8) inherits (pp1, cc1)")
        .unwrap();

    clear_backend_notices();
    session
        .execute(&db, "alter table pp1 add column a2 int4")
        .unwrap();
    assert_eq!(
        take_backend_notice_messages(),
        vec![r#"merging definition of column "a2" for child "cc2""#.to_string()]
    );

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select c.relname, a.attname, a.attinhcount, a.attislocal
             from pg_attribute a
             join pg_class c on c.oid = a.attrelid
             where attname = 'a2'
             order by 1",
        ),
        vec![
            vec![
                Value::Text("cc1".into()),
                Value::Text("a2".into()),
                Value::Int16(1),
                Value::Bool(false),
            ],
            vec![
                Value::Text("cc2".into()),
                Value::Text("a2".into()),
                Value::Int16(2),
                Value::Bool(false),
            ],
            vec![
                Value::Text("pp1".into()),
                Value::Text("a2".into()),
                Value::Int16(0),
                Value::Bool(true),
            ],
        ]
    );
}

#[test]
fn alter_table_add_column_propagates_temp_not_null_constraints() {
    let base = temp_dir("alter_table_add_column_temp_not_null");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table parent1 (f1 int4)")
        .unwrap();
    session
        .execute(&db, "create temp table child1 () inherits (parent1)")
        .unwrap();
    session
        .execute(&db, "insert into child1 values (1)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table parent1 add column a1 int4 not null default 3",
        )
        .unwrap();

    assert_eq!(
        session_query_rows(&mut session, &db, "select f1, a1 from child1 order by f1",),
        vec![vec![Value::Int32(1), Value::Int32(3)]]
    );
    match session.execute(&db, "insert into child1 (f1, a1) values (2, null)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "child1" && column == "a1" && constraint == "parent1_a1_not_null" => {}
        other => panic!("expected propagated temp not-null violation, got {other:?}"),
    }
}

#[test]
fn alter_table_add_column_propagates_temp_check_constraints() {
    let base = temp_dir("alter_table_add_column_temp_check");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table parent1 (f1 int4)")
        .unwrap();
    session
        .execute(&db, "create temp table child1 () inherits (parent1)")
        .unwrap();
    session
        .execute(&db, "insert into child1 values (1)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table parent1 add column a1 int4 check (a1 > 0) default 3",
        )
        .unwrap();

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select c.relname, pgc.conname
             from pg_constraint pgc
             join pg_class c on c.oid = pgc.conrelid
             where pgc.conname = 'parent1_a1_check'
             order by 1",
        ),
        vec![
            vec![
                Value::Text("child1".into()),
                Value::Text("parent1_a1_check".into()),
            ],
            vec![
                Value::Text("parent1".into()),
                Value::Text("parent1_a1_check".into()),
            ],
        ]
    );
    match session.execute(&db, "insert into child1 (f1, a1) values (2, -1)") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "child1" && constraint == "parent1_a1_check" => {}
        other => panic!("expected propagated temp check violation, got {other:?}"),
    }
}

#[test]
fn alter_table_add_column_temp_not_null_validates_inherited_child_rows() {
    let base = temp_dir("alter_table_add_column_temp_not_null_validate");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table parent1 (f1 int4)")
        .unwrap();
    session
        .execute(&db, "create temp table child1 () inherits (parent1)")
        .unwrap();
    session
        .execute(&db, "insert into child1 values (1)")
        .unwrap();

    match session.execute(&db, "alter table parent1 add column a1 int4 not null") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "child1" && column == "a1" && constraint == "parent1_a1_not_null" => {}
        other => panic!("expected inherited child validation failure, got {other:?}"),
    }
}

#[test]
fn alter_table_drop_column_hides_column_and_retargets_inserts() {
    let base = temp_dir("alter_table_drop_column");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table items (a int4 not null, b int4, c int4 not null, d int4)",
    )
    .unwrap();
    db.execute(1, "insert into items values (1, 2, 3, 4)")
        .unwrap();
    db.execute(1, "alter table items drop a").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select * from items order by b"),
        vec![vec![Value::Int32(2), Value::Int32(3), Value::Int32(4)]]
    );

    match db.execute(1, "select a from items") {
        Err(ExecError::Parse(ParseError::UnknownColumn(name))) if name == "a" => {}
        other => panic!("expected dropped column lookup to fail, got {other:?}"),
    }

    match db.execute(1, "insert into items values (10, 11, 12, 13)") {
        Err(ExecError::Parse(ParseError::InvalidInsertTargetCount { expected, actual }))
            if expected == 3 && actual == 4 => {}
        other => panic!("expected visible-column insert width check, got {other:?}"),
    }

    db.execute(1, "insert into items values (11, 12, 13)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select * from items order by b"),
        vec![
            vec![Value::Int32(2), Value::Int32(3), Value::Int32(4)],
            vec![Value::Int32(11), Value::Int32(12), Value::Int32(13)],
        ]
    );
}

#[test]
fn alter_table_drop_column_persists_hidden_metadata() {
    let base = temp_dir("alter_table_drop_column_reopen");
    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(16)).unwrap();

    db.execute(1, "create table items (a int4 not null, b int4)")
        .unwrap();
    db.execute(1, "insert into items values (1, 2)").unwrap();
    db.execute(1, "alter table items drop a").unwrap();
    drop(db);

    let reopened = Database::open(&base, 16).unwrap();
    let relcache = reopened.catalog.read().relcache().unwrap();
    let entry = relcache.get_by_name("items").unwrap();
    assert!(entry.desc.columns[0].dropped);
    assert_eq!(entry.desc.columns[0].name, "........pg.dropped.1........");
    assert_eq!(
        query_rows(&reopened, 1, "select * from items"),
        vec![vec![Value::Int32(2)]]
    );
}

#[test]
fn alter_table_rename_updates_name_and_rolls_back() {
    let base = temp_dir("alter_table_rename_txn");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null)")
        .unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "alter table items rename to renamed_items")
        .unwrap();

    match session
        .execute(&db, "select count(*) from renamed_items")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
    match session.execute(&db, "select count(*) from items") {
        Err(ExecError::Parse(ParseError::UnknownTable(name))) if name == "items" => {}
        other => panic!("expected renamed table to hide old name, got {other:?}"),
    }

    session.execute(&db, "rollback").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );
    match db.execute(1, "select count(*) from renamed_items") {
        Err(ExecError::Parse(ParseError::UnknownTable(name))) if name == "renamed_items" => {}
        other => panic!("expected rollback to restore old name, got {other:?}"),
    }
}

#[test]
fn alter_table_rename_unmasks_permanent_table_after_temp_rename() {
    let base = temp_dir("alter_table_rename_temp_shadow");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (regtable int4)")
        .unwrap();
    session
        .execute(&db, "create temp table items (attmptable int4)")
        .unwrap();

    session
        .execute(&db, "alter table items rename to items_temp")
        .unwrap();
    match session.execute(&db, "select count(*) from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected permanent table after temp rename, got {other:?}"),
    }
    match session
        .execute(&db, "select count(*) from items_temp")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected renamed temp table lookup, got {other:?}"),
    }

    session
        .execute(&db, "alter table items rename to items_perm")
        .unwrap();
    match session.execute(&db, "select count(*) from items") {
        Err(ExecError::Parse(ParseError::UnknownTable(name))) if name == "items" => {}
        other => panic!("expected old permanent name to disappear, got {other:?}"),
    }
    match session
        .execute(&db, "select count(*) from items_perm")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected renamed permanent table lookup, got {other:?}"),
    }
}

#[test]
fn alter_table_rename_temp_table_rolls_back() {
    let base = temp_dir("alter_table_rename_temp_txn");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "alter table items rename to renamed_items")
        .unwrap();
    match session
        .execute(&db, "select count(*) from renamed_items")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected renamed temp table lookup, got {other:?}"),
    }
    session.execute(&db, "rollback").unwrap();

    match session.execute(&db, "select count(*) from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected temp rename rollback to restore old name, got {other:?}"),
    }
    match session.execute(&db, "select count(*) from renamed_items") {
        Err(ExecError::Parse(ParseError::UnknownTable(name))) if name == "renamed_items" => {}
        other => panic!("expected rolled-back temp rename to hide new name, got {other:?}"),
    }
}

#[test]
fn alter_table_rename_moves_conflicting_array_type_names() {
    let base = temp_dir("alter_table_rename_array_type_conflict");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp_array (id int4)").unwrap();
    db.execute(1, "create table attmp_array2 (id int4)")
        .unwrap();
    db.execute(1, "alter table attmp_array2 rename to _attmp_array")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select typname from pg_type where oid = 'attmp_array[]'::regtype",
        ),
        vec![vec![Value::Text("__attmp_array".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select typname from pg_type where oid = '_attmp_array[]'::regtype",
        ),
        vec![vec![Value::Text("__attmp_array_1".into())]]
    );

    db.execute(1, "drop table _attmp_array").unwrap();
    db.execute(1, "drop table attmp_array").unwrap();
}

#[test]
fn alter_table_rename_to_own_array_type_name_moves_self_array_type() {
    let base = temp_dir("alter_table_rename_self_array_type");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table attmp_array (id int4)").unwrap();
    db.execute(1, "alter table attmp_array rename to _attmp_array")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select typname from pg_type where oid = '_attmp_array[]'::regtype",
        ),
        vec![vec![Value::Text("__attmp_array".into())]]
    );
}

#[test]
fn alter_table_rename_rejects_non_array_type_name_conflicts() {
    let base = temp_dir("alter_table_rename_type_conflict");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create type _items as enum ('one')").unwrap();
    db.execute(1, "create table items (id int4)").unwrap();

    match db.execute(1, "alter table items rename to _items") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(message, "type \"_items\" already exists");
            assert_eq!(sqlstate, "42710");
        }
        other => panic!("expected duplicate type error, got {other:?}"),
    }
}

#[test]
fn alter_table_rename_column_updates_lookup_and_rolls_back() {
    let base = temp_dir("alter_table_rename_column_txn");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null, note text)")
        .unwrap();
    session
        .execute(&db, "insert into items values (1, 'hello')")
        .unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "alter table items rename column note to body")
        .unwrap();

    match session.execute(&db, "select body from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("hello".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
    match session.execute(&db, "select note from items") {
        Err(ExecError::Parse(ParseError::UnknownColumn(name))) if name == "note" => {}
        other => panic!("expected old column name to disappear, got {other:?}"),
    }

    session.execute(&db, "rollback").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select note from items"),
        vec![vec![Value::Text("hello".into())]]
    );
    match db.execute(1, "select body from items") {
        Err(ExecError::Parse(ParseError::UnknownColumn(name))) if name == "body" => {}
        other => panic!("expected rollback to restore old column name, got {other:?}"),
    }
}

#[test]
fn alter_table_rename_column_persists_after_reopen() {
    let base = temp_dir("alter_table_rename_column_reopen");
    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(16)).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'hello')")
        .unwrap();
    db.execute(1, "alter table items rename column note to body")
        .unwrap();
    drop(db);

    let reopened = Database::open(&base, 16).unwrap();
    assert_eq!(
        query_rows(&reopened, 1, "select body from items"),
        vec![vec![Value::Text("hello".into())]]
    );
    match reopened.execute(1, "select note from items") {
        Err(ExecError::Parse(ParseError::UnknownColumn(name))) if name == "note" => {}
        other => panic!("expected persisted renamed column to hide old name, got {other:?}"),
    }
}

#[test]
fn alter_table_alter_column_type_rewrites_rows_with_using_expr() {
    let base = temp_dir("alter_table_alter_column_type_using");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, '7'), (2, '42')")
        .unwrap();
    db.execute(
        1,
        "alter table items alter column note type int4 using note::int4",
    )
    .unwrap();

    match db.execute(1, "select note from items order by id").unwrap() {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(columns[0].sql_type, SqlType::new(SqlTypeKind::Int4));
            assert_eq!(rows, vec![vec![Value::Int32(7)], vec![Value::Int32(42)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn alter_table_alter_column_type_rejects_nonautomatic_cast_without_using() {
    let base = temp_dir("alter_table_alter_column_type_needs_using");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (note text)").unwrap();

    match db.execute(1, "alter table items alter column note type int4") {
        Err(ExecError::DetailedError {
            message,
            hint,
            sqlstate,
            ..
        }) if message == "column \"note\" cannot be cast automatically to type integer"
            && hint.as_deref() == Some("You might need to specify \"USING note::integer\".")
            && sqlstate == "42804" => {}
        other => panic!("expected automatic-cast rejection, got {other:?}"),
    }
}

#[test]
fn alter_table_alter_column_type_allows_textlike_cast_without_using() {
    let base = temp_dir("alter_table_alter_column_type_textlike");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (note text)").unwrap();
    db.execute(1, "insert into items values ('hello')").unwrap();
    db.execute(1, "alter table items alter column note type varchar(10)")
        .unwrap();

    match db.execute(1, "select note from items").unwrap() {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(
                columns[0].sql_type,
                SqlType::with_char_len(SqlTypeKind::Varchar, 10)
            );
            assert_eq!(rows, vec![vec![Value::Text("hello".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn alter_table_alter_column_type_allows_foreign_key_columns() {
    let base = temp_dir("alter_table_alter_column_type_foreign_key");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table pktable (ptest1 int, ptest2 int, ptest3 text, primary key (ptest1, ptest2))",
    )
    .unwrap();
    db.execute(
        1,
        "create table fktable (
            ftest1 int,
            ftest2 int,
            ftest3 int,
            constraint constrname foreign key (ftest1, ftest2)
                references pktable match full on delete set null on update set null
        )",
    )
    .unwrap();

    db.execute(1, "insert into pktable values (1, 2, 'Test1')")
        .unwrap();
    db.execute(1, "insert into pktable values (2, 4, 'Test2')")
        .unwrap();
    db.execute(1, "insert into fktable values (1, 2, 4)")
        .unwrap();
    db.execute(1, "insert into fktable values (2, 4, 8)")
        .unwrap();

    db.execute(1, "alter table pktable alter column ptest1 type bigint")
        .unwrap();
    db.execute(1, "alter table fktable alter column ftest1 type bigint")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select ptest1, ptest2, ptest3 from pktable order by ptest2",
        ),
        vec![
            vec![
                Value::Int64(1),
                Value::Int32(2),
                Value::Text("Test1".into()),
            ],
            vec![
                Value::Int64(2),
                Value::Int32(4),
                Value::Text("Test2".into()),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select ftest1, ftest2, ftest3 from fktable order by ftest2",
        ),
        vec![
            vec![Value::Int64(1), Value::Int32(2), Value::Int32(4)],
            vec![Value::Int64(2), Value::Int32(4), Value::Int32(8)],
        ]
    );
}

#[test]
fn alter_table_alter_column_type_rejects_indexed_target_column() {
    let base = temp_dir("alter_table_alter_column_type_index_guard");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note int4)")
        .unwrap();
    db.execute(1, "create index items_note_idx on items (note)")
        .unwrap();

    match db.execute(1, "alter table items alter column note type int8") {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "ALTER TABLE ALTER COLUMN TYPE with dependent indexes" => {}
        other => panic!("expected dependent-index rejection, got {other:?}"),
    }
}

#[test]
fn create_index_builds_ready_valid_btree_and_explain_uses_it() {
    let base = temp_dir("btree_index_scan_explain");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    match db
        .execute(1, "explain select name from items where id = 2")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let lines = rows
                .into_iter()
                .filter_map(|row| match row.first() {
                    Some(Value::Text(text)) => Some(text.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert!(
                lines.iter().any(|line| line.contains("Index Scan")),
                "expected Index Scan in EXPLAIN, got {lines:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_gist_box_index_explain_and_query_use_it() {
    let base = temp_dir("gist_box_index_scan_explain");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table boxes (id int4 not null, b box)")
        .unwrap();
    db.execute(
        1,
        "insert into boxes values \
         (1, '(0,0),(1,1)'::box), \
         (2, '(5,5),(8,8)'::box), \
         (3, '(10,10),(12,12)'::box)",
    )
    .unwrap();
    db.execute(1, "create index boxes_b_gist on boxes using gist (b)")
        .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select id from boxes where b && '(6,6),(7,7)'::box",
        "boxes_b_gist",
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from boxes where b && '(6,6),(7,7)'::box order by id",
        ),
        vec![vec![Value::Int32(2)]]
    );

    db.execute(1, "insert into boxes values (4, '(6,6),(9,9)'::box)")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from boxes where b && '(6,6),(7,7)'::box order by id",
        ),
        vec![vec![Value::Int32(2)], vec![Value::Int32(4)]]
    );
}

#[test]
fn create_gist_point_index_explain_and_query_use_it() {
    let base = temp_dir("gist_point_index_scan_explain");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table points (id int4 not null, p point)")
        .unwrap();
    db.execute(
        1,
        "insert into points values \
         (1, '(1,1)'::point), \
         (2, '(5,5)'::point), \
         (3, '(9,9)'::point)",
    )
    .unwrap();
    db.execute(1, "create index points_p_gist on points using gist (p)")
        .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select id from points where p <@ '(4,4),(6,6)'::box",
        "points_p_gist",
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from points where p <@ '(4,4),(6,6)'::box order by id",
        ),
        vec![vec![Value::Int32(2)]]
    );

    db.execute(1, "insert into points values (4, '(5.5,5.5)'::point)")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from points where p <@ '(4,4),(6,6)'::box order by id",
        ),
        vec![vec![Value::Int32(2)], vec![Value::Int32(4)]]
    );

    db.execute(1, "create table fuzzy_points (p point)")
        .unwrap();
    db.execute(
        1,
        "insert into fuzzy_points select '(0,0)'::point from generate_series(0,1000)",
    )
    .unwrap();
    db.execute(
        1,
        "create index fuzzy_points_p_gist on fuzzy_points using gist (p)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into fuzzy_points values ('(0.0000009,0.0000009)'::point)",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from fuzzy_points where p ~= '(0.0000009,0.0000009)'::point",
        ),
        vec![vec![Value::Int64(1002)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from fuzzy_points where p ~= '(0.0000018,0.0000018)'::point",
        ),
        vec![vec![Value::Int64(1)]]
    );
}

#[test]
fn create_brin_index_explain_uses_bitmap_scan_and_recheck() {
    let base = temp_dir("brin_bitmap_scan_explain");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (a int4 not null, note text)")
        .unwrap();
    db.execute(
        1,
        "insert into items select i, repeat('x', 200) from generate_series(1, 2000) i",
    )
    .unwrap();
    db.execute(
        1,
        "create index items_a_brin on items using brin (a) with (pages_per_range = 1)",
    )
    .unwrap();
    db.execute(1, "analyze items").unwrap();

    let relfilenode = relfilenode_for(&db, 1, "items_a_brin");
    let lines = explain_lines(&db, 1, "select a from items where a >= 200 and a < 210");
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Bitmap Heap Scan on items")),
        "expected Bitmap Heap Scan in EXPLAIN, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains(&format!("Bitmap Index Scan using rel {relfilenode} "))),
        "expected Bitmap Index Scan on items_a_brin, got {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("Index Cond:")
            && line.contains("a >= 200")
            && line.contains("a < 210")),
        "expected BRIN Index Cond in EXPLAIN, got {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("Recheck Cond:")
            && line.contains("a >= 200")
            && line.contains("a < 210")),
        "expected BRIN Recheck Cond in EXPLAIN, got {lines:?}"
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select a from items where a >= 200 and a < 210 order by a"
        ),
        (200..210)
            .map(|value| vec![Value::Int32(value)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn create_gin_jsonb_index_uses_bitmap_scan_and_rechecks() {
    let base = temp_dir("gin_jsonb_bitmap_scan");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table docs (id int4 not null, j jsonb)")
        .unwrap();
    db.execute(
        1,
        "insert into docs \
         select i, \
                case \
                  when i % 10 = 0 then '{\"a\":1,\"b\":true}'::jsonb \
                  when i % 10 = 1 then '{\"a\":1}'::jsonb \
                  else '{\"c\":2}'::jsonb \
                end \
         from generate_series(1, 2000) i",
    )
    .unwrap();
    db.execute(
        1,
        "create index docs_j_gin on docs using gin (j) \
         with (fastupdate = on, gin_pending_list_limit = 64)",
    )
    .unwrap();
    db.execute(1, "analyze docs").unwrap();

    let relfilenode = relfilenode_for(&db, 1, "docs_j_gin");
    let lines = explain_lines(&db, 1, "select id from docs where j @> '{\"a\":1}'::jsonb");
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Bitmap Heap Scan on docs")),
        "expected Bitmap Heap Scan in EXPLAIN, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains(&format!("Bitmap Index Scan using rel {relfilenode} "))),
        "expected Bitmap Index Scan on docs_j_gin, got {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.contains("Recheck Cond:")),
        "expected GIN Recheck Cond in EXPLAIN, got {lines:?}"
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from docs where j @> '{\"a\":1}'::jsonb",
        ),
        vec![vec![Value::Int64(400)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from docs where j ? 'a'"),
        vec![vec![Value::Int64(400)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from docs where j ?| array['b','missing']::text[]",
        ),
        vec![vec![Value::Int64(200)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from docs where j ?& array['a','b']::text[]",
        ),
        vec![vec![Value::Int64(200)]]
    );

    db.execute(
        1,
        "insert into docs values (2001, '{\"a\":1,\"b\":true}'::jsonb)",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from docs where j ?& array['a','b']::text[]",
        ),
        vec![vec![Value::Int64(201)]]
    );
}

#[test]
fn reopen_brin_index_preserves_pages_per_range_in_catalog() {
    let base = temp_dir("brin_reopen_catalog_options");

    {
        let db = Database::open(&base, 16).unwrap();
        db.execute(1, "create table items (a int4 not null)")
            .unwrap();
        db.execute(
            1,
            "create index items_a_brin on items using brin (a) with (pages_per_range = 32)",
        )
        .unwrap();

        let catalog = db.catalog.read().catalog_snapshot().unwrap();
        let index = catalog.get("items_a_brin").unwrap();
        assert_eq!(
            index
                .index_meta
                .as_ref()
                .and_then(|meta| meta.brin_options.as_ref())
                .map(|options| options.pages_per_range),
            Some(32)
        );
    }

    let reopened = Database::open(&base, 16).unwrap();
    let catalog = reopened.catalog.read().catalog_snapshot().unwrap();
    let index = catalog.get("items_a_brin").unwrap();
    assert_eq!(
        index
            .index_meta
            .as_ref()
            .and_then(|meta| meta.brin_options.as_ref())
            .map(|options| options.pages_per_range),
        Some(32)
    );
}

#[test]
fn create_gist_box_index_supports_knn_order_by() {
    let base = temp_dir("gist_box_knn_order");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table boxes (id int4 not null, b box)")
        .unwrap();
    db.execute(
        1,
        "insert into boxes values \
         (1, '(0,0),(1,1)'::box), \
         (2, '(5,5),(6,6)'::box), \
         (3, '(10,10),(12,12)'::box), \
         (4, '(7,7),(8,8)'::box)",
    )
    .unwrap();

    let sql = "select id from boxes \
               order by b <-> '(5.2,5.2),(5.2,5.2)'::box \
               limit 3";
    let expected = query_rows(&db, 1, sql);
    assert_eq!(
        expected,
        vec![
            vec![Value::Int32(2)],
            vec![Value::Int32(4)],
            vec![Value::Int32(1)],
        ]
    );

    db.execute(1, "create index boxes_b_gist on boxes using gist (b)")
        .unwrap();

    assert_explain_uses_index(&db, 1, sql, "boxes_b_gist");
    assert_eq!(query_rows(&db, 1, sql), expected);
}

#[test]
fn create_spgist_text_index_reports_missing_default_opclass() {
    let base = temp_dir("spgist_missing_default_opclass");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table texts (t text)").unwrap();

    match db.execute(1, "create index texts_t_spgist on texts using spgist (t)") {
        Err(ExecError::Parse(ParseError::MissingDefaultOpclass {
            access_method,
            type_name,
        })) => {
            assert_eq!(access_method, "spgist");
            assert_eq!(type_name, "text");
        }
        other => panic!("expected missing default opclass error, got {:?}", other),
    }
}

#[test]
fn create_spgist_rejects_multicolumn_indexes() {
    let base = temp_dir("spgist_reject_multicolumn");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table boxes (a box, b box)").unwrap();

    match db.execute(
        1,
        "create index boxes_ab_spgist on boxes using spgist (a, b)",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(
                message,
                "access method \"spgist\" does not support multicolumn indexes"
            );
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected multicolumn SP-GiST rejection, got {:?}", other),
    }
}

#[test]
fn create_spgist_rejects_expression_indexes() {
    let base = temp_dir("spgist_reject_expression");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table boxes (b box)").unwrap();

    match db.execute(
        1,
        "create index boxes_expr_spgist on boxes using spgist ((box('(0,0)'::point)))",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(
                message,
                "access method \"spgist\" does not support expression indexes"
            );
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected expression SP-GiST rejection, got {:?}", other),
    }
}

#[test]
fn create_statistics_rejects_single_column_with_postgres_message() {
    let base = temp_dir("create_statistics_single_column");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table ext_stats_test (x text, y int, z int)")
        .unwrap();

    match db.execute(1, "create statistics tst on (y) from ext_stats_test") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(message, "extended statistics require at least 2 columns");
            assert_eq!(sqlstate, "42P16");
        }
        other => panic!(
            "expected single-column CREATE STATISTICS rejection, got {:?}",
            other
        ),
    }
}

#[test]
fn create_statistics_rejects_unwrapped_expression_with_syntax_error() {
    let base = temp_dir("create_statistics_unwrapped_expression");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table ext_stats_test (x text, y int, z int)")
        .unwrap();

    match db.execute(1, "create statistics tst on y + z from ext_stats_test") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. })) => {
            assert_eq!(actual, "syntax error at or near \"+\"");
        }
        other => panic!("expected CREATE STATISTICS syntax error, got {:?}", other),
    }
}

#[test]
fn create_statistics_rejects_tuple_expression_with_syntax_error() {
    let base = temp_dir("create_statistics_tuple_expression");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table ext_stats_test (x text, y int, z int)")
        .unwrap();

    match db.execute(1, "create statistics tst on (x, y) from ext_stats_test") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. })) => {
            assert_eq!(actual, "syntax error at or near \",\"");
        }
        other => panic!(
            "expected CREATE STATISTICS tuple syntax error, got {:?}",
            other
        ),
    }
}

#[test]
fn create_statistics_rejects_xid_column_with_postgres_message() {
    let base = temp_dir("create_statistics_xid_column");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table ext_stats_test1 (x int, y int, z int, w xid)",
    )
    .unwrap();

    match db.execute(
        1,
        "create statistics tst (ndistinct) on w from ext_stats_test1",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(
                message,
                "column \"w\" cannot be used in statistics because its type xid has no default btree operator class"
            );
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected xid CREATE STATISTICS rejection, got {:?}", other),
    }
}

#[test]
fn create_spgist_box_index_supports_overlap_and_knn_order_by() {
    let base = temp_dir("spgist_box_overlap_knn");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table boxes (id int4 not null, b box)")
        .unwrap();
    db.execute(
        1,
        "insert into boxes values \
         (1, '(0,0),(1,1)'::box), \
         (2, '(5,5),(6,6)'::box), \
         (3, '(10,10),(12,12)'::box), \
         (4, '(7,7),(8,8)'::box)",
    )
    .unwrap();
    db.execute(1, "create index boxes_b_spgist on boxes using spgist (b)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select indclass \
             from pg_index \
             where indexrelid = (select oid from pg_class where relname = 'boxes_b_spgist')",
        ),
        vec![vec![Value::Text(
            crate::include::catalog::BOX_SPGIST_OPCLASS_OID
                .to_string()
                .into()
        )]]
    );

    let overlap_sql = "select id from boxes where b && '(6,6),(7,7)'::box order by id";
    assert_explain_uses_index(&db, 1, overlap_sql, "boxes_b_spgist");
    assert_eq!(
        query_rows(&db, 1, overlap_sql),
        vec![vec![Value::Int32(2)], vec![Value::Int32(4)]]
    );

    let left_of_sql = "select * from boxes where b << '(10,20),(30,40)'::box";
    let left_of_lines = explain_lines(&db, 1, left_of_sql);
    assert!(
        left_of_lines
            .iter()
            .any(|line| line.contains("Index Scan using boxes_b_spgist on boxes")),
        "expected named index scan in EXPLAIN, got {left_of_lines:?}"
    );
    assert!(
        left_of_lines
            .iter()
            .any(|line| line.contains("Index Cond: (b << '(30,40),(10,20)'::box)")),
        "expected box index condition in EXPLAIN, got {left_of_lines:?}"
    );

    let knn_sql = "select id from boxes \
                   order by b <-> '(5.2,5.2)'::point \
                   limit 3";
    let expected = query_rows(&db, 1, knn_sql);
    assert_eq!(
        expected,
        vec![
            vec![Value::Int32(2)],
            vec![Value::Int32(4)],
            vec![Value::Int32(1)],
        ]
    );
    assert_explain_uses_index(&db, 1, knn_sql, "boxes_b_spgist");
}

#[test]
fn create_spgist_polygon_index_uses_default_opclass() {
    let base = temp_dir("spgist_polygon_overlap_knn");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table polys (id int4 not null, p polygon)")
        .unwrap();
    db.execute(
        1,
        "insert into polys values \
         (1, '((0,0),(2,0),(1,2))'::polygon), \
         (2, '((5,5),(7,5),(6,7))'::polygon), \
         (3, '((10,10),(13,10),(11,13))'::polygon), \
         (4, '((6,6),(9,6),(8,9))'::polygon)",
    )
    .unwrap();

    let overlap_sql = "select id from polys where p && '((6,6),(8,6),(7,8))'::polygon order by id";
    let expected_overlap = query_rows(&db, 1, overlap_sql);
    let knn_sql = "select id from polys order by p <-> '(5.5,5.5)'::point limit 3";
    let expected_knn = query_rows(&db, 1, knn_sql);

    db.execute(1, "create index polys_p_spgist on polys using spgist (p)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select indclass \
             from pg_index \
             where indexrelid = (select oid from pg_class where relname = 'polys_p_spgist')",
        ),
        vec![vec![Value::Text(
            crate::include::catalog::POLY_SPGIST_OPCLASS_OID
                .to_string()
                .into()
        )]]
    );
    assert_eq!(query_rows(&db, 1, overlap_sql), expected_overlap);
    assert_eq!(query_rows(&db, 1, knn_sql), expected_knn);
}

#[test]
fn spgist_box_index_matches_seq_scan_on_medium_dataset() {
    let base = temp_dir("spgist_box_medium_semantics");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table boxes (id int4 not null, b box)")
        .unwrap();
    for row in 0..8 {
        for col in 0..8 {
            let id = row * 8 + col + 1;
            let x = col as f64 * 10.0;
            let y = row as f64 * 7.0;
            db.execute(
                1,
                &format!(
                    "insert into boxes values ({id}, '({x},{y}),({},{})'::box)",
                    x + 2.5,
                    y + 3.5
                ),
            )
            .unwrap();
        }
    }

    let overlap_sql = "select id from boxes where b && '(15,10),(32,23)'::box order by id";
    let left_sql = "select id from boxes where b << '(40,0),(80,80)'::box order by id";
    let contained_sql = "select id from boxes where b <@ '(0,0),(35,26)'::box order by id";
    let knn_sql = "select id from boxes order by b <-> '(23,19)'::point limit 10";

    let expected_overlap = query_rows(&db, 1, overlap_sql);
    let expected_left = query_rows(&db, 1, left_sql);
    let expected_contained = query_rows(&db, 1, contained_sql);
    let expected_knn = query_rows(&db, 1, knn_sql);

    db.execute(1, "create index boxes_b_spgist on boxes using spgist (b)")
        .unwrap();

    assert_explain_uses_index(&db, 1, overlap_sql, "boxes_b_spgist");
    assert_explain_uses_index(&db, 1, knn_sql, "boxes_b_spgist");
    assert_eq!(query_rows(&db, 1, overlap_sql), expected_overlap);
    assert_eq!(query_rows(&db, 1, left_sql), expected_left);
    assert_eq!(query_rows(&db, 1, contained_sql), expected_contained);
    assert_eq!(query_rows(&db, 1, knn_sql), expected_knn);
}

#[test]
fn spgist_box_window_knn_avoids_sort_when_index_can_supply_order() {
    let base = temp_dir("spgist_box_window_knn");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table boxes (id int4 not null, b box)")
        .unwrap();
    for row in 0..24 {
        for col in 0..24 {
            let id = row * 24 + col + 1;
            let x = col as f64 * 10.0;
            let y = row as f64 * 7.0;
            db.execute(
                1,
                &format!(
                    "insert into boxes values ({id}, '({x},{y}),({},{})'::box)",
                    x + 3.0,
                    y + 2.0
                ),
            )
            .unwrap();
        }
    }
    db.execute(1, "create index boxes_b_spgist on boxes using spgist (b)")
        .unwrap();

    let window_sql = "select rank() over (order by b <-> '(123,456)'::point) from boxes";
    let window_lines = explain_lines(&db, 1, window_sql);
    assert!(
        window_lines
            .iter()
            .any(|line| line.contains("Index Scan using boxes_b_spgist on boxes")),
        "expected ordered window query to use SP-GiST index, got {window_lines:?}"
    );
    assert!(
        !window_lines.iter().any(|line| line.contains("Sort")),
        "expected ordered window query to avoid Sort, got {window_lines:?}"
    );

    let filtered_window_sql = "select rank() over (order by b <-> '(123,456)'::point) \
                               from boxes \
                               where b <@ '(100,150),(220,320)'::box";
    let filtered_window_lines = explain_lines(&db, 1, filtered_window_sql);
    assert!(
        filtered_window_lines
            .iter()
            .any(|line| line.contains("Index Scan using boxes_b_spgist on boxes")),
        "expected filtered ordered window query to use SP-GiST index, got {filtered_window_lines:?}"
    );
    assert!(
        !filtered_window_lines
            .iter()
            .any(|line| line.contains("Sort")),
        "expected filtered ordered window query to avoid Sort, got {filtered_window_lines:?}"
    );
}

#[test]
fn create_gist_range_index_explain_and_query_use_it() {
    let base = temp_dir("gist_range_index_scan_explain");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table spans (id int4 not null, span int4range)")
        .unwrap();
    db.execute(
        1,
        "insert into spans values \
         (1, '[1,5)'::int4range), \
         (2, '[5,9)'::int4range), \
         (3, '[20,30)'::int4range)",
    )
    .unwrap();
    db.execute(
        1,
        "create index spans_range_gist on spans using gist (span)",
    )
    .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select id from spans where span @> 7",
        "spans_range_gist",
    );
    assert_eq!(
        query_rows(&db, 1, "select id from spans where span @> 7 order by id"),
        vec![vec![Value::Int32(2)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans where span && '[4,6)'::int4range order by id",
        ),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]
    );
}

#[test]
fn create_gist_multirange_index_explain_and_query_use_it() {
    let base = temp_dir("gist_multirange_index_scan_explain");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table spans (id int4 not null, mr int4multirange)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into spans values \
         (1, int4multirange(int4range(1,5), int4range(10,15))), \
         (2, int4multirange(int4range(20,30))), \
         (3, int4multirange(int4range(3,8))), \
         (4, '{}'::int4multirange)",
    )
    .unwrap();
    db.execute(1, "create index spans_mr_gist on spans using gist (mr)")
        .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select id from spans where mr @> 11",
        "spans_mr_gist",
    );
    assert_explain_uses_index(
        &db,
        1,
        "select id from spans where mr = '{}'::int4multirange",
        "spans_mr_gist",
    );
    assert_explain_uses_index(
        &db,
        1,
        "select id from spans where mr @> 'empty'::int4range",
        "spans_mr_gist",
    );
    assert_eq!(
        query_rows(&db, 1, "select id from spans where mr @> 11 order by id"),
        vec![vec![Value::Int32(1)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans where mr && int4range(4,12) order by id",
        ),
        vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans where mr @> 'empty'::int4range order by id",
        ),
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
            vec![Value::Int32(4)]
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans where mr @> '{}'::int4multirange order by id",
        ),
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
            vec![Value::Int32(4)]
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans \
             where mr &< int4multirange(int4range(18,20)) \
             order by id",
        ),
        vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans where mr = '{}'::int4multirange order by id",
        ),
        vec![vec![Value::Int32(4)]]
    );
}

#[test]
fn multirange_adjacency_uses_outer_endpoints_only() {
    let base = temp_dir("multirange_endpoint_adjacency");
    let db = Database::open(&base, 16).unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
             int4multirange(int4range(1,2), int4range(5,6)) -|- int4range(3,5), \
             int4multirange(int4range(1,2), int4range(5,6)) -|- int4range(6,7), \
             int4range(0,1) -|- int4multirange(int4range(1,2), int4range(5,6)), \
             int4multirange(int4range(1,2), int4range(5,6)) -|- int4multirange(int4range(3,5)), \
             int4multirange(int4range(1,2), int4range(5,6)) -|- int4multirange(int4range(6,7))",
        ),
        vec![vec![
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
        ]]
    );
}

#[test]
fn create_gist_range_index_with_explicit_opclass_uses_matching_type() {
    let base = temp_dir("gist_range_explicit_opclass");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table spans (span tsrange)").unwrap();
    db.execute(
        1,
        "create index spans_ts_gist on spans using gist (span range_ops)",
    )
    .unwrap();

    let rows = query_rows(
        &db,
        1,
        "select indclass \
         from pg_index \
         where indexrelid = (select oid from pg_class where relname = 'spans_ts_gist')",
    );
    assert_eq!(
        rows,
        vec![vec![Value::Text(
            crate::include::catalog::TSRANGE_GIST_OPCLASS_OID
                .to_string()
                .into()
        )]]
    );
}

#[test]
fn without_overlaps_primary_key_records_catalog_metadata_and_enforces_overlaps() {
    let base = temp_dir("without_overlaps_pk_enforcement");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table temporal_items (\
             id int4, \
             valid_at int4range, \
             constraint temporal_items_pk primary key (id, valid_at without overlaps)\
         )",
    )
    .unwrap();

    let lookup = db.lazy_catalog_lookup(1, None, None);
    let relation = lookup.lookup_any_relation("temporal_items").unwrap();
    let constraint = lookup
        .constraint_rows_for_relation(relation.relation_oid)
        .into_iter()
        .find(|row| row.conname == "temporal_items_pk")
        .unwrap();
    assert!(constraint.conperiod);
    assert_eq!(constraint.conkey.as_deref(), Some(&[1, 2][..]));
    assert_eq!(constraint.conexclop.as_ref().map(Vec::len), Some(2));
    let index = lookup
        .index_relations_for_heap(relation.relation_oid)
        .into_iter()
        .find(|index| index.relation_oid == constraint.conindid)
        .unwrap();
    assert!(index.index_meta.indisexclusion);
    assert_eq!(
        psql_index_definition(&db, 1, "temporal_items", "temporal_items_pk"),
        "CREATE UNIQUE INDEX temporal_items_pk ON temporal_items USING gist (id, valid_at)"
    );
    drop(lookup);

    assert_eq!(
        query_rows(
            &db,
            1,
            "select pg_get_constraintdef(oid) from pg_constraint where conname = 'temporal_items_pk'",
        ),
        vec![vec![Value::Text(
            "PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)".into()
        )]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select pg_get_indexdef(conindid, 0, true) from pg_constraint where conname = 'temporal_items_pk'",
        ),
        vec![vec![Value::Text(
            "CREATE UNIQUE INDEX temporal_items_pk ON temporal_items USING gist (id, valid_at)"
                .into()
        )]]
    );

    db.execute(
        1,
        "insert into temporal_items values \
         (1, '[1,5)'::int4range), \
         (1, '[5,9)'::int4range), \
         (2, '[3,7)'::int4range)",
    )
    .unwrap();

    match db.execute(
        1,
        "insert into temporal_items values (1, '[4,6)'::int4range)",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(sqlstate, "23P01");
            assert_eq!(
                message,
                "conflicting key value violates exclusion constraint \"temporal_items_pk\""
            );
        }
        other => panic!("expected temporal exclusion violation, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select count(*) from temporal_items"),
        vec![vec![Value::Int64(3)]]
    );
}

#[test]
fn without_overlaps_unique_handles_nulls_empty_ranges_and_updates() {
    let base = temp_dir("without_overlaps_unique_runtime");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table temporal_unique (\
             id int4, \
             marker int4, \
             valid_at int4range, \
             constraint temporal_unique_key unique (id, valid_at without overlaps)\
         )",
    )
    .unwrap();
    db.execute(
        1,
        "insert into temporal_unique values \
         (1, 1, '[1,5)'::int4range), \
         (1, 2, '[5,9)'::int4range), \
         (null, 3, '[1,5)'::int4range), \
         (null, 4, '[2,4)'::int4range), \
         (2, 5, null), \
         (2, 6, null)",
    )
    .unwrap();

    match db.execute(
        1,
        "insert into temporal_unique values (null, 7, 'empty'::int4range)",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(sqlstate, "23P01");
            assert!(message.contains("empty WITHOUT OVERLAPS value"));
        }
        other => panic!("expected empty range violation, got {other:?}"),
    }

    match db.execute(
        1,
        "update temporal_unique set valid_at = '[2,6)'::int4range where marker = 2",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(sqlstate, "23P01");
            assert_eq!(
                message,
                "conflicting key value violates exclusion constraint \"temporal_unique_key\""
            );
        }
        other => panic!("expected temporal exclusion violation on update, got {other:?}"),
    }
}

#[test]
fn alter_table_add_without_overlaps_validates_existing_rows() {
    let base = temp_dir("without_overlaps_alter_table");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table clean_periods (id int4, valid_at int4range)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into clean_periods values (1, '[1,5)'::int4range), (1, '[5,9)'::int4range)",
    )
    .unwrap();
    db.execute(
        1,
        "alter table clean_periods add constraint clean_periods_key unique (id, valid_at without overlaps)",
    )
    .unwrap();

    let lookup = db.lazy_catalog_lookup(1, None, None);
    let relation = lookup.lookup_any_relation("clean_periods").unwrap();
    let constraint = lookup
        .constraint_rows_for_relation(relation.relation_oid)
        .into_iter()
        .find(|row| row.conname == "clean_periods_key")
        .unwrap();
    assert!(constraint.conperiod);
    drop(lookup);

    db.execute(
        1,
        "create table conflicting_periods (id int4, valid_at int4range)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into conflicting_periods values (1, '[1,5)'::int4range), (1, '[4,9)'::int4range)",
    )
    .unwrap();
    match db.execute(
        1,
        "alter table conflicting_periods add constraint conflicting_periods_key unique (id, valid_at without overlaps)",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(sqlstate, "23P01");
            assert_eq!(
                message,
                "could not create exclusion constraint \"conflicting_periods_key\""
            );
        }
        other => panic!("expected temporal validation failure, got {other:?}"),
    }
}

#[test]
fn without_overlaps_on_conflict_do_nothing_uses_temporal_arbiters() {
    let base = temp_dir("without_overlaps_on_conflict");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table temporal_conflict (\
             id int4, \
             valid_at int4range, \
             constraint temporal_conflict_pk primary key (id, valid_at without overlaps)\
         )",
    )
    .unwrap();
    db.execute(
        1,
        "insert into temporal_conflict values (1, '[1,5)'::int4range)",
    )
    .unwrap();

    assert_eq!(
        db.execute(
            1,
            "insert into temporal_conflict values (1, '[4,8)'::int4range) on conflict do nothing",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute(
            1,
            "insert into temporal_conflict values (1, '[4,8)'::int4range) \
             on conflict on constraint temporal_conflict_pk do nothing",
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );

    match db.execute(
        1,
        "insert into temporal_conflict values (1, '[4,8)'::int4range) \
         on conflict (id, valid_at) do nothing",
    ) {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual })) => {
            assert_eq!(expected, "inferable unique btree index");
            assert_eq!(
                actual,
                "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            );
        }
        other => panic!("expected arbiter inference rejection, got {other:?}"),
    }

    match db.execute(
        1,
        "insert into temporal_conflict values (1, '[4,8)'::int4range) \
         on conflict on constraint temporal_conflict_pk do update set valid_at = excluded.valid_at",
    ) {
        Err(ExecError::Parse(ParseError::DetailedError {
            message, sqlstate, ..
        })) => {
            assert_eq!(sqlstate, "0A000");
            assert_eq!(
                message,
                "ON CONFLICT DO UPDATE not supported with exclusion constraints"
            );
        }
        other => panic!("expected unsupported temporal DO UPDATE, got {other:?}"),
    }
}

#[test]
fn like_including_all_copies_without_overlaps_constraints() {
    let base = temp_dir("without_overlaps_like_including_all");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table temporal_like_src (\
             id int4, \
             valid_at int4range, \
             constraint temporal_like_src_pk primary key (id, valid_at without overlaps)\
         )",
    )
    .unwrap();
    db.execute(
        1,
        "create table temporal_like_dst (like temporal_like_src including all)",
    )
    .unwrap();

    let lookup = db.lazy_catalog_lookup(1, None, None);
    let relation = lookup.lookup_any_relation("temporal_like_dst").unwrap();
    let constraint = lookup
        .constraint_rows_for_relation(relation.relation_oid)
        .into_iter()
        .find(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
        .unwrap();
    assert!(constraint.conperiod);
    let index = lookup
        .index_relations_for_heap(relation.relation_oid)
        .into_iter()
        .find(|index| index.relation_oid == constraint.conindid)
        .unwrap();
    assert!(index.index_meta.indisexclusion);
}

#[test]
fn create_table_like_copies_generated_only_when_requested() {
    let base = temp_dir("create_table_like_generated");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table like_gen_src (a int4, b int4 generated always as (a + 1) stored)",
    )
    .unwrap();
    db.execute(1, "create table like_gen_plain (like like_gen_src)")
        .unwrap();
    db.execute(
        1,
        "create table like_gen_copy (like like_gen_src including generated)",
    )
    .unwrap();

    db.execute(1, "insert into like_gen_plain values (4, 99)")
        .unwrap();
    db.execute(1, "insert into like_gen_copy(a) values (4)")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select b from like_gen_plain"),
        vec![vec![Value::Int32(99)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select b from like_gen_copy"),
        vec![vec![Value::Int32(5)]]
    );
}

#[test]
fn create_table_like_copies_identity_only_when_requested() {
    let base = temp_dir("create_table_like_identity");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table like_id_src (a bigint generated always as identity, b text)",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attidentity from pg_attribute where attrelid = (select oid from pg_class where relname = 'like_id_src') and attname = 'a'",
        ),
        vec![vec![Value::Text("a".into())]]
    );
    db.execute(1, "create table like_id_plain (like like_id_src)")
        .unwrap();
    db.execute(
        1,
        "create table like_id_copy (like like_id_src including identity)",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attidentity from pg_attribute where attrelid = (select oid from pg_class where relname = 'like_id_copy') and attname = 'a'",
        ),
        vec![vec![Value::Text("a".into())]]
    );

    match db.execute(1, "insert into like_id_plain (b) values ('plain')") {
        Err(ExecError::NotNullViolation { column, .. }) => assert_eq!(column, "a"),
        other => panic!("expected missing identity default to fail, got {other:?}"),
    }
    db.execute(1, "insert into like_id_copy (b) values ('copy')")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select a, b from like_id_copy"),
        vec![vec![Value::Int64(1), Value::Text("copy".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attidentity from pg_attribute where attrelid = (select oid from pg_class where relname = 'like_id_copy') and attname = 'a'",
        ),
        vec![vec![Value::Text("a".into())]]
    );
}

#[test]
fn create_table_like_copies_storage_and_compression_only_when_requested() {
    let base = temp_dir("create_table_like_storage_compression");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table like_storage_src (a text compression pglz)")
        .unwrap();
    db.execute(
        1,
        "alter table like_storage_src alter column a set storage external",
    )
    .unwrap();
    db.execute(1, "create table like_storage_plain (like like_storage_src)")
        .unwrap();
    db.execute(
        1,
        "create table like_storage_copy (like like_storage_src including storage including compression)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstorage, attcompression from pg_attribute where attrelid = (select oid from pg_class where relname = 'like_storage_plain') and attname = 'a'",
        ),
        vec![vec![Value::Text("x".into()), Value::Text("".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select attstorage, attcompression from pg_attribute where attrelid = (select oid from pg_class where relname = 'like_storage_copy') and attname = 'a'",
        ),
        vec![vec![Value::Text("e".into()), Value::Text("p".into())]]
    );
}

#[test]
fn create_table_like_copies_column_comments_only_when_requested() {
    let base = temp_dir("create_table_like_comments");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table like_comment_src (a int4, b text)")
        .unwrap();
    let relation = db
        .lazy_catalog_lookup(1, None, None)
        .lookup_any_relation("like_comment_src")
        .unwrap();
    let xid = db.txns.write().begin();
    let ctx = crate::backend::catalog::store::CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid: 0,
        client_id: 1,
        waiter: Some(db.txn_waiter.clone()),
        interrupts: db.interrupt_state(1),
    };
    let effect = db
        .catalog
        .write()
        .comment_column_mvcc(relation.relation_oid, 2, Some("copied b"), &ctx)
        .unwrap();
    db.apply_catalog_mutation_effect_immediate(&effect).unwrap();
    db.txns.write().commit(xid).unwrap();

    db.execute(1, "create table like_comment_plain (like like_comment_src)")
        .unwrap();
    db.execute(
        1,
        "create table like_comment_copy (like like_comment_src including comments)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'like_comment_plain' and d.classoid = 1259 and d.objsubid = 2",
        ),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_class c on c.oid = d.objoid \
             where c.relname = 'like_comment_copy' and d.classoid = 1259 and d.objsubid = 2",
        ),
        vec![vec![Value::Text("copied b".into())]]
    );
}

#[test]
fn create_table_like_copies_statistics_only_when_requested() {
    let base = temp_dir("create_table_like_statistics");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table like_stats_src (a int4, b int4, c int4)")
        .unwrap();
    db.execute(
        1,
        "create statistics like_stats_src_ab_stats (dependencies) on a, b from like_stats_src",
    )
    .unwrap();
    db.execute(
        1,
        "comment on statistics like_stats_src_ab_stats is 'source stats'",
    )
    .unwrap();

    db.execute(1, "create table like_stats_plain (like like_stats_src)")
        .unwrap();
    db.execute(
        1,
        "create table like_stats_copy (like like_stats_src including statistics including comments)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_statistic_ext where stxrelid = (select oid from pg_class where relname = 'like_stats_plain')",
        ),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select stxname, stxkeys, stxkind \
             from pg_statistic_ext \
             where stxrelid = (select oid from pg_class where relname = 'like_stats_copy')",
        ),
        vec![vec![
            Value::Text("like_stats_copy_a_stat".into()),
            Value::Text("1 2".into()),
            typed_text_array_value(&["f"], crate::include::catalog::INTERNAL_CHAR_TYPE_OID),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.description \
             from pg_description d \
             join pg_statistic_ext s on s.oid = d.objoid \
             where s.stxname = 'like_stats_copy_a_stat' and d.classoid = 3381",
        ),
        vec![vec![Value::Text("source stats".into())]]
    );
}

#[test]
fn alter_table_add_without_overlaps_on_inherited_columns() {
    let base = temp_dir("without_overlaps_inherited_alter");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table temporal_parent (id int4range, valid_at daterange)",
    )
    .unwrap();
    db.execute(
        1,
        "create table temporal_child () inherits (temporal_parent)",
    )
    .unwrap();
    let lookup = db.lazy_catalog_lookup(1, None, None);
    let relation = lookup.lookup_any_relation("temporal_child").unwrap();
    assert_eq!(
        relation
            .desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "valid_at"]
    );
    drop(lookup);

    db.execute(
        1,
        "alter table temporal_child \
         add constraint temporal_child_pk primary key (id, valid_at without overlaps)",
    )
    .unwrap();

    let lookup = db.lazy_catalog_lookup(1, None, None);
    let relation = lookup.lookup_any_relation("temporal_child").unwrap();
    let constraint = lookup
        .constraint_rows_for_relation(relation.relation_oid)
        .into_iter()
        .find(|row| row.conname == "temporal_child_pk")
        .unwrap();
    assert!(constraint.conperiod);
    assert!(
        relation
            .desc
            .columns
            .iter()
            .all(|column| !column.storage.nullable)
    );
}

#[test]
fn create_gist_expression_index_builds_and_tracks_expression_metadata() {
    let base = temp_dir("gist_expression_index_build");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table spans (id int4 not null, span int4range)")
        .unwrap();
    db.execute(
        1,
        "insert into spans values \
         (1, '[1,5)'::int4range), \
         (2, '[5,9)'::int4range), \
         (3, '[20,30)'::int4range)",
    )
    .unwrap();
    db.execute(
        1,
        "create index spans_expr_gist on spans using gist ((range_merge(span, span)))",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select indkey, indexprs \
             from pg_index \
             where indexrelid = (select oid from pg_class where relname = 'spans_expr_gist')",
        ),
        vec![vec![
            Value::Text("0".into()),
            Value::Text("[\"range_merge(span, span)\"]".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans \
             where range_merge(span, span) @> 7 \
             order by id",
        ),
        vec![vec![Value::Int32(2)]]
    );

    db.execute(1, "insert into spans values (4, '[6,10)'::int4range)")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id from spans \
             where range_merge(span, span) @> 7 \
             order by id",
        ),
        vec![vec![Value::Int32(2)], vec![Value::Int32(4)]]
    );
}

#[test]
fn gist_split_recovery_preserves_exact_and_knn_scans_after_reopen() {
    let base = temp_dir("gist_split_recovery");
    let exact_sql = "select id from boxes \
                     where b && '(250.1,250.1),(250.9,250.9)'::box \
                     order by id";
    let knn_sql = "select id from boxes \
                   order by b <-> '(250.2,250.2),(250.2,250.2)'::box \
                   limit 3";

    let (expected_exact, expected_knn) = {
        let db = Database::open(&base, 128).unwrap();

        db.execute(1, "create table boxes (id int4 not null, b box)")
            .unwrap();
        db.execute(1, "create index boxes_b_gist on boxes using gist (b)")
            .unwrap();

        for i in 0..600 {
            db.execute(
                1,
                &format!(
                    "insert into boxes values ({id}, '({low},{low}),({high},{high})'::box)",
                    id = i,
                    low = i,
                    high = i + 1
                ),
            )
            .unwrap();
        }

        let index_rel = relation_locator_for(&db, 1, "boxes_b_gist");
        assert!(
            relation_fork_nblocks(
                &db,
                index_rel,
                crate::backend::storage::smgr::ForkNumber::Main
            ) > 1
        );
        let root_page = read_buffered_relation_block(&db, 1, index_rel, 0);
        let root_opaque = crate::include::access::gist::gist_page_get_opaque(&root_page).unwrap();
        assert!(
            !root_opaque.is_leaf(),
            "expected internal GiST root after splits"
        );
        assert!(
            relation_has_dirty_buffered_page(&db, 1, index_rel),
            "expected at least one uncheckpointed GiST page before reopen"
        );

        assert_explain_uses_index(&db, 1, exact_sql, "boxes_b_gist");
        assert_explain_uses_index(&db, 1, knn_sql, "boxes_b_gist");

        (query_rows(&db, 1, exact_sql), query_rows(&db, 1, knn_sql))
    };

    let reopened = Database::open(&base, 128).unwrap();
    let index_rel = relation_locator_for(&reopened, 1, "boxes_b_gist");
    let root_page = read_relation_block(&reopened, index_rel, 0);
    let root_opaque = crate::include::access::gist::gist_page_get_opaque(&root_page).unwrap();
    assert!(
        !root_opaque.is_leaf(),
        "expected internal GiST root after recovery"
    );

    assert_explain_uses_index(&reopened, 1, exact_sql, "boxes_b_gist");
    assert_explain_uses_index(&reopened, 1, knn_sql, "boxes_b_gist");
    assert_eq!(query_rows(&reopened, 1, exact_sql), expected_exact);
    assert_eq!(query_rows(&reopened, 1, knn_sql), expected_knn);
}

#[test]
fn gist_vacuum_removes_dead_entries_and_replays_after_reopen() {
    let base = temp_dir("gist_vacuum_recovery");
    let exact_sql = "select id from boxes where b && '(6,6),(7,7)'::box order by id";
    let knn_sql = "select id from boxes \
                   order by b <-> '(5.2,5.2),(5.2,5.2)'::box \
                   limit 3";

    let (expected_exact, expected_knn, tuples_after_vacuum) = {
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table boxes (id int4 not null, b box)")
            .unwrap();
        session
            .execute(
                &db,
                "insert into boxes values \
                 (1, '(0,0),(1,1)'::box), \
                 (2, '(5,5),(6,6)'::box), \
                 (3, '(10,10),(12,12)'::box), \
                 (4, '(7,7),(8,8)'::box)",
            )
            .unwrap();
        session
            .execute(&db, "create index boxes_b_gist on boxes using gist (b)")
            .unwrap();

        let index_rel = relation_locator_for(&db, 1, "boxes_b_gist");
        let tuples_before_delete = gist_leaf_tuple_count(&db, 1, index_rel);
        assert_eq!(tuples_before_delete, 4);

        session
            .execute(&db, "delete from boxes where id = 2")
            .unwrap();
        assert_eq!(
            gist_leaf_tuple_count(&db, 1, index_rel),
            tuples_before_delete
        );

        session.execute(&db, "vacuum boxes").unwrap();

        let tuples_after_vacuum = gist_leaf_tuple_count(&db, 1, index_rel);
        assert_eq!(tuples_after_vacuum, 3);
        assert!(
            relation_has_dirty_buffered_page(&db, 1, index_rel),
            "expected GiST vacuum to leave uncheckpointed page images for recovery"
        );

        assert_explain_uses_index(&db, 1, exact_sql, "boxes_b_gist");
        assert_explain_uses_index(&db, 1, knn_sql, "boxes_b_gist");
        let expected_exact = query_rows(&db, 1, exact_sql);
        let expected_knn = query_rows(&db, 1, knn_sql);
        assert_eq!(expected_exact, vec![vec![Value::Int32(4)]]);
        assert_eq!(
            expected_knn,
            vec![
                vec![Value::Int32(4)],
                vec![Value::Int32(1)],
                vec![Value::Int32(3)],
            ]
        );

        (expected_exact, expected_knn, tuples_after_vacuum)
    };

    let reopened = Database::open(&base, 64).unwrap();
    let index_rel = relation_locator_for(&reopened, 1, "boxes_b_gist");
    assert_eq!(
        gist_leaf_tuple_count(&reopened, 1, index_rel),
        tuples_after_vacuum
    );
    assert_explain_uses_index(&reopened, 1, exact_sql, "boxes_b_gist");
    assert_explain_uses_index(&reopened, 1, knn_sql, "boxes_b_gist");
    assert_eq!(query_rows(&reopened, 1, exact_sql), expected_exact);
    assert_eq!(query_rows(&reopened, 1, knn_sql), expected_knn);
}

#[test]
fn gist_concurrent_split_scans_do_not_miss_committed_rows() {
    let base = temp_dir("gist_concurrent_splits");
    let db = Database::open(&base, 128).unwrap();
    let sql = "select id from boxes where b && '(0,0),(400,400)'::box order by id";

    db.execute(1, "create table boxes (id int4 not null, b box)")
        .unwrap();
    db.execute(1, "create index boxes_b_gist on boxes using gist (b)")
        .unwrap();

    for i in 0..200 {
        db.execute(
            1,
            &format!(
                "insert into boxes values ({id}, '({low},{low}),({high},{high})'::box)",
                id = 10_000 + i,
                low = 1_000 + i,
                high = 1_001 + i
            ),
        )
        .unwrap();
    }

    let committed = Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
    for i in 0..120 {
        db.execute(
            1,
            &format!(
                "insert into boxes values ({id}, '({low},{low}),({high},{high})'::box)",
                id = i,
                low = i,
                high = i + 1
            ),
        )
        .unwrap();
        committed.lock().unwrap().push(i);
    }

    assert_explain_uses_index(&db, 1, sql, "boxes_b_gist");

    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut handles = Vec::new();

    for writer in 0..2 {
        let db = db.clone();
        let committed = Arc::clone(&committed);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..120 {
                let id = 1_000 + writer * 1_000 + i;
                let low = 120 + writer * 120 + i;
                db.execute(
                    (writer + 2) as ClientId,
                    &format!(
                        "insert into boxes values ({id}, '({low},{low}),({high},{high})'::box)",
                        high = low + 1
                    ),
                )
                .unwrap();
                committed.lock().unwrap().push(id);
            }
        }));
    }

    for reader in 0..2 {
        let db = db.clone();
        let committed = Arc::clone(&committed);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for iteration in 0..60 {
                let expected = {
                    let snapshot = committed.lock().unwrap().clone();
                    snapshot
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>()
                };
                let rows = query_rows(&db, (reader + 100) as ClientId, sql);
                let actual = rows
                    .iter()
                    .map(|row| int_value(&row[0]) as i32)
                    .collect::<std::collections::BTreeSet<_>>();
                for id in expected {
                    assert!(
                        actual.contains(&id),
                        "reader {reader} iteration {iteration}: missing committed id {id}"
                    );
                }
            }
        }));
    }

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    let final_expected = committed
        .lock()
        .unwrap()
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let final_actual = query_rows(&db, 1, sql)
        .iter()
        .map(|row| int_value(&row[0]) as i32)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(final_actual, final_expected);
}

#[test]
fn create_unique_gist_index_is_rejected() {
    let base = temp_dir("gist_unique_guard");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table spans (span int4range)")
        .unwrap();

    match db.execute(
        1,
        "create unique index spans_unique_gist on spans using gist (span)",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(message)))
            if message == "access method \"gist\" does not support unique indexes" => {}
        other => panic!("expected unique GiST rejection, got {other:?}"),
    }
}

#[test]
fn create_index_supports_expression_keys() {
    let base = temp_dir("create_index_expression_keys");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (a int4, d float8, e float8, b text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 2.5, 3.5, 'x')")
        .unwrap();
    db.execute(1, "create index items_expr_idx on items (a, (d + e), b)")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select indkey, indexprs \
             from pg_index \
             where indexrelid = (select oid from pg_class where relname = 'items_expr_idx')",
        ),
        vec![vec![
            Value::Text("1 0 4".into()),
            Value::Text("[\"d + e\"]".into()),
        ]]
    );
}

#[test]
fn unique_expression_index_rejects_duplicate_expression_value() {
    let base = temp_dir("unique_expression_index");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (name text)").unwrap();
    db.execute(
        1,
        "create unique index items_name_lower_key on items ((lower(name)))",
    )
    .unwrap();
    db.execute(1, "insert into items values ('Alpha')").unwrap();

    match db.execute(1, "insert into items values ('alpha')") {
        Err(ExecError::UniqueViolation { constraint, .. })
            if constraint == "items_name_lower_key" => {}
        other => panic!("expected unique expression violation, got {other:?}"),
    }
}

#[test]
fn expression_index_self_join_query_does_not_recurse_while_planning() {
    let base = temp_dir("expression_index_self_join");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table sj (a int unique, b int, c int unique)")
        .unwrap();
    db.execute(
        1,
        "insert into sj values (1, null, 2), (null, 2, null), (2, 1, 1), (3, 1, 3)",
    )
    .unwrap();
    db.execute(1, "create unique index sj_fn_idx on sj((a * a))")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from sj j1, sj j2 \
             where j1.b = j2.b and j1.a*j1.a = 1 and j2.a*j2.a = 1",
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn explain_bootstrap_seqscan_shows_relation_name_and_filter() {
    let base = temp_dir("explain_bootstrap_seqscan");
    let db = Database::open(&base, 16).unwrap();

    let lines = explain_lines(&db, 1, "select * from pg_proc where proname ~ 'abc'");
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("Seq Scan on pg_proc  (cost=")),
        "expected bootstrap relation name in EXPLAIN, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line == "  Filter: (proname ~ 'abc'::text)"),
        "expected pushed-down seqscan filter in EXPLAIN, got {lines:?}"
    );
}

#[test]
fn explain_heap_seqscan_shows_relation_name() {
    let base = temp_dir("explain_heap_seqscan_relation_name");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();

    let lines = explain_lines(&db, 1, "select * from items where id = 1");
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("Seq Scan on items  (cost=")),
        "expected heap relation name in EXPLAIN, got {lines:?}"
    );
}

#[test]
fn explain_verbose_count_nonnull_constant_elides_projection() {
    let base = temp_dir("explain_verbose_count_nonnull_constant");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table agg_simplify (a int4, not_null_col int4 not null, nullable_col int4)",
    )
    .unwrap();

    let lines = explain_lines(
        &db,
        1,
        "(verbose, costs off) select count('bananas'::text) from agg_simplify",
    );

    assert!(
        lines.iter().any(|line| line == "Aggregate"),
        "expected aggregate node, got {lines:?}"
    );
    assert!(
        lines.iter().any(|line| line == "  Output: count(*)"),
        "expected simplified count(*) output, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Seq Scan on agg_simplify")),
        "expected seq scan on agg_simplify, got {lines:?}"
    );
    assert!(
        lines.iter().all(|line| !line.contains("Projection")),
        "expected passthrough projection elision, got {lines:?}"
    );
    assert!(
        lines
            .iter()
            .all(|line| !line.contains("count('bananas'::text)")),
        "expected count argument simplification, got {lines:?}"
    );
}

#[test]
fn explain_inner_join_can_reorder_commutative_inputs() {
    let base = temp_dir("explain_inner_join_reorder");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table big_items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "create table small_items (id int4 not null, note text)")
        .unwrap();

    for id in 0..64 {
        db.execute(
            1,
            &format!("insert into big_items values ({id}, 'big{id}')"),
        )
        .unwrap();
    }
    for id in 0..4 {
        db.execute(
            1,
            &format!("insert into small_items values ({id}, 'small{id}')"),
        )
        .unwrap();
    }

    db.execute(1, "analyze big_items").unwrap();
    db.execute(1, "analyze small_items").unwrap();

    let lines = explain_lines(
        &db,
        1,
        "select * from big_items join small_items on big_items.id = small_items.id",
    );
    let big_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on big_items"))
        .unwrap();
    let small_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on small_items"))
        .unwrap();
    let hash_pos = lines
        .iter()
        .position(|line| line.trim_start().starts_with("->  Hash  "));
    assert!(
        small_pos < big_pos
            || hash_pos.is_some_and(|hash_pos| big_pos < hash_pos && hash_pos < small_pos),
        "expected planner to either scan the smaller relation first or hash it as the inner side after join reordering, got {lines:?}"
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select * from big_items join small_items on big_items.id = small_items.id order by 1",
        ),
        vec![
            vec![
                Value::Int32(0),
                Value::Text("big0".into()),
                Value::Int32(0),
                Value::Text("small0".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Text("big1".into()),
                Value::Int32(1),
                Value::Text("small1".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Text("big2".into()),
                Value::Int32(2),
                Value::Text("small2".into()),
            ],
            vec![
                Value::Int32(3),
                Value::Text("big3".into()),
                Value::Int32(3),
                Value::Text("small3".into()),
            ],
        ]
    );
}

#[test]
fn explain_ordered_equijoin_can_choose_merge_join() {
    std::thread::Builder::new()
        .name("explain_ordered_equijoin_can_choose_merge_join".into())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let base = temp_dir("explain_ordered_equijoin_merge_join");
            let db = Database::open(&base, 16).unwrap();
            let mut session = Session::new(1);

            set_large_test_max_stack_depth(&mut session, &db, 32 * 1024);
            session
                .execute(&db, "create table left_items (id int4 not null, note text)")
                .unwrap();
            session
                .execute(
                    &db,
                    "create table right_items (id int4 not null, note text)",
                )
                .unwrap();

            for id in 0..96 {
                session
                    .execute(
                        &db,
                        &format!("insert into left_items values ({id}, 'l{id}')"),
                    )
                    .unwrap();
                session
                    .execute(
                        &db,
                        &format!("insert into right_items values ({id}, 'r{id}')"),
                    )
                    .unwrap();
            }

            session.execute(&db, "analyze left_items").unwrap();
            session.execute(&db, "analyze right_items").unwrap();

            let lines = match session
                .execute(
                    &db,
                    "explain select l.id, r.note from \
                     (select id, note from left_items order by id) l \
                     join (select id, note from right_items order by id) r \
                       on l.id = r.id \
                     order by l.id",
                )
                .unwrap()
            {
                StatementResult::Query { rows, .. } => rows
                    .into_iter()
                    .map(|row| match row.first() {
                        Some(Value::Text(text)) => text.to_string(),
                        other => panic!("expected explain text row, got {:?}", other),
                    })
                    .collect::<Vec<_>>(),
                other => panic!("expected query result, got {:?}", other),
            };
            assert!(
                lines
                    .iter()
                    .any(|line| line.trim_start().starts_with("->  Merge Join  ")),
                "expected ordered equijoin to choose merge join, got {lines:?}"
            );

            assert_eq!(
                session_query_rows(
                    &mut session,
                    &db,
                    "select l.id, r.note from \
                     (select id, note from left_items order by id) l \
                     join (select id, note from right_items order by id) r \
                       on l.id = r.id \
                     where l.id < 3 \
                     order by l.id",
                ),
                vec![
                    vec![Value::Int32(0), Value::Text("r0".into())],
                    vec![Value::Int32(1), Value::Text("r1".into())],
                    vec![Value::Int32(2), Value::Text("r2".into())],
                ],
            );
        })
        .expect("spawn merge join planner test")
        .join()
        .unwrap();
}

#[test]
fn explain_cte_self_join_pushes_single_rel_filter_below_join() {
    let base = temp_dir("explain_cte_self_join_filter_pushdown");
    let db = Database::open(&base, 16).unwrap();

    let lines = explain_lines(
        &db,
        1,
        "with v(x) as (values (0::numeric), (1::numeric), (2::numeric)) select x1, x2 from v as v1(x1), v as v2(x2) where x2 != 0",
    );
    let nested_loop_pos = lines
        .iter()
        .position(|line| line.trim_start().starts_with("Nested Loop  (cost="))
        .unwrap_or_else(|| panic!("expected nested loop explain output, got {lines:?}"));
    let filtered_child_pos = lines
        .iter()
        .position(|line| line.trim_start().starts_with("->  Filter  (cost="))
        .unwrap_or_else(|| panic!("expected filtered child node in explain output, got {lines:?}"));
    let top_level_cte_pos = lines
        .iter()
        .enumerate()
        .skip(filtered_child_pos + 1)
        .find(|(_, line)| line.trim_start().starts_with("->  CTE Scan  (cost="))
        .map(|(idx, _)| idx)
        .unwrap_or_else(|| panic!("expected unfiltered cte scan in explain output, got {lines:?}"));
    let pushed_filter_pos = lines
        .iter()
        .position(|line| line.trim_start().starts_with("Filter: (x2 <>"))
        .unwrap_or_else(|| {
            panic!("expected pushed-down filter detail in explain output, got {lines:?}")
        });

    assert!(
        !lines[nested_loop_pos + 1..filtered_child_pos]
            .iter()
            .any(|line| line.trim_start().starts_with("Filter: (x2 <>")),
        "expected filter to be attached below the join, got {lines:?}"
    );
    assert!(
        nested_loop_pos < filtered_child_pos && filtered_child_pos < top_level_cte_pos,
        "expected filtered child to appear before the unfiltered side, got {lines:?}"
    );
    assert!(
        filtered_child_pos < pushed_filter_pos && pushed_filter_pos < top_level_cte_pos,
        "expected pushed-down filter to appear under the filtered input before the unfiltered side, got {lines:?}"
    );
}

#[test]
fn explain_three_way_inner_join_can_build_smaller_join_first() {
    let base = temp_dir("explain_three_way_inner_join_reorder");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table big_items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "create table medium_items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "create table small_items (id int4 not null, note text)")
        .unwrap();

    for id in 0..64 {
        db.execute(
            1,
            &format!("insert into big_items values ({id}, 'big{id}')"),
        )
        .unwrap();
    }
    for id in 0..16 {
        db.execute(
            1,
            &format!("insert into medium_items values ({id}, 'medium{id}')"),
        )
        .unwrap();
    }
    for id in 0..4 {
        db.execute(
            1,
            &format!("insert into small_items values ({id}, 'small{id}')"),
        )
        .unwrap();
    }

    db.execute(1, "analyze big_items").unwrap();
    db.execute(1, "analyze medium_items").unwrap();
    db.execute(1, "analyze small_items").unwrap();

    let lines = explain_lines(
        &db,
        1,
        "select * from big_items join medium_items on big_items.id = medium_items.id join small_items on medium_items.id = small_items.id",
    );
    let big_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on big_items"))
        .unwrap();
    let medium_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on medium_items"))
        .unwrap();
    let small_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on small_items"))
        .unwrap();
    let join_positions = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| line.contains("Join").then_some(index))
        .collect::<Vec<_>>();
    let smaller_join_is_inner_hash_subtree = join_positions.len() >= 2
        && join_positions[0] < big_pos
        && big_pos < join_positions[1]
        && join_positions[1] < medium_pos
        && join_positions[1] < small_pos;
    assert!(
        (medium_pos < big_pos && small_pos < big_pos) || smaller_join_is_inner_hash_subtree,
        "expected planner to join medium/small before big, got {lines:?}"
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select big_items.id, big_items.note, medium_items.note, small_items.note from big_items join medium_items on big_items.id = medium_items.id join small_items on medium_items.id = small_items.id order by 1",
        ),
        vec![
            vec![
                Value::Int32(0),
                Value::Text("big0".into()),
                Value::Text("medium0".into()),
                Value::Text("small0".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Text("big1".into()),
                Value::Text("medium1".into()),
                Value::Text("small1".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Text("big2".into()),
                Value::Text("medium2".into()),
                Value::Text("small2".into()),
            ],
            vec![
                Value::Int32(3),
                Value::Text("big3".into()),
                Value::Text("medium3".into()),
                Value::Text("small3".into()),
            ],
        ]
    );
}

#[test]
fn cross_join_chain_with_aliases_executes_without_rebinding_panic() {
    let base = temp_dir("cross_join_chain_aliases");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table j1_tbl (i int4 not null, j int4 not null, t text)",
    )
    .unwrap();
    db.execute(1, "create table j2_tbl (i int4 not null, k int4 not null)")
        .unwrap();
    db.execute(1, "insert into j1_tbl values (1, 4, 'one')")
        .unwrap();
    db.execute(1, "insert into j2_tbl values (1, -1)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select * from j1_tbl cross join j2_tbl a cross join j2_tbl b",
        ),
        vec![vec![
            Value::Int32(1),
            Value::Int32(4),
            Value::Text("one".into()),
            Value::Int32(1),
            Value::Int32(-1),
            Value::Int32(1),
            Value::Int32(-1),
        ]]
    );
}

fn setup_join_regress_crash_tables(db: &Database) {
    for sql in [
        "create temp table x (x1 int4, x2 int4)",
        "create temp table y (y1 int4, y2 int4)",
        "insert into x values (1,11), (2,22), (3,null), (4,44), (5,null)",
        "insert into y values (1,111), (2,222), (3,333), (4,null)",
        "create temp table xx (pkxx int4)",
        "create temp table yy (pkyy int4, pkxx int4)",
        "insert into xx values (1), (2), (3)",
        "insert into yy values (101,1), (201,2), (301,null)",
        "create temp table onek (unique1 int4, unique2 int4, hundred int4, ten int4)",
        "insert into onek values (1,1,1,1)",
        "create temp table tenk1 (unique1 int4, unique2 int4, thousand int4, tenthous int4)",
        "insert into tenk1 values (1,1,1,1), (42,1,1,1)",
        "create temp table int4_tbl (f1 int4)",
        "insert into int4_tbl values (1), (42)",
        "create temp table int8_tbl (q1 int8, q2 int8)",
        "insert into int8_tbl values (1,456)",
        "create temp table text_tbl (f1 text)",
        "insert into text_tbl values ('doh!'), ('x')",
    ] {
        db.execute(1, sql).unwrap();
    }
}

#[test]
fn join_regress_outer_join_filter_executes_without_var_rewrite_panic() {
    let base = temp_dir("join_regress_outer_join_filter");
    let db = Database::open(&base, 16).unwrap();

    setup_join_regress_crash_tables(&db);

    let _ = query_rows(
        &db,
        1,
        "select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2) \
         on (x1 = xx1) where (x2 is not null)",
    );
}

#[test]
fn join_regress_outer_join_subquery_alias_executes_without_var_rewrite_panic() {
    let base = temp_dir("join_regress_outer_join_subquery_alias");
    let db = Database::open(&base, 16).unwrap();

    setup_join_regress_crash_tables(&db);

    let _ = query_rows(
        &db,
        1,
        "select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy, \
         xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx \
         from yy \
         left join (select * from yy where pkyy = 101) as yya on yy.pkyy = yya.pkyy \
         left join xx xxa on yya.pkxx = xxa.pkxx \
         left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx",
    );
}

#[test]
fn join_regress_remaining_outer_join_crash_queries_do_not_panic() {
    let base = temp_dir("join_regress_remaining_crash_queries");
    let db = Database::open(&base, 16).unwrap();

    setup_join_regress_crash_tables(&db);

    for sql in [
        "explain (costs off) select * from onek t1 \
         left join onek t2 on t1.unique1 = t2.unique1 \
         left join onek t3 on t2.unique1 != t3.unique1 \
         left join onek t4 on t3.unique1 = t4.unique1",
        "explain (costs off) select * from int4_tbl t1 \
         left join int4_tbl t2 on true \
         left join int4_tbl t3 on t2.f1 = t3.f1 \
         left join int4_tbl t4 on t3.f1 != t4.f1",
        "explain (costs off) select * from int4_tbl t1 \
         left join ((select t2.f1 from int4_tbl t2 left join int4_tbl t3 on t2.f1 > 0 where t3.f1 is null) s \
                    left join tenk1 t4 on s.f1 > 1) \
         on s.f1 = t1.f1",
        "explain (costs off) select * from int4_tbl t1 \
         left join ((select t2.f1 from int4_tbl t2 left join int4_tbl t3 on t2.f1 > 0 where t2.f1 <> coalesce(t3.f1, -1)) s \
                    left join tenk1 t4 on s.f1 > 1) \
         on s.f1 = t1.f1",
        "select * from (select 1 as key1) sub1 \
         left join (select sub3.key3, sub4.value2, coalesce(sub4.value2, 66) as value3 \
                    from (select 1 as key3) sub3 \
                    left join (select sub5.key5, coalesce(sub6.value1, 1) as value2 \
                               from (select 1 as key5) sub5 \
                               left join (select 2 as key6, 42 as value1) sub6 \
                               on sub5.key5 = sub6.key6) sub4 \
                    on sub4.key5 = sub3.key3) sub2 \
         on sub1.key1 = sub2.key3",
        "select * from (select 1 as key1) sub1 \
         left join (select sub3.key3, value2, coalesce(value2, 66) as value3 \
                    from (select 1 as key3) sub3 \
                    left join (select sub5.key5, coalesce(sub6.value1, 1) as value2 \
                               from (select 1 as key5) sub5 \
                               left join (select 2 as key6, 42 as value1) sub6 \
                               on sub5.key5 = sub6.key6) sub4 \
                    on sub4.key5 = sub3.key3) sub2 \
         on sub1.key1 = sub2.key3",
        "explain (costs off) select b.unique1 from \
         tenk1 a join tenk1 b on a.unique1 = b.unique2 \
         left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand \
         join int4_tbl i1 on b.thousand = f1 \
         right join int4_tbl i2 on i2.f1 = b.tenthous \
         order by 1",
        "select b.unique1 from \
         tenk1 a join tenk1 b on a.unique1 = b.unique2 \
         left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand \
         join int4_tbl i1 on b.thousand = f1 \
         right join int4_tbl i2 on i2.f1 = b.tenthous \
         order by 1",
        "explain (verbose, costs off) select * from \
         text_tbl t1 \
         inner join int8_tbl i8 on i8.q2 = 456 \
         right join text_tbl t2 on t1.f1 = 'doh!' \
         left join int4_tbl i4 on i8.q1 = i4.f1",
    ] {
        db.execute(1, sql).unwrap();
    }
}

#[test]
fn explain_join_order_by_can_reuse_ordered_outer_path() {
    let base = temp_dir("explain_join_ordered_outer_path");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table big_items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "create table small_items (id int4 not null)")
        .unwrap();

    for id in 0..400 {
        db.execute(
            1,
            &format!("insert into big_items values ({}, 'big{id}')", id % 4),
        )
        .unwrap();
    }
    for id in 0..4 {
        db.execute(1, &format!("insert into small_items values ({id})"))
            .unwrap();
    }

    db.execute(1, "analyze big_items").unwrap();
    db.execute(1, "analyze small_items").unwrap();

    let lines = explain_lines(
        &db,
        1,
        "select small_items.id \
         from big_items join small_items on big_items.id = small_items.id \
         order by small_items.id limit 5",
    );
    let sort_positions = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim_start();
            (trimmed.starts_with("Sort  ") || trimmed.starts_with("->  Sort  ")).then_some(index)
        })
        .collect::<Vec<_>>();
    assert!(
        sort_positions.len() == 1,
        "expected planner to produce a single-sort plan for ORDER BY/LIMIT join queries, got {lines:?}"
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select small_items.id \
             from big_items join small_items on big_items.id = small_items.id \
             order by small_items.id limit 5",
        ),
        vec![
            vec![Value::Int32(0)],
            vec![Value::Int32(0)],
            vec![Value::Int32(0)],
            vec![Value::Int32(0)],
            vec![Value::Int32(0)],
        ]
    );
}

#[test]
fn select_list_srf_order_by_limit_is_sorted_before_project_set() {
    let base = temp_dir("project_set_order_by_limit");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table items (unique1 int4 not null, unique2 int4 not null, tenthous int4 not null)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into items values (2, 20, 200), (1, 10, 100), (3, 30, 300)",
    )
    .unwrap();

    let lines = explain_lines(
        &db,
        1,
        "select unique1, unique2, generate_series(1, 10) \
         from items order by tenthous limit 7",
    );
    let project_set_idx = lines
        .iter()
        .position(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("ProjectSet") || trimmed.starts_with("->  ProjectSet")
        })
        .expect("expected ProjectSet in explain output");
    let sort_idx = lines
        .iter()
        .position(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("Sort  ") || trimmed.starts_with("->  Sort  ")
        })
        .expect("expected Sort in explain output");
    assert!(
        project_set_idx < sort_idx,
        "expected planner to postpone ProjectSet until after Sort, got {lines:?}"
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select unique1, unique2, generate_series(1, 10) \
             from items order by tenthous limit 7",
        ),
        vec![
            vec![Value::Int32(1), Value::Int32(10), Value::Int32(1)],
            vec![Value::Int32(1), Value::Int32(10), Value::Int32(2)],
            vec![Value::Int32(1), Value::Int32(10), Value::Int32(3)],
            vec![Value::Int32(1), Value::Int32(10), Value::Int32(4)],
            vec![Value::Int32(1), Value::Int32(10), Value::Int32(5)],
            vec![Value::Int32(1), Value::Int32(10), Value::Int32(6)],
            vec![Value::Int32(1), Value::Int32(10), Value::Int32(7)],
        ]
    );
}

#[test]
fn left_join_rhs_boundary_stays_legal() {
    let base = temp_dir("left_join_rhs_boundary");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table a (id int4 not null)").unwrap();
    db.execute(1, "create table b (id int4 not null)").unwrap();
    db.execute(1, "create table c (id int4 not null)").unwrap();

    db.execute(1, "insert into a values (1), (2)").unwrap();
    db.execute(1, "insert into b values (1), (2)").unwrap();
    db.execute(1, "insert into c values (1)").unwrap();

    db.execute(1, "analyze a").unwrap();
    db.execute(1, "analyze b").unwrap();
    db.execute(1, "analyze c").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select a.id, b.id, c.id \
             from a left join (b join c on b.id = c.id) on a.id = b.id \
             order by 1, 2, 3",
        ),
        vec![
            vec![Value::Int32(1), Value::Int32(1), Value::Int32(1)],
            vec![Value::Int32(2), Value::Null, Value::Null],
        ]
    );
}

#[test]
fn explain_left_join_can_reassociate_strict_rhs() {
    let base = temp_dir("left_join_reassociate_strict_rhs");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table a (id int4 not null)").unwrap();
    db.execute(1, "create table b (id int4 not null)").unwrap();
    db.execute(1, "create table c (id int4 not null)").unwrap();

    for id in 0..16 {
        db.execute(1, &format!("insert into a values ({id})"))
            .unwrap();
    }
    for id in 0..4 {
        db.execute(1, &format!("insert into b values ({id})"))
            .unwrap();
    }
    for id in 0..64 {
        db.execute(1, &format!("insert into c values ({id})"))
            .unwrap();
    }

    db.execute(1, "analyze a").unwrap();
    db.execute(1, "analyze b").unwrap();
    db.execute(1, "analyze c").unwrap();

    let lines = explain_lines(
        &db,
        1,
        "select a.id, b.id, c.id \
         from a left join (b left join c on b.id = c.id) on a.id = b.id",
    );
    let a_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on a"))
        .unwrap();
    let b_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on b"))
        .unwrap();
    let c_pos = lines
        .iter()
        .position(|line| line.contains("Seq Scan on c"))
        .unwrap();
    let ab_join_pos = lines.iter().position(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with("Hash Left Join  ") || trimmed.starts_with("Nested Loop Left Join  ")
    });
    assert!(
        (a_pos < c_pos && b_pos < c_pos)
            || ab_join_pos.is_some_and(|ab_join_pos| {
                c_pos < ab_join_pos && ab_join_pos < a_pos && ab_join_pos < b_pos
            }),
        "expected planner to build the a/b left-join subtree before combining it with c when LEFT JOIN identity 3 is legal, got {lines:?}"
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select a.id, b.id, c.id \
             from a left join (b left join c on b.id = c.id) on a.id = b.id \
             order by 1, 2, 3",
        ),
        vec![
            vec![Value::Int32(0), Value::Int32(0), Value::Int32(0)],
            vec![Value::Int32(1), Value::Int32(1), Value::Int32(1)],
            vec![Value::Int32(2), Value::Int32(2), Value::Int32(2)],
            vec![Value::Int32(3), Value::Int32(3), Value::Int32(3)],
            vec![Value::Int32(4), Value::Null, Value::Null],
            vec![Value::Int32(5), Value::Null, Value::Null],
            vec![Value::Int32(6), Value::Null, Value::Null],
            vec![Value::Int32(7), Value::Null, Value::Null],
            vec![Value::Int32(8), Value::Null, Value::Null],
            vec![Value::Int32(9), Value::Null, Value::Null],
            vec![Value::Int32(10), Value::Null, Value::Null],
            vec![Value::Int32(11), Value::Null, Value::Null],
            vec![Value::Int32(12), Value::Null, Value::Null],
            vec![Value::Int32(13), Value::Null, Value::Null],
            vec![Value::Int32(14), Value::Null, Value::Null],
            vec![Value::Int32(15), Value::Null, Value::Null],
        ]
    );
}

#[test]
fn create_index_builds_multilevel_btree_root() {
    let base = temp_dir("btree_multilevel_root");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, &insert_items_sql(0..1500, "row")).unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    let rel = relation_locator_for(&db, 1, "items_id_idx");
    let meta_page = read_relation_block(&db, rel, 0);
    let meta = crate::include::access::nbtree::bt_page_get_meta(&meta_page).unwrap();
    assert!(meta.btm_level > 0, "expected multilevel root, got {meta:?}");
    assert!(
        meta.btm_root > 1,
        "expected root above leaf block 1, got {meta:?}"
    );

    let root_page = read_relation_block(&db, rel, meta.btm_root);
    let root_opaque = crate::include::access::nbtree::bt_page_get_opaque(&root_page).unwrap();
    assert!(root_opaque.is_root());
    assert!(
        !root_opaque.is_leaf(),
        "expected internal root, got {root_opaque:?}"
    );

    assert_explain_uses_index(
        &db,
        1,
        "select note from items where id = 1499",
        "items_id_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 1499"),
        vec![vec![Value::Text("row1499".into())]]
    );
}

#[test]
fn create_unique_index_rejects_duplicate_live_keys() {
    let base = temp_dir("create_unique_index_rejects_duplicates");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'a'), (1, 'b')")
        .unwrap();

    match db.execute(1, "create unique index items_id_key on items (id)") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_id_key");
        }
        other => panic!("expected unique violation, got {:?}", other),
    }
}

#[test]
fn create_unique_index_allows_multiple_nulls() {
    let base = temp_dir("create_unique_index_allows_nulls");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (null, 'a'), (null, 'b')")
        .unwrap();
    db.execute(1, "create unique index items_id_key on items (id)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(2)]]
    );
}

#[test]
fn create_table_primary_key_and_unique_constraints_are_enforced_and_persisted() {
    let base = temp_dir("create_table_primary_key_unique");
    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(16)).unwrap();

    db.execute(
        1,
        "create table items (id int4 primary key, code int4 unique)",
    )
    .unwrap();

    match db.execute(1, "insert into items values (null, 10)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "id" && constraint == "items_id_not_null" => {}
        other => panic!("expected primary-key NOT NULL rejection, got {other:?}"),
    }

    db.execute(1, "insert into items values (1, 10)").unwrap();
    db.execute(1, "insert into items values (2, null)").unwrap();
    db.execute(1, "insert into items values (3, null)").unwrap();

    match db.execute(1, "insert into items values (1, 11)") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_pkey");
        }
        other => panic!("expected primary-key duplicate rejection, got {other:?}"),
    }

    match db.execute(1, "insert into items values (4, 10)") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_code_key");
        }
        other => panic!("expected unique duplicate rejection, got {other:?}"),
    }

    let constraint_rows = query_rows(
        &db,
        1,
        "select conname, contype, conindid \
         from pg_constraint \
         where conrelid = (select oid from pg_class where relname = 'items') \
         order by conname",
    );
    assert_eq!(constraint_rows.len(), 3);
    assert_eq!(constraint_rows[0][0], Value::Text("items_code_key".into()));
    assert_eq!(constraint_rows[0][1], Value::Text("u".into()));
    assert!(int_value(&constraint_rows[0][2]) > 0);
    assert_eq!(
        constraint_rows[1][0],
        Value::Text("items_id_not_null".into())
    );
    assert_eq!(constraint_rows[1][1], Value::Text("n".into()));
    assert_eq!(int_value(&constraint_rows[1][2]), 0);
    assert_eq!(constraint_rows[2][0], Value::Text("items_pkey".into()));
    assert_eq!(constraint_rows[2][1], Value::Text("p".into()));
    assert!(int_value(&constraint_rows[2][2]) > 0);

    assert_eq!(
        query_rows(
            &db,
            1,
            "select c.relname, i.indisprimary \
             from pg_index i \
             join pg_class c on c.oid = i.indexrelid \
             where i.indrelid = (select oid from pg_class where relname = 'items') \
             order by c.relname",
        ),
        vec![
            vec![Value::Text("items_code_key".into()), Value::Bool(false)],
            vec![Value::Text("items_pkey".into()), Value::Bool(true)],
        ]
    );

    drop(db);
    let reopened = Database::open(&base, 16).unwrap();
    assert_eq!(
        query_rows(
            &reopened,
            1,
            "select conname, contype \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items') \
             order by conname",
        ),
        vec![
            vec![
                Value::Text("items_code_key".into()),
                Value::Text("u".into())
            ],
            vec![
                Value::Text("items_id_not_null".into()),
                Value::Text("n".into())
            ],
            vec![Value::Text("items_pkey".into()), Value::Text("p".into())],
        ]
    );
}

#[test]
fn create_table_like_including_indexes_copies_deferrable_key_flags() {
    let base = temp_dir("create_table_like_deferrable_keys");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table src_items (
            id int4 primary key deferrable initially deferred,
            code int4,
            constraint src_items_code_key unique (code) deferrable
        )",
    )
    .unwrap();
    db.execute(
        1,
        "create table cloned_items (like src_items including indexes)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select con.contype, con.condeferrable, con.condeferred, i.indimmediate, pg_get_constraintdef(con.oid) \
               from pg_constraint con \
               join pg_index i on i.indexrelid = con.conindid \
              where con.conrelid = (select oid from pg_class where relname = 'cloned_items') \
                and con.contype in ('p', 'u') \
              order by con.contype",
        ),
        vec![
            vec![
                Value::Text("p".into()),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(false),
                Value::Text("PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED".into()),
            ],
            vec![
                Value::Text("u".into()),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::Text("UNIQUE (code) DEFERRABLE".into()),
            ],
        ]
    );
}

#[test]
fn create_table_table_level_primary_key_and_unique_constraints_work() {
    let base = temp_dir("create_table_composite_constraints");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table memberships (id int4, tag int4, note int4, primary key (id, tag), unique (tag, note))",
    )
    .unwrap();
    db.execute(1, "insert into memberships values (1, 10, 100)")
        .unwrap();
    db.execute(1, "insert into memberships values (2, 10, null)")
        .unwrap();
    db.execute(1, "insert into memberships values (3, 10, null)")
        .unwrap();

    match db.execute(1, "insert into memberships values (1, 10, 101)") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "memberships_pkey");
        }
        other => panic!("expected composite primary-key rejection, got {other:?}"),
    }

    match db.execute(1, "insert into memberships values (4, 10, 100)") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "memberships_tag_note_key");
        }
        other => panic!("expected composite unique rejection, got {other:?}"),
    }

    match db.execute(1, "insert into memberships values (5, null, 102)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "memberships"
            && column == "tag"
            && constraint == "memberships_tag_not_null" => {}
        other => panic!("expected primary-key column NOT NULL rejection, got {other:?}"),
    }
}

#[test]
fn create_table_check_and_named_not_null_constraints_are_enforced_and_persisted() {
    let base = temp_dir("create_table_check_constraints");
    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(16)).unwrap();

    db.execute(
        1,
        "create table items (
            id int4 constraint items_id_positive check (id > 0),
            note text constraint items_note_required not null,
            constraint items_note_nonempty check (note <> '')
        )",
    )
    .unwrap();

    match db.execute(1, "insert into items values (0, 'hello')") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_positive" => {}
        other => panic!("expected check violation, got {other:?}"),
    }

    match db.execute(1, "insert into items values (1, null)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected named not-null violation, got {other:?}"),
    }

    match db.execute(1, "insert into items values (1, '')") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_note_nonempty" => {}
        other => panic!("expected second check violation, got {other:?}"),
    }

    db.execute(1, "insert into items values (null, 'nullable id')")
        .unwrap();
    db.execute(1, "insert into items values (2, 'ok')").unwrap();
    db.execute(1, "insert into items values (3, 'fine')")
        .unwrap();

    let rows = query_rows(
        &db,
        1,
        "select conname, contype, convalidated, conbin \
         from pg_constraint \
         where conrelid = (select oid from pg_class where relname = 'items') \
         order by conname",
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("items_id_positive".into()),
                Value::Text("c".into()),
                Value::Bool(true),
                Value::Text("id > 0".into()),
            ],
            vec![
                Value::Text("items_note_nonempty".into()),
                Value::Text("c".into()),
                Value::Bool(true),
                Value::Text("note <> ''".into()),
            ],
            vec![
                Value::Text("items_note_required".into()),
                Value::Text("n".into()),
                Value::Bool(true),
                Value::Null,
            ],
        ]
    );

    drop(db);
    let reopened = Database::open(&base, 16).unwrap();
    match reopened.execute(1, "insert into items values (4, null)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected reopened named not-null violation, got {other:?}"),
    }
    match reopened.execute(1, "insert into items values (0, 'after reopen')") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_positive" => {}
        other => panic!("expected reopened check violation, got {other:?}"),
    }
    reopened
        .execute(1, "insert into items values (null, 'still nullable')")
        .unwrap();
}

#[test]
fn create_table_foreign_keys_are_enforced_and_persisted() {
    let base = temp_dir("create_table_foreign_keys");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table parents (id int4 primary key, code int4 unique)",
    )
    .unwrap();
    db.execute(
        1,
        "create table children (
            id int4 primary key,
            parent_id int4 references parents,
            parent_code int4,
            constraint children_parent_code_fkey foreign key (parent_code) references parents(code) on delete restrict
        )",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1, 10), (2, 20)")
        .unwrap();
    db.execute(1, "insert into children values (1, 1, 10)")
        .unwrap();
    db.execute(1, "insert into children values (2, null, null)")
        .unwrap();

    match db.execute(1, "insert into children values (3, 3, 10)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected foreign-key violation, got {other:?}"),
    }

    match db.execute(1, "insert into children values (3, 1, 30)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_code_fkey");
        }
        other => panic!("expected second foreign-key violation, got {other:?}"),
    }

    let rows = query_rows(
        &db,
        1,
        "select conname, contype, convalidated, confupdtype, confdeltype, confmatchtype, conindid \
         from pg_constraint \
         where conrelid = (select oid from pg_class where relname = 'children') \
           and contype = 'f' \
         order by conname",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Text("children_parent_code_fkey".into()));
    assert_eq!(rows[0][1], Value::Text("f".into()));
    assert_eq!(rows[0][2], Value::Bool(true));
    assert_eq!(rows[0][3], Value::Text("a".into()));
    assert_eq!(rows[0][4], Value::Text("r".into()));
    assert_eq!(rows[0][5], Value::Text("s".into()));
    assert!(int_value(&rows[0][6]) > 0);
    assert_eq!(rows[1][0], Value::Text("children_parent_id_fkey".into()));
    assert_eq!(rows[1][1], Value::Text("f".into()));
    assert_eq!(rows[1][2], Value::Bool(true));
    assert_eq!(rows[1][3], Value::Text("a".into()));
    assert_eq!(rows[1][4], Value::Text("a".into()));
    assert_eq!(rows[1][5], Value::Text("s".into()));
    assert!(int_value(&rows[1][6]) > 0);
}

#[test]
fn foreign_keys_support_match_full() {
    let base = temp_dir("foreign_keys_match_full");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table parents (id int4, code text, primary key (id, code))",
    )
    .unwrap();
    db.execute(
        1,
        "create table children (
            id int4 primary key,
            parent_id int4,
            parent_code text,
            constraint children_parent_fk
                foreign key (parent_id, parent_code) references parents(id, code) match full
        )",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1, 'one')")
        .unwrap();
    db.execute(1, "insert into children values (1, 1, 'one')")
        .unwrap();
    db.execute(1, "insert into children values (2, null, null)")
        .unwrap();

    match db.execute(1, "insert into children values (3, 1, null)") {
        Err(ExecError::ForeignKeyViolation {
            constraint, detail, ..
        }) => {
            assert_eq!(constraint, "children_parent_fk");
            assert!(
                detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("MATCH FULL"))
            );
        }
        other => panic!("expected MATCH FULL foreign-key violation, got {other:?}"),
    }
}

#[test]
fn alter_table_add_foreign_key_supports_match_full() {
    let base = temp_dir("alter_table_add_fk_match_full");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table parents (id int4, code text, primary key (id, code))",
    )
    .unwrap();
    db.execute(
        1,
        "create table children (
            id int4 primary key,
            parent_id int4,
            parent_code text
        )",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1, 'one')")
        .unwrap();
    db.execute(
        1,
        "insert into children values (1, 1, 'one'), (2, null, null), (3, 1, null)",
    )
    .unwrap();

    match db.execute(
        1,
        "alter table children add constraint children_parent_fk foreign key (parent_id, parent_code) references parents(id, code) match full",
    ) {
        Err(ExecError::ForeignKeyViolation {
            constraint, detail, ..
        }) => {
            assert_eq!(constraint, "children_parent_fk");
            assert!(
                detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("MATCH FULL"))
            );
        }
        other => panic!("expected ALTER TABLE MATCH FULL validation failure, got {other:?}"),
    }

    db.execute(1, "update children set parent_id = null where id = 3")
        .unwrap();
    db.execute(
        1,
        "alter table children add constraint children_parent_fk foreign key (parent_id, parent_code) references parents(id, code) match full",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select confmatchtype, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![Value::Text("f".into()), Value::Bool(true)]]
    );

    match db.execute(1, "insert into children values (4, 1, null)") {
        Err(ExecError::ForeignKeyViolation {
            constraint, detail, ..
        }) => {
            assert_eq!(constraint, "children_parent_fk");
            assert!(
                detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("MATCH FULL"))
            );
        }
        other => panic!("expected post-add MATCH FULL foreign-key violation, got {other:?}"),
    }
}

#[test]
fn foreign_keys_apply_referential_actions() {
    let base = temp_dir("foreign_keys_referential_actions");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(1, "insert into parents values (0), (1)")
        .unwrap();
    db.execute(
        1,
        "create table cascade_children (
            id int4 primary key,
            parent_id int4 references parents(id) on update cascade on delete cascade
        )",
    )
    .unwrap();
    db.execute(
        1,
        "create table set_null_children (
            id int4 primary key,
            parent_id int4 references parents(id) on update set null on delete set null
        )",
    )
    .unwrap();
    db.execute(
        1,
        "create table set_default_update_children (
            id int4 primary key,
            parent_id int4 default 0 references parents(id) on update set default
        )",
    )
    .unwrap();
    db.execute(
        1,
        "create table set_default_delete_children (
            id int4 primary key,
            parent_id int4 default 0 references parents(id) on delete set default
        )",
    )
    .unwrap();

    db.execute(1, "insert into cascade_children values (1, 1)")
        .unwrap();
    db.execute(1, "insert into set_null_children values (1, 1)")
        .unwrap();
    db.execute(1, "insert into set_default_update_children values (1, 1)")
        .unwrap();

    db.execute(1, "update parents set id = 2 where id = 1")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select parent_id from cascade_children"),
        vec![vec![Value::Int32(2)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select parent_id from set_null_children"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        query_rows(&db, 1, "select parent_id from set_default_update_children"),
        vec![vec![Value::Int32(0)]]
    );

    db.execute(1, "insert into set_default_delete_children values (1, 2)")
        .unwrap();
    db.execute(1, "delete from parents where id = 2").unwrap();

    assert!(query_rows(&db, 1, "select * from cascade_children").is_empty());
    assert_eq!(
        query_rows(&db, 1, "select parent_id from set_null_children"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        query_rows(&db, 1, "select parent_id from set_default_update_children"),
        vec![vec![Value::Int32(0)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select parent_id from set_default_delete_children"),
        vec![vec![Value::Int32(0)]]
    );
}

#[test]
fn foreign_keys_on_update_set_default_rejects_missing_default_key() {
    let base = temp_dir("foreign_keys_update_set_default_missing_key");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table pktable (
            ptest1 int4,
            ptest2 int4,
            ptest3 int4,
            ptest4 text,
            primary key (ptest1, ptest2, ptest3)
        )",
    )
    .unwrap();
    db.execute(
        1,
        "create table fktable (
            ftest1 int4 default 0,
            ftest2 int4 default -1,
            ftest3 int4 default -2,
            ftest4 int4,
            constraint constrname3 foreign key (ftest1, ftest2, ftest3)
                references pktable
                on delete set null
                on update set default
        )",
    )
    .unwrap();

    db.execute(
        1,
        "insert into pktable values
            (1, 2, 3, 'test1'),
            (1, 3, 3, 'test2'),
            (2, 3, 4, 'test3'),
            (2, 4, 5, 'test4'),
            (2, -1, 5, 'test5')",
    )
    .unwrap();
    db.execute(
        1,
        "insert into fktable values
            (1, 2, 3, 1),
            (2, 3, 4, 1),
            (2, 4, 5, 1),
            (null, 2, 3, 2),
            (2, null, 3, 3),
            (null, 2, 7, 4),
            (null, 3, 4, 5)",
    )
    .unwrap();

    match db.execute(1, "update pktable set ptest2 = 5 where ptest2 = 2") {
        Err(ExecError::ForeignKeyViolation {
            constraint, detail, ..
        }) => {
            assert_eq!(constraint, "constrname3");
            assert!(detail.as_deref().is_some_and(|detail| {
                detail.contains("Key (ftest1, ftest2, ftest3)=(0, -1, -2)")
            }));
        }
        other => panic!("expected SET DEFAULT update violation, got {other:?}"),
    }

    assert_eq!(
        query_rows(
            &db,
            1,
            "select ptest1, ptest2, ptest3, ptest4 from pktable order by ptest4",
        ),
        vec![
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Text("test1".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Int32(3),
                Value::Int32(3),
                Value::Text("test2".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
                Value::Text("test3".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(4),
                Value::Int32(5),
                Value::Text("test4".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(-1),
                Value::Int32(5),
                Value::Text("test5".into()),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select ftest1, ftest2, ftest3, ftest4 from fktable order by ftest4, ftest1, ftest2, ftest3",
        ),
        vec![
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(1),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(1),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(4),
                Value::Int32(5),
                Value::Int32(1),
            ],
            vec![
                Value::Null,
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(2)
            ],
            vec![
                Value::Int32(2),
                Value::Null,
                Value::Int32(3),
                Value::Int32(3)
            ],
            vec![
                Value::Null,
                Value::Int32(2),
                Value::Int32(7),
                Value::Int32(4)
            ],
            vec![
                Value::Null,
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(5)
            ],
        ]
    );

    db.execute(
        1,
        "update pktable
            set ptest1 = 0, ptest2 = -1, ptest3 = -2
            where ptest2 = 2",
    )
    .unwrap();
    db.execute(1, "update pktable set ptest2 = 10 where ptest2 = 4")
        .unwrap();
    db.execute(
        1,
        "update pktable set ptest2 = 2 where ptest2 = 3 and ptest1 = 1",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select ptest1, ptest2, ptest3, ptest4 from pktable order by ptest4",
        ),
        vec![
            vec![
                Value::Int32(0),
                Value::Int32(-1),
                Value::Int32(-2),
                Value::Text("test1".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Text("test2".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
                Value::Text("test3".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(10),
                Value::Int32(5),
                Value::Text("test4".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(-1),
                Value::Int32(5),
                Value::Text("test5".into()),
            ],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select ftest1, ftest2, ftest3, ftest4 from fktable order by ftest4, ftest1, ftest2, ftest3",
        ),
        vec![
            vec![
                Value::Int32(0),
                Value::Int32(-1),
                Value::Int32(-2),
                Value::Int32(1),
            ],
            vec![
                Value::Int32(0),
                Value::Int32(-1),
                Value::Int32(-2),
                Value::Int32(1),
            ],
            vec![
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(1),
            ],
            vec![
                Value::Null,
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(2)
            ],
            vec![
                Value::Int32(2),
                Value::Null,
                Value::Int32(3),
                Value::Int32(3)
            ],
            vec![
                Value::Null,
                Value::Int32(2),
                Value::Int32(7),
                Value::Int32(4)
            ],
            vec![
                Value::Null,
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(5)
            ],
        ]
    );
}

#[test]
fn foreign_keys_accept_supported_cross_type_columns() {
    let db = Database::open_ephemeral(16).unwrap();

    db.execute(1, "create table int_parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table int_children (id int4 primary key, parent_id int8 references int_parents)",
    )
    .unwrap();
    db.execute(1, "insert into int_parents values (1)").unwrap();
    db.execute(1, "insert into int_children values (1, 1)")
        .unwrap();
    assert!(matches!(
        db.execute(1, "insert into int_children values (2, 2)"),
        Err(ExecError::ForeignKeyViolation { .. })
    ));
    assert!(matches!(
        db.execute(1, "delete from int_parents where id = 1"),
        Err(ExecError::ForeignKeyViolation { .. })
    ));

    db.execute(1, "create table numeric_parents (id numeric primary key)")
        .unwrap();
    db.execute(
        1,
        "create table numeric_children (id int4 primary key, parent_id int4 references numeric_parents)",
    )
    .unwrap();
    db.execute(1, "insert into numeric_parents values (10)")
        .unwrap();
    db.execute(1, "insert into numeric_children values (1, 10)")
        .unwrap();
    assert!(matches!(
        db.execute(1, "insert into numeric_children values (2, 11)"),
        Err(ExecError::ForeignKeyViolation { .. })
    ));
}

#[test]
fn foreign_keys_reject_unsupported_cross_type_columns() {
    let db = Database::open_ephemeral(16).unwrap();

    db.execute(1, "create table int_parents (id int4 primary key)")
        .unwrap();
    assert!(matches!(
        db.execute(1, "create table numeric_children (parent_id numeric references int_parents)"),
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature)))
            if feature == "FOREIGN KEY with cross-type columns"
    ));
}

#[test]
fn foreign_keys_set_default_rechecks_existing_default_reference() {
    let base = temp_dir("foreign_keys_set_default_recheck");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create temp table defp (f1 int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create temp table defc (f1 int4 default 0 references defp on delete set default)",
    )
    .unwrap();
    db.execute(1, "insert into defp values (0), (1), (2)")
        .unwrap();
    db.execute(1, "insert into defc values (2)").unwrap();

    db.execute(1, "delete from defp where f1 = 2").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select f1 from defc"),
        vec![vec![Value::Int32(0)]]
    );

    match db.execute(1, "delete from defp where f1 = 0") {
        Err(ExecError::ForeignKeyViolation {
            constraint, detail, ..
        }) => {
            assert_eq!(constraint, "defc_f1_fkey");
            assert!(
                detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("Key (f1)=(0)"))
            );
        }
        other => panic!("expected delete-default recheck violation, got {other:?}"),
    }
    assert_eq!(
        query_rows(&db, 1, "select f1 from defc"),
        vec![vec![Value::Int32(0)]]
    );

    db.execute(1, "alter table defc alter column f1 set default 1")
        .unwrap();
    db.execute(1, "delete from defp where f1 = 0").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select f1 from defc"),
        vec![vec![Value::Int32(1)]]
    );

    match db.execute(1, "delete from defp where f1 = 1") {
        Err(ExecError::ForeignKeyViolation {
            constraint, detail, ..
        }) => {
            assert_eq!(constraint, "defc_f1_fkey");
            assert!(
                detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("Key (f1)=(1)"))
            );
        }
        other => panic!("expected updated-default recheck violation, got {other:?}"),
    }
    assert_eq!(
        query_rows(&db, 1, "select f1 from defc"),
        vec![vec![Value::Int32(1)]]
    );
}

#[test]
fn alter_table_add_foreign_key_supports_delete_set_column_lists() {
    let base = temp_dir("alter_table_add_fk_delete_set_columns");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table parents (a int4, b int4, primary key (a, b))",
    )
    .unwrap();
    db.execute(1, "insert into parents values (10, 20), (500, 20)")
        .unwrap();

    db.execute(
        1,
        "create table children_set_null (id int4 primary key, a int4, b int4)",
    )
    .unwrap();
    db.execute(1, "insert into children_set_null values (1, 10, 20)")
        .unwrap();
    db.execute(
        1,
        "alter table children_set_null add foreign key (a, b) references parents(a, b) on delete set null (a)",
    )
    .unwrap();

    db.execute(1, "delete from parents where a = 10 and b = 20")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select a, b from children_set_null",),
        vec![vec![Value::Null, Value::Int32(20)]]
    );

    db.execute(1, "insert into parents values (10, 20)")
        .unwrap();
    db.execute(
        1,
        "create table children_set_default (id int4 primary key, a int4 default 500, b int4)",
    )
    .unwrap();
    db.execute(1, "insert into children_set_default values (1, 10, 20)")
        .unwrap();
    db.execute(
        1,
        "alter table children_set_default add foreign key (a, b) references parents(a, b) on delete set default (a)",
    )
    .unwrap();

    db.execute(1, "delete from parents where a = 10 and b = 20")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select a, b from children_set_default",),
        vec![vec![Value::Int32(500), Value::Int32(20)]]
    );
}

#[test]
fn create_table_serial_creates_sequence_defaults_and_persists_state() {
    let base = temp_dir("create_table_serial_defaults");
    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(16)).unwrap();

    db.execute(1, "create table items (id serial, note text)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select pg_get_serial_sequence('items', 'id')"),
        vec![vec![Value::Text("items_id_seq".into())]]
    );

    db.execute(1, "insert into items (note) values ('a'), ('b')")
        .unwrap();
    db.execute(1, "insert into items values (10, 'manual')")
        .unwrap();
    db.execute(1, "insert into items (note) values ('c')")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("a".into())],
            vec![Value::Int32(2), Value::Text("b".into())],
            vec![Value::Int32(3), Value::Text("c".into())],
            vec![Value::Int32(10), Value::Text("manual".into())],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select last_value, log_cnt, is_called from items_id_seq"
        ),
        vec![vec![Value::Int64(3), Value::Int64(0), Value::Bool(true)]]
    );

    drop(db);
    let reopened = Database::open(&base, 16).unwrap();
    assert_eq!(
        query_rows(
            &reopened,
            1,
            "select last_value, log_cnt, is_called from items_id_seq",
        ),
        vec![vec![Value::Int64(3), Value::Int64(0), Value::Bool(true)]]
    );
}

#[test]
fn create_table_serial_is_visible_inside_same_transaction_before_commit() {
    let base = temp_dir("txn_create_table_serial_visibility");
    let db = Database::open(&base, 64).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    writer.execute(&db, "begin").unwrap();
    writer
        .execute(&db, "create table tx_serial (id serial, note text)")
        .unwrap();
    writer
        .execute(&db, "insert into tx_serial (note) values ('a'), ('b')")
        .unwrap();

    match writer
        .execute(&db, "select id, note from tx_serial order by id")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1), Value::Text("a".into())],
                    vec![Value::Int32(2), Value::Text("b".into())],
                ]
            );
        }
        other => panic!("expected query, got {:?}", other),
    }
    match writer
        .execute(
            &db,
            "select last_value, log_cnt, is_called from tx_serial_id_seq",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(2), Value::Int64(0), Value::Bool(true)]]
            );
        }
        other => panic!("expected query, got {:?}", other),
    }

    assert!(
        reader
            .execute(&db, "select count(*) from tx_serial")
            .is_err(),
        "other sessions must not see uncommitted serial-backed tables"
    );
    assert!(
        reader
            .execute(&db, "select last_value from tx_serial_id_seq")
            .is_err(),
        "other sessions must not see the implicit sequence before commit"
    );

    writer.execute(&db, "commit").unwrap();

    match reader
        .execute(&db, "select count(*) from tx_serial")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query, got {:?}", other),
    }
    match reader
        .execute(
            &db,
            "select last_value, log_cnt, is_called from tx_serial_id_seq",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(2), Value::Int64(0), Value::Bool(true)]]
            );
        }
        other => panic!("expected query, got {:?}", other),
    }
}

#[test]
fn create_sequence_supports_functions_and_sequence_scans() {
    let base = temp_dir("create_sequence_functions");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create sequence seq start with 5 increment by 2")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select last_value, log_cnt, is_called from seq"),
        vec![vec![Value::Int64(5), Value::Int64(0), Value::Bool(false)]]
    );

    match db.execute(1, "select currval('seq')") {
        Err(ExecError::DetailedError { sqlstate, .. }) => assert_eq!(sqlstate, "55000"),
        other => panic!("expected currval failure before nextval, got {:?}", other),
    }

    assert_eq!(
        query_rows(&db, 1, "select nextval('seq'), currval('seq')"),
        vec![vec![Value::Int64(5), Value::Int64(5)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select nextval('seq')"),
        vec![vec![Value::Int64(7)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select setval('seq', 20, false)"),
        vec![vec![Value::Int64(20)]]
    );
    match db.execute(1, "select currval('seq')") {
        Err(ExecError::DetailedError { sqlstate, .. }) => assert_eq!(sqlstate, "55000"),
        other => panic!(
            "expected currval reset after setval(..., false), got {:?}",
            other
        ),
    }
    assert_eq!(
        query_rows(&db, 1, "select nextval('seq')"),
        vec![vec![Value::Int64(20)]]
    );
    db.execute(1, "alter sequence seq restart with 11").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select last_value, log_cnt, is_called from seq"),
        vec![vec![Value::Int64(11), Value::Int64(0), Value::Bool(false)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select nextval('seq')"),
        vec![vec![Value::Int64(11)]]
    );
}

#[test]
fn alter_sequence_rename_moves_conflicting_array_type_names() {
    let base = temp_dir("alter_sequence_rename_array_type_conflict");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create sequence seq_array").unwrap();
    db.execute(1, "create sequence seq_array2").unwrap();
    db.execute(1, "alter sequence seq_array2 rename to _seq_array")
        .unwrap();

    let catcache = db.catalog.read().catcache().unwrap();
    let original_class = catcache.class_by_name("seq_array").unwrap();
    let original_type = catcache.type_by_oid(original_class.reltype).unwrap();
    let original_array_type = catcache.type_by_oid(original_type.typarray).unwrap();
    assert_eq!(original_array_type.typname, "__seq_array");

    let renamed_class = catcache.class_by_name("_seq_array").unwrap();
    let renamed_type = catcache.type_by_oid(renamed_class.reltype).unwrap();
    assert_eq!(renamed_type.typname, "_seq_array");
    let renamed_array_type = catcache.type_by_oid(renamed_type.typarray).unwrap();
    assert_eq!(renamed_array_type.typname, "__seq_array_1");
    assert_ne!(original_array_type.oid, renamed_array_type.oid);
}

#[test]
fn drop_sequence_restrict_and_cascade_respect_serial_dependencies() {
    let base = temp_dir("drop_sequence_dependencies");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table items (id serial, note text)")
        .unwrap();

    match db.execute(1, "drop sequence items_id_seq") {
        Err(ExecError::DetailedError { sqlstate, .. }) => assert_eq!(sqlstate, "2BP01"),
        other => panic!("expected dependent-object error, got {:?}", other),
    }

    db.execute(1, "drop sequence items_id_seq cascade").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select pg_get_serial_sequence('items', 'id')"),
        vec![vec![Value::Null]]
    );

    match db.execute(1, "insert into items (note) values ('x')") {
        Err(ExecError::NotNullViolation { column, .. }) => assert_eq!(column, "id"),
        other => panic!(
            "expected not-null violation after dropping serial default, got {:?}",
            other
        ),
    }
}

#[test]
fn drop_sequence_restrict_and_cascade_respect_row_type_dependencies() {
    let base = temp_dir("drop_sequence_row_type_dependencies");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create sequence depseq").unwrap();
    db.execute(1, "create table dep_table (payload depseq)")
        .unwrap();

    match db.execute(1, "drop sequence depseq") {
        Err(ExecError::DetailedError { sqlstate, .. }) => assert_eq!(sqlstate, "2BP01"),
        other => panic!("expected dependent-object error, got {:?}", other),
    }

    db.execute(1, "drop sequence depseq cascade").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_class where relname = 'dep_table'"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn rejected_create_table_like_sequence_does_not_poison_catalog_after_sequence_drop() {
    let base = temp_dir("rejected_create_table_like_sequence");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(1, "create sequence ctlseq1").unwrap();
    match db.execute(1, "create table ctlt10 (like ctlseq1)") {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature))) => {
            assert_eq!(feature, "CREATE TABLE LIKE source relation kind S")
        }
        other => panic!(
            "expected rejected CREATE TABLE LIKE sequence error, got {:?}",
            other
        ),
    }
    db.execute(1, "drop sequence ctlseq1").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from pg_namespace"),
        vec![vec![Value::Int64(3)]]
    );
}

#[test]
fn create_table_check_not_enforced_skips_write_enforcement_and_cannot_validate() {
    let base = temp_dir("check_not_enforced");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table items (id int4 constraint items_id_check check (id > 0) not enforced)",
    )
    .unwrap();

    db.execute(1, "insert into items values (0)").unwrap();
    db.execute(1, "insert into items values (1)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conenforced, convalidated from pg_constraint where conname = 'items_id_check'"
        ),
        vec![vec![Value::Bool(false), Value::Bool(false)]]
    );

    match db.execute(1, "alter table items validate constraint items_id_check") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) if message == "cannot validate NOT ENFORCED constraint" && sqlstate == "0A000" => {}
        other => panic!("expected validate-not-enforced failure, got {other:?}"),
    }
}

#[test]
fn update_and_copy_from_enforce_check_and_not_null_constraints() {
    let base = temp_dir("update_and_copy_constraint_checks");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(
        1,
        "create table items (
            id int4 constraint items_id_positive check (id > 0),
            note text constraint items_note_required not null
        )",
    )
    .unwrap();
    db.execute(1, "insert into items values (1, 'ok')").unwrap();

    match db.execute(1, "update items set id = 0 where id = 1") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_positive" => {}
        other => panic!("expected update check violation, got {other:?}"),
    }

    match db.execute(1, "update items set note = null where id = 1") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected update not-null violation, got {other:?}"),
    }

    match session.copy_from_rows(&db, "items", &[vec!["0".into(), "copy".into()]]) {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_positive" => {}
        other => panic!("expected copy check violation, got {other:?}"),
    }

    match session.copy_from_rows(&db, "items", &[vec!["2".into(), "\\N".into()]]) {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected copy not-null violation, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![vec![Value::Int32(1), Value::Text("ok".into())]]
    );
}

#[test]
fn prepared_insert_enforces_check_and_not_null_constraints() {
    let base = temp_dir("prepared_insert_constraints");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(
        1,
        "create table items (
            id int4 constraint items_id_positive check (id > 0),
            note text constraint items_note_required not null
        )",
    )
    .unwrap();

    let prepared = session.prepare_insert(&db, "items", None, 2).unwrap();
    session.execute(&db, "begin").unwrap();

    match session.execute_prepared_insert(
        &db,
        &prepared,
        &[Value::Int32(0), Value::Text("bad".into())],
    ) {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_positive" => {}
        other => panic!("expected prepared-insert check violation, got {other:?}"),
    }

    match session.execute_prepared_insert(&db, &prepared, &[Value::Int32(2), Value::Null]) {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected prepared-insert not-null violation, got {other:?}"),
    }

    session
        .execute_prepared_insert(&db, &prepared, &[Value::Int32(3), Value::Text("ok".into())])
        .unwrap();
    session.execute(&db, "commit").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items"),
        vec![vec![Value::Int32(3), Value::Text("ok".into())]]
    );
}

#[test]
fn prepared_insert_and_copy_from_enforce_foreign_keys() {
    let base = temp_dir("prepared_insert_copy_foreign_keys");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4 references parents)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1)").unwrap();

    match session.copy_from_rows(&db, "children", &[vec!["1".into(), "2".into()]]) {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected COPY foreign-key violation, got {other:?}"),
    }

    let prepared = session.prepare_insert(&db, "children", None, 2).unwrap();
    session.execute(&db, "begin").unwrap();
    match session.execute_prepared_insert(&db, &prepared, &[Value::Int32(1), Value::Int32(2)]) {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected prepared foreign-key violation, got {other:?}"),
    }
    session
        .execute_prepared_insert(&db, &prepared, &[Value::Int32(1), Value::Int32(1)])
        .unwrap();
    session.execute(&db, "commit").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, parent_id from children"),
        vec![vec![Value::Int32(1), Value::Int32(1)]]
    );
}

#[test]
fn alter_table_add_constraints_support_not_valid_and_validate() {
    let base = temp_dir("alter_table_add_constraints_validate");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (0, null), (2, 'ok')")
        .unwrap();

    db.execute(
        1,
        "alter table items add constraint items_id_positive check (id > 0) not valid",
    )
    .unwrap();
    db.execute(
        1,
        "alter table items add constraint items_note_required not null note not valid",
    )
    .unwrap();

    match db.execute(1, "insert into items values (0, 'later')") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_positive" => {}
        other => panic!("expected ALTER TABLE CHECK violation, got {other:?}"),
    }

    match db.execute(1, "insert into items values (3, null)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected ALTER TABLE NOT NULL violation, got {other:?}"),
    }

    let rows = query_rows(
        &db,
        1,
        "select conname, contype, convalidated, array_length(conkey, 1), conbin \
         from pg_constraint \
         where conrelid = (select oid from pg_class where relname = 'items') \
         order by conname",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Text("items_id_positive".into()));
    assert_eq!(rows[0][1], Value::Text("c".into()));
    assert_eq!(rows[0][2], Value::Bool(false));
    assert_eq!(rows[0][3], Value::Null);
    assert_eq!(rows[0][4], Value::Text("id > 0".into()));
    assert_eq!(rows[1][0], Value::Text("items_note_required".into()));
    assert_eq!(rows[1][1], Value::Text("n".into()));
    assert_eq!(rows[1][2], Value::Bool(false));
    assert_eq!(int_value(&rows[1][3]), 1);
    assert_eq!(rows[1][4], Value::Null);

    match db.execute(1, "alter table items validate constraint items_id_positive") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_positive" => {}
        other => panic!("expected CHECK validate failure, got {other:?}"),
    }

    match db.execute(
        1,
        "alter table items validate constraint items_note_required",
    ) {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected NOT NULL validate failure, got {other:?}"),
    }

    db.execute(
        1,
        "update items set id = 1, note = 'filled' where id = 0 and note is null",
    )
    .unwrap();
    db.execute(1, "alter table items validate constraint items_id_positive")
        .unwrap();
    db.execute(
        1,
        "alter table items validate constraint items_note_required",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, convalidated \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items') \
             order by conname",
        ),
        vec![
            vec![Value::Text("items_id_positive".into()), Value::Bool(true)],
            vec![Value::Text("items_note_required".into()), Value::Bool(true)],
        ]
    );
}

#[test]
fn alter_table_add_validate_and_drop_foreign_keys() {
    let base = temp_dir("alter_table_foreign_keys");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1)").unwrap();
    db.execute(1, "insert into children values (1, 1), (2, 2)")
        .unwrap();

    db.execute(
        1,
        "alter table children add constraint children_parent_fk foreign key (parent_id) references parents(id) not valid",
    )
    .unwrap();

    match db.execute(1, "insert into children values (3, 3)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_fk");
        }
        other => panic!("expected ALTER TABLE foreign-key violation, got {other:?}"),
    }

    assert_eq!(
        query_rows(
            &db,
            1,
            "select convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![Value::Bool(false)]]
    );

    match db.execute(
        1,
        "alter table children validate constraint children_parent_fk",
    ) {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_fk");
        }
        other => panic!("expected foreign-key validate failure, got {other:?}"),
    }

    db.execute(1, "update children set parent_id = 1 where id = 2")
        .unwrap();
    db.execute(
        1,
        "alter table children validate constraint children_parent_fk",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![Value::Bool(true)]]
    );

    db.execute(1, "alter table children drop constraint children_parent_fk")
        .unwrap();
    db.execute(1, "delete from parents where id = 1").unwrap();
}

#[test]
fn alter_table_add_foreign_key_without_constraint_name_generates_default_name() {
    let base = temp_dir("alter_table_add_fk_unnamed");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1)").unwrap();
    db.execute(1, "insert into children values (1, 1)").unwrap();

    db.execute(
        1,
        "alter table children add foreign key (parent_id) references parents(id)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, convalidated from pg_constraint where conrelid = (select oid from pg_class where relname = 'children') and contype = 'f'",
        ),
        vec![vec![
            Value::Text("children_parent_id_fkey".into()),
            Value::Bool(true),
        ]]
    );

    match db.execute(1, "insert into children values (2, 2)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected unnamed ALTER TABLE foreign-key violation, got {other:?}"),
    }

    db.execute(
        1,
        "alter table children drop constraint children_parent_id_fkey",
    )
    .unwrap();
}

#[test]
fn alter_table_add_foreign_key_without_constraint_name_accepts_no_space_before_column_list() {
    let base = temp_dir("alter_table_add_fk_unnamed_nospace");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table parents (id int4, code text, primary key (id, code))",
    )
    .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4, parent_code text)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1, 'one')")
        .unwrap();
    db.execute(
        1,
        "insert into children values (1, 1, 'one'), (2, null, null)",
    )
    .unwrap();

    db.execute(
        1,
        "alter table children add foreign key(parent_id, parent_code) references parents(id, code) match full",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select confmatchtype, convalidated from pg_constraint where conname = 'children_parent_id_parent_code_fkey'",
        ),
        vec![vec![Value::Text("f".into()), Value::Bool(true)]]
    );
}

#[test]
fn create_table_unique_nulls_not_distinct_treats_nulls_as_conflicting() {
    let base = temp_dir("unique_nulls_not_distinct");
    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(16)).unwrap();

    db.execute(
        1,
        "create table items (id int4 unique nulls not distinct, note text)",
    )
    .unwrap();
    db.execute(1, "insert into items (note) values ('first')")
        .unwrap();

    match db.execute(1, "insert into items (note) values ('second')") {
        Err(ExecError::UniqueViolation { constraint, .. }) if constraint == "items_id_key" => {}
        other => panic!("expected null unique violation, got {other:?}"),
    }

    assert_eq!(
        query_rows(
            &db,
            1,
            "select indnullsnotdistinct from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = 'items_id_key'"
        ),
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn foreign_keys_support_enforced_not_valid_on_create_and_alter_table_add() {
    let base = temp_dir("foreign_keys_enforced_not_valid");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(1, "insert into parents values (1)").unwrap();
    db.execute(
        1,
        "create table children_create (id int4 primary key, parent_id int4 references parents not valid)",
    )
    .unwrap();
    db.execute(1, "insert into children_create values (1, 1)")
        .unwrap();
    match db.execute(1, "insert into children_create values (2, 2)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_create_parent_id_fkey");
        }
        other => panic!("expected create-table NOT VALID foreign-key violation, got {other:?}"),
    }
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conenforced, convalidated from pg_constraint where conname = 'children_create_parent_id_fkey'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(false)]]
    );

    db.execute(
        1,
        "create table children_add (id int4 primary key, parent_id int4)",
    )
    .unwrap();
    db.execute(1, "insert into children_add values (1, 1), (2, 2)")
        .unwrap();
    db.execute(
        1,
        "alter table children_add add constraint children_add_parent_fk foreign key (parent_id) references parents(id) not valid",
    )
    .unwrap();
    match db.execute(1, "insert into children_add values (3, 3)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_add_parent_fk");
        }
        other => panic!("expected alter-table-add NOT VALID foreign-key violation, got {other:?}"),
    }
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conenforced, convalidated from pg_constraint where conname = 'children_add_parent_fk'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(false)]]
    );
}

#[test]
fn alter_table_alter_constraint_updates_foreign_key_deferrability_flags() {
    let base = temp_dir("alter_table_alter_constraint_fk_deferrability");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4 references parents)",
    )
    .unwrap();

    db.execute(
        1,
        "alter table children alter constraint children_parent_id_fkey deferrable initially deferred",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select condeferrable, condeferred from pg_constraint where conname = 'children_parent_id_fkey'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(true)]]
    );

    db.execute(
        1,
        "alter table children alter constraint children_parent_id_fkey not deferrable initially immediate",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select condeferrable, condeferred from pg_constraint where conname = 'children_parent_id_fkey'",
        ),
        vec![vec![Value::Bool(false), Value::Bool(false)]]
    );
}

#[test]
fn foreign_keys_support_not_enforced_and_alter_enforced_state() {
    let base = temp_dir("foreign_keys_not_enforced");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4 references parents not enforced)",
    )
    .unwrap();

    db.execute(1, "insert into children values (1, 42)")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conenforced, convalidated from pg_constraint where conname = 'children_parent_id_fkey'",
        ),
        vec![vec![Value::Bool(false), Value::Bool(false)]]
    );

    match db.execute(
        1,
        "alter table children alter constraint children_parent_id_fkey enforced",
    ) {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected alter constraint enforced validation failure, got {other:?}"),
    }

    db.execute(1, "insert into parents values (42)").unwrap();
    db.execute(
        1,
        "alter table children alter constraint children_parent_id_fkey enforced",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conenforced, convalidated from pg_constraint where conname = 'children_parent_id_fkey'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(true)]]
    );

    match db.execute(1, "insert into children values (2, 99)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected enforced foreign-key violation, got {other:?}"),
    }

    db.execute(
        1,
        "alter table children alter constraint children_parent_id_fkey not enforced not deferrable",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select condeferrable, condeferred, conenforced, convalidated from pg_constraint where conname = 'children_parent_id_fkey'",
        ),
        vec![vec![
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
        ]]
    );

    db.execute(1, "insert into children values (2, 99)")
        .unwrap();
}

#[test]
fn alter_constraint_not_enforced_preserves_deferrability_flags() {
    let base = temp_dir("fk_alter_constraint_not_enforced_preserves_deferrability");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4)",
    )
    .unwrap();
    db.execute(1, "insert into children values (1, 42)")
        .unwrap();
    db.execute(
        1,
        "alter table children add constraint children_parent_fk foreign key (parent_id) references parents(id) not valid not enforced",
    )
    .unwrap();

    db.execute(
        1,
        "alter table children alter constraint children_parent_fk deferrable initially deferred",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select condeferrable, condeferred, conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
        ]]
    );

    db.execute(
        1,
        "alter table children alter constraint children_parent_fk enforced",
    )
    .unwrap_err();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select condeferrable, condeferred, conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
        ]]
    );

    db.execute(1, "insert into parents values (42)").unwrap();
    db.execute(
        1,
        "alter table children alter constraint children_parent_fk enforced",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select condeferrable, condeferred, conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
        ]]
    );

    db.execute(
        1,
        "alter table children alter constraint children_parent_fk not enforced",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select condeferrable, condeferred, conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
        ]]
    );
}

#[test]
fn alter_constraint_enforced_validates_existing_unvalidated_foreign_key() {
    let base = temp_dir("fk_alter_constraint_enforced_validates");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1)").unwrap();
    db.execute(1, "insert into children values (1, 1), (2, 2)")
        .unwrap();
    db.execute(
        1,
        "alter table children add constraint children_parent_fk foreign key (parent_id) references parents(id) not valid",
    )
    .unwrap();

    match db.execute(
        1,
        "alter table children alter constraint children_parent_fk enforced",
    ) {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_fk");
        }
        other => panic!("expected ALTER CONSTRAINT ENFORCED validation failure, got {other:?}"),
    }

    db.execute(1, "update children set parent_id = 1 where id = 2")
        .unwrap();
    db.execute(
        1,
        "alter table children alter constraint children_parent_fk enforced",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(true)]]
    );
}

#[test]
fn alter_constraint_enforced_validates_match_full_existing_rows() {
    let base = temp_dir("fk_alter_constraint_enforced_match_full");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table parents (id int4, code text, primary key (id, code))",
    )
    .unwrap();
    db.execute(
        1,
        "create table children (
            id int4 primary key,
            parent_id int4,
            parent_code text
        )",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1, 'one')")
        .unwrap();
    db.execute(1, "insert into children values (1, 1, null)")
        .unwrap();
    db.execute(
        1,
        "alter table children add constraint children_parent_fk foreign key (parent_id, parent_code) references parents(id, code) match full not valid not enforced",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select confmatchtype, conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![
            Value::Text("f".into()),
            Value::Bool(false),
            Value::Bool(false),
        ]]
    );

    match db.execute(
        1,
        "alter table children alter constraint children_parent_fk enforced",
    ) {
        Err(ExecError::ForeignKeyViolation {
            constraint, detail, ..
        }) => {
            assert_eq!(constraint, "children_parent_fk");
            assert!(
                detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("MATCH FULL"))
            );
        }
        other => panic!("expected ALTER CONSTRAINT ENFORCED MATCH FULL failure, got {other:?}"),
    }
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![Value::Bool(false), Value::Bool(false)]]
    );

    db.execute(1, "update children set parent_id = null where id = 1")
        .unwrap();
    db.execute(
        1,
        "alter table children alter constraint children_parent_fk enforced",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select confmatchtype, conenforced, convalidated from pg_constraint where conname = 'children_parent_fk'",
        ),
        vec![vec![
            Value::Text("f".into()),
            Value::Bool(true),
            Value::Bool(true),
        ]]
    );
}

#[test]
fn alter_table_alter_constraint_rejects_non_foreign_keys() {
    let base = temp_dir("alter_table_alter_constraint_non_fk");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 primary key)")
        .unwrap();

    match db.execute(
        1,
        "alter table items alter constraint items_pkey deferrable",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(
                message,
                "constraint \"items_pkey\" of relation \"items\" is not a foreign key constraint"
            );
            assert_eq!(sqlstate, "42809");
        }
        other => panic!("expected ALTER CONSTRAINT wrong-object-type error, got {other:?}"),
    }
}

#[test]
fn alter_table_alter_constraint_initially_deferred_defers_until_commit() {
    let base = temp_dir("alter_table_alter_constraint_deferred_commit");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parents (id int4 primary key)")
        .unwrap();
    session
        .execute(
            &db,
            "create table children (id int4 primary key, parent_id int4 references parents)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "alter table children alter constraint children_parent_id_fkey deferrable initially deferred",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into children values (1, 42)")
        .unwrap();
    match session.execute(&db, "commit") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected deferred foreign-key violation at commit, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select count(*) from children"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn alter_table_alter_constraint_initially_deferred_allows_fixup_before_commit() {
    let base = temp_dir("alter_table_alter_constraint_fixup_before_commit");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parents (id int4 primary key)")
        .unwrap();
    session
        .execute(
            &db,
            "create table children (id int4 primary key, parent_id int4 references parents)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "alter table children alter constraint children_parent_id_fkey deferrable initially deferred",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into children values (1, 42)")
        .unwrap();
    session
        .execute(&db, "insert into parents values (42)")
        .unwrap();
    session.execute(&db, "commit").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, parent_id from children order by id",),
        vec![vec![Value::Int32(1), Value::Int32(42)]]
    );
}

#[test]
fn alter_table_alter_constraint_initially_deferred_parent_delete_fails_at_commit() {
    let base = temp_dir("alter_table_alter_constraint_parent_delete_commit");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parents (id int4 primary key)")
        .unwrap();
    session
        .execute(
            &db,
            "create table children (id int4 primary key, parent_id int4 references parents)",
        )
        .unwrap();
    session
        .execute(&db, "insert into parents values (1)")
        .unwrap();
    session
        .execute(&db, "insert into children values (1, 1)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table children alter constraint children_parent_id_fkey deferrable initially deferred",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "delete from parents where id = 1")
        .unwrap();
    match session.execute(&db, "commit") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected deferred parent-delete violation at commit, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select id from parents order by id"),
        vec![vec![Value::Int32(1)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select id, parent_id from children order by id",),
        vec![vec![Value::Int32(1), Value::Int32(1)]]
    );
}

#[test]
fn alter_table_alter_constraint_initially_deferred_parent_update_allows_fixup_before_commit() {
    let base = temp_dir("alter_table_alter_constraint_parent_update_fixup");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parents (id int4 primary key)")
        .unwrap();
    session
        .execute(
            &db,
            "create table children (id int4 primary key, parent_id int4 references parents)",
        )
        .unwrap();
    session
        .execute(&db, "insert into parents values (1)")
        .unwrap();
    session
        .execute(&db, "insert into children values (1, 1)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table children alter constraint children_parent_id_fkey deferrable initially deferred",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "update parents set id = 2 where id = 1")
        .unwrap();
    session
        .execute(&db, "update children set parent_id = 2 where id = 1")
        .unwrap();
    session.execute(&db, "commit").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id from parents order by id"),
        vec![vec![Value::Int32(2)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select id, parent_id from children order by id",),
        vec![vec![Value::Int32(1), Value::Int32(2)]]
    );
}

#[test]
fn alter_table_alter_constraint_initially_immediate_keeps_checks_immediate() {
    let base = temp_dir("alter_table_alter_constraint_immediate_runtime");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parents (id int4 primary key)")
        .unwrap();
    session
        .execute(
            &db,
            "create table children (id int4 primary key, parent_id int4 references parents)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "alter table children alter constraint children_parent_id_fkey deferrable initially immediate",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    match session.execute(&db, "insert into children values (1, 42)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected immediate foreign-key violation, got {other:?}"),
    }
    session.execute(&db, "rollback").unwrap();
}

#[test]
fn alter_table_alter_constraint_deferred_auto_commit_validates_before_implicit_commit() {
    let base = temp_dir("alter_table_alter_constraint_autocommit");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4 references parents)",
    )
    .unwrap();
    db.execute(
        1,
        "alter table children alter constraint children_parent_id_fkey deferrable initially deferred",
    )
    .unwrap();

    match db.execute(1, "insert into children values (1, 42)") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected implicit-commit foreign-key violation, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select count(*) from children"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn deferrable_primary_key_initially_immediate_fails_at_statement_end() {
    let base = temp_dir("deferrable_primary_key_immediate_statement_end");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 primary key deferrable)")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select con.condeferrable, con.condeferred, i.indimmediate \
               from pg_constraint con \
               join pg_index i on i.indexrelid = con.conindid \
              where con.conname = 'items_pkey'",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
        ]]
    );

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into items values (1)")
        .unwrap();
    assert_eq!(
        session_query_rows(&mut session, &db, "select id from items"),
        vec![vec![Value::Int32(1)]]
    );
    match session.execute(&db, "insert into items values (1)") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_pkey");
        }
        other => panic!("expected deferred primary-key violation at statement end, got {other:?}"),
    }
    session.execute(&db, "rollback").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn alter_table_add_deferrable_unique_initially_deferred_fails_at_commit() {
    let base = temp_dir("alter_table_add_deferrable_unique_commit");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 primary key, code int4)")
        .unwrap();
    session
        .execute(
            &db,
            "alter table items add constraint items_code_key unique (code) deferrable initially deferred",
        )
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select con.condeferrable, con.condeferred, i.indimmediate \
               from pg_constraint con \
               join pg_index i on i.indexrelid = con.conindid \
              where con.conname = 'items_code_key'",
        ),
        vec![vec![
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
        ]]
    );

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into items values (1, 10)")
        .unwrap();
    session
        .execute(&db, "insert into items values (2, 10)")
        .unwrap();
    match session.execute(&db, "commit") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_code_key");
        }
        other => panic!("expected deferred unique violation at commit, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn set_constraints_named_deferred_postpones_deferrable_foreign_key_checks() {
    let base = temp_dir("set_constraints_named_deferred_fk");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parents (id int4 primary key)")
        .unwrap();
    session
        .execute(
            &db,
            "create table children (
                id int4 primary key,
                parent_id int4 references parents deferrable
            )",
        )
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select pg_get_constraintdef(oid) \
               from pg_constraint \
              where conname = 'children_parent_id_fkey'",
        ),
        vec![vec![Value::Text(
            "FOREIGN KEY (parent_id) REFERENCES parents(id) DEFERRABLE".into()
        )]]
    );

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "set constraints children_parent_id_fkey deferred")
        .unwrap();
    session
        .execute(&db, "insert into children values (1, 42)")
        .unwrap();
    session
        .execute(&db, "insert into parents values (42)")
        .unwrap();
    session.execute(&db, "commit").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, parent_id from children order by id"),
        vec![vec![Value::Int32(1), Value::Int32(42)]]
    );
}

#[test]
fn set_constraints_all_deferred_and_named_immediate_control_unique_checks() {
    let base = temp_dir("set_constraints_all_deferred_unique");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table items (
                id int4 primary key,
                code int4 unique deferrable
            )",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "set constraints all deferred")
        .unwrap();
    session
        .execute(&db, "insert into items values (1, 10)")
        .unwrap();
    session
        .execute(&db, "insert into items values (2, 10)")
        .unwrap();
    match session.execute(&db, "set constraints items_code_key immediate") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_code_key");
        }
        other => panic!(
            "expected retroactive unique validation failure from SET CONSTRAINTS IMMEDIATE, got {other:?}"
        ),
    }
    session.execute(&db, "rollback").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn set_constraints_outside_transaction_emits_warning() {
    let base = temp_dir("set_constraints_outside_transaction_warning");
    let db = Database::open(&base, 16).unwrap();

    clear_backend_notices();
    db.execute(1, "set constraints all deferred").unwrap();

    let notices = take_backend_notices();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].severity, "WARNING");
    assert_eq!(notices[0].sqlstate, "01000");
    assert_eq!(
        notices[0].message,
        "SET CONSTRAINTS can only be used in transaction blocks"
    );
}

#[test]
fn on_conflict_rejects_deferrable_unique_arbiters() {
    let base = temp_dir("on_conflict_rejects_deferrable_arbiter");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table items (id int4 primary key, code int4 unique deferrable)",
    )
    .unwrap();

    match db.execute(
        1,
        "insert into items values (1, 10) on conflict on constraint items_code_key do nothing",
    ) {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature))) => {
            assert_eq!(
                feature,
                "ON CONFLICT does not support deferrable unique constraints as arbiters"
            );
        }
        other => panic!("expected ON CONFLICT deferrable-arbiter rejection, got {other:?}"),
    }
}

#[test]
fn foreign_keys_restrict_parent_updates_and_deletes() {
    let base = temp_dir("foreign_key_parent_restrict");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4 references parents)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1), (2)")
        .unwrap();
    db.execute(1, "insert into children values (1, 1)").unwrap();

    match db.execute(1, "delete from parents where id = 1") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected delete foreign-key restriction, got {other:?}"),
    }

    match db.execute(1, "update parents set id = 3 where id = 1") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected update foreign-key restriction, got {other:?}"),
    }

    match db.execute(1, "update children set parent_id = 9 where id = 1") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => panic!("expected child update foreign-key violation, got {other:?}"),
    }

    db.execute(1, "update children set parent_id = 2 where id = 1")
        .unwrap();
    db.execute(1, "delete from parents where id = 1").unwrap();
}

#[test]
fn foreign_keys_block_parent_ddl_and_allow_child_drop() {
    let base = temp_dir("foreign_key_ddl_blockers");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4 references parents)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1)").unwrap();
    db.execute(1, "insert into children values (1, 1)").unwrap();

    for sql in [
        "drop table parents",
        "truncate parents",
        "alter table parents drop column id",
        "alter table children drop column parent_id",
        "alter table parents drop constraint parents_pkey",
    ] {
        match db.execute(1, sql) {
            Err(ExecError::DetailedError {
                detail: Some(detail),
                ..
            }) if detail.contains("foreign key constraint")
                || detail.contains("depends on table")
                || detail.contains("depends on") => {}
            Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. }))
                if actual.contains("foreign key constraint")
                    || actual.contains("referenced by foreign key")
                    || actual.contains("used by foreign key") => {}
            other => panic!("expected foreign-key DDL blocker for `{sql}`, got {other:?}"),
        }
    }

    db.execute(1, "alter table parents alter column id type int8")
        .unwrap();
    db.execute(1, "alter table children alter column parent_id type int8")
        .unwrap();

    db.execute(1, "drop table children").unwrap();
    db.execute(1, "drop table parents").unwrap();
}

#[test]
fn foreign_key_locking_blocks_parent_delete_until_child_insert_finishes() {
    use std::sync::mpsc;

    let base = temp_dir("foreign_key_locking_delete_block");
    let db = Database::open(&base, 64).unwrap();
    let mut session_a = Session::new(1);

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create table children (id int4 primary key, parent_id int4 references parents)",
    )
    .unwrap();
    db.execute(1, "insert into parents values (1)").unwrap();

    session_a.execute(&db, "begin").unwrap();
    session_a
        .execute(&db, "insert into children values (1, 1)")
        .unwrap();

    let db2 = db.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = db2.execute(2, "delete from parents where id = 1");
        done_tx.send(result).unwrap();
    });

    started_rx.recv().unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "parent delete should block while the child insert holds foreign-key partner locks"
    );

    session_a.execute(&db, "commit").unwrap();

    match done_rx.recv_timeout(TEST_TIMEOUT).unwrap() {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            assert_eq!(constraint, "children_parent_id_fkey");
        }
        other => {
            panic!("expected blocked delete to fail with foreign-key violation, got {other:?}")
        }
    }
    worker.join().unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id from parents"),
        vec![vec![Value::Int32(1)]]
    );
}

#[test]
fn alter_table_set_and_drop_not_null_updates_enforcement_and_catalog() {
    let base = temp_dir("alter_table_set_drop_not_null");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, null), (2, 'ok')")
        .unwrap();

    match db.execute(1, "alter table items alter column note set not null") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_not_null" => {}
        other => panic!("expected SET NOT NULL validation failure, got {other:?}"),
    }

    db.execute(1, "update items set note = 'filled' where id = 1")
        .unwrap();
    db.execute(1, "alter table items alter column note set not null")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname, contype, convalidated \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items') \
             order by conname",
        ),
        vec![vec![
            Value::Text("items_note_not_null".into()),
            Value::Text("n".into()),
            Value::Bool(true),
        ]]
    );

    match db.execute(1, "insert into items values (3, null)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_not_null" => {}
        other => panic!("expected enforced SET NOT NULL violation, got {other:?}"),
    }

    db.execute(1, "alter table items alter column note drop not null")
        .unwrap();
    db.execute(1, "insert into items values (3, null)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items')",
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn alter_table_add_and_drop_key_constraints_manage_indexes() {
    let base = temp_dir("alter_table_key_constraints_indexes");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, code int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 10, 'a'), (2, 20, 'b')")
        .unwrap();

    db.execute(
        1,
        "alter table items add constraint items_pkey primary key (id)",
    )
    .unwrap();
    db.execute(
        1,
        "alter table items add constraint items_code_key unique (code)",
    )
    .unwrap();

    let rows = query_rows(
        &db,
        1,
        "select conname, contype, convalidated, conindid, array_length(conkey, 1) \
         from pg_constraint \
         where conrelid = (select oid from pg_class where relname = 'items') \
         order by conname",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Text("items_code_key".into()));
    assert_eq!(rows[0][1], Value::Text("u".into()));
    assert_eq!(rows[0][2], Value::Bool(true));
    assert!(int_value(&rows[0][3]) > 0);
    assert_eq!(int_value(&rows[0][4]), 1);
    assert_eq!(rows[1][0], Value::Text("items_id_not_null".into()));
    assert_eq!(rows[1][1], Value::Text("n".into()));
    assert_eq!(rows[1][2], Value::Bool(true));
    assert_eq!(int_value(&rows[1][3]), 0);
    assert_eq!(int_value(&rows[1][4]), 1);
    assert_eq!(rows[2][0], Value::Text("items_pkey".into()));
    assert_eq!(rows[2][1], Value::Text("p".into()));
    assert_eq!(rows[2][2], Value::Bool(true));
    assert!(int_value(&rows[2][3]) > 0);
    assert_eq!(int_value(&rows[2][4]), 1);

    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname \
             from pg_class \
             where relname in ('items_code_key', 'items_pkey') \
             order by relname",
        ),
        vec![
            vec![Value::Text("items_code_key".into())],
            vec![Value::Text("items_pkey".into())],
        ]
    );

    db.execute(1, "alter table items drop constraint items_code_key")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname \
             from pg_class \
             where relname in ('items_code_key', 'items_pkey') \
             order by relname",
        ),
        vec![vec![Value::Text("items_pkey".into())]]
    );

    db.execute(1, "alter table items drop constraint items_pkey")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items') \
             order by conname",
        ),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname \
             from pg_class \
             where relname in ('items_code_key', 'items_pkey')",
        ),
        Vec::<Vec<Value>>::new()
    );
    db.execute(1, "insert into items values (null, 10, 'after drop')")
        .unwrap();
}

#[test]
fn alter_table_rename_constraint_updates_catalog_and_enforcement() {
    let base = temp_dir("alter_table_rename_constraint_check");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(
        1,
        "alter table items add constraint items_id_positive check (id > 0)",
    )
    .unwrap();
    db.execute(
        1,
        "alter table items rename constraint items_id_positive to items_id_guard",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items')",
        ),
        vec![vec![Value::Text("items_id_guard".into())]]
    );
    match db.execute(1, "insert into items values (0)") {
        Err(ExecError::CheckViolation {
            relation,
            constraint,
        }) if relation == "items" && constraint == "items_id_guard" => {}
        other => panic!("expected renamed CHECK constraint violation, got {other:?}"),
    }
}

#[test]
fn alter_table_rename_constraint_renames_backing_index() {
    let base = temp_dir("alter_table_rename_constraint_index");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, code int4)")
        .unwrap();
    db.execute(
        1,
        "alter table items add constraint items_pkey primary key (id)",
    )
    .unwrap();
    db.execute(
        1,
        "alter table items rename constraint items_pkey to items_primary",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items') \
             order by conname",
        ),
        vec![
            vec![Value::Text("items_id_not_null".into())],
            vec![Value::Text("items_primary".into())],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname \
             from pg_class \
             where relname in ('items_pkey', 'items_primary') \
             order by relname",
        ),
        vec![vec![Value::Text("items_primary".into())]]
    );
}

#[test]
fn alter_table_rename_not_null_constraint_updates_column_enforcement() {
    let base = temp_dir("alter_table_rename_constraint_not_null");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "alter table items alter column note set not null")
        .unwrap();
    db.execute(
        1,
        "alter table items rename constraint items_note_not_null to items_note_required",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items')",
        ),
        vec![vec![Value::Text("items_note_required".into())]]
    );
    match db.execute(1, "insert into items values (1, null)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "note" && constraint == "items_note_required" => {}
        other => panic!("expected renamed NOT NULL constraint violation, got {other:?}"),
    }
}

#[test]
fn alter_table_drop_primary_key_removes_only_pk_owned_not_null_constraints() {
    let base = temp_dir("alter_table_drop_primary_key_owned_not_null");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table items (id int4 not null, code int4, note text)",
    )
    .unwrap();
    db.execute(
        1,
        "alter table items add constraint items_pkey primary key (code)",
    )
    .unwrap();

    match db.execute(1, "alter table items drop constraint items_code_not_null") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. }))
            if actual.contains("PRIMARY KEY constraint \"items_pkey\"") => {}
        other => panic!("expected PK-owned NOT NULL drop rejection, got {other:?}"),
    }

    match db.execute(1, "alter table items alter column code drop not null") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. }))
            if actual.contains("PRIMARY KEY constraint \"items_pkey\"") => {}
        other => panic!("expected PK-owned column drop-not-null rejection, got {other:?}"),
    }

    assert_eq!(
        query_rows(
            &db,
            1,
            "select d.deptype \
             from pg_depend d \
             join pg_constraint n on n.oid = d.objid \
             join pg_constraint p on p.oid = d.refobjid \
             where n.conname = 'items_code_not_null' and p.conname = 'items_pkey'",
        ),
        vec![vec![Value::Text("i".into())]]
    );

    db.execute(1, "alter table items drop constraint items_pkey")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select conname \
             from pg_constraint \
             where conrelid = (select oid from pg_class where relname = 'items') \
             order by conname",
        ),
        vec![vec![Value::Text("items_id_not_null".into())]]
    );

    db.execute(1, "insert into items values (1, null, 'nullable code')")
        .unwrap();
    match db.execute(1, "insert into items values (null, 2, 'missing id')") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
            ..
        }) if relation == "items" && column == "id" && constraint == "items_id_not_null" => {}
        other => panic!("expected user-owned NOT NULL to remain, got {other:?}"),
    }
}

#[test]
fn create_temp_table_constraints_are_supported_with_postgres_persistence_rules() {
    let base = temp_dir("temp_table_constraints_supported");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table parents (id int4 primary key)")
        .unwrap();
    db.execute(
        1,
        "create temp table department (
            id int4 primary key,
            parent_department int4 references department,
            name text,
            unique (name)
        )",
    )
    .unwrap();
    db.execute(1, "insert into department values (0, null, 'ROOT')")
        .unwrap();
    db.execute(1, "insert into department values (1, 0, 'A')")
        .unwrap();

    match db.execute(1, "insert into department values (2, 9, 'bad parent')") {
        Err(ExecError::ForeignKeyViolation { constraint, .. })
            if constraint == "department_parent_department_fkey" => {}
        other => panic!("expected temp self-reference foreign-key violation, got {other:?}"),
    }

    match db.execute(1, "insert into department values (2, 0, 'A')") {
        Err(ExecError::UniqueViolation { constraint, .. })
            if constraint == "department_name_key" => {}
        other => panic!("expected temp unique violation, got {other:?}"),
    }

    match db.execute(
        1,
        "create temp table temp_children (id int4, parent_id int4 references parents)",
    ) {
        Err(ExecError::Parse(ParseError::InvalidTableDefinition(message)))
            if message == "constraints on temporary tables may reference only temporary tables" => {
        }
        other => panic!("expected postgres-style temp foreign-key rejection, got {other:?}"),
    }
}

#[test]
fn insert_and_copy_from_maintain_btree_index() {
    let base = temp_dir("btree_index_insert_copy");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null, name text)")
        .unwrap();
    session
        .execute(&db, "create index items_id_idx on items (id)")
        .unwrap();
    session
        .execute(&db, "insert into items values (1, 'alpha')")
        .unwrap();
    session
        .copy_from_rows(&db, "items", &[vec!["2".into(), "beta".into()]])
        .unwrap();

    match session
        .execute(&db, "select name from items where id = 2")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("beta".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session
        .execute(&db, "explain select name from items where id = 2")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert!(rows.iter().any(|row| {
                matches!(row.first(), Some(Value::Text(text)) if text.contains("Index Scan"))
            }));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn indexed_update_maintains_indexes() {
    let base = temp_dir("indexed_update_maintains_indexes");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    db.execute(1, "update items set id = 2, name = 'beta' where id = 1")
        .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select name from items where id = 2",
        "items_id_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select name from items where id = 1"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        query_rows(&db, 1, "select name from items where id = 2"),
        vec![vec![Value::Text("beta".into())]]
    );
}

#[test]
fn unique_index_insert_rejects_duplicate_key() {
    let base = temp_dir("unique_index_insert_rejects_duplicate_key");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "create unique index items_id_key on items (id)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();

    match db.execute(1, "insert into items values (1, 'beta')") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_id_key");
            assert_eq!(
                crate::backend::libpq::pqformat::format_exec_error(&ExecError::UniqueViolation {
                    constraint: constraint.clone(),
                    detail: None,
                }),
                "duplicate key value violates unique constraint \"items_id_key\""
            );
        }
        other => panic!("expected unique violation, got {:?}", other),
    }
}

#[test]
fn partial_index_catalog_persists_predicate_and_pg_get_indexdef_renders_where() {
    let db = setup_partial_index_matrix_db("partial_index_catalog");

    assert_eq!(
        query_rows(
            &db,
            1,
            "select indpred \
             from pg_index \
             where indexrelid = (select oid from pg_class where relname = 'items_keep_idx')",
        ),
        vec![vec![Value::Text("flag = 'keep'".into())]]
    );
    assert_eq!(
        psql_index_definition(&db, 1, "items", "items_keep_idx"),
        "CREATE INDEX items_keep_idx ON items USING btree (id) WHERE (flag = 'keep')"
    );
}

#[test]
fn partial_unique_index_build_and_insert_only_enforce_qualifying_rows() {
    let base = temp_dir("partial_unique_index_build");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, flag text, note text)")
        .unwrap();
    db.execute(
        1,
        "insert into items values \
         (1, 'skip', 'outside1'), \
         (1, 'skip', 'outside2'), \
         (2, 'keep', 'inside')",
    )
    .unwrap();
    db.execute(
        1,
        "create unique index items_keep_key on items (id) where flag = 'keep'",
    )
    .unwrap();

    db.execute(1, "insert into items values (1, 'skip', 'outside3')")
        .unwrap();

    match db.execute(1, "insert into items values (2, 'keep', 'dup')") {
        Err(ExecError::UniqueViolation { constraint, detail }) => {
            assert_eq!(constraint, "items_keep_key");
            assert_eq!(detail.as_deref(), Some("Key (id)=(2) already exists."));
        }
        other => panic!("expected unique violation, got {:?}", other),
    }

    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, flag, note from items order by id, flag, note"
        ),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("skip".into()),
                Value::Text("outside1".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Text("skip".into()),
                Value::Text("outside2".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Text("skip".into()),
                Value::Text("outside3".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Text("keep".into()),
                Value::Text("inside".into()),
            ],
        ]
    );
}

#[test]
fn unique_array_column_supports_duplicates_and_index_quals() {
    let base = temp_dir("unique_array_column_supports_duplicates_and_index_quals");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create temp table arr_tbl (f1 int[] unique)")
        .unwrap();
    db.execute(1, "insert into arr_tbl values ('{1,2,3}')")
        .unwrap();
    db.execute(1, "insert into arr_tbl values ('{1,2}')")
        .unwrap();
    db.execute(1, "insert into arr_tbl values ('{2,3,4}')")
        .unwrap();
    db.execute(1, "insert into arr_tbl values ('{1,5,3}')")
        .unwrap();
    db.execute(1, "insert into arr_tbl values ('{1,2,10}')")
        .unwrap();

    match db.execute(1, "insert into arr_tbl values ('{1,2,3}')") {
        Err(ExecError::UniqueViolation { constraint, detail }) => {
            assert_eq!(constraint, "arr_tbl_f1_key");
            assert_eq!(
                detail.as_deref(),
                Some("Key (f1)=({1,2,3}) already exists.")
            );
        }
        other => panic!("expected unique violation, got {:?}", other),
    }

    db.execute(1, "set enable_seqscan to off").unwrap();
    db.execute(1, "set enable_bitmapscan to off").unwrap();
    assert_explain_uses_index(
        &db,
        1,
        "select * from arr_tbl where f1 > '{1,2,3}' and f1 <= '{1,5,3}'",
        "arr_tbl_f1_key",
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select * from arr_tbl where f1 > '{1,2,3}' and f1 <= '{1,5,3}'"
        ),
        vec![
            vec![Value::PgArray(
                crate::include::nodes::datum::ArrayValue::from_1d(vec![
                    Value::Int32(1),
                    Value::Int32(2),
                    Value::Int32(10),
                ])
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            )],
            vec![Value::PgArray(
                crate::include::nodes::datum::ArrayValue::from_1d(vec![
                    Value::Int32(1),
                    Value::Int32(5),
                    Value::Int32(3),
                ])
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            )],
        ]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select * from arr_tbl where f1 >= '{1,2,3}' and f1 < '{1,5,3}'"
        ),
        vec![
            vec![Value::PgArray(
                crate::include::nodes::datum::ArrayValue::from_1d(vec![
                    Value::Int32(1),
                    Value::Int32(2),
                    Value::Int32(3),
                ])
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            )],
            vec![Value::PgArray(
                crate::include::nodes::datum::ArrayValue::from_1d(vec![
                    Value::Int32(1),
                    Value::Int32(2),
                    Value::Int32(10),
                ])
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            )],
        ]
    );
}

#[test]
fn unique_index_update_rejects_duplicate_key() {
    let base = temp_dir("unique_index_update_rejects_duplicate_key");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha'), (2, 'beta')")
        .unwrap();
    db.execute(1, "create unique index items_id_key on items (id)")
        .unwrap();

    match db.execute(1, "update items set id = 1 where id = 2") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_id_key");
        }
        other => panic!("expected unique violation, got {:?}", other),
    }

    assert_eq!(
        query_rows(&db, 1, "select id from items order by id"),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]
    );
}

#[test]
fn unique_index_update_same_key_succeeds_without_self_conflict() {
    let base = temp_dir("unique_index_update_same_key_succeeds");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();
    db.execute(1, "create unique index items_id_key on items (id)")
        .unwrap();
    db.execute(1, "update items set note = 'beta' where id = 1")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items where id = 1"),
        vec![vec![Value::Int32(1), Value::Text("beta".into())]]
    );
}

#[test]
fn indexed_bpchar_repeated_update_keeps_one_visible_row() {
    let base = temp_dir("indexed_bpchar_repeated_update_one_visible");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table slots (slotname char(20), backlink char(20))",
    )
    .unwrap();
    db.execute(
        1,
        "create index slots_name_idx on slots using btree (slotname bpchar_ops)",
    )
    .unwrap();
    db.execute(1, "insert into slots values ('PS.base.a1', '')")
        .unwrap();
    db.execute(
        1,
        "update slots set backlink = 'WS.001.1a' where slotname = 'PS.base.a1'::bpchar",
    )
    .unwrap();
    db.execute(
        1,
        "update slots set backlink = 'WS.001.1a' where slotname = 'PS.base.a1'::bpchar",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select slotname, backlink from slots order by slotname"
        ),
        vec![vec![
            Value::Text("PS.base.a1          ".into()),
            Value::Text("WS.001.1a           ".into())
        ]]
    );
}

#[test]
fn reciprocal_bpchar_after_triggers_keep_one_visible_row() {
    let base = temp_dir("reciprocal_bpchar_after_triggers_one_visible");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table left_slots (slotname char(20), backlink char(20))",
    )
    .unwrap();
    db.execute(
        1,
        "create table right_slots (slotname char(20), backlink char(20))",
    )
    .unwrap();
    db.execute(1, "insert into right_slots values ('R1', '')")
        .unwrap();
    db.execute(
        1,
        "create function set_right(myname bpchar, blname bpchar) returns int4 language plpgsql as $$ declare rec record; begin select into rec * from right_slots where slotname = myname; if not found then raise exception '% missing', myname; end if; if rec.backlink != blname then update right_slots set backlink = blname where slotname = myname; end if; return 0; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function set_left(myname bpchar, blname bpchar) returns int4 language plpgsql as $$ declare rec record; begin select into rec * from left_slots where slotname = myname; if not found then raise exception '% missing', myname; end if; if rec.backlink != blname then update left_slots set backlink = blname where slotname = myname; end if; return 0; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function left_after() returns trigger language plpgsql as $$ declare dummy int4; begin if new.backlink != '' then dummy := set_right(new.backlink, new.slotname); end if; return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function right_after() returns trigger language plpgsql as $$ declare dummy int4; begin if new.backlink != '' then dummy := set_left(new.backlink, new.slotname); end if; return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function left_before_update() returns trigger language plpgsql as $$ begin if new.slotname != old.slotname then delete from left_slots where slotname = old.slotname; insert into left_slots values (new.slotname, new.backlink); return null; end if; return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function right_before_update() returns trigger language plpgsql as $$ begin if new.slotname != old.slotname then delete from right_slots where slotname = old.slotname; insert into right_slots values (new.slotname, new.backlink); return null; end if; return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger left_before_update before update on left_slots for each row execute function left_before_update()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger right_before_update before update on right_slots for each row execute function right_before_update()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger left_after after insert or update on left_slots for each row execute function left_after()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger right_after after insert or update on right_slots for each row execute function right_after()",
    )
    .unwrap();

    db.execute(1, "insert into left_slots values ('L1', 'R1')")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select slotname, backlink from left_slots order by slotname"
        ),
        vec![vec![
            Value::Text("L1                  ".into()),
            Value::Text("R1                  ".into())
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select slotname, backlink from right_slots order by slotname"
        ),
        vec![vec![
            Value::Text("R1                  ".into()),
            Value::Text("L1                  ".into())
        ]]
    );
}

#[test]
fn bpchar_before_update_trigger_does_not_treat_unchanged_key_as_renamed() {
    let base = temp_dir("bpchar_before_update_same_key_not_renamed");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table slots (slotname char(20), backlink char(20))",
    )
    .unwrap();
    db.execute(
        1,
        "create function slots_bu() returns trigger language plpgsql as $$ begin if new.slotname != old.slotname then delete from slots where slotname = old.slotname; insert into slots values (new.slotname, new.backlink); return null; end if; return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger slots_bu before update on slots for each row execute function slots_bu()",
    )
    .unwrap();
    db.execute(1, "insert into slots values ('S1', '')")
        .unwrap();
    db.execute(1, "update slots set backlink = 'B1' where slotname = 'S1'")
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select slotname, backlink from slots order by slotname"
        ),
        vec![vec![
            Value::Text("S1                  ".into()),
            Value::Text("B1                  ".into())
        ]]
    );
}

#[test]
fn plpgsql_update_maintains_each_visible_index_once() {
    let base = temp_dir("plpgsql_update_maintains_index_once");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table slots (slotname char(20), backlink char(20))",
    )
    .unwrap();
    db.execute(
        1,
        "create unique index slots_name_idx on slots using btree (slotname bpchar_ops)",
    )
    .unwrap();
    db.execute(1, "insert into slots values ('WS.001.1a', '')")
        .unwrap();
    db.execute(
        1,
        "create function set_backlink(myname bpchar, blname bpchar) returns int4 language plpgsql as $$ begin update slots set backlink = blname where slotname = myname; return 0; end $$",
    )
    .unwrap();
    db.execute(1, "select set_backlink('WS.001.1a', 'PS.base.a1')")
        .unwrap();

    let rows = query_rows(
        &db,
        1,
        "select slotname, backlink from slots order by slotname",
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Text("WS.001.1a           ".into()),
            Value::Text("PS.base.a1          ".into())
        ]],
    );
}

#[test]
fn unique_index_delete_then_reinsert_same_key_succeeds() {
    let base = temp_dir("unique_index_delete_then_reinsert_same_key");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();
    db.execute(1, "create unique index items_id_key on items (id)")
        .unwrap();
    db.execute(1, "delete from items where id = 1").unwrap();
    db.execute(1, "insert into items values (1, 'beta')")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 1"),
        vec![vec![Value::Text("beta".into())]]
    );
}

#[test]
fn indexed_delete_keeps_index_scans_correct() {
    let base = temp_dir("indexed_delete_keeps_index_scans_correct");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(
        1,
        "insert into items values (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
    )
    .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    db.execute(1, "delete from items where id = 2").unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select name from items where id = 2",
        "items_id_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select name from items where id = 2"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        query_rows(&db, 1, "select id from items order by id"),
        vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]
    );
}

#[test]
fn indexed_update_and_delete_apply_residual_predicates() {
    let base = temp_dir("indexed_dml_residual_predicates");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table items (id int4 not null, tag text, name text)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into items values (1, 'keep', 'alpha'), (1, 'skip', 'beta'), (2, 'keep', 'gamma')",
    )
    .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    db.execute(
        1,
        "update items set name = 'updated' where id = 1 and tag = 'keep'",
    )
    .unwrap();
    db.execute(1, "delete from items where id = 1 and tag = 'skip'")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, tag, name from items order by id, tag"),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("keep".into()),
                Value::Text("updated".into())
            ],
            vec![
                Value::Int32(2),
                Value::Text("keep".into()),
                Value::Text("gamma".into())
            ],
        ]
    );
}

#[test]
fn partial_unique_index_update_maintenance_tracks_predicate_boundary() {
    let base = temp_dir("partial_unique_index_update_boundary");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, flag text, note text)")
        .unwrap();
    db.execute(
        1,
        "insert into items values (1, 'skip', 'a'), (1, 'skip', 'b')",
    )
    .unwrap();
    db.execute(
        1,
        "create unique index items_keep_key on items (id) where flag = 'keep'",
    )
    .unwrap();

    db.execute(1, "update items set flag = 'keep' where note = 'a'")
        .unwrap();
    match db.execute(1, "update items set flag = 'keep' where note = 'b'") {
        Err(ExecError::UniqueViolation { constraint, .. }) => {
            assert_eq!(constraint, "items_keep_key");
        }
        other => panic!("expected unique violation, got {:?}", other),
    }

    db.execute(1, "update items set flag = 'skip' where note = 'a'")
        .unwrap();
    db.execute(1, "update items set flag = 'keep' where note = 'b'")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, flag, note from items order by note"),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("skip".into()),
                Value::Text("a".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Text("keep".into()),
                Value::Text("b".into()),
            ],
        ]
    );
}

#[test]
fn indexed_truncate_reinitializes_indexes() {
    let base = temp_dir("indexed_truncate_reinitializes_indexes");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4 not null, name text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha'), (2, 'beta')")
        .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    db.execute(1, "truncate items").unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select name from items where id = 1",
        "items_id_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(0)]]
    );

    db.execute(1, "insert into items values (3, 'gamma')")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select name from items where id = 3"),
        vec![vec![Value::Text("gamma".into())]]
    );
}

#[test]
fn concurrent_indexed_inserts_and_lookups_remain_correct() {
    let base = temp_dir("concurrent_indexed_inserts_and_lookups");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    let writers: Vec<_> = (0..4)
        .map(|worker| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..60 {
                    let id = worker * 1000 + i;
                    db.execute(
                        (worker + 10) as ClientId,
                        &format!("insert into items values ({id}, 'w{worker}-{i}')"),
                    )
                    .unwrap();
                    let rows =
                        query_rows(&db, 1, &format!("select note from items where id = {id}"));
                    assert_eq!(rows.len(), 1, "expected one row for id {id}, got {rows:?}");
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..2)
        .map(|reader| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..60 {
                    let id = (i % 60) as i32;
                    db.execute(
                        (reader + 100) as ClientId,
                        &format!("select note from items where id = {id}"),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(writers, HEAVY_CONTENTION_TEST_TIMEOUT);
    join_all_with_timeout(readers, HEAVY_CONTENTION_TEST_TIMEOUT);

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(240)]]
    );
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where id = 1005",
        "items_id_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 1005"),
        vec![vec![Value::Text("w1-5".into())]]
    );
}

#[test]
fn concurrent_indexed_inserts_and_range_scans_survive_splits() {
    let base = temp_dir("concurrent_indexed_inserts_and_range_scans_survive_splits");
    let db = Database::open(&base, 128).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    let writers: Vec<_> = (0..4)
        .map(|worker| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..120 {
                    let id = worker * 10_000 + i;
                    db.execute(
                        (worker + 20) as ClientId,
                        &format!("insert into items values ({id}, 'w{worker}-{i}')"),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..3)
        .map(|reader| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..40 {
                    let rows = query_rows(
                        &db,
                        (reader + 200) as ClientId,
                        "select id from items where id >= 0 order by id limit 20",
                    );
                    let ids = rows
                        .into_iter()
                        .map(|row| match &row[0] {
                            Value::Int32(v) => *v,
                            other => panic!("expected int row, got {:?}", other),
                        })
                        .collect::<Vec<_>>();
                    assert!(
                        ids.windows(2).all(|w| w[0] <= w[1]),
                        "range scan returned unsorted ids: {ids:?}"
                    );
                }
            })
        })
        .collect();

    join_all_with_timeout(writers, HEAVY_CONTENTION_TEST_TIMEOUT);
    join_all_with_timeout(readers, HEAVY_CONTENTION_TEST_TIMEOUT);

    assert_explain_uses_index(
        &db,
        1,
        "select id from items where id >= 0 order by id limit 20",
        "items_id_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(480)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 30042"),
        vec![vec![Value::Text("w3-42".into())]]
    );
}

#[test]
fn concurrent_unique_index_inserts_only_allow_one_live_key() {
    let base = temp_dir("concurrent_unique_index_inserts_only_allow_one_live_key");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "create unique index items_id_key on items (id)")
        .unwrap();

    let handles: Vec<_> = (0..8)
        .map(|worker| {
            let db = db.clone();
            thread::spawn(move || {
                db.execute(
                    (worker + 300) as ClientId,
                    &format!("insert into items values (1, 'worker{worker}')"),
                )
            })
        })
        .collect();

    let mut successes = 0usize;
    let mut violations = 0usize;
    for handle in handles {
        match handle.join().unwrap() {
            Ok(StatementResult::AffectedRows(1)) => successes += 1,
            Err(ExecError::UniqueViolation { constraint, .. }) => {
                assert_eq!(constraint, "items_id_key");
                violations += 1;
            }
            other => panic!("unexpected concurrent insert result: {:?}", other),
        }
    }

    assert_eq!(successes, 1, "expected one successful insert");
    assert_eq!(violations, 7, "expected seven unique violations");
    assert_eq!(
        query_rows(&db, 1, "select count(*) from items where id = 1"),
        vec![vec![Value::Int64(1)]]
    );
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where id = 1",
        "items_id_key",
    );
}

#[test]
fn concurrent_indexed_updates_and_deletes_keep_index_results_correct() {
    let base = temp_dir("concurrent_indexed_updates_and_deletes_keep_index_results_correct");
    let db = Database::open(&base, 128).unwrap();

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    for i in 0..120 {
        db.execute(1, &format!("insert into items values ({i}, 'row{i}')"))
            .unwrap();
    }
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();

    let updaters: Vec<_> = (0..3)
        .map(|worker| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..20 {
                    let old_id = worker * 20 + i;
                    let new_id = 1000 + worker * 20 + i;
                    db.execute(
                        (worker + 400) as ClientId,
                        &format!(
                            "update items set id = {new_id}, note = 'u{worker}-{i}' where id = {old_id}"
                        ),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    let deleters: Vec<_> = (0..2)
        .map(|worker| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..15 {
                    let id = 60 + worker * 15 + i;
                    db.execute(
                        (worker + 500) as ClientId,
                        &format!("delete from items where id = {id}"),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(updaters, TEST_TIMEOUT);
    join_all_with_timeout(deleters, TEST_TIMEOUT);

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items"),
        vec![vec![Value::Int64(90)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 5"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 1005"),
        vec![vec![Value::Text("u0-5".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 74"),
        Vec::<Vec<Value>>::new()
    );
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where id = 1005",
        "items_id_idx",
    );
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where id = 74",
        "items_id_idx",
    );
}

#[test]
fn reopening_database_replays_btree_wal() {
    let base = temp_dir("reopening_database_replays_btree_wal");
    {
        let db = Database::open_with_options(&base, DatabaseOpenOptions::new(256)).unwrap();
        db.execute(1, "create table items (id int4 not null, note text)")
            .unwrap();
        db.execute(1, &insert_items_sql(0..400, "before")).unwrap();
        db.execute(1, "create index items_id_idx on items (id)")
            .unwrap();
        db.execute(1, &insert_items_sql(400..900, "after")).unwrap();
        assert_explain_uses_index(
            &db,
            1,
            "select note from items where id = 777",
            "items_id_idx",
        );
    }

    let reopened = Database::open_with_options(&base, DatabaseOpenOptions::new(256)).unwrap();
    assert_explain_uses_index(
        &reopened,
        1,
        "select note from items where id = 777",
        "items_id_idx",
    );
    assert_eq!(
        query_rows(&reopened, 1, "select note from items where id = 777"),
        vec![vec![Value::Text("after777".into())]]
    );
    assert_eq!(
        query_rows(&reopened, 1, "select count(*) from items where id >= 890"),
        vec![vec![Value::Int64(10)]]
    );
}

#[test]
fn durable_open_bootstraps_control_file_and_clean_shutdown_marks_shutdown() {
    use crate::backend::access::transam::{ControlFileState, ControlFileStore};

    let base = scratch_temp_dir("control_file_bootstrap");
    let control_path = ControlFileStore::path(&base);

    {
        let db = Database::open_with_options(&base, DatabaseOpenOptions::new(32)).unwrap();
        assert!(control_path.exists(), "expected control file to be created");
        let raw = std::fs::read(&control_path).unwrap();
        assert_ne!(raw.first(), Some(&b'{'));
        let control = ControlFileStore::load(&base).unwrap().snapshot();
        assert_eq!(control.state, ControlFileState::InProduction);
        assert_eq!(control.next_xid, db.txns.read().next_xid());
    }

    let control = ControlFileStore::load(&base).unwrap().snapshot();
    assert_eq!(control.state, ControlFileState::ShutDown);
}

#[test]
fn durable_open_ignores_non_cluster_files_in_empty_data_dir() {
    use crate::backend::access::transam::ControlFileStore;

    let base = scratch_temp_dir("control_file_ignores_junk");
    std::fs::write(base.join("server.log"), b"bootstrap probe").unwrap();

    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(32)).unwrap();
    assert!(ControlFileStore::path(&base).exists());
    assert_eq!(query_rows(&db, 1, "select 1"), vec![vec![Value::Int32(1)]]);
}

#[test]
fn vacuum_records_recyclable_btree_pages_in_fsm() {
    let base = temp_dir("vacuum_recycles_btree_pages");
    let db = Database::open(&base, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null, note text)")
        .unwrap();
    session
        .execute(&db, &insert_items_sql(0..1500, "row"))
        .unwrap();
    session
        .execute(&db, "create index items_id_idx on items (id)")
        .unwrap();

    let index_rel = relation_locator_for(&db, 1, "items_id_idx");
    session.execute(&db, &delete_items_before_sql(900)).unwrap();
    session.execute(&db, "vacuum items").unwrap();

    let fsm_page = read_relation_fork_block(
        &db,
        index_rel,
        crate::backend::storage::smgr::ForkNumber::Fsm,
        0,
    );
    let free_count = u32::from_le_bytes(fsm_page[0..4].try_into().unwrap());
    assert!(
        free_count > 0,
        "expected VACUUM to record reusable index pages in _fsm"
    );

    assert_eq!(
        query_rows(&db, 1, "select count(*) from items where id >= 900"),
        vec![vec![Value::Int64(600)]]
    );
}

#[test]
fn vacuum_reused_btree_pages_prevent_relation_growth() {
    let base = temp_dir("vacuum_reused_btree_pages_prevent_relation_growth");
    let db = Database::open(&base, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null, note text)")
        .unwrap();
    session
        .execute(&db, &insert_items_sql(0..1800, "row"))
        .unwrap();
    session
        .execute(&db, "create index items_id_idx on items (id)")
        .unwrap();

    let index_rel = relation_locator_for(&db, 1, "items_id_idx");
    session
        .execute(&db, &delete_items_before_sql(1200))
        .unwrap();
    session.execute(&db, "vacuum items").unwrap();

    let blocks_after_vacuum = relation_fork_nblocks(
        &db,
        index_rel,
        crate::backend::storage::smgr::ForkNumber::Main,
    );

    session
        .execute(&db, &insert_items_sql(2000..2600, "row"))
        .unwrap();

    let blocks_after_reinsert = relation_fork_nblocks(
        &db,
        index_rel,
        crate::backend::storage::smgr::ForkNumber::Main,
    );
    assert_eq!(
        blocks_after_reinsert, blocks_after_vacuum,
        "expected post-vacuum inserts to reuse deleted btree pages before extending the index"
    );
}

#[test]
fn create_index_respects_maintenance_work_mem_budget() {
    let base = temp_dir("btree_index_work_mem");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null, name text)")
        .unwrap();
    for i in 0..200 {
        session
            .execute(
                &db,
                &format!("insert into items values ({i}, '{}')", "x".repeat(64)),
            )
            .unwrap();
    }
    session
        .execute(&db, "set maintenance_work_mem = '1kB'")
        .unwrap();

    match session.execute(&db, "create index items_id_idx on items (id)") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. })) => {
            assert!(
                actual.contains("index build failed"),
                "expected build failure, got {actual}"
            );
        }
        other => panic!(
            "expected maintenance_work_mem build failure, got {:?}",
            other
        ),
    }
}

#[test]
fn checkpoint_gucs_show_defaults_and_reject_runtime_set() {
    let base = temp_dir("checkpoint_gucs");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    match session.execute(&db, "show checkpoint_timeout").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("5min".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "show max_wal_size").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("1GB".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "set checkpoint_timeout = '10min'") {
        Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(name))) => {
            assert_eq!(name, "checkpoint_timeout");
        }
        other => panic!("expected checkpoint_timeout runtime change error, got {other:?}"),
    }

    match session.execute(&db, "reset checkpoint_timeout") {
        Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(name))) => {
            assert_eq!(name, "checkpoint_timeout");
        }
        other => panic!("expected checkpoint_timeout reset error, got {other:?}"),
    }
}

#[test]
fn autovacuum_gucs_show_defaults_and_reject_runtime_set() {
    assert!(AutovacuumConfig::production_default().enabled);
    assert!(!DatabaseOpenOptions::for_tests(16).autovacuum.enabled);

    let base = temp_dir("autovacuum_gucs");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    match session.execute(&db, "show autovacuum").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("off".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session
        .execute(&db, "show autovacuum_vacuum_threshold")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("50".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "set autovacuum = off") {
        Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(name))) => {
            assert_eq!(name, "autovacuum");
        }
        other => panic!("expected autovacuum runtime change error, got {other:?}"),
    }

    match session.execute(&db, "reset autovacuum_vacuum_threshold") {
        Err(ExecError::Parse(ParseError::CantChangeRuntimeParam(name))) => {
            assert_eq!(name, "autovacuum_vacuum_threshold");
        }
        other => panic!("expected autovacuum reset error, got {other:?}"),
    }
}

#[test]
fn stats_gucs_show_postgres_like_defaults_and_runtime_values() {
    let base = temp_dir("stats_gucs_show_defaults");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    match session.execute(&db, "show track_counts").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("on".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "show track_functions").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("none".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session
        .execute(&db, "show stats_fetch_consistency")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("cache".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    session.execute(&db, "set track_functions = 'all'").unwrap();
    session
        .execute(&db, "set stats_fetch_consistency = snapshot")
        .unwrap();

    match session.execute(&db, "show track_functions").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("all".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session
        .execute(&db, "show stats_fetch_consistency")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("snapshot".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn plpgsql_gucs_show_set_current_setting_and_drive_asserts() {
    let base = temp_dir("plpgsql_gucs");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    assert_eq!(
        session_query_rows(&mut session, &db, "show plpgsql.check_asserts"),
        vec![vec![Value::Text("on".into())]]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "show plpgsql.extra_warnings"),
        vec![vec![Value::Text("none".into())]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select current_setting('plpgsql.variable_conflict')",
        ),
        vec![vec![Value::Text("error".into())]]
    );

    session
        .execute(&db, "set plpgsql.check_asserts = off")
        .unwrap();
    session
        .execute(
            &db,
            "set plpgsql.extra_warnings = 'shadowed_variables,too_many_rows'",
        )
        .unwrap();
    session
        .execute(&db, "set plpgsql.variable_conflict = use_column")
        .unwrap();

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select current_setting('plpgsql.check_asserts'), current_setting('plpgsql.extra_warnings'), current_setting('plpgsql.variable_conflict')",
        ),
        vec![vec![
            Value::Text("off".into()),
            Value::Text("shadowed_variables,too_many_rows".into()),
            Value::Text("use_column".into()),
        ]]
    );
    assert_eq!(
        session
            .execute(&db, "do $$ begin assert false, 'disabled'; end $$")
            .unwrap(),
        StatementResult::AffectedRows(0)
    );

    match session.execute(&db, "set plpgsql.variable_conflict = bogus") {
        Err(ExecError::Parse(ParseError::UnrecognizedParameter(value))) => {
            assert_eq!(value, "bogus");
        }
        other => panic!("expected invalid plpgsql.variable_conflict error, got {other:?}"),
    }
}

#[test]
fn stats_snapshot_timestamp_requires_snapshot_mode_and_clear_snapshot_resets_it() {
    let base = temp_dir("stats_snapshot_timestamp");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "begin").unwrap();

    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_get_snapshot_timestamp()"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_get_function_calls(0)"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_get_snapshot_timestamp()"),
        vec![vec![Value::Null]]
    );

    session
        .execute(&db, "set local stats_fetch_consistency = snapshot")
        .unwrap();

    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_get_snapshot_timestamp()"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_get_function_calls(0)"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select pg_stat_get_snapshot_timestamp() is not null",
        ),
        vec![vec![Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_clear_snapshot()"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select pg_stat_get_snapshot_timestamp()"),
        vec![vec![Value::Null]]
    );

    session.execute(&db, "commit").unwrap();
}

#[test]
fn stats_fetch_consistency_cache_holds_cached_relation_entry_until_clear() {
    let base = temp_dir("stats_cache_entry_visibility");
    let db = Database::open(&base, 16).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    writer
        .execute(&db, "create table items (id int4 primary key)")
        .unwrap();
    writer
        .execute(&db, "set stats_fetch_consistency = none")
        .unwrap();
    reader
        .execute(&db, "set stats_fetch_consistency = cache")
        .unwrap();
    reader.execute(&db, "begin").unwrap();

    assert_eq!(
        session_query_rows(
            &mut reader,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(0)]]
    );

    writer.execute(&db, "insert into items values (1)").unwrap();
    assert_eq!(
        session_query_rows(&mut writer, &db, "select pg_stat_force_next_flush()"),
        vec![vec![Value::Null]]
    );

    assert_eq!(
        session_query_rows(
            &mut reader,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        session_query_rows(&mut reader, &db, "select pg_stat_clear_snapshot()"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(
            &mut reader,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(1)]]
    );
    reader.execute(&db, "commit").unwrap();
}

#[test]
fn stats_snapshot_holds_unseen_relation_entries_stable_until_clear() {
    let base = temp_dir("stats_snapshot_unseen_entry_visibility");
    let db = Database::open(&base, 16).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    writer
        .execute(&db, "create table items (id int4 primary key)")
        .unwrap();
    writer
        .execute(&db, "create table other_items (id int4 primary key)")
        .unwrap();
    writer
        .execute(&db, "set stats_fetch_consistency = none")
        .unwrap();

    reader.execute(&db, "begin").unwrap();
    reader
        .execute(&db, "set local stats_fetch_consistency = snapshot")
        .unwrap();

    assert_eq!(
        session_query_rows(
            &mut reader,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(0)]]
    );

    writer
        .execute(&db, "insert into other_items values (1)")
        .unwrap();
    assert_eq!(
        session_query_rows(&mut writer, &db, "select pg_stat_force_next_flush()"),
        vec![vec![Value::Null]]
    );

    assert_eq!(
        session_query_rows(
            &mut reader,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'other_items'",
        ),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        session_query_rows(&mut reader, &db, "select pg_stat_clear_snapshot()"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(
            &mut reader,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'other_items'",
        ),
        vec![vec![Value::Int64(1)]]
    );
    reader.execute(&db, "commit").unwrap();
}

#[test]
fn set_local_time_zone_updates_timestamptz_json_output() {
    let base = temp_dir("set_local_time_zone_jsonb");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "begin").unwrap();
    session.execute(&db, "set local time zone 10.5").unwrap();

    assert_eq!(
        session_query_rows(&mut session, &db, "show timezone"),
        vec![vec![Value::Text("+10:30".into())]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select timestamptz '2014-05-28 12:22:35.614298-04'::text",
        ),
        vec![vec![Value::Text("2014-05-29 02:52:35.614298+10:30".into())]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select to_jsonb(timestamptz '2014-05-28 12:22:35.614298-04')",
        ),
        vec![vec![Value::Jsonb(
            crate::backend::executor::jsonb::parse_jsonb_text(
                "\"2014-05-29T02:52:35.614298+10:30\"",
            )
            .unwrap()
        )]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select to_jsonb(timestamp '2014-05-28 12:22:35.614298')",
        ),
        vec![vec![Value::Jsonb(
            crate::backend::executor::jsonb::parse_jsonb_text("\"2014-05-28T12:22:35.614298\"",)
                .unwrap()
        )]]
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select to_jsonb(date '2014-05-28')"),
        vec![vec![Value::Jsonb(
            crate::backend::executor::jsonb::parse_jsonb_text("\"2014-05-28\"").unwrap()
        )]]
    );

    session.execute(&db, "rollback").unwrap();
}

#[test]
fn pg_my_temp_schema_filters_temp_pg_stats_rows() {
    let base = temp_dir("pg_my_temp_schema_pg_stats");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create temp table rows as
             select x, 'txt' || x as y
             from generate_series(1, 3) as x",
        )
        .unwrap();
    session.execute(&db, "analyze rows").unwrap();

    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select pg_my_temp_schema()::regnamespace::text"
        ),
        vec![vec![Value::Text("pg_temp_1".into())]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select attname, to_jsonb(histogram_bounds)
             from pg_stats
             where tablename = 'rows'
               and schemaname = pg_my_temp_schema()::regnamespace::text
             order by 1",
        ),
        vec![
            vec![
                Value::Text("x".into()),
                Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("[1,2,3]").unwrap()),
            ],
            vec![
                Value::Text("y".into()),
                Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text(
                        "[\"txt1\",\"txt2\",\"txt3\"]"
                    )
                    .unwrap()
                ),
            ],
        ]
    );
}

#[test]
fn relation_stats_views_track_commit_flush_and_rollback() {
    let base = temp_dir("relation_stats_views");
    let db = Database::open(&base, 16).unwrap();
    let mut session1 = Session::new(1);
    let mut session2 = Session::new(2);

    session1
        .execute(&db, "set stats_fetch_consistency = none")
        .unwrap();
    session2
        .execute(&db, "set stats_fetch_consistency = none")
        .unwrap();

    session1
        .execute(&db, "create table items (id int4 primary key, note text)")
        .unwrap();
    session1
        .execute(&db, "create index items_note_idx on items(note)")
        .unwrap();
    session1
        .execute(&db, "insert into items values (1, 'a'), (2, 'b')")
        .unwrap();

    assert_eq!(
        session_query_rows(
            &mut session1,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(2)]]
    );
    assert_eq!(
        session_query_rows(
            &mut session2,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(0)]]
    );

    assert_eq!(
        session_query_rows(&mut session1, &db, "select count(*) from items"),
        vec![vec![Value::Int64(2)]]
    );
    assert_eq!(
        session_query_rows(
            &mut session1,
            &db,
            "select seq_scan > 0, seq_tup_read >= 2 from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(true)]]
    );
    assert_eq!(
        session_query_rows(
            &mut session1,
            &db,
            "select heap_blks_read + heap_blks_hit > 0 from pg_statio_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Bool(true)]]
    );

    assert_eq!(
        session_query_rows(&mut session1, &db, "select pg_stat_force_next_flush()"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        session_query_rows(
            &mut session2,
            &db,
            "select n_tup_ins, seq_scan > 0 from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(2), Value::Bool(true)]]
    );

    session1.execute(&db, "begin").unwrap();
    session1
        .execute(&db, "insert into items values (3, 'c')")
        .unwrap();
    session1.execute(&db, "rollback").unwrap();
    session_query_rows(&mut session1, &db, "select pg_stat_force_next_flush()");

    assert_eq!(
        session_query_rows(
            &mut session2,
            &db,
            "select n_tup_ins from pg_stat_user_tables where relname = 'items'",
        ),
        vec![vec![Value::Int64(2)]]
    );
}

#[test]
fn function_stats_respect_track_functions_and_rollback() {
    let base = temp_dir("function_stats_views");
    let db = Database::open(&base, 16).unwrap();
    let mut session1 = Session::new(1);
    let mut session2 = Session::new(2);

    session1
        .execute(&db, "set stats_fetch_consistency = none")
        .unwrap();
    session2
        .execute(&db, "set stats_fetch_consistency = none")
        .unwrap();

    session1
        .execute(
            &db,
            "create function add_one(n int4) returns int4 language plpgsql as $$ begin return n + 1; end $$",
        )
        .unwrap();

    assert_eq!(
        session_query_rows(&mut session1, &db, "select add_one(1)"),
        vec![vec![Value::Int32(2)]]
    );
    assert_eq!(
        session_query_rows(
            &mut session1,
            &db,
            "select count(*) from pg_stat_user_functions where funcname = 'add_one'",
        ),
        vec![vec![Value::Int64(0)]]
    );

    session1.execute(&db, "set track_functions = all").unwrap();
    assert_eq!(
        session_query_rows(&mut session1, &db, "select add_one(4), add_one(5)"),
        vec![vec![Value::Int32(5), Value::Int32(6)]]
    );
    assert_eq!(
        session_query_rows(
            &mut session1,
            &db,
            "select calls, total_time is not null, self_time is not null from pg_stat_user_functions where funcname = 'add_one'",
        ),
        vec![vec![Value::Int64(2), Value::Bool(true), Value::Bool(true)]]
    );

    session1.execute(&db, "begin").unwrap();
    assert_eq!(
        session_query_rows(&mut session1, &db, "select add_one(9)"),
        vec![vec![Value::Int32(10)]]
    );
    session1.execute(&db, "rollback").unwrap();

    assert_eq!(
        session_query_rows(
            &mut session1,
            &db,
            "select pg_stat_get_function_calls('add_one(int4)'::regprocedure::oid)",
        ),
        vec![vec![Value::Int64(2)]]
    );

    session_query_rows(&mut session1, &db, "select pg_stat_force_next_flush()");
    assert_eq!(
        session_query_rows(
            &mut session2,
            &db,
            "select calls from pg_stat_user_functions where funcname = 'add_one'",
        ),
        vec![vec![Value::Int64(2)]]
    );
}

#[test]
fn pg_stat_io_exposes_pg_shaped_rows() {
    let base = temp_dir("pg_stat_io_rows");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "set stats_fetch_consistency = none")
        .unwrap();
    session
        .execute(&db, "create table items (id int4)")
        .unwrap();
    session
        .execute(&db, "insert into items values (1), (2), (3)")
        .unwrap();
    let _ = session_query_rows(&mut session, &db, "select * from items");

    assert_eq!(
        session_query_rows(&mut session, &db, "select count(*) from pg_stat_io"),
        vec![vec![Value::Int64(79)]]
    );
    assert_eq!(
        session_query_rows(
            &mut session,
            &db,
            "select reads > 0, read_bytes > 0 from pg_stat_io where backend_type = 'client backend' and object = 'relation' and context = 'bulkread'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(true)]]
    );
}

#[test]
fn checkpoint_gucs_load_from_postgresql_conf_and_auto_conf() {
    use crate::backend::access::transam::ControlFileStore;

    let base = temp_dir("checkpoint_guc_files");
    std::fs::write(
        base.join("postgresql.conf"),
        "checkpoint_timeout = '7min'\nmax_wal_size = '64MB'\nfull_page_writes = off\n",
    )
    .unwrap();
    std::fs::write(
        base.join("postgresql.auto.conf"),
        "checkpoint_timeout = '9min'\nmin_wal_size = '16MB'\n",
    )
    .unwrap();

    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    match session.execute(&db, "show checkpoint_timeout").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("9min".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "show max_wal_size").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("64MB".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "show min_wal_size").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("16MB".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "show full_page_writes").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("off".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let control = ControlFileStore::load(&base).unwrap().snapshot();
    assert!(!control.full_page_writes);
}

#[test]
fn checkpoint_requires_pg_checkpoint_membership() {
    let base = temp_dir("checkpoint_privileges");
    let db = Database::open(&base, 16).unwrap();
    let mut bootstrap = Session::new(1);

    bootstrap.execute(&db, "create role tenant login").unwrap();
    bootstrap
        .execute(&db, "create role outsider login")
        .unwrap();
    bootstrap
        .execute(&db, "grant pg_checkpoint to tenant")
        .unwrap();

    let mut session = Session::new(2);
    session
        .execute(&db, "set session authorization tenant")
        .unwrap();
    assert_eq!(
        session.execute(&db, "checkpoint").unwrap(),
        StatementResult::AffectedRows(0)
    );

    session
        .execute(&db, "set session authorization outsider")
        .unwrap();
    match session.execute(&db, "checkpoint") {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(message, "permission denied to execute CHECKPOINT command");
            assert_eq!(sqlstate, "42501");
            assert_eq!(
                detail.as_deref(),
                Some(
                    "Only roles with privileges of the \"pg_checkpoint\" role may execute this command."
                )
            );
        }
        other => panic!("expected checkpoint privilege error, got {other:?}"),
    }
}

#[test]
fn checkpoint_updates_checkpointer_stats() {
    let base = temp_dir("checkpoint_stats");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    match session
        .execute(
            &db,
            "select pg_stat_get_checkpointer_num_requested(), \
             pg_stat_get_checkpointer_num_performed(), \
             pg_stat_get_checkpointer_num_timed()",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(0), Value::Int64(0), Value::Int64(0)]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    session.execute(&db, "checkpoint").unwrap();

    match session
        .execute(
            &db,
            "select pg_stat_get_checkpointer_num_requested(), \
             pg_stat_get_checkpointer_num_performed(), \
             pg_stat_get_checkpointer_write_time(), \
             pg_stat_get_checkpointer_sync_time(), \
             pg_stat_get_checkpointer_stat_reset_time()",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Int64(1));
            assert_eq!(rows[0][1], Value::Int64(1));
            assert!(matches!(rows[0][2], Value::Float64(value) if value >= 0.0));
            assert!(matches!(rows[0][3], Value::Float64(value) if value >= 0.0));
            assert!(matches!(rows[0][4], Value::TimestampTz(_)));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn checkpoint_flushes_dirty_pages_and_clog_to_disk() {
    use crate::backend::access::transam::xact::{
        INVALID_TRANSACTION_ID, TransactionManager, TransactionStatus,
    };
    use crate::backend::storage::smgr::ForkNumber;

    let base = temp_dir("checkpoint_flushes_dirty_pages");
    let committed_xid;
    let rel;
    let buffer_page;
    let buffer_id;

    {
        let db = Database::open(&base, 64).unwrap();
        let mut session = Session::new(1);

        session
            .execute(&db, "create table items (id int4, note text)")
            .unwrap();
        session
            .execute(&db, "insert into items values (1, 'alpha')")
            .unwrap();

        rel = relation_locator_for(&db, 1, "items");
        let pinned = db
            .pool
            .pin_existing_block(1, rel, ForkNumber::Main, 0)
            .unwrap();
        buffer_id = pinned.buffer_id();
        buffer_page = db.pool.read_page(buffer_id).unwrap();
        db.pool.mark_dirty(buffer_id).unwrap();
        assert!(
            db.pool.buffer_state(buffer_id).unwrap().dirty,
            "expected test heap page to be marked dirty before CHECKPOINT"
        );
        drop(pinned);

        committed_xid = db
            .txns
            .read()
            .snapshot(INVALID_TRANSACTION_ID)
            .unwrap()
            .xmax
            - 1;

        session.execute(&db, "checkpoint").unwrap();

        assert!(
            !db.pool.buffer_state(buffer_id).unwrap().dirty,
            "expected CHECKPOINT to clear the dirty heap buffer"
        );
        let disk_page_after = read_relation_block(&db, rel, 0);
        assert_eq!(
            disk_page_after, buffer_page,
            "expected CHECKPOINT to flush the dirty heap page to disk"
        );
    }

    let durable_txns = TransactionManager::new_durable(&base).unwrap();
    assert_eq!(
        durable_txns.status(committed_xid),
        Some(TransactionStatus::Committed),
        "expected CHECKPOINT to persist committed CLOG state"
    );

    let reopened = Database::open_with_options(&base, DatabaseOpenOptions::new(64)).unwrap();
    assert_eq!(
        query_rows(&reopened, 1, "select id, note from items"),
        vec![vec![Value::Int32(1), Value::Text("alpha".into())]]
    );
}

#[test]
fn index_matrix_equality_search_uses_single_column_index() {
    let db = setup_index_matrix_db("index_matrix_eq_search");
    assert_explain_uses_index(&db, 1, "select note from items where a = 2", "items_a_idx");
    assert_eq!(
        query_rows(&db, 1, "select note from items where a = 2 order by note"),
        vec![
            vec![Value::Text("b1".into())],
            vec![Value::Text("b2".into())],
            vec![Value::Text("b3".into())],
        ]
    );
}

#[test]
fn index_matrix_equality_plus_range_uses_multicol_index() {
    let db = setup_index_matrix_db("index_matrix_eq_range");
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where a = 2 and b >= 25",
        "items_ab_idx",
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select note from items where a = 2 and b >= 25 order by b"
        ),
        vec![
            vec![Value::Text("b2".into())],
            vec![Value::Text("b3".into())],
        ]
    );
}

#[test]
fn index_matrix_order_only_uses_forward_index_scan() {
    let db = setup_index_matrix_db("index_matrix_order_forward");
    assert_explain_uses_index(&db, 1, "select a, b from items order by a", "items_a_idx");
    assert_eq!(
        query_rows(&db, 1, "select a, b from items order by a"),
        vec![
            vec![Value::Int32(1), Value::Int32(10)],
            vec![Value::Int32(1), Value::Int32(20)],
            vec![Value::Int32(2), Value::Int32(15)],
            vec![Value::Int32(2), Value::Int32(25)],
            vec![Value::Int32(2), Value::Int32(35)],
            vec![Value::Int32(3), Value::Int32(30)],
        ]
    );
}

#[test]
fn index_matrix_projection_over_ordered_index_keeps_order_without_sort() {
    let db = setup_index_matrix_db("index_matrix_order_projection");
    let lines = explain_lines(&db, 1, "select a + 1 from items order by a");
    let relfilenode = relfilenode_for(&db, 1, "items_a_idx");
    assert!(
        lines.iter().any(|line| {
            (line.contains("Index Scan") || line.contains("Index Only Scan"))
                && (line.contains(&format!("using rel {relfilenode} "))
                    || line.contains("using items_a_idx "))
        }),
        "expected ordered index scan, got {lines:?}"
    );
    assert!(
        !lines.iter().any(|line| line.contains("Sort")),
        "expected final projection to preserve ordering without a sort, got {lines:?}"
    );
    assert_eq!(
        query_rows(&db, 1, "select a + 1 from items order by a"),
        vec![
            vec![Value::Int32(2)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
            vec![Value::Int32(3)],
            vec![Value::Int32(3)],
            vec![Value::Int32(4)],
        ]
    );
}

#[test]
fn index_matrix_order_only_uses_backward_index_scan() {
    let db = setup_index_matrix_db("index_matrix_order_backward");
    assert_explain_uses_index(&db, 1, "select a from items order by a desc", "items_a_idx");
    assert_eq!(
        query_rows(&db, 1, "select a from items order by a desc"),
        vec![
            vec![Value::Int32(3)],
            vec![Value::Int32(2)],
            vec![Value::Int32(2)],
            vec![Value::Int32(2)],
            vec![Value::Int32(1)],
            vec![Value::Int32(1)],
        ]
    );
}

#[test]
fn inherited_minmax_explain_uses_desc_and_partial_child_indexes() {
    let base = temp_dir("inherited_minmax_desc_partial_indexes");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(1, "create table items1 () inherits (items)")
        .unwrap();
    db.execute(1, "create table items2 () inherits (items)")
        .unwrap();
    db.execute(1, "create table items3 () inherits (items)")
        .unwrap();
    db.execute(1, "create index items_id_idx on items (id)")
        .unwrap();
    db.execute(1, "create index items1_id_idx on items1 (id)")
        .unwrap();
    db.execute(1, "create index items2_id_desc_idx on items2 (id desc)")
        .unwrap();
    db.execute(
        1,
        "create index items3_id_partial_idx on items3 (id) where id is not null",
    )
    .unwrap();
    db.execute(1, "insert into items values (11), (12)")
        .unwrap();
    db.execute(1, "insert into items1 values (13), (14)")
        .unwrap();
    db.execute(1, "insert into items2 values (15), (16)")
        .unwrap();
    db.execute(1, "insert into items3 values (17), (18)")
        .unwrap();

    let sql = "select min(id), max(id) from items";
    assert_explain_uses_index(&db, 1, sql, "items_id_idx");
    assert_explain_uses_index(&db, 1, sql, "items1_id_idx");
    assert_explain_uses_index(&db, 1, sql, "items2_id_desc_idx");
    assert_explain_uses_index(&db, 1, sql, "items3_id_partial_idx");
    let lines = explain_lines(&db, 1, sql);
    assert!(
        lines.iter().any(|line| line.contains("Merge Append")),
        "expected inherited min/max rewrite to use Merge Append, got {lines:?}"
    );

    assert_eq!(
        query_rows(&db, 1, sql),
        vec![vec![Value::Int32(11), Value::Int32(18)]]
    );
}

#[test]
fn index_matrix_non_indexed_predicate_falls_back_to_seqscan() {
    let db = setup_index_matrix_db("index_matrix_non_indexed");
    assert_explain_uses_seqscan(&db, 1, "select note from items where c = 100", "items");
    assert_eq!(
        query_rows(&db, 1, "select note from items where c = 100 order by note"),
        vec![
            vec![Value::Text("a1".into())],
            vec![Value::Text("b1".into())],
            vec![Value::Text("c1".into())],
        ]
    );
}

#[test]
fn index_matrix_or_predicate_falls_back_to_seqscan() {
    let db = setup_index_matrix_db("index_matrix_or");
    assert_explain_uses_seqscan(
        &db,
        1,
        "select note from items where a = 1 or a = 2",
        "items",
    );
}

#[test]
fn index_matrix_mixed_direction_order_falls_back_to_seqscan() {
    let db = setup_index_matrix_db("index_matrix_mixed_order");
    assert_explain_uses_seqscan(&db, 1, "select a, b from items order by a, b desc", "items");
}

#[test]
fn index_matrix_picks_longest_qual_prefix_index() {
    let db = setup_index_matrix_db("index_matrix_longest_prefix");
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where a = 2 and b = 25",
        "items_ab_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where a = 2 and b = 25"),
        vec![vec![Value::Text("b2".into())]]
    );
}

#[test]
fn index_matrix_prefers_qual_index_over_order_only_index() {
    let db = setup_index_matrix_db("index_matrix_qual_over_order");
    assert_explain_uses_index(
        &db,
        1,
        "select a, b from items where b = 30 order by a",
        "items_ba_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select a, b from items where b = 30 order by a"),
        vec![vec![Value::Int32(3), Value::Int32(30)]]
    );
}

#[test]
fn index_matrix_prefers_order_removing_index_when_prefix_ties() {
    let db = setup_index_matrix_db("index_matrix_order_tiebreak");
    assert_explain_uses_index(
        &db,
        1,
        "select a, b from items where a = 2 order by b",
        "items_ab_idx",
    );
    let lines = explain_lines(&db, 1, "select a, b from items where a = 2 order by b");
    assert!(
        !lines.iter().any(|line| line.contains("Sort")),
        "expected order by removal after choosing items_ab_idx, got {lines:?}"
    );
    assert_eq!(
        query_rows(&db, 1, "select a, b from items where a = 2 order by b"),
        vec![
            vec![Value::Int32(2), Value::Int32(15)],
            vec![Value::Int32(2), Value::Int32(25)],
            vec![Value::Int32(2), Value::Int32(35)],
        ]
    );
}

#[test]
fn index_matrix_residual_filter_still_returns_correct_rows() {
    let db = setup_index_matrix_db("index_matrix_residual");
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where a = 2 and c = 200",
        "items_a_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where a = 2 and c = 200"),
        vec![vec![Value::Text("b2".into())]]
    );
}

#[test]
fn index_matrix_equality_search_on_second_column_uses_single_column_index() {
    let db = setup_index_matrix_db("index_matrix_eq_search_b");
    assert_explain_uses_index(&db, 1, "select note from items where b = 25", "items_b_idx");
    assert_eq!(
        query_rows(&db, 1, "select note from items where b = 25"),
        vec![vec![Value::Text("b2".into())]]
    );
}

#[test]
fn index_matrix_second_column_equality_plus_range_uses_ba_index() {
    let db = setup_index_matrix_db("index_matrix_eq_range_ba");
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where b = 25 and a >= 2",
        "items_ba_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select note from items where b = 25 and a >= 2"),
        vec![vec![Value::Text("b2".into())]]
    );
}

#[test]
fn index_matrix_range_only_on_first_column_uses_index() {
    let db = setup_index_matrix_db("index_matrix_range_only_first");
    assert_explain_uses_index(
        &db,
        1,
        "select a, b from items where a >= 2 order by a",
        "items_a_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select a, b from items where a >= 2 order by a, b"),
        vec![
            vec![Value::Int32(2), Value::Int32(15)],
            vec![Value::Int32(2), Value::Int32(25)],
            vec![Value::Int32(2), Value::Int32(35)],
            vec![Value::Int32(3), Value::Int32(30)],
        ]
    );
}

#[test]
fn index_matrix_order_by_two_columns_uses_matching_multicolumn_index() {
    let db = setup_index_matrix_db("index_matrix_order_two_cols");
    assert_explain_uses_index(
        &db,
        1,
        "select a, b from items order by a, b",
        "items_ab_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select a, b from items order by a, b"),
        vec![
            vec![Value::Int32(1), Value::Int32(10)],
            vec![Value::Int32(1), Value::Int32(20)],
            vec![Value::Int32(2), Value::Int32(15)],
            vec![Value::Int32(2), Value::Int32(25)],
            vec![Value::Int32(2), Value::Int32(35)],
            vec![Value::Int32(3), Value::Int32(30)],
        ]
    );
}

#[test]
fn index_matrix_order_by_two_columns_uses_matching_ba_index() {
    let db = setup_index_matrix_db("index_matrix_order_two_cols_ba");
    assert_explain_uses_index(
        &db,
        1,
        "select a, b from items order by b, a",
        "items_ba_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select a, b from items order by b, a"),
        vec![
            vec![Value::Int32(1), Value::Int32(10)],
            vec![Value::Int32(2), Value::Int32(15)],
            vec![Value::Int32(1), Value::Int32(20)],
            vec![Value::Int32(2), Value::Int32(25)],
            vec![Value::Int32(3), Value::Int32(30)],
            vec![Value::Int32(2), Value::Int32(35)],
        ]
    );
}

#[test]
fn index_matrix_order_by_second_column_desc_uses_backward_scan() {
    let db = setup_index_matrix_db("index_matrix_order_b_desc");
    assert_explain_uses_index(&db, 1, "select b from items order by b desc", "items_b_idx");
    assert_eq!(
        query_rows(&db, 1, "select b from items order by b desc"),
        vec![
            vec![Value::Int32(35)],
            vec![Value::Int32(30)],
            vec![Value::Int32(25)],
            vec![Value::Int32(20)],
            vec![Value::Int32(15)],
            vec![Value::Int32(10)],
        ]
    );
}

#[test]
fn index_matrix_order_by_non_indexed_column_falls_back_to_seqscan() {
    let db = setup_index_matrix_db("index_matrix_order_c");
    assert_explain_uses_seqscan(&db, 1, "select c from items order by c", "items");
}

#[test]
fn index_matrix_expression_predicate_falls_back_to_seqscan() {
    let db = setup_index_matrix_db("index_matrix_expression_predicate");
    assert_explain_uses_seqscan(&db, 1, "select note from items where a + 1 = 3", "items");
}

#[test]
fn index_matrix_equalities_on_multiple_indexes_tie_break_by_catalog_order() {
    let db = setup_index_matrix_db("index_matrix_catalog_tiebreak");
    assert_explain_uses_index(
        &db,
        1,
        "select note from items where a = 2 and b = 25",
        "items_ab_idx",
    );
}

#[test]
fn index_matrix_order_only_prefix_tie_breaks_by_catalog_order() {
    let db = setup_index_matrix_db("index_matrix_order_prefix_tiebreak");
    assert_explain_uses_index(&db, 1, "select b from items order by b", "items_b_idx");
}

#[test]
fn index_matrix_equality_then_desc_order_uses_matching_multicolumn_index() {
    let db = setup_index_matrix_db("index_matrix_eq_then_desc_order");
    assert_explain_uses_index(
        &db,
        1,
        "select b from items where a = 2 order by b desc",
        "items_ab_idx",
    );
    assert_eq!(
        query_rows(&db, 1, "select b from items where a = 2 order by b desc"),
        vec![
            vec![Value::Int32(35)],
            vec![Value::Int32(25)],
            vec![Value::Int32(15)],
        ]
    );
}

#[test]
fn index_matrix_insert_after_build_remains_queryable_via_index() {
    let db = setup_index_matrix_db("index_matrix_insert_after_build");
    db.execute(1, "insert into items values (4, 40, 400, 'd1')")
        .unwrap();
    assert_explain_uses_index(&db, 1, "select note from items where a = 4", "items_a_idx");
    assert_eq!(
        query_rows(&db, 1, "select note from items where a = 4"),
        vec![vec![Value::Text("d1".into())]]
    );
}

#[test]
fn partial_index_query_planner_uses_index_only_when_query_implies_predicate() {
    let db = setup_partial_index_matrix_db("partial_index_planner");

    assert_explain_uses_index(
        &db,
        1,
        "select note from items where flag = 'keep' and id = 2",
        "items_keep_idx",
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select note from items where flag = 'keep' and id = 2",
        ),
        vec![vec![Value::Text("k2".into())]]
    );

    assert_explain_uses_seqscan(&db, 1, "select note from items where id = 2", "items");
    assert_eq!(
        query_rows(&db, 1, "select note from items where id = 2 order by note"),
        vec![
            vec![Value::Text("k2".into())],
            vec![Value::Text("s2".into())],
        ]
    );
}

#[test]
fn partial_unique_index_on_conflict_respects_predicate() {
    let base = temp_dir("partial_unique_index_on_conflict");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, flag text, note text)")
        .unwrap();
    db.execute(
        1,
        "create unique index items_keep_key on items (id) where flag = 'keep'",
    )
    .unwrap();
    db.execute(1, "insert into items values (1, 'keep', 'alpha')")
        .unwrap();

    db.execute(
        1,
        "insert into items values (1, 'keep', 'beta') \
         on conflict (id) where flag = 'keep' \
         do update set note = excluded.note",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, flag, note from items order by flag, note"
        ),
        vec![vec![
            Value::Int32(1),
            Value::Text("keep".into()),
            Value::Text("beta".into()),
        ]]
    );

    db.execute(
        1,
        "insert into items values (1, 'skip', 'gamma') \
         on conflict (id) where flag = 'keep' \
         do update set note = excluded.note",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select id, flag, note from items order by flag, note"
        ),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("keep".into()),
                Value::Text("beta".into()),
            ],
            vec![
                Value::Int32(1),
                Value::Text("skip".into()),
                Value::Text("gamma".into()),
            ],
        ]
    );
}

#[test]
fn partial_index_with_ctid_predicate_builds_and_persists_catalog_predicate() {
    let base = temp_dir("partial_index_ctid_predicate");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "insert into items values (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
    )
    .unwrap();
    db.execute(
        1,
        "create index items_ctid_idx on items (id) where ctid >= '(0,1)'",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select indpred \
             from pg_index \
             where indexrelid = (select oid from pg_class where relname = 'items_ctid_idx')",
        ),
        vec![vec![Value::Text("ctid >= '(0,1)'".into())]]
    );
    assert_eq!(
        psql_index_definition(&db, 1, "items", "items_ctid_idx"),
        "CREATE INDEX items_ctid_idx ON items USING btree (id) WHERE (ctid >= '(0,1)')"
    );
}

#[test]
fn copy_from_rows_inserts_typed_rows() {
    let base = temp_dir("copy_from_rows");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(
        1,
        "create table pgbench_branches (bid int not null, bbalance int not null, filler text)",
    )
    .unwrap();

    let inserted = session
        .copy_from_rows(
            &db,
            "pgbench_branches",
            &[
                vec!["1".into(), "0".into(), "\\N".into()],
                vec!["2".into(), "5".into(), "branch".into()],
            ],
        )
        .unwrap();
    assert_eq!(inserted, 2);

    match db
        .execute(
            1,
            "select bid, bbalance, filler from pgbench_branches order by bid",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1), Value::Int32(0), Value::Null],
                    vec![
                        Value::Int32(2),
                        Value::Int32(5),
                        Value::Text("branch".into()),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn lazy_index_catalog_helpers_resolve_am_and_opclass_metadata() {
    let base = temp_dir("lazy_index_catalog_helpers");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table items(id int4, name text)")
        .unwrap();

    let btree =
        crate::backend::utils::cache::lsyscache::access_method_row_by_name(&db, 1, None, "btree")
            .unwrap();
    assert_eq!(btree.oid, crate::include::catalog::BTREE_AM_OID);

    let int4_opclass = crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
        &db,
        1,
        None,
        btree.oid,
        crate::include::catalog::INT4_TYPE_OID,
    )
    .unwrap();
    assert_eq!(
        int4_opclass.oid,
        crate::include::catalog::INT4_BTREE_OPCLASS_OID
    );
    let int4_amops = crate::backend::utils::cache::lsyscache::amop_rows_for_family(
        &db,
        1,
        None,
        int4_opclass.opcfamily,
    );
    assert_eq!(int4_amops.len(), 5);
    assert!(int4_amops.iter().any(|row| row.amopstrategy == 3));
    let int4_amprocs = crate::backend::utils::cache::lsyscache::amproc_rows_for_family(
        &db,
        1,
        None,
        int4_opclass.opcfamily,
    );
    assert!(int4_amprocs.iter().any(|row| row.amprocnum == 1));

    db.execute(1, "create index items_idx on items (id)")
        .unwrap();
    let heap_rel = db
        .lazy_catalog_lookup(1, None, None)
        .lookup_any_relation("items")
        .unwrap();
    let index_oids = crate::backend::utils::cache::lsyscache::index_relation_oids_for_heap(
        &db,
        1,
        None,
        heap_rel.relation_oid,
    );
    assert_eq!(index_oids.len(), 1);
}

#[test]
fn create_index_accepts_bpchar_typmods_with_bpchar_ops() {
    let base = temp_dir("bpchar_typmod_index_opclass");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table room(roomno char(8))").unwrap();
    db.execute(
        1,
        "create unique index room_rno on room using btree (roomno bpchar_ops)",
    )
    .unwrap();

    let rel = db
        .lazy_catalog_lookup(1, None, None)
        .lookup_any_relation("room")
        .unwrap();
    let index_oids = crate::backend::utils::cache::lsyscache::index_relation_oids_for_heap(
        &db,
        1,
        None,
        rel.relation_oid,
    );
    assert_eq!(index_oids.len(), 1);
    assert_eq!(
        query_rows(
            &db,
            1,
            "select indclass \
             from pg_index \
             where indexrelid = (select oid from pg_class where relname = 'room_rno')",
        ),
        vec![vec![Value::Text(
            crate::include::catalog::BPCHAR_BTREE_OPCLASS_OID
                .to_string()
                .into()
        )]]
    );
}

#[test]
fn create_function_accepts_bpchar_argument_types() {
    let base = temp_dir("create_function_bpchar_args");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create function bpchar_prefix(value bpchar) returns text \
         as $$ select substr(value, 1, 2) $$ language sql immutable",
    )
    .unwrap();

    match db
        .execute(1, "select bpchar_prefix('WS.001.1a'::char(20))")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("WS".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn btree_index_supports_builtin_nummultirange_keys() {
    let base = temp_dir("btree_nummultirange_keys");
    let db = Database::open(&base, 16).unwrap();

    let btree =
        crate::backend::utils::cache::lsyscache::access_method_row_by_name(&db, 1, None, "btree")
            .unwrap();
    let multirange_opclass =
        crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
            &db,
            1,
            None,
            btree.oid,
            crate::include::catalog::NUMMULTIRANGE_TYPE_OID,
        )
        .unwrap();
    assert_eq!(
        multirange_opclass.oid,
        crate::include::catalog::MULTIRANGE_BTREE_OPCLASS_OID
    );

    db.execute(1, "create table mr_items(nmr nummultirange)")
        .unwrap();
    db.execute(1, "create index mr_items_idx on mr_items (nmr)")
        .unwrap();
    db.execute(
        1,
        "insert into mr_items values (nummultirange(variadic '{}'::numrange[])), ('{[1.0,2.0)}')",
    )
    .unwrap();

    match db
        .execute(
            1,
            "select nmr::text from mr_items where nmr = '{}'::nummultirange order by nmr",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("{}".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn hash_index_supports_builtin_nummultirange_keys() {
    let base = temp_dir("hash_nummultirange_keys");
    let db = Database::open(&base, 16).unwrap();

    let hash =
        crate::backend::utils::cache::lsyscache::access_method_row_by_name(&db, 1, None, "hash")
            .unwrap();
    let multirange_opclass =
        crate::backend::utils::cache::lsyscache::default_opclass_for_am_and_type(
            &db,
            1,
            None,
            hash.oid,
            crate::include::catalog::NUMMULTIRANGE_TYPE_OID,
        )
        .unwrap();
    assert_eq!(
        multirange_opclass.oid,
        crate::include::catalog::MULTIRANGE_HASH_OPCLASS_OID
    );

    db.execute(1, "create table mr_hash_items(nmr nummultirange)")
        .unwrap();
    db.execute(
        1,
        "create index mr_hash_items_idx on mr_hash_items using hash (nmr)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into mr_hash_items values \
         (nummultirange(variadic '{}'::numrange[])), \
         ('{[1.0,2.0)}'), \
         ('{[1.0,2.0)}')",
    )
    .unwrap();

    assert_explain_uses_index(
        &db,
        1,
        "select nmr::text from mr_hash_items where nmr = '{}'::nummultirange",
        "mr_hash_items_idx",
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select nmr::text from mr_hash_items \
             where nmr = '{}'::nummultirange",
        ),
        vec![vec![Value::Text("{}".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from mr_hash_items \
             where nmr = '{[1.0,2.0)}'::nummultirange",
        ),
        vec![vec![Value::Int64(2)]]
    );
}

#[test]
fn create_operator_class_persists_catalog_rows() {
    let base = temp_dir("create_operator_class_rows");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create function part_hashint4_noop(value int4, seed int8) returns int8 as $$ select value + seed; $$ language sql strict immutable parallel safe",
    )
    .unwrap();
    db.execute(
        1,
        "create operator class part_test_int4_ops for type int4 using hash as operator 1 =, function 2 part_hashint4_noop(int4, int8)",
    )
    .unwrap();

    match db
        .execute(
            1,
            "select opcname, opcintype, opcdefault from pg_opclass where opcname = 'part_test_int4_ops'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("part_test_int4_ops".into()),
                    Value::Int64(i64::from(crate::include::catalog::INT4_TYPE_OID)),
                    Value::Bool(false),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(
            1,
            "select amopstrategy, amprocnum from pg_opclass c join pg_amop o on o.amopfamily = c.opcfamily join pg_amproc p on p.amprocfamily = c.opcfamily where c.opcname = 'part_test_int4_ops'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int16(1), Value::Int16(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_operator_bool_bool_regression_debug() {
    let base = temp_dir("create_operator_bool_bool_regression");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create function alter_op_test_fn(boolean, boolean) returns boolean as $$ select null::boolean; $$ language sql immutable",
    )
    .unwrap();
    db.execute(
        1,
        "create function customcontsel(internal, oid, internal, integer) returns float8 as 'contsel' language internal stable strict",
    )
    .unwrap();

    let xid = db.txns.write().begin();
    let cid = 0;
    let catalog = db.lazy_catalog_lookup(1, Some((xid, cid)), None);
    let proc_oid = catalog
        .proc_rows_by_name("alter_op_test_fn")
        .into_iter()
        .find(|row| row.proargtypes == "16 16")
        .expect("alter_op_test_fn(bool,bool)")
        .oid;
    let restrict_oid = catalog
        .proc_rows_by_name("customcontsel")
        .into_iter()
        .find(|row| row.proargtypes == "2281 26 2281 23")
        .expect("customcontsel(internal,oid,internal,int4)")
        .oid;
    let join_oid = catalog
        .proc_rows_by_name("contjoinsel")
        .into_iter()
        .find(|row| row.proargtypes == "2281 26 2281 21 2281")
        .expect("contjoinsel(internal,oid,internal,int2,internal)")
        .oid;
    let ctx = crate::backend::catalog::store::CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid,
        client_id: 1,
        waiter: Some(db.txn_waiter.clone()),
        interrupts: db.interrupt_state(1),
    };
    let row = crate::include::catalog::PgOperatorRow {
        oid: 0,
        oprname: "===".into(),
        oprnamespace: crate::include::catalog::PUBLIC_NAMESPACE_OID,
        oprowner: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        oprkind: 'b',
        oprcanmerge: true,
        oprcanhash: true,
        oprleft: crate::include::catalog::BOOL_TYPE_OID,
        oprright: crate::include::catalog::BOOL_TYPE_OID,
        oprresult: crate::include::catalog::BOOL_TYPE_OID,
        oprcom: 0,
        oprnegate: 0,
        oprcode: proc_oid,
        oprrest: restrict_oid,
        oprjoin: join_oid,
    };
    let (operator_oid, create_effect) = db
        .catalog
        .write()
        .create_operator_mvcc(row.clone(), &ctx)
        .unwrap();
    db.apply_catalog_mutation_effect_immediate(&create_effect)
        .unwrap();

    let mut current = row;
    current.oid = operator_oid;
    let mut updated = current.clone();
    updated.oprcom = operator_oid;
    let replace_ctx = crate::backend::catalog::store::CatalogWriteContext {
        pool: db.pool.clone(),
        txns: db.txns.clone(),
        xid,
        cid: cid.saturating_add(1),
        client_id: 1,
        waiter: Some(db.txn_waiter.clone()),
        interrupts: db.interrupt_state(1),
    };
    let replace_result = db
        .catalog
        .write()
        .replace_operator_mvcc(&current, updated, &replace_ctx);
    assert!(replace_result.is_ok(), "{replace_result:?}");
}

#[test]
fn copy_from_rows_respects_active_transaction() {
    let base = temp_dir("copy_from_rows_txn");
    let db = Database::open(&base, 16).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    db.execute(
            1,
            "create table pgbench_tellers (tid int not null, bid int not null, tbalance int not null, filler text)",
        )
        .unwrap();

    writer.execute(&db, "begin").unwrap();
    let inserted = writer
        .copy_from_rows(
            &db,
            "pgbench_tellers",
            &[vec!["10".into(), "1".into(), "0".into(), "\\N".into()]],
        )
        .unwrap();
    assert_eq!(inserted, 1);

    match reader
        .execute(&db, "select count(*) from pgbench_tellers")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    writer.execute(&db, "commit").unwrap();

    match reader
        .execute(
            &db,
            "select tid, bid, tbalance, filler from pgbench_tellers",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(10),
                    Value::Int32(1),
                    Value::Int32(0),
                    Value::Null,
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn copy_from_rows_parses_array_literals() {
    let base = temp_dir("copy_from_rows_arrays");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(
        1,
        "create table shipments (id int not null, tags varchar[], sizes int4[])",
    )
    .unwrap();

    let inserted = session
        .copy_from_rows(
            &db,
            "shipments",
            &[vec![
                "1".into(),
                "{\"a\",\"b\"}".into(),
                "{1,NULL,3}".into(),
            ]],
        )
        .unwrap();
    assert_eq!(inserted, 1);

    match db
        .execute(1, "select id, tags, sizes from shipments")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(1),
                    Value::PgArray(
                        crate::include::nodes::datum::ArrayValue::from_1d(vec![
                            Value::Text("a".into()),
                            Value::Text("b".into()),
                        ])
                        .with_element_type_oid(crate::include::catalog::VARCHAR_TYPE_OID),
                    ),
                    Value::PgArray(
                        crate::include::nodes::datum::ArrayValue::from_1d(vec![
                            Value::Int32(1),
                            Value::Null,
                            Value::Int32(3),
                        ])
                        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn copy_from_rows_parses_quoted_array_text_and_empty_arrays() {
    let base = temp_dir("copy_from_rows_arrays_quoted");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(
        1,
        "create table shipments (id int not null, tags varchar[], sizes int4[])",
    )
    .unwrap();

    session
        .copy_from_rows(
            &db,
            "shipments",
            &[
                vec!["1".into(), "{\"a,b\",\"c\\\"d\"}".into(), "{}".into()],
                vec!["2".into(), "{}".into(), "{NULL}".into()],
            ],
        )
        .unwrap();

    match db
        .execute(1, "select id, tags, sizes from shipments order by id")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::PgArray(
                            crate::include::nodes::datum::ArrayValue::from_1d(vec![
                                Value::Text("a,b".into()),
                                Value::Text("c\"d".into()),
                            ])
                            .with_element_type_oid(crate::include::catalog::VARCHAR_TYPE_OID),
                        ),
                        Value::PgArray(
                            crate::include::nodes::datum::ArrayValue::empty()
                                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                        ),
                    ],
                    vec![
                        Value::Int32(2),
                        Value::PgArray(
                            crate::include::nodes::datum::ArrayValue::empty()
                                .with_element_type_oid(crate::include::catalog::VARCHAR_TYPE_OID),
                        ),
                        Value::PgArray(
                            crate::include::nodes::datum::ArrayValue::from_1d(vec![Value::Null])
                                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                        ),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn temp_tables_are_session_local_and_mask_permanent_tables() {
    let base = temp_dir("temp_table_masking");
    let db = Database::open(&base, 16).unwrap();
    let mut session_a = Session::new(1);
    let mut session_b = Session::new(2);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "insert into items (id) values (1)").unwrap();

    session_a
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    let catalog = session_a.catalog_lookup(&db);
    let unqualified = catalog.lookup_any_relation("items").unwrap();
    let qualified = catalog.lookup_any_relation("public.items").unwrap();
    assert_ne!(unqualified.rel, qualified.rel);
    session_a
        .execute(&db, "insert into items (id) values (2)")
        .unwrap();

    match session_a.execute(&db, "select id from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session_b.execute(&db, "select id from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    session_a.execute(&db, "drop table items").unwrap();
    match session_a.execute(&db, "select id from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session_a
        .execute(&db, "select id from public.items")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn drop_table_supports_qualified_public_name_under_temp_shadowing() {
    let base = temp_dir("drop_table_public_under_temp_shadow");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "insert into items (id) values (1)").unwrap();
    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into items (id) values (2)")
        .unwrap();

    session.execute(&db, "drop table public.items").unwrap();

    match session.execute(&db, "select id from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let err = session
        .execute(&db, "select id from public.items")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::UnknownTable(name)) if name == "public.items"
    ));
}

#[test]
fn drop_table_if_exists_accepts_qualified_public_name_under_temp_shadowing() {
    let base = temp_dir("drop_table_if_exists_public_under_temp_shadow");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    session.execute(&db, "drop table public.items").unwrap();
    session
        .execute(&db, "drop table if exists public.items")
        .unwrap();

    match session.execute(&db, "select count(*) from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn temp_catalog_rows_appear_with_pg_temp_namespace() {
    let base = temp_dir("temp_catalog_rows");
    let db = Database::open(&base, 16).unwrap();
    let mut session_a = Session::new(1);
    let mut session_b = Session::new(2);

    session_a
        .execute(&db, "create temp table temp_items (id int4 not null)")
        .unwrap();

    match session_a
        .execute(
            &db,
            "select count(*) from pg_class where relname = 'temp_items'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session_a
            .execute(
                &db,
                "select n.nspname, c.relname, c.relpersistence from pg_class c join pg_namespace n on n.oid = c.relnamespace where c.relname = 'temp_items'",
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Text("pg_temp_1".into()),
                        Value::Text("temp_items".into()),
                        Value::Text("t".into()),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }

    match session_a
        .execute(
            &db,
            "select n.nspname, t.typname from pg_type t join pg_namespace n on n.oid = t.typnamespace where t.typname in ('temp_items', '_temp_items') order by t.typname",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Text("pg_temp_1".into()),
                        Value::Text("_temp_items".into()),
                    ],
                    vec![
                        Value::Text("pg_temp_1".into()),
                        Value::Text("temp_items".into()),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session_b
        .execute(
            &db,
            "select count(*) from pg_class where relname = 'temp_items'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn temp_catalog_reads_do_not_materialize_temp_catalog_relfiles() {
    let base = temp_dir("temp_catalog_no_read_sync");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table temp_items (id int4 not null)")
        .unwrap();
    session
        .execute(
            &db,
            "select count(*) from pg_class where relname = 'temp_items'",
        )
        .unwrap();

    let temp_entry = db.temp_entry(1, "temp_items").unwrap();
    let temp_db_oid = temp_entry.rel.db_oid;
    let class_path = crate::backend::storage::smgr::segment_path(
        &base,
        crate::backend::storage::smgr::RelFileLocator {
            spc_oid: 0,
            db_oid: temp_db_oid,
            rel_number: crate::include::catalog::BootstrapCatalogKind::PgClass.relation_oid(),
        },
        crate::backend::storage::smgr::ForkNumber::Main,
        0,
    );
    let proc_path = crate::backend::storage::smgr::segment_path(
        &base,
        crate::backend::storage::smgr::RelFileLocator {
            spc_oid: 0,
            db_oid: temp_db_oid,
            rel_number: crate::include::catalog::BootstrapCatalogKind::PgProc.relation_oid(),
        },
        crate::backend::storage::smgr::ForkNumber::Main,
        0,
    );

    assert!(
        !class_path.exists(),
        "temp pg_class relfile should stay absent on catalog reads"
    );
    assert!(
        !proc_path.exists(),
        "temp pg_proc relfile should stay absent"
    );
}

#[test]
fn temp_namespace_persists_after_last_temp_table_is_dropped() {
    let base = temp_dir("temp_namespace_persists");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table temp_items (id int4 not null)")
        .unwrap();
    session.execute(&db, "drop table temp_items").unwrap();

    let namespaces = db.temp_relations.read();
    let namespace = namespaces.get(&1).unwrap();
    assert_eq!(namespace.name, "pg_temp_1");
    assert!(namespace.tables.is_empty());
}

#[test]
fn pg_constraint_lists_not_null_columns_for_permanent_and_temp_tables() {
    let base = temp_dir("pg_constraint_not_null_rows");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null, note text)")
        .unwrap();
    session
        .execute(
            &db,
            "create temp table temp_items (id int4 not null, note text not null)",
        )
        .unwrap();

    match session
        .execute(
            &db,
            "select c.conname, r.relname, c.contype \
                 from pg_constraint c \
                 join pg_class r on r.oid = c.conrelid \
                 where r.relname in ('items', 'temp_items') \
                 order by r.relname, c.conname",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Text("items_id_not_null".into()),
                        Value::Text("items".into()),
                        Value::Text("n".into()),
                    ],
                    vec![
                        Value::Text("temp_items_id_not_null".into()),
                        Value::Text("temp_items".into()),
                        Value::Text("n".into()),
                    ],
                    vec![
                        Value::Text("temp_items_note_not_null".into()),
                        Value::Text("temp_items".into()),
                        Value::Text("n".into()),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn search_path_can_hide_public_tables_from_unqualified_lookup() {
    let base = temp_dir("search_path_hides_public");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into items (id) values (1)")
        .unwrap();
    session
        .execute(&db, "set search_path = pg_catalog")
        .unwrap();

    let err = session.execute(&db, "select id from items").unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::UnknownTable(name)) if name == "items"
    ));

    match session.execute(&db, "select id from public.items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn search_path_keeps_temp_tables_ahead_of_public_even_when_omitted() {
    let base = temp_dir("search_path_temp_precedence");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "insert into items (id) values (1)").unwrap();

    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into items (id) values (2)")
        .unwrap();
    session.execute(&db, "set search_path = public").unwrap();

    match session.execute(&db, "select id from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match session.execute(&db, "select id from public.items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_table_after_temp_table_still_uses_public_by_default() {
    let base = temp_dir("create_after_temp_uses_public");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table temp_marker (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "create table regular_after_temp (id int4 not null)")
        .unwrap();

    match session
        .execute(
            &db,
            "select n.nspname, c.relpersistence \
             from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where c.relname = 'regular_after_temp'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Text("public".into()), Value::Text("p".into())]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_table_errors_when_search_path_selects_no_creatable_schema() {
    let base = temp_dir("search_path_no_create_schema");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "set search_path = ''").unwrap();
    let err = session
        .execute(&db, "create table nope (id int4 not null)")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::NoSchemaSelectedForCreate)
    ));
}

#[test]
fn create_function_uses_search_path_for_unqualified_creation() {
    let base = temp_dir("search_path_function_create");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create schema tenant_fn").unwrap();
    session.execute(&db, "set search_path = tenant_fn").unwrap();
    session
        .execute(
            &db,
            "create function add_one(x int4) returns int4 language sql as $$ select x + 1 $$",
        )
        .unwrap();

    let visible = db.backend_catcache(1, None).unwrap();
    let proc = visible
        .proc_rows_by_name("add_one")
        .into_iter()
        .find(|row| row.proname == "add_one")
        .expect("function row");
    let tenant_ns = visible
        .namespace_by_name("tenant_fn")
        .expect("tenant namespace")
        .oid;
    assert_eq!(proc.pronamespace, tenant_ns);
}

#[test]
fn create_function_persists_explicit_cost() {
    let base = temp_dir("search_path_function_cost");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create schema tenant_fn").unwrap();
    session.execute(&db, "set search_path = tenant_fn").unwrap();
    session
        .execute(
            &db,
            "create or replace function add_one(x int4) returns int4 \
             cost 0.0000001 language sql as $$ select x + 1 $$",
        )
        .unwrap();

    let visible = db.backend_catcache(1, None).unwrap();
    let proc = visible
        .proc_rows_by_name("add_one")
        .into_iter()
        .find(|row| row.proname == "add_one")
        .expect("function row");
    assert!((proc.procost - 0.0000001).abs() < 1e-12);
}

#[test]
fn drop_function_uses_search_path_and_signature() {
    let base = temp_dir("search_path_function_drop");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "create schema tenant_fn").unwrap();
    session.execute(&db, "set search_path = tenant_fn").unwrap();
    session
        .execute(
            &db,
            "create function add_one(x int4) returns int4 language sql as $$ select x + 1 $$",
        )
        .unwrap();
    session.execute(&db, "drop function add_one(int4)").unwrap();

    let visible = db.backend_catcache(1, None).unwrap();
    assert!(
        visible.proc_rows_by_name("add_one").is_empty(),
        "expected dropped function to be absent from pg_proc"
    );
}

#[test]
fn drop_table_cascade_notice_omits_temp_schema_name() {
    let base = temp_dir("drop_temp_child_notice");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table some_tab (id int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create temp table some_tab_child () inherits (some_tab)",
        )
        .unwrap();
    take_backend_notice_messages();

    session.execute(&db, "drop table some_tab cascade").unwrap();

    assert_eq!(
        take_backend_notice_messages(),
        vec![String::from("drop cascades to table some_tab_child")]
    );
}

#[test]
fn create_table_uses_pg_temp_search_path_for_unqualified_creation() {
    let base = temp_dir("search_path_pg_temp_create");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "set search_path = pg_temp").unwrap();
    session
        .execute(&db, "create table tempy (id int4 not null)")
        .unwrap();

    assert!(db.temp_entry(1, "tempy").is_some());
    assert!(
        db.catalog
            .read()
            .catalog_snapshot()
            .unwrap()
            .get("tempy")
            .is_none()
    );
}

#[test]
fn create_table_as_uses_pg_temp_search_path_for_unqualified_creation() {
    let base = temp_dir("search_path_pg_temp_ctas");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "set search_path = pg_temp").unwrap();
    session
        .execute(&db, "create table tempy as select 1 as id")
        .unwrap();

    assert!(db.temp_entry(1, "tempy").is_some());
    match session.execute(&db, "select id from tempy").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_index_supports_qualified_public_target_under_temp_shadowing() {
    let base = temp_dir("qualified_create_index_public");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    let public_items_oid = db
        .catalog
        .read()
        .catalog_snapshot()
        .unwrap()
        .get("items")
        .unwrap()
        .relation_oid;

    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "create index items_public_idx on public.items (id)")
        .unwrap();

    let catalog = db.catalog.read().catalog_snapshot().unwrap();
    let index = catalog.get("items_public_idx").unwrap();
    let index_meta = index.index_meta.as_ref().unwrap();
    assert_eq!(index_meta.indrelid, public_items_oid);
}

#[test]
fn create_index_supports_temp_tables_when_temp_is_first_visible() {
    let base = temp_dir("search_path_create_index_temp");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();

    session
        .execute(&db, "create index items_temp_idx on items (id)")
        .unwrap();

    let temp_table = db.temp_entry(1, "items").unwrap();
    let temp_index = db.temp_entry(1, "items_temp_idx").unwrap();
    let index_meta = temp_index.index.as_ref().unwrap();
    assert_eq!(index_meta.indrelid, temp_table.relation_oid);
}

#[test]
fn temp_primary_key_indexes_are_visible_through_catalog_lookup() {
    let base = temp_dir("temp_primary_key_indexes_visible");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create temp table items (id int4 primary key, note int4)",
        )
        .unwrap();

    let lookup = db.lazy_catalog_lookup(1, None, None);
    let relation = lookup.lookup_any_relation("items").unwrap();
    let indexes = lookup.index_relations_for_heap(relation.relation_oid);

    assert_eq!(indexes.len(), 1);
    assert!(indexes[0].index_meta.indisprimary);
    assert!(indexes[0].index_meta.indisready);
    assert!(indexes[0].index_meta.indisvalid);
}

#[test]
fn temp_constraint_backed_indexes_preserve_indimmediate() {
    let base = temp_dir("temp_constraint_backed_indexes_indimmediate");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table items (id int4 primary key)")
        .unwrap();
    let immediate_index = db.temp_entry(1, "items_pkey").unwrap();
    assert!(
        immediate_index
            .index
            .as_ref()
            .is_some_and(|index| index.indimmediate)
    );

    session
        .execute(
            &db,
            "create temp table deferred_items (id int4 unique deferrable initially deferred)",
        )
        .unwrap();
    let deferred_index = db.temp_entry(1, "deferred_items_id_key").unwrap();
    assert!(
        deferred_index
            .index
            .as_ref()
            .is_some_and(|index| !index.indimmediate)
    );
}

#[test]
fn temp_table_on_commit_actions_apply_at_commit() {
    let base = temp_dir("temp_table_on_commit");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table keep_rows (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into keep_rows (id) values (1)")
        .unwrap();
    match session
        .execute(&db, "select count(*) from keep_rows")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => assert_eq!(rows, vec![vec![Value::Int64(1)]]),
        other => panic!("expected query result, got {:?}", other),
    }

    session
        .execute(
            &db,
            "create temp table delete_rows (id int4 not null) on commit delete rows",
        )
        .unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into delete_rows (id) values (10)")
        .unwrap();
    session.execute(&db, "commit").unwrap();
    match session
        .execute(&db, "select count(*) from delete_rows")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => assert_eq!(rows, vec![vec![Value::Int64(0)]]),
        other => panic!("expected query result, got {:?}", other),
    }

    session.execute(&db, "begin").unwrap();
    session
        .execute(
            &db,
            "create temp table drop_rows (id int4 not null) on commit drop",
        )
        .unwrap();
    session
        .execute(&db, "insert into drop_rows (id) values (11)")
        .unwrap();
    session.execute(&db, "commit").unwrap();
    let err = session
        .execute(&db, "select count(*) from drop_rows")
        .unwrap_err();
    assert!(matches!(err, ExecError::Parse(ParseError::UnknownTable(name)) if name == "drop_rows"));
}

#[test]
fn temp_create_table_as_select_works() {
    let base = temp_dir("temp_ctas");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table source_items (id int4 not null, note text)")
        .unwrap();
    db.execute(
        1,
        "insert into source_items (id, note) values (1, 'a'), (2, 'b')",
    )
    .unwrap();

    session
            .execute(&db, "create temp table temp_items(tmp_id, tmp_note) as select id, note from source_items order by id")
            .unwrap();

    match session
        .execute(
            &db,
            "select tmp_id, tmp_note from temp_items order by tmp_id",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1), Value::Text("a".into())],
                    vec![Value::Int32(2), Value::Text("b".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_into_creates_table_from_query() {
    let base = temp_dir("select_into_ctas");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table cmdata (f1 text)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into cmdata values (repeat('1234567890', 1000))",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(&db, "select * into cmmove1 from cmdata")
            .unwrap(),
        StatementResult::AffectedRows(1)
    );

    match session
        .execute(&db, "select length(f1) from cmmove1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(10_000)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn pg_column_compression_reports_large_inserted_value() {
    let base = temp_dir("pg_column_compression_reports_large_inserted_value");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table cmdata (f1 text compression pglz)")
        .unwrap();
    session
        .execute(
            &db,
            "insert into cmdata values (repeat('1234567890', 1000))",
        )
        .unwrap();

    match session
        .execute(&db, "select pg_column_compression(f1) from cmdata")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("pglz".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_table_as_autocommit_publishes_permanent_catalog_rows() {
    let base = temp_dir("autocommit_ctas_permanent");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create table source_items (id int4 not null)")
        .unwrap();
    db.execute(1, "insert into source_items (id) values (1), (2)")
        .unwrap();

    assert_eq!(
        db.execute(
            1,
            "create table copied_items as select id from source_items order by id",
        )
        .unwrap(),
        StatementResult::AffectedRows(2)
    );

    match db.execute(1, "select count(*) from copied_items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match db
        .execute(
            1,
            "select count(*) from pg_class where relname = 'copied_items'",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn create_table_as_is_visible_in_same_txn_before_commit() {
    let base = temp_dir("txn_ctas_visibility");
    let db = Database::open(&base, 16).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    writer
        .execute(&db, "create table source_items (id int4 not null)")
        .unwrap();
    writer
        .execute(&db, "insert into source_items (id) values (1), (2)")
        .unwrap();

    writer.execute(&db, "begin").unwrap();
    writer
        .execute(
            &db,
            "create table copied_items as select id from source_items order by id",
        )
        .unwrap();

    match writer
        .execute(&db, "select count(*) from copied_items")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    assert!(
        reader
            .execute(&db, "select count(*) from copied_items")
            .is_err(),
        "other sessions must not see uncommitted CTAS catalog rows"
    );

    writer.execute(&db, "commit").unwrap();

    match reader
        .execute(&db, "select count(*) from copied_items")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn streaming_select_uses_temp_table_shadowing() {
    let base = temp_dir("streaming_temp_shadowing");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    db.execute(1, "insert into items (id) values (1), (2)")
        .unwrap();

    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into items (id) values (10), (20), (30)")
        .unwrap();

    let stmt = crate::backend::parser::parse_select("select id from items order by id").unwrap();
    let mut guard = session.execute_streaming(&db, &stmt).unwrap();
    let mut rows = Vec::new();
    while let Some(slot) =
        crate::backend::executor::exec_next(&mut guard.state, &mut guard.ctx).unwrap()
    {
        rows.push(
            slot.values()
                .unwrap()
                .iter()
                .map(|v| v.to_owned_value())
                .collect::<Vec<_>>(),
        );
    }
    drop(guard);

    assert_eq!(
        rows,
        vec![
            vec![Value::Int32(10)],
            vec![Value::Int32(20)],
            vec![Value::Int32(30)],
        ]
    );
}

#[test]
fn temp_tables_are_removed_on_client_cleanup() {
    let base = temp_dir("temp_cleanup");
    let db = Database::open(&base, 16).unwrap();

    db.execute(1, "create temp table cleanup_me (id int4 not null)")
        .unwrap();
    db.execute(1, "insert into cleanup_me (id) values (1)")
        .unwrap();
    match db.execute(1, "select count(*) from cleanup_me").unwrap() {
        StatementResult::Query { rows, .. } => assert_eq!(rows, vec![vec![Value::Int64(1)]]),
        other => panic!("expected query result, got {:?}", other),
    }

    db.cleanup_client_temp_relations(1);
    let err = db
        .execute(1, "select count(*) from cleanup_me")
        .unwrap_err();
    assert!(
        matches!(err, ExecError::Parse(ParseError::UnknownTable(name)) if name == "cleanup_me")
    );
}

#[test]
fn recovery_skips_stale_temp_sequence_state_until_namespace_cleanup() {
    let base = temp_dir("temp_sequence_recovery_cleanup");
    {
        let db = Database::open(&base, 16).unwrap();
        db.execute(1, "create temp sequence ts1").unwrap();
        assert_eq!(
            query_rows(
                &db,
                1,
                "select count(*) from pg_class where relname = 'ts1' and relkind = 'S' and relpersistence = 't'",
            ),
            vec![vec![Value::Int64(1)]]
        );
    }

    let db = Database::open(&base, 16).unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_class where relname = 'ts1' and relkind = 'S' and relpersistence = 't'",
        ),
        vec![vec![Value::Int64(1)]]
    );

    db.execute(1, "create temp sequence ts2").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relname from pg_class where relkind = 'S' and relpersistence = 't' order by relname",
        ),
        vec![vec![Value::Text("ts2".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select nextval('ts2')"),
        vec![vec![Value::Int64(1)]]
    );
}

#[test]
fn temp_cleanup_keeps_namespace_rows_and_permanent_catalogs_visible() {
    let base = temp_dir("temp_cleanup_keeps_namespace");
    let db = Database::open(&base, 16).unwrap();
    let mut temp_session = Session::new(1);
    let mut other_session = Session::new(2);

    other_session
        .execute(&db, "create table items (id int4 not null)")
        .unwrap();
    other_session
        .execute(&db, "insert into items (id) values (1)")
        .unwrap();

    temp_session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();
    temp_session
        .execute(&db, "insert into items (id) values (2)")
        .unwrap();

    match temp_session.execute(&db, "select id from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    db.cleanup_client_temp_relations(1);
    db.clear_temp_backend_id(1);

    match other_session.execute(&db, "select id from items").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match other_session
        .execute(
            &db,
            "select nspname from pg_namespace \
             where nspname in ('pg_temp_1', 'pg_toast_temp_1') \
             order by nspname",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("pg_temp_1".into())],
                    vec![Value::Text("pg_toast_temp_1".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    assert!(
        db.execute(2, "select count(*) from pg_namespace").is_ok(),
        "catalog lookups must stay intact after temp cleanup"
    );
}

#[test]
fn temp_slot_reuse_starts_from_clean_namespace_contents() {
    let base = temp_dir("temp_slot_reuse_cleanup");
    let db = Database::open(&base, 16).unwrap();
    let mut first_session = Session::with_temp_backend_id(10, 1);
    let mut reused_session = Session::with_temp_backend_id(11, 1);

    first_session
        .execute(&db, "create temp table temp_old (id int4 not null)")
        .unwrap();
    first_session
        .execute(&db, "insert into temp_old (id) values (7)")
        .unwrap();

    db.cleanup_client_temp_relations(10);
    db.clear_temp_backend_id(10);

    let err = reused_session
        .execute(&db, "select count(*) from temp_old")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::UnknownTable(name)) if name == "temp_old"
    ));

    reused_session
        .execute(&db, "create temp table temp_new (id int4 not null)")
        .unwrap();
    reused_session
        .execute(&db, "insert into temp_new (id) values (9)")
        .unwrap();

    match reused_session
        .execute(&db, "select count(*) from temp_new")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match reused_session
        .execute(
            &db,
            "select nspname from pg_namespace \
             where nspname in ('pg_temp_1', 'pg_toast_temp_1') \
             order by nspname",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("pg_temp_1".into())],
                    vec![Value::Text("pg_toast_temp_1".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn temp_namespace_creation_does_not_poison_global_next_oid() {
    let base = temp_dir("temp_namespace_next_oid");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::with_temp_backend_id(1, 7);

    let initial_next_oid = db.catalog.read().catalog_snapshot().unwrap().next_oid;
    assert!(initial_next_oid < Database::temp_namespace_oid(7));

    session
        .execute(&db, "create temp table temp_probe (id int4)")
        .unwrap();

    let catalog = db.catalog.read().catalog_snapshot().unwrap();
    let temp_entry = db.temp_entry(1, "temp_probe").unwrap();
    assert!(
        catalog.next_oid < Database::temp_namespace_oid(7),
        "next_oid jumped into reserved temp namespace range: {}",
        catalog.next_oid
    );
    assert!(
        temp_entry.relation_oid < Database::temp_namespace_oid(7),
        "temp relation oid jumped into reserved temp namespace range: {}",
        temp_entry.relation_oid
    );
}

#[test]
fn copy_from_rows_parses_extended_numeric_types() {
    let base = temp_dir("copy_from_rows_extended_numeric");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(
        1,
        "create table metrics (a int2, b int8, c float4, d float8)",
    )
    .unwrap();

    let inserted = session
        .copy_from_rows(
            &db,
            "metrics",
            &[vec![
                "7".into(),
                "9000000000".into(),
                "1.5".into(),
                "2.5".into(),
            ]],
        )
        .unwrap();
    assert_eq!(inserted, 1);

    match db.execute(1, "select a, b, c, d from metrics").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int16(7),
                    Value::Int64(9_000_000_000),
                    Value::Float64(1.5),
                    Value::Float64(2.5),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn copy_from_rows_into_column_subset_leaves_other_columns_null() {
    let base = temp_dir("copy_from_rows_subset");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(
        1,
        "create table width_bucket_test (operand_num numeric, operand_f8 float8)",
    )
    .unwrap();

    let inserted = session
        .copy_from_rows_into(
            &db,
            "width_bucket_test",
            Some(&["operand_num".into()]),
            &[vec!["5.5".into()]],
        )
        .unwrap();
    assert_eq!(inserted, 1);

    match db
        .execute(
            1,
            "select operand_num, operand_f8 is null from width_bucket_test",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Numeric("5.5".into()), Value::Bool(true)]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn copy_from_rows_into_failed_implicit_transaction_cleans_session_state() {
    let base = temp_dir("copy_from_rows_cleanup");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (a int4, b int4, c int4)")
        .unwrap();
    db.execute(1, "alter table items drop a").unwrap();

    match session.copy_from_rows_into(&db, "items", Some(&["a".into()]), &[vec!["10".into()]]) {
        Err(ExecError::Parse(ParseError::UnknownColumn(name))) if name == "a" => {}
        other => panic!(
            "expected dropped-column COPY target failure, got {:?}",
            other
        ),
    }

    assert!(!session.in_transaction());
    assert!(!session.transaction_failed());

    session
        .execute(&db, "create table after_copy_failure (id int4)")
        .unwrap();
    session
        .execute(&db, "insert into after_copy_failure values (1)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select * from after_copy_failure"),
        vec![vec![Value::Int32(1)]]
    );
}

#[test]
fn concurrent_selects_on_shared_data() {
    let base = temp_dir("concurrent_selects");
    let db = Database::open(&base, 32).unwrap();

    db.execute(1, "create table nums (id int4 not null, val int4 not null)")
        .unwrap();
    for i in 1..=10 {
        db.execute(
            1,
            &format!("insert into nums (id, val) values ({i}, {})", i * 10),
        )
        .unwrap();
    }

    let num_threads = 4;
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..5 {
                    match db
                        .execute((t + 100) as ClientId, "select count(*) from nums")
                        .unwrap()
                    {
                        StatementResult::Query { rows, .. } => {
                            assert_eq!(rows, vec![vec![Value::Int64(10)]]);
                        }
                        other => panic!("expected query result, got {:?}", other),
                    }
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, HEAVY_CONTENTION_TEST_TIMEOUT);
}

#[test]
fn concurrent_inserts_and_selects() {
    let base = temp_dir("concurrent_inserts");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table log (id int4 not null, thread_id int4 not null)",
    )
    .unwrap();

    let num_threads = 4;
    let inserts_per_thread = 5;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..inserts_per_thread {
                    let id = t * 100 + i;
                    db.execute(
                        (t + 200) as ClientId,
                        &format!("insert into log (id, thread_id) values ({id}, {t})"),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    let total = num_threads * inserts_per_thread;
    match db.execute(1, "select count(*) from log").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(total as i64)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn mixed_concurrent_reads_and_writes() {
    let base = temp_dir("mixed_concurrent");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table counters (id int4 not null, val int4 not null)",
    )
    .unwrap();
    db.execute(1, "insert into counters (id, val) values (1, 0)")
        .unwrap();

    let num_readers = 3;
    let num_writers = 2;
    let ops_per_thread = 5;

    let mut handles = Vec::new();

    for t in 0..num_readers {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let result = db
                    .execute(
                        (t + 300) as ClientId,
                        "select val from counters where id = 1",
                    )
                    .unwrap();
                match result {
                    StatementResult::Query { rows, .. } => {
                        assert_eq!(rows.len(), 1);
                    }
                    other => panic!("expected query result, got {:?}", other),
                }
            }
        }));
    }

    for t in 0..num_writers {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let id = 1000 + t * 100 + i;
                db.execute(
                    (t + 400) as ClientId,
                    &format!("insert into counters (id, val) values ({id}, {i})"),
                )
                .unwrap();
            }
        }));
    }

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    match db.execute(1, "select count(*) from counters").unwrap() {
        StatementResult::Query { rows, .. } => {
            let expected = 1 + num_writers * ops_per_thread;
            assert_eq!(rows, vec![vec![Value::Int64(expected as i64)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn concurrent_updates_same_row_no_lost_updates() {
    let base = temp_dir("concurrent_update_same_row");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table counter (id int4 not null, val int4 not null)",
    )
    .unwrap();
    db.execute(1, "insert into counter (id, val) values (1, 0)")
        .unwrap();

    let num_threads = 4;
    let updates_per_thread = 10;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..updates_per_thread {
                    db.execute(
                        (t + 500) as ClientId,
                        "update counter set val = val + 1 where id = 1",
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    let expected = num_threads * updates_per_thread;
    match db
        .execute(1, "select val from counter where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int32(expected as i32)]],
                "expected val={expected} after {num_threads} threads x {updates_per_thread} increments"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn concurrent_updates_different_rows() {
    let base = temp_dir("concurrent_update_diff_rows");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table slots (id int4 not null, val int4 not null)",
    )
    .unwrap();

    let num_threads = 4;
    for i in 0..num_threads {
        db.execute(1, &format!("insert into slots (id, val) values ({i}, 0)"))
            .unwrap();
    }

    let updates_per_thread = 20;
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..updates_per_thread {
                    db.execute(
                        (t + 600) as ClientId,
                        &format!("update slots set val = val + 1 where id = {t}"),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    for i in 0..num_threads {
        match db
            .execute(1, &format!("select val from slots where id = {i}"))
            .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(updates_per_thread as i32)]],
                    "row {i} should have val={updates_per_thread}"
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
}

#[test]
fn epq_predicate_recheck_skips_non_matching() {
    let base = temp_dir("epq_predicate_recheck");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table flag (id int4 not null, val int4 not null)")
        .unwrap();
    db.execute(1, "insert into flag (id, val) values (1, 0)")
        .unwrap();

    let num_threads = 4;
    let results: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                db.execute(
                    (t + 700) as ClientId,
                    "update flag set val = 99 where val = 0",
                )
                .unwrap()
            })
        })
        .collect();

    let deadline = Instant::now() + TEST_TIMEOUT;
    let affected: Vec<usize> = results
        .into_iter()
        .map(|h| {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                panic!("test timed out after {TEST_TIMEOUT:?} — likely deadlock");
            }
            let (tx, rx) = std::sync::mpsc::channel();
            let waiter = thread::spawn(move || {
                let _ = tx.send(h.join());
            });
            let result = rx.recv_timeout(remaining).unwrap_or_else(|_| {
                panic!("test timed out after {TEST_TIMEOUT:?} — likely deadlock")
            });
            let _ = waiter.join();
            match result.unwrap() {
                StatementResult::AffectedRows(n) => n,
                other => panic!("expected affected rows, got {:?}", other),
            }
        })
        .collect();

    let total_affected: usize = affected.iter().sum();
    assert!(
        total_affected >= 1,
        "at least one thread should have updated the row, got {total_affected}"
    );

    match db.execute(1, "select val from flag where id = 1").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(99)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Regression: heap_flush used to call complete_write unconditionally even
/// when another thread already flushed the buffer (FlushResult::AlreadyClean).
/// This caused a NoIoInProgress error under concurrency.
#[test]
fn concurrent_flush_does_not_error() {
    let base = temp_dir("concurrent_flush");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table ftest (id int4 not null, val int4 not null)",
    )
    .unwrap();

    let num_threads = 4;
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..20 {
                    let id = t * 1000 + i;
                    db.execute(
                        (t + 800) as ClientId,
                        &format!("insert into ftest (id, val) values ({id}, {i})"),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    match db.execute(1, "select count(*) from ftest").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(80)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Regression: heap_insert_version used to do read-modify-write on a page
/// without the content lock. Two concurrent inserts could overwrite each
/// other's tuples, losing rows.
#[test]
fn concurrent_inserts_no_lost_rows() {
    let base = temp_dir("concurrent_insert_no_loss");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table itest (id int4 not null)")
        .unwrap();

    let num_threads = 4;
    let inserts_per_thread = 25;
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..inserts_per_thread {
                    let id = t * 1000 + i;
                    db.execute(
                        (t + 900) as ClientId,
                        &format!("insert into itest (id) values ({id})"),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, PGBENCH_STYLE_TEST_TIMEOUT);

    let expected = num_threads * inserts_per_thread;
    match db.execute(1, "select count(*) from itest").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(expected as i64)]],
                "expected {expected} rows, no lost inserts"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Regression: pin_existing_block used to call complete_read even for
/// WaitingOnRead, which failed with NoIoInProgress because another thread
/// already completed the read. Now uses wait_for_io instead.
#[test]
fn concurrent_reads_same_page_no_io_error() {
    let base = temp_dir("concurrent_reads_same_page");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table rtest (id int4 not null, val int4 not null)",
    )
    .unwrap();
    for i in 0..5 {
        db.execute(1, &format!("insert into rtest (id, val) values ({i}, {i})"))
            .unwrap();
    }

    let num_threads = 8;
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..30 {
                    match db
                        .execute((t + 1000) as ClientId, "select count(*) from rtest")
                        .unwrap()
                    {
                        StatementResult::Query { rows, .. } => {
                            assert_eq!(rows, vec![vec![Value::Int64(5)]]);
                        }
                        other => panic!("expected query result, got {:?}", other),
                    }
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, HEAVY_CONTENTION_TEST_TIMEOUT);
}

/// Regression: without the content lock on heap_scan_next, a reader could
/// see a partially written page from a concurrent writer (torn read).
/// This test exercises concurrent reads and writes on the same table to
/// verify no panics or corrupt data.
#[test]
fn concurrent_read_write_same_table_no_corruption() {
    let base = temp_dir("concurrent_rw_corruption");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table rwtest (id int4 not null, val int4 not null)",
    )
    .unwrap();
    db.execute(1, "insert into rwtest (id, val) values (1, 0)")
        .unwrap();

    let num_readers = 4;
    let num_writers = 2;
    let mut handles = Vec::new();

    for t in 0..num_writers {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..20 {
                if let Err(e) = db.execute(
                    (t + 1100) as ClientId,
                    "update rwtest set val = val + 1 where id = 1",
                ) {
                    panic!("writer {t} iteration {i} failed: {e:?}");
                }
            }
        }));
    }

    for t in 0..num_readers {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..50 {
                let result = db.execute(
                    (t + 1200) as ClientId,
                    "select val from rwtest where id = 1",
                );
                match result {
                    Err(e) => panic!("reader {t} iteration {i} failed: {e:?}"),
                    Ok(StatementResult::Query { rows, .. }) => {
                        assert_eq!(rows.len(), 1, "should always see exactly one row");
                        match &rows[0][0] {
                            Value::Int32(v) => assert!(*v >= 0, "val should never be negative"),
                            other => panic!("expected Int32, got {:?}", other),
                        }
                    }
                    Ok(other) => panic!("expected query result, got {:?}", other),
                }
            }
        }));
    }

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    let expected_val = num_writers * 20;
    match db
        .execute(1, "select val from rwtest where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int32(expected_val as i32)]],
                "all writer updates should be applied"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Regression: the write-preferring RwLock in parking_lot caused a deadlock
/// when a thread tried to acquire a txns read lock (to check xmax status)
/// while another thread was pending a txns write lock (to commit). The
/// pending writer blocks new readers, creating a cycle.
/// This test verifies the deadlock is resolved (would timeout otherwise).
#[test]
fn no_deadlock_under_write_preferring_rwlock() {
    let base = temp_dir("write_preferring_deadlock");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table dltest (id int4 not null, val int4 not null)",
    )
    .unwrap();
    db.execute(1, "insert into dltest (id, val) values (1, 0)")
        .unwrap();

    let num_threads = 4;
    let updates_per_thread = 20;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..updates_per_thread {
                    db.execute(
                        (t + 1300) as ClientId,
                        "update dltest set val = val + 1 where id = 1",
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);

    let expected = num_threads * updates_per_thread;
    match db
        .execute(1, "select val from dltest where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(expected as i32)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Reproduces the pgbench benchmark hang more closely than the simple
/// counter tests: many concurrent full-table UPDATE/SELECT cycles over a
/// relation much larger than the buffer pool.
#[test]
fn pgbench_style_accounts_workload_completes() {
    let base = temp_dir("pgbench_style_hang");
    let db = Database::open(&base, 128).unwrap();

    db.execute(
            1,
            "create table pgbench_accounts (aid int4 not null, bid int4 not null, abalance int4 not null, filler text)",
        )
        .unwrap();

    let values = (1..=5000)
        .map(|aid| format!("({aid}, 1, 0, 'x')"))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(
        1,
        &format!("insert into pgbench_accounts (aid, bid, abalance, filler) values {values}"),
    )
    .unwrap();

    let num_threads = 10;
    let ops_per_thread = 10;
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let aid = ((t * 997 + i * 389) % 5000) + 1;
                    db.execute(
                        (t + 2100) as ClientId,
                        &format!(
                            "update pgbench_accounts set abalance = abalance + -1 where aid = {aid}"
                        ),
                    )
                    .unwrap();
                    match db
                        .execute(
                            (t + 2200) as ClientId,
                            &format!("select abalance from pgbench_accounts where aid = {aid}"),
                        )
                        .unwrap()
                    {
                        StatementResult::Query { rows, .. } => {
                            assert_eq!(rows.len(), 1);
                        }
                        other => panic!("expected query result, got {:?}", other),
                    }
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, TEST_TIMEOUT);
}

/// Regression: try_claim_tuple reads ctid before dropping the buffer lock,
/// then checks xmax status after dropping it. If the updater commits and
/// sets ctid between those two points, the stale ctid (== self) makes us
/// think the row was deleted rather than updated, losing the update.
///
/// Uses a Barrier so all threads start simultaneously, maximizing the
/// chance of hitting the race window.
#[test]
fn no_lost_updates_under_heavy_contention() {
    let base = temp_dir("no_lost_updates_heavy");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table counter (id int4 not null, val int4 not null)",
    )
    .unwrap();
    db.execute(1, "insert into counter (id, val) values (1, 0)")
        .unwrap();

    let num_threads = 4usize;
    let increments_per_thread = 10;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_threads));
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for i in 0..increments_per_thread {
                    if let Err(e) = db.execute(
                        (t + 5000) as ClientId,
                        "update counter set val = val + 1 where id = 1",
                    ) {
                        panic!("thread {t} iteration {i}: {e:?}");
                    }
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, STRESS_TEST_TIMEOUT);

    let expected = num_threads * increments_per_thread;
    match db
        .execute(1, "select val from counter where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let actual = match &rows[0][0] {
                Value::Int32(v) => *v,
                other => panic!("expected Int32, got {:?}", other),
            };
            assert_eq!(
                actual,
                expected as i32,
                "lost {} update(s): expected {expected}, got {actual}",
                expected as i32 - actual
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Regression test for bugs/005: try_read contention busy-loop.
///
/// Before the fix, try_claim_tuple used try_read() to check xmax status.
/// Under contention (16 threads updating the same row), all try_read
/// attempts could fail, causing an infinite busy-loop. The fix replaced
/// try_read() with blocking read().
///
/// This test verifies no lost updates under high contention.
#[test]
fn poc_try_read_contention_lost_update() {
    let base = temp_dir("poc_try_read_contention");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table ctr (id int4 not null, val int4 not null)")
        .unwrap();
    db.execute(1, "insert into ctr (id, val) values (1, 0)")
        .unwrap();

    let num_threads = 16;
    let updates_per_thread = 5;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for i in 0..updates_per_thread {
                    if let Err(e) = db.execute(
                        (t + 6000) as ClientId,
                        "update ctr set val = val + 1 where id = 1",
                    ) {
                        panic!("thread {t} iteration {i}: {e:?}");
                    }
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, Duration::from_secs(60));

    let expected = (num_threads * updates_per_thread) as i32;
    match db.execute(1, "select val from ctr where id = 1").unwrap() {
        StatementResult::Query { rows, .. } => {
            let actual = match &rows[0][0] {
                Value::Int32(v) => *v,
                other => panic!("expected Int32, got {:?}", other),
            };
            assert_eq!(
                actual,
                expected,
                "LOST {} update(s): expected {expected}, got {actual}. \
                     This demonstrates the try_read contention bug — \
                     try_read returns None under txns write-lock contention, \
                     causing committed updates to be treated as in-progress.",
                expected - actual
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn begin_commit_groups_statements() {
    let base = temp_dir("begin_commit");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table txtest (id int4 not null, val int4 not null)",
        )
        .unwrap();
    session.execute(&db, "begin").unwrap();
    assert!(session.in_transaction());
    assert_eq!(session.ready_status(), b'T');

    session
        .execute(&db, "insert into txtest (id, val) values (1, 10)")
        .unwrap();
    session
        .execute(&db, "insert into txtest (id, val) values (2, 20)")
        .unwrap();
    session.execute(&db, "commit").unwrap();
    assert!(!session.in_transaction());
    assert_eq!(session.ready_status(), b'I');

    match session.execute(&db, "select count(*) from txtest").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query, got {:?}", other),
    }
}

#[test]
fn create_table_is_visible_in_same_txn_before_commit() {
    let base = temp_dir("txn_create_table_visibility");
    let db = Database::open(&base, 64).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    writer.execute(&db, "begin").unwrap();
    writer
        .execute(&db, "create table tx_new (id int4 not null)")
        .unwrap();
    writer
        .execute(&db, "insert into tx_new (id) values (1)")
        .unwrap();

    match writer.execute(&db, "select count(*) from tx_new").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query, got {:?}", other),
    }

    assert!(
        reader.execute(&db, "select count(*) from tx_new").is_err(),
        "other sessions must not see uncommitted catalog rows"
    );

    writer.execute(&db, "commit").unwrap();

    match reader.execute(&db, "select count(*) from tx_new").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query, got {:?}", other),
    }
}

#[test]
fn rollback_discards_created_table() {
    let base = temp_dir("txn_create_table_rollback");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "create table tx_rollback_only (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into tx_rollback_only (id) values (1)")
        .unwrap();
    session.execute(&db, "rollback").unwrap();

    assert!(
        session
            .execute(&db, "select count(*) from tx_rollback_only")
            .is_err(),
        "rolled-back table creation must disappear"
    );
}

#[test]
fn rollback_discards_changes() {
    let base = temp_dir("rollback");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table rbtest (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into rbtest (id) values (1)")
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into rbtest (id) values (2)")
        .unwrap();
    session
        .execute(&db, "insert into rbtest (id) values (3)")
        .unwrap();
    session.execute(&db, "rollback").unwrap();

    match session.execute(&db, "select count(*) from rbtest").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(1)]],
                "only the autocommitted row should survive rollback"
            );
        }
        other => panic!("expected query, got {:?}", other),
    }
}

#[test]
fn drop_table_is_transactional() {
    let base = temp_dir("txn_drop_table_visibility");
    let db = Database::open(&base, 64).unwrap();
    let mut writer = Session::new(1);
    let mut reader = Session::new(2);

    writer
        .execute(&db, "create table drop_me (id int4 not null)")
        .unwrap();
    writer
        .execute(&db, "insert into drop_me (id) values (1)")
        .unwrap();

    writer.execute(&db, "begin").unwrap();
    writer.execute(&db, "drop table drop_me").unwrap();

    assert!(
        writer.execute(&db, "select count(*) from drop_me").is_err(),
        "dropping session should stop seeing the table immediately"
    );

    writer.execute(&db, "commit").unwrap();

    assert!(
        reader.execute(&db, "select count(*) from drop_me").is_err(),
        "other sessions should stop seeing the table after commit"
    );
}

#[test]
fn rollback_restores_dropped_table() {
    let base = temp_dir("txn_drop_table_rollback");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table restore_me (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into restore_me (id) values (1)")
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session.execute(&db, "drop table restore_me").unwrap();
    session.execute(&db, "rollback").unwrap();

    match session
        .execute(&db, "select count(*) from restore_me")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query, got {:?}", other),
    }
}

#[test]
fn truncate_rollback_restores_relfilenodes_and_rows() {
    let base = temp_dir("txn_truncate_rollback");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table trunc_rb (id int4 not null, note text)")
        .unwrap();
    session
        .execute(&db, "insert into trunc_rb values (1, 'alpha'), (2, 'beta')")
        .unwrap();
    session
        .execute(&db, "create index trunc_rb_idx on trunc_rb (id)")
        .unwrap();

    let old_heap = relfilenode_for(&db, 1, "trunc_rb");
    let old_index = relfilenode_for(&db, 1, "trunc_rb_idx");

    session.execute(&db, "begin").unwrap();
    session.execute(&db, "truncate trunc_rb").unwrap();

    let current_relfilenode = |session: &mut Session, relname: &str| -> i64 {
        match session
            .execute(
                &db,
                &format!("select relfilenode from pg_class where relname = '{relname}'"),
            )
            .unwrap()
        {
            StatementResult::Query { rows, .. } => match rows.as_slice() {
                [row] => int_value(row.first().expect("relfilenode value")),
                other => panic!("expected one relfilenode row, got {:?}", other),
            },
            other => panic!("expected query, got {:?}", other),
        }
    };

    let truncated_heap = current_relfilenode(&mut session, "trunc_rb");
    let truncated_index = current_relfilenode(&mut session, "trunc_rb_idx");
    assert_ne!(truncated_heap, old_heap);
    assert_ne!(truncated_index, old_index);

    session
        .execute(&db, "insert into trunc_rb values (9, 'new')")
        .unwrap();
    session.execute(&db, "rollback").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from trunc_rb order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("alpha".into())],
            vec![Value::Int32(2), Value::Text("beta".into())],
        ]
    );
    assert_eq!(relfilenode_for(&db, 1, "trunc_rb"), old_heap);
    assert_eq!(relfilenode_for(&db, 1, "trunc_rb_idx"), old_index);
}

#[test]
fn failed_transaction_rejects_commands() {
    let base = temp_dir("failed_txn");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table ftest (id int4 not null)")
        .unwrap();
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into ftest (id) values (1)")
        .unwrap();

    let err = session.execute(&db, "select * from nonexistent");
    assert!(err.is_err());
    assert!(session.transaction_failed());
    assert_eq!(session.ready_status(), b'E');

    let err = session.execute(&db, "select * from ftest");
    assert!(
        err.is_err(),
        "commands should be rejected in failed transaction"
    );

    session.execute(&db, "rollback").unwrap();
    assert!(!session.in_transaction());
    assert_eq!(session.ready_status(), b'I');

    match session.execute(&db, "select count(*) from ftest").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(0)]],
                "all inserts should be rolled back"
            );
        }
        other => panic!("expected query, got {:?}", other),
    }
}

#[test]
fn autocommit_still_works_without_begin() {
    let base = temp_dir("autocommit");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table atest (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "insert into atest (id) values (1)")
        .unwrap();
    session
        .execute(&db, "insert into atest (id) values (2)")
        .unwrap();

    match session.execute(&db, "select count(*) from atest").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query, got {:?}", other),
    }
}

#[test]
fn vacuum_analyze_is_rejected_inside_transaction_block() {
    let base = temp_dir("vacuum_txn_block");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session.execute(&db, "begin").unwrap();
    match session.execute(&db, "vacuum analyze pgbench_branches") {
        Err(crate::backend::executor::ExecError::Parse(
            crate::backend::parser::ParseError::ActiveSqlTransaction(stmt),
        )) => {
            assert_eq!(stmt, "VACUUM");
        }
        other => panic!("expected active transaction error, got {:?}", other),
    }
    session.execute(&db, "rollback").unwrap();
}

#[test]
fn create_and_drop_database_are_rejected_inside_transaction_blocks() {
    let base = temp_dir("database_ddl_txn_block");
    let cluster = Cluster::open(&base, 16).unwrap();
    let postgres = cluster.connect_database("postgres").unwrap();
    let mut session = Session::new(1);

    session.execute(&postgres, "begin").unwrap();
    match session.execute(&postgres, "create database txdb") {
        Err(ExecError::Parse(ParseError::ActiveSqlTransaction(stmt))) => {
            assert_eq!(stmt, "CREATE DATABASE");
        }
        other => panic!(
            "expected create database transaction error, got {:?}",
            other
        ),
    }
    session.execute(&postgres, "rollback").unwrap();

    session.execute(&postgres, "create database txdb").unwrap();
    session.execute(&postgres, "begin").unwrap();
    match session.execute(&postgres, "drop database txdb") {
        Err(ExecError::Parse(ParseError::ActiveSqlTransaction(stmt))) => {
            assert_eq!(stmt, "DROP DATABASE");
        }
        other => panic!("expected drop database transaction error, got {:?}", other),
    }
    session.execute(&postgres, "rollback").unwrap();
    session.execute(&postgres, "drop database txdb").unwrap();
}

#[test]
fn read_committed_isolation() {
    let base = temp_dir("read_committed");
    let db = Database::open(&base, 64).unwrap();
    let mut session_a = Session::new(1);
    let mut session_b = Session::new(2);

    session_a
        .execute(
            &db,
            "create table isotest (id int4 not null, val int4 not null)",
        )
        .unwrap();
    session_a
        .execute(&db, "insert into isotest (id, val) values (1, 100)")
        .unwrap();

    session_a.execute(&db, "begin").unwrap();
    session_a
        .execute(&db, "insert into isotest (id, val) values (2, 200)")
        .unwrap();

    match session_b
        .execute(&db, "select count(*) from isotest")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(1)]],
                "session B should not see session A's uncommitted insert"
            );
        }
        other => panic!("expected query, got {:?}", other),
    }

    session_a.execute(&db, "commit").unwrap();

    match session_b
        .execute(&db, "select count(*) from isotest")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(2)]],
                "session B should see session A's committed insert"
            );
        }
        other => panic!("expected query, got {:?}", other),
    }
}

/// Multiple threads each run a loop of BEGIN; UPDATE counter; COMMIT.
/// The row-level lock must be held for the duration of the explicit
/// transaction, so updates cannot interleave — the final value must equal
/// num_threads × iterations_per_thread.
#[test]
fn concurrent_transactions_update_counter() {
    let base = temp_dir("txn_update_counter");
    let db = Database::open(&base, 64).unwrap();

    db.execute(
        1,
        "create table counter (id int4 not null, val int4 not null)",
    )
    .unwrap();
    db.execute(1, "insert into counter (id, val) values (1, 0)")
        .unwrap();

    let num_threads = 4;
    let iters_per_thread = 10;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                let mut session = Session::new((t + 1500) as ClientId);
                for _ in 0..iters_per_thread {
                    session.execute(&db, "begin").unwrap();
                    session
                        .execute(&db, "update counter set val = val + 1 where id = 1")
                        .unwrap();
                    session.execute(&db, "commit").unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, TEST_TIMEOUT);

    let expected = num_threads * iters_per_thread;
    match db
        .execute(1, "select val from counter where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int32(expected as i32)]],
                "all transactional updates must be serialized — expected {expected}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn standalone_selects_do_not_allocate_xids() {
    let base = temp_dir("standalone_selects_no_xids");
    let db = Database::open(&base, 64).unwrap();
    let before = db.txns.read().next_xid();

    for _ in 0..5 {
        assert_eq!(query_rows(&db, 1, "select 1"), vec![vec![Value::Int32(1)]]);
    }

    assert_eq!(db.txns.read().next_xid(), before);
}

#[test]
fn begin_select_commit_does_not_allocate_xid() {
    let base = temp_dir("begin_select_commit_no_xid");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);
    let before = db.txns.read().next_xid();

    session.execute(&db, "begin").unwrap();
    assert_eq!(
        session_query_rows(&mut session, &db, "select 1"),
        vec![vec![Value::Int32(1)]]
    );
    session.execute(&db, "commit").unwrap();

    assert_eq!(db.txns.read().next_xid(), before);
}

#[test]
fn explicit_transaction_allocates_xid_on_first_write() {
    let base = temp_dir("lazy_xid_first_write");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table lazy_xid_items (id int4)")
        .unwrap();
    let before = db.txns.read().next_xid();

    session.execute(&db, "begin").unwrap();
    assert_eq!(
        session_query_rows(&mut session, &db, "select 1"),
        vec![vec![Value::Int32(1)]]
    );
    assert_eq!(db.txns.read().next_xid(), before);

    session
        .execute(&db, "insert into lazy_xid_items values (7)")
        .unwrap();
    assert!(
        db.txns.read().next_xid() > before,
        "first write should allocate a real xid"
    );
    assert_eq!(
        session_query_rows(&mut session, &db, "select id from lazy_xid_items"),
        vec![vec![Value::Int32(7)]]
    );

    session.execute(&db, "commit").unwrap();
}

#[test]
fn plpgsql_update_inside_autocommit_select_allocates_xid() {
    let base = temp_dir("plpgsql_update_inside_select_xid");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table lazy_fn_items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into lazy_fn_items values (1, 'old')")
        .unwrap();
    db.execute(
        1,
        "create function lazy_fn_update() returns int4 language plpgsql as $$ begin update lazy_fn_items set note = 'new' where id = 1; return 0; end $$",
    )
    .unwrap();
    let before = db.txns.read().next_xid();

    assert_eq!(
        query_rows(&db, 1, "select lazy_fn_update()"),
        vec![vec![Value::Int32(0)]]
    );
    assert_eq!(db.txns.read().next_xid(), before + 1);
    assert_eq!(
        query_rows(&db, 1, "select note from lazy_fn_items where id = 1"),
        vec![vec![Value::Text("new".into())]]
    );
}

#[test]
fn plpgsql_insert_inside_autocommit_select_commits() {
    let base = temp_dir("plpgsql_insert_inside_select_commits");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table lazy_fn_insert_items (id int4)")
        .unwrap();
    db.execute(
        1,
        "create function lazy_fn_insert() returns int4 language plpgsql as $$ begin insert into lazy_fn_insert_items values (7); return 0; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select lazy_fn_insert()"),
        vec![vec![Value::Int32(0)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select id from lazy_fn_insert_items"),
        vec![vec![Value::Int32(7)]]
    );
}

#[test]
fn plpgsql_delete_inside_autocommit_select_commits() {
    let base = temp_dir("plpgsql_delete_inside_select_commits");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table lazy_fn_delete_items (id int4)")
        .unwrap();
    db.execute(1, "insert into lazy_fn_delete_items values (7)")
        .unwrap();
    db.execute(
        1,
        "create function lazy_fn_delete() returns int4 language plpgsql as $$ begin delete from lazy_fn_delete_items where id = 7; return 0; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select lazy_fn_delete()"),
        vec![vec![Value::Int32(0)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select count(*) from lazy_fn_delete_items"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn plpgsql_write_inside_autocommit_select_error_aborts_xid() {
    let base = temp_dir("plpgsql_write_inside_select_abort");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table lazy_fn_abort_items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into lazy_fn_abort_items values (1, 'old')")
        .unwrap();
    db.execute(
        1,
        "create function lazy_fn_abort() returns int4 language plpgsql as $$ begin update lazy_fn_abort_items set note = 'new' where id = 1; raise exception 'boom'; end $$",
    )
    .unwrap();

    assert!(db.execute(1, "select lazy_fn_abort()").is_err());
    assert_eq!(
        query_rows(&db, 1, "select note from lazy_fn_abort_items where id = 1"),
        vec![vec![Value::Text("old".into())]]
    );
}

#[test]
fn plpgsql_insert_failure_inside_autocommit_select_has_xid_and_rolls_back() {
    let base = temp_dir("plpgsql_insert_failure_xid");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table lazy_bad_insert_items (id int4 not null)")
        .unwrap();
    db.execute(
        1,
        "create function lazy_bad_insert() returns int4 language plpgsql as $$ begin insert into lazy_bad_insert_items values (null); return 0; end $$",
    )
    .unwrap();

    let err = db.execute(1, "select lazy_bad_insert()").unwrap_err();
    let err = match err {
        ExecError::WithContext { source, .. } => *source,
        err => err,
    };
    assert!(matches!(
        err,
        ExecError::NotNullViolation { column, .. } if column == "id"
    ));
    assert_eq!(
        query_rows(&db, 1, "select count(*) from lazy_bad_insert_items"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn plpgsql_write_inside_explicit_transaction_select_allocates_xid() {
    let base = temp_dir("plpgsql_write_inside_explicit_select_xid");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table lazy_session_fn_items (id int4, note text)",
        )
        .unwrap();
    session
        .execute(&db, "insert into lazy_session_fn_items values (1, 'old')")
        .unwrap();
    session
        .execute(
            &db,
            "create function lazy_session_fn_update() returns int4 language plpgsql as $$ begin update lazy_session_fn_items set note = 'new' where id = 1; return 0; end $$",
        )
        .unwrap();
    let before = db.txns.read().next_xid();

    session.execute(&db, "begin").unwrap();
    assert_eq!(
        session_query_rows(&mut session, &db, "select 1"),
        vec![vec![Value::Int32(1)]]
    );
    assert_eq!(db.txns.read().next_xid(), before);
    assert_eq!(
        session_query_rows(&mut session, &db, "select lazy_session_fn_update()"),
        vec![vec![Value::Int32(0)]]
    );
    assert_eq!(db.txns.read().next_xid(), before + 1);
    session.execute(&db, "commit").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select note from lazy_session_fn_items where id = 1"
        ),
        vec![vec![Value::Text("new".into())]]
    );
}

#[test]
fn transaction_scoped_advisory_lock_does_not_allocate_xid() {
    let base = temp_dir("advisory_xact_lock_no_xid");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);
    let before = db.txns.read().next_xid();

    session.execute(&db, "begin").unwrap();
    let _ = session_query_rows(&mut session, &db, "select pg_advisory_xact_lock(42)");
    assert_eq!(db.txns.read().next_xid(), before);
    assert_eq!(db.advisory_locks.snapshot().len(), 1);

    session.execute(&db, "commit").unwrap();
    assert_eq!(db.txns.read().next_xid(), before);
    assert!(db.advisory_locks.snapshot().is_empty());
}

#[test]
fn pg_notify_in_read_only_transaction_does_not_allocate_xid() {
    let base = temp_dir("pg_notify_read_only_no_xid");
    let db = Database::open(&base, 64).unwrap();
    let mut notifier = Session::new(1);
    let mut listener = Session::new(2);
    listener.execute(&db, "listen alerts").unwrap();
    let before = db.txns.read().next_xid();

    notifier.execute(&db, "begin").unwrap();
    let _ = session_query_rows(&mut notifier, &db, "select pg_notify('alerts', 'payload')");
    assert_eq!(db.txns.read().next_xid(), before);
    assert!(db.async_notify_runtime.pending_notifications(2).is_empty());

    notifier.execute(&db, "commit").unwrap();
    assert_eq!(db.txns.read().next_xid(), before);
    let delivered = db.async_notify_runtime.pending_notifications(2);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].channel, "alerts");
    assert_eq!(delivered[0].payload, "payload");
}

/// Within a single BEGIN block, a row inserted earlier in the transaction
/// must be visible to a SELECT issued later in the same transaction
/// (read-your-own-writes / command-id visibility).
#[test]
fn read_your_own_writes_within_transaction() {
    let base = temp_dir("read_own_writes");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table rowtable (id int4 not null, val int4 not null)",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into rowtable (id, val) values (1, 42)")
        .unwrap();

    // The insert is not yet committed, but the same session must see it.
    match session
        .execute(&db, "select val from rowtable where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int32(42)]],
                "own uncommitted insert must be visible within the transaction"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    session.execute(&db, "commit").unwrap();

    // After commit the row must still be there.
    match session
        .execute(&db, "select count(*) from rowtable")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn read_your_own_updates_within_transaction() {
    let base = temp_dir("read_own_updates");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create table rowtable (id int4 not null, val int4 not null)",
        )
        .unwrap();

    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into rowtable (id, val) values (1, 42)")
        .unwrap();
    session
        .execute(&db, "update rowtable set val = 7 where id = 1")
        .unwrap();

    match session
        .execute(&db, "select val from rowtable where id = 1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(7)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    session
        .execute(&db, "delete from rowtable where id = 1")
        .unwrap();
    match session
        .execute(&db, "select count(*) from rowtable")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    session.execute(&db, "commit").unwrap();
}

/// Each thread runs one explicit transaction that inserts a batch of rows.
/// No row must be lost: the final count must equal num_threads × batch_size,
/// even though all transactions overlap in time.
#[test]
fn concurrent_transactions_bulk_insert() {
    let base = temp_dir("txn_bulk_insert");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table bulk (id int4 not null)")
        .unwrap();

    let num_threads = 4;
    let batch_size = 10;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                let mut session = Session::new((t + 1600) as ClientId);
                session.execute(&db, "begin").unwrap();
                for i in 0..batch_size {
                    let id = t * 10_000 + i;
                    session
                        .execute(&db, &format!("insert into bulk (id) values ({id})"))
                        .unwrap();
                }
                session.execute(&db, "commit").unwrap();
            })
        })
        .collect();

    join_all_with_timeout(handles, TEST_TIMEOUT);

    let expected = (num_threads * batch_size) as i32;
    match db.execute(1, "select count(*) from bulk").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(expected as i64)]],
                "all bulk-inserted rows must survive — expected {expected}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Thread A opens a transaction and inserts a row, then waits for a signal
/// before committing.  Thread B reads the table repeatedly while A is still
/// in progress — it must never see A's uncommitted row (no dirty reads).
/// After A commits, B must see the row.
#[test]
fn no_dirty_reads_concurrent() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    let base = temp_dir("no_dirty_reads");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table dirty (id int4 not null)")
        .unwrap();

    // Shared flags: writer signals when it has inserted (but not committed),
    // and when it has committed.
    let inserted = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicBool::new(false));
    let (commit_tx, commit_rx) = mpsc::channel();

    let inserted_w = inserted.clone();
    let committed_w = committed.clone();
    let db_w = db.clone();

    let writer = thread::spawn(move || {
        let mut session = Session::new(1700);
        session.execute(&db_w, "begin").unwrap();
        session
            .execute(&db_w, "insert into dirty (id) values (1)")
            .unwrap();
        inserted_w.store(true, Ordering::Release);
        commit_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("reader should allow writer to commit");
        session.execute(&db_w, "commit").unwrap();
        committed_w.store(true, Ordering::Release);
    });

    // Reader: spin until writer has inserted, then verify no dirty read.
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !inserted.load(Ordering::Acquire) {
        if Instant::now() > deadline {
            panic!("timed out waiting for writer to insert");
        }
        std::hint::spin_loop();
    }

    // While writer is still in progress, we must see 0 rows.
    match db.execute(1800, "select count(*) from dirty").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(0)]],
                "must not see uncommitted row (dirty read)"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    commit_tx.send(()).unwrap();
    writer
        .join()
        .unwrap_or_else(|e| std::panic::resume_unwind(e));

    assert!(committed.load(Ordering::Acquire));

    // After commit, the row must now be visible.
    match db.execute(1800, "select count(*) from dirty").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(1)]],
                "committed row must be visible after commit"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Half the threads commit their transaction; half roll it back.
/// Only the committed threads' rows must survive.
#[test]
fn concurrent_mixed_commit_and_rollback() {
    let base = temp_dir("mixed_commit_rollback");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table mixed (id int4 not null)")
        .unwrap();

    let num_threads = 6; // must be even
    let rows_per_thread = 5;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                let mut session = Session::new((t + 1900) as ClientId);
                session.execute(&db, "begin").unwrap();
                for i in 0..rows_per_thread {
                    let id = t * 10_000 + i;
                    session
                        .execute(&db, &format!("insert into mixed (id) values ({id})"))
                        .unwrap();
                }
                // Even-numbered threads commit; odd-numbered threads roll back.
                if t % 2 == 0 {
                    session.execute(&db, "commit").unwrap();
                } else {
                    session.execute(&db, "rollback").unwrap();
                }
            })
        })
        .collect();

    join_all_with_timeout(handles, TEST_TIMEOUT);

    let committing_threads = num_threads / 2; // threads 0, 2, 4, …
    let expected = (committing_threads * rows_per_thread) as i32;
    match db.execute(1, "select count(*) from mixed").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Int64(expected as i64)]],
                "only committed rows should survive — expected {expected}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

/// Reproduces a lock-ordering deadlock between the write-preferring
/// `txns` RwLock and the write-preferring buffer `content_lock` RwLock.
///
/// The SELECT path acquires content_lock(shared) → txns.read(),
/// while the UPDATE scan path acquires txns.read() → content_lock(shared).
/// With pending exclusive waiters on both locks, this creates a cycle.
///
/// Many readers + writers on a single-row table maximises the chance
/// that all four roles (R, W, R2, WW) overlap on the same page.
#[test]
fn lock_ordering_deadlock_repro() {
    let base = temp_dir("lock_ordering_deadlock");
    let db = Database::open(&base, 16).unwrap();

    db.execute(
        1,
        "create table locktest (id int4 not null, val int4 not null)",
    )
    .unwrap();
    db.execute(1, "insert into locktest (id, val) values (1, 0)")
        .unwrap();

    let num_readers = 8;
    let num_writers = 4;
    let mut handles = Vec::new();

    for t in 0..num_writers {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..30 {
                db.execute(
                    (t + 2000) as ClientId,
                    "update locktest set val = val + 1 where id = 1",
                )
                .unwrap();
            }
        }));
    }

    for t in 0..num_readers {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let _ = db
                    .execute(
                        (t + 3000) as ClientId,
                        "select val from locktest where id = 1",
                    )
                    .unwrap();
            }
        }));
    }

    join_all_with_timeout(handles, PIN_LEAK_CONTENTION_TEST_TIMEOUT);
}

#[test]
fn no_pins_leaked_concurrent_contention() {
    // The cold table accumulates dead versions (no vacuum), so scans get
    // slower with bloat; a small pool adds eviction pressure on top.
    let base = temp_dir("no_pins_concurrent");
    let db = Database::open(&base, 128).unwrap();

    // Create two tables so threads contend on the same rows from
    // different directions (readers vs writers, writers vs writers).
    db.execute(1, "create table hot (id int4 not null, val int4 not null)")
        .unwrap();
    db.execute(1, "create table cold (id int4 not null, val int4 not null)")
        .unwrap();
    for i in 0..20 {
        db.execute(1, &format!("insert into hot (id, val) values ({i}, 0)"))
            .unwrap();
        db.execute(1, &format!("insert into cold (id, val) values ({i}, 0)"))
            .unwrap();
    }

    let num_threads = 8;
    let iters = 50;
    let mut handles = Vec::new();

    // Writers: all contend on the same hot rows.
    for t in 0..num_threads / 2 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let client = (t + 10) as ClientId;
            for i in 0..iters {
                let row = i % 5; // contend on rows 0-4
                let _ = db.execute(
                    client,
                    &format!("update hot set val = val + 1 where id = {row}"),
                );
                // Full-table scan to pin many pages at once.
                let _ = db.execute(client, "select * from hot");
            }
        }));
    }

    // Readers + cross-table writers: read hot, write cold.
    for t in 0..num_threads / 2 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let client = (t + 20) as ClientId;
            for i in 0..iters {
                let row = i % 5;
                let _ = db.execute(client, "select count(*) from hot");
                let _ = db.execute(
                    client,
                    &format!("update cold set val = val + 1 where id = {row}"),
                );
                // Delete + reinsert to force page layout changes.
                let _ = db.execute(client, &format!("delete from cold where id = {}", (i % 20)));
                let _ = db.execute(
                    client,
                    &format!("insert into cold (id, val) values ({}, {})", i % 20, i),
                );
            }
        }));
    }

    join_all_with_timeout(handles, PIN_LEAK_CONTENTION_TEST_TIMEOUT);

    // After all threads finish, no pins should remain.
    let capacity = db.pool.capacity();
    let mut pinned = Vec::new();
    for buffer_id in 0..capacity {
        if let Some(state) = db.pool.buffer_state(buffer_id) {
            if state.pin_count > 0 {
                pinned.push((buffer_id, state));
            }
        }
    }
    assert!(
        pinned.is_empty(),
        "buffer pin leak: {} buffer(s) still pinned after concurrent workload:\n{:#?}",
        pinned.len(),
        pinned,
    );
}

#[test]
fn concurrent_same_row_updates_do_not_deadlock() {
    let base = temp_dir("no_deadlock_same_row");
    let db = Database::open(&base, 64).unwrap();
    db.execute(1, "create table t (id int4 not null, val int4 not null)")
        .unwrap();
    db.execute(1, "insert into t (id, val) values (1, 0)")
        .unwrap();

    let num_threads = 4;
    let iters = 200;
    let mut handles = Vec::new();
    for t in 0..num_threads {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let client = (t + 10) as ClientId;
            for _ in 0..iters {
                db.execute(client, "update t set val = val + 1 where id = 1")
                    .unwrap();
            }
        }));
    }
    join_all_with_timeout(handles, SAME_ROW_UPDATE_FULL_SUITE_TIMEOUT);

    let result = db.execute(1, "select val from t where id = 1").unwrap();
    let expected = num_threads * iters;
    match result {
        StatementResult::Query { rows, .. } => {
            let val = match &rows[0][0] {
                crate::backend::executor::Value::Int32(v) => *v,
                other => panic!("expected Int32, got {other:?}"),
            };
            assert_eq!(
                val, expected as i32,
                "expected val={expected} after {num_threads} threads x {iters} increments"
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn no_pins_leaked_after_queries() {
    let base = temp_dir("no_pins_leaked");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    // Set up schema and data.
    session
        .execute(
            &db,
            "create table pintest (id int4 not null, val int4 not null)",
        )
        .unwrap();
    for i in 0..50 {
        session
            .execute(
                &db,
                &format!("insert into pintest (id, val) values ({i}, {i})"),
            )
            .unwrap();
    }

    // Run a variety of query types.
    session.execute(&db, "select * from pintest").unwrap();
    session
        .execute(&db, "select count(*) from pintest")
        .unwrap();
    session
        .execute(&db, "select id, val from pintest where id > 10")
        .unwrap();
    session
        .execute(&db, "select id + val from pintest")
        .unwrap();
    session
        .execute(&db, "update pintest set val = val + 1 where id = 1")
        .unwrap();
    session.execute(&db, "update pintest set val = 0").unwrap();
    session
        .execute(&db, "delete from pintest where id > 40")
        .unwrap();

    // Explicit transaction.
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into pintest (id, val) values (999, 999)")
        .unwrap();
    session
        .execute(&db, "select * from pintest where id = 999")
        .unwrap();
    session.execute(&db, "commit").unwrap();

    // Rolled-back transaction.
    session.execute(&db, "begin").unwrap();
    session
        .execute(&db, "insert into pintest (id, val) values (1000, 1000)")
        .unwrap();
    session.execute(&db, "rollback").unwrap();

    // Assert no pins are held anywhere in the buffer pool.
    let capacity = db.pool.capacity();
    let mut pinned = Vec::new();
    for buffer_id in 0..capacity {
        if let Some(state) = db.pool.buffer_state(buffer_id) {
            if state.pin_count > 0 {
                pinned.push((buffer_id, state));
            }
        }
    }
    assert!(
        pinned.is_empty(),
        "buffer pin leak: {} buffer(s) still pinned after all queries completed:\n{:#?}",
        pinned.len(),
        pinned,
    );
}

#[test]
fn streaming_select_supports_correlated_subqueries() {
    let base = temp_dir("streaming_correlated_subquery");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table people (id int4 not null, name text)")
        .unwrap();
    session
        .execute(
            &db,
            "create table pets (id int4 not null, owner_id int4, name text)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "insert into people (id, name) values (1, 'alice'), (2, 'bob'), (3, 'carol')",
        )
        .unwrap();
    session
            .execute(&db, "insert into pets (id, owner_id, name) values (10, 1, 'mocha'), (11, 1, 'pixel'), (12, 2, 'otis')")
            .unwrap();

    let stmt = crate::backend::parser::parse_select(
            "select p.id, (select count(*) from pets q where q.owner_id = p.id) from people p order by p.id",
        )
        .unwrap();
    let mut guard = session.execute_streaming(&db, &stmt).unwrap();
    let mut rows = Vec::new();
    while let Some(slot) =
        crate::backend::executor::exec_next(&mut guard.state, &mut guard.ctx).unwrap()
    {
        rows.push(
            slot.values()
                .unwrap()
                .iter()
                .map(|v| v.to_owned_value())
                .collect::<Vec<_>>(),
        );
    }
    drop(guard);

    assert_eq!(
        rows,
        vec![
            vec![Value::Int32(1), Value::Int64(2)],
            vec![Value::Int32(2), Value::Int64(1)],
            vec![Value::Int32(3), Value::Int64(0)],
        ]
    );
}

#[test]
fn streaming_correlated_subquery_holds_access_share_lock_on_inner_relation() {
    use std::sync::mpsc;

    let base = temp_dir("streaming_correlated_subquery_lock");
    let db = Database::open(&base, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table people (id int4 not null, name text)")
        .unwrap();
    session
        .execute(
            &db,
            "create table pets (id int4 not null, owner_id int4, name text)",
        )
        .unwrap();
    session
        .execute(&db, "insert into people (id, name) values (1, 'alice')")
        .unwrap();
    session
        .execute(
            &db,
            "insert into pets (id, owner_id, name) values (10, 1, 'mocha')",
        )
        .unwrap();

    let stmt = crate::backend::parser::parse_select(
        "select p.id, exists (select 1 from pets q where q.owner_id = p.id) from people p",
    )
    .unwrap();
    let guard = session.execute_streaming(&db, &stmt).unwrap();

    let db2 = db.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        started_tx.send(()).unwrap();
        db2.execute(2, "truncate pets").unwrap();
        done_tx.send(()).unwrap();
    });

    started_rx.recv().unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "truncate should block while the streaming guard holds the inner relation lock"
    );

    drop(guard);
    done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    worker.join().unwrap();
}

#[test]
fn create_function_scalar_calls_work_in_select_and_where() {
    let dir = temp_dir("create_function_scalar");
    let db = Database::open(&dir, 64).unwrap();

    match db
        .execute(
            1,
            "create function inc(x int4) returns int4 language plpgsql as $$ begin return x + 1; end $$",
        )
        .unwrap()
    {
        StatementResult::AffectedRows(0) => {}
        other => panic!("expected create function affected rows, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select inc(4), inc(4) = 5"),
        vec![vec![Value::Int32(5), Value::Bool(true)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select inc(1) where inc(1) = 2"),
        vec![vec![Value::Int32(2)]]
    );
}

#[test]
fn create_or_replace_function_updates_existing_body() {
    let dir = temp_dir("create_or_replace_function");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function inc(x int4) returns int4 language plpgsql as $$ begin return x + 1; end $$",
    )
    .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select inc(4)"),
        vec![vec![Value::Int32(5)]]
    );

    db.execute(
        1,
        "create or replace function inc(x int4) returns int4 language plpgsql as $$ begin return x + 2; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select inc(4)"),
        vec![vec![Value::Int32(6)]]
    );
}

#[test]
fn grant_all_on_schema_public_is_accepted() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    match session
        .execute(&db, "grant all on schema public to public")
        .unwrap()
    {
        StatementResult::AffectedRows(0) => {}
        other => panic!("expected grant affected rows, got {other:?}"),
    }
}

#[test]
fn durable_bootstrap_preserves_public_schema_grants() {
    let base = temp_dir("durable_public_schema_grant");
    let db = Database::open(&base, 16).expect("open durable database");
    let mut session = Session::new(1);

    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_namespace where nspname = 'public'",
        ),
        vec![vec![Value::Int64(1)]]
    );

    match session
        .execute(&db, "grant all on schema public to public")
        .unwrap()
    {
        StatementResult::AffectedRows(0) => {}
        other => panic!("expected grant affected rows, got {other:?}"),
    }
}

#[test]
fn grant_select_on_table_is_accepted() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create table widgets (id int4)")
        .unwrap();
    match session
        .execute(&db, "grant select on widgets to public")
        .unwrap()
    {
        StatementResult::AffectedRows(0) => {}
        other => panic!("expected grant affected rows, got {other:?}"),
    }
}

#[test]
fn grant_execute_on_function_is_accepted() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(
            &db,
            "create function add_one(x int4) returns int4 language sql as $$ select x + 1 $$",
        )
        .unwrap();
    match session
        .execute(&db, "grant execute on function add_one(int4) to public")
        .unwrap()
    {
        StatementResult::AffectedRows(0) => {}
        other => panic!("expected grant affected rows, got {other:?}"),
    }
}

#[test]
fn create_alter_and_drop_policy_updates_pg_policy() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create role app_role nologin")
        .unwrap();
    session
        .execute(&db, "create table items (a int4, owner text)")
        .unwrap();
    session
        .execute(
            &db,
            "create policy p1 on items as restrictive for select to app_role using (a > 0)",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select polname, polqual from pg_policy where polname = 'p1'",
        ),
        vec![vec![Value::Text("p1".into()), Value::Text("a > 0".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select policyname, roles, qual, with_check
             from pg_policies
             where policyname = 'p1'",
        ),
        vec![vec![
            Value::Text("p1".into()),
            typed_text_array_value(&["app_role"], crate::include::catalog::NAME_TYPE_OID),
            Value::Text("a > 0".into()),
            Value::Null,
        ]]
    );

    session
        .execute(&db, "alter policy p1 on items rename to p2")
        .unwrap();
    session
        .execute(
            &db,
            "alter policy p2 on items using (a > 1) with check (a > 2)",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select polname, polqual, polwithcheck from pg_policy where polname = 'p2'",
        ),
        vec![vec![
            Value::Text("p2".into()),
            Value::Text("a > 1".into()),
            Value::Text("a > 2".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select policyname, roles, qual, with_check
             from pg_policies
             where policyname = 'p2'",
        ),
        vec![vec![
            Value::Text("p2".into()),
            typed_text_array_value(&["app_role"], crate::include::catalog::NAME_TYPE_OID),
            Value::Text("a > 1".into()),
            Value::Text("a > 2".into()),
        ]]
    );

    session.execute(&db, "drop policy p2 on items").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_policy where polname = 'p2'"
        ),
        vec![vec![Value::Int64(0)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_policies where policyname = 'p2'"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn pg_policies_exposes_public_and_named_role_policies() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create role app_role nologin")
        .unwrap();
    session
        .execute(&db, "create role report_role nologin")
        .unwrap();
    session
        .execute(&db, "create table items (a int4, owner text)")
        .unwrap();
    session
        .execute(
            &db,
            "create policy p_named on items for select to report_role, app_role using (a > 0)",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create policy p_public on items for insert to public with check (a > 1)",
        )
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select schemaname, tablename, policyname, permissive, roles, cmd, qual, with_check
             from pg_policies
             order by policyname",
        ),
        vec![
            vec![
                Value::Text("public".into()),
                Value::Text("items".into()),
                Value::Text("p_named".into()),
                Value::Text("PERMISSIVE".into()),
                typed_text_array_value(
                    &["app_role", "report_role"],
                    crate::include::catalog::NAME_TYPE_OID,
                ),
                Value::Text("SELECT".into()),
                Value::Text("a > 0".into()),
                Value::Null,
            ],
            vec![
                Value::Text("public".into()),
                Value::Text("items".into()),
                Value::Text("p_public".into()),
                Value::Text("PERMISSIVE".into()),
                typed_text_array_value(&["public"], crate::include::catalog::NAME_TYPE_OID),
                Value::Text("INSERT".into()),
                Value::Null,
                Value::Text("a > 1".into()),
            ],
        ]
    );
}

#[test]
fn pg_policies_query_succeeds_on_fresh_database() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");

    assert!(query_rows(&db, 1, "select * from pg_policies").is_empty());
}

#[test]
fn create_tablespace_adds_pg_tablespace_row() {
    let dir = temp_dir("create_tablespace_adds_pg_tablespace_row");
    let db = Database::open(&dir, 32).expect("open database");
    let mut session = Session::new(1);
    session
        .execute(&db, "set allow_in_place_tablespaces = true")
        .unwrap();

    match session
        .execute(&db, "create tablespace regress_tblspace location ''")
        .unwrap()
    {
        StatementResult::AffectedRows(0) => {}
        other => panic!("expected create tablespace affected rows, got {other:?}"),
    }

    assert_eq!(
        query_rows(
            &db,
            1,
            "select oid, spcname from pg_tablespace where spcname = 'regress_tblspace'",
        ),
        vec![vec![
            Value::Int64(16384),
            Value::Text("regress_tblspace".into()),
        ]]
    );
    let tablespace_oid = match &query_rows(
        &db,
        1,
        "select oid from pg_tablespace where spcname = 'regress_tblspace'",
    )[0][0]
    {
        Value::Int64(oid) => *oid as u32,
        other => panic!("expected oid row, got {other:?}"),
    };
    assert!(
        dir.join("pg_tblspc")
            .join(tablespace_oid.to_string())
            .join("PG_18_202406281")
            .is_dir()
    );
}

#[test]
fn create_tablespace_rejects_empty_location_without_guc() {
    let dir = temp_dir("create_tablespace_rejects_empty_location_without_guc");
    let db = Database::open(&dir, 32).expect("open database");
    let mut session = Session::new(1);

    let err = session
        .execute(&db, "create tablespace regress_tblspace location ''")
        .unwrap_err();
    match err {
        ExecError::DetailedError { message, .. } => {
            assert_eq!(message, "tablespace location must be an absolute path");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }
}

#[test]
fn create_tablespace_absolute_location_creates_symlinked_version_dir() {
    let dir = temp_dir("create_tablespace_absolute_location");
    let tablespace_dir = dir.join("external_tablespace");
    fs::create_dir_all(&tablespace_dir).unwrap();

    let db = Database::open(&dir, 32).expect("open database");
    let mut session = Session::new(1);
    let sql = format!(
        "create tablespace regress_tblspace location '{}'",
        tablespace_dir.display()
    );

    session.execute(&db, &sql).unwrap();

    let tablespace_oid = match &query_rows(
        &db,
        1,
        "select oid from pg_tablespace where spcname = 'regress_tblspace'",
    )[0][0]
    {
        Value::Int64(oid) => *oid as u32,
        other => panic!("expected oid row, got {other:?}"),
    };
    let link_path = dir.join("pg_tblspc").join(tablespace_oid.to_string());
    assert!(link_path.exists());
    assert!(tablespace_dir.join("PG_18_202406281").is_dir());
}

#[test]
fn create_function_scalar_elsif_branches_work() {
    let dir = temp_dir("create_function_scalar_elsif");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function classify_size(x int4) returns text language plpgsql as $$ begin if x >= 10 then return 'large'; elsif x >= 5 then return 'medium'; else return 'small'; end if; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select classify_size(12), classify_size(7), classify_size(3)",
        ),
        vec![vec![
            Value::Text("large".into()),
            Value::Text("medium".into()),
            Value::Text("small".into()),
        ]]
    );
}

#[test]
fn create_function_setof_scalar_works_in_from_and_project_set() {
    let dir = temp_dir("create_function_setof_scalar");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function pair_series(x int4) returns setof int4 language plpgsql as $$ begin return next x; return next x + 1; return; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select * from pair_series(3)"),
        vec![vec![Value::Int32(3)], vec![Value::Int32(4)]]
    );
    assert_eq!(
        query_rows(&db, 1, "select pair_series(3)"),
        vec![vec![Value::Int32(3)], vec![Value::Int32(4)]]
    );
}

#[test]
fn create_function_supports_void_returns_and_regprocedure_oid_lookup() {
    let dir = temp_dir("create_function_void_regprocedure");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function stats_test_func1() returns void language plpgsql as $$ begin return; end $$",
    )
    .unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    let proc = visible
        .proc_rows_by_name("stats_test_func1")
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select p.proname, t.typname from pg_proc p join pg_type t on t.oid = p.prorettype where p.proname = 'stats_test_func1'",
        ),
        vec![vec![
            Value::Text("stats_test_func1".into()),
            Value::Text("void".into()),
        ]]
    );
    assert_eq!(
        query_rows(&db, 1, "select stats_test_func1()"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        query_rows(&db, 1, "select 'stats_test_func1()'::regprocedure::oid"),
        vec![vec![Value::Int64(proc.oid as i64)]]
    );
}

#[test]
fn role_name_literal_cast_supports_regrole() {
    let dir = temp_dir("regrole_literal_cast");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create role app_role").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select 'app_role'::regrole::oid"),
        vec![vec![Value::Int64(role_oid(&db, "app_role") as i64)]]
    );
}

#[test]
fn regrole_cast_to_text_renders_role_name() {
    let dir = temp_dir("regrole_text_cast");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create role app_role").unwrap();
    db.execute(1, "create role app_member").unwrap();
    db.execute(1, "grant app_role to app_member").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select 'app_role'::regrole::text"),
        vec![vec![Value::Text("app_role".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select oid::regrole::text from pg_authid where rolname = 'app_role'",
        ),
        vec![vec![Value::Text("app_role".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select member::regrole::text, grantor::regrole::text \
             from pg_auth_members where roleid = 'app_role'::regrole",
        ),
        vec![vec![
            Value::Text("app_member".into()),
            Value::Text("postgres".into()),
        ]]
    );
}

#[test]
fn pg_get_userbyid_returns_role_name() {
    let dir = temp_dir("pg_get_userbyid");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create role app_role login").unwrap();
    let oid = role_oid(&db, "app_role");

    assert_eq!(
        query_rows(&db, 1, &format!("select pg_get_userbyid({oid})")),
        vec![vec![Value::Text("app_role".into())]]
    );
}

#[test]
fn pg_get_viewdef_returns_canonical_view_query() {
    let dir = temp_dir("pg_get_viewdef");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table t1 (f1 int4)").unwrap();
    db.execute(1, "create table t2 (f1 int4)").unwrap();
    db.execute(
        1,
        "create view v1 as select f1 from t1 left join t2 using (f1) group by f1",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select pg_get_viewdef('v1'::regclass)"),
        vec![vec![Value::Text(
            " SELECT (f1)::integer AS f1\n   FROM (t1\n      LEFT JOIN t2 USING (f1))\n  GROUP BY f1;"
                .into()
        )]]
    );
}

#[test]
fn pg_get_acl_returns_relation_owner_acl() {
    let dir = temp_dir("pg_get_acl");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create role app_reader login").unwrap();
    db.execute(1, "create table acl_test(id int)").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select pg_get_acl('pg_class'::regclass, 'acl_test'::regclass::oid, 0)"
        ),
        vec![vec![Value::Null]]
    );
    db.execute(1, "grant select on acl_test to app_reader")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select unnest(pg_get_acl('pg_class'::regclass, 'acl_test'::regclass::oid, 0))",
        ),
        vec![
            vec![Value::Text("postgres=arwdDxtm/postgres".into())],
            vec![Value::Text("app_reader=r/postgres".into())],
        ]
    );
    db.execute(1, "revoke all privileges on acl_test from app_reader")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select pg_get_acl('pg_class'::regclass, 'acl_test'::regclass::oid, 0)"
        ),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        query_rows(&db, 1, "select pg_get_acl('pg_class'::regclass, 0, 0)"),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        query_rows(&db, 1, "select pg_get_acl(0, 0, 0)"),
        vec![vec![Value::Null]]
    );
}
fn current_database_function_matches_pg_database_name() {
    let dir = temp_dir("current_database_function");
    let db = Database::open(&dir, 64).unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select oid as datoid from pg_database where datname = current_database()"
        ),
        vec![vec![Value::Int64(i64::from(
            crate::include::catalog::CURRENT_DATABASE_OID,
        ))]]
    );
}

#[test]
fn pg_catalog_array_length_resolves_builtin_function() {
    let dir = temp_dir("pg_catalog_array_length");
    let db = Database::open(&dir, 64).unwrap();

    assert_eq!(
        query_rows(&db, 1, "select pg_catalog.array_length(array[1,2,3], 1)"),
        vec![vec![Value::Int32(3)]]
    );
}

#[test]
fn regproc_cast_aliases_resolve_in_queries() {
    let dir = temp_dir("regproc_cast_aliases");
    let db = Database::open(&dir, 64).unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select 6403::regproc::text, 6403::pg_catalog.regproc::text"
        ),
        vec![vec![
            Value::Text("pg_rust_test_fdw_handler".into()),
            Value::Text("pg_rust_test_fdw_handler".into()),
        ]]
    );
}

#[test]
fn session_user_and_current_role_are_sql_visible() {
    let dir = temp_dir("session_user_current_role");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create role tenant login").unwrap();
    db.execute(1, "create role manager").unwrap();
    db.execute(1, "grant manager to tenant").unwrap();
    db.execute(1, "set session authorization tenant").unwrap();

    match db
        .execute(
            1,
            "select session_user, current_role, current_user, current_setting('role') as role",
        )
        .unwrap()
    {
        StatementResult::Query { column_names, .. } => assert_eq!(
            column_names,
            vec!["session_user", "current_role", "current_user", "role"]
        ),
        other => panic!("expected query result, got {other:?}"),
    }

    assert_eq!(
        query_rows(&db, 1, "select session_user, current_user, current_role"),
        vec![vec![
            Value::Text("tenant".into()),
            Value::Text("tenant".into()),
            Value::Text("tenant".into()),
        ]]
    );
    assert_eq!(
        query_rows(&db, 1, "select current_setting('role')"),
        vec![vec![Value::Text("none".into())]]
    );

    db.execute(1, "set role manager").unwrap();

    assert_eq!(
        query_rows(&db, 1, "select session_user, current_user, current_role"),
        vec![vec![
            Value::Text("tenant".into()),
            Value::Text("manager".into()),
            Value::Text("manager".into()),
        ]]
    );
    assert_eq!(
        query_rows(&db, 1, "select current_setting('role')"),
        vec![vec![Value::Text("manager".into())]]
    );

    db.execute(1, "reset role").unwrap();
    assert_eq!(
        query_rows(&db, 1, "select current_setting('role')"),
        vec![vec![Value::Text("none".into())]]
    );
}

#[test]
fn create_function_row_returns_work_for_table_and_record() {
    let dir = temp_dir("create_function_row_returns");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function pair_rows(n int4) returns table(a int4, b text) language plpgsql as $$ begin a := n; b := 'left'; return next; return query values (n + 1, 'right'); end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function dyn_pair(n int4) returns setof record language plpgsql as $$ begin return query values (n, 'dyn'); end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select * from pair_rows(7)"),
        vec![
            vec![Value::Int32(7), Value::Text("left".into())],
            vec![Value::Int32(8), Value::Text("right".into())],
        ]
    );
    assert_eq!(
        query_rows(&db, 1, "select * from dyn_pair(9) as t(a int4, b text)"),
        vec![vec![Value::Int32(9), Value::Text("dyn".into())]]
    );
}

#[test]
fn create_function_nonset_record_composite_and_multi_out_work() {
    let dir = temp_dir("create_function_nonset_row_returns");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table one_widget (id int4, label text)")
        .unwrap();
    db.execute(
        1,
        "create function one_widget_row(n int4) returns one_widget language plpgsql as $$ begin return row(n, 'widget'); end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function one_record_row(n int4) returns record language plpgsql as $$ begin return row(n, 'record'); end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function one_out_row(n int4, out a int4, out b text) language plpgsql as $$ begin a := n; b := 'out'; return; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select * from one_widget_row(5)"),
        vec![vec![Value::Int32(5), Value::Text("widget".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select * from one_record_row(6) as t(a int4, b text)"
        ),
        vec![vec![Value::Int32(6), Value::Text("record".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select * from one_out_row(7)"),
        vec![vec![Value::Int32(7), Value::Text("out".into())]]
    );
}

#[test]
fn plpgsql_refcursor_open_fetch_close_work() {
    let dir = temp_dir("plpgsql_refcursor_open_fetch_close");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table pl_cursor_items (id int4)")
        .unwrap();
    db.execute(1, "insert into pl_cursor_items values (1), (2), (3)")
        .unwrap();
    db.execute(
        1,
        "create function cursor_total() returns int4 language plpgsql as $$
            declare
                c cursor for select id from pl_cursor_items order by id;
                v int4;
                total int4 := 0;
            begin
                open c;
                fetch c into v;
                total := total + v;
                fetch c into v;
                total := total + v;
                close c;
                return total;
            end
        $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select cursor_total()"),
        vec![vec![Value::Int32(3)]]
    );
}

#[test]
fn drop_function_ignores_argument_names_and_out_only_modes() {
    let dir = temp_dir("drop_function_mode_signature");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function drop_mode_sig(in a int4, inout b int4, out c text) language plpgsql as $$ begin b := a + b; c := b::text; return; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "drop function drop_mode_sig(in a int4, inout b int4, out c text)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select count(*) from pg_proc where proname = 'drop_mode_sig'"
        ),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn plpgsql_savepoint_reports_unsupported_transaction_command() {
    let dir = temp_dir("plpgsql_savepoint_unsupported");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function bad_savepoint() returns void language plpgsql as $$ begin savepoint s; end $$",
    )
    .unwrap();

    let err = db.execute(1, "select bad_savepoint()").unwrap_err();
    let err = match err {
        ExecError::WithContext { source, context } => {
            assert!(context.contains("PL/pgSQL function bad_savepoint"));
            assert!(context.contains("at SQL statement"));
            *source
        }
        other => panic!("expected PL/pgSQL context, got {other:?}"),
    };
    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            sqlstate: "0A000",
            ..
        } if message == "unsupported transaction command in PL/pgSQL"
    ));
}

#[test]
fn plpgsql_runtime_errors_include_statement_context() {
    let dir = temp_dir("plpgsql_error_context");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function fail_context() returns int4 language plpgsql as $$ declare z int4 := 0; begin perform 1 / z; return 0; end $$",
    )
    .unwrap();

    let err = db.execute(1, "select fail_context()").unwrap_err();
    match err {
        ExecError::WithContext { source, context } => {
            assert!(context.contains("PL/pgSQL function fail_context"));
            assert!(context.contains("at PERFORM"));
            assert!(matches!(*source, ExecError::DivisionByZero(_)));
        }
        other => panic!("expected PL/pgSQL context, got {other:?}"),
    }
}

#[test]
fn create_function_named_composite_rows_expand_from_relation_rowtype() {
    let dir = temp_dir("create_function_named_composite");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table widgets (id int4, label text)")
        .unwrap();
    db.execute(
        1,
        "create function widget_rows(n int4) returns setof widgets language plpgsql as $$ begin return query values (n, 'widget'); end $$",
    )
    .unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    let widget_type = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "widgets")
        .unwrap();
    let proc = visible
        .proc_rows_by_name("widget_rows")
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(proc.prorettype, widget_type.oid);
    assert!(proc.proretset);

    assert_eq!(
        query_rows(&db, 1, "select * from widget_rows(5)"),
        vec![vec![Value::Int32(5), Value::Text("widget".into())]]
    );
}

#[test]
fn plpgsql_declare_type_rowtype_and_labeled_record_qualification_work() {
    let dir = temp_dir("plpgsql_decl_type_rowtype_labels");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table pl_decl_items (id int4, note text)")
        .unwrap();
    db.execute(1, "insert into pl_decl_items values (1, 'alpha')")
        .unwrap();
    db.execute(
        1,
        "create function pl_decl_type_ref() returns text language plpgsql as $$
            declare
                copied pl_decl_items.note%TYPE;
            begin
                select into copied note from pl_decl_items where id = 1;
                return copied;
            end
        $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function pl_decl_rowtype_ref() returns text language plpgsql as $$
            declare
                item pl_decl_items%ROWTYPE;
            begin
                select into item * from pl_decl_items where id = 1;
                return item.note;
            end
        $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function pl_decl_labeled_ref() returns text language plpgsql as $$
            <<outer>>
            declare
                item record;
                result text;
            begin
                select into item * from pl_decl_items where id = 1;
                declare
                    inner_item record;
                begin
                    select into inner_item * from pl_decl_items where note = \"outer\".item.note;
                    result := inner_item.note;
                end;
                return result;
            end
        $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function pl_decl_empty_declare() returns bool language plpgsql as $$
            declare
            begin
                return true;
            end
        $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select pl_decl_type_ref()"),
        vec![vec![Value::Text("alpha".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select pl_decl_rowtype_ref()"),
        vec![vec![Value::Text("alpha".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select pl_decl_labeled_ref()"),
        vec![vec![Value::Text("alpha".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select pl_decl_empty_declare()"),
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn create_trigger_updates_pg_trigger_and_relhastriggers() {
    let dir = temp_dir("create_trigger_catalog_rows");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "create function items_before() returns trigger language plpgsql as $$ begin return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_before before insert on items for each row execute function items_before()",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select t.typname from pg_proc p join pg_type t on t.oid = p.prorettype where p.proname = 'items_before'",
        ),
        vec![vec![Value::Text("trigger".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select tgname from pg_trigger where tgname = 'items_before'",
        ),
        vec![vec![Value::Text("items_before".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relhastriggers from pg_class where relname = 'items'",
        ),
        vec![vec![Value::Bool(true)]]
    );

    db.execute(1, "drop trigger items_before on items").unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select tgname from pg_trigger where tgname = 'items_before'",
        ),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relhastriggers from pg_class where relname = 'items'",
        ),
        vec![vec![Value::Bool(false)]]
    );
}

#[test]
fn partitioned_table_row_triggers_clone_to_existing_new_and_attached_partitions() {
    let dir = temp_dir("partitioned_trigger_clones");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create table part_trig (a int4, b int4) partition by list (a)",
    )
    .unwrap();
    db.execute(
        1,
        "create table part_trig1 partition of part_trig for values in (1) partition by list (b)",
    )
    .unwrap();
    db.execute(
        1,
        "create table part_trig11 partition of part_trig1 for values in (1)",
    )
    .unwrap();
    db.execute(
        1,
        "create function part_trig_notice() returns trigger language plpgsql as $$
begin
  raise notice 'hit % on %', TG_NAME, TG_TABLE_NAME;
  return new;
end;
$$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger part_trig_ai after insert on part_trig for each row execute function part_trig_notice()",
    )
    .unwrap();
    db.execute(
        1,
        "create table part_trig2 partition of part_trig for values in (2)",
    )
    .unwrap();
    db.execute(1, "create table part_trig3 (a int4, b int4)")
        .unwrap();
    db.execute(
        1,
        "alter table part_trig attach partition part_trig3 for values in (3)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select c.relname, t.tgname, t.tgparentid <> 0
               from pg_trigger t join pg_class c on c.oid = t.tgrelid
              where t.tgname = 'part_trig_ai'
              order by c.relname",
        ),
        vec![
            vec![
                Value::Text("part_trig".into()),
                Value::Text("part_trig_ai".into()),
                Value::Bool(false),
            ],
            vec![
                Value::Text("part_trig1".into()),
                Value::Text("part_trig_ai".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Text("part_trig11".into()),
                Value::Text("part_trig_ai".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Text("part_trig2".into()),
                Value::Text("part_trig_ai".into()),
                Value::Bool(true),
            ],
            vec![
                Value::Text("part_trig3".into()),
                Value::Text("part_trig_ai".into()),
                Value::Bool(true),
            ],
        ]
    );

    db.execute(1, "insert into part_trig values (1, 1), (2, 2), (3, 3)")
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec![
            String::from("hit part_trig_ai on part_trig11"),
            String::from("hit part_trig_ai on part_trig2"),
            String::from("hit part_trig_ai on part_trig3"),
        ]
    );

    let err = db
        .execute(1, "drop trigger part_trig_ai on part_trig1")
        .unwrap_err();
    assert!(format!("{err:?}").contains("cannot drop trigger part_trig_ai"));

    db.execute(1, "drop trigger part_trig_ai on part_trig")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select c.relname from pg_trigger t join pg_class c on c.oid = t.tgrelid
              where t.tgname = 'part_trig_ai'
              order by c.relname",
        ),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn information_schema_triggers_exposes_trigger_metadata() {
    let dir = temp_dir("info_schema_triggers");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table main_table (a int4, b int4)")
        .unwrap();
    db.execute(
        1,
        "create function trigger_func() returns trigger language plpgsql as $$ begin return null; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger after_ins_stmt_trig after insert on main_table for each statement execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger after_upd_row_trig after update on main_table for each row execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger after_upd_stmt_trig after update on main_table for each statement execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger before_ins_stmt_trig before insert on main_table for each statement execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger delete_a after delete on main_table for each row when (old.a = 123) execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger delete_when after delete on main_table for each statement when (true) execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger insert_a after insert on main_table for each row when (new.a = 123) execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger insert_when before insert on main_table for each statement when (true) execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger modified_a before update of a on main_table for each row when (old.a <> new.a) execute function trigger_func()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger modified_any before update of a on main_table for each row when (old.* is distinct from new.*) execute function trigger_func()",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select trigger_name, event_manipulation, event_object_schema, event_object_table,
                    action_order, action_condition, action_orientation, action_timing,
                    action_reference_old_table, action_reference_new_table
             from information_schema.triggers
             where event_object_table = 'main_table'
             order by trigger_name, event_manipulation",
        ),
        vec![
            vec![
                Value::Text("after_ins_stmt_trig".into()),
                Value::Text("INSERT".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Null,
                Value::Text("STATEMENT".into()),
                Value::Text("AFTER".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("after_upd_row_trig".into()),
                Value::Text("UPDATE".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Null,
                Value::Text("ROW".into()),
                Value::Text("AFTER".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("after_upd_stmt_trig".into()),
                Value::Text("UPDATE".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Null,
                Value::Text("STATEMENT".into()),
                Value::Text("AFTER".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("before_ins_stmt_trig".into()),
                Value::Text("INSERT".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Null,
                Value::Text("STATEMENT".into()),
                Value::Text("BEFORE".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("delete_a".into()),
                Value::Text("DELETE".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Text("(old.a = 123)".into()),
                Value::Text("ROW".into()),
                Value::Text("AFTER".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("delete_when".into()),
                Value::Text("DELETE".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Text("true".into()),
                Value::Text("STATEMENT".into()),
                Value::Text("AFTER".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("insert_a".into()),
                Value::Text("INSERT".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Text("(new.a = 123)".into()),
                Value::Text("ROW".into()),
                Value::Text("AFTER".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("insert_when".into()),
                Value::Text("INSERT".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(2),
                Value::Text("true".into()),
                Value::Text("STATEMENT".into()),
                Value::Text("BEFORE".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("modified_a".into()),
                Value::Text("UPDATE".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(1),
                Value::Text("(old.a <> new.a)".into()),
                Value::Text("ROW".into()),
                Value::Text("BEFORE".into()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Text("modified_any".into()),
                Value::Text("UPDATE".into()),
                Value::Text("public".into()),
                Value::Text("main_table".into()),
                Value::Int32(2),
                Value::Text("(old.* is distinct from new.*)".into()),
                Value::Text("ROW".into()),
                Value::Text("BEFORE".into()),
                Value::Null,
                Value::Null,
            ],
        ]
    );
}

#[test]
fn alter_table_row_security_flags_update_pg_class() {
    let dir = temp_dir("alter_table_row_security_flags");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relrowsecurity, relforcerowsecurity from pg_class where relname = 'items'",
        ),
        vec![vec![Value::Bool(false), Value::Bool(false)]]
    );

    db.execute(1, "alter table items enable row level security")
        .unwrap();
    db.execute(1, "alter table items force row level security")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relrowsecurity, relforcerowsecurity from pg_class where relname = 'items'",
        ),
        vec![vec![Value::Bool(true), Value::Bool(true)]]
    );

    db.execute(1, "alter table items no force row level security")
        .unwrap();
    db.execute(1, "alter table items disable row level security")
        .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select relrowsecurity, relforcerowsecurity from pg_class where relname = 'items'",
        ),
        vec![vec![Value::Bool(false), Value::Bool(false)]]
    );
}

#[test]
fn alter_table_row_security_if_exists_ignores_missing_table() {
    let dir = temp_dir("alter_table_row_security_if_exists");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "alter table if exists missing enable row level security")
        .unwrap();
    db.execute(
        1,
        "alter table if exists missing no force row level security",
    )
    .unwrap();
}

#[test]
fn before_insert_trigger_can_mutate_new_and_skip_rows() {
    let dir = temp_dir("before_insert_trigger_mutate_skip");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "create function items_before_insert() returns trigger language plpgsql as $$ begin if NEW.id < 0 then return null; end if; NEW.note := NEW.note || '-mutated'; return NEW; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_before_insert before insert on items for each row execute function items_before_insert()",
    )
    .unwrap();

    db.execute(1, "insert into items values (1, 'a'), (-1, 'skip')")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![vec![Value::Int32(1), Value::Text("a-mutated".into()),]]
    );
}

#[test]
fn plpgsql_alias_record_select_into_and_update_work() {
    let dir = temp_dir("plpgsql_alias_record_select_into_update");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table slots (slotname text, backlink text)")
        .unwrap();
    db.execute(1, "insert into slots values ('PS.base.a1', '')")
        .unwrap();
    db.execute(
        1,
        "create function tg_backlink_set(text, text) returns int4 language plpgsql as $$ declare myname alias for $1; blname alias for $2; rec record; begin select into rec * from slots where slotname = myname; if not found then raise exception '% missing', myname; end if; if rec.backlink != blname then update slots set backlink = blname where slotname = myname; end if; return 0; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select tg_backlink_set('PS.base.a1', 'WS.001.1a')"),
        vec![vec![Value::Int32(0)]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select backlink from slots where slotname = 'PS.base.a1'"
        ),
        vec![vec![Value::Text("WS.001.1a".into())]]
    );
}

#[test]
fn after_insert_trigger_nested_sql_sees_inserted_row() {
    let dir = temp_dir("after_insert_trigger_nested_sql_sees_row");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table slots (slotname text, backlink text)")
        .unwrap();
    db.execute(
        1,
        "create function require_slot(text) returns int4 language plpgsql as $$ declare rec record; begin select into rec * from slots where slotname = $1; if not found then raise exception '% missing', $1; end if; return 0; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function slots_after_insert() returns trigger language plpgsql as $$ declare dummy int4; begin dummy := require_slot(new.slotname); return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger slots_after_insert after insert on slots for each row execute function slots_after_insert()",
    )
    .unwrap();

    db.execute(1, "insert into slots values ('PS.base.b1', '')")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select slotname from slots"),
        vec![vec![Value::Text("PS.base.b1".into())]]
    );
}

#[test]
fn plpgsql_static_query_for_loop_record_target_supports_field_access() {
    let dir = temp_dir("plpgsql_query_loop_record_fields");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function last_pair() returns text language plpgsql as $$ declare rec record; begin for rec in values (1, 'a'), (2, 'b') loop null; end loop; return rec.column1::text || rec.column2; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select last_pair()"),
        vec![vec![Value::Text("2b".into())]]
    );
}

#[test]
fn plpgsql_nested_static_query_for_loops_over_scalar_targets_work() {
    let dir = temp_dir("plpgsql_nested_query_loops");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function nested_query_loops() returns int4 language plpgsql as $$ declare a int4; b int4; inner_v int4; total int4 := 0; begin for a, b in values (1, 10), (2, 20) loop for inner_v in values (100), (200) loop total := total + a + b + inner_v; end loop; end loop; return total; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select nested_query_loops()"),
        vec![vec![Value::Int32(666)]]
    );
}

#[test]
fn plpgsql_dynamic_execute_query_for_loop_supports_explain_lines() {
    let dir = temp_dir("plpgsql_dynamic_query_loop_explain");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function explain_line_count(text) returns int4 language plpgsql as $$ declare ln text; total int4 := 0; begin for ln in execute format('explain analyze %s', $1) loop total := total + 1; end loop; return total; end $$",
    )
    .unwrap();

    let rows = query_rows(&db, 1, "select explain_line_count('select 1')");
    match &rows[..] {
        [row] => match &row[..] {
            [Value::Int32(count)] => assert!(*count > 0),
            other => panic!("expected single int4 result, got {other:?}"),
        },
        other => panic!("expected single row result, got {other:?}"),
    }
}

#[test]
fn plpgsql_dynamic_execute_query_for_loop_supports_using() {
    let dir = temp_dir("plpgsql_dynamic_query_loop_using");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function dynamic_using_sum(text) returns int4 language plpgsql as $$ declare v int4; total int4 := 0; begin for v in execute $1 using 3, 4 loop total := total + v; end loop; return total; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select dynamic_using_sum('values ($1), ($2)')"),
        vec![vec![Value::Int32(7)]]
    );
}

#[test]
fn plpgsql_dynamic_execute_statement_supports_into_and_using() {
    let dir = temp_dir("plpgsql_dynamic_execute_into_using");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function dynamic_execute_value(x int4) returns int4 language plpgsql as $$ declare v int4; begin execute 'select $1 + 2' into v using x; return v; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select dynamic_execute_value(5)"),
        vec![vec![Value::Int32(7)]]
    );
}

#[test]
fn plpgsql_exception_block_handles_named_condition() {
    let dir = temp_dir("plpgsql_exception_named_condition");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function catch_assert() returns text language plpgsql as $$ begin assert false, 'bad'; return 'missed'; exception when assert_failure then return 'handled'; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select catch_assert()"),
        vec![vec![Value::Text("handled".into())]]
    );
}

#[test]
fn plpgsql_query_for_loop_sets_found_false_when_empty() {
    let dir = temp_dir("plpgsql_query_loop_found_false");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function query_loop_found_false() returns bool language plpgsql as $$ declare v int4; begin found := true; for v in select 1 where false loop null; end loop; return found; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select query_loop_found_false()"),
        vec![vec![Value::Bool(false)]]
    );
}

#[test]
fn plpgsql_query_for_loop_sets_found_true_after_nonempty_loop() {
    let dir = temp_dir("plpgsql_query_loop_found_true");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function query_loop_found_true() returns bool language plpgsql as $$ declare v int4; begin found := false; for v in values (1) loop found := false; end loop; return found; end $$",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select query_loop_found_true()"),
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn plpgsql_query_for_loop_reports_row_shape_mismatch() {
    let dir = temp_dir("plpgsql_query_loop_shape_mismatch");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function bad_query_loop_shape() returns int4 language plpgsql as $$ declare v int4; begin for v in values (1, 2) loop null; end loop; return 0; end $$",
    )
    .unwrap();

    let err = db.execute(1, "select bad_query_loop_shape()").unwrap_err();
    let err = match err {
        ExecError::WithContext { source, context } => {
            assert!(context.contains("PL/pgSQL function bad_query_loop_shape"));
            *source
        }
        err => err,
    };
    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            detail: Some(detail),
            sqlstate,
            ..
        } if message == "query returned an unexpected row shape"
            && detail == "expected 1 column, got 2"
            && sqlstate == "42804"
    ));
}

#[test]
fn plpgsql_dynamic_execute_query_for_loop_rejects_null_query_string() {
    let dir = temp_dir("plpgsql_query_loop_null_execute");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function null_dynamic_loop() returns int4 language plpgsql as $$ declare v int4; q text := null; begin for v in execute q loop null; end loop; return 0; end $$",
    )
    .unwrap();

    let err = db.execute(1, "select null_dynamic_loop()").unwrap_err();
    let err = match err {
        ExecError::WithContext { source, context } => {
            assert!(context.contains("PL/pgSQL function null_dynamic_loop"));
            *source
        }
        err => err,
    };
    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            detail: None,
            sqlstate,
            ..
        } if message == "query string argument of EXECUTE is null"
            && sqlstate == "22004"
    ));
}

#[test]
fn after_insert_triggers_fire_per_row_in_alphabetical_order() {
    let dir = temp_dir("after_insert_trigger_notices");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "create function trig_a() returns trigger language plpgsql as $$ begin raise notice 'a:%', NEW.id; return NEW; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function trig_b() returns trigger language plpgsql as $$ begin raise notice 'b:%', NEW.id; return NEW; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger b_after after insert on items for each row execute function trig_b()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger a_after after insert on items for each row execute function trig_a()",
    )
    .unwrap();

    clear_notices();
    db.execute(1, "insert into items values (1, 'a'), (2, 'b')")
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec![
            "a:1".to_string(),
            "b:1".to_string(),
            "a:2".to_string(),
            "b:2".to_string(),
        ]
    );
}

#[test]
fn update_triggers_honor_update_of_when_and_statement_firing() {
    let dir = temp_dir("update_trigger_update_of_when");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, name text)")
        .unwrap();
    db.execute(1, "insert into items values (1, 'alpha')")
        .unwrap();
    db.execute(
        1,
        "create function row_update_notice() returns trigger language plpgsql as $$ begin raise notice 'row:%', NEW.name; return NEW; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function stmt_update_notice() returns trigger language plpgsql as $$ begin raise notice 'stmt:%:%', TG_WHEN, TG_LEVEL; return; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_stmt_before before update on items for each statement execute function stmt_update_notice()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_stmt_after after update on items for each statement execute function stmt_update_notice()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_name_update before update of name on items for each row when (NEW.name <> OLD.name) execute function row_update_notice()",
    )
    .unwrap();

    clear_notices();
    db.execute(1, "update items set id = id where false")
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec![
            "stmt:BEFORE:STATEMENT".to_string(),
            "stmt:AFTER:STATEMENT".to_string(),
        ]
    );

    clear_notices();
    db.execute(1, "update items set id = 7 where id = 1")
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec![
            "stmt:BEFORE:STATEMENT".to_string(),
            "stmt:AFTER:STATEMENT".to_string(),
        ]
    );

    clear_notices();
    db.execute(1, "update items set name = 'beta' where id = 7")
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec![
            "stmt:BEFORE:STATEMENT".to_string(),
            "row:beta".to_string(),
            "stmt:AFTER:STATEMENT".to_string(),
        ]
    );

    clear_notices();
    db.execute(1, "update items set name = 'beta' where id = 7")
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec![
            "stmt:BEFORE:STATEMENT".to_string(),
            "stmt:AFTER:STATEMENT".to_string(),
        ]
    );
}

#[test]
fn session_replication_role_replica_skips_origin_statement_triggers() {
    let dir = temp_dir("session_replication_role_replica_triggers");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table trigtest (i int4)")
        .unwrap();
    session
        .execute(
            &db,
            "create function trigtest_notice() returns trigger language plpgsql as $$ begin raise notice 'stmt'; return null; end $$",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create trigger trigtest_stmt after insert on trigtest for each statement execute function trigtest_notice()",
        )
        .unwrap();

    session
        .execute(&db, "set session_replication_role = replica")
        .unwrap();
    clear_notices();
    session
        .execute(&db, "insert into trigtest default values")
        .unwrap();

    assert!(take_notice_messages().is_empty());
    assert_eq!(
        session_query_rows(&mut session, &db, "select count(*) from trigtest"),
        vec![vec![Value::Int64(1)]]
    );
}

#[test]
fn pg_get_triggerdef_defaults_to_unpretty_and_lowercase_old_new() {
    let dir = temp_dir("pg_get_triggerdef_trigger_old_new");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "create function items_before() returns trigger language plpgsql as $$ begin return new; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_before before update of note on items for each row when (OLD.note IS DISTINCT FROM NEW.note) execute function items_before()",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select pg_get_triggerdef(oid), pg_get_triggerdef(oid, true) from pg_trigger where tgname = 'items_before'",
        ),
        vec![vec![
            Value::Text(
                "CREATE TRIGGER items_before BEFORE UPDATE OF note ON public.items FOR EACH ROW WHEN ((old.note IS DISTINCT FROM new.note)) EXECUTE FUNCTION items_before()".into(),
            ),
            Value::Text(
                "CREATE TRIGGER items_before BEFORE UPDATE OF note ON items FOR EACH ROW WHEN (old.note IS DISTINCT FROM new.note) EXECUTE FUNCTION items_before()".into(),
            ),
        ]]
    );
}

#[test]
fn trigger_transition_table_names_are_visible_in_catalog_helpers() {
    let dir = temp_dir("trigger_transition_catalog_helpers");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "create function items_after() returns trigger language plpgsql as $$ begin return null; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_after after update on items referencing old table as old_rows new table as new_rows for each statement execute function items_after()",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select tgoldtable, tgnewtable from pg_trigger where tgname = 'items_after'",
        ),
        vec![vec![
            Value::Text("old_rows".into()),
            Value::Text("new_rows".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select action_reference_old_table, action_reference_new_table
             from information_schema.triggers
             where trigger_name = 'items_after'",
        ),
        vec![vec![
            Value::Text("old_rows".into()),
            Value::Text("new_rows".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select pg_get_triggerdef(oid), pg_get_triggerdef(oid, true)
             from pg_trigger
             where tgname = 'items_after'",
        ),
        vec![vec![
            Value::Text(
                "CREATE TRIGGER items_after AFTER UPDATE ON public.items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION items_after()".into(),
            ),
            Value::Text(
                "CREATE TRIGGER items_after AFTER UPDATE ON items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION items_after()".into(),
            ),
        ]]
    );
}

#[test]
fn trigger_functions_can_use_new_old_and_tg_argv_inside_sql() {
    let dir = temp_dir("trigger_function_new_old_tg_argv_sql");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(1, "create table audit (id int4, payload text)")
        .unwrap();
    db.execute(
        1,
        "create function capture_trigger_state() returns trigger language plpgsql as $$
declare
    argstr text;
begin
    argstr := '[';
    for i in 0 .. TG_nargs - 1 loop
        if i > 0 then
            argstr := argstr || ', ';
        end if;
        argstr := argstr || TG_argv[i];
    end loop;
    argstr := argstr || ']';

    if TG_OP = 'INSERT' then
        insert into audit values (NEW.id, argstr);
    else
        insert into audit values (NEW.id, OLD.note || '->' || NEW.note);
    end if;

    return NEW;
end;
$$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger capture_trigger_state before insert or update on items for each row execute function capture_trigger_state('left', 'right')",
    )
    .unwrap();

    db.execute(1, "insert into items values (1, 'a')").unwrap();
    db.execute(1, "update items set note = 'b' where id = 1")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select id, payload from audit order by payload"),
        vec![
            vec![Value::Int32(1), Value::Text("[left, right]".into())],
            vec![Value::Int32(1), Value::Text("a->b".into())],
        ]
    );
}

#[test]
fn view_instead_of_triggers_fire_statement_triggers_and_return_rows() {
    let dir = temp_dir("view_instead_of_triggers");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table main_table (a int4, b int4)")
        .unwrap();
    db.execute(1, "create view main_view as select a, b from main_table")
        .unwrap();
    db.execute(
        1,
        "create function view_trigger() returns trigger language plpgsql as $$
begin
    raise notice '% % % % (%)', TG_TABLE_NAME, TG_WHEN, TG_OP, TG_LEVEL, TG_ARGV[0];
    if TG_LEVEL = 'ROW' then
        if TG_OP = 'INSERT' then
            insert into main_table values (NEW.a, NEW.b);
            return NEW;
        end if;
        if TG_OP = 'UPDATE' then
            update main_table set a = NEW.a, b = NEW.b where a = OLD.a and b = OLD.b;
            if not found then
                return null;
            end if;
            return NEW;
        end if;
        if TG_OP = 'DELETE' then
            delete from main_table where a = OLD.a and b = OLD.b;
            if not found then
                return null;
            end if;
            return OLD;
        end if;
    end if;
    return null;
end;
$$",
    )
    .unwrap();
    for sql in [
        "create trigger instead_of_insert_trig instead of insert on main_view for each row execute function view_trigger('instead_of_ins')",
        "create trigger instead_of_update_trig instead of update on main_view for each row execute function view_trigger('instead_of_upd')",
        "create trigger instead_of_delete_trig instead of delete on main_view for each row execute function view_trigger('instead_of_del')",
        "create trigger before_ins_stmt_trig before insert on main_view for each statement execute function view_trigger('before_view_ins_stmt')",
        "create trigger before_upd_stmt_trig before update on main_view for each statement execute function view_trigger('before_view_upd_stmt')",
        "create trigger before_del_stmt_trig before delete on main_view for each statement execute function view_trigger('before_view_del_stmt')",
        "create trigger after_ins_stmt_trig after insert on main_view for each statement execute function view_trigger('after_view_ins_stmt')",
        "create trigger after_upd_stmt_trig after update on main_view for each statement execute function view_trigger('after_view_upd_stmt')",
        "create trigger after_del_stmt_trig after delete on main_view for each statement execute function view_trigger('after_view_del_stmt')",
    ] {
        db.execute(1, sql).unwrap();
    }

    clear_notices();
    assert_eq!(
        query_rows(&db, 1, "insert into main_view values (20, 30) returning *"),
        vec![vec![Value::Int32(20), Value::Int32(30)]]
    );
    assert_eq!(
        take_notice_messages(),
        vec![
            "main_view BEFORE INSERT STATEMENT (before_view_ins_stmt)".to_string(),
            "main_view INSTEAD OF INSERT ROW (instead_of_ins)".to_string(),
            "main_view AFTER INSERT STATEMENT (after_view_ins_stmt)".to_string(),
        ]
    );

    clear_notices();
    assert_eq!(
        query_rows(
            &db,
            1,
            "update main_view set b = 31 where a = 20 returning *"
        ),
        vec![vec![Value::Int32(20), Value::Int32(31)]]
    );
    assert_eq!(
        take_notice_messages(),
        vec![
            "main_view BEFORE UPDATE STATEMENT (before_view_upd_stmt)".to_string(),
            "main_view INSTEAD OF UPDATE ROW (instead_of_upd)".to_string(),
            "main_view AFTER UPDATE STATEMENT (after_view_upd_stmt)".to_string(),
        ]
    );

    clear_notices();
    assert_eq!(
        query_rows(&db, 1, "delete from main_view where a = 20 returning *"),
        vec![vec![Value::Int32(20), Value::Int32(31)]]
    );
    assert_eq!(
        take_notice_messages(),
        vec![
            "main_view BEFORE DELETE STATEMENT (before_view_del_stmt)".to_string(),
            "main_view INSTEAD OF DELETE ROW (instead_of_del)".to_string(),
            "main_view AFTER DELETE STATEMENT (after_view_del_stmt)".to_string(),
        ]
    );
    assert!(query_rows(&db, 1, "select * from main_table").is_empty());
}

#[test]
fn rules_can_route_into_trigger_backed_views() {
    let dir = temp_dir("rule_to_trigger_backed_view");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table city_table (city_id int4, city_name text)")
        .unwrap();
    db.execute(
        1,
        "create view city_view as select city_id, city_name from city_table",
    )
    .unwrap();
    db.execute(
        1,
        "create function city_insert() returns trigger language plpgsql as $$
begin
    insert into city_table values (NEW.city_id, NEW.city_name);
    return NEW;
end;
$$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger city_insert_trig instead of insert on city_view for each row execute function city_insert()",
    )
    .unwrap();
    db.execute(
        1,
        "create view european_city_view as select * from city_view where city_id > 0",
    )
    .unwrap();
    db.execute(
        1,
        "create function no_op_trig_fn() returns trigger language plpgsql as $$ begin return null; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger no_op_trig instead of insert on european_city_view for each row execute function no_op_trig_fn()",
    )
    .unwrap();

    assert!(matches!(
        db.execute(1, "insert into european_city_view values (1, 'noop')"),
        Ok(StatementResult::AffectedRows(0))
    ));
    assert!(query_rows(&db, 1, "select * from city_table").is_empty());

    db.execute(
        1,
        "create rule european_city_insert_rule as on insert to european_city_view do instead insert into city_view values (NEW.city_id, NEW.city_name) returning *",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "insert into european_city_view values (2, 'Cambridge') returning *",
        ),
        vec![vec![Value::Int32(2), Value::Text("Cambridge".into())]]
    );
    assert_eq!(
        query_rows(&db, 1, "select city_id, city_name from city_table"),
        vec![vec![Value::Int32(2), Value::Text("Cambridge".into())]]
    );
}

#[test]
fn trigger_select_into_can_assign_new_record_fields() {
    let dir = temp_dir("trigger_select_into_new_field");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create table country_table (country_name text, continent text)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into country_table values ('USA', 'North America')",
    )
    .unwrap();
    db.execute(
        1,
        "create table city_table (city_name text, country_name text, continent text)",
    )
    .unwrap();
    db.execute(
        1,
        "create view city_view as select city_name, country_name, continent from city_table",
    )
    .unwrap();
    db.execute(
        1,
        "create function city_insert() returns trigger language plpgsql as $$
begin
    select continent into NEW.continent
        from country_table
        where country_name = NEW.country_name;
    insert into city_table values (NEW.city_name, NEW.country_name, NEW.continent);
    return NEW;
end;
$$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger city_insert_trig instead of insert on city_view for each row execute function city_insert()",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "insert into city_view values ('Washington DC', 'USA', null) returning *",
        ),
        vec![vec![
            Value::Text("Washington DC".into()),
            Value::Text("USA".into()),
            Value::Text("North America".into()),
        ]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select city_name, country_name, continent from city_table"
        ),
        vec![vec![
            Value::Text("Washington DC".into()),
            Value::Text("USA".into()),
            Value::Text("North America".into()),
        ]]
    );
}

#[test]
fn rule_actions_can_return_new_star_from_outer_scope() {
    let dir = temp_dir("rule_action_returning_new_star");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table city_table (city_id int4, city_name text)")
        .unwrap();
    db.execute(1, "insert into city_table values (1, 'Old Town')")
        .unwrap();
    db.execute(
        1,
        "create view city_view as select city_id, city_name from city_table",
    )
    .unwrap();
    db.execute(
        1,
        "create function city_update() returns trigger language plpgsql as $$
begin
    update city_table set city_name = NEW.city_name where city_id = OLD.city_id;
    if not found then
        return null;
    end if;
    return NEW;
end;
$$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger city_update_trig instead of update on city_view for each row execute function city_update()",
    )
    .unwrap();
    db.execute(
        1,
        "create view european_city_view as select * from city_view",
    )
    .unwrap();
    db.execute(
        1,
        "create rule european_city_update_rule as on update to european_city_view do instead update city_view set city_name = NEW.city_name where city_id = OLD.city_id returning NEW.*",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "update european_city_view set city_name = 'New Town' where city_id = 1 returning *",
        ),
        vec![vec![Value::Int32(1), Value::Text("New Town".into()),]]
    );
    assert_eq!(
        query_rows(&db, 1, "select city_id, city_name from city_table"),
        vec![vec![Value::Int32(1), Value::Text("New Town".into()),]]
    );
}

#[test]
fn trigger_insert_returning_into_can_assign_new_record_fields() {
    let dir = temp_dir("trigger_insert_returning_into_new_field");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table city_table (city_id int4, city_name text)")
        .unwrap();
    db.execute(
        1,
        "create view city_view as select city_id, city_name from city_table",
    )
    .unwrap();
    db.execute(
        1,
        "create function city_insert() returns trigger language plpgsql as $$
begin
    insert into city_table values (7, NEW.city_name)
        returning city_id into NEW.city_id;
    return NEW;
end;
$$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger city_insert_trig instead of insert on city_view for each row execute function city_insert()",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "insert into city_view values (null, 'Tokyo') returning *",
        ),
        vec![vec![Value::Int32(7), Value::Text("Tokyo".into()),]]
    );
    assert_eq!(
        query_rows(&db, 1, "select city_id, city_name from city_table"),
        vec![vec![Value::Int32(7), Value::Text("Tokyo".into()),]]
    );
}

#[test]
fn temp_trigger_function_is_resolved_from_temp_schema() {
    let dir = temp_dir("temp_trigger_function_search_path");
    let db = Database::open(&dir, 16).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create temp table items (id int4, note text)")
        .unwrap();
    session
        .execute(
            &db,
            "create function temp_stmt_notice() returns trigger language plpgsql as $$ begin raise notice 'temp-stmt'; return null; end $$",
        )
        .unwrap();
    session
        .execute(
            &db,
            "create trigger items_stmt_before before update on items for each statement execute function temp_stmt_notice()",
        )
        .unwrap();

    clear_backend_notices();
    clear_notices();
    session
        .execute(&db, "update items set note = note where false")
        .unwrap();

    assert_eq!(take_notice_messages(), vec![String::from("temp-stmt")]);
}

#[test]
fn delete_prepared_insert_and_copy_from_fire_triggers() {
    let dir = temp_dir("trigger_delete_prepared_copy");
    let db = Database::open(&dir, 64).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4, note text)")
        .unwrap();
    db.execute(
        1,
        "create function before_insert_notice() returns trigger language plpgsql as $$ begin raise notice 'insert:%', NEW.id; NEW.note := NEW.note || '-ok'; return NEW; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create function before_delete_notice() returns trigger language plpgsql as $$ begin if OLD.id = 2 then return null; end if; raise notice 'delete:%', OLD.id; return OLD; end $$",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_before_insert before insert on items for each row execute function before_insert_notice()",
    )
    .unwrap();
    db.execute(
        1,
        "create trigger items_before_delete before delete on items for each row execute function before_delete_notice()",
    )
    .unwrap();

    let prepared = session.prepare_insert(&db, "items", None, 2).unwrap();
    session.execute(&db, "begin").unwrap();
    clear_notices();
    session
        .execute_prepared_insert(
            &db,
            &prepared,
            &[Value::Int32(1), Value::Text("prepared".into())],
        )
        .unwrap();
    session.execute(&db, "commit").unwrap();
    assert_eq!(take_notice_messages(), vec!["insert:1".to_string()]);

    clear_notices();
    session
        .copy_from_rows(
            &db,
            "items",
            &[
                vec!["2".into(), "copied".into()],
                vec!["3".into(), "copied".into()],
            ],
        )
        .unwrap();
    assert_eq!(
        take_notice_messages(),
        vec!["insert:2".to_string(), "insert:3".to_string()]
    );
    assert_eq!(
        query_rows(&db, 1, "select id, note from items order by id"),
        vec![
            vec![Value::Int32(1), Value::Text("prepared-ok".into())],
            vec![Value::Int32(2), Value::Text("copied-ok".into())],
            vec![Value::Int32(3), Value::Text("copied-ok".into())],
        ]
    );

    clear_notices();
    db.execute(1, "delete from items where id in (1, 2)")
        .unwrap();
    assert_eq!(take_notice_messages(), vec!["delete:1".to_string()]);
    assert_eq!(
        query_rows(&db, 1, "select id from items order by id"),
        vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]
    );
}

#[test]
fn create_type_exposes_catalog_rows_and_function_row_expansion() {
    let dir = temp_dir("create_type_catalog_rows");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create type widget as (id int4, label text)")
        .unwrap();
    db.execute(
        1,
        "create function widget_rows(n int4) returns setof widget language plpgsql as $$ begin return query values (n, 'widget'); end $$",
    )
    .unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    let widget_relation = visible.lookup_any_relation("widget").unwrap();
    let widget_type = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "widget")
        .unwrap();
    let widget_proc = visible
        .proc_rows_by_name("widget_rows")
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(widget_relation.relkind, 'c');
    assert_eq!(widget_type.typrelid, widget_relation.relation_oid);
    assert_eq!(widget_proc.prorettype, widget_type.oid);
    assert!(widget_proc.proretset);
    assert_eq!(relfilenode_for(&db, 1, "widget"), 0);
    assert_eq!(
        query_rows(
            &db,
            1,
            &format!(
                "select attname from pg_attribute where attrelid = {} and attnum > 0 order by attnum",
                widget_relation.relation_oid
            ),
        ),
        vec![
            vec![Value::Text("id".into())],
            vec![Value::Text("label".into())],
        ]
    );
    assert!(
        db.backend_catcache(1, None)
            .unwrap()
            .depend_rows()
            .iter()
            .any(|row| {
                row.classid == PG_PROC_RELATION_OID
                    && row.objid == widget_proc.oid
                    && row.refclassid == PG_TYPE_RELATION_OID
                    && row.refobjid == widget_type.oid
            })
    );
    assert_eq!(
        query_rows(&db, 1, "select * from widget_rows(5)"),
        vec![vec![Value::Int32(5), Value::Text("widget".into())]]
    );
}

#[test]
fn create_enum_type_exposes_catalog_rows_and_can_back_table_columns() {
    let dir = temp_dir("create_enum_type_catalog_rows");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create type mood as enum ('sad', 'ok')")
        .unwrap();
    db.execute(1, "create table feelings(current_mood mood)")
        .unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    let mood_type = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "mood")
        .unwrap();
    let mood_array_type = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "_mood")
        .unwrap();

    assert_eq!(mood_type.typelem, 0);
    assert_eq!(mood_type.typarray, mood_array_type.oid);
    assert_eq!(mood_array_type.typelem, mood_type.oid);
    assert_eq!(mood_type.sql_type.kind, SqlTypeKind::Text);
}

#[test]
fn create_range_type_exposes_catalog_rows_and_can_back_table_columns() {
    let dir = temp_dir("create_range_type_catalog_rows");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create type float8range as range (subtype = float8, subtype_diff = float8mi)",
    )
    .unwrap();
    db.execute(1, "create table measurements(span float8range)")
        .unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    let range_type = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "float8range")
        .unwrap();
    let range_array_type = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "_float8range")
        .unwrap();

    assert_eq!(range_type.typelem, 0);
    assert_eq!(range_type.typarray, range_array_type.oid);
    assert_eq!(range_array_type.typelem, range_type.oid);
    assert_eq!(range_type.sql_type.kind, SqlTypeKind::Range);
    assert_eq!(range_type.sql_type.range_subtype_oid, FLOAT8_TYPE_OID);
}

#[test]
fn create_range_type_exposes_pg_range_metadata() {
    let dir = temp_dir("create_range_type_pg_range_rows");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create type float8range as range (subtype = float8, subtype_diff = float8mi)",
    )
    .unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    let type_row = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "float8range")
        .unwrap();
    let range_row = visible
        .range_rows()
        .into_iter()
        .find(|row| row.rngtypid == type_row.oid)
        .unwrap();

    assert_eq!(range_row.rngsubtype, FLOAT8_TYPE_OID);
    assert_eq!(range_row.rngsubdiff.as_deref(), Some("float8mi"));
}

#[test]
fn user_defined_ranges_resolve_constructor_and_accessor_calls() {
    let dir = temp_dir("user_defined_range_function_resolution");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create type float8range as range (subtype = float8, subtype_diff = float8mi)",
    )
    .unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
                float8range(1.5, 3.5)::text, \
                lower(float8range(1.5, 3.5))::text, \
                upper(float8range(1.5, 3.5))::text",
        ),
        vec![vec![
            Value::Text("[1.5,3.5)".into()),
            Value::Text("1.5".into()),
            Value::Text("3.5".into()),
        ]],
    );
}

#[test]
fn user_defined_ranges_support_default_and_manual_multirange_names() {
    let dir = temp_dir("user_defined_range_multirange_names");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create type intr as range(subtype=int)")
        .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select intr_multirange(intr(1,10))::text"),
        vec![vec![Value::Text("{[1,10)}".into())]]
    );

    match db.execute(
        1,
        "create type textrange1 as range(subtype=text, multirange_type_name=int, collation=\"C\")",
    ) {
        Err(ExecError::DetailedError { message, .. })
            if message == "type \"int4\" already exists" => {}
        other => panic!("expected builtin alias name conflict, got {other:?}"),
    }

    db.execute(
        1,
        "create type textrange1 as range(subtype=text, multirange_type_name=multirange_of_text, collation=\"C\")",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select multirange_of_text(textrange1('a','b'), textrange1('d','e'))::text",
        ),
        vec![vec![Value::Text("{[a,b),[d,e)}".into())]]
    );

    db.execute(
        1,
        "create temp table temp_multitext(f1 multirange_of_text[])",
    )
    .unwrap();
    db.execute(1, "drop table temp_multitext").unwrap();
}

#[test]
fn explicit_multirange_name_renames_existing_range_array_type() {
    let dir = temp_dir("explicit_multirange_name_renames_range_array");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create type textrange1 as range(subtype=text, multirange_type_name=multirange_of_text, collation=\"C\")",
    )
    .unwrap();
    db.execute(
        1,
        "create type textrange2 as range(subtype=text, multirange_type_name=_textrange1, collation=\"C\")",
    )
    .unwrap();

    let type_rows = db.lazy_catalog_lookup(1, None, None).type_rows();
    let textrange1 = type_rows
        .iter()
        .find(|row| row.typname == "textrange1")
        .unwrap();
    let renamed_array = type_rows
        .iter()
        .find(|row| row.typname == "__textrange1" && row.typelem == textrange1.oid)
        .unwrap();
    let multirange = type_rows
        .iter()
        .find(|row| row.typname == "_textrange1" && row.typelem == 0)
        .unwrap();

    assert_eq!(textrange1.typarray, renamed_array.oid);
    assert_eq!(multirange.sql_type.kind, SqlTypeKind::Multirange);
    assert_eq!(
        query_rows(
            &db,
            1,
            "select _textrange1(textrange2('a','z')) @> 'b'::text",
        ),
        vec![vec![Value::Bool(true)]]
    );
}

#[test]
fn domain_over_multirange_check_is_enforced_on_cast() {
    let dir = temp_dir("domain_over_multirange_check");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create domain restrictedmultirange as int4multirange check (upper(value) < 10)",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select '{[4,5)}'::restrictedmultirange @> 7",),
        vec![vec![Value::Bool(false)]]
    );

    match db.execute(1, "select '{[4,50)}'::restrictedmultirange @> 7") {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(sqlstate, "23514");
            assert_eq!(
                message,
                "value for domain restrictedmultirange violates check constraint \"restrictedmultirange_check\""
            );
        }
        other => panic!("expected domain check violation, got {other:?}"),
    }
}

#[test]
fn range_owner_and_usage_privileges_apply_to_multirange_columns() {
    let dir = temp_dir("range_owner_usage_privileges");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create role regress_multirange_owner login")
        .unwrap();
    db.execute(
        1,
        "create type textrange1 as range(subtype=text, multirange_type_name=multitextrange1, collation=\"C\")",
    )
    .unwrap();

    match db.execute(
        1,
        "alter type multitextrange1 owner to regress_multirange_owner",
    ) {
        Err(ExecError::DetailedError {
            message,
            hint,
            sqlstate,
            ..
        }) => {
            assert_eq!(sqlstate, "42809");
            assert_eq!(message, "cannot alter multirange type multitextrange1");
            assert_eq!(
                hint.as_deref(),
                Some(
                    "You can alter type textrange1, which will alter the multirange type as well."
                )
            );
        }
        other => panic!("expected multirange alter type error, got {other:?}"),
    }

    db.execute(1, "alter type textrange1 owner to regress_multirange_owner")
        .unwrap();
    let owner_oid = role_oid(&db, "regress_multirange_owner");
    let type_rows = db.lazy_catalog_lookup(1, None, None).type_rows();
    assert_eq!(
        type_rows
            .iter()
            .find(|row| row.typname == "textrange1")
            .unwrap()
            .typowner,
        owner_oid
    );
    assert_eq!(
        type_rows
            .iter()
            .find(|row| row.typname == "multitextrange1")
            .unwrap()
            .typowner,
        owner_oid
    );

    db.execute(1, "set role regress_multirange_owner").unwrap();

    match db.execute(1, "revoke usage on type multitextrange1 from public") {
        Err(ExecError::DetailedError {
            message,
            hint,
            sqlstate,
            ..
        }) => {
            assert_eq!(sqlstate, "42809");
            assert_eq!(message, "cannot set privileges of multirange types");
            assert_eq!(
                hint.as_deref(),
                Some("Set the privileges of the range type instead.")
            );
        }
        other => panic!("expected multirange privilege error, got {other:?}"),
    }

    db.execute(1, "revoke usage on type textrange1 from public")
        .unwrap();
    db.execute(1, "create temp table owner_can_use(f1 multitextrange1[])")
        .unwrap();
    db.execute(
        1,
        "revoke usage on type textrange1 from regress_multirange_owner",
    )
    .unwrap();

    match db.execute(
        1,
        "create temp table owner_cannot_use(f1 multitextrange1[])",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(sqlstate, "42501");
            assert_eq!(message, "permission denied for type multitextrange1");
        }
        other => panic!("expected type usage permission error, got {other:?}"),
    }
}

#[test]
fn polymorphic_sql_functions_accept_anymultirange_arguments() {
    let dir = temp_dir("polymorphic_sql_anymultirange");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create function anyarray_anymultirange_func(a anyarray, r anymultirange) \
         returns anyelement as 'select $1[1] + lower($2);' language sql",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select anyarray_anymultirange_func(ARRAY[1,2], int4multirange(int4range(10,20)))",
        ),
        vec![vec![Value::Int32(11)]]
    );
    assert!(
        db.execute(
            1,
            "select anyarray_anymultirange_func(ARRAY[1,2], nummultirange(numrange(10,20)))",
        )
        .is_err()
    );

    db.execute(
        1,
        "create function anycompatiblearray_anycompatiblemultirange_func(a anycompatiblearray, mr anycompatiblemultirange) \
         returns anycompatible as 'select $1[1] + lower($2);' language sql",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select anycompatiblearray_anycompatiblemultirange_func(ARRAY[1,2], multirange(int4range(10,20)))::text",
        ),
        vec![vec![Value::Text("11".into())]]
    );
    assert_eq!(
        query_rows(
            &db,
            1,
            "select anycompatiblearray_anycompatiblemultirange_func(ARRAY[1,2], multirange(numrange(10,20)))::text",
        ),
        vec![vec![Value::Text("11".into())]]
    );
    assert!(db
        .execute(
            1,
            "select anycompatiblearray_anycompatiblemultirange_func(ARRAY[1.1,2], multirange(int4range(10,20)))",
        )
        .is_err());

    db.execute(
        1,
        "create function mr_table_succeed(i anyelement, r anymultirange) returns table(i anyelement, r anymultirange) \
         as $$ select $1, $2 $$ language sql",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select i, r::text from mr_table_succeed(123, int4multirange(int4range(1,11)))",
        ),
        vec![vec![Value::Int32(123), Value::Text("{[1,11)}".into())]]
    );

    match db.execute(
        1,
        "create function mr_table_fail(i anyelement) returns table(i anyelement, r anymultirange) \
         as $$ select $1, '[1,10]' $$ language sql",
    ) {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(sqlstate, "42P13");
            assert_eq!(message, "cannot determine result data type");
            assert_eq!(
                detail.as_deref(),
                Some(
                    "A result of type anymultirange requires at least one input of type anyrange or anymultirange."
                )
            );
        }
        other => panic!("expected unresolved polymorphic result error, got {other:?}"),
    }

    db.execute(
        1,
        "create function mr_plpgsql(i anyrange) returns anymultirange \
         as $$ begin return multirange($1); end; $$ language plpgsql",
    )
    .unwrap();
    assert_eq!(
        query_rows(&db, 1, "select mr_plpgsql(int4range(1, 4))::text"),
        vec![vec![Value::Text("{[1,4)}".into())]]
    );
}

#[test]
fn multiranges_support_array_varbit_and_composite_subtypes() {
    let dir = temp_dir("multirange_array_varbit_composite_subtypes");
    let db = Database::open(&dir, 64).unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
                arraymultirange(arrayrange(array[1,2], array[2,1]))::text, \
                array[1,3] <@ arraymultirange(arrayrange(array[1,2], array[2,1])), \
                array[1,1] <@ arraymultirange(arrayrange(array[1,2], array[2,1]))",
        ),
        vec![vec![
            Value::Text(r#"{["{1,2}","{2,1}")}"#.into()),
            Value::Bool(true),
            Value::Bool(false),
        ]]
    );

    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
                varbitmultirange(varbitrange(B'01'::varbit, B'10'::varbit))::text, \
                B'011'::varbit <@ varbitmultirange(varbitrange(B'01'::varbit, B'10'::varbit))",
        ),
        vec![vec![Value::Text("{[01,10)}".into()), Value::Bool(true),]]
    );

    db.execute(1, "create type two_ints as (a int, b int)")
        .unwrap();
    db.execute(
        1,
        "create type two_ints_range as range (subtype = two_ints)",
    )
    .unwrap();
    assert_eq!(
        query_rows(
            &db,
            1,
            "select \
                two_ints_multirange(two_ints_range(row(1,2), row(3,4)))::text, \
                row_to_json(upper(two_ints_range(row(1,2), row(3,4))))::text",
        ),
        vec![vec![
            Value::Text(r#"{["(1,2)","(3,4)")}"#.into()),
            Value::Text(r#"{"a":3,"b":4}"#.into()),
        ]]
    );
}

#[test]
fn user_defined_multiranges_can_back_table_columns() {
    let dir = temp_dir("user_defined_multirange_table_columns");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(
        1,
        "create type float8range as range (subtype = float8, subtype_diff = float8mi)",
    )
    .unwrap();
    db.execute(
        1,
        "create table float8multirange_test(f8mr float8multirange, i int)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into float8multirange_test values \
         (float8multirange(float8range(-100.00007, '1.111113e9')), 42)",
    )
    .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select f8mr::text, i from float8multirange_test",),
        vec![vec![
            Value::Text("{[-100.00007,1111113000)}".into()),
            Value::Int32(42),
        ]]
    );

    db.execute(1, "drop table float8multirange_test").unwrap();
}

#[test]
fn builtin_range_aliases_resolve_through_generic_range_catalog_rows() {
    let dir = temp_dir("builtin_range_alias_generic_catalog");
    let db = Database::open(&dir, 64).unwrap();

    let visible = db.lazy_catalog_lookup(1, None, None);
    let int4range_row = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "int4range")
        .unwrap();

    assert_eq!(int4range_row.oid, INT4RANGE_TYPE_OID);
    assert_eq!(int4range_row.sql_type.kind, SqlTypeKind::Range);
    assert_eq!(int4range_row.sql_type.range_subtype_oid, INT4_TYPE_OID);

    db.execute(1, "create table t (span int4range)").unwrap();
    db.execute(1, "insert into t values ('[1,5)'::int4range)")
        .unwrap();

    assert_eq!(
        query_rows(&db, 1, "select span::text, lower(span)::text from t"),
        vec![vec![Value::Text("[1,5)".into()), Value::Text("1".into())]],
    );
}

#[test]
fn create_type_nested_dependencies_and_named_composite_arrays_work() {
    let dir = temp_dir("create_type_nested_dependencies");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create type complex as (r float8, i float8)")
        .unwrap();
    db.execute(1, "create type complex_bucket as (items complex[])")
        .unwrap();

    db.execute(1, "create type holder as (payload complex)")
        .unwrap();

    let catcache = db.backend_catcache(1, None).unwrap();
    let complex_type = catcache
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "complex")
        .unwrap();
    let complex_array_type = catcache
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "_complex")
        .unwrap();
    assert_eq!(complex_type.typarray, complex_array_type.oid);
    assert_eq!(complex_array_type.typelem, complex_type.oid);
    let holder_relation = db
        .lazy_catalog_lookup(1, None, None)
        .lookup_any_relation("holder")
        .unwrap();
    let bucket_relation = db
        .lazy_catalog_lookup(1, None, None)
        .lookup_any_relation("complex_bucket")
        .unwrap();
    assert!(catcache.depend_rows().iter().any(|row| {
        row.classid == PG_CLASS_RELATION_OID
            && row.objid == holder_relation.relation_oid
            && row.refclassid == PG_TYPE_RELATION_OID
            && row.refobjid == complex_type.oid
    }));
    assert!(catcache.depend_rows().iter().any(|row| {
        row.classid == PG_CLASS_RELATION_OID
            && row.objid == bucket_relation.relation_oid
            && row.refclassid == PG_TYPE_RELATION_OID
            && row.refobjid == complex_type.oid
    }));

    match db.execute(1, "drop type complex") {
        Err(ExecError::DetailedError {
            sqlstate,
            message,
            detail,
            ..
        }) => {
            assert_eq!(sqlstate, "2BP01");
            assert!(message.contains("cannot drop type complex"));
            assert!(
                detail
                    .unwrap_or_default()
                    .contains("type holder depends on type complex")
            );
        }
        other => panic!("expected dependent-type drop restriction, got {other:?}"),
    }
}

#[test]
fn recursive_cte_cycle_tracking_returns_record_arrays() {
    let dir = temp_dir("recursive_cte_record_arrays");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table graph(f int4, t int4, label text)")
        .unwrap();
    db.execute(
        1,
        "insert into graph values (1, 2, 'a'), (2, 3, 'b'), (3, 1, 'c')",
    )
    .unwrap();

    let rows = query_rows(
        &db,
        1,
        "with recursive search_graph(f, t, label, is_cycle, path) as (
            select *, false, array[row(g.f, g.t)] from graph g
            union all
            select g.*, row(g.f, g.t) = any(path), path || row(g.f, g.t)
            from graph g, search_graph sg
            where g.f = sg.t and not is_cycle
        )
        select * from search_graph order by label, is_cycle",
    );

    assert!(!rows.is_empty());
    assert!(rows.iter().all(|row| matches!(row[4], Value::PgArray(_))));
    assert!(rows.iter().any(|row| match &row[4] {
        Value::PgArray(array) => matches!(array.elements.first(), Some(Value::Record(_))),
        _ => false,
    }));
}

#[test]
fn setop_for_no_key_update_reports_postgres_compat_error() {
    let dir = temp_dir("setop_for_no_key_update");
    let db = Database::open(&dir, 64).unwrap();

    match db.execute(1, "select 1 except all select 1 for no key update") {
        Err(ExecError::Parse(ParseError::FeatureNotSupportedMessage(message)))
            if message == "FOR NO KEY UPDATE is not allowed with UNION/INTERSECT/EXCEPT" => {}
        other => panic!("expected set-op FOR NO KEY UPDATE rejection, got {other:?}"),
    }
}

#[test]
fn select_locking_family_executes_for_all_strengths() {
    let dir = temp_dir("select_locking_family_executes");
    let db = Database::open(&dir, 64).unwrap();

    db.execute(1, "create table lock_items (id int4 not null primary key)")
        .unwrap();
    db.execute(1, "insert into lock_items (id) values (1)")
        .unwrap();

    for sql in [
        "select id from lock_items where id = 1 for update",
        "select id from lock_items where id = 1 for no key update",
        "select id from lock_items where id = 1 for share",
        "select id from lock_items where id = 1 for key share",
    ] {
        match db.execute(1, sql).unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Int32(1)]],
                    "unexpected rows for {sql}"
                );
            }
            other => panic!("expected query result for {sql}, got {other:?}"),
        }
    }
}

#[test]
fn drop_type_enforces_restrict_and_if_exists() {
    let dir = temp_dir("drop_type_restrict");
    let db = Database::open(&dir, 64).unwrap();

    match db.execute(1, "drop type if exists missing_widget") {
        Ok(StatementResult::AffectedRows(0)) => {}
        other => panic!("expected no-op drop type if exists, got {other:?}"),
    }

    db.execute(1, "create type unused_widget as (id int4)")
        .unwrap();
    match db.execute(1, "drop type unused_widget") {
        Ok(StatementResult::AffectedRows(1)) => {}
        other => panic!("expected unused drop type success, got {other:?}"),
    }

    db.execute(1, "create type widget as (id int4, label text)")
        .unwrap();

    match db.execute(1, "drop type widget cascade") {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(message)))
            if message == "DROP TYPE CASCADE is not supported yet" => {}
        other => panic!("expected drop-type cascade rejection, got {other:?}"),
    }

    db.execute(
        1,
        "create function widget_rows(n int4) returns setof widget language plpgsql as $$ begin return query values (n, 'widget'); end $$",
    )
    .unwrap();

    match db.execute(1, "drop type widget") {
        Err(ExecError::DetailedError {
            sqlstate,
            message,
            detail,
            ..
        }) => {
            assert_eq!(sqlstate, "2BP01");
            assert!(message.contains("cannot drop type widget"));
            assert!(
                detail
                    .unwrap_or_default()
                    .contains("function widget_rows depends on type widget")
            );
        }
        other => panic!("expected dependent-function drop restriction, got {other:?}"),
    }
}

#[test]
fn drop_enum_type_enforces_restrict_and_if_exists() {
    let dir = temp_dir("drop_enum_type_restrict");
    let db = Database::open(&dir, 64).unwrap();

    match db.execute(1, "drop type if exists missing_mood") {
        Ok(StatementResult::AffectedRows(0)) => {}
        other => panic!("expected no-op drop type if exists, got {other:?}"),
    }

    db.execute(1, "create type unused_mood as enum ('sad')")
        .unwrap();
    match db.execute(1, "drop type unused_mood") {
        Ok(StatementResult::AffectedRows(1)) => {}
        other => panic!("expected unused enum drop success, got {other:?}"),
    }

    db.execute(1, "create type mood as enum ('sad', 'ok')")
        .unwrap();
    db.execute(1, "create table feelings(current_mood mood)")
        .unwrap();

    match db.execute(1, "drop type mood") {
        Err(ExecError::DetailedError {
            sqlstate,
            message,
            detail,
            ..
        }) => {
            assert_eq!(sqlstate, "2BP01");
            assert!(message.contains("cannot drop type mood"));
            assert!(
                detail
                    .unwrap_or_default()
                    .contains("table feelings depends on type mood")
            );
        }
        other => panic!("expected dependent enum drop restriction, got {other:?}"),
    }
}

#[test]
fn drop_range_type_enforces_restrict_and_if_exists() {
    let dir = temp_dir("drop_range_type_restrict");
    let db = Database::open(&dir, 64).unwrap();

    match db.execute(1, "drop type if exists missing_float8range") {
        Ok(StatementResult::AffectedRows(0)) => {}
        other => panic!("expected no-op drop type if exists, got {other:?}"),
    }

    db.execute(
        1,
        "create type unused_float8range as range (subtype = float8)",
    )
    .unwrap();
    match db.execute(1, "drop type unused_float8range") {
        Ok(StatementResult::AffectedRows(1)) => {}
        other => panic!("expected unused range drop success, got {other:?}"),
    }

    db.execute(1, "create type float8range as range (subtype = float8)")
        .unwrap();
    db.execute(1, "create table measurements(span float8range)")
        .unwrap();

    match db.execute(1, "drop type float8range") {
        Err(ExecError::DetailedError {
            sqlstate,
            message,
            detail,
            ..
        }) => {
            assert_eq!(sqlstate, "2BP01");
            assert!(message.contains("cannot drop type float8range"));
            assert!(
                detail
                    .unwrap_or_default()
                    .contains("table measurements depends on type float8range")
            );
        }
        other => panic!("expected dependent range drop restriction, got {other:?}"),
    }
}

#[test]
fn composite_type_persists_across_reopen_without_storage() {
    let base = temp_dir("composite_type_reopen");
    let db = Database::open_with_options(&base, DatabaseOpenOptions::new(64)).unwrap();

    db.execute(1, "create type widget as (id int4, label text)")
        .unwrap();
    assert_eq!(relfilenode_for(&db, 1, "widget"), 0);

    drop(db);

    let reopened = Database::open(&base, 64).unwrap();
    let visible = reopened.lazy_catalog_lookup(1, None, None);
    let widget_relation = visible.lookup_any_relation("widget").unwrap();
    let widget_type = visible
        .type_rows()
        .into_iter()
        .find(|row| row.typname == "widget")
        .unwrap();

    assert_eq!(widget_relation.relkind, 'c');
    assert_eq!(widget_type.typrelid, widget_relation.relation_oid);
    assert_eq!(relfilenode_for(&reopened, 1, "widget"), 0);
    assert_eq!(
        query_rows(
            &reopened,
            1,
            "select attname from pg_attribute where attrelid = (select oid from pg_class where relname = 'widget') and attnum > 0 order by attnum",
        ),
        vec![
            vec![Value::Text("id".into())],
            vec![Value::Text("label".into())],
        ]
    );
}

#[test]
fn explicit_text_to_name_cast_works_via_pg_cast() {
    let dir = temp_dir("explicit_text_to_name_cast");
    let db = Database::open(&dir, 64).unwrap();

    assert_eq!(
        query_rows(&db, 1, "select 'hi mom'::name, '{alice,bob}'::name[]"),
        vec![vec![
            Value::Text("hi mom".into()),
            Value::Array(vec![Value::Text("alice".into()), Value::Text("bob".into()),]),
        ]]
    );
}

#[test]
fn parse_ident_splits_qualified_identifiers() {
    let db = Database::open_ephemeral(32).unwrap();

    assert_eq!(
        query_rows(
            &db,
            1,
            "select parse_ident('\"SomeSchema\".someTable'), parse_ident('public.fn(int4)', false)",
        ),
        vec![vec![
            Value::Array(vec![
                Value::Text("SomeSchema".into()),
                Value::Text("sometable".into()),
            ]),
            Value::Array(vec![Value::Text("public".into()), Value::Text("fn".into()),]),
        ]]
    );
}

#[test]
fn parse_ident_rejects_invalid_identifiers_with_pg_style_detail() {
    let db = Database::open_ephemeral(32).unwrap();

    match db.execute(1, "select parse_ident('foo.')") {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(message, "string is not a valid identifier: \"foo.\"");
            assert_eq!(detail.as_deref(), Some("No valid identifier after \".\"."));
            assert_eq!(sqlstate, "22023");
        }
        other => panic!("expected parse_ident error, got {:?}", other),
    }

    match db.execute(1, "select parse_ident('\"\"')") {
        Err(ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }) => {
            assert_eq!(message, "string is not a valid identifier: \"\"\"\"");
            assert_eq!(
                detail.as_deref(),
                Some("Quoted identifier must not be empty.")
            );
            assert_eq!(sqlstate, "22023");
        }
        other => panic!("expected parse_ident empty-quote error, got {:?}", other),
    }
}

#[test]
fn case_expressions_execute_with_pg_style_null_and_short_circuit_semantics() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    let result = session
        .execute(
            &db,
            "select
                case 2 when 1 then 'a' when 2 then 'b' else 'c' end,
                case null when null then 1 else 2 end,
                case when false then 1 end,
                case when true then 1 else 'nope'::int4 end",
        )
        .expect("run case query");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("b".into()),
                    Value::Int32(2),
                    Value::Null,
                    Value::Int32(1),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn case_expressions_work_in_where_and_order_by() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4, label text)")
        .expect("create table");
    session
        .execute(&db, "insert into items values (1, 'c'), (2, 'a'), (3, 'b')")
        .expect("insert rows");

    let result = session
        .execute(
            &db,
            "select id
             from items
             where case when id = 1 then false else true end
             order by case when id = 2 then 0 else 1 end, id",
        )
        .expect("run case query");

    match result {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
