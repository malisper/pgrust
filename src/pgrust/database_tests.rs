use super::*;
use crate::backend::commands::analyze::collect_analyze_stats;
use crate::backend::executor::{ExecError, Value};
use crate::backend::parser::{BoundRelation, CatalogLookup, ParseError, SqlType, SqlTypeKind};
use crate::backend::utils::cache::lsyscache::LazyCatalogLookup;
use crate::include::catalog::{
    FLOAT8_TYPE_OID, INT4_TYPE_OID, INT4RANGE_TYPE_OID, PG_CLASS_RELATION_OID,
    PG_PROC_RELATION_OID, PG_TYPE_RELATION_OID,
};
use crate::include::nodes::parsenodes::MaintenanceTarget;
use crate::include::nodes::primnodes::QueryColumn;
use crate::pl::plpgsql::{clear_notices, take_notices};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use std::time::{Duration, Instant};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const CONTENTION_TEST_TIMEOUT: Duration = Duration::from_secs(15);
const HEAVY_CONTENTION_TEST_TIMEOUT: Duration = Duration::from_secs(30);
const STRESS_TEST_TIMEOUT: Duration = Duration::from_secs(60);
const SAME_ROW_UPDATE_TEST_TIMEOUT: Duration = Duration::from_secs(20);
const PGBENCH_STYLE_TEST_TIMEOUT: Duration = Duration::from_secs(20);
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
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts: db.interrupt_state(client_id),
        stats: Arc::clone(&db.stats),
        session_stats: db.session_stats_state(client_id),
        snapshot: db.txns.read().snapshot_for_command(xid, cid).unwrap(),
        client_id,
        current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        next_command_id: cid,
        timed: false,
        allow_side_effects: false,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        catalog: visible_catalog,
        compiled_functions: HashMap::new(),
        cte_tables: HashMap::new(),
        cte_producers: HashMap::new(),
        recursive_worktables: HashMap::new(),
        deferred_foreign_keys: None,
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
fn jsonb_input_respects_max_stack_depth_setting() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

    session
        .execute(&db, "set max_stack_depth = '100kB'")
        .expect("set max_stack_depth");

    let err = session
        .execute(&db, &format!("select '{}'::jsonb", "[".repeat(10_000)))
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            hint: Some(hint),
            sqlstate,
            ..
        } if message == "stack depth limit exceeded"
            && sqlstate == "54001"
            && hint.contains("\"max_stack_depth\" (currently 100kB)")
    ));
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

fn session_query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match session.execute(db, sql).unwrap() {
        StatementResult::Query { rows, .. } => rows,
        other => panic!("expected query result, got {:?}", other),
    }
}

fn take_notice_messages() -> Vec<String> {
    take_notices()
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

fn relation_locator_for(db: &Database, client_id: u32, relname: &str) -> crate::RelFileLocator {
    crate::RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: relfilenode_for(db, client_id, relname) as u32,
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

    waiter
        .execute(&db, "set statement_timeout = '200ms'")
        .unwrap();
    match waiter.execute(&db, "select count(*) from t").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
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
    session
        .execute(&db, "create table d (dd text) inherits (b, c, a)")
        .unwrap();

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
            "select schemaname, viewname, viewowner, definition from pg_views order by viewname",
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
        Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. }))
            if actual.contains("view depends on it: base_view") => {}
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
                if name == "item_view" && expected == "table" => {}
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

fn relation_fork_nblocks(
    db: &Database,
    rel: crate::RelFileLocator,
    fork: crate::backend::storage::smgr::ForkNumber,
) -> u32 {
    db.pool
        .with_storage_mut(|storage| storage.smgr.nblocks(rel, fork))
        .unwrap()
}

fn assert_explain_uses_index(db: &Database, client_id: u32, sql: &str, index_name: &str) {
    let relfilenode = relfilenode_for(db, client_id, index_name);
    let lines = explain_lines(db, client_id, sql);
    assert!(
        lines
            .iter()
            .any(|line| line.contains(&format!("Index Scan using rel {relfilenode} "))),
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
        !lines.iter().any(|line| line.contains("Index Scan")),
        "expected no index scan, got {lines:?}"
    );
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
                && (expected == "table" || expected == "table, view, or sequence") => {}
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

    match db.execute(1, "alter table items add column note text not null") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual }))
            if expected == "ADD COLUMN without NOT NULL" && actual == "NOT NULL" => {}
        other => panic!("expected NOT NULL rejection, got {other:?}"),
    }

    match db.execute(1, "alter table items add column key_id int4 primary key") {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual }))
            if (expected == "ADD COLUMN without PRIMARY KEY" && actual == "PRIMARY KEY")
                || (expected == "ADD COLUMN without NOT NULL" && actual == "NOT NULL") => {}
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
                    Value::Text("@ 1 hour 10 secs".into())
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
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
        Err(ExecError::UniqueViolation { constraint }) if constraint == "items_name_lower_key" => {}
        other => panic!("expected unique expression violation, got {other:?}"),
    }
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
        .position(|line| line.trim_start().starts_with("Hash  "));
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
        .filter_map(|(index, line)| line.trim_start().starts_with("Sort  ").then_some(index))
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
        .position(|line| line.trim_start().starts_with("ProjectSet"))
        .expect("expected ProjectSet in explain output");
    let sort_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("Sort  "))
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
    for i in 0..1500 {
        db.execute(1, &format!("insert into items values ({i}, 'row{i}')"))
            .unwrap();
    }
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
        Err(ExecError::UniqueViolation { constraint }) => {
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
        }) if relation == "items" && column == "id" && constraint == "items_id_not_null" => {}
        other => panic!("expected primary-key NOT NULL rejection, got {other:?}"),
    }

    db.execute(1, "insert into items values (1, 10)").unwrap();
    db.execute(1, "insert into items values (2, null)").unwrap();
    db.execute(1, "insert into items values (3, null)").unwrap();

    match db.execute(1, "insert into items values (1, 11)") {
        Err(ExecError::UniqueViolation { constraint }) => {
            assert_eq!(constraint, "items_pkey");
        }
        other => panic!("expected primary-key duplicate rejection, got {other:?}"),
    }

    match db.execute(1, "insert into items values (4, 10)") {
        Err(ExecError::UniqueViolation { constraint }) => {
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
        Err(ExecError::UniqueViolation { constraint }) => {
            assert_eq!(constraint, "memberships_pkey");
        }
        other => panic!("expected composite primary-key rejection, got {other:?}"),
    }

    match db.execute(1, "insert into memberships values (4, 10, 100)") {
        Err(ExecError::UniqueViolation { constraint }) => {
            assert_eq!(constraint, "memberships_tag_note_key");
        }
        other => panic!("expected composite unique rejection, got {other:?}"),
    }

    match db.execute(1, "insert into memberships values (5, null, 102)") {
        Err(ExecError::NotNullViolation {
            relation,
            column,
            constraint,
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
fn unsupported_create_table_like_does_not_poison_catalog_after_sequence_drop() {
    let base = temp_dir("unsupported_create_table_like_sequence");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table items (id int4)").unwrap();
    db.execute(1, "create sequence ctlseq1").unwrap();
    match db.execute(1, "create table ctlt10 (like ctlseq1)") {
        Err(ExecError::Parse(ParseError::FeatureNotSupported(feature))) => {
            assert_eq!(feature, "CREATE TABLE ... LIKE")
        }
        other => panic!("expected unsupported CREATE TABLE LIKE error, got {:?}", other),
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

    db.execute(1, "insert into children values (1, 42)").unwrap();
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

    db.execute(1, "insert into children values (2, 99)").unwrap();
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
        "alter table parents alter column id type int8",
        "alter table children alter column parent_id type int8",
        "alter table parents drop constraint parents_pkey",
    ] {
        match db.execute(1, sql) {
            Err(ExecError::Parse(ParseError::UnexpectedToken { actual, .. }))
                if actual.contains("foreign key constraint")
                    || actual.contains("referenced by foreign key")
                    || actual.contains("used by foreign key") => {}
            other => panic!("expected foreign-key DDL blocker for `{sql}`, got {other:?}"),
        }
    }

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
        Err(ExecError::UniqueViolation { constraint }) if constraint == "department_name_key" => {}
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
        Err(ExecError::UniqueViolation { constraint }) => {
            assert_eq!(constraint, "items_id_key");
            assert_eq!(
                crate::backend::libpq::pqformat::format_exec_error(&ExecError::UniqueViolation {
                    constraint: constraint.clone()
                }),
                "duplicate key value violates unique constraint \"items_id_key\""
            );
        }
        other => panic!("expected unique violation, got {:?}", other),
    }
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
        Err(ExecError::UniqueViolation { constraint }) => {
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
                for i in 0..75 {
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
                for i in 0..120 {
                    let id = (i % 75) as i32;
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
        vec![vec![Value::Int64(300)]]
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
                for i in 0..200 {
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
                for _ in 0..80 {
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
        vec![vec![Value::Int64(800)]]
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
            Err(ExecError::UniqueViolation { constraint }) => {
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
        for i in 0..400 {
            db.execute(1, &format!("insert into items values ({i}, 'before{i}')"))
                .unwrap();
        }
        db.execute(1, "create index items_id_idx on items (id)")
            .unwrap();
        for i in 400..900 {
            db.execute(1, &format!("insert into items values ({i}, 'after{i}')"))
                .unwrap();
        }
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
fn vacuum_records_recyclable_btree_pages_in_fsm() {
    let base = temp_dir("vacuum_recycles_btree_pages");
    let db = Database::open(&base, 128).unwrap();
    let mut session = Session::new(1);

    session
        .execute(&db, "create table items (id int4 not null, note text)")
        .unwrap();
    for i in 0..1500 {
        session
            .execute(&db, &format!("insert into items values ({i}, 'row{i}')"))
            .unwrap();
    }
    session
        .execute(&db, "create index items_id_idx on items (id)")
        .unwrap();

    let index_rel = relation_locator_for(&db, 1, "items_id_idx");
    for i in 0..900 {
        session
            .execute(&db, &format!("delete from items where id = {i}"))
            .unwrap();
    }
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
    for i in 0..1800 {
        session
            .execute(&db, &format!("insert into items values ({i}, 'row{i}')"))
            .unwrap();
    }
    session
        .execute(&db, "create index items_id_idx on items (id)")
        .unwrap();

    let index_rel = relation_locator_for(&db, 1, "items_id_idx");
    for i in 0..1200 {
        session
            .execute(&db, &format!("delete from items where id = {i}"))
            .unwrap();
    }
    session.execute(&db, "vacuum items").unwrap();

    let blocks_after_vacuum = relation_fork_nblocks(
        &db,
        index_rel,
        crate::backend::storage::smgr::ForkNumber::Main,
    );

    for i in 2000..2600 {
        session
            .execute(&db, &format!("insert into items values ({i}, 'row{i}')"))
            .unwrap();
    }

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
    bootstrap.execute(&db, "grant pg_checkpoint to tenant").unwrap();

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
        buffer_page = db.pool.read_page(pinned.buffer_id()).unwrap();
        drop(pinned);

        let disk_page_before = read_relation_block(&db, rel, 0);
        assert_ne!(
            disk_page_before, buffer_page,
            "expected heap page to remain dirty in shared buffers before CHECKPOINT"
        );

        committed_xid = db
            .txns
            .read()
            .snapshot(INVALID_TRANSACTION_ID)
            .unwrap()
            .xmax
            - 1;

        session.execute(&db, "checkpoint").unwrap();

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
        lines
            .iter()
            .any(|line| line.contains(&format!("Index Scan using rel {relfilenode} "))),
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
fn create_index_still_rejects_temp_tables_when_temp_is_first_visible() {
    let base = temp_dir("search_path_create_index_temp");
    let db = Database::open(&base, 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create table items (id int4 not null)")
        .unwrap();
    session
        .execute(&db, "create temp table items (id int4 not null)")
        .unwrap();

    let err = session
        .execute(&db, "create index items_temp_idx on items (id)")
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::UnexpectedToken { expected, actual })
            if expected == "permanent table for CREATE INDEX" && actual == "temporary table"
    ));
    assert!(
        db.catalog
            .read()
            .catalog_snapshot()
            .unwrap()
            .get("items_temp_idx")
            .is_none()
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
                for _ in 0..50 {
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

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);
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

    join_all_with_timeout(handles, TEST_TIMEOUT);

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

    join_all_with_timeout(handles, TEST_TIMEOUT);

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

    for aid in 1..=5000 {
        db.execute(
                1,
                &format!(
                    "insert into pgbench_accounts (aid, bid, abalance, filler) values ({aid}, 1, 0, 'x')"
                ),
            )
            .unwrap();
    }

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

    let base = temp_dir("no_dirty_reads");
    let db = Database::open(&base, 64).unwrap();

    db.execute(1, "create table dirty (id int4 not null)")
        .unwrap();

    // Shared flags: writer signals when it has inserted (but not committed),
    // and when it has committed.
    let inserted = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicBool::new(false));

    let inserted_w = inserted.clone();
    let committed_w = committed.clone();
    let db_w = db.clone();

    let writer = thread::spawn(move || {
        let mut session = Session::new(1700);
        session.execute(&db_w, "begin").unwrap();
        session
            .execute(&db_w, "insert into dirty (id) values (1)")
            .unwrap();
        // Signal that the insert is done but not yet committed.
        inserted_w.store(true, Ordering::Release);
        // Busy-wait a moment to give the reader time to observe the state.
        let deadline = Instant::now() + Duration::from_millis(200);
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
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
            for _ in 0..50 {
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
            for _ in 0..200 {
                let _ = db
                    .execute(
                        (t + 3000) as ClientId,
                        "select val from locktest where id = 1",
                    )
                    .unwrap();
            }
        }));
    }

    join_all_with_timeout(handles, CONTENTION_TEST_TIMEOUT);
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
    let iters = 100;
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

    join_all_with_timeout(handles, STRESS_TEST_TIMEOUT);

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
fn create_tablespace_adds_pg_tablespace_row() {
    let db = Database::open_ephemeral(32).expect("open ephemeral database");
    let mut session = Session::new(1);

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
            "select spcname from pg_tablespace where spcname = 'regress_tblspace'",
        ),
        vec![vec![Value::Text("regress_tblspace".into())]]
    );
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
