//! PLPGSQL: the plpgsql/SPI procedural-execution residual-arm drain.
//!
//! The P2 `plpg` module already covers the common plpgsql surface
//! (IF/CASE, the basic loops, SELECT INTO STRICT, a `RAISE ... USING
//! ERRCODE` form, `EXECUTE format() INTO USING`, and GET DIAGNOSTICS
//! ROW_COUNT). This module drains the *residual* arms the coverage gap
//! reports (docs/fuzzing/gap-report-006.tsv, line-drain-queue `plpgsql-
//! arms`) still show hollow in pl_exec.c / pl_comp.c / pl_gram.y / spi.c:
//!
//!   - exec_stmt_raise: the full USING option matrix (MESSAGE/DETAIL/HINT/
//!     ERRCODE/COLUMN/CONSTRAINT/DATATYPE/TABLE/SCHEMA), RAISE by condition
//!     name, RAISE SQLSTATE, the multi-level ladder, and the bare re-RAISE;
//!   - exec_stmt_getdiag: GET STACKED DIAGNOSTICS across the whole field
//!     set, and GET [CURRENT] DIAGNOSTICS PG_CONTEXT / PG_EXCEPTION_CONTEXT;
//!   - exec_stmt_return_next / exec_stmt_return_query: RETURNS TABLE(...),
//!     RETURN NEXT of a record, RETURN QUERY and RETURN QUERY EXECUTE;
//!   - exec_stmt_open / exec_stmt_fetch / exec_stmt_forc + read_fetch_
//!     direction / read_cursor_args + SPI_cursor_open_internal: bound
//!     cursors with args (cursor FOR loop), explicit OPEN SCROLL / every
//!     FETCH direction / MOVE / CLOSE, and OPEN FOR EXECUTE;
//!   - exec_stmt_foreach_a: FOREACH ... SLICE over a multi-dim array;
//!   - exec_stmt_dynexecute: EXECUTE ... INTO STRICT (record target) and
//!     dynamic DML with GET DIAGNOSTICS ROW_COUNT;
//!   - exec_move_row_from_datum / exec_move_row_from_fields + exec_stmt_
//!     execsql INTO: %ROWTYPE and record targets, whole-row (composite
//!     datum) assignment into a record;
//!   - make_callstmt_target: CALL of an INOUT procedure with a variable
//!     target from inside plpgsql;
//!   - plpgsql_compile_inline / plpgsql_inline_handler: DO blocks;
//!   - exec_stmt_assert: ASSERT pass + message eval and the caught
//!     assert_failure path;
//!   - read_datatype / plpgsql_parse_cwordtype: %TYPE, tab.col%TYPE,
//!     %TYPE[], var%TYPE, CONSTANT NOT NULL DEFAULT.
//!
//! Every group is self-contained on the P2 discipline: it creates its
//! objects (functions/procedures/tables) under session-unique `fz_pl2_*`
//! names (crate::plpg::PlpgState::next_p2), exercises + probes them with
//! deterministic literal-driven, pk-total-ordered, float-free statements,
//! and drops everything it created. Error paths are targets, not hazards:
//! every deliberate error is raised and caught *inside* a function body so
//! the top-level statement returns a deterministic value (identical on both
//! differential sides by construction — same plpgsql source, same
//! behaviour). No cross-group object edges, no catalog dependency.

use crate::stmt::{Gen, StmtKind};

/// Registry entry point (stmt::STMT_MODULES / toggles::REGISTRY: "plpgsql").
pub fn gen_plpg2_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpgsql");
    let action = g.weights.pick(
        g.rng,
        &[
            "plpg2:raise",
            "plpg2:diag",
            "plpg2:srf",
            "plpg2:cursor",
            "plpg2:foreach",
            "plpg2:dynexec",
            "plpg2:record",
            "plpg2:call",
            "plpg2:do",
            "plpg2:assert",
            "plpg2:vartype",
        ],
    );
    match action {
        "plpg2:raise" => gen_raise_group(g),
        "plpg2:diag" => gen_diag_group(g),
        "plpg2:srf" => gen_srf_group(g),
        "plpg2:cursor" => gen_cursor_group(g),
        "plpg2:foreach" => gen_foreach_group(g),
        "plpg2:dynexec" => gen_dynexec_group(g),
        "plpg2:record" => gen_record_group(g),
        "plpg2:call" => gen_call_group(g),
        "plpg2:do" => gen_do_group(g),
        "plpg2:assert" => gen_assert_group(g),
        _ => gen_vartype_group(g),
    }
}

/// Boundary-heavy small int literal (deterministic, no float).
fn int_lit(g: &mut Gen) -> i64 {
    match g.rng.below(6) {
        0 => 0,
        1 => 1,
        2 => -1,
        3 => 7,
        _ => g.rng.range_i64(-20, 40),
    }
}

/// Wrap a body into a no-arg scalar function group: CREATE + SELECT + DROP.
fn fn_group(name: &str, ret: &str, body: &str) -> Vec<StmtKind> {
    vec![
        StmtKind::Raw(format!(
            "CREATE FUNCTION {name}() RETURNS {ret} LANGUAGE plpgsql AS $fzp$ {body} $fzp$;"
        )),
        StmtKind::Raw(format!("SELECT {name}();")),
        StmtKind::Raw(format!("DROP FUNCTION {name}();")),
    ]
}

// ------------------------------------------------------------ RAISE ----

/// exec_stmt_raise: full USING matrix caught by GET STACKED DIAGNOSTICS,
/// RAISE-by-condition, RAISE SQLSTATE, the level ladder, and re-RAISE.
fn gen_raise_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:raise");
    let n = g.plpg.next_p2();
    let name = format!("fz_pl2_ra_{n}");
    let form = g.weights.pick(
        g.rng,
        &[
            "plpg2:raise:full",
            "plpg2:raise:cond",
            "plpg2:raise:sqlstate",
            "plpg2:raise:level",
            "plpg2:raise:reraise",
        ],
    );
    g.fire(form);
    let k = int_lit(g);
    let (ret, body) = match form {
        // Every USING option set, read back through the full stacked-
        // diagnostics field set. ERRCODE 22012 = division_by_zero.
        "plpg2:raise:full" => (
            "text",
            "DECLARE st text; ms text; dt text; hn text; cn text; kn text; \
             tn text; bn text; sn text; \
             BEGIN RAISE EXCEPTION USING ERRCODE = '22012', MESSAGE = 'fz raise msg', \
             DETAIL = 'fz detail', HINT = 'fz hint', COLUMN = 'colx', \
             CONSTRAINT = 'conx', DATATYPE = 'int4', TABLE = 'tblx', SCHEMA = 'public'; \
             EXCEPTION WHEN division_by_zero THEN \
             GET STACKED DIAGNOSTICS st = RETURNED_SQLSTATE, ms = MESSAGE_TEXT, \
             dt = PG_EXCEPTION_DETAIL, hn = PG_EXCEPTION_HINT, cn = COLUMN_NAME, \
             kn = CONSTRAINT_NAME, tn = PG_DATATYPE_NAME, bn = TABLE_NAME, \
             sn = SCHEMA_NAME; \
             RETURN concat_ws('|', st, ms, dt, hn, cn, kn, tn, bn, sn); END"
                .to_string(),
        ),
        // RAISE by bare condition name (no message string).
        "plpg2:raise:cond" => (
            "text",
            "BEGIN RAISE division_by_zero; \
             EXCEPTION WHEN division_by_zero THEN RETURN 'C:' || SQLSTATE; END"
                .to_string(),
        ),
        // RAISE SQLSTATE literal.
        "plpg2:raise:sqlstate" => (
            "text",
            "BEGIN RAISE SQLSTATE '22012'; \
             EXCEPTION WHEN division_by_zero THEN RETURN 'S:' || SQLSTATE; END"
                .to_string(),
        ),
        // The non-error level ladder (client sees INFO/NOTICE/WARNING).
        "plpg2:raise:level" => (
            "text",
            format!(
                "DECLARE k int4 := {k}; \
                 BEGIN RAISE NOTICE 'fz notice % / %', k, k * 2 USING HINT = 'nh'; \
                 RAISE WARNING 'fz warning %', k USING DETAIL = 'wd'; \
                 RAISE INFO 'fz info %', k; RAISE LOG 'fz log %', k; \
                 RAISE DEBUG 'fz debug %', k; RETURN 'L:' || k::text; END"
            ),
        ),
        // Inner handler re-raises with a bare RAISE; the outer catches it.
        _ => (
            "text",
            "BEGIN BEGIN RAISE EXCEPTION 'fz inner' USING ERRCODE = '22012'; \
             EXCEPTION WHEN division_by_zero THEN RAISE; END; RETURN 'unreached'; \
             EXCEPTION WHEN division_by_zero THEN RETURN 'RR:' || SQLSTATE; END"
                .to_string(),
        ),
    };
    fn_group(&name, ret, &body)
}

// ------------------------------------------------------- diagnostics ----

/// exec_stmt_getdiag: GET [CURRENT] DIAGNOSTICS PG_CONTEXT / ROW_COUNT and
/// GET STACKED DIAGNOSTICS PG_EXCEPTION_CONTEXT. The context strings embed
/// the (per-side identical) call frame, so the probe returns only presence
/// booleans + the row count — the arm is covered without pinning volatile
/// context text.
fn gen_diag_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:diag");
    let n = g.plpg.next_p2();
    let name = format!("fz_pl2_dg_{n}");
    let form = g.weights.pick(g.rng, &["plpg2:diag:cur", "plpg2:diag:stacked"]);
    g.fire(form);
    let (ret, body) = if form == "plpg2:diag:cur" {
        (
            "text",
            "DECLARE c text; r int4; \
             BEGIN PERFORM * FROM generate_series(1, 5); \
             GET DIAGNOSTICS r = ROW_COUNT; GET DIAGNOSTICS c = PG_CONTEXT; \
             RETURN 'cur:' || r::text || ':' || (c IS NOT NULL)::text; END"
                .to_string(),
        )
    } else {
        (
            "text",
            "DECLARE x text; \
             BEGIN RAISE EXCEPTION 'fz ctx' USING ERRCODE = '22012'; \
             EXCEPTION WHEN division_by_zero THEN \
             GET STACKED DIAGNOSTICS x = PG_EXCEPTION_CONTEXT; \
             RETURN 'stk:' || (x IS NOT NULL)::text; END"
                .to_string(),
        )
    };
    fn_group(&name, ret, &body)
}

// -------------------------------------------------- set-returning fns ----

/// exec_stmt_return_next / exec_stmt_return_query: RETURNS TABLE, RETURN
/// NEXT of a record, RETURN QUERY [EXECUTE].
fn gen_srf_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:srf");
    let n = g.plpg.next_p2();
    let name = format!("fz_pl2_sf_{n}");
    let form = g
        .weights
        .pick(g.rng, &["plpg2:srf:table", "plpg2:srf:setof", "plpg2:srf:record"]);
    g.fire(form);
    match form {
        // RETURNS TABLE: OUT-column assignment + RETURN NEXT, then RETURN
        // QUERY appends a generated tail.
        "plpg2:srf:table" => vec![
            StmtKind::Raw(format!(
                "CREATE FUNCTION {name}() RETURNS TABLE(k int4, v text) LANGUAGE plpgsql \
                 AS $fzp$ BEGIN k := 1; v := 'a'; RETURN NEXT; k := 2; v := 'b'; \
                 RETURN NEXT; RETURN QUERY SELECT gs, ('r' || gs)::text \
                 FROM generate_series(3, 5) gs; RETURN; END $fzp$;"
            )),
            StmtKind::Raw(format!("SELECT k, v FROM {name}() ORDER BY k;")),
            StmtKind::Raw(format!("DROP FUNCTION {name}();")),
        ],
        // RETURNS SETOF scalar: RETURN NEXT in a query FOR loop, then a
        // dynamic RETURN QUERY EXECUTE tail.
        "plpg2:srf:setof" => vec![
            StmtKind::Raw(format!(
                "CREATE FUNCTION {name}() RETURNS SETOF int4 LANGUAGE plpgsql \
                 AS $fzp$ DECLARE r int4; BEGIN FOR r IN SELECT gs FROM \
                 generate_series(1, 4) gs LOOP RETURN NEXT r * 10; END LOOP; \
                 RETURN QUERY EXECUTE 'SELECT $1 UNION SELECT $2' USING 100, 200; \
                 RETURN; END $fzp$;"
            )),
            StmtKind::Raw(format!("SELECT * FROM {name}() ORDER BY 1;")),
            StmtKind::Raw(format!("DROP FUNCTION {name}();")),
        ],
        // RETURNS SETOF record: RETURN NEXT of a whole record var.
        _ => vec![
            StmtKind::Raw(format!(
                "CREATE FUNCTION {name}() RETURNS SETOF record LANGUAGE plpgsql \
                 AS $fzp$ DECLARE r record; BEGIN FOR r IN SELECT gs AS a, gs * gs AS b \
                 FROM generate_series(1, 3) gs LOOP RETURN NEXT r; END LOOP; \
                 RETURN; END $fzp$;"
            )),
            StmtKind::Raw(format!("SELECT a, b FROM {name}() AS tt(a int4, b int4) ORDER BY a;")),
            StmtKind::Raw(format!("DROP FUNCTION {name}();")),
        ],
    }
}

// ------------------------------------------------------------ cursors ----

/// exec_stmt_open / exec_stmt_fetch / exec_stmt_forc + read_fetch_direction
/// + read_cursor_args + SPI_cursor_open_internal.
fn gen_cursor_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:cursor");
    let n = g.plpg.next_p2();
    let name = format!("fz_pl2_cu_{n}");
    let form = g.weights.pick(
        g.rng,
        &["plpg2:cursor:forc", "plpg2:cursor:explicit", "plpg2:cursor:exec"],
    );
    g.fire(form);
    let body = match form {
        // Bound cursor with args driven by a cursor FOR loop (exec_stmt_forc
        // + read_cursor_args).
        "plpg2:cursor:forc" => "DECLARE cur CURSOR (lo int4) FOR \
             SELECT gs FROM generate_series(lo, lo + 4) gs; s int4 := 0; rec record; \
             BEGIN FOR rec IN cur(2) LOOP s := s + rec.gs; END LOOP; RETURN s; END"
            .to_string(),
        // Explicit OPEN SCROLL + every FETCH direction + MOVE + CLOSE.
        "plpg2:cursor:explicit" => "DECLARE cur refcursor; v int4; t int4 := 0; \
             BEGIN OPEN cur SCROLL FOR SELECT gs FROM generate_series(1, 10) gs; \
             FETCH FIRST FROM cur INTO v; t := t + coalesce(v, 0); \
             FETCH LAST FROM cur INTO v; t := t + coalesce(v, 0); \
             FETCH ABSOLUTE 3 FROM cur INTO v; t := t + coalesce(v, 0); \
             FETCH RELATIVE -1 FROM cur INTO v; t := t + coalesce(v, 0); \
             MOVE FORWARD 2 FROM cur; FETCH NEXT FROM cur INTO v; t := t + coalesce(v, 0); \
             MOVE BACKWARD 1 FROM cur; FETCH PRIOR FROM cur INTO v; t := t + coalesce(v, 0); \
             CLOSE cur; RETURN t; END"
            .to_string(),
        // OPEN FOR EXECUTE (dynamic cursor) + FOUND-terminated FETCH loop.
        _ => "DECLARE cur refcursor; v int4; t int4 := 0; \
             BEGIN OPEN cur FOR EXECUTE 'SELECT gs FROM generate_series($1, $2) gs' \
             USING 1, 6; LOOP FETCH cur INTO v; EXIT WHEN NOT FOUND; t := t + v; \
             END LOOP; CLOSE cur; RETURN t; END"
            .to_string(),
    };
    fn_group(&name, "int4", &body)
}

// ------------------------------------------------------------ FOREACH ----

/// exec_stmt_foreach_a with SLICE over a multi-dimensional array.
fn gen_foreach_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:foreach");
    let n = g.plpg.next_p2();
    let name = format!("fz_pl2_fe_{n}");
    let body = "DECLARE m int4[] := ARRAY[[1, 2, 3], [4, 5, 6]]; row int4[]; x int4; \
         s int4 := 0; \
         BEGIN FOREACH x SLICE 0 IN ARRAY m LOOP s := s + x; END LOOP; \
         FOREACH row SLICE 1 IN ARRAY m LOOP \
         s := s + coalesce(array_length(row, 1), 0) * 100; END LOOP; \
         RETURN s; END";
    fn_group(&name, "int4", body)
}

// --------------------------------------------------- dynamic EXECUTE ----

/// exec_stmt_dynexecute: EXECUTE INTO STRICT (record) and dynamic DML with
/// GET DIAGNOSTICS ROW_COUNT over a group-local table.
fn gen_dynexec_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:dynexec");
    let n = g.plpg.next_p2();
    let form = g.weights.pick(g.rng, &["plpg2:dyn2:into", "plpg2:dyn2:dml"]);
    g.fire(form);
    if form == "plpg2:dyn2:into" {
        let name = format!("fz_pl2_dy_{n}");
        let a = int_lit(g).abs() % 20 + 1;
        let b = int_lit(g).abs() % 20 + 1;
        let body = format!(
            "DECLARE r record; \
             BEGIN EXECUTE 'SELECT $1::int4 AS x, ($1 * $2)::int4 AS y' \
             INTO STRICT r USING {a}, {b}; RETURN r.x + r.y; END"
        );
        return fn_group(&name, "int4", &body);
    }
    // Dynamic DML with GET DIAGNOSTICS, over a group-local pk table.
    let tbl = format!("fz_pl2_dt_{n}");
    let name = format!("fz_pl2_dm_{n}");
    vec![
        StmtKind::Raw(format!("CREATE TABLE {tbl} (k int4 PRIMARY KEY, v int4);")),
        StmtKind::Raw(format!(
            "INSERT INTO {tbl} VALUES (1, 10), (2, 20), (3, 30);"
        )),
        StmtKind::Raw(format!(
            "CREATE FUNCTION {name}() RETURNS int4 LANGUAGE plpgsql AS $fzp$ \
             DECLARE c int4; BEGIN EXECUTE 'UPDATE {tbl} SET v = v + $1 WHERE k >= $2' \
             USING 5, 2; GET DIAGNOSTICS c = ROW_COUNT; RETURN c; END $fzp$;"
        )),
        StmtKind::Raw(format!("SELECT {name}();")),
        StmtKind::Raw(format!("SELECT k, v FROM {tbl} ORDER BY k;")),
        StmtKind::Raw(format!("DROP FUNCTION {name}();")),
        StmtKind::Raw(format!("DROP TABLE {tbl};")),
    ]
}

// -------------------------------------------------- record / rowtype ----

/// exec_move_row_from_fields / exec_move_row_from_datum + exec_stmt_execsql
/// INTO: SELECT * INTO a %ROWTYPE (Row target, from_fields), a multi-column
/// SELECT INTO a record (Rec target, from_fields), and a composite-datum
/// assignment `rec := r` of the %ROWTYPE value into the record (from_datum).
fn gen_record_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:record");
    let n = g.plpg.next_p2();
    let tbl = format!("fz_pl2_rt_{n}");
    let name = format!("fz_pl2_rf_{n}");
    vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {tbl} (k int4 PRIMARY KEY, a int4, b text);"
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {tbl} VALUES (1, 100, 'x'), (2, 200, 'y');"
        )),
        StmtKind::Raw(format!(
            "CREATE FUNCTION {name}() RETURNS text LANGUAGE plpgsql AS $fzp$ \
             DECLARE r {tbl}%ROWTYPE; rec record; rec2 record; \
             BEGIN SELECT * INTO r FROM {tbl} WHERE k = 1; \
             SELECT k, a, b INTO rec FROM {tbl} WHERE k = 2; \
             rec2 := r; \
             RETURN r.a::text || ':' || rec.a::text || ':' || rec2.b; \
             END $fzp$;"
        )),
        StmtKind::Raw(format!("SELECT {name}();")),
        StmtKind::Raw(format!("DROP FUNCTION {name}();")),
        StmtKind::Raw(format!("DROP TABLE {tbl};")),
    ]
}

// --------------------------------------------------------------- CALL ----

/// make_callstmt_target: CALL of an INOUT procedure with a variable target
/// from inside plpgsql (the variable is rebound from the OUT tuple).
fn gen_call_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:call");
    let n = g.plpg.next_p2();
    let proc = format!("fz_pl2_pc_{n}");
    let name = format!("fz_pl2_cl_{n}");
    vec![
        StmtKind::Raw(format!(
            "CREATE PROCEDURE {proc}(INOUT x int4, IN y int4) LANGUAGE plpgsql \
             AS $fzp$ BEGIN x := x * 10 + y; END $fzp$;"
        )),
        StmtKind::Raw(format!(
            "CREATE FUNCTION {name}() RETURNS int4 LANGUAGE plpgsql AS $fzp$ \
             DECLARE v int4 := 3; BEGIN CALL {proc}(v, 7); CALL {proc}(v, 1); \
             RETURN v; END $fzp$;"
        )),
        StmtKind::Raw(format!("SELECT {name}();")),
        StmtKind::Raw(format!("DROP FUNCTION {name}();")),
        StmtKind::Raw(format!("DROP PROCEDURE {proc}(int4, int4);")),
    ]
}

// ----------------------------------------------------------- DO block ----

/// plpgsql_compile_inline / plpgsql_inline_handler: DO blocks. The block
/// emits an INFO message (client-visible, compared) so the differential run
/// has an observable surface.
fn gen_do_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:do");
    let form = g.weights.pick(g.rng, &["plpg2:do:plain", "plpg2:do:exc"]);
    g.fire(form);
    let raw = if form == "plpg2:do:plain" {
        "DO $fzp$ DECLARE s int4 := 0; BEGIN FOR i IN 1..5 LOOP s := s + i; END LOOP; \
         RAISE INFO 'fz do sum %', s; END $fzp$;"
            .to_string()
    } else {
        "DO $fzp$ BEGIN BEGIN RAISE EXCEPTION 'fz do' USING ERRCODE = '22012'; \
         EXCEPTION WHEN division_by_zero THEN RAISE INFO 'fz do caught %', SQLSTATE; \
         END; END $fzp$;"
            .to_string()
    };
    vec![StmtKind::Raw(raw)]
}

// ---------------------------------------------------------- ASSERT ----

/// exec_stmt_assert: the passing path with a message expression, and the
/// caught assert_failure path.
fn gen_assert_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:assert");
    let n = g.plpg.next_p2();
    let name = format!("fz_pl2_as_{n}");
    let form = g.weights.pick(g.rng, &["plpg2:assert:pass", "plpg2:assert:fail"]);
    g.fire(form);
    let body = if form == "plpg2:assert:pass" {
        "DECLARE s int4 := 0; BEGIN ASSERT 1 = 1; ASSERT s = 0, 'expected zero'; \
         FOR i IN 1..4 LOOP s := s + i; END LOOP; \
         ASSERT s = 10, 'sum should be 10 got ' || s; RETURN s; END"
            .to_string()
    } else {
        "BEGIN ASSERT 1 = 2, 'fz deliberate'; RETURN 0; \
         EXCEPTION WHEN assert_failure THEN RETURN 42; END"
            .to_string()
    };
    fn_group(&name, "int4", &body)
}

// ------------------------------------------------------------- %TYPE ----

/// read_datatype / plpgsql_parse_cwordtype: %TYPE, tab.col%TYPE, %TYPE[],
/// var%TYPE, and CONSTANT NOT NULL DEFAULT declarations.
fn gen_vartype_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg2:vartype");
    let n = g.plpg.next_p2();
    let tbl = format!("fz_pl2_vt_{n}");
    let name = format!("fz_pl2_vf_{n}");
    vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {tbl} (k int4 PRIMARY KEY, a int4, b text);"
        )),
        StmtKind::Raw(format!("INSERT INTO {tbl} VALUES (1, 5, 'p');")),
        StmtKind::Raw(format!(
            "CREATE FUNCTION {name}() RETURNS text LANGUAGE plpgsql AS $fzp$ \
             DECLARE x {tbl}.a%TYPE := 9; y {tbl}.b%TYPE DEFAULT 'z'; \
             arr {tbl}.a%TYPE[] := ARRAY[1, 2, 3]; c CONSTANT int4 NOT NULL := 4; \
             w x%TYPE := x + 1; \
             BEGIN RETURN x::text || y || coalesce(array_length(arr, 1), 0)::text \
             || c::text || w::text; END $fzp$;"
        )),
        StmtKind::Raw(format!("SELECT {name}();")),
        StmtKind::Raw(format!("DROP FUNCTION {name}();")),
        StmtKind::Raw(format!("DROP TABLE {tbl};")),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Every group kind fires under a heavy per-kind bias and produces only
    /// single-line, semicolon-terminated, paren-balanced statements, and
    /// every CREATE has a matching DROP of the same object name (leak-free
    /// self-containment).
    #[test]
    fn all_group_kinds_are_self_contained() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let kinds = [
            "plpg2:raise",
            "plpg2:diag",
            "plpg2:srf",
            "plpg2:cursor",
            "plpg2:foreach",
            "plpg2:dynexec",
            "plpg2:record",
            "plpg2:call",
            "plpg2:do",
            "plpg2:assert",
            "plpg2:vartype",
        ];
        for kind in kinds {
            let spec = format!("{kind}=100");
            let w = WeightTable::parse(&spec).unwrap();
            let mut rng = Rng::new(0x9E37);
            let mut saw = false;
            for _ in 0..200 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                let stmts = gen_plpg2_module(&mut g);
                assert!(!stmts.is_empty(), "{kind} produced nothing");
                if prods.iter().any(|p| p == kind) {
                    saw = true;
                }
                let mut creates = 0usize;
                let mut drops = 0usize;
                for s in &stmts {
                    let sql = s.to_sql();
                    assert!(!sql.contains('\n'), "multi-line: {sql}");
                    assert!(sql.ends_with(';'), "unterminated: {sql}");
                    assert_eq!(
                        sql.matches('(').count(),
                        sql.matches(')').count(),
                        "unbalanced parens: {sql}"
                    );
                    let up = sql.to_ascii_uppercase();
                    if up.starts_with("CREATE FUNCTION")
                        || up.starts_with("CREATE PROCEDURE")
                        || up.starts_with("CREATE TABLE")
                    {
                        creates += 1;
                    }
                    if up.starts_with("DROP ") {
                        drops += 1;
                    }
                }
                assert_eq!(creates, drops, "{kind}: create/drop imbalance");
            }
            assert!(saw, "{kind} never fired under 100x bias");
        }
    }
}
