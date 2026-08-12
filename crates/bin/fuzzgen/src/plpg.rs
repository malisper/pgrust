//! P2: plpgsql depth + trigger depth + collation/operator/opclass DDL.
//!
//! Every group this module emits is self-contained: it creates its objects
//! (functions, procedures, tables, views, triggers, collations, operators,
//! operator classes), exercises them, probes any mutated table state with a
//! pk-total-ordered SELECT inside the same group, and drops everything it
//! created before the group ends. No cross-group object edges, no probe
//! windows, no interaction with the other modules' state models — the
//! validity discipline is entirely local. Name counters live in
//! `PlpgState` so names are session-unique even though objects are
//! group-local (a failed drop can then never collide a later create).
//!
//! Determinism disciplines:
//!   - function bodies are single-line seeded templates over the call
//!     arguments plus catalog reads that are always aggregate or
//!     pk-total-ordered (SELECT INTO without a total order would compare
//!     scan order, not semantics);
//!   - loop bounds are small constants and every WHILE increment is
//!     `greatest(_, 1)`-guarded, so loops terminate by construction;
//!   - deliberate error fuel (division_by_zero via a zero argument, STRICT
//!     NO_DATA_FOUND/TOO_MANY_ROWS, RAISE USING ERRCODE, unique_violation
//!     inside an EXCEPTION block) always errors identically on both sides
//!     — error paths are targets, never hazards;
//!   - trigger bodies mutate NEW deterministically (protected-column
//!     appends/bumps, TG_OP/TG_ARGV concatenations) and row suppression
//!     keys off pk arithmetic, so the group probe pins the exact
//!     post-trigger table image;
//!   - CALL procedures with COMMIT/ROLLBACK inside are only ever emitted
//!     as standalone statements (module groups are never inside txn
//!     brackets), where transaction control in a procedure is legal;
//!   - operator classes stick to the verified-working int4 form (support
//!     function + operators all registered under int4): domain-typed
//!     opclasses with defaulted amproc argtypes are a known A/B-divergent
//!     error surface (see docs/fuzzing/findings-p2-plpgsql.md) and are
//!     deliberately not generated.

use crate::catalog::{SqlType, Table};
use crate::stmt::{Gen, StmtKind};

/// Session-persistent name counters (objects themselves are group-local).
#[derive(Clone, Debug, Default)]
pub struct PlpgState {
    next_fn: u32,
    next_proc: u32,
    next_table: u32,
    next_view: u32,
    next_trig: u32,
    next_coll: u32,
    next_opc: u32,
}

impl PlpgState {
    pub fn new() -> PlpgState {
        PlpgState::default()
    }
}

/// A catalog table usable for deterministic in-function reads: has a real
/// unique pk (total order witness). Returns (table, pk column).
fn pk_table<'a>(g: &mut Gen<'a>) -> Option<(&'a Table, String)> {
    let cands: Vec<&Table> = g
        .catalog
        .tables
        .iter()
        .filter(|t| t.pk_unique && t.pk.is_some())
        .collect();
    if cands.is_empty() {
        return None;
    }
    let t = cands[g.rng.below_usize(cands.len())];
    let pk = t.pk.as_ref().unwrap().column.clone();
    Some((t, pk))
}

/// A catalog table with a pk and a text-family column (collation probes).
fn text_pk_table<'a>(g: &mut Gen<'a>) -> Option<(&'a Table, String, String)> {
    let cands: Vec<(&Table, String)> = g
        .catalog
        .tables
        .iter()
        .filter(|t| t.pk_unique && t.pk.is_some())
        .filter_map(|t| {
            t.columns
                .iter()
                .find(|c| c.ty.is_text_family())
                .map(|c| (t, c.name.clone()))
        })
        .collect();
    if cands.is_empty() {
        return None;
    }
    let (t, col) = cands[g.rng.below_usize(cands.len())].clone();
    let pk = t.pk.as_ref().unwrap().column.clone();
    Some((t, col, pk))
}

/// Boundary-heavy int argument literal (0 is deliberate division fuel).
fn int_arg(g: &mut Gen) -> String {
    match g.rng.below(8) {
        0 => "0".to_string(),
        1 => "1".to_string(),
        2 => "-1".to_string(),
        3 => "7".to_string(),
        4 => "2147483647".to_string(),
        5 => "-2147483648".to_string(),
        _ => g.rng.range_i64(-100, 100).to_string(),
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_plpg_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg");
    let action = g.weights.pick(
        g.rng,
        &[
            "plpg:fn",
            "plpg:out",
            "plpg:srf",
            "plpg:proc",
            "plpg:trig",
            "plpg:instead",
            "plpg:coll",
            "plpg:op",
            "plpg:opclass",
        ],
    );
    match action {
        "plpg:fn" => gen_fn_group(g),
        "plpg:out" => gen_out_group(g),
        "plpg:srf" => gen_srf_group(g),
        "plpg:proc" => gen_proc_group(g),
        "plpg:trig" => gen_trig_group(g),
        "plpg:instead" => gen_instead_group(g),
        "plpg:coll" => gen_coll_group(g),
        "plpg:op" => gen_op_group(g),
        _ => gen_opclass_group(g),
    }
}

// -------------------------------------------------- scalar functions ----

/// CREATE FUNCTION (seeded body template) + 2 calls + DROP FUNCTION.
fn gen_fn_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:fn");
    let name = format!("fz_plf_{}", g.plpg.next_fn);
    g.plpg.next_fn += 1;
    let tpl = g.weights.pick(
        g.rng,
        &[
            "plpg:fn:ctrl",
            "plpg:fn:loop",
            "plpg:fn:strict",
            "plpg:fn:exc",
            "plpg:fn:dyn",
            "plpg:fn:diag",
        ],
    );
    g.fire(tpl);
    let (ret, body) = match tpl {
        "plpg:fn:ctrl" => body_ctrl(g, &name),
        "plpg:fn:loop" => body_loop(g),
        "plpg:fn:strict" => body_strict(g),
        "plpg:fn:exc" => body_exc(g),
        "plpg:fn:dyn" => body_dyn(g),
        _ => body_diag(g),
    };
    let mut out = vec![StmtKind::Raw(format!(
        "CREATE FUNCTION {}(a int4, b int4) RETURNS {} LANGUAGE plpgsql AS $fzp$ {} $fzp$;",
        name, ret, body
    ))];
    for _ in 0..2 {
        let (x, y) = (int_arg(g), int_arg(g));
        out.push(StmtKind::Raw(format!("SELECT {}({}, {});", name, x, y)));
    }
    if g.rng.chance(1, 6) {
        g.fire("plpg:fn:deparse");
        out.push(StmtKind::Raw(format!(
            "SELECT pg_get_functiondef('{}(int4,int4)'::regprocedure);",
            name
        )));
    }
    out.push(StmtKind::Raw(format!("DROP FUNCTION {}(int4, int4);", name)));
    out
}

/// IF/ELSIF chains, CASE statement (simple + searched), CASE expression,
/// CONSTANT + %TYPE declarations, nested block with variable shadowing.
fn body_ctrl(g: &mut Gen, fname: &str) -> (&'static str, String) {
    let k = g.rng.range_i64(1, 9);
    let m = g.rng.range_i64(2, 5);
    (
        "int4",
        format!(
            "DECLARE r int4 := 0; c CONSTANT int4 := {k}; w {fname}.a%TYPE := a; \
             BEGIN IF a > b THEN r := c + 1; ELSIF a = b THEN r := c + 2; \
             ELSIF a IS NULL THEN r := -c; ELSE r := c - 3; END IF; \
             CASE r % {m} WHEN 0, 1 THEN r := r + 10; WHEN 2 THEN r := r - 10; \
             ELSE r := r * 2; END CASE; \
             CASE WHEN w > 50 THEN w := 50; WHEN w < -50 THEN w := -50; ELSE w := w; END CASE; \
             DECLARE w int4 := 1000; BEGIN r := r + w / 100; END; \
             RETURN r + (CASE WHEN w >= 0 THEN w ELSE -w END); END"
        ),
    )
}

/// FOR (BY / REVERSE), WHILE, FOREACH over an array, labelled
/// EXIT/CONTINUE. Bounds are small constants; the WHILE increment is
/// `greatest(_, 1)`-guarded so termination is by construction.
fn body_loop(g: &mut Gen) -> (&'static str, String) {
    let hi = g.rng.range_i64(3, 12);
    let by = g.rng.range_i64(1, 3);
    let skip = g.rng.range_i64(1, 5);
    let cap = g.rng.range_i64(30, 60);
    (
        "int4",
        format!(
            "DECLARE s int4 := 0; i int4; x int4; \
             BEGIN <<l1>> FOR i IN 1..{hi} BY {by} LOOP \
             CONTINUE l1 WHEN i = {skip}; s := s + i; \
             EXIT l1 WHEN s > {cap}; END LOOP; \
             FOR i IN REVERSE {hi}..1 LOOP s := s + 1; END LOOP; \
             <<l2>> WHILE s < {cap} LOOP s := s + greatest(coalesce(b, 1), 1); \
             EXIT l2 WHEN s > 2000000; END LOOP; \
             FOREACH x IN ARRAY ARRAY[a, b, {skip}, NULL] LOOP \
             s := s + coalesce(x / 8, -1); END LOOP; \
             LOOP s := s - 1; EXIT WHEN s % 2 = 0; END LOOP; \
             RETURN s; END"
        ),
    )
}

/// SELECT INTO [STRICT] with NO_DATA_FOUND / TOO_MANY_ROWS handlers.
/// Non-strict reads are pk-total-ordered with LIMIT 1 (scan order must
/// never be the compared surface).
fn body_strict(g: &mut Gen) -> (&'static str, String) {
    let Some((t, pk)) = pk_table(g) else {
        return body_ctrl(g, "fz");
    };
    let tname = t.name.clone();
    let lit = g.rng.range_i64(-5, 30);
    let form = g.weights.pick(
        g.rng,
        &["plpg:strict:pk", "plpg:strict:many", "plpg:strict:ordered"],
    );
    g.fire(form);
    let body = match form {
        // Unique-pk lookup: hits or NO_DATA_FOUND depending on the seed row.
        "plpg:strict:pk" => format!(
            "DECLARE v int4; BEGIN SELECT ({pk})::int4 INTO STRICT v FROM {tname} \
             WHERE {pk} = {lit}; RETURN v; \
             EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1; \
             WHEN TOO_MANY_ROWS THEN RETURN -2; END"
        ),
        // Unfiltered STRICT: TOO_MANY_ROWS on multi-row tables (deliberate).
        "plpg:strict:many" => format!(
            "DECLARE v int4; BEGIN SELECT 1 INTO STRICT v FROM {tname}; RETURN v; \
             EXCEPTION WHEN NO_DATA_FOUND THEN RETURN -1; \
             WHEN TOO_MANY_ROWS THEN RETURN -2; END"
        ),
        // Non-strict, pk-total-ordered; NULL (empty table) coalesces.
        _ => format!(
            "DECLARE v int4; BEGIN SELECT ({pk})::int4 INTO v FROM {tname} \
             WHERE {pk} > {lit} ORDER BY {pk} LIMIT 1; \
             RETURN coalesce(v, -9); END"
        ),
    };
    ("int4", body)
}

/// EXCEPTION depth: division_by_zero, RAISE ... USING ERRCODE caught by a
/// named condition / SQLSTATE literal / OTHERS, SQLSTATE + SQLERRM probes,
/// nested BEGIN blocks re-raising outward.
fn body_exc(g: &mut Gen) -> (&'static str, String) {
    let thr = g.rng.range_i64(50, 500);
    let form = g.weights.pick(g.rng, &["plpg:exc:flat", "plpg:exc:nested"]);
    g.fire(form);
    let body = if form == "plpg:exc:flat" {
        format!(
            "DECLARE v int4; BEGIN v := a / b; \
             IF v > {thr} THEN RAISE EXCEPTION 'fz big %', v USING ERRCODE = 'P7001'; END IF; \
             RETURN v::text; \
             EXCEPTION WHEN division_by_zero THEN RETURN 'D:' || SQLSTATE; \
             WHEN SQLSTATE 'P7001' THEN RETURN 'R:' || SQLERRM; \
             WHEN OTHERS THEN RETURN 'O:' || SQLSTATE || ':' || SQLERRM; END"
        )
    } else {
        // Inner handler catches div0, converts to unique_violation; the
        // outer handler catches that by name.
        format!(
            "DECLARE v int4 := 0; BEGIN BEGIN v := a / b; \
             EXCEPTION WHEN division_by_zero THEN \
             RAISE EXCEPTION 'fz dup' USING ERRCODE = 'unique_violation'; END; \
             RETURN 'ok:' || v::text; \
             EXCEPTION WHEN unique_violation THEN RETURN 'U:' || SQLSTATE || ':' || SQLERRM; END"
        )
    };
    ("text", body)
}

/// Dynamic SQL: EXECUTE format(...) INTO ... USING, %s and %I directives.
fn body_dyn(g: &mut Gen) -> (&'static str, String) {
    let k = g.rng.range_i64(1, 50);
    let body = match pk_table(g) {
        Some((t, pk)) if g.rng.chance(1, 2) => {
            g.fire("plpg:dyn:table");
            let tname = t.name.clone();
            format!(
                "DECLARE v int4; BEGIN \
                 EXECUTE format('SELECT count(*)::int4 FROM %I WHERE %I > $1', \
                 '{tname}', '{pk}') INTO v USING a; \
                 RETURN coalesce(v, -1) + {k}; END"
            )
        }
        _ => {
            g.fire("plpg:dyn:scalar");
            format!(
                "DECLARE v int4; BEGIN EXECUTE format('SELECT $1 + $2 + %s', {k}) \
                 INTO STRICT v USING a, b; RETURN v; END"
            )
        }
    };
    ("int4", body)
}

/// GET DIAGNOSTICS ROW_COUNT after PERFORM (deterministic: both sides hold
/// identical table state at every stream point).
fn body_diag(g: &mut Gen) -> (&'static str, String) {
    let Some((t, pk)) = pk_table(g) else {
        return body_ctrl(g, "fz");
    };
    let tname = t.name.clone();
    let lit = g.rng.range_i64(-5, 30);
    (
        "int4",
        format!(
            "DECLARE n1 int4; n2 int4; BEGIN PERFORM 1 FROM {tname} WHERE {pk} > {lit}; \
             GET DIAGNOSTICS n1 = ROW_COUNT; \
             PERFORM 1 FROM {tname} WHERE {pk} = {lit}; \
             GET DIAGNOSTICS n2 = ROW_COUNT; RETURN n1 * 100 + n2; END"
        ),
    )
}

// ------------------------------------------------------ OUT params ----

/// OUT/INOUT parameters, called with SELECT * (record output).
fn gen_out_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:out");
    let name = format!("fz_plf_{}", g.plpg.next_fn);
    g.plpg.next_fn += 1;
    let create = format!(
        "CREATE FUNCTION {}(a int4, OUT s int4, OUT d text, INOUT m int4) \
         LANGUAGE plpgsql AS $fzp$ BEGIN s := a + coalesce(m, 0); \
         d := 'fz' || coalesce(a, -1)::text; m := coalesce(m, 0) * 2; END $fzp$;",
        name
    );
    let (x, y) = (int_arg(g), int_arg(g));
    vec![
        StmtKind::Raw(create),
        StmtKind::Raw(format!("SELECT * FROM {}({}, {});", name, x, y)),
        StmtKind::Raw(format!("DROP FUNCTION {}(int4, int4);", name)),
    ]
}

// ------------------------------------------------------------- SRFs ----

/// RETURN NEXT / RETURN QUERY set-returning functions called in FROM with
/// a total ORDER BY over the output columns.
fn gen_srf_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:srf");
    let name = format!("fz_plf_{}", g.plpg.next_fn);
    g.plpg.next_fn += 1;
    let form = g.weights.pick(g.rng, &["plpg:srf:setof", "plpg:srf:table"]);
    g.fire(form);
    let n = g.rng.range_i64(2, 6);
    let mut out = Vec::new();
    if form == "plpg:srf:setof" {
        let query_leg = match pk_table(g) {
            Some((t, pk)) => format!(
                " IF b > 0 THEN RETURN QUERY SELECT ({pk})::int4 FROM {} ORDER BY {pk}; END IF;",
                t.name
            ),
            None => String::new(),
        };
        out.push(StmtKind::Raw(format!(
            "CREATE FUNCTION {}(a int4, b int4) RETURNS SETOF int4 LANGUAGE plpgsql \
             AS $fzp$ DECLARE i int4; BEGIN FOR i IN 1..{} LOOP \
             RETURN NEXT i * coalesce(a, 1);{} END LOOP;{} RETURN; END $fzp$;",
            name,
            n,
            " CONTINUE WHEN i = 2;",
            query_leg
        )));
        let (x, y) = (int_arg(g), int_arg(g));
        out.push(StmtKind::Raw(format!(
            "SELECT * FROM {}({}, {}) ORDER BY 1;",
            name, x, y
        )));
        out.push(StmtKind::Raw(format!(
            "SELECT count(*), coalesce(sum(v), 0) FROM {}({}, {}) v;",
            name, x, y
        )));
    } else {
        // RETURNS TABLE with RETURN NEXT assignments + a RETURN QUERY leg
        // (qualified columns: output names shadow unqualified references).
        let query_leg = match pk_table(g) {
            Some((t, pk)) => format!(
                " RETURN QUERY SELECT (s.{pk})::int4, (s.{pk})::text FROM {} s \
                 ORDER BY s.{pk} LIMIT {};",
                t.name, n
            ),
            None => String::new(),
        };
        out.push(StmtKind::Raw(format!(
            "CREATE FUNCTION {}(a int4, b int4) RETURNS TABLE(o1 int4, o2 text) \
             LANGUAGE plpgsql AS $fzp$ DECLARE i int4; BEGIN FOR i IN 1..{} LOOP \
             o1 := i + coalesce(a / 4, 0); o2 := 'r' || i::text; RETURN NEXT; \
             END LOOP;{} RETURN; END $fzp$;",
            name, n, query_leg
        )));
        let (x, y) = (int_arg(g), int_arg(g));
        out.push(StmtKind::Raw(format!(
            "SELECT * FROM {}({}, {}) ORDER BY 1, 2;",
            name, x, y
        )));
    }
    out.push(StmtKind::Raw(format!("DROP FUNCTION {}(int4, int4);", name)));
    out
}

// ------------------------------------------------------- procedures ----

/// CREATE PROCEDURE + CALL. The txn-control variant COMMITs and ROLLBACKs
/// inside the procedure over a group-local table, then probes it ordered.
fn gen_proc_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:proc");
    let name = format!("fz_plp_{}", g.plpg.next_proc);
    g.plpg.next_proc += 1;
    let form = g.weights.pick(g.rng, &["plpg:proc:inout", "plpg:proc:txn"]);
    g.fire(form);
    if form == "plpg:proc:inout" {
        let (x, y) = (int_arg(g), int_arg(g));
        return vec![
            StmtKind::Raw(format!(
                "CREATE PROCEDURE {}(a int4, INOUT r int4) LANGUAGE plpgsql \
                 AS $fzp$ BEGIN r := coalesce(r, 0) + coalesce(a, -1) * 2; \
                 IF r > 1000 THEN r := 1000; END IF; END $fzp$;",
                name
            )),
            StmtKind::Raw(format!("CALL {}({}, {});", name, x, y)),
            StmtKind::Raw(format!("DROP PROCEDURE {}(int4, int4);", name)),
        ];
    }
    // Transaction control inside a procedure: legal because module groups
    // are standalone statements, never inside txn-module brackets.
    let tname = format!("fz_plt_{}", g.plpg.next_table);
    g.plpg.next_table += 1;
    let (v1, v2, v3) = (
        g.rng.range_i64(0, 99),
        g.rng.range_i64(0, 99),
        g.rng.range_i64(0, 99),
    );
    vec![
        StmtKind::Raw(format!("CREATE TABLE {} (pk int4 PRIMARY KEY, v int4);", tname)),
        StmtKind::Raw(format!(
            "CREATE PROCEDURE {}() LANGUAGE plpgsql AS $fzp$ BEGIN \
             INSERT INTO {} VALUES (1, {v1}); COMMIT; \
             INSERT INTO {} VALUES (2, {v2}); ROLLBACK; \
             INSERT INTO {} VALUES (3, {v3}); END $fzp$;",
            name, tname, tname, tname
        )),
        StmtKind::Raw(format!("CALL {}();", name)),
        StmtKind::Raw(format!("SELECT pk, v FROM {} ORDER BY pk;", tname)),
        StmtKind::Raw(format!("DROP TABLE {};", tname)),
        StmtKind::Raw(format!("DROP PROCEDURE {}();", name)),
    ]
}

// ---------------------------------------------------- trigger depth ----

/// Trigger-depth group over a fresh group-local table: NEW/OLD mutation,
/// row suppression, TG_OP/TG_ARGV, WHEN conditions, transition tables,
/// deferred constraint triggers, alphabetical firing order — then DML, a
/// pk-total-ordered probe of the exact post-trigger image, and drops.
fn gen_trig_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:trig");
    let tname = format!("fz_plt_{}", g.plpg.next_table);
    g.plpg.next_table += 1;
    let n = g.plpg.next_trig;
    g.plpg.next_trig += 1;
    let mut out = vec![StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, k_int int4, k_text text);",
        tname
    ))];
    let mut fns: Vec<String> = Vec::new();

    // First row trigger: BEFORE mutation with TG_OP and (optionally)
    // TG_ARGV, alphabetically first (name suffix 'a').
    let fa = format!("fz_ptf_{}a", n);
    let use_args = g.rng.chance(1, 2);
    if use_args {
        g.fire("plpg:trig:args");
    } else {
        g.fire("plpg:trig:tgop");
    }
    let (body_a, args_a) = if use_args {
        (
            "BEGIN NEW.k_text := coalesce(NEW.k_text, '') || TG_OP || TG_ARGV[0]; \
             NEW.k_int := coalesce(NEW.k_int, 0) + TG_ARGV[1]::int4; RETURN NEW; END"
                .to_string(),
            format!("('x{}', '{}')", n, g.rng.range_i64(1, 9)),
        )
    } else {
        (
            "BEGIN NEW.k_text := coalesce(NEW.k_text, '') || left(TG_OP, 1); \
             NEW.k_int := coalesce(NEW.k_int, 0) + 1; RETURN NEW; END"
                .to_string(),
            "()".to_string(),
        )
    };
    let when_a = if g.rng.chance(1, 3) {
        g.fire("plpg:trig:when");
        " WHEN (NEW.pk <> 4)"
    } else {
        ""
    };
    out.push(StmtKind::Raw(format!(
        "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzp$ {} $fzp$;",
        fa, body_a
    )));
    out.push(StmtKind::Raw(format!(
        "CREATE TRIGGER fz_ptg_{}a BEFORE INSERT OR UPDATE ON {} FOR EACH ROW{} \
         EXECUTE FUNCTION {}{};",
        n, tname, when_a, fa, args_a
    )));
    fns.push(fa);

    // Second row trigger (fires after 'a' alphabetically): suppression by
    // pk arithmetic, observable in the probe as missing rows.
    if g.rng.chance(2, 3) {
        g.fire("plpg:trig:suppress");
        let fb = format!("fz_ptf_{}b", n);
        let m = 2 + g.rng.below(3); // 2..4
        out.push(StmtKind::Raw(format!(
            "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzp$ \
             BEGIN IF NEW.pk % {} = 0 THEN RETURN NULL; END IF; \
             NEW.k_text := coalesce(NEW.k_text, '') || 'b'; RETURN NEW; END $fzp$;",
            fb, m
        )));
        out.push(StmtKind::Raw(format!(
            "CREATE TRIGGER fz_ptg_{}b BEFORE INSERT ON {} FOR EACH ROW \
             EXECUTE FUNCTION {}();",
            n, tname, fb
        )));
        fns.push(fb);
    }

    // BEFORE DELETE with a WHEN over OLD: suppresses some deletes.
    if g.rng.chance(1, 2) {
        g.fire("plpg:trig:delete");
        let fd = format!("fz_ptf_{}d", n);
        out.push(StmtKind::Raw(format!(
            "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzp$ \
             BEGIN IF OLD.pk = 2 THEN RETURN NULL; END IF; RETURN OLD; END $fzp$;",
            fd
        )));
        out.push(StmtKind::Raw(format!(
            "CREATE TRIGGER fz_ptg_{}d BEFORE DELETE ON {} FOR EACH ROW \
             WHEN (OLD.k_int IS NOT NULL) EXECUTE FUNCTION {}();",
            n, tname, fd
        )));
        fns.push(fd);
    }

    // AFTER STATEMENT trigger reading a transition table.
    if g.rng.chance(1, 2) {
        g.fire("plpg:trig:transition");
        let ft = format!("fz_ptf_{}t", n);
        let (event, refclause, reltab) = match g.rng.below(3) {
            0 => ("INSERT", "REFERENCING NEW TABLE AS fz_nt", "fz_nt"),
            1 => ("UPDATE", "REFERENCING OLD TABLE AS fz_ot NEW TABLE AS fz_nt", "fz_nt"),
            _ => ("DELETE", "REFERENCING OLD TABLE AS fz_ot", "fz_ot"),
        };
        out.push(StmtKind::Raw(format!(
            "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzp$ \
             DECLARE c int4; BEGIN SELECT count(*)::int4 INTO c FROM {}; \
             IF c < 0 THEN RAISE EXCEPTION 'fz neg'; END IF; RETURN NULL; END $fzp$;",
            ft, reltab
        )));
        out.push(StmtKind::Raw(format!(
            "CREATE TRIGGER fz_ptg_{}t AFTER {} ON {} {} FOR EACH STATEMENT \
             EXECUTE FUNCTION {}();",
            n, event, tname, refclause, ft
        )));
        fns.push(ft);
    }

    // Deferred constraint trigger (fires at statement-commit time).
    if g.rng.chance(1, 3) {
        g.fire("plpg:trig:constraint");
        let fc = format!("fz_ptf_{}c", n);
        out.push(StmtKind::Raw(format!(
            "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzp$ \
             BEGIN RETURN NULL; END $fzp$;",
            fc
        )));
        out.push(StmtKind::Raw(format!(
            "CREATE CONSTRAINT TRIGGER fz_ptg_{}c AFTER UPDATE ON {} \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {}();",
            n, tname, fc
        )));
        fns.push(fc);
    }

    // DML through the trigger stack.
    let base = g.rng.range_i64(0, 40);
    out.push(StmtKind::Raw(format!(
        "INSERT INTO {} VALUES (1, {}, 'a'), (2, {}, NULL), (3, NULL, 'c'), (4, 0, ''), (6, 6, 'f');",
        tname,
        base,
        base + 1
    )));
    out.push(StmtKind::Raw(format!(
        "UPDATE {} SET k_int = k_int + 10 WHERE pk <= 3;",
        tname
    )));
    out.push(StmtKind::Raw(format!("DELETE FROM {} WHERE pk IN (2, 6);", tname)));
    // unique_violation fuel inside an EXCEPTION block (whole subxact rolls
    // back, so the probe image is unaffected).
    if g.rng.chance(1, 3) {
        g.fire("plpg:trig:uniqfuel");
        let fu = format!("fz_ptf_{}u", n);
        out.push(StmtKind::Raw(format!(
            "CREATE FUNCTION {}() RETURNS text LANGUAGE plpgsql AS $fzp$ \
             BEGIN INSERT INTO {} VALUES (999, 0, 'u'); \
             INSERT INTO {} VALUES (999, 0, 'u'); RETURN 'no'; \
             EXCEPTION WHEN unique_violation THEN RETURN SQLSTATE; END $fzp$;",
            fu, tname, tname
        )));
        out.push(StmtKind::Raw(format!("SELECT {}();", fu)));
        fns.push(fu);
    }
    // Deparse probe + the pk-total-ordered state probe.
    if g.rng.chance(1, 3) {
        g.fire("plpg:trig:deparse");
        out.push(StmtKind::Raw(format!(
            "SELECT pg_get_triggerdef(oid) FROM pg_trigger \
             WHERE tgname = 'fz_ptg_{}a' AND NOT tgisinternal;",
            n
        )));
    }
    out.push(StmtKind::Raw(format!(
        "SELECT pk, k_int, k_text FROM {} ORDER BY pk;",
        tname
    )));
    out.push(StmtKind::Raw(format!("DROP TABLE {};", tname)));
    for f in fns {
        let sig = if f.ends_with('u') { "" } else { "()" };
        out.push(StmtKind::Raw(format!("DROP FUNCTION {}{};", f, sig)));
    }
    out
}

/// INSTEAD OF triggers on a group-local view: TG_OP-dispatched DML onto
/// the base table, exercised through INSERT/UPDATE/DELETE on the view.
fn gen_instead_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:instead");
    let tname = format!("fz_plt_{}", g.plpg.next_table);
    g.plpg.next_table += 1;
    let vname = format!("fz_plv_{}", g.plpg.next_view);
    g.plpg.next_view += 1;
    let n = g.plpg.next_trig;
    g.plpg.next_trig += 1;
    let fi = format!("fz_ptf_{}i", n);
    let off = g.rng.range_i64(100, 200);
    vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {} (pk int4 PRIMARY KEY, k_int int4, k_text text);",
            tname
        )),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');",
            tname
        )),
        StmtKind::Raw(format!(
            "CREATE VIEW {} AS SELECT pk, k_int FROM {} WHERE pk < 1000;",
            vname, tname
        )),
        StmtKind::Raw(format!(
            "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $fzp$ BEGIN \
             IF TG_OP = 'INSERT' THEN INSERT INTO {} VALUES (NEW.pk + {off}, NEW.k_int, 'via'); \
             RETURN NEW; ELSIF TG_OP = 'UPDATE' THEN UPDATE {} SET k_int = NEW.k_int \
             WHERE pk = OLD.pk; RETURN NEW; ELSE DELETE FROM {} WHERE pk = OLD.pk; \
             RETURN OLD; END IF; END $fzp$;",
            fi, tname, tname, tname
        )),
        StmtKind::Raw(format!(
            "CREATE TRIGGER fz_ptg_{}i INSTEAD OF INSERT OR UPDATE OR DELETE ON {} \
             FOR EACH ROW EXECUTE FUNCTION {}();",
            n, vname, fi
        )),
        StmtKind::Raw(format!("INSERT INTO {} VALUES (7, 70);", vname)),
        StmtKind::Raw(format!(
            "UPDATE {} SET k_int = k_int + 5 WHERE pk = 2;",
            vname
        )),
        StmtKind::Raw(format!("DELETE FROM {} WHERE pk = 3;", vname)),
        StmtKind::Raw(format!("SELECT pk, k_int FROM {} ORDER BY pk;", vname)),
        StmtKind::Raw(format!(
            "SELECT pk, k_int, k_text FROM {} ORDER BY pk;",
            tname
        )),
        StmtKind::Raw(format!("DROP VIEW {};", vname)),
        StmtKind::Raw(format!("DROP TABLE {};", tname)),
        StmtKind::Raw(format!("DROP FUNCTION {}();", fi)),
    ]
}

// -------------------------------------------------------- collations ----

/// CREATE COLLATION (deterministic libc C/POSIX forms only — no ICU),
/// COLLATE in expressions / ORDER BY / column DDL, pg_collation probe.
fn gen_coll_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:coll");
    let cname = format!("fz_coll_{}", g.plpg.next_coll);
    g.plpg.next_coll += 1;
    let form = g.weights.pick(
        g.rng,
        &["plpg:coll:locale", "plpg:coll:from", "plpg:coll:lc_pair"],
    );
    g.fire(form);
    let create = match form {
        "plpg:coll:from" => {
            let src = if g.rng.chance(1, 2) { "\"C\"" } else { "\"POSIX\"" };
            format!("CREATE COLLATION {} FROM {};", cname, src)
        }
        "plpg:coll:lc_pair" => format!(
            "CREATE COLLATION {} (lc_collate = 'C', lc_ctype = 'C');",
            cname
        ),
        _ => {
            let loc = if g.rng.chance(1, 2) { "C" } else { "POSIX" };
            format!("CREATE COLLATION {} (locale = '{}');", cname, loc)
        }
    };
    let mut out = vec![StmtKind::Raw(create)];
    let (l1, l2) = (
        g.gen_literal(SqlType::Text),
        g.gen_literal(SqlType::Text),
    );
    out.push(StmtKind::Raw(format!(
        "SELECT {} < {} COLLATE {}, ({} COLLATE {}) >= {};",
        l1, l2, cname, l2, cname, l1
    )));
    if let Some((t, col, pk)) = text_pk_table(g) {
        g.fire("plpg:coll:orderby");
        out.push(StmtKind::Raw(format!(
            "SELECT {} FROM {} ORDER BY {} COLLATE {}, {};",
            col, t.name, col, cname, pk
        )));
    }
    if g.rng.chance(1, 2) {
        g.fire("plpg:coll:coldef");
        let tname = format!("fz_plt_{}", g.plpg.next_table);
        g.plpg.next_table += 1;
        let (a, b, c) = (
            g.gen_literal(SqlType::Text),
            g.gen_literal(SqlType::Text),
            g.gen_literal(SqlType::Text),
        );
        out.push(StmtKind::Raw(format!(
            "CREATE TABLE {} (pk int4 PRIMARY KEY, t text COLLATE {});",
            tname, cname
        )));
        out.push(StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (1, {}), (2, {}), (3, {});",
            tname, a, b, c
        )));
        out.push(StmtKind::Raw(format!(
            "SELECT pk, t FROM {} ORDER BY t, pk;",
            tname
        )));
        out.push(StmtKind::Raw(format!("DROP TABLE {};", tname)));
    }
    out.push(StmtKind::Raw(format!(
        "SELECT collname, collprovider::text, collencoding, collisdeterministic \
         FROM pg_collation WHERE collname = '{}';",
        cname
    )));
    out.push(StmtKind::Raw(format!("DROP COLLATION {};", cname)));
    out
}

// --------------------------------------------------------- operators ----

/// Custom-symbol operator names: distinct from every builtin, reused
/// freely because each group drops its operator before ending.
const OP_SYMBOLS: &[&str] = &["<#>", "|#|", "@#@", "&#&", "~#~"];

/// (proc name, commutative) — commutative sfuncs may name themselves as
/// COMMUTATOR.
const OP_FUNCS: &[(&str, bool)] = &[
    ("int4pl", true),
    ("int4mi", false),
    ("int4mul", true),
    ("int4smaller", true),
    ("int4larger", true),
];

/// CREATE OPERATOR over int4 wrappers + uses + DROP OPERATOR.
fn gen_op_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:op");
    let sym = OP_SYMBOLS[g.rng.below_usize(OP_SYMBOLS.len())];
    let (func, commutes) = OP_FUNCS[g.rng.below_usize(OP_FUNCS.len())];
    let commutator = if commutes && g.rng.chance(1, 2) {
        g.fire("plpg:op:commutator");
        format!(", COMMUTATOR = {}", sym)
    } else {
        String::new()
    };
    let (x, y) = (g.rng.range_i64(-40, 40), g.rng.range_i64(-40, 40));
    vec![
        StmtKind::Raw(format!(
            "CREATE OPERATOR {} (LEFTARG = int4, RIGHTARG = int4, FUNCTION = {}{});",
            sym, func, commutator
        )),
        StmtKind::Raw(format!(
            "SELECT {} {} {}, ({} {} {}) {} 2;",
            x, sym, y, y, sym, x, sym
        )),
        StmtKind::Raw(format!(
            "SELECT oprname, oprkind::text FROM pg_operator WHERE oprname = '{}' \
             AND oprleft = 'int4'::regtype ORDER BY oprname;",
            sym
        )),
        StmtKind::Raw(format!("DROP OPERATOR {} (int4, int4);", sym)),
    ]
}

// --------------------------------------------- operator classes/families

/// CREATE OPERATOR CLASS/FAMILY in the verified-working int4 form: every
/// operator and the support function registered under int4, an index built
/// with the explicit opclass, seqscan-off probes through it, REINDEX, and
/// full drops.
fn gen_opclass_group(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("plpg:opclass");
    let id = g.plpg.next_opc;
    g.plpg.next_opc += 1;
    let opc = format!("fz_opc_{}", id);
    let tname = format!("fz_plt_{}", g.plpg.next_table);
    g.plpg.next_table += 1;
    let idx = format!("fz_pli_{}", id);
    let am_btree = g.weights.pick(g.rng, &["plpg:opclass:btree", "plpg:opclass:hash"])
        == "plpg:opclass:btree";
    g.fire(if am_btree { "plpg:opclass:btree" } else { "plpg:opclass:hash" });
    let mut out = Vec::new();
    let mut fam: Option<String> = None;
    if am_btree && g.rng.chance(1, 2) {
        g.fire("plpg:opclass:family");
        let f = format!("fz_opf_{}", id);
        out.push(StmtKind::Raw(format!(
            "CREATE OPERATOR FAMILY {} USING btree;",
            f
        )));
        fam = Some(f);
    }
    if am_btree {
        let famclause = fam
            .as_ref()
            .map(|f| format!(" FAMILY {}", f))
            .unwrap_or_default();
        out.push(StmtKind::Raw(format!(
            "CREATE OPERATOR CLASS {} FOR TYPE int4 USING btree{} AS \
             OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >, \
             FUNCTION 1 btint4cmp;",
            opc, famclause
        )));
    } else {
        out.push(StmtKind::Raw(format!(
            "CREATE OPERATOR CLASS {} FOR TYPE int4 USING hash AS \
             OPERATOR 1 =, FUNCTION 1 hashint4;",
            opc
        )));
    }
    out.push(StmtKind::Raw(format!(
        "CREATE TABLE {} (pk int4 PRIMARY KEY, c int4);",
        tname
    )));
    out.push(StmtKind::Raw(format!(
        "CREATE INDEX {} ON {} USING {} (c {});",
        idx,
        tname,
        if am_btree { "btree" } else { "hash" },
        opc
    )));
    let base = g.rng.range_i64(-20, 20);
    out.push(StmtKind::Raw(format!(
        "INSERT INTO {} VALUES (1, {}), (2, {}), (3, {}), (4, NULL), (5, {});",
        tname,
        base + 5,
        base,
        base + 9,
        base
    )));
    out.push(StmtKind::Raw("SET enable_seqscan TO off;".to_string()));
    if am_btree {
        out.push(StmtKind::Raw(format!(
            "SELECT pk, c FROM {} WHERE c > {} ORDER BY c, pk;",
            tname, base
        )));
    }
    out.push(StmtKind::Raw(format!(
        "SELECT pk FROM {} WHERE c = {} ORDER BY pk;",
        tname, base
    )));
    out.push(StmtKind::Raw("RESET enable_seqscan;".to_string()));
    if g.rng.chance(1, 2) {
        g.fire("plpg:opclass:reindex");
        out.push(StmtKind::Raw(format!("REINDEX INDEX {};", idx)));
    }
    out.push(StmtKind::Raw(format!(
        "SELECT opcname FROM pg_opclass WHERE opcname = '{}';",
        opc
    )));
    out.push(StmtKind::Raw(format!("DROP TABLE {};", tname)));
    out.push(StmtKind::Raw(format!(
        "DROP OPERATOR CLASS {} USING {};",
        opc,
        if am_btree { "btree" } else { "hash" }
    )));
    if let Some(f) = fam {
        out.push(StmtKind::Raw(format!("DROP OPERATOR FAMILY {} USING btree;", f)));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut state = PlpgState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.plpg, &mut state);
            let kinds = gen_plpg_module(&mut g);
            std::mem::swap(&mut g.plpg, &mut state);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods)
    }

    #[test]
    fn plpg_is_deterministic() {
        let (a, _) = gen_groups(11, 300, "");
        let (b, _) = gen_groups(11, 300, "");
        assert_eq!(a, b);
        let (c, _) = gen_groups(12, 300, "");
        assert_ne!(a, c);
    }

    #[test]
    fn plpg_variety_and_shape() {
        let (groups, prods) = gen_groups(0x9219, 900, "");
        let all: Vec<String> = groups.iter().flatten().cloned().collect();
        let hay = all.join("\n");
        for frag in [
            "CREATE FUNCTION fz_plf_",
            "LANGUAGE plpgsql",
            "ELSIF",
            "END CASE",
            "%TYPE",
            "CONSTANT",
            "FOR i IN 1..",
            " REVERSE ",
            "FOREACH x IN ARRAY",
            "CONTINUE l1 WHEN",
            "EXIT l1 WHEN",
            "INTO STRICT",
            "NO_DATA_FOUND",
            "TOO_MANY_ROWS",
            "division_by_zero",
            "SQLERRM",
            "USING ERRCODE",
            "GET DIAGNOSTICS",
            "EXECUTE format(",
            "RETURN NEXT",
            "RETURN QUERY",
            "RETURNS SETOF int4",
            "RETURNS TABLE(o1 int4, o2 text)",
            "OUT s int4",
            "INOUT",
            "CREATE PROCEDURE fz_plp_",
            "CALL fz_plp_",
            "COMMIT;",
            "ROLLBACK;",
            "CREATE TRIGGER fz_ptg_",
            "TG_OP",
            "TG_ARGV",
            "RETURN NULL",
            "REFERENCING",
            "FOR EACH STATEMENT",
            "CREATE CONSTRAINT TRIGGER",
            "DEFERRABLE INITIALLY DEFERRED",
            "INSTEAD OF INSERT OR UPDATE OR DELETE",
            "unique_violation",
            "pg_get_triggerdef",
            "CREATE COLLATION fz_coll_",
            " COLLATE fz_coll_",
            "pg_collation",
            "DROP COLLATION fz_coll_",
            "CREATE OPERATOR ",
            "DROP OPERATOR ",
            "CREATE OPERATOR CLASS fz_opc_",
            "CREATE OPERATOR FAMILY fz_opf_",
            "USING hash",
            "REINDEX INDEX fz_pli_",
            "DROP OPERATOR CLASS fz_opc_",
        ] {
            assert!(hay.contains(frag), "plpg flavor {frag:?} never generated");
        }
        for p in [
            "plpg:fn",
            "plpg:fn:ctrl",
            "plpg:fn:loop",
            "plpg:fn:strict",
            "plpg:fn:exc",
            "plpg:fn:dyn",
            "plpg:fn:diag",
            "plpg:out",
            "plpg:srf:setof",
            "plpg:srf:table",
            "plpg:proc:inout",
            "plpg:proc:txn",
            "plpg:trig",
            "plpg:trig:suppress",
            "plpg:trig:transition",
            "plpg:trig:constraint",
            "plpg:instead",
            "plpg:coll:locale",
            "plpg:coll:from",
            "plpg:coll:lc_pair",
            "plpg:op",
            "plpg:opclass:btree",
            "plpg:opclass:hash",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in &all {
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(
                sql.matches('(').count(),
                sql.matches(')').count(),
                "unbalanced parens: {sql}"
            );
            assert!(!sql.contains("$fzp$ $fzp$"), "empty body: {sql}");
        }
    }

    /// Group self-containment: every object a group creates is dropped by
    /// that same group (functions, procedures, tables, views, collations,
    /// operators, operator classes/families), so nothing leaks into the
    /// shared catalog space and no later statement can dangle.
    #[test]
    fn plpg_groups_are_self_contained() {
        let (groups, _) = gen_groups(0x51CA, 800, "");
        for group in &groups {
            let mut created: Vec<(String, String)> = Vec::new(); // (kind, name)
            for sql in group {
                for (kind, pfx) in [
                    ("fn", "CREATE FUNCTION "),
                    ("proc", "CREATE PROCEDURE "),
                    ("table", "CREATE TABLE "),
                    ("view", "CREATE VIEW "),
                    ("coll", "CREATE COLLATION "),
                    ("opc", "CREATE OPERATOR CLASS "),
                    ("opf", "CREATE OPERATOR FAMILY "),
                ] {
                    if let Some(rest) = sql.strip_prefix(pfx) {
                        let name = rest.split(['(', ' ']).next().unwrap().to_string();
                        created.push((kind.to_string(), name));
                    }
                }
                if let Some(rest) = sql.strip_prefix("CREATE OPERATOR ") {
                    if !rest.starts_with("CLASS") && !rest.starts_with("FAMILY") {
                        let name = rest.split(' ').next().unwrap().to_string();
                        created.push(("op".to_string(), name));
                    }
                }
            }
            for (kind, name) in &created {
                let dropped = group.iter().any(|sql| {
                    sql.starts_with("DROP ") && sql.contains(name.as_str())
                });
                assert!(dropped, "group leaks {kind} {name}: {group:?}");
            }
            // Drops come after the last use: the final statement of any
            // creating group must be a DROP.
            if !created.is_empty() {
                assert!(
                    group.last().unwrap().starts_with("DROP "),
                    "group does not end in DROP: {group:?}"
                );
            }
        }
    }

    /// Trigger groups probe the mutated table pk-ordered before dropping
    /// it, and the probe covers all three columns (NEW/OLD mutations are
    /// the differential surface).
    #[test]
    fn trig_groups_probe_before_drop() {
        let (groups, _) = gen_groups(
            77,
            300,
            "plpg:trig=10,plpg:fn=0,plpg:out=0,plpg:srf=0,plpg:proc=0,\
             plpg:instead=0,plpg:coll=0,plpg:op=0,plpg:opclass=0",
        );
        let mut saw = false;
        for group in &groups {
            let Some(create) = group.first() else { continue };
            let Some(rest) = create.strip_prefix("CREATE TABLE ") else { continue };
            let tname = rest.split(' ').next().unwrap().to_string();
            saw = true;
            let probe_at = group
                .iter()
                .position(|s| {
                    *s == format!("SELECT pk, k_int, k_text FROM {} ORDER BY pk;", tname)
                })
                .unwrap_or_else(|| panic!("no probe in trig group: {group:?}"));
            let drop_at = group
                .iter()
                .position(|s| *s == format!("DROP TABLE {};", tname))
                .expect("no table drop");
            assert!(probe_at < drop_at, "probe after drop: {group:?}");
        }
        assert!(saw, "no trigger group generated under heavy bias");
    }

    /// GUC bracket discipline: every opclass group's SET enable_seqscan
    /// has its RESET inside the same group.
    #[test]
    fn opclass_guc_brackets_are_group_local() {
        let (groups, _) = gen_groups(
            5,
            200,
            "plpg:opclass=10,plpg:fn=0,plpg:out=0,plpg:srf=0,plpg:proc=0,\
             plpg:trig=0,plpg:instead=0,plpg:coll=0,plpg:op=0",
        );
        let mut saw = false;
        for group in &groups {
            let resets = group.iter().filter(|s| *s == "RESET enable_seqscan;").count();
            let set_only = group
                .iter()
                .filter(|s| *s == "SET enable_seqscan TO off;")
                .count();
            assert_eq!(set_only, resets, "unbalanced seqscan bracket: {group:?}");
            if set_only > 0 {
                saw = true;
            }
        }
        assert!(saw, "no opclass group with a seqscan bracket");
    }

    /// Names are session-unique across groups (monotonic counters).
    #[test]
    fn names_never_reused() {
        let (groups, _) = gen_groups(0xABCD, 600, "");
        let mut seen: Vec<String> = Vec::new();
        for group in &groups {
            for sql in group {
                for pfx in ["CREATE FUNCTION ", "CREATE PROCEDURE ", "CREATE TABLE ", "CREATE VIEW ", "CREATE COLLATION "] {
                    if let Some(rest) = sql.strip_prefix(pfx) {
                        let name = rest.split(['(', ' ']).next().unwrap().to_string();
                        assert!(
                            !seen.contains(&name),
                            "object name reused across groups: {name}"
                        );
                        seen.push(name);
                    }
                }
            }
        }
        assert!(!seen.is_empty());
    }
}
