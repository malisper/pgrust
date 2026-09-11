//! compose: composition shells for `sitediff smoke --compose <p>`.
//!
//! The generator (`session::run_session`) emits statements one feature
//! at a time; the real bugs of the 2026-09-10 appbench batch came from
//! feature PAIRS it never composes (a SECURITY DEFINER SQL SRF at
//! ExecutorEnd, a plpgsql record with a mixed-case field, an aggregate
//! under a pulled-up subquery, a WITH HOLD cursor across session exit, a
//! multi-statement simple-query message, a rule with a multi-column SET
//! deparsed by ruleutils). This module wraps a generated statement in
//! one of those shells without touching the generator: the wrapped
//! statement keeps its production tags and gains `compose:<shell>`, so a
//! finding still attributes to the inner production and to the shell.
//!
//! A shell is an execution context that must not change the statement's
//! result or error, so any A/B divergence a shelled step shows is a
//! shell×statement interaction bug. Shells are only ever applied to the
//! statement kinds they are legal for (`Kind`: SELECT-shaped / DML with
//! RETURNING / DML without / transaction-safe DDL / utility) so a shell
//! never manufactures rig noise; `Template::applies` is the filter.
//!
//! Row-returning shells render the inner query's rows through `q::text`
//! (composite text form), so the wrapped result has one text column and
//! compares on the rows plane like any other statement; the inner
//! `ORDER BY` is not preserved through a subquery / function / cursor
//! shell, so such a step is `ordered: none` (multiset compare). Shells
//! that replay the statement verbatim (prepared, cursor FETCH ALL,
//! savepoint, multi-statement, pipeline) keep the generator's order
//! witness. DML shells that reroute the statement's target table
//! (`view-rule`, `partition-dml`) need the target in the fixture catalog
//! (`ShellCtx`).

use crate::catalog::Catalog;
use crate::contracts::{Ordered, StepKind, XProto};
use crate::recipe::{shape_of, Shape};
use crate::rng::Rng;

/// One statement of the composed stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Composed {
    pub sql: String,
    pub productions: Vec<String>,
    pub ordered: Ordered,
    /// Index of the generated statement this step derives from.
    pub source: usize,
    /// `sql` (default), `xproto` (pipeline shell) or `disconnect`
    /// (cursor-leak shell: the session ends with the cursor open).
    pub kind: StepKind,
    pub xproto: Option<XProto>,
}

/// One rendered wire step of a shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Piece {
    pub sql: String,
    pub ordered: Ordered,
    pub kind: StepKind,
    pub xproto: Option<XProto>,
}

impl Piece {
    fn sql(sql: impl Into<String>, ordered: Ordered) -> Piece {
        Piece { sql: sql.into(), ordered, kind: StepKind::Sql, xproto: None }
    }
    fn disconnect() -> Piece {
        Piece { sql: String::new(), ordered: Ordered::None, kind: StepKind::Disconnect, xproto: None }
    }
    fn pipeline(sql: impl Into<String>, ordered: Ordered, repeat: u32) -> Piece {
        let mut x = XProto::simple("pipeline");
        x.repeat = repeat;
        Piece { sql: sql.into(), ordered, kind: StepKind::Xproto, xproto: Some(x) }
    }
}

/// Statement kind for the shell filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// SELECT / VALUES / TABLE (row-returning, side-effect free shape).
    Select,
    /// INSERT / UPDATE / DELETE / MERGE with RETURNING.
    DmlReturning,
    /// INSERT / UPDATE / DELETE / MERGE without RETURNING.
    Dml,
    /// CREATE / ALTER / DROP / TRUNCATE / COMMENT / GRANT / REVOKE /
    /// REINDEX / REFRESH that can run inside a transaction block.
    Ddl,
    /// Everything else (SET, SHOW, EXPLAIN, BEGIN/COMMIT, VACUUM, FETCH,
    /// COPY, ...): never shelled.
    Utility,
}

impl Kind {
    pub fn is_dml(self) -> bool {
        matches!(self, Kind::Dml | Kind::DmlReturning)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verb {
    Insert,
    Update,
    Delete,
    Merge,
}

/// The target relation of a DML statement and where its name sits in
/// the statement text (for rerouting shells).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DmlTarget {
    pub verb: Verb,
    /// Lowercased unquoted name (quoted / qualified names are not
    /// rerouted: `None` target).
    pub table: String,
    pub span: (usize, usize),
    pub on_conflict: bool,
}

/// A classified, shell-eligible statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classified {
    pub kind: Kind,
    pub ordered: Ordered,
    pub target: Option<DmlTarget>,
}

/// What the shells know about the fixture: DML-target tables and their
/// primary keys (rerouting shells build a look-alike relation).
#[derive(Clone, Debug, Default)]
pub struct ShellCtx {
    pub tables: Vec<ShellTable>,
}

#[derive(Clone, Debug)]
pub struct ShellTable {
    pub name: String,
    pub pk: String,
}

impl ShellCtx {
    pub fn from_catalog(catalog: &Catalog) -> ShellCtx {
        ShellCtx {
            tables: catalog
                .tables
                .iter()
                .filter(|t| !t.at_most_one_row)
                .filter_map(|t| t.pk.as_ref().map(|pk| ShellTable { name: t.name.clone(), pk: pk.column.clone() }))
                .collect(),
        }
    }

    fn table(&self, name: &str) -> Option<&ShellTable> {
        self.tables.iter().find(|t| t.name == name)
    }
}

/// The composition shells. Each wraps one statement in one or more wire
/// steps; the last ones are the queries whose result exposes the
/// composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Template {
    /// SQL-language `SECURITY DEFINER` set-returning function, called in
    /// FROM (the PostgREST PR #2095 shape).
    SecdefSrf,
    /// plpgsql DO block iterating a mixed-case quoted `record` variable
    /// over the query, counting rows into a NOTICE (PR #2094 shape).
    PlpgsqlRecord,
    /// `WITH "Cte" AS MATERIALIZED (...)` body.
    Cte,
    /// `PREPARE` / `EXECUTE` / `DEALLOCATE`.
    Prepared,
    /// `WITH HOLD` cursor declared in a transaction, fetched after
    /// `COMMIT` (PR #2092 shape).
    HoldCursor,
    /// Aggregate over the query as a FROM-subquery with a sublink
    /// referencing the same query (PR #2089 / #2090 shape).
    AggSublink,
    /// The query and a second statement in ONE simple-query message
    /// (multi-statement, PR #2093 shape).
    MultiStatement,
    /// Ordered-set / hypothetical-set aggregates over the query with a
    /// sublink direct argument (PR #2090 bug B shape).
    OrderedSetAgg,
    /// Row trigger on a quoted mixed-case table whose plpgsql body reads
    /// and assigns `NEW."camelCase"` and runs the statement (as a query
    /// for SELECT, via EXECUTE for DML / DDL), fired by INSERT / UPDATE /
    /// DELETE (PR #2094 shape).
    TriggerExec,
    /// SCROLL WITH HOLD cursor fetched partly before COMMIT, partly
    /// after (needs a total order witness).
    HoldCursorSplit,
    /// WITH HOLD cursor fetched after COMMIT and left open when the
    /// session ends (PR #2092 leak shape; the next session's log lines
    /// belong to the observation).
    HoldCursorLeak,
    /// The statement N times (N in {2, 50, 500}) in ONE simple-query
    /// message with its setup (PR #2093 shape).
    MultiN,
    /// The statement N times as one extended-protocol pipeline:
    /// Parse/Bind/Execute ×N, one Sync at the end (PR #2093 shape).
    PipelineN,
    /// Aggregate in a nested scalar subquery over the query as a
    /// FROM-subquery (`agglevelsup >= 2`, PR #2090 bug A) and a json_agg
    /// over a grouped subquery.
    AggNested,
    /// CREATE VIEW over the SELECT, SELECT from it, `pg_get_viewdef`.
    ViewSelect,
    /// Auto-updatable view over the DML's target with DO ALSO rules
    /// carrying a multi-column SET; DML rerouted through the view;
    /// `pg_get_ruledef` (PR #2105 shape).
    ViewRule,
    /// SQL function with SET clauses, LEAKPROOF, SECURITY DEFINER; called
    /// in FROM and in the select list.
    SecdefSetLeakproof,
    /// plpgsql SRF `RETURN QUERY <select>` called in FROM and in the
    /// select list.
    PlpgsqlSrf,
    /// DML rerouted through a range-partitioned look-alike of its target
    /// with a row trigger on the partition.
    PartitionDml,
    /// The SELECT's rows materialized under a row-level-security policy
    /// and read back as a non-superuser.
    Rls,
    /// plpgsql `FOR r IN <select> LOOP` accumulating `r::text` into a
    /// `text[]` returned by the function.
    PlpgsqlLoopArray,
    /// Prepared statement executed past the generic-plan threshold under
    /// `plan_cache_mode` auto / force_generic_plan / force_custom_plan.
    PlanCache,
    /// SAVEPOINT; statement; ROLLBACK TO; statement again; RELEASE.
    Savepoint,
}

impl Template {
    pub const ALL: [Template; 23] = [
        Template::SecdefSrf,
        Template::PlpgsqlRecord,
        Template::Cte,
        Template::Prepared,
        Template::HoldCursor,
        Template::AggSublink,
        Template::MultiStatement,
        Template::OrderedSetAgg,
        Template::TriggerExec,
        Template::HoldCursorSplit,
        Template::HoldCursorLeak,
        Template::MultiN,
        Template::PipelineN,
        Template::AggNested,
        Template::ViewSelect,
        Template::ViewRule,
        Template::SecdefSetLeakproof,
        Template::PlpgsqlSrf,
        Template::PartitionDml,
        Template::Rls,
        Template::PlpgsqlLoopArray,
        Template::PlanCache,
        Template::Savepoint,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Template::SecdefSrf => "secdef-srf",
            Template::PlpgsqlRecord => "plpgsql-record",
            Template::Cte => "cte",
            Template::Prepared => "prepared",
            Template::HoldCursor => "hold-cursor",
            Template::AggSublink => "agg-sublink",
            Template::MultiStatement => "multi-statement",
            Template::OrderedSetAgg => "ordered-set-agg",
            Template::TriggerExec => "trigger-exec",
            Template::HoldCursorSplit => "hold-cursor-split",
            Template::HoldCursorLeak => "hold-cursor-leak",
            Template::MultiN => "multi-n",
            Template::PipelineN => "pipeline-n",
            Template::AggNested => "agg-nested",
            Template::ViewSelect => "view-select",
            Template::ViewRule => "view-rule",
            Template::SecdefSetLeakproof => "secdef-set-leakproof",
            Template::PlpgsqlSrf => "plpgsql-srf",
            Template::PartitionDml => "partition-dml",
            Template::Rls => "rls",
            Template::PlpgsqlLoopArray => "plpgsql-loop-array",
            Template::PlanCache => "plan-cache",
            Template::Savepoint => "savepoint",
        }
    }

    pub fn parse(s: &str) -> Option<Template> {
        Template::ALL.iter().copied().find(|t| t.name() == s)
    }

    /// The kind filter: is the shell legal for this statement?
    pub fn applies(self, c: &Classified, ctx: &ShellCtx) -> bool {
        let k = c.kind;
        // DML rerouted through a look-alike relation: the target must be
        // a plain fixture table.
        let reroutable = |no_conflict: bool, no_merge: bool| {
            k.is_dml()
                && c.target.as_ref().is_some_and(|t| {
                    ctx.table(&t.table).is_some() && !(no_conflict && t.on_conflict) && !(no_merge && t.verb == Verb::Merge)
                })
        };
        // Repeated execution: a fresh-pk INSERT collides with itself.
        let repeatable = k == Kind::Select || (k.is_dml() && c.target.as_ref().is_some_and(|t| t.verb != Verb::Insert));
        match self {
            Template::SecdefSrf
            | Template::PlpgsqlRecord
            | Template::Cte
            | Template::Prepared
            | Template::HoldCursor
            | Template::AggSublink
            | Template::MultiStatement
            | Template::OrderedSetAgg
            | Template::HoldCursorLeak
            | Template::AggNested
            | Template::ViewSelect
            | Template::SecdefSetLeakproof
            | Template::PlpgsqlSrf
            | Template::Rls
            | Template::PlpgsqlLoopArray => k == Kind::Select,
            Template::HoldCursorSplit => k == Kind::Select && c.ordered == Ordered::Total,
            Template::TriggerExec => matches!(k, Kind::Select | Kind::Dml | Kind::DmlReturning | Kind::Ddl),
            Template::MultiN | Template::PipelineN | Template::PlanCache => repeatable,
            Template::ViewRule => reroutable(true, true),
            Template::PartitionDml => reroutable(false, false),
            Template::Savepoint => matches!(k, Kind::Select | Kind::Dml | Kind::DmlReturning | Kind::Ddl),
        }
    }

    /// Render the shell around `inner` (a `;`-free statement classified
    /// as `c`). `salt` picks a variant where the shell has one (the
    /// repeat count of `multi-n` / `pipeline-n`).
    pub fn render(self, inner: &str, c: &Classified, ctx: &ShellCtx, salt: u64) -> Vec<Piece> {
        let n = Ordered::None;
        let t = Ordered::Total;
        let ordered = c.ordered;
        let rows = format!("SELECT q::text FROM ({inner}) q");
        let repeat = [2u32, 50, 500][(salt % 3) as usize];
        match self {
            Template::SecdefSrf => vec![
                Piece::sql(
                    format!(
                        "CREATE OR REPLACE FUNCTION fz_comp_srf() RETURNS SETOF text LANGUAGE sql SECURITY DEFINER SET search_path = public AS $fzc$ {rows} $fzc$;"
                    ),
                    n,
                ),
                Piece::sql("SELECT * FROM fz_comp_srf() AS r(v);", n),
                Piece::sql("DROP FUNCTION fz_comp_srf();", n),
            ],
            Template::PlpgsqlRecord => vec![Piece::sql(
                format!(
                    "DO $fzc$ DECLARE \"Rec\" record; \"nRows\" bigint := 0; \"lastRow\" text; BEGIN FOR \"Rec\" IN {inner} LOOP \"nRows\" := \"nRows\" + 1; \"lastRow\" := \"Rec\"::text; END LOOP; RAISE NOTICE 'fz_comp rows=% last_len=%', \"nRows\", length(\"lastRow\"); END $fzc$;"
                ),
                n,
            )],
            Template::Cte => vec![Piece::sql(format!("WITH \"Cte\" AS MATERIALIZED ({inner}) SELECT \"Cte\"::text AS v FROM \"Cte\";"), n)],
            Template::Prepared => vec![
                Piece::sql(format!("PREPARE fz_comp_p AS {inner};"), n),
                Piece::sql("EXECUTE fz_comp_p;", ordered),
                Piece::sql("DEALLOCATE fz_comp_p;", n),
            ],
            Template::HoldCursor => vec![
                Piece::sql("BEGIN;", n),
                Piece::sql(format!("DECLARE fz_comp_c CURSOR WITH HOLD FOR {inner};"), n),
                Piece::sql("COMMIT;", n),
                Piece::sql("FETCH ALL FROM fz_comp_c;", ordered),
                Piece::sql("CLOSE fz_comp_c;", n),
            ],
            Template::AggSublink => vec![Piece::sql(
                format!(
                    "SELECT count(*) AS n, count(DISTINCT q::text) AS nd, (SELECT count(*) FROM ({inner}) q2 WHERE q2::text = (SELECT min(q3::text) FROM ({inner}) q3)) AS nmin FROM ({inner}) q;"
                ),
                t,
            )],
            Template::MultiStatement => vec![Piece::sql(
                format!("SELECT q::text AS v FROM ({inner}) q; SELECT count(*) AS n FROM ({inner}) q;"),
                n,
            )],
            // 1. ordered-set / hypothetical-set aggregates with sublink
            // direct arguments (the #2090 bug-B shape) over the rows as
            // text (always sortable).
            Template::OrderedSetAgg => vec![Piece::sql(
                format!(
                    "SELECT percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY x) AS med, \
percentile_disc((SELECT ARRAY[0.0, 1.0]::float8[])) WITHIN GROUP (ORDER BY x) AS ends, \
rank((SELECT 'm'::text)) WITHIN GROUP (ORDER BY x) AS rk, \
dense_rank((SELECT min(x2) FROM (SELECT q2::text AS x2 FROM ({inner}) q2) s2)) WITHIN GROUP (ORDER BY x) AS drk, \
percent_rank((SELECT max(x3) FROM (SELECT q3::text AS x3 FROM ({inner}) q3) s3)) WITHIN GROUP (ORDER BY x) AS prk, \
count(*) AS n FROM (SELECT q::text AS x FROM ({inner}) q) s;"
                ),
                t,
            )],
            // 2. trigger on a quoted mixed-case table; the body runs the
            // statement (SELECT as a query, DML / DDL via EXECUTE) and
            // reads / assigns NEW."camelCase" (the #2094 shape).
            Template::TriggerExec => {
                let run = if c.kind == Kind::Select {
                    format!("IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN SELECT count(*) INTO \"fzN\" FROM ({inner}) q; END IF;")
                } else {
                    format!("IF TG_OP = 'INSERT' THEN EXECUTE $fzq${inner}$fzq$; GET DIAGNOSTICS \"fzN\" = ROW_COUNT; END IF;")
                };
                vec![
                    Piece::sql("DROP TABLE IF EXISTS \"fzCompTrig\";", n),
                    Piece::sql("CREATE TABLE \"fzCompTrig\" (\"camelCase\" text, \"rowCount\" bigint, \"opName\" text) WITH (autovacuum_enabled = off);", n),
                    Piece::sql(
                        format!(
                            "CREATE OR REPLACE FUNCTION fz_comp_trg() RETURNS trigger LANGUAGE plpgsql AS $fzc$ DECLARE \"fzN\" bigint := 0; BEGIN \
IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; {run} \
NEW.\"camelCase\" := NEW.\"camelCase\" || ':' || TG_OP; NEW.\"rowCount\" := \"fzN\"; NEW.\"opName\" := lower(TG_OP); RETURN NEW; END $fzc$;"
                        ),
                        n,
                    ),
                    Piece::sql("CREATE TRIGGER \"fzTrg\" BEFORE INSERT OR UPDATE OR DELETE ON \"fzCompTrig\" FOR EACH ROW EXECUTE FUNCTION fz_comp_trg();", n),
                    Piece::sql("INSERT INTO \"fzCompTrig\" VALUES ('ins', 0, '');", n),
                    Piece::sql("UPDATE \"fzCompTrig\" SET \"camelCase\" = \"camelCase\" || '/upd' RETURNING \"camelCase\", \"rowCount\", \"opName\";", t),
                    Piece::sql("DELETE FROM \"fzCompTrig\" RETURNING \"camelCase\", \"rowCount\";", t),
                    Piece::sql("DROP TABLE \"fzCompTrig\";", n),
                    Piece::sql("DROP FUNCTION fz_comp_trg();", n),
                ]
            }
            // 3a. WITH HOLD cursor split across COMMIT (total order only:
            // the chunks are compared in order).
            Template::HoldCursorSplit => vec![
                Piece::sql("BEGIN;", n),
                Piece::sql(format!("DECLARE fz_comp_hs SCROLL CURSOR WITH HOLD FOR {inner};"), n),
                Piece::sql("FETCH FORWARD 2 FROM fz_comp_hs;", t),
                Piece::sql("COMMIT;", n),
                Piece::sql("FETCH FORWARD 3 FROM fz_comp_hs;", t),
                Piece::sql("FETCH BACKWARD 1 FROM fz_comp_hs;", t),
                Piece::sql("FETCH ALL FROM fz_comp_hs;", t),
                Piece::sql("CLOSE fz_comp_hs;", n),
            ],
            // 3b. WITH HOLD cursor left open at session end (the #2092
            // leak shape): the runner slices the exited backend's log
            // lines into this step and the next.
            Template::HoldCursorLeak => vec![
                Piece::sql("BEGIN;", n),
                Piece::sql(format!("DECLARE fz_comp_hl CURSOR WITH HOLD FOR {inner};"), n),
                Piece::sql("COMMIT;", n),
                Piece::sql("FETCH ALL FROM fz_comp_hl;", ordered),
                Piece::disconnect(),
            ],
            // 4a. N copies of the statement in ONE simple-query message
            // with a setup and a check statement (the #2093 shape).
            Template::MultiN => {
                let mut msg = format!("SELECT set_config('fz.compose_n', '{repeat}', false);");
                for _ in 0..repeat {
                    msg.push(' ');
                    msg.push_str(inner);
                    msg.push(';');
                }
                msg.push_str(" SELECT current_setting('fz.compose_n') AS n;");
                vec![Piece::sql(msg, ordered)]
            }
            // 4b. the same as one extended-protocol pipeline.
            Template::PipelineN => vec![Piece::pipeline(inner, ordered, repeat)],
            // 5. aggregate in a nested scalar subquery (agglevelsup 2
            // through pull-up, #2090 bug A) and json_agg over a grouped
            // subquery.
            Template::AggNested => vec![
                Piece::sql(
                    format!(
                        "SELECT (SELECT s FROM (SELECT count(x) AS s) r) AS cnt, (SELECT s FROM (SELECT max(x) AS s) r) AS mx, \
(SELECT s || '' FROM (SELECT min(x) AS s) r) AS mn, (SELECT s FROM (SELECT count(*) AS s) r) AS n \
FROM (SELECT q::text AS x FROM ({inner}) q) src(x);"
                    ),
                    t,
                ),
                Piece::sql(
                    format!(
                        "SELECT json_agg(g ORDER BY g.k) AS j, (SELECT s FROM (SELECT sum(c) AS s) r) AS total \
FROM (SELECT x AS k, count(*) AS c FROM (SELECT q::text AS x FROM ({inner}) q) src GROUP BY x) g;"
                    ),
                    t,
                ),
            ],
            // 6a. view over the SELECT; SELECT from it; pg_get_viewdef
            // (ruleutils over the inner query).
            Template::ViewSelect => vec![
                Piece::sql("DROP VIEW IF EXISTS \"fzCompView\";", n),
                Piece::sql(format!("CREATE VIEW \"fzCompView\" WITH (security_barrier) AS SELECT q::text AS \"rowText\" FROM ({inner}) q;"), n),
                Piece::sql("SELECT pg_get_viewdef('\"fzCompView\"'::regclass, true) AS def;", t),
                Piece::sql("SELECT \"rowText\" FROM \"fzCompView\";", n),
                Piece::sql("SELECT count(*) AS n FROM \"fzCompView\" WHERE \"rowText\" IS NOT NULL;", t),
                Piece::sql("DROP VIEW \"fzCompView\";", n),
            ],
            // 6b. auto-updatable view over the DML target with DO ALSO
            // rules (multi-column SET with a sub-select: the #2105
            // PARAM_MULTIEXPR deparse shape); the DML rerouted through
            // the view.
            Template::ViewRule => {
                let tg = c.target.as_ref().expect("applies() checked the target");
                let tbl = ctx.table(&tg.table).expect("applies() checked the catalog");
                let pk = &tbl.pk;
                let rerouted = retarget(inner, tg, "fz_comp_v");
                vec![
                    Piece::sql("DROP VIEW IF EXISTS fz_comp_v;", n),
                    Piece::sql("DROP TABLE IF EXISTS fz_comp_log, fz_comp_rows;", n),
                    Piece::sql("CREATE TABLE fz_comp_log (n int, last text) WITH (autovacuum_enabled = off);", n),
                    Piece::sql("INSERT INTO fz_comp_log VALUES (0, '');", n),
                    Piece::sql("CREATE TABLE fz_comp_rows (op text, k text) WITH (autovacuum_enabled = off);", n),
                    Piece::sql(format!("CREATE VIEW fz_comp_v AS SELECT * FROM {};", tbl.name), n),
                    Piece::sql(
                        "CREATE RULE fz_comp_r_ins AS ON INSERT TO fz_comp_v DO ALSO (UPDATE fz_comp_log SET (n, last) = (SELECT fz_comp_log.n + 1, 'ins'); INSERT INTO fz_comp_rows VALUES ('ins', NEW.".to_string() + pk + "::text));",
                        n,
                    ),
                    Piece::sql(
                        "CREATE RULE fz_comp_r_upd AS ON UPDATE TO fz_comp_v DO ALSO (UPDATE fz_comp_log SET (n, last) = (SELECT fz_comp_log.n + 1, 'upd'); INSERT INTO fz_comp_rows VALUES ('upd', OLD.".to_string() + pk + "::text || '>' || NEW." + pk + "::text));",
                        n,
                    ),
                    Piece::sql(
                        "CREATE RULE fz_comp_r_del AS ON DELETE TO fz_comp_v DO ALSO (UPDATE fz_comp_log SET (n, last) = (SELECT fz_comp_log.n + 1, 'del'); INSERT INTO fz_comp_rows VALUES ('del', OLD.".to_string() + pk + "::text));",
                        n,
                    ),
                    Piece::sql("SELECT r.rulename, pg_get_ruledef(r.oid, true) AS def FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class WHERE c.relname = 'fz_comp_v' ORDER BY r.rulename;", t),
                    Piece::sql(format!("{rerouted};"), ordered),
                    Piece::sql("SELECT n, last FROM fz_comp_log;", t),
                    Piece::sql("SELECT op, k FROM fz_comp_rows;", n),
                    Piece::sql("DROP VIEW fz_comp_v;", n),
                    Piece::sql("DROP TABLE fz_comp_log, fz_comp_rows;", n),
                ]
            }
            // 7a. SQL function with SET clauses, LEAKPROOF, SECURITY
            // DEFINER, in FROM and in the select list.
            Template::SecdefSetLeakproof => vec![
                Piece::sql(
                    format!(
                        "CREATE OR REPLACE FUNCTION fz_comp_sfl() RETURNS SETOF text LANGUAGE sql LEAKPROOF SECURITY DEFINER \
SET search_path = public, pg_temp SET enable_seqscan = off SET work_mem = '256kB' AS $fzc$ {rows} $fzc$;"
                    ),
                    n,
                ),
                Piece::sql("SELECT * FROM fz_comp_sfl() AS r(v);", n),
                Piece::sql("SELECT fz_comp_sfl() AS v;", n),
                Piece::sql("DROP FUNCTION fz_comp_sfl();", n),
            ],
            // 7b. plpgsql SRF RETURN QUERY, in FROM and in the select list.
            Template::PlpgsqlSrf => vec![
                Piece::sql(
                    format!(
                        "CREATE OR REPLACE FUNCTION fz_comp_rq() RETURNS SETOF text LANGUAGE plpgsql AS $fzc$ BEGIN RETURN QUERY {rows}; RETURN; END $fzc$;"
                    ),
                    n,
                ),
                Piece::sql("SELECT * FROM fz_comp_rq() AS r(v);", n),
                Piece::sql("SELECT fz_comp_rq() AS v;", n),
                Piece::sql("DROP FUNCTION fz_comp_rq();", n),
            ],
            // 8. DML rerouted through a range-partitioned look-alike of
            // its target, with a row trigger on one partition.
            Template::PartitionDml => {
                let tg = c.target.as_ref().expect("applies() checked the target");
                let tbl = ctx.table(&tg.table).expect("applies() checked the catalog");
                let pk = &tbl.pk;
                let rerouted = retarget(inner, tg, "fz_comp_part");
                vec![
                    Piece::sql("DROP TABLE IF EXISTS fz_comp_part, fz_comp_cnt;", n),
                    Piece::sql(format!("CREATE TABLE fz_comp_part (LIKE {} INCLUDING ALL) PARTITION BY RANGE ({pk});", tbl.name), n),
                    Piece::sql("CREATE TABLE fz_comp_part_lo PARTITION OF fz_comp_part FOR VALUES FROM (MINVALUE) TO (3) WITH (autovacuum_enabled = off);", n),
                    Piece::sql("CREATE TABLE fz_comp_part_hi PARTITION OF fz_comp_part FOR VALUES FROM (3) TO (MAXVALUE) WITH (autovacuum_enabled = off);", n),
                    Piece::sql("CREATE TABLE fz_comp_cnt (n int) WITH (autovacuum_enabled = off);", n),
                    Piece::sql("INSERT INTO fz_comp_cnt VALUES (0);", n),
                    Piece::sql(
                        "CREATE OR REPLACE FUNCTION fz_comp_ptrg() RETURNS trigger LANGUAGE plpgsql AS $fzc$ BEGIN UPDATE fz_comp_cnt SET n = n + 1; IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $fzc$;",
                        n,
                    ),
                    Piece::sql("CREATE TRIGGER fz_comp_ptrg BEFORE INSERT OR UPDATE OR DELETE ON fz_comp_part_hi FOR EACH ROW EXECUTE FUNCTION fz_comp_ptrg();", n),
                    Piece::sql(format!("INSERT INTO fz_comp_part SELECT * FROM {};", tbl.name), n),
                    Piece::sql(format!("{rerouted};"), ordered),
                    Piece::sql("SELECT n FROM fz_comp_cnt;", t),
                    Piece::sql("SELECT tableoid::regclass::text AS part, p::text AS r FROM fz_comp_part p;", n),
                    Piece::sql("DROP TABLE fz_comp_part, fz_comp_cnt;", n),
                    Piece::sql("DROP FUNCTION fz_comp_ptrg();", n),
                ]
            }
            // 9. the SELECT's rows under an RLS policy, read as a
            // non-superuser (SET ROLE).
            Template::Rls => vec![
                Piece::sql("DROP TABLE IF EXISTS \"fzRls\";", n),
                Piece::sql("DROP ROLE IF EXISTS fz_comp_role;", n),
                Piece::sql("CREATE ROLE fz_comp_role;", n),
                Piece::sql(format!("CREATE TABLE \"fzRls\" WITH (autovacuum_enabled = off) AS SELECT row_number() OVER () AS \"rowNo\", q::text AS \"rowText\" FROM ({inner}) q;"), n),
                Piece::sql("ALTER TABLE \"fzRls\" ENABLE ROW LEVEL SECURITY;", n),
                Piece::sql("CREATE POLICY \"fzPol\" ON \"fzRls\" FOR SELECT TO fz_comp_role USING (\"rowNo\" > 0 AND length(\"rowText\") >= 0);", n),
                Piece::sql("GRANT SELECT ON \"fzRls\" TO fz_comp_role;", n),
                Piece::sql("SET ROLE fz_comp_role;", n),
                Piece::sql("SELECT \"rowText\" FROM \"fzRls\";", n),
                Piece::sql("SELECT count(*) AS n, current_user::text = 'fz_comp_role' AS as_role FROM \"fzRls\" WHERE \"rowNo\" > 0;", t),
                Piece::sql("RESET ROLE;", n),
                Piece::sql("DROP TABLE \"fzRls\";", n),
                Piece::sql("DROP ROLE fz_comp_role;", n),
            ],
            // 10. plpgsql FOR-IN loop accumulating into a text[].
            Template::PlpgsqlLoopArray => vec![
                Piece::sql(
                    format!(
                        "CREATE OR REPLACE FUNCTION fz_comp_loop() RETURNS text[] LANGUAGE plpgsql AS $fzc$ DECLARE \"fzRow\" record; \"fzAcc\" text[] := '{{}}'; BEGIN FOR \"fzRow\" IN {inner} LOOP \"fzAcc\" := \"fzAcc\" || \"fzRow\"::text; END LOOP; RETURN \"fzAcc\"; END $fzc$;"
                    ),
                    n,
                ),
                Piece::sql("SELECT u.v FROM unnest(fz_comp_loop()) AS u(v);", n),
                Piece::sql("SELECT cardinality(fz_comp_loop()) AS n;", t),
                Piece::sql("DROP FUNCTION fz_comp_loop();", n),
            ],
            // 11. prepared statement past the generic-plan threshold under
            // the three plan_cache_mode settings.
            Template::PlanCache => {
                let body = if c.kind == Kind::Select { format!("SELECT q::text AS v FROM ({inner}) q WHERE $1 >= 0") } else { inner.to_string() };
                let ord = if c.kind == Kind::Select { n } else { ordered };
                let mut v = vec![Piece::sql("SET plan_cache_mode = auto;", n), Piece::sql(format!("PREPARE fz_comp_pc(int) AS {body};"), n)];
                for i in 0..6 {
                    v.push(Piece::sql(format!("EXECUTE fz_comp_pc({i});"), ord));
                }
                v.push(Piece::sql("SET plan_cache_mode = force_generic_plan;", n));
                v.push(Piece::sql("EXECUTE fz_comp_pc(6);", ord));
                v.push(Piece::sql("SET plan_cache_mode = force_custom_plan;", n));
                v.push(Piece::sql("EXECUTE fz_comp_pc(7);", ord));
                v.push(Piece::sql("SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'fz_comp_pc';", t));
                v.push(Piece::sql("RESET plan_cache_mode;", n));
                v.push(Piece::sql("DEALLOCATE fz_comp_pc;", n));
                v
            }
            // 12. subtransaction: the statement, rolled back to the
            // savepoint, then again.
            Template::Savepoint => vec![
                Piece::sql("BEGIN;", n),
                Piece::sql("SAVEPOINT fz_comp_sp;", n),
                Piece::sql(format!("{inner};"), ordered),
                Piece::sql("ROLLBACK TO SAVEPOINT fz_comp_sp;", n),
                Piece::sql(format!("{inner};"), ordered),
                Piece::sql("RELEASE SAVEPOINT fz_comp_sp;", n),
                Piece::sql("COMMIT;", n),
            ],
        }
    }
}

/// Rewrite the DML's target name to `new`.
fn retarget(inner: &str, tg: &DmlTarget, new: &str) -> String {
    format!("{}{}{}", &inner[..tg.span.0], new, &inner[tg.span.1..])
}

/// A word of the statement with its byte span and paren depth: unquoted
/// identifiers / keywords lowercased, quoted identifiers verbatim
/// (quotes included). Strings, dollar quotes, comments, numbers,
/// parameters and punctuation are skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Word {
    text: String,
    start: usize,
    end: usize,
    depth: usize,
}

fn words(sql: &str) -> Vec<Word> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = Vec::new();
    let mut i = 0;
    let mut depth = 0usize;
    while i < n {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < n && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut lvl = 1;
            i += 2;
            while i < n && lvl > 0 {
                if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                    lvl += 1;
                    i += 2;
                } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    lvl -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if c == b'\'' || ((c == b'E' || c == b'e') && b.get(i + 1) == Some(&b'\'')) {
            let escapes = c != b'\'';
            i += if escapes { 2 } else { 1 };
            while i < n {
                if escapes && b[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
        } else if c == b'"' {
            let s = i;
            i += 1;
            while i < n {
                if b[i] == b'"' {
                    if b.get(i + 1) == Some(&b'"') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push(Word { text: sql[s..i].to_string(), start: s, end: i, depth });
        } else if c == b'$' {
            // $tag$ ... $tag$ or a $n parameter.
            let mut j = i + 1;
            while j < n && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            if j < n && b[j] == b'$' && !(b[i + 1..j].iter().all(u8::is_ascii_digit) && j > i + 1) {
                let tag = &sql[i..=j];
                i = match sql[j + 1..].find(tag) {
                    Some(k) => j + 1 + k + tag.len(),
                    None => n,
                };
            } else {
                i = j.max(i + 1);
            }
        } else if c == b'(' {
            depth += 1;
            i += 1;
        } else if c == b')' {
            depth = depth.saturating_sub(1);
            i += 1;
        } else if c == b'_' || c.is_ascii_alphabetic() || c >= 0x80 {
            let s = i;
            while i < n && (b[i] == b'_' || b[i] == b'$' || b[i].is_ascii_alphanumeric() || b[i] >= 0x80) {
                i += 1;
            }
            out.push(Word { text: sql[s..i].to_ascii_lowercase(), start: s, end: i, depth });
        } else {
            i += 1;
        }
    }
    out
}

const DDL_VERBS: &[&str] = &["create", "alter", "drop", "truncate", "comment", "grant", "revoke", "reindex", "refresh"];
/// DDL words that cannot run inside a transaction block / a function.
const NON_TXN_WORDS: &[&str] = &["concurrently", "database", "tablespace", "subscription", "system"];

/// Classify `sql` for the shell filter; `None` when no shell may touch
/// it (empty, multi-statement, or carrying a shell dollar tag).
pub fn classify(sql: &str, ordered: Ordered) -> Option<Classified> {
    let body = sql.trim().trim_end_matches(';').trim();
    if body.is_empty() || body.contains("$fz") || body.contains(';') {
        return None;
    }
    let shape = shape_of(body);
    if matches!(shape, Shape::Select | Shape::Values | Shape::Table) {
        let upper = body.to_ascii_uppercase();
        // SELECT INTO / locking clauses cannot live in a subquery or a
        // SQL function body.
        if upper.contains(" INTO ") || upper.contains("FOR UPDATE") || upper.contains("FOR SHARE") || upper.contains("FOR NO KEY") || upper.contains("FOR KEY SHARE") {
            return None;
        }
        return Some(Classified { kind: Kind::Select, ordered, target: None });
    }
    if matches!(shape, Shape::Sequential) {
        return Some(Classified { kind: Kind::Utility, ordered, target: None });
    }
    let ws = words(body);
    let Some(first) = ws.first() else { return None };
    // Main verb after a CTE prefix: the first depth-0 DML keyword not
    // followed by AS (a CTE name).
    let verb_at = if first.text == "with" {
        ws.iter().enumerate().skip(1).find_map(|(i, w)| {
            let is_verb = w.depth == 0 && matches!(w.text.as_str(), "insert" | "update" | "delete" | "merge");
            let next_as = ws.get(i + 1).is_some_and(|x| x.text == "as");
            (is_verb && !next_as).then_some(i)
        })
    } else {
        Some(0)
    };
    let Some(vi) = verb_at else { return Some(Classified { kind: Kind::Utility, ordered, target: None }) };
    let verb = match ws[vi].text.as_str() {
        "insert" => Some(Verb::Insert),
        "update" => Some(Verb::Update),
        "delete" => Some(Verb::Delete),
        "merge" => Some(Verb::Merge),
        _ => None,
    };
    if let Some(verb) = verb {
        let returning = ws.iter().skip(vi + 1).any(|w| w.depth == 0 && w.text == "returning");
        let on_conflict = ws.iter().skip(vi + 1).any(|w| w.depth == 0 && w.text == "conflict");
        // The name: after INTO / FROM (INSERT / MERGE / DELETE) or right
        // after UPDATE, skipping ONLY.
        let mut j = vi + 1;
        if matches!(verb, Verb::Insert | Verb::Merge | Verb::Delete) {
            j += 1;
        }
        if ws.get(j).is_some_and(|w| w.text == "only") {
            j += 1;
        }
        let target = ws.get(j).filter(|w| w.depth == 0 && !w.text.starts_with('"')).and_then(|w| {
            // Not schema-qualified: the next byte is not a dot.
            (body.as_bytes().get(w.end) != Some(&b'.')).then(|| DmlTarget { verb, table: w.text.clone(), span: (w.start, w.end), on_conflict })
        });
        let kind = if returning { Kind::DmlReturning } else { Kind::Dml };
        return Some(Classified { kind, ordered, target });
    }
    if DDL_VERBS.contains(&ws[vi].text.as_str()) && !ws.iter().any(|w| NON_TXN_WORDS.contains(&w.text.as_str())) {
        return Some(Classified { kind: Kind::Ddl, ordered, target: None });
    }
    Some(Classified { kind: Kind::Utility, ordered, target: None })
}

/// Is any shell legal for the statement? (SELECT-shaped, single, no
/// shell dollar tag, no INTO / locking clause — the original hook's
/// filter, kept for the SELECT-only shells.)
pub fn wrappable(sql: &str) -> bool {
    classify(sql, Ordered::None).is_some_and(|c| c.kind == Kind::Select)
}

/// Wrap each statement with probability `p` (per mille) using a shell
/// drawn from `templates` (those legal for the statement's kind) by
/// `rng`; everything else passes through. `source` is the index into
/// the input stream.
pub fn compose(stmts: &[(String, Vec<String>, Ordered)], p_per_mille: u64, templates: &[Template], ctx: &ShellCtx, rng: &mut Rng) -> Vec<Composed> {
    let mut out = Vec::with_capacity(stmts.len());
    for (i, (sql, productions, ordered)) in stmts.iter().enumerate() {
        let pass = |out: &mut Vec<Composed>| {
            out.push(Composed { sql: sql.clone(), productions: productions.clone(), ordered: *ordered, source: i, kind: StepKind::Sql, xproto: None });
        };
        let cl = match classify(sql, *ordered) {
            Some(c) if !templates.is_empty() && c.kind != Kind::Utility => c,
            _ => {
                pass(&mut out);
                continue;
            }
        };
        if !rng.chance(p_per_mille, 1000) {
            pass(&mut out);
            continue;
        }
        let cands: Vec<Template> = templates.iter().copied().filter(|t| t.applies(&cl, ctx)).collect();
        if cands.is_empty() {
            pass(&mut out);
            continue;
        }
        let t = *rng.pick(&cands);
        let salt = rng.next_u64();
        let inner = sql.trim().trim_end_matches(';').trim();
        let mut prods = productions.clone();
        prods.push(format!("compose:{}", t.name()));
        prods.sort();
        prods.dedup();
        for p in t.render(inner, &cl, ctx, salt) {
            out.push(Composed { sql: p.sql, productions: prods.clone(), ordered: p.ordered, source: i, kind: p.kind, xproto: p.xproto });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};

    fn ctx() -> ShellCtx {
        ShellCtx::from_catalog(&FixtureCatalog.load_catalog().unwrap())
    }

    fn cl(sql: &str) -> Classified {
        classify(sql, Ordered::None).unwrap()
    }

    fn cl_total(sql: &str) -> Classified {
        classify(sql, Ordered::Total).unwrap()
    }

    #[test]
    fn wrappable_is_select_shaped_only() {
        assert!(wrappable("SELECT 1;"));
        assert!(wrappable("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(wrappable("VALUES (1), (2);"));
        assert!(wrappable("TABLE t"));
        assert!(!wrappable("INSERT INTO t VALUES (1);"));
        assert!(!wrappable("SET x = 1;"));
        assert!(!wrappable("SELECT 1; SELECT 2;"));
        assert!(!wrappable("SELECT a FROM t FOR UPDATE"));
        assert!(!wrappable("SELECT $fzc$x$fzc$"));
        assert!(!wrappable("SELECT $fzq$x$fzq$"));
    }

    #[test]
    fn classify_kinds_and_targets() {
        assert_eq!(cl("SELECT 1").kind, Kind::Select);
        assert_eq!(cl("SET x = 1").kind, Kind::Utility);
        assert_eq!(cl("EXPLAIN SELECT 1").kind, Kind::Utility);
        assert_eq!(cl("BEGIN").kind, Kind::Utility);
        assert_eq!(cl("VACUUM fz_scalar").kind, Kind::Utility);
        assert_eq!(cl("CREATE TABLE t (a int)").kind, Kind::Ddl);
        assert_eq!(cl("ALTER TABLE t ADD COLUMN b int").kind, Kind::Ddl);
        assert_eq!(cl("CREATE INDEX CONCURRENTLY i ON t (a)").kind, Kind::Utility);
        assert_eq!(cl("CREATE DATABASE d").kind, Kind::Utility);
        assert_eq!(cl("DROP TABLE IF EXISTS t").kind, Kind::Ddl);

        let c = cl("INSERT INTO fz_scalar AS s (pk, k_int) VALUES (100, 1) ON CONFLICT (pk) DO NOTHING");
        assert_eq!(c.kind, Kind::Dml);
        let t = c.target.unwrap();
        assert_eq!((t.verb, t.table.as_str(), t.on_conflict), (Verb::Insert, "fz_scalar", true));
        assert_eq!(&"INSERT INTO fz_scalar AS s"[t.span.0..t.span.1], "fz_scalar");

        let c = cl("UPDATE fz_mixed AS u SET val = 1 WHERE id = 2 RETURNING *");
        assert_eq!(c.kind, Kind::DmlReturning);
        let t = c.target.unwrap();
        assert_eq!((t.verb, t.table.as_str(), t.on_conflict), (Verb::Update, "fz_mixed", false));

        let c = cl("DELETE FROM ONLY fz_wide AS d USING fz_one WHERE d.pk = fz_one.k_int");
        assert_eq!(c.target.unwrap().table, "fz_wide");
        let c = cl("MERGE INTO fz_rich AS m USING fz_one o ON m.pk = o.k_int WHEN MATCHED THEN DELETE");
        assert_eq!(c.target.unwrap().verb, Verb::Merge);

        // CTE prefix: the main verb is the one not followed by AS.
        let c = cl("WITH del AS (DELETE FROM fz_scalar WHERE pk = 1 RETURNING pk) INSERT INTO fz_mixed (id) SELECT pk FROM del");
        assert_eq!(c.kind, Kind::Dml);
        assert_eq!(c.target.unwrap().table, "fz_mixed");
        // A string literal containing RETURNING does not count.
        assert_eq!(cl("UPDATE fz_scalar SET t = 'x returning y' WHERE pk = 1").kind, Kind::Dml);
        // Quoted or qualified targets are not rerouted.
        assert!(cl("UPDATE \"Fz\" SET a = 1").target.is_none());
        assert!(cl("UPDATE public.fz_scalar SET k_int = 1").target.is_none());
        // Multi-statement / shell tags: no shell at all.
        assert!(classify("INSERT INTO t VALUES (1); SELECT 1", Ordered::None).is_none());
        assert!(classify("DO $fzc$ BEGIN END $fzc$", Ordered::None).is_none());
    }

    #[test]
    fn retarget_rewrites_only_the_target_token() {
        let sql = "UPDATE fz_scalar AS u SET k_int = 1 FROM fz_scalar x WHERE u.pk = x.pk";
        let c = cl(sql);
        assert_eq!(retarget(sql, c.target.as_ref().unwrap(), "fz_comp_part"), "UPDATE fz_comp_part AS u SET k_int = 1 FROM fz_scalar x WHERE u.pk = x.pk");
    }

    #[test]
    fn kind_filter_per_shell() {
        let ctx = ctx();
        let sel = cl("SELECT pk FROM fz_scalar");
        let sel_total = cl_total("SELECT pk FROM fz_scalar ORDER BY pk");
        let ins = cl("INSERT INTO fz_scalar (pk) VALUES (100)");
        let ins_conflict = cl("INSERT INTO fz_scalar (pk) VALUES (100) ON CONFLICT DO NOTHING");
        let upd = cl("UPDATE fz_scalar SET k_int = 1 WHERE pk = 1 RETURNING pk");
        let merge = cl("MERGE INTO fz_scalar m USING fz_one o ON m.pk = o.k_int WHEN MATCHED THEN DELETE");
        let upd_unknown = cl("UPDATE fz_ddl_9 SET a = 1");
        let ddl = cl("CREATE TABLE fz_x (a int)");
        let util = cl("SET work_mem = '1MB'");
        for t in Template::ALL {
            assert!(!t.applies(&util, &ctx), "{} accepts utility", t.name());
            let dml_only = matches!(t, Template::ViewRule | Template::PartitionDml);
            assert_eq!(t.applies(&sel_total, &ctx), !dml_only, "{} on a total-order select", t.name());
        }
        // SELECT-only shells.
        for t in [
            Template::SecdefSrf,
            Template::PlpgsqlRecord,
            Template::Cte,
            Template::Prepared,
            Template::HoldCursor,
            Template::AggSublink,
            Template::MultiStatement,
            Template::OrderedSetAgg,
            Template::HoldCursorLeak,
            Template::AggNested,
            Template::ViewSelect,
            Template::SecdefSetLeakproof,
            Template::PlpgsqlSrf,
            Template::Rls,
            Template::PlpgsqlLoopArray,
        ] {
            assert!(t.applies(&sel, &ctx), "{}", t.name());
            for c in [&ins, &upd, &merge, &ddl] {
                assert!(!t.applies(c, &ctx), "{} accepts {:?}", t.name(), c.kind);
            }
        }
        assert!(!Template::HoldCursorSplit.applies(&sel, &ctx));
        assert!(Template::HoldCursorSplit.applies(&sel_total, &ctx));
        for t in [Template::TriggerExec, Template::Savepoint] {
            for c in [&sel, &ins, &upd, &merge, &ddl, &upd_unknown] {
                assert!(t.applies(c, &ctx), "{} rejects {:?}", t.name(), c.kind);
            }
        }
        for t in [Template::MultiN, Template::PipelineN, Template::PlanCache] {
            assert!(t.applies(&sel, &ctx));
            assert!(t.applies(&upd, &ctx));
            assert!(t.applies(&merge, &ctx));
            assert!(!t.applies(&ins, &ctx), "{} repeats a fresh-pk INSERT", t.name());
            assert!(!t.applies(&ddl, &ctx));
        }
        assert!(Template::ViewRule.applies(&ins, &ctx));
        assert!(Template::ViewRule.applies(&upd, &ctx));
        assert!(!Template::ViewRule.applies(&ins_conflict, &ctx));
        assert!(!Template::ViewRule.applies(&merge, &ctx));
        assert!(!Template::ViewRule.applies(&upd_unknown, &ctx));
        assert!(!Template::ViewRule.applies(&sel, &ctx));
        assert!(Template::PartitionDml.applies(&ins, &ctx));
        assert!(Template::PartitionDml.applies(&ins_conflict, &ctx));
        assert!(Template::PartitionDml.applies(&merge, &ctx));
        assert!(!Template::PartitionDml.applies(&upd_unknown, &ctx));
        assert!(!Template::PartitionDml.applies(&sel, &ctx));
        assert!(!Template::PartitionDml.applies(&ddl, &ctx));
    }

    #[test]
    fn every_shell_renders_the_inner_once_per_shell_and_ends_in_a_statement() {
        let ctx = ctx();
        let cases = [
            ("SELECT pk, k_text FROM fz_scalar ORDER BY pk", Ordered::Total),
            ("UPDATE fz_scalar AS u SET k_int = 1 WHERE pk = 1 RETURNING pk", Ordered::None),
            ("DELETE FROM fz_mixed WHERE id = 1", Ordered::None),
            ("CREATE TABLE fz_x (a int)", Ordered::None),
        ];
        for (sql, ordered) in cases {
            let c = classify(sql, ordered).unwrap();
            for t in Template::ALL.into_iter().filter(|t| t.applies(&c, &ctx)) {
                for salt in 0..3 {
                    let pieces = t.render(sql, &c, &ctx, salt);
                    assert!(!pieces.is_empty(), "{}", t.name());
                    let joined: String = pieces.iter().map(|p| p.sql.as_str()).collect::<Vec<_>>().join("\n");
                    let inner_present = joined.contains(sql) || c.target.as_ref().is_some_and(|tg| joined.contains(&sql[tg.span.1..]));
                    assert!(inner_present, "{} lost the inner statement:\n{joined}", t.name());
                    for p in &pieces {
                        match p.kind {
                            StepKind::Sql => assert!(p.sql.ends_with(';'), "{}: {}", t.name(), p.sql),
                            StepKind::Xproto => assert_eq!(p.xproto.as_ref().unwrap().mode, "pipeline"),
                            StepKind::Disconnect => assert!(p.sql.is_empty()),
                            ref k => panic!("{}: unexpected step kind {k:?}", t.name()),
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn shell_shapes() {
        let ctx = ctx();
        let sel = cl_total("SELECT pk FROM fz_scalar ORDER BY pk");
        let inner = "SELECT pk FROM fz_scalar ORDER BY pk";
        // 1. sublink direct argument of an ordered-set aggregate.
        let p = Template::OrderedSetAgg.render(inner, &sel, &ctx, 0);
        assert!(p[0].sql.contains("percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY x)"));
        assert_eq!(p[0].ordered, Ordered::Total);
        // 2. trigger: SELECT runs as a query, DML via EXECUTE.
        let p = Template::TriggerExec.render(inner, &sel, &ctx, 0);
        assert!(p.iter().any(|p| p.sql.contains("NEW.\"camelCase\" := NEW.\"camelCase\"") && p.sql.contains(&format!("FROM ({inner}) q"))));
        let dml = cl("DELETE FROM fz_mixed WHERE id = 1");
        let p = Template::TriggerExec.render("DELETE FROM fz_mixed WHERE id = 1", &dml, &ctx, 0);
        assert!(p.iter().any(|p| p.sql.contains("EXECUTE $fzq$DELETE FROM fz_mixed WHERE id = 1$fzq$")));
        // 3. hold cursors: split keeps the total witness on every chunk;
        // leak ends the session.
        let p = Template::HoldCursorSplit.render(inner, &sel, &ctx, 0);
        assert!(p.iter().filter(|p| p.sql.starts_with("FETCH")).all(|p| p.ordered == Ordered::Total));
        assert!(p.iter().any(|p| p.sql == "COMMIT;") && p.iter().position(|p| p.sql == "COMMIT;") > p.iter().position(|p| p.sql.starts_with("FETCH")));
        let p = Template::HoldCursorLeak.render(inner, &sel, &ctx, 0);
        assert_eq!(p.last().unwrap().kind, StepKind::Disconnect);
        assert!(!p.iter().any(|p| p.sql.starts_with("CLOSE")));
        // 4. multi-n / pipeline-n: N in {2, 50, 500} from the salt.
        for (salt, n) in [(0u64, 2usize), (1, 50), (2, 500)] {
            let p = Template::MultiN.render(inner, &sel, &ctx, salt);
            assert_eq!(p.len(), 1);
            assert_eq!(p[0].sql.matches(inner).count(), n, "salt {salt}");
            assert_eq!(p[0].ordered, Ordered::Total);
            let p = Template::PipelineN.render(inner, &sel, &ctx, salt);
            assert_eq!(p[0].kind, StepKind::Xproto);
            assert_eq!(p[0].xproto.as_ref().unwrap().repeat, n as u32);
            assert_eq!(p[0].sql, inner);
        }
        // 5. nested aggregate + json_agg over a grouped subquery.
        let p = Template::AggNested.render(inner, &sel, &ctx, 0);
        assert!(p[0].sql.contains("(SELECT s FROM (SELECT count(x) AS s) r)"));
        assert!(p[1].sql.contains("json_agg(g ORDER BY g.k)") && p[1].sql.contains("GROUP BY x"));
        // 6. views: viewdef for SELECT; rules with a multi-column SET for DML.
        let p = Template::ViewSelect.render(inner, &sel, &ctx, 0);
        assert!(p.iter().any(|p| p.sql.contains("pg_get_viewdef")));
        let upd = cl("UPDATE fz_mixed AS u SET val = 1 WHERE id = 2");
        let p = Template::ViewRule.render("UPDATE fz_mixed AS u SET val = 1 WHERE id = 2", &upd, &ctx, 0);
        assert!(p.iter().any(|p| p.sql == "UPDATE fz_comp_v AS u SET val = 1 WHERE id = 2;"));
        assert!(p.iter().any(|p| p.sql.contains("SET (n, last) = (SELECT fz_comp_log.n + 1, 'upd')") && p.sql.contains("NEW.id::text")));
        assert!(p.iter().any(|p| p.sql.contains("pg_get_ruledef")));
        // 7. SET clauses + LEAKPROOF; plpgsql RETURN QUERY; both in FROM and select list.
        let p = Template::SecdefSetLeakproof.render(inner, &sel, &ctx, 0);
        assert!(p[0].sql.contains("LEAKPROOF SECURITY DEFINER SET search_path") && p[0].sql.contains("SET work_mem"));
        assert!(p.iter().any(|p| p.sql == "SELECT fz_comp_sfl() AS v;") && p.iter().any(|p| p.sql.contains("FROM fz_comp_sfl()")));
        let p = Template::PlpgsqlSrf.render(inner, &sel, &ctx, 0);
        assert!(p[0].sql.contains("RETURN QUERY SELECT q::text FROM"));
        // 8. partition: the DML rerouted, a trigger on a partition.
        let p = Template::PartitionDml.render("UPDATE fz_mixed AS u SET val = 1 WHERE id = 2", &upd, &ctx, 0);
        assert!(p.iter().any(|p| p.sql == "CREATE TABLE fz_comp_part (LIKE fz_mixed INCLUDING ALL) PARTITION BY RANGE (id);"));
        assert!(p.iter().any(|p| p.sql == "UPDATE fz_comp_part AS u SET val = 1 WHERE id = 2;"));
        assert!(p.iter().any(|p| p.sql.contains("CREATE TRIGGER fz_comp_ptrg BEFORE INSERT OR UPDATE OR DELETE ON fz_comp_part_hi")));
        // 9. RLS: policy over the row columns, read under SET ROLE.
        let p = Template::Rls.render(inner, &sel, &ctx, 0);
        let set = p.iter().position(|p| p.sql == "SET ROLE fz_comp_role;").unwrap();
        let reset = p.iter().position(|p| p.sql == "RESET ROLE;").unwrap();
        assert!(p[set + 1..reset].iter().all(|p| p.sql.starts_with("SELECT")));
        assert!(p.iter().any(|p| p.sql.contains("CREATE POLICY") && p.sql.contains("\"rowText\"")));
        // 10. loop into text[].
        let p = Template::PlpgsqlLoopArray.render(inner, &sel, &ctx, 0);
        assert!(p[0].sql.contains(&format!("FOR \"fzRow\" IN {inner} LOOP \"fzAcc\" := \"fzAcc\" || \"fzRow\"::text")));
        assert!(p[0].sql.contains("RETURNS text[]"));
        // 11. plan cache: 6 auto executions, then generic and custom.
        let p = Template::PlanCache.render(inner, &sel, &ctx, 0);
        assert_eq!(p.iter().filter(|p| p.sql.starts_with("EXECUTE fz_comp_pc(")).count(), 8);
        assert!(p.iter().any(|p| p.sql == "SET plan_cache_mode = force_generic_plan;"));
        assert!(p.iter().any(|p| p.sql == "SET plan_cache_mode = force_custom_plan;"));
        assert_eq!(p.last().unwrap().sql, "DEALLOCATE fz_comp_pc;");
        // 12. savepoint: the statement twice, verbatim, with its witness.
        let p = Template::Savepoint.render(inner, &sel, &ctx, 0);
        assert_eq!(p.iter().filter(|p| p.sql == format!("{inner};") && p.ordered == Ordered::Total).count(), 2);
        assert!(p.iter().any(|p| p.sql == "ROLLBACK TO SAVEPOINT fz_comp_sp;"));
    }

    #[test]
    fn templates_render_the_inner_query_once_per_shell_and_tag() {
        let ctx = ctx();
        let stmts = vec![
            ("SELECT 1 AS a;".to_string(), vec!["expr.const".to_string()], Ordered::Total),
            ("SET x = 1;".to_string(), vec!["cfgm.set".to_string()], Ordered::None),
        ];
        let c = cl_total("SELECT 1 AS a");
        for t in Template::ALL.into_iter().filter(|t| t.applies(&c, &ctx)) {
            let mut rng = Rng::new(7);
            let out = compose(&stmts, 1000, &[t], &ctx, &mut rng);
            let shell = t.render("SELECT 1 AS a", &c, &ctx, 0);
            assert_eq!(out.len(), shell.len() + 1, "{}", t.name());
            for c in &out[..shell.len()] {
                assert!(c.productions.contains(&format!("compose:{}", t.name())));
                assert!(c.productions.contains(&"expr.const".to_string()));
                assert_eq!(c.source, 0);
            }
            assert_eq!(out.last().unwrap().sql, "SET x = 1;");
            assert_eq!(out.last().unwrap().source, 1);
            assert_eq!(out.last().unwrap().kind, StepKind::Sql);
        }
    }

    #[test]
    fn p_zero_passes_through_and_p_full_wraps_every_select() {
        let ctx = ctx();
        let stmts: Vec<(String, Vec<String>, Ordered)> =
            (0..20).map(|i| (format!("SELECT {i};"), vec![], Ordered::Total)).collect();
        let mut rng = Rng::new(1);
        assert_eq!(compose(&stmts, 0, &Template::ALL, &ctx, &mut rng).len(), 20);
        let mut rng = Rng::new(1);
        let all = compose(&stmts, 1000, &Template::ALL, &ctx, &mut rng);
        assert!(all.iter().all(|c| c.productions.iter().any(|p| p.starts_with("compose:"))));
        // Deterministic under the seed.
        let mut rng2 = Rng::new(1);
        assert_eq!(compose(&stmts, 1000, &Template::ALL, &ctx, &mut rng2), all);
    }

    #[test]
    fn dml_only_gets_dml_shells_and_unknown_targets_pass_when_nothing_applies() {
        let ctx = ctx();
        let stmts = vec![("INSERT INTO fz_scalar (pk) VALUES (100);".to_string(), vec!["dml.insert".to_string()], Ordered::None)];
        let mut rng = Rng::new(3);
        let out = compose(&stmts, 1000, &[Template::Cte, Template::Rls], &ctx, &mut rng);
        assert_eq!(out.len(), 1, "no legal shell: pass through");
        let mut rng = Rng::new(3);
        let out = compose(&stmts, 1000, &[Template::Cte, Template::PartitionDml], &ctx, &mut rng);
        assert!(out.iter().all(|c| c.productions.contains(&"compose:partition-dml".to_string())));
        assert!(out.iter().any(|c| c.sql == "INSERT INTO fz_comp_part (pk) VALUES (100);"));
    }

    #[test]
    fn parse_names() {
        for t in Template::ALL {
            assert_eq!(Template::parse(t.name()), Some(t));
        }
        assert_eq!(Template::parse("nope"), None);
        let names: std::collections::BTreeSet<&str> = Template::ALL.iter().map(|t| t.name()).collect();
        assert_eq!(names.len(), Template::ALL.len());
    }

    #[test]
    fn words_skip_strings_comments_and_dollar_quotes() {
        let w = words("UPDATE t SET a = 'x returning ''y''' /* returning */ -- returning\n, b = $q$returning$q$, c = $1 WHERE (d = E'\\'returning') RETURNING *");
        let texts: Vec<&str> = w.iter().map(|w| w.text.as_str()).collect();
        assert_eq!(texts, ["update", "t", "set", "a", "b", "c", "where", "d", "returning"]);
        assert_eq!(w.iter().find(|w| w.text == "d").unwrap().depth, 1);
        assert_eq!(words("select \"Quoted\"\"x\" from t")[1].text, "\"Quoted\"\"x\"");
    }
}
