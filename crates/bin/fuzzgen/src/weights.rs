//! Per-production bias weights: the coverage-loop steering surface.
//!
//! Every choice point *inside* a feature module (which composite expression
//! production, which comparison operator, which leaf kind, ...) draws
//! through a named weight in this table. Biasing a production is either a
//! CLI flag (`--weight case=5,cmp:>=0`) or a one-line edit to
//! `PROD_WEIGHTS` below — both feed the same table. All weighted picks
//! draw from the session PRNG, so same seed + same weights = byte-identical
//! stream; changing any weight changes the stream (the weights are part of
//! the witness and are recorded in snapshots by the covloop tooling).
//!
//! Module-level on/off toggles and module selection weights stay in
//! `toggles.rs`; this table is strictly intra-module.

use crate::rng::Rng;

#[derive(Clone, Copy, Debug)]
pub struct ProdWeight {
    pub name: &'static str,
    pub default: f64,
}

/// All weighted productions. One line per production — the "edit one line,
/// re-run, watch coverage" path. A weight of 0 removes the production from
/// its choice point (if every option at a choice point is 0 the pick falls
/// back to uniform, so a statement can always be produced).
pub const PROD_WEIGHTS: &[ProdWeight] = &[
    // Leaf kinds (historical ratio 5:4:1).
    ProdWeight { name: "colref", default: 5.0 },
    ProdWeight { name: "lit", default: 4.0 },
    ProdWeight { name: "null", default: 1.0 },
    // Within "lit": mid-range random spelling vs boundary-bank draw
    // (boundary.rs; mutation-pilot corpus fix — keep boundary weight
    // meaningful or the random stream regresses to mid-range-only).
    ProdWeight { name: "lit:mid", default: 2.0 },
    ProdWeight { name: "lit:boundary", default: 1.0 },
    // Typed composite productions.
    ProdWeight { name: "case", default: 1.0 },
    ProdWeight { name: "cast", default: 1.0 },
    ProdWeight { name: "coalesce", default: 1.0 },
    ProdWeight { name: "nullif", default: 1.0 },
    ProdWeight { name: "arith", default: 1.0 },
    ProdWeight { name: "neg", default: 1.0 },
    ProdWeight { name: "func_num", default: 1.0 },
    ProdWeight { name: "concat", default: 1.0 },
    ProdWeight { name: "func_text", default: 1.0 },
    ProdWeight { name: "func_varchar", default: 1.0 },
    ProdWeight { name: "length", default: 1.0 },
    ProdWeight { name: "date_plus_int", default: 1.0 },
    ProdWeight { name: "date_trunc", default: 1.0 },
    // Boolean-grammar productions ("case"/"coalesce" above are shared with
    // the boolean grammar; cmp keeps its historical double weight).
    ProdWeight { name: "cmp", default: 2.0 },
    ProdWeight { name: "and_or", default: 1.0 },
    ProdWeight { name: "not", default: 1.0 },
    ProdWeight { name: "isnull", default: 1.0 },
    // Comparison operator family.
    ProdWeight { name: "cmp:=", default: 1.0 },
    ProdWeight { name: "cmp:<>", default: 1.0 },
    ProdWeight { name: "cmp:<", default: 1.0 },
    ProdWeight { name: "cmp:<=", default: 1.0 },
    ProdWeight { name: "cmp:>", default: 1.0 },
    ProdWeight { name: "cmp:>=", default: 1.0 },
    // Arithmetic operator family (% only offered for integer families).
    ProdWeight { name: "binop:+", default: 1.0 },
    ProdWeight { name: "binop:-", default: 1.0 },
    ProdWeight { name: "binop:*", default: 1.0 },
    ProdWeight { name: "binop:/", default: 1.0 },
    ProdWeight { name: "binop:%", default: 1.0 },
    // Boolean connectives.
    ProdWeight { name: "binop:and", default: 1.0 },
    ProdWeight { name: "binop:or", default: 1.0 },
    // Numeric function family (sqrt only offered for float8).
    ProdWeight { name: "func:abs", default: 1.0 },
    ProdWeight { name: "func:ceil", default: 1.0 },
    ProdWeight { name: "func:floor", default: 1.0 },
    ProdWeight { name: "func:round", default: 1.0 },
    ProdWeight { name: "func:sqrt", default: 1.0 },
    // Text function family.
    ProdWeight { name: "func:lower", default: 1.0 },
    ProdWeight { name: "func:upper", default: 1.0 },
    ProdWeight { name: "func:btrim", default: 1.0 },
    // Leaf kind: correlated outer-scope column reference (offered only
    // inside subqueries when the enclosing scope has a column of the type).
    ProdWeight { name: "colref:outer", default: 2.0 },
    // Joins module: chain length.
    ProdWeight { name: "join:2", default: 4.0 },
    ProdWeight { name: "join:3", default: 2.0 },
    ProdWeight { name: "join:4", default: 1.0 },
    // Joins module: join kind (CROSS rare).
    ProdWeight { name: "join:inner", default: 3.0 },
    ProdWeight { name: "join:left", default: 2.0 },
    ProdWeight { name: "join:right", default: 1.0 },
    ProdWeight { name: "join:full", default: 1.0 },
    ProdWeight { name: "join:cross", default: 0.5 },
    // Joins module: condition form (USING/NATURAL occasional, first join
    // only) and ON shape (key equality vs random predicate vs both).
    ProdWeight { name: "join:on", default: 4.0 },
    ProdWeight { name: "join:using", default: 1.0 },
    ProdWeight { name: "join:natural", default: 0.5 },
    ProdWeight { name: "join:on_key", default: 3.0 },
    ProdWeight { name: "join:on_rand", default: 1.0 },
    ProdWeight { name: "join:on_mixed", default: 1.0 },
    // Subqueries module: statement shape.
    ProdWeight { name: "subq:plain", default: 2.0 },
    ProdWeight { name: "subq:derived", default: 1.0 },
    ProdWeight { name: "subq:cte", default: 1.0 },
    ProdWeight { name: "cte:once", default: 2.0 },
    ProdWeight { name: "cte:twice", default: 1.0 },
    // Subqueries module: expression-level subquery productions.
    ProdWeight { name: "subq:scalar", default: 1.0 },
    ProdWeight { name: "subq:scalar_table", default: 2.0 },
    ProdWeight { name: "subq:scalar_nofrom", default: 1.0 },
    ProdWeight { name: "subq:cmp", default: 1.0 },
    ProdWeight { name: "subq:in", default: 2.0 },
    ProdWeight { name: "subq:not_in", default: 1.0 },
    ProdWeight { name: "subq:exists", default: 2.0 },
    ProdWeight { name: "subq:not_exists", default: 1.0 },
    // Subqueries module (X1): recursive CTEs with SEARCH/CYCLE. Raw
    // self-contained statements, so a low share keeps the scoped-AST
    // surface dominant.
    ProdWeight { name: "subq:ast", default: 5.0 },
    ProdWeight { name: "subq:rec", default: 1.0 },
    ProdWeight { name: "subq:rec:plain", default: 1.0 },
    ProdWeight { name: "subq:rec:search", default: 2.0 },
    ProdWeight { name: "subq:rec:cycle", default: 2.0 },
    ProdWeight { name: "subq:rec:both", default: 1.5 },
    ProdWeight { name: "subq:rec:depth", default: 2.0 },
    ProdWeight { name: "subq:rec:breadth", default: 1.0 },
    // Aggregates module: statement shape (grouping-set forms low weight).
    ProdWeight { name: "agg:plain", default: 3.0 },
    ProdWeight { name: "agg:group", default: 4.0 },
    ProdWeight { name: "agg:distinct", default: 2.0 },
    ProdWeight { name: "agg:rollup", default: 0.5 },
    ProdWeight { name: "agg:cube", default: 0.5 },
    ProdWeight { name: "agg:groupingsets", default: 0.5 },
    // Aggregates module: GROUP BY key form.
    ProdWeight { name: "agg:groupby:col", default: 3.0 },
    ProdWeight { name: "agg:groupby:expr", default: 1.0 },
    // Aggregates module: aggregate calls. Order-sensitive float aggregates
    // are deliberately low-weight AND ruled-soft (crate::agg module docs);
    // the exact numeric forms carry the weight instead.
    ProdWeight { name: "agg:count_star", default: 2.0 },
    ProdWeight { name: "agg:count", default: 2.0 },
    ProdWeight { name: "agg:count_distinct", default: 1.5 },
    ProdWeight { name: "agg:sum:int", default: 2.0 },
    ProdWeight { name: "agg:sum:numeric", default: 3.0 },
    ProdWeight { name: "agg:sum:float", default: 0.3 },
    ProdWeight { name: "agg:avg:int", default: 2.0 },
    ProdWeight { name: "agg:avg:numeric", default: 3.0 },
    ProdWeight { name: "agg:avg:float", default: 0.3 },
    ProdWeight { name: "agg:min", default: 2.0 },
    ProdWeight { name: "agg:max", default: 2.0 },
    ProdWeight { name: "agg:bool_and", default: 1.0 },
    ProdWeight { name: "agg:bool_or", default: 1.0 },
    ProdWeight { name: "agg:string_agg", default: 1.5 },
    ProdWeight { name: "agg:array_agg", default: 1.5 },
    ProdWeight { name: "agg:stddev:numeric", default: 1.0 },
    ProdWeight { name: "agg:stddev:float", default: 0.3 },
    // Aggregates module: FILTER and HAVING.
    ProdWeight { name: "agg:filter", default: 1.0 },
    ProdWeight { name: "agg:filter:none", default: 4.0 },
    ProdWeight { name: "agg:having", default: 1.0 },
    ProdWeight { name: "agg:having:none", default: 1.0 },
    ProdWeight { name: "agg:having:agg", default: 3.0 },
    ProdWeight { name: "agg:having:group", default: 1.0 },
    // Window module: row source and named-window shape.
    ProdWeight { name: "win:single", default: 3.0 },
    ProdWeight { name: "win:join", default: 1.0 },
    ProdWeight { name: "win:join:inner", default: 2.0 },
    ProdWeight { name: "win:join:left", default: 1.0 },
    ProdWeight { name: "win:named", default: 1.0 },
    ProdWeight { name: "win:named:none", default: 3.0 },
    // Window module: function family (order-sensitive forms require the
    // total window order; see crate::win module docs).
    ProdWeight { name: "win:row_number", default: 2.0 },
    ProdWeight { name: "win:rank", default: 1.5 },
    ProdWeight { name: "win:dense_rank", default: 1.5 },
    ProdWeight { name: "win:ntile", default: 1.0 },
    ProdWeight { name: "win:lag", default: 2.0 },
    ProdWeight { name: "win:lead", default: 1.5 },
    ProdWeight { name: "win:first_value", default: 1.0 },
    ProdWeight { name: "win:last_value", default: 1.0 },
    ProdWeight { name: "win:nth_value", default: 1.0 },
    ProdWeight { name: "win:count", default: 1.5 },
    ProdWeight { name: "win:sum", default: 2.0 },
    ProdWeight { name: "win:min", default: 1.0 },
    ProdWeight { name: "win:max", default: 1.0 },
    // Window module: PARTITION BY arity, extra order keys, frame clauses.
    ProdWeight { name: "win:part:0", default: 2.0 },
    ProdWeight { name: "win:part:1", default: 2.0 },
    ProdWeight { name: "win:part:2", default: 1.0 },
    ProdWeight { name: "win:order:extra", default: 1.0 },
    ProdWeight { name: "win:order:plain", default: 2.0 },
    ProdWeight { name: "win:frame:none", default: 3.0 },
    ProdWeight { name: "win:frame:rows", default: 1.5 },
    ProdWeight { name: "win:frame:range", default: 1.0 },
    // winfunc module: builtin window-FUNCTION argument/result edge probes.
    // percent_rank/cume_dist (absent from the win module entirely), and the
    // NULL/error/out-of-range argument regimes for ntile/nth_value/lead/lag
    // that the win module deliberately steers around.
    ProdWeight { name: "winfunc:pct_rank", default: 1.5 },
    ProdWeight { name: "winfunc:cume_dist", default: 1.5 },
    ProdWeight { name: "winfunc:ntile_null", default: 1.0 },
    ProdWeight { name: "winfunc:ntile_err", default: 1.0 },
    ProdWeight { name: "winfunc:ntile_one", default: 1.0 },
    ProdWeight { name: "winfunc:ntile_big", default: 1.0 },
    ProdWeight { name: "winfunc:leadlag_nulloff", default: 1.0 },
    ProdWeight { name: "winfunc:leadlag_negoff", default: 1.0 },
    ProdWeight { name: "winfunc:leadlag_bigoff", default: 1.0 },
    ProdWeight { name: "winfunc:leadlag_default", default: 1.0 },
    ProdWeight { name: "winfunc:nth_null", default: 1.0 },
    ProdWeight { name: "winfunc:nth_err", default: 1.0 },
    ProdWeight { name: "winfunc:nth_big", default: 1.0 },
    ProdWeight { name: "winfunc:firstlast_frame", default: 1.0 },
    // ORDER BY / LIMIT / OFFSET suffix (all statement modules). A total
    // ORDER BY covers every output column; only it may carry LIMIT/OFFSET.
    ProdWeight { name: "orderby:none", default: 2.0 },
    ProdWeight { name: "orderby:total", default: 2.0 },
    ProdWeight { name: "orderby:partial", default: 1.0 },
    ProdWeight { name: "orderby:asc", default: 3.0 },
    ProdWeight { name: "orderby:desc", default: 1.0 },
    ProdWeight { name: "orderby:nulls_default", default: 4.0 },
    ProdWeight { name: "orderby:nulls_first", default: 1.0 },
    ProdWeight { name: "orderby:nulls_last", default: 1.0 },
    ProdWeight { name: "limit", default: 1.0 },
    ProdWeight { name: "limit:none", default: 2.0 },
    ProdWeight { name: "offset", default: 1.0 },
    ProdWeight { name: "offset:none", default: 3.0 },
    // DML module: statement kind.
    ProdWeight { name: "dml:insert", default: 3.0 },
    ProdWeight { name: "dml:update", default: 3.0 },
    ProdWeight { name: "dml:delete", default: 2.0 },
    // DML module: INSERT shape.
    ProdWeight { name: "dml:insert:single", default: 3.0 },
    ProdWeight { name: "dml:insert:multirow", default: 2.0 },
    ProdWeight { name: "dml:insert:select", default: 1.0 },
    // DML module: ON CONFLICT handling (VALUES inserts only).
    ProdWeight { name: "dml:onconflict:none", default: 8.0 },
    ProdWeight { name: "dml:onconflict:nothing", default: 1.0 },
    ProdWeight { name: "dml:onconflict:update", default: 1.0 },
    // DML module: pk choice per inserted row. Collisions outside ON
    // CONFLICT stay rare and deliberate (23505 spam wastes budget); under
    // ON CONFLICT the bias flips so the clauses actually engage.
    ProdWeight { name: "dml:pk:fresh", default: 12.0 },
    ProdWeight { name: "dml:pk:collide", default: 1.0 },
    ProdWeight { name: "dml:onconflict:hit", default: 3.0 },
    ProdWeight { name: "dml:onconflict:miss", default: 1.0 },
    // DML module: VALUES cell form (DEFAULT only on nullable columns).
    ProdWeight { name: "dml:insert:value", default: 7.0 },
    ProdWeight { name: "dml:insert:default", default: 1.0 },
    // DML module: SET-list width, UPDATE/DELETE shapes, WHERE form.
    ProdWeight { name: "dml:set:1", default: 3.0 },
    ProdWeight { name: "dml:set:2", default: 2.0 },
    ProdWeight { name: "dml:set:3", default: 1.0 },
    // G2 subscripted SET targets: jsonb subscript writes (jsonbsubs.c
    // assign arm) and array element/slice assignment (array_set_element /
    // array_set_slice) on the subscriptable rich columns.
    ProdWeight { name: "dml:set:plain", default: 4.0 },
    ProdWeight { name: "dml:set:sub", default: 3.0 },
    ProdWeight { name: "dml:update:plain", default: 4.0 },
    ProdWeight { name: "dml:update:from", default: 1.0 },
    ProdWeight { name: "dml:delete:plain", default: 4.0 },
    ProdWeight { name: "dml:delete:using", default: 1.0 },
    ProdWeight { name: "dml:where:pk", default: 3.0 },
    ProdWeight { name: "dml:where:expr", default: 3.0 },
    ProdWeight { name: "dml:where:none", default: 0.5 },
    ProdWeight { name: "dml:returning", default: 1.0 },
    ProdWeight { name: "dml:returning:none", default: 2.0 },
    // MERGE module: source form (VALUES = key control; table; bounded
    // subquery), VALUES row count and key mix, the deliberate
    // cardinality-violation shape (both engines must 21000), extra ON
    // conjunct, arm counts/actions/conditions, PG17 NOT MATCHED BY
    // SOURCE (writes always pk-guarded) and RETURNING/merge_action().
    ProdWeight { name: "merge:src:values", default: 3.0 },
    ProdWeight { name: "merge:src:table", default: 2.0 },
    ProdWeight { name: "merge:src:subq", default: 1.0 },
    ProdWeight { name: "merge:rows:1", default: 2.0 },
    ProdWeight { name: "merge:rows:2", default: 3.0 },
    ProdWeight { name: "merge:rows:3", default: 2.0 },
    ProdWeight { name: "merge:key:known", default: 3.0 },
    ProdWeight { name: "merge:key:fresh", default: 2.0 },
    ProdWeight { name: "merge:card:dup", default: 0.5 },
    ProdWeight { name: "merge:card:none", default: 6.0 },
    ProdWeight { name: "merge:on:extra", default: 1.0 },
    ProdWeight { name: "merge:on:plain", default: 3.0 },
    ProdWeight { name: "merge:matched:0", default: 1.0 },
    ProdWeight { name: "merge:matched:1", default: 4.0 },
    ProdWeight { name: "merge:matched:2", default: 2.0 },
    ProdWeight { name: "merge:m:update", default: 4.0 },
    ProdWeight { name: "merge:m:delete", default: 1.5 },
    ProdWeight { name: "merge:m:nothing", default: 1.0 },
    ProdWeight { name: "merge:nmt:insert", default: 4.0 },
    ProdWeight { name: "merge:nmt:nothing", default: 1.0 },
    ProdWeight { name: "merge:nmt:none", default: 2.0 },
    ProdWeight { name: "merge:nmt:by_target", default: 1.0 },
    ProdWeight { name: "merge:nmt:plain", default: 3.0 },
    ProdWeight { name: "merge:nmbs:update", default: 1.0 },
    ProdWeight { name: "merge:nmbs:delete", default: 0.5 },
    ProdWeight { name: "merge:nmbs:nothing", default: 1.0 },
    ProdWeight { name: "merge:nmbs:none", default: 5.0 },
    ProdWeight { name: "merge:returning", default: 1.0 },
    ProdWeight { name: "merge:returning:none", default: 2.0 },
    // MERGE/ModifyTable/rewrite residue batteries (mergex; self-contained
    // per group). Family selection weights, then fire-only witnesses.
    ProdWeight { name: "mergex", default: 1.0 },
    ProdWeight { name: "mergex:gencol", default: 1.5 },
    ProdWeight { name: "mergex:defcol", default: 1.5 },
    ProdWeight { name: "mergex:subfield", default: 1.2 },
    ProdWeight { name: "mergex:wcte", default: 1.2 },
    ProdWeight { name: "mergex:mergedef", default: 1.0 },
    ProdWeight { name: "mergex:gencol:returning", default: 1.0 },
    ProdWeight { name: "mergex:defcol:allrow", default: 1.0 },
    ProdWeight { name: "mergex:mergedef:nmbs", default: 1.0 },
    // Transactions module: bracket length bucket, savepoint window,
    // terminator, inner statement kind.
    ProdWeight { name: "txn:short", default: 3.0 },
    ProdWeight { name: "txn:mid", default: 2.0 },
    ProdWeight { name: "txn:long", default: 1.0 },
    ProdWeight { name: "txn:plain", default: 2.0 },
    ProdWeight { name: "txn:savepoint", default: 1.0 },
    ProdWeight { name: "txn:sp:rollback", default: 1.0 },
    ProdWeight { name: "txn:sp:release", default: 1.0 },
    ProdWeight { name: "txn:commit", default: 3.0 },
    ProdWeight { name: "txn:rollback", default: 1.0 },
    ProdWeight { name: "txn:inner:dml", default: 3.0 },
    ProdWeight { name: "txn:inner:select", default: 1.0 },
    // DDL module: action mix.
    ProdWeight { name: "ddl:create_table", default: 3.0 },
    ProdWeight { name: "ddl:drop_table", default: 1.0 },
    ProdWeight { name: "ddl:alter", default: 2.5 },
    ProdWeight { name: "ddl:create_index", default: 3.0 },
    ProdWeight { name: "ddl:drop_index", default: 1.0 },
    ProdWeight { name: "ddl:reindex", default: 1.5 },
    ProdWeight { name: "ddl:view", default: 2.0 },
    ProdWeight { name: "ddl:seq", default: 1.5 },
    ProdWeight { name: "ddl:agg", default: 0.7 },
    ProdWeight { name: "ddl:trigger", default: 0.7 },
    ProdWeight { name: "ddl:truncate", default: 0.7 },
    // DDL module (X1): foreign keys. The RI-check surface is reached by
    // OTHER modules' DML against the child tables, so a moderate creation
    // weight buys a lot.
    ProdWeight { name: "ddl:fk", default: 1.5 },
    ProdWeight { name: "ddl:fk:create_child", default: 2.0 },
    ProdWeight { name: "ddl:fk:alter_add", default: 2.0 },
    ProdWeight { name: "ddl:fk:self", default: 0.7 },
    ProdWeight { name: "ddl:fk:valid", default: 3.0 },
    ProdWeight { name: "ddl:fk:notvalid", default: 1.0 },
    ProdWeight { name: "ddl:fk:del:noaction", default: 2.0 },
    ProdWeight { name: "ddl:fk:del:restrict", default: 1.0 },
    ProdWeight { name: "ddl:fk:del:cascade", default: 2.0 },
    ProdWeight { name: "ddl:fk:del:setnull", default: 1.5 },
    ProdWeight { name: "ddl:fk:del:setdefault", default: 1.0 },
    ProdWeight { name: "ddl:fk:upd:noaction", default: 2.0 },
    ProdWeight { name: "ddl:fk:upd:restrict", default: 1.0 },
    ProdWeight { name: "ddl:fk:upd:cascade", default: 1.5 },
    // DDL module: CREATE TABLE shape.
    ProdWeight { name: "ddl:table:temp", default: 1.0 },
    ProdWeight { name: "ddl:table:plain", default: 4.0 },
    ProdWeight { name: "ddl:col:default", default: 1.0 },
    ProdWeight { name: "ddl:col:default:none", default: 3.0 },
    ProdWeight { name: "ddl:col:check", default: 1.0 },
    ProdWeight { name: "ddl:col:check:none", default: 6.0 },
    // DDL module: ALTER TABLE forms.
    ProdWeight { name: "ddl:alter:add_col", default: 3.0 },
    ProdWeight { name: "ddl:alter:drop_col", default: 2.0 },
    ProdWeight { name: "ddl:alter:type", default: 2.0 },
    ProdWeight { name: "ddl:alter:set_not_null", default: 1.0 },
    ProdWeight { name: "ddl:alter:drop_not_null", default: 1.0 },
    ProdWeight { name: "ddl:alter:rename_col", default: 1.5 },
    ProdWeight { name: "ddl:alter:add_check", default: 1.0 },
    ProdWeight { name: "ddl:addcol:plain", default: 3.0 },
    ProdWeight { name: "ddl:addcol:default", default: 1.5 },
    ProdWeight { name: "ddl:addcol:notnull_default", default: 1.0 },
    ProdWeight { name: "ddl:type:widen", default: 3.0 },
    ProdWeight { name: "ddl:type:text", default: 1.0 },
    // DDL module: CREATE INDEX shape (spgist above its neighbors: the
    // text pick-split path needs volume to engage).
    ProdWeight { name: "ddl:idx:btree", default: 4.0 },
    ProdWeight { name: "ddl:idx:hash", default: 1.0 },
    ProdWeight { name: "ddl:idx:gin", default: 0.7 },
    ProdWeight { name: "ddl:idx:gist", default: 0.7 },
    ProdWeight { name: "ddl:idx:spgist", default: 1.2 },
    ProdWeight { name: "ddl:idx:brin", default: 0.7 },
    ProdWeight { name: "ddl:idx:unique", default: 1.0 },
    ProdWeight { name: "ddl:idx:plain", default: 5.0 },
    ProdWeight { name: "ddl:idx:expr", default: 1.0 },
    ProdWeight { name: "ddl:idx:cols", default: 3.0 },
    ProdWeight { name: "ddl:idx:partial", default: 1.0 },
    ProdWeight { name: "ddl:idx:partial:none", default: 3.0 },
    // DDL module: REINDEX forms.
    ProdWeight { name: "ddl:reindex:index", default: 2.0 },
    ProdWeight { name: "ddl:reindex:table", default: 1.0 },
    ProdWeight { name: "ddl:reindex:concurrent", default: 1.0 },
    ProdWeight { name: "ddl:reindex:plain", default: 3.0 },
    // DDL module: views (create/query-once/drop groups).
    ProdWeight { name: "ddl:view:expr", default: 2.0 },
    ProdWeight { name: "ddl:view:join", default: 1.0 },
    ProdWeight { name: "ddl:view:star", default: 2.0 },
    ProdWeight { name: "ddl:view:count", default: 1.0 },
    // DDL module: sequences.
    ProdWeight { name: "ddl:seq:create", default: 2.0 },
    ProdWeight { name: "ddl:seq:use", default: 3.0 },
    ProdWeight { name: "ddl:seq:drop", default: 1.0 },
    ProdWeight { name: "ddl:seq:nextval", default: 3.0 },
    ProdWeight { name: "ddl:seq:currval", default: 1.0 },
    ProdWeight { name: "ddl:seq:setval", default: 1.0 },
    // DDL module: CREATE AGGREGATE variants (commutative sfuncs only).
    ProdWeight { name: "ddl:agg:sum4", default: 2.0 },
    ProdWeight { name: "ddl:agg:max4", default: 1.0 },
    ProdWeight { name: "ddl:agg:min4", default: 1.0 },
    ProdWeight { name: "ddl:agg:sum8", default: 1.0 },
    // DDL module: triggers (plpgsql; deterministic bodies).
    ProdWeight { name: "ddl:trigger:create", default: 3.0 },
    ProdWeight { name: "ddl:trigger:drop", default: 1.0 },
    ProdWeight { name: "ddl:trigger:before_row", default: 2.0 },
    ProdWeight { name: "ddl:trigger:after_row", default: 1.0 },
    ProdWeight { name: "ddl:trigger:stmt", default: 1.0 },
    ProdWeight { name: "ddl:trigger:insert", default: 2.0 },
    ProdWeight { name: "ddl:trigger:update", default: 1.5 },
    ProdWeight { name: "ddl:trigger:insupd", default: 1.0 },
    ProdWeight { name: "ddl:trigger:delete", default: 1.0 },
    // DDL module: TRUNCATE form.
    ProdWeight { name: "ddl:truncate:restart", default: 1.0 },
    ProdWeight { name: "ddl:truncate:plain", default: 3.0 },
    // T1 formatting productions (crate::rich): generated format pictures.
    ProdWeight { name: "fmt:to_char_num", default: 1.5 },
    ProdWeight { name: "fmt:to_char_dt", default: 1.5 },
    ProdWeight { name: "fmt:to_date_rt", default: 1.0 },
    ProdWeight { name: "fmt:to_timestamp_rt", default: 1.0 },
    ProdWeight { name: "fmt:to_number_rt", default: 0.5 },
    ProdWeight { name: "fmt:extract", default: 1.0 },
    // T1 json/jsonb productions.
    ProdWeight { name: "jsonb:composite", default: 2.0 },
    ProdWeight { name: "jsonb:arrow_text", default: 1.0 },
    ProdWeight { name: "jsonb:bool", default: 1.0 },
    // T1 full-text productions (one surface lights the stemmer family).
    ProdWeight { name: "ts:vector_text", default: 1.0 },
    ProdWeight { name: "ts:query_text", default: 0.7 },
    ProdWeight { name: "ts:match", default: 1.0 },
    ProdWeight { name: "ts:rank", default: 1.0 },
    ProdWeight { name: "ts:headline", default: 0.7 },

    // tsrank module (FTS-RANK lane): ranking / headline / tsvector-op /
    // ts_stat / tsquery-op probes over literal tsvector/tsquery inputs.
    ProdWeight { name: "tsr:rank", default: 1.0 },
    ProdWeight { name: "tsr:rankcd", default: 1.0 },
    ProdWeight { name: "tsr:headline", default: 1.0 },
    ProdWeight { name: "tsr:vecop", default: 1.0 },
    ProdWeight { name: "tsr:tsqop", default: 1.0 },
    ProdWeight { name: "tsr:stat", default: 0.8 },
    ProdWeight { name: "tsr:rewrite", default: 1.0 },

    // tsdl module (T2 text-search DDL + language probes).
    // LD3 stemmer drain: exhaustive among-table suffix sweep across every
    // shipped snowball dictionary (stem_data.rs cursor walk).
    ProdWeight { name: "tsdl:stemdrain", default: 2.0 },
    ProdWeight { name: "tsdl:stemvocab", default: 2.0 },
    ProdWeight { name: "tsdl:probe", default: 3.0 },
    ProdWeight { name: "tsdl:debug", default: 2.0 },
    ProdWeight { name: "tsdl:lexize", default: 1.0 },
    ProdWeight { name: "tsdl:create_cfg", default: 2.0 },
    ProdWeight { name: "tsdl:create_dict", default: 1.5 },
    ProdWeight { name: "tsdl:alter_mapping", default: 2.0 },
    ProdWeight { name: "tsdl:alter_dict", default: 1.0 },
    ProdWeight { name: "tsdl:comment", default: 0.5 },
    ProdWeight { name: "tsdl:rename", default: 0.5 },
    ProdWeight { name: "tsdl:drop", default: 1.0 },
    // T1 regexp / bytea / interval / time / date_part.
    ProdWeight { name: "func:regexp_replace", default: 0.7 },
    ProdWeight { name: "func:encode", default: 1.0 },
    ProdWeight { name: "func:date_part", default: 1.0 },
    ProdWeight { name: "func:string_to_array", default: 1.0 },
    ProdWeight { name: "func:regexp_split_arr", default: 0.7 },
    ProdWeight { name: "func:regexp_match", default: 0.7 },
    ProdWeight { name: "bytea:composite", default: 1.0 },
    ProdWeight { name: "interval:composite", default: 1.0 },
    ProdWeight { name: "binop:ts+interval", default: 1.0 },
    ProdWeight { name: "binop:time+interval", default: 1.0 },
    // T1 array productions.
    ProdWeight { name: "arr:int", default: 1.0 },
    ProdWeight { name: "arr:text_composite", default: 1.0 },
    ProdWeight { name: "arr:int_composite", default: 1.0 },
    ProdWeight { name: "arr:any_all", default: 1.0 },
    // T1 types statement module: SRF-in-FROM shape choice.
    ProdWeight { name: "types:unnest", default: 3.0 },
    ProdWeight { name: "types:unnest_ord", default: 1.0 },
    ProdWeight { name: "types:jsonb_each", default: 2.0 },
    ProdWeight { name: "types:jsonb_elements", default: 2.0 },
    ProdWeight { name: "types:split_table", default: 1.0 },
    // G2 jsonfuncs SRF tail: column-driven LATERAL deconstructors
    // (typeof-filtered, fully ordered) and the record-materializing
    // family (jsonb_to_record(set) AS, jsonb_populate_record(set)).
    ProdWeight { name: "types:each_col", default: 2.0 },
    ProdWeight { name: "types:elements_col", default: 2.0 },
    ProdWeight { name: "types:object_keys", default: 1.5 },
    ProdWeight { name: "types:recordset", default: 1.5 },
    ProdWeight { name: "types:populate", default: 1.5 },
    // A2 datetime cross-type matrix module: statement-shape choice.
    // arith/extract/trunc/cast carry the widest C surface; make/bin/age
    // sit lower (error fuel lives inside them at 1/6-1/8).
    ProdWeight { name: "dtm:arith", default: 4.0 },
    ProdWeight { name: "dtm:cmp", default: 1.5 },
    ProdWeight { name: "dtm:overlaps", default: 1.5 },
    ProdWeight { name: "dtm:trunc", default: 2.5 },
    ProdWeight { name: "dtm:extract", default: 3.0 },
    ProdWeight { name: "dtm:bin", default: 1.5 },
    ProdWeight { name: "dtm:age", default: 1.5 },
    ProdWeight { name: "dtm:justify", default: 1.0 },
    ProdWeight { name: "dtm:make", default: 2.0 },
    ProdWeight { name: "dtm:attz", default: 2.5 },
    ProdWeight { name: "dtm:tochar", default: 2.0 },
    ProdWeight { name: "dtm:cast", default: 2.5 },
    // TYPEIO type-I/O + operator residue drain (typeio.rs). Statement-shape
    // choice: the network/mac/mac8/varbit comparison+bitwise arms and the
    // float4 to_char picture matrix carry the widest uncovered surface;
    // numwin (moving-aggregate inverse + in_range) and dtedge (datetime
    // out-of-range error identity) sit alongside.
    ProdWeight { name: "tio:netord", default: 3.0 },
    ProdWeight { name: "tio:mac", default: 2.5 },
    ProdWeight { name: "tio:mac8", default: 3.0 },
    ProdWeight { name: "tio:varbit", default: 3.0 },
    ProdWeight { name: "tio:fmtf4", default: 2.5 },
    ProdWeight { name: "tio:numwin", default: 2.0 },
    ProdWeight { name: "tio:dtedge", default: 1.5 },
    // LIKE lane (like.rs): the LIKE/ILIKE/SIMILAR TO pattern-match drain.
    // basic/ilike/escape/esclit carry the like.c match_text + do_like_escape
    // hollow lines and weigh highest; similar/substr are the regex-surface
    // tail (lower); err is deliberate error fuel (lowest).
    ProdWeight { name: "like:basic", default: 4.0 },
    ProdWeight { name: "like:ilike", default: 3.0 },
    ProdWeight { name: "like:escape", default: 3.5 },
    ProdWeight { name: "like:esclit", default: 3.0 },
    ProdWeight { name: "like:mb", default: 2.5 },
    ProdWeight { name: "like:bytea", default: 2.0 },
    ProdWeight { name: "like:similar", default: 1.5 },
    ProdWeight { name: "like:substr", default: 1.0 },
    ProdWeight { name: "like:col", default: 1.5 },
    ProdWeight { name: "like:err", default: 1.5 },
    // LD3 datetime decode drain (dtmdec.rs): raw-literal permutation
    // casts. The dtm:decode gate sits alongside the matrix shapes; the
    // dtmdec:* subs split the six type targets, the deterministic-special
    // and style-bracket shapes, and the malformed-literal error fuel.
    ProdWeight { name: "dtm:decode", default: 3.0 },
    // dtx (datetime-extra): the surface dtm's scalar SELECTs cannot reach.
    ProdWeight { name: "dtx:gseries", default: 2.0 },
    ProdWeight { name: "dtx:isfinite", default: 1.5 },
    ProdWeight { name: "dtx:typmod", default: 3.0 },
    ProdWeight { name: "dtx:tzfunc", default: 2.5 },
    ProdWeight { name: "dtx:epoch", default: 1.5 },
    ProdWeight { name: "dtmdec:date", default: 2.0 },
    ProdWeight { name: "dtmdec:time", default: 2.0 },
    ProdWeight { name: "dtmdec:timetz", default: 1.5 },
    ProdWeight { name: "dtmdec:ts", default: 2.5 },
    ProdWeight { name: "dtmdec:tstz", default: 2.0 },
    ProdWeight { name: "dtmdec:interval", default: 2.5 },
    ProdWeight { name: "dtmdec:special", default: 1.0 },
    ProdWeight { name: "dtmdec:style", default: 1.0 },
    ProdWeight { name: "dtmdec:err", default: 1.0 },
    // LD10 residual-arm shapes (adt-datetime residue: *_part_common,
    // *_trunc, interval arithmetic, OVERLAPS).
    ProdWeight { name: "dtmdec:extract", default: 2.0 },
    ProdWeight { name: "dtmdec:trunc", default: 1.5 },
    ProdWeight { name: "dtmdec:ivarith", default: 1.5 },
    ProdWeight { name: "dtmdec:overlaps", default: 1.0 },
    // EXPLAIN module: wrapped-statement source (all read-only SELECT
    // producers — ANALYZE-safety invariant), ANALYZE at LOW weight
    // (executes the query), VERBOSE, output format mix.
    ProdWeight { name: "explain:src:expr", default: 2.0 },
    ProdWeight { name: "explain:src:joins", default: 2.0 },
    ProdWeight { name: "explain:src:subq", default: 1.5 },
    ProdWeight { name: "explain:src:agg", default: 1.5 },
    ProdWeight { name: "explain:src:win", default: 1.0 },
    ProdWeight { name: "explain:plain", default: 6.0 },
    ProdWeight { name: "explain:analyze", default: 1.0 },
    // G2: BUFFERS under ANALYZE (counters masked by the differ; the line
    // structure is the compared surface — show_buffer_usage).
    ProdWeight { name: "explain:buffers:on", default: 1.0 },
    ProdWeight { name: "explain:buffers:off", default: 1.0 },
    ProdWeight { name: "explain:verbose", default: 1.0 },
    ProdWeight { name: "explain:verbose:none", default: 2.0 },
    ProdWeight { name: "explain:fmt:text", default: 4.0 },
    ProdWeight { name: "explain:fmt:json", default: 1.0 },
    ProdWeight { name: "explain:fmt:yaml", default: 1.0 },
    // Utility module: statement shape (CHECKPOINT very low weight —
    // expensive and coverage-flat after the first few), comment text
    // form, PREPARE execute-count variant (many = 6 crosses the
    // plancache generic-plan flip at 5).
    ProdWeight { name: "util:set", default: 4.0 },
    ProdWeight { name: "util:reset", default: 1.5 },
    ProdWeight { name: "util:reset_all", default: 1.0 },
    ProdWeight { name: "util:show", default: 1.5 },
    ProdWeight { name: "util:discard:plans", default: 0.7 },
    ProdWeight { name: "util:discard:sequences", default: 0.3 },
    ProdWeight { name: "util:vacuum", default: 1.0 },
    ProdWeight { name: "util:analyze", default: 1.0 },
    ProdWeight { name: "util:checkpoint", default: 0.1 },
    ProdWeight { name: "util:comment:table", default: 1.0 },
    ProdWeight { name: "util:comment:column", default: 1.0 },
    ProdWeight { name: "util:prepare", default: 2.5 },
    ProdWeight { name: "util:prepare:once", default: 2.0 },
    ProdWeight { name: "util:prepare:many", default: 1.0 },
    ProdWeight { name: "util:comment:text", default: 3.0 },
    ProdWeight { name: "util:comment:null", default: 1.0 },
    // Utility module (X1): deterministic system-view probes (pg_locks,
    // pg_stat_activity-shaped views; volatile columns projected away).
    ProdWeight { name: "util:sysview", default: 1.5 },
    // Partitioning module: action mix (prune SELECTs cheap and high-value;
    // drop low so parents live long enough for other modules to hit them).
    ProdWeight { name: "part:create", default: 3.0 },
    ProdWeight { name: "part:drop", default: 0.7 },
    ProdWeight { name: "part:detach_attach", default: 1.5 },
    ProdWeight { name: "part:detach_drop", default: 0.7 },
    ProdWeight { name: "part:prune", default: 3.0 },
    ProdWeight { name: "part:child_select", default: 1.0 },
    // Partitioning module: parent kind (range_kint highest: nullable
    // mutable key = cross-partition UPDATE moves, the classic bug surface).
    ProdWeight { name: "part:kind:range_pk", default: 2.0 },
    ProdWeight { name: "part:kind:range_kint", default: 3.0 },
    ProdWeight { name: "part:kind:range_multi", default: 1.0 },
    ProdWeight { name: "part:kind:list_text", default: 1.5 },
    ProdWeight { name: "part:kind:list_expr", default: 1.5 },
    ProdWeight { name: "part:kind:hash_pk", default: 1.5 },
    // Partitioning module: partition count per parent.
    ProdWeight { name: "part:parts:2", default: 3.0 },
    ProdWeight { name: "part:parts:3", default: 2.0 },
    ProdWeight { name: "part:parts:4", default: 1.0 },
    // Partitioning module: DETACH form (CONCURRENTLY only offered where no
    // DEFAULT partition exists — Postgres forbids the combination).
    ProdWeight { name: "part:detach:concurrent", default: 1.0 },
    ProdWeight { name: "part:detach:plain", default: 3.0 },
    // Partitioning module: pruning predicate and statement shapes.
    ProdWeight { name: "part:prune:eq", default: 3.0 },
    ProdWeight { name: "part:prune:range", default: 2.0 },
    ProdWeight { name: "part:prune:in", default: 1.5 },
    ProdWeight { name: "part:prune:null", default: 0.7 },
    ProdWeight { name: "part:prune:select", default: 3.0 },
    ProdWeight { name: "part:prune:count", default: 2.0 },
    ProdWeight { name: "part:prune:explain", default: 1.5 },
    // Object/utility DDL module (U1): statement shapes.
    ProdWeight { name: "objddl:comment", default: 4.0 },
    ProdWeight { name: "objddl:describe", default: 3.0 },
    ProdWeight { name: "objddl:role:create", default: 2.0 },
    ProdWeight { name: "objddl:role:alter", default: 1.5 },
    ProdWeight { name: "objddl:role:grant", default: 1.5 },
    ProdWeight { name: "objddl:role:setrole", default: 1.5 },
    ProdWeight { name: "objddl:role:drop", default: 1.0 },
    ProdWeight { name: "objddl:type:create", default: 2.5 },
    ProdWeight { name: "objddl:type:alter_enum", default: 1.5 },
    ProdWeight { name: "objddl:type:use", default: 2.5 },
    ProdWeight { name: "objddl:type:coltab", default: 1.0 },
    ProdWeight { name: "objddl:type:drop", default: 1.0 },
    ProdWeight { name: "objddl:stats:create", default: 2.0 },
    ProdWeight { name: "objddl:stats:drop", default: 0.7 },
    // Q6 opclass-ddl chunk: opclass/opfamily member lists over every AM,
    // access methods, casts, conversions, languages, transform/seclabel
    // error surfaces, shell operators, aggregate FINALFUNC_MODIFY.
    ProdWeight { name: "objddl:opc", default: 3.0 },
    ProdWeight { name: "objddl:opcerr", default: 1.0 },
    ProdWeight { name: "objddl:am", default: 1.5 },
    ProdWeight { name: "objddl:cast", default: 1.5 },
    ProdWeight { name: "objddl:conv", default: 1.2 },
    ProdWeight { name: "objddl:plang", default: 1.0 },
    ProdWeight { name: "objddl:xform", default: 0.6 },
    ProdWeight { name: "objddl:seclabel", default: 0.6 },
    ProdWeight { name: "objddl:opshell", default: 1.0 },
    ProdWeight { name: "objddl:aggmod", default: 1.2 },
    ProdWeight { name: "objddl:opc:bt", default: 2.5 },
    ProdWeight { name: "objddl:opc:hash", default: 1.5 },
    ProdWeight { name: "objddl:opc:gist", default: 2.0 },
    ProdWeight { name: "objddl:opc:gin", default: 1.2 },
    ProdWeight { name: "objddl:opc:spgist", default: 1.2 },
    ProdWeight { name: "objddl:opc:brin", default: 1.2 },
    ProdWeight { name: "objddl:opc:fam", default: 2.0 },
    ProdWeight { name: "objddl:opc:nofam", default: 1.0 },
    ProdWeight { name: "objddl:opc:sort", default: 1.5 },
    ProdWeight { name: "objddl:opc:nosort", default: 1.0 },
    ProdWeight { name: "objddl:opc:eqimg", default: 1.5 },
    ProdWeight { name: "objddl:opc:noeqimg", default: 1.0 },
    ProdWeight { name: "objddl:opc:xtype", default: 2.0 },
    ProdWeight { name: "objddl:opc:noxtype", default: 1.0 },
    ProdWeight { name: "objddl:opc:index", default: 1.5 },
    ProdWeight { name: "objddl:opc:noindex", default: 1.5 },
    ProdWeight { name: "objddl:opc:rename", default: 1.0 },
    ProdWeight { name: "objddl:opc:norename", default: 2.0 },
    ProdWeight { name: "objddl:opc:dist", default: 1.5 },
    ProdWeight { name: "objddl:opc:nodist", default: 1.0 },
    ProdWeight { name: "objddl:opcerr:sig", default: 1.0 },
    ProdWeight { name: "objddl:opcerr:default", default: 1.0 },
    ProdWeight { name: "objddl:opcerr:member", default: 1.0 },
    ProdWeight { name: "objddl:opcerr:nosuch", default: 1.0 },
    ProdWeight { name: "objddl:opcerr:incomplete", default: 1.5 },
    ProdWeight { name: "objddl:am:index", default: 2.0 },
    ProdWeight { name: "objddl:am:table", default: 2.0 },
    ProdWeight { name: "objddl:am:part", default: 1.0 },
    ProdWeight { name: "objddl:am:err", default: 0.7 },
    ProdWeight { name: "objddl:cast:err", default: 0.5 },
    ProdWeight { name: "objddl:cast:ok", default: 3.0 },
    ProdWeight { name: "objddl:cast:implicit", default: 1.0 },
    ProdWeight { name: "objddl:cast:plain", default: 2.0 },
    ProdWeight { name: "objddl:cast:fn", default: 1.5 },
    ProdWeight { name: "objddl:cast:nofn", default: 1.5 },
    ProdWeight { name: "objddl:conv:err", default: 0.5 },
    ProdWeight { name: "objddl:conv:ok", default: 3.0 },
    ProdWeight { name: "objddl:conv:default", default: 1.0 },
    ProdWeight { name: "objddl:conv:plain", default: 2.0 },
    ProdWeight { name: "objddl:plang:err", default: 0.5 },
    ProdWeight { name: "objddl:plang:ok", default: 3.0 },
    ProdWeight { name: "objddl:seclabel:provider", default: 1.0 },
    ProdWeight { name: "objddl:seclabel:plain", default: 2.0 },
    ProdWeight { name: "objddl:opshell:neg", default: 1.5 },
    ProdWeight { name: "objddl:opshell:noneg", default: 1.0 },
    ProdWeight { name: "objddl:aggmod:plain", default: 2.0 },
    ProdWeight { name: "objddl:aggmod:moving", default: 1.5 },
    ProdWeight { name: "objddl:aggmod:err", default: 0.5 },
    // objddl: COMMENT ON object kind.
    ProdWeight { name: "objddl:comment:table", default: 2.0 },
    ProdWeight { name: "objddl:comment:column", default: 2.0 },
    ProdWeight { name: "objddl:comment:index", default: 1.0 },
    ProdWeight { name: "objddl:comment:sequence", default: 1.0 },
    ProdWeight { name: "objddl:comment:function", default: 1.0 },
    ProdWeight { name: "objddl:comment:type", default: 1.5 },
    ProdWeight { name: "objddl:comment:schema", default: 0.7 },
    ProdWeight { name: "objddl:comment:constraint", default: 1.0 },
    ProdWeight { name: "objddl:comment:am", default: 0.7 },
    ProdWeight { name: "objddl:comment:role", default: 1.0 },
    ProdWeight { name: "objddl:comment:language", default: 0.5 },
    // objddl: comment text vs removal.
    ProdWeight { name: "objddl:comment:text", default: 3.0 },
    ProdWeight { name: "objddl:comment:null", default: 1.0 },
    // objddl: CREATE TYPE kind (base is deliberate matched-error fuel for
    // DefineType's option parsing — both engines reject identically; token
    // weight per the both-sides-error budget rule).
    ProdWeight { name: "objddl:type:enum", default: 2.0 },
    ProdWeight { name: "objddl:type:composite", default: 1.5 },
    ProdWeight { name: "objddl:type:range", default: 1.5 },
    ProdWeight { name: "objddl:type:shell", default: 0.5 },
    ProdWeight { name: "objddl:type:base", default: 0.3 },
    // Parallel-query module (Q1): action mix. Query families heaviest
    // (the payoff is forced-parallel execution over the live table); drop
    // low so a table lives long enough for the massdel -> parallel-vacuum
    // -> recycle cycle to fire repeatedly.
    ProdWeight { name: "par:create", default: 1.5 },
    ProdWeight { name: "par:drop", default: 0.3 },
    ProdWeight { name: "par:agg", default: 4.0 },
    ProdWeight { name: "par:join", default: 4.0 },
    ProdWeight { name: "par:scan", default: 3.0 },
    ProdWeight { name: "par:gm", default: 2.5 },
    ProdWeight { name: "par:debug", default: 3.0 },
    ProdWeight { name: "par:idxbuild", default: 2.0 },
    ProdWeight { name: "par:massdel", default: 2.5 },
    ProdWeight { name: "par:vacuum", default: 2.5 },
    ProdWeight { name: "par:explain", default: 1.5 },
    // par: initial table volume (parallel engagement + TidStore volume
    // vs create-group cost; all tiers stay ANALYZE-exhaustive).
    ProdWeight { name: "par:rows:8000", default: 2.0 },
    ProdWeight { name: "par:rows:16000", default: 2.0 },
    ProdWeight { name: "par:rows:24000", default: 1.0 },
    // par: aggregate statement shapes.
    ProdWeight { name: "par:agg:plain", default: 1.0 },
    ProdWeight { name: "par:agg:group", default: 2.0 },
    ProdWeight { name: "par:agg:filtered", default: 1.5 },
    // par: hash-join shapes and the small-work_mem spill variant.
    ProdWeight { name: "par:join:count", default: 1.5 },
    ProdWeight { name: "par:join:agg", default: 2.0 },
    ProdWeight { name: "par:join:rows", default: 1.5 },
    ProdWeight { name: "par:join:spill", default: 1.0 },
    ProdWeight { name: "par:join:mem", default: 1.0 },
    // par: forced scan strategies.
    ProdWeight { name: "par:scan:idx", default: 1.5 },
    ProdWeight { name: "par:scan:ionly", default: 1.5 },
    ProdWeight { name: "par:scan:bitmap", default: 1.5 },
    // par: debug_parallel_query statement shapes.
    ProdWeight { name: "par:debug:rows", default: 1.5 },
    ProdWeight { name: "par:debug:arrayagg", default: 1.5 },
    ProdWeight { name: "par:debug:distinct", default: 1.0 },
    ProdWeight { name: "par:debug:winagg", default: 1.0 },
    ProdWeight { name: "par:debug:mjoin", default: 1.0 },
    ProdWeight { name: "par:debug:memoize", default: 1.0 },
    // par: parallel index-build AMs and the spill variant.
    ProdWeight { name: "par:ib:btree", default: 2.0 },
    ProdWeight { name: "par:ib:brin", default: 1.5 },
    ProdWeight { name: "par:ib:gin", default: 1.5 },
    ProdWeight { name: "par:ib:w2", default: 1.5 },
    ProdWeight { name: "par:ib:w1", default: 1.0 },
    // par: vacuum forms (parallel heaviest — the chunk's modal detail).
    ProdWeight { name: "par:vac:parallel", default: 3.0 },
    ProdWeight { name: "par:vac:plain", default: 1.0 },
    ProdWeight { name: "par:vac:analyze", default: 1.0 },
    // par: EXPLAIN plan-shape probes.
    ProdWeight { name: "par:ex:agg", default: 1.0 },
    ProdWeight { name: "par:ex:join", default: 1.0 },
    ProdWeight { name: "par:ex:gm", default: 1.0 },
    // Index-AM module: action mix (query heaviest — the create groups are
    // bulky and the payoff is data-driven index scans; drop low so tables
    // live long enough to be churned and queried).
    ProdWeight { name: "idx:create", default: 2.0 },
    ProdWeight { name: "idx:drop", default: 0.4 },
    ProdWeight { name: "idx:query", default: 5.0 },
    ProdWeight { name: "idx:explain", default: 1.5 },
    ProdWeight { name: "idx:knn", default: 1.5 },
    ProdWeight { name: "idx:churn", default: 3.0 },
    // Index-AM module: initial table volume (splits need volume; 1800 rows
    // over ~6-15 indexes is the heavy tier).
    ProdWeight { name: "idx:rows:400", default: 2.0 },
    ProdWeight { name: "idx:rows:900", default: 2.0 },
    ProdWeight { name: "idx:rows:1800", default: 1.0 },
    // Index-AM module: optional index shapes beyond the always-on core
    // set (each an independent include-vs-skip pick against idx:x:skip).
    ProdWeight { name: "idx:x:kdpt", default: 1.0 },
    ProdWeight { name: "idx:x:spgrange", default: 1.5 },
    ProdWeight { name: "idx:x:spginet", default: 1.5 },
    ProdWeight { name: "idx:x:gistpt", default: 1.0 },
    ProdWeight { name: "idx:x:gistrange", default: 1.0 },
    ProdWeight { name: "idx:x:gisttsv", default: 1.0 },
    ProdWeight { name: "idx:x:ginarr", default: 1.0 },
    ProdWeight { name: "idx:x:gintsv", default: 1.0 },
    ProdWeight { name: "idx:x:ginpath", default: 1.0 },
    ProdWeight { name: "idx:x:brints", default: 1.0 },
    ProdWeight { name: "idx:x:brininet", default: 1.0 },
    ProdWeight { name: "idx:x:hashtxt", default: 1.0 },
    ProdWeight { name: "idx:x:hashint", default: 1.0 },
    ProdWeight { name: "idx:x:btmulti", default: 0.7 },
    ProdWeight { name: "idx:x:partial", default: 1.0 },
    ProdWeight { name: "idx:x:expr", default: 0.7 },
    ProdWeight { name: "idx:x:skip", default: 1.5 },
    // Index-AM module: query family mix.
    ProdWeight { name: "idx:fam:point", default: 2.0 },
    ProdWeight { name: "idx:fam:box", default: 1.5 },
    ProdWeight { name: "idx:fam:boxdir", default: 1.2 },
    ProdWeight { name: "idx:fam:range", default: 1.5 },
    ProdWeight { name: "idx:fam:inet", default: 1.5 },
    ProdWeight { name: "idx:fam:tsv", default: 2.0 },
    ProdWeight { name: "idx:fam:jsonb", default: 2.0 },
    ProdWeight { name: "idx:fam:array", default: 1.5 },
    ProdWeight { name: "idx:fam:brin", default: 1.5 },
    ProdWeight { name: "idx:fam:hash", default: 1.0 },
    // Index-AM module: query shape and bitmap-scan toggle (bitmap-off only
    // offered on amgettuple families).
    ProdWeight { name: "idx:q:rows", default: 3.0 },
    ProdWeight { name: "idx:q:count", default: 2.0 },
    ProdWeight { name: "idx:bitmap:off", default: 1.0 },
    ProdWeight { name: "idx:bitmap:on", default: 2.0 },
    // Index-AM module: churn form (inserts and updates are the split fuel).
    // geo module (A1): action mix — scalar/cast/query/calc dominate (the
    // operator-matrix statements are the geo_ops.c fuel); create/drop keep
    // a table population alive for the column-based shapes.
    ProdWeight { name: "geo:create", default: 1.5 },
    ProdWeight { name: "geo:drop", default: 0.3 },
    ProdWeight { name: "geo:scalar", default: 6.0 },
    ProdWeight { name: "geo:cast", default: 2.0 },
    ProdWeight { name: "geo:query", default: 4.0 },
    ProdWeight { name: "geo:calc", default: 4.0 },
    ProdWeight { name: "geo:knn", default: 2.0 },
    ProdWeight { name: "geo:join", default: 1.5 },
    ProdWeight { name: "geo:churn", default: 2.0 },
    // Data-table row volume.
    ProdWeight { name: "geo:rows:120", default: 2.0 },
    ProdWeight { name: "geo:rows:300", default: 2.0 },
    ProdWeight { name: "geo:rows:700", default: 1.0 },
    // Literal specials: NaN/Infinity/huge coordinates (verified identical
    // both engines) vs plain structured coordinates.
    ProdWeight { name: "geo:lit:nan", default: 0.5 },
    ProdWeight { name: "geo:lit:plain", default: 6.0 },
    // Scalar expression classes over the operator matrix.
    ProdWeight { name: "geo:sc:bool", default: 3.0 },
    ProdWeight { name: "geo:sc:dist", default: 2.5 },
    ProdWeight { name: "geo:sc:arith", default: 2.0 },
    ProdWeight { name: "geo:sc:closest", default: 1.5 },
    ProdWeight { name: "geo:sc:inter", default: 1.5 },
    ProdWeight { name: "geo:sc:prefix", default: 1.5 },
    ProdWeight { name: "geo:sc:func", default: 3.0 },
    // Predicate column family.
    ProdWeight { name: "geo:fam:pt", default: 2.0 },
    ProdWeight { name: "geo:fam:ln", default: 1.0 },
    ProdWeight { name: "geo:fam:sg", default: 1.5 },
    ProdWeight { name: "geo:fam:bx", default: 2.0 },
    ProdWeight { name: "geo:fam:pa", default: 1.0 },
    ProdWeight { name: "geo:fam:pgn", default: 2.0 },
    ProdWeight { name: "geo:fam:cr", default: 2.0 },
    // Query shape.
    ProdWeight { name: "geo:q:rows", default: 3.0 },
    ProdWeight { name: "geo:q:count", default: 2.0 },
    ProdWeight { name: "geo:seqscan:off", default: 2.0 },
    ProdWeight { name: "geo:seqscan:on", default: 3.0 },
    // Churn forms.
    ProdWeight { name: "geo:churn:update", default: 3.0 },
    ProdWeight { name: "geo:churn:delete", default: 2.0 },
    ProdWeight { name: "geo:churn:insert", default: 3.0 },
    ProdWeight { name: "geo:churn:reindex", default: 1.5 },
    ProdWeight { name: "geo:churn:vacuum", default: 1.5 },
    ProdWeight { name: "idx:churn:update", default: 3.0 },
    ProdWeight { name: "idx:churn:delete", default: 2.0 },
    ProdWeight { name: "idx:churn:insert", default: 3.0 },
    ProdWeight { name: "idx:churn:reindex", default: 1.5 },
    ProdWeight { name: "idx:churn:vacuum", default: 1.5 },
    // B2: btree page-deletion depth family (module-level action shares
    // sized so the original fz_iam_* surface keeps ~2/3 of the module).
    ProdWeight { name: "idx:bt", default: 2.5 },
    ProdWeight { name: "idx:adv", default: 2.0 },
    ProdWeight { name: "idx:stats", default: 2.0 },
    ProdWeight { name: "idx:gist", default: 2.5 },
    // Q6 gist-paths family (fz_gst_* rich-GiST tables + self-contained
    // buffered-build / exclusion-constraint groups).
    ProdWeight { name: "idx:gist:create", default: 2.0 },
    ProdWeight { name: "idx:gist:query", default: 5.0 },
    ProdWeight { name: "idx:gist:ios", default: 1.5 },
    ProdWeight { name: "idx:gist:prop", default: 1.0 },
    ProdWeight { name: "idx:gist:delvac", default: 2.5 },
    ProdWeight { name: "idx:gist:reinsert", default: 1.5 },
    ProdWeight { name: "idx:gist:buffered", default: 0.8 },
    ProdWeight { name: "idx:gist:excl", default: 1.2 },
    ProdWeight { name: "idx:gist:drop", default: 0.3 },
    ProdWeight { name: "idx:gist:unlogged", default: 1.0 },
    ProdWeight { name: "idx:gist:logged", default: 2.0 },
    ProdWeight { name: "idx:gist:rows:1200", default: 2.0 },
    ProdWeight { name: "idx:gist:rows:2400", default: 1.0 },
    ProdWeight { name: "idx:gist:x:ts", default: 1.5 },
    ProdWeight { name: "idx:gist:x:tstz", default: 1.5 },
    ProdWeight { name: "idx:gist:x:date", default: 1.5 },
    ProdWeight { name: "idx:gist:x:ff", default: 1.0 },
    ProdWeight { name: "idx:gist:x:skip", default: 1.0 },
    ProdWeight { name: "idx:gist:buffered:off", default: 1.0 },
    ProdWeight { name: "idx:gist:excl:adj", default: 1.0 },
    ProdWeight { name: "idx:gq:r8", default: 2.0 },
    ProdWeight { name: "idx:gq:num", default: 1.5 },
    ProdWeight { name: "idx:gq:ts", default: 2.0 },
    ProdWeight { name: "idx:gq:mr", default: 2.0 },
    ProdWeight { name: "idx:gq:tsq", default: 2.0 },
    ProdWeight { name: "idx:gq:inet", default: 2.0 },
    ProdWeight { name: "idx:gq:tsv", default: 1.5 },
    ProdWeight { name: "idx:gq:boxdir", default: 1.5 },
    ProdWeight { name: "idx:gq:knnbox", default: 1.0 },
    ProdWeight { name: "idx:gq:plain", default: 4.0 },
    ProdWeight { name: "idx:gq:gtsv", default: 0.2 },
    ProdWeight { name: "idx:gq:pred", default: 6.0 },
    ProdWeight { name: "idx:gq:rows", default: 3.0 },
    ProdWeight { name: "idx:gq:count", default: 2.0 },
    ProdWeight { name: "idx:x:spgbox", default: 1.2 },
    ProdWeight { name: "idx:bt:create", default: 2.0 },
    ProdWeight { name: "idx:bt:massdel", default: 1.5 },
    ProdWeight { name: "idx:bt:delvac", default: 4.0 },
    ProdWeight { name: "idx:bt:vacuum", default: 2.5 },
    ProdWeight { name: "idx:bt:recycle", default: 2.0 },
    ProdWeight { name: "idx:bt:query", default: 3.0 },
    ProdWeight { name: "idx:bt:drop", default: 0.3 },
    ProdWeight { name: "idx:bt:shape:int", default: 2.0 },
    ProdWeight { name: "idx:bt:shape:text", default: 1.0 },
    ProdWeight { name: "idx:bt:rows:3000", default: 2.0 },
    ProdWeight { name: "idx:bt:rows:6000", default: 1.0 },
    ProdWeight { name: "idx:bt:q:pkrange", default: 2.0 },
    ProdWeight { name: "idx:bt:q:pkcount", default: 1.5 },
    ProdWeight { name: "idx:bt:q:sec", default: 2.0 },
    ProdWeight { name: "idx:bt:q:seccount", default: 1.5 },
    // B2: adversarial picksplit family.
    ProdWeight { name: "idx:adv:create", default: 2.0 },
    ProdWeight { name: "idx:adv:insert", default: 3.0 },
    ProdWeight { name: "idx:adv:query", default: 4.0 },
    ProdWeight { name: "idx:adv:churn", default: 2.5 },
    ProdWeight { name: "idx:adv:drop", default: 0.3 },
    ProdWeight { name: "idx:adv:ins:same", default: 2.0 },
    ProdWeight { name: "idx:adv:ins:line", default: 1.5 },
    ProdWeight { name: "idx:adv:ins:big", default: 1.5 },
    ProdWeight { name: "idx:adv:q:box", default: 2.0 },
    ProdWeight { name: "idx:adv:q:left", default: 1.5 },
    ProdWeight { name: "idx:adv:q:txteq", default: 1.5 },
    ProdWeight { name: "idx:adv:q:txtlt", default: 0.7 },
    ProdWeight { name: "idx:adv:q:poly", default: 1.5 },
    ProdWeight { name: "idx:adv:q:knn", default: 1.5 },
    ProdWeight { name: "idx:adv:q:explain", default: 1.0 },
    ProdWeight { name: "idx:adv:churn:delete", default: 2.0 },
    ProdWeight { name: "idx:adv:churn:vacuum", default: 2.0 },
    ProdWeight { name: "idx:adv:churn:reindex", default: 1.0 },
    // B2: statistics-import family.
    ProdWeight { name: "idx:stats:rel", default: 2.0 },
    ProdWeight { name: "idx:stats:attr", default: 3.0 },
    ProdWeight { name: "idx:stats:clear", default: 1.5 },
    ProdWeight { name: "idx:stats:probe", default: 3.0 },
    ProdWeight { name: "idx:stats:err", default: 1.0 },
    ProdWeight { name: "idx:stats:attr:full", default: 2.0 },
    ProdWeight { name: "idx:stats:attr:min", default: 1.5 },
    ProdWeight { name: "idx:stats:attr:hist", default: 1.5 },
    ProdWeight { name: "idx:stats:err:mismatch", default: 1.0 },
    ProdWeight { name: "idx:stats:err:badlit", default: 1.0 },
    ProdWeight { name: "idx:stats:err:range", default: 1.0 },
    ProdWeight { name: "idx:stats:err:negpages", default: 1.0 },
    // Cursor / SQL-PREPARE module (C1): group shape first. Bracket
    // (DECLARE + FETCH/MOVE run inside a transaction) is the bread and
    // butter; held/wco are the materialization and CURRENT-OF surfaces;
    // execute above prepare so pooled statements accumulate executions
    // (the plancache generic-plan flip needs 6 on one statement).
    ProdWeight { name: "cursor:bracket", default: 4.0 },
    ProdWeight { name: "cursor:wco", default: 1.5 },
    ProdWeight { name: "cursor:held", default: 2.0 },
    ProdWeight { name: "cursor:prepare", default: 2.0 },
    ProdWeight { name: "cursor:execute", default: 3.0 },
    ProdWeight { name: "cursor:dealloc", default: 0.7 },
    // DECLARE options. SCROLL carries the backward-direction surface, so
    // it outweighs the forward-only spellings; NO SCROLL exists mainly to
    // host the deliberate 55000.
    ProdWeight { name: "cursor:scroll", default: 4.0 },
    ProdWeight { name: "cursor:scroll:default", default: 2.0 },
    ProdWeight { name: "cursor:noscroll", default: 1.0 },
    ProdWeight { name: "cursor:hold", default: 1.5 },
    ProdWeight { name: "cursor:hold:none", default: 3.0 },
    ProdWeight { name: "cursor:hold:without", default: 1.0 },
    ProdWeight { name: "cursor:binary", default: 1.0 },
    ProdWeight { name: "cursor:binary:none", default: 5.0 },
    ProdWeight { name: "cursor:sens:none", default: 5.0 },
    ProdWeight { name: "cursor:sens:insensitive", default: 1.0 },
    ProdWeight { name: "cursor:sens:asensitive", default: 1.0 },
    ProdWeight { name: "cursor:where", default: 1.0 },
    ProdWeight { name: "cursor:where:none", default: 2.0 },
    ProdWeight { name: "cursor:orderasc", default: 3.0 },
    ProdWeight { name: "cursor:orderdesc", default: 1.0 },
    // FETCH/MOVE directions (DoPortalRunFetch's direction dispatch).
    ProdWeight { name: "cursor:op:next", default: 3.0 },
    ProdWeight { name: "cursor:op:count", default: 2.0 },
    ProdWeight { name: "cursor:op:fwd_n", default: 2.0 },
    ProdWeight { name: "cursor:op:fwd_all", default: 1.0 },
    ProdWeight { name: "cursor:op:rel_fwd", default: 1.0 },
    ProdWeight { name: "cursor:op:prior", default: 2.0 },
    ProdWeight { name: "cursor:op:first", default: 1.0 },
    ProdWeight { name: "cursor:op:last", default: 1.0 },
    ProdWeight { name: "cursor:op:abs", default: 1.5 },
    ProdWeight { name: "cursor:op:rel_back", default: 1.0 },
    ProdWeight { name: "cursor:op:back_n", default: 1.5 },
    ProdWeight { name: "cursor:op:back_all", default: 1.0 },
    ProdWeight { name: "cursor:op:fetch", default: 4.0 },
    ProdWeight { name: "cursor:op:move", default: 1.0 },
    // Matched-error surfaces: low weight (both engines raise identically,
    // so they cost budget without buying divergence signal).
    ProdWeight { name: "cursor:op:invalid_back", default: 1.0 },
    ProdWeight { name: "cursor:op:valid", default: 3.0 },
    ProdWeight { name: "cursor:fetch:dead", default: 1.0 },
    ProdWeight { name: "cursor:fetch:dead:none", default: 4.0 },
    // Bracket ending / cursor lifetime.
    ProdWeight { name: "cursor:close", default: 1.0 },
    ProdWeight { name: "cursor:close:none", default: 2.0 },
    ProdWeight { name: "cursor:close_all", default: 0.5 },
    ProdWeight { name: "cursor:end:commit", default: 3.0 },
    ProdWeight { name: "cursor:end:rollback", default: 1.0 },
    ProdWeight { name: "cursor:post_ops", default: 2.0 },
    ProdWeight { name: "cursor:post_ops:none", default: 1.0 },
    ProdWeight { name: "cursor:held:create", default: 2.0 },
    ProdWeight { name: "cursor:held:use", default: 3.0 },
    ProdWeight { name: "cursor:held:keep", default: 3.0 },
    ProdWeight { name: "cursor:held:close", default: 1.0 },
    // WHERE CURRENT OF forms.
    ProdWeight { name: "cursor:wco:update", default: 3.0 },
    ProdWeight { name: "cursor:wco:update2", default: 1.0 },
    ProdWeight { name: "cursor:wco:delete", default: 1.5 },
    // PREPARE statement kinds and argument arity.
    ProdWeight { name: "cursor:prep:select", default: 4.0 },
    ProdWeight { name: "cursor:prep:update", default: 2.0 },
    ProdWeight { name: "cursor:prep:delete", default: 1.0 },
    ProdWeight { name: "cursor:prep:insert", default: 1.5 },
    ProdWeight { name: "cursor:prep:arg1", default: 3.0 },
    ProdWeight { name: "cursor:prep:arg2", default: 1.0 },
    // EXECUTE variants: burst crosses the plancache custom->generic flip
    // at 5, explain probes which plan the cache chose.
    ProdWeight { name: "cursor:exec:few", default: 4.0 },
    ProdWeight { name: "cursor:exec:burst", default: 2.0 },
    ProdWeight { name: "cursor:exec:explain", default: 1.5 },
    ProdWeight { name: "cursor:exec:badargs", default: 0.5 },
    ProdWeight { name: "cursor:exec:hit", default: 3.0 },
    ProdWeight { name: "cursor:exec:miss", default: 1.0 },
    ProdWeight { name: "cursor:exec:fresh", default: 4.0 },
    ProdWeight { name: "cursor:exec:collide", default: 1.0 },
    ProdWeight { name: "cursor:dealloc:one", default: 3.0 },
    ProdWeight { name: "cursor:dealloc:all", default: 1.0 },
    // Views/rules module: action mix. create/replace keep registered views
    // in the stream (the cross-module DML surface that drives
    // rewriteTargetView); drop stays low so views live long enough for the
    // other modules to reach them.
    ProdWeight { name: "views:create", default: 3.0 },
    ProdWeight { name: "views:replace", default: 1.5 },
    ProdWeight { name: "views:drop", default: 0.7 },
    ProdWeight { name: "views:nested", default: 2.0 },
    ProdWeight { name: "views:readonly", default: 1.5 },
    ProdWeight { name: "views:matview", default: 2.0 },
    ProdWeight { name: "views:rule", default: 3.0 },
    ProdWeight { name: "views:deparse", default: 1.5 },
    // Views module (G2): field-selection deparse group — whole-row/RECORD
    // Var fields in a view's SELECT list + pg_get_viewdef probes
    // (get_name_for_var_field, gap-007 rank 12).
    ProdWeight { name: "views:fieldsel", default: 1.5 },
    ProdWeight { name: "views:fs:wholerow", default: 2.0 },
    ProdWeight { name: "views:fs:recfn", default: 2.0 },
    ProdWeight { name: "views:fs:nestedrec", default: 1.5 },
    ProdWeight { name: "views:fs:anonrec", default: 1.5 },
    ProdWeight { name: "views:fs:star", default: 1.5 },
    // Views module: the registered view's qual. The hiding qual is the
    // interesting one (it makes CASCADED check options bite on the parent),
    // but it never carries a check option itself.
    ProdWeight { name: "views:qual:none", default: 2.0 },
    ProdWeight { name: "views:qual:pk", default: 2.0 },
    ProdWeight { name: "views:qual:hide", default: 2.0 },
    // Views module: WITH CHECK OPTION presence and form.
    ProdWeight { name: "views:wco:on", default: 1.0 },
    ProdWeight { name: "views:wco:none", default: 2.0 },
    ProdWeight { name: "views:wco:local", default: 2.0 },
    ProdWeight { name: "views:wco:cascaded", default: 2.0 },
    // Views module: TEMP views (relpersistence surface in the rewriter).
    ProdWeight { name: "views:temp", default: 1.0 },
    ProdWeight { name: "views:plain", default: 4.0 },
    // Views module: nested-view extras (the deliberate check-option
    // violation surface writes on its own).
    ProdWeight { name: "views:nested:update", default: 1.0 },
    ProdWeight { name: "views:nested:delete", default: 1.0 },
    ProdWeight { name: "views:nested:deparse", default: 1.0 },
    // Views module: non-updatable view shapes and their write attempts
    // (matched errors through the auto-updatability analysis).
    ProdWeight { name: "views:ro:agg", default: 2.0 },
    ProdWeight { name: "views:ro:join", default: 2.0 },
    ProdWeight { name: "views:ro:distinct", default: 1.5 },
    ProdWeight { name: "views:ro:limit", default: 1.5 },
    ProdWeight { name: "views:ro:write", default: 2.0 },
    ProdWeight { name: "views:ro:read_only", default: 1.0 },
    ProdWeight { name: "views:ro:deparse", default: 1.0 },
    // Views module: materialized views.
    ProdWeight { name: "views:mv:create", default: 2.0 },
    ProdWeight { name: "views:mv:refresh", default: 3.0 },
    ProdWeight { name: "views:mv:drop", default: 0.7 },
    ProdWeight { name: "views:mv:src:view", default: 1.0 },
    ProdWeight { name: "views:mv:src:table", default: 1.0 },
    ProdWeight { name: "views:mv:nodata", default: 1.0 },
    ProdWeight { name: "views:mv:withdata", default: 3.0 },
    ProdWeight { name: "views:mv:concurrent", default: 1.5 },
    ProdWeight { name: "views:mv:plain", default: 2.0 },
    // Views module: rule-table lifecycle and rule action kinds (the
    // raw_expression_tree_walker surface is the action list itself).
    ProdWeight { name: "views:rule:create", default: 2.0 },
    ProdWeight { name: "views:rule:add", default: 3.0 },
    ProdWeight { name: "views:rule:drop", default: 1.0 },
    ProdWeight { name: "views:rule:ins_also", default: 3.0 },
    ProdWeight { name: "views:rule:ins_also_multi", default: 2.0 },
    ProdWeight { name: "views:rule:ins_also_select", default: 1.5 },
    ProdWeight { name: "views:rule:ins_instead_redirect", default: 1.0 },
    ProdWeight { name: "views:rule:ins_instead_nothing", default: 0.7 },
    ProdWeight { name: "views:rule:upd_cond_also", default: 2.5 },
    ProdWeight { name: "views:rule:upd_cond_instead", default: 1.5 },
    ProdWeight { name: "views:rule:del_instead_nothing", default: 1.0 },
    ProdWeight { name: "views:rule:del_also_nothing", default: 0.7 },
    // Views module: the DML this module drives on its own rule tables.
    ProdWeight { name: "views:rule:dml:insert", default: 3.0 },
    ProdWeight { name: "views:rule:dml:update", default: 2.0 },
    ProdWeight { name: "views:rule:dml:delete", default: 1.0 },
    ProdWeight { name: "views:rule:returning", default: 1.0 },
    // Views module: deparse probe targets and pg_get_viewdef arg forms.
    ProdWeight { name: "views:deparse:view", default: 3.0 },
    ProdWeight { name: "views:deparse:matview", default: 1.0 },
    ProdWeight { name: "views:deparse:rules", default: 2.0 },
    ProdWeight { name: "views:deparse:plain", default: 2.0 },
    ProdWeight { name: "views:deparse:pretty", default: 2.0 },
    ProdWeight { name: "views:deparse:wrap", default: 1.0 },
    ProdWeight { name: "views:deparse:updatable", default: 1.0 },
    // matview module (Track-B complement to `views`): the createas.c /
    // matview.c / view.c arms `views` leaves cold. Action mix — content and
    // match-merge (the HIGH-severity refresh-content surfaces) lead.
    ProdWeight { name: "matview:content", default: 3.0 },
    ProdWeight { name: "matview:scan", default: 1.5 },
    ProdWeight { name: "matview:merge", default: 2.5 },
    ProdWeight { name: "matview:nouniq", default: 1.0 },
    ProdWeight { name: "matview:ctas", default: 2.0 },
    ProdWeight { name: "matview:secview", default: 1.5 },
    ProdWeight { name: "matview:replace", default: 1.5 },
    ProdWeight { name: "matview:recursive", default: 1.5 },
    // matview: content-arm source shapes (join / aggregate / DISTINCT).
    ProdWeight { name: "matview:content:join", default: 2.0 },
    ProdWeight { name: "matview:content:agg", default: 2.0 },
    ProdWeight { name: "matview:content:distinct", default: 1.5 },
    // matview: scannability arm — plain refresh vs the CONCURRENTLY-on-
    // unpopulated error variant.
    ProdWeight { name: "matview:scan:plain", default: 2.0 },
    ProdWeight { name: "matview:scan:concurrent", default: 1.5 },
    // matview: CREATE TABLE AS / SELECT INTO shapes.
    ProdWeight { name: "matview:ctas:data", default: 2.0 },
    ProdWeight { name: "matview:ctas:nodata", default: 1.5 },
    ProdWeight { name: "matview:ctas:into", default: 1.5 },
    ProdWeight { name: "matview:ctas:agg", default: 1.5 },
    // matview: security-option view shapes and whether to write through.
    ProdWeight { name: "matview:sec:barrier", default: 2.0 },
    ProdWeight { name: "matview:sec:invoker", default: 2.0 },
    ProdWeight { name: "matview:sec:both", default: 1.0 },
    ProdWeight { name: "matview:sec:write", default: 1.5 },
    ProdWeight { name: "matview:sec:read", default: 1.0 },
    // matview: recursive-view shapes.
    ProdWeight { name: "matview:rec:count", default: 2.0 },
    ProdWeight { name: "matview:rec:pair", default: 1.5 },
    // adt-misc breadth module (A3): statement shapes. The four families
    // (acl / varbit+varlena / multirange / float-numeric edges) each get
    // their own steering knob; money is its own low-weight surface.
    ProdWeight { name: "adtm:acl:probe", default: 2.5 },
    ProdWeight { name: "adtm:acl:aclitem", default: 1.5 },
    ProdWeight { name: "adtm:acl:grant", default: 2.0 },
    ProdWeight { name: "adtm:acl:denial", default: 1.0 },
    ProdWeight { name: "adtm:bit", default: 2.5 },
    ProdWeight { name: "adtm:vltext", default: 3.0 },
    ProdWeight { name: "adtm:vlbytea", default: 2.0 },
    ProdWeight { name: "adtm:mr", default: 2.5 },
    ProdWeight { name: "adtm:num", default: 3.0 },
    ProdWeight { name: "adtm:money", default: 1.0 },
    // adt-misc error-fuel bias: err arms host deliberate matched errors
    // (both-sides-error budget rule keeps them a small minority).
    ProdWeight { name: "adtm:ok", default: 6.0 },
    ProdWeight { name: "adtm:err", default: 1.0 },
    // Q2 expr-misc-adt breadth shapes (sql-reachable-queue chunk):
    // literal-pool probes weigh like the original families; the bracket
    // shapes (deparse/enum: 4-9 statement groups) sit lower to keep the
    // statement mix balanced; errpath is deliberate-error traffic and
    // stays low per the findings-budget rule.
    ProdWeight { name: "adtm:pseudo", default: 2.5 },
    ProdWeight { name: "adtm:binupg", default: 1.0 },
    ProdWeight { name: "adtm:deparse", default: 1.5 },
    ProdWeight { name: "adtm:rec", default: 2.5 },
    ProdWeight { name: "adtm:uuid", default: 1.5 },
    ProdWeight { name: "adtm:inlist", default: 1.5 },
    ProdWeight { name: "adtm:enum", default: 1.0 },
    ProdWeight { name: "adtm:probe2", default: 2.5 },
    ProdWeight { name: "adtm:errpath", default: 1.0 },
    // sqljson module (J1): the PG16-18 SQL/JSON surface. Query functions +
    // JSON_TABLE + jsonpath item methods dominate (the transformJsonFuncExpr/
    // ExecEvalJsonExprPath/executeDateTimeMethod fuel); err is matched-error
    // fuel and stays a small minority.
    ProdWeight { name: "sqljson:obj", default: 2.5 },
    ProdWeight { name: "sqljson:arr", default: 2.0 },
    ProdWeight { name: "sqljson:agg", default: 2.0 },
    ProdWeight { name: "sqljson:ctor", default: 2.0 },
    ProdWeight { name: "sqljson:exists", default: 2.5 },
    ProdWeight { name: "sqljson:value", default: 3.0 },
    ProdWeight { name: "sqljson:query", default: 3.0 },
    ProdWeight { name: "sqljson:isjson", default: 1.5 },
    ProdWeight { name: "sqljson:table", default: 2.5 },
    ProdWeight { name: "sqljson:pathdt", default: 3.0 },
    ProdWeight { name: "sqljson:pathm", default: 2.5 },
    ProdWeight { name: "sqljson:err", default: 1.0 },
    // Q2 sqljson-tweaks breadth shapes (sql-reachable-queue chunk).
    ProdWeight { name: "sqljson:pop", default: 2.5 },
    ProdWeight { name: "sqljson:jops", default: 2.5 },
    ProdWeight { name: "sqljson:mut", default: 2.0 },
    ProdWeight { name: "sqljson:cast", default: 1.5 },
    ProdWeight { name: "sqljson:jts", default: 1.5 },
    ProdWeight { name: "sqljson:windup", default: 1.0 },
    ProdWeight { name: "sqljson:viewdef", default: 1.5 },
    // jsonpath module: the raw jsonpath execution-engine drain (gap-006
    // jsonpath cluster). The predicate-grammar and variable families carry
    // the most dark C mass (executeBinaryArithmExpr / executeLikeRegex /
    // getJsonPathVariableFromJsonb / jspIsMutableWalker), so they sit above
    // the operator/error fillers.
    ProdWeight { name: "jsonpath:opexists", default: 2.0 },
    ProdWeight { name: "jsonpath:opmatch", default: 2.0 },
    ProdWeight { name: "jsonpath:exists", default: 2.0 },
    ProdWeight { name: "jsonpath:match", default: 2.0 },
    ProdWeight { name: "jsonpath:vars", default: 3.0 },
    ProdWeight { name: "jsonpath:arith", default: 3.0 },
    ProdWeight { name: "jsonpath:cmp", default: 2.5 },
    ProdWeight { name: "jsonpath:likeregex", default: 3.0 },
    ProdWeight { name: "jsonpath:startswith", default: 2.0 },
    ProdWeight { name: "jsonpath:recursive", default: 2.0 },
    ProdWeight { name: "jsonpath:unicode", default: 2.5 },
    ProdWeight { name: "jsonpath:dtcmp", default: 2.0 },
    ProdWeight { name: "jsonpath:mutidx", default: 1.5 },
    ProdWeight { name: "jsonpath:err", default: 1.0 },
    // JSONFUNCS module: the json/jsonb function + operator surface J1's
    // sqljson does not reach (jsonb_op.c operators, jsonfuncs.c expand
    // SRFs + classic aggregates, jsonb subscripting).
    ProdWeight { name: "jsonfuncs:jacc", default: 2.5 },
    ProdWeight { name: "jsonfuncs:contain", default: 2.5 },
    ProdWeight { name: "jsonfuncs:exist", default: 2.5 },
    ProdWeight { name: "jsonfuncs:concat", default: 2.0 },
    ProdWeight { name: "jsonfuncs:each", default: 2.5 },
    ProdWeight { name: "jsonfuncs:elems", default: 2.5 },
    ProdWeight { name: "jsonfuncs:keys", default: 2.0 },
    ProdWeight { name: "jsonfuncs:extract", default: 2.5 },
    ProdWeight { name: "jsonfuncs:meta", default: 2.0 },
    ProdWeight { name: "jsonfuncs:agg", default: 2.5 },
    ProdWeight { name: "jsonfuncs:sub", default: 2.0 },
    ProdWeight { name: "jsonfuncs:err", default: 1.0 },
    // Q2 mbconv module (encoding-conversion production; data tables in
    // mbconv_data.rs are pre-verified on both engines). The ok/err knob
    // biases away from the matched 22P05/22021 arms.
    ProdWeight { name: "mbconv:conv", default: 3.0 },
    ProdWeight { name: "mbconv:from", default: 2.5 },
    ProdWeight { name: "mbconv:to", default: 2.0 },
    ProdWeight { name: "mbconv:len", default: 1.5 },
    ProdWeight { name: "mbconv:ok", default: 6.0 },
    ProdWeight { name: "mbconv:err", default: 1.0 },
    // Q2 xnum module (cross-type numeric operator matrix). rand emits
    // predicate-only compares (engine streams differ after setseed —
    // banked Q2 observation).
    ProdWeight { name: "xnum:xint", default: 3.0 },
    ProdWeight { name: "xnum:bit", default: 2.0 },
    ProdWeight { name: "xnum:money", default: 2.0 },
    ProdWeight { name: "xnum:oid", default: 1.5 },
    ProdWeight { name: "xnum:snap", default: 2.0 },
    ProdWeight { name: "xnum:math", default: 2.5 },
    ProdWeight { name: "xnum:bucket", default: 1.5 },
    ProdWeight { name: "xnum:series", default: 1.5 },
    ProdWeight { name: "xnum:lsn", default: 1.5 },
    ProdWeight { name: "xnum:lit", default: 1.5 },
    ProdWeight { name: "xnum:rand", default: 1.0 },
    ProdWeight { name: "xnum:ok", default: 6.0 },
    ProdWeight { name: "xnum:err", default: 1.0 },
    // Q3 nodes module (debug-print bracket groups): group shapes. The
    // GEN-GAP utility batteries sit slightly above the single-statement
    // arms; the matched-error LOAD arm stays low.
    ProdWeight { name: "nodes:collation", default: 1.0 },
    ProdWeight { name: "nodes:fdw", default: 1.0 },
    ProdWeight { name: "nodes:function", default: 1.5 },
    ProdWeight { name: "nodes:atomic", default: 1.0 },
    ProdWeight { name: "nodes:role", default: 1.0 },
    ProdWeight { name: "nodes:operator", default: 1.0 },
    ProdWeight { name: "nodes:schema", default: 1.0 },
    ProdWeight { name: "nodes:stats", default: 1.0 },
    ProdWeight { name: "nodes:system", default: 0.7 },
    ProdWeight { name: "nodes:lock", default: 1.0 },
    ProdWeight { name: "nodes:constraints", default: 1.0 },
    ProdWeight { name: "nodes:returning", default: 1.0 },
    ProdWeight { name: "nodes:load", default: 0.4 },
    ProdWeight { name: "nodes:storedview", default: 1.5 },
    ProdWeight { name: "nodes:mix", default: 2.0 },
    ProdWeight { name: "nodes:prepared", default: 1.5 },
    ProdWeight { name: "nodes:domain", default: 1.0 },
    ProdWeight { name: "nodes:cluster", default: 0.7 },
    ProdWeight { name: "nodes:do", default: 0.7 },
    // Q6: opclass/opfamily + object-DDL raw trees under the debug-print
    // brackets (outfuncs/queryjumble/copyfuncs of the Q6 statement kinds).
    ProdWeight { name: "nodes:opfam", default: 1.0 },
    ProdWeight { name: "nodes:objextra", default: 1.0 },
    ProdWeight { name: "nodes:objx:am", default: 1.0 },
    ProdWeight { name: "nodes:objx:conv", default: 1.0 },
    ProdWeight { name: "nodes:objx:cast", default: 1.0 },
    ProdWeight { name: "nodes:objx:plang", default: 1.0 },
    ProdWeight { name: "nodes:objx:seclabel", default: 0.7 },
    ProdWeight { name: "nodes:objx:xform", default: 0.7 },
    // Q3 nodes: PREPARE raw-wrapper forms (raw grammar nodes reach
    // outfuncs/queryjumblefuncs only inside utility statements).
    ProdWeight { name: "nodes:prepared:merge", default: 1.0 },
    ProdWeight { name: "nodes:prepared:onconflict", default: 1.0 },
    ProdWeight { name: "nodes:prepared:multiassign", default: 1.0 },
    ProdWeight { name: "nodes:prepared:window", default: 1.0 },
    ProdWeight { name: "nodes:prepared:json", default: 1.0 },
    ProdWeight { name: "nodes:prepared:gsets", default: 1.0 },
    // Q3 nodes: stored-view body forms (readfuncs via pg_rewrite).
    ProdWeight { name: "nodes:storedview:search", default: 1.0 },
    ProdWeight { name: "nodes:storedview:cycle", default: 1.0 },
    ProdWeight { name: "nodes:storedview:gsets", default: 1.0 },
    ProdWeight { name: "nodes:storedview:jsontable", default: 1.0 },
    ProdWeight { name: "nodes:storedview:forupdate", default: 1.0 },
    // Q3 obs module (observability probe groups): group shapes.
    ProdWeight { name: "obs:table", default: 1.5 },
    ProdWeight { name: "obs:xact", default: 1.0 },
    ProdWeight { name: "obs:func", default: 1.0 },
    ProdWeight { name: "obs:db", default: 1.0 },
    ProdWeight { name: "obs:global", default: 1.5 },
    ProdWeight { name: "obs:backend", default: 1.0 },
    ProdWeight { name: "obs:reset", default: 1.0 },
    ProdWeight { name: "obs:snapshot", default: 1.0 },
    ProdWeight { name: "obs:views", default: 1.5 },
    ProdWeight { name: "obs:advisory", default: 0.7 },
    // P2 plpgsql-depth module: group kind.
    ProdWeight { name: "plpg:fn", default: 3.0 },
    ProdWeight { name: "plpg:out", default: 1.0 },
    ProdWeight { name: "plpg:srf", default: 1.5 },
    ProdWeight { name: "plpg:proc", default: 1.0 },
    ProdWeight { name: "plpg:trig", default: 2.5 },
    ProdWeight { name: "plpg:instead", default: 1.0 },
    ProdWeight { name: "plpg:coll", default: 1.5 },
    ProdWeight { name: "plpg:op", default: 1.0 },
    ProdWeight { name: "plpg:opclass", default: 1.0 },
    // P2: scalar-function body templates.
    ProdWeight { name: "plpg:fn:ctrl", default: 2.0 },
    ProdWeight { name: "plpg:fn:loop", default: 2.0 },
    ProdWeight { name: "plpg:fn:strict", default: 1.5 },
    ProdWeight { name: "plpg:fn:exc", default: 1.5 },
    ProdWeight { name: "plpg:fn:dyn", default: 1.0 },
    ProdWeight { name: "plpg:fn:diag", default: 1.0 },
    ProdWeight { name: "plpg:strict:pk", default: 2.0 },
    ProdWeight { name: "plpg:strict:many", default: 1.0 },
    ProdWeight { name: "plpg:strict:ordered", default: 1.0 },
    ProdWeight { name: "plpg:exc:flat", default: 2.0 },
    ProdWeight { name: "plpg:exc:nested", default: 1.0 },
    // P2: SRF and procedure forms.
    ProdWeight { name: "plpg:srf:setof", default: 1.5 },
    ProdWeight { name: "plpg:srf:table", default: 1.0 },
    ProdWeight { name: "plpg:proc:inout", default: 1.0 },
    ProdWeight { name: "plpg:proc:txn", default: 1.0 },
    // P2: collation creation forms.
    ProdWeight { name: "plpg:coll:locale", default: 2.0 },
    ProdWeight { name: "plpg:coll:from", default: 1.0 },
    ProdWeight { name: "plpg:coll:lc_pair", default: 1.0 },
    // P2: opclass access method.
    ProdWeight { name: "plpg:opclass:btree", default: 2.0 },
    ProdWeight { name: "plpg:opclass:hash", default: 1.0 },
    // D1 trigger-depth extension (ddl module): body/feature variants.
    ProdWeight { name: "ddl:trigger:transition", default: 1.0 },
    ProdWeight { name: "ddl:trigger:constraint", default: 0.7 },
    ProdWeight { name: "ddl:tgbody:ret", default: 1.5 },
    ProdWeight { name: "ddl:tgbody:bump", default: 1.5 },
    ProdWeight { name: "ddl:tgbody:tgop", default: 1.0 },
    ProdWeight { name: "ddl:tgbody:args", default: 1.0 },
    ProdWeight { name: "ddl:tgbody:suppress", default: 0.7 },
    // C3: locale-collation module group forms. The table form carries the
    // core sort-order surface (ORDER BY / sorted-array projections), so it
    // keeps the highest share; index buys the abbreviated-key/strxfrm
    // path; range is the narrowest.
    ProdWeight { name: "coll:cmp", default: 2.0 },
    ProdWeight { name: "coll:table", default: 3.0 },
    ProdWeight { name: "coll:like", default: 2.0 },
    ProdWeight { name: "coll:case", default: 2.0 },
    ProdWeight { name: "coll:index", default: 2.0 },
    ProdWeight { name: "coll:range", default: 1.0 },
    // Collation choice inside a group: fresh libc CREATE COLLATION vs a
    // pinned catalog ICU-provider collation ("unicode", the ICU-VERIFY
    // surface). Defaults preserve the historical 1-in-4 pinned share.
    // Rigs whose A side is built --without-icu run the F1 known-divergence
    // surface by keeping pinned on; pass --weight coll:pinned=0 to steer
    // it off for clean sanity legs against a non-ICU reference.
    ProdWeight { name: "coll:fresh", default: 3.0 },
    ProdWeight { name: "coll:pinned", default: 1.0 },
    // Q5 admin-funcs module: statement shapes. Size/pretty carry the wide
    // dbsize surface; backup/summ emit multi-statement brackets with real
    // WAL cost (switch + checkpoint) so they stay at token weights.
    ProdWeight { name: "admin:size", default: 2.5 },
    ProdWeight { name: "admin:pretty", default: 2.5 },
    ProdWeight { name: "admin:file", default: 2.0 },
    ProdWeight { name: "admin:ls", default: 2.0 },
    ProdWeight { name: "admin:wal", default: 2.0 },
    ProdWeight { name: "admin:backup", default: 0.8 },
    ProdWeight { name: "admin:recovery", default: 1.5 },
    ProdWeight { name: "admin:snap", default: 1.5 },
    ProdWeight { name: "admin:sig", default: 1.5 },
    ProdWeight { name: "admin:control", default: 1.5 },
    ProdWeight { name: "admin:summ", default: 0.8 },
    // Q5: admin error-fuel bias (biased away from both-sides-error).
    ProdWeight { name: "admin:ok", default: 6.0 },
    ProdWeight { name: "admin:err", default: 1.0 },
    ProdWeight { name: "objid:address", default: 1.0 },
    ProdWeight { name: "objid:deparse", default: 1.2 },
    ProdWeight { name: "objid:explain", default: 0.8 },
    // Q5 acl-grant extensions to the adtmisc ACL families: full arity
    // matrix, per-object-class grant brackets (language/sequence/large
    // object/fdw+server/parameter), GRANTED BY spellings, DROP OWNED /
    // REASSIGN OWNED, row-level-security policy brackets, and the aclitem
    // long tail (aclinsert/aclremove stubs, hash functions).
    ProdWeight { name: "adtm:acl:arity", default: 2.0 },
    ProdWeight { name: "adtm:acl:objx", default: 2.0 },
    ProdWeight { name: "adtm:acl:grantby", default: 1.0 },
    ProdWeight { name: "adtm:acl:owned", default: 0.8 },
    ProdWeight { name: "adtm:acl:rls", default: 1.0 },
    ProdWeight { name: "adtm:acl:aclx", default: 1.0 },
    // LD10 adt-misc-residue shapes.
    ProdWeight { name: "adtm:strsafe", default: 2.5 },
    ProdWeight { name: "adtm:regex2", default: 2.5 },
    ProdWeight { name: "adtm:locks", default: 1.5 },
    ProdWeight { name: "adtm:activity", default: 1.0 },
    ProdWeight { name: "adtm:hba", default: 1.0 },
    ProdWeight { name: "adtm:amprop", default: 1.5 },
    ProdWeight { name: "adtm:inet2", default: 2.0 },
    ProdWeight { name: "adtm:fmtty", default: 1.5 },
    ProdWeight { name: "adtm:trim2", default: 2.0 },
    ProdWeight { name: "adtm:lev", default: 1.5 },
    ProdWeight { name: "adtm:ident", default: 1.5 },
    ProdWeight { name: "adtm:bit2", default: 2.0 },
    ProdWeight { name: "adtm:cash2", default: 2.0 },
    ProdWeight { name: "adtm:spgbox", default: 1.5 },
    ProdWeight { name: "adtm:xrec", default: 1.5 },
    ProdWeight { name: "adtm:progress", default: 0.8 },
    // LD10 pubsub module shapes.
    ProdWeight { name: "pubsub:pub_create", default: 2.0 },
    ProdWeight { name: "pubsub:pub_alter", default: 2.0 },
    ProdWeight { name: "pubsub:pub_err", default: 1.5 },
    ProdWeight { name: "pubsub:sub_create", default: 2.0 },
    ProdWeight { name: "pubsub:sub_alter", default: 2.0 },
    ProdWeight { name: "pubsub:sub_err", default: 2.0 },
    // LD2 einterp module (interpreter-opcode + tree-walker drain groups):
    // group shapes. raw:* shapes arm the parse_cte raw-walker via WITH
    // RECURSIVE; op:* shapes target dark EEOP_* arms. Uniform weights —
    // every shape exists because a specific line region is dark, so none
    // is a filler production.
    ProdWeight { name: "einterp:raw:dml", default: 1.2 },
    ProdWeight { name: "einterp:raw:select", default: 1.2 },
    ProdWeight { name: "einterp:raw:from", default: 1.0 },
    ProdWeight { name: "einterp:raw:window", default: 1.0 },
    ProdWeight { name: "einterp:raw:sublink", default: 1.0 },
    ProdWeight { name: "einterp:raw:sqljson", default: 1.2 },
    ProdWeight { name: "einterp:op:distinct", default: 1.0 },
    ProdWeight { name: "einterp:op:rowcmp", default: 1.0 },
    ProdWeight { name: "einterp:op:booltest", default: 1.0 },
    ProdWeight { name: "einterp:op:sysvar", default: 1.0 },
    ProdWeight { name: "einterp:op:oldnew", default: 1.2 },
    ProdWeight { name: "einterp:op:fieldstore", default: 1.0 },
    ProdWeight { name: "einterp:op:sbsref", default: 1.0 },
    ProdWeight { name: "einterp:op:domain", default: 1.0 },
    ProdWeight { name: "einterp:op:iocoerce", default: 1.0 },
    ProdWeight { name: "einterp:op:aggvariants", default: 1.2 },
    ProdWeight { name: "einterp:op:aggsorted", default: 1.0 },
    ProdWeight { name: "einterp:op:nextval", default: 0.8 },
    ProdWeight { name: "einterp:op:currentof", default: 0.8 },
    ProdWeight { name: "einterp:op:hashsaop", default: 1.0 },
    ProdWeight { name: "einterp:op:wholerow", default: 1.0 },
    ProdWeight { name: "einterp:op:svf", default: 0.8 },
    ProdWeight { name: "einterp:op:casetest", default: 1.0 },
    ProdWeight { name: "einterp:op:mergeaction", default: 1.0 },
    ProdWeight { name: "einterp:op:paramset", default: 1.0 },
    ProdWeight { name: "einterp:op:runcond", default: 1.0 },
    ProdWeight { name: "einterp:op:cycle", default: 1.0 },
    ProdWeight { name: "einterp:op:fusage", default: 0.8 },
    ProdWeight { name: "einterp:op:aggparallel", default: 0.6 },
    ProdWeight { name: "einterp:op:retview", default: 1.0 },
    ProdWeight { name: "einterp:op:partbound", default: 0.8 },
    ProdWeight { name: "einterp:op:viewwalk", default: 1.0 },
    // W4-WALK lane: nodeFuncs walker/mutator + execExpr residue drain
    // (docs/fuzzing/findings-w4walk.md). All hand-verified byte-identical.
    ProdWeight { name: "einterp:w4:typmod", default: 1.0 },
    ProdWeight { name: "einterp:w4:hazard", default: 1.2 },
    ProdWeight { name: "einterp:w4:wholerow", default: 1.2 },
    ProdWeight { name: "einterp:w4:jsonret", default: 1.2 },
    ProdWeight { name: "einterp:w4:plassign", default: 1.0 },
    ProdWeight { name: "einterp:w4:ruledrv", default: 1.0 },
    ProdWeight { name: "einterp:w4:saop", default: 1.0 },
    ProdWeight { name: "einterp:w4:arraymd", default: 1.0 },
    ProdWeight { name: "einterp:w4:errloc", default: 1.0 },
    ProdWeight { name: "einterp:w4:collate", default: 1.0 },
    ProdWeight { name: "einterp:w4:partition", default: 1.0 },
    // EXPREVAL scalar-opcode saturation module (crate::expreval): the exact
    // ExecInterpExpr scalar arms the mutation lane flagged as thin oracle
    // coverage. Each shape is one deep-but-cheap VALUES-driven SELECT group.
    ProdWeight { name: "expreval:saop:hash", default: 1.2 },
    ProdWeight { name: "expreval:saop:linear", default: 1.2 },
    ProdWeight { name: "expreval:isjson", default: 1.2 },
    ProdWeight { name: "expreval:wholerow", default: 1.0 },
    ProdWeight { name: "expreval:distinct", default: 1.0 },
    ProdWeight { name: "expreval:greatest", default: 1.0 },
    ProdWeight { name: "expreval:case", default: 1.0 },
    ProdWeight { name: "expreval:coalesce", default: 1.0 },
    ProdWeight { name: "expreval:bool3vl", default: 1.0 },
    ProdWeight { name: "expreval:rowcmp", default: 1.0 },
    // LD4 EXPLAIN plan-node/option drain module (crate::exd): shape arms
    // (each a self-contained fixture + probe group over the explain.c
    // hollow cluster) and the plain-EXPLAIN format rider. The heavier
    // fixture arms (bitmap: 20k-row load; exec: 4k) sit below the light
    // ones.
    ProdWeight { name: "exd:planshape", default: 1.5 },
    ProdWeight { name: "exd:scans", default: 1.5 },
    ProdWeight { name: "exd:joins", default: 1.0 },
    ProdWeight { name: "exd:opterr", default: 0.8 },
    ProdWeight { name: "exd:bitmap", default: 0.6 },
    ProdWeight { name: "exd:setop", default: 1.2 },
    ProdWeight { name: "exd:modify", default: 1.2 },
    ProdWeight { name: "exd:exec", default: 0.8 },
    ProdWeight { name: "exd:utility", default: 1.2 },
    ProdWeight { name: "exd:formats", default: 1.0 },
    ProdWeight { name: "exd:partition", default: 1.0 },
    ProdWeight { name: "exd:namedts", default: 0.6 },
    ProdWeight { name: "exd:setop:intersect", default: 1.0 },
    ProdWeight { name: "exd:setop:intersectall", default: 1.0 },
    ProdWeight { name: "exd:setop:except", default: 1.0 },
    ProdWeight { name: "exd:setop:exceptall", default: 1.0 },
    ProdWeight { name: "exd:formats:json", default: 1.0 },
    ProdWeight { name: "exd:formats:yaml", default: 1.0 },
    // TEXT dominates (it is the only spelling the standing explain module
    // uses); the three structured formats each get a real share so the
    // explain_format.c XML/JSON/YAML arms fire every leg.
    ProdWeight { name: "exd:fmt:text", default: 2.0 },
    ProdWeight { name: "exd:fmt:json", default: 1.0 },
    ProdWeight { name: "exd:fmt:yaml", default: 1.0 },
    ProdWeight { name: "exd:fmt:xml", default: 1.0 },
    // LD5 spill module: family selection. create leads until a table is
    // live (the fallback path re-routes to create anyway); drop stays
    // rare so one table serves many spill queries.
    ProdWeight { name: "spill:create", default: 1.2 },
    ProdWeight { name: "spill:drop", default: 0.2 },
    ProdWeight { name: "spill:sort", default: 3.0 },
    ProdWeight { name: "spill:scroll", default: 2.5 },
    ProdWeight { name: "spill:hold", default: 2.0 },
    ProdWeight { name: "spill:hj", default: 3.5 },
    ProdWeight { name: "spill:mj", default: 2.0 },
    ProdWeight { name: "spill:hashagg", default: 3.5 },
    ProdWeight { name: "spill:groupagg", default: 3.0 },
    ProdWeight { name: "spill:window", default: 2.5 },
    ProdWeight { name: "spill:cte", default: 2.0 },
    ProdWeight { name: "spill:material", default: 2.0 },
    ProdWeight { name: "spill:memoize", default: 2.0 },
    ProdWeight { name: "spill:incsort", default: 2.0 },
    ProdWeight { name: "spill:cluster", default: 1.5 },
    ProdWeight { name: "spill:hashidx", default: 1.0 },
    ProdWeight { name: "spill:rescan", default: 1.5 },
    ProdWeight { name: "spill:explain", default: 1.0 },
    // spill: initial table volume (spill depth vs leg runtime).
    ProdWeight { name: "spill:rows:8000", default: 1.5 },
    ProdWeight { name: "spill:rows:16000", default: 2.0 },
    ProdWeight { name: "spill:rows:20000", default: 1.5 },
    // spill: external-sort statement shapes.
    ProdWeight { name: "spill:sort:rows", default: 2.0 },
    ProdWeight { name: "spill:sort:wrap", default: 2.0 },
    ProdWeight { name: "spill:sort:desc", default: 1.5 },
    ProdWeight { name: "spill:sort:bounded", default: 1.0 },
    ProdWeight { name: "spill:sort:abbrev", default: 1.5 },
    // W5 r2: narrow sort at 256kB — grow_memtuples growth/clamp edges.
    ProdWeight { name: "spill:sort:grow", default: 1.5 },
    // spill: serial hash-join arms.
    ProdWeight { name: "spill:hj:inner", default: 2.0 },
    ProdWeight { name: "spill:hj:skew", default: 2.0 },
    ProdWeight { name: "spill:hj:rows", default: 1.5 },
    ProdWeight { name: "spill:hj:outer", default: 1.5 },
    ProdWeight { name: "spill:hj:antisemi", default: 1.5 },
    // W5 r2: right-fill/right-semi/right-anti, oversized build tuples,
    // bucket-grow (nbatch==1), and empty-side early-out arms.
    ProdWeight { name: "spill:hj:right", default: 1.5 },
    ProdWeight { name: "spill:hj:bigtuple", default: 1.5 },
    ProdWeight { name: "spill:hj:growbuckets", default: 1.5 },
    ProdWeight { name: "spill:hj:empty", default: 1.0 },
    // spill: HashAgg spill shapes.
    ProdWeight { name: "spill:ha:wrap", default: 2.0 },
    ProdWeight { name: "spill:ha:distinct", default: 1.5 },
    ProdWeight { name: "spill:ha:gsets", default: 1.5 },
    // W5 r2: AGG_MIXED (sorted rollup chain + hashed set) multi-phase.
    ProdWeight { name: "spill:ha:mixed", default: 1.5 },
    // spill: sort-based aggregation shapes.
    ProdWeight { name: "spill:ga:group", default: 2.0 },
    ProdWeight { name: "spill:ga:distinct", default: 1.5 },
    ProdWeight { name: "spill:ga:oset", default: 1.5 },
    // W5 r2: FILTER+strict-NULL transitions, hypothetical-set aggregates,
    // low-cardinality DATUM sort (abbrev abort).
    ProdWeight { name: "spill:ga:filter", default: 1.5 },
    ProdWeight { name: "spill:ga:hypo", default: 1.5 },
    ProdWeight { name: "spill:ga:dpad", default: 1.5 },
    // spill: window-over-spilled-tuplestore shapes.
    ProdWeight { name: "spill:win:rowsframe", default: 2.0 },
    ProdWeight { name: "spill:win:part", default: 1.5 },
    ProdWeight { name: "spill:win:range", default: 1.5 },
    // spill: EXPLAIN plan-shape probes.
    ProdWeight { name: "spill:ex:hj", default: 1.0 },
    ProdWeight { name: "spill:ex:ha", default: 1.0 },
    ProdWeight { name: "spill:ex:incsort", default: 1.0 },
    // LD6 earm module (ERROR-ARM drain groups): every shape family exists
    // because a specific DDL/parser/catalog ereport cluster is dark, so
    // none is a filler production. The heavier families (altable/acl/drop)
    // sit slightly above 1.0 because their C-side arm mass is the largest
    // (tablecmds.c / aclchk.c / dropcmds.c).
    ProdWeight { name: "earm:schema", default: 0.8 },
    ProdWeight { name: "earm:drop", default: 1.2 },
    ProdWeight { name: "earm:typmod", default: 0.8 },
    ProdWeight { name: "earm:acl", default: 1.2 },
    ProdWeight { name: "earm:role", default: 1.0 },
    ProdWeight { name: "earm:seq", default: 1.0 },
    ProdWeight { name: "earm:idx", default: 1.2 },
    ProdWeight { name: "earm:agg", default: 1.2 },
    ProdWeight { name: "earm:type", default: 1.2 },
    ProdWeight { name: "earm:coerce", default: 1.0 },
    ProdWeight { name: "earm:colref", default: 1.0 },
    ProdWeight { name: "earm:aggplace", default: 1.0 },
    ProdWeight { name: "earm:srf", default: 1.0 },
    ProdWeight { name: "earm:cte", default: 1.0 },
    ProdWeight { name: "earm:altable", default: 1.4 },
    ProdWeight { name: "earm:fk", default: 1.0 },
    ProdWeight { name: "earm:stats", default: 0.8 },
    ProdWeight { name: "earm:guc", default: 0.8 },
    ProdWeight { name: "earm:opclass", default: 0.8 },
    ProdWeight { name: "earm:tabdef", default: 1.2 },
    // LD7 plansel module: family selection. create leads until a fixture
    // set exists (need_set! falls back to create); drop stays rare so one
    // set serves many sweeps.
    ProdWeight { name: "plansel:create", default: 1.2 },
    ProdWeight { name: "plansel:drop", default: 0.15 },
    ProdWeight { name: "plansel:scan", default: 3.0 },
    ProdWeight { name: "plansel:join", default: 3.5 },
    ProdWeight { name: "plansel:pwise", default: 3.0 },
    ProdWeight { name: "plansel:agg", default: 3.0 },
    ProdWeight { name: "plansel:setop", default: 2.0 },
    ProdWeight { name: "plansel:subq", default: 3.0 },
    ProdWeight { name: "plansel:prep", default: 2.5 },
    ProdWeight { name: "plansel:joinrm", default: 1.5 },
    ProdWeight { name: "plansel:dml", default: 2.0 },
    ProdWeight { name: "plansel:explain", default: 1.0 },
    // plansel scan shapes.
    ProdWeight { name: "plansel:scan:range", default: 1.0 },
    ProdWeight { name: "plansel:scan:or", default: 1.4 },
    ProdWeight { name: "plansel:scan:bool", default: 1.2 },
    ProdWeight { name: "plansel:scan:partial", default: 1.2 },
    ProdWeight { name: "plansel:scan:exprIdx", default: 1.0 },
    ProdWeight { name: "plansel:scan:inlist", default: 1.0 },
    ProdWeight { name: "plansel:scan:composite", default: 1.0 },
    // plansel join shapes.
    ProdWeight { name: "plansel:join:inner", default: 1.0 },
    ProdWeight { name: "plansel:join:left", default: 1.0 },
    ProdWeight { name: "plansel:join:full", default: 1.4 },
    ProdWeight { name: "plansel:join:semi", default: 1.0 },
    ProdWeight { name: "plansel:join:anti", default: 1.0 },
    ProdWeight { name: "plansel:join:multi", default: 1.4 },
    ProdWeight { name: "plansel:join:lateral", default: 1.2 },
    // plansel partitionwise shapes.
    ProdWeight { name: "plansel:pwise:join", default: 1.2 },
    ProdWeight { name: "plansel:pwise:leftjoin", default: 1.0 },
    ProdWeight { name: "plansel:pwise:agg", default: 1.2 },
    ProdWeight { name: "plansel:pwise:joinagg", default: 1.0 },
    ProdWeight { name: "plansel:pwise:lateral", default: 1.2 },
    // plansel grouping shapes.
    ProdWeight { name: "plansel:agg:group", default: 1.2 },
    ProdWeight { name: "plansel:agg:gsets", default: 1.2 },
    ProdWeight { name: "plansel:agg:distinct", default: 1.0 },
    ProdWeight { name: "plansel:agg:countd", default: 1.0 },
    ProdWeight { name: "plansel:agg:having", default: 0.8 },
    ProdWeight { name: "plansel:agg:ordered", default: 0.8 },
    // plansel subquery shapes.
    ProdWeight { name: "plansel:subq:in", default: 1.2 },
    ProdWeight { name: "plansel:subq:ortree", default: 1.2 },
    ProdWeight { name: "plansel:subq:scalar", default: 1.0 },
    ProdWeight { name: "plansel:subq:winrun", default: 1.2 },
    ProdWeight { name: "plansel:subq:limit", default: 1.0 },
    ProdWeight { name: "plansel:subq:any", default: 1.0 },
    // plansel prep/pullup shapes.
    ProdWeight { name: "plansel:prep:pullup", default: 1.2 },
    ProdWeight { name: "plansel:prep:values", default: 1.0 },
    ProdWeight { name: "plansel:prep:result", default: 1.0 },
    ProdWeight { name: "plansel:prep:srf", default: 1.4 },
    ProdWeight { name: "plansel:prep:constfn", default: 1.0 },
    ProdWeight { name: "plansel:prep:negate", default: 1.0 },
    ProdWeight { name: "plansel:prep:dupors", default: 1.0 },
    ProdWeight { name: "plansel:prep:genvirt", default: 1.2 },
    // plansel join-removal shapes.
    ProdWeight { name: "plansel:joinrm:left", default: 1.2 },
    ProdWeight { name: "plansel:joinrm:distinct", default: 1.0 },
    ProdWeight { name: "plansel:joinrm:self", default: 0.8 },
    // plansel write shapes (BEGIN..ROLLBACK bracketed).
    ProdWeight { name: "plansel:dml:update", default: 1.0 },
    ProdWeight { name: "plansel:dml:delete", default: 1.0 },
    ProdWeight { name: "plansel:dml:conflict", default: 1.2 },
    ProdWeight { name: "plansel:dml:partupd", default: 1.2 },
    ProdWeight { name: "plansel:dml:merge", default: 1.0 },
    // LD8 earm2 module (ERROR-ARM round 2): verbatim hand-verified
    // sections; weights bias toward the biggest residual C-arm mass
    // (tablecmds partition/altype, parse_utilcmd/parse_expr relref,
    // trigger/rule definition).
    ProdWeight { name: "earm2:parttab", default: 1.4 },
    ProdWeight { name: "earm2:altype", default: 1.2 },
    ProdWeight { name: "earm2:typio", default: 1.0 },
    ProdWeight { name: "earm2:funcddl", default: 1.1 },
    ProdWeight { name: "earm2:funcres", default: 1.0 },
    ProdWeight { name: "earm2:winval", default: 1.0 },
    ProdWeight { name: "earm2:ext", default: 0.8 },
    ProdWeight { name: "earm2:tsearch", default: 0.9 },
    ProdWeight { name: "earm2:schema2", default: 0.8 },
    ProdWeight { name: "earm2:relref", default: 1.2 },
    ProdWeight { name: "earm2:shdep", default: 1.0 },
    ProdWeight { name: "earm2:seqid", default: 1.1 },
    ProdWeight { name: "earm2:trigrule", default: 1.2 },
    ProdWeight { name: "earm2:coerce2", default: 0.9 },
    ProdWeight { name: "earm2:parser2", default: 0.9 },
    ProdWeight { name: "earm2:async", default: 0.8 },
    ProdWeight { name: "earm2:collenc", default: 0.9 },
    ProdWeight { name: "earm2:alter2", default: 1.0 },
    // LD8 wave-2 (earm2b): residual-arm sections after the wave-1 drain;
    // acl/typ/role/aggpl carry the biggest remaining C-arm mass.
    ProdWeight { name: "earm2:w2acl", default: 1.3 },
    ProdWeight { name: "earm2:w2typ", default: 1.2 },
    ProdWeight { name: "earm2:w2role", default: 1.1 },
    ProdWeight { name: "earm2:w2agg", default: 1.1 },
    ProdWeight { name: "earm2:w2aggpl", default: 1.3 },
    ProdWeight { name: "earm2:w2udesc", default: 0.8 },
    ProdWeight { name: "earm2:w2colref", default: 0.8 },
    ProdWeight { name: "earm2:w2idx", default: 1.2 },
    ProdWeight { name: "earm2:w2poly", default: 1.0 },
    // W4-ERR earm3 module (ERROR-ARM round 3): verbatim hand-verified
    // sections; weights bias toward the biggest residual C-arm mass
    // (tablecmds partition-FK lifecycle, DefineIndex, LIKE/inheritance
    // merge, wrong-relkind ALTER arms, column-ref resolution).
    ProdWeight { name: "earm3:pfk", default: 1.4 },
    ProdWeight { name: "earm3:idxddl", default: 1.3 },
    ProdWeight { name: "earm3:likei", default: 1.3 },
    ProdWeight { name: "earm3:idxcon", default: 1.1 },
    ProdWeight { name: "earm3:gentype", default: 1.1 },
    ProdWeight { name: "earm3:aclres", default: 1.2 },
    ProdWeight { name: "earm3:roles", default: 1.0 },
    ProdWeight { name: "earm3:trunc", default: 1.1 },
    ProdWeight { name: "earm3:dropcon", default: 1.0 },
    ProdWeight { name: "earm3:altcol", default: 1.2 },
    ProdWeight { name: "earm3:wrongk", default: 1.3 },
    ProdWeight { name: "earm3:addcol", default: 1.0 },
    ProdWeight { name: "earm3:castop", default: 1.1 },
    ProdWeight { name: "earm3:rcte", default: 1.0 },
    ProdWeight { name: "earm3:colname", default: 0.9 },
    ProdWeight { name: "earm3:colref", default: 1.2 },
    ProdWeight { name: "earm3:shdep2", default: 1.1 },
    ProdWeight { name: "earm3:tsdes", default: 0.9 },
    // ERR3 earm4 module (ERROR-ARM round 4): verbatim hand-verified
    // sections over the catalog/DDL error surface the first three passes
    // left (domain/constraint validation, tablespace/AM, ownership +
    // dependency, RLS policy, rules, publication/subscription DDL,
    // COMMENT/SECURITY LABEL, sequence-identity, parser grammar).
    ProdWeight { name: "earm4:domcon", default: 1.2 },
    ProdWeight { name: "earm4:chkval", default: 1.3 },
    ProdWeight { name: "earm4:atmisc", default: 1.2 },
    ProdWeight { name: "earm4:tblspc", default: 1.0 },
    ProdWeight { name: "earm4:owndep", default: 1.1 },
    ProdWeight { name: "earm4:policy", default: 1.1 },
    ProdWeight { name: "earm4:rules", default: 1.0 },
    ProdWeight { name: "earm4:pubval", default: 1.1 },
    ProdWeight { name: "earm4:subval", default: 1.0 },
    ProdWeight { name: "earm4:commlbl", default: 1.0 },
    ProdWeight { name: "earm4:seqid2", default: 1.1 },
    ProdWeight { name: "earm4:pgram2", default: 1.2 },
    // exr (LD9): executor-residue shape selection.
    ProdWeight { name: "exr:create", default: 1.2 },
    ProdWeight { name: "exr:drop", default: 0.15 },
    ProdWeight { name: "exr:prune", default: 2.5 },
    ProdWeight { name: "exr:winframe", default: 3.0 },
    ProdWeight { name: "exr:subplan", default: 2.5 },
    ProdWeight { name: "exr:setop", default: 2.0 },
    ProdWeight { name: "exr:lockrows", default: 2.0 },
    ProdWeight { name: "exr:dml", default: 2.5 },
    ProdWeight { name: "exr:merge", default: 2.0 },
    ProdWeight { name: "exr:tid", default: 2.0 },
    ProdWeight { name: "exr:tfunc", default: 2.0 },
    ProdWeight { name: "exr:ntstore", default: 1.5 },
    ProdWeight { name: "exr:currentof", default: 1.5 },
    ProdWeight { name: "exr:sqlfn", default: 2.0 },
    ProdWeight { name: "exr:limit", default: 1.5 },
    ProdWeight { name: "exr:iscan", default: 2.0 },
    ProdWeight { name: "exr:rescan", default: 2.0 },
    ProdWeight { name: "exr:mj", default: 1.5 },
    // exr: runtime-pruning sub-shapes.
    ProdWeight { name: "exr:prune:eq", default: 1.5 },
    ProdWeight { name: "exr:prune:range", default: 1.0 },
    ProdWeight { name: "exr:prune:merge", default: 1.2 },
    ProdWeight { name: "exr:prune:join", default: 1.0 },
    ProdWeight { name: "exr:prune:hash", default: 1.0 },
    // exr: window frame-option sub-shapes.
    ProdWeight { name: "exr:wf:rangeint", default: 1.2 },
    ProdWeight { name: "exr:wf:rangenum", default: 1.0 },
    ProdWeight { name: "exr:wf:groups", default: 1.2 },
    ProdWeight { name: "exr:wf:rows", default: 1.0 },
    ProdWeight { name: "exr:wf:pos", default: 1.5 },
    ProdWeight { name: "exr:wf:moving", default: 1.0 },
    // exr: subplan sub-shapes.
    ProdWeight { name: "exr:sp:hashed", default: 1.0 },
    ProdWeight { name: "exr:sp:nullhash", default: 1.2 },
    ProdWeight { name: "exr:sp:multicol", default: 1.0 },
    ProdWeight { name: "exr:sp:scan", default: 1.0 },
    ProdWeight { name: "exr:sp:corr", default: 1.0 },
    ProdWeight { name: "exr:sp:any", default: 0.8 },
    // exr: setop strategy.
    ProdWeight { name: "exr:so:hash", default: 1.0 },
    ProdWeight { name: "exr:so:sort", default: 1.0 },
    // exr: lock-rows sub-shapes.
    ProdWeight { name: "exr:lr:plain", default: 1.0 },
    ProdWeight { name: "exr:lr:join", default: 1.0 },
    ProdWeight { name: "exr:lr:limit", default: 0.8 },
    // exr: modify-table sub-shapes.
    ProdWeight { name: "exr:dml:oldnew", default: 1.2 },
    ProdWeight { name: "exr:dml:conflict", default: 1.2 },
    ProdWeight { name: "exr:dml:conflictwhere", default: 1.0 },
    ProdWeight { name: "exr:dml:crosspart", default: 1.2 },
    ProdWeight { name: "exr:dml:gen", default: 1.0 },
    ProdWeight { name: "exr:dml:wco", default: 0.8 },
    ProdWeight { name: "exr:dml:fromusing", default: 1.0 },
    // exr: MERGE sub-shapes.
    ProdWeight { name: "exr:mg:full", default: 1.2 },
    ProdWeight { name: "exr:mg:bysource", default: 1.0 },
    ProdWeight { name: "exr:mg:part", default: 1.0 },
    // exr: tid-scan sub-shapes.
    ProdWeight { name: "exr:tid:eq", default: 1.0 },
    ProdWeight { name: "exr:tid:in", default: 1.0 },
    ProdWeight { name: "exr:tid:range", default: 1.0 },
    ProdWeight { name: "exr:tid:back", default: 0.8 },
    // exr: table-func (JSON_TABLE) sub-shapes.
    ProdWeight { name: "exr:tf:nested", default: 1.2 },
    ProdWeight { name: "exr:tf:opts", default: 1.0 },
    ProdWeight { name: "exr:tf:err", default: 0.8 },
    ProdWeight { name: "exr:tf:lateral", default: 1.0 },
    // exr: named-tuplestore trigger sub-shapes.
    ProdWeight { name: "exr:nt:ins", default: 1.0 },
    ProdWeight { name: "exr:nt:upd", default: 1.0 },
    ProdWeight { name: "exr:nt:del", default: 1.0 },
    ProdWeight { name: "exr:nt:mix", default: 1.0 },
    // exr: SQL-function sub-shapes.
    ProdWeight { name: "exr:fn:poly", default: 1.0 },
    ProdWeight { name: "exr:fn:table", default: 1.0 },
    ProdWeight { name: "exr:fn:atomic", default: 1.0 },
    ProdWeight { name: "exr:fn:variadic", default: 0.8 },
    ProdWeight { name: "exr:fn:comp", default: 1.0 },
    ProdWeight { name: "exr:fn:setof", default: 1.0 },
    // exr: limit sub-shapes.
    ProdWeight { name: "exr:lim:ties", default: 1.2 },
    ProdWeight { name: "exr:lim:edge", default: 1.0 },
    ProdWeight { name: "exr:lim:back", default: 1.0 },
    // exr: index-scan key sub-shapes.
    ProdWeight { name: "exr:is:rowcmp", default: 1.2 },
    ProdWeight { name: "exr:is:saop", default: 1.2 },
    ProdWeight { name: "exr:is:null", default: 0.8 },
    ProdWeight { name: "exr:is:desc", default: 0.8 },
    // exr: rescan inner-node sub-shapes.
    ProdWeight { name: "exr:rs:sort", default: 1.0 },
    ProdWeight { name: "exr:rs:agg", default: 1.0 },
    ProdWeight { name: "exr:rs:setop", default: 1.0 },
    ProdWeight { name: "exr:rs:win", default: 1.0 },
    ProdWeight { name: "exr:rs:limit", default: 1.0 },
    // exr: parallel-append + exclusion families.
    ProdWeight { name: "exr:parappend", default: 1.5 },
    ProdWeight { name: "exr:excl", default: 1.2 },
    ProdWeight { name: "exr:pa:agg", default: 1.2 },
    ProdWeight { name: "exr:pa:mixed", default: 1.0 },
    ProdWeight { name: "exr:pa:rescan", default: 1.0 },
    ProdWeight { name: "exr:fn:dml", default: 0.8 },
    // exr: merge-join sub-shapes.
    ProdWeight { name: "exr:mj:dup", default: 1.2 },
    ProdWeight { name: "exr:mj:dim", default: 1.0 },
    // exr2 (EXEC-RESIDUE): rescan-half + serialization shape selection.
    ProdWeight { name: "exr2:create", default: 1.2 },
    ProdWeight { name: "exr2:drop", default: 0.15 },
    ProdWeight { name: "exr2:setop", default: 2.0 },
    ProdWeight { name: "exr2:setoprescan", default: 3.0 },
    ProdWeight { name: "exr2:recursive", default: 2.5 },
    ProdWeight { name: "exr2:winrescan", default: 2.5 },
    ProdWeight { name: "exr2:ntrescan", default: 2.0 },
    ProdWeight { name: "exr2:serial", default: 1.5 },
    // exr2: top-level setop strategy.
    ProdWeight { name: "exr2:so:hash", default: 1.0 },
    ProdWeight { name: "exr2:so:sort", default: 1.0 },
    // exr2: setop-rescan strategy.
    ProdWeight { name: "exr2:sor:hash", default: 1.0 },
    ProdWeight { name: "exr2:sor:sort", default: 1.0 },
    // exr2: recursive-union sub-shapes.
    ProdWeight { name: "exr2:rec:lateral", default: 1.5 },
    ProdWeight { name: "exr2:rec:plain", default: 1.0 },
    // exr2: named-tuplestore rescan sub-shapes.
    ProdWeight { name: "exr2:nt:ins", default: 1.0 },
    ProdWeight { name: "exr2:nt:upd", default: 1.0 },
    ProdWeight { name: "exr2:nt:del", default: 1.0 },
    ProdWeight { name: "exr2:nt:mix", default: 1.0 },
    // exr2: plan-serialization toggle sub-shapes.
    ProdWeight { name: "exr2:ser:print", default: 1.2 },
    ProdWeight { name: "exr2:ser:parallel", default: 1.0 },
    // numeric (NUMERIC lane): top-level shape selection.
    ProdWeight { name: "numeric:istat", default: 2.0 },
    ProdWeight { name: "numeric:xprec", default: 1.5 },
    // numeric: integer statistical-aggregate (int128 poly) sub-shapes.
    ProdWeight { name: "numeric:is:serial", default: 1.2 },
    ProdWeight { name: "numeric:is:par", default: 1.0 },
    ProdWeight { name: "numeric:is:move", default: 0.8 },
    // numeric: extreme-precision arithmetic sub-shapes.
    ProdWeight { name: "numeric:xp:mul", default: 1.2 },
    ProdWeight { name: "numeric:xp:div", default: 1.2 },
    ProdWeight { name: "numeric:xp:trans", default: 1.0 },
    // numx (LD9): adt-numeric shape selection.
    ProdWeight { name: "numx:div", default: 2.5 },
    ProdWeight { name: "numx:pow", default: 2.5 },
    ProdWeight { name: "numx:instr", default: 2.0 },
    ProdWeight { name: "numx:fmt", default: 2.5 },
    ProdWeight { name: "numx:round", default: 2.0 },
    ProdWeight { name: "numx:cast", default: 2.0 },
    ProdWeight { name: "numx:agg", default: 1.2 },
    ProdWeight { name: "numx:wb", default: 1.2 },
    ProdWeight { name: "numx:misc", default: 1.5 },
    ProdWeight { name: "numx:sort", default: 0.4 },
    ProdWeight { name: "numx:series", default: 1.0 },
    ProdWeight { name: "numx:float", default: 2.0 },
    // numx: division/multiplication sub-shapes.
    ProdWeight { name: "numx:div:op", default: 1.5 },
    ProdWeight { name: "numx:div:fn", default: 1.0 },
    ProdWeight { name: "numx:div:zero", default: 0.5 },
    ProdWeight { name: "numx:div:special", default: 1.0 },
    // numx: power/log sub-shapes.
    ProdWeight { name: "numx:pow:pow", default: 1.2 },
    ProdWeight { name: "numx:pow:int", default: 1.2 },
    ProdWeight { name: "numx:pow:explog", default: 1.2 },
    ProdWeight { name: "numx:pow:sqrt", default: 1.0 },
    ProdWeight { name: "numx:pow:err", default: 0.8 },
    ProdWeight { name: "numx:pow:fac", default: 0.6 },
    // numx: numeric_in sub-shapes.
    ProdWeight { name: "numx:in:ok", default: 1.2 },
    ProdWeight { name: "numx:in:nondec", default: 1.2 },
    ProdWeight { name: "numx:in:err", default: 0.8 },
    // numx: format-picture sub-shapes.
    ProdWeight { name: "numx:fmt:tochar", default: 1.5 },
    ProdWeight { name: "numx:fmt:tonum", default: 1.2 },
    ProdWeight { name: "numx:fmt:err", default: 0.8 },
    // numx: rounding/typmod sub-shapes.
    ProdWeight { name: "numx:rd:round", default: 1.2 },
    ProdWeight { name: "numx:rd:typmod", default: 1.2 },
    ProdWeight { name: "numx:rd:scale", default: 1.0 },
    ProdWeight { name: "numx:rd:err", default: 0.8 },
    // numx: cast sub-shapes.
    ProdWeight { name: "numx:ca:int", default: 1.2 },
    ProdWeight { name: "numx:ca:float", default: 1.2 },
    ProdWeight { name: "numx:ca:err", default: 0.8 },
    ProdWeight { name: "numx:ca:lsn", default: 0.6 },
    ProdWeight { name: "numx:ca:gcd", default: 0.8 },
    // numx: accumulator sub-shapes.
    ProdWeight { name: "numx:ag:win", default: 1.2 },
    ProdWeight { name: "numx:ag:special", default: 1.2 },
    ProdWeight { name: "numx:ag:int", default: 1.0 },
    ProdWeight { name: "numx:ag:stat", default: 1.0 },
    // numx: width_bucket sub-shapes.
    ProdWeight { name: "numx:wb:num", default: 1.0 },
    ProdWeight { name: "numx:wb:f8", default: 1.0 },
    ProdWeight { name: "numx:wb:err", default: 0.8 },
    // numx: misc sub-shapes.
    ProdWeight { name: "numx:mi:unary", default: 1.0 },
    ProdWeight { name: "numx:mi:cmp", default: 1.0 },
    ProdWeight { name: "numx:mi:rand", default: 0.6 },
    ProdWeight { name: "numx:mi:hash", default: 0.8 },
    // numx: abbrev-sort sub-shapes.
    ProdWeight { name: "numx:st:lowcard", default: 1.0 },
    ProdWeight { name: "numx:st:mixed", default: 1.0 },
    // numx: generate_series sub-shapes.
    ProdWeight { name: "numx:se:ok", default: 1.2 },
    ProdWeight { name: "numx:se:err", default: 0.8 },
    ProdWeight { name: "numx:se:plan", default: 0.6 },
    // numx: float-rider sub-shapes.
    ProdWeight { name: "numx:fl:in", default: 1.2 },
    ProdWeight { name: "numx:fl:inerr", default: 0.8 },
    ProdWeight { name: "numx:fl:pow", default: 1.2 },
    ProdWeight { name: "numx:fl:regr", default: 1.0 },
    // Q7 expr-misc-adt + expr-strings breadth (crate::adtmisc).
    // adtm:xml is coverage-only: set --weight adtm:xml=0 on every
    // differential leg (no-libxml C reference vs native pgrust XML).
    ProdWeight { name: "adtm:misc2", default: 2.5 },
    ProdWeight { name: "adtm:errhint", default: 1.0 },
    ProdWeight { name: "adtm:regex3", default: 2.0 },
    ProdWeight { name: "adtm:enum2", default: 1.5 },
    ProdWeight { name: "adtm:rec2", default: 2.0 },
    ProdWeight { name: "adtm:uuid2", default: 1.5 },
    ProdWeight { name: "adtm:bit3", default: 2.0 },
    ProdWeight { name: "adtm:poly", default: 2.0 },
    ProdWeight { name: "adtm:toastslice", default: 1.0 },
    ProdWeight { name: "adtm:ri", default: 2.0 },
    ProdWeight { name: "adtm:part3", default: 1.5 },
    ProdWeight { name: "adtm:deparse3", default: 2.5 },
    ProdWeight { name: "adtm:pseudo3", default: 2.0 },
    ProdWeight { name: "adtm:tid3", default: 1.0 },
    ProdWeight { name: "adtm:xml", default: 1.0 },
    ProdWeight { name: "adtm:char2", default: 1.5 },
    ProdWeight { name: "adtm:bpchar", default: 2.5 },
    ProdWeight { name: "adtm:nametext", default: 1.5 },
    ProdWeight { name: "adtm:strfns", default: 3.0 },
    ProdWeight { name: "adtm:byteax", default: 2.0 },
    ProdWeight { name: "adtm:patidx", default: 1.5 },
    // Q7 agg-tweaks split + raw families (crate::agg::gen_aggx_stmts).
    ProdWeight { name: "agg:ast", default: 3.0 },
    ProdWeight { name: "agg:aggx", default: 2.0 },
    ProdWeight { name: "aggx:ordset", default: 3.0 },
    ProdWeight { name: "aggx:regr", default: 2.0 },
    ProdWeight { name: "aggx:minmax2", default: 2.5 },
    ProdWeight { name: "aggx:hashx", default: 2.5 },
    ProdWeight { name: "aggx:gsets2", default: 1.5 },
    ProdWeight { name: "aggx:aggerr", default: 0.8 },
    ProdWeight { name: "aggx:distinctm", default: 1.5 },
    ProdWeight { name: "aggx:groupnode", default: 1.5 },
    ProdWeight { name: "aggx:fdep", default: 1.0 },
    ProdWeight { name: "aggx:serialize", default: 1.0 },
    ProdWeight { name: "aggx:bitmapor", default: 1.5 },
    ProdWeight { name: "aggx:semijoin", default: 1.5 },
    ProdWeight { name: "aggx:datumsort", default: 0.8 },
    ProdWeight { name: "aggx:aggddl", default: 1.0 },
    // Q7 tsearch-funcs value-level families (crate::tsdl::gen_tsfx_stmts,
    // dispatched from the tsdl module under the tsdl:tsfx arm).
    ProdWeight { name: "tsdl:tsfx", default: 3.0 },
    ProdWeight { name: "tsfx:cast", default: 2.5 },
    ProdWeight { name: "tsfx:vecops", default: 3.0 },
    ProdWeight { name: "tsfx:qryops", default: 2.5 },
    ProdWeight { name: "tsfx:match", default: 2.5 },
    ProdWeight { name: "tsfx:rewrite", default: 2.0 },
    ProdWeight { name: "tsfx:headline", default: 2.5 },
    ProdWeight { name: "tsfx:parse", default: 1.5 },
    ProdWeight { name: "tsfx:stat", default: 1.5 },
    ProdWeight { name: "tsfx:trigger", default: 1.5 },
    ProdWeight { name: "tsfx:parserddl", default: 1.0 },
    ProdWeight { name: "tsfx:analyze", default: 1.0 },
    ProdWeight { name: "tsfx:likesup", default: 1.5 },
    ProdWeight { name: "tsfx:errlit", default: 0.8 },
    // ddldeep (SQLcov-A): verbatim hand-verified DDL-deep sections;
    // weights price the per-section drain mass (atpass/fkpart/typec are
    // the big tablecmds/typecmds lifecycles; wmin is the wal-minimal
    // pending-sync fuel and runs cheap).
    ProdWeight { name: "ddldeep:atpass", default: 1.5 },
    ProdWeight { name: "ddldeep:atmulti", default: 1.0 },
    ProdWeight { name: "ddldeep:fkpart", default: 1.5 },
    ProdWeight { name: "ddldeep:constr", default: 1.2 },
    ProdWeight { name: "ddldeep:typec", default: 1.3 },
    ProdWeight { name: "ddldeep:funcs", default: 1.1 },
    ProdWeight { name: "ddldeep:idxc", default: 1.3 },
    ProdWeight { name: "ddldeep:seqview", default: 1.1 },
    ProdWeight { name: "ddldeep:trigpol", default: 1.2 },
    ProdWeight { name: "ddldeep:stats", default: 0.9 },
    ProdWeight { name: "ddldeep:tblspc", default: 0.9 },
    ProdWeight { name: "ddldeep:wmin", default: 1.0 },
    // pgram (SQLcov-B): parser rare-grammar shape selection. The
    // transform-heavy DDL-ish shapes carry slightly more weight than the
    // pure-expression ones (the parser-arms chunk is dominated by
    // parse_utilcmd/parse_coerce/parse_func residue).
    ProdWeight { name: "pgram:ordinality", default: 1.0 },
    ProdWeight { name: "pgram:xmltab", default: 1.0 },
    ProdWeight { name: "pgram:jsontab", default: 1.2 },
    ProdWeight { name: "pgram:gsets", default: 1.2 },
    ProdWeight { name: "pgram:lateral", default: 1.0 },
    ProdWeight { name: "pgram:frame", default: 1.2 },
    ProdWeight { name: "pgram:merge", default: 1.2 },
    ProdWeight { name: "pgram:copyopt", default: 1.2 },
    ProdWeight { name: "pgram:stats", default: 1.0 },
    ProdWeight { name: "pgram:partbound", default: 1.2 },
    ProdWeight { name: "pgram:castcoll", default: 1.0 },
    ProdWeight { name: "pgram:opagg", default: 1.2 },
    ProdWeight { name: "pgram:rowarr", default: 1.2 },
    ProdWeight { name: "pgram:poly", default: 1.5 },
    ProdWeight { name: "pgram:cte", default: 1.2 },
    ProdWeight { name: "pgram:like", default: 1.5 },
    ProdWeight { name: "pgram:coldef", default: 1.5 },
    ProdWeight { name: "pgram:idxcon", default: 1.5 },
    ProdWeight { name: "pgram:funcsel", default: 1.2 },
    ProdWeight { name: "pgram:colref", default: 1.0 },
    ProdWeight { name: "pgram:exprkind", default: 1.2 },
    ProdWeight { name: "pgram:lockrows", default: 1.2 },
    ProdWeight { name: "pgram:plassign", default: 1.0 },
    // opt2 (SQLcov-B): optimizer round-2 shape selection. The
    // reparameterization shapes lead (the top residue rows of the
    // optimizer-arms chunk are pathnode.c reparameterize arms).
    ProdWeight { name: "opt2:lateralrp", default: 1.8 },
    ProdWeight { name: "opt2:tsrp", default: 1.5 },
    ProdWeight { name: "opt2:apprel", default: 1.5 },
    ProdWeight { name: "opt2:joinrm", default: 1.2 },
    ProdWeight { name: "opt2:eclass", default: 1.2 },
    ProdWeight { name: "opt2:constfold", default: 1.2 },
    ProdWeight { name: "opt2:indexmatch", default: 1.2 },
    ProdWeight { name: "opt2:pullup", default: 1.0 },
    ProdWeight { name: "opt2:uniq", default: 0.8 },
    ProdWeight { name: "opt2:refute", default: 1.2 },
    ProdWeight { name: "opt2:gucmatrix", default: 1.0 },
    // opt3 (W4-OPT): optimizer + executor residue shape selection. Every
    // family was written against named unhit line regions of the in-lane
    // BEFORE linegap (see findings-w4opt.md); weights bias toward the
    // biggest residue masses (routing/winframe/reparam).
    ProdWeight { name: "opt3:fkjoin", default: 1.4 },
    ProdWeight { name: "opt3:ojnest", default: 1.2 },
    ProdWeight { name: "opt3:constdeep", default: 1.0 },
    ProdWeight { name: "opt3:reparam", default: 1.6 },
    ProdWeight { name: "opt3:initplan", default: 1.2 },
    ProdWeight { name: "opt3:winframe", default: 1.6 },
    ProdWeight { name: "opt3:routing", default: 1.6 },
    ProdWeight { name: "opt3:idxkeys", default: 1.2 },
    ProdWeight { name: "opt3:sqlfn", default: 1.0 },
    ProdWeight { name: "opt3:scanmisc", default: 1.4 },
    // cfgm (CFG lane): config-machinery shape selection. set/local/sweep
    // carry the guc.c drain mass; conv/clienc carry the encoding-conv
    // chunk; the ok/err knob prices the matched-error SET arms.
    ProdWeight { name: "cfgm:set", default: 2.0 },
    ProdWeight { name: "cfgm:local", default: 1.5 },
    ProdWeight { name: "cfgm:setcfg", default: 1.2 },
    ProdWeight { name: "cfgm:custom", default: 1.0 },
    ProdWeight { name: "cfgm:sweep", default: 1.2 },
    ProdWeight { name: "cfgm:tz", default: 1.2 },
    ProdWeight { name: "cfgm:txniso", default: 1.0 },
    ProdWeight { name: "cfgm:conv", default: 1.5 },
    ProdWeight { name: "cfgm:clienc", default: 1.0 },
    ProdWeight { name: "cfgm:ok", default: 6.0 },
    ProdWeight { name: "cfgm:err", default: 1.0 },
    // btbrin (W5-BTBRIN) shape picks: btree ALT-PATH + BRIN drain arms.
    // Even weights — the drain wants each shape exercised roughly equally
    // since every one covers a disjoint C region.
    ProdWeight { name: "btbrin:keys", default: 1.0 },
    ProdWeight { name: "btbrin:uniq", default: 1.0 },
    ProdWeight { name: "btbrin:scan", default: 1.0 },
    ProdWeight { name: "btbrin:split", default: 1.0 },
    ProdWeight { name: "btbrin:pagedel", default: 1.0 },
    ProdWeight { name: "btbrin:build", default: 1.0 },
    ProdWeight { name: "btbrin:brinup", default: 1.0 },
    ProdWeight { name: "btbrin:brininc", default: 1.0 },
    ProdWeight { name: "btbrin:brinmm", default: 1.0 },
    ProdWeight { name: "btbrin:brinmaint", default: 1.0 },
    ProdWeight { name: "btbrin:opfam", default: 1.0 },
    // heap (W5-HEAP): the heapam alt-path drain module. Action mix biased
    // toward the tuple-state churn arms (hot/nonhot/abort/lock) with vacuum/
    // cluster/toast at modest weight (each is a whole-heap or bulk group).
    ProdWeight { name: "heap:create", default: 1.5 },
    ProdWeight { name: "heap:drop", default: 0.3 },
    ProdWeight { name: "heap:hot", default: 3.0 },
    ProdWeight { name: "heap:nonhot", default: 2.5 },
    ProdWeight { name: "heap:abortchurn", default: 2.0 },
    ProdWeight { name: "heap:lock", default: 2.0 },
    ProdWeight { name: "heap:toastwrite", default: 1.5 },
    ProdWeight { name: "heap:toastread", default: 1.5 },
    ProdWeight { name: "heap:toastupd", default: 1.5 },
    ProdWeight { name: "heap:vacuum", default: 2.0 },
    ProdWeight { name: "heap:trunc", default: 1.0 },
    ProdWeight { name: "heap:allvis", default: 1.5 },
    ProdWeight { name: "heap:newpage", default: 1.0 },
    ProdWeight { name: "heap:cic", default: 1.0 },
    ProdWeight { name: "heap:cluster", default: 1.0 },
    ProdWeight { name: "heap:tid", default: 1.5 },
    ProdWeight { name: "heap:serial", default: 1.5 },
    // create picks (fillfactor / base-load rows / toast_tuple_target /
    // storage class / replica identity).
    ProdWeight { name: "heap:ff:30", default: 1.5 },
    ProdWeight { name: "heap:ff:70", default: 1.0 },
    ProdWeight { name: "heap:ff:100", default: 1.0 },
    ProdWeight { name: "heap:rows:1500", default: 1.5 },
    ProdWeight { name: "heap:rows:3000", default: 1.5 },
    ProdWeight { name: "heap:rows:6000", default: 1.0 },
    ProdWeight { name: "heap:tt:default", default: 1.0 },
    ProdWeight { name: "heap:tt:256", default: 1.0 },
    ProdWeight { name: "heap:st:extended", default: 2.0 },
    ProdWeight { name: "heap:st:main", default: 1.0 },
    ProdWeight { name: "heap:st:plain", default: 1.0 },
    ProdWeight { name: "heap:ri:default", default: 1.5 },
    ProdWeight { name: "heap:ri:full", default: 1.0 },
    // non-HOT update shapes.
    ProdWeight { name: "heap:nonhot:key", default: 2.0 },
    ProdWeight { name: "heap:nonhot:pk", default: 1.0 },
    ProdWeight { name: "heap:nonhot:both", default: 1.5 },
    // aborted-write shapes.
    ProdWeight { name: "heap:abort:rollback", default: 2.0 },
    ProdWeight { name: "heap:abort:savepoint", default: 2.0 },
    ProdWeight { name: "heap:abort:insabort", default: 1.0 },
    // tuple lock modes and shapes.
    ProdWeight { name: "heap:lock:keyshare", default: 1.5 },
    ProdWeight { name: "heap:lock:share", default: 1.5 },
    ProdWeight { name: "heap:lock:nokeyupd", default: 1.5 },
    ProdWeight { name: "heap:lock:update", default: 1.5 },
    ProdWeight { name: "heap:lock:plain", default: 1.5 },
    ProdWeight { name: "heap:lock:upgrade", default: 1.5 },
    ProdWeight { name: "heap:lock:abortlock", default: 1.5 },
    ProdWeight { name: "heap:lock:lockwrite", default: 2.0 },
    // toast write / read / update shapes.
    ProdWeight { name: "heap:tw:comp", default: 2.0 },
    ProdWeight { name: "heap:tw:incomp", default: 2.0 },
    ProdWeight { name: "heap:tw:both", default: 1.5 },
    ProdWeight { name: "heap:tr:slice", default: 2.0 },
    ProdWeight { name: "heap:tr:len", default: 1.5 },
    ProdWeight { name: "heap:tr:full", default: 1.0 },
    ProdWeight { name: "heap:tr:size", default: 1.5 },
    ProdWeight { name: "heap:tu:append", default: 1.5 },
    ProdWeight { name: "heap:tu:null", default: 1.0 },
    ProdWeight { name: "heap:tu:plaincol", default: 1.5 },
    ProdWeight { name: "heap:tu:delins", default: 1.5 },
    // vacuum variants.
    ProdWeight { name: "heap:vac:plain", default: 2.0 },
    ProdWeight { name: "heap:vac:freeze", default: 1.5 },
    ProdWeight { name: "heap:vac:minage", default: 1.5 },
    ProdWeight { name: "heap:vac:skip", default: 1.5 },
    ProdWeight { name: "heap:vac:indexoff", default: 1.5 },
    ProdWeight { name: "heap:vac:indexon", default: 1.5 },
    ProdWeight { name: "heap:vac:verbose", default: 1.0 },
    // create-index-concurrently, cluster, tid-scan shapes.
    ProdWeight { name: "heap:cic:conc", default: 1.5 },
    ProdWeight { name: "heap:cic:plain", default: 1.5 },
    ProdWeight { name: "heap:cluster:pk", default: 1.5 },
    ProdWeight { name: "heap:cluster:k", default: 1.5 },
    ProdWeight { name: "heap:cluster:full", default: 1.5 },
    ProdWeight { name: "heap:tid:eq", default: 1.5 },
    ProdWeight { name: "heap:tid:range", default: 1.5 },
    // partalt (W5-PART): partition-prune / partitionwise ALT-PATH shape
    // selection. clauses+runtime carry the partprune.c match/step drain;
    // pwjmerge+pwagg carry the partition_bounds_merge / pwise-grouping
    // mass; multilevel+reparam carry the hierarchical-prune and
    // reparameterize_path_by_child arms; hashfn carries
    // satisfies_hash_partition + partition deparse.
    ProdWeight { name: "partalt:clauses", default: 1.6 },
    ProdWeight { name: "partalt:runtime", default: 1.5 },
    ProdWeight { name: "partalt:multilevel", default: 1.3 },
    ProdWeight { name: "partalt:pwjmerge", default: 1.4 },
    ProdWeight { name: "partalt:pwagg", default: 1.3 },
    ProdWeight { name: "partalt:reparam", default: 1.3 },
    ProdWeight { name: "partalt:hashfn", default: 1.0 },
    // stats (STATS lane): extended-stats build+apply (dep/ndist/mcv),
    // per-column ANALYZE stats-build (analyze), selfuncs restriction+join
    // estimation (sel), and index cost estimators (index).
    ProdWeight { name: "stats:dep", default: 1.3 },
    ProdWeight { name: "stats:ndist", default: 1.1 },
    ProdWeight { name: "stats:mcv", default: 1.3 },
    ProdWeight { name: "stats:analyze", default: 1.2 },
    ProdWeight { name: "stats:sel", default: 1.4 },
    ProdWeight { name: "stats:index", default: 1.2 },
    // planner (fuzz-planner): optimizer path/plan-shape drain family
    // selection. geqo/winrun carry the biggest cold regions (whole GEQO
    // subsystem; find_window_run_conditions), so they weigh highest.
    ProdWeight { name: "planner:sample", default: 1.4 },
    ProdWeight { name: "planner:tidrange", default: 1.4 },
    ProdWeight { name: "planner:groupsort", default: 1.3 },
    ProdWeight { name: "planner:setop", default: 1.3 },
    ProdWeight { name: "planner:winrun", default: 1.6 },
    ProdWeight { name: "planner:geqo", default: 1.6 },
    ProdWeight { name: "planner:bitmapor", default: 1.5 },
    // partition (PARTITION lane): partition-wise-join bound-MERGE drain.
    // pwrange/pwlist carry the partbounds.c merge_range_bounds /
    // merge_list_bounds mass (compatibly-partitioned parents with DIFFERENT
    // bounds); route_err carries ExecBuildSlotPartitionKeyDescription;
    // attach_default carries check_default_partition_contents; colmap
    // carries adjust_partition_colnos[_using_map]; prune_prefix carries
    // get_steps_using_prefix_recurse; constraintdef carries
    // get_partition_qual_relid + the partition_bounds_equal fast-path arm.
    ProdWeight { name: "partition:pwrange", default: 2.0 },
    ProdWeight { name: "partition:pwlist", default: 2.0 },
    ProdWeight { name: "partition:route_err", default: 1.0 },
    ProdWeight { name: "partition:attach_default", default: 1.0 },
    ProdWeight { name: "partition:colmap", default: 1.0 },
    ProdWeight { name: "partition:prune_prefix", default: 1.2 },
    ProdWeight { name: "partition:constraintdef", default: 1.0 },
    // triggers (Track-B): trigger/rule/event-trigger EXECUTION-drain shapes.
    // Event-trigger and partitioned-clone sections target the largest hollow
    // spans (event_trigger.c ddl/sql_drop SRF handlers, trigger.c
    // parent-clone), so they carry above-parity section weight.
    ProdWeight { name: "triggers:setconstr", default: 1.3 },
    ProdWeight { name: "triggers:enabledisable", default: 1.1 },
    ProdWeight { name: "triggers:replrole", default: 1.0 },
    ProdWeight { name: "triggers:rename", default: 1.2 },
    ProdWeight { name: "triggers:parted", default: 1.3 },
    ProdWeight { name: "triggers:eventddl", default: 1.5 },
    ProdWeight { name: "triggers:eventdrop", default: 1.4 },
    ProdWeight { name: "triggers:rules", default: 1.1 },
    ProdWeight { name: "triggers:viewupd", default: 0.9 },
    // PLPGSQL lane (crate::plpg2): plpgsql/SPI residual-arm drain. Group
    // kinds, then per-group sub-forms.
    ProdWeight { name: "plpg2:raise", default: 2.5 },
    ProdWeight { name: "plpg2:diag", default: 1.5 },
    ProdWeight { name: "plpg2:srf", default: 2.0 },
    ProdWeight { name: "plpg2:cursor", default: 2.5 },
    ProdWeight { name: "plpg2:foreach", default: 1.5 },
    ProdWeight { name: "plpg2:dynexec", default: 2.0 },
    ProdWeight { name: "plpg2:record", default: 2.0 },
    ProdWeight { name: "plpg2:call", default: 1.5 },
    ProdWeight { name: "plpg2:do", default: 1.5 },
    ProdWeight { name: "plpg2:assert", default: 1.5 },
    ProdWeight { name: "plpg2:vartype", default: 1.5 },
    // raise sub-forms.
    ProdWeight { name: "plpg2:raise:full", default: 2.5 },
    ProdWeight { name: "plpg2:raise:cond", default: 1.0 },
    ProdWeight { name: "plpg2:raise:sqlstate", default: 1.0 },
    ProdWeight { name: "plpg2:raise:level", default: 1.5 },
    ProdWeight { name: "plpg2:raise:reraise", default: 1.0 },
    // diag sub-forms.
    ProdWeight { name: "plpg2:diag:cur", default: 1.0 },
    ProdWeight { name: "plpg2:diag:stacked", default: 1.0 },
    // srf sub-forms.
    ProdWeight { name: "plpg2:srf:table", default: 1.5 },
    ProdWeight { name: "plpg2:srf:setof", default: 1.5 },
    ProdWeight { name: "plpg2:srf:record", default: 1.5 },
    // cursor sub-forms.
    ProdWeight { name: "plpg2:cursor:forc", default: 1.5 },
    ProdWeight { name: "plpg2:cursor:explicit", default: 1.5 },
    ProdWeight { name: "plpg2:cursor:exec", default: 1.5 },
    // dynexec sub-forms.
    ProdWeight { name: "plpg2:dyn2:into", default: 1.5 },
    ProdWeight { name: "plpg2:dyn2:dml", default: 1.5 },
    // do sub-forms.
    ProdWeight { name: "plpg2:do:plain", default: 1.0 },
    ProdWeight { name: "plpg2:do:exc", default: 1.0 },
    // assert sub-forms.
    ProdWeight { name: "plpg2:assert:pass", default: 1.5 },
    ProdWeight { name: "plpg2:assert:fail", default: 1.0 },
    // regex (crafted-pattern engine drain): shape selection + the ok/err
    // fuel knob. The complexity-guard shapes (cat/bigquant/deepnest) and
    // the always-error invalid shape carry the crash/hang/ETOOBIG surface;
    // the function/operator shapes carry differential result identity.
    ProdWeight { name: "regex:match", default: 1.5 },
    ProdWeight { name: "regex:matches", default: 1.5 },
    ProdWeight { name: "regex:replace", default: 1.5 },
    ProdWeight { name: "regex:count", default: 1.0 },
    ProdWeight { name: "regex:substr", default: 1.0 },
    ProdWeight { name: "regex:split", default: 1.0 },
    ProdWeight { name: "regex:op", default: 1.5 },
    ProdWeight { name: "regex:cat", default: 1.5 },
    ProdWeight { name: "regex:bigquant", default: 1.5 },
    ProdWeight { name: "regex:deepnest", default: 1.5 },
    ProdWeight { name: "regex:backref", default: 1.2 },
    ProdWeight { name: "regex:charclass", default: 1.2 },
    ProdWeight { name: "regex:anchor", default: 1.0 },
    ProdWeight { name: "regex:flags", default: 1.0 },
    ProdWeight { name: "regex:invalid", default: 1.0 },
    ProdWeight { name: "regex:ok", default: 4.0 },
    ProdWeight { name: "regex:err", default: 1.0 },
    // aggwin (AGGWIN): window-frame + grouping-sets-spill residue families.
    ProdWeight { name: "aggwin", default: 1.0 },
    ProdWeight { name: "aggwin:groups", default: 3.0 },
    ProdWeight { name: "aggwin:exclude", default: 3.0 },
    ProdWeight { name: "aggwin:rangetyped", default: 2.5 },
    ProdWeight { name: "aggwin:rescan", default: 2.0 },
    ProdWeight { name: "aggwin:namedchain", default: 1.5 },
    ProdWeight { name: "aggwin:filter", default: 1.5 },
    ProdWeight { name: "aggwin:moving", default: 2.0 },
    ProdWeight { name: "aggwin:hashaggspill", default: 1.5 },
    // indexam (INDEXAM) shape picks: GIN / GiST / SP-GiST operator-class
    // drain + partial/expr/INCLUDE cover arms. Even weights — each shape
    // covers a disjoint C region (a whole AM's internals).
    ProdWeight { name: "indexam:gin", default: 1.0 },
    ProdWeight { name: "indexam:gist", default: 1.0 },
    ProdWeight { name: "indexam:spgist", default: 1.0 },
    ProdWeight { name: "indexam:cover", default: 1.0 },
    // aclrls (ACL/RLS drain): shape selection. The RLS-application shapes
    // carry the rowsecurity.c mass (get_row_security_policies +
    // add_security_quals + add_with_check_options), so they sit above the
    // rest; own:shdep carries the pg_shdepend owner-dependency machinery
    // adtmisc never builds; grant:rel carries the ExecGrant_* delegation
    // chain; aclitem is a cheap scalar family kept at a token weight.
    ProdWeight { name: "aclrls:rls:apply", default: 2.0 },
    ProdWeight { name: "aclrls:rls:force", default: 1.3 },
    ProdWeight { name: "aclrls:rls:ddl", default: 1.5 },
    ProdWeight { name: "aclrls:grant:rel", default: 1.5 },
    ProdWeight { name: "aclrls:grant:defacl", default: 1.2 },
    ProdWeight { name: "aclrls:own:shdep", default: 1.5 },
    ProdWeight { name: "aclrls:role:member", default: 1.0 },
    ProdWeight { name: "aclrls:aclitem", default: 0.8 },
    // Error-fuel bias (deliberate matched errors: WITH CHECK violations,
    // drop-role-with-deps, denied writes, bogus aclitem literals). Biased
    // toward the non-error arm per the findings-budget rule.
    ProdWeight { name: "aclrls:ok", default: 3.0 },
    ProdWeight { name: "aclrls:err", default: 1.0 },
    // lockcursor: shape picks + sub-picks (LOCK TABLE all-modes / advisory
    // ref-counting / nested savepoints / savepoint + NOTIFY error arms).
    ProdWeight { name: "lockcursor:locktable", default: 2.0 },
    ProdWeight { name: "lockcursor:advisory", default: 1.5 },
    ProdWeight { name: "lockcursor:savepoint", default: 2.0 },
    ProdWeight { name: "lockcursor:savepoint_err", default: 1.5 },
    ProdWeight { name: "lockcursor:notify", default: 1.5 },
    ProdWeight { name: "lockcursor:lt:err", default: 1.0 },
    ProdWeight { name: "lockcursor:lt:noerr", default: 2.0 },
    ProdWeight { name: "lockcursor:sp:commit", default: 1.0 },
    ProdWeight { name: "lockcursor:sp:rollback", default: 1.0 },
    // largeobj (LARGEOBJECT): the lo_* / pg_largeobject drain module. Action
    // mix favors the write/read/seek/truncate descriptor and bytea paths;
    // create/creat/meta/err at modest weight.
    ProdWeight { name: "largeobj:bytea", default: 2.0 },
    ProdWeight { name: "largeobj:creat", default: 1.0 },
    ProdWeight { name: "largeobj:putget", default: 2.0 },
    ProdWeight { name: "largeobj:fd", default: 2.5 },
    ProdWeight { name: "largeobj:chunk", default: 2.0 },
    ProdWeight { name: "largeobj:trunc", default: 2.0 },
    ProdWeight { name: "largeobj:lo64", default: 1.5 },
    ProdWeight { name: "largeobj:meta", default: 1.0 },
    ProdWeight { name: "largeobj:err", default: 1.5 },
    // lo_creat mode arg.
    ProdWeight { name: "largeobj:creat:neg", default: 1.0 },
    ProdWeight { name: "largeobj:creat:rw", default: 1.0 },
    // descriptor seek whence.
    ProdWeight { name: "largeobj:whence:set", default: 1.5 },
    ProdWeight { name: "largeobj:whence:cur", default: 1.0 },
    ProdWeight { name: "largeobj:whence:end", default: 1.0 },
    // truncate direction.
    ProdWeight { name: "largeobj:trunc:grow", default: 1.0 },
    ProdWeight { name: "largeobj:trunc:shrink", default: 1.0 },
    // error arms.
    ProdWeight { name: "largeobj:err:getne", default: 1.0 },
    ProdWeight { name: "largeobj:err:putne", default: 1.0 },
    ProdWeight { name: "largeobj:err:unlinkne", default: 1.0 },
    ProdWeight { name: "largeobj:err:openne", default: 1.0 },
    ProdWeight { name: "largeobj:err:badfdread", default: 1.0 },
    ProdWeight { name: "largeobj:err:badfdwrite", default: 1.0 },
    ProdWeight { name: "largeobj:err:negseek", default: 1.0 },
    ProdWeight { name: "largeobj:err:badwhence", default: 1.0 },
    ProdWeight { name: "largeobj:err:negfrag", default: 1.0 },
    ProdWeight { name: "largeobj:err:readonly", default: 1.0 },
    ProdWeight { name: "largeobj:err:writeonly", default: 1.0 },
    // plancache (PLANCACHE): generic-vs-custom plan drain. mode carries the
    // force_generic/force_custom result-identity sweep; transition carries
    // the 5-custom->generic flip; prune carries generic-plan runtime
    // partition pruning; invalidate carries plancache DDL invalidation;
    // introspect carries pg_prepared_statements. q:* pick the prepared-
    // query shape; prune:* the pruning-qual shape; explain gates the
    // EXPLAIN EXECUTE plan-text witness at the flip boundary.
    ProdWeight { name: "plancache:mode", default: 2.0 },
    ProdWeight { name: "plancache:transition", default: 1.5 },
    ProdWeight { name: "plancache:prune", default: 2.0 },
    ProdWeight { name: "plancache:invalidate", default: 1.5 },
    ProdWeight { name: "plancache:introspect", default: 0.8 },
    ProdWeight { name: "plancache:q:rows", default: 2.0 },
    ProdWeight { name: "plancache:q:agg", default: 2.0 },
    ProdWeight { name: "plancache:q:partial", default: 1.5 },
    ProdWeight { name: "plancache:q:in", default: 1.5 },
    ProdWeight { name: "plancache:prune:range", default: 1.5 },
    ProdWeight { name: "plancache:prune:eq", default: 1.5 },
    ProdWeight { name: "plancache:explain", default: 1.0 },
    ProdWeight { name: "plancache:explain:none", default: 2.0 },
    // vacuum (VACUUM lane): SQL-maintenance option-matrix shape selection.
    ProdWeight { name: "vac:create", default: 1.5 },
    ProdWeight { name: "vac:createpart", default: 0.8 },
    ProdWeight { name: "vac:drop", default: 0.3 },
    ProdWeight { name: "vac:vacuum", default: 3.0 },
    ProdWeight { name: "vac:analyze", default: 1.5 },
    ProdWeight { name: "vac:cluster", default: 1.5 },
    ProdWeight { name: "vac:reindex", default: 1.5 },
    ProdWeight { name: "vac:vacpart", default: 1.2 },
    ProdWeight { name: "vac:reindexschema", default: 0.6 },
    ProdWeight { name: "vac:reindextblspc", default: 0.6 },
    ProdWeight { name: "vac:dbstats", default: 0.6 },
    ProdWeight { name: "vac:badcombo", default: 0.4 },
    // create picks.
    ProdWeight { name: "vac:ff:30", default: 1.5 },
    ProdWeight { name: "vac:ff:70", default: 1.0 },
    ProdWeight { name: "vac:ff:100", default: 1.0 },
    ProdWeight { name: "vac:rows:1000", default: 1.5 },
    ProdWeight { name: "vac:rows:3000", default: 1.5 },
    ProdWeight { name: "vac:rows:6000", default: 1.0 },
    ProdWeight { name: "vac:clusteron:yes", default: 1.5 },
    ProdWeight { name: "vac:clusteron:no", default: 1.0 },
    ProdWeight { name: "vac:pp:2", default: 1.5 },
    ProdWeight { name: "vac:pp:3", default: 1.0 },
    // VACUUM form + legacy keyword shapes.
    ProdWeight { name: "vac:v:legacy", default: 1.5 },
    ProdWeight { name: "vac:v:paren", default: 3.0 },
    ProdWeight { name: "vac:vl:plain", default: 1.5 },
    ProdWeight { name: "vac:vl:full", default: 1.0 },
    ProdWeight { name: "vac:vl:freeze", default: 1.0 },
    ProdWeight { name: "vac:vl:analyze", default: 1.5 },
    ProdWeight { name: "vac:vl:fullanalyze", default: 1.0 },
    ProdWeight { name: "vac:vl:freezeanalyze", default: 1.0 },
    // VACUUM parenthesized option values.
    ProdWeight { name: "vac:full:yes", default: 1.0 },
    ProdWeight { name: "vac:full:no", default: 2.5 },
    ProdWeight { name: "vac:ic:on", default: 1.0 },
    ProdWeight { name: "vac:ic:off", default: 1.0 },
    ProdWeight { name: "vac:ic:auto", default: 1.0 },
    ProdWeight { name: "vac:pt:on", default: 1.0 },
    ProdWeight { name: "vac:pt:off", default: 1.0 },
    ProdWeight { name: "vac:pm:on", default: 1.0 },
    ProdWeight { name: "vac:pm:off", default: 1.0 },
    ProdWeight { name: "vac:tr:on", default: 1.0 },
    ProdWeight { name: "vac:tr:off", default: 1.0 },
    ProdWeight { name: "vac:par:0", default: 1.0 },
    ProdWeight { name: "vac:par:1", default: 1.0 },
    ProdWeight { name: "vac:par:2", default: 1.0 },
    ProdWeight { name: "vac:par:4", default: 0.5 },
    ProdWeight { name: "vac:bul:small", default: 1.0 },
    ProdWeight { name: "vac:bul:med", default: 1.0 },
    ProdWeight { name: "vac:bul:big", default: 1.0 },
    // ANALYZE option shapes.
    ProdWeight { name: "vac:an:plain", default: 1.5 },
    ProdWeight { name: "vac:an:cols", default: 1.5 },
    ProdWeight { name: "vac:an:verbose", default: 1.0 },
    ProdWeight { name: "vac:an:verbosecols", default: 1.0 },
    ProdWeight { name: "vac:an:skiplocked", default: 1.0 },
    ProdWeight { name: "vac:an:bul", default: 1.0 },
    // CLUSTER shapes.
    ProdWeight { name: "vac:cl:usingpk", default: 1.5 },
    ProdWeight { name: "vac:cl:usingidx", default: 1.5 },
    ProdWeight { name: "vac:cl:verboseidx", default: 1.0 },
    ProdWeight { name: "vac:cl:noidx", default: 1.0 },
    ProdWeight { name: "vac:cl:verbosenoidx", default: 1.0 },
    // REINDEX shapes.
    ProdWeight { name: "vac:ri:indexpk", default: 1.5 },
    ProdWeight { name: "vac:ri:indexa", default: 1.5 },
    ProdWeight { name: "vac:ri:verboseindex", default: 1.0 },
    ProdWeight { name: "vac:ri:table", default: 1.5 },
    ProdWeight { name: "vac:ri:verbosetable", default: 1.0 },
    ProdWeight { name: "vac:ri:indexconc", default: 1.0 },
    ProdWeight { name: "vac:ri:tableconc", default: 1.0 },
    // Partitioned-maintenance propagation shapes.
    ProdWeight { name: "vac:vp:vacuum", default: 1.5 },
    ProdWeight { name: "vac:vp:analyze", default: 1.5 },
    ProdWeight { name: "vac:vp:vacanalyze", default: 1.5 },
    ProdWeight { name: "vac:vp:freeze", default: 1.0 },
    ProdWeight { name: "vac:vp:leaf", default: 1.0 },
    // Group-local REINDEX SCHEMA / TABLESPACE + dbstats shapes.
    ProdWeight { name: "vac:rs:plain", default: 1.5 },
    ProdWeight { name: "vac:rs:conc", default: 1.0 },
    ProdWeight { name: "vac:rt:plain", default: 1.5 },
    ProdWeight { name: "vac:rt:conc", default: 1.0 },
    ProdWeight { name: "vac:ds:plain", default: 1.5 },
    ProdWeight { name: "vac:ds:verbose", default: 1.0 },
    // Deliberate error-identity combinations.
    ProdWeight { name: "vac:bad:fullparallel", default: 1.0 },
    ProdWeight { name: "vac:bad:fullbul", default: 1.0 },
    ProdWeight { name: "vac:bad:statstable", default: 1.0 },
    ProdWeight { name: "vac:bad:analyzeonlylist", default: 1.0 },
    // seqident (SEQIDENT): sequence / GENERATED IDENTITY / serial group
    // form selection and intra-group shape steering. create/alter/setval/
    // currval carry the sequence.c nextval/currval/setval + ALTER surface;
    // cycle/astype carry the wraparound + boundary (2200H) overflow arms;
    // identity/altercol/serial carry the parse_utilcmd + tablecmds identity
    // and serial-desugaring arms; owned/dropdep carry the ownership /
    // dependency-cascade drain.
    ProdWeight { name: "seqident:create", default: 2.0 },
    ProdWeight { name: "seqident:astype", default: 1.5 },
    ProdWeight { name: "seqident:cycle", default: 1.5 },
    ProdWeight { name: "seqident:setval", default: 1.5 },
    ProdWeight { name: "seqident:currval", default: 1.0 },
    ProdWeight { name: "seqident:alter", default: 1.5 },
    ProdWeight { name: "seqident:identity", default: 2.0 },
    ProdWeight { name: "seqident:serial", default: 1.5 },
    ProdWeight { name: "seqident:altercol", default: 1.5 },
    ProdWeight { name: "seqident:owned", default: 1.0 },
    ProdWeight { name: "seqident:dropdep", default: 1.0 },
    // Ascending vs descending increment direction (create/cycle).
    ProdWeight { name: "seqident:asc", default: 2.0 },
    ProdWeight { name: "seqident:desc", default: 1.0 },
    // CACHE 1 vs CACHE>1 (single-session values are identical either way;
    // the arm still exercises the cache path).
    ProdWeight { name: "seqident:cache1", default: 2.0 },
    ProdWeight { name: "seqident:cachehi", default: 1.0 },
    // GENERATED ALWAYS vs BY DEFAULT identity kind.
    ProdWeight { name: "seqident:id:always", default: 1.0 },
    ProdWeight { name: "seqident:id:bydefault", default: 1.0 },
    // ritrig (RITRIG): referential-integrity FK-action / RI-trigger /
    // constraint-validation shape selection. cascade+setnull+setdefault
    // carry the ri_Cascade/ri_set drain; restrict+matchfull carry the
    // ri_restrict + MATCH FULL partial-null arms; deferred+notvalid carry
    // the deferred-check + validateForeignKeyConstraint arms; part carries
    // the cross-partition RI routing; check/exclude/unique carry the sibling
    // non-FK constraint identities.
    ProdWeight { name: "ritrig:cascade", default: 2.5 },
    ProdWeight { name: "ritrig:setnull", default: 2.0 },
    ProdWeight { name: "ritrig:setdefault", default: 1.5 },
    ProdWeight { name: "ritrig:restrict", default: 2.0 },
    ProdWeight { name: "ritrig:matchfull", default: 1.5 },
    ProdWeight { name: "ritrig:composite", default: 1.5 },
    ProdWeight { name: "ritrig:selfref", default: 1.5 },
    ProdWeight { name: "ritrig:deferred", default: 1.5 },
    ProdWeight { name: "ritrig:notvalid", default: 1.5 },
    ProdWeight { name: "ritrig:part", default: 1.5 },
    ProdWeight { name: "ritrig:check", default: 1.0 },
    ProdWeight { name: "ritrig:exclude", default: 1.0 },
    ProdWeight { name: "ritrig:unique", default: 1.0 },
    // deferred sub-shape split (satisfy-before-commit vs fail-at-commit vs
    // force-immediate mid-txn).
    ProdWeight { name: "ritrig:deferred:satisfy", default: 1.0 },
    ProdWeight { name: "ritrig:deferred:commitfail", default: 1.0 },
    ProdWeight { name: "ritrig:deferred:setimm", default: 1.0 },
    // rangeops module (RANGEOPS): range/multirange operator + function +
    // custom-range-type surface. Action mix (scalar dominant — the
    // rangetypes.c operator/function fuel; mr carries multirangetypes.c;
    // flags carries the bound-flag/canonicalization/infinite-edge surface;
    // custom emits self-contained CREATE TYPE AS RANGE groups).
    ProdWeight { name: "rangeops:scalar", default: 5.0 },
    ProdWeight { name: "rangeops:mr", default: 3.0 },
    ProdWeight { name: "rangeops:flags", default: 2.0 },
    ProdWeight { name: "rangeops:agg", default: 1.5 },
    ProdWeight { name: "rangeops:custom", default: 1.0 },
    ProdWeight { name: "rangeops:err", default: 1.0 },
    // rangeops: range scalar expression class.
    ProdWeight { name: "ro:cmp", default: 4.0 },
    ProdWeight { name: "ro:setop", default: 2.0 },
    ProdWeight { name: "ro:elem", default: 2.0 },
    ProdWeight { name: "ro:bounds", default: 3.0 },
    ProdWeight { name: "ro:merge", default: 1.5 },
    ProdWeight { name: "ro:hash", default: 1.0 },
    // rangeops: multirange expression class.
    ProdWeight { name: "ro:mr:op", default: 3.0 },
    ProdWeight { name: "ro:mr:mixed", default: 2.5 },
    ProdWeight { name: "ro:mr:bounds", default: 2.0 },
    ProdWeight { name: "ro:mr:unnest", default: 1.5 },
    ProdWeight { name: "ro:mr:ctor", default: 2.0 },
    // rangeops: custom-range-type variant.
    ProdWeight { name: "ro:ct:int", default: 2.0 },
    ProdWeight { name: "ro:ct:float", default: 1.5 },
    ProdWeight { name: "ro:ct:text", default: 1.5 },
    // udt (UDT lane): user-defined-type family selection. domain carries the
    // constraint-decision surface (the HIGH-severity target), so it keeps the
    // highest share; enum carries the sort-order/ALTER-ADD-VALUE surface;
    // composite carries the row-type I/O + ATTRIBUTE surgery.
    ProdWeight { name: "udt:enum", default: 2.0 },
    ProdWeight { name: "udt:composite", default: 2.0 },
    ProdWeight { name: "udt:domain", default: 3.0 },
    // Domain base type inside a domain group (int/text/numeric CHECK fuel).
    ProdWeight { name: "udt:dom:int", default: 1.0 },
    ProdWeight { name: "udt:dom:text", default: 1.0 },
    ProdWeight { name: "udt:dom:num", default: 1.0 },
    // Domain-over-domain vs flat domain (the nested inner+outer CHECK path).
    ProdWeight { name: "udt:dom:nest", default: 1.0 },
    ProdWeight { name: "udt:dom:flat", default: 1.0 },
    // arrayops (Track-B): array operator/function/subscript drain. Top-level
    // shapes.
    ProdWeight { name: "arr:sub", default: 2.5 },
    ProdWeight { name: "arr:dims", default: 1.5 },
    ProdWeight { name: "arr:op", default: 2.5 },
    ProdWeight { name: "arr:anyall", default: 1.5 },
    ProdWeight { name: "arr:fn", default: 2.5 },
    ProdWeight { name: "arr:str", default: 1.5 },
    ProdWeight { name: "arr:agg", default: 1.5 },
    ProdWeight { name: "arr:trim", default: 1.2 },
    ProdWeight { name: "arr:ctor", default: 1.2 },
    ProdWeight { name: "arr:update", default: 1.0 },
    // arrayops: subscript sub-shapes.
    ProdWeight { name: "arr:sub:elem", default: 1.2 },
    ProdWeight { name: "arr:sub:slice", default: 1.2 },
    ProdWeight { name: "arr:sub:md", default: 1.0 },
    ProdWeight { name: "arr:sub:oob", default: 0.8 },
    // arrayops: operator sub-shapes.
    ProdWeight { name: "arr:op:contain", default: 1.2 },
    ProdWeight { name: "arr:op:concat", default: 1.2 },
    ProdWeight { name: "arr:op:cmp", default: 1.0 },
    // arrayops: function sub-shapes.
    ProdWeight { name: "arr:fn:mutate", default: 1.0 },
    ProdWeight { name: "arr:fn:cat", default: 1.0 },
    ProdWeight { name: "arr:fn:find", default: 1.0 },
    ProdWeight { name: "arr:fn:replace", default: 1.0 },
    ProdWeight { name: "arr:fn:fill", default: 0.8 },
    ProdWeight { name: "arr:fn:fill:ok", default: 1.2 },
    ProdWeight { name: "arr:fn:fill:err", default: 0.6 },
    // arrayops: agg/unnest sub-shapes.
    ProdWeight { name: "arr:agg:agg", default: 1.0 },
    ProdWeight { name: "arr:agg:unnest", default: 1.0 },
    ProdWeight { name: "arr:agg:ord", default: 1.0 },
    // arrayops: trim/sample sub-shapes.
    ProdWeight { name: "arr:trim:trim", default: 1.2 },
    ProdWeight { name: "arr:trim:sample", default: 0.8 },
    ProdWeight { name: "arr:trim:sample:ok", default: 1.2 },
    ProdWeight { name: "arr:trim:sample:err", default: 0.6 },
    // arrayops: constructor sub-shapes.
    ProdWeight { name: "arr:ctor:ok", default: 1.2 },
    ProdWeight { name: "arr:ctor:err", default: 0.8 },
    // altertable (Track-B): ALTER TABLE rewrite/phase EXECUTION drain.
    // Family selectors — the coltype/rewrite paths carry the ATRewriteTable
    // mass, so they sit above the metadata-only families.
    ProdWeight { name: "alt:coltype_using", default: 2.5 },
    ProdWeight { name: "alt:coltype_norewrite", default: 1.0 },
    ProdWeight { name: "alt:addcol_const", default: 1.5 },
    ProdWeight { name: "alt:addcol_volatile", default: 1.5 },
    ProdWeight { name: "alt:setdropdefault", default: 1.0 },
    ProdWeight { name: "alt:notnull", default: 1.0 },
    ProdWeight { name: "alt:setopts", default: 1.0 },
    ProdWeight { name: "alt:dropcol", default: 1.0 },
    ProdWeight { name: "alt:logged", default: 1.5 },
    ProdWeight { name: "alt:generated", default: 1.5 },
    ProdWeight { name: "alt:multi", default: 2.0 },
    ProdWeight { name: "alt:coltype_indexed", default: 2.0 },
    ProdWeight { name: "alt:inherit", default: 1.5 },
    ProdWeight { name: "alt:replident", default: 1.0 },
    ProdWeight { name: "alt:partition", default: 1.5 },
    // Base-load row count.
    ProdWeight { name: "alt:rows:200", default: 2.0 },
    ProdWeight { name: "alt:rows:800", default: 1.5 },
    ProdWeight { name: "alt:rows:2000", default: 1.0 },
    // coltype (USING) transform shapes.
    ProdWeight { name: "alt:cu:text2int", default: 1.5 },
    ProdWeight { name: "alt:cu:int2text", default: 1.5 },
    ProdWeight { name: "alt:cu:int2numeric", default: 1.5 },
    ProdWeight { name: "alt:cu:text2bool", default: 1.0 },
    ProdWeight { name: "alt:cu:int2bigint", default: 1.5 },
    // coltype (no-rewrite / binary-coercible) shapes.
    ProdWeight { name: "alt:cn:vc2text", default: 1.0 },
    ProdWeight { name: "alt:cn:widen", default: 1.0 },
    // ADD COLUMN (constant default) shapes.
    ProdWeight { name: "alt:ac:int", default: 1.5 },
    ProdWeight { name: "alt:ac:text", default: 1.0 },
    ProdWeight { name: "alt:ac:bool", default: 1.0 },
    ProdWeight { name: "alt:ac:numeric", default: 1.0 },
    ProdWeight { name: "alt:ac:nodefault", default: 1.0 },
    ProdWeight { name: "alt:ac:notnull", default: 1.0 },
    // ADD COLUMN (volatile default) shapes.
    ProdWeight { name: "alt:av:nextval", default: 1.5 },
    ProdWeight { name: "alt:av:random", default: 1.0 },
    // SET/DROP DEFAULT.
    ProdWeight { name: "alt:dd:set", default: 1.0 },
    ProdWeight { name: "alt:dd:drop", default: 1.0 },
    // SET STORAGE/STATISTICS/COMPRESSION.
    ProdWeight { name: "alt:so:storage_ext", default: 1.0 },
    ProdWeight { name: "alt:so:storage_main", default: 1.0 },
    ProdWeight { name: "alt:so:storage_plain", default: 1.0 },
    ProdWeight { name: "alt:so:stats", default: 1.0 },
    ProdWeight { name: "alt:so:compression", default: 1.0 },
    // DROP COLUMN.
    ProdWeight { name: "alt:dc:existing", default: 1.0 },
    ProdWeight { name: "alt:dc:addrop", default: 1.0 },
    // GENERATED STORED.
    ProdWeight { name: "alt:gen:add", default: 1.5 },
    ProdWeight { name: "alt:gen:addcol_then_gen", default: 1.0 },
    // type change under an index.
    ProdWeight { name: "alt:ci:plain", default: 1.0 },
    ProdWeight { name: "alt:ci:unique", default: 1.5 },
    // inheritance.
    ProdWeight { name: "alt:inh:cascade_add", default: 1.5 },
    ProdWeight { name: "alt:inh:only_set", default: 1.5 },
    ProdWeight { name: "alt:inh:noinherit", default: 1.0 },
    // replica identity.
    ProdWeight { name: "alt:ri:full", default: 1.5 },
    ProdWeight { name: "alt:ri:nothing", default: 1.0 },
    ProdWeight { name: "alt:ri:default", default: 1.0 },
    ProdWeight { name: "alt:ri:index", default: 1.5 },
    // partitioned-parent propagation.
    ProdWeight { name: "alt:pt:addcol", default: 1.5 },
    ProdWeight { name: "alt:pt:setdefault", default: 1.0 },
    ProdWeight { name: "alt:pt:coltype", default: 1.5 },
    // byteaenc (BYTEAENC): bytea + encode/decode + encoding-convert drain.
    // Top-level shape selection.
    ProdWeight { name: "byteaenc:enc", default: 2.0 },
    ProdWeight { name: "byteaenc:encerr", default: 1.5 },
    ProdWeight { name: "byteaenc:getset", default: 1.8 },
    ProdWeight { name: "byteaenc:ops", default: 2.0 },
    ProdWeight { name: "byteaenc:cmp", default: 1.5 },
    ProdWeight { name: "byteaenc:cast", default: 1.5 },
    ProdWeight { name: "byteaenc:conv", default: 1.2 },
    // enc sub-shapes.
    ProdWeight { name: "byteaenc:enc:hex", default: 1.2 },
    ProdWeight { name: "byteaenc:enc:b64", default: 1.2 },
    ProdWeight { name: "byteaenc:enc:esc", default: 1.2 },
    ProdWeight { name: "byteaenc:enc:rt", default: 1.0 },
    // decode error sub-shapes.
    ProdWeight { name: "byteaenc:err:hex", default: 1.0 },
    ProdWeight { name: "byteaenc:err:b64", default: 1.0 },
    ProdWeight { name: "byteaenc:err:esc", default: 1.0 },
    ProdWeight { name: "byteaenc:err:fmt", default: 0.6 },
    // get/set byte/bit sub-shapes.
    ProdWeight { name: "byteaenc:gs:byte", default: 1.2 },
    ProdWeight { name: "byteaenc:gs:bit", default: 1.2 },
    ProdWeight { name: "byteaenc:gs:err", default: 0.9 },
    // function/operator sub-shapes.
    ProdWeight { name: "byteaenc:op:cat", default: 1.0 },
    ProdWeight { name: "byteaenc:op:sub", default: 1.2 },
    ProdWeight { name: "byteaenc:op:pos", default: 1.0 },
    ProdWeight { name: "byteaenc:op:trim", default: 1.0 },
    ProdWeight { name: "byteaenc:op:len", default: 1.0 },
    ProdWeight { name: "byteaenc:op:overlay", default: 1.0 },
    // comparison sub-shapes.
    ProdWeight { name: "byteaenc:cmp:ops", default: 1.2 },
    ProdWeight { name: "byteaenc:cmp:order", default: 0.8 },
    ProdWeight { name: "byteaenc:cmp:fn", default: 1.0 },
    // cast sub-shapes.
    ProdWeight { name: "byteaenc:ca:text", default: 1.2 },
    ProdWeight { name: "byteaenc:ca:int", default: 1.0 },
    ProdWeight { name: "byteaenc:ca:guc", default: 1.0 },
    // convert sub-shapes.
    ProdWeight { name: "byteaenc:cv:roundtrip", default: 1.2 },
    ProdWeight { name: "byteaenc:cv:bad", default: 0.8 },
    // srf (Track-B SRF / nodeFunctionscan): production-family selection for
    // the generate_series / generate_subscripts / unnest / ROWS FROM /
    // string_to_table + SRF-context-error surface. Numeric generate_series
    // stays in numx:series; this module owns the int4/int8/timestamp arms
    // and the whole functionscan / ordinality / lockstep-tlist surface.
    ProdWeight { name: "srf:gs_int", default: 1.5 },
    ProdWeight { name: "srf:gs_ts", default: 1.5 },
    ProdWeight { name: "srf:gs_err", default: 0.8 },
    ProdWeight { name: "srf:subscripts", default: 1.2 },
    ProdWeight { name: "srf:unnest1", default: 1.5 },
    ProdWeight { name: "srf:unnest_multi", default: 1.3 },
    ProdWeight { name: "srf:rowsfrom", default: 1.3 },
    ProdWeight { name: "srf:tlist", default: 1.3 },
    ProdWeight { name: "srf:lateral", default: 1.3 },
    ProdWeight { name: "srf:s2t", default: 1.0 },
    ProdWeight { name: "srf:case_err", default: 0.8 },
    // stringfunc (STRINGFUNC lane): top-level string-function shape mix
    // over varlena/oracle_compat/formatting/ascii. Every shape is one
    // literal-driven scalar SELECT; err rides low per the findings-budget
    // rule (deliberate matched-SQLSTATE arms).
    ProdWeight { name: "strf:substr", default: 1.5 },
    ProdWeight { name: "strf:overlay", default: 1.2 },
    ProdWeight { name: "strf:pos", default: 1.0 },
    ProdWeight { name: "strf:leftright", default: 1.0 },
    ProdWeight { name: "strf:pad", default: 1.2 },
    ProdWeight { name: "strf:trim", default: 1.5 },
    ProdWeight { name: "strf:reprep", default: 1.0 },
    ProdWeight { name: "strf:translate", default: 1.0 },
    ProdWeight { name: "strf:split", default: 1.3 },
    ProdWeight { name: "strf:concat", default: 1.0 },
    ProdWeight { name: "strf:format", default: 1.3 },
    ProdWeight { name: "strf:quote", default: 1.0 },
    ProdWeight { name: "strf:case", default: 1.2 },
    ProdWeight { name: "strf:asciichr", default: 1.0 },
    ProdWeight { name: "strf:revstart", default: 1.0 },
    ProdWeight { name: "strf:len", default: 1.0 },
    ProdWeight { name: "strf:norm", default: 1.3 },
    ProdWeight { name: "strf:hash", default: 1.0 },
    ProdWeight { name: "strf:err", default: 0.4 },

    // floatmath (Track-B): float4/float8 math-function + special-value
    // drain over the float.c surface. Top-level shape mix, then per-family
    // arms. Value probes return bare float (ulp compare); text-roundtrip
    // and sign-of-zero arms return ::text (byte-exact); error arms match on
    // SQLSTATE.
    ProdWeight { name: "floatmath:trig", default: 2.0 },
    ProdWeight { name: "floatmath:trigd", default: 2.0 },
    ProdWeight { name: "floatmath:hyp", default: 1.5 },
    ProdWeight { name: "floatmath:explog", default: 2.5 },
    ProdWeight { name: "floatmath:round", default: 1.5 },
    ProdWeight { name: "floatmath:arith", default: 2.0 },
    ProdWeight { name: "floatmath:cmp", default: 1.5 },
    ProdWeight { name: "floatmath:conv", default: 1.0 },
    ProdWeight { name: "floatmath:wb", default: 1.2 },
    ProdWeight { name: "floatmath:cast", default: 2.0 },
    ProdWeight { name: "floatmath:text", default: 2.5 },
    ProdWeight { name: "floatmath:special", default: 2.0 },
    ProdWeight { name: "floatmath:agg", default: 0.8 },

    ProdWeight { name: "floatmath:trig:one", default: 1.5 },
    ProdWeight { name: "floatmath:trig:atan2", default: 1.0 },
    ProdWeight { name: "floatmath:trig:err", default: 0.8 },

    ProdWeight { name: "floatmath:trigd:one", default: 1.5 },
    ProdWeight { name: "floatmath:trigd:atan2", default: 1.0 },
    ProdWeight { name: "floatmath:trigd:err", default: 0.8 },

    ProdWeight { name: "floatmath:hyp:ok", default: 1.2 },
    ProdWeight { name: "floatmath:hyp:err", default: 0.8 },

    ProdWeight { name: "floatmath:explog:ln", default: 1.2 },
    ProdWeight { name: "floatmath:explog:exp", default: 1.2 },
    ProdWeight { name: "floatmath:explog:pow", default: 1.2 },
    ProdWeight { name: "floatmath:explog:sqrt", default: 1.2 },
    ProdWeight { name: "floatmath:explog:err", default: 0.9 },

    ProdWeight { name: "floatmath:round:core", default: 1.3 },
    ProdWeight { name: "floatmath:round:op", default: 1.0 },

    ProdWeight { name: "floatmath:arith:f8", default: 1.3 },
    ProdWeight { name: "floatmath:arith:f4", default: 1.0 },
    ProdWeight { name: "floatmath:arith:mixed", default: 1.0 },
    ProdWeight { name: "floatmath:arith:err", default: 0.8 },

    ProdWeight { name: "floatmath:cmp:f8", default: 1.2 },
    ProdWeight { name: "floatmath:cmp:f4", default: 1.0 },
    ProdWeight { name: "floatmath:cmp:mixed", default: 1.0 },

    ProdWeight { name: "floatmath:conv:dr", default: 1.2 },
    ProdWeight { name: "floatmath:conv:pi", default: 0.8 },

    ProdWeight { name: "floatmath:wb:ok", default: 1.2 },
    ProdWeight { name: "floatmath:wb:err", default: 0.9 },

    ProdWeight { name: "floatmath:cast:toint", default: 1.2 },
    ProdWeight { name: "floatmath:cast:tofloat", default: 1.2 },
    ProdWeight { name: "floatmath:cast:err", default: 1.0 },

    ProdWeight { name: "floatmath:text:roundtrip", default: 1.5 },
    ProdWeight { name: "floatmath:text:parse", default: 1.2 },
    ProdWeight { name: "floatmath:text:signzero", default: 1.0 },
    ProdWeight { name: "floatmath:text:err", default: 0.8 },

    ProdWeight { name: "floatmath:special:prop", default: 1.3 },
    ProdWeight { name: "floatmath:special:cmp", default: 1.0 },

    ProdWeight { name: "floatmath:agg:build", default: 1.2 },
    ProdWeight { name: "floatmath:agg:stat", default: 1.0 },
    // ruleutils (Track-B deparse drain): the pg_get_*def shape selection and
    // the viewdef idempotence round-trip arm. viewdef/funcdef carry the
    // heaviest deparse mass (wrap-column path, function-signature trio), so
    // they sit above the fixed-shape probes.
    ProdWeight { name: "ruleutils:viewdef", default: 1.4 },
    ProdWeight { name: "ruleutils:indexdef", default: 1.0 },
    ProdWeight { name: "ruleutils:constraintdef", default: 1.0 },
    ProdWeight { name: "ruleutils:funcdef", default: 1.3 },
    ProdWeight { name: "ruleutils:partkeydef", default: 0.8 },
    ProdWeight { name: "ruleutils:statsdef", default: 0.8 },
    ProdWeight { name: "ruleutils:exprdef", default: 1.0 },
    ProdWeight { name: "ruleutils:formattype", default: 1.0 },
    ProdWeight { name: "ruleutils:triggerdef", default: 1.0 },
    ProdWeight { name: "ruleutils:ruledef", default: 1.0 },
    ProdWeight { name: "ruleutils:idem", default: 1.0 },
    ProdWeight { name: "ruleutils:noidem", default: 1.0 },
    // cterec (Track-B): recursive-CTE SEARCH/CYCLE planning, data-modifying
    // CTE and the sublink surface. Family split, then per-family shape
    // picks. search_* and cycle_* carry the SET-column rewrite drain;
    // union carries the distinct-working-table path; notin3vl/in3vl carry
    // the three-valued sublink arms; toomany carries the 21000 error arm.
    ProdWeight { name: "cterec:graph", default: 3.0 },
    ProdWeight { name: "cterec:aux", default: 1.5 },
    ProdWeight { name: "cterec:mat", default: 1.0 },
    ProdWeight { name: "cterec:dml", default: 1.5 },
    ProdWeight { name: "cterec:sublink", default: 3.0 },
    ProdWeight { name: "cterec:union_all", default: 2.0 },
    ProdWeight { name: "cterec:union", default: 1.0 },
    ProdWeight { name: "cterec:search_none", default: 1.0 },
    ProdWeight { name: "cterec:search_depth", default: 2.0 },
    ProdWeight { name: "cterec:search_breadth", default: 2.0 },
    ProdWeight { name: "cterec:cycle_none", default: 1.0 },
    ProdWeight { name: "cterec:cycle_bool", default: 2.0 },
    ProdWeight { name: "cterec:cycle_marked", default: 2.0 },
    ProdWeight { name: "cterec:aux_plain", default: 1.0 },
    ProdWeight { name: "cterec:aux_mat", default: 1.5 },
    ProdWeight { name: "cterec:aux_nested", default: 1.5 },
    ProdWeight { name: "cterec:dml_move", default: 1.0 },
    ProdWeight { name: "cterec:dml_multi", default: 1.5 },
    ProdWeight { name: "cterec:dml_visibility", default: 1.5 },
    ProdWeight { name: "cterec:dml_chain", default: 1.0 },
    ProdWeight { name: "cterec:any", default: 1.5 },
    ProdWeight { name: "cterec:all", default: 1.5 },
    ProdWeight { name: "cterec:rowcmp", default: 1.5 },
    ProdWeight { name: "cterec:scalar", default: 1.0 },
    ProdWeight { name: "cterec:toomany", default: 1.0 },
    ProdWeight { name: "cterec:notin3vl", default: 2.0 },
    ProdWeight { name: "cterec:in3vl", default: 2.0 },
    ProdWeight { name: "cterec:corr_exists", default: 1.0 },
    ProdWeight { name: "cterec:corr_in", default: 1.0 },
    // castcoerce (CASTCOERCE): the cast/coercion drain. Single-SELECT
    // productions carry the parser coercion core (explicit/typmod/unify/
    // unknown/array/bincoerce/failed); the DDL-group productions carry the
    // CREATE CAST / domain / enum / composite / assignment surfaces. The
    // decision-surface producers (failed, unify's unresolvable arm) sit at
    // a healthy weight — the coercion DECISION is the load-bearing compare.
    ProdWeight { name: "castcoerce:explicit", default: 3.0 },
    ProdWeight { name: "castcoerce:typmod", default: 3.0 },
    ProdWeight { name: "castcoerce:unify", default: 2.0 },
    ProdWeight { name: "castcoerce:unknown", default: 1.5 },
    ProdWeight { name: "castcoerce:array", default: 2.0 },
    ProdWeight { name: "castcoerce:bincoerce", default: 1.5 },
    ProdWeight { name: "castcoerce:failed", default: 2.5 },
    ProdWeight { name: "castcoerce:assign", default: 1.5 },
    ProdWeight { name: "castcoerce:domain", default: 1.5 },
    ProdWeight { name: "castcoerce:enum", default: 1.0 },
    ProdWeight { name: "castcoerce:createcast", default: 1.0 },
    ProdWeight { name: "castcoerce:composite", default: 1.0 },
    // intops (INTOPS): int.c/int8.c boundary drain — shape selection.
    ProdWeight { name: "intops:arith", default: 2.0 },
    ProdWeight { name: "intops:div", default: 2.5 },
    ProdWeight { name: "intops:unary", default: 1.5 },
    ProdWeight { name: "intops:bit", default: 1.8 },
    ProdWeight { name: "intops:gcdlcm", default: 1.5 },
    ProdWeight { name: "intops:cast", default: 2.0 },
    ProdWeight { name: "intops:parse", default: 1.8 },
    ProdWeight { name: "intops:cmp", default: 1.2 },
    ProdWeight { name: "intops:series", default: 1.5 },
    ProdWeight { name: "intops:agg", default: 1.0 },
    ProdWeight { name: "intops:inrange", default: 1.2 },
    ProdWeight { name: "intops:misc", default: 1.2 },
    // intops: arithmetic sub-shapes (over = overflow arms).
    ProdWeight { name: "intops:ar:same", default: 1.5 },
    ProdWeight { name: "intops:ar:cross", default: 1.2 },
    ProdWeight { name: "intops:ar:over", default: 1.5 },
    // intops: division sub-shapes.
    ProdWeight { name: "intops:dv:op", default: 1.5 },
    ProdWeight { name: "intops:dv:cross", default: 1.2 },
    ProdWeight { name: "intops:dv:zero", default: 1.2 },
    ProdWeight { name: "intops:dv:over", default: 1.2 },
    // intops: unary sub-shapes.
    ProdWeight { name: "intops:un:ok", default: 1.2 },
    ProdWeight { name: "intops:un:over", default: 1.2 },
    // intops: bit sub-shapes.
    ProdWeight { name: "intops:bt:logic", default: 1.2 },
    ProdWeight { name: "intops:bt:shift", default: 1.5 },
    // intops: gcd/lcm sub-shapes.
    ProdWeight { name: "intops:gl:ok", default: 1.0 },
    ProdWeight { name: "intops:gl:over", default: 1.5 },
    // intops: cast sub-shapes.
    ProdWeight { name: "intops:ca:widen", default: 1.0 },
    ProdWeight { name: "intops:ca:narrow", default: 1.0 },
    ProdWeight { name: "intops:ca:bool", default: 0.8 },
    ProdWeight { name: "intops:ca:err", default: 1.5 },
    // intops: parse sub-shapes.
    ProdWeight { name: "intops:pa:ok", default: 1.0 },
    ProdWeight { name: "intops:pa:err", default: 1.5 },
    ProdWeight { name: "intops:pa:out", default: 0.8 },
    // intops: comparison sub-shapes.
    ProdWeight { name: "intops:cm:same", default: 1.0 },
    ProdWeight { name: "intops:cm:cross", default: 1.0 },
    // intops: generate_series sub-shapes.
    ProdWeight { name: "intops:se:ok", default: 1.0 },
    ProdWeight { name: "intops:se:edge", default: 1.5 },
    ProdWeight { name: "intops:se:err", default: 1.0 },
    // intops: aggregate sub-shapes.
    ProdWeight { name: "intops:ag:sum", default: 1.0 },
    ProdWeight { name: "intops:ag:bit", default: 1.0 },
    // intops: misc sub-shapes.
    ProdWeight { name: "intops:mi:incdec", default: 1.2 },
    ProdWeight { name: "intops:mi:fac", default: 1.0 },
    ProdWeight { name: "intops:mi:hash", default: 0.8 },
    // subplan (SUBPLAN-1): SubPlan/InitPlan execution drain. The module name
    // fires per group; the shape names select the context battery. `ctx`
    // (the SUBPLAN-1 heartland — subquery in every clause) carries the most
    // weight; the rest are even.
    ProdWeight { name: "subplan", default: 1.0 },
    ProdWeight { name: "subplan:ctx", default: 2.0 },
    ProdWeight { name: "subplan:sublink", default: 1.5 },
    ProdWeight { name: "subplan:corr", default: 1.5 },
    ProdWeight { name: "subplan:initplan", default: 1.5 },
    ProdWeight { name: "subplan:agg", default: 1.0 },
    ProdWeight { name: "subplan:gather", default: 1.0 },
    // scalartypes (Track-B): niche fixed-width scalar-type drain. Top-level
    // type split, then per-type shape split.
    ProdWeight { name: "scl:money", default: 1.2 },
    ProdWeight { name: "scl:uuid", default: 1.0 },
    ProdWeight { name: "scl:lsn", default: 1.0 },
    ProdWeight { name: "scl:tid", default: 0.8 },
    ProdWeight { name: "scl:xid", default: 0.8 },
    ProdWeight { name: "scl:mac8", default: 0.9 },
    // money (cash.c) shapes.
    ProdWeight { name: "scl:money:io", default: 1.2 },
    ProdWeight { name: "scl:money:arith", default: 1.4 },
    ProdWeight { name: "scl:money:cmp", default: 1.0 },
    ProdWeight { name: "scl:money:agg", default: 0.8 },
    ProdWeight { name: "scl:money:cast", default: 1.0 },
    ProdWeight { name: "scl:money:words", default: 0.7 },
    // uuid (uuid.c) shapes.
    ProdWeight { name: "scl:uuid:io", default: 1.2 },
    ProdWeight { name: "scl:uuid:ioerr", default: 0.8 },
    ProdWeight { name: "scl:uuid:cmp", default: 1.0 },
    ProdWeight { name: "scl:uuid:extract", default: 1.0 },
    // pg_lsn (pg_lsn.c) shapes.
    ProdWeight { name: "scl:lsn:io", default: 1.0 },
    ProdWeight { name: "scl:lsn:arith", default: 1.2 },
    ProdWeight { name: "scl:lsn:cmp", default: 1.0 },
    // tid (tid.c) shapes.
    ProdWeight { name: "scl:tid:io", default: 1.0 },
    ProdWeight { name: "scl:tid:cmp", default: 1.0 },
    // xid/xid8 (xid.c) shapes.
    ProdWeight { name: "scl:xid:cmp", default: 1.2 },
    ProdWeight { name: "scl:xid:conv", default: 1.0 },
    // macaddr8 (mac8.c) shapes.
    ProdWeight { name: "scl:mac8:io", default: 1.0 },
    ProdWeight { name: "scl:mac8:ops", default: 1.2 },
    ProdWeight { name: "scl:mac8:cmp", default: 1.0 },
    // inherit (INHERIT lane): classic table-inheritance form selection.
    // constraint/alter/dml carry the richest bookkeeping + recursion
    // surface, so they weigh a touch higher.
    ProdWeight { name: "inherit:basic", default: 1.0 },
    ProdWeight { name: "inherit:multi", default: 1.2 },
    ProdWeight { name: "inherit:constraint", default: 1.4 },
    ProdWeight { name: "inherit:alter", default: 1.4 },
    ProdWeight { name: "inherit:attach", default: 1.2 },
    ProdWeight { name: "inherit:dml", default: 1.4 },
    ProdWeight { name: "inherit:droptree", default: 0.8 },
    // bitstring (varbit.c drain): top-level statement-shape selection. io/
    // typmod/cast carry the input + coercion + int-cast width arms; cat/
    // logic/shift the operator bodies; sub/overlay/pos the string ops;
    // getset the subscript accessors; len/cmp/order the measurement,
    // comparison and hash/ordering surface. The ok/err pair biases the
    // deliberate matched-error arms low per the findings-budget rule.
    ProdWeight { name: "bitstr:io", default: 1.5 },
    ProdWeight { name: "bitstr:typmod", default: 1.5 },
    ProdWeight { name: "bitstr:cast", default: 1.5 },
    ProdWeight { name: "bitstr:cat", default: 1.2 },
    ProdWeight { name: "bitstr:logic", default: 1.2 },
    ProdWeight { name: "bitstr:shift", default: 1.2 },
    ProdWeight { name: "bitstr:sub", default: 1.2 },
    ProdWeight { name: "bitstr:overlay", default: 1.2 },
    ProdWeight { name: "bitstr:pos", default: 1.0 },
    ProdWeight { name: "bitstr:getset", default: 1.2 },
    ProdWeight { name: "bitstr:len", default: 1.0 },
    ProdWeight { name: "bitstr:cmp", default: 1.0 },
    ProdWeight { name: "bitstr:order", default: 1.0 },
    ProdWeight { name: "bitstr:ok", default: 4.0 },
    ProdWeight { name: "bitstr:err", default: 1.0 },
    // groupingsets (GROUPINGSETS lane): grouping-set execution-drain shape
    // selection. rollup/cube/sets/mixed/nested carry the planner grouping-
    // set expansion + parse_agg.c GROUPING() surface; chain forces the
    // sorted AggState phase chain (enable_hashagg=off) and hashmix the
    // hashed-set mix + hash-spill refill (work_mem='64kB') in nodeAgg.c;
    // distinct/join/having/gorder carry the DISTINCT, join, HAVING
    // NULL-vs-present and ORDER-BY-GROUPING() arms.
    ProdWeight { name: "gs:rollup", default: 1.5 },
    ProdWeight { name: "gs:cube", default: 1.5 },
    ProdWeight { name: "gs:sets", default: 1.5 },
    ProdWeight { name: "gs:mixed", default: 1.3 },
    ProdWeight { name: "gs:nested", default: 1.2 },
    ProdWeight { name: "gs:chain", default: 1.2 },
    ProdWeight { name: "gs:hashmix", default: 1.2 },
    ProdWeight { name: "gs:distinct", default: 1.0 },
    ProdWeight { name: "gs:join", default: 1.0 },
    ProdWeight { name: "gs:having", default: 1.3 },
    ProdWeight { name: "gs:gorder", default: 1.0 },
    // tablesample module: family selection. create leads until a fixture
    // exists; the query families then dominate. bernoulli/system carry the
    // tsmapi + nodeSamplescan drain; limit carries nodeLimit; distincton
    // carries the DISTINCT ON first-row path; err carries the validation
    // arms (incl. the bug-129 REPEATABLE-Var rejection).
    ProdWeight { name: "tsm:create", default: 1.2 },
    ProdWeight { name: "tsm:drop", default: 0.15 },
    ProdWeight { name: "tsm:bernoulli", default: 3.0 },
    ProdWeight { name: "tsm:system", default: 3.0 },
    ProdWeight { name: "tsm:sameseed", default: 2.0 },
    ProdWeight { name: "tsm:join", default: 2.0 },
    ProdWeight { name: "tsm:inherit", default: 1.5 },
    ProdWeight { name: "tsm:limit", default: 3.0 },
    ProdWeight { name: "tsm:distincton", default: 2.5 },
    ProdWeight { name: "tsm:err", default: 2.0 },
    // tablesample sample shapes (bernoulli/system).
    ProdWeight { name: "tsm:samp:plain", default: 1.0 },
    ProdWeight { name: "tsm:samp:where", default: 1.0 },
    ProdWeight { name: "tsm:samp:proj", default: 1.0 },
    // tablesample nodeLimit shapes.
    ProdWeight { name: "tsm:limit:only", default: 1.0 },
    ProdWeight { name: "tsm:limit:offset", default: 1.0 },
    ProdWeight { name: "tsm:limit:both", default: 1.0 },
    ProdWeight { name: "tsm:limit:all", default: 1.0 },
    ProdWeight { name: "tsm:limit:offend", default: 1.0 },
    ProdWeight { name: "tsm:limit:fetch", default: 1.2 },
    ProdWeight { name: "tsm:limit:fetchoff", default: 1.0 },
    ProdWeight { name: "tsm:limit:ties", default: 1.4 },
    ProdWeight { name: "tsm:limit:param", default: 1.2 },
    // tablesample DISTINCT ON shapes.
    ProdWeight { name: "tsm:don:single", default: 1.0 },
    ProdWeight { name: "tsm:don:multi", default: 1.0 },
    ProdWeight { name: "tsm:don:desc", default: 1.0 },
    ProdWeight { name: "tsm:don:expr", default: 1.0 },
    // tablesample error arms.
    ProdWeight { name: "tsm:err:range", default: 1.2 },
    ProdWeight { name: "tsm:err:nontable", default: 1.0 },
    ProdWeight { name: "tsm:err:repvar", default: 1.2 },
    ProdWeight { name: "tsm:err:distinct", default: 1.0 },
    ProdWeight { name: "tsm:err:neglimit", default: 1.0 },
];

/// Resolved weight vector, parallel to `PROD_WEIGHTS`.
#[derive(Clone, Debug, PartialEq)]
pub struct WeightTable {
    values: Vec<f64>,
}

impl Default for WeightTable {
    fn default() -> WeightTable {
        WeightTable::defaults()
    }
}

impl WeightTable {
    pub fn defaults() -> WeightTable {
        WeightTable { values: PROD_WEIGHTS.iter().map(|p| p.default).collect() }
    }

    /// Apply a `--weight` spec: comma-separated `name=value` entries.
    /// Production names may themselves contain `=` (`cmp:>=`), so each
    /// entry is matched longest-name-first: `cmp:>=0` sets `cmp:>` to 0,
    /// `cmp:>==2` sets `cmp:>=` to 2. Values must be finite and >= 0.
    pub fn apply_spec(&mut self, spec: &str) -> Result<(), String> {
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            // Longest known name that is followed by "=<value>".
            let mut best: Option<(usize, &str)> = None;
            for (i, p) in PROD_WEIGHTS.iter().enumerate() {
                if let Some(rest) = part.strip_prefix(p.name) {
                    if let Some(v) = rest.strip_prefix('=') {
                        if best.is_none_or(|(j, _)| p.name.len() > PROD_WEIGHTS[j].name.len()) {
                            best = Some((i, v));
                        }
                    }
                }
            }
            let (idx, value) = best.ok_or_else(|| {
                format!(
                    "bad weight entry {:?}: expected <production>=<value> \
                     (see --print-weights for names)",
                    part
                )
            })?;
            let w: f64 = value
                .parse()
                .map_err(|_| format!("bad weight value {:?} for {:?}", value, PROD_WEIGHTS[idx].name))?;
            if !(w >= 0.0 && w.is_finite()) {
                return Err(format!(
                    "weight for {:?} must be finite and >= 0",
                    PROD_WEIGHTS[idx].name
                ));
            }
            self.values[idx] = w;
        }
        Ok(())
    }

    /// Parse a spec on top of the defaults.
    pub fn parse(spec: &str) -> Result<WeightTable, String> {
        let mut t = WeightTable::defaults();
        t.apply_spec(spec)?;
        Ok(t)
    }

    pub fn get(&self, name: &str) -> f64 {
        match PROD_WEIGHTS.iter().position(|p| p.name == name) {
            Some(i) => self.values[i],
            None => {
                debug_assert!(false, "unregistered production weight {name}");
                1.0
            }
        }
    }

    /// `--print-weights` dump: one `name=value` per line, `*` marking
    /// entries changed from their default.
    pub fn dump(&self) -> String {
        let mut out = String::new();
        for (p, v) in PROD_WEIGHTS.iter().zip(&self.values) {
            let mark = if *v != p.default { " *" } else { "" };
            out.push_str(&format!("{}={}{}\n", p.name, v, mark));
        }
        out
    }

    /// Canonical non-default spec (empty when all defaults); recorded by
    /// the covloop tooling next to the seed.
    pub fn spec_string(&self) -> String {
        let mut out = String::new();
        for (p, v) in PROD_WEIGHTS.iter().zip(&self.values) {
            if *v != p.default {
                if !out.is_empty() {
                    out.push(',');
                }
                out.push_str(&format!("{}={}", p.name, v));
            }
        }
        out
    }

    /// Weighted pick among `options` (each a registered production name),
    /// drawing one value from the session PRNG. If every option weighs 0
    /// the pick degrades to uniform — a choice point can never dead-end.
    pub fn pick<'x>(&self, rng: &mut Rng, options: &[&'x str]) -> &'x str {
        debug_assert!(!options.is_empty());
        let total: f64 = options.iter().map(|o| self.get(o)).sum();
        if total <= 0.0 {
            return options[rng.below_usize(options.len())];
        }
        let mut x = rng.f64_unit() * total;
        let mut last_positive = options[0];
        for o in options {
            let w = self.get(o);
            if w <= 0.0 {
                continue;
            }
            last_positive = o;
            if x < w {
                return o;
            }
            x -= w;
        }
        last_positive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_names_unique() {
        for (i, a) in PROD_WEIGHTS.iter().enumerate() {
            for b in &PROD_WEIGHTS[i + 1..] {
                assert_ne!(a.name, b.name);
            }
            assert!(a.default > 0.0, "{} must default positive", a.name);
        }
    }

    #[test]
    fn parse_and_round_trip() {
        let t = WeightTable::parse("case=5,binop:%=0").unwrap();
        assert_eq!(t.get("case"), 5.0);
        assert_eq!(t.get("binop:%"), 0.0);
        assert_eq!(t.get("cmp"), 2.0); // untouched default
        let again = WeightTable::parse(&t.spec_string()).unwrap();
        assert_eq!(again, t);
        assert!(WeightTable::defaults().spec_string().is_empty());
    }

    #[test]
    fn names_containing_equals_parse_longest_first() {
        // "cmp:>=0" = production "cmp:>" set to 0.
        let t = WeightTable::parse("cmp:>=0").unwrap();
        assert_eq!(t.get("cmp:>"), 0.0);
        assert_eq!(t.get("cmp:>="), 1.0);
        // "cmp:>==2" = production "cmp:>=" set to 2.
        let t = WeightTable::parse("cmp:>==2").unwrap();
        assert_eq!(t.get("cmp:>="), 2.0);
        assert_eq!(t.get("cmp:>"), 1.0);
    }

    #[test]
    fn parse_rejects_bad_entries() {
        assert!(WeightTable::parse("nosuch=1").is_err());
        assert!(WeightTable::parse("case").is_err());
        assert!(WeightTable::parse("case=-1").is_err());
        assert!(WeightTable::parse("case=inf").is_err());
        assert!(WeightTable::parse("case=zebra").is_err());
    }

    #[test]
    fn pick_is_deterministic_and_respects_zero() {
        let t = WeightTable::parse("case=0,cast=0,coalesce=0").unwrap();
        let opts = ["case", "cast", "coalesce", "nullif"];
        let mut r1 = Rng::new(4);
        let mut r2 = Rng::new(4);
        for _ in 0..256 {
            let a = t.pick(&mut r1, &opts);
            assert_eq!(a, t.pick(&mut r2, &opts));
            assert_eq!(a, "nullif"); // sole positive weight
        }
    }

    #[test]
    fn pick_all_zero_falls_back_uniform() {
        let t = WeightTable::parse("case=0,cast=0").unwrap();
        let opts = ["case", "cast"];
        let mut rng = Rng::new(7);
        let mut seen = [false, false];
        for _ in 0..64 {
            match t.pick(&mut rng, &opts) {
                "case" => seen[0] = true,
                "cast" => seen[1] = true,
                other => panic!("unexpected pick {other}"),
            }
        }
        assert!(seen[0] && seen[1]);
    }

    #[test]
    fn dump_marks_overrides() {
        let t = WeightTable::parse("case=5").unwrap();
        let d = t.dump();
        assert!(d.contains("case=5 *"));
        assert!(d.contains("cmp=2\n"));
    }
}
