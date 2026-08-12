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
    // LD3 datetime decode drain (dtmdec.rs): raw-literal permutation
    // casts. The dtm:decode gate sits alongside the matrix shapes; the
    // dtmdec:* subs split the six type targets, the deterministic-special
    // and style-bracket shapes, and the malformed-literal error fuel.
    ProdWeight { name: "dtm:decode", default: 3.0 },
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
    // spill: serial hash-join arms.
    ProdWeight { name: "spill:hj:inner", default: 2.0 },
    ProdWeight { name: "spill:hj:skew", default: 2.0 },
    ProdWeight { name: "spill:hj:rows", default: 1.5 },
    ProdWeight { name: "spill:hj:outer", default: 1.5 },
    ProdWeight { name: "spill:hj:antisemi", default: 1.5 },
    // spill: HashAgg spill shapes.
    ProdWeight { name: "spill:ha:wrap", default: 2.0 },
    ProdWeight { name: "spill:ha:distinct", default: 1.5 },
    ProdWeight { name: "spill:ha:gsets", default: 1.5 },
    // spill: sort-based aggregation shapes.
    ProdWeight { name: "spill:ga:group", default: 2.0 },
    ProdWeight { name: "spill:ga:distinct", default: 1.5 },
    ProdWeight { name: "spill:ga:oset", default: 1.5 },
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
