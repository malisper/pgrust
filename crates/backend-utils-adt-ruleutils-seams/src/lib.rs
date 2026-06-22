//! Seam declarations for the `backend-utils-adt-ruleutils` unit
//! (`utils/adt/ruleutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::Node;
use types_nodes::planstate::PlanStateNode;

/// One element of a deparse context `List *` (`deparse_context_for_plan_tree`
/// result), a ruleutils-private `deparse_namespace`. Carried as the generic C
/// `List`-of-`Node` element so the EXPLAIN unit can thread the context list
/// through `set_deparse_context_plan` / `deparse_expression` without inventing a
/// type; ruleutils materialises the real `deparse_namespace` when it lands.
pub type DeparseContext<'mcx> = PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>;

seam_core::seam!(
    /// `pg_get_partkeydef_columns(relid, pretty)` (ruleutils.c): the
    /// comma-separated list of the relation's partition-key column/expression
    /// definitions (the inside of the `PARTITION BY (...)` clause), allocated
    /// in `mcx`. Reads the catalog and deparses, so it can `ereport(ERROR)`;
    /// `Err` also carries OOM.
    pub fn pg_get_partkeydef_columns<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        pretty: bool,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `generate_opclass_name(opclass)` (ruleutils.c): the schema-qualified,
    /// quoted operator-class name (e.g. `pg_catalog.int4_ops`) for an opclass
    /// OID, allocated in `mcx`. Reads `pg_opclass`/`pg_namespace`, so it can
    /// `ereport(ERROR)` (cache lookup failure); `Err` also carries OOM.
    pub fn generate_opclass_name<'mcx>(
        mcx: Mcx<'mcx>,
        opclass: Oid,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `quote_qualified_identifier(qualifier, ident)` (ruleutils.c): each
    /// part quoted with `quote_identifier` if needed, joined with a dot,
    /// allocated in `mcx` (C: palloc in the current context). `Err` is OOM.
    pub fn quote_qualified_identifier<'mcx>(
        mcx: Mcx<'mcx>,
        qualifier: Option<&str>,
        ident: &str,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `quote_identifier(ident)` (ruleutils.c): double-quote the identifier
    /// if needed for re-parse safety (non-lowercase letters, keywords, ...).
    /// The result is copied into `mcx` (C pallocs the quoted form in the
    /// current context; the unquoted case returns the input pointer — the
    /// owned image copies either way). `Err` carries OOM.
    pub fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<PgString<'mcx>>
);

// (quote_identifier is already declared above; postinit reuses it.)

seam_core::seam!(
    /// `generate_operator_clause(buf, leftop, leftoptype, opoid, rightop,
    /// rightoptype)` (ruleutils.c): the schema-qualified, casted
    /// `leftop OPERATOR(...) rightop` fragment `ri_GenerateQual` appends.
    /// `leftop`/`rightop` are raw server-encoded identifier/parameter bytes;
    /// the returned fragment is likewise raw bytes (C operates on `char *`
    /// end-to-end), copied into `mcx`. Catalog lookups can `ereport(ERROR)`.
    pub fn generate_operator_clause<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        leftop: &[u8],
        leftoptype: types_core::Oid,
        opoid: types_core::Oid,
        rightop: &[u8],
        rightoptype: types_core::Oid,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `pg_get_partconstrdef_string(RelationGetRelid(pk_rel), "pk")`
    /// (ruleutils.c): the partition's bound constraint as SQL text, copied
    /// into `mcx`; `Ok(None)` for the empty default-partition constraint. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn partition_constraint_def<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pk_relid: types_core::Oid,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `pg_get_expr_worker(exprstr, relid, prettyFlags)` (ruleutils.c 2709) —
    /// the shared body of `pg_get_expr`: parse the `pg_node_tree` text in
    /// `exprstr` into a node tree and reverse-compile it to source text in
    /// the deparse context of `relid` (a single-relation context when
    /// `relid` is valid). Returns `Ok(None)` when the relation has gone away
    /// (C returns NULL). Reads the catalog / deparses, so it can
    /// `ereport(ERROR)`; `Err` also carries OOM.
    pub fn pg_get_expr_worker<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        exprstr: &str,
        relid: types_core::Oid,
        pretty_flags: i32,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `select_rtable_names_for_explain(rtable, rels_used)` (ruleutils.c):
    /// choose the unique alias name to display for each RTE actually referenced
    /// in the plan (`rels_used`), returning a `List *` of `char *` (a `None`
    /// element means "use the RTE's eref alias"). Reads the catalog, so it can
    /// `ereport(ERROR)`. Allocated in `mcx`.
    ///
    /// `rels_used` is `Option` so the C NULL-vs-empty distinction is preserved:
    /// in C the caller passes `*rels_used` which is left NULL when no scan node
    /// added any member (e.g. a dummy `Result` plan). `set_rtable_names` then
    /// names *every* RTE (the `rels_used != NULL` guard is false), so Vars in the
    /// plan's targetlist still get relation prefixes. Passing `Some(empty_bms)`
    /// would instead suppress all names — the bug this `Option` prevents.
    pub fn select_rtable_names_for_explain<'mcx>(
        mcx: Mcx<'mcx>,
        rtable: &PgVec<'mcx, types_nodes::parsenodes::RangeTblEntry<'mcx>>,
        rels_used: Option<&types_nodes::bitmapset::Bitmapset<'mcx>>,
    ) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>>
);

seam_core::seam!(
    /// `deparse_context_for_plan_tree(pstmt, rtable_names)` (ruleutils.c): build
    /// the deparse-namespace context list for an entire plan tree, so plan-node
    /// expressions can be deparsed. Reads the catalog; can `ereport(ERROR)`.
    pub fn deparse_context_for_plan_tree<'mcx>(
        mcx: Mcx<'mcx>,
        pstmt: &PlannedStmt<'mcx>,
        rtable_names: &PgVec<'mcx, Option<PgString<'mcx>>>,
    ) -> PgResult<DeparseContext<'mcx>>
);

seam_core::seam!(
    /// `set_deparse_context_plan(dpcontext, plan, ancestors)` (ruleutils.c):
    /// point the head deparse namespace at a specific plan node (and its
    /// ancestor list) so `PARAM_EXEC`/`Var` references resolve. Returns the
    /// updated context list. Can `ereport(ERROR)`.
    pub fn set_deparse_context_plan<'mcx, 'p>(
        mcx: Mcx<'mcx>,
        dpcontext: &DeparseContext<'mcx>,
        plan: &PlanStateNode<'p>,
        ancestors: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    ) -> PgResult<DeparseContext<'mcx>>
);

seam_core::seam!(
    /// `deparse_expression(expr, dpcontext, forceprefix, showimplicit)`
    /// (ruleutils.c): deparse a single expression tree to SQL text using the
    /// deparse context. Reads the catalog; can `ereport(ERROR)`. Allocated in
    /// `mcx`.
    pub fn deparse_expression<'mcx>(
        mcx: Mcx<'mcx>,
        expr: &Node<'mcx>,
        dpcontext: &DeparseContext<'mcx>,
        forceprefix: bool,
        showimplicit: bool,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// EXPLAIN's `show_expression` deparse step, folded into one owner-private
    /// call (ruleutils.c `set_deparse_context_plan` + `deparse_expression`).
    ///
    /// C's `show_expression` does:
    /// ```text
    /// context = set_deparse_context_plan(es->deparse_cxt, planstate->plan, ancestors);
    /// exprstr = deparse_expression(node, context, useprefix, false);
    /// ```
    /// where `es->deparse_cxt` is itself `deparse_context_for_plan_tree(pstmt,
    /// rtable_names)`. The `deparse_namespace`/`deparse_context` it threads are
    /// ruleutils-private (never exposed to explain.c in C either), so this seam
    /// carries the *substrate* explain owns — the `PlannedStmt`, the per-RTE
    /// display names, the current `Plan` node, the ancestor `Plan` list, and the
    /// expression to deparse — and ruleutils builds the namespace, points it at
    /// the node, and renders the SQL text internally. Reads the catalog (column
    /// names, operator/function names), so it can `ereport(ERROR)`; allocated in
    /// `mcx`.
    pub fn deparse_expr_for_plan<'mcx, 'p>(
        mcx: Mcx<'mcx>,
        pstmt: &PlannedStmt<'p>,
        rtable_names: &PgVec<'mcx, Option<PgString<'mcx>>>,
        plan: &Node<'p>,
        ancestors: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
        expr: &Node<'p>,
        forceprefix: bool,
        showimplicit: bool,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// EXPLAIN's `show_window_def` frame-options step, folded into one
    /// owner-private call (ruleutils.c `set_deparse_context_plan` +
    /// `get_window_frame_options_for_explain`).
    ///
    /// Like [`deparse_expr_for_plan`], carries the substrate explain owns — the
    /// `PlannedStmt`, per-RTE display names, the WindowAgg `Plan` node, and the
    /// ancestor `Plan` list — plus the `frameOptions` bitmask and the optional
    /// `startOffset`/`endOffset` bound expressions. Ruleutils builds the
    /// namespace, points it at the WindowAgg, renders the frame clause text
    /// (e.g. `ROWS UNBOUNDED PRECEDING`), and returns it. Reads the catalog when
    /// offsets are present, so it can `ereport(ERROR)`; allocated in `mcx`.
    pub fn deparse_window_frame_for_plan<'mcx, 'p>(
        mcx: Mcx<'mcx>,
        pstmt: &PlannedStmt<'p>,
        rtable_names: &PgVec<'mcx, Option<PgString<'mcx>>>,
        plan: &Node<'p>,
        ancestors: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
        frame_options: i32,
        start_offset: Option<&Node<'p>>,
        end_offset: Option<&Node<'p>>,
        forceprefix: bool,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `get_rule_expr(expr, context, showimplicit)` (ruleutils.c): the
    /// lower-level expression deparser `deparse_expression` wraps, appending the
    /// SQL text for `expr` to the context's output buffer. Carried here because
    /// some EXPLAIN show_* helpers reach it directly. Can `ereport(ERROR)`;
    /// returns the rendered text in `mcx`.
    pub fn get_rule_expr<'mcx>(
        mcx: Mcx<'mcx>,
        expr: &Node<'mcx>,
        dpcontext: &DeparseContext<'mcx>,
        showimplicit: bool,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// The relation-RTE branch of `set_relation_column_names` (ruleutils.c
    /// 4390-4412): open the relation (`relation_open(relid, AccessShareLock)`),
    /// read its current `TupleDesc`, and return one entry per *physical* column
    /// — the live `attname` for a live column, `None` for a dropped column
    /// (`attisdropped`). Reads the catalog and takes a lock, so it can
    /// `ereport(ERROR)`; allocated in `mcx`. This is the only catalog-coupled
    /// step of the name-resolution engine for a plain relation; the engine that
    /// *consumes* the names is fully ported in `backend-utils-adt-ruleutils`.
    /// Owner: the relcache/table-AM substrate (not the deparse engine).
    pub fn ruleutils_relation_real_colnames<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>>
);

seam_core::seam!(
    /// The function-RTE branch of `set_relation_column_names` (ruleutils.c
    /// 4434-4439): `expandRTE(rte, 1, 0, VAR_RETURNING_DEFAULT, -1, true
    /// /* include dropped */, &colnames, NULL)` — the up-to-date column names of
    /// a `RTE_FUNCTION` returning a composite, with dropped columns included as
    /// empty strings (which the engine maps to `None`). Returns one entry per
    /// column (`None` for a dropped column). Reads type catalogs, so it can
    /// `ereport(ERROR)`; allocated in `mcx`. Owner: the parser's `parse_relation`
    /// (`expandRTE`), not the deparse engine.
    pub fn ruleutils_expand_function_rte_colnames<'mcx>(
        mcx: Mcx<'mcx>,
        rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
    ) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>>
);

seam_core::seam!(
    /// `generate_operator_name(operid, arg1, arg2)` (ruleutils.c, static): the
    /// possibly-schema-qualified operator name to use in deparsed output. C's
    /// body calls `OpernameGetCandidates` / `OpernameGetOprid` (parse_oper.c) to
    /// decide whether the unqualified name would re-parse to the same operator,
    /// and `get_namespace_name_or_temp` to qualify if not. Those parser/catalog
    /// helpers are unported (a later ruleutils family / parse_oper), so the
    /// expression deparser (F1) reaches this name generator through a seam; the
    /// owner installs the body when the catalog-def-builder family lands. Reads
    /// the catalog, so it can `ereport(ERROR)`. Allocated in `mcx`.
    pub fn generate_operator_name<'mcx>(
        mcx: Mcx<'mcx>,
        operid: Oid,
        arg1: Oid,
        arg2: Oid,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `generate_function_name(funcid, nargs, argnames, argtypes, has_variadic,
    /// use_variadic_p, inGroupBy)` (ruleutils.c, static): the
    /// possibly-schema-qualified function name to use in deparsed output, and
    /// (when `want_use_variadic`) whether `VARIADIC` should be printed. C's body
    /// calls `func_get_detail` (parse_func.c) to decide whether the unqualified
    /// name + argtypes would re-resolve to the same function. That parser helper
    /// is unported, so the expression deparser (F1) reaches this name generator
    /// through a seam; the owner installs the body when the catalog-def-builder
    /// family lands. Returns `(name, use_variadic)`. Reads the catalog, so it can
    /// `ereport(ERROR)`. Allocated in `mcx`.
    pub fn generate_function_name<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
        nargs: i32,
        argnames: PgVec<'mcx, Option<PgString<'mcx>>>,
        argtypes: PgVec<'mcx, Oid>,
        has_variadic: bool,
        want_use_variadic: bool,
        in_group_by: bool,
    ) -> PgResult<(PgString<'mcx>, bool)>
);

seam_core::seam!(
    /// `get_rte_attribute_name(rte, attnum)` (parser/parse_relation.c): the
    /// column name for a (possibly system-) column number of an RTE, used by
    /// ruleutils' `get_variable` for system columns (`attnum < 0`). C consults
    /// the RTE's alias/eref column lists and the fixed system-column names. The
    /// parser owner is unported, so ruleutils' expression deparser reaches it
    /// through a seam. Allocated in `mcx`; can `ereport(ERROR)`.
    pub fn get_rte_attribute_name<'mcx>(
        mcx: Mcx<'mcx>,
        rte: &types_nodes::parsenodes::RangeTblEntry<'mcx>,
        attnum: i16,
    ) -> PgResult<PgString<'mcx>>
);

seam_core::seam!(
    /// `generate_relation_name(relid, namespaces)` (ruleutils.c, static): the
    /// possibly-schema-qualified, quoted relation name to display for `relid`.
    /// The CTE-name-conflict scan over `namespaces->ctes` is done in-crate (the
    /// deparse engine owns the namespace), so the seam carries only the
    /// remaining catalog half: `force_qual` forces qualification (the in-crate
    /// CTE check found a conflict), otherwise C qualifies iff
    /// `!RelationIsVisible(relid)`. C body: `SearchSysCache1(RELOID)` →
    /// `relname`/`relnamespace`, `RelationIsVisible`, `get_namespace_name_or_temp`,
    /// `quote_qualified_identifier`. Reads the catalog, so it can
    /// `ereport(ERROR)` (cache lookup failure); allocated in `mcx`. Owner: the
    /// relcache/namespace substrate (`SearchSysCacheRELOID` + `RelationIsVisible`).
    pub fn generate_relation_name<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        force_qual: bool,
    ) -> PgResult<PgString<'mcx>>
);


seam_core::seam!(
    /// `AcquireRewriteLocks(query, false, false)` (rewriteHandler.c): before
    /// deparsing a `Query`, take `AccessShareLock` on all referenced relations
    /// and fix up deleted columns in JOIN RTEs (scribbling on the passed
    /// querytree). `get_query_def` calls this first. The rewriter is unported
    /// (rewriteHandler.c NEEDS_DECOMP), so this crosses an owner seam; it takes
    /// `&mut Query` because C mutates the tree in place. Reads the catalog and
    /// takes locks, so it can `ereport(ERROR)`. Owner: rewriteHandler.c.
    pub fn acquire_rewrite_locks<'mcx>(
        mcx: Mcx<'mcx>,
        query: &mut types_nodes::copy_query::Query<'mcx>,
        forexecute: bool,
        forupdatepusheddown: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `flatten_group_exprs(NULL, query, node)` (optimizer/util/var.c): when a
    /// query has a GROUP RTE (`query->hasGroupRTE`), replace any Vars in `node`
    /// that reference GROUP outputs with the underlying grouping expressions, so
    /// the deparsed targetlist/HAVING reads back as the original expressions.
    /// `get_query_def` applies this to the targetlist and havingQual. The
    /// optimizer var.c half is unported, so this crosses an owner seam. Reads
    /// the query's grouping clauses; can `ereport(ERROR)`. Owner: optimizer var.c.
    pub fn flatten_group_exprs<'mcx>(
        mcx: Mcx<'mcx>,
        query: &types_nodes::copy_query::Query<'mcx>,
        node: &Node<'mcx>,
    ) -> PgResult<PgBox<'mcx, Node<'mcx>>>
);

seam_core::seam!(
    /// `lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR)`
    /// (typcache.c) — the `(lt_opr, gt_opr)` pair for a type, as
    /// `get_rule_orderby` uses to decide whether a SortGroupClause's `sortop`
    /// is the default ASC (`lt_opr`) or DESC (`gt_opr`) operator. Returns
    /// `(InvalidOid, InvalidOid)` when the type has no btree ordering. The
    /// trimmed `TypeCacheEntry` returned by ordinary `lookup_type_cache` does
    /// not carry both at once, so the deparser reaches them through this
    /// dedicated accessor. `Err` carries the typcache lookup surface.
    pub fn lookup_type_cache_lt_gt_opr(
        type_id: Oid,
    ) -> PgResult<(Oid, Oid)>
);

seam_core::seam!(
    /// `generate_collation_name(collid)` (ruleutils.c): the schema-qualified,
    /// quoted collation name for a collation OID, allocated in `mcx`. Reads
    /// `pg_collation`/`pg_namespace`, so it can `ereport(ERROR)` (cache lookup
    /// failure); `Err` also carries OOM. Owner unported, so this panics until
    /// ruleutils lands.
    pub fn generate_collation_name<'mcx>(
        mcx: Mcx<'mcx>,
        collid: Oid,
    ) -> PgResult<PgString<'mcx>>
);
