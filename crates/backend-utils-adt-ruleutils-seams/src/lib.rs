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
    /// `select_rtable_names_for_explain(rtable, rels_used)` (ruleutils.c):
    /// choose the unique alias name to display for each RTE actually referenced
    /// in the plan (`rels_used`), returning a `List *` of `char *` (a `None`
    /// element means "use the RTE's eref alias"). Reads the catalog, so it can
    /// `ereport(ERROR)`. Allocated in `mcx`.
    pub fn select_rtable_names_for_explain<'mcx>(
        mcx: Mcx<'mcx>,
        rtable: &PgVec<'mcx, types_nodes::parsenodes::RangeTblEntry<'mcx>>,
        rels_used: &types_nodes::bitmapset::Bitmapset<'mcx>,
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
