//! Seam declarations for the `backend-nodes-core` unit (here:
//! `nodes/bitmapset.c`, the `Bitmapset` set operations).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Allocating operations take the target context
//! handle (C: they palloc in `CurrentMemoryContext`).

seam_core::seam!(
    /// `bms_is_member(x, a)` (bitmapset.c): is `x` a member of `a`? A `None`
    /// set is the C NULL (empty) set. Infallible (the C can `elog(ERROR)` on
    /// a negative `x`, which the owner ports as a panic — caller bug).
    pub fn bms_is_member(x: i32, a: Option<&types_nodes::Bitmapset<'_>>) -> bool
);

seam_core::seam!(
    /// `bms_add_member(a, x)` (bitmapset.c): add `x` to the set, recycling
    /// the input (the C reallocs/extends `a` in place and returns it; a
    /// `None` input is the C NULL set). Growth allocates in `mcx`, so the
    /// call is fallible on OOM; the C `elog(ERROR)` on a negative `x` is the
    /// owner's to raise, also carried on `Err`.
    pub fn bms_add_member<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        x: i32,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>
);

seam_core::seam!(
    /// `bms_next_member(a, prevbit)` (bitmapset.c): return the next set bit
    /// strictly greater than `prevbit`, or `-2` past the last member (the C
    /// returns `-2` once exhausted; callers stop on `< 0`). A `None` set is the
    /// C NULL (empty) set. Infallible.
    pub fn bms_next_member(a: Option<&types_nodes::Bitmapset<'_>>, prevbit: i32) -> i32
);

seam_core::seam!(
    /// `bms_is_empty(a)` (bitmapset.c): is the set empty? A `None` set is the
    /// C NULL set, which is empty. Infallible.
    pub fn bms_is_empty(a: Option<&types_nodes::Bitmapset<'_>>) -> bool
);

seam_core::seam!(
    /// `bms_intersect(a, b)` (bitmapset.c): form a new set with the
    /// intersection of the inputs (allocates the copy in `mcx`; `None` in or
    /// empty result is the C NULL).
    pub fn bms_intersect<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_join(a, b)` (bitmapset.c): form the union, recycling the inputs
    /// (both are consumed; the C reuses the larger input's storage and frees
    /// the other — no allocation, so the call is infallible).
    pub fn bms_join<'mcx>(
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        b: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
    ) -> Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>
);

seam_core::seam!(
    /// `bms_union(a, b)` (bitmapset.c): form a new set with the union of the
    /// inputs (copies the larger input into `mcx`).
    pub fn bms_union<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_nonempty_difference(a, b)` (bitmapset.c): is there a member of `a`
    /// that is not in `b`? Computes `a - b` and reports whether it is nonempty
    /// without materializing the difference; a `None` set is the C NULL (empty).
    /// Infallible (no allocation).
    pub fn bms_nonempty_difference(
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> bool
);

seam_core::seam!(
    /// `bms_copy(a)` (bitmapset.c): a palloc'd duplicate of `a` (a `None` input
    /// is the C NULL, copied as `None`). Allocates in `mcx`, so fallible on OOM.
    pub fn bms_copy<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_add_members(a, b)` (bitmapset.c): add every member of `b` to `a`,
    /// recycling `a` (the C extends `a` in place and returns it; a `None` input
    /// is the C NULL set). Growth allocates in `mcx`, so fallible on OOM.
    pub fn bms_add_members<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_del_member(a, x)` (bitmapset.c): remove `x` from the set,
    /// recycling the input (the C clears the bit in place and returns `a`,
    /// shrinking `nwords` if trailing words become zero; a `None` input is
    /// the C NULL set, returned unchanged). No allocation, so infallible.
    pub fn bms_del_member<'mcx>(
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        x: i32,
    ) -> Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>
);

// === tidbitmap (tidbitmap.c) ===============================================

seam_core::seam!(
    /// `tbm_add_tuples(tbm, &tid, 1, false)` (tidbitmap.c): add one heap TID
    /// to the bitmap. The index AM holds the real `TIDBitmap *` (C: the
    /// executor's `node->tbm`); the owner mutates its boxed interior. `Err`
    /// carries OOM from growing the bitmap.
    pub fn tbm_add_tuple(
        tbm: &mut types_tidbitmap::TIDBitmap,
        tid: types_tuple::heaptuple::ItemPointerData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tbm_add_tuples(tbm, tids, ntids, recheck)` (tidbitmap.c): add an array
    /// of heap TIDs to the bitmap. The index AM holds the real `TIDBitmap *`;
    /// the owner mutates its boxed interior. `Err` carries OOM from growing the
    /// bitmap.
    pub fn tbm_add_tuples(
        tbm: &mut types_tidbitmap::TIDBitmap,
        tids: &[types_tuple::heaptuple::ItemPointerData],
        recheck: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tbm_add_page(tbm, pageno)` (tidbitmap.c): mark an entire heap page lossy
    /// in the bitmap. BRIN's `bringetbitmap` adds whole page ranges this way.
    /// The index AM holds the real `TIDBitmap *`; the owner mutates its boxed
    /// interior. `Err` carries OOM from growing the bitmap.
    pub fn tbm_add_page(
        tbm: &mut types_tidbitmap::TIDBitmap,
        pageno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `bms_num_members(a)` (bitmapset.c): count the members of `a`. A `None`
    /// set is the C NULL (empty) set, yielding 0. Infallible.
    pub fn bms_num_members(a: Option<&types_nodes::Bitmapset<'_>>) -> i32
);

seam_core::seam!(
    /// `bms_prev_member(a, prevbit)` (bitmapset.c): the greatest member of `a`
    /// less than `prevbit` (pass `-1` to start from the top). Returns `-2`
    /// when there is no such member. Infallible.
    pub fn bms_prev_member(a: Option<&types_nodes::Bitmapset<'_>>, prevbit: i32) -> i32
);

seam_core::seam!(
    /// `bms_overlap(a, b)` (bitmapset.c): do the two sets have a common
    /// member? Infallible.
    pub fn bms_overlap(
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> bool
);

seam_core::seam!(
    /// `bms_add_range(a, lower, upper)` (bitmapset.c): add all integers in the
    /// inclusive range `[lower, upper]` to the set, recycling the input (the C
    /// extends `a` in place and returns it; a `None` input is the C NULL set,
    /// and an empty range with `upper < lower` returns it unchanged). Growth
    /// allocates in `mcx`, so the call is fallible on OOM.
    pub fn bms_add_range<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        lower: i32,
        upper: i32,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// `bms_del_members(a, b)` (bitmapset.c): remove the members of `b` from
    /// `a`, recycling and returning `a` (a `None`/empty result is the C NULL
    /// set). No allocation, so infallible.
    pub fn bms_del_members<'mcx>(
        a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>
);

seam_core::seam!(
    /// `bms_equal(a, b)` (bitmapset.c): do `a` and `b` contain the same
    /// members? (`None`/empty sets are equal to each other.) Infallible.
    pub fn bms_equal(
        a: Option<&types_nodes::Bitmapset<'_>>,
        b: Option<&types_nodes::Bitmapset<'_>>,
    ) -> bool
);

seam_core::seam!(
    /// `bms_free(a)` (bitmapset.c): free the bitmapset (a `None` input is the C
    /// NULL, a no-op). The owned model consumes the set; infallible.
    pub fn bms_free<'mcx>(a: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>)
);

seam_core::seam!(
    /// `expression_tree_walker(node, walker, context)` (nodes/nodeFuncs.c): the
    /// generic expression-tree recursion driver. Visits `node`'s immediate
    /// expression children, invoking `walker` on each (the walker is
    /// responsible for re-recursing). `context` is captured by the `FnMut`
    /// closure in the owned model. Returns `true` as soon as a `walker` call
    /// returns `true` (early abort), else `false`. Infallible (the recursion
    /// itself never `ereport`s; a walker may).
    pub fn expression_tree_walker(
        node: &types_nodes::nodes::Node<'_>,
        walker: &mut dyn FnMut(&types_nodes::nodes::Node<'_>) -> bool,
    ) -> bool
);

/* ==========================================================================
 * print.c consumed edges
 *
 * The `print` family (`nodes/print.c`) renders node trees for debugging. Its
 * whole-tree serialization edge is owned by `outfuncs` (`nodes/outfuncs.c`,
 * the separate `backend-nodes-outfuncs` catalog unit) which is NOT yet ported;
 * `print`/`pprint`/`elog_node_display` drive it through the seam below. It
 * stays UNINSTALLED (panics on call) until `outfuncs` lands — `mirror-pg-and-panic`.
 * ======================================================================== */

seam_core::seam!(
    /// `nodeToStringWithLocations(obj)` (`nodes/outfuncs.c`): serialize an
    /// arbitrary node tree to its textual `nodeToString` rendering (with parse
    /// locations retained), allocated in `mcx` (C: palloc'd `char *`). The
    /// owner is the unported `outfuncs` unit. `Err` carries OOM / any error the
    /// serializer raises.
    pub fn node_to_string_with_locations<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        obj: &types_nodes::nodes::Node<'_>,
    ) -> types_error::PgResult<mcx::PgString<'mcx>>
);

/* --------------------------------------------------------------------------
 * print.c ad-hoc debug printers blocked on trimmed / unmodeled foreign carriers
 *
 * These four `print.c` routines reach into carrier fields that are deliberately
 * trimmed out of foreign-owned carrier structs, or into an entirely-unmodeled
 * foreign planner / execTuples model, so they cannot be expressed against the
 * current vocabulary:
 *
 *   * `print_rt`   needs `RangeTblEntry.{eref->aliasname, inh, inFromCl}` —
 *                  trimmed out of `types_nodes::parsenodes::RangeTblEntry`
 *                  (eref/`Alias` not modeled at all);
 *   * `print_expr` needs, in its `Var` arm, `rt_fetch(varno)->eref->aliasname`
 *                  and `get_rte_attribute_name(rte, varattno)` (parser
 *                  `parsetree`), again gated on the trimmed `eref`;
 *   * `print_pathkeys` walks `pk_eclass->ec_members` chasing `ec_merged` —
 *                  `EquivalenceMember`/`ec_members` are unmodeled in
 *                  `types_pathnodes` and resolving the `EcId` handle needs the
 *                  planner's `eq_classes` side-table;
 *   * `print_tl`   needs `TargetEntry.{resno, ressortgroupref}` (trimmed out of
 *                  `types_nodes::primnodes::TargetEntry`) and `print_expr`;
 *   * `print_slot` drives `printtup::debugtup` over the `TupleTableSlot`
 *                  execution runtime, which the execTuples slot model does not
 *                  yet expose.
 *
 * The seams are owned by those genuine (unported / not-yet-expanded) owners and
 * stay UNINSTALLED — a call panics — until they land. `mirror-pg-and-panic`.
 * ------------------------------------------------------------------------ */

seam_core::seam!(
    /// `print_rt(rtable)` (`nodes/print.c`): print the range table to stdout.
    /// Owned and installed by `backend-nodes-core` (the `print` family). Takes an
    /// `Mcx` for the transient strings the printer materializes (C uses the
    /// ambient `CurrentMemoryContext`).
    pub fn print_rt<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `print_expr(expr, rtable)` (`nodes/print.c`): print an expression to
    /// stdout. Owned and installed by `backend-nodes-core`; reaches
    /// `get_rte_attribute_name`/`get_type_output_info`/`oid_output_function_call`/
    /// `get_opname`/`get_func_name` through their owner seams. Takes an `Mcx` for
    /// the transient strings (C uses the ambient `CurrentMemoryContext`).
    pub fn print_expr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        expr: Option<&types_nodes::nodes::Node<'_>>,
        rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `print_pathkeys(pathkeys, rtable)` (`nodes/print.c`): print a `PathKey`
    /// list to stdout. Owned by the planner pathkeys surface (needs the
    /// `EquivalenceMember`/`ec_members` model + `eq_classes` side-table).
    pub fn print_pathkeys(
        pathkeys: &[types_pathnodes::PathKey],
        rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `print_tl(tlist, rtable)` (`nodes/print.c`): print a targetlist to
    /// stdout. Owned by the outfuncs/parsetree surface that carries the full
    /// `TargetEntry` (with `resno`/`ressortgroupref`).
    pub fn print_tl<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tlist: &[types_nodes::primnodes::TargetEntry<'mcx>],
        rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `print_slot(slot)` (`nodes/print.c`): print the tuple in the given
    /// `TupleTableSlot` via `debugtup`. Owned by the execTuples/printtup
    /// surface that exposes the live `TupleTableSlot` runtime.
    pub fn print_slot(slot: &types_nodes::tuptable::SlotBase<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CallStmtResultDesc(stmt)` (functioncmds.c:2383) — the polymorphic
    /// output-argument tuple descriptor for a CALL. Re-homed here (from
    /// `backend-nodes-nodeFuncs-seams`) onto the `backend-nodes-core` owner so
    /// the seam-install guard can track it: the function is keyed entirely by
    /// the unported planner expression node `stmt->funcexpr` (`FuncExpr.funcid`,
    /// which functioncmds carries opaquely — the layered node tree does not yet
    /// model the call expression), runs `build_function_result_tupdesc_t` over
    /// the `PROCOID` tuple, and re-types each output column from
    /// `stmt->outargs[i]` via `exprType`. Both the `funcid` read and the
    /// `exprType` fixup are nodeFuncs/nodes-core expression-tree territory, and
    /// the tupdesc spine is funcapi-owned, so the whole body still lands behind
    /// the real owners — it is DESIGN_DEBT (CONTRACT_RECONCILE_PENDING). Takes
    /// `Mcx<'mcx>`, returns the descriptor in the caller's context. Fallible on
    /// the cache-lookup `ereport(ERROR)`.
    pub fn call_stmt_result_desc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: types_parsenodes::CallStmt,
    ) -> types_error::PgResult<types_tuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// The non-`FuncExpr`/`OpExpr` arms of `get_expr_result_type` (funcapi.c):
    /// the `IsA` dispatch over `RowExpr`/`Const`/generic expression that
    /// inspects the expression node's tag and per-variant fields (`row_typeid`,
    /// `args`/`colnames`, the RECORD `Const` datum) and runs `exprType` /
    /// `CreateTemplateTupleDesc` / `BlessTupleDesc` / `lookup_rowtype_tupdesc_copy`
    /// / `get_type_func_class` over them. Re-homed here (from
    /// `backend-nodes-nodeFuncs-seams`) onto the `backend-nodes-core` owner so
    /// the guard can track it. The expression-node tree is owned by the
    /// nodeFuncs/parser side and the FuncExpr/OpExpr funnel folds back into the
    /// funcapi-owned `internal_get_result_type` (no callback seam exists yet), so
    /// the body still lands behind the real owners — DESIGN_DEBT
    /// (CONTRACT_RECONCILE_PENDING). `expr == None` is the C `NULL` (generic
    /// `exprType` path on a NULL node). `Err` carries the lookup/
    /// `assign_record_type_typmod` `ereport(ERROR)` surface.
    pub fn get_expr_result_type_node<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        expr: Option<&types_nodes::nodes::Node<'mcx>>,
    ) -> types_error::PgResult<types_nodes::funcapi::ResolvedResultType<'mcx>>
);
