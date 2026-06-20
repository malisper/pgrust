// Raw expression/grammar nodes, range/join nodes, clauses, value nodes, and
// the small-enum discriminant mappers (included into convert.rs).

use types_nodes::jointype::JoinType;
use types_nodes::modifytable::{MergeMatchKind, OverridingKind};
use types_nodes::nodelimit::LimitOption;
use types_nodes::nodes::{CmdType, OnConflictAction};

// ---------------------------------------------------------------------------
// Raw expression nodes
// ---------------------------------------------------------------------------

fn conv_a_expr<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::A_Expr) -> PgResult<tn::A_Expr<'mcx>> {
    let e = unsafe { &*p };
    Ok(tn::A_Expr {
        kind: a_expr_kind(e.kind),
        name: node_list(mcx, e.name)?,
        lexpr: node_opt(mcx, e.lexpr)?,
        rexpr: node_opt(mcx, e.rexpr)?,
        rexpr_list_start: e.rexpr_list_start,
        rexpr_list_end: e.rexpr_list_end,
        location: e.location,
    })
}

fn conv_columnref<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::ColumnRef) -> PgResult<tn::ColumnRef<'mcx>> {
    let c = unsafe { &*p };
    Ok(tn::ColumnRef {
        fields: node_list(mcx, c.fields)?,
        location: c.location,
    })
}

fn conv_paramref(p: *mut cs::ParamRef) -> tn::ParamRef {
    let r = unsafe { &*p };
    tn::ParamRef {
        number: r.number,
        location: r.location,
    }
}

fn conv_funccall<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::FuncCall) -> PgResult<tn::FuncCall<'mcx>> {
    let f = unsafe { &*p };
    Ok(tn::FuncCall {
        funcname: node_list(mcx, f.funcname)?,
        args: node_list(mcx, f.args)?,
        agg_order: node_list(mcx, f.agg_order)?,
        agg_filter: node_opt(mcx, f.agg_filter)?,
        over: child_opt(mcx, f.over, conv_windowdef)?,
        agg_within_group: f.agg_within_group,
        agg_star: f.agg_star,
        agg_distinct: f.agg_distinct,
        func_variadic: f.func_variadic,
        funcformat: coercion_form(f.funcformat),
        location: f.location,
    })
}

fn conv_a_indices<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::A_Indices) -> PgResult<tn::A_Indices<'mcx>> {
    let a = unsafe { &*p };
    Ok(tn::A_Indices {
        is_slice: a.is_slice,
        lidx: node_opt(mcx, a.lidx)?,
        uidx: node_opt(mcx, a.uidx)?,
    })
}

fn conv_a_indirection<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::A_Indirection,
) -> PgResult<tn::A_Indirection<'mcx>> {
    let a = unsafe { &*p };
    Ok(tn::A_Indirection {
        arg: node_opt(mcx, a.arg)?,
        indirection: node_list(mcx, a.indirection)?,
    })
}

fn conv_a_arrayexpr<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::A_ArrayExpr,
) -> PgResult<tn::A_ArrayExpr<'mcx>> {
    let a = unsafe { &*p };
    Ok(tn::A_ArrayExpr {
        elements: node_list(mcx, a.elements)?,
        list_start: a.list_start,
        list_end: a.list_end,
        location: a.location,
    })
}

fn conv_restarget<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::ResTarget) -> PgResult<tn::ResTarget<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::ResTarget {
        name: cstr_opt(mcx, r.name)?,
        indirection: node_list(mcx, r.indirection)?,
        val: node_opt(mcx, r.val)?,
        location: r.location,
    })
}

fn conv_multiassignref<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::MultiAssignRef,
) -> PgResult<tn::MultiAssignRef<'mcx>> {
    let m = unsafe { &*p };
    Ok(tn::MultiAssignRef {
        source: node_opt(mcx, m.source)?,
        colno: m.colno,
        ncolumns: m.ncolumns,
    })
}

fn conv_typecast<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::TypeCast) -> PgResult<tn::TypeCast<'mcx>> {
    let t = unsafe { &*p };
    Ok(tn::TypeCast {
        arg: node_opt(mcx, t.arg)?,
        typeName: child_opt(mcx, t.typeName, conv_typename)?,
        location: t.location,
    })
}

fn conv_collate<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::CollateClause) -> PgResult<tn::CollateClause<'mcx>> {
    let c = unsafe { &*p };
    Ok(tn::CollateClause {
        arg: node_opt(mcx, c.arg)?,
        collname: node_list(mcx, c.collname)?,
        location: c.location,
    })
}

fn conv_sortby<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::SortBy) -> PgResult<tn::SortBy<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::SortBy {
        node: node_opt(mcx, s.node)?,
        sortby_dir: sort_by_dir(s.sortby_dir),
        sortby_nulls: sort_by_nulls(s.sortby_nulls),
        useOp: node_list(mcx, s.useOp)?,
        location: s.location,
    })
}

fn conv_windowdef<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::WindowDef) -> PgResult<tn::WindowDef<'mcx>> {
    let w = unsafe { &*p };
    Ok(tn::WindowDef {
        name: cstr_opt(mcx, w.name)?,
        refname: cstr_opt(mcx, w.refname)?,
        partitionClause: node_list(mcx, w.partitionClause)?,
        orderClause: node_list(mcx, w.orderClause)?,
        frameOptions: w.frameOptions,
        startOffset: node_opt(mcx, w.startOffset)?,
        endOffset: node_opt(mcx, w.endOffset)?,
        location: w.location,
    })
}

fn conv_rangesubselect<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::RangeSubselect,
) -> PgResult<tn::RangeSubselect<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::RangeSubselect {
        lateral: r.lateral,
        subquery: node_opt(mcx, r.subquery)?,
        alias: child_opt(mcx, r.alias, conv_alias)?,
    })
}

fn conv_rangefunction<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::RangeFunction,
) -> PgResult<tn::RangeFunction<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::RangeFunction {
        lateral: r.lateral,
        ordinality: r.ordinality,
        is_rowsfrom: r.is_rowsfrom,
        functions: conv_rangefunction_functions(mcx, r.functions)?,
        alias: child_opt(mcx, r.alias, conv_alias)?,
        coldeflist: node_list(mcx, r.coldeflist)?,
    })
}

/// Convert `RangeFunction.functions` — a `List` whose every element is itself a
/// 2-element sublist `list_make2(func_expr, coldeflist)` (gram.y `func_table`).
/// The first element is the function-call expression (required); the second is
/// a `List *` column-definition list that is **NIL (NULL) when absent**.
///
/// The generic `node_list` path can't handle this: it routes each sublist cell
/// through `node_req`, which would panic on the NIL coldeflist cell (a NULL
/// `List *` carries no `NodeTag`). Mirror C faithfully: each sublist becomes a
/// `Node::List` of exactly two entries — the converted funcexpr and the
/// coldeflist as a `Node::List` (empty when NIL).
fn conv_rangefunction_functions<'mcx>(
    mcx: Mcx<'mcx>,
    l: *mut RawList,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    if l.is_null() {
        return Ok(PgVec::new_in(mcx));
    }
    let list: &RawList = unsafe { &*l };
    let mut out = mcx::vec_with_capacity_in(mcx, list.len().max(0) as usize)?;
    for cell in list.cells() {
        let sub: *mut RawList = cell.ptr::<RawList>();
        let sublist: &RawList = unsafe { &*sub };
        let sub_cells = sublist.cells();

        // First element: the function-call expression (required Node).
        let fexpr_cell = sub_cells
            .first()
            .expect("RangeFunction per-function sublist is missing its funcexpr");
        let fexpr_ptr: *mut RawNode = fexpr_cell.ptr::<RawNode>();
        let fexpr = node_req(mcx, fexpr_ptr)?;

        // Second element: the coldeflist, a `List *` that may be NIL (NULL).
        let coldef_raw: *mut RawList = match sub_cells.get(1) {
            Some(c) => c.ptr::<RawList>(),
            None => core::ptr::null_mut(),
        };
        let coldef = Node::mk_list(mcx, node_list(mcx, coldef_raw)?)?;

        let mut pair = mcx::vec_with_capacity_in::<NodePtr<'mcx>>(mcx, 2)?;
        pair.push(fexpr);
        pair.push(mcx::alloc_in(mcx, coldef)?);
        out.push(mcx::alloc_in(mcx, Node::mk_list(mcx, pair)?)?);
    }
    Ok(out)
}

fn conv_rangetablesample<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::RangeTableSample,
) -> PgResult<tn::RangeTableSample<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::RangeTableSample {
        relation: node_opt(mcx, r.relation)?,
        method: node_list(mcx, r.method)?,
        args: node_list(mcx, r.args)?,
        repeatable: node_opt(mcx, r.repeatable)?,
        location: r.location,
    })
}

fn conv_typename<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::TypeName) -> PgResult<tn::TypeName<'mcx>> {
    let t = unsafe { &*p };
    Ok(tn::TypeName {
        names: node_list(mcx, t.names)?,
        typeOid: t.typeOid,
        setof: t.setof,
        pct_type: t.pct_type,
        typmods: node_list(mcx, t.typmods)?,
        typemod: t.typemod,
        arrayBounds: node_list(mcx, t.arrayBounds)?,
        location: t.location,
    })
}

fn conv_columndef<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut backend_nodes_types::parsenodes_ddl::ColumnDef,
) -> PgResult<tn::ColumnDef<'mcx>> {
    let c = unsafe { &*p };
    Ok(tn::ColumnDef {
        colname: cstr_opt(mcx, c.colname)?,
        typeName: child_opt(mcx, c.typeName, conv_typename)?,
        compression: cstr_opt(mcx, c.compression)?,
        inhcount: c.inhcount,
        is_local: c.is_local,
        is_not_null: c.is_not_null,
        is_from_type: c.is_from_type,
        storage: c.storage as i8,
        storage_name: cstr_opt(mcx, c.storage_name)?,
        raw_default: node_opt(mcx, c.raw_default)?,
        cooked_default: node_opt(mcx, c.cooked_default)?,
        identity: c.identity as i8,
        identitySequence: child_opt(mcx, c.identitySequence, conv_rangevar)?,
        generated: c.generated as i8,
        collClause: child_opt(mcx, c.collClause, conv_collate)?,
        collOid: c.collOid,
        constraints: node_list(mcx, c.constraints)?,
        fdwoptions: node_list(mcx, c.fdwoptions)?,
        location: c.location,
    })
}

// ---------------------------------------------------------------------------
// Range / join structure
// ---------------------------------------------------------------------------

fn conv_rangevar<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::RangeVar) -> PgResult<tn::RangeVar<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::RangeVar {
        catalogname: cstr_opt(mcx, r.catalogname)?,
        schemaname: cstr_opt(mcx, r.schemaname)?,
        relname: cstr_opt(mcx, r.relname)?,
        inh: r.inh,
        relpersistence: r.relpersistence as i8,
        alias: child_opt(mcx, r.alias, conv_alias)?,
        location: r.location,
    })
}

fn conv_alias<'mcx>(mcx: Mcx<'mcx>, p: *mut backend_nodes_types::Alias) -> PgResult<tn::Alias<'mcx>> {
    let a = unsafe { &*p };
    Ok(tn::Alias {
        aliasname: cstr_opt(mcx, a.aliasname)?,
        colnames: node_list(mcx, a.colnames)?,
    })
}

fn conv_joinexpr<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::JoinExpr) -> PgResult<tn::JoinExpr<'mcx>> {
    let j = unsafe { &*p };
    Ok(tn::JoinExpr {
        jointype: join_type(j.jointype),
        isNatural: j.is_natural,
        larg: node_opt(mcx, j.larg)?,
        rarg: node_opt(mcx, j.rarg)?,
        usingClause: node_list(mcx, j.using_clause)?,
        join_using_alias: child_opt(mcx, j.join_using_alias, conv_alias)?,
        quals: node_opt(mcx, j.quals)?,
        alias: child_opt(mcx, j.alias, conv_alias)?,
        rtindex: j.rtindex,
    })
}

fn conv_fromexpr<'mcx>(mcx: Mcx<'mcx>, p: *mut cpr::FromExpr) -> PgResult<tn::FromExpr<'mcx>> {
    let f = unsafe { &*p };
    Ok(tn::FromExpr {
        fromlist: node_list(mcx, f.fromlist)?,
        quals: node_opt(mcx, f.quals)?,
    })
}

fn conv_rangetblref(p: *mut cpr::RangeTblRef) -> tn::RangeTblRef {
    let r = unsafe { &*p };
    tn::RangeTblRef { rtindex: r.rtindex }
}

// ---------------------------------------------------------------------------
// Clauses / specs
// ---------------------------------------------------------------------------

fn conv_withclause<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut backend_nodes_types::WithClause,
) -> PgResult<tn::WithClause<'mcx>> {
    let w = unsafe { &*p };
    Ok(tn::WithClause {
        ctes: node_list(mcx, w.ctes)?,
        recursive: w.recursive,
        location: w.location,
    })
}

fn conv_cte<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::CommonTableExpr,
) -> PgResult<tn::CommonTableExpr<'mcx>> {
    let c = unsafe { &*p };
    Ok(tn::CommonTableExpr {
        ctename: cstr_opt(mcx, c.ctename)?,
        aliascolnames: node_list(mcx, c.aliascolnames)?,
        ctematerialized: cte_materialize(c.ctematerialized),
        ctequery: node_opt(mcx, c.ctequery)?,
        search_clause: child_opt(mcx, c.search_clause, conv_cte_search)?,
        cycle_clause: if c.cycle_clause.is_null() {
            None
        } else {
            let cc = conv_cte_cycle(mcx, c.cycle_clause)?;
            Some(mcx::alloc_in(
                mcx,
                types_nodes::nodes::Node::mk_cte_cycle_clause(mcx, cc)?,
            )?)
        },
        location: c.location,
        cterecursive: c.cterecursive,
        cterefcount: c.cterefcount,
        ctecolnames: node_list(mcx, c.ctecolnames)?,
        ctecoltypes: oid_list(mcx, c.ctecoltypes)?,
        ctecoltypmods: int_list(mcx, c.ctecoltypmods)?,
        ctecolcollations: oid_list(mcx, c.ctecolcollations)?,
    })
}

fn conv_cte_search<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::CTESearchClause,
) -> PgResult<tn::CTESearchClause<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::CTESearchClause {
        search_col_list: node_list(mcx, s.search_col_list)?,
        search_breadth_first: s.search_breadth_first,
        search_seq_column: cstr_opt(mcx, s.search_seq_column)?,
        location: s.location,
    })
}

fn conv_cte_cycle<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::CTECycleClause,
) -> PgResult<tn::CTECycleClause<'mcx>> {
    let c = unsafe { &*p };
    Ok(tn::CTECycleClause {
        cycle_col_list: node_list(mcx, c.cycle_col_list)?,
        cycle_mark_column: cstr_opt(mcx, c.cycle_mark_column)?,
        cycle_mark_value: node_opt(mcx, c.cycle_mark_value)?,
        cycle_mark_default: node_opt(mcx, c.cycle_mark_default)?,
        cycle_path_column: cstr_opt(mcx, c.cycle_path_column)?,
        location: c.location,
        cycle_mark_type: c.cycle_mark_type,
        cycle_mark_typmod: c.cycle_mark_typmod,
        cycle_mark_collation: c.cycle_mark_collation,
        cycle_mark_neop: c.cycle_mark_neop,
    })
}

fn conv_infer<'mcx>(mcx: Mcx<'mcx>, p: *mut cp::InferClause) -> PgResult<tn::InferClause<'mcx>> {
    let i = unsafe { &*p };
    Ok(tn::InferClause {
        indexElems: node_list(mcx, i.index_elems)?,
        whereClause: node_opt(mcx, i.where_clause)?,
        conname: cstr_opt(mcx, i.conname)?,
        location: i.location,
    })
}

fn conv_onconflict_clause<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::OnConflictClause,
) -> PgResult<tn::OnConflictClause<'mcx>> {
    let o = unsafe { &*p };
    Ok(tn::OnConflictClause {
        action: on_conflict_action(o.action),
        infer: child_opt(mcx, o.infer, conv_infer)?,
        targetList: node_list(mcx, o.target_list)?,
        whereClause: node_opt(mcx, o.where_clause)?,
        location: o.location,
    })
}

fn conv_mergewhen<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::MergeWhenClause,
) -> PgResult<tn::MergeWhenClause<'mcx>> {
    let m = unsafe { &*p };
    Ok(tn::MergeWhenClause {
        matchKind: merge_match_kind(m.match_kind),
        commandType: cmd_type(m.command_type),
        r#override: overriding_kind(m.override_),
        condition: node_opt(mcx, m.condition)?,
        targetList: node_list(mcx, m.target_list)?,
        values: node_list(mcx, m.values)?,
    })
}

fn conv_returning<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::ReturningClause,
) -> PgResult<tn::ReturningClause<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::ReturningClause {
        options: node_list(mcx, r.options)?,
        exprs: node_list(mcx, r.exprs)?,
    })
}

fn conv_returning_option<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::ReturningOption,
) -> PgResult<tn::ReturningOption<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::ReturningOption {
        option: returning_option_kind(r.option),
        value: cstr_opt(mcx, r.value)?,
        location: r.location,
    })
}

fn returning_option_kind(v: cp::ReturningOptionKind) -> tn::ReturningOptionKind {
    match v {
        cp::RETURNING_OPTION_OLD => tn::ReturningOptionKind::Old,
        cp::RETURNING_OPTION_NEW => tn::ReturningOptionKind::New,
        other => panic!("gram converter: invalid ReturningOptionKind {other}"),
    }
}

fn conv_trigger_transition<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::TriggerTransition,
) -> PgResult<tn::TriggerTransition<'mcx>> {
    let t = unsafe { &*p };
    Ok(tn::TriggerTransition {
        name: cstr_opt(mcx, t.name)?,
        isNew: t.is_new,
        isTable: t.is_table,
    })
}

fn conv_range_table_func<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::RangeTableFunc,
) -> PgResult<tn::RangeTableFunc<'mcx>> {
    let r = unsafe { &*p };
    Ok(tn::RangeTableFunc {
        lateral: r.lateral,
        docexpr: node_opt(mcx, r.docexpr)?,
        rowexpr: node_opt(mcx, r.rowexpr)?,
        namespaces: node_list(mcx, r.namespaces)?,
        columns: node_list(mcx, r.columns)?,
        alias: child_opt(mcx, r.alias, conv_alias)?,
        location: r.location,
    })
}

fn conv_range_table_func_col<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::RangeTableFuncCol,
) -> PgResult<tn::RangeTableFuncCol<'mcx>> {
    let c = unsafe { &*p };
    Ok(tn::RangeTableFuncCol {
        colname: cstr_opt(mcx, c.colname)?,
        typeName: child_opt(mcx, c.typeName, conv_typename)?,
        for_ordinality: c.for_ordinality,
        is_not_null: c.is_not_null,
        colexpr: node_opt(mcx, c.colexpr)?,
        coldefexpr: node_opt(mcx, c.coldefexpr)?,
        location: c.location,
    })
}

fn conv_groupingset<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::GroupingSet,
) -> PgResult<tn::GroupingSet<'mcx>> {
    let g = unsafe { &*p };
    Ok(tn::GroupingSet {
        kind: grouping_set_kind(g.kind),
        content: node_list(mcx, g.content)?,
        location: g.location,
    })
}

fn conv_windowclause<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::WindowClause,
) -> PgResult<tn::WindowClause<'mcx>> {
    let w = unsafe { &*p };
    Ok(tn::WindowClause {
        name: cstr_opt(mcx, w.name)?,
        refname: cstr_opt(mcx, w.refname)?,
        partitionClause: node_list(mcx, w.partition_clause)?,
        orderClause: node_list(mcx, w.order_clause)?,
        frameOptions: w.frame_options,
        startOffset: node_opt(mcx, w.start_offset)?,
        endOffset: node_opt(mcx, w.end_offset)?,
        startInRangeFunc: w.start_in_range_func,
        endInRangeFunc: w.end_in_range_func,
        inRangeColl: w.in_range_coll,
        inRangeAsc: w.in_range_asc,
        inRangeNullsFirst: w.in_range_nulls_first,
        winref: w.winref,
        copiedOrder: w.copied_order,
    })
}

fn conv_sortgroupclause(p: *mut cp::SortGroupClause) -> tn::SortGroupClause {
    let s = unsafe { &*p };
    tn::SortGroupClause {
        tleSortGroupRef: s.tle_sort_group_ref,
        eqop: s.eqop,
        sortop: s.sortop,
        reverse_sort: s.reverse_sort,
        nulls_first: s.nulls_first,
        hashable: s.hashable,
    }
}

fn conv_rowmark(p: *mut cp::RowMarkClause) -> tn::RowMarkClause {
    let r = unsafe { &*p };
    tn::RowMarkClause {
        rti: r.rti,
        strength: lock_clause_strength(r.strength),
        waitPolicy: lock_wait_policy(r.wait_policy),
        pushedDown: r.pushed_down,
    }
}

/// `LockingClause` (FOR [KEY] UPDATE/SHARE in the raw parse tree). `lockedRels`
/// is a `List *` of `RangeVar` (empty == "all rels"). Surfaced as the owned
/// `tn::LockingClause`; analyze (`transformLockingClause`) consumes it.
fn conv_lockingclause<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cp::LockingClause,
) -> PgResult<tn::LockingClause<'mcx>> {
    let l = unsafe { &*p };
    Ok(tn::LockingClause {
        lockedRels: node_list(mcx, l.locked_rels)?,
        strength: lock_clause_strength(l.strength),
        waitPolicy: lock_wait_policy(l.wait_policy),
    })
}

// ---------------------------------------------------------------------------
// Value (leaf literal) nodes — produce the central Node arms directly.
// ---------------------------------------------------------------------------

fn conv_value_node<'mcx>(mcx: Mcx<'mcx>, n: *mut RawNode) -> PgResult<Node<'mcx>> {
    let tag = unsafe { (*n).type_ };
    match tag {
        tags::T_Integer => {
            let v = unsafe { &*n.cast::<pgrust_pg_ffi::nodes::Integer>() };
            Ok(Node::mk_integer(mcx, tn_val::Integer { ival: v.ival })?)
        }
        tags::T_Float => {
            let v = unsafe { &*n.cast::<pgrust_pg_ffi::nodes::Float>() };
            Ok(Node::mk_float(mcx, tn_val::Float {
                fval: cstr(mcx, v.fval)?,
            })?)
        }
        tags::T_Boolean => {
            let v = unsafe { &*n.cast::<pgrust_pg_ffi::nodes::Boolean>() };
            Ok(Node::mk_boolean(mcx, tn_val::Boolean { boolval: v.boolval })?)
        }
        tags::T_String => {
            let v = unsafe { &*n.cast::<pgrust_pg_ffi::nodes::StringNode>() };
            Ok(Node::mk_string(mcx, tn_val::StringNode {
                sval: cstr(mcx, v.sval)?,
            })?)
        }
        tags::T_BitString => {
            let v = unsafe { &*n.cast::<pgrust_pg_ffi::nodes::BitString>() };
            Ok(Node::mk_bit_string(mcx, tn_val::BitString {
                bsval: cstr(mcx, v.bsval)?,
            })?)
        }
        _ => unreachable!("conv_value_node on non-value tag {tag}"),
    }
}

// ---------------------------------------------------------------------------
// Small-enum discriminant mappers.
//
// The raw side is a plain `c_uint` (PostgreSQL's C enum); the owned side is a
// `#[repr(u32)]` enum with identical C discriminants. We match the integer and
// build the owned variant explicitly (a corrupt out-of-range value is a
// mirror-PG-and-panic — the grammar never emits one).
// ---------------------------------------------------------------------------

fn a_expr_kind(v: cs::A_Expr_Kind) -> tn::A_Expr_Kind {
    use tn::A_Expr_Kind::*;
    match v {
        cs::AEXPR_OP => AEXPR_OP,
        cs::AEXPR_OP_ANY => AEXPR_OP_ANY,
        cs::AEXPR_OP_ALL => AEXPR_OP_ALL,
        cs::AEXPR_DISTINCT => AEXPR_DISTINCT,
        cs::AEXPR_NOT_DISTINCT => AEXPR_NOT_DISTINCT,
        cs::AEXPR_NULLIF => AEXPR_NULLIF,
        cs::AEXPR_IN => AEXPR_IN,
        cs::AEXPR_LIKE => AEXPR_LIKE,
        cs::AEXPR_ILIKE => AEXPR_ILIKE,
        cs::AEXPR_SIMILAR => AEXPR_SIMILAR,
        cs::AEXPR_BETWEEN => AEXPR_BETWEEN,
        cs::AEXPR_NOT_BETWEEN => AEXPR_NOT_BETWEEN,
        cs::AEXPR_BETWEEN_SYM => AEXPR_BETWEEN_SYM,
        cs::AEXPR_NOT_BETWEEN_SYM => AEXPR_NOT_BETWEEN_SYM,
        other => panic!("gram converter: invalid A_Expr_Kind {other}"),
    }
}

fn set_operation(v: cs::SetOperation) -> tn::SetOperation {
    use tn::SetOperation::*;
    match v {
        cs::SETOP_NONE => SETOP_NONE,
        cs::SETOP_UNION => SETOP_UNION,
        cs::SETOP_INTERSECT => SETOP_INTERSECT,
        cs::SETOP_EXCEPT => SETOP_EXCEPT,
        other => panic!("gram converter: invalid SetOperation {other}"),
    }
}

fn limit_option(v: cs::LimitOption) -> LimitOption {
    match v {
        cs::LIMIT_OPTION_COUNT => LimitOption::LIMIT_OPTION_COUNT,
        cs::LIMIT_OPTION_WITH_TIES => LimitOption::LIMIT_OPTION_WITH_TIES,
        other => panic!("gram converter: invalid LimitOption {other}"),
    }
}

fn overriding_kind(v: cpr::OverridingKind) -> OverridingKind {
    match v {
        cpr::OVERRIDING_NOT_SET => OverridingKind::OVERRIDING_NOT_SET,
        cpr::OVERRIDING_USER_VALUE => OverridingKind::OVERRIDING_USER_VALUE,
        cpr::OVERRIDING_SYSTEM_VALUE => OverridingKind::OVERRIDING_SYSTEM_VALUE,
        other => panic!("gram converter: invalid OverridingKind {other}"),
    }
}

fn on_conflict_action(v: cp::OnConflictAction) -> OnConflictAction {
    match v {
        cp::ONCONFLICT_NONE => OnConflictAction::ONCONFLICT_NONE,
        cp::ONCONFLICT_NOTHING => OnConflictAction::ONCONFLICT_NOTHING,
        cp::ONCONFLICT_UPDATE => OnConflictAction::ONCONFLICT_UPDATE,
        other => panic!("gram converter: invalid OnConflictAction {other}"),
    }
}

fn cmd_type(v: cpr::CmdType) -> CmdType {
    use types_nodes::nodes::CmdType::*;
    match v {
        cpr::CMD_UNKNOWN => CMD_UNKNOWN,
        cpr::CMD_SELECT => CMD_SELECT,
        cpr::CMD_UPDATE => CMD_UPDATE,
        cpr::CMD_INSERT => CMD_INSERT,
        cpr::CMD_DELETE => CMD_DELETE,
        cpr::CMD_MERGE => CMD_MERGE,
        cpr::CMD_UTILITY => CMD_UTILITY,
        cpr::CMD_NOTHING => CMD_NOTHING,
        other => panic!("gram converter: invalid CmdType {other}"),
    }
}

fn merge_match_kind(v: cp::MergeMatchKind) -> MergeMatchKind {
    match v {
        cp::MERGE_WHEN_MATCHED => MergeMatchKind::MERGE_WHEN_MATCHED,
        cp::MERGE_WHEN_NOT_MATCHED_BY_SOURCE => {
            MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE
        }
        cp::MERGE_WHEN_NOT_MATCHED_BY_TARGET => {
            MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET
        }
        other => panic!("gram converter: invalid MergeMatchKind {other}"),
    }
}

fn join_type(v: cpr::JoinType) -> JoinType {
    use types_nodes::jointype::JoinType::*;
    match v {
        cpr::JOIN_INNER => JOIN_INNER,
        cpr::JOIN_LEFT => JOIN_LEFT,
        cpr::JOIN_FULL => JOIN_FULL,
        cpr::JOIN_RIGHT => JOIN_RIGHT,
        cpr::JOIN_SEMI => JOIN_SEMI,
        cpr::JOIN_ANTI => JOIN_ANTI,
        cpr::JOIN_RIGHT_SEMI => JOIN_RIGHT_SEMI,
        cpr::JOIN_RIGHT_ANTI => JOIN_RIGHT_ANTI,
        cpr::JOIN_UNIQUE_OUTER => JOIN_UNIQUE_OUTER,
        cpr::JOIN_UNIQUE_INNER => JOIN_UNIQUE_INNER,
        other => panic!("gram converter: invalid JoinType {other}"),
    }
}

fn coercion_form(v: cpr::CoercionForm) -> tn_prim::CoercionForm {
    use tn_prim::CoercionForm::*;
    match v {
        cpr::COERCE_EXPLICIT_CALL => COERCE_EXPLICIT_CALL,
        cpr::COERCE_EXPLICIT_CAST => COERCE_EXPLICIT_CAST,
        cpr::COERCE_IMPLICIT_CAST => COERCE_IMPLICIT_CAST,
        cpr::COERCE_SQL_SYNTAX => COERCE_SQL_SYNTAX,
        other => panic!("gram converter: invalid CoercionForm {other}"),
    }
}

fn sort_by_dir(v: cs::SortByDir) -> tn::SortByDir {
    use tn::SortByDir::*;
    match v {
        cs::SORTBY_DEFAULT => SORTBY_DEFAULT,
        cs::SORTBY_ASC => SORTBY_ASC,
        cs::SORTBY_DESC => SORTBY_DESC,
        cs::SORTBY_USING => SORTBY_USING,
        other => panic!("gram converter: invalid SortByDir {other}"),
    }
}

fn sort_by_nulls(v: cs::SortByNulls) -> tn::SortByNulls {
    use tn::SortByNulls::*;
    match v {
        cs::SORTBY_NULLS_DEFAULT => SORTBY_NULLS_DEFAULT,
        cs::SORTBY_NULLS_FIRST => SORTBY_NULLS_FIRST,
        cs::SORTBY_NULLS_LAST => SORTBY_NULLS_LAST,
        other => panic!("gram converter: invalid SortByNulls {other}"),
    }
}

fn grouping_set_kind(v: cp::GroupingSetKind) -> tn::GroupingSetKind {
    use tn::GroupingSetKind::*;
    match v {
        cp::GROUPING_SET_EMPTY => GROUPING_SET_EMPTY,
        cp::GROUPING_SET_SIMPLE => GROUPING_SET_SIMPLE,
        cp::GROUPING_SET_ROLLUP => GROUPING_SET_ROLLUP,
        cp::GROUPING_SET_CUBE => GROUPING_SET_CUBE,
        cp::GROUPING_SET_SETS => GROUPING_SET_SETS,
        other => panic!("gram converter: invalid GroupingSetKind {other}"),
    }
}

fn cte_materialize(v: cp::CTEMaterialize) -> tn::CTEMaterialize {
    use tn::CTEMaterialize::*;
    match v {
        cp::CTE_MATERIALIZE_DEFAULT => CTEMaterializeDefault,
        cp::CTE_MATERIALIZE_ALWAYS => CTEMaterializeAlways,
        cp::CTE_MATERIALIZE_NEVER => CTEMaterializeNever,
        other => panic!("gram converter: invalid CTEMaterialize {other}"),
    }
}

fn lock_clause_strength(v: cp::LockClauseStrength) -> types_nodes::rawnodes::LockClauseStrength {
    use types_nodes::rawnodes::LockClauseStrength::*;
    match v {
        cp::LCS_NONE => LCS_NONE,
        cp::LCS_FORKEYSHARE => LCS_FORKEYSHARE,
        cp::LCS_FORSHARE => LCS_FORSHARE,
        cp::LCS_FORNOKEYUPDATE => LCS_FORNOKEYUPDATE,
        cp::LCS_FORUPDATE => LCS_FORUPDATE,
        other => panic!("gram converter: invalid LockClauseStrength {other}"),
    }
}

fn lock_wait_policy(v: cp::LockWaitPolicy) -> types_nodes::rawnodes::LockWaitPolicy {
    use types_nodes::rawnodes::LockWaitPolicy::*;
    match v {
        cp::LOCK_WAIT_BLOCK => LockWaitBlock,
        cp::LOCK_WAIT_SKIP => LockWaitSkip,
        cp::LOCK_WAIT_ERROR => LockWaitError,
        other => panic!("gram converter: invalid LockWaitPolicy {other}"),
    }
}
