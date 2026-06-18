// Statement-node converters (included into convert.rs).
//
// c2rust statement structs live in `cs` (parsenodes_stmts); the owned targets
// in `tn` (types_nodes::rawnodes), except RawStmt which lives in
// types_nodes::parsestmt.

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

fn conv_select<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::SelectStmt) -> PgResult<tn::SelectStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::SelectStmt {
        distinctClause: distinct_clause_list(mcx, s.distinctClause)?,
        intoClause: into_clause_opt(mcx, s.intoClause)?,
        targetList: node_list(mcx, s.targetList)?,
        fromClause: node_list(mcx, s.fromClause)?,
        whereClause: node_opt(mcx, s.whereClause)?,
        groupClause: node_list(mcx, s.groupClause)?,
        groupDistinct: s.groupDistinct,
        havingClause: node_opt(mcx, s.havingClause)?,
        windowClause: node_list(mcx, s.windowClause)?,
        valuesLists: node_list(mcx, s.valuesLists)?,
        sortClause: node_list(mcx, s.sortClause)?,
        limitOffset: node_opt(mcx, s.limitOffset)?,
        limitCount: node_opt(mcx, s.limitCount)?,
        limitOption: limit_option(s.limitOption),
        lockingClause: node_list(mcx, s.lockingClause)?,
        withClause: child_opt(mcx, s.withClause, conv_withclause)?,
        op: set_operation(s.op),
        all: s.all,
        larg: child_opt(mcx, s.larg, conv_select)?,
        rarg: child_opt(mcx, s.rarg, conv_select)?,
    })
}

/// `intoClause` — surfaced as the full owned `ddlnodes::IntoClause` carried as
/// a `Node::IntoClause` (F2). The grammar reinterprets the `IntoClause *` like
/// any tagged sub-node, so it routes through `convert_node`.
fn into_clause_opt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::IntoClause,
) -> PgResult<Option<NodePtr<'mcx>>> {
    child_node_opt(mcx, p)
}

// ---------------------------------------------------------------------------
// INSERT / UPDATE / DELETE / MERGE
// ---------------------------------------------------------------------------

fn conv_insert<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::InsertStmt) -> PgResult<tn::InsertStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::InsertStmt {
        relation: child_opt(mcx, s.relation, conv_rangevar)?,
        cols: node_list(mcx, s.cols)?,
        selectStmt: node_opt(mcx, s.selectStmt)?,
        onConflictClause: child_opt(mcx, s.onConflictClause, conv_onconflict_clause)?,
        returningClause: child_opt(mcx, s.returningClause, conv_returning)?,
        withClause: child_opt(mcx, s.withClause, conv_withclause)?,
        r#override: overriding_kind(s.override_),
    })
}

fn conv_update<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::UpdateStmt) -> PgResult<tn::UpdateStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::UpdateStmt {
        relation: child_opt(mcx, s.relation, conv_rangevar)?,
        targetList: node_list(mcx, s.targetList)?,
        whereClause: node_opt(mcx, s.whereClause)?,
        fromClause: node_list(mcx, s.fromClause)?,
        returningClause: child_opt(mcx, s.returningClause, conv_returning)?,
        withClause: child_opt(mcx, s.withClause, conv_withclause)?,
    })
}

fn conv_delete<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::DeleteStmt) -> PgResult<tn::DeleteStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::DeleteStmt {
        relation: child_opt(mcx, s.relation, conv_rangevar)?,
        usingClause: node_list(mcx, s.usingClause)?,
        whereClause: node_opt(mcx, s.whereClause)?,
        returningClause: child_opt(mcx, s.returningClause, conv_returning)?,
        withClause: child_opt(mcx, s.withClause, conv_withclause)?,
    })
}

fn conv_merge<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::MergeStmt) -> PgResult<tn::MergeStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::MergeStmt {
        relation: child_opt(mcx, s.relation, conv_rangevar)?,
        sourceRelation: node_opt(mcx, s.sourceRelation)?,
        joinCondition: node_opt(mcx, s.joinCondition)?,
        mergeWhenClauses: node_list(mcx, s.mergeWhenClauses)?,
        returningClause: child_opt(mcx, s.returningClause, conv_returning)?,
        withClause: child_opt(mcx, s.withClause, conv_withclause)?,
    })
}

fn conv_setop_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::SetOperationStmt,
) -> PgResult<tn::SetOperationStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tn::SetOperationStmt {
        op: set_operation(s.op),
        all: s.all,
        larg: node_opt(mcx, s.larg)?,
        rarg: node_opt(mcx, s.rarg)?,
        colTypes: oid_list(mcx, s.colTypes)?,
        colTypmods: int_list(mcx, s.colTypmods)?,
        colCollations: oid_list(mcx, s.colCollations)?,
        groupClauses: node_list(mcx, s.groupClauses)?,
    })
}

// --- PL/pgSQL raw-parse-mode statements ---

fn conv_returnstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cs::ReturnStmt) -> PgResult<tdn::ReturnStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ReturnStmt { returnval: node_opt(mcx, s.returnval)? })
}

fn conv_plassignstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cs::PLAssignStmt,
) -> PgResult<tdn::PLAssignStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::PLAssignStmt {
        name: cstr_opt(mcx, s.name)?,
        indirection: node_list(mcx, s.indirection)?,
        nnames: s.nnames,
        val: child_node_opt(mcx, s.val)?,
        location: s.location,
    })
}
